// Extracts a routable car-profile road graph from an OSM PBF extract.
//
// Output is a single binary container (rfroutesrc-v1) with the directed
// junction-collapsed edge list, per-edge polyline geometry, and a street-name
// table. The route-graph builder (src/route_graph_build.js) consumes it.
//
//   node scripts/osm_road_graph.mjs <extract.osm.pbf> <out.graph.bin>
//
// Only the largest strongly connected component is kept so every retained
// node can reach every other retained node; oneway pockets and disconnected
// fragments would otherwise poison random-pair benchmarks.

import { closeSync, fstatSync, fsyncSync, openSync, readSync, renameSync, unlinkSync, writeSync } from "node:fs";
import { scanPbf } from "./osm_pbf.mjs";

export const ROAD_SOURCE_FORMAT = "rfroutesrc-v8";

const EARTH_RADIUS_METERS = 6371008.7714;

/**
 * The class a way routes as.
 *
 * A ferry has no `highway` tag at all — it is `route=ferry` — so every class
 * lookup keyed on `highway` treated the crossing as unroutable and dropped it.
 * On a coastline that is not a missing edge, it is a missing road: the router
 * would send a driver the long way round a river, or refuse the trip outright,
 * with nothing on screen to say a boat was the answer.
 */
export const FERRY_CLASS = "ferry";

/**
 * A train that carries cars: the Chunnel, the Alpine tunnels, the Rocky
 * Mountaineer's motorail. Routable exactly like a ferry — you drive on, you
 * wait, you drive off — and unroutable without this, which cuts a country in
 * half wherever one is the only way through a mountain.
 */
export const SHUTTLE_CLASS = "shuttle_train";

export function wayClass(tags) {
  if (tags.get("route") === "ferry") return FERRY_CLASS;
  if (tags.get("route") === "shuttle_train" || tags.get("route") === "shuttle") return SHUTTLE_CLASS;
  return tags.get("highway");
}

// Default car speeds in km/h per highway class. Ways whose class is absent
// here are not drivable and are skipped.
const CAR_SPEEDS = {
  motorway: 105,
  motorway_link: 55,
  trunk: 85,
  trunk_link: 45,
  primary: 65,
  primary_link: 40,
  secondary: 55,
  secondary_link: 35,
  tertiary: 45,
  tertiary_link: 30,
  unclassified: 40,
  residential: 30,
  living_street: 10,
  service: 15,
  road: 30,
  // Nominal only: a crossing's real speed comes from its `duration` tag and
  // its real cost is mostly the wait for the next sailing. See ferryProfile.
  [FERRY_CLASS]: 20,
  // Nominal only, like a ferry: what it really costs is the wait and the
  // timetable, not the speed of the train.
  [SHUTTLE_CLASS]: 60
};

// Effective cycling speeds, not the speed a bicycle is capable of. A
// segregated cycleway is ridden at a steady pace; the same effort on an
// arterial is spent stopping, waiting and threading past traffic, and comes
// out slower over the distance. Pricing that honestly is also what makes the
// router prefer cycle infrastructure: with a cycleway only a shade quicker
// than a primary road, any cycleway more than a few percent longer lost, and
// riders were sent down main roads to save seconds.
const BIKE_SPEEDS = {
  cycleway: 18,
  living_street: 14,
  residential: 15,
  unclassified: 15,
  tertiary: 15,
  tertiary_link: 15,
  secondary: 13,
  secondary_link: 13,
  primary: 10,
  primary_link: 10,
  service: 12,
  track: 11,
  path: 12,
  footway: 6,
  pedestrian: 6,
  road: 14,
  [FERRY_CLASS]: 20,
  // Nominal only, like a ferry: what it really costs is the wait and the
  // timetable, not the speed of the train.
  [SHUTTLE_CLASS]: 60
};

/**
 * Cycle infrastructure painted on an ordinary road.
 *
 * A residential street with a marked lane is a better ride than one without,
 * and OSM says so on the road itself rather than as a separate way — which
 * the profile previously read only to work out contraflow, never preference.
 */
const CYCLE_INFRASTRUCTURE = new Set([
  "lane", "track", "opposite_lane", "opposite_track", "opposite",
  "share_busway", "opposite_share_busway", "shared_lane", "sidepath",
  "segregated", "crossing", "yes"
]);

function bikeInfrastructureFactor(tags) {
  if (tags.get("bicycle") === "designated") return 1.2;
  for (const key of ["cycleway", "cycleway:both", "cycleway:left", "cycleway:right"]) {
    const value = tags.get(key);
    if (value && CYCLE_INFRASTRUCTURE.has(value)) return 1.2;
  }
  return 1;
}

const FOOT_SPEEDS = {
  footway: 5,
  path: 5,
  pedestrian: 5,
  steps: 3,
  living_street: 5,
  residential: 5,
  service: 5,
  track: 5,
  unclassified: 5,
  tertiary: 5,
  tertiary_link: 5,
  secondary: 5,
  secondary_link: 5,
  primary: 4,
  primary_link: 4,
  cycleway: 5,
  road: 5,
  [FERRY_CLASS]: 20,
  // Nominal only, like a ferry: what it really costs is the wait and the
  // timetable, not the speed of the train.
  [SHUTTLE_CLASS]: 60
};

const ACCESS_DENIED = new Set(["no", "private", "delivery", "agricultural", "forestry", "military"]);

/**
 * Access that permits reaching a place but not passing through it.
 *
 * `access=customers` is how a shopping centre's car park is tagged, and it
 * says you may drive here if you are a customer — which is exactly what
 * somebody being routed to that centre is. Treating it as a refusal made
 * every aisle, drive-through and service road on the site invisible: the
 * router could not deliver anyone into the car park, could not say which
 * entrance to use, and a driver crossing it was permanently off-route.
 *
 * They are not through-routes either, so they are slowed rather than simply
 * opened. A car park is reachable when it is where you are going, and
 * unattractive as a shortcut past the queue on the boulevard.
 */
const ACCESS_DESTINATION_ONLY = new Set(["customers", "destination", "permit"]);

/** Access values that mean "yes, this class may use this road". */
const ACCESS_ALLOWS = new Set(["yes", "designated", "permissive", "official"]);

function destinationOnly(tags) {
  for (const key of ["motor_vehicle", "vehicle", "access"]) {
    const value = tags.get(key);
    if (value != null) return ACCESS_DESTINATION_ONLY.has(value);
  }
  return false;
}

/**
 * Barriers on a node, and who may pass them.
 *
 * A gate across a track, a bollard at the end of an alley, a kissing gate on
 * a field path: these sever a way as completely as a missing road, and none
 * of them was being read. Every one of them was a routable road — so a
 * driver could be sent down a lane that ends at a locked gate, and told to
 * turn into an alley a car has never been able to enter.
 *
 * Three kinds, because OSM's barriers behave in three ways:
 *
 * - **Impassable to everything.** A wall or a ditch is not a way through.
 * - **Impassable to motor vehicles, open to people.** A bollard, a stile, a
 *   kissing gate. This is the common case and the one that matters most for
 *   a delivery van: an alley closed by bollards is a perfectly good footpath
 *   and not a road at all.
 * - **Gated but ordinarily passable.** A gate, a lift gate, a toll booth.
 *   Open unless the tagging says otherwise, because most of them stand open
 *   and refusing them all would cut up farm tracks and car parks everywhere.
 *
 * An explicit access tag on the node outranks the table in both directions,
 * which is how a gate marked `foot=yes` stays a footpath and a bollard
 * marked `motor_vehicle=yes` stays a road.
 */
const BARRIER_BLOCKS_ALL = new Set([
  "wall", "fence", "hedge", "ditch", "jersey_barrier", "debris", "yes", "guard_rail"
]);

/**
 * Barriers that stop a motor vehicle. Most of them exist for exactly that:
 * a bollard is put there to keep cars out of a lane people still walk and
 * cycle down, and reading it as a closure for everyone would delete the
 * footpath along with the road.
 */
const BARRIER_BLOCKS_MOTOR = new Set([
  "bollard", "block", "planter", "cycle_barrier", "motorcycle_barrier",
  "sump_buster", "stile", "kissing_gate", "turnstile", "full-height_turnstile",
  "wicket_gate", "hampshire_gate"
]);

/** Barriers people on foot get through but wheels do not. */
const BARRIER_FOOT_ONLY = new Set([
  "stile", "kissing_gate", "turnstile", "full-height_turnstile", "wicket_gate"
]);

/**
 * Whether [profile] is stopped by the barrier on this node.
 *
 * Absence of a barrier tag is the overwhelmingly common case and returns
 * false immediately; nothing else here runs for an ordinary junction.
 */
export function barrierBlocks(profileName, tags) {
  if (!tags) return false;
  const barrier = tags.get("barrier");
  if (!barrier) return false;

  // What the node itself says, most specific first. An explicit yes or no is
  // the mapper telling us about this barrier in particular, and it wins.
  const keys = profileName === "car"
    ? ["motorcar", "motor_vehicle", "vehicle", "access"]
    : profileName === "bike"
      ? ["bicycle", "vehicle", "access"]
      : ["foot", "access"];
  for (const key of keys) {
    const value = tags.get(key);
    if (value == null) continue;
    if (ACCESS_DENIED.has(value)) return true;
    // `customers`/`destination` are a way in, not a way through; the way-level
    // rules already slow those, so the barrier itself does not refuse them.
    return false;
  }
  // A gate that is actually locked is a wall with hinges.
  if (tags.get("locked") === "yes") return true;

  if (BARRIER_BLOCKS_ALL.has(barrier)) return true;
  // Everything left is something a person walks through — that is what a
  // stile is for — so on foot none of it is a refusal.
  if (profileName === "foot") return false;
  // A cycle barrier is built to slow a bicycle, not to stop one; the rest of
  // the foot-only barriers do stop one.
  // On a bicycle only the ones built around a person stop you. A bollard is
  // put there to let you through, and a cycle barrier to slow you down.
  if (profileName === "bike") return BARRIER_FOOT_ONLY.has(barrier);
  return BARRIER_BLOCKS_MOTOR.has(barrier);
}

/**
 * How long a crossing takes, in seconds, from OSM's `duration`.
 *
 * Written either as a clock ("00:12", "1:30:00") or as an ISO 8601 period
 * ("PT10M"). Returns 0 when there is no usable value, which leaves the
 * crossing costed from its nominal class speed.
 */
export function parseDuration(value) {
  if (!value) return 0;
  const text = String(value).trim();
  const iso = /^P(?:(\d+)D)?T?(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$/i.exec(text);
  if (iso && (iso[1] || iso[2] || iso[3] || iso[4])) {
    return Number(iso[1] || 0) * 86400 + Number(iso[2] || 0) * 3600 +
      Number(iso[3] || 0) * 60 + Number(iso[4] || 0);
  }
  const parts = text.split(":").map(part => Number(part.trim()));
  if (!parts.length || parts.some(part => !Number.isFinite(part))) return 0;
  // Two fields are hours:minutes — OSM's convention for `duration`, not
  // minutes:seconds. A twelve-minute crossing is "00:12".
  if (parts.length === 2) return parts[0] * 3600 + parts[1] * 60;
  if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
  if (parts.length === 1) return parts[0] * 60;
  return 0;
}

/**
 * The wait before a crossing, in seconds.
 *
 * A ferry is not a road, and pricing one as if it were is what makes a router
 * send somebody down to a slip to sit for forty minutes to save four. What it
 * costs is mostly the wait for the next sailing, and `interval` is the
 * headway — so half of it is the wait a driver arriving at random expects.
 * With no interval tagged, a default stands in rather than nothing: zero is
 * the one answer that is certainly wrong.
 */
export function ferryWaitSeconds(tags) {
  const interval = parseDuration(tags.get("interval"));
  const expected = interval > 0 ? interval / 2 : FERRY_DEFAULT_WAIT_SECONDS;
  return Math.min(expected, FERRY_MAX_WAIT_SECONDS);
}

const FERRY_DEFAULT_WAIT_SECONDS = 600;
const FERRY_MAX_WAIT_SECONDS = 1800;

/**
 * Whether a profile may board this crossing.
 *
 * Ferries carry different things — some take cars, some are foot-and-bicycle
 * only — and they say so on themselves. Absent tags mean the usual: a ferry
 * route in the road network carries vehicles unless it says otherwise.
 */
/** A crossing you board and wait for: a boat, or a train that takes cars. */
export function isBoarded(cls) {
  return cls === FERRY_CLASS || cls === SHUTTLE_CLASS;
}

function ferryAllowed(tags, profileName) {
  const keys = profileName === "car"
    ? ["motorcar", "motor_vehicle", "vehicle", "access"]
    : profileName === "bike"
      ? ["bicycle", "vehicle", "access"]
      : ["foot", "access"];
  for (const key of keys) {
    const value = tags.get(key);
    if (value != null) return !ACCESS_DENIED.has(value);
  }
  return true;
}

/**
 * An expressway that is not tagged as one.
 *
 * `motorroad=yes` carries motorway rules onto an ordinary highway class — no
 * pedestrians, no bicycles — and nothing in `highway=primary` reveals it.
 * Excluding motorway and trunk by class alone therefore misses exactly the
 * roads a rider would be most alarmed to be sent down.
 */
function motorroad(tags) {
  return tags.get("motorroad") === "yes";
}

// Fixed junction penalties in deciseconds, applied to every edge entering a
// tagged node. Signals dominate systematic urban ETA error.
// A mini-roundabout is a give-way that everybody slows for, so it costs a
// little more than the painted triangle and far less than a signal.
const CAR_NODE_PENALTIES = { traffic_signals: 100, stop: 20, give_way: 10, level_crossing: 60, crossing_signals: 50, mini_roundabout: 25 };
const BIKE_NODE_PENALTIES = { traffic_signals: 80, stop: 15, give_way: 5, level_crossing: 40, crossing_signals: 40, mini_roundabout: 15 };
const FOOT_NODE_PENALTIES = { traffic_signals: 60, level_crossing: 30, crossing_signals: 20 };

/**
 * Collapses the tagged nodes of one intersection into one charge.
 *
 * Penalties are read off individual nodes, and a wide intersection carries
 * several: a divided highway has a signal on each carriageway, and a signalised
 * junction has a pedestrian crossing on each arm. All of them are one red
 * light to a driver, but the cost model charged each in turn, so crossing one
 * boulevard cost two or three signal waits.
 *
 * That is not a rounding error, it inverts routes. Chemin de la Grande-Côte
 * crosses Boulevard Labelle at grade in Rosemère; the twelve-metre segment
 * between the carriageways priced out at twenty-six seconds, and the router
 * preferred to leave the road, run a hundred metres up the boulevard and come
 * back — a detour a driver on a straight road watched it propose. The detour
 * does not avoid the wait; it just passed fewer tagged nodes.
 *
 * The query layer already merges junction *markers* within thirty metres for
 * display, on the same reasoning and the same radius. This applies it to the
 * cost, in rank order so the signal is the one that gets charged and the
 * crossings beside it fall in behind it. Kind codes are left alone: what is
 * drawn on the map is the query layer's business.
 *
 * Run once per direction of travel. Two signals five metres apart facing
 * opposite ways are one red light to nobody — they are one each to two
 * drivers — so merging them across directions would leave one carriageway
 * paying for a wait and the other paying for nothing. [faces] says which
 * approaches each node governs, and only nodes a given driver actually meets
 * take part in that driver's merge.
 */
export function chargeEachIntersectionOnce(usedIds, latE7, lonE7, penaltyDs, kindCode, log, faces = null, facing = FACES_BOTH) {
  // Same radius as the query layer's marker merge, for the same reason —
  // widened with it when a 31 m crossroads in Rosemère turned out to be one
  // intersection that both layers were calling two. No along-route bound here:
  // there is no route yet, only nodes, and a node cannot be visited twice.
  const MERGE_METERS = 45;
  const charged = [];
  const grid = new Map();
  // Cell side comfortably over the merge radius, so a neighbour can only be in
  // this cell or one adjacent to it.
  const CELL_E7 = 5000;
  const key = (cx, cy) => `${cx},${cy}`;

  const candidates = [];
  for (let i = 0; i < penaltyDs.length; i++) {
    if (penaltyDs[i] <= 0) continue;
    if (faces && (faces[i] & facing) === 0) {
      // Not this driver's sign at all: no charge, and it must not absorb one
      // that is.
      penaltyDs[i] = 0;
      continue;
    }
    candidates.push(i);
  }
  // Heaviest first: the signal becomes the intersection's charge, not whichever
  // crossing happened to be scanned first.
  candidates.sort((a, b) => penaltyDs[b] - penaltyDs[a] || usedIds[a] - usedIds[b]);

  let merged = 0;
  for (const index of candidates) {
    const lat = latE7[index];
    const lon = lonE7[index];
    const cx = Math.floor(lat / CELL_E7);
    const cy = Math.floor(lon / CELL_E7);
    let absorbed = false;
    for (let dx = -1; dx <= 1 && !absorbed; dx++) {
      for (let dy = -1; dy <= 1 && !absorbed; dy++) {
        const bucket = grid.get(key(cx + dx, cy + dy));
        if (!bucket) continue;
        for (const other of bucket) {
          if (haversineMetersE7(lat, lon, latE7[other], lonE7[other]) < MERGE_METERS) {
            absorbed = true;
            break;
          }
        }
      }
    }
    if (absorbed) {
      penaltyDs[index] = 0;
      merged++;
      continue;
    }
    const cell = key(cx, cy);
    if (!grid.has(cell)) grid.set(cell, []);
    grid.get(cell).push(index);
    charged.push(index);
  }
  const which = facing === FACES_FORWARD ? "forward" : facing === FACES_BACKWARD ? "backward" : "both";
  log(`junction penalties (${which}): ${charged.length} intersections charged, ${merged} duplicate nodes absorbed`);
}

/** Per-edge flags. Bit 0: the edge runs inside a roundabout. */
export const EDGE_FLAG_ROUNDABOUT = 1;

/**
 * This road is tolled.
 *
 * A flag rather than a cost, because a toll is not slow — it is a decision.
 * Some drivers pay it every day without thinking and some will drive twenty
 * minutes to avoid two dollars, and a router that folds the money into the
 * time has answered that question for both of them.
 */
export const EDGE_FLAG_TOLL = 2;

/**
 * This edge is a boat.
 *
 * The class already says so on the roads themselves, but a shortcut carries
 * no class — and "avoid ferries" has to survive being collapsed into one, or
 * the hierarchy is a hole the preference falls through exactly the way the
 * height limits did.
 */
export const EDGE_FLAG_FERRY = 4;

/**
 * Dangerous goods are refused here.
 *
 * `hazmat=no` on a tunnel is not a preference and not a cost: a tanker in
 * the Ville-Marie is a criminal matter, and the sign is there because the
 * consequence of ignoring it is measured in lives rather than minutes. A
 * flag, so a vehicle that declares it is carrying any is simply refused the
 * road — and a vehicle that declares nothing is unaffected.
 */
export const EDGE_FLAG_NO_HAZMAT = 8;

/**
 * Goods vehicles are refused here.
 *
 * `hgv=no` is the most-mapped restriction in the world that this router had
 * no way to read, and it is aimed squarely at the vehicle a delivery app is
 * routing: a residential street signed against lorries, a bridge approach,
 * a town centre closed to through freight. A van under 3.5 t is not an HGV
 * and is not affected, which is why this is a flag on the road and a
 * declaration on the vehicle rather than something baked into the profile —
 * one index serves the car and the truck.
 *
 * `hgv=destination` is refused for the same reason `access=destination` is:
 * the router cannot tell a delivery on this street from a shortcut through
 * it, and guessing wrong sends a lorry down a road it is signed out of.
 */
export const EDGE_FLAG_NO_HGV = 16;

/**
 * A road reserved for public service vehicles, and one reserved for
 * car-pools.
 *
 * These used to be deleted at extract time — `access=psv` meant the way
 * never entered the graph — which is right for the car it was written for
 * and leaves a bus or taxi driver unable to be routed down the very roads
 * their job is on. Deleting also makes the decision unrecoverable: no
 * option at query time can bring back a way that was never written.
 *
 * So the road stays and says who it is for, and the ordinary driver is
 * refused it by the same mechanism that refuses them a toll or a ferry.
 * Two bits rather than one because the entitlements differ: a car with a
 * passenger may use an HOV lane and never a bus lane.
 */
export const EDGE_FLAG_PSV_ONLY = 32;
export const EDGE_FLAG_HOV_ONLY = 64;

/**
 * A car-pool road that asks for three aboard rather than two.
 *
 * Not every reserved lane wants the same carful: Québec tags 109 ways with
 * `hov:minimum` and 22 of them read 3. A driver with one passenger who is
 * sent down one of those is in a reserved lane they do not qualify for,
 * which is the fine this whole family of flags exists to avoid.
 *
 * Encoded as a second value rather than a modifier — a road is HOV-2 or
 * HOV-3, never both — so it drops straight into the same refusal mask as
 * everything else: a driver avoids whichever minimums they fall short of.
 */
export const EDGE_FLAG_HOV3_ONLY = 128;

/**
 * How many have to be aboard for a car-pool lane here.
 *
 * Two where the map does not say, that being what nearly every sign reads
 * and what `hov=designated` means on its own.
 */
export const HOV_DEFAULT_MINIMUM = 2;

export function hovMinimum(tags) {
  const raw = Number(String(tags.get("hov:minimum") ?? "").trim());
  return Number.isFinite(raw) && raw >= 2 ? Math.min(9, Math.round(raw)) : HOV_DEFAULT_MINIMUM;
}

/**
 * Who a whole road is reserved for, as edge flags; 0 for an ordinary road.
 *
 * The same question [laneAccessList] asks of one lane, asked of the way.
 */
export function reservedFor(tags) {
  const access = tags.get("access");
  const carpool = hovMinimum(tags) >= 3 ? EDGE_FLAG_HOV3_ONLY : EDGE_FLAG_HOV_ONLY;
  if (access === "psv" || access === "bus") return EDGE_FLAG_PSV_ONLY;
  if (access === "hov") return carpool;
  if (tags.get("motor_vehicle") === "psv" || tags.get("motorcar") === "psv") {
    return EDGE_FLAG_PSV_ONLY;
  }
  // `hov=designated` with nothing said about cars is a car-pool lane; where
  // `motorcar` is spelled out, that is the more specific claim and wins.
  if (tags.get("hov") === "designated" && tags.get("motorcar") == null) {
    return carpool;
  }
  return 0;
}

/**
 * What passing here costs, in the smallest unit of its currency.
 *
 * OSM tags money as `charge=2.50 CAD` or `toll:charge=5 EUR`, and sometimes
 * with a per-vehicle qualifier this deliberately ignores — a price that is
 * only right for one class of vehicle is worse than no price, and the honest
 * answer for a van is "there is a toll here" rather than a car's fare.
 *
 * Returns null when there is nothing usable, which is most tolled roads:
 * `toll=yes` is mapped far more often than any price is.
 */
export function parseCharge(value) {
  if (value == null) return null;
  const text = String(value).trim();
  if (!text) return null;
  // A conditional or per-vehicle charge is a table, not a number.
  if (text.includes("@") || text.includes(";")) return null;
  const match = text.match(/^([0-9]+(?:[.,][0-9]+)?)\s*([A-Za-z]{3})?$/);
  if (!match) return null;
  const amount = Number(match[1].replace(",", "."));
  if (!Number.isFinite(amount) || amount <= 0) return null;
  return { cents: Math.round(amount * 100), currency: (match[2] || "").toUpperCase() };
}

/** Whether this way is tolled for the profile being built. */
export function wayTolled(tags) {
  for (const key of ["toll:motorcar", "toll"]) {
    const value = tags.get(key);
    if (value == null) continue;
    return value === "yes" || value === "true" || value === "1";
  }
  return false;
}

/** Which way a sign faces: 1 forward along the way, 2 backward, 3 both. */
export const FACES_FORWARD = 1;
export const FACES_BACKWARD = 2;
export const FACES_BOTH = FACES_FORWARD | FACES_BACKWARD;

/**
 * Which approach a stop sign, signal or give-way actually governs.
 *
 * A stop sign is not a property of an intersection, it is a property of one
 * approach to it — and OSM says so: 36,704 of Québec's stop nodes are tagged
 * `direction=forward` and 31,468 `direction=backward`, against 4,882 with no
 * direction at all. Reading the node without its direction put a stop sign in
 * front of every driver who passed it, including the ones on the road that
 * has right of way. On a drive down Rue Main the app drew three stops the
 * driver never had to make, all of them belonging to the side streets.
 *
 * Cardinal forms ("N", "SSE") and bearings ("225") are also in use, but far
 * down the tail and not resolvable without the way's own heading at that
 * node; they are read as facing both ways, which is the old behaviour and the
 * safe one — a stop shown that is not there is a smaller fault than a stop
 * hidden that is.
 */
export function signFacing(tags) {
  const direction = tags.get("direction");
  if (direction === "forward") return FACES_FORWARD;
  if (direction === "backward") return FACES_BACKWARD;
  return FACES_BOTH;
}

function nodePenaltyDs(profile, tags) {
  if (!tags) return 0;
  const table = profile.nodePenalties;
  const highway = tags.get("highway");
  if (highway === "crossing") {
    return tags.get("crossing") === "traffic_signals" ? table.crossing_signals || 0 : 0;
  }
  if (highway && table[highway]) return table[highway];
  if (tags.get("railway") === "level_crossing") return table.level_crossing || 0;
  return 0;
}

// Junction kinds surfaced to navigation UIs (route results annotate where
// an edge enters one): 0 none, 1 traffic signals, 2 stop, 3 give way,
// 4 level crossing, 5 pedestrian crossing.
export const JUNCTION_KINDS = {
  traffic_signals: 1, stop: 2, give_way: 3, level_crossing: 4, crossing: 5,
  // A mini-roundabout is a painted circle on an ordinary crossroads: no arcs,
  // no way to count exits from, and nothing in the geometry that says it is
  // there at all. The turn angle through it is the real instruction, so this
  // is drawn beside that turn rather than replacing it — "turn left" is still
  // what the driver does, and the symbol says what they will be turning at.
  mini_roundabout: 6
};

/**
 * Crossing values that mean there is something painted on the road.
 *
 * `uncontrolled` is the historical tag for a marked crossing without signals
 * and still the commonest in North America; `zebra` and `marked` say it
 * outright. Everything else — `unmarked` above all — is a place the footway
 * meets the road and nothing is drawn there at all.
 */
const MARKED_CROSSINGS = new Set(["marked", "zebra", "uncontrolled", "traffic_signals"]);

export function nodeKindCode(tags) {
  if (!tags) return 0;
  const highway = tags.get("highway");
  // A signal-controlled pedestrian crossing is a signal for a driver.
  if (highway === "crossing" && tags.get("crossing") === "traffic_signals") {
    return JUNCTION_KINDS.traffic_signals;
  }
  // Most crossing nodes in OSM are not crossings a driver would recognise:
  // they exist to join a sidewalk or footpath to the roadway so pedestrian
  // routing works at all, and nothing is painted there. Drawing them put a
  // marker on the driver's map every few metres for something that is not on
  // the road, which is exactly what was reported. Only a crossing that is
  // actually marked is a feature of the road.
  if (highway === "crossing") {
    const crossing = tags.get("crossing");
    const markings = tags.get("crossing:markings");
    const marked = (crossing && MARKED_CROSSINGS.has(crossing)) ||
      (markings != null && markings !== "no");
    if (!marked) return 0;
  }
  if (highway && JUNCTION_KINDS[highway]) return JUNCTION_KINDS[highway];
  if (tags.get("railway") === "level_crossing") return JUNCTION_KINDS.level_crossing;
  return 0;
}

// Importance when several tagged nodes fall on one edge: signals > stop >
// level crossing > mini-roundabout > give way > plain crossing.
const JUNCTION_RANK = [0, 6, 5, 2, 4, 1, 3];

// Turn costs in deciseconds by geometric turn kind. Left turns cross
// oncoming traffic under right-hand driving, so they cost more than right
// turns; u-turns are heavily penalized (never forbidden — dead ends need
// them) unless an explicit restriction forbids the movement.
const CAR_TURN_COSTS = { uturn: 150, left: 40, right: 15, slightLeft: 20, slightRight: 8 };
const BIKE_TURN_COSTS = { uturn: 40, left: 15, right: 8, slightLeft: 8, slightRight: 4 };

export const PROFILES = {
  car: {
    name: "car",
    speeds: CAR_SPEEDS,
    nodePenalties: CAR_NODE_PENALTIES,
    turnCosts: CAR_TURN_COSTS,
    maxSpeedKmh: 130,
    allowed(tags) {
      return carAllowed(tags);
    },
    oneway(tags) {
      return parseOneway(tags);
    },
    speedTags: true
  },
  bike: {
    name: "bike",
    speeds: BIKE_SPEEDS,
    nodePenalties: BIKE_NODE_PENALTIES,
    turnCosts: BIKE_TURN_COSTS,
    maxSpeedKmh: 30,
    allowed(tags) {
      const highway = wayClass(tags);
      if (!highway || !(highway in BIKE_SPEEDS)) return false;
      if (isBoarded(highway)) return ferryAllowed(tags, "bike");
      if (tags.get("area") === "yes") return false;
      if (tags.get("ford") && tags.get("ford") !== "no") return false;
      const bicycle = tags.get("bicycle");
      // An explicit permission outranks everything below: a bridge that
      // carries a cycle track across an expressway says so on itself.
      if (bicycle != null) return !ACCESS_DENIED.has(bicycle) && bicycle !== "use_sidepath";
      if (motorroad(tags)) return false;
      if (highway === "footway" || highway === "pedestrian") return false;
      const access = tags.get("access");
      if (access != null && ACCESS_DENIED.has(access)) return false;
      return true;
    },
    adjustSpeed(tags, speed) {
      return Math.min(20, speed * bikeInfrastructureFactor(tags));
    },
    oneway(tags) {
      if (tags.get("oneway:bicycle") === "no") return 0;
      const cycleway = tags.get("cycleway") || tags.get("cycleway:left") || tags.get("cycleway:right");
      if (cycleway && cycleway.startsWith("opposite")) return 0;
      return parseOneway(tags);
    },
    speedTags: false
  },
  foot: {
    name: "foot",
    speeds: FOOT_SPEEDS,
    nodePenalties: FOOT_NODE_PENALTIES,
    // Pedestrians turn freely; no edge-based expansion for foot.
    turnCosts: null,
    maxSpeedKmh: 6,
    allowed(tags) {
      const highway = wayClass(tags);
      if (!highway || !(highway in FOOT_SPEEDS)) return false;
      if (isBoarded(highway)) return ferryAllowed(tags, "foot");
      if (tags.get("area") === "yes") return false;
      const foot = tags.get("foot");
      if (foot != null) return !ACCESS_DENIED.has(foot);
      if (motorroad(tags)) return false;
      if (highway === "cycleway") return true;
      const access = tags.get("access");
      if (access != null && ACCESS_DENIED.has(access)) return false;
      return true;
    },
    // Pedestrians ignore vehicular oneway.
    oneway() {
      return 0;
    },
    speedTags: false
  }
};

function toE7(value) {
  return Math.round(value * 1e7);
}

function haversineMetersE7(latA, lonA, latB, lonB) {
  const toRad = Math.PI / 180 / 1e7;
  const dLat = (latB - latA) * toRad;
  const dLon = (lonB - lonA) * toRad;
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const a = sinLat * sinLat + Math.cos(latA * toRad) * Math.cos(latB * toRad) * sinLon * sinLon;
  return 2 * EARTH_RADIUS_METERS * Math.asin(Math.min(1, Math.sqrt(a)));
}

function parseMaxspeed(value) {
  if (!value) return 0;
  const text = String(value).trim().toLowerCase();
  if (text === "none") return 120;
  if (text === "walk") return 8;
  const match = text.match(/^(\d+(?:\.\d+)?)\s*(mph)?/);
  if (!match) return 0;
  const speed = Number(match[1]);
  if (!Number.isFinite(speed) || speed <= 0) return 0;
  return match[2] ? speed * 1.609344 : speed;
}

/**
 * A speed limit that only applies at certain times, as OSM records it.
 *
 * School zones are the reason this exists: a road posted 50 that drops to 30
 * on school-day mornings is one road with two limits, and showing the driver
 * the wrong one at 8am is showing them the wrong one at exactly the moment
 * the limit is there for.
 *
 * The tagging is inconsistent in the wild. Across Québec the same idea is
 * written four ways — `30 @ Mo-Fr 07:00-19:00`, `30 @ (Sep-Jun AND Mo-Fr
 * 07:00-17:00)`, `30 @ (Sep-Jun: Mo-Fr 07:00-17:00)` and a bare
 * `30 @ (07:00-21:00)` — two of which are not valid opening-hours syntax at
 * all. So this parses the shape that is actually used rather than the shape
 * the wiki specifies, and returns null on anything it does not recognise
 * instead of guessing a limit into existence.
 *
 * Returns `{ speedKmh, days, startMinute, endMinute, monthStart, monthEnd }`,
 * where `days` is a 7-bit mask from Monday and the month range is inclusive
 * and may wrap (Sep-Jun is a school year, not an error).
 */
/**
 * The `@ (Mo-Fr 07:00-09:00)` half of a conditional tag, as a window.
 *
 * Shared by the two tags that carry one: a speed limit that drops on school
 * mornings, and a turn that is banned during the peak. Null when there is no
 * time range to act on — a condition like `@ wet` is real tagging and simply
 * not something a clock can answer.
 */
export function parseConditionWindow(text) {
  // Parentheses are optional in the wild, and so are the separators inside.
  const condition = String(text || "").trim().replace(/^\(|\)$/g, "").trim();
  if (!condition) return null;

  const time = condition.match(/(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})/);
  if (!time) return null;
  const startMinute = Number(time[1]) * 60 + Number(time[2]);
  const endMinute = Number(time[3]) * 60 + Number(time[4]);
  if (!(startMinute < endMinute)) return null;

  const before = condition.slice(0, time.index);
  const days = parseDayRange(before);
  const months = parseMonthRange(before);

  return {
    // No day range means every day, which is what a bare time range says.
    days: days ?? 0b1111111,
    startMinute,
    endMinute,
    monthStart: months ? months.start : 1,
    monthEnd: months ? months.end : 12
  };
}

export function parseConditionalMaxspeed(value) {
  if (!value) return null;
  const text = String(value).trim();
  const at = text.indexOf("@");
  if (at < 0) return null;

  const speedKmh = parseMaxspeed(text.slice(0, at));
  if (!speedKmh) return null;

  const window = parseConditionWindow(text.slice(at + 1));
  if (!window) return null;
  return { speedKmh: Math.round(speedKmh), ...window };
}

/**
 * A turn restriction that only applies at certain hours.
 *
 * `no_left_turn @ (Mo-Fr 07:00-09:00)` is one sign with two meanings, and the
 * router had exactly one way of reading it: throw the whole relation away and
 * let the turn stand at every hour, including the two it is banned in. That
 * is the wrong direction to fail — a detour costs minutes, an illegal turn
 * costs a ticket and sometimes more.
 */
export function parseConditionalRestriction(value) {
  if (!value) return null;
  const text = String(value).trim();
  const at = text.indexOf("@");
  if (at < 0) return null;
  const kind = text.slice(0, at).trim().toLowerCase();
  if (!/^(no_|only_)/.test(kind)) return null;
  const window = parseConditionWindow(text.slice(at + 1));
  if (!window) return null;
  return { kind, ...window };
}

const DAY_NAMES = ["mo", "tu", "we", "th", "fr", "sa", "su"];
const MONTH_NAMES = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];

/** `Mo-Fr` or `Mo,We,Fr` to a 7-bit mask from Monday, or null if absent. */
function parseDayRange(text) {
  const lower = text.toLowerCase();
  const range = lower.match(/\b(mo|tu|we|th|fr|sa|su)\s*-\s*(mo|tu|we|th|fr|sa|su)\b/);
  if (range) {
    const from = DAY_NAMES.indexOf(range[1]);
    const to = DAY_NAMES.indexOf(range[2]);
    let mask = 0;
    // Ranges may wrap the week (Fr-Mo is a weekend, not nothing).
    for (let i = from; ; i = (i + 1) % 7) {
      mask |= 1 << i;
      if (i === to) break;
    }
    return mask;
  }
  const singles = lower.match(/\b(mo|tu|we|th|fr|sa|su)\b/g);
  if (!singles) return null;
  return singles.reduce((mask, day) => mask | (1 << DAY_NAMES.indexOf(day)), 0);
}

/** `Sep-Jun` to inclusive 1-based months, or null if absent. */
function parseMonthRange(text) {
  const lower = text.toLowerCase();
  const range = lower.match(/\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s*-\s*(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b/);
  if (!range) return null;
  return {
    start: MONTH_NAMES.indexOf(range[1]) + 1,
    end: MONTH_NAMES.indexOf(range[2]) + 1
  };
}

// Surfaces where posted class speeds are unrealistic for a car.
/** How much a destination-only way is slowed, to keep it off through-routes. */
const DESTINATION_ONLY_FACTOR = 0.35;

const SLOW_SURFACES = new Set(["unpaved", "gravel", "fine_gravel", "dirt", "earth", "ground", "grass", "sand", "mud", "compacted", "pebblestone", "wood"]);
const VERY_SLOW_SMOOTHNESS = new Set(["bad", "very_bad", "horrible", "very_horrible", "impassable"]);

// Per-direction km/h for a way: profile class default, capped by maxspeed
// tags (car only), degraded by surface and smoothness.
// Lane guidance. OSM writes `turn:lanes` as one entry per lane, left to right,
// separated by "|", and a lane that serves more than one movement joins them
// with ";" — "left|through;right" is a two-lane approach. A lane's movements
// pack into one byte, which keeps a whole approach inside a handful of bytes
// even on the widest junction.
const LANE_TURNS = new Map([
  ["reverse", 1],
  ["sharp_left", 2],
  ["left", 4],
  ["slight_left", 8],
  ["merge_to_left", 8],
  ["through", 16],
  ["slight_right", 32],
  ["merge_to_right", 32],
  ["right", 64],
  ["sharp_right", 128]
]);

/** A lane's movements as a bit set; 0 for "none" or anything unrecognised. */
function laneMask(spec) {
  let mask = 0;
  for (const part of spec.split(";")) {
    const turn = LANE_TURNS.get(part.trim());
    if (turn) mask |= turn;
  }
  return mask;
}

function laneListFrom(spec, count) {
  if (spec) {
    const masks = spec.split("|").map(laneMask);
    // A tagged lane count that disagrees with the turn list is a mapping
    // error, not something to reconcile: the turn list is the specific claim.
    if (masks.length) return masks;
  }
  // A lane count with no turn tags still tells a driver how many lanes the
  // road has, which is worth drawing even when no movement is known.
  if (count > 0 && count <= 12) return new Array(count).fill(0);
  return [];
}

function parseLaneCount(value) {
  const count = Number.parseInt(value ?? "", 10);
  return Number.isFinite(count) && count > 0 ? count : 0;
}

/**
 * Whether a one-way carriageway shares a centre two-way left-turn lane.
 *
 * A wide street with a painted centre turn lane — the double-headed arrow — is
 * one undivided road: a driver may turn left out of it into any driveway, and
 * the oncoming traffic may do the same. Mappers routinely draw it as two
 * `oneway=yes` ways a lane apart, each claiming the shared lane with
 * `lanes:both_ways=1` and `turn:lanes:both_ways=left`. Boulevard Labelle
 * through Rosemère is tagged exactly that way, ten metres between the two
 * lines.
 *
 * Read literally that is a divided highway with no way across, and the router
 * treated it as one: asked for a business on the far side it drove past the
 * door, crossed at the next driveway that happened to be mapped through the
 * median, and came back — a hundred metres of dogleg for a turn the driver
 * makes every day. This tag is what says the crossing is allowed.
 */
export function centreTurnLane(tags) {
  if (parseLaneCount(tags.get("lanes:both_ways")) > 0) return true;
  const turns = tags.get("turn:lanes:both_ways");
  return Boolean(turns) && turns.includes("left");
}

/**
 * Why a lane is not this driver's, as a bit set; 0 means it is theirs.
 *
 * Kept as reasons rather than a single "closed" bit because the reason is
 * the instruction. "Bus lane" and "HOV lane" are different facts to a driver
 * — one is never theirs, the other is theirs with a passenger — and a lane
 * greyed out with no explanation reads as a rendering fault.
 */
export const LANE_BUS = 1;
export const LANE_HOV = 2;
export const LANE_TAXI = 4;
export const LANE_BICYCLE = 8;
/** Closed to motor traffic outright, by `motor_vehicle:lanes=no` and kin. */
export const LANE_NO_MOTOR = 16;
/**
 * A car-pool lane wanting three aboard rather than two, per `hov:minimum`.
 *
 * A separate value from [LANE_HOV] rather than a modifier on it: a lane asks
 * for one number or the other, and keeping them distinct means a driver with
 * a single passenger is matched against 2 and refused 3 by the same test
 * that handles buses and taxis. Québec tags 91 ways with both `hov:lanes`
 * and a minimum, so this is where the number actually lives — whole-way
 * car-pool roads are the rarity.
 */
export const LANE_HOV3 = 32;

/**
 * Values that mean the lane is *for* this class, rather than merely open to
 * it.
 *
 * `bus:lanes=yes|yes` says buses may use both lanes, which is true of nearly
 * every road on earth; reading it as "reserved" marked whole carriageways as
 * somebody else's and left the driver with no lane at all. Only `designated`
 * — and `official`, its formal twin — reserve a lane. The Tunnel Albert
 * Bousser is the case in point: `bus:lanes:backward=yes|designated` with
 * `access:lanes:backward=yes|no` is one ordinary lane and one bus lane, and
 * the first cut called both of them buses'.
 */
const LANE_RESERVED_FOR = new Set(["designated", "official"]);

/**
 * The per-lane access tags, read into one reason byte per lane.
 *
 * OSM writes these exactly like `turn:lanes` — one entry per lane, left to
 * right, "|"-separated — and Québec alone tags 2,554 ways with `bus:lanes`.
 * Without them a reserved lane is drawn as an ordinary one, and an arrow
 * saying "get right" points a driver into a lane they will be ticketed for
 * using.
 *
 * A lane the driver may not use is a display fact rather than a routing one:
 * the road is still theirs and it is only this lane of it that is not.
 */
export function laneAccessList(tags, suffix, laneCount) {
  if (!laneCount) return null;
  const pick = (key) => {
    const specific = tags.get(`${key}:lanes${suffix}`);
    if (specific != null) return specific;
    // The unsuffixed form describes a one-way's only direction, or — on a
    // two-way — the lanes running the way's own way, same as the turns do.
    return suffix === ":backward" ? null : tags.get(`${key}:lanes`);
  };
  // `hov:minimum` is a property of the way rather than of one lane — the
  // per-lane spelling is not mapped anywhere — so every car-pool lane on
  // this way asks for the same carful.
  const reserved = [
    [pick("bus"), LANE_BUS],
    [pick("psv"), LANE_BUS],
    [pick("taxi"), LANE_TAXI],
    [pick("hov"), hovMinimum(tags) >= 3 ? LANE_HOV3 : LANE_HOV],
    [pick("bicycle"), LANE_BICYCLE]
  ];
  const denials = [pick("motor_vehicle"), pick("vehicle"), pick("access"), pick("motorcar")];
  if (!reserved.some(([spec]) => spec) && !denials.some(Boolean)) return null;

  const out = new Array(laneCount).fill(0);
  for (const [spec, bit] of reserved) {
    if (!spec) continue;
    const parts = spec.split("|");
    for (let i = 0; i < laneCount && i < parts.length; i++) {
      if (LANE_RESERVED_FOR.has(parts[i].trim())) out[i] |= bit;
    }
  }
  for (const spec of denials) {
    if (!spec) continue;
    const parts = spec.split("|");
    for (let i = 0; i < laneCount && i < parts.length; i++) {
      if (ACCESS_DENIED.has(parts[i].trim())) out[i] |= LANE_NO_MOTOR;
    }
  }
  return out.some(Boolean) ? out : null;
}

/**
 * Both directions' lane access, aligned to the lists [wayLanes] built.
 *
 * Alignment is the whole point: a reason byte at index 2 has to describe the
 * same lane as the arrow at index 2, or the panel labels the wrong one. So
 * the counts come from the turn lists rather than from the access tags,
 * which are often the shorter of the two.
 */
export function wayLaneAccess(tags, lanes) {
  return {
    forward: laneAccessList(tags, ":forward", lanes.forward.length),
    backward: laneAccessList(tags, ":backward", lanes.backward.length)
  };
}

/**
 * Per-direction lane lists for a way.
 *
 * Unsuffixed `turn:lanes` describes a one-way's only direction, or — on a
 * two-way road — the lanes in the way's own direction. The suffixed forms win
 * where present, which is how a two-way street with different approaches at
 * each end is tagged.
 */
export function wayLanes(tags, oneway) {
  const shared = tags.get("turn:lanes");
  const forwardSpec = tags.get("turn:lanes:forward") || shared || "";
  const backwardSpec = tags.get("turn:lanes:backward") || (oneway ? "" : shared) || "";
  const sharedCount = parseLaneCount(tags.get("lanes"));
  const forwardCount = parseLaneCount(tags.get("lanes:forward")) ||
    (oneway ? sharedCount : Math.floor(sharedCount / 2));
  const backwardCount = parseLaneCount(tags.get("lanes:backward")) ||
    (oneway ? 0 : Math.floor(sharedCount / 2));
  return {
    forward: laneListFrom(forwardSpec, forwardCount),
    backward: laneListFrom(backwardSpec, backwardCount)
  };
}

/**
 * What the overhead panel says above each lane, left to right.
 *
 * `destination:lanes=A 4;Montréal|A 4|Local` is the sign itself, lane by
 * lane, and it is the difference between "keep left" and "the left two lanes
 * are yours, the right one leaves". A driver on a five-lane approach with an
 * exit in ninety seconds is reading the panel, not the road — and the arrows
 * alone say which movements each lane allows without ever saying where any
 * of them goes.
 *
 * The number outranks the name where both exist, the way the panel is
 * painted: "40 Ouest" is above the lane and "Autoroute Félix-Leclerc" is
 * nowhere a driver can see. Returned as one `|`-joined string per direction
 * so a whole approach costs one table entry — a motorway repeats the same
 * panel across every way of its run.
 */
export function laneDestinations(tags, oneway) {
  const pick = (suffix) => {
    const refs = tags.get(`destination:ref:lanes${suffix}`);
    const names = tags.get(`destination:lanes${suffix}`);
    if (!refs && !names) return "";
    const refLanes = String(refs || "").split("|");
    const nameLanes = String(names || "").split("|");
    const count = Math.max(refLanes.length, nameLanes.length);
    const out = [];
    for (let i = 0; i < count; i++) {
      const ref = (refLanes[i] || "").trim();
      const name = (nameLanes[i] || "").trim();
      // One destination per lane: a panel that lists three places above one
      // lane is read for the first, and a chip has room for one answer.
      const first = (text) => text.split(";")[0].trim();
      out.push(ref ? first(ref) : first(name));
    }
    return out.some(Boolean) ? out.join("|") : "";
  };
  const shared = pick("");
  return {
    forward: pick(":forward") || shared,
    backward: pick(":backward") || (oneway ? "" : shared)
  };
}

/** The distinct lane panels edges point at, deduplicated. */
export function makeLaneSignTable() {
  const list = [];
  const byKey = new Map();
  return {
    list,
    idFor(text) {
      if (!text) return 0;
      const existing = byKey.get(text);
      if (existing) return existing;
      list.push(text);
      byKey.set(text, list.length);
      return list.length;
    }
  };
}

export function waySpeeds(tags, profile) {
  const base = profile.speeds[wayClass(tags)];
  let forward = base;
  let backward = base;
  if (profile.speedTags) {
    const shared = parseMaxspeed(tags.get("maxspeed"));
    const forwardTag = parseMaxspeed(tags.get("maxspeed:forward"));
    const backwardTag = parseMaxspeed(tags.get("maxspeed:backward"));
    forward = forwardTag || shared || base;
    backward = backwardTag || shared || base;
  }
  if (SLOW_SURFACES.has(tags.get("surface"))) {
    const cap = profile.name === "car" ? 40 : profile.name === "bike" ? 12 : Infinity;
    forward = Math.min(forward, cap);
    backward = Math.min(backward, cap);
  }
  if (VERY_SLOW_SMOOTHNESS.has(tags.get("smoothness"))) {
    const cap = profile.name === "car" ? 30 : profile.name === "bike" ? 8 : Infinity;
    forward = Math.min(forward, cap);
    backward = Math.min(backward, cap);
  }
  if (profile.adjustSpeed) {
    forward = profile.adjustSpeed(tags, forward);
    backward = profile.adjustSpeed(tags, backward);
  }
  if (destinationOnly(tags)) {
    forward = Math.max(3, forward * DESTINATION_ONLY_FACTOR);
    backward = Math.max(3, backward * DESTINATION_ONLY_FACTOR);
  }
  // The posted limit is what a sign says; the modelled speed above also folds
  // in surface, smoothness and the profile cap, so the two must not be
  // conflated. 0 means "no maxspeed tag" — a guess is not a limit.
  const postedShared = profile.speedTags ? parseMaxspeed(tags.get("maxspeed")) : 0;
  const postedForward = profile.speedTags
    ? (parseMaxspeed(tags.get("maxspeed:forward")) || postedShared) : 0;
  const postedBackward = profile.speedTags
    ? (parseMaxspeed(tags.get("maxspeed:backward")) || postedShared) : 0;

  // A limit that only applies at certain times travels beside the posted one
  // rather than replacing it: the driver needs to be shown whichever is in
  // force, and the router needs to cost the road differently at those hours.
  const conditional = profile.speedTags
    ? parseConditionalMaxspeed(tags.get("maxspeed:conditional"))
    : null;

  return {
    forward: Math.min(forward, profile.maxSpeedKmh),
    backward: Math.min(backward, profile.maxSpeedKmh),
    postedForward: Math.min(postedForward || 0, 255),
    postedBackward: Math.min(postedBackward || 0, 255),
    // Only a limit that actually drops is worth carrying. A conditional that
    // matches or exceeds the posted one changes nothing and would cost a rule
    // slot and a byte per edge to say so.
    conditional: conditional && conditional.speedKmh < (postedShared || Infinity)
      ? conditional
      : null
  };
}

/**
 * What the signs over a road actually say.
 *
 * A motorway's name is the one thing never written on a sign. Nobody in
 * Québec is looking for "Autoroute Félix-Leclerc"; they are looking for a
 * green panel reading **40 Ouest**, and above the slip road, **Sortie 32**.
 * Guiding by `name` meant the app announced a road by a label the driver
 * could not see, and announced the exit — the one instruction on a motorway
 * that has to be right — as a nameless ramp.
 *
 * All four fields come straight off the map:
 *
 * - `ref` is the road's own number, on 12,605 of Québec's 12,866 motorway and
 *   trunk ways. "40".
 * - `exit` is the exit number, from `junction:ref` on the slip road or from
 *   the `motorway_junction` node it leaves. "32", "89-N".
 * - `destRef` is what the slip road leads to, as a number *with its cardinal*
 *   — which is where "40 Ouest" actually comes from, since direction is
 *   tagged on only 41 route relations in the whole province and is useless.
 *   "20 Est;30".
 * - `dest` is the place the sign names. "Montréal;Québec".
 *
 * Semicolons are OSM's list separator and are kept verbatim: which of the
 * listed destinations to lead with is a presentation decision, and the client
 * is the only thing that knows how much room the banner has.
 */
/**
 * A physical dimension off a sign, in centimetres.
 *
 * OSM writes these however the sign does. Metres are the default and the
 * common case ("3.5", "3.5 m"), but imperial countries post feet and inches
 * and mappers copy them down as they appear: `12'6"`, `12 ft`, `12'`. A
 * router that understands only the first of those sends a van under every
 * bridge in North America, because an unparsed limit reads as no limit.
 *
 * Returns 0 for anything unusable — including `none`, `default`, and the
 * relative values like `below_default` that say nothing measurable.
 */
export function parseLengthCm(value) {
  if (value == null) return 0;
  const text = String(value).trim().toLowerCase().replace(",", ".");
  if (!text || text === "none" || text === "default" || text === "unsigned") return 0;

  // Feet and inches: 12'6", 12'6, 12', 12 ft, 12ft 6in.
  const feetInches = text.match(/^(\d+(?:\.\d+)?)\s*(?:'|ft|feet)\s*(?:(\d+(?:\.\d+)?)\s*(?:"|in|inch(?:es)?)?)?$/);
  if (feetInches) {
    const feet = Number(feetInches[1]);
    const inches = feetInches[2] ? Number(feetInches[2]) : 0;
    return Math.round((feet * 12 + inches) * 2.54);
  }
  const metres = text.match(/^(\d+(?:\.\d+)?)\s*(m|metre|meter|metres|meters)?$/);
  if (metres) return Math.round(Number(metres[1]) * 100);
  return 0;
}

/**
 * A weight limit in kilograms.
 *
 * Tonnes are the default and what almost every sign in the world says;
 * kilograms, pounds and short tons turn up where the sign does.
 */
export function parseWeightKg(value) {
  if (value == null) return 0;
  const text = String(value).trim().toLowerCase().replace(",", ".");
  if (!text || text === "none" || text === "default" || text === "unsigned") return 0;
  const match = text.match(/^(\d+(?:\.\d+)?)\s*(t|to?nnes?|kg|lbs?|st)?$/);
  if (!match) return 0;
  const amount = Number(match[1]);
  switch (match[2]) {
    case "kg": return Math.round(amount);
    case "lb":
    case "lbs": return Math.round(amount * 0.45359237);
    case "st": return Math.round(amount * 907.18474);
    default: return Math.round(amount * 1000);
  }
}

/**
 * What a way physically will not admit, or null when it admits everything.
 *
 * The `:physical` and `:signed` variants matter and are read first where
 * they exist: `maxheight:physical` is the height of the arch itself, which
 * is the number that decides whether a vehicle fits, while a bare
 * `maxheight` may be a legal posting with clearance to spare. Where a way
 * carries both, the physical one is the one that stops you.
 */
export function wayLimits(tags) {
  const heightCm = parseLengthCm(tags.get("maxheight:physical")) || parseLengthCm(tags.get("maxheight"));
  const widthCm = parseLengthCm(tags.get("maxwidth:physical")) || parseLengthCm(tags.get("maxwidth"));
  const lengthCm = parseLengthCm(tags.get("maxlength"));
  // `maxweight:hgv` stands in when there is no plain limit. Reading it as a
  // general weight limit is exact rather than merely cautious: anything heavy
  // enough to be stopped by an HGV weight limit is, by that fact, an HGV.
  // The same trick does not work for height — a tall light van is not an HGV
  // and an `maxheight:hgv` sign does not apply to it — so only weight folds.
  const weightKg = parseWeightKg(tags.get("maxweight:signed")) ||
    parseWeightKg(tags.get("maxweight")) ||
    parseWeightKg(tags.get("maxweight:hgv")) ||
    parseWeightKg(tags.get("maxweightrating:hgv"));
  if (!heightCm && !widthCm && !lengthCm && !weightKg) return null;
  return { heightCm, weightKg, widthCm, lengthCm };
}

/**
 * The distinct limit sets edges point at, deduplicated.
 *
 * A province posts the same handful over and over — 3.5 t, 4.0 m, 2.6 m —
 * so they are written to the root once and referenced by a one-byte index
 * on the edges that carry them. Index 0 is "nothing posted", which is the
 * overwhelming majority.
 */
/**
 * The distinct windows conditional turn bans are in force during.
 *
 * Shaped exactly like the conditional speed table and for the same reason: a
 * city posts the same handful of peak windows over and over, so they are
 * written once and referenced by index. Id 0 is "never shut".
 */
export function makeBanTable() {
  const list = [];
  const byKey = new Map();
  return {
    list,
    idFor(window) {
      if (!window) return 0;
      const key = `${window.days}/${window.startMinute}/${window.endMinute}/${window.monthStart}/${window.monthEnd}`;
      const existing = byKey.get(key);
      if (existing) return existing;
      list.push(window);
      byKey.set(key, list.length);
      return list.length;
    }
  };
}

/**
 * The distinct prices charged along a route, deduplicated.
 *
 * A crossing costs what it costs however many ways describe it, so the price
 * is written once and referenced by index — and the route sums it once per
 * stretch rather than once per way, because you pay a bridge to cross it,
 * not per hundred metres of it.
 */
export function makeChargeTable() {
  const list = [];
  const byKey = new Map();
  return {
    list,
    idFor(charge) {
      if (!charge) return 0;
      const key = `${charge.cents}/${charge.currency}`;
      const existing = byKey.get(key);
      if (existing) return existing;
      list.push(charge);
      byKey.set(key, list.length);
      return list.length;
    }
  };
}

export function makeLimitTable() {
  const list = [];
  const byKey = new Map();
  return {
    list,
    idFor(limits) {
      if (!limits) return 0;
      const key = `${limits.heightCm}/${limits.weightKg}/${limits.widthCm}/${limits.lengthCm}`;
      const existing = byKey.get(key);
      if (existing) return existing;
      list.push(limits);
      const id = list.length;
      byKey.set(key, id);
      return id;
    }
  };
}

export function makeSignTable() {
  const signs = [];
  const byKey = new Map();
  return {
    signs,
    /** 0 means "no sign data"; anything else is a 1-based entry. */
    idFor(sign) {
      if (!sign || (!sign.ref && !sign.exit && !sign.destRef && !sign.dest)) return 0;
      const key = `${sign.ref}\u0000${sign.exit}\u0000${sign.destRef}\u0000${sign.dest}` +
        `\u0000${sign.network || ""}`;
      const existing = byKey.get(key);
      if (existing != null) return existing;
      signs.push(sign);
      const id = signs.length;
      byKey.set(key, id);
      return id;
    }
  };
}

/** Trim, collapse whitespace, and drop the empty string. */
function signText(value) {
  return String(value ?? "").replace(/\s+/g, " ").trim();
}

/**
 * The signs for a way, per direction.
 *
 * Directional suffixes win where present: a two-way road signed differently
 * at each end is tagged `destination:ref:forward` and `:backward`, and taking
 * the unsuffixed value for both would point half its traffic at the wrong
 * city. Slip roads are one-way, so in practice the plain form covers nearly
 * all of it.
 */
/**
 * The road numbers a way carries, from its own tag and from the route
 * relations it belongs to.
 *
 * A way's `ref` is where North America puts the number, and much of the rest
 * of the world does not: an autoroute in France carries `A 4` on the way and
 * `E 50` only on a `type=route` relation, and the panel overhead says both.
 * Guidance reading the way alone announces half of what the driver can see,
 * and on a European motorway the half it drops is often the one they are
 * following.
 *
 * The way's own number leads — it is the more local and the more specific —
 * and relation numbers follow in the order the sign stacks them.
 */
export function mergeRefs(own, relationRefs) {
  const seen = new Set();
  const out = [];
  for (const value of [own, ...(relationRefs || [])]) {
    for (const part of String(value || "").split(";")) {
      const ref = part.trim();
      // The same number, spelled two ways. A Luxembourg motorway carries
      // `ref=A 1` on the way and `ref=A1` on its route relation, and reading
      // those as two roads put "A 1;A1;E44" on the sign — the driver's own
      // number, said twice, followed by the one they were looking for.
      const key = refKey(ref);
      if (!ref || seen.has(key)) continue;
      // A bare number repeating one already listed with its letter is the
      // same road again: a way tagged `N 4` collected by a relation whose
      // own ref is `4`. Dropping the letter is not another road, and
      // "N 4;4" on a sign is the driver's own number said twice.
      if (!/[A-Za-z]/.test(ref) && digitsOf(ref) && seen.has(`#${digitsOf(ref)}`)) continue;
      seen.add(key);
      if (digitsOf(ref)) seen.add(`#${digitsOf(ref)}`);
      out.push(ref);
    }
  }
  return out.join(";");
}

/**
 * A road number stripped to what makes it that number.
 *
 * Spacing and case are how the same road differs between a way and the
 * relation that collects it; nothing else about a ref is optional.
 */
export function refKey(ref) {
  return String(ref || "").toUpperCase().replace(/[\s\u00a0-]+/g, "");
}

/** Just the digits, for matching a number against a shorter spelling of it. */
function digitsOf(ref) {
  return String(ref || "").replace(/\D+/g, "");
}

/**
 * Whether a ref's own letter and a network's road letter describe the same
 * kind of road.
 *
 * `A 4` against `LU:N-road` is Autoroute 4 being offered route 4's marker.
 * Where either side says nothing about its letter there is nothing to
 * contradict, and the match stands.
 */
function schemesAgree(ref, network) {
  const refLetter = (String(ref || "").match(/^([A-Za-z]+)/) || [])[1];
  const netLetter = (String(network || "").match(/[:^]([A-Za-z]+)-road$/) || [])[1];
  if (!refLetter || !netLetter) return true;
  return refLetter.toUpperCase() === netLetter.toUpperCase();
}

export function waySigns(tags, relationRefs) {
  const ref = signText(mergeRefs(tags.get("ref"), relationRefs));
  const exit = signText(tags.get("junction:ref"));
  const pick = (key, suffix) =>
    signText(tags.get(`${key}:${suffix}`) || tags.get(key));
  const make = (suffix) => ({
    ref,
    exit,
    destRef: pick("destination:ref", suffix),
    // `destination:street` is the same promise in words when the target is a
    // street rather than a numbered route, and is what the sign says there.
    dest: pick("destination", suffix) || pick("destination:street", suffix)
  });
  return { forward: make("forward"), backward: make("backward") };
}

/**
 * Distinct conditional rules, so an edge can name one in a byte.
 *
 * A whole province shares a handful of these — Québec's 105 conditional ways
 * use four distinct windows between them — so a table plus a per-edge index
 * costs almost nothing next to repeating the window on every edge.
 */
export function makeConditionalTable() {
  const rules = [];
  const byKey = new Map();
  return {
    rules,
    /** 0 means "no conditional limit"; anything else is a 1-based rule. */
    idFor(conditional) {
      if (!conditional) return 0;
      const key = [
        conditional.speedKmh, conditional.days, conditional.startMinute,
        conditional.endMinute, conditional.monthStart, conditional.monthEnd
      ].join(":");
      let id = byKey.get(key);
      if (id == null) {
        rules.push(conditional);
        id = rules.length;
        byKey.set(key, id);
      }
      return id;
    }
  };
}

// Turn restriction relations applying to cars: plain `restriction` or
// `restriction:motorcar`, minus anything excepted for cars, minus
// conditional variants. Only single-via-node restrictions are supported
// (via-way restrictions are rare and skipped with a counter).
/**
 * A guide sign at a junction, as the relation that describes one.
 *
 * `type=destination_sign` is the panel itself: which approach it faces, which
 * turning it points at, and what is written on it. It is how an ordinary
 * junction gets the "toward Montréal" that a motorway ramp gets from its own
 * `destination` tag — and without it, the turn onto a numbered route is
 * announced by a street name the driver will not find on any panel.
 *
 * Shaped exactly like a turn restriction — from, via, to — because it is a
 * claim about the same thing: one movement through one junction.
 */
export function parseDestinationSign(tags, members) {
  const text = signText(tags.get("destination"));
  const ref = signText(tags.get("destination:ref") || tags.get("destination:symbol"));
  if (!text && !ref) return null;
  let fromWay = null;
  let toWay = null;
  let viaNode = null;
  for (const member of members) {
    if (member.role === "from" && member.type === "way") fromWay = member.ref;
    else if (member.role === "to" && member.type === "way") toWay = member.ref;
    // The junction the sign governs. `intersection` is the documented role;
    // `via` turns up too, written by mappers who know the restriction shape.
    else if ((member.role === "intersection" || member.role === "via") && member.type === "node") {
      viaNode = member.ref;
    }
  }
  if (fromWay == null || toWay == null || viaNode == null) return null;
  return { fromWay, toWay, viaNode, ref, text };
}

/**
 * Which lanes of the approach actually lead where the route goes.
 *
 * `type=connectivity` spells it out lane by lane —
 * `connectivity=1:1|2:2,3|3:` reads "lane 1 goes to lane 1, lane 2 to lanes 2
 * and 3, lane 3 goes nowhere on this turning". Guidance without it has to
 * infer usable lanes from the arrows painted on them, which is a good guess
 * and still a guess: an arrow says a movement is allowed from that lane, not
 * that *this* turning is reachable from it.
 *
 * Returned as a mask of approach lanes, bit 0 being the leftmost.
 */
export function parseConnectivity(tags, members) {
  const spec = tags.get("connectivity");
  if (!spec) return null;
  let fromWay = null;
  let toWay = null;
  let viaNode = null;
  for (const member of members) {
    if (member.role === "from" && member.type === "way") fromWay = member.ref;
    else if (member.role === "to" && member.type === "way") toWay = member.ref;
    else if (member.role === "via" && member.type === "node") viaNode = member.ref;
  }
  if (fromWay == null || toWay == null || viaNode == null) return null;

  let mask = 0;
  const lanes = String(spec).split("|");
  for (let i = 0; i < lanes.length && i < 32; i++) {
    const entry = lanes[i].trim();
    if (!entry) continue;
    // "2:3" — this lane leads somewhere on the far side. "3:" or "3:none"
    // is a lane that does not, and is exactly what a driver must not be in.
    const colon = entry.indexOf(":");
    const leadsTo = colon < 0 ? entry : entry.slice(colon + 1);
    const target = leadsTo.trim().toLowerCase();
    if (!target || target === "none") continue;
    mask |= 1 << i;
  }
  if (!mask) return null;
  return { fromWay, toWay, viaNode, mask };
}

function parseRestriction(tags, members) {
  // A restriction that only applies at certain hours is still a restriction.
  // Both spellings carry one, and the conditional form is read first because
  // a relation that has both is describing the same turn twice — once for
  // always and once for the window, and the window is the specific claim.
  const conditional = parseConditionalRestriction(
    tags.get("restriction:motorcar:conditional") || tags.get("restriction:conditional")
  );
  const kind = conditional?.kind || tags.get("restriction:motorcar") || tags.get("restriction");
  if (!kind) return null;
  if (!/^(no_|only_)/.test(kind)) return null;
  const except = tags.get("except");
  if (except && /\b(motorcar|motor_vehicle|vehicle)\b/.test(except)) return null;
  let fromWay = null;
  let toWay = null;
  let viaNode = null;
  const viaWays = [];
  for (const member of members) {
    if (member.role === "from" && member.type === "way") fromWay = member.ref;
    else if (member.role === "to" && member.type === "way") toWay = member.ref;
    else if (member.role === "via" && member.type === "node") viaNode = member.ref;
    else if (member.role === "via" && member.type === "way") viaWays.push(member.ref);
  }
  if (fromWay == null || toWay == null) return null;
  // The window this restriction is in force during, or null for always. It
  // rides along to the expansion, which keeps the turn and marks when it is
  // shut rather than deleting it outright.
  const window = conditional
    ? {
      days: conditional.days,
      startMinute: conditional.startMinute,
      endMinute: conditional.endMinute,
      monthStart: conditional.monthStart,
      monthEnd: conditional.monthEnd
    }
    : null;
  if (viaNode != null && !viaWays.length) {
    return { kind, fromWay, toWay, viaNode, window, only: kind.startsWith("only_") };
  }
  // Via ways (possibly several, possibly with a via node on the chain that
  // we can ignore): the union of their edges defines the restricted path.
  if (viaWays.length >= 1 && viaWays.length <= 4) {
    return { kind, fromWay, toWay, viaWays, window, only: kind.startsWith("only_") };
  }
  return viaWays.length ? { unsupportedVia: true } : null;
}

// oneway: 0 = both, 1 = forward only, -1 = backward only.
function parseOneway(tags) {
  const oneway = tags.get("oneway");
  if (oneway === "yes" || oneway === "1" || oneway === "true") return 1;
  if (oneway === "-1" || oneway === "reverse") return -1;
  if (oneway === "no" || oneway === "0" || oneway === "false") return 0;
  if (tags.get("junction") === "roundabout" || tags.get("junction") === "circular") return 1;
  const highway = tags.get("highway");
  if (highway === "motorway" || highway === "motorway_link") return 1;
  return 0;
}

/**
 * Roads a vehicle cannot or should not be sent down whatever its tags allow.
 *
 * A ford is a stream running over the road: passable in a pickup in summer
 * and not in a van in April, and OSM cannot tell you which day it is. A
 * seasonal or winter road is the same claim about the calendar. Neither is
 * something to route a courier over on the strength of a `highway=` value.
 *
 * A bus lane and an HOV lane are different: they are perfectly good roads
 * that this vehicle is not allowed on, and being sent down one is a fine.
 */
export function drivableSurface(tags) {
  if (tags.get("ford") && tags.get("ford") !== "no") return false;
  const seasonal = tags.get("seasonal");
  if (seasonal && seasonal !== "no") return false;
  if (tags.get("winter_road") === "yes") return false;
  // Reserved for buses, taxis or car-pools is no longer a reason to drop the
  // way: see [reservedFor], which flags it instead so the driver it *is* for
  // can be routed down it.
  return true;
}

function carAllowed(tags) {
  const highway = wayClass(tags);
  if (!highway || !(highway in CAR_SPEEDS)) return false;
  if (isBoarded(highway)) return ferryAllowed(tags, "car");
  if (tags.get("area") === "yes") return false;
  if (!drivableSurface(tags)) return false;
  const motorVehicle = tags.get("motor_vehicle") ?? tags.get("vehicle");
  if (motorVehicle != null) return !ACCESS_DENIED.has(motorVehicle);
  const access = tags.get("access");
  if (access != null && ACCESS_DENIED.has(access)) return false;
  return true;
}

class GrowFloat64 {
  constructor(capacity = 1 << 20) {
    this.data = new Float64Array(capacity);
    this.length = 0;
  }
  push(value) {
    if (this.length >= this.data.length) {
      const next = new Float64Array(this.data.length * 2);
      next.set(this.data);
      this.data = next;
    }
    this.data[this.length++] = value;
  }
  view() {
    return this.data.subarray(0, this.length);
  }
}

class GrowUint8 {
  constructor(capacity = 1 << 20) {
    this.data = new Uint8Array(capacity);
    this.length = 0;
  }
  ensure(extra) {
    if (this.length + extra <= this.data.length) return;
    let capacity = this.data.length * 2;
    while (capacity < this.length + extra) capacity *= 2;
    const next = new Uint8Array(capacity);
    next.set(this.data.subarray(0, this.length));
    this.data = next;
  }
  push(value) {
    this.ensure(1);
    this.data[this.length++] = value;
  }
  view() {
    return this.data.subarray(0, this.length);
  }
}

class GrowTyped {
  constructor(Type, capacity = 1 << 14) {
    this.Type = Type;
    this.data = new Type(capacity);
    this.length = 0;
  }
  push(value) {
    if (this.length >= this.data.length) {
      const next = new this.Type(this.data.length * 2);
      next.set(this.data);
      this.data = next;
    }
    this.data[this.length++] = value;
  }
  set(index, value) {
    this.data[index] = value;
  }
}

const EMPTY_LANES = new Uint8Array(0);

/**
 * Compact retained-way storage for the two-pass extractor.
 *
 * Country extracts keep millions of ways alive between the way and node
 * passes. One JavaScript object plus two lane arrays per way made that table
 * several GiB and forced long garbage-collection pauses. Numeric columns keep
 * the same values in typed arrays, while the uncommon lane data shares one
 * byte spool. A single mutable view is reused by each hot loop below.
 */
class PackedWayStore {
  constructor() {
    this.id = new GrowTyped(Float64Array);
    this.refStart = new GrowTyped(Uint32Array);
    this.refCount = new GrowTyped(Uint32Array);
    this.speedFwd = new GrowTyped(Float64Array);
    this.speedBwd = new GrowTyped(Float64Array);
    this.postedFwd = new GrowTyped(Uint8Array);
    this.postedBwd = new GrowTyped(Uint8Array);
    this.condRule = new GrowTyped(Uint32Array);
    this.oneway = new GrowTyped(Int8Array);
    this.nameId = new GrowTyped(Uint32Array);
    this.signFwd = new GrowTyped(Uint32Array);
    this.signBwd = new GrowTyped(Uint32Array);
    // What the way physically will not admit, as an index into the limit
    // table. A column this store did not have was a field the way silently
    // lost on the way in: the tags were read and the table was built, and
    // every edge came out pointing at nothing.
    this.limitId = new GrowTyped(Uint32Array);
    // Costs money, and how much. Columns rather than fields, because a field
    // this store does not know about is a field the way loses on the way in.
    this.tolled = new GrowTyped(Uint8Array);
    this.noHazmat = new GrowTyped(Uint8Array);
    this.noHgv = new GrowTyped(Uint8Array);
    this.reserved = new GrowTyped(Uint8Array);
    this.chargeId = new GrowTyped(Uint32Array);
    this.laneSignFwd = new GrowTyped(Uint32Array);
    this.laneSignBwd = new GrowTyped(Uint32Array);
    this.flags = new GrowTyped(Uint8Array);
    this.ferryDurationSeconds = new GrowTyped(Float64Array);
    this.ferryWaitSeconds = new GrowTyped(Float64Array);
    this.classCode = new GrowTyped(Uint8Array);
    this.laneStartFwd = new GrowTyped(Uint32Array);
    this.laneCountFwd = new GrowTyped(Uint8Array);
    this.laneStartBwd = new GrowTyped(Uint32Array);
    this.laneCountBwd = new GrowTyped(Uint8Array);
    this.laneBytes = new GrowUint8(1 << 14);
    // One reason byte per lane, at the same offsets as the movements above:
    // sharing the starts and counts is what guarantees the arrow and the
    // label always describe the same lane.
    this.laneAccessBytes = new GrowUint8(1 << 14);
  }

  get length() {
    return this.id.length;
  }

  push(way) {
    this.id.push(way.id);
    this.refStart.push(way.refStart);
    this.refCount.push(way.refCount);
    this.speedFwd.push(way.speedFwd);
    this.speedBwd.push(way.speedBwd);
    this.postedFwd.push(way.postedFwd);
    this.postedBwd.push(way.postedBwd);
    this.condRule.push(way.condRule);
    this.oneway.push(way.oneway);
    this.nameId.push(way.nameId);
    this.signFwd.push(way.signFwd);
    this.signBwd.push(way.signBwd);
    this.limitId.push(way.limitId || 0);
    this.tolled.push(way.tolled ? 1 : 0);
    this.noHazmat.push(way.noHazmat ? 1 : 0);
    this.noHgv.push(way.noHgv ? 1 : 0);
    this.reserved.push(way.reserved || 0);
    this.chargeId.push(way.chargeId || 0);
    this.laneSignFwd.push(way.laneSigns?.forward || 0);
    this.laneSignBwd.push(way.laneSigns?.backward || 0);
    this.flags.push((way.roundabout ? 1 : 0) | (way.ferry ? 2 : 0));
    this.ferryDurationSeconds.push(way.ferryDurationSeconds);
    this.ferryWaitSeconds.push(way.ferryWaitSeconds);
    this.classCode.push(way.classCode);
    this.pushLanes(way.lanes.forward, way.laneAccess?.forward, this.laneStartFwd, this.laneCountFwd);
    this.pushLanes(way.lanes.backward, way.laneAccess?.backward, this.laneStartBwd, this.laneCountBwd);
  }

  pushLanes(lanes, access, starts, counts) {
    starts.push(this.laneBytes.length);
    counts.push(Math.min(lanes.length, 255));
    this.laneBytes.ensure(lanes.length);
    this.laneAccessBytes.ensure(lanes.length);
    for (let i = 0; i < lanes.length && i < 255; i++) {
      this.laneBytes.data[this.laneBytes.length++] = lanes[i] & 0xff;
      this.laneAccessBytes.data[this.laneAccessBytes.length++] = (access?.[i] || 0) & 0xff;
    }
  }

  read(index, target, includeLanes = true) {
    target.id = this.id.data[index];
    target.refStart = this.refStart.data[index];
    target.refCount = this.refCount.data[index];
    target.speedFwd = this.speedFwd.data[index];
    target.speedBwd = this.speedBwd.data[index];
    target.postedFwd = this.postedFwd.data[index];
    target.postedBwd = this.postedBwd.data[index];
    target.condRule = this.condRule.data[index];
    target.oneway = this.oneway.data[index];
    target.nameId = this.nameId.data[index];
    target.signFwd = this.signFwd.data[index];
    target.limitId = this.limitId.data[index];
    target.tolled = this.tolled.data[index] === 1;
    target.noHazmat = this.noHazmat.data[index] === 1;
    target.noHgv = this.noHgv.data[index] === 1;
    target.reserved = this.reserved.data[index];
    target.chargeId = this.chargeId.data[index];
    target.laneSignFwd = this.laneSignFwd.data[index];
    target.laneSignBwd = this.laneSignBwd.data[index];
    target.signBwd = this.signBwd.data[index];
    const flags = this.flags.data[index];
    target.roundabout = (flags & 1) !== 0;
    target.ferry = (flags & 2) !== 0;
    target.ferryDurationSeconds = this.ferryDurationSeconds.data[index];
    target.ferryWaitSeconds = this.ferryWaitSeconds.data[index];
    target.classCode = this.classCode.data[index];
    if (includeLanes) {
      if (!target.lanes) target.lanes = {};
      target.lanes.forward = this.lanesAt(index, this.laneStartFwd, this.laneCountFwd);
      target.lanes.backward = this.lanesAt(index, this.laneStartBwd, this.laneCountBwd);
      target.laneAccess = {
        forward: this.lanesAt(index, this.laneStartFwd, this.laneCountFwd, this.laneAccessBytes),
        backward: this.lanesAt(index, this.laneStartBwd, this.laneCountBwd, this.laneAccessBytes)
      };
    }
    return target;
  }

  lanesAt(index, starts, counts, store = this.laneBytes) {
    const count = counts.data[index];
    if (!count) return EMPTY_LANES;
    const start = starts.data[index];
    return store.data.subarray(start, start + count);
  }

  setSpeeds(index, forward, backward) {
    this.speedFwd.set(index, forward);
    this.speedBwd.set(index, backward);
  }
}

function pushVarint(out, value) {
  let n = Math.max(0, Math.floor(value));
  while (n >= 0x80) {
    out.push((n % 0x80) | 0x80);
    n = Math.floor(n / 0x80);
  }
  out.push(n);
}

function pushZigzag(out, value) {
  pushVarint(out, value < 0 ? -value * 2 - 1 : value * 2);
}

function sortedUnique(values) {
  const sorted = Float64Array.from(values);
  sorted.sort();
  let count = 0;
  for (let i = 0; i < sorted.length; i++) {
    if (i === 0 || sorted[i] !== sorted[i - 1]) sorted[count++] = sorted[i];
  }
  return sorted.subarray(0, count);
}

/**
 * Sort one reference spool once and derive both its unique values and the
 * values that occur more than once. Road extraction used to sort this very
 * large spool twice: once for used node ids and again to discover junctions.
 * On country-sized extracts that duplicated hundreds of MiB of memory and a
 * full O(n log n) typed-array sort.
 */
export function sortedUniqueAndDuplicates(values, duplicates) {
  const sorted = Float64Array.from(values);
  sorted.sort();
  let count = 0;
  let previous = 0;
  let repeated = false;
  for (let i = 0; i < sorted.length; i++) {
    const value = sorted[i];
    if (i === 0 || value !== previous) {
      sorted[count++] = value;
      previous = value;
      repeated = false;
    } else if (!repeated) {
      duplicates.push(value);
      repeated = true;
    }
  }
  return sorted.subarray(0, count);
}

function lowerBound(sorted, value) {
  let low = 0;
  let high = sorted.length;
  while (low < high) {
    const mid = (low + high) >>> 1;
    if (sorted[mid] < value) low = mid + 1;
    else high = mid;
  }
  return low;
}

/**
 * Finds ids in a mostly ascending stream with a single forward cursor.
 * Geofabrik node records are ordered, so the coordinate pass becomes O(n)
 * instead of doing a binary search for every PBF node. A lower-bound reset
 * preserves correctness if a source contains an out-of-order block.
 */
export function createMonotonicLookup(sorted) {
  let cursor = 0;
  let previous = -Infinity;
  return value => {
    if (value < previous) cursor = lowerBound(sorted, value);
    else while (cursor < sorted.length && sorted[cursor] < value) cursor++;
    previous = value;
    return cursor < sorted.length && sorted[cursor] === value ? cursor : -1;
  };
}

function binarySearch(sorted, value) {
  let low = 0;
  let high = sorted.length - 1;
  while (low <= high) {
    const mid = (low + high) >>> 1;
    const item = sorted[mid];
    if (item === value) return mid;
    if (item < value) low = mid + 1;
    else high = mid - 1;
  }
  return -1;
}

function appendFederationPortal(portals, nodeId, latE7, lonE7, portalRegions) {
  const lat = latE7 / 1e7;
  const lon = lonE7 / 1e7;
  for (const neighbor of portalRegions) {
    const bbox = neighbor.bbox;
    const inside = lat >= bbox[0] && lat <= bbox[2] && (bbox[1] <= bbox[3]
      ? lon >= bbox[1] && lon <= bbox[3]
      : lon >= bbox[1] || lon <= bbox[3]);
    if (inside) portals[neighbor.id].push(nodeId, latE7, lonE7);
  }
}

export function collectFederationPortals({ junctionIds, usedIds, graphNodeByUsed, found, latE7, lonE7, portalRegions }) {
  const portals = Object.fromEntries(portalRegions.map(neighbor => [neighbor.id, []]));
  for (let i = 0; i < junctionIds.length && portalRegions.length; i++) {
    const used = binarySearch(usedIds, junctionIds[i]);
    if (used < 0 || graphNodeByUsed[used] < 0 || !found[used]) continue;
    appendFederationPortal(portals, junctionIds[i], latE7[used], lonE7[used], portalRegions);
  }
  return portals;
}

export function extractRoadGraph(pbfPath, options = {}) {
  const log = options.log || (() => {});
  const profileName = options.profile || "car";
  const profile = PROFILES[profileName];
  if (!profile) throw new Error(`Unknown routing profile "${profileName}" (car, bike, foot).`);
  const classTable = Object.keys(profile.speeds);
  const classCodes = new Map(classTable.map((name, index) => [name, index]));
  const ferryClassCode = classCodes.get(FERRY_CLASS);
  const shuttleClassCode = classCodes.get(SHUTTLE_CLASS);
  const chargeTable = makeChargeTable();
  const laneSignTable = makeLaneSignTable();
  const junctionSigns = [];
  const laneConnections = [];
  // Way id -> the numbers its route relations carry. Relations come after
  // ways in a PBF, so these are collected during the same pass and stitched
  // onto the ways afterwards rather than being read too late to use.
  const routeRefs = new Map();
  // Turn restrictions target motor vehicles; bicycles are commonly excepted
  // and pedestrians always are, so only the car profile applies them.
  const useRestrictions = profile.name === "car";
  const portalRegions = (options.portalRegions || []).filter(region => (
    region && region.id && Array.isArray(region.bbox) && region.bbox.length === 4
  ));

  // Pass 1: allowed ways and turn-restriction relations. Refs go into one
  // shared spool; endpoints are pushed twice so the duplicate scan below
  // marks them as graph nodes for free.
  let refSpool = new GrowFloat64();
  let junctionSpool = new GrowFloat64();
  // Distinct conditional windows, shared across the whole extract. Declared
  // with the way spool because the way pass is what fills it.
  const conditionalTable = makeConditionalTable();
  // Distinct sign faces, shared across the extract. A province's motorways
  // repeat a few thousand between them, so a table plus a per-edge index is
  // the whole cost of carrying what the green panels say.
  const signTable = makeSignTable();
  const limitTable = makeLimitTable();
  let ways = new PackedWayStore();
  // One-way carriageways that share a painted centre left-turn lane with the
  // line facing them, collected for [linkCentreTurnLanes].
  const centreTurnWays = new Set();
  let ferryWays = 0;
  const names = [""];
  const nameIds = new Map([["", 0]]);
  const restrictions = [];
  let wayCount = 0;
  let viaWayRestrictions = 0;
  scanPbf(pbfPath, {
    onWay(id, refs, tags) {
      wayCount++;
      if (refs.length < 2 || !profile.allowed(tags)) return;
      const speeds = waySpeeds(tags, profile);
      const signs = waySigns(tags);
      const className = wayClass(tags);
      const isFerry = isBoarded(className);
      const oneway = isFerry ? 0 : profile.oneway(tags);
      if (isFerry) ferryWays++;
      const name = tags.get("name") || tags.get("ref") || "";
      let nameId = nameIds.get(name);
      if (nameId == null) {
        nameId = names.length;
        names.push(name);
        nameIds.set(name, nameId);
      }
      // Only a one-way line needs the crossing opened: where the way itself
      // runs both directions the left turn was never in question.
      if (oneway !== 0 && centreTurnLane(tags)) centreTurnWays.add(id);
      // Built once and shared, because the access bytes are indexed against
      // exactly these lists: recomputing them apart is how the arrow and the
      // label end up describing different lanes.
      const wayLaneList = wayLanes(tags, oneway);
      const refStart = refSpool.length;
      const seen = new Set();
      for (const ref of refs) {
        refSpool.push(ref);
        // A ref repeated inside one way (loops, self-intersections) must be a
        // graph node or the two passes through it would merge into one edge.
        if (seen.has(ref)) junctionSpool.push(ref);
        seen.add(ref);
      }
      junctionSpool.push(refs[0]);
      junctionSpool.push(refs[refs.length - 1]);
      ways.push({
        id,
        refStart,
        refCount: refs.length,
        speedFwd: speeds.forward,
        speedBwd: speeds.backward,
        postedFwd: speeds.postedForward,
        postedBwd: speeds.postedBackward,
        condRule: conditionalTable.idFor(speeds.conditional),
        oneway,
        lanes: wayLaneList,
        // Which of those lanes are somebody else's, and why.
        laneAccess: wayLaneAccess(tags, wayLaneList),
        // What the panel says above each of those lanes. Same shape as the
        // lane arrows and read at the same moment, because a driver deciding
        // which lane to be in is reading both at once.
        laneSigns: (() => {
          const signs = laneDestinations(tags, oneway);
          return {
            forward: laneSignTable.idFor(signs.forward),
            backward: laneSignTable.idFor(signs.backward)
          };
        })(),
        nameId,
        signFwd: signTable.idFor(signs.forward),
        signBwd: signTable.idFor(signs.backward),
        // What this way physically will not admit. Both directions of one
        // road share it: an arch is the same height whichever way you drive
        // under it.
        limitId: limitTable.idFor(wayLimits(tags)),
        // Whether this costs money to use, and how much where OSM says.
        // `toll=yes` is mapped far more often than any price is, so the two
        // are separate facts: one decides routes, the other decides receipts.
        tolled: wayTolled(tags),
        noHazmat: (() => {
          const hazmat = tags.get("hazmat");
          return hazmat != null && hazmat !== "yes" && hazmat !== "designated";
        })(),
        // Who the whole road is for, when it is not for everybody.
        reserved: reservedFor(tags),
        noHgv: (() => {
          const hgv = tags.get("hgv") || tags.get("goods");
          // `hgv=destination` is not a refusal, and treating it as one is
          // backwards for the vehicle this router exists to send: a town
          // centre signed against through freight is signed *for* the lorry
          // delivering into it. A whole country's worth of last streets
          // would otherwise come back "no route this vehicle fits through",
          // which is both wrong and the one answer a courier cannot use.
          // Destination-only access is already handled as a slowdown that
          // keeps a street off through-routes while leaving it reachable,
          // and hgv follows the same rule rather than inventing a harsher one.
          if (hgv == null) return false;
          return !ACCESS_ALLOWS.has(hgv) && !ACCESS_DESTINATION_ONLY.has(hgv);
        })(),
        chargeId: chargeTable.idFor(
          parseCharge(tags.get("charge")) ||
          parseCharge(tags.get("toll:charge")) ||
          parseCharge(tags.get("fee"))
        ),
        // A roundabout is a maneuver, not a road, and the only thing that
        // says so is this tag. Without it the circle arrives as two or three
        // nameless forty-metre steps with turn angles that describe the
        // curve rather than the exit — which is exactly what a driver was
        // shown at the Boulevard Labelle giratoire.
        roundabout: tags.get("junction") === "roundabout" || tags.get("junction") === "circular",
        ferry: isFerry,
        ferryDurationSeconds: isFerry ? parseDuration(tags.get("duration")) : 0,
        ferryWaitSeconds: isFerry ? ferryWaitSeconds(tags) : 0,
        classCode: classCodes.get(className) ?? 0
      });
    },
    onRelation(id, members, tags) {
      const type = tags.get("type");
      // A sign and a lane connection are claims about one movement through
      // one junction, exactly like a restriction — and they need the same
      // via node promoted to a graph node to be attachable at all.
      // A road route: the number lives on the relation rather than on the
      // ways, which is how E-roads and most European numbering is mapped.
      if (type === "route" && (tags.get("route") === "road" || tags.get("route") === "hgv")) {
        const ref = signText(tags.get("ref"));
        if (!ref) return;
        // Which numbering scheme the number belongs to — `CA:QC:A`, `US:I`,
        // `e-road`. A bare "15" is an autoroute here, an Interstate three
        // hundred kilometres south and a *departmental* road in France, and
        // they are posted on three different shields. Nothing else in the
        // data says which, and without it a renderer can only guess.
        const network = signText(tags.get("network"));
        for (const member of members) {
          if (member.type !== "way") continue;
          let refs = routeRefs.get(member.ref);
          if (!refs) routeRefs.set(member.ref, (refs = []));
          if (refs.length < 4 && !refs.some(entry => entry.ref === ref)) {
            refs.push({ ref, network });
          }
        }
        return;
      }
      if (type === "destination_sign" || type === "connectivity") {
        const parsed = type === "destination_sign"
          ? parseDestinationSign(tags, members)
          : parseConnectivity(tags, members);
        if (!parsed) return;
        (type === "destination_sign" ? junctionSigns : laneConnections).push(parsed);
        junctionSpool.push(parsed.viaNode);
        junctionSpool.push(parsed.viaNode);
        return;
      }
      if (!useRestrictions || type !== "restriction") return;
      const parsed = parseRestriction(tags, members);
      if (!parsed) return;
      if (parsed.unsupportedVia) {
        viaWayRestrictions++;
        return;
      }
      restrictions.push(parsed);
      // A via node must become a graph node even when it is a plain
      // mid-way vertex of both ways. (Via-way endpoints are way ends, so
      // they are junction candidates already.)
      if (parsed.viaNode != null) {
        junctionSpool.push(parsed.viaNode);
        junctionSpool.push(parsed.viaNode);
      }
    }
  });
  const viaWayKept = restrictions.filter(restriction => restriction.viaWays != null).length;
  log(`ways: kept ${ways.length} of ${wayCount}`);

  // Route relations arrive after the ways that belong to them, so the numbers
  // they carry are stitched on here rather than read too late to use. Only
  // the sign face changes: the geometry, the speeds and the access decisions
  // were all correct without it.
  let numberedByRelation = 0;
  if (routeRefs.size) {
    const scratch = {};
    for (let i = 0; i < ways.length; i++) {
      const refs = routeRefs.get(ways.id.data[i]);
      if (!refs || !refs.length) continue;
      const merged = signText(mergeRefs(
        signTable.signs[ways.signFwd.data[i] - 1]?.ref,
        refs.map(entry => entry.ref)
      ));
      if (!merged) continue;
      // The shield belongs to the number that leads, which is the one a
      // renderer will draw. A way carrying both A 4 and E 50 is posted on two
      // shields and shows the first.
      const leading = merged.split(";")[0].trim();
      // Matched on the number rather than its spelling, or the way's own
      // "A 1" would never find the relation that calls it "A1" — which is
      // most of them, and is why the scheme was coming back empty.
      const network =
        refs.find(entry => refKey(entry.ref) === refKey(leading))?.network ||
        // Or the relation that spells the same number without its letter,
        // which is how Luxembourg's N-roads are collected: the way says
        // "N 4", the relation says "4".
        //
        // Only where the letters agree, though. Autoroute 4 and route 4 are
        // different roads that share a number, and matching on digits alone
        // drew the motorway on the N-road's marker — a wrong shield, which
        // is worse than the plain plate an unmatched road falls back to.
        refs.find(entry =>
          digitsOf(entry.ref) &&
          digitsOf(entry.ref) === digitsOf(leading) &&
          schemesAgree(leading, entry.network)
        )?.network ||
        "";
      for (const [column, id] of [["signFwd", ways.signFwd], ["signBwd", ways.signBwd]]) {
        const base = id.data[i] ? signTable.signs[id.data[i] - 1] : null;
        // A way with no sign at all still gains one: the number is the sign.
        scratch.ref = merged;
        scratch.exit = base?.exit || "";
        scratch.destRef = base?.destRef || "";
        scratch.dest = base?.dest || "";
        scratch.network = network;
        id.data[i] = signTable.idFor({ ...scratch });
      }
      numberedByRelation++;
    }
  }
  log(`route relations: ${routeRefs.size} way(s) numbered by relation, ${numberedByRelation} applied`);
  log(`restrictions: ${restrictions.length - viaWayKept} via-node + ${viaWayKept} via-way kept, ${viaWayRestrictions} oversized-via skipped`);

  // Junctions: any ref appearing more than once across the kept ways.
  let allRefs = refSpool.view();
  let usedIds = sortedUniqueAndDuplicates(allRefs, junctionSpool);
  let junctionIds = sortedUnique(junctionSpool.view());
  junctionSpool = null;
  log(`nodes: ${usedIds.length} used, ${junctionIds.length} junctions`);

  // Pass 2: coordinates and junction penalties for every used node.
  let latE7 = new Int32Array(usedIds.length);
  let lonE7 = new Int32Array(usedIds.length);
  let found = new Uint8Array(usedIds.length);
  let penaltyDs = new Uint16Array(usedIds.length);
  let kindCode = new Uint8Array(usedIds.length);
  // Which approach each tagged node governs, so a stop sign facing the side
  // street is not shown to the traffic that has right of way.
  let kindFaces = new Uint8Array(usedIds.length).fill(FACES_BOTH);
  // Nodes this profile simply cannot pass: a gate, a bollard, a stile. The
  // way is severed at them rather than costed, because no penalty is the
  // right price for a road that is not there.
  let blocked = new Uint8Array(usedIds.length);
  let blockedCount = 0;
  // Exit numbers, from the `motorway_junction` node the slip road leaves.
  // 1,326 of Québec's 1,798 such nodes carry one, which is the most reliable
  // source there is for the number on the green panel.
  const exitRefByNode = new Map();
  const findUsedNode = createMonotonicLookup(usedIds);
  scanPbf(pbfPath, {
    onNode(id, lat, lon, tags) {
      const index = findUsedNode(id);
      if (index < 0) return;
      latE7[index] = toE7(lat);
      lonE7[index] = toE7(lon);
      found[index] = 1;
      if (tags) {
        penaltyDs[index] = nodePenaltyDs(profile, tags);
        if (barrierBlocks(profile.name, tags)) {
          blocked[index] = 1;
          blockedCount++;
        }
        kindCode[index] = nodeKindCode(tags);
        kindFaces[index] = signFacing(tags);
        if (tags.get("highway") === "motorway_junction") {
          const exit = signText(tags.get("ref"));
          if (exit) exitRefByNode.set(index, exit);
        }
      }
    }
  });
  log(`exits: ${exitRefByNode.size} numbered motorway junctions`);
  log(`barriers: ${blockedCount} nodes impassable to ${profile.name}`);

  // Penalties are charged per direction of travel, because a sign is charged
  // to the driver who faces it. The two runs write into separate arrays; the
  // shared `penaltyDs` is only the source they are copied from.
  let penaltyFwd = Uint16Array.from(penaltyDs);
  let penaltyBwd = Uint16Array.from(penaltyDs);
  chargeEachIntersectionOnce(usedIds, latE7, lonE7, penaltyFwd, kindCode, log, kindFaces, FACES_FORWARD);
  chargeEachIntersectionOnce(usedIds, latE7, lonE7, penaltyBwd, kindCode, log, kindFaces, FACES_BACKWARD);

  /** The kind a driver travelling [facing] actually meets at this node. */
  const kindFor = (index, facing) =>
    (kindFaces[index] & facing) !== 0 ? kindCode[index] : 0;

  // Graph nodes are junctions with a known coordinate, numbered by sorted id.
  // Keep the mapping in a dense typed column keyed by used-node index. The old
  // Map held more than ten million boxed keys for Brazil and was queried once
  // per retained ref during edge construction.
  let graphNodeByUsed = new Int32Array(usedIds.length).fill(-1);
  let nodeLat = [];
  let nodeLon = [];
  const portals = Object.fromEntries(portalRegions.map(neighbor => [neighbor.id, []]));
  for (let i = 0; i < junctionIds.length; i++) {
    const used = binarySearch(usedIds, junctionIds[i]);
    if (used < 0 || !found[used]) continue;
    graphNodeByUsed[used] = nodeLat.length;
    nodeLat.push(latE7[used]);
    nodeLon.push(lonE7[used]);
    appendFederationPortal(portals, junctionIds[i], latE7[used], lonE7[used], portalRegions);
  }
  // Inter-region routing does not guess that two nearby roads connect. Keep
  // the original OSM junction ids that fall inside a neighboring extract's
  // coverage bbox; the two independently built sidecars are intersected at
  // query time, so only an id present in both graphs becomes a zero-cost
  // handoff. Bboxes are merely a compact candidate filter.
  if (portalRegions.length) {
    const candidates = Object.values(portals).reduce((sum, values) => sum + values.length / 3, 0);
    log(`federation: ${candidates.toLocaleString()} shared-id candidates for ${portalRegions.length} neighboring bbox(es)`);
  }
  // Restriction expansion only needs via nodes, not every graph junction.
  // Resolve those few ids now so the large used-id and graph-node columns can
  // be released before turn expansion reaches its own memory peak.
  const nodeIndex = new Map();
  // Restrictions, signs and lane connections all name a junction the same
  // way, and all three need it resolved here — this loop used to know about
  // restrictions only, which is why a sign relation parsed cleanly and then
  // attached to nothing at all.
  const resolveVia = (viaNode) => {
    if (viaNode == null || nodeIndex.has(viaNode)) return;
    const used = binarySearch(usedIds, viaNode);
    if (used < 0) return;
    const graphNode = graphNodeByUsed[used];
    if (graphNode >= 0) nodeIndex.set(viaNode, graphNode);
  };
  for (const restriction of restrictions) resolveVia(restriction.viaNode);
  for (const sign of junctionSigns) resolveVia(sign.viaNode);
  for (const link of laneConnections) resolveVia(link.viaNode);

  // Resolve each retained OSM node id exactly once. From here on the shared
  // spool contains used-node indexes, so both ferry costing and edge creation
  // use direct typed-array access instead of repeated binary searches.
  for (let i = 0; i < allRefs.length; i++) {
    allRefs[i] = binarySearch(usedIds, allRefs[i]);
  }

  // Ferry crossings are priced from their own `duration` rather than from a
  // class speed: the whole point of the tag is that a boat's pace is not a
  // road's. Done here because it needs the coordinates pass 2 just landed.
  let ferryTimed = 0;
  const ferryWay = {};
  for (let wayIndex = 0; wayIndex < ways.length; wayIndex++) {
    const way = ways.read(wayIndex, ferryWay, false);
    if (!way.ferry || way.ferryDurationSeconds <= 0) continue;
    let meters = 0;
    let previous = -1;
    for (let i = 0; i < way.refCount; i++) {
      const used = allRefs[way.refStart + i];
      if (used < 0 || !found[used]) continue;
      if (previous >= 0) {
        meters += haversineMetersE7(latE7[previous], lonE7[previous], latE7[used], lonE7[used]);
      }
      previous = used;
    }
    if (meters <= 0) continue;
    const kmh = Math.max(1, (meters / 1000) / (way.ferryDurationSeconds / 3600));
    ways.setSpeeds(wayIndex, kmh, kmh);
    ferryTimed++;
  }
  if (ferryWays) log(`ferries: ${ferryWays} crossings, ${ferryTimed} timed from duration`);

  // Split ways into directed edges at junction nodes.
  let edgeFrom = [];
  let edgeTo = [];
  let edgeWeightDs = [];
  let edgeDistDm = [];
  let edgeName = [];
  let edgeWay = [];
  let edgeClass = [];
  let edgeJunction = [];
  let edgeSpeed = [];
  let edgeCond = [];
  let geomOffsets = [0];
  let geomBytes = new GrowUint8();
  const geomScratch = [];
  // Lane movements, jagged the same way the geometry is: most edges carry
  // none, so an empty list costs the single byte that says so.
  let laneOffsets = [0];
  let laneBytes = new GrowUint8();
  // What the signs say, as an index into the extract's sign table, and the
  // per-edge flag byte (bit 0: this edge is inside a roundabout).
  let edgeSign = [];
  let edgeLimit = [];
  let edgeCharge = [];
  let edgeLaneSign = [];
  let reservedLaneEdges = 0;
  let edgeFlags = [];
  const emitEdge = ({
    from, to, weightDs, distDm, nameId, wayId, classCode, junctionKind,
    postedKmh, condRule, lanes, laneAccess, points, reversed, signId, limitId, chargeId,
    laneSignId, flags
  }) => {
    edgeFrom.push(from);
    edgeTo.push(to);
    edgeWeightDs.push(weightDs);
    edgeDistDm.push(distDm);
    edgeName.push(nameId);
    edgeWay.push(wayId);
    edgeClass.push(classCode);
    edgeJunction.push(junctionKind);
    edgeSpeed.push(postedKmh || 0);
    edgeCond.push(condRule || 0);
    edgeSign.push(signId || 0);
    edgeLimit.push(limitId || 0);
    edgeCharge.push(chargeId || 0);
    edgeLaneSign.push(laneSignId || 0);
    edgeFlags.push(flags || 0);
    geomScratch.length = 0;
    // Interior polyline points only, zigzag-delta E7 from the from-node.
    const interior = points.length - 2;
    pushVarint(geomScratch, Math.max(0, interior));
    let prevLat = reversed ? points[points.length - 1][0] : points[0][0];
    let prevLon = reversed ? points[points.length - 1][1] : points[0][1];
    for (let i = 1; i <= interior; i++) {
      const point = reversed ? points[points.length - 1 - i] : points[i];
      pushZigzag(geomScratch, point[0] - prevLat);
      pushZigzag(geomScratch, point[1] - prevLon);
      prevLat = point[0];
      prevLon = point[1];
    }
    geomBytes.ensure(geomScratch.length);
    for (const byte of geomScratch) geomBytes.data[geomBytes.length++] = byte;
    geomOffsets.push(geomBytes.length);
    // `[count, movement × count, reason × count]`. The reasons ride in the
    // same record as the arrows rather than in a column of their own,
    // because they are indexed by lane rather than by edge and the two must
    // stay aligned; a separate column is a second thing to drop, and this
    // pipeline has dropped a column silently three times.
    const laneList = lanes || [];
    const laneKept = Math.min(laneList.length, 255);
    laneBytes.ensure(laneKept * 2 + 1);
    laneBytes.data[laneBytes.length++] = laneKept;
    for (let i = 0; i < laneKept; i++) {
      laneBytes.data[laneBytes.length++] = laneList[i] & 0xff;
    }
    for (let i = 0; i < laneKept; i++) {
      laneBytes.data[laneBytes.length++] = (laneAccess?.[i] || 0) & 0xff;
    }
    if (laneAccess?.some(Boolean)) reservedLaneEdges++;
    laneOffsets.push(laneBytes.length);
  };

  const segment = [];
  // Everything a slip road is signed with rides on its edges. The exit
  // number is the exception: it belongs to the `motorway_junction` node the
  // ramp leaves, so it is resolved per edge rather than per way, and only
  // for the first edge of the ramp — the number is announced once.
  const signWithExit = (baseId, exit) => {
    if (!exit) return baseId;
    const base = baseId ? signTable.signs[baseId - 1] : null;
    if (base && base.exit === exit) return baseId;
    return signTable.idFor({
      ref: base ? base.ref : "",
      exit,
      destRef: base ? base.destRef : "",
      dest: base ? base.dest : ""
    });
  };
  const isLinkClass = (classCode) => classTable[classCode]?.endsWith("_link") === true;
  const retainedWay = {};
  for (let wayIndex = 0; wayIndex < ways.length; wayIndex++) {
    const way = ways.read(wayIndex, retainedWay);
    segment.length = 0;
    let fromNode = -1;
    let fromUsed = -1;
    let fromPenalty = 0;
    let fromKind = 0;
    let lengthMeters = 0;
    // Signals and stops usually sit on interior way nodes, not on the
    // shared junction node; carry their delay and kind onto the edge.
    // Accumulated per direction, because a sign facing one way is not a
    // delay for traffic going the other.
    let segPenaltyFwd = 0;
    let segPenaltyBwd = 0;
    let segKindFwd = 0;
    let segKindFwdIndex = 0;
    let segKindBwd = 0;
    let segKindBwdIndex = 0;
    const flags = (way.roundabout ? EDGE_FLAG_ROUNDABOUT : 0) |
      (way.tolled ? EDGE_FLAG_TOLL : 0) |
      // A shuttle train is boarded, waited for and paid for exactly like a
      // boat, so "avoid ferries" means it too — a driver who will not put
      // their van on a boat will not put it on a train through a mountain.
      ((way.classCode === ferryClassCode || way.classCode === shuttleClassCode) ? EDGE_FLAG_FERRY : 0) |
      (way.noHazmat ? EDGE_FLAG_NO_HAZMAT : 0) |
      (way.noHgv ? EDGE_FLAG_NO_HGV : 0) |
      (way.reserved || 0);
    // A boat leaves when it leaves. The wait is charged once to the crossing
    // rather than spread along it, so a longer ferry is not made to look
    // like a worse one.
    const ferryWaitDs = way.ferry ? Math.round(way.ferryWaitSeconds * 10) : 0;
    const resetSegment = () => {
      segPenaltyFwd = 0;
      segPenaltyBwd = 0;
      segKindFwd = 0;
      segKindBwd = 0;
    };
    for (let i = 0; i < way.refCount; i++) {
      const used = allRefs[way.refStart + i];
      if (used < 0 || !found[used] || blocked[used]) {
        // Missing node (clipped extract), or one this profile cannot pass —
        // a gate, a bollard, a stile. Either way there is no road through
        // here, so the segment breaks and the two sides never join.
        segment.length = 0;
        fromNode = -1;
        fromUsed = -1;
        lengthMeters = 0;
        resetSegment();
        continue;
      }
      const lat = latE7[used];
      const lon = lonE7[used];
      if (segment.length) {
        const prev = segment[segment.length - 1];
        lengthMeters += haversineMetersE7(prev[0], prev[1], lat, lon);
      }
      segment.push([lat, lon]);
      const graphNode = graphNodeByUsed[used];
      if (graphNode < 0) {
        segPenaltyFwd += penaltyFwd[used];
        segPenaltyBwd += penaltyBwd[used];
        const forwardKind = kindFor(used, FACES_FORWARD);
        if (JUNCTION_RANK[forwardKind] > JUNCTION_RANK[segKindFwd]) {
          segKindFwd = forwardKind;
          segKindFwdIndex = segment.length - 1;
        }
        const backwardKind = kindFor(used, FACES_BACKWARD);
        if (JUNCTION_RANK[backwardKind] > JUNCTION_RANK[segKindBwd]) {
          segKindBwd = backwardKind;
          segKindBwdIndex = segment.length - 1;
        }
        continue;
      }
      if (fromNode < 0) {
        fromNode = graphNode;
        fromUsed = used;
        fromPenalty = penaltyBwd[used];
        fromKind = kindFor(used, FACES_BACKWARD);
        segment.length = 0;
        segment.push([lat, lon]);
        lengthMeters = 0;
        resetSegment();
        continue;
      }
      if (graphNode === fromNode && lengthMeters < 0.01) continue;
      const distDm = Math.max(1, Math.round(lengthMeters * 10));
      const lastIndex = segment.length - 1;
      // Pick the most important junction on this edge per direction, with
      // its polyline point index packed alongside (kind + index * 8).
      const pick = (segKind, segIndex, endKind, endIndex) => {
        if (JUNCTION_RANK[segKind] > JUNCTION_RANK[endKind]) return { kind: segKind, index: segIndex };
        return endKind ? { kind: endKind, index: endIndex } : { kind: 0, index: 0 };
      };
      const link = isLinkClass(way.classCode);
      if (way.oneway >= 0) {
        const junction = pick(segKindFwd, segKindFwdIndex, kindFor(used, FACES_FORWARD), lastIndex);
        const weightDs = ferryWaitDs +
          Math.max(1, Math.round((lengthMeters / ((way.speedFwd * 1000) / 3600)) * 10) + penaltyFwd[used] + segPenaltyFwd);
        emitEdge({
          from: fromNode, to: graphNode, weightDs, distDm,
          nameId: way.nameId, wayId: way.id, classCode: way.classCode,
          junctionKind: junction.kind + junction.index * 8,
          postedKmh: way.postedFwd, condRule: way.condRule,
          lanes: way.lanes.forward, laneAccess: way.laneAccess?.forward,
          laneSignId: way.laneSignFwd, points: segment, reversed: false,
          signId: link ? signWithExit(way.signFwd, exitRefByNode.get(fromUsed)) : way.signFwd,
          limitId: way.limitId,
          chargeId: way.chargeId,
          flags
        });
      }
      if (way.oneway <= 0) {
        const junction = pick(segKindBwd, segKindBwdIndex, fromKind, lastIndex);
        // Reversed polyline: mirror the point index.
        const mirrored = junction.kind ? { kind: junction.kind, index: lastIndex - junction.index } : junction;
        const weightDs = ferryWaitDs +
          Math.max(1, Math.round((lengthMeters / ((way.speedBwd * 1000) / 3600)) * 10) + fromPenalty + segPenaltyBwd);
        emitEdge({
          from: graphNode, to: fromNode, weightDs, distDm,
          nameId: way.nameId, wayId: way.id, classCode: way.classCode,
          junctionKind: mirrored.kind + mirrored.index * 8,
          postedKmh: way.postedBwd, condRule: way.condRule,
          lanes: way.lanes.backward, laneAccess: way.laneAccess?.backward,
          laneSignId: way.laneSignBwd, points: segment, reversed: true,
          signId: link ? signWithExit(way.signBwd, exitRefByNode.get(used)) : way.signBwd,
          limitId: way.limitId,
          chargeId: way.chargeId,
          flags
        });
      }
      fromNode = graphNode;
      fromUsed = used;
      fromPenalty = penaltyBwd[used];
      fromKind = kindFor(used, FACES_BACKWARD);
      segment.length = 0;
      segment.push([lat, lon]);
      lengthMeters = 0;
      resetSegment();
    }
  }
  log(`edges: ${edgeFrom.length} directed`);
  log(`lane panels: ${edgeLaneSign.filter(Boolean).length} approaches signed, ${laneSignTable.list.length} distinct panels`);
  log(`lane access: ${reservedLaneEdges} approaches carry a lane that is not the driver's`);
  log(`hazmat: ${edgeFlags.filter(f => (f & EDGE_FLAG_NO_HAZMAT) !== 0).length} edges refuse dangerous goods`);
  log(`hgv: ${edgeFlags.filter(f => (f & EDGE_FLAG_NO_HGV) !== 0).length} edges refuse goods vehicles`);
  log(`reserved roads: ${edgeFlags.filter(f => (f & EDGE_FLAG_PSV_ONLY) !== 0).length} edges for public service vehicles, ${edgeFlags.filter(f => (f & EDGE_FLAG_HOV_ONLY) !== 0).length} for car-pools of two, ${edgeFlags.filter(f => (f & EDGE_FLAG_HOV3_ONLY) !== 0).length} of three`);
  log(`tolls: ${edgeFlags.filter(f => (f & EDGE_FLAG_TOLL) !== 0).length} tolled edges, ${edgeFlags.filter(f => (f & EDGE_FLAG_FERRY) !== 0).length} ferry edges, ${edgeCharge.filter(Boolean).length} priced, ${chargeTable.list.length} distinct prices`);
  log(`limits: ${limitTable.list.length} distinct, ${edgeLimit.reduce ? edgeLimit.filter(Boolean).length : 0} directed edges posted`);

  // Everything below works on collapsed graph nodes and edges. Drop the raw
  // country-scale extraction columns before turn expansion allocates its
  // edge-based graph; otherwise both representations overlap at peak RSS.
  if (retainedWay.lanes) {
    retainedWay.lanes.forward = EMPTY_LANES;
    retainedWay.lanes.backward = EMPTY_LANES;
  }
  ways = null;
  allRefs = null;
  refSpool = null;
  usedIds = null;
  junctionIds = null;
  graphNodeByUsed = null;
  latE7 = null;
  lonE7 = null;
  found = null;
  penaltyDs = null;
  penaltyFwd = null;
  penaltyBwd = null;
  kindCode = null;
  kindFaces = null;
  exitRefByNode.clear();

  // A painted centre turn lane is a left turn the road allows and the two
  // drawn lines deny. Opened before turn handling, so restrictions and turn
  // costs are compiled over the crossings too and everything after this sees
  // plain edges.
  linkCentreTurnLanes({
    centreTurnWays, nodeLat, nodeLon,
    edgeFrom, edgeTo, edgeWeightDs, edgeDistDm, edgeName, edgeWay, edgeClass,
    edgeJunction, edgeSpeed, edgeCond, edgeSign, edgeLimit, edgeCharge, edgeLaneSign, edgeFlags,
    geomOffsets, geomBytes, laneOffsets, laneBytes, log
  });

  // Turn handling. With turn costs enabled (car and bike by default), the
  // graph is fully junction-expanded into an edge-based graph: via-way
  // restrictions are compiled as chain copies first, then every junction
  // splits per approach with bearing-derived turn costs, and via-node
  // restrictions become exact per-approach filters inside the expansion.
  // Without turn costs, via-node restrictions fall back to targeted
  // via-node expansion.
  const useTurnCosts = Boolean(profile.turnCosts) && options.turnCosts !== false;
  const context = {
    restrictions: useTurnCosts ? restrictions.filter(restriction => restriction.viaWays != null) : restrictions,
    nodeIndex,
    nodeLat,
    nodeLon,
    edgeFrom,
    edgeTo,
    edgeWeightDs,
    edgeDistDm,
    edgeName,
    edgeWay,
    edgeClass,
    edgeJunction,
    edgeSpeed,
    edgeCond,
    edgeSign,
    edgeLimit,
    edgeCharge,
    edgeLaneSign,
    edgeFlags,
    geomOffsets,
    geomBytes,
    laneOffsets,
    laneBytes,
    log
  };
  applyTurnRestrictions(context);

  if (useTurnCosts) {
    // Restriction compilation may append edges, so compact only after it is
    // finished. Replacing each boxed source column one at a time keeps the
    // conversion peak bounded and leaves turn expansion with typed inputs.
    // India carries 42M base edges; retaining those as fourteen ordinary JS
    // arrays consumed several GiB of the heap before expansion even started.
    const compact = (key, Type) => {
      const value = Type.from(context[key].view ? context[key].view() : context[key]);
      context[key] = value;
      return value;
    };
    nodeLat = compact("nodeLat", Int32Array);
    nodeLon = compact("nodeLon", Int32Array);
    edgeFrom = compact("edgeFrom", Uint32Array);
    edgeTo = compact("edgeTo", Uint32Array);
    edgeWeightDs = compact("edgeWeightDs", Uint32Array);
    edgeDistDm = compact("edgeDistDm", Uint32Array);
    edgeName = compact("edgeName", Uint32Array);
    edgeWay = compact("edgeWay", Float64Array);
    edgeClass = compact("edgeClass", Uint8Array);
    edgeJunction = compact("edgeJunction", Uint8Array);
    edgeSpeed = compact("edgeSpeed", Uint8Array);
    edgeCond = compact("edgeCond", Uint8Array);
    edgeSign = compact("edgeSign", Uint32Array);
    edgeLimit = compact("edgeLimit", Uint32Array);
    edgeCharge = compact("edgeCharge", Uint32Array);
    edgeLaneSign = compact("edgeLaneSign", Uint32Array);
    edgeFlags = compact("edgeFlags", Uint8Array);
    geomOffsets = compact("geomOffsets", Uint32Array);
    laneOffsets = compact("laneOffsets", Uint32Array);

    const expanded = expandTurnCosts(
      { ...context, restrictions, junctionSigns, laneConnections, signTable },
      profile.turnCosts
    );
    // The expansion already returns exact typed columns. `take` remains
    // tolerant of growable/array callers used by focused tests and custom
    // integrations, but never copies the production result.
    const prepared = {
      nodeLat: expanded.nodeLat,
      nodeLon: expanded.nodeLon,
      names,
      profile: profile.name,
      classes: classTable,
      condRules: conditionalTable.rules,
      signs: signTable.signs,
      limits: limitTable.list,
      charges: chargeTable.list,
      laneSigns: laneSignTable.list,
      banRules: expanded.banRules || [],
      portals
    };
    const take = (key, Type, fallback = []) => {
      const source = expanded[key] || fallback;
      prepared[key] = source instanceof Type
        ? source
        : Type.from(source.view ? source.view() : source);
      expanded[key] = null;
    };
    take("edgeFrom", Uint32Array);
    take("edgeTo", Uint32Array);
    take("edgeWeightDs", Uint32Array);
    take("edgeDistDm", Uint32Array);
    take("edgeName", Uint32Array);
    take("edgeClass", Uint8Array);
    take("edgeJunction", Uint8Array);
    take("edgeSpeed", Uint8Array);
    take("edgeCond", Uint8Array);
    take("edgeSign", Uint32Array);
    take("edgeLimit", Uint32Array);
    take("edgeCharge", Uint32Array);
    take("edgeLaneSign", Uint32Array);
    take("edgeLaneMask", Uint32Array);
    take("edgeBan", Uint32Array);
    take("edgeFlags", Uint8Array);
    take("geomOffsets", Uint32Array);
    take("geomBytes", Uint8Array);
    take("laneOffsets", Uint32Array);
    take("laneBytes", Uint8Array);

    // The expanded graph owns its own coordinates and edge payloads now. Drop
    // the pre-expansion graph before SCC allocates its two CSR traversals.
    for (const key of [
      "nodeLat", "nodeLon", "edgeFrom", "edgeTo", "edgeWeightDs", "edgeDistDm",
      "edgeName", "edgeWay", "edgeClass", "edgeJunction", "edgeSpeed", "edgeCond",
      "edgeSign", "edgeFlags", "geomOffsets", "geomBytes", "laneOffsets", "laneBytes"
    ]) context[key] = null;
    nodeLat = null;
    nodeLon = null;
    edgeFrom = null;
    edgeTo = null;
    edgeWeightDs = null;
    edgeDistDm = null;
    edgeName = null;
    edgeWay = null;
    edgeClass = null;
    edgeJunction = null;
    edgeSpeed = null;
    edgeCond = null;
    edgeSign = null;
    edgeFlags = null;
    geomOffsets = null;
    geomBytes = null;
    laneOffsets = null;
    laneBytes = null;
    return filterLargestScc(prepared, log);
  }

  return filterLargestScc({
    nodeLat: Int32Array.from(nodeLat),
    nodeLon: Int32Array.from(nodeLon),
    edgeFrom: Uint32Array.from(edgeFrom),
    edgeTo: Uint32Array.from(edgeTo),
    edgeWeightDs: Uint32Array.from(edgeWeightDs),
    edgeDistDm: Uint32Array.from(edgeDistDm),
    edgeName: Uint32Array.from(edgeName),
    edgeClass: Uint8Array.from(edgeClass),
    edgeJunction: Uint8Array.from(edgeJunction),
    edgeSpeed: Uint8Array.from(edgeSpeed),
    edgeCond: Uint8Array.from(edgeCond),
    edgeSign: Uint32Array.from(edgeSign),
    edgeLimit: Uint32Array.from(edgeLimit),
    edgeCharge: Uint32Array.from(edgeCharge),
    edgeLaneSign: Uint32Array.from(edgeLaneSign),
    edgeLaneMask: new Uint32Array(edgeLaneSign.length),
    edgeFlags: Uint8Array.from(edgeFlags),
    laneOffsets: Uint32Array.from(laneOffsets),
    laneBytes: Uint8Array.from(laneBytes.view()),
    geomOffsets: Uint32Array.from(geomOffsets),
    geomBytes: Uint8Array.from(geomBytes.view()),
    names,
    profile: profile.name,
    classes: classTable,
    condRules: conditionalTable.rules,
    signs: signTable.signs,
    limits: limitTable.list,
    charges: chargeTable.list,
    laneSigns: laneSignTable.list,
    portals
  }, log);
}

/**
 * How far apart the two lines of a centre-turn-lane road may sit and still be
 * one road. Boulevard Labelle's are 10.4 m — two lanes of centreline offset
 * plus the shared turn lane between them. Anything much wider is a boulevard
 * with a real median, whatever it claims in its lane tags, and crossing it
 * needs an intersection rather than a paint line.
 */
const CENTRE_CROSS_MIN_METERS = 3;
const CENTRE_CROSS_MAX_METERS = 22;

/**
 * What crossing the oncoming lanes costs, in deciseconds.
 *
 * A left turn out of the centre lane is a wait for a gap, and the wait is the
 * whole cost — the crossing itself is ten metres. Five seconds also keeps the
 * one movement this opens up that is not simply a left turn honest: two
 * crossings back to back are a u-turn across the centre lane, and at 2 x 50
 * plus two left-turn charges they stay dearer than the 150 the profile already
 * prices a u-turn at, so the router reaches for one no more often than before.
 */
const CENTRE_CROSS_PENALTY_DS = 50;

/** Speed a driver crosses at, km/h — a crawl, because it is a standing start. */
const CENTRE_CROSS_KMH = 20;

/** How far a crossing must land from either end of the edge it splits. */
const CENTRE_CROSS_MARGIN_METERS = 2;

/** Two carriageways face each other when their headings differ by 120° or more. */
const CENTRE_CROSS_OPPOSITE_DOT = -0.5;

/**
 * Opens the left turns a centre two-way left-turn lane actually allows.
 *
 * See [centreTurnLane]: such a road is one undivided street drawn as two
 * one-way lines, and read literally it has no way across. Where the far line
 * carries a junction with something else — a driveway, a side street, anything
 * a driver would turn into — this splits the near line opposite it and joins
 * the two with a short crossing edge in both directions. That is the movement
 * the paint on the road permits, and the only one: crossings are added
 * opposite somewhere to turn, never between two plain stretches of road.
 *
 * Runs before turn handling, so the crossings are ordinary edges by the time
 * restrictions and turn costs are compiled and everything downstream — cliques,
 * unpacking, guidance — sees nothing new. A crossing carries the carriageway's
 * own name, because a driver waiting in the centre lane has not left the
 * street yet; the maneuver is the turn out of it.
 *
 * Returns how many crossings were opened.
 */
export function linkCentreTurnLanes(context) {
  const {
    centreTurnWays, nodeLat, nodeLon,
    edgeFrom, edgeTo, edgeWeightDs, edgeDistDm, edgeName, edgeWay, edgeClass,
    edgeJunction, edgeSpeed, edgeCond, edgeSign, edgeLimit, edgeCharge, edgeLaneSign, edgeFlags,
    geomOffsets, geomBytes, laneOffsets, laneBytes, log
  } = context;
  const note = log || (() => {});
  if (!centreTurnWays || centreTurnWays.size === 0) return 0;

  const edgeCount = edgeFrom.length;
  const data = geomBytes.data;
  let cursor = 0;
  const readVarint = () => {
    let value = 0;
    let multiplier = 1;
    for (;;) {
      const byte = data[cursor++];
      value += (byte & 0x7f) * multiplier;
      if ((byte & 0x80) === 0) return value;
      multiplier *= 0x80;
    }
  };
  const readZigzag = () => {
    const raw = readVarint();
    return raw % 2 === 1 ? -(raw + 1) / 2 : raw / 2;
  };
  /** An edge's polyline, both endpoints included. */
  const pointsOf = (edge) => {
    cursor = geomOffsets[edge];
    const interior = readVarint();
    let lat = nodeLat[edgeFrom[edge]];
    let lon = nodeLon[edgeFrom[edge]];
    const points = [[lat, lon]];
    for (let i = 0; i < interior; i++) {
      lat += readZigzag();
      lon += readZigzag();
      points.push([lat, lon]);
    }
    points.push([nodeLat[edgeTo[edge]], nodeLon[edgeTo[edge]]]);
    return points;
  };

  // Only named centre-turn carriageways take part: the name is what pairs a
  // line with the one facing it, and an unnamed pair would match far too much.
  const centreEdges = [];
  for (let e = 0; e < edgeCount; e++) {
    if (edgeName[e] && centreTurnWays.has(edgeWay[e])) centreEdges.push(e);
  }
  if (!centreEdges.length) return 0;

  const METERS_PER_E7 = (EARTH_RADIUS_METERS * Math.PI) / 180 / 1e7;
  const CELL_E7 = 4000;
  const polylines = new Map();
  const alongs = new Map();
  const grid = new Map();
  const cellKey = (cx, cy) => `${cx},${cy}`;
  for (const e of centreEdges) {
    const points = pointsOf(e);
    polylines.set(e, points);
    const along = [0];
    for (let i = 0; i + 1 < points.length; i++) {
      along.push(along[i] + haversineMetersE7(points[i][0], points[i][1], points[i + 1][0], points[i + 1][1]));
      const loLat = Math.min(points[i][0], points[i + 1][0]);
      const hiLat = Math.max(points[i][0], points[i + 1][0]);
      const loLon = Math.min(points[i][1], points[i + 1][1]);
      const hiLon = Math.max(points[i][1], points[i + 1][1]);
      for (let cx = Math.floor(loLat / CELL_E7); cx <= Math.floor(hiLat / CELL_E7); cx++) {
        for (let cy = Math.floor(loLon / CELL_E7); cy <= Math.floor(hiLon / CELL_E7); cy++) {
          const key = cellKey(cx, cy);
          let bucket = grid.get(key);
          if (!bucket) grid.set(key, (bucket = []));
          bucket.push(e, i);
        }
      }
    }
    alongs.set(e, along);
  }

  // Which centre-turn street each node sits on, which way its traffic runs
  // there, and whether anything else meets it — the "somewhere to turn into"
  // that makes a crossing worth opening.
  const nodeCount = nodeLat.length;
  const centreName = new Int32Array(nodeCount).fill(-1);
  const dirLat = new Float64Array(nodeCount);
  const dirLon = new Float64Array(nodeCount);
  for (const e of centreEdges) {
    const points = polylines.get(e);
    for (const [node, from, to] of [
      [edgeFrom[e], points[0], points[1]],
      [edgeTo[e], points[points.length - 2], points[points.length - 1]]
    ]) {
      if (centreName[node] >= 0) continue;
      centreName[node] = edgeName[e];
      const cosLat = Math.max(0.05, Math.cos(nodeLat[node] * 1e-7 * (Math.PI / 180)));
      const dy = to[0] - from[0];
      const dx = (to[1] - from[1]) * cosLat;
      const length = Math.hypot(dx, dy) || 1;
      dirLat[node] = dy / length;
      dirLon[node] = dx / length;
    }
  }
  const hasSideRoad = new Uint8Array(nodeCount);
  for (let e = 0; e < edgeCount; e++) {
    for (const node of [edgeFrom[e], edgeTo[e]]) {
      if (centreName[node] >= 0 && edgeName[e] !== centreName[node]) hasSideRoad[node] = 1;
    }
  }

  // One crossing per node worth crossing to, at the closest point on the line
  // facing it.
  const cutsByEdge = new Map();
  const crossings = [];
  for (let node = 0; node < nodeCount; node++) {
    if (centreName[node] < 0 || !hasSideRoad[node]) continue;
    const lat = nodeLat[node];
    const lon = nodeLon[node];
    const cosLat = Math.max(0.05, Math.cos(lat * 1e-7 * (Math.PI / 180)));
    const cx = Math.floor(lat / CELL_E7);
    const cy = Math.floor(lon / CELL_E7);
    let best = null;
    for (let dx = -1; dx <= 1; dx++) {
      for (let dy = -1; dy <= 1; dy++) {
        const bucket = grid.get(cellKey(cx + dx, cy + dy));
        if (!bucket) continue;
        for (let b = 0; b < bucket.length; b += 2) {
          const edge = bucket[b];
          const index = bucket[b + 1];
          if (edgeName[edge] !== centreName[node]) continue;
          if (edgeFrom[edge] === node || edgeTo[edge] === node) continue;
          const points = polylines.get(edge);
          const a = points[index];
          const c = points[index + 1];
          const ax = (a[1] - lon) * METERS_PER_E7 * cosLat;
          const ay = (a[0] - lat) * METERS_PER_E7;
          const vx = (c[1] - a[1]) * METERS_PER_E7 * cosLat;
          const vy = (c[0] - a[0]) * METERS_PER_E7;
          const span = vx * vx + vy * vy;
          if (span <= 0) continue;
          const t = -(ax * vx + ay * vy) / span;
          if (t < 0 || t > 1) continue;
          const length = Math.sqrt(span);
          // Facing traffic, not a service road running alongside.
          if ((vy / length) * dirLat[node] + (vx / length) * dirLon[node] > CENTRE_CROSS_OPPOSITE_DOT) continue;
          const perp = Math.hypot(ax + t * vx, ay + t * vy);
          if (perp < CENTRE_CROSS_MIN_METERS || perp > CENTRE_CROSS_MAX_METERS) continue;
          const along = alongs.get(edge);
          const at = along[index] + t * length;
          if (at < CENTRE_CROSS_MARGIN_METERS) continue;
          if (at > along[along.length - 1] - CENTRE_CROSS_MARGIN_METERS) continue;
          if (best && best.perp <= perp) continue;
          best = {
            edge,
            index,
            at,
            perp,
            lat: Math.round(a[0] + t * (c[0] - a[0])),
            lon: Math.round(a[1] + t * (c[1] - a[1]))
          };
        }
      }
    }
    if (!best) continue;
    const cut = {
      seg: best.index,
      at: best.at,
      lat: best.lat,
      lon: best.lon,
      node: nodeLat.length
    };
    nodeLat.push(cut.lat);
    nodeLon.push(cut.lon);
    let list = cutsByEdge.get(best.edge);
    if (!list) cutsByEdge.set(best.edge, (list = []));
    list.push(cut);
    cut.crossing = { from: cut.node, to: node, meters: best.perp, like: best.edge };
    crossings.push(cut.crossing);
  }
  if (!crossings.length) return 0;

  // Two nodes opposite the same spot would split one edge twice a hair apart,
  // leaving a piece too short to carry geometry. The second reuses the first.
  for (const [edge, list] of cutsByEdge) {
    list.sort((a, b) => a.at - b.at);
    const kept = [];
    for (const cut of list) {
      const previous = kept[kept.length - 1];
      if (previous && cut.at - previous.at < CENTRE_CROSS_MARGIN_METERS) {
        // The node it opened stays where it is with nothing attached; the
        // strongly-connected filter at the end of extraction drops it.
        cut.crossing.from = previous.node;
        continue;
      }
      kept.push(cut);
    }
    cutsByEdge.set(edge, kept);
  }

  // Rebuild: geometry lives in one shared byte spool with a prefix-offset per
  // edge, so an edge cannot be lengthened or shortened where it lies.
  const nextGeom = new GrowUint8();
  const nextGeomOffsets = [0];
  const nextLaneBytes = laneBytes ? new GrowUint8() : null;
  const nextLaneOffsets = laneOffsets ? [0] : null;
  const scratch = [];
  const writeGeometry = (points) => {
    scratch.length = 0;
    const interior = points.length - 2;
    pushVarint(scratch, Math.max(0, interior));
    let prevLat = points[0][0];
    let prevLon = points[0][1];
    for (let i = 1; i <= interior; i++) {
      pushZigzag(scratch, points[i][0] - prevLat);
      pushZigzag(scratch, points[i][1] - prevLon);
      prevLat = points[i][0];
      prevLon = points[i][1];
    }
    nextGeom.ensure(scratch.length);
    for (const byte of scratch) nextGeom.data[nextGeom.length++] = byte;
    nextGeomOffsets.push(nextGeom.length);
  };
  const copyGeometry = (edge) => {
    const start = geomOffsets[edge];
    const end = geomOffsets[edge + 1];
    nextGeom.ensure(end - start);
    nextGeom.data.set(data.subarray(start, end), nextGeom.length);
    nextGeom.length += end - start;
    nextGeomOffsets.push(nextGeom.length);
  };
  const copyLanes = (edge) => {
    if (!nextLaneBytes) return;
    if (edge < 0) {
      nextLaneBytes.push(0);
      nextLaneOffsets.push(nextLaneBytes.length);
      return;
    }
    const start = laneOffsets[edge];
    const end = laneOffsets[edge + 1];
    nextLaneBytes.ensure(end - start);
    nextLaneBytes.data.set(laneBytes.data.subarray(start, end), nextLaneBytes.length);
    nextLaneBytes.length += end - start;
    nextLaneOffsets.push(nextLaneBytes.length);
  };

  const out = {
    from: [], to: [], weight: [], dist: [], name: [], way: [], cls: [],
    junction: [], speed: [], cond: [], sign: [], flags: []
  };
  const emit = (fields) => {
    out.from.push(fields.from);
    out.to.push(fields.to);
    out.weight.push(fields.weight);
    out.dist.push(fields.dist);
    out.name.push(fields.name);
    out.way.push(fields.way);
    out.cls.push(fields.cls);
    out.junction.push(fields.junction);
    out.speed.push(fields.speed);
    out.cond.push(fields.cond);
    out.sign.push(fields.sign);
    out.flags.push(fields.flags);
  };

  let splitEdges = 0;
  for (let e = 0; e < edgeCount; e++) {
    const cuts = cutsByEdge.get(e);
    if (!cuts || !cuts.length) {
      emit({
        from: edgeFrom[e], to: edgeTo[e], weight: edgeWeightDs[e], dist: edgeDistDm[e],
        name: edgeName[e], way: edgeWay[e], cls: edgeClass ? edgeClass[e] : 0,
        junction: edgeJunction ? edgeJunction[e] : 0, speed: edgeSpeed ? edgeSpeed[e] : 0,
        cond: edgeCond ? edgeCond[e] : 0, sign: edgeSign ? edgeSign[e] : 0,
        flags: edgeFlags ? edgeFlags[e] : 0
      });
      copyGeometry(e);
      copyLanes(e);
      continue;
    }
    splitEdges++;
    const points = polylines.get(e);
    const packed = edgeJunction ? edgeJunction[e] : 0;
    const junctionKind = packed % 8;
    const junctionAt = junctionKind ? (packed - junctionKind) / 8 : -1;
    const totalMeters = alongs.get(e)[points.length - 1] || 1;
    const pieces = [];
    let start = points[0];
    let startOriginal = 0;
    let from = edgeFrom[e];
    let taken = 0;
    for (const cut of cuts) {
      const piece = [start];
      const original = [startOriginal];
      for (let i = Math.max(1, taken); i <= cut.seg; i++) {
        piece.push(points[i]);
        original.push(i);
      }
      piece.push([cut.lat, cut.lon]);
      original.push(-1);
      pieces.push({ from, to: cut.node, points: piece, original });
      from = cut.node;
      start = [cut.lat, cut.lon];
      startOriginal = -1;
      taken = cut.seg + 1;
    }
    const tail = [start];
    const tailOriginal = [startOriginal];
    for (let i = Math.max(1, taken); i < points.length - 1; i++) {
      tail.push(points[i]);
      tailOriginal.push(i);
    }
    tail.push(points[points.length - 1]);
    tailOriginal.push(points.length - 1);
    pieces.push({ from, to: edgeTo[e], points: tail, original: tailOriginal });

    for (let p = 0; p < pieces.length; p++) {
      const piece = pieces[p];
      let meters = 0;
      for (let i = 0; i + 1 < piece.points.length; i++) {
        meters += haversineMetersE7(
          piece.points[i][0], piece.points[i][1],
          piece.points[i + 1][0], piece.points[i + 1][1]
        );
      }
      // The wait folded into this edge's weight is spread over its pieces
      // rather than charged to one of them: the drive costs what it cost, and
      // no piece is long enough for where the seconds sit to be visible.
      const share = meters / totalMeters;
      const at = junctionAt >= 0 ? piece.original.indexOf(junctionAt) : -1;
      const last = p === pieces.length - 1;
      emit({
        from: piece.from, to: piece.to,
        weight: Math.max(1, Math.round(edgeWeightDs[e] * share)),
        dist: Math.max(1, Math.round(meters * 10)),
        name: edgeName[e], way: edgeWay[e], cls: edgeClass ? edgeClass[e] : 0,
        junction: at >= 0 ? junctionKind + at * 8 : 0,
        speed: edgeSpeed ? edgeSpeed[e] : 0, cond: edgeCond ? edgeCond[e] : 0,
        sign: edgeSign ? edgeSign[e] : 0, flags: edgeFlags ? edgeFlags[e] : 0
      });
      writeGeometry(piece.points);
      // Lane guidance describes the approach to the junction the edge ends at,
      // so it belongs to the piece that still ends there.
      copyLanes(last ? e : -1);
    }
  }

  const crossKmhDs = (meters) => Math.max(1, Math.round((meters / ((CENTRE_CROSS_KMH * 1000) / 3600)) * 10));
  for (const crossing of crossings) {
    const like = crossing.like;
    const weight = CENTRE_CROSS_PENALTY_DS + crossKmhDs(crossing.meters);
    const dist = Math.max(1, Math.round(crossing.meters * 10));
    for (const [from, to] of [[crossing.from, crossing.to], [crossing.to, crossing.from]]) {
      emit({
        from, to, weight, dist,
        name: edgeName[like],
        // No OSM way behind it, and way ids are what restrictions are matched
        // by: zero is the one value no way can have.
        way: 0,
        cls: edgeClass ? edgeClass[like] : 0,
        junction: 0,
        speed: edgeSpeed ? edgeSpeed[like] : 0,
        cond: 0,
        sign: 0,
        flags: 0
      });
      writeGeometry([[nodeLat[from], nodeLon[from]], [nodeLat[to], nodeLon[to]]]);
      copyLanes(-1);
    }
  }

  const replace = (target, values) => {
    target.length = 0;
    for (const value of values) target.push(value);
  };
  replace(edgeFrom, out.from);
  replace(edgeTo, out.to);
  replace(edgeWeightDs, out.weight);
  replace(edgeDistDm, out.dist);
  replace(edgeName, out.name);
  replace(edgeWay, out.way);
  if (edgeClass) replace(edgeClass, out.cls);
  if (edgeJunction) replace(edgeJunction, out.junction);
  if (edgeSpeed) replace(edgeSpeed, out.speed);
  if (edgeCond) replace(edgeCond, out.cond);
  if (edgeSign) replace(edgeSign, out.sign);
  if (edgeFlags) replace(edgeFlags, out.flags);
  replace(geomOffsets, nextGeomOffsets);
  geomBytes.length = 0;
  geomBytes.ensure(nextGeom.length);
  geomBytes.data.set(nextGeom.view(), 0);
  geomBytes.length = nextGeom.length;
  if (nextLaneOffsets) {
    replace(laneOffsets, nextLaneOffsets);
    laneBytes.length = 0;
    laneBytes.ensure(nextLaneBytes.length);
    laneBytes.data.set(nextLaneBytes.view(), 0);
    laneBytes.length = nextLaneBytes.length;
  }

  note(`centre turn lanes: ${crossings.length} crossings opened, ${splitEdges} carriageway edges split`);
  return crossings.length;
}

export function applyTurnRestrictions(context) {
  const {
    restrictions, junctionSigns, laneConnections, signTable, nodeIndex, nodeLat, nodeLon,
    edgeFrom, edgeTo, edgeWeightDs, edgeDistDm, edgeName, edgeWay,
    geomOffsets, geomBytes, laneOffsets, laneBytes, log
  } = context;
  const edgeClass = context.edgeClass || null;
  const edgeJunction = context.edgeJunction || null;
  const edgeSpeed = context.edgeSpeed || null;
  const edgeCond = context.edgeCond || null;
  const edgeSign = context.edgeSign || null;
  const edgeFlags = context.edgeFlags || null;
  if (!restrictions.length) return;

  const copyEdgeTo = (source, from, toOverride) => {
    edgeFrom.push(from);
    edgeTo.push(toOverride ?? edgeTo[source]);
    edgeWeightDs.push(edgeWeightDs[source]);
    edgeDistDm.push(edgeDistDm[source]);
    edgeName.push(edgeName[source]);
    edgeWay.push(edgeWay[source]);
    if (edgeClass) edgeClass.push(edgeClass[source]);
    if (edgeJunction) edgeJunction.push(edgeJunction[source]);
    if (edgeSpeed) edgeSpeed.push(edgeSpeed[source]);
    if (edgeCond) edgeCond.push(edgeCond[source]);
    if (edgeSign) edgeSign.push(edgeSign[source]);
    if (edgeFlags) edgeFlags.push(edgeFlags[source]);
    const start = geomOffsets[source];
    const end = geomOffsets[source + 1];
    geomBytes.ensure(end - start);
    geomBytes.data.set(geomBytes.data.subarray(start, end), geomBytes.length);
    geomBytes.length += end - start;
    geomOffsets.push(geomBytes.length);
    if (laneOffsets && laneBytes) {
      const laneStart = laneOffsets[source];
      const laneEnd = laneOffsets[source + 1];
      laneBytes.ensure(laneEnd - laneStart);
      laneBytes.data.set(laneBytes.data.subarray(laneStart, laneEnd), laneBytes.length);
      laneBytes.length += laneEnd - laneStart;
      laneOffsets.push(laneBytes.length);
    }
    return edgeFrom.length - 1;
  };

  // --- Single-via-way restrictions: expand the via chain with path memory
  // so only traffic that entered from the from-way sees the restricted exit.
  const viaWayRestrictions = restrictions.filter(restriction => restriction.viaWays != null);
  let viaWayApplied = 0;
  let viaWayUnresolved = 0;
  if (viaWayRestrictions.length) {
    const involvedWays = new Set();
    for (const restriction of viaWayRestrictions) {
      involvedWays.add(restriction.fromWay);
      for (const way of restriction.viaWays) involvedWays.add(way);
      involvedWays.add(restriction.toWay);
    }
    const wayEdges = new Map();
    const originalEdgeCount = edgeFrom.length;
    for (let e = 0; e < originalEdgeCount; e++) {
      if (!involvedWays.has(edgeWay[e])) continue;
      let list = wayEdges.get(edgeWay[e]);
      if (!list) wayEdges.set(edgeWay[e], (list = []));
      list.push(e);
    }
    const nodesOf = (wayId) => {
      const set = new Set();
      for (const e of wayEdges.get(wayId) || []) {
        set.add(edgeFrom[e]);
        set.add(edgeTo[e]);
      }
      return set;
    };
    for (const restriction of viaWayRestrictions) {
      // Union of all via ways: member order in the relation stops mattering
      // and multi-via-way chains resolve exactly like single ones.
      const viaNodes = new Set();
      const viaEdges = [];
      for (const way of restriction.viaWays) {
        for (const node of nodesOf(way)) viaNodes.add(node);
        for (const e of wayEdges.get(way) || []) viaEdges.push(e);
      }
      const entries = [...nodesOf(restriction.fromWay)].filter(node => viaNodes.has(node));
      const exits = [...nodesOf(restriction.toWay)].filter(node => viaNodes.has(node));
      if (entries.length !== 1 || exits.length !== 1 || entries[0] === exits[0]) {
        viaWayUnresolved++;
        continue;
      }
      const entry = entries[0];
      const exit = exits[0];
      // Directed BFS along the via-way union from entry to exit, capped.
      const parentEdge = new Map([[entry, -1]]);
      let frontier = [entry];
      let found = false;
      for (let depth = 0; depth < 12 && frontier.length && !found; depth++) {
        const next = [];
        for (const node of frontier) {
          for (const e of viaEdges) {
            if (edgeFrom[e] !== node || parentEdge.has(edgeTo[e])) continue;
            parentEdge.set(edgeTo[e], e);
            if (edgeTo[e] === exit) {
              found = true;
              break;
            }
            next.push(edgeTo[e]);
          }
          if (found) break;
        }
        frontier = next;
      }
      if (!found) {
        viaWayUnresolved++;
        continue;
      }
      const chainEdges = [];
      for (let node = exit; node !== entry; node = edgeFrom[parentEdge.get(node)]) {
        chainEdges.push(parentEdge.get(node));
      }
      chainEdges.reverse();
      // Copies of every chain node past the entry, with the chain edges
      // rewired copy-to-copy so path memory survives intermediate junctions.
      const chainNodes = [entry, ...chainEdges.map(e => edgeTo[e])];
      const copyOf = new Map();
      for (let i = 1; i < chainNodes.length; i++) {
        copyOf.set(chainNodes[i], nodeLat.length);
        nodeLat.push(nodeLat[chainNodes[i]]);
        nodeLon.push(nodeLon[chainNodes[i]]);
      }
      const entryCopy = nodeLat.length;
      nodeLat.push(nodeLat[entry]);
      nodeLon.push(nodeLon[entry]);
      copyOf.set(entry, entryCopy);
      // Redirect from-way approaches into the entry copy.
      for (let e = 0; e < originalEdgeCount; e++) {
        if (edgeWay[e] === restriction.fromWay && edgeTo[e] === entry) edgeTo[e] = entryCopy;
      }
      // Rebuild each chain node's outgoing edges on its copy.
      for (let i = 0; i < chainNodes.length; i++) {
        const original = chainNodes[i];
        const copy = copyOf.get(original);
        const isExit = original === exit;
        const chainNext = i + 1 < chainNodes.length ? chainEdges[i] : -1;
        for (let e = 0; e < originalEdgeCount; e++) {
          if (edgeFrom[e] !== original) continue;
          if (isExit) {
            const onToWay = edgeWay[e] === restriction.toWay;
            if (restriction.only ? onToWay : !onToWay) copyEdgeTo(e, copy);
            continue;
          }
          if (e === chainNext) {
            copyEdgeTo(e, copy, copyOf.get(edgeTo[e]));
            continue;
          }
          // Leaving the via way mid-chain abandons the restricted movement:
          // allowed for no_, forbidden for only_.
          if (!restriction.only) copyEdgeTo(e, copy);
        }
      }
      viaWayApplied++;
    }
  }

  // --- Single-via-node restrictions, grouped by (via graph node, from way).
  const byVia = new Map();
  let mapped = 0;
  for (const restriction of restrictions) {
    if (restriction.viaWays != null) continue;
    const via = nodeIndex.get(restriction.viaNode);
    if (via == null) continue;
    mapped++;
    const key = `${via}:${restriction.fromWay}`;
    let list = byVia.get(key);
    if (!list) {
      list = { via, fromWay: restriction.fromWay, onlys: [], nos: [] };
      byVia.set(key, list);
    }
    (restriction.only ? list.onlys : list.nos).push(restriction);
  }
  if (!byVia.size) {
    log(`restrictions: 0 via-node mapped, ${viaWayApplied} via-way applied, ${viaWayUnresolved} via-way unresolved`);
    return;
  }

  const viaNodes = new Set([...byVia.values()].map(group => group.via));
  const incoming = new Map();
  const outgoing = new Map();
  for (let e = 0; e < edgeFrom.length; e++) {
    if (viaNodes.has(edgeTo[e])) {
      let list = incoming.get(edgeTo[e]);
      if (!list) incoming.set(edgeTo[e], (list = []));
      list.push(e);
    }
    if (viaNodes.has(edgeFrom[e])) {
      let list = outgoing.get(edgeFrom[e]);
      if (!list) outgoing.set(edgeFrom[e], (list = []));
      list.push(e);
    }
  }
  // Snapshot outgoing lists: copies appended later must not join them.
  for (const [node, list] of outgoing) outgoing.set(node, [...list]);

  const allowedOut = (group, outEdge, inEdge) => {
    const way = edgeWay[outEdge];
    if (group.onlys.length) {
      return group.onlys.some(only => way === only.toWay);
    }
    for (const no of group.nos) {
      if (way !== no.toWay) continue;
      // A u-turn restriction on one way forbids only the immediate
      // reversal, not continuing straight along the same way.
      if (no.toWay === group.fromWay && edgeTo[outEdge] !== edgeFrom[inEdge]) continue;
      return false;
    }
    return true;
  };

  const copyEdge = copyEdgeTo;

  // Work queue of (restricted group, incoming edge, chain depth). Copies
  // that land on another restricted via node re-enqueue, bounded by depth
  // so pathological mutually restricted micro-loops terminate.
  const queue = [];
  for (const group of byVia.values()) {
    for (const inEdge of incoming.get(group.via) || []) {
      if (edgeWay[inEdge] === group.fromWay) queue.push([group, inEdge, 0]);
    }
  }
  let copies = 0;
  let copyEdges = 0;
  let depthLimited = 0;
  while (queue.length) {
    const [group, inEdge, depth] = queue.pop();
    const outs = outgoing.get(group.via) || [];
    const allowed = outs.filter(outEdge => allowedOut(group, outEdge, inEdge));
    if (allowed.length === outs.length) continue; // restriction is a no-op here
    const copyNode = nodeLat.length;
    nodeLat.push(nodeLat[group.via]);
    nodeLon.push(nodeLon[group.via]);
    copies++;
    edgeTo[inEdge] = copyNode;
    for (const outEdge of allowed) {
      const created = copyEdge(outEdge, copyNode);
      copyEdges++;
      const target = edgeTo[created];
      if (depth >= 3) {
        if (viaNodes.has(target)) depthLimited++;
        continue;
      }
      const chained = byVia.get(`${target}:${edgeWay[created]}`);
      if (chained) queue.push([chained, created, depth + 1]);
    }
  }
  log(`restrictions: ${mapped} via-node mapped (${copies} copies, ${copyEdges} copied edges${depthLimited ? `, ${depthLimited} depth-limited` : ""}), ${viaWayApplied} via-way applied, ${viaWayUnresolved} via-way unresolved`);
}

// Edge-based graph by full junction expansion: every junction J splits into
// one copy per incoming edge, and every outgoing edge is re-emitted per
// approach with a bearing-derived turn cost added — the standard line-graph
// construction expressed as node splitting, so edges keep their geometry,
// names, and distances and the whole downstream pipeline (partition,
// cliques, multilevel query, snapping) is untouched. Via-node restrictions
// become exact (approach, exit) filters here, which also makes chained
// restrictions exact (the old depth-limited queue only applies to the
// non-turn-cost mode).
export function expandTurnCosts(context, turnCosts) {
  const {
    restrictions, junctionSigns, laneConnections, signTable, nodeIndex, nodeLat, nodeLon,
    edgeFrom, edgeTo, edgeWeightDs, edgeDistDm, edgeName, edgeWay, edgeClass, edgeJunction,
    edgeSpeed, edgeCond, edgeSign, edgeLimit, edgeCharge, edgeLaneSign, edgeFlags, geomOffsets, geomBytes, laneOffsets, laneBytes, log
  } = context;
  const edgeCount = edgeFrom.length;
  const nodeCount = nodeLat.length;
  const geomData = geomBytes.data;

  // Departure and arrival bearings per edge from its polyline endpoints.
  const depBearing = new Float32Array(edgeCount);
  const arrBearing = new Float32Array(edgeCount);
  const readState = { pos: 0 };
  const readVarint = () => {
    let value = 0;
    let multiplier = 1;
    for (;;) {
      const byte = geomData[readState.pos++];
      value += (byte & 0x7f) * multiplier;
      if ((byte & 0x80) === 0) return value;
      multiplier *= 0x80;
    }
  };
  const readZigzag = () => {
    const raw = readVarint();
    return raw % 2 === 1 ? -(raw + 1) / 2 : raw / 2;
  };
  const bearing = (aLat, aLon, bLat, bLon, cosLat) => {
    const dx = (bLon - aLon) * cosLat;
    const dy = bLat - aLat;
    return (Math.atan2(dx, dy) * 180) / Math.PI;
  };
  for (let e = 0; e < edgeCount; e++) {
    const fromLat = nodeLat[edgeFrom[e]];
    const fromLon = nodeLon[edgeFrom[e]];
    const toLat = nodeLat[edgeTo[e]];
    const toLon = nodeLon[edgeTo[e]];
    const cosLat = Math.max(0.05, Math.cos(fromLat * 1e-7 * (Math.PI / 180)));
    readState.pos = geomOffsets[e];
    const interior = readVarint();
    let firstLat = toLat;
    let firstLon = toLon;
    let lastLat = fromLat;
    let lastLon = fromLon;
    let lat = fromLat;
    let lon = fromLon;
    for (let i = 0; i < interior; i++) {
      lat += readZigzag();
      lon += readZigzag();
      if (i === 0) {
        firstLat = lat;
        firstLon = lon;
      }
      lastLat = lat;
      lastLon = lon;
    }
    depBearing[e] = bearing(fromLat, fromLon, firstLat, firstLon, cosLat);
    arrBearing[e] = bearing(lastLat, lastLon, toLat, toLon, cosLat);
  }

  const turnCostFor = (inEdge, outEdge) => {
    const isTwin = edgeTo[outEdge] === edgeFrom[inEdge];
    let delta = depBearing[outEdge] - arrBearing[inEdge];
    while (delta > 180) delta -= 360;
    while (delta <= -180) delta += 360;
    const magnitude = Math.abs(delta);
    if (isTwin || magnitude >= 150) return turnCosts.uturn;
    if (magnitude < 30) return 0;
    const left = delta < 0;
    if (magnitude < 60) return left ? turnCosts.slightLeft : turnCosts.slightRight;
    return left ? turnCosts.left : turnCosts.right;
  };

  // Via-node restrictions grouped by junction, then approach way. Looking up
  // a `${junction}:${way}` string for every candidate turn created hundreds
  // of millions of short-lived strings on country graphs. Nested numeric
  // maps keep the hot loop allocation-free.
  const byVia = new Map();
  // Signs and lane connections, indexed the same way turns are: junction,
  // then approach way. One movement through one junction is what all three
  // describe, so they share the lookup rather than each growing their own.
  const signByVia = new Map();
  const laneByVia = new Map();
  let signViaMissing = 0;
  for (const sign of context.junctionSigns || []) {
    const via = nodeIndex.get(sign.viaNode);
    if (via == null) { signViaMissing++; continue; }
    let byWay = signByVia.get(via);
    if (!byWay) signByVia.set(via, (byWay = new Map()));
    byWay.set(sign.fromWay, sign);
  }
  for (const link of context.laneConnections || []) {
    const via = nodeIndex.get(link.viaNode);
    if (via == null) continue;
    let byWay = laneByVia.get(via);
    if (!byWay) laneByVia.set(via, (byWay = new Map()));
    let byTo = byWay.get(link.fromWay);
    if (!byTo) byWay.set(link.fromWay, (byTo = new Map()));
    byTo.set(link.toWay, link.mask);
  }
  /** The sign a driver making this turn will be reading, or null. */
  const signForTurn = (inEdge, outEdge) => {
    const byWay = signByVia.get(edgeFrom[outEdge]);
    if (!byWay) return null;
    const sign = byWay.get(edgeWay[inEdge]);
    if (!sign || sign.toWay !== edgeWay[outEdge]) return null;
    return sign;
  };
  /** Which approach lanes actually reach this turning, or 0 for unknown. */
  const lanesForTurn = (inEdge, outEdge) => {
    const byWay = laneByVia.get(edgeFrom[outEdge]);
    if (!byWay) return 0;
    return byWay.get(edgeWay[inEdge])?.get(edgeWay[outEdge]) || 0;
  };
  const banTable = makeBanTable();
  let restrictedApproaches = 0;
  let timedTurns = 0;
  let signedTurns = 0;
  let connectedTurns = 0;
  for (const restriction of restrictions) {
    if (restriction.viaWays != null || restriction.viaNode == null) continue;
    const via = nodeIndex.get(restriction.viaNode);
    if (via == null) continue;
    let byWay = byVia.get(via);
    if (!byWay) byVia.set(via, (byWay = new Map()));
    let group = byWay.get(restriction.fromWay);
    if (!group) {
      byWay.set(restriction.fromWay, (group = { onlys: [], nos: [] }));
      restrictedApproaches++;
    }
    (restriction.only ? group.onlys : group.nos).push(restriction);
  }
  /**
   * What a restriction says about this turn.
   *
   * Returns 0 when nothing forbids it, -1 when it is forbidden outright, and
   * a positive ban-rule id when it is forbidden only during a window. The
   * three answers want three different things done: keep the turn, delete it,
   * or keep it and record when it is shut. Reading a conditional restriction
   * as "no restriction" — which is what discarding the relation amounted to —
   * left the turn open at the two hours of the day it is closed.
   */
  const turnBan = (inEdge, outEdge) => {
    const byWay = byVia.get(edgeFrom[outEdge]);
    if (!byWay) return 0;
    const group = byWay.get(edgeWay[inEdge]);
    if (!group) return 0;
    const way = edgeWay[outEdge];
    if (group.onlys.length) {
      const permitted = group.onlys.find(only => way === only.toWay);
      if (permitted) return 0;
      // An `only_` restriction forbids every other turn. When it applies at
      // all hours the turn simply goes; when it is a window, the turn stays
      // and is shut for that window like any other conditional ban.
      const timed = group.onlys.every(only => only.window);
      return timed ? banTable.idFor(group.onlys[0].window) : -1;
    }
    for (const no of group.nos) {
      if (way !== no.toWay) continue;
      if (no.toWay === no.fromWay && edgeTo[outEdge] !== edgeFrom[inEdge]) continue;
      return no.window ? banTable.idFor(no.window) : -1;
    }
    return 0;
  };

  // Incoming-edge CSR.
  const inStart = new Uint32Array(nodeCount + 1);
  for (let e = 0; e < edgeCount; e++) inStart[edgeTo[e] + 1]++;
  for (let i = 0; i < nodeCount; i++) inStart[i + 1] += inStart[i];
  const inEdges = new Uint32Array(edgeCount);
  {
    const cursor = Uint32Array.from(inStart.subarray(0, nodeCount));
    for (let e = 0; e < edgeCount; e++) inEdges[cursor[edgeTo[e]]++] = e;
  }

  // One expanded node per original edge: the copy of to(e) reached via e.
  const newLat = new Int32Array(edgeCount);
  const newLon = new Int32Array(edgeCount);
  for (let e = 0; e < edgeCount; e++) {
    newLat[e] = nodeLat[edgeTo[e]];
    newLon[e] = nodeLon[edgeTo[e]];
  }
  const laneData = laneBytes ? laneBytes.view() : null;

  // Count exact output sizes first. India expands 42M base edges into roughly
  // 120M turn edges: growing thirteen ordinary JS arrays until then exhausted
  // the 16 GiB V8 heap before conversion could begin. The count pass is cheap
  // compared with geometry decoding and lets every result column be allocated
  // once, at its final typed width.
  let expandedEdgeCount = 0;
  let expandedGeomBytes = 0;
  let expandedLaneBytes = 0;
  let filteredTurns = 0;
  for (let out = 0; out < edgeCount; out++) {
    const junction = edgeFrom[out];
    const geomLength = geomOffsets[out + 1] - geomOffsets[out];
    const laneLength = laneData && laneOffsets
      ? laneOffsets[out + 1] - laneOffsets[out]
      : 1;
    for (let slot = inStart[junction]; slot < inStart[junction + 1]; slot++) {
      const inEdge = inEdges[slot];
      if (turnBan(inEdge, out) < 0) {
        filteredTurns++;
        continue;
      }
      expandedEdgeCount++;
      expandedGeomBytes += geomLength;
      expandedLaneBytes += laneLength;
    }
  }
  if (expandedEdgeCount > 0xffffffff) {
    throw new Error(`Turn-expanded graph has ${expandedEdgeCount} edges; rfroute source format supports at most 4,294,967,295.`);
  }
  if (expandedGeomBytes > 0xffffffff || expandedLaneBytes > 0xffffffff) {
    throw new Error("Turn-expanded geometry or lane payload exceeds the 4 GiB source-column limit.");
  }

  const newFrom = new Uint32Array(expandedEdgeCount);
  const newTo = new Uint32Array(expandedEdgeCount);
  const newWeight = new Uint32Array(expandedEdgeCount);
  const newDist = new Uint32Array(expandedEdgeCount);
  const newName = new Uint32Array(expandedEdgeCount);
  const newClass = new Uint8Array(expandedEdgeCount);
  const newJunction = new Uint8Array(expandedEdgeCount);
  const newSpeed = new Uint8Array(expandedEdgeCount);
  const newCond = new Uint8Array(expandedEdgeCount);
  const newSign = new Uint32Array(expandedEdgeCount);
  // When this turn is shut, as an index into the ban table. Zero on all but a
  // handful of turns in a city, and the reason a peak-hour ban can be obeyed
  // at all: the turn stays in the graph and says when it is closed, instead
  // of being deleted for every hour of the week or kept for none of them.
  const newBan = new Uint32Array(expandedEdgeCount);
  // Every approach copy of a road is still the same road, and the arch over
  // it is still the same arch. Dropping this column here was invisible: the
  // tags were read and the table was written, and afterwards not one edge in
  // the country pointed at it.
  const newLimit = new Uint32Array(expandedEdgeCount);
  const newCharge = new Uint32Array(expandedEdgeCount);
  const newLaneSign = new Uint32Array(expandedEdgeCount);
  const newLaneMask = new Uint32Array(expandedEdgeCount);
  const newFlags = new Uint8Array(expandedEdgeCount);
  const newGeomOffsets = new Uint32Array(expandedEdgeCount + 1);
  const newGeomBytes = new Uint8Array(expandedGeomBytes);
  const newLaneOffsets = new Uint32Array(expandedEdgeCount + 1);
  const newLaneBytes = new Uint8Array(expandedLaneBytes);
  let edgeCursor = 0;
  let geomCursor = 0;
  let laneCursor = 0;
  for (let out = 0; out < edgeCount; out++) {
    const junction = edgeFrom[out];
    for (let slot = inStart[junction]; slot < inStart[junction + 1]; slot++) {
      const inEdge = inEdges[slot];
      const ban = turnBan(inEdge, out);
      if (ban < 0) continue;
      if (ban > 0) timedTurns++;
      newBan[edgeCursor] = ban;
      newFrom[edgeCursor] = inEdge;
      newTo[edgeCursor] = out;
      newWeight[edgeCursor] = edgeWeightDs[out] + turnCostFor(inEdge, out);
      newDist[edgeCursor] = edgeDistDm[out];
      newName[edgeCursor] = edgeName[out];
      newClass[edgeCursor] = edgeClass[out];
      newJunction[edgeCursor] = edgeJunction ? edgeJunction[out] : 0;
      newSpeed[edgeCursor] = edgeSpeed ? edgeSpeed[out] : 0;
      newCond[edgeCursor] = edgeCond ? edgeCond[out] : 0;
      // The panel a driver making this turn will actually be reading. It
      // outranks whatever the road itself carries, because a sign at the
      // junction is written for this movement and the road's own tags are
      // written for the road.
      const turnSign = signForTurn(inEdge, out);
      if (turnSign && signTable) {
        newSign[edgeCursor] = signTable.idFor({
          ref: turnSign.ref,
          exit: "",
          destRef: turnSign.ref,
          dest: turnSign.text
        });
        signedTurns++;
      } else {
        newSign[edgeCursor] = edgeSign ? edgeSign[out] : 0;
      }
      // Which approach lanes reach this turning, when the map says outright
      // rather than leaving it to be inferred from the arrows.
      const laneMask = lanesForTurn(inEdge, out);
      newLaneMask[edgeCursor] = laneMask;
      if (laneMask) connectedTurns++;
      newLimit[edgeCursor] = edgeLimit ? edgeLimit[out] : 0;
      newCharge[edgeCursor] = edgeCharge ? edgeCharge[out] : 0;
      newLaneSign[edgeCursor] = edgeLaneSign ? edgeLaneSign[out] : 0;
      newFlags[edgeCursor] = edgeFlags ? edgeFlags[out] : 0;
      const start = geomOffsets[out];
      const end = geomOffsets[out + 1];
      newGeomBytes.set(geomData.subarray(start, end), geomCursor);
      geomCursor += end - start;
      if (laneData && laneOffsets) {
        const laneStart = laneOffsets[out];
        const laneEnd = laneOffsets[out + 1];
        newLaneBytes.set(laneData.subarray(laneStart, laneEnd), laneCursor);
        laneCursor += laneEnd - laneStart;
      } else {
        newLaneBytes[laneCursor++] = 0;
      }
      edgeCursor++;
      newGeomOffsets[edgeCursor] = geomCursor;
      newLaneOffsets[edgeCursor] = laneCursor;
    }
  }
  log(`turn costs: ${edgeCount} junction copies, ${expandedEdgeCount} expanded edges, ${filteredTurns} restricted turns filtered, ${restrictedApproaches} restricted approaches`);
  // Counted out loud, because a column that quietly arrives empty is exactly
  // how the posted-limit work went wrong: the tags parsed, the table filled,
  // and not one edge pointed at it. A zero here with rules in the table means
  // the ban never reached an edge.
  log(`timed turns: ${timedTurns} shut on a schedule, ${banTable.list.length} distinct windows`);
  // Counted out loud like every other column that has to survive the trip
  // from a tag to an edge. A zero here with relations in the extract means
  // the sign never reached the turn it describes.
  log(`junction signs: ${(context.junctionSigns || []).length} relation(s), ${signViaMissing} with a via node off the graph, ${signedTurns} turns carry their own panel`);
  log(`lane connections: ${(context.laneConnections || []).length} relation(s), ${connectedTurns} turns say which lanes reach them`);
  return {
    nodeLat: newLat,
    nodeLon: newLon,
    edgeFrom: newFrom,
    edgeTo: newTo,
    edgeWeightDs: newWeight,
    edgeDistDm: newDist,
    edgeName: newName,
    edgeClass: newClass,
    edgeJunction: newJunction,
    edgeSpeed: newSpeed,
    edgeCond: newCond,
    edgeSign: newSign,
    edgeLimit: newLimit,
    edgeCharge: newCharge,
    edgeLaneSign: newLaneSign,
    edgeLaneMask: newLaneMask,
    signedTurns,
    connectedTurns,
    edgeBan: newBan,
    banRules: banTable.list,
    timedTurns,
    edgeFlags: newFlags,
    geomOffsets: newGeomOffsets,
    geomBytes: newGeomBytes,
    laneOffsets: newLaneOffsets,
    laneBytes: newLaneBytes
  };
}

function buildCsr(nodeCount, from, to) {
  const degree = new Uint32Array(nodeCount + 1);
  for (let i = 0; i < from.length; i++) degree[from[i] + 1]++;
  for (let i = 0; i < nodeCount; i++) degree[i + 1] += degree[i];
  const targets = new Uint32Array(from.length);
  const edgeIds = new Uint32Array(from.length);
  const cursor = Uint32Array.from(degree.subarray(0, nodeCount));
  for (let i = 0; i < from.length; i++) {
    const slot = cursor[from[i]]++;
    targets[slot] = to[i];
    edgeIds[slot] = i;
  }
  return { rowStart: degree, targets, edgeIds };
}

// Iterative Kosaraju: keep only the largest strongly connected component so
// random origin/destination pairs are always mutually reachable.
export function filterLargestScc(graph, log) {
  const nodeCount = graph.nodeLat.length;
  let component;
  let componentCount = 0;
  let bestComponent = -1;
  let bestSize = 0;
  // Keep both CSR traversals and their DFS scratch in a block so they become
  // unreachable before the retained component is materialized. For Brazil
  // this releases roughly two GiB at exactly the former peak boundary.
  {
    const forward = buildCsr(nodeCount, graph.edgeFrom, graph.edgeTo);
    const backward = buildCsr(nodeCount, graph.edgeTo, graph.edgeFrom);
    const order = new Uint32Array(nodeCount);
    let orderLength = 0;
    const state = new Uint8Array(nodeCount);
    const stack = new Uint32Array(nodeCount + 1);
    const iter = new Uint32Array(nodeCount);
    for (let root = 0; root < nodeCount; root++) {
      if (state[root]) continue;
      let top = 0;
      stack[top] = root;
      state[root] = 1;
      iter[root] = forward.rowStart[root];
      while (top >= 0) {
        const node = stack[top];
        if (iter[node] < forward.rowStart[node + 1]) {
          const next = forward.targets[iter[node]++];
          if (!state[next]) {
            state[next] = 1;
            iter[next] = forward.rowStart[next];
            stack[++top] = next;
          }
        } else {
          order[orderLength++] = node;
          top--;
        }
      }
    }
    component = new Int32Array(nodeCount).fill(-1);
    const reverseStack = new Uint32Array(nodeCount);
    for (let i = orderLength - 1; i >= 0; i--) {
      const root = order[i];
      if (component[root] >= 0) continue;
      const id = componentCount++;
      let size = 0;
      let top = 0;
      reverseStack[top++] = root;
      component[root] = id;
      while (top > 0) {
        const node = reverseStack[--top];
        size++;
        for (let e = backward.rowStart[node]; e < backward.rowStart[node + 1]; e++) {
          const next = backward.targets[e];
          if (component[next] < 0) {
            component[next] = id;
            reverseStack[top++] = next;
          }
        }
      }
      if (size > bestSize) {
        bestSize = size;
        bestComponent = id;
      }
    }
  }
  log(`scc: ${componentCount} components, largest ${bestSize} of ${nodeCount} nodes`);
  if (bestSize === nodeCount) return graph;

  // Reuse the component column as the node remap instead of overlapping two
  // node-sized Int32 arrays.
  const remap = component;
  let keptNodes = 0;
  for (let i = 0; i < nodeCount; i++) {
    remap[i] = remap[i] === bestComponent ? keptNodes++ : -1;
  }
  // Compact into the source buffers. The retained node/edge order is stable,
  // and every destination index is at or before its source index, so forward
  // writes cannot clobber unread input. Keeping subarray views avoids holding
  // a second country-sized graph at the end of SCC filtering.
  for (let i = 0; i < nodeCount; i++) {
    if (remap[i] >= 0) {
      graph.nodeLat[remap[i]] = graph.nodeLat[i];
      graph.nodeLon[remap[i]] = graph.nodeLon[i];
    }
  }
  // Count payload sizes for format-limit validation and diagnostics. The
  // second pass compacts all columns and payloads into their existing buffers
  // rather than materializing an equally large retained graph beside them.
  let keptEdges = 0;
  let geomByteCount = 0;
  let laneByteCount = 0;
  for (let i = 0; i < graph.edgeFrom.length; i++) {
    const from = remap[graph.edgeFrom[i]];
    const to = remap[graph.edgeTo[i]];
    if (from < 0 || to < 0) continue;
    keptEdges++;
    geomByteCount += graph.geomOffsets[i + 1] - graph.geomOffsets[i];
    if (graph.laneOffsets && graph.laneBytes) {
      laneByteCount += graph.laneOffsets[i + 1] - graph.laneOffsets[i];
    } else {
      laneByteCount++;
    }
  }
  if (geomByteCount > 0xffffffff || laneByteCount > 0xffffffff) {
    throw new Error("Retained SCC geometry or lane payload exceeds the 4 GiB source-column limit.");
  }
  const edgeSpeed = graph.edgeSpeed || new Uint8Array(graph.edgeFrom.length);
  const edgeCond = graph.edgeCond || new Uint8Array(graph.edgeFrom.length);
  const edgeSign = graph.edgeSign || new Uint32Array(graph.edgeFrom.length);
  const edgeLimit = graph.edgeLimit || new Uint32Array(graph.edgeFrom.length);
  const edgeBan = graph.edgeBan || new Uint32Array(graph.edgeFrom.length);
  const edgeCharge = graph.edgeCharge || new Uint32Array(graph.edgeFrom.length);
  const edgeLaneSign = graph.edgeLaneSign || new Uint32Array(graph.edgeFrom.length);
  const edgeLaneMask = graph.edgeLaneMask || new Uint32Array(graph.edgeFrom.length);
  const edgeFlags = graph.edgeFlags || new Uint8Array(graph.edgeFrom.length);
  const laneOffsets = graph.laneOffsets || new Uint32Array(graph.edgeFrom.length + 1);
  const laneBytes = graph.laneBytes || new Uint8Array(graph.edgeFrom.length);
  let edgeIndex = 0;
  let geomOffset = 0;
  let laneOffset = 0;
  for (let i = 0; i < graph.edgeFrom.length; i++) {
    const from = remap[graph.edgeFrom[i]];
    const to = remap[graph.edgeTo[i]];
    if (from < 0 || to < 0) continue;
    graph.edgeFrom[edgeIndex] = from;
    graph.edgeTo[edgeIndex] = to;
    graph.edgeWeightDs[edgeIndex] = graph.edgeWeightDs[i];
    graph.edgeDistDm[edgeIndex] = graph.edgeDistDm[i];
    graph.edgeName[edgeIndex] = graph.edgeName[i];
    graph.edgeClass[edgeIndex] = graph.edgeClass[i];
    graph.edgeJunction[edgeIndex] = graph.edgeJunction[i];
    edgeSpeed[edgeIndex] = graph.edgeSpeed ? graph.edgeSpeed[i] : 0;
    edgeCond[edgeIndex] = graph.edgeCond ? graph.edgeCond[i] : 0;
    edgeSign[edgeIndex] = graph.edgeSign ? graph.edgeSign[i] : 0;
    edgeLimit[edgeIndex] = graph.edgeLimit ? graph.edgeLimit[i] : 0;
    edgeBan[edgeIndex] = graph.edgeBan ? graph.edgeBan[i] : 0;
    edgeCharge[edgeIndex] = graph.edgeCharge ? graph.edgeCharge[i] : 0;
    edgeLaneSign[edgeIndex] = graph.edgeLaneSign ? graph.edgeLaneSign[i] : 0;
    edgeLaneMask[edgeIndex] = graph.edgeLaneMask ? graph.edgeLaneMask[i] : 0;
    edgeFlags[edgeIndex] = graph.edgeFlags ? graph.edgeFlags[i] : 0;
    const geomStart = graph.geomOffsets[i];
    const geomEnd = graph.geomOffsets[i + 1];
    if (geomOffset !== geomStart) {
      graph.geomBytes.copyWithin(geomOffset, geomStart, geomEnd);
    }
    geomOffset += geomEnd - geomStart;
    if (graph.laneOffsets && graph.laneBytes) {
      const laneStart = graph.laneOffsets[i];
      const laneEnd = graph.laneOffsets[i + 1];
      if (laneOffset !== laneStart) {
        laneBytes.copyWithin(laneOffset, laneStart, laneEnd);
      }
      laneOffset += laneEnd - laneStart;
    } else {
      laneBytes[laneOffset++] = 0;
    }
    edgeIndex++;
    graph.geomOffsets[edgeIndex] = geomOffset;
    laneOffsets[edgeIndex] = laneOffset;
  }
  return {
    nodeLat: graph.nodeLat.subarray(0, keptNodes),
    nodeLon: graph.nodeLon.subarray(0, keptNodes),
    edgeFrom: graph.edgeFrom.subarray(0, keptEdges),
    edgeTo: graph.edgeTo.subarray(0, keptEdges),
    edgeWeightDs: graph.edgeWeightDs.subarray(0, keptEdges),
    edgeDistDm: graph.edgeDistDm.subarray(0, keptEdges),
    edgeName: graph.edgeName.subarray(0, keptEdges),
    edgeClass: graph.edgeClass.subarray(0, keptEdges),
    edgeJunction: graph.edgeJunction.subarray(0, keptEdges),
    edgeSpeed: edgeSpeed.subarray(0, keptEdges),
    edgeCond: edgeCond.subarray(0, keptEdges),
    edgeSign: edgeSign.subarray(0, keptEdges),
    edgeLimit: edgeLimit.subarray(0, keptEdges),
    edgeBan: edgeBan.subarray(0, keptEdges),
    edgeCharge: edgeCharge.subarray(0, keptEdges),
    edgeLaneSign: edgeLaneSign.subarray(0, keptEdges),
    edgeLaneMask: edgeLaneMask.subarray(0, keptEdges),
    edgeFlags: edgeFlags.subarray(0, keptEdges),
    laneOffsets: laneOffsets.subarray(0, keptEdges + 1),
    laneBytes: laneBytes.subarray(0, laneByteCount),
    geomOffsets: graph.geomOffsets.subarray(0, keptEdges + 1),
    geomBytes: graph.geomBytes.subarray(0, geomByteCount),
    names: graph.names,
    profile: graph.profile,
    classes: graph.classes,
    condRules: graph.condRules || [],
    signs: graph.signs || [],
    limits: graph.limits || [],
    charges: graph.charges || [],
    laneSigns: graph.laneSigns || [],
    banRules: graph.banRules || [],
    portals: graph.portals || {}
  };
}

export function writeRoadGraph(path, graph) {
  const namesBytes = new TextEncoder().encode(JSON.stringify(graph.names));
  const portalsBytes = new TextEncoder().encode(JSON.stringify(graph.portals || {}));
  const sections = [
    ["nodeLat", graph.nodeLat],
    ["nodeLon", graph.nodeLon],
    ["edgeFrom", graph.edgeFrom],
    ["edgeTo", graph.edgeTo],
    ["edgeWeightDs", graph.edgeWeightDs],
    ["edgeDistDm", graph.edgeDistDm],
    ["edgeName", graph.edgeName],
    ["edgeClass", graph.edgeClass],
    ["edgeJunction", graph.edgeJunction],
    ["edgeSpeed", graph.edgeSpeed],
    ["edgeCond", graph.edgeCond],
    ["edgeSign", graph.edgeSign],
    // What each edge physically will not admit, as an index into the header's
    // table. Omitting this column lost every posted limit between the
    // extractor and the index builder — the tags were read, the table was
    // written, and not one edge pointed at it.
    // Defaulted rather than required: a caller that never read a limit has
    // none, and that is a graph with no posted restrictions rather than a
    // broken one. The column being *absent mid-pipeline* is the failure that
    // matters, and it is caught where the extractor reports its own count.
    ["edgeLimit", graph.edgeLimit ?? new Uint32Array(graph.edgeFrom.length)],
    ["edgeBan", graph.edgeBan ?? new Uint32Array(graph.edgeFrom.length)],
    ["edgeCharge", graph.edgeCharge ?? new Uint32Array(graph.edgeFrom.length)],
    ["edgeLaneSign", graph.edgeLaneSign ?? new Uint32Array(graph.edgeFrom.length)],
    ["edgeLaneMask", graph.edgeLaneMask ?? new Uint32Array(graph.edgeFrom.length)],
    ["edgeFlags", graph.edgeFlags],
    ["laneOffsets", graph.laneOffsets],
    ["laneBytes", graph.laneBytes],
    ["geomOffsets", graph.geomOffsets],
    ["geomBytes", graph.geomBytes],
    ["namesBytes", namesBytes],
    ["portalsBytes", portalsBytes]
  ];
  const header = {
    format: ROAD_SOURCE_FORMAT,
    // The distinct conditional windows an edge's condRule indexes into. A
    // whole province shares a handful, so they ride in the header rather than
    // being repeated per edge.
    condRules: graph.condRules || [],
    // What the green panels say, as distinct sign faces an edge names by
    // index. A province repeats a few thousand between all its motorways.
    signs: graph.signs || [],
    limits: graph.limits || [],
    // When each turn is shut, for the turns that are shut on a schedule.
    banRules: graph.banRules || [],
    // What the tolled and ferried stretches cost, where OSM says.
    charges: graph.charges || [],
    // What the overhead panel says above each lane of an approach.
    laneSigns: graph.laneSigns || [],
    nodes: graph.nodeLat.length,
    edges: graph.edgeFrom.length,
    profile: graph.profile || "car",
    classes: graph.classes || [],
    sections: sections.map(([name, array]) => ({
      name,
      bytes: array.byteLength,
      type: array.constructor.name
    }))
  };
  const headerBytes = new TextEncoder().encode(JSON.stringify(header) + "\n");

  // Do not Buffer.concat the graph. Brazil's expanded car graph is multiple
  // GiB; concatenating every section retained the graph and allocated a full
  // second copy before issuing the first write, which pushed the production
  // worker above MemoryHigh and left it indefinitely cgroup-throttled. Write
  // zero-copy typed-array views in format order instead. A sibling temporary
  // file keeps the destination atomic if the process is interrupted.
  const temporary = `${path}.tmp-${process.pid}-${Date.now()}`;
  let fd = -1;
  const writeAll = bytes => {
    let offset = 0;
    while (offset < bytes.byteLength) {
      offset += writeSync(fd, bytes, offset, bytes.byteLength - offset);
    }
  };
  try {
    fd = openSync(temporary, "wx");
    writeAll(headerBytes);
    for (const [, array] of sections) {
      writeAll(new Uint8Array(array.buffer, array.byteOffset, array.byteLength));
    }
    fsyncSync(fd);
    closeSync(fd);
    fd = -1;
    renameSync(temporary, path);
  } catch (error) {
    if (fd >= 0) {
      try { closeSync(fd); } catch {}
    }
    try { unlinkSync(temporary); } catch {}
    throw error;
  }
}

const TYPED_ARRAYS = { Int32Array, Uint32Array, Uint8Array };
const ROAD_GRAPH_READ_CHUNK_BYTES = 64 * 1024 * 1024;
const ROAD_GRAPH_MAX_HEADER_BYTES = 64 * 1024 * 1024;

/** Read exactly one graph section without asking Node to create a whole-file Buffer. */
export function readRoadGraphBytes(fd, target, position, chunkBytes = ROAD_GRAPH_READ_CHUNK_BYTES) {
  let offset = 0;
  while (offset < target.byteLength) {
    const length = Math.min(chunkBytes, target.byteLength - offset);
    const read = readSync(fd, target, offset, length, position + offset);
    if (read === 0) throw new Error(`Truncated road graph at byte ${position + offset}.`);
    offset += read;
  }
  return target;
}

function readRoadGraphHeader(fd, fileBytes) {
  const chunks = [];
  let position = 0;
  while (position < fileBytes && position < ROAD_GRAPH_MAX_HEADER_BYTES) {
    const length = Math.min(64 * 1024, fileBytes - position, ROAD_GRAPH_MAX_HEADER_BYTES - position);
    const chunk = Buffer.allocUnsafe(length);
    const read = readSync(fd, chunk, 0, length, position);
    if (read === 0) break;
    const newline = chunk.subarray(0, read).indexOf(0x0a);
    if (newline >= 0) {
      chunks.push(chunk.subarray(0, newline));
      return {
        header: JSON.parse(Buffer.concat(chunks).toString("utf8")),
        dataOffset: position + newline + 1
      };
    }
    chunks.push(chunk.subarray(0, read));
    position += read;
  }
  throw new Error(`Road graph header is missing or exceeds ${ROAD_GRAPH_MAX_HEADER_BYTES} bytes.`);
}

export async function readRoadGraph(path) {
  const fd = openSync(path, "r");
  try {
    const fileBytes = fstatSync(fd).size;
    const { header, dataOffset } = readRoadGraphHeader(fd, fileBytes);
    if (header.format !== ROAD_SOURCE_FORMAT) throw new Error(`Unsupported road graph format: ${header.format} (re-run the extractor).`);
    const graph = {
      profile: header.profile || "car",
      classes: header.classes || [],
      condRules: header.condRules || [],
      signs: header.signs || [],
      limits: header.limits || [],
      banRules: header.banRules || [],
      charges: header.charges || [],
      laneSigns: header.laneSigns || []
    };
    let offset = dataOffset;
    for (const section of header.sections) {
      if (!Number.isSafeInteger(section.bytes) || section.bytes < 0 || offset + section.bytes > fileBytes) {
        throw new Error(`Invalid or truncated road graph section: ${section.name}.`);
      }
      const bytes = readRoadGraphBytes(fd, new Uint8Array(section.bytes), offset);
      offset += section.bytes;
      if (section.name === "namesBytes") {
        graph.names = JSON.parse(Buffer.from(bytes.buffer).toString("utf8"));
        continue;
      }
      if (section.name === "portalsBytes") {
        graph.portals = JSON.parse(Buffer.from(bytes.buffer).toString("utf8"));
        continue;
      }
      const Type = TYPED_ARRAYS[section.type];
      if (!Type || section.bytes % Type.BYTES_PER_ELEMENT !== 0) {
        throw new Error(`Invalid road graph section type: ${section.name} (${section.type}).`);
      }
      graph[section.name] = new Type(bytes.buffer);
    }
    return graph;
  } finally {
    closeSync(fd);
  }
}

const invokedAsScript = process.argv[1] && import.meta.url.endsWith(process.argv[1].split("/").pop());
if (invokedAsScript && process.argv[2] && process.argv[3]) {
  const started = Date.now();
  const profileFlag = process.argv.indexOf("--profile");
  const profileName = profileFlag > 0 ? process.argv[profileFlag + 1] : "car";
  const graph = extractRoadGraph(process.argv[2], {
    log: message => console.log(message),
    profile: profileName,
    turnCosts: !process.argv.includes("--no-turn-costs")
  });
  writeRoadGraph(process.argv[3], graph);
  console.log(`graph (${graph.profile}): ${graph.nodeLat.length} nodes, ${graph.edgeFrom.length} edges, ${graph.names.length} names`);
  console.log(`wrote ${process.argv[3]} in ${((Date.now() - started) / 1000).toFixed(1)}s`);
}
