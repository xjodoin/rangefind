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

import { writeFileSync } from "node:fs";
import { scanPbf } from "./osm_pbf.mjs";

const EARTH_RADIUS_METERS = 6371008.7714;

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
  road: 30
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
  road: 14
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
  road: 5
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

function destinationOnly(tags) {
  for (const key of ["motor_vehicle", "vehicle", "access"]) {
    const value = tags.get(key);
    if (value != null) return ACCESS_DESTINATION_ONLY.has(value);
  }
  return false;
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
const CAR_NODE_PENALTIES = { traffic_signals: 100, stop: 20, give_way: 10, level_crossing: 60, crossing_signals: 50 };
const BIKE_NODE_PENALTIES = { traffic_signals: 80, stop: 15, give_way: 5, level_crossing: 40, crossing_signals: 40 };
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
 */
export function chargeEachIntersectionOnce(usedIds, latE7, lonE7, penaltyDs, kindCode, log) {
  const MERGE_METERS = 30;
  const charged = [];
  const grid = new Map();
  // Cell side comfortably over the merge radius, so a neighbour can only be in
  // this cell or one adjacent to it.
  const CELL_E7 = 5000;
  const key = (cx, cy) => `${cx},${cy}`;

  const candidates = [];
  for (let i = 0; i < penaltyDs.length; i++) if (penaltyDs[i] > 0) candidates.push(i);
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
  log(`junction penalties: ${charged.length} intersections charged, ${merged} duplicate nodes absorbed`);
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
export const JUNCTION_KINDS = { traffic_signals: 1, stop: 2, give_way: 3, level_crossing: 4, crossing: 5 };

function nodeKindCode(tags) {
  if (!tags) return 0;
  const highway = tags.get("highway");
  // A signal-controlled pedestrian crossing is a signal for a driver.
  if (highway === "crossing" && tags.get("crossing") === "traffic_signals") {
    return JUNCTION_KINDS.traffic_signals;
  }
  if (highway && JUNCTION_KINDS[highway]) return JUNCTION_KINDS[highway];
  if (tags.get("railway") === "level_crossing") return JUNCTION_KINDS.level_crossing;
  return 0;
}

// Importance when several tagged nodes fall on one edge: signals > stop >
// level crossing > give way > plain crossing.
const JUNCTION_RANK = [0, 5, 4, 2, 3, 1];

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
      const highway = tags.get("highway");
      if (!highway || !(highway in BIKE_SPEEDS)) return false;
      if (tags.get("area") === "yes") return false;
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
      const highway = tags.get("highway");
      if (!highway || !(highway in FOOT_SPEEDS)) return false;
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
export function parseConditionalMaxspeed(value) {
  if (!value) return null;
  const text = String(value).trim();
  const at = text.indexOf("@");
  if (at < 0) return null;

  const speedKmh = parseMaxspeed(text.slice(0, at));
  if (!speedKmh) return null;

  // Parentheses are optional in the wild, and so are the separators inside.
  let condition = text.slice(at + 1).trim().replace(/^\(|\)$/g, "").trim();
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
    speedKmh: Math.round(speedKmh),
    // No day range means every day, which is what a bare time range says.
    days: days ?? 0b1111111,
    startMinute,
    endMinute,
    monthStart: months ? months.start : 1,
    monthEnd: months ? months.end : 12
  };
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

export function waySpeeds(tags, profile) {
  const base = profile.speeds[tags.get("highway")];
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
function parseRestriction(tags, members) {
  const kind = tags.get("restriction:motorcar") || tags.get("restriction");
  if (!kind || tags.get("restriction:conditional")) return null;
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
  if (viaNode != null && !viaWays.length) {
    return { kind, fromWay, toWay, viaNode, only: kind.startsWith("only_") };
  }
  // Via ways (possibly several, possibly with a via node on the chain that
  // we can ignore): the union of their edges defines the restricted path.
  if (viaWays.length >= 1 && viaWays.length <= 4) {
    return { kind, fromWay, toWay, viaWays, only: kind.startsWith("only_") };
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

function carAllowed(tags) {
  const highway = tags.get("highway");
  if (!highway || !(highway in CAR_SPEEDS)) return false;
  if (tags.get("area") === "yes") return false;
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

export function extractRoadGraph(pbfPath, options = {}) {
  const log = options.log || (() => {});
  const profileName = options.profile || "car";
  const profile = PROFILES[profileName];
  if (!profile) throw new Error(`Unknown routing profile "${profileName}" (car, bike, foot).`);
  const classTable = Object.keys(profile.speeds);
  const classCodes = new Map(classTable.map((name, index) => [name, index]));
  // Turn restrictions target motor vehicles; bicycles are commonly excepted
  // and pedestrians always are, so only the car profile applies them.
  const useRestrictions = profile.name === "car";

  // Pass 1: allowed ways and turn-restriction relations. Refs go into one
  // shared spool; endpoints are pushed twice so the duplicate scan below
  // marks them as graph nodes for free.
  const refSpool = new GrowFloat64();
  const junctionSpool = new GrowFloat64();
  // Distinct conditional windows, shared across the whole extract. Declared
  // with the way spool because the way pass is what fills it.
  const conditionalTable = makeConditionalTable();
  const ways = [];
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
      const name = tags.get("name") || tags.get("ref") || "";
      let nameId = nameIds.get(name);
      if (nameId == null) {
        nameId = names.length;
        names.push(name);
        nameIds.set(name, nameId);
      }
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
        oneway: profile.oneway(tags),
        lanes: wayLanes(tags, profile.oneway(tags)),
        nameId,
        classCode: classCodes.get(tags.get("highway")) ?? 0
      });
    },
    onRelation(id, members, tags) {
      if (!useRestrictions || tags.get("type") !== "restriction") return;
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
  log(`restrictions: ${restrictions.length - viaWayKept} via-node + ${viaWayKept} via-way kept, ${viaWayRestrictions} oversized-via skipped`);

  // Junctions: any ref appearing more than once across the kept ways.
  const allRefs = refSpool.view();
  const usedIds = sortedUnique(allRefs);
  {
    const sorted = Float64Array.from(allRefs);
    sorted.sort();
    for (let i = 1; i < sorted.length; i++) {
      if (sorted[i] === sorted[i - 1] && (i < 2 || sorted[i] !== sorted[i - 2])) junctionSpool.push(sorted[i]);
    }
  }
  const junctionIds = sortedUnique(junctionSpool.view());
  log(`nodes: ${usedIds.length} used, ${junctionIds.length} junctions`);

  // Pass 2: coordinates and junction penalties for every used node.
  const latE7 = new Int32Array(usedIds.length);
  const lonE7 = new Int32Array(usedIds.length);
  const found = new Uint8Array(usedIds.length);
  const penaltyDs = new Uint16Array(usedIds.length);
  const kindCode = new Uint8Array(usedIds.length);
  scanPbf(pbfPath, {
    onNode(id, lat, lon, tags) {
      const index = binarySearch(usedIds, id);
      if (index < 0) return;
      latE7[index] = toE7(lat);
      lonE7[index] = toE7(lon);
      found[index] = 1;
      if (tags) {
        penaltyDs[index] = nodePenaltyDs(profile, tags);
        kindCode[index] = nodeKindCode(tags);
      }
    }
  });

  chargeEachIntersectionOnce(usedIds, latE7, lonE7, penaltyDs, kindCode, log);

  // Graph nodes are junctions with a known coordinate, numbered by sorted id.
  const nodeIndex = new Map();
  const nodeLat = [];
  const nodeLon = [];
  for (let i = 0; i < junctionIds.length; i++) {
    const used = binarySearch(usedIds, junctionIds[i]);
    if (used < 0 || !found[used]) continue;
    nodeIndex.set(junctionIds[i], nodeLat.length);
    nodeLat.push(latE7[used]);
    nodeLon.push(lonE7[used]);
  }

  // Split ways into directed edges at junction nodes.
  const edgeFrom = [];
  const edgeTo = [];
  const edgeWeightDs = [];
  const edgeDistDm = [];
  const edgeName = [];
  const edgeWay = [];
  const edgeClass = [];
  const edgeJunction = [];
  const edgeSpeed = [];
  const edgeCond = [];
  const geomOffsets = [0];
  const geomBytes = new GrowUint8();
  const geomScratch = [];
  // Lane movements, jagged the same way the geometry is: most edges carry
  // none, so an empty list costs the single byte that says so.
  const laneOffsets = [0];
  const laneBytes = new GrowUint8();
  const emitEdge = (from, to, weightDs, distDm, nameId, wayId, classCode, junctionKind, postedKmh, condRule, lanes, points, reversed) => {
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
    const laneList = lanes || [];
    laneBytes.ensure(laneList.length + 1);
    laneBytes.data[laneBytes.length++] = Math.min(laneList.length, 255);
    for (let i = 0; i < laneList.length && i < 255; i++) {
      laneBytes.data[laneBytes.length++] = laneList[i] & 0xff;
    }
    laneOffsets.push(laneBytes.length);
  };

  const segment = [];
  for (const way of ways) {
    segment.length = 0;
    let fromNode = -1;
    let fromPenalty = 0;
    let fromKind = 0;
    let lengthMeters = 0;
    // Signals and stops usually sit on interior way nodes, not on the
    // shared junction node; carry their delay and kind onto the edge.
    let segPenalty = 0;
    let segKind = 0;
    let segKindIndex = 0;
    for (let i = 0; i < way.refCount; i++) {
      const ref = allRefs[way.refStart + i];
      const used = binarySearch(usedIds, ref);
      if (used < 0 || !found[used]) {
        // Missing node (clipped extract): break the segment here.
        segment.length = 0;
        fromNode = -1;
        lengthMeters = 0;
        segPenalty = 0;
        segKind = 0;
        continue;
      }
      const lat = latE7[used];
      const lon = lonE7[used];
      if (segment.length) {
        const prev = segment[segment.length - 1];
        lengthMeters += haversineMetersE7(prev[0], prev[1], lat, lon);
      }
      segment.push([lat, lon]);
      const graphNode = nodeIndex.get(ref);
      if (graphNode == null) {
        segPenalty += penaltyDs[used];
        if (JUNCTION_RANK[kindCode[used]] > JUNCTION_RANK[segKind]) {
          segKind = kindCode[used];
          segKindIndex = segment.length - 1;
        }
        continue;
      }
      if (fromNode < 0) {
        fromNode = graphNode;
        fromPenalty = penaltyDs[used];
        fromKind = kindCode[used];
        segment.length = 0;
        segment.push([lat, lon]);
        lengthMeters = 0;
        segPenalty = 0;
        segKind = 0;
        continue;
      }
      if (graphNode === fromNode && lengthMeters < 0.01) continue;
      const distDm = Math.max(1, Math.round(lengthMeters * 10));
      const lastIndex = segment.length - 1;
      // Pick the most important junction on this edge per direction, with
      // its polyline point index packed alongside (kind + index * 8).
      const pick = (endKind, endIndex) => {
        if (JUNCTION_RANK[segKind] > JUNCTION_RANK[endKind]) return { kind: segKind, index: segKindIndex };
        return endKind ? { kind: endKind, index: endIndex } : { kind: 0, index: 0 };
      };
      if (way.oneway >= 0) {
        const junction = pick(kindCode[used], lastIndex);
        const weightDs = Math.max(1, Math.round((lengthMeters / ((way.speedFwd * 1000) / 3600)) * 10) + penaltyDs[used] + segPenalty);
        emitEdge(fromNode, graphNode, weightDs, distDm, way.nameId, way.id, way.classCode, junction.kind + junction.index * 8, way.postedFwd, way.condRule, way.lanes.forward, segment, false);
      }
      if (way.oneway <= 0) {
        const junction = pick(fromKind, lastIndex);
        // Reversed polyline: mirror the point index.
        const mirrored = junction.kind ? { kind: junction.kind, index: lastIndex - junction.index } : junction;
        const weightDs = Math.max(1, Math.round((lengthMeters / ((way.speedBwd * 1000) / 3600)) * 10) + fromPenalty + segPenalty);
        emitEdge(graphNode, fromNode, weightDs, distDm, way.nameId, way.id, way.classCode, mirrored.kind + mirrored.index * 8, way.postedBwd, way.condRule, way.lanes.backward, segment, true);
      }
      fromNode = graphNode;
      fromPenalty = penaltyDs[used];
      fromKind = kindCode[used];
      segment.length = 0;
      segment.push([lat, lon]);
      lengthMeters = 0;
      segPenalty = 0;
      segKind = 0;
    }
  }
  log(`edges: ${edgeFrom.length} directed`);

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
    geomOffsets,
    geomBytes,
    laneOffsets,
    laneBytes,
    log
  };
  applyTurnRestrictions(context);

  if (useTurnCosts) {
    const expanded = expandTurnCosts({ ...context, restrictions }, profile.turnCosts);
    return filterLargestScc({
      nodeLat: expanded.nodeLat,
      nodeLon: expanded.nodeLon,
      edgeFrom: Uint32Array.from(expanded.edgeFrom),
      edgeTo: Uint32Array.from(expanded.edgeTo),
      edgeWeightDs: Uint32Array.from(expanded.edgeWeightDs),
      edgeDistDm: Uint32Array.from(expanded.edgeDistDm),
      edgeName: Uint32Array.from(expanded.edgeName),
      edgeClass: Uint8Array.from(expanded.edgeClass),
      edgeJunction: Uint8Array.from(expanded.edgeJunction),
      edgeSpeed: Uint8Array.from(expanded.edgeSpeed),
      edgeCond: Uint8Array.from(expanded.edgeCond || []),
      geomOffsets: Uint32Array.from(expanded.geomOffsets),
      geomBytes: Uint8Array.from(expanded.geomBytes.view()),
      laneOffsets: Uint32Array.from(expanded.laneOffsets),
      laneBytes: Uint8Array.from(expanded.laneBytes.view()),
      names,
      profile: profile.name,
      classes: classTable,
      condRules: conditionalTable.rules
    }, log);
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
    laneOffsets: Uint32Array.from(laneOffsets),
    laneBytes: Uint8Array.from(laneBytes.view()),
    geomOffsets: Uint32Array.from(geomOffsets),
    geomBytes: Uint8Array.from(geomBytes.view()),
    names,
    profile: profile.name,
    classes: classTable,
    condRules: conditionalTable.rules
  }, log);
}

export function applyTurnRestrictions(context) {
  const {
    restrictions, nodeIndex, nodeLat, nodeLon,
    edgeFrom, edgeTo, edgeWeightDs, edgeDistDm, edgeName, edgeWay,
    geomOffsets, geomBytes, laneOffsets, laneBytes, log
  } = context;
  const edgeClass = context.edgeClass || null;
  const edgeJunction = context.edgeJunction || null;
  const edgeSpeed = context.edgeSpeed || null;
  const edgeCond = context.edgeCond || null;
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
    restrictions, nodeIndex, nodeLat, nodeLon,
    edgeFrom, edgeTo, edgeWeightDs, edgeDistDm, edgeName, edgeWay, edgeClass, edgeJunction,
    edgeSpeed, edgeCond, geomOffsets, geomBytes, laneOffsets, laneBytes, log
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

  // Via-node restrictions grouped by (junction, approach way).
  const byVia = new Map();
  for (const restriction of restrictions) {
    if (restriction.viaWays != null || restriction.viaNode == null) continue;
    const via = nodeIndex.get(restriction.viaNode);
    if (via == null) continue;
    const key = `${via}:${restriction.fromWay}`;
    let group = byVia.get(key);
    if (!group) byVia.set(key, (group = { onlys: [], nos: [] }));
    (restriction.only ? group.onlys : group.nos).push(restriction);
  }
  const allowed = (inEdge, outEdge) => {
    const group = byVia.get(`${edgeFrom[outEdge]}:${edgeWay[inEdge]}`);
    if (!group) return true;
    const way = edgeWay[outEdge];
    if (group.onlys.length) return group.onlys.some(only => way === only.toWay);
    for (const no of group.nos) {
      if (way !== no.toWay) continue;
      if (no.toWay === no.fromWay && edgeTo[outEdge] !== edgeFrom[inEdge]) continue;
      return false;
    }
    return true;
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
  const newFrom = [];
  const newTo = [];
  const newWeight = [];
  const newDist = [];
  const newName = [];
  const newClass = [];
  const newJunction = [];
  const newSpeed = [];
  const newCond = [];
  const newGeomOffsets = [0];
  const newGeomBytes = new GrowUint8();
  const newLaneOffsets = [0];
  const newLaneBytes = new GrowUint8();
  const laneData = laneBytes ? laneBytes.view() : null;
  let filteredTurns = 0;
  for (let out = 0; out < edgeCount; out++) {
    const junction = edgeFrom[out];
    for (let slot = inStart[junction]; slot < inStart[junction + 1]; slot++) {
      const inEdge = inEdges[slot];
      if (!allowed(inEdge, out)) {
        filteredTurns++;
        continue;
      }
      newFrom.push(inEdge);
      newTo.push(out);
      newWeight.push(edgeWeightDs[out] + turnCostFor(inEdge, out));
      newDist.push(edgeDistDm[out]);
      newName.push(edgeName[out]);
      newClass.push(edgeClass[out]);
      newJunction.push(edgeJunction ? edgeJunction[out] : 0);
      newSpeed.push(edgeSpeed ? edgeSpeed[out] : 0);
      newCond.push(edgeCond ? edgeCond[out] : 0);
      const start = geomOffsets[out];
      const end = geomOffsets[out + 1];
      newGeomBytes.ensure(end - start);
      newGeomBytes.data.set(geomData.subarray(start, end), newGeomBytes.length);
      newGeomBytes.length += end - start;
      newGeomOffsets.push(newGeomBytes.length);
      if (laneData && laneOffsets) {
        const laneStart = laneOffsets[out];
        const laneEnd = laneOffsets[out + 1];
        newLaneBytes.ensure(laneEnd - laneStart);
        newLaneBytes.data.set(laneData.subarray(laneStart, laneEnd), newLaneBytes.length);
        newLaneBytes.length += laneEnd - laneStart;
      } else {
        newLaneBytes.ensure(1);
        newLaneBytes.data[newLaneBytes.length++] = 0;
      }
      newLaneOffsets.push(newLaneBytes.length);
    }
  }
  log(`turn costs: ${edgeCount} junction copies, ${newFrom.length} expanded edges, ${filteredTurns} restricted turns filtered, ${byVia.size} restricted approaches`);
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
function filterLargestScc(graph, log) {
  const nodeCount = graph.nodeLat.length;
  const forward = buildCsr(nodeCount, graph.edgeFrom, graph.edgeTo);
  const backward = buildCsr(nodeCount, graph.edgeTo, graph.edgeFrom);
  const order = new Uint32Array(nodeCount);
  let orderLength = 0;
  {
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
  }
  const component = new Int32Array(nodeCount).fill(-1);
  let componentCount = 0;
  let bestComponent = -1;
  let bestSize = 0;
  {
    const stack = new Uint32Array(nodeCount);
    for (let i = orderLength - 1; i >= 0; i--) {
      const root = order[i];
      if (component[root] >= 0) continue;
      const id = componentCount++;
      let size = 0;
      let top = 0;
      stack[top++] = root;
      component[root] = id;
      while (top > 0) {
        const node = stack[--top];
        size++;
        for (let e = backward.rowStart[node]; e < backward.rowStart[node + 1]; e++) {
          const next = backward.targets[e];
          if (component[next] < 0) {
            component[next] = id;
            stack[top++] = next;
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

  const remap = new Int32Array(nodeCount).fill(-1);
  let keptNodes = 0;
  for (let i = 0; i < nodeCount; i++) {
    if (component[i] === bestComponent) remap[i] = keptNodes++;
  }
  const nodeLat = new Int32Array(keptNodes);
  const nodeLon = new Int32Array(keptNodes);
  for (let i = 0; i < nodeCount; i++) {
    if (remap[i] >= 0) {
      nodeLat[remap[i]] = graph.nodeLat[i];
      nodeLon[remap[i]] = graph.nodeLon[i];
    }
  }
  const edgeFrom = [];
  const edgeTo = [];
  const edgeWeightDs = [];
  const edgeDistDm = [];
  const edgeName = [];
  const edgeClass = [];
  const edgeJunction = [];
  const edgeSpeed = [];
  const edgeCond = [];
  const geomOffsets = [0];
  const geomBytes = new GrowUint8();
  const laneOffsets = [0];
  const laneBytes = new GrowUint8();
  for (let i = 0; i < graph.edgeFrom.length; i++) {
    const from = remap[graph.edgeFrom[i]];
    const to = remap[graph.edgeTo[i]];
    if (from < 0 || to < 0) continue;
    edgeFrom.push(from);
    edgeTo.push(to);
    edgeWeightDs.push(graph.edgeWeightDs[i]);
    edgeDistDm.push(graph.edgeDistDm[i]);
    edgeName.push(graph.edgeName[i]);
    edgeClass.push(graph.edgeClass[i]);
    edgeJunction.push(graph.edgeJunction[i]);
    edgeSpeed.push(graph.edgeSpeed ? graph.edgeSpeed[i] : 0);
    edgeCond.push(graph.edgeCond ? graph.edgeCond[i] : 0);
    const start = graph.geomOffsets[i];
    const end = graph.geomOffsets[i + 1];
    geomBytes.ensure(end - start);
    geomBytes.data.set(graph.geomBytes.subarray(start, end), geomBytes.length);
    geomBytes.length += end - start;
    geomOffsets.push(geomBytes.length);
    if (graph.laneOffsets && graph.laneBytes) {
      const laneStart = graph.laneOffsets[i];
      const laneEnd = graph.laneOffsets[i + 1];
      laneBytes.ensure(laneEnd - laneStart);
      laneBytes.data.set(graph.laneBytes.subarray(laneStart, laneEnd), laneBytes.length);
      laneBytes.length += laneEnd - laneStart;
    } else {
      laneBytes.ensure(1);
      laneBytes.data[laneBytes.length++] = 0;
    }
    laneOffsets.push(laneBytes.length);
  }
  return {
    nodeLat,
    nodeLon,
    edgeFrom: Uint32Array.from(edgeFrom),
    edgeTo: Uint32Array.from(edgeTo),
    edgeWeightDs: Uint32Array.from(edgeWeightDs),
    edgeDistDm: Uint32Array.from(edgeDistDm),
    edgeName: Uint32Array.from(edgeName),
    edgeClass: Uint8Array.from(edgeClass),
    edgeJunction: Uint8Array.from(edgeJunction),
    edgeSpeed: Uint8Array.from(edgeSpeed),
    edgeCond: Uint8Array.from(edgeCond),
    laneOffsets: Uint32Array.from(laneOffsets),
    laneBytes: Uint8Array.from(laneBytes.view()),
    geomOffsets: Uint32Array.from(geomOffsets),
    geomBytes: Uint8Array.from(geomBytes.view()),
    names: graph.names,
    profile: graph.profile,
    classes: graph.classes,
    condRules: graph.condRules || []
  };
}

export function writeRoadGraph(path, graph) {
  const namesBytes = new TextEncoder().encode(JSON.stringify(graph.names));
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
    ["laneOffsets", graph.laneOffsets],
    ["laneBytes", graph.laneBytes],
    ["geomOffsets", graph.geomOffsets],
    ["geomBytes", graph.geomBytes],
    ["namesBytes", namesBytes]
  ];
  const header = {
    format: "rfroutesrc-v6",
    // The distinct conditional windows an edge's condRule indexes into. A
    // whole province shares a handful, so they ride in the header rather than
    // being repeated per edge.
    condRules: graph.condRules || [],
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
  const chunks = [headerBytes];
  for (const [, array] of sections) {
    chunks.push(new Uint8Array(array.buffer, array.byteOffset, array.byteLength));
  }
  writeFileSync(path, Buffer.concat(chunks.map(chunk => Buffer.from(chunk))));
}

const TYPED_ARRAYS = { Int32Array, Uint32Array, Uint8Array };

export async function readRoadGraph(path) {
  const { readFileSync } = await import("node:fs");
  const bytes = readFileSync(path);
  const newline = bytes.indexOf(0x0a);
  const header = JSON.parse(bytes.subarray(0, newline).toString("utf8"));
  if (header.format !== "rfroutesrc-v6") throw new Error(`Unsupported road graph format: ${header.format} (re-run the extractor).`);
  const graph = {
    profile: header.profile || "car",
    classes: header.classes || [],
    condRules: header.condRules || []
  };
  let offset = newline + 1;
  for (const section of header.sections) {
    const slice = bytes.subarray(offset, offset + section.bytes);
    offset += section.bytes;
    if (section.name === "namesBytes") {
      graph.names = JSON.parse(Buffer.from(slice).toString("utf8"));
      continue;
    }
    const Type = TYPED_ARRAYS[section.type];
    // Copy so alignment does not depend on header length.
    const copy = new Uint8Array(section.bytes);
    copy.set(slice);
    graph[section.name] = new Type(copy.buffer);
  }
  return graph;
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
