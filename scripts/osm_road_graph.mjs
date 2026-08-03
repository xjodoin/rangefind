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

const BIKE_SPEEDS = {
  cycleway: 18,
  primary: 16,
  primary_link: 16,
  secondary: 17,
  secondary_link: 17,
  tertiary: 17,
  tertiary_link: 17,
  unclassified: 17,
  residential: 16,
  living_street: 14,
  service: 14,
  track: 12,
  path: 12,
  footway: 8,
  pedestrian: 8,
  road: 15
};

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

const ACCESS_DENIED = new Set(["no", "private", "delivery", "agricultural", "forestry", "military", "customers"]);

// Fixed junction penalties in deciseconds, applied to every edge entering a
// tagged node. Signals dominate systematic urban ETA error.
const CAR_NODE_PENALTIES = { traffic_signals: 100, stop: 20, give_way: 10, level_crossing: 60 };
const BIKE_NODE_PENALTIES = { traffic_signals: 80, stop: 15, give_way: 5, level_crossing: 40 };
const FOOT_NODE_PENALTIES = { traffic_signals: 60, level_crossing: 30 };

function nodePenaltyDs(profile, tags) {
  if (!tags) return 0;
  const table = profile.nodePenalties;
  const highway = tags.get("highway");
  if (highway && table[highway]) return table[highway];
  if (tags.get("railway") === "level_crossing") return table.level_crossing || 0;
  return 0;
}

export const PROFILES = {
  car: {
    name: "car",
    speeds: CAR_SPEEDS,
    nodePenalties: CAR_NODE_PENALTIES,
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
    maxSpeedKmh: 30,
    allowed(tags) {
      const highway = tags.get("highway");
      if (!highway || !(highway in BIKE_SPEEDS)) return false;
      if (tags.get("area") === "yes") return false;
      const bicycle = tags.get("bicycle");
      if (bicycle != null) return !ACCESS_DENIED.has(bicycle) && bicycle !== "use_sidepath";
      if (highway === "footway" || highway === "pedestrian") return false;
      const access = tags.get("access");
      if (access != null && ACCESS_DENIED.has(access)) return false;
      return true;
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
    maxSpeedKmh: 6,
    allowed(tags) {
      const highway = tags.get("highway");
      if (!highway || !(highway in FOOT_SPEEDS)) return false;
      if (tags.get("area") === "yes") return false;
      const foot = tags.get("foot");
      if (foot != null) return !ACCESS_DENIED.has(foot);
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

// Surfaces where posted class speeds are unrealistic for a car.
const SLOW_SURFACES = new Set(["unpaved", "gravel", "fine_gravel", "dirt", "earth", "ground", "grass", "sand", "mud", "compacted", "pebblestone", "wood"]);
const VERY_SLOW_SMOOTHNESS = new Set(["bad", "very_bad", "horrible", "very_horrible", "impassable"]);

// Per-direction km/h for a way: profile class default, capped by maxspeed
// tags (car only), degraded by surface and smoothness.
function waySpeeds(tags, profile) {
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
  return {
    forward: Math.min(forward, profile.maxSpeedKmh),
    backward: Math.min(backward, profile.maxSpeedKmh)
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
  if (viaWays.length === 1 && viaNode == null) {
    return { kind, fromWay, toWay, viaWay: viaWays[0], only: kind.startsWith("only_") };
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

export function extractRoadGraph(pbfPath, { log = () => {}, profile: profileName = "car" } = {}) {
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
        oneway: profile.oneway(tags),
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
  const viaWayKept = restrictions.filter(restriction => restriction.viaWay != null).length;
  log(`ways: kept ${ways.length} of ${wayCount}`);
  log(`restrictions: ${restrictions.length - viaWayKept} via-node + ${viaWayKept} single-via-way kept, ${viaWayRestrictions} multi/mixed-via skipped`);

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
  scanPbf(pbfPath, {
    onNode(id, lat, lon, tags) {
      const index = binarySearch(usedIds, id);
      if (index < 0) return;
      latE7[index] = toE7(lat);
      lonE7[index] = toE7(lon);
      found[index] = 1;
      if (tags) penaltyDs[index] = nodePenaltyDs(profile, tags);
    }
  });

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
  const geomOffsets = [0];
  const geomBytes = new GrowUint8();
  const geomScratch = [];
  const emitEdge = (from, to, weightDs, distDm, nameId, wayId, classCode, points, reversed) => {
    edgeFrom.push(from);
    edgeTo.push(to);
    edgeWeightDs.push(weightDs);
    edgeDistDm.push(distDm);
    edgeName.push(nameId);
    edgeWay.push(wayId);
    edgeClass.push(classCode);
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
  };

  const segment = [];
  for (const way of ways) {
    segment.length = 0;
    let fromNode = -1;
    let fromPenalty = 0;
    let lengthMeters = 0;
    for (let i = 0; i < way.refCount; i++) {
      const ref = allRefs[way.refStart + i];
      const used = binarySearch(usedIds, ref);
      if (used < 0 || !found[used]) {
        // Missing node (clipped extract): break the segment here.
        segment.length = 0;
        fromNode = -1;
        lengthMeters = 0;
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
      if (graphNode == null) continue;
      if (fromNode < 0) {
        fromNode = graphNode;
        fromPenalty = penaltyDs[used];
        segment.length = 0;
        segment.push([lat, lon]);
        lengthMeters = 0;
        continue;
      }
      if (graphNode === fromNode && lengthMeters < 0.01) continue;
      const distDm = Math.max(1, Math.round(lengthMeters * 10));
      // Junction penalties charge the edge that enters the tagged node.
      if (way.oneway >= 0) {
        const weightDs = Math.max(1, Math.round((lengthMeters / ((way.speedFwd * 1000) / 3600)) * 10) + penaltyDs[used]);
        emitEdge(fromNode, graphNode, weightDs, distDm, way.nameId, way.id, way.classCode, segment, false);
      }
      if (way.oneway <= 0) {
        const weightDs = Math.max(1, Math.round((lengthMeters / ((way.speedBwd * 1000) / 3600)) * 10) + fromPenalty);
        emitEdge(graphNode, fromNode, weightDs, distDm, way.nameId, way.id, way.classCode, segment, true);
      }
      fromNode = graphNode;
      fromPenalty = penaltyDs[used];
      segment.length = 0;
      segment.push([lat, lon]);
      lengthMeters = 0;
    }
  }
  log(`edges: ${edgeFrom.length} directed`);

  // Turn restrictions by via-node expansion: for every restricted approach
  // (via graph node + from way), the incoming edge is redirected to a copy
  // of the via node whose outgoing edges honor the restriction. The rest of
  // the pipeline (partition, cliques, query) sees plain topology, so
  // restrictions cost nothing at query time.
  applyTurnRestrictions({
    restrictions,
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
    geomOffsets,
    geomBytes,
    log
  });

  return filterLargestScc({
    nodeLat: Int32Array.from(nodeLat),
    nodeLon: Int32Array.from(nodeLon),
    edgeFrom: Uint32Array.from(edgeFrom),
    edgeTo: Uint32Array.from(edgeTo),
    edgeWeightDs: Uint32Array.from(edgeWeightDs),
    edgeDistDm: Uint32Array.from(edgeDistDm),
    edgeName: Uint32Array.from(edgeName),
    edgeClass: Uint8Array.from(edgeClass),
    geomOffsets: Uint32Array.from(geomOffsets),
    geomBytes: Uint8Array.from(geomBytes.view()),
    names,
    profile: profile.name,
    classes: classTable
  }, log);
}

export function applyTurnRestrictions(context) {
  const {
    restrictions, nodeIndex, nodeLat, nodeLon,
    edgeFrom, edgeTo, edgeWeightDs, edgeDistDm, edgeName, edgeWay,
    geomOffsets, geomBytes, log
  } = context;
  const edgeClass = context.edgeClass || null;
  if (!restrictions.length) return;

  const copyEdgeTo = (source, from, toOverride) => {
    edgeFrom.push(from);
    edgeTo.push(toOverride ?? edgeTo[source]);
    edgeWeightDs.push(edgeWeightDs[source]);
    edgeDistDm.push(edgeDistDm[source]);
    edgeName.push(edgeName[source]);
    edgeWay.push(edgeWay[source]);
    if (edgeClass) edgeClass.push(edgeClass[source]);
    const start = geomOffsets[source];
    const end = geomOffsets[source + 1];
    geomBytes.ensure(end - start);
    geomBytes.data.set(geomBytes.data.subarray(start, end), geomBytes.length);
    geomBytes.length += end - start;
    geomOffsets.push(geomBytes.length);
    return edgeFrom.length - 1;
  };

  // --- Single-via-way restrictions: expand the via chain with path memory
  // so only traffic that entered from the from-way sees the restricted exit.
  const viaWayRestrictions = restrictions.filter(restriction => restriction.viaWay != null);
  let viaWayApplied = 0;
  let viaWayUnresolved = 0;
  if (viaWayRestrictions.length) {
    const involvedWays = new Set();
    for (const restriction of viaWayRestrictions) {
      involvedWays.add(restriction.fromWay);
      involvedWays.add(restriction.viaWay);
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
      const viaNodes = nodesOf(restriction.viaWay);
      const entries = [...nodesOf(restriction.fromWay)].filter(node => viaNodes.has(node));
      const exits = [...nodesOf(restriction.toWay)].filter(node => viaNodes.has(node));
      if (entries.length !== 1 || exits.length !== 1 || entries[0] === exits[0]) {
        viaWayUnresolved++;
        continue;
      }
      const entry = entries[0];
      const exit = exits[0];
      // Directed BFS along the via way from entry to exit, capped.
      const parentEdge = new Map([[entry, -1]]);
      let frontier = [entry];
      let found = false;
      for (let depth = 0; depth < 8 && frontier.length && !found; depth++) {
        const next = [];
        for (const node of frontier) {
          for (const e of wayEdges.get(restriction.viaWay) || []) {
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
    if (restriction.viaWay != null) continue;
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
  const geomOffsets = [0];
  const geomBytes = new GrowUint8();
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
    const start = graph.geomOffsets[i];
    const end = graph.geomOffsets[i + 1];
    geomBytes.ensure(end - start);
    geomBytes.data.set(graph.geomBytes.subarray(start, end), geomBytes.length);
    geomBytes.length += end - start;
    geomOffsets.push(geomBytes.length);
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
    geomOffsets: Uint32Array.from(geomOffsets),
    geomBytes: Uint8Array.from(geomBytes.view()),
    names: graph.names,
    profile: graph.profile,
    classes: graph.classes
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
    ["geomOffsets", graph.geomOffsets],
    ["geomBytes", graph.geomBytes],
    ["namesBytes", namesBytes]
  ];
  const header = {
    format: "rfroutesrc-v2",
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
  if (header.format !== "rfroutesrc-v2") throw new Error(`Unsupported road graph format: ${header.format} (re-run the extractor).`);
  const graph = { profile: header.profile || "car", classes: header.classes || [] };
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
  const graph = extractRoadGraph(process.argv[2], { log: message => console.log(message), profile: profileName });
  writeRoadGraph(process.argv[3], graph);
  console.log(`graph (${graph.profile}): ${graph.nodeLat.length} nodes, ${graph.edgeFrom.length} edges, ${graph.names.length} names`);
  console.log(`wrote ${process.argv[3]} in ${((Date.now() - started) / 1000).toFixed(1)}s`);
}
