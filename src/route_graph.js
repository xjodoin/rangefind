// Static route-graph formats (rfroutegraph-v1).
//
// A road network is partitioned into contiguous KD leaf cells over a
// locality-preserving node order, grouped into nested parent cells. Three
// object kinds ship on disk, every one range-addressed, gzipped, and
// SHA-256 checksummed like every other Rangefind lane:
//
// - cell blocks (RFRC): raw directed edges of one leaf cell, with per-edge
//   weights, distances, street-name ids, and polyline geometry;
// - overlay blocks (RFRO): the overlay graph of one parent cell — child
//   boundary-node cliques plus the original edges crossing between children;
// - the root (RFRT): leaf bboxes, node ranges, shard and pack tables, and
//   object pointers for every cell and overlay block.
//
// The query engine (route_graph_query.js) fetches a fixed, provably bounded
// object set per query: the two leaf cells plus one overlay per ancestor
// level per endpoint plus the shared top overlay.

import { fixedWidth, pushVarint, readFixedInt, readVarint, writeFixedInt } from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const ROUTE_GRAPH_FORMAT = "rfroutegraph-v1";
export const ROUTE_ROOT_MAGIC = [0x52, 0x46, 0x52, 0x54]; // RFRT
export const ROUTE_CELL_MAGIC = [0x52, 0x46, 0x52, 0x43]; // RFRC
export const ROUTE_OVERLAY_MAGIC = [0x52, 0x46, 0x52, 0x4f]; // RFRO
export const ROUTE_GEOMETRY_MAGIC = [0x52, 0x46, 0x52, 0x50]; // RFRP
const ROOT_VERSION = 3;
const CELL_VERSION = 5;
const OVERLAY_VERSION = 1;
const GEOMETRY_VERSION = 1;

// Integer time-of-day metric: factors are time multipliers scaled by 1000,
// applied identically at build (cliques) and query (raw edges) so bucketed
// searches stay exact.
export function bucketWeight(weightDs, classCode, factors) {
  const factor = factors[classCode] ?? 1000;
  if (factor === 1000) return weightDs;
  return Math.max(1, Math.round((weightDs * factor) / 1000));
}

function pushZigzag(out, value) {
  pushVarint(out, value < 0 ? -value * 2 - 1 : value * 2);
}

function readZigzag(bytes, state) {
  const raw = readVarint(bytes, state);
  return raw % 2 === 1 ? -(raw + 1) / 2 : raw / 2;
}

function pushChecksum(out, checksum) {
  const hex = checksum?.value || "";
  if (hex.length !== 64) throw new Error("Route graph object pointers require a sha256 checksum.");
  for (let i = 0; i < 32; i++) out.push(parseInt(hex.slice(i * 2, i * 2 + 2), 16));
}

function readChecksum(bytes, state) {
  let hex = "";
  for (let i = 0; i < 32; i++) hex += bytes[state.pos + i].toString(16).padStart(2, "0");
  state.pos += 32;
  return { algorithm: "sha256", value: hex };
}

function pushPointer(out, pointer) {
  pushVarint(out, pointer.packIndex);
  pushVarint(out, pointer.offset);
  pushVarint(out, pointer.length);
  pushVarint(out, pointer.logicalLength ?? 0);
  pushChecksum(out, pointer.checksum);
}

function readPointer(bytes, state) {
  const packIndex = readVarint(bytes, state);
  const offset = readVarint(bytes, state);
  const length = readVarint(bytes, state);
  const logicalLength = readVarint(bytes, state);
  const checksum = readChecksum(bytes, state);
  return { packIndex, offset, length, logicalLength, checksum };
}

// --- Cell block ---------------------------------------------------------
//
// `cell` is { cellId, firstNode, nodeCount, latE7, lonE7, rowStart, targets,
// weights, distsDm, nameIds, classes, extLat, extLon, geomRefs }. Since v3,
// polylines live in a separate per-leaf geometry object (RFRP below) and
// each edge stores a reference: uniqueIndex * 2 + reversedFlag. The search
// phase fetches only these topology blocks; geometry objects are fetched
// for snapping and for the final route corridor.

export function encodeRouteCell(cell) {
  const out = [...ROUTE_CELL_MAGIC];
  pushVarint(out, CELL_VERSION);
  pushVarint(out, cell.cellId);
  pushVarint(out, cell.firstNode);
  pushVarint(out, cell.nodeCount);
  let minLat = Infinity;
  let minLon = Infinity;
  for (let i = 0; i < cell.nodeCount; i++) {
    if (cell.latE7[i] < minLat) minLat = cell.latE7[i];
    if (cell.lonE7[i] < minLon) minLon = cell.lonE7[i];
  }
  if (!cell.nodeCount) {
    minLat = 0;
    minLon = 0;
  }
  pushZigzag(out, minLat);
  pushZigzag(out, minLon);
  const latDeltas = new Array(cell.nodeCount);
  const lonDeltas = new Array(cell.nodeCount);
  for (let i = 0; i < cell.nodeCount; i++) {
    latDeltas[i] = cell.latE7[i] - minLat;
    lonDeltas[i] = cell.lonE7[i] - minLon;
  }
  const latWidth = fixedWidth(latDeltas);
  const lonWidth = fixedWidth(lonDeltas);
  out.push(latWidth, lonWidth);
  const coords = new Uint8Array(cell.nodeCount * (latWidth + lonWidth));
  for (let i = 0; i < cell.nodeCount; i++) {
    writeFixedInt(coords, i * latWidth, latWidth, latDeltas[i]);
    writeFixedInt(coords, cell.nodeCount * latWidth + i * lonWidth, lonWidth, lonDeltas[i]);
  }
  for (const byte of coords) out.push(byte);
  const edgeCount = cell.rowStart[cell.nodeCount];
  pushVarint(out, edgeCount);
  for (let node = 0; node < cell.nodeCount; node++) {
    const start = cell.rowStart[node];
    const end = cell.rowStart[node + 1];
    pushVarint(out, end - start);
    const from = cell.firstNode + node;
    for (let e = start; e < end; e++) {
      pushZigzag(out, cell.targets[e] - from);
      pushVarint(out, cell.weights[e]);
      pushVarint(out, cell.distsDm[e]);
      pushVarint(out, cell.nameIds[e]);
      pushVarint(out, cell.classes ? cell.classes[e] : 0);
      pushVarint(out, cell.junctions ? cell.junctions[e] : 0);
      // Posted limit in km/h, 0 when the way carries no maxspeed tag. Kept
      // apart from the weight, which also folds in surface and junction
      // penalties and so cannot be read back as a legal limit.
      pushVarint(out, cell.speeds ? cell.speeds[e] : 0);
      const external = cell.targets[e] < cell.firstNode || cell.targets[e] >= cell.firstNode + cell.nodeCount;
      if (external) {
        // Cross-cell edges carry their far endpoint so snapping and
        // geometry never need the neighboring cell for this edge alone.
        pushZigzag(out, (cell.extLat ? cell.extLat[e] : 0) - cell.latE7[node]);
        pushZigzag(out, (cell.extLon ? cell.extLon[e] : 0) - cell.lonE7[node]);
      }
      pushVarint(out, cell.geomRefs[e]);
    }
  }
  return Uint8Array.from(out);
}

export function decodeRouteCell(bytes) {
  assertMagic(bytes, ROUTE_CELL_MAGIC, "Invalid Rangefind route cell block.");
  const state = { pos: ROUTE_CELL_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== CELL_VERSION) throw new Error(`Unsupported route cell version ${version}.`);
  const cellId = readVarint(bytes, state);
  const firstNode = readVarint(bytes, state);
  const nodeCount = readVarint(bytes, state);
  const minLat = readZigzag(bytes, state);
  const minLon = readZigzag(bytes, state);
  const latWidth = bytes[state.pos++];
  const lonWidth = bytes[state.pos++];
  const latE7 = new Int32Array(nodeCount);
  const lonE7 = new Int32Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) {
    latE7[i] = minLat + readFixedInt(bytes, state.pos + i * latWidth, latWidth);
    lonE7[i] = minLon + readFixedInt(bytes, state.pos + nodeCount * latWidth + i * lonWidth, lonWidth);
  }
  state.pos += nodeCount * (latWidth + lonWidth);
  const edgeCount = readVarint(bytes, state);
  const rowStart = new Uint32Array(nodeCount + 1);
  const targets = new Uint32Array(edgeCount);
  const weights = new Uint32Array(edgeCount);
  const distsDm = new Uint32Array(edgeCount);
  const nameIds = new Uint32Array(edgeCount);
  const classes = new Uint8Array(edgeCount);
  const junctions = new Uint8Array(edgeCount);
  const speeds = new Uint8Array(edgeCount);
  const extLat = new Int32Array(edgeCount);
  const extLon = new Int32Array(edgeCount);
  const geomRefs = new Uint32Array(edgeCount);
  let cursor = 0;
  for (let node = 0; node < nodeCount; node++) {
    const degree = readVarint(bytes, state);
    rowStart[node + 1] = rowStart[node] + degree;
    const from = firstNode + node;
    for (let i = 0; i < degree; i++) {
      targets[cursor] = from + readZigzag(bytes, state);
      weights[cursor] = readVarint(bytes, state);
      distsDm[cursor] = readVarint(bytes, state);
      nameIds[cursor] = readVarint(bytes, state);
      classes[cursor] = readVarint(bytes, state);
      junctions[cursor] = readVarint(bytes, state);
      speeds[cursor] = readVarint(bytes, state);
      const external = targets[cursor] < firstNode || targets[cursor] >= firstNode + nodeCount;
      if (external) {
        extLat[cursor] = latE7[node] + readZigzag(bytes, state);
        extLon[cursor] = lonE7[node] + readZigzag(bytes, state);
      }
      geomRefs[cursor] = readVarint(bytes, state);
      cursor++;
    }
  }
  if (state.pos !== bytes.length) throw new Error("Trailing bytes in Rangefind route cell block.");
  return { cellId, firstNode, nodeCount, latE7, lonE7, rowStart, targets, weights, distsDm, nameIds, classes, junctions, speeds, extLat, extLon, geomRefs };
}

// --- Geometry object ----------------------------------------------------
//
// One per leaf cell: the deduplicated polylines its edges reference. Each
// polyline is a full point chain (both endpoints included) stored in a
// canonical direction — approach copies of one road edge and the two
// directions of a two-way road all share a single entry. First points are
// delta-encoded against the previous polyline's first point.

export function encodeRouteGeometry(geometry) {
  const out = [...ROUTE_GEOMETRY_MAGIC];
  pushVarint(out, GEOMETRY_VERSION);
  pushVarint(out, geometry.cellId);
  pushVarint(out, geometry.polylines.length);
  let prevLat = 0;
  let prevLon = 0;
  for (const points of geometry.polylines) {
    const count = points.length / 2;
    pushVarint(out, count);
    pushZigzag(out, points[0] - prevLat);
    pushZigzag(out, points[1] - prevLon);
    for (let i = 1; i < count; i++) {
      pushZigzag(out, points[i * 2] - points[(i - 1) * 2]);
      pushZigzag(out, points[i * 2 + 1] - points[(i - 1) * 2 + 1]);
    }
    prevLat = points[0];
    prevLon = points[1];
  }
  return Uint8Array.from(out);
}

export function decodeRouteGeometry(bytes) {
  assertMagic(bytes, ROUTE_GEOMETRY_MAGIC, "Invalid Rangefind route geometry block.");
  const state = { pos: ROUTE_GEOMETRY_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== GEOMETRY_VERSION) throw new Error(`Unsupported route geometry version ${version}.`);
  const cellId = readVarint(bytes, state);
  const count = readVarint(bytes, state);
  const polylines = new Array(count);
  let prevLat = 0;
  let prevLon = 0;
  for (let p = 0; p < count; p++) {
    const points = new Int32Array(readVarint(bytes, state) * 2);
    points[0] = prevLat + readZigzag(bytes, state);
    points[1] = prevLon + readZigzag(bytes, state);
    for (let i = 2; i < points.length; i += 2) {
      points[i] = points[i - 2] + readZigzag(bytes, state);
      points[i + 1] = points[i - 1] + readZigzag(bytes, state);
    }
    prevLat = points[0];
    prevLon = points[1];
    polylines[p] = points;
  }
  if (state.pos !== bytes.length) throw new Error("Trailing bytes in Rangefind route geometry block.");
  return { cellId, polylines };
}

// Resolves an edge's geometry reference against its leaf's geometry block:
// [latE7, lonE7, ...] pairs oriented in the edge's travel direction.
export function edgePolyline(geomRef, geometryBlock) {
  const canonical = geometryBlock.polylines[geomRef >>> 1];
  if (!(geomRef & 1)) return canonical;
  const points = new Int32Array(canonical.length);
  for (let i = 0; i < canonical.length; i += 2) {
    points[i] = canonical[canonical.length - 2 - i];
    points[i + 1] = canonical[canonical.length - 1 - i];
  }
  return points;
}

// --- Overlay block ------------------------------------------------------
//
// `overlay` is { level, cellId, nodes (sorted global ids), rowStart,
// targetIndex (into nodes), weights, isClique }. Clique edges shortcut
// through one child cell; crossing edges are original graph edges whose
// endpoints live in different children.

export function encodeRouteOverlay(overlay) {
  const out = [...ROUTE_OVERLAY_MAGIC];
  pushVarint(out, OVERLAY_VERSION);
  pushVarint(out, overlay.level);
  pushVarint(out, overlay.cellId);
  pushVarint(out, overlay.nodes.length);
  let previous = 0;
  for (const node of overlay.nodes) {
    pushVarint(out, node - previous);
    previous = node;
  }
  const edgeCount = overlay.rowStart[overlay.nodes.length];
  pushVarint(out, edgeCount);
  for (let node = 0; node < overlay.nodes.length; node++) {
    const start = overlay.rowStart[node];
    const end = overlay.rowStart[node + 1];
    pushVarint(out, end - start);
    for (let e = start; e < end; e++) {
      pushZigzag(out, overlay.targetIndex[e] - node);
      pushVarint(out, overlay.weights[e] * 2 + (overlay.isClique[e] ? 1 : 0));
    }
  }
  return Uint8Array.from(out);
}

export function decodeRouteOverlay(bytes) {
  assertMagic(bytes, ROUTE_OVERLAY_MAGIC, "Invalid Rangefind route overlay block.");
  const state = { pos: ROUTE_OVERLAY_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== OVERLAY_VERSION) throw new Error(`Unsupported route overlay version ${version}.`);
  const level = readVarint(bytes, state);
  const cellId = readVarint(bytes, state);
  const nodeCount = readVarint(bytes, state);
  const nodes = new Uint32Array(nodeCount);
  let previous = 0;
  for (let i = 0; i < nodeCount; i++) {
    previous += readVarint(bytes, state);
    nodes[i] = previous;
  }
  const edgeCount = readVarint(bytes, state);
  const rowStart = new Uint32Array(nodeCount + 1);
  const targetIndex = new Uint32Array(edgeCount);
  const weights = new Uint32Array(edgeCount);
  const isClique = new Uint8Array(edgeCount);
  let cursor = 0;
  for (let node = 0; node < nodeCount; node++) {
    const degree = readVarint(bytes, state);
    rowStart[node + 1] = rowStart[node] + degree;
    for (let i = 0; i < degree; i++) {
      targetIndex[cursor] = node + readZigzag(bytes, state);
      const packed = readVarint(bytes, state);
      weights[cursor] = Math.floor(packed / 2);
      isClique[cursor] = packed % 2;
      cursor++;
    }
  }
  if (state.pos !== bytes.length) throw new Error("Trailing bytes in Rangefind route overlay block.");
  return { level, cellId, nodes, rowStart, targetIndex, weights, isClique };
}

// --- Root ---------------------------------------------------------------
//
// `root` is {
//   sourceHash, nodeCount, edgeCount, levelFanouts, names,
//   shards: [{ id, dir, packs: [fileName] }],
//   leaves: [{ firstNode, nodeCount, bbox: {minLat, maxLat, minLon, maxLon},
//              shardIndex, pointer }],
//   levels: [[{ firstLeaf, leafCount, shardIndex, pointer }], ...],
//   topOverlay: { shardIndex, pointer },
//   namesFile
// }
// levels[0] groups leaves into level-1 cells; the last entry is the level
// whose cells the top overlay connects. Street names live in a separate
// lazily fetched gzipped JSON object so the root stays small.

// Binary min-heap over (weight, value) pairs used by both the builder's
// clique computation and the query engine's multilevel Dijkstra.
export class MinHeap {
  constructor() {
    this.weights = [];
    this.values = [];
  }
  get size() {
    return this.weights.length;
  }
  push(weight, value) {
    const weights = this.weights;
    const values = this.values;
    let index = weights.length;
    weights.push(weight);
    values.push(value);
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (weights[parent] <= weight) break;
      weights[index] = weights[parent];
      values[index] = values[parent];
      index = parent;
    }
    weights[index] = weight;
    values[index] = value;
  }
  pop() {
    const weights = this.weights;
    const values = this.values;
    const topValue = values[0];
    const lastWeight = weights.pop();
    const lastValue = values.pop();
    if (weights.length > 0) {
      let index = 0;
      for (;;) {
        const left = index * 2 + 1;
        if (left >= weights.length) break;
        const right = left + 1;
        const child = right < weights.length && weights[right] < weights[left] ? right : left;
        if (weights[child] >= lastWeight) break;
        weights[index] = weights[child];
        values[index] = values[child];
        index = child;
      }
      weights[index] = lastWeight;
      values[index] = lastValue;
    }
    return topValue;
  }
  peekWeight() {
    return this.weights.length ? this.weights[0] : Infinity;
  }
}

export function encodeRouteRoot(root) {
  const out = [...ROUTE_ROOT_MAGIC];
  pushVarint(out, ROOT_VERSION);
  pushUtf8(out, ROUTE_GRAPH_FORMAT);
  pushUtf8(out, root.sourceHash || "");
  pushVarint(out, root.nodeCount);
  pushVarint(out, root.edgeCount);
  pushVarint(out, root.levelFanouts.length);
  for (const fanout of root.levelFanouts) pushVarint(out, fanout);
  pushUtf8(out, root.profile || "car");
  pushVarint(out, (root.classes || []).length);
  for (const name of root.classes || []) pushUtf8(out, name);
  const buckets = root.buckets || [{ name: "base", rules: [], factors: [] }];
  pushVarint(out, buckets.length);
  for (const bucket of buckets) {
    pushUtf8(out, bucket.name);
    pushVarint(out, (bucket.rules || []).length);
    for (const rule of bucket.rules || []) {
      out.push(rule.dayMask & 0xff, rule.startHour & 0xff, rule.endHour & 0xff);
    }
    pushVarint(out, (bucket.factors || []).length);
    for (const factor of bucket.factors || []) pushVarint(out, factor);
  }
  pushVarint(out, root.shards.length);
  for (const shard of root.shards) {
    pushUtf8(out, shard.id);
    pushUtf8(out, shard.dir);
    pushVarint(out, shard.packs.length);
    for (const pack of shard.packs) pushUtf8(out, pack);
  }
  pushVarint(out, root.leaves.length);
  for (const leaf of root.leaves) {
    pushVarint(out, leaf.firstNode);
    pushVarint(out, leaf.nodeCount);
    pushZigzag(out, leaf.bbox.minLat);
    pushVarint(out, leaf.bbox.maxLat - leaf.bbox.minLat);
    pushZigzag(out, leaf.bbox.minLon);
    pushVarint(out, leaf.bbox.maxLon - leaf.bbox.minLon);
    pushVarint(out, leaf.shardIndex);
    pushPointer(out, leaf.pointer);
    pushPointer(out, leaf.geometryPointer);
  }
  pushVarint(out, root.levels.length);
  for (const level of root.levels) {
    pushVarint(out, level.length);
    for (const cell of level) {
      pushVarint(out, cell.firstLeaf);
      pushVarint(out, cell.leafCount);
      pushVarint(out, cell.shardIndex);
      for (const pointer of cell.pointers) pushPointer(out, pointer);
    }
  }
  pushVarint(out, root.topOverlay.shardIndex);
  for (const pointer of root.topOverlay.pointers) pushPointer(out, pointer);
  pushUtf8(out, root.namesFile || "");
  return Uint8Array.from(out);
}

export function decodeRouteRoot(bytes) {
  assertMagic(bytes, ROUTE_ROOT_MAGIC, "Invalid Rangefind route graph root.");
  const state = { pos: ROUTE_ROOT_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== ROOT_VERSION) throw new Error(`Unsupported route graph root version ${version}.`);
  const format = readUtf8(bytes, state);
  if (format !== ROUTE_GRAPH_FORMAT) throw new Error(`Unsupported route graph format ${format}.`);
  const sourceHash = readUtf8(bytes, state);
  const nodeCount = readVarint(bytes, state);
  const edgeCount = readVarint(bytes, state);
  const fanoutCount = readVarint(bytes, state);
  const levelFanouts = [];
  for (let i = 0; i < fanoutCount; i++) levelFanouts.push(readVarint(bytes, state));
  const profile = readUtf8(bytes, state);
  const classCount = readVarint(bytes, state);
  const classes = [];
  for (let i = 0; i < classCount; i++) classes.push(readUtf8(bytes, state));
  const bucketCount = readVarint(bytes, state);
  const buckets = [];
  for (let i = 0; i < bucketCount; i++) {
    const name = readUtf8(bytes, state);
    const ruleCount = readVarint(bytes, state);
    const rules = [];
    for (let r = 0; r < ruleCount; r++) {
      rules.push({ dayMask: bytes[state.pos], startHour: bytes[state.pos + 1], endHour: bytes[state.pos + 2] });
      state.pos += 3;
    }
    const factorCount = readVarint(bytes, state);
    const factors = [];
    for (let f = 0; f < factorCount; f++) factors.push(readVarint(bytes, state));
    buckets.push({ name, rules, factors });
  }
  const shardCount = readVarint(bytes, state);
  const shards = [];
  for (let i = 0; i < shardCount; i++) {
    const id = readUtf8(bytes, state);
    const dir = readUtf8(bytes, state);
    const packCount = readVarint(bytes, state);
    const packs = [];
    for (let p = 0; p < packCount; p++) packs.push(readUtf8(bytes, state));
    shards.push({ id, dir, packs });
  }
  const leafCount = readVarint(bytes, state);
  const leaves = [];
  for (let i = 0; i < leafCount; i++) {
    const firstNode = readVarint(bytes, state);
    const nodeCountLeaf = readVarint(bytes, state);
    const minLat = readZigzag(bytes, state);
    const maxLat = minLat + readVarint(bytes, state);
    const minLon = readZigzag(bytes, state);
    const maxLon = minLon + readVarint(bytes, state);
    const shardIndex = readVarint(bytes, state);
    const pointer = readPointer(bytes, state);
    const geometryPointer = readPointer(bytes, state);
    leaves.push({ firstNode, nodeCount: nodeCountLeaf, bbox: { minLat, maxLat, minLon, maxLon }, shardIndex, pointer, geometryPointer });
  }
  const levelCount = readVarint(bytes, state);
  const levels = [];
  for (let i = 0; i < levelCount; i++) {
    const cellCount = readVarint(bytes, state);
    const level = [];
    for (let c = 0; c < cellCount; c++) {
      const firstLeaf = readVarint(bytes, state);
      const leafCountCell = readVarint(bytes, state);
      const shardIndex = readVarint(bytes, state);
      const pointers = [];
      for (let b = 0; b < bucketCount; b++) pointers.push(readPointer(bytes, state));
      level.push({ firstLeaf, leafCount: leafCountCell, shardIndex, pointers });
    }
    levels.push(level);
  }
  const topShardIndex = readVarint(bytes, state);
  const topPointers = [];
  for (let b = 0; b < bucketCount; b++) topPointers.push(readPointer(bytes, state));
  const namesFile = readUtf8(bytes, state);
  if (state.pos !== bytes.length) throw new Error("Trailing bytes in Rangefind route graph root.");
  return {
    sourceHash,
    nodeCount,
    edgeCount,
    levelFanouts,
    profile,
    classes,
    buckets,
    shards,
    leaves,
    levels,
    topOverlay: { shardIndex: topShardIndex, pointers: topPointers },
    namesFile
  };
}
