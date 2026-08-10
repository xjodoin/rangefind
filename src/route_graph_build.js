// Builds a static rfroutegraph-v1 index from an extracted road graph.
//
// Pipeline: KD-partition the nodes into contiguous leaf cells over a
// locality-preserving order, group leaves into nested parent cells, compute
// boundary-node cliques bottom-up (CRP customization), then pack cell blocks
// and overlay blocks into content-addressed gzip packs with a small root.
//
// Node-only module (fs/zlib/crypto); exported as rangefind/route/build.

import { mkdirSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { join } from "node:path";
import { gzipSync } from "node:zlib";
import { createPackWriter, finalizePackWriter, writePackedShard } from "./packs.js";
import {
  MinHeap,
  ROUTE_GRAPH_FORMAT,
  bucketWeight,
  encodeRouteCell,
  encodeRouteGeometry,
  encodeRouteOverlay,
  encodeRouteRoot
} from "./route_graph.js";
import {
  ROUTE_PORTAL_FORMAT,
  encodeRoutePortalIds,
  encodeRoutePortalRecords,
  routePortalCount
} from "./route_portals.js";

const OBJECT_NAME_HASH_LENGTH = 24;
export { ROUTE_PORTAL_FORMAT } from "./route_portals.js";

function kdPartition(latE7, lonE7, leafNodes) {
  const nodeCount = latE7.length;
  const perm = new Uint32Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) perm[i] = i;
  const leaves = [];
  const stack = [[0, nodeCount]];
  const coord = (useLat, index) => (useLat ? latE7[perm[index]] : lonE7[perm[index]]);
  while (stack.length) {
    const [start, end] = stack.pop();
    if (end - start <= leafNodes) {
      leaves.push([start, end]);
      continue;
    }
    let minLat = Infinity;
    let maxLat = -Infinity;
    let minLon = Infinity;
    let maxLon = -Infinity;
    for (let i = start; i < end; i++) {
      const lat = latE7[perm[i]];
      const lon = lonE7[perm[i]];
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
      if (lon < minLon) minLon = lon;
      if (lon > maxLon) maxLon = lon;
    }
    const midLatRad = ((minLat + maxLat) / 2) * (Math.PI / 180 / 1e7);
    const lonSpan = (maxLon - minLon) * Math.max(0.05, Math.cos(midLatRad));
    const useLat = maxLat - minLat >= lonSpan;
    const mid = (start + end) >> 1;
    quickselect(perm, start, end, mid, useLat, coord);
    // Push right first so leaves come out in left-to-right DFS order.
    stack.push([mid, end]);
    stack.push([start, mid]);
  }
  leaves.sort((a, b) => a[0] - b[0]);
  // Within each leaf, order nodes by coordinate (source id as tiebreak).
  // Junction-expanded graphs have many copies sharing one coordinate; making
  // them byte-adjacent lets gzip collapse their near-identical edge rows and
  // clique rows, and keeps CSR deltas small for everyone else.
  for (const [start, end] of leaves) {
    const slice = Array.from(perm.subarray(start, end));
    slice.sort((a, b) => latE7[a] - latE7[b] || lonE7[a] - lonE7[b] || a - b);
    perm.set(slice, start);
  }
  return { perm, leaves };
}

function quickselect(perm, start, end, target, useLat, coord) {
  let low = start;
  let high = end - 1;
  while (low < high) {
    const pivot = coord(useLat, (low + high) >> 1);
    let i = low;
    let j = high;
    while (i <= j) {
      while (coord(useLat, i) < pivot) i++;
      while (coord(useLat, j) > pivot) j--;
      if (i <= j) {
        const swap = perm[i];
        perm[i] = perm[j];
        perm[j] = swap;
        i++;
        j--;
      }
    }
    if (target <= j) high = j;
    else if (target >= i) low = i;
    else break;
  }
}

function buildCsr(nodeCount, from, to) {
  const rowStart = new Uint32Array(nodeCount + 1);
  for (let i = 0; i < from.length; i++) rowStart[from[i] + 1]++;
  for (let i = 0; i < nodeCount; i++) rowStart[i + 1] += rowStart[i];
  const targets = new Uint32Array(from.length);
  const edgeIds = new Uint32Array(from.length);
  // Fill each row backwards from its cumulative end. Walking the source
  // backwards preserves its stable order inside every row and lets rowStart
  // itself serve as the cursor, avoiding one node-sized scratch column. Once
  // filled, the decremented ends are shifted back into canonical row starts.
  for (let i = from.length - 1; i >= 0; i--) {
    const slot = --rowStart[from[i] + 1];
    targets[slot] = to[i];
    edgeIds[slot] = i;
  }
  for (let i = 1; i < nodeCount; i++) rowStart[i] = rowStart[i + 1];
  rowStart[nodeCount] = from.length;
  return { rowStart, targets, edgeIds };
}

function readGeomVarint(bytes, state) {
  let value = 0;
  let multiplier = 1;
  for (;;) {
    const byte = bytes[state.pos++];
    value += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) return value;
    multiplier *= 0x80;
  }
}

function readGeomZigzag(bytes, state) {
  const raw = readGeomVarint(bytes, state);
  return raw % 2 === 1 ? -(raw + 1) / 2 : raw / 2;
}

// Dijkstra pushes monotonically increasing non-negative integer distances.
// A radix heap replaces O(log n) binary-heap maintenance with 54 small
// buckets while retaining exact Number-safe distances (up to 2^53 - 1).
export class IntegerRadixHeap {
  constructor() {
    this.bucketWeights = Array.from({ length: 54 }, () => []);
    this.bucketValues = Array.from({ length: 54 }, () => []);
    this.last = 0;
    this.length = 0;
  }
  get size() {
    return this.length;
  }
  clear() {
    for (let i = 0; i < this.bucketWeights.length; i++) {
      this.bucketWeights[i].length = 0;
      this.bucketValues[i].length = 0;
    }
    this.last = 0;
    this.length = 0;
  }
  bucket(weight) {
    if (weight <= 0xffffffff && this.last <= 0xffffffff) {
      const difference = ((weight >>> 0) ^ (this.last >>> 0)) >>> 0;
      return difference ? 1 + (31 - Math.clz32(difference)) : 0;
    }
    const high = Math.floor(weight / 0x100000000);
    const lastHigh = Math.floor(this.last / 0x100000000);
    const highDifference = (high ^ lastHigh) >>> 0;
    if (highDifference) return 33 + (31 - Math.clz32(highDifference));
    const lowDifference = ((weight >>> 0) ^ (this.last >>> 0)) >>> 0;
    return lowDifference ? 1 + (31 - Math.clz32(lowDifference)) : 0;
  }
  push(weight, value) {
    if (weight < this.last || weight > Number.MAX_SAFE_INTEGER) {
      throw new Error(`Radix heap requires monotone safe-integer weights (got ${weight} after ${this.last})`);
    }
    const bucket = this.bucket(weight);
    this.bucketWeights[bucket].push(weight);
    this.bucketValues[bucket].push(value);
    this.length++;
  }
  prepareFirstBucket() {
    if (this.bucketWeights[0].length || !this.length) return;
    let bucket = 1;
    while (!this.bucketWeights[bucket].length) bucket++;
    const weights = this.bucketWeights[bucket];
    const values = this.bucketValues[bucket];
    let next = Infinity;
    for (let i = 0; i < weights.length; i++) {
      if (weights[i] < next) next = weights[i];
    }
    this.last = next;
    for (let i = 0; i < weights.length; i++) {
      const target = this.bucket(weights[i]);
      this.bucketWeights[target].push(weights[i]);
      this.bucketValues[target].push(values[i]);
    }
    weights.length = 0;
    values.length = 0;
  }
  peekWeight() {
    this.prepareFirstBucket();
    return this.length ? this.last : Infinity;
  }
  pop() {
    this.prepareFirstBucket();
    this.length--;
    this.bucketWeights[0].pop();
    return this.bucketValues[0].pop();
  }
}

class ReusableMinHeap extends MinHeap {
  clear() {
    this.weights.length = 0;
    this.values.length = 0;
  }
}

// Computes only irreducible boundary arcs. While Dijkstra runs, witnessed[v]
// records whether an equally-short route from the source to v already passes
// through another boundary. This is equivalent to the old O(B^3) post-pass
// test d(u,x) + d(x,v) === d(u,v), but needs no B-by-B distance matrix and no
// cubic scan. Positive weights make witness state final before a node settles.
/**
 * The tighter of two posted limits, where zero means nothing was posted.
 *
 * Not `Math.min`: an unposted road is unlimited, and taking the minimum
 * against zero would make every road in the country three metres high.
 */
function tighter(a, b) {
  if (!a) return b;
  if (!b) return a;
  return a < b ? a : b;
}

function localBoundaryDijkstra(
  rowStart,
  targets,
  weights,
  source,
  dist,
  heap,
  boundaryMask,
  boundaryCount,
  witnessed,
  stamp,
  generation,
  limits,
  edgeFlags,
  arena
) {
  dist[source] = 0;
  witnessed[source] = 0;
  if (arena) {
    arena.flagsAlong[source] = 0;
    arena.limH[source] = 0;
    arena.limW[source] = 0;
    arena.limL[source] = 0;
    arena.limKg[source] = 0;
  }
  stamp[source] = generation;
  heap.clear();
  heap.push(0, source);
  let remaining = boundaryCount - 1;
  while (heap.size) {
    const weight = heap.peekWeight();
    const node = heap.pop();
    if (stamp[node] !== generation || weight !== dist[node]) continue;
    if (node !== source && boundaryMask[node] && --remaining === 0) break;
    const passesBoundary = witnessed[node] || (node !== source && boundaryMask[node]) ? 1 : 0;
    for (let e = rowStart[node]; e < rowStart[node + 1]; e++) {
      const next = weight + weights[e];
      const target = targets[e];
      const previous = stamp[target] === generation ? dist[target] : Infinity;
      if (next < previous) {
        dist[target] = next;
        witnessed[target] = passesBoundary;
        if (arena) {
          arena.flagsAlong[target] = arena.flagsAlong[node] | (edgeFlags ? edgeFlags[e] : 0);
          // The tightest thing over the path we just found, which is the
          // tightest so far plus whatever stands over this edge.
          const edge = limits ? limits[e] : null;
          arena.limH[target] = tighter(arena.limH[node], edge ? edge.heightCm : 0);
          arena.limW[target] = tighter(arena.limW[node], edge ? edge.widthCm : 0);
          arena.limL[target] = tighter(arena.limL[node], edge ? edge.lengthCm : 0);
          arena.limKg[target] = tighter(arena.limKg[node], edge ? edge.weightKg : 0);
        }
        stamp[target] = generation;
        heap.push(next, target);
      } else if (next === previous && passesBoundary) {
        witnessed[target] = 1;
      }
    }
  }
}

/**
 * Boundary-to-boundary shortcuts for one cell, each carrying the tightest
 * limit standing over the path it replaces.
 *
 * A shortcut is a whole path through a cell collapsed to one number, and the
 * low bridge is somewhere inside it — so without this a restricted vehicle
 * either has to abandon the hierarchy (and then cannot be routed beyond the
 * few cells it has roads loaded for) or falls straight through the shortcut
 * onto the bridge.
 *
 * What is recorded is the limit along the *shortest* path, which is safe and
 * conservative rather than exact. A vehicle that clears it takes the shortcut
 * and genuinely fits, because that is the path the shortcut stands for. A
 * vehicle that does not is refused the shortcut, and may be sent around a
 * cell it could in fact have crossed by some longer way inside. Being sent
 * the long way round is a worse route; being sent under the bridge is a
 * wedged van, and only one of those is allowed to be the error.
 */
function boundaryClique(
  nodeCount,
  rowStart,
  targets,
  weights,
  boundary,
  globalNodes,
  globalOffset,
  arenas,
  heaps,
  edgeLimits,
  edgeFlagsAlong,
  limitTable
) {
  if (boundary.length < 2) return new Uint32Array(0);
  arenas.ensure(nodeCount);
  const { dist, boundaryMask, witnessed } = arenas;
  const heap = nodeCount >= heaps.radixMinNodes ? heaps.radix : heaps.binary;
  boundaryMask.fill(0, 0, nodeCount);
  for (const node of boundary) boundaryMask[node] = 1;
  const quads = [];
  const tracking = Boolean(limitTable);
  for (const source of boundary) {
    const generation = arenas.nextGeneration();
    localBoundaryDijkstra(
      rowStart,
      targets,
      weights,
      source,
      dist,
      heap,
      boundaryMask,
      boundary.length,
      witnessed,
      arenas.stamp,
      generation,
      edgeLimits,
      edgeFlagsAlong,
      tracking ? arenas : null
    );
    const globalSource = globalNodes ? globalNodes[source] : globalOffset + source;
    for (const target of boundary) {
      if (target === source || arenas.stamp[target] !== generation || witnessed[target]) continue;
      const limitId = tracking
        ? limitTable.idFor(arenas.limH[target], arenas.limW[target], arenas.limL[target], arenas.limKg[target])
        : 0;
      quads.push(
        globalSource,
        globalNodes ? globalNodes[target] : globalOffset + target,
        dist[target],
        // The limit and what the path passed through, packed together: a
        // shortcut has to carry both or it is a hole in one of them.
        limitId,
        tracking ? arenas.flagsAlong[target] : 0
      );
    }
  }
  return Uint32Array.from(quads);
}

export function buildRouteGraph(graph, outDir, options = {}) {
  const leafNodes = Math.max(64, Math.floor(options.leafNodes ?? 1280));
  const fanout = Math.max(2, Math.floor(options.fanout ?? 8));
  // A small top keeps the always-fetched top overlay tiny; deeper levels
  // cost two extra mid-level objects per query but benched strictly better
  // on transfer and latency (see docs/route-graph.md).
  const topMaxCells = Math.max(2, Math.floor(options.topMaxCells ?? 8));
  const shardCount = Math.max(1, Math.floor(options.shards ?? 1));
  const packBytes = Math.max(64 * 1024, Math.floor(options.packBytes ?? 2 * 1024 * 1024));
  const log = options.log || (() => {});
  const nodeCount = graph.nodeLat.length;
  const edgeCount = graph.edgeFrom.length;
  // Hash before optional source release. The production indexer does not use
  // the extracted columns after this build, so it can hand their backing
  // stores back to GC as soon as the locality-ordered equivalents exist.
  const sourceHash = createHash("sha256")
    .update(Buffer.from(graph.nodeLat.buffer, graph.nodeLat.byteOffset, graph.nodeLat.byteLength))
    .update(Buffer.from(graph.edgeFrom.buffer, graph.edgeFrom.byteOffset, graph.edgeFrom.byteLength))
    .update(Buffer.from(graph.edgeWeightDs.buffer, graph.edgeWeightDs.byteOffset, graph.edgeWeightDs.byteLength))
    .digest("hex");

  // 1. Partition and renumber into a locality-preserving order.
  const { perm, leaves } = kdPartition(graph.nodeLat, graph.nodeLon, leafNodes);
  const newId = new Uint32Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) newId[perm[i]] = i;
  let latE7 = new Int32Array(nodeCount);
  let lonE7 = new Int32Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) {
    latE7[i] = graph.nodeLat[perm[i]];
    lonE7[i] = graph.nodeLon[perm[i]];
  }
  if (options.releaseSource === true) {
    graph.nodeLat = null;
    graph.nodeLon = null;
  }
  let edgeFrom = new Uint32Array(edgeCount);
  let edgeTo = new Uint32Array(edgeCount);
  for (let i = 0; i < edgeCount; i++) {
    edgeFrom[i] = newId[graph.edgeFrom[i]];
    edgeTo[i] = newId[graph.edgeTo[i]];
  }
  const csr = buildCsr(nodeCount, edgeFrom, edgeTo);
  if (options.releaseSource === true) {
    graph.edgeFrom = null;
    graph.edgeTo = null;
  }
  // newId is dead after edge remapping. Reuse its backing store for the leaf
  // assignment instead of retaining a third node-sized Uint32 column.
  const leafOfNode = newId;
  for (let leaf = 0; leaf < leaves.length; leaf++) {
    leafOfNode.fill(leaf, leaves[leaf][0], leaves[leaf][1]);
  }
  log(`partition: ${nodeCount.toLocaleString()} nodes into ${leaves.length.toLocaleString()} leaves`);

  // 2. Level structure: group leaves by fanout until the top stays small.
  const levelFanouts = [];
  let cellsAtTop = leaves.length;
  while (cellsAtTop > topMaxCells) {
    levelFanouts.push(fanout);
    cellsAtTop = Math.ceil(cellsAtTop / fanout);
  }
  const levelCount = levelFanouts.length;
  const cumFanout = [1];
  for (const value of levelFanouts) cumFanout.push(cumFanout[cumFanout.length - 1] * value);
  const cellAtLevel = (node, level) => (level > levelCount ? 0 : Math.floor(leafOfNode[node] / cumFanout[level]));
  const cellsPerLevel = [];
  for (let level = 1; level <= levelCount; level++) {
    cellsPerLevel.push(Math.ceil(leaves.length / cumFanout[level]));
  }

  // 3. Edge LCA levels and per-node boundary requirements.
  // lca(edge) = smallest level at which both endpoints share a cell;
  // level 0 = same leaf, levelCount + 1 = only the top region.
  const edgeLca = new Uint8Array(edgeCount);
  const maxLca = new Uint8Array(nodeCount);
  // Traverse CSR so edgeFrom is no longer needed as an LCA input. Both
  // remapped endpoint columns become unreachable at the end of this phase;
  // India releases about 900 MiB before allocating its overlay graphs.
  for (let from = 0; from < nodeCount; from++) {
    for (let slot = csr.rowStart[from]; slot < csr.rowStart[from + 1]; slot++) {
      const edgeId = csr.edgeIds[slot];
      const to = csr.targets[slot];
      let lca = levelCount + 1;
      for (let level = 0; level <= levelCount; level++) {
        if (cellAtLevel(from, level) === cellAtLevel(to, level)) {
          lca = level;
          break;
        }
      }
      edgeLca[edgeId] = lca;
      if (lca > maxLca[from]) maxLca[from] = lca;
      if (lca > maxLca[to]) maxLca[to] = lca;
    }
  }
  edgeFrom = null;
  edgeTo = null;
  if (options.releaseSource === true && typeof options.collectGarbage === "function") {
    options.collectGarbage("topology");
  }
  log(`topology: ${edgeCount.toLocaleString()} edges across ${levelCount + 1} overlay level(s)`);

  // 4. Time-of-day buckets: bucket 0 is the base metric; extra buckets scale
  // edge weights by per-class time factors, and get their own exact cliques.
  const classes = graph.classes || [];
  const buckets = [
    { name: "base", rules: [], factors: classes.map(() => 1000) },
    ...(options.timeBuckets || []).map(bucket => ({
      name: bucket.name,
      rules: bucket.rules || [],
      factors: classes.map(name => Math.round((bucket.classFactors?.[name] ?? 1) * 1000))
    }))
  ];
  const edgeClassOf = (edgeId) => (graph.edgeClass ? graph.edgeClass[edgeId] : 0);

  /**
   * Which buckets a timed turn ban is in force for.
   *
   * A ban is applied to a bucket only when the bucket's whole window sits
   * *inside* the ban's — so a no-left-turn from 07:00 to 09:00 shuts the peak
   * bucket, which is exactly those hours, and leaves the base metric alone.
   *
   * Containment rather than overlap, deliberately. Overlap would let an
   * evening ban shut the base bucket, which is every other hour of the week,
   * and send a driver the long way round at three in the morning to obey a
   * sign that only applies at rush hour. Under containment a ban that matches
   * no bucket is simply not applied — the same as before this existed, never
   * worse — and adding a bucket for those hours is what turns it on. So the
   * resolution of this is the resolution of the bucket table, and that is a
   * dial rather than a limit.
   */
  const banRules = graph.banRules || [];
  const bucketBans = buckets.map(bucket => {
    const banned = new Set();
    // The base bucket has no window of its own: it is whatever the others are
    // not, so nothing can be contained in it and nothing is ever banned there.
    if (!bucket.rules?.length) return banned;
    for (let id = 1; id <= banRules.length; id++) {
      const ban = banRules[id - 1];
      const covered = bucket.rules.every(rule =>
        (rule.dayMask & ban.days) === rule.dayMask &&
        rule.startHour * 60 >= ban.startMinute &&
        rule.endHour * 60 <= ban.endMinute
      );
      if (covered) banned.add(id);
    }
    return banned;
  });
  const bannedTurns = bucketBans.reduce((sum, set) => sum + set.size, 0);
  if (banRules.length) {
    log(`timed turns: ${banRules.length} window(s), ${bannedTurns} bucket application(s)`);
  }

  /**
   * A shut turn is not a slow one. Priced out of the metric entirely rather
   * than penalised, so no route can buy its way through a ban by being long
   * enough elsewhere — and priced rather than deleted so the same topology
   * serves every bucket, which is what lets the overlays stay exact.
   */
  const IMPASSABLE_DS = 0x3fffffff;
  const weightOf = (edgeId, bucket) => {
    const ban = graph.edgeBan ? graph.edgeBan[edgeId] : 0;
    if (ban && bucketBans[bucket].has(ban)) return IMPASSABLE_DS;
    return bucketWeight(graph.edgeWeightDs[edgeId], edgeClassOf(edgeId), buckets[bucket].factors);
  };

  // 5. Bottom-up cliques per bucket. cliques[level] maps cellId -> flat
  // [u, v, w, limitId, flags, ...] quints over nodes with maxLca > level, exact within the
  // cell for that bucket's metric.
  const heaps = {
    binary: new ReusableMinHeap(),
    radix: new IntegerRadixHeap(),
    radixMinNodes: Math.max(0, Number(options.overlayRadixMinNodes ?? 4096))
  };
  /**
   * The limit sets edges and shortcuts point at, growable during the build.
   *
   * Seeded from what the extractor found on the ways, and added to as
   * shortcuts are formed: collapsing a path takes the tightest of everything
   * along it, and that combination — a 3.2 m arch on a road with a 7.5 t
   * bridge — need never have been posted anywhere as a single sign.
   */
  const limitTable = (() => {
    const list = (graph.limits || []).map(limit => ({
      heightCm: limit.heightCm || 0,
      weightKg: limit.weightKg || 0,
      widthCm: limit.widthCm || 0,
      lengthCm: limit.lengthCm || 0
    }));
    const byKey = new Map();
    list.forEach((limit, index) => {
      byKey.set(`${limit.heightCm}/${limit.weightKg}/${limit.widthCm}/${limit.lengthCm}`, index + 1);
    });
    return {
      list,
      idFor(heightCm, widthCm, lengthCm, weightKg) {
        if (!heightCm && !widthCm && !lengthCm && !weightKg) return 0;
        const key = `${heightCm}/${weightKg}/${widthCm}/${lengthCm}`;
        const existing = byKey.get(key);
        if (existing) return existing;
        list.push({ heightCm, weightKg, widthCm, lengthCm });
        byKey.set(key, list.length);
        return list.length;
      },
      /** The four numbers an id stands for, for propagating through a path. */
      at(id) {
        return id ? list[id - 1] : null;
      }
    };
  })();

  const cliqueArenas = {
    dist: new Float64Array(leafNodes + 8),
    boundaryMask: new Uint8Array(leafNodes + 8),
    witnessed: new Uint8Array(leafNodes + 8),
    // The tightest thing standing over the best path to each node, carried
    // alongside the distance so a shortcut can say what its own interior
    // will admit. Zero is "nothing posted", which is almost every path.
    // What the best path to each node has passed through, OR-ed along it:
    // a toll booth or a boat anywhere inside a shortcut is a toll booth or a
    // boat on the shortcut, or "avoid ferries" falls through the hierarchy
    // exactly the way a low bridge did.
    flagsAlong: new Uint8Array(leafNodes + 8),
    limH: new Uint32Array(leafNodes + 8),
    limW: new Uint32Array(leafNodes + 8),
    limL: new Uint32Array(leafNodes + 8),
    limKg: new Uint32Array(leafNodes + 8),
    stamp: new Uint32Array(leafNodes + 8),
    generation: 0,
    ensure(size) {
      if (this.dist.length < size) {
        this.dist = new Float64Array(size);
        this.boundaryMask = new Uint8Array(size);
        this.witnessed = new Uint8Array(size);
        this.flagsAlong = new Uint8Array(size);
        this.limH = new Uint32Array(size);
        this.limW = new Uint32Array(size);
        this.limL = new Uint32Array(size);
        this.limKg = new Uint32Array(size);
      }
      if (this.stamp.length < size) this.stamp = new Uint32Array(size);
    },
    resetStamp() {
      this.stamp.fill(0);
      this.generation = 0;
    },
    nextGeneration() {
      this.generation = (this.generation + 1) >>> 0;
      if (this.generation === 0) {
        this.stamp.fill(0);
        this.generation = 1;
      }
      return this.generation;
    }
  };
  // Every level rebuilds the same global->cell-local lookup, and no retained
  // overlay references it. One phase-owned arena avoids allocating another
  // 160 MiB column at each of India's five levels.
  let overlayLocalIndex;
  const computeBucketOverlays = (bucket) => {
    cliqueArenas.resetStamp();
    const cliques = [];
    {
      const leafCliques = new Map();
      for (let leaf = 0; leaf < leaves.length; leaf++) {
        const [start, end] = leaves[leaf];
        const size = end - start;
        const localFrom = [];
        const localTo = [];
        const localWeight = [];
        const localLimit = [];
        const localFlag = [];
        for (let node = start; node < end; node++) {
          for (let e = csr.rowStart[node]; e < csr.rowStart[node + 1]; e++) {
            if (edgeLca[csr.edgeIds[e]] !== 0) continue;
            localFrom.push(node - start);
            localTo.push(csr.targets[e] - start);
            localWeight.push(weightOf(csr.edgeIds[e], bucket));
            localLimit.push(graph.edgeLimit ? graph.edgeLimit[csr.edgeIds[e]] : 0);
            localFlag.push(graph.edgeFlags ? graph.edgeFlags[csr.edgeIds[e]] : 0);
          }
        }
        const local = buildCsr(size, Uint32Array.from(localFrom), Uint32Array.from(localTo));
        const localWeights = new Uint32Array(localFrom.length);
        // Resolved rather than kept as ids: the search takes minimums across
        // four numbers per relaxation, and a table lookup in that loop is the
        // one place this could cost anything.
        const localLimits = new Array(localFrom.length);
        const localFlags = new Uint8Array(localFrom.length);
        for (let i = 0; i < localFrom.length; i++) {
          localWeights[i] = localWeight[local.edgeIds[i]];
          localLimits[i] = limitTable.at(localLimit[local.edgeIds[i]]);
          localFlags[i] = localFlag[local.edgeIds[i]];
        }
        const boundary = [];
        for (let node = start; node < end; node++) {
          if (maxLca[node] >= 1) boundary.push(node - start);
        }
        leafCliques.set(leaf, boundaryClique(
          size,
          local.rowStart,
          local.targets,
          localWeights,
          boundary,
          null,
          start,
          cliqueArenas,
          heaps,
          localLimits,
          localFlags,
          limitTable
        ));
      }
      cliques.push(leafCliques);
      log(`overlays (${buckets[bucket].name}): leaf cliques complete`);
    }

    // Overlay graphs per parent cell, and cliques over them for the next
    // level. overlays[level - 1] maps cellId -> { nodes, rowStart,
    // targetIndex, weights, isClique } for cells at `level`.
    const overlays = [];
    const buildOverlayGraphs = (level) => {
      const cellOf = (node) => cellAtLevel(node, level);
      const cellCount = level <= levelCount ? cellsPerLevel[level - 1] : 1;
      const nodeCounts = new Uint32Array(cellCount);
      for (let node = 0; node < nodeCount; node++) {
        if (maxLca[node] >= level) nodeCounts[cellOf(node)]++;
      }
      const nodesByCell = Array.from(nodeCounts, count => new Uint32Array(count));
      const nodeCursor = new Uint32Array(cellCount);
      for (let node = 0; node < nodeCount; node++) {
        if (maxLca[node] >= level) {
          const cell = cellOf(node);
          nodesByCell[cell][nodeCursor[cell]++] = node;
        }
      }
      // Every retained node belongs to one cell at this level. A flat lookup
      // avoids a Map entry (and boxed key/value pair) for every boundary node.
      for (let cell = 0; cell < cellCount; cell++) {
        const nodes = nodesByCell[cell];
        for (let i = 0; i < nodes.length; i++) overlayLocalIndex[nodes[i]] = i;
      }

      // Count once and allocate exact typed columns. Country-scale overlays
      // can contain tens of millions of arcs; nested `[u,v,w,flag]` JS arrays
      // cost several GiB before CSR conversion and were the Brazil builder's
      // dominant peak.
      const edgeCounts = new Uint32Array(cellCount);
      const childCliques = cliques[level - 1];
      for (const quints of childCliques.values()) {
        for (let i = 0; i < quints.length; i += 5) {
          edgeCounts[cellOf(quints[i])]++;
        }
      }
      for (let node = 0; node < nodeCount; node++) {
        for (let e = csr.rowStart[node]; e < csr.rowStart[node + 1]; e++) {
          if (edgeLca[csr.edgeIds[e]] === level) edgeCounts[cellOf(node)]++;
        }
      }
      const columnsByCell = Array.from(edgeCounts, count => ({
        from: new Uint32Array(count),
        to: new Uint32Array(count),
        weights: new Uint32Array(count),
        // What the path behind this arc will admit, so the tightest point of
        // a shortcut survives being collapsed into the level above it.
        limits: new Uint32Array(count),
        pathFlags: new Uint8Array(count),
        isClique: new Uint8Array(count)
      }));
      const edgeCursor = new Uint32Array(cellCount);
      for (const quints of childCliques.values()) {
        for (let i = 0; i < quints.length; i += 5) {
          const cell = cellOf(quints[i]);
          const cursor = edgeCursor[cell]++;
          const columns = columnsByCell[cell];
          columns.from[cursor] = overlayLocalIndex[quints[i]];
          columns.to[cursor] = overlayLocalIndex[quints[i + 1]];
          columns.weights[cursor] = quints[i + 2];
          columns.limits[cursor] = quints[i + 3];
          columns.pathFlags[cursor] = quints[i + 4];
          columns.isClique[cursor] = 1;
        }
      }
      for (let node = 0; node < nodeCount; node++) {
        for (let e = csr.rowStart[node]; e < csr.rowStart[node + 1]; e++) {
          const edgeId = csr.edgeIds[e];
          if (edgeLca[edgeId] !== level) continue;
          const cell = cellOf(node);
          const cursor = edgeCursor[cell]++;
          const columns = columnsByCell[cell];
          columns.from[cursor] = overlayLocalIndex[node];
          columns.to[cursor] = overlayLocalIndex[csr.targets[e]];
          columns.weights[cursor] = weightOf(edgeId, bucket);
          columns.limits[cursor] = graph.edgeLimit ? graph.edgeLimit[edgeId] : 0;
          columns.pathFlags[cursor] = graph.edgeFlags ? graph.edgeFlags[edgeId] : 0;
        }
      }
      const result = new Map();
      for (let cell = 0; cell < cellCount; cell++) {
        const nodes = nodesByCell[cell];
        if (!nodes.length) continue;
        const columns = columnsByCell[cell];
        const local = buildCsr(nodes.length, columns.from, columns.to);
        const weights = new Uint32Array(columns.weights.length);
        const limits = new Uint32Array(columns.limits.length);
        const pathFlags = new Uint8Array(columns.pathFlags.length);
        const isClique = new Uint8Array(columns.isClique.length);
        for (let i = 0; i < local.edgeIds.length; i++) {
          const edgeId = local.edgeIds[i];
          weights[i] = columns.weights[edgeId];
          limits[i] = columns.limits[edgeId];
          pathFlags[i] = columns.pathFlags[edgeId];
          isClique[i] = columns.isClique[edgeId];
        }
        result.set(cell, { nodes, rowStart: local.rowStart, targetIndex: local.targets, weights, limits, pathFlags, isClique });
      }
      return result;
    };

    for (let level = 1; level <= levelCount + 1; level++) {
      const graphs = buildOverlayGraphs(level);
      overlays.push(graphs);
      log(`overlays (${buckets[bucket].name}): level ${level}/${levelCount + 1} graph complete`);
      if (level > levelCount) break;
      // buildOverlayGraphs has just used the shared column as its global to
      // local lookup. Clear it once, then reuse the same 160 MiB India arena
      // as generation stamps for every boundary search at this level.
      cliqueArenas.resetStamp();
      const levelCliques = new Map();
      for (const [cell, overlay] of graphs) {
        const boundary = [];
        for (let i = 0; i < overlay.nodes.length; i++) {
          if (maxLca[overlay.nodes[i]] >= level + 1) boundary.push(i);
        }
        levelCliques.set(cell, boundaryClique(
          overlay.nodes.length,
          overlay.rowStart,
          overlay.targetIndex,
          overlay.weights,
          boundary,
          overlay.nodes,
          0,
          cliqueArenas,
          heaps,
          Array.from(overlay.limits, id => limitTable.at(id)),
          overlay.pathFlags,
          limitTable
        ));
      }
      cliques.push(levelCliques);
    }
    return overlays;
  };
  // 5. Shard assignment: contiguous top-child cell groups.
  const topChildren = cellsAtTop;
  const shardOfTopChild = new Uint32Array(topChildren);
  for (let i = 0; i < topChildren; i++) {
    shardOfTopChild[i] = Math.min(shardCount - 1, Math.floor((i * shardCount) / topChildren));
  }
  const shardOfLeaf = (leaf) => shardOfTopChild[Math.floor(leaf / cumFanout[levelCount])];

  // 6. Encode and pack.
  mkdirSync(outDir, { recursive: true });
  const shardDirs = [];
  const writers = [];
  const geometryWriters = [];
  for (let s = 0; s < shardCount; s++) {
    const dir = shardCount === 1 ? "objects" : `shards/${String(s).padStart(2, "0")}`;
    const packIndexes = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
    shardDirs.push(dir);
    writers.push(createPackWriter(join(outDir, dir), packBytes, { dedupe: false, indexCounter: packIndexes }));
    geometryWriters.push(createPackWriter(join(outDir, dir), packBytes, { dedupe: false, indexCounter: packIndexes }));
  }
  const rootWriter = createPackWriter(join(outDir, "top"), packBytes, { dedupe: false });

  const leafEntries = [];
  for (let leaf = 0; leaf < leaves.length; leaf++) {
    const [start, end] = leaves[leaf];
    const size = end - start;
    const cellLat = latE7.subarray(start, end);
    const cellLon = lonE7.subarray(start, end);
    const rowStart = new Uint32Array(size + 1);
    for (let i = 0; i < size; i++) rowStart[i + 1] = rowStart[i] + (csr.rowStart[start + i + 1] - csr.rowStart[start + i]);
    const cellEdgeCount = rowStart[size];
    const targets = new Uint32Array(cellEdgeCount);
    const weights = new Uint32Array(cellEdgeCount);
    const distsDm = new Uint32Array(cellEdgeCount);
    const nameIds = new Uint32Array(cellEdgeCount);
    const cellClasses = new Uint8Array(cellEdgeCount);
    const cellJunctions = new Uint8Array(cellEdgeCount);
    const cellSpeeds = new Uint8Array(cellEdgeCount);
    const cellCondRules = new Uint8Array(cellEdgeCount);
    // What the signs over this edge say, as an index into the root's table,
    // and the flag byte that says whether it is inside a roundabout.
    const cellSigns = new Uint32Array(cellEdgeCount);
    // What each edge physically will not admit, as an index into the root's
    // table. Zero on nearly every edge, which is why it is an index.
    const cellLimits = new Uint32Array(cellEdgeCount);
    // When each turn is shut, for the few that are shut on a schedule.
    const cellBans = new Uint32Array(cellEdgeCount);
    const cellCharges = new Uint32Array(cellEdgeCount);
    const cellLaneSigns = new Uint32Array(cellEdgeCount);
    const cellLaneReach = new Uint32Array(cellEdgeCount);
    const cellFlags = new Uint8Array(cellEdgeCount);
    // Lane movements per edge, in the edge's own travel direction. Jagged,
    // and empty for the overwhelming majority of edges.
    const cellLanes = new Array(cellEdgeCount);
    const cellLaneAccess = new Array(cellEdgeCount);
    const extLat = new Int32Array(cellEdgeCount);
    const extLon = new Int32Array(cellEdgeCount);
    const geomRefs = new Uint32Array(cellEdgeCount);
    // Deduplicated full-chain polylines: approach copies and two-way twins
    // of one physical road edge collapse into a single canonical entry.
    const uniquePolylines = [];
    const uniqueIndex = new Map();
    let cursor = 0;
    for (let node = start; node < end; node++) {
      for (let e = csr.rowStart[node]; e < csr.rowStart[node + 1]; e++) {
        const edgeId = csr.edgeIds[e];
        const target = csr.targets[e];
        targets[cursor] = target;
        weights[cursor] = graph.edgeWeightDs[edgeId];
        distsDm[cursor] = graph.edgeDistDm[edgeId];
        nameIds[cursor] = graph.edgeName[edgeId];
        cellClasses[cursor] = edgeClassOf(edgeId);
        cellJunctions[cursor] = graph.edgeJunction ? graph.edgeJunction[edgeId] : 0;
        cellSpeeds[cursor] = graph.edgeSpeed ? graph.edgeSpeed[edgeId] : 0;
        cellCondRules[cursor] = graph.edgeCond ? graph.edgeCond[edgeId] : 0;
        cellSigns[cursor] = graph.edgeSign ? graph.edgeSign[edgeId] : 0;
        cellLimits[cursor] = graph.edgeLimit ? graph.edgeLimit[edgeId] : 0;
        cellBans[cursor] = graph.edgeBan ? graph.edgeBan[edgeId] : 0;
        cellCharges[cursor] = graph.edgeCharge ? graph.edgeCharge[edgeId] : 0;
        cellLaneSigns[cursor] = graph.edgeLaneSign ? graph.edgeLaneSign[edgeId] : 0;
        cellLaneReach[cursor] = graph.edgeLaneMask ? graph.edgeLaneMask[edgeId] : 0;
        cellFlags[cursor] = graph.edgeFlags ? graph.edgeFlags[edgeId] : 0;
        if (graph.laneOffsets && graph.laneBytes) {
          // `[count, movement × count, reason × count]`; a record written
          // before reasons existed simply ends after the movements, and the
          // slice comes back empty rather than wrong.
          const laneStart = graph.laneOffsets[edgeId];
          const laneCount = graph.laneBytes[laneStart] || 0;
          const reasonStart = laneStart + 1 + laneCount;
          const hasReasons = graph.laneOffsets[edgeId + 1] >= reasonStart + laneCount;
          cellLanes[cursor] = laneCount
            ? Array.from(graph.laneBytes.subarray(laneStart + 1, reasonStart))
            : null;
          cellLaneAccess[cursor] = laneCount && hasReasons
            ? Array.from(graph.laneBytes.subarray(reasonStart, reasonStart + laneCount))
            : null;
        } else {
          cellLanes[cursor] = null;
          cellLaneAccess[cursor] = null;
        }
        if (target < start || target >= end) {
          extLat[cursor] = latE7[target];
          extLon[cursor] = lonE7[target];
        }
        // Full point chain in travel direction.
        const geomState = { pos: graph.geomOffsets[edgeId] };
        const interior = readGeomVarint(graph.geomBytes, geomState);
        const points = new Int32Array((interior + 2) * 2);
        points[0] = latE7[node];
        points[1] = lonE7[node];
        let pointLat = points[0];
        let pointLon = points[1];
        for (let p = 0; p < interior; p++) {
          pointLat += readGeomZigzag(graph.geomBytes, geomState);
          pointLon += readGeomZigzag(graph.geomBytes, geomState);
          points[(p + 1) * 2] = pointLat;
          points[(p + 1) * 2 + 1] = pointLon;
        }
        points[(interior + 1) * 2] = latE7[target];
        points[(interior + 1) * 2 + 1] = lonE7[target];
        // Canonical direction: lexicographically smaller endpoint first.
        const last = points.length - 2;
        const reversed = points[last] < points[0] || (points[last] === points[0] && points[last + 1] < points[1]);
        let canonical = points;
        if (reversed) {
          canonical = new Int32Array(points.length);
          for (let p = 0; p < points.length; p += 2) {
            canonical[p] = points[points.length - 2 - p];
            canonical[p + 1] = points[points.length - 1 - p];
          }
        }
        const key = canonical.join(",");
        let index = uniqueIndex.get(key);
        if (index == null) {
          index = uniquePolylines.length;
          uniquePolylines.push(canonical);
          uniqueIndex.set(key, index);
        }
        geomRefs[cursor] = index * 2 + (reversed ? 1 : 0);
        cursor++;
      }
    }
    const encoded = encodeRouteCell({
      cellId: leaf,
      firstNode: start,
      nodeCount: size,
      latE7: cellLat,
      lonE7: cellLon,
      rowStart,
      targets,
      weights,
      distsDm,
      nameIds,
      classes: cellClasses,
      speeds: cellSpeeds,
      condRules: cellCondRules,
      signs: cellSigns,
      limits: cellLimits,
      bans: cellBans,
      charges: cellCharges,
      laneSigns: cellLaneSigns,
      laneReach: cellLaneReach,
      flags: cellFlags,
      lanes: cellLanes,
      laneAccess: cellLaneAccess,
      junctions: cellJunctions,
      extLat,
      extLon,
      geomRefs
    });
    const shardIndex = shardOfLeaf(leaf);
    const compressed = gzipSync(Buffer.from(encoded), { level: 6 });
    const written = writeShard(writers[shardIndex], `cell-${leaf}`, compressed, encoded.length);
    const encodedGeometry = encodeRouteGeometry({ cellId: leaf, polylines: uniquePolylines });
    const compressedGeometry = gzipSync(Buffer.from(encodedGeometry), { level: 6 });
    const writtenGeometry = writeShard(geometryWriters[shardIndex], `geom-${leaf}`, compressedGeometry, encodedGeometry.length);
    let minLat = Infinity;
    let maxLat = -Infinity;
    let minLon = Infinity;
    let maxLon = -Infinity;
    for (let i = 0; i < size; i++) {
      if (cellLat[i] < minLat) minLat = cellLat[i];
      if (cellLat[i] > maxLat) maxLat = cellLat[i];
      if (cellLon[i] < minLon) minLon = cellLon[i];
      if (cellLon[i] > maxLon) maxLon = cellLon[i];
    }
    leafEntries.push({
      firstNode: start,
      nodeCount: size,
      bbox: { minLat, maxLat, minLon, maxLon },
      shardIndex,
      entry: written,
      geometryEntry: writtenGeometry
    });
  }
  log(`packing: ${leaves.length.toLocaleString()} leaf cell(s) complete`);

  // Leaf blocks are the sole consumers of display, geometry, and lane source
  // columns. Retire them before overlay construction instead of retaining
  // several GiB of payload beside every overlay generation. Computation moved
  // earlier, but pack write order remains leaves then overlays, byte-for-byte.
  if (options.releaseSource === true) {
    for (const key of [
      "edgeDistDm", "edgeName", "edgeJunction", "edgeSpeed", "edgeCond",
      "edgeSign", "edgeFlags", "geomOffsets", "geomBytes", "laneOffsets",
      "laneBytes"
    ]) graph[key] = null;
    latE7 = null;
    lonE7 = null;
    if (typeof options.collectGarbage === "function") options.collectGarbage("leaf-packing");
  }

  overlayLocalIndex = new Uint32Array(nodeCount);
  cliqueArenas.stamp = overlayLocalIndex;
  const bucketOverlays = buckets.map((_, bucket) => computeBucketOverlays(bucket));
  const overlays = bucketOverlays[0];
  if (options.releaseSource === true) {
    graph.edgeWeightDs = null;
    graph.edgeClass = null;
    if (typeof options.collectGarbage === "function") options.collectGarbage("overlays");
  }

  const emptyOverlay = {
    nodes: new Uint32Array(0),
    rowStart: new Uint32Array(1),
    targetIndex: new Uint32Array(0),
    weights: new Uint32Array(0),
    isClique: new Uint8Array(0)
  };
  const levelEntries = [];
  for (let level = 1; level <= levelCount; level++) {
    const cellCount = cellsPerLevel[level - 1];
    const entries = [];
    for (let cell = 0; cell < cellCount; cell++) {
      const firstLeaf = cell * cumFanout[level];
      const leafCountCell = Math.min(cumFanout[level], leaves.length - firstLeaf);
      const shardIndex = shardOfLeaf(firstLeaf);
      const written = buckets.map((bucket, b) => {
        const overlay = bucketOverlays[b][level - 1].get(cell) || emptyOverlay;
        const encoded = encodeRouteOverlay({ level, cellId: cell, ...overlay });
        const compressed = gzipSync(Buffer.from(encoded), { level: 6 });
        return writeShard(writers[shardIndex], `overlay-${b}-${level}-${cell}`, compressed, encoded.length);
      });
      entries.push({ firstLeaf, leafCount: leafCountCell, shardIndex, entries: written });
    }
    levelEntries.push(entries);
  }
  log(`packing: ${levelCount.toLocaleString()} overlay level(s) complete`);

  // Top overlay per bucket: connects the top-child cells; lives in its own
  // directory so sharded deployments publish one shared boundary artifact.
  const topEntries = buckets.map((bucket, b) => {
    const topOverlayGraph = mergeOverlayGraphs(bucketOverlays[b][levelCount], levelCount + 1);
    const encodedTop = encodeRouteOverlay(topOverlayGraph);
    const compressedTop = gzipSync(Buffer.from(encodedTop), { level: 6 });
    return {
      entry: writeShard(rootWriter, `top-overlay-${b}`, compressedTop, encodedTop.length),
      nodes: topOverlayGraph.nodes.length
    };
  });
  const topOverlayGraph = { nodes: { length: topEntries[0].nodes } };

  for (const writer of writers) finalizePackWriter(writer);
  for (const writer of geometryWriters) finalizePackWriter(writer);
  finalizePackWriter(rootWriter);

  // Build-only debug artifact: source-graph id per index node. Never
  // fetched at query time; benchmarks use it to run reference searches
  // against the original graph with the engine's node ids.
  writeFileSync(join(outDir, "node-order.bin"), Buffer.from(perm.buffer, perm.byteOffset, perm.byteLength));

  // Names sidecar.
  const namesJson = Buffer.from(JSON.stringify(graph.names), "utf8");
  const namesGz = gzipSync(namesJson, { level: 6 });
  const namesHash = createHash("sha256").update(namesGz).digest("hex").slice(0, OBJECT_NAME_HASH_LENGTH);
  const namesFile = `names.${namesHash}.bin.gz`;
  writeFileSync(join(outDir, namesFile), namesGz);

  // Build-time federation metadata stays outside the graph packs: clients
  // fetch it only when endpoints lie in different regional graphs. Each
  // neighbor has independently compressed id-only and coordinate-bearing
  // blocks in one content-addressed range file. A query reads the smaller
  // side's records plus the larger side's ids, never unrelated neighbors.
  const portalNeighbors = Object.fromEntries(
    Object.entries(graph.portals || {}).filter(([, values]) => routePortalCount(values) > 0)
  );
  const portalChunks = [];
  const portalDirectory = {};
  let portalOffset = 0;
  for (const [neighbor, values] of Object.entries(portalNeighbors).sort(([left], [right]) => left.localeCompare(right))) {
    const ids = gzipSync(Buffer.from(encodeRoutePortalIds(values)), { level: 6 });
    const records = gzipSync(Buffer.from(encodeRoutePortalRecords(values)), { level: 6 });
    const pointer = bytes => ({
      offset: portalOffset,
      length: bytes.length,
      checksum: createHash("sha256").update(bytes).digest("hex")
    });
    const idsPointer = pointer(ids);
    portalChunks.push(ids);
    portalOffset += ids.length;
    const recordsPointer = pointer(records);
    portalChunks.push(records);
    portalOffset += records.length;
    portalDirectory[neighbor] = {
      count: routePortalCount(values),
      ids: idsPointer,
      records: recordsPointer
    };
  }
  const portalsBin = Buffer.concat(portalChunks);
  const portalsHash = createHash("sha256").update(portalsBin).digest("hex").slice(0, OBJECT_NAME_HASH_LENGTH);
  const portalsFile = `portals.${portalsHash}.bin`;
  writeFileSync(join(outDir, portalsFile), portalsBin);
  const portals = {
    format: ROUTE_PORTAL_FORMAT,
    file: portalsFile,
    neighbors: portalDirectory
  };

  const shardPackTables = writers.map((writer, s) => (
    [...writer.packs, ...geometryWriters[s].packs]
      .sort((left, right) => left.index - right.index)
      .map(pack => pack.file)
  ));
  const shards = writers.map((writer, s) => ({
    id: shardCount === 1 ? "all" : `shard-${String(s).padStart(2, "0")}`,
    dir: shardDirs[s],
    packs: shardPackTables[s]
  }));
  shards.push({ id: "__top", dir: "top", packs: rootWriter.packs.map(pack => pack.file) });
  const topShardIndex = shards.length - 1;

  const packTableOf = writer => {
    const dataShard = writers.indexOf(writer);
    if (dataShard >= 0) return shardPackTables[dataShard];
    const geometryShard = geometryWriters.indexOf(writer);
    if (geometryShard >= 0) return shardPackTables[geometryShard];
    if (writer === rootWriter) return rootWriter.packs.map(pack => pack.file);
    return [];
  };
  const packIndexOf = (writer, entry) => {
    const index = packTableOf(writer).indexOf(entry.pack);
    if (index < 0) throw new Error("Route graph pack entry does not resolve to a finalized pack.");
    return index;
  };
  const pointerOf = (writer, entry) => ({
    packIndex: packIndexOf(writer, entry),
    offset: entry.offset,
    length: entry.length,
    logicalLength: entry.logicalLength ?? 0,
    checksum: entry.checksum
  });

  const root = {
    sourceHash,
    nodeCount,
    edgeCount,
    levelFanouts,
    profile: graph.profile || "car",
    condRules: graph.condRules || [],
    // The distinct sign faces an edge's signId indexes into. Shared across
    // the whole graph the way conditional windows are: a route's steps read
    // them without another fetch.
    signs: graph.signs || [],
    limits: limitTable.list,
    banRules,
    charges: graph.charges || [],
    laneSigns: graph.laneSigns || [],
    classes,
    buckets,
    shards,
    leaves: leafEntries.map(leaf => ({
      firstNode: leaf.firstNode,
      nodeCount: leaf.nodeCount,
      bbox: leaf.bbox,
      shardIndex: leaf.shardIndex,
      pointer: pointerOf(writers[leaf.shardIndex], leaf.entry),
      geometryPointer: pointerOf(geometryWriters[leaf.shardIndex], leaf.geometryEntry)
    })),
    levels: levelEntries.map(level => level.map(cell => ({
      firstLeaf: cell.firstLeaf,
      leafCount: cell.leafCount,
      shardIndex: cell.shardIndex,
      pointers: cell.entries.map(entry => pointerOf(writers[cell.shardIndex], entry))
    }))),
    topOverlay: { shardIndex: topShardIndex, pointers: topEntries.map(top => pointerOf(rootWriter, top.entry)) },
    namesFile
  };
  const rootEncoded = encodeRouteRoot(root);
  const rootGz = gzipSync(Buffer.from(rootEncoded), { level: 6 });
  const rootHash = createHash("sha256").update(rootGz).digest("hex").slice(0, OBJECT_NAME_HASH_LENGTH);
  const rootFile = `root.${rootHash}.bin.gz`;
  writeFileSync(join(outDir, rootFile), rootGz);
  writeFileSync(join(outDir, "manifest.json"), JSON.stringify({
    format: ROUTE_GRAPH_FORMAT,
    root: rootFile,
    profile: graph.profile || "car",
    condRules: graph.condRules || [],
    nodes: nodeCount,
    edges: edgeCount,
    leaves: leaves.length,
    levels: levelFanouts,
    buckets: buckets.map(bucket => bucket.name),
    shards: shards.length - 1,
    portals,
    portalCandidates: Object.values(portalNeighbors).reduce((sum, values) => sum + routePortalCount(values), 0)
  }, null, 2));

  let overlayNodes = 0;
  for (const graphs of overlays) for (const overlay of graphs.values()) overlayNodes += overlay.nodes.length;
  const summary = {
    profile: graph.profile || "car",
    condRules: graph.condRules || [],
    nodes: nodeCount,
    edges: edgeCount,
    leaves: leaves.length,
    levelFanouts,
    topChildren,
    overlayNodes,
    topOverlayNodes: topOverlayGraph.nodes.length,
    buckets: buckets.map(bucket => bucket.name),
    shardCount,
    rootFile,
    rootBytes: rootGz.length
  };
  log(`route graph: ${JSON.stringify(summary)}`);
  // nodeOrder[i] = source-graph id of index node i; index node ids are the
  // KD order, so callers correlating with the source graph need this map.
  return { ...summary, nodeOrder: perm };
}

function writeShard(writer, key, compressed, logicalLength) {
  return writePackedShard(writer, key, compressed, { logicalLength, kind: "route-graph", codec: ROUTE_GRAPH_FORMAT });
}

// Merges the per-cell overlay graphs at the top child level into the single
// top overlay object (they are disjoint node sets plus the crossing edges
// whose LCA is the top region, already grouped under the root cell 0..n).
function mergeOverlayGraphs(graphs, level) {
  let nodeCount = 0;
  let edgeCount = 0;
  let maxNode = 0;
  for (const overlay of graphs.values()) {
    nodeCount += overlay.nodes.length;
    edgeCount += overlay.targetIndex.length;
    for (const node of overlay.nodes) if (node > maxNode) maxNode = node;
  }
  const nodes = new Uint32Array(nodeCount);
  let nodeCursor = 0;
  for (const overlay of graphs.values()) {
    nodes.set(overlay.nodes, nodeCursor);
    nodeCursor += overlay.nodes.length;
  }
  nodes.sort();
  const index = new Uint32Array(maxNode + 1);
  for (let i = 0; i < nodes.length; i++) index[nodes[i]] = i;
  const from = new Uint32Array(edgeCount);
  const to = new Uint32Array(edgeCount);
  const sourceWeights = new Uint32Array(edgeCount);
  const sourceIsClique = new Uint8Array(edgeCount);
  let edgeCursor = 0;
  for (const overlay of graphs.values()) {
    for (let i = 0; i < overlay.nodes.length; i++) {
      for (let e = overlay.rowStart[i]; e < overlay.rowStart[i + 1]; e++) {
        from[edgeCursor] = index[overlay.nodes[i]];
        to[edgeCursor] = index[overlay.nodes[overlay.targetIndex[e]]];
        sourceWeights[edgeCursor] = overlay.weights[e];
        sourceIsClique[edgeCursor] = overlay.isClique[e];
        edgeCursor++;
      }
    }
  }
  const csr = buildCsr(nodes.length, from, to);
  const weights = new Uint32Array(edgeCount);
  const isClique = new Uint8Array(edgeCount);
  for (let i = 0; i < edgeCount; i++) {
    weights[i] = sourceWeights[csr.edgeIds[i]];
    isClique[i] = sourceIsClique[csr.edgeIds[i]];
  }
  return { level, cellId: 0, nodes, rowStart: csr.rowStart, targetIndex: csr.targets, weights, isClique };
}
