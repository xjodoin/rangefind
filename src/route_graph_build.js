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

const OBJECT_NAME_HASH_LENGTH = 24;

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
  const cursor = Uint32Array.from(rowStart.subarray(0, nodeCount));
  for (let i = 0; i < from.length; i++) {
    const slot = cursor[from[i]]++;
    targets[slot] = to[i];
    edgeIds[slot] = i;
  }
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

// Drops clique edges that another boundary node realizes exactly:
// (u, v) is redundant when d(u, x) + d(x, v) === d(u, v) for some boundary
// x. Distances are preserved (weights are >= 1, so witness decompositions
// strictly shrink), and road-network cliques typically sparsify several-fold.
function pruneCliqueTriples(boundary, triples) {
  const count = boundary.length;
  if (count < 3 || !triples.length) return triples;
  const index = new Map();
  for (let i = 0; i < count; i++) index.set(boundary[i], i);
  const dist = new Float64Array(count * count).fill(Infinity);
  for (let t = 0; t < triples.length; t += 3) {
    dist[index.get(triples[t]) * count + index.get(triples[t + 1])] = triples[t + 2];
  }
  const kept = [];
  for (let t = 0; t < triples.length; t += 3) {
    const i = index.get(triples[t]);
    const j = index.get(triples[t + 1]);
    const direct = triples[t + 2];
    let redundant = false;
    for (let k = 0; k < count; k++) {
      if (k === i || k === j) continue;
      const viaK = dist[i * count + k] + dist[k * count + j];
      if (viaK <= direct) {
        redundant = true;
        break;
      }
    }
    if (!redundant) kept.push(triples[t], triples[t + 1], direct);
  }
  return kept;
}

// Dijkstra over a local CSR from one source; returns distances (Infinity
// when unreachable). Used for cliques over small per-cell graphs.
function localDijkstra(nodeCount, rowStart, targets, weights, source, dist, heap) {
  dist.fill(Infinity, 0, nodeCount);
  dist[source] = 0;
  heap.weights.length = 0;
  heap.values.length = 0;
  heap.push(0, source);
  while (heap.size) {
    const weight = heap.peekWeight();
    const node = heap.pop();
    if (weight !== dist[node]) continue;
    for (let e = rowStart[node]; e < rowStart[node + 1]; e++) {
      const next = weight + weights[e];
      if (next < dist[targets[e]]) {
        dist[targets[e]] = next;
        heap.push(next, targets[e]);
      }
    }
  }
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

  // 1. Partition and renumber into a locality-preserving order.
  const { perm, leaves } = kdPartition(graph.nodeLat, graph.nodeLon, leafNodes);
  const newId = new Uint32Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) newId[perm[i]] = i;
  const latE7 = new Int32Array(nodeCount);
  const lonE7 = new Int32Array(nodeCount);
  for (let i = 0; i < nodeCount; i++) {
    latE7[i] = graph.nodeLat[perm[i]];
    lonE7[i] = graph.nodeLon[perm[i]];
  }
  const edgeFrom = new Uint32Array(edgeCount);
  const edgeTo = new Uint32Array(edgeCount);
  for (let i = 0; i < edgeCount; i++) {
    edgeFrom[i] = newId[graph.edgeFrom[i]];
    edgeTo[i] = newId[graph.edgeTo[i]];
  }
  const csr = buildCsr(nodeCount, edgeFrom, edgeTo);
  const leafOfNode = new Uint32Array(nodeCount);
  for (let leaf = 0; leaf < leaves.length; leaf++) {
    leafOfNode.fill(leaf, leaves[leaf][0], leaves[leaf][1]);
  }

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
  for (let i = 0; i < edgeCount; i++) {
    const from = edgeFrom[i];
    const to = edgeTo[i];
    let lca = levelCount + 1;
    for (let level = 0; level <= levelCount; level++) {
      if (cellAtLevel(from, level) === cellAtLevel(to, level)) {
        lca = level;
        break;
      }
    }
    edgeLca[i] = lca;
    if (lca > maxLca[from]) maxLca[from] = lca;
    if (lca > maxLca[to]) maxLca[to] = lca;
  }

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
  const weightOf = (edgeId, bucket) => bucketWeight(graph.edgeWeightDs[edgeId], edgeClassOf(edgeId), buckets[bucket].factors);

  // 5. Bottom-up cliques per bucket. cliques[level] maps cellId -> flat
  // [u, v, w, ...] triples over nodes with maxLca > level, exact within the
  // cell for that bucket's metric.
  const heap = new MinHeap();
  const computeBucketOverlays = (bucket) => {
    const cliques = [];
    {
      const leafCliques = new Map();
      const dist = new Float64Array(leafNodes + 8);
      for (let leaf = 0; leaf < leaves.length; leaf++) {
        const [start, end] = leaves[leaf];
        const size = end - start;
        const localFrom = [];
        const localTo = [];
        const localWeight = [];
        for (let node = start; node < end; node++) {
          for (let e = csr.rowStart[node]; e < csr.rowStart[node + 1]; e++) {
            if (edgeLca[csr.edgeIds[e]] !== 0) continue;
            localFrom.push(node - start);
            localTo.push(csr.targets[e] - start);
            localWeight.push(weightOf(csr.edgeIds[e], bucket));
          }
        }
        const local = buildCsr(size, Uint32Array.from(localFrom), Uint32Array.from(localTo));
        const localWeights = new Uint32Array(localFrom.length);
        for (let i = 0; i < localFrom.length; i++) localWeights[i] = localWeight[local.edgeIds[i]];
        const boundary = [];
        for (let node = start; node < end; node++) {
          if (maxLca[node] >= 1) boundary.push(node - start);
        }
        const triples = [];
        const localDist = size <= dist.length ? dist : new Float64Array(size);
        for (const source of boundary) {
          localDijkstra(size, local.rowStart, local.targets, localWeights, source, localDist, heap);
          for (const target of boundary) {
            if (target === source || localDist[target] === Infinity) continue;
            triples.push(start + source, start + target, localDist[target]);
          }
        }
        leafCliques.set(leaf, pruneCliqueTriples(boundary.map(node => start + node), triples));
      }
      cliques.push(leafCliques);
    }

    // Overlay graphs per parent cell, and cliques over them for the next
    // level. overlays[level - 1] maps cellId -> { nodes, rowStart,
    // targetIndex, weights, isClique } for cells at `level`.
    const overlays = [];
    const buildOverlayGraphs = (level) => {
      const byCell = new Map();
      const cellOf = (node) => cellAtLevel(node, level);
      const entry = (cell) => {
        let value = byCell.get(cell);
        if (!value) {
          value = { nodes: [], edges: [] };
          byCell.set(cell, value);
        }
        return value;
      };
      for (let node = 0; node < nodeCount; node++) {
        if (maxLca[node] >= level) entry(cellOf(node)).nodes.push(node);
      }
      const childCliques = cliques[level - 1];
      for (const [, triples] of [...childCliques.entries()].sort((a, b) => a[0] - b[0])) {
        for (let i = 0; i < triples.length; i += 3) {
          entry(cellOf(triples[i])).edges.push([triples[i], triples[i + 1], triples[i + 2], 1]);
        }
      }
      for (let i = 0; i < edgeCount; i++) {
        if (edgeLca[i] !== level) continue;
        entry(cellOf(edgeFrom[i])).edges.push([edgeFrom[i], edgeTo[i], weightOf(i, bucket), 0]);
      }
      const result = new Map();
      for (const [cell, { nodes, edges }] of [...byCell.entries()].sort((a, b) => a[0] - b[0])) {
        nodes.sort((a, b) => a - b);
        const index = new Map(nodes.map((node, i) => [node, i]));
        const from = new Uint32Array(edges.length);
        const to = new Uint32Array(edges.length);
        for (let i = 0; i < edges.length; i++) {
          from[i] = index.get(edges[i][0]);
          to[i] = index.get(edges[i][1]);
        }
        const local = buildCsr(nodes.length, from, to);
        const weights = new Uint32Array(edges.length);
        const isClique = new Uint8Array(edges.length);
        const targetIndex = new Uint32Array(edges.length);
        for (let i = 0; i < edges.length; i++) {
          const edge = edges[local.edgeIds[i]];
          weights[i] = edge[2];
          isClique[i] = edge[3];
          targetIndex[i] = local.targets[i];
        }
        result.set(cell, { nodes: Uint32Array.from(nodes), rowStart: local.rowStart, targetIndex, weights, isClique });
      }
      return result;
    };

    for (let level = 1; level <= levelCount + 1; level++) {
      const graphs = buildOverlayGraphs(level);
      overlays.push(graphs);
      if (level > levelCount) break;
      const levelCliques = new Map();
      for (const [cell, overlay] of graphs) {
        const boundary = [];
        for (let i = 0; i < overlay.nodes.length; i++) {
          if (maxLca[overlay.nodes[i]] >= level + 1) boundary.push(i);
        }
        const dist = new Float64Array(overlay.nodes.length);
        const triples = [];
        for (const source of boundary) {
          localDijkstra(overlay.nodes.length, overlay.rowStart, overlay.targetIndex, overlay.weights, source, dist, heap);
          for (const target of boundary) {
            if (target === source || dist[target] === Infinity) continue;
            triples.push(overlay.nodes[source], overlay.nodes[target], dist[target]);
          }
        }
        levelCliques.set(cell, pruneCliqueTriples(boundary.map(local => overlay.nodes[local]), triples));
      }
      cliques.push(levelCliques);
    }
    return overlays;
  };
  const bucketOverlays = buckets.map((_, bucket) => computeBucketOverlays(bucket));
  const overlays = bucketOverlays[0];

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
  for (let s = 0; s < shardCount; s++) {
    const dir = shardCount === 1 ? "objects" : `shards/${String(s).padStart(2, "0")}`;
    shardDirs.push(dir);
    writers.push(createPackWriter(join(outDir, dir), packBytes, { dedupe: false }));
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
    const writtenGeometry = writeShard(writers[shardIndex], `geom-${leaf}`, compressedGeometry, encodedGeometry.length);
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

  const sourceHash = createHash("sha256")
    .update(Buffer.from(graph.nodeLat.buffer, graph.nodeLat.byteOffset, graph.nodeLat.byteLength))
    .update(Buffer.from(graph.edgeFrom.buffer, graph.edgeFrom.byteOffset, graph.edgeFrom.byteLength))
    .update(Buffer.from(graph.edgeWeightDs.buffer, graph.edgeWeightDs.byteOffset, graph.edgeWeightDs.byteLength))
    .digest("hex");

  const shards = writers.map((writer, s) => ({
    id: shardCount === 1 ? "all" : `shard-${String(s).padStart(2, "0")}`,
    dir: shardDirs[s],
    packs: writer.packs.map(pack => pack.file)
  }));
  shards.push({ id: "__top", dir: "top", packs: rootWriter.packs.map(pack => pack.file) });
  const topShardIndex = shards.length - 1;

  const packIndexOf = (writer, entry) => {
    const index = writer.packs.findIndex(pack => pack.file === entry.pack);
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
    classes,
    buckets,
    shards,
    leaves: leafEntries.map(leaf => ({
      firstNode: leaf.firstNode,
      nodeCount: leaf.nodeCount,
      bbox: leaf.bbox,
      shardIndex: leaf.shardIndex,
      pointer: pointerOf(writers[leaf.shardIndex], leaf.entry),
      geometryPointer: pointerOf(writers[leaf.shardIndex], leaf.geometryEntry)
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
    nodes: nodeCount,
    edges: edgeCount,
    leaves: leaves.length,
    levels: levelFanouts,
    buckets: buckets.map(bucket => bucket.name),
    shards: shards.length - 1
  }, null, 2));

  let overlayNodes = 0;
  for (const graphs of overlays) for (const overlay of graphs.values()) overlayNodes += overlay.nodes.length;
  const summary = {
    profile: graph.profile || "car",
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
  const nodeList = [];
  const edges = [];
  for (const [, overlay] of [...graphs.entries()].sort((a, b) => a[0] - b[0])) {
    for (let i = 0; i < overlay.nodes.length; i++) {
      for (let e = overlay.rowStart[i]; e < overlay.rowStart[i + 1]; e++) {
        edges.push([overlay.nodes[i], overlay.nodes[overlay.targetIndex[e]], overlay.weights[e], overlay.isClique[e]]);
      }
    }
    for (const node of overlay.nodes) nodeList.push(node);
  }
  nodeList.sort((a, b) => a - b);
  const nodes = Uint32Array.from(nodeList);
  const index = new Map();
  for (let i = 0; i < nodes.length; i++) index.set(nodes[i], i);
  const from = new Uint32Array(edges.length);
  const to = new Uint32Array(edges.length);
  for (let i = 0; i < edges.length; i++) {
    from[i] = index.get(edges[i][0]);
    to[i] = index.get(edges[i][1]);
  }
  const csr = buildCsr(nodes.length, from, to);
  const weights = new Uint32Array(edges.length);
  const isClique = new Uint8Array(edges.length);
  for (let i = 0; i < edges.length; i++) {
    weights[i] = edges[csr.edgeIds[i]][2];
    isClique[i] = edges[csr.edgeIds[i]][3];
  }
  return { level, cellId: 0, nodes, rowStart: csr.rowStart, targetIndex: csr.targets, weights, isClique };
}
