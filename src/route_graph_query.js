// Query engine for rfroutegraph-v1 static route graphs.
//
// A point-to-point query fetches a fixed, bounded object set computed from
// the two endpoints alone — the snap leaf cells plus one overlay per
// ancestor level per endpoint plus the shared top overlay — in one parallel
// wave, then runs a multilevel bidirectional Dijkstra entirely client-side.
// Path geometry is unpacked afterwards by expanding clique edges through
// their child cells, which touches only the cells the route passes through.
//
// Platform-neutral: all I/O goes through an injected adapter
//   { readFile(path) -> bytes, readRange(path, offset, length) -> bytes }
// and gzip inflation uses DecompressionStream unless `inflate` is supplied.

import {
  MinHeap,
  bucketWeight,
  decodeEdgeGeometry,
  decodeRouteCell,
  decodeRouteOverlay,
  decodeRouteRoot
} from "./route_graph.js";

const EARTH_RADIUS_METERS = 6371008.7714;
const E7_RAD = Math.PI / 180 / 1e7;

function routeError(code, message) {
  const error = new Error(message);
  error.code = code;
  return error;
}

async function defaultInflate(bytes) {
  if (typeof DecompressionStream !== "function") {
    throw new Error("Route graph queries need an inflate implementation on this platform.");
  }
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function sha256Hex(bytes) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)].map(byte => byte.toString(16).padStart(2, "0")).join("");
}

function metersPerLatE7() {
  return EARTH_RADIUS_METERS * E7_RAD;
}

function haversineMetersE7(latA, lonA, latB, lonB) {
  const dLat = (latB - latA) * E7_RAD;
  const dLon = (lonB - lonA) * E7_RAD;
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const a = sinLat * sinLat + Math.cos(latA * E7_RAD) * Math.cos(latB * E7_RAD) * sinLon * sinLon;
  return 2 * EARTH_RADIUS_METERS * Math.asin(Math.min(1, Math.sqrt(a)));
}

function pointToBboxMeters(latE7, lonE7, bbox) {
  const clampedLat = Math.min(Math.max(latE7, bbox.minLat), bbox.maxLat);
  const clampedLon = Math.min(Math.max(lonE7, bbox.minLon), bbox.maxLon);
  return haversineMetersE7(latE7, lonE7, clampedLat, clampedLon);
}

// HTTP(S) io adapter for browsers and any fetch-capable host. Whole files
// are plain GETs; objects use single-range requests against the immutable
// content-addressed packs, tolerating servers that answer 200 with the
// full body by slicing client-side.
export function createRouteGraphHttpIo(baseUrl, options = {}) {
  const fetchImpl = options.fetch || ((...args) => fetch(...args));
  const base = String(baseUrl).endsWith("/") ? String(baseUrl) : `${baseUrl}/`;
  const counters = { requests: 0, bytes: 0, files: new Set() };
  return {
    async readFile(path) {
      counters.requests++;
      counters.files.add(path);
      const response = await fetchImpl(`${base}${path}`);
      if (!response.ok) throw routeError("RANGEFIND_ROUTE_FETCH", `GET ${path} failed with ${response.status}.`);
      const bytes = new Uint8Array(await response.arrayBuffer());
      counters.bytes += bytes.length;
      return bytes;
    },
    async readRange(path, offset, length) {
      counters.requests++;
      counters.files.add(path);
      counters.bytes += length;
      const response = await fetchImpl(`${base}${path}`, {
        headers: { Range: `bytes=${offset}-${offset + length - 1}` }
      });
      if (response.status === 206) {
        const bytes = new Uint8Array(await response.arrayBuffer());
        if (bytes.length !== length) {
          throw routeError("RANGEFIND_ROUTE_FETCH", `Range read from ${path} returned ${bytes.length} of ${length} bytes.`);
        }
        return bytes;
      }
      if (response.status === 200) {
        const body = new Uint8Array(await response.arrayBuffer());
        return body.subarray(offset, offset + length);
      }
      throw routeError("RANGEFIND_ROUTE_FETCH", `Range GET ${path} failed with ${response.status}.`);
    },
    counters,
    resetCounters() {
      counters.requests = 0;
      counters.bytes = 0;
      counters.files.clear();
    }
  };
}

// Convenience: open a route graph served over HTTP(S).
export async function openRouteGraphUrl(baseUrl, options = {}) {
  const io = options.io || createRouteGraphHttpIo(baseUrl, options);
  const engine = await openRouteGraph({ ...options, io });
  engine.io = io;
  return engine;
}

export async function openRouteGraph(options) {
  const io = options.io;
  if (!io?.readFile || !io?.readRange) throw new Error("openRouteGraph requires an io adapter with readFile and readRange.");
  const inflate = options.inflate || defaultInflate;
  const verify = options.verifyChecksums !== false && typeof crypto !== "undefined" && crypto.subtle;

  const manifest = JSON.parse(new TextDecoder().decode(await io.readFile("manifest.json")));
  const root = decodeRouteRoot(await inflate(await io.readFile(manifest.root)));
  const levelCount = root.levelFanouts.length;
  const cumFanout = [1];
  for (const fanout of root.levelFanouts) cumFanout.push(cumFanout[cumFanout.length - 1] * fanout);
  const leafFirstNode = new Uint32Array(root.leaves.length);
  for (let i = 0; i < root.leaves.length; i++) leafFirstNode[i] = root.leaves[i].firstNode;

  const stats = {
    objectFetches: 0,
    bytesFetched: 0,
    cellFetches: 0,
    overlayFetches: 0,
    unpackCellFetches: 0,
    shardsTouched: new Set()
  };

  const objectCache = new Map();
  async function fetchObject(shardIndex, pointer, kind) {
    const key = `${shardIndex}:${pointer.packIndex}:${pointer.offset}`;
    let promise = objectCache.get(key);
    if (!promise) {
      promise = (async () => {
        const shard = root.shards[shardIndex];
        const path = `${shard.dir}/${shard.packs[pointer.packIndex]}`;
        const compressed = await io.readRange(path, pointer.offset, pointer.length);
        stats.objectFetches++;
        stats.bytesFetched += compressed.length;
        stats.shardsTouched.add(shard.id);
        if (verify) {
          const digest = await sha256Hex(compressed);
          if (digest !== pointer.checksum.value) {
            throw new Error(`Route graph object checksum mismatch in ${path} @${pointer.offset}.`);
          }
        }
        return inflate(compressed);
      })();
      objectCache.set(key, promise);
      promise.catch(() => objectCache.delete(key));
    } else {
      kind = null;
    }
    if (kind === "cell") stats.cellFetches++;
    else if (kind === "overlay") stats.overlayFetches++;
    else if (kind === "unpack") stats.unpackCellFetches++;
    return promise;
  }

  const cellCache = new Map();
  function loadCell(leaf, kind = "cell") {
    let promise = cellCache.get(leaf);
    if (!promise) {
      const entry = root.leaves[leaf];
      promise = fetchObject(entry.shardIndex, entry.pointer, kind).then(bytes => {
        const cell = decodeRouteCell(bytes);
        cell.reverse = null;
        return cell;
      });
      cellCache.set(leaf, promise);
      promise.catch(() => cellCache.delete(leaf));
    }
    return promise;
  }

  const overlayCache = new Map();
  function loadOverlay(bucket, level, cellId) {
    const key = `${bucket}:${level}:${cellId}`;
    let promise = overlayCache.get(key);
    if (!promise) {
      const entry = level > levelCount
        ? { shardIndex: root.topOverlay.shardIndex, pointer: root.topOverlay.pointers[bucket] }
        : { shardIndex: root.levels[level - 1][cellId].shardIndex, pointer: root.levels[level - 1][cellId].pointers[bucket] };
      promise = fetchObject(entry.shardIndex, entry.pointer, "overlay").then(bytes => {
        const overlay = decodeRouteOverlay(bytes);
        overlay.index = new Map();
        for (let i = 0; i < overlay.nodes.length; i++) overlay.index.set(overlay.nodes[i], i);
        overlay.reverse = null;
        return overlay;
      });
      overlayCache.set(key, promise);
      promise.catch(() => overlayCache.delete(key));
    }
    return promise;
  }

  // Time-of-day metric selection: explicit bucket name, or departureTime
  // matched against each bucket's day/hour rules, falling back to base.
  function bucketForParams(params) {
    if (params?.bucket != null) {
      const index = root.buckets.findIndex(bucket => bucket.name === params.bucket);
      if (index < 0) {
        throw routeError("RANGEFIND_ROUTE_BAD_BUCKET", `Unknown time bucket "${params.bucket}"; index has ${root.buckets.map(bucket => bucket.name).join(", ")}.`);
      }
      return index;
    }
    if (params?.departureTime != null) {
      const date = new Date(params.departureTime);
      if (Number.isNaN(date.getTime())) {
        throw routeError("RANGEFIND_ROUTE_BAD_POINT", `Invalid departureTime ${JSON.stringify(params.departureTime)}.`);
      }
      const day = date.getDay();
      const hour = date.getHours();
      for (let i = root.buckets.length - 1; i >= 1; i--) {
        for (const rule of root.buckets[i].rules) {
          if ((rule.dayMask >> day) & 1 && hour >= rule.startHour && hour < rule.endHour) return i;
        }
      }
    }
    return 0;
  }

  const cellEdgeWeight = (cell, edge, factors) => bucketWeight(cell.weights[edge], cell.classes[edge], factors);

  let namesPromise = null;
  function loadNames() {
    if (!namesPromise) {
      namesPromise = io.readFile(root.namesFile).then(async bytes => JSON.parse(new TextDecoder().decode(await inflate(bytes))));
    }
    return namesPromise;
  }

  function leafOfNode(node) {
    let low = 0;
    let high = leafFirstNode.length - 1;
    while (low < high) {
      const mid = (low + high + 1) >> 1;
      if (leafFirstNode[mid] <= node) low = mid;
      else high = mid - 1;
    }
    return low;
  }

  const cellAtLevel = (leaf, level) => (level > levelCount ? 0 : Math.floor(leaf / cumFanout[level]));

  // --- Snapping ---------------------------------------------------------

  function projectToEdge(latE7, lonE7, points, cosLat) {
    // Equirectangular local projection; exact enough for snapping.
    const scale = metersPerLatE7();
    let best = { distMeters: Infinity, segment: 0, ratio: 0, latE7: 0, lonE7: 0, alongMeters: 0 };
    let alongBase = 0;
    for (let i = 0; i + 3 < points.length; i += 2) {
      const ax = (points[i + 1] - lonE7) * cosLat * scale;
      const ay = (points[i] - latE7) * scale;
      const bx = (points[i + 3] - lonE7) * cosLat * scale;
      const by = (points[i + 2] - latE7) * scale;
      const dx = bx - ax;
      const dy = by - ay;
      const lengthSq = dx * dx + dy * dy;
      const t = lengthSq > 0 ? Math.min(1, Math.max(0, -(ax * dx + ay * dy) / lengthSq)) : 0;
      const px = ax + t * dx;
      const py = ay + t * dy;
      const dist = Math.sqrt(px * px + py * py);
      const segMeters = Math.sqrt(lengthSq);
      if (dist < best.distMeters) {
        best = {
          distMeters: dist,
          segment: i / 2,
          ratio: t,
          latE7: Math.round(points[i] + t * (points[i + 2] - points[i])),
          lonE7: Math.round(points[i + 1] + t * (points[i + 3] - points[i + 1])),
          alongMeters: alongBase + t * segMeters
        };
      }
      alongBase += segMeters;
    }
    best.totalMeters = alongBase;
    return best;
  }

  const snapCache = new Map();
  const defaultMaxSnapMeters = Number(options.maxSnapMeters ?? 250);

  async function snap(point, { maxCandidates = 8, extraMeters = 25, maxSnapMeters = defaultMaxSnapMeters } = {}) {
    const lat = Number(point?.lat);
    const lon = Number(point?.lon ?? point?.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
      throw routeError("RANGEFIND_ROUTE_BAD_POINT", `Route points need finite lat/lon; got ${JSON.stringify(point)}.`);
    }
    const cacheKey = `${Math.round(lat * 1e7)}:${Math.round(lon * 1e7)}:${maxCandidates}:${extraMeters}`;
    const cached = snapCache.get(cacheKey);
    if (cached) {
      const result = await cached;
      enforceSnapDistance(result, maxSnapMeters, point);
      return result;
    }
    const promise = snapUncached({ lat, lon }, maxCandidates, extraMeters);
    if (snapCache.size >= 512) snapCache.clear();
    snapCache.set(cacheKey, promise);
    promise.catch(() => snapCache.delete(cacheKey));
    const result = await promise;
    enforceSnapDistance(result, maxSnapMeters, point);
    return result;
  }

  function enforceSnapDistance(result, maxSnapMeters, point) {
    if (maxSnapMeters > 0 && result.matches[0].distMeters > maxSnapMeters) {
      throw routeError(
        "RANGEFIND_ROUTE_SNAP_TOO_FAR",
        `Nearest road is ${Math.round(result.matches[0].distMeters)}m from (${point.lat}, ${point.lon ?? point.lng}); limit is ${maxSnapMeters}m.`
      );
    }
  }

  async function snapUncached(point, maxCandidates, extraMeters) {
    const latE7 = Math.round(point.lat * 1e7);
    const lonE7 = Math.round((point.lon ?? point.lng) * 1e7);
    const cosLat = Math.max(0.05, Math.cos(latE7 * E7_RAD));
    const byDistance = root.leaves
      .map((leaf, index) => ({ index, meters: pointToBboxMeters(latE7, lonE7, leaf.bbox) }))
      .sort((a, b) => a.meters - b.meters);
    // Fetch the nearest leaf plus any other leaf whose bbox could beat the
    // current best snap once padded for geometry that strays outside.
    const candidates = [];
    for (const { index, meters } of byDistance) {
      if (candidates.length >= 4) break;
      if (candidates.length && meters > byDistance[0].meters + 2000) break;
      candidates.push(index);
    }
    const cells = await Promise.all(candidates.map(leaf => loadCell(leaf)));
    const matches = [];
    for (const cell of cells) {
      for (let node = 0; node < cell.nodeCount; node++) {
        for (let e = cell.rowStart[node]; e < cell.rowStart[node + 1]; e++) {
          const target = cell.targets[e];
          const inCell = target >= cell.firstNode && target < cell.firstNode + cell.nodeCount;
          // Cross-cell edges carry their far endpoint inline (v2 cells), so
          // every edge projects against its full polyline.
          const points = decodeEdgeGeometry(
            cell.geometry[e],
            cell.latE7[node],
            cell.lonE7[node],
            inCell ? cell.latE7[target - cell.firstNode] : cell.extLat[e],
            inCell ? cell.lonE7[target - cell.firstNode] : cell.extLon[e]
          );
          const projected = projectToEdge(latE7, lonE7, points, cosLat);
          if (projected.distMeters === Infinity) continue;
          matches.push({
            leaf: cell.cellId,
            edgeIndex: e,
            fromNode: cell.firstNode + node,
            toNode: target,
            weight: cell.weights[e],
            classCode: cell.classes[e],
            distDm: cell.distsDm[e],
            nameId: cell.nameIds[e],
            distMeters: projected.distMeters,
            ratio: projected.totalMeters > 0 ? projected.alongMeters / projected.totalMeters : 0,
            snappedLatE7: projected.latE7,
            snappedLonE7: projected.lonE7
          });
        }
      }
    }
    matches.sort((a, b) => a.distMeters - b.distMeters || a.edgeIndex - b.edgeIndex);
    if (!matches.length) {
      throw routeError("RANGEFIND_ROUTE_SNAP_TOO_FAR", "Route graph snap found no edges near the requested point.");
    }
    const best = matches[0];
    const kept = matches.filter(match => match.distMeters <= best.distMeters + extraMeters).slice(0, maxCandidates);
    return { latE7, lonE7, matches: kept };
  }

  // --- Multilevel bidirectional Dijkstra --------------------------------

  function buildContexts(snapLeaves) {
    const contextLeaves = new Set(snapLeaves);
    const contextCells = [];
    for (let level = 1; level <= levelCount; level++) {
      const cells = new Set();
      for (const leaf of snapLeaves) cells.add(cellAtLevel(leaf, level));
      contextCells.push(cells);
    }
    return { contextLeaves, contextCells };
  }

  async function fetchContexts(contexts, bucket) {
    const cellPromises = [...contexts.contextLeaves].sort((a, b) => a - b).map(leaf => [leaf, loadCell(leaf)]);
    const overlayPromises = [];
    for (let level = 1; level <= levelCount; level++) {
      for (const cell of [...contexts.contextCells[level - 1]].sort((a, b) => a - b)) {
        overlayPromises.push([`${level}:${cell}`, loadOverlay(bucket, level, cell)]);
      }
    }
    overlayPromises.push([`top`, loadOverlay(bucket, levelCount + 1, 0)]);
    const cells = new Map();
    for (const [leaf, promise] of cellPromises) cells.set(leaf, await promise);
    const overlays = new Map();
    for (const [key, promise] of overlayPromises) overlays.set(key, await promise);
    return { cells, overlays };
  }

  function reverseCell(cell) {
    if (cell.reverse) return cell.reverse;
    // In-cell reverse CSR for internal targets, plus an external map for
    // cross-cell targets so the backward search sees incoming raw edges.
    const degree = new Uint32Array(cell.nodeCount + 1);
    const edgeCount = cell.rowStart[cell.nodeCount];
    for (let e = 0; e < edgeCount; e++) {
      const target = cell.targets[e];
      if (target >= cell.firstNode && target < cell.firstNode + cell.nodeCount) degree[target - cell.firstNode + 1]++;
    }
    for (let i = 0; i < cell.nodeCount; i++) degree[i + 1] += degree[i];
    const sources = new Uint32Array(degree[cell.nodeCount]);
    const edgeIds = new Uint32Array(degree[cell.nodeCount]);
    const cursor = Uint32Array.from(degree.subarray(0, cell.nodeCount));
    const external = new Map();
    for (let node = 0; node < cell.nodeCount; node++) {
      for (let e = cell.rowStart[node]; e < cell.rowStart[node + 1]; e++) {
        const target = cell.targets[e];
        if (target < cell.firstNode || target >= cell.firstNode + cell.nodeCount) {
          let list = external.get(target);
          if (!list) {
            list = [];
            external.set(target, list);
          }
          list.push(cell.firstNode + node, e);
          continue;
        }
        const slot = cursor[target - cell.firstNode]++;
        sources[slot] = cell.firstNode + node;
        edgeIds[slot] = e;
      }
    }
    cell.reverse = { rowStart: degree, sources, edgeIds, external };
    return cell.reverse;
  }

  function reverseOverlay(overlay) {
    if (overlay.reverse) return overlay.reverse;
    const nodeCount = overlay.nodes.length;
    const degree = new Uint32Array(nodeCount + 1);
    const edgeCount = overlay.rowStart[nodeCount];
    for (let e = 0; e < edgeCount; e++) degree[overlay.targetIndex[e] + 1]++;
    for (let i = 0; i < nodeCount; i++) degree[i + 1] += degree[i];
    const sources = new Uint32Array(edgeCount);
    const edgeIds = new Uint32Array(edgeCount);
    const cursor = Uint32Array.from(degree.subarray(0, nodeCount));
    for (let node = 0; node < nodeCount; node++) {
      for (let e = overlay.rowStart[node]; e < overlay.rowStart[node + 1]; e++) {
        const slot = cursor[overlay.targetIndex[e]]++;
        sources[slot] = node;
        edgeIds[slot] = e;
      }
    }
    overlay.reverse = { rowStart: degree, sources, edgeIds };
    return overlay.reverse;
  }

  // The query graph is the union of every fetched object's edges. Every
  // clique edge's weight equals an exact shortest-path length through its
  // child cell, so redundant memberships (a node appearing in several
  // fetched objects) can never produce a distance below the true one, and
  // the CRP region decomposition guarantees every needed edge is present in
  // at least one fetched object. Relaxing the union is therefore both
  // complete and exact, and needs no region bookkeeping.
  function searchQueryGraph(contexts, fetched, forwardSeeds, backwardSeeds, factors, penalized) {
    const INF = Infinity;
    // Optional query-graph edge penalties (alternative-route computation):
    // multiplies specific (from, to) transitions without refetching.
    const penaltyFor = penalized
      ? (from, to, weight) => (penalized.has(`${from}:${to}`) ? Math.round(weight * penalized.factor) : weight)
      : (from, to, weight) => weight;
    const cellList = [...fetched.cells.values()];
    const overlayEntries = [...fetched.overlays.entries()].map(([key, overlay]) => {
      const [levelText, cellText] = key === "top" ? [levelCount + 1, 0] : key.split(":");
      return { overlay, level: Number(levelText), cell: Number(cellText) };
    });
    const distF = new Map();
    const distB = new Map();
    const prevF = new Map();
    const prevB = new Map();
    const heapF = new MinHeap();
    const heapB = new MinHeap();
    for (const seed of forwardSeeds) {
      if (seed.weight < (distF.get(seed.node) ?? INF)) {
        distF.set(seed.node, seed.weight);
        prevF.set(seed.node, { prev: -1, seed });
        heapF.push(seed.weight, seed.node);
      }
    }
    for (const seed of backwardSeeds) {
      if (seed.weight < (distB.get(seed.node) ?? INF)) {
        distB.set(seed.node, seed.weight);
        prevB.set(seed.node, { prev: -1, seed });
        heapB.push(seed.weight, seed.node);
      }
    }
    let best = INF;
    let meeting = -1;
    const settle = (node, dist, otherDist) => {
      const other = otherDist.get(node);
      if (other != null && dist + other < best) {
        best = dist + other;
        meeting = node;
      }
    };
    const relaxForward = (node, weight) => {
      for (const cell of cellList) {
        if (node < cell.firstNode || node >= cell.firstNode + cell.nodeCount) continue;
        const local = node - cell.firstNode;
        for (let e = cell.rowStart[local]; e < cell.rowStart[local + 1]; e++) {
          const target = cell.targets[e];
          const next = weight + penaltyFor(node, target, cellEdgeWeight(cell, e, factors));
          if (next < (distF.get(target) ?? INF)) {
            distF.set(target, next);
            prevF.set(target, { prev: node, kind: "raw", leaf: cell.cellId, edge: e });
            heapF.push(next, target);
          }
        }
      }
      for (const { overlay, level, cell } of overlayEntries) {
        const index = overlay.index.get(node);
        if (index == null) continue;
        for (let e = overlay.rowStart[index]; e < overlay.rowStart[index + 1]; e++) {
          const target = overlay.nodes[overlay.targetIndex[e]];
          const next = weight + penaltyFor(node, target, overlay.weights[e]);
          if (next < (distF.get(target) ?? INF)) {
            distF.set(target, next);
            prevF.set(target, {
              prev: node,
              kind: overlay.isClique[e] ? "clique" : "crossing",
              level,
              cell,
              weight: overlay.weights[e]
            });
            heapF.push(next, target);
          }
        }
      }
    };
    const relaxBackward = (node, weight) => {
      for (const cell of cellList) {
        const reverse = reverseCell(cell);
        if (node >= cell.firstNode && node < cell.firstNode + cell.nodeCount) {
          const local = node - cell.firstNode;
          for (let e = reverse.rowStart[local]; e < reverse.rowStart[local + 1]; e++) {
            const edge = reverse.edgeIds[e];
            const source = reverse.sources[e];
            const next = weight + penaltyFor(source, node, cellEdgeWeight(cell, edge, factors));
            if (next < (distB.get(source) ?? INF)) {
              distB.set(source, next);
              prevB.set(source, { prev: node, kind: "raw", leaf: cell.cellId, edge });
              heapB.push(next, source);
            }
          }
        }
        const external = reverse.external.get(node);
        if (external) {
          for (let i = 0; i < external.length; i += 2) {
            const source = external[i];
            const edge = external[i + 1];
            const next = weight + penaltyFor(source, node, cellEdgeWeight(cell, edge, factors));
            if (next < (distB.get(source) ?? INF)) {
              distB.set(source, next);
              prevB.set(source, { prev: node, kind: "raw", leaf: cell.cellId, edge });
              heapB.push(next, source);
            }
          }
        }
      }
      for (const { overlay, level, cell } of overlayEntries) {
        const index = overlay.index.get(node);
        if (index == null) continue;
        const reverse = reverseOverlay(overlay);
        for (let e = reverse.rowStart[index]; e < reverse.rowStart[index + 1]; e++) {
          const edge = reverse.edgeIds[e];
          const source = overlay.nodes[reverse.sources[e]];
          const next = weight + penaltyFor(source, node, overlay.weights[edge]);
          if (next < (distB.get(source) ?? INF)) {
            distB.set(source, next);
            prevB.set(source, {
              prev: node,
              kind: overlay.isClique[edge] ? "clique" : "crossing",
              level,
              cell,
              weight: overlay.weights[edge]
            });
            heapB.push(next, source);
          }
        }
      }
    };
    const settledF = new Set();
    const settledB = new Set();
    while (heapF.size || heapB.size) {
      const topF = heapF.peekWeight();
      const topB = heapB.peekWeight();
      if (Math.min(topF, topB) >= best) break;
      if (topF <= topB && heapF.size) {
        const weight = heapF.peekWeight();
        const node = heapF.pop();
        if (weight !== distF.get(node) || settledF.has(node)) continue;
        settledF.add(node);
        settle(node, weight, distB);
        relaxForward(node, weight);
      } else if (heapB.size) {
        const weight = heapB.peekWeight();
        const node = heapB.pop();
        if (weight !== distB.get(node) || settledB.has(node)) continue;
        settledB.add(node);
        settle(node, weight, distF);
        relaxBackward(node, weight);
      }
    }
    return { best, meeting, distF, distB, prevF, prevB, settled: settledF.size + settledB.size };
  }

  // --- Path unpacking ---------------------------------------------------
  //
  // Breadth-wise: every round collects the objects the current frontier
  // needs (cells for raw/crossing edges, child cells or overlays for clique
  // edges), fetches them in one parallel wave, then expands. Rounds are
  // bounded by the level depth, so a long route costs a handful of waves
  // instead of one dependent fetch per edge.

  function unpackNeed(item) {
    const { step, fromNode } = item;
    if (step.kind === "raw") return { kind: "cell", leaf: step.leaf };
    if (step.kind === "crossing") return { kind: "cell", leaf: leafOfNode(fromNode) };
    const childLevel = step.level - 1;
    if (childLevel === 0) return { kind: "cell", leaf: leafOfNode(fromNode) };
    return { kind: "overlay", level: childLevel, cell: cellAtLevel(leafOfNode(fromNode), childLevel) };
  }

  async function unpackChain(items, bucket, factors) {
    const root = { children: items.map(item => ({ ...item, resolved: null, children: null })) };
    let frontier = root.children;
    while (frontier.length) {
      const loads = new Map();
      for (const item of frontier) {
        const need = unpackNeed(item);
        const key = need.kind === "cell" ? `c${need.leaf}` : `o${need.level}:${need.cell}`;
        if (!loads.has(key)) {
          loads.set(key, need.kind === "cell" ? loadCell(need.leaf, "unpack") : loadOverlay(bucket, need.level, need.cell));
        }
        item.needKey = key;
      }
      const resolvedLoads = new Map();
      await Promise.all([...loads.entries()].map(async ([key, promise]) => {
        resolvedLoads.set(key, await promise);
      }));
      const next = [];
      for (const item of frontier) {
        const object = resolvedLoads.get(item.needKey);
        const { step, fromNode, toNode } = item;
        if (step.kind === "raw") {
          item.resolved = [{ leaf: step.leaf, edge: step.edge, cell: object }];
        } else if (step.kind === "crossing") {
          const cell = object;
          const local = fromNode - cell.firstNode;
          let found = -1;
          for (let e = cell.rowStart[local]; e < cell.rowStart[local + 1]; e++) {
            if (cell.targets[e] === toNode && cellEdgeWeight(cell, e, factors) === step.weight) {
              found = e;
              break;
            }
          }
          if (found < 0) {
            for (let e = cell.rowStart[local]; e < cell.rowStart[local + 1]; e++) {
              if (cell.targets[e] === toNode) {
                found = e;
                break;
              }
            }
          }
          if (found < 0) throw routeError("RANGEFIND_ROUTE_UNPACK", "Route graph crossing edge not found during unpack.");
          item.resolved = [{ leaf: cell.cellId, edge: found, cell }];
        } else if (step.level - 1 === 0) {
          const cell = object;
          const path = dijkstraWithinCell(cell, fromNode, toNode, step.weight, factors);
          item.resolved = path.map(edge => ({ leaf: cell.cellId, edge, cell }));
        } else {
          const childLevel = step.level - 1;
          const childCell = cellAtLevel(leafOfNode(fromNode), childLevel);
          const steps = dijkstraWithinOverlay(object, fromNode, toNode, step.weight, childLevel, childCell);
          item.children = steps.map(sub => ({ ...sub, resolved: null, children: null }));
          next.push(...item.children);
        }
      }
      frontier = next;
    }
    const output = [];
    const flatten = (nodes) => {
      for (const node of nodes) {
        if (node.resolved) output.push(...node.resolved);
        else if (node.children) flatten(node.children);
      }
    };
    flatten(root.children);
    return output;
  }

  function dijkstraWithinCell(cell, fromNode, toNode, expectedWeight, factors) {
    const size = cell.nodeCount;
    const dist = new Float64Array(size).fill(Infinity);
    const prevEdge = new Int32Array(size).fill(-1);
    const prevNode = new Int32Array(size).fill(-1);
    const heap = new MinHeap();
    const source = fromNode - cell.firstNode;
    const targetLocal = toNode - cell.firstNode;
    dist[source] = 0;
    heap.push(0, source);
    while (heap.size) {
      const weight = heap.peekWeight();
      const node = heap.pop();
      if (weight !== dist[node]) continue;
      if (node === targetLocal) break;
      for (let e = cell.rowStart[node]; e < cell.rowStart[node + 1]; e++) {
        const target = cell.targets[e];
        if (target < cell.firstNode || target >= cell.firstNode + size) continue;
        const local = target - cell.firstNode;
        const next = weight + cellEdgeWeight(cell, e, factors);
        if (next < dist[local]) {
          dist[local] = next;
          prevEdge[local] = e;
          prevNode[local] = node;
          heap.push(next, local);
        }
      }
    }
    if (dist[targetLocal] !== expectedWeight) {
      throw new Error(`Route graph clique unpack mismatch: ${dist[targetLocal]} != ${expectedWeight}.`);
    }
    const edges = [];
    for (let node = targetLocal; node !== source; node = prevNode[node]) {
      edges.push(prevEdge[node]);
    }
    edges.reverse();
    return edges;
  }

  function dijkstraWithinOverlay(overlay, fromNode, toNode, expectedWeight, level, cell) {
    const size = overlay.nodes.length;
    const dist = new Float64Array(size).fill(Infinity);
    const prevIdx = new Int32Array(size).fill(-1);
    const prevEdge = new Int32Array(size).fill(-1);
    const heap = new MinHeap();
    const source = overlay.index.get(fromNode);
    const target = overlay.index.get(toNode);
    if (source == null || target == null) throw new Error("Route graph overlay unpack endpoints missing.");
    dist[source] = 0;
    heap.push(0, source);
    while (heap.size) {
      const weight = heap.peekWeight();
      const node = heap.pop();
      if (weight !== dist[node]) continue;
      if (node === target) break;
      for (let e = overlay.rowStart[node]; e < overlay.rowStart[node + 1]; e++) {
        const next = weight + overlay.weights[e];
        const to = overlay.targetIndex[e];
        if (next < dist[to]) {
          dist[to] = next;
          prevIdx[to] = node;
          prevEdge[to] = e;
          heap.push(next, to);
        }
      }
    }
    if (dist[target] !== expectedWeight) {
      throw new Error(`Route graph overlay unpack mismatch: ${dist[target]} != ${expectedWeight}.`);
    }
    const steps = [];
    for (let node = target; node !== source; node = prevIdx[node]) {
      const edge = prevEdge[node];
      steps.push({
        fromNode: overlay.nodes[prevIdx[node]],
        toNode: overlay.nodes[node],
        step: {
          kind: overlay.isClique[edge] ? "clique" : "crossing",
          level,
          cell,
          weight: overlay.weights[edge]
        }
      });
    }
    steps.reverse();
    return steps;
  }

  // --- Public API -------------------------------------------------------

  // Walks a finished bidirectional search back into an ordered list of
  // query-graph steps, plus the snap matches actually used.
  function reconstructChains(result) {
    const forwardChain = [];
    let startMatch = null;
    for (let node = result.meeting; ; ) {
      const step = result.prevF.get(node);
      if (!step || step.prev === -1) {
        startMatch = step?.seed?.match ?? null;
        break;
      }
      forwardChain.push({ node, step });
      node = step.prev;
    }
    forwardChain.reverse();
    const backwardChain = [];
    let endMatch = null;
    for (let node = result.meeting; ; ) {
      const step = result.prevB.get(node);
      if (!step || step.prev === -1) {
        endMatch = step?.seed?.match ?? null;
        break;
      }
      backwardChain.push({ node, step });
      node = step.prev;
    }
    const items = [
      ...forwardChain.map(({ node, step }) => ({ fromNode: step.prev, toNode: node, step })),
      ...backwardChain.map(({ node, step }) => ({ fromNode: node, toNode: step.prev, step }))
    ];
    return { items, startMatch, endMatch };
  }

  async function finishRoute(response, chain, sameEdgeUsed, bucket, factors, wantNames) {
    const rawEdges = sameEdgeUsed ? [] : await unpackChain(chain.items, bucket, factors);
    const names = wantNames ? await loadNames() : null;
    const geometry = [];
    let distanceMeters = 0;
    const steps = [];
    const edges = [];
    const pushPoint = (latE7, lonE7) => {
      const last = geometry[geometry.length - 1];
      if (last && last[0] === latE7 / 1e7 && last[1] === lonE7 / 1e7) return;
      geometry.push([latE7 / 1e7, lonE7 / 1e7]);
    };
    if (chain.startMatch) pushPoint(chain.startMatch.snappedLatE7, chain.startMatch.snappedLonE7);
    for (const raw of rawEdges) {
      const cell = raw.cell;
      const edge = raw.edge;
      // Locate the from-node of this CSR edge index.
      let low = 0;
      let high = cell.nodeCount;
      while (low < high) {
        const mid = (low + high) >> 1;
        if (cell.rowStart[mid + 1] <= edge) low = mid + 1;
        else high = mid;
      }
      const local = low;
      const target = cell.targets[edge];
      const inCell = target >= cell.firstNode && target < cell.firstNode + cell.nodeCount;
      const points = decodeEdgeGeometry(
        cell.geometry[edge],
        cell.latE7[local],
        cell.lonE7[local],
        inCell ? cell.latE7[target - cell.firstNode] : cell.extLat[edge],
        inCell ? cell.lonE7[target - cell.firstNode] : cell.extLon[edge]
      );
      for (let i = 0; i < points.length; i += 2) pushPoint(points[i], points[i + 1]);
      const meters = cell.distsDm[edge] / 10;
      const seconds = cellEdgeWeight(cell, edge, factors) / 10;
      distanceMeters += meters;
      edges.push({ leaf: raw.leaf, edge, seconds, meters });
      const name = names ? names[cell.nameIds[edge]] || "" : "";
      const last = steps[steps.length - 1];
      if (last && last.name === name) {
        last.meters += meters;
        last.seconds += seconds;
      } else {
        steps.push({ name, meters, seconds });
      }
    }
    if (chain.endMatch) pushPoint(chain.endMatch.snappedLatE7, chain.endMatch.snappedLonE7);
    response.distanceMeters = distanceMeters;
    response.geometry = geometry;
    response.steps = steps;
    response.edges = edges;
    // Recompute the exact unpenalized total from seeds plus raw edges; for
    // the primary route this equals the search result, for penalized
    // alternatives it replaces the inflated search weight.
    if (!sameEdgeUsed && chain.startMatch && chain.endMatch) {
      const seedF = Math.round(bucketWeight(chain.startMatch.weight, chain.startMatch.classCode, factors) * (1 - chain.startMatch.ratio));
      const seedB = Math.round(bucketWeight(chain.endMatch.weight, chain.endMatch.classCode, factors) * chain.endMatch.ratio);
      const edgeDs = rawEdges.reduce((sum, raw) => sum + cellEdgeWeight(raw.cell, raw.edge, factors), 0);
      response.seconds = (seedF + edgeDs + seedB) / 10;
    }
    return response;
  }

  // Live-weight re-ranking: adjusts a finished route's ETA with per-edge
  // multiplicative factors keyed by stable "leaf/edgeIndex" ids. This is a
  // re-rank of already-computed routes, not a re-route — the intended use
  // is picking among alternatives with fresh data.
  function adjustedSeconds(response, liveWeights) {
    if (!response.edges) return response.seconds;
    let adjusted = response.seconds;
    for (const edge of response.edges) {
      const factor = liveWeights.factors?.[`${edge.leaf}/${edge.edge}`];
      if (factor != null && Number.isFinite(factor) && factor > 0) {
        adjusted += edge.seconds * (factor - 1);
      }
    }
    return Math.round(adjusted * 10) / 10;
  }

  async function route(params) {
    const bucket = bucketForParams(params);
    const factors = root.buckets[bucket].factors;
    const alternativeCount = Math.min(3, Math.max(0, Math.floor(params.alternatives ?? 0)));
    // Alternatives need their exact unpenalized totals, which come from the
    // unpacked path, so they imply geometry.
    const wantGeometry = params.geometry !== false || alternativeCount > 0;
    const liveWeights = params.liveWeights || null;
    if (liveWeights?.epoch && liveWeights.epoch !== root.sourceHash) {
      throw routeError("RANGEFIND_ROUTE_STALE_LIVE", "liveWeights epoch does not match this index build.");
    }
    const [snapFrom, snapTo] = await Promise.all([snap(params.from), snap(params.to)]);
    const snapLeaves = new Set();
    for (const match of snapFrom.matches) {
      snapLeaves.add(match.leaf);
      // Cross-cell snap edges seed the search at a node in another leaf;
      // that leaf must join the context set so the seed has outgoing edges.
      snapLeaves.add(leafOfNode(match.toNode));
    }
    for (const match of snapTo.matches) {
      snapLeaves.add(match.leaf);
      snapLeaves.add(leafOfNode(match.fromNode));
    }
    const contexts = buildContexts([...snapLeaves]);
    const fetched = await fetchContexts(contexts, bucket);

    const effMatchWeight = (match) => bucketWeight(match.weight, match.classCode, factors);
    const forwardSeeds = snapFrom.matches.map(match => ({
      node: match.toNode,
      weight: Math.round(effMatchWeight(match) * (1 - match.ratio)),
      match
    }));
    const backwardSeeds = snapTo.matches.map(match => ({
      node: match.fromNode,
      weight: Math.round(effMatchWeight(match) * match.ratio),
      match
    }));

    // Same-edge special case: both points on one directed edge, in order.
    let sameEdge = null;
    for (const from of snapFrom.matches) {
      for (const to of snapTo.matches) {
        if (from.leaf === to.leaf && from.edgeIndex === to.edgeIndex && to.ratio >= from.ratio) {
          const weight = Math.round(effMatchWeight(from) * (to.ratio - from.ratio));
          if (!sameEdge || weight < sameEdge.weight) sameEdge = { weight, from, to };
        }
      }
    }

    const snapInfo = (snapResult) => ({
      snapped: { lat: snapResult.matches[0].snappedLatE7 / 1e7, lon: snapResult.matches[0].snappedLonE7 / 1e7 },
      snapDistanceMeters: Math.round(snapResult.matches[0].distMeters * 10) / 10
    });
    const buildResponse = async (searchResult, totalWeight, sameEdgeUsed) => {
      const response = {
        seconds: totalWeight / 10,
        bucket: root.buckets[bucket].name,
        settledNodes: searchResult ? searchResult.settled : 0,
        from: snapInfo(snapFrom),
        to: snapInfo(snapTo),
        stats: statsSnapshot()
      };
      if (!wantGeometry) return response;
      const chain = sameEdgeUsed
        ? { items: [], startMatch: sameEdge.from, endMatch: sameEdge.to }
        : reconstructChains(searchResult);
      await finishRoute(response, chain, sameEdgeUsed, bucket, factors, params.names !== false);
      response.stats = statsSnapshot();
      return response;
    };

    const primarySearch = searchQueryGraph(contexts, fetched, forwardSeeds, backwardSeeds, factors, null);
    let totalWeight = primarySearch.best;
    let usedSameEdge = false;
    if (sameEdge && sameEdge.weight <= totalWeight) {
      totalWeight = sameEdge.weight;
      usedSameEdge = true;
    }
    if (totalWeight === Infinity) {
      throw routeError("RANGEFIND_ROUTE_NO_PATH", "No route found between the requested points.");
    }
    const primary = await buildResponse(primarySearch, totalWeight, usedSameEdge);

    if (!alternativeCount || usedSameEdge) {
      if (liveWeights) primary.adjustedSeconds = adjustedSeconds(primary, liveWeights);
      return primary;
    }

    // Alternatives: penalize the transitions of every accepted route in the
    // already-fetched query graph and re-search — no extra fetches.
    const transitionsOf = (searchResult) => {
      const chain = reconstructChains(searchResult);
      return chain.items.map(item => `${item.fromNode}:${item.toNode}`);
    };
    const penalized = new Set(transitionsOf(primarySearch));
    penalized.factor = 1.3;
    const candidates = [primary];
    const searches = [primarySearch];
    for (let i = 0; i < alternativeCount; i++) {
      const alternativeSearch = searchQueryGraph(contexts, fetched, forwardSeeds, backwardSeeds, factors, penalized);
      if (alternativeSearch.best === Infinity) break;
      // Recompute the alternative's true (unpenalized) weight from its path.
      const transitions = transitionsOf(alternativeSearch);
      const overlap = transitions.filter(key => penalized.has(key)).length / Math.max(1, transitions.length);
      for (const key of transitions) penalized.add(key);
      if (overlap > 0.8) continue;
      // finishRoute recomputes the exact unpenalized total from the path.
      const alternative = await buildResponse(alternativeSearch, alternativeSearch.best, false);
      if (alternative.seconds > primary.seconds * 1.6) continue;
      candidates.push(alternative);
      searches.push(alternativeSearch);
    }
    if (liveWeights) {
      for (const candidate of candidates) candidate.adjustedSeconds = adjustedSeconds(candidate, liveWeights);
      candidates.sort((a, b) => (a.adjustedSeconds ?? a.seconds) - (b.adjustedSeconds ?? b.seconds));
    }
    const [best, ...rest] = candidates;
    best.alternatives = rest;
    return best;
  }

  async function matrix(params) {
    const points = params.points;
    const size = points.length;
    const seconds = Array.from({ length: size }, () => new Array(size).fill(0));
    for (let i = 0; i < size; i++) {
      for (let j = 0; j < size; j++) {
        if (i === j) continue;
        const result = await route({
          from: points[i],
          to: points[j],
          geometry: false,
          bucket: params.bucket,
          departureTime: params.departureTime
        });
        seconds[i][j] = result.seconds;
      }
    }
    return { seconds, stats: statsSnapshot() };
  }

  // Orders intermediate stops with exact Held-Karp up to 12 stops and a
  // nearest-neighbor + 2-opt heuristic beyond, then routes each leg.
  async function itinerary(params) {
    const stops = params.stops;
    if (!Array.isArray(stops) || stops.length < 2) throw new Error("Itineraries need at least two stops.");
    const roundTrip = params.roundTrip === true;
    const fixedEnd = !roundTrip;
    const { seconds } = await matrix({ points: stops, bucket: params.bucket, departureTime: params.departureTime });
    const size = stops.length;
    const interior = [];
    for (let i = 1; i < size - (fixedEnd ? 1 : 0); i++) interior.push(i);
    let order;
    if (interior.length <= 1) {
      order = [0, ...interior, ...(fixedEnd ? [size - 1] : [])];
    } else if (interior.length <= 10) {
      order = heldKarp(seconds, interior, fixedEnd ? size - 1 : 0, roundTrip);
    } else {
      order = twoOpt(seconds, interior, fixedEnd ? size - 1 : 0, roundTrip);
    }
    if (roundTrip) order = [...order, 0];
    const legs = [];
    let totalSeconds = 0;
    let totalMeters = 0;
    for (let i = 0; i + 1 < order.length; i++) {
      const leg = await route({
        from: stops[order[i]],
        to: stops[order[i + 1]],
        geometry: params.geometry !== false,
        bucket: params.bucket,
        departureTime: params.departureTime
      });
      legs.push({ ...leg, fromStop: order[i], toStop: order[i + 1] });
      totalSeconds += leg.seconds;
      totalMeters += leg.distanceMeters || 0;
    }
    return { order, legs, totalSeconds, totalMeters, stats: statsSnapshot() };
  }

  function heldKarp(seconds, interior, endIndex, roundTrip) {
    const n = interior.length;
    const full = 1 << n;
    const dp = Array.from({ length: full }, () => new Float64Array(n).fill(Infinity));
    const parent = Array.from({ length: full }, () => new Int32Array(n).fill(-1));
    for (let i = 0; i < n; i++) dp[1 << i][i] = seconds[0][interior[i]];
    for (let mask = 1; mask < full; mask++) {
      for (let last = 0; last < n; last++) {
        if (!(mask & (1 << last)) || dp[mask][last] === Infinity) continue;
        for (let next = 0; next < n; next++) {
          if (mask & (1 << next)) continue;
          const nextMask = mask | (1 << next);
          const cost = dp[mask][last] + seconds[interior[last]][interior[next]];
          if (cost < dp[nextMask][next]) {
            dp[nextMask][next] = cost;
            parent[nextMask][next] = last;
          }
        }
      }
    }
    let bestCost = Infinity;
    let bestLast = -1;
    for (let last = 0; last < n; last++) {
      const tail = roundTrip ? seconds[interior[last]][0] : seconds[interior[last]][endIndex];
      const cost = dp[full - 1][last] + tail;
      if (cost < bestCost) {
        bestCost = cost;
        bestLast = last;
      }
    }
    const orderInterior = [];
    let mask = full - 1;
    let last = bestLast;
    while (last >= 0) {
      orderInterior.push(interior[last]);
      const prev = parent[mask][last];
      mask ^= 1 << last;
      last = prev;
    }
    orderInterior.reverse();
    return roundTrip ? [0, ...orderInterior] : [0, ...orderInterior, endIndex];
  }

  function twoOpt(seconds, interior, endIndex, roundTrip) {
    // Nearest-neighbor construction.
    const remaining = new Set(interior);
    const tour = [0];
    let current = 0;
    while (remaining.size) {
      let best = -1;
      let bestCost = Infinity;
      for (const candidate of remaining) {
        if (seconds[current][candidate] < bestCost) {
          bestCost = seconds[current][candidate];
          best = candidate;
        }
      }
      tour.push(best);
      remaining.delete(best);
      current = best;
    }
    if (!roundTrip) tour.push(endIndex);
    // 2-opt improvement.
    const cost = (a, b) => seconds[tour[a]][tour[b]];
    let improved = true;
    while (improved) {
      improved = false;
      for (let i = 1; i + 1 < tour.length - 1; i++) {
        for (let j = i + 1; j < tour.length - 1; j++) {
          const before = cost(i - 1, i) + cost(j, j + 1);
          const after = seconds[tour[i - 1]][tour[j]] + seconds[tour[i]][tour[j + 1]];
          if (after + 1e-9 < before) {
            let low = i;
            let high = j;
            while (low < high) {
              const swap = tour[low];
              tour[low] = tour[high];
              tour[high] = swap;
              low++;
              high--;
            }
            improved = true;
          }
        }
      }
    }
    return tour;
  }

  function statsSnapshot() {
    return {
      objectFetches: stats.objectFetches,
      bytesFetched: stats.bytesFetched,
      cellFetches: stats.cellFetches,
      overlayFetches: stats.overlayFetches,
      unpackCellFetches: stats.unpackCellFetches,
      shardsTouched: [...stats.shardsTouched]
    };
  }

  function resetStats() {
    stats.objectFetches = 0;
    stats.bytesFetched = 0;
    stats.cellFetches = 0;
    stats.overlayFetches = 0;
    stats.unpackCellFetches = 0;
    stats.shardsTouched.clear();
  }

  function clearCaches() {
    objectCache.clear();
    cellCache.clear();
    overlayCache.clear();
  }

  return { root, route, matrix, itinerary, snap, stats: statsSnapshot, resetStats, clearCaches };
}
