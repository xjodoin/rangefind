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
  decodeRouteCell,
  decodeRouteGeometry,
  decodeRouteOverlay,
  decodeRouteRoot,
  edgePolyline
} from "./route_graph.js";

const EARTH_RADIUS_METERS = 6371008.7714;
const E7_RAD = Math.PI / 180 / 1e7;
// How far a snap candidate's direction may differ from the reported heading
// before it starts to look like a U-turn: free up to the first, charged in
// full past the second, ramped in between so a noisy compass does not flip
// the decision.
const HEADING_ALIGNED_DEG = 45;
const HEADING_OPPOSED_DEG = 135;
// Per-edge flag bits, matching the extractor's own encoding.
const EDGE_FLAG_ROUNDABOUT = 1;
// How much further than the nearest road a snap candidate may be and still
// be considered. Ordinary GPS error on an open road lives inside the first
// figure; a caller reporting worse accuracy can widen it up to the second,
// beyond which the "candidates" are just other roads in the neighbourhood.
const SNAP_EXTRA_METERS = 25;
const SNAP_EXTRA_METERS_MAX = 60;

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

/**
 * How close two junction markers of the same kind have to be before they are
 * treated as the one intersection. Sized for the width of a signalised
 * crossroads, and well under the gap between two intersections a driver would
 * ever count separately.
 */
const JUNCTION_MERGE_METERS = 30;

/**
 * Whether a junction marker describes the same intersection as the last one
 * reported. Same kind and close enough to be the far side of one crossroads
 * rather than the next one along.
 */
export function mergesWithPreviousJunction(previous, kind, latE7, lonE7) {
  if (!previous || previous.kind !== kind) return false;
  return haversineMetersE7(
    Math.round(previous.lat * 1e7),
    Math.round(previous.lon * 1e7),
    latE7,
    lonE7
  ) < JUNCTION_MERGE_METERS;
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

// In-memory provider: serves a fixed (or lazily computed) state list.
// The reference implementation of the provider contract — useful for
// tests, demo loopbacks, and adapting pre-fetched CDN sidecar payloads.
export function createStaticLiveProvider(states, options = {}) {
  return {
    name: options.name || "static-live",
    fetch({ epoch }) {
      if (options.epoch && options.epoch !== epoch) return [];
      return typeof states === "function" ? states({ epoch }) : states;
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

/**
 * A cell edge's lane movements, left to right, or an empty list when the way
 * carried no lane tags. Each entry is a bit set of the turns that lane allows.
 */
function laneListOf(cell, edge) {
  if (!cell || edge < 0 || !cell.laneOffsets || !cell.laneMasks) return [];
  const start = cell.laneOffsets[edge];
  const end = cell.laneOffsets[edge + 1];
  if (!(end > start)) return [];
  return Array.from(cell.laneMasks.subarray(start, end));
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
    httpRequests: 0,
    shardsTouched: new Set()
  };

  // Same-file reads issued within one microtask window coalesce into merged
  // Range requests when the gap between them is small — snap topology +
  // geometry pairs are byte-adjacent by construction, and each fetch wave
  // issues all its reads synchronously, so this collapses request counts
  // without changing any call site.
  const COALESCE_GAP_BYTES = 16 * 1024;
  const pendingReads = new Map();
  let flushScheduled = false;
  function coalescedRead(path, offset, length) {
    return new Promise((resolve, reject) => {
      let list = pendingReads.get(path);
      if (!list) pendingReads.set(path, (list = []));
      list.push({ offset, length, resolve, reject });
      if (!flushScheduled) {
        flushScheduled = true;
        queueMicrotask(flushReads);
      }
    });
  }
  function flushReads() {
    flushScheduled = false;
    const batches = [...pendingReads.entries()];
    pendingReads.clear();
    for (const [path, items] of batches) {
      items.sort((a, b) => a.offset - b.offset);
      let group = null;
      const groups = [];
      for (const item of items) {
        if (group && item.offset <= group.end + COALESCE_GAP_BYTES) {
          group.end = Math.max(group.end, item.offset + item.length);
          group.items.push(item);
        } else {
          group = { start: item.offset, end: item.offset + item.length, items: [item] };
          groups.push(group);
        }
      }
      for (const merged of groups) {
        stats.httpRequests++;
        io.readRange(path, merged.start, merged.end - merged.start).then(bytes => {
          for (const item of merged.items) {
            item.resolve(bytes.subarray(item.offset - merged.start, item.offset - merged.start + item.length));
          }
        }, error => {
          for (const item of merged.items) item.reject(error);
        });
      }
    }
  }

  const objectCache = new Map();
  async function fetchObject(shardIndex, pointer, kind) {
    const key = `${shardIndex}:${pointer.packIndex}:${pointer.offset}`;
    let promise = objectCache.get(key);
    if (!promise) {
      promise = (async () => {
        const shard = root.shards[shardIndex];
        const path = `${shard.dir}/${shard.packs[pointer.packIndex]}`;
        const compressed = await coalescedRead(path, pointer.offset, pointer.length);
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

  const geometryCache = new Map();
  function loadCellGeometry(leaf) {
    let promise = geometryCache.get(leaf);
    if (!promise) {
      const entry = root.leaves[leaf];
      promise = fetchObject(entry.shardIndex, entry.geometryPointer, "unpack").then(bytes => decodeRouteGeometry(bytes));
      geometryCache.set(leaf, promise);
      promise.catch(() => geometryCache.delete(leaf));
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

  // --- Live traffic providers -------------------------------------------
  //
  // A provider supplies ephemeral per-segment states (speeds, closures,
  // incident penalties) from any source: a P2P mesh such as PulseMesh, a
  // CDN-published delta sidecar, a municipal feed, or an in-memory test
  // loopback. The engine stays source-agnostic; the contract is:
  //
  //   provider.fetch({ epoch, areas: [{leaf, bbox°}], maxAgeSeconds })
  //     -> [{ segment, factor? | speedMps?, confidence?, observedAt?,
  //           closed?, penaltySeconds? }]
  //
  // `segment` is the stable physical directed-segment id "leaf/poly/dir":
  // every approach copy of one road edge (junction expansion) shares its
  // leaf's deduplicated canonical polyline, so one live state fans out to
  // all copies automatically. Ids are only valid for `epoch` ===
  // root.sourceHash; providers must return [] for unknown epochs.

  function edgeSegmentId(cell, edge) {
    const ref = cell.geomRefs[edge];
    return `${cell.cellId}/${ref >>> 1}/${ref & 1}`;
  }

  // Blend an observed state into a time multiplier over the static edge
  // weight: confidence decays with age, and a low-confidence report only
  // nudges the cost toward the observation (PulseMesh section 14 blend).
  function resolveLiveFactor(state, staticSeconds, nowMs) {
    if (state.closed === true) return { factor: Infinity, penaltyDs: 0 };
    let factor = Number(state.factor);
    if (!Number.isFinite(factor) && Number.isFinite(state.speedMps) && state.speedMps > 0 && staticSeconds > 0) {
      const meters = Number(state.meters);
      // Without meters we cannot turn a speed into a time; providers that
      // send speedMps should send meters too, or precompute `factor`.
      factor = Number.isFinite(meters) && meters > 0
        ? (meters / Math.max(1.4, state.speedMps)) / staticSeconds
        : NaN;
    }
    if (!Number.isFinite(factor) || factor <= 0) return null;
    let confidence = Number.isFinite(state.confidence) ? Math.max(0, Math.min(1, state.confidence)) : 1;
    if (Number.isFinite(state.observedAt)) {
      const ageSeconds = Math.max(0, (nowMs - state.observedAt) / 1000);
      confidence *= Math.exp(-ageSeconds / 60);
    }
    const blended = confidence * factor + (1 - confidence);
    const clamped = Math.max(0.25, Math.min(10, blended));
    const penaltyDs = Number.isFinite(state.penaltySeconds) && state.penaltySeconds > 0
      ? Math.round(Math.min(600, state.penaltySeconds) * 10)
      : 0;
    return { factor: clamped, penaltyDs };
  }

  // Resolves provider states into per-fetched-cell adjustment arrays.
  // Kept per query (never attached to the shared cell cache) so providers
  // and queries cannot leak into each other.
  async function buildLiveAdjustments(providerSpec, fetched, factors) {
    if (!providerSpec) return null;
    const provider = providerSpec.provider || providerSpec;
    if (typeof provider.fetch !== "function") {
      throw routeError("RANGEFIND_ROUTE_BAD_PROVIDER", "Live traffic providers must expose fetch().");
    }
    const cells = [...fetched.cells.values()];
    const areas = cells.map(cell => {
      const bbox = root.leaves[cell.cellId].bbox;
      return {
        leaf: cell.cellId,
        bbox: {
          minLat: bbox.minLat / 1e7,
          maxLat: bbox.maxLat / 1e7,
          minLon: bbox.minLon / 1e7,
          maxLon: bbox.maxLon / 1e7
        }
      };
    });
    const live = {
      name: provider.name || "live",
      byCell: new Map(),
      bySegment: new Map(),
      referencedLeaves: new Set(),
      dirtyKeys: new Set(),
      applied: 0,
      states: 0,
      error: null
    };
    let states;
    try {
      states = await provider.fetch({
        epoch: root.sourceHash,
        areas,
        maxAgeSeconds: providerSpec.maxAgeSeconds ?? 120
      });
    } catch (error) {
      // Live data is best-effort: degrade to the static metric.
      live.error = error?.message || String(error);
      return live;
    }
    if (!Array.isArray(states) || !states.length) return live;
    live.states = states.length;
    for (const state of states) {
      if (!state || typeof state.segment !== "string") continue;
      live.bySegment.set(state.segment, state);
      const leaf = Number(state.segment.split("/")[0]);
      if (Number.isInteger(leaf) && leaf >= 0 && leaf < root.leaves.length) live.referencedLeaves.add(leaf);
    }
    applyLiveToCells(live, cells, factors);
    return live;
  }

  // Resolves states onto the raw edges of the given fetched cells and
  // marks every ancestor cell of a live-adjusted leaf as dirty. Dirty
  // subtrees have their overlay shortcuts suppressed during the search,
  // forcing the descent that makes closures and jams exact under the live
  // metric (paths through unaffected sibling cells keep their shortcuts).
  function applyLiveToCells(live, cells, factors) {
    const nowMs = Date.now();
    for (const cell of cells) {
      if (live.byCell.has(cell)) continue;
      const edgeCount = cell.rowStart[cell.nodeCount];
      let factorsArr = null;
      let penalties = null;
      for (let e = 0; e < edgeCount; e++) {
        const state = live.bySegment.get(edgeSegmentId(cell, e));
        if (!state) continue;
        const resolved = resolveLiveFactor(state, cellEdgeWeight(cell, e, factors) / 10, nowMs);
        if (!resolved) continue;
        if (!factorsArr) {
          factorsArr = new Float64Array(edgeCount).fill(1);
          penalties = new Float64Array(edgeCount);
        }
        factorsArr[e] = resolved.factor;
        penalties[e] = resolved.penaltyDs;
        live.applied++;
      }
      if (factorsArr) {
        live.byCell.set(cell, { factors: factorsArr, penalties });
        for (let level = 0; level <= levelCount; level++) {
          live.dirtyKeys.add(`${level}:${cellAtLevel(cell.cellId, level)}`);
        }
      }
    }
  }

  // Applies live adjustments to one raw edge weight. Only the search uses
  // this — clique unpacking must keep exact static weights, and overlays
  // stay on the static metric (live detail belongs to the corridor; the
  // far field falls back to static, which is where live data is sparse
  // anyway).
  function liveAdjustedWeight(live, cell, edge, weight) {
    const entry = live?.byCell.get(cell);
    if (!entry) return weight;
    const factor = entry.factors[edge];
    if (factor === Infinity) return Infinity;
    if (factor === 1 && !entry.penalties[edge]) return weight;
    return Math.round(weight * factor) + entry.penalties[edge];
  }

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

  // Bearing of the polyline segment a point projected onto, in degrees
  // clockwise from north. Edge geometry is stored oriented along the edge's
  // travel direction, so this is the direction a vehicle on that edge is
  // actually moving — which is what tells the two directions of one road
  // apart when they snap to the same spot.
  function segmentBearing(points, segment, cosLat) {
    const i = segment * 2;
    if (i + 3 >= points.length) return null;
    const dLat = points[i + 2] - points[i];
    const dLon = (points[i + 3] - points[i + 1]) * cosLat;
    if (dLat === 0 && dLon === 0) return null;
    return (Math.atan2(dLon, dLat) * 180 / Math.PI + 360) % 360;
  }

  // The inverse of snap(): decode a physical segment's canonical
  // polyline and interpolate along it by arc length. Useful on its own
  // for rendering a snapped marker or replaying a matched trace, and
  // required by the thread channel, whose records carry position as
  // (segment, ratio) rather than coordinates.
  async function locate(segment, ratio = 0) {
    const parts = String(segment).split("/");
    if (parts.length !== 3) {
      throw routeError("RANGEFIND_ROUTE_BAD_SEGMENT", `Segment ids look like "leaf/polyline/direction"; got ${JSON.stringify(segment)}.`);
    }
    const leaf = Number(parts[0]);
    const polyline = Number(parts[1]);
    const direction = Number(parts[2]);
    if (!Number.isInteger(leaf) || leaf < 0 || leaf >= root.leaves.length) {
      throw routeError("RANGEFIND_ROUTE_BAD_SEGMENT", `Segment leaf ${parts[0]} is not in this graph.`);
    }
    if (!Number.isInteger(polyline) || polyline < 0 || (direction !== 0 && direction !== 1)) {
      throw routeError("RANGEFIND_ROUTE_BAD_SEGMENT", `Bad segment id ${JSON.stringify(segment)}.`);
    }
    const geometryBlock = await loadCellGeometry(leaf);
    if (polyline >= geometryBlock.polylines.length) {
      throw routeError("RANGEFIND_ROUTE_BAD_SEGMENT", `Segment ${segment} does not exist in leaf ${leaf}.`);
    }
    const points = edgePolyline(polyline * 2 + direction, geometryBlock);
    if (points.length < 2) {
      throw routeError("RANGEFIND_ROUTE_BAD_SEGMENT", `Segment ${segment} has no geometry.`);
    }
    const clamped = Math.max(0, Math.min(1, Number(ratio) || 0));
    // Interpolate by distance along the polyline, not by point index:
    // vertices cluster on curves, so index interpolation would slide the
    // marker toward every bend.
    const scale = metersPerLatE7();
    const cosLat = Math.cos(points[0] / 1e7 * Math.PI / 180);
    const spans = [];
    let total = 0;
    for (let i = 0; i + 3 < points.length; i += 2) {
      const dLat = (points[i + 2] - points[i]) * scale;
      const dLon = (points[i + 3] - points[i + 1]) * scale * cosLat;
      const span = Math.sqrt(dLat * dLat + dLon * dLon);
      spans.push(span);
      total += span;
    }
    if (total <= 0) return { lat: points[0] / 1e7, lon: points[1] / 1e7, segment, ratio: clamped };
    let target = clamped * total;
    for (let i = 0; i < spans.length; i++) {
      if (target > spans[i] && i < spans.length - 1) {
        target -= spans[i];
        continue;
      }
      const t = spans[i] > 0 ? Math.max(0, Math.min(1, target / spans[i])) : 0;
      const base = i * 2;
      return {
        lat: (points[base] + (points[base + 2] - points[base]) * t) / 1e7,
        lon: (points[base + 1] + (points[base + 3] - points[base + 1]) * t) / 1e7,
        segment,
        ratio: clamped
      };
    }
    return { lat: points[points.length - 2] / 1e7, lon: points[points.length - 1] / 1e7, segment, ratio: clamped };
  }

  const snapCache = new Map();
  const defaultMaxSnapMeters = Number(options.maxSnapMeters ?? 250);

  async function snap(point, { maxCandidates = 8, extraMeters = SNAP_EXTRA_METERS, maxSnapMeters = defaultMaxSnapMeters } = {}) {
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

  // Candidate snap leaves for a point, from root bboxes alone: the nearest
  // leaf plus any other leaf whose bbox could beat the best snap once padded
  // for geometry that strays outside. Shared by snapping and by the
  // speculative context planner, so their fetch sets coincide.
  function candidateLeavesFor(latE7, lonE7) {
    const byDistance = root.leaves
      .map((leaf, index) => ({ index, meters: pointToBboxMeters(latE7, lonE7, leaf.bbox) }))
      .sort((a, b) => a.meters - b.meters);
    const candidates = [];
    for (const { index, meters } of byDistance) {
      if (candidates.length >= 4) break;
      if (candidates.length && meters > byDistance[0].meters + 2000) break;
      candidates.push(index);
    }
    return candidates;
  }

  async function snapUncached(point, maxCandidates, extraMeters) {
    const latE7 = Math.round(point.lat * 1e7);
    const lonE7 = Math.round((point.lon ?? point.lng) * 1e7);
    const cosLat = Math.max(0.05, Math.cos(latE7 * E7_RAD));
    const candidates = candidateLeavesFor(latE7, lonE7);
    const cells = await Promise.all(candidates.map(leaf => Promise.all([loadCell(leaf), loadCellGeometry(leaf)])));
    const matches = [];
    for (const [cell, geometryBlock] of cells) {
      for (let node = 0; node < cell.nodeCount; node++) {
        for (let e = cell.rowStart[node]; e < cell.rowStart[node + 1]; e++) {
          const target = cell.targets[e];
          const points = edgePolyline(cell.geomRefs[e], geometryBlock);
          const projected = projectToEdge(latE7, lonE7, points, cosLat);
          if (projected.distMeters === Infinity) continue;
          matches.push({
            leaf: cell.cellId,
            edgeIndex: e,
            segment: edgeSegmentId(cell, e),
            fromNode: cell.firstNode + node,
            toNode: target,
            weight: cell.weights[e],
            classCode: cell.classes[e],
            distDm: cell.distsDm[e],
            nameId: cell.nameIds[e],
            distMeters: projected.distMeters,
            ratio: projected.totalMeters > 0 ? projected.alongMeters / projected.totalMeters : 0,
            snappedLatE7: projected.latE7,
            snappedLonE7: projected.lonE7,
            bearingDeg: segmentBearing(points, projected.segment, cosLat)
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
    // How much further off each candidate is than the nearest one. The
    // search charges for this, so a slip road twenty metres away stops
    // beating the road the car is actually on.
    for (const match of kept) match.extraMeters = match.distMeters - best.distMeters;
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
  function searchQueryGraph(contexts, fetched, forwardSeeds, backwardSeeds, factors, penalized, live) {
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
    // Per-node membership resolution, computed once per node instead of
    // scanning every fetched object on every relaxation.
    const membership = new Map();
    const membershipOf = (node) => {
      let entry = membership.get(node);
      if (entry) return entry;
      entry = { cells: null, overlays: null };
      for (const cell of cellList) {
        if (node >= cell.firstNode && node < cell.firstNode + cell.nodeCount) {
          (entry.cells ??= []).push(cell);
        } else if (reverseCell(cell).external.has(node)) {
          (entry.cells ??= []).push(cell);
        }
      }
      for (const overlayEntry of overlayEntries) {
        if (overlayEntry.overlay.index.has(node)) (entry.overlays ??= []).push(overlayEntry);
      }
      membership.set(node, entry);
      return entry;
    };
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
      const entry = membershipOf(node);
      for (const cell of entry.cells || []) {
        if (node < cell.firstNode || node >= cell.firstNode + cell.nodeCount) continue;
        const local = node - cell.firstNode;
        for (let e = cell.rowStart[local]; e < cell.rowStart[local + 1]; e++) {
          const target = cell.targets[e];
          const next = weight + penaltyFor(node, target, liveAdjustedWeight(live, cell, e, cellEdgeWeight(cell, e, factors)));
          if (next < (distF.get(target) ?? INF)) {
            distF.set(target, next);
            prevF.set(target, { prev: node, kind: "raw", leaf: cell.cellId, edge: e });
            heapF.push(next, target);
          }
        }
      }
      for (const { overlay, level, cell } of entry.overlays || []) {
        const index = overlay.index.get(node);
        if (index == null) continue;
        const dirtyChildKey = live?.dirtyKeys.size
          ? `${level - 1}:${cellAtLevel(leafOfNode(node), level - 1)}`
          : null;
        for (let e = overlay.rowStart[index]; e < overlay.rowStart[index + 1]; e++) {
          // Shortcuts through a live-adjusted subtree are stale; skip them
          // so the search descends to the adjusted raw edges instead.
          if (dirtyChildKey && live.dirtyKeys.has(dirtyChildKey)) {
            if (overlay.isClique[e]) continue;
            if (live.byCell.has(fetched.cells.get(leafOfNode(node)))) continue;
          }
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
      const entry = membershipOf(node);
      for (const cell of entry.cells || []) {
        const reverse = reverseCell(cell);
        if (node >= cell.firstNode && node < cell.firstNode + cell.nodeCount) {
          const local = node - cell.firstNode;
          for (let e = reverse.rowStart[local]; e < reverse.rowStart[local + 1]; e++) {
            const edge = reverse.edgeIds[e];
            const source = reverse.sources[e];
            const next = weight + penaltyFor(source, node, liveAdjustedWeight(live, cell, edge, cellEdgeWeight(cell, edge, factors)));
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
            const next = weight + penaltyFor(source, node, liveAdjustedWeight(live, cell, edge, cellEdgeWeight(cell, edge, factors)));
            if (next < (distB.get(source) ?? INF)) {
              distB.set(source, next);
              prevB.set(source, { prev: node, kind: "raw", leaf: cell.cellId, edge });
              heapB.push(next, source);
            }
          }
        }
      }
      for (const { overlay, level, cell } of entry.overlays || []) {
        const index = overlay.index.get(node);
        if (index == null) continue;
        const reverse = reverseOverlay(overlay);
        for (let e = reverse.rowStart[index]; e < reverse.rowStart[index + 1]; e++) {
          const edge = reverse.edgeIds[e];
          const source = overlay.nodes[reverse.sources[e]];
          if (live?.dirtyKeys.size) {
            const key = `${level - 1}:${cellAtLevel(leafOfNode(source), level - 1)}`;
            if (live.dirtyKeys.has(key)) {
              if (overlay.isClique[edge]) continue;
              if (live.byCell.has(fetched.cells.get(leafOfNode(source)))) continue;
            }
          }
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
      // Standard bidirectional stop: no undiscovered path can beat
      // topF + topB. When one direction is exhausted its distances are
      // final, so the other only needs to run until its own top passes
      // the best meeting.
      if (heapF.size && heapB.size) {
        if (topF + topB >= best) break;
      } else if (Math.min(topF, topB) >= best) {
        break;
      }
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

  /**
   * Whether a conditional window covers a moment.
   *
   * [at] is `{ month, weekday, minute }` — weekday from Monday as 0. Months
   * are inclusive and may wrap, because a school year runs September to June
   * and reading that as a plain low-to-high span makes it empty.
   */
  /**
   * When the trip starts, for answering conditional windows.
   *
   * Taken from the caller rather than the server's clock: the limit that
   * matters is the one on the sign the driver will be looking at, in their
   * timezone, at the hour they get there. `departAt` is an ISO local datetime
   * or a Date; absent, the answer is simply the posted limit throughout.
   */
  function departureMoment(params) {
    const raw = params?.departAt;
    if (!raw) return null;
    const when = raw instanceof Date ? raw : new Date(raw);
    if (Number.isNaN(when.getTime())) return null;
    return {
      month: when.getMonth() + 1,
      // getDay is 0=Sunday; the mask counts from Monday.
      weekday: (when.getDay() + 6) % 7,
      minute: when.getHours() * 60 + when.getMinutes()
    };
  }

  function conditionalApplies(rule, at) {
    const inMonths = rule.monthStart <= rule.monthEnd
      ? at.month >= rule.monthStart && at.month <= rule.monthEnd
      : at.month >= rule.monthStart || at.month <= rule.monthEnd;
    if (!inMonths) return false;
    if (((rule.days >> at.weekday) & 1) === 0) return false;
    return at.minute >= rule.startMinute && at.minute < rule.endMinute;
  }

  async function finishRoute(response, chain, sameEdgeUsed, bucket, factors, wantNames, departure = null) {
    const rawEdges = sameEdgeUsed ? [] : await unpackChain(chain.items, bucket, factors);
    // Geometry blocks for exactly the leaves the route passes through, in
    // one parallel wave alongside the names sidecar.
    const geometryLeaves = [...new Set(rawEdges.map(raw => raw.leaf))];
    const [names, ...geometryBlocks] = await Promise.all([
      wantNames ? loadNames() : null,
      ...geometryLeaves.map(leaf => loadCellGeometry(leaf))
    ]);
    const geometryByLeaf = new Map(geometryLeaves.map((leaf, i) => [leaf, geometryBlocks[i]]));

    /** The sign face an edge carries, or null. */
    const signTable = root.signs || [];
    const signOf = (cell, edge) => {
      const id = cell.signs ? cell.signs[edge] : 0;
      const entry = id ? signTable[id - 1] : null;
      if (!entry) return null;
      if (!entry.ref && !entry.exit && !entry.destRef && !entry.dest) return null;
      return entry;
    };

    // Cells the route already passes through, so an exit count can read a
    // roundabout node's other arms without another fetch. A roundabout that
    // straddles a leaf boundary simply goes uncounted, and the instruction
    // falls back to "at the roundabout, take the exit onto X" — which is
    // still the maneuver, just without the number.
    const cellsInPlay = [...new Map(rawEdges.map(raw => [raw.leaf, raw.cell])).values()];
    const cellForNode = (node) => cellsInPlay.find(
      cell => node >= cell.firstNode && node < cell.firstNode + cell.nodeCount
    );
    /** How many arms lead *out* of the circle at this node: 0 or 1. */
    const exitsAtNode = (node) => {
      const cell = cellForNode(node);
      if (!cell) return 0;
      const local = node - cell.firstNode;
      for (let e = cell.rowStart[local]; e < cell.rowStart[local + 1]; e++) {
        if (!cell.flags || (cell.flags[e] & EDGE_FLAG_ROUNDABOUT) === 0) return 1;
      }
      return 0;
    };
    let roundaboutExits = 0;

    const geometry = [];
    let distanceMeters = 0;
    let conditionalDelaySeconds = 0;
    const steps = [];
    const edges = [];
    // Junctions the route passes through (traffic signals, stop signs,
    // give-way, level crossings, pedestrian crossings) with positions —
    // navigation UIs draw these along the line.
    const junctions = [];
    const pushPoint = (latE7, lonE7) => {
      const last = geometry[geometry.length - 1];
      if (last && last[0] === latE7 / 1e7 && last[1] === lonE7 / 1e7) return;
      geometry.push([latE7 / 1e7, lonE7 / 1e7]);
    };
    if (chain.startMatch) pushPoint(chain.startMatch.snappedLatE7, chain.startMatch.snappedLonE7);
    // The edge that ran into the junction currently being crossed; a step's
    // lane guidance describes the approach, not the road being joined.
    let previousCell = null;
    let previousEdge = -1;
    for (const raw of rawEdges) {
      const cell = raw.cell;
      const edge = raw.edge;
      const points = edgePolyline(cell.geomRefs[edge], geometryByLeaf.get(raw.leaf));
      // Where this edge joins the previous one, captured before its own
      // points land: that junction is where a turn onto a new street
      // actually happens, and it is the only point whose bearings describe
      // the maneuver. Reading the index after the push pointed part-way down
      // the new street instead, where the road is straight by definition.
      const edgeStartIndex = Math.max(0, geometry.length - 1);
      const approachCell = previousCell;
      const approachEdge = previousEdge;
      previousCell = cell;
      previousEdge = edge;
      for (let i = 0; i < points.length; i += 2) pushPoint(points[i], points[i + 1]);
      if (cell.junctions[edge]) {
        // Packed as kind + polylinePointIndex * 8 (signals often sit on
        // interior way nodes, not the junction node itself).
        const packed = cell.junctions[edge];
        const kind = packed % 8;
        const pointCount = points.length / 2;
        const pointIndex = Math.min((packed - kind) / 8, pointCount - 1);
        const along = pointCount > 1 ? pointIndex / (pointCount - 1) : 1;
        // A wide intersection carries a signal node on each approach, and a
        // crossing on each side of it — separate nodes in OSM, metres apart,
        // all describing the one intersection a driver is about to cross.
        // Reported as they are, the same junction is drawn two or three
        // times over. Consecutive markers of the same kind that are within a
        // few metres of each other are that one thing.
        const previous = junctions[junctions.length - 1];
        const lat = points[pointIndex * 2] / 1e7;
        const lon = points[pointIndex * 2 + 1] / 1e7;
        const duplicate = mergesWithPreviousJunction(
          previous,
          kind,
          points[pointIndex * 2],
          points[pointIndex * 2 + 1]
        );
        if (!duplicate) {
          junctions.push({
            kind,
            lat,
            lon,
            atMeters: Math.round((distanceMeters + (cell.distsDm[edge] / 10) * along) * 10) / 10
          });
        }
      }
      const meters = cell.distsDm[edge] / 10;
      let seconds = cellEdgeWeight(cell, edge, factors) / 10;
      distanceMeters += meters;
      // Posted limit in km/h, 0 when the way carried no maxspeed tag. This is
      // the sign, not the modelled speed: `seconds` also absorbs surface
      // degradation and junction penalties.
      const speedLimitKmh = cell.speeds ? cell.speeds[edge] : 0;
      // A window this edge's limit drops inside, if it has one. Carried out
      // rather than resolved here: the index is static and the answer depends
      // on the clock, so only the caller can say which limit is in force.
      const condRule = cell.condRules ? cell.condRules[edge] : 0;
      // A school zone in force costs the time it actually costs. The search
      // itself is left alone: 757 edges in 4.7 million cannot change which
      // way is quickest, but they can make the arrival time wrong by the
      // seconds a driver spends at 30 instead of 50, and an ETA is a promise.
      if (condRule && departure) {
        const rule = (root.condRules || [])[condRule - 1];
        if (rule && conditionalApplies(rule, departure) && speedLimitKmh > rule.speedKmh) {
          const slowed = seconds * (speedLimitKmh / rule.speedKmh);
          conditionalDelaySeconds += slowed - seconds;
          seconds = slowed;
        }
      }
      edges.push({
        leaf: raw.leaf,
        edge,
        segment: edgeSegmentId(cell, edge),
        seconds,
        meters,
        speedLimitKmh,
        condRule
      });
      const name = names ? names[cell.nameIds[edge]] || "" : "";
      const roundabout = cell.flags ? (cell.flags[edge] & EDGE_FLAG_ROUNDABOUT) !== 0 : false;
      const sign = signOf(cell, edge);
      // A roundabout is one instruction however many arcs it is made of, and
      // it is never the road before or after it. Two ways round that: arcs
      // always join each other, and never join anything else.
      const last = steps[steps.length - 1];
      const continues = last && (
        roundabout ? last.roundabout : (!last.roundabout && last.name === name)
      );
      if (roundabout) {
        // Every arc ends at an arm of the circle. The ones that lead out are
        // the exits a driver counts, so counting them as they go past is what
        // turns "at the roundabout" into "take the second exit".
        roundaboutExits += exitsAtNode(cell.targets[edge]);
      }
      if (continues) {
        last.meters += meters;
        last.seconds += seconds;
        if (speedLimitKmh) last.limitMeters.set(speedLimitKmh, (last.limitMeters.get(speedLimitKmh) || 0) + meters);
        // A run of edges is signed by the first of them that says anything,
        // and an exit number outranks a bare route number: it is the thing
        // written largest on the panel and the only one that is unambiguous.
        if (sign && (!last.sign || (sign.exit && !last.sign.exit))) last.sign = sign;
        if (roundabout) last.roundaboutExit = roundaboutExits;
        if (!last.name && name) last.name = name;
      } else {
        // `at` indexes the route geometry point where this step begins, so
        // clients can slice per-street geometry (e.g. road-name labels) and
        // read the turn angle onto it.
        //
        // Lane guidance belongs to the road being left, not the one being
        // joined: "get in the left two lanes" is an instruction about the
        // approach. So a step carries the lanes of the edge that ran into its
        // junction, which is the last edge of the step before it.
        steps.push({
          name,
          meters,
          seconds,
          at: edgeStartIndex,
          // The kind of road this step runs on, as an index into the root's
          // class table. What a route is made of is otherwise invisible: a
          // cycling profile that claims to prefer cycleways can only be
          // checked against the classes it actually chose.
          roadClass: cell.classes ? cell.classes[edge] : 0,
          lanes: laneListOf(approachCell, approachEdge),
          // What a driver would read on a sign here, rather than what the
          // road is called in the database. Null on the ordinary street that
          // carries no numbers, which is most of them.
          sign,
          roundabout,
          roundaboutExit: roundabout ? roundaboutExits : 0,
          limitMeters: new Map(speedLimitKmh ? [[speedLimitKmh, meters]] : [])
        });
      }
      if (!roundabout) roundaboutExits = 0;
    }
    if (chain.endMatch) pushPoint(chain.endMatch.snappedLatE7, chain.endMatch.snappedLonE7);
    // Where the posted limit changes along the route, as a step function over
    // distance travelled.
    //
    // A step is a stretch of road with one name, and a road with one name
    // routinely carries several limits: an autoroute drops 100 to 70 through
    // an interchange and climbs back, all of it "Autoroute 640". Reporting a
    // single limit per step means reporting the one that covers most of it,
    // so the sign reads 100 while the driver is doing 70 past a camera. The
    // edges already know better; this is only a matter of saying so.
    const speedLimits = [];
    let limitAt = 0;
    const condRuleTable = root.condRules || [];
    for (const edge of edges) {
      const last = speedLimits[speedLimits.length - 1];
      if (!last || last.limitKmh !== edge.speedLimitKmh || last.condRule !== edge.condRule) {
        const rule = edge.condRule ? condRuleTable[edge.condRule - 1] : null;
        speedLimits.push({
          atMeters: limitAt,
          limitKmh: edge.speedLimitKmh,
          condRule: edge.condRule,
          // The window spelled out, so a client never needs the root's table.
          conditional: rule
            ? {
                limitKmh: rule.speedKmh,
                days: rule.days,
                startMinute: rule.startMinute,
                endMinute: rule.endMinute,
                monthStart: rule.monthStart,
                monthEnd: rule.monthEnd
              }
            : null
        });
      }
      limitAt += edge.meters;
    }

    // A step can span several posted limits; report the one covering the most
    // of it, which is what the driver is under for most of the street. Kept
    // for the itinerary list, where one number per street is the right answer
    // and there is no position to be more precise about.
    for (const step of steps) {
      let best = 0;
      let bestMeters = 0;
      for (const [limit, covered] of step.limitMeters || []) {
        if (covered > bestMeters) {
          best = limit;
          bestMeters = covered;
        }
      }
      step.speedLimitKmh = best;
      delete step.limitMeters;
      // Flattened, because every consumer wants the four strings and none of
      // them wants to know a sign table exists. Empty rather than absent so
      // a client never has to distinguish "no sign" from "not this version".
      const sign = step.sign;
      step.ref = sign?.ref || "";
      step.exitRef = sign?.exit || "";
      step.destinationRef = sign?.destRef || "";
      step.destination = sign?.dest || "";
      delete step.sign;
    }
    response.distanceMeters = distanceMeters;
    response.geometry = geometry;
    response.steps = steps;
    response.speedLimits = speedLimits;
    response.edges = edges;
    response.junctions = junctions;
    // Recompute the exact unpenalized total from seeds plus raw edges; for
    // the primary route this equals the search result, for penalized
    // alternatives it replaces the inflated search weight.
    if (!sameEdgeUsed && chain.startMatch && chain.endMatch) {
      const seedF = Math.round(bucketWeight(chain.startMatch.weight, chain.startMatch.classCode, factors) * (1 - chain.startMatch.ratio));
      const seedB = Math.round(bucketWeight(chain.endMatch.weight, chain.endMatch.classCode, factors) * chain.endMatch.ratio);
      const edgeDs = rawEdges.reduce((sum, raw) => sum + cellEdgeWeight(raw.cell, raw.edge, factors), 0);
      response.seconds = (seedF + edgeDs + seedB) / 10;
    }
    // Last, because the recompute above replaces the total outright. Time
    // spent at a school-zone limit is added on top rather than searched with:
    // 757 edges in 4.7 million cannot change which way is quickest, but they
    // can certainly make an arrival time wrong.
    if (conditionalDelaySeconds > 0) {
      response.seconds += conditionalDelaySeconds;
      response.conditionalDelaySeconds = conditionalDelaySeconds;
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
      const factor = liveWeights.factors?.[`${edge.leaf}/${edge.edge}`]
        ?? liveWeights.factors?.[edge.segment];
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
    // Speculative single-wave fetch: the candidate snap leaves are known
    // from root bboxes before any fetch, so the context objects (leaves,
    // overlays, top) launch in the same wave as the snap cells. Extra
    // speculative leaves share the snap fetches and almost always the same
    // parent overlays, so the byte overhead is marginal; a top-up wave only
    // happens when a cross-cell snap seed escapes the plan.
    let speculative = null;
    {
      const fromLat = Number(params.from?.lat);
      const fromLon = Number(params.from?.lon ?? params.from?.lng);
      const toLat = Number(params.to?.lat);
      const toLon = Number(params.to?.lon ?? params.to?.lng);
      if (Number.isFinite(fromLat) && Number.isFinite(fromLon) && Number.isFinite(toLat) && Number.isFinite(toLon)) {
        const leaves = new Set([
          ...candidateLeavesFor(Math.round(fromLat * 1e7), Math.round(fromLon * 1e7)),
          ...candidateLeavesFor(Math.round(toLat * 1e7), Math.round(toLon * 1e7))
        ]);
        const contexts = buildContexts([...leaves]);
        const promise = fetchContexts(contexts, bucket);
        promise.catch(() => {});
        speculative = { leaves, contexts, promise };
      }
    }
    // A fix good to twenty-five metres cannot be used to pick between two
    // roads twenty metres apart, and a candidate band narrower than the
    // error simply throws away the road the car is actually on before the
    // search ever sees it — which is how a driver on a service road beside a
    // motorway gets routed onto the motorway. Widen the band to the reported
    // accuracy and let the costs above decide; they now charge for distance.
    const accuracyMeters = Math.max(0, Number(params.accuracyMeters) || 0);
    const fromOptions = accuracyMeters > SNAP_EXTRA_METERS
      ? { extraMeters: Math.min(accuracyMeters, SNAP_EXTRA_METERS_MAX) }
      : undefined;
    const [snapFrom, snapTo] = await Promise.all([snap(params.from, fromOptions), snap(params.to)]);
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
    let contexts;
    let fetched;
    if (speculative && [...snapLeaves].every(leaf => speculative.leaves.has(leaf))) {
      contexts = speculative.contexts;
      fetched = await speculative.promise;
    } else {
      const union = speculative ? new Set([...speculative.leaves, ...snapLeaves]) : snapLeaves;
      contexts = buildContexts([...union]);
      fetched = await fetchContexts(contexts, bucket);
    }
    let live = await buildLiveAdjustments(params.live, fetched, factors);
    if (live && live.referencedLeaves.size) {
      // Leaves carrying live states join the context set exactly like snap
      // leaves: their ancestors' overlays let the search descend into them
      // from anywhere on the route, capped to keep queries bounded.
      const extra = [...live.referencedLeaves].filter(leaf => !fetched.cells.has(leaf)).slice(0, 24);
      if (extra.length) {
        contexts = buildContexts([...new Set([...fetched.cells.keys(), ...extra])]);
        fetched = await fetchContexts(contexts, bucket);
        applyLiveToCells(live, [...fetched.cells.values()], factors);
      }
    }

    const effMatchWeight = (match) => {
      const base = bucketWeight(match.weight, match.classCode, factors);
      const cell = fetched.cells.get(match.leaf);
      return cell ? liveAdjustedWeight(live, cell, match.edgeIndex, base) : base;
    };
    // How far off the line a candidate is, priced.
    //
    // The seed cost of a snap match was the cost of the rest of its own edge
    // and nothing else, so which road a reroute started from was decided by
    // how *fast* the road was, not by how close it was: an autoroute twenty
    // metres away out-seeded the boulevard the car was driving on, because
    // twenty metres of autoroute costs less than twenty metres of boulevard.
    // A driver on Boulevard Notre-Dame beside the A-13 was handed a route
    // down the desserte, drove straight on as anyone would, and was declared
    // off-route eleven times in four minutes.
    //
    // Charged on the *excess* over the nearest candidate rather than on the
    // absolute distance: a trip that genuinely starts eighty metres from any
    // road — a car park, a forecourt — must not have every one of its
    // candidates taxed equally for the same eighty metres.
    const snapPenaltyDsPerMeter = Math.max(0, Math.round((params.snapPenaltySecondsPerMeter ?? 1) * 10));
    const snapPenalty = (match) =>
      Math.round((match.extraMeters || 0) * snapPenaltyDsPerMeter);

    // A reroute happens while the driver is moving, and the snap offers both
    // directions of the road they are on — the two are metres apart, so both
    // are near-equally good matches. Seeding the opposing edge at its bare
    // cost lets the search turn the vehicle around for free and hand back a
    // route that starts by driving back the way they just came. Charging the
    // misaligned seeds makes going back cost what a U-turn costs, so it wins
    // only when it genuinely saves more than that.
    const headingPenaltyDs = Math.max(0, Math.round((params.headingPenaltySeconds ?? 60) * 10));
    const fromHeading = Number(params.fromHeading);
    const headingPenalty = (match) => {
      if (headingPenaltyDs === 0) return 0;
      if (!Number.isFinite(fromHeading) || !Number.isFinite(match.bearingDeg)) return 0;
      // 0° means travelling the same way as the edge, 180° straight against it.
      const off = Math.abs(((match.bearingDeg - fromHeading + 540) % 360) - 180);
      if (off <= HEADING_ALIGNED_DEG) return 0;
      if (off >= HEADING_OPPOSED_DEG) return headingPenaltyDs;
      return Math.round(
        headingPenaltyDs * (off - HEADING_ALIGNED_DEG) / (HEADING_OPPOSED_DEG - HEADING_ALIGNED_DEG)
      );
    };

    const forwardSeeds = snapFrom.matches.map(match => ({
      node: match.toNode,
      weight: Math.round(effMatchWeight(match) * (1 - match.ratio)) + headingPenalty(match) + snapPenalty(match),
      match
    })).filter(seed => Number.isFinite(seed.weight));
    const backwardSeeds = snapTo.matches.map(match => ({
      node: match.fromNode,
      weight: Math.round(effMatchWeight(match) * match.ratio) + snapPenalty(match),
      match
    })).filter(seed => Number.isFinite(seed.weight));

    // Same-edge special case: both points on one directed edge, in order.
    let sameEdge = null;
    for (const from of snapFrom.matches) {
      for (const to of snapTo.matches) {
        if (from.leaf === to.leaf && from.edgeIndex === to.edgeIndex && to.ratio >= from.ratio) {
          const weight = Math.round(effMatchWeight(from) * (to.ratio - from.ratio)) +
            headingPenalty(from) + snapPenalty(from) + snapPenalty(to);
          if (Number.isFinite(weight) && (!sameEdge || weight < sameEdge.weight)) sameEdge = { weight, from, to };
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
      // Instant coarse polyline before any unpack fetch: snapped endpoints
      // joined through the bbox centers of the leaf cells the path
      // traverses. Render this immediately, then replace it with the exact
      // geometry that follows.
      if (!sameEdgeUsed) {
        const coarse = [];
        const pushCoarse = (lat, lon) => {
          const last = coarse[coarse.length - 1];
          if (last && last[0] === lat && last[1] === lon) return;
          coarse.push([lat, lon]);
        };
        pushCoarse(snapFrom.matches[0].snappedLatE7 / 1e7, snapFrom.matches[0].snappedLonE7 / 1e7);
        let previousLeaf = -1;
        for (const item of chain.items) {
          const leaf = leafOfNode(item.toNode);
          if (leaf === previousLeaf) continue;
          previousLeaf = leaf;
          const bbox = root.leaves[leaf].bbox;
          pushCoarse(((bbox.minLat + bbox.maxLat) / 2) / 1e7, ((bbox.minLon + bbox.maxLon) / 2) / 1e7);
        }
        pushCoarse(snapTo.matches[0].snappedLatE7 / 1e7, snapTo.matches[0].snappedLonE7 / 1e7);
        response.coarseGeometry = coarse;
        if (params.onCoarseRoute) {
          try {
            params.onCoarseRoute({ seconds: response.seconds, geometry: coarse, bucket: response.bucket });
          } catch {
            // Listener errors must not break routing.
          }
        }
      }
      await finishRoute(
        response, chain, sameEdgeUsed, bucket, factors, params.names !== false,
        departureMoment(params)
      );
      response.stats = statsSnapshot();
      return response;
    };

    const primarySearch = searchQueryGraph(contexts, fetched, forwardSeeds, backwardSeeds, factors, null, live);
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
    if (live) {
      primary.live = { provider: live.name, states: live.states, applied: live.applied, error: live.error };
      // The search itself ran on live-adjusted corridor weights, so the
      // optimal total under the live metric is the search result.
      primary.adjustedSeconds = totalWeight / 10;
    }

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
      const alternativeSearch = searchQueryGraph(contexts, fetched, forwardSeeds, backwardSeeds, factors, penalized, live);
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

  // Forward-only multi-target Dijkstra over the union query graph of all
  // stops. Exact for every pair by the same argument as the bidirectional
  // search: the union contains each pair's context, and every extra edge is
  // a real path length.
  function forwardToTargets(contexts, fetched, seeds, targetSeeds, factors, live) {
    const INF = Infinity;
    const cellList = [...fetched.cells.values()];
    const overlayList = [...fetched.overlays.values()];
    const membership = new Map();
    const membershipOf = (node) => {
      let entry = membership.get(node);
      if (entry) return entry;
      entry = { cells: null, overlays: null };
      for (const cell of cellList) {
        if (node >= cell.firstNode && node < cell.firstNode + cell.nodeCount) (entry.cells ??= []).push(cell);
      }
      for (const overlay of overlayList) {
        if (overlay.index.has(node)) (entry.overlays ??= []).push(overlay);
      }
      membership.set(node, entry);
      return entry;
    };
    const dist = new Map();
    const heap = new MinHeap();
    for (const seed of seeds) {
      if (seed.weight < (dist.get(seed.node) ?? INF)) {
        dist.set(seed.node, seed.weight);
        heap.push(seed.weight, seed.node);
      }
    }
    // best[j] improves as target seed nodes settle; stop once the heap top
    // can no longer improve any unfinished target.
    const best = targetSeeds.map(() => INF);
    const settled = new Set();
    while (heap.size) {
      let unfinished = false;
      for (let j = 0; j < best.length; j++) {
        if (heap.peekWeight() < best[j]) {
          unfinished = true;
          break;
        }
      }
      if (!unfinished) break;
      const weight = heap.peekWeight();
      const node = heap.pop();
      if (weight !== dist.get(node) || settled.has(node)) continue;
      settled.add(node);
      for (let j = 0; j < targetSeeds.length; j++) {
        for (const seed of targetSeeds[j]) {
          if (seed.node === node && weight + seed.weight < best[j]) best[j] = weight + seed.weight;
        }
      }
      const entry = membershipOf(node);
      for (const cell of entry.cells || []) {
        const local = node - cell.firstNode;
        for (let e = cell.rowStart[local]; e < cell.rowStart[local + 1]; e++) {
          const next = weight + liveAdjustedWeight(live, cell, e, cellEdgeWeight(cell, e, factors));
          const target = cell.targets[e];
          if (next < (dist.get(target) ?? INF)) {
            dist.set(target, next);
            heap.push(next, target);
          }
        }
      }
      for (const overlay of entry.overlays || []) {
        const index = overlay.index.get(node);
        for (let e = overlay.rowStart[index]; e < overlay.rowStart[index + 1]; e++) {
          const next = weight + overlay.weights[e];
          const target = overlay.nodes[overlay.targetIndex[e]];
          if (next < (dist.get(target) ?? INF)) {
            dist.set(target, next);
            heap.push(next, target);
          }
        }
      }
    }
    return best;
  }

  async function matrix(params) {
    const points = params.points;
    const size = points.length;
    const seconds = Array.from({ length: size }, () => new Array(size).fill(0));
    const bucket = bucketForParams(params);
    const factors = root.buckets[bucket].factors;
    if (params.pairwise === true || size <= 2) {
      for (let i = 0; i < size; i++) {
        for (let j = 0; j < size; j++) {
          if (i === j) continue;
          const result = await route({
            from: points[i],
            to: points[j],
            geometry: false,
            bucket: params.bucket,
            departureTime: params.departureTime,
            live: params.live
          });
          seconds[i][j] = result.seconds;
        }
      }
      return { seconds, stats: statsSnapshot() };
    }
    // One shared context fetch for every stop, then one forward multi-target
    // search per stop.
    const snaps = await Promise.all(points.map(point => snap(point)));
    const allLeaves = new Set();
    for (const snapped of snaps) {
      for (const match of snapped.matches) {
        allLeaves.add(match.leaf);
        allLeaves.add(leafOfNode(match.toNode));
        allLeaves.add(leafOfNode(match.fromNode));
      }
    }
    const contexts = buildContexts([...allLeaves]);
    const fetched = await fetchContexts(contexts, bucket);
    const live = await buildLiveAdjustments(params.live, fetched, factors);
    const effMatchWeight = (match) => {
      const base = bucketWeight(match.weight, match.classCode, factors);
      const cell = fetched.cells.get(match.leaf);
      return cell ? liveAdjustedWeight(live, cell, match.edgeIndex, base) : base;
    };
    const forwardSeedsOf = (snapped) => snapped.matches.map(match => ({
      node: match.toNode,
      weight: Math.round(effMatchWeight(match) * (1 - match.ratio))
    }));
    const backwardSeedsOf = (snapped) => snapped.matches.map(match => ({
      node: match.fromNode,
      weight: Math.round(effMatchWeight(match) * match.ratio)
    }));
    const targetSeeds = snaps.map(backwardSeedsOf);
    for (let i = 0; i < size; i++) {
      const best = forwardToTargets(contexts, fetched, forwardSeedsOf(snaps[i]), targetSeeds, factors, live);
      for (let j = 0; j < size; j++) {
        if (i === j) continue;
        let weight = best[j];
        // Same-edge shortcut per pair.
        for (const from of snaps[i].matches) {
          for (const to of snaps[j].matches) {
            if (from.leaf === to.leaf && from.edgeIndex === to.edgeIndex && to.ratio >= from.ratio) {
              const direct = Math.round(effMatchWeight(from) * (to.ratio - from.ratio));
              if (direct < weight) weight = direct;
            }
          }
        }
        if (weight === Infinity) {
          throw routeError("RANGEFIND_ROUTE_NO_PATH", `No route between stops ${i} and ${j}.`);
        }
        seconds[i][j] = weight / 10;
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
    const { seconds } = await matrix({ points: stops, bucket: params.bucket, departureTime: params.departureTime, live: params.live });
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
        departureTime: params.departureTime,
        live: params.live
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
      httpRequests: stats.httpRequests,
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

  return { root, route, matrix, itinerary, snap, locate, stats: statsSnapshot, resetStats, clearCaches };
}
