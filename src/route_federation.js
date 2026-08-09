// Client-only routing across independently published rfroutegraph-v1 regions.
// Regional graphs remain immutable and range-addressed. A tiny mutable catalog
// describes candidate neighbors, while per-region portal sidecars prove that
// both extracts contain the same original OSM junction before a handoff is
// considered. No routing server or whole-continent graph is required.

import { openRouteGraphUrl } from "./route_graph_query.js";

const PORTAL_FORMAT = "rfrouteportals-v1";
const CATALOG_FORMAT = "rangefind-route-catalog-v1";
const EARTH_METERS = 6371008.8;

function routeError(code, message) {
  const error = new Error(message);
  error.code = code;
  return error;
}

function coordinates(point) {
  const lat = Number(point?.lat);
  const lon = Number(point?.lon ?? point?.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
    throw routeError("RANGEFIND_ROUTE_BAD_POINT", `Route points need finite lat/lon; got ${JSON.stringify(point)}.`);
  }
  return { lat, lon };
}

function contains(bbox, point) {
  if (!bbox || point.lat < bbox[0] || point.lat > bbox[2]) return false;
  return bbox[1] <= bbox[3]
    ? point.lon >= bbox[1] && point.lon <= bbox[3]
    : point.lon >= bbox[1] || point.lon <= bbox[3];
}

function bboxArea(bbox) {
  if (!bbox) return Infinity;
  const width = bbox[1] <= bbox[3] ? bbox[3] - bbox[1] : 360 - bbox[1] + bbox[3];
  return Math.max(0, bbox[2] - bbox[0]) * Math.max(0, width);
}

function haversine(a, b) {
  const rad = Math.PI / 180;
  const dLat = (b.lat - a.lat) * rad;
  const dLon = (b.lon - a.lon) * rad;
  const x = Math.sin(dLat / 2) ** 2
    + Math.cos(a.lat * rad) * Math.cos(b.lat * rad) * Math.sin(dLon / 2) ** 2;
  return 2 * EARTH_METERS * Math.asin(Math.min(1, Math.sqrt(x)));
}

function portalTriples(values) {
  const portals = [];
  for (let i = 0; i + 2 < (values?.length || 0); i += 3) {
    portals.push({ id: Number(values[i]), lat: Number(values[i + 1]) / 1e7, lon: Number(values[i + 2]) / 1e7 });
  }
  return portals;
}

function intersectPortals(left, right) {
  const a = portalTriples(left);
  const b = portalTriples(right);
  const small = a.length <= b.length ? a : b;
  const large = a.length <= b.length ? b : a;
  const byId = new Map(small.map(portal => [portal.id, portal]));
  const shared = [];
  for (const portal of large) {
    const peer = byId.get(portal.id);
    if (!peer) continue;
    // Shared ids should have byte-identical coordinates. Keep a small guard
    // for sources independently rounded to E7, but reject corrupted matches.
    if (haversine(peer, portal) > 3) continue;
    shared.push({ id: portal.id, lat: (portal.lat + peer.lat) / 2, lon: (portal.lon + peer.lon) / 2 });
  }
  return shared;
}

function selectPortals(portals, from, to, limit) {
  const ranked = portals.slice().sort((a, b) => (
    haversine(from, a) + haversine(a, to) - haversine(from, to)
    - (haversine(from, b) + haversine(b, to) - haversine(from, to))
  ));
  if (ranked.length <= limit) return ranked;
  const selected = [];
  // Spatial diversity prevents six ramps of one interchange from crowding
  // out a different border crossing that wins under the road metric.
  for (const portal of ranked) {
    if (!selected.length || selected.every(other => haversine(portal, other) >= 1500)) selected.push(portal);
    if (selected.length === limit) break;
  }
  for (const portal of ranked) {
    if (selected.length === limit) break;
    if (!selected.some(other => other.id === portal.id)) selected.push(portal);
  }
  return selected;
}

async function connectedRegionPaths(start, target, indexes, maxPaths, maxHops, maxExpansions, connects) {
  if (start === target) return [[start]];
  const paths = [];
  const queue = [[start]];
  let expansions = 0;
  while (queue.length && paths.length < maxPaths && expansions++ < maxExpansions) {
    const path = queue.shift();
    if (path.length - 1 > maxHops) continue;
    const current = indexes.get(path[path.length - 1]);
    const neighbors = (current?.neighbors || []).filter(neighbor => indexes.has(neighbor) && !path.includes(neighbor));
    const connected = await Promise.all(neighbors.map(neighbor => connects(path[path.length - 1], neighbor)));
    for (let neighborIndex = 0; neighborIndex < neighbors.length; neighborIndex++) {
      if (!connected[neighborIndex]) continue;
      const neighbor = neighbors[neighborIndex];
      const next = [...path, neighbor];
      if (neighbor === target) {
        paths.push(next);
      } else if (next.length - 1 < maxHops) {
        queue.push(next);
      }
    }
  }
  return paths;
}

async function mapLimit(items, concurrency, callback) {
  const output = new Array(items.length);
  let cursor = 0;
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    for (;;) {
      const index = cursor++;
      if (index >= items.length) return;
      output[index] = await callback(items[index], index);
    }
  }));
  return output;
}

function mergeLegs(legs, regionPath, transitions) {
  const geometry = [];
  const steps = [];
  const edges = [];
  const junctions = [];
  const shards = new Set();
  let seconds = 0;
  let distanceMeters = 0;
  let settledNodes = 0;
  const stats = { objectFetches: 0, bytesFetched: 0, cellFetches: 0, overlayFetches: 0, unpackCellFetches: 0, httpRequests: 0 };
  for (let index = 0; index < legs.length; index++) {
    const leg = legs[index];
    const region = regionPath[index];
    seconds += leg.seconds;
    distanceMeters += leg.distanceMeters || 0;
    settledNodes += leg.settledNodes || 0;
    const geometryOffset = geometry.length;
    for (let point = 0; point < (leg.geometry?.length || 0); point++) {
      if (geometry.length && point === 0) continue;
      geometry.push(leg.geometry[point]);
    }
    for (const step of leg.steps || []) steps.push({ ...step, at: Math.max(0, geometryOffset - (geometryOffset ? 1 : 0)) + (step.at || 0), region });
    for (const edge of leg.edges || []) edges.push({ ...edge, region, segment: `${region}:${edge.segment}` });
    for (const junction of leg.junctions || []) junctions.push({ ...junction, atMeters: junction.atMeters + distanceMeters - (leg.distanceMeters || 0), region });
    for (const key of Object.keys(stats)) stats[key] += Number(leg.stats?.[key] || 0);
    for (const shard of leg.stats?.shardsTouched || []) shards.add(`${region}:${shard}`);
  }
  return {
    federated: true,
    regions: regionPath,
    transitions,
    seconds,
    bucket: legs[0]?.bucket || "base",
    settledNodes,
    from: legs[0]?.from,
    to: legs[legs.length - 1]?.to,
    distanceMeters,
    geometry,
    steps,
    edges,
    junctions,
    stats: { ...stats, shardsTouched: [...shards], regionsTouched: regionPath.slice() }
  };
}

/**
 * Opens routes/catalog.json and returns a route engine that transparently
 * delegates same-region queries or stitches exact shared-OSM-id portals for
 * cross-region queries. All regional objects are opened lazily.
 */
export async function openRouteCatalogUrl(catalogUrl, options = {}) {
  const fetcher = options.fetch || globalThis.fetch;
  if (typeof fetcher !== "function") throw new Error("openRouteCatalogUrl requires fetch().");
  const response = await fetcher(catalogUrl, { cache: "no-store" });
  if (!response.ok) throw new Error(`Route catalog HTTP ${response.status}: ${catalogUrl}`);
  const catalog = await response.json();
  if (catalog.format !== CATALOG_FORMAT) throw new Error(`Unsupported route catalog format: ${catalog.format}.`);
  const profile = options.profile || "car";
  const catalogBase = new URL(".", catalogUrl).href;
  const indexes = new Map(catalog.indexes.filter(index => index.profile === profile).map(index => [index.region, index]));
  if (!indexes.size) throw routeError("RANGEFIND_ROUTE_NO_PROFILE", `Route catalog has no ${profile} indexes.`);
  const graphCache = new Map();
  const portalCache = new Map();
  const inflate = options.inflate || (async bytes => {
    if (typeof DecompressionStream === "undefined") throw new Error("Gzip route portals require DecompressionStream or options.inflate.");
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
    return new Uint8Array(await new Response(stream).arrayBuffer());
  });
  const openGraph = options.openGraph || ((index, base) => openRouteGraphUrl(base, options.graphOptions || {}));
  const graphFor = region => {
    let promise = graphCache.get(region);
    if (!promise) {
      const index = indexes.get(region);
      const base = new URL(index.base, catalogBase).href;
      promise = Promise.resolve(openGraph(index, base));
      graphCache.set(region, promise);
      promise.catch(() => graphCache.delete(region));
    }
    return promise;
  };
  const portalsFor = region => {
    let promise = portalCache.get(region);
    if (!promise) {
      const index = indexes.get(region);
      const file = index.portals || index.manifest?.portals;
      if (!file) return Promise.resolve({ format: PORTAL_FORMAT, neighbors: {} });
      const url = new URL(file, new URL(index.base, catalogBase)).href;
      promise = fetcher(url, { cache: "force-cache" }).then(async result => {
        if (!result.ok) throw new Error(`Route portals HTTP ${result.status}: ${url}`);
        const sidecar = url.endsWith(".gz")
          ? JSON.parse(new TextDecoder().decode(await inflate(new Uint8Array(await result.arrayBuffer()))))
          : await result.json();
        if (sidecar.format !== PORTAL_FORMAT) throw new Error(`Unsupported route portal format: ${sidecar.format}.`);
        return sidecar;
      });
      portalCache.set(region, promise);
      promise.catch(() => portalCache.delete(region));
    }
    return promise;
  };
  const candidateRegions = point => {
    const candidates = [...indexes.values()].filter(index => contains(index.bbox, point));
    if (!candidates.length) throw routeError("RANGEFIND_ROUTE_NO_REGION", `No ${profile} route region covers (${point.lat}, ${point.lon}).`);
    candidates.sort((a, b) => bboxArea(a.bbox) - bboxArea(b.bbox));
    return candidates.map(index => index.region);
  };
  const regionFor = point => candidateRegions(point)[0];
  const resolveRegion = async (point, explicit) => {
    if (explicit) return explicit;
    const candidates = candidateRegions(point);
    if (candidates.length === 1) return candidates[0];
    const measured = await Promise.all(candidates.slice(0, 4).map(async region => {
      try {
        const graph = await graphFor(region);
        if (typeof graph.snap !== "function") return { region, meters: Infinity };
        const snapped = await graph.snap(point, { maxCandidates: 2, maxSnapMeters: 0 });
        return { region, meters: snapped.matches?.[0]?.distMeters ?? Infinity };
      } catch {
        return { region, meters: Infinity };
      }
    }));
    measured.sort((a, b) => a.meters - b.meters);
    return Number.isFinite(measured[0].meters) ? measured[0].region : candidates[0];
  };
  const sharedPortals = async (left, right, from, to) => {
    const [a, b] = await Promise.all([portalsFor(left), portalsFor(right)]);
    const shared = intersectPortals(a.neighbors?.[right], b.neighbors?.[left]);
    return selectPortals(shared, from, to, Math.max(1, Math.floor(options.maxPortalsPerBorder ?? 8)));
  };
  const safeRoute = async (region, from, to, params, geometry) => {
    try {
      const engine = await graphFor(region);
      return await engine.route({ ...params, from, to, geometry });
    } catch (error) {
      if (error?.code === "RANGEFIND_ROUTE_SNAP_TOO_FAR" || error?.code === "RANGEFIND_ROUTE_NO_PATH") return null;
      throw error;
    }
  };

  async function evaluatePath(regionPath, from, to, params) {
    const boundaries = [];
    for (let index = 0; index + 1 < regionPath.length; index++) {
      const portals = await sharedPortals(regionPath[index], regionPath[index + 1], from, to);
      if (!portals.length) return null;
      boundaries.push(portals);
    }
    let costs = await mapLimit(boundaries[0], options.portalConcurrency || 8, async portal => (
      (await safeRoute(regionPath[0], from, portal, params, false))?.seconds ?? Infinity
    ));
    const parents = [];
    for (let regionIndex = 1; regionIndex + 1 < regionPath.length; regionIndex++) {
      const incoming = boundaries[regionIndex - 1];
      const outgoing = boundaries[regionIndex];
      const pairs = incoming.flatMap((entry, i) => outgoing.map((exit, j) => ({ entry, exit, i, j })));
      const legs = await mapLimit(pairs, options.portalConcurrency || 8, async pair => (
        (await safeRoute(regionPath[regionIndex], pair.entry, pair.exit, params, false))?.seconds ?? Infinity
      ));
      const nextCosts = new Array(outgoing.length).fill(Infinity);
      const parent = new Int32Array(outgoing.length).fill(-1);
      for (let pairIndex = 0; pairIndex < pairs.length; pairIndex++) {
        const pair = pairs[pairIndex];
        const cost = costs[pair.i] + legs[pairIndex];
        if (cost < nextCosts[pair.j]) {
          nextCosts[pair.j] = cost;
          parent[pair.j] = pair.i;
        }
      }
      parents.push(parent);
      costs = nextCosts;
    }
    const lastBoundary = boundaries[boundaries.length - 1];
    const tails = await mapLimit(lastBoundary, options.portalConcurrency || 8, async portal => (
      (await safeRoute(regionPath[regionPath.length - 1], portal, to, params, false))?.seconds ?? Infinity
    ));
    let best = Infinity;
    let cursor = -1;
    for (let index = 0; index < tails.length; index++) {
      if (costs[index] + tails[index] < best) {
        best = costs[index] + tails[index];
        cursor = index;
      }
    }
    if (!Number.isFinite(best)) return null;
    const transitions = new Array(boundaries.length);
    transitions[transitions.length - 1] = lastBoundary[cursor];
    for (let parentIndex = parents.length - 1; parentIndex >= 0; parentIndex--) {
      cursor = parents[parentIndex][cursor];
      transitions[parentIndex] = boundaries[parentIndex][cursor];
    }
    return { seconds: best, transitions };
  }

  async function route(params) {
    const from = coordinates(params.from);
    const to = coordinates(params.to);
    // Prefer one graph whenever it genuinely covers and can snap both ends.
    // This handles alternate/parent extracts without treating their massive
    // overlapping interiors as a border federation.
    if (!params.fromRegion && !params.toRegion) {
      const common = [...indexes.values()]
        .filter(index => contains(index.bbox, from) && contains(index.bbox, to))
        .sort((a, b) => bboxArea(a.bbox) - bboxArea(b.bbox));
      for (const index of common.slice(0, 4)) {
        const direct = await safeRoute(index.region, from, to, params, params.geometry !== false);
        if (direct) return direct;
      }
    }
    const [fromRegion, toRegion] = await Promise.all([
      resolveRegion(from, params.fromRegion),
      resolveRegion(to, params.toRegion)
    ]);
    if (!indexes.has(fromRegion) || !indexes.has(toRegion)) {
      throw routeError("RANGEFIND_ROUTE_NO_REGION", `Unknown ${profile} route region ${fromRegion} or ${toRegion}.`);
    }
    if (fromRegion === toRegion) return (await graphFor(fromRegion)).route(params);
    const paths = await connectedRegionPaths(
      fromRegion,
      toRegion,
      indexes,
      Math.max(1, Math.floor(options.maxRegionPaths ?? 3)),
      Math.max(1, Math.floor(options.maxRegionHops ?? 12)),
      Math.max(1, Math.floor(options.maxRegionExpansions ?? 512)),
      async (left, right) => (await sharedPortals(left, right, from, to)).length > 0
    );
    let winner = null;
    for (const regionPath of paths) {
      const candidate = await evaluatePath(regionPath, from, to, params);
      if (candidate && (!winner || candidate.seconds < winner.seconds)) winner = { ...candidate, regionPath };
    }
    if (!winner) {
      throw routeError("RANGEFIND_ROUTE_REGIONS_DISCONNECTED", `No shared OSM road portals connect ${fromRegion} to ${toRegion}.`);
    }
    const points = [from, ...winner.transitions, to];
    const legs = await Promise.all(winner.regionPath.map((region, index) => (
      safeRoute(region, points[index], points[index + 1], params, params.geometry !== false)
    )));
    if (legs.some(leg => !leg)) throw routeError("RANGEFIND_ROUTE_REGIONS_DISCONNECTED", "The selected regional portal path became unavailable.");
    const merged = mergeLegs(legs, winner.regionPath, winner.transitions.map((portal, index) => ({
      osmNodeId: portal.id,
      point: { lat: portal.lat, lon: portal.lon },
      fromRegion: winner.regionPath[index],
      toRegion: winner.regionPath[index + 1]
    })));
    if (params.geometry === false) {
      delete merged.geometry;
      delete merged.steps;
      delete merged.edges;
      delete merged.junctions;
      delete merged.distanceMeters;
    }
    return merged;
  }

  async function matrix(params) {
    const points = params.points || [];
    const seconds = Array.from({ length: points.length }, () => new Float64Array(points.length).fill(Infinity));
    for (let index = 0; index < points.length; index++) seconds[index][index] = 0;
    const pairs = [];
    const stats = { objectFetches: 0, bytesFetched: 0, cellFetches: 0, overlayFetches: 0, unpackCellFetches: 0, httpRequests: 0, shardsTouched: [], regionsTouched: [] };
    const shards = new Set();
    const regions = new Set();
    for (let from = 0; from < points.length; from++) for (let to = 0; to < points.length; to++) {
      if (from !== to) pairs.push({ from, to });
    }
    await mapLimit(pairs, options.portalConcurrency || 8, async pair => {
      const result = await route({ ...params, from: points[pair.from], to: points[pair.to], geometry: false });
      seconds[pair.from][pair.to] = result.seconds;
      for (const key of ["objectFetches", "bytesFetched", "cellFetches", "overlayFetches", "unpackCellFetches", "httpRequests"]) {
        stats[key] += Number(result.stats?.[key] || 0);
      }
      for (const shard of result.stats?.shardsTouched || []) shards.add(shard);
      for (const region of result.regions || []) regions.add(region);
    });
    stats.shardsTouched = [...shards];
    stats.regionsTouched = [...regions];
    return { seconds, stats };
  }

  return {
    catalog,
    profile,
    route,
    matrix,
    regionFor: point => regionFor(coordinates(point)),
    clearCaches() {
      graphCache.clear();
      portalCache.clear();
    }
  };
}
