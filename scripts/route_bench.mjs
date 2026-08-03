// Route-graph benchmark: correctness against reference Dijkstra, fetch
// budgets, and latency for rfroutegraph-v1 indexes built from OSM extracts.
//
//   node scripts/route_bench.mjs build <graph.bin> <indexDir> [--shards N]
//   node scripts/route_bench.mjs bench <graph.bin> <indexDir> [--pairs N]
//     [--exact N] [--rtt MS] [--seed N]
//   node scripts/route_bench.mjs compare <graph.bin> <dirA> <dirB> [--pairs N]
//   node scripts/route_bench.mjs itinerary <indexDir> <lat,lon> <lat,lon> ...
//
// bench reports per-distance-bucket medians for settled nodes, object
// fetches, bytes, and wall time (cold = fresh engine and caches per query,
// with an optional simulated per-request RTT; warm = shared engine).

import { readFileSync } from "node:fs";
import { join } from "node:path";
import { MinHeap, bucketWeight } from "../src/route_graph.js";

// Heuristic weekday-rush metric for the --peak build flag; real speed
// profiles plug into the same classFactors shape.
const PEAK_BUCKET = {
  name: "peak",
  rules: [
    { dayMask: 0b0111110, startHour: 7, endHour: 9 },
    { dayMask: 0b0111110, startHour: 16, endHour: 18 }
  ],
  classFactors: {
    motorway: 1.15, motorway_link: 1.3, trunk: 1.2, trunk_link: 1.3,
    primary: 1.5, primary_link: 1.3, secondary: 1.45, secondary_link: 1.3,
    tertiary: 1.4, tertiary_link: 1.3, unclassified: 1.3, residential: 1.35,
    living_street: 1.2, service: 1.2, road: 1.3
  }
};
import { buildRouteGraph } from "../src/route_graph_build.js";
import { createRouteGraphFileIo, openRouteGraphDir } from "../src/route_graph_node.js";
import { readRoadGraph } from "./osm_road_graph.mjs";

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function flag(name, fallback) {
  const index = process.argv.indexOf(`--${name}`);
  if (index < 0 || index + 1 >= process.argv.length) return fallback;
  return Number(process.argv[index + 1]);
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

function renumberedCsr(graph, indexDir) {
  const orderBytes = readFileSync(join(indexDir, "node-order.bin"));
  const nodeOrder = new Uint32Array(orderBytes.buffer, orderBytes.byteOffset, orderBytes.byteLength / 4);
  const newId = new Uint32Array(graph.nodeLat.length);
  for (let i = 0; i < nodeOrder.length; i++) newId[nodeOrder[i]] = i;
  const from = Uint32Array.from(graph.edgeFrom, node => newId[node]);
  const to = Uint32Array.from(graph.edgeTo, node => newId[node]);
  const csr = buildCsr(graph.nodeLat.length, from, to);
  const lat = new Int32Array(graph.nodeLat.length);
  const lon = new Int32Array(graph.nodeLat.length);
  for (let i = 0; i < nodeOrder.length; i++) {
    lat[i] = graph.nodeLat[nodeOrder[i]];
    lon[i] = graph.nodeLon[nodeOrder[i]];
  }
  return { csr, lat, lon, weights: graph.edgeWeightDs, nodeOrder };
}

function referenceSeconds(reference, forwardSeeds, backwardSeeds, sameEdgeWeight) {
  const { csr, weights } = reference;
  const dist = reference.dist || (reference.dist = new Float64Array(reference.lat.length));
  dist.fill(Infinity);
  const heap = new MinHeap();
  for (const seed of forwardSeeds) {
    if (seed.weight < dist[seed.node]) {
      dist[seed.node] = seed.weight;
      heap.push(seed.weight, seed.node);
    }
  }
  while (heap.size) {
    const weight = heap.peekWeight();
    const node = heap.pop();
    if (weight !== dist[node]) continue;
    for (let e = csr.rowStart[node]; e < csr.rowStart[node + 1]; e++) {
      const next = weight + weights[csr.edgeIds[e]];
      if (next < dist[csr.targets[e]]) {
        dist[csr.targets[e]] = next;
        heap.push(next, csr.targets[e]);
      }
    }
  }
  let best = sameEdgeWeight ?? Infinity;
  for (const seed of backwardSeeds) {
    if (dist[seed.node] + seed.weight < best) best = dist[seed.node] + seed.weight;
  }
  return best / 10;
}

function seedsFromSnap(snapResult, side) {
  return snapResult.matches.map(match => side === "forward"
    ? { node: match.toNode, weight: Math.round(match.weight * (1 - match.ratio)) }
    : { node: match.fromNode, weight: Math.round(match.weight * match.ratio) });
}

function sameEdgeShortcut(snapFrom, snapTo) {
  let best = null;
  for (const from of snapFrom.matches) {
    for (const to of snapTo.matches) {
      if (from.leaf === to.leaf && from.edgeIndex === to.edgeIndex && to.ratio >= from.ratio) {
        const weight = Math.round(from.weight * (to.ratio - from.ratio));
        if (best == null || weight < best) best = weight;
      }
    }
  }
  return best;
}

function haversineKm(a, b) {
  const toRad = Math.PI / 180;
  const dLat = (b.lat - a.lat) * toRad;
  const dLon = (b.lon - a.lon) * toRad;
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const h = sinLat * sinLat + Math.cos(a.lat * toRad) * Math.cos(b.lat * toRad) * sinLon * sinLon;
  return 2 * 6371.0087714 * Math.asin(Math.min(1, Math.sqrt(h)));
}

function median(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length * p) / 100))];
}

function randomPoints(graph, count, seed) {
  const random = lcg(seed);
  const points = [];
  for (let i = 0; i < count; i++) {
    const from = Math.floor(random() * graph.nodeLat.length);
    const to = Math.floor(random() * graph.nodeLat.length);
    points.push([
      { lat: graph.nodeLat[from] / 1e7, lon: graph.nodeLon[from] / 1e7 },
      { lat: graph.nodeLat[to] / 1e7, lon: graph.nodeLon[to] / 1e7 }
    ]);
  }
  return points;
}

// Wraps the file io with a simulated per-request RTT, so local benches show
// what a CDN-backed browser client would experience per query wave.
function delayedIo(dir, rttMs) {
  const io = createRouteGraphFileIo(dir);
  if (!rttMs) return io;
  const delay = () => new Promise(resolve => setTimeout(resolve, rttMs));
  return {
    ...io,
    async readFile(path) {
      await delay();
      return io.readFile(path);
    },
    async readRange(path, offset, length) {
      await delay();
      return io.readRange(path, offset, length);
    },
    counters: io.counters,
    resetCounters: io.resetCounters
  };
}

async function commandBuild() {
  const [graphPath, indexDir] = process.argv.slice(3);
  const shards = flag("shards", 1);
  // Content-addressed pack names change with content; clear stale packs
  // from previous builds so the directory holds exactly one index.
  const { rmSync } = await import("node:fs");
  rmSync(indexDir, { recursive: true, force: true });
  const graph = await readRoadGraph(graphPath);
  const started = Date.now();
  buildRouteGraph(graph, indexDir, {
    shards,
    leafNodes: flag("leaf-nodes", 1280),
    fanout: flag("fanout", 8),
    topMaxCells: flag("top-max-cells", 32),
    timeBuckets: process.argv.includes("--peak") ? [PEAK_BUCKET] : [],
    log: message => console.log(message)
  });
  console.log(`build: ${((Date.now() - started) / 1000).toFixed(1)}s`);
}

async function commandBench() {
  const [graphPath, indexDir] = process.argv.slice(3);
  const pairCount = flag("pairs", 200);
  const exactCount = flag("exact", 50);
  const rttMs = flag("rtt", 0);
  const seed = flag("seed", 20260803);
  const graph = await readRoadGraph(graphPath);
  const reference = renumberedCsr(graph, indexDir);
  const pairs = randomPoints(graph, pairCount, seed);

  // Correctness: exact equality against reference Dijkstra with identical
  // snap seeds, on the first `exact` pairs, for every time bucket.
  const warmEngine = await openRouteGraphDir(indexDir);
  for (let bucket = 0; bucket < warmEngine.root.buckets.length; bucket++) {
    const bucketName = warmEngine.root.buckets[bucket].name;
    const factors = warmEngine.root.buckets[bucket].factors;
    const scaledWeights = bucket === 0
      ? graph.edgeWeightDs
      : Uint32Array.from(graph.edgeWeightDs, (weight, i) => bucketWeight(weight, graph.edgeClass[i], factors));
    const bucketReference = { ...reference, weights: scaledWeights, dist: null };
    const scale = (match) => bucketWeight(match.weight, match.classCode, factors);
    let exactChecked = 0;
    const bucketExact = bucket === 0 ? exactCount : Math.min(15, exactCount);
    for (let i = 0; i < Math.min(bucketExact, pairs.length); i++) {
      const [from, to] = pairs[i];
      const [snapFrom, snapTo] = await Promise.all([warmEngine.snap(from), warmEngine.snap(to)]);
      const forwardSeeds = snapFrom.matches.map(match => ({ node: match.toNode, weight: Math.round(scale(match) * (1 - match.ratio)) }));
      const backwardSeeds = snapTo.matches.map(match => ({ node: match.fromNode, weight: Math.round(scale(match) * match.ratio) }));
      let sameEdgeWeight = null;
      for (const fromMatch of snapFrom.matches) {
        for (const toMatch of snapTo.matches) {
          if (fromMatch.leaf === toMatch.leaf && fromMatch.edgeIndex === toMatch.edgeIndex && toMatch.ratio >= fromMatch.ratio) {
            const weight = Math.round(scale(fromMatch) * (toMatch.ratio - fromMatch.ratio));
            if (sameEdgeWeight == null || weight < sameEdgeWeight) sameEdgeWeight = weight;
          }
        }
      }
      const expected = referenceSeconds(bucketReference, forwardSeeds, backwardSeeds, sameEdgeWeight);
      const result = await warmEngine.route({ from, to, bucket: bucketName });
      if (result.seconds !== expected) {
        throw new Error(`Mismatch on pair ${i} bucket ${bucketName}: engine ${result.seconds}s vs reference ${expected}s`);
      }
      exactChecked++;
    }
    console.log(`correctness[${bucketName}]: ${exactChecked}/${exactChecked} exact matches vs reference Dijkstra (with geometry unpack asserts)`);
  }

  // Reference CPU baseline: in-memory full-graph Dijkstra.
  {
    const times = [];
    for (let i = 0; i < Math.min(20, pairs.length); i++) {
      const [from, to] = pairs[i];
      const [snapFrom, snapTo] = await Promise.all([warmEngine.snap(from), warmEngine.snap(to)]);
      const started = process.hrtime.bigint();
      referenceSeconds(reference, seedsFromSnap(snapFrom, "forward"), seedsFromSnap(snapTo, "backward"), null);
      times.push(Number(process.hrtime.bigint() - started) / 1e6);
    }
    console.log(`baseline: in-memory full Dijkstra median ${median(times).toFixed(1)}ms over ${reference.lat.length} nodes`);
  }

  // Fetch budgets and latency per distance bucket.
  const buckets = {
    "local <10km": [],
    "regional 10-100km": [],
    "long >100km": []
  };
  for (const [from, to] of pairs) {
    const km = haversineKm(from, to);
    const bucket = km < 10 ? "local <10km" : km < 100 ? "regional 10-100km" : "long >100km";
    buckets[bucket].push([from, to]);
  }
  for (const [name, bucketPairs] of Object.entries(buckets)) {
    if (!bucketPairs.length) continue;
    const cold = { time: [], requests: [], bytes: [], settled: [] };
    const warm = { time: [] };
    const geometryTimes = [];
    for (const [from, to] of bucketPairs.slice(0, 40)) {
      const io = delayedIo(indexDir, rttMs);
      const engine = await openRouteGraphDir(indexDir, { io });
      const started = process.hrtime.bigint();
      const result = await engine.route({ from, to, geometry: false });
      cold.time.push(Number(process.hrtime.bigint() - started) / 1e6);
      cold.requests.push(io.counters.requests);
      cold.bytes.push(io.counters.bytes);
      cold.settled.push(result.settledNodes);
      const geometryStart = process.hrtime.bigint();
      await engine.route({ from, to });
      geometryTimes.push(Number(process.hrtime.bigint() - geometryStart) / 1e6);
      const warmStart = process.hrtime.bigint();
      await warmEngine.route({ from, to, geometry: false });
      warm.time.push(Number(process.hrtime.bigint() - warmStart) / 1e6);
    }
    console.log(`${name}: ${bucketPairs.length} pairs (measured ${cold.time.length})`);
    console.log(`  cold: median ${median(cold.time).toFixed(1)}ms p90 ${percentile(cold.time, 90).toFixed(1)}ms | requests median ${median(cold.requests)} p90 ${percentile(cold.requests, 90)} | KB median ${(median(cold.bytes) / 1024).toFixed(0)} p90 ${(percentile(cold.bytes, 90) / 1024).toFixed(0)} | settled median ${median(cold.settled)}`);
    console.log(`  cold+geometry: median ${median(geometryTimes).toFixed(1)}ms (adds path unpack fetches)`);
    console.log(`  warm: median ${median(warm.time).toFixed(1)}ms`);
  }
  if (rttMs) console.log(`(cold timings include ${rttMs}ms simulated RTT per request)`);
}

async function commandCompare() {
  const [graphPath, dirA, dirB] = process.argv.slice(3);
  const pairCount = flag("pairs", 100);
  const graph = await readRoadGraph(graphPath);
  const pairs = randomPoints(graph, pairCount, flag("seed", 4242));
  const engineA = await openRouteGraphDir(dirA);
  const engineB = await openRouteGraphDir(dirB);
  let matches = 0;
  for (const [from, to] of pairs) {
    const [a, b] = await Promise.all([
      engineA.route({ from, to, geometry: false }),
      engineB.route({ from, to, geometry: false })
    ]);
    if (a.seconds !== b.seconds) {
      throw new Error(`Divergence: ${a.seconds}s vs ${b.seconds}s for ${JSON.stringify([from, to])}`);
    }
    matches++;
  }
  const statsB = engineB.stats();
  console.log(`compare: ${matches}/${pairs.length} identical routes`);
  console.log(`second index touched shards: ${statsB.shardsTouched.join(", ")}`);
}

async function commandItinerary() {
  const indexDir = process.argv[3];
  const stops = process.argv.slice(4)
    .filter(arg => arg.includes(","))
    .map(arg => {
      const [lat, lon] = arg.split(",").map(Number);
      return { lat, lon };
    });
  const engine = await openRouteGraphDir(indexDir);
  const started = Date.now();
  const trip = await engine.itinerary({ stops, geometry: true });
  console.log(`stops in optimized order: ${trip.order.join(" -> ")}`);
  console.log(`total: ${(trip.totalSeconds / 60).toFixed(1)}min, ${(trip.totalMeters / 1000).toFixed(1)}km in ${Date.now() - started}ms`);
  for (const leg of trip.legs) {
    console.log(`  leg ${leg.fromStop}->${leg.toStop}: ${(leg.seconds / 60).toFixed(1)}min ${(leg.distanceMeters / 1000).toFixed(1)}km, ${leg.geometry.length} geometry points, ${leg.steps.length} named steps`);
  }
  console.log(`fetch stats: ${JSON.stringify(engine.stats())}`);

  // Close the loop with the existing corridor lane: the itinerary geometry
  // is directly consumable by prepareRoute / search-along-route.
  const { prepareRoute } = await import("../src/geo_route.js");
  const line = trip.legs.flatMap(leg => leg.geometry.map(([lat, lon]) => ({ lat, lon })));
  const prepared = prepareRoute(line, { corridorMeters: 1500 });
  console.log(`corridor: ${(prepared.totalMeters / 1000).toFixed(1)}km route rasterizes to ${prepared.boxes.length} prune boxes for search-along-route`);
}

const command = process.argv[2];
if (command === "build") await commandBuild();
else if (command === "bench") await commandBench();
else if (command === "compare") await commandCompare();
else if (command === "itinerary") await commandItinerary();
else {
  console.log("usage: node scripts/route_bench.mjs build|bench|compare|itinerary ...");
  process.exit(1);
}
