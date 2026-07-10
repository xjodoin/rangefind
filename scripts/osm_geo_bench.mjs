#!/usr/bin/env node

// OpenStreetMap geo query benchmark for Rangefind.
//
// Serves a geo-enabled index built by scripts/osm_fixture.mjs, replays a set
// of geo query lanes, and reports cold request counts, transfer, warm
// latency, and exhaustive-oracle correctness for each lane.
//
// Usage:
//   node scripts/osm_fixture.mjs all --region=luxembourg
//   node scripts/osm_geo_bench.mjs --root=examples/osm-geo
//   node scripts/osm_geo_bench.mjs --root=examples/osm-geo --skip-oracle

import { readFileSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { createReadStream } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createFetchMeter, kb, mean, quantile, serveStatic } from "./bench_support.mjs";
import { DEFAULT_ANALYZER } from "../src/analysis.js";
import { foldMulti } from "../src/analysis_fold.js";
import { readConfig } from "../src/config.js";
import { createSearch } from "../src/runtime.js";
import { haversineMetersE7, latToE7, lonToE7, boxContainsPointE7 } from "../src/geo_tree.js";
import { analyzeDocumentTerms, analyzeFieldText } from "../src/scoring.js";

function parseArgs(argv) {
  const args = { root: "examples/osm-geo", runs: 5, size: 10, oracle: true, out: "" };
  for (const arg of argv) {
    if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--size=")) args.size = Number(arg.slice("--size=".length)) || args.size;
    else if (arg === "--skip-oracle") args.oracle = false;
    else if (arg.startsWith("--out=")) args.out = arg.slice("--out=".length);
  }
  if (!args.out) args.out = `${args.root}/osm-geo-bench.json`;
  return args;
}

async function measureCorpus(path, config, needAvgLens) {
  const cells = new Map();
  const fieldTotals = Object.fromEntries(config.fields.map(field => [field.name, 0]));
  let docs = 0;
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line) continue;
    const doc = JSON.parse(line);
    const latE7 = latToE7(doc.lat);
    const lonE7 = lonToE7(doc.lon);
    if (latE7 == null || lonE7 == null) continue;
    docs++;
    const key = `${Math.round(latE7 / 500000)},${Math.round(lonE7 / 500000)}`;
    cells.set(key, (cells.get(key) || 0) + 1);
    if (needAvgLens) {
      for (const field of config.fields) fieldTotals[field.name] += analyzeFieldText(doc, field, config).length;
    }
  }
  let bestKey = "";
  let bestCount = -1;
  for (const [key, count] of cells) {
    if (count > bestCount) {
      bestKey = key;
      bestCount = count;
    }
  }
  const [latCell, lonCell] = bestKey.split(",").map(Number);
  return {
    center: { lat: latCell / 20, lon: lonCell / 20 },
    avgLens: needAvgLens
      ? Object.fromEntries(config.fields.map(field => [field.name, Math.max(1, fieldTotals[field.name] / Math.max(1, docs))]))
      : null
  };
}

function boundedNearest(rows, item, size) {
  const compare = (a, b) => a.dist - b.dist || (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  if (rows.length < size) {
    rows.push(item);
    rows.sort(compare);
    return;
  }
  if (compare(item, rows[rows.length - 1]) >= 0) return;
  rows[rows.length - 1] = item;
  for (let index = rows.length - 1; index > 0 && compare(rows[index], rows[index - 1]) < 0; index--) {
    [rows[index], rows[index - 1]] = [rows[index - 1], rows[index]];
  }
}

async function streamOracle(path, center, box, radius, nearestSize, textGuard, textTerm, config, avgLens) {
  const boxE7 = {
    minLatE7: latToE7(box.minLat),
    maxLatE7: latToE7(box.maxLat),
    minLonE7: lonToE7(box.minLon),
    maxLonE7: lonToE7(box.maxLon)
  };
  const centerLatE7 = latToE7(center.lat);
  const centerLonE7 = lonToE7(center.lon);
  const result = {
    points: 0,
    boxCount: 0,
    radiusCount: 0,
    textRadiusCount: 0,
    nearest: [],
    textNearest: []
  };
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line) continue;
    const doc = JSON.parse(line);
    const latE7 = latToE7(doc.lat);
    const lonE7 = lonToE7(doc.lon);
    if (latE7 == null || lonE7 == null) continue;
    result.points++;
    if (boxContainsPointE7(boxE7, latE7, lonE7)) result.boxCount++;
    const dist = haversineMetersE7(centerLatE7, centerLonE7, latE7, lonE7);
    if (dist <= radius) result.radiusCount++;
    boundedNearest(result.nearest, { id: doc.id, dist }, nearestSize);

    // The exact analyzer pass is relatively expensive over tens of millions
    // of documents. A folded substring guard retains diacritic, plural, and
    // compound forms while skipping the analyzer for almost every OSM row.
    if (!foldMulti(line).includes(textGuard)) continue;
    const selectedTerms = new Set(analyzeDocumentTerms(doc, config, avgLens).map(([term]) => term));
    if (!selectedTerms.has(textTerm)) continue;
    boundedNearest(result.textNearest, { id: doc.id, dist }, nearestSize);
    if (dist <= radius) result.textRadiusCount++;
  }
  return result;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const root = resolve(args.root);
  const sourcePath = resolve(root, "data/osm-places.jsonl");
  const config = await readConfig(resolve(root, "rangefind.config.json"));
  const manifest = JSON.parse(readFileSync(resolve(root, "public/rangefind/manifest.min.json"), "utf8"));
  if (!manifest.features?.geo) throw new Error(`Index at ${args.root} has no geo support; run scripts/osm_fixture.mjs first.`);
  const measuredAvgLens = manifest.stats?.field_avg_lengths || null;
  const measured = args.oracle
    ? await measureCorpus(sourcePath, config, !measuredAvgLens)
    : { center: { lat: 49.61, lon: 6.13 }, avgLens: null };
  const center = measured.center;
  const avgLens = measuredAvgLens || measured.avgLens;
  console.log(`[bench] ${manifest.total} docs, ${manifest.geo.fields.location.total} points, ${manifest.geo.fields.location.leaves} leaves`);
  console.log(`[bench] query center ${center.lat.toFixed(4)}, ${center.lon.toFixed(4)}`);

  const viewport = {
    minLat: center.lat - 0.02,
    maxLat: center.lat + 0.02,
    minLon: center.lon - 0.03,
    maxLon: center.lon + 0.03
  };
  const wideViewport = {
    minLat: center.lat - 0.12,
    maxLat: center.lat + 0.12,
    minLon: center.lon - 0.18,
    maxLon: center.lon + 0.18
  };
  const near1km = { lat: center.lat, lon: center.lon, radiusMeters: 1000 };
  const near5km = { lat: center.lat, lon: center.lon, radiusMeters: 5000 };
  // "restaurant" is intentionally broad enough to exceed the runtime's
  // exact distance-sort posting guard on a national corpus. Use a common but
  // selective place term there so the lane measures exact text-nearest work
  // rather than merely re-testing the safety error.
  const textQuery = manifest.total >= 10_000_000 ? "bakery" : "restaurant";
  const textGuard = textQuery === "bakery" ? "baker" : foldMulti(textQuery);

  const lanes = [
    { name: "viewport-browse", params: { q: "", geo: { box: viewport } } },
    { name: "wide-viewport-browse", params: { q: "", geo: { box: wideViewport } } },
    { name: "radius-1km-browse", params: { q: "", geo: { near: near1km } } },
    { name: "radius-5km-browse", params: { q: "", geo: { near: near5km } } },
    { name: "nearest", params: { q: "", geo: { near: { lat: center.lat, lon: center.lon }, sort: "distance" } } },
    { name: "nearest-radius-5km", params: { q: "", geo: { near: near5km, sort: "distance" } } },
    {
      name: "nearest-pharmacy",
      params: {
        q: "",
        filters: { facets: { category: ["amenity"] } },
        geo: { near: { lat: center.lat, lon: center.lon }, sort: "distance" }
      }
    },
    { name: "text-radius-5km", params: { q: textQuery, geo: { near: near5km } } },
    {
      name: "text-nearest",
      params: { q: textQuery, geo: { near: { lat: center.lat, lon: center.lon }, sort: "distance" } }
    },
    { name: "text-viewport", params: { q: "pharmacie", geo: { box: wideViewport } } },
    {
      name: "text-radius-boost",
      params: { q: textQuery, geo: { near: near5km, boost: { weight: 2, pivotMeters: 500 } } }
    },
    { name: "text-only-baseline", params: { q: textQuery } }
  ];

  const meter = createFetchMeter(/\/rangefind\//u);
  const report = { generated_for: args.root, runs: args.runs, size: args.size, text_query: textQuery, lanes: {} };

  for (const lane of lanes) {
    const server = await serveStatic(resolve(root, "public"));
    try {
      meter.reset();
      const engine = await createSearch({ baseUrl: `${server.url}rangefind/` });
      const boot = meter.snapshot();
      meter.reset();
      const coldStart = performance.now();
      const coldResponse = await engine.search({ ...lane.params, size: args.size });
      const coldMs = performance.now() - coldStart;
      const cold = meter.snapshot();

      const warmTimes = [];
      for (let run = 0; run < args.runs; run++) {
        const start = performance.now();
        await engine.search({ ...lane.params, size: args.size });
        warmTimes.push(performance.now() - start);
      }

      const stats = coldResponse.stats || {};
      report.lanes[lane.name] = {
        total: coldResponse.total,
        results: coldResponse.results.length,
        boot_requests: boot.requests,
        boot_kb: kb(boot.bytes),
        cold_requests: cold.requests,
        cold_kb: kb(cold.bytes),
        cold_ms: Math.round(coldMs * 10) / 10,
        warm_ms_mean: Math.round(mean(warmTimes) * 100) / 100,
        warm_ms_p95: Math.round(quantile(warmTimes, 0.95) * 100) / 100,
        geo: {
          lane: stats.geoLane || (stats.geoBoost ? "textBoost" : lane.params.geo ? "textFilter" : "none"),
          tree_levels: stats.geoTreeLevels,
          directory_leaves: stats.geoDirectoryLeaves,
          candidate_branches: stats.geoCandidateBranches,
          branch_pages_fetched: stats.geoBranchPagesFetched,
          candidate_leaves: stats.geoCandidateLeaves,
          leaves_visited: stats.geoLeavesVisited,
          leaf_pages_fetched: stats.geoLeafPagesFetched,
          points_scanned: stats.geoPointsScanned,
          points_accepted: stats.geoPointsAccepted,
          exact: stats.exact
        }
      };
      const row = report.lanes[lane.name];
      console.log(
        `[lane] ${lane.name.padEnd(22)} total=${String(row.total).padStart(6)} cold=${String(row.cold_requests).padStart(3)} req ${String(row.cold_kb).padStart(8)} KB ${String(row.cold_ms).padStart(7)} ms warm=${String(row.warm_ms_mean).padStart(7)} ms` +
        (row.geo.leaves_visited != null ? ` leaves=${row.geo.leaves_visited}/${row.geo.candidate_leaves} scanned=${row.geo.points_scanned}` : "")
      );
    } finally {
      await server.close();
    }
  }

  if (args.oracle) {
    console.log("[oracle] streaming exhaustive verification without retaining the corpus");
    const textTerm = DEFAULT_ANALYZER.analyzeTerms(textQuery)[0].term;
    const oracle = await streamOracle(sourcePath, center, viewport, near5km.radiusMeters, 20, textGuard, textTerm, config, avgLens);
    const server = await serveStatic(resolve(root, "public"));
    try {
      const engine = await createSearch({ baseUrl: `${server.url}rangefind/` });
      const failures = [];

      const boxResponse = await engine.search({ q: "", geo: { box: viewport }, size: 100 });
      const boxOk = boxResponse.approximate
        ? boxResponse.results.length === Math.min(100, oracle.boxCount)
        : boxResponse.total === oracle.boxCount;
      if (!boxOk) failures.push(`viewport-browse: got total ${boxResponse.total}, expected ${oracle.boxCount}`);

      const radiusResponse = await engine.search({ q: "", geo: { near: near5km }, size: 100 });
      const radiusOk = radiusResponse.approximate
        ? radiusResponse.results.length === Math.min(100, oracle.radiusCount)
        : radiusResponse.total === oracle.radiusCount;
      if (!radiusOk) failures.push(`radius-5km: got total ${radiusResponse.total}, expected ${oracle.radiusCount}`);

      const nearestResponse = await engine.search({
        q: "",
        geo: { near: { lat: center.lat, lon: center.lon }, sort: "distance" },
        size: 20
      });
      const nearestDistances = nearestResponse.results.map(result => result.distanceMeters);
      const expectedDistances = oracle.nearest.map(item => Math.round(item.dist * 10) / 10);
      if (JSON.stringify(nearestDistances) !== JSON.stringify(expectedDistances)) {
        failures.push(`nearest: distances ${JSON.stringify(nearestDistances.slice(0, 5))} != ${JSON.stringify(expectedDistances.slice(0, 5))}`);
      }

      const textNearest = await engine.search({
        q: textQuery,
        geo: { near: { lat: center.lat, lon: center.lon }, sort: "distance" },
        size: 20
      });
      const textNearestDistances = textNearest.results.map(result => result.distanceMeters);
      const textNearestExpectedDistances = oracle.textNearest.map(item => Math.round(item.dist * 10) / 10);
      if (JSON.stringify(textNearestDistances) !== JSON.stringify(textNearestExpectedDistances)) {
        failures.push(`text-nearest: distances ${JSON.stringify(textNearestDistances.slice(0, 5))} != ${JSON.stringify(textNearestExpectedDistances.slice(0, 5))}`);
      }

      const textRadius = await engine.search({ q: textQuery, geo: { near: near5km }, size: 100 });
      for (const result of textRadius.results) {
        if (result.distanceMeters > near5km.radiusMeters) {
          failures.push(`text-radius: ${result.name} outside radius at ${result.distanceMeters}m`);
        }
      }
      if (!textRadius.approximate && textRadius.total > oracle.textRadiusCount) {
        failures.push(`text-radius: got total ${textRadius.total}, oracle upper bound ${oracle.textRadiusCount}`);
      }

      report.oracle = { failures, checks: 4, points: oracle.points, mode: "streaming-bounded" };
      if (failures.length) {
        console.error(`[oracle] FAILURES:\n  ${failures.join("\n  ")}`);
        process.exitCode = 1;
      } else {
        console.log("[oracle] all lane checks passed");
      }
    } finally {
      await server.close();
    }
  }

  writeFileSync(resolve(args.out), JSON.stringify(report, null, 2));
  console.log(`[bench] wrote ${args.out}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
