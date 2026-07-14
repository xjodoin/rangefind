#!/usr/bin/env node

// Sharded OSM benchmark: splits a region corpus into K geographic shards,
// builds a monolithic and a sharded index from the same corpus, then
// compares correctness (identical rankings), cold/warm latency, and
// transfer per query lane.
//
// Usage:
//   node scripts/osm_fixture.mjs jsonl --region=quebec --no-rqa
//   node scripts/osm_shard_bench.mjs --root=examples/osm-geo --shards=4
//   node scripts/osm_shard_bench.mjs --skip-build            # reuse indexes
//   node scripts/osm_shard_bench.mjs --skip-mono             # sharded only

import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { availableParallelism } from "node:os";
import { createFetchMeter, dirStats, kb, mean, quantile, serveStatic } from "./bench_support.mjs";
import { buildOsmIndex, buildOsmShardedIndex } from "../src/integrations/osm/node/builder.js";
import { createSearch } from "../src/runtime.js";

function parseArgs(argv) {
  const args = {
    root: "examples/osm-geo",
    input: "",
    shards: 4,
    runs: 5,
    size: 10,
    build: true,
    mono: true,
    out: ""
  };
  for (const arg of argv) {
    if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--input=")) args.input = arg.slice("--input=".length);
    else if (arg.startsWith("--shards=")) args.shards = Number(arg.slice("--shards=".length)) || args.shards;
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--size=")) args.size = Number(arg.slice("--size=".length)) || args.size;
    else if (arg === "--skip-build") args.build = false;
    else if (arg === "--skip-mono") args.mono = false;
    else if (arg.startsWith("--out=")) args.out = arg.slice("--out=".length);
  }
  if (!args.input) args.input = resolve(args.root, "data/osm-places.jsonl");
  if (!args.out) args.out = resolve(args.root, "osm-shard-bench.json");
  return args;
}

async function eachLine(path, fn) {
  const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of rl) {
    if (line) await fn(line);
  }
}

// Split by longitude quantiles so shards carry equal doc counts — a stand-in
// for the geographic cells a planet build would use.
async function splitCorpus(input, shardCount, outDir) {
  const lons = [];
  await eachLine(input, line => {
    const lonMatch = line.match(/"lon":(-?\d+(?:\.\d+)?)/u);
    if (lonMatch) lons.push(Number(lonMatch[1]));
  });
  const sorted = Float64Array.from(lons).sort();
  const cuts = [];
  for (let i = 1; i < shardCount; i++) {
    cuts.push(sorted[Math.floor((sorted.length * i) / shardCount)]);
  }
  mkdirSync(outDir, { recursive: true });
  const writers = Array.from({ length: shardCount }, (_, i) =>
    createWriteStream(resolve(outDir, `shard-${i}.jsonl`)));
  const counts = new Array(shardCount).fill(0);
  await eachLine(input, async line => {
    const lonMatch = line.match(/"lon":(-?\d+(?:\.\d+)?)/u);
    const lon = lonMatch ? Number(lonMatch[1]) : 0;
    let shard = 0;
    while (shard < cuts.length && lon >= cuts[shard]) shard++;
    counts[shard]++;
    if (!writers[shard].write(line + "\n")) {
      await new Promise(resolveDrain => writers[shard].once("drain", resolveDrain));
    }
  });
  await Promise.all(writers.map(writer => new Promise(resolveEnd => writer.end(resolveEnd))));
  return { cuts, counts, docs: lons.length };
}

// Deterministic query sampling: place names at fixed corpus offsets, so runs
// are reproducible without hardcoding a region's vocabulary.
async function sampleQueries(input, docs) {
  const wanted = [0.1, 0.3, 0.5, 0.7, 0.9].map(f => Math.floor(docs * f));
  const targets = new Set(wanted);
  const names = [];
  let densestKey = "";
  const cells = new Map();
  let index = 0;
  let center = null;
  await eachLine(input, line => {
    const doc = JSON.parse(line);
    if (Number.isFinite(doc.lat) && Number.isFinite(doc.lon)) {
      const key = `${Math.round(doc.lat * 20)},${Math.round(doc.lon * 20)}`;
      const cell = (cells.get(key) || 0) + 1;
      cells.set(key, cell);
      if (!densestKey || cell > cells.get(densestKey)) {
        densestKey = key;
        center = { lat: doc.lat, lon: doc.lon };
      }
    }
    if (targets.has(index) && doc.search_name) {
      names.push(String(doc.search_name).split(/\s+/u).slice(0, 3).join(" "));
    }
    index++;
  });
  return { names: names.filter(Boolean), center };
}

async function timeQuery(engine, run) {
  const started = performance.now();
  const response = await run(engine);
  return { ms: performance.now() - started, response };
}

async function benchTarget(name, baseUrl, lanes, runs) {
  const meter = createFetchMeter();
  const report = {};
  for (const lane of lanes) {
    // Cold: fresh engine, empty caches — includes manifest and routing cost.
    meter.reset();
    const coldEngine = await createSearch({ baseUrl });
    const cold = await timeQuery(coldEngine, lane.run);
    const coldMeter = meter.snapshot();

    const warmMs = [];
    let warmResponse = cold.response;
    for (let i = 0; i < runs; i++) {
      const warm = await timeQuery(coldEngine, lane.run);
      warmMs.push(warm.ms);
      warmResponse = warm.response;
    }
    report[lane.name] = {
      coldMs: Math.round(cold.ms * 10) / 10,
      coldRequests: coldMeter.requests,
      coldKb: kb(coldMeter.bytes),
      warmMeanMs: Math.round(mean(warmMs) * 100) / 100,
      warmP95Ms: Math.round(quantile(warmMs, 0.95) * 100) / 100,
      total: warmResponse.total,
      shardsQueried: warmResponse.stats?.shardsQueried ?? null,
      results: (warmResponse.results || []).map(item => ({ id: item.id, score: item.score, distanceMeters: item.distanceMeters }))
    };
    console.log(`  ${name} ${lane.name}: cold ${report[lane.name].coldMs}ms/${report[lane.name].coldKb}KB (${coldMeter.requests} req), warm ${report[lane.name].warmMeanMs}ms${report[lane.name].shardsQueried != null ? `, shards ${report[lane.name].shardsQueried}` : ""}`);
  }
  meter.restore();
  return report;
}

function compareLanes(monoReport, shardReport) {
  const notes = [];
  for (const [lane, mono] of Object.entries(monoReport)) {
    const shard = shardReport[lane];
    if (!shard) continue;
    const monoIds = mono.results.map(item => item.id);
    const shardIds = shard.results.map(item => item.id);
    const sameIds = JSON.stringify(monoIds) === JSON.stringify(shardIds);
    let scoreDrift = 0;
    const monoScores = new Map(mono.results.map(item => [item.id, item.score]));
    for (const item of shard.results) {
      const monoScore = monoScores.get(item.id);
      if (monoScore != null && item.score != null) {
        scoreDrift = Math.max(scoreDrift, Math.abs(monoScore - item.score));
      }
    }
    notes.push({ lane, sameIds, scoreDrift });
    console.log(`  ${lane}: ids ${sameIds ? "identical" : "DIFFER"}, max score drift ${scoreDrift.toExponential(2)}`);
    if (!sameIds) {
      console.log(`    mono:    ${monoIds.slice(0, 5).join(", ")}`);
      console.log(`    sharded: ${shardIds.slice(0, 5).join(", ")}`);
    }
  }
  return notes;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const benchRoot = resolve(args.root, "shard-bench");
  const shardsDir = resolve(benchRoot, "inputs");
  const monoOut = resolve(benchRoot, "mono/public/rangefind");
  const shardedOut = resolve(benchRoot, "sharded/public/rangefind");
  const workerCount = Math.max(1, availableParallelism() - 2);
  const timings = {};

  let split = null;
  if (args.build) {
    console.log(`Splitting ${args.input} into ${args.shards} longitude-quantile shards…`);
    let started = performance.now();
    split = await splitCorpus(args.input, args.shards, shardsDir);
    timings.splitSeconds = Math.round((performance.now() - started) / 100) / 10;
    console.log(`  ${split.docs.toLocaleString()} docs → [${split.counts.map(n => n.toLocaleString()).join(", ")}] in ${timings.splitSeconds}s`);

    if (args.mono) {
      console.log("Building monolithic index…");
      started = performance.now();
      await buildOsmIndex({
        root: resolve(benchRoot, "mono"),
        input: args.input,
        output: monoOut,
        workerCount
      });
      timings.monoBuildSeconds = Math.round((performance.now() - started) / 100) / 10;
      console.log(`  monolithic build: ${timings.monoBuildSeconds}s`);
    }

    console.log("Building sharded index…");
    started = performance.now();
    const sharded = await buildOsmShardedIndex({
      output: shardedOut,
      workerCount,
      shards: Array.from({ length: args.shards }, (_, i) => ({
        id: `shard-${i}`,
        input: resolve(shardsDir, `shard-${i}.jsonl`)
      }))
    });
    timings.shardedBuildSeconds = sharded.seconds;
    console.log(`  sharded build: ${sharded.seconds}s`);
  }

  const rootManifest = JSON.parse(readFileSync(resolve(shardedOut, "manifest.min.json"), "utf8"));
  const docs = rootManifest.total;
  console.log("Sampling queries…");
  const { names, center } = await sampleQueries(args.input, docs);
  console.log(`  text queries: ${names.map(name => JSON.stringify(name)).join(", ")}`);
  console.log(`  geo center: ${center.lat.toFixed(4)}, ${center.lon.toFixed(4)}`);

  const size = args.size;
  const lanes = [
    ...names.slice(0, 3).map((name, i) => ({
      name: `text-${i}`,
      run: engine => engine.search({ q: name, size })
    })),
    {
      name: "suggest",
      run: engine => engine.suggest({ q: (names[0] || "mont").slice(0, 4).toLowerCase() })
    },
    {
      name: "nearest",
      run: engine => engine.search({ q: "", geo: { near: { lat: center.lat, lon: center.lon }, sort: "distance" }, size })
    },
    {
      name: "nearest-radius-2km",
      run: engine => engine.search({ q: "", geo: { near: { lat: center.lat, lon: center.lon, radiusMeters: 2000 }, sort: "distance" }, size })
    },
    {
      name: "text+near-boost",
      run: engine => engine.search({ q: names[0] || "rue", geo: { near: { lat: center.lat, lon: center.lon, radiusMeters: 50000 }, boost: { weight: 2, pivotMeters: 2000 } }, size })
    },
    {
      name: "nearest+facets",
      run: engine => engine.search({ q: "", geo: { near: { lat: center.lat, lon: center.lon, radiusMeters: 5000 }, sort: "distance" }, size, facets: ["category"] })
    }
  ];

  const report = { input: args.input, docs, shards: args.shards, timings, sizes: {} };
  report.sizes.sharded = dirStats(shardedOut);
  if (existsSync(monoOut)) report.sizes.mono = dirStats(monoOut);

  const shardedServer = await serveStatic(resolve(benchRoot, "sharded/public"));
  try {
    console.log("Benchmarking sharded index…");
    report.sharded = await benchTarget("sharded", `${shardedServer.url}rangefind/`, lanes, args.runs);
  } finally {
    await shardedServer.close();
  }

  if (existsSync(monoOut)) {
    const monoServer = await serveStatic(resolve(benchRoot, "mono/public"));
    try {
      console.log("Benchmarking monolithic index…");
      report.mono = await benchTarget("mono", `${monoServer.url}rangefind/`, lanes, args.runs);
    } finally {
      await monoServer.close();
    }
    console.log("Correctness (sharded vs monolithic):");
    report.correctness = compareLanes(report.mono, report.sharded);
  }

  writeFileSync(args.out, JSON.stringify(report, null, 2));
  console.log(`Report written to ${args.out}`);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
