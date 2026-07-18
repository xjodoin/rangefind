#!/usr/bin/env node

// Remote sharded-index benchmark: runs representative query lanes against a
// live sharded deployment (default https://osm.rangefind.dev/) and reports
// cold/warm latency plus request/transfer breakdown per file bucket.
//
// Usage:
//   node scripts/osm_remote_bench.mjs
//   node scripts/osm_remote_bench.mjs --base=https://osm.rangefind.dev/ --runs=5
//   node scripts/osm_remote_bench.mjs --lanes=text-common,nearest
//
// Cold = fresh engine (includes root + shard manifest fetches); warm repeats
// the same query on the same engine so only uncached data pages are fetched.

import { writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { createFetchMeter, kb, mean, quantile } from "./bench_support.mjs";
import { createSearch } from "../src/runtime.js";
import { searchOsmQuery, suggestOsmQuery } from "../src/integrations/osm/query.js";

function parseArgs(argv) {
  const args = {
    base: "https://osm.rangefind.dev/",
    runs: 5,
    size: 10,
    lanes: null,
    out: ""
  };
  for (const arg of argv) {
    if (arg.startsWith("--base=")) args.base = arg.slice("--base=".length);
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--size=")) args.size = Number(arg.slice("--size=".length)) || args.size;
    else if (arg.startsWith("--lanes=")) args.lanes = arg.slice("--lanes=".length).split(",");
    else if (arg.startsWith("--out=")) args.out = arg.slice("--out=".length);
  }
  return args;
}

function bucketFromUrl(url) {
  let path;
  try {
    path = new URL(String(url)).pathname;
  } catch {
    return "other";
  }
  if (/manifest[^/]*\.json/u.test(path)) return path.includes("/shards/") ? "shardManifest" : "rootManifest";
  if (path.includes("/terms/block-packs/")) return "postingBlocks";
  if (path.includes("/terms/packs/")) return "terms";
  if (path.includes("/authority/")) return "authority";
  if (path.includes("/doc-values/")) return "docValues";
  if (path.includes("/docs/pointers/")) return "docPointers";
  if (path.includes("/docs/pages/")) return "docPagePointers";
  if (path.includes("/docs/page-packs/")) return "docPages";
  if (path.includes("/docs/")) return "docs";
  if (path.includes("/geo/")) return "geo";
  if (path.includes("/facets/")) return "facetDictionaries";
  if (path.includes("/filter-bitmaps/")) return "filterBitmaps";
  if (path.includes("/directory-")) return "directory";
  if (path.includes("/suggest/")) return "suggest";
  return "other";
}

// Calgary downtown: inside the alberta shard, far from most others.
const CALGARY = { lat: 51.0447, lon: -114.0719 };
// Berlin Mitte: dense European coverage.
const BERLIN = { lat: 52.52, lon: 13.405 };

function buildLanes(size) {
  return [
    { name: "text-unique", run: engine => engine.search({ q: "calgary tower", size }) },
    { name: "text-common", run: engine => engine.search({ q: "restaurant", size }) },
    { name: "text-city", run: engine => engine.search({ q: "berlin hauptbahnhof", size }) },
    { name: "suggest", run: engine => engine.suggest({ q: "berl" }) },
    { name: "locality-global", run: engine => searchOsmQuery(engine, { q: "Berlin", size }) },
    {
      name: "suggest-select",
      run: async engine => {
        const suggested = await suggestOsmQuery(engine, { q: "berl", size: 8 });
        const item = suggested.suggestions.find(candidate => candidate.text === "Berlin") || suggested.suggestions[0];
        return searchOsmQuery(engine, {
          q: item?.text || "Berlin",
          size,
          ...(item?.shards?.length ? { shards: item.shards } : {})
        });
      }
    },
    { name: "nearest", run: engine => engine.search({ q: "", geo: { near: CALGARY, sort: "distance" }, size }) },
    { name: "nearest-radius-2km", run: engine => engine.search({ q: "", geo: { near: { ...CALGARY, radiusMeters: 2000 }, sort: "distance" }, size }) },
    { name: "text+near-boost", run: engine => engine.search({ q: "coffee", geo: { near: { ...BERLIN, radiusMeters: 5000 }, boost: { weight: 2, pivotMeters: 2000 } }, size }) },
    { name: "scoped-group", run: engine => engine.search({ q: "tim hortons", shards: ["canada"], size }) }
  ];
}

// Remote benches run thousands of concurrent requests against a live CDN.
// Node's fetch opens one socket per in-flight request, so an uncapped storm
// exhausts the OS and times out; browsers multiplex over a handful of
// connections instead. Cap in-flight requests to browser-like concurrency
// and retry transient network failures so one hiccup doesn't kill a lane.
function installRetryingFetch({ attempts = 3, delayMs = 250, concurrency = 64 } = {}) {
  const nativeFetch = globalThis.fetch;
  let active = 0;
  const waiters = [];
  const acquire = () => active < concurrency
    ? (active++, Promise.resolve())
    : new Promise(resolve => waiters.push(resolve));
  const release = () => {
    active--;
    const next = waiters.shift();
    if (next) {
      active++;
      next();
    }
  };
  globalThis.fetch = async (input, init) => {
    await acquire();
    try {
      let lastError = null;
      for (let attempt = 0; attempt < attempts; attempt++) {
        if (attempt) await new Promise(r => setTimeout(r, delayMs * attempt));
        try {
          const response = await nativeFetch(input, init);
          if (attempt < attempts - 1 && [429, 500, 502, 503, 504].includes(response.status)) continue;
          return response;
        } catch (error) {
          lastError = error;
        }
      }
      throw lastError;
    } finally {
      release();
    }
  };
}

async function timeQuery(engine, run) {
  const started = performance.now();
  const response = await run(engine);
  return { ms: performance.now() - started, response };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const lanes = buildLanes(args.size).filter(lane => !args.lanes || args.lanes.includes(lane.name));
  installRetryingFetch();
  const meter = createFetchMeter(/./u, bucketFromUrl);
  const report = { base: args.base, runs: args.runs, at: new Date().toISOString(), lanes: {} };

  console.log(`Benchmarking ${args.base} (${lanes.length} lanes, ${args.runs} warm runs)…`);
  for (const lane of lanes) {
    meter.reset();
    const coldEngine = await createSearch({ baseUrl: args.base });
    const cold = await timeQuery(coldEngine, lane.run);
    const coldMeter = meter.snapshot();

    meter.reset();
    const warmMs = [];
    let warmResponse = cold.response;
    for (let i = 0; i < args.runs; i++) {
      const warm = await timeQuery(coldEngine, lane.run);
      warmMs.push(warm.ms);
      warmResponse = warm.response;
    }
    const warmMeter = meter.snapshot();

    report.lanes[lane.name] = {
      coldMs: Math.round(cold.ms),
      coldRequests: coldMeter.requests,
      coldKb: Math.round(kb(coldMeter.bytes)),
      coldBy: coldMeter.by,
      warmMeanMs: Math.round(mean(warmMs)),
      warmP95Ms: Math.round(quantile(warmMs, 0.95)),
      warmRequestsPerRun: Math.round(warmMeter.requests / args.runs * 10) / 10,
      warmKbPerRun: Math.round(kb(warmMeter.bytes) / args.runs),
      total: warmResponse.total,
      shardsQueried: warmResponse.stats?.shardsQueried ?? null,
      topIds: (warmResponse.results || warmResponse.suggestions || []).slice(0, 3).map(item => item.id || item.text)
    };
    const r = report.lanes[lane.name];
    console.log(`  ${lane.name}: cold ${r.coldMs}ms / ${r.coldRequests} req / ${r.coldKb}KB — warm ${r.warmMeanMs}ms (p95 ${r.warmP95Ms}) / ${r.warmRequestsPerRun} req / ${r.warmKbPerRun}KB${r.shardsQueried != null ? ` — shards ${r.shardsQueried}` : ""}`);
    const byLine = Object.entries(coldMeter.by)
      .sort((a, b) => b[1].bytes - a[1].bytes)
      .map(([bucket, value]) => `${bucket} ${value.requests}req/${Math.round(kb(value.bytes))}KB`)
      .join(", ");
    console.log(`    cold by bucket: ${byLine}`);
  }
  meter.restore();

  if (args.out) {
    writeFileSync(args.out, JSON.stringify(report, null, 2));
    console.log(`Report written to ${args.out}`);
  }
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
