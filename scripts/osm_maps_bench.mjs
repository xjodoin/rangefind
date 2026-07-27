#!/usr/bin/env node

// Production benchmark for common interactive map-search use cases. Each
// case runs in its own Node process so a pathological cold query cannot
// retain caches or heap state that contaminates the next measurement.
//
// Usage:
//   node scripts/osm_maps_bench.mjs
//   node scripts/osm_maps_bench.mjs --cases=category-near,landmark
//   node scripts/osm_maps_bench.mjs --out=/tmp/maps-bench.json

import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { createSearch } from "../src/runtime.js";
import { searchOsmQuery, suggestOsmQuery } from "../src/integrations/osm/query.js";

const MONTREAL = { lat: 45.5019, lon: -73.5674 };
const LAVAL = { lat: 45.6066, lon: -73.7124 };
const TOKYO = { lat: 35.6812, lon: 139.7671 };
const MONTREAL_BOX = {
  minLat: 45.45,
  maxLat: 45.60,
  minLon: -73.70,
  maxLon: -73.50
};

const CASES = [
  {
    id: "suggest-locality",
    family: "autocomplete",
    run: engine => suggestOsmQuery(engine, { q: "mont", size: 8 })
  },
  {
    id: "suggest-poi-locality",
    family: "autocomplete",
    run: engine => suggestOsmQuery(engine, { q: "cinema lav", size: 8 })
  },
  {
    id: "suggest-address",
    family: "autocomplete",
    run: engine => suggestOsmQuery(engine, { q: "845 sher", size: 8 })
  },
  {
    id: "locality-exact",
    family: "locality",
    run: engine => searchOsmQuery(engine, { q: "Laval", size: 18 })
  },
  {
    id: "landmark",
    family: "poi",
    run: engine => searchOsmQuery(engine, { q: "McGill University", near: MONTREAL, size: 18 })
  },
  {
    id: "category-locality",
    family: "category",
    run: engine => searchOsmQuery(engine, { q: "cinema laval", size: 18 })
  },
  {
    id: "category-locality-common",
    family: "category",
    run: engine => searchOsmQuery(engine, { q: "restaurant montreal", size: 18 })
  },
  {
    id: "category-near-sparse",
    family: "near-me",
    run: engine => searchOsmQuery(engine, { q: "gas station near me", near: MONTREAL, size: 18 })
  },
  {
    id: "category-near-dense",
    family: "near-me",
    run: engine => searchOsmQuery(engine, { q: "restaurant", near: MONTREAL, size: 18 })
  },
  {
    id: "brand-near",
    family: "poi",
    run: engine => searchOsmQuery(engine, { q: "Tim Hortons", near: LAVAL, size: 18 })
  },
  {
    id: "civic-address",
    family: "address",
    run: engine => searchOsmQuery(engine, { q: "845 rue Sherbrooke Ouest Montréal", size: 18 })
  },
  {
    id: "street-locality",
    family: "address",
    run: engine => searchOsmQuery(engine, { q: "rue saint denis montreal", size: 18 })
  },
  {
    id: "postal-code",
    family: "address",
    run: engine => searchOsmQuery(engine, { q: "H2X 1Y4", size: 18 })
  },
  {
    id: "viewport-browse",
    family: "viewport",
    run: engine => engine.search({ q: "", geo: { box: MONTREAL_BOX }, size: 18 })
  },
  {
    id: "viewport-category",
    family: "viewport",
    run: engine => searchOsmQuery(engine, {
      q: "restaurant",
      geo: { box: MONTREAL_BOX },
      size: 18
    })
  },
  {
    id: "typo-landmark",
    family: "typo",
    run: engine => searchOsmQuery(engine, { q: "McGil University", near: MONTREAL, size: 18 })
  },
  {
    id: "typo-category-locality",
    family: "typo",
    run: engine => searchOsmQuery(engine, { q: "cinma laval", size: 18 })
  },
  {
    id: "unicode-landmark",
    family: "international",
    run: engine => searchOsmQuery(engine, { q: "東京駅", near: TOKYO, size: 18 })
  }
];

function parseArgs(argv) {
  const out = { base: "https://osm.rangefind.dev/", cases: null, output: "", timeoutMs: 90_000 };
  for (const arg of argv) {
    if (arg.startsWith("--base=")) out.base = arg.slice("--base=".length);
    else if (arg.startsWith("--cases=")) out.cases = new Set(arg.slice("--cases=".length).split(",").filter(Boolean));
    else if (arg.startsWith("--out=")) out.output = arg.slice("--out=".length);
    else if (arg.startsWith("--timeout-ms=")) out.timeoutMs = Math.max(1000, Number(arg.slice("--timeout-ms=".length)) || out.timeoutMs);
  }
  return out;
}

function bucketFromUrl(value) {
  let path;
  try {
    path = new URL(String(value)).pathname;
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
  if (path.includes("/facets/")) return "facets";
  if (path.includes("/filter-bitmaps/")) return "filterBitmaps";
  if (path.includes("/directory-")) return "directory";
  if (path.includes("/suggest/")) return "suggest";
  return "other";
}

function createFetchMeter({ concurrency = 32, attempts = 3 } = {}) {
  const nativeFetch = globalThis.fetch;
  let active = 0;
  const waiters = [];
  let state;
  const reset = () => {
    state = { requests: 0, bytes: 0, by: {}, shards: new Set() };
  };
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
  reset();
  globalThis.fetch = async (input, init) => {
    await acquire();
    try {
      let response;
      let lastError;
      for (let attempt = 0; attempt < attempts; attempt++) {
        try {
          response = await nativeFetch(input, init);
          if (attempt < attempts - 1 && [429, 500, 502, 503, 504].includes(response.status)) {
            await response.body?.cancel();
            continue;
          }
          break;
        } catch (error) {
          lastError = error;
        }
      }
      if (!response) throw lastError;
      const url = String(input?.url || input);
      const bucket = bucketFromUrl(url);
      const row = state.by[bucket] || (state.by[bucket] = { requests: 0, bytes: 0 });
      const bytes = Number(response.headers.get("content-length") || 0);
      state.requests++;
      row.requests++;
      if (Number.isFinite(bytes) && bytes > 0) {
        state.bytes += bytes;
        row.bytes += bytes;
      }
      const shard = /\/shards\/([^/]+)\//u.exec(url)?.[1];
      if (shard) state.shards.add(shard);
      return response;
    } finally {
      release();
    }
  };
  return {
    reset,
    snapshot: () => ({
      requests: state.requests,
      bytes: state.bytes,
      by: state.by,
      shards: [...state.shards].sort()
    }),
    restore: () => {
      globalThis.fetch = nativeFetch;
    }
  };
}

function resultSummary(response) {
  const rows = response.results || response.suggestions || [];
  return {
    total: Number(response.total ?? rows.length),
    lane: response.stats?.plannerLane || response.stats?.suggestLane || response.stats?.geoLane || null,
    shardsQueried: response.stats?.shardsQueried ?? null,
    items: rows.slice(0, 5).map(item => ({
      text: item.name || item.title || item.text || "",
      type: item.type || item.category || "",
      shard: item.shard || "",
      distanceMeters: Number.isFinite(item.distanceMeters) ? item.distanceMeters : null
    }))
  };
}

async function timed(run) {
  const started = performance.now();
  const response = await run();
  return { ms: performance.now() - started, response };
}

async function runWorker(caseId, base) {
  const definition = CASES.find(item => item.id === caseId);
  if (!definition) throw new Error(`Unknown Maps benchmark case: ${caseId}`);
  const meter = createFetchMeter();
  try {
    const engine = await createSearch({ baseUrl: base });
    const cold = await timed(() => definition.run(engine));
    const coldMeter = meter.snapshot();
    meter.reset();
    const warm = await timed(() => definition.run(engine));
    return {
      id: definition.id,
      family: definition.family,
      cold: { ms: cold.ms, ...coldMeter },
      warm: { ms: warm.ms, ...meter.snapshot() },
      result: resultSummary(cold.response)
    };
  } finally {
    meter.restore();
  }
}

function runChild(definition, args) {
  return new Promise(resolve => {
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url)], {
      env: {
        ...process.env,
        RANGEFIND_MAPS_BENCH_CASE: definition.id,
        RANGEFIND_MAPS_BENCH_BASE: args.base
      },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, args.timeoutMs);
    child.stdout.on("data", chunk => {
      stdout += chunk;
    });
    child.stderr.on("data", chunk => {
      stderr += chunk;
    });
    child.on("close", code => {
      clearTimeout(timer);
      if (timedOut) {
        resolve({ id: definition.id, family: definition.family, error: `timeout after ${args.timeoutMs}ms` });
        return;
      }
      if (code !== 0) {
        resolve({ id: definition.id, family: definition.family, error: stderr.trim() || `worker exited ${code}` });
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch {
        resolve({ id: definition.id, family: definition.family, error: `invalid worker output: ${stdout.slice(0, 500)}` });
      }
    });
  });
}

const workerCase = process.env.RANGEFIND_MAPS_BENCH_CASE;
if (workerCase) {
  const result = await runWorker(workerCase, process.env.RANGEFIND_MAPS_BENCH_BASE || "https://osm.rangefind.dev/");
  process.stdout.write(JSON.stringify(result));
} else {
  const args = parseArgs(process.argv.slice(2));
  const selected = CASES.filter(item => !args.cases || args.cases.has(item.id));
  const report = {
    base: args.base,
    at: new Date().toISOString(),
    runtimeVersion: JSON.parse(await (await import("node:fs/promises")).readFile(
      new URL("../package.json", import.meta.url),
      "utf8"
    )).version,
    cases: []
  };
  for (let index = 0; index < selected.length; index++) {
    const definition = selected[index];
    const result = await runChild(definition, args);
    report.cases.push(result);
    if (result.error) {
      console.log(`[${index + 1}/${selected.length}] ${definition.id}: ERROR ${result.error}`);
    } else {
      console.log(
        `[${index + 1}/${selected.length}] ${definition.id}: `
        + `${Math.round(result.cold.ms)}ms, ${result.cold.requests} req, `
        + `${(result.cold.bytes / 1024).toFixed(0)} KiB, `
        + `${result.cold.shards.length} shard(s), ${result.result.total} result(s)`
      );
    }
  }
  if (args.output) writeFileSync(args.output, JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify(report, null, 2));
}
