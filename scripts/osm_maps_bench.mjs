#!/usr/bin/env node

// Production benchmark for common interactive map-search use cases. Each
// case runs in its own Node process so a pathological cold query cannot
// retain caches or heap state that contaminates the next measurement.
//
// Usage:
//   node scripts/osm_maps_bench.mjs
//   node scripts/osm_maps_bench.mjs --profile=full
//   node scripts/osm_maps_bench.mjs --cases=category-near,landmark
//   node scripts/osm_maps_bench.mjs --list
//   node scripts/osm_maps_bench.mjs --out=/tmp/maps-bench.json

import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { createSearch } from "../src/runtime.js";
import { searchOsmQuery, suggestOsmQuery } from "../src/integrations/osm/query.js";
import { evaluateBudgets, evaluateExpectations, summarizeCases } from "./osm_maps_bench_lib.mjs";

const MONTREAL = { lat: 45.5019, lon: -73.5674 };
const LAVAL = { lat: 45.6066, lon: -73.7124 };
const TOKYO = { lat: 35.6812, lon: 139.7671 };
const MONTREAL_BOX = {
  minLat: 45.45,
  maxLat: 45.60,
  minLon: -73.70,
  maxLon: -73.50
};
const LAVAL_BOX = {
  minLat: 45.55,
  maxLat: 45.66,
  minLon: -73.80,
  maxLon: -73.62
};
const OLD_MONTREAL_BOX = {
  minLat: 45.495,
  maxLat: 45.515,
  minLon: -73.565,
  maxLon: -73.535
};

const MIB = 1024 * 1024;
const BUDGETS = {
  autocomplete: { coldMs: 2500, coldRequests: 30, coldBytes: 2 * MIB, warmMs: 100, warmRequests: 0 },
  addressAutocomplete: { coldMs: 2500, coldRequests: 35, coldBytes: 2 * MIB, warmMs: 100, warmRequests: 0 },
  direct: { coldMs: 4000, coldRequests: 60, coldBytes: 4 * MIB, warmMs: 100, warmRequests: 0 },
  discovery: { coldMs: 6000, coldRequests: 100, coldBytes: 5 * MIB, warmMs: 100, warmRequests: 0 },
  recovery: { coldMs: 7000, coldRequests: 130, coldBytes: 6 * MIB, warmMs: 150, warmRequests: 0 }
};

const CASES = [
  {
    id: "suggest-locality",
    family: "autocomplete",
    scenario: "Type a city name",
    weight: 9,
    budget: BUDGETS.autocomplete,
    expect: { minResults: 1, anyTextAny: ["Montréal", "Montreal"] },
    run: engine => suggestOsmQuery(engine, { q: "mont", near: MONTREAL, size: 8 })
  },
  {
    id: "suggest-poi-locality",
    family: "autocomplete",
    scenario: "Type a category and locality",
    weight: 9,
    budget: BUDGETS.autocomplete,
    expect: { minResults: 1, anyTextAny: ["cinema", "cinéma", "Laval"] },
    run: engine => suggestOsmQuery(engine, { q: "cinema lav", near: MONTREAL, size: 8 })
  },
  {
    id: "suggest-address",
    family: "autocomplete",
    scenario: "Type a partial civic address",
    weight: 8,
    budget: BUDGETS.addressAutocomplete,
    expect: { minResults: 1, anyTextAny: ["Sherbrooke"] },
    run: engine => suggestOsmQuery(engine, { q: "845 sher", near: MONTREAL, size: 8 })
  },
  {
    id: "suggest-brand",
    family: "autocomplete",
    scenario: "Type a chain or brand",
    weight: 7,
    common: false,
    budget: BUDGETS.autocomplete,
    expect: { minResults: 1, anyTextAny: ["Tim Horton"] },
    run: engine => suggestOsmQuery(engine, { q: "tim hor", near: LAVAL, size: 8 })
  },
  {
    id: "locality-exact",
    family: "locality",
    scenario: "Open an exact city",
    weight: 8,
    budget: BUDGETS.direct,
    expect: { minResults: 1, topTextAny: ["Laval"], topShard: "quebec", maxShardsQueried: 1 },
    run: engine => searchOsmQuery(engine, { q: "Laval", size: 18 })
  },
  {
    id: "landmark",
    family: "poi",
    scenario: "Find a named landmark near the map",
    weight: 8,
    budget: BUDGETS.direct,
    expect: { minResults: 1, topTextAny: ["McGill University"], topShard: "quebec", firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "McGill University", near: MONTREAL, size: 18 })
  },
  {
    id: "airport",
    family: "poi",
    scenario: "Find an airport by name",
    weight: 5,
    budget: BUDGETS.direct,
    expect: { minResults: 1, anyTextAny: ["Trudeau", "Dorval", "YUL"], allTopShards: ["quebec"], checkTop: 5 },
    run: engine => searchOsmQuery(engine, { q: "Montréal Trudeau Airport", near: MONTREAL, size: 18 })
  },
  {
    id: "transit-station",
    family: "poi",
    scenario: "Find a transit station",
    weight: 6,
    common: false,
    budget: BUDGETS.direct,
    expect: { minResults: 1, topTextAny: ["Berri-UQAM", "Berri UQAM"], topShard: "quebec", firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "Berri-UQAM", near: MONTREAL, size: 18 })
  },
  {
    id: "category-locality",
    family: "category",
    scenario: "Find a place type in a city",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 3,
      topTypes: ["cinema"],
      allTopShards: ["quebec"],
      distanceAscending: true,
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "cinema laval", size: 18 })
  },
  {
    id: "category-locality-common",
    family: "category",
    scenario: "Find a dense place type in a city",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: { minResults: 10, allTopShards: ["quebec"], distanceAscending: true, maxShardsQueried: 1 },
    run: engine => searchOsmQuery(engine, { q: "restaurant montreal", size: 18 })
  },
  {
    id: "category-locality-french",
    family: "category",
    scenario: "Use the local language for category search",
    weight: 7,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, topTypes: ["cinema"], allTopShards: ["quebec"], distanceAscending: true },
    run: engine => searchOsmQuery(engine, { q: "cinéma laval", size: 18 })
  },
  {
    id: "category-near-sparse",
    family: "near-me",
    scenario: "Find a sparse utility near me",
    weight: 8,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, allTopShards: ["quebec"], distanceAscending: true, firstDistanceMax: 10000 },
    run: engine => searchOsmQuery(engine, { q: "gas station near me", near: MONTREAL, size: 18 })
  },
  {
    id: "category-near-dense",
    family: "near-me",
    scenario: "Find a dense category around the current location",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: { minResults: 10, allTopShards: ["quebec"], distanceAscending: true, firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "restaurant", near: MONTREAL, size: 18 })
  },
  {
    id: "pharmacy-near",
    family: "near-me",
    scenario: "Find an essential service near me",
    weight: 6,
    common: false,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, allTopShards: ["quebec"], distanceAscending: true, firstDistanceMax: 10000 },
    run: engine => searchOsmQuery(engine, { q: "pharmacy", near: MONTREAL, size: 18 })
  },
  {
    id: "parking-near",
    family: "near-me",
    scenario: "Find parking near the destination",
    weight: 6,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, allTopShards: ["quebec"], distanceAscending: true, firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "parking", near: MONTREAL, size: 18 })
  },
  {
    id: "brand-near",
    family: "poi",
    scenario: "Find the nearest branch of a chain",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 5,
      topTextAny: ["Tim Hortons"],
      allTopShards: ["quebec"],
      firstDistanceMax: 1000,
      distanceAscending: true,
      lanes: ["osmNearExactGeo"]
    },
    run: engine => searchOsmQuery(engine, { q: "Tim Hortons", near: LAVAL, size: 18 })
  },
  {
    id: "brand-near-variant",
    family: "poi",
    scenario: "Resolve a singular or incomplete brand variant",
    weight: 5,
    common: false,
    budget: BUDGETS.recovery,
    expect: {
      minResults: 5,
      topTextAny: ["Tim Hortons"],
      allTopShards: ["quebec"],
      firstDistanceMax: 1000,
      distanceAscending: true,
      lanes: ["osmNearExactGeo"]
    },
    run: engine => searchOsmQuery(engine, { q: "Tim Horton", near: LAVAL, size: 18 })
  },
  {
    id: "civic-address",
    family: "address",
    scenario: "Find a complete civic address",
    weight: 10,
    budget: BUDGETS.direct,
    expect: { minResults: 1, anyTextAny: ["845", "Sherbrooke"], allTopShards: ["quebec"], checkTop: 5 },
    run: engine => searchOsmQuery(engine, { q: "845 rue Sherbrooke Ouest Montréal", size: 18 })
  },
  {
    id: "street-locality",
    family: "address",
    scenario: "Find a street within a locality",
    weight: 7,
    budget: BUDGETS.direct,
    expect: { minResults: 1, topTextAny: ["Saint-Denis", "Saint Denis"], topShard: "quebec" },
    run: engine => searchOsmQuery(engine, { q: "rue saint denis montreal", size: 18 })
  },
  {
    id: "postal-code",
    family: "address",
    scenario: "Find an area by postal code",
    weight: 5,
    common: false,
    budget: BUDGETS.direct,
    expect: { minResults: 1, anyTextAny: ["H2X 1Y4", "H2X"], allTopShards: ["quebec"], checkTop: 5 },
    run: engine => searchOsmQuery(engine, { q: "H2X 1Y4", size: 18 })
  },
  {
    id: "intersection",
    family: "address",
    scenario: "Find an intersection",
    weight: 6,
    budget: BUDGETS.recovery,
    expect: { minResults: 1, anyTextAny: ["Saint-Laurent", "Sainte-Catherine"], allTopShards: ["quebec"], checkTop: 8 },
    run: engine => searchOsmQuery(engine, {
      q: "boulevard Saint-Laurent and rue Sainte-Catherine Montréal",
      size: 18
    })
  },
  {
    id: "viewport-browse",
    family: "viewport",
    scenario: "Browse visible places after moving the map",
    weight: 4,
    common: false,
    budget: BUDGETS.discovery,
    expect: { minResults: 10, viewportBox: MONTREAL_BOX, maxShardsQueried: 1 },
    run: engine => engine.search({ q: "", geo: { box: MONTREAL_BOX }, size: 18 })
  },
  {
    id: "viewport-category",
    family: "viewport",
    scenario: "Search this area for a category",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 10,
      viewportBox: MONTREAL_BOX,
      allTopShards: ["quebec"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, {
      q: "restaurant",
      geo: { box: MONTREAL_BOX },
      size: 18
    })
  },
  {
    id: "viewport-brand",
    family: "viewport",
    scenario: "Search this area for a chain",
    weight: 9,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 5,
      viewportBox: LAVAL_BOX,
      topTextAny: ["Tim Hortons"],
      allTopShards: ["quebec"],
      distanceAscending: true,
      lanes: ["osmViewportExactGeo"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, {
      q: "Tim Horton",
      geo: { box: LAVAL_BOX },
      size: 18
    })
  },
  {
    id: "viewport-pan-category",
    family: "viewport",
    scenario: "Repeat search after panning into a smaller area",
    weight: 7,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, viewportBox: OLD_MONTREAL_BOX, allTopShards: ["quebec"], maxShardsQueried: 1 },
    run: engine => searchOsmQuery(engine, {
      q: "cafe",
      geo: { box: OLD_MONTREAL_BOX },
      size: 18
    })
  },
  {
    id: "typo-landmark",
    family: "typo",
    scenario: "Recover a typo in a named place",
    weight: 5,
    budget: BUDGETS.recovery,
    expect: { minResults: 1, topTextAny: ["McGill University"], topShard: "quebec", firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "McGil University", near: MONTREAL, size: 18 })
  },
  {
    id: "typo-category-locality",
    family: "typo",
    scenario: "Recover a typo in category plus locality",
    weight: 4,
    common: false,
    budget: BUDGETS.recovery,
    expect: { minResults: 3, allTopShards: ["quebec"], distanceAscending: true },
    run: engine => searchOsmQuery(engine, { q: "cinma laval", size: 18 })
  },
  {
    id: "unicode-landmark",
    family: "international",
    scenario: "Find a landmark in its native script",
    weight: 5,
    common: false,
    budget: BUDGETS.discovery,
    expect: { minResults: 1, topTextAny: ["東京駅"], topShard: "japan", firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "東京駅", near: TOKYO, size: 18 })
  },
  {
    id: "coordinates",
    family: "international",
    scenario: "Open decimal latitude and longitude",
    weight: 2,
    common: false,
    budget: BUDGETS.direct,
    expect: { minResults: 1, firstDistanceMax: 1000 },
    run: engine => searchOsmQuery(engine, { q: "45.5019, -73.5674", size: 18 })
  }
];

function parseArgs(argv) {
  const out = {
    base: "https://osm.rangefind.dev/",
    cases: null,
    output: "",
    profile: "common",
    timeoutMs: 90_000,
    list: false,
    strict: false,
    summaryOnly: false
  };
  for (const arg of argv) {
    if (arg.startsWith("--base=")) out.base = arg.slice("--base=".length);
    else if (arg.startsWith("--cases=")) out.cases = new Set(arg.slice("--cases=".length).split(",").filter(Boolean));
    else if (arg.startsWith("--out=")) out.output = arg.slice("--out=".length);
    else if (arg.startsWith("--profile=")) out.profile = arg.slice("--profile=".length);
    else if (arg.startsWith("--timeout-ms=")) out.timeoutMs = Math.max(1000, Number(arg.slice("--timeout-ms=".length)) || out.timeoutMs);
    else if (arg === "--list") out.list = true;
    else if (arg === "--strict") out.strict = true;
    else if (arg === "--summary-only") out.summaryOnly = true;
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
    const coldRun = { ms: cold.ms, ...coldMeter };
    const warmRun = { ms: warm.ms, ...meter.snapshot() };
    return {
      id: definition.id,
      family: definition.family,
      scenario: definition.scenario,
      weight: definition.weight,
      cold: coldRun,
      warm: warmRun,
      quality: evaluateExpectations(cold.response, definition.expect),
      budget: evaluateBudgets(coldRun, warmRun, definition.budget),
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
  const knownProfiles = new Set(["common", "full", ...CASES.map(item => item.family)]);
  if (!knownProfiles.has(args.profile)) {
    throw new Error(`Unknown Maps benchmark profile ${args.profile}; expected common, full, or a case family.`);
  }
  const selected = CASES.filter(item => {
    if (args.cases) return args.cases.has(item.id);
    if (args.profile === "full") return true;
    if (args.profile === "common") return item.common !== false;
    return item.family === args.profile;
  });
  if (args.list) {
    console.log(JSON.stringify(CASES.map(item => ({
      id: item.id,
      family: item.family,
      scenario: item.scenario,
      weight: item.weight,
      common: item.common !== false
    })), null, 2));
    process.exit(0);
  }
  const report = {
    base: args.base,
    at: new Date().toISOString(),
    profile: args.cases ? "custom" : args.profile,
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
        + `${result.cold.shards.length} shard(s), ${result.result.total} result(s), `
        + `quality=${result.quality.passed ? "pass" : "FAIL"}, budget=${result.budget.passed ? "pass" : "MISS"}`
      );
    }
  }
  report.summary = summarizeCases(report.cases);
  if (args.output) writeFileSync(args.output, JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify(args.summaryOnly ? report.summary : report, null, 2));
  if (args.strict && report.summary.failures.length) process.exitCode = 1;
}
