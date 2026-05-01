#!/usr/bin/env node

import { performance } from "node:perf_hooks";
import { resolve } from "node:path";
import { createSearch } from "../src/runtime.js";
import {
  createFetchMeter,
  dirStats,
  kb,
  mean,
  parseArgs,
  quantile,
  serveStatic
} from "./bench_support.mjs";

const args = parseArgs(process.argv.slice(2), {
  root: "examples/basic/public",
  basePath: "rangefind/",
  runs: 5,
  size: 10,
  queries: ["", "range static search", "statik range search", "pagefind comparison", "facet numeric filters"],
  json: false
});

const server = await serveStatic(args.root);
const meter = createFetchMeter(/\/rangefind\//u);

try {
  meter.reset();
  const initStart = performance.now();
  const engine = await createSearch({ baseUrl: new URL(args.basePath, server.url) });
  const initMs = performance.now() - initStart;
  const initNetwork = meter.snapshot();
  const indexStats = dirStats(resolve(args.root, args.basePath));

  const rows = [];
  for (const q of args.queries) {
    const times = [];
    const requests = [];
    const bytes = [];
    let total = 0;
    let correctedQuery = null;
    let firstTitle = "";
    for (let i = 0; i < args.runs; i++) {
      meter.reset();
      const start = performance.now();
      const response = await engine.search({ q, size: args.size });
      times.push(performance.now() - start);
      const network = meter.snapshot();
      requests.push(network.requests);
      bytes.push(network.bytes);
      total = response.total;
      correctedQuery = response.correctedQuery || null;
      firstTitle = response.results[0]?.title || "";
    }
    rows.push({
      q,
      total,
      firstTitle,
      correctedQuery,
      p50Ms: quantile(times, 0.5),
      p95Ms: quantile(times, 0.95),
      avgRequests: mean(requests),
      avgKb: kb(mean(bytes))
    });
  }

  const report = {
    engine: "rangefind",
    index: indexStats,
    init: { ms: initMs, requests: initNetwork.requests, kb: kb(initNetwork.bytes) },
    rows
  };

  if (args.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log("# Rangefind performance benchmark\n");
    console.log(`Index: ${indexStats.files} files, ${(indexStats.bytes / 1024 / 1024).toFixed(2)} MB`);
    console.log(`Init: ${initMs.toFixed(1)} ms, ${initNetwork.requests} requests, ${kb(initNetwork.bytes).toFixed(1)} KB\n`);
    console.log("| Query | Total | P50 ms | P95 ms | Avg req | Avg KB | Corrected | Top result |");
    console.log("| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |");
    for (const row of rows) {
      console.log(`| ${row.q || "(empty)"} | ${row.total} | ${row.p50Ms.toFixed(1)} | ${row.p95Ms.toFixed(1)} | ${row.avgRequests.toFixed(1)} | ${row.avgKb.toFixed(1)} | ${row.correctedQuery || ""} | ${row.firstTitle} |`);
    }
  }
} finally {
  meter.restore();
  await server.close();
}
