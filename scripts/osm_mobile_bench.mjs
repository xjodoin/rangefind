#!/usr/bin/env node

// Mobile-browser benchmark: runs the real browser bundles (dist/runtime.browser.js
// + dist/osm.browser.js) in headless Chromium with CPU and network throttling
// against a live sharded deployment, and reports cold/warm latency, request
// count, and transfer per lane — the numbers a phone user actually feels.
//
//   node scripts/osm_mobile_bench.mjs
//   node scripts/osm_mobile_bench.mjs --network=3g --cpu=6 --runs=3
//   node scripts/osm_mobile_bench.mjs --lanes=suggest,locality --out=mobile.json
//   node scripts/osm_mobile_bench.mjs --network=none --cpu=1     # desktop reference
//
// Profiles (CDP Network.emulateNetworkConditions):
//   4g (default)  RTT 100ms, 9 Mbps down / 1.5 Mbps up   — mid-range phone on LTE
//   3g            RTT 300ms, 1.6 Mbps down / 750 Kbps up — slow connection
//   none          no throttling
// CPU (CDP Emulation.setCPUThrottlingRate): --cpu=4 by default, matching a
// mid-range Android phone relative to a desktop core.
//
// Requires bench/competitive to be installed once:
//   cd bench/competitive && npm install && npx playwright install chromium
// Rebuild bundles after runtime changes: npm run build:browser

import { createServer } from "node:http";
import { readFile, writeFile } from "node:fs/promises";
import { join, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, "..");
const require = createRequire(join(ROOT, "bench", "competitive", "package.json"));
const { chromium } = require("playwright");

function parseArgs(argv) {
  const args = {
    base: "https://osm.rangefind.dev/",
    runs: 3,
    network: "4g",
    cpu: 4,
    lanes: null,
    out: ""
  };
  for (const arg of argv) {
    if (arg.startsWith("--base=")) args.base = arg.slice("--base=".length);
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--network=")) args.network = arg.slice("--network=".length);
    else if (arg.startsWith("--cpu=")) args.cpu = Number(arg.slice("--cpu=".length)) || args.cpu;
    else if (arg.startsWith("--lanes=")) args.lanes = arg.slice("--lanes=".length).split(",");
    else if (arg.startsWith("--out=")) args.out = arg.slice("--out=".length);
  }
  return args;
}

const NETWORK_PROFILES = {
  "4g": { latency: 100, downloadThroughput: 9 * 1024 * 1024 / 8, uploadThroughput: 1.5 * 1024 * 1024 / 8 },
  "3g": { latency: 300, downloadThroughput: 1.6 * 1024 * 1024 / 8, uploadThroughput: 750 * 1024 / 8 },
  none: null
};

// The lanes a phone user actually drives from the demo: suggest keystrokes,
// locality/category/street intents, and map interactions. Each lane body runs
// inside the page with `engine`, `osm` (integration module), and `params`.
const LANES = [
  { name: "suggest", body: "return osm.suggestOsmQuery(engine, { q: 'berl', size: 8 })" },
  { name: "suggest-address", body: "return osm.suggestOsmQuery(engine, { q: '101 9 av', size: 8 })" },
  { name: "locality", body: "return osm.searchOsmQuery(engine, { q: 'Berlin', size: 10 })" },
  { name: "locality-common-name", body: "return osm.searchOsmQuery(engine, { q: 'laval', size: 10 })" },
  { name: "category-locality", body: "return osm.searchOsmQuery(engine, { q: 'pharmacy in Birmingham', size: 10 })" },
  { name: "street-locality", body: "return osm.searchOsmQuery(engine, { q: 'rue saint denis, montreal', size: 10 })" },
  { name: "civic-address", body: "return osm.searchOsmQuery(engine, { q: '101 9 avenue sw, calgary', size: 10 })" },
  { name: "text-unique", body: "return engine.search({ q: 'calgary tower', size: 10 })" },
  { name: "nearest", body: "return engine.search({ q: '', geo: { near: { lat: 51.0447, lon: -114.0719 }, sort: 'distance' }, size: 10 })" },
  { name: "area-box-text", body: "return engine.search({ q: 'cafe', geo: { box: { minLat: 52.49, maxLat: 52.55, minLon: 13.35, maxLon: 13.46 } }, size: 10 })" }
];

const PAGE_HTML = `<!doctype html><html><head><meta charset="utf-8"></head><body>ready</body></html>`;

async function serveBundles() {
  const runtime = await readFile(join(ROOT, "dist", "runtime.browser.js"));
  const osm = await readFile(join(ROOT, "dist", "osm.browser.js"));
  const server = createServer((request, response) => {
    const path = new URL(request.url, "http://x").pathname;
    const headers = { "Cache-Control": "no-store" };
    if (path === "/") return response.writeHead(200, { ...headers, "Content-Type": "text/html" }).end(PAGE_HTML);
    if (path === "/runtime.browser.js") return response.writeHead(200, { ...headers, "Content-Type": "text/javascript" }).end(runtime);
    if (path === "/osm.browser.js") return response.writeHead(200, { ...headers, "Content-Type": "text/javascript" }).end(osm);
    response.writeHead(404).end();
  });
  await new Promise(resolveListen => server.listen(0, "127.0.0.1", resolveListen));
  return {
    url: `http://127.0.0.1:${server.address().port}/`,
    close: () => new Promise(resolveClose => server.close(resolveClose))
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const profile = NETWORK_PROFILES[args.network];
  if (profile === undefined) throw new Error(`Unknown --network=${args.network} (use ${Object.keys(NETWORK_PROFILES).join(", ")})`);
  const lanes = LANES.filter(lane => !args.lanes || args.lanes.includes(lane.name));

  const bundles = await serveBundles();
  const browser = await chromium.launch();
  const report = {
    base: args.base,
    network: args.network,
    cpu: args.cpu,
    runs: args.runs,
    at: new Date().toISOString(),
    lanes: {}
  };
  console.log(`Mobile bench vs ${args.base} — network=${args.network}, cpu=${args.cpu}x, ${lanes.length} lanes, ${args.runs} warm runs`);

  try {
    for (const lane of lanes) {
      // Fresh context per lane: cold browser cache, cold engine.
      const context = await browser.newContext({
        viewport: { width: 390, height: 844 },
        userAgent: "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
      });
      const page = await context.newPage();
      const session = await context.newCDPSession(page);
      await session.send("Emulation.setCPUThrottlingRate", { rate: args.cpu });
      if (profile) await session.send("Network.emulateNetworkConditions", { offline: false, ...profile });

      let requests = 0;
      let bytes = 0;
      page.on("response", response => {
        const url = response.url();
        if (!url.startsWith(args.base)) return;
        requests++;
        bytes += Number(response.headers()["content-length"] || 0);
      });

      await page.goto(bundles.url);

      const result = await page.evaluate(async ({ bundleBase, base, body, runs }) => {
        const runtime = await import(`${bundleBase}runtime.browser.js`);
        const osm = await import(`${bundleBase}osm.browser.js`);
        const engine = await runtime.createSearch({ baseUrl: base });
        const run = new Function("engine", "osm", body);
        const time = async () => {
          const started = performance.now();
          const response = await run(engine, osm);
          return { ms: performance.now() - started, response };
        };
        const cold = await time();
        const warm = [];
        let last = cold.response;
        for (let i = 0; i < runs; i++) {
          const item = await time();
          warm.push(item.ms);
          last = item.response;
        }
        const items = last.results || last.suggestions || [];
        return {
          coldMs: Math.round(cold.ms),
          warmMs: warm.map(Math.round),
          total: last.total ?? items.length,
          heapMB: performance.memory ? Math.round(performance.memory.usedJSHeapSize / 1048576) : null,
          topIds: items.slice(0, 3).map(item => item.id || item.text || item.name)
        };
      }, { bundleBase: bundles.url, base: args.base, body: lane.body, runs: args.runs });

      const warmMean = Math.round(result.warmMs.reduce((sum, value) => sum + value, 0) / Math.max(1, result.warmMs.length));
      report.lanes[lane.name] = {
        coldMs: result.coldMs,
        warmMeanMs: warmMean,
        coldRequests: requests,
        coldKb: Math.round(bytes / 1024),
        total: result.total,
        heapMB: result.heapMB,
        topIds: result.topIds
      };
      console.log(`  ${lane.name}: cold ${result.coldMs}ms / ${requests} req / ${Math.round(bytes / 1024)}KB — warm ${warmMean}ms — heap ${result.heapMB}MB`);
      await context.close();
    }
  } finally {
    await browser.close();
    await bundles.close();
  }

  if (args.out) {
    await writeFile(args.out, JSON.stringify(report, null, 2));
    console.log(`Report written to ${args.out}`);
  }
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
