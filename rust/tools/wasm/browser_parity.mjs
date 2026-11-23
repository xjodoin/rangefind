#!/usr/bin/env node
// Runs the wasm engine inside headless Chromium (Playwright) against the
// parity indexes served over HTTP with byte-range support, replaying the
// recorded JS cases in the page and comparing there.
//   node rust/tools/wasm/browser_parity.mjs [--only=basic] [--browser=chromium|firefox|webkit]
import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "../../..");
const parityRoot = resolve(repo, "rust/target/parity");
const args = Object.fromEntries(process.argv.slice(2).map(a => a.replace(/^--/, "").split("=")));
const only = args.only ? args.only.split(",") : null;
const browserName = args.browser || "chromium";

function loadPlaywright() {
  for (const base of [repo, join(repo, "bench/competitive")]) {
    try {
      return createRequire(join(base, "package.json"))("playwright");
    } catch {}
  }
  throw new Error("playwright not found: cd bench/competitive && npm install && npx playwright install chromium");
}

const TYPES = { ".mjs": "text/javascript", ".js": "text/javascript", ".wasm": "application/wasm", ".json": "application/json", ".html": "text/html" };
const mounts = {
  "/glue/": resolve(repo, "rust/rangefind/js"),
  "/tools/": here,
  "/parity/": parityRoot
};

const server = createServer(async (request, response) => {
  const url = new URL(request.url, "http://localhost");
  if (url.pathname === "/") {
    response.writeHead(200, { "Content-Type": "text/html" });
    response.end("<!doctype html><meta charset=utf-8><title>rangefind wasm</title>");
    return;
  }
  const mount = Object.keys(mounts).find(prefix => url.pathname.startsWith(prefix));
  if (!mount) return response.writeHead(404).end();
  const file = resolve(mounts[mount], `.${decodeURIComponent(url.pathname.slice(mount.length - 1))}`);
  if (!file.startsWith(mounts[mount])) return response.writeHead(403).end();
  try {
    const info = await stat(file);
    const range = /^bytes=(\d+)-(\d+)$/u.exec(request.headers.range || "");
    const type = TYPES[extname(file)] || "application/octet-stream";
    const data = await readFile(file);
    if (range) {
      const start = Number(range[1]);
      const end = Math.min(Number(range[2]), info.size - 1);
      response.writeHead(206, { "Content-Type": type, "Content-Range": `bytes ${start}-${end}/${info.size}`, "Content-Length": end - start + 1, "Accept-Ranges": "bytes" });
      response.end(data.subarray(start, end + 1));
    } else {
      response.writeHead(200, { "Content-Type": type, "Content-Length": info.size });
      response.end(data);
    }
  } catch {
    response.writeHead(404).end();
  }
});
await new Promise(done => server.listen(0, "127.0.0.1", done));
const origin = `http://127.0.0.1:${server.address().port}`;

const playwright = loadPlaywright();
const browser = await playwright[browserName].launch();
let exitCode = 0;
try {
  const page = await browser.newPage();
  page.on("pageerror", error => console.error("page error:", error.message));
  await page.goto(`${origin}/`);
  const summary = JSON.parse(await readFile(join(parityRoot, "summary.json"), "utf8"));
  let total = 0;
  let failed = 0;
  const started = Date.now();
  for (const { index } of summary) {
    if (only && !only.includes(index)) continue;
    const result = await page.evaluate(async ({ index }) => {
      const { createWasmSearch } = await import("/glue/rangefind_wasm.mjs");
      const { compareCase } = await import("/tools/compare.mjs");
      const fixture = await (await fetch(`/parity/${index}.cases.json`)).json();
      const engine = await createWasmSearch({ baseUrl: `/parity/${index}/index/` });
      const failures = [];
      for (const [ordinal, item] of fixture.cases.entries()) {
        let actual = null;
        let error = null;
        try {
          actual = item.method === "authorityLookup" ? await engine.authorityLookup(item.params.surface, item.params) : item.method === "hydrateRows" ? await engine.hydrateRows(item.params.rows, item.params.context) : await engine[item.method](structuredClone(item.params));
        } catch (caught) {
          error = caught;
        }
        const diff = compareCase(item, actual, error);
        if (diff) failures.push(`${index}#${ordinal} ${item.method}: ${diff}`);
      }
      return { cases: fixture.cases.length, failures };
    }, { index });
    total += result.cases;
    failed += result.failures.length;
    if (result.failures.length) console.error(result.failures.slice(0, 20).join("\n"));
  }
  const ms = Date.now() - started;
  if (failed) {
    console.error(`wasm/${browserName}: ${failed} of ${total} cases failed (${ms} ms)`);
    exitCode = 1;
  } else {
    console.log(`wasm/${browserName}: ${total} parity cases matched (${ms} ms)`);
  }
} finally {
  await browser.close();
  server.close();
}
process.exit(exitCode);
