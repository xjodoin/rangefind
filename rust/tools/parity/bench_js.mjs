#!/usr/bin/env node
// JS counterpart of `rangefind-rs bench`: replays a parity cases file cold
// (fresh engine + cleared caches) and warm, so the native Rust engine, the
// wasm engine and the JS runtime can be compared on identical work. Each
// cold wasm engine owns its file-handle pool and closes it, so wasm pays the
// same open() cost per cold query as the JS runtime after
// resetNodeRuntimeCaches().
//   node rust/tools/parity/bench_js.mjs <index-dir> <cases.json> [--repeat=N] [--wasm]
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { createNodeSearch, resetNodeRuntimeCaches } from "../../../src/runtime.node.js";
import { createWasmSearch } from "../../rangefind/js/rangefind_wasm.mjs";

const [dir, casesPath] = process.argv.slice(2).filter(a => !a.startsWith("--"));
const repeat = Number(process.argv.find(a => a.startsWith("--repeat="))?.slice(9) || 3);
const useWasm = process.argv.includes("--wasm");
const base = pathToFileURL(resolve(dir) + "/").href;
const { cases } = JSON.parse(await readFile(casesPath, "utf8"));
const cold = [];
const warm = [];
for (const item of cases) {
  await resetNodeRuntimeCaches();
  const engine = useWasm ? await createWasmSearch({ baseUrl: base }) : await createNodeSearch({ baseUrl: base });
  let start = performance.now();
  try { await engine[item.method](structuredClone(item.params)); } catch {}
  cold.push(performance.now() - start);
  for (let i = 0; i < repeat; i++) {
    start = performance.now();
    try { await engine[item.method](structuredClone(item.params)); } catch {}
    warm.push(performance.now() - start);
  }
  engine.close?.();
}
const pct = (values, p) => values.slice().sort((a, b) => a - b)[Math.round((values.length - 1) * p)] || 0;
console.log(JSON.stringify({
  engine: useWasm ? "wasm" : "js",
  cases: cases.length,
  cold_ms: { p50: pct(cold, 0.5), p95: pct(cold, 0.95), max: pct(cold, 1), sum: cold.reduce((a, b) => a + b, 0) },
  warm_ms: { p50: pct(warm, 0.5), p95: pct(warm, 0.95), max: pct(warm, 1) }
}, null, 2));
