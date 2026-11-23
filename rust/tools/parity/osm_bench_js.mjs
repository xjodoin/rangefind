#!/usr/bin/env node
// JS side of the OSM query benchmark (Rust side: `cargo test --release
// --test osm_parity osm_bench -- --ignored --nocapture`). Replays the
// recorded OSM cases of one fixture cold (fresh engine, cleared runtime
// caches) and warm (same engine, `--repeat` more times), the methodology of
// bench_js.mjs / `rangefind-rs bench`.
//   node rust/tools/parity/osm_bench_js.mjs [lux] [--repeat=3] [--wasm]
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { gunzipSync } from "node:zlib";
import * as query from "../../../src/integrations/osm/query.js";
import { createNodeSearch, resetNodeRuntimeCaches } from "../../../src/runtime.node.js";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../target/osm-parity");
const name = process.argv.slice(2).find(arg => !arg.startsWith("--")) || "lux";
const repeat = Number(process.argv.find(arg => arg.startsWith("--repeat="))?.slice(9) || 3);
const useWasm = process.argv.includes("--wasm");
const fixture = JSON.parse(gunzipSync(readFileSync(join(root, "cases", `${name}.json.gz`))));
const base = pathToFileURL(join(root, fixture.index) + "/").href;
const cases = fixture.cases.filter(item => item.warm && !item.fn.startsWith("maps."));
const { createWasmSearch } = useWasm ? await import("../../rangefind/js/rangefind_wasm.mjs") : {};

const cold = [];
const warm = [];
const byFn = {};
let reads = 0;
let bytes = 0;
for (const item of cases) {
  await resetNodeRuntimeCaches();
  const engine = useWasm ? await createWasmSearch({ baseUrl: base }) : await createNodeSearch({ baseUrl: base });
  const run = () => query[item.fn](engine, ...structuredClone(item.args)).catch(() => null);
  let start = performance.now();
  await run();
  const elapsed = performance.now() - start;
  cold.push(elapsed);
  (byFn[item.fn] ||= []).push(elapsed);
  if (!useWasm) {
    const stats = engine.cacheStats?.() || {};
    reads += Number(stats.fileRangeReads || 0) + Number(stats.fileFullReads || 0);
    bytes += Number(stats.fileBytesRead || 0);
  }
  for (let i = 0; i < repeat; i++) {
    start = performance.now();
    await run();
    warm.push(performance.now() - start);
  }
  engine.close?.();
}
const pct = (values, p) => values.slice().sort((a, b) => a - b)[Math.round((values.length - 1) * p)] || 0;
const round = value => Math.round(value * 1000) / 1000;
console.log(JSON.stringify({
  engine: useWasm ? "wasm" : "js",
  fixture: name,
  cases: cases.length,
  cold_ms: { p50: round(pct(cold, 0.5)), p95: round(pct(cold, 0.95)), max: round(pct(cold, 1)), sum: round(cold.reduce((a, b) => a + b, 0)) },
  warm_ms: { p50: round(pct(warm, 0.5)), p95: round(pct(warm, 0.95)), max: round(pct(warm, 1)) },
  ...(useWasm ? {} : { cold_file_reads: reads, cold_bytes_read: bytes }),
  by_function_cold_p50: Object.fromEntries(Object.entries(byFn).map(([fn, values]) => [fn, round(pct(values, 0.5))]))
}, null, 2));
