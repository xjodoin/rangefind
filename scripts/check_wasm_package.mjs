#!/usr/bin/env node
// Publish guard: the `rangefind/wasm` export must ship a matching glue +
// module pair. `dist/` is gitignored and built by `npm run build:wasm`, so a
// publish from a checkout that skipped the build would otherwise ship an
// export pointing at missing files.
import { readFileSync, statSync } from "node:fs";

const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
const target = pkg.exports?.["./wasm"]?.default;
const glue = new URL(`../${target}`, import.meta.url);
const wasm = new URL("../dist/rangefind_wasm.wasm", import.meta.url);
const source = new URL("../rust/rangefind/js/rangefind_wasm.mjs", import.meta.url);
for (const file of [glue, wasm]) {
  try {
    statSync(file);
  } catch {
    console.error(`missing ${file.pathname}: run npm run build:wasm`);
    process.exit(1);
  }
}
if (readFileSync(glue, "utf8") !== readFileSync(source, "utf8")) {
  console.error("dist/rangefind_wasm.mjs is stale: run npm run build:wasm");
  process.exit(1);
}
const { createWasmSearch, loadRangefindWasm } = await import(glue.href);
await loadRangefindWasm();
if (typeof createWasmSearch !== "function") process.exit(1);
console.log(`wasm package ok (${statSync(wasm).size} bytes)`);
