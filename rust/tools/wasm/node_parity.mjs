#!/usr/bin/env node
// Replays every recorded JS parity case through the wasm engine in Node
// (file:// transport) and requires identical answers.
//   node rust/tools/wasm/node_parity.mjs [--only=basic,geo]
import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createWasmSearch } from "../../rangefind/js/rangefind_wasm.mjs";
import { compareCase } from "./compare.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../target/parity");
const only = process.argv.find(arg => arg.startsWith("--only="))?.slice(7).split(",");
const summary = JSON.parse(await readFile(join(root, "summary.json"), "utf8"));
let cases = 0;
const failures = [];
const started = performance.now();
for (const { index } of summary) {
  if (only && !only.includes(index)) continue;
  const fixture = JSON.parse(await readFile(join(root, `${index}.cases.json`), "utf8"));
  const engine = await createWasmSearch({ baseUrl: pathToFileURL(join(root, index, "index") + "/").href });
  for (const [ordinal, item] of fixture.cases.entries()) {
    cases++;
    let actual = null;
    let error = null;
    try {
      actual = item.method === "authorityLookup" ? await engine.authorityLookup(item.params.surface, item.params) : item.method === "hydrateRows" ? await engine.hydrateRows(item.params.rows, item.params.context) : await engine[item.method](structuredClone(item.params));
    } catch (caught) {
      error = caught;
    }
    const diff = compareCase(item, actual, error);
    if (diff) failures.push(`${index}#${ordinal} ${item.method}(${JSON.stringify(item.params).slice(0, 120)}): ${diff}`);
  }
}
const ms = Math.round(performance.now() - started);
if (failures.length) {
  if (process.env.RANGEFIND_PARITY_DUMP) (await import("node:fs")).writeFileSync(process.env.RANGEFIND_PARITY_DUMP, failures.join("\n"));
  console.error(failures.slice(0, 40).join("\n"));
  console.error(`wasm/node: ${failures.length} of ${cases} cases failed (${ms} ms)`);
  process.exit(1);
}
console.log(`wasm/node: ${cases} parity cases matched (${ms} ms)`);
