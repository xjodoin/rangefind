#!/usr/bin/env node
// Replays the recorded OSM query-layer cases (rust/target/osm-parity, built
// by rust/tools/parity/osm_fixtures.mjs) through the wasm engine's `osm.*`
// methods and compares with the JS outputs. Every key except `stats` must
// match (stats carry cache-dependent fetch counters).
//   node rust/tools/wasm/osm_parity.mjs [--only=lux,luxsharded] [--limit=N]
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { gunzipSync } from "node:zlib";
import { createWasmSearch } from "../../rangefind/js/rangefind_wasm.mjs";
import { firstDifference } from "./compare.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, "../../target/osm-parity/cases");
const only = process.argv.find(arg => arg.startsWith("--only="))?.slice(7).split(",") || ["lux", "luxsharded", "testville", "twoshards"];
const limit = Number(process.argv.find(arg => arg.startsWith("--limit="))?.slice(8) || Infinity);

function stripStats(value) {
  if (Array.isArray(value)) return value.map(stripStats);
  if (!value || typeof value !== "object") return value;
  const out = {};
  for (const [key, item] of Object.entries(value)) if (key !== "stats") out[key] = stripStats(item);
  return out;
}

function invoke(engine, fn, args) {
  const [first, second] = args;
  switch (fn) {
    case "searchOsmQuery": return engine.osm.search(first);
    case "suggestOsmQuery": return engine.osm.suggest(first);
    case "reverseGeocodeOsm": return engine.osm.reverseGeocode(first);
    case "searchAlongRouteOsm": return engine.osm.searchAlongRoute(first);
    case "resolveOsmSuggestion": return engine.osm.resolveSuggestion(first);
    case "hydrateOsmSuggestions": return engine.osm.hydrateSuggestions(first);
    case "collapseStreetSuggestions": return engine.osm.collapseStreetSuggestions(first, second);
    // The recorded JS calls use the default lexicon argument.
    case "parseOsmQueryIntent": return engine.osm.parseQueryIntent(first, second ? {} : { lexicon: "default" });
    case "parseNearbyCategoryIntent": return engine.osm.parseNearbyCategoryIntent(first, second ? {} : { lexicon: "default" });
    case "parseCoordinateIntent": return engine.osm.parseCoordinateIntent(first);
    default:
      if (fn.startsWith("maps.")) return engine.osm.maps[fn.slice(5)](first);
      throw new Error(`unknown fn ${fn}`);
  }
}

let total = 0;
const failures = [];
const started = performance.now();
for (const name of only) {
  let fixture;
  try {
    fixture = JSON.parse(gunzipSync(readFileSync(join(root, `${name}.json.gz`))).toString("utf8"));
  } catch {
    console.error(`skipping ${name}: no recorded cases`);
    continue;
  }
  const engine = await createWasmSearch({ baseUrl: pathToFileURL(resolve(root, "..", fixture.index) + "/").href });
  for (const item of fixture.cases.slice(0, limit)) {
    const expected = item.cold;
    if (!expected) continue;
    total++;
    let actual = null;
    let error = null;
    try {
      actual = await invoke(engine, item.fn, structuredClone(item.args));
    } catch (caught) {
      error = caught;
    }
    let diff;
    if (expected.error) {
      const message = expected.error.message ?? expected.error;
      diff = !error ? `js threw ${JSON.stringify(message)}, wasm answered` : error.message === message ? null : `error js=${message} wasm=${error.message}`;
    } else if (error) {
      diff = `wasm threw ${error.message}`;
    } else {
      diff = firstDifference("output", stripStats(expected.output ?? null), stripStats(actual ?? null));
    }
    if (diff) failures.push(`${name}#${item.id} ${item.fn}(${JSON.stringify(item.args).slice(0, 100)}): ${diff}`);
  }
  engine.close();
}
const ms = Math.round(performance.now() - started);
if (process.env.RANGEFIND_PARITY_DUMP) writeFileSync(process.env.RANGEFIND_PARITY_DUMP, failures.join("\n"));
if (failures.length) {
  console.error(failures.slice(0, 30).join("\n"));
  console.error(`wasm/osm: ${failures.length} of ${total} cases failed (${ms} ms)`);
  process.exit(1);
}
console.log(`wasm/osm: ${total} OSM cases matched (${ms} ms)`);
