import { performance } from "node:perf_hooks";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createCodeStore, openCodeStore } from "../src/build_store.js";

const docs = Math.max(10_000, Math.floor(Number(process.argv.find(arg => arg.startsWith("--docs="))?.split("=")[1] || 250_000)));
const reads = Math.max(10_000, Math.floor(Number(process.argv.find(arg => arg.startsWith("--reads="))?.split("=")[1] || 1_000_000)));
const root = mkdtempSync(join(tmpdir(), "rangefind-geo-builder-bench-"));
const config = {
  // One-row chunks isolate positional-read overhead without turning the
  // benchmark itself into the production cache-thrash case (where a random
  // miss can issue thousands of tiny index reads).
  codeStoreCacheDocs: 1,
  codeStoreCacheChunks: 8,
  facets: [{ name: "type" }],
  numbers: [{ name: "location.lat" }, { name: "location.lon" }],
  booleans: []
};
const dicts = { type: { values: [{ value: "" }, { value: "cafe" }, { value: "restaurant" }] } };

function randomOrder(count, modulo) {
  const out = new Uint32Array(count);
  let value = 0x9e3779b9;
  for (let index = 0; index < count; index++) {
    value = Math.imul(value ^ (value >>> 16), 0x21f0aaad) >>> 0;
    out[index] = value % modulo;
  }
  return out;
}

function timed(label, handler) {
  const started = performance.now();
  const checksum = handler();
  const elapsedMs = performance.now() - started;
  return { label, elapsedMs, readsPerSecond: Math.round(reads / (elapsedMs / 1000)), checksum };
}

try {
  const writer = createCodeStore(root, config, docs, dicts);
  for (let doc = 0; doc < docs; doc++) {
    writer.set("type", doc, { codes: [1 + (doc % 2)] });
    writer.set("location.lat", doc, 45 + (doc % 1000) / 10000);
    writer.set("location.lon", doc, -73 - (doc % 1000) / 10000);
  }
  const descriptor = writer.descriptor();
  writer.close();
  const order = randomOrder(reads, docs);

  const cold = openCodeStore(descriptor);
  const fileBacked = timed("file-backed random facet reads", () => {
    let checksum = 0;
    for (const doc of order) checksum += cold.get("type", doc).codes[0] || 0;
    return checksum;
  });
  cold.close();

  const hot = openCodeStore(descriptor);
  const preload = hot.preloadFields(["type"], Number.MAX_SAFE_INTEGER);
  const memoryBacked = timed("field-preloaded random facet reads", () => {
    let checksum = 0;
    for (const doc of order) checksum += hot.get("type", doc).codes[0] || 0;
    return checksum;
  });
  hot.close();

  if (fileBacked.checksum !== memoryBacked.checksum) throw new Error("benchmark paths produced different results");
  console.log(JSON.stringify({
    docs,
    reads,
    selectivePreloadBytes: preload.preloadedBytes,
    fileBacked,
    memoryBacked,
    speedup: Number((fileBacked.elapsedMs / memoryBacked.elapsedMs).toFixed(2))
  }, null, 2));
} finally {
  rmSync(root, { recursive: true, force: true });
}
