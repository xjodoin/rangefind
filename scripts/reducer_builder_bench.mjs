import { performance } from "node:perf_hooks";
import { mkdtempSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createCodeStore, openCodeStore } from "../src/build_store.js";

const docs = Math.max(10_000, Math.floor(Number(process.argv.find(arg => arg.startsWith("--docs="))?.split("=")[1] || 100_000)));
const rows = Math.max(10_000, Math.floor(Number(process.argv.find(arg => arg.startsWith("--rows="))?.split("=")[1] || 500_000)));
const facetNames = [
  "category", "type", "brand", "cuisine", "wheelchair", "toilets_wheelchair",
  "internet_access", "outdoor_seating", "takeaway", "delivery", "drive_through",
  "reservation", "payment_cash", "payment_cards", "payment_contactless", "fee", "access"
];
const numberNames = ["population", "prominence", "location.lat", "location.lon"];
const hotFields = ["category", "type", ...numberNames];
const root = mkdtempSync(join(tmpdir(), "rangefind-reducer-builder-bench-"));
const config = {
  codeStoreCacheDocs: 16_384,
  codeStoreCacheChunks: 8,
  codeStoreWriteBufferBytes: 1024 * 1024,
  facets: facetNames.map(name => ({ name })),
  numbers: numberNames.map(name => ({ name, type: "double" })),
  booleans: []
};
const dicts = Object.fromEntries(facetNames.map(name => [name, {
  values: [{ value: "" }, { value: `${name}-1` }, { value: `${name}-2` }]
}]));

function byteSize(descriptor) {
  return descriptor.fields.reduce((sum, field) => sum
    + statSync(field.path).size
    + (field.indexPath ? statSync(field.indexPath).size : 0), 0);
}

function timed(label, handler) {
  const started = performance.now();
  const checksum = handler();
  return { label, elapsedMs: performance.now() - started, checksum };
}

try {
  const buildStarted = performance.now();
  const writer = createCodeStore(root, config, docs, dicts);
  for (let doc = 0; doc < docs; doc++) {
    writer.set("category", doc, { codes: [1 + (doc % 2)] });
    writer.set("type", doc, { codes: [1 + (doc % 2)] });
    for (let index = 2; index < facetNames.length; index++) {
      writer.set(facetNames[index], doc, { codes: doc % (index * 13) === 0 ? [1] : [] });
    }
    writer.set("population", doc, doc % 1000 === 0 ? doc * 10 : null);
    writer.set("prominence", doc, (doc % 1000) / 1000);
    writer.set("location.lat", doc, -80 + (doc % 160_000) / 1000);
    writer.set("location.lon", doc, -180 + (doc % 360_000) / 1000);
  }
  const descriptor = writer.descriptor();
  writer.close();
  const buildMs = performance.now() - buildStarted;
  const actualBytes = byteSize(descriptor);
  const legacyFacetIndexBytes = docs * facetNames.length * 16;
  const compactFacetIndexBytes = docs * facetNames.length * 4;

  const reader = openCodeStore(descriptor);
  const preload = reader.preloadFields([...facetNames, ...numberNames], Number.MAX_SAFE_INTEGER);
  const run = fields => timed(`${fields.length} summary fields`, () => {
    let checksum = 0;
    let value = 0x9e3779b9;
    for (let row = 0; row < rows; row++) {
      value = Math.imul(value ^ (value >>> 16), 0x21f0aaad) >>> 0;
      const doc = value % docs;
      for (const name of fields) {
        const item = reader.get(name, doc);
        checksum += item?.codes?.[0] || (Number.isFinite(item) ? item : 0);
      }
    }
    return checksum;
  });
  const all = run([...facetNames, ...numberNames]);
  const selected = run(hotFields);
  reader.close();

  if (!Number.isFinite(all.checksum) || !Number.isFinite(selected.checksum)) throw new Error("invalid benchmark checksum");
  console.log(JSON.stringify({
    docs,
    rows,
    codeStoreBuildMs: Number(buildMs.toFixed(2)),
    actualBytes,
    preloadedBytes: preload.preloadedBytes,
    facetIndex: {
      legacyBytes: legacyFacetIndexBytes,
      compactBytes: compactFacetIndexBytes,
      reduction: Number((legacyFacetIndexBytes / compactFacetIndexBytes).toFixed(2))
    },
    allFields: { fields: facetNames.length + numberNames.length, elapsedMs: Number(all.elapsedMs.toFixed(2)) },
    selectedFields: { fields: hotFields.length, elapsedMs: Number(selected.elapsedMs.toFixed(2)) },
    reducerReadSpeedup: Number((all.elapsedMs / selected.elapsedMs).toFixed(2))
  }, null, 2));
} finally {
  rmSync(root, { recursive: true, force: true });
}
