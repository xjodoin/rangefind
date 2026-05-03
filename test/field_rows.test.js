import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createCodeStore } from "../src/build_store.js";
import { createFieldRowPipeline, FIELD_ROW_PIPELINE_FORMAT } from "../src/field_rows.js";

test("field row pipeline wraps typed scan rows for all build field kinds", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-field-rows-"));
  const config = {
    codeStoreCacheDocs: 2,
    codeStoreCacheChunks: 1,
    facets: [{ name: "tag" }, { name: "section" }],
    numbers: [{ name: "year" }, { name: "published", type: "date" }, { name: "rating", type: "double" }],
    booleans: [{ name: "featured" }]
  };
  const dicts = {
    tag: { values: ["static", "runtime", "bench"] },
    section: { values: ["docs"] }
  };
  const store = createCodeStore(root, config, 3, dicts);
  store.set("tag", 0, { codes: [0, 1] });
  store.set("tag", 1, { codes: [2] });
  store.set("tag", 2, { codes: [] });
  store.set("section", 0, { codes: [0] });
  store.set("section", 1, { codes: [0] });
  store.set("section", 2, { codes: [] });
  store.set("year", 0, 2024);
  store.set("year", 1, 2025);
  store.set("year", 2, null);
  store.set("published", 0, Date.parse("2024-01-01"));
  store.set("published", 1, Date.parse("2025-01-01"));
  store.set("published", 2, null);
  store.set("rating", 0, 4.5);
  store.set("rating", 1, 3.25);
  store.set("rating", 2, null);
  store.set("featured", 0, true);
  store.set("featured", 1, false);
  store.set("featured", 2, null);

  try {
    const rows = createFieldRowPipeline(store, config, 3);
    assert.equal(rows.format, FIELD_ROW_PIPELINE_FORMAT);
    assert.equal(rows.source, "rf-build-code-store-v1");
    assert.equal(rows.fieldCount, 6);
    assert.equal(rows.facetFields, 2);
    assert.equal(rows.numericFields, 3);
    assert.equal(rows.booleanFields, 1);
    assert.equal(rows.dateFields, 1);
    assert.deepEqual(rows.get("tag", 0), { codes: [0, 1] });
    assert.deepEqual(rows.chunk("featured", 0, 3), [true, false, null]);
    assert.deepEqual(rows.chunk("year", 1, 2), [2025, null]);
    assert.deepEqual(rows.descriptor(), {
      format: FIELD_ROW_PIPELINE_FORMAT,
      source: "rf-build-code-store-v1",
      total: 3,
      fields: [
        { name: "tag", kind: "facet", type: "keyword", words: 1, bytesPerDoc: 16 },
        { name: "section", kind: "facet", type: "keyword", words: 1, bytesPerDoc: 16 },
        { name: "year", kind: "number", type: "int", words: 0, bytesPerDoc: 8 },
        { name: "published", kind: "number", type: "date", words: 0, bytesPerDoc: 8 },
        { name: "rating", kind: "number", type: "double", words: 0, bytesPerDoc: 8 },
        { name: "featured", kind: "boolean", type: "boolean", words: 0, bytesPerDoc: 1 }
      ]
    });
  } finally {
    store.close();
  }
});

