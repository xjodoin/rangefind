import assert from "node:assert/strict";
import { mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { CODE_STORE_FORMAT, createCodeStore, openCodeStore, preloadCodeStoreDescriptor } from "../src/build_store.js";

test("file-backed build code store reads random values and chunks without heap arrays", () => {
  const root = mkdtempSync(join(tmpdir(), "rangefind-codes-"));
  const config = {
    codeStoreCacheDocs: 2,
    codeStoreCacheChunks: 2,
    facets: [{ name: "tags" }],
    numbers: [{ name: "year" }, { name: "rating", type: "double" }],
    booleans: [{ name: "featured" }]
  };
  const dicts = {
    tags: { values: [{ value: "" }, { value: "static" }, { value: "range" }, { value: "bench" }] }
  };
  const store = createCodeStore(root, config, 4, dicts);
  try {
    store.set("tags", 0, { codes: [1] });
    store.set("year", 0, 2024);
    store.set("rating", 0, 1.5);
    store.set("featured", 0, true);
    store.set("tags", 1, { codes: [2] });
    store.set("year", 1, null);
    store.set("rating", 1, null);
    store.set("featured", 1, false);
    store.set("tags", 2, { codes: [3] });
    store.set("year", 2, 2026);
    store.set("rating", 2, 2.25);
    store.set("featured", 2, null);
    store.set("tags", 3, { codes: [1, 3] });
    store.set("year", 3, -5);
    store.set("rating", 3, -0.5);
    store.set("featured", 3, true);

    assert.deepEqual(store.get("tags", 3), { codes: [1, 3] });
    assert.equal(store.format, CODE_STORE_FORMAT);
    const tagField = store.descriptor().fields.find(field => field.name === "tags");
    assert.equal(tagField.bytesPerDoc, 4);
    assert.equal(statSync(tagField.indexPath).size, 16);
    // Only the multi-valued row uses overflow storage; empty and single-valued
    // rows are represented entirely inside their four-byte index cell.
    assert.equal(statSync(tagField.path).size, 12);
    assert.equal(store.get("year", 1), null);
    assert.equal(store.get("rating", 2), 2.25);
    assert.equal(store.get("featured", 1), false);
    assert.equal(store.descriptor().cacheChunks, 2);

    const reopened = openCodeStore(store.descriptor());
    try {
      const preload = reopened.preloadFields(["tags", "featured"], 1024);
      assert.deepEqual(preload.loadedFields, ["tags", "featured"]);
      assert.deepEqual(preload.skippedFields, []);
      assert.ok(preload.preloadedBytes > 0);
      assert.deepEqual(reopened.chunk("tags", 1, 3), [{ codes: [2] }, { codes: [3] }, { codes: [1, 3] }]);
      assert.deepEqual(reopened.chunk("year", 0, 4), [2024, null, 2026, -5]);
      assert.deepEqual(reopened.chunk("featured", 0, 4), [true, false, null, true]);
    } finally {
      reopened.close();
    }
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("code-store reader remains compatible with resumable v1 facet descriptors", () => {
  const root = mkdtempSync(join(tmpdir(), "rangefind-codes-v1-"));
  try {
    const path = join(root, "tags.bin");
    const indexPath = join(root, "tags.idx");
    const data = Buffer.alloc(12);
    data.writeUInt32LE(5, 0);
    data.writeUInt32LE(2, 4);
    data.writeUInt32LE(7, 8);
    const index = Buffer.alloc(48);
    index.writeBigUInt64LE(0n, 0);
    index.writeBigUInt64LE(4n, 8);
    index.writeBigUInt64LE(4n, 16);
    index.writeBigUInt64LE(0n, 24);
    index.writeBigUInt64LE(4n, 32);
    index.writeBigUInt64LE(8n, 40);
    writeFileSync(path, data);
    writeFileSync(indexPath, index);
    const store = openCodeStore({
      format: "rf-build-code-store-v1",
      total: 3,
      cacheDocs: 2,
      cacheChunks: 1,
      fields: [{ name: "tags", kind: "facet", path, indexPath, bytesPerDoc: 16 }]
    });
    try {
      assert.deepEqual(store.chunk("tags", 0, 3), [{ codes: [5] }, { codes: [] }, { codes: [2, 7] }]);
    } finally {
      store.close();
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("field-selective code-store preload skips oversized fields without blocking smaller hot fields", () => {
  const root = mkdtempSync(join(tmpdir(), "rangefind-codes-selective-"));
  const config = {
    facets: [{ name: "large" }, { name: "small" }],
    numbers: [{ name: "latitude" }],
    booleans: []
  };
  const store = createCodeStore(root, config, 4, {
    large: { values: [{ value: "" }, { value: "large" }] },
    small: { values: [{ value: "" }, { value: "small" }] }
  });
  try {
    for (let doc = 0; doc < 4; doc++) {
      store.set("large", doc, { codes: [1, 2] });
      store.set("small", doc, { codes: [1] });
      store.set("latitude", doc, 45 + doc);
    }
    const reopened = openCodeStore(store.descriptor());
    try {
      // The facet needs its 64-byte index plus data and cannot fit, while the
      // numeric column does. Priority-budgeted loading must continue past it.
      const result = reopened.preloadFields(["large", "latitude"], 40);
      assert.deepEqual(result.loadedFields, ["latitude"]);
      assert.deepEqual(result.skippedFields, ["large"]);
      assert.deepEqual(reopened.chunk("latitude", 0, 4), [45, 46, 47, 48]);
      assert.deepEqual(reopened.get("large", 2), { codes: [1, 2] });
    } finally {
      reopened.close();
    }
  } finally {
    store.close();
    rmSync(root, { recursive: true, force: true });
  }
});

test("worker descriptor preload continues after an oversized field", () => {
  const root = mkdtempSync(join(tmpdir(), "rangefind-codes-worker-preload-"));
  try {
    const largePath = join(root, "large.bin");
    const smallPath = join(root, "small.bin");
    writeFileSync(largePath, Buffer.alloc(64));
    writeFileSync(smallPath, Buffer.alloc(8));
    const descriptor = preloadCodeStoreDescriptor({
      format: CODE_STORE_FORMAT,
      total: 1,
      fields: [
        { name: "large", kind: "number", path: largePath, bytesPerDoc: 8 },
        { name: "small", kind: "number", path: smallPath, bytesPerDoc: 8 }
      ]
    }, 16, { bestEffort: true });
    assert.deepEqual(descriptor.preloadedFields, ["small"]);
    assert.deepEqual(descriptor.skippedFields, ["large"]);
    assert.equal(descriptor.preloadedBytes, 8);
    assert.equal(descriptor.fields[0].sharedData, undefined);
    assert.ok(descriptor.fields[1].sharedData instanceof SharedArrayBuffer);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
