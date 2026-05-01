import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createCodeStore, openCodeStore } from "../src/build_store.js";

test("file-backed build code store reads random values and chunks without heap arrays", () => {
  const root = mkdtempSync(join(tmpdir(), "rangefind-codes-"));
  const config = {
    codeStoreCacheDocs: 2,
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
    assert.equal(store.get("year", 1), null);
    assert.equal(store.get("rating", 2), 2.25);
    assert.equal(store.get("featured", 1), false);

    const reopened = openCodeStore(store.descriptor());
    try {
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
