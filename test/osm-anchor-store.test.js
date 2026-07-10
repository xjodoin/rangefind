import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";
import {
  coordinateStoreExists,
  createAnchorRefWriter,
  createCoordinateStore,
  openSortedAnchorRefs,
  sortUniqueAnchorRefs
} from "../scripts/osm_anchor_store.mjs";

test("anchor references are externally sorted and deduplicated", () => {
  const root = mkdtempSync(resolve(tmpdir(), "rangefind-osm-anchors-"));
  try {
    const input = resolve(root, "input.bin");
    const output = resolve(root, "output.bin");
    const writer = createAnchorRefWriter(input);
    for (const ref of [42, 7, 99, 7, 12, 42, 1, 1000, 2]) writer.write(ref);
    writer.close();

    const result = sortUniqueAnchorRefs(input, output, resolve(root, "scratch"), { chunkBytes: 24 });
    assert.equal(result.runs, 3);
    assert.equal(result.count, 7);

    const reader = openSortedAnchorRefs(output);
    const actual = [];
    while (reader.current != null) {
      actual.push(reader.current);
      reader.advance();
    }
    reader.close();
    assert.deepEqual(actual, [1, 2, 7, 12, 42, 99, 1000]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("coordinate store batches writes and supports indexed lookups", () => {
  const root = mkdtempSync(resolve(tmpdir(), "rangefind-osm-coords-"));
  try {
    const path = resolve(root, "coords.sqlite");
    const store = createCoordinateStore(path, { reset: true });
    store.put(7, 45.5019, -73.5674);
    store.put(42, 40.7128, -74.006);
    assert.equal(store.count(), 2);
    assert.deepEqual(store.get(42), { lat: 40.7128, lon: -74.006 });
    assert.equal(store.get(999), null);
    store.close();
    assert.equal(coordinateStoreExists(path), true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
