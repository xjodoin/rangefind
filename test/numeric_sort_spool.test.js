import assert from "node:assert/strict";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { forEachExternallySortedNumericRow } from "../src/numeric_sort_spool.js";

test("numeric rows are externally sorted by value then document", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-numeric-sort-"));
  const values = [7, -2, null, 7, 1.5, Number.NaN, -2, 0, 7, 1.5];
  const actual = [];
  try {
    const stats = forEachExternallySortedNumericRow({
      total: values.length,
      tempDir: root,
      chunkRows: 2,
      readChunkRows: 3,
      maxOpenRuns: 2,
      readValues: (start, count) => values.slice(start, start + count),
      valueOf: value => value == null ? null : Number(value)
    }, (doc, value) => actual.push({ doc, value }));
    assert.deepEqual(actual, [
      { doc: 1, value: -2 },
      { doc: 6, value: -2 },
      { doc: 7, value: 0 },
      { doc: 4, value: 1.5 },
      { doc: 9, value: 1.5 },
      { doc: 0, value: 7 },
      { doc: 3, value: 7 },
      { doc: 8, value: 7 }
    ]);
    assert.equal(stats.rows, 8);
    assert.equal(stats.runs, 4);
    assert.equal(stats.mergePasses, 1);
    assert.equal(stats.tempBytes, 8 * 12);
    assert.deepEqual(await readdir(root), []);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("numeric sort removes partial runs when the consumer fails", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-numeric-sort-failure-"));
  try {
    assert.throws(() => forEachExternallySortedNumericRow({
      total: 5,
      tempDir: root,
      chunkRows: 2,
      readValues: (start, count) => [5, 4, 3, 2, 1].slice(start, start + count),
      valueOf: Number
    }, () => {
      throw new Error("consumer stopped");
    }), /consumer stopped/u);
    assert.deepEqual(await readdir(root), []);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
