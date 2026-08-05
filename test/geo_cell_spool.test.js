import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  appendGeoCellRoute,
  createGeoCellRouteSpool,
  sortedGeoCellRoutes
} from "../src/geo_cell_spool.js";

test("geo-cell route spool preserves records across buffered writes and external merge sorting", async () => {
  const root = mkdtempSync(join(tmpdir(), "rangefind-geo-routes-"));
  const spool = createGeoCellRouteSpool(join(root, "routes.bin"));
  const rows = [];
  try {
    for (let index = 0; index < 2500; index++) {
      const row = [
        index % 3,
        index % 17,
        (index * 19) % 101,
        (index * 23) % 103,
        index % 16,
        (index * 29) % 4096,
        (index * 31) % 4096,
        index % 257,
        (index * 37) % 1000,
        index % 512
      ];
      rows.push(row);
      appendGeoCellRoute(spool, row);
    }
    const actual = [];
    for await (const row of sortedGeoCellRoutes(spool, { chunkRecords: 97 })) actual.push(row);
    const expected = rows.toSorted((left, right) => {
      for (let field = 0; field < left.length; field++) {
        if (left[field] !== right[field]) return left[field] - right[field];
      }
      return 0;
    });
    assert.deepEqual(actual, expected);
    assert.equal(spool.records, rows.length);
    assert.ok(spool.bytes > 0);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
