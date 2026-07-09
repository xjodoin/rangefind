import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";
import {
  addAuthorityRecord,
  createAuthorityRunBuffer,
  finishAuthorityRuns
} from "../src/authority_index.js";

test("authority run rows flush under their own bounded record budget", () => {
  const root = mkdtempSync(resolve(tmpdir(), "rangefind-authority-flush-"));
  const config = {
    authority: [{ name: "title", path: "title" }],
    authorityRunFlushRecords: 2,
    baseShardDepth: 1,
    maxShardDepth: 3,
    targetShardPostings: 1000
  };
  try {
    const buffer = createAuthorityRunBuffer(config, root);
    addAuthorityRecord(buffer, config, "alpha", 0, 100);
    assert.equal(buffer.lines, 1);
    addAuthorityRecord(buffer, config, "alpine", 1, 90);
    assert.equal(buffer.lines, 0);
    assert.equal(buffer.byShard.size, 0);
    assert.ok(readdirSync(root).some(file => file.endsWith(".run")));
    const shards = finishAuthorityRuns(buffer);
    assert.deepEqual(shards, ["a"]);
    assert.ok(existsSync(resolve(root, "a.run")));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
