import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { reduceRunToPartitions } from "../src/reduce_stream.js";
import { encodeRunRecord } from "../src/runs.js";

const SCHEMA = ["string", "number", "number"];

function runBytes(records) {
  return Buffer.concat(records.map(record => encodeRunRecord(SCHEMA, record)));
}

test("external run reducer sorts, merges duplicate postings, and streams prefix partitions", async () => {
  const root = mkdtempSync(join(tmpdir(), "rangefind-reduce-"));
  const runPath = join(root, "ab.run");
  const scratchDir = join(root, "sort");
  writeFileSync(runPath, runBytes([
    ["abc", 3, 10],
    ["abd", 2, 20],
    ["abc", 1, 7],
    ["abc", 3, 5],
    ["abe", 4, 9],
    ["abd", 2, 1]
  ]));

  const terms = [];
  const partitions = [];
  try {
    const stats = await reduceRunToPartitions({
      runPath,
      scratchDir,
      config: {
        baseShardDepth: 2,
        maxShardDepth: 3,
        targetShardPostings: 2,
        reduceSortChunkRecords: 2,
        reduceSortChunkBytes: 1024
      },
      onTerm(term, df) {
        terms.push([term, df]);
      },
      onPartition(partition, sequence) {
        assert.equal(existsSync(join(scratchDir, "reduced-terms.run")), false);
        partitions.push({ sequence, name: partition.name, entries: partition.entries });
        return partition.name;
      }
    });

    assert.deepEqual(stats, { terms: 3, postings: 4, partitions: ["abc", "abd", "abe"] });
    assert.deepEqual(terms, [["abc", 2], ["abd", 1], ["abe", 1]]);
    assert.deepEqual(partitions, [
      { sequence: 0, name: "abc", entries: [["abc", [[1, 7], [3, 15]]]] },
      { sequence: 1, name: "abd", entries: [["abd", [[2, 21]]]] },
      { sequence: 2, name: "abe", entries: [["abe", [[4, 9]]]] }
    ]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
