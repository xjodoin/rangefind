import assert from "node:assert/strict";
import test from "node:test";
import { groupRanges, partitionEntries, shardKey } from "../src/shards.js";

test("shardKey pads short terms for stable file names", () => {
  assert.equal(shardKey("ai", 3), "ai_");
});

test("partitionEntries recursively splits oversized shards", () => {
  const entries = [
    ["alpha", [[0, 1], [1, 1]]],
    ["alpine", [[2, 1], [3, 1]]],
    ["beta", [[4, 1], [5, 1]]]
  ];
  const partitions = partitionEntries(entries, { baseShardDepth: 1, maxShardDepth: 2, targetShardPostings: 2 });
  assert.deepEqual(partitions.map(item => item.name), ["al", "be"]);
});

test("groupRanges merges nearby ranges by pack", () => {
  const groups = groupRanges([
    { entry: { pack: "0000.bin", offset: 0, length: 10 } },
    { entry: { pack: "0000.bin", offset: 15, length: 10 } },
    { entry: { pack: "0001.bin", offset: 0, length: 5 } }
  ], 8);
  assert.equal(groups.length, 2);
  assert.deepEqual(groups[0], { pack: "0000.bin", start: 0, end: 25, items: groups[0].items });
});

test("groupRanges limits adaptive overfetch", () => {
  const merged = groupRanges([
    { entry: { pack: "0000.bin", offset: 0, length: 10 } },
    { entry: { pack: "0000.bin", offset: 90, length: 10 } }
  ], { mergeGapBytes: 100, maxOverfetchBytes: 100, maxOverfetchRatio: 10 });
  assert.equal(merged.length, 1);

  const split = groupRanges([
    { entry: { pack: "0000.bin", offset: 0, length: 10 } },
    { entry: { pack: "0000.bin", offset: 90, length: 10 } }
  ], { mergeGapBytes: 100, maxOverfetchBytes: 20, maxOverfetchRatio: 10 });
  assert.equal(split.length, 2);
});
