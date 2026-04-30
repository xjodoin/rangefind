import assert from "node:assert/strict";
import test from "node:test";
import {
  buildBlockFilters,
  buildCodesFile,
  buildRangeFile,
  buildTermShard,
  decodePostings,
  parseCodes,
  parseRangeDirectory,
  parseShard
} from "../src/codec.js";

test("term shard codec round-trips postings and block filters", () => {
  const config = {
    facets: [{ name: "category" }],
    numbers: [{ name: "year" }],
    postingBlockSize: 2
  };
  const dicts = {
    category: { values: [{ value: "", label: "", n: 0 }, { value: "docs", label: "Docs", n: 2 }] }
  };
  const codes = {
    category: [1, 1, 0],
    year: [2024, 2026, 0]
  };
  const filters = buildBlockFilters(config, dicts);
  const buffer = buildTermShard([["search", [[0, 1000], [1, 800], [2, 100]]]], 3, codes, filters, config);
  const shard = parseShard(buffer, { block_filters: filters });
  const entry = shard.terms.get("search");
  assert.equal(entry.count, 3);
  assert.deepEqual([...decodePostings(shard, entry)].filter((_, index) => index % 2 === 0).sort(), [0, 1, 2]);
  assert.deepEqual(entry.blocks[0].filters.category.words, [2]);
  assert.deepEqual(entry.blocks[0].filters.year, { min: 2024, max: 2026 });
});

test("range directory maps manifest shard order to packed file offsets", () => {
  const buffer = buildRangeFile([[0, 10, 50], [2, 5, 30]]);
  const ranges = parseRangeDirectory(buffer, { shards: ["aaa", "bbb"] });
  assert.deepEqual(ranges.get("aaa"), { pack: "0000.bin", offset: 10, length: 50 });
  assert.deepEqual(ranges.get("bbb"), { pack: "0002.bin", offset: 5, length: 30 });
});

test("code table codec round-trips facet and numeric columns", () => {
  const config = {
    facets: [{ name: "category" }],
    numbers: [{ name: "year" }]
  };
  const buffer = buildCodesFile(config, 3, { category: [1, 2, 1], year: [2024, 2025, 2026] });
  assert.deepEqual(parseCodes(buffer), { category: [1, 2, 1], year: [2024, 2025, 2026] });
});
