import assert from "node:assert/strict";
import test from "node:test";
import {
  buildBlockFilters,
  buildCodesFile,
  buildTermShard,
  decodePostings,
  parseCodes,
  parseShard
} from "../src/codec.js";
import {
  buildPagedDirectory,
  findDirectoryPage,
  parseDirectoryPage,
  parseDirectoryRoot
} from "../src/directory.js";

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

test("paged directory maps named shards to packed file offsets", () => {
  const directory = buildPagedDirectory([
    { shard: "bbb", packIndex: 2, offset: 5, length: 30 },
    { shard: "aaa", packIndex: 0, offset: 10, length: 50 }
  ], { pageBytes: 1024 });
  const root = parseDirectoryRoot(directory.root);
  const page = findDirectoryPage(root, "aaa");
  assert.ok(page);
  const ranges = parseDirectoryPage(directory.pages.find(item => item.file === page.file).buffer);
  assert.deepEqual(ranges.get("aaa"), { pack: "0000.bin", offset: 10, length: 50 });
  assert.deepEqual(ranges.get("bbb"), { pack: "0002.bin", offset: 5, length: 30 });
  assert.equal(findDirectoryPage(root, "zzz"), null);
});

test("code table codec round-trips facet and numeric columns", () => {
  const config = {
    facets: [{ name: "category" }],
    numbers: [{ name: "year" }]
  };
  const buffer = buildCodesFile(config, 3, { category: [1, 2, 1], year: [2024, 2025, 2026] });
  assert.deepEqual(parseCodes(buffer), { category: [1, 2, 1], year: [2024, 2025, 2026] });
});
