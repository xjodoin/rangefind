import assert from "node:assert/strict";
import test from "node:test";
import {
  buildBlockFilters,
  buildCodesFile,
  buildTermShard,
  decodePostingBytes,
  decodePostings,
  parseCodes,
  parseShard,
  rewriteTermShardForExternalBlocks
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
    booleans: [{ name: "featured" }],
    postingBlockSize: 2
  };
  const dicts = {
    category: { values: [{ value: "", label: "", n: 0 }, { value: "docs", label: "Docs", n: 2 }] }
  };
  const codes = {
    category: [1, 1, 0],
    year: [0, -5, 10],
    featured: [false, true, null]
  };
  const filters = buildBlockFilters(config, dicts);
  const buffer = buildTermShard([["search", [[0, 1000], [1, 800], [2, 100]]]], 3, codes, filters, config);
  const shard = parseShard(buffer, { block_filters: filters });
  const entry = shard.terms.get("search");
  assert.equal(entry.count, 3);
  assert.deepEqual([...decodePostings(shard, entry)].filter((_, index) => index % 2 === 0).sort(), [0, 1, 2]);
  assert.deepEqual(entry.blocks[0].filters.category.words, [2]);
  assert.deepEqual(entry.blocks[0].filters.year, { min: -5, max: 0 });
  assert.deepEqual(entry.blocks[0].filters.featured, { min: 1, max: 2 });
});

test("term shard codec can externalize posting blocks", () => {
  const config = {
    facets: [],
    numbers: [{ name: "year" }],
    booleans: [{ name: "featured" }],
    postingBlockSize: 2,
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0
  };
  const filters = buildBlockFilters(config, {});
  const codes = {
    year: [-1, 0, 1],
    featured: [false, false, true]
  };
  const buffer = buildTermShard([["search", [[0, 1000], [1, 800], [2, 100]]]], 3, codes, filters, config);
  const stored = [];
  const rewritten = rewriteTermShardForExternalBlocks(buffer, { block_filters: filters }, config, ({ bytes }) => {
    stored.push(bytes);
    return { pack: "0000.bin", offset: stored.length * 10, length: bytes.length };
  });
  const shard = parseShard(rewritten.buffer, { block_filters: filters, stats: { posting_block_storage: "range-pack-v1" } });
  const entry = shard.terms.get("search");
  assert.equal(entry.external, true);
  assert.equal(entry.blocks.length, 2);
  assert.equal(entry.blocks[0].range.pack, "0000.bin");
  assert.deepEqual(entry.blocks[0].filters.year, { min: -1, max: 0 });
  assert.deepEqual(entry.blocks[0].filters.featured, { min: 1, max: 1 });
  assert.deepEqual([...decodePostingBytes(stored[0])], [0, 13, 1, 11]);
  assert.equal(rewritten.stats.externalBlocks, 2);
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
    numbers: [{ name: "year" }, { name: "published", type: "date" }, { name: "rating", type: "double" }],
    booleans: [{ name: "featured" }]
  };
  const buffer = buildCodesFile(config, 3, {
    _dicts: { category: { values: [{ value: "" }, { value: "docs" }, { value: "api" }] } },
    category: [[6], [4], [2]],
    year: [2024, 2025, 2026],
    published: [Date.parse("2024-01-01"), null, Date.parse("2026-05-01")],
    rating: [1.5, 2.25, null],
    featured: [true, false, null]
  });
  assert.deepEqual(parseCodes(buffer), {
    category: [[6], [4], [2]],
    year: [2024, 2025, 2026],
    published: [Date.parse("2024-01-01"), null, Date.parse("2026-05-01")],
    rating: [1.5, 2.25, null],
    featured: [true, false, null]
  });
});
