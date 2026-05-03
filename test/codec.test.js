import assert from "node:assert/strict";
import test from "node:test";
import { authorityKeysForValue, buildAuthorityShard, parseAuthorityShard } from "../src/authority_codec.js";
import {
  buildBlockFilters,
  buildCodesFile,
  buildDocValueChunk,
  buildFacetDictionary,
  buildPostingSegment,
  docValueFields,
  decodePostingBytes,
  decodePostings,
  parseCodes,
  parseDocValueChunk,
  parseFacetDictionary,
  parsePostingSegment
} from "../src/codec.js";
import {
  buildPagedDirectory,
  DIRECTORY_FORMAT,
  findDirectoryPage,
  parseDirectoryPage,
  parseDirectoryRoot
} from "../src/directory.js";
import { buildQueryBundle, parseQueryBundle } from "../src/query_bundle_codec.js";

const checksum = { algorithm: "sha256", value: "a".repeat(64) };

test("posting segment codec round-trips postings and block filters", () => {
  const config = {
    facets: [{ name: "category" }],
    numbers: [{ name: "year" }],
    booleans: [{ name: "featured" }],
    postingBlockSize: 2,
    postingSuperblockSize: 2
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
  const segment = buildPostingSegment([["search", [[0, 1000], [1, 800], [2, 100]]]], 3, codes, filters, config);
  const shard = parsePostingSegment(segment.buffer, { block_filters: filters });
  const entry = shard.terms.get("search");
  assert.equal(segment.format, "rfsegpost-v3");
  assert.equal(shard.format, "rfsegpost-v3");
  assert.equal(entry.format, "rfsegpost-v3");
  assert.equal(entry.count, 3);
  assert.equal(entry.blocks[0].rowCount, 2);
  assert.equal(entry.blocks[1].rowCount, 1);
  assert.equal(entry.blocks[0].maxImpactDoc, 0);
  assert.equal(entry.blocks[1].maxImpactDoc, 2);
  assert.deepEqual(entry.superblocks.map(item => ({
    firstBlock: item.firstBlock,
    blockCount: item.blockCount,
    rowCount: item.rowCount,
    maxImpact: item.maxImpact,
    maxImpactDoc: item.maxImpactDoc
  })), [{ firstBlock: 0, blockCount: 2, rowCount: 3, maxImpact: 13, maxImpactDoc: 0 }]);
  assert.deepEqual(entry.superblocks[0].filters.category.words, [3]);
  assert.deepEqual(entry.superblocks[0].filters.year, { min: -5, max: 10 });
  assert.deepEqual(entry.superblocks[0].filters.featured, { min: 0, max: 2 });
  assert.equal(segment.stats.superblocks, 1);
  assert.equal(segment.stats.superblockTerms, 1);
  assert.equal(segment.stats.superblockBlocks, 2);
  assert.deepEqual([...decodePostings(shard, entry)].filter((_, index) => index % 2 === 0).sort(), [0, 1, 2]);
  assert.deepEqual(entry.blocks[0].filters.category.words, [2]);
  assert.deepEqual(entry.blocks[0].filters.year, { min: -5, max: 0 });
  assert.deepEqual(entry.blocks[0].filters.featured, { min: 1, max: 2 });
});

test("posting segment codec writes external posting blocks directly", () => {
  const config = {
    facets: [],
    numbers: [{ name: "year" }],
    booleans: [{ name: "featured" }],
    postingBlockSize: 2,
    postingSuperblockSize: 1,
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0
  };
  const filters = buildBlockFilters(config, {});
  const codes = {
    year: [-1, 0, 1],
    featured: [false, false, true]
  };
  const stored = [];
  const segment = buildPostingSegment([["search", [[0, 1000], [1, 800], [2, 100]]]], 3, codes, filters, config, ({ bytes }) => {
    stored.push(bytes);
    return {
      pack: "0000.bin",
      offset: stored.length * 10,
      length: bytes.length,
      logicalLength: bytes.length,
      checksum
    };
  });
  const shard = parsePostingSegment(segment.buffer, {
    block_filters: filters,
    object_store: {
      pointer_format: "rfbp-v1",
      pack_table: { postingBlocks: ["0000.immutable.bin"] }
    }
  });
  const entry = shard.terms.get("search");
  assert.equal(segment.format, "rfsegpost-v3");
  assert.equal(entry.external, true);
  assert.equal(entry.blocks.length, 2);
  assert.equal(entry.blocks[0].rowCount, 2);
  assert.equal(entry.blocks[1].rowCount, 1);
  assert.equal(entry.blocks[0].maxImpactDoc, 0);
  assert.equal(entry.blocks[1].maxImpactDoc, 2);
  assert.equal(entry.superblocks.length, 2);
  assert.deepEqual(entry.superblocks[0].filters.year, { min: -1, max: 0 });
  assert.deepEqual(entry.superblocks[1].filters.featured, { min: 2, max: 2 });
  assert.equal(entry.blocks[0].range.pack, "0000.immutable.bin");
  assert.equal(entry.blocks[0].range.logicalLength, 4);
  assert.equal(entry.blocks[0].range.checksum.value, checksum.value);
  assert.deepEqual(entry.blocks[0].filters.year, { min: -1, max: 0 });
  assert.deepEqual(entry.blocks[0].filters.featured, { min: 1, max: 1 });
  assert.deepEqual([...decodePostingBytes(stored[0], entry.blocks[0])], [0, 13, 1, 11]);
  assert.equal(segment.stats.externalBlocks, 2);
  assert.equal(segment.stats.externalTerms, 1);
  assert.equal(segment.stats.inlinePostingBytes, 0);
  assert.equal(segment.stats.superblocks, 2);
  assert.equal(segment.stats.superblockTerms, 1);
  assert.equal(segment.stats.superblockBlocks, 2);
});

test("posting segment codec selects compact impact runs when measured smaller", () => {
  const config = {
    facets: [],
    numbers: [],
    booleans: [],
    postingBlockSize: 4,
    postingSuperblockSize: 2,
    codecs: { mode: "auto" }
  };
  const filters = buildBlockFilters(config, {});
  const segment = buildPostingSegment([["common", [[0, 1000], [100, 1000], [200, 1000], [300, 1000]]]], 301, {}, filters, config);
  const shard = parsePostingSegment(segment.buffer, { block_filters: filters });
  const entry = shard.terms.get("common");
  assert.equal(entry.blocks[0].codec, "impact-runs-v1");
  assert.equal(segment.stats.impactRunBlocks, 1);
  assert.equal(segment.stats.pairVarintBlocks, 0);
  assert.ok(segment.stats.blockCodecSelectedBytes < segment.stats.blockCodecBaselineBytes);
  assert.deepEqual([...decodePostings(shard, entry)].filter((_, index) => index % 2 === 0), [0, 100, 200, 300]);
});

test("posting segment codec selects dense impact bitsets when measured smaller", () => {
  const config = {
    facets: [],
    numbers: [],
    booleans: [],
    postingBlockSize: 32,
    postingSuperblockSize: 2,
    codecs: { mode: "auto" }
  };
  const filters = buildBlockFilters(config, {});
  const rows = Array.from({ length: 32 }, (_, doc) => [doc, 1000]);
  const segment = buildPostingSegment([["dense", rows]], 32, {}, filters, config);
  const shard = parsePostingSegment(segment.buffer, { block_filters: filters });
  const entry = shard.terms.get("dense");
  assert.equal(entry.blocks[0].codec, "impact-bitset-v1");
  assert.equal(segment.stats.impactBitsetBlocks, 1);
  assert.equal(segment.stats.impactRunBlocks, 0);
  assert.ok(segment.stats.blockCodecSelectedBytes < segment.stats.blockCodecBaselineBytes);
  assert.deepEqual([...decodePostings(shard, entry)].filter((_, index) => index % 2 === 0), Array.from({ length: 32 }, (_, doc) => doc));
});

test("posting segment codec selects partitioned deltas for sparse regular doc ids", () => {
  const config = {
    facets: [],
    numbers: [],
    booleans: [],
    postingBlockSize: 32,
    postingSuperblockSize: 2,
    codecs: { mode: "auto" }
  };
  const filters = buildBlockFilters(config, {});
  const docs = Array.from({ length: 32 }, (_, index) => index * 10);
  const segment = buildPostingSegment([["regular", docs.map(doc => [doc, 1000])]], 311, {}, filters, config);
  const shard = parsePostingSegment(segment.buffer, { block_filters: filters });
  const entry = shard.terms.get("regular");
  assert.equal(entry.blocks[0].codec, "partitioned-deltas-v1");
  assert.equal(segment.stats.partitionedDeltaBlocks, 1);
  assert.ok(segment.stats.blockCodecSelectedBytes < segment.stats.blockCodecBaselineBytes);
  assert.deepEqual([...decodePostings(shard, entry)].filter((_, index) => index % 2 === 0), docs);
});

test("query bundle codec round-trips proof metadata and rows", () => {
  const bundle = parseQueryBundle(buildQueryBundle({
    key: "exact-expanded-v1|chang climat",
    baseTerms: ["chang", "climat"],
    expandedTerms: ["chang", "climat", "chang_climat"],
    total: 68,
    complete: false,
    nextScoreBound: 41,
    nextTieDoc: 812,
    rows: [{ doc: 2062, score: 50 }, { doc: 9405, score: 44 }],
    rowGroups: [{ rowStart: 0, rowCount: 2, scoreMax: 50, scoreMin: 44, docMin: 2062, docMax: 9405 }]
  }));
  assert.equal(bundle.key, "exact-expanded-v1|chang climat");
  assert.deepEqual(bundle.baseTerms, ["chang", "climat"]);
  assert.deepEqual(bundle.expandedTerms, ["chang", "climat", "chang_climat"]);
  assert.equal(bundle.total, 68);
  assert.equal(bundle.complete, false);
  assert.equal(bundle.nextScoreBound, 41);
  assert.equal(bundle.nextTieDoc, 812);
  assert.deepEqual(bundle.rows, [[2062, 50], [9405, 44]]);
  assert.deepEqual(bundle.rowGroups, [{ rowStart: 0, rowCount: 2, scoreMax: 50, scoreMin: 44, docMin: 2062, docMax: 9405, filters: {} }]);
});

test("authority shard codec round-trips exact and token rows", () => {
  const shard = parseAuthorityShard(buildAuthorityShard([
    ["t|paris", [[3, 800000], [1, 400000]]],
    ["x|paris", [[1, 1000000]]]
  ], { maxRows: 1 }));
  assert.equal(shard.format, "rfauth-v1");
  assert.equal(shard.entries.get("t|paris").total, 2);
  assert.equal(shard.entries.get("t|paris").complete, false);
  assert.deepEqual(shard.entries.get("t|paris").rows, [[3, 800000]]);
  assert.equal(shard.entries.get("x|paris").complete, true);
  assert.deepEqual(shard.entries.get("x|paris").rows, [[1, 1000000]]);
});

test("authority keys keep surface-exact accents separate from folded exact lookup", () => {
  assert.deepEqual(
    authorityKeysForValue("Paris").map(item => item.key),
    ["r|paris", "x|paris", "t|pari"]
  );
  assert.deepEqual(
    authorityKeysForValue("Pâris").map(item => item.key),
    ["r|pâris", "x|paris", "t|pari"]
  );
});

test("paged directory can encode checksummed block pointers", () => {
  const directory = buildPagedDirectory([
    { shard: "object", packIndex: 1, offset: 20, length: 30, logicalLength: 120, checksum }
  ], { pageBytes: 1024 });
  assert.equal(directory.format, DIRECTORY_FORMAT);
  const root = parseDirectoryRoot(directory.root);
  assert.equal(root.version, 2);
  const page = findDirectoryPage(root, "object");
  const ranges = parseDirectoryPage(directory.pages.find(item => item.file === page.file).buffer);
  assert.deepEqual(ranges.get("object"), {
    pack: "0001.bin",
    offset: 20,
    length: 30,
    physicalLength: 30,
    logicalLength: 120,
    checksum
  });
});

test("paged directory maps pack indexes through immutable pack table", () => {
  const directory = buildPagedDirectory([
    { shard: "object", packIndex: 1, offset: 20, length: 30, logicalLength: 40, checksum }
  ], { pageBytes: 1024 });
  const root = parseDirectoryRoot(directory.root);
  const page = findDirectoryPage(root, "object");
  const ranges = parseDirectoryPage(directory.pages.find(item => item.file === page.file).buffer, {
    packTable: ["0000.a.bin", "0001.b.bin"]
  });
  assert.equal(ranges.get("object").pack, "0001.b.bin");
});

test("paged directory maps named shards to packed file offsets", () => {
  const directory = buildPagedDirectory([
    { shard: "bbb", packIndex: 2, offset: 5, length: 30, checksum },
    { shard: "aaa", packIndex: 0, offset: 10, length: 50, checksum }
  ], { pageBytes: 1024 });
  const root = parseDirectoryRoot(directory.root);
  const page = findDirectoryPage(root, "aaa");
  assert.ok(page);
  const ranges = parseDirectoryPage(directory.pages.find(item => item.file === page.file).buffer);
  assert.deepEqual(ranges.get("aaa"), { pack: "0000.bin", offset: 10, length: 50, physicalLength: 50, logicalLength: null, checksum });
  assert.deepEqual(ranges.get("bbb"), { pack: "0002.bin", offset: 5, length: 30, physicalLength: 30, logicalLength: null, checksum });
  assert.equal(findDirectoryPage(root, "zzz"), null);
});

test("paged directory uses binary key order for lookup bounds", () => {
  const directory = buildPagedDirectory([
    { shard: "r|p", packIndex: 0, offset: 10, length: 20, checksum },
    { shard: "r|â", packIndex: 0, offset: 30, length: 20, checksum },
    { shard: "x|p", packIndex: 0, offset: 50, length: 20, checksum }
  ], { pageBytes: 1024 });
  const root = parseDirectoryRoot(directory.root);
  const page = findDirectoryPage(root, "r|p");
  assert.ok(page);
  const ranges = parseDirectoryPage(directory.pages.find(item => item.file === page.file).buffer);
  assert.deepEqual(ranges.get("r|p"), { pack: "0000.bin", offset: 10, length: 20, physicalLength: 20, logicalLength: null, checksum });
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

test("doc-value chunks round-trip typed column slices", () => {
  const config = {
    facets: [{ name: "category" }],
    numbers: [{ name: "published", type: "date" }, { name: "rating", type: "double" }],
    booleans: [{ name: "featured" }]
  };
  const fields = docValueFields(config, {
    _dicts: { category: { values: [{ value: "" }, { value: "docs" }, { value: "api" }] } }
  });
  const byName = Object.fromEntries(fields.map(field => [field.name, field]));
  const facet = buildDocValueChunk(byName.category, 2, [{ codes: [1, 2] }, { codes: [2] }]);
  assert.deepEqual(facet.summary.words, [6]);
  assert.deepEqual(parseDocValueChunk(facet.buffer).values, [{ codes: [1, 2] }, { codes: [2] }]);

  const dates = buildDocValueChunk(byName.published, 2, [Date.parse("2026-01-01"), null]);
  assert.deepEqual(parseDocValueChunk(dates.buffer).values, [Date.parse("2026-01-01"), null]);

  const rating = buildDocValueChunk(byName.rating, 2, [1.25, null]);
  assert.deepEqual(parseDocValueChunk(rating.buffer).values, [1.25, null]);

  const bool = buildDocValueChunk(byName.featured, 2, [false, true]);
  assert.deepEqual(bool.summary, { min: 1, max: 2 });
  assert.deepEqual(parseDocValueChunk(bool.buffer).values, [false, true]);
});

test("sparse facet doc-value chunks do not allocate dense high-cardinality summaries", () => {
  const values = Array.from({ length: 3000 }, (_, index) => ({ value: String(index) }));
  const [field] = docValueFields({ facets: [{ name: "tag" }], numbers: [], booleans: [] }, {
    _dicts: { tag: { values } }
  });
  const chunk = buildDocValueChunk(field, 0, [{ codes: [12, 2048] }, { codes: [2999] }]);
  assert.equal(chunk.summary.words, null);
  assert.deepEqual(parseDocValueChunk(chunk.buffer).values, [{ codes: [12, 2048] }, { codes: [2999] }]);
});

test("facet dictionary codec round-trips labels and counts", () => {
  const values = [
    { value: "", label: "", n: 0 },
    { value: "science", label: "Science", n: 42 },
    { value: "santé", label: "Santé", n: 7 }
  ];
  assert.deepEqual(parseFacetDictionary(buildFacetDictionary(values)), values);
});
