// Parity fixtures for the Rust `codec` module group.
//
// Run from the repo root:  node rust/tools/fixtures/codec.mjs
//
// Every binary input is produced by the real JS encoders/builders, decoded
// with the real JS decoders, and written as JSON (binary as base64) to
// rust/target/fixtures/codec/. rust/rangefind/tests/codec_parity.rs decodes
// the same bytes and requires identical results.

import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gunzipSync } from "node:zlib";

import {
  buildCodesFile,
  buildDocValueChunk,
  buildFacetDictionary,
  buildPostingSegment,
  decodePostingBlock,
  decodePostingBytes,
  decodePostings,
  lookupDecodedPostingRows,
  lookupPostingBlock,
  lookupPostingBytes,
  parseCodes,
  parseDocValueChunk,
  parseFacetDictionary,
  parsePostingSegment
} from "../../../src/codec.js";
import { pushVarint } from "../../../src/binary.js";
import {
  buildPagedDirectory,
  compareDirectoryKeys,
  directoryMightContain,
  findDirectoryPage,
  parseDirectoryPage,
  parseDirectoryRoot
} from "../../../src/directory.js";
import { buildDocPointerTable, decodeDocPointerRecord, parseDocPointerPage } from "../../../src/doc_pointers.js";
import { buildDocPagePointerTable, decodeDocPageColumns, encodeDocPageColumns, parseDocPagePointerPage } from "../../../src/doc_pages.js";
import { decodeDocValueSortPage, encodeDocValueSortDirectory, encodeDocValueSortPage, parseDocValueSortDirectory } from "../../../src/doc_value_tree.js";
import { encodeDocValueChecksums, encodeDocValueManifest, parseDocValueManifest } from "../../../src/doc_value_manifest.js";
import { createFilterBitmap, encodeFilterBitmap, filterBitmapHas, parseFilterBitmap, setFilterBitmapBit } from "../../../src/filter_bitmaps.js";
import { buildQueryBundle, parseQueryBundle } from "../../../src/query_bundle_codec.js";
import { decodeSegmentRows, parseSegmentTerms } from "../../../src/segment_codec.js";
import { writeSegmentFromTermRows } from "../../../src/segment_builder.js";
import { encodeTextRoutingSegment, floorDirectoryPageIndex, floorSortedKeyIndex, parseTextRoutingSegment } from "../../../src/text_routing_codec.js";
import { parseMultipartByteRanges, selectMultipartByteRanges } from "../../../src/http_ranges.js";
import { verifyBlockPointer } from "../../../src/object_store.js";
import { build } from "../../../src/builder.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, "../../..");
const OUT = resolve(REPO, "rust/target/fixtures/codec");
mkdirSync(OUT, { recursive: true });

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function rng(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const b64 = value => {
  if (value instanceof ArrayBuffer) return Buffer.from(value).toString("base64");
  return Buffer.from(value.buffer, value.byteOffset, value.byteLength).toString("base64");
};
const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
const units = text => Array.from({ length: text.length }, (_, i) => text.charCodeAt(i));
const arr = typed => Array.from(typed || []);

function tryCall(fn) {
  try {
    return { ok: fn() };
  } catch (error) {
    return { error: error.message };
  }
}

async function tryCallAsync(fn) {
  try {
    return { ok: await fn() };
  } catch (error) {
    return { error: error.message };
  }
}

function write(name, value) {
  writeFileSync(join(OUT, `${name}.json`), JSON.stringify(value));
  const count = Array.isArray(value) ? value.length : Object.values(value).reduce((sum, item) => sum + (Array.isArray(item) ? item.length : 1), 0);
  console.log(`${name}: ${count}`);
}

const errors = [];
function errorCase(fn, bytes, args, run) {
  const result = tryCall(run);
  if (!("error" in result)) throw new Error(`expected ${fn} to fail`);
  errors.push({ fn, bytes: b64(bytes), args, error: result.error });
}

// ---------------------------------------------------------------------------
// postings
// ---------------------------------------------------------------------------

function pointerJson(range) {
  if (!range) return null;
  return {
    pack: range.pack,
    offset: range.offset,
    length: range.length,
    physicalLength: range.physicalLength ?? range.length,
    logicalLength: range.logicalLength ?? null,
    checksum: range.checksum ?? null
  };
}

function entryJson(entry) {
  return {
    format: entry.format,
    df: entry.df,
    count: entry.count,
    offset: entry.offset,
    byteLength: entry.byteLength,
    blockSize: entry.blockSize,
    external: entry.external,
    blocks: entry.blocks.map(block => ({
      offset: block.offset,
      rowCount: block.rowCount,
      maxImpact: block.maxImpact,
      maxImpactDoc: block.maxImpactDoc,
      docMin: block.docMin,
      docMax: block.docMax,
      codec: block.codec,
      filters: block.filters,
      range: pointerJson(block.range),
      docRanges: block.docRanges ? block.docRanges.ranges.map(r => ({ index: r.index, maxImpact: r.maxImpact, quantizedMaxImpact: r.quantizedMaxImpact })) : null
    })),
    superblocks: entry.superblocks,
    docRanges: entry.docRanges,
    impactTiers: entry.impactTiers ? { blocks: arr(entry.impactTiers.blocks), tiers: entry.impactTiers.tiers } : null
  };
}

function targetSets(random, docs, total) {
  const sets = [[]];
  const pick = n => Array.from({ length: n }, () => docs[Math.floor(random() * docs.length)]);
  for (let k = 0; k < 4; k++) {
    const set = new Set([...pick(1 + Math.floor(random() * 6)), ...Array.from({ length: 3 }, () => Math.floor(random() * (total + 50)))]);
    sets.push([...set]);
  }
  sets.push([...new Set(docs)].slice(0, 400));
  return sets;
}

function postingCaseFromSegment(name, buffer, manifest, externalBlocks, random, total) {
  const segment = parsePostingSegment(buffer, manifest);
  const terms = [];
  for (const [term, entry] of segment.terms) {
    const item = { term, entry: entryJson(entry) };
    if (!entry.external) {
      item.postings = arr(decodePostings(segment, entry));
      item.blocks = entry.blocks.map((_, index) => arr(decodePostingBlock(segment, entry, index)));
      const docs = [];
      for (let i = 0; i < item.postings.length; i += 2) docs.push(item.postings[i]);
      item.lookups = [];
      for (let index = 0; index < entry.blocks.length; index++) {
        for (const targets of targetSets(random, docs.length ? docs : [0], total)) {
          const result = lookupPostingBlock(segment, entry, index, new Set(targets));
          item.lookups.push({ block: index, targets, rows: arr(result.rows), scanned: result.scanned });
        }
      }
      item.outOfRange = { decoded: arr(decodePostingBlock(segment, entry, entry.blocks.length)), lookup: arr(lookupPostingBlock(segment, entry, entry.blocks.length + 3, new Set([1])).rows) };
    } else {
      item.decodeError = tryCall(() => decodePostings(segment, entry)).error;
      item.external = [];
      entry.blocks.forEach((block, index) => {
        const bytes = externalBlocks.get(`${term}\u0000${index}`);
        if (!bytes) return;
        const decoded = arr(decodePostingBytes(bytes, block));
        const docs = [];
        for (let i = 0; i < decoded.length; i += 2) docs.push(decoded[i]);
        const lookups = targetSets(random, docs.length ? docs : [0], total).map(targets => {
          const bytesResult = lookupPostingBytes(bytes, new Set(targets), block);
          const rowsResult = lookupDecodedPostingRows(Int32Array.from(decoded), new Set(targets));
          return { targets, rows: arr(bytesResult.rows), scanned: bytesResult.scanned, decodedRows: arr(rowsResult.rows), decodedScanned: rowsResult.scanned };
        });
        item.external.push({ block: index, codec: block.codec, bytes: b64(bytes), decoded, lookups });
      });
    }
    terms.push(item);
  }
  return { name, manifest, segment: b64(buffer), dataStart: segment.dataStart, terms };
}

function syntheticPostingCases() {
  const cases = [];
  const filters = [
    { name: "cat", kind: "facet", words: 3 },
    { name: "tag", kind: "facet", words: 1 },
    { name: "year", kind: "number", type: "int" },
    { name: "price", kind: "number", type: "float" },
    { name: "flag", kind: "boolean", type: "boolean" }
  ];
  const variants = [];
  for (const mode of ["varint", "impact-runs", "impact-bitset", "partitioned-deltas", "auto"]) {
    for (const order of ["doc-id", "impact"]) {
      variants.push({ mode, order });
    }
  }
  let seed = 1;
  for (const variant of variants) {
    for (const objectPointers of [true, false]) {
      const random = rng(seed++);
      const total = 3000 + Math.floor(random() * 3000);
      const codes = {
        cat: Array.from({ length: total }, () => (random() < 0.15 ? null : { codes: [Math.floor(random() * 96), Math.floor(random() * 96)] })),
        tag: Array.from({ length: total }, () => (random() < 0.2 ? null : Math.floor(random() * 32))),
        year: Array.from({ length: total }, () => (random() < 0.1 ? null : 1900 + Math.floor(random() * 130))),
        price: Array.from({ length: total }, () => (random() < 0.1 ? null : Math.round(random() * 100000) / 100 - 20)),
        flag: Array.from({ length: total }, () => (random() < 0.3 ? null : random() < 0.5))
      };
      const entries = [];
      const termCount = 6 + Math.floor(random() * 8);
      const extraTerms = ["é", "naïve", "日本", "😀a", "😁b", "﻿bom", "zz"];
      for (let t = 0; t < termCount + extraTerms.length; t++) {
        const term = t < termCount ? `t${t}-${Math.floor(random() * 1000)}` : extraTerms[t - termCount];
        const size = [1, 2, 7, 40, 300, 900, 2500][Math.floor(random() * 7)];
        const docs = new Set();
        const dense = random() < 0.4;
        const start = Math.floor(random() * total / 2);
        while (docs.size < Math.min(size, total)) {
          docs.add(dense ? Math.min(total - 1, start + Math.floor(random() * size * 1.5)) : Math.floor(random() * total));
          if (dense && docs.size >= total - start) break;
        }
        const rows = [...docs].sort((a, b) => a - b).map(doc => [doc, 1 + Math.floor(random() * (random() < 0.5 ? 4 : 400))]);
        entries.push([term, rows]);
      }
      const externalBlocks = new Map();
      let packOffset = 0;
      const writeBlock = ({ term, blockIndex, bytes }) => {
        externalBlocks.set(`${term}\u0000${blockIndex}`, Buffer.from(bytes));
        const packIndex = blockIndex % 3;
        const range = {
          pack: `${String(packIndex).padStart(4, "0")}.f00d.bin.gz`,
          offset: packOffset,
          length: bytes.length,
          logicalLength: blockIndex % 2 ? bytes.length + 11 : 0
        };
        // The writer emits checksum fields only for a non-empty value and the
        // parser reads them whenever object pointers are on, so both agree.
        if (objectPointers) range.checksum = { algorithm: blockIndex % 7 === 3 ? "" : "sha256", value: sha256(bytes) };
        packOffset += bytes.length;
        return range;
      };
      const config = {
        postingBlockSize: [16, 32, 64, 128][Math.floor(random() * 4)],
        codecs: { mode: variant.mode },
        postingOrder: variant.order,
        postingSuperblockSize: 2 + Math.floor(random() * 4),
        postingImpactTiers: random() < 0.8,
        postingImpactTierMinBlocks: 2,
        postingImpactTierMaxBlocks: 6,
        postingDocRangeBlockMax: random() < 0.75 ? true : false,
        postingDocRangeSize: 64 + Math.floor(random() * 512),
        postingDocRangeQuantizationBits: 2 + Math.floor(random() * 10),
        postingImpactBucketOrderMinRows: random() < 0.5 ? 64 : 4096,
        externalPostingBlockMinBlocks: 3,
        externalPostingBlockMinBytes: 64
      };
      const segment = buildPostingSegment(entries, total, codes, filters, config, random() < 0.85 ? writeBlock : null);
      const manifest = {
        block_filters: filters,
        object_store: objectPointers
          ? { pointer_format: "rfbp-v1", pack_table: { postingBlocks: ["0000.f00d.bin.gz", ""] } }
          : { packs: { postingBlocks: ["0000.legacy.bin"] } }
      };
      cases.push(postingCaseFromSegment(`synthetic-${variant.mode}-${variant.order}-${objectPointers}`, segment.buffer, manifest, externalBlocks, random, total));
    }
  }
  // features.checksummedObjects instead of pointer_format; no block filters.
  {
    const random = rng(999);
    const entries = [["solo", [[0, 5], [3, 9], [70000, 2]]], ["deux", [[1, 1]]]];
    const externalBlocks = new Map();
    const segment = buildPostingSegment(entries, 80000, {}, [], { postingBlockSize: 1, codecs: { mode: "varint" }, externalPostingBlockMinBlocks: 2 }, ({ term, blockIndex, bytes }) => {
      externalBlocks.set(`${term}\u0000${blockIndex}`, Buffer.from(bytes));
      return { pack: "0007.x.bin", offset: blockIndex * 10, length: bytes.length, checksum: { value: sha256(bytes) } };
    });
    cases.push(postingCaseFromSegment("checksummed-objects-feature", segment.buffer, { features: { checksummedObjects: true } }, externalBlocks, random, 80000));
  }
  return cases;
}

// ---------------------------------------------------------------------------
// directories
// ---------------------------------------------------------------------------

function directoryKeys(random, count) {
  const keys = new Set();
  const alphabet = ["a", "b", "c", "x", "é", "ß", "日", "本", "語", "😀", "😁", "😂", "𠀀", "𠀁", "", "￿", "﻿", "-", "0", "9", "Z"];
  while (keys.size < count) {
    const length = 1 + Math.floor(random() * 6);
    let key = "";
    for (let i = 0; i < length; i++) key += alphabet[Math.floor(random() * alphabet.length)];
    keys.add(key);
  }
  for (const extra of ["😀", "😀a", "😁", "😁a", "😂😀", "﻿abc", "abc", "a﻿b", "𠀀x", "𠀁x"]) keys.add(extra);
  return [...keys];
}

function probes(random, keys) {
  const out = [...keys.slice(0, 200)];
  const extra = ["", "zzzz", "\u0000", "😀", "😃", "a", "日本", "￿￿", "𠀂", "abc", "abd"];
  out.push(...extra);
  for (let i = 0; i < 60; i++) out.push(`miss-${Math.floor(random() * 1e6)}`);
  return out.filter(key => !/[\ud800-\udfff]/u.test(key.replace(/[\ud800-\udbff][\udc00-\udfff]/gu, "")));
}

function directoryCases() {
  const cases = [];
  const configs = [
    { count: 0, pageBytes: 1024, bloom: 10 },
    { count: 1, pageBytes: 1024, bloom: 10 },
    { count: 40, pageBytes: 1024, bloom: 1 },
    { count: 300, pageBytes: 1024, bloom: 10 },
    { count: 700, pageBytes: 2048, bloom: 4 },
    { count: 120, pageBytes: 65536, bloom: 16 }
  ];
  let seed = 100;
  for (const config of configs) {
    const random = rng(seed++);
    const keys = config.count ? directoryKeys(random, config.count) : [];
    const packTable = ["0000.aa.bin.gz", "", "0002.cc.bin.gz"];
    const entries = keys.map((shard, i) => ({
      shard,
      packIndex: i % 5,
      offset: Math.floor(random() * 2 ** (8 + (i % 30))),
      length: Math.floor(random() * 70000),
      logicalLength: i % 3 ? Math.floor(random() * 200000) : 0,
      checksum: { algorithm: i % 4 ? "sha256" : "", value: sha256(Buffer.from(shard)) }
    }));
    const directory = buildPagedDirectory(entries, { pageBytes: config.pageBytes, bloomBitsPerKey: config.bloom });
    const root = parseDirectoryRoot(directory.root);
    const pages = directory.pages.map(page => {
      const parsed = parseDirectoryPage(page.buffer, { packTable });
      return {
        bytes: b64(page.buffer),
        entries: [...parsed.entries()].map(([key, pointer]) => [units(key), pointerJson(pointer)]),
        keys: [...parsed.keys()]
      };
    });
    const probeKeys = probes(random, keys);
    const probeResults = probeKeys.map(key => {
      const page = findDirectoryPage(root, key);
      return {
        key,
        mightContain: directoryMightContain(root, key),
        page: page ? root.pages.indexOf(page) : null,
        floorPage: floorDirectoryPageIndex(root, key),
        floorKeys: pages.map(page => floorSortedKeyIndex(page.keys, key))
      };
    });
    cases.push({
      name: `directory-${config.count}-${config.pageBytes}-${config.bloom}`,
      packTable,
      root: b64(directory.root),
      expectedRoot: { version: root.version, entries: root.entries, pages: root.pages, bloom: { bits: root.bloom.bits, hashes: root.bloom.hashes, bytes: b64(root.bloom.bytes) } },
      pages: pages.map(page => ({ bytes: page.bytes, entries: page.entries })),
      probes: probeResults
    });
  }
  const random = rng(77);
  const pairs = [];
  const sample = directoryKeys(random, 60);
  for (let i = 0; i < 300; i++) {
    const a = sample[Math.floor(random() * sample.length)];
    const b = sample[Math.floor(random() * sample.length)];
    pairs.push([a, b, compareDirectoryKeys(a, b)]);
  }
  return { cases, pairs };
}

// ---------------------------------------------------------------------------
// doc pointers / doc pages
// ---------------------------------------------------------------------------

function docPointerCases() {
  const cases = [];
  let seed = 200;
  for (const scale of [1, 300, 70000, 2 ** 33, 2 ** 45]) {
    for (const flavor of ["docs", "pages"]) {
      const random = rng(seed++);
      const packs = ["0000.a.bin.gz", "0001.b.bin.gz", "0002.c.bin.gz"];
      const packIndexes = new Map(packs.map((file, index) => [file, index]));
      const count = 1 + Math.floor(random() * 40);
      const entries = Array.from({ length: count }, (_, i) => ({
        pack: packs[i % packs.length],
        offset: Math.floor(random() * scale),
        length: Math.floor(random() * Math.min(scale, 2 ** 31)),
        logicalLength: i % 4 ? Math.floor(random() * scale) : 0,
        checksum: { algorithm: "sha256", value: sha256(Buffer.from(`doc-${seed}-${i}`)) }
      }));
      const table = flavor === "docs" ? buildDocPointerTable(entries, packIndexes) : buildDocPagePointerTable(entries, packIndexes);
      const parsed = flavor === "docs" ? parseDocPointerPage(table.buffer, { packTable: packs }) : parseDocPagePointerPage(table.buffer, { packTable: packs });
      const meta = JSON.parse(JSON.stringify(table.meta));
      const records = entries.map((_, i) => pointerJson(decodeDocPointerRecord(table.buffer, meta.dataOffset + i * meta.recordBytes, meta, packs)));
      const short = tryCall(() => (flavor === "docs" ? parseDocPointerPage(table.buffer, { packTable: packs.slice(0, 1) }) : parseDocPagePointerPage(table.buffer, { packTable: packs.slice(0, 1) })));
      cases.push({
        name: `pointers-${flavor}-${scale}`,
        flavor,
        bytes: b64(table.buffer),
        packTable: packs,
        meta,
        expected: { format: parsed.format, version: parsed.version, count: parsed.count, checksum_bytes: parsed.checksum_bytes, recordBytes: parsed.recordBytes, dataOffset: parsed.dataOffset, widths: parsed.widths, start: parsed.start, entries: parsed.entries.map(pointerJson) },
        records,
        shortPackTableError: short.error ?? null
      });
    }
  }
  return cases;
}

function docPageCases() {
  const cases = [];
  const random = rng(300);
  const nested = { b: 1, a: [1, "x", null, { "2": true, "10": false, z: 1.5, "1": "one" }], "7": "seven", "-1": "neg", "01": "lead" };
  const pool = [
    null, undefined, true, false, 0, -0, 1, -1, 0.1, 1e21, 1e-7, 123456789.125, -2.5e-300, 2 ** 53 + 2, Number.MAX_VALUE, NaN, Infinity,
    "", "hello", "﻿bom", "日本語", "😀 emoji", "tab\tnew\nline\"quote\\", " sep",
    [], ["a", "b"], ["", "﻿"], [1, 2], ["a", 1], [[1]], nested, { x: { y: { z: [] } } }, { "0": 0 }, {}
  ];
  const fieldSets = [
    ["title"],
    ["title", "body", "year", "tags"],
    ["index", "title"],
    ["0", "10", "b", "2", "__proto__", "z"],
    ["n", "s", "a", "o"]
  ];
  for (let c = 0; c < 40; c++) {
    const fields = fieldSets[c % fieldSets.length];
    const count = c === 0 ? 0 : 1 + Math.floor(random() * 12);
    const docs = Array.from({ length: count }, () => {
      const doc = {};
      for (const field of fields) {
        if (random() < 0.85) doc[field] = pool[Math.floor(random() * pool.length)];
      }
      return doc;
    });
    const bytes = encodeDocPageColumns(docs, fields);
    const start = Math.floor(random() * 5000);
    const decoded = decodeDocPageColumns(bytes, fields, start);
    cases.push({ name: `docpage-${c}`, bytes: b64(bytes), fields, start, expected: JSON.stringify(decoded) });
  }
  const sample = encodeDocPageColumns([{ title: "x" }], ["title"]);
  errorCase("decodeDocPageColumns", sample, { fields: ["a", "b"], start: 0 }, () => decodeDocPageColumns(sample, ["a", "b"], 0));
  errorCase("decodeDocPageColumns", Buffer.concat([Buffer.from(sample), Buffer.from([0])]), { fields: ["title"], start: 0 }, () => decodeDocPageColumns(Buffer.concat([Buffer.from(sample), Buffer.from([0])]), ["title"], 0));
  errorCase("decodeDocPageColumns", sample.subarray(0, sample.length - 1), { fields: ["title"], start: 0 }, () => decodeDocPageColumns(sample.subarray(0, sample.length - 1), ["title"], 0));
  const badType = Uint8Array.from([...sample.subarray(0, 7), 9]);
  errorCase("decodeDocPageColumns", badType, { fields: ["title"], start: 0 }, () => decodeDocPageColumns(badType, ["title"], 0));
  const shortNumber = Uint8Array.from([...encodeDocPageColumns([{ n: 1.5 }], ["n"]).subarray(0, 10)]);
  errorCase("decodeDocPageColumns", shortNumber, { fields: ["n"], start: 0 }, () => decodeDocPageColumns(shortNumber, ["n"], 0));
  return cases;
}

// ---------------------------------------------------------------------------
// doc values, codes, facet dictionaries
// ---------------------------------------------------------------------------

function docValueJson(value) {
  return value === undefined ? null : value;
}

function chunkJson(chunk) {
  return {
    name: chunk.name,
    kind: chunk.kind,
    type: chunk.type,
    width: chunk.width,
    words: chunk.words,
    encoding: chunk.encoding,
    start: chunk.start,
    count: chunk.count,
    min: chunk.min,
    values: chunk.values.map(docValueJson)
  };
}

function pushF64(out, value) {
  const buffer = Buffer.alloc(8);
  buffer.writeDoubleLE(value);
  out.push(...buffer);
}

function pushStr(out, value) {
  const bytes = Buffer.from(value, "utf8");
  pushVarint(out, bytes.length);
  out.push(...bytes);
}

function denseFacetChunk(version, name, words, width, start, rows) {
  const out = [0x52, 0x46, 0x56, 0x42];
  pushVarint(out, version);
  pushStr(out, name);
  out.push(1, 1, width);
  pushVarint(out, words);
  if (version >= 2) out.push(0);
  pushVarint(out, start);
  pushVarint(out, rows.length);
  pushF64(out, 0);
  for (const row of rows) {
    for (let w = 0; w < words; w++) {
      let value = row[w] || 0;
      for (let b = 0; b < width; b++) {
        out.push(value % 256);
        value = Math.floor(value / 256);
      }
    }
  }
  return Uint8Array.from(out);
}

function docValueCases() {
  const cases = [];
  const random = rng(400);
  const fields = [
    { name: "cat", kind: "facet", type: "keyword", words: 2 },
    { name: "big", kind: "facet", type: "keyword", words: 70 },
    { name: "year", kind: "number", type: "int" },
    { name: "views", kind: "number", type: "long" },
    { name: "when", kind: "number", type: "date" },
    { name: "price", kind: "number", type: "float" },
    { name: "ratio", kind: "number", type: "double" },
    { name: "flag", kind: "boolean", type: "boolean" },
    { name: "odd", kind: "number", type: "weird" },
    { name: "日本😀", kind: "number", type: "int" }
  ];
  for (const field of fields) {
    for (let c = 0; c < 8; c++) {
      const count = c === 0 ? 0 : 1 + Math.floor(random() * 300);
      const scale = [10, 300, 70000, 2 ** 33, 2 ** 50][c % 5];
      const rows = Array.from({ length: count }, () => {
        if (random() < 0.15) return null;
        if (field.kind === "facet") return { codes: Array.from({ length: Math.floor(random() * 4) }, () => Math.floor(random() * 64 * (field.words === 70 ? 30 : 1))) };
        if (field.kind === "boolean") return [true, false, "true", "0", 1, null, "x"][Math.floor(random() * 7)];
        if (field.type === "float" || field.type === "double") return random() < 0.05 ? NaN : (random() - 0.3) * scale;
        return Math.floor((random() - 0.2) * scale);
      });
      const start = Math.floor(random() * 100000);
      const chunk = buildDocValueChunk(field, start, rows);
      cases.push({ name: `dv-${field.name}-${c}`, bytes: b64(chunk.buffer), expected: chunkJson(parseDocValueChunk(chunk.buffer)) });
    }
  }
  for (const version of [1, 2]) {
    for (const width of [1, 2, 4]) {
      const rows = Array.from({ length: 20 }, () => Array.from({ length: 3 }, () => Math.floor(random() * 2 ** (8 * width))));
      const bytes = denseFacetChunk(version, "dense", 3, width, 40, rows);
      cases.push({ name: `dv-dense-v${version}-w${width}`, bytes: b64(bytes), expected: chunkJson(parseDocValueChunk(bytes)) });
    }
  }
  const good = buildDocValueChunk(fields[2], 0, [1, 2]).buffer;
  const badVersion = Uint8Array.from(good);
  badVersion[4] = 3;
  errorCase("parseDocValueChunk", badVersion, {}, () => parseDocValueChunk(badVersion));
  errorCase("parseDocValueChunk", Uint8Array.from([1, 2, 3]), {}, () => parseDocValueChunk(Uint8Array.from([1, 2, 3])));
  const floatChunk = buildDocValueChunk(fields[5], 0, [1.5, 2.5]).buffer;
  const truncated = Uint8Array.from(floatChunk.subarray(0, floatChunk.length - 3));
  errorCase("parseDocValueChunk", truncated, {}, () => parseDocValueChunk(truncated));
  return cases;
}

function codesJson(codes) {
  return {
    columns: Object.entries(codes).map(([name, values]) => [name, values.map(docValueJson)]),
    meta: codes._meta ?? null
  };
}

function codesCases() {
  const cases = [];
  const random = rng(500);
  for (let c = 0; c < 12; c++) {
    const total = c === 0 ? 0 : 1 + Math.floor(random() * 200);
    const dictSize = [1, 31, 33, 100][c % 4];
    const config = {
      facets: [{ name: "cat" }, { name: "tag" }],
      numbers: [{ name: "year", type: "int" }, { name: "price", type: "float" }, { name: "when", type: "DATE" }, { name: "n" }],
      booleans: [{ name: "flag" }]
    };
    const codes = {
      _dicts: { cat: { values: Array.from({ length: dictSize }) }, tag: { values: [1, 2] } },
      cat: Array.from({ length: total }, () => (random() < 0.2 ? null : random() < 0.5 ? { codes: [Math.floor(random() * dictSize), Math.floor(random() * dictSize)] } : Math.floor(random() * dictSize))),
      tag: Array.from({ length: total }, () => [Math.floor(random() * 2)]),
      year: Array.from({ length: total }, () => (random() < 0.2 ? null : Math.floor((random() - 0.5) * 10 ** (c % 7)))),
      price: Array.from({ length: total }, () => (random() < 0.2 ? null : random() * 1000)),
      when: Array.from({ length: total }, () => Math.floor(random() * 2 ** 40)),
      n: [],
      flag: Array.from({ length: total }, () => [true, false, null, "1"][Math.floor(random() * 4)])
    };
    const bytes = buildCodesFile(config, total, codes);
    cases.push({ name: `codes-${c}`, bytes: b64(bytes), expected: codesJson(parseCodes(bytes)) });
  }
  // Legacy layout: total, fieldCount, {name, width}, raw columns.
  for (const total of [3, 5]) {
    const out = [0x52, 0x46, 0x43, 0x42];
    pushVarint(out, total);
    pushVarint(out, 2);
    pushStr(out, "a");
    out.push(1);
    pushStr(out, "b");
    out.push(2);
    for (let i = 0; i < total; i++) out.push(i * 7);
    for (let i = 0; i < total; i++) out.push(i, 1);
    const bytes = Uint8Array.from(out);
    cases.push({ name: `codes-legacy-${total}`, bytes: b64(bytes), expected: codesJson(parseCodes(bytes)) });
  }
  errorCase("parseCodes", Uint8Array.from([0x52, 0x46, 0x43]), {}, () => parseCodes(Uint8Array.from([0x52, 0x46, 0x43])));
  return cases;
}

function facetDictCases() {
  const cases = [];
  const random = rng(600);
  for (let c = 0; c < 10; c++) {
    const values = Array.from({ length: c * 7 }, (_, i) => ({
      value: ["a", "b", "日本", "😀", "﻿x", ""][i % 6] + i,
      label: i % 3 ? `Label ${i} é` : "",
      n: Math.floor(random() * 2 ** (i % 40))
    }));
    const bytes = buildFacetDictionary(values);
    cases.push({ name: `facets-${c}`, bytes: b64(bytes), expected: parseFacetDictionary(bytes) });
  }
  const bad = buildFacetDictionary([]);
  bad[4] = 2;
  errorCase("parseFacetDictionary", bad, {}, () => parseFacetDictionary(bad));
  return cases;
}

// ---------------------------------------------------------------------------
// doc value sort trees
// ---------------------------------------------------------------------------

function sortPageJson(page) {
  return { name: page.name, kind: page.kind, type: page.type, rankStart: page.rankStart, count: page.count, rows: page.rows };
}

function sortDirectoryJson(directory) {
  return {
    format: directory.format,
    pageFormat: directory.pageFormat,
    field: directory.field,
    pageSize: directory.pageSize,
    total: directory.total,
    packTable: directory.packTable,
    summaryFields: directory.summaryFields,
    pages: directory.pages.map(page => ({ ...page, pack: page.pack ?? null }))
  };
}

function sortTreeCases() {
  const pages = [];
  const directories = [];
  const random = rng(700);
  const fields = [
    { name: "year", kind: "number", type: "int" },
    { name: "price", kind: "number", type: "double" },
    { name: "rate", kind: "number", type: "float" },
    { name: "flag", kind: "boolean", type: "boolean" },
    { name: "when", kind: "number", type: "" }
  ];
  for (const field of fields) {
    for (let c = 0; c < 6; c++) {
      const count = c === 0 ? 0 : 1 + Math.floor(random() * 200);
      const scale = [10, 1000, 2 ** 20, 2 ** 40][c % 4];
      const rows = Array.from({ length: count }, () => ({
        doc: Math.floor(random() * 2 ** (8 + c * 6)),
        value: field.kind === "boolean" ? random() < 0.5 : field.type === "double" || field.type === "float" ? (random() - 0.5) * scale : Math.floor((random() - 0.5) * scale)
      })).sort((a, b) => Number(a.value) - Number(b.value));
      const encoded = encodeDocValueSortPage(field, Math.floor(random() * 10000), rows);
      pages.push({ name: `sortpage-${field.name}-${c}`, bytes: b64(encoded.buffer), expectedName: c % 2 ? field.name : null, expected: sortPageJson(decodeDocValueSortPage(encoded.buffer, c % 2 ? { name: field.name } : {})) });
    }
    const packTable = ["0000.s.bin.gz", "0001.t.bin.gz"];
    const summaryFields = fields.slice(0, 3 + (field.name.length % 3));
    const pageMetas = Array.from({ length: 1 + Math.floor(random() * 30) }, (_, i) => ({
      rankStart: i * 512,
      count: 512,
      min: random() * 100,
      max: 100 + random() * 100,
      entry: {
        pack: packTable[i % 3] ?? "9999.unknown.bin",
        offset: Math.floor(random() * 2 ** 32),
        length: Math.floor(random() * 5000),
        physicalLength: i % 2 ? Math.floor(random() * 6000) : 0,
        logicalLength: i % 3 ? Math.floor(random() * 9000) : 0,
        checksum: i % 4 ? { algorithm: i % 8 === 1 ? "" : "sha256", value: sha256(Buffer.from(String(i))) } : null
      },
      summaries: Object.fromEntries(summaryFields.map((summaryField, k) => [summaryField.name, (i + k) % 3 ? { min: random() * 10, max: 10 + random() } : { min: null, max: 5 }]))
    }));
    const encoded = encodeDocValueSortDirectory({
      field,
      pageSize: 512,
      total: pageMetas.length * 512,
      pages: pageMetas,
      summaryFields,
      packTable,
      packIndexes: new Map(packTable.map((file, index) => [file, index]))
    });
    directories.push({ name: `sortdir-${field.name}`, bytes: b64(encoded.buffer), expected: sortDirectoryJson(parseDocValueSortDirectory(encoded.buffer)) });
  }
  const page = encodeDocValueSortPage(fields[0], 0, [{ doc: 1, value: 3 }]).buffer;
  errorCase("decodeDocValueSortPage", page, { name: "other" }, () => decodeDocValueSortPage(page, { name: "other" }));
  const trailing = Buffer.concat([page, Buffer.from([1])]);
  errorCase("decodeDocValueSortPage", trailing, { name: null }, () => decodeDocValueSortPage(trailing));
  const badEncoding = Buffer.from(page);
  badEncoding[4 + 1 + 5 + 7 + 4 + 1 + 1] = 9; // magic, version, "year", "number", "int", rankStart, count
  errorCase("decodeDocValueSortPage", badEncoding, { name: null }, () => decodeDocValueSortPage(badEncoding));
  return { pages, directories };
}

// ---------------------------------------------------------------------------
// doc-values manifest (binary + JSON)
// ---------------------------------------------------------------------------

function dvManifestCases() {
  const cases = [];
  const random = rng(800);
  for (let c = 0; c < 8; c++) {
    const total = 1 + Math.floor(random() * 5000);
    const chunkSize = [64, 100, 2048][c % 3];
    const lookup = c % 2 ? Math.max(1, Math.floor(chunkSize / 4)) : chunkSize;
    const packs = Array.from({ length: 1 + (c % 4) }, (_, i) => ({ file: `${String(i).padStart(4, "0")}.p${c}.bin.gz`, bytes: 100 }));
    const chunkList = size => {
      const out = [];
      for (let start = 0; start < total; start += size) {
        const kind = Math.floor(random() * 4);
        out.push({
          start,
          count: Math.min(size, total - start),
          pack: packs[Math.floor(random() * packs.length)].file,
          offset: Math.floor(random() * 2 ** 34),
          length: Math.floor(random() * 4000),
          physicalLength: 1,
          logicalLength: 2,
          checksum: { algorithm: "sha256", value: sha256(Buffer.from(`${c}-${start}-${size}`)) },
          width: 1 + Math.floor(random() * 8),
          min: kind === 0 ? null : (random() - 0.5) * 1e6,
          max: kind === 0 ? null : random() * 1e6,
          words: kind === 2 ? Array.from({ length: 1 + (c % 3) }, () => Math.floor(random() * 2 ** 32)) : null
        });
      }
      return out;
    };
    const fields = {};
    for (const [name, kind, type] of [["cat", "facet", "keyword"], ["year", "number", "int"], ["flag", "boolean", "boolean"], ["日本", "number", "float"]].slice(0, 1 + (c % 4))) {
      const chunks = chunkList(chunkSize);
      const lookupChunks = lookup < chunkSize ? chunkList(lookup) : null;
      fields[name] = { name, kind, type, words: kind === "facet" ? 3 : 0, chunks, ...(lookupChunks ? { lookup_chunks: lookupChunks } : {}) };
    }
    const manifest = { storage: "range-pack-v1", compression: "gzip-member", format: "rfdocvalues-v1", chunk_size: chunkSize, lookup_chunk_size: lookup, fields, packs };
    const checksums = c % 3 === 2 ? null : encodeDocValueChecksums(manifest);
    const bytes = encodeDocValueManifest(manifest, checksums ? { checksumsFile: `doc-values/checksums.${c}.bin`, checksumAlgorithm: checksums.algorithm } : {}, total);
    cases.push({ name: `dvm-${c}`, bytes: b64(bytes), expected: parseDocValueManifest(bytes), json: manifest, checksums: checksums ? b64(checksums.buffer) : null });
  }
  const bad = encodeDocValueManifest({ chunk_size: 1, fields: {}, packs: [] }, {}, 0);
  errorCase("parseDocValueManifest", Buffer.concat([Buffer.from(bad), Buffer.from([0])]), {}, () => parseDocValueManifest(Buffer.concat([Buffer.from(bad), Buffer.from([0])])));
  const badVersion = Uint8Array.from(bad);
  badVersion[4] = 7;
  errorCase("parseDocValueManifest", badVersion, {}, () => parseDocValueManifest(badVersion));
  return cases;
}

// ---------------------------------------------------------------------------
// filter bitmaps, query bundles, segments, text routing, http, checksums
// ---------------------------------------------------------------------------

function filterBitmapCases() {
  const cases = [];
  const random = rng(900);
  for (const total of [0, 1, 7, 8, 9, 100, 1000]) {
    const bytes = createFilterBitmap(total);
    for (let i = 0; i < total; i++) if (random() < 0.3) setFilterBitmapBit(bytes, i);
    const encoded = encodeFilterBitmap(total, bytes);
    const bitmap = parseFilterBitmap(encoded);
    const probes = [-5, -1, ...Array.from({ length: total + 12 }, (_, i) => i), 2 ** 31, 2 ** 33];
    cases.push({ name: `bitmap-${total}`, bytes: b64(encoded), total: bitmap.total, payload: b64(bitmap.bytes), probes: probes.map(doc => [doc, filterBitmapHas(bitmap, doc)]) });
  }
  const good = encodeFilterBitmap(3, Uint8Array.of(5));
  const badVersion = Buffer.from(good);
  badVersion[4] = 2;
  errorCase("parseFilterBitmap", badVersion, {}, () => parseFilterBitmap(badVersion));
  errorCase("parseFilterBitmap", Buffer.from("RFBX"), {}, () => parseFilterBitmap(Buffer.from("RFBX")));
  errorCase("parseFilterBitmap", good.subarray(0, 9), {}, () => parseFilterBitmap(good.subarray(0, 9)));
  return cases;
}

function bundleJson(bundle) {
  return { ...bundle };
}

function queryBundleCases() {
  const cases = [];
  const random = rng(1000);
  const manifest = {
    block_filters: [
      { name: "cat", kind: "facet", words: 2 },
      { name: "year", kind: "number", type: "int" },
      { name: "price", kind: "number", type: "FLOAT" },
      { name: "flag", kind: "boolean", type: "boolean" },
      { name: "when", kind: "number" }
    ]
  };
  for (let c = 0; c < 16; c++) {
    const rowCount = c === 0 ? 0 : 1 + Math.floor(random() * 40);
    const rows = Array.from({ length: rowCount }, () => ({ doc: Math.floor(random() * (c % 3 ? 100 : 100000)), score: Math.floor(random() * 5000) }));
    const rowGroups = Array.from({ length: c % 4 }, (_, i) => ({
      rowStart: i * 16,
      rowCount: 16,
      scoreMax: 900 - i,
      scoreMin: 10 + i,
      docMin: i,
      docMax: 1000 + i,
      filters: {
        cat: { words: [Math.floor(random() * 2 ** 32), 7] },
        year: { min: 1990, max: 2020 + i },
        price: i % 2 ? { min: null, max: null } : { min: 1.5, max: 99.25 },
        flag: { min: 1, max: 2 },
        when: { min: -5, max: 5 }
      }
    }));
    const filterValues = {};
    if (c % 2) {
      filterValues.cat = rows.map(() => (random() < 0.3 ? null : random() < 0.5 ? { codes: [1, 40] } : random() < 0.5 ? [3] : 9));
      filterValues.year = rows.map(() => (random() < 0.3 ? null : Math.floor((random() - 0.5) * 5000)));
      filterValues.price = rows.map(() => (random() < 0.3 ? undefined : (random() - 0.5) * 100));
      filterValues.flag = rows.map(() => [true, false, null][Math.floor(random() * 3)]);
      filterValues.when = rows.map(() => (random() < 0.2 ? "x" : -Math.floor(random() * 2 ** 40)));
    }
    const bundle = {
      key: c % 5 ? `key-${c}` : "日本 😀",
      baseTerms: ["alpha", "beta", "﻿gamma"].slice(0, c % 4),
      expandedTerms: ["alpha", "alphas"].slice(0, c % 3),
      total: Math.floor(random() * 1e6),
      complete: c % 2 === 0,
      nextScoreBound: Math.floor(random() * 100),
      nextTieDoc: c % 3 ? Math.floor(random() * 100) : null,
      rows,
      rowGroups,
      filterValues
    };
    let bytes = buildQueryBundle(bundle, manifest);
    if (c === 3) {
      // Older bundles end right after the rows.
      const head = buildQueryBundle({ ...bundle, rowGroups: [], filterValues: {} }, manifest);
      bytes = head.subarray(0, head.length - 2);
    }
    cases.push({ name: `bundle-${c}`, bytes: b64(bytes), manifest, expected: bundleJson(parseQueryBundle(bytes, manifest)) });
  }
  const bad = buildQueryBundle({ key: "", baseTerms: [], expandedTerms: [], total: 0, rows: [] }, manifest);
  bad[4] = 9;
  errorCase("parseQueryBundle", bad, {}, () => parseQueryBundle(bad, manifest));
  return cases;
}

async function segmentCases() {
  const cases = [];
  const random = rng(1100);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-codec-seg-"));
  try {
    for (let c = 0; c < 6; c++) {
      const termRows = [];
      const termCount = c === 0 ? 1 : 3 + Math.floor(random() * 20);
      for (let t = 0; t < termCount; t++) {
        const rows = Array.from({ length: 1 + Math.floor(random() * 60) }, () => [Math.floor(random() * 2 ** (6 + c * 5)), Math.floor(random() * 4000)]);
        termRows.push([["word", "日本", "😀", "é", "﻿b"][t % 5] + t, rows]);
      }
      const segment = await writeSegmentFromTermRows(dir, `seg-${c}`, termRows);
      const termsBytes = readFileSync(segment.termsPath);
      const postings = readFileSync(segment.postingsPath);
      const parsed = parseSegmentTerms(termsBytes);
      const terms = [...parsed.terms.values()];
      const decoded = [];
      for (const entry of terms) {
        const slice = postings.subarray(entry.offset, entry.offset + entry.bytes);
        for (const options of [{}, { df: 3, total: 1000 }, { df: 0, total: 0 }, { total: 5 }, { df: 1e6, total: 10 }]) {
          decoded.push({ term: entry.term, options, rows: arr(decodeSegmentRows(slice, entry, options)) });
        }
      }
      cases.push({ name: `segment-${c}`, terms: b64(termsBytes), postings: b64(postings), expected: terms, decoded });
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
  const bad = Uint8Array.from([0x52, 0x46, 0x53, 0x47, 0x54, 0x45, 0x52, 0x31, 0x81]);
  errorCase("parseSegmentTerms", bad, {}, () => parseSegmentTerms(bad));
  const badString = Uint8Array.from([0x52, 0x46, 0x53, 0x47, 0x54, 0x45, 0x52, 0x31, 1, 9, 0x61]);
  errorCase("parseSegmentTerms", badString, {}, () => parseSegmentTerms(badString));
  const rows = Uint8Array.from([1, 2, 3]);
  errorCase("decodeSegmentRows", rows, { term: "x", count: 1 }, () => decodeSegmentRows(rows, { term: "x", count: 1, df: 1 }));
  errorCase("decodeSegmentRows", rows, { term: "y", count: 2 }, () => decodeSegmentRows(rows, { term: "y", count: 2, df: 1 }));
  return cases;
}

function textRoutingCases() {
  const cases = [];
  const random = rng(1200);
  for (let c = 0; c < 10; c++) {
    const terms = c === 0 ? [] : directoryKeys(random, 5 + Math.floor(random() * 200));
    terms.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    const items = terms.map(term => {
      const shards = [...new Set(Array.from({ length: 1 + Math.floor(random() * 8) }, () => Math.floor(random() * (c % 2 ? 50 : 70000))))].sort((a, b) => a - b);
      return { term, shards };
    });
    const bytes = encodeTextRoutingSegment(items);
    const parsed = parseTextRoutingSegment(bytes);
    const probeKeys = probes(random, terms);
    cases.push({
      name: `textroute-${c}`,
      bytes: b64(bytes),
      terms: [...parsed.entries()].map(([term, shards]) => [units(term), shards]),
      probes: probeKeys.map(key => ({ key, shards: parsed.get(key) ?? null, floor: floorSortedKeyIndex([...parsed.keys()], key) }))
    });
  }
  return cases;
}

function multipart(boundary, parts, { newline = "\r\n", total = "1000" } = {}) {
  const chunks = [];
  for (const part of parts) {
    chunks.push(Buffer.from(`--${boundary}${newline}Content-Type: application/octet-stream${newline}${part.header ?? `Content-Range: bytes ${part.start}-${part.start + part.bytes.length - 1}/${total}`}${newline}${newline}`, "latin1"));
    chunks.push(part.bytes);
    chunks.push(Buffer.from(newline, "latin1"));
  }
  chunks.push(Buffer.from(`--${boundary}--${newline}`, "latin1"));
  return Buffer.concat(chunks);
}

function httpCases() {
  const cases = [];
  const random = rng(1300);
  const bytes = n => Buffer.from(Array.from({ length: n }, () => Math.floor(random() * 256)));
  const bodies = [];
  const p1 = { start: 0, bytes: bytes(10) };
  const p2 = { start: 100, bytes: bytes(50) };
  const p3 = { start: 4000, bytes: Buffer.from("--rf-boundary inside body\r\n\r\n") };
  bodies.push(["basic", multipart("rf-boundary", [p1, p2]), "multipart/byteranges; boundary=rf-boundary"]);
  bodies.push(["quoted", multipart("a b;c", [p1, p3]), 'multipart/byteranges; boundary="a b;c"']);
  bodies.push(["lf", multipart("xyz", [p2], { newline: "\n" }), "multipart/byteranges;boundary=xyz"]);
  bodies.push(["case", multipart("Q", [p1], { total: "*" }), "Multipart/ByteRanges; BOUNDARY=Q"]);
  bodies.push(["leading-semicolon", multipart("Q", [p1]), ";boundary=Q"]);
  bodies.push(["no-boundary", multipart("Q", [p1]), "multipart/byteranges"]);
  bodies.push(["wrong-boundary", multipart("Q", [p1]), "multipart/byteranges; boundary=R"]);
  bodies.push(["no-range", multipart("Q", [{ ...p1, header: "X-Other: 1" }]), "multipart/byteranges; boundary=Q"]);
  bodies.push(["bad-range", multipart("Q", [{ ...p1, header: "Content-Range: bytes 9-3/100" }]), "multipart/byteranges; boundary=Q"]);
  bodies.push(["too-long", multipart("Q", [{ ...p1, header: "Content-Range: bytes 0-99999/100000" }]), "multipart/byteranges; boundary=Q"]);
  bodies.push(["malformed", Buffer.from("--Q\r\nContent-Range: bytes 0-1/2"), "multipart/byteranges; boundary=Q"]);
  bodies.push(["nbsp-and-case", multipart("Q", [{ ...p1, header: "content-RANGE: BYTES 0-9/10" }]), "multipart/byteranges; boundary=Q"]);
  bodies.push(["second-header-line", multipart("Q", [{ ...p2, header: `X-A: 1\r\nContent-Range: bytes 100-149/*` }]), "multipart/byteranges; boundary=Q"]);
  bodies.push(["euro-boundary", multipart("€x", [p1]), "multipart/byteranges; boundary=€x"]);
  bodies.push(["unencodable-boundary", multipart("Q", [p1]), "multipart/byteranges; boundary=日本"]);
  bodies.push(["empty", Buffer.alloc(0), "multipart/byteranges; boundary=Q"]);
  bodies.push(["unterminated-quote", multipart('"abc', [p1]), 'multipart/byteranges; boundary="abc']);
  for (const [name, body, contentType] of bodies) {
    const ab = body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength);
    const parsed = tryCall(() => parseMultipartByteRanges(ab, contentType));
    const selections = [];
    if (parsed.ok) {
      const requests = [[[0, 10]], [[2, 3], [100, 50]], [[120, 5], [0, 1]], [[5, 10]], [[4000, 4]], [[0, 0]]];
      for (const request of requests) {
        const ranges = request.map(([offset, length]) => ({ offset, length }));
        const result = tryCall(() => selectMultipartByteRanges(parsed.ok, ranges).map(buffer => b64(buffer)));
        selections.push({ ranges: request, result: result.ok ?? null, error: result.error ?? null });
      }
    }
    cases.push({
      name,
      body: b64(body),
      contentType,
      parts: parsed.ok ? parsed.ok.map(part => ({ start: part.start, end: part.end, bytes: b64(part.buffer) })) : null,
      error: parsed.error ?? null,
      selections
    });
  }
  return cases;
}

async function verifyCases() {
  const cases = [];
  const data = Buffer.from("rangefind checksum payload");
  const good = sha256(data);
  const pointers = [
    ["good", { checksum: { algorithm: "sha256", value: good } }],
    ["default-algorithm", { checksum: { algorithm: "", value: good } }],
    ["mismatch", { checksum: { algorithm: "sha256", value: sha256(Buffer.from("other")) } }],
    ["missing", { checksum: null }],
    ["empty-value", { checksum: { algorithm: "sha256", value: "" } }],
    ["md5", { checksum: { algorithm: "md5", value: good } }]
  ];
  for (const [name, pointer] of pointers) {
    const result = await tryCallAsync(() => verifyBlockPointer(data, pointer, `object ${name}`));
    cases.push({ name, bytes: b64(data), checksum: pointer.checksum, label: `object ${name}`, error: result.error ?? null });
  }
  return cases;
}

// ---------------------------------------------------------------------------
// real builds
// ---------------------------------------------------------------------------

function gunzipFile(path) {
  const data = readFileSync(path);
  return data[0] === 0x1f && data[1] === 0x8b ? gunzipSync(data) : data;
}

function packSlice(path, pointer) {
  const data = readFileSync(path);
  return data.subarray(pointer.offset, pointer.offset + pointer.length);
}

function corpus(random, count) {
  const words = ["alpha", "beta", "gamma", "delta", "épée", "naïve", "日本", "語", "😀", "river", "stone", "light", "north", "south", "über"];
  const categories = Array.from({ length: 45 }, (_, i) => `cat-${i}`);
  return Array.from({ length: count }, (_, i) => {
    const title = Array.from({ length: 2 + Math.floor(random() * 4) }, () => words[Math.floor(random() * words.length)]).join(" ");
    const body = Array.from({ length: 5 + Math.floor(random() * 40) }, () => words[Math.floor(random() * words.length)]).join(" ");
    const doc = {
      id: `doc-${i}`,
      url: `/doc/${i}`,
      title: i % 17 === 0 ? `﻿${title}` : title,
      body,
      category: categories[Math.floor(random() * (i % 3 ? 45 : 5))],
      tags: Array.from({ length: Math.floor(random() * 3) }, () => words[Math.floor(random() * words.length)]),
      year: random() < 0.1 ? null : 1950 + Math.floor(random() * 75),
      price: random() < 0.1 ? null : Math.round(random() * 100000) / 100,
      published: `20${String(10 + Math.floor(random() * 15))}-0${1 + Math.floor(random() * 9)}-1${Math.floor(random() * 9)}`,
      featured: random() < 0.2 ? null : random() < 0.5,
      extra: i % 5 === 0 ? { nested: [1, 2, { "3": "x", a: "y" }] } : null
    };
    return doc;
  });
}

async function runBuild(name, random, count, overrides) {
  const root = mkdtempSync(join(tmpdir(), `rangefind-codec-${name}-`));
  writeFileSync(join(root, "docs.jsonl"), corpus(random, count).map(doc => JSON.stringify(doc)).join("\n"));
  writeFileSync(join(root, "rangefind.config.json"), JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    idPath: "id",
    urlPath: "url",
    display: ["id", "url", "title", "body", "category", "tags", "year", "price", "published", "featured", "extra"],
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: [{ name: "category", path: "category" }, { name: "tags", path: "tags" }],
    numbers: [{ name: "year", path: "year", type: "int" }, { name: "price", path: "price", type: "float" }, { name: "published", path: "published", type: "date" }],
    booleans: [{ name: "featured", path: "featured" }],
    ...overrides
  }));
  const log = console.log;
  const info = console.info;
  const warn = console.warn;
  console.log = () => {};
  console.info = () => {};
  console.warn = () => {};
  try {
    await build({ configPath: join(root, "rangefind.config.json") });
  } finally {
    console.log = log;
    console.info = info;
    console.warn = warn;
  }
  return { root, out: join(root, "public/rangefind") };
}

function harvestDirectory(out, directory) {
  const root = parseDirectoryRoot(gunzipFile(join(out, directory.root)));
  const entries = [];
  for (const page of root.pages) {
    const parsed = parseDirectoryPage(gunzipFile(join(out, directory.pages, page.file)), { packTable: directory.pack_table || [] });
    for (const [key, pointer] of parsed) entries.push([key, pointer]);
  }
  return entries;
}

async function harvestBuild(name, random, count, overrides, sinks) {
  const { root, out } = await runBuild(name, random, count, overrides);
  try {
    const manifest = JSON.parse(readFileSync(join(out, "manifest.json"), "utf8"));
    // Posting segments + external blocks.
    const termDirectory = manifest.object_store.directories.terms;
    const blockPacks = manifest.object_store.pack_table.postingBlocks || [];
    const postingManifest = { block_filters: manifest.block_filters, object_store: manifest.object_store, features: manifest.features };
    for (const [shard, pointer] of harvestDirectory(out, termDirectory)) {
      const buffer = gunzipSync(packSlice(join(out, "terms/packs", pointer.pack), pointer));
      const segment = parsePostingSegment(buffer, postingManifest);
      const externalBlocks = new Map();
      for (const [term, entry] of segment.terms) {
        if (!entry.external) continue;
        entry.blocks.forEach((block, index) => {
          externalBlocks.set(`${term}\u0000${index}`, gunzipSync(packSlice(join(out, "terms/block-packs", block.range.pack), block.range)));
        });
      }
      sinks.postings.push(postingCaseFromSegment(`build-${name}-${shard}`, buffer, postingManifest, externalBlocks, random, manifest.total));
    }
    void blockPacks;
    // Facet dictionaries.
    const facetManifest = JSON.parse(gunzipFile(join(out, "facets/manifest.json.gz")));
    for (const [, pointer] of harvestDirectory(out, facetManifest.directory)) {
      const bytes = gunzipSync(packSlice(join(out, "facets/packs", pointer.pack), pointer));
      sinks.facets.push({ name: `build-${name}-facet`, bytes: b64(bytes), expected: parseFacetDictionary(bytes) });
    }
    // Doc values: chunks + JSON/binary manifests.
    const dvJson = JSON.parse(gunzipFile(join(out, "doc-values/manifest.json.gz")));
    for (const field of Object.values(dvJson.fields)) {
      for (const chunk of [...field.chunks, ...(field.lookup_chunks || [])]) {
        const bytes = gunzipSync(packSlice(join(out, "doc-values/packs", chunk.pack), chunk));
        sinks.docValues.push({ name: `build-${name}-dv-${field.name}-${chunk.start}`, bytes: b64(bytes), expected: chunkJson(parseDocValueChunk(bytes)) });
      }
    }
    if (manifest.doc_values_binary?.file) {
      const bytes = gunzipFile(join(out, manifest.doc_values_binary.file));
      const parsed = parseDocValueManifest(bytes);
      const checksums = parsed.checksum_rows ? readFileSync(join(out, parsed.checksum_rows.file)) : null;
      sinks.dvManifests.push({ name: `build-${name}-dvm`, bytes: b64(bytes), expected: parsed, json: dvJson, checksums: checksums ? b64(checksums) : null, total: manifest.total });
    }
    // Sorted doc values.
    const sorted = JSON.parse(gunzipFile(join(out, "doc-values/sorted/manifest.json.gz")));
    for (const field of Object.values(sorted.fields || {})) {
      const dirBytes = gunzipFile(join(out, field.directory.file));
      const directory = parseDocValueSortDirectory(dirBytes);
      sinks.sortDirectories.push({ name: `build-${name}-sortdir-${field.name}`, bytes: b64(dirBytes), expected: sortDirectoryJson(directory) });
      for (const page of directory.pages) {
        const bytes = gunzipSync(packSlice(join(out, "doc-values/sorted-packs", page.pack), page));
        sinks.sortPages.push({ name: `build-${name}-sortpage-${field.name}-${page.index}`, bytes: b64(bytes), expectedName: field.name, expected: sortPageJson(decodeDocValueSortPage(bytes, { name: field.name })) });
      }
    }
    // Filter bitmaps.
    const bitmaps = JSON.parse(gunzipFile(join(out, "filter-bitmaps/manifest.json.gz")));
    for (const field of Object.values(bitmaps.fields || {})) {
      for (const [value, pointer] of Object.entries(field.values || {})) {
        const bytes = gunzipSync(packSlice(join(out, "filter-bitmaps/packs", pointer.pack), pointer));
        const bitmap = parseFilterBitmap(bytes);
        const probes = Array.from({ length: bitmap.total + 3 }, (_, i) => i - 1);
        sinks.bitmaps.push({ name: `build-${name}-bitmap-${field.name}-${value}`, bytes: b64(bytes), total: bitmap.total, payload: b64(bitmap.bytes), probes: probes.map(doc => [doc, filterBitmapHas(bitmap, doc)]) });
      }
    }
    // Doc pointers and doc pages.
    for (const [flavor, meta] of [["docs", manifest.docs.pointers], ["pages", manifest.docs.pages?.pointers]]) {
      if (!meta?.file) continue;
      const bytes = gunzipFile(join(out, meta.file));
      const parsed = flavor === "docs" ? parseDocPointerPage(bytes, { packTable: meta.pack_table }) : parseDocPagePointerPage(bytes, { packTable: meta.pack_table });
      const records = parsed.entries.map((_, i) => pointerJson(decodeDocPointerRecord(bytes, meta.dataOffset + i * meta.recordBytes, meta, meta.pack_table)));
      sinks.pointers.push({
        name: `build-${name}-pointers-${flavor}`,
        flavor,
        bytes: b64(bytes),
        packTable: meta.pack_table,
        meta,
        expected: { format: parsed.format, version: parsed.version, count: parsed.count, checksum_bytes: parsed.checksum_bytes, recordBytes: parsed.recordBytes, dataOffset: parsed.dataOffset, widths: parsed.widths, start: parsed.start, entries: parsed.entries.map(pointerJson) },
        records,
        shortPackTableError: null
      });
      if (flavor === "pages") {
        const pages = manifest.docs.pages;
        parsed.entries.forEach((pointer, pageIndex) => {
          const pageBytes = gunzipSync(packSlice(join(out, "docs/page-packs", pointer.pack), pointer));
          const decoded = decodeDocPageColumns(pageBytes, pages.fields, pageIndex * pages.page_size);
          sinks.docPages.push({ name: `build-${name}-docpage-${pageIndex}`, bytes: b64(pageBytes), fields: pages.fields, start: pageIndex * pages.page_size, expected: JSON.stringify(decoded) });
        });
      }
    }
    // Query bundles.
    const bundleDirectory = manifest.object_store.directories.queryBundles;
    if (bundleDirectory) {
      const bundleEntries = harvestDirectory(out, bundleDirectory);
      const stride = Math.max(1, Math.ceil(bundleEntries.length / 150));
      for (const [key, pointer] of bundleEntries.filter((_, i) => i % stride === 0)) {
        const bytes = gunzipSync(packSlice(join(out, "bundles/packs", pointer.pack), pointer));
        sinks.bundles.push({ name: `build-${name}-bundle-${key}`, bytes: b64(bytes), manifest: { block_filters: manifest.block_filters }, expected: bundleJson(parseQueryBundle(bytes, manifest)) });
      }
    }
    // Published build segments.
    const segManifestPath = join(out, "segments/manifest.json.gz");
    if (existsSync(segManifestPath)) {
      const segManifest = JSON.parse(gunzipFile(segManifestPath));
      for (const segment of segManifest.segments || []) {
        const termsPath = segment.files?.terms?.path;
        const postingsPath = segment.files?.postings?.path;
        if (!termsPath || !postingsPath) continue;
        const termsBytes = readFileSync(join(out, termsPath));
        const postings = readFileSync(join(out, postingsPath));
        const parsed = parseSegmentTerms(termsBytes);
        const terms = [...parsed.terms.values()];
        const decoded = terms.slice(0, 300).map(entry => ({
          term: entry.term,
          options: { df: entry.df + 1, total: manifest.total },
          rows: arr(decodeSegmentRows(postings.subarray(entry.offset, entry.offset + entry.bytes), entry, { df: entry.df + 1, total: manifest.total }))
        }));
        sinks.segments.push({ name: `build-${name}-segment-${segment.id}`, terms: b64(termsBytes), postings: b64(postings), expected: terms, decoded });
      }
    }
    // Directory roots/pages as-is.
    const termRoot = gunzipFile(join(out, termDirectory.root));
    const parsedRoot = parseDirectoryRoot(termRoot);
    sinks.directories.push({
      name: `build-${name}-terms-directory`,
      packTable: termDirectory.pack_table,
      root: b64(termRoot),
      expectedRoot: { version: parsedRoot.version, entries: parsedRoot.entries, pages: parsedRoot.pages, bloom: { bits: parsedRoot.bloom.bits, hashes: parsedRoot.bloom.hashes, bytes: b64(parsedRoot.bloom.bytes) } },
      pages: parsedRoot.pages.map(page => {
        const bytes = gunzipFile(join(out, termDirectory.pages, page.file));
        const parsed = parseDirectoryPage(bytes, { packTable: termDirectory.pack_table });
        return { bytes: b64(bytes), entries: [...parsed.entries()].map(([key, pointer]) => [units(key), pointerJson(pointer)]) };
      }),
      probes: []
    });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
}

// ---------------------------------------------------------------------------

const random = rng(4242);
const postings = syntheticPostingCases();
const { cases: directories, pairs } = directoryCases();
const pointers = docPointerCases();
const docPages = docPageCases();
const docValues = docValueCases();
const codes = codesCases();
const facets = facetDictCases();
const { pages: sortPages, directories: sortDirectories } = sortTreeCases();
const dvManifests = dvManifestCases();
const bitmaps = filterBitmapCases();
const bundles = queryBundleCases();
const segments = await segmentCases();
const textRouting = textRoutingCases();
const http = httpCases();
const verify = await verifyCases();

const sinks = { postings, directories, pointers, docPages, docValues, facets, sortPages, sortDirectories, dvManifests, bitmaps, bundles, segments };
await harvestBuild("small", random, 120, {
  docChunkSize: 4,
  postingBlockSize: 8,
  externalPostingBlockMinBlocks: 3,
  externalPostingBlockMinBytes: 16,
  codecs: { mode: "auto" },
  postingImpactTiers: true,
  postingImpactTierMinBlocks: 2,
  postingDocRangeBlockMax: true,
  postingDocRangeSize: 16,
  docValueChunkSize: 32,
  docValueLookupChunkSize: 8,
  docValueSortedPageSize: 16,
  queryBundles: true,
  queryBundleMinSeedDocs: 1,
  targetShardPostings: 40
}, sinks);
await harvestBuild("default", random, 400, {}, sinks);
await harvestBuild("impact", random, 300, {
  postingOrder: "impact",
  postingBlockSize: 16,
  codecs: { mode: "partitioned-deltas" },
  externalPostingBlocks: false,
  docValueChunkSize: 64,
  queryBundles: true
}, sinks);

for (const name of readdirSync(OUT)) rmSync(join(OUT, name));
write("postings", postings);
write("directories", { cases: directories, pairs });
write("doc_pointers", pointers);
write("doc_pages", docPages);
write("doc_values", docValues);
write("codes", codes);
write("facet_dictionaries", facets);
write("sort_pages", sortPages);
write("sort_directories", sortDirectories);
write("dv_manifests", dvManifests);
write("filter_bitmaps", bitmaps);
write("query_bundles", bundles);
write("segments", segments);
write("text_routing", textRouting);
write("http_ranges", http);
write("verify", verify);
write("errors", errors);
