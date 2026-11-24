// Parity fixtures for the `vector` group: src/vector_index.js (query side)
// and src/typo_main_index.js. Run from the repo root:
//   node rust/tools/fixtures/vector.mjs
// Writes JSON to rust/target/fixtures/vector/.
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyPermutation,
  decodeVectorClusterPage,
  dotInt8,
  encodeVectorClusterPage,
  encodeVectorRoot,
  nearestCentroid,
  normalizeVector,
  parseVectorRoot,
  quantizeVector,
  trainCentroids,
  trainDimensionPermutation,
  vectorFromValue
} from "../../../src/vector_index.js";
import {
  bestMainIndexTypoDistance,
  boundedDamerauLevenshtein,
  isTypoCorrectionToken,
  mainIndexTypoCandidateScore,
  mainIndexTypoProbeValues,
  ngramOverlap,
  normalizeMainIndexTypoOptions,
  typoMaxEditsFor
} from "../../../src/typo_main_index.js";

const here = dirname(fileURLToPath(import.meta.url));
const outDir = resolve(here, "../../target/fixtures/vector");
mkdirSync(outDir, { recursive: true });

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function gaussian(random) {
  const u = Math.max(random(), 1e-9);
  const v = random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

// Exact number encoding: JSON numbers for finite non-negative-zero values,
// tagged strings otherwise.
function F(x) {
  if (typeof x !== "number") return x;
  if (Number.isNaN(x)) return "NaN";
  if (x === Infinity) return "Infinity";
  if (x === -Infinity) return "-Infinity";
  if (Object.is(x, -0)) return "-0";
  return x;
}
const FA = array => Array.from(array, F);
const b64 = bytes => Buffer.from(bytes.buffer ? new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength) : bytes).toString("base64");

function capture(fn) {
  try {
    return { ok: fn() };
  } catch (error) {
    return { error: String(error.message) };
  }
}

function write(name, value) {
  writeFileSync(resolve(outDir, name), JSON.stringify(value));
}

// ---------------------------------------------------------------- vectors

function rootJson(root) {
  return {
    format: root.format,
    pageFormat: root.pageFormat,
    field: root.field,
    metric: root.metric,
    dims: root.dims,
    coarseDims: root.coarseDims,
    total: root.total,
    permutation: Array.from(root.permutation),
    packTable: root.packTable.map(pack => (pack === undefined ? null : pack)),
    clusterCount: root.clusterCount,
    centroidScales: FA(root.centroidScales),
    centroidCodes: Array.from(root.centroidCodes),
    clusters: root.clusters.map(cluster => ({
      index: cluster.index,
      count: cluster.count,
      pack: cluster.pack === undefined ? null : cluster.pack,
      offset: cluster.offset,
      length: cluster.length,
      physicalLength: cluster.physicalLength,
      logicalLength: cluster.logicalLength,
      checksum: cluster.checksum
    })),
    refine: root.refine
  };
}

function pageJson(page) {
  return {
    field: page.field,
    clusterIndex: page.clusterIndex,
    coarseDims: page.coarseDims,
    count: page.count,
    docs: Array.from(page.docs),
    ordinals: Array.from(page.ordinals),
    scales: FA(page.scales),
    codes: Array.from(page.codes)
  };
}

const FIELD_NAMES = ["embedding", "vec", "", "émbedding", "向量", "emoji😀field"];
const METRICS = ["cosine", "dot", ""];

function buildIndex(seed, { dims, coarseDims, docs, k, permute, field, metric, rowsPerPack, checksumMode }) {
  const random = mulberry32(seed);
  const raw = new Float32Array(docs * dims);
  for (let i = 0; i < docs; i++) {
    const vector = Float32Array.from({ length: dims }, () => gaussian(random) * (1 + (i % 3)));
    normalizeVector(vector);
    raw.set(vector, i * dims);
  }
  const permutation = permute ? trainDimensionPermutation(raw, dims) : Uint16Array.from({ length: dims }, (_, d) => d);
  const sample = new Float32Array(raw.length);
  const scratch = new Float32Array(dims);
  for (let i = 0; i < docs; i++) sample.set(applyPermutation(raw, i * dims, permutation, scratch), i * dims);
  const { centroids, clusterCount } = trainCentroids(sample, dims, k, { coarseDims, iterations: 3 });
  const rowBytes = 4 + dims;
  const refine = Buffer.alloc(docs * rowBytes);
  const buckets = Array.from({ length: clusterCount }, () => ({ docs: [], ordinals: [], scales: [], codes: [] }));
  const codesScratch = new Int8Array(dims);
  for (let i = 0; i < docs; i++) {
    const permuted = sample.subarray(i * dims, i * dims + dims);
    const scale = quantizeVector(permuted, codesScratch, 0);
    refine.writeFloatLE(scale, i * rowBytes);
    Buffer.from(codesScratch.buffer, 0, dims).copy(refine, i * rowBytes + 4);
    const cluster = nearestCentroid(permuted, 0, centroids, clusterCount, dims, coarseDims);
    const coarse = new Int8Array(coarseDims);
    const coarseScale = quantizeVector(permuted.subarray(0, coarseDims), coarse, 0);
    const bucket = buckets[cluster];
    // Doc ids are scrambled and large to exercise the u32 fields.
    bucket.docs.push((i * 2654435761) >>> 0);
    bucket.ordinals.push(i);
    bucket.scales.push(coarseScale);
    bucket.codes.push(coarse);
  }
  const packTable = ["pack-a", "pack-b.bin", "päck😀"];
  const packIndexes = new Map(packTable.map((pack, index) => [pack, index]));
  const pages = [];
  const clusters = [];
  for (let c = 0; c < clusterCount; c++) {
    const bucket = buckets[c];
    const codes = new Int8Array(bucket.docs.length * coarseDims);
    for (let i = 0; i < bucket.codes.length; i++) codes.set(bucket.codes[i], i * coarseDims);
    const encoded = encodeVectorClusterPage({ field, clusterIndex: c, coarseDims, docs: bucket.docs, ordinals: bucket.ordinals, scales: bucket.scales, codes });
    pages.push(encoded);
    const mode = (checksumMode + c) % 4;
    clusters.push({
      count: bucket.docs.length,
      entry: {
        pack: packTable[c % packTable.length],
        offset: c * 1000 + (seed % 97),
        length: encoded.length,
        physicalLength: mode === 1 ? encoded.length + 3 : undefined,
        logicalLength: mode === 2 ? 0 : encoded.length * 2,
        checksum: mode === 0 ? { algorithm: "sha256", value: "ab".repeat(32) }
          : mode === 1 ? { algorithm: "", value: "cd12" }
          : mode === 2 ? null
          : { algorithm: "blake3", value: "" }
      }
    });
  }
  const refinePacks = [];
  for (let i = 0; i * rowsPerPack < docs; i++) refinePacks.push(`refine-${i}.bin`);
  const root = encodeVectorRoot({
    field,
    dims,
    coarseDims,
    metric,
    total: docs,
    permutation: permute ? permutation : null,
    centroids,
    clusterCount,
    clusters,
    refine: { rowBytes, rowsPerPack, packs: refinePacks },
    packTable,
    packIndexes
  });
  return { root: root.buffer, pages, refine, rowBytes };
}

const indexes = [];
const vectorQueries = [];
const configs = [];
let seed = 1;
for (const dims of [1, 2, 3, 5, 8, 17, 32, 64]) {
  for (const permute of [false, true]) {
    for (const k of [1, 3, 7]) {
      const coarseDims = Math.max(1, Math.min(dims, Math.ceil(dims / (1 + (seed % 3)))));
      configs.push({
        dims,
        coarseDims,
        docs: 5 + ((seed * 13) % 60),
        k,
        permute,
        field: FIELD_NAMES[seed % FIELD_NAMES.length],
        metric: METRICS[seed % METRICS.length],
        rowsPerPack: 1 + (seed % 16),
        checksumMode: seed % 4,
        seed: seed++
      });
    }
  }
}
for (const config of configs) {
  const built = buildIndex(config.seed, config);
  const root = parseVectorRoot(built.root);
  const pages = built.pages.map(page => decodeVectorClusterPage(page, { name: root.field }));
  // Runtime-style scoring against real queries.
  const random = mulberry32(config.seed * 7919);
  const queries = [];
  for (let q = 0; q < 4; q++) {
    const value = Array.from({ length: config.dims }, () => gaussian(random) * (q + 1) * 3.7);
    const normalized = normalizeVector(vectorFromValue(value, config.dims));
    const query = applyPermutation(normalized, 0, root.permutation, new Float32Array(root.dims));
    const centroidScores = [];
    for (let c = 0; c < root.clusterCount; c++) {
      centroidScores.push(F(dotInt8(query, root.centroidCodes, c * root.dims, root.dims, root.centroidScales[c])));
    }
    const coarseScores = pages.map(page => {
      const out = [];
      for (let i = 0; i < page.count; i++) out.push(F(dotInt8(query, page.codes, i * page.coarseDims, page.coarseDims, page.scales[i])));
      return out;
    });
    const refineCodes = new Int8Array(built.refine.buffer, built.refine.byteOffset, built.refine.byteLength);
    const view = new DataView(built.refine.buffer, built.refine.byteOffset, built.refine.byteLength);
    const refineScores = [];
    for (let i = 0; i < config.docs; i++) {
      const at = i * built.rowBytes;
      refineScores.push(F(dotInt8(query, refineCodes, at + 4, root.dims, view.getFloat32(at, true))));
    }
    queries.push({ value, normalized: FA(normalized), query: FA(query), centroidScores, coarseScores, refineScores });
  }
  indexes.push({
    config,
    root: b64(built.root),
    rootParsed: rootJson(root),
    pages: built.pages.map((page, i) => ({ bytes: b64(page), parsed: pageJson(pages[i]) })),
    refine: b64(built.refine),
    queries
  });
}

// Corruptions: every truncation and several byte rewrites of small roots and
// pages, decoded with and without an expected field name.
const corruptRoots = [];
const corruptPages = [];
for (const config of configs.filter(c => c.dims <= 5 && c.docs <= 30).slice(0, 6)) {
  const built = buildIndex(config.seed, config);
  const rootBytes = new Uint8Array(built.root);
  const mutations = [];
  for (let len = 0; len <= rootBytes.length + 1; len++) {
    const bytes = new Uint8Array(len);
    bytes.set(rootBytes.subarray(0, Math.min(len, rootBytes.length)));
    mutations.push(bytes);
  }
  for (let pos = 0; pos < rootBytes.length; pos++) {
    for (const value of [0x00, 0x7f, 0x80, 0xff, rootBytes[pos] ^ 0x01]) {
      const bytes = Uint8Array.from(rootBytes);
      bytes[pos] = value;
      mutations.push(bytes);
    }
  }
  for (const bytes of mutations) {
    corruptRoots.push({ bytes: b64(bytes), result: (r => (r.error !== undefined ? r : { ok: rootJson(r.ok) }))(capture(() => parseVectorRoot(bytes))) });
  }
  for (const page of built.pages.slice(0, 2)) {
    const pageBytes = new Uint8Array(page);
    const pageMutations = [];
    for (let len = 0; len <= pageBytes.length + 1; len++) {
      const bytes = new Uint8Array(len);
      bytes.set(pageBytes.subarray(0, Math.min(len, pageBytes.length)));
      pageMutations.push(bytes);
    }
    for (let pos = 0; pos < pageBytes.length; pos++) {
      for (const value of [0x00, 0x80, 0xff, pageBytes[pos] ^ 0x02]) {
        const bytes = Uint8Array.from(pageBytes);
        bytes[pos] = value;
        pageMutations.push(bytes);
      }
    }
    for (const bytes of pageMutations) {
      for (const name of [undefined, "", config.field, "other"]) {
        corruptPages.push({
          bytes: b64(bytes),
          name: name ?? null,
          result: (r => (r.error !== undefined ? r : { ok: pageJson(r.ok) }))(capture(() => decodeVectorClusterPage(bytes, name === undefined ? {} : { name })))
        });
      }
    }
  }
}

// vectorFromValue over JSON-representable values.
const fromValueCases = [];
const random = mulberry32(424242);
const f32Bytes = values => {
  const out = Buffer.alloc(values.length * 4);
  values.forEach((v, i) => out.writeFloatLE(v, i * 4));
  return out;
};
const scalarPool = [
  0, 1, -1, 0.1, -0.5, 3.4028234663852886e38, 3.4028235677973366e38, 3.402823669209385e38, 1e39, -1e39, 1e-45, 1e-46, 1.401298464324817e-45,
  2.5e-45, 5e-324, 123456789.123, "1", " 2.5 ", " 3﻿", "\u00854", "0x1f", "0b101", "0o17", "-0x5", "1e3", ".5", "5.", "1_000", "",
  " ", "Infinity", "-Infinity", "+Infinity", "infinity", "nan", "abc", null, true, false, [], [7], [[8]], [1, 2], ["9"], [null], [true], {}, { a: 1 }
];
for (let i = 0; i < 200; i++) {
  const dims = 1 + (i % 6);
  const value = Array.from({ length: dims }, () => scalarPool[Math.floor(random() * scalarPool.length)]);
  fromValueCases.push({ value, dims });
  fromValueCases.push({ value: value.map(() => (random() - 0.5) * 10 ** Math.floor(random() * 80 - 40)), dims });
}
for (let dims = 0; dims <= 6; dims++) {
  fromValueCases.push({ value: Array.from({ length: dims }, (_, i) => i + 0.25), dims });
  fromValueCases.push({ value: Array.from({ length: dims + 1 }, (_, i) => i), dims });
  const vals = Array.from({ length: dims }, () => gaussian(random));
  const encoded = f32Bytes(vals);
  fromValueCases.push({ value: encoded.toString("base64"), dims });
  fromValueCases.push({ value: encoded.toString("base64url"), dims });
  fromValueCases.push({ value: encoded.toString("base64").replace(/=+$/, ""), dims });
  fromValueCases.push({ value: ` ${encoded.toString("base64").split("").join("\n")} `, dims });
  fromValueCases.push({ value: encoded.toString("base64") + "AAAA", dims });
  fromValueCases.push({ value: encoded.toString("base64") + "=AAAA", dims });
}
const nonFinite = f32Bytes([1, 2]);
nonFinite.writeUInt32LE(0x7fc00000, 4);
fromValueCases.push({ value: nonFinite.toString("base64"), dims: 2 });
nonFinite.writeUInt32LE(0x7f800000, 4);
fromValueCases.push({ value: nonFinite.toString("base64"), dims: 2 });
const b64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/-_=";
const junk = [" ", "\n", "\t", "!", "*", ".", "é", "Ł", "Ā", "ł", "Ń", "😀", "Ľ", "İ", " ", "Á", "\u0000"];
for (let i = 0; i < 600; i++) {
  const len = Math.floor(random() * 30);
  let text = "";
  for (let j = 0; j < len; j++) {
    text += random() < 0.85 ? b64Alphabet[Math.floor(random() * b64Alphabet.length)] : junk[Math.floor(random() * junk.length)];
  }
  for (const dims of [0, 1, 2, 3, 4, 5]) fromValueCases.push({ value: text, dims });
}
for (const value of [null, "", 5, true, false, { a: 1 }, "A", "AAAA"]) fromValueCases.push({ value, dims: 1 });
fromValueCases.push({ value: [], dims: 0 });
for (const item of fromValueCases) {
  const out = vectorFromValue(item.value, item.dims);
  item.result = out ? FA(out) : null;
}

// normalizeVector over f32 inputs (as f32 bit-exact numbers).
const normalizeCases = [];
for (let i = 0; i < 1500; i++) {
  const dims = 1 + Math.floor(random() * 70);
  const exp = Math.floor(random() * 90 - 45);
  const values = Float32Array.from({ length: dims }, () => (random() < 0.1 ? 0 : gaussian(random) * 10 ** exp));
  if (i % 50 === 0) values.fill(0);
  if (i % 77 === 0) values[0] = 3.4028234663852886e38;
  if (i % 91 === 0) values.fill(1e-45);
  const input = FA(values);
  const out = normalizeVector(Float32Array.from(values));
  normalizeCases.push({ input, result: out ? FA(out) : null });
}

// applyPermutation with offsets (including out-of-range reads).
const permutationCases = [];
for (let i = 0; i < 400; i++) {
  const dims = 1 + Math.floor(random() * 20);
  const offset = Math.floor(random() * 4);
  const extra = i % 10 === 0 ? -1 : Math.floor(random() * 3);
  const vector = Float32Array.from({ length: Math.max(0, offset + dims + extra) }, () => gaussian(random));
  const perm = Array.from({ length: dims }, (_, d) => d);
  for (let j = perm.length - 1; j > 0; j--) {
    const r = Math.floor(random() * (j + 1));
    [perm[j], perm[r]] = [perm[r], perm[j]];
  }
  const permutation = Uint16Array.from(perm);
  const targetLen = i % 13 === 0 ? Math.max(0, dims - 2) : dims;
  const target = applyPermutation(vector, offset, permutation, new Float32Array(targetLen));
  permutationCases.push({ vector: FA(vector), offset, permutation: perm, targetLen, result: FA(target) });
}

// dotInt8 over random queries and codes (out-of-range reads give NaN).
const dotCases = [];
for (let i = 0; i < 2000; i++) {
  const dims = Math.floor(random() * 80);
  const offset = Math.floor(random() * 5);
  const codesLen = offset + dims + (i % 17 === 0 ? -1 : Math.floor(random() * 3));
  const query = Float32Array.from({ length: dims + (i % 23 === 0 ? -1 : 0) }, () => gaussian(random) * (random() < 0.5 ? 1 : 1e-3));
  const codes = Int8Array.from({ length: Math.max(0, codesLen) }, () => Math.floor(random() * 255) - 127);
  if (i % 31 === 0) codes.fill(-128);
  const scale = Math.fround(random() < 0.05 ? 0 : random() * 0.02);
  dotCases.push({ query: FA(query), codes: Array.from(codes), offset, dims, scale: F(scale), result: F(dotInt8(query, codes, offset, dims, scale)) });
}

// normalizedQueryVector (runtime.js) semantics: errors when not normalizable.
function normalizedQueryVector(value, dims) {
  const vector = vectorFromValue(value, dims);
  const normalized = vector ? normalizeVector(vector) : null;
  if (!normalized) throw new Error(`Rangefind: vector queries need ${dims} finite dimensions.`);
  return normalized;
}
const queryVectorCases = fromValueCases.filter((_, i) => i % 3 === 0).map(item => ({
  value: item.value,
  dims: item.dims,
  result: (r => (r.error !== undefined ? r : { ok: FA(r.ok) }))(capture(() => normalizedQueryVector(item.value, item.dims)))
}));
queryVectorCases.push({ value: [0, 0], dims: 2, result: capture(() => normalizedQueryVector([0, 0], 2)) });

write("vector.json", { indexes, corruptRoots, corruptPages, fromValueCases, normalizeCases, permutationCases, dotCases, queryVectorCases });

// ---------------------------------------------------------------- typo

const BASE_WORDS = [
  "paris", "pari", "parisien", "changement", "chang", "climatique", "climat", "electrif", "elecrtif", "statik", "static", "sqlite",
  "boulangerie", "boulanger", "montreal", "montréal", "québec", "quebec", "équipement", "élèves", "garçon", "naïve", "cœur", "straße",
  "東京都", "北京", "大阪府立大学", "日本語", "한국어", "서울", "русский", "москва", "ελληνικά", "αθήνα", "עברית", "العربية", "हिन्दी",
  "𝐀𝐁𝐂", "𠀀𠀁𠀂", "a😀b", "😀😀😀", "x𝒳y", "abc", "ab", "a", "", "123", "abc123", "a1", "1abc", "test", "testing", "tester", "tests",
  "information", "informations", "nation", "national", "communication", "électrique", "électricité", "aaaa", "aaab", "abab", "baba",
  "xylophone", "rhythm", "queue", "zzzz", "ça", "ÇA", "İstanbul", "Ǆemal", "abcdefghijklmnopqrstuvwxyzabcdefg", "abcdefghijklmnopqrstuvwxyzabcdef",
  "áb", "école", "ab-cd", "ab cd", "ab_cd", "٣٤٥abc", "abc٣٤٥", "Ⅻabc", "abcⅫ", "ǅabc", "ʰabc"
];
const LETTERS = "abcdefghijklmnopqrstuvwxyzéèàçôü東京😀𝐀ж";
function mutate(word, rand) {
  const chars = [...word];
  const op = Math.floor(rand() * 5);
  const pos = Math.floor(rand() * (chars.length + 1));
  const letter = [...LETTERS][Math.floor(rand() * [...LETTERS].length)];
  if (op === 0 && chars.length) chars.splice(Math.min(pos, chars.length - 1), 1);
  else if (op === 1) chars.splice(pos, 0, letter);
  else if (op === 2 && chars.length) chars[Math.min(pos, chars.length - 1)] = letter;
  else if (op === 3 && chars.length > 1) {
    const i = Math.min(pos, chars.length - 2);
    [chars[i], chars[i + 1]] = [chars[i + 1], chars[i]];
  } else chars.push(["s", "es", "ement", "ique", "ations"][Math.floor(rand() * 5)]);
  return chars.join("");
}
const trand = mulberry32(777);
const words = [...BASE_WORDS];
for (let i = 0; i < 700; i++) {
  let word = BASE_WORDS[Math.floor(trand() * BASE_WORDS.length)];
  const steps = 1 + Math.floor(trand() * 3);
  for (let s = 0; s < steps; s++) word = mutate(word, trand);
  words.push(word);
}
// Raw UTF-16 splitting: surrogate halves are skipped here because the Rust
// API takes valid strings; typo helpers only ever see whole tokens.
const pairs = [];
for (let i = 0; i < 4000; i++) {
  const a = words[Math.floor(trand() * words.length)];
  let b;
  const r = trand();
  if (r < 0.4) b = mutate(a, trand);
  else if (r < 0.55) b = mutate(mutate(a, trand), trand);
  else if (r < 0.6) b = a;
  else b = words[Math.floor(trand() * words.length)];
  pairs.push([a, b]);
}
const dfPool = [0, 1, 2, 3, 7, 10, 20, 99, 1000, 123456, 1e9, 0.5, 1e-10, 2.5e-8, 4.2e15, 1e300];
const pairCases = pairs.map(([a, b], i) => {
  const maxDistance = i % 4;
  const maxEdits = 1 + (i % 3);
  const best = bestMainIndexTypoDistance(a, b, maxEdits);
  const df = dfPool[i % dfPool.length];
  const distance = i % 5;
  return {
    a,
    b,
    maxDistance,
    dl: boundedDamerauLevenshtein(a, b, maxDistance),
    maxEdits,
    best,
    overlap: F(ngramOverlap(a, b)),
    df,
    distance,
    score: F(mainIndexTypoCandidateScore(a, b, df, distance))
  };
});

const log1pInputs = [];
for (let i = 0; i < 3000; i++) log1pInputs.push(i);
for (let i = 0; i < 3000; i++) log1pInputs.push((trand() - 0.3) * 10 ** Math.floor(trand() * 40 - 20));
for (let i = 0; i < 1000; i++) log1pInputs.push(Math.floor(trand() * 2 ** 53));
log1pInputs.push(-1, -2, -0.9999999, 1e-30, -1e-30, 2 ** -29, 2 ** -54, 0.41421356, -0.29289321, 1e308, Infinity, -Infinity, NaN, -0, 0x3FDA827A);
const log1pCases = log1pInputs.map(x => [F(x), F(Math.log1p(x))]);

const tokenCases = words.concat(["ab1", "a12", "ⅫⅫⅫ", "Ⅻab", "̀abc", "ab̀c", "𝟙ab", "a𝟙b", "aaa", "a".repeat(32), "a".repeat(33), "é".repeat(32), "😀".repeat(16)])
  .map(token => ({ token, ok: isTypoCorrectionToken(token) }));

// Exhaustive \p{L} / \p{N} tables as code point ranges.
function ranges(test) {
  const out = [];
  let start = -1;
  for (let cp = 0; cp <= 0x110000; cp++) {
    const inside = cp < 0x110000 && !(cp >= 0xd800 && cp <= 0xdfff) && test(String.fromCodePoint(cp));
    if (inside && start < 0) start = cp;
    if (!inside && start >= 0) {
      out.push([start, cp - 1]);
      start = -1;
    }
  }
  return out;
}
const letterRanges = ranges(ch => /^\p{L}$/u.test(ch));
const numberRanges = ranges(ch => /^\p{N}$/u.test(ch));

const editCases = [];
for (const token of words) {
  for (const maxEdits of [0, 1, 2, 3, 5]) editCases.push({ token, maxEdits, result: typoMaxEditsFor(token, { maxEdits }) });
}

const probeCases = [];
const probeWords = words.filter((_, i) => i % 2 === 0);
for (let i = 0; i < probeWords.length; i++) {
  const term = probeWords[i];
  const raw = i % 3 === 0 ? term : i % 3 === 1 ? mutate(term, trand) : probeWords[(i * 7) % probeWords.length];
  for (const max of [0, 1, 2, 3, 5, 12, 24, 40, 200]) {
    probeCases.push({ raw, term, maxShardLookups: max, result: mainIndexTypoProbeValues(raw, term, { maxShardLookups: max }) });
  }
}

const optionValuePool = [
  undefined, null, 0, 1, 2, 3, 4, 5, 8, 12, 32, 33, 64, 100, -1, -0.5, 0.5, 1.9, 2.5, 7.999, 1e300, -1e300, "2", " 3 ", "﻿4 ", "\u00855",
  "0x10", "0b11", "0o7", "-0x2", "1e1", ".9", "2.", "abc", "", " ", "Infinity", "-Infinity", "NaN", true, false, [], [2], [[3]], ["4"], [1, 2], [null], [true], {}, { v: 2 },
  "OFF", "off", "False", "none", "None", "zero", "ZERO", "Zero", "zero-or-weak", "main-index", "weird", ["off"], [["zero"]], ["o", "ff"]
];
const pick = rand => optionValuePool[Math.floor(rand() * optionValuePool.length)];
const optionKeys = ["typoMode", "typoTrigger", "typoMaxEdits", "typoMaxTokenCandidates", "typoMaxQueryPlans", "typoMaxCorrectedSearches", "typoMaxShardLookups", "typoWeakResultTotal"];
const manifestKeys = ["mode", "trigger", "maxEdits", "maxTokenCandidates", "maxQueryPlans", "maxCorrectedSearches", "maxShardLookups", "weakResultTotal"];
const optionCases = [];
const orand = mulberry32(99);
const clean = value => JSON.parse(JSON.stringify(value === undefined ? null : value));
for (let i = 0; i < 2500; i++) {
  const options = {};
  const typo = {};
  for (const key of optionKeys) if (orand() < 0.35) options[key] = pick(orand);
  for (const key of manifestKeys) if (orand() < 0.35) typo[key] = pick(orand);
  let manifest;
  const shape = i % 10;
  if (shape === 0) manifest = {};
  else if (shape === 1) manifest = { search: null };
  else if (shape === 2) manifest = { search: { typo: "str" } };
  else if (shape === 3) manifest = { search: { typo: [1, 2] } };
  else if (shape === 4) manifest = { search: "x" };
  else if (shape === 5) manifest = { search: { typo: null } };
  else manifest = { search: { typo } };
  // Round-trip through JSON so undefined members vanish exactly as they do
  // for the Rust side (undefined is only represented by absence).
  const jsonOptions = clean(options);
  const jsonManifest = clean(manifest);
  optionCases.push({ options: jsonOptions, manifest: jsonManifest, result: normalizeMainIndexTypoOptions(jsonOptions, jsonManifest) });
}
optionCases.push({ options: {}, manifest: {}, result: normalizeMainIndexTypoOptions({}, {}) });

write("typo.json", { pairCases, log1pCases, tokenCases, letterRanges, numberRanges, editCases, probeCases, optionCases });

console.log(
  `vector fixtures: ${indexes.length} indexes, ${corruptRoots.length} corrupt roots, ${corruptPages.length} corrupt pages, `
  + `${fromValueCases.length} fromValue, ${normalizeCases.length} normalize, ${permutationCases.length} permutation, ${dotCases.length} dot, `
  + `${queryVectorCases.length} query vectors; typo: ${pairCases.length} pairs, ${log1pCases.length} log1p, ${tokenCases.length} tokens, `
  + `${editCases.length} maxEdits, ${probeCases.length} probes, ${optionCases.length} options -> ${outDir}`
);
