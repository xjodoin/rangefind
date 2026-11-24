#!/usr/bin/env node
// Engine-level parity fixtures for the Rust query engine.
//
// Builds a matrix of real Rangefind indexes (different corpora and builder
// configs covering every runtime lane), runs a large query matrix through the
// JavaScript runtime, and records the responses. The Rust engine's parity
// tests (rust/rangefind/tests/engine_parity.rs) and the WebAssembly tests
// (rust/tools/wasm/*.mjs) replay the same queries against the same files and
// must reproduce the recorded responses.
//
// Usage (from the repo root):
//   node rust/tools/parity/build_fixtures.mjs [--only=name,name] [--out=dir]
//
// Output: rust/target/parity/<index>/ (the index files) and
//         rust/target/parity/<index>.cases.json (queries + JS responses).

// V8 reads zone-less date-times and legacy date strings as local time; the
// Rust port reads them as UTC. Record every response in UTC (Node applies a
// runtime TZ change to its date cache).
process.env.TZ = "UTC";

import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { build } from "../../../src/builder.js";
import { createNodeSearch, resetNodeRuntimeCaches } from "../../../src/runtime.node.js";
import { addressCases, buildAddressIndex } from "./extra/address.mjs";
import { buildLegacyTitleIndex, legacyTitleCases } from "./extra/legacy_title.mjs";
import { dateParseCases } from "./extra/dates.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "../../..");
const args = Object.fromEntries(process.argv.slice(2).map(arg => {
  const [key, value = "true"] = arg.replace(/^--/, "").split("=");
  return [key, value];
}));
const outRoot = resolve(args.out || join(repo, "rust/target/parity"));
const only = args.only ? new Set(args.only.split(",")) : null;

// ---------------------------------------------------------------- corpora

function rng(seed) {
  let state = seed >>> 0 || 1;
  return () => {
    state ^= state << 13;
    state >>>= 0;
    state ^= state >>> 17;
    state ^= state << 5;
    state >>>= 0;
    return state / 0x100000000;
  };
}

const EN = ("search engine static index range request posting block impact score query document title body "
  + "river mountain city museum bakery library station garden bridge castle forest harbor island valley "
  + "history science music painting theater festival market railway airport school hospital university "
  + "ancient modern famous national public central northern southern eastern western quick brown fox "
  + "jumps lazy dog running runner runs ran cities museums libraries stations gardens bridges castles").split(" ");
const FR = ("musée boulangerie bibliothèque gare jardin pont château forêt port île vallée histoire musique "
  + "peinture théâtre marché école hôpital université ancien célèbre nationale centrale rivière montagne "
  + "élève étudiants françaises naïve cœur").split(" ");
const OTHER = ("Straße München Köln Zürich Москва библиотека музей Αθήνα μουσείο 東京 博物館 図書館 서울 도서관 "
  + "القاهرة متحف 北京 大学 カフェ ラーメン naïve café résumé").split(" ");
const CATEGORIES = ["architecture", "indexing", "runtime", "geo", "culture", "transport", "science", "food"];
const TAGS = ["static", "range", "fast", "cdn", "browser", "mobile", "offline", "local", "open", "edge", "wasm", "rust"];

function sentence(rand, words, min, max) {
  const count = min + Math.floor(rand() * (max - min + 1));
  const out = [];
  for (let i = 0; i < count; i++) {
    // Zipf-ish: favor the head of the vocabulary so terms get long posting lists.
    const index = Math.floor(words.length * rand() ** 2.2);
    out.push(words[index]);
  }
  return out.join(" ");
}

function syntheticDocs(count, seed, { geo = false, vectors = 0, multilingual = true } = {}) {
  const rand = rng(seed);
  const docs = [];
  for (let i = 0; i < count; i++) {
    const vocab = multilingual && rand() < 0.25 ? (rand() < 0.6 ? FR : OTHER) : EN;
    const doc = {
      id: `doc-${i}`,
      url: `/doc/${i}`,
      title: sentence(rand, vocab, 2, 6),
      body: sentence(rand, vocab, 8, 40),
      category: CATEGORIES[Math.floor(rand() * CATEGORIES.length)],
      tags: [...new Set(Array.from({ length: 1 + Math.floor(rand() * 3) }, () => TAGS[Math.floor(rand() * TAGS.length)]))],
      year: 1990 + Math.floor(rand() * 36),
      temperature: Math.floor(rand() * 80) - 30,
      published: new Date(Date.UTC(2000 + Math.floor(rand() * 26), Math.floor(rand() * 12), 1 + Math.floor(rand() * 28))).toISOString().slice(0, 10),
      featured: rand() < 0.2,
      popularity: Math.round(rand() * 1000) / 1000
    };
    if (rand() < 0.05) delete doc.year;
    if (rand() < 0.05) delete doc.featured;
    if (geo) {
      // Clusters around a few cities plus scattered points.
      const centers = [[45.5017, -73.5673], [46.8139, -71.208], [48.8566, 2.3522], [49.6116, 6.1319], [-17.7134, 178.065]];
      const [clat, clon] = centers[Math.floor(rand() * centers.length)];
      doc.lat = Math.round((clat + (rand() - 0.5) * 0.4) * 1e6) / 1e6;
      doc.lon = Math.round((clon + (rand() - 0.5) * 0.4) * 1e6) / 1e6;
      if (rand() < 0.03) {
        delete doc.lat;
        delete doc.lon;
      }
      doc.name = doc.title;
    }
    if (vectors) {
      const topic = i % 7;
      doc.embedding = Array.from({ length: vectors }, (_, d) => Math.round(((d % 7 === topic ? 1 : 0) + (rand() - 0.5) * 0.4) * 1000) / 1000);
      if (i % 97 === 0) delete doc.embedding;
    }
    docs.push(doc);
  }
  return docs;
}

const FIELDS = [
  { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true, phraseWeight: 8 },
  { name: "body", path: "body", weight: 1.0, b: 0.75 },
  { name: "category", path: "category", weight: 1.4, b: 0 }
];
const FACETS = [{ name: "category", path: "category" }, { name: "tags", path: "tags" }];
const NUMBERS = [
  { name: "year", path: "year", type: "int" },
  { name: "temperature", path: "temperature", type: "int" },
  { name: "published", path: "published", type: "date" },
  { name: "popularity", path: "popularity", type: "double" }
];
const BOOLEANS = [{ name: "featured", path: "featured" }];
const DISPLAY = ["id", "url", "title", "category", "tags", "year", "temperature", "published", "featured", { name: "snippet", path: "body", maxChars: 40 }];

const base = {
  input: "docs.jsonl",
  output: "index",
  idPath: "id",
  urlPath: "url",
  fields: FIELDS,
  facets: FACETS,
  numbers: NUMBERS,
  booleans: BOOLEANS,
  display: DISPLAY,
  buildTelemetrySampleMs: 0,
  resumeBuild: false
};

// ------------------------------------------------------- federated corpora

const GENERATION_CONFIG = {
  ...base,
  geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
  vectors: [{ name: "embedding", path: "embedding", dims: 16 }],
  vectorClusterTargetDocs: 64,
  suggest: [{ path: "title" }],
  display: [...DISPLAY, "lat", "lon"]
};

function renumber(docs, ids) {
  return docs.map((doc, i) => ({ ...doc, id: ids(i), url: `/doc/${ids(i)}` }));
}

// Base (700 docs), delta 1 (replaces 40 base docs, adds 150), delta 2
// (replaces 20 base docs and 10 delta-1 docs, adds 100).
function generationBatches() {
  const opts = { geo: true, vectors: 16 };
  const first = syntheticDocs(700, 21, opts);
  const second = renumber(syntheticDocs(190, 22, opts), i => (i < 40 ? `doc-${i * 7}` : `doc-${700 + i - 40}`));
  const third = renumber(syntheticDocs(130, 23, opts), i => (
    i < 20 ? `doc-${i * 11 + 3}` : i < 30 ? `doc-${700 + (i - 20) * 5}` : `doc-${850 + i - 30}`
  ));
  return [first, second, third];
}

async function writeDocs(file, docs) {
  await writeFile(file, `${docs.map(doc => JSON.stringify(doc)).join("\n")}\n`);
}

async function quietBuild(options) {
  const log = console.log;
  const warn = console.warn;
  console.log = () => {};
  console.warn = () => {};
  try {
    await build(options);
  } finally {
    console.log = log;
    console.warn = warn;
  }
}

async function buildGenerations(root, index) {
  const configPath = join(root, "rangefind.config.json");
  await writeFile(configPath, JSON.stringify(index.config, null, 2));
  const batches = generationBatches();
  for (let i = 0; i < batches.length; i++) {
    await writeDocs(join(root, "docs.jsonl"), batches[i]);
    await quietBuild({ configPath, ...(i > 0 ? { update: true } : {}) });
  }
}

const SHARDED_CONFIG = {
  ...base,
  geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
  vectors: [{ name: "embedding", path: "embedding", dims: 16 }],
  vectorClusterTargetDocs: 64,
  suggest: [{ path: "title" }],
  authority: [{ name: "title", path: "title" }],
  display: [...DISPLAY, "lat", "lon"]
};

// Shards split the synthetic geo corpus by cluster. The europe shard is
// generational: a base batch plus a delta that replaces and adds docs.
function shardedBatches() {
  const docs = syntheticDocs(2400, 31, { geo: true, vectors: 16 });
  const region = doc => {
    if (doc.lat == null) return "europe";
    if (doc.lat < 0) return "fiji";
    return doc.lon < 0 ? "quebec" : "europe";
  };
  const by = { quebec: [], europe: [], fiji: [] };
  for (const doc of docs) by[region(doc)].push(doc);
  const europeBase = by.europe.slice(0, by.europe.length - 120);
  const replaced = renumber(syntheticDocs(400, 32, { geo: true, vectors: 16 }).filter(doc => doc.lat != null && doc.lat > 0 && doc.lon > 0).slice(0, 25), i => europeBase[i * 9].id);
  const europeDelta = [...replaced, ...by.europe.slice(by.europe.length - 120)];
  return [
    { id: "quebec", groups: ["north-america", "world"], batches: [by.quebec] },
    { id: "europe", groups: ["europe", "world"], batches: [europeBase, europeDelta] },
    // Overlapping shards: a few Quebec docs also live in the Fiji shard, so
    // the merge must collapse duplicate public ids.
    { id: "fiji", groups: ["oceania", "world"], batches: [[...by.fiji, ...by.quebec.slice(0, 15)]] }
  ];
}

async function buildSharded(root, index) {
  const { readConfig } = await import("../../../src/config.js");
  const { collectScoringStats } = await import("../../../src/scoring_stats.js");
  const { writeShardedRootManifest, writeTextRoutingIndex, writeSuggestRoutingIndex } = await import("../../../src/index_shards.js");
  const shards = shardedBatches();
  for (const shard of shards) await writeDocs(join(root, `${shard.id}.jsonl`), shard.batches.flat().filter(doc => shard.id !== "fiji" || doc.lat < 0));
  await writeFile(join(root, "template.config.json"), JSON.stringify(index.config));
  const template = await readConfig(join(root, "template.config.json"));
  const log = console.log;
  console.log = () => {};
  let stats;
  try {
    stats = await collectScoringStats({
      config: template,
      inputs: shards.map(shard => ({ id: shard.id, input: join(root, `${shard.id}.jsonl`) })),
      outDir: join(root, "scoring-stats")
    });
  } finally {
    console.log = log;
  }
  for (const shard of shards) {
    const configPath = join(root, `${shard.id}.config.json`);
    await writeFile(configPath, JSON.stringify({
      ...index.config,
      input: `${shard.id}.batch.jsonl`,
      output: `index/shards/${shard.id}`,
      scoringStats: "scoring-stats/scoring-stats.json"
    }));
    for (let i = 0; i < shard.batches.length; i++) {
      await writeDocs(join(root, `${shard.id}.batch.jsonl`), shard.batches[i]);
      await quietBuild({ configPath, ...(i > 0 ? { update: true } : {}) });
    }
    await rm(join(root, "index", "shards", shard.id, "_build"), { recursive: true, force: true });
  }
  const inputs = shards.map(shard => ({ id: shard.id, dir: join(root, `index/shards/${shard.id}`) }));
  const textRouting = await writeTextRoutingIndex({ outDir: join(root, "index"), shards: inputs, segmentTerms: 32 });
  const suggestRouting = await writeSuggestRoutingIndex({ outDir: join(root, "index"), shards: inputs });
  writeShardedRootManifest({
    outDir: join(root, "index"),
    shards: shards.map(shard => ({
      id: shard.id,
      path: `shards/${shard.id}/`,
      bbox: stats.stats.inputs.find(item => item.id === shard.id)?.bbox,
      groups: shard.groups
    })),
    scoringStats: stats.stats,
    textRouting,
    suggestRouting
  });
}

const INDEXES = [
  {
    name: "basic",
    docs: () => syntheticDocs(400, 1),
    config: { ...base, docValueChunkSize: 64, docValueLookupChunkSize: 64, targetShardPostings: 200 }
  },
  {
    // Tiny blocks + external posting blocks + doc-range/impact-tier metadata:
    // forces the block-max tail proof, frontier batching and range planner.
    name: "blocks",
    docs: () => syntheticDocs(2500, 2),
    config: {
      ...base,
      postingBlockSize: 16,
      postingSuperblockSize: 4,
      postingDocRangeBlockMax: true,
      postingDocRangeSize: 128,
      postingImpactTiers: true,
      postingImpactTierMinBlocks: 2,
      externalPostingBlockMinBlocks: 2,
      externalPostingBlockMinBytes: 0,
      baseShardDepth: 2,
      maxShardDepth: 4,
      targetShardPostings: 400,
      docValueChunkSize: 128,
      docValueLookupChunkSize: 128,
      docPageSize: 16
    }
  },
  {
    name: "codecs",
    docs: () => syntheticDocs(1500, 3),
    config: { ...base, codecs: { mode: "auto" }, postingBlockSize: 32, externalPostingBlockMinBlocks: 3, externalPostingBlockMinBytes: 0 }
  },
  {
    // Query bundles, authority keys, sort replicas, typo correction.
    name: "features",
    docs: () => syntheticDocs(1200, 4),
    config: {
      ...base,
      queryBundles: true,
      queryBundleMinSeedDocs: 1,
      authority: [{ name: "title", path: "title" }],
      sortReplicas: [{ field: "year", order: "desc" }, { field: "temperature", order: "asc" }],
      typoMode: "main-index",
      postingBlockSize: 32
    },
    cases: featuresCases
  },
  {
    name: "rankprior",
    docs: () => syntheticDocs(800, 5),
    config: { ...base, rankPrior: { field: "popularity", boost: 0.5 } }
  },
  {
    name: "geo",
    docs: () => syntheticDocs(3000, 6, { geo: true }),
    config: {
      ...base,
      geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
      geoLeafSize: 64,
      geoCellIndexes: [{ field: "location", facet: "category", levels: [6, 9, 12] }],
      suggest: [{ path: "name" }],
      display: [...DISPLAY, "lat", "lon"]
    }
  },
  {
    name: "geocapsules",
    docs: () => syntheticDocs(1500, 7, { geo: true }),
    config: {
      ...base,
      geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
      geoLeafSize: 32,
      geoCapsules: true,
      geoCapsuleFields: ["id", "title", "category"],
      display: [...DISPLAY, "lat", "lon"]
    }
  },
  {
    // Two-level geo tree (branch pages), category cells with a wildcard
    // code: covers branch traversal, cell routing and route ordering, which
    // single-level trees never reach.
    name: "geo2",
    docs: () => syntheticDocs(6000, 16, { geo: true }),
    deepGeo: true,
    config: {
      ...base,
      geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
      geoLeafSize: 16,
      geoBranchLeaves: 4,
      geoCellIndexes: [{ field: "location", facet: "category", levels: [6, 9, 12], includeAll: true }],
      display: [...DISPLAY, "lat", "lon"]
    }
  },
  {
    name: "geo2c",
    docs: () => syntheticDocs(4000, 17, { geo: true }),
    deepGeo: true,
    config: {
      ...base,
      geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
      geoLeafSize: 16,
      geoBranchLeaves: 4,
      geoCapsules: true,
      geoCapsuleFields: ["id", "title", "category"],
      display: [...DISPLAY, "lat", "lon"]
    }
  },
  {
    name: "vectors",
    docs: () => syntheticDocs(900, 8, { vectors: 16 }),
    config: { ...base, vectors: [{ name: "embedding", path: "embedding", dims: 16 }], vectorClusterTargetDocs: 64 }
  },
  {
    name: "suggest",
    docs: () => syntheticDocs(1000, 9, { geo: true }),
    config: {
      ...base,
      suggest: [{ path: "title" }],
      authority: [{ name: "title", path: "title" }],
      geo: [{ name: "location", latPath: "lat", lonPath: "lon" }]
    }
  },
  {
    name: "segments",
    docs: () => syntheticDocs(1200, 10),
    config: { ...base, segmentFlushDocs: 200, segmentMergeFanIn: 2, finalSegmentTargetCount: 0 }
  },
  {
    // Generational index (`build --update`): a base generation plus two
    // deltas that add documents and replace earlier ones (tombstones in
    // generations 0 and 1). Exercises createGenerationalSearch.
    name: "generations",
    docs: () => generationBatches().flat(),
    config: GENERATION_CONFIG,
    build: buildGenerations,
    cases: generationCases
  },
  {
    // Sharded root over three geographic shards (one of them generational),
    // with frozen scoring stats, text routing (suggest prefix routing on)
    // and the root suggest-routing authority artifact. Exercises
    // createShardedSearch end to end.
    name: "sharded",
    docs: () => shardedBatches().flatMap(item => item.batches.flat()),
    config: SHARDED_CONFIG,
    build: buildSharded,
    cases: shardedCases
  },
  {
    // Address authority: exact address keys, interpolated house numbers,
    // postal codes, address suggest, searchAddressAuthority.
    name: "address",
    docs: () => [],
    build: root => quietCall(() => buildAddressIndex(root)),
    cases: () => [
      ...addressCases(),
      { method: "authorityLookup", params: { surface: "350 5th Avenue", size: 3 } },
      { method: "authorityLookup", params: { surface: "Chipotle", size: 3 } }
    ]
  },
  {
    // Authority field without a suggest config: legacy title suggest.
    name: "legacytitle",
    docs: () => [],
    build: root => quietCall(buildLegacyTitleIndex.bind(null, root)),
    cases: legacyTitleCases
  }
];

async function quietCall(fn) {
  const log = console.log;
  console.log = () => {};
  try {
    return await fn();
  } finally {
    console.log = log;
  }
}

// JS methods whose signature is not `(params)`.
function invoke(engine, method, params) {
  if (method === "authorityLookup") return engine.authorityLookup(params.surface, params);
  if (method === "hydrateRows") return engine.hydrateRows(params.rows, params.context);
  return engine[method](params);
}

// ---------------------------------------------------------------- queries

function textQueries(docs, seed, count) {
  const rand = rng(seed);
  const out = new Set([
    "search", "museum", "museums", "Museum Library", "quick brown fox", "the", "of the", "river city",
    "bibliothèque", "musee", "élève", "Straße", "strasse", "博物館", "東京 図書館", "Москва", "café",
    "zzzznotaword", "searhc engine", "musuem", "libary statoin", "\"static index\"", "a", "",
    "running runner", "ancient modern famous national public", "range request posting block impact score query",
    "S&P 500", "x-ray", "rock'n'roll", "12345", "H3Z 2Y7", "h3z2y7"
  ]);
  while (out.size < count) {
    const doc = docs[Math.floor(rand() * docs.length)];
    const words = `${doc.title} ${doc.body}`.split(" ");
    const start = Math.floor(rand() * words.length);
    const length = 1 + Math.floor(rand() * 4);
    out.add(words.slice(start, start + length).join(" "));
  }
  return [...out];
}

function queryMatrix(index, docs) {
  const cases = [];
  const add = (method, params) => cases.push({ method, params });
  const texts = textQueries(docs, 1000 + index.name.length, 70);
  for (const q of texts) add("search", { q, size: 10 });
  for (const q of texts.slice(0, 25)) {
    add("search", { q, size: 5, page: 2 });
    add("search", { q, size: 10, rerank: false });
    add("search", { q, size: 10, exact: true });
    add("search", { q, size: 3, includeResults: false });
    add("count", { q });
  }
  for (const q of texts.slice(0, 15)) {
    add("search", { q, size: 10, filters: { facets: { category: ["culture", "science"] } } });
    add("search", { q, size: 10, filters: { facets: { tags: ["static"] }, booleans: { featured: true } } });
    add("search", { q, size: 10, filters: { numbers: { year: { min: 2000, max: 2015 } } } });
    add("search", { q, size: 10, filters: { numbers: { published: { min: "2010-01-01" } } } });
    add("search", { q, size: 10, sort: "-year" });
    add("search", { q, size: 10, sort: { field: "temperature", order: "asc" } });
    add("search", { q, size: 10, sort: "year", filters: { booleans: { featured: false } } });
    add("search", { q, size: 10, highlight: true });
    add("search", { q, size: 10, facets: ["category", "tags"] });
  }
  // Browse lanes (no text).
  add("search", { q: "", size: 10 });
  add("search", { q: "", size: 10, page: 3 });
  add("search", { q: "", size: 10, sort: "-year" });
  add("search", { q: "", size: 10, sort: "published" });
  add("search", { q: "", size: 10, sort: "-featured" });
  add("search", { q: "", size: 20, filters: { facets: { category: ["geo"] } } });
  add("search", { q: "", size: 20, filters: { facets: { tags: ["wasm", "rust"] }, numbers: { temperature: { min: 0 } } } });
  add("search", { q: "", size: 20, filters: { booleans: { featured: true } }, sort: "-temperature" });
  add("search", { q: "", size: 10, filters: { numbers: { year: { min: 2030 } } }, sort: "year" });
  add("search", { q: "", size: 10, facets: ["category"] });

  const config = index.config;
  if (config.geo) {
    const centers = [{ lat: 45.5017, lon: -73.5673 }, { lat: 48.8566, lon: 2.3522 }, { lat: -17.7134, lon: 178.065 }, { lat: 0, lon: 0 }];
    for (const center of centers) {
      add("search", { q: "", size: 10, geo: { near: center, sort: "distance" } });
      add("search", { q: "", size: 10, page: 2, geo: { near: center, sort: "distance" } });
      add("search", { q: "", size: 50, geo: { near: { ...center, radiusMeters: 5000 } } });
      add("search", { q: "", size: 10, geo: { near: { ...center, radiusMeters: 20000 }, sort: "distance" }, filters: { facets: { category: ["culture"] } } });
      add("search", { q: "museum", size: 10, geo: { near: { ...center, radiusMeters: 30000 } } });
      add("search", { q: "museum", size: 10, geo: { near: center, sort: "distance" } });
      add("search", { q: "river city", size: 10, geo: { near: center, boost: { weight: 4, pivotMeters: 500 } } });
      add("search", { q: "", size: 10, geo: { near: center, sort: "distance" }, filters: { facets: { category: ["food"] } } });
    }
    add("search", { q: "", size: 100, geo: { box: { minLat: 45.3, maxLat: 45.7, minLon: -73.8, maxLon: -73.3 } } });
    add("search", { q: "", size: 100, geo: { box: { minLat: -18, maxLat: -17.4, minLon: 177.8, maxLon: -179.5 } } });
    add("search", { q: "bakery", size: 10, geo: { box: { minLat: 48.6, maxLat: 49.1, minLon: 2.1, maxLon: 2.6 } } });
    add("search", { q: "", size: 10, geo: { route: { points: [{ lat: 45.4, lon: -73.7 }, { lat: 45.6, lon: -73.4 }], bufferMeters: 2000 }, routeSort: "progress" } });
  }
  if (index.deepGeo) deepGeoCases(add);
  if (config.vectors) {
    const rand = rng(77);
    for (let t = 0; t < 10; t++) {
      const vector = Array.from({ length: 16 }, (_, d) => (d % 7 === t % 7 ? 1 : 0) + (rand() - 0.5) * 0.3);
      add("vectorSearch", { vector, k: 10 });
      add("vectorSearch", { vector, k: 5, nprobe: 1000, refineFactor: 8 });
      add("vectorSearch", { vector, k: 10, includeResults: false });
      add("search", { q: texts[t], vector, size: 10 });
    }
  }
  if (config.suggest || config.authority?.length) {
    for (const q of ["m", "mu", "mus", "muse", "museum", "lib", "st", "s", "ca", "cas", "bib", "é", "東", "zz", "", "river c", "quick br", "Mus"]) {
      add("suggest", { q, size: 5 });
      add("suggest", { q, size: 3, hydrate: true });
    }
  }
  edgeCases(add, texts, docs);
  return cases;
}

// Paging, sorting, filter-value and hydration edge cases: pages are
// doubles in JS (fractional and huge pages page by double offsets), sorted
// lanes must agree with the exact sort (missing values last, booleans
// ordered), date filters go through Date.parse, non-iterable facet values
// throw, and hydrateRows rejects ids that are not document indexes.
function edgeCases(add, texts, docs) {
  const pages = [1.5, 2.25, 3.7, 42949674, 1e15, "1e400", "abc", "2", 0, -3];
  for (const page of pages) {
    add("search", { q: texts[0], size: 3, page });
    add("search", { q: "", size: 4, page });
  }
  for (const page of [1.5, 2.5, 42949674]) {
    add("search", { q: texts[1], size: 3, page, rerank: false });
    add("search", { q: texts[2], size: 4, page, exact: true });
    add("search", { q: texts[0], size: 3, page, sort: "-year" });
    add("search", { q: "", size: 3, page, sort: "temperature" });
    add("search", { q: "", size: 3, page, filters: { facets: { category: ["culture"] } } });
    add("search", { q: texts[3], size: 3, page, filters: { booleans: { featured: true } } });
  }
  for (const size of ["abc", 0, 2.7, 1e9]) add("search", { q: texts[0], size });
  for (const sort of ["year", "-year", "featured", "-featured", "temperature", "-published"]) {
    add("search", { q: "", size: 10, sort });
    add("search", { q: "", size: 10, page: 38, sort });
    add("search", { q: "", size: 25, sort, filters: { facets: { category: ["food"] } } });
    for (const q of texts.slice(4, 8)) {
      add("search", { q, size: 10, sort, rerank: false });
      add("search", { q, size: 50, sort });
    }
  }
  for (const value of ["2010", "20100101", "Jan 1 2010", "2010/06/15", "15 June 2010", "2010-06-15Z", "2010-06-15T10:00",
    "Tue, 15 Jun 2010 10:00:00 GMT", "12345678901", "1", "nope", 1262304000000, true, [2010], { y: 2010 }]) {
    add("search", { q: "", size: 5, filters: { numbers: { published: { min: value } } } });
    add("search", { q: texts[0], size: 5, filters: { numbers: { published: { max: value } } } });
  }
  for (const value of [5, true, { a: 1 }, null, "fo", 1e21]) {
    add("search", { q: texts[0], size: 5, filters: { facets: { category: value } } });
    add("search", { q: "", size: 5, filters: { facets: { category: value } } });
    add("search", { q: "zzzznotaword", size: 5, exact: true, filters: { facets: { category: value } } });
  }
  const last = docs.length - 1;
  for (const rows of [[[0, 1], [last, 2.5]], [[3, 0]], [[1.5, 0]], [[-1, 0]], [[docs.length, 0]], [[1e20, 0]], [["3", 0]], [[null, 0]], [[true, 0]]]) {
    if (docs.length) add("hydrateRows", { rows });
  }
}

// The features index first replays a facet filter, an exact query (which
// merges the full manifest) and another facet filter on one engine: the
// merge used to replace the loaded facet dictionaries with summaries and
// drop the second filter. Then lane soundness cases: bundles over a partial
// base-term window, filtered complete bundles, sorted lanes.
function featuresCases(index, docs) {
  const cases = [];
  const add = (method, params) => cases.push({ method, params });
  add("search", { q: "museum", size: 3, filters: { facets: { category: ["culture"] } } });
  add("search", { q: "museum", size: 3, exact: true });
  add("search", { q: "river", size: 3, filters: { facets: { category: ["culture", "science"] } } });
  add("search", { q: "", size: 5, filters: { facets: { category: ["science"] } } });
  cases.push(...queryMatrix(index, docs));
  for (const q of ["score valley title", "modern static", "river city museum", "static index range", "query national"]) {
    for (const extra of [{}, { rerank: false }, { filters: { booleans: { featured: true } } }, { filters: { numbers: { year: { min: 2000 } } } }]) {
      add("search", { q, size: 3, ...extra });
      add("search", { q, size: 3, ...extra, exact: true });
    }
  }
  for (const sort of ["year", "-year", "temperature", "-featured"]) {
    add("search", { q: "bridge", size: 10, sort, rerank: false });
    add("search", { q: "bridge", size: 10, sort, rerank: false, exact: true });
    add("search", { q: "lazy", size: 100, sort });
  }
  return cases;
}

// Federation-specific cases on top of the standard matrix: tombstoned and
// replaced docs, deep pages, ordered merges, hybrid and vector merges,
// hydrated suggestions, postal-code normalization.
function federationCommonCases(add) {
  for (const q of ["museum", "river city", "search engine", "lazy dog", "zzzznotaword", "musuem", "Museum Library"]) {
    add("search", { q, size: 20, page: 3 });
    add("search", { q, size: 7, page: 2, sort: "-popularity" });
    add("search", { q, size: 10, sort: "-featured" });
    add("search", { q, size: 10, sort: { field: "year", direction: "desc" } });
    add("search", { q, size: 10, sort: "nosuchfield" });
    add("search", { q, size: 10, highlight: { maxTerms: 3 }, facets: { fields: ["category", "tags"], size: 3 } });
    add("count", { q, filters: { facets: { category: ["culture"] } } });
    add("count", { q, geo: { near: { lat: 45.5017, lon: -73.5673, radiusMeters: 20000 } } });
  }
  add("search", { q: "", size: 100, page: 4, sort: "temperature" });
  add("search", { q: "", size: 100, page: 11 });
  add("search", { q: "museum", size: 100, page: 11 });
  add("search", { q: "H3Z 2Y7", size: 5 });
  add("search", { q: "", size: 10, sort: "-year", geo: { near: { lat: 45.5, lon: -73.6 }, sort: "distance" } });
  add("search", { q: "", size: 25, geo: { near: { lat: 48.85, lon: 2.35 }, sort: "distance" }, facets: ["category"] });
  add("search", { q: "museum", size: 25, page: 2, geo: { near: { lat: 48.85, lon: 2.35, radiusMeters: 50000 }, sort: "distance" }, facets: ["tags"] });
  add("search", { q: "", size: 30, geo: { box: { minLat: 45, maxLat: 50, minLon: -74, maxLon: 7 } }, facets: ["category"] });
  add("search", { q: "", size: 10, geo: { route: [{ lat: 45.4, lon: -73.7 }, { lat: 45.6, lon: -73.4 }], corridorMeters: 3000, sort: "route" } });
  add("search", { q: "", size: 10, geo: { route: [{ lat: 45.4, lon: -73.7 }, { lat: 45.6, lon: -73.4 }], corridorMeters: 3000 } });
  add("count", { q: "" });
  add("count", { q: "", filters: { booleans: { featured: true } } });
  add("count", { q: "h3z2y7" });
  const rand = rng(91);
  for (let t = 0; t < 6; t++) {
    const vector = Array.from({ length: 16 }, (_, d) => (d % 7 === t % 7 ? 1 : 0) + (rand() - 0.5) * 0.3);
    add("vectorSearch", { vector, k: 15 });
    add("vectorSearch", { vector, k: 8, includeResults: false });
    add("search", { q: "", vector, size: 10, page: 2 });
    add("search", { q: "museum", vector, size: 5, hybrid: { rrfK: 10 } });
    add("search", { q: "river", vector, size: 10, filters: { facets: { category: ["culture", "geo"] } } });
  }
  for (const q of ["mus", "museum", "riv", "a", "the", "zzz", "H3Z 2", "lib", "ca"]) {
    add("suggest", { q, size: 10 });
    add("suggest", { q, size: 6, hydrate: true });
  }
}

function generationCases(index, docs) {
  const cases = queryMatrix(index, docs);
  const add = (method, params) => cases.push({ method, params });
  federationCommonCases(add);
  return cases;
}

function shardedCases(index, docs) {
  const cases = queryMatrix(index, docs);
  const add = (method, params) => cases.push({ method, params });
  federationCommonCases(add);
  for (const shards of [["quebec"], "europe", ["world"], ["north-america", "oceania"], ["europe", "fiji", "europe"], ["atlantis"]]) {
    add("search", { q: "museum", size: 10, shards });
    add("search", { q: "", size: 10, shards, sort: "-year" });
    add("search", { q: "", size: 10, shards, geo: { near: { lat: 45.5, lon: -73.6 }, sort: "distance" } });
    add("count", { q: "river", shards });
    add("suggest", { q: "mus", size: 5, shards });
    add("suggest", { q: "riv", size: 5, hydrate: true, shards });
    add("vectorSearch", { vector: Array.from({ length: 16 }, (_, d) => (d === 2 ? 1 : 0.05)), k: 5, shards });
  }
  for (const q of ["museum", "river city", "zzzznotaword", "lazy dog", "Москва", "東京 図書館", "musuem"]) {
    add("search", { q, size: 10, geo: { box: { minLat: -18, maxLat: -17.4, minLon: 177.8, maxLon: 178.4 } } });
    add("search", { q, size: 10, geo: { near: { lat: 46.8, lon: -71.2, radiusMeters: 40000 } } });
    add("search", { q, size: 10, typo: false });
    add("count", { q, geo: { box: { minLat: 48, maxLat: 50, minLon: 1, maxLon: 7 } } });
  }
  for (const q of ["mus", "muse", "bib", "lib", "river", "zz", "Mo", "東"]) {
    add("suggest", { q, size: 5, shards: ["world"] });
    add("suggest", { q, size: 4, hydrate: true, shards: ["quebec", "fiji"] });
  }
  add("search", { q: "", size: 10, geo: { near: { lat: -17.7, lon: 178.1 } } });
  add("search", { q: "", size: 200, page: 6, geo: { near: { lat: 49.6, lon: 6.1 }, sort: "distance" } });
  add("search", { q: "museum", size: 10, geo: { near: { lat: 10, lon: -150 }, sort: "distance" } });
  add("search", { q: "", size: 10, geo: { near: { lat: "x", lon: 2 }, sort: "distance" } });
  add("search", { q: "", size: 10, geo: { route: [{ lat: 48.8, lon: 2.3 }, { lat: 49.6, lon: 6.1 }], corridorMeters: 5000, sort: "route" } });
  add("search", { q: "", size: 10, geo: { route: "not a polyline!!", corridorMeters: 5000 } });
  return cases;
}

// Route sorting, viewports, polylines, GeoJSON, boosts and paging deep into
// the nearest-first traversal.
function deepGeoCases(add) {
  const centers = [{ lat: 45.5017, lon: -73.5673 }, { lat: 48.8566, lon: 2.3522 }, { lat: -17.7134, lon: 178.065 }, { lat: 49.6116, lon: 6.1319 }];
  const route = [{ lat: 45.4, lon: -73.7 }, { lat: 45.6, lon: -73.4 }, { lat: 46.8, lon: -71.2 }];
  for (const center of centers) {
    add("search", { q: "", size: 10, geo: { near: center, sort: "distance" } });
    add("search", { q: "", size: 10, page: 3, geo: { near: center, sort: "distance" } });
    add("search", { q: "", size: 50, geo: { near: { ...center, radiusMeters: 5000 } } });
    add("search", { q: "", size: 100, geo: { near: { ...center, radiusMeters: 3000 } } });
    add("search", { q: "", size: 10, geo: { near: { ...center, radiusMeters: 20000 }, sort: "distance" }, filters: { facets: { category: ["culture"] } } });
    add("search", { q: "", size: 10, geo: { near: { ...center, radiusMeters: 20000 } }, filters: { facets: { category: ["culture", "food"] }, booleans: { featured: true } } });
    add("search", { q: "", size: 10, geo: { near: { ...center, radiusMeters: 20000 }, sort: "distance" }, filters: { numbers: { year: { min: 2000 } } } });
    add("search", { q: "museum", size: 10, geo: { near: { ...center, radiusMeters: 30000 } } });
    add("search", { q: "museum", size: 10, geo: { near: center, sort: "distance" } });
    add("search", { q: "museum", size: 10, page: 2, geo: { near: center, sort: "distance" }, filters: { facets: { category: ["food"] } } });
    add("search", { q: "lazy dog", size: 100, page: 7, geo: { near: center, sort: "distance" } });
    add("search", { q: "river city", size: 10, geo: { near: { ...center, radiusMeters: 30000 }, boost: { weight: 4, pivotMeters: 500 } } });
    add("search", { q: "river city", size: 10, geo: { near: center, boost: { weight: 4, pivotMeters: 500 }, box: { minLat: -90, maxLat: 90, minLon: -180, maxLon: 180 } } });
    for (const word of ["Αθήνα", "naïve", "서울", "lazy"]) add("search", { q: word, size: 100, page: 20, geo: { near: center, sort: "distance" } });
    add("search", { q: "Αθήνα", size: 10, geo: { near: center, sort: "distance" } });
    add("search", { q: "", size: 10, geo: { near: center, sort: "distance" }, filters: { facets: { category: ["food"] } } });
    add("search", { q: "zzzznotaword", size: 10, geo: { near: center, sort: "distance" } });
  }
  add("search", { q: "", size: 100, geo: { box: { minLat: 45.3, maxLat: 45.7, minLon: -73.8, maxLon: -73.3 } } });
  add("search", { q: "", size: 20, geo: { box: { minLat: 45.3, maxLat: 45.7, minLon: -73.8, maxLon: -73.3 }, near: { lat: 45.5, lon: -73.6 }, sort: "distance" } });
  add("search", { q: "", size: 100, geo: { box: { minLat: -18, maxLat: -17.4, minLon: 177.8, maxLon: -179.5 } } });
  add("search", { q: "", size: 30, geo: { box: { minLat: 45.3, maxLat: 49.7, minLon: -73.8, maxLon: 6.5 } }, filters: { facets: { category: ["geo"] } } });
  add("search", { q: "bakery", size: 10, geo: { box: { minLat: 48.6, maxLat: 49.1, minLon: 2.1, maxLon: 2.6 } } });
  add("search", { q: "search", size: 10, geo: { box: { minLat: -90, maxLat: 90, minLon: -180, maxLon: 180 } } });
  add("search", { q: "", size: 10, geo: { route, corridorMeters: 3000 } });
  add("search", { q: "", size: 10, geo: { route, corridorMeters: 3000, sort: "route" } });
  add("search", { q: "", size: 10, geo: { route, corridorMeters: 3000, sort: "route", routePositionMeters: 20000 } });
  add("search", { q: "", size: 10, geo: { route, corridorMeters: 3000, sort: "route", routeDirection: "reverse", viewport: { minLat: 45.4, maxLat: 45.6, minLon: -73.7, maxLon: -73.5 } } });
  add("search", { q: "", size: 10, geo: { route, corridorMeters: 3000, sort: "route" }, filters: { facets: { category: ["food"] } } });
  add("search", { q: "museum", size: 10, geo: { route, corridorMeters: 5000, sort: "route" } });
  add("search", { q: "museum", size: 10, geo: { route, corridorMeters: 5000 } });
  add("search", { q: "", size: 10, geo: { route: "_p~iF~ps|U_ulLnnqC_mqNvxq`@", corridorMeters: 50000, near: { lat: 40, lon: -120 } } });
  add("search", { q: "", size: 10, geo: { route, corridorMeters: 4000, near: { lat: 45.5, lon: -73.6 }, sort: "distance" } });
  add("search", { q: "", size: 10, geo: { route, corridorMeters: 4000, near: { lat: 45.5, lon: -73.6 }, sort: "route" } });
  add("search", { q: "museum", size: 10, geo: { route, corridorMeters: 8000, near: { lat: 45.5, lon: -73.6 }, boost: { weight: 2 } } });
  add("search", { q: "", size: 10, geo: { route: { type: "LineString", coordinates: [[-73.7, 45.4], [-73.4, 45.6]] }, corridorMeters: 2500, sort: "route" }, filters: { booleans: { featured: true } } });
  add("search", { q: "", size: 10, geo: { near: { lat: 45.5, lon: -73.5 }, sort: "route" } });
  add("search", { q: "", size: 10, geo: { near: { lat: 45.5, lon: -73.5, radiusMeters: 100 }, box: { minLat: 1, maxLat: 2, minLon: 1, maxLon: 2 } } });
  add("search", { q: "", size: 10, geo: { near: { lat: 45.5, lon: -73.5 } } });

}

// ---------------------------------------------------------------- run

function jsonSafe(value) {
  return JSON.parse(JSON.stringify(value, (key, item) => {
    if (item instanceof Map) return Object.fromEntries(item);
    if (item instanceof Set) return [...item];
    if (typeof item === "number" && !Number.isFinite(item)) return null;
    return item;
  }));
}

async function main() {
  await mkdir(outRoot, { recursive: true });
  // Date.parse parity (rust/rangefind/tests/date_parse_parity.rs).
  await writeFile(join(outRoot, "dates.json"), JSON.stringify({ tz: process.env.TZ, cases: dateParseCases() }));
  const summary = [];
  for (const index of INDEXES) {
    if (only && !only.has(index.name)) continue;
    const root = join(outRoot, index.name);
    await rm(root, { recursive: true, force: true });
    await mkdir(root, { recursive: true });
    const docs = index.docs();
    if (index.build) {
      await index.build(root, index);
    } else {
      await writeFile(join(root, "docs.jsonl"), `${docs.map(doc => JSON.stringify(doc)).join("\n")}\n`);
      await writeFile(join(root, "rangefind.config.json"), JSON.stringify(index.config, null, 2));
      const log = console.log;
      console.log = () => {};
      try {
        await build({ configPath: join(root, "rangefind.config.json") });
      } finally {
        console.log = log;
      }
    }
    await rm(join(root, "index", "_build"), { recursive: true, force: true });
    const cases = index.cases ? index.cases(index, docs) : queryMatrix(index, docs);
    await resetNodeRuntimeCaches();
    const engine = await createNodeSearch({ baseUrl: pathToFileURL(join(root, "index") + "/").href });
    let errors = 0;
    for (const item of cases) {
      try {
        item.response = jsonSafe(await invoke(engine, item.method, structuredClone(item.params)));
      } catch (error) {
        item.error = { message: String(error?.message || error), code: error?.code || null };
        errors++;
      }
    }
    await writeFile(join(outRoot, `${index.name}.cases.json`), JSON.stringify({ index: index.name, cases }));
    summary.push({ index: index.name, docs: docs.length, cases: cases.length, errors });
    console.log(`${index.name}: ${docs.length} docs, ${cases.length} cases, ${errors} errors`);
  }
  let previous = [];
  if (only) {
    try {
      previous = JSON.parse(await readFile(join(outRoot, "summary.json"), "utf8"));
    } catch {}
  }
  const merged = [...previous.filter(item => !summary.some(next => next.index === item.index)), ...summary];
  await writeFile(join(outRoot, "summary.json"), JSON.stringify(merged, null, 2));
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
