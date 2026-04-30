import {
  appendFileSync,
  createReadStream,
  existsSync,
  mkdirSync,
  rmSync,
  unlinkSync,
  writeFileSync
} from "node:fs";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { createInterface } from "node:readline";
import { gzipSync } from "node:zlib";
import { fileURLToPath } from "node:url";
import { termCounts, tokenize } from "./analyzer.js";
import { CODE_MAGIC, TERM_RANGE_MAGIC, TERM_SHARD_MAGIC, fixedWidth, pushVarint, writeFixedInt } from "./binary.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const textEncoder = new TextEncoder();

const DEFAULTS = {
  docChunkSize: 100,
  baseShardDepth: 3,
  maxShardDepth: 5,
  targetShardPostings: 30000,
  packBytes: 4 * 1024 * 1024,
  postingFlushLines: 100000,
  maxTermsPerDoc: 160,
  initialResultLimit: 20,
  postingBlockSize: 128,
  bm25fK1: 1.2
};

function configDir(configPath) {
  return dirname(resolve(configPath));
}

function resolveFrom(base, value) {
  return resolve(base, value || ".");
}

async function readConfig(configPath) {
  const full = resolve(configPath);
  const base = configDir(full);
  const raw = JSON.parse(await readFile(full, "utf8"));
  const config = {
    ...DEFAULTS,
    ...raw,
    input: resolveFrom(base, raw.input),
    output: resolveFrom(base, raw.output || "public/rangefind"),
    fields: raw.fields || [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: raw.facets || [],
    numbers: raw.numbers || [],
    display: raw.display || ["title", "url"]
  };
  return config;
}

function getPath(object, path, fallback = "") {
  if (!path) return fallback;
  let value = object;
  for (const part of String(path).split(".")) {
    if (value == null) return fallback;
    value = value[part];
  }
  if (Array.isArray(value)) return value.join(" ");
  return value ?? fallback;
}

async function eachJsonLine(path, fn) {
  const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  let index = 0;
  for await (const line of rl) {
    if (!line.trim()) continue;
    await fn(JSON.parse(line), index++);
  }
  return index;
}

function addDict(dict, value, label = value) {
  const key = String(value || "");
  if (!key) return 0;
  if (dict.ids.has(key)) return dict.ids.get(key);
  const id = dict.values.length;
  dict.ids.set(key, id);
  dict.values.push({ value: key, label: String(label || key), n: 0 });
  return id;
}

function fieldText(doc, field) {
  return String(getPath(doc, field.path, ""));
}

async function measure(config) {
  const fieldTotals = Object.fromEntries(config.fields.map(field => [field.name, 0]));
  const dicts = Object.fromEntries(config.facets.map(facet => [facet.name, { ids: new Map(), values: [{ value: "", label: "", n: 0 }] }]));
  let total = 0;
  await eachJsonLine(config.input, async (doc) => {
    total++;
    for (const field of config.fields) {
      fieldTotals[field.name] += tokenize(fieldText(doc, field), { unique: false }).length;
    }
    for (const facet of config.facets) {
      const code = addDict(dicts[facet.name], getPath(doc, facet.path), getPath(doc, facet.labelPath || facet.path));
      dicts[facet.name].values[code].n++;
    }
  });
  return {
    total,
    avgLens: Object.fromEntries(config.fields.map(field => [
      field.name,
      Math.max(1, fieldTotals[field.name] / Math.max(1, total))
    ])),
    dicts
  };
}

function addWeighted(scores, term, weight) {
  if (!term || weight <= 0) return;
  scores.set(term, (scores.get(term) || 0) + weight);
}

function addFieldScores(doc, field, avgLen, scores) {
  const counts = termCounts(fieldText(doc, field));
  const len = [...counts.values()].reduce((sum, n) => sum + n, 0);
  const b = field.b ?? 0.75;
  const norm = 1 - b + b * (len / Math.max(1, avgLen));
  for (const [term, tf] of counts) {
    addWeighted(scores, term, (field.weight ?? 1) * tf / Math.max(0.2, norm));
  }

  if (field.phrase) {
    const terms = tokenize(fieldText(doc, field));
    for (const n of [2, 3]) {
      for (let i = 0; i <= terms.length - n; i++) {
        addWeighted(scores, terms.slice(i, i + n).join("_"), (field.phraseWeight ?? 8));
      }
    }
  }
}

function bm25fScores(weightedTf, k1) {
  const out = new Map();
  for (const [term, tf] of weightedTf) {
    out.set(term, ((k1 + 1) * tf) / (k1 + tf));
  }
  return out;
}

function topTerms(scores, limit) {
  return [...scores.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, limit);
}

function shardKey(term, depth) {
  return String(term || "").slice(0, depth).padEnd(depth, "_");
}

function baseShardFor(term, config) {
  return shardKey(term, config.baseShardDepth);
}

function bufferPosting(buffer, config, term, doc, score) {
  const shard = baseShardFor(term, config);
  if (!buffer.byShard.has(shard)) buffer.byShard.set(shard, []);
  buffer.byShard.get(shard).push(`${term}\t${doc}\t${Math.max(1, Math.round(score * 1000))}\n`);
  buffer.lines++;
  if (buffer.lines >= config.postingFlushLines) flushPostingBuffer(buffer);
}

function flushPostingBuffer(buffer) {
  for (const [shard, lines] of buffer.byShard) {
    appendFileSync(resolve(buffer.runsOut, `${shard}.tsv`), lines.join(""));
    lines.length = 0;
  }
  buffer.byShard.clear();
  buffer.lines = 0;
}

function docPayload(doc, config, index) {
  const payload = { id: String(getPath(doc, config.idPath || "id", index)), index };
  for (const item of config.display) {
    if (typeof item === "string") payload[item] = getPath(doc, item);
    else payload[item.name] = getPath(doc, item.path);
  }
  if (!payload.title) payload.title = payload.id;
  if (!payload.url) payload.url = getPath(doc, config.urlPath || "url", "");
  return payload;
}

function writeDocChunk(out, docs, index) {
  if (!docs.length) return;
  const file = `${String(index).padStart(4, "0")}.json`;
  writeFileSync(resolve(out, "docs", file), JSON.stringify(docs));
}

async function writePostingRuns(config, measured, dirs) {
  const codes = {};
  for (const facet of config.facets) codes[facet.name] = new Array(measured.total);
  for (const number of config.numbers) codes[number.name] = new Array(measured.total);

  const initialResults = [];
  const buffer = { byShard: new Map(), lines: 0, runsOut: dirs.runsOut };
  const baseShards = new Set();
  let chunk = [];
  let chunkIndex = 0;

  await eachJsonLine(config.input, async (doc, index) => {
    const weighted = new Map();
    for (const field of config.fields) addFieldScores(doc, field, measured.avgLens[field.name], weighted);
    for (const [term, score] of topTerms(bm25fScores(weighted, config.bm25fK1), config.maxTermsPerDoc)) {
      bufferPosting(buffer, config, term, index, score);
      baseShards.add(baseShardFor(term, config));
    }

    for (const facet of config.facets) {
      codes[facet.name][index] = addDict(measured.dicts[facet.name], getPath(doc, facet.path), getPath(doc, facet.labelPath || facet.path));
    }
    for (const number of config.numbers) codes[number.name][index] = Number(getPath(doc, number.path, 0)) || 0;

    const payload = docPayload(doc, config, index);
    if (initialResults.length < config.initialResultLimit) initialResults.push(payload);
    chunk.push(payload);
    if (chunk.length >= config.docChunkSize) {
      writeDocChunk(dirs.out, chunk, chunkIndex++);
      chunk = [];
    }
  });
  writeDocChunk(dirs.out, chunk, chunkIndex);
  flushPostingBuffer(buffer);
  return { codes, initialResults, baseShards: [...baseShards].sort() };
}

function entryPostingCount(entries) {
  return entries.reduce((sum, [, rows]) => sum + rows.length, 0);
}

function partitionEntries(entries, config, depth = config.baseShardDepth) {
  if (!entries.length) return [];
  if (entryPostingCount(entries) <= config.targetShardPostings || depth >= config.maxShardDepth) {
    return [{ name: shardKey(entries[0][0], depth), entries }];
  }
  const groups = new Map();
  for (const entry of entries) {
    const key = shardKey(entry[0], depth + 1);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(entry);
  }
  return [...groups.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .flatMap(([, group]) => partitionEntries(group, config, depth + 1));
}

function pushUtf8(out, value) {
  const bytes = textEncoder.encode(String(value || ""));
  pushVarint(out, bytes.length);
  for (const byte of bytes) out.push(byte);
}

function buildBlockFilters(config, dicts) {
  return [
    ...config.facets.map(facet => ({
      name: facet.name,
      kind: "facet",
      words: Math.max(1, Math.ceil((dicts[facet.name]?.values?.length || 1) / 32))
    })),
    ...config.numbers.map(number => ({ name: number.name, kind: "number" }))
  ];
}

function addBit(words, value) {
  if (value < 0) return;
  const word = Math.floor(value / 32);
  const bit = value % 32;
  words[word] |= 2 ** bit;
}

function emptySummary(filters) {
  return filters.map(filter => filter.kind === "facet" ? { words: new Array(filter.words).fill(0) } : { min: 0, max: 0 });
}

function updateSummary(summary, filters, codes, doc) {
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    const value = codes[filter.name]?.[doc] || 0;
    if (filter.kind === "facet") addBit(summary[i].words, value);
    else if (value) {
      summary[i].min = summary[i].min ? Math.min(summary[i].min, value) : value;
      summary[i].max = Math.max(summary[i].max, value);
    }
  }
}

function encodePostings(rows, total, codes, filters, config) {
  const df = rows.length;
  const idf = Math.log(1 + (total - df + 0.5) / (df + 0.5));
  const encoded = rows
    .map(([doc, score]) => [doc, Math.max(1, Math.round(score * idf / 10))])
    .sort((a, b) => b[1] - a[1] || a[0] - b[0]);
  const bytes = [];
  const blocks = [];
  for (let i = 0; i < encoded.length; i++) {
    const [doc, impact] = encoded[i];
    if (i % config.postingBlockSize === 0) {
      blocks.push({ offset: bytes.length, maxImpact: impact, filters: emptySummary(filters) });
    }
    updateSummary(blocks[blocks.length - 1].filters, filters, codes, doc);
    pushVarint(bytes, doc);
    pushVarint(bytes, impact);
  }
  return { df, count: encoded.length, bytes: Uint8Array.from(bytes), blocks };
}

function buildTermShard(entries, total, codes, filters, config) {
  const header = [...TERM_SHARD_MAGIC];
  const postingChunks = [];
  const directory = [];
  let postingOffset = 0;

  for (const [term, rows] of entries.sort((a, b) => a[0].localeCompare(b[0]))) {
    const postings = encodePostings(rows, total, codes, filters, config);
    directory.push({ term, postings, offset: postingOffset });
    postingChunks.push(postings.bytes);
    postingOffset += postings.bytes.length;
  }

  pushVarint(header, directory.length);
  for (const entry of directory) {
    pushUtf8(header, entry.term);
    pushVarint(header, entry.postings.df);
    pushVarint(header, entry.postings.count);
    pushVarint(header, entry.offset);
    pushVarint(header, entry.postings.bytes.length);
    pushVarint(header, config.postingBlockSize);
    pushVarint(header, entry.postings.blocks.length);
    for (const block of entry.postings.blocks) {
      pushVarint(header, block.offset);
      pushVarint(header, block.maxImpact);
      for (let i = 0; i < filters.length; i++) {
        const filter = filters[i];
        const summary = block.filters[i];
        if (filter.kind === "facet") for (const word of summary.words) pushVarint(header, word);
        else {
          pushVarint(header, summary.min);
          pushVarint(header, summary.max);
        }
      }
    }
  }

  return Buffer.concat([
    Buffer.from(Uint8Array.from(header)),
    ...postingChunks.map(chunk => Buffer.from(chunk))
  ]);
}

function createPackWriter(outDir, targetBytes) {
  mkdirSync(outDir, { recursive: true });
  return { index: -1, file: "", path: "", offset: 0, bytes: 0, entries: {}, packs: [], outDir, targetBytes };
}

function openPack(writer) {
  writer.index++;
  writer.file = `${String(writer.index).padStart(4, "0")}.bin`;
  writer.path = resolve(writer.outDir, writer.file);
  writer.offset = 0;
  writer.packs.push({ file: writer.file, bytes: 0, shards: 0 });
  writeFileSync(writer.path, "");
}

function writePackedShard(writer, shard, compressed) {
  if (!writer.file || (writer.offset > 0 && writer.offset + compressed.length > writer.targetBytes)) openPack(writer);
  appendFileSync(writer.path, compressed);
  writer.entries[shard] = { pack: writer.file, offset: writer.offset, length: compressed.length };
  writer.offset += compressed.length;
  writer.bytes += compressed.length;
  const pack = writer.packs[writer.packs.length - 1];
  pack.bytes += compressed.length;
  pack.shards++;
}

function buildRangeFile(ranges) {
  const out = [...TERM_RANGE_MAGIC];
  pushVarint(out, ranges.length);
  for (const [packIndex, offset, length] of ranges) {
    pushVarint(out, packIndex);
    pushVarint(out, offset);
    pushVarint(out, length);
  }
  return Buffer.from(Uint8Array.from(out));
}

async function reduceShard(baseShard, config, measured, runData, filters, packWriter) {
  const path = resolve(runData.runsOut, `${baseShard}.tsv`);
  const byTerm = new Map();
  const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    const [term, docRaw, scoreRaw] = line.split("\t");
    if (!term) continue;
    if (!byTerm.has(term)) byTerm.set(term, new Map());
    const rows = byTerm.get(term);
    const doc = Number(docRaw);
    rows.set(doc, (rows.get(doc) || 0) + Number(scoreRaw));
  }
  const entries = [...byTerm.entries()].map(([term, rows]) => [term, [...rows.entries()]]);
  const partitions = partitionEntries(entries, config);
  const finalShards = [];
  let postings = 0;
  for (const rows of byTerm.values()) postings += rows.size;
  for (const partition of partitions) {
    const encoded = buildTermShard(partition.entries, measured.total, runData.codes, filters, config);
    writePackedShard(packWriter, partition.name, gzipSync(encoded, { level: 6 }));
    finalShards.push(partition.name);
  }
  unlinkSync(path);
  return { terms: byTerm.size, postings, shards: finalShards };
}

async function reduceRuns(config, measured, runData, dirs) {
  const filters = buildBlockFilters(config, measured.dicts);
  const packWriter = createPackWriter(resolve(dirs.out, "terms", "packs"), config.packBytes);
  const finalShards = new Set();
  let termCount = 0;
  let postingCount = 0;
  for (let i = 0; i < runData.baseShards.length; i++) {
    const stats = await reduceShard(runData.baseShards[i], config, measured, { ...runData, runsOut: dirs.runsOut }, filters, packWriter);
    termCount += stats.terms;
    postingCount += stats.postings;
    for (const shard of stats.shards) finalShards.add(shard);
  }
  const shards = [...finalShards].sort();
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const ranges = shards.map((shard) => {
    const entry = packWriter.entries[shard];
    return [packIndexes.get(entry.pack), entry.offset, entry.length];
  });
  writeFileSync(resolve(dirs.out, "terms", "ranges.bin.gz"), gzipSync(buildRangeFile(ranges), { level: 9 }));
  return { filters, shards, packs: packWriter.packs, termCount, postingCount, packBytes: packWriter.bytes };
}

function buildCodesFile(config, measured, codes) {
  const fields = [...config.facets.map(f => ({ name: f.name })), ...config.numbers.map(n => ({ name: n.name }))];
  const header = [...CODE_MAGIC];
  const chunks = [];
  pushVarint(header, measured.total);
  pushVarint(header, fields.length);
  for (const field of fields) {
    const values = codes[field.name] || [];
    const width = fixedWidth(values);
    pushUtf8(header, field.name);
    header.push(width);
    const chunk = Buffer.alloc(measured.total * width);
    for (let i = 0; i < measured.total; i++) writeFixedInt(chunk, i * width, width, values[i] || 0);
    chunks.push(chunk);
  }
  return Buffer.concat([Buffer.from(Uint8Array.from(header)), ...chunks]);
}

export async function build({ configPath }) {
  const config = await readConfig(configPath);
  const dirs = {
    out: config.output,
    runsOut: resolve(config.output, "_build", "runs")
  };
  rmSync(dirs.out, { recursive: true, force: true });
  mkdirSync(resolve(dirs.out, "docs"), { recursive: true });
  mkdirSync(resolve(dirs.out, "terms"), { recursive: true });
  mkdirSync(dirs.runsOut, { recursive: true });

  console.log(`Rangefind: reading ${config.input}`);
  const measured = await measure(config);
  const runData = await writePostingRuns(config, measured, dirs);
  const reduced = await reduceRuns(config, measured, runData, dirs);
  writeFileSync(resolve(dirs.out, "codes.bin.gz"), gzipSync(buildCodesFile(config, measured, runData.codes), { level: 9 }));

  const manifest = {
    version: 1,
    engine: "rangefind",
    built_at: new Date().toISOString(),
    total: measured.total,
    doc_chunk_size: config.docChunkSize,
    initial_results: runData.initialResults,
    fields: config.fields.map(({ name, weight, b, phrase }) => ({ name, weight, b, phrase: !!phrase })),
    facets: Object.fromEntries(Object.entries(measured.dicts).map(([name, dict]) => [name, dict.values])),
    numbers: config.numbers.map(n => ({ name: n.name })),
    block_filters: reduced.filters,
    shards: reduced.shards,
    stats: {
      terms: reduced.termCount,
      postings: reduced.postingCount,
      term_storage: "range-pack-v1",
      term_range_format: "rfranges-v1",
      term_pack_files: reduced.packs.length,
      term_pack_bytes: reduced.packBytes,
      posting_block_size: config.postingBlockSize,
      base_shard_depth: config.baseShardDepth,
      max_shard_depth: config.maxShardDepth,
      target_shard_postings: config.targetShardPostings
    }
  };
  writeFileSync(resolve(dirs.out, "manifest.json"), JSON.stringify(manifest));
  rmSync(resolve(dirs.out, "_build"), { recursive: true, force: true });
  console.log(`Rangefind: built ${measured.total.toLocaleString()} docs, ${reduced.shards.length.toLocaleString()} logical shards, ${reduced.packs.length.toLocaleString()} packs`);
}
