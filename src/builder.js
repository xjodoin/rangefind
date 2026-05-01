import {
  appendFileSync,
  readdirSync,
  readFileSync,
  mkdirSync,
  rmSync,
  unlinkSync,
  writeFileSync
} from "node:fs";
import { resolve } from "node:path";
import { Worker } from "node:worker_threads";
import { gzipSync, gunzipSync } from "node:zlib";
import { tokenize } from "./analyzer.js";
import {
  buildBlockFilters,
  buildCodesFile,
  buildTermShard,
  rewriteTermShardForExternalBlocks
} from "./codec.js";
import { getPath, readConfig } from "./config.js";
import { writeDirectoryFiles } from "./directory_writer.js";
import { eachJsonLine } from "./jsonl.js";
import { createPackWriter, writePackedShard } from "./packs.js";
import { encodeRunRecord, readRunRecords } from "./runs.js";
import { addFieldExpansionScores, addFieldScores, bm25fScores, fieldText, selectDocTerms } from "./scoring.js";
import { baseShardFor, partitionEntries } from "./shards.js";
import {
  addTypoIndexTerm,
  addTypoSurfacePairs,
  createTypoRunBuffer,
  reduceTypoRuns,
  surfacePairsForFields,
  typoOptions
} from "./typo.js";

function addDict(dict, value, label = value) {
  const key = String(value || "");
  if (!key) return 0;
  if (dict.ids.has(key)) return dict.ids.get(key);
  const id = dict.values.length;
  dict.ids.set(key, id);
  dict.values.push({ value: key, label: String(label || key), n: 0 });
  return id;
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

function bufferPosting(buffer, config, term, doc, score) {
  const shard = baseShardFor(term, config);
  if (!buffer.byShard.has(shard)) buffer.byShard.set(shard, []);
  buffer.byShard.get(shard).push(encodeRunRecord(["string", "number", "number"], [term, doc, Math.max(1, Math.round(score * 1000))]));
  buffer.lines++;
  if (buffer.lines >= config.postingFlushLines) flushPostingBuffer(buffer);
}

function flushPostingBuffer(buffer) {
  for (const [shard, records] of buffer.byShard) {
    appendFileSync(resolve(buffer.runsOut, `${shard}.run`), Buffer.concat(records));
    records.length = 0;
  }
  buffer.byShard.clear();
  buffer.lines = 0;
}

function docPayload(doc, config, index) {
  const payload = { id: String(getPath(doc, config.idPath || "id", index)), index };
  for (const item of config.display) {
    if (typeof item === "string") {
      payload[item] = getPath(doc, item);
    } else {
      let value = getPath(doc, item.path);
      const maxChars = Number(item.maxChars || 0);
      if (maxChars > 0 && typeof value === "string" && value.length > maxChars) {
        value = value.slice(0, maxChars).trimEnd();
      }
      payload[item.name] = value;
    }
  }
  if (!payload.title) payload.title = payload.id;
  if (!payload.url) payload.url = getPath(doc, config.urlPath || "url", "");
  return payload;
}

function docIndexKey(index) {
  return String(index).padStart(8, "0");
}

function writePackedDoc(packWriter, payload, index) {
  const bytes = Buffer.from(JSON.stringify(payload));
  writePackedShard(packWriter, docIndexKey(index), gzipSync(bytes, { level: 6 }));
}

function finishDocPacks(out, packWriter, total, pageBytes) {
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const entries = [];
  for (let index = 0; index < total; index++) {
    const key = docIndexKey(index);
    const entry = packWriter.entries[key];
    entries.push({ shard: key, packIndex: packIndexes.get(entry.pack), offset: entry.offset, length: entry.length });
  }
  const directory = writeDirectoryFiles(resolve(out, "docs"), entries, pageBytes, "docs");
  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    directory,
    packs: packWriter.packs
  };
}

function emptyPostingBlockStats() {
  return {
    externalBlocks: 0,
    externalTerms: 0,
    externalPostings: 0,
    externalPostingBytes: 0,
    inlinePostingBytes: 0
  };
}

function addPostingBlockStats(target, source) {
  for (const key of Object.keys(target)) target[key] += source?.[key] || 0;
}

function externalizeTermShard(encoded, config, filters, blockPackWriter) {
  if (!blockPackWriter || config.externalPostingBlocks === false) {
    return { buffer: encoded, stats: emptyPostingBlockStats() };
  }
  return rewriteTermShardForExternalBlocks(encoded, { block_filters: filters }, config, ({ term, blockIndex, bytes }) => {
    const key = `${term}\u0000${blockIndex}\u0000${blockPackWriter.bytes}`;
    return writePackedShard(blockPackWriter, key, gzipSync(Buffer.from(bytes), { level: 6 }));
  });
}

async function writePostingRuns(config, measured, dirs, typoBuffer) {
  const codes = {};
  for (const facet of config.facets) codes[facet.name] = new Array(measured.total);
  for (const number of config.numbers) codes[number.name] = new Array(measured.total);

  const initialResults = [];
  const buffer = { byShard: new Map(), lines: 0, runsOut: dirs.runsOut };
  const docPackWriter = createPackWriter(resolve(dirs.out, "docs", "packs"), config.docPackBytes);
  const baseShards = new Set();

  await eachJsonLine(config.input, async (doc, index) => {
    const weighted = new Map();
    const expansion = new Map();
    for (const field of config.fields) addFieldScores(doc, field, measured.avgLens[field.name], weighted);
    for (const field of config.fields) addFieldExpansionScores(doc, field, expansion);
    for (const [term, score] of selectDocTerms(
      bm25fScores(weighted, config.bm25fK1),
      expansion,
      config.maxTermsPerDoc,
      config.maxExpansionTermsPerDoc
    )) {
      bufferPosting(buffer, config, term, index, score);
      baseShards.add(baseShardFor(term, config));
    }

    for (const facet of config.facets) {
      codes[facet.name][index] = addDict(measured.dicts[facet.name], getPath(doc, facet.path), getPath(doc, facet.labelPath || facet.path));
    }
    for (const number of config.numbers) codes[number.name][index] = Number(getPath(doc, number.path, 0)) || 0;
    addTypoSurfacePairs(typoBuffer, surfacePairsForFields(doc, config.fields, fieldText));

    const payload = docPayload(doc, config, index);
    if (initialResults.length < config.initialResultLimit) initialResults.push(payload);
    writePackedDoc(docPackWriter, payload, index);
  });
  flushPostingBuffer(buffer);
  return {
    codes,
    initialResults,
    baseShards: [...baseShards].sort(),
    docs: finishDocPacks(dirs.out, docPackWriter, measured.total, config.docDirectoryPageBytes)
  };
}

async function reduceShard(baseShard, config, measured, runData, filters, packWriter, blockPackWriter, typoBuffer) {
  const path = resolve(runData.runsOut, `${baseShard}.run`);
  const byTerm = new Map();
  for await (const [term, doc, score] of readRunRecords(path, ["string", "number", "number"])) {
    if (!term) continue;
    if (!byTerm.has(term)) byTerm.set(term, new Map());
    const rows = byTerm.get(term);
    rows.set(doc, (rows.get(doc) || 0) + score);
  }

  const entries = [...byTerm.entries()].map(([term, rows]) => [term, [...rows.entries()]]);
  const partitions = partitionEntries(entries, config);
  const finalShards = [];
  const blockStats = emptyPostingBlockStats();
  let postings = 0;
  for (const [term, rows] of byTerm) {
    postings += rows.size;
    addTypoIndexTerm(typoBuffer, term, rows.size, measured.total);
  }
  for (const partition of partitions) {
    const encoded = buildTermShard(partition.entries, measured.total, runData.codes, filters, config);
    const externalized = externalizeTermShard(encoded, config, filters, blockPackWriter);
    addPostingBlockStats(blockStats, externalized.stats);
    writePackedShard(packWriter, partition.name, gzipSync(externalized.buffer, { level: 6 }));
    finalShards.push(partition.name);
  }
  unlinkSync(path);
  return { terms: byTerm.size, postings, shards: finalShards, blockStats };
}

function createReduceWorker(workerData) {
  const worker = new Worker(new URL("./reduce_worker.js", import.meta.url), { workerData });
  let nextId = 0;
  const pending = new Map();
  worker.on("message", (message) => {
    const item = pending.get(message.id);
    if (!item) return;
    pending.delete(message.id);
    if (message.ok) item.resolve(message.value);
    else item.reject(new Error(message.error || "Rangefind reduce worker failed"));
  });
  worker.on("error", (error) => {
    for (const item of pending.values()) item.reject(error);
    pending.clear();
  });
  worker.on("exit", (code) => {
    if (code === 0 || !pending.size) return;
    const error = new Error(`Rangefind reduce worker exited with code ${code}`);
    for (const item of pending.values()) item.reject(error);
    pending.clear();
  });
  return {
    call(message) {
      const id = nextId++;
      return new Promise((resolveCall, rejectCall) => {
        pending.set(id, { resolve: resolveCall, reject: rejectCall });
        worker.postMessage({ ...message, id });
      });
    },
    terminate() {
      return worker.terminate();
    }
  };
}

function mergeTypoWorkerRuns(typoBuffer, workerTypo) {
  if (!typoBuffer || !workerTypo) return;
  typoBuffer.terms += workerTypo.terms || 0;
  typoBuffer.deletePairs += workerTypo.deletePairs || 0;
  for (const shard of workerTypo.shards || []) typoBuffer.shards.add(shard);
  for (const file of readdirSync(workerTypo.runsOut)) {
    if (!file.endsWith(".run")) continue;
    appendFileSync(resolve(typoBuffer.runsOut, file), readFileSync(resolve(workerTypo.runsOut, file)));
  }
}

function sharedCodeTables(codes) {
  const shared = {};
  for (const [name, values] of Object.entries(codes || {})) {
    const buffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT * values.length);
    const view = new Int32Array(buffer);
    view.set(values);
    shared[name] = view;
  }
  return shared;
}

async function reduceRunsParallel(config, measured, runData, dirs, typoBuffer, filters, workerCount) {
  const shardOutRoot = resolve(dirs.out, "_build", "term-shards");
  mkdirSync(shardOutRoot, { recursive: true });
  const sharedCodes = sharedCodeTables(runData.codes);
  const workers = Array.from({ length: workerCount }, (_, index) => createReduceWorker({
    config,
    codes: sharedCodes,
    filters,
    measuredTotal: measured.total,
    runsOut: dirs.runsOut,
    shardOut: resolve(shardOutRoot, `worker-${index}`),
    typoOptions: typoBuffer?.options || { enabled: false },
    typoRunsOut: resolve(dirs.out, "_build", "typo-index-runs", `worker-${index}`)
  }));

  const taskResults = [];
  let nextTask = 0;
  let termCount = 0;
  let postingCount = 0;
  try {
    await Promise.all(workers.map(async (worker) => {
      while (nextTask < runData.baseShards.length) {
        const order = nextTask++;
        const stats = await worker.call({ type: "reduce", baseShard: runData.baseShards[order] });
        termCount += stats.terms;
        postingCount += stats.postings;
        taskResults.push({ order, ...stats });
      }
    }));
    const finishes = await Promise.all(workers.map(worker => worker.call({ type: "finish" })));
    for (const item of finishes) mergeTypoWorkerRuns(typoBuffer, item.typo);
  } finally {
    await Promise.allSettled(workers.map(worker => worker.terminate()));
  }

  const packWriter = createPackWriter(resolve(dirs.out, "terms", "packs"), config.packBytes);
  const blockPackWriter = config.externalPostingBlocks === false
    ? null
    : createPackWriter(resolve(dirs.out, "terms", "block-packs"), config.postingBlockPackBytes);
  const finalShards = new Set();
  const blockStats = emptyPostingBlockStats();
  for (const result of taskResults.sort((a, b) => a.order - b.order)) {
    for (const item of result.shards.sort((a, b) => a.sequence - b.sequence)) {
      const encoded = gunzipSync(readFileSync(item.path));
      const externalized = externalizeTermShard(encoded, config, filters, blockPackWriter);
      addPostingBlockStats(blockStats, externalized.stats);
      writePackedShard(packWriter, item.shard, gzipSync(externalized.buffer, { level: 6 }));
      finalShards.add(item.shard);
    }
  }
  return { finalShards, packWriter, blockPackWriter, blockStats, termCount, postingCount };
}

async function reduceRuns(config, measured, runData, dirs, typoBuffer) {
  const filters = buildBlockFilters(config, measured.dicts);
  const workerCount = Math.min(runData.baseShards.length, Math.max(1, config.reduceWorkers || 1));
  let reduced;
  if (workerCount > 1) {
    reduced = await reduceRunsParallel(config, measured, runData, dirs, typoBuffer, filters, workerCount);
  } else {
    const packWriter = createPackWriter(resolve(dirs.out, "terms", "packs"), config.packBytes);
    const blockPackWriter = config.externalPostingBlocks === false
      ? null
      : createPackWriter(resolve(dirs.out, "terms", "block-packs"), config.postingBlockPackBytes);
    const finalShards = new Set();
    const blockStats = emptyPostingBlockStats();
    let termCount = 0;
    let postingCount = 0;
    for (let i = 0; i < runData.baseShards.length; i++) {
      const stats = await reduceShard(runData.baseShards[i], config, measured, { ...runData, runsOut: dirs.runsOut }, filters, packWriter, blockPackWriter, typoBuffer);
      termCount += stats.terms;
      postingCount += stats.postings;
      addPostingBlockStats(blockStats, stats.blockStats);
      for (const shard of stats.shards) finalShards.add(shard);
    }
    reduced = { finalShards, packWriter, blockPackWriter, blockStats, termCount, postingCount };
  }
  const shards = [...reduced.finalShards].sort();
  const packIndexes = new Map(reduced.packWriter.packs.map((pack, index) => [pack.file, index]));
  const entries = shards.map((shard) => {
    const entry = reduced.packWriter.entries[shard];
    return { shard, packIndex: packIndexes.get(entry.pack), offset: entry.offset, length: entry.length };
  });
  const directory = writeDirectoryFiles(resolve(dirs.out, "terms"), entries, config.directoryPageBytes, "terms");
  return {
    filters,
    shards,
    directory,
    packs: reduced.packWriter.packs,
    blockPacks: reduced.blockPackWriter?.packs || [],
    blockStats: reduced.blockStats || emptyPostingBlockStats(),
    termCount: reduced.termCount,
    postingCount: reduced.postingCount,
    packBytes: reduced.packWriter.bytes,
    blockPackBytes: reduced.blockPackWriter?.bytes || 0,
    reduceWorkers: workerCount
  };
}

export async function build({ configPath }) {
  const config = await readConfig(configPath);
  const dirs = {
    out: config.output,
    runsOut: resolve(config.output, "_build", "runs"),
    typoRunsOut: resolve(config.output, "_build", "typo-runs")
  };
  rmSync(dirs.out, { recursive: true, force: true });
  mkdirSync(resolve(dirs.out, "docs"), { recursive: true });
  mkdirSync(resolve(dirs.out, "terms"), { recursive: true });
  mkdirSync(dirs.runsOut, { recursive: true });

  console.log(`Rangefind: reading ${config.input}`);
  const measured = await measure(config);
  const typo = typoOptions(config);
  const typoBuffer = typo.enabled ? createTypoRunBuffer(dirs.typoRunsOut, typo) : null;
  const runData = await writePostingRuns(config, measured, dirs, typoBuffer);
  const reduced = await reduceRuns(config, measured, runData, dirs, typoBuffer);
  const typoManifest = await reduceTypoRuns(typoBuffer, dirs.out);
  writeFileSync(resolve(dirs.out, "codes.bin.gz"), gzipSync(buildCodesFile(config, measured.total, runData.codes), { level: 9 }));

  const manifest = {
    version: 1,
    engine: "rangefind",
    built_at: new Date().toISOString(),
    total: measured.total,
    doc_chunk_size: config.docChunkSize,
    docs: runData.docs,
    initial_results: runData.initialResults,
    fields: config.fields.map(({ name, weight, b, phrase, proximity, proximityWeight }) => ({ name, weight, b, phrase: !!phrase, proximity: !!proximity, proximityWeight: proximityWeight || 0 })),
    facets: Object.fromEntries(Object.entries(measured.dicts).map(([name, dict]) => [name, dict.values])),
    numbers: config.numbers.map(n => ({ name: n.name })),
    block_filters: reduced.filters,
    directory: reduced.directory,
    typo: typoManifest ? {
      format: typoManifest.format,
      compression: typoManifest.compression,
      directory: typoManifest.directory,
      shards: typoManifest.directory.entries,
      packs: typoManifest.packs.length,
      stats: { ...typoManifest.stats, pack_files: typoManifest.packs.length }
    } : null,
    stats: {
      terms: reduced.termCount,
      postings: reduced.postingCount,
      term_storage: "range-pack-v1",
      posting_block_storage: config.externalPostingBlocks === false ? "inline" : "range-pack-v1",
      term_directory_format: reduced.directory.format,
      term_directory_page_files: reduced.directory.page_files,
      term_directory_bytes: reduced.directory.total_bytes,
      term_pack_files: reduced.packs.length,
      term_pack_bytes: reduced.packBytes,
      posting_block_pack_files: reduced.blockPacks.length,
      posting_block_pack_bytes: reduced.blockPackBytes,
      external_posting_blocks: reduced.blockStats.externalBlocks,
      external_posting_terms: reduced.blockStats.externalTerms,
      external_posting_postings: reduced.blockStats.externalPostings,
      external_posting_source_bytes: reduced.blockStats.externalPostingBytes,
      inline_posting_source_bytes: reduced.blockStats.inlinePostingBytes,
      doc_storage: runData.docs.storage,
      doc_directory_format: runData.docs.directory.format,
      doc_directory_page_files: runData.docs.directory.page_files,
      doc_directory_bytes: runData.docs.directory.total_bytes,
      doc_pack_files: runData.docs.packs.length,
      doc_pack_bytes: runData.docs.packs.reduce((sum, pack) => sum + pack.bytes, 0),
      reduce_workers: reduced.reduceWorkers,
      posting_block_size: config.postingBlockSize,
      base_shard_depth: config.baseShardDepth,
      max_shard_depth: config.maxShardDepth,
      target_shard_postings: config.targetShardPostings,
      max_expansion_terms_per_doc: config.maxExpansionTermsPerDoc,
      proximity_window: Math.max(0, ...config.fields.map(field => field.proximityWindow || 0)),
      scoring: "rangefind-bm25f-phrase-proximity-v2"
    }
  };
  writeFileSync(resolve(dirs.out, "manifest.json"), JSON.stringify(manifest));
  rmSync(resolve(dirs.out, "_build"), { recursive: true, force: true });
  console.log(`Rangefind: built ${measured.total.toLocaleString()} docs, ${reduced.shards.length.toLocaleString()} logical shards, ${reduced.packs.length.toLocaleString()} packs`);
}
