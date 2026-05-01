import {
  appendFileSync,
  readdirSync,
  readFileSync,
  mkdirSync,
  openSync,
  closeSync,
  readSync,
  rmSync,
  unlinkSync,
  writeSync,
  writeFileSync
} from "node:fs";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { Worker } from "node:worker_threads";
import { gzipSync, gunzipSync } from "node:zlib";
import { tokenize } from "./analyzer.js";
import {
  buildBlockFilters,
  buildDocValueChunk,
  buildFacetDictionary,
  buildTermShard,
  docValueFields,
  rewriteTermShardForExternalBlocks
} from "./codec.js";
import { getPath, readConfig } from "./config.js";
import { docLayoutRecord, orderDocIdsByLocality, summarizeDocLayout } from "./doc_layout.js";
import { buildDocPagePointerTable, DOC_PAGE_FORMAT } from "./doc_pages.js";
import { writeDirectoryFiles } from "./directory_writer.js";
import { buildDocOrdinalTable, buildDocPointerTable } from "./doc_pointers.js";
import { eachJsonLine } from "./jsonl.js";
import { OBJECT_CHECKSUM_ALGORITHM, OBJECT_NAME_HASH_LENGTH, OBJECT_POINTER_FORMAT, OBJECT_STORE_FORMAT } from "./object_store.js";
import { createPackWriter, finalizePackWriter, writePackedShard } from "./packs.js";
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

function rawPath(object, path, fallback = "") {
  if (!path) return fallback;
  let value = object;
  for (const part of String(path).split(".")) {
    if (value == null) return fallback;
    value = value[part];
  }
  return value ?? fallback;
}

function valueList(value) {
  if (Array.isArray(value)) return value;
  if (value == null || value === "") return [];
  return [value];
}

function normalizedNumberType(field) {
  return String(field.type || "int").toLowerCase();
}

function numericValue(doc, field) {
  const value = rawPath(doc, field.path);
  if (value == null || value === "") return null;
  const type = normalizedNumberType(field);
  if (type === "date") {
    const time = value instanceof Date ? value.getTime() : Date.parse(String(value));
    return Number.isFinite(time) ? time : null;
  }
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  if (type === "float" || type === "double") return number;
  return Math.round(number);
}

function booleanValue(doc, field) {
  const value = rawPath(doc, field.path);
  if (value === true || value === 1 || value === "true" || value === "1") return true;
  if (value === false || value === 0 || value === "false" || value === "0") return false;
  return null;
}

function facetValues(doc, facet) {
  const labels = valueList(rawPath(doc, facet.labelPath || facet.path));
  return valueList(rawPath(doc, facet.path)).map((value, index) => ({
    value,
    label: labels[index] ?? value
  }));
}

function addBit(words, value) {
  if (value <= 0) return;
  const word = Math.floor(value / 32);
  const bit = value % 32;
  words[word] |= 2 ** bit;
}

function facetBits(dict, values) {
  const words = Math.max(1, Math.ceil(dict.values.length / 32));
  const out = new Array(words).fill(0);
  for (const value of values) addBit(out, value);
  return out;
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
      for (const item of facetValues(doc, facet)) {
        const code = addDict(dicts[facet.name], item.value, item.label);
        dicts[facet.name].values[code].n++;
      }
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

function docPageKey(index) {
  return String(index).padStart(8, "0");
}

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function hashedFile(prefix, hash, suffix) {
  return `${prefix}.${hash.slice(0, OBJECT_NAME_HASH_LENGTH)}${suffix}`;
}

function createDocSpool(outDir) {
  mkdirSync(outDir, { recursive: true });
  const path = resolve(outDir, "payloads.bin");
  return {
    path,
    fd: openSync(path, "w"),
    offset: 0,
    bytes: 0,
    entries: [],
    layout: []
  };
}

function closeDocSpool(spool) {
  if (spool.fd == null) return;
  closeSync(spool.fd);
  spool.fd = null;
}

function writeSpooledDoc(spool, payload, index, layoutRecord) {
  const bytes = Buffer.from(JSON.stringify(payload));
  const compressed = gzipSync(bytes, { level: 6 });
  writeSync(spool.fd, compressed, 0, compressed.length, spool.offset);
  spool.entries[index] = {
    offset: spool.offset,
    length: compressed.length,
    logicalLength: bytes.length
  };
  spool.layout[index] = layoutRecord;
  spool.offset += compressed.length;
  spool.bytes += compressed.length;
}

function readSpooledDoc(fd, entry) {
  const buffer = Buffer.alloc(entry.length);
  const bytesRead = readSync(fd, buffer, 0, entry.length, entry.offset);
  if (bytesRead !== entry.length) throw new Error("Rangefind doc spool ended before a payload could be read.");
  return buffer;
}

function finishDocPacks(out, spool, total, config) {
  const packWriter = createPackWriter(resolve(out, "docs", "packs"), config.docPackBytes);
  const order = orderDocIdsByLocality(spool.layout, total);
  const entriesByDoc = new Array(total);
  const fd = openSync(spool.path, "r");
  try {
    for (const index of order) {
      const entry = spool.entries[index];
      if (!entry) throw new Error(`Rangefind doc spool is missing document ${index}.`);
      writePackedShard(packWriter, docIndexKey(index), readSpooledDoc(fd, entry), {
        kind: "doc",
        codec: "json-v1",
        logicalLength: entry.logicalLength
      });
      entriesByDoc[index] = packWriter.entries[docIndexKey(index)];
    }
  } finally {
    closeSync(fd);
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const pointerTable = buildDocPointerTable(order.map(index => entriesByDoc[index]), packIndexes);
  const hash = sha256Hex(pointerTable.buffer);
  const file = `docs/pointers/${hashedFile("0000", hash, ".bin")}`;
  mkdirSync(resolve(out, "docs", "pointers"), { recursive: true });
  writeFileSync(resolve(out, file), pointerTable.buffer);
  const ordinalTable = buildDocOrdinalTable(order, total);
  const ordinalHash = sha256Hex(ordinalTable.buffer);
  const ordinalFile = `docs/ordinals/${hashedFile("0000", ordinalHash, ".bin")}`;
  mkdirSync(resolve(out, "docs", "ordinals"), { recursive: true });
  writeFileSync(resolve(out, ordinalFile), ordinalTable.buffer);
  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    layout: {
      ...summarizeDocLayout(spool.layout, total, config),
      spool_bytes: spool.bytes
    },
    pointers: {
      ...pointerTable.meta,
      file,
      order: "layout",
      content_hash: hash,
      immutable: true,
      bytes: pointerTable.buffer.length,
      pack_table: packTable(packWriter.packs),
      ordinals: {
        ...ordinalTable.meta,
        file: ordinalFile,
        content_hash: ordinalHash,
        immutable: true,
        bytes: ordinalTable.buffer.length
      }
    },
    packs: packWriter.packs
  };
}

function finishDocPages(out, spool, total, config) {
  const pageSize = Math.max(1, Math.floor(Number(config.docPageSize || 32)));
  const packWriter = createPackWriter(resolve(out, "docs", "page-packs"), config.docPagePackBytes || config.docPackBytes);
  const entries = [];
  const fd = openSync(spool.path, "r");
  try {
    for (let pageStart = 0, pageIndex = 0; pageStart < total; pageStart += pageSize, pageIndex++) {
      const pageEnd = Math.min(total, pageStart + pageSize);
      const parts = [];
      let logicalLength = 2;
      for (let index = pageStart; index < pageEnd; index++) {
        const entry = spool.entries[index];
        if (!entry) throw new Error(`Rangefind doc spool is missing document ${index}.`);
        const payload = gunzipSync(readSpooledDoc(fd, entry)).toString("utf8");
        parts.push(payload);
        logicalLength += Buffer.byteLength(payload) + (parts.length > 1 ? 1 : 0);
      }
      const source = Buffer.from(`[${parts.join(",")}]`);
      const packed = writePackedShard(packWriter, docPageKey(pageIndex), gzipSync(source, { level: 6 }), {
        kind: "doc-page",
        codec: DOC_PAGE_FORMAT,
        logicalLength
      });
      entries[pageIndex] = packed;
    }
  } finally {
    closeSync(fd);
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const pointerTable = buildDocPagePointerTable(entries, packIndexes);
  const hash = sha256Hex(pointerTable.buffer);
  const file = `docs/pages/${hashedFile("0000", hash, ".bin")}`;
  mkdirSync(resolve(out, "docs", "pages"), { recursive: true });
  writeFileSync(resolve(out, file), pointerTable.buffer);
  return {
    storage: "range-pack-v1",
    format: DOC_PAGE_FORMAT,
    compression: "gzip-member",
    page_size: pageSize,
    max_overfetch_docs: Math.max(1, Math.floor(Number(config.docPageMaxOverfetchDocs || 16))),
    pointers: {
      ...pointerTable.meta,
      file,
      order: "doc-id-page",
      content_hash: hash,
      immutable: true,
      bytes: pointerTable.buffer.length,
      pack_table: packTable(packWriter.packs)
    },
    packs: packWriter.packs
  };
}

function writeFacetDictionaries(out, dicts, config) {
  const packWriter = createPackWriter(resolve(out, "facets", "packs"), config.facetDictionaryPackBytes || config.packBytes);
  const fields = {};
  for (const [name, dict] of Object.entries(dicts || {})) {
    const values = dict.values || [];
    const source = buildFacetDictionary(values);
    const entry = writePackedShard(packWriter, name, gzipSync(source, { level: 6 }), {
      kind: "facet-dictionary",
      codec: "rffacetdict-v1",
      logicalLength: source.length
    });
    fields[name] = {
      count: values.length,
      source_bytes: source.length,
      bytes: entry.length
    };
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const entries = Object.keys(dicts || {}).map((name) => {
    const entry = packWriter.entries[name];
    return { shard: name, packIndex: packIndexes.get(entry.pack), ...entry };
  }).filter(entry => entry.pack);
  const directory = writeDirectoryFiles(resolve(out, "facets"), entries, config.directoryPageBytes, "facets", { packTable: packWriter.packs });
  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    format: "rffacetdict-v1",
    directory,
    packs: "facets/packs/",
    pack_table: packTable(packWriter.packs),
    pack_objects: packWriter.packs,
    pack_files: packWriter.packs.length,
    pack_bytes: packWriter.packs.reduce((sum, pack) => sum + pack.bytes, 0),
    fields
  };
}

function writeDocValuePacks(out, config, total, codes) {
  const chunkSize = Math.max(1, Number(config.docValueChunkSize || 2048));
  const packWriter = createPackWriter(resolve(out, "doc-values", "packs"), config.docValuePackBytes);
  const fields = {};
  for (const field of docValueFields(config, codes)) {
    const chunks = [];
    for (let start = 0; start < total; start += chunkSize) {
      const rows = (codes[field.name] || []).slice(start, Math.min(total, start + chunkSize));
      const encoded = buildDocValueChunk(field, start, rows);
      const key = `${field.name}\u0000${chunks.length}`;
      const entry = writePackedShard(packWriter, key, gzipSync(encoded.buffer, { level: 6 }), {
        kind: "doc-value",
        codec: "rfdocvalues-v1",
        logicalLength: encoded.buffer.length
      });
      chunks.push({
        key,
        start,
        count: rows.length,
        pack: entry.pack,
        offset: entry.offset,
        length: entry.length,
        physicalLength: entry.physicalLength,
        logicalLength: entry.logicalLength,
        checksum: entry.checksum,
        width: encoded.width,
        min: encoded.summary?.min ?? null,
        max: encoded.summary?.max ?? null,
        words: encoded.summary?.words ?? null
      });
    }
    fields[field.name] = {
      name: field.name,
      kind: field.kind,
      type: field.type,
      words: field.words || 0,
      chunks
    };
  }
  finalizePackWriter(packWriter);
  const indexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  for (const field of Object.values(fields)) {
    for (const chunk of field.chunks) {
      const entry = packWriter.entries[chunk.key];
      Object.assign(chunk, {
        pack: entry.pack,
        offset: entry.offset,
        length: entry.length,
        physicalLength: entry.physicalLength,
        logicalLength: entry.logicalLength,
        checksum: entry.checksum,
        packIndex: indexes.get(entry.pack)
      });
      delete chunk.key;
    }
  }
  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    format: "rfdocvalues-v1",
    chunk_size: chunkSize,
    fields,
    packs: packWriter.packs
  };
}

function packTable(packs) {
  return (packs || []).map(pack => pack.file);
}

function summarizeDedup(...packSets) {
  const packs = packSets.flat().filter(Boolean);
  return {
    strategy: "sha256-exact-compressed-object",
    objects: packs.reduce((sum, pack) => sum + (pack.objects || 0), 0),
    references: packs.reduce((sum, pack) => sum + (pack.references || pack.shards || 0), 0),
    deduped_objects: packs.reduce((sum, pack) => sum + (pack.dedupedObjects || 0), 0),
    deduped_bytes: packs.reduce((sum, pack) => sum + (pack.dedupedBytes || 0), 0)
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
    return writePackedShard(blockPackWriter, key, gzipSync(Buffer.from(bytes), { level: 6 }), {
      kind: "posting-block",
      codec: "rfpostings-v1",
      logicalLength: bytes.length
    });
  });
}

async function writePostingRuns(config, measured, dirs, typoBuffer) {
  const codes = {};
  for (const facet of config.facets) codes[facet.name] = new Array(measured.total);
  for (const number of config.numbers) codes[number.name] = new Array(measured.total);
  for (const boolean of config.booleans || []) codes[boolean.name] = new Array(measured.total);
  codes._dicts = measured.dicts;

  const initialResults = [];
  const buffer = { byShard: new Map(), lines: 0, runsOut: dirs.runsOut };
  const docSpool = createDocSpool(resolve(dirs.out, "_build", "docs"));
  const baseShards = new Set();

  try {
    await eachJsonLine(config.input, async (doc, index) => {
      const weighted = new Map();
      const expansion = new Map();
      for (const field of config.fields) addFieldScores(doc, field, measured.avgLens[field.name], weighted);
      for (const field of config.fields) addFieldExpansionScores(doc, field, expansion);
      const selectedTerms = selectDocTerms(
        bm25fScores(weighted, config.bm25fK1),
        expansion,
        config.maxTermsPerDoc,
        config.maxExpansionTermsPerDoc
      );
      for (const [term, score] of selectedTerms) {
        bufferPosting(buffer, config, term, index, score);
        baseShards.add(baseShardFor(term, config));
      }

      for (const facet of config.facets) {
        const values = [];
        for (const item of facetValues(doc, facet)) {
          const code = addDict(measured.dicts[facet.name], item.value, item.label);
          values.push(code);
        }
        codes[facet.name][index] = facetBits(measured.dicts[facet.name], values);
      }
      for (const number of config.numbers) codes[number.name][index] = numericValue(doc, number);
      for (const boolean of config.booleans || []) codes[boolean.name][index] = booleanValue(doc, boolean);
      addTypoSurfacePairs(typoBuffer, surfacePairsForFields(doc, config.fields, fieldText));

      const payload = docPayload(doc, config, index);
      if (initialResults.length < config.initialResultLimit) initialResults.push(payload);
      writeSpooledDoc(docSpool, payload, index, docLayoutRecord(index, selectedTerms, config));
    });
  } finally {
    closeDocSpool(docSpool);
  }
  flushPostingBuffer(buffer);
  return {
    codes,
    initialResults,
    baseShards: [...baseShards].sort(),
    docSpool
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
    writePackedShard(packWriter, partition.name, gzipSync(externalized.buffer, { level: 6 }), {
      kind: "term-shard",
      codec: "rfterm-v1",
      logicalLength: externalized.buffer.length
    });
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

function workerCodeTables(codes) {
  const cloned = {};
  for (const [name, values] of Object.entries(codes || {})) {
    if (name === "_dicts") continue;
    cloned[name] = values;
  }
  return cloned;
}

async function reduceRunsParallel(config, measured, runData, dirs, typoBuffer, filters, workerCount) {
  const shardOutRoot = resolve(dirs.out, "_build", "term-shards");
  mkdirSync(shardOutRoot, { recursive: true });
  const sharedCodes = workerCodeTables(runData.codes);
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
      writePackedShard(packWriter, item.shard, gzipSync(externalized.buffer, { level: 6 }), {
        kind: "term-shard",
        codec: "rfterm-v1",
        logicalLength: externalized.buffer.length
      });
      finalShards.add(item.shard);
    }
  }
  if (blockPackWriter) finalizePackWriter(blockPackWriter);
  finalizePackWriter(packWriter);
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
    if (blockPackWriter) finalizePackWriter(blockPackWriter);
    finalizePackWriter(packWriter);
    reduced = { finalShards, packWriter, blockPackWriter, blockStats, termCount, postingCount };
  }
  const shards = [...reduced.finalShards].sort();
  const packIndexes = new Map(reduced.packWriter.packs.map((pack, index) => [pack.file, index]));
  const entries = shards.map((shard) => {
    const entry = reduced.packWriter.entries[shard];
    return { shard, packIndex: packIndexes.get(entry.pack), ...entry };
  });
  const directory = writeDirectoryFiles(resolve(dirs.out, "terms"), entries, config.directoryPageBytes, "terms", { packTable: reduced.packWriter.packs });
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
  const docs = finishDocPacks(dirs.out, runData.docSpool, measured.total, config);
  docs.pages = finishDocPages(dirs.out, runData.docSpool, measured.total, config);
  const typoManifest = await reduceTypoRuns(typoBuffer, dirs.out);
  const docValues = writeDocValuePacks(dirs.out, config, measured.total, runData.codes);
  const facetDictionaries = writeFacetDictionaries(dirs.out, measured.dicts, config);

  const manifest = {
    version: 1,
    engine: "rangefind",
    features: {
      objectPointers: true,
      checksummedObjects: true,
      contentAddressedObjects: true,
      deduplicatedObjects: true,
      denseDocPointers: true,
      docLocalityLayout: true,
      docPages: true,
      rangeDirectoryV2: true,
      docValues: true,
      facetDictionaries: true,
      externalPostingBlocks: config.externalPostingBlocks !== false,
      typoSidecar: !!typoManifest
    },
    object_store: {
      format: OBJECT_STORE_FORMAT,
      pointer_format: OBJECT_POINTER_FORMAT,
      checksum: OBJECT_CHECKSUM_ALGORITHM,
      compression: "gzip-member",
      immutable_names: true,
      name_hash: {
        algorithm: OBJECT_CHECKSUM_ALGORITHM,
        length: OBJECT_NAME_HASH_LENGTH
      },
      pack_table: {
        terms: packTable(reduced.packs),
        postingBlocks: packTable(reduced.blockPacks),
        docs: packTable(docs.packs),
        docPages: packTable(docs.pages.packs),
        docValues: packTable(docValues.packs),
        facets: packTable(facetDictionaries.pack_objects),
        typo: packTable(typoManifest?.packs)
      },
      dedupe: summarizeDedup(
        reduced.packs,
        reduced.blockPacks,
        docs.packs,
        docs.pages.packs,
        docValues.packs,
        facetDictionaries.pack_objects,
        typoManifest?.packs || []
      ),
      directories: {
        terms: reduced.directory,
        facets: facetDictionaries.directory,
        typo: typoManifest?.directory || null
      },
      pointers: {
        docs: docs.pointers,
        docPages: docs.pages.pointers
      }
    },
    built_at: new Date().toISOString(),
    total: measured.total,
    docs,
    doc_values: {
      storage: docValues.storage,
      compression: docValues.compression,
      format: docValues.format,
      chunk_size: docValues.chunk_size,
      fields: docValues.fields,
      packs: docValues.packs.length
    },
    initial_results: runData.initialResults,
    fields: config.fields.map(({ name, weight, b, phrase, proximity, proximityWeight }) => ({ name, weight, b, phrase: !!phrase, proximity: !!proximity, proximityWeight: proximityWeight || 0 })),
    facets: Object.fromEntries(Object.entries(facetDictionaries.fields).map(([name, field]) => [name, { count: field.count }])),
    facet_dictionaries: facetDictionaries,
    numbers: config.numbers.map(n => ({ name: n.name, type: normalizedNumberType(n), sortable: n.sortable !== false })),
    booleans: (config.booleans || []).map(n => ({ name: n.name, sortable: n.sortable !== false })),
    sorts: config.sorts || [],
    block_filters: reduced.filters,
    directory: reduced.directory,
    typo: typoManifest ? {
      format: typoManifest.format,
      compression: typoManifest.compression,
      manifest: typoManifest.manifest,
      manifest_hash: typoManifest.manifest_hash,
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
      doc_storage: docs.storage,
      doc_layout_format: docs.layout.format,
      doc_layout_primary_terms: docs.layout.primary_terms,
      doc_pointer_format: docs.pointers.format,
      doc_pointer_bytes: docs.pointers.bytes,
      doc_pointer_record_bytes: docs.pointers.recordBytes,
      doc_pack_files: docs.packs.length,
      doc_pack_bytes: docs.packs.reduce((sum, pack) => sum + pack.bytes, 0),
      doc_page_format: docs.pages.format,
      doc_page_size: docs.pages.page_size,
      doc_page_max_overfetch_docs: docs.pages.max_overfetch_docs,
      doc_page_pointer_format: docs.pages.pointers.format,
      doc_page_pointer_bytes: docs.pages.pointers.bytes,
      doc_page_pack_files: docs.pages.packs.length,
      doc_page_pack_bytes: docs.pages.packs.reduce((sum, pack) => sum + pack.bytes, 0),
      doc_value_storage: docValues.storage,
      doc_value_format: docValues.format,
      doc_value_chunk_size: docValues.chunk_size,
      doc_value_fields: Object.keys(docValues.fields).length,
      doc_value_pack_files: docValues.packs.length,
      doc_value_pack_bytes: docValues.packs.reduce((sum, pack) => sum + pack.bytes, 0),
      facet_dictionary_storage: facetDictionaries.storage,
      facet_dictionary_format: facetDictionaries.format,
      facet_dictionary_page_files: facetDictionaries.directory.page_files,
      facet_dictionary_bytes: facetDictionaries.directory.total_bytes + facetDictionaries.pack_bytes,
      facet_dictionary_fields: Object.keys(facetDictionaries.fields).length,
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
