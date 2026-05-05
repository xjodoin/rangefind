import {
  createReadStream,
  existsSync,
  mkdirSync,
  openSync,
  closeSync,
  readFileSync,
  readSync,
  renameSync,
  statSync,
  writeSync,
  writeFileSync
} from "node:fs";
import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { createInterface } from "node:readline";
import { performance } from "node:perf_hooks";
import { Worker } from "node:worker_threads";
import { expandedTermsFromBaseTerms, queryBundleKeyFromBaseTerms, tokenize } from "./analyzer.js";
import { addAuthorityDoc, createAuthorityRunBuffer, finishAuthorityRuns, reduceAuthorityRuns } from "./authority_index.js";
import { addBuildCounter, createBuildTelemetry, finishBuildTelemetry, recordBuildWorkers, timeBuildPhase } from "./build_telemetry.js";
import { createCodeStore, openCodeStore } from "./build_store.js";
import {
  buildBlockFilters,
  buildDocValueChunk,
  buildFacetDictionary,
  buildPostingSegmentChunks,
  docValueFields,
  POSTING_SEGMENT_FORMAT
} from "./codec.js";
import { getPath, readConfig } from "./config.js";
import { DOC_LAYOUT_FORMAT, docLayoutRecord } from "./doc_layout.js";
import { buildDocPagePointerTable, DOC_PAGE_ENCODING, DOC_PAGE_FORMAT, encodeDocPageColumns } from "./doc_pages.js";
import { writeDirectoryFiles, writeDirectoryFilesFromSortedEntries } from "./directory_writer.js";
import { appendDirectoryEntry, createDirectoryEntrySpool, sortedDirectoryEntrySpool } from "./directory_spool.js";
import {
  DOC_VALUE_SORT_DIRECTORY_FORMAT,
  DOC_VALUE_SORT_PAGE_FORMAT,
  encodeDocValueSortDirectory,
  encodeDocValueSortPage
} from "./doc_value_tree.js";
import { createFilterBitmap, encodeFilterBitmap, FILTER_BITMAP_FORMAT, setFilterBitmapBit } from "./filter_bitmaps.js";
import { buildDocOrdinalTable, buildDocPointerTableFromReader } from "./doc_pointers.js";
import { eachJsonLine } from "./jsonl.js";
import { createFieldRowPipeline } from "./field_rows.js";
import { OBJECT_CHECKSUM_ALGORITHM, OBJECT_NAME_HASH_LENGTH, OBJECT_POINTER_FORMAT, OBJECT_STORE_FORMAT } from "./object_store.js";
import { buildIndexOptimizerReport, INDEX_OPTIMIZER_PATH } from "./optimizer.js";
import { createAppendOnlyPackWriter, createPackWriter, finalizePackWriter, resolvePackEntry, writePackedShard, writePackedShardChunks } from "./packs.js";
import { partitionInputBytes, partitionTermEntries } from "./reduced_terms.js";
import { addQueryBundleRow, createQueryBundleCollector, queryBundleCollectorResults, writeQueryBundleObjects } from "./query_bundles.js";
import { tryReadVarint, varintLength, writeVarint } from "./runs.js";
import { analyzeDocumentTerms, fieldIndexText } from "./scoring.js";
import { addSegmentPosting, createSegmentBuilder, finishSegmentBuilder, flushSegment, shouldFlushSegment } from "./segment_builder.js";
import { mergeSegmentsToPartitions, segmentMergeSummary } from "./segment_merge.js";
import { writeSegmentManifest } from "./segment_manifest.js";
import {
  addTypoIndexTerm,
  addTypoSurfacePairsToBuffer,
  createTypoSurfacePairBuffer,
  flushTypoSurfacePairBuffer,
  createTypoRunBuffer,
  reduceTypoRuns,
  surfacePairsForFields,
  typoOptions
} from "./typo.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const SORT_REPLICA_FORMAT = "rfsortreplicas-v1";
const SORT_REPLICA_RANK_MAP_FORMAT = "rfsortrankmap-v1";
const SORT_REPLICA_RANK_RECORD_BYTES = 12;

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

async function measure(config) {
  const fieldTotals = Object.fromEntries(config.fields.map(field => [field.name, 0]));
  const dicts = Object.fromEntries(config.facets.map(facet => [facet.name, { ids: new Map(), values: [{ value: "", label: "", n: 0 }] }]));
  let total = 0;
  await eachJsonLine(config.input, async (doc) => {
    total++;
    for (const field of config.fields) {
      fieldTotals[field.name] += tokenize(fieldIndexText(doc, field, config), { unique: false }).length;
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

function queryBundlesEnabled(config) {
  return config.queryBundles !== false && Math.max(0, Number(config.queryBundleMaxKeys || 0)) > 0;
}

function isBundlePhraseTerm(term, config) {
  if (!term || term.startsWith("n_") || !term.includes("_")) return false;
  const parts = term.split("_").filter(Boolean);
  const maxTerms = Math.max(2, Math.min(3, Math.floor(Number(config.queryBundleMaxTerms || 3))));
  return parts.length >= 2 && parts.length <= maxTerms && parts.every(part => part && !part.includes("_"));
}

function createQueryBundleSeedBuffer(config) {
  const maxKeys = Math.max(0, Math.floor(Number(config.queryBundleMaxKeys || 0)));
  const factor = Math.max(1, Number(config.queryBundleSeedCandidateFactor || 4));
  return queryBundlesEnabled(config)
    ? { counts: new Map(), seeds: new Map(), enabled: true, maxSeedCandidates: Math.max(maxKeys, Math.floor(maxKeys * factor)) }
    : { counts: new Map(), seeds: new Map(), enabled: false, maxSeedCandidates: 0 };
}

function addQueryBundleSeed(buffer, baseTerms, selected, docKeys) {
  if (!baseTerms.every(base => selected.has(base))) return;
  const key = queryBundleKeyFromBaseTerms(baseTerms);
  if (!key || docKeys.has(key)) return;
  docKeys.add(key);
  if (!buffer.seeds.has(key)) {
    if (buffer.seeds.size >= buffer.maxSeedCandidates) return;
    buffer.seeds.set(key, {
      key,
      baseTerms,
      expandedTerms: expandedTermsFromBaseTerms(baseTerms)
    });
  }
  buffer.counts.set(key, (buffer.counts.get(key) || 0) + 1);
}

function addQueryBundleSeeds(buffer, selectedTerms, config, doc) {
  if (!buffer.enabled) return;
  const selected = new Set(selectedTerms.map(([term]) => term));
  const docKeys = new Set();
  const maxTerms = Math.max(2, Math.min(3, Math.floor(Number(config.queryBundleMaxTerms || 3))));
  for (const [term] of selectedTerms) {
    if (!isBundlePhraseTerm(term, config)) continue;
    addQueryBundleSeed(buffer, term.split("_"), selected, docKeys);
  }
  for (const field of config.fields) {
    const limit = Math.max(0, Math.floor(Number(field.queryBundleSeedMaxTokens ?? config.queryBundleSeedMaxFieldTokens ?? 512)));
    if (!limit || field.queryBundles === false) continue;
    const terms = tokenize(fieldIndexText(doc, field, config), { unique: false }).slice(0, limit);
    for (let n = 2; n <= maxTerms; n++) {
      for (let i = 0; i <= terms.length - n; i++) {
        const baseTerms = terms.slice(i, i + n);
        if (new Set(baseTerms).size !== baseTerms.length) continue;
        addQueryBundleSeed(buffer, baseTerms, selected, docKeys);
      }
    }
  }
}

function finalizeQueryBundleSeeds(buffer, config) {
  if (!buffer.enabled || !buffer.seeds.size) return [];
  const minDocs = Math.max(1, Math.floor(Number(config.queryBundleMinSeedDocs || 1)));
  const maxKeys = Math.max(0, Math.floor(Number(config.queryBundleMaxKeys || 0)));
  return [...buffer.seeds.values()]
    .map(seed => ({ ...seed, seedDocs: buffer.counts.get(seed.key) || 0 }))
    .filter(seed => seed.seedDocs >= minDocs)
    .sort((a, b) => b.seedDocs - a.seedDocs || a.key.localeCompare(b.key))
    .slice(0, maxKeys);
}

function queryBundleTerms(seeds) {
  const terms = new Set();
  for (const seed of seeds || []) {
    for (const term of seed.expandedTerms || []) terms.add(term);
  }
  return [...terms].sort();
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

function docPayloadFieldNames(config) {
  const fields = ["id"];
  for (const item of config.display) fields.push(typeof item === "string" ? item : item.name);
  fields.push("title", "url");
  return [...new Set(fields)].filter(field => field && field !== "index");
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

const DOC_SPOOL_ENTRY_BYTES = 24;
const PACKED_DOC_ENTRY_BYTES = 60;

function createDocSpool(outDir) {
  mkdirSync(outDir, { recursive: true });
  const path = resolve(outDir, "payloads.bin");
  const entryPath = resolve(outDir, "payloads.idx");
  const rawPath = resolve(outDir, "payloads.raw.bin");
  const rawEntryPath = resolve(outDir, "payloads.raw.idx");
  const layoutPath = resolve(outDir, "layout.jsonl");
  return {
    path,
    entryPath,
    rawPath,
    rawEntryPath,
    layoutPath,
    fd: openSync(path, "w"),
    entryFd: openSync(entryPath, "w"),
    rawFd: openSync(rawPath, "w"),
    rawEntryFd: openSync(rawEntryPath, "w"),
    layoutFd: openSync(layoutPath, "w"),
    offset: 0,
    rawOffset: 0,
    bytes: 0,
    rawBytes: 0,
    layoutDocs: 0
  };
}

function closeDocSpool(spool) {
  for (const key of ["fd", "entryFd", "rawFd", "rawEntryFd", "layoutFd"]) {
    if (spool[key] == null) continue;
    closeSync(spool[key]);
    spool[key] = null;
  }
}

function writeBigUInt(buffer, offset, value) {
  buffer.writeBigUInt64LE(BigInt(Math.max(0, Math.floor(value || 0))), offset);
}

function readBigUInt(buffer, offset) {
  return Number(buffer.readBigUInt64LE(offset));
}

function writeDocSpoolEntry(spoolOrFd, index, entry) {
  const fd = typeof spoolOrFd === "number" ? spoolOrFd : spoolOrFd.entryFd;
  const buffer = Buffer.alloc(DOC_SPOOL_ENTRY_BYTES);
  writeBigUInt(buffer, 0, entry.offset);
  writeBigUInt(buffer, 8, entry.length);
  writeBigUInt(buffer, 16, entry.logicalLength);
  writeSync(fd, buffer, 0, buffer.length, index * DOC_SPOOL_ENTRY_BYTES);
}

function readDocSpoolEntry(fd, index) {
  const buffer = Buffer.alloc(DOC_SPOOL_ENTRY_BYTES);
  const bytesRead = readSync(fd, buffer, 0, buffer.length, index * DOC_SPOOL_ENTRY_BYTES);
  if (bytesRead !== buffer.length) throw new Error(`Rangefind doc spool is missing document ${index}.`);
  return {
    offset: readBigUInt(buffer, 0),
    length: readBigUInt(buffer, 8),
    logicalLength: readBigUInt(buffer, 16)
  };
}

function writeSpooledDoc(spool, payload, index, layoutRecord) {
  const bytes = Buffer.from(JSON.stringify(payload));
  writeSync(spool.rawFd, bytes, 0, bytes.length, spool.rawOffset);
  writeDocSpoolEntry(spool.rawEntryFd, index, {
    offset: spool.rawOffset,
    length: bytes.length,
    logicalLength: bytes.length
  });
  const compressed = gzipSync(bytes, { level: 6 });
  writeSync(spool.fd, compressed, 0, compressed.length, spool.offset);
  writeDocSpoolEntry(spool, index, {
    offset: spool.offset,
    length: compressed.length,
    logicalLength: bytes.length
  });
  writeSync(spool.layoutFd, `${JSON.stringify(layoutRecord)}\n`);
  spool.layoutDocs++;
  spool.rawOffset += bytes.length;
  spool.rawBytes += bytes.length;
  spool.offset += compressed.length;
  spool.bytes += compressed.length;
}

function readSpooledDoc(fd, entry) {
  const buffer = Buffer.alloc(entry.length);
  const bytesRead = readSync(fd, buffer, 0, entry.length, entry.offset);
  if (bytesRead !== entry.length) throw new Error("Rangefind doc spool ended before a payload could be read.");
  return buffer;
}

function createSelectedTermSpool(outDir) {
  mkdirSync(outDir, { recursive: true });
  const path = resolve(outDir, "selected-terms.bin");
  return {
    path,
    fd: openSync(path, "w"),
    docs: 0,
    terms: 0,
    bytes: 0
  };
}

function closeSelectedTermSpool(spool) {
  if (!spool || spool.fd == null) return;
  closeSync(spool.fd);
  spool.fd = null;
}

function encodeSelectedTerms(selectedTerms) {
  const terms = selectedTerms.map(([term, score]) => [String(term || ""), Math.max(1, Math.round(score * 1000))]);
  let bytes = varintLength(terms.length);
  const encodedTerms = terms.map(([term, score]) => {
    const termBytes = textEncoder.encode(term);
    bytes += varintLength(termBytes.length) + termBytes.length + varintLength(score);
    return [termBytes, score];
  });
  const out = Buffer.allocUnsafe(bytes);
  let pos = writeVarint(out, 0, encodedTerms.length);
  for (const [termBytes, score] of encodedTerms) {
    pos = writeVarint(out, pos, termBytes.length);
    out.set(termBytes, pos);
    pos += termBytes.length;
    pos = writeVarint(out, pos, score);
  }
  return out;
}

function writeSelectedTerms(spool, selectedTerms) {
  const bytes = encodeSelectedTerms(selectedTerms);
  writeSync(spool.fd, bytes, 0, bytes.length);
  spool.docs++;
  spool.terms += selectedTerms.length;
  spool.bytes += bytes.length;
}

function selectedTermsFromBytes(bytes, state) {
  const start = state.pos;
  const count = tryReadVarint(bytes, state);
  if (count == null) {
    state.pos = start;
    return null;
  }
  const terms = new Array(count);
  for (let i = 0; i < count; i++) {
    const length = tryReadVarint(bytes, state);
    if (length == null || state.pos + length > bytes.length) {
      state.pos = start;
      return null;
    }
    const term = textDecoder.decode(bytes.subarray(state.pos, state.pos + length));
    state.pos += length;
    const score = tryReadVarint(bytes, state);
    if (score == null) {
      state.pos = start;
      return null;
    }
    terms[i] = [term, score];
  }
  return terms;
}

async function* readSelectedTermSpool(path) {
  let pending = Buffer.alloc(0);
  let doc = 0;
  for await (const chunk of createReadStream(path)) {
    const bytes = pending.length ? Buffer.concat([pending, chunk]) : chunk;
    const state = { pos: 0 };
    while (state.pos < bytes.length) {
      const selectedTerms = selectedTermsFromBytes(bytes, state);
      if (!selectedTerms) break;
      yield { doc: doc++, selectedTerms };
    }
    pending = state.pos < bytes.length ? bytes.subarray(state.pos) : Buffer.alloc(0);
  }
  if (pending.length) throw new Error(`Truncated Rangefind selected term spool: ${path}`);
}

function compareLayoutRecords(a, b) {
  if (!!a.primary !== !!b.primary) return a.primary ? -1 : 1;
  return String(a.shard || "").localeCompare(String(b.shard || ""))
    || String(a.primary || "").localeCompare(String(b.primary || ""))
    || (Number(b.score) || 0) - (Number(a.score) || 0)
    || String(a.secondary || "").localeCompare(String(b.secondary || ""))
    || (Number(a.index) || 0) - (Number(b.index) || 0);
}

function layoutTermLimit(config) {
  return Math.max(1, Math.floor(Number(config.docLocalityTerms || 2) || 2));
}

function layoutShardDepth(config) {
  return Math.max(1, Math.floor(Number(config.docLocalityShardDepth || config.baseShardDepth || 1) || 1));
}

function layoutSummary(total, config, stats) {
  return {
    format: DOC_LAYOUT_FORMAT,
    strategy: "primary-base-term-impact",
    terms: layoutTermLimit(config),
    shard_depth: layoutShardDepth(config),
    docs: total,
    docs_without_terms: stats.docsWithoutTerms,
    primary_terms: stats.primaryTerms
  };
}

function writeLayoutChunk(rows, outDir, chunkIndex) {
  rows.sort(compareLayoutRecords);
  const file = resolve(outDir, `layout-${String(chunkIndex).padStart(5, "0")}.jsonl`);
  writeFileSync(file, rows.map(row => `${JSON.stringify(row)}\n`).join(""));
  rows.length = 0;
  return file;
}

async function nextLayoutRow(reader) {
  const item = await reader.iterator.next();
  return item.done ? null : JSON.parse(item.value);
}

async function createLayoutReader(file) {
  const input = createReadStream(file);
  const rl = createInterface({ input, crlfDelay: Infinity });
  const reader = { input, rl, iterator: rl[Symbol.asyncIterator]() };
  reader.row = await nextLayoutRow(reader);
  return reader;
}

async function sortedLayoutOrder(spool, total, config) {
  const chunkDocs = Math.max(1, Math.floor(Number(config.docLayoutSortChunkDocs || 100000)));
  const rows = [];
  const chunks = [];
  const rl = createInterface({ input: createReadStream(spool.layoutPath), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    rows.push(JSON.parse(line));
    if (rows.length >= chunkDocs) chunks.push(writeLayoutChunk(rows, buildPath(config, "docs"), chunks.length));
  }
  if (rows.length) chunks.push(writeLayoutChunk(rows, buildPath(config, "docs"), chunks.length));

  const readers = await Promise.all(chunks.map(createLayoutReader));
  const order = [];
  const stats = { docsWithoutTerms: 0, primaryTerms: 0 };
  let lastPrimary = null;
  while (readers.some(reader => reader.row)) {
    let best = -1;
    for (let i = 0; i < readers.length; i++) {
      if (!readers[i].row) continue;
      if (best < 0 || compareLayoutRecords(readers[i].row, readers[best].row) < 0) best = i;
    }
    const row = readers[best].row;
    order.push(row.index);
    if (row.primary) {
      if (row.primary !== lastPrimary) stats.primaryTerms++;
      lastPrimary = row.primary;
    } else {
      stats.docsWithoutTerms++;
    }
    readers[best].row = await nextLayoutRow(readers[best]);
  }
  for (const reader of readers) {
    reader.rl.close();
    reader.input.destroy();
  }
  if (order.length !== total) throw new Error(`Rangefind doc layout expected ${total} docs but sorted ${order.length}.`);
  return { order, summary: layoutSummary(total, config, stats) };
}

function hexToBytes(hex) {
  const buffer = Buffer.alloc(32);
  for (let i = 0; i < buffer.length; i++) buffer[i] = Number.parseInt(String(hex).slice(i * 2, i * 2 + 2), 16);
  return buffer;
}

function bytesToHex(buffer, offset) {
  let out = "";
  for (let i = 0; i < 32; i++) out += buffer[offset + i].toString(16).padStart(2, "0");
  return out;
}

function tempPackIndex(file) {
  const match = /^(\d+)/u.exec(String(file || "0"));
  return match ? Number(match[1]) || 0 : 0;
}

function writePackedDocEntry(fd, doc, entry) {
  const buffer = Buffer.alloc(PACKED_DOC_ENTRY_BYTES);
  buffer.writeUInt32LE(tempPackIndex(entry.pack), 0);
  writeBigUInt(buffer, 4, entry.offset);
  writeBigUInt(buffer, 12, entry.length);
  writeBigUInt(buffer, 20, entry.logicalLength || 0);
  buffer.set(hexToBytes(entry.checksum.value), 28);
  writeSync(fd, buffer, 0, buffer.length, doc * PACKED_DOC_ENTRY_BYTES);
}

function readPackedDocEntry(fd, doc, packFiles) {
  const buffer = Buffer.alloc(PACKED_DOC_ENTRY_BYTES);
  const bytesRead = readSync(fd, buffer, 0, buffer.length, doc * PACKED_DOC_ENTRY_BYTES);
  if (bytesRead !== buffer.length) throw new Error(`Rangefind packed doc entry is missing document ${doc}.`);
  const packIndex = buffer.readUInt32LE(0);
  return {
    pack: packFiles[packIndex],
    offset: readBigUInt(buffer, 4),
    length: readBigUInt(buffer, 12),
    physicalLength: readBigUInt(buffer, 12),
    logicalLength: readBigUInt(buffer, 20),
    checksum: { algorithm: "sha256", value: bytesToHex(buffer, 28) }
  };
}

async function finishDocPacks(out, spool, total, config) {
  const layout = await sortedLayoutOrder(spool, total, config);
  const packWriter = createAppendOnlyPackWriter(resolve(out, "docs", "packs"), config.docPackBytes);
  const entryPath = buildPath(config, "docs", "doc-pack-entries.bin");
  const entryOutFd = openSync(entryPath, "w");
  const fd = openSync(spool.path, "r");
  const spoolEntryFd = openSync(spool.entryPath, "r");
  try {
    for (const index of layout.order) {
      const entry = readDocSpoolEntry(spoolEntryFd, index);
      const packed = writePackedShard(packWriter, docIndexKey(index), readSpooledDoc(fd, entry), {
        kind: "doc",
        codec: "json-v1",
        logicalLength: entry.logicalLength
      });
      writePackedDocEntry(entryOutFd, index, packed);
    }
  } finally {
    closeSync(fd);
    closeSync(spoolEntryFd);
    closeSync(entryOutFd);
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const packFiles = packTable(packWriter.packs);
  const entryInFd = openSync(entryPath, "r");
  let pointerTable;
  try {
    pointerTable = buildDocPointerTableFromReader(layout.order.length, packIndexes, ordinal => readPackedDocEntry(entryInFd, layout.order[ordinal], packFiles));
  } finally {
    closeSync(entryInFd);
  }
  const hash = sha256Hex(pointerTable.buffer);
  const file = `docs/pointers/${hashedFile("0000", hash, ".bin")}`;
  mkdirSync(resolve(out, "docs", "pointers"), { recursive: true });
  writeFileSync(resolve(out, file), pointerTable.buffer);
  const ordinalTable = buildDocOrdinalTable(layout.order, total);
  const ordinalHash = sha256Hex(ordinalTable.buffer);
  const ordinalFile = `docs/ordinals/${hashedFile("0000", ordinalHash, ".bin")}`;
  mkdirSync(resolve(out, "docs", "ordinals"), { recursive: true });
  writeFileSync(resolve(out, ordinalFile), ordinalTable.buffer);
  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    layout: {
      ...layout.summary,
      spool_bytes: spool.bytes,
      raw_spool_bytes: spool.rawBytes
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
  const fields = docPayloadFieldNames(config);
  const packWriter = createAppendOnlyPackWriter(resolve(out, "docs", "page-packs"), config.docPagePackBytes || config.docPackBytes);
  const entries = [];
  const fd = openSync(spool.rawPath, "r");
  const spoolEntryFd = openSync(spool.rawEntryPath, "r");
  try {
    for (let pageStart = 0, pageIndex = 0; pageStart < total; pageStart += pageSize, pageIndex++) {
      const pageEnd = Math.min(total, pageStart + pageSize);
      const docs = [];
      for (let index = pageStart; index < pageEnd; index++) {
        const entry = readDocSpoolEntry(spoolEntryFd, index);
        docs.push(JSON.parse(readSpooledDoc(fd, entry).toString("utf8")));
      }
      const source = encodeDocPageColumns(docs, fields);
      const packed = writePackedShard(packWriter, docPageKey(pageIndex), gzipSync(source, { level: 6 }), {
        kind: "doc-page",
        codec: DOC_PAGE_FORMAT,
        logicalLength: source.length
      });
      entries[pageIndex] = packed;
    }
  } finally {
    closeSync(fd);
    closeSync(spoolEntryFd);
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const pointerTable = buildDocPagePointerTable(entries.map(entry => resolvePackEntry(packWriter, entry)), packIndexes);
  const hash = sha256Hex(pointerTable.buffer);
  const file = `docs/pages/${hashedFile("0000", hash, ".bin")}`;
  mkdirSync(resolve(out, "docs", "pages"), { recursive: true });
  writeFileSync(resolve(out, file), pointerTable.buffer);
  return {
    storage: "range-pack-v1",
    format: DOC_PAGE_FORMAT,
    encoding: DOC_PAGE_ENCODING,
    compression: "gzip-member",
    fields,
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
  const lookupChunkSize = Math.max(1, Number(config.docValueLookupChunkSize || Math.min(256, chunkSize)));
  const packWriter = createPackWriter(resolve(out, "doc-values", "packs"), config.docValuePackBytes);
  const fields = {};
  const writeChunks = (field, activeChunkSize, keyPrefix = "") => {
    const chunks = [];
    for (let start = 0; start < total; start += activeChunkSize) {
      const rows = codeRows(codes, field.name, start, Math.min(total, start + activeChunkSize));
      const encoded = buildDocValueChunk(field, start, rows);
      const key = `${field.name}\u0000${keyPrefix}${chunks.length}`;
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
    return chunks;
  };
  for (const field of codes.fields || docValueFields(config, codes)) {
    const chunks = writeChunks(field, chunkSize);
    const lookupChunks = lookupChunkSize < chunkSize ? writeChunks(field, lookupChunkSize, "lookup\u0000") : null;
    fields[field.name] = {
      name: field.name,
      kind: field.kind,
      type: field.type,
      words: field.words || 0,
      chunks,
      ...(lookupChunks ? { lookup_chunks: lookupChunks } : {})
    };
  }
  finalizePackWriter(packWriter);
  const indexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const hydrateChunkEntries = (chunks) => {
    for (const chunk of chunks || []) {
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
  };
  for (const field of Object.values(fields)) {
    hydrateChunkEntries(field.chunks);
    hydrateChunkEntries(field.lookup_chunks);
  }
  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    format: "rfdocvalues-v1",
    chunk_size: chunkSize,
    lookup_chunk_size: lookupChunkSize < chunkSize ? lookupChunkSize : chunkSize,
    fields,
    packs: packWriter.packs
  };
}

function safeObjectName(value) {
  return String(value || "field").replace(/[^A-Za-z0-9_-]+/gu, "_").replace(/^_+|_+$/gu, "") || "field";
}

function sortableDocValue(field, value) {
  if (field.kind === "boolean") {
    if (value === true || value === 1 || value === "true" || value === "1") return 2;
    if (value === false || value === 0 || value === "false" || value === "0") return 1;
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function codeValue(codes, name, doc) {
  if (codes && typeof codes.get === "function") return codes.get(name, doc);
  const values = codes?.[name] || [];
  return values[doc];
}

function codeRows(codes, name, start, end) {
  const count = Math.max(0, end - start);
  if (codes && typeof codes.chunk === "function") return codes.chunk(name, start, count);
  return (codes?.[name] || []).slice(start, end);
}

function facetCodesForBitmap(value) {
  return value?.codes?.map(Number).filter(Number.isFinite) || [];
}

function bitmapBooleanKey(value) {
  if (value === true || value === 1 || value === "true" || value === "1") return "true";
  if (value === false || value === 0 || value === "false" || value === "0") return "false";
  return "";
}

function writeFilterBitmapIndex(out, config, total, codes, dicts) {
  const packWriter = createPackWriter(resolve(out, "filter-bitmaps", "packs"), config.filterBitmapPackBytes || config.packBytes);
  const fields = {};
  const maxFacetValues = Math.max(0, Number(config.filterBitmapMaxFacetValues ?? 64));

  const writeBitmap = (field, value, bytes, count) => {
    if (!count) return null;
    const source = encodeFilterBitmap(total, bytes);
    const key = `${field}\u0000${value}`;
    const entry = writePackedShard(packWriter, key, gzipSync(source, { level: 6 }), {
      kind: "filter-bitmap",
      codec: FILTER_BITMAP_FORMAT,
      logicalLength: source.length
    });
    return {
      key,
      count,
      pack: entry.pack,
      offset: entry.offset,
      length: entry.length,
      physicalLength: entry.physicalLength,
      logicalLength: entry.logicalLength,
      checksum: entry.checksum
    };
  };

  if (config.filterBitmaps !== false) {
    for (const facet of config.facets || []) {
      const valueCount = dicts?.[facet.name]?.values?.length || 0;
      if (!valueCount || valueCount > maxFacetValues) continue;
      const bitmaps = Array.from({ length: valueCount }, () => createFilterBitmap(total));
      const counts = new Array(valueCount).fill(0);
      for (let doc = 0; doc < total; doc++) {
        for (const code of facetCodesForBitmap(codeValue(codes, facet.name, doc))) {
          if (code < 0 || code >= valueCount) continue;
          setFilterBitmapBit(bitmaps[code], doc);
          counts[code]++;
        }
      }
      const values = {};
      for (let code = 0; code < valueCount; code++) {
        const entry = writeBitmap(facet.name, String(code), bitmaps[code], counts[code]);
        if (entry) values[String(code)] = entry;
      }
      if (Object.keys(values).length) fields[facet.name] = { name: facet.name, kind: "facet", values };
    }

    for (const field of config.booleans || []) {
      const bitmaps = { true: createFilterBitmap(total), false: createFilterBitmap(total) };
      const counts = { true: 0, false: 0 };
      for (let doc = 0; doc < total; doc++) {
        const key = bitmapBooleanKey(codeValue(codes, field.name, doc));
        if (!key) continue;
        setFilterBitmapBit(bitmaps[key], doc);
        counts[key]++;
      }
      const values = {};
      for (const key of ["false", "true"]) {
        const entry = writeBitmap(field.name, key, bitmaps[key], counts[key]);
        if (entry) values[key] = entry;
      }
      if (Object.keys(values).length) fields[field.name] = { name: field.name, kind: "boolean", values };
    }
  }

  finalizePackWriter(packWriter);
  const indexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  for (const field of Object.values(fields)) {
    for (const entry of Object.values(field.values || {})) {
      const packed = packWriter.entries[entry.key];
      Object.assign(entry, {
        pack: packed.pack,
        offset: packed.offset,
        length: packed.length,
        physicalLength: packed.physicalLength,
        logicalLength: packed.logicalLength,
        checksum: packed.checksum,
        packIndex: indexes.get(packed.pack)
      });
      delete entry.key;
    }
  }

  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    format: FILTER_BITMAP_FORMAT,
    max_facet_values: maxFacetValues,
    fields,
    packs: packWriter.packs,
    pack_table: packTable(packWriter.packs),
    pack_bytes: packWriter.packs.reduce((sum, pack) => sum + pack.bytes, 0)
  };
}

function summarizeDocValuePage(summaryFields, codes, docs) {
  const summaries = {};
  for (const field of summaryFields) {
    let min = null;
    let max = null;
    for (const doc of docs) {
      const value = sortableDocValue(field, codeValue(codes, field.name, doc));
      if (!Number.isFinite(value)) continue;
      min = min == null ? value : Math.min(min, value);
      max = max == null ? value : Math.max(max, value);
    }
    summaries[field.name] = { min, max };
  }
  return summaries;
}

function nextSortPageEnd(rows, start, pageSize) {
  let end = Math.min(rows.length, start + pageSize);
  const maxEnd = Math.min(rows.length, start + pageSize * 4);
  while (end < maxEnd && rows[end]?.value === rows[end - 1]?.value) end++;
  return end;
}

function writeDocValueSortedIndexes(out, config, total, codes) {
  const pageSize = Math.max(1, Math.floor(Number(config.docValueSortedPageSize || 512)));
  const packWriter = createPackWriter(resolve(out, "doc-values", "sorted-packs"), config.docValueSortedPackBytes || config.docValuePackBytes);
  const fields = {};
  const sourceFields = (codes.fields || docValueFields(config, codes)).filter(field => field.kind !== "facet");
  const summaryFields = sourceFields.map(field => ({ name: field.name, kind: field.kind, type: field.type }));
  const pagesByField = new Map();

  for (const field of sourceFields) {
    const rows = [];
    const readChunkSize = Math.max(1, Math.floor(Number(config.docValueChunkSize || 2048)));
    for (let start = 0; start < total; start += readChunkSize) {
      const values = codeRows(codes, field.name, start, Math.min(total, start + readChunkSize));
      for (let row = 0; row < values.length; row++) {
        const value = sortableDocValue(field, values[row]);
        if (Number.isFinite(value)) rows.push({ doc: start + row, value });
      }
    }
    rows.sort((a, b) => a.value - b.value || a.doc - b.doc);
    const pages = [];
    for (let start = 0, pageIndex = 0; start < rows.length; pageIndex++) {
      const end = nextSortPageEnd(rows, start, pageSize);
      const pageRows = rows.slice(start, end);
      const encoded = encodeDocValueSortPage(field, start, pageRows);
      const entry = writePackedShard(packWriter, `${field.name}\u0000${pageIndex}`, gzipSync(encoded.buffer, { level: 6 }), {
        kind: "doc-value-sort-page",
        codec: DOC_VALUE_SORT_PAGE_FORMAT,
        logicalLength: encoded.buffer.length
      });
      pages.push({
        ...encoded.meta,
        entry,
        summaries: summarizeDocValuePage(summaryFields, codes, pageRows.map(row => row.doc))
      });
      start = end;
    }
    pagesByField.set(field.name, { field, pages, total: rows.length });
  }

  finalizePackWriter(packWriter);
  const packFiles = packTable(packWriter.packs);
  const packIndexes = new Map(packFiles.map((file, index) => [file, index]));
  let directoryBytes = 0;
  let directoryLogicalBytes = 0;
  mkdirSync(resolve(out, "doc-values", "sorted"), { recursive: true });

  for (const { field, pages, total: fieldTotal } of pagesByField.values()) {
    const directory = encodeDocValueSortDirectory({
      field,
      pageSize,
      total: fieldTotal,
      pages,
      summaryFields,
      packTable: packFiles,
      packIndexes
    });
    const compressed = gzipSync(directory.buffer, { level: 6 });
    const hash = sha256Hex(compressed);
    const file = `doc-values/sorted/${hashedFile(safeObjectName(field.name), hash, ".bin.gz")}`;
    writeFileSync(resolve(out, file), compressed);
    directoryBytes += compressed.length;
    directoryLogicalBytes += directory.buffer.length;
    fields[field.name] = {
      name: field.name,
      kind: field.kind,
      type: field.type,
      total: fieldTotal,
      page_size: pageSize,
      pages: pages.length,
      directory: {
        format: DOC_VALUE_SORT_DIRECTORY_FORMAT,
        compression: "gzip-member",
        file,
        content_hash: hash,
        immutable: true,
        bytes: compressed.length,
        logical_bytes: directory.buffer.length
      },
      summary_fields: summaryFields.map(item => ({ name: item.name, kind: item.kind, type: item.type }))
    };
  }

  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    directory_format: DOC_VALUE_SORT_DIRECTORY_FORMAT,
    page_format: DOC_VALUE_SORT_PAGE_FORMAT,
    page_size: pageSize,
    fields,
    packs: packWriter.packs,
    pack_table: packFiles,
    directory_bytes: directoryBytes,
    directory_logical_bytes: directoryLogicalBytes,
    pack_bytes: packWriter.packs.reduce((sum, pack) => sum + pack.bytes, 0)
  };
}

function packTable(packs) {
  return (packs || []).map(pack => pack.file);
}

function sortReplicaKey(field, order) {
  return `${field}:${order === "desc" ? "desc" : "asc"}`;
}

function sortReplicaId(field, order) {
  return safeObjectName(`${field}_${order === "desc" ? "desc" : "asc"}`);
}

function sortReplicaFieldMap(config) {
  const fields = new Map();
  for (const field of config.numbers || []) {
    fields.set(field.name, { ...field, kind: "number", type: normalizedNumberType(field) });
  }
  for (const field of config.booleans || []) {
    fields.set(field.name, { ...field, kind: "boolean", type: "boolean" });
  }
  return fields;
}

function sortReplicaDefinitions(config) {
  const fields = sortReplicaFieldMap(config);
  const definitions = [];
  const seen = new Set();
  for (const item of config.sortReplicas || []) {
    const rawField = typeof item === "string" ? item.replace(/^-/, "") : item.field || item.name;
    const fieldName = String(rawField || "");
    if (!fieldName) continue;
    const source = fields.get(fieldName);
    if (!source) {
      throw new Error(`Rangefind sort replica field "${fieldName}" must be configured as a number or boolean field.`);
    }
    const requestedOrder = typeof item === "string" && item.startsWith("-")
      ? "desc"
      : String(item.order || item.direction || "asc").toLowerCase();
    const order = requestedOrder === "desc" ? "desc" : "asc";
    const key = sortReplicaKey(fieldName, order);
    if (seen.has(key)) continue;
    seen.add(key);
    definitions.push({
      key,
      id: sortReplicaId(fieldName, order),
      field: { name: source.name, kind: source.kind, type: source.type },
      order
    });
  }
  return definitions;
}

function sortReplicaRankChunkSize(config) {
  return Math.max(1, Math.floor(Number(config.sortReplicaRankChunkSize || 4096)));
}

function sortRowsForReplica(config, total, codes, field, order) {
  const rows = [];
  const readChunkSize = Math.max(1, Math.floor(Number(config.docValueChunkSize || 2048)));
  for (let start = 0; start < total; start += readChunkSize) {
    const values = codeRows(codes, field.name, start, Math.min(total, start + readChunkSize));
    for (let row = 0; row < values.length; row++) {
      const value = sortableDocValue(field, values[row]);
      if (Number.isFinite(value)) rows.push({ doc: start + row, value });
    }
  }
  rows.sort((a, b) => (
    order === "desc"
      ? b.value - a.value || a.doc - b.doc
      : a.value - b.value || a.doc - b.doc
  ));
  return rows;
}

function encodeSortReplicaRankChunk(rows, start, end) {
  const buffer = Buffer.allocUnsafe((end - start) * SORT_REPLICA_RANK_RECORD_BYTES);
  let offset = 0;
  for (let index = start; index < end; index++) {
    buffer.writeUInt32LE(rows[index].doc, offset);
    offset += 4;
    buffer.writeDoubleLE(rows[index].value, offset);
    offset += 8;
  }
  return buffer;
}

function writeSortReplicaRankMap(out, config, replica, rows) {
  const chunkSize = sortReplicaRankChunkSize(config);
  const basePath = `sort-replicas/${replica.id}/rank-packs`;
  const packWriter = createPackWriter(resolve(out, basePath), config.sortReplicaPackBytes || config.packBytes);
  const chunks = [];
  for (let start = 0; start < rows.length; start += chunkSize) {
    const end = Math.min(rows.length, start + chunkSize);
    const encoded = encodeSortReplicaRankChunk(rows, start, end);
    const entry = writePackedShard(packWriter, `${replica.id}\u0000${start}`, gzipSync(encoded, { level: 6 }), {
      kind: "sort-replica-rank-map",
      codec: SORT_REPLICA_RANK_MAP_FORMAT,
      logicalLength: encoded.length
    });
    chunks.push({
      start,
      count: end - start,
      ...entry
    });
  }
  finalizePackWriter(packWriter);
  const packFiles = packTable(packWriter.packs);
  const packIndexes = new Map(packFiles.map((file, index) => [file, index]));
  for (const chunk of chunks) {
    chunk.pack = packWriter.packNameMap?.get(chunk.pack) || chunk.pack;
    chunk.packIndex = packIndexes.get(chunk.pack);
  }
  return {
    format: SORT_REPLICA_RANK_MAP_FORMAT,
    compression: "gzip-member",
    record_bytes: SORT_REPLICA_RANK_RECORD_BYTES,
    chunk_size: chunkSize,
    total: rows.length,
    chunks,
    packs_path: basePath,
    packs: packWriter.packs.length,
    pack_table: packFiles,
    pack_bytes: packWriter.packs.reduce((sum, pack) => sum + pack.bytes, 0)
  };
}

function sortReplicaDocPageSize(config) {
  return Math.max(1, Math.floor(Number(config.sortReplicaDocPageSize || config.docPageSize || 32)));
}

function writeSortReplicaDocPages(out, config, replica, rows, spool) {
  const pageSize = sortReplicaDocPageSize(config);
  const fields = [...new Set([...docPayloadFieldNames(config), "index"])];
  const packBase = `sort-replicas/${replica.id}/docs/page-packs`;
  const pointerBase = `sort-replicas/${replica.id}/docs/pages`;
  const packWriter = createAppendOnlyPackWriter(resolve(out, packBase), config.sortReplicaDocPagePackBytes || config.docPagePackBytes || config.docPackBytes);
  const entries = [];
  const fd = openSync(spool.rawPath, "r");
  const spoolEntryFd = openSync(spool.rawEntryPath, "r");
  try {
    for (let pageStart = 0, pageIndex = 0; pageStart < rows.length; pageStart += pageSize, pageIndex++) {
      const pageEnd = Math.min(rows.length, pageStart + pageSize);
      const docs = [];
      for (let rank = pageStart; rank < pageEnd; rank++) {
        const doc = rows[rank].doc;
        const entry = readDocSpoolEntry(spoolEntryFd, doc);
        docs.push(JSON.parse(readSpooledDoc(fd, entry).toString("utf8")));
      }
      const source = encodeDocPageColumns(docs, fields);
      const packed = writePackedShard(packWriter, docPageKey(pageIndex), gzipSync(source, { level: 6 }), {
        kind: "sort-replica-doc-page",
        codec: DOC_PAGE_FORMAT,
        logicalLength: source.length
      });
      entries[pageIndex] = packed;
    }
  } finally {
    closeSync(fd);
    closeSync(spoolEntryFd);
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const pointerTable = buildDocPagePointerTable(entries.map(entry => resolvePackEntry(packWriter, entry)), packIndexes);
  const hash = sha256Hex(pointerTable.buffer);
  const file = `${pointerBase}/${hashedFile("0000", hash, ".bin")}`;
  mkdirSync(resolve(out, pointerBase), { recursive: true });
  writeFileSync(resolve(out, file), pointerTable.buffer);
  return {
    storage: "range-pack-v1",
    format: DOC_PAGE_FORMAT,
    encoding: DOC_PAGE_ENCODING,
    compression: "gzip-member",
    role: "display",
    order: "sort-rank-page",
    fields,
    page_size: pageSize,
    total: rows.length,
    pointers: {
      ...pointerTable.meta,
      file,
      order: "sort-rank-page",
      content_hash: hash,
      immutable: true,
      bytes: pointerTable.buffer.length,
      pack_table: packTable(packWriter.packs)
    },
    packs_path: packBase,
    packs: packWriter.packs.length,
    pack_table: packTable(packWriter.packs),
    pack_bytes: packWriter.bytes
  };
}

function sortReplicaBuildConfig(config) {
  return {
    ...config,
    postingOrder: "doc-id",
    postingImpactBucketOrderMinRows: Number.MAX_SAFE_INTEGER
  };
}

async function buildSortReplicaSegments(config, dirs, selectedTermSpool, replica, docToRank) {
  const replicaConfig = sortReplicaBuildConfig(config);
  const builder = createSegmentBuilder(resolve(dirs.build, "sort-replicas", replica.id, "segments"), replicaConfig);
  let docs = 0;
  let skippedDocs = 0;
  let postings = 0;
  for await (const { doc, selectedTerms } of readSelectedTermSpool(selectedTermSpool.path)) {
    const rank = docToRank[doc];
    if (rank < 0) {
      skippedDocs++;
      continue;
    }
    docs++;
    for (const [term, score] of selectedTerms) {
      addSegmentPosting(builder, term, rank, score);
      postings++;
    }
    if (shouldFlushSegment(builder)) flushSegment(builder);
  }
  const segments = finishSegmentBuilder(builder);
  return {
    segments,
    summary: segmentMergeSummary(segments),
    docs,
    skippedDocs,
    postings
  };
}

async function reduceSortReplicaSegments(config, measured, dirs, replica, segmentData, totalRanks) {
  const replicaConfig = sortReplicaBuildConfig(config);
  const termsBase = resolve(dirs.out, "sort-replicas", replica.id, "terms");
  const scratchDir = resolve(dirs.build, "sort-replicas", replica.id, "segment-merge");
  const packWriter = createAppendOnlyPackWriter(resolve(termsBase, "packs"), config.sortReplicaPackBytes || config.packBytes);
  const blockPackWriter = config.externalPostingBlocks === false
    ? null
    : createAppendOnlyPackWriter(resolve(termsBase, "block-packs"), config.sortReplicaPostingBlockPackBytes || config.postingBlockPackBytes);
  const directorySpool = createDirectoryEntrySpool(resolve(dirs.build, "sort-replicas", replica.id, "terms-directory.run"));
  const finalShards = new Set();
  const blockStats = emptyPostingSegmentStats();
  let stats = { terms: 0, postings: 0, mergeTiers: [], mergePolicy: null, timings: {}, partitionSpoolBytes: 0, partitionSpoolEntries: 0 };

  if (segmentData.segments.length) {
    stats = await mergeSegmentsToPartitions({
      segments: segmentData.segments,
      scratchDir,
      config: replicaConfig,
      partitionConcurrency: 1,
      onPartition: async (partition) => {
        const encoded = buildFinalPostingSegmentChunks(partitionTermEntries(partition), totalRanks, null, [], replicaConfig, blockPackWriter);
        addPostingSegmentStats(blockStats, encoded.stats);
        const entry = await writePackedShardChunks(packWriter, partition.name, encoded.chunks, {
          kind: "posting-segment",
          codec: encoded.format || POSTING_SEGMENT_FORMAT,
          logicalLength: encoded.logicalLength,
          streamMinBytes: replicaConfig.postingSegmentStreamMinBytes
        });
        appendDirectoryEntry(directorySpool, partition.name, entry);
        finalShards.add(partition.name);
        return partition.name;
      }
    });
  }

  if (blockPackWriter) finalizePackWriter(blockPackWriter);
  finalizePackWriter(packWriter);
  const termPacks = packWriter.packs;
  const blockPacks = blockPackWriter?.packs || [];
  const packIndexes = new Map(termPacks.map((pack, index) => [pack.file, index]));
  const directoryEntries = directorySpool.entries
    ? sortedDirectoryEntrySpool(directorySpool, {
        packNameMap: packWriter.packNameMap,
        packIndexes,
        chunkEntries: config.directorySortChunkEntries
      })
    : (async function* emptyDirectoryEntries() {})();
  const directory = await writeDirectoryFilesFromSortedEntries(
    termsBase,
    directoryEntries,
    directorySpool.entries,
    config.directoryPageBytes,
    `sort-replicas/${replica.id}/terms`,
    { packTable: termPacks }
  );

  return {
    directory,
    shards: [...finalShards].sort(),
    packs: termPacks,
    blockPacks,
    blockStats,
    termCount: stats.terms || 0,
    postingCount: stats.postings || 0,
    packBytes: packWriter.bytes,
    blockPackBytes: blockPackWriter?.bytes || 0,
    directoryBytes: directory.total_bytes,
    directorySpoolBytes: directorySpool.bytes,
    directorySpoolEntries: directorySpool.entries,
    segmentSummary: segmentData.summary,
    mergeTiers: stats.mergeTiers || [],
    mergePolicy: stats.mergePolicy || null,
    reduceTimings: {
      segmentTierMergeMs: stats.timings?.tierMergeMs || 0,
      segmentPrefixCountMs: stats.timings?.prefixCountMs || 0,
      segmentPartitionAssemblyMs: stats.timings?.partitionAssemblyMs || 0
    },
    partitionSpoolBytes: stats.partitionSpoolBytes || 0,
    partitionSpoolEntries: stats.partitionSpoolEntries || 0
  };
}

async function buildSortReplicas(config, measured, dirs, selectedTermSpool, docSpool, codes) {
  const definitions = sortReplicaDefinitions(config);
  const replicas = {};
  const aggregate = {
    docs: 0,
    terms: 0,
    postings: 0,
    segmentFiles: 0,
    termPackFiles: 0,
    termPackBytes: 0,
    blockPackFiles: 0,
    blockPackBytes: 0,
    rankPackFiles: 0,
    rankPackBytes: 0,
    docPackFiles: 0,
    docPackBytes: 0,
    docPointerBytes: 0,
    docPagePackFiles: 0,
    docPagePackBytes: 0,
    docPagePointerBytes: 0,
    directoryBytes: 0
  };

  for (const definition of definitions) {
    const rows = sortRowsForReplica(config, measured.total, codes, definition.field, definition.order);
    const docToRank = new Int32Array(measured.total);
    docToRank.fill(-1);
    for (let rank = 0; rank < rows.length; rank++) docToRank[rows[rank].doc] = rank;
    const rankMap = writeSortReplicaRankMap(dirs.out, config, definition, rows);
    const docPages = writeSortReplicaDocPages(dirs.out, config, definition, rows, docSpool);
    const segmentData = await buildSortReplicaSegments(config, dirs, selectedTermSpool, definition, docToRank);
    const reduced = await reduceSortReplicaSegments(config, measured, dirs, definition, segmentData, rows.length);

    aggregate.docs += rows.length;
    aggregate.terms += reduced.termCount;
    aggregate.postings += reduced.postingCount;
    aggregate.segmentFiles += segmentData.segments.length;
    aggregate.termPackFiles += reduced.packs.length;
    aggregate.termPackBytes += reduced.packBytes;
    aggregate.blockPackFiles += reduced.blockPacks.length;
    aggregate.blockPackBytes += reduced.blockPackBytes;
    aggregate.rankPackFiles += rankMap.packs;
    aggregate.rankPackBytes += rankMap.pack_bytes;
    aggregate.docPagePackFiles += docPages.packs;
    aggregate.docPagePackBytes += docPages.pack_bytes;
    aggregate.docPagePointerBytes += docPages.pointers.bytes;
    aggregate.directoryBytes += reduced.directoryBytes;

    replicas[definition.key] = {
      format: "rfsortreplica-v1",
      key: definition.key,
      id: definition.id,
      field: definition.field.name,
      field_kind: definition.field.kind,
      field_type: definition.field.type,
      order: definition.order,
      total: rows.length,
      posting_order: "sort-rank",
      base_shard_depth: config.baseShardDepth,
      max_shard_depth: config.maxShardDepth,
      terms: {
        directory: reduced.directory,
        packs_path: `sort-replicas/${definition.id}/terms/packs`,
        block_packs_path: `sort-replicas/${definition.id}/terms/block-packs`,
        packs: reduced.packs.length,
        pack_table: packTable(reduced.packs),
        block_packs: reduced.blockPacks.length,
        block_pack_table: packTable(reduced.blockPacks)
      },
      rank_map: rankMap,
      doc_pages: docPages,
      stats: {
        docs: rows.length,
        skipped_docs: segmentData.skippedDocs,
        terms: reduced.termCount,
        postings: reduced.postingCount,
        segment_files: segmentData.segments.length,
        term_pack_files: reduced.packs.length,
        term_pack_bytes: reduced.packBytes,
        block_pack_files: reduced.blockPacks.length,
        block_pack_bytes: reduced.blockPackBytes,
        rank_pack_files: rankMap.packs,
        rank_pack_bytes: rankMap.pack_bytes,
        doc_pack_files: 0,
        doc_pack_bytes: 0,
        doc_pointer_bytes: 0,
        doc_page_pack_files: docPages.packs,
        doc_page_pack_bytes: docPages.pack_bytes,
        doc_page_pointer_bytes: docPages.pointers.bytes,
        directory_bytes: reduced.directoryBytes,
        external_blocks: reduced.blockStats.externalBlocks,
        external_terms: reduced.blockStats.externalTerms,
        external_postings: reduced.blockStats.externalPostings,
        partition_spool_bytes: reduced.partitionSpoolBytes,
        partition_spool_entries: reduced.partitionSpoolEntries
      }
    };
  }

  return {
    format: SORT_REPLICA_FORMAT,
    compression: "gzip-member",
    count: Object.keys(replicas).length,
    replicas,
    stats: aggregate
  };
}

function packFileIndex(file) {
  const match = /^(\d+)/u.exec(String(file || ""));
  return match ? Number(match[1]) : Number.MAX_SAFE_INTEGER;
}

function comparePackFiles(left, right) {
  const leftIndex = packFileIndex(left.file);
  const rightIndex = packFileIndex(right.file);
  return leftIndex - rightIndex || String(left.file).localeCompare(String(right.file));
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

function emptyPostingSegmentStats() {
  return {
    externalBlocks: 0,
    externalTerms: 0,
    externalPostings: 0,
    externalPostingBytes: 0,
    inlinePostingBytes: 0,
    superblocks: 0,
    superblockTerms: 0,
    superblockBlocks: 0,
    pairVarintBlocks: 0,
    impactRunBlocks: 0,
    impactBitsetBlocks: 0,
    partitionedDeltaBlocks: 0,
    blockCodecBaselineBytes: 0,
    blockCodecSelectedBytes: 0,
    blockCodecImpactRunCandidateBytes: 0,
    blockCodecImpactBitsetCandidateBytes: 0,
    blockCodecPartitionedDeltaCandidateBytes: 0,
    codecPlannerSampledTerms: 0,
    codecPlannerSampledBlocks: 0,
    codecPlannerSkipImpactCandidates: 0,
    codecPlannerSkipBitsetCandidates: 0,
    codecPlannerSkipPartitionedDeltaCandidates: 0,
    impactBucketOrderTerms: 0,
    impactBucketOrderPostings: 0,
    impactTierTerms: 0,
    impactTierBlocks: 0,
    impactTierTiers: 0,
    docRangeTerms: 0,
    docRangeEntries: 0,
    docRangeBlocks: 0,
    docRangeBlockEntries: 0
  };
}

function addPostingSegmentStats(target, source) {
  for (const key of Object.keys(target)) target[key] += source?.[key] || 0;
}

function buildFinalPostingSegmentChunks(entries, total, codes, filters, config, blockPackWriter) {
  const writeBlock = !blockPackWriter || config.externalPostingBlocks === false ? null : ({ term, blockIndex, bytes }) => {
    const key = `${term}\u0000${blockIndex}\u0000${blockPackWriter.bytes}`;
    return writePackedShard(blockPackWriter, key, gzipSync(bytes, { level: 6 }), {
      kind: "posting-segment-block",
      codec: "rfsegpost-block-v1",
      logicalLength: bytes.length
    });
  };
  return buildPostingSegmentChunks(entries, total, codes, filters, config, writeBlock);
}

function scanWorkerCount(config) {
  return Math.max(1, Math.floor(Number(config.scanWorkers || 1)));
}

function partitionReducerWorkerCount(config) {
  const explicit = Math.max(0, Math.floor(Number(config.partitionReducerWorkers || 0)));
  const fallback = Math.max(1, Math.floor(Number(config.builderWorkerCount || 1)));
  return Math.max(1, explicit || fallback);
}

function codeStoreDescriptorForPartitionWorkers(codes, config) {
  const descriptor = codes.descriptor();
  const explicit = Math.max(0, Math.floor(Number(config.codeStoreWorkerCacheChunks || 0)));
  const cacheDocs = Math.max(1, Math.floor(Number(descriptor.cacheDocs || config.codeStoreCacheDocs || 1)));
  const totalChunks = Math.max(1, Math.ceil(Math.max(0, Number(descriptor.total || 0)) / cacheDocs));
  const maxAuto = Math.max(1, Math.floor(Number(config.codeStoreWorkerMaxAutoCacheChunks || 64)));
  const cacheChunks = explicit || Math.min(maxAuto, totalChunks);
  return {
    ...descriptor,
    cacheChunks
  };
}

function scanBatchDocs(config) {
  return Math.max(1, Math.floor(Number(config.scanBatchDocs || 128)));
}

function positiveIntegerOption(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? Math.floor(number) : fallback;
}

function choosePostingBlockSize(totalDocs, totalPostings, termCount) {
  const avgPostings = totalPostings / Math.max(1, termCount);
  if (totalDocs >= 500000 || avgPostings >= 4096) return 256;
  if (totalDocs >= 100000 || avgPostings >= 1024) return 128;
  if (avgPostings <= 16) return 32;
  return 64;
}

function choosePostingSuperblockSize(postingBlockSize) {
  if (postingBlockSize <= 32) return 32;
  if (postingBlockSize >= 256) return 8;
  return 16;
}

function applyAutoPostingLayout(config, measured, runData) {
  const codecMode = String(config.codecs?.mode || "auto").toLowerCase();
  const postingBlockAuto = config.postingBlockSize === "auto";
  const postingSuperblockAuto = config.postingSuperblockSize === "auto";
  const selectedPostingBlockSize = postingBlockAuto
    ? choosePostingBlockSize(measured.total, runData.segmentSummary.postings, runData.selectedTermSpool.terms)
    : positiveIntegerOption(config.postingBlockSize, 128);
  const selectedPostingSuperblockSize = postingSuperblockAuto
    ? choosePostingSuperblockSize(selectedPostingBlockSize)
    : positiveIntegerOption(config.postingSuperblockSize, 16);
  config.postingBlockSize = selectedPostingBlockSize;
  config.postingSuperblockSize = selectedPostingSuperblockSize;
  config._layoutDecisions = {
    codecs: {
      mode: codecMode,
      selected_posting_codec: codecMode === "auto" ? "term-sampled-auto-block-codec" : "pair-varint-v1",
      candidate_codecs: ["pair-varint-v1", "impact-runs-v1", "impact-bitset-v1", "partitioned-deltas-v1"]
    },
    posting_block_size: {
      source: postingBlockAuto ? "auto" : "configured",
      value: selectedPostingBlockSize
    },
    posting_superblock_size: {
      source: postingSuperblockAuto ? "auto" : "configured",
      value: selectedPostingSuperblockSize
    },
    corpus: {
      docs: measured.total,
      postings: runData.segmentSummary.postings,
      terms: runData.selectedTermSpool.terms,
      avg_postings_per_term: runData.segmentSummary.postings / Math.max(1, runData.selectedTermSpool.terms)
    }
  };
}

function analyzeDocForScan(doc, index, config, avgLens) {
  return {
    index,
    selectedTerms: analyzeDocumentTerms(doc, config, avgLens),
    typoSurfacePairs: [...surfacePairsForFields(doc, config.fields, (source, field) => fieldIndexText(source, field, config))]
  };
}

function consumeScanDoc(state, doc, index, analysis) {
  const {
    config,
    measured,
    codes,
    initialResults,
    segmentBuilder,
    queryBundleSeedBuffer,
    authorityBuffer,
    docSpool,
    selectedTermSpool,
    typoBuffer,
    typoSurfacePairs
  } = state;
  const selectedTerms = analysis.selectedTerms || [];
  writeSelectedTerms(selectedTermSpool, selectedTerms);
  for (const [term, score] of selectedTerms) {
    addSegmentPosting(segmentBuilder, term, index, Math.max(1, Math.round(score * 1000)));
  }
  addQueryBundleSeeds(queryBundleSeedBuffer, selectedTerms, config, doc);
  addAuthorityDoc(authorityBuffer, config, doc, index);
  addTypoSurfacePairsToBuffer(typoSurfacePairs, analysis.typoSurfacePairs || []);

  if (shouldFlushSegment(segmentBuilder)) {
    flushTypoSurfacePairBuffer(typoBuffer, typoSurfacePairs);
    flushSegment(segmentBuilder);
  }

  for (const facet of config.facets) {
    const values = [];
    for (const item of facetValues(doc, facet)) {
      const code = addDict(measured.dicts[facet.name], item.value, item.label);
      values.push(code);
    }
    codes.set(facet.name, index, { codes: values });
  }
  for (const number of config.numbers) codes.set(number.name, index, numericValue(doc, number));
  for (const boolean of config.booleans || []) codes.set(boolean.name, index, booleanValue(doc, boolean));

  const payload = docPayload(doc, config, index);
  if (initialResults.length < config.initialResultLimit) initialResults.push(payload);
  writeSpooledDoc(docSpool, payload, index, docLayoutRecord(index, selectedTerms, config));
}

async function scanSequential(state) {
  const started = performance.now();
  let docs = 0;
  await eachJsonLine(state.config.input, async (doc, index) => {
    const analysis = analyzeDocForScan(doc, index, state.config, state.measured.avgLens);
    consumeScanDoc(state, doc, index, analysis);
    docs++;
  });
  return [{
    worker: 0,
    docs,
    batches: docs ? 1 : 0,
    analysisMs: performance.now() - started,
    mode: "main-thread"
  }];
}

function postScanBatch(worker, message) {
  return new Promise((resolveBatch, rejectBatch) => {
    function cleanup() {
      worker.off("message", onMessage);
      worker.off("error", onError);
      worker.off("exit", onExit);
    }
    function onMessage(response) {
      if (response.id !== message.id) return;
      cleanup();
      if (response.error) rejectBatch(new Error(response.error));
      else resolveBatch(response);
    }
    function onError(error) {
      cleanup();
      rejectBatch(error);
    }
    function onExit(code) {
      cleanup();
      if (code !== 0) rejectBatch(new Error(`Rangefind scan worker exited with code ${code}.`));
    }
    worker.on("message", onMessage);
    worker.once("error", onError);
    worker.once("exit", onExit);
    worker.postMessage(message);
  });
}

async function scanWithWorkers(state) {
  const workerCount = scanWorkerCount(state.config);
  const batchDocs = scanBatchDocs(state.config);
  const workers = Array.from({ length: workerCount }, (_, index) => ({
    index,
    worker: new Worker(new URL("./analyze_worker.js", import.meta.url), { type: "module" }),
    docs: 0,
    batches: 0,
    analysisMs: 0
  }));
  const available = workers.slice();
  const active = new Set();
  const pending = new Map();
  let nextBatch = 0;
  let nextWrite = 0;

  function drainPending() {
    while (pending.has(nextWrite)) {
      const { batch, response } = pending.get(nextWrite);
      pending.delete(nextWrite);
      for (let i = 0; i < batch.length; i++) {
        consumeScanDoc(state, batch[i].doc, batch[i].index, response.docs[i]);
      }
      nextWrite++;
    }
  }

  async function waitForWorker() {
    while (!available.length) await Promise.race(active);
    return available.pop();
  }

  async function queueBatch(batch) {
    const entry = await waitForWorker();
    const id = nextBatch++;
    const started = performance.now();
    const promise = postScanBatch(entry.worker, {
      id,
      docs: batch,
      config: state.config,
      avgLens: state.measured.avgLens
    }).then((response) => {
      entry.docs += batch.length;
      entry.batches++;
      entry.analysisMs += performance.now() - started;
      pending.set(id, { batch, response });
      drainPending();
    }).finally(() => {
      active.delete(promise);
      available.push(entry);
    });
    active.add(promise);
  }

  try {
    const rl = createInterface({ input: createReadStream(state.config.input), crlfDelay: Infinity });
    let index = 0;
    let batch = [];
    for await (const line of rl) {
      if (!line.trim()) continue;
      batch.push({ doc: JSON.parse(line), index: index++ });
      if (batch.length >= batchDocs) {
        await queueBatch(batch);
        batch = [];
      }
    }
    if (batch.length) await queueBatch(batch);
    while (active.size) await Promise.race(active);
    drainPending();
    if (pending.size) throw new Error("Rangefind scan workers finished out of order.");
  } finally {
    await Promise.allSettled(workers.map(entry => entry.worker.terminate()));
  }

  return workers.map(entry => ({
    worker: entry.index,
    docs: entry.docs,
    batches: entry.batches,
    analysisMs: entry.analysisMs,
    mode: "worker-thread"
  }));
}

function postReducePartition(worker, message) {
  return new Promise((resolvePartition, rejectPartition) => {
    function cleanup() {
      worker.off("message", onMessage);
      worker.off("error", onError);
      worker.off("exit", onExit);
    }
    function onMessage(response) {
      if (response.id !== message.id) return;
      cleanup();
      if (response.error) rejectPartition(new Error(response.error));
      else resolvePartition(response);
    }
    function onError(error) {
      cleanup();
      rejectPartition(error);
    }
    function onExit(code) {
      cleanup();
      if (code !== 0) rejectPartition(new Error(`Rangefind reduce partition worker exited with code ${code}.`));
    }
    worker.on("message", onMessage);
    worker.once("error", onError);
    worker.once("exit", onExit);
    worker.postMessage(message);
  });
}

function createPartitionReducerPool(config) {
  const count = partitionReducerWorkerCount(config);
  const termPackCounterBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
  const blockPackCounterBuffer = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
  const explicitCredit = Math.max(0, Math.floor(Number(config.partitionReducerInFlightBytes || 0)));
  const memoryBudget = Math.max(0, Math.floor(Number(config.builderMemoryBudgetBytes || 0)));
  const packBudget = Math.max(Number(config.packBytes || 0), Number(config.postingBlockPackBytes || 0), 4 * 1024 * 1024);
  const autoCredit = Math.max(32 * 1024 * 1024, Math.floor(count * packBudget * 4));
  const creditLimitBytes = explicitCredit || (memoryBudget ? Math.max(8 * 1024 * 1024, Math.floor(memoryBudget / 2)) : autoCredit);
  const workers = Array.from({ length: count }, (_, index) => ({
    index,
    worker: new Worker(new URL("./reduce_partition_worker.js", import.meta.url), { type: "module" }),
    tasks: 0,
    inputBytes: 0,
    reduceMs: 0,
    finishMs: 0,
    mode: "worker-thread",
    closed: false
  }));
  const available = workers.slice();
  const active = new Set();
  let nextId = 0;
  let activeInputBytes = 0;
  let maxActiveInputBytes = 0;
  let creditWaitMs = 0;
  let creditWaits = 0;

  function hasCredit(inputBytes) {
    return !Number.isFinite(creditLimitBytes) || activeInputBytes === 0 || activeInputBytes + inputBytes <= creditLimitBytes;
  }

  async function checkoutWorker(inputBytes) {
    const started = performance.now();
    let waited = false;
    while (!available.length || !hasCredit(inputBytes)) {
      waited = true;
      await Promise.race(active);
    }
    if (waited) {
      creditWaits++;
      creditWaitMs += performance.now() - started;
    }
    activeInputBytes += inputBytes;
    maxActiveInputBytes = Math.max(maxActiveInputBytes, activeInputBytes);
    return available.pop();
  }

  return {
    count,
    termPackCounterBuffer,
    blockPackCounterBuffer,
    async reduce(partition, message) {
      const inputBytes = partitionInputBytes(partition);
      const entry = await checkoutWorker(inputBytes);
      const id = nextId++;
      const promise = postReducePartition(entry.worker, { ...message, id, partition })
        .then((result) => {
          entry.tasks++;
          entry.inputBytes += result.inputBytes || inputBytes;
          entry.reduceMs += result.ms || 0;
          return { ...result, worker: entry.index };
        })
        .finally(() => {
          activeInputBytes -= inputBytes;
          active.delete(promise);
          available.push(entry);
        });
      active.add(promise);
      return promise;
    },
    stats() {
      return workers.map(entry => ({
        worker: entry.index,
        tasks: entry.tasks,
        inputBytes: entry.inputBytes,
        reduceMs: entry.reduceMs,
        finishMs: entry.finishMs,
        mode: entry.mode
      }));
    },
    schedulerStats() {
      return {
        creditLimitBytes: Number.isFinite(creditLimitBytes) ? creditLimitBytes : 0,
        maxActiveInputBytes,
        creditWaitMs,
        creditWaits,
        finishMode: "staggered"
      };
    },
    async finish() {
      while (active.size) await Promise.race(active);
      const results = [];
      for (const entry of workers) {
        const result = await postReducePartition(entry.worker, { id: nextId++, kind: "finish" });
        entry.finishMs += result.ms || 0;
        await entry.worker.terminate();
        entry.closed = true;
        results.push(result);
      }
      return {
        packs: results.flatMap(result => result.packs || []),
        packBytes: results.reduce((sum, result) => sum + (result.packBytes || 0), 0),
        blockPacks: results.flatMap(result => result.blockPacks || []),
        blockPackBytes: results.reduce((sum, result) => sum + (result.blockPackBytes || 0), 0)
      };
    },
    async close() {
      await Promise.allSettled(workers.filter(entry => !entry.closed).map(entry => entry.worker.terminate()));
    }
  };
}

async function writePostingRuns(config, measured, dirs, typoBuffer) {
  const codes = createCodeStore(resolve(dirs.build, "codes"), config, measured.total, measured.dicts);

  const initialResults = [];
  const segmentBuilder = createSegmentBuilder(resolve(dirs.build, "segments"), config);
  const queryBundleSeedBuffer = createQueryBundleSeedBuffer(config);
  const authorityBuffer = createAuthorityRunBuffer(config, dirs.authorityRunsOut);
  const docSpool = createDocSpool(resolve(dirs.build, "docs"));
  const selectedTermSpool = createSelectedTermSpool(resolve(dirs.build, "terms"));
  const typoSurfacePairs = createTypoSurfacePairBuffer();
  const state = {
    config,
    measured,
    codes,
    initialResults,
    segmentBuilder,
    queryBundleSeedBuffer,
    authorityBuffer,
    docSpool,
    selectedTermSpool,
    typoBuffer,
    typoSurfacePairs
  };
  let scanWorkerStats = [];

  try {
    scanWorkerStats = scanWorkerCount(config) > 1 ? await scanWithWorkers(state) : await scanSequential(state);
  } finally {
    closeDocSpool(docSpool);
    closeSelectedTermSpool(selectedTermSpool);
  }
  flushTypoSurfacePairBuffer(typoBuffer, typoSurfacePairs);
  const segments = finishSegmentBuilder(segmentBuilder);
  const authorityBaseShards = finishAuthorityRuns(authorityBuffer);
  const queryBundleSeeds = finalizeQueryBundleSeeds(queryBundleSeedBuffer, config);
  return {
    codes,
    initialResults,
    segments,
    segmentSummary: segmentMergeSummary(segments),
    docSpool,
    selectedTermSpool,
    queryBundleSeeds,
    queryBundleTerms: queryBundleTerms(queryBundleSeeds),
    authorityBaseShards,
    scanWorkerStats
  };
}

async function reduceRuns(config, measured, runData, dirs, typoBuffer) {
  const filters = buildBlockFilters(config, measured.dicts);
  const started = performance.now();
  const usePartitionWorkers = partitionReducerWorkerCount(config) > 1;
  const partitionPool = usePartitionWorkers ? createPartitionReducerPool(config) : null;
  const packWriter = usePartitionWorkers ? null : createAppendOnlyPackWriter(resolve(dirs.out, "terms", "packs"), config.packBytes);
  const directorySpool = createDirectoryEntrySpool(resolve(dirs.build, "terms-directory.run"));
  const blockPackWriter = usePartitionWorkers || config.externalPostingBlocks === false
    ? null
    : createAppendOnlyPackWriter(resolve(dirs.out, "terms", "block-packs"), config.postingBlockPackBytes);
  const finalShards = new Set();
  const blockStats = emptyPostingSegmentStats();
  const bundleDfs = new Map();
  const bundleTermSet = new Set(runData.queryBundleTerms || []);
  let partitionOutput = { packs: [], packBytes: 0, blockPacks: [], blockPackBytes: 0 };
  const workerCodesDescriptor = usePartitionWorkers ? codeStoreDescriptorForPartitionWorkers(runData.codes, config) : null;
  let typoIndexTerms = 0;
  let stats;
  try {
    stats = await mergeSegmentsToPartitions({
      segments: runData.segments,
      scratchDir: resolve(dirs.build, "segment-merge"),
      config,
      partitionConcurrency: usePartitionWorkers ? partitionPool.count : 1,
      onTerm: (term, df) => {
        if (bundleTermSet.has(term)) bundleDfs.set(term, df);
        if (addTypoIndexTerm(typoBuffer, term, df, measured.total)) typoIndexTerms++;
      },
      onPartition: async (partition, sequence) => {
        if (usePartitionWorkers) {
          const result = await partitionPool.reduce(partition, {
            config,
            codesDescriptor: workerCodesDescriptor,
            filters,
            termsOutDir: resolve(dirs.out, "terms", "packs"),
            blockOutDir: resolve(dirs.out, "terms", "block-packs"),
            termPackCounter: partitionPool.termPackCounterBuffer,
            blockPackCounter: partitionPool.blockPackCounterBuffer,
            targetBytes: config.packBytes,
            blockTargetBytes: config.postingBlockPackBytes,
            total: measured.total
          });
          addPostingSegmentStats(blockStats, result.stats);
          appendDirectoryEntry(directorySpool, partition.name, result.entry);
          finalShards.add(partition.name);
          return partition.name;
        }
        const encoded = buildFinalPostingSegmentChunks(partitionTermEntries(partition), measured.total, runData.codes, filters, config, blockPackWriter);
        addPostingSegmentStats(blockStats, encoded.stats);
        const entry = await writePackedShardChunks(packWriter, partition.name, encoded.chunks, {
          kind: "posting-segment",
          codec: encoded.format || POSTING_SEGMENT_FORMAT,
          logicalLength: encoded.logicalLength,
          streamMinBytes: config.postingSegmentStreamMinBytes
        });
        appendDirectoryEntry(directorySpool, partition.name, entry);
        finalShards.add(partition.name);
        return partition.name;
      }
    });
    if (usePartitionWorkers) partitionOutput = await partitionPool.finish();
  } finally {
    await partitionPool?.close();
  }
  if (blockPackWriter) finalizePackWriter(blockPackWriter);
  if (packWriter) finalizePackWriter(packWriter);
  const termPacks = usePartitionWorkers ? partitionOutput.packs.sort(comparePackFiles) : packWriter.packs;
  const blockPacks = usePartitionWorkers ? partitionOutput.blockPacks.sort(comparePackFiles) : (blockPackWriter?.packs || []);
  const termPackBytes = usePartitionWorkers ? partitionOutput.packBytes : packWriter.bytes;
  const blockPackBytes = usePartitionWorkers ? partitionOutput.blockPackBytes : (blockPackWriter?.bytes || 0);
  const poolScheduler = usePartitionWorkers ? partitionPool.schedulerStats() : null;
  const mergeScheduler = stats.partitionScheduler || {};
  const partitionScheduler = {
    creditLimitBytes: mergeScheduler.creditLimitBytes || poolScheduler?.creditLimitBytes || 0,
    maxActiveInputBytes: Math.max(mergeScheduler.maxActiveInputBytes || 0, poolScheduler?.maxActiveInputBytes || 0),
    creditWaitMs: (mergeScheduler.creditWaitMs || 0) + (poolScheduler?.creditWaitMs || 0),
    creditWaits: (mergeScheduler.creditWaits || 0) + (poolScheduler?.creditWaits || 0),
    oversizedPartitions: mergeScheduler.oversizedPartitions || 0,
    finishMode: usePartitionWorkers ? (poolScheduler?.finishMode || "staggered") : "main-thread"
  };
  const reduced = {
    finalShards,
    packs: termPacks,
    packBytes: termPackBytes,
    directorySpool,
    blockPacks,
    blockPackBytes,
    blockStats,
    termCount: stats.terms,
    postingCount: stats.postings,
    bundleDfs,
    workerCodeStoreCacheChunks: workerCodesDescriptor?.cacheChunks || 0,
    workerStats: usePartitionWorkers ? partitionPool.stats() : [{
      worker: 0,
      tasks: runData.segments.length,
      inputBytes: runData.segments.reduce((sum, segment) => sum + (segment.termsBytes || 0) + (segment.postingBytes || 0), 0),
      reduceMs: performance.now() - started,
      finishMs: 0,
      mode: "main-thread"
    }],
    reduceTimings: {
      finalPackAssemblyMs: 0,
      partitionScheduler
    }
  };
  const packIndexes = new Map(reduced.packs.map((pack, index) => [pack.file, index]));
  const entries = sortedDirectoryEntrySpool(reduced.directorySpool, {
    packNameMap: reduced.packWriter?.packNameMap,
    packIndexes,
    chunkEntries: config.directorySortChunkEntries
  });
  const directory = await writeDirectoryFilesFromSortedEntries(resolve(dirs.out, "terms"), entries, reduced.directorySpool.entries, config.directoryPageBytes, "terms", { packTable: reduced.packs });
  const shards = [...reduced.finalShards].sort();
  return {
    filters,
    shards,
    directory,
    packs: reduced.packs,
    blockPacks: reduced.blockPacks || [],
    blockStats: reduced.blockStats || emptyPostingSegmentStats(),
    termCount: reduced.termCount,
    postingCount: reduced.postingCount,
    bundleDfs: reduced.bundleDfs || new Map(),
    packBytes: reduced.packBytes,
    blockPackBytes: reduced.blockPackBytes || 0,
    directorySpoolBytes: reduced.directorySpool.bytes,
    directorySpoolEntries: reduced.directorySpool.entries,
    segmentSummary: runData.segmentSummary,
    mergeTiers: stats.mergeTiers || [],
    mergePolicy: stats.mergePolicy || null,
    workerCodeStoreCacheChunks: reduced.workerCodeStoreCacheChunks || 0,
    workerStats: reduced.workerStats || [],
    reduceTimings: {
      ...(reduced.reduceTimings || {}),
      segmentTierMergeMs: stats.timings?.tierMergeMs || 0,
      segmentPrefixCountMs: stats.timings?.prefixCountMs || 0,
      segmentPartitionAssemblyMs: stats.timings?.partitionAssemblyMs || 0
    },
    partitionSpoolBytes: stats.partitionSpoolBytes || 0,
    partitionSpoolEntries: stats.partitionSpoolEntries || 0,
    typoIndexTerms
  };
}

function impactForBundleScore(score, df, total) {
  return impactForBundleScoreInt(Math.max(1, Math.round(score * 1000)), df, total);
}

function impactForBundleScoreInt(scoreInt, df, total) {
  const idf = Math.log(1 + (total - df + 0.5) / (df + 0.5));
  return Math.max(1, Math.round(scoreInt * idf / 10));
}

function queryBundlePivot(seed, termDfs) {
  return seed.baseTerms
    .slice()
    .sort((a, b) => (termDfs.get(a) || Infinity) - (termDfs.get(b) || Infinity) || a.localeCompare(b))[0] || seed.baseTerms[0];
}

function queryBundleSeedIndex(seeds, termDfs) {
  const byPivot = new Map();
  for (const seed of seeds || []) {
    const pivot = queryBundlePivot(seed, termDfs);
    if (!byPivot.has(pivot)) byPivot.set(pivot, []);
    byPivot.get(pivot).push(seed);
  }
  return byPivot;
}

function emitQueryBundleRows(collector, seedIndex, termDfs, total, selectedTerms, doc, options = {}) {
  const selected = new Map(selectedTerms);
  const seenKeys = new Set();
  const scaledScores = options.scaledScores === true;

  function emitSeed(seed) {
    if (!seed || seenKeys.has(seed.key) || !seed.baseTerms.every(base => selected.has(base))) return;
    seenKeys.add(seed.key);
    let score = 0;
    for (const scoringTerm of seed.expandedTerms) {
      if (!selected.has(scoringTerm)) continue;
      const df = termDfs.get(scoringTerm);
      if (!df) continue;
      score += scaledScores
        ? impactForBundleScoreInt(selected.get(scoringTerm), df, total)
        : impactForBundleScore(selected.get(scoringTerm), df, total);
    }
    addQueryBundleRow(collector, seed.key, doc, score);
  }

  for (const [term] of selectedTerms) {
    if (term.includes("_")) continue;
    for (const seed of seedIndex.get(term) || []) emitSeed(seed);
  }
}

function buildTelemetryDiskByteGroups(out, buildRootPath = resolve(out, "_build")) {
  return {
    build: [buildRootPath],
    final_packs: [
      resolve(out, "terms", "packs"),
      resolve(out, "docs", "packs"),
      resolve(out, "docs", "page-packs"),
      resolve(out, "doc-values", "packs"),
      resolve(out, "doc-values", "sorted-packs"),
      resolve(out, "filter-bitmaps", "packs"),
      resolve(out, "facets", "packs"),
      resolve(out, "bundles", "packs"),
      resolve(out, "authority", "packs"),
      resolve(out, "sort-replicas")
    ],
    sidecars: [
      resolve(out, "terms", "block-packs"),
      resolve(out, "terms", "directory"),
      resolve(out, "docs", "pointers"),
      resolve(out, "docs", "ordinals"),
      resolve(out, "docs", "pages"),
      resolve(out, "doc-values", "sorted"),
      resolve(out, "filter-bitmaps", "manifest.json.gz"),
      resolve(out, "facets", "directory"),
      resolve(out, "bundles", "directory"),
      resolve(out, "authority", "directory"),
      resolve(out, "segments"),
      resolve(out, "typo")
    ]
  };
}

function minimalManifest(manifest) {
  return {
    version: manifest.version,
    engine: manifest.engine,
    features: manifest.features,
    object_store: {
      format: manifest.object_store.format,
      pointer_format: manifest.object_store.pointer_format,
      checksum: manifest.object_store.checksum,
      compression: manifest.object_store.compression,
      immutable_names: manifest.object_store.immutable_names,
      name_hash: manifest.object_store.name_hash,
      pack_table: {
        terms: manifest.object_store.pack_table.terms,
        postingBlocks: manifest.object_store.pack_table.postingBlocks,
        docs: manifest.object_store.pack_table.docs,
        docPages: manifest.object_store.pack_table.docPages,
        queryBundles: manifest.object_store.pack_table.queryBundles,
        authority: manifest.object_store.pack_table.authority
      },
      directories: {
        terms: manifest.object_store.directories.terms,
        queryBundles: manifest.object_store.directories.queryBundles,
        authority: manifest.object_store.directories.authority
      },
      pointers: manifest.object_store.pointers
    },
    built_at: manifest.built_at,
    segments: manifest.segments,
    lazy_manifests: {
      full: "manifest.full.json",
      build: "debug/build-telemetry.json",
      optimizer: INDEX_OPTIMIZER_PATH,
      doc_values: "doc-values/manifest.json.gz",
      doc_value_sorted: "doc-values/sorted/manifest.json.gz",
      filter_bitmaps: "filter-bitmaps/manifest.json.gz",
      facet_dictionaries: "facets/manifest.json.gz"
    },
    total: manifest.total,
    docs: manifest.docs,
    initial_results: manifest.initial_results,
    fields: manifest.fields,
    facets: Object.fromEntries(Object.entries(manifest.facets || {}).map(([field, values]) => [field, { count: values.count ?? values.length ?? 0 }])),
    numbers: manifest.numbers,
    booleans: manifest.booleans,
    sorts: manifest.sorts,
    block_filters: manifest.block_filters,
    directory: manifest.directory,
    sort_replicas: manifest.sort_replicas,
    query_bundles: manifest.query_bundles,
    authority: manifest.authority,
    optimizer: manifest.optimizer,
    typo: manifest.typo ? {
      manifest: manifest.typo.manifest,
      manifest_hash: manifest.typo.manifest_hash
    } : null,
    stats: manifest.stats
  };
}

function serializableBuildError(error) {
  return {
    name: error?.name || "Error",
    message: error?.message || String(error),
    stack: error?.stack || ""
  };
}

function writeBuildFailureArtifacts(dirs, telemetry, error) {
  const debugDir = resolve(dirs.out, "debug");
  mkdirSync(debugDir, { recursive: true });
  const failedTelemetry = {
    ...finishBuildTelemetry(telemetry),
    status: "failed",
    error: serializableBuildError(error)
  };
  writeFileSync(resolve(debugDir, "build-telemetry.failed.json"), JSON.stringify(failedTelemetry, null, 2));
  writeFileSync(resolve(debugDir, "build-failure.json"), JSON.stringify({
    status: "failed",
    error: failedTelemetry.error,
    cleanup: {
      preserved: "_build/resume"
    }
  }, null, 2));
}

async function buildQueryBundleIndex(config, measured, dirs, seeds, termDfs, selectedTermSpool, filters, codes) {
  if (!queryBundlesEnabled(config) || !seeds?.length || !termDfs?.size) return null;
  const seedIndex = queryBundleSeedIndex(seeds, termDfs);
  const collector = createQueryBundleCollector(seeds, config.queryBundleMaxRows);

  for await (const { doc, selectedTerms } of readSelectedTermSpool(selectedTermSpool.path)) {
    emitQueryBundleRows(collector, seedIndex, termDfs, measured.total, selectedTerms, doc, { scaledScores: true });
  }
  const bundles = queryBundleCollectorResults(collector);
  if (!bundles.length) return null;
  return writeQueryBundleObjects({
    outDir: dirs.out,
    config,
    bundles,
    coverage: "all-base-docs",
    filters,
    codes
  });
}

const BUILD_RESUME_SCHEMA_VERSION = 1;

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function buildFingerprint(config) {
  const inputStat = statSync(config.input);
  const configForHash = { ...config };
  for (const key of ["_buildRoot", "debugFailAfterStage", "failAfterStage"]) delete configForHash[key];
  const payload = {
    schema: BUILD_RESUME_SCHEMA_VERSION,
    config: configForHash,
    input: {
      path: config.input,
      size: inputStat.size,
      mtimeMs: Math.floor(inputStat.mtimeMs)
    }
  };
  return createHash("sha256").update(stableJson(payload)).digest("hex").slice(0, 24);
}

function buildRoot(config) {
  return config._buildRoot || resolve(config.output, "_build");
}

function buildPath(config, ...parts) {
  return resolve(buildRoot(config), ...parts);
}

function stagePath(config, stage) {
  return buildPath(config, "stages", `${stage}.json`);
}

function readStage(config, stage) {
  if (!config.resumeBuild) return null;
  const path = stagePath(config, stage);
  if (!existsSync(path)) return null;
  const data = JSON.parse(readFileSync(path, "utf8"));
  return data?.status === "complete" && data.schema === BUILD_RESUME_SCHEMA_VERSION ? data.payload : null;
}

function writeAtomic(path, data) {
  mkdirSync(dirname(path), { recursive: true });
  const tmp = `${path}.${process.pid}.${Date.now()}.tmp`;
  writeFileSync(tmp, data);
  renameSync(tmp, path);
}

function writeStage(config, stage, payload) {
  if (!config.resumeBuild) return;
  writeAtomic(stagePath(config, stage), JSON.stringify({
    schema: BUILD_RESUME_SCHEMA_VERSION,
    status: "complete",
    stage,
    completedAt: new Date().toISOString(),
    payload
  }, null, 2));
}

function maybeFailAfterStage(config, stage) {
  if (config.debugFailAfterStage === stage || config.failAfterStage === stage) {
    throw new Error(`Rangefind debug failure after ${stage}`);
  }
}

async function runResumableStage(config, telemetry, stage, phase, run, hydrate = value => value) {
  const cached = readStage(config, stage);
  if (cached) return hydrate(cached);
  const value = await timeBuildPhase(telemetry, phase, run);
  writeStage(config, stage, value);
  maybeFailAfterStage(config, stage);
  return hydrate(value);
}

function serializeDicts(dicts) {
  return Object.fromEntries(Object.entries(dicts || {}).map(([name, dict]) => [name, { values: dict.values || [] }]));
}

function hydrateDicts(dicts) {
  return Object.fromEntries(Object.entries(dicts || {}).map(([name, dict]) => {
    const values = dict.values || [];
    return [name, {
      values,
      ids: new Map(values.map((item, index) => [String(item.value || ""), index]))
    }];
  }));
}

function serializeMeasured(measured) {
  return {
    total: measured.total,
    avgLens: measured.avgLens,
    dicts: serializeDicts(measured.dicts)
  };
}

function hydrateMeasured(payload) {
  return {
    total: payload.total,
    avgLens: payload.avgLens || {},
    dicts: hydrateDicts(payload.dicts)
  };
}

function serializeDocSpool(spool) {
  return {
    path: spool.path,
    entryPath: spool.entryPath,
    rawPath: spool.rawPath,
    rawEntryPath: spool.rawEntryPath,
    layoutPath: spool.layoutPath,
    bytes: spool.bytes || 0,
    rawBytes: spool.rawBytes || 0,
    layoutDocs: spool.layoutDocs || 0
  };
}

function serializeSelectedTermSpool(spool) {
  return {
    path: spool.path,
    docs: spool.docs || 0,
    terms: spool.terms || 0,
    bytes: spool.bytes || 0
  };
}

function serializeTypoBuffer(buffer) {
  if (!buffer) return null;
  return {
    runsOut: buffer.runsOut,
    options: buffer.options,
    terms: buffer.terms || 0,
    deletePairs: buffer.deletePairs || 0,
    lexiconPairs: buffer.lexiconPairs || 0,
    shards: [...(buffer.shards || [])],
    lexiconShards: [...(buffer.lexiconShards || [])]
  };
}

function hydrateTypoBuffer(payload) {
  if (!payload?.options?.enabled) return null;
  return {
    byShard: new Map(),
    lexiconByShard: new Map(),
    lines: 0,
    lexiconLines: 0,
    runsOut: payload.runsOut,
    options: payload.options,
    terms: payload.terms || 0,
    deletePairs: payload.deletePairs || 0,
    lexiconPairs: payload.lexiconPairs || 0,
    shards: new Set(payload.shards || []),
    lexiconShards: new Set(payload.lexiconShards || [])
  };
}

function serializeScanStage(runData, measured, typoBuffer) {
  const codeDescriptor = runData.codes.descriptor();
  return {
    codeDescriptor: { ...codeDescriptor, dicts: serializeDicts(measured.dicts) },
    initialResults: runData.initialResults,
    segments: runData.segments,
    segmentSummary: runData.segmentSummary,
    docSpool: serializeDocSpool(runData.docSpool),
    selectedTermSpool: serializeSelectedTermSpool(runData.selectedTermSpool),
    queryBundleSeeds: runData.queryBundleSeeds,
    queryBundleTerms: runData.queryBundleTerms,
    authorityBaseShards: runData.authorityBaseShards,
    scanWorkerStats: runData.scanWorkerStats,
    typoBuffer: serializeTypoBuffer(typoBuffer)
  };
}

function hydrateScanStage(payload, measured) {
  return {
    codes: openCodeStore({ ...payload.codeDescriptor, dicts: measured.dicts }),
    initialResults: payload.initialResults || [],
    segments: payload.segments || [],
    segmentSummary: payload.segmentSummary || { segments: 0, terms: 0, postings: 0 },
    docSpool: payload.docSpool,
    selectedTermSpool: payload.selectedTermSpool,
    queryBundleSeeds: payload.queryBundleSeeds || [],
    queryBundleTerms: payload.queryBundleTerms || [],
    authorityBaseShards: payload.authorityBaseShards || [],
    scanWorkerStats: payload.scanWorkerStats || []
  };
}

function serializeReducedStage(reduced, typoBuffer) {
  return {
    ...reduced,
    finalShards: undefined,
    bundleDfs: [...(reduced.bundleDfs || new Map()).entries()],
    typoBuffer: serializeTypoBuffer(typoBuffer)
  };
}

function hydrateReducedStage(payload) {
  return {
    ...payload,
    bundleDfs: new Map(payload.bundleDfs || [])
  };
}

export async function build({ configPath }) {
  const config = await readConfig(configPath);
  const fingerprint = config.resumeBuild ? buildFingerprint(config) : "scratch";
  config._buildRoot = config.resumeBuild
    ? resolve(config.output, config.resumeDir, fingerprint)
    : resolve(config.output, "_build");
  const dirs = {
    out: config.output,
    build: config._buildRoot,
    typoRunsOut: resolve(config._buildRoot, "typo-runs"),
    authorityRunsOut: resolve(config._buildRoot, "authority-runs")
  };
  const telemetry = createBuildTelemetry({
    sampleIntervalMs: config.buildTelemetrySampleMs,
    progressLogMs: config.buildProgressLogMs,
    progressLogger: line => console.error(line),
    diskByteGroups: buildTelemetryDiskByteGroups(dirs.out, dirs.build)
  });
  mkdirSync(resolve(dirs.out, "docs"), { recursive: true });
  mkdirSync(resolve(dirs.out, "terms"), { recursive: true });
  mkdirSync(resolve(dirs.build, "stages"), { recursive: true });
  mkdirSync(dirs.authorityRunsOut, { recursive: true });

  let runData = null;
  try {
    console.log(`Rangefind: reading ${config.input}`);
    const measured = await runResumableStage(
      config,
      telemetry,
      "measure",
      "measure",
      async () => serializeMeasured(await measure(config)),
      hydrateMeasured
    );
    const typo = typoOptions(config);
    let typoBuffer = null;
    const scanStage = readStage(config, "scan");
    if (scanStage) {
      runData = hydrateScanStage(scanStage, measured);
      typoBuffer = hydrateTypoBuffer(scanStage.typoBuffer);
    } else {
      typoBuffer = typo.enabled ? createTypoRunBuffer(dirs.typoRunsOut, typo) : null;
      runData = await timeBuildPhase(telemetry, "scan-and-spool", () => writePostingRuns(config, measured, dirs, typoBuffer));
      writeStage(config, "scan", serializeScanStage(runData, measured, typoBuffer));
      maybeFailAfterStage(config, "scan");
    }
    recordBuildWorkers(telemetry, "scan-and-spool", runData.scanWorkerStats, {
      configured_workers: scanWorkerCount(config),
      batch_docs: scanBatchDocs(config)
    });
    addBuildCounter(telemetry, "selected_term_spool_bytes", runData.selectedTermSpool.bytes);
    addBuildCounter(telemetry, "selected_term_spool_terms", runData.selectedTermSpool.terms);
    addBuildCounter(telemetry, "doc_raw_spool_bytes", runData.docSpool.rawBytes);
    addBuildCounter(telemetry, "doc_gzip_spool_bytes", runData.docSpool.bytes);
    addBuildCounter(telemetry, "segment_files", runData.segments.length);
    addBuildCounter(telemetry, "segment_postings", runData.segmentSummary.postings);
    applyAutoPostingLayout(config, measured, runData);
    const reduceStage = readStage(config, "reduce");
    let reduced;
    if (reduceStage) {
      reduced = hydrateReducedStage(reduceStage);
      typoBuffer = hydrateTypoBuffer(reduceStage.typoBuffer) || typoBuffer;
    } else {
      reduced = await timeBuildPhase(telemetry, "reduce-postings", () => reduceRuns(config, measured, runData, dirs, typoBuffer));
      writeStage(config, "reduce", serializeReducedStage(reduced, typoBuffer));
      maybeFailAfterStage(config, "reduce");
    }
    recordBuildWorkers(telemetry, "reduce-postings", reduced.workerStats, {
      final_pack_assembly_ms: reduced.reduceTimings.finalPackAssemblyMs || 0,
      segment_tier_merge_ms: reduced.reduceTimings.segmentTierMergeMs || 0,
      segment_prefix_count_ms: reduced.reduceTimings.segmentPrefixCountMs || 0,
      segment_partition_assembly_ms: reduced.reduceTimings.segmentPartitionAssemblyMs || 0,
      segment_partition_spool_bytes: reduced.partitionSpoolBytes || 0,
      segment_partition_spool_entries: reduced.partitionSpoolEntries || 0,
      segment_directory_spool_bytes: reduced.directorySpoolBytes || 0,
      segment_directory_spool_entries: reduced.directorySpoolEntries || 0,
      segment_merge_policy: reduced.mergePolicy?.policy || "",
      segment_merge_target_segments: reduced.mergePolicy?.targetSegments || 0,
      segment_merge_write_amplification: reduced.mergePolicy?.writeAmplification || 0,
      segment_merge_intermediate_bytes: reduced.mergePolicy?.intermediateBytes || 0,
      segment_merge_skipped_segments: reduced.mergePolicy?.skippedSegments || 0,
      segment_merge_blocked_by_temp_budget: Boolean(reduced.mergePolicy?.blockedByTempBudget),
      segment_merge_fan_in: config.segmentMergeFanIn,
      segment_merge_tiers: reduced.mergeTiers.length,
      segment_merge_tier_outputs: reduced.mergeTiers.map(tier => tier.output_segments),
      partition_reducer_workers: reduced.workerStats.length,
      partition_reducer_worker_mode: reduced.workerStats.some(worker => worker.mode === "worker-thread") ? "worker-thread-owned-packs" : "main-thread",
      partition_reducer_credit_limit_bytes: reduced.reduceTimings.partitionScheduler?.creditLimitBytes || 0,
      partition_reducer_max_active_input_bytes: reduced.reduceTimings.partitionScheduler?.maxActiveInputBytes || 0,
      partition_reducer_credit_wait_ms: reduced.reduceTimings.partitionScheduler?.creditWaitMs || 0,
      partition_reducer_credit_waits: reduced.reduceTimings.partitionScheduler?.creditWaits || 0,
      partition_reducer_oversized_partitions: reduced.reduceTimings.partitionScheduler?.oversizedPartitions || 0,
      partition_reducer_finish_mode: reduced.reduceTimings.partitionScheduler?.finishMode || "",
      typo_index_terms: reduced.typoIndexTerms || 0
    });
    const segmentManifest = await runResumableStage(config, telemetry, "segment-manifest", "segment-manifest", () => writeSegmentManifest(dirs.out, {
      config,
      total: measured.total,
      segments: runData.segments,
      summary: runData.segmentSummary,
      mergeTiers: reduced.mergeTiers,
      mergePolicy: reduced.mergePolicy,
      publishSegments: true
    }));
    const fieldRows = createFieldRowPipeline(runData.codes, config, measured.total);
    addBuildCounter(telemetry, "field_row_fields", fieldRows.fieldCount);
    addBuildCounter(telemetry, "field_row_facet_fields", fieldRows.facetFields);
    addBuildCounter(telemetry, "field_row_numeric_fields", fieldRows.numericFields);
    addBuildCounter(telemetry, "field_row_boolean_fields", fieldRows.booleanFields);
    addBuildCounter(telemetry, "field_row_date_fields", fieldRows.dateFields);
    let sortReplicas;
    let queryBundles;
    let authority;
    let docs;
    let typoManifest;
    let docValues;
    let docValueSorted;
    let filterBitmaps;
    let facetDictionaries;
    const sidecarStage = readStage(config, "sidecars");
    if (sidecarStage) {
      ({ sortReplicas, queryBundles, authority, docs, typoManifest, docValues, docValueSorted, filterBitmaps, facetDictionaries } = sidecarStage);
    } else {
      sortReplicas = await timeBuildPhase(telemetry, "sort-replicas", () => buildSortReplicas(config, measured, dirs, runData.selectedTermSpool, runData.docSpool, fieldRows));
      queryBundles = await timeBuildPhase(telemetry, "query-bundles", () => buildQueryBundleIndex(config, measured, dirs, runData.queryBundleSeeds, reduced.bundleDfs, runData.selectedTermSpool, reduced.filters, fieldRows));
      authority = await timeBuildPhase(telemetry, "authority", () => reduceAuthorityRuns(config, dirs, runData.authorityBaseShards));
      docs = await timeBuildPhase(telemetry, "doc-packs", () => finishDocPacks(dirs.out, runData.docSpool, measured.total, config));
      docs.pages = await timeBuildPhase(telemetry, "doc-pages", () => finishDocPages(dirs.out, runData.docSpool, measured.total, config));
      typoManifest = await timeBuildPhase(telemetry, "typo", () => reduceTypoRuns(typoBuffer, dirs.out));
      docValues = await timeBuildPhase(telemetry, "doc-values", () => writeDocValuePacks(dirs.out, config, measured.total, fieldRows));
      docValueSorted = await timeBuildPhase(telemetry, "doc-value-sorted", () => writeDocValueSortedIndexes(dirs.out, config, measured.total, fieldRows));
      filterBitmaps = await timeBuildPhase(telemetry, "filter-bitmaps", () => writeFilterBitmapIndex(dirs.out, config, measured.total, fieldRows, measured.dicts));
      facetDictionaries = await timeBuildPhase(telemetry, "facet-dictionaries", () => writeFacetDictionaries(dirs.out, measured.dicts, config));
      writeStage(config, "sidecars", { sortReplicas, queryBundles, authority, docs, typoManifest, docValues, docValueSorted, filterBitmaps, facetDictionaries });
      maybeFailAfterStage(config, "sidecars");
    }
    addBuildCounter(telemetry, "sort_replica_count", sortReplicas.count);
    addBuildCounter(telemetry, "sort_replica_docs", sortReplicas.stats.docs);
    addBuildCounter(telemetry, "sort_replica_postings", sortReplicas.stats.postings);
    addBuildCounter(telemetry, "sort_replica_doc_page_pack_bytes", sortReplicas.stats.docPagePackBytes);
    const buildTelemetry = finishBuildTelemetry(telemetry);

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
        fieldRowPipeline: true,
        docValues: true,
        docValueSorted: true,
        filterBitmaps: Object.keys(filterBitmaps.fields).length > 0,
        facetDictionaries: true,
        externalPostingBlocks: config.externalPostingBlocks !== false,
        segmentManifest: true,
        queryBundles: !!queryBundles,
        authority: !!authority,
        sortReplicas: sortReplicas.count > 0,
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
        docValueSorted: packTable(docValueSorted.packs),
        filterBitmaps: packTable(filterBitmaps.packs),
        facets: packTable(facetDictionaries.pack_objects),
        queryBundles: packTable(queryBundles?.packs),
        authority: packTable(authority?.packs),
        typo: packTable(typoManifest?.packs),
        typoLexicon: packTable(typoManifest?.lexicon?.packs)
      },
      dedupe: summarizeDedup(
        reduced.packs,
        reduced.blockPacks,
        docs.packs,
        docs.pages.packs,
        docValues.packs,
        docValueSorted.packs,
        filterBitmaps.packs,
        facetDictionaries.pack_objects,
        queryBundles?.packs || [],
        authority?.packs || [],
        typoManifest?.packs || [],
        typoManifest?.lexicon?.packs || []
      ),
      directories: {
        terms: reduced.directory,
        facets: facetDictionaries.directory,
        queryBundles: queryBundles?.directory || null,
        authority: authority?.directory || null,
        typo: typoManifest?.directory || null
      },
      pointers: {
        docs: docs.pointers,
        docPages: docs.pages.pointers
      }
    },
    built_at: new Date().toISOString(),
    build: buildTelemetry,
    field_rows: fieldRows.descriptor(),
    segments: {
      format: segmentManifest.format,
      source_format: segmentManifest.sourceFormat,
      storage: segmentManifest.storage,
      published: segmentManifest.published,
      manifest: segmentManifest.path,
      count: segmentManifest.segmentCount,
      bytes: segmentManifest.compressedBytes,
      term_count: segmentManifest.termCount,
      posting_count: segmentManifest.postingCount
    },
    total: measured.total,
    docs,
    doc_values: {
      storage: docValues.storage,
      compression: docValues.compression,
      format: docValues.format,
      chunk_size: docValues.chunk_size,
      lookup_chunk_size: docValues.lookup_chunk_size,
      fields: docValues.fields,
      packs: docValues.packs.length
    },
    doc_value_sorted: {
      storage: docValueSorted.storage,
      compression: docValueSorted.compression,
      directory_format: docValueSorted.directory_format,
      page_format: docValueSorted.page_format,
      page_size: docValueSorted.page_size,
      fields: docValueSorted.fields,
      packs: docValueSorted.packs.length,
      pack_table: docValueSorted.pack_table
    },
    sort_replicas: sortReplicas,
    filter_bitmaps: {
      storage: filterBitmaps.storage,
      compression: filterBitmaps.compression,
      format: filterBitmaps.format,
      max_facet_values: filterBitmaps.max_facet_values,
      fields: filterBitmaps.fields,
      packs: filterBitmaps.packs.length,
      pack_table: filterBitmaps.pack_table
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
    query_bundles: queryBundles ? {
      storage: queryBundles.storage,
      compression: queryBundles.compression,
      format: queryBundles.format,
      coverage: queryBundles.coverage,
      max_rows: queryBundles.max_rows,
      row_group_size: queryBundles.row_group_size,
      row_group_filter_fields: queryBundles.row_group_filter_fields,
      keys: queryBundles.keys,
      directory: queryBundles.directory,
      packs: queryBundles.packs.length,
      stats: {
        seed_keys: runData.queryBundleSeeds.length,
        seed_terms: runData.queryBundleTerms.length,
        pack_files: queryBundles.packs.length,
        pack_bytes: queryBundles.pack_bytes,
        directory_bytes: queryBundles.directory_bytes
      }
    } : null,
    authority: authority ? {
      storage: authority.storage,
      compression: authority.compression,
      format: authority.format,
      fields: authority.fields,
      max_rows_per_key: authority.max_rows_per_key,
      base_shard_depth: authority.base_shard_depth,
      max_shard_depth: authority.max_shard_depth,
      target_shard_rows: authority.target_shard_rows,
      keys: authority.keys,
      rows: authority.rows,
      shards: authority.shards,
      directory: authority.directory,
      packs: authority.packs.length,
      stats: {
        pack_files: authority.packs.length,
        pack_bytes: authority.pack_bytes,
        directory_bytes: authority.directory_bytes
      }
    } : null,
    typo: typoManifest ? {
      format: typoManifest.format,
      compression: typoManifest.compression,
      manifest: typoManifest.manifest,
      manifest_hash: typoManifest.manifest_hash,
      directory: typoManifest.directory,
      shards: typoManifest.directory.entries,
      packs: typoManifest.packs.length,
      lexicon: typoManifest.lexicon ? {
        format: typoManifest.lexicon.format,
        directory: typoManifest.lexicon.directory,
        shards: typoManifest.lexicon.directory.entries,
        packs: typoManifest.lexicon.packs.length,
        stats: typoManifest.lexicon.stats
      } : null,
      stats: {
        ...typoManifest.stats,
        pack_files: typoManifest.packs.length,
        lexicon_pack_files: typoManifest.lexicon?.packs.length || 0,
        lexicon_pack_bytes: typoManifest.lexicon?.stats?.pack_bytes || 0,
        lexicon_directory_bytes: typoManifest.lexicon?.stats?.directory_bytes || 0
      }
    } : null,
    stats: {
      terms: reduced.termCount,
      postings: reduced.postingCount,
      build_total_ms: Math.round(buildTelemetry.total_ms),
      build_peak_rss: buildTelemetry.peak_rss,
      selected_term_spool_bytes: runData.selectedTermSpool.bytes,
      selected_term_spool_terms: runData.selectedTermSpool.terms,
      doc_raw_spool_bytes: runData.docSpool.rawBytes,
      doc_gzip_spool_bytes: runData.docSpool.bytes,
      posting_segment_storage: "range-pack-v1",
      posting_segment_format: POSTING_SEGMENT_FORMAT,
      posting_segment_block_storage: config.externalPostingBlocks === false ? "inline" : "range-pack-v1",
      posting_segment_directory_format: reduced.directory.format,
      posting_segment_directory_page_files: reduced.directory.page_files,
      posting_segment_directory_bytes: reduced.directory.total_bytes,
      posting_segment_pack_files: reduced.packs.length,
      posting_segment_pack_bytes: reduced.packBytes,
      posting_segment_stream_min_bytes: config.postingSegmentStreamMinBytes,
      posting_segment_block_pack_files: reduced.blockPacks.length,
      posting_segment_block_pack_bytes: reduced.blockPackBytes,
      external_posting_segment_blocks: reduced.blockStats.externalBlocks,
      external_posting_segment_terms: reduced.blockStats.externalTerms,
      external_posting_segment_postings: reduced.blockStats.externalPostings,
      external_posting_segment_source_bytes: reduced.blockStats.externalPostingBytes,
      inline_posting_segment_source_bytes: reduced.blockStats.inlinePostingBytes,
      posting_segment_superblocks: reduced.blockStats.superblocks,
      posting_segment_superblock_terms: reduced.blockStats.superblockTerms,
      posting_segment_superblock_blocks: reduced.blockStats.superblockBlocks,
      posting_segment_superblock_size: config.postingSuperblockSize,
      posting_segment_superblock_size_source: config._layoutDecisions?.posting_superblock_size?.source || "configured",
      posting_segment_codec_mode: config._layoutDecisions?.codecs?.mode || "varint",
      posting_segment_codec: config._layoutDecisions?.codecs?.selected_posting_codec || "pair-varint-v1",
      posting_segment_block_codec_pair_varint_blocks: reduced.blockStats.pairVarintBlocks,
      posting_segment_block_codec_impact_run_blocks: reduced.blockStats.impactRunBlocks,
      posting_segment_block_codec_impact_bitset_blocks: reduced.blockStats.impactBitsetBlocks,
      posting_segment_block_codec_partitioned_delta_blocks: reduced.blockStats.partitionedDeltaBlocks,
      posting_segment_block_codec_baseline_bytes: reduced.blockStats.blockCodecBaselineBytes,
      posting_segment_block_codec_selected_bytes: reduced.blockStats.blockCodecSelectedBytes,
      posting_segment_block_codec_impact_run_candidate_bytes: reduced.blockStats.blockCodecImpactRunCandidateBytes,
      posting_segment_block_codec_impact_bitset_candidate_bytes: reduced.blockStats.blockCodecImpactBitsetCandidateBytes,
      posting_segment_block_codec_partitioned_delta_candidate_bytes: reduced.blockStats.blockCodecPartitionedDeltaCandidateBytes,
      posting_segment_block_codec_bytes_saved: Math.max(0, reduced.blockStats.blockCodecBaselineBytes - reduced.blockStats.blockCodecSelectedBytes),
      posting_segment_codec_planner_mode: config._layoutDecisions?.codecs?.mode || "varint",
      posting_segment_codec_planner_sampled_terms: reduced.blockStats.codecPlannerSampledTerms,
      posting_segment_codec_planner_sampled_blocks: reduced.blockStats.codecPlannerSampledBlocks,
      posting_segment_codec_planner_skip_impact_candidates: reduced.blockStats.codecPlannerSkipImpactCandidates,
      posting_segment_codec_planner_skip_bitset_candidates: reduced.blockStats.codecPlannerSkipBitsetCandidates,
      posting_segment_codec_planner_skip_partitioned_delta_candidates: reduced.blockStats.codecPlannerSkipPartitionedDeltaCandidates,
      posting_segment_impact_bucket_order_terms: reduced.blockStats.impactBucketOrderTerms,
      posting_segment_impact_bucket_order_postings: reduced.blockStats.impactBucketOrderPostings,
      posting_segment_impact_tier_terms: reduced.blockStats.impactTierTerms,
      posting_segment_impact_tier_blocks: reduced.blockStats.impactTierBlocks,
      posting_segment_impact_tier_tiers: reduced.blockStats.impactTierTiers,
      posting_segment_impact_tier_min_blocks: config.postingImpactTierMinBlocks,
      posting_segment_impact_tier_max_blocks: config.postingImpactTierMaxBlocks,
      posting_segment_doc_range_block_max: config.postingDocRangeBlockMax !== false,
      posting_segment_doc_range_size: config.postingDocRangeSize,
      posting_segment_doc_range_quantization_bits: config.postingDocRangeQuantizationBits,
      posting_segment_doc_range_terms: reduced.blockStats.docRangeTerms,
      posting_segment_doc_range_entries: reduced.blockStats.docRangeEntries,
      posting_segment_doc_range_blocks: reduced.blockStats.docRangeBlocks,
      posting_segment_doc_range_block_entries: reduced.blockStats.docRangeBlockEntries,
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
      doc_value_lookup_chunk_size: docValues.lookup_chunk_size,
      doc_value_fields: Object.keys(docValues.fields).length,
      doc_value_pack_files: docValues.packs.length,
      doc_value_pack_bytes: docValues.packs.reduce((sum, pack) => sum + pack.bytes, 0),
      field_row_format: fieldRows.format,
      field_row_source: fieldRows.source,
      field_row_fields: fieldRows.fieldCount,
      field_row_facet_fields: fieldRows.facetFields,
      field_row_numeric_fields: fieldRows.numericFields,
      field_row_boolean_fields: fieldRows.booleanFields,
      field_row_date_fields: fieldRows.dateFields,
      doc_value_sorted_storage: docValueSorted.storage,
      doc_value_sorted_directory_format: docValueSorted.directory_format,
      doc_value_sorted_page_format: docValueSorted.page_format,
      doc_value_sorted_page_size: docValueSorted.page_size,
      doc_value_sorted_fields: Object.keys(docValueSorted.fields).length,
      doc_value_sorted_directory_bytes: docValueSorted.directory_bytes,
      doc_value_sorted_directory_logical_bytes: docValueSorted.directory_logical_bytes,
      doc_value_sorted_pack_files: docValueSorted.packs.length,
      doc_value_sorted_pack_bytes: docValueSorted.pack_bytes,
      sort_replica_format: sortReplicas.format,
      sort_replica_count: sortReplicas.count,
      sort_replica_docs: sortReplicas.stats.docs,
      sort_replica_terms: sortReplicas.stats.terms,
      sort_replica_postings: sortReplicas.stats.postings,
      sort_replica_segment_files: sortReplicas.stats.segmentFiles,
      sort_replica_term_pack_files: sortReplicas.stats.termPackFiles,
      sort_replica_term_pack_bytes: sortReplicas.stats.termPackBytes,
      sort_replica_block_pack_files: sortReplicas.stats.blockPackFiles,
      sort_replica_block_pack_bytes: sortReplicas.stats.blockPackBytes,
      sort_replica_rank_pack_files: sortReplicas.stats.rankPackFiles,
      sort_replica_rank_pack_bytes: sortReplicas.stats.rankPackBytes,
      sort_replica_doc_pack_files: sortReplicas.stats.docPackFiles,
      sort_replica_doc_pack_bytes: sortReplicas.stats.docPackBytes,
      sort_replica_doc_pointer_bytes: sortReplicas.stats.docPointerBytes,
      sort_replica_doc_page_pack_files: sortReplicas.stats.docPagePackFiles,
      sort_replica_doc_page_pack_bytes: sortReplicas.stats.docPagePackBytes,
      sort_replica_doc_page_pointer_bytes: sortReplicas.stats.docPagePointerBytes,
      sort_replica_directory_bytes: sortReplicas.stats.directoryBytes,
      filter_bitmap_storage: filterBitmaps.storage,
      filter_bitmap_format: filterBitmaps.format,
      filter_bitmap_fields: Object.keys(filterBitmaps.fields).length,
      filter_bitmap_pack_files: filterBitmaps.packs.length,
      filter_bitmap_pack_bytes: filterBitmaps.pack_bytes,
      facet_dictionary_storage: facetDictionaries.storage,
      facet_dictionary_format: facetDictionaries.format,
      facet_dictionary_page_files: facetDictionaries.directory.page_files,
      facet_dictionary_bytes: facetDictionaries.directory.total_bytes + facetDictionaries.pack_bytes,
      facet_dictionary_fields: Object.keys(facetDictionaries.fields).length,
      query_bundle_format: queryBundles?.format || "",
      query_bundle_seed_keys: runData.queryBundleSeeds.length,
      query_bundle_seed_terms: runData.queryBundleTerms.length,
      query_bundle_keys: queryBundles?.keys || 0,
      query_bundle_max_rows: queryBundles?.max_rows || 0,
      query_bundle_row_group_size: queryBundles?.row_group_size || 0,
      query_bundle_row_group_filter_fields: queryBundles?.row_group_filter_fields || 0,
      query_bundle_directory_bytes: queryBundles?.directory_bytes || 0,
      query_bundle_pack_files: queryBundles?.packs.length || 0,
      query_bundle_pack_bytes: queryBundles?.pack_bytes || 0,
      typo_index_terms: reduced.typoIndexTerms || 0,
      authority_format: authority?.format || "",
      authority_fields: authority?.fields.length || 0,
      authority_keys: authority?.keys || 0,
      authority_rows: authority?.rows || 0,
      authority_shards: authority?.shards || 0,
      authority_target_shard_rows: authority?.target_shard_rows || 0,
      authority_directory_bytes: authority?.directory_bytes || 0,
      authority_pack_files: authority?.packs.length || 0,
      authority_pack_bytes: authority?.pack_bytes || 0,
      scan_workers: scanWorkerCount(config),
      scan_batch_docs: scanBatchDocs(config),
      segment_merge_workers: 1,
      partition_reducer_workers: reduced.workerStats.length,
      partition_reducer_worker_mode: reduced.workerStats.some(worker => worker.mode === "worker-thread") ? "worker-thread-owned-packs" : "main-thread",
      partition_reducer_credit_limit_bytes: reduced.reduceTimings.partitionScheduler?.creditLimitBytes || 0,
      partition_reducer_max_active_input_bytes: reduced.reduceTimings.partitionScheduler?.maxActiveInputBytes || 0,
      partition_reducer_credit_wait_ms: Math.round(reduced.reduceTimings.partitionScheduler?.creditWaitMs || 0),
      partition_reducer_credit_waits: reduced.reduceTimings.partitionScheduler?.creditWaits || 0,
      partition_reducer_oversized_partitions: reduced.reduceTimings.partitionScheduler?.oversizedPartitions || 0,
      partition_reducer_finish_mode: reduced.reduceTimings.partitionScheduler?.finishMode || "",
      code_store_worker_cache_chunks: reduced.workerCodeStoreCacheChunks || 0,
      segment_merge_fan_in: config.segmentMergeFanIn,
      segment_merge_tiers: reduced.mergeTiers.length,
      segment_partition_spool_bytes: reduced.partitionSpoolBytes || 0,
      segment_partition_spool_entries: reduced.partitionSpoolEntries || 0,
      segment_directory_spool_bytes: reduced.directorySpoolBytes || 0,
      segment_directory_spool_entries: reduced.directorySpoolEntries || 0,
      segment_merge_policy: reduced.mergePolicy?.policy || "",
      segment_merge_target_segments: reduced.mergePolicy?.targetSegments || 0,
      segment_merge_write_amplification: reduced.mergePolicy?.writeAmplification || 0,
      segment_merge_intermediate_bytes: reduced.mergePolicy?.intermediateBytes || 0,
      segment_merge_skipped_segments: reduced.mergePolicy?.skippedSegments || 0,
      segment_merge_blocked_by_temp_budget: Boolean(reduced.mergePolicy?.blockedByTempBudget),
      segment_tier_merge_ms: Math.round(reduced.reduceTimings.segmentTierMergeMs || 0),
      segment_prefix_count_ms: Math.round(reduced.reduceTimings.segmentPrefixCountMs || 0),
      segment_partition_assembly_ms: Math.round(reduced.reduceTimings.segmentPartitionAssemblyMs || 0),
      segment_format: "rfsegment-v1",
      segment_manifest_format: segmentManifest.format,
      segment_manifest_path: segmentManifest.path,
      segment_manifest_published: Boolean(segmentManifest.published),
      segment_manifest_storage: segmentManifest.storage,
      segment_manifest_bytes: segmentManifest.compressedBytes,
      segment_files: reduced.segmentSummary.segments,
      segment_terms: reduced.segmentSummary.terms,
      segment_postings: reduced.segmentSummary.postings,
      segment_peak_memory_bytes: reduced.segmentSummary.approxMemoryBytes || 0,
      segment_max_docs: reduced.segmentSummary.maxDocs || 0,
      segment_flush_reasons: reduced.segmentSummary.flushReasons || {},
      segment_flush_docs: config.segmentFlushDocs || config.segmentMaxDocs || 0,
      segment_flush_bytes: config.segmentFlushBytes || config.segmentMaxBytes || 0,
      segment_effective_flush_bytes: runData.segments[0]?.maxBytes || config.segmentFlushBytes || config.segmentMaxBytes || 0,
      builder_memory_budget_bytes: config.builderMemoryBudgetBytes || 0,
      posting_segment_block_size: config.postingBlockSize,
      posting_segment_block_size_source: config._layoutDecisions?.posting_block_size?.source || "configured",
      base_shard_depth: config.baseShardDepth,
      max_shard_depth: config.maxShardDepth,
      target_shard_postings: config.targetShardPostings,
      target_postings_per_doc: config.targetPostingsPerDoc,
      body_index_chars: config.bodyIndexChars,
      always_index_fields: config.alwaysIndexFields,
      max_expansion_terms_per_doc: config.maxExpansionTermsPerDoc,
      proximity_window: Math.max(0, ...config.fields.map(field => field.proximityWindow || 0)),
      scoring: "rangefind-bm25f-phrase-proximity-v2"
    }
  };
  const optimizerReport = buildIndexOptimizerReport({ config, manifest });
  manifest.optimizer = optimizerReport.summary;
    mkdirSync(resolve(dirs.out, "debug"), { recursive: true });
    mkdirSync(resolve(dirs.out, "doc-values"), { recursive: true });
    mkdirSync(resolve(dirs.out, "doc-values", "sorted"), { recursive: true });
    mkdirSync(resolve(dirs.out, "filter-bitmaps"), { recursive: true });
    mkdirSync(resolve(dirs.out, "facets"), { recursive: true });
    writeAtomic(resolve(dirs.out, "debug", "build-telemetry.json"), JSON.stringify(buildTelemetry, null, 2));
    writeAtomic(resolve(dirs.out, INDEX_OPTIMIZER_PATH), JSON.stringify(optimizerReport, null, 2));
    writeAtomic(resolve(dirs.out, "doc-values", "manifest.json.gz"), gzipSync(JSON.stringify(docValues), { level: 6 }));
    writeAtomic(resolve(dirs.out, "doc-values", "sorted", "manifest.json.gz"), gzipSync(JSON.stringify(manifest.doc_value_sorted), { level: 6 }));
    writeAtomic(resolve(dirs.out, "filter-bitmaps", "manifest.json.gz"), gzipSync(JSON.stringify(filterBitmaps), { level: 6 }));
    writeAtomic(resolve(dirs.out, "facets", "manifest.json.gz"), gzipSync(JSON.stringify(facetDictionaries), { level: 6 }));
    writeAtomic(resolve(dirs.out, "manifest.full.json"), JSON.stringify(manifest));
    writeAtomic(resolve(dirs.out, "manifest.min.json"), JSON.stringify(minimalManifest(manifest)));
    writeAtomic(resolve(dirs.out, "manifest.json"), JSON.stringify(manifest));
    writeStage(config, "publish", { manifest: "manifest.json", builtAt: manifest.built_at });
    maybeFailAfterStage(config, "publish");
    if (config.buildTelemetryPath) {
      mkdirSync(dirname(config.buildTelemetryPath), { recursive: true });
      writeFileSync(config.buildTelemetryPath, JSON.stringify(buildTelemetry, null, 2));
    }
    console.log(`Rangefind: built ${measured.total.toLocaleString()} docs, ${reduced.shards.length.toLocaleString()} posting segments, ${reduced.packs.length.toLocaleString()} packs`);
  } catch (error) {
    writeBuildFailureArtifacts(dirs, telemetry, error);
    throw error;
  } finally {
    runData?.codes?.close?.();
  }
}
