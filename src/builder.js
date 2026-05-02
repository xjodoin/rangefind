import {
  appendFileSync,
  createReadStream,
  readdirSync,
  readFileSync,
  mkdirSync,
  openSync,
  closeSync,
  readSync,
  statSync,
  rmSync,
  unlinkSync,
  writeSync,
  writeFileSync
} from "node:fs";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { Worker } from "node:worker_threads";
import { gzipSync, gunzipSync } from "node:zlib";
import { createInterface } from "node:readline";
import { expandedTermsFromBaseTerms, queryBundleKeyFromBaseTerms, tokenize } from "./analyzer.js";
import { addAuthorityDoc, createAuthorityRunBuffer, finishAuthorityRuns, reduceAuthorityRuns } from "./authority_index.js";
import { createCodeStore } from "./build_store.js";
import {
  buildBlockFilters,
  buildDocValueChunk,
  buildFacetDictionary,
  buildTermShard,
  docValueFields,
  rewriteTermShardForExternalBlocks
} from "./codec.js";
import { getPath, readConfig } from "./config.js";
import { DOC_LAYOUT_FORMAT, docLayoutRecord } from "./doc_layout.js";
import { buildDocPagePointerTable, DOC_PAGE_ENCODING, DOC_PAGE_FORMAT, encodeDocPageColumns } from "./doc_pages.js";
import { writeDirectoryFiles } from "./directory_writer.js";
import {
  DOC_VALUE_SORT_DIRECTORY_FORMAT,
  DOC_VALUE_SORT_PAGE_FORMAT,
  encodeDocValueSortDirectory,
  encodeDocValueSortPage
} from "./doc_value_tree.js";
import { buildDocOrdinalTable, buildDocPointerTable, buildDocPointerTableFromReader } from "./doc_pointers.js";
import { eachJsonLine } from "./jsonl.js";
import { OBJECT_CHECKSUM_ALGORITHM, OBJECT_NAME_HASH_LENGTH, OBJECT_POINTER_FORMAT, OBJECT_STORE_FORMAT } from "./object_store.js";
import { createPackWriter, finalizePackWriter, writePackedShard } from "./packs.js";
import { addQueryBundleRow, createQueryBundleCollector, queryBundleCollectorResults, writeQueryBundleObjects } from "./query_bundles.js";
import { reduceRunToPartitions } from "./reduce_stream.js";
import { encodeRunRecord } from "./runs.js";
import { addFieldExpansionScores, addFieldScores, bm25fScores, fieldText, selectDocTerms } from "./scoring.js";
import { baseShardFor } from "./shards.js";
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
    const terms = tokenize(fieldText(doc, field), { unique: false }).slice(0, limit);
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
  const layoutPath = resolve(outDir, "layout.jsonl");
  return {
    path,
    entryPath,
    layoutPath,
    fd: openSync(path, "w"),
    entryFd: openSync(entryPath, "w"),
    layoutFd: openSync(layoutPath, "w"),
    offset: 0,
    bytes: 0,
    layoutDocs: 0
  };
}

function closeDocSpool(spool) {
  for (const key of ["fd", "entryFd", "layoutFd"]) {
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

function writeDocSpoolEntry(spool, index, entry) {
  const buffer = Buffer.alloc(DOC_SPOOL_ENTRY_BYTES);
  writeBigUInt(buffer, 0, entry.offset);
  writeBigUInt(buffer, 8, entry.length);
  writeBigUInt(buffer, 16, entry.logicalLength);
  writeSync(spool.entryFd, buffer, 0, buffer.length, index * DOC_SPOOL_ENTRY_BYTES);
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
  const compressed = gzipSync(bytes, { level: 6 });
  writeSync(spool.fd, compressed, 0, compressed.length, spool.offset);
  writeDocSpoolEntry(spool, index, {
    offset: spool.offset,
    length: compressed.length,
    logicalLength: bytes.length
  });
  writeSync(spool.layoutFd, `${JSON.stringify(layoutRecord)}\n`);
  spool.layoutDocs++;
  spool.offset += compressed.length;
  spool.bytes += compressed.length;
}

function readSpooledDoc(fd, entry) {
  const buffer = Buffer.alloc(entry.length);
  const bytesRead = readSync(fd, buffer, 0, entry.length, entry.offset);
  if (bytesRead !== entry.length) throw new Error("Rangefind doc spool ended before a payload could be read.");
  return buffer;
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
    if (rows.length >= chunkDocs) chunks.push(writeLayoutChunk(rows, resolve(config.output, "_build", "docs"), chunks.length));
  }
  if (rows.length) chunks.push(writeLayoutChunk(rows, resolve(config.output, "_build", "docs"), chunks.length));

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
  const packWriter = createPackWriter(resolve(out, "docs", "packs"), config.docPackBytes, { keepEntries: false, dedupe: false });
  const entryPath = resolve(out, "_build", "docs", "doc-pack-entries.bin");
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
  const fields = docPayloadFieldNames(config);
  const packWriter = createPackWriter(resolve(out, "docs", "page-packs"), config.docPagePackBytes || config.docPackBytes);
  const entries = [];
  const fd = openSync(spool.path, "r");
  const spoolEntryFd = openSync(spool.entryPath, "r");
  try {
    for (let pageStart = 0, pageIndex = 0; pageStart < total; pageStart += pageSize, pageIndex++) {
      const pageEnd = Math.min(total, pageStart + pageSize);
      const docs = [];
      for (let index = pageStart; index < pageEnd; index++) {
        const entry = readDocSpoolEntry(spoolEntryFd, index);
        docs.push(JSON.parse(gunzipSync(readSpooledDoc(fd, entry)).toString("utf8")));
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
  const pointerTable = buildDocPagePointerTable(entries, packIndexes);
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
  const packWriter = createPackWriter(resolve(out, "doc-values", "packs"), config.docValuePackBytes);
  const fields = {};
  for (const field of docValueFields(config, codes)) {
    const chunks = [];
    for (let start = 0; start < total; start += chunkSize) {
      const rows = codeRows(codes, field.name, start, Math.min(total, start + chunkSize));
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
  const sourceFields = docValueFields(config, codes).filter(field => field.kind !== "facet");
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
  const codes = createCodeStore(resolve(dirs.out, "_build", "codes"), config, measured.total, measured.dicts);

  const initialResults = [];
  const buffer = { byShard: new Map(), lines: 0, runsOut: dirs.runsOut };
  const queryBundleSeedBuffer = createQueryBundleSeedBuffer(config);
  const authorityBuffer = createAuthorityRunBuffer(config, dirs.authorityRunsOut);
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
      addQueryBundleSeeds(queryBundleSeedBuffer, selectedTerms, config, doc);
      addAuthorityDoc(authorityBuffer, config, doc, index);

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
      addTypoSurfacePairs(typoBuffer, surfacePairsForFields(doc, config.fields, fieldText));

      const payload = docPayload(doc, config, index);
      if (initialResults.length < config.initialResultLimit) initialResults.push(payload);
      writeSpooledDoc(docSpool, payload, index, docLayoutRecord(index, selectedTerms, config));
    });
  } finally {
    closeDocSpool(docSpool);
  }
  flushPostingBuffer(buffer);
  const authorityBaseShards = finishAuthorityRuns(authorityBuffer);
  const queryBundleSeeds = finalizeQueryBundleSeeds(queryBundleSeedBuffer, config);
  return {
    codes,
    initialResults,
    baseShards: [...baseShards].sort(),
    docSpool,
    queryBundleSeeds,
    queryBundleTerms: queryBundleTerms(queryBundleSeeds),
    authorityBaseShards
  };
}

async function reduceShard(baseShard, config, measured, runData, filters, packWriter, blockPackWriter, typoBuffer, bundleTermSet) {
  const path = resolve(runData.runsOut, `${baseShard}.run`);
  const blockStats = emptyPostingBlockStats();
  const bundleDfs = [];
  const stats = await reduceRunToPartitions({
    runPath: path,
    scratchDir: resolve(runData.reduceSortOut, encodeURIComponent(baseShard)),
    config,
    onTerm: (term, df) => {
      addTypoIndexTerm(typoBuffer, term, df, measured.total);
      if (bundleTermSet?.has(term)) bundleDfs.push([term, df]);
    },
    onPartition: (partition) => {
      const encoded = buildTermShard(partition.entries, measured.total, runData.codes, filters, config);
      const externalized = externalizeTermShard(encoded, config, filters, blockPackWriter);
      addPostingBlockStats(blockStats, externalized.stats);
      writePackedShard(packWriter, partition.name, gzipSync(externalized.buffer, { level: 6 }), {
        kind: "term-shard",
        codec: "rfterm-v1",
        logicalLength: externalized.buffer.length
      });
      return partition.name;
    }
  });
  unlinkSync(path);
  return { terms: stats.terms, postings: stats.postings, shards: stats.partitions, blockStats, bundleDfs };
}

function createReduceWorker(workerData) {
  const heapMb = Math.max(0, Math.floor(Number(workerData.config?.reduceWorkerHeapMb || 0)));
  const worker = new Worker(new URL("./reduce_worker.js", import.meta.url), {
    workerData,
    ...(heapMb > 0 ? { resourceLimits: { maxOldGenerationSizeMb: heapMb } } : {})
  });
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
    appendFileRange(resolve(typoBuffer.runsOut, file), resolve(workerTypo.runsOut, file));
  }
}

function appendFileRange(targetPath, sourcePath) {
  const sourceFd = openSync(sourcePath, "r");
  const targetFd = openSync(targetPath, "a");
  const buffer = Buffer.alloc(1024 * 1024);
  let offset = 0;
  const size = statSync(sourcePath).size;
  try {
    while (offset < size) {
      const bytesRead = readSync(sourceFd, buffer, 0, Math.min(buffer.length, size - offset), offset);
      if (!bytesRead) break;
      writeSync(targetFd, buffer, 0, bytesRead);
      offset += bytesRead;
    }
  } finally {
    closeSync(sourceFd);
    closeSync(targetFd);
  }
}

function workerCodeTables(codes) {
  if (codes && typeof codes.descriptor === "function") return codes.descriptor();
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
    sortOut: resolve(dirs.out, "_build", "reduce-sort", `worker-${index}`),
    bundleTerms: runData.queryBundleTerms || [],
    typoOptions: typoBuffer?.options || { enabled: false },
    typoRunsOut: resolve(dirs.out, "_build", "typo-index-runs", `worker-${index}`)
  }));

  const taskResults = [];
  const largeRunBytes = Math.max(0, Math.floor(Number(config.reduceLargeRunBytes || 0)));
  const tasks = runData.baseShards.map((baseShard, order) => {
    const path = resolve(dirs.runsOut, `${baseShard}.run`);
    const bytes = statSync(path).size;
    return { order, baseShard, bytes };
  });
  const smallTasks = tasks.filter(task => task.bytes < largeRunBytes);
  const largeTasks = tasks.filter(task => task.bytes >= largeRunBytes).sort((a, b) => b.bytes - a.bytes || a.order - b.order);
  let nextSmallTask = 0;
  let termCount = 0;
  let postingCount = 0;
  const bundleDfs = new Map();
  async function runTask(worker, task) {
    const stats = await worker.call({ type: "reduce", baseShard: task.baseShard });
    termCount += stats.terms;
    postingCount += stats.postings;
    for (const [term, df] of stats.bundleDfs || []) bundleDfs.set(term, df);
    taskResults.push({ order: task.order, ...stats });
  }
  try {
    await Promise.all(workers.map(async (worker) => {
      while (nextSmallTask < smallTasks.length) {
        const task = smallTasks[nextSmallTask++];
        await runTask(worker, task);
      }
    }));
    for (const task of largeTasks) await runTask(workers[0], task);
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
  return { finalShards, packWriter, blockPackWriter, blockStats, termCount, postingCount, bundleDfs };
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
    const bundleDfs = new Map();
    let termCount = 0;
    let postingCount = 0;
    const bundleTermSet = new Set(runData.queryBundleTerms || []);
    for (let i = 0; i < runData.baseShards.length; i++) {
      const stats = await reduceShard(runData.baseShards[i], config, measured, { ...runData, runsOut: dirs.runsOut, reduceSortOut: resolve(dirs.out, "_build", "reduce-sort") }, filters, packWriter, blockPackWriter, typoBuffer, bundleTermSet);
      termCount += stats.terms;
      postingCount += stats.postings;
      for (const [term, df] of stats.bundleDfs || []) bundleDfs.set(term, df);
      addPostingBlockStats(blockStats, stats.blockStats);
      for (const shard of stats.shards) finalShards.add(shard);
    }
    if (blockPackWriter) finalizePackWriter(blockPackWriter);
    finalizePackWriter(packWriter);
    reduced = { finalShards, packWriter, blockPackWriter, blockStats, termCount, postingCount, bundleDfs };
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
    bundleDfs: reduced.bundleDfs || new Map(),
    packBytes: reduced.packWriter.bytes,
    blockPackBytes: reduced.blockPackWriter?.bytes || 0,
    reduceWorkers: workerCount
  };
}

function impactForBundleScore(score, df, total) {
  const scoreInt = Math.max(1, Math.round(score * 1000));
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

function emitQueryBundleRows(collector, seedIndex, termDfs, total, selectedTerms, doc) {
  const selected = new Map(selectedTerms);
  const seenKeys = new Set();

  function emitSeed(seed) {
    if (!seed || seenKeys.has(seed.key) || !seed.baseTerms.every(base => selected.has(base))) return;
    seenKeys.add(seed.key);
    let score = 0;
    for (const scoringTerm of seed.expandedTerms) {
      if (!selected.has(scoringTerm)) continue;
      const df = termDfs.get(scoringTerm);
      if (!df) continue;
      score += impactForBundleScore(selected.get(scoringTerm), df, total);
    }
    addQueryBundleRow(collector, seed.key, doc, score);
  }

  for (const [term] of selectedTerms) {
    if (term.includes("_")) continue;
    for (const seed of seedIndex.get(term) || []) emitSeed(seed);
  }
}

async function buildQueryBundleIndex(config, measured, dirs, seeds, termDfs) {
  if (!queryBundlesEnabled(config) || !seeds?.length || !termDfs?.size) return null;
  const seedIndex = queryBundleSeedIndex(seeds, termDfs);
  const collector = createQueryBundleCollector(seeds, config.queryBundleMaxRows);

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
    emitQueryBundleRows(collector, seedIndex, termDfs, measured.total, selectedTerms, index);
  });
  const bundles = queryBundleCollectorResults(collector);
  if (!bundles.length) return null;
  return writeQueryBundleObjects({
    outDir: dirs.out,
    config,
    bundles,
    coverage: "all-base-docs"
  });
}

export async function build({ configPath }) {
  const config = await readConfig(configPath);
  const dirs = {
    out: config.output,
    runsOut: resolve(config.output, "_build", "runs"),
    typoRunsOut: resolve(config.output, "_build", "typo-runs"),
    authorityRunsOut: resolve(config.output, "_build", "authority-runs")
  };
  rmSync(dirs.out, { recursive: true, force: true });
  mkdirSync(resolve(dirs.out, "docs"), { recursive: true });
  mkdirSync(resolve(dirs.out, "terms"), { recursive: true });
  mkdirSync(dirs.runsOut, { recursive: true });
  mkdirSync(dirs.authorityRunsOut, { recursive: true });

  console.log(`Rangefind: reading ${config.input}`);
  const measured = await measure(config);
  const typo = typoOptions(config);
  const typoBuffer = typo.enabled ? createTypoRunBuffer(dirs.typoRunsOut, typo) : null;
  const runData = await writePostingRuns(config, measured, dirs, typoBuffer);
  const reduced = await reduceRuns(config, measured, runData, dirs, typoBuffer);
  const queryBundles = await buildQueryBundleIndex(config, measured, dirs, runData.queryBundleSeeds, reduced.bundleDfs);
  const authority = await reduceAuthorityRuns(config, dirs, runData.authorityBaseShards);
  const docs = await finishDocPacks(dirs.out, runData.docSpool, measured.total, config);
  docs.pages = finishDocPages(dirs.out, runData.docSpool, measured.total, config);
  const typoManifest = await reduceTypoRuns(typoBuffer, dirs.out);
  const docValues = writeDocValuePacks(dirs.out, config, measured.total, runData.codes);
  const docValueSorted = writeDocValueSortedIndexes(dirs.out, config, measured.total, runData.codes);
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
      docValueSorted: true,
      facetDictionaries: true,
      externalPostingBlocks: config.externalPostingBlocks !== false,
      queryBundles: !!queryBundles,
      authority: !!authority,
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
        facets: packTable(facetDictionaries.pack_objects),
        queryBundles: packTable(queryBundles?.packs),
        authority: packTable(authority?.packs),
        typo: packTable(typoManifest?.packs)
      },
      dedupe: summarizeDedup(
        reduced.packs,
        reduced.blockPacks,
        docs.packs,
        docs.pages.packs,
        docValues.packs,
        docValueSorted.packs,
        facetDictionaries.pack_objects,
        queryBundles?.packs || [],
        authority?.packs || [],
        typoManifest?.packs || []
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
      doc_value_sorted_storage: docValueSorted.storage,
      doc_value_sorted_directory_format: docValueSorted.directory_format,
      doc_value_sorted_page_format: docValueSorted.page_format,
      doc_value_sorted_page_size: docValueSorted.page_size,
      doc_value_sorted_fields: Object.keys(docValueSorted.fields).length,
      doc_value_sorted_directory_bytes: docValueSorted.directory_bytes,
      doc_value_sorted_directory_logical_bytes: docValueSorted.directory_logical_bytes,
      doc_value_sorted_pack_files: docValueSorted.packs.length,
      doc_value_sorted_pack_bytes: docValueSorted.pack_bytes,
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
      query_bundle_directory_bytes: queryBundles?.directory_bytes || 0,
      query_bundle_pack_files: queryBundles?.packs.length || 0,
      query_bundle_pack_bytes: queryBundles?.pack_bytes || 0,
      authority_format: authority?.format || "",
      authority_fields: authority?.fields.length || 0,
      authority_keys: authority?.keys || 0,
      authority_rows: authority?.rows || 0,
      authority_shards: authority?.shards || 0,
      authority_target_shard_rows: authority?.target_shard_rows || 0,
      authority_directory_bytes: authority?.directory_bytes || 0,
      authority_pack_files: authority?.packs.length || 0,
      authority_pack_bytes: authority?.pack_bytes || 0,
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
  runData.codes.close?.();
  rmSync(resolve(dirs.out, "_build"), { recursive: true, force: true });
  console.log(`Rangefind: built ${measured.total.toLocaleString()} docs, ${reduced.shards.length.toLocaleString()} logical shards, ${reduced.packs.length.toLocaleString()} packs`);
}
