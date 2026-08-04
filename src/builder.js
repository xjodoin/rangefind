import {
  copyFileSync,
  createWriteStream,
  createReadStream,
  existsSync,
  mkdirSync,
  openSync,
  readdirSync,
  rmSync,
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
import { createGunzip, createGzip, gunzipSync, gzipSync } from "node:zlib";
import { createInterface } from "node:readline";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { performance } from "node:perf_hooks";
import { Worker } from "node:worker_threads";
import { expandedTermsFromBaseTerms, queryBundleKeyFromBaseTerms } from "./terms.js";
import { analyzerForConfig } from "./analysis.js";
import { addAuthorityDoc, addAuthorityRecord, createAuthorityRunBuffer, finishAuthorityRuns, reduceAuthorityRuns } from "./authority_index.js";
import {
  booleanValue,
  docPayload,
  docPayloadFieldNames,
  encodeSelectedTerms,
  facetValues,
  isBundlePhraseTerm,
  normalizedNumberType,
  numericValue,
  queryBundlesEnabled,
  rawPath,
  valueList,
  vectorsEnabled
} from "./scan_doc.js";
import {
  applyPermutation,
  encodeVectorClusterPage,
  encodeVectorRoot,
  nearestCentroid,
  normalizeVector,
  quantizeVector,
  trainCentroids,
  trainDimensionPermutation,
  VECTOR_CLUSTER_PAGE_FORMAT,
  VECTOR_ROOT_FORMAT,
  vectorFromValue
} from "./vector_index.js";
import { addBuildCounter, createBuildTelemetry, finishBuildTelemetry, recordBuildWorkers, timeBuildPhase } from "./build_telemetry.js";
import { createCodeStore, openCodeStore, preloadCodeStoreDescriptor } from "./build_store.js";
import {
  buildBlockFilters,
  buildDocValueChunk,
  buildFacetDictionary,
  buildPostingSegmentChunks,
  docValueFields,
  parsePostingSegment,
  POSTING_SEGMENT_FORMAT,
  summarizeBlockFilters
} from "./codec.js";
import { geoComponentFieldNames, getPath, readConfig } from "./config.js";
import { closeScoringDfReaders, installScoringDfProvider, loadScoringStats } from "./scoring_stats.js";
import {
  buildGeoTreeLeaves,
  encodeGeoBranchPage,
  encodeGeoLeafPage,
  encodeGeoTreeRoot,
  GEO_BRANCH_PAGE_FORMAT,
  GEO_LEAF_CAPSULE_PAGE_FORMAT,
  GEO_LEAF_PAGE_FORMAT,
  GEO_TREE_ROOT_FORMAT,
  latToE7,
  lonToE7,
  mergeBlockFilterSummaries
} from "./geo_tree.js";
import {
  encodeGeoCellBlock,
  geoCellBlock,
  geoCellBlockKey,
  geoCellForE7,
  GEO_CATEGORY_CELL_FORMAT
} from "./geo_cells.js";
import {
  appendGeoCellRoute,
  closeGeoCellRouteSpool,
  createGeoCellRouteSpool,
  sortedGeoCellRoutes
} from "./geo_cell_spool.js";
import { DOC_LAYOUT_FORMAT, docLayoutRecord } from "./doc_layout.js";
import { buildDocPagePointerTable, DOC_PAGE_ENCODING, DOC_PAGE_FORMAT, decodeDocPageColumns, encodeDocPageColumns } from "./doc_pages.js";
import { parseDirectoryPage, parseDirectoryRoot } from "./directory.js";
import { writeDirectoryFiles, writeDirectoryFilesFromSortedEntries } from "./directory_writer.js";
import { appendDirectoryEntry, createDirectoryEntrySpool, sortedDirectoryEntrySpool } from "./directory_spool.js";
import {
  DOC_VALUE_SORT_DIRECTORY_FORMAT,
  DOC_VALUE_SORT_PAGE_FORMAT,
  encodeDocValueSortDirectory,
  encodeDocValueSortPage
} from "./doc_value_tree.js";
import { createFilterBitmap, encodeFilterBitmap, FILTER_BITMAP_FORMAT, setFilterBitmapBit } from "./filter_bitmaps.js";
import { buildDocPointerTableFromReader } from "./doc_pointers.js";
import { createJsonlReadStream, eachJsonLine } from "./jsonl.js";
import { createFieldRowPipeline } from "./field_rows.js";
import { OBJECT_CHECKSUM_ALGORITHM, OBJECT_NAME_HASH_LENGTH, OBJECT_POINTER_FORMAT, OBJECT_STORE_FORMAT } from "./object_store.js";
import { buildIndexOptimizerReport, INDEX_OPTIMIZER_PATH } from "./optimizer.js";
import { createAppendOnlyPackWriter, createPackWriter, finalizePackWriter, resolvePackEntry, writePackedShard, writePackedShardChunks } from "./packs.js";
import { partitionInputBytes, partitionTermEntries } from "./reduced_terms.js";
import { addQueryBundleRow, createQueryBundleCollector, queryBundleCollectorResults, writeQueryBundleObjects } from "./query_bundles.js";
import { tryReadVarint, varintLength, writeVarint } from "./runs.js";
import { analyzeDocumentForIndex, analyzeFieldText, fieldIndexText } from "./scoring.js";
import { addSegmentPosting, createSegmentBuilder, finishSegmentBuilder, flushSegment, shouldFlushSegment } from "./segment_builder.js";
import { mergeSegmentsToPartitions, segmentMergeSummary } from "./segment_merge.js";
import { writeSegmentManifest } from "./segment_manifest.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const SORT_REPLICA_FORMAT = "rfsortreplicas-v1";
const SORT_REPLICA_RANK_MAP_FORMAT = "rfsortrankmap-v1";
const SORT_REPLICA_RANK_RECORD_BYTES = 12;

// Main-thread reduce and final pack assembly resolve frozen shard df
// through the same provider the reduce workers install.
installScoringDfProvider();

function addDict(dict, value, label = value) {
  const key = String(value || "");
  if (!key) return 0;
  if (dict.ids.has(key)) return dict.ids.get(key);
  const id = dict.values.length;
  dict.ids.set(key, id);
  dict.values.push({ value: key, label: String(label || key), n: 0 });
  return id;
}

function createMeasureAccumulator(config) {
  return {
    fieldTotals: Object.fromEntries(config.fields.map(field => [field.name, 0])),
    dicts: Object.fromEntries(config.facets.map(facet => [facet.name, { ids: new Map(), values: [{ value: "", label: "", n: 0 }] }])),
    total: 0
  };
}

function mergeMeasureSummary(accumulator, config, summary) {
  accumulator.total += summary.total || 0;
  for (const field of config.fields) {
    accumulator.fieldTotals[field.name] += summary.fieldTotals?.[field.name] || 0;
  }
  for (const facet of config.facets) {
    const dict = accumulator.dicts[facet.name];
    for (const item of summary.facets?.[facet.name] || []) {
      const code = addDict(dict, item.value, item.label);
      dict.values[code].n += item.n || 0;
    }
  }
}

function finalizeMeasure(accumulator, config, workerStats) {
  return {
    total: accumulator.total,
    fieldTotals: { ...accumulator.fieldTotals },
    avgLens: Object.fromEntries(config.fields.map(field => [
      field.name,
      Math.max(1, accumulator.fieldTotals[field.name] / Math.max(1, accumulator.total))
    ])),
    dicts: accumulator.dicts,
    workerStats
  };
}

async function measureSequential(config) {
  const started = performance.now();
  const accumulator = createMeasureAccumulator(config);
  const analyzer = analyzerForConfig(config);
  await eachJsonLine(config.input, async (doc) => {
    accumulator.total++;
    const docLang = analyzer.docLanguage(doc, config);
    for (const field of config.fields) {
      accumulator.fieldTotals[field.name] += analyzer.tokenize(fieldIndexText(doc, field, config), { unique: false, lang: docLang }).length;
    }
    for (const facet of config.facets) {
      for (const item of facetValues(doc, facet)) {
        const dict = accumulator.dicts[facet.name];
        const code = addDict(dict, item.value, item.label);
        dict.values[code].n++;
      }
    }
  });
  return finalizeMeasure(accumulator, config, [{
    worker: 0,
    docs: accumulator.total,
    batches: accumulator.total ? 1 : 0,
    analysisMs: performance.now() - started,
    mode: "main-thread"
  }]);
}

function measureBatchDocs(config) {
  const fallback = Math.max(512, scanBatchDocs(config) * 4);
  return Math.max(1, Math.floor(Number(config.measureBatchDocs || fallback)));
}

function postMeasureBatch(worker, message) {
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
      if (code !== 0) rejectBatch(new Error(`Rangefind measure worker exited with code ${code}.`));
    }
    worker.on("message", onMessage);
    worker.once("error", onError);
    worker.once("exit", onExit);
    worker.postMessage(message);
  });
}

async function measureWithWorkers(config) {
  const workerCount = scanWorkerCount(config);
  const batchDocs = measureBatchDocs(config);
  const accumulator = createMeasureAccumulator(config);
  const workers = Array.from({ length: workerCount }, (_, index) => ({
    index,
    worker: new Worker(new URL("./measure_worker.js", import.meta.url), { type: "module" }),
    docs: 0,
    batches: 0,
    inputBytes: 0,
    analysisMs: 0
  }));
  const available = workers.slice();
  const active = new Set();
  const pending = new Map();
  let nextBatch = 0;
  let nextMerge = 0;

  function drainPending() {
    while (pending.has(nextMerge)) {
      const response = pending.get(nextMerge);
      pending.delete(nextMerge);
      mergeMeasureSummary(accumulator, config, response);
      nextMerge++;
    }
  }

  async function waitForWorker() {
    while (!available.length) await Promise.race(active);
    return available.pop();
  }

  async function queueBatch(lines) {
    const entry = await waitForWorker();
    const id = nextBatch++;
    const started = performance.now();
    const promise = postMeasureBatch(entry.worker, { id, lines, config })
      .then((response) => {
        entry.docs += response.total || 0;
        entry.batches++;
        entry.inputBytes += response.inputBytes || 0;
        entry.analysisMs += performance.now() - started;
        pending.set(id, response);
        drainPending();
      })
      .finally(() => {
        active.delete(promise);
        available.push(entry);
      });
    active.add(promise);
  }

  try {
    const rl = createInterface({ input: createJsonlReadStream(config.input), crlfDelay: Infinity });
    let batch = [];
    for await (const line of rl) {
      if (!line.trim()) continue;
      batch.push(line);
      if (batch.length >= batchDocs) {
        await queueBatch(batch);
        batch = [];
      }
    }
    if (batch.length) await queueBatch(batch);
    while (active.size) await Promise.race(active);
    drainPending();
    if (pending.size) throw new Error("Rangefind measure workers finished out of order.");
  } finally {
    await Promise.allSettled(workers.map(entry => entry.worker.terminate()));
  }

  return finalizeMeasure(accumulator, config, workers.map(entry => ({
    worker: entry.index,
    tasks: entry.batches,
    docs: entry.docs,
    batches: entry.batches,
    inputBytes: entry.inputBytes,
    analysisMs: entry.analysisMs,
    mode: "worker-thread"
  })));
}

async function measure(config) {
  return scanWorkerCount(config) > 1 ? measureWithWorkers(config) : measureSequential(config);
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

function addQueryBundleSeeds(buffer, selectedTerms, config, doc, fieldTerms = null) {
  if (!buffer.enabled) return;
  const selected = new Set(selectedTerms.map(([term]) => term));
  const docKeys = new Set();
  const maxTerms = Math.max(2, Math.min(3, Math.floor(Number(config.queryBundleMaxTerms || 3))));
  for (const [term] of selectedTerms) {
    if (!isBundlePhraseTerm(term, config)) continue;
    addQueryBundleSeed(buffer, term.split("_"), selected, docKeys);
  }
  for (let fieldIndex = 0; fieldIndex < config.fields.length; fieldIndex++) {
    const field = config.fields[fieldIndex];
    const limit = Math.max(0, Math.floor(Number(field.queryBundleSeedMaxTokens ?? config.queryBundleSeedMaxFieldTokens ?? 512)));
    if (!limit || field.queryBundles === false) continue;
    const terms = fieldTerms?.[fieldIndex] || analyzeFieldText(doc, field, config).terms.slice(0, limit);
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

function createDocSpool(outDir, config = {}) {
  mkdirSync(outDir, { recursive: true });
  const path = resolve(outDir, "payloads.bin");
  const entryPath = resolve(outDir, "payloads.idx");
  const layoutPath = resolve(outDir, "layout.jsonl");
  const pagePath = resolve(outDir, "pages.bin");
  const pageEntryPath = resolve(outDir, "pages.idx");
  return {
    path,
    entryPath,
    layoutPath,
    fd: openSync(path, "w"),
    entryFd: openSync(entryPath, "w"),
    layoutFd: openSync(layoutPath, "w"),
    offset: 0,
    bytes: 0,
    layoutDocs: 0,
    pagePath,
    pageEntryPath,
    pageFd: openSync(pagePath, "w"),
    pageEntryFd: openSync(pageEntryPath, "w"),
    pageSize: Math.max(1, Math.floor(Number(config.docPageSize || 32))),
    pageFields: docPayloadFieldNames(config),
    pageBuffer: [],
    pageOffset: 0,
    pageBytes: 0,
    pageDocs: 0,
    pageCount: 0,
    writeQueues: new Map()
  };
}

const DOC_SPOOL_FLUSH_BYTES = 4 * 1024 * 1024;

function queueSpoolWrite(spool, key, fd, chunk) {
  let queue = spool.writeQueues.get(key);
  if (!queue) {
    queue = { fd, pending: [], pendingBytes: 0 };
    spool.writeQueues.set(key, queue);
  }
  queue.pending.push(chunk);
  queue.pendingBytes += chunk.length;
  if (queue.pendingBytes >= DOC_SPOOL_FLUSH_BYTES) flushSpoolWrites(spool, key);
}

function flushSpoolWrites(spool, key = null) {
  if (!spool.writeQueues) return;
  for (const [queueKey, queue] of spool.writeQueues) {
    if (key != null && queueKey !== key) continue;
    if (!queue.pending.length) continue;
    const chunk = Buffer.concat(queue.pending, queue.pendingBytes);
    writeSync(queue.fd, chunk, 0, chunk.length);
    queue.pending.length = 0;
    queue.pendingBytes = 0;
  }
}

function flushDocPageSpool(spool) {
  if (!spool?.pageBuffer?.length || spool.pageFd == null || spool.pageEntryFd == null) return;
  const source = encodeDocPageColumns(spool.pageBuffer, spool.pageFields || []);
  queueSpoolWrite(spool, "page", spool.pageFd, Buffer.from(source.buffer, source.byteOffset, source.length));
  queueSpoolWrite(spool, "pageEntry", spool.pageEntryFd, encodeDocSpoolEntry({
    offset: spool.pageOffset,
    length: source.length,
    logicalLength: source.length
  }));
  spool.pageOffset += source.length;
  spool.pageBytes += source.length;
  spool.pageDocs += spool.pageBuffer.length;
  spool.pageCount++;
  spool.pageBuffer.length = 0;
}

function closeDocSpool(spool) {
  flushDocPageSpool(spool);
  flushSpoolWrites(spool);
  for (const key of ["fd", "entryFd", "layoutFd", "pageFd", "pageEntryFd"]) {
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

function encodeDocSpoolEntry(entry) {
  const buffer = Buffer.allocUnsafe(DOC_SPOOL_ENTRY_BYTES);
  writeBigUInt(buffer, 0, entry.offset);
  writeBigUInt(buffer, 8, entry.length);
  writeBigUInt(buffer, 16, entry.logicalLength);
  return buffer;
}

function writeDocSpoolEntry(spoolOrFd, index, entry) {
  const fd = typeof spoolOrFd === "number" ? spoolOrFd : spoolOrFd.entryFd;
  const buffer = encodeDocSpoolEntry(entry);
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

function writeSpooledDoc(spool, payload, index, layoutRecord, precompressed = null) {
  const bytes = precompressed ? null : Buffer.from(JSON.stringify(payload));
  const compressed = precompressed?.compressed || gzipSync(bytes, { level: 6 });
  const logicalLength = precompressed?.logicalLength ?? bytes.length;
  queueSpoolWrite(spool, "payload", spool.fd, compressed);
  queueSpoolWrite(spool, "entry", spool.entryFd, encodeDocSpoolEntry({
    offset: spool.offset,
    length: compressed.length,
    logicalLength
  }));
  queueSpoolWrite(spool, "layout", spool.layoutFd, Buffer.from(`${JSON.stringify(layoutRecord)}\n`));
  spool.layoutDocs++;
  spool.offset += compressed.length;
  spool.bytes += compressed.length;

  const expectedIndex = spool.pageDocs + spool.pageBuffer.length;
  if (index !== expectedIndex) throw new Error(`Rangefind doc page spool expected document ${expectedIndex} but received ${index}.`);
  spool.pageBuffer.push(payload);
  if (spool.pageBuffer.length >= spool.pageSize) flushDocPageSpool(spool);
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
    pending: [],
    pendingBytes: 0,
    docs: 0,
    terms: 0,
    bytes: 0
  };
}

function flushSelectedTermSpool(spool) {
  if (!spool?.pending?.length) return;
  const chunk = Buffer.concat(spool.pending, spool.pendingBytes);
  writeSync(spool.fd, chunk, 0, chunk.length);
  spool.pending.length = 0;
  spool.pendingBytes = 0;
}

function closeSelectedTermSpool(spool) {
  if (!spool || spool.fd == null) return;
  flushSelectedTermSpool(spool);
  closeSync(spool.fd);
  spool.fd = null;
}

function writeSelectedTerms(spool, selectedTerms, encoded = null) {
  const bytes = encoded || encodeSelectedTerms(selectedTerms);
  spool.pending.push(bytes);
  spool.pendingBytes += bytes.length;
  if (spool.pendingBytes >= 1024 * 1024) flushSelectedTermSpool(spool);
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

function layoutHeapPush(heap, readerIndex, readers) {
  heap.push(readerIndex);
  let child = heap.length - 1;
  while (child > 0) {
    const parent = Math.floor((child - 1) / 2);
    if (compareLayoutRecords(readers[heap[parent]].row, readers[heap[child]].row) <= 0) break;
    [heap[parent], heap[child]] = [heap[child], heap[parent]];
    child = parent;
  }
}

function layoutHeapPop(heap, readers) {
  const first = heap[0];
  const last = heap.pop();
  if (heap.length) {
    heap[0] = last;
    let parent = 0;
    while (true) {
      const left = parent * 2 + 1;
      const right = left + 1;
      if (left >= heap.length) break;
      let child = left;
      if (right < heap.length && compareLayoutRecords(readers[heap[right]].row, readers[heap[left]].row) < 0) child = right;
      if (compareLayoutRecords(readers[heap[parent]].row, readers[heap[child]].row) <= 0) break;
      [heap[parent], heap[child]] = [heap[child], heap[parent]];
      parent = child;
    }
  }
  return first;
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
  // A binary heap makes the external merge O(totalDocs * log(chunkCount)).
  // The previous linear reader scan became quadratic as full-corpus builds
  // produced more sort chunks. Uint32Array also avoids millions of boxed JS
  // numbers while retaining direct random layout access below.
  const heap = [];
  for (let i = 0; i < readers.length; i++) if (readers[i].row) layoutHeapPush(heap, i, readers);
  const order = new Uint32Array(total);
  let orderLength = 0;
  const stats = { docsWithoutTerms: 0, primaryTerms: 0 };
  let lastPrimary = null;
  while (heap.length) {
    const best = layoutHeapPop(heap, readers);
    const row = readers[best].row;
    order[orderLength++] = row.index;
    if (row.primary) {
      if (row.primary !== lastPrimary) stats.primaryTerms++;
      lastPrimary = row.primary;
    } else {
      stats.docsWithoutTerms++;
    }
    readers[best].row = await nextLayoutRow(readers[best]);
    if (readers[best].row) layoutHeapPush(heap, best, readers);
  }
  for (const reader of readers) {
    reader.rl.close();
    reader.input.destroy();
  }
  if (orderLength !== total) throw new Error(`Rangefind doc layout expected ${total} docs but sorted ${orderLength}.`);
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

function readPackedDocEntryBatch(fd, start, count, packFiles, includeChecksum) {
  const buffer = Buffer.allocUnsafe(count * PACKED_DOC_ENTRY_BYTES);
  let filled = 0;
  const fileOffset = start * PACKED_DOC_ENTRY_BYTES;
  while (filled < buffer.length) {
    const bytesRead = readSync(fd, buffer, filled, buffer.length - filled, fileOffset + filled);
    if (!bytesRead) throw new Error(`Rangefind packed doc entries ended at document ${start + Math.floor(filled / PACKED_DOC_ENTRY_BYTES)}.`);
    filled += bytesRead;
  }
  const entries = new Array(count);
  for (let row = 0; row < count; row++) {
    const offset = row * PACKED_DOC_ENTRY_BYTES;
    const packIndex = buffer.readUInt32LE(offset);
    entries[row] = {
      pack: packFiles[packIndex],
      offset: readBigUInt(buffer, offset + 4),
      length: readBigUInt(buffer, offset + 12),
      logicalLength: readBigUInt(buffer, offset + 20),
      ...(includeChecksum ? { checksum: { algorithm: "sha256", value: buffer.subarray(offset + 28, offset + 60) } } : {})
    };
  }
  return entries;
}

function docPageWorkerCount(config) {
  const explicit = Math.max(0, Math.floor(Number(config.docPageWorkers || 0)));
  return Math.max(1, explicit || Math.floor(Number(config.builderWorkerCount || 1)));
}

async function createDocPageWorkerPool(config, options = {}) {
  const count = Math.max(1, Math.floor(Number(options.count ?? docPageWorkerCount(config))));
  if (count <= 1) return null;
  const workers = Array.from({ length: count }, () => new Worker(new URL("./doc_page_worker.js", import.meta.url), { type: "module" }));
  await Promise.all(workers.map((worker, index) => postScanBatch(worker, {
    type: "init",
    id: `init-${index}`,
    payloadPath: options.payloadPath || null,
    payloadEntryPath: options.payloadEntryPath || null,
    fields: options.fields || [],
    gzipLevel: options.gzipLevel ?? 6
  })));
  const available = workers.slice();
  const waiters = [];
  let nextId = 0;

  function acquire() {
    if (available.length) return Promise.resolve(available.pop());
    return new Promise(resolveWaiter => waiters.push(resolveWaiter));
  }

  function release(worker) {
    const waiter = waiters.shift();
    if (waiter) waiter(worker);
    else available.push(worker);
  }

  return {
    count,
    async run(message) {
      const worker = await acquire();
      try {
        return await postScanBatch(worker, { ...message, id: nextId++ });
      } finally {
        release(worker);
      }
    },
    async close() {
      await Promise.allSettled(workers.map((worker, index) => postScanBatch(worker, { type: "close", id: `close-${index}` })));
      await Promise.allSettled(workers.map(worker => worker.terminate()));
    }
  };
}

async function mapWorkerBatchesOrdered(pool, batches, handle) {
  const inFlight = [];
  const limit = pool.count + 1;
  for (const batch of batches) {
    inFlight.push(pool.run(batch));
    while (inFlight.length >= limit) await handle(await inFlight.shift());
  }
  while (inFlight.length) await handle(await inFlight.shift());
}

function preloadFileChunks(path, bytes, chunkBytes) {
  const fd = openSync(path, "r");
  const chunks = [];
  try {
    for (let offset = 0; offset < bytes;) {
      const length = Math.min(chunkBytes, bytes - offset);
      const chunk = Buffer.allocUnsafe(length);
      let filled = 0;
      while (filled < length) {
        const count = readSync(fd, chunk, filled, length - filled, offset + filled);
        if (!count) throw new Error(`Rangefind preload ended early at ${offset + filled} of ${bytes} bytes.`);
        filled += count;
      }
      chunks.push(chunk);
      offset += length;
    }
  } finally {
    closeSync(fd);
  }
  return { chunks, chunkBytes, bytes };
}

function preloadedFileSlice(file, offset, length) {
  if (offset < 0 || length < 0 || offset + length > file.bytes) {
    throw new Error(`Rangefind preload slice ${offset}+${length} exceeds ${file.bytes} bytes.`);
  }
  if (!length) return Buffer.alloc(0);
  const first = Math.floor(offset / file.chunkBytes);
  const firstOffset = offset % file.chunkBytes;
  if (firstOffset + length <= file.chunks[first].length) {
    return file.chunks[first].subarray(firstOffset, firstOffset + length);
  }
  const out = Buffer.allocUnsafe(length);
  let copied = 0;
  let chunkIndex = first;
  let chunkOffset = firstOffset;
  while (copied < length) {
    const chunk = file.chunks[chunkIndex++];
    const count = Math.min(length - copied, chunk.length - chunkOffset);
    chunk.copy(out, copied, chunkOffset, chunkOffset + count);
    copied += count;
    chunkOffset = 0;
  }
  return out;
}

function preloadedBigUInt(file, offset) {
  return readBigUInt(preloadedFileSlice(file, offset, 8), 0);
}

function createReadWindow(path, bytes, windowBytes) {
  return {
    fd: openSync(path, "r"),
    bytes,
    windowBytes,
    start: 0,
    end: 0,
    buffer: Buffer.alloc(0)
  };
}

function readWindowSlice(reader, offset, length) {
  if (offset < 0 || length < 0 || offset + length > reader.bytes) {
    throw new Error(`Rangefind read window ${offset}+${length} exceeds ${reader.bytes} bytes.`);
  }
  if (offset < reader.start || offset + length > reader.end) {
    const bytes = Math.min(reader.bytes - offset, Math.max(reader.windowBytes, length));
    reader.buffer = Buffer.allocUnsafe(bytes);
    let filled = 0;
    while (filled < bytes) {
      const count = readSync(reader.fd, reader.buffer, filled, bytes - filled, offset + filled);
      if (!count) throw new Error(`Rangefind sequential read ended early at ${offset + filled} of ${reader.bytes} bytes.`);
      filled += count;
    }
    reader.start = offset;
    reader.end = offset + bytes;
  }
  const start = offset - reader.start;
  return reader.buffer.subarray(start, start + length);
}

function closeReadWindow(reader) {
  if (reader?.fd != null) closeSync(reader.fd);
}

function docIdLayout(total) {
  return {
    order: null,
    summary: {
      format: DOC_LAYOUT_FORMAT,
      strategy: "doc-id",
      terms: 0,
      shard_depth: 0,
      docs: total,
      docs_without_terms: 0,
      primary_terms: 0
    }
  };
}

async function finishDocPacks(out, spool, total, config) {
  const sequential = config.docLayoutStrategy === "doc-id";
  const layout = sequential ? docIdLayout(total) : await sortedLayoutOrder(spool, total, config);
  const packWriter = createAppendOnlyPackWriter(resolve(out, "docs", "packs"), config.docPackBytes);
  const entryPath = buildPath(config, "docs", "doc-pack-entries.bin");
  const entryOutFd = openSync(entryPath, "w");
  const preloadLimit = Math.max(0, Math.floor(Number(config.docPackSpoolPreloadMaxBytes ?? 256 * 1024 * 1024)));
  const preloadChunkBytes = Math.max(64, Math.floor(Number(config.docPackSpoolPreloadChunkBytes ?? 256 * 1024 * 1024)));
  const preload = !sequential && spool.bytes > 0 && spool.bytes <= preloadLimit;
  // Node cannot create a single Buffer larger than 2 GiB. Chunk both files so
  // the preload fast path remains available to multi-gigabyte corpora while
  // preserving zero-copy slices for the common within-chunk document.
  const payloadFile = preload ? preloadFileChunks(spool.path, spool.bytes, preloadChunkBytes) : null;
  const entryFile = preload ? preloadFileChunks(spool.entryPath, total * DOC_SPOOL_ENTRY_BYTES, preloadChunkBytes) : null;
  const readWindowBytes = Math.max(64 * 1024, Math.floor(Number(config.docPackSequentialReadBytes ?? 64 * 1024 * 1024)));
  const payloadWindow = sequential ? createReadWindow(spool.path, spool.bytes, readWindowBytes) : null;
  const entryWindow = sequential ? createReadWindow(spool.entryPath, total * DOC_SPOOL_ENTRY_BYTES, readWindowBytes) : null;
  const fd = preload || sequential ? null : openSync(spool.path, "r");
  const spoolEntryFd = preload || sequential ? null : openSync(spool.entryPath, "r");
  try {
    for (let rank = 0; rank < total; rank++) {
      const index = layout.order ? layout.order[rank] : rank;
      const entryOffset = index * DOC_SPOOL_ENTRY_BYTES;
      const entry = preload
        ? {
            offset: preloadedBigUInt(entryFile, entryOffset),
            length: preloadedBigUInt(entryFile, entryOffset + 8),
            logicalLength: preloadedBigUInt(entryFile, entryOffset + 16)
          }
        : sequential
          ? {
              offset: readBigUInt(readWindowSlice(entryWindow, entryOffset, DOC_SPOOL_ENTRY_BYTES), 0),
              length: readBigUInt(readWindowSlice(entryWindow, entryOffset, DOC_SPOOL_ENTRY_BYTES), 8),
              logicalLength: readBigUInt(readWindowSlice(entryWindow, entryOffset, DOC_SPOOL_ENTRY_BYTES), 16)
            }
        : readDocSpoolEntry(spoolEntryFd, index);
      const compressed = preload
        ? preloadedFileSlice(payloadFile, entry.offset, entry.length)
        : sequential
          ? readWindowSlice(payloadWindow, entry.offset, entry.length)
        : readSpooledDoc(fd, entry);
      const packed = writePackedShard(packWriter, docIndexKey(index), compressed, {
        kind: "doc",
        codec: "json-v1",
        logicalLength: entry.logicalLength
      });
      writePackedDocEntry(entryOutFd, index, packed);
    }
  } finally {
    if (fd != null) closeSync(fd);
    if (spoolEntryFd != null) closeSync(spoolEntryFd);
    closeReadWindow(payloadWindow);
    closeReadWindow(entryWindow);
    closeSync(entryOutFd);
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const packFiles = packTable(packWriter.packs);
  const entryInFd = openSync(entryPath, "r");
  let pointerTable;
  try {
    pointerTable = buildDocPointerTableFromReader(
      total,
      packIndexes,
      doc => readPackedDocEntry(entryInFd, doc, packFiles),
      {
        batchSize: Math.max(1, Math.floor(Number(config.docPointerReadBatchDocs || 65536))),
        readBatch: (start, count, pass) => readPackedDocEntryBatch(entryInFd, start, count, packFiles, pass === "write")
      }
    );
  } finally {
    closeSync(entryInFd);
  }
  const hash = sha256Hex(pointerTable.buffer);
  const file = `docs/pointers/${hashedFile("0000", hash, ".bin.gz")}`;
  mkdirSync(resolve(out, "docs", "pointers"), { recursive: true });
  writeFileSync(resolve(out, file), pointerTable.buffer);
  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    layout: {
      ...layout.summary,
      spool_bytes: spool.bytes,
      spool_preloaded: preload,
      spool_preload_chunks: payloadFile?.chunks.length || 0,
      sequential_read_bytes: sequential ? readWindowBytes : 0
    },
    pointers: {
      ...pointerTable.meta,
      file,
      order: "doc-id",
      content_hash: hash,
      immutable: true,
      bytes: pointerTable.buffer.length,
      pack_table: packTable(packWriter.packs)
    },
    packs: packWriter.packs
  };
}

async function finishDocPages(out, spool, total, config) {
  const pageSize = Math.max(1, Math.floor(Number(config.docPageSize || 32)));
  const fields = docPayloadFieldNames(config);
  const packWriter = createAppendOnlyPackWriter(resolve(out, "docs", "page-packs"), config.docPagePackBytes);
  const entries = [];
  const fd = openSync(spool.pagePath, "r");
  const spoolEntryFd = openSync(spool.pageEntryPath, "r");
  const pool = await createDocPageWorkerPool(config, { gzipLevel: 6 });
  const pageCount = Math.ceil(total / pageSize);
  try {
    if (pool) {
      const batchPages = 256;
      function* sourceBatches() {
        for (let pageIndex = 0; pageIndex < pageCount; pageIndex += batchPages) {
          const items = [];
          for (let page = pageIndex; page < Math.min(pageCount, pageIndex + batchPages); page++) {
            const entry = readDocSpoolEntry(spoolEntryFd, page);
            items.push({ key: page, bytes: readSpooledDoc(fd, entry) });
          }
          yield { kind: "gzip", items };
        }
      }
      await mapWorkerBatchesOrdered(pool, sourceBatches(), (response) => {
        for (const item of response.items) {
          entries[item.key] = writePackedShard(packWriter, docPageKey(item.key), toBatchBuffer(item.compressed), {
            kind: "doc-page",
            codec: DOC_PAGE_FORMAT,
            logicalLength: item.logicalLength
          });
        }
      });
    } else {
      for (let pageIndex = 0; pageIndex < pageCount; pageIndex++) {
        const entry = readDocSpoolEntry(spoolEntryFd, pageIndex);
        const source = readSpooledDoc(fd, entry);
        entries[pageIndex] = writePackedShard(packWriter, docPageKey(pageIndex), gzipSync(source, { level: 6 }), {
          kind: "doc-page",
          codec: DOC_PAGE_FORMAT,
          logicalLength: entry.logicalLength
        });
      }
    }
  } finally {
    closeSync(fd);
    closeSync(spoolEntryFd);
    await pool?.close();
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const pointerTable = buildDocPagePointerTable(entries.map(entry => resolvePackEntry(packWriter, entry)), packIndexes);
  const hash = sha256Hex(pointerTable.buffer);
  const file = `docs/pages/${hashedFile("0000", hash, ".bin.gz")}`;
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

function openDocPageSpoolReader(spool, config, cacheLimit = 1024) {
  const fd = openSync(spool.pagePath, "r");
  const entryFd = openSync(spool.pageEntryPath, "r");
  const pageSize = Math.max(1, Math.floor(Number(spool.pageSize || config.docPageSize || 32)));
  const fields = spool.pageFields || docPayloadFieldNames(config);
  const maxCachePages = Math.max(1, Math.floor(Number(cacheLimit || 1024)));
  const cache = new Map();

  function readPage(pageIndex) {
    if (cache.has(pageIndex)) {
      const docs = cache.get(pageIndex);
      cache.delete(pageIndex);
      cache.set(pageIndex, docs);
      return docs;
    }
    const entry = readDocSpoolEntry(entryFd, pageIndex);
    const docs = decodeDocPageColumns(readSpooledDoc(fd, entry), fields, pageIndex * pageSize);
    cache.set(pageIndex, docs);
    while (cache.size > maxCachePages) cache.delete(cache.keys().next().value);
    return docs;
  }

  return {
    doc(index) {
      const pageIndex = Math.floor(index / pageSize);
      const page = readPage(pageIndex);
      const doc = page[index - pageIndex * pageSize];
      if (!doc) throw new Error(`Rangefind doc page spool is missing document ${index}.`);
      return doc;
    },
    docs(indexes) {
      // Resolve a whole consumer batch page-by-page. Calling doc() in an
      // arbitrary spatial order can evict a page before another row in the
      // same leaf reaches it; this form opens every unique page at most once
      // per leaf and lets the bounded LRU retain overlap with adjacent leaves.
      const pages = new Map();
      for (const index of indexes) {
        const pageIndex = Math.floor(index / pageSize);
        if (!pages.has(pageIndex)) pages.set(pageIndex, readPage(pageIndex));
      }
      return indexes.map(index => {
        const pageIndex = Math.floor(index / pageSize);
        const doc = pages.get(pageIndex)?.[index - pageIndex * pageSize];
        if (!doc) throw new Error(`Rangefind doc page spool is missing document ${index}.`);
        return doc;
      });
    },
    close() {
      closeSync(fd);
      closeSync(entryFd);
    }
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
  const maxBitmapBytes = Math.max(0, Number(config.filterBitmapMaxBytes ?? 256 * 1024 * 1024));
  const bytesPerBitmap = Math.ceil(Math.max(0, total) / 8);
  const maxBitmaps = bytesPerBitmap > 0 ? Math.floor(maxBitmapBytes / bytesPerBitmap) : 0;

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
      const dict = dicts?.[facet.name];
      const values = dict?.values || [];
      const valueCount = values.length;
      if (!valueCount || !maxBitmaps) continue;
      const explicit = Array.isArray(config.filterBitmapFacetValues?.[facet.name])
        ? config.filterBitmapFacetValues[facet.name]
        : [];
      const explicitCodes = explicit
        .map(value => dict.ids?.get(String(value)))
        .filter(code => Number.isInteger(code) && code >= 0 && code < valueCount);
      const explicitCodeSet = new Set(explicitCodes);
      const candidates = valueCount <= maxFacetValues
        ? values.map((value, code) => ({ code, count: Number(value?.n || 0), explicit: explicitCodeSet.has(code) }))
        : explicitCodes.map(code => ({ code, count: Number(values[code]?.n || 0), explicit: true }));
      candidates.sort((a, b) => (
        Number(b.explicit) - Number(a.explicit)
        || b.count - a.count
        || a.code - b.code
      ));
      const selectedCodes = [...new Set(candidates.slice(0, maxBitmaps).map(item => item.code))];
      if (!selectedCodes.length) continue;
      const selected = new Map(selectedCodes.map(code => [code, {
        bytes: createFilterBitmap(total),
        count: 0
      }]));
      for (let doc = 0; doc < total; doc++) {
        for (const code of facetCodesForBitmap(codeValue(codes, facet.name, doc))) {
          const bitmap = selected.get(code);
          if (!bitmap) continue;
          setFilterBitmapBit(bitmap.bytes, doc);
          bitmap.count++;
        }
      }
      const bitmapValues = {};
      for (const [code, bitmap] of selected) {
        const entry = writeBitmap(facet.name, String(code), bitmap.bytes, bitmap.count);
        if (entry) bitmapValues[String(code)] = entry;
      }
      if (Object.keys(bitmapValues).length) {
        fields[facet.name] = { name: facet.name, kind: "facet", values: bitmapValues };
      }
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
    max_bitmap_bytes: maxBitmapBytes,
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

function geoComponentNames(config) {
  const names = new Set();
  for (const geoField of config.geo || []) {
    const components = geoComponentFieldNames(geoField);
    names.add(components.lat);
    names.add(components.lon);
  }
  return names;
}

function writeDocValueSortedIndexes(out, config, total, codes) {
  const pageSize = Math.max(1, Math.floor(Number(config.docValueSortedPageSize || 512)));
  const packWriter = createPackWriter(resolve(out, "doc-values", "sorted-packs"), config.docValueSortedPackBytes || config.docValuePackBytes);
  const fields = {};
  const geoComponents = geoComponentNames(config);
  // Geo coordinate components stay out of the sorted trees (their spatial
  // index lives in the geo tree) but remain page summary fields for pruning.
  const nonFacetFields = (codes.fields || docValueFields(config, codes)).filter(field => field.kind !== "facet");
  const sourceFields = nonFacetFields.filter(field => !geoComponents.has(field.name));
  const summaryFields = nonFacetFields.map(field => ({ name: field.name, kind: field.kind, type: field.type }));
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

function geoEntriesBbox(entries) {
  if (!entries.length) return { minLatE7: 0, maxLatE7: 0, minLonE7: 0, maxLonE7: 0 };
  let minLatE7 = entries[0].minLatE7;
  let maxLatE7 = entries[0].maxLatE7;
  let minLonE7 = entries[0].minLonE7;
  let maxLonE7 = entries[0].maxLonE7;
  for (let index = 1; index < entries.length; index++) {
    const entry = entries[index];
    if (entry.minLatE7 < minLatE7) minLatE7 = entry.minLatE7;
    if (entry.maxLatE7 > maxLatE7) maxLatE7 = entry.maxLatE7;
    if (entry.minLonE7 < minLonE7) minLonE7 = entry.minLonE7;
    if (entry.maxLonE7 > maxLonE7) maxLonE7 = entry.maxLonE7;
  }
  return { minLatE7, maxLatE7, minLonE7, maxLonE7 };
}

function prepareGeoCellIndexes(config, dicts) {
  return (config.geoCellIndexes || []).map((item, id) => {
    const dict = dicts?.[item.facet];
    const explicitCodes = item.values
      .map(value => dict?.ids?.get(String(value)))
      .filter(code => Number.isInteger(code) && code > 0);
    const rankedCodes = item.values.length
      ? explicitCodes
      : (dict?.values || [])
        .map((value, code) => ({ code, count: Number(value?.n || 0) }))
        .filter(itemValue => itemValue.code > 0 && itemValue.count > 0)
        .sort((a, b) => b.count - a.count || a.code - b.code)
        .slice(0, item.maxFacetValues)
        .map(itemValue => itemValue.code);
    const selectedCodes = [...new Set(rankedCodes)].sort((a, b) => a - b);
    return {
      ...item,
      id,
      codes: selectedCodes,
      codeSet: new Set(selectedCodes),
      includeAll: item.includeAll === true,
      records: 0
    };
  }).filter(item => item.codes.length || item.includeAll);
}

function appendGeoCellLeafRoutes(spool, indexes, points, leaf, codes) {
  for (const index of indexes) {
    const tuples = new Map();
    for (let position = leaf.start; position < leaf.end; position++) {
      const doc = points.docs[position];
      const selected = facetCodesForBitmap(codeValue(codes, index.facet, doc))
        .filter(code => index.codeSet.has(code));
      if (index.includeAll) selected.unshift(0);
      if (!selected.length) continue;
      for (const level of index.levels) {
        const cell = geoCellForE7(points.latsE7[position], points.lonsE7[position], level);
        const block = geoCellBlock(cell, index.blockZoom);
        for (const code of selected) {
          const group = Math.floor(code / index.codeGroupSize);
          const tuple = [
            index.id,
            group,
            block.y,
            block.x,
            level,
            cell.y,
            cell.x,
            code,
            leaf.index,
            position - leaf.start
          ];
          tuples.set(tuple.join(":"), tuple);
        }
      }
    }
    for (const tuple of tuples.values()) {
      appendGeoCellRoute(spool, tuple);
      index.records++;
    }
  }
}

async function writeGeoCellRoutes(out, config, indexes, spool) {
  if (!spool?.records || !indexes.length) {
    if (spool?.path) rmSync(spool.path, { force: true });
    return null;
  }
  const packWriter = createPackWriter(
    resolve(out, "geo", "category-cells", "packs"),
    config.geoCellPackBytes || config.geoPackBytes
  );
  const directorySpool = createDirectoryEntrySpool(resolve(out, "_build", "geo-cell-directory.bin"));
  const byId = new Map(indexes.map(index => [index.id, index]));
  let blockKey = "";
  let routeKey = "";
  let currentBlock = null;
  let currentRoute = null;
  let currentMember = null;
  let blocks = 0;
  let routes = 0;

  function finishRoute() {
    if (!currentRoute) return;
    if (currentMember) currentRoute.members.push(currentMember);
    currentBlock.routes.push(currentRoute);
    currentRoute = null;
    currentMember = null;
    routes++;
  }

  function finishBlock() {
    if (!currentBlock) return;
    finishRoute();
    const index = byId.get(currentBlock.indexId);
    const key = geoCellBlockKey(index.field, index.facet, currentBlock.group, {
      zoom: index.blockZoom,
      x: currentBlock.blockX,
      y: currentBlock.blockY
    });
    const encoded = encodeGeoCellBlock({
      blockZoom: index.blockZoom,
      blockX: currentBlock.blockX,
      blockY: currentBlock.blockY,
      routes: currentBlock.routes
    });
    const entry = writePackedShard(packWriter, key, gzipSync(encoded, { level: 6 }), {
      kind: "geo-category-cell",
      codec: GEO_CATEGORY_CELL_FORMAT,
      logicalLength: encoded.length
    });
    appendDirectoryEntry(directorySpool, key, entry);
    blocks++;
    currentBlock = null;
  }

  try {
    for await (const tuple of sortedGeoCellRoutes(spool, {
      chunkRecords: config.geoCellSortChunkRecords
    })) {
      const [indexId, group, blockY, blockX, zoom, cellY, cellX, code, leaf, ordinal] = tuple;
      const nextBlockKey = `${indexId}:${group}:${blockY}:${blockX}`;
      if (nextBlockKey !== blockKey) {
        finishBlock();
        blockKey = nextBlockKey;
        routeKey = "";
        currentBlock = { indexId, group, blockY, blockX, routes: [] };
      }
      const nextRouteKey = `${zoom}:${cellY}:${cellX}:${code}`;
      if (nextRouteKey !== routeKey) {
        finishRoute();
        routeKey = nextRouteKey;
        currentRoute = { zoom, y: cellY, x: cellX, code, members: [] };
      }
      if (!currentMember || currentMember.leaf !== leaf) {
        if (currentMember) currentRoute.members.push(currentMember);
        currentMember = { leaf, ordinals: [] };
      }
      if (currentMember.ordinals.at(-1) !== ordinal) currentMember.ordinals.push(ordinal);
    }
    finishBlock();
    finalizePackWriter(packWriter);
    const packFiles = packTable(packWriter.packs);
    const packIndexes = new Map(packFiles.map((file, index) => [file, index]));
    const directoryEntries = sortedDirectoryEntrySpool(directorySpool, {
      chunkEntries: config.directorySortChunkEntries,
      packNameMap: packWriter.packNameMap,
      packIndexes
    });
    const directory = await writeDirectoryFilesFromSortedEntries(
      resolve(out, "geo", "category-cells"),
      directoryEntries,
      directorySpool.entries,
      config.geoCellDirectoryPageBytes,
      "geo/category-cells",
      { packTable: packFiles }
    );
    return {
      format: GEO_CATEGORY_CELL_FORMAT,
      directory,
      pack_dir: "geo/category-cells/packs",
      packs: packWriter.packs,
      pack_table: packFiles,
      blocks,
      routes,
      records: spool.records,
      indexes: indexes.filter(index => index.records).map(index => ({
        field: index.field,
        facet: index.facet,
        levels: index.levels,
        block_zoom: index.blockZoom,
        code_group_size: index.codeGroupSize,
        max_cells_per_query: index.maxCellsPerQuery,
        codes: index.codes,
        ...(index.includeAll ? { all_code: 0 } : {}),
        records: index.records
      }))
    };
  } finally {
    rmSync(spool.path, { force: true });
    rmSync(directorySpool.path, { force: true });
  }
}

async function writeGeoTrees(out, config, total, codes, blockFilters, docSpool, dicts) {
  if (!config.geo?.length) return null;
  const summaryFilters = Array.isArray(blockFilters) && blockFilters.length ? blockFilters : null;
  const leafSize = Math.max(16, Math.floor(Number(config.geoLeafSize || 512)));
  const capsuleFields = config.geoCapsules ? (config.geoCapsuleFields || []) : [];
  const capsuleReader = capsuleFields.length
    ? openDocPageSpoolReader(docSpool, config, config.geoCapsuleDocPageCachePages)
    : null;
  const packWriter = createPackWriter(resolve(out, "geo", "point-packs"), config.geoPackBytes || config.docValuePackBytes);
  const readChunkSize = Math.max(1, Math.floor(Number(config.docValueChunkSize || 2048)));
  const fields = {};
  const rootsByField = new Map();
  const geoCellIndexes = prepareGeoCellIndexes(config, dicts);
  const geoCellIndexesByField = new Map();
  for (const index of geoCellIndexes) {
    const list = geoCellIndexesByField.get(index.field) || [];
    list.push(index);
    geoCellIndexesByField.set(index.field, list);
  }
  const geoCellSpool = geoCellIndexes.length
    ? createGeoCellRouteSpool(resolve(out, "_build", "geo-cell-routes.bin"))
    : null;

  try {
    for (const geoField of config.geo) {
      const components = geoComponentFieldNames(geoField);
      const latsE7 = new Int32Array(total);
      const lonsE7 = new Int32Array(total);
      const docs = new Uint32Array(total);
      let count = 0;
      for (let start = 0; start < total; start += readChunkSize) {
        const end = Math.min(total, start + readChunkSize);
        const latRows = codeRows(codes, components.lat, start, end);
        const lonRows = codeRows(codes, components.lon, start, end);
        for (let row = 0; row < latRows.length; row++) {
          const latE7 = latToE7(latRows[row]);
          const lonE7 = lonToE7(lonRows[row]);
          if (latE7 == null || lonE7 == null) continue;
          latsE7[count] = latE7;
          lonsE7[count] = lonE7;
          docs[count] = start + row;
          count += 1;
        }
      }
      const points = {
        latsE7: latsE7.subarray(0, count),
        lonsE7: lonsE7.subarray(0, count),
        docs: docs.subarray(0, count)
      };
      const leaves = buildGeoTreeLeaves(points.latsE7, points.lonsE7, points.docs, leafSize);
      for (let leafIndex = 0; leafIndex < leaves.length; leafIndex++) {
        const leaf = leaves[leafIndex];
        leaf.index = leafIndex;
        const capsules = capsuleReader
          ? capsuleReader.docs(Array.from(points.docs.subarray(leaf.start, leaf.end)))
          : null;
        const encoded = encodeGeoLeafPage(
          geoField.name,
          points.latsE7,
          points.lonsE7,
          points.docs,
          leaf,
          capsules ? { capsules, capsuleFields } : {}
        );
        leaf.entry = writePackedShard(packWriter, `${geoField.name}\u0000${leafIndex}`, gzipSync(encoded, { level: 6 }), {
          kind: "geo-leaf-page",
          codec: capsules ? GEO_LEAF_CAPSULE_PAGE_FORMAT : GEO_LEAF_PAGE_FORMAT,
          logicalLength: encoded.length
        });
        if (summaryFilters) {
          leaf.summary = summarizeBlockFilters(summaryFilters, codes, points.docs.subarray(leaf.start, leaf.end));
        }
        const fieldCellIndexes = geoCellIndexesByField.get(geoField.name);
        if (geoCellSpool && fieldCellIndexes?.length) {
          appendGeoCellLeafRoutes(geoCellSpool, fieldCellIndexes, points, leaf, codes);
        }
      }
      const bbox = geoEntriesBbox(leaves);
      rootsByField.set(geoField.name, { geoField, total: count, bbox, leaves });
    }
  } finally {
    capsuleReader?.close();
    if (geoCellSpool) closeGeoCellRouteSpool(geoCellSpool);
  }

  // Large trees page their leaf tables into lazily fetched branch pages so
  // the root a browser downloads cold stays small at any point count. Branch
  // pages reference leaf objects by pack INDEX, which stays stable when
  // finalizePackWriter renames packs to their content-addressed names.
  const branchLeaves = Math.max(2, Math.floor(Number(config.geoBranchLeaves || 256)));
  const provisionalPackIndexes = () => new Map(packTable(packWriter.packs).map((file, index) => [file, index]));
  for (const state of rootsByField.values()) {
    if (state.leaves.length <= branchLeaves * 2) continue;
    const packIndexesNow = provisionalPackIndexes();
    state.branches = [];
    for (let start = 0; start < state.leaves.length; start += branchLeaves) {
      const slice = state.leaves.slice(start, start + branchLeaves);
      const bbox = geoEntriesBbox(slice);
      const branch = {
        ...bbox,
        count: slice.reduce((sum, leaf) => sum + leaf.count, 0),
        firstLeafIndex: start,
        leafCount: slice.length
      };
      if (summaryFilters) {
        branch.summary = mergeBlockFilterSummaries(summaryFilters, slice.map(leaf => leaf.summary));
      }
      const encoded = encodeGeoBranchPage({
        field: state.geoField.name,
        branchIndex: state.branches.length,
        firstLeafIndex: start,
        bbox: branch,
        leaves: slice,
        packIndexes: packIndexesNow,
        blockFilters: summaryFilters
      });
      branch.entry = writePackedShard(packWriter, `${state.geoField.name}\u0000branch\u0000${start}`, gzipSync(encoded, { level: 6 }), {
        kind: "geo-branch-page",
        codec: GEO_BRANCH_PAGE_FORMAT,
        logicalLength: encoded.length
      });
      state.branches.push(branch);
    }
  }

  finalizePackWriter(packWriter);
  const geoCells = await writeGeoCellRoutes(out, config, geoCellIndexes, geoCellSpool);
  const packFiles = packTable(packWriter.packs);
  const packIndexes = new Map(packFiles.map((file, index) => [file, index]));
  let directoryBytes = 0;
  mkdirSync(resolve(out, "geo"), { recursive: true });

  for (const { geoField, total: fieldTotal, bbox, leaves, branches } of rootsByField.values()) {
    const root = encodeGeoTreeRoot({
      field: geoField.name,
      total: fieldTotal,
      leafSize,
      leafCount: leaves.length,
      bbox,
      leaves: branches ? null : leaves,
      branches: branches || null,
      packTable: packFiles,
      packIndexes,
      blockFilters: summaryFilters
    });
    const compressed = gzipSync(root.buffer, { level: 6 });
    const hash = sha256Hex(compressed);
    const file = `geo/${hashedFile(safeObjectName(geoField.name), hash, ".bin.gz")}`;
    writeFileSync(resolve(out, file), compressed);
    directoryBytes += compressed.length;
    fields[geoField.name] = {
      name: geoField.name,
      lat_component: geoComponentFieldNames(geoField).lat,
      lon_component: geoComponentFieldNames(geoField).lon,
      total: fieldTotal,
      leaf_size: leafSize,
      leaves: leaves.length,
      levels: branches ? 2 : 1,
      branches: branches ? branches.length : 0,
      ...(capsuleFields.length ? {
        capsules: {
          format: "rfgeocapsule-v1",
          page_format: GEO_LEAF_CAPSULE_PAGE_FORMAT,
          fields: capsuleFields,
          rows: fieldTotal
        }
      } : {}),
      ...(geoCells?.indexes?.some(index => index.field === geoField.name) ? {
        category_cells: geoCells.indexes
          .filter(index => index.field === geoField.name)
          .map(index => ({
            ...index,
            format: geoCells.format,
            directory: geoCells.directory,
            pack_dir: geoCells.pack_dir
          }))
      } : {}),
      bbox,
      directory: {
        format: GEO_TREE_ROOT_FORMAT,
        compression: "gzip-member",
        file,
        content_hash: hash,
        immutable: true,
        bytes: compressed.length,
        logical_bytes: root.buffer.length
      }
    };
  }

  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    directory_format: GEO_TREE_ROOT_FORMAT,
    page_format: capsuleFields.length ? GEO_LEAF_CAPSULE_PAGE_FORMAT : GEO_LEAF_PAGE_FORMAT,
    capsule_fields: capsuleFields,
    leaf_size: leafSize,
    fields,
    packs: packWriter.packs,
    pack_table: packFiles,
    cell_packs: geoCells?.packs || [],
    cell_pack_table: geoCells?.pack_table || [],
    category_cells: geoCells,
    directory_bytes: directoryBytes,
    pack_bytes: packWriter.packs.reduce((sum, pack) => sum + pack.bytes, 0)
  };
}

function forEachSpooledVector(spool, dims, handler) {
  const fd = openSync(spool.file, "r");
  const rowBytes = dims * 4;
  const batchRows = 4096;
  const buffer = Buffer.alloc(batchRows * rowBytes);
  try {
    let row = 0;
    while (row < spool.rows) {
      const rows = Math.min(batchRows, spool.rows - row);
      const bytes = rows * rowBytes;
      if (readSync(fd, buffer, 0, bytes, row * rowBytes) !== bytes) {
        throw new Error("Rangefind vector spool ended early.");
      }
      const block = new Float32Array(buffer.buffer, buffer.byteOffset, rows * dims);
      for (let i = 0; i < rows; i++) {
        if (!Number.isNaN(block[i * dims])) handler(row + i, block, i * dims);
      }
      row += rows;
    }
  } finally {
    closeSync(fd);
  }
}

function vectorCoarseDims(config, dims) {
  const configured = Math.floor(Number(config.vectorCoarseDims || 0));
  if (configured > 0) return Math.min(dims, Math.max(8, configured));
  return Math.min(dims, Math.max(Math.min(32, dims), Math.round(dims / 4)));
}

async function writeVectorIndexes(out, config, vectorSpools) {
  if (!vectorsEnabled(config) || !vectorSpools?.length) return null;
  const fields = {};
  let packBytesTotal = 0;
  let directoryBytes = 0;

  for (const spoolInfo of vectorSpools) {
    const field = config.vectors.find(item => item.name === spoolInfo.name);
    if (!field) continue;
    const dims = field.dims;
    const coarseDims = vectorCoarseDims(config, dims);
    const refineRowBytes = 4 + dims;
    const refineRowsPerPack = Math.max(1, Math.floor((config.vectorPackBytes || 4 * 1024 * 1024) / refineRowBytes));
    const fieldDir = safeObjectName(field.name);
    const refineWriter = createAppendOnlyPackWriter(
      resolve(out, "vectors", fieldDir, "refine-packs"),
      refineRowsPerPack * refineRowBytes + 1
    );
    const trainSample = Math.max(1000, Math.floor(Number(config.vectorTrainSample || 20000)));
    const sampleStride = Math.max(1, Math.floor(spoolInfo.rows / trainSample));

    // Pass 1 (sample): collect a strided training sample, learn the
    // variance-descending dimension permutation (so the coarse prefix used
    // for candidate ranking carries the most informative components), and
    // train coarse centroids in permuted space.
    const sampleRows = [];
    let sampled = 0;
    forEachSpooledVector(spoolInfo, dims, (row, block, offset) => {
      if (sampled % sampleStride === 0 && sampleRows.length < trainSample) {
        sampleRows.push(Float32Array.from(block.subarray(offset, offset + dims)));
      }
      sampled += 1;
    });
    const rawSample = new Float32Array(sampleRows.length * dims);
    for (let i = 0; i < sampleRows.length; i++) rawSample.set(sampleRows[i], i * dims);
    const permutation = trainDimensionPermutation(rawSample, dims);
    const sample = new Float32Array(rawSample.length);
    const permuteScratch = new Float32Array(dims);
    for (let i = 0; i < sampleRows.length; i++) {
      sample.set(applyPermutation(rawSample, i * dims, permutation, permuteScratch), i * dims);
    }
    const clusterTarget = Math.max(64, Math.floor(Number(config.vectorClusterTargetDocs || 512)));
    const clusterGoal = Math.max(1, Math.min(4096, Math.ceil(sampled / clusterTarget)));
    const { centroids, clusterCount } = trainCentroids(sample, dims, clusterGoal, {
      coarseDims,
      iterations: Math.max(2, Math.floor(Number(config.vectorKmeansIterations || 6)))
    });

    // Pass 2: permute each vector, write its refine row (full-dim int8 +
    // scale, addressed by ordinal), assign it to a centroid, and build the
    // cluster rows (doc id, ordinal, coarse-prefix scale, int8 codes).
    const buckets = Array.from({ length: clusterCount }, () => ({ docs: [], ordinals: [], scales: [], codes: [] }));
    let vectorCount = 0;
    let chunk = Buffer.alloc(refineRowsPerPack * refineRowBytes);
    let chunkRows = 0;
    let chunkIndex = 0;
    const flushChunk = () => {
      if (!chunkRows) return;
      writePackedShard(refineWriter, `refine ${chunkIndex++}`, chunk.subarray(0, chunkRows * refineRowBytes), {
        kind: "vector-refine-rows",
        compression: "none",
        logicalLength: chunkRows * refineRowBytes
      });
      chunkRows = 0;
    };
    const codesScratch = new Int8Array(dims);
    forEachSpooledVector(spoolInfo, dims, (row, block, offset) => {
      const permuted = applyPermutation(block, offset, permutation, permuteScratch);
      const scale = quantizeVector(permuted, codesScratch, 0);
      const at = chunkRows * refineRowBytes;
      chunk.writeFloatLE(scale, at);
      chunk.set(Buffer.from(codesScratch.buffer, 0, dims), at + 4);
      chunkRows += 1;
      if (chunkRows === refineRowsPerPack) flushChunk();

      const cluster = nearestCentroid(permuted, 0, centroids, clusterCount, dims, coarseDims);
      const bucket = buckets[cluster];
      const coarse = new Int8Array(coarseDims);
      const coarseScale = quantizeVector(permuted.subarray(0, coarseDims), coarse, 0);
      bucket.docs.push(row);
      bucket.ordinals.push(vectorCount);
      bucket.scales.push(coarseScale);
      bucket.codes.push(coarse);
      vectorCount += 1;
    });
    flushChunk();
    finalizePackWriter(refineWriter);

    const packWriter = createPackWriter(resolve(out, "vectors", fieldDir, "packs"), config.vectorPackBytes || 4 * 1024 * 1024);
    const clusters = new Array(clusterCount);
    for (let c = 0; c < clusterCount; c++) {
      const bucket = buckets[c];
      const codes = new Int8Array(bucket.docs.length * coarseDims);
      for (let i = 0; i < bucket.codes.length; i++) codes.set(bucket.codes[i], i * coarseDims);
      const encoded = encodeVectorClusterPage({
        field: field.name,
        clusterIndex: c,
        coarseDims,
        docs: bucket.docs,
        ordinals: bucket.ordinals,
        scales: bucket.scales,
        codes
      });
      const entry = writePackedShard(packWriter, `${field.name} cluster ${c}`, gzipSync(encoded, { level: 6 }), {
        kind: "vector-cluster-page",
        codec: VECTOR_CLUSTER_PAGE_FORMAT,
        logicalLength: encoded.length
      });
      clusters[c] = { count: bucket.docs.length, entry };
    }
    finalizePackWriter(packWriter);
    const packFiles = packTable(packWriter.packs);
    const packIndexes = new Map(packFiles.map((file, index) => [file, index]));
    const root = encodeVectorRoot({
      field: field.name,
      dims,
      coarseDims,
      metric: field.metric,
      total: vectorCount,
      permutation,
      centroids,
      clusterCount,
      clusters,
      refine: {
        rowBytes: refineRowBytes,
        rowsPerPack: refineRowsPerPack,
        packs: packTable(refineWriter.packs)
      },
      packTable: packFiles,
      packIndexes
    });
    const compressed = gzipSync(root.buffer, { level: 6 });
    const hash = sha256Hex(compressed);
    mkdirSync(resolve(out, "vectors"), { recursive: true });
    const file = `vectors/${hashedFile(fieldDir, hash, ".bin.gz")}`;
    writeFileSync(resolve(out, file), compressed);
    directoryBytes += compressed.length;
    packBytesTotal += packWriter.packs.reduce((sum, pack) => sum + pack.bytes, 0)
      + refineWriter.packs.reduce((sum, pack) => sum + pack.bytes, 0);

    fields[field.name] = {
      name: field.name,
      metric: field.metric,
      dims,
      coarse_dims: coarseDims,
      total: vectorCount,
      clusters: clusterCount,
      pack_dir: `vectors/${fieldDir}/packs`,
      refine_pack_dir: `vectors/${fieldDir}/refine-packs`,
      directory: {
        format: VECTOR_ROOT_FORMAT,
        compression: "gzip-member",
        file,
        content_hash: hash,
        immutable: true,
        bytes: compressed.length,
        logical_bytes: root.buffer.length
      }
    };
  }

  return {
    storage: "range-pack-v1",
    directory_format: VECTOR_ROOT_FORMAT,
    page_format: VECTOR_CLUSTER_PAGE_FORMAT,
    fields,
    directory_bytes: directoryBytes,
    pack_bytes: packBytesTotal
  };
}

// External-id → local doc index map, written with every build so future
// `--update` runs can tombstone replaced documents. Loaded only by the
// builder, never by the runtime.
async function writeIdMap(out, config, total, docSpool) {
  const reader = openDocPageSpoolReader(docSpool, config, 64);
  async function* chunks() {
    const lines = [];
    try {
      for (let index = 0; index < total; index++) {
        lines.push(JSON.stringify([String(reader.doc(index)?.id ?? index), index]));
        if (lines.length >= 20000) {
          yield `${lines.join("\n")}\n`;
          lines.length = 0;
        }
      }
      if (lines.length) yield `${lines.join("\n")}\n`;
    } finally {
      reader.close();
    }
  }
  const file = "docs/id-map.jsonl.gz";
  const path = resolve(out, file);
  const partial = `${path}.partial`;
  rmSync(partial, { force: true });
  await pipeline(Readable.from(chunks()), createGzip({ level: 6 }), createWriteStream(partial));
  renameSync(partial, path);
  return { file, docs: total };
}

async function* readIdMapEntries(path) {
  const source = createReadStream(path).pipe(createGunzip());
  const lines = createInterface({ input: source, crlfDelay: Infinity });
  try {
    for await (const line of lines) {
      if (line) yield JSON.parse(line);
    }
  } finally {
    lines.close();
    source.destroy();
  }
}

// Streams a previous generation's published term shards from local disk and
// accumulates per-term document frequencies, so delta builds bake impact
// scores with corpus-wide idf. Works against any existing index — no extra
// sidecar format is required.
function collectGenerationDf(genDir, genManifest, dfBase) {
  const directory = genManifest.directory;
  if (!directory?.root) throw new Error(`Rangefind update: no term directory descriptor for ${genDir}.`);
  const root = parseDirectoryRoot(gunzipSync(readFileSync(resolve(genDir, directory.root))));
  const pagesDir = String(directory.pages || "terms/directory-pages/").replace(/\/?$/u, "/");
  const packCache = new Map();
  for (const page of root.pages) {
    const pageBuffer = gunzipSync(readFileSync(resolve(genDir, `${pagesDir}${page.file}`)));
    const entries = parseDirectoryPage(pageBuffer, { packTable: directory.pack_table || [] });
    for (const entry of entries.values()) {
      let pack = packCache.get(entry.pack);
      if (!pack) {
        pack = readFileSync(resolve(genDir, "terms", "packs", entry.pack));
        packCache.set(entry.pack, pack);
      }
      const shardBytes = gunzipSync(pack.subarray(entry.offset, entry.offset + entry.length));
      // The generation manifest is required: per-term block-filter summaries
      // inside shard payloads are sized by manifest.block_filters.
      const shard = parsePostingSegment(shardBytes, genManifest);
      for (const [term, termEntry] of shard.terms) {
        dfBase.set(term, (dfBase.get(term) || 0) + (termEntry.df || 0));
      }
    }
  }
}

function generationDirName(index) {
  return `gen-${String(index).padStart(4, "0")}`;
}

async function prepareGenerationalUpdate(config, { frozenStats = false } = {}) {
  const rootDir = config.output;
  const rootManifestPath = resolve(rootDir, "manifest.json");
  if (!existsSync(rootManifestPath)) {
    throw new Error("Rangefind update: no existing index to update (run a full build first).");
  }
  const rootManifest = JSON.parse(readFileSync(rootManifestPath, "utf8"));
  let generations;
  if (Array.isArray(rootManifest.generations)) {
    generations = rootManifest.generations;
  } else {
    if (!rootManifest.field_stats || !rootManifest.id_map) {
      throw new Error("Rangefind update: the base index predates generational support; rebuild it once with this version.");
    }
    // Wrap the existing single index as generation 0 without touching any of
    // its pack bytes; only its manifests get stable generation-scoped names.
    const genManifestMin = "manifest.gen0000.min.json";
    const genManifestFull = "manifest.gen0000.json";
    if (!existsSync(resolve(rootDir, genManifestMin))) {
      copyFileSync(resolve(rootDir, "manifest.min.json"), resolve(rootDir, genManifestMin));
      copyFileSync(resolve(rootDir, "manifest.json"), resolve(rootDir, genManifestFull));
    }
    generations = [{
      path: "",
      manifest: genManifestMin,
      total: rootManifest.total,
      field_stats: rootManifest.field_stats,
      id_map: rootManifest.id_map,
      tombstones: []
    }];
  }

  const genIndex = generations.length;
  const genDir = generationDirName(genIndex);
  let prevTotal = 0;
  const prevFieldTotals = {};
  const idMap = new Map();
  const dfBase = new Map();
  for (let g = 0; g < generations.length; g++) {
    const generation = generations[g];
    prevTotal += generation.total || 0;
    for (const [field, tokens] of Object.entries(generation.field_stats?.field_totals || {})) {
      prevFieldTotals[field] = (prevFieldTotals[field] || 0) + tokens;
    }
    const genRoot = resolve(rootDir, generation.path || ".");
    const genManifest = JSON.parse(readFileSync(resolve(rootDir, generation.manifest), "utf8"));
    // Generations share one term space: a delta built with a different
    // analysis profile would emit incomparable terms, so scores could never
    // merge. Frozen like the field stats. Both sides use the resolved
    // profile so a default and an explicit-but-identical block still match.
    if (stableJson(genManifest.analysis || null) !== stableJson(analyzerForConfig(config).profile)) {
      throw new Error(
        "Rangefind update: the analysis profile differs from the existing index; " +
        "keep the same `analysis` config for delta builds or run a full rebuild."
      );
    }
    const tombstoned = new Set(generation.tombstones || []);
    for await (const [externalId, localIndex] of readIdMapEntries(resolve(genRoot, generation.id_map))) {
      if (!tombstoned.has(localIndex)) idMap.set(externalId, [g, localIndex]);
    }
    // Stats-frozen shards take idf from the shared scoring-stats artifact
    // instead — the same table the base generation baked from, so the delta
    // stays exactly comparable across generations AND across shards. It also
    // spares deltas the term-directory scan over every prior generation.
    if (!frozenStats) collectGenerationDf(genRoot, genManifest, dfBase);
  }

  config.output = resolve(rootDir, genDir);
  if (!frozenStats) {
    // Delta builds run the reducer on the main thread so the df map is
    // shared, not cloned per worker; deltas are small by definition. The
    // frozen-stats path keeps parallel reducers — workers resolve df through
    // the on-disk table.
    config.partitionReducerWorkers = 0;
  }
  config.resumeBuild = false;
  config._buildRoot = resolve(config.output, "_build");
  mkdirSync(resolve(config.output), { recursive: true });
  return { rootDir, rootManifest, generations, genIndex, genDir, prevTotal, prevFieldTotals, idMap, dfBase };
}

async function writeGenerationalRoot(generational, deltaManifest, config) {
  const { rootDir, generations, genDir, idMap } = generational;
  let replaced = 0;
  for await (const [externalId] of readIdMapEntries(resolve(config.output, deltaManifest.id_map))) {
    const previous = idMap.get(externalId);
    if (!previous) continue;
    const [genIndexPrev, localIndex] = previous;
    generations[genIndexPrev].tombstones = generations[genIndexPrev].tombstones || [];
    generations[genIndexPrev].tombstones.push(localIndex);
    replaced += 1;
  }
  generations.push({
    path: `${genDir}/`,
    manifest: `${genDir}/manifest.min.json`,
    total: deltaManifest.total,
    field_stats: deltaManifest.field_stats,
    id_map: deltaManifest.id_map,
    tombstones: []
  });
  const aliveTotal = generations.reduce(
    (sum, generation) => sum + (generation.total || 0) - (generation.tombstones?.length || 0),
    0
  );
  const root = {
    version: 1,
    engine: "rangefind",
    ...(config.meta ? { meta: config.meta } : {}),
    features: { generations: true },
    generations,
    total: aliveTotal,
    built_at: new Date().toISOString()
  };
  writeFileSync(resolve(rootDir, "manifest.min.json"), JSON.stringify(root));
  writeFileSync(resolve(rootDir, "manifest.json"), JSON.stringify(root, null, 2));
  const tombstoneCount = generations.reduce((sum, generation) => sum + (generation.tombstones?.length || 0), 0);
  return {
    replaced,
    aliveTotal,
    generations: generations.length,
    tombstoneRatio: tombstoneCount / Math.max(1, aliveTotal + tombstoneCount)
  };
}

// Compaction folds every generation back into one full index. The published
// packs only hold display fields — not the indexed source text — so the fold
// is a full rebuild from the config's input, which therefore must be the
// complete corpus; prepare/finalize verify that before deleting anything.
const COMPACT_MAX_GENERATIONS = 8;
const COMPACT_MAX_TOMBSTONE_RATIO = 0.25;

async function prepareCompaction(config) {
  const rootDir = config.output;
  const rootManifestPath = resolve(rootDir, "manifest.json");
  if (!existsSync(rootManifestPath)) return null;
  const rootManifest = JSON.parse(readFileSync(rootManifestPath, "utf8"));
  const liveIds = new Set();
  const generationDirs = new Set();
  const generationManifests = new Set();
  // Sweep unreferenced leftovers too — a previously failed compaction
  // publishes a plain manifest but keeps the old generation files around.
  for (const name of readdirSync(rootDir)) {
    if (/^gen-\d{4}$/u.test(name)) generationDirs.add(name);
    if (/^manifest\.gen\d{4}(\.min)?\.json$/u.test(name)) generationManifests.add(name);
  }
  if (!Array.isArray(rootManifest.generations)) {
    if (!generationDirs.size && !generationManifests.size) return null;
    return { rootDir, liveIds, generationDirs, generationManifests };
  }
  for (const generation of rootManifest.generations) {
    if (generation.path) {
      generationDirs.add(generation.path.replace(/\/+$/u, ""));
    } else if (generation.manifest) {
      generationManifests.add(generation.manifest);
      generationManifests.add(generation.manifest.replace(/\.min\.json$/u, ".json"));
    }
    const tombstoned = new Set(generation.tombstones || []);
    for await (const [externalId, localIndex] of readIdMapEntries(resolve(rootDir, generation.path || ".", generation.id_map))) {
      if (!tombstoned.has(localIndex)) liveIds.add(externalId);
    }
  }
  return { rootDir, liveIds, generationDirs, generationManifests };
}

async function finalizeCompaction(compaction, manifest, config) {
  const missing = new Set(compaction.liveIds);
  for await (const [externalId] of readIdMapEntries(resolve(config.output, manifest.id_map))) missing.delete(externalId);
  if (missing.size) {
    const examples = [...missing].slice(0, 5);
    throw new Error(
      `Rangefind compact: ${missing.size} live document(s) from the generational index are missing from the input ` +
      `(e.g. ${examples.join(", ")}). Compaction rebuilds from scratch, so the input must be the FULL corpus. ` +
      "The rebuilt index was published, but the old generation directories were kept untouched."
    );
  }
  for (const dir of compaction.generationDirs) {
    rmSync(resolve(compaction.rootDir, dir), { recursive: true, force: true });
  }
  for (const file of compaction.generationManifests) {
    rmSync(resolve(compaction.rootDir, file), { force: true });
  }
  return { removedGenerations: compaction.generationDirs.size + (compaction.generationManifests.size ? 1 : 0) };
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

async function writeSortReplicaRankMap(out, config, replica, rows) {
  const chunkSize = sortReplicaRankChunkSize(config);
  const basePath = `sort-replicas/${replica.id}/rank-packs`;
  const packWriter = createPackWriter(resolve(out, basePath), config.sortReplicaPackBytes || config.packBytes);
  const chunks = [];
  const pool = await createDocPageWorkerPool(config, { gzipLevel: 6 });

  function addChunk(start, compressed, logicalLength) {
    const entry = writePackedShard(packWriter, `${replica.id}\u0000${start}`, compressed, {
      kind: "sort-replica-rank-map",
      codec: SORT_REPLICA_RANK_MAP_FORMAT,
      logicalLength
    });
    chunks.push({
      start,
      count: Math.min(chunkSize, rows.length - start),
      ...entry
    });
  }

  if (pool) {
    try {
      const batchChunks = 64;
      function* chunkBatches() {
        for (let batchStart = 0; batchStart < rows.length; batchStart += chunkSize * batchChunks) {
          const items = [];
          const batchEnd = Math.min(rows.length, batchStart + chunkSize * batchChunks);
          for (let start = batchStart; start < batchEnd; start += chunkSize) {
            items.push({ key: start, bytes: encodeSortReplicaRankChunk(rows, start, Math.min(rows.length, start + chunkSize)) });
          }
          yield { kind: "gzip", items };
        }
      }
      await mapWorkerBatchesOrdered(pool, chunkBatches(), (response) => {
        for (const item of response.items) addChunk(item.key, toBatchBuffer(item.compressed), item.logicalLength);
      });
    } finally {
      await pool.close();
    }
  } else {
    for (let start = 0; start < rows.length; start += chunkSize) {
      const encoded = encodeSortReplicaRankChunk(rows, start, Math.min(rows.length, start + chunkSize));
      addChunk(start, gzipSync(encoded, { level: 6 }), encoded.length);
    }
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

async function writeSortReplicaDocPages(out, config, replica, rows, spool) {
  const pageSize = sortReplicaDocPageSize(config);
  const fields = [...new Set([...docPayloadFieldNames(config), "index"])];
  const packBase = `sort-replicas/${replica.id}/docs/page-packs`;
  const pointerBase = `sort-replicas/${replica.id}/docs/pages`;
  const packWriter = createAppendOnlyPackWriter(resolve(out, packBase), config.sortReplicaDocPagePackBytes || config.docPagePackBytes);
  const entries = [];
  const pool = await createDocPageWorkerPool(config, {
    payloadPath: spool.path,
    payloadEntryPath: spool.entryPath,
    fields,
    gzipLevel: 6
  });
  if (pool) {
    try {
      const batchPages = 64;
      const pageCount = Math.ceil(rows.length / pageSize);
      function* pageBatches() {
        for (let batchStart = 0; batchStart < pageCount; batchStart += batchPages) {
          const pages = [];
          for (let pageIndex = batchStart; pageIndex < Math.min(pageCount, batchStart + batchPages); pageIndex++) {
            const pageStart = pageIndex * pageSize;
            const pageEnd = Math.min(rows.length, pageStart + pageSize);
            const docIds = new Uint32Array(pageEnd - pageStart);
            for (let rank = pageStart; rank < pageEnd; rank++) docIds[rank - pageStart] = rows[rank].doc;
            pages.push({ pageIndex, docIds });
          }
          yield { kind: "rank-pages", pages };
        }
      }
      await mapWorkerBatchesOrdered(pool, pageBatches(), (response) => {
        for (const page of response.pages) {
          entries[page.pageIndex] = writePackedShard(packWriter, docPageKey(page.pageIndex), toBatchBuffer(page.compressed), {
            kind: "sort-replica-doc-page",
            codec: DOC_PAGE_FORMAT,
            logicalLength: page.logicalLength
          });
        }
      });
    } finally {
      await pool.close();
    }
  } else {
    const reader = openDocPageSpoolReader(spool, config, config.sortReplicaDocPageSourceCachePages);
    try {
      for (let pageStart = 0, pageIndex = 0; pageStart < rows.length; pageStart += pageSize, pageIndex++) {
        const pageEnd = Math.min(rows.length, pageStart + pageSize);
        const docs = [];
        for (let rank = pageStart; rank < pageEnd; rank++) {
          docs.push(reader.doc(rows[rank].doc));
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
      reader.close();
    }
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const pointerTable = buildDocPagePointerTable(entries.map(entry => resolvePackEntry(packWriter, entry)), packIndexes);
  const hash = sha256Hex(pointerTable.buffer);
  const file = `${pointerBase}/${hashedFile("0000", hash, ".bin.gz")}`;
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
  const reducerWorkers = partitionReducerWorkerCount(config);
  const usePartitionWorkers = reducerWorkers > 1 && segmentData.segments.length > 0;
  const partitionPool = usePartitionWorkers ? createPartitionReducerPool(config, reducerWorkers) : null;
  const packTargetBytes = config.sortReplicaPackBytes || config.packBytes;
  const blockTargetBytes = config.sortReplicaPostingBlockPackBytes || config.postingBlockPackBytes;
  const packWriter = usePartitionWorkers ? null : createAppendOnlyPackWriter(resolve(termsBase, "packs"), packTargetBytes);
  const blockPackWriter = usePartitionWorkers || config.externalPostingBlocks === false
    ? null
    : createAppendOnlyPackWriter(resolve(termsBase, "block-packs"), blockTargetBytes);
  const directorySpool = createDirectoryEntrySpool(resolve(dirs.build, "sort-replicas", replica.id, "terms-directory.run"));
  const finalShards = new Set();
  const blockStats = emptyPostingSegmentStats();
  let partitionOutput = { packs: [], packBytes: 0, blockPacks: [], blockPackBytes: 0 };
  let stats = { terms: 0, postings: 0, mergeTiers: [], mergePolicy: null, timings: {}, partitionSpoolBytes: 0, partitionSpoolEntries: 0 };

  if (segmentData.segments.length) {
    const workerMessage = {
      config: replicaConfig,
      codesDescriptor: null,
      filters: [],
      termsOutDir: resolve(termsBase, "packs"),
      blockOutDir: resolve(termsBase, "block-packs"),
      termPackCounter: partitionPool?.termPackCounterBuffer,
      blockPackCounter: partitionPool?.blockPackCounterBuffer,
      targetBytes: packTargetBytes,
      blockTargetBytes,
      total: totalRanks
    };
    try {
      stats = await mergeSegmentsToPartitions({
        segments: segmentData.segments,
        scratchDir,
        config: replicaConfig,
        partitionConcurrency: usePartitionWorkers ? partitionPool.count : 1,
        onPartition: async (partition) => {
          if (usePartitionWorkers) {
            const result = await partitionPool.reduce(partition, workerMessage);
            addPostingSegmentStats(blockStats, result.stats);
            appendDirectoryEntry(directorySpool, partition.name, result.entry);
            finalShards.add(partition.name);
            return partition.name;
          }
          const encoded = buildFinalPostingSegmentChunks(partitionTermEntries(partition), totalRanks, null, [], replicaConfig, blockPackWriter);
          addPostingSegmentStats(blockStats, encoded.stats);
          const entry = await writePackedShardChunks(packWriter, partition.name, encoded.chunks, {
            kind: "posting-segment",
            codec: encoded.format || POSTING_SEGMENT_FORMAT,
            logicalLength: encoded.logicalLength,
            streamMinBytes: replicaConfig.postingSegmentStreamMinBytes,
            gzipLevel: replicaConfig.postingGzipLevel
          });
          appendDirectoryEntry(directorySpool, partition.name, entry);
          finalShards.add(partition.name);
          return partition.name;
        },
        onPartitions: usePartitionWorkers ? async (partitions) => {
          const results = await partitionPool.reduceBatch(partitions, workerMessage);
          for (const result of results) {
            addPostingSegmentStats(blockStats, result.stats);
            appendDirectoryEntry(directorySpool, result.name, result.entry);
            finalShards.add(result.name);
          }
          return results.map(result => result.name);
        } : null
      });
      if (usePartitionWorkers) partitionOutput = await partitionPool.finish();
    } finally {
      await partitionPool?.close();
    }
  }

  if (blockPackWriter) finalizePackWriter(blockPackWriter);
  if (packWriter) finalizePackWriter(packWriter);
  if (directorySpool.entries !== finalShards.size) {
    throw new Error(`Rangefind sort replica produced ${directorySpool.entries - finalShards.size} duplicate term shard keys.`);
  }
  const termPacks = usePartitionWorkers ? partitionOutput.packs.sort(comparePackFiles) : packWriter.packs;
  const blockPacks = usePartitionWorkers ? partitionOutput.blockPacks.sort(comparePackFiles) : (blockPackWriter?.packs || []);
  const termPackBytes = usePartitionWorkers ? partitionOutput.packBytes : packWriter.bytes;
  const blockPackBytes = usePartitionWorkers ? partitionOutput.blockPackBytes : (blockPackWriter?.bytes || 0);
  const packIndexes = new Map(termPacks.map((pack, index) => [pack.file, index]));
  const directoryEntries = directorySpool.entries
    ? sortedDirectoryEntrySpool(directorySpool, {
        packNameMap: packWriter?.packNameMap,
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
    packBytes: termPackBytes,
    blockPackBytes,
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
    const rankMap = await writeSortReplicaRankMap(dirs.out, config, definition, rows);
    const docPagesPromise = writeSortReplicaDocPages(dirs.out, config, definition, rows, docSpool);
    const segmentData = await buildSortReplicaSegments(config, dirs, selectedTermSpool, definition, docToRank);
    const reduced = await reduceSortReplicaSegments(config, measured, dirs, definition, segmentData, rows.length);
    const docPages = await docPagesPromise;

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
    return writePackedShard(blockPackWriter, key, gzipSync(bytes, { level: config.postingGzipLevel }), {
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

function codeStoreDescriptorForPartitionWorkers(codes, config, filters = []) {
  const descriptor = codes.descriptor();
  // Posting reduction reads code values only to build the configured block
  // filter summaries. Geo and other doc-value columns can be hundreds of MiB
  // at national scale; including them made the all-or-nothing shared preload
  // exceed its budget and forced every worker into random chunk-cache reads.
  const filterNames = new Set(filters.map(filter => filter.name));
  const fields = (descriptor.fields || []).filter(field => filterNames.has(field.name));
  const explicit = Math.max(0, Math.floor(Number(config.codeStoreWorkerCacheChunks || 0)));
  const cacheDocs = Math.max(1, Math.floor(Number(descriptor.cacheDocs || config.codeStoreCacheDocs || 1)));
  const totalChunks = Math.max(1, Math.ceil(Math.max(0, Number(descriptor.total || 0)) / cacheDocs));
  const maxAuto = Math.max(1, Math.floor(Number(config.codeStoreWorkerMaxAutoCacheChunks || 64)));
  const cacheChunks = explicit || Math.min(maxAuto, totalChunks);
  const preloadMaxBytes = Math.max(0, Math.floor(Number(config.codeStoreWorkerPreloadMaxBytes ?? 1536 * 1024 * 1024)));
  return preloadCodeStoreDescriptor({
    ...descriptor,
    fields,
    cacheChunks
  }, preloadMaxBytes);
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
  const analysis = analyzeDocumentForIndex(doc, config, avgLens, {
    includeFieldTerms: queryBundlesEnabled(config)
  });
  return {
    index,
    selectedTerms: analysis.selectedTerms,
    fieldTerms: analysis.fieldTerms || null
  };
}

function mergeQueryBundleSeedCandidates(buffer, candidates) {
  if (!buffer.enabled || !candidates) return;
  for (const { key, baseTerms } of candidates) {
    if (!buffer.seeds.has(key)) {
      if (buffer.seeds.size >= buffer.maxSeedCandidates) continue;
      buffer.seeds.set(key, {
        key,
        baseTerms,
        expandedTerms: expandedTermsFromBaseTerms(baseTerms)
      });
    }
    buffer.counts.set(key, (buffer.counts.get(key) || 0) + 1);
  }
}

function toBatchBuffer(value) {
  return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
}

function consumeScanBatch(state, response) {
  const {
    config,
    measured,
    codes,
    initialResults,
    queryBundleSeedBuffer,
    authorityBuffer,
    docSpool,
    selectedTermSpool
  } = state;
  const n = response.docs;
  const baseIndex = response.baseIndex;

  const payloadBytes = toBatchBuffer(response.payloadBytes);
  queueSpoolWrite(docSpool, "payload", docSpool.fd, payloadBytes);
  const entriesBuffer = Buffer.allocUnsafe(n * DOC_SPOOL_ENTRY_BYTES);
  for (let i = 0; i < n; i++) {
    writeBigUInt(entriesBuffer, i * DOC_SPOOL_ENTRY_BYTES, docSpool.offset);
    writeBigUInt(entriesBuffer, i * DOC_SPOOL_ENTRY_BYTES + 8, response.payloadLengths[i]);
    writeBigUInt(entriesBuffer, i * DOC_SPOOL_ENTRY_BYTES + 16, response.payloadLogicalLengths[i]);
    docSpool.offset += response.payloadLengths[i];
    docSpool.bytes += response.payloadLengths[i];
  }
  queueSpoolWrite(docSpool, "entry", docSpool.entryFd, entriesBuffer);
  queueSpoolWrite(docSpool, "layout", docSpool.layoutFd, toBatchBuffer(response.layoutBytes));
  docSpool.layoutDocs += n;

  for (let page = 0; page < response.pageColumns.length; page++) {
    const source = toBatchBuffer(response.pageColumns[page]);
    queueSpoolWrite(docSpool, "page", docSpool.pageFd, source);
    queueSpoolWrite(docSpool, "pageEntry", docSpool.pageEntryFd, encodeDocSpoolEntry({
      offset: docSpool.pageOffset,
      length: source.length,
      logicalLength: source.length
    }));
    docSpool.pageOffset += source.length;
    docSpool.pageBytes += source.length;
    docSpool.pageDocs += response.pageDocCounts[page];
    docSpool.pageCount++;
  }

  const termBytes = toBatchBuffer(response.termBytes);
  selectedTermSpool.pending.push(termBytes);
  selectedTermSpool.pendingBytes += termBytes.length;
  if (selectedTermSpool.pendingBytes >= 1024 * 1024) flushSelectedTermSpool(selectedTermSpool);
  selectedTermSpool.docs += n;
  selectedTermSpool.bytes += termBytes.length;
  for (let i = 0; i < n; i++) selectedTermSpool.terms += response.termCounts[i];

  for (const payload of response.initialPayloads) {
    if (initialResults.length < config.initialResultLimit) initialResults.push(payload);
  }

  for (let f = 0; f < config.facets.length; f++) {
    const facet = config.facets[f];
    const dict = measured.dicts[facet.name];
    const rows = response.facets[f];
    for (let i = 0; i < n; i++) {
      const items = rows[i];
      const values = new Array(items.length);
      for (let v = 0; v < items.length; v++) values[v] = addDict(dict, items[v][0], items[v][1]);
      codes.set(facet.name, baseIndex + i, { codes: values });
    }
  }
  for (let f = 0; f < config.numbers.length; f++) {
    const rows = response.numbers[f];
    for (let i = 0; i < n; i++) {
      codes.set(config.numbers[f].name, baseIndex + i, Number.isNaN(rows[i]) ? null : rows[i]);
    }
  }
  for (let f = 0; f < (config.booleans || []).length; f++) {
    const rows = response.booleans[f];
    for (let i = 0; i < n; i++) {
      codes.set(config.booleans[f].name, baseIndex + i, rows[i] < 0 ? null : rows[i] > 0);
    }
  }

  if (authorityBuffer.enabled && response.authority) {
    const { keys, docs, scores } = response.authority;
    for (let r = 0; r < keys.length; r++) {
      addAuthorityRecord(authorityBuffer, config, keys[r], docs[r], scores[r]);
    }
  }

  if (response.bundleCandidates) {
    for (let i = 0; i < n; i++) mergeQueryBundleSeedCandidates(queryBundleSeedBuffer, response.bundleCandidates[i]);
  }

  appendVectorBatch(state.vectorSpools, response, n);
}

// Vectors spool straight to disk as fixed-width Float32 rows (NaN in the
// first component marks a missing vector); millions of embeddings never sit
// in builder memory.
function createVectorSpools(config, dirs) {
  if (!vectorsEnabled(config)) return null;
  const spools = [];
  for (const field of config.vectors) {
    const file = resolve(dirs.build, `vectors-${safeObjectName(field.name)}.f32`);
    spools.push({ field, file, fd: openSync(file, "w"), rows: 0 });
  }
  return spools;
}

function appendVectorBatch(spools, response, docs) {
  if (!spools || !response.vectors) return;
  for (let f = 0; f < spools.length; f++) {
    const spool = spools[f];
    const block = response.vectors[f];
    const buffer = Buffer.from(block.buffer, block.byteOffset, docs * spool.field.dims * 4);
    writeSync(spool.fd, buffer, 0, buffer.length);
    spool.rows += docs;
  }
}

function appendVectorDoc(spools, config, doc) {
  if (!spools) return;
  for (const spool of spools) {
    const vector = vectorFromValue(rawPath(doc, spool.field.path, null), spool.field.dims);
    const normalized = vector ? normalizeVector(vector) : null;
    const row = normalized || (() => {
      const missing = new Float32Array(spool.field.dims);
      missing.fill(Number.NaN);
      return missing;
    })();
    writeSync(spool.fd, Buffer.from(row.buffer, row.byteOffset, spool.field.dims * 4));
    spool.rows += 1;
  }
}

function finishVectorSpools(spools) {
  if (!spools) return null;
  return spools.map(spool => {
    closeSync(spool.fd);
    return { name: spool.field.name, file: spool.file, rows: spool.rows };
  });
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
    selectedTermSpool
  } = state;
  const selectedTerms = analysis.selectedTerms || [];
  writeSelectedTerms(selectedTermSpool, selectedTerms);
  for (const [term, score] of selectedTerms) {
    addSegmentPosting(segmentBuilder, term, index, Math.max(1, Math.round(score * 1000)));
  }
  addQueryBundleSeeds(queryBundleSeedBuffer, selectedTerms, config, doc, analysis.fieldTerms);
  addAuthorityDoc(authorityBuffer, config, doc, index, analyzerForConfig(config));
  appendVectorDoc(state.vectorSpools, config, doc);

  if (shouldFlushSegment(segmentBuilder)) {
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

async function scanWithWorkers(state, dirs) {
  const workerCount = scanWorkerCount(state.config);
  const pageSize = state.docSpool.pageSize;
  const batchDocs = Math.max(pageSize, Math.ceil(scanBatchDocs(state.config) / pageSize) * pageSize);
  const workers = Array.from({ length: workerCount }, (_, index) => ({
    index,
    worker: new Worker(new URL("./scan_worker.js", import.meta.url), { type: "module" }),
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
      const response = pending.get(nextWrite);
      pending.delete(nextWrite);
      consumeScanBatch(state, response);
      nextWrite++;
    }
  }

  async function waitForWorker() {
    while (!available.length) await Promise.race(active);
    return available.pop();
  }

  async function queueBatch(lines, baseIndex) {
    const entry = await waitForWorker();
    const id = nextBatch++;
    const started = performance.now();
    const promise = postScanBatch(entry.worker, { id, baseIndex, lines }).then((response) => {
      entry.docs += lines.length;
      entry.batches++;
      entry.analysisMs += performance.now() - started;
      pending.set(id, response);
      drainPending();
    }).finally(() => {
      active.delete(promise);
      available.push(entry);
    });
    active.add(promise);
  }

  let segments = [];
  try {
    await Promise.all(workers.map((entry, index) => postScanBatch(entry.worker, {
      type: "init",
      id: `init-${index}`,
      config: state.config,
      avgLens: state.measured.avgLens,
      segmentsDir: resolve(dirs.build, "segments", `worker-${String(index).padStart(2, "0")}`),
      segmentIdPrefix: `segment-w${String(index).padStart(2, "0")}`
    })));
    const rl = createInterface({ input: createJsonlReadStream(state.config.input), crlfDelay: Infinity });
    let index = 0;
    let baseIndex = 0;
    let batch = [];
    for await (const line of rl) {
      if (!line.trim()) continue;
      batch.push(line);
      index++;
      if (batch.length >= batchDocs) {
        await queueBatch(batch, baseIndex);
        baseIndex = index;
        batch = [];
      }
    }
    if (batch.length) await queueBatch(batch, baseIndex);
    while (active.size) await Promise.race(active);
    drainPending();
    if (pending.size) throw new Error("Rangefind scan workers finished out of order.");
    const finished = await Promise.all(workers.map((entry, index) => postScanBatch(entry.worker, {
      type: "finish",
      id: `finish-${index}`
    })));
    segments = finished.flatMap(response => response.segments || []);
  } finally {
    await Promise.allSettled(workers.map(entry => entry.worker.terminate()));
  }

  return {
    segments,
    workerStats: workers.map(entry => ({
      worker: entry.index,
      docs: entry.docs,
      batches: entry.batches,
      analysisMs: entry.analysisMs,
      mode: "worker-thread"
    }))
  };
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

function createPartitionReducerPool(config, countOverride = 0) {
  const count = Math.max(1, Math.floor(Number(countOverride || partitionReducerWorkerCount(config))));
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
    async reduceBatch(partitions, message) {
      const inputBytes = partitions.reduce((sum, partition) => sum + partitionInputBytes(partition), 0);
      const entry = await checkoutWorker(inputBytes);
      const id = nextId++;
      const promise = postReducePartition(entry.worker, { ...message, id, partitions })
        .then((result) => {
          const results = result.results || [];
          entry.tasks += results.length;
          entry.inputBytes += result.inputBytes || inputBytes;
          entry.reduceMs += result.ms || results.reduce((sum, item) => sum + (item.ms || 0), 0);
          return results.map(item => ({ ...item, worker: entry.index }));
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

async function writePostingRuns(config, measured, dirs) {
  const codes = createCodeStore(resolve(dirs.build, "codes"), config, measured.total, measured.dicts);

  const initialResults = [];
  const segmentBuilder = createSegmentBuilder(resolve(dirs.build, "segments"), config);
  const queryBundleSeedBuffer = createQueryBundleSeedBuffer(config);
  const authorityBuffer = createAuthorityRunBuffer(config, dirs.authorityRunsOut);
  const docSpool = createDocSpool(resolve(dirs.build, "docs"), config);
  const selectedTermSpool = createSelectedTermSpool(resolve(dirs.build, "terms"));
  const state = {
    config,
    measured,
    codes,
    initialResults,
    segmentBuilder,
    queryBundleSeedBuffer,
    authorityBuffer,
    vectorSpools: createVectorSpools(config, dirs),
    docSpool,
    selectedTermSpool
  };
  let scanWorkerStats = [];
  let workerSegments = null;

  try {
    if (scanWorkerCount(config) > 1) {
      const scanned = await scanWithWorkers(state, dirs);
      scanWorkerStats = scanned.workerStats;
      workerSegments = scanned.segments;
    } else {
      scanWorkerStats = await scanSequential(state);
    }
  } finally {
    closeDocSpool(docSpool);
    closeSelectedTermSpool(selectedTermSpool);
  }
  const segments = workerSegments ?? finishSegmentBuilder(segmentBuilder);
  const authorityBaseShards = finishAuthorityRuns(authorityBuffer);
  const queryBundleSeeds = finalizeQueryBundleSeeds(queryBundleSeedBuffer, config);
  const vectorSpools = finishVectorSpools(state.vectorSpools);
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
    vectorSpools,
    scanWorkerStats
  };
}

async function reduceRuns(config, measured, runData, dirs) {
  const filters = buildBlockFilters(config, measured.dicts);
  const started = performance.now();
  const reducerWorkers = partitionReducerWorkerCount(config);
  const usePartitionWorkers = reducerWorkers > 1;
  const partitionPool = usePartitionWorkers ? createPartitionReducerPool(config, reducerWorkers) : null;
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
  const workerCodesDescriptor = usePartitionWorkers ? codeStoreDescriptorForPartitionWorkers(runData.codes, config, filters) : null;
  let stats;
  try {
    stats = await mergeSegmentsToPartitions({
      segments: runData.segments,
      scratchDir: resolve(dirs.build, "segment-merge"),
      config,
      partitionConcurrency: usePartitionWorkers ? partitionPool.count : 1,
      onTerm: (term, df) => {
        if (bundleTermSet.has(term)) bundleDfs.set(term, df);
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
          streamMinBytes: config.postingSegmentStreamMinBytes,
          gzipLevel: config.postingGzipLevel
        });
        appendDirectoryEntry(directorySpool, partition.name, entry);
        finalShards.add(partition.name);
        return partition.name;
      },
      onPartitions: usePartitionWorkers ? async (partitions) => {
        const results = await partitionPool.reduceBatch(partitions, {
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
        for (const result of results) {
          addPostingSegmentStats(blockStats, result.stats);
          appendDirectoryEntry(directorySpool, result.name, result.entry);
          finalShards.add(result.name);
        }
        return results.map(result => result.name);
      } : null
    });
    if (usePartitionWorkers) partitionOutput = await partitionPool.finish();
  } finally {
    await partitionPool?.close();
  }
  if (blockPackWriter) finalizePackWriter(blockPackWriter);
  if (packWriter) finalizePackWriter(packWriter);
  if (directorySpool.entries !== finalShards.size) {
    throw new Error(`Rangefind reducer produced ${directorySpool.entries - finalShards.size} duplicate term shard keys.`);
  }
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
    workerCodeStorePreloadedBytes: workerCodesDescriptor?.preloadedBytes || 0,
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
    workerCodeStorePreloadedBytes: reduced.workerCodeStorePreloadedBytes || 0,
    workerStats: reduced.workerStats || [],
    reduceTimings: {
      ...(reduced.reduceTimings || {}),
      segmentTierMergeMs: stats.timings?.tierMergeMs || 0,
      segmentPrefixCountMs: stats.timings?.prefixCountMs || 0,
      segmentPartitionAssemblyMs: stats.timings?.partitionAssemblyMs || 0
    },
    partitionSpoolBytes: stats.partitionSpoolBytes || 0,
    partitionSpoolEntries: stats.partitionSpoolEntries || 0
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
      resolve(out, "geo", "point-packs"),
      resolve(out, "geo", "category-cells", "packs"),
      resolve(out, "sort-replicas")
    ],
    sidecars: [
      resolve(out, "terms", "block-packs"),
      resolve(out, "terms", "directory"),
      resolve(out, "docs", "pointers"),
      resolve(out, "docs", "pages"),
      resolve(out, "doc-values", "sorted"),
      resolve(out, "filter-bitmaps", "manifest.json.gz"),
      resolve(out, "facets", "directory"),
      resolve(out, "bundles", "directory"),
      resolve(out, "authority", "directory"),
      resolve(out, "geo", "category-cells", "directory-pages"),
      resolve(out, "segments")
    ]
  };
}

function minimalManifest(manifest) {
  return {
    version: manifest.version,
    engine: manifest.engine,
    ...(manifest.meta ? { meta: manifest.meta } : {}),
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
    field_stats: manifest.field_stats,
    id_map: manifest.id_map,
    docs: manifest.docs,
    initial_results: manifest.initial_results,
    fields: manifest.fields,
    facets: Object.fromEntries(Object.entries(manifest.facets || {}).map(([field, values]) => [field, { count: values.count ?? values.length ?? 0 }])),
    numbers: manifest.numbers,
    booleans: manifest.booleans,
    sorts: manifest.sorts,
    geo: manifest.geo,
    linkGraph: manifest.linkGraph,
    rankPrior: manifest.rankPrior,
    vectors: manifest.vectors,
    block_filters: manifest.block_filters,
    directory: manifest.directory,
    sort_replicas: manifest.sort_replicas,
    query_bundles: manifest.query_bundles,
    authority: manifest.authority,
    analysis: manifest.analysis || null,
    search: manifest.search,
    optimizer: manifest.optimizer,
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

// v7: autocomplete records moved into authority runs. Reusing a v6 scan stage
// would silently omit the unified lexicon even when `suggest` is configured.
// v8: authority fields can emit canonical address keys. Reusing v7 authority
// runs would publish an index whose manifest advertises keys it does not have.
// v9: address interpolation range keys use their own namespace.
// v10: range keys put street/locality before their bucket so prefix sharding
// distributes national corpora; v9 runs could retain oversized partitions.
const BUILD_RESUME_SCHEMA_VERSION = 10;

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
  if (config.scoringStats) {
    // Regenerated stats at the same path must invalidate resume stages:
    // frozen averages and df feed both the scan and reduce outputs.
    const statsStat = statSync(config.scoringStats);
    payload.scoringStats = {
      path: config.scoringStats,
      size: statsStat.size,
      mtimeMs: Math.floor(statsStat.mtimeMs)
    };
  }
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
  if (config.resumeBuild) {
    const path = stagePath(config, stage);
    if (existsSync(path)) {
      const cached = JSON.parse(readFileSync(path, "utf8"));
      if (cached?.status === "complete" && cached.schema === BUILD_RESUME_SCHEMA_VERSION) {
        return hydrate(cached.payload);
      }
    }
  }
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
    fieldTotals: measured.fieldTotals || {},
    avgLens: measured.avgLens,
    dicts: serializeDicts(measured.dicts),
    workerStats: measured.workerStats || []
  };
}

function hydrateMeasured(payload) {
  return {
    total: payload.total,
    fieldTotals: payload.fieldTotals || {},
    avgLens: payload.avgLens || {},
    dicts: hydrateDicts(payload.dicts),
    workerStats: payload.workerStats || []
  };
}

function serializeDocSpool(spool) {
  return {
    path: spool.path,
    entryPath: spool.entryPath,
    layoutPath: spool.layoutPath,
    bytes: spool.bytes || 0,
    layoutDocs: spool.layoutDocs || 0,
    pagePath: spool.pagePath,
    pageEntryPath: spool.pageEntryPath,
    pageSize: spool.pageSize || 0,
    pageFields: spool.pageFields || [],
    pageBytes: spool.pageBytes || 0,
    pageDocs: spool.pageDocs || 0,
    pageCount: spool.pageCount || 0
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

function serializeScanStage(runData) {
  const codeDescriptor = runData.codes.descriptor();
  return {
    // Dictionaries already live in the measure checkpoint and hydrateScanStage
    // deliberately reattaches that canonical copy. Repeating them here made a
    // full-Wikipedia scan checkpoint 67+ MiB larger and doubled parse-time
    // object retention on resume.
    codeDescriptor: { ...codeDescriptor, dicts: undefined },
    initialResults: runData.initialResults,
    segments: runData.segments,
    segmentSummary: runData.segmentSummary,
    docSpool: serializeDocSpool(runData.docSpool),
    selectedTermSpool: serializeSelectedTermSpool(runData.selectedTermSpool),
    queryBundleSeeds: runData.queryBundleSeeds,
    queryBundleTerms: runData.queryBundleTerms,
    authorityBaseShards: runData.authorityBaseShards,
    vectorSpools: runData.vectorSpools,
    scanWorkerStats: runData.scanWorkerStats
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
    vectorSpools: payload.vectorSpools || null,
    scanWorkerStats: payload.scanWorkerStats || []
  };
}

function serializeReducedStage(reduced) {
  return {
    ...reduced,
    finalShards: undefined,
    bundleDfs: [...(reduced.bundleDfs || new Map()).entries()]
  };
}

function hydrateReducedStage(payload) {
  return {
    ...payload,
    bundleDfs: new Map(payload.bundleDfs || [])
  };
}

export async function build({ configPath, update = false, compact = false }) {
  if (update && compact) throw new Error("Rangefind: pass either --update or --compact, not both.");
  const config = await readConfig(configPath);
  const scoringStats = config.scoringStats ? loadScoringStats(config.scoringStats) : null;
  // Compaction of a stats-frozen shard is just a fresh full build with the
  // same artifact into a clean directory; the generational --compact path
  // would re-freeze at shard-local statistics and break comparability.
  if (scoringStats && compact) {
    throw new Error("Rangefind: --compact does not apply to scoringStats shards; rebuild the shard fully instead.");
  }
  // Shards share one term space, exactly like generations: a shard built
  // with a different analysis profile would emit incomparable terms.
  if (scoringStats && stableJson(scoringStats.analysis || null) !== stableJson(analyzerForConfig(config).profile)) {
    throw new Error(
      "Rangefind: the analysis profile differs from the scoring stats; " +
      "regenerate the stats or keep the same `analysis` config across shards."
    );
  }
  // Query-bundle impacts bake idf outside encodePostings and would use
  // shard-local statistics, breaking cross-shard comparability.
  if (scoringStats && queryBundlesEnabled(config)) {
    throw new Error("Rangefind: scoringStats shard builds do not support queryBundles yet; disable queryBundles.");
  }
  const generational = update ? await prepareGenerationalUpdate(config, { frozenStats: Boolean(scoringStats) }) : null;
  const compaction = compact ? await prepareCompaction(config) : null;
  if (compact && !compaction) console.log("Rangefind: no generational index at the output; --compact runs as a plain full build.");
  const fingerprint = config.resumeBuild ? buildFingerprint(config) : "scratch";
  config._buildRoot = config.resumeBuild
    ? resolve(config.output, config.resumeDir, fingerprint)
    : resolve(config.output, "_build");
  const dirs = {
    out: config.output,
    build: config._buildRoot,
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
  rmSync(resolve(dirs.out, "typo"), { recursive: true, force: true });
  rmSync(resolve(dirs.out, "suggest"), { recursive: true, force: true });
  rmSync(resolve(dirs.out, "docs", "ordinals"), { recursive: true, force: true });

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
    recordBuildWorkers(telemetry, "measure", measured.workerStats, {
      configured_workers: scanWorkerCount(config),
      batch_docs: measureBatchDocs(config)
    });
    if (generational) {
      // Freeze field-length normalization at the base generations' averages
      // so a delta document scores exactly as it would have in the base
      // build — the property cross-generation merging depends on.
      measured.avgLens = Object.fromEntries(config.fields.map(field => [
        field.name,
        Math.max(1, (generational.prevFieldTotals[field.name] || 0) / Math.max(1, generational.prevTotal))
      ]));
    }
    if (scoringStats) {
      // Freeze field-length normalization at the corpus-wide averages so a
      // document scores identically no matter which shard indexes it.
      measured.avgLens = Object.fromEntries(config.fields.map(field => [
        field.name,
        Math.max(1, (scoringStats.field_totals?.[field.name] || 0) / Math.max(1, scoringStats.total))
      ]));
    }
    const scanStage = readStage(config, "scan");
    if (scanStage) {
      runData = hydrateScanStage(scanStage, measured);
    } else {
      runData = await timeBuildPhase(telemetry, "scan-and-spool", () => writePostingRuns(config, measured, dirs));
      writeStage(config, "scan", serializeScanStage(runData));
      maybeFailAfterStage(config, "scan");
    }
    recordBuildWorkers(telemetry, "scan-and-spool", runData.scanWorkerStats, {
      configured_workers: scanWorkerCount(config),
      batch_docs: scanBatchDocs(config)
    });
    if (generational) {
      // Set after scan so the df map never gets structured-cloned into scan
      // workers; only the (main-thread) reducer bakes idf.
      config._scoringOverrides = {
        total: generational.prevTotal,
        dfBase: generational.dfBase
      };
    }
    if (scoringStats) {
      // Clone-safe by design: reduce workers get a file path and resolve
      // terms through the sorted df table lazily, so parallel reducers stay
      // enabled for shard builds (unlike generational deltas).
      config._scoringOverrides = {
        total: scoringStats.total,
        dfFile: scoringStats.dfPath
      };
    }
    addBuildCounter(telemetry, "selected_term_spool_bytes", runData.selectedTermSpool.bytes);
    addBuildCounter(telemetry, "selected_term_spool_terms", runData.selectedTermSpool.terms);
    addBuildCounter(telemetry, "doc_gzip_spool_bytes", runData.docSpool.bytes || 0);
    addBuildCounter(telemetry, "doc_page_spool_bytes", runData.docSpool.pageBytes || 0);
    addBuildCounter(telemetry, "doc_page_spool_pages", runData.docSpool.pageCount || 0);
    addBuildCounter(telemetry, "segment_files", runData.segments.length);
    addBuildCounter(telemetry, "segment_postings", runData.segmentSummary.postings);
    applyAutoPostingLayout(config, measured, runData);
    const reduceStage = readStage(config, "reduce");
    let reduced;
    if (reduceStage) {
      reduced = hydrateReducedStage(reduceStage);
    } else {
      reduced = await timeBuildPhase(telemetry, "reduce-postings", () => reduceRuns(config, measured, runData, dirs));
      writeStage(config, "reduce", serializeReducedStage(reduced));
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
      segment_merge_blocked_by_directory_budget: Boolean(reduced.mergePolicy?.blockedByDirectoryBudget),
      segment_merge_max_directory_bytes: reduced.mergePolicy?.maxDirectoryBytes || 0,
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
      code_store_worker_preloaded_bytes: reduced.workerCodeStorePreloadedBytes || 0
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
    const defaultCodeStorePreloadBytes = measured.total >= 10_000_000
      ? 2304 * 1024 * 1024
      : 1536 * 1024 * 1024;
    runData.codes.preload?.(Math.max(0, Math.floor(Number(config.codeStorePreloadMaxBytes ?? defaultCodeStorePreloadBytes))));
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
    let docValues;
    let docValueSorted;
    let filterBitmaps;
    let facetDictionaries;
    let geoTrees;
    let vectors;
    const sidecarStage = readStage(config, "sidecars");
    if (sidecarStage) {
      ({ sortReplicas, queryBundles, authority, docs, docValues, docValueSorted, filterBitmaps, facetDictionaries, geoTrees, vectors } = sidecarStage);
    } else {
      // Checkpoint every independently publishable sidecar. A national geo
      // build can spend minutes in each phase; the former monolithic
      // "sidecars" checkpoint repeated authority and all document packs when
      // a later geo/vector phase was interrupted.
      sortReplicas = await runResumableStage(config, telemetry, "sidecar-sort-replicas", "sort-replicas", () => buildSortReplicas(config, measured, dirs, runData.selectedTermSpool, runData.docSpool, fieldRows));
      queryBundles = await runResumableStage(config, telemetry, "sidecar-query-bundles", "query-bundles", () => buildQueryBundleIndex(config, measured, dirs, runData.queryBundleSeeds, reduced.bundleDfs, runData.selectedTermSpool, reduced.filters, fieldRows));
      authority = await runResumableStage(config, telemetry, "sidecar-authority", "authority", () => reduceAuthorityRuns(config, dirs, runData.authorityBaseShards));
      docs = await runResumableStage(config, telemetry, "sidecar-doc-packs", "doc-packs", () => finishDocPacks(dirs.out, runData.docSpool, measured.total, config));
      docs.pages = await runResumableStage(config, telemetry, "sidecar-doc-pages", "doc-pages", () => finishDocPages(dirs.out, runData.docSpool, measured.total, config));
      docValues = await runResumableStage(config, telemetry, "sidecar-doc-values", "doc-values", () => writeDocValuePacks(dirs.out, config, measured.total, fieldRows));
      docValueSorted = await runResumableStage(config, telemetry, "sidecar-doc-value-sorted", "doc-value-sorted", () => writeDocValueSortedIndexes(dirs.out, config, measured.total, fieldRows));
      geoTrees = await runResumableStage(config, telemetry, "sidecar-geo-trees", "geo-trees", () => writeGeoTrees(
        dirs.out,
        config,
        measured.total,
        fieldRows,
        reduced.filters,
        runData.docSpool,
        measured.dicts
      ));
      vectors = await runResumableStage(config, telemetry, "sidecar-vectors", "vectors", () => writeVectorIndexes(dirs.out, config, runData.vectorSpools));
      docs.id_map = await runResumableStage(config, telemetry, "sidecar-id-map", "id-map", () => writeIdMap(dirs.out, config, measured.total, runData.docSpool));
      filterBitmaps = await runResumableStage(config, telemetry, "sidecar-filter-bitmaps", "filter-bitmaps", () => writeFilterBitmapIndex(dirs.out, config, measured.total, fieldRows, measured.dicts));
      facetDictionaries = await runResumableStage(config, telemetry, "sidecar-facet-dictionaries", "facet-dictionaries", () => writeFacetDictionaries(dirs.out, measured.dicts, config));
      writeStage(config, "sidecars", { sortReplicas, queryBundles, authority, docs, docValues, docValueSorted, filterBitmaps, facetDictionaries, geoTrees, vectors });
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
      ...(config.meta ? { meta: config.meta } : {}),
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
        facetSummaryUint32: true,
        docValues: true,
        docValueSorted: true,
        filterBitmaps: Object.keys(filterBitmaps.fields).length > 0,
        facetDictionaries: true,
        externalPostingBlocks: config.externalPostingBlocks !== false,
        segmentManifest: true,
        queryBundles: !!queryBundles,
        authority: !!authority,
        sortReplicas: sortReplicas.count > 0,
        mainIndexTypo: config.typoMode !== "off",
        geo: !!geoTrees && Object.keys(geoTrees.fields).length > 0,
        geoCapsules: Boolean(geoTrees?.capsule_fields?.length),
        geoCategoryCells: Boolean(geoTrees?.category_cells?.indexes?.length),
        suggest: !!authority?.autocomplete?.keys,
        vectors: !!vectors && Object.keys(vectors.fields).length > 0
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
        geo: packTable(geoTrees?.packs),
        geoCategoryCells: packTable(geoTrees?.cell_packs)
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
        geoTrees?.packs || [],
        geoTrees?.cell_packs || []
      ),
      directories: {
        terms: reduced.directory,
        facets: facetDictionaries.directory,
        queryBundles: queryBundles?.directory || null,
        authority: authority?.directory || null
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
    field_stats: {
      total: measured.total,
      field_totals: measured.fieldTotals || {}
    },
    id_map: docs.id_map?.file || null,
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
    geo: geoTrees
      ? {
          storage: geoTrees.storage,
          compression: geoTrees.compression,
          directory_format: geoTrees.directory_format,
          page_format: geoTrees.page_format,
          leaf_size: geoTrees.leaf_size,
          fields: geoTrees.fields,
          packs: geoTrees.packs.length,
          pack_table: geoTrees.pack_table,
          category_cell_packs: geoTrees.cell_packs.length,
          category_cell_pack_table: geoTrees.cell_pack_table
        }
      : null,
    linkGraph: config.linkGraph || null,
    rankPrior: config.rankPrior || null,
    vectors: vectors
      ? {
          storage: vectors.storage,
          directory_format: vectors.directory_format,
          page_format: vectors.page_format,
          fields: vectors.fields
        }
      : null,
    sort_replicas: sortReplicas,
    filter_bitmaps: {
      storage: filterBitmaps.storage,
      compression: filterBitmaps.compression,
      format: filterBitmaps.format,
      max_facet_values: filterBitmaps.max_facet_values,
      max_bitmap_bytes: filterBitmaps.max_bitmap_bytes,
      fields: filterBitmaps.fields,
      packs: filterBitmaps.packs.length,
      pack_table: filterBitmaps.pack_table
    },
    initial_results: runData.initialResults,
    fields: config.fields.map(({ name, path, weight, b, phrase, proximity, proximityWeight }) => ({ name, path, weight, b, phrase: !!phrase, proximity: !!proximity, proximityWeight: proximityWeight || 0 })),
    facets: Object.fromEntries(Object.entries(facetDictionaries.fields).map(([name, field]) => [name, { count: field.count }])),
    facet_dictionaries: facetDictionaries,
    numbers: config.numbers.map(n => ({
      name: n.name,
      type: normalizedNumberType(n),
      sortable: n.sortable !== false,
      ...(n.geoComponent ? { geo_component: n.geoComponent } : {})
    })),
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
      autocomplete: authority.autocomplete,
      directory: authority.directory,
      packs: authority.packs.length,
      stats: {
        pack_files: authority.packs.length,
        pack_bytes: authority.pack_bytes,
        directory_bytes: authority.directory_bytes
      }
    } : null,
    // Record the resolved profile (the default is explicit, never null), so
    // the runtime always reconstructs the exact analyzer used at build time.
    analysis: analyzerForConfig(config).profile,
    search: {
      postingOrder: config.postingOrder,
      typo: {
        mode: config.typoMode,
        trigger: config.typoTrigger,
        maxEdits: config.typoMaxEdits,
        maxTokenCandidates: config.typoMaxTokenCandidates,
        maxQueryPlans: config.typoMaxQueryPlans,
        maxCorrectedSearches: config.typoMaxCorrectedSearches,
        maxShardLookups: config.typoMaxShardLookups
      }
    },
    stats: {
      terms: reduced.termCount,
      postings: reduced.postingCount,
      build_total_ms: Math.round(buildTelemetry.total_ms),
      build_peak_rss: buildTelemetry.peak_rss,
      selected_term_spool_bytes: runData.selectedTermSpool.bytes,
      selected_term_spool_terms: runData.selectedTermSpool.terms,
      doc_gzip_spool_bytes: runData.docSpool.bytes || 0,
      doc_page_spool_bytes: runData.docSpool.pageBytes || 0,
      doc_page_spool_pages: runData.docSpool.pageCount || 0,
      posting_segment_storage: "range-pack-v1",
      posting_segment_format: POSTING_SEGMENT_FORMAT,
      posting_segment_block_storage: config.externalPostingBlocks === false ? "inline" : "range-pack-v1",
      posting_order: config.postingOrder,
      posting_segment_directory_format: reduced.directory.format,
      posting_segment_directory_page_files: reduced.directory.page_files,
      posting_segment_directory_bytes: reduced.directory.total_bytes,
      posting_segment_pack_files: reduced.packs.length,
      posting_segment_pack_bytes: reduced.packBytes,
      posting_segment_stream_min_bytes: config.postingSegmentStreamMinBytes,
      posting_segment_gzip_level: config.postingGzipLevel,
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
      typo_mode: config.typoMode,
      typo_trigger: config.typoTrigger,
      typo_max_edits: config.typoMaxEdits,
      typo_max_token_candidates: config.typoMaxTokenCandidates,
      typo_max_query_plans: config.typoMaxQueryPlans,
      typo_max_corrected_searches: config.typoMaxCorrectedSearches,
      typo_max_shard_lookups: config.typoMaxShardLookups,
      authority_format: authority?.format || "",
      authority_fields: authority?.fields.length || 0,
      authority_keys: authority?.keys || 0,
      authority_rows: authority?.rows || 0,
      authority_shards: authority?.shards || 0,
      authority_target_shard_rows: authority?.target_shard_rows || 0,
      authority_directory_bytes: authority?.directory_bytes || 0,
      authority_pack_files: authority?.packs.length || 0,
      authority_pack_bytes: authority?.pack_bytes || 0,
      autocomplete_keys: authority?.autocomplete?.keys || 0,
      autocomplete_rows: authority?.autocomplete?.rows || 0,
      autocomplete_shards: authority?.autocomplete?.shards || 0,
      autocomplete_hot_prefixes: authority?.autocomplete?.hot_prefixes || 0,
      autocomplete_directory_bytes: authority?.autocomplete?.directory?.bytes || 0,
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
      code_store_worker_preloaded_bytes: reduced.workerCodeStorePreloadedBytes || 0,
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
      segment_merge_blocked_by_directory_budget: Boolean(reduced.mergePolicy?.blockedByDirectoryBudget),
      segment_merge_max_directory_bytes: reduced.mergePolicy?.maxDirectoryBytes || 0,
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
      field_avg_lengths: measured.avgLens,
      frozen_scoring_stats: Boolean(scoringStats),
      frozen_scoring_total: scoringStats ? scoringStats.total : 0,
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
    if (generational) {
      const summary = await writeGenerationalRoot(generational, manifest, config);
      console.log(`Rangefind: generation ${generational.genDir} added (${measured.total.toLocaleString()} docs, ${summary.replaced.toLocaleString()} replaced, ${summary.generations} generations, ${summary.aliveTotal.toLocaleString()} alive docs)`);
      if (summary.generations >= COMPACT_MAX_GENERATIONS || summary.tombstoneRatio >= COMPACT_MAX_TOMBSTONE_RATIO) {
        console.warn(
          `Rangefind: ${summary.generations} generations, ${(summary.tombstoneRatio * 100).toFixed(1)}% tombstoned — ` +
          "queries now pay a fan-out per generation. Run `rangefind build --compact` with the full corpus as input to fold them."
        );
      }
    }
    if (compaction) {
      const summary = await finalizeCompaction(compaction, manifest, config);
      console.log(`Rangefind: compacted ${summary.removedGenerations} generation(s) into a single index`);
    }
    console.log(`Rangefind: built ${measured.total.toLocaleString()} docs, ${reduced.shards.length.toLocaleString()} posting segments, ${reduced.packs.length.toLocaleString()} packs`);
  } catch (error) {
    writeBuildFailureArtifacts(dirs, telemetry, error);
    throw error;
  } finally {
    runData?.codes?.close?.();
    // Main-thread df readers must not outlive the build: a later build in
    // the same process may regenerate the stats file at the same path.
    closeScoringDfReaders();
  }
}
