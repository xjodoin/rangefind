import { closeSync, createReadStream, mkdirSync, openSync, readFileSync, readSync, rmSync, writeFileSync, writeSync } from "node:fs";
import { createInterface } from "node:readline";
import { dirname, resolve } from "node:path";
import { gunzipSync, gzipSync } from "node:zlib";
import { Worker } from "node:worker_threads";
import { analyzerForConfig } from "./analysis.js";
import { setScoringDfProvider } from "./codec.js";
import { computeScoringStatsBatch } from "./scoring_stats_batch.js";

// Cross-shard scoring statistics.
//
// A sharded corpus is indexed as N independent Rangefind builds, but BM25
// impacts are baked at build time from corpus-wide inputs: the document
// total, average field lengths, and per-term document frequencies. This
// module collects those statistics across every shard input up front and
// freezes them into an artifact each shard build consumes, so a document
// scores identically no matter which shard indexes it — the invariant
// cross-shard merging depends on (the sharded analogue of the generational
// frozen stats in `prepareGenerationalUpdate`).
//
// The document-frequency table is a sorted on-disk file, not an in-heap map:
// reduce workers resolve terms through it lazily, so shard builds keep their
// parallel reducers (unlike generational deltas, which pin the reducer to the
// main thread to share a df map).

export const SCORING_STATS_FORMAT = "rfscoringstats-v1";
export const SCORING_DF_FORMAT = "rfdf-v1";

const DF_BLOCK_TERMS = 2048;
const DF_SPILL_TERMS = 4_000_000;
const DF_READER_CACHE_BLOCKS = 64;
const RUN_READ_BUFFER_BYTES = 1024 * 1024;

function compareTerms(a, b) {
  return a < b ? -1 : a > b ? 1 : 0;
}

function writeVarintTo(bytes, value) {
  // Arithmetic, not bitwise: df values live in full double range (bitwise
  // ops truncate to 32 bits, silently corrupting planet-scale counts).
  let remaining = value;
  while (remaining >= 0x80) {
    bytes.push((remaining % 0x80) | 0x80);
    remaining = Math.floor(remaining / 0x80);
  }
  bytes.push(remaining);
}

function readVarintAt(buffer, offset) {
  let value = 0;
  let shift = 0;
  let position = offset;
  for (;;) {
    const byte = buffer[position++];
    value += (byte & 0x7f) * 2 ** shift;
    if ((byte & 0x80) === 0) return [value, position];
    shift += 7;
  }
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function encodeDfEntries(entries) {
  const bytes = [];
  for (const [term, df] of entries) {
    const termBytes = textEncoder.encode(term);
    writeVarintTo(bytes, termBytes.length);
    for (const byte of termBytes) bytes.push(byte);
    writeVarintTo(bytes, df);
  }
  return Buffer.from(bytes);
}

function decodeDfEntries(buffer, onEntry) {
  let offset = 0;
  while (offset < buffer.length) {
    let length;
    [length, offset] = readVarintAt(buffer, offset);
    const term = textDecoder.decode(buffer.subarray(offset, offset + length));
    offset += length;
    let df;
    [df, offset] = readVarintAt(buffer, offset);
    onEntry(term, df);
  }
}

// --- df file writer -------------------------------------------------------

// rfdf-v1 layout: [4B LE header length][JSON header][gzipped blocks].
// Block offsets are relative to the end of the header; each block holds
// code-unit-sorted (varint termLen, utf8 term, varint df) entries. The
// header carries each block's first term, so lookups binary-search the
// block table and touch one block.
function createDfFileWriter(outPath, blockTerms = DF_BLOCK_TERMS) {
  const dataPath = `${outPath}.data`;
  const dataFd = openSync(dataPath, "w");
  const blocks = [];
  let pending = [];
  let dataOffset = 0;
  let terms = 0;

  function flushBlock() {
    if (!pending.length) return;
    const payload = gzipSync(encodeDfEntries(pending), { level: 6 });
    writeSync(dataFd, payload);
    blocks.push({ first: pending[0][0], offset: dataOffset, length: payload.length, terms: pending.length });
    dataOffset += payload.length;
    terms += pending.length;
    pending = [];
  }

  return {
    add(term, df) {
      pending.push([term, df]);
      if (pending.length >= blockTerms) flushBlock();
    },
    finish() {
      flushBlock();
      closeSync(dataFd);
      const header = Buffer.from(JSON.stringify({
        format: SCORING_DF_FORMAT,
        terms,
        blocks
      }), "utf8");
      const outFd = openSync(outPath, "w");
      const lengthPrefix = Buffer.alloc(4);
      lengthPrefix.writeUInt32LE(header.length, 0);
      writeSync(outFd, lengthPrefix);
      writeSync(outFd, header);
      const copyBuffer = Buffer.alloc(8 * 1024 * 1024);
      const inFd = openSync(dataPath, "r");
      for (;;) {
        const read = readSync(inFd, copyBuffer, 0, copyBuffer.length, null);
        if (read <= 0) break;
        writeSync(outFd, copyBuffer.subarray(0, read));
      }
      closeSync(inFd);
      closeSync(outFd);
      rmSync(dataPath, { force: true });
      return { terms, blocks: blocks.length };
    }
  };
}

// --- df file reader -------------------------------------------------------

export function openDfFile(path, options = {}) {
  const fd = openSync(path, "r");
  const lengthPrefix = Buffer.alloc(4);
  readSync(fd, lengthPrefix, 0, 4, 0);
  const headerLength = lengthPrefix.readUInt32LE(0);
  const headerBuffer = Buffer.alloc(headerLength);
  readSync(fd, headerBuffer, 0, headerLength, 4);
  const header = JSON.parse(headerBuffer.toString("utf8"));
  if (header.format !== SCORING_DF_FORMAT) {
    throw new Error(`Rangefind scoring stats: unsupported df file format "${header.format}" at ${path}.`);
  }
  const dataStart = 4 + headerLength;
  const blocks = header.blocks;
  const cacheBlocks = Math.max(1, Math.floor(Number(options.cacheBlocks || DF_READER_CACHE_BLOCKS)));
  const cache = new Map();

  function loadBlock(index) {
    const cached = cache.get(index);
    if (cached) {
      // Refresh insertion order so the Map doubles as an LRU.
      cache.delete(index);
      cache.set(index, cached);
      return cached;
    }
    const block = blocks[index];
    const compressed = Buffer.alloc(block.length);
    readSync(fd, compressed, 0, block.length, dataStart + block.offset);
    const entries = new Map();
    decodeDfEntries(gunzipSync(compressed), (term, df) => entries.set(term, df));
    cache.set(index, entries);
    if (cache.size > cacheBlocks) cache.delete(cache.keys().next().value);
    return entries;
  }

  function blockIndexFor(term) {
    let low = 0;
    let high = blocks.length - 1;
    let found = -1;
    while (low <= high) {
      const mid = (low + high) >> 1;
      if (blocks[mid].first <= term) {
        found = mid;
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return found;
  }

  return {
    terms: header.terms,
    lookup(term) {
      if (!blocks.length) return undefined;
      const index = blockIndexFor(String(term));
      if (index < 0) return undefined;
      return loadBlock(index).get(term);
    },
    close() {
      closeSync(fd);
      cache.clear();
    }
  };
}

// --- codec provider -------------------------------------------------------

const dfReaders = new Map();

export function scoringDfLookup(path, term) {
  let reader = dfReaders.get(path);
  if (!reader) {
    reader = openDfFile(path);
    dfReaders.set(path, reader);
  }
  return reader.lookup(term);
}

// The codec stays platform-neutral (it also ships to browsers), so the
// node-side df reader is injected instead of imported there. Both the
// builder main thread and every reduce worker install this at module load.
export function installScoringDfProvider() {
  setScoringDfProvider(scoringDfLookup);
}

export function closeScoringDfReaders() {
  for (const reader of dfReaders.values()) reader.close();
  dfReaders.clear();
}

// A rewritten df file at a cached path must drop its stale reader (block
// offsets shift), or a later same-process build reads garbage frequencies.
function invalidateScoringDfReader(path) {
  const reader = dfReaders.get(path);
  if (reader) {
    reader.close();
    dfReaders.delete(path);
  }
}

// --- spillable df accumulation --------------------------------------------

function readVarintBounded(buffer, offset) {
  let value = 0;
  let shift = 0;
  let position = offset;
  for (;;) {
    if (position >= buffer.length) return null;
    const byte = buffer[position++];
    value += (byte & 0x7f) * 2 ** shift;
    if ((byte & 0x80) === 0) return [value, position];
    shift += 7;
  }
}

function createRunReader(path) {
  const fd = openSync(path, "r");
  let buffer = Buffer.alloc(0);
  let position = 0;
  let exhausted = false;
  let current = null;

  function refill() {
    const chunk = Buffer.alloc(RUN_READ_BUFFER_BYTES);
    const read = readSync(fd, chunk, 0, chunk.length, position);
    position += read;
    if (read < chunk.length) exhausted = true;
    if (read > 0) {
      buffer = buffer.length ? Buffer.concat([buffer, chunk.subarray(0, read)]) : chunk.subarray(0, read);
    }
  }

  function tryDecodeEntry() {
    const lengthRead = readVarintBounded(buffer, 0);
    if (!lengthRead) return null;
    const [length, termStart] = lengthRead;
    if (termStart + length > buffer.length) return null;
    const dfRead = readVarintBounded(buffer, termStart + length);
    if (!dfRead) return null;
    const term = textDecoder.decode(buffer.subarray(termStart, termStart + length));
    buffer = buffer.subarray(dfRead[1]);
    return [term, dfRead[0]];
  }

  function advance() {
    for (;;) {
      const entry = tryDecodeEntry();
      if (entry) {
        current = entry;
        return;
      }
      if (exhausted) {
        if (buffer.length) throw new Error(`Rangefind scoring stats: truncated df run at ${path}.`);
        current = null;
        closeSync(fd);
        return;
      }
      refill();
    }
  }

  advance();
  return {
    peek: () => current,
    next() {
      const entry = current;
      advance();
      return entry;
    }
  };
}

function mergeSortedRuns(readers, onEntry) {
  // Binary min-heap over run cursors: planet-scale merges see dozens of
  // runs, so per-entry selection must be O(log k), not O(k).
  const heap = readers.filter(reader => reader.peek());
  const less = (a, b) => compareTerms(a.peek()[0], b.peek()[0]) < 0;
  const siftDown = (start) => {
    let index = start;
    for (;;) {
      const left = index * 2 + 1;
      const right = left + 1;
      let smallest = index;
      if (left < heap.length && less(heap[left], heap[smallest])) smallest = left;
      if (right < heap.length && less(heap[right], heap[smallest])) smallest = right;
      if (smallest === index) return;
      [heap[index], heap[smallest]] = [heap[smallest], heap[index]];
      index = smallest;
    }
  };
  for (let i = (heap.length >> 1) - 1; i >= 0; i--) siftDown(i);

  let currentTerm = null;
  let currentDf = 0;
  while (heap.length) {
    const [term, df] = heap[0].next();
    if (term === currentTerm) {
      currentDf += df;
    } else {
      if (currentTerm !== null) onEntry(currentTerm, currentDf);
      currentTerm = term;
      currentDf = df;
    }
    if (heap[0].peek()) {
      siftDown(0);
    } else {
      heap[0] = heap[heap.length - 1];
      heap.pop();
      siftDown(0);
    }
  }
  if (currentTerm !== null) onEntry(currentTerm, currentDf);
}

function createDfAccumulator(runDir, spillTerms = DF_SPILL_TERMS) {
  const map = new Map();
  const runs = [];

  function spill() {
    if (!map.size) return;
    mkdirSync(runDir, { recursive: true });
    const entries = [...map.entries()].sort((a, b) => compareTerms(a[0], b[0]));
    const path = resolve(runDir, `df-run-${String(runs.length).padStart(4, "0")}.bin`);
    const fd = openSync(path, "w");
    // Encode in bounded slices so a 4M-term spill never materializes one
    // giant byte array.
    const sliceTerms = 65536;
    for (let start = 0; start < entries.length; start += sliceTerms) {
      writeSync(fd, encodeDfEntries(entries.slice(start, start + sliceTerms)));
    }
    closeSync(fd);
    runs.push(path);
    map.clear();
  }

  return {
    addMany(terms, dfs) {
      for (let i = 0; i < terms.length; i++) {
        const term = terms[i];
        map.set(term, (map.get(term) || 0) + dfs[i]);
      }
      if (map.size >= spillTerms) spill();
    },
    finish(writer) {
      if (!runs.length) {
        const entries = [...map.entries()].sort((a, b) => compareTerms(a[0], b[0]));
        for (const [term, df] of entries) writer.add(term, df);
        map.clear();
        return;
      }
      spill();
      mergeSortedRuns(runs.map(createRunReader), (term, df) => writer.add(term, df));
      rmSync(runDir, { recursive: true, force: true });
    }
  };
}

// --- corpus passes ---------------------------------------------------------

function statsWorkerCount(config) {
  return Math.max(1, Math.floor(Number(config.scanWorkers || 1)));
}

function statsBatchDocs(config) {
  return Math.max(512, Math.floor(Number(config.scanBatchDocs || 128)) * 4);
}

function mergeBbox(target, bbox) {
  if (!bbox) return target;
  if (!target) return bbox.slice();
  target[0] = Math.min(target[0], bbox[0]);
  target[1] = Math.min(target[1], bbox[1]);
  target[2] = Math.max(target[2], bbox[2]);
  target[3] = Math.max(target[3], bbox[3]);
  return target;
}

async function eachInputBatch(input, batchDocs, onBatch) {
  const rl = createInterface({ input: createReadStream(input), crlfDelay: Infinity });
  let batch = [];
  for await (const line of rl) {
    if (!line.trim()) continue;
    batch.push(line);
    if (batch.length >= batchDocs) {
      await onBatch(batch);
      batch = [];
    }
  }
  if (batch.length) await onBatch(batch);
}

async function runStatsPass(input, config, options, onSummary) {
  const workerCount = statsWorkerCount(config);
  const batchDocs = statsBatchDocs(config);
  if (workerCount <= 1) {
    await eachInputBatch(input, batchDocs, async lines => {
      onSummary(computeScoringStatsBatch(lines, config, options));
    });
    return;
  }
  const workers = Array.from({ length: workerCount }, () =>
    new Worker(new URL("./scoring_stats_worker.js", import.meta.url), { type: "module" }));

  function postToWorker(worker, message) {
    return new Promise((resolveBatch, rejectBatch) => {
      function cleanup() {
        worker.off("message", onMessage);
        worker.off("error", onError);
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
      worker.on("message", onMessage);
      worker.once("error", onError);
      worker.postMessage(message);
    });
  }

  const available = workers.slice();
  const active = new Set();
  let nextBatch = 0;
  // Batch failures are captured, not left to reject unobserved — a rejection
  // landing while the loop awaits readline I/O would otherwise become an
  // unhandled rejection instead of failing the pass.
  let failure = null;
  try {
    // The config crosses into each worker once; batches only carry lines.
    await Promise.all(workers.map(worker => postToWorker(worker, { kind: "init", id: -1, config, options })));
    await eachInputBatch(input, batchDocs, async lines => {
      if (failure) throw failure;
      while (!available.length) await Promise.race(active);
      const worker = available.pop();
      const promise = postToWorker(worker, { id: nextBatch++, lines })
        .then(response => {
          onSummary(response);
        })
        .catch(error => {
          failure = failure || error;
        })
        .finally(() => {
          active.delete(promise);
          available.push(worker);
        });
      active.add(promise);
    });
    while (active.size) await Promise.race(active);
    if (failure) throw failure;
  } finally {
    await Promise.allSettled(workers.map(worker => worker.terminate()));
  }
}

function normalizeStatsInputs(inputs) {
  return (inputs || []).map((item, index) => {
    if (typeof item === "string") return { id: `shard-${index}`, input: resolve(item) };
    if (!item?.input) throw new Error("Rangefind scoring stats: every input needs an `input` JSONL path.");
    return { id: String(item.id || `shard-${index}`), input: resolve(item.input) };
  });
}

// Collects frozen scoring statistics over every shard input.
//
// Pass A tokenizes each corpus for document totals and per-field token
// totals (the avgdl inputs). Pass B re-runs term selection with the frozen
// global average lengths — selection depends on them — and counts per-term
// document frequencies, spilling sorted runs to disk so the term space never
// has to fit in one heap. Both passes fan out to worker threads.
export async function collectScoringStats(options = {}) {
  const config = options.config;
  if (!config || !Array.isArray(config.fields)) {
    throw new Error("Rangefind scoring stats: pass a resolved rangefind config (with fields).");
  }
  const inputs = normalizeStatsInputs(options.inputs);
  if (!inputs.length) throw new Error("Rangefind scoring stats: no inputs.");
  const outDir = resolve(options.outDir || "scoring-stats");
  mkdirSync(outDir, { recursive: true });
  const statsPath = resolve(outDir, options.statsFileName || "scoring-stats.json");
  const dfFileName = options.dfFileName || "scoring-stats-df.bin";
  const dfPath = resolve(outDir, dfFileName);
  const log = options.log || (() => {});

  // Pass A: corpus totals.
  let total = 0;
  const fieldTotals = Object.fromEntries(config.fields.map(field => [field.name, 0]));
  const perInput = inputs.map(item => ({ ...item, total: 0, bbox: null }));
  for (const item of perInput) {
    await runStatsPass(item.input, config, { mode: "totals" }, summary => {
      item.total += summary.total || 0;
      for (const field of config.fields) {
        fieldTotals[field.name] += summary.fieldTotals?.[field.name] || 0;
      }
      item.bbox = mergeBbox(item.bbox, summary.bbox);
    });
    total += item.total;
    log(`scoring-stats: measured ${item.id} (${item.total.toLocaleString()} docs)`);
  }
  const avgLens = Object.fromEntries(config.fields.map(field => [
    field.name,
    Math.max(1, fieldTotals[field.name] / Math.max(1, total))
  ]));

  // Pass B: document frequencies under the frozen average lengths.
  const accumulator = createDfAccumulator(resolve(outDir, "_df-runs"), options.spillTerms);
  for (const item of perInput) {
    await runStatsPass(item.input, config, { mode: "df", avgLens }, summary => {
      accumulator.addMany(summary.terms || [], summary.dfs || []);
    });
    log(`scoring-stats: counted df for ${item.id}`);
  }
  const writer = createDfFileWriter(dfPath, options.blockTerms);
  accumulator.finish(writer);
  const dfSummary = writer.finish();
  invalidateScoringDfReader(dfPath);

  const stats = {
    format: SCORING_STATS_FORMAT,
    total,
    field_totals: fieldTotals,
    avg_field_lengths: avgLens,
    df_file: dfFileName,
    df_terms: dfSummary.terms,
    analysis: analyzerForConfig(config).profile,
    inputs: perInput.map(item => ({ id: item.id, input: item.input, total: item.total, bbox: item.bbox }))
  };
  writeFileSync(statsPath, JSON.stringify(stats, null, 2));
  log(`scoring-stats: ${total.toLocaleString()} docs, ${dfSummary.terms.toLocaleString()} terms`);
  return { statsPath, dfPath, stats };
}

export function loadScoringStats(statsPath) {
  const path = resolve(statsPath);
  const stats = JSON.parse(readFileSync(path, "utf8"));
  if (stats.format !== SCORING_STATS_FORMAT) {
    throw new Error(`Rangefind scoring stats: unsupported format "${stats.format}" at ${path}.`);
  }
  return {
    ...stats,
    dfPath: stats.df_file ? resolve(dirname(path), stats.df_file) : ""
  };
}
