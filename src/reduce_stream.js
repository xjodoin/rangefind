import { closeSync, createReadStream, mkdirSync, openSync, rmSync, writeSync } from "node:fs";
import { resolve } from "node:path";
import { encodeRunRecord, readRunRecords, tryReadVarint, varintLength, writeVarint } from "./runs.js";
import { shardKey } from "./shards.js";

const RUN_SCHEMA = ["string", "number", "number"];
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function compareTerms(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function compareRecords(left, right) {
  return compareTerms(left[0], right[0]) || left[1] - right[1] || left[2] - right[2];
}

function estimateRecordBytes(record) {
  return String(record[0] || "").length + 16;
}

function sortedChunkLimits(config) {
  return {
    records: Math.max(1, Math.floor(Number(config.reduceSortChunkRecords || 25000))),
    bytes: Math.max(1024, Math.floor(Number(config.reduceSortChunkBytes || 4 * 1024 * 1024)))
  };
}

function chunkPath(outDir, index) {
  return resolve(outDir, `${String(index).padStart(5, "0")}.run`);
}

function writeSortedChunk(records, outDir, index) {
  records.sort(compareRecords);
  const path = chunkPath(outDir, index);
  const fd = openSync(path, "w");
  try {
    for (const record of records) {
      const bytes = encodeRunRecord(RUN_SCHEMA, record);
      writeSync(fd, bytes, 0, bytes.length);
    }
  } finally {
    closeSync(fd);
  }
  records.length = 0;
  return path;
}

async function writeSortedChunks(runPath, scratchDir, config) {
  mkdirSync(scratchDir, { recursive: true });
  const limits = sortedChunkLimits(config);
  const chunks = [];
  const records = [];
  let bytes = 0;
  for await (const record of readRunRecords(runPath, RUN_SCHEMA)) {
    if (!record[0]) continue;
    records.push(record);
    bytes += estimateRecordBytes(record);
    if (records.length >= limits.records || bytes >= limits.bytes) {
      chunks.push(writeSortedChunk(records, scratchDir, chunks.length));
      bytes = 0;
    }
  }
  if (records.length) chunks.push(writeSortedChunk(records, scratchDir, chunks.length));
  return chunks;
}

class MinHeap {
  constructor(compare) {
    this.compare = compare;
    this.items = [];
  }

  push(item) {
    const items = this.items;
    items.push(item);
    let index = items.length - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (this.compare(items[parent], items[index]) <= 0) break;
      const value = items[parent];
      items[parent] = items[index];
      items[index] = value;
      index = parent;
    }
  }

  pop() {
    const items = this.items;
    if (!items.length) return null;
    const out = items[0];
    const last = items.pop();
    if (items.length) {
      items[0] = last;
      let index = 0;
      while (true) {
        const left = index * 2 + 1;
        const right = left + 1;
        let best = index;
        if (left < items.length && this.compare(items[left], items[best]) < 0) best = left;
        if (right < items.length && this.compare(items[right], items[best]) < 0) best = right;
        if (best === index) break;
        const value = items[index];
        items[index] = items[best];
        items[best] = value;
        index = best;
      }
    }
    return out;
  }

  get size() {
    return this.items.length;
  }
}

async function createChunkReader(path, index) {
  const iterator = readRunRecords(path, RUN_SCHEMA)[Symbol.asyncIterator]();
  const first = await iterator.next();
  return { index, iterator, record: first.done ? null : first.value };
}

async function* mergeSortedChunks(chunks) {
  const readers = await Promise.all(chunks.map((path, index) => createChunkReader(path, index)));
  const heap = new MinHeap((left, right) => compareRecords(left.record, right.record) || left.reader - right.reader);
  try {
    for (const reader of readers) {
      if (reader.record) heap.push({ reader: reader.index, record: reader.record });
    }
    while (heap.size) {
      const item = heap.pop();
      const reader = readers[item.reader];
      const record = item.record;
      const next = await reader.iterator.next();
      if (!next.done) heap.push({ reader: reader.index, record: next.value });
      yield record;
    }
  } finally {
    await Promise.allSettled(readers.map(reader => reader.iterator.return?.()));
  }
}

function encodeReducedTerm(term, rows, df) {
  const termBytes = textEncoder.encode(String(term || ""));
  let bytes = varintLength(termBytes.length) + termBytes.length + varintLength(df) + varintLength(rows.length);
  for (const [doc, score] of rows) bytes += varintLength(doc) + varintLength(score);
  const out = Buffer.allocUnsafe(bytes);
  let pos = writeVarint(out, 0, termBytes.length);
  out.set(termBytes, pos);
  pos += termBytes.length;
  pos = writeVarint(out, pos, df);
  pos = writeVarint(out, pos, rows.length);
  for (const [doc, score] of rows) {
    pos = writeVarint(out, pos, doc);
    pos = writeVarint(out, pos, score);
  }
  return out;
}

function reducedTermFromBytes(bytes, state) {
  const start = state.pos;
  const termLength = tryReadVarint(bytes, state);
  if (termLength == null || state.pos + termLength > bytes.length) {
    state.pos = start;
    return null;
  }
  const term = textDecoder.decode(bytes.subarray(state.pos, state.pos + termLength));
  state.pos += termLength;
  const df = tryReadVarint(bytes, state);
  if (df == null) {
    state.pos = start;
    return null;
  }
  const rowCount = tryReadVarint(bytes, state);
  if (rowCount == null) {
    state.pos = start;
    return null;
  }
  const rows = new Array(rowCount);
  for (let i = 0; i < rowCount; i++) {
    const doc = tryReadVarint(bytes, state);
    const score = tryReadVarint(bytes, state);
    if (doc == null || score == null) {
      state.pos = start;
      return null;
    }
    rows[i] = [doc, score];
  }
  return { term, df, rows };
}

async function* readReducedTerms(path) {
  let pending = Buffer.alloc(0);
  for await (const chunk of createReadStream(path)) {
    const bytes = pending.length ? Buffer.concat([pending, chunk]) : chunk;
    const state = { pos: 0 };
    while (state.pos < bytes.length) {
      const item = reducedTermFromBytes(bytes, state);
      if (!item) break;
      yield item;
    }
    pending = state.pos < bytes.length ? bytes.subarray(state.pos) : Buffer.alloc(0);
  }
  if (pending.length) throw new Error(`Truncated Rangefind reduced term file: ${path}`);
}

async function reduceChunksToTermSpool(chunks, reducedPath, options = {}) {
  let currentTerm = null;
  let currentDoc = null;
  let currentScore = 0;
  let rows = [];
  let df = 0;
  let terms = 0;
  let postings = 0;
  const prefixCounts = new Map();
  const fd = openSync(reducedPath, "w");

  function finishDoc() {
    if (currentDoc == null) return;
    df++;
    rows.push([currentDoc, currentScore]);
  }

  function finishTerm() {
    if (currentTerm == null) return null;
    finishDoc();
    const item = { term: currentTerm, rows, df };
    const encoded = encodeReducedTerm(item.term, item.rows, item.df);
    writeSync(fd, encoded, 0, encoded.length);
    terms++;
    postings += item.df;
    addPrefixCounts(prefixCounts, item.term, item.df, options.config);
    options.onTerm?.(item.term, item.df);
    rows = [];
    df = 0;
    currentDoc = null;
    currentScore = 0;
    return item;
  }

  try {
    for await (const [term, doc, score] of mergeSortedChunks(chunks)) {
      if (term !== currentTerm) {
        finishTerm();
        currentTerm = term;
        currentDoc = doc;
        currentScore = score;
        continue;
      }
      if (doc !== currentDoc) {
        finishDoc();
        currentDoc = doc;
        currentScore = score;
        continue;
      }
      currentScore += score;
    }
    finishTerm();
  } finally {
    closeSync(fd);
  }
  return { terms, postings, prefixCounts };
}

function prefixKey(term, depth) {
  return `${depth}\u0000${shardKey(term, depth)}`;
}

function addPrefixCounts(prefixCounts, term, df, config) {
  const baseDepth = Math.max(1, Math.floor(Number(config.baseShardDepth || 1)));
  const maxDepth = Math.max(baseDepth, Math.floor(Number(config.maxShardDepth || baseDepth)));
  for (let depth = baseDepth; depth <= maxDepth; depth++) {
    const key = prefixKey(term, depth);
    prefixCounts.set(key, (prefixCounts.get(key) || 0) + df);
  }
}

function partitionNameForTerm(term, prefixCounts, config) {
  const target = Math.max(1, Math.floor(Number(config.targetShardPostings || 1)));
  const baseDepth = Math.max(1, Math.floor(Number(config.baseShardDepth || 1)));
  const maxDepth = Math.max(baseDepth, Math.floor(Number(config.maxShardDepth || baseDepth)));
  let depth = baseDepth;
  while (depth < maxDepth && (prefixCounts.get(prefixKey(term, depth)) || 0) > target) depth++;
  return shardKey(term, depth);
}

export async function reduceRunToPartitions(options) {
  const {
    runPath,
    scratchDir,
    config,
    onTerm,
    onPartition
  } = options;
  const chunks = await writeSortedChunks(runPath, scratchDir, config);
  const reducedPath = resolve(scratchDir, "reduced-terms.run");
  let sequence = 0;
  try {
    const reduced = await reduceChunksToTermSpool(chunks, reducedPath, { config, onTerm });

    const partitions = [];
    let currentName = null;
    let entries = [];
    for await (const { term, rows } of readReducedTerms(reducedPath)) {
      const name = partitionNameForTerm(term, reduced.prefixCounts, config);
      if (currentName && name !== currentName) {
        partitions.push(await onPartition({ name: currentName, entries }, sequence++));
        entries = [];
      }
      currentName = name;
      entries.push([term, rows]);
    }
    if (currentName) partitions.push(await onPartition({ name: currentName, entries }, sequence++));
    return { terms: reduced.terms, postings: reduced.postings, partitions };
  } finally {
    rmSync(scratchDir, { recursive: true, force: true });
  }
}
