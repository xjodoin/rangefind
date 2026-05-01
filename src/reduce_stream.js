import { closeSync, mkdirSync, openSync, rmSync, writeSync } from "node:fs";
import { resolve } from "node:path";
import { encodeRunRecord, readRunRecords } from "./runs.js";
import { shardKey } from "./shards.js";

const RUN_SCHEMA = ["string", "number", "number"];

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

async function createChunkReader(path) {
  const iterator = readRunRecords(path, RUN_SCHEMA)[Symbol.asyncIterator]();
  const first = await iterator.next();
  return { iterator, record: first.done ? null : first.value };
}

async function* mergeSortedChunks(chunks) {
  const readers = await Promise.all(chunks.map(createChunkReader));
  try {
    while (readers.some(reader => reader.record)) {
      let best = -1;
      for (let i = 0; i < readers.length; i++) {
        if (!readers[i].record) continue;
        if (best < 0 || compareRecords(readers[i].record, readers[best].record) < 0) best = i;
      }
      const record = readers[best].record;
      const next = await readers[best].iterator.next();
      readers[best].record = next.done ? null : next.value;
      yield record;
    }
  } finally {
    await Promise.allSettled(readers.map(reader => reader.iterator.return?.()));
  }
}

async function* reducedTerms(chunks, options = {}) {
  const emitRows = options.emitRows !== false;
  let currentTerm = null;
  let currentDoc = null;
  let currentScore = 0;
  let rows = [];
  let df = 0;

  function finishDoc() {
    if (currentDoc == null) return;
    df++;
    if (emitRows) rows.push([currentDoc, currentScore]);
  }

  function finishTerm() {
    if (currentTerm == null) return null;
    finishDoc();
    const item = emitRows ? { term: currentTerm, rows, df } : { term: currentTerm, df };
    rows = [];
    df = 0;
    currentDoc = null;
    currentScore = 0;
    return item;
  }

  for await (const [term, doc, score] of mergeSortedChunks(chunks)) {
    if (term !== currentTerm) {
      const item = finishTerm();
      if (item) yield item;
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
  const item = finishTerm();
  if (item) yield item;
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
  const prefixCounts = new Map();
  let terms = 0;
  let postings = 0;
  let sequence = 0;
  try {
    for await (const { term, df } of reducedTerms(chunks, { emitRows: false })) {
      terms++;
      postings += df;
      addPrefixCounts(prefixCounts, term, df, config);
      onTerm?.(term, df);
    }

    const partitions = [];
    let currentName = null;
    let entries = [];
    for await (const { term, rows } of reducedTerms(chunks, { emitRows: true })) {
      const name = partitionNameForTerm(term, prefixCounts, config);
      if (currentName && name !== currentName) {
        partitions.push(await onPartition({ name: currentName, entries }, sequence++));
        entries = [];
      }
      currentName = name;
      entries.push([term, rows]);
    }
    if (currentName) partitions.push(await onPartition({ name: currentName, entries }, sequence++));
    return { terms, postings, partitions };
  } finally {
    rmSync(scratchDir, { recursive: true, force: true });
  }
}
