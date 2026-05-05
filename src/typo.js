import { appendFileSync, closeSync, mkdirSync, openSync, rmSync, writeFileSync, writeSync } from "node:fs";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { surfaceStemPairs } from "./analyzer.js";
import { TYPO_SHARD_MAGIC, pushVarint, readVarint } from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";
import { writeDirectoryFiles } from "./directory_writer.js";
import { OBJECT_NAME_HASH_LENGTH } from "./object_store.js";
import { createPackWriter, finalizePackWriter, writePackedShard } from "./packs.js";
import { encodeRunRecord, readRunRecords } from "./runs.js";
import { buildTypoLexiconShard, typoLexiconShardKey, TYPO_LEXICON_FORMAT } from "./typo_lexicon.js";

const SEPARATOR = "\u0001";

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

export const TYPO_DEFAULTS = {
  enabled: true,
  minTermLength: 5,
  minSurfaceLength: 4,
  maxSurfaceLength: 24,
  maxTermLength: 20,
  minDf: 1,
  maxDfRatio: 0.08,
  maxEdits: 2,
  deleteMaxSurfaceLength: 8,
  lexiconEnabled: true,
  lexiconMinSurfaceLength: 8,
  lexiconShardDepth: 2,
  lexiconPackBytes: 4 * 1024 * 1024,
  lexiconDirectoryPageBytes: 64 * 1024,
  lexiconMaxScanCandidates: 2048,
  baseShardDepth: 2,
  maxShardDepth: 3,
  targetShardCandidates: 12000,
  maxCandidatesPerDelete: 32,
  packBytes: 4 * 1024 * 1024,
  directoryPageBytes: 64 * 1024,
  flushLines: 200000
};

export function typoOptions(config) {
  if (config.typo === false || config.typo?.enabled === false) return { ...TYPO_DEFAULTS, enabled: false };
  return { ...TYPO_DEFAULTS, ...(typeof config.typo === "object" ? config.typo : {}) };
}

export function typoMaxEditsFor(term, options = TYPO_DEFAULTS) {
  const max = options.maxEdits ?? TYPO_DEFAULTS.maxEdits;
  return term.length >= 8 ? max : Math.min(1, max);
}

export function typoDeleteKeys(term, options = TYPO_DEFAULTS, maxEdits = typoMaxEditsFor(term, options)) {
  const minLength = (options.minTermLength ?? TYPO_DEFAULTS.minTermLength) - (options.maxEdits ?? TYPO_DEFAULTS.maxEdits);
  const keys = new Set([term]);
  let frontier = new Set([term]);
  for (let edits = 1; edits <= maxEdits; edits++) {
    const next = new Set();
    for (const value of frontier) {
      if (value.length <= 1) continue;
      for (let i = 0; i < value.length; i++) {
        const key = value.slice(0, i) + value.slice(i + 1);
        if (key.length < minLength) continue;
        keys.add(key);
        next.add(key);
      }
    }
    frontier = next;
  }
  return keys;
}

function typoShardKey(deleteKey, options, depth = options.baseShardDepth) {
  return String(deleteKey || "").slice(0, depth).padEnd(depth, "_");
}

export function createTypoRunBuffer(runsOut, options) {
  mkdirSync(runsOut, { recursive: true });
  return {
    byShard: new Map(),
    lexiconByShard: new Map(),
    lines: 0,
    lexiconLines: 0,
    runsOut,
    options,
    terms: 0,
    deletePairs: 0,
    lexiconPairs: 0,
    shards: new Set(),
    lexiconShards: new Set()
  };
}

export function flushTypoBuffer(buffer) {
  if (!buffer) return;
  for (const [shard, records] of buffer.byShard) {
    if (!records.length) continue;
    appendFileSync(resolve(buffer.runsOut, `${shard}.run`), Buffer.concat(records));
  }
  buffer.byShard.clear();
  for (const [shard, records] of buffer.lexiconByShard) {
    if (!records.length) continue;
    appendFileSync(resolve(buffer.runsOut, `${shard}.lex.run`), Buffer.concat(records));
  }
  buffer.lexiconByShard.clear();
  buffer.lines = 0;
  buffer.lexiconLines = 0;
}

function bufferTypoCandidate(buffer, deleteKey, text, df) {
  const shard = typoShardKey(deleteKey, buffer.options);
  if (!buffer.byShard.has(shard)) buffer.byShard.set(shard, []);
  buffer.byShard.get(shard).push(encodeRunRecord(["string", "string", "number"], [deleteKey, text, df]));
  buffer.shards.add(shard);
  buffer.lines++;
  buffer.deletePairs++;
  if (buffer.lines >= buffer.options.flushLines) flushTypoBuffer(buffer);
}

function shouldWriteTypoLexicon(surface, options) {
  return options.lexiconEnabled !== false && surface.length >= Math.max(1, Number(options.lexiconMinSurfaceLength || 1));
}

function shouldWriteTypoDeletes(surface, options) {
  return surface.length <= Math.max(1, Number(options.deleteMaxSurfaceLength || 1));
}

function bufferTypoLexiconCandidate(buffer, surface, term, df) {
  if (!shouldWriteTypoLexicon(surface, buffer.options)) return;
  const shard = typoLexiconShardKey(surface, buffer.options.lexiconShardDepth);
  if (!buffer.lexiconByShard.has(shard)) buffer.lexiconByShard.set(shard, []);
  buffer.lexiconByShard.get(shard).push(encodeRunRecord(["string", "string", "number"], [surface, term, df]));
  buffer.lexiconShards.add(shard);
  buffer.lexiconLines++;
  buffer.lexiconPairs++;
  if (buffer.lexiconLines >= buffer.options.flushLines) flushTypoBuffer(buffer);
}

function bufferTypoCorrectionCandidate(buffer, surface, term, df) {
  bufferTypoLexiconCandidate(buffer, surface, term, df);
  if (!shouldWriteTypoDeletes(surface, buffer.options) && shouldWriteTypoLexicon(surface, buffer.options)) return;
  for (const deleteKey of typoDeleteKeys(surface, buffer.options, typoMaxEditsFor(surface, buffer.options))) {
    bufferTypoCandidate(buffer, deleteKey, surface === term ? term : `${surface}${SEPARATOR}${term}`, df);
  }
}

function isTypoCandidate(surface, term, options) {
  if (surface.length < options.minSurfaceLength || surface.length > options.maxSurfaceLength) return false;
  if (term.length < 2 || term.length > options.maxTermLength) return false;
  if (term.includes("_") || term.startsWith("n_")) return false;
  if (!/^[a-z][a-z0-9]*$/u.test(surface) || !/^[a-z][a-z0-9]*$/u.test(term)) return false;
  if (/^\d+$/u.test(surface) || /^\d+$/u.test(term)) return false;
  return true;
}

export function addTypoSurfacePairs(buffer, pairs) {
  if (!buffer) return;
  const seen = new Set();
  for (const [surface, term] of pairs) {
    const key = `${surface}${SEPARATOR}${term}`;
    if (seen.has(key) || !isTypoCandidate(surface, term, buffer.options)) continue;
    seen.add(key);
    buffer.terms++;
    bufferTypoCorrectionCandidate(buffer, surface, term, 1);
  }
}

export function createTypoSurfacePairBuffer() {
  return new Map();
}

export function addTypoSurfacePairsToBuffer(pairBuffer, pairs) {
  if (!pairBuffer) return;
  for (const [surface, term] of pairs) {
    pairBuffer.set(`${surface}${SEPARATOR}${term}`, [surface, term]);
  }
}

export function flushTypoSurfacePairBuffer(buffer, pairBuffer) {
  if (!pairBuffer?.size) return;
  addTypoSurfacePairs(buffer, pairBuffer.values());
  pairBuffer.clear();
}

export function surfacePairsForFields(doc, fields, fieldText) {
  const pairs = new Map();
  for (const field of fields) {
    if (field.typo === false) continue;
    for (const [surface, term] of surfaceStemPairs(fieldText(doc, field))) {
      pairs.set(`${surface}${SEPARATOR}${term}`, [surface, term]);
    }
  }
  return pairs.values();
}

function isTypoIndexTerm(term, df, total, options) {
  if (term.length < options.minTermLength || term.length > options.maxTermLength) return false;
  if (df < options.minDf || df > Math.max(options.minDf, Math.floor(total * options.maxDfRatio))) return false;
  if (term.includes("_") || term.startsWith("n_")) return false;
  if (!/^[a-z][a-z0-9]*$/u.test(term) || /^\d+$/u.test(term)) return false;
  return true;
}

export function addTypoIndexTerm(buffer, term, df, total) {
  if (!buffer || !isTypoIndexTerm(term, df, total, buffer.options)) return false;
  buffer.terms++;
  bufferTypoCorrectionCandidate(buffer, term, term, df);
  return true;
}

function parseCandidateText(text) {
  const index = text.indexOf(SEPARATOR);
  if (index < 0) return { surface: text, term: text };
  return { surface: text.slice(0, index), term: text.slice(index + SEPARATOR.length) };
}

function commonPrefixLength(a, b) {
  const max = Math.min(a.length, b.length);
  let i = 0;
  while (i < max && a[i] === b[i]) i++;
  return i;
}

function buildTypoShard(byDelete, options) {
  const deleteEntries = [...byDelete.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  const selectedByDelete = [];
  const pairIds = new Map();
  const pairs = [];

  for (const [deleteKey, candidates] of deleteEntries) {
    const selected = [...candidates.entries()]
      .map(([text, df]) => ({ ...parseCandidateText(text), df }))
      .filter(candidate => candidate.surface && candidate.term)
      .sort((a, b) =>
        b.df - a.df
        || a.surface.length - b.surface.length
        || a.term.length - b.term.length
        || a.surface.localeCompare(b.surface)
        || a.term.localeCompare(b.term))
      .slice(0, options.maxCandidatesPerDelete);
    if (!selected.length) continue;
    selectedByDelete.push({ deleteKey, selected });
    for (const candidate of selected) {
      const key = `${candidate.surface}${SEPARATOR}${candidate.term}`;
      if (!pairIds.has(key)) {
        pairIds.set(key, pairs.length);
        pairs.push(candidate);
      }
    }
  }

  pairs.sort((a, b) => b.df - a.df || a.surface.localeCompare(b.surface) || a.term.localeCompare(b.term));
  pairIds.clear();
  pairs.forEach((pair, index) => pairIds.set(`${pair.surface}${SEPARATOR}${pair.term}`, index));

  const postingBytes = [];
  const directory = [];
  for (const { deleteKey, selected } of selectedByDelete) {
    const offset = postingBytes.length;
    for (const candidate of selected) pushVarint(postingBytes, pairIds.get(`${candidate.surface}${SEPARATOR}${candidate.term}`));
    directory.push({ deleteKey, offset, count: selected.length });
  }

  const header = [...TYPO_SHARD_MAGIC];
  pushVarint(header, pairs.length);
  for (const pair of pairs) {
    pushUtf8(header, pair.surface);
    pushUtf8(header, pair.term);
    pushVarint(header, pair.df);
  }
  pushVarint(header, directory.length);
  let previous = "";
  for (const entry of directory) {
    const prefix = commonPrefixLength(previous, entry.deleteKey);
    pushVarint(header, prefix);
    pushUtf8(header, entry.deleteKey.slice(prefix));
    pushVarint(header, entry.offset);
    pushVarint(header, entry.count);
    previous = entry.deleteKey;
  }

  return {
    buffer: Buffer.concat([Buffer.from(Uint8Array.from(header)), Buffer.from(Uint8Array.from(postingBytes))]),
    stats: { deleteKeys: directory.length, pairs: pairs.length, candidates: postingBytes.length }
  };
}

function compareTypoRecords(left, right) {
  return left[0].localeCompare(right[0]) || left[1].localeCompare(right[1]) || left[2] - right[2];
}

function typoSortLimits(options) {
  return {
    records: Math.max(1, Math.floor(Number(options.sortChunkRecords || 25000))),
    bytes: Math.max(1024, Math.floor(Number(options.sortChunkBytes || 4 * 1024 * 1024)))
  };
}

function estimateTypoRecordBytes(record) {
  return String(record[0] || "").length + String(record[1] || "").length + 16;
}

function writeSortedTypoChunk(records, outDir, index) {
  records.sort(compareTypoRecords);
  const path = resolve(outDir, `${String(index).padStart(5, "0")}.run`);
  const fd = openSync(path, "w");
  try {
    for (const record of records) {
      const bytes = encodeRunRecord(["string", "string", "number"], record);
      writeSync(fd, bytes, 0, bytes.length);
    }
  } finally {
    closeSync(fd);
  }
  records.length = 0;
  return path;
}

async function writeSortedTypoChunks(path, scratchDir, options) {
  mkdirSync(scratchDir, { recursive: true });
  const limits = typoSortLimits(options);
  const records = [];
  const chunks = [];
  let bytes = 0;
  for await (const record of readRunRecords(path, ["string", "string", "number"])) {
    if (!record[0] || !record[1]) continue;
    records.push(record);
    bytes += estimateTypoRecordBytes(record);
    if (records.length >= limits.records || bytes >= limits.bytes) {
      chunks.push(writeSortedTypoChunk(records, scratchDir, chunks.length));
      bytes = 0;
    }
  }
  if (records.length) chunks.push(writeSortedTypoChunk(records, scratchDir, chunks.length));
  return chunks;
}

async function createTypoChunkReader(path) {
  const iterator = readRunRecords(path, ["string", "string", "number"])[Symbol.asyncIterator]();
  const first = await iterator.next();
  return { iterator, record: first.done ? null : first.value };
}

async function* mergeSortedTypoChunks(chunks) {
  const readers = await Promise.all(chunks.map(createTypoChunkReader));
  try {
    while (readers.some(reader => reader.record)) {
      let best = -1;
      for (let i = 0; i < readers.length; i++) {
        if (!readers[i].record) continue;
        if (best < 0 || compareTypoRecords(readers[i].record, readers[best].record) < 0) best = i;
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

async function* reducedDeleteKeys(chunks, options = {}) {
  const emitCandidates = options.emitCandidates !== false;
  let currentDeleteKey = null;
  let currentText = null;
  let currentDf = 0;
  let candidates = new Map();
  let candidateCount = 0;

  function finishCandidate() {
    if (!currentText) return;
    candidateCount++;
    if (emitCandidates) candidates.set(currentText, currentDf);
  }

  function finishDeleteKey() {
    if (!currentDeleteKey) return null;
    finishCandidate();
    const item = emitCandidates
      ? { deleteKey: currentDeleteKey, candidates }
      : { deleteKey: currentDeleteKey, candidateCount };
    candidates = new Map();
    candidateCount = 0;
    currentText = null;
    currentDf = 0;
    return item;
  }

  for await (const [deleteKey, text, df] of mergeSortedTypoChunks(chunks)) {
    if (deleteKey !== currentDeleteKey) {
      const item = finishDeleteKey();
      if (item) yield item;
      currentDeleteKey = deleteKey;
      currentText = text;
      currentDf = df;
      continue;
    }
    if (text !== currentText) {
      finishCandidate();
      currentText = text;
      currentDf = df;
      continue;
    }
    currentDf += df;
  }
  const item = finishDeleteKey();
  if (item) yield item;
}

function typoPrefixKey(deleteKey, options, depth) {
  return `${depth}\u0000${typoShardKey(deleteKey, options, depth)}`;
}

function addTypoPrefixCounts(prefixCounts, deleteKey, candidateCount, options) {
  const baseDepth = Math.max(1, Math.floor(Number(options.baseShardDepth || 1)));
  const maxDepth = Math.max(baseDepth, Math.floor(Number(options.maxShardDepth || baseDepth)));
  const effective = Math.min(options.maxCandidatesPerDelete, candidateCount);
  for (let depth = baseDepth; depth <= maxDepth; depth++) {
    const key = typoPrefixKey(deleteKey, options, depth);
    prefixCounts.set(key, (prefixCounts.get(key) || 0) + effective);
  }
}

function typoPartitionNameForKey(deleteKey, prefixCounts, options) {
  const target = Math.max(1, Math.floor(Number(options.targetShardCandidates || 1)));
  const baseDepth = Math.max(1, Math.floor(Number(options.baseShardDepth || 1)));
  const maxDepth = Math.max(baseDepth, Math.floor(Number(options.maxShardDepth || baseDepth)));
  let depth = baseDepth;
  while (depth < maxDepth && (prefixCounts.get(typoPrefixKey(deleteKey, options, depth)) || 0) > target) depth++;
  return typoShardKey(deleteKey, options, depth);
}

async function reduceTypoShard(baseShard, buffer, packWriter, scratchRoot) {
  const path = resolve(buffer.runsOut, `${baseShard}.run`);
  const scratchDir = resolve(scratchRoot, encodeURIComponent(baseShard));
  const chunks = await writeSortedTypoChunks(path, scratchDir, buffer.options);
  const prefixCounts = new Map();
  const finalShards = [];
  let deleteKeys = 0;
  let pairs = 0;
  let candidates = 0;
  try {
    for await (const item of reducedDeleteKeys(chunks, { emitCandidates: false })) {
      addTypoPrefixCounts(prefixCounts, item.deleteKey, item.candidateCount, buffer.options);
    }

    let currentPartition = null;
    let entries = [];
    function flushPartition() {
      if (!currentPartition || !entries.length) return;
      const encoded = buildTypoShard(new Map(entries), buffer.options);
      entries = [];
      if (!encoded.stats.deleteKeys) return;
      writePackedShard(packWriter, currentPartition, gzipSync(encoded.buffer, { level: 6 }), {
        kind: "typo-shard",
        codec: "rftypo-v1",
        logicalLength: encoded.buffer.length
      });
      finalShards.push(currentPartition);
      deleteKeys += encoded.stats.deleteKeys;
      pairs += encoded.stats.pairs;
      candidates += encoded.stats.candidates;
    }

    for await (const item of reducedDeleteKeys(chunks, { emitCandidates: true })) {
      const partition = typoPartitionNameForKey(item.deleteKey, prefixCounts, buffer.options);
      if (currentPartition && partition !== currentPartition) flushPartition();
      currentPartition = partition;
      entries.push([item.deleteKey, item.candidates]);
    }
    flushPartition();
  } finally {
    rmSync(scratchDir, { recursive: true, force: true });
  }
  return { shards: finalShards, deleteKeys, pairs, candidates };
}

async function* reducedLexiconEntries(chunks) {
  let currentSurface = null;
  let currentTerm = null;
  let currentDf = 0;

  function finish() {
    if (!currentSurface || !currentTerm) return null;
    return { surface: currentSurface, term: currentTerm, df: currentDf };
  }

  for await (const [surface, term, df] of mergeSortedTypoChunks(chunks)) {
    if (surface !== currentSurface || term !== currentTerm) {
      const item = finish();
      if (item) yield item;
      currentSurface = surface;
      currentTerm = term;
      currentDf = df;
      continue;
    }
    currentDf += df;
  }
  const item = finish();
  if (item) yield item;
}

async function reduceTypoLexiconShard(shard, buffer, packWriter, scratchRoot) {
  const path = resolve(buffer.runsOut, `${shard}.lex.run`);
  const scratchDir = resolve(scratchRoot, encodeURIComponent(shard));
  const chunks = await writeSortedTypoChunks(path, scratchDir, buffer.options);
  const entries = [];
  try {
    for await (const entry of reducedLexiconEntries(chunks)) entries.push(entry);
    const encoded = buildTypoLexiconShard(entries, buffer.options);
    if (!encoded.stats.entries) return { entries: 0 };
    writePackedShard(packWriter, shard, gzipSync(encoded.buffer, { level: 6 }), {
      kind: "typo-lexicon",
      codec: TYPO_LEXICON_FORMAT,
      logicalLength: encoded.buffer.length
    });
    return {
      entries: encoded.stats.entries,
      trieNodes: encoded.stats.trie_nodes || 0,
      trieArcs: encoded.stats.trie_arcs || 0
    };
  } finally {
    rmSync(scratchDir, { recursive: true, force: true });
  }
}

async function reduceTypoLexiconRuns(buffer, outDir) {
  if (!buffer.lexiconShards.size) return null;
  const packWriter = createPackWriter(resolve(outDir, "typo", "lexicon-packs"), buffer.options.lexiconPackBytes || buffer.options.packBytes);
  const scratchRoot = resolve(buffer.runsOut, "..", "typo-lexicon-reduce-sort");
  let entries = 0;
  let trieNodes = 0;
  let trieArcs = 0;
  const shards = [];
  for (const shard of [...buffer.lexiconShards].sort()) {
    const stats = await reduceTypoLexiconShard(shard, buffer, packWriter, scratchRoot);
    if (stats.entries > 0) {
      entries += stats.entries;
      trieNodes += stats.trieNodes || 0;
      trieArcs += stats.trieArcs || 0;
      shards.push(shard);
    }
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const directoryEntries = shards.map((shard) => {
    const entry = packWriter.entries[shard];
    return { shard, packIndex: packIndexes.get(entry.pack), ...entry };
  });
  const directory = writeDirectoryFiles(
    resolve(outDir, "typo", "lexicon"),
    directoryEntries,
    buffer.options.lexiconDirectoryPageBytes || buffer.options.directoryPageBytes,
    "typo/lexicon",
    { packTable: packWriter.packs }
  );
  return {
    format: TYPO_LEXICON_FORMAT,
    storage: "range-pack-v1",
    shard_depth: Math.max(1, Math.floor(Number(buffer.options.lexiconShardDepth || 1))),
    min_surface_length: Math.max(1, Math.floor(Number(buffer.options.lexiconMinSurfaceLength || 1))),
    max_scan_candidates: Math.max(1, Math.floor(Number(buffer.options.lexiconMaxScanCandidates || 2048))),
    directory,
    packs: packWriter.packs,
    stats: {
      raw_pairs: buffer.lexiconPairs,
      entries,
      trie_nodes: trieNodes,
      trie_arcs: trieArcs,
      pack_bytes: packWriter.bytes,
      pack_files: packWriter.packs.length,
      directory_page_files: directory.page_files,
      directory_bytes: directory.total_bytes
    }
  };
}

export async function reduceTypoRuns(buffer, outDir) {
  if (!buffer || !buffer.options.enabled) return null;
  flushTypoBuffer(buffer);
  const packWriter = createPackWriter(resolve(outDir, "typo", "packs"), buffer.options.packBytes);
  const finalShards = new Set();
  const scratchRoot = resolve(buffer.runsOut, "..", "typo-reduce-sort");
  let deleteKeys = 0;
  let pairs = 0;
  let candidates = 0;
  for (const shard of [...buffer.shards].sort()) {
    const stats = await reduceTypoShard(shard, buffer, packWriter, scratchRoot);
    deleteKeys += stats.deleteKeys;
    pairs += stats.pairs;
    candidates += stats.candidates;
    for (const finalShard of stats.shards) finalShards.add(finalShard);
  }
  finalizePackWriter(packWriter);
  const lexicon = await reduceTypoLexiconRuns(buffer, outDir);
  const shards = [...finalShards].sort();
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const directoryEntries = shards.map((shard) => {
    const entry = packWriter.entries[shard];
    return { shard, packIndex: packIndexes.get(entry.pack), ...entry };
  });
  mkdirSync(resolve(outDir, "typo"), { recursive: true });
  const directory = writeDirectoryFiles(resolve(outDir, "typo"), directoryEntries, buffer.options.directoryPageBytes, "typo", { packTable: packWriter.packs });
  const manifest = {
    version: 1,
    format: "rftypo-v1",
    compression: "gzip",
    min_term_length: buffer.options.minTermLength,
    min_surface_length: buffer.options.minSurfaceLength,
    max_surface_length: buffer.options.maxSurfaceLength,
    max_term_length: buffer.options.maxTermLength,
    max_edits: buffer.options.maxEdits,
    delete_max_surface_length: buffer.options.deleteMaxSurfaceLength,
    base_shard_depth: buffer.options.baseShardDepth,
    max_shard_depth: buffer.options.maxShardDepth,
    max_candidates_per_delete: buffer.options.maxCandidatesPerDelete,
    storage: "range-pack-v1",
    directory,
    packs: packWriter.packs,
    lexicon,
    stats: {
      terms: buffer.terms,
      delete_pairs: buffer.deletePairs,
      lexicon_pairs: buffer.lexiconPairs,
      lexicon_entries: lexicon?.stats?.entries || 0,
      delete_keys: deleteKeys,
      pairs,
      candidates,
      pack_bytes: packWriter.bytes,
      directory_page_files: directory.page_files,
      directory_bytes: directory.total_bytes
    }
  };
  const manifestJson = JSON.stringify(manifest);
  const manifestHash = sha256Hex(manifestJson);
  manifest.manifest = `typo/manifest.${manifestHash.slice(0, OBJECT_NAME_HASH_LENGTH)}.json`;
  manifest.manifest_hash = manifestHash;
  writeFileSync(resolve(outDir, manifest.manifest), manifestJson);
  return manifest;
}

export function parseTypoShard(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, TYPO_SHARD_MAGIC, "Unsupported Rangefind typo shard");
  const state = { pos: TYPO_SHARD_MAGIC.length };
  const pairCount = readVarint(bytes, state);
  const pairs = new Array(pairCount);
  for (let i = 0; i < pairCount; i++) {
    pairs[i] = { surface: readUtf8(bytes, state), term: readUtf8(bytes, state), df: readVarint(bytes, state) };
  }
  const keyCount = readVarint(bytes, state);
  const keys = new Map();
  let previous = "";
  for (let i = 0; i < keyCount; i++) {
    const prefix = readVarint(bytes, state);
    const suffix = readUtf8(bytes, state);
    const key = previous.slice(0, prefix) + suffix;
    keys.set(key, { offset: readVarint(bytes, state), count: readVarint(bytes, state), candidates: null });
    previous = key;
  }
  return { bytes, dataStart: state.pos, pairs, keys };
}

export function typoCandidatesForDeleteKey(shard, key) {
  const entry = shard.keys?.get(key);
  if (!entry) return [];
  if (entry.candidates) return entry.candidates;
  const state = { pos: shard.dataStart + entry.offset };
  const candidates = new Array(entry.count);
  for (let i = 0; i < entry.count; i++) candidates[i] = shard.pairs[readVarint(shard.bytes, state)] || { surface: "", term: "", df: 0 };
  entry.candidates = candidates;
  return candidates;
}

export function boundedDamerauLevenshtein(a, b, maxDistance) {
  if (a === b) return 0;
  if (Math.abs(a.length - b.length) > maxDistance) return maxDistance + 1;
  let prevPrev = new Array(b.length + 1).fill(0);
  let prev = Array.from({ length: b.length + 1 }, (_, i) => i);
  let current = new Array(b.length + 1);
  for (let i = 1; i <= a.length; i++) {
    current[0] = i;
    let rowMin = current[0];
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      let value = Math.min(prev[j] + 1, current[j - 1] + 1, prev[j - 1] + cost);
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) {
        value = Math.min(value, prevPrev[j - 2] + 1);
      }
      current[j] = value;
      if (value < rowMin) rowMin = value;
    }
    if (rowMin > maxDistance) return maxDistance + 1;
    [prevPrev, prev, current] = [prev, current, prevPrev];
  }
  return prev[b.length];
}

function lcsLength(a, b) {
  const previous = new Array(b.length + 1).fill(0);
  const current = new Array(b.length + 1).fill(0);
  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      current[j] = a[i - 1] === b[j - 1] ? previous[j - 1] + 1 : Math.max(previous[j], current[j - 1]);
    }
    for (let j = 0; j <= b.length; j++) previous[j] = current[j];
  }
  return previous[b.length];
}

export function typoCandidateScore(token, surface, df, distance) {
  const prefix = commonPrefixLength(token, surface);
  const sequenceSimilarity = lcsLength(token, surface) / Math.max(1, token.length);
  const sameFirst = token[0] && token[0] === surface[0] ? 1.2 : -1.6;
  const sameLast = token[token.length - 1] === surface[surface.length - 1] ? 0.35 : 0;
  const lengthPenalty = Math.abs(token.length - surface.length) * 0.25;
  return Math.log1p(df) * 1.15
    + Math.min(prefix, 4) * 0.15
    + sequenceSimilarity * 4.0
    + sameFirst
    + sameLast
    - distance * 2.35
    - lengthPenalty;
}
