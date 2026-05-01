import { appendFileSync, mkdirSync, unlinkSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { surfaceStemPairs } from "./analyzer.js";
import { TYPO_SHARD_MAGIC, pushVarint, readVarint } from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";
import { writeDirectoryFiles } from "./directory.js";
import { createPackWriter, writePackedShard } from "./packs.js";
import { encodeRunRecord, readRunRecords } from "./runs.js";

const SEPARATOR = "\u0001";

export const TYPO_DEFAULTS = {
  enabled: true,
  minTermLength: 5,
  minSurfaceLength: 4,
  maxSurfaceLength: 24,
  maxTermLength: 20,
  minDf: 1,
  maxDfRatio: 0.08,
  maxEdits: 2,
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
  return { byShard: new Map(), lines: 0, runsOut, options, terms: 0, deletePairs: 0, shards: new Set() };
}

export function flushTypoBuffer(buffer) {
  if (!buffer) return;
  for (const [shard, records] of buffer.byShard) {
    if (!records.length) continue;
    appendFileSync(resolve(buffer.runsOut, `${shard}.run`), Buffer.concat(records));
  }
  buffer.byShard.clear();
  buffer.lines = 0;
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
    for (const deleteKey of typoDeleteKeys(surface, buffer.options, typoMaxEditsFor(surface, buffer.options))) {
      bufferTypoCandidate(buffer, deleteKey, key, 1);
    }
  }
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
  if (!buffer || !isTypoIndexTerm(term, df, total, buffer.options)) return;
  buffer.terms++;
  for (const deleteKey of typoDeleteKeys(term, buffer.options)) {
    bufferTypoCandidate(buffer, deleteKey, term, df);
  }
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

function typoEntryCandidateCount(entries, options) {
  return entries.reduce((sum, [, candidates]) => sum + Math.min(options.maxCandidatesPerDelete, candidates.size), 0);
}

function partitionTypoEntries(entries, options, depth = options.baseShardDepth) {
  if (!entries.length) return [];
  if (typoEntryCandidateCount(entries, options) <= options.targetShardCandidates || depth >= options.maxShardDepth) {
    return [{ name: typoShardKey(entries[0][0], options, depth), entries }];
  }
  const groups = new Map();
  for (const entry of entries) {
    const key = typoShardKey(entry[0], options, depth + 1);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(entry);
  }
  return [...groups.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .flatMap(([, group]) => partitionTypoEntries(group, options, depth + 1));
}

async function reduceTypoShard(baseShard, buffer, packWriter) {
  const path = resolve(buffer.runsOut, `${baseShard}.run`);
  const byDelete = new Map();
  for await (const [deleteKey, text, df] of readRunRecords(path, ["string", "string", "number"])) {
    if (!deleteKey || !text) continue;
    if (!byDelete.has(deleteKey)) byDelete.set(deleteKey, new Map());
    const candidates = byDelete.get(deleteKey);
    candidates.set(text, (candidates.get(text) || 0) + df);
  }

  const partitions = partitionTypoEntries([...byDelete.entries()].sort((a, b) => a[0].localeCompare(b[0])), buffer.options);
  const finalShards = [];
  let deleteKeys = 0;
  let pairs = 0;
  let candidates = 0;
  for (const partition of partitions) {
    const encoded = buildTypoShard(new Map(partition.entries), buffer.options);
    if (!encoded.stats.deleteKeys) continue;
    writePackedShard(packWriter, partition.name, gzipSync(encoded.buffer, { level: 6 }));
    finalShards.push(partition.name);
    deleteKeys += encoded.stats.deleteKeys;
    pairs += encoded.stats.pairs;
    candidates += encoded.stats.candidates;
  }
  unlinkSync(path);
  return { shards: finalShards, deleteKeys, pairs, candidates };
}

export async function reduceTypoRuns(buffer, outDir) {
  if (!buffer || !buffer.options.enabled) return null;
  flushTypoBuffer(buffer);
  const packWriter = createPackWriter(resolve(outDir, "typo", "packs"), buffer.options.packBytes);
  const finalShards = new Set();
  let deleteKeys = 0;
  let pairs = 0;
  let candidates = 0;
  for (const shard of [...buffer.shards].sort()) {
    const stats = await reduceTypoShard(shard, buffer, packWriter);
    deleteKeys += stats.deleteKeys;
    pairs += stats.pairs;
    candidates += stats.candidates;
    for (const finalShard of stats.shards) finalShards.add(finalShard);
  }
  const shards = [...finalShards].sort();
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const directoryEntries = shards.map((shard) => {
    const entry = packWriter.entries[shard];
    return { shard, packIndex: packIndexes.get(entry.pack), offset: entry.offset, length: entry.length };
  });
  mkdirSync(resolve(outDir, "typo"), { recursive: true });
  const directory = writeDirectoryFiles(resolve(outDir, "typo"), directoryEntries, buffer.options.directoryPageBytes, "typo");
  const manifest = {
    version: 1,
    format: "rftypo-v1",
    compression: "gzip",
    min_term_length: buffer.options.minTermLength,
    min_surface_length: buffer.options.minSurfaceLength,
    max_surface_length: buffer.options.maxSurfaceLength,
    max_term_length: buffer.options.maxTermLength,
    max_edits: buffer.options.maxEdits,
    base_shard_depth: buffer.options.baseShardDepth,
    max_shard_depth: buffer.options.maxShardDepth,
    max_candidates_per_delete: buffer.options.maxCandidatesPerDelete,
    storage: "range-pack-v1",
    directory,
    packs: packWriter.packs,
    stats: {
      terms: buffer.terms,
      delete_pairs: buffer.deletePairs,
      delete_keys: deleteKeys,
      pairs,
      candidates,
      pack_bytes: packWriter.bytes,
      directory_page_files: directory.page_files,
      directory_bytes: directory.total_bytes
    }
  };
  writeFileSync(resolve(outDir, "typo", "manifest.json"), JSON.stringify(manifest));
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
