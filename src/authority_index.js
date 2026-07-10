import { createHash } from "node:crypto";
import { appendFileSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { authorityKeysForValue, AUTHORITY_FORMAT, buildAuthorityShard } from "./authority_codec.js";
import {
  autocompleteEntrySummary,
  autocompleteKeysForValue,
  compareAutocomplete,
  encodeAuthorityHotList,
  encodeAuthorityLexiconRoot,
  isAutocompleteKey,
  AUTHORITY_LEXICON_FORMAT
} from "./authority_lexicon.js";
import { writeDirectoryFiles } from "./directory_writer.js";
import { createPackWriter, finalizePackWriter, writePackedShard } from "./packs.js";
import { reduceRunToPartitions } from "./reduce_stream.js";
import { encodeRunRecord } from "./runs.js";
import { baseShardFor } from "./shards.js";

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

export function authorityFields(config) {
  return (config.authority || [])
    .map(field => typeof field === "string" ? { name: field, path: field } : field)
    .map((field, index) => ({
      name: field.name || field.path || `authority${index}`,
      path: field.path || field.name,
      weight: Math.max(1, Math.floor(Number(field.weight ?? 1000000))),
      surfaceWeight: Math.max(1, Math.floor(Number(field.surfaceWeight ?? (Number(field.exactWeight ?? field.weight ?? 1000000) * 2)))),
      exactWeight: Math.max(1, Math.floor(Number(field.exactWeight ?? field.weight ?? 1000000))),
      tokenWeight: Math.max(1, Math.floor(Number(field.tokenWeight ?? Math.floor(Number(field.weight ?? 1000000) * 0.8)))),
      surface: field.surface !== false,
      exact: field.exact !== false,
      tokens: field.tokens !== false
    }))
    .filter(field => field.path);
}

export function autocompleteFields(config) {
  return (config.suggest || []).map(field => ({
    path: field.path,
    weightPath: field.weightPath || "",
    tokenPrefixes: field.tokenPrefixes !== false,
    maxTokenKeys: config.suggestMaxTokenKeys,
    minKeyLength: config.suggestMinKeyLength
  })).filter(field => field.path);
}

export function authorityEnabled(config) {
  return (config.authority !== false && authorityFields(config).length > 0) || autocompleteFields(config).length > 0;
}

function authorityShardConfig(config) {
  const baseShardDepth = Math.max(1, Math.floor(Number(config.authorityBaseShardDepth ?? config.baseShardDepth ?? 1)));
  const maxShardDepth = Math.max(baseShardDepth, Math.floor(Number(config.authorityMaxShardDepth ?? Math.max(baseShardDepth, config.maxShardDepth || baseShardDepth))));
  const targetShardPostings = Math.max(1, Math.floor(Number(config.authorityTargetShardRows ?? Math.max(1024, Math.floor(Number(config.targetShardPostings || 4096) / 8)))));
  return { ...config, baseShardDepth, maxShardDepth, targetShardPostings };
}

export function createAuthorityRunBuffer(config, outDir) {
  return authorityEnabled(config)
    ? {
        byShard: new Map(),
        lines: 0,
        runsOut: outDir,
        baseShards: new Set(),
        fields: authorityFields(config),
        autocompleteFields: autocompleteFields(config),
        shardConfig: authorityShardConfig(config),
        enabled: true
      }
    : { enabled: false, baseShards: new Set(), fields: [], autocompleteFields: [] };
}

function flushAuthorityBuffer(buffer) {
  if (!buffer.enabled) return;
  for (const [shard, records] of buffer.byShard) {
    appendFileSync(resolve(buffer.runsOut, shardRunFileName(shard)), Buffer.concat(records));
    records.length = 0;
  }
  buffer.byShard.clear();
  buffer.lines = 0;
}

export function authorityRecordsForDoc(fields, doc, analyzer = null, suggestionFields = []) {
  const records = new Map();
  for (const field of fields || []) {
    for (const value of valueList(rawPath(doc, field.path))) {
      for (const { key, kind } of authorityKeysForValue(value, analyzer ? { ...field, analyzer } : field)) {
        const score = kind === "surface" ? field.surfaceWeight : kind === "exact" ? field.exactWeight : field.tokenWeight;
        records.set(key, Math.max(records.get(key) || 0, score));
      }
    }
  }
  for (const field of suggestionFields || []) {
    const rawWeight = field.weightPath ? Number(rawPath(doc, field.weightPath, 0)) : 0;
    const score = Number.isFinite(rawWeight) && rawWeight > 0 ? Math.floor(rawWeight) : 0;
    for (const value of valueList(rawPath(doc, field.path))) {
      for (const { key } of autocompleteKeysForValue(value, field)) {
        records.set(key, Math.max(records.get(key) || 0, score));
      }
    }
  }
  return [...records].map(([key, score]) => ({ key, score }));
}

export function addAuthorityRecord(buffer, config, key, doc, score) {
  if (!key || (score <= 0 && !isAutocompleteKey(key))) return;
  const shard = baseShardFor(key, buffer.shardConfig || authorityShardConfig(config));
  if (!buffer.byShard.has(shard)) buffer.byShard.set(shard, []);
  buffer.byShard.get(shard).push(encodeRunRecord(["string", "number", "number"], [key, doc, score]));
  buffer.baseShards.add(shard);
  buffer.lines++;
  const flushRecords = Math.max(1, Math.floor(Number(config.authorityRunFlushRecords || 5000)));
  if (buffer.lines >= flushRecords) flushAuthorityBuffer(buffer);
}

export function addAuthorityDoc(buffer, config, doc, index, analyzer = null) {
  if (!buffer.enabled) return;
  for (const { key, score } of authorityRecordsForDoc(buffer.fields, doc, analyzer, buffer.autocompleteFields)) {
    addAuthorityRecord(buffer, config, key, index, score);
  }
}

export function finishAuthorityRuns(buffer) {
  flushAuthorityBuffer(buffer);
  return [...(buffer.baseShards || [])].sort();
}

function lonePairSafeName(value) {
  return Array.from(value, char => char.charCodeAt(0).toString(16)).join("-") || "empty";
}

function safeShardDirName(shard) {
  // Base shards can start with a lone surrogate when a shard prefix splits an
  // astral code point, which encodeURIComponent and utf8 filenames reject or
  // collapse; encode such names as UTF-16 unit hex instead.
  const value = String(shard);
  return value.isWellFormed() ? encodeURIComponent(value) : lonePairSafeName(value);
}

function shardRunFileName(shard) {
  const value = String(shard);
  return value.isWellFormed() ? `${value}.run` : `${lonePairSafeName(value)}.run`;
}

export async function reduceAuthorityRuns(config, dirs, baseShards) {
  if (!authorityEnabled(config) || !baseShards?.length) return null;
  const shardConfig = authorityShardConfig(config);
  const packWriter = createPackWriter(resolve(dirs.out, "authority", "packs"), config.authorityPackBytes || config.packBytes);
  const finalShards = [];
  const entries = [];
  let keyCount = 0;
  let rowCount = 0;
  let autocompleteKeyCount = 0;
  let autocompleteRowCount = 0;
  const autocompleteShards = [];
  const autocompleteWeighted = autocompleteFields(config).some(field => field.weightPath);
  const hotLimit = Math.max(8, Math.floor(Number(config.suggestHotListSize || 64)));
  const hot = new Map();

  function mergeHotCandidate(item) {
    const prefix = Array.from(item.normalized)[0] || "";
    if (!prefix) return;
    if (!hot.has(prefix)) hot.set(prefix, new Map());
    const bucket = hot.get(prefix);
    const previous = bucket.get(item.display);
    if (!previous || compareAutocomplete(item, previous) < 0) bucket.set(item.display, item);
    if (bucket.size > hotLimit * 2) {
      const kept = [...bucket.values()].sort(compareAutocomplete).slice(0, hotLimit);
      bucket.clear();
      for (const entry of kept) bucket.set(entry.display, entry);
    }
  }
  mkdirSync(resolve(dirs.out, "authority"), { recursive: true });
  const processedRuns = new Set();
  for (const baseShard of baseShards) {
    const preferredPath = resolve(dirs.authorityRunsOut, shardRunFileName(baseShard));
    // Older builds spooled malformed shard names through the filesystem's
    // replacement-character mangling; fall back so resumed builds still reduce.
    const runPath = existsSync(preferredPath) ? preferredPath : resolve(dirs.authorityRunsOut, `${baseShard}.run`);
    if (processedRuns.has(runPath) || !existsSync(runPath)) continue;
    processedRuns.add(runPath);
    const stats = await reduceRunToPartitions({
      runPath,
      scratchDir: resolve(dirs.build || resolve(dirs.out, "_build"), "authority-reduce-sort", safeShardDirName(baseShard)),
      config: shardConfig,
      onPartition: (partition) => {
        const autocomplete = partition.entries
          .filter(([key]) => isAutocompleteKey(key))
          .map(([key, rows]) => autocompleteEntrySummary(key, rows, { weighted: autocompleteWeighted }));
        if (autocomplete.length) {
          let maxRank = 0;
          let rows = 0;
          for (const item of autocomplete) {
            maxRank = Math.max(maxRank, item.rank);
            rows += item.count;
            mergeHotCandidate(item);
          }
          autocompleteShards.push({
            shard: partition.name,
            maxRank,
            count: autocomplete.length
          });
          autocompleteKeyCount += autocomplete.length;
          autocompleteRowCount += rows;
        }
        const buffer = buildAuthorityShard(partition.entries, { maxRows: config.authorityMaxRowsPerKey });
        const entry = writePackedShard(packWriter, partition.name, gzipSync(buffer, { level: 6 }), {
          kind: "authority-shard",
          codec: AUTHORITY_FORMAT,
          logicalLength: buffer.length
        });
        entries.push({ shard: partition.name, ...entry });
        finalShards.push(partition.name);
        return partition.name;
      }
    });
    keyCount += stats.terms;
    rowCount += stats.postings;
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const directoryEntries = entries.map(entry => ({ ...entry, packIndex: packIndexes.get(entry.pack) }));
  const directory = writeDirectoryFiles(resolve(dirs.out, "authority"), directoryEntries, config.authorityDirectoryPageBytes || config.directoryPageBytes, "authority", { packTable: packWriter.packs });
  let autocomplete = null;
  if (autocompleteShards.length) {
    const hotLists = new Map();
    for (const [prefix, bucket] of hot) {
      hotLists.set(prefix, [...bucket.values()].sort(compareAutocomplete).slice(0, hotLimit));
    }
    const hotObjects = new Map();
    let hotBytes = 0;
    mkdirSync(resolve(dirs.out, "authority", "hot"), { recursive: true });
    for (const [prefix, items] of hotLists) {
      const source = encodeAuthorityHotList(items);
      const compressed = gzipSync(source, { level: 6 });
      const hash = createHash("sha256").update(compressed).digest("hex");
      const file = `authority/hot/${hash.slice(0, 24)}.bin.gz`;
      writeFileSync(resolve(dirs.out, file), compressed);
      hotBytes += compressed.length;
      hotObjects.set(prefix, { file, bytes: compressed.length, count: items.length });
    }
    const root = encodeAuthorityLexiconRoot({ shards: autocompleteShards, hot: hotObjects, weighted: autocompleteWeighted });
    const compressed = gzipSync(root, { level: 6 });
    const hash = createHash("sha256").update(compressed).digest("hex");
    const file = `authority/lexicon-root.${hash.slice(0, 24)}.bin.gz`;
    writeFileSync(resolve(dirs.out, file), compressed);
    autocomplete = {
      format: AUTHORITY_LEXICON_FORMAT,
      fields: autocompleteFields(config),
      keys: autocompleteKeyCount,
      rows: autocompleteRowCount,
      shards: autocompleteShards.length,
      hot_prefixes: hotLists.size,
      hot_list_size: hotLimit,
      hot_bytes: hotBytes,
      directory: { file, bytes: compressed.length, logical_bytes: root.length, immutable: true }
    };
  }
  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    format: AUTHORITY_FORMAT,
    fields: authorityFields(config).map(({ name, path, weight, surfaceWeight, exactWeight, tokenWeight, surface, exact, tokens }) => ({ name, path, weight, surfaceWeight, exactWeight, tokenWeight, surface, exact, tokens })),
    max_rows_per_key: Math.max(1, Math.floor(Number(config.authorityMaxRowsPerKey || 16))),
    base_shard_depth: shardConfig.baseShardDepth,
    max_shard_depth: shardConfig.maxShardDepth,
    target_shard_rows: shardConfig.targetShardPostings,
    keys: keyCount,
    rows: rowCount,
    shards: finalShards.length,
    autocomplete,
    directory,
    packs: packWriter.packs,
    pack_bytes: packWriter.bytes,
    directory_bytes: directory.total_bytes
  };
}
