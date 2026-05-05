import { appendFileSync, mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { authorityKeysForValue, AUTHORITY_FORMAT, buildAuthorityShard } from "./authority_codec.js";
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

export function authorityEnabled(config) {
  return config.authority !== false && authorityFields(config).length > 0;
}

function authorityShardConfig(config) {
  const baseShardDepth = Math.max(1, Math.floor(Number(config.authorityBaseShardDepth ?? config.baseShardDepth ?? 1)));
  const maxShardDepth = Math.max(baseShardDepth, Math.floor(Number(config.authorityMaxShardDepth ?? Math.max(baseShardDepth, config.maxShardDepth || baseShardDepth))));
  const targetShardPostings = Math.max(1, Math.floor(Number(config.authorityTargetShardRows ?? Math.max(1024, Math.floor(Number(config.targetShardPostings || 4096) / 8)))));
  return { ...config, baseShardDepth, maxShardDepth, targetShardPostings };
}

export function createAuthorityRunBuffer(config, outDir) {
  return authorityEnabled(config)
    ? { byShard: new Map(), lines: 0, runsOut: outDir, baseShards: new Set(), fields: authorityFields(config), shardConfig: authorityShardConfig(config), enabled: true }
    : { enabled: false, baseShards: new Set(), fields: [] };
}

function flushAuthorityBuffer(buffer) {
  if (!buffer.enabled) return;
  for (const [shard, records] of buffer.byShard) {
    appendFileSync(resolve(buffer.runsOut, `${shard}.run`), Buffer.concat(records));
    records.length = 0;
  }
  buffer.byShard.clear();
  buffer.lines = 0;
}

function addAuthorityRecord(buffer, config, key, doc, score) {
  if (!key || score <= 0) return;
  const shard = baseShardFor(key, buffer.shardConfig || authorityShardConfig(config));
  if (!buffer.byShard.has(shard)) buffer.byShard.set(shard, []);
  buffer.byShard.get(shard).push(encodeRunRecord(["string", "number", "number"], [key, doc, score]));
  buffer.baseShards.add(shard);
  buffer.lines++;
  if (buffer.lines >= config.postingFlushLines) flushAuthorityBuffer(buffer);
}

export function addAuthorityDoc(buffer, config, doc, index) {
  if (!buffer.enabled) return;
  const seen = new Set();
  for (const field of buffer.fields) {
    for (const value of valueList(rawPath(doc, field.path))) {
      for (const { key, kind } of authorityKeysForValue(value, field)) {
        const dedupeKey = `${key}\u0000${index}`;
        if (seen.has(dedupeKey)) continue;
        seen.add(dedupeKey);
        const score = kind === "surface" ? field.surfaceWeight : kind === "exact" ? field.exactWeight : field.tokenWeight;
        addAuthorityRecord(buffer, config, key, index, score);
      }
    }
  }
}

export function finishAuthorityRuns(buffer) {
  flushAuthorityBuffer(buffer);
  return [...(buffer.baseShards || [])].sort();
}

export async function reduceAuthorityRuns(config, dirs, baseShards) {
  if (!authorityEnabled(config) || !baseShards?.length) return null;
  const shardConfig = authorityShardConfig(config);
  const packWriter = createPackWriter(resolve(dirs.out, "authority", "packs"), config.authorityPackBytes || config.packBytes);
  const finalShards = [];
  const entries = [];
  let keyCount = 0;
  let rowCount = 0;
  mkdirSync(resolve(dirs.out, "authority"), { recursive: true });
  for (const baseShard of baseShards) {
    const runPath = resolve(dirs.authorityRunsOut, `${baseShard}.run`);
    const stats = await reduceRunToPartitions({
      runPath,
      scratchDir: resolve(dirs.build || resolve(dirs.out, "_build"), "authority-reduce-sort", encodeURIComponent(baseShard)),
      config: shardConfig,
      onPartition: (partition) => {
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
    directory,
    packs: packWriter.packs,
    pack_bytes: packWriter.bytes,
    directory_bytes: directory.total_bytes
  };
}
