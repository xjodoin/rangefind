import { appendFileSync, createWriteStream, existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { resolve } from "node:path";
import { createGzip, gzipSync } from "node:zlib";
import { OBJECT_CHECKSUM_ALGORITHM, OBJECT_NAME_HASH_LENGTH } from "./object_store.js";

export function createPackWriter(outDir, targetBytes, options = {}) {
  mkdirSync(outDir, { recursive: true });
  const indexCounter = normalizeIndexCounter(options.indexCounter);
  return {
    index: -1,
    file: "",
    path: "",
    offset: 0,
    bytes: 0,
    entryCount: 0,
    entries: {},
    packs: [],
    outDir,
    targetBytes,
    indexCounter,
    dedupe: options.dedupe !== false,
    keepEntries: options.keepEntries !== false,
    objects: new Map(),
    dedupedObjects: 0,
    dedupedBytes: 0,
    finalized: false
  };
}

export function createAppendOnlyPackWriter(outDir, targetBytes, options = {}) {
  return createPackWriter(outDir, targetBytes, {
    ...options,
    keepEntries: false,
    dedupe: options.dedupe === true
  });
}

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function normalizeIndexCounter(counter) {
  if (!counter) return null;
  if (counter instanceof Int32Array) return counter;
  if (counter instanceof SharedArrayBuffer) return new Int32Array(counter);
  throw new Error("Rangefind pack writer indexCounter must be an Int32Array or SharedArrayBuffer.");
}

function nextPackIndex(writer) {
  if (!writer.indexCounter) {
    writer.index++;
    return writer.index;
  }
  const index = Atomics.add(writer.indexCounter, 0, 1);
  writer.index = index;
  return index;
}

function openPack(writer) {
  const index = nextPackIndex(writer);
  writer.file = `${String(index).padStart(4, "0")}.bin`;
  writer.path = resolve(writer.outDir, writer.file);
  writer.offset = 0;
  const pack = { index, file: writer.file, bytes: 0, shards: 0, objects: 0, references: 0, dedupedObjects: 0, dedupedBytes: 0 };
  Object.defineProperty(pack, "path", { value: writer.path, writable: true, enumerable: false, configurable: true });
  writer.packs.push(pack);
  writeFileSync(writer.path, "");
}

export function writePackedShard(writer, shard, compressed, metadata = {}) {
  if (writer.finalized) throw new Error("Cannot write to finalized Rangefind pack writer.");
  const checksum = {
    algorithm: metadata.checksumAlgorithm || OBJECT_CHECKSUM_ALGORITHM,
    value: sha256Hex(compressed)
  };
  const existing = writer.dedupe ? writer.objects.get(checksum.value) : null;
  if (existing) {
    const pack = writer.packs.find(item => item.file === existing.pack);
    if (pack) {
      pack.shards++;
      pack.references++;
      pack.dedupedObjects++;
      pack.dedupedBytes += compressed.length;
    }
    writer.dedupedObjects++;
    writer.dedupedBytes += compressed.length;
    const entry = {
      pack: existing.pack,
      offset: existing.offset,
      length: existing.length,
      physicalLength: existing.physicalLength,
      logicalLength: metadata.logicalLength ?? existing.logicalLength ?? null,
      kind: metadata.kind || null,
      codec: metadata.codec || null,
      compression: metadata.compression || existing.compression || "gzip-member",
      checksum
    };
    writer.entryCount++;
    if (writer.keepEntries) writer.entries[shard] = entry;
    return entry;
  }
  if (!writer.file || (writer.offset > 0 && writer.offset + compressed.length > writer.targetBytes)) openPack(writer);
  appendFileSync(writer.path, compressed);
  const entry = {
    pack: writer.file,
    offset: writer.offset,
    length: compressed.length,
    physicalLength: compressed.length,
    logicalLength: metadata.logicalLength ?? null,
    kind: metadata.kind || null,
    codec: metadata.codec || null,
    compression: metadata.compression || "gzip-member",
    checksum
  };
  writer.entryCount++;
  if (writer.keepEntries) writer.entries[shard] = entry;
  if (writer.dedupe) writer.objects.set(checksum.value, entry);
  writer.offset += compressed.length;
  writer.bytes += compressed.length;
  const pack = writer.packs[writer.packs.length - 1];
  pack.bytes += compressed.length;
  pack.shards++;
  pack.objects++;
  pack.references++;
  return entry;
}

function chunksByteLength(chunks) {
  let total = 0;
  for (const chunk of chunks || []) total += chunk?.length || 0;
  return total;
}

export async function writePackedShardChunks(writer, shard, chunks, metadata = {}) {
  if (writer.finalized) throw new Error("Cannot write to finalized Rangefind pack writer.");
  const logicalLength = metadata.logicalLength ?? chunksByteLength(chunks);
  const streamMinBytes = Math.max(0, Math.floor(Number(metadata.streamMinBytes ?? 64 * 1024)));
  if (logicalLength < streamMinBytes) {
    const source = Buffer.concat(Array.from(chunks || [], chunk => Buffer.from(chunk)), logicalLength);
    return writePackedShard(writer, shard, gzipSync(source, { level: metadata.gzipLevel ?? 6 }), {
      ...metadata,
      logicalLength
    });
  }
  if (writer.dedupe) {
    throw new Error("Streaming Rangefind pack writes require an append-only writer without dedupe.");
  }
  if (!writer.file || (writer.offset > 0 && writer.offset + logicalLength > writer.targetBytes)) openPack(writer);

  const checksum = {
    algorithm: metadata.checksumAlgorithm || OBJECT_CHECKSUM_ALGORITHM,
    value: ""
  };
  const hash = createHash(checksum.algorithm);
  const out = createWriteStream(writer.path, { flags: "a" });
  const gzip = createGzip({ level: metadata.gzipLevel ?? 6 });
  let physicalLength = 0;
  const finished = new Promise((resolveDone, rejectDone) => {
    const reject = (error) => {
      gzip.destroy();
      out.destroy();
      rejectDone(error);
    };
    gzip.once("error", reject);
    out.once("error", reject);
    out.once("finish", resolveDone);
  });
  gzip.on("data", chunk => {
    physicalLength += chunk.length;
    hash.update(chunk);
  });
  gzip.pipe(out);
  for (const chunk of chunks || []) {
    if (!gzip.write(chunk)) await once(gzip, "drain");
  }
  gzip.end();
  await finished;
  checksum.value = hash.digest("hex");

  const entry = {
    pack: writer.file,
    offset: writer.offset,
    length: physicalLength,
    physicalLength,
    logicalLength,
    kind: metadata.kind || null,
    codec: metadata.codec || null,
    compression: metadata.compression || "gzip-member",
    checksum
  };
  writer.entryCount++;
  if (writer.keepEntries) writer.entries[shard] = entry;
  writer.offset += physicalLength;
  writer.bytes += physicalLength;
  const pack = writer.packs[writer.packs.length - 1];
  pack.bytes += physicalLength;
  pack.shards++;
  pack.objects++;
  pack.references++;
  return entry;
}

export function finalizePackWriter(writer) {
  if (writer.finalized) return writer;
  const nameMap = new Map();
  for (const pack of writer.packs) {
    const bytes = readFileSync(pack.path);
    const hash = sha256Hex(bytes);
    const prefix = String(pack.index).padStart(4, "0");
    const file = `${prefix}.${hash.slice(0, OBJECT_NAME_HASH_LENGTH)}.bin`;
    const path = resolve(writer.outDir, file);
    if (path !== pack.path) {
      if (existsSync(path)) unlinkSync(pack.path);
      else renameSync(pack.path, path);
    }
    nameMap.set(pack.file, file);
    pack.file = file;
    Object.defineProperty(pack, "path", { value: path, writable: true, enumerable: false, configurable: true });
    pack.contentHash = hash;
    pack.immutable = true;
  }
  for (const entry of Object.values(writer.entries)) {
    entry.pack = nameMap.get(entry.pack) || entry.pack;
  }
  writer.packNameMap = nameMap;
  writer.file = writer.packs.at(-1)?.file || "";
  writer.path = writer.packs.at(-1)?.path || "";
  writer.finalized = true;
  return writer;
}

export function resolvePackEntry(writer, entry) {
  if (!entry) return entry;
  const mapped = writer.packNameMap?.get(entry.pack);
  return mapped && mapped !== entry.pack ? { ...entry, pack: mapped } : entry;
}
