import { appendFileSync, existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { OBJECT_CHECKSUM_ALGORITHM, OBJECT_NAME_HASH_LENGTH } from "./object_store.js";

export function createPackWriter(outDir, targetBytes, options = {}) {
  mkdirSync(outDir, { recursive: true });
  return {
    index: -1,
    file: "",
    path: "",
    offset: 0,
    bytes: 0,
    entries: {},
    packs: [],
    outDir,
    targetBytes,
    dedupe: options.dedupe !== false,
    objects: new Map(),
    dedupedObjects: 0,
    dedupedBytes: 0,
    finalized: false
  };
}

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function openPack(writer) {
  writer.index++;
  writer.file = `${String(writer.index).padStart(4, "0")}.bin`;
  writer.path = resolve(writer.outDir, writer.file);
  writer.offset = 0;
  const pack = { index: writer.index, file: writer.file, bytes: 0, shards: 0, objects: 0, references: 0, dedupedObjects: 0, dedupedBytes: 0 };
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
    writer.entries[shard] = {
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
    return writer.entries[shard];
  }
  if (!writer.file || (writer.offset > 0 && writer.offset + compressed.length > writer.targetBytes)) openPack(writer);
  appendFileSync(writer.path, compressed);
  writer.entries[shard] = {
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
  writer.objects.set(checksum.value, writer.entries[shard]);
  writer.offset += compressed.length;
  writer.bytes += compressed.length;
  const pack = writer.packs[writer.packs.length - 1];
  pack.bytes += compressed.length;
  pack.shards++;
  pack.objects++;
  pack.references++;
  return writer.entries[shard];
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
  writer.file = writer.packs.at(-1)?.file || "";
  writer.path = writer.packs.at(-1)?.path || "";
  writer.finalized = true;
  return writer;
}
