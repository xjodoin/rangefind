import { appendFileSync, mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

export function createPackWriter(outDir, targetBytes) {
  mkdirSync(outDir, { recursive: true });
  return { index: -1, file: "", path: "", offset: 0, bytes: 0, entries: {}, packs: [], outDir, targetBytes };
}

function openPack(writer) {
  writer.index++;
  writer.file = `${String(writer.index).padStart(4, "0")}.bin`;
  writer.path = resolve(writer.outDir, writer.file);
  writer.offset = 0;
  writer.packs.push({ file: writer.file, bytes: 0, shards: 0 });
  writeFileSync(writer.path, "");
}

export function writePackedShard(writer, shard, compressed) {
  if (!writer.file || (writer.offset > 0 && writer.offset + compressed.length > writer.targetBytes)) openPack(writer);
  appendFileSync(writer.path, compressed);
  writer.entries[shard] = { pack: writer.file, offset: writer.offset, length: compressed.length };
  writer.offset += compressed.length;
  writer.bytes += compressed.length;
  const pack = writer.packs[writer.packs.length - 1];
  pack.bytes += compressed.length;
  pack.shards++;
  return writer.entries[shard];
}
