import { appendFileSync, mkdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { encodeRunRecord, readRunRecords } from "./runs.js";

const DIRECTORY_ENTRY_SCHEMA = ["string", "string", "number", "number", "number", "string", "string"];

export function createDirectoryEntrySpool(path) {
  mkdirSync(dirname(path), { recursive: true });
  return {
    format: "rfdirectoryspool-v1",
    path,
    entries: 0,
    bytes: 0
  };
}

export function appendDirectoryEntry(spool, shard, entry) {
  if (!spool || !entry?.pack || !entry?.checksum?.value) return null;
  const record = encodeRunRecord(DIRECTORY_ENTRY_SCHEMA, [
    shard,
    entry.pack,
    entry.offset,
    entry.length,
    entry.logicalLength || 0,
    entry.checksum.algorithm || "sha256",
    entry.checksum.value
  ]);
  appendFileSync(spool.path, record);
  spool.entries++;
  spool.bytes += record.length;
  return entry;
}

export async function readDirectoryEntrySpool(spool, options = {}) {
  const entries = [];
  for await (const entry of sortedDirectoryEntrySpool(spool, options)) entries.push(entry);
  return entries;
}

function hydrateRecord(record, options) {
  const [shard, pack, offset, length, logicalLength, checksumAlgorithm, checksumValue] = record;
  const finalPack = options.packNameMap?.get(pack) || pack;
  return {
    shard,
    pack: finalPack,
    packIndex: options.packIndexes?.get(finalPack),
    offset,
    length,
    physicalLength: length,
    logicalLength,
    checksum: {
      algorithm: checksumAlgorithm || "sha256",
      value: checksumValue
    }
  };
}

function compareEntries(left, right) {
  return left.shard.localeCompare(right.shard) || left.pack.localeCompare(right.pack) || left.offset - right.offset;
}

function encodeEntry(entry) {
  return encodeRunRecord(DIRECTORY_ENTRY_SCHEMA, [
    entry.shard,
    entry.pack,
    entry.offset,
    entry.length,
    entry.logicalLength || 0,
    entry.checksum.algorithm || "sha256",
    entry.checksum.value
  ]);
}

function writeChunk(path, entries) {
  for (const entry of entries) appendFileSync(path, encodeEntry(entry));
}

class EntryHeap {
  constructor() {
    this.items = [];
  }

  push(item) {
    const items = this.items;
    items.push(item);
    let index = items.length - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (compareEntries(items[parent].entry, items[index].entry) <= 0) break;
      [items[parent], items[index]] = [items[index], items[parent]];
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
        if (left < items.length && compareEntries(items[left].entry, items[best].entry) < 0) best = left;
        if (right < items.length && compareEntries(items[right].entry, items[best].entry) < 0) best = right;
        if (best === index) break;
        [items[index], items[best]] = [items[best], items[index]];
        index = best;
      }
    }
    return out;
  }

  get size() {
    return this.items.length;
  }
}

async function nextIterator(iterator) {
  const result = await iterator.next();
  return result.done ? null : result.value;
}

async function* readChunkEntries(path, options) {
  for await (const record of readRunRecords(path, DIRECTORY_ENTRY_SCHEMA)) {
    yield hydrateRecord(record, options);
  }
}

export async function* sortedDirectoryEntrySpool(spool, options = {}) {
  const chunkEntries = Math.max(1, Math.floor(Number(options.chunkEntries || 16384)));
  const sourcePath = spool.path || spool;
  const tempDir = resolve(dirname(sourcePath), `directory-sort-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`);
  const chunks = [];
  let current = [];
  try {
    for await (const record of readRunRecords(sourcePath, DIRECTORY_ENTRY_SCHEMA)) {
      current.push(hydrateRecord(record, options));
      if (current.length >= chunkEntries) {
        current.sort(compareEntries);
        mkdirSync(tempDir, { recursive: true });
        const path = resolve(tempDir, `${String(chunks.length).padStart(6, "0")}.bin`);
        writeChunk(path, current);
        chunks.push(path);
        current = [];
      }
    }
    if (!chunks.length) {
      current.sort(compareEntries);
      for (const entry of current) yield entry;
      return;
    }
    if (current.length) {
      current.sort(compareEntries);
      mkdirSync(tempDir, { recursive: true });
      const path = resolve(tempDir, `${String(chunks.length).padStart(6, "0")}.bin`);
      writeChunk(path, current);
      chunks.push(path);
    }

    const iterators = chunks.map(path => readChunkEntries(path, options)[Symbol.asyncIterator]());
    const heap = new EntryHeap();
    for (let index = 0; index < iterators.length; index++) {
      const entry = await nextIterator(iterators[index]);
      if (entry) heap.push({ index, entry });
    }
    while (heap.size) {
      const item = heap.pop();
      yield item.entry;
      const next = await nextIterator(iterators[item.index]);
      if (next) heap.push({ index: item.index, entry: next });
    }
  } finally {
    if (chunks.length) rmSync(tempDir, { recursive: true, force: true });
  }
}
