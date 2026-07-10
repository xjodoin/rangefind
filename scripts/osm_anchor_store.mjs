import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readSync,
  renameSync,
  rmSync,
  statSync,
  writeSync
} from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";

const REF_BYTES = 8;
const REF_BUFFER_BYTES = 1024 * 1024;
const require = createRequire(import.meta.url);
let DatabaseSync = null;

function writeAll(fd, buffer) {
  let offset = 0;
  while (offset < buffer.length) offset += writeSync(fd, buffer, offset, buffer.length - offset);
}

export function createAnchorRefWriter(path) {
  mkdirSync(dirname(resolve(path)), { recursive: true });
  const fd = openSync(path, "w");
  const buffer = Buffer.allocUnsafe(REF_BUFFER_BYTES);
  let position = 0;
  let count = 0;
  let closed = false;

  function flush() {
    if (!position) return;
    writeAll(fd, buffer.subarray(0, position));
    position = 0;
  }

  return {
    write(ref) {
      if (position + REF_BYTES > buffer.length) flush();
      buffer.writeBigUInt64LE(BigInt(ref), position);
      position += REF_BYTES;
      count++;
    },
    close() {
      if (closed) return;
      flush();
      closeSync(fd);
      closed = true;
    },
    get count() {
      return count;
    }
  };
}

function readFully(fd, buffer, bytes, fileOffset) {
  let offset = 0;
  while (offset < bytes) {
    const read = readSync(fd, buffer, offset, bytes - offset, fileOffset + offset);
    if (!read) break;
    offset += read;
  }
  return offset;
}

class AnchorReader {
  constructor(path) {
    this.path = path;
    this.fd = openSync(path, "r");
    this.size = statSync(path).size;
    this.fileOffset = 0;
    this.buffer = Buffer.allocUnsafe(REF_BUFFER_BYTES);
    this.position = 0;
    this.length = 0;
    this.value = null;
    this.advance();
  }

  advance() {
    if (this.position >= this.length) {
      const remaining = this.size - this.fileOffset;
      if (remaining <= 0) {
        this.value = null;
        return null;
      }
      const wanted = Math.min(this.buffer.length, remaining);
      const read = readFully(this.fd, this.buffer, wanted, this.fileOffset);
      if (read % REF_BYTES) throw new Error(`OSM anchor file ${this.path} is truncated.`);
      this.fileOffset += read;
      this.position = 0;
      this.length = read;
    }
    this.value = this.buffer.readBigUInt64LE(this.position);
    this.position += REF_BYTES;
    return this.value;
  }

  close() {
    closeSync(this.fd);
  }
}

class MinHeap {
  constructor() {
    this.items = [];
  }

  push(item) {
    const items = this.items;
    items.push(item);
    let index = items.length - 1;
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (items[parent].value <= items[index].value) break;
      [items[parent], items[index]] = [items[index], items[parent]];
      index = parent;
    }
  }

  pop() {
    if (!this.items.length) return null;
    const first = this.items[0];
    const last = this.items.pop();
    if (this.items.length) {
      this.items[0] = last;
      let index = 0;
      for (;;) {
        const left = index * 2 + 1;
        const right = left + 1;
        let best = index;
        if (left < this.items.length && this.items[left].value < this.items[best].value) best = left;
        if (right < this.items.length && this.items[right].value < this.items[best].value) best = right;
        if (best === index) break;
        [this.items[index], this.items[best]] = [this.items[best], this.items[index]];
        index = best;
      }
    }
    return first;
  }

  get size() {
    return this.items.length;
  }
}

function writeSortedUniqueRun(values, path) {
  values.sort();
  const writer = createAnchorRefWriter(path);
  let previous = null;
  try {
    for (const value of values) {
      if (value === previous) continue;
      writer.write(value);
      previous = value;
    }
  } finally {
    writer.close();
  }
}

export function sortUniqueAnchorRefs(inputPath, outputPath, scratchDir, options = {}) {
  const chunkBytes = Math.max(REF_BYTES, Math.floor(Number(options.chunkBytes || 64 * 1024 * 1024) / REF_BYTES) * REF_BYTES);
  const size = statSync(inputPath).size;
  if (size % REF_BYTES) throw new Error(`OSM anchor file ${inputPath} is truncated.`);
  mkdirSync(scratchDir, { recursive: true });
  const input = openSync(inputPath, "r");
  const runs = [];
  try {
    for (let offset = 0, index = 0; offset < size; index++) {
      const bytes = Math.min(chunkBytes, size - offset);
      const buffer = Buffer.allocUnsafeSlow(bytes);
      if (readFully(input, buffer, bytes, offset) !== bytes) throw new Error(`OSM anchor file ${inputPath} ended early.`);
      const values = new BigUint64Array(buffer.buffer, buffer.byteOffset, bytes / REF_BYTES);
      const run = resolve(scratchDir, `anchors-${String(index).padStart(4, "0")}.bin`);
      writeSortedUniqueRun(values, run);
      runs.push(run);
      offset += bytes;
    }
  } finally {
    closeSync(input);
  }

  rmSync(outputPath, { force: true });
  if (!runs.length) {
    createAnchorRefWriter(outputPath).close();
    return { count: 0, runs: 0, bytes: 0 };
  }
  if (runs.length === 1) {
    renameSync(runs[0], outputPath);
    const bytes = statSync(outputPath).size;
    return { count: bytes / REF_BYTES, runs: 1, bytes };
  }

  const readers = runs.map(path => new AnchorReader(path));
  const heap = new MinHeap();
  for (let index = 0; index < readers.length; index++) {
    if (readers[index].value != null) heap.push({ index, value: readers[index].value });
  }
  const writer = createAnchorRefWriter(outputPath);
  let previous = null;
  try {
    while (heap.size) {
      const item = heap.pop();
      if (item.value !== previous) {
        writer.write(item.value);
        previous = item.value;
      }
      const reader = readers[item.index];
      reader.advance();
      if (reader.value != null) heap.push({ index: item.index, value: reader.value });
    }
  } finally {
    writer.close();
    for (const reader of readers) reader.close();
    for (const run of runs) rmSync(run, { force: true });
  }
  const bytes = statSync(outputPath).size;
  return { count: bytes / REF_BYTES, runs: runs.length, bytes };
}

export function openSortedAnchorRefs(path) {
  const reader = new AnchorReader(path);
  return {
    get current() {
      return reader.value == null ? null : Number(reader.value);
    },
    advance() {
      reader.advance();
      return reader.value == null ? null : Number(reader.value);
    },
    close() {
      reader.close();
    }
  };
}

export function createCoordinateStore(path, options = {}) {
  DatabaseSync ||= require("node:sqlite").DatabaseSync;
  if (options.reset) rmSync(path, { force: true });
  mkdirSync(dirname(resolve(path)), { recursive: true });
  const database = new DatabaseSync(path);
  database.exec("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA temp_store=MEMORY; PRAGMA locking_mode=EXCLUSIVE;");
  database.exec("CREATE TABLE IF NOT EXISTS coords (ref INTEGER PRIMARY KEY, lat REAL NOT NULL, lon REAL NOT NULL) WITHOUT ROWID");
  const insert = database.prepare("INSERT OR REPLACE INTO coords (ref, lat, lon) VALUES (?, ?, ?)");
  const lookup = database.prepare("SELECT lat, lon FROM coords WHERE ref = ?");
  let transactionRows = 0;
  let transactionOpen = false;

  function begin() {
    if (transactionOpen) return;
    database.exec("BEGIN");
    transactionOpen = true;
  }

  function commit() {
    if (!transactionOpen) return;
    database.exec("COMMIT");
    transactionOpen = false;
    transactionRows = 0;
  }

  return {
    put(ref, lat, lon) {
      begin();
      insert.run(ref, lat, lon);
      transactionRows++;
      if (transactionRows >= 100000) commit();
    },
    get(ref) {
      const row = lookup.get(ref);
      return row ? { lat: row.lat, lon: row.lon } : null;
    },
    count() {
      commit();
      return Number(database.prepare("SELECT COUNT(*) AS count FROM coords").get().count);
    },
    close() {
      commit();
      database.close();
    }
  };
}

export function coordinateStoreExists(path) {
  return existsSync(path) && statSync(path).size > 0;
}
