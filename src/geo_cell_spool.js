import {
  closeSync,
  mkdirSync,
  openSync,
  rmSync,
  writeFileSync,
  writeSync
} from "node:fs";
import { dirname, resolve } from "node:path";
import { readRunRecords, varintLength, writeVarint } from "./runs.js";

const ROUTE_SCHEMA = new Array(10).fill("number");
const SPOOL_FLUSH_BYTES = 256 * 1024;

function compareTuple(left, right) {
  for (let index = 0; index < ROUTE_SCHEMA.length; index++) {
    if (left[index] !== right[index]) return left[index] - right[index];
  }
  return 0;
}

export function createGeoCellRouteSpool(path) {
  mkdirSync(dirname(path), { recursive: true });
  return {
    path,
    fd: null,
    pending: Buffer.allocUnsafe(SPOOL_FLUSH_BYTES + 128),
    pendingBytes: 0,
    records: 0,
    bytes: 0
  };
}

function flushGeoCellRouteSpool(spool) {
  if (!spool.pendingBytes) return;
  if (spool.fd == null) spool.fd = openSync(spool.path, "w");
  writeSync(spool.fd, spool.pending, 0, spool.pendingBytes);
  spool.pendingBytes = 0;
}

export function appendGeoCellRoute(spool, tuple) {
  let recordBytes = 0;
  for (const value of tuple) recordBytes += varintLength(value);
  if (spool.pendingBytes + recordBytes > SPOOL_FLUSH_BYTES) flushGeoCellRouteSpool(spool);
  let offset = spool.pendingBytes;
  for (const value of tuple) offset = writeVarint(spool.pending, offset, value);
  spool.pendingBytes = offset;
  spool.records++;
  spool.bytes += recordBytes;
  if (spool.pendingBytes >= SPOOL_FLUSH_BYTES) flushGeoCellRouteSpool(spool);
}

export function closeGeoCellRouteSpool(spool) {
  flushGeoCellRouteSpool(spool);
  if (spool.fd != null) {
    closeSync(spool.fd);
    spool.fd = null;
  }
}

function writeChunk(path, rows) {
  let bytes = 0;
  for (const row of rows) {
    for (const value of row) bytes += varintLength(value);
  }
  const buffer = Buffer.allocUnsafe(bytes);
  let offset = 0;
  for (const row of rows) {
    for (const value of row) offset = writeVarint(buffer, offset, value);
  }
  writeFileSync(path, buffer);
}

class TupleHeap {
  constructor() {
    this.items = [];
  }

  push(item) {
    this.items.push(item);
    let index = this.items.length - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (compareTuple(this.items[parent].row, this.items[index].row) <= 0) break;
      [this.items[parent], this.items[index]] = [this.items[index], this.items[parent]];
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
        if (left < this.items.length && compareTuple(this.items[left].row, this.items[best].row) < 0) best = left;
        if (right < this.items.length && compareTuple(this.items[right].row, this.items[best].row) < 0) best = right;
        if (best === index) break;
        [this.items[index], this.items[best]] = [this.items[best], this.items[index]];
        index = best;
      }
    }
    return first;
  }
}

async function next(iterator) {
  const value = await iterator.next();
  return value.done ? null : value.value;
}

export async function* sortedGeoCellRoutes(spool, options = {}) {
  closeGeoCellRouteSpool(spool);
  const chunkRecords = Math.max(1, Math.floor(Number(options.chunkRecords || 262144)));
  const onProgress = typeof options.onProgress === "function" ? options.onProgress : null;
  const tempDir = resolve(
    dirname(spool.path),
    `geo-cell-sort-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`
  );
  const chunks = [];
  let rows = [];
  try {
    for await (const row of readRunRecords(spool.path, ROUTE_SCHEMA)) {
      rows.push(row);
      if (rows.length >= chunkRecords) {
        rows.sort(compareTuple);
        mkdirSync(tempDir, { recursive: true });
        const path = resolve(tempDir, `${String(chunks.length).padStart(6, "0")}.bin`);
        writeChunk(path, rows);
        chunks.push(path);
        onProgress?.("geo-cell-sort", Math.min(spool.records, chunks.length * chunkRecords), spool.records);
        rows = [];
      }
    }
    if (!chunks.length) {
      rows.sort(compareTuple);
      onProgress?.("geo-cell-sort", spool.records, spool.records);
      for (const row of rows) yield row;
      onProgress?.("geo-cell-merge", rows.length, spool.records);
      return;
    }
    if (rows.length) {
      rows.sort(compareTuple);
      const path = resolve(tempDir, `${String(chunks.length).padStart(6, "0")}.bin`);
      writeChunk(path, rows);
      chunks.push(path);
    }
    const iterators = chunks.map(path => readRunRecords(path, ROUTE_SCHEMA)[Symbol.asyncIterator]());
    const heap = new TupleHeap();
    let merged = 0;
    for (let index = 0; index < iterators.length; index++) {
      const row = await next(iterators[index]);
      if (row) heap.push({ index, row });
    }
    while (heap.items.length) {
      const item = heap.pop();
      yield item.row;
      merged++;
      if (merged % chunkRecords === 0) onProgress?.("geo-cell-merge", merged, spool.records);
      const row = await next(iterators[item.index]);
      if (row) heap.push({ index: item.index, row });
    }
    onProgress?.("geo-cell-merge", merged, spool.records);
  } finally {
    if (chunks.length) rmSync(tempDir, { recursive: true, force: true });
  }
}
