import {
  closeSync,
  mkdirSync,
  openSync,
  rmSync,
  writeFileSync,
  writeSync
} from "node:fs";
import { dirname, resolve } from "node:path";
import { encodeRunRecord, readRunRecords } from "./runs.js";

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
    pending: [],
    pendingBytes: 0,
    records: 0,
    bytes: 0
  };
}

function flushGeoCellRouteSpool(spool) {
  if (!spool.pending.length) return;
  if (spool.fd == null) spool.fd = openSync(spool.path, "w");
  const bytes = Buffer.concat(spool.pending, spool.pendingBytes);
  writeSync(spool.fd, bytes, 0, bytes.length);
  spool.pending.length = 0;
  spool.pendingBytes = 0;
}

export function appendGeoCellRoute(spool, tuple) {
  const record = encodeRunRecord(ROUTE_SCHEMA, tuple);
  spool.pending.push(record);
  spool.pendingBytes += record.length;
  spool.records++;
  spool.bytes += record.length;
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
  writeFileSync(path, Buffer.concat(rows.map(row => encodeRunRecord(ROUTE_SCHEMA, row))));
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
        rows = [];
      }
    }
    if (!chunks.length) {
      rows.sort(compareTuple);
      for (const row of rows) yield row;
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
    for (let index = 0; index < iterators.length; index++) {
      const row = await next(iterators[index]);
      if (row) heap.push({ index, row });
    }
    while (heap.items.length) {
      const item = heap.pop();
      yield item.row;
      const row = await next(iterators[item.index]);
      if (row) heap.push({ index: item.index, row });
    }
  } finally {
    if (chunks.length) rmSync(tempDir, { recursive: true, force: true });
  }
}
