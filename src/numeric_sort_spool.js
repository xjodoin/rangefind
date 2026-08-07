import {
  closeSync,
  mkdirSync,
  openSync,
  readSync,
  rmSync,
  unlinkSync,
  writeFileSync,
  writeSync
} from "node:fs";
import { resolve } from "node:path";

const RECORD_BYTES = 12;
const DEFAULT_CHUNK_ROWS = 1_048_576;
const DEFAULT_MAX_OPEN_RUNS = 64;
const DEFAULT_READ_BUFFER_BYTES = 256 * 1024;
const DEFAULT_WRITE_BUFFER_BYTES = 1024 * 1024;

function positiveInteger(value, fallback) {
  const parsed = Math.floor(Number(value));
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function compareRows(leftValue, leftDoc, rightValue, rightDoc) {
  return leftValue - rightValue || leftDoc - rightDoc;
}

function writeAll(fd, buffer, length = buffer.length) {
  let offset = 0;
  while (offset < length) {
    const written = writeSync(fd, buffer, offset, length - offset);
    if (!written) throw new Error("Rangefind numeric sort write made no progress.");
    offset += written;
  }
}

function writeSortedRun(path, docs, values, count) {
  const order = new Uint32Array(count);
  for (let index = 0; index < count; index++) order[index] = index;
  order.sort((left, right) => compareRows(values[left], docs[left], values[right], docs[right]));
  const buffer = Buffer.allocUnsafe(count * RECORD_BYTES);
  for (let rank = 0, offset = 0; rank < count; rank++, offset += RECORD_BYTES) {
    const index = order[rank];
    buffer.writeDoubleLE(values[index], offset);
    buffer.writeUInt32LE(docs[index], offset + 8);
  }
  writeFileSync(path, buffer);
  return buffer.length;
}

function createRunReader(path, bufferBytes) {
  const bytes = Math.max(RECORD_BYTES, Math.floor(bufferBytes / RECORD_BYTES) * RECORD_BYTES);
  return {
    fd: openSync(path, "r"),
    buffer: Buffer.allocUnsafe(bytes),
    buffered: 0,
    offset: 0,
    position: 0,
    value: 0,
    doc: 0,
    done: false
  };
}

function closeRunReader(reader) {
  if (reader.fd != null) closeSync(reader.fd);
  reader.fd = null;
}

function nextRunRow(reader) {
  if (reader.done) return false;
  if (reader.offset >= reader.buffered) {
    reader.buffered = readSync(reader.fd, reader.buffer, 0, reader.buffer.length, reader.position);
    reader.position += reader.buffered;
    reader.offset = 0;
    if (!reader.buffered) {
      reader.done = true;
      return false;
    }
    if (reader.buffered % RECORD_BYTES !== 0) throw new Error("Rangefind numeric sort run has a truncated record.");
  }
  reader.value = reader.buffer.readDoubleLE(reader.offset);
  reader.doc = reader.buffer.readUInt32LE(reader.offset + 8);
  reader.offset += RECORD_BYTES;
  return true;
}

function heapPush(heap, readerIndex, readers) {
  heap.push(readerIndex);
  let child = heap.length - 1;
  while (child > 0) {
    const parent = Math.floor((child - 1) / 2);
    const parentReader = readers[heap[parent]];
    const childReader = readers[heap[child]];
    if (compareRows(parentReader.value, parentReader.doc, childReader.value, childReader.doc) <= 0) break;
    [heap[parent], heap[child]] = [heap[child], heap[parent]];
    child = parent;
  }
}

function heapPop(heap, readers) {
  const first = heap[0];
  const last = heap.pop();
  if (heap.length) {
    heap[0] = last;
    let parent = 0;
    while (true) {
      const left = parent * 2 + 1;
      const right = left + 1;
      if (left >= heap.length) break;
      let child = left;
      const leftReader = readers[heap[left]];
      if (right < heap.length) {
        const rightReader = readers[heap[right]];
        if (compareRows(rightReader.value, rightReader.doc, leftReader.value, leftReader.doc) < 0) child = right;
      }
      const parentReader = readers[heap[parent]];
      const childReader = readers[heap[child]];
      if (compareRows(parentReader.value, parentReader.doc, childReader.value, childReader.doc) <= 0) break;
      [heap[parent], heap[child]] = [heap[child], heap[parent]];
      parent = child;
    }
  }
  return first;
}

function visitMergedRuns(paths, options, visit) {
  const readers = paths.map(path => createRunReader(path, options.readBufferBytes));
  const heap = [];
  try {
    for (let index = 0; index < readers.length; index++) {
      if (nextRunRow(readers[index])) heapPush(heap, index, readers);
    }
    while (heap.length) {
      const index = heapPop(heap, readers);
      const reader = readers[index];
      visit(reader.doc, reader.value);
      if (nextRunRow(reader)) heapPush(heap, index, readers);
    }
  } finally {
    for (const reader of readers) closeRunReader(reader);
  }
}

function mergeRunGroup(paths, target, options) {
  const fd = openSync(target, "w");
  const output = Buffer.allocUnsafe(
    Math.max(RECORD_BYTES, Math.floor(options.writeBufferBytes / RECORD_BYTES) * RECORD_BYTES)
  );
  let offset = 0;
  try {
    visitMergedRuns(paths, options, (doc, value) => {
      output.writeDoubleLE(value, offset);
      output.writeUInt32LE(doc, offset + 8);
      offset += RECORD_BYTES;
      if (offset === output.length) {
        writeAll(fd, output, offset);
        offset = 0;
      }
    });
    if (offset) writeAll(fd, output, offset);
  } finally {
    closeSync(fd);
  }
}

function reduceRunFanIn(paths, tempDir, options) {
  let current = paths;
  let pass = 0;
  while (current.length > options.maxOpenRuns) {
    const next = [];
    for (let start = 0; start < current.length; start += options.maxOpenRuns) {
      const group = current.slice(start, start + options.maxOpenRuns);
      const path = resolve(tempDir, `merge-${String(pass).padStart(3, "0")}-${String(next.length).padStart(6, "0")}.bin`);
      mergeRunGroup(group, path, options);
      next.push(path);
    }
    for (const path of current) unlinkSync(path);
    current = next;
    pass++;
  }
  return { paths: current, passes: pass };
}

export function forEachExternallySortedNumericRow(options, visit) {
  if (typeof options?.readValues !== "function") throw new Error("Rangefind numeric sort needs a readValues callback.");
  if (typeof options?.valueOf !== "function") throw new Error("Rangefind numeric sort needs a valueOf callback.");
  if (typeof visit !== "function") throw new Error("Rangefind numeric sort needs a row visitor.");
  const total = Math.max(0, Math.floor(Number(options.total) || 0));
  if (total > 0xffffffff) throw new Error("Rangefind numeric sort supports at most 4,294,967,295 documents.");
  const chunkRows = positiveInteger(options.chunkRows, DEFAULT_CHUNK_ROWS);
  const readChunkRows = positiveInteger(options.readChunkRows, Math.min(chunkRows, 2048));
  const settings = {
    maxOpenRuns: Math.max(2, positiveInteger(options.maxOpenRuns, DEFAULT_MAX_OPEN_RUNS)),
    readBufferBytes: positiveInteger(options.readBufferBytes, DEFAULT_READ_BUFFER_BYTES),
    writeBufferBytes: positiveInteger(options.writeBufferBytes, DEFAULT_WRITE_BUFFER_BYTES)
  };
  const baseDir = resolve(options.tempDir || ".");
  const tempDir = resolve(baseDir, `numeric-sort-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}`);
  const docs = new Uint32Array(Math.min(total || 1, chunkRows));
  const values = new Float64Array(docs.length);
  const runs = [];
  let pending = 0;
  let rows = 0;
  let tempBytes = 0;

  const flush = () => {
    if (!pending) return;
    mkdirSync(tempDir, { recursive: true });
    const path = resolve(tempDir, `${String(runs.length).padStart(6, "0")}.bin`);
    tempBytes += writeSortedRun(path, docs, values, pending);
    runs.push(path);
    pending = 0;
  };

  try {
    for (let start = 0; start < total; start += readChunkRows) {
      const count = Math.min(readChunkRows, total - start);
      const source = options.readValues(start, count);
      if (!source || source.length !== count) {
        throw new Error(`Rangefind numeric sort read ${source?.length ?? 0} of ${count} values at document ${start}.`);
      }
      for (let index = 0; index < count; index++) {
        const value = options.valueOf(source[index]);
        if (!Number.isFinite(value)) continue;
        docs[pending] = start + index;
        values[pending] = value;
        pending++;
        rows++;
        if (pending === docs.length) flush();
      }
      options.onProgress?.("spill", start + count, total);
    }
    flush();
    if (!runs.length) return { rows: 0, runs: 0, mergePasses: 0, tempBytes: 0, chunkRows };
    const reduced = reduceRunFanIn(runs, tempDir, settings);
    let merged = 0;
    visitMergedRuns(reduced.paths, settings, (doc, value) => {
      visit(doc, value);
      merged++;
      if (merged % chunkRows === 0) options.onProgress?.("merge", merged, rows);
    });
    if (merged !== rows) throw new Error(`Rangefind numeric sort merged ${merged} of ${rows} rows.`);
    options.onProgress?.("merge", merged, rows);
    return { rows, runs: runs.length, mergePasses: reduced.passes, tempBytes, chunkRows };
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
}
