import { closeSync, mkdirSync, openSync, readFileSync, readSync, statSync, writeSync } from "node:fs";
import { resolve } from "node:path";
import { docValueFields } from "./codec.js";

function readFileIntoShared(path) {
  const size = statSync(path).size;
  const shared = new SharedArrayBuffer(size);
  const view = Buffer.from(shared);
  const fd = openSync(path, "r");
  try {
    let offset = 0;
    while (offset < size) {
      const bytesRead = readSync(fd, view, offset, Math.min(size - offset, 1 << 26), offset);
      if (!bytesRead) throw new Error(`Rangefind build code store could not preload ${path}.`);
      offset += bytesRead;
    }
  } finally {
    closeSync(fd);
  }
  return shared;
}

export function preloadCodeStoreDescriptor(descriptor, maxBytes, options = {}) {
  if (!descriptor?.fields?.length) return descriptor;
  const limit = Math.max(0, Math.floor(Number(maxBytes || 0)));
  if (!limit) return descriptor;
  const sizes = descriptor.fields.map(field => ({
    field,
    bytes: statSync(field.path).size + (field.indexPath ? statSync(field.indexPath).size : 0)
  }));
  const total = sizes.reduce((sum, item) => sum + item.bytes, 0);
  if (total > limit && options.bestEffort !== true) return descriptor;
  let preloadedBytes = 0;
  const preloadedFields = [];
  const skippedFields = [];
  return {
    ...descriptor,
    fields: sizes.map(({ field, bytes }) => {
      if (preloadedBytes + bytes > limit) {
        skippedFields.push(field.name);
        return field;
      }
      preloadedBytes += bytes;
      preloadedFields.push(field.name);
      return {
        ...field,
        sharedData: readFileIntoShared(field.path),
        sharedIndex: field.indexPath ? readFileIntoShared(field.indexPath) : null
      };
    }),
    preloadedBytes,
    preloadedFields,
    skippedFields
  };
}

export const CODE_STORE_FORMAT = "rf-build-code-store-v2";
const LEGACY_CODE_STORE_FORMAT = "rf-build-code-store-v1";
const LEGACY_FACET_INDEX_BYTES = 16;
const COMPACT_FACET_INDEX_BYTES = 4;
const COMPACT_FACET_MULTI_FLAG = 0x80000000;
const COMPACT_FACET_VALUE_MASK = 0x7fffffff;

function safeFieldName(value) {
  return String(value || "field").replace(/[^A-Za-z0-9_-]+/gu, "_").replace(/^_+|_+$/gu, "") || "field";
}

function bytesPerDoc(field, format = CODE_STORE_FORMAT) {
  if (field.kind === "facet") return format === LEGACY_CODE_STORE_FORMAT
    ? LEGACY_FACET_INDEX_BYTES
    : COMPACT_FACET_INDEX_BYTES;
  if (field.kind === "boolean") return 1;
  return 8;
}

function facetCodes(value) {
  const values = Array.isArray(value) ? value : value?.codes || [];
  return [...new Set(values.map(Number).filter(Number.isFinite))].sort((a, b) => a - b);
}

function normalizedBoolean(value) {
  if (value === true || value === 1 || value === "true" || value === "1") return 2;
  if (value === false || value === 0 || value === "false" || value === "0") return 1;
  return 0;
}

function writeValue(buffer, field, value) {
  if (field.kind === "boolean") {
    buffer[0] = normalizedBoolean(value);
    return;
  }
  const number = value == null || value === "" ? Number.NaN : Number(value);
  buffer.writeDoubleLE(Number.isFinite(number) ? number : Number.NaN, 0);
}

function readValue(buffer, field, row) {
  const offset = row * field.bytesPerDoc;
  if (field.kind === "boolean") {
    const value = buffer[offset];
    return value === 0 ? null : value === 2;
  }
  const number = buffer.readDoubleLE(offset);
  return Number.isNaN(number) ? null : number;
}

function createReader(descriptor, openMode) {
  const format = descriptor.format || LEGACY_CODE_STORE_FORMAT;
  const fields = descriptor.fields.map(field => ({
    ...field,
    fd: field.sharedData ? null : openSync(field.path, openMode),
    indexFd: field.kind === "facet" && !field.sharedData ? openSync(field.indexPath, openMode) : null,
    dataView: field.sharedData ? Buffer.from(field.sharedData) : null,
    indexView: field.sharedIndex ? Buffer.from(field.sharedIndex) : null,
    cache: new Map(),
    bytesPerDoc: field.bytesPerDoc || bytesPerDoc(field, format),
    offset: 0
  }));
  const byName = new Map(fields.map(field => [field.name, field]));
  const total = descriptor.total;
  const cacheDocs = Math.max(1, Math.floor(Number(descriptor.cacheDocs || 16384)));
  const cacheChunks = Math.max(1, Math.floor(Number(descriptor.cacheChunks || 1)));

  function fieldFor(name) {
    const field = byName.get(name);
    if (!field) throw new Error(`Rangefind build code store is missing field ${name}.`);
    return field;
  }

  function readRange(field, start, count) {
    const buffer = Buffer.alloc(count * field.bytesPerDoc);
    const bytesRead = readSync(field.fd, buffer, 0, buffer.length, start * field.bytesPerDoc);
    if (bytesRead !== buffer.length) {
      throw new Error(`Rangefind build code store ended before field ${field.name} row ${start + count}.`);
    }
    return buffer;
  }

  function readFacetValue(field, doc) {
    if (field.bytesPerDoc === COMPACT_FACET_INDEX_BYTES) {
      const indexOffset = doc * COMPACT_FACET_INDEX_BYTES;
      let encoded;
      if (field.indexView) {
        encoded = field.indexView.readUInt32LE(indexOffset);
      } else {
        const index = Buffer.allocUnsafe(COMPACT_FACET_INDEX_BYTES);
        const indexBytes = readSync(field.indexFd, index, 0, index.length, indexOffset);
        if (indexBytes !== index.length) throw new Error(`Rangefind build code store ended before facet ${field.name} row ${doc}.`);
        encoded = index.readUInt32LE(0);
      }
      if (encoded === 0) return { codes: [] };
      if ((encoded & COMPACT_FACET_MULTI_FLAG) === 0) return { codes: [encoded - 1] };
      const offset = (encoded & COMPACT_FACET_VALUE_MASK) * 4;
      const countBuffer = field.dataView
        ? field.dataView.subarray(offset, offset + 4)
        : Buffer.allocUnsafe(4);
      if (!field.dataView) {
        const bytesRead = readSync(field.fd, countBuffer, 0, 4, offset);
        if (bytesRead !== 4) throw new Error(`Rangefind build code store ended inside facet ${field.name} row ${doc}.`);
      }
      const count = countBuffer.readUInt32LE(0);
      const codes = new Array(count);
      if (!count) return { codes };
      if (field.dataView) {
        for (let i = 0; i < count; i++) codes[i] = field.dataView.readUInt32LE(offset + 4 + i * 4);
      } else {
        const data = Buffer.allocUnsafe(count * 4);
        const bytesRead = readSync(field.fd, data, 0, data.length, offset + 4);
        if (bytesRead !== data.length) throw new Error(`Rangefind build code store ended inside facet ${field.name} row ${doc}.`);
        for (let i = 0; i < count; i++) codes[i] = data.readUInt32LE(i * 4);
      }
      return { codes };
    }
    if (field.indexView) {
      const offset = readBigUInt(field.indexView, doc * LEGACY_FACET_INDEX_BYTES);
      const length = readBigUInt(field.indexView, doc * LEGACY_FACET_INDEX_BYTES + 8);
      if (!length) return { codes: [] };
      const codes = new Array(length / 4);
      for (let i = 0; i < codes.length; i++) codes[i] = field.dataView.readUInt32LE(offset + i * 4);
      return { codes };
    }
    const index = Buffer.alloc(LEGACY_FACET_INDEX_BYTES);
    const indexBytes = readSync(field.indexFd, index, 0, index.length, doc * LEGACY_FACET_INDEX_BYTES);
    if (indexBytes !== index.length) throw new Error(`Rangefind build code store ended before facet ${field.name} row ${doc}.`);
    const offset = readBigUInt(index, 0);
    const length = readBigUInt(index, 8);
    if (!length) return { codes: [] };
    const data = Buffer.alloc(length);
    const bytesRead = readSync(field.fd, data, 0, data.length, offset);
    if (bytesRead !== data.length) throw new Error(`Rangefind build code store ended inside facet ${field.name} row ${doc}.`);
    const codes = new Array(length / 4);
    for (let i = 0; i < codes.length; i++) codes[i] = data.readUInt32LE(i * 4);
    return { codes };
  }

  function readFacetChunk(field, start, count) {
    const out = new Array(count);
    for (let row = 0; row < count; row++) out[row] = readFacetValue(field, start + row);
    return out;
  }

  function cacheGet(field, chunkIndex, createValue) {
    if (field.cache.has(chunkIndex)) {
      const value = field.cache.get(chunkIndex);
      field.cache.delete(chunkIndex);
      field.cache.set(chunkIndex, value);
      return value;
    }
    const start = chunkIndex * cacheDocs;
    const count = Math.min(cacheDocs, total - start);
    const value = createValue(start, count);
    field.cache.set(chunkIndex, value);
    while (field.cache.size > cacheChunks) {
      const oldest = field.cache.keys().next().value;
      field.cache.delete(oldest);
    }
    return value;
  }

  function preloadFields(names, maxBytes) {
    const limit = Math.max(0, Math.floor(Number(maxBytes || 0)));
    const candidates = names == null
      ? fields.filter(field => !field.dataView)
      : [...new Set(Array.from(names, String))]
        .map(name => byName.get(name))
        .filter(field => field && !field.dataView);
    const loadedFields = [];
    const skippedFields = [];
    let preloadedBytes = 0;
    for (const field of candidates) {
      const bytes = statSync(field.path).size + (field.indexPath ? statSync(field.indexPath).size : 0);
      if (limit && preloadedBytes + bytes > limit) {
        skippedFields.push(field.name);
        continue;
      }
      field.dataView = readFileSync(field.path);
      if (field.indexPath) field.indexView = readFileSync(field.indexPath);
      field.cache.clear();
      loadedFields.push(field.name);
      preloadedBytes += bytes;
    }
    return { loadedFields, skippedFields, preloadedBytes };
  }

  return {
    format,
    _fieldRecords: fields,
    _fields: fields.map(({ fd, indexFd, cache, offset, ...field }) => ({ ...field })),
    _dicts: descriptor.dicts || {},
    total,
    get(name, doc) {
      if (doc < 0 || doc >= total) return null;
      const field = fieldFor(name);
      if (field.dataView) {
        return field.kind === "facet" ? readFacetValue(field, doc) : readValue(field.dataView, field, doc);
      }
      const chunkIndex = Math.floor(doc / cacheDocs);
      const chunk = cacheGet(field, chunkIndex, (start, count) => field.kind === "facet"
        ? { start, count, values: readFacetChunk(field, start, count) }
        : { start, count, buffer: readRange(field, start, count) });
      return field.kind === "facet"
        ? chunk.values[doc - chunk.start]
        : readValue(chunk.buffer, field, doc - chunk.start);
    },
    chunk(name, start, count) {
      const field = fieldFor(name);
      const safeStart = Math.max(0, Math.min(total, start));
      const safeCount = Math.max(0, Math.min(count, total - safeStart));
      if (field.kind === "facet") {
        if (field.dataView) {
          const out = new Array(safeCount);
          for (let row = 0; row < safeCount; row++) out[row] = readFacetValue(field, safeStart + row);
          return out;
        }
        return readFacetChunk(field, safeStart, safeCount);
      }
      if (field.dataView) {
        const out = new Array(safeCount);
        for (let row = 0; row < safeCount; row++) out[row] = readValue(field.dataView, field, safeStart + row);
        return out;
      }
      const buffer = readRange(field, safeStart, safeCount);
      const out = new Array(safeCount);
      for (let row = 0; row < safeCount; row++) out[row] = readValue(buffer, field, row);
      return out;
    },
    descriptor() {
      return {
        ...descriptor,
        dicts: undefined,
        fields: fields.map(({ fd, indexFd, cache, offset, dataView, indexView, ...field }) => ({ ...field })),
        cacheChunks
      };
    },
    preload(maxBytes) {
      const limit = Math.max(0, Math.floor(Number(maxBytes || 0)));
      let totalBytes = 0;
      for (const field of fields) {
        if (field.dataView) continue;
        totalBytes += statSync(field.path).size;
        if (field.indexPath) totalBytes += statSync(field.indexPath).size;
      }
      if (limit && totalBytes > limit) return false;
      preloadFields(null, limit);
      return true;
    },
    // Preload only the fields a random-access stage needs. Unlike preload(),
    // this is deliberately best-effort: fields are loaded in caller priority
    // order until the byte budget is exhausted. A single oversized field no
    // longer prevents every smaller hot field from using the memory fast path.
    preloadFields,
    close() {
      for (const field of fields) {
        if (field.fd != null) closeSync(field.fd);
        if (field.indexFd != null) closeSync(field.indexFd);
        field.fd = null;
        field.indexFd = null;
      }
    }
  };
}

function readBigUInt(buffer, offset) {
  return Number(buffer.readBigUInt64LE(offset));
}

function writeBigUInt(buffer, offset, value) {
  buffer.writeBigUInt64LE(BigInt(Math.max(0, Math.floor(value || 0))), offset);
}

function createBufferedSink(fd, capacity) {
  let buffer = Buffer.allocUnsafe(capacity);
  let start = 0;
  let length = 0;
  function flush() {
    if (!length) return;
    writeSync(fd, buffer, 0, length, start);
    length = 0;
  }
  return {
    write(source, position) {
      if (source.length > buffer.length) {
        flush();
        writeSync(fd, source, 0, source.length, position);
        return;
      }
      if (length && (position !== start + length || length + source.length > buffer.length)) flush();
      if (!length) start = position;
      source.copy(buffer, length);
      length += source.length;
    },
    flush
  };
}

export function createCodeStore(outDir, config, total, dicts, options = {}) {
  mkdirSync(outDir, { recursive: true });
  const fields = docValueFields(config, { _dicts: dicts }).map((field, index) => {
    const path = resolve(outDir, `${String(index).padStart(3, "0")}-${safeFieldName(field.name)}.bin`);
    const indexPath = field.kind === "facet"
      ? resolve(outDir, `${String(index).padStart(3, "0")}-${safeFieldName(field.name)}.idx`)
      : null;
    return {
      ...field,
      path,
      indexPath,
      bytesPerDoc: bytesPerDoc(field, CODE_STORE_FORMAT)
    };
  });
  const descriptor = {
    format: CODE_STORE_FORMAT,
    total,
    cacheDocs: Math.max(1, Math.floor(Number(options.cacheDocs || config.codeStoreCacheDocs || 16384))),
    cacheChunks: Math.max(1, Math.floor(Number(options.cacheChunks || config.codeStoreCacheChunks || 8))),
    fields,
    dicts
  };
  const store = createReader(descriptor, "w+");
  const writeBufferBytes = Math.max(4096, Math.floor(Number(options.writeBufferBytes || config.codeStoreWriteBufferBytes || 1024 * 1024)));
  const sinks = new Map(store._fieldRecords.map(field => [field.name, {
    data: createBufferedSink(field.fd, writeBufferBytes),
    index: field.indexFd == null ? null : createBufferedSink(field.indexFd, writeBufferBytes)
  }]));
  const writeFields = new Map(store._fieldRecords.map(field => [field.name, field]));
  const scratch = new Map(store._fields.map(field => [field.name, Buffer.alloc(field.bytesPerDoc)]));

  function flushWrites() {
    for (const sink of sinks.values()) {
      sink.data.flush();
      sink.index?.flush();
    }
  }

  for (const method of ["get", "chunk", "descriptor", "preload", "preloadFields", "close"]) {
    const original = store[method].bind(store);
    store[method] = (...args) => {
      flushWrites();
      return original(...args);
    };
  }

  store.set = (name, doc, value) => {
    const field = writeFields.get(name);
    if (!field) throw new Error(`Rangefind build code store is missing field ${name}.`);
    const sink = sinks.get(name);
    if (field.kind === "facet") {
      const codes = facetCodes(value);
      const index = Buffer.allocUnsafe(COMPACT_FACET_INDEX_BYTES);
      if (!codes.length) {
        index.writeUInt32LE(0, 0);
      } else if (codes.length === 1 && codes[0] < COMPACT_FACET_VALUE_MASK) {
        index.writeUInt32LE(codes[0] + 1, 0);
      } else {
        const offsetWords = field.offset / 4;
        if (offsetWords > COMPACT_FACET_VALUE_MASK) {
          throw new Error(`Rangefind compact facet store ${field.name} exceeded its 8 GiB overflow limit.`);
        }
        const data = Buffer.allocUnsafe((codes.length + 1) * 4);
        data.writeUInt32LE(codes.length, 0);
        for (let i = 0; i < codes.length; i++) data.writeUInt32LE(codes[i] >>> 0, 4 + i * 4);
        index.writeUInt32LE((COMPACT_FACET_MULTI_FLAG | offsetWords) >>> 0, 0);
        sink.data.write(data, field.offset);
        field.offset += data.length;
      }
      sink.index.write(index, doc * COMPACT_FACET_INDEX_BYTES);
      return;
    }
    const buffer = scratch.get(name);
    buffer.fill(0);
    writeValue(buffer, field, value);
    sink.data.write(buffer, doc * field.bytesPerDoc);
  };

  store.writeDoc = (doc, values) => {
    for (const field of store._fields) store.set(field.name, doc, values[field.name]);
  };

  return store;
}

export function openCodeStore(descriptor) {
  if (![CODE_STORE_FORMAT, LEGACY_CODE_STORE_FORMAT].includes(descriptor?.format)) return descriptor;
  return createReader(descriptor, "r");
}
