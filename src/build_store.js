import { closeSync, mkdirSync, openSync, readSync, writeSync } from "node:fs";
import { resolve } from "node:path";
import { docValueFields } from "./codec.js";

export const CODE_STORE_FORMAT = "rf-build-code-store-v1";
const FACET_INDEX_BYTES = 16;

function safeFieldName(value) {
  return String(value || "field").replace(/[^A-Za-z0-9_-]+/gu, "_").replace(/^_+|_+$/gu, "") || "field";
}

function bytesPerDoc(field) {
  if (field.kind === "facet") return FACET_INDEX_BYTES;
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
  const fields = descriptor.fields.map(field => ({
    ...field,
    fd: openSync(field.path, openMode),
    indexFd: field.kind === "facet" ? openSync(field.indexPath, openMode) : null,
    cache: null,
    bytesPerDoc: field.bytesPerDoc || bytesPerDoc(field),
    offset: 0
  }));
  const byName = new Map(fields.map(field => [field.name, field]));
  const total = descriptor.total;
  const cacheDocs = Math.max(1, Math.floor(Number(descriptor.cacheDocs || 16384)));

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
    const index = Buffer.alloc(FACET_INDEX_BYTES);
    const indexBytes = readSync(field.indexFd, index, 0, index.length, doc * FACET_INDEX_BYTES);
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

  return {
    format: CODE_STORE_FORMAT,
    _fieldRecords: fields,
    _fields: fields.map(({ fd, indexFd, cache, offset, ...field }) => ({ ...field })),
    _dicts: descriptor.dicts || {},
    total,
    get(name, doc) {
      if (doc < 0 || doc >= total) return null;
      const field = fieldFor(name);
      if (field.kind === "facet") return readFacetValue(field, doc);
      const cache = field.cache;
      if (!cache || doc < cache.start || doc >= cache.start + cache.count) {
        const start = Math.floor(doc / cacheDocs) * cacheDocs;
        const count = Math.min(cacheDocs, total - start);
        field.cache = { start, count, buffer: readRange(field, start, count) };
      }
      return readValue(field.cache.buffer, field, doc - field.cache.start);
    },
    chunk(name, start, count) {
      const field = fieldFor(name);
      const safeStart = Math.max(0, Math.min(total, start));
      const safeCount = Math.max(0, Math.min(count, total - safeStart));
      if (field.kind === "facet") {
        const out = new Array(safeCount);
        for (let row = 0; row < safeCount; row++) out[row] = readFacetValue(field, safeStart + row);
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
        fields: fields.map(({ fd, indexFd, cache, offset, ...field }) => ({ ...field }))
      };
    },
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
      bytesPerDoc: bytesPerDoc(field)
    };
  });
  const descriptor = {
    format: CODE_STORE_FORMAT,
    total,
    cacheDocs: Math.max(1, Math.floor(Number(options.cacheDocs || config.codeStoreCacheDocs || 16384))),
    fields,
    dicts
  };
  const store = createReader(descriptor, "w+");
  const scratch = new Map(store._fields.map(field => [field.name, Buffer.alloc(field.bytesPerDoc)]));

  store.set = (name, doc, value) => {
    const field = store._fieldRecords.find(item => item.name === name);
    if (!field) throw new Error(`Rangefind build code store is missing field ${name}.`);
    if (field.kind === "facet") {
      const codes = facetCodes(value);
      const data = Buffer.alloc(codes.length * 4);
      for (let i = 0; i < codes.length; i++) data.writeUInt32LE(codes[i] >>> 0, i * 4);
      const index = Buffer.alloc(FACET_INDEX_BYTES);
      writeBigUInt(index, 0, field.offset);
      writeBigUInt(index, 8, data.length);
      if (data.length) writeSync(field.fd, data, 0, data.length, field.offset);
      writeSync(field.indexFd, index, 0, index.length, doc * FACET_INDEX_BYTES);
      field.offset += data.length;
      return;
    }
    const buffer = scratch.get(name);
    buffer.fill(0);
    writeValue(buffer, field, value);
    writeSync(field.fd, buffer, 0, buffer.length, doc * field.bytesPerDoc);
  };

  store.writeDoc = (doc, values) => {
    for (const field of store._fields) store.set(field.name, doc, values[field.name]);
  };

  return store;
}

export function openCodeStore(descriptor) {
  if (descriptor?.format !== CODE_STORE_FORMAT) return descriptor;
  return createReader(descriptor, "r");
}
