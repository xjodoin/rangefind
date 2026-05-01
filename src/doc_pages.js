import { DOC_PAGE_PAYLOAD_MAGIC, DOC_PAGE_POINTER_MAGIC, pushVarint, readVarint } from "./binary.js";
import { assertMagic } from "./codec.js";
import { buildDocPointerTable, decodeDocPointerRecord, parseDocPointerHeader, parseDocPointerPage } from "./doc_pointers.js";

export const DOC_PAGE_FORMAT = "rfdocpage-v1";
export const DOC_PAGE_POINTER_FORMAT = "rfdocpageptr-v1";
export const DOC_PAGE_ENCODING = "rfdocpagecols-v1";
const DOC_PAGE_VERSION = 1;
const VALUE_NULL = 0;
const VALUE_FALSE = 1;
const VALUE_TRUE = 2;
const VALUE_NUMBER = 3;
const VALUE_STRING = 4;
const VALUE_STRING_ARRAY = 5;
const VALUE_JSON = 6;
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function encodeDocPageColumns(docs, fields) {
  const out = [...DOC_PAGE_PAYLOAD_MAGIC];
  pushVarint(out, DOC_PAGE_VERSION);
  pushVarint(out, docs.length);
  pushVarint(out, fields.length);
  for (const field of fields) {
    for (const doc of docs) encodeValue(out, doc[field]);
  }
  return Uint8Array.from(out);
}

function pushBytes(out, bytes) {
  pushVarint(out, bytes.length);
  for (const byte of bytes) out.push(byte);
}

function encodeString(out, value) {
  pushBytes(out, textEncoder.encode(String(value)));
}

function encodeNumber(out, value) {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, value, true);
  for (const byte of bytes) out.push(byte);
}

function encodeValue(out, value) {
  if (value == null) {
    out.push(VALUE_NULL);
  } else if (value === false) {
    out.push(VALUE_FALSE);
  } else if (value === true) {
    out.push(VALUE_TRUE);
  } else if (typeof value === "number" && Number.isFinite(value)) {
    out.push(VALUE_NUMBER);
    encodeNumber(out, value);
  } else if (typeof value === "string") {
    out.push(VALUE_STRING);
    encodeString(out, value);
  } else if (Array.isArray(value) && value.every(item => typeof item === "string")) {
    out.push(VALUE_STRING_ARRAY);
    pushVarint(out, value.length);
    for (const item of value) encodeString(out, item);
  } else {
    out.push(VALUE_JSON);
    encodeString(out, JSON.stringify(value));
  }
}

function readBytes(bytes, state) {
  const length = readVarint(bytes, state);
  const start = state.pos;
  state.pos += length;
  if (state.pos > bytes.length) throw new Error("Rangefind doc page payload ended inside a value.");
  return bytes.subarray(start, state.pos);
}

function decodeString(bytes, state) {
  return textDecoder.decode(readBytes(bytes, state));
}

function decodeNumber(bytes, state) {
  if (state.pos + 8 > bytes.length) throw new Error("Rangefind doc page payload ended inside a number.");
  const value = new DataView(bytes.buffer, bytes.byteOffset + state.pos, 8).getFloat64(0, true);
  state.pos += 8;
  return value;
}

function decodeValue(bytes, state) {
  const type = bytes[state.pos++];
  if (type === VALUE_NULL) return null;
  if (type === VALUE_FALSE) return false;
  if (type === VALUE_TRUE) return true;
  if (type === VALUE_NUMBER) return decodeNumber(bytes, state);
  if (type === VALUE_STRING) return decodeString(bytes, state);
  if (type === VALUE_STRING_ARRAY) {
    const count = readVarint(bytes, state);
    const out = new Array(count);
    for (let i = 0; i < count; i++) out[i] = decodeString(bytes, state);
    return out;
  }
  if (type === VALUE_JSON) return JSON.parse(decodeString(bytes, state));
  throw new Error(`Unsupported Rangefind doc page value type ${type}.`);
}

export function decodeDocPageColumns(buffer, fields, startIndex = 0) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, DOC_PAGE_PAYLOAD_MAGIC, "Unsupported Rangefind doc page payload");
  const state = { pos: DOC_PAGE_PAYLOAD_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== DOC_PAGE_VERSION) throw new Error(`Unsupported Rangefind doc page payload version ${version}`);
  const count = readVarint(bytes, state);
  const fieldCount = readVarint(bytes, state);
  if (fieldCount !== fields.length) throw new Error("Rangefind doc page field count does not match the manifest.");
  const docs = new Array(count);
  for (let row = 0; row < count; row++) docs[row] = { index: startIndex + row };
  for (const field of fields) {
    for (let row = 0; row < count; row++) {
      const value = decodeValue(bytes, state);
      if (value !== null && value !== undefined) docs[row][field] = value;
    }
  }
  if (state.pos !== bytes.length) throw new Error("Rangefind doc page payload has trailing bytes.");
  return docs;
}

export function buildDocPagePointerTable(entries, packIndexes) {
  return buildDocPointerTable(entries, packIndexes, {
    format: DOC_PAGE_POINTER_FORMAT,
    magic: DOC_PAGE_POINTER_MAGIC
  });
}

export function parseDocPagePointerHeader(buffer) {
  return parseDocPointerHeader(buffer, {
    format: DOC_PAGE_POINTER_FORMAT,
    magic: DOC_PAGE_POINTER_MAGIC
  });
}

export function parseDocPagePointerPage(buffer, options = {}) {
  return parseDocPointerPage(buffer, {
    ...options,
    format: DOC_PAGE_POINTER_FORMAT,
    magic: DOC_PAGE_POINTER_MAGIC
  });
}

export function decodeDocPagePointerRecord(buffer, offset, meta, packTable = []) {
  return decodeDocPointerRecord(buffer, offset, meta, packTable);
}
