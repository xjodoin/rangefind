import {
  DOC_VALUE_SORT_DIRECTORY_MAGIC,
  DOC_VALUE_SORT_PAGE_MAGIC,
  fixedWidth,
  pushVarint,
  readFixedInt,
  readVarint,
  writeFixedInt
} from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const DOC_VALUE_SORT_DIRECTORY_FORMAT = "rfdocvaluesortdir-v1";
export const DOC_VALUE_SORT_PAGE_FORMAT = "rfdocvaluesortpage-v1";

const FORMAT_VERSION = 1;
const VALUE_FLOAT64 = 1;
const VALUE_INT_DELTA = 2;

function pushFloat64(out, value) {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, Number(value || 0), true);
  for (const byte of bytes) out.push(byte);
}

function readFloat64(bytes, state) {
  if (state.pos + 8 > bytes.length) throw new Error("Rangefind doc-value sort payload ended inside a float64.");
  const value = new DataView(bytes.buffer, bytes.byteOffset + state.pos, 8).getFloat64(0, true);
  state.pos += 8;
  return value;
}

function numericSortValue(field, value) {
  if (field.kind === "boolean") {
    if (value === true || value === 1 || value === "true" || value === "1") return 2;
    if (value === false || value === 0 || value === "false" || value === "0") return 1;
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function runtimeValue(field, value) {
  if (field.kind === "boolean") return value === 2;
  return value;
}

function shouldUseFloat64(field) {
  return field.type === "float" || field.type === "double";
}

export function encodeDocValueSortPage(field, rankStart, rows) {
  const values = rows.map(row => numericSortValue(field, row.value));
  const present = values.filter(value => Number.isFinite(value));
  const min = present.length ? present[0] : 0;
  const max = present.length ? present[present.length - 1] : 0;
  const valueEncoding = shouldUseFloat64(field) ? VALUE_FLOAT64 : VALUE_INT_DELTA;
  const deltas = valueEncoding === VALUE_INT_DELTA
    ? values.map(value => Math.max(0, Math.round(value - min)))
    : [];
  const valueWidth = valueEncoding === VALUE_INT_DELTA ? fixedWidth(deltas) : 8;
  const docWidth = fixedWidth(rows.map(row => row.doc));
  const header = [...DOC_VALUE_SORT_PAGE_MAGIC];
  pushVarint(header, FORMAT_VERSION);
  pushUtf8(header, field.name);
  pushUtf8(header, field.kind);
  pushUtf8(header, field.type || "");
  pushVarint(header, rankStart);
  pushVarint(header, rows.length);
  header.push(valueEncoding);
  header.push(valueWidth);
  header.push(docWidth);
  pushFloat64(header, min);

  const valueBytes = Buffer.alloc(rows.length * valueWidth);
  if (valueEncoding === VALUE_FLOAT64) {
    for (let i = 0; i < values.length; i++) valueBytes.writeDoubleLE(values[i], i * 8);
  } else {
    for (let i = 0; i < deltas.length; i++) writeFixedInt(valueBytes, i * valueWidth, valueWidth, deltas[i]);
  }

  const docBytes = Buffer.alloc(rows.length * docWidth);
  for (let i = 0; i < rows.length; i++) writeFixedInt(docBytes, i * docWidth, docWidth, rows[i].doc);

  return {
    buffer: Buffer.concat([Buffer.from(Uint8Array.from(header)), valueBytes, docBytes]),
    meta: {
      rankStart,
      count: rows.length,
      min,
      max,
      valueWidth,
      docWidth
    }
  };
}

export function decodeDocValueSortPage(buffer, expected = {}) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, DOC_VALUE_SORT_PAGE_MAGIC, "Unsupported Rangefind doc-value sort page");
  const state = { pos: DOC_VALUE_SORT_PAGE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) throw new Error(`Unsupported Rangefind doc-value sort page version ${version}`);
  const field = {
    name: readUtf8(bytes, state),
    kind: readUtf8(bytes, state),
    type: readUtf8(bytes, state)
  };
  if (expected.name && expected.name !== field.name) throw new Error(`Rangefind doc-value sort page field mismatch: ${field.name}`);
  const rankStart = readVarint(bytes, state);
  const count = readVarint(bytes, state);
  const valueEncoding = bytes[state.pos++];
  const valueWidth = bytes[state.pos++];
  const docWidth = bytes[state.pos++];
  const min = readFloat64(bytes, state);
  const values = new Array(count);
  if (valueEncoding === VALUE_FLOAT64) {
    for (let i = 0; i < count; i++) values[i] = new DataView(bytes.buffer, bytes.byteOffset + state.pos + i * 8, 8).getFloat64(0, true);
    state.pos += count * 8;
  } else if (valueEncoding === VALUE_INT_DELTA) {
    for (let i = 0; i < count; i++) values[i] = min + readFixedInt(bytes, state.pos + i * valueWidth, valueWidth);
    state.pos += count * valueWidth;
  } else {
    throw new Error(`Unsupported Rangefind doc-value sort value encoding ${valueEncoding}`);
  }
  const rows = new Array(count);
  for (let i = 0; i < count; i++) {
    const doc = readFixedInt(bytes, state.pos + i * docWidth, docWidth);
    rows[i] = {
      doc,
      value: runtimeValue(field, values[i]),
      sortValue: values[i],
      rank: rankStart + i
    };
  }
  state.pos += count * docWidth;
  if (state.pos !== bytes.length) throw new Error("Rangefind doc-value sort page has trailing bytes.");
  return { ...field, rankStart, count, rows };
}

export function encodeDocValueSortDirectory({ field, pageSize, total, pages, summaryFields, packTable, packIndexes }) {
  const out = [...DOC_VALUE_SORT_DIRECTORY_MAGIC];
  pushVarint(out, FORMAT_VERSION);
  pushUtf8(out, field.name);
  pushUtf8(out, field.kind);
  pushUtf8(out, field.type || "");
  pushVarint(out, pageSize);
  pushVarint(out, total);
  pushVarint(out, packTable.length);
  for (const pack of packTable) pushUtf8(out, pack);
  pushVarint(out, summaryFields.length);
  for (const summaryField of summaryFields) {
    pushUtf8(out, summaryField.name);
    pushUtf8(out, summaryField.kind);
    pushUtf8(out, summaryField.type || "");
  }
  pushVarint(out, pages.length);
  for (const page of pages) {
    pushVarint(out, page.rankStart);
    pushVarint(out, page.count);
    pushFloat64(out, page.min);
    pushFloat64(out, page.max);
    pushVarint(out, packIndexes.get(page.entry.pack) ?? 0);
    pushVarint(out, page.entry.offset);
    pushVarint(out, page.entry.length);
    pushVarint(out, page.entry.physicalLength || page.entry.length);
    pushVarint(out, page.entry.logicalLength || 0);
    pushUtf8(out, page.entry.checksum?.algorithm || "");
    pushUtf8(out, page.entry.checksum?.value || "");
    for (const summaryField of summaryFields) {
      const summary = page.summaries?.[summaryField.name];
      const hasValues = Number.isFinite(summary?.min) && Number.isFinite(summary?.max);
      out.push(hasValues ? 1 : 0);
      if (hasValues) {
        pushFloat64(out, summary.min);
        pushFloat64(out, summary.max);
      }
    }
  }
  return {
    buffer: Buffer.from(Uint8Array.from(out)),
    meta: {
      format: DOC_VALUE_SORT_DIRECTORY_FORMAT,
      page_format: DOC_VALUE_SORT_PAGE_FORMAT,
      field: field.name,
      kind: field.kind,
      type: field.type || "",
      page_size: pageSize,
      total,
      pages: pages.length
    }
  };
}

export function parseDocValueSortDirectory(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, DOC_VALUE_SORT_DIRECTORY_MAGIC, "Unsupported Rangefind doc-value sort directory");
  const state = { pos: DOC_VALUE_SORT_DIRECTORY_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) throw new Error(`Unsupported Rangefind doc-value sort directory version ${version}`);
  const field = {
    name: readUtf8(bytes, state),
    kind: readUtf8(bytes, state),
    type: readUtf8(bytes, state)
  };
  const pageSize = readVarint(bytes, state);
  const total = readVarint(bytes, state);
  const packCount = readVarint(bytes, state);
  const packTable = new Array(packCount);
  for (let i = 0; i < packCount; i++) packTable[i] = readUtf8(bytes, state);
  const summaryFieldCount = readVarint(bytes, state);
  const summaryFields = new Array(summaryFieldCount);
  for (let i = 0; i < summaryFieldCount; i++) {
    summaryFields[i] = {
      name: readUtf8(bytes, state),
      kind: readUtf8(bytes, state),
      type: readUtf8(bytes, state)
    };
  }
  const pageCount = readVarint(bytes, state);
  const pages = new Array(pageCount);
  for (let i = 0; i < pageCount; i++) {
    const rankStart = readVarint(bytes, state);
    const count = readVarint(bytes, state);
    const min = readFloat64(bytes, state);
    const max = readFloat64(bytes, state);
    const packIndex = readVarint(bytes, state);
    const page = {
      index: i,
      rankStart,
      count,
      min,
      max,
      pack: packTable[packIndex],
      offset: readVarint(bytes, state),
      length: readVarint(bytes, state),
      physicalLength: readVarint(bytes, state),
      logicalLength: readVarint(bytes, state) || null
    };
    const algorithm = readUtf8(bytes, state);
    const value = readUtf8(bytes, state);
    page.checksum = value ? { algorithm: algorithm || "sha256", value } : null;
    page.summaries = {};
    for (const summaryField of summaryFields) {
      const hasValues = bytes[state.pos++] === 1;
      page.summaries[summaryField.name] = hasValues
        ? { min: readFloat64(bytes, state), max: readFloat64(bytes, state) }
        : { min: null, max: null };
    }
    pages[i] = page;
  }
  if (state.pos !== bytes.length) throw new Error("Rangefind doc-value sort directory has trailing bytes.");
  return {
    format: DOC_VALUE_SORT_DIRECTORY_FORMAT,
    pageFormat: DOC_VALUE_SORT_PAGE_FORMAT,
    field,
    pageSize,
    total,
    packTable,
    summaryFields,
    pages
  };
}
