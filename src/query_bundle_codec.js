import { QUERY_BUNDLE_MAGIC, pushVarint, readVarint } from "./binary.js";
import { assertMagic, pushUtf8, readBlockFilterSummary, readUtf8, writeBlockFilterSummary } from "./codec.js";

export const QUERY_BUNDLE_FORMAT = "rfqbundle-v1";
const QUERY_BUNDLE_VERSION = 1;

function pushFloat64(out, value) {
  const buffer = new ArrayBuffer(8);
  new DataView(buffer).setFloat64(0, Number(value || 0), true);
  for (const byte of new Uint8Array(buffer)) out.push(byte);
}

function readFloat64(bytes, state) {
  const view = new DataView(bytes.buffer, bytes.byteOffset + state.pos, 8);
  state.pos += 8;
  return view.getFloat64(0, true);
}

function pushSignedVarint(out, value) {
  const n = Math.round(Number(value || 0));
  pushVarint(out, n < 0 ? (-n * 2) - 1 : n * 2);
}

function readSignedVarint(bytes, state) {
  const n = readVarint(bytes, state);
  return n % 2 ? -((n + 1) / 2) : n / 2;
}

function writeFilterValue(out, filter, value) {
  if (filter.kind === "facet") {
    const codes = Array.isArray(value?.codes)
      ? value.codes
      : Array.isArray(value)
        ? value
        : value == null
          ? []
          : [value];
    pushVarint(out, codes.length);
    for (const code of codes) pushVarint(out, code);
    return;
  }
  if (filter.kind === "boolean") {
    pushVarint(out, value == null ? 0 : value === true ? 2 : 1);
    return;
  }
  if (value == null || !Number.isFinite(Number(value))) {
    pushVarint(out, 0);
    return;
  }
  pushVarint(out, 1);
  if (String(filter.type || "int").toLowerCase() === "float") pushFloat64(out, value);
  else pushSignedVarint(out, value);
}

function readFilterValue(bytes, state, filter) {
  if (filter.kind === "facet") {
    const count = readVarint(bytes, state);
    const codes = new Array(count);
    for (let i = 0; i < count; i++) codes[i] = readVarint(bytes, state);
    return { codes };
  }
  if (filter.kind === "boolean") {
    const code = readVarint(bytes, state);
    return code === 0 ? null : code === 2;
  }
  const present = readVarint(bytes, state);
  if (!present) return null;
  return String(filter.type || "int").toLowerCase() === "float"
    ? readFloat64(bytes, state)
    : readSignedVarint(bytes, state);
}

export function buildQueryBundle(bundle, manifest = {}) {
  const out = [...QUERY_BUNDLE_MAGIC];
  pushVarint(out, QUERY_BUNDLE_VERSION);
  pushUtf8(out, bundle.key);
  pushVarint(out, bundle.baseTerms.length);
  for (const term of bundle.baseTerms) pushUtf8(out, term);
  pushVarint(out, bundle.expandedTerms.length);
  for (const term of bundle.expandedTerms) pushUtf8(out, term);
  pushVarint(out, bundle.total);
  pushVarint(out, bundle.complete ? 1 : 0);
  pushVarint(out, bundle.nextScoreBound || 0);
  if (bundle.nextTieDoc == null) {
    pushVarint(out, 0);
  } else {
    pushVarint(out, 1);
    pushVarint(out, bundle.nextTieDoc);
  }
  pushVarint(out, bundle.rows.length);
  for (const row of bundle.rows) {
    pushVarint(out, row.doc);
    pushVarint(out, row.score);
  }
  const rowGroups = bundle.rowGroups || [];
  pushVarint(out, rowGroups.length);
  for (const group of rowGroups) {
    pushVarint(out, group.rowStart || 0);
    pushVarint(out, group.rowCount || 0);
    pushVarint(out, group.scoreMax || 0);
    pushVarint(out, group.scoreMin || 0);
    pushVarint(out, group.docMin || 0);
    pushVarint(out, group.docMax || 0);
    writeBlockFilterSummary(out, manifest.block_filters || [], group.filters);
  }
  const filters = manifest.block_filters || [];
  const filterValues = bundle.filterValues || {};
  const valueFields = filters
    .map((filter, index) => ({ filter, index }))
    .filter(({ filter }) => Object.prototype.hasOwnProperty.call(filterValues, filter.name));
  pushVarint(out, valueFields.length);
  for (const { filter, index } of valueFields) {
    pushVarint(out, index);
    const values = filterValues[filter.name] || [];
    for (let i = 0; i < bundle.rows.length; i++) writeFilterValue(out, filter, values[i]);
  }
  return Uint8Array.from(out);
}

export function parseQueryBundle(buffer, manifest = {}) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, QUERY_BUNDLE_MAGIC, "Unsupported Rangefind query bundle");
  const state = { pos: QUERY_BUNDLE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== QUERY_BUNDLE_VERSION) throw new Error(`Unsupported Rangefind query bundle version ${version}`);
  const key = readUtf8(bytes, state);
  const baseTerms = new Array(readVarint(bytes, state));
  for (let i = 0; i < baseTerms.length; i++) baseTerms[i] = readUtf8(bytes, state);
  const expandedTerms = new Array(readVarint(bytes, state));
  for (let i = 0; i < expandedTerms.length; i++) expandedTerms[i] = readUtf8(bytes, state);
  const total = readVarint(bytes, state);
  const complete = readVarint(bytes, state) === 1;
  const nextScoreBound = readVarint(bytes, state);
  const hasNextDoc = readVarint(bytes, state) === 1;
  const nextTieDoc = hasNextDoc ? readVarint(bytes, state) : null;
  const rowCount = readVarint(bytes, state);
  const rows = new Array(rowCount);
  for (let i = 0; i < rowCount; i++) {
    rows[i] = [readVarint(bytes, state), readVarint(bytes, state)];
  }
  const rowGroupCount = state.pos < bytes.length ? readVarint(bytes, state) : 0;
  const rowGroups = new Array(rowGroupCount);
  for (let i = 0; i < rowGroupCount; i++) {
    rowGroups[i] = {
      rowStart: readVarint(bytes, state),
      rowCount: readVarint(bytes, state),
      scoreMax: readVarint(bytes, state),
      scoreMin: readVarint(bytes, state),
      docMin: readVarint(bytes, state),
      docMax: readVarint(bytes, state),
      filters: readBlockFilterSummary(bytes, state, manifest)
    };
  }
  const filterValues = {};
  const filters = manifest.block_filters || [];
  const filterValueFieldCount = state.pos < bytes.length ? readVarint(bytes, state) : 0;
  for (let fieldIndex = 0; fieldIndex < filterValueFieldCount; fieldIndex++) {
    const filter = filters[readVarint(bytes, state)];
    const valuesByDoc = {};
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex++) {
      const value = filter ? readFilterValue(bytes, state, filter) : null;
      if (filter) valuesByDoc[rows[rowIndex][0]] = value;
    }
    if (filter) filterValues[filter.name] = valuesByDoc;
  }
  return { key, baseTerms, expandedTerms, total, complete, nextScoreBound, nextTieDoc, rows, rowGroups, filterValues };
}
