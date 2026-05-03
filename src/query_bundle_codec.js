import { QUERY_BUNDLE_MAGIC, pushVarint, readVarint } from "./binary.js";
import { assertMagic, pushUtf8, readBlockFilterSummary, readUtf8, writeBlockFilterSummary } from "./codec.js";

export const QUERY_BUNDLE_FORMAT = "rfqbundle-v1";
const QUERY_BUNDLE_VERSION = 1;

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
  return { key, baseTerms, expandedTerms, total, complete, nextScoreBound, nextTieDoc, rows, rowGroups };
}
