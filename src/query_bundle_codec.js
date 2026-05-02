import { QUERY_BUNDLE_MAGIC, pushVarint, readVarint } from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const QUERY_BUNDLE_FORMAT = "rfqbundle-v1";
const QUERY_BUNDLE_VERSION = 1;

export function buildQueryBundle(bundle) {
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
  return Uint8Array.from(out);
}

export function parseQueryBundle(buffer) {
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
  return { key, baseTerms, expandedTerms, total, complete, nextScoreBound, nextTieDoc, rows };
}
