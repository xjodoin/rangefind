import { AUTHORITY_SHARD_MAGIC, pushVarint, readVarint } from "./binary.js";
import { foldMulti } from "./analysis_fold.js";
import { DEFAULT_ANALYZER } from "./analysis.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";
import { autocompleteEntrySummary, isAutocompleteKey } from "./authority_lexicon.js";

export const AUTHORITY_FORMAT = "rfauth-v2";
const AUTHORITY_VERSION = 2;
const AUTHORITY_LEGACY_VERSION = 1;
const SURFACE_PREFIX = "r|";
const EXACT_PREFIX = "x|";
const TOKEN_PREFIX = "t|";

export function authorityNormalizeRawSurface(value) {
  return String(value || "")
    .normalize("NFKC")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/gu, " ");
}

export function authorityNormalizeSurface(value) {
  return foldMulti(value)
    .replace(/[^a-z0-9]+/gu, " ")
    .trim()
    .replace(/\s+/gu, " ");
}

export function authorityTokenKeyFromTerms(terms) {
  const key = (terms || []).map(term => String(term || "")).filter(Boolean).join(" ");
  return key ? `${TOKEN_PREFIX}${key}` : "";
}

// Token keys detect the value's language from the value text itself (never
// from surrounding document or query context), so the builder and the
// runtime derive identical keys for identical strings by construction.
function authorityTokens(value, analyzer) {
  const lang = analyzer.detectLanguage(String(value || ""));
  return analyzer.tokenize(value, { unique: false, lang });
}

export function authorityKeysForValue(value, options = {}) {
  const analyzer = options.analyzer || DEFAULT_ANALYZER;
  const out = [];
  const surface = options.surface !== false ? authorityNormalizeRawSurface(value) : "";
  if (surface) out.push({ key: `${SURFACE_PREFIX}${surface}`, kind: "surface" });
  const exact = options.exact !== false ? authorityNormalizeSurface(value) : "";
  if (exact && !out.some(item => item.key === `${EXACT_PREFIX}${exact}`)) out.push({ key: `${EXACT_PREFIX}${exact}`, kind: "exact" });
  if (options.tokens !== false) {
    const tokenKey = authorityTokenKeyFromTerms(authorityTokens(value, analyzer));
    if (tokenKey && !out.some(item => item.key === tokenKey)) out.push({ key: tokenKey, kind: "tokens" });
  }
  return out;
}

export function authorityKeysForQuery(query, baseTerms, options = {}) {
  const out = authorityKeysForValue(query, { analyzer: options.analyzer });
  const tokenKey = authorityTokenKeyFromTerms(baseTerms);
  if (tokenKey && !out.some(item => item.key === tokenKey)) out.push({ key: tokenKey, kind: "tokens" });
  return out;
}

function commonPrefixLength(a, b) {
  const max = Math.min(a.length, b.length);
  let i = 0;
  while (i < max && a[i] === b[i]) i++;
  return i;
}

function compareRows(left, right) {
  return right[1] - left[1] || left[0] - right[0];
}

export function buildAuthorityShard(entries, options = {}) {
  const maxRows = Math.max(1, Math.floor(Number(options.maxRows || 16)));
  const out = [...AUTHORITY_SHARD_MAGIC];
  pushVarint(out, AUTHORITY_VERSION);
  pushVarint(out, entries.length);
  let previous = "";
  for (const [key, rows] of entries) {
    const autocomplete = autocompleteEntrySummary(key, rows);
    const sorted = rows.slice().sort(compareRows);
    const kept = sorted.slice(0, maxRows);
    const prefix = commonPrefixLength(previous, key);
    pushVarint(out, prefix);
    pushUtf8(out, key.slice(prefix));
    pushVarint(out, rows.length);
    if (autocomplete) {
      pushVarint(out, autocomplete.weight);
      previous = key;
      continue;
    }
    pushVarint(out, kept.length);
    for (const [doc, score] of kept) {
      pushVarint(out, doc);
      pushVarint(out, score);
    }
    previous = key;
  }
  return Uint8Array.from(out);
}

export function parseAuthorityShard(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, AUTHORITY_SHARD_MAGIC, "Unsupported Rangefind authority shard");
  const state = { pos: AUTHORITY_SHARD_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== AUTHORITY_VERSION && version !== AUTHORITY_LEGACY_VERSION) {
    throw new Error(`Unsupported Rangefind authority shard version ${version}`);
  }
  const count = readVarint(bytes, state);
  const entries = new Map();
  let previous = "";
  for (let i = 0; i < count; i++) {
    const prefix = readVarint(bytes, state);
    const suffix = readUtf8(bytes, state);
    const key = previous.slice(0, prefix) + suffix;
    const total = readVarint(bytes, state);
    if (version >= AUTHORITY_VERSION && isAutocompleteKey(key)) {
      entries.set(key, { total, complete: true, rows: [], autocompleteWeight: readVarint(bytes, state) });
      previous = key;
      continue;
    }
    const rowCount = readVarint(bytes, state);
    const rows = new Array(rowCount);
    for (let j = 0; j < rowCount; j++) rows[j] = [readVarint(bytes, state), readVarint(bytes, state)];
    entries.set(key, { total, complete: total === rowCount, rows });
    previous = key;
  }
  return { format: version === AUTHORITY_LEGACY_VERSION ? "rfauth-v1" : AUTHORITY_FORMAT, entries };
}
