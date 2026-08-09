import { AUTHORITY_SHARD_MAGIC, pushVarint, readVarint } from "./binary.js";
import { foldMulti } from "./analysis_fold.js";
import { DEFAULT_ANALYZER } from "./analysis.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";
import { autocompleteEntrySummary, isAutocompleteKey } from "./authority_lexicon.js";
import { normalizeAddressAuthorityKey } from "./address.js";

export const AUTHORITY_FORMAT = "rfauth-v2";
// Root-level suggest routing artifact for sharded indexes (the block lives
// in the sharded root manifest as `suggest_routing`; see suggest_routing.js).
export const SUGGEST_ROUTING_FORMAT = "rfsuggestroute-v1";
const AUTHORITY_VERSION = 2;
const AUTHORITY_LEGACY_VERSION = 1;
// Version 3 is written only by root-level suggest routing artifacts
// (src/suggest_routing.js): autocomplete entries keep their summary weight
// but also carry rows, whose "doc" column holds a federation shard ordinal
// instead of a document index — provenance for merged suggestions.
const AUTHORITY_ORDINAL_ROWS_VERSION = 3;
// Version 4 is the per-shard layout of the same idea: autocomplete entries
// keep their best [doc, score] rows so a selected suggestion can hydrate its
// document directly instead of re-running the query as a search.
const AUTHORITY_DOC_ROWS_VERSION = 4;
const SURFACE_PREFIX = "r|";
const EXACT_PREFIX = "x|";
const TOKEN_PREFIX = "t|";
const ADDRESS_PREFIX = "a|";
const ADDRESS_RANGE_PREFIX = "i|";

class AuthorityByteWriter {
  constructor(chunkBytes = 64 * 1024) {
    this.chunks = [];
    this.current = new Uint8Array(chunkBytes);
    this.offset = 0;
    this.length = 0;
  }

  push(value) {
    if (this.offset === this.current.length) {
      this.chunks.push(this.current);
      this.current = new Uint8Array(this.current.length);
      this.offset = 0;
    }
    this.current[this.offset++] = value;
    this.length++;
  }

  finish() {
    if (this.offset) this.chunks.push(this.current.subarray(0, this.offset));
    const out = Buffer.allocUnsafe(this.length);
    let offset = 0;
    for (const chunk of this.chunks) {
      out.set(chunk, offset);
      offset += chunk.length;
    }
    return out;
  }
}

export function authorityAddressRangeKey(value) {
  const normalized = authorityNormalizeSurface(value);
  return normalized ? `${ADDRESS_RANGE_PREFIX}${normalized}` : "";
}

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
  if (options.normalizer === "address-range") {
    const key = authorityAddressRangeKey(value);
    return key ? [{ key, kind: "address-range" }] : [];
  }
  const out = [];
  const surface = options.surface !== false ? authorityNormalizeRawSurface(value) : "";
  if (surface) out.push({ key: `${SURFACE_PREFIX}${surface}`, kind: "surface" });
  const exact = options.exact !== false ? authorityNormalizeSurface(value) : "";
  if (exact && !out.some(item => item.key === `${EXACT_PREFIX}${exact}`)) out.push({ key: `${EXACT_PREFIX}${exact}`, kind: "exact" });
  if (options.tokens !== false) {
    const tokenKey = authorityTokenKeyFromTerms(authorityTokens(value, analyzer));
    if (tokenKey && !out.some(item => item.key === tokenKey)) out.push({ key: tokenKey, kind: "tokens" });
  }
  if (options.normalizer === "address") {
    const address = normalizeAddressAuthorityKey(value);
    if (address && !out.some(item => item.key === `${ADDRESS_PREFIX}${address}`)) {
      out.push({ key: `${ADDRESS_PREFIX}${address}`, kind: "address" });
    }
  }
  return out;
}

export function authorityKeysForQuery(query, baseTerms, options = {}) {
  const out = authorityKeysForValue(query, { analyzer: options.analyzer });
  const tokenKey = authorityTokenKeyFromTerms(baseTerms);
  if (tokenKey && !out.some(item => item.key === tokenKey)) out.push({ key: tokenKey, kind: "tokens" });
  if (options.address) {
    const address = normalizeAddressAuthorityKey(query);
    if (address && !out.some(item => item.key === `${ADDRESS_PREFIX}${address}`)) {
      out.push({ key: `${ADDRESS_PREFIX}${address}`, kind: "address" });
    }
  }
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
  const ordinalRows = options.ordinalRows === true;
  const docRows = !ordinalRows && options.docRows === true;
  // A suggestion only resolves directly to a document when it is unambiguous,
  // so autocomplete entries keep just their best row(s) — the byte cost stays
  // a few varints per surface.
  const maxAutocompleteRows = docRows
    ? Math.max(1, Math.floor(Number(options.maxAutocompleteRows || 1)))
    : maxRows;
  const out = new AuthorityByteWriter();
  for (const byte of AUTHORITY_SHARD_MAGIC) out.push(byte);
  pushVarint(out, ordinalRows ? AUTHORITY_ORDINAL_ROWS_VERSION : docRows ? AUTHORITY_DOC_ROWS_VERSION : AUTHORITY_VERSION);
  pushVarint(out, entries.length);
  let previous = "";
  for (const [key, rows, meta] of entries) {
    const autocomplete = autocompleteEntrySummary(key, rows);
    const sorted = rows.slice().sort(compareRows);
    const kept = sorted.slice(0, maxRows);
    const prefix = commonPrefixLength(previous, key);
    pushVarint(out, prefix);
    pushUtf8(out, key.slice(prefix));
    // Ordinal-row entries aggregate many documents behind each row (one row
    // per federation shard), so the true document count travels separately.
    pushVarint(out, ordinalRows ? Math.max(rows.length, Math.floor(Number(meta?.total || 0))) : rows.length);
    if (autocomplete) {
      // Ordinal-row artifacts append the rows after the summary weight so
      // readers can recover which federation shards back the suggestion.
      // Doc-row artifacts append the best [doc, score] rows so a selected
      // suggestion can hydrate its document without a search.
      pushVarint(out, autocomplete.weight);
      if (ordinalRows || docRows) {
        const keptAutocomplete = kept.slice(0, maxAutocompleteRows);
        pushVarint(out, keptAutocomplete.length);
        for (const [doc, score] of keptAutocomplete) {
          pushVarint(out, doc);
          pushVarint(out, score);
        }
      }
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
  return out.finish();
}

export function parseAuthorityShard(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, AUTHORITY_SHARD_MAGIC, "Unsupported Rangefind authority shard");
  const state = { pos: AUTHORITY_SHARD_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== AUTHORITY_VERSION && version !== AUTHORITY_LEGACY_VERSION
    && version !== AUTHORITY_ORDINAL_ROWS_VERSION && version !== AUTHORITY_DOC_ROWS_VERSION) {
    throw new Error(`Unsupported Rangefind authority shard version ${version}`);
  }
  const ordinalRows = version === AUTHORITY_ORDINAL_ROWS_VERSION;
  const docRows = version === AUTHORITY_DOC_ROWS_VERSION;
  const count = readVarint(bytes, state);
  const entries = new Map();
  let previous = "";
  for (let i = 0; i < count; i++) {
    const prefix = readVarint(bytes, state);
    const suffix = readUtf8(bytes, state);
    const key = previous.slice(0, prefix) + suffix;
    const total = readVarint(bytes, state);
    if (version >= AUTHORITY_VERSION && isAutocompleteKey(key)) {
      const autocompleteWeight = readVarint(bytes, state);
      let rows = [];
      if (ordinalRows || docRows) {
        const rowCount = readVarint(bytes, state);
        rows = new Array(rowCount);
        for (let j = 0; j < rowCount; j++) rows[j] = [readVarint(bytes, state), readVarint(bytes, state)];
      }
      entries.set(key, {
        total,
        complete: true,
        rows,
        autocompleteWeight,
        ...(ordinalRows ? { ordinalRows: true } : {}),
        ...(docRows ? { docRows: true } : {})
      });
      previous = key;
      continue;
    }
    const rowCount = readVarint(bytes, state);
    const rows = new Array(rowCount);
    for (let j = 0; j < rowCount; j++) rows[j] = [readVarint(bytes, state), readVarint(bytes, state)];
    entries.set(key, { total, complete: total === rowCount, rows, ...(ordinalRows ? { ordinalRows: true } : {}) });
    previous = key;
  }
  return { format: version === AUTHORITY_LEGACY_VERSION ? "rfauth-v1" : AUTHORITY_FORMAT, entries };
}
