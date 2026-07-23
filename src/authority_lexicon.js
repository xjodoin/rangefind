import { AUTHORITY_HOT_MAGIC, AUTHORITY_LEXICON_MAGIC, pushVarint, readVarint } from "./binary.js";
import { foldMulti } from "./analysis_fold.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const AUTHORITY_LEXICON_FORMAT = "rflexicon-v1";
export const AUTOCOMPLETE_PREFIX = "s|";
export const AUTOCOMPLETE_SEPARATOR = "\u0000";
const AUTHORITY_LEXICON_VERSION = 1;
const UNWEIGHTED_SURFACE_BONUS = 2 ** 32;

export function suggestKey(value) {
  return foldMulti(value)
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/gu, " ");
}

function displayValue(value) {
  return String(value ?? "")
    .replaceAll(AUTOCOMPLETE_SEPARATOR, " ")
    .trim()
    .replace(/\s+/gu, " ");
}

export function isAutocompleteKey(key) {
  return String(key || "").startsWith(AUTOCOMPLETE_PREFIX);
}

export function parseAutocompleteKey(key) {
  if (!isAutocompleteKey(key)) return null;
  const separator = key.indexOf(AUTOCOMPLETE_SEPARATOR, AUTOCOMPLETE_PREFIX.length);
  if (separator < 0) return null;
  return {
    normalized: key.slice(AUTOCOMPLETE_PREFIX.length, separator),
    display: key.slice(separator + AUTOCOMPLETE_SEPARATOR.length)
  };
}

export function autocompleteKeysForValue(value, options = {}) {
  const display = displayValue(value);
  const normalized = suggestKey(display);
  const minKeyLength = Math.max(1, Math.floor(Number(options.minKeyLength ?? 1)));
  if (!display || normalized.length < minKeyLength) return [];
  const normalizedKeys = [normalized];
  if (options.tokenPrefixes !== false) {
    const maxTokenKeys = Math.max(1, Math.floor(Number(options.maxTokenKeys ?? 4)));
    let start = normalized.indexOf(" ");
    while (start !== -1 && normalizedKeys.length < maxTokenKeys) {
      const tokenKey = normalized.slice(start + 1);
      if (tokenKey.length >= minKeyLength && !normalizedKeys.includes(tokenKey)) normalizedKeys.push(tokenKey);
      start = normalized.indexOf(" ", start + 1);
    }
  }
  return normalizedKeys.map(key => ({
    key: `${AUTOCOMPLETE_PREFIX}${key}${AUTOCOMPLETE_SEPARATOR}${display}`,
    normalized: key,
    display
  }));
}

export function autocompleteRank(item, weighted = false) {
  const weight = Math.max(0, Math.floor(Number(item.weight) || 0));
  const full = item.full ?? suggestKey(item.display) === item.normalized;
  return weighted
    ? Math.min(Number.MAX_SAFE_INTEGER, weight * 2 + (full ? 1 : 0))
    : (full ? UNWEIGHTED_SURFACE_BONUS : 0) + Math.min(UNWEIGHTED_SURFACE_BONUS - 1, weight);
}

export function autocompleteEntrySummary(key, rows, options = {}) {
  const parsed = parseAutocompleteKey(key);
  if (!parsed) return null;
  let maxWeight = 0;
  for (const row of rows || []) maxWeight = Math.max(maxWeight, Number(row[1]) || 0);
  const item = {
    key,
    ...parsed,
    weight: maxWeight || rows.length,
    count: rows.length,
    full: suggestKey(parsed.display) === parsed.normalized
  };
  item.rank = autocompleteRank(item, options.weighted);
  return item;
}

// Memoized code-point length: compareAutocomplete runs inside hot sort loops,
// so the per-comparison Array.from allocation is replaced by a counted pass
// cached on the candidate itself.
function displayLengthOf(item) {
  if (item.displayLength == null) {
    let length = 0;
    for (const _ of item.display) length++;
    item.displayLength = length;
  }
  return item.displayLength;
}

export function compareAutocomplete(left, right) {
  return (right.rank ?? right.weight) - (left.rank ?? left.weight)
    || displayLengthOf(left) - displayLengthOf(right)
    || (left.normalized < right.normalized ? -1 : left.normalized > right.normalized ? 1 : 0)
    || (left.display < right.display ? -1 : left.display > right.display ? 1 : 0);
}

function commonPrefixLength(left, right) {
  const limit = Math.min(left.length, right.length);
  let index = 0;
  while (index < limit && left.charCodeAt(index) === right.charCodeAt(index)) index++;
  return index;
}

function pushFrontCoded(out, value, previous) {
  const prefix = commonPrefixLength(value, previous);
  pushVarint(out, prefix);
  pushUtf8(out, value.slice(prefix));
}

function readFrontCoded(bytes, state, previous) {
  const prefix = readVarint(bytes, state);
  return previous.slice(0, prefix) + readUtf8(bytes, state);
}

export function encodeAuthorityLexiconRoot({ shards, hot, weighted = false }) {
  const sortedShards = shards.slice().sort((a, b) => a.shard < b.shard ? -1 : a.shard > b.shard ? 1 : 0);
  const out = [...AUTHORITY_LEXICON_MAGIC];
  pushVarint(out, AUTHORITY_LEXICON_VERSION);
  pushVarint(out, weighted ? 1 : 0);
  pushVarint(out, sortedShards.length);
  let previousShard = "";
  for (const shard of sortedShards) {
    pushFrontCoded(out, shard.shard, previousShard);
    pushVarint(out, shard.maxRank);
    pushVarint(out, shard.count);
    previousShard = shard.shard;
  }
  const hotEntries = [...hot.entries()].sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
  pushVarint(out, hotEntries.length);
  for (const [prefix, entry] of hotEntries) {
    pushUtf8(out, prefix);
    pushUtf8(out, entry.file);
    pushVarint(out, entry.bytes);
    pushVarint(out, entry.count);
  }
  return Buffer.from(Uint8Array.from(out));
}

// Streaming variant for planet-scale routing artifacts. `shards` must already
// be in code-unit shard order and `shardCount` must be exact, which lets the
// caller keep the directory metadata in an on-disk spool instead of retaining
// and sorting millions of JavaScript objects.
export async function* encodeAuthorityLexiconRootChunks({
  shards,
  shardCount,
  hot,
  weighted = false
}) {
  const count = Math.max(0, Math.floor(Number(shardCount) || 0));
  const header = [...AUTHORITY_LEXICON_MAGIC];
  pushVarint(header, AUTHORITY_LEXICON_VERSION);
  pushVarint(header, weighted ? 1 : 0);
  pushVarint(header, count);
  yield Buffer.from(header);

  let previousShard = "";
  let actualCount = 0;
  for await (const shard of shards) {
    if (actualCount && shard.shard < previousShard) {
      throw new Error("Rangefind authority lexicon shards must be sorted.");
    }
    const out = [];
    pushFrontCoded(out, shard.shard, previousShard);
    pushVarint(out, shard.maxRank);
    pushVarint(out, shard.count);
    yield Buffer.from(out);
    previousShard = shard.shard;
    actualCount++;
  }
  if (actualCount !== count) {
    throw new Error(`Rangefind authority lexicon expected ${count} shards, received ${actualCount}.`);
  }

  const hotEntries = [...hot.entries()].sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
  const hotHeader = [];
  pushVarint(hotHeader, hotEntries.length);
  yield Buffer.from(hotHeader);
  for (const [prefix, entry] of hotEntries) {
    const out = [];
    pushUtf8(out, prefix);
    pushUtf8(out, entry.file);
    pushVarint(out, entry.bytes);
    pushVarint(out, entry.count);
    yield Buffer.from(out);
  }
}

export function parseAuthorityLexiconRoot(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, AUTHORITY_LEXICON_MAGIC, "Unsupported Rangefind authority lexicon root");
  const state = { pos: AUTHORITY_LEXICON_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== AUTHORITY_LEXICON_VERSION) throw new Error(`Unsupported Rangefind authority lexicon root version ${version}`);
  const weighted = readVarint(bytes, state) === 1;
  const shardCount = readVarint(bytes, state);
  const shards = new Array(shardCount);
  let previousShard = "";
  for (let index = 0; index < shardCount; index++) {
    const shard = readFrontCoded(bytes, state, previousShard);
    shards[index] = {
      shard,
      maxRank: readVarint(bytes, state),
      count: readVarint(bytes, state)
    };
    previousShard = shard;
  }
  const hotCount = readVarint(bytes, state);
  const hot = new Map();
  for (let index = 0; index < hotCount; index++) {
    const prefix = readUtf8(bytes, state);
    hot.set(prefix, {
      file: readUtf8(bytes, state),
      bytes: readVarint(bytes, state),
      count: readVarint(bytes, state)
    });
  }
  if (state.pos !== bytes.length) throw new Error("Rangefind authority lexicon root has trailing bytes.");
  return { format: AUTHORITY_LEXICON_FORMAT, weighted, shards, hot };
}

export function encodeAuthorityHotList(entries) {
  const out = [...AUTHORITY_HOT_MAGIC];
  pushVarint(out, AUTHORITY_LEXICON_VERSION);
  pushVarint(out, entries.length);
  for (const entry of entries) {
    pushUtf8(out, entry.normalized);
    pushUtf8(out, entry.display);
    pushVarint(out, entry.weight);
    pushVarint(out, entry.count);
  }
  return Buffer.from(Uint8Array.from(out));
}

export function parseAuthorityHotList(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, AUTHORITY_HOT_MAGIC, "Unsupported Rangefind authority hot list");
  const state = { pos: AUTHORITY_HOT_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== AUTHORITY_LEXICON_VERSION) throw new Error(`Unsupported Rangefind authority hot-list version ${version}`);
  const count = readVarint(bytes, state);
  const entries = new Array(count);
  for (let index = 0; index < count; index++) {
    entries[index] = {
      normalized: readUtf8(bytes, state),
      display: readUtf8(bytes, state),
      weight: readVarint(bytes, state),
      count: readVarint(bytes, state)
    };
  }
  if (state.pos !== bytes.length) throw new Error("Rangefind authority hot list has trailing bytes.");
  return entries;
}
