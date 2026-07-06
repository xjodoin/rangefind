import {
  SUGGEST_BRANCH_PAGE_MAGIC,
  SUGGEST_PAGE_MAGIC,
  SUGGEST_ROOT_MAGIC,
  pushVarint,
  readVarint
} from "./binary.js";
import { fold } from "./analyzer.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const SUGGEST_ROOT_FORMAT = "rfsuggestroot-v1";
export const SUGGEST_BRANCH_PAGE_FORMAT = "rfsuggestbranch-v1";
export const SUGGEST_PAGE_FORMAT = "rfsuggestpage-v1";

const FORMAT_VERSION = 1;
const FLAG_DISPLAY_EQUALS_KEY = 1;

// Suggestion keys are diacritic-folded, lowercased, punctuation-collapsed,
// and keep letters/numbers from every script, so "Saint-Denis" is reachable
// by typing "saint d" and "Montréal" by "montre".
export function suggestKey(value) {
  return fold(value)
    .replace(/[^\p{L}\p{N}]+/gu, " ")
    .trim()
    .replace(/\s+/gu, " ");
}

function commonPrefixLength(a, b) {
  const max = Math.min(a.length, b.length);
  let i = 0;
  while (i < max && a.charCodeAt(i) === b.charCodeAt(i)) i++;
  return i;
}

export function compareSuggestions(a, b) {
  if (a.key !== b.key) return a.key < b.key ? -1 : 1;
  if (a.weight !== b.weight) return b.weight - a.weight;
  if (a.display !== b.display) return a.display < b.display ? -1 : 1;
  return 0;
}

// Entries: [{ key, display, weight, count }] sorted with compareSuggestions.
export function encodeSuggestPage(entries) {
  const out = [...SUGGEST_PAGE_MAGIC];
  pushVarint(out, FORMAT_VERSION);
  pushVarint(out, entries.length);
  let previousKey = "";
  for (const entry of entries) {
    const lcp = commonPrefixLength(previousKey, entry.key);
    pushVarint(out, lcp);
    pushUtf8(out, entry.key.slice(lcp));
    const displayEqualsKey = entry.display === entry.key;
    out.push(displayEqualsKey ? FLAG_DISPLAY_EQUALS_KEY : 0);
    if (!displayEqualsKey) pushUtf8(out, entry.display);
    pushVarint(out, entry.weight);
    pushVarint(out, entry.count);
    previousKey = entry.key;
  }
  return Buffer.from(Uint8Array.from(out));
}

export function decodeSuggestPage(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, SUGGEST_PAGE_MAGIC, "Unsupported Rangefind suggest page");
  const state = { pos: SUGGEST_PAGE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) throw new Error(`Unsupported Rangefind suggest page version ${version}`);
  const count = readVarint(bytes, state);
  const entries = new Array(count);
  let previousKey = "";
  for (let i = 0; i < count; i++) {
    const lcp = readVarint(bytes, state);
    const key = previousKey.slice(0, lcp) + readUtf8(bytes, state);
    const flags = bytes[state.pos++];
    const display = flags & FLAG_DISPLAY_EQUALS_KEY ? key : readUtf8(bytes, state);
    const weight = readVarint(bytes, state);
    const entryCount = readVarint(bytes, state);
    entries[i] = { key, display, weight, count: entryCount };
    previousKey = key;
  }
  if (state.pos !== bytes.length) throw new Error("Rangefind suggest page has trailing bytes.");
  return { count, entries };
}

function pushObjectPointer(out, entry, packIndexes) {
  pushVarint(out, packIndexes.get(entry.pack) ?? 0);
  pushVarint(out, entry.offset);
  pushVarint(out, entry.length);
  pushVarint(out, entry.physicalLength || entry.length);
  pushVarint(out, entry.logicalLength || 0);
  pushUtf8(out, entry.checksum?.algorithm || "");
  pushUtf8(out, entry.checksum?.value || "");
}

function readObjectPointer(bytes, state, packTable, target) {
  const packIndex = readVarint(bytes, state);
  target.pack = packTable[packIndex];
  target.offset = readVarint(bytes, state);
  target.length = readVarint(bytes, state);
  target.physicalLength = readVarint(bytes, state);
  target.logicalLength = readVarint(bytes, state) || null;
  const algorithm = readUtf8(bytes, state);
  const value = readUtf8(bytes, state);
  target.checksum = value ? { algorithm: algorithm || "sha256", value } : null;
  return target;
}

// Page summaries are front-coded against the previous minKey. A page covers
// keys [minKey, next page's minKey); the runtime prunes by prefix range and
// visits candidates in max-weight order with a block-max style stop proof.
function pushPageSummary(out, page, previousMinKey, packIndexes) {
  const lcp = commonPrefixLength(previousMinKey, page.minKey);
  pushVarint(out, lcp);
  pushUtf8(out, page.minKey.slice(lcp));
  pushVarint(out, page.maxWeight);
  pushVarint(out, page.count);
  pushObjectPointer(out, page.entry, packIndexes);
}

function readPageSummary(bytes, state, packTable, previousMinKey, index) {
  const lcp = readVarint(bytes, state);
  const minKey = previousMinKey.slice(0, lcp) + readUtf8(bytes, state);
  const maxWeight = readVarint(bytes, state);
  const count = readVarint(bytes, state);
  return readObjectPointer(bytes, state, packTable, { index, minKey, maxWeight, count });
}

export function encodeSuggestRoot({ total, pageSize, pages, branches, hot, packTable, packIndexes }) {
  const out = [...SUGGEST_ROOT_MAGIC];
  pushVarint(out, FORMAT_VERSION);
  pushVarint(out, total);
  pushVarint(out, pageSize);
  pushVarint(out, pages?.length ?? branches.reduce((sum, branch) => sum + branch.pageCount, 0));
  pushVarint(out, packTable.length);
  for (const pack of packTable) pushUtf8(out, pack);
  const levels = branches ? 2 : 1;
  out.push(levels);
  if (levels === 1) {
    pushVarint(out, pages.length);
    let previousMinKey = "";
    for (const page of pages) {
      pushPageSummary(out, page, previousMinKey, packIndexes);
      previousMinKey = page.minKey;
    }
  } else {
    pushVarint(out, branches.length);
    let previousMinKey = "";
    for (const branch of branches) {
      const lcp = commonPrefixLength(previousMinKey, branch.minKey);
      pushVarint(out, lcp);
      pushUtf8(out, branch.minKey.slice(lcp));
      pushVarint(out, branch.maxWeight);
      pushVarint(out, branch.count);
      pushVarint(out, branch.firstPageIndex);
      pushVarint(out, branch.pageCount);
      pushObjectPointer(out, branch.entry, packIndexes);
      previousMinKey = branch.minKey;
    }
  }
  // Hot prefixes carry a precomputed, display-deduped top list so the very
  // first keystroke costs one small page fetch instead of a best-first walk.
  pushVarint(out, hot?.length || 0);
  for (const item of hot || []) {
    pushUtf8(out, item.prefix);
    pushVarint(out, item.count);
    pushObjectPointer(out, item.entry, packIndexes);
  }
  return {
    buffer: Buffer.from(Uint8Array.from(out)),
    meta: {
      format: SUGGEST_ROOT_FORMAT,
      page_format: SUGGEST_PAGE_FORMAT,
      total,
      page_size: pageSize,
      levels,
      pages: pages?.length ?? branches.reduce((sum, branch) => sum + branch.pageCount, 0),
      branches: branches ? branches.length : 0,
      hot_prefixes: hot?.length || 0
    }
  };
}

export function parseSuggestRoot(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, SUGGEST_ROOT_MAGIC, "Unsupported Rangefind suggest root");
  const state = { pos: SUGGEST_ROOT_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) throw new Error(`Unsupported Rangefind suggest root version ${version}`);
  const total = readVarint(bytes, state);
  const pageSize = readVarint(bytes, state);
  const pageCount = readVarint(bytes, state);
  const packCount = readVarint(bytes, state);
  const packTable = new Array(packCount);
  for (let i = 0; i < packCount; i++) packTable[i] = readUtf8(bytes, state);
  const levels = bytes[state.pos++];
  const root = {
    format: SUGGEST_ROOT_FORMAT,
    pageFormat: SUGGEST_PAGE_FORMAT,
    total,
    pageSize,
    pageCount,
    levels,
    packTable,
    pages: null,
    branches: null
  };
  if (levels === 1) {
    const count = readVarint(bytes, state);
    const pages = new Array(count);
    let previousMinKey = "";
    for (let i = 0; i < count; i++) {
      pages[i] = readPageSummary(bytes, state, packTable, previousMinKey, i);
      previousMinKey = pages[i].minKey;
    }
    root.pages = pages;
  } else if (levels === 2) {
    const count = readVarint(bytes, state);
    const branches = new Array(count);
    let previousMinKey = "";
    for (let i = 0; i < count; i++) {
      const lcp = readVarint(bytes, state);
      const minKey = previousMinKey.slice(0, lcp) + readUtf8(bytes, state);
      const maxWeight = readVarint(bytes, state);
      const branchCount = readVarint(bytes, state);
      const firstPageIndex = readVarint(bytes, state);
      const branchPageCount = readVarint(bytes, state);
      branches[i] = readObjectPointer(bytes, state, packTable, {
        index: i,
        minKey,
        maxWeight,
        count: branchCount,
        firstPageIndex,
        pageCount: branchPageCount
      });
      previousMinKey = minKey;
    }
    root.branches = branches;
  } else {
    throw new Error(`Unsupported Rangefind suggest root levels ${levels}`);
  }
  const hotCount = readVarint(bytes, state);
  const hot = new Map();
  for (let i = 0; i < hotCount; i++) {
    const prefix = readUtf8(bytes, state);
    const count = readVarint(bytes, state);
    hot.set(prefix, readObjectPointer(bytes, state, packTable, {
      index: pageCount + i,
      prefix,
      count
    }));
  }
  root.hot = hot;
  if (state.pos !== bytes.length) throw new Error("Rangefind suggest root has trailing bytes.");
  return root;
}

export function encodeSuggestBranchPage({ branchIndex, firstPageIndex, pages, packIndexes }) {
  const out = [...SUGGEST_BRANCH_PAGE_MAGIC];
  pushVarint(out, FORMAT_VERSION);
  pushVarint(out, branchIndex);
  pushVarint(out, firstPageIndex);
  pushVarint(out, pages.length);
  let previousMinKey = "";
  for (const page of pages) {
    pushPageSummary(out, page, previousMinKey, packIndexes);
    previousMinKey = page.minKey;
  }
  return Buffer.from(Uint8Array.from(out));
}

export function decodeSuggestBranchPage(buffer, packTable) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, SUGGEST_BRANCH_PAGE_MAGIC, "Unsupported Rangefind suggest branch page");
  const state = { pos: SUGGEST_BRANCH_PAGE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) throw new Error(`Unsupported Rangefind suggest branch page version ${version}`);
  const branchIndex = readVarint(bytes, state);
  const firstPageIndex = readVarint(bytes, state);
  const count = readVarint(bytes, state);
  const pages = new Array(count);
  let previousMinKey = "";
  for (let i = 0; i < count; i++) {
    pages[i] = readPageSummary(bytes, state, packTable, previousMinKey, firstPageIndex + i);
    previousMinKey = pages[i].minKey;
  }
  if (state.pos !== bytes.length) throw new Error("Rangefind suggest branch page has trailing bytes.");
  return { branchIndex, firstPageIndex, pages };
}

// Aggregates raw (surface, weight) rows into unique suggestion entries.
// `rows` is a Map from `${key}|${display}` to { key, display, weight,
// count } maintained by the caller via addSuggestionRow.
export function addSuggestionRow(rows, surface, weight, options = {}) {
  const display = String(surface || "").trim().replace(/\s+/gu, " ");
  if (!display) return;
  const key = suggestKey(display);
  if (!key || key.length < 1) return;
  const count = Math.max(1, Math.floor(Number(options.count ?? 1)));
  addSuggestionKeyRow(rows, key, display, weight, count);
  if (options.tokenPrefixes === false) return;
  const maxTokenKeys = Math.max(1, Math.floor(Number(options.maxTokenKeys ?? 4)));
  let start = key.indexOf(" ");
  let emitted = 1;
  while (start !== -1 && emitted < maxTokenKeys) {
    const tokenKey = key.slice(start + 1);
    if (tokenKey.length > 1) {
      addSuggestionKeyRow(rows, tokenKey, display, weight, count);
      emitted++;
    }
    start = key.indexOf(" ", start + 1);
  }
}

function addSuggestionKeyRow(rows, key, display, weight, count) {
  const mapKey = `${key}|${display}`;
  const existing = rows.get(mapKey);
  const numericWeight = Number.isFinite(weight) ? Math.max(0, Math.floor(weight)) : 0;
  if (existing) {
    existing.count += count;
    if (numericWeight > existing.weight) existing.weight = numericWeight;
  } else {
    rows.set(mapKey, { key, display, weight: numericWeight, count });
  }
}

// Final ranking weight: explicit weight when provided, otherwise popularity
// (how many documents carry the surface).
export function finalizeSuggestionRows(rows) {
  const entries = [...rows.values()];
  for (const entry of entries) {
    if (entry.weight === 0) entry.weight = entry.count;
  }
  entries.sort(compareSuggestions);
  return entries;
}
