import {
  DIRECTORY_PAGE_MAGIC,
  DIRECTORY_ROOT_MAGIC,
  pushVarint,
  readVarint
} from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const DIRECTORY_FORMAT = "rfdir-v2";
export const DEFAULT_DIRECTORY_PAGE_BYTES = 64 * 1024;
const DIRECTORY_VERSION = 2;
const DEFAULT_BLOOM_BITS_PER_KEY = 10;

const textEncoder = new TextEncoder();

function varintLength(value) {
  let n = Math.max(0, Math.floor(value));
  let length = 1;
  while (n >= 0x80) {
    length++;
    n = Math.floor(n / 0x80);
  }
  return length;
}

function utf8Length(value) {
  const bytes = textEncoder.encode(String(value || ""));
  return varintLength(bytes.length) + bytes.length;
}

function commonPrefixLength(a, b) {
  const max = Math.min(a.length, b.length);
  let i = 0;
  while (i < max && a[i] === b[i]) i++;
  return i;
}

function compareKeys(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function pageEntryLength(entry, previous) {
  const prefix = commonPrefixLength(previous, entry.shard);
  return varintLength(prefix)
    + utf8Length(entry.shard.slice(prefix))
    + varintLength(entry.packIndex)
    + varintLength(entry.offset)
    + varintLength(entry.length)
    + varintLength(entry.logicalLength || 0)
    + utf8Length(entry.checksum.algorithm)
    + utf8Length(entry.checksum.value);
}

function normalizePackIndex(value) {
  if (Number.isFinite(value)) return Math.max(0, Math.floor(value));
  const parsed = Number(String(value || "0").replace(/\D/gu, ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function normalizeEntry(entry) {
  if (!entry.checksum?.value) throw new Error(`Rangefind directory entry ${entry.shard || ""} is missing a checksum.`);
  return {
    shard: String(entry.shard || ""),
    packIndex: normalizePackIndex(entry.packIndex ?? entry.pack),
    offset: Math.max(0, Math.floor(entry.offset ?? 0)),
    length: Math.max(0, Math.floor(entry.length ?? entry.physicalLength ?? 0)),
    logicalLength: Math.max(0, Math.floor(entry.logicalLength || 0)),
    checksum: {
      algorithm: entry.checksum.algorithm || "sha256",
      value: entry.checksum.value
    }
  };
}

function hashString(value, seed) {
  let hash = (0x811c9dc5 ^ seed) >>> 0;
  for (let i = 0; i < value.length; i++) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash >>> 0;
}

function setBit(bytes, bit) {
  bytes[bit >> 3] |= 1 << (bit & 7);
}

function hasBit(bytes, bit) {
  return (bytes[bit >> 3] & (1 << (bit & 7))) !== 0;
}

function buildBloom(keys, bitsPerKey = DEFAULT_BLOOM_BITS_PER_KEY) {
  if (!keys.length) return { bits: 0, hashes: 0, bytes: Uint8Array.of() };
  const bits = Math.max(64, keys.length * bitsPerKey);
  const hashes = Math.max(1, Math.round(bitsPerKey * Math.LN2));
  const bytes = new Uint8Array(Math.ceil(bits / 8));
  for (const key of keys) {
    const h1 = hashString(key, 0);
    const h2 = hashString(key, 0x9e3779b9) | 1;
    for (let i = 0; i < hashes; i++) setBit(bytes, ((h1 + Math.imul(i, h2)) >>> 0) % bits);
  }
  return { bits, hashes, bytes };
}

function bloomMightContain(bloom, key) {
  if (!bloom?.bits || !bloom.hashes) return true;
  const h1 = hashString(key, 0);
  const h2 = hashString(key, 0x9e3779b9) | 1;
  for (let i = 0; i < bloom.hashes; i++) {
    if (!hasBit(bloom.bytes, ((h1 + Math.imul(i, h2)) >>> 0) % bloom.bits)) return false;
  }
  return true;
}

function buildDirectoryPage(entries) {
  const out = [...DIRECTORY_PAGE_MAGIC];
  pushVarint(out, DIRECTORY_VERSION);
  pushVarint(out, entries.length);
  let previous = "";
  for (const entry of entries) {
    const prefix = commonPrefixLength(previous, entry.shard);
    pushVarint(out, prefix);
    pushUtf8(out, entry.shard.slice(prefix));
    pushVarint(out, entry.packIndex);
    pushVarint(out, entry.offset);
    pushVarint(out, entry.length);
    pushVarint(out, entry.logicalLength || 0);
    pushUtf8(out, entry.checksum.algorithm);
    pushUtf8(out, entry.checksum.value);
    previous = entry.shard;
  }
  return Buffer.from(Uint8Array.from(out));
}

export function buildDirectoryRoot(pages, entryCount, bloom) {
  const out = [...DIRECTORY_ROOT_MAGIC];
  pushVarint(out, DIRECTORY_VERSION);
  pushVarint(out, entryCount);
  pushVarint(out, pages.length);
  pushVarint(out, bloom.bits);
  pushVarint(out, bloom.hashes);
  pushVarint(out, bloom.bytes.length);
  for (const byte of bloom.bytes) out.push(byte);
  for (const page of pages) {
    pushUtf8(out, page.first);
    pushUtf8(out, page.last);
    pushUtf8(out, page.file);
    pushVarint(out, page.count);
    pushVarint(out, page.rawBytes);
  }
  return Buffer.from(Uint8Array.from(out));
}

function pageFile(index) {
  return `${String(index).padStart(4, "0")}.bin.gz`;
}

export function buildPagedDirectory(entries, options = {}) {
  const pageBytes = Math.max(1024, options.pageBytes || DEFAULT_DIRECTORY_PAGE_BYTES);
  const normalized = entries
    .map(normalizeEntry)
    .filter(entry => entry.shard)
    .sort((a, b) => compareKeys(a.shard, b.shard));
  const pages = [];
  let current = [];
  let rawBytes = DIRECTORY_PAGE_MAGIC.length + varintLength(DIRECTORY_VERSION);
  let previous = "";

  function flushPage() {
    if (!current.length) return;
    const index = pages.length;
    const buffer = buildDirectoryPage(current);
    pages.push({
      file: pageFile(index),
      first: current[0].shard,
      last: current[current.length - 1].shard,
      count: current.length,
      rawBytes: buffer.length,
      buffer,
      entries: current
    });
    current = [];
    rawBytes = DIRECTORY_PAGE_MAGIC.length + varintLength(DIRECTORY_VERSION);
    previous = "";
  }

  for (const entry of normalized) {
    const entryBytes = pageEntryLength(entry, previous);
    if (current.length && rawBytes + entryBytes > pageBytes) flushPage();
    current.push(entry);
    rawBytes += entryBytes;
    previous = entry.shard;
  }
  flushPage();

  const bloom = buildBloom(normalized.map(entry => entry.shard), options.bloomBitsPerKey || DEFAULT_BLOOM_BITS_PER_KEY);
  const root = buildDirectoryRoot(pages, normalized.length, bloom);
  return {
    format: DIRECTORY_FORMAT,
    version: DIRECTORY_VERSION,
    pageBytes,
    root,
    bloom,
    pages,
    stats: {
      format: DIRECTORY_FORMAT,
      version: DIRECTORY_VERSION,
      entries: normalized.length,
      page_bytes: pageBytes,
      page_files: pages.length,
      root_bytes: root.length,
      pages_bytes: pages.reduce((sum, page) => sum + page.buffer.length, 0),
      bloom_bits: bloom.bits,
      bloom_hashes: bloom.hashes,
      bloom_bytes: bloom.bytes.length
    }
  };
}

export function parseDirectoryRoot(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, DIRECTORY_ROOT_MAGIC, "Unsupported Rangefind directory root");
  const state = { pos: DIRECTORY_ROOT_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== DIRECTORY_VERSION) throw new Error(`Unsupported Rangefind directory root version ${version}`);
  const entries = readVarint(bytes, state);
  const pageCount = readVarint(bytes, state);
  const bloomBits = readVarint(bytes, state);
  const bloomHashes = readVarint(bytes, state);
  const bloomLength = readVarint(bytes, state);
  const bloomBytes = bytes.subarray(state.pos, state.pos + bloomLength);
  state.pos += bloomLength;
  const pages = new Array(pageCount);
  for (let i = 0; i < pageCount; i++) {
    pages[i] = {
      first: readUtf8(bytes, state),
      last: readUtf8(bytes, state),
      file: readUtf8(bytes, state),
      count: readVarint(bytes, state),
      rawBytes: readVarint(bytes, state)
    };
  }
  return {
    format: DIRECTORY_FORMAT,
    version,
    entries,
    pages,
    bloom: { bits: bloomBits, hashes: bloomHashes, bytes: bloomBytes }
  };
}

export function parseDirectoryPage(buffer, options = {}) {
  const packTable = Array.isArray(options) ? options : options.packTable || options.packs || [];
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, DIRECTORY_PAGE_MAGIC, "Unsupported Rangefind directory page");
  const state = { pos: DIRECTORY_PAGE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== DIRECTORY_VERSION) throw new Error(`Unsupported Rangefind directory page version ${version}`);
  const count = readVarint(bytes, state);
  const entries = new Map();
  let previous = "";
  for (let i = 0; i < count; i++) {
    const prefix = readVarint(bytes, state);
    const suffix = readUtf8(bytes, state);
    const shard = previous.slice(0, prefix) + suffix;
    const packIndex = readVarint(bytes, state);
    const entry = {
      pack: packTable[packIndex] || `${String(packIndex).padStart(4, "0")}.bin`,
      offset: readVarint(bytes, state),
      length: readVarint(bytes, state)
    };
    entry.physicalLength = entry.length;
    const logicalLength = readVarint(bytes, state);
    const algorithm = readUtf8(bytes, state);
    const value = readUtf8(bytes, state);
    if (!value) throw new Error(`Rangefind directory page entry ${shard} is missing a checksum.`);
    entry.logicalLength = logicalLength || null;
    entry.checksum = { algorithm: algorithm || "sha256", value };
    entries.set(shard, entry);
    previous = shard;
  }
  return entries;
}

export function directoryMightContain(root, shard) {
  return bloomMightContain(root?.bloom, shard);
}

export function findDirectoryPage(root, shard) {
  if (!root?.pages?.length || !directoryMightContain(root, shard)) return null;
  let lo = 0;
  let hi = root.pages.length - 1;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    const page = root.pages[mid];
    if (shard < page.first) hi = mid - 1;
    else if (shard > page.last) lo = mid + 1;
    else return page;
  }
  return null;
}
