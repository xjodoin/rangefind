import { mkdirSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { gzipSync } from "node:zlib";
import {
  addDirectoryBloomKey,
  buildDirectoryPage,
  buildDirectoryRoot,
  buildPagedDirectory,
  createDirectoryBloom,
  DIRECTORY_FORMAT,
  directoryPageEntryLength,
  normalizeDirectoryEntry
} from "./directory.js";
import { OBJECT_NAME_HASH_LENGTH } from "./object_store.js";

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function hashedFile(prefix, hash, suffix) {
  return `${prefix}.${hash.slice(0, OBJECT_NAME_HASH_LENGTH)}${suffix}`;
}

export function writeDirectoryFiles(outDir, entries, pageBytes, relativeBase, options = {}) {
  const directory = buildPagedDirectory(entries, { pageBytes });
  const pagesDir = resolve(outDir, "directory-pages");
  mkdirSync(pagesDir, { recursive: true });
  let pageCompressedBytes = 0;
  for (const page of directory.pages) {
    const compressed = gzipSync(page.buffer, { level: 9 });
    const hash = sha256Hex(compressed);
    page.file = hashedFile(page.file.replace(/\.bin\.gz$/u, ""), hash, ".bin.gz");
    page.contentHash = hash;
    pageCompressedBytes += compressed.length;
    writeFileSync(resolve(pagesDir, page.file), compressed);
  }
  const root = buildDirectoryRoot(directory.pages, directory.stats.entries, directory.bloom);
  const rootCompressed = gzipSync(root, { level: 9 });
  const rootHash = sha256Hex(rootCompressed);
  const rootFile = hashedFile("directory-root", rootHash, ".bin.gz");
  writeFileSync(resolve(outDir, rootFile), rootCompressed);
  const base = relativeBase ? `${relativeBase.replace(/\/?$/u, "/")}` : "";
  const packTable = (options.packTable || options.packs || []).map(pack => typeof pack === "string" ? pack : pack.file);
  return {
    format: directory.format,
    version: directory.version,
    root: `${base}${rootFile}`,
    pages: `${base}directory-pages/`,
    pack_table: packTable,
    immutable: true,
    root_hash: rootHash,
    entries: directory.stats.entries,
    page_bytes: directory.stats.page_bytes,
    page_files: directory.stats.page_files,
    root_bytes: rootCompressed.length,
    page_bytes_compressed: pageCompressedBytes,
    total_bytes: rootCompressed.length + pageCompressedBytes,
    bloom_bits: directory.stats.bloom_bits,
    bloom_hashes: directory.stats.bloom_hashes,
    bloom_bytes: directory.stats.bloom_bytes
  };
}

export async function writeDirectoryFilesFromSortedEntries(outDir, entries, entryCount, pageBytes, relativeBase, options = {}) {
  const resolvedPageBytes = Math.max(1024, pageBytes || 64 * 1024);
  const pagesDir = resolve(outDir, "directory-pages");
  mkdirSync(pagesDir, { recursive: true });
  const bloom = createDirectoryBloom(entryCount, options.bloomBitsPerKey || 10);
  const pages = [];
  let pageCompressedBytes = 0;
  let current = [];
  let rawBytes = 4 + 1;
  let previous = "";
  let actualEntries = 0;

  function flushPage() {
    if (!current.length) return;
    const index = pages.length;
    const buffer = buildDirectoryPage(current);
    const compressed = gzipSync(buffer, { level: 9 });
    const hash = sha256Hex(compressed);
    const file = hashedFile(String(index).padStart(4, "0"), hash, ".bin.gz");
    writeFileSync(resolve(pagesDir, file), compressed);
    pages.push({
      file,
      contentHash: hash,
      first: current[0].shard,
      last: current[current.length - 1].shard,
      count: current.length,
      rawBytes: buffer.length
    });
    pageCompressedBytes += compressed.length;
    current = [];
    rawBytes = 4 + 1;
    previous = "";
  }

  for await (const sourceEntry of entries) {
    const entry = normalizeDirectoryEntry(sourceEntry);
    if (!entry.shard) continue;
    addDirectoryBloomKey(bloom, entry.shard);
    const entryBytes = directoryPageEntryLength(entry, previous);
    if (current.length && rawBytes + entryBytes > resolvedPageBytes) flushPage();
    current.push(entry);
    rawBytes += entryBytes;
    previous = entry.shard;
    actualEntries++;
  }
  flushPage();

  const root = buildDirectoryRoot(pages, actualEntries, bloom);
  const rootCompressed = gzipSync(root, { level: 9 });
  const rootHash = sha256Hex(rootCompressed);
  const rootFile = hashedFile("directory-root", rootHash, ".bin.gz");
  writeFileSync(resolve(outDir, rootFile), rootCompressed);
  const base = relativeBase ? `${relativeBase.replace(/\/?$/u, "/")}` : "";
  const packTable = (options.packTable || options.packs || []).map(pack => typeof pack === "string" ? pack : pack.file);
  return {
    format: DIRECTORY_FORMAT,
    version: 2,
    root: `${base}${rootFile}`,
    pages: `${base}directory-pages/`,
    pack_table: packTable,
    immutable: true,
    root_hash: rootHash,
    entries: actualEntries,
    page_bytes: resolvedPageBytes,
    page_files: pages.length,
    root_bytes: rootCompressed.length,
    page_bytes_compressed: pageCompressedBytes,
    total_bytes: rootCompressed.length + pageCompressedBytes,
    bloom_bits: bloom.bits,
    bloom_hashes: bloom.hashes,
    bloom_bytes: bloom.bytes.length
  };
}
