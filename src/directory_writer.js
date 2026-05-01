import { mkdirSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { buildPagedDirectory } from "./directory.js";

export function writeDirectoryFiles(outDir, entries, pageBytes, relativeBase) {
  const directory = buildPagedDirectory(entries, { pageBytes });
  const pagesDir = resolve(outDir, "directory-pages");
  mkdirSync(pagesDir, { recursive: true });
  const rootCompressed = gzipSync(directory.root, { level: 9 });
  writeFileSync(resolve(outDir, "directory-root.bin.gz"), rootCompressed);
  let pageCompressedBytes = 0;
  for (const page of directory.pages) {
    const compressed = gzipSync(page.buffer, { level: 9 });
    pageCompressedBytes += compressed.length;
    writeFileSync(resolve(pagesDir, page.file), compressed);
  }
  const base = relativeBase ? `${relativeBase.replace(/\/?$/u, "/")}` : "";
  return {
    format: directory.format,
    root: `${base}directory-root.bin.gz`,
    pages: `${base}directory-pages/`,
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
