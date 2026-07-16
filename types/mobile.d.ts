// `rangefind/mobile`: the same query engine on embedded JS hosts
// (React Native/Hermes, QuickJS, JavaScriptCore) over local index
// directories (positional reads through an io adapter) or HTTP(S) indexes
// with memory + optional persistent caching.

import type { CreateSearchOptions, SearchEngine } from "./runtime.js";

/** Positional-read adapter over the host file system (react-native-fs, expo-file-system, ...). */
export interface MobileIoAdapter {
  /** Read `length` bytes starting at `offset`; may return fewer at end of file. */
  read(path: string, offset: number, length: number): Promise<Uint8Array | ArrayBuffer>;
  /** File size in bytes (used for whole-file reads). */
  size(path: string): Promise<number>;
  /** Optional whole-file fast path. */
  readFile?(path: string): Promise<Uint8Array | ArrayBuffer>;
}

/** Persistent cache for immutable content-addressed objects from http(s) sources. */
export interface MobileCacheAdapter {
  get(key: string): Promise<Uint8Array | ArrayBuffer | null> | Uint8Array | ArrayBuffer | null;
  set(key: string, bytes: Uint8Array): Promise<void> | void;
}

export interface MobileSearchOptions extends CreateSearchOptions {
  /** Absolute device path, file:// URL, or http(s) URL of the index directory. */
  source?: string;
  io?: MobileIoAdapter;
  /** Gzip inflation (e.g. pako.ungzip); required on hosts without DecompressionStream. */
  inflate?: (compressed: ArrayBuffer) => ArrayBuffer | Uint8Array | Promise<ArrayBuffer | Uint8Array>;
  fetch?: typeof fetch;
  cache?: MobileCacheAdapter | null;
  /** Memory LRU budget in bytes (default 16 MiB, process-wide). */
  memoryCacheBytes?: number;
  [key: string]: unknown;
}

export interface MobileCacheStats {
  memoryHits: number;
  persistentHits: number;
  networkRequests: number;
  networkBytes: number;
  fileRangeReads: number;
  fileFullReads: number;
  fileBytesRead: number;
  memoryBytes: number;
}

export interface MobileSearchEngine extends SearchEngine {
  baseUrl: string;
  cacheStats(): MobileCacheStats;
}

export function createMobileSearch(options?: MobileSearchOptions): Promise<MobileSearchEngine>;
export function resetMobileRuntimeCaches(): void;
export default createMobileSearch;
