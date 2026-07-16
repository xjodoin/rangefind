// Mobile runtime for Rangefind indexes.
//
// The core runtime (src/runtime.js) needs exactly two platform capabilities,
// both injectable: a fetch that honors Range headers, and gzip inflation.
// Embedded JS hosts — React Native/Hermes, QuickJS, JavaScriptCore — have
// neither DecompressionStream nor a range-caching HTTP stack, and their file
// systems are reached through host-specific modules rather than node:fs.
//
// createMobileSearch() closes those gaps without forking the runtime, the
// same way src/runtime.node.js does for Node:
//   - Local index directories (bundled with the app or downloaded to device
//     storage) bypass HTTP entirely: ranges are served by positional reads
//     through a caller-provided io adapter (react-native-fs,
//     expo-file-system, or anything exposing { read, size }). Search works
//     fully offline.
//   - http(s) sources go through the host fetch wrapped with caching the
//     browser would otherwise provide: content-addressed objects (hash in
//     the filename) are kept in a bytes-bounded memory LRU and, when the
//     caller provides a persistent cache adapter, on device storage.
//   - Gzip inflation is supplied by the caller (e.g. pako.ungzip) and
//     installed through setInflateImplementation().
//
// Every response handed to the core runtime is a minimal Response-like
// object ({ ok, status, arrayBuffer, json }), so nothing depends on the
// host's Response, Blob, or streams support. The host must still provide
// URL and TextDecoder (standard React Native polyfills cover both).

import { createSearch, setFetchImplementation, setInflateImplementation } from "./runtime.js";

const DEFAULT_MEMORY_CACHE_BYTES = 16 * 1024 * 1024;
// Content-addressed Rangefind artifacts carry a hex digest in the filename
// (packs, directory pages, doc pages, ...). Anything matching is immutable.
const CONTENT_ADDRESSED_NAME = /(^|[.-])[0-9a-f]{8,}\.(bin|gz|bin\.gz)$/u;

function rangeOf(init) {
  const header = init?.headers?.Range || init?.headers?.range;
  if (!header) return null;
  const match = /^bytes=(\d+)-(\d+)$/u.exec(header);
  if (!match) return null;
  const start = Number(match[1]);
  const end = Number(match[2]);
  return { start, end, length: end - start + 1 };
}

function asUint8Array(bytes) {
  if (bytes instanceof Uint8Array) return bytes;
  if (bytes instanceof ArrayBuffer) return new Uint8Array(bytes);
  if (ArrayBuffer.isView(bytes)) return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  throw new Error("io adapter must return a Uint8Array or ArrayBuffer.");
}

function bytesResponse(bytes, status = 200) {
  const view = asUint8Array(bytes);
  return {
    ok: status >= 200 && status < 300,
    status,
    arrayBuffer: async () => view.buffer.slice(view.byteOffset, view.byteOffset + view.byteLength),
    json: async () => JSON.parse(new TextDecoder().decode(view))
  };
}

function statusResponse(status) {
  return {
    ok: false,
    status,
    arrayBuffer: async () => { throw new Error(`No response body (status ${status}).`); },
    json: async () => { throw new Error(`No response body (status ${status}).`); }
  };
}

// ---------------------------------------------------------------------------
// Memory LRU with a byte budget, shared across all engines in the process.

class ByteLru {
  constructor(maxBytes) {
    this.maxBytes = maxBytes;
    this.bytes = 0;
    this.map = new Map();
  }

  get(key) {
    const entry = this.map.get(key);
    if (!entry) return undefined;
    this.map.delete(key);
    this.map.set(key, entry);
    return entry;
  }

  set(key, entry) {
    if (entry.buffer.byteLength > this.maxBytes) return;
    const previous = this.map.get(key);
    if (previous) {
      this.bytes -= previous.buffer.byteLength;
      this.map.delete(key);
    }
    this.map.set(key, entry);
    this.bytes += entry.buffer.byteLength;
    while (this.bytes > this.maxBytes) {
      const oldest = this.map.keys().next().value;
      const evicted = this.map.get(oldest);
      this.map.delete(oldest);
      this.bytes -= evicted.buffer.byteLength;
    }
  }

  clear() {
    this.map.clear();
    this.bytes = 0;
  }
}

// ---------------------------------------------------------------------------
// Local transport — positional reads through the caller's io adapter.

function wrapIo(io) {
  if (typeof io?.read !== "function" || typeof io?.size !== "function") {
    throw new Error("io adapter must provide read(path, offset, length) and size(path).");
  }
  const sizes = new Map();
  return {
    read: (path, offset, length) => io.read(path, offset, length),
    readFile: typeof io.readFile === "function" ? path => io.readFile(path) : null,
    size(path) {
      let pending = sizes.get(path);
      if (!pending) {
        pending = Promise.resolve(io.size(path));
        pending.catch(() => sizes.delete(path));
        sizes.set(path, pending);
      }
      return pending;
    }
  };
}

function filePathOf(href) {
  const url = new URL(href);
  return decodeURIComponent(url.pathname);
}

async function fileFetch(io, href, init, stats) {
  if (!io) throw new Error("Local Rangefind sources need an io adapter ({ read, size }).");
  const path = filePathOf(href);
  const range = rangeOf(init);
  if (range) {
    // Ranges always come from manifest-declared entries, so a failed read is
    // a broken index, not a probe — let the error surface with context.
    const bytes = await io.read(path, range.start, range.length);
    stats.fileRangeReads += 1;
    stats.fileBytesRead += asUint8Array(bytes).byteLength;
    return bytesResponse(bytes, 206);
  }
  try {
    const bytes = io.readFile
      ? await io.readFile(path)
      : await io.read(path, 0, await io.size(path));
    stats.fileFullReads += 1;
    stats.fileBytesRead += asUint8Array(bytes).byteLength;
    return bytesResponse(bytes);
  } catch {
    // Full reads include optional probes (manifest.min.json before
    // manifest.json), so a missing file is a 404, not a failure.
    return statusResponse(404);
  }
}

// ---------------------------------------------------------------------------
// http(s) transport — host fetch plus the caching a browser would provide.

function isImmutableUrl(href) {
  const pathname = new URL(href).pathname;
  const name = pathname.slice(pathname.lastIndexOf("/") + 1);
  return CONTENT_ADDRESSED_NAME.test(name);
}

function isImmutableResponse(response) {
  const control = response.headers?.get?.("cache-control") || "";
  if (/\bimmutable\b/u.test(control)) return true;
  const maxAge = /max-age=(\d+)/u.exec(control);
  return !!maxAge && Number(maxAge[1]) >= 24 * 60 * 60;
}

async function httpFetch(context, href, init, stats) {
  if (!context.fetch) throw new Error("http(s) Rangefind sources need a fetch implementation.");
  const range = rangeOf(init);
  const key = range ? `${href}#${range.start}-${range.end}` : href;
  const immutable = isImmutableUrl(href);

  if (immutable) {
    const cached = context.memory.get(key);
    if (cached) {
      stats.memoryHits += 1;
      return bytesResponse(cached.buffer, range ? 206 : 200);
    }
    if (context.store) {
      const bytes = await Promise.resolve(context.store.get(key)).catch(() => null);
      if (bytes) {
        const buffer = asUint8Array(bytes);
        stats.persistentHits += 1;
        context.memory.set(key, { buffer });
        return bytesResponse(buffer, range ? 206 : 200);
      }
    }
  }

  stats.networkRequests += 1;
  const response = await context.fetch(href, init);
  if (!response.ok && response.status !== 206) return response;
  const buffer = new Uint8Array(await response.arrayBuffer());
  stats.networkBytes += buffer.byteLength;
  if (immutable || isImmutableResponse(response)) {
    context.memory.set(key, { buffer });
    if (context.store) Promise.resolve(context.store.set(key, buffer)).catch(() => {});
  }
  return bytesResponse(buffer, response.status);
}

// ---------------------------------------------------------------------------
// Process-wide router installed as the runtime's fetch implementation.

const router = {
  installed: false,
  memory: null,
  io: null,
  store: null,
  fetch: null,
  stats: {
    memoryHits: 0,
    persistentHits: 0,
    networkRequests: 0,
    networkBytes: 0,
    fileRangeReads: 0,
    fileFullReads: 0,
    fileBytesRead: 0
  }
};

function installRouter(options) {
  if (!router.installed) {
    router.memory = new ByteLru(options.memoryCacheBytes || DEFAULT_MEMORY_CACHE_BYTES);
    router.installed = true;
    setFetchImplementation((url, init) => {
      const href = String(url);
      if (href.startsWith("file:")) return fileFetch(router.io, href, init, router.stats);
      return httpFetch(router, href, init, router.stats);
    });
  }
  if (options.io) router.io = wrapIo(options.io);
  if (options.fetch) router.fetch = options.fetch;
  else if (!router.fetch && typeof fetch === "function") router.fetch = (url, init) => fetch(url, init);
  if (options.cache !== undefined) router.store = options.cache || null;
  if (options.inflate) setInflateImplementation(options.inflate);
}

function normalizeSource(source) {
  const text = String(source || "");
  if (!text) throw new Error("createMobileSearch requires a source (index directory path or http(s) URL).");
  if (/^https?:\/\//u.test(text) || text.startsWith("file:")) {
    return text.endsWith("/") ? text : `${text}/`;
  }
  if (!text.startsWith("/")) {
    throw new Error("Local index paths must be absolute on mobile hosts (e.g. DocumentDirectoryPath + \"/rangefind\").");
  }
  const url = `file://${encodeURI(text)}`;
  return url.endsWith("/") ? url : `${url}/`;
}

/**
 * Create a Rangefind search engine for embedded JS hosts (React Native,
 * QuickJS, JavaScriptCore).
 *
 * @param {object} options
 *   - source: index location — absolute device path, file:// URL, or http(s)
 *     base URL (aliases: baseUrl).
 *   - io: adapter for local sources — { read(path, offset, length),
 *     size(path), readFile?(path) } returning Uint8Array/ArrayBuffer.
 *   - inflate: gzip inflation (e.g. pako.ungzip); required on hosts without
 *     DecompressionStream.
 *   - fetch: fetch for http(s) sources (default: globalThis.fetch).
 *   - cache: optional persistent cache adapter for immutable objects from
 *     http(s) sources — { get(key), set(key, bytes) }.
 *   - memoryCacheBytes: memory LRU budget (default 16 MiB, process-wide).
 *   Remaining options pass through to createSearch (manifestName, trace, ...).
 */
export async function createMobileSearch(options = {}) {
  const { source, baseUrl, io, inflate, fetch: fetchFn, cache, memoryCacheBytes, ...rest } = options;
  if (!inflate && !("DecompressionStream" in globalThis)) {
    throw new Error("This host has no DecompressionStream: pass inflate (e.g. pako.ungzip) to createMobileSearch.");
  }
  const resolved = normalizeSource(source || baseUrl);
  if (resolved.startsWith("file:") && !io && !router.io) {
    throw new Error("Local Rangefind sources need an io adapter ({ read, size }).");
  }
  installRouter({ io, inflate, fetch: fetchFn, cache, memoryCacheBytes });
  const engine = await createSearch({ ...rest, baseUrl: resolved });
  return {
    ...engine,
    baseUrl: resolved,
    cacheStats: () => ({ ...router.stats, memoryBytes: router.memory.bytes })
  };
}

/** Reset the process-wide memory cache and counters. */
export function resetMobileRuntimeCaches() {
  if (router.memory) router.memory.clear();
  for (const key of Object.keys(router.stats)) router.stats[key] = 0;
}

export default createMobileSearch;
