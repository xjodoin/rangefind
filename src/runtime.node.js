// Node runtime for Rangefind indexes.
//
// The core runtime (src/runtime.js) speaks plain fetch + HTTP Range requests
// and, in the browser, gets caching for free from the HTTP cache: immutable
// content-addressed packs are cached forever, mutable manifests revalidate
// with ETags, and repeated range requests hit the disk cache. Node's fetch
// has none of that, and cannot read file:// URLs at all.
//
// createNodeSearch() closes both gaps for server-side consumers (MCP servers,
// CLIs, SSR) without forking the runtime:
//   - file:// / local directory sources bypass HTTP entirely: ranges are
//     served by positional reads on pooled file handles.
//   - http(s) sources go through a caching fetch that mirrors browser
//     semantics: content-addressed objects (hash in the filename, or
//     Cache-Control immutable/long max-age) are cached on disk and in a
//     bytes-bounded memory LRU; everything else revalidates with
//     If-None-Match / If-Modified-Since and reuses the cached body on 304.
//
// The cache is keyed by absolute URL (+ byte range), so one process-wide
// fetch router serves any number of engines over different indexes.

import { createHash } from "node:crypto";
import { open, mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { createSearch, setFetchImplementation } from "./runtime.js";

const DEFAULT_MEMORY_CACHE_BYTES = 64 * 1024 * 1024;
const DEFAULT_FILE_HANDLE_LIMIT = 32;
// Content-addressed Rangefind artifacts carry a hex digest in the filename
// (packs, directory pages, doc pages, ...). Anything matching is immutable.
const CONTENT_ADDRESSED_NAME = /(^|[.-])[0-9a-f]{8,}\.(bin|gz|bin\.gz)$/u;

function sha256Hex(text) {
  return createHash("sha256").update(text).digest("hex");
}

function rangeOf(init) {
  const header = init?.headers?.Range || init?.headers?.range;
  if (!header) return null;
  const match = /^bytes=(\d+)-(\d+)$/u.exec(header);
  if (!match) return null;
  const start = Number(match[1]);
  const end = Number(match[2]);
  return { start, end, length: end - start + 1 };
}

function bufferResponse(buffer, { status = 200, headers } = {}) {
  const body = buffer instanceof Uint8Array
    ? buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength)
    : buffer;
  return new Response(body, { status, headers });
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
// file:// transport — pooled handles, positional reads, no HTTP.

class FileHandlePool {
  constructor(limit = DEFAULT_FILE_HANDLE_LIMIT) {
    this.limit = limit;
    this.map = new Map();
  }

  async acquire(path) {
    const cached = this.map.get(path);
    if (cached) {
      this.map.delete(path);
      this.map.set(path, cached);
      return cached;
    }
    const entry = open(path, "r").then(async handle => ({ handle, size: (await handle.stat()).size }));
    this.map.set(path, entry);
    while (this.map.size > this.limit) {
      const oldestKey = this.map.keys().next().value;
      const oldest = this.map.get(oldestKey);
      this.map.delete(oldestKey);
      oldest.then(({ handle }) => handle.close()).catch(() => {});
    }
    try {
      return await entry;
    } catch (error) {
      if (this.map.get(path) === entry) this.map.delete(path);
      throw error;
    }
  }

  async close() {
    const entries = [...this.map.values()];
    this.map.clear();
    await Promise.all(entries.map(entry => entry.then(({ handle }) => handle.close()).catch(() => {})));
  }
}

async function fileFetch(pool, url, init, stats) {
  const path = fileURLToPath(url);
  const range = rangeOf(init);
  try {
    if (range) {
      const { handle, size } = await pool.acquire(path);
      const end = Math.min(range.end, size - 1);
      const length = end - range.start + 1;
      if (length <= 0) return new Response(null, { status: 416 });
      const buffer = new Uint8Array(length);
      await handle.read(buffer, 0, length, range.start);
      stats.fileRangeReads += 1;
      stats.fileBytesRead += length;
      return bufferResponse(buffer, {
        status: 206,
        headers: { "Content-Range": `bytes ${range.start}-${end}/${size}` }
      });
    }
    const buffer = await readFile(path);
    stats.fileFullReads += 1;
    stats.fileBytesRead += buffer.byteLength;
    return bufferResponse(buffer);
  } catch (error) {
    if (error?.code === "ENOENT" || error?.code === "EISDIR") return new Response(null, { status: 404 });
    throw error;
  }
}

// ---------------------------------------------------------------------------
// http(s) transport with browser-equivalent caching.

function isImmutableUrl(url) {
  const pathname = new URL(url).pathname;
  const name = pathname.slice(pathname.lastIndexOf("/") + 1);
  return CONTENT_ADDRESSED_NAME.test(name);
}

function isImmutableResponse(response) {
  const control = response.headers.get("cache-control") || "";
  if (/\bimmutable\b/u.test(control)) return true;
  const maxAge = /max-age=(\d+)/u.exec(control);
  return !!maxAge && Number(maxAge[1]) >= 24 * 60 * 60;
}

class DiskCache {
  constructor(root) {
    this.root = root;
  }

  pathFor(key, suffix) {
    const digest = sha256Hex(key);
    return join(this.root, digest.slice(0, 2), `${digest}.${suffix}`);
  }

  async read(key) {
    try {
      return await readFile(this.pathFor(key, "bin"));
    } catch {
      return null;
    }
  }

  async readMeta(key) {
    try {
      return JSON.parse(await readFile(this.pathFor(key, "json"), "utf8"));
    } catch {
      return null;
    }
  }

  async write(key, buffer, meta) {
    const path = this.pathFor(key, "bin");
    await mkdir(dirname(path), { recursive: true });
    const tmp = `${path}.${process.pid}.${Math.random().toString(36).slice(2)}.tmp`;
    await writeFile(tmp, buffer);
    await rename(tmp, path);
    if (meta) await writeFile(this.pathFor(key, "json"), JSON.stringify(meta));
  }
}

async function httpFetch(context, url, init, stats) {
  const range = rangeOf(init);
  const key = range ? `${url}#${range.start}-${range.end}` : String(url);
  const immutable = isImmutableUrl(url);

  const cached = context.memory.get(key);
  if (cached && (immutable || cached.immutable)) {
    stats.memoryHits += 1;
    return bufferResponse(cached.buffer, range ? { status: 206 } : {});
  }

  if (immutable && context.disk) {
    const buffer = await context.disk.read(key);
    if (buffer) {
      stats.diskHits += 1;
      context.memory.set(key, { buffer, immutable: true });
      return bufferResponse(buffer, range ? { status: 206 } : {});
    }
  }

  // Mutable full-body entries (manifests) revalidate like the browser does.
  const meta = !range && !immutable && context.disk ? await context.disk.readMeta(key) : null;
  const headers = { ...(init?.headers || {}) };
  if (meta?.etag) headers["If-None-Match"] = meta.etag;
  else if (meta?.lastModified) headers["If-Modified-Since"] = meta.lastModified;

  stats.networkRequests += 1;
  const response = await fetch(url, { ...init, headers });

  if (response.status === 304 && meta) {
    const buffer = await context.disk.read(key);
    if (buffer) {
      stats.revalidated304 += 1;
      return bufferResponse(buffer, { headers: meta.headers });
    }
  }
  if (!response.ok && response.status !== 206) return response;

  const buffer = Buffer.from(await response.arrayBuffer());
  stats.networkBytes += buffer.byteLength;
  const responseImmutable = immutable || isImmutableResponse(response);
  if (responseImmutable) {
    context.memory.set(key, { buffer, immutable: true });
    if (context.disk) context.disk.write(key, buffer).catch(() => {});
  } else if (!range && context.disk && (response.headers.get("etag") || response.headers.get("last-modified"))) {
    context.disk.write(key, buffer, {
      etag: response.headers.get("etag") || undefined,
      lastModified: response.headers.get("last-modified") || undefined,
      headers: { "content-type": response.headers.get("content-type") || "application/octet-stream" }
    }).catch(() => {});
  }
  return bufferResponse(buffer, {
    status: response.status,
    headers: { "content-type": response.headers.get("content-type") || "application/octet-stream" }
  });
}

// ---------------------------------------------------------------------------
// Process-wide router installed as the runtime's fetch implementation.

const router = {
  installed: false,
  pool: null,
  memory: null,
  disk: null,
  stats: {
    memoryHits: 0,
    diskHits: 0,
    revalidated304: 0,
    networkRequests: 0,
    networkBytes: 0,
    fileRangeReads: 0,
    fileFullReads: 0,
    fileBytesRead: 0
  }
};

function installRouter(options) {
  if (!router.installed) {
    router.pool = new FileHandlePool(options.fileHandleLimit || DEFAULT_FILE_HANDLE_LIMIT);
    router.memory = new ByteLru(options.memoryCacheBytes || DEFAULT_MEMORY_CACHE_BYTES);
    router.installed = true;
    setFetchImplementation((url, init) => {
      const href = String(url);
      if (href.startsWith("file:")) return fileFetch(router.pool, href, init, router.stats);
      return httpFetch(router, href, init, router.stats);
    }, { multiRange: false });
  }
  if (options.diskCache !== false && !router.disk) {
    router.disk = new DiskCache(options.cacheDir || join(tmpdir(), "rangefind-node-cache"));
  }
  if (options.diskCache === false) router.disk = null;
}

function normalizeSource(source) {
  const text = String(source || "./rangefind/");
  if (/^https?:\/\//u.test(text)) return text.endsWith("/") ? text : `${text}/`;
  if (text.startsWith("file:")) return text.endsWith("/") ? text : `${text}/`;
  const url = pathToFileURL(resolve(text)).href;
  return url.endsWith("/") ? url : `${url}/`;
}

/**
 * Create a Rangefind search engine for Node.
 *
 * @param {object} options
 *   - source: index location — local directory path, file:// URL, or http(s)
 *     base URL (aliases: baseUrl).
 *   - cacheDir: disk cache directory for http(s) sources
 *     (default: <tmpdir>/rangefind-node-cache).
 *   - diskCache: set false to disable the disk cache.
 *   - memoryCacheBytes: memory LRU budget (default 64 MiB, process-wide).
 *   - fileHandleLimit: pooled open handles for file:// sources (default 32).
 *   Remaining options pass through to createSearch (manifestName, trace, ...).
 */
export async function createNodeSearch(options = {}) {
  const { source, baseUrl, cacheDir, diskCache, memoryCacheBytes, fileHandleLimit, ...rest } = options;
  installRouter({ cacheDir, diskCache, memoryCacheBytes, fileHandleLimit });
  const resolved = normalizeSource(source || baseUrl);
  if (resolved.startsWith("file:")) {
    await stat(fileURLToPath(resolved));
  }
  const engine = await createSearch({ ...rest, baseUrl: resolved });
  return {
    ...engine,
    baseUrl: resolved,
    cacheStats: () => ({ ...router.stats, memoryBytes: router.memory.bytes }),
    close: () => router.pool.close()
  };
}

/** Reset the process-wide caches (memory + pooled file handles). */
export async function resetNodeRuntimeCaches({ disk = false } = {}) {
  if (router.memory) router.memory.clear();
  if (router.pool) await router.pool.close();
  for (const key of Object.keys(router.stats)) router.stats[key] = 0;
  if (disk && router.disk) await rm(router.disk.root, { recursive: true, force: true });
}

export default createNodeSearch;
