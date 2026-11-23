// Rangefind WebAssembly engine — JavaScript glue.
//
// Loads the Rust query engine compiled to wasm32-unknown-unknown and
// exposes the same surface as `createSearch()` from `rangefind`:
//
//   import { createWasmSearch } from "rangefind/wasm";
//   const engine = await createWasmSearch({ baseUrl: "/rangefind/" });
//   const response = await engine.search({ q: "static search", size: 10 });
//
// The engine itself never touches the network: it asks for byte ranges and
// this glue performs them with `fetch` (or a caller-supplied transport), so
// the module runs unchanged in browsers, workers, Node, Deno, Bun and
// edge runtimes. Inflation and SHA-256 verification happen inside the
// module, so hosts without DecompressionStream or crypto.subtle work too.
//
// Node-only modules are imported through a computed specifier so browser
// bundlers (esbuild, webpack, Vite) never try to resolve them; they are only
// loaded for `file:` URLs.

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const NODE_FS = "node:fs/promises";
const NODE_URL = "node:url";

function nodeImport(specifier) {
  return import(/* webpackIgnore: true */ /* @vite-ignore */ specifier);
}

let modulePromise = null;

async function readFileUrl(url) {
  const { readFile } = await nodeImport(NODE_FS);
  const { fileURLToPath } = await nodeImport(NODE_URL);
  return readFile(fileURLToPath(url));
}

async function wasmBytesFrom(source) {
  const url = source instanceof URL ? source : new URL(String(source), globalThis.location?.href || import.meta.url);
  if (url.protocol === "file:") return readFileUrl(url);
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Rangefind could not load ${url} (HTTP ${response.status})`);
  return response.arrayBuffer();
}

/**
 * Platform services the module cannot provide itself on
 * wasm32-unknown-unknown: a clock and IANA time zones (opening-hours
 * evaluation for "open now" constraints). Using `Intl` here keeps zone
 * handling identical to the JavaScript OSM layer.
 */
function hostImports() {
  let exports = null;
  const formats = new Map();
  function format(zone) {
    let entry = formats.get(zone);
    if (entry === undefined) {
      try {
        entry = new Intl.DateTimeFormat("en-US", {
          timeZone: zone || undefined,
          hourCycle: "h23",
          year: "numeric",
          month: "numeric",
          day: "numeric",
          hour: "numeric",
          minute: "numeric",
          second: "numeric",
          era: "short"
        });
      } catch {
        entry = null;
      }
      formats.set(zone, entry);
    }
    return entry;
  }
  return {
    bind(instanceExports) {
      exports = instanceExports;
    },
    imports: {
      host_now_ms: () => Date.now(),
      host_zone_offset_minutes(ptr, len, atMs) {
        const zone = len ? decoder.decode(new Uint8Array(exports.memory.buffer, ptr >>> 0, len >>> 0)) : "";
        const formatter = format(zone);
        if (!formatter || !Number.isFinite(atMs)) return NaN;
        const parts = {};
        for (const part of formatter.formatToParts(new Date(atMs))) parts[part.type] = part.value;
        const year = parts.era === "BC" || parts.era === "B" ? 1 - Number(parts.year) : Number(parts.year);
        const utc = new Date(0);
        utc.setUTCFullYear(year, Number(parts.month) - 1, Number(parts.day));
        utc.setUTCHours(Number(parts.hour), Number(parts.minute), Number(parts.second), 0);
        // Whole-second instant, so sub-second `atMs` does not skew the offset.
        return (utc.getTime() - Math.floor(atMs / 1000) * 1000) / 60000;
      }
    }
  };
}

/** Compile (once) and instantiate the engine module. */
export async function loadRangefindWasm(source) {
  if (!source && modulePromise) return modulePromise;
  const promise = (async () => {
    let bytes = source || new URL("./rangefind_wasm.wasm", import.meta.url);
    if (bytes instanceof URL || typeof bytes === "string") bytes = await wasmBytesFrom(bytes);
    const module = bytes instanceof WebAssembly.Module ? bytes : await WebAssembly.compile(bytes);
    const host = hostImports();
    const instance = await WebAssembly.instantiate(module, { rangefind: host.imports });
    host.bind(instance.exports);
    return instance.exports;
  })();
  if (!source) {
    modulePromise = promise;
    // A transient failure must not poison every later load.
    promise.catch(() => {
      if (modulePromise === promise) modulePromise = null;
    });
  }
  return promise;
}

function writeBytes(exports, bytes) {
  if (!bytes.length) return [0, 0];
  // Pointers above 2 GiB arrive as negative i32: read them unsigned.
  const ptr = exports.rf_alloc(bytes.length) >>> 0;
  // Create the view after rf_alloc: allocation can grow (and detach) memory.
  new Uint8Array(exports.memory.buffer, ptr, bytes.length).set(bytes);
  return [ptr, bytes.length];
}

/** JSON for the engine: typed arrays (e.g. Float32Array vectors) become arrays. */
function engineJson(value) {
  return JSON.stringify(value ?? {}, (key, item) => (ArrayBuffer.isView(item) && !(item instanceof DataView) ? Array.from(item) : item));
}

// ------------------------------------------------------------------ file://

const FILE_HANDLE_LIMIT = 32;

/**
 * A `file:` fetch with its own bounded pool of open handles. Each engine owns
 * one, so `close()` releases its descriptors and a rebuilt index is reopened
 * from disk. Handles are closed only after their in-flight reads finish.
 */
function createFileFetch() {
  // path -> { handle: Promise<FileHandle>, users, evicted }
  const handles = new Map();
  let closed = false;
  let fsPromises = null;
  let urlModule = null;

  const closeEntry = entry => entry.handle.then(handle => handle.close(), () => {});
  const release = entry => {
    entry.users--;
    if (entry.evicted && entry.users === 0) closeEntry(entry);
  };
  // Synchronous registration: concurrent reads of one file must share a
  // single open() or the losing handle leaks until GC.
  const acquire = path => {
    let entry = handles.get(path);
    if (entry) {
      handles.delete(path);
    } else {
      fsPromises ||= nodeImport(NODE_FS);
      entry = { handle: fsPromises.then(({ open }) => open(path, "r")), users: 0, evicted: false };
      entry.handle.catch(() => {
        if (handles.get(path) === entry) handles.delete(path);
      });
    }
    handles.set(path, entry);
    entry.users++;
    while (handles.size > FILE_HANDLE_LIMIT) {
      const [oldPath, oldest] = handles.entries().next().value;
      handles.delete(oldPath);
      oldest.evicted = true;
      if (oldest.users === 0) closeEntry(oldest);
    }
    return entry;
  };

  async function fileFetch(url, init = {}) {
    if (closed) throw new Error("Rangefind engine is closed.");
    urlModule ||= nodeImport(NODE_URL);
    const path = (await urlModule).fileURLToPath(url);
    const entry = acquire(path);
    try {
      let handle;
      try {
        handle = await entry.handle;
      } catch (error) {
        if (error?.code === "ENOENT") return { status: 404, ok: false, headers: new Map() };
        throw error;
      }
      const range = /^bytes=(\d+)-(\d+)$/u.exec(init.headers?.Range || "");
      if (range) {
        const start = Number(range[1]);
        const { size } = await handle.stat();
        if (start >= size) return { status: 416, ok: false, headers: new Map() };
        const length = Math.min(Number(range[2]) - start + 1, size - start);
        const buffer = new Uint8Array(length);
        const { bytesRead } = await handle.read(buffer, 0, length, start);
        return { status: 206, ok: true, headers: new Map(), arrayBuffer: async () => buffer.subarray(0, bytesRead) };
      }
      const { size } = await handle.stat();
      const data = new Uint8Array(size);
      const { bytesRead } = await handle.read(data, 0, size, 0);
      return { status: 200, ok: true, headers: new Map(), arrayBuffer: async () => data.subarray(0, bytesRead) };
    } finally {
      release(entry);
    }
  }

  fileFetch.close = () => {
    closed = true;
    for (const entry of handles.values()) {
      entry.evicted = true;
      if (entry.users === 0) closeEntry(entry);
    }
    handles.clear();
  };
  return fileFetch;
}

function headerValue(headers, name) {
  return headers?.get?.(name) ?? headers?.get?.(name.toLowerCase()) ?? null;
}

async function cancelBody(response) {
  try {
    await response.body?.cancel?.();
  } catch {
    // Best effort; some transports have no body stream.
  }
}

/**
 * Byte transport over a fetch-compatible function. Each range is one
 * `Range: bytes=a-b` request. Like the JS runtime, a server that ignores
 * Range (HTTP 200) is salvaged by slicing only when the whole object is
 * small (≤ max(4× the range, 1 MiB)); larger bodies are cancelled unread and
 * the request retried, so a multi-hundred-MB file is never pulled down for
 * one range.
 */
export function fetchTransport(fetchFn) {
  const fileFetch = fetchFn ? null : createFileFetch();
  const doFetch = fetchFn || ((url, init) => (String(url).startsWith("file:") ? fileFetch(url, init) : fetch(url, init)));

  async function range(url, offset, length) {
    for (let attempt = 0; ; attempt++) {
      const response = await doFetch(url, { headers: { Range: `bytes=${offset}-${offset + length - 1}` } });
      if (response.status === 206) {
        const contentRange = headerValue(response.headers, "Content-Range");
        const match = contentRange && /^bytes\s+(\d+)-/iu.exec(contentRange);
        if (match && Number(match[1]) !== offset) throw new Error(`Range request failed for ${url}: server returned ${contentRange}`);
        return new Uint8Array(await response.arrayBuffer());
      }
      if (response.status === 200) {
        const total = Number(headerValue(response.headers, "Content-Length") || 0);
        if (total > 0 && total <= Math.max(length * 4, 1 << 20)) {
          const body = new Uint8Array(await response.arrayBuffer());
          return body.subarray(offset, offset + length);
        }
        await cancelBody(response);
        if (attempt < 2) {
          await new Promise(done => setTimeout(done, 150 * (attempt + 1)));
          continue;
        }
      }
      throw new Error(`Range request failed for ${url}`);
    }
  }

  async function transport(url, ranges) {
    if (!ranges.length) {
      const response = await doFetch(url);
      if (response.status === 404 || response.status === 410) return null;
      if (!response.ok) throw new Error(`Unable to fetch ${url}`);
      return [new Uint8Array(await response.arrayBuffer())];
    }
    return Promise.all(ranges.map(([offset, length]) => range(url, offset, length)));
  }
  transport.close = () => fileFetch?.close();
  return transport;
}

// ------------------------------------------------------------------ session

class Session {
  constructor(exports, transport) {
    this.exports = exports;
    this.transport = transport;
    this.id = exports.rf_session_new();
    this.waiters = new Map();
    this.pumping = false;
    this.freed = false;
    this.closing = false;
    // Strong reference to the public handle while work is pending, so a
    // FinalizationRegistry cannot free the session under an in-flight query.
    this.owner = null;
  }

  start(op) {
    if (this.freed) return Promise.reject(new Error("Rangefind engine is closed."));
    return new Promise((resolve, reject) => {
      this.waiters.set(op, { resolve, reject });
      this.pump();
    });
  }

  poll() {
    const { exports } = this;
    const ptr = exports.rf_poll(this.id) >>> 0;
    const len = exports.rf_result_len(this.id) >>> 0;
    if (!ptr && !len) throw new Error("Rangefind session is no longer available.");
    return JSON.parse(decoder.decode(new Uint8Array(exports.memory.buffer, ptr, len)));
  }

  pump() {
    if (this.pumping || this.freed) return;
    this.pumping = true;
    try {
      const events = this.poll();
      for (const item of events.done) {
        const waiter = this.waiters.get(item.op);
        if (!waiter) continue;
        this.waiters.delete(item.op);
        if (item.ok) waiter.resolve(item.value);
        else {
          const error = new Error(item.error);
          if (item.code) error.code = item.code;
          waiter.reject(error);
        }
      }
      for (const request of events.requests) this.perform(request);
    } catch (error) {
      this.fail(error);
    } finally {
      this.pumping = false;
    }
    if (!this.waiters.size) {
      this.owner = null;
      if (this.closing) this.free();
    }
  }

  /** Reject every pending operation (the module trapped or the session died). */
  fail(error) {
    for (const waiter of this.waiters.values()) waiter.reject(error);
    this.waiters.clear();
    this.free();
  }

  free() {
    if (this.freed) return;
    this.freed = true;
    this.owner = null;
    for (const waiter of this.waiters.values()) waiter.reject(new Error("Rangefind engine is closed."));
    this.waiters.clear();
    try {
      this.exports.rf_session_free(this.id);
    } catch {
      // The instance may already have trapped; nothing left to release.
    }
    this.transport.close?.();
  }

  /** Close once in-flight operations settle (they still complete normally). */
  close() {
    this.closing = true;
    if (!this.waiters.size) this.free();
  }

  complete(request, parts) {
    const { exports } = this;
    if (parts == null) {
      exports.rf_complete_missing(this.id, request.id);
      return;
    }
    if (!Array.isArray(parts) || !parts.every(part => part instanceof Uint8Array)) {
      throw new TypeError("Rangefind transport must resolve to an array of Uint8Array (or null when missing).");
    }
    const expected = request.ranges.length || 1;
    if (parts.length !== expected) throw new Error(`Rangefind transport returned ${parts.length} parts for ${expected} ranges of ${request.url}.`);
    const total = parts.reduce((sum, part) => sum + part.length, 0);
    const data = new Uint8Array(total);
    let at = 0;
    for (const part of parts) {
      data.set(part, at);
      at += part.length;
    }
    const lengths = new Uint32Array(parts.map(part => part.length));
    const [dataPtr, dataLen] = writeBytes(exports, data);
    const [lenPtr] = writeBytes(exports, new Uint8Array(lengths.buffer));
    try {
      exports.rf_complete(this.id, request.id, dataPtr, dataLen, lenPtr, request.ranges.length ? parts.length : 0);
    } finally {
      if (dataLen) exports.rf_free(dataPtr, dataLen);
      if (lenPtr) exports.rf_free(lenPtr, lengths.byteLength);
    }
  }

  completeError(request, error) {
    const [ptr, len] = writeBytes(this.exports, encoder.encode(String(error?.message || error)));
    try {
      this.exports.rf_complete_error(this.id, request.id, ptr, len);
    } finally {
      if (len) this.exports.rf_free(ptr, len);
    }
  }

  perform(request) {
    Promise.resolve()
      .then(() => this.transport(request.url, request.ranges))
      .then(parts => {
        if (this.freed) return;
        try {
          this.complete(request, parts);
        } catch (error) {
          // Bad transport output or an engine trap: fail this request so the
          // operation settles instead of hanging.
          this.completeError(request, error);
        }
      }, error => {
        if (!this.freed) this.completeError(request, error);
      })
      .catch(error => this.fail(error))
      .finally(() => this.pump());
  }

  invoke(start) {
    if (this.freed || this.closing) return Promise.reject(new Error("Rangefind engine is closed."));
    let op;
    try {
      op = start();
    } catch (error) {
      return Promise.reject(error);
    }
    return this.start(op);
  }

  open(baseUrl, options) {
    return this.invoke(() => {
      const { exports } = this;
      const optionsJson = encoder.encode(engineJson(options));
      const [urlPtr, urlLen] = writeBytes(exports, encoder.encode(baseUrl));
      const [optPtr, optLen] = writeBytes(exports, optionsJson);
      try {
        return exports.rf_open(this.id, urlPtr, urlLen, optPtr, optLen);
      } finally {
        if (urlLen) exports.rf_free(urlPtr, urlLen);
        if (optLen) exports.rf_free(optPtr, optLen);
      }
    });
  }

  call(engine, method, params) {
    return this.invoke(() => {
      const { exports } = this;
      // Serialize before allocating so a non-JSON value cannot leak memory.
      const paramsJson = encoder.encode(engineJson(params));
      const [methodPtr, methodLen] = writeBytes(exports, encoder.encode(method));
      const [paramPtr, paramLen] = writeBytes(exports, paramsJson);
      try {
        return exports.rf_call(this.id, engine, methodPtr, methodLen, paramPtr, paramLen);
      } finally {
        if (methodLen) exports.rf_free(methodPtr, methodLen);
        if (paramLen) exports.rf_free(paramPtr, paramLen);
      }
    });
  }
}

function absoluteBase(baseUrl) {
  const base = baseUrl || "./rangefind/";
  try {
    return new URL(base).href;
  } catch {
    const origin = globalThis.location?.href;
    return origin ? new URL(base, origin).href : base;
  }
}

// Engines dropped without close() still release their wasm memory. The
// registry holds the session (not the handle); in-flight work keeps the
// handle reachable through `session.owner`, so collection only happens when
// the engine is idle.
const sessionRegistry = typeof FinalizationRegistry === "function"
  ? new FinalizationRegistry(session => session.free())
  : null;

/**
 * Open a Rangefind index with the wasm engine. Options mirror
 * `createSearch()`; additionally `wasm` (bytes, URL or WebAssembly.Module),
 * `fetch` (a fetch-compatible function) or `transport`
 * (`(url, ranges) => Promise<Uint8Array[] | null>`).
 */
export async function createWasmSearch(options = {}) {
  const { wasm, fetch: fetchFn, transport, baseUrl, ...engineOptions } = options;
  const exports = await loadRangefindWasm(wasm);
  const session = new Session(exports, transport || fetchTransport(fetchFn));
  let opened;
  try {
    opened = await session.open(absoluteBase(baseUrl), engineOptions);
  } catch (error) {
    session.free();
    throw error;
  }
  const engineId = opened.engine;
  let closed = false;
  let handle = null;
  const call = (method, params) => {
    if (closed) return Promise.reject(new Error("Rangefind engine is closed."));
    session.owner = handle;
    return session.call(engineId, method, params);
  };
  handle = {
    summary: opened.summary,
    search: params => call("search", params),
    count: params => call("count", params),
    suggest: params => call("suggest", params),
    vectorSearch: params => call("vectorSearch", params),
    authorityLookup: (surface, params = {}) => call("authorityLookup", typeof surface === "string" ? { ...params, surface } : surface),
    searchAddressAuthority: params => call("searchAddressAuthority", params),
    loadFacetValues: field => call("loadFacetValues", { field }),
    loadDocValues: (fields, indexes) => call("loadDocValues", { fields, indexes }),
    hydrateRows: (rows, context) => call("hydrateRows", { rows, context }),
    stats: () => call("stats", {}),
    /** OSM query layer (`rangefind/osm` functions, bound to this engine). */
    osm: {
      search: params => call("osm.search", params),
      suggest: params => call("osm.suggest", params),
      reverseGeocode: params => call("osm.reverseGeocode", params),
      searchAlongRoute: params => call("osm.searchAlongRoute", params),
      resolveSuggestion: suggestion => call("osm.resolveSuggestion", suggestion),
      /** Like the JS function, fills `result` on the given suggestion objects in place. */
      hydrateSuggestions: async suggestions => {
        const hydrated = await call("osm.hydrateSuggestions", { suggestions });
        if (Array.isArray(suggestions) && Array.isArray(hydrated)) {
          hydrated.forEach((item, index) => {
            if (suggestions[index] && item && "result" in item) suggestions[index].result = item.result;
          });
        }
        return suggestions;
      },
      collapseStreetSuggestions: (response, params = {}) => call("osm.collapseStreetSuggestions", { response, params }),
      /** `lexicon: "default"` uses the built-in lexicon instead of the index's own (JS default argument). */
      parseQueryIntent: (q, options = {}) => call("osm.parseQueryIntent", { q, ...options }),
      parseNearbyCategoryIntent: (q, options = {}) => call("osm.parseNearbyCategoryIntent", { q, ...options }),
      parseCoordinateIntent: q => call("osm.parseCoordinateIntent", { q }),
      /** Google Maps-style adapter (`createRangefindMapsAdapter`). */
      maps: {
        configure: defaults => call("osm.maps.configure", { defaults }),
        autocomplete: request => call("osm.maps.autocomplete", request),
        textSearch: request => call("osm.maps.textSearch", request),
        nearbySearch: request => call("osm.maps.nearbySearch", request),
        geocode: request => call("osm.maps.geocode", request),
        reverseGeocode: request => call("osm.maps.reverseGeocode", request),
        searchAlongRoute: request => call("osm.maps.searchAlongRoute", request),
        placeDetails: request => call("osm.maps.placeDetails", request),
        clearPlaceCache: () => call("osm.maps.clearPlaceCache", {})
      }
    },
    /** Release the engine, its wasm memory and its file handles. In-flight calls still settle. */
    close: () => {
      if (closed) return;
      closed = true;
      sessionRegistry?.unregister(session);
      session.close();
    }
  };
  sessionRegistry?.register(handle, session, session);
  return handle;
}

export default createWasmSearch;
