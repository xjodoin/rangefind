// Recording harness for the OSM query layer parity fixtures.
//
// Wraps an engine object in a Proxy that records every engine method call
// (method, params in the Rust `engine.call` shape, JSON response or error)
// and wraps the exported query.js functions so each top-level call records
// its arguments, output and the engine calls it made. The Rust parity test
// replays these logs (tests/osm_parity.rs).

import { AsyncLocalStorage } from "node:async_hooks";

const ENGINE_METHODS = ["search", "suggest", "count", "authorityLookup", "loadFacetValues", "hydrateRows", "prefetchShards", "vectorSearch"];

function snapshot(value) {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value, (key, item) => {
    if (item instanceof Map) return { $map: [...item.entries()] };
    if (item instanceof Set) return { $set: [...item] };
    if (typeof item === "number" && !Number.isFinite(item)) return null;
    return item;
  }) ?? "null");
}

// Arguments: non-finite numbers become their String() form, which every
// Number()/Number.isFinite read in query.js treats identically.
function argSnapshot(value) {
  if (value === undefined) return null;
  return JSON.parse(JSON.stringify(value, (key, item) => {
    if (item instanceof Map) return { $map: [...item.entries()] };
    if (item instanceof Set) return { $set: [...item] };
    if (typeof item === "number" && !Number.isFinite(item)) return String(item);
    return item;
  }) ?? "null");
}

function errorRecord(error) {
  return {
    message: String(error?.message ?? error),
    ...(error?.code ? { code: String(error.code) } : {})
  };
}

// Rust `engine.call(method, params)` shape for each JS method signature.
function callShape(method, args) {
  switch (method) {
    case "authorityLookup": {
      const [surface, params] = args;
      return typeof surface === "string" ? { ...(params || {}), surface } : (surface ?? {});
    }
    case "loadFacetValues":
      return { field: args[0] };
    case "hydrateRows":
      return { rows: args[0], context: args[1] ?? {} };
    case "prefetchShards":
      return { names: args[0], ...(args[1]?.docValues ? { docValues: true } : {}) };
    default:
      return args[0] ?? {};
  }
}

// The manifest fields query.js reads, pruned so fixtures stay small.
export function prunedManifest(manifest) {
  if (!manifest || typeof manifest !== "object") return null;
  const out = {};
  if (manifest.features !== undefined) out.features = manifest.features;
  if (manifest.facets && typeof manifest.facets === "object") {
    out.facets = Object.fromEntries(Object.keys(manifest.facets).map(key => [key, {}]));
  }
  if (Array.isArray(manifest.shards)) {
    out.shards = manifest.shards.map(shard => {
      if (!shard || typeof shard !== "object") return shard;
      const item = {};
      for (const key of ["id", "bbox", "groups", "features"]) {
        if (shard[key] !== undefined) item[key] = shard[key];
      }
      if (shard.facets && typeof shard.facets === "object") {
        item.facets = Object.fromEntries(Object.keys(shard.facets).map(key => [key, {}]));
      }
      return item;
    });
  } else if (manifest.shards !== undefined) {
    out.shards = manifest.shards;
  }
  if (manifest.category_lexicon !== undefined) out.category_lexicon = manifest.category_lexicon;
  return snapshot(out);
}

export function createRecorder() {
  const storage = new AsyncLocalStorage();
  const records = [];
  const engineIds = new WeakMap();
  const proxies = new WeakMap();
  let nextEngine = 0;

  function engineId(engine) {
    if (!engine || (typeof engine !== "object" && typeof engine !== "function")) return null;
    if (!engineIds.has(engine)) engineIds.set(engine, nextEngine++);
    return engineIds.get(engine);
  }

  function wrapEngine(engine) {
    if (!engine || typeof engine !== "object") return engine;
    if (proxies.has(engine)) return proxies.get(engine);
    const proxy = new Proxy(engine, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver);
        if (typeof value !== "function" || !ENGINE_METHODS.includes(property)) return value;
        return function recorded(...args) {
          const record = storage.getStore();
          const entry = { method: property, params: snapshot(callShape(property, args)) };
          if (record) record.calls.push(entry);
          let result;
          try {
            result = value.apply(target, args);
          } catch (error) {
            entry.error = errorRecord(error);
            throw error;
          }
          if (result && typeof result.then === "function") {
            return result.then(response => {
              entry.response = snapshot(response);
              return response;
            }, error => {
              entry.error = errorRecord(error);
              throw error;
            });
          }
          entry.response = snapshot(result);
          return result;
        };
      }
    });
    proxies.set(engine, proxy);
    engineIds.set(proxy, engineId(engine));
    return proxy;
  }

  function engineInfo(engine) {
    if (!engine || typeof engine !== "object") return { id: null, has: [], manifest: null };
    return {
      id: engineId(engine),
      has: ENGINE_METHODS.filter(method => typeof engine[method] === "function"),
      manifest: prunedManifest(engine.manifest)
    };
  }

  // Wrap an engine-taking async function (first argument is the engine).
  function wrapEngineFunction(name, fn, meta = {}) {
    return async function wrapped(engine, ...args) {
      const proxied = wrapEngine(engine);
      const record = {
        fn: name,
        ...meta,
        engine: engineInfo(engine),
        args: args.map(argSnapshot),
        calls: []
      };
      records.push(record);
      return storage.run(record, async () => {
        try {
          const output = await fn(proxied, ...args);
          record.output = snapshot(name === "hydrateOsmSuggestions" ? args[0] : output);
          return output;
        } catch (error) {
          record.error = errorRecord(error);
          throw error;
        }
      });
    };
  }

  function wrapPureFunction(name, fn, meta = {}) {
    return function wrapped(...args) {
      const record = { fn: name, ...meta, args: args.map(argSnapshot) };
      records.push(record);
      try {
        const output = fn(...args);
        record.output = snapshot(output);
        return output;
      } catch (error) {
        record.error = errorRecord(error);
        throw error;
      }
    };
  }

  return { records, wrapEngine, wrapEngineFunction, wrapPureFunction, engineId };
}

export const ENGINE_FUNCTIONS = [
  "reverseGeocodeOsm",
  "resolveOsmSuggestion",
  "hydrateOsmSuggestions",
  "suggestOsmQuery",
  "searchAlongRouteOsm",
  "searchOsmQuery"
];

export const PURE_FUNCTIONS = [
  "parseCoordinateIntent",
  "parseOsmQueryIntent",
  "parseNearbyCategoryIntent",
  "collapseStreetSuggestions"
];
