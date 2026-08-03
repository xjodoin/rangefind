// Node adapter for rfroutegraph-v1 queries: positional file reads over a
// local index directory plus gzip inflation, with request/byte counters so
// benchmarks can report exactly what a browser would have fetched.

import { openSync, readSync, statSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import { openRouteGraph } from "./route_graph_query.js";

export function createRouteGraphFileIo(dir) {
  const handles = new Map();
  const counters = { requests: 0, bytes: 0, files: new Set() };
  const handleFor = (path) => {
    let handle = handles.get(path);
    if (handle == null) {
      handle = openSync(join(dir, path), "r");
      handles.set(path, handle);
    }
    return handle;
  };
  return {
    async readFile(path) {
      counters.requests++;
      counters.files.add(path);
      const bytes = await readFile(join(dir, path));
      counters.bytes += bytes.length;
      return new Uint8Array(bytes);
    },
    async readRange(path, offset, length) {
      counters.requests++;
      counters.files.add(path);
      counters.bytes += length;
      const buffer = Buffer.alloc(length);
      const read = readSync(handleFor(path), buffer, 0, length, offset);
      if (read !== length) throw new Error(`Short read from ${path}: ${read} of ${length} bytes.`);
      return new Uint8Array(buffer);
    },
    size(path) {
      return statSync(join(dir, path)).size;
    },
    counters,
    resetCounters() {
      counters.requests = 0;
      counters.bytes = 0;
      counters.files.clear();
    }
  };
}

export async function openRouteGraphDir(dir, options = {}) {
  const io = options.io || createRouteGraphFileIo(dir);
  const engine = await openRouteGraph({
    io,
    inflate: bytes => gunzipSync(Buffer.from(bytes)),
    ...options
  });
  engine.io = io;
  return engine;
}
