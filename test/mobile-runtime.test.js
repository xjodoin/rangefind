import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, open, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { gunzipSync } from "node:zlib";
import { build } from "../src/builder.js";
import { createMobileSearch, resetMobileRuntimeCaches } from "../src/runtime.mobile.js";

// Simulate an embedded JS host (React Native/Hermes, QuickJS): no
// DecompressionStream, so every inflate must go through the injected
// implementation. This file runs in its own process, so deleting the global
// cannot leak into other tests.
delete globalThis.DecompressionStream;

const inflate = compressed => gunzipSync(Buffer.from(compressed));

// Positional-read io adapter, standing in for react-native-fs /
// expo-file-system on device.
const io = {
  async read(path, offset, length) {
    const handle = await open(path, "r");
    try {
      const buffer = new Uint8Array(length);
      const { bytesRead } = await handle.read(buffer, 0, length, offset);
      return buffer.subarray(0, bytesRead);
    } finally {
      await handle.close();
    }
  },
  async size(path) {
    return (await stat(path)).size;
  }
};

async function buildFixture() {
  const root = await mkdtemp(join(tmpdir(), "rangefind-mobile-runtime-"));
  const docs = [
    { id: "a", title: "Range request search", body: "Static search over range requests without a server.", url: "/a" },
    { id: "b", title: "SQLite retrieval baseline", body: "A server-side SQLite benchmark compares retrieval quality.", url: "/b" },
    { id: "c", title: "Sparse inverted index", body: "Sparse posting segments packed into immutable static files.", url: "/c" }
  ];
  await writeFile(join(root, "docs.jsonl"), docs.map(doc => JSON.stringify(doc)).join("\n"));
  const configPath = join(root, "rangefind.config.json");
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    fields: [
      { name: "title", path: "title", weight: 4 },
      { name: "body", path: "body", weight: 1 }
    ],
    display: ["title", "url"]
  }));
  await build({ configPath });
  return { root, indexDir: join(root, "public", "rangefind") };
}

function serveStatic(root, counters) {
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://localhost");
      counters.requests += 1;
      const path = resolve(root, `.${decodeURIComponent(url.pathname)}`);
      if (!path.startsWith(resolve(root))) return void response.writeHead(403).end();
      const data = await readFile(path);
      const range = request.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (range) {
        const start = Number(range[1]);
        const end = Math.min(Number(range[2]), data.length - 1);
        response.writeHead(206, { "Content-Range": `bytes ${start}-${end}/${data.length}` });
        return void response.end(data.subarray(start, end + 1));
      }
      response.writeHead(200);
      response.end(data);
    } catch {
      response.writeHead(404).end();
    }
  });
  return new Promise(resolveListen => server.listen(0, "127.0.0.1", () => resolveListen({
    url: `http://127.0.0.1:${server.address().port}/rangefind/`,
    close: () => new Promise(done => server.close(done))
  })));
}

test("mobile runtime searches a local index offline via positional reads", async () => {
  resetMobileRuntimeCaches();
  const { indexDir } = await buildFixture();
  const engine = await createMobileSearch({ source: indexDir, io, inflate });
  const response = await engine.search({ q: "sqlite retrieval" });
  assert.equal(response.results[0]?.title, "SQLite retrieval baseline");
  const stats = engine.cacheStats();
  assert.ok(stats.fileRangeReads > 0, "postings should come from positional range reads");
  assert.ok(stats.fileBytesRead > 0);
  assert.equal(stats.networkRequests, 0);
});

test("mobile runtime requires an inflate implementation on hosts without DecompressionStream", async () => {
  const { indexDir } = await buildFixture();
  await assert.rejects(
    createMobileSearch({ source: indexDir, io }),
    /DecompressionStream/u
  );
});

test("mobile runtime caches immutable http objects in memory and the persistent adapter", async () => {
  resetMobileRuntimeCaches();
  const { root } = await buildFixture();
  const counters = { requests: 0 };
  const server = await serveStatic(join(root, "public"), counters);
  // Map-backed stand-in for a device persistent cache (files, AsyncStorage, ...).
  const persisted = new Map();
  const cache = {
    get: key => persisted.get(key) || null,
    set: (key, bytes) => void persisted.set(key, bytes)
  };

  const first = await createMobileSearch({ source: server.url, inflate, cache, fetch: (url, init) => fetch(url, init) });
  const warm = await first.search({ q: "sparse inverted" });
  assert.equal(warm.results[0]?.title, "Sparse inverted index");
  assert.ok(persisted.size > 0, "immutable objects should be written to the persistent cache");

  // Same process, new engine: immutable objects come from the memory LRU.
  const sibling = await createMobileSearch({ source: server.url, inflate, cache });
  await sibling.search({ q: "sparse inverted" });
  assert.ok(sibling.cacheStats().memoryHits > 0, "second engine should hit the memory cache");

  // Fresh process simulation: memory cleared, persistent adapter still warm.
  resetMobileRuntimeCaches();
  const requestsBefore = counters.requests;
  const second = await createMobileSearch({ source: server.url, inflate, cache });
  const cold = await second.search({ q: "sparse inverted" });
  assert.equal(cold.results[0]?.title, "Sparse inverted index");
  const stats = second.cacheStats();
  assert.ok(stats.persistentHits > 0, "content-addressed objects should come from the persistent cache");
  assert.ok(counters.requests - requestsBefore < requestsBefore, "persistent cache should absorb most requests");

  await server.close();
});
