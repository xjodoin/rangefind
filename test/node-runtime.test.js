import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createNodeSearch, resetNodeRuntimeCaches } from "../src/runtime.node.js";

async function buildFixture() {
  const root = await mkdtemp(join(tmpdir(), "rangefind-node-runtime-"));
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
      const etag = `"${data.length}"`;
      if (request.headers["if-none-match"] === etag) {
        counters.notModified += 1;
        return void response.writeHead(304).end();
      }
      const range = request.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (range) {
        const start = Number(range[1]);
        const end = Math.min(Number(range[2]), data.length - 1);
        response.writeHead(206, { "Content-Range": `bytes ${start}-${end}/${data.length}` });
        return void response.end(data.subarray(start, end + 1));
      }
      response.writeHead(200, { ETag: etag });
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

test("node runtime searches a local index directory via positional reads", async () => {
  await resetNodeRuntimeCaches();
  const { indexDir } = await buildFixture();
  const engine = await createNodeSearch({ source: indexDir });
  const response = await engine.search({ q: "sqlite retrieval" });
  assert.equal(response.results[0]?.title, "SQLite retrieval baseline");
  const stats = engine.cacheStats();
  assert.ok(stats.fileBytesRead > 0);
  assert.equal(stats.networkRequests, 0);
  await engine.close();
});

test("node runtime caches immutable http objects and revalidates the manifest", async () => {
  await resetNodeRuntimeCaches();
  const { root } = await buildFixture();
  const counters = { requests: 0, notModified: 0 };
  const server = await serveStatic(join(root, "public"), counters);
  const cacheDir = await mkdtemp(join(tmpdir(), "rangefind-node-cache-"));

  const first = await createNodeSearch({ source: server.url, cacheDir });
  const warm = await first.search({ q: "sparse inverted" });
  assert.equal(warm.results[0]?.title, "Sparse inverted index");
  // Same process, new engine: immutable objects come from the memory LRU.
  const sibling = await createNodeSearch({ source: server.url, cacheDir });
  await sibling.search({ q: "sparse inverted" });
  assert.ok(sibling.cacheStats().memoryHits > 0, "second engine should hit the memory cache");

  // Fresh process simulation: memory cleared, disk cache still valid.
  await resetNodeRuntimeCaches();
  const second = await createNodeSearch({ source: server.url, cacheDir });
  const cold = await second.search({ q: "sparse inverted" });
  assert.equal(cold.results[0]?.title, "Sparse inverted index");
  const stats = second.cacheStats();
  assert.ok(stats.diskHits > 0, "content-addressed objects should come from the disk cache");
  assert.ok(counters.notModified > 0, "manifest should revalidate with 304");

  await server.close();
  await second.close();
});
