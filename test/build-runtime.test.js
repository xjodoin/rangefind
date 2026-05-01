import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";

async function serveStatic(root) {
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://localhost");
      const path = resolve(root, `.${decodeURIComponent(url.pathname)}`);
      if (!path.startsWith(resolve(root))) {
        response.writeHead(403).end();
        return;
      }
      const data = await readFile(path);
      const range = request.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (range) {
        const start = Number(range[1]);
        const end = Math.min(Number(range[2]), data.length - 1);
        response.writeHead(206, {
          "Accept-Ranges": "bytes",
          "Content-Length": String(end - start + 1),
          "Content-Range": `bytes ${start}-${end}/${data.length}`
        });
        response.end(data.subarray(start, end + 1));
        return;
      }
      response.writeHead(200, { "Content-Length": String(data.length) });
      response.end(data);
    } catch {
      response.writeHead(404).end();
    }
  });
  await new Promise(resolveListen => server.listen(0, "127.0.0.1", resolveListen));
  const { port } = server.address();
  return {
    baseUrl: `http://127.0.0.1:${port}/rangefind/`,
    close: () => new Promise(resolveClose => server.close(resolveClose))
  };
}

test("builder output is searchable through the range-based runtime", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-build-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, [
    JSON.stringify({ id: "a", title: "Static range search", body: "Rangefind builds a static index with range requests.", category: "indexing", year: 2026, url: "/a" }),
    JSON.stringify({ id: "b", title: "SQLite retrieval baseline", body: "A server-side SQLite benchmark compares retrieval quality.", category: "baseline", year: 2025, url: "/b" }),
    JSON.stringify({ id: "c", title: "Client search runtime", body: "The runtime fetches packed term shards lazily.", category: "runtime", year: 2026, url: "/c" })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    docChunkSize: 2,
    baseShardDepth: 2,
    maxShardDepth: 3,
    targetShardPostings: 2,
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: [{ name: "category", path: "category" }],
    numbers: [{ name: "year", path: "year" }],
    display: ["title", "url", "category", "year"]
  }));

  await build({ configPath });
  assert.ok(await readFile(join(output, "manifest.json"), "utf8"));
  assert.ok(await readFile(join(output, "terms", "ranges.bin.gz")));
  assert.ok(await readFile(join(output, "codes.bin.gz")));
  assert.ok(await readFile(join(output, "typo", "manifest.json")));

  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });

  const results = await search.search({ q: "static range search", size: 3 });
  assert.equal(results.results[0].title, "Static range search");
  assert.ok(results.stats.shards > 0);

  const typo = await search.search({ q: "statik range search", size: 3 });
  assert.equal(typo.results[0].title, "Static range search");
  assert.equal(typo.correctedQuery, "static range search");
  assert.deepEqual(typo.corrections.map(item => item.to), ["static"]);
  assert.equal(typo.stats.typoApplied, true);

  const filtered = await search.search({
    q: "search",
    filters: {
      facets: { category: ["indexing"] },
      numbers: { year: { min: 2026 } }
    }
  });
  assert.deepEqual(filtered.results.map(result => result.id), ["a"]);
});
