// Proves the Rangefind index produced by the MkDocs plugin is actually
// searchable through the real range-request runtime (not merely present on
// disk). Serves the built site with a Range-capable static server (mirroring
// the serveStatic helper in the repo's test/build-runtime.test.js), then runs a
// real query and asserts the expected document comes back.
//
// Usage: node verify_search.mjs <site_dir> [outputDir] [query]

import assert from "node:assert/strict";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

// Resolve the runtime from the monorepo checkout (this file lives at
// integrations/mkdocs-rangefind/test/, so the repo root is three levels up).
const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(here, "..", "..", "..");
const { createSearch } = await import(resolve(repoRoot, "src", "runtime.js"));

const siteDir = process.argv[2];
const outputDir = process.argv[3] || "rangefind";
const query = process.argv[4] || "xylophone";
if (!siteDir) {
  console.error("verify_search.mjs: missing <site_dir> argument");
  process.exit(2);
}

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
  await new Promise(res => server.listen(0, "127.0.0.1", res));
  const { port } = server.address();
  return {
    baseUrl: `http://127.0.0.1:${port}/${outputDir}/`,
    close: () => new Promise(res => server.close(res))
  };
}

const server = await serveStatic(siteDir);
try {
  const engine = await createSearch({ baseUrl: server.baseUrl });
  const response = await engine.search({ q: query, size: 5 });
  const results = response.results || [];
  console.log(
    `query=${JSON.stringify(query)} -> ${results.length} result(s):`,
    results.map(r => ({ id: r.id, title: r.title, url: r.url }))
  );
  assert.ok(results.length >= 1, `expected at least one result for "${query}"`);
  const match = results.find(
    r =>
      (r.url && r.url.toLowerCase().includes("about")) ||
      (r.id && r.id.toLowerCase().includes("about")) ||
      (r.title && r.title.toLowerCase().includes("about"))
  );
  assert.ok(
    match,
    `expected the About page in results for "${query}", got ${JSON.stringify(
      results.map(r => r.url || r.id)
    )}`
  );
  console.log(`OK: "${query}" resolved to the About page (${match.url || match.id}).`);
} finally {
  await server.close();
}
