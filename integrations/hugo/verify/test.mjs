// End-to-end check for the Hugo recipe.
//
// Assumes the fixture has already been built:
//   hugo                                  # -> public/
//   node ../../../bin/rangefind.js build public   # -> public/rangefind/
//
// This test serves public/ with a Range-capable static server (mirroring the
// serveStatic helper in test/build-runtime.test.js), loads the index through
// the real runtime, and asserts the rare word "xylophone" returns the second
// post and only the second post.

import assert from "node:assert/strict";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { createSearch } from "../../../src/runtime.js";

const here = dirname(fileURLToPath(import.meta.url));
const publicDir = resolve(here, "public");

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
  await new Promise(r => server.listen(0, "127.0.0.1", r));
  const { port } = server.address();
  return {
    baseUrl: `http://127.0.0.1:${port}/rangefind/`,
    close: () => new Promise(r => server.close(r))
  };
}

test("Hugo-built site is searchable through the Rangefind runtime", async () => {
  const manifest = JSON.parse(
    await readFile(resolve(publicDir, "rangefind", "manifest.min.json"), "utf8")
  );
  assert.ok(manifest.total >= 2, `expected total >= 2, got ${manifest.total}`);

  const server = await serveStatic(publicDir);
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });
    const { results } = await engine.search({ q: "xylophone" });
    assert.equal(results.length, 1, "xylophone should match exactly one page");
    assert.match(results[0].url, /second/, `expected the second post, got ${results[0].url}`);
  } finally {
    await server.close();
  }
});
