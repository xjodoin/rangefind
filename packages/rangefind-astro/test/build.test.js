// Real end-to-end test: spawn the local Astro CLI to build the fixture site,
// then assert the Rangefind integration produced a searchable index and copied
// the client assets. Finally, load the built index with Rangefind's own runtime
// over a tiny Range-capable static server and run a real query.

import assert from "node:assert/strict";
import test from "node:test";
import { execFileSync } from "node:child_process";
import { createServer } from "node:http";
import { existsSync } from "node:fs";
import { readFile, rm, stat } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";
import { createSearch } from "rangefind";

const testDir = dirname(fileURLToPath(import.meta.url));
const pkgDir = resolve(testDir, "..");
const fixtureDir = join(testDir, "fixture");
const distDir = join(fixtureDir, "dist");
// npm workspaces hoist binaries to the repo root's node_modules/.bin; walk
// up from the package so the test works standalone and in the workspace.
const astroBin = (() => {
  let dir = pkgDir;
  for (;;) {
    const candidate = join(dir, "node_modules", ".bin", "astro");
    if (existsSync(candidate)) return candidate;
    const parent = dirname(dir);
    if (parent === dir) throw new Error(`astro binary not found above ${pkgDir}`);
    dir = parent;
  }
})();

// Range-capable static file server (mirrors test/build-runtime.test.js).
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
    origin: `http://127.0.0.1:${port}`,
    close: () => new Promise(res => server.close(res))
  };
}

async function exists(path) {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

test("astro build produces a searchable Rangefind index and client assets", async () => {
  await rm(distDir, { recursive: true, force: true });

  // 1. Real Astro build via the local CLI.
  execFileSync(astroBin, ["build"], {
    cwd: fixtureDir,
    stdio: "inherit",
    timeout: 120000
  });

  // 2. Index manifest exists, is valid JSON, and covers both pages.
  const manifestPath = join(distDir, "rangefind", "manifest.min.json");
  assert.ok(await exists(manifestPath), "manifest.min.json should exist");
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  assert.ok(manifest.total >= 2, `expected total >= 2, got ${manifest.total}`);

  // 3. Client assets were copied.
  assert.ok(
    await exists(join(distDir, "_rangefind", "rangefind-search.js")),
    "rangefind-search.js should exist"
  );
  assert.ok(
    await exists(join(distDir, "_rangefind", "rangefind-search.css")),
    "rangefind-search.css should exist"
  );

  // 4. Query the built index through the real runtime over Range requests.
  const server = await serveStatic(distDir);
  try {
    const engine = await createSearch({ baseUrl: `${server.origin}/rangefind/` });
    const response = await engine.search({ q: "xylophone" });
    assert.ok(response.results.length >= 1, "xylophone should return a result");
    const urls = response.results.map(r => r.url);
    assert.ok(
      urls.some(u => String(u).includes("/about")),
      `expected the About page, got ${JSON.stringify(urls)}`
    );
  } finally {
    await server.close();
  }
});
