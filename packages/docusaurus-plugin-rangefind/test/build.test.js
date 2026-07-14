"use strict";

// Real end-to-end test: run the actual @docusaurus/core build CLI over a
// minimal fixture site that registers docusaurus-plugin-rangefind, then assert
// the index, the copied client assets, the injected script tag, and a working
// range-based query over the built output.

const assert = require("node:assert/strict");
const test = require("node:test");
const fs = require("node:fs");
const { createServer } = require("node:http");
const { execFileSync } = require("node:child_process");
const path = require("node:path");
const { readFile, readdir, stat } = require("node:fs/promises");

const PKG_DIR = path.resolve(__dirname, "..");
const FIXTURE_DIR = path.join(PKG_DIR, "test", "fixture");
const BUILD_DIR = path.join(FIXTURE_DIR, "build");

// npm workspaces hoist binaries to the repo root's node_modules/.bin; walk
// up from the package so the test works standalone and in the workspace.
function findBin(name) {
  let dir = PKG_DIR;
  for (;;) {
    const candidate = path.join(dir, "node_modules", ".bin", name);
    if (fs.existsSync(candidate)) return candidate;
    const parent = path.dirname(dir);
    if (parent === dir) throw new Error(`${name} binary not found above ${PKG_DIR}`);
    dir = parent;
  }
}
const DOCUSAURUS_BIN = findBin("docusaurus");

// Static file server with HTTP Range support (mirrors test/build-runtime.test.js
// serveStatic in the repo root). Serves `root`; the Rangefind runtime is pointed
// at `${baseUrl}rangefind/`.
async function serveStatic(root) {
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://localhost");
      const filePath = path.resolve(root, `.${decodeURIComponent(url.pathname)}`);
      if (!filePath.startsWith(path.resolve(root))) {
        response.writeHead(403).end();
        return;
      }
      const data = await readFile(filePath);
      const range = request.headers.range && request.headers.range.match(/^bytes=(\d+)-(\d+)$/);
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
  await new Promise((resolveListen) => server.listen(0, "127.0.0.1", resolveListen));
  const { port } = server.address();
  return {
    baseUrl: `http://127.0.0.1:${port}/`,
    close: () => new Promise((resolveClose) => server.close(resolveClose))
  };
}

// Recursively find the first .html file whose contents satisfy `predicate`.
async function findHtml(dir, predicate) {
  const entries = await readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      const hit = await findHtml(full, predicate);
      if (hit) return hit;
    } else if (entry.isFile() && entry.name.endsWith(".html")) {
      const html = await readFile(full, "utf8");
      if (predicate(html, full)) return { path: full, html };
    }
  }
  return null;
}

test("docusaurus build produces a Rangefind index, assets, tags, and is searchable", async (t) => {
  // 1) Run the real Docusaurus build CLI over the fixture.
  execFileSync(DOCUSAURUS_BIN, ["build"], {
    cwd: FIXTURE_DIR,
    timeout: 300000,
    stdio: "inherit",
    env: { ...process.env, NODE_ENV: "production" }
  });

  // 2) The index exists and looks healthy.
  const manifestPath = path.join(BUILD_DIR, "rangefind", "manifest.min.json");
  const manifestRaw = await readFile(manifestPath, "utf8");
  const manifest = JSON.parse(manifestRaw);
  assert.ok(typeof manifest.total === "number", "manifest.total is a number");
  assert.ok(manifest.total >= 2, `expected >= 2 indexed docs, got ${manifest.total}`);

  // 3) Client assets were copied.
  await stat(path.join(BUILD_DIR, "_rangefind", "rangefind-search.js"));
  await stat(path.join(BUILD_DIR, "_rangefind", "rangefind-search.css"));

  // 4) The injected module script tag is present in built HTML.
  const scriptHit = await findHtml(BUILD_DIR, (html) =>
    /<script[^>]*type="module"[^>]*src="\/_rangefind\/rangefind-search\.js"/.test(html)
  );
  assert.ok(scriptHit, "a built page contains the injected <script type=module ...> tag");

  // Bonus: the placed <rangefind-search> element made it into the HTML too.
  const elementHit = await findHtml(BUILD_DIR, (html) => html.includes("<rangefind-search"));
  assert.ok(elementHit, "a built page contains a <rangefind-search> element");

  // 5) Serve the build with Range support and run a real query.
  const server = await serveStatic(BUILD_DIR);
  t.after(() => server.close());
  const { createSearch } = await import("rangefind");
  const engine = await createSearch({ baseUrl: `${server.baseUrl}rangefind/` });
  const response = await engine.search({ q: "xylophone" });
  assert.ok(response.results.length >= 1, "search for 'xylophone' returns at least one result");
  const urls = response.results.map((r) => r.url || "");
  assert.ok(
    urls.some((u) => /about/.test(u)),
    `expected the About page in results, got urls: ${JSON.stringify(urls)}`
  );
});
