// Real end-to-end test: run an actual Eleventy build of the fixture through
// Eleventy's programmatic API, then prove the produced index is searchable over
// HTTP Range requests via Rangefind's own runtime.
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import Eleventy from "@11ty/eleventy";
import { createSearch } from "rangefind";
import eleventyConfigFn from "./fixture/eleventy.config.js";

const here = dirname(fileURLToPath(import.meta.url));
const inputDir = join(here, "fixture", "src");

// Minimal static file server with HTTP Range support (mirrors the serveStatic
// helper pattern in the monorepo's test/build-runtime.test.js).
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

test("eleventy-plugin-rangefind: real Eleventy build produces a searchable index", async (t) => {
  const outputDir = await mkdtemp(join(tmpdir(), "eleventy-rangefind-"));
  t.after(() => rm(outputDir, { recursive: true, force: true }));

  // 1. Run a real Eleventy build. Must not throw. The plugin reads the real
  // output directory from the eleventy.after event's `directories.output`,
  // which reflects this constructor output argument.
  const elev = new Eleventy(inputDir, outputDir, { config: eleventyConfigFn });
  await elev.write();

  // Pages built.
  assert.ok(await exists(join(outputDir, "index.html")), "index.html written");
  assert.ok(await exists(join(outputDir, "about", "index.html")), "about/index.html written");

  // 2. Rangefind manifest exists, is valid JSON, indexes >= 2 docs.
  const manifestPath = join(outputDir, "rangefind", "manifest.min.json");
  assert.ok(await exists(manifestPath), "manifest.min.json exists");
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  assert.ok(Number(manifest.total) >= 2, `manifest.total >= 2 (got ${manifest.total})`);

  // 3. Client assets copied.
  assert.ok(await exists(join(outputDir, "_rangefind", "rangefind-search.js")), "rangefind-search.js copied");
  assert.ok(await exists(join(outputDir, "_rangefind", "rangefind-search.css")), "rangefind-search.css copied");

  // The shortcode rendered the component markup into the home page.
  const indexHtml = await readFile(join(outputDir, "index.html"), "utf8");
  assert.match(indexHtml, /<rangefind-search src="\/rangefind\/"/, "component element rendered");
  assert.match(indexHtml, /_rangefind\/rangefind-search\.js/, "component script src rendered");
  assert.match(indexHtml, /rel="stylesheet" href="\/_rangefind\/rangefind-search\.css"/, "theme stylesheet rendered when theme:true");

  // 4. Serve the output and search it through the real runtime over Range reqs.
  const server = await serveStatic(outputDir);
  t.after(() => server.close());
  const engine = await createSearch({ baseUrl: `${server.origin}/rangefind/` });
  const response = await engine.search({ q: "xylophone" });
  const urls = response.results.map(r => r.url);
  assert.ok(
    urls.some(u => u === "/about" || u === "/about/" || String(u).includes("about")),
    `About page returned for "xylophone" (got ${JSON.stringify(urls)})`
  );
});
