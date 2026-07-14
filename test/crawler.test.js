import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { buildFromCrawl } from "../src/crawler.js";
import { createSearch } from "../src/runtime.js";

// Range-capable static file server, mirrored from test/build-runtime.test.js so
// the runtime can query the crawled index over real HTTP Range requests.
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

async function writeSite(root) {
  const page = (body, attrs = "") => `<!doctype html><html ${attrs}><head></head><body>${body}</body></html>`;
  await writeFile(join(root, "index.html"), page(`<main>
    <h1>Welcome to Rangefind</h1>
    <p>Static search over crawled HTML with no server required.</p>
  </main>`, `lang="en"`));

  await mkdir(join(root, "guide"), { recursive: true });
  await writeFile(join(root, "guide", "index.html"), page(`<main>
    <h1>Indexing Guide</h1>
    <p>Learn how the crawler extracts headings and body text.</p>
    <span data-rangefind-filter="section">documentation</span>
  </main>`, `lang="en"`));

  await writeFile(join(root, "recette.html"), page(`<main>
    <h1>Recette de crepes</h1>
    <p>Melangez la farine, les oeufs et le lait pour preparer des crepes legeres.</p>
    <span data-rangefind-filter="section">cuisine</span>
  </main>`, `lang="fr"`));

  await writeFile(join(root, "draft.html"), page(`<main>
    <h1>Secret Draft</h1>
    <p>This unfinished page should never appear in the index.</p>
  </main>`, `data-rangefind-ignore`));
}

test("buildFromCrawl indexes a static site and serves it through the runtime", async (t) => {
  const site = await mkdtemp(join(tmpdir(), "rangefind-site-"));
  await writeSite(site);
  const output = join(site, "rangefind");

  const summary = await buildFromCrawl({ root: site, output, baseUrl: "/" });
  assert.equal(summary.files, 4);
  assert.equal(summary.docs, 3);
  assert.deepEqual(summary.languages.sort(), ["en", "fr"]);

  const server = await serveStatic(site);
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });

  // (a) English and French queries return the right pages.
  const english = await search.search({ q: "crawler headings body", size: 5 });
  assert.equal(english.results[0].id, "guide");
  assert.equal(english.results[0].url, "/guide/");
  assert.equal(english.results[0].title, "Indexing Guide");

  const welcome = await search.search({ q: "static search server", size: 5 });
  assert.equal(welcome.results[0].id, "index");
  assert.equal(welcome.results[0].url, "/");

  const french = await search.search({ q: "farine oeufs lait crepes", size: 5 });
  assert.equal(french.results[0].id, "recette");
  assert.equal(french.results[0].url, "/recette");

  // (b) The ignored page never appears.
  const draft = await search.search({ q: "secret unfinished draft", size: 5 });
  assert.equal(draft.results.some(result => /draft/i.test(result.id)), false);

  // (c) A facet filter narrows results.
  const cuisine = await search.search({ q: "", filters: { facets: { section: ["cuisine"] } }, size: 5 });
  assert.deepEqual(cuisine.results.map(result => result.id), ["recette"]);
  const docs = await search.search({ q: "", filters: { facets: { section: ["documentation"] } }, size: 5 });
  assert.deepEqual(docs.results.map(result => result.id), ["guide"]);

  // (d) URLs are correct: index collapses to a directory URL, others are pretty.
  const all = await search.search({ q: "", size: 10 });
  const urls = Object.fromEntries(all.results.map(result => [result.id, result.url]));
  assert.equal(urls.index, "/");
  assert.equal(urls.guide, "/guide/");
  assert.equal(urls.recette, "/recette");
});

test("buildFromCrawl honors a base URL prefix and skips a nested output dir", async (t) => {
  const site = await mkdtemp(join(tmpdir(), "rangefind-baseurl-"));
  await writeFile(join(site, "index.html"),
    `<html lang="en"><body><main><h1>Home</h1><p>alpha beta gamma content</p></main></body></html>`);
  const output = join(site, "rangefind");

  await buildFromCrawl({ root: site, output, baseUrl: "https://example.com/blog/" });
  const server = await serveStatic(site);
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });
  const result = await search.search({ q: "alpha beta gamma", size: 5 });
  assert.equal(result.results[0].url, "https://example.com/blog/");
});

test("buildFromCrawl enrich hook: function form, and module path with config export", async (t) => {
  const site = await mkdtemp(join(tmpdir(), "rangefind-enrich-"));
  await writeFile(join(site, "index.html"),
    `<html lang="en"><body><main><h1>Beacon</h1><p>harbor beacon over the wharf</p></main></body></html>`);

  // Function form + config overrides.
  const fnOutput = join(site, "rangefind");
  await buildFromCrawl({
    root: site,
    output: fnOutput,
    config: { facets: [{ name: "kind", path: "kind" }] },
    enrich: async docs => {
      for (const doc of docs) doc.kind = "fn-enriched";
      return docs;
    }
  });
  const server = await serveStatic(site);
  t.after(() => server.close());
  const viaFn = await createSearch({ baseUrl: server.baseUrl });
  const fnHit = await viaFn.search({ q: "harbor beacon", facets: ["kind"], size: 2 });
  assert.equal(fnHit.facets.kind.values[0].value, "fn-enriched");

  // Module-path form: default export enriches, `config` export declares the
  // facet — the shape every integration (CLI --enrich, plugin options,
  // mkdocs setting) forwards.
  const modulePath = join(site, "enrich.mjs");
  await writeFile(modulePath, [
    `export const config = { facets: [{ name: "kind", path: "kind" }] };`,
    `export default async docs => docs.map(doc => ({ ...doc, kind: "module-enriched" }));`
  ].join("\n"));
  await rm(fnOutput, { recursive: true, force: true });
  await buildFromCrawl({ root: site, output: fnOutput, enrich: modulePath });
  const viaModule = await createSearch({ baseUrl: server.baseUrl });
  const moduleHit = await viaModule.search({ q: "harbor beacon", facets: ["kind"], size: 2 });
  assert.equal(moduleHit.facets.kind.values[0].value, "module-enriched");
});
