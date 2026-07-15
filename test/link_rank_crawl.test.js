import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { buildFromCrawl, crawlSite } from "../src/crawler.js";
import { createSearch } from "../src/runtime.js";

// Range-capable static file server, mirrored from test/crawler.test.js.
async function serveStatic(root) {
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://localhost");
      const path = resolve(root, `.${decodeURIComponent(url.pathname)}`);
      if (!path.startsWith(resolve(root))) return void response.writeHead(403).end();
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
        return void response.end(data.subarray(start, end + 1));
      }
      response.writeHead(200, { "Content-Length": String(data.length) });
      response.end(data);
    } catch {
      response.writeHead(404).end();
    }
  });
  await new Promise(done => server.listen(0, "127.0.0.1", done));
  const { port } = server.address();
  return {
    baseUrl: `http://127.0.0.1:${port}/rangefind/`,
    close: () => new Promise(done => server.close(done))
  };
}

const page = (title, body) =>
  `<!doctype html><html lang="en"><head><title>${title}</title></head><body><main>${body}</main></body></html>`;

// A hub linked by four referrers, plus a "lonely" page nobody links to. Every
// page shares the term "widget" so text relevance alone does not decide order.
async function writeLinkedSite(root) {
  const toHub = `<a href="hub.html">the widget hub</a>`;
  await writeFile(join(root, "hub.html"), page("Widget Hub", `<h1>Widget Hub</h1><p>The central widget catalog and reference.</p>`));
  await writeFile(join(root, "alpha.html"), page("Alpha", `<h1>Alpha widget</h1><p>Alpha widget notes. ${toHub}</p>`));
  await writeFile(join(root, "beta.html"), page("Beta", `<h1>Beta widget</h1><p>Beta widget notes. ${toHub}</p>`));
  await writeFile(join(root, "gamma.html"), page("Gamma", `<h1>Gamma widget</h1><p>Gamma widget notes. ${toHub}</p>`));
  await writeFile(join(root, "delta.html"), page("Delta", `<h1>Delta widget</h1><p>Delta widget notes. ${toHub}</p>`));
  await writeFile(join(root, "lonely.html"), page("Lonely", `<h1>Lonely widget</h1><p>Lonely widget notes, linked by nobody.</p>`));
}

test("link graph resolves absolute same-origin links under a base URL", async () => {
  const site = await mkdtemp(join(tmpdir(), "rangefind-abs-"));
  const base = "https://example.com/blog/";
  // A hub linked by two pages via ABSOLUTE internal URLs — the case that used to
  // silently produce an empty graph.
  const abs = `<a href="${base}hub">hub</a>`;
  await writeFile(join(site, "hub.html"), page("Hub", `<h1>Hub</h1><p>central widget</p>`));
  await writeFile(join(site, "one.html"), page("One", `<h1>One widget</h1><p>one ${abs}</p>`));
  await writeFile(join(site, "two.html"), page("Two", `<h1>Two widget</h1><p>two ${abs}</p>`));

  const { docs, config } = await crawlSite({ root: site, baseUrl: base });
  assert.ok(config.linkGraph, "a linkGraph block is produced from absolute links");
  assert.equal(config.linkGraph.edges, 2, "both absolute links resolved to edges");
  const rankById = Object.fromEntries(docs.map(doc => [doc.id, doc.linkRank]));
  assert.equal(rankById.hub, 1, "hub is the top authority");
  assert.ok(rankById.one < 1 && rankById.two < 1, "referrers rank below the hub");
});

test("the boost surfaces an authoritative page from beyond the requested window", async (t) => {
  const site = await mkdtemp(join(tmpdir(), "rangefind-surface-"));
  // 14 filler pages, then a hub that sorts last (highest doc id). Every page has
  // IDENTICAL indexable text, so BM25 for "widget" ties and ordering falls to
  // doc id — the hub lands at position 15, off a size-10 page. Only the hub is
  // linked (by every filler), so only the hub has authority.
  const body = `widget shared body text about widgets`;
  for (let i = 0; i < 14; i++) {
    await writeFile(join(site, `p${String(i).padStart(2, "0")}.html`),
      page("widget", `<h1>widget</h1><p>${body} <a href="zzz-hub.html">hub</a></p>`));
  }
  await writeFile(join(site, "zzz-hub.html"), page("widget", `<h1>widget</h1><p>${body} hub</p>`));
  await buildFromCrawl({ root: site, output: join(site, "rangefind"), baseUrl: "/" });

  const server = await serveStatic(site);
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });

  // Without the prior, the tied hub is off page 1.
  const plain = await search.search({ q: "widget", size: 10, linkRankBoost: 0 });
  assert.equal(plain.results.some(result => result.id === "zzz-hub"), false, "hub is off-page by BM25 alone");

  // With the default prior, overfetch reaches it and authority lifts it in.
  const boosted = await search.search({ q: "widget", size: 10 });
  assert.equal(boosted.results.some(result => result.id === "zzz-hub"), true, "authority surfaces the hub into the page");
  assert.equal(boosted.results[0].id, "zzz-hub", "the authoritative page wins the tie");
  assert.ok(boosted.stats.linkRankBoostPool > boosted.results.length, "the prior ranked a wider pool than the page");
});

test("the default boost respects a clear relevance gap (authority is a tie-breaker)", async (t) => {
  const site = await mkdtemp(join(tmpdir(), "rangefind-gap-"));
  // A strongly relevant page with no authority vs. a barely relevant hub with
  // maximum authority. The default prior must not let authority override a clear
  // textual win — it should only sway near-ties.
  await writeFile(join(site, "strong.html"),
    page("Widgets", `<h1>Widget widget widget</h1><p>${"widget ".repeat(20)}</p>`));
  await writeFile(join(site, "hub.html"), page("Hub", `<h1>Reference</h1><p>a single widget mention</p>`));
  for (let i = 0; i < 4; i++) {
    await writeFile(join(site, `ref${i}.html`),
      page("Ref", `<h1>Ref ${i}</h1><p>see the <a href="hub.html">hub</a> for details</p>`));
  }
  await buildFromCrawl({ root: site, output: join(site, "rangefind"), baseUrl: "/" });

  const server = await serveStatic(site);
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });

  const result = await search.search({ q: "widget", size: 10 });
  assert.equal(result.results[0].id, "strong", "a clear relevance win survives the default prior");
});

test("crawl materializes a linkRank authority signal and applies it at query time", async (t) => {
  const site = await mkdtemp(join(tmpdir(), "rangefind-linkrank-"));
  await writeLinkedSite(site);
  const output = join(site, "rangefind");
  await buildFromCrawl({ root: site, output, baseUrl: "/" });

  const server = await serveStatic(site);
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });

  // (a) linkRank is materialized as a doc-value; the hub — linked by four pages —
  // is the uniquely most-authoritative page (normalized to 1).
  const ranked = await search.search({ q: "widget", size: 10 });
  const byId = Object.fromEntries(ranked.results.map(result => [result.id, result.index]));
  const values = await search.loadDocValues(["linkRank"], Object.values(byId));
  const linkRankById = Object.fromEntries(Object.keys(byId).map((id, i) => [id, values[i].linkRank]));
  assert.equal(linkRankById.hub, 1, "hub normalizes to the top authority");
  for (const id of ["alpha", "beta", "gamma", "delta", "lonely"]) {
    assert.ok(linkRankById[id] < 1, `${id} is below the hub`);
  }

  // (b) linkRank is a sortable field, with and without a text query. The prior
  // must NOT corrupt an explicit sort (regression: the boost once resorted the
  // zero-scored browse window back into doc-id order).
  const byAuthority = await search.search({ q: "widget", sort: { field: "linkRank", order: "desc" }, size: 10 });
  assert.equal(byAuthority.results[0].id, "hub", "hub sorts first by authority (with query)");
  assert.equal(byAuthority.stats?.linkRankBoost, undefined, "no boost applied under an explicit sort");

  const browseByAuthority = await search.search({ q: "", sort: { field: "linkRank", order: "desc" }, size: 10 });
  assert.equal(browseByAuthority.results[0].id, "hub", "hub sorts first by authority (browse)");
  assert.equal(browseByAuthority.stats?.linkRankBoost, undefined, "no boost applied on a browse");

  // (c) With the boost disabled, the prior does not touch the response.
  const noBoost = await search.search({ q: "widget", size: 10, linkRankBoost: 0 });
  assert.equal(noBoost.stats?.linkRankBoost, undefined, "no boost stat when disabled");

  // (d) A strong boost lifts the authoritative hub to the top of the window.
  const boosted = await search.search({ q: "widget", size: 10, linkRankBoost: 20 });
  assert.equal(boosted.results[0].id, "hub", "authority wins under a strong prior");
  assert.equal(boosted.stats.linkRankBoost, true);

  // (e) The default crawl boost is active without any query parameter.
  const defaultBoost = await search.search({ q: "widget", size: 10 });
  assert.equal(defaultBoost.stats.linkRankBoost, true, "crawl enables a default prior");
});
