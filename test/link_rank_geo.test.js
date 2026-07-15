import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import { computeLinkRank } from "../src/link_graph.js";

async function serveStatic(root) {
  const server = createServer(async (rq, rs) => {
    try {
      const u = new URL(rq.url, "http://x");
      const p = resolve(root, `.${decodeURIComponent(u.pathname)}`);
      if (!p.startsWith(resolve(root))) return void rs.writeHead(403).end();
      const data = await readFile(p);
      const r = rq.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (r) {
        const a = +r[1], b = Math.min(+r[2], data.length - 1);
        rs.writeHead(206, { "Accept-Ranges": "bytes", "Content-Length": String(b - a + 1), "Content-Range": `bytes ${a}-${b}/${data.length}` });
        return void rs.end(data.subarray(a, b + 1));
      }
      rs.writeHead(200, { "Content-Length": String(data.length) });
      rs.end(data);
    } catch { rs.writeHead(404).end(); }
  });
  await new Promise(done => server.listen(0, "127.0.0.1", done));
  return { baseUrl: `http://127.0.0.1:${server.address().port}/rangefind/`, close: () => new Promise(d => server.close(d)) };
}

// A small corpus that has BOTH coordinates and a link graph: 12 places sharing
// the term "widget", each linking to a single hub (place 0), spread across a
// region so geo filters select subsets.
async function buildGeoLinkIndex(dir) {
  const n = 12;
  const adjacency = Array.from({ length: n }, (_, i) => (i === 0 ? [] : [0]));
  const linkRank = computeLinkRank(adjacency);
  const docs = Array.from({ length: n }, (_, i) => ({
    id: `p${i}`,
    title: `widget place ${i}`,
    body: "widget shared body text",
    lat: 45 + i * 0.1,
    lon: 2 + i * 0.1,
    linkRank: Math.round(linkRank[i] * 1e6) / 1e6
  }));
  const input = join(dir, "docs.jsonl");
  await writeFile(input, docs.map(d => JSON.stringify(d)).join("\n") + "\n");
  await writeFile(join(dir, "config.json"), JSON.stringify({
    input, output: join(dir, "rangefind"), idPath: "id", urlPath: "id", targetPostingsPerDoc: 32,
    fields: [{ name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true }, { name: "body", path: "body", weight: 1, b: 0.75 }],
    geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
    numbers: [{ name: "linkRank", path: "linkRank", type: "double", sortable: true }],
    sorts: [{ field: "linkRank", order: "desc" }],
    linkGraph: { field: "linkRank", boost: 0.5 },
    display: ["title"]
  }));
  await build({ configPath: join(dir, "config.json") });
}

test("linkRank boost composes with geo filters but is skipped under distance sort", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "rangefind-geolink-"));
  await buildGeoLinkIndex(dir);
  const server = await serveStatic(dir);
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });

  const box = { minLat: 45, maxLat: 46.5, minLon: 2, maxLon: 3.5 };

  // (a) Under a bounding-box FILTER the query is still relevance-ranked, so the
  // authority prior applies and reranks the geo-filtered results.
  const bbox = await search.search({ q: "widget", geo: { box }, size: 10 });
  assert.equal(bbox.stats?.linkRankBoost, true, "boost applies under a geo filter");
  assert.ok(bbox.results.length > 0, "geo filter returns results");

  // (b) Under a radius filter the prior likewise applies.
  const radius = await search.search({ q: "widget", geo: { near: { lat: 45.5, lon: 2.5, radiusMeters: 300000 } }, size: 10 });
  assert.equal(radius.stats?.linkRankBoost, true, "boost applies under a radius filter");

  // (c) Under distance SORT the ordering is geometric, not relevance — the prior
  // must not fire (multiplying zero-ish scores would corrupt the order).
  const distance = await search.search({ q: "widget", geo: { near: { lat: 45.5, lon: 2.5 }, sort: "distance" }, size: 10 });
  assert.equal(distance.stats?.linkRankBoost, undefined, "boost is skipped under distance sort");
  const distances = distance.results.map(r => r.distanceMeters).filter(d => typeof d === "number");
  assert.deepEqual(distances, [...distances].sort((a, b) => a - b), "distance order is preserved");
});
