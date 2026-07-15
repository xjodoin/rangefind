// linkRank authority prior combined with geo queries.
//
//   node scripts/link_rank_geo_bench.mjs [--pages=8000] [--queries=150] [--size=10]
//
// No existing corpus has both a link graph and coordinates, so this synthesizes
// one: documents with lat/lon, text, and Zipf-biased internal links (so
// authority varies). It builds a real index with a geo field + linkGraph, then
// measures query latency and boost behavior across geo lanes — verifying the
// authority prior composes with geo *filters* (bbox/radius) but is correctly
// skipped under distance sort, where it would corrupt the ordering.

import { createServer } from "node:http";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import { computeLinkRank } from "../src/link_graph.js";

function arg(name, fallback) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? Number(hit.slice(name.length + 3)) : fallback;
}
const PAGES = arg("pages", 8000);
const QUERIES = arg("queries", 150);
const SIZE = arg("size", 10);

// Region the documents live in (roughly France-sized), and a sub-box/center
// used for geo filters so a query hits a meaningful subset.
const REGION = { minLat: 42, maxLat: 51, minLon: -4, maxLon: 8 };
const SUB_BOX = { minLat: 45, maxLat: 47, minLon: 1, maxLon: 4 };
const CENTER = { lat: 46, lon: 2.5 };
const RADIUS = 200000; // meters

function lcg(seed) {
  let s = seed >>> 0;
  return () => (s = (Math.imul(s, 1664525) + 1013904223) >>> 0) / 0x100000000;
}
const rand = lcg(0x2f6a11c3);
const VOCAB = Array.from({ length: 300 }, (_, i) => `term${i}`);
const words = n => Array.from({ length: n }, () => VOCAB[Math.floor(rand() * VOCAB.length)]).join(" ");
const zipf = () => Math.floor(PAGES * Math.pow(rand(), 3));
const ms = ns => Number(ns) / 1e6;
function pct(sorted, p) {
  return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] : 0;
}

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

async function timed(search, label, extra) {
  const times = [];
  let boosted = 0, pool = 0, empty = 0;
  for (let i = 0; i < QUERIES; i++) {
    const q = `${VOCAB[Math.floor(rand() * VOCAB.length)]} ${VOCAB[Math.floor(rand() * VOCAB.length)]}`;
    const start = process.hrtime.bigint();
    const res = await search.search({ q, size: SIZE, ...extra });
    times.push(ms(process.hrtime.bigint() - start));
    if (res.stats?.linkRankBoost) boosted++;
    if (res.stats?.linkRankBoostPool) pool += res.stats.linkRankBoostPool;
    if (!res.results?.length) empty++;
  }
  times.sort((a, b) => a - b);
  const avg = times.reduce((s, t) => s + t, 0) / times.length;
  console.log(`  ${label.padEnd(30)} p50=${pct(times, 50).toFixed(1)}ms p95=${pct(times, 95).toFixed(1)}ms avg=${avg.toFixed(1)}ms`
    + `  boosted=${boosted}/${QUERIES}${boosted ? ` pool≈${(pool / boosted).toFixed(0)}` : ""}${empty ? ` empty=${empty}` : ""}`);
}

async function main() {
  console.log(`Generating ${PAGES} geo-located, linked documents...`);
  const dir = await mkdtemp(join(tmpdir(), "rangefind-geobench-"));
  const adjacency = new Array(PAGES);
  const docs = new Array(PAGES);
  for (let i = 0; i < PAGES; i++) {
    const deg = 4 + Math.floor(rand() * 8);
    const row = [];
    for (let k = 0; k < deg; k++) { const t = zipf(); if (t !== i) row.push(t); }
    adjacency[i] = row;
    docs[i] = {
      id: `n${String(i).padStart(6, "0")}`,
      title: `place ${words(2)}`,
      body: words(60),
      lat: REGION.minLat + rand() * (REGION.maxLat - REGION.minLat),
      lon: REGION.minLon + rand() * (REGION.maxLon - REGION.minLon)
    };
  }
  const linkRank = computeLinkRank(adjacency);
  for (let i = 0; i < PAGES; i++) docs[i].linkRank = Math.round(linkRank[i] * 1e6) / 1e6;

  const input = join(dir, "docs.jsonl");
  const output = join(dir, "rangefind");
  await writeFile(input, docs.map(d => JSON.stringify(d)).join("\n") + "\n");
  await writeFile(join(dir, "config.json"), JSON.stringify({
    input, output, idPath: "id", urlPath: "id", targetPostingsPerDoc: 64,
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
    numbers: [{ name: "linkRank", path: "linkRank", type: "double", sortable: true }],
    sorts: [{ field: "linkRank", order: "desc" }],
    linkGraph: { field: "linkRank", boost: 0.5 },
    display: ["title"]
  }));

  console.log("Building index (text + geo + linkRank)...");
  const bt = process.hrtime.bigint();
  await build({ configPath: join(dir, "config.json") });
  console.log(`  build=${(ms(process.hrtime.bigint() - bt) / 1000).toFixed(1)}s`);

  const server = await serveStatic(dir);
  const search = await createSearch({ baseUrl: server.baseUrl });
  for (let i = 0; i < 10; i++) await search.search({ q: VOCAB[i], size: SIZE }); // warm

  console.log(`\nLatency over ${QUERIES} two-term queries (size=${SIZE}):`);
  await timed(search, "text only, boost off", { linkRankBoost: 0 });
  await timed(search, "text only, boost on", {});
  await timed(search, "text + bbox, boost off", { geo: { box: SUB_BOX }, linkRankBoost: 0 });
  await timed(search, "text + bbox, boost on", { geo: { box: SUB_BOX } });
  await timed(search, "text + radius, boost off", { geo: { near: { ...CENTER, radiusMeters: RADIUS } }, linkRankBoost: 0 });
  await timed(search, "text + radius, boost on", { geo: { near: { ...CENTER, radiusMeters: RADIUS } } });
  await timed(search, "text + distance sort", { geo: { near: CENTER, sort: "distance" } });

  // Correctness spot-check: the boost must NOT fire under distance sort.
  const distSort = await search.search({ q: `${VOCAB[3]} ${VOCAB[7]}`, geo: { near: CENTER, sort: "distance" }, size: SIZE });
  console.log(`\nDistance-sort boost correctly skipped: ${distSort.stats?.linkRankBoost === undefined}`);
  const bboxBoost = await search.search({ q: `${VOCAB[3]} ${VOCAB[7]}`, geo: { box: SUB_BOX }, size: SIZE });
  console.log(`Bbox-filter boost applied: ${bboxBoost.stats?.linkRankBoost === true}`);

  await server.close();
  await rm(dir, { recursive: true, force: true });
}

main().catch(err => { console.error(err); process.exitCode = 1; });
