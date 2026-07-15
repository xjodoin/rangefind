// Query-side benchmark for the link-graph authority prior on a real built index.
//
//   node scripts/link_rank_query_bench.mjs [--pages=5000] [--queries=200] [--size=10] [--overfetch=4]
//
// Generates a synthetic linked site (Zipf-biased internal links so authority
// actually varies), builds it with the real crawler + builder, serves it over a
// range-capable HTTP server, and measures search latency with the boost off vs.
// on. Answers "what does the overfetch cost at scale?" — the part the pure
// PageRank micro-bench (scripts/link_rank_bench.mjs) does not cover.

import { createServer } from "node:http";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { buildFromCrawl } from "../src/crawler.js";
import { createSearch } from "../src/runtime.js";

function arg(name, fallback) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? Number(hit.slice(name.length + 3)) : fallback;
}

const PAGES = arg("pages", 5000);
const QUERIES = arg("queries", 200);
const SIZE = arg("size", 10);
const OVERFETCH = arg("overfetch", 4);

// Deterministic PRNG (Math.random is fine in a script, but a seed makes runs
// comparable).
function lcg(seed) {
  let s = seed >>> 0;
  return () => (s = (Math.imul(s, 1664525) + 1013904223) >>> 0) / 0x100000000;
}
const rand = lcg(0x51ed270b);

const VOCAB = Array.from({ length: 300 }, (_, i) => `term${i}`);
function words(n) {
  const out = [];
  for (let i = 0; i < n; i++) out.push(VOCAB[Math.floor(rand() * VOCAB.length)]);
  return out.join(" ");
}
// Zipf-ish target: small ids are far more likely to be linked, creating hubs.
function zipfTarget() {
  return Math.floor(PAGES * Math.pow(rand(), 3));
}

function pct(sorted, p) {
  if (!sorted.length) return 0;
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[idx];
}
const ms = ns => Number(ns) / 1e6;

async function serveStatic(root) {
  const server = createServer(async (rq, rs) => {
    try {
      const u = new URL(rq.url, "http://localhost");
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
    } catch {
      rs.writeHead(404).end();
    }
  });
  await new Promise(done => server.listen(0, "127.0.0.1", done));
  return { baseUrl: `http://127.0.0.1:${server.address().port}/rangefind/`, close: () => new Promise(d => server.close(d)) };
}

async function timedQueries(search, label, extra) {
  const times = [];
  let pool = 0;
  let boostedHits = 0;
  for (let i = 0; i < QUERIES; i++) {
    const q = VOCAB[Math.floor(rand() * VOCAB.length)] + " " + VOCAB[Math.floor(rand() * VOCAB.length)];
    const start = process.hrtime.bigint();
    const res = await search.search({ q, size: SIZE, ...extra });
    times.push(ms(process.hrtime.bigint() - start));
    if (res.stats?.linkRankBoost) boostedHits++;
    if (res.stats?.linkRankBoostPool) pool += res.stats.linkRankBoostPool;
  }
  times.sort((a, b) => a - b);
  const avg = times.reduce((s, t) => s + t, 0) / times.length;
  console.log(`  ${label.padEnd(22)} p50=${pct(times, 50).toFixed(1)}ms  p95=${pct(times, 95).toFixed(1)}ms  avg=${avg.toFixed(1)}ms`
    + (boostedHits ? `  (boosted ${boostedHits}/${QUERIES}, avg pool ${(pool / Math.max(1, boostedHits)).toFixed(0)})` : ""));
}

async function main() {
  console.log(`Generating ${PAGES} linked pages...`);
  const site = await mkdtemp(join(tmpdir(), "rangefind-qbench-"));
  const idOf = i => `page${String(i).padStart(6, "0")}`;
  for (let i = 0; i < PAGES; i++) {
    const links = [];
    const degree = 4 + Math.floor(rand() * 10);
    for (let k = 0; k < degree; k++) {
      const t = zipfTarget();
      if (t !== i) links.push(`<a href="${idOf(t)}.html">${VOCAB[t % VOCAB.length]}</a>`);
    }
    const html = `<!doctype html><html lang="en"><head><title>${idOf(i)}</title></head><body><main>`
      + `<h1>${words(4)}</h1><p>${words(80)} ${links.join(" ")}</p></main></body></html>`;
    await writeFile(join(site, `${idOf(i)}.html`), html);
  }

  console.log("Building index...");
  const buildStart = process.hrtime.bigint();
  await buildFromCrawl({ root: site, output: join(site, "rangefind"), baseUrl: "/" });
  const buildMs = ms(process.hrtime.bigint() - buildStart);
  const manifest = JSON.parse(await readFile(join(site, "rangefind", "manifest.json"), "utf8"));
  console.log(`  build=${(buildMs / 1000).toFixed(1)}s  linkGraph=${JSON.stringify(manifest.linkGraph)}`);

  const server = await serveStatic(site);
  const search = await createSearch({ baseUrl: server.baseUrl });

  // Warm caches (directory, manifests) so the first query doesn't skew p50.
  for (let i = 0; i < 10; i++) await search.search({ q: VOCAB[i], size: SIZE });

  console.log(`\nQuery latency over ${QUERIES} two-term queries (size=${SIZE}):`);
  await timedQueries(search, "boost off", { linkRankBoost: 0 });
  await timedQueries(search, "boost on (default 4x)", {});
  await timedQueries(search, `boost on (${OVERFETCH}x)`, { linkRankOverfetch: OVERFETCH });

  await server.close();
  await rm(site, { recursive: true, force: true });
}

main();
