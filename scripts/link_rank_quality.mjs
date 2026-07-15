// Quality sweep justifying the default linkRank boost.
//
//   node scripts/link_rank_quality.mjs [--groups=50]
//
// A document prior is a tradeoff: too weak and it never sways ranking, too
// strong and it overrides relevance. This measures both sides on a labeled
// synthetic corpus as the boost varies, so the default (0.5) is chosen from a
// curve rather than a single anecdote.
//
// Two query families, each with a known-correct #1 result:
//   TIE   — several documents with IDENTICAL text (BM25 ties); one is the
//           authoritative hub, deliberately placed at a non-first doc id. The
//           prior SHOULD lift the hub to #1. Metric: tieWin (higher = better).
//   CLEAR — one strongly relevant document (the query term repeated) with low
//           authority, versus a barely-relevant hub with maximum authority.
//           Relevance SHOULD win. Metric: clearHold (should stay 1.0).
//
// The sweet spot is the boost where tieWin is high while clearHold is still 1.0.

import { createServer } from "node:http";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";

const GROUPS = Number((process.argv.find(a => a.startsWith("--groups=")) || "").slice(9)) || 50;
const BOOSTS = [0, 0.25, 0.5, 1, 2, 4];

function lcg(seed) { let s = seed >>> 0; return () => (s = (Math.imul(s, 1664525) + 1013904223) >>> 0) / 0x100000000; }
const rand = lcg(0x13572468);
const FILLER = Array.from({ length: 40 }, (_, i) => `filler${i}`);
const fill = n => Array.from({ length: n }, () => FILLER[Math.floor(rand() * FILLER.length)]).join(" ");

async function serveStatic(root) {
  const server = createServer(async (rq, rs) => {
    try {
      const u = new URL(rq.url, "http://x");
      const p = resolve(root, `.${decodeURIComponent(u.pathname)}`);
      const data = await readFile(p);
      const r = rq.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (r) { const a = +r[1], b = Math.min(+r[2], data.length - 1); rs.writeHead(206, { "Accept-Ranges": "bytes", "Content-Length": String(b - a + 1), "Content-Range": `bytes ${a}-${b}/${data.length}` }); return void rs.end(data.subarray(a, b + 1)); }
      rs.writeHead(200, { "Content-Length": String(data.length) }); rs.end(data);
    } catch { rs.writeHead(404).end(); }
  });
  await new Promise(done => server.listen(0, "127.0.0.1", done));
  return { baseUrl: `http://127.0.0.1:${server.address().port}/rangefind/`, close: () => new Promise(d => server.close(d)) };
}

async function main() {
  const dir = await mkdtemp(join(tmpdir(), "rangefind-quality-"));
  const docs = [];
  const tieQueries = [];
  const clearWideQueries = [];
  const clearNarrowQueries = [];

  for (let g = 0; g < GROUPS; g++) {
    // TIE group: 8 identical docs; the hub (max authority) is the 4th doc id.
    const term = `tie${g}`;
    for (let i = 0; i < 8; i++) {
      docs.push({ id: `${term}_${i}`, title: term, body: `${term} shared body text ${fill(20)}`, linkRank: i === 3 ? 1.0 : 0.05 + rand() * 0.25 });
    }
    tieQueries.push({ q: term, ideal: `${term}_3` });

    // CLEAR groups: a strong-relevance doc (low authority) vs a hub (max
    // authority, weak relevance). "wide" gives the strong doc a large term-
    // frequency lead (6x), "narrow" only a modest one (2x) — the narrow case is
    // where an over-strong prior would wrongly override relevance.
    // "wide": the strong doc also out-matches on the title, a large margin.
    // "narrow": the hub matches the title equally, so the strong doc leads only
    // by a small body term-frequency margin — a genuinely close call.
    {
      const cterm = `clearwide${g}`;
      docs.push({ id: `${cterm}_strong`, title: cterm, body: `${(cterm + " ").repeat(6)} ${fill(20)}`, linkRank: 0.1 });
      docs.push({ id: `${cterm}_hub`, title: "reference", body: `a single ${cterm} mention ${fill(20)}`, linkRank: 1.0 });
      for (let i = 0; i < 3; i++) docs.push({ id: `${cterm}_f${i}`, title: cterm, body: `${cterm} ${fill(20)}`, linkRank: 0.1 });
      clearWideQueries.push({ q: cterm, ideal: `${cterm}_strong` });
    }
    {
      const cterm = `clearnarrow${g}`;
      docs.push({ id: `${cterm}_strong`, title: cterm, body: `${cterm} ${cterm} ${fill(20)}`, linkRank: 0.1 });
      docs.push({ id: `${cterm}_hub`, title: cterm, body: `${cterm} ${fill(20)}`, linkRank: 1.0 });
      clearNarrowQueries.push({ q: cterm, ideal: `${cterm}_strong` });
    }
  }

  const input = join(dir, "docs.jsonl");
  await writeFile(input, docs.map(d => JSON.stringify(d)).join("\n") + "\n");
  await writeFile(join(dir, "config.json"), JSON.stringify({
    input, output: join(dir, "rangefind"), idPath: "id", urlPath: "id", targetPostingsPerDoc: 48,
    fields: [{ name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true }, { name: "body", path: "body", weight: 1.0, b: 0.75 }],
    numbers: [{ name: "linkRank", path: "linkRank", type: "double", sortable: true }],
    linkGraph: { field: "linkRank", boost: 0.5 }, display: ["title"]
  }));
  await build({ configPath: join(dir, "config.json") });

  const server = await serveStatic(dir);
  const search = await createSearch({ baseUrl: server.baseUrl });

  const winRate = async (queries, boost) => {
    let win = 0;
    for (const { q, ideal } of queries) {
      const res = await search.search({ q, size: 5, linkRankBoost: boost });
      if (res.results[0]?.id === ideal) win++;
    }
    return win / queries.length;
  };

  // Interpretation:
  //   tieWin       — authority breaks EXACT ties (higher = the prior works).
  //   nearTieToHub — on a near-tie (equal title, +1 body mention) the far-more-
  //                  authoritative hub wins. This is the prior DOING ITS JOB, so
  //                  a high number here is expected once the prior is active.
  //   relevancePreserved — a CLEAR relevance winner (large margin) stays #1.
  //                  This is the guardrail; it must remain 100%.
  console.log(`Quality sweep over ${GROUPS} tie + ${GROUPS} clear + ${GROUPS} near-tie queries\n`);
  console.log("  boost   tieWin   nearTieToHub   relevancePreserved");
  let safeMax = 0;
  for (const boost of BOOSTS) {
    const tie = await winRate(tieQueries, boost);
    const preserved = await winRate(clearWideQueries, boost);
    const nearTieToHub = 1 - await winRate(clearNarrowQueries, boost);
    if (preserved === 1 && boost > safeMax) safeMax = boost;
    const pct = v => `${(v * 100).toFixed(0)}%`.padStart(4);
    const mark = boost === 0.5 ? "   <- default" : "";
    console.log(`  ${String(boost).padEnd(6)}  ${pct(tie)}     ${pct(nearTieToHub)}          ${pct(preserved)}${mark}`);
  }
  console.log(`\nStrongest boost that still preserves clear relevance winners: ${safeMax}`);
  console.log(`Default 0.5 is ${0.5 <= safeMax ? "within" : "ABOVE"} the safe band `
    + `(breaks clear relevance above ${safeMax}); it decides ties and near-ties by authority.`);

  await server.close();
  await rm(dir, { recursive: true, force: true });
}

main().catch(err => { console.error(err); process.exitCode = 1; });
