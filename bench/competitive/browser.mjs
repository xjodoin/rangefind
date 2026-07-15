// Competitive benchmark — browser stage (Pagefind).
//
//   node browser.mjs [--transfer=60]
//
// Pagefind's search runtime is browser-only WASM, so we measure it in a real
// headless Chromium: known-item recall@10/MRR over the shared query set, and
// cold per-query index transfer (pf_index chunks + pf_fragments) using
// cache-busted fresh module instances against a no-store server. Requires the
// node stage (run.mjs) to have produced work/pagefind-site and work/queries.json.
//
// Setup once: npm install && npx playwright install chromium

import { createServer } from "node:http";
import { readFile, writeFile, stat } from "node:fs/promises";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const HERE = dirname(fileURLToPath(import.meta.url));
const WORK = join(HERE, "work");
const SITE = join(WORK, "pagefind-site");
const arg = (n, d) => { const h = process.argv.find(a => a.startsWith(`--${n}=`)); return h ? Number(h.slice(n.length + 3)) : d; };
const TRANSFER = arg("transfer", 60);
const pctl = (a, p) => { const s = [...a].sort((x, y) => x - y); return s.length ? s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))] : 0; };

const PAGE = `<!doctype html><html lang="fr"><head><meta charset="utf-8"></head><body>ready</body></html>`;

async function serve(root) {
  let indexBytes = 0;
  // Count all index DATA fetched to answer a query — entry + meta + index chunks
  // + fragments — mirroring rangefind's cold count (manifest + directory +
  // postings + doc pages). Exclude only the runtime engine: pagefind.js and the
  // .pagefind wasm binaries (rangefind's JS runtime is likewise not counted).
  const isIndex = path => /\/fragment\/|\.pf_fragment|\/index\/|\.pf_index|pagefind-entry\.json|\.pf_meta/.test(path);
  const server = createServer(async (rq, rs) => {
    try {
      const u = new URL(rq.url, "http://x");
      const path = decodeURIComponent(u.pathname);
      // No-store so cache-busted fresh imports actually re-fetch (cold).
      const headers = { "Cache-Control": "no-store" };
      if (path === "/" || path === "/index.html") {
        rs.writeHead(200, { ...headers, "Content-Type": "text/html" });
        return void rs.end(PAGE);
      }
      const p = resolve(root, `.${path.split("?")[0]}`);
      if (!p.startsWith(resolve(root))) return void rs.writeHead(403).end();
      const data = await readFile(p);
      if (isIndex(path)) indexBytes += data.length;
      const type = p.endsWith(".js") ? "text/javascript" : p.endsWith(".json") ? "application/json" : "application/octet-stream";
      rs.writeHead(200, { ...headers, "Content-Type": type, "Content-Length": String(data.length) });
      rs.end(data);
    } catch { rs.writeHead(404).end(); }
  });
  await new Promise(done => server.listen(0, "127.0.0.1", done));
  return {
    origin: `http://127.0.0.1:${server.address().port}`,
    reset() { indexBytes = 0; },
    read() { return indexBytes; },
    close: () => new Promise(d => server.close(d))
  };
}

async function main() {
  const queries = JSON.parse(await readFile(join(WORK, "queries.json"), "utf8"));
  const server = await serve(SITE);
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto(`${server.origin}/`);

  const base = `${server.origin}/pagefind/pagefind.js`;

  // Warm recall@10 / MRR over all queries (single module instance).
  const quality = await page.evaluate(async ({ base, queries }) => {
    const pf = await import(base);
    if (pf.options) await pf.options({});
    let hits = 0, rr = 0, n = 0;
    for (const { q, target } of queries) {
      const s = await pf.search(q);
      const data = await Promise.all((s.results || []).slice(0, 10).map(r => r.data()));
      const rank = data.map(d => d?.meta?.id).indexOf(target);
      n++;
      if (rank >= 0) { hits++; rr += 1 / (rank + 1); }
    }
    return { recall: hits / n, mrr: rr / n, n };
  }, { base, queries });

  // Cold per-query index transfer, counted server-side. A fresh (cache-busted)
  // module instance each time means an empty in-memory chunk cache, so every
  // pf_index/pf_fragment the query needs is actually fetched from the server.
  const cold = [];
  const sample = Math.min(TRANSFER, queries.length);
  for (let i = 0; i < sample; i++) {
    server.reset();
    await page.evaluate(async ({ base, q, i }) => {
      const mod = await import(`${base}?b=${i}_${Date.now()}`);
      if (mod.options) await mod.options({});
      const s = await mod.search(q);
      await Promise.all((s.results || []).slice(0, 10).map(r => r.data()));
    }, { base, q: queries[i].q, i });
    cold.push(server.read());
  }
  const out = { recall: quality.recall, mrr: quality.mrr, n: quality.n, cold };

  await browser.close();
  await server.close();

  const results = JSON.parse(await readFile(join(HERE, "results.json"), "utf8"));
  const pf = results.engines.pagefind;
  pf.recall10 = +out.recall.toFixed(3);
  pf.mrr = +out.mrr.toFixed(3);
  pf.coldQueryKB = +(pctl(out.cold, 50) / 1e3).toFixed(1);
  pf.coldQueryKBp95 = +(pctl(out.cold, 95) / 1e3).toFixed(1);
  delete pf.note;
  await writeFile(join(HERE, "results.json"), JSON.stringify(results, null, 2));
  console.log("pagefind (browser):", JSON.stringify({ recall10: pf.recall10, mrr: pf.mrr, coldQueryKB: pf.coldQueryKB, coldQueryKBp95: pf.coldQueryKBp95, sampled: out.n }));
}

main().catch(e => { console.error(e); process.exitCode = 1; });
