// Competitive static-search benchmark — node stage.
//
//   node run.mjs [--docs=8000] [--queries=300]
//
// Indexes the SAME frwiki corpus in rangefind, Orama, and Pagefind; measures
// build time and index size for all three; and measures known-item retrieval
// quality (recall@10, MRR) plus cold per-query index transfer for rangefind and
// Orama here. Pagefind is indexed here and queried in the browser stage
// (browser.mjs) because its search runtime is WASM/browser-only.
//
// Design notes on fairness:
//  - All three use French analysis (stemming + stopwords).
//  - "Transfer" is index bytes fetched to answer a query, excluding the
//    one-time runtime/wasm. Orama must fetch the whole index before any query
//    (in-memory model), so its cold per-query transfer IS the index size.
//  - Quality uses known-item title queries: analyzer-agnostic and unambiguous.

import { mkdir, readdir, rm, stat, writeFile, readFile } from "node:fs/promises";
import { join, resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { build } from "../../src/builder.js";
import { createSearch } from "../../src/runtime.js";
import { create, insertMultiple, search as oramaSearch } from "@orama/orama";
import { stemmer as frenchStemmer } from "@orama/stemmers/french";
import { persistToFile } from "@orama/plugin-data-persistence/server";
import * as pagefind from "pagefind";
import { loadCorpus, knownItemQueries, scoreQuality, byteCountingServer, pct, nowMs } from "./lib.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(HERE, "../..");
const arg = (n, d) => { const h = process.argv.find(a => a.startsWith(`--${n}=`)); return h ? Number(h.slice(n.length + 3)) : d; };
const DOCS = arg("docs", 8000);
const QUERIES = arg("queries", 300);
const CORPUS = resolve(REPO, "examples/frwiki/scale/50000/data/frwiki.jsonl");
const WORK = join(HERE, "work");

async function dirSize(dir) {
  let total = 0;
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const p = join(dir, entry.name);
    if (entry.isDirectory()) total += await dirSize(p);
    else total += (await stat(p)).size;
  }
  return total;
}
const mb = b => +(b / 1e6).toFixed(2);

async function main() {
  await rm(WORK, { recursive: true, force: true });
  await mkdir(WORK, { recursive: true });
  console.log(`Loading ${DOCS} frwiki articles...`);
  const docs = await loadCorpus(CORPUS, DOCS);
  const queries = knownItemQueries(docs, QUERIES);
  console.log(`  ${docs.length} docs, ${queries.length} known-item queries`);
  const results = { corpus: "frwiki", docs: docs.length, queries: queries.length, engines: {} };

  // ---- rangefind ----------------------------------------------------------
  {
    const out = join(WORK, "rangefind");
    const input = join(WORK, "rf-docs.jsonl");
    await writeFile(input, docs.map(d => JSON.stringify(d)).join("\n") + "\n");
    await writeFile(join(WORK, "rf.config.json"), JSON.stringify({
      input, output: out, idPath: "id", urlPath: "url", targetPostingsPerDoc: 24, bodyIndexChars: 6000,
      analysis: { languages: ["fr"] },
      fields: [
        { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
        { name: "body", path: "body", weight: 1.0, b: 0.75 }
      ],
      display: ["title", "url"]
    }));
    const t0 = nowMs();
    await build({ configPath: join(WORK, "rf.config.json") });
    const buildMs = nowMs() - t0;
    const size = await dirSize(out);

    const server = await byteCountingServer(out);
    const engine = await createSearch({ baseUrl: server.baseUrl });
    // Quality (warm engine).
    const ids = new Map();
    for (const { q } of queries) {
      const res = await engine.search({ q, size: 10 });
      ids.set(q, res.results.map(r => r.id));
    }
    const quality = scoreQuality(queries, q => ids.get(q) || []);
    // Cold per-query transfer + latency: a fresh engine per query, cache empty.
    const transfers = [], lats = [];
    for (const { q } of queries.slice(0, 60)) {
      const cold = await createSearch({ baseUrl: server.baseUrl });
      server.reset();
      const t = nowMs();
      await cold.search({ q, size: 10 });
      lats.push(nowMs() - t);
      transfers.push(server.read().bytes);
    }
    await server.close();
    transfers.sort((a, b) => a - b); lats.sort((a, b) => a - b);
    results.engines.rangefind = {
      model: "static index, HTTP range", buildMs: Math.round(buildMs), indexMB: mb(size),
      recall10: +quality.recall.toFixed(3), mrr: +quality.mrr.toFixed(3),
      coldQueryKB: +(pct(transfers, 50) / 1e3).toFixed(1), coldQueryKBp95: +(pct(transfers, 95) / 1e3).toFixed(1),
      latencyMsP50: +pct(lats, 50).toFixed(2)
    };
    console.log("  rangefind:", JSON.stringify(results.engines.rangefind));
  }

  // ---- Orama --------------------------------------------------------------
  {
    const t0 = nowMs();
    const db = await create({
      schema: { id: "string", title: "string", body: "string" },
      components: { tokenizer: { language: "french", stemming: true, stemmer: frenchStemmer } }
    });
    await insertMultiple(db, docs.map(d => ({ id: d.id, title: d.title, body: d.body })), 500);
    const buildMs = nowMs() - t0;
    const persistPath = join(WORK, "orama-index.msp");
    await persistToFile(db, "binary", persistPath);
    const size = (await stat(persistPath)).size;

    const lats = [];
    const ids = new Map();
    for (const { q } of queries) {
      const t = nowMs();
      // Give Orama a competent config: title-weighted, full recall (threshold 0).
      const res = await oramaSearch(db, { term: q, limit: 10, properties: ["title", "body"], boost: { title: 4 }, threshold: 0 });
      lats.push(nowMs() - t);
      ids.set(q, res.hits.map(h => h.document.id));
    }
    const quality = scoreQuality(queries, q => ids.get(q) || []);
    lats.sort((a, b) => a - b);
    results.engines.orama = {
      model: "in-memory (full index downloaded first)", buildMs: Math.round(buildMs), indexMB: mb(size),
      recall10: +quality.recall.toFixed(3), mrr: +quality.mrr.toFixed(3),
      // Orama must download the entire index before it can answer anything.
      coldQueryKB: +(size / 1e3).toFixed(1), coldQueryKBp95: +(size / 1e3).toFixed(1),
      latencyMsP50: +pct(lats, 50).toFixed(3)
    };
    console.log("  orama:", JSON.stringify(results.engines.orama));
  }

  // ---- Pagefind (index here; query in browser stage) ----------------------
  {
    const out = join(WORK, "pagefind-site");
    await mkdir(out, { recursive: true });
    const t0 = nowMs();
    const { index } = await pagefind.createIndex({});
    for (const d of docs) {
      await index.addCustomRecord({
        url: `/a/${d.id}`,
        content: `${d.title}. ${d.body}`,
        language: "fr",
        meta: { title: d.title, id: d.id }
      });
    }
    await index.writeFiles({ outputPath: join(out, "pagefind") });
    await pagefind.close();
    const buildMs = nowMs() - t0;
    const size = await dirSize(join(out, "pagefind"));
    results.engines.pagefind = {
      model: "static index, fragment fetch (WASM)", buildMs: Math.round(buildMs), indexMB: mb(size),
      recall10: null, mrr: null, coldQueryKB: null, coldQueryKBp95: null, latencyMsP50: null,
      note: "quality + transfer measured in browser stage"
    };
    console.log("  pagefind indexed:", JSON.stringify(results.engines.pagefind));
  }

  await writeFile(join(HERE, "results.json"), JSON.stringify(results, null, 2));
  await writeFile(join(WORK, "queries.json"), JSON.stringify(queries));
  console.log(`\nWrote results.json (pagefind query stage pending).`);
}

main().catch(e => { console.error(e); process.exitCode = 1; });
