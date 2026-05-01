#!/usr/bin/env node

import { resolve } from "node:path";
import { createSearch } from "../src/runtime.js";
import {
  fold,
  mutateToken,
  parseArgs,
  readJsonLines,
  serveStatic
} from "./bench_support.mjs";

const STOPWORDS = new Set("a an and are as at be by for from in is of on or the to with static without into any can".split(/\s+/u));

const args = parseArgs(process.argv.slice(2), {
  root: "examples/basic/public",
  basePath: "rangefind/",
  docs: "examples/basic/docs.jsonl",
  known: 50,
  typos: 50,
  size: 10,
  json: false
});

function tokens(text) {
  return fold(text).split(/[^a-z0-9]+/u).filter(token => token.length >= 4 && !STOPWORDS.has(token));
}

function knownQueries(docs, limit) {
  return docs.slice(0, limit).map(doc => ({
    id: String(doc.id),
    title: doc.title,
    url: doc.url,
    q: tokens(doc.title).slice(0, 4).join(" ") || doc.title
  }));
}

function typoQueries(known, limit) {
  const out = [];
  for (let i = 0; i < known.length && out.length < limit; i++) {
    const parts = known[i].q.split(/\s+/u).filter(Boolean);
    const selected = parts.map((token, index) => ({ token, index })).filter(item => item.token.length >= 5)[0];
    if (!selected) continue;
    const typo = mutateToken(selected.token, i + 17);
    if (!typo || typo === selected.token) continue;
    const mutated = parts.slice();
    mutated[selected.index] = typo;
    out.push({ ...known[i], cleanQ: known[i].q, typoQ: mutated.join(" ") });
  }
  return out;
}

function rankIn(results, target) {
  const index = results.findIndex(result =>
    String(result.id) === String(target.id)
    || result.url === target.url
    || result.title === target.title);
  return index < 0 ? 0 : index + 1;
}

function metrics(ranks) {
  const n = ranks.length || 1;
  return {
    n: ranks.length,
    hit1: ranks.filter(rank => rank === 1).length / n,
    hit3: ranks.filter(rank => rank > 0 && rank <= 3).length / n,
    hit10: ranks.filter(rank => rank > 0 && rank <= 10).length / n,
    mrr10: ranks.reduce((sum, rank) => sum + (rank > 0 && rank <= 10 ? 1 / rank : 0), 0) / n
  };
}

function pct(value) {
  return `${(value * 100).toFixed(1)}%`;
}

const docs = await readJsonLines(resolve(args.docs));
const server = await serveStatic(args.root);
try {
  const engine = await createSearch({ baseUrl: new URL(args.basePath, server.url) });
  const known = knownQueries(docs, args.known);
  const typo = typoQueries(known, args.typos);

  const knownRows = [];
  for (const target of known) {
    const response = await engine.search({ q: target.q, size: args.size });
    knownRows.push({ ...target, rank: rankIn(response.results, target), total: response.total });
  }

  const typoRows = [];
  for (const target of typo) {
    const response = await engine.search({ q: target.typoQ, size: args.size });
    typoRows.push({
      ...target,
      rank: rankIn(response.results, target),
      total: response.total,
      correctedQuery: response.correctedQuery || null,
      typoApplied: !!response.stats?.typoApplied
    });
  }

  const report = {
    engine: "rangefind",
    knownItem: {
      metrics: metrics(knownRows.map(row => row.rank)),
      misses: knownRows.filter(row => !row.rank).slice(0, 10)
    },
    typoRecovery: {
      metrics: metrics(typoRows.map(row => row.rank)),
      appliedRate: typoRows.length ? typoRows.filter(row => row.typoApplied).length / typoRows.length : 0,
      examples: typoRows.filter(row => !row.rank || !row.typoApplied).slice(0, 10)
    }
  };

  if (args.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log("# Rangefind quality benchmark\n");
    const k = report.knownItem.metrics;
    console.log(`Known item: n=${k.n} Hit@1 ${pct(k.hit1)} Hit@3 ${pct(k.hit3)} Hit@10 ${pct(k.hit10)} MRR@10 ${k.mrr10.toFixed(3)}`);
    const t = report.typoRecovery.metrics;
    console.log(`Typo recovery: n=${t.n} Hit@1 ${pct(t.hit1)} Hit@3 ${pct(t.hit3)} Hit@10 ${pct(t.hit10)} MRR@10 ${t.mrr10.toFixed(3)} Applied ${pct(report.typoRecovery.appliedRate)}`);
  }
} finally {
  await server.close();
}
