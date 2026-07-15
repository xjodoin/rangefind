// Link-graph authority on a real Wikipedia corpus, no download required.
//
//   node scripts/wiki_linkrank_bench.mjs [--scale=50000] [--top=25] [--max-title-words=4]
//
// The cached frwiki JSONL (examples/frwiki/scale/<N>/data/frwiki.jsonl) has its
// [[wikilinks]] stripped to plain text, so we reconstruct the link graph by
// title-mention detection: an article that names another article's title is
// treated as linking to it (a standard "wikification" approximation). We then
// run the same PageRank used at build time and report the most authoritative
// articles — a real sanity check that linkRank surfaces central topics — plus
// timing for the graph build at scale.

import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { resolve } from "node:path";
import { pageRank, normalizeRanks } from "../src/link_graph.js";

function arg(name, fallback) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? Number(hit.slice(name.length + 3)) : fallback;
}
const SCALE = arg("scale", 50000);
const TOP = arg("top", 25);
const MAX_TITLE_WORDS = arg("max-title-words", 4);
const ms = ns => Number(ns) / 1e6;

const path = resolve(`examples/frwiki/scale/${SCALE}/data/frwiki.jsonl`);

async function* records() {
  const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    try {
      yield JSON.parse(line);
    } catch {
      // The fixture may split records oddly on rare lines; skip them.
    }
  }
}

// A phrase is "title-like" only if it starts with an uppercase letter — this
// cuts candidate n-grams by an order of magnitude and matches how article
// titles (proper nouns) appear verbatim in the stripped body text.
function startsUpper(token) {
  const c = token.charCodeAt(0);
  return (c >= 65 && c <= 90) || c > 127; // ASCII A-Z or any accented/non-latin
}
function clean(token) {
  return token.replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, "");
}

async function main() {
  console.log(`Wikipedia linkRank bench — frwiki scale ${SCALE}`);
  console.log(`  ${path}`);

  // Pass 1: titles -> ordinals.
  const titleToOrdinal = new Map();
  const titles = [];
  const p1 = process.hrtime.bigint();
  for await (const rec of records()) {
    const title = String(rec.title || "");
    if (!title) continue;
    if (!titleToOrdinal.has(title)) {
      titleToOrdinal.set(title, titles.length);
      titles.push(title);
    }
  }
  const n = titles.length;
  console.log(`  pass 1: ${n} titles indexed in ${(ms(process.hrtime.bigint() - p1) / 1000).toFixed(1)}s`);

  // Pass 2: scan bodies, detect mentions of other titles, build adjacency.
  const adjacency = new Array(n);
  for (let i = 0; i < n; i++) adjacency[i] = [];
  let edges = 0;
  let ordinal = 0;
  const p2 = process.hrtime.bigint();
  for await (const rec of records()) {
    if (!rec.title) continue;
    const self = titleToOrdinal.get(String(rec.title));
    if (self !== ordinal) { ordinal = self; } // keep alignment even on dupes
    const tokens = String(rec.body || "").split(/\s+/);
    const seen = new Set();
    const row = adjacency[self];
    for (let i = 0; i < tokens.length; i++) {
      if (!tokens[i] || !startsUpper(tokens[i])) continue;
      // Longest-match: try the widest title phrase first.
      for (let len = Math.min(MAX_TITLE_WORDS, tokens.length - i); len >= 1; len--) {
        const phrase = clean(tokens.slice(i, i + len).join(" "));
        const target = titleToOrdinal.get(phrase);
        if (target !== undefined && target !== self && !seen.has(target)) {
          seen.add(target);
          row.push(target);
          i += len - 1;
          break;
        }
      }
    }
    edges += row.length;
    ordinal++;
  }
  console.log(`  pass 2: ${edges} mention edges in ${(ms(process.hrtime.bigint() - p2) / 1000).toFixed(1)}s`
    + `  (avg out-degree ${(edges / n).toFixed(1)})`);

  // PageRank — the exact computation the builder ships.
  const prStart = process.hrtime.bigint();
  const linkRank = normalizeRanks(pageRank(adjacency));
  console.log(`  PageRank: ${(ms(process.hrtime.bigint() - prStart) / 1000).toFixed(2)}s\n`);

  const order = Array.from({ length: n }, (_, i) => i).sort((a, b) => linkRank[b] - linkRank[a]);
  console.log(`Top ${TOP} articles by linkRank authority:`);
  for (let r = 0; r < Math.min(TOP, n); r++) {
    const i = order[r];
    console.log(`  ${String(r + 1).padStart(3)}. ${linkRank[i].toFixed(4)}  ${titles[i]}`);
  }
}

main();
