// Benchmark for the link-graph authority signal.
//
//   node scripts/link_rank_bench.mjs [--nodes=20000] [--avg-out=12] [--pages=20000]
//
// Reports two things:
//   1. Build-time PageRank throughput over a synthetic link graph — the cost the
//      crawler pays once per build to materialize `linkRank`.
//   2. The html_extract `stripNonContent` speedup from hoisting the raw-text
//      regexes out of the per-document hot path (this change), measured against
//      an inline recompiling variant on identical input.
//
// Deterministic: a small LCG stands in for Math.random(), which the build tools
// forbid, so runs are comparable.

import { pageRank } from "../src/link_graph.js";
import { extractHtml } from "../src/html_extract.js";

function parseArg(name, fallback) {
  const hit = process.argv.find(arg => arg.startsWith(`--${name}=`));
  return hit ? Number(hit.slice(name.length + 3)) : fallback;
}

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function ms(nanos) {
  return (Number(nanos) / 1e6).toFixed(1);
}

const NODES = parseArg("nodes", 20000);
const AVG_OUT = parseArg("avg-out", 12);
const PAGES = parseArg("pages", 20000);

// --- 1. PageRank throughput ------------------------------------------------
const rand = lcg(0x9e3779b9);
const adjacency = new Array(NODES);
let edges = 0;
for (let i = 0; i < NODES; i++) {
  const deg = Math.floor(rand() * AVG_OUT * 2);
  const row = new Array(deg);
  for (let k = 0; k < deg; k++) row[k] = Math.floor(rand() * NODES);
  adjacency[i] = row;
  edges += deg;
}

// Warm, then measure.
pageRank(adjacency, { iterations: 5 });
const prStart = process.hrtime.bigint();
const ranks = pageRank(adjacency);
const prEnd = process.hrtime.bigint();
let checksum = 0;
for (let i = 0; i < NODES; i++) checksum += ranks[i];

console.log("PageRank");
console.log(`  nodes=${NODES} edges=${edges} (avg out-degree ${(edges / NODES).toFixed(1)})`);
console.log(`  time=${ms(prEnd - prStart)}ms  sum=${checksum.toFixed(6)} (expect ~1)`);

// --- 2. stripNonContent regex hoist ---------------------------------------
const RAWTEXT_TAGS = ["script", "style", "template", "noscript"];
const HOISTED = RAWTEXT_TAGS.map(tag => ({
  paired: new RegExp(`<${tag}\\b[^>]*>[\\s\\S]*?<\\/${tag}\\s*>`, "gi"),
  dangling: new RegExp(`<${tag}\\b[^>]*>[\\s\\S]*$`, "i")
}));

function stripHoisted(html) {
  let out = html.replace(/<!--[\s\S]*?-->/g, " ").replace(/<!\[CDATA\[[\s\S]*?\]\]>/g, " ");
  for (const p of HOISTED) out = out.replace(p.paired, " ").replace(p.dangling, " ");
  return out;
}

function stripInline(html) {
  let out = html.replace(/<!--[\s\S]*?-->/g, " ").replace(/<!\[CDATA\[[\s\S]*?\]\]>/g, " ");
  for (const tag of RAWTEXT_TAGS) {
    out = out.replace(new RegExp(`<${tag}\\b[^>]*>[\\s\\S]*?<\\/${tag}\\s*>`, "gi"), " ");
    out = out.replace(new RegExp(`<${tag}\\b[^>]*>[\\s\\S]*$`, "i"), " ");
  }
  return out;
}

const sampleHtml = `<!doctype html><html lang="en"><head><title>Sample Page</title>
<style>.a{color:red}</style><script>var x = 1 < 2 && 3 > 1;</script></head>
<body><nav><a href="/">home</a></nav><main><h1>Heading</h1>
<p>Body text with a <a href="/next">link</a> and more words to tokenize.</p>
<noscript>enable js</noscript></main></body></html>`;

function timeStrip(fn) {
  fn(sampleHtml); // warm
  const start = process.hrtime.bigint();
  let sink = 0;
  for (let i = 0; i < PAGES; i++) sink += fn(sampleHtml).length;
  const end = process.hrtime.bigint();
  return { time: end - start, sink };
}

const inline = timeStrip(stripInline);
const hoisted = timeStrip(stripHoisted);
const speedup = (Number(inline.time) / Number(hoisted.time)).toFixed(2);

console.log("\nstripNonContent (per-document raw-text stripping)");
console.log(`  pages=${PAGES}`);
console.log(`  inline recompile : ${ms(inline.time)}ms`);
console.log(`  hoisted regexes  : ${ms(hoisted.time)}ms`);
console.log(`  speedup          : ${speedup}x`);

// Also exercise the real extractHtml path so the number reflects end-to-end.
extractHtml(sampleHtml);
const exStart = process.hrtime.bigint();
let bodyLen = 0;
for (let i = 0; i < PAGES; i++) bodyLen += extractHtml(sampleHtml).body.length;
const exEnd = process.hrtime.bigint();
console.log(`\nextractHtml end-to-end: ${ms(exEnd - exStart)}ms for ${PAGES} pages ` +
  `(${((exEnd - exStart) / BigInt(PAGES) / 1000n)}µs/page)`);
