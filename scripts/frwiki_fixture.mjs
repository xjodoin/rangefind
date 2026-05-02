#!/usr/bin/env node

import { createWriteStream, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync, copyFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createSearch } from "../src/runtime.js";
import { build } from "../src/builder.js";
import {
  createFetchMeter,
  dirStats,
  kb,
  mean,
  quantile,
  serveStatic
} from "./bench_support.mjs";

const DEFAULT_DUMP_URL = "https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-pages-articles.xml.bz2";
const FRWIKI_SCHEMA_VERSION = 3;
const DEFAULT_QUERIES = [
  "Paris",
  "Révolution française",
  "intelligence artificielle",
  "Victor Hugo",
  "football",
  "médecine",
  "changement climatique",
  "fromage",
  "Québec",
  "Napoléon Bonaparte"
];

function parseArgs(argv) {
  const args = {
    command: argv[0] || "all",
    dumpUrl: process.env.FRWIKI_DUMP_URL || DEFAULT_DUMP_URL,
    limit: Number(process.env.FRWIKI_LIMIT || 5000),
    root: "examples/frwiki",
    queries: DEFAULT_QUERIES,
    runs: 3,
    size: 10,
    bodyChars: Number(process.env.FRWIKI_BODY_CHARS || 6000),
    force: false,
    exactChecks: process.env.FRWIKI_EXACT_CHECKS !== "0",
    reduceWorkers: process.env.RANGEFIND_REDUCE_WORKERS || ""
  };
  for (const arg of argv.slice(1)) {
    if (arg === "--force") args.force = true;
    else if (arg.startsWith("--dump-url=")) args.dumpUrl = arg.slice("--dump-url=".length);
    else if (arg.startsWith("--limit=")) args.limit = Number(arg.slice("--limit=".length)) || 0;
    else if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--queries=")) args.queries = arg.slice("--queries=".length).split("|").filter(Boolean);
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--size=")) args.size = Number(arg.slice("--size=".length)) || args.size;
    else if (arg.startsWith("--body-chars=")) args.bodyChars = Number(arg.slice("--body-chars=".length)) || 0;
    else if (arg.startsWith("--reduce-workers=")) args.reduceWorkers = arg.slice("--reduce-workers=".length);
    else if (arg.startsWith("--scale-limits=")) args.scaleLimits = arg.slice("--scale-limits=".length).split(",").map(value => Number(value.trim())).filter(Boolean);
    else if (arg === "--exact-checks") args.exactChecks = true;
    else if (arg === "--no-exact-checks") args.exactChecks = false;
  }
  args.scaleLimits ||= [10000, 25000, 50000, 100000];
  return args;
}

function decodeXml(value) {
  return String(value || "")
    .replace(/&lt;/gu, "<")
    .replace(/&gt;/gu, ">")
    .replace(/&amp;/gu, "&")
    .replace(/&quot;/gu, "\"")
    .replace(/&#039;|&apos;/gu, "'");
}

function tag(page, name) {
  const match = new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)</${name}>`, "u").exec(page);
  return decodeXml(match?.[1] || "");
}

function titleToUrl(title) {
  return `https://fr.wikipedia.org/wiki/${encodeURIComponent(title.replaceAll(" ", "_"))}`;
}

function categoriesFromWikitext(text) {
  const out = [];
  const re = /\[\[\s*Cat[ée]gorie\s*:\s*([^\]|#]+)(?:[^\]]*)\]\]/giu;
  let match;
  while ((match = re.exec(text))) {
    const value = match[1].trim();
    if (value && out.length < 8) out.push(value);
  }
  return out;
}

function articleTags(title, body, categories) {
  return [
    categories.length ? "has-categories" : "uncategorized",
    categories.length >= 4 ? "many-categories" : "few-categories",
    body.length >= 5000 ? "long-body" : "short-body",
    title.length >= 24 ? "long-title" : "short-title"
  ];
}

function stripWikitext(text) {
  return String(text || "")
    .replace(/<ref\b[\s\S]*?<\/ref>/giu, " ")
    .replace(/<ref\b[^/]*\/>/giu, " ")
    .replace(/<!--[\s\S]*?-->/gu, " ")
    .replace(/\{\{[\s\S]{0,2000}?\}\}/gu, " ")
    .replace(/\{\|[\s\S]*?\|\}/gu, " ")
    .replace(/\[\[Fichier:[^\]]+\]\]/giu, " ")
    .replace(/\[\[Image:[^\]]+\]\]/giu, " ")
    .replace(/\[\[Cat[ée]gorie:[^\]]+\]\]/giu, " ")
    .replace(/\[\[[^\]|]+\|([^\]]+)\]\]/gu, "$1")
    .replace(/\[\[([^\]]+)\]\]/gu, "$1")
    .replace(/\[https?:\/\/[^\s\]]+\s*([^\]]*)\]/gu, "$1")
    .replace(/'{2,}/gu, "")
    .replace(/={2,}\s*([^=]+?)\s*={2,}/gu, " $1 ")
    .replace(/<[^>]+>/gu, " ")
    .replace(/[{}[\]|]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

function pageToDoc(page, index, args) {
  const ns = tag(page, "ns");
  if (ns && ns !== "0") return null;
  const title = tag(page, "title").trim();
  const id = tag(page, "id").trim() || String(index + 1);
  const redirect = /<redirect\b/iu.test(page);
  const raw = tag(page, "text");
  const timestamp = tag(page, "timestamp");
  let body = stripWikitext(raw);
  if (args.bodyChars > 0 && body.length > args.bodyChars) body = body.slice(0, args.bodyChars);
  if (!title || redirect || body.length < 80) return null;
  const categories = categoriesFromWikitext(raw);
  const revisionTime = Date.parse(timestamp);
  return {
    id,
    articleId: Number(id) || index + 1,
    title,
    titleLength: title.length,
    url: titleToUrl(title),
    body,
    bodyLength: body.length,
    categories: categories.join(" "),
    categoryList: categories,
    articleTags: articleTags(title, body, categories),
    category: categories[0] || "",
    categoryCount: categories.length,
    hasCategories: categories.length > 0,
    revisionDate: Number.isFinite(revisionTime) ? new Date(revisionTime).toISOString().slice(0, 10) : "",
    source: "frwiki"
  };
}

function sourceCommand(url) {
  if (/^https?:\/\//u.test(url)) return { cmd: "curl", args: ["-L", "--fail", "--silent", "--show-error", url] };
  return { cmd: "cat", args: [url] };
}

function decompressorCommand(url) {
  if (url.endsWith(".bz2")) return { cmd: "bzip2", args: ["-dc"] };
  if (url.endsWith(".gz")) return { cmd: "gzip", args: ["-dc"] };
  return null;
}

function waitForChild(child, name, allowSignal = false) {
  return new Promise((resolveWait, rejectWait) => {
    child.on("error", rejectWait);
    child.on("close", (code, signal) => {
      if (code === 0 || (allowSignal && signal)) resolveWait();
      else rejectWait(new Error(`${name} exited with code ${code}${signal ? ` signal ${signal}` : ""}`));
    });
  });
}

function expectedMeta(args) {
  return {
    schemaVersion: FRWIKI_SCHEMA_VERSION,
    dumpUrl: args.dumpUrl,
    limit: args.limit || null,
    bodyChars: args.bodyChars || null
  };
}

function jsonlMatchesRun(args, out, metaPath) {
  if (!existsSync(out) || !existsSync(metaPath)) return false;
  try {
    const meta = JSON.parse(readFileSync(metaPath, "utf8"));
    const expected = expectedMeta(args);
    return meta.schemaVersion === expected.schemaVersion
      && meta.dumpUrl === expected.dumpUrl
      && (meta.limit ?? null) === expected.limit
      && (meta.bodyChars ?? null) === expected.bodyChars;
  } catch {
    return false;
  }
}

async function writeJsonl(args) {
  const dataDir = resolve(args.root, "data");
  const out = resolve(dataDir, "frwiki.jsonl");
  const metaPath = resolve(dataDir, "frwiki.meta.json");
  if (!args.force && jsonlMatchesRun(args, out, metaPath)) return out;
  mkdirSync(dataDir, { recursive: true });

  const sourceSpec = sourceCommand(args.dumpUrl);
  const source = spawn(sourceSpec.cmd, sourceSpec.args, { stdio: ["ignore", "pipe", "inherit"] });
  const sourceDone = waitForChild(source, sourceSpec.cmd, true);
  const decompSpec = decompressorCommand(args.dumpUrl);
  const stream = decompSpec
    ? spawn(decompSpec.cmd, decompSpec.args, { stdio: ["pipe", "pipe", "inherit"] })
    : null;
  const streamDone = stream ? waitForChild(stream, decompSpec.cmd, true) : Promise.resolve();
  if (stream) source.stdout.pipe(stream.stdin);
  source.stdout.on("error", () => {});
  if (stream) {
    stream.stdin.on("error", () => {});
    stream.stdout.on("error", () => {});
  }
  const input = stream ? stream.stdout : source.stdout;
  input.setEncoding("utf8");

  const output = createWriteStream(out);
  let buffer = "";
  let docs = 0;
  let pages = 0;
  const started = performance.now();

  async function finish() {
    await new Promise(resolveFinish => output.end(resolveFinish));
    await Promise.allSettled([sourceDone, streamDone]);
    writeFileSync(metaPath, JSON.stringify({
      ...expectedMeta(args),
      docs,
      pagesRead: pages,
      builtAt: new Date().toISOString()
    }, null, 2));
    return out;
  }

  for await (const chunk of input) {
    buffer += chunk;
    while (true) {
      const start = buffer.indexOf("<page>");
      const end = buffer.indexOf("</page>");
      if (start < 0 || end < start) {
        if (start > 0) buffer = buffer.slice(start);
        break;
      }
      const page = buffer.slice(start, end + "</page>".length);
      buffer = buffer.slice(end + "</page>".length);
      pages++;
      const doc = pageToDoc(page, docs, args);
      if (!doc) continue;
      output.write(`${JSON.stringify(doc)}\n`);
      docs++;
      if (docs % 1000 === 0) {
        const seconds = (performance.now() - started) / 1000;
        console.error(`frwiki: ${docs.toLocaleString()} docs from ${pages.toLocaleString()} pages (${(docs / Math.max(1, seconds)).toFixed(0)} docs/s)`);
      }
      if (args.limit && docs >= args.limit) {
        input.destroy();
        source.kill("SIGTERM");
        if (stream) stream.kill("SIGTERM");
        return finish();
      }
    }
  }

  return finish();
}

function writeSite(args, docsPath) {
  const root = resolve(args.root);
  const publicDir = resolve(root, "public");
  mkdirSync(publicDir, { recursive: true });
  copyFileSync(resolve("dist/runtime.browser.js"), resolve(publicDir, "runtime.browser.js"));
  const configPath = resolve(root, "rangefind.config.json");
  const config = {
    input: docsPath,
    output: "public/rangefind",
    idPath: "id",
    urlPath: "url",
    maxTermsPerDoc: 180,
    maxExpansionTermsPerDoc: 8,
    targetShardPostings: 45000,
    reduceWorkers: args.reduceWorkers ? (args.reduceWorkers === "auto" ? "auto" : Number(args.reduceWorkers)) : 1,
    fields: [
      { name: "title", path: "title", weight: 5.5, b: 0.25, phrase: true, proximity: true, proximityWeight: 3, proximityWindow: 5 },
      { name: "categories", path: "categories", weight: 2.0, b: 0.0 },
      { name: "body", path: "body", weight: 1.0, b: 0.75, typo: false }
    ],
    facets: [
      { name: "category", path: "category" },
      { name: "articleTags", path: "articleTags" }
    ],
    numbers: [
      { name: "articleId", path: "articleId", type: "int" },
      { name: "titleLength", path: "titleLength", type: "int" },
      { name: "bodyLength", path: "bodyLength", type: "int" },
      { name: "categoryCount", path: "categoryCount", type: "int" },
      { name: "revisionDate", path: "revisionDate", type: "date" }
    ],
    booleans: [
      { name: "hasCategories", path: "hasCategories" }
    ],
    display: [
      "id",
      "articleId",
      "title",
      "titleLength",
      "url",
      { name: "body", path: "body", maxChars: 640 },
      "bodyLength",
      "category",
      "categoryList",
      "articleTags",
      "categoryCount",
      "hasCategories",
      "revisionDate"
    ]
  };
  writeFileSync(configPath, JSON.stringify(config, null, 2));
  writeFileSync(resolve(publicDir, "index.html"), `<!doctype html>
<meta charset="utf-8">
<title>Rangefind French Wikipedia scalability fixture</title>
<style>
body{font:16px/1.45 system-ui,sans-serif;margin:24px;max-width:980px}
form{display:flex;gap:8px;margin:0 0 18px}
input{flex:1;font:inherit;padding:10px}
button{font:inherit;padding:10px 14px}
article{border-top:1px solid #ddd;padding:14px 0}
h1{font-size:24px} h2{font-size:18px;margin:0 0 4px}
small{color:#666}.body{color:#333}
</style>
<h1>Rangefind French Wikipedia scalability fixture</h1>
<form id="form"><input id="q" value="Révolution française"><button>Search</button></form>
<div id="meta"></div>
<div id="out"></div>
<script type="module">
import { createSearch } from "./runtime.browser.js";
const engine = await createSearch({ baseUrl: "./rangefind/" });
const q = document.querySelector("#q");
const out = document.querySelector("#out");
const meta = document.querySelector("#meta");
async function run(){
  const t0 = performance.now();
  const res = await engine.search({ q: q.value, size: 10 });
  meta.textContent = res.total + " results in " + (performance.now() - t0).toFixed(1) + " ms";
  out.innerHTML = res.results.map(item => '<article><h2><a href="'+item.url+'">'+item.title+'</a></h2><small>'+ (item.category || '') +' · '+ (item.revisionDate || '') +' · '+ (item.bodyLength || 0) +' chars · score '+ item.score +'</small><p class="body">'+(item.body || '').slice(0, 420)+'</p></article>').join("");
}
document.querySelector("#form").addEventListener("submit", event => { event.preventDefault(); run(); });
run();
</script>
`);
  return configPath;
}

async function buildFixture(args) {
  const docsPath = resolve(args.root, "data", "frwiki.jsonl");
  const configPath = writeSite(args, docsPath);
  await build({ configPath });
}

function networkBucket(url) {
  const path = new URL(url).pathname;
  if (path.endsWith("/runtime.browser.js")) return "runtime";
  if (/\/manifest(?:\.[0-9a-f]+)?\.json$/u.test(path)) return "manifest";
  if (path.includes("/directory-")) return "directory";
  if (path.includes("/bundles/packs/")) return "queryBundles";
  if (path.includes("/terms/block-packs/")) return "postingBlocks";
  if (path.includes("/terms/packs/")) return "terms";
  if (path.includes("/facets/packs/")) return "facetDictionaries";
  if (path.includes("/doc-values/sorted")) return "docValueSorted";
  if (path.includes("/doc-values/")) return "docValues";
  if (path.includes("/docs/ordinals/")) return "docOrdinals";
  if (path.includes("/docs/pointers/")) return "docPointers";
  if (path.includes("/docs/pages/")) return "docPagePointers";
  if (path.includes("/docs/page-packs/")) return "docPages";
  if (path.includes("/typo/")) return "typo";
  if (path.includes("/docs/")) return "docs";
  if (path.endsWith("/codes.bin.gz")) return "codes";
  return "other";
}

function networkKbBy(snapshot) {
  return Object.fromEntries(Object.entries(snapshot.by || {}).map(([bucket, value]) => [bucket, {
    requests: value.requests,
    kb: kb(value.bytes)
  }]));
}

function fieldType(manifest, field) {
  return (manifest.numbers || []).find(item => item.name === field)?.type || "number";
}

function normalizeComparable(value, type = "number") {
  if (value == null || value === "") return null;
  if (type === "date") {
    const time = typeof value === "number" ? value : Date.parse(String(value));
    return Number.isFinite(time) ? time : null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function commonFacetValue(manifest, field) {
  const values = Array.isArray(manifest.facets?.[field]) ? manifest.facets[field] : [];
  return values
    .filter(item => item.value)
    .sort((a, b) => (b.n || 0) - (a.n || 0))[0]?.value || null;
}

function typedCases(manifest) {
  const cases = [
    {
      label: "typed dates sorted",
      category: "sort",
      q: "",
      filters: {
        numbers: {
          titleLength: { min: 1 },
          bodyLength: { min: 80 },
          revisionDate: { min: "2001-01-01" }
        }
      },
      sort: { field: "bodyLength", order: "desc" },
      expect: { docValuePruning: true, docPayloadLane: "docPages" }
    },
    {
      label: "dense filter browse",
      category: "filter",
      q: "",
      filters: {
        numbers: {
          bodyLength: { min: 80 }
        }
      },
      expect: { docPayloadLane: "docPages" }
    },
    {
      label: "selective long-body browse",
      category: "filter",
      q: "",
      filters: {
        numbers: {
          bodyLength: { min: 5000 }
        }
      },
      expect: { totalMin: 1, docPayloadLane: "docPages" }
    },
    {
      label: "recent article sort",
      category: "sort",
      q: "",
      filters: {
        numbers: {
          revisionDate: { min: "2001-01-01" }
        }
      },
      sort: { field: "revisionDate", order: "desc" },
      expect: { docValuePruning: true }
    }
  ];
  const tagValues = Array.isArray(manifest.facets?.articleTags) ? manifest.facets.articleTags : [];
  const tag = !tagValues.length || tagValues.some(item => item.value === "has-categories")
    ? "has-categories"
    : commonFacetValue(manifest, "articleTags");
  if (tag) {
    const expectsCategories = tag !== "uncategorized";
    cases.push({
      label: `multi facet boolean (${tag})`,
      category: "facet-boolean-sort",
      q: "",
      filters: {
        facets: { articleTags: [tag] },
        numbers: { categoryCount: { min: expectsCategories ? 1 : 0 } },
        booleans: { hasCategories: expectsCategories }
      },
      sort: { field: "articleId", order: "asc" },
      expect: { docValuePruning: true, docPayloadLane: "docPages" }
    });
    cases.push({
      label: `facet browse (${tag})`,
      category: "facet",
      q: "",
      filters: {
        facets: { articleTags: [tag] }
      },
      expect: { totalMin: 1, docPayloadLane: "docPages" }
    });
  }
  const category = commonFacetValue(manifest, "category");
  if (category) {
    cases.push({
      label: `category facet (${category})`,
      category: "facet",
      q: "",
      filters: {
        facets: { category: [category] }
      },
      expect: { totalMin: 1, docPayloadLane: "docPages" }
    });
  }
  return cases;
}

function textScenarioCases(args) {
  const base = args.queries.map(q => ({ label: q, category: "text", q }));
  return [
    ...base,
    {
      label: "changement climatique page 2",
      category: "pagination",
      q: "changement climatique",
      page: 2,
      expect: { totalMin: 11, topKProven: true }
    },
    {
      label: "Révolution française size 25",
      category: "large-page",
      q: "Révolution française",
      size: 25,
      expect: { totalMin: 25, topKProven: true }
    },
    {
      label: "zero result nonsense",
      category: "zero-result",
      q: "zzzzrangefindaucunresultatzzzz",
      exactCheck: true,
      expect: { totalMax: 0, plannerLane: "empty", topKProven: true }
    },
    {
      label: "typo Paris",
      category: "typo",
      q: "Pxris",
      exactCheck: false,
      expect: { totalMin: 1, typoApplied: true }
    },
    {
      label: "typo changement climatique",
      category: "typo",
      q: "changment climatiqe",
      exactCheck: false,
      expect: { totalMin: 1, typoApplied: true }
    },
    {
      label: "filtered Paris has categories",
      category: "text-filter",
      q: "Paris",
      filters: {
        facets: { articleTags: ["has-categories"] }
      },
      expect: { totalMin: 1, topKProven: true }
    },
    {
      label: "filtered changement climatique long body",
      category: "text-filter",
      q: "changement climatique",
      filters: {
        numbers: { bodyLength: { min: 1000 } }
      },
      expect: { totalMin: 1, topKProven: true }
    },
    {
      label: "Paris sorted by revision date",
      category: "text-sort",
      q: "Paris",
      sort: { field: "revisionDate", order: "desc" },
      exactCheck: false,
      expect: { totalMin: 1 }
    },
    {
      label: "common term rerank disabled",
      category: "no-rerank",
      q: "Paris",
      options: { rerank: false },
      expect: { totalMin: 1, topKProven: true }
    }
  ];
}

function benchmarkCases(args, manifest) {
  return [
    ...textScenarioCases(args),
    ...typedCases(manifest)
  ];
}

function resultMatchesFacet(result, field, selected) {
  const value = result[field];
  if (Array.isArray(value)) return value.some(item => selected.includes(item));
  return selected.some(item => String(value || "").includes(item));
}

function validateResponse(item, response, manifest) {
  const errors = [];
  for (const result of response.results) {
    for (const [field, selected] of Object.entries(item.filters?.facets || {})) {
      if (!resultMatchesFacet(result, field, selected)) errors.push(`${field} missing selected facet`);
    }
    for (const [field, range] of Object.entries(item.filters?.numbers || {})) {
      const type = fieldType(manifest, field);
      const value = normalizeComparable(result[field], type);
      const min = normalizeComparable(range.min, type);
      const max = normalizeComparable(range.max, type);
      if (value == null) errors.push(`${field} missing numeric value`);
      if (min != null && value < min) errors.push(`${field} below min`);
      if (max != null && value > max) errors.push(`${field} above max`);
    }
    for (const [field, expected] of Object.entries(item.filters?.booleans || {})) {
      if (Boolean(result[field]) !== Boolean(expected)) errors.push(`${field} boolean mismatch`);
    }
  }
  const sort = item.sort;
  if (sort?.field && response.results.length > 1) {
    const type = fieldType(manifest, sort.field);
    const values = response.results.map(result => normalizeComparable(result[sort.field], type)).filter(value => value != null);
    for (let i = 1; i < values.length; i++) {
      if (sort.order === "desc" ? values[i - 1] < values[i] : values[i - 1] > values[i]) {
        errors.push(`${sort.field} sort order mismatch`);
        break;
      }
    }
  }
  const expect = item.expect || {};
  if (expect.totalMin != null && response.total < expect.totalMin) errors.push(`total below expected minimum ${expect.totalMin}`);
  if (expect.totalMax != null && response.total > expect.totalMax) errors.push(`total above expected maximum ${expect.totalMax}`);
  if (expect.plannerLane && response.stats?.plannerLane !== expect.plannerLane) errors.push(`planner lane expected ${expect.plannerLane}`);
  if (expect.docPayloadLane && response.stats?.docPayloadLane !== expect.docPayloadLane) errors.push(`doc payload lane expected ${expect.docPayloadLane}`);
  for (const field of ["queryBundleHit", "typoApplied", "typoAttempted", "docValuePruning", "topKProven"]) {
    if (expect[field] != null && Boolean(response.stats?.[field]) !== Boolean(expect[field])) {
      errors.push(`${field} expected ${Boolean(expect[field])}`);
    }
  }
  return [...new Set(errors)];
}

function compactRuntimeStats(stats = {}) {
  return {
    exact: Boolean(stats.exact),
    plannerLane: stats.plannerLane || "",
    topKProven: Boolean(stats.topKProven),
    totalExact: Boolean(stats.totalExact),
    tailExhausted: Boolean(stats.tailExhausted),
    blocksDecoded: stats.blocksDecoded || 0,
    postingsDecoded: stats.postingsDecoded || 0,
    postingsAccepted: stats.postingsAccepted || 0,
    skippedBlocks: stats.skippedBlocks || 0,
    terms: stats.terms || 0,
    shards: stats.shards || 0,
    postingBlockFrontier: stats.postingBlockFrontier || 0,
    postingBlockFrontierBatches: stats.postingBlockFrontierBatches || 0,
    postingBlockFrontierBlocks: stats.postingBlockFrontierBlocks || 0,
    postingBlockFrontierMax: stats.postingBlockFrontierMax || 0,
    postingBlockFrontierFetchedBlocks: stats.postingBlockFrontierFetchedBlocks || 0,
    postingBlockFrontierFetchGroups: stats.postingBlockFrontierFetchGroups || 0,
    postingBlockFrontierWantedBlocks: stats.postingBlockFrontierWantedBlocks || 0,
    rerankCandidates: stats.rerankCandidates || 0,
    dependencyFeatures: stats.dependencyFeatures || 0,
    dependencyTermsMatched: stats.dependencyTermsMatched || 0,
    dependencyPostingsScanned: stats.dependencyPostingsScanned || 0,
    dependencyCandidateMatches: stats.dependencyCandidateMatches || 0,
    docPayloadLane: stats.docPayloadLane || "",
    docPayloadPages: stats.docPayloadPages || 0,
    docPayloadOverfetchDocs: stats.docPayloadOverfetchDocs || 0,
    docPayloadAdaptive: Boolean(stats.docPayloadAdaptive),
    docValuePruning: Boolean(stats.docValuePruning),
    docValuePruneField: stats.docValuePruneField || "",
    docValueDirectoryPages: stats.docValueDirectoryPages || 0,
    docValueCandidatePages: stats.docValueCandidatePages || 0,
    docValuePagesPruned: stats.docValuePagesPruned || 0,
    docValuePagesVisited: stats.docValuePagesVisited || 0,
    docValueRowsScanned: stats.docValueRowsScanned || 0,
    docValueRowsAccepted: stats.docValueRowsAccepted || 0,
    docValueDefinitePages: stats.docValueDefinitePages || 0,
    docValueChunkPruning: Boolean(stats.docValueChunkPruning),
    docValueChunksVisited: stats.docValueChunksVisited || 0,
    docValueChunksPruned: stats.docValueChunksPruned || 0,
    queryBundleLookups: stats.queryBundleLookups || 0,
    queryBundleHit: Boolean(stats.queryBundleHit),
    queryBundleRows: stats.queryBundleRows || 0,
    queryBundleTotal: stats.queryBundleTotal || 0,
    queryBundleBytes: stats.queryBundleBytes || 0,
    queryBundleComplete: Boolean(stats.queryBundleComplete),
    surfaceFallbackAttempted: Boolean(stats.surfaceFallbackAttempted),
    surfaceFallbackApplied: Boolean(stats.surfaceFallbackApplied),
    surfaceFallbackTerms: stats.surfaceFallbackTerms || [],
    typoAttempted: Boolean(stats.typoAttempted),
    typoApplied: Boolean(stats.typoApplied),
    typoSkippedReason: stats.typoSkippedReason || ""
  };
}

function sameIds(left, right) {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

async function addExactChecks(args, serverUrl, rows, cases) {
  if (!args.exactChecks) return;
  const exactEngine = await createSearch({ baseUrl: new URL("rangefind/", serverUrl) });
  const rowsByLabel = new Map(rows.map(row => [row.q, row]));
  for (const item of cases) {
    if (item.exactCheck === false || !item.q || item.sort) continue;
    const row = rowsByLabel.get(item.label);
    if (!row) continue;
    const start = performance.now();
    const response = await exactEngine.search({
      q: item.q,
      page: item.page || 1,
      filters: item.filters,
      sort: item.sort,
      size: item.size || args.size,
      ...(item.options || {}),
      exact: true
    });
    const exactIds = response.results.map(result => result.id);
    const match = sameIds(row.resultIds, exactIds);
    row.exactTopKMatch = match;
    row.exactTotal = response.total;
    row.exactMs = performance.now() - start;
    row.exactStats = compactRuntimeStats(response.stats);
    if (!match) {
      row.valid = false;
      row.validationErrors = [...new Set([...(row.validationErrors || []), "exact top-k mismatch"])];
    }
  }
}

async function benchFixture(args, options = {}) {
  const root = resolve(args.root, "public");
  const server = await serveStatic(root);
  const meter = createFetchMeter(/\/(rangefind|runtime\.browser\.js)/u, networkBucket);
  try {
    meter.reset();
    const initStart = performance.now();
    const engine = await createSearch({ baseUrl: new URL("rangefind/", server.url) });
    const initMs = performance.now() - initStart;
    const initNetwork = meter.snapshot();
    const rows = [];
    const cases = benchmarkCases(args, engine.manifest);
    for (const item of cases) {
      const caseEngine = await createSearch({ baseUrl: new URL("rangefind/", server.url) });
      const times = [];
      const requests = [];
      const bytes = [];
      const networks = [];
      let total = 0;
      let top = "";
      let resultIds = [];
      let coldStats = {};
      let validationErrors = [];
      for (let i = 0; i < args.runs; i++) {
        meter.reset();
        const start = performance.now();
        const response = await caseEngine.search({
          q: item.q,
          page: item.page || 1,
          filters: item.filters,
          sort: item.sort,
          size: item.size || args.size,
          ...(item.options || {})
        });
        times.push(performance.now() - start);
        const network = meter.snapshot();
        networks.push(network);
        requests.push(network.requests);
        bytes.push(network.bytes);
        total = response.total;
        top = response.results[0]?.title || "";
        if (i === 0) {
          resultIds = response.results.map(result => result.id);
          coldStats = compactRuntimeStats(response.stats);
          validationErrors = validateResponse(item, response, caseEngine.manifest);
        }
      }
      rows.push({
        q: item.label,
        category: item.category || "default",
        request: {
          q: item.q,
          page: item.page || 1,
          size: item.size || args.size,
          filters: item.filters || null,
          sort: item.sort || null,
          options: item.options || null
        },
        total,
        top,
        resultIds,
        valid: validationErrors.length === 0,
        validationErrors,
        coldStats,
        coldMs: times[0] || 0,
        coldRequests: requests[0] || 0,
        coldKb: kb(bytes[0] || 0),
        coldBy: networkKbBy(networks[0] || {}),
        warmP50Ms: quantile(times.slice(1), 0.5),
        warmAvgRequests: mean(requests.slice(1)),
        warmAvgKb: kb(mean(bytes.slice(1))),
        p50Ms: quantile(times, 0.5),
        p95Ms: quantile(times, 0.95),
        avgRequests: mean(requests),
        avgKb: kb(mean(bytes))
      });
    }
    await addExactChecks(args, server.url, rows, cases);
    const report = {
      fixture: "frwiki",
      root,
      index: dirStats(resolve(root, "rangefind")),
      rangefindStats: engine.manifest.stats,
      docPages: engine.manifest.docs?.pages ? {
        format: engine.manifest.docs.pages.format,
        encoding: engine.manifest.docs.pages.encoding,
        pageSize: engine.manifest.docs.pages.page_size,
        fields: engine.manifest.docs.pages.fields?.length || 0,
        pointerBytes: engine.manifest.docs.pages.pointers?.bytes || 0,
        packFiles: engine.manifest.docs.pages.packs?.length || 0,
        packBytes: (engine.manifest.docs.pages.packs || []).reduce((sum, pack) => sum + (pack.bytes || 0), 0)
      } : null,
      docValueSorted: engine.manifest.doc_value_sorted ? {
        directoryFormat: engine.manifest.doc_value_sorted.directory_format,
        pageFormat: engine.manifest.doc_value_sorted.page_format,
        pageSize: engine.manifest.doc_value_sorted.page_size,
        fields: Object.keys(engine.manifest.doc_value_sorted.fields || {}).length,
        directoryBytes: engine.manifest.stats?.doc_value_sorted_directory_bytes || 0,
        packFiles: engine.manifest.doc_value_sorted.packs || 0,
        packBytes: engine.manifest.stats?.doc_value_sorted_pack_bytes || 0
      } : null,
      meta: existsSync(resolve(args.root, "data", "frwiki.meta.json")) ? JSON.parse(readFileSync(resolve(args.root, "data", "frwiki.meta.json"), "utf8")) : null,
      benchmark: {
        coldEnginePerCase: true,
        queryColdExcludesInit: true
      },
      init: { ms: initMs, requests: initNetwork.requests, kb: kb(initNetwork.bytes), by: networkKbBy(initNetwork) },
      rows
    };
    const invalid = rows.filter(row => !row.valid);
    if (invalid.length) report.validationErrors = invalid.map(row => ({ q: row.q, errors: row.validationErrors }));
    const reportPath = resolve(args.root, "frwiki-bench.json");
    writeFileSync(reportPath, JSON.stringify(report, null, 2));
    if (!options.quiet) console.log(JSON.stringify(report, null, 2));
    if (invalid.length) throw new Error(`frwiki typed bench validation failed for ${invalid.map(row => row.q).join(", ")}`);
    return report;
  } finally {
    meter.restore();
    await server.close();
  }
}

function clean(args) {
  rmSync(resolve(args.root, "data"), { recursive: true, force: true });
  rmSync(resolve(args.root, "public"), { recursive: true, force: true });
  rmSync(resolve(args.root, "rangefind.config.json"), { force: true });
  rmSync(resolve(args.root, "frwiki-bench.json"), { force: true });
  rmSync(resolve(args.root, "scale"), { recursive: true, force: true });
}

function rowByLabel(report, label) {
  return report.rows.find(row => row.q === label) || null;
}

function summarizeScaleRow(row) {
  if (!row) return null;
  return {
    total: row.total,
    top: row.top,
    valid: row.valid,
    coldMs: row.coldMs,
    coldRequests: row.coldRequests,
    coldKb: row.coldKb,
    coldBy: row.coldBy,
    lane: row.coldStats?.docPayloadLane || "",
    postingBlockFrontierBatches: row.coldStats?.postingBlockFrontierBatches || 0,
    postingBlockFrontierBlocks: row.coldStats?.postingBlockFrontierBlocks || 0,
    postingBlockFrontierFetchGroups: row.coldStats?.postingBlockFrontierFetchGroups || 0,
    postingBlockFrontierWantedBlocks: row.coldStats?.postingBlockFrontierWantedBlocks || 0,
    plannerLane: row.coldStats?.plannerLane || "",
    topKProven: row.coldStats?.topKProven || false,
    totalExact: row.coldStats?.totalExact || false,
    queryBundleHit: row.coldStats?.queryBundleHit || false,
    queryBundleBytes: row.coldStats?.queryBundleBytes || 0,
    docValuePruning: row.coldStats?.docValuePruning || false,
    docValuePruneField: row.coldStats?.docValuePruneField || "",
    docValuePagesVisited: row.coldStats?.docValuePagesVisited || 0,
    docValuePagesPruned: row.coldStats?.docValuePagesPruned || 0,
    docValueRowsScanned: row.coldStats?.docValueRowsScanned || 0,
    docValueRowsAccepted: row.coldStats?.docValueRowsAccepted || 0,
    docValueChunkPruning: row.coldStats?.docValueChunkPruning || false,
    docValueChunksVisited: row.coldStats?.docValueChunksVisited || 0,
    docValueChunksPruned: row.coldStats?.docValueChunksPruned || 0,
    surfaceFallbackApplied: row.coldStats?.surfaceFallbackApplied || false,
    typoAttempted: row.coldStats?.typoAttempted || false,
    typoApplied: row.coldStats?.typoApplied || false,
    exactTopKMatch: row.exactTopKMatch ?? null,
    exactTotal: row.exactTotal ?? null
  };
}

function scalePoint(limit, report) {
  const textRows = report.rows.filter(row => row.request?.q);
  const browseRows = report.rows.filter(row => row.coldStats?.docPayloadLane === "docPages");
  return {
    limit,
    docs: report.meta?.docs || limit,
    indexFiles: report.index.files,
    indexBytes: report.index.bytes,
    bytesPerDoc: report.index.bytes / Math.max(1, report.meta?.docs || limit),
    init: report.init,
    docPages: report.docPages,
    docValueSorted: report.docValueSorted,
    termPackBytes: report.rangefindStats?.term_pack_bytes || 0,
    postingBlockPackBytes: report.rangefindStats?.posting_block_pack_bytes || 0,
    docPackBytes: report.rangefindStats?.doc_pack_bytes || 0,
    docPagePackBytes: report.rangefindStats?.doc_page_pack_bytes || 0,
    docValuePackBytes: report.rangefindStats?.doc_value_pack_bytes || 0,
    docValueSortedDirectoryBytes: report.rangefindStats?.doc_value_sorted_directory_bytes || 0,
    docValueSortedPackBytes: report.rangefindStats?.doc_value_sorted_pack_bytes || 0,
    avgTextColdRequests: mean(textRows.map(row => row.coldRequests)),
    avgTextColdKb: kb(mean(textRows.map(row => row.coldKb * 1024))),
    maxTextColdRequests: Math.max(0, ...textRows.map(row => row.coldRequests)),
    avgBrowseColdRequests: mean(browseRows.map(row => row.coldRequests)),
    avgBrowseColdKb: kb(mean(browseRows.map(row => row.coldKb * 1024))),
    textExactMatches: textRows.filter(row => row.exactTopKMatch === true).length,
    textRows: textRows.length,
    browseRows: browseRows.length,
    selectedRows: {
      paris: summarizeScaleRow(rowByLabel(report, "Paris")),
      revolution: summarizeScaleRow(rowByLabel(report, "Révolution française")),
      typedDates: summarizeScaleRow(rowByLabel(report, "typed dates sorted")),
      denseBrowse: summarizeScaleRow(rowByLabel(report, "dense filter browse"))
    }
  };
}

async function scaleFixture(args) {
  const baseRoot = resolve(args.root, "scale");
  mkdirSync(baseRoot, { recursive: true });
  const points = [];
  for (const limit of args.scaleLimits) {
    const pointArgs = {
      ...args,
      command: "all",
      limit,
      root: resolve(baseRoot, String(limit)),
      runs: args.runs,
      exactChecks: args.exactChecks
    };
    const docs = await writeJsonl(pointArgs);
    writeSite(pointArgs, docs);
    await buildFixture(pointArgs);
    const report = await benchFixture(pointArgs, { quiet: true });
    points.push(scalePoint(limit, report));
    console.error(`frwiki scale: ${limit.toLocaleString()} docs, ${(report.index.bytes / 1024 / 1024).toFixed(1)} MiB, init ${report.init.requests} req ${report.init.kb.toFixed(1)} KB`);
  }
  const scaleReport = {
    fixture: "frwiki-scale",
    limits: args.scaleLimits,
    runs: args.runs,
    bodyChars: args.bodyChars,
    exactChecks: args.exactChecks,
    generatedAt: new Date().toISOString(),
    points
  };
  const reportPath = resolve(args.root, "frwiki-scale-bench.json");
  writeFileSync(reportPath, JSON.stringify(scaleReport, null, 2));
  console.log(JSON.stringify(scaleReport, null, 2));
  return scaleReport;
}

const args = parseArgs(process.argv.slice(2));
mkdirSync(resolve(args.root), { recursive: true });

if (args.command === "clean") {
  clean(args);
} else if (args.command === "prepare") {
  await writeJsonl(args);
} else if (args.command === "build") {
  await buildFixture(args);
} else if (args.command === "bench") {
  await benchFixture(args);
} else if (args.command === "scale") {
  await scaleFixture(args);
} else if (args.command === "all") {
  const docs = await writeJsonl(args);
  writeSite(args, docs);
  await buildFixture(args);
  await benchFixture(args);
} else {
  console.error(`Unknown command: ${args.command}`);
  process.exit(1);
}
