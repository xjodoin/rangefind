#!/usr/bin/env node

import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, writeFileSync, copyFileSync } from "node:fs";
import { execFileSync, spawn } from "node:child_process";
import { relative, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { gunzipSync } from "node:zlib";
import { createSearch } from "../src/runtime.js";
import { build } from "../src/builder.js";
import { createBuildBenchmarkReport } from "../src/build_report.js";
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
const BENCHMARK_ARTIFACT_FORMAT = "rffrwikibench-artifact-v1";
const BENCHMARK_INDEX_FORMAT = "rffrwikibench-index-v1";
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
    buildProgressLogMs: Number(process.env.FRWIKI_BUILD_PROGRESS_MS || 15000),
    force: false,
    limitExplicit: process.env.FRWIKI_LIMIT != null,
    reuseIndex: false,
    builderOnly: false,
    exactChecks: process.env.FRWIKI_EXACT_CHECKS !== "0"
  };
  for (const arg of argv.slice(1)) {
    if (arg === "--force") args.force = true;
    else if (arg.startsWith("--dump-url=")) args.dumpUrl = arg.slice("--dump-url=".length);
    else if (arg.startsWith("--limit=")) {
      args.limit = Number(arg.slice("--limit=".length)) || 0;
      args.limitExplicit = true;
    }
    else if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--queries=")) args.queries = arg.slice("--queries=".length).split("|").filter(Boolean);
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--size=")) args.size = Number(arg.slice("--size=".length)) || args.size;
    else if (arg.startsWith("--body-chars=")) args.bodyChars = Number(arg.slice("--body-chars=".length)) || 0;
    else if (arg.startsWith("--build-progress-ms=")) args.buildProgressLogMs = Number(arg.slice("--build-progress-ms=".length)) || 0;
    else if (arg.startsWith("--scale-limits=")) args.scaleLimits = arg.slice("--scale-limits=".length).split(",").map(value => Number(value.trim())).filter(Boolean);
    else if (arg === "--exact-checks") args.exactChecks = true;
    else if (arg === "--no-exact-checks") args.exactChecks = false;
    else if (arg === "--reuse-index" || arg === "--bench-only" || arg === "--runtime-only") args.reuseIndex = true;
    else if (arg === "--builder-only") args.builderOnly = true;
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

function expectedSourceMeta(args) {
  return {
    schemaVersion: FRWIKI_SCHEMA_VERSION,
    dumpUrl: args.dumpUrl,
    bodyChars: args.bodyChars || null
  };
}

function tempPath(path) {
  return `${path}.tmp-${process.pid}-${Date.now()}`;
}

function sourceMetaMatches(args, meta) {
  const expected = expectedSourceMeta(args);
  return meta?.schemaVersion === expected.schemaVersion
    && meta.dumpUrl === expected.dumpUrl
    && (meta.bodyChars ?? null) === expected.bodyChars;
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

function readJson(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

function cacheSatisfiesRun(args, cachePath, cacheMetaPath) {
  if (!existsSync(cachePath) || !existsSync(cacheMetaPath)) return false;
  const meta = readJson(cacheMetaPath);
  if (!sourceMetaMatches(args, meta)) return false;
  if (!args.limit) return meta.complete === true;
  return (meta.docs || 0) >= args.limit;
}

function finishStream(stream) {
  return new Promise((resolveFinish, rejectFinish) => {
    stream.on("error", rejectFinish);
    stream.end(resolveFinish);
  });
}

async function extractJsonl(args, out) {
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
    await finishStream(output);
    await Promise.allSettled([sourceDone, streamDone]);
    return {
      docs,
      pagesRead: pages,
      complete: !args.limit,
      builtAt: new Date().toISOString()
    };
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

async function writeJsonlPrefix(sourcePath, out, limit) {
  if (!limit) {
    copyFileSync(sourcePath, out);
    return null;
  }

  const tmp = tempPath(out);
  const input = createReadStream(sourcePath, { encoding: "utf8" });
  const output = createWriteStream(tmp);
  let buffer = "";
  let docs = 0;

  try {
    for await (const chunk of input) {
      buffer += chunk;
      while (true) {
        const index = buffer.indexOf("\n");
        if (index < 0) break;
        const line = buffer.slice(0, index);
        buffer = buffer.slice(index + 1);
        if (!line) continue;
        output.write(`${line}\n`);
        docs++;
        if (docs >= limit) {
          input.destroy();
          await finishStream(output);
          renameSync(tmp, out);
          return docs;
        }
      }
    }
    if (buffer.trim() && docs < limit) {
      output.write(`${buffer.trimEnd()}\n`);
      docs++;
    }
    await finishStream(output);
    renameSync(tmp, out);
    return docs;
  } catch (error) {
    rmSync(tmp, { force: true });
    throw error;
  }
}

async function materializeJsonlFromCache(args, cachePath, cacheMetaPath, out, metaPath) {
  const cacheMeta = readJson(cacheMetaPath);
  const docs = cacheMeta.docs === args.limit
    ? (copyFileSync(cachePath, out), cacheMeta.docs)
    : await writeJsonlPrefix(cachePath, out, args.limit || 0);
  writeFileSync(metaPath, JSON.stringify({
    ...expectedMeta(args),
    docs: docs ?? cacheMeta.docs,
    pagesRead: cacheMeta.pagesRead,
    builtAt: new Date().toISOString(),
    source: "cache",
    cacheDocs: cacheMeta.docs
  }, null, 2));
  return out;
}

async function writeJsonl(args) {
  const dataDir = resolve(args.root, "data");
  const out = resolve(dataDir, "frwiki.jsonl");
  const metaPath = resolve(dataDir, "frwiki.meta.json");
  const cachePath = resolve(dataDir, "frwiki.cache.jsonl");
  const cacheMetaPath = resolve(dataDir, "frwiki.cache.meta.json");
  if (!args.force && jsonlMatchesRun(args, out, metaPath)) return out;
  mkdirSync(dataDir, { recursive: true });

  if (!args.force && cacheSatisfiesRun(args, cachePath, cacheMetaPath)) {
    return materializeJsonlFromCache(args, cachePath, cacheMetaPath, out, metaPath);
  }

  const tmp = tempPath(cachePath);
  try {
    const extracted = await extractJsonl(args, tmp);
    renameSync(tmp, cachePath);
    writeFileSync(cacheMetaPath, JSON.stringify({
      ...expectedSourceMeta(args),
      limit: args.limit || null,
      docs: extracted.docs,
      pagesRead: extracted.pagesRead,
      complete: extracted.complete,
      builtAt: extracted.builtAt
    }, null, 2));
    return materializeJsonlFromCache(args, cachePath, cacheMetaPath, out, metaPath);
  } catch (error) {
    rmSync(tmp, { force: true });
    throw error;
  }
}

function writeSite(args, docsPath) {
  const root = resolve(args.root);
  const publicDir = resolve(root, "public");
  mkdirSync(publicDir, { recursive: true });
  syncRuntimeBundle(args);
  const configPath = resolve(root, "rangefind.config.json");
  const config = {
    input: docsPath,
    output: "public/rangefind",
    idPath: "id",
    urlPath: "url",
    indexProfile: "static-large",
    targetPostingsPerDoc: 12,
    bodyIndexChars: args.bodyChars,
    alwaysIndexFields: ["title", "categories"],
    typoMode: "main-index",
    typoTrigger: "zero-or-weak",
    typoMaxEdits: 2,
    typoMaxTokenCandidates: 8,
    typoMaxQueryPlans: 5,
    typoMaxCorrectedSearches: 3,
    typoMaxShardLookups: 12,
    queryBundles: true,
    targetShardPostings: 45000,
    segmentMergeFanIn: 512,
    buildTelemetryPath: "frwiki-build-telemetry.json",
    buildProgressLogMs: args.buildProgressLogMs,
    scanWorkers: 4,
    scanBatchDocs: 128,
    builderWorkerCount: 4,
    fields: [
      { name: "title", path: "title", weight: 5.5, b: 0.25, phrase: true, proximity: true, proximityWeight: 3, proximityWindow: 5 },
      { name: "categories", path: "categories", weight: 2.0, b: 0.0 },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    authority: [
      { name: "title", path: "title", weight: 1000000, exactWeight: 1000000, tokenWeight: 800000 }
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
    sortReplicas: [
      { field: "revisionDate", order: "desc" }
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

function syncRuntimeBundle(args) {
  const publicDir = resolve(args.root, "public");
  mkdirSync(publicDir, { recursive: true });
  copyFileSync(resolve("dist/runtime.browser.js"), resolve(publicDir, "runtime.browser.js"));
}

function buildTelemetryPath(args) {
  return resolve(args.root, "frwiki-build-telemetry.json");
}

function benchmarkRoot(_args) {
  return resolve("benchmarks", "frwiki");
}

function repoRelativePath(filePath) {
  return relative(resolve("."), filePath).replace(/\\/gu, "/");
}

function benchmarkLatestPath(args, kind, limit = args.limit) {
  return resolve(benchmarkRoot(args), "latest", kind, `${limitSlug(limit)}.json`);
}

function benchmarkHistoryDir(args, kind, limit = args.limit) {
  return resolve(benchmarkRoot(args), "history", kind, limitSlug(limit));
}

function benchmarkIndexPath(args) {
  return resolve(benchmarkRoot(args), "index.json");
}

function legacyBenchmarkPath(args, kind) {
  if (kind === "runtime") return resolve(args.root, "frwiki-bench.json");
  if (kind === "builder") return resolve(args.root, "frwiki-builder-bench.json");
  if (kind === "scale") return resolve(args.root, "frwiki-scale-bench.json");
  return null;
}

function limitSlug(limit) {
  return Number(limit) > 0 ? `limit-${Number(limit)}` : "full-dump";
}

function safeRunId(generatedAt, commit = null) {
  const timestamp = String(generatedAt || new Date().toISOString())
    .replace(/[.:]/gu, "-");
  return commit ? `${timestamp}_${commit}` : timestamp;
}

function currentGitCommit() {
  try {
    const commit = execFileSync("git", ["rev-parse", "--short", "HEAD"], {
      cwd: resolve("."),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"]
    }).trim();
    if (!commit) return null;
    const dirty = execFileSync("git", ["status", "--short", "--untracked-files=no"], {
      cwd: resolve("."),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"]
    }).trim();
    return dirty ? `${commit}-dirty` : commit;
  } catch {
    return null;
  }
}

let benchmarkGitCommitSnapshot;

function benchmarkGitCommit() {
  if (benchmarkGitCommitSnapshot === undefined) benchmarkGitCommitSnapshot = currentGitCommit();
  return benchmarkGitCommitSnapshot;
}

function generatedAtForReport(report) {
  return report.generatedAt || new Date().toISOString();
}

function runtimeSummary(report) {
  const rows = report.rows || [];
  const selected = Object.fromEntries([
    "Paris",
    "Révolution française",
    "Révolution française size 25",
    "typo changement climatique",
    "Paris sorted by revision date"
  ].map(label => {
    const row = rowByLabel(report, label);
    return [label, row ? {
      coldMs: row.coldMs,
      coldRequests: row.coldRequests,
      coldKb: row.coldKb,
      valid: row.valid,
      lane: row.coldStats?.plannerLane || "",
      docPayloadLane: row.coldStats?.docPayloadLane || "",
      docPayloadForced: Boolean(row.coldStats?.docPayloadForced),
      blocksDecoded: row.coldStats?.blocksDecoded || 0,
      postingsDecoded: row.coldStats?.postingsDecoded || 0,
      sortReplicaText: Boolean(row.coldStats?.sortReplicaText),
      sortReplicaFetchedBlocks: row.coldStats?.sortReplicaFetchedBlocks || 0,
      sortReplicaRankChunksFetched: row.coldStats?.sortReplicaRankChunksFetched || 0,
      sortReplicaDocPackFetches: row.coldStats?.sortReplicaDocPackFetches || 0,
      sortReplicaDocPackSkippedReason: row.coldStats?.sortReplicaDocPackSkippedReason || "",
      sortReplicaDocPagesFetched: row.coldStats?.sortReplicaDocPagesFetched || 0,
      sortReplicaDocPageSkippedReason: row.coldStats?.sortReplicaDocPageSkippedReason || "",
      docValuePagesVisited: row.coldStats?.docValuePagesVisited || 0,
      docValueSortPageFetchGroups: row.coldStats?.docValueSortPageFetchGroups || 0
    } : null];
  }));
  return {
    docs: report.meta?.docs || report.rangefindStats?.totalDocs || 0,
    indexBytes: report.index?.bytes || 0,
    indexFiles: report.index?.files || 0,
    rows: rows.length,
    validRows: rows.filter(row => row.valid).length,
    avgColdMs: mean(rows.map(row => row.coldMs || 0)),
    avgColdRequests: mean(rows.map(row => row.coldRequests || 0)),
    avgColdKb: mean(rows.map(row => row.coldKb || 0)),
    maxColdMs: Math.max(0, ...rows.map(row => row.coldMs || 0)),
    selected
  };
}

function builderSummary(report) {
  return {
    docs: report.docs || 0,
    indexBytes: report.index?.bytes || 0,
    indexFiles: report.index?.files || 0,
    bytesPerDoc: report.index?.bytesPerDoc || 0,
    totalMs: report.builder?.totalMs || 0,
    peakRss: report.builder?.peakRss || 0,
    tempPeakBytes: report.builder?.tempPeakBytes || 0,
    outputWrittenBytes: report.builder?.outputWrittenBytes || 0,
    writeAmplification: report.builder?.writeAmplification || 0
  };
}

function scaleSummary(report) {
  const points = report.points || [];
  return {
    mode: report.mode,
    limits: report.limits || points.map(point => point.limit),
    points: points.map(point => ({
      limit: point.limit,
      docs: point.docs,
      indexBytes: point.indexBytes,
      bytesPerDoc: point.bytesPerDoc,
      avgTextColdRequests: point.avgTextColdRequests || 0,
      avgTextColdKb: point.avgTextColdKb || 0,
      builderMs: point.builder?.totalMs || 0,
      peakRss: point.builder?.peakRss || 0
    }))
  };
}

function benchmarkSummary(kind, report) {
  if (kind === "runtime") return runtimeSummary(report);
  if (kind === "builder") return builderSummary(report);
  if (kind === "scale") return scaleSummary(report);
  return {};
}

function numericDeltas(current, previous, prefix = "") {
  if (!previous) return {};
  const out = {};
  for (const [key, value] of Object.entries(current || {})) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (typeof value === "number" && Number.isFinite(value) && typeof previous[key] === "number" && Number.isFinite(previous[key])) {
      const delta = value - previous[key];
      out[path] = {
        previous: previous[key],
        current: value,
        delta,
        pct: previous[key] ? delta / previous[key] : null
      };
    } else if (value && typeof value === "object" && !Array.isArray(value) && previous[key] && typeof previous[key] === "object" && !Array.isArray(previous[key])) {
      Object.assign(out, numericDeltas(value, previous[key], path));
    }
  }
  return out;
}

function readBenchmarkIndex(args) {
  return readJson(benchmarkIndexPath(args)) || {
    format: BENCHMARK_INDEX_FORMAT,
    updatedAt: null,
    latest: {},
    history: []
  };
}

function benchmarkKey(record) {
  return `${record.kind}:${record.limitSlug}`;
}

function writeBenchmarkIndex(args, record) {
  const indexPath = benchmarkIndexPath(args);
  const index = readBenchmarkIndex(args);
  const key = benchmarkKey(record);
  const previous = [...(index.history || [])].reverse()
    .find(item => item.kind === record.kind && item.limitSlug === record.limitSlug && item.historyPath !== record.historyPath);
  record.previousHistoryPath = previous?.historyPath || null;
  record.deltas = numericDeltas(record.summary, previous?.summary);
  index.updatedAt = new Date().toISOString();
  index.latest[key] = record;
  index.history = [...(index.history || []).filter(item => item.historyPath !== record.historyPath), record]
    .sort((a, b) => String(a.generatedAt).localeCompare(String(b.generatedAt)));
  mkdirSync(benchmarkRoot(args), { recursive: true });
  writeFileSync(indexPath, `${JSON.stringify(index, null, 2)}\n`);
}

function writeBenchmarkArtifact(args, kind, report, options = {}) {
  const generatedAt = generatedAtForReport(report);
  const commit = benchmarkGitCommit();
  const limit = options.limit ?? args.limit;
  const limitName = limitSlug(limit);
  const historyDir = benchmarkHistoryDir(args, kind, limit);
  const latestPath = benchmarkLatestPath(args, kind, limit);
  const runId = safeRunId(generatedAt, commit);
  const historyPath = resolve(historyDir, `${runId}.json`);
  const artifact = {
    format: BENCHMARK_ARTIFACT_FORMAT,
    kind,
    limit,
    limitSlug: limitName,
    generatedAt,
    gitCommit: commit,
    command: args.command,
    runs: args.runs,
    size: args.size,
    historyPath: repoRelativePath(historyPath),
    latestPath: repoRelativePath(latestPath)
  };
  report.benchmarkArtifact = artifact;
  mkdirSync(historyDir, { recursive: true });
  mkdirSync(resolve(benchmarkRoot(args), "latest", kind), { recursive: true });
  writeFileSync(historyPath, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(latestPath, `${JSON.stringify(report, null, 2)}\n`);
  writeBenchmarkIndex(args, {
    ...artifact,
    summary: benchmarkSummary(kind, report)
  });
  return { historyPath, latestPath };
}

function readBenchmarkReport(args, kind) {
  return readJson(benchmarkLatestPath(args, kind)) || readJson(legacyBenchmarkPath(args, kind));
}

function readBuildTelemetry(args) {
  return readJson(buildTelemetryPath(args))
    || readJson(resolve(args.root, "public", "rangefind", "debug", "build-telemetry.json"));
}

function publishedIndexStats(root) {
  const manifestPath = resolve(root, "manifest.json");
  if (!existsSync(manifestPath)) return dirStats(root, { skipNames: ["_build"] });
  const byBasename = new Map();
  const walkFiles = (dir) => {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      if (entry.name === "_build") continue;
      const full = resolve(dir, entry.name);
      if (entry.isDirectory()) {
        walkFiles(full);
        continue;
      }
      if (!entry.isFile()) continue;
      const rel = relative(root, full).replace(/\\/gu, "/");
      const list = byBasename.get(entry.name) || [];
      list.push(rel);
      byBasename.set(entry.name, list);
    }
  };
  walkFiles(root);

  const files = new Set();
  const processed = new Set();
  const addFile = (relPath) => {
    const normalized = String(relPath || "").replace(/^\.?\//u, "");
    if (!normalized || normalized.includes("..")) return;
    const full = resolve(root, normalized);
    if (!full.startsWith(resolve(root)) || !existsSync(full) || !statSync(full).isFile()) return;
    files.add(normalized);
    if (processed.has(normalized)) return;
    processed.add(normalized);
    if (normalized.endsWith(".json") || normalized.endsWith(".json.gz")) {
      try {
        const raw = readFileSync(full);
        const text = normalized.endsWith(".gz") ? gunzipSync(raw).toString("utf8") : raw.toString("utf8");
        collect(JSON.parse(text));
      } catch {
        // Non-JSON gzip members are counted but not expanded.
      }
    }
  };
  const collect = (value) => {
    if (Array.isArray(value)) {
      for (const item of value) collect(item);
      return;
    }
    if (value && typeof value === "object") {
      for (const item of Object.values(value)) collect(item);
      return;
    }
    if (typeof value !== "string") return;
    const normalized = value.replace(/^\.?\//u, "");
    if (existsSync(resolve(root, normalized))) addFile(normalized);
    const basename = normalized.split("/").pop();
    for (const rel of byBasename.get(basename) || []) addFile(rel);
  };

  addFile("manifest.json");
  addFile("manifest.min.json");
  addFile("manifest.full.json");
  let bytes = 0;
  for (const file of files) bytes += statSync(resolve(root, file)).size;
  return { files: files.size, bytes };
}

function writeBuilderBenchReport(args, options = {}) {
  const root = resolve(args.root, "public");
  const manifest = readJson(resolve(root, "rangefind", "manifest.min.json"))
    || readJson(resolve(root, "rangefind", "manifest.json"));
  const meta = readJson(resolve(args.root, "data", "frwiki.meta.json"));
  const telemetry = readBuildTelemetry(args);
  if (!telemetry) {
    throw new Error(`No build telemetry found for builder report at ${buildTelemetryPath(args)}`);
  }
  const report = createBuildBenchmarkReport({
    telemetry,
    index: publishedIndexStats(resolve(root, "rangefind")),
    docs: meta?.docs || manifest?.total || args.limit,
    meta,
    mode: options.mode || "build"
  });
  const artifact = writeBenchmarkArtifact(args, "builder", report);
  if (options.quiet === false) console.log(JSON.stringify(report, null, 2));
  return { ...report, benchmarkArtifact: report.benchmarkArtifact || { latestPath: artifact.latestPath, historyPath: artifact.historyPath } };
}

function readBuilderBenchReport(args) {
  return readBenchmarkReport(args, "builder");
}

async function buildFixture(args, options = {}) {
  const docsPath = resolve(args.root, "data", "frwiki.jsonl");
  const configPath = writeSite(args, docsPath);
  await build({ configPath });
  return writeBuilderBenchReport(args, options);
}

function networkBucket(url) {
  const path = new URL(url).pathname;
  if (path.endsWith("/manifest.min.json")) return "manifestMin";
  if (path.endsWith("/manifest.full.json")) return "manifestFull";
  if (path.endsWith("/debug/build-telemetry.json")) return "buildTelemetry";
  if (path.endsWith("/runtime.browser.js")) return "runtime";
  if (/\/manifest(?:\.[0-9a-f]+)?\.json$/u.test(path)) return "manifest";
  if (path.includes("/sort-replicas/") && path.includes("/docs/pointers/")) return "sortReplicaDocPointers";
  if (path.includes("/sort-replicas/") && path.includes("/docs/packs/")) return "sortReplicaDocs";
  if (path.includes("/sort-replicas/") && path.includes("/docs/pages/")) return "sortReplicaDocPagePointers";
  if (path.includes("/sort-replicas/") && path.includes("/docs/page-packs/")) return "sortReplicaDocPages";
  if (path.includes("/sort-replicas/") && path.includes("/rank-packs/")) return "sortReplicaRankMaps";
  if (path.includes("/sort-replicas/") && path.includes("/terms/block-packs/")) return "sortReplicaPostingBlocks";
  if (path.includes("/sort-replicas/") && path.includes("/terms/packs/")) return "sortReplicaTerms";
  if (path.includes("/directory-")) return "directory";
  if (path.includes("/bundles/packs/")) return "queryBundles";
  if (path.includes("/authority/packs/")) return "authority";
  if (path.includes("/terms/block-packs/")) return "postingBlocks";
  if (path.includes("/terms/packs/")) return "terms";
  if (path.includes("/facets/packs/")) return "facetDictionaries";
  if (path.includes("/filter-bitmaps/")) return "filterBitmaps";
  if (path.includes("/doc-values/sorted")) return "docValueSorted";
  if (path.includes("/doc-values/")) return "docValues";
  if (path.includes("/docs/pointers/")) return "docPointers";
  if (path.includes("/docs/pages/")) return "docPagePointers";
  if (path.includes("/docs/page-packs/")) return "docPages";
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
  for (const field of ["queryBundleHit", "authorityApplied", "typoApplied", "typoAttempted", "docValuePruning", "topKProven"]) {
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
    topKProofDocRangeAware: Boolean(stats.topKProofDocRangeAware),
    topKProofThreshold: stats.topKProofThreshold || 0,
    topKProofMaxOutsidePotential: stats.topKProofMaxOutsidePotential || 0,
    blocksDecoded: stats.blocksDecoded || 0,
    postingsDecoded: stats.postingsDecoded || 0,
    postingsAccepted: stats.postingsAccepted || 0,
    skippedBlocks: stats.skippedBlocks || 0,
    terms: stats.terms || 0,
    shards: stats.shards || 0,
    missingBaseTerms: stats.missingBaseTerms || 0,
    postingBlockFrontier: stats.postingBlockFrontier || 0,
    postingBlockFrontierBatches: stats.postingBlockFrontierBatches || 0,
    postingBlockFrontierBlocks: stats.postingBlockFrontierBlocks || 0,
    postingBlockFrontierMax: stats.postingBlockFrontierMax || 0,
    postingBlockFrontierFetchedBlocks: stats.postingBlockFrontierFetchedBlocks || 0,
    postingBlockFrontierFetchGroups: stats.postingBlockFrontierFetchGroups || 0,
    postingBlockFrontierWantedBlocks: stats.postingBlockFrontierWantedBlocks || 0,
    sortReplicaText: Boolean(stats.sortReplicaText),
    sortReplicaId: stats.sortReplicaId || "",
    sortReplicaField: stats.sortReplicaField || "",
    sortReplicaDirection: stats.sortReplicaDirection || "",
    sortReplicaStopReason: stats.sortReplicaStopReason || "",
    sortReplicaStopChecks: stats.sortReplicaStopChecks || 0,
    sortReplicaFrontier: stats.sortReplicaFrontier || 0,
    sortReplicaFrontierBatches: stats.sortReplicaFrontierBatches || 0,
    sortReplicaFrontierBlocks: stats.sortReplicaFrontierBlocks || 0,
    sortReplicaFrontierMax: stats.sortReplicaFrontierMax || 0,
    sortReplicaFetchedBlocks: stats.sortReplicaFetchedBlocks || 0,
    sortReplicaFetchGroups: stats.sortReplicaFetchGroups || 0,
    sortReplicaWantedBlocks: stats.sortReplicaWantedBlocks || 0,
    sortReplicaRankLookups: stats.sortReplicaRankLookups || 0,
    sortReplicaRankChunksWanted: stats.sortReplicaRankChunksWanted || 0,
    sortReplicaRankChunksFetched: stats.sortReplicaRankChunksFetched || 0,
    sortReplicaRankChunkFetchGroups: stats.sortReplicaRankChunkFetchGroups || 0,
    sortReplicaDocPackPointerLookups: stats.sortReplicaDocPackPointerLookups || 0,
    sortReplicaDocPackPointerFetches: stats.sortReplicaDocPackPointerFetches || 0,
    sortReplicaDocPackPointerFetchGroups: stats.sortReplicaDocPackPointerFetchGroups || 0,
    sortReplicaDocPackFetches: stats.sortReplicaDocPackFetches || 0,
    sortReplicaDocPackFetchGroups: stats.sortReplicaDocPackFetchGroups || 0,
    sortReplicaDocPackPlannedFetchGroups: stats.sortReplicaDocPackPlannedFetchGroups || 0,
    sortReplicaDocPackPlannedFetchBytes: stats.sortReplicaDocPackPlannedFetchBytes || 0,
    sortReplicaDocPackSkippedReason: stats.sortReplicaDocPackSkippedReason || "",
    sortReplicaDocPageLookups: stats.sortReplicaDocPageLookups || 0,
    sortReplicaDocPagesWanted: stats.sortReplicaDocPagesWanted || 0,
    sortReplicaDocPagesFetched: stats.sortReplicaDocPagesFetched || 0,
    sortReplicaDocPageFetchGroups: stats.sortReplicaDocPageFetchGroups || 0,
    sortReplicaDocPagePlannedFetchGroups: stats.sortReplicaDocPagePlannedFetchGroups || 0,
    sortReplicaDocPagePlannedFetchBytes: stats.sortReplicaDocPagePlannedFetchBytes || 0,
    sortReplicaDocPageSkippedReason: stats.sortReplicaDocPageSkippedReason || "",
    sortReplicaDocPagePointerPagesWanted: stats.sortReplicaDocPagePointerPagesWanted || 0,
    sortReplicaDocPagePointerPagesFetched: stats.sortReplicaDocPagePointerPagesFetched || 0,
    sortReplicaDocPagePointerFetchGroups: stats.sortReplicaDocPagePointerFetchGroups || 0,
    docRangeBlockMax: Boolean(stats.docRangeBlockMax),
    docRangeSize: stats.docRangeSize || 0,
    docRangeCandidateRanges: stats.docRangeCandidateRanges || 0,
    docRangeRangesVisited: stats.docRangeRangesVisited || 0,
    docRangeRangesPruned: stats.docRangeRangesPruned || 0,
    docRangeNextUpperBound: stats.docRangeNextUpperBound || 0,
    docRangeCandidateBlockRatio: stats.docRangeCandidateBlockRatio || 0,
    docRangeBlocksVisited: stats.docRangeBlocksVisited || 0,
    docRangePostingRowsScanned: stats.docRangePostingRowsScanned || 0,
    docRangePostingBlocksCandidate: stats.docRangePostingBlocksCandidate || 0,
    docRangePostingBlocksProcessed: stats.docRangePostingBlocksProcessed || 0,
    docRangeInnerBlockBatches: stats.docRangeInnerBlockBatches || 0,
    docRangeInnerBlocksPruned: stats.docRangeInnerBlocksPruned || 0,
    docRangeInitialBatchLimit: stats.docRangeInitialBatchLimit || 0,
    docRangeImpactPlanner: Boolean(stats.docRangeImpactPlanner),
    docRangeImpactTierTerms: stats.docRangeImpactTierTerms || 0,
    docRangeImpactTierTasks: stats.docRangeImpactTierTasks || 0,
    docRangeImpactSeed: Boolean(stats.docRangeImpactSeed),
    docRangeImpactSeedBlocks: stats.docRangeImpactSeedBlocks || 0,
    docRangeImpactSeedRowsScanned: stats.docRangeImpactSeedRowsScanned || 0,
    docRangeImpactSeedPostingsAccepted: stats.docRangeImpactSeedPostingsAccepted || 0,
    docRangeImpactSeedFetchedBlocks: stats.docRangeImpactSeedFetchedBlocks || 0,
    docRangeImpactSeedFetchGroups: stats.docRangeImpactSeedFetchGroups || 0,
    docRangeImpactSeedWantedBlocks: stats.docRangeImpactSeedWantedBlocks || 0,
    docRangeImpactSeedIndexedTerms: stats.docRangeImpactSeedIndexedTerms || 0,
    docRangeImpactSeedScannedTerms: stats.docRangeImpactSeedScannedTerms || 0,
    docRangeFetchedBlocks: stats.docRangeFetchedBlocks || 0,
    docRangeFetchGroups: stats.docRangeFetchGroups || 0,
    rerankCandidates: stats.rerankCandidates || 0,
    dependencyFeatures: stats.dependencyFeatures || 0,
    dependencyTermsMatched: stats.dependencyTermsMatched || 0,
    dependencyPostingsScanned: stats.dependencyPostingsScanned || 0,
    dependencyCandidateMatches: stats.dependencyCandidateMatches || 0,
    docPayloadLane: stats.docPayloadLane || "",
    docPayloadPages: stats.docPayloadPages || 0,
    docPayloadOverfetchDocs: stats.docPayloadOverfetchDocs || 0,
    docPayloadAdaptive: Boolean(stats.docPayloadAdaptive),
    docPayloadForced: Boolean(stats.docPayloadForced),
    docValuePruning: Boolean(stats.docValuePruning),
    docValuePruneField: stats.docValuePruneField || "",
    docValueDirectoryPages: stats.docValueDirectoryPages || 0,
    docValueCandidatePages: stats.docValueCandidatePages || 0,
    docValuePagesPruned: stats.docValuePagesPruned || 0,
    docValuePagesVisited: stats.docValuePagesVisited || 0,
    docValueSortPageBatchSize: stats.docValueSortPageBatchSize || 0,
    docValueSortPagesPrefetched: stats.docValueSortPagesPrefetched || 0,
    docValueSortPagesFetched: stats.docValueSortPagesFetched || 0,
    docValueSortPageFetchGroups: stats.docValueSortPageFetchGroups || 0,
    docValueSortPageOverfetch: stats.docValueSortPageOverfetch || 0,
    docValueRowsScanned: stats.docValueRowsScanned || 0,
    docValueRowsAccepted: stats.docValueRowsAccepted || 0,
    docValueDefinitePages: stats.docValueDefinitePages || 0,
    docValueChunkPruning: Boolean(stats.docValueChunkPruning),
    docValueChunksVisited: stats.docValueChunksVisited || 0,
    docValueChunksPruned: stats.docValueChunksPruned || 0,
    queryBundleLookups: stats.queryBundleLookups || 0,
    queryBundleHit: Boolean(stats.queryBundleHit),
    queryBundleFiltered: Boolean(stats.queryBundleFiltered),
    queryBundleRows: stats.queryBundleRows || 0,
    queryBundleRowGroups: stats.queryBundleRowGroups || 0,
    queryBundleRowGroupsScanned: stats.queryBundleRowGroupsScanned || 0,
    queryBundleRowsAccepted: stats.queryBundleRowsAccepted || 0,
    queryBundleTotal: stats.queryBundleTotal || 0,
    queryBundleBytes: stats.queryBundleBytes || 0,
    queryBundleComplete: Boolean(stats.queryBundleComplete),
    queryBundleFilterProof: stats.queryBundleFilterProof || "",
    queryBundleFilterProgressive: Boolean(stats.queryBundleFilterProgressive),
    queryBundleFilterExhausted: stats.queryBundleFilterExhausted !== false,
    queryBundleFilterValueSource: stats.queryBundleFilterValueSource || "",
    queryBundleFilterRowsScanned: stats.queryBundleFilterRowsScanned || 0,
    queryBundleFilterRowsAccepted: stats.queryBundleFilterRowsAccepted || 0,
    authorityAttempted: Boolean(stats.authorityAttempted),
    authorityApplied: Boolean(stats.authorityApplied),
    authorityComplete: Boolean(stats.authorityComplete),
    authorityKeys: stats.authorityKeys || 0,
    authorityEntries: stats.authorityEntries || 0,
    authorityRows: stats.authorityRows || 0,
    authorityInjected: stats.authorityInjected || 0,
    surfaceFallbackAttempted: Boolean(stats.surfaceFallbackAttempted),
    surfaceFallbackApplied: Boolean(stats.surfaceFallbackApplied),
    surfaceFallbackTerms: stats.surfaceFallbackTerms || [],
    typoAttempted: Boolean(stats.typoAttempted),
    typoApplied: Boolean(stats.typoApplied),
    typoSkippedReason: stats.typoSkippedReason || "",
    typoOriginalTotal: stats.typoOriginalTotal || 0,
    typoCorrectedQuery: stats.typoCorrectedQuery || "",
    typoCandidateTerms: stats.typoCandidateTerms || 0,
    typoCorrectionPlans: stats.typoCorrectionPlans || 0,
    typoCorrectionPlansEstimated: stats.typoCorrectionPlansEstimated || 0,
    typoCorrectionPlansExecuted: stats.typoCorrectionPlansExecuted || 0,
    typoCorrectionBestUpperBound: stats.typoCorrectionBestUpperBound || 0,
    typoCorrectedUpperBound: stats.typoCorrectedUpperBound || 0,
    typoShardLookups: stats.typoShardLookups || 0,
    typoCandidateShardLookups: stats.typoCandidateShardLookups || stats.typoShardLookups || 0,
    typoCandidateTermsScanned: stats.typoCandidateTermsScanned || 0,
    typoSuggested: Boolean(stats.typoSuggested),
    typoSuggestedQuery: stats.typoSuggestedQuery || "",
    trace: compactRuntimeTrace(stats.trace)
  };
}

function compactRuntimeTrace(trace) {
  if (!trace?.spans?.length) return null;
  return {
    totalMs: trace.totalMs || 0,
    spans: trace.spans.map(span => ({
      name: span.name,
      count: span.count || 0,
      totalMs: span.totalMs || 0,
      maxMs: span.maxMs || 0
    }))
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
    if (row.coldStats?.surfaceFallbackApplied) {
      row.exactTopKMatch = null;
      row.exactSkippedReason = "surface-fallback";
      continue;
    }
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
          trace: i === 0,
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
      generatedAt: new Date().toISOString(),
      root: repoRelativePath(root),
      index: publishedIndexStats(resolve(root, "rangefind")),
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
      builder: readBuilderBenchReport(args),
      benchmark: {
        coldEnginePerCase: true,
        queryColdExcludesInit: true
      },
      init: { ms: initMs, requests: initNetwork.requests, kb: kb(initNetwork.bytes), by: networkKbBy(initNetwork) },
      rows
    };
    const invalid = rows.filter(row => !row.valid);
    if (invalid.length) report.validationErrors = invalid.map(row => ({ q: row.q, errors: row.validationErrors }));
    writeBenchmarkArtifact(args, "runtime", report);
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
  rmSync(resolve(args.root, "frwiki-builder-bench.json"), { force: true });
  rmSync(resolve(args.root, "frwiki-build-telemetry.json"), { force: true });
  rmSync(benchmarkRoot(args), { recursive: true, force: true });
  rmSync(resolve(args.root, "benchmarks"), { recursive: true, force: true });
  rmSync(resolve(args.root, "scale"), { recursive: true, force: true });
}

function assertReusableIndex(args) {
  const manifestPath = resolve(args.root, "public", "rangefind", "manifest.min.json");
  const manifest = readJson(manifestPath);
  if (!manifest) {
    throw new Error(`No reusable Rangefind index found at ${manifestPath}; run the build or all command first.`);
  }
  if (args.limitExplicit && args.limit && manifest.total !== args.limit) {
    throw new Error(`Reusable index has ${manifest.total} docs, but --limit=${args.limit}; rerun without --reuse-index or use the matching limit.`);
  }
  return manifest;
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
    docPayloadForced: Boolean(row.coldStats?.docPayloadForced),
    postingBlockFrontierBatches: row.coldStats?.postingBlockFrontierBatches || 0,
    postingBlockFrontierBlocks: row.coldStats?.postingBlockFrontierBlocks || 0,
    postingBlockFrontierFetchGroups: row.coldStats?.postingBlockFrontierFetchGroups || 0,
    postingBlockFrontierWantedBlocks: row.coldStats?.postingBlockFrontierWantedBlocks || 0,
    docRangeBlockMax: row.coldStats?.docRangeBlockMax || false,
    docRangeRangesVisited: row.coldStats?.docRangeRangesVisited || 0,
    docRangeRangesPruned: row.coldStats?.docRangeRangesPruned || 0,
    docRangeInnerBlocksPruned: row.coldStats?.docRangeInnerBlocksPruned || 0,
    docRangeInitialBatchLimit: row.coldStats?.docRangeInitialBatchLimit || 0,
    docRangeCandidateBlockRatio: row.coldStats?.docRangeCandidateBlockRatio || 0,
    docRangeImpactPlanner: row.coldStats?.docRangeImpactPlanner || false,
    docRangeImpactTierTerms: row.coldStats?.docRangeImpactTierTerms || 0,
    docRangeImpactTierTasks: row.coldStats?.docRangeImpactTierTasks || 0,
    docRangeImpactSeed: row.coldStats?.docRangeImpactSeed || false,
    docRangeImpactSeedBlocks: row.coldStats?.docRangeImpactSeedBlocks || 0,
    docRangeImpactSeedFetchGroups: row.coldStats?.docRangeImpactSeedFetchGroups || 0,
    docRangeImpactSeedWantedBlocks: row.coldStats?.docRangeImpactSeedWantedBlocks || 0,
    docRangeImpactSeedIndexedTerms: row.coldStats?.docRangeImpactSeedIndexedTerms || 0,
    docRangeImpactSeedScannedTerms: row.coldStats?.docRangeImpactSeedScannedTerms || 0,
    plannerLane: row.coldStats?.plannerLane || "",
    sortReplicaText: row.coldStats?.sortReplicaText || false,
    sortReplicaField: row.coldStats?.sortReplicaField || "",
    sortReplicaStopReason: row.coldStats?.sortReplicaStopReason || "",
    sortReplicaFetchedBlocks: row.coldStats?.sortReplicaFetchedBlocks || 0,
    sortReplicaRankChunksFetched: row.coldStats?.sortReplicaRankChunksFetched || 0,
    sortReplicaDocPackFetches: row.coldStats?.sortReplicaDocPackFetches || 0,
    sortReplicaDocPackSkippedReason: row.coldStats?.sortReplicaDocPackSkippedReason || "",
    sortReplicaDocPagesFetched: row.coldStats?.sortReplicaDocPagesFetched || 0,
    sortReplicaDocPageSkippedReason: row.coldStats?.sortReplicaDocPageSkippedReason || "",
    topKProven: row.coldStats?.topKProven || false,
    totalExact: row.coldStats?.totalExact || false,
    queryBundleHit: row.coldStats?.queryBundleHit || false,
    queryBundleBytes: row.coldStats?.queryBundleBytes || 0,
    queryBundleRowGroups: row.coldStats?.queryBundleRowGroups || 0,
    queryBundleRowGroupsScanned: row.coldStats?.queryBundleRowGroupsScanned || 0,
    authorityAttempted: row.coldStats?.authorityAttempted || false,
    authorityApplied: row.coldStats?.authorityApplied || false,
    authorityRows: row.coldStats?.authorityRows || 0,
    authorityInjected: row.coldStats?.authorityInjected || 0,
    docValuePruning: row.coldStats?.docValuePruning || false,
    docValuePruneField: row.coldStats?.docValuePruneField || "",
    docValuePagesVisited: row.coldStats?.docValuePagesVisited || 0,
    docValuePagesPruned: row.coldStats?.docValuePagesPruned || 0,
    docValueSortPageBatchSize: row.coldStats?.docValueSortPageBatchSize || 0,
    docValueSortPagesPrefetched: row.coldStats?.docValueSortPagesPrefetched || 0,
    docValueSortPagesFetched: row.coldStats?.docValueSortPagesFetched || 0,
    docValueSortPageFetchGroups: row.coldStats?.docValueSortPageFetchGroups || 0,
    docValueSortPageOverfetch: row.coldStats?.docValueSortPageOverfetch || 0,
    docValueRowsScanned: row.coldStats?.docValueRowsScanned || 0,
    docValueRowsAccepted: row.coldStats?.docValueRowsAccepted || 0,
    docValueChunkPruning: row.coldStats?.docValueChunkPruning || false,
    docValueChunksVisited: row.coldStats?.docValueChunksVisited || 0,
    docValueChunksPruned: row.coldStats?.docValueChunksPruned || 0,
    surfaceFallbackApplied: row.coldStats?.surfaceFallbackApplied || false,
    typoAttempted: row.coldStats?.typoAttempted || false,
    typoApplied: row.coldStats?.typoApplied || false,
    typoCorrectionPlansExecuted: row.coldStats?.typoCorrectionPlansExecuted || 0,
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
    authorityDirectoryBytes: report.rangefindStats?.authority_directory_bytes || 0,
    authorityPackBytes: report.rangefindStats?.authority_pack_bytes || 0,
    builder: summarizeBuilderReport(report.builder),
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

function summarizeBuilderReport(report) {
  if (!report) return null;
  return {
    totalMs: report.builder?.totalMs || 0,
    peakRss: report.builder?.peakRss || 0,
    peakHeapUsed: report.builder?.peakHeapUsed || 0,
    memorySampleCount: report.builder?.memorySampleCount || 0,
    phaseSampleCount: report.builder?.phaseSampleCount || 0,
    tempPeakBytes: report.builder?.tempPeakBytes || 0,
    tempWrittenBytes: report.builder?.tempWrittenBytes || 0,
    outputWrittenBytes: report.builder?.outputWrittenBytes || 0,
    writeAmplification: report.builder?.writeAmplification || 0,
    phases: (report.phases || []).map(phase => ({
      name: phase.name,
      ms: phase.ms,
      peakRss: phase.peakRss,
      tempDeltaBytes: phase.tempDeltaBytes,
      outputDeltaBytes: phase.outputDeltaBytes
    }))
  };
}

function builderScalePoint(limit, report) {
  return {
    limit,
    docs: report.docs || limit,
    indexFiles: report.index?.files || 0,
    indexBytes: report.index?.bytes || 0,
    bytesPerDoc: report.index?.bytesPerDoc || 0,
    builder: summarizeBuilderReport(report)
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
    const builderReport = await buildFixture(pointArgs, { quiet: true, mode: args.builderOnly ? "scale-builder-only" : "scale" });
    if (args.builderOnly) {
      points.push(builderScalePoint(limit, builderReport));
      console.error(`frwiki scale builder: ${limit.toLocaleString()} docs, ${(builderReport.index.bytes / 1024 / 1024).toFixed(1)} MiB, build ${(builderReport.builder.totalMs / 1000).toFixed(1)} s`);
    } else {
      const report = await benchFixture(pointArgs, { quiet: true });
      points.push(scalePoint(limit, report));
      console.error(`frwiki scale: ${limit.toLocaleString()} docs, ${(report.index.bytes / 1024 / 1024).toFixed(1)} MiB, init ${report.init.requests} req ${report.init.kb.toFixed(1)} KB`);
    }
  }
  const scaleReport = {
    fixture: "frwiki-scale",
    mode: args.builderOnly ? "builder-only" : "full",
    limits: args.scaleLimits,
    runs: args.runs,
    bodyChars: args.bodyChars,
    exactChecks: args.exactChecks,
    generatedAt: new Date().toISOString(),
    points
  };
  writeBenchmarkArtifact(args, "scale", scaleReport, { limit: 0 });
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
} else if (args.command === "builder-bench") {
  const docs = await writeJsonl(args);
  writeSite(args, docs);
  await buildFixture(args, { quiet: false, mode: "builder-only" });
} else if (args.command === "bench" || args.command === "runtime-bench") {
  assertReusableIndex(args);
  syncRuntimeBundle(args);
  await benchFixture(args);
} else if (args.command === "scale") {
  await scaleFixture(args);
} else if (args.command === "all") {
  if (!args.reuseIndex) {
    const docs = await writeJsonl(args);
    writeSite(args, docs);
    await buildFixture(args, { mode: args.builderOnly ? "builder-only" : "all" });
  } else {
    assertReusableIndex(args);
    syncRuntimeBundle(args);
  }
  if (args.builderOnly) {
    writeBuilderBenchReport(args, { quiet: false, mode: args.reuseIndex ? "reused-builder-report" : "builder-only" });
  } else {
    await benchFixture(args);
  }
} else {
  console.error(`Unknown command: ${args.command}`);
  process.exit(1);
}
