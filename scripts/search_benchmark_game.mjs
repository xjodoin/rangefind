#!/usr/bin/env node

import { createReadStream, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createInterface } from "node:readline";
import { dirname, resolve } from "node:path";
import { createSearch } from "../src/runtime.js";
import { fold, mean, quantile, serveStatic } from "./bench_support.mjs";

const ARTIFACT_FORMAT = "rfsearchbenchmarkgame-artifact-v1";
const INDEX_FORMAT = "rffrwikibench-index-v1";
const DEFAULT_COMMANDS = ["TOP_10", "TOP_100", "TOP_1000"];
const DEFAULT_SEED = 2;
const STOPWORDS = new Set("a an and are as at be by for from in is of on or the to with au aux avec dans de des du en et la le les pour que qui sur une".split(/\s+/u));

function parseArgs(argv) {
  const args = {
    root: "examples/frwiki",
    docs: "",
    public: "",
    basePath: "rangefind/",
    queries: "",
    queryLimit: 60,
    commands: DEFAULT_COMMANDS,
    runs: 3,
    warmupMs: 250,
    json: false,
    writeArtifact: true,
    upstreamResults: "",
    seed: DEFAULT_SEED
  };
  for (const arg of argv) {
    if (arg === "--json") args.json = true;
    else if (arg === "--no-artifact") args.writeArtifact = false;
    else if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--docs=")) args.docs = arg.slice("--docs=".length);
    else if (arg.startsWith("--public=")) args.public = arg.slice("--public=".length);
    else if (arg.startsWith("--base-path=")) args.basePath = arg.slice("--base-path=".length);
    else if (arg.startsWith("--queries=")) args.queries = arg.slice("--queries=".length);
    else if (arg.startsWith("--query-limit=")) args.queryLimit = Number(arg.slice("--query-limit=".length)) || args.queryLimit;
    else if (arg.startsWith("--commands=")) args.commands = arg.slice("--commands=".length).split(/[,\s]+/u).filter(Boolean);
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--warmup-ms=")) args.warmupMs = Number(arg.slice("--warmup-ms=".length)) || 0;
    else if (arg.startsWith("--upstream-results=")) args.upstreamResults = arg.slice("--upstream-results=".length);
    else if (arg.startsWith("--seed=")) args.seed = Number(arg.slice("--seed=".length)) || args.seed;
  }
  args.docs ||= resolve(args.root, "data", "frwiki.jsonl");
  args.public ||= resolve(args.root, "public");
  return args;
}

function tokenize(value) {
  return fold(value).split(/[^a-z0-9]+/u).filter(token => token.length >= 3 && !STOPWORDS.has(token));
}

function normalizeGameQuery(query) {
  return String(query || "")
    .replace(/([+\-])([\p{L}\p{N}_]+)/gu, "$2")
    .replace(/"/gu, " ")
    .replace(/\b[a-zA-Z_][\w.]*:/gu, " ")
    .replace(/\b(?:AND|OR|NOT)\b/giu, " ")
    .replace(/[()]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

async function readGameQueries(path, limit) {
  const rows = [];
  const input = createReadStream(path, { encoding: "utf8" });
  const rl = createInterface({ input, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line.trim()) continue;
    const parsed = JSON.parse(line);
    const query = normalizeGameQuery(parsed.query);
    if (!query) continue;
    rows.push({
      query,
      originalQuery: String(parsed.query || ""),
      tags: Array.isArray(parsed.tags) ? parsed.tags : [],
      source: "search-benchmark-game"
    });
    if (rows.length >= limit) {
      input.destroy();
      break;
    }
  }
  return rows;
}

async function generatedQueries(docsPath, limit) {
  const rows = [];
  const seen = new Set();
  const input = createReadStream(docsPath, { encoding: "utf8" });
  const rl = createInterface({ input, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line.trim()) continue;
    const doc = JSON.parse(line);
    const title = String(doc.title || "");
    if (!title || title.length > 80) continue;
    const tokens = tokenize(title);
    if (tokens.length < 2) continue;
    const phrase = tokens.slice(0, Math.min(4, tokens.length)).join(" ");
    const specs = [
      { query: phrase, tags: ["union", "generated"] },
      { query: tokens.slice(0, 2).map(token => `+${token}`).join(" "), tags: ["intersection", "generated"] },
      { query: `"${phrase}"`, tags: ["phrase", "generated"] }
    ];
    for (const spec of specs) {
      const normalized = normalizeGameQuery(spec.query);
      if (!normalized || seen.has(spec.query)) continue;
      seen.add(spec.query);
      rows.push({
        query: normalized,
        originalQuery: spec.query,
        tags: spec.tags,
        source: "frwiki-generated"
      });
      if (rows.length >= limit) {
        input.destroy();
        break;
      }
    }
    if (rows.length >= limit) break;
  }
  return rows;
}

function commandSize(command) {
  const match = /^TOP_(\d+)(?:_COUNT)?$/u.exec(command);
  if (match) return Number(match[1]);
  if (command === "COUNT" || command === "UNOPTIMIZED_COUNT") return 1;
  return null;
}

function commandCounts(command) {
  return command === "COUNT" || command === "UNOPTIMIZED_COUNT" || /^TOP_\d+_COUNT$/u.test(command);
}

function commandTopKCounts(command) {
  return /^TOP_\d+_COUNT$/u.test(command);
}

function mulberry32(seed) {
  let value = seed >>> 0;
  return () => {
    value += 0x6D2B79F5;
    let t = value;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function shuffled(items, seed) {
  const out = items.slice();
  const random = mulberry32(seed);
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

async function runQuery(engine, command, query) {
  const size = commandSize(command);
  if (!size) return { unsupported: true };
  const started = performance.now();
  if (commandCounts(command)) {
    const hasTerms = Boolean(query.query);
    let topKResponse = null;
    if (hasTerms && commandTopKCounts(command)) {
      topKResponse = await engine.search({ q: query.query, size, exact: false, rerank: false, includeResults: false, authority: false });
    }
    const countResponse = hasTerms
      ? await engine.count({ q: query.query })
      : { total: 0, stats: { plannerLane: "countTermless", countLane: "countTermless" } };
    const durationUs = Math.round((performance.now() - started) * 1000);
    return {
      query: query.originalQuery,
      normalizedQuery: query.query,
      tags: query.tags,
      count: countResponse.total,
      durationUs,
      top: "",
      stats: {
        total: countResponse.total,
        plannerLane: countResponse.stats?.plannerLane || "",
        countLane: countResponse.stats?.countLane || "",
        baseTermCount: countResponse.stats?.baseTermCount || 0,
        termEntriesVisited: countResponse.stats?.termEntriesVisited || 0,
        postingsDecoded: countResponse.stats?.postingsDecoded || 0,
        topKTotal: topKResponse?.total ?? null,
        topKPlannerLane: topKResponse?.stats?.plannerLane || ""
      }
    };
  }
  const response = await engine.search({ q: query.query, size, exact: false, rerank: false, includeResults: false, authority: false });
  const durationUs = Math.round((performance.now() - started) * 1000);
  return {
    query: query.originalQuery,
    normalizedQuery: query.query,
    tags: query.tags,
    count: 1,
    durationUs,
    top: response.results?.[0]?.title || "",
    stats: {
      total: response.total,
      plannerLane: response.stats?.plannerLane || "",
      docPayloadLane: response.stats?.docPayloadLane || "",
      queryBundleHit: Boolean(response.stats?.queryBundleHit),
      typoApplied: Boolean(response.stats?.typoApplied)
    }
  };
}

async function runCommand(engine, command, queries, args) {
  if (!commandSize(command)) {
    return queries.map(query => ({
      query: query.originalQuery,
      normalizedQuery: query.query,
      tags: query.tags,
      count: null,
      duration: [],
      unsupported: true
    }));
  }

  const shuffledQueries = shuffled(queries, args.seed);
  const warmupStarted = performance.now();
  while (args.warmupMs > 0 && performance.now() - warmupStarted < args.warmupMs) {
    for (const query of shuffledQueries) await runQuery(engine, command, query);
  }

  const byQuery = new Map(queries.map(query => [query.originalQuery, {
    query: query.originalQuery,
    normalizedQuery: query.query,
    tags: query.tags,
    count: 0,
    duration: [],
    top: "",
    stats: null
  }]));
  for (let run = 0; run < args.runs; run++) {
    for (const query of shuffledQueries) {
      const row = await runQuery(engine, command, query);
      const target = byQuery.get(query.originalQuery);
      target.count = row.count;
      target.duration.push(row.durationUs);
      target.top = row.top;
      target.stats = row.stats;
    }
  }
  return [...byQuery.values()].map(row => ({ ...row, duration: row.duration.slice().sort((a, b) => a - b) }));
}

function summarizeCommand(rows) {
  const supported = rows.filter(row => !row.unsupported && row.duration.length);
  const best = supported.map(row => row.duration[0]);
  const p50 = supported.map(row => quantile(row.duration, 0.5));
  return {
    queries: rows.length,
    supported: supported.length,
    bestUs: {
      mean: Math.round(mean(best)),
      p50: Math.round(quantile(best, 0.5)),
      p90: Math.round(quantile(best, 0.9)),
      max: Math.round(Math.max(0, ...best))
    },
    medianRunUs: {
      mean: Math.round(mean(p50)),
      p50: Math.round(quantile(p50, 0.5)),
      p90: Math.round(quantile(p50, 0.9)),
      max: Math.round(Math.max(0, ...p50))
    }
  };
}

function summarizeResults(results) {
  return Object.fromEntries(Object.entries(results).map(([command, engines]) => [
    command,
    summarizeCommand(engines.rangefind || [])
  ]));
}

function readJson(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

function upstreamComparison(path, queries, commands) {
  if (!path) return null;
  const upstream = readJson(resolve(path));
  if (!upstream?.results) return { path: resolve(path), error: "unreadable_results_json" };
  const wanted = new Set(queries.map(query => query.originalQuery));
  const out = {};
  for (const command of commands) {
    const commandResults = upstream.results[command];
    if (!commandResults) continue;
    out[command] = {};
    for (const [engine, rows] of Object.entries(commandResults)) {
      const overlapping = rows.filter(row => wanted.has(row.query) && Array.isArray(row.duration) && row.duration.length);
      const best = overlapping.map(row => row.duration[0]);
      out[command][engine] = {
        overlap: overlapping.length,
        bestUsMean: Math.round(mean(best)),
        bestUsP50: Math.round(quantile(best, 0.5))
      };
    }
  }
  return { path: resolve(path), commands: out };
}

function limitSlug(limit) {
  return Number(limit) > 0 ? `limit-${Number(limit)}` : "full-dump";
}

function detectLimit(args) {
  const meta = readJson(resolve(args.root, "data", "frwiki.meta.json"));
  return Number(meta?.docs || meta?.limit || 0);
}

function currentGitCommit() {
  try {
    const commit = execFileSync("git", ["rev-parse", "--short", "HEAD"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
    const dirty = execFileSync("git", ["status", "--short", "--untracked-files=no"], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
    return dirty ? `${commit}-dirty` : commit;
  } catch {
    return null;
  }
}

function benchmarkRoot() {
  return resolve("benchmarks", "frwiki");
}

function updateBenchmarkIndex(record) {
  const path = resolve(benchmarkRoot(), "index.json");
  const index = readJson(path) || { format: INDEX_FORMAT, updatedAt: null, latest: {}, history: [] };
  const key = `${record.kind}:${record.limitSlug}`;
  index.updatedAt = new Date().toISOString();
  index.latest[key] = record;
  index.history = [...(index.history || []).filter(item => item.historyPath !== record.historyPath), record]
    .sort((a, b) => String(a.generatedAt).localeCompare(String(b.generatedAt)));
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, `${JSON.stringify(index, null, 2)}\n`);
}

function writeBenchmarkArtifact(args, report) {
  const generatedAt = report.generatedAt;
  const commit = currentGitCommit();
  const slug = limitSlug(report.limit);
  const timestamp = generatedAt.replace(/[.:]/gu, "-");
  const runId = commit ? `${timestamp}_${commit}` : timestamp;
  const kind = "search-benchmark-game";
  const historyPath = resolve(benchmarkRoot(), "history", kind, slug, `${runId}.json`);
  const latestPath = resolve(benchmarkRoot(), "latest", kind, `${slug}.json`);
  const artifact = {
    format: ARTIFACT_FORMAT,
    kind,
    limit: report.limit,
    limitSlug: slug,
    generatedAt,
    gitCommit: commit,
    historyPath: historyPath.replace(`${resolve(".")}/`, ""),
    latestPath: latestPath.replace(`${resolve(".")}/`, ""),
    summary: report.summary
  };
  report.benchmarkArtifact = artifact;
  mkdirSync(dirname(historyPath), { recursive: true });
  mkdirSync(dirname(latestPath), { recursive: true });
  writeFileSync(historyPath, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(latestPath, `${JSON.stringify(report, null, 2)}\n`);
  updateBenchmarkIndex(artifact);
  return artifact;
}

function printSummary(report) {
  console.log("# Search Benchmark Game compatibility bench\n");
  console.log(`Fixture: ${report.fixture}, docs=${report.limit.toLocaleString()}, queries=${report.queryCount}, runs=${report.runs}, warmupMs=${report.warmupMs}`);
  for (const [command, summary] of Object.entries(report.summary.commands)) {
    console.log(`${command}: best mean ${(summary.bestUs.mean / 1000).toFixed(2)} ms, p50 ${(summary.bestUs.p50 / 1000).toFixed(2)} ms, p90 ${(summary.bestUs.p90 / 1000).toFixed(2)} ms`);
  }
  if (report.upstreamComparison?.path) {
    console.log(`Upstream results: ${report.upstreamComparison.path}`);
  }
  if (report.benchmarkArtifact) console.log(`Report: ${report.benchmarkArtifact.latestPath}`);
}

const args = parseArgs(process.argv.slice(2));
if (!existsSync(args.public)) throw new Error(`Public fixture does not exist: ${args.public}`);
const queries = args.queries
  ? await readGameQueries(resolve(args.queries), args.queryLimit)
  : await generatedQueries(resolve(args.docs), args.queryLimit);
if (!queries.length) throw new Error("No benchmark queries were selected.");

const server = await serveStatic(args.public);
let results;
try {
  const engine = await createSearch({
    baseUrl: new URL(args.basePath, server.url),
    maxPageSize: 1000,
    topKProofMaxK: 1000,
    postingBlockFrontier: 16,
    topKProofCheckInterval: 1024,
    topKBlockBudget: 2048,
    typoMode: "off"
  });
  results = {};
  for (const command of args.commands) {
    results[command] = {
      rangefind: await runCommand(engine, command, queries, args)
    };
  }
} finally {
  await server.close();
}

const report = {
  fixture: "search-benchmark-game-compatible-frwiki",
  protocolSource: "https://github.com/quickwit-oss/search-benchmark-game",
  root: resolve(args.root),
  public: resolve(args.public),
  docs: resolve(args.docs),
  queriesPath: args.queries ? resolve(args.queries) : null,
  querySource: args.queries ? "search-benchmark-game" : "frwiki-generated",
  queryCount: queries.length,
  commands: args.commands,
  runs: args.runs,
  warmupMs: args.warmupMs,
  generatedAt: new Date().toISOString(),
  limit: detectLimit(args),
  notes: [
    "Rangefind is measured through its browser/static-index runtime over local HTTP range requests.",
    "COUNT and TOP_K_COUNT return exact counts for Rangefind-normalized query semantics through a postings-only count path.",
    "Top-k protocol timing disables result hydration, dependency rerank, typo correction, authority rerank, and uses a bounded posting-block budget for pathological broad queries.",
    "COUNT timings do not use the top-k posting-block budget and do not hydrate result payloads.",
    "Lucene query syntax from search-benchmark-game is normalized to Rangefind query text; phrase and intersection tags are retained for grouping."
  ],
  results,
  summary: {
    commands: summarizeResults(results)
  },
  upstreamComparison: upstreamComparison(args.upstreamResults, queries, args.commands)
};
if (args.writeArtifact) writeBenchmarkArtifact(args, report);
if (args.json) console.log(JSON.stringify(report, null, 2));
else printSummary(report);
