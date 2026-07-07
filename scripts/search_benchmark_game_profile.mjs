#!/usr/bin/env node

import { createReadStream, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { createInterface } from "node:readline";
import { basename, dirname, resolve } from "node:path";
import { DEFAULT_ANALYZER } from "../src/analysis.js";
import { createSearch } from "../src/runtime.js";
import { mean, quantile, serveStatic } from "./bench_support.mjs";

const ARTIFACT_FORMAT = "rfsearchbenchmarkgame-profile-v1";
const DEFAULT_COMMANDS = ["TOP_10", "TOP_100", "TOP_1000"];
const DEFAULT_INDEX = "/tmp/search-benchmark-game/engines/rangefind/index/public/rangefind";
const DEFAULT_QUERIES = "/tmp/search-benchmark-game/queries.txt";

function parseArgs(argv) {
  const args = {
    index: DEFAULT_INDEX,
    queries: DEFAULT_QUERIES,
    commands: DEFAULT_COMMANDS,
    queryLimit: 50,
    runs: 1,
    warmupRuns: 1,
    output: "",
    artifact: true,
    json: false,
    fetchConcurrency: 64,
    fetchRetries: 2,
    cpuProfilePath: ""
  };
  for (const arg of argv) {
    if (arg === "--json") args.json = true;
    else if (arg === "--no-artifact") args.artifact = false;
    else if (arg.startsWith("--index=")) args.index = arg.slice("--index=".length);
    else if (arg.startsWith("--queries=")) args.queries = arg.slice("--queries=".length);
    else if (arg.startsWith("--commands=")) args.commands = arg.slice("--commands=".length).split(/[,\s]+/u).filter(Boolean);
    else if (arg.startsWith("--query-limit=")) args.queryLimit = Number(arg.slice("--query-limit=".length)) || args.queryLimit;
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--warmup-runs=")) args.warmupRuns = Number(arg.slice("--warmup-runs=".length)) || 0;
    else if (arg.startsWith("--output=")) args.output = arg.slice("--output=".length);
    else if (arg.startsWith("--fetch-concurrency=")) args.fetchConcurrency = Number(arg.slice("--fetch-concurrency=".length)) || args.fetchConcurrency;
    else if (arg.startsWith("--fetch-retries=")) args.fetchRetries = Number(arg.slice("--fetch-retries=".length)) || 0;
    else if (arg.startsWith("--cpu-profile-path=")) args.cpuProfilePath = arg.slice("--cpu-profile-path=".length);
  }
  return args;
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchWithRetries(nativeFetch, input, init, retries) {
  let attempt = 0;
  while (true) {
    try {
      return await nativeFetch(input, init);
    } catch (error) {
      if (attempt >= retries) throw error;
      attempt++;
      await delay(20 * attempt);
    }
  }
}

function installFetchLimit(limit, retries) {
  const max = Math.max(1, Math.floor(Number(limit) || 64));
  const attempts = Math.max(0, Math.floor(Number(retries) || 0));
  const nativeFetch = globalThis.fetch;
  let active = 0;
  const queue = [];
  globalThis.fetch = (input, init) => new Promise((resolve, reject) => {
    const run = () => {
      active++;
      fetchWithRetries(nativeFetch, input, init, attempts)
        .then(resolve, reject)
        .finally(() => {
          active--;
          queue.shift()?.();
        });
    };
    if (active < max) run();
    else queue.push(run);
  });
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

async function readGameQueries(path, limit) {
  const rows = [];
  const input = createReadStream(path, { encoding: "utf8" });
  const rl = createInterface({ input, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line.trim()) continue;
    const parsed = JSON.parse(line);
    rows.push({
      query: normalizeGameQuery(parsed.query),
      originalQuery: String(parsed.query || ""),
      tags: Array.isArray(parsed.tags) ? parsed.tags : []
    });
    if (rows.length >= limit) {
      input.destroy();
      break;
    }
  }
  return rows;
}

function readManifest(indexPath) {
  const full = resolve(indexPath, "manifest.json");
  const min = resolve(indexPath, "manifest.min.json");
  const path = existsSync(full) ? full : min;
  return existsSync(path) ? JSON.parse(readFileSync(path, "utf8")) : null;
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

function cpuProfileMetadata(args) {
  const nameArg = process.execArgv.find(arg => arg.startsWith("--cpu-prof-name="));
  const dirArg = process.execArgv.find(arg => arg.startsWith("--cpu-prof-dir="));
  const requested = process.execArgv.includes("--cpu-prof") || process.execArgv.some(arg => arg.startsWith("--cpu-prof="));
  const name = nameArg ? nameArg.slice("--cpu-prof-name=".length) : "";
  const dir = dirArg ? dirArg.slice("--cpu-prof-dir=".length) : "";
  return {
    requested,
    path: args.cpuProfilePath || (requested && name ? resolve(dir || ".", name) : ""),
    execArgv: process.execArgv
  };
}

function compactTrace(trace) {
  if (!trace?.spans?.length) return null;
  return {
    totalMs: trace.totalMs || 0,
    spans: trace.spans.slice(0, 20).map(span => ({
      name: span.name,
      count: span.count,
      totalMs: span.totalMs,
      maxMs: span.maxMs
    }))
  };
}

function compactStats(response) {
  const stats = response?.stats || {};
  return {
    total: response?.total ?? 0,
    approximate: Boolean(response?.approximate),
    plannerLane: stats.plannerLane || "",
    exact: Boolean(stats.exact),
    topKProven: Boolean(stats.topKProven),
    totalExact: Boolean(stats.totalExact),
    blocksDecoded: stats.blocksDecoded || 0,
    postingsDecoded: stats.postingsDecoded || 0,
    postingsAccepted: stats.postingsAccepted || 0,
    baseTermCount: stats.baseTermCount || 0,
    minShouldMatch: stats.minShouldMatch || 0,
    termEntriesVisited: stats.termEntriesVisited || 0,
    countLane: stats.countLane || "",
    skippedBlocks: stats.skippedBlocks || 0,
    postingBlockFrontier: stats.postingBlockFrontier || 0,
    postingBlockFrontierBatches: stats.postingBlockFrontierBatches || 0,
    postingBlockFrontierFetchedBlocks: stats.postingBlockFrontierFetchedBlocks || 0,
    postingBlockFrontierFetchGroups: stats.postingBlockFrontierFetchGroups || 0,
    topKProofCheckInterval: stats.topKProofCheckInterval || 0,
    topKBlockBudget: stats.topKBlockBudget || 0,
    topKBlockBudgetExhausted: Boolean(stats.topKBlockBudgetExhausted),
    docRangeBlocksVisited: stats.docRangeBlocksVisited || 0,
    docRangePostingRowsScanned: stats.docRangePostingRowsScanned || 0,
    docRangeFetchedBlocks: stats.docRangeFetchedBlocks || 0,
    docPayloadLane: stats.docPayloadLane || "",
    docPayloadPages: stats.docPayloadPages || 0,
    docPayloadOverfetchDocs: stats.docPayloadOverfetchDocs || 0,
    rerankCandidates: stats.rerankCandidates || 0,
    dependencyPostingsScanned: stats.dependencyPostingsScanned || 0
  };
}

function memorySnapshot() {
  const memory = process.memoryUsage();
  return {
    rss: memory.rss,
    heapUsed: memory.heapUsed,
    heapTotal: memory.heapTotal,
    external: memory.external
  };
}

async function runQuery(engine, command, query, options = {}) {
  const size = commandSize(command);
  const started = performance.now();
  if (!size) {
    return {
      unsupported: true,
      durationUs: Math.round((performance.now() - started) * 1000),
      memory: memorySnapshot()
    };
  }

  const hasTerms = Boolean(query.query && DEFAULT_ANALYZER.analyzeTerms(query.query).length);
  if (commandCounts(command)) {
    let searchResponse = null;
    if (hasTerms && commandTopKCounts(command)) {
      searchResponse = await engine.search({
        q: query.query,
        size,
        exact: false,
        rerank: false,
        includeResults: false,
        authority: false,
        trace: options.trace === true
      });
    }
    const countResponse = hasTerms
      ? await engine.count({ q: query.query, trace: options.trace === true })
      : {
          total: 0,
          approximate: false,
          totalExact: true,
          stats: { plannerLane: "countTermless", countLane: "countTermless", exact: true, totalExact: true }
        };
    return {
      unsupported: false,
      termless: !hasTerms,
      count: countResponse.total,
      durationUs: Math.round((performance.now() - started) * 1000),
      memory: memorySnapshot(),
      total: countResponse.total,
      topTotal: searchResponse?.total ?? null,
      stats: compactStats(countResponse),
      topKStats: searchResponse ? compactStats(searchResponse) : null,
      trace: compactTrace(countResponse.trace || countResponse.stats?.trace),
      topKTrace: searchResponse ? compactTrace(searchResponse.stats?.trace) : null
    };
  }

  if (!hasTerms) {
    return {
      unsupported: false,
      termless: true,
      count: 1,
      durationUs: Math.round((performance.now() - started) * 1000),
      memory: memorySnapshot(),
      stats: { plannerLane: "termless" },
      trace: null
    };
  }

  const response = await engine.search({
    q: query.query,
    size,
    exact: false,
    rerank: false,
    includeResults: false,
    authority: false,
    trace: options.trace === true
  });
  return {
    unsupported: false,
    count: 1,
    durationUs: Math.round((performance.now() - started) * 1000),
    memory: memorySnapshot(),
    total: response.total,
    top: response.results?.[0]?.title || "",
    stats: compactStats(response),
    trace: compactTrace(response.stats?.trace)
  };
}

async function runCommand(engine, command, queries, args) {
  for (let warmup = 0; warmup < args.warmupRuns; warmup++) {
    for (const query of queries) await runQuery(engine, command, query, { trace: false });
  }

  const rows = [];
  for (const query of queries) {
    const runs = [];
    for (let run = 0; run < args.runs; run++) {
      runs.push(await runQuery(engine, command, query, { trace: true }));
    }
    const durations = runs.filter(run => !run.unsupported).map(run => run.durationUs);
    rows.push({
      query: query.originalQuery,
      normalizedQuery: query.query,
      tags: query.tags,
      unsupported: runs.every(run => run.unsupported),
      termless: runs.some(run => run.termless),
      bestUs: durations.length ? Math.min(...durations) : 0,
      meanUs: Math.round(mean(durations)),
      maxUs: durations.length ? Math.max(...durations) : 0,
      runs
    });
  }
  return rows;
}

function summarizeCommand(rows) {
  const supported = rows.filter(row => !row.unsupported);
  const best = supported.map(row => row.bestUs);
  const slowest = supported
    .slice()
    .sort((left, right) => right.bestUs - left.bestUs)
    .slice(0, 10)
    .map(row => ({
      query: row.query,
      normalizedQuery: row.normalizedQuery,
      bestUs: row.bestUs,
      meanUs: row.meanUs,
      plannerLane: row.runs.find(run => run.stats)?.stats?.plannerLane || ""
    }));
  return {
    queries: rows.length,
    supported: supported.length,
    unsupported: rows.length - supported.length,
    bestUs: {
      mean: Math.round(mean(best)),
      p50: Math.round(quantile(best, 0.5)),
      p90: Math.round(quantile(best, 0.9)),
      max: Math.round(Math.max(0, ...best))
    },
    slowest
  };
}

function benchmarkPaths(report, args) {
  if (args.output) return { output: resolve(args.output), latest: "", history: "" };
  const docs = report.index?.docs || 0;
  const slug = docs > 0 ? `limit-${docs}` : "unknown";
  const timestamp = report.generatedAt.replace(/[.:]/gu, "-");
  const commit = report.gitCommit || "unknown";
  const history = resolve("benchmarks", "frwiki", "history", "search-benchmark-game", `profile-${slug}`, `${timestamp}_${commit}.json`);
  const latest = resolve("benchmarks", "frwiki", "latest", "search-benchmark-game", `profile-${slug}.json`);
  return { output: latest, latest, history };
}

function writeReport(report, args) {
  if (!args.artifact && !args.output) return null;
  const paths = benchmarkPaths(report, args);
  mkdirSync(dirname(paths.output), { recursive: true });
  writeFileSync(paths.output, `${JSON.stringify(report, null, 2)}\n`);
  if (paths.history) {
    mkdirSync(dirname(paths.history), { recursive: true });
    writeFileSync(paths.history, `${JSON.stringify(report, null, 2)}\n`);
  }
  return paths;
}

function printSummary(report, paths) {
  console.log("# Search Benchmark Game profile\n");
  console.log(`Index: ${report.indexPath}`);
  console.log(`Queries: ${report.queryCount}, commands: ${report.commands.join(", ")}, runs: ${report.runs}, warmupRuns: ${report.warmupRuns}`);
  for (const [command, summary] of Object.entries(report.summary.commands)) {
    console.log(`${command}: best mean ${(summary.bestUs.mean / 1000).toFixed(2)} ms, p50 ${(summary.bestUs.p50 / 1000).toFixed(2)} ms, p90 ${(summary.bestUs.p90 / 1000).toFixed(2)} ms, max ${(summary.bestUs.max / 1000).toFixed(2)} ms`);
  }
  if (report.cpuProfile.path) console.log(`CPU profile: ${report.cpuProfile.path}`);
  if (paths?.output) console.log(`Report: ${paths.output}`);
}

const args = parseArgs(process.argv.slice(2));
const indexPath = resolve(args.index);
const queryPath = resolve(args.queries);
if (!existsSync(indexPath)) throw new Error(`Rangefind index does not exist: ${indexPath}`);
if (!existsSync(queryPath)) throw new Error(`Search Benchmark Game queries do not exist: ${queryPath}`);
installFetchLimit(args.fetchConcurrency, args.fetchRetries);

const manifest = readManifest(indexPath);
const queries = await readGameQueries(queryPath, args.queryLimit);
if (!queries.length) throw new Error("No Search Benchmark Game queries were selected.");

const server = await serveStatic(dirname(indexPath));
let results;
try {
  const engine = await createSearch({
    baseUrl: new URL(`${basename(indexPath)}/`, server.url),
    maxPageSize: 1000,
    topKProofMaxK: 1000,
    postingBlockFrontier: 16,
    topKProofCheckInterval: 1024,
    topKBlockBudget: 2048,
    typoMode: "off"
  });
  results = {};
  for (const command of args.commands) results[command] = await runCommand(engine, command, queries, args);
} finally {
  await server.close();
}

const report = {
  format: ARTIFACT_FORMAT,
  protocolSource: "https://github.com/quickwit-oss/search-benchmark-game",
  generatedAt: new Date().toISOString(),
  gitCommit: currentGitCommit(),
  indexPath,
  queriesPath: queryPath,
  index: {
    docs: manifest?.total || 0,
    format: manifest?.format || ""
  },
  queryCount: queries.length,
  commands: args.commands,
  runs: args.runs,
  warmupRuns: args.warmupRuns,
  searchOptions: {
    maxPageSize: 1000,
    topKProofMaxK: 1000,
    postingBlockFrontier: 16,
    topKProofCheckInterval: 1024,
    topKBlockBudget: 2048,
    rerank: false,
    includeResults: false,
    authority: false,
    typoMode: "off",
    fetchConcurrency: args.fetchConcurrency,
    fetchRetries: args.fetchRetries
  },
  cpuProfile: cpuProfileMetadata(args),
  notes: [
    "This profile is single-process and deterministic so Node --cpu-prof output maps directly to the measured query replay.",
    "COUNT and TOP_K_COUNT return exact counts for Rangefind-normalized query semantics through a postings-only count path.",
    "Top-k profile timing disables result hydration, dependency rerank, typo correction, authority rerank, and uses a bounded posting-block budget for pathological broad queries.",
    "COUNT timings do not use the top-k posting-block budget and do not hydrate result payloads.",
    "Search Benchmark Game query syntax is normalized to Rangefind query text before execution."
  ],
  results,
  summary: {
    commands: Object.fromEntries(Object.entries(results).map(([command, rows]) => [command, summarizeCommand(rows)]))
  }
};

const paths = writeReport(report, args);
if (args.json) console.log(JSON.stringify(report, null, 2));
else printSummary(report, paths);
