#!/usr/bin/env node

import { createReadStream, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { execFileSync, spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { fold, serveStatic } from "./bench_support.mjs";

const BENCHMARK_ARTIFACT_FORMAT = "rffrwikiquality-artifact-v1";
const BENCHMARK_INDEX_FORMAT = "rffrwikibench-index-v1";

function parseArgs(argv) {
  const args = {
    root: "examples/frwiki",
    docs: "",
    public: "",
    basePath: "rangefind/",
    limit: 0,
    known: 200,
    typos: 200,
    guards: 50,
    size: 10,
    json: false,
    lucene: true,
    forceLucene: false,
    tantivy: true,
    forceTantivy: false,
    sidecarBaseline: "",
    runtime: "",
    artifactKind: "quality"
  };
  for (const arg of argv) {
    if (arg === "--json") args.json = true;
    else if (arg === "--no-lucene") args.lucene = false;
    else if (arg === "--force-lucene") args.forceLucene = true;
    else if (arg === "--no-tantivy") args.tantivy = false;
    else if (arg === "--force-tantivy") args.forceTantivy = true;
    else if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--docs=")) args.docs = arg.slice("--docs=".length);
    else if (arg.startsWith("--public=")) args.public = arg.slice("--public=".length);
    else if (arg.startsWith("--base-path=")) args.basePath = arg.slice("--base-path=".length);
    else if (arg.startsWith("--limit=")) args.limit = Number(arg.slice("--limit=".length)) || 0;
    else if (arg.startsWith("--known=")) args.known = Number(arg.slice("--known=".length)) || args.known;
    else if (arg.startsWith("--typos=")) args.typos = Number(arg.slice("--typos=".length)) || args.typos;
    else if (arg.startsWith("--guards=")) args.guards = Number(arg.slice("--guards=".length)) || args.guards;
    else if (arg.startsWith("--size=")) args.size = Number(arg.slice("--size=".length)) || args.size;
    else if (arg.startsWith("--sidecar-baseline=")) args.sidecarBaseline = arg.slice("--sidecar-baseline=".length);
    else if (arg.startsWith("--runtime=")) args.runtime = arg.slice("--runtime=".length);
    else if (arg.startsWith("--artifact-kind=")) args.artifactKind = arg.slice("--artifact-kind=".length);
  }
  if (!["quality", "quality-sidecar"].includes(args.artifactKind)) throw new Error(`Unsupported artifact kind: ${args.artifactKind}`);
  args.docs ||= resolve(args.root, "data", "frwiki.jsonl");
  args.public ||= resolve(args.root, "public");
  args.work ||= resolve(args.root, "quality");
  return args;
}

function titleTokens(title) {
  return titleWordTokens(title).map(item => item.folded);
}

function titleWordTokens(title) {
  return [...String(title || "").matchAll(/[\p{L}\p{N}]+/gu)]
    .map(match => {
      const text = match[0];
      return { text, folded: fold(text) };
    })
    .filter(item => item.folded.length >= 3);
}

function mutateToken(token, seed, mode) {
  if (mode === "accent") return token;
  if (mode === "french") {
    const replacements = [
      [/qu/u, "k"],
      [/ph/u, "f"],
      [/eau/u, "o"],
      [/ement/u, "ment"],
      [/tion/u, "cion"]
    ];
    for (const [pattern, replacement] of replacements) {
      if (pattern.test(token)) return token.replace(pattern, replacement);
    }
  }
  const innerStart = token.length > 6 ? 1 : 0;
  const span = Math.max(1, token.length - innerStart - (token.length > 6 ? 1 : 0));
  const pos = innerStart + (seed % span);
  if (mode === "delete") return token.slice(0, pos) + token.slice(pos + 1);
  if (mode === "insert") return token.slice(0, pos) + token[pos] + token.slice(pos);
  if (mode === "transpose" && pos < token.length - 1) return token.slice(0, pos) + token[pos + 1] + token[pos] + token.slice(pos + 2);
  const alphabet = "etaoinshrdlucmpgfbvyq";
  const replacement = alphabet[(alphabet.indexOf(token[pos]) + seed + 7) % alphabet.length] || "e";
  return token.slice(0, pos) + replacement + token.slice(pos + 1);
}

function typoQuery(title, seed, preferredMode = null) {
  const modes = ["delete", "insert", "substitute", "transpose", "accent", "french"];
  const orderedModes = preferredMode && modes.includes(preferredMode)
    ? [preferredMode, ...modes.filter(mode => mode !== preferredMode)]
    : modes.map((_, index) => modes[(seed + index) % modes.length]);
  const words = titleWordTokens(title);
  for (const mode of orderedModes) {
    const selected = words
      .map((item, index) => ({ ...item, index }))
      .filter(item => item.folded.length >= 5)
      .find(item => mode !== "accent" || item.folded !== item.text.toLowerCase());
    if (!selected) continue;
    const out = words.map(item => item.text);
    out[selected.index] = mode === "accent"
      ? selected.folded
      : mutateToken(selected.folded, seed + selected.index, mode);
    const q = out.join(" ");
    if (!q || q === words.map(item => item.text).join(" ")) continue;
    return { q, mutation: mode };
  }
  return null;
}

async function selectJudgments(args) {
  const known = [];
  const typos = [];
  const guards = [];
  const input = createReadStream(args.docs, { encoding: "utf8" });
  const rl = createInterface({ input, crlfDelay: Infinity });
  let seen = 0;
  for await (const line of rl) {
    if (!line.trim()) continue;
    const doc = JSON.parse(line);
    const title = String(doc.title || "");
    if (!title || title.length > 96) continue;
    const tokens = titleTokens(title);
    if (!tokens.length) continue;
    seen++;
    if (known.length < args.known) known.push({ set: "known", label: title, q: title, expectedTitle: title, id: String(doc.id || "") });
    else if (typos.length < args.typos) {
      const mutationModes = ["delete", "insert", "substitute", "transpose", "accent", "french"];
      const typo = typoQuery(title, seen + 17, mutationModes[typos.length % mutationModes.length]);
      if (typo) typos.push({ set: "typo", label: `${typo.mutation} ${title}`, q: typo.q, expectedTitle: title, id: String(doc.id || ""), mutation: typo.mutation });
    } else if (guards.length < args.guards) {
      guards.push({ set: "guard", label: title, q: title, expectedTitle: title, id: String(doc.id || "") });
    }
    if (known.length >= args.known && typos.length >= args.typos && guards.length >= args.guards) {
      input.destroy();
      break;
    }
  }
  return { known, typos, guards, queries: [...known, ...typos, ...guards] };
}

function rankByTitle(results, expectedTitle) {
  const index = results.findIndex(result => result.title === expectedTitle);
  return index < 0 ? 0 : index + 1;
}

function metrics(rows) {
  const n = rows.length || 1;
  return {
    n: rows.length,
    hit1: rows.filter(row => row.rank === 1).length / n,
    hit3: rows.filter(row => row.rank > 0 && row.rank <= 3).length / n,
    hit10: rows.filter(row => row.rank > 0 && row.rank <= 10).length / n,
    mrr10: rows.reduce((sum, row) => sum + (row.rank > 0 && row.rank <= 10 ? 1 / row.rank : 0), 0) / n
  };
}

async function loadRuntime(args) {
  if (!args.runtime) return import("../src/runtime.js");
  return import(pathToFileURL(resolve(args.runtime)).href);
}

function sleep(ms) {
  return new Promise(resolveSleep => setTimeout(resolveSleep, ms));
}

async function searchWithRetry(engine, request, retries = 2) {
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await engine.search(request);
    } catch (error) {
      lastError = error;
      if (attempt >= retries) break;
      await sleep(100 * (attempt + 1));
    }
  }
  throw lastError;
}

async function rangefindRows(args, queries) {
  const runtime = await loadRuntime(args);
  const server = await serveStatic(args.public);
  try {
    const engine = await runtime.createSearch({ baseUrl: new URL(args.basePath, server.url) });
    const rows = [];
    for (const query of queries) {
      const start = performance.now();
      const response = await searchWithRetry(engine, { q: query.q, size: args.size });
      rows.push({
        ...query,
        rank: rankByTitle(response.results, query.expectedTitle),
        total: response.total,
        ms: performance.now() - start,
        top: response.results[0]?.title || "",
        correctedQuery: response.correctedQuery || "",
        suggestedQuery: response.suggestedQuery || "",
        typoApplied: Boolean(response.stats?.typoApplied),
        typoSuggested: Boolean(response.stats?.typoSuggested),
        typoShardLookups: response.stats?.typoCandidateShardLookups || response.stats?.typoShardLookups || 0,
        typoCandidateTermsScanned: response.stats?.typoCandidateTermsScanned || 0,
        typoCorrectionPlansExecuted: response.stats?.typoCorrectionPlansExecuted || 0,
        plannerLane: response.stats?.plannerLane || ""
      });
    }
    return rows;
  } finally {
    await server.close();
  }
}

function rangefindReport(rows) {
  const known = rows.filter(row => row.set === "known");
  const typo = rows.filter(row => row.set === "typo");
  const guard = rows.filter(row => row.set === "guard");
  const falseCorrections = guard.filter(row => row.typoApplied || row.correctedQuery || row.typoSuggested || row.suggestedQuery);
  return {
    known: { metrics: metrics(known), rows: known },
    typo: {
      metrics: metrics(typo),
      appliedRate: typo.length ? typo.filter(row => row.typoApplied).length / typo.length : 0,
      suggestedRate: typo.length ? typo.filter(row => row.typoSuggested).length / typo.length : 0,
      rows: typo
    },
    guard: {
      metrics: metrics(guard),
      falseCorrectionRate: guard.length ? falseCorrections.length / guard.length : 0,
      falseCorrections,
      rows: guard
    }
  };
}

function commandAvailable(command) {
  try {
    execFileSync("sh", ["-lc", `command -v ${command}`], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

function commandWorks(command, commandArgs = ["--version"]) {
  try {
    execFileSync(command, commandArgs, { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

function run(command, commandArgs, options = {}) {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(command, commandArgs, {
      stdio: options.stdio || "inherit",
      cwd: options.cwd || process.cwd(),
      env: options.env || process.env
    });
    child.on("error", rejectRun);
    child.on("close", code => {
      if (code === 0) resolveRun();
      else rejectRun(new Error(`${command} exited with ${code}`));
    });
  });
}

function externalQueries(queries) {
  return queries
    .filter(query => query.set === "known" || query.set === "typo")
    .map(query => ({ set: query.set, label: query.label, q: query.q, expectedTitle: query.expectedTitle }));
}

async function luceneReport(args, queries) {
  if (!args.lucene) return { skipped: true, reason: "disabled" };
  if (!commandAvailable("mvn") || !commandAvailable("java")) return { skipped: true, reason: "maven_or_java_unavailable" };
  mkdirSync(args.work, { recursive: true });
  const queryPath = resolve(args.work, "queries.json");
  const luceneReportPath = resolve(args.work, "lucene-report.json");
  writeFileSync(queryPath, JSON.stringify(externalQueries(queries), null, 2));
  try {
    const mavenProject = resolve("scripts/lucene_quality/pom.xml");
    const classpathPath = resolve(args.work, "classpath.txt");
    await run("mvn", ["-q", "-f", mavenProject, "compile", "dependency:build-classpath", `-Dmdep.outputFile=${classpathPath}`]);
    const classpath = `${resolve("scripts/lucene_quality/target/classes")}:${readFileSync(classpathPath, "utf8").trim()}`;
    await run("java", [
      "-cp",
      classpath,
      "rangefind.bench.LuceneFrwikiQuality",
      resolve(args.docs),
      resolve(args.work, "index"),
      queryPath,
      luceneReportPath,
      String(args.size),
      ...(args.forceLucene ? ["--force"] : [])
    ]);
    return JSON.parse(readFileSync(luceneReportPath, "utf8"));
  } catch (error) {
    return { skipped: true, reason: error?.message || String(error) };
  }
}

async function tantivyReport(args, queries) {
  if (!args.tantivy) return { skipped: true, reason: "disabled" };
  mkdirSync(args.work, { recursive: true });
  const queryPath = resolve(args.work, "tantivy-queries.json");
  const tantivyReportPath = resolve(args.work, "tantivy-report.json");
  writeFileSync(queryPath, JSON.stringify(externalQueries(queries), null, 2));
  const manifestPath = resolve("scripts/tantivy_quality/Cargo.toml");
  const indexPath = resolve(args.work, "tantivy-index");
  const cargoArgs = [
    "run",
    "--release",
    "--manifest-path",
    manifestPath,
    "--",
    resolve(args.docs),
    indexPath,
    queryPath,
    tantivyReportPath,
    String(args.size),
    ...(args.forceTantivy ? ["--force"] : [])
  ];
  try {
    const cargo = process.env.TANTIVY_CARGO || "cargo";
    const cargoEnv = {
      ...process.env,
      CARGO_HOME: resolve("scripts/tantivy_quality/target/cargo-home"),
      CARGO_TARGET_DIR: resolve("scripts/tantivy_quality/target")
    };
    if (commandWorks(cargo, ["--version"])) {
      await run(cargo, cargoArgs, { env: cargoEnv });
    } else {
      return { skipped: true, reason: "cargo_unavailable" };
    }
    return JSON.parse(readFileSync(tantivyReportPath, "utf8"));
  } catch (error) {
    return { skipped: true, reason: error?.message || String(error) };
  }
}

function limitSlug(limit) {
  return Number(limit) > 0 ? `limit-${Number(limit)}` : "full-dump";
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

function readJson(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

function qualitySummary(report) {
  const rangefind = report.engines?.rangefind || report.rangefind || report;
  return {
    judgments: report.judgments,
    knownHit10: rangefind.known.metrics.hit10,
    typoHit10: rangefind.typo.metrics.hit10,
    typoMrr10: rangefind.typo.metrics.mrr10,
    typoAppliedRate: rangefind.typo.appliedRate,
    guardFalseCorrectionRate: rangefind.guard.falseCorrectionRate
  };
}

function benchmarkRoot() {
  return resolve("benchmarks", "frwiki");
}

function updateBenchmarkIndex(record) {
  const path = resolve(benchmarkRoot(), "index.json");
  const index = readJson(path) || { format: BENCHMARK_INDEX_FORMAT, updatedAt: null, latest: {}, history: [] };
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
  const slug = limitSlug(args.limit);
  const safeTimestamp = generatedAt.replace(/[.:]/gu, "-");
  const runId = commit ? `${safeTimestamp}_${commit}` : safeTimestamp;
  const historyPath = resolve(benchmarkRoot(), "history", args.artifactKind, slug, `${runId}.json`);
  const latestPath = resolve(benchmarkRoot(), "latest", args.artifactKind, `${slug}.json`);
  const artifact = {
    format: BENCHMARK_ARTIFACT_FORMAT,
    kind: args.artifactKind,
    limit: args.limit,
    limitSlug: slug,
    generatedAt,
    gitCommit: commit,
    historyPath: historyPath.replace(`${resolve(".")}/`, ""),
    latestPath: latestPath.replace(`${resolve(".")}/`, "")
  };
  report.benchmarkArtifact = artifact;
  mkdirSync(dirname(historyPath), { recursive: true });
  mkdirSync(dirname(latestPath), { recursive: true });
  writeFileSync(historyPath, `${JSON.stringify(report, null, 2)}\n`);
  writeFileSync(latestPath, `${JSON.stringify(report, null, 2)}\n`);
  updateBenchmarkIndex({ ...artifact, summary: qualitySummary(report) });
  return artifact;
}

function loadSidecarBaseline(args) {
  const explicit = args.sidecarBaseline ? readJson(resolve(args.sidecarBaseline)) : null;
  if (explicit) return explicit;
  const defaultPath = resolve(benchmarkRoot(), "latest", "quality-sidecar", `${limitSlug(args.limit)}.json`);
  return readJson(defaultPath);
}

function printSummary(report) {
  console.log("# French Wikipedia quality benchmark\n");
  console.log(`Judgments: known=${report.judgments.known}, typo=${report.judgments.typo}, guard=${report.judgments.guard}`);
  const rf = report.engines.rangefind;
  console.log(`Rangefind known Hit@10 ${(rf.known.metrics.hit10 * 100).toFixed(1)}% MRR@10 ${rf.known.metrics.mrr10.toFixed(3)}`);
  console.log(`Rangefind typo  Hit@10 ${(rf.typo.metrics.hit10 * 100).toFixed(1)}% MRR@10 ${rf.typo.metrics.mrr10.toFixed(3)} Applied ${(rf.typo.appliedRate * 100).toFixed(1)}%`);
  console.log(`Rangefind guard false corrections ${(rf.guard.falseCorrectionRate * 100).toFixed(1)}%`);
  const tantivy = report.engines.tantivy;
  if (tantivy?.profiles) {
    const best = Object.entries(tantivy.profiles)
      .map(([name, profile]) => ({ name, typo: profile.typo.metrics.hit10, mrr: profile.typo.metrics.mrr10 }))
      .sort((a, b) => b.typo - a.typo || b.mrr - a.mrr)[0];
    if (best) console.log(`Tantivy best typo ${best.name}: Hit@10 ${(best.typo * 100).toFixed(1)}% MRR@10 ${best.mrr.toFixed(3)}`);
  } else if (tantivy?.skipped) {
    console.log(`Tantivy skipped: ${tantivy.reason}`);
  }
  console.log(`Report: ${report.benchmarkArtifact.latestPath}`);
}

const args = parseArgs(process.argv.slice(2));
const judgments = await selectJudgments(args);
const rfRows = await rangefindRows(args, judgments.queries);
const lucene = await luceneReport(args, judgments.queries);
const tantivy = await tantivyReport(args, judgments.queries);
const sidecarBaseline = args.artifactKind === "quality-sidecar" ? null : loadSidecarBaseline(args);
const report = {
  fixture: "frwiki-quality",
  root: resolve(args.root),
  docs: resolve(args.docs),
  public: resolve(args.public),
  runtime: args.runtime ? resolve(args.runtime) : "src/runtime.js",
  limit: args.limit,
  size: args.size,
  generatedAt: new Date().toISOString(),
  judgments: {
    known: judgments.known.length,
    typo: judgments.typos.length,
    guard: judgments.guards.length,
    typoMutations: Object.fromEntries(["delete", "insert", "substitute", "transpose", "accent", "french"].map(mode => [
      mode,
      judgments.typos.filter(query => query.mutation === mode).length
    ]))
  },
  sidecarBaseline: sidecarBaseline ? {
    source: args.sidecarBaseline || resolve(benchmarkRoot(), "latest", "quality-sidecar", `${limitSlug(args.limit)}.json`),
    summary: qualitySummary(sidecarBaseline)
  } : null,
  engines: {
    rangefind: rangefindReport(rfRows),
    lucene,
    tantivy
  }
};
writeBenchmarkArtifact(args, report);
if (args.json) console.log(JSON.stringify(report, null, 2));
else printSummary(report);
