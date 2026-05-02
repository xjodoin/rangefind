#!/usr/bin/env node

import { createReadStream, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { createInterface } from "node:readline";
import { resolve } from "node:path";
import { createSearch } from "../src/runtime.js";
import { parseArgs, serveStatic } from "./bench_support.mjs";

const DEFAULT_KNOWN = [
  ["Paris", "Paris"],
  ["Révolution française", "Révolution française"],
  ["intelligence artificielle", "Intelligence artificielle"],
  ["changement climatique", "Changement climatique"],
  ["Victor Hugo", "Victor Hugo"],
  ["médecine", "Médecine"],
  ["Québec", "Québec"]
];

const DEFAULT_TYPOS = [
  ["typo Paris", "Pxris", "Paris"],
  ["typo changement climatique", "changment climatiqe", "Changement climatique"],
  ["typo intelligence artificielle", "inteligence artificiel", "Intelligence artificielle"],
  ["typo Victor Hugo", "Viktor Hugo", "Victor Hugo"]
];

const args = parseArgs(process.argv.slice(2), {
  root: "/tmp/rangefind-frwiki-500k-current",
  docs: "",
  basePath: "rangefind/",
  size: 10,
  json: false
});
args.docs ||= resolve(args.root, "data", "frwiki.jsonl");
args.public ||= resolve(args.root, "public");
args.work ||= resolve(args.root, "lucene-quality");
args.forceLucene = process.argv.includes("--force-lucene");

function run(command, commandArgs, options = {}) {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(command, commandArgs, { stdio: options.stdio || "inherit", cwd: options.cwd || process.cwd() });
    child.on("error", rejectRun);
    child.on("close", code => {
      if (code === 0) resolveRun();
      else rejectRun(new Error(`${command} exited with ${code}`));
    });
  });
}

async function presentTitles(docsPath, wantedTitles) {
  const wanted = new Set(wantedTitles);
  const found = new Set();
  const input = createReadStream(docsPath);
  const rl = createInterface({ input, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    const title = JSON.parse(line).title;
    if (wanted.has(title)) found.add(title);
    if (found.size === wanted.size) {
      input.destroy();
      break;
    }
  }
  return found;
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

function rankByTitle(results, expectedTitle) {
  const index = results.findIndex(result => result.title === expectedTitle);
  return index < 0 ? 0 : index + 1;
}

async function rangefindRows(queries) {
  const server = await serveStatic(args.public);
  try {
    const engine = await createSearch({ baseUrl: new URL(args.basePath, server.url) });
    const rows = [];
    for (const query of queries) {
      const start = performance.now();
      const response = await engine.search({ q: query.q, size: args.size });
      rows.push({
        set: query.set,
        label: query.label,
        q: query.q,
        expectedTitle: query.expectedTitle,
        rank: rankByTitle(response.results, query.expectedTitle),
        total: response.total,
        ms: performance.now() - start,
        top: response.results[0]?.title || "",
        typoApplied: Boolean(response.stats?.typoApplied),
        queryBundleHit: Boolean(response.stats?.queryBundleHit),
        plannerLane: response.stats?.plannerLane || "",
        results: response.results.map((result, index) => ({
          rank: index + 1,
          id: result.id,
          title: result.title,
          score: result.score
        }))
      });
    }
    return rows;
  } finally {
    await server.close();
  }
}

function profileSummary(profile) {
  return {
    known: profile.known.metrics,
    typo: profile.typo.metrics
  };
}

function printSummary(report) {
  console.log("# French Wikipedia Lucene quality bench\n");
  console.log(`Docs: ${report.documents.toLocaleString()}`);
  console.log(`Judgments: known=${report.judgments.known}, typo=${report.judgments.typo}`);
  for (const [name, profile] of Object.entries(report.engines)) {
    const summary = profileSummary(profile);
    console.log(`\n${name}`);
    console.log(`  known Hit@1 ${(summary.known.hit1 * 100).toFixed(1)}% Hit@10 ${(summary.known.hit10 * 100).toFixed(1)}% MRR@10 ${summary.known.mrr10.toFixed(3)}`);
    console.log(`  typo  Hit@1 ${(summary.typo.hit1 * 100).toFixed(1)}% Hit@10 ${(summary.typo.hit10 * 100).toFixed(1)}% MRR@10 ${summary.typo.mrr10.toFixed(3)}`);
  }
  console.log(`\nReport: ${report.reportPath}`);
}

mkdirSync(args.work, { recursive: true });
const present = await presentTitles(args.docs, [...DEFAULT_KNOWN.map(([, title]) => title), ...DEFAULT_TYPOS.map(([, , title]) => title)]);
const queries = [
  ...DEFAULT_KNOWN
    .filter(([, expectedTitle]) => present.has(expectedTitle))
    .map(([q, expectedTitle]) => ({ set: "known", label: expectedTitle, q, expectedTitle })),
  ...DEFAULT_TYPOS
    .filter(([, , expectedTitle]) => present.has(expectedTitle))
    .map(([label, q, expectedTitle]) => ({ set: "typo", label, q, expectedTitle }))
];
const queryPath = resolve(args.work, "queries.json");
const luceneReportPath = resolve(args.work, "lucene-report.json");
const combinedReportPath = resolve(args.root, "frwiki-lucene-quality.json");
writeFileSync(queryPath, JSON.stringify(queries, null, 2));

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

const lucene = JSON.parse(readFileSync(luceneReportPath, "utf8"));
const rfRows = await rangefindRows(queries);
const engines = {
  rangefind: {
    known: {
      metrics: metrics(rfRows.filter(row => row.set === "known")),
      rows: rfRows.filter(row => row.set === "known")
    },
    typo: {
      metrics: metrics(rfRows.filter(row => row.set === "typo")),
      rows: rfRows.filter(row => row.set === "typo")
    }
  }
};
for (const [name, profile] of Object.entries(lucene.profiles)) engines[name] = profile;
const report = {
  fixture: "frwiki-lucene-quality",
  root: resolve(args.root),
  docs: resolve(args.docs),
  documents: lucene.documents,
  size: args.size,
  judgments: {
    known: queries.filter(query => query.set === "known").length,
    typo: queries.filter(query => query.set === "typo").length
  },
  generatedAt: new Date().toISOString(),
  lucene: {
    index: lucene.index,
    buildOrOpenMs: lucene.buildOrOpenMs
  },
  engines
};
report.reportPath = combinedReportPath;
writeFileSync(combinedReportPath, JSON.stringify(report, null, 2));
if (args.json) console.log(JSON.stringify(report, null, 2));
else printSummary(report);
