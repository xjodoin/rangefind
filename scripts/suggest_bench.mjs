#!/usr/bin/env node

// Search-as-you-type benchmark for the Rangefind suggestion sidecar.
//
// Replays realistic keystroke sequences against a built index, measuring
// requests, transfer, and latency per keystroke — both for a cold session
// (first keystroke pays the root fetch) and across a warm typing session,
// which is how an autocomplete box actually behaves.
//
// Usage:
//   node scripts/osm_fixture.mjs all --region=luxembourg
//   node scripts/suggest_bench.mjs --root=examples/osm-geo
//   node scripts/suggest_bench.mjs --root=examples/osm-geo --queries="montreal|tim hortons"

import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createFetchMeter, kb, mean, serveStatic } from "./bench_support.mjs";
import { createSearch } from "../src/runtime.js";

const DEFAULT_QUERIES = [
  "montreal",
  "saint denis",
  "boulangerie",
  "tim hortons",
  "parc",
  "ecole secondaire",
  "rue principale"
];

function parseArgs(argv) {
  const args = { root: "examples/osm-geo", size: 8, queries: DEFAULT_QUERIES, out: "" };
  for (const arg of argv) {
    if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--size=")) args.size = Number(arg.slice("--size=".length)) || args.size;
    else if (arg.startsWith("--queries=")) args.queries = arg.slice("--queries=".length).split("|").filter(Boolean);
    else if (arg.startsWith("--out=")) args.out = arg.slice("--out=".length);
  }
  if (!args.out) args.out = `${args.root}/suggest-bench.json`;
  return args;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const root = resolve(args.root);
  const manifest = JSON.parse(readFileSync(resolve(root, "public/rangefind/manifest.min.json"), "utf8"));
  if (!manifest.features?.suggest) {
    throw new Error(`Index at ${args.root} has no suggestion sidecar; rebuild with suggest fields.`);
  }
  const meta = manifest.suggest;
  console.log(`[bench] ${meta.total} suggestion keys from ${meta.surfaces} surfaces, ${meta.pages} pages, levels ${meta.levels}, root ${kb(meta.directory.bytes)} KB`);

  const meter = createFetchMeter(/\/rangefind\//u);
  const report = { root: args.root, size: args.size, total_keys: meta.total, queries: {} };
  const allKeystrokes = [];

  for (const query of args.queries) {
    const server = await serveStatic(resolve(root, "public"));
    try {
      meter.reset();
      const engine = await createSearch({ baseUrl: `${server.url}rangefind/` });
      const keystrokes = [];
      for (let length = 1; length <= query.length; length++) {
        const q = query.slice(0, length);
        meter.reset();
        const start = performance.now();
        const response = await engine.suggest({ q, size: args.size });
        const ms = performance.now() - start;
        const snapshot = meter.snapshot();
        keystrokes.push({
          q,
          ms: Math.round(ms * 100) / 100,
          requests: snapshot.requests,
          kb: kb(snapshot.bytes),
          suggestions: response.suggestions.length,
          top: response.suggestions[0]?.text || "",
          pages_visited: response.stats.suggestPagesVisited,
          entries_scanned: response.stats.suggestEntriesScanned
        });
      }
      allKeystrokes.push(...keystrokes);
      report.queries[query] = keystrokes;
      const first = keystrokes[0];
      const rest = keystrokes.slice(1);
      console.log(
        `[query] ${JSON.stringify(query).padEnd(20)} first=${first.requests} req ${String(first.kb).padStart(7)} KB ${String(first.ms).padStart(7)} ms | ` +
        `later avg=${rest.length ? Math.round(mean(rest.map(k => k.ms)) * 100) / 100 : 0} ms ${rest.length ? Math.round(mean(rest.map(k => k.kb)) * 10) / 10 : 0} KB | ` +
        `top="${keystrokes.at(-1).top}"`
      );
    } finally {
      await server.close();
    }
  }

  const laterKeystrokes = allKeystrokes.filter((_, i) => i > 0);
  report.summary = {
    keystrokes: allKeystrokes.length,
    mean_ms: Math.round(mean(allKeystrokes.map(k => k.ms)) * 100) / 100,
    mean_kb: Math.round(mean(allKeystrokes.map(k => k.kb)) * 10) / 10,
    mean_requests: Math.round(mean(allKeystrokes.map(k => k.requests)) * 100) / 100,
    zero_fetch_keystrokes: allKeystrokes.filter(k => k.requests === 0).length
  };
  console.log(`[bench] ${report.summary.keystrokes} keystrokes, mean ${report.summary.mean_ms} ms, mean ${report.summary.mean_kb} KB, ${report.summary.zero_fetch_keystrokes} served fully from cache`);
  writeFileSync(resolve(args.out), JSON.stringify(report, null, 2));
  console.log(`[bench] wrote ${args.out}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
