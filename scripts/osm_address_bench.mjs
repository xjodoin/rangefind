#!/usr/bin/env node

// Address quality and latency benchmark for an OSM fixture. It samples real
// structured addresses from the generated JSONL, tests normalized, reordered,
// and component exact forms, and records bounded Node-runtime I/O.

import { createReadStream, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createInterface } from "node:readline";
import { normalizeAddressKey } from "../src/address.js";
import { createNodeSearch, resetNodeRuntimeCaches } from "../src/runtime.node.js";

function parseArgs(argv) {
  const args = { root: "examples/osm-geo", samples: 100, runs: 2, out: "" };
  for (const arg of argv) {
    if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--samples=")) args.samples = Math.max(1, Number(arg.slice("--samples=".length)) || args.samples);
    else if (arg.startsWith("--runs=")) args.runs = Math.max(1, Number(arg.slice("--runs=".length)) || args.runs);
    else if (arg.startsWith("--out=")) args.out = arg.slice("--out=".length);
  }
  if (!args.out) args.out = `${args.root}/osm-address-bench.json`;
  return args;
}

async function sampleAddresses(path, limit) {
  const samples = [];
  const seen = new Set();
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line) continue;
    const doc = JSON.parse(line);
    if (!doc.address || !doc.house_number || !doc.street) continue;
    const key = normalizeAddressKey(doc.address);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    samples.push(doc);
    if (samples.length >= limit) break;
  }
  return samples;
}

function equivalentQuery(address) {
  return String(address)
    .replace(/\bAvenue\b/giu, "Ave.")
    .replace(/\bStreet\b/giu, "St.")
    .replace(/\bBoulevard\b/giu, "Blvd.")
    .replace(/\bRoad\b/giu, "Rd.")
    .replace(/\bDrive\b/giu, "Dr.")
    .replace(/\bLane\b/giu, "Ln.")
    .replace(/\bCourt\b/giu, "Ct.")
    .replace(/\bNorthwest\b/giu, "NW")
    .replace(/\bNortheast\b/giu, "NE")
    .replace(/\bSouthwest\b/giu, "SW")
    .replace(/\bSoutheast\b/giu, "SE");
}

function reorderedQuery(address) {
  const parts = String(address).split(",").map(part => part.trim()).filter(Boolean);
  return parts.length > 1 ? [...parts.slice(1), parts[0]].join(" ") : address;
}

function partialQuery(sample) {
  return [sample.house_number, sample.street, sample.city || sample.suburb || ""].filter(Boolean).join(" ");
}

function percentile(values, fraction) {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * fraction))];
}

function rounded(value) {
  return Math.round(value * 100) / 100;
}

function hasAddress(results, expected) {
  const key = normalizeAddressKey(expected);
  return results.some(result => normalizeAddressKey(result.address) === key);
}

async function runLane(source, samples, queryFor, runs) {
  await resetNodeRuntimeCaches();
  const engine = await createNodeSearch({ source });
  const times = [];
  const coldTimes = [];
  const warmTimes = [];
  let hit1 = 0;
  let hit10 = 0;
  let exactLane = 0;
  let postings = 0;
  try {
    for (let run = 0; run < runs; run++) {
      for (const sample of samples) {
        const started = performance.now();
        const response = await engine.search({ q: queryFor(sample), size: 10 });
        const elapsed = performance.now() - started;
        times.push(elapsed);
        (run === 0 ? coldTimes : warmTimes).push(elapsed);
        if (hasAddress(response.results.slice(0, 1), sample.address)) hit1++;
        if (hasAddress(response.results, sample.address)) hit10++;
        if (response.stats?.plannerLane === "addressAuthorityExact") exactLane++;
        postings += response.stats?.postingsDecoded || 0;
      }
    }
  } finally {
    await engine.close();
  }
  const queries = samples.length * runs;
  return {
    queries,
    hit_at_1: rounded(hit1 / Math.max(1, queries)),
    hit_at_10: rounded(hit10 / Math.max(1, queries)),
    exact_lane_ratio: rounded(exactLane / Math.max(1, queries)),
    postings_decoded: postings,
    latency_ms: {
      mean: rounded(times.reduce((sum, value) => sum + value, 0) / Math.max(1, times.length)),
      p50: rounded(percentile(times, 0.5)),
      p95: rounded(percentile(times, 0.95)),
      max: rounded(Math.max(0, ...times)),
      cold_p50: rounded(percentile(coldTimes, 0.5)),
      cold_p95: rounded(percentile(coldTimes, 0.95)),
      warm_p50: rounded(percentile(warmTimes, 0.5)),
      warm_p95: rounded(percentile(warmTimes, 0.95))
    }
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const root = resolve(args.root);
  const samples = await sampleAddresses(resolve(root, "data", "osm-places.jsonl"), args.samples);
  if (!samples.length) throw new Error("No structured complete addresses found; regenerate the OSM JSONL with the current fixture.");
  console.log(`[address] sampled ${samples.length} unique complete addresses`);
  const source = resolve(root, "public", "rangefind");
  const report = {
    generated_for: root,
    samples: samples.length,
    runs: args.runs,
    lanes: {
      equivalent_exact: await runLane(source, samples, sample => equivalentQuery(sample.address), args.runs),
      reordered_exact: await runLane(source, samples, sample => reorderedQuery(sample.address), args.runs),
      partial_exact: await runLane(source, samples, partialQuery, args.runs)
    }
  };
  writeFileSync(resolve(args.out), JSON.stringify(report, null, 2));
  for (const [name, lane] of Object.entries(report.lanes)) {
    console.log(
      `[address] ${name.padEnd(20)} hit@1=${lane.hit_at_1.toFixed(2)} hit@10=${lane.hit_at_10.toFixed(2)} `
      + `exact=${lane.exact_lane_ratio.toFixed(2)} cold-p50=${lane.latency_ms.cold_p50.toFixed(2)}ms `
      + `warm-p50=${lane.latency_ms.warm_p50.toFixed(2)}ms p95=${lane.latency_ms.p95.toFixed(2)}ms postings=${lane.postings_decoded}`
    );
  }
  console.log(`[address] wrote ${resolve(args.out)}`);
  if (report.lanes.equivalent_exact.hit_at_10 < 0.99 || report.lanes.equivalent_exact.exact_lane_ratio < 0.99) {
    throw new Error("Exact address quality gate failed.");
  }
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
