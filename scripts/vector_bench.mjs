#!/usr/bin/env node

// Vector search benchmark: builds a synthetic clustered corpus, indexes it,
// and measures recall@k against an in-memory brute-force oracle plus cold
// and warm latency/transfer per query across nprobe settings.
//
// Usage:
//   node scripts/vector_bench.mjs
//   node scripts/vector_bench.mjs --docs=200000 --dims=384 --centers=1024

import { createWriteStream, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import { normalizeVector } from "../src/vector_index.js";
import { createFetchMeter, kb, mean, serveStatic } from "./bench_support.mjs";

function parseArgs(argv) {
  const args = {
    docs: 100000,
    dims: 256,
    centers: 512,
    queries: 50,
    k: 10,
    root: "examples/vector-bench",
    out: ""
  };
  for (const arg of argv) {
    if (arg.startsWith("--docs=")) args.docs = Number(arg.slice(7)) || args.docs;
    else if (arg.startsWith("--dims=")) args.dims = Number(arg.slice(7)) || args.dims;
    else if (arg.startsWith("--centers=")) args.centers = Number(arg.slice(10)) || args.centers;
    else if (arg.startsWith("--queries=")) args.queries = Number(arg.slice(10)) || args.queries;
    else if (arg.startsWith("--root=")) args.root = arg.slice(7);
    else if (arg.startsWith("--out=")) args.out = arg.slice(6);
  }
  if (!args.out) args.out = `${args.root}/vector-bench.json`;
  return args;
}

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function gaussian(random) {
  const u = Math.max(random(), 1e-9);
  const v = random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

function buildNoiseSpectrum(dims, totalNorm) {
  const weights = Float64Array.from({ length: dims }, (_, d) => Math.exp(-d / (dims / 6)));
  const norm = Math.sqrt(weights.reduce((sum, w) => sum + w * w, 0));
  return Float32Array.from(weights, w => (w / norm) * totalNorm);
}

async function generateCorpus(args) {
  const random = mulberry32(1234);
  const { docs, dims, centers } = args;
  const centerRows = new Float32Array(centers * dims);
  for (let c = 0; c < centers; c++) {
    const row = Float32Array.from({ length: dims }, () => gaussian(random));
    normalizeVector(row);
    centerRows.set(row, c * dims);
  }
  const vectors = new Float32Array(docs * dims);
  mkdirSync(resolve(args.root, "data"), { recursive: true });
  const jsonlPath = resolve(args.root, "data", "vectors.jsonl");
  const stream = createWriteStream(jsonlPath);
  // Real embedding models are strongly anisotropic: variance concentrates in
  // a fraction of the dimensions. Model that with a decaying noise spectrum
  // whose total norm stays fixed relative to the unit-norm centers.
  const noiseSpectrum = buildNoiseSpectrum(dims, 0.55);
  for (let i = 0; i < docs; i++) {
    const center = (i * 2654435761) % centers;
    const row = new Float32Array(dims);
    for (let d = 0; d < dims; d++) row[d] = centerRows[center * dims + d] + gaussian(random) * noiseSpectrum[d];
    normalizeVector(row);
    vectors.set(row, i * dims);
    const doc = {
      id: `v${i}`,
      title: `Synthetic document ${i} cluster ${center}`,
      embedding: Buffer.from(row.buffer.slice(0)).toString("base64")
    };
    if (!stream.write(`${JSON.stringify(doc)}\n`)) {
      await new Promise(resolveDrain => stream.once("drain", resolveDrain));
    }
  }
  await new Promise((resolveEnd, reject) => stream.end(err => (err ? reject(err) : resolveEnd())));
  return { jsonlPath, vectors, centerRows };
}

function bruteForceTopK(vectors, dims, query, k) {
  const count = vectors.length / dims;
  const top = [];
  for (let i = 0; i < count; i++) {
    let dot = 0;
    const base = i * dims;
    for (let d = 0; d < dims; d++) dot += vectors[base + d] * query[d];
    if (top.length < k) {
      top.push([i, dot]);
      if (top.length === k) top.sort((a, b) => a[1] - b[1]);
    } else if (dot > top[0][1]) {
      top[0] = [i, dot];
      top.sort((a, b) => a[1] - b[1]);
    }
  }
  return top.sort((a, b) => b[1] - a[1]).map(([i]) => i);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  console.log(`[bench] generating ${args.docs} docs x ${args.dims} dims (${args.centers} true centers)`);
  const t0 = performance.now();
  const { vectors, centerRows } = await generateCorpus(args);
  console.log(`[bench] corpus generated in ${Math.round((performance.now() - t0) / 100) / 10}s`);

  const configPath = resolve(args.root, "rangefind.config.json");
  writeFileSync(configPath, JSON.stringify({
    input: "data/vectors.jsonl",
    output: "public/rangefind",
    scanWorkers: 4,
    fields: [{ name: "title", path: "title", weight: 2.0, b: 0.5 }],
    vectors: [{ name: "embedding", path: "embedding", dims: args.dims }],
    display: ["title"]
  }, null, 2));
  const buildStart = performance.now();
  await build({ configPath });
  const buildSeconds = Math.round((performance.now() - buildStart) / 100) / 10;
  const manifest = JSON.parse(readFileSync(resolve(args.root, "public/rangefind/manifest.min.json"), "utf8"));
  const meta = manifest.vectors.fields.embedding;
  console.log(`[bench] built in ${buildSeconds}s: ${meta.total} vectors, ${meta.clusters} clusters, coarse dims ${meta.coarse_dims}, root ${kb(meta.directory.bytes)} KB`);

  const random = mulberry32(777);
  const queries = [];
  for (let i = 0; i < args.queries; i++) {
    const center = Math.floor(random() * args.centers);
    const query = new Float32Array(args.dims);
    const querySpectrum = buildNoiseSpectrum(args.dims, 0.55);
    for (let d = 0; d < args.dims; d++) query[d] = centerRows[center * args.dims + d] + gaussian(random) * querySpectrum[d];
    normalizeVector(query);
    queries.push(query);
  }
  console.log("[bench] computing brute-force oracle...");
  const oracleStart = performance.now();
  const expected = queries.map(query => new Set(bruteForceTopK(vectors, args.dims, query, args.k)));
  console.log(`[bench] oracle in ${Math.round((performance.now() - oracleStart) / 100) / 10}s`);

  const meter = createFetchMeter(/\/rangefind\//u);
  const report = { docs: args.docs, dims: args.dims, centers: args.centers, build_seconds: buildSeconds, clusters: meta.clusters, coarse_dims: meta.coarse_dims, sweeps: {} };

  for (const nprobe of [1, 4, 8, 16, 32]) {
    const server = await serveStatic(resolve(args.root, "public"));
    try {
      const engine = await createSearch({ baseUrl: `${server.url}rangefind/` });
      let hits = 0;
      let coldKb = 0;
      let coldRequests = 0;
      const times = [];
      for (let i = 0; i < queries.length; i++) {
        meter.reset();
        const start = performance.now();
        const response = await engine.vectorSearch({ vector: queries[i], k: args.k, nprobe, includeResults: false });
        times.push(performance.now() - start);
        const snapshot = meter.snapshot();
        if (i === 0) {
          coldKb = kb(snapshot.bytes);
          coldRequests = snapshot.requests;
        }
        for (const item of response.results) if (expected[i].has(item.index)) hits++;
      }
      const recall = hits / (queries.length * args.k);
      report.sweeps[nprobe] = {
        recall_at_k: Math.round(recall * 1000) / 1000,
        cold_requests: coldRequests,
        cold_kb: Math.round(coldKb * 10) / 10,
        mean_ms: Math.round(mean(times) * 100) / 100
      };
      console.log(`[nprobe=${String(nprobe).padStart(2)}] recall@${args.k}=${recall.toFixed(3)} cold=${coldRequests} req ${Math.round(coldKb)} KB mean=${mean(times).toFixed(2)} ms`);
    } finally {
      await server.close();
    }
  }

  writeFileSync(resolve(args.out), JSON.stringify(report, null, 2));
  console.log(`[bench] wrote ${args.out}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
