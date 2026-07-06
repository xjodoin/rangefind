import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import {
  decodeVectorClusterPage,
  dotInt8,
  encodeVectorClusterPage,
  encodeVectorRoot,
  normalizeVector,
  parseVectorRoot,
  quantizeVector,
  trainCentroids,
  vectorFromValue
} from "../src/vector_index.js";

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

test("vector values parse from arrays and base64 and normalize", () => {
  const fromArray = vectorFromValue([3, 4], 2);
  assert.deepEqual([...fromArray], [3, 4]);
  const normalized = normalizeVector(fromArray);
  assert.ok(Math.abs(normalized[0] - 0.6) < 1e-6);
  const buffer = Buffer.alloc(8);
  buffer.writeFloatLE(1, 0);
  buffer.writeFloatLE(2, 4);
  const fromBase64 = vectorFromValue(buffer.toString("base64"), 2);
  assert.deepEqual([...fromBase64], [1, 2]);
  assert.equal(vectorFromValue([1, 2, 3], 2), null);
  assert.equal(vectorFromValue([1, Number.NaN], 2), null);
  assert.equal(normalizeVector(new Float32Array([0, 0])), null);
});

test("int8 quantization keeps cosine ranking fidelity", () => {
  const random = mulberry32(5);
  const dims = 64;
  const query = normalizeVector(Float32Array.from({ length: dims }, () => gaussian(random)));
  const vectors = Array.from({ length: 50 }, () => normalizeVector(Float32Array.from({ length: dims }, () => gaussian(random))));
  const codes = new Int8Array(dims);
  for (const vector of vectors) {
    let exact = 0;
    for (let d = 0; d < dims; d++) exact += query[d] * vector[d];
    const scale = quantizeVector(Float32Array.from(vector), codes, 0);
    const approx = dotInt8(query, codes, 0, dims, scale);
    assert.ok(Math.abs(exact - approx) < 0.03, `int8 dot drifted: ${exact} vs ${approx}`);
  }
});

test("vector cluster pages and roots round trip", () => {
  const count = 100;
  const coarseDims = 16;
  const docs = Array.from({ length: count }, (_, i) => i * 3);
  const ordinals = Array.from({ length: count }, (_, i) => i);
  const scales = Array.from({ length: count }, (_, i) => 0.01 + i / 1000);
  const codes = new Int8Array(count * coarseDims);
  for (let i = 0; i < codes.length; i++) codes[i] = (i % 255) - 127;
  const page = decodeVectorClusterPage(encodeVectorClusterPage({
    field: "embedding",
    clusterIndex: 4,
    coarseDims,
    docs,
    ordinals,
    scales,
    codes
  }), { name: "embedding" });
  assert.equal(page.count, count);
  assert.deepEqual([...page.docs], docs);
  assert.deepEqual([...page.ordinals], ordinals);
  assert.ok(Math.abs(page.scales[10] - scales[10]) < 1e-6);
  assert.deepEqual([...page.codes], [...codes]);

  const dims = 32;
  const clusterCount = 4;
  const centroids = new Float32Array(clusterCount * dims);
  for (let i = 0; i < centroids.length; i++) centroids[i] = Math.sin(i / 5);
  const entry = {
    pack: "0000.abc.bin",
    offset: 12,
    length: 34,
    physicalLength: 34,
    logicalLength: 56,
    checksum: { algorithm: "sha256", value: "cd".repeat(32) }
  };
  const root = parseVectorRoot(encodeVectorRoot({
    field: "embedding",
    dims,
    coarseDims,
    metric: "cosine",
    total: 400,
    centroids: Float32Array.from(centroids),
    clusterCount,
    clusters: Array.from({ length: clusterCount }, (_, c) => ({ count: 100 + c, entry })),
    refine: { rowBytes: dims + 4, rowsPerPack: 1000, packs: ["0000.ref.bin", "0001.ref.bin"] },
    packTable: ["0000.abc.bin"],
    packIndexes: new Map([["0000.abc.bin", 0]])
  }).buffer);
  assert.equal(root.dims, dims);
  assert.equal(root.coarseDims, coarseDims);
  assert.equal(root.clusterCount, clusterCount);
  assert.equal(root.clusters[2].count, 102);
  assert.equal(root.clusters[2].offset, 12);
  assert.equal(root.refine.rowsPerPack, 1000);
  assert.deepEqual(root.refine.packs, ["0000.ref.bin", "0001.ref.bin"]);
  // Quantized centroids stay close to the originals.
  for (let c = 0; c < clusterCount; c++) {
    for (let d = 0; d < dims; d++) {
      const approx = root.centroidCodes[c * dims + d] * root.centroidScales[c];
      assert.ok(Math.abs(approx - Math.sin((c * dims + d) / 5)) < 0.02);
    }
  }
});

test("k-means separates well-clustered data", () => {
  const random = mulberry32(9);
  const dims = 16;
  const centers = Array.from({ length: 4 }, () => normalizeVector(Float32Array.from({ length: dims }, () => gaussian(random))));
  const count = 400;
  const sample = new Float32Array(count * dims);
  for (let i = 0; i < count; i++) {
    const center = centers[i % 4];
    const row = Float32Array.from(center, value => value + gaussian(random) * 0.05);
    normalizeVector(row);
    sample.set(row, i * dims);
  }
  const { centroids, clusterCount } = trainCentroids(sample, dims, 4, { iterations: 8 });
  assert.equal(clusterCount, 4);
  // Every true center should have a trained centroid very close to it.
  for (const center of centers) {
    let best = -Infinity;
    for (let c = 0; c < clusterCount; c++) {
      let dot = 0;
      let norm = 0;
      for (let d = 0; d < dims; d++) {
        dot += center[d] * centroids[c * dims + d];
        norm += centroids[c * dims + d] ** 2;
      }
      best = Math.max(best, dot / Math.sqrt(norm));
    }
    assert.ok(best > 0.98, `no centroid recovered a true center (best cosine ${best})`);
  }
});

async function serveStatic(root) {
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://localhost");
      const path = resolve(root, `.${decodeURIComponent(url.pathname)}`);
      if (!path.startsWith(resolve(root))) {
        response.writeHead(403).end();
        return;
      }
      const data = await readFile(path);
      const range = request.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (range) {
        const start = Number(range[1]);
        const end = Math.min(Number(range[2]), data.length - 1);
        response.writeHead(206, {
          "Accept-Ranges": "bytes",
          "Content-Length": String(end - start + 1),
          "Content-Range": `bytes ${start}-${end}/${data.length}`
        });
        response.end(data.subarray(start, end + 1));
        return;
      }
      response.writeHead(200, { "Content-Length": String(data.length) });
      response.end(data);
    } catch {
      response.writeHead(404).end();
    }
  });
  await new Promise(resolveListen => server.listen(0, "127.0.0.1", resolveListen));
  const { port } = server.address();
  return {
    baseUrl: `http://127.0.0.1:${port}/rangefind/`,
    close: () => new Promise(resolveClose => server.close(resolveClose))
  };
}

const DIMS = 32;
const CLUSTERS = 12;

function vectorFixture() {
  const random = mulberry32(42);
  const centers = Array.from({ length: CLUSTERS }, () => normalizeVector(Float32Array.from({ length: DIMS }, () => gaussian(random))));
  const docs = [];
  const vectors = [];
  for (let i = 0; i < 1500; i++) {
    const center = centers[i % CLUSTERS];
    const vector = Float32Array.from(center, value => value + gaussian(random) * 0.12);
    normalizeVector(vector);
    vectors.push(vector);
    const asBase64 = i % 2 === 0;
    docs.push({
      id: `doc-${i}`,
      title: `Document ${i} topic-${i % CLUSTERS}`,
      body: `synthetic content for cluster ${i % CLUSTERS}`,
      group: `g${i % 5}`,
      embedding: asBase64
        ? Buffer.from(vector.buffer.slice(0)).toString("base64")
        : [...vector].map(value => Math.round(value * 1e6) / 1e6)
    });
  }
  // A doc with no vector: text-searchable, vector-invisible.
  docs.push({ id: "no-vector", title: "Document without embedding topic-0", body: "no vector here", group: "g0" });
  return { docs, vectors, centers, random };
}

function bruteForceTopK(vectors, query, k) {
  const scored = vectors.map((vector, index) => {
    let dot = 0;
    for (let d = 0; d < DIMS; d++) dot += vector[d] * query[d];
    return [index, dot];
  });
  scored.sort((a, b) => b[1] - a[1] || a[0] - b[0]);
  return scored.slice(0, k).map(([index]) => index);
}

test("vector search recall against a brute-force oracle", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-vector-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  const { docs, vectors, centers, random } = vectorFixture();
  await writeFile(docsPath, docs.map(doc => JSON.stringify(doc)).join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: [{ name: "group", path: "group" }],
    vectors: [{ name: "embedding", path: "embedding", dims: DIMS }],
    vectorClusterTargetDocs: 128,
    vectorTrainSample: 1500,
    display: ["title", "group"]
  }));
  await build({ configPath });
  const manifest = JSON.parse(await readFile(join(root, "public", "rangefind", "manifest.min.json"), "utf8"));
  assert.equal(manifest.features.vectors, true);
  assert.equal(manifest.vectors.fields.embedding.total, docs.length - 1);

  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });

    // Queries near cluster centers: recall@10 must be high with a partial
    // probe and near-perfect with a full probe.
    let partialHits = 0;
    let fullHits = 0;
    let checked = 0;
    for (let trial = 0; trial < 20; trial++) {
      const center = centers[trial % CLUSTERS];
      const query = Float32Array.from(center, value => value + gaussian(random) * 0.1);
      normalizeVector(query);
      const expected = new Set(bruteForceTopK(vectors, query, 10));
      const partial = await engine.vectorSearch({ vector: [...query], k: 10, nprobe: 4, includeResults: false });
      const full = await engine.vectorSearch({ vector: [...query], k: 10, nprobe: 1000, refineFactor: 16, includeResults: false });
      for (const item of partial.results) if (expected.has(item.index)) partialHits++;
      for (const item of full.results) if (expected.has(item.index)) fullHits++;
      checked += 10;
    }
    assert.ok(fullHits / checked >= 0.97, `full-probe recall ${fullHits / checked}`);
    assert.ok(partialHits / checked >= 0.8, `partial-probe recall ${partialHits / checked}`);

    // Deterministic across repeat queries.
    const query = [...centers[3]];
    const first = await engine.vectorSearch({ vector: query, k: 5 });
    const second = await engine.vectorSearch({ vector: query, k: 5 });
    assert.deepEqual(first.results.map(item => item.id), second.results.map(item => item.id));
    assert.ok(first.results[0].title.includes("topic-3"), first.results[0].title);

    // The no-vector doc never appears in vector results.
    const wide = await engine.vectorSearch({ vector: query, k: 50, nprobe: 1000 });
    assert.ok(!wide.results.some(item => item.id === "no-vector"));

    // Hybrid fuses text and vector lanes and respects filters.
    const hybrid = await engine.search({ q: "topic-3", vector: query, size: 10 });
    assert.ok(hybrid.stats.hybrid);
    assert.ok(hybrid.results.length > 0);
    assert.ok(hybrid.results.some(item => item.hybrid?.text && item.hybrid?.vector), "some result should rank in both lanes");

    const filtered = await engine.search({ q: "", vector: query, size: 20, filters: { facets: { group: ["g1"] } } });
    assert.ok(filtered.results.length > 0);
    assert.ok(filtered.results.every(item => item.group === "g1"));

    // Clear errors for bad input.
    await assert.rejects(engine.vectorSearch({ vector: [1, 2, 3] }), /finite dimensions/);
  } finally {
    await server.close();
  }
});

test("indexes without vector fields reject vector queries clearly", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-novector-"));
  await writeFile(join(root, "docs.jsonl"), JSON.stringify({ id: "a", title: "Plain", body: "text" }));
  await writeFile(join(root, "rangefind.config.json"), JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    fields: [{ name: "title", path: "title", weight: 4.5, b: 0.55 }],
    display: ["title"]
  }));
  await build({ configPath: join(root, "rangefind.config.json") });
  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });
    await assert.rejects(engine.vectorSearch({ vector: [1, 0] }), /no vector fields/);
  } finally {
    await server.close();
  }
});
