import assert from "node:assert/strict";
import { createServer } from "node:http";
import { existsSync } from "node:fs";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";

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

const TOPICS = ["glacier", "harbor", "meadow", "quarry", "lagoon"];

function makeDoc(i, revision = 1) {
  const topic = TOPICS[i % TOPICS.length];
  const filler = `filler${i % 11} common shared corpus text`.repeat(1 + (i % 3));
  return {
    id: `doc-${i}`,
    title: `Entry ${i} about ${topic}${revision > 1 ? " revised" : ""}`,
    body: `${topic} ${topic} study number ${i}. ${filler}${revision > 1 ? " freshly updated content marker" : ""}`,
    topic,
    year: 2000 + (i % 10)
  };
}

const CONFIG = {
  input: "docs.jsonl",
  output: "public/rangefind",
  scanWorkers: 2,
  // Index every term: budget-dropped terms would trigger typo fallback,
  // which corrects differently per engine and breaks score comparisons.
  targetPostingsPerDoc: 32,
  fields: [
    { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
    { name: "body", path: "body", weight: 1.0, b: 0.75 }
  ],
  facets: [{ name: "topic", path: "topic" }],
  numbers: [{ name: "year", path: "year", type: "int" }],
  suggest: [{ path: "title" }],
  display: ["title", "topic", "year"]
};

async function buildAt(root, docs, configOverrides = {}, buildOptions = {}) {
  await writeFile(join(root, "docs.jsonl"), docs.map(doc => JSON.stringify(doc)).join("\n"));
  await writeFile(join(root, "rangefind.config.json"), JSON.stringify({ ...CONFIG, ...configOverrides }));
  await build({ configPath: join(root, "rangefind.config.json"), ...buildOptions });
}

test("pure-addition generational build matches the full rebuild", async () => {
  const fullRoot = await mkdtemp(join(tmpdir(), "rangefind-gen-full-"));
  const deltaRoot = await mkdtemp(join(tmpdir(), "rangefind-gen-delta-"));
  const baseDocs = Array.from({ length: 200 }, (_, i) => makeDoc(i));
  const addedDocs = Array.from({ length: 100 }, (_, i) => makeDoc(200 + i));

  await buildAt(fullRoot, [...baseDocs, ...addedDocs]);
  await buildAt(deltaRoot, baseDocs);
  await buildAt(deltaRoot, addedDocs, {}, { update: true });

  const rootManifest = JSON.parse(await readFile(join(deltaRoot, "public/rangefind/manifest.min.json"), "utf8"));
  assert.equal(rootManifest.features.generations, true);
  assert.equal(rootManifest.generations.length, 2);
  assert.equal(rootManifest.total, 300);

  const fullServer = await serveStatic(join(fullRoot, "public"));
  const deltaServer = await serveStatic(join(deltaRoot, "public"));
  try {
    const fullEngine = await createSearch({ baseUrl: fullServer.baseUrl });
    const deltaEngine = await createSearch({ baseUrl: deltaServer.baseUrl });

    // Cross-generation comparability invariant: a document added by the
    // delta scores EXACTLY like its structural twin in the base generation
    // (frozen corpus statistics), so merged rankings interleave fairly.
    const twins = await deltaEngine.search({ q: "glacier study number", size: 60 });
    const twinScores = new Map(twins.results.map(item => [item.id, item.score]));
    for (let i = 0; i < 100; i += 5) {
      const baseScore = twinScores.get(`doc-${i}`);
      const deltaScore = twinScores.get(`doc-${i + 200}`);
      if (baseScore == null || deltaScore == null) continue;
      assert.ok(Math.abs(baseScore - deltaScore) < 1e-9,
        `twin docs diverged: doc-${i}=${baseScore} vs doc-${i + 200}=${deltaScore}`);
    }
    assert.ok(twins.results.some(item => item.generation === 0), "merged results include the base generation");
    assert.ok(twins.results.some(item => item.generation === 1), "merged results include the delta generation");

    for (const q of ["glacier study", "harbor", "meadow number", "filler3 common", "quarry study number"]) {
      const fullResponse = await fullEngine.search({ q, size: 100 });
      const deltaResponse = await deltaEngine.search({ q, size: 100 });
      // The generational index keeps the base's frozen statistics, so
      // absolute scores drift slightly from a fresh full rebuild; the
      // matching document SET must be identical and scores stay close.
      assert.deepEqual(
        deltaResponse.results.map(item => item.id).sort(),
        fullResponse.results.map(item => item.id).sort(),
        `result sets diverged for ${JSON.stringify(q)}`
      );
      const fullScores = new Map(fullResponse.results.map(item => [item.id, item.score]));
      for (const item of deltaResponse.results) {
        const reference = fullScores.get(item.id);
        assert.ok(Math.abs(item.score - reference) / Math.max(1, reference) < 0.05,
          `score drifted beyond 5% for ${item.id} on ${JSON.stringify(q)}: ${item.score} vs ${reference} (corrected: ${deltaResponse.correctedQuery || "none"} / ${fullResponse.correctedQuery || "none"})`);
      }
      const scores = deltaResponse.results.map(item => item.score);
      for (let i = 1; i < scores.length; i++) assert.ok(scores[i] <= scores[i - 1] + 1e-9);
    }

    // Facet counts merge across generations exactly.
    const fullFacets = await fullEngine.search({ q: "study", facets: ["topic"], size: 5 });
    const deltaFacets = await deltaEngine.search({ q: "study", facets: ["topic"], size: 5 });
    assert.deepEqual(
      deltaFacets.facets.topic.values,
      fullFacets.facets.topic.values
    );

    // Suggestions merge with summed popularity.
    const fullSuggest = await fullEngine.suggest({ q: "entry 1", size: 5 });
    const deltaSuggest = await deltaEngine.suggest({ q: "entry 1", size: 5 });
    assert.deepEqual(
      deltaSuggest.suggestions.map(item => item.text).sort(),
      fullSuggest.suggestions.map(item => item.text).sort()
    );

    // count() sums exactly with no tombstones.
    const fullCount = await fullEngine.count({ q: "glacier" });
    const deltaCount = await deltaEngine.count({ q: "glacier" });
    assert.equal(deltaCount.total, fullCount.total);
    assert.equal(deltaCount.totalExact, true);

    // Sorted browse and text + sort merge by the real doc-value keys.
    for (const params of [
      { q: "", sort: { field: "year", order: "desc" }, size: 40 },
      { q: "study", sort: "year", size: 40 },
      { q: "glacier", sort: "-year", size: 30 }
    ]) {
      const fullSorted = await fullEngine.search(params);
      const deltaSorted = await deltaEngine.search(params);
      assert.deepEqual(
        deltaSorted.results.map(item => item.year),
        fullSorted.results.map(item => item.year),
        `sorted key sequence diverged for ${JSON.stringify(params)}`
      );
      // Within a key, ordering is score/tie-break dependent, so compare the
      // id multiset per year instead of the raw sequence.
      const byYear = list => {
        const groups = new Map();
        for (const item of list) {
          if (!groups.has(item.year)) groups.set(item.year, []);
          groups.get(item.year).push(item.id);
        }
        for (const ids of groups.values()) ids.sort();
        return groups;
      };
      assert.deepEqual(byYear(deltaSorted.results), byYear(fullSorted.results));
    }
    const sortedPage2 = await deltaEngine.search({ q: "", sort: "-year", size: 25, page: 3 });
    assert.equal(sortedPage2.results.length, 25);
    const sortedAll = await deltaEngine.search({ q: "", sort: "-year", size: 75 });
    assert.deepEqual(
      sortedPage2.results.map(item => item.id),
      sortedAll.results.slice(50, 75).map(item => item.id),
      "sorted pagination must window the same merged order"
    );
  } finally {
    await fullServer.close();
    await deltaServer.close();
  }
});

// Deterministic pseudo-random in [0, 1): tests must not call Math.random.
function unit(i, salt) {
  const x = Math.sin(i * 127.1 + salt * 311.7) * 43758.5453;
  return x - Math.floor(x);
}

function makeGeoVectorDoc(i) {
  const doc = makeDoc(i);
  // Points scattered around Montreal-ish coordinates.
  doc.lat = 45.4 + unit(i, 1) * 0.4;
  doc.lon = -73.8 + unit(i, 2) * 0.4;
  // Embeddings cluster by topic with deterministic jitter, so nearest
  // neighbors of a topic axis are docs of that topic.
  const topicIndex = i % TOPICS.length;
  doc.embedding = Array.from({ length: 8 }, (_, d) =>
    (d === topicIndex ? 1 : 0) + (unit(i, 3 + d) - 0.5) * 0.2);
  return doc;
}

const GEO_VECTOR_CONFIG = {
  ...CONFIG,
  geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
  vectors: [{ name: "embedding", path: "embedding", dims: 8 }]
};

test("geo and vector lanes merge across generations", async () => {
  const fullRoot = await mkdtemp(join(tmpdir(), "rangefind-gen-geovec-full-"));
  const deltaRoot = await mkdtemp(join(tmpdir(), "rangefind-gen-geovec-delta-"));
  const baseDocs = Array.from({ length: 150 }, (_, i) => makeGeoVectorDoc(i));
  const addedDocs = Array.from({ length: 60 }, (_, i) => makeGeoVectorDoc(150 + i));

  await buildAt(fullRoot, [...baseDocs, ...addedDocs], GEO_VECTOR_CONFIG);
  await buildAt(deltaRoot, baseDocs, GEO_VECTOR_CONFIG);
  await buildAt(deltaRoot, addedDocs, GEO_VECTOR_CONFIG, { update: true });

  const fullServer = await serveStatic(join(fullRoot, "public"));
  const deltaServer = await serveStatic(join(deltaRoot, "public"));
  try {
    const fullEngine = await createSearch({ baseUrl: fullServer.baseUrl });
    const deltaEngine = await createSearch({ baseUrl: deltaServer.baseUrl });
    const center = { lat: 45.6, lon: -73.6 };

    // Geo box browse: same match set, both generations represented.
    const box = { minLat: 45.45, maxLat: 45.75, minLon: -73.75, maxLon: -73.45 };
    const fullBox = await fullEngine.search({ q: "", geo: { box }, size: 100 });
    const deltaBox = await deltaEngine.search({ q: "", geo: { box }, size: 100 });
    assert.deepEqual(
      deltaBox.results.map(item => item.id).sort(),
      fullBox.results.map(item => item.id).sort()
    );
    assert.ok(deltaBox.results.some(item => item.generation === 0));
    assert.ok(deltaBox.results.some(item => item.generation === 1));

    // Nearest-first: merged distance order matches the full rebuild (ids at
    // equal rounded distances may tie-break differently, so compare grouped
    // by distance).
    const byDistance = list => list.map(item => `${item.distanceMeters}`).join(",");
    const idsByDistance = list => {
      const groups = new Map();
      for (const item of list) {
        if (!groups.has(item.distanceMeters)) groups.set(item.distanceMeters, []);
        groups.get(item.distanceMeters).push(item.id);
      }
      for (const ids of groups.values()) ids.sort();
      return groups;
    };
    const fullNear = await fullEngine.search({ q: "", geo: { near: center, sort: "distance" }, size: 30 });
    const deltaNear = await deltaEngine.search({ q: "", geo: { near: center, sort: "distance" }, size: 30 });
    assert.equal(byDistance(deltaNear.results), byDistance(fullNear.results));
    assert.deepEqual(idsByDistance(deltaNear.results), idsByDistance(fullNear.results));
    for (let i = 1; i < deltaNear.results.length; i++) {
      assert.ok(deltaNear.results[i].distanceMeters >= deltaNear.results[i - 1].distanceMeters);
    }

    // Text + distance sort and radius-filtered text both merge.
    const fullTextNear = await fullEngine.search({ q: "study", geo: { near: center, sort: "distance" }, size: 25 });
    const deltaTextNear = await deltaEngine.search({ q: "study", geo: { near: center, sort: "distance" }, size: 25 });
    assert.equal(byDistance(deltaTextNear.results), byDistance(fullTextNear.results));
    assert.deepEqual(idsByDistance(deltaTextNear.results), idsByDistance(fullTextNear.results));
    const radius = { near: { ...center, radiusMeters: 12000 } };
    const fullRadius = await fullEngine.search({ q: "glacier study", geo: radius, size: 50 });
    const deltaRadius = await deltaEngine.search({ q: "glacier study", geo: radius, size: 50 });
    assert.deepEqual(
      deltaRadius.results.map(item => item.id).sort(),
      fullRadius.results.map(item => item.id).sort()
    );
    for (const item of deltaRadius.results) assert.ok(item.distanceMeters <= 12000);

    // Vector search: similarities are absolute, so the generational top-k
    // agrees with the full rebuild up to int8 quantization noise.
    const query = Array.from({ length: 8 }, (_, d) => (d === 0 ? 1 : 0));
    const fullVec = await fullEngine.vectorSearch({ vector: query, k: 12, nprobe: 1000, refineFactor: 16 });
    const deltaVec = await deltaEngine.vectorSearch({ vector: query, k: 12, nprobe: 1000, refineFactor: 16 });
    const fullIds = new Set(fullVec.results.map(item => item.id));
    const overlap = deltaVec.results.filter(item => fullIds.has(item.id)).length;
    assert.ok(overlap >= 10, `vector top-k overlap too low: ${overlap}/12`);
    for (const item of deltaVec.results) assert.equal(item.topic, "glacier");
    const scores = deltaVec.results.map(item => item.score);
    for (let i = 1; i < scores.length; i++) assert.ok(scores[i] <= scores[i - 1] + 1e-6);
    assert.ok(deltaVec.results.some(item => item.generation === 0));
    assert.ok(deltaVec.results.some(item => item.generation === 1));

    // Hybrid text + vector fuses at the merged level: same fused ranking and
    // same per-lane ranks as the equivalent single-index build.
    const fullHybrid = await fullEngine.search({ q: "glacier study", vector: query, size: 10 });
    const deltaHybrid = await deltaEngine.search({ q: "glacier study", vector: query, size: 10 });
    assert.deepEqual(
      deltaHybrid.results.map(item => ({ id: item.id, lanes: item.hybrid })),
      fullHybrid.results.map(item => ({ id: item.id, lanes: item.hybrid }))
    );
    assert.ok(deltaHybrid.results.every(item => item.hybrid?.text || item.hybrid?.vector));
  } finally {
    await fullServer.close();
    await deltaServer.close();
  }
});

test("compaction folds generations back into a single index", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-gen-compact-"));
  const baseDocs = Array.from({ length: 120 }, (_, i) => makeDoc(i));
  await buildAt(root, baseDocs);
  const delta = [
    ...Array.from({ length: 10 }, (_, i) => makeDoc(i * 4, 2)),
    ...Array.from({ length: 20 }, (_, i) => makeDoc(120 + i))
  ];
  await buildAt(root, delta, {}, { update: true });

  // Compacting with a partial corpus must fail loudly and keep gen dirs.
  await assert.rejects(
    buildAt(root, baseDocs.slice(0, 50), {}, { compact: true }),
    /must be the FULL corpus/
  );

  const fullCorpus = [
    ...baseDocs.map((doc, i) => (i % 4 === 0 && i / 4 < 10 ? makeDoc(i, 2) : doc)),
    ...Array.from({ length: 20 }, (_, i) => makeDoc(120 + i))
  ];
  await buildAt(root, fullCorpus, {}, { compact: true });

  const manifest = JSON.parse(await readFile(join(root, "public/rangefind/manifest.min.json"), "utf8"));
  assert.equal(manifest.generations, undefined);
  assert.equal(manifest.total, 140);
  assert.equal(existsSync(join(root, "public/rangefind/gen-0001")), false);
  assert.equal(existsSync(join(root, "public/rangefind/manifest.gen0000.min.json")), false);

  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });
    const revised = await engine.search({ q: "freshly updated content marker", size: 30 });
    assert.equal(revised.results.length, 10);
    const glacier = await engine.search({ q: "glacier study", size: 50 });
    assert.ok(glacier.results.length > 0);
    assert.equal(glacier.results.every(item => item.generation === undefined), true);
  } finally {
    await server.close();
  }
});

test("replaced documents tombstone their old version", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-gen-replace-"));
  const baseDocs = Array.from({ length: 120 }, (_, i) => makeDoc(i));
  await buildAt(root, baseDocs);
  // Replace 15 docs with revised content and add 10 new ones.
  const delta = [
    ...Array.from({ length: 15 }, (_, i) => makeDoc(i * 3, 2)),
    ...Array.from({ length: 10 }, (_, i) => makeDoc(120 + i))
  ];
  await buildAt(root, delta, {}, { update: true });

  const rootManifest = JSON.parse(await readFile(join(root, "public/rangefind/manifest.min.json"), "utf8"));
  assert.equal(rootManifest.generations.length, 2);
  assert.equal(rootManifest.generations[0].tombstones.length, 15);
  assert.equal(rootManifest.total, 130);

  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });

    // Revised docs are findable through their new content...
    const updated = await engine.search({ q: "freshly updated content marker", size: 30 });
    assert.equal(updated.results.length, 15);
    for (const item of updated.results) {
      assert.ok(item.title.endsWith("revised"), item.title);
      assert.equal(item.generation, 1);
    }

    // ...and each replaced id resolves to exactly one (new) version.
    const glacier = await engine.search({ q: "glacier study", size: 50 });
    const ids = glacier.results.map(item => item.id);
    assert.equal(new Set(ids).size, ids.length, "no id may appear twice across generations");
    const replaced = glacier.results.find(item => item.id === "doc-0");
    assert.ok(replaced, "replaced doc should still match");
    assert.ok(replaced.title.endsWith("revised"));
  } finally {
    await server.close();
  }
});
