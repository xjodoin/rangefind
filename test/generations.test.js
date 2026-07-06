import assert from "node:assert/strict";
import { createServer } from "node:http";
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

    // Unsupported lanes fail clearly instead of silently degrading.
    await assert.rejects(deltaEngine.search({ q: "glacier", sort: { field: "year", order: "desc" } }), /not yet supported/);
    await assert.rejects(deltaEngine.vectorSearch({ vector: [1, 0] }), /not yet supported/);
  } finally {
    await fullServer.close();
    await deltaServer.close();
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
