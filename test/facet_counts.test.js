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

const CATEGORIES = ["news", "guide", "review", "opinion", "recipe", "profile"];

function facetFixtureDocs() {
  const docs = [];
  for (let i = 0; i < 600; i++) {
    // Skewed category distribution, deterministic.
    const category = CATEGORIES[Math.floor(Math.sqrt(i % 36))];
    const tags = [];
    if (i % 2 === 0) tags.push("even");
    if (i % 3 === 0) tags.push("third");
    if (i % 5 === 0) tags.push("fifth");
    docs.push({
      id: `d${i}`,
      title: `Article ${i} ${i % 4 === 0 ? "alpha" : "omega"}`,
      body: `content ${i % 7 === 0 ? "beta" : "gamma"} filler`,
      category,
      tags,
      year: 2000 + (i % 20)
    });
  }
  return docs;
}

function oracleCounts(docs, field, predicate) {
  const counts = new Map();
  for (const doc of docs) {
    if (predicate && !predicate(doc)) continue;
    for (const value of Array.isArray(doc[field]) ? doc[field] : [doc[field]]) {
      if (value) counts.set(value, (counts.get(value) || 0) + 1);
    }
  }
  return counts;
}

function asMap(facetResult) {
  return new Map(facetResult.values.map(item => [item.value, item.count]));
}

function assertCountsEqual(actual, expected, label) {
  for (const [value, count] of actual) {
    assert.equal(count, expected.get(value), `${label}: ${value} => ${count} != ${expected.get(value)}`);
  }
}

async function buildFixture(configOverrides = {}) {
  const root = await mkdtemp(join(tmpdir(), "rangefind-facets-"));
  const docs = facetFixtureDocs();
  await writeFile(join(root, "docs.jsonl"), docs.map(doc => JSON.stringify(doc)).join("\n"));
  await writeFile(join(root, "rangefind.config.json"), JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    docValueChunkSize: 64,
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: [
      { name: "category", path: "category" },
      { name: "tags", path: "tags" }
    ],
    numbers: [{ name: "year", path: "year", type: "int" }],
    display: ["title", "category"],
    ...configOverrides
  }));
  await build({ configPath: join(root, "rangefind.config.json") });
  return { root, docs };
}

test("facet counts agree with an exhaustive oracle", async () => {
  const { root, docs } = await buildFixture();
  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });

    // Unfiltered browse: exact global counts straight from the dictionary.
    const global = await engine.search({ q: "", facets: ["category", "tags"] });
    assert.equal(global.stats.facetCountLane, "dictionary");
    assert.equal(global.facets.category.exact, true);
    assertCountsEqual(asMap(global.facets.category), oracleCounts(docs, "category"), "global category");
    assertCountsEqual(asMap(global.facets.tags), oracleCounts(docs, "tags"), "global tags");

    // Text query: exact counts over the match set.
    const text = await engine.search({ q: "alpha", facets: ["category", "tags"], size: 5 });
    assert.equal(text.stats.facetCountLane, "text-match-set");
    assert.equal(text.facets.category.exact, true);
    const alphaDocs = doc => doc.title.includes("alpha");
    assertCountsEqual(asMap(text.facets.category), oracleCounts(docs, "category", alphaDocs), "alpha category");
    assertCountsEqual(asMap(text.facets.tags), oracleCounts(docs, "tags", alphaDocs), "alpha tags");

    // Text + filters: counts respect the active filter plan.
    const filteredText = await engine.search({
      q: "alpha",
      filters: { numbers: { year: { min: 2010 } } },
      facets: ["category"]
    });
    assertCountsEqual(
      asMap(filteredText.facets.category),
      oracleCounts(docs, "category", doc => alphaDocs(doc) && doc.year >= 2010),
      "filtered alpha category"
    );

    // Filtered browse without text: exact chunk-scan counts.
    const browse = await engine.search({
      q: "",
      filters: { numbers: { year: { min: 2015 } } },
      facets: ["category", "tags"]
    });
    assert.equal(browse.stats.facetCountLane, "chunk-scan");
    assert.equal(browse.facets.category.exact, true);
    assertCountsEqual(
      asMap(browse.facets.category),
      oracleCounts(docs, "category", doc => doc.year >= 2015),
      "browse category"
    );

    // Facet-filtered browse counts the other field exactly.
    const facetFiltered = await engine.search({
      q: "",
      filters: { facets: { tags: ["fifth"] } },
      facets: ["category"]
    });
    assertCountsEqual(
      asMap(facetFiltered.facets.category),
      oracleCounts(docs, "category", doc => doc.tags.includes("fifth")),
      "tag-filtered category"
    );

    // Requested size caps the value list, keeping the biggest counts.
    const sized = await engine.search({ q: "", facets: { fields: ["category"], size: 2 } });
    assert.equal(sized.facets.category.values.length, 2);
    const fullSorted = [...oracleCounts(docs, "category").entries()].sort((a, b) => b[1] - a[1]);
    assert.equal(sized.facets.category.values[0].count, fullSorted[0][1]);
  } finally {
    await server.close();
  }
});

test("facet counts sample chunks beyond the budget and stay proportionate", async () => {
  const { root, docs } = await buildFixture({ docValueChunkSize: 16 });
  const server = await serveStatic(join(root, "public"));
  try {
    // 600 docs / 16 per chunk = 38 chunks; budget of 4 forces sampling.
    const engine = await createSearch({ baseUrl: server.baseUrl, facetCountMaxChunks: 4 });
    const browse = await engine.search({
      q: "",
      filters: { numbers: { year: { min: 2005 } } },
      facets: ["category"]
    });
    assert.equal(browse.facets.category.exact, false);
    assert.ok(browse.facets.category.sampled_docs > 0);
    const expected = oracleCounts(docs, "category", doc => doc.year >= 2005);
    for (const item of browse.facets.category.values) {
      const truth = expected.get(item.value) || 0;
      assert.ok(
        Math.abs(item.count - truth) <= Math.max(10, truth * 0.6),
        `sampled ${item.value}: ${item.count} vs ${truth}`
      );
    }

    // Text lane with the same budget also flags sampling.
    const text = await engine.search({ q: "gamma", facets: ["category"] });
    assert.equal(text.stats.facetCountLane, "text-match-set");
    assert.equal(text.facets.category.exact, false);
  } finally {
    await server.close();
  }
});
