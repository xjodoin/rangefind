import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { build } from "../src/builder.js";
import { readConfig } from "../src/config.js";
import { collectScoringStats, loadScoringStats } from "../src/scoring_stats.js";
import { writeShardedRootManifest } from "../src/index_shards.js";
import { writeTextRoutingIndex } from "../src/text_routing.js";
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
const SUGGEST_MARKERS = ["", "alpineunique", "borealunique", "coastalunique"];

// Three geographic bands, one per shard; vocabulary is shared across bands
// so text queries exercise cross-shard merging.
function makeDoc(i, band) {
  const topic = TOPICS[i % TOPICS.length];
  const filler = `filler${i % 11} common shared corpus text`.repeat(1 + (i % 3));
  return {
    id: `doc-${band}-${i}`,
    title: i === 0
      ? `${SUGGEST_MARKERS[band]} landmark in band ${band}`
      : i === 119
        ? "Shared landmark"
      : `Entry ${i} about ${topic} in band ${band}`,
    // bandmarkN is unique to its band: text routing should resolve queries
    // containing it to exactly one shard.
    body: `${topic} ${topic} study number ${i} bandmark${band}. ${filler}`,
    topic,
    population: 100 + ((i * 37) % 5000),
    lat: band * 10 + (i % 100) / 100,
    lon: -70 + (i % 100) / 100
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
  numbers: [{ name: "population", path: "population", type: "int" }],
  geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
  suggest: [{ path: "title" }],
  display: ["title", "topic", "population"]
};

const BANDS = [1, 2, 3];

function bandDocs(band, count = 120) {
  return Array.from({ length: count }, (_, i) => makeDoc(i, band));
}

async function buildMonolithic(root, docsByBand) {
  await writeFile(join(root, "docs.jsonl"), docsByBand.flat().map(doc => JSON.stringify(doc)).join("\n"));
  await writeFile(join(root, "rangefind.config.json"), JSON.stringify(CONFIG));
  await build({ configPath: join(root, "rangefind.config.json") });
}

async function buildSharded(root, docsByBand) {
  for (let b = 0; b < BANDS.length; b++) {
    await writeFile(join(root, `band-${BANDS[b]}.jsonl`), docsByBand[b].map(doc => JSON.stringify(doc)).join("\n"));
  }
  await writeFile(join(root, "template.config.json"), JSON.stringify(CONFIG));
  const templateConfig = await readConfig(join(root, "template.config.json"));
  const stats = await collectScoringStats({
    config: templateConfig,
    inputs: BANDS.map(band => ({ id: `band-${band}`, input: join(root, `band-${band}.jsonl`) })),
    outDir: join(root, "scoring-stats")
  });
  for (let b = 0; b < BANDS.length; b++) {
    const band = BANDS[b];
    const configPath = join(root, `band-${band}.config.json`);
    await writeFile(configPath, JSON.stringify({
      ...CONFIG,
      input: `band-${band}.jsonl`,
      output: `public/rangefind/shards/band-${band}`,
      scoringStats: "scoring-stats/scoring-stats.json",
      // One shard exercises the worker-thread df path, the rest main-thread.
      partitionReducerWorkers: b === 0 ? 2 : 0
    }));
    await build({ configPath });
  }
  const textRouting = await writeTextRoutingIndex({
    outDir: join(root, "public/rangefind"),
    shards: BANDS.map(band => ({ id: `band-${band}`, dir: join(root, `public/rangefind/shards/band-${band}`) }))
  });
  return writeShardedRootManifest({
    outDir: join(root, "public/rangefind"),
    shards: BANDS.map(band => ({
      id: `band-${band}`,
      path: `shards/band-${band}/`,
      bbox: stats.stats.inputs.find(item => item.id === `band-${band}`)?.bbox,
      // Multi-level scoping: bands 1-2 form the "south" group, all three
      // sit in "world".
      groups: band <= 2 ? ["south", "world"] : ["world"]
    })),
    scoringStats: stats.stats,
    textRouting
  });
}

test("sharded index matches the monolithic build exactly", { timeout: 120000 }, async () => {
  const monoRoot = await mkdtemp(join(tmpdir(), "rangefind-shard-mono-"));
  const shardRoot = await mkdtemp(join(tmpdir(), "rangefind-shard-split-"));
  const docsByBand = BANDS.map(band => bandDocs(band));

  await buildMonolithic(monoRoot, docsByBand);
  const rootManifest = await buildSharded(shardRoot, docsByBand);
  assert.equal(rootManifest.total, 360);
  assert.equal(rootManifest.shards.length, 3);
  assert.ok(rootManifest.shards.every(shard => Array.isArray(shard.bbox)));
  assert.equal(rootManifest.features.facetSummaryUint32, true);
  assert.ok(rootManifest.shards.every(shard => shard.features?.facetSummaryUint32 === true));

  const monoServer = await serveStatic(join(monoRoot, "public"));
  const shardServer = await serveStatic(join(shardRoot, "public"));
  try {
    const mono = await createSearch({ baseUrl: monoServer.baseUrl });
    const sharded = await createSearch({ baseUrl: shardServer.baseUrl });

    const traced = await sharded.search({ q: "glacier bandmark2", size: 5, trace: true });
    const fetchSpans = traced.stats.trace?.spans?.filter(span => span.name.endsWith(".fetch")) || [];
    assert.ok(fetchSpans.reduce((sum, span) => sum + span.count, 0) > 0, "federated trace should count static reads");
    assert.ok(traced.stats.trace.totalBytes > 0, "federated trace should report transferred bytes");
    assert.ok(traced.stats.trace.spans.some(span => span.name === "shards.searchTotal"));

    // Text queries: identical per-document scores (frozen stats invariant)
    // and identical score-ordered ranking. Search `total` is approximate
    // under early termination, so exact totals are compared via count().
    for (const q of ["glacier study", "harbor", "meadow number", "filler3 common", "quarry study number"]) {
      const monoResponse = await mono.search({ q, size: 50 });
      const shardResponse = await sharded.search({ q, size: 50 });
      const monoScores = new Map(monoResponse.results.map(item => [item.id, item.score]));
      const shardScores = new Map(shardResponse.results.map(item => [item.id, item.score]));
      // Every document both sides ranked must score identically.
      let compared = 0;
      for (const [id, score] of shardScores) {
        if (!monoScores.has(id)) continue;
        compared++;
        assert.ok(Math.abs(score - monoScores.get(id)) < 1e-9,
          `score diverged for ${id} on "${q}": ${score} vs ${monoScores.get(id)}`);
      }
      assert.ok(compared >= Math.min(shardScores.size, 40), `too few overlapping results for "${q}"`);
      // Ranking agreement modulo ties: the score sequences must be equal.
      assert.deepEqual(
        shardResponse.results.map(item => item.score),
        monoResponse.results.map(item => item.score),
        `score ordering diverged for "${q}"`
      );
      assert.ok(new Set(shardResponse.results.map(item => item.shard)).size > 1,
        `results for "${q}" should span shards`);
    }

    // Counts and facets add across shards.
    for (const q of ["glacier", "glacier study", "harbor", "study"]) {
      const monoCount = await mono.count({ q });
      const shardCount = await sharded.count({ q });
      assert.equal(shardCount.total, monoCount.total, `count mismatch for "${q}"`);
    }

    const monoFacets = await mono.search({ q: "study", size: 10, facets: ["topic"] });
    const shardFacets = await sharded.search({ q: "study", size: 10, facets: ["topic"] });
    const monoTopic = Object.fromEntries(monoFacets.facets.topic.values.map(item => [item.value, item.count]));
    const shardTopic = Object.fromEntries(shardFacets.facets.topic.values.map(item => [item.value, item.count]));
    assert.deepEqual(shardTopic, monoTopic);

    // Suggest merges across shards.
    const monoSuggest = await mono.suggest({ q: "entr" });
    const shardSuggest = await sharded.suggest({ q: "entr" });
    assert.ok(shardSuggest.suggestions.length > 0);
    assert.deepEqual(
      shardSuggest.suggestions.map(item => item.text).slice(0, 3),
      monoSuggest.suggestions.map(item => item.text).slice(0, 3)
    );
    assert.equal(shardSuggest.stats.shardsQueried, 3, "broad prefixes should retain all shards");
    assert.deepEqual(shardSuggest.suggestions[0].shards, ["band-1"]);

    const sharedSuggest = await sharded.suggest({ q: "shared" });
    assert.equal(sharedSuggest.suggestions[0].text, "Shared landmark");
    assert.deepEqual(sharedSuggest.suggestions[0].shards, ["band-1", "band-2", "band-3"]);

    // Prefix-aware routing keeps autocomplete on the shard whose indexed
    // suggest field can contain the requested prefix.
    const routedSuggest = await sharded.suggest({ q: "borealunique" });
    const monoRoutedSuggest = await mono.suggest({ q: "borealunique" });
    assert.equal(routedSuggest.stats.shardsQueried, 1);
    assert.equal(routedSuggest.stats.textRouting?.selected, 1);
    assert.ok(routedSuggest.suggestions.length > 0);
    assert.ok(routedSuggest.suggestions.every(item => item.text.startsWith("borealunique")));
    assert.deepEqual(routedSuggest.suggestions.map(({ shards, ...item }) => item), monoRoutedSuggest.suggestions);
    assert.deepEqual(routedSuggest.suggestions[0].shards, ["band-2"]);

    const unroutedSuggest = await sharded.suggest({ q: "borealuniqxe" });
    assert.equal(unroutedSuggest.stats.shardsQueried, 3);
    assert.equal(unroutedSuggest.stats.textRouting?.fallback, "no-shard-support");

    // Sorted browse merges by the real key across shards.
    const shardSorted = await sharded.search({ q: "study", sort: "-population", size: 30 });
    const populations = shardSorted.results.map(item => Number(item.population));
    for (let i = 1; i < populations.length; i++) {
      assert.ok(populations[i - 1] >= populations[i], "population sort not monotone");
    }
    assert.ok(new Set(shardSorted.results.map(item => item.shard)).size > 1, "sorted browse should span shards");

    // Text routing: a band-unique term opens exactly one shard, with the
    // same scores the monolithic index produces.
    const routedResponse = await sharded.search({ q: "glacier bandmark2", size: 20 });
    assert.equal(routedResponse.stats.shardsQueried, 1, "bandmark2 should route to a single shard");
    assert.equal(routedResponse.stats.textRouting?.selected, 1);
    assert.ok(routedResponse.results.length > 0);
    assert.ok(routedResponse.results.every(item => item.shard === "band-2"));
    const monoRouted = await mono.search({ q: "glacier bandmark2", size: 20 });
    assert.deepEqual(
      routedResponse.results.map(item => [item.id, item.score]),
      monoRouted.results.map(item => [item.id, item.score]),
      "routed results diverged from monolithic"
    );
    const routedCount = await sharded.count({ q: "glacier bandmark2" });
    const monoRoutedCount = await mono.count({ q: "glacier bandmark2" });
    assert.equal(routedCount.total, monoRoutedCount.total);
    assert.equal(routedCount.stats.shardsQueried, 1);

    // A term no shard contains cannot be routed: the query falls back to
    // the full fan-out so per-shard typo correction still applies.
    const fallbackResponse = await sharded.search({ q: "glacier bandmarq2", size: 5 });
    assert.equal(fallbackResponse.stats.textRouting?.fallback, "no-shard-support");
    assert.equal(fallbackResponse.stats.shardsQueried, 3);
    // Every shard still runs its own typo correction (band-N corrects to its
    // local bandmarkN); the merge surfaces one of them.
    assert.match(fallbackResponse.correctedQuery || "", /bandmark\d/u, "typo path should still correct");
    assert.ok(fallbackResponse.results.length > 0);

    // Broad vocabulary routes everywhere: no narrowing, no fallback.
    const broadResponse = await sharded.search({ q: "glacier study", size: 5 });
    assert.equal(broadResponse.stats.shardsQueried, 3);
    assert.equal(broadResponse.stats.textRouting?.selected, 3);
  } finally {
    await monoServer.close();
    await shardServer.close();
  }
});

test("hierarchical roots compose: a shard can itself be a sharded index", { timeout: 120000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-shard-nested-"));
  const docsByBand = BANDS.map(band => bandDocs(band, 80));
  const flatRoot = await buildSharded(root, docsByBand);
  const bboxOf = id => flatRoot.shards.find(shard => shard.id === id).bbox;
  const unionBbox = (a, b) => [
    Math.min(a[0], b[0]), Math.min(a[1], b[1]),
    Math.max(a[2], b[2]), Math.max(a[3], b[3])
  ];

  // Nested topology over the same leaf shards:
  //   rangefind-nested/ → [ north → (band-1, band-2), band-3 ]
  writeShardedRootManifest({
    outDir: join(root, "public/rangefind-nested/north"),
    shards: [1, 2].map(band => ({
      id: `band-${band}`,
      path: `../../rangefind/shards/band-${band}/`,
      bbox: bboxOf(`band-${band}`)
    }))
  });
  writeShardedRootManifest({
    outDir: join(root, "public/rangefind-nested"),
    shards: [
      { id: "north", path: "north/", bbox: unionBbox(bboxOf("band-1"), bboxOf("band-2")) },
      { id: "band-3", path: "../rangefind/shards/band-3/", bbox: bboxOf("band-3") }
    ]
  });

  const server = await serveStatic(join(root, "public"));
  try {
    const flat = await createSearch({ baseUrl: `${server.baseUrl}` });
    const nested = await createSearch({ baseUrl: server.baseUrl.replace("/rangefind/", "/rangefind-nested/") });
    assert.equal(nested.manifest.total, flat.manifest.total);

    // Same corpus, same frozen stats → identical scores and result sets,
    // one level deep or two (tie order differs — merge keys are per-level).
    const flatResponse = await flat.search({ q: "glacier study", size: 40 });
    const nestedResponse = await nested.search({ q: "glacier study", size: 40 });
    assert.deepEqual(
      nestedResponse.results.map(item => item.score),
      flatResponse.results.map(item => item.score)
    );
    const flatScores = new Map(flatResponse.results.map(item => [item.id, item.score]));
    for (const item of nestedResponse.results) {
      if (flatScores.has(item.id)) {
        assert.ok(Math.abs(item.score - flatScores.get(item.id)) < 1e-9, `score diverged for ${item.id}`);
      }
    }
    // Results carry the hierarchical shard path.
    assert.ok(nestedResponse.results.some(item => /^north\/band-[12]$/u.test(item.shard)));
    assert.ok(nestedResponse.results.some(item => item.shard === "band-3"));

    // Geo routing recurses: a radius inside band 2 touches only the north
    // subtree at the outer level and only band-2 inside it.
    const near = { lat: 20.402, lon: -69.594, radiusMeters: 20000 };
    const geoNested = await nested.search({ q: "", geo: { near, sort: "distance" }, size: 5 });
    assert.equal(geoNested.stats.shardsQueried, 1);
    assert.ok(geoNested.results.every(item => item.shard === "north/band-2"));
    const geoFlat = await flat.search({ q: "", geo: { near, sort: "distance" }, size: 5 });
    assert.deepEqual(geoNested.results.map(item => item.id), geoFlat.results.map(item => item.id));

    const counts = await Promise.all([nested.count({ q: "glacier" }), flat.count({ q: "glacier" })]);
    assert.equal(counts[0].total, counts[1].total);
  } finally {
    await server.close();
  }
});

test("generational delta on a stats-frozen shard matches a full rebuild", { timeout: 120000 }, async () => {
  const deltaRoot = await mkdtemp(join(tmpdir(), "rangefind-shard-delta-"));
  const fullRoot = await mkdtemp(join(tmpdir(), "rangefind-shard-full-"));
  const baseBand1 = bandDocs(1, 100);
  const band2 = bandDocs(2, 100);
  // The delta adds new docs and replaces two existing ones — all after the
  // stats artifact was collected, like a nightly region refresh.
  const deltaDocs = [
    ...Array.from({ length: 30 }, (_, i) => makeDoc(100 + i, 1)),
    { ...makeDoc(0, 1), title: "Entry 0 about glacier in band 1 revised" },
    { ...makeDoc(5, 1), title: "Entry 5 about glacier in band 1 revised" }
  ];

  async function buildShardAt(root, id, docs, buildOptions = {}) {
    await writeFile(join(root, `${id}.jsonl`), docs.map(doc => JSON.stringify(doc)).join("\n"));
    const configPath = join(root, `${id}.config.json`);
    await writeFile(configPath, JSON.stringify({
      ...CONFIG,
      input: `${id}.jsonl`,
      output: `public/rangefind/shards/band-1`,
      scoringStats: "scoring-stats/scoring-stats.json"
    }));
    await build({ configPath, ...buildOptions });
  }

  for (const root of [deltaRoot, fullRoot]) {
    // Stats artifact frozen at the BASE corpus — deltas arrive later.
    await writeFile(join(root, "band-1.jsonl"), baseBand1.map(doc => JSON.stringify(doc)).join("\n"));
    await writeFile(join(root, "band-2.jsonl"), band2.map(doc => JSON.stringify(doc)).join("\n"));
    await writeFile(join(root, "template.config.json"), JSON.stringify(CONFIG));
    const templateConfig = await readConfig(join(root, "template.config.json"));
    await collectScoringStats({
      config: templateConfig,
      inputs: [1, 2].map(band => ({ id: `band-${band}`, input: join(root, `band-${band}.jsonl`) })),
      outDir: join(root, "scoring-stats")
    });
    await writeFile(join(root, "band-2.config.json"), JSON.stringify({
      ...CONFIG,
      input: "band-2.jsonl",
      output: "public/rangefind/shards/band-2",
      scoringStats: "scoring-stats/scoring-stats.json"
    }));
    await build({ configPath: join(root, "band-2.config.json") });
  }

  // Path A: base build + generational delta. Path B: one full rebuild of the
  // final corpus. Both freeze to the same artifact.
  await buildShardAt(deltaRoot, "band-1", baseBand1);
  await buildShardAt(deltaRoot, "band-1-delta", deltaDocs, { update: true });
  const finalBand1 = [...baseBand1.filter(doc => !["doc-1-0", "doc-1-5"].includes(doc.id)),
    ...deltaDocs];
  await buildShardAt(fullRoot, "band-1", finalBand1);

  for (const root of [deltaRoot, fullRoot]) {
    const stats = loadScoringStats(join(root, "scoring-stats/scoring-stats.json"));
    writeShardedRootManifest({
      outDir: join(root, "public/rangefind"),
      shards: [1, 2].map(band => ({
        id: `band-${band}`,
        path: `shards/band-${band}/`,
        bbox: stats.inputs.find(item => item.id === `band-${band}`)?.bbox
      })),
      scoringStats: stats
    });
  }

  const deltaServer = await serveStatic(join(deltaRoot, "public"));
  const fullServer = await serveStatic(join(fullRoot, "public"));
  try {
    const viaDelta = await createSearch({ baseUrl: deltaServer.baseUrl });
    const viaFull = await createSearch({ baseUrl: fullServer.baseUrl });
    // Generational shard totals count alive docs, so both paths agree.
    assert.equal(viaDelta.manifest.total, viaFull.manifest.total);

    for (const q of ["glacier study", "harbor", "revised", "meadow number", "quarry study number"]) {
      const deltaResponse = await viaDelta.search({ q, size: 60 });
      const fullResponse = await viaFull.search({ q, size: 60 });
      const deltaScores = new Map(deltaResponse.results.map(item => [item.id, item.score]));
      const fullScores = new Map(fullResponse.results.map(item => [item.id, item.score]));
      for (const [id, score] of deltaScores) {
        if (!fullScores.has(id)) continue;
        assert.ok(Math.abs(score - fullScores.get(id)) < 1e-9,
          `score diverged for ${id} on "${q}": ${score} vs ${fullScores.get(id)}`);
      }
      assert.deepEqual([...deltaScores.keys()].sort(), [...fullScores.keys()].sort(),
        `result sets differ for "${q}"`);
    }

    // Replaced docs resolve to the delta version only.
    const replaced = await viaDelta.search({ q: "revised", size: 10 });
    assert.ok(replaced.results.some(item => item.id === "doc-1-0"));
    assert.equal(replaced.results.filter(item => item.id === "doc-1-0").length, 1);
    // Generational counts cannot subtract tombstoned matches (documented):
    // the delta path over-counts by at most the tombstones and flags itself.
    const counts = await Promise.all([viaDelta.count({ q: "glacier" }), viaFull.count({ q: "glacier" })]);
    assert.ok(counts[0].total >= counts[1].total && counts[0].total <= counts[1].total + 2);
  } finally {
    await deltaServer.close();
    await fullServer.close();
  }
});

test("geo queries route to intersecting shards only", { timeout: 120000 }, async () => {
  const shardRoot = await mkdtemp(join(tmpdir(), "rangefind-shard-geo-"));
  const monoRoot = await mkdtemp(join(tmpdir(), "rangefind-shard-geo-mono-"));
  const docsByBand = BANDS.map(band => bandDocs(band, 80));
  await buildMonolithic(monoRoot, docsByBand);
  await buildSharded(shardRoot, docsByBand);

  const shardServer = await serveStatic(join(shardRoot, "public"));
  const monoServer = await serveStatic(join(monoRoot, "public"));
  try {
    const sharded = await createSearch({ baseUrl: shardServer.baseUrl });
    const mono = await createSearch({ baseUrl: monoServer.baseUrl });

    // Radius query inside band 2 (lat 20-21): only that shard is queried.
    // The point sits off the doc grid's diagonal so no two docs are
    // equidistant — tie order is layout-dependent and not comparable.
    const near = { lat: 20.402, lon: -69.594, radiusMeters: 20000 };
    const radius = await sharded.search({ q: "", geo: { near, sort: "distance" }, size: 10 });
    assert.equal(radius.stats.shardsQueried, 1);
    const monoRadius = await mono.search({ q: "", geo: { near, sort: "distance" }, size: 10 });
    assert.deepEqual(
      radius.results.map(item => item.id),
      monoRadius.results.map(item => item.id),
      "radius nearest ordering diverged"
    );

    // Text+geo: same routing applies with a query.
    const textGeo = await sharded.search({ q: "glacier", geo: { near, sort: "distance" }, size: 10 });
    assert.equal(textGeo.stats.shardsQueried, 1);

    // Nearest without radius: expanding front finds the same global order
    // without necessarily querying every shard.
    const nearest = await sharded.search({ q: "", geo: { near: { lat: 20.402, lon: -69.594 }, sort: "distance" }, size: 10 });
    const monoNearest = await mono.search({ q: "", geo: { near: { lat: 20.402, lon: -69.594 }, sort: "distance" }, size: 10 });
    assert.deepEqual(
      nearest.results.map(item => item.id),
      monoNearest.results.map(item => item.id),
      "expanding nearest ordering diverged"
    );
    assert.ok(nearest.stats.shardsQueried <= 3);

    // Box query spanning bands 1-2 skips band 3.
    const box = await sharded.search({ q: "", geo: { box: { minLat: 9.5, maxLat: 21.5, minLon: -71, maxLon: -68 } }, size: 10 });
    assert.equal(box.stats.shardsQueried, 2);

    // Explicit shard scoping: query one region through the federated
    // engine, and it behaves like a direct single-shard engine.
    const scoped = await sharded.search({ q: "glacier", shards: ["band-2"], size: 50 });
    assert.equal(scoped.stats.shardsQueried, 1);
    assert.ok(scoped.results.length > 0);
    assert.ok(scoped.results.every(item => item.shard === "band-2"));
    const direct = await createSearch({ baseUrl: `${shardServer.baseUrl}shards/band-2/` });
    const directResponse = await direct.search({ q: "glacier", size: 50 });
    assert.deepEqual(
      scoped.results.map(item => [item.id, item.score]),
      directResponse.results.map(item => [item.id, item.score]),
      "scoped federation should match the standalone shard engine"
    );
    const scopedCount = await sharded.count({ q: "glacier", shards: ["band-2"] });
    assert.equal(scopedCount.total, (await direct.count({ q: "glacier" })).total);
    const scopedSuggest = await sharded.suggest({ q: "entr", shards: ["band-2"] });
    assert.equal(scopedSuggest.stats.shards, 3);
    await assert.rejects(
      () => sharded.search({ q: "glacier", shards: ["atlantis"] }),
      /unknown shard or group "atlantis"/
    );

    // Multi-level scoping: group labels expand to their member shards.
    const grouped = await sharded.search({ q: "glacier", shards: ["south"], size: 50 });
    assert.equal(grouped.stats.shardsQueried, 2);
    assert.ok(grouped.results.every(item => item.shard !== "band-3"));
    const world = await sharded.search({ q: "glacier", shards: ["world"], size: 10 });
    assert.equal(world.stats.shardsQueried, 3);
    // Mixing a group and an id dedupes members.
    const mixed = await sharded.search({ q: "glacier", shards: ["south", "band-1"], size: 10 });
    assert.equal(mixed.stats.shardsQueried, 2);

    // Text + box filter returns the same matches (complete sets: under the
    // page cap on both sides).
    const boxParams = { q: "glacier", geo: { box: { minLat: 9.5, maxLat: 21.5, minLon: -71, maxLon: -68 } }, size: 100 };
    const shardBoxText = await sharded.search(boxParams);
    const monoBoxText = await mono.search(boxParams);
    assert.deepEqual(
      shardBoxText.results.map(item => item.id).sort(),
      monoBoxText.results.map(item => item.id).sort()
    );
  } finally {
    await shardServer.close();
    await monoServer.close();
  }
});

test("text routing fails open for shards it does not know", { timeout: 120000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-shard-routefo-"));
  const docsByBand = BANDS.map(band => bandDocs(band, 60));
  const rootManifest = await buildSharded(root, docsByBand);

  // Rewrite the root as if band-3 were added after the routing build: its
  // id is unknown to the routing table, so every query must include it.
  const manifestPath = join(root, "public/rangefind/manifest.min.json");
  const patched = {
    ...rootManifest,
    text_routing: {
      ...rootManifest.text_routing,
      shard_ids: [rootManifest.text_routing.shard_ids[0], rootManifest.text_routing.shard_ids[1], "band-3-stale"]
    }
  };
  await writeFile(manifestPath, JSON.stringify(patched));
  await writeFile(join(root, "public/rangefind/manifest.json"), JSON.stringify(patched));

  const server = await serveStatic(join(root, "public"));
  try {
    const sharded = await createSearch({ baseUrl: server.baseUrl });
    // band-3's marker resolves through the fail-open path: no routed shard
    // has it, but the unrouted shard is always searched.
    const unrouted = await sharded.search({ q: "bandmark3", size: 5 });
    assert.equal(unrouted.stats.shardsQueried, 1);
    assert.ok(unrouted.results.length > 0);
    assert.ok(unrouted.results.every(item => item.shard === "band-3"));
    // A routed band keeps narrowing, but the unknown shard rides along.
    const routed = await sharded.search({ q: "bandmark1", size: 5 });
    assert.equal(routed.stats.shardsQueried, 2);
    assert.ok(routed.results.every(item => item.shard === "band-1"));
  } finally {
    await server.close();
  }
});

test("term-set sidecars rebuild identical text routing", { timeout: 120000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-shard-termset-"));
  const docsByBand = BANDS.map(band => bandDocs(band, 60));
  const rootManifest = await buildSharded(root, docsByBand);

  const { writeShardTermSet, writeTextRoutingIndex } = await import("../src/text_routing.js");
  const sidecarShards = [];
  for (const band of BANDS) {
    const outFile = join(root, `term-sets/band-${band}.terms.gz`);
    const written = writeShardTermSet({ dir: join(root, `public/rangefind/shards/band-${band}`), outFile });
    assert.ok(written.terms > 0);
    sidecarShards.push({ id: `band-${band}`, termSet: outFile });
  }
  const fromSidecars = await writeTextRoutingIndex({
    outDir: join(root, "routing-from-sidecars"),
    shards: sidecarShards
  });
  assert.equal(fromSidecars.term_count, rootManifest.text_routing.term_count);
  assert.deepEqual(fromSidecars.shard_ids, rootManifest.text_routing.shard_ids);
  assert.equal(fromSidecars.suggest_prefix, true);
  assert.equal(fromSidecars.suggest_prefix, rootManifest.text_routing.suggest_prefix);
  // Same inputs must produce the same content-addressed artifact.
  assert.equal(fromSidecars.directory.root_hash, rootManifest.text_routing.directory.root_hash);
});

test("text routing streams large term-set merges within a bounded heap", { timeout: 120000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-routing-memory-"));
  const shardCount = 310;
  const termCount = 2000;
  const suffix = "x".repeat(128);
  const terms = Array.from({ length: termCount }, (_, index) => `term-${String(index).padStart(6, "0")}-${suffix}`);
  const header = JSON.stringify({ format: "rftermset-v1", analysis: null, suggest_prefix: false });
  const compressed = gzipSync(Buffer.from(`${header}\n${terms.join("\n")}\n`, "utf8"), { level: 1 });
  await Promise.all(Array.from({ length: shardCount }, (_, index) => (
    writeFile(join(root, `shard-${index}.terms.gz`), compressed)
  )));

  const script = `
    const { writeTextRoutingIndex } = await import("./src/text_routing.js");
    const root = process.env.RANGEFIND_MEMORY_TEST_ROOT;
    const shardCount = Number(process.env.RANGEFIND_MEMORY_TEST_SHARDS);
    const expectedTerms = Number(process.env.RANGEFIND_MEMORY_TEST_TERMS);
    const shards = Array.from({ length: shardCount }, (_, index) => ({
      id: \`shard-\${index}\`,
      termSet: \`\${root}/shard-\${index}.terms.gz\`
    }));
    const result = await writeTextRoutingIndex({ outDir: \`\${root}/out\`, shards, segmentTerms: 64 });
    if (result.term_count !== expectedTerms) {
      throw new Error(\`Expected \${expectedTerms} merged terms, received \${result.term_count}.\`);
    }
  `;
  const child = spawn(process.execPath, ["--max-old-space-size=64", "--input-type=module", "--eval", script], {
    cwd: resolve("."),
    env: {
      ...process.env,
      RANGEFIND_MEMORY_TEST_ROOT: root,
      RANGEFIND_MEMORY_TEST_SHARDS: String(shardCount),
      RANGEFIND_MEMORY_TEST_TERMS: String(termCount)
    },
    stdio: ["ignore", "ignore", "pipe"]
  });
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", chunk => {
    if (stderr.length < 12000) stderr += chunk;
  });
  const { code, signal } = await new Promise((resolveChild, rejectChild) => {
    child.once("error", rejectChild);
    child.once("close", (code, signal) => resolveChild({ code, signal }));
  });
  assert.equal(code, 0, `low-heap routing merge failed (${signal || "exit"}):\n${stderr}`);
});
