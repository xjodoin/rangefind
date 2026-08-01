import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gunzipSync } from "node:zlib";
import {
  OSM_DISPLAY_FIELDS,
  OSM_INTEGRATION_SCHEMA_VERSION,
  createOsmIndexConfig
} from "../src/integrations/osm/schema.js";
import { buildOsmIndex, buildOsmShardedIndex, writeOsmSite } from "../src/integrations/osm/node/builder.js";
import { createNodeSearch } from "../src/runtime.node.js";
import { searchOsmQuery } from "../src/integrations/osm/query.js";

test("OSM integration publishes the canonical Rangefind schema", () => {
  const config = createOsmIndexConfig({
    region: "quebec",
    rqa: true,
    workerCount: 6,
    buildProgressLogMs: 0
  });
  assert.equal(OSM_INTEGRATION_SCHEMA_VERSION, 1);
  assert.equal(config.input, "data/osm-rqa-places.jsonl");
  assert.equal(config.output, "public/rangefind");
  assert.equal(config.scanWorkers, 6);
  assert.equal(config.builderWorkerCount, 6);
  assert.deepEqual(config.geo, [{ name: "location", latPath: "geo_lat", lonPath: "geo_lon" }]);
  assert.deepEqual(config.display, [...OSM_DISPLAY_FIELDS]);
  assert.deepEqual(config.authority.map(field => field.name), ["address", "address_interpolation", "postcode"]);
  assert.ok(config.filterBitmapFacetValues.type.includes("cinema"));
  assert.equal(config.buildProgressLogMs, 0);
});

test("OSM integration applies national-scale builder tuning without another index format", () => {
  const config = createOsmIndexConfig({ region: "us", workerCount: 2 });
  assert.equal(config.docLayoutStrategy, "doc-id");
  assert.equal(config.postingGzipLevel, 3);
  assert.equal(config.segmentMergeFanIn, 512);
  assert.equal(config.codeStorePreloadMaxBytes, 2304 * 1024 * 1024);
  assert.equal(config.buildTelemetryPath, "osm-us-build-telemetry.json");
  assert.equal(config.sidecar, undefined);
});

test("Node OSM integration writes config, demo metadata, and both browser bundles", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-osm-integration-"));
  try {
    const runtimeBundlePath = join(root, "runtime.input.js");
    const osmBundlePath = join(root, "osm.input.js");
    await writeFile(runtimeBundlePath, "runtime");
    await writeFile(osmBundlePath, "osm");
    const site = writeOsmSite({
      root,
      region: "quebec",
      rqa: true,
      workerCount: 3,
      runtimeBundlePath,
      osmBundlePath
    });
    const config = JSON.parse(await readFile(site.configPath, "utf8"));
    const demo = JSON.parse(await readFile(join(root, "public", "osm-demo.json"), "utf8"));
    assert.equal(config.input, "data/osm-rqa-places.jsonl");
    assert.equal(config.scanWorkers, 3);
    assert.deepEqual(demo, { region: "quebec", rqa: true, center: [-73.6, 45.55], zoom: 12 });
    assert.equal(await readFile(join(root, "public", "runtime.browser.js"), "utf8"), "runtime");
    assert.equal(await readFile(join(root, "public", "osm.browser.js"), "utf8"), "osm");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("Node OSM integration builds a normal searchable Rangefind index", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-osm-build-"));
  try {
    await mkdir(join(root, "data"), { recursive: true });
    await writeFile(join(root, "data", "osm-places.jsonl"), [{
      id: "node/1",
      name: "Testville",
      search_name: "Testville",
      body: "city place",
      category: "place",
      type: "city",
      lat: 45.5,
      lon: -73.6,
      geo_lat: 45.5,
      geo_lon: -73.6
    }, {
      id: "node/2",
      name: "Testville Cinema",
      search_name: "Testville Cinema",
      body: "cinema amenity",
      category: "amenity",
      type: "cinema",
      lat: 45.51,
      lon: -73.61,
      geo_lat: 45.51,
      geo_lon: -73.61
    }].map(doc => JSON.stringify(doc)).join("\n"));
    const built = await buildOsmIndex({
      root,
      region: "luxembourg",
      rqa: false,
      workerCount: 1,
      buildProgressLogMs: 0,
      config: { filterBitmapMaxFacetValues: 1 },
      runtimeBundlePath: join(root, "missing-runtime.js"),
      osmBundlePath: join(root, "missing-osm.js")
    });
    const manifest = JSON.parse(await readFile(join(root, "public", "rangefind", "manifest.json"), "utf8"));
    const filterBitmaps = JSON.parse(gunzipSync(
      await readFile(join(root, "public", "rangefind", "filter-bitmaps", "manifest.json.gz"))
    ));
    assert.equal(manifest.total, 2);
    assert.equal(manifest.features.geo, true);
    assert.equal(manifest.features.geoCapsules, true);
    assert.equal(manifest.features.geoCategoryCells, true);
    assert.equal(manifest.geo.fields.location.category_cells[0].facet, "type");
    // High-cardinality facets skip blanket bitmap generation, but the OSM
    // category vocabulary still materializes its explicitly selected types.
    assert.equal(Object.keys(filterBitmaps.fields.type.values).length, 1);
    const engine = await createNodeSearch({ source: join(root, "public", "rangefind") });
    const nearbyCinema = await engine.search({
      size: 1,
      filters: { facets: { type: ["cinema"] } },
      geo: {
        near: { lat: 45.5, lon: -73.6, radiusMeters: 5000 },
        sort: "distance"
      },
      trace: true
    });
    assert.equal(nearbyCinema.results[0]?.name, "Testville Cinema");
    assert.equal(nearbyCinema.results[0]?.id, "node/2");
    assert.equal(nearbyCinema.stats.docPayloadLane, "geoCapsules");
    assert.equal(nearbyCinema.stats.geoCapsuleHits, 1);
    assert.doesNotMatch(nearbyCinema.stats.geoLane, /CategoryCells$/u);
    assert.equal(nearbyCinema.stats.geoCategoryCellBlocksFetched, 0);
    assert.ok(nearbyCinema.stats.trace.spans.some(span => span.name === "filterBitmaps.fetch"));
    assert.ok(!nearbyCinema.stats.trace.spans.some(span => span.name === "docValues.fetch"));
    assert.equal(
      nearbyCinema.stats.trace.spans.find(span => span.name === "manifest.fetch")?.count,
      2
    );
    assert.equal(built.config.output, "public/rangefind");
    assert.equal(built.seconds >= 0, true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("Sharded OSM build embeds the category lexicon and keeps categories local", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-osm-sharded-"));
  try {
    await mkdir(join(root, "data"), { recursive: true });
    const doc = (id, fields) => `${JSON.stringify({ id, ...fields })}\n`;
    // Quebec shard: a cinema POI near Montreal plus its host city.
    await writeFile(join(root, "data", "quebec.jsonl"),
      doc("node/1", {
        name: "Cinéma Beaubien", search_name: "Cinéma Beaubien", body: "cinema amenity",
        category: "amenity", type: "cinema",
        lat: 45.535, lon: -73.58, geo_lat: 45.535, geo_lon: -73.58
      })
      + doc("node/2", {
        name: "Montréal", search_name: "Montréal", body: "city place",
        category: "place", type: "city", population: 1704694,
        lat: 45.5032, lon: -73.5698, geo_lat: 45.5032, geo_lon: -73.5698
      }));
    // Uganda shard: a village literally named Cinema — the old planner's trap.
    await writeFile(join(root, "data", "uganda.jsonl"),
      doc("node/3", {
        name: "Cinema", search_name: "Cinema", body: "village place",
        category: "place", type: "village",
        lat: -0.3408, lon: 31.739, geo_lat: -0.3408, geo_lon: 31.739
      }));
    const output = join(root, "public", "rangefind");
    const built = await buildOsmShardedIndex({
      output,
      workerCount: 1,
      buildProgressLogMs: 0,
      shards: [
        { id: "quebec", input: join(root, "data", "quebec.jsonl") },
        { id: "uganda", input: join(root, "data", "uganda.jsonl") }
      ]
    });
    const lexicon = built.rootManifest.category_lexicon;
    assert.equal(built.rootManifest.features.geoCapsules, true);
    assert.ok(built.rootManifest.shards.every(shard => shard.features?.geoCapsules === true));
    assert.equal(built.rootManifest.features.geoCategoryCells, false);
    assert.equal(
      built.rootManifest.shards.find(shard => shard.id === "quebec")?.features?.geoCategoryCells,
      true
    );
    assert.equal(
      built.rootManifest.shards.find(shard => shard.id === "uganda")?.features?.geoCategoryCells,
      undefined
    );
    assert.equal(lexicon.facet, "type");
    assert.ok(lexicon.types.includes("cinema"));
    // Place values never gate as categories ("Quebec City" stays a city).
    assert.ok(!lexicon.types.includes("village"));
    assert.ok(!lexicon.types.includes("city"));
    assert.equal(lexicon.aliases.cinema, "cinema");
    assert.equal(lexicon.aliases["movie theater"], "cinema");

    const engine = await createNodeSearch({ source: output });
    // Anchored near Montreal, "cinema" is a nearest-first category search —
    // not a teleport to the same-named village.
    const near = await searchOsmQuery(engine, {
      q: "cinema", size: 5, near: { lat: 45.5, lon: -73.57 }
    });
    assert.equal(near.stats.plannerLane, "osmCategoryNearby");
    assert.equal(near.results[0]?.name, "Cinéma Beaubien");
    assert.equal(near.stats.osmIntentCategoryFacet, true);
    assert.equal(near.stats.shardsQueried, 1);
    assert.deepEqual(near.stats.osmIntentCoverageShards, ["quebec"]);
    // The French alias resolves through the embedded lexicon too.
    const french = await searchOsmQuery(engine, {
      q: "cinéma près de moi", size: 5, near: { lat: 45.5, lon: -73.57 }
    });
    assert.equal(french.stats.plannerLane, "osmCategoryNearby");
    assert.equal(french.resolvedQuery, "Cinema nearby");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
