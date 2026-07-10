import assert from "node:assert/strict";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  OSM_DISPLAY_FIELDS,
  OSM_INTEGRATION_SCHEMA_VERSION,
  createOsmIndexConfig
} from "../src/integrations/osm/schema.js";
import { buildOsmIndex, writeOsmSite } from "../src/integrations/osm/node/builder.js";

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
    await writeFile(join(root, "data", "osm-places.jsonl"), `${JSON.stringify({
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
    })}\n`);
    const built = await buildOsmIndex({
      root,
      region: "luxembourg",
      rqa: false,
      workerCount: 1,
      buildProgressLogMs: 0,
      runtimeBundlePath: join(root, "missing-runtime.js"),
      osmBundlePath: join(root, "missing-osm.js")
    });
    const manifest = JSON.parse(await readFile(join(root, "public", "rangefind", "manifest.json"), "utf8"));
    assert.equal(manifest.total, 1);
    assert.equal(manifest.features.geo, true);
    assert.equal(built.config.output, "public/rangefind");
    assert.equal(built.seconds >= 0, true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
