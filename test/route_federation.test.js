import assert from "node:assert/strict";
import test from "node:test";
import { openRouteCatalogUrl } from "../src/route_federation.js";

const catalogUrl = "https://routes.test/routes/catalog.json";
const point = (lat, lon) => ({ lat, lon });

function jsonResponse(value) {
  return { ok: true, status: 200, async json() { return value; } };
}

function fixture({ disconnect = false } = {}) {
  const catalog = {
    format: "rangefind-route-catalog-v1",
    coverage: "federated-regions",
    indexes: [
      { region: "west", profile: "car", base: "car/west/", bbox: [0, 0, 10, 10], portals: "portals.json", neighbors: ["east"] },
      { region: "east", profile: "car", base: "car/east/", bbox: [0, 10, 10, 20], portals: "portals.json", neighbors: ["west"] }
    ]
  };
  const westPortals = { format: "rfrouteportals-v1", profile: "car", neighbors: { east: [101, 5e7, 10e7, 102, 6e7, 10e7] } };
  const eastPortals = { format: "rfrouteportals-v1", profile: "car", neighbors: { west: disconnect ? [999, 5e7, 10e7] : [101, 5e7, 10e7, 102, 6e7, 10e7] } };
  const opened = [];
  const routeEngine = region => ({
    async route({ from, to, geometry = true }) {
      const portalLat = region === "west" ? to.lat : from.lat;
      const seconds = portalLat === 6 ? 20 : 100;
      return {
        seconds,
        bucket: "base",
        settledNodes: 4,
        from: { snapped: from, snapDistanceMeters: 0 },
        to: { snapped: to, snapDistanceMeters: 0 },
        ...(geometry ? {
          distanceMeters: seconds * 10,
          geometry: [[from.lat, from.lon], [to.lat, to.lon]],
          steps: [{ name: region, meters: seconds * 10, seconds, at: 0 }],
          edges: [{ leaf: 0, edge: 0, segment: "0/0/0", seconds, meters: seconds * 10 }],
          junctions: []
        } : {}),
        stats: { objectFetches: 1, bytesFetched: 10, cellFetches: 1, overlayFetches: 0, unpackCellFetches: 0, httpRequests: 1, shardsTouched: ["all"] }
      };
    }
  });
  return {
    opened,
    fetch: async url => {
      if (url === catalogUrl) return jsonResponse(catalog);
      if (url.endsWith("car/west/portals.json")) return jsonResponse(westPortals);
      if (url.endsWith("car/east/portals.json")) return jsonResponse(eastPortals);
      throw new Error(`unexpected fetch ${url}`);
    },
    openGraph(index) {
      opened.push(index.region);
      return routeEngine(index.region);
    }
  };
}

test("federated routing intersects OSM ids and chooses portals by actual regional costs", async () => {
  const source = fixture();
  const engine = await openRouteCatalogUrl(catalogUrl, source);
  const result = await engine.route({ from: point(5, 1), to: point(5, 19) });
  assert.equal(result.federated, true);
  assert.deepEqual(result.regions, ["west", "east"]);
  assert.equal(result.transitions[0].osmNodeId, 102);
  assert.equal(result.seconds, 40);
  assert.deepEqual(result.geometry, [[5, 1], [6, 10], [5, 19]]);
  assert.deepEqual(result.stats.regionsTouched, ["west", "east"]);
  assert.ok(result.edges.every(edge => edge.segment.startsWith(`${edge.region}:`)), "edge identities are region-qualified");
  assert.deepEqual(new Set(source.opened), new Set(["west", "east"]));
});

test("same-region routing opens only its regional graph", async () => {
  const source = fixture();
  const engine = await openRouteCatalogUrl(catalogUrl, source);
  const result = await engine.route({ from: point(5, 1), to: point(6, 2) });
  assert.equal(result.federated, undefined);
  assert.deepEqual(source.opened, ["west"]);
});

test("nearby regional roads are never stitched without a shared OSM id", async () => {
  const source = fixture({ disconnect: true });
  const engine = await openRouteCatalogUrl(catalogUrl, source);
  await assert.rejects(
    engine.route({ from: point(5, 1), to: point(5, 19) }),
    error => error.code === "RANGEFIND_ROUTE_REGIONS_DISCONNECTED"
  );
  assert.deepEqual(source.opened, [], "regional packs are not opened when portal proof fails");
});

test("portal-aware catalog search bypasses a false direct bbox edge", async () => {
  const catalog = {
    format: "rangefind-route-catalog-v1",
    indexes: [
      { region: "west", profile: "car", base: "west/", bbox: [0, 0, 10, 7], portals: "p.json", neighbors: ["east", "middle"] },
      { region: "middle", profile: "car", base: "middle/", bbox: [0, 6, 10, 14], portals: "p.json", neighbors: ["west", "east"] },
      { region: "east", profile: "car", base: "east/", bbox: [0, 13, 10, 20], portals: "p.json", neighbors: ["west", "middle"] }
    ]
  };
  const sidecars = {
    west: { format: "rfrouteportals-v1", neighbors: { east: [1, 5e7, 7e7], middle: [2, 5e7, 65e6] } },
    middle: { format: "rfrouteportals-v1", neighbors: { west: [2, 5e7, 65e6], east: [3, 5e7, 135e6] } },
    east: { format: "rfrouteportals-v1", neighbors: { west: [999, 5e7, 13e7], middle: [3, 5e7, 135e6] } }
  };
  const engine = await openRouteCatalogUrl(catalogUrl, {
    fetch: async url => {
      if (url === catalogUrl) return jsonResponse(catalog);
      const region = new URL(url).pathname.split("/").at(-2);
      return jsonResponse(sidecars[region]);
    },
    openGraph: index => ({
      async route({ from, to, geometry = true }) {
        return {
          seconds: 10,
          bucket: "base",
          settledNodes: 1,
          from: { snapped: from, snapDistanceMeters: 0 },
          to: { snapped: to, snapDistanceMeters: 0 },
          ...(geometry ? { distanceMeters: 100, geometry: [[from.lat, from.lon], [to.lat, to.lon]], steps: [], edges: [], junctions: [] } : {}),
          stats: { shardsTouched: [index.region] }
        };
      }
    })
  });
  const result = await engine.route({ from: point(5, 1), to: point(5, 19) });
  assert.deepEqual(result.regions, ["west", "middle", "east"]);
  assert.deepEqual(result.transitions.map(value => value.osmNodeId), [2, 3]);
});
