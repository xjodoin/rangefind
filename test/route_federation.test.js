import assert from "node:assert/strict";
import test from "node:test";
import { openRouteCatalogUrl } from "../src/route_federation.js";
import { encodeRoutePortalIds, encodeRoutePortalRecords } from "../src/route_portals.js";

const catalogUrl = "https://routes.test/routes/catalog.json";
const point = (lat, lon) => ({ lat, lon });

function jsonResponse(value) {
  return { ok: true, status: 200, async json() { return value; } };
}

function rangeResponse(bytes) {
  return {
    ok: true,
    status: 206,
    async arrayBuffer() { return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength); }
  };
}

function portalPack(values) {
  const ids = encodeRoutePortalIds(values);
  const records = encodeRoutePortalRecords(values);
  const pack = new Uint8Array(ids.length + records.length);
  pack.set(ids);
  pack.set(records, ids.length);
  return {
    pack,
    entry: {
      count: values.length / 3,
      ids: { offset: 0, length: ids.length },
      records: { offset: ids.length, length: records.length }
    }
  };
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

test("dataset-root route bases resolve beside routes/catalog.json without duplicating routes", async () => {
  const source = fixture();
  const catalog = await source.fetch(catalogUrl).then(response => response.json());
  for (const index of catalog.indexes) index.base = `routes/${index.base}`;
  const opened = [];
  const engine = await openRouteCatalogUrl(catalogUrl, {
    ...source,
    fetch: async (url, options) => {
      if (url === catalogUrl) return jsonResponse(catalog);
      assert.ok(!String(url).includes("/routes/routes/"));
      return source.fetch(url, options);
    },
    openGraph(index, base) {
      opened.push(base);
      return source.openGraph(index);
    }
  });
  await engine.route({ from: point(5, 1), to: point(5, 19), geometry: false });
  assert.deepEqual(opened.sort(), [
    "https://routes.test/routes/car/east/",
    "https://routes.test/routes/car/west/"
  ]);
});

test("v2 federation reads only one record range and the peer id range", async () => {
  const west = portalPack([101, 5e7, 10e7, 102, 6e7, 10e7]);
  const east = portalPack([101, 5e7, 10e7, 102, 6e7, 10e7, 999, 7e7, 10e7]);
  const catalog = {
    format: "rangefind-route-catalog-v1",
    indexes: [
      {
        region: "west", profile: "car", base: "routes/car/west/", bbox: [0, 0, 10, 10], neighbors: ["east"],
        portals: { format: "rfrouteportals-v2", file: "portals.west.bin", neighbors: { east: west.entry } }
      },
      {
        region: "east", profile: "car", base: "routes/car/east/", bbox: [0, 10, 10, 20], neighbors: ["west"],
        portals: { format: "rfrouteportals-v2", file: "portals.east.bin", neighbors: { west: east.entry } }
      }
    ]
  };
  const packs = { "portals.west.bin": west.pack, "portals.east.bin": east.pack };
  const ranges = [];
  const source = fixture();
  const engine = await openRouteCatalogUrl(catalogUrl, {
    inflate: async bytes => bytes,
    fetch: async (url, options = {}) => {
      if (url === catalogUrl) return jsonResponse(catalog);
      const name = new URL(url).pathname.split("/").at(-1);
      const match = options.headers?.Range?.match(/^bytes=(\d+)-(\d+)$/u);
      assert.ok(match, "portal packs use Range requests");
      const start = Number(match[1]);
      const end = Number(match[2]);
      ranges.push({ name, start, end });
      return rangeResponse(packs[name].subarray(start, end + 1));
    },
    openGraph: source.openGraph
  });
  const result = await engine.route({ from: point(5, 1), to: point(5, 19), geometry: false });
  assert.equal(result.federated, true);
  assert.equal(ranges.length, 2);
  assert.deepEqual(ranges.map(range => range.name).sort(), ["portals.east.bin", "portals.west.bin"]);
  assert.equal(ranges.find(range => range.name === "portals.west.bin").start, west.entry.records.offset);
  assert.equal(ranges.find(range => range.name === "portals.east.bin").start, east.entry.ids.offset);
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

test("an adjacent endpoint region is proved before unrelated neighbor expansion", async () => {
  const catalog = {
    format: "rangefind-route-catalog-v1",
    indexes: [
      { region: "west", profile: "car", base: "west/", bbox: [0, 0, 10, 7], portals: "p.json", neighbors: ["middle", "east"] },
      { region: "middle", profile: "car", base: "middle/", bbox: [0, 6, 10, 14], portals: "p.json", neighbors: ["west", "east"] },
      { region: "east", profile: "car", base: "east/", bbox: [0, 13, 10, 20], portals: "p.json", neighbors: ["middle", "west"] }
    ]
  };
  const sidecars = {
    west: { format: "rfrouteportals-v1", neighbors: { middle: [2, 5e7, 65e6], east: [1, 5e7, 7e7] } },
    middle: { format: "rfrouteportals-v1", neighbors: { west: [2, 5e7, 65e6], east: [3, 5e7, 135e6] } },
    east: { format: "rfrouteportals-v1", neighbors: { middle: [3, 5e7, 135e6], west: [1, 5e7, 7e7] } }
  };
  const sidecarReads = [];
  const source = fixture();
  const engine = await openRouteCatalogUrl(catalogUrl, {
    fetch: async url => {
      if (url === catalogUrl) return jsonResponse(catalog);
      const region = new URL(url).pathname.split("/").at(-2);
      sidecarReads.push(region);
      return jsonResponse(sidecars[region]);
    },
    openGraph: source.openGraph
  });
  const result = await engine.route({ from: point(5, 1), to: point(5, 19), geometry: false });
  assert.deepEqual(result.regions, ["west", "east"]);
  assert.deepEqual(new Set(sidecarReads), new Set(["west", "east"]));
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
