import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import {
  boxContainsPointE7,
  boxesForRadiusE7,
  buildGeoTreeLeaves,
  decodeGeoLeafPage,
  encodeGeoLeafPage,
  encodeGeoTreeRoot,
  haversineMeters,
  haversineMetersE7,
  latToE7,
  lonToE7,
  parseGeoTreeRoot,
  pointToBoxDistanceMetersE7,
  pointToBoxMaxDistanceMetersE7
} from "../src/geo_tree.js";
import {
  decodeGeoCellBlock,
  encodeGeoCellBlock,
  geoCellBlock,
  geoCellBlockKey,
  geoCellForE7,
  geoCellsForBoxes
} from "../src/geo_cells.js";

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

test("geo E7 encoding validates bounds and missing values", () => {
  assert.equal(latToE7(45.5017), 455017000);
  assert.equal(lonToE7(-73.5673), -735673000);
  assert.equal(latToE7(90), 900000000);
  assert.equal(latToE7(-90), -900000000);
  assert.equal(latToE7(90.0001), null);
  assert.equal(lonToE7(180), -1800000000);
  assert.equal(lonToE7(-180), -1800000000);
  assert.equal(lonToE7(180.0001), null);
  assert.equal(latToE7(null), null);
  assert.equal(latToE7(""), null);
  assert.equal(latToE7(undefined), null);
  assert.equal(latToE7("not a number"), null);
  assert.equal(latToE7("45.5"), 455000000);
});

test("haversine distance matches known city pairs", () => {
  // Paris <-> London is roughly 344 km.
  const parisLondon = haversineMeters(48.8566, 2.3522, 51.5074, -0.1278);
  assert.ok(Math.abs(parisLondon - 344000) < 4000, `got ${parisLondon}`);
  // Montreal <-> Quebec City is roughly 233 km.
  const montrealQuebec = haversineMeters(45.5017, -73.5673, 46.8139, -71.208);
  assert.ok(Math.abs(montrealQuebec - 233000) < 4000, `got ${montrealQuebec}`);
  assert.equal(haversineMeters(45, 45, 45, 45), 0);
  const e7 = haversineMetersE7(455017000, -735673000, 468139000, -712080000);
  assert.ok(Math.abs(e7 - montrealQuebec) < 1);
});

test("boxes for radius handle the antimeridian and the poles", () => {
  const plain = boxesForRadiusE7(latToE7(45), lonToE7(-73), 10000);
  assert.equal(plain.length, 1);
  assert.ok(plain[0].minLatE7 < latToE7(45) && plain[0].maxLatE7 > latToE7(45));

  const dateline = boxesForRadiusE7(latToE7(0), lonToE7(179.999), 50000);
  assert.equal(dateline.length, 2);
  assert.ok(dateline.some(box => box.maxLonE7 === 1800000000));
  assert.ok(dateline.some(box => box.minLonE7 === -1800000000));

  const polar = boxesForRadiusE7(latToE7(89.9), lonToE7(0), 100000);
  assert.equal(polar.length, 1);
  assert.equal(polar[0].minLonE7, -1800000000);
  assert.equal(polar[0].maxLonE7, 1800000000);

  // Radius boxes must contain every point within the radius.
  const random = mulberry32(7);
  for (let i = 0; i < 500; i++) {
    const lat = random() * 170 - 85;
    const lon = random() * 360 - 180;
    const radius = random() * 500000 + 100;
    const boxes = boxesForRadiusE7(latToE7(lat), lonToE7(lon), radius);
    const bearing = random() * 2 * Math.PI;
    const angular = (radius * 0.999) / 6371008.7714;
    const lat1 = (lat * Math.PI) / 180;
    const lon1 = (lon * Math.PI) / 180;
    const lat2 = Math.asin(Math.sin(lat1) * Math.cos(angular) + Math.cos(lat1) * Math.sin(angular) * Math.cos(bearing));
    const lon2 = lon1 + Math.atan2(
      Math.sin(bearing) * Math.sin(angular) * Math.cos(lat1),
      Math.cos(angular) - Math.sin(lat1) * Math.sin(lat2)
    );
    const targetLat = latToE7((lat2 * 180) / Math.PI);
    let lonDeg = ((lon2 * 180) / Math.PI + 540) % 360 - 180;
    const targetLon = lonToE7(lonDeg);
    if (targetLat == null || targetLon == null) continue;
    assert.ok(
      boxes.some(box => boxContainsPointE7(box, targetLat, targetLon)),
      `point at bearing ${bearing} radius ${radius} escaped its boxes (${lat}, ${lon})`
    );
  }
});

test("point-to-box distance is a true lower bound", () => {
  const random = mulberry32(11);
  for (let i = 0; i < 300; i++) {
    const box = (() => {
      const minLat = random() * 160 - 80;
      const minLon = random() * 340 - 170;
      return {
        minLatE7: latToE7(minLat),
        maxLatE7: latToE7(minLat + random() * 8),
        minLonE7: lonToE7(minLon),
        maxLonE7: lonToE7(minLon + random() * 8)
      };
    })();
    const lat = random() * 170 - 85;
    const lon = random() * 360 - 180;
    const latE7 = latToE7(lat);
    const lonE7 = lonToE7(lon);
    const lower = pointToBoxDistanceMetersE7(latE7, lonE7, box);
    const upper = pointToBoxMaxDistanceMetersE7(latE7, lonE7, box);
    assert.ok(lower <= upper + 1e-6);
    if (boxContainsPointE7(box, latE7, lonE7)) {
      assert.equal(lower, 0);
      continue;
    }
    // Sample the box interior and edges; nothing may be closer than `lower`.
    for (let sample = 0; sample < 40; sample++) {
      const sampleLatFrac = sample % 2 === 0 ? random() : Math.round(random());
      const sampleLonFrac = sample % 2 === 0 ? Math.round(random()) : random();
      const sampleLat = box.minLatE7 + sampleLatFrac * (box.maxLatE7 - box.minLatE7);
      const sampleLon = box.minLonE7 + sampleLonFrac * (box.maxLonE7 - box.minLonE7);
      const distance = haversineMetersE7(latE7, lonE7, sampleLat, sampleLon);
      assert.ok(
        distance >= lower - 1,
        `sampled ${distance} below bound ${lower} for point (${lat}, ${lon})`
      );
      assert.ok(distance <= upper + 1, `sampled ${distance} above max bound ${upper}`);
    }
  }
});

test("geo leaf pages and tree roots round trip", () => {
  const random = mulberry32(3);
  const count = 1000;
  const lats = new Int32Array(count);
  const lons = new Int32Array(count);
  const docs = new Uint32Array(count);
  for (let i = 0; i < count; i++) {
    lats[i] = latToE7(random() * 170 - 85);
    lons[i] = lonToE7(random() * 360 - 180);
    docs[i] = Math.floor(random() * 5000000);
  }
  const leaves = buildGeoTreeLeaves(lats, lons, docs, 64);
  assert.ok(leaves.length >= Math.floor(count / 256));
  let covered = 0;
  for (const leaf of leaves) {
    covered += leaf.count;
    for (let i = leaf.start; i < leaf.end; i++) {
      assert.ok(lats[i] >= leaf.minLatE7 && lats[i] <= leaf.maxLatE7);
      assert.ok(lons[i] >= leaf.minLonE7 && lons[i] <= leaf.maxLonE7);
    }
    const encoded = encodeGeoLeafPage("location", lats, lons, docs, leaf);
    const decoded = decodeGeoLeafPage(encoded, { name: "location" });
    assert.equal(decoded.count, leaf.count);
    assert.deepEqual([...decoded.latsE7], [...lats.subarray(leaf.start, leaf.end)]);
    assert.deepEqual([...decoded.lonsE7], [...lons.subarray(leaf.start, leaf.end)]);
    assert.deepEqual([...decoded.docs], [...docs.subarray(leaf.start, leaf.end)]);
  }
  assert.equal(covered, count);

  const packTable = ["0000.abc.bin"];
  const packIndexes = new Map([["0000.abc.bin", 0]]);
  const withEntries = leaves.map(leaf => ({
    ...leaf,
    entry: {
      pack: "0000.abc.bin",
      offset: leaf.start * 10,
      length: 100,
      physicalLength: 100,
      logicalLength: 200,
      checksum: { algorithm: "sha256", value: "ff".repeat(32) }
    }
  }));
  const bbox = {
    minLatE7: Math.min(...leaves.map(leaf => leaf.minLatE7)),
    maxLatE7: Math.max(...leaves.map(leaf => leaf.maxLatE7)),
    minLonE7: Math.min(...leaves.map(leaf => leaf.minLonE7)),
    maxLonE7: Math.max(...leaves.map(leaf => leaf.maxLonE7))
  };
  const root = encodeGeoTreeRoot({
    field: "location",
    total: count,
    leafSize: 64,
    bbox,
    leaves: withEntries,
    packTable,
    packIndexes
  });
  const parsed = parseGeoTreeRoot(root.buffer);
  assert.equal(parsed.field, "location");
  assert.equal(parsed.total, count);
  assert.equal(parsed.leaves.length, leaves.length);
  assert.deepEqual(parsed.bbox, bbox);
  for (let i = 0; i < leaves.length; i++) {
    assert.equal(parsed.leaves[i].minLatE7, withEntries[i].minLatE7);
    assert.equal(parsed.leaves[i].maxLonE7, withEntries[i].maxLonE7);
    assert.equal(parsed.leaves[i].count, withEntries[i].count);
    assert.equal(parsed.leaves[i].offset, withEntries[i].entry.offset);
    assert.equal(parsed.leaves[i].checksum.value, "ff".repeat(32));
  }
});

test("geo leaf capsules round trip compact display rows", () => {
  const lats = new Int32Array([latToE7(45.5), latToE7(45.6)]);
  const lons = new Int32Array([lonToE7(-73.5), lonToE7(-73.6)]);
  const docs = new Uint32Array([7, 19]);
  const leaf = buildGeoTreeLeaves(lats, lons, docs, 16)[0];
  const capsules = [
    { title: "First place", category: "cafe", rating: 4.5 },
    { title: "Second place", category: "museum", rating: 4.8 }
  ];
  const encoded = encodeGeoLeafPage("location", lats, lons, docs, leaf, {
    capsules,
    capsuleFields: ["title", "category", "rating"]
  });
  const decoded = decodeGeoLeafPage(encoded, { name: "location" });
  assert.deepEqual(decoded.capsuleFields, ["title", "category", "rating"]);
  assert.deepEqual(decoded.capsules, capsules);
  assert.deepEqual([...decoded.docs], [7, 19]);
});

test("multi-resolution category-cell blocks round trip exact leaf routes", () => {
  const cell = geoCellForE7(latToE7(45.5017), lonToE7(-73.5673), 15);
  const block = geoCellBlock(cell, 9);
  assert.match(geoCellBlockKey("location", "category", 2, block), /^location\|category\|/u);
  const encoded = encodeGeoCellBlock({
    blockZoom: block.zoom,
    blockX: block.x,
    blockY: block.y,
    routes: [
      {
        zoom: 15,
        x: cell.x,
        y: cell.y,
        code: 7,
        members: [
          { leaf: 2, ordinals: [0, 4] },
          { leaf: 9, ordinals: [1] },
          { leaf: 15, ordinals: [3, 8] }
        ]
      },
      {
        zoom: 12,
        x: Math.floor(cell.x / 8),
        y: Math.floor(cell.y / 8),
        code: 7,
        members: [
          { leaf: 2, ordinals: [0, 4] },
          { leaf: 15, ordinals: [3, 8] }
        ]
      }
    ]
  });
  const decoded = decodeGeoCellBlock(encoded);
  assert.deepEqual(
    decoded.routes.get(`15:${cell.y}:${cell.x}:7`),
    new Map([[2, [0, 4]], [9, [1]], [15, [3, 8]]])
  );
  assert.deepEqual(
    decoded.routes.get(`12:${Math.floor(cell.y / 8)}:${Math.floor(cell.x / 8)}:7`),
    new Map([[2, [0, 4]], [15, [3, 8]]])
  );

  const cells = geoCellsForBoxes([{
    minLatE7: latToE7(45.49),
    maxLatE7: latToE7(45.51),
    minLonE7: lonToE7(-73.59),
    maxLonE7: lonToE7(-73.55)
  }], 12, 16);
  assert.ok(cells.length >= 1 && cells.length <= 16);
  assert.equal(geoCellsForBoxes([{
    minLatE7: latToE7(-80),
    maxLatE7: latToE7(80),
    minLonE7: lonToE7(-170),
    maxLonE7: lonToE7(170)
  }], 15, 16), null);
});

async function serveStatic(root) {
  const requests = [];
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://localhost");
      requests.push({ pathname: url.pathname, range: request.headers.range || "" });
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
    requests,
    close: () => new Promise(resolveClose => server.close(resolveClose))
  };
}

function placeFixtureDocs() {
  const random = mulberry32(42);
  const docs = [];
  const categories = ["bakery", "museum", "pharmacy", "cafe"];
  // Dense cluster around Montreal.
  for (let i = 0; i < 260; i++) {
    const category = categories[i % categories.length];
    docs.push({
      id: `mtl-${i}`,
      title: `Montreal ${category} ${i}`,
      body: `A ${category} in Montreal.`,
      category,
      rating: (i % 50) / 10,
      lat: 45.4 + random() * 0.3,
      lon: -73.8 + random() * 0.5
    });
  }
  // Sparse cluster around Quebec City.
  for (let i = 0; i < 60; i++) {
    const category = categories[i % categories.length];
    docs.push({
      id: `qc-${i}`,
      title: `Quebec ${category} ${i}`,
      body: `A ${category} in Quebec City.`,
      category,
      rating: (i % 50) / 10,
      lat: 46.75 + random() * 0.15,
      lon: -71.3 + random() * 0.25
    });
  }
  // Antimeridian cluster (Fiji-ish) on both sides of the dateline.
  for (let i = 0; i < 40; i++) {
    const category = categories[i % categories.length];
    docs.push({
      id: `fj-${i}`,
      title: `Fiji ${category} ${i}`,
      body: `A ${category} near the dateline.`,
      category,
      rating: (i % 50) / 10,
      lat: -17.8 + random() * 0.2,
      lon: i % 2 === 0 ? 179.9 + random() * 0.099 : -179.9 - random() * 0.099
    });
  }
  // A category that exists only in one tight cluster, to exercise per-leaf
  // filter summary pruning.
  for (let i = 0; i < 16; i++) {
    docs.push({
      id: `obs-${i}`,
      title: `Quebec observatory ${i}`,
      body: "An observatory near Quebec City.",
      category: "observatory",
      rating: (i % 50) / 10,
      lat: 46.8 + random() * 0.01,
      lon: -71.25 + random() * 0.01
    });
  }
  // One doc with no coordinates: text-searchable, geo-invisible.
  docs.push({ id: "nowhere", title: "Nowhere bakery", body: "A bakery with no coordinates.", category: "bakery", rating: 5 });
  return docs;
}

function oraclePoints(docs) {
  return docs
    .filter(doc => doc.lat != null && doc.lon != null)
    .map(doc => ({ ...doc, latE7: latToE7(doc.lat), lonE7: lonToE7(doc.lon) }));
}

async function runGeoOracleSuite(configOverrides, assertManifest) {
  const root = await mkdtemp(join(tmpdir(), "rangefind-geo-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  const docs = placeFixtureDocs();
  await writeFile(docsPath, docs.map(doc => JSON.stringify(doc)).join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    docValueChunkSize: 32,
    docValueSortedPageSize: 16,
    geoLeafSize: 32,
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: [{ name: "category", path: "category" }],
    numbers: [{ name: "rating", path: "rating", type: "float" }],
    geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
    display: ["title", "category", "rating"],
    ...configOverrides
  }));

  await build({ configPath });
  const manifest = JSON.parse(await readFile(join(output, "manifest.min.json"), "utf8"));
  assert.equal(manifest.features.geo, true);
  assert.ok(manifest.geo.fields.location.directory.file.startsWith("geo/"));
  assert.equal(manifest.geo.fields.location.total, docs.length - 1);
  assert.ok(manifest.numbers.some(field => field.name === "location.lat" && field.geo_component === "location"));
  if (assertManifest) assertManifest(manifest);

  const points = oraclePoints(docs);
  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });
    const titles = response => response.results.map(result => result.title).sort();

    // Bounding-box browse against the oracle.
    const box = { minLat: 45.45, maxLat: 45.62, minLon: -73.7, maxLon: -73.45 };
    const boxE7 = {
      minLatE7: latToE7(box.minLat),
      maxLatE7: latToE7(box.maxLat),
      minLonE7: lonToE7(box.minLon),
      maxLonE7: lonToE7(box.maxLon)
    };
    const boxExpected = points.filter(point => boxContainsPointE7(boxE7, point.latE7, point.lonE7));
    const boxRequestStart = server.requests.length;
    const boxResponse = await engine.search({ q: "", geo: { box }, size: 100 });
    assert.equal(boxResponse.total, boxExpected.length);
    assert.deepEqual(titles(boxResponse), boxExpected.map(point => point.title).sort());
    assert.equal(boxResponse.stats.geoLane, "browse");
    assert.ok(boxResponse.stats.geoLeavesVisited <= boxResponse.stats.geoDirectoryLeaves);
    if (manifest.geo.fields.location.capsules) {
      const boxRequests = server.requests.slice(boxRequestStart);
      assert.equal(boxResponse.stats.docPayloadLane, "geoCapsules");
      assert.equal(boxResponse.stats.geoCapsuleHits, boxResponse.results.length);
      assert.equal(boxResponse.stats.geoCapsuleMisses, 0);
      assert.ok(!boxRequests.some(request => request.pathname.includes("/docs/")));
    }

    // Radius browse with distances.
    const center = { lat: 45.5017, lon: -73.5673 };
    const centerE7 = { latE7: latToE7(center.lat), lonE7: lonToE7(center.lon) };
    const radius = 12000;
    const radiusExpected = points.filter(point => haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7) <= radius);
    const radiusResponse = await engine.search({ q: "", geo: { near: { ...center, radiusMeters: radius } }, size: 100 });
    assert.equal(radiusResponse.total, radiusExpected.length);
    assert.deepEqual(titles(radiusResponse), radiusExpected.map(point => point.title).sort());
    for (const result of radiusResponse.results) {
      assert.ok(result.distanceMeters <= radius);
    }

    // Exact nearest-neighbor order with an early-stop proof.
    const nearestResponse = await engine.search({ q: "", geo: { near: center, sort: "distance" }, size: 10 });
    const nearestExpected = points
      .map(point => ({ title: point.title, dist: haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7) }))
      .sort((a, b) => a.dist - b.dist)
      .slice(0, 10);
    assert.deepEqual(
      nearestResponse.results.map(result => result.title),
      nearestExpected.map(item => item.title)
    );
    assert.deepEqual(
      nearestResponse.results.map(result => result.distanceMeters),
      nearestExpected.map(item => Math.round(item.dist * 10) / 10)
    );
    assert.equal(nearestResponse.stats.exact, true);
    assert.ok(nearestResponse.stats.geoLeavesVisited < nearestResponse.stats.geoCandidateLeaves);

    // An explicit viewport can be ordered from its center without admitting
    // results outside the box. This is the map-search shape used for exact
    // brands: the box is the hard predicate and near only supplies ordering.
    const viewportNearest = await engine.search({
      q: "bakery",
      geo: { box, near: center, sort: "distance" },
      size: 10
    });
    const viewportNearestExpected = boxExpected
      .filter(point => point.category === "bakery")
      .map(point => ({
        title: point.title,
        dist: haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7)
      }))
      .sort((a, b) => a.dist - b.dist)
      .slice(0, 10);
    assert.deepEqual(
      viewportNearest.results.map(result => result.title),
      viewportNearestExpected.map(item => item.title)
    );
    assert.deepEqual(
      viewportNearest.results.map(result => result.distanceMeters),
      viewportNearestExpected.map(item => Math.round(item.dist * 10) / 10)
    );
    assert.ok(viewportNearest.results.every(result => boxExpected.some(point => point.title === result.title)));

    if (manifest.geo.fields.location.category_cells?.length) {
      const requestStart = server.requests.length;
      const categoryCells = await engine.search({
        q: "",
        filters: { facets: { category: ["bakery"] } },
        geo: { box, near: center, sort: "distance" },
        size: 20
      });
      const cellRequests = server.requests.slice(requestStart);
      const expected = boxExpected
        .filter(point => point.category === "bakery")
        .map(point => ({
          title: point.title,
          dist: haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7)
        }))
        .sort((a, b) => a.dist - b.dist)
        .slice(0, 20);
      assert.deepEqual(categoryCells.results.map(result => result.title), expected.map(item => item.title));
      assert.match(categoryCells.stats.geoLane, /CategoryCells$/u);
      assert.ok(categoryCells.stats.geoCategoryCellLevel > 0);
      assert.ok(categoryCells.stats.geoCategoryCellBlocksFetched > 0);
      assert.ok(cellRequests.some(request => request.pathname.includes("/geo/category-cells/")));

      const multiCategory = await engine.search({
        q: "",
        filters: { facets: { category: ["bakery", "museum"] } },
        geo: { box, near: center, sort: "distance" },
        size: 20
      });
      const multiExpected = boxExpected
        .filter(point => point.category === "bakery" || point.category === "museum")
        .map(point => ({
          title: point.title,
          dist: haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7)
        }))
        .sort((a, b) => a.dist - b.dist)
        .slice(0, 20);
      assert.deepEqual(
        multiCategory.results.map(result => result.title),
        multiExpected.map(item => item.title)
      );

      const fallback = await createSearch({ baseUrl: server.baseUrl, geoCellIndexes: false });
      const fallbackStart = server.requests.length;
      const fallbackResponse = await fallback.search({
        q: "",
        filters: { facets: { category: ["bakery"] } },
        geo: { box, near: center, sort: "distance" },
        size: 20
      });
      const fallbackRequests = server.requests.slice(fallbackStart);
      assert.deepEqual(
        categoryCells.results.map(result => result.title),
        fallbackResponse.results.map(result => result.title)
      );
      const pointPacks = requests => requests.filter(request => request.pathname.includes("/geo/point-packs/")).length;
      assert.ok(
        pointPacks(cellRequests) < pointPacks(fallbackRequests),
        `category cells should fetch fewer point ranges (${pointPacks(cellRequests)} vs ${pointPacks(fallbackRequests)})`
      );

      const broadFallback = await engine.search({
        q: "",
        filters: { facets: { category: ["bakery"] } },
        geo: { box: { minLat: -80, maxLat: 80, minLon: -170, maxLon: 170 } },
        size: 5
      });
      assert.doesNotMatch(broadFallback.stats.geoLane, /CategoryCells/u);
      assert.equal(broadFallback.stats.geoCategoryCellLevel, null);
    }

    // Nearest with facet + numeric filters.
    const filteredNearest = await engine.search({
      q: "",
      filters: { facets: { category: ["bakery"] }, numbers: { rating: { min: 2 } } },
      geo: { near: center, sort: "distance" },
      size: 5
    });
    const filteredExpected = points
      .filter(point => point.category === "bakery" && point.rating >= 2)
      .map(point => ({ title: point.title, dist: haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7) }))
      .sort((a, b) => a.dist - b.dist)
      .slice(0, 5);
    assert.deepEqual(filteredNearest.results.map(result => result.title), filteredExpected.map(item => item.title));

    // Per-leaf filter summaries prune cells that provably lack the facet, so
    // a sparse-category nearest query touches almost no leaves.
    const obsNearest = await engine.search({
      q: "",
      filters: { facets: { category: ["observatory"] } },
      geo: { near: center, sort: "distance" },
      size: 5
    });
    const obsExpected = points
      .filter(point => point.category === "observatory")
      .map(point => ({ title: point.title, dist: haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7) }))
      .sort((a, b) => a.dist - b.dist)
      .slice(0, 5);
    assert.deepEqual(obsNearest.results.map(result => result.title), obsExpected.map(item => item.title));
    assert.ok(
      obsNearest.stats.geoCandidateLeaves <= 4,
      `filter summaries should prune to the observatory cells, saw ${obsNearest.stats.geoCandidateLeaves}`
    );

    // Text search restricted to a radius (also covers the no-coordinates doc).
    const textRadius = await engine.search({ q: "bakery", geo: { near: { ...center, radiusMeters: radius } }, size: 100 });
    const textRadiusExpected = radiusExpected.filter(point => point.category === "bakery");
    assert.deepEqual(titles(textRadius), textRadiusExpected.map(point => point.title).sort());
    assert.ok(!textRadius.results.some(result => result.title === "Nowhere bakery"));
    for (const result of textRadius.results) {
      assert.ok(result.distanceMeters <= radius);
    }

    // Text search restricted to a bounding box.
    const textBox = await engine.search({ q: "museum", geo: { box }, size: 100 });
    const textBoxExpected = boxExpected.filter(point => point.category === "museum");
    assert.deepEqual(titles(textBox), textBoxExpected.map(point => point.title).sort());

    // Distance boost reorders the page window and reports stats.
    const boosted = await engine.search({
      q: "bakery",
      geo: { near: { ...center, radiusMeters: radius }, boost: { weight: 4, pivotMeters: 500 } },
      size: 10
    });
    assert.equal(boosted.stats.geoBoost, true);
    const boostedScores = boosted.results.map(result => result.score);
    assert.deepEqual(boostedScores, [...boostedScores].sort((a, b) => b - a));

    // Dateline-crossing radius sees both sides of the antimeridian.
    const fijiCenter = { lat: -17.75, lon: 179.995 };
    const fijiE7 = { latE7: latToE7(fijiCenter.lat), lonE7: lonToE7(fijiCenter.lon) };
    const fijiRadius = 40000;
    const fijiExpected = points.filter(point => haversineMetersE7(fijiE7.latE7, fijiE7.lonE7, point.latE7, point.lonE7) <= fijiRadius);
    assert.ok(fijiExpected.some(point => point.lon > 0) && fijiExpected.some(point => point.lon < 0), "fixture must straddle the dateline");
    const fijiResponse = await engine.search({ q: "", geo: { near: { ...fijiCenter, radiusMeters: fijiRadius } }, size: 100 });
    assert.equal(fijiResponse.total, fijiExpected.length);
    assert.deepEqual(titles(fijiResponse), fijiExpected.map(point => point.title).sort());

    // Dateline-crossing box (minLon > maxLon).
    const datelineBox = { minLat: -18.0, maxLat: -17.6, minLon: 179.9, maxLon: -179.9 };
    const datelineExpected = points.filter(point => (
      point.latE7 >= latToE7(datelineBox.minLat) && point.latE7 <= latToE7(datelineBox.maxLat)
      && (point.lonE7 >= lonToE7(179.9) || point.lonE7 <= lonToE7(-179.9))
    ));
    const datelineResponse = await engine.search({ q: "", geo: { box: datelineBox }, size: 100 });
    assert.equal(datelineResponse.total, datelineExpected.length);
    assert.deepEqual(titles(datelineResponse), datelineExpected.map(point => point.title).sort());
    if (manifest.geo.fields.location.category_cells?.length) {
      const datelineCategoryExpected = datelineExpected.filter(point => point.category === "bakery");
      const datelineCategory = await engine.search({
        q: "",
        filters: { facets: { category: ["bakery"] } },
        geo: { box: datelineBox },
        size: 100
      });
      assert.match(datelineCategory.stats.geoLane, /CategoryCells$/u);
      assert.equal(datelineCategory.total, datelineCategoryExpected.length);
      assert.deepEqual(
        titles(datelineCategory),
        datelineCategoryExpected.map(point => point.title).sort()
      );
    }

    // Sorting by another field with a geo filter goes through the sorted lane.
    const sortedResponse = await engine.search({
      q: "",
      geo: { box },
      sort: { field: "rating", order: "desc" },
      size: 10
    });
    const sortedExpected = boxExpected
      .slice()
      .sort((a, b) => b.rating - a.rating || a.title.localeCompare(b.title))
      .slice(0, 10);
    assert.equal(sortedResponse.results.length, sortedExpected.length);
    for (const result of sortedResponse.results) {
      assert.ok(boxExpected.some(point => point.title === result.title));
    }
    assert.deepEqual(
      sortedResponse.results.map(result => result.rating),
      sortedExpected.map(point => point.rating)
    );

    // Pagination is stable and non-overlapping for nearest queries.
    const pageOne = await engine.search({ q: "", geo: { near: center, sort: "distance" }, size: 5, page: 1 });
    const pageTwo = await engine.search({ q: "", geo: { near: center, sort: "distance" }, size: 5, page: 2 });
    const nearestTen = points
      .map(point => ({ title: point.title, dist: haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7) }))
      .sort((a, b) => a.dist - b.dist)
      .slice(0, 10)
      .map(item => item.title);
    assert.deepEqual([...pageOne.results, ...pageTwo.results].map(result => result.title), nearestTen);

    // A continental radius fills the page and reports an approximate total.
    const exhaustive = await engine.search({ q: "", geo: { near: { ...center, radiusMeters: 2500000 } }, size: 100 });
    assert.equal(exhaustive.results.length, 100);
    assert.ok(exhaustive.total >= 100);
    // A smaller page-size radius that is fully scanned reports exact totals.
    const scannedAll = await engine.search({ q: "", geo: { near: { ...center, radiusMeters: 2500000 } }, size: 100, page: 4 });
    assert.equal(scannedAll.total, points.length - 40, "continental radius should match all non-Fiji points exactly");

    // Geo queries on unknown fields fail clearly.
    await assert.rejects(
      engine.search({ q: "", geo: { field: "unknown", box } }),
      /unknown geo field/
    );
    // Text + exact distance sort: nearest bakeries in true distance order.
    const textNearest = await engine.search({ q: "bakery", geo: { near: center, sort: "distance" }, size: 5 });
    const textNearestExpected = points
      .filter(point => point.category === "bakery")
      .map(point => ({ title: point.title, dist: haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7) }))
      .sort((a, b) => a.dist - b.dist)
      .slice(0, 5);
    assert.deepEqual(textNearest.results.map(result => result.title), textNearestExpected.map(item => item.title));
    assert.deepEqual(
      textNearest.results.map(result => result.distanceMeters),
      textNearestExpected.map(item => Math.round(item.dist * 10) / 10)
    );
    assert.equal(textNearest.stats.geoLane, "nearestText");
    assert.equal(textNearest.stats.exact, true);
    assert.ok(textNearest.stats.textMatchDocs >= textNearestExpected.length);
    assert.ok(!textNearest.results.some(result => result.title === "Nowhere bakery"));

    // Text + distance sort composes with facet and numeric filters.
    const filteredTextNearest = await engine.search({
      q: "montreal",
      filters: { numbers: { rating: { min: 2 } } },
      geo: { near: center, sort: "distance" },
      size: 5
    });
    const filteredTextExpected = points
      .filter(point => point.title.startsWith("Montreal") && point.rating >= 2)
      .map(point => ({ title: point.title, dist: haversineMetersE7(centerE7.latE7, centerE7.lonE7, point.latE7, point.lonE7) }))
      .sort((a, b) => a.dist - b.dist)
      .slice(0, 5);
    assert.deepEqual(filteredTextNearest.results.map(result => result.title), filteredTextExpected.map(item => item.title));

    // The posting budget produces a clear error instead of silent truncation.
    const tinyBudget = await createSearch({ baseUrl: server.baseUrl, geoTextSortMaxDf: 1 });
    await assert.rejects(
      tinyBudget.search({ q: "bakery", geo: { near: center, sort: "distance" } }),
      /geoTextSortMaxDf/
    );
  } finally {
    await server.close();
  }
}

test("geo queries agree with an exhaustive oracle (single-level tree)", async () => {
  await runGeoOracleSuite({}, manifest => {
    assert.equal(manifest.geo.fields.location.levels, 1);
  });
});

test("geo queries agree with an exhaustive oracle (two-level branch-paged tree)", async () => {
  await runGeoOracleSuite({ geoBranchLeaves: 4 }, manifest => {
    assert.equal(manifest.geo.fields.location.levels, 2);
    assert.ok(manifest.geo.fields.location.branches >= 2);
  });
});

test("geo cell capsules return complete map rows without document fetches", async () => {
  await runGeoOracleSuite({ geoCapsules: true }, manifest => {
    assert.equal(manifest.geo.page_format, "rfgeoleafpage-v2");
    assert.deepEqual(
      manifest.geo.fields.location.capsules.fields,
      ["title", "category", "rating"]
    );
    assert.equal(manifest.geo.fields.location.capsules.rows, manifest.geo.fields.location.total);
  });
});

test("multi-resolution category cells route exact map results to fewer geo leaves", async () => {
  await runGeoOracleSuite({
    geoCapsules: true,
    geoBranchLeaves: 4,
    geoCellSortChunkRecords: 20,
    geoCellIndexes: [{
      field: "location",
      facet: "category",
      levels: [6, 10, 14],
      blockZoom: 6,
      codeGroupSize: 4,
      maxCellsPerQuery: 48,
      values: ["bakery", "museum", "pharmacy", "cafe", "observatory"]
    }]
  }, manifest => {
    assert.equal(manifest.features.geoCategoryCells, true);
    assert.equal(manifest.geo.fields.location.category_cells[0].format, "rfgeocategorycell-v1");
    assert.deepEqual(manifest.geo.fields.location.category_cells[0].levels, [6, 10, 14]);
    assert.ok(manifest.geo.category_cell_packs >= 1);
  });
});

test("non-geo builds emit no geo artifacts and reject geo queries", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-nongeo-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, [
    JSON.stringify({ id: "a", title: "Plain text doc", body: "No coordinates here.", lat: 45.5, lon: -73.5 }),
    JSON.stringify({ id: "b", title: "Another doc", body: "Still no geo schema." })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    display: ["title"]
  }));
  await build({ configPath });
  const manifest = JSON.parse(await readFile(join(output, "manifest.min.json"), "utf8"));
  assert.equal(manifest.features.geo, false);
  assert.equal(manifest.geo, null);
  assert.equal(existsSync(join(output, "geo")), false);
  assert.ok(!manifest.numbers.some(field => field.geo_component));

  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });
    const plain = await engine.search({ q: "plain text" });
    assert.ok(plain.results.length >= 1);
    await assert.rejects(
      engine.search({ q: "", geo: { near: { lat: 45.5, lon: -73.5, radiusMeters: 1000 } } }),
      /no geo fields/
    );
  } finally {
    await server.close();
  }
});
