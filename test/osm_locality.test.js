import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { scanPbf } from "../scripts/osm_pbf.mjs";
import { extractOsmPlaces } from "../scripts/osm_fixture.mjs";
import { enrichDocLocality } from "../src/integrations/osm/documents.js";
import { assembleRings, createLocalityIndex } from "../src/integrations/osm/locality_index.js";

// --- minimal OSM PBF encoder (enough for scanPbf's subset) -----------------

function pushVarint(out, value) {
  let remaining = value;
  while (remaining >= 0x80) {
    out.push((remaining & 0x7f) | 0x80);
    remaining = Math.floor(remaining / 128);
  }
  out.push(remaining);
}

function zigzag(value) {
  return value >= 0 ? value * 2 : -value * 2 - 1;
}

function pushBytesField(out, field, bytes) {
  pushVarint(out, (field << 3) | 2);
  pushVarint(out, bytes.length);
  out.push(...bytes);
}

function pushVarintField(out, field, value) {
  pushVarint(out, field << 3);
  pushVarint(out, value);
}

function packed(values) {
  const out = [];
  for (const value of values) pushVarint(out, value);
  return out;
}

function utf8(text) {
  return [...Buffer.from(text, "utf8")];
}

// Coordinates encode at the default granularity (100 nanodegree units).
function coord(value) {
  return Math.round(value / 1e-7);
}

function encodeStringTable(strings) {
  const out = [];
  for (const value of strings) pushBytesField(out, 1, utf8(value));
  return out;
}

function tagIndexes(strings, tags) {
  const keys = [];
  const vals = [];
  for (const [key, value] of Object.entries(tags || {})) {
    keys.push(strings.indexOf(key));
    vals.push(strings.indexOf(value));
  }
  return { keys, vals };
}

function encodeNode(strings, node) {
  const out = [];
  pushVarintField(out, 1, zigzag(node.id));
  const { keys, vals } = tagIndexes(strings, node.tags);
  if (keys.length) {
    pushBytesField(out, 2, packed(keys));
    pushBytesField(out, 3, packed(vals));
  }
  pushVarintField(out, 8, zigzag(coord(node.lat)));
  pushVarintField(out, 9, zigzag(coord(node.lon)));
  return out;
}

function encodeWay(strings, way) {
  const out = [];
  pushVarintField(out, 1, way.id);
  const { keys, vals } = tagIndexes(strings, way.tags);
  if (keys.length) {
    pushBytesField(out, 2, packed(keys));
    pushBytesField(out, 3, packed(vals));
  }
  const deltas = [];
  let previous = 0;
  for (const ref of way.refs) {
    deltas.push(zigzag(ref - previous));
    previous = ref;
  }
  pushBytesField(out, 8, packed(deltas));
  return out;
}

const MEMBER_TYPE_CODES = { node: 0, way: 1, relation: 2 };

function encodeRelation(strings, relation) {
  const out = [];
  pushVarintField(out, 1, relation.id);
  const { keys, vals } = tagIndexes(strings, relation.tags);
  if (keys.length) {
    pushBytesField(out, 2, packed(keys));
    pushBytesField(out, 3, packed(vals));
  }
  pushBytesField(out, 8, packed(relation.members.map(member => strings.indexOf(member.role))));
  const deltas = [];
  let previous = 0;
  for (const member of relation.members) {
    deltas.push(zigzag(member.ref - previous));
    previous = member.ref;
  }
  pushBytesField(out, 9, packed(deltas));
  pushBytesField(out, 10, packed(relation.members.map(member => MEMBER_TYPE_CODES[member.type])));
  return out;
}

function encodeBlock(strings, { nodes = [], ways = [], relations = [] }) {
  const group = [];
  for (const node of nodes) pushBytesField(group, 1, encodeNode(strings, node));
  for (const way of ways) pushBytesField(group, 3, encodeWay(strings, way));
  for (const relation of relations) pushBytesField(group, 4, encodeRelation(strings, relation));
  const block = [];
  pushBytesField(block, 1, encodeStringTable(strings));
  pushBytesField(block, 2, group);
  return block;
}

function encodePbf(blocks) {
  const chunks = [];
  for (const block of blocks) {
    const blob = [];
    pushBytesField(blob, 1, block); // raw (uncompressed) blob
    const header = [];
    pushBytesField(header, 1, utf8("OSMData"));
    pushVarintField(header, 3, blob.length);
    const length = Buffer.alloc(4);
    length.writeUInt32BE(header.length, 0);
    chunks.push(length, Buffer.from(header), Buffer.from(blob));
  }
  return Buffer.concat(chunks);
}

function collectStrings(elements) {
  const strings = [""];
  const add = value => {
    if (!strings.includes(value)) strings.push(value);
  };
  for (const list of elements) {
    for (const item of list) {
      for (const [key, value] of Object.entries(item.tags || {})) {
        add(key);
        add(value);
      }
      for (const member of item.members || []) add(member.role);
    }
  }
  return strings;
}

// --- fixture: a square municipality with a hole, plus a fallback hamlet ----
//
// "Testville" is a 0.2° square boundary (admin_level 8) with a small square
// hole. Inside it: an untagged-address pharmacy. Inside the hole: a cafe
// (must NOT resolve to Testville; it falls back to the place node
// "Holeburg" sitting in the hole). Far away: a bakery near the hamlet
// "Fallbackton" with no boundary at all.
function fixtureElements() {
  const nodes = [];
  let nextId = 1;
  const ring = (minLat, minLon, maxLat, maxLon) => {
    const ids = [];
    for (const [lat, lon] of [[minLat, minLon], [minLat, maxLon], [maxLat, maxLon], [maxLat, minLon]]) {
      nodes.push({ id: nextId, lat, lon });
      ids.push(nextId++);
    }
    return ids;
  };
  const outer = ring(45.0, -73.2, 45.2, -73.0);
  const hole = ring(45.08, -73.12, 45.12, -73.08);

  const pois = [
    { id: 9001, lat: 45.05, lon: -73.15, tags: { name: "Pharmacie Test", amenity: "pharmacy" } },
    { id: 9002, lat: 45.1, lon: -73.1, tags: { name: "Cafe Hole", amenity: "cafe" } },
    { id: 9003, lat: 47.001, lon: -70.001, tags: { name: "Boulangerie Loin", shop: "bakery" } },
    { id: 9004, lat: 45.06, lon: -73.14, tags: { name: "Chez Tagged", amenity: "restaurant", "addr:city": "Mapperville" } },
    { id: 9010, lat: 45.1, lon: -73.1, tags: { name: "Holeburg", place: "village" } },
    { id: 9011, lat: 47.0, lon: -70.0, tags: { name: "Fallbackton", place: "hamlet" } },
    { id: 9012, lat: 45.09, lon: -73.11, tags: { name: "Testville", place: "town" } }
  ];
  nodes.push(...pois);

  // Outer ring split across two ways sharing endpoints, the second reversed —
  // exercises multi-way stitching. The hole is one closed way.
  const ways = [
    { id: 501, refs: [outer[0], outer[1], outer[2]], tags: {} },
    { id: 502, refs: [outer[0], outer[3], outer[2]], tags: {} },
    { id: 503, refs: [...hole, hole[0]], tags: {} }
  ];
  const relations = [
    {
      id: 7001,
      tags: { boundary: "administrative", admin_level: "8", name: "Testville" },
      members: [
        { type: "way", ref: 501, role: "outer" },
        { type: "way", ref: 502, role: "outer" },
        { type: "way", ref: 503, role: "inner" },
        { type: "node", ref: 9012, role: "admin_centre" }
      ]
    }
  ];
  return { nodes: nodes.sort((a, b) => a.id - b.id), ways, relations };
}

test("scanPbf parses relations with members and roles", async () => {
  const dir = await mkdtemp(join(tmpdir(), "rangefind-pbf-"));
  const { nodes, ways, relations } = fixtureElements();
  const strings = collectStrings([nodes, ways, relations]);
  const pbf = join(dir, "test.osm.pbf");
  await writeFile(pbf, encodePbf([encodeBlock(strings, { nodes, ways, relations })]));

  const seen = { nodes: [], ways: [], relations: [] };
  const counts = scanPbf(pbf, {
    onNode(id, lat, lon, tags) {
      seen.nodes.push({ id, lat, lon, tags });
    },
    onWay(id, refs, tags) {
      seen.ways.push({ id, refs: [...refs], tags });
    },
    onRelation(id, members, tags) {
      seen.relations.push({ id, members, tags });
    }
  });
  assert.equal(counts.relations, 1);
  assert.equal(seen.relations[0].id, 7001);
  assert.equal(seen.relations[0].tags.get("name"), "Testville");
  assert.deepEqual(
    seen.relations[0].members.map(member => [member.type, member.ref, member.role]),
    [["way", 501, "outer"], ["way", 502, "outer"], ["way", 503, "inner"], ["node", 9012, "admin_centre"]]
  );
  const pharmacy = seen.nodes.find(node => node.id === 9001);
  assert.ok(Math.abs(pharmacy.lat - 45.05) < 1e-6);
  assert.equal(pharmacy.tags.get("amenity"), "pharmacy");
  assert.deepEqual(seen.ways.find(way => way.id === 503).refs.length, 5);
});

test("assembleRings stitches multi-way rings and drops open chains", () => {
  const closed = assembleRings([
    [1, 2, 3],
    [5, 4, 3], // reversed continuation
    [1, 5]     // reversed closure
  ]);
  assert.equal(closed.dropped, 0);
  assert.equal(closed.rings.length, 1);
  const ring = closed.rings[0];
  assert.equal(ring[0], ring[ring.length - 1]);
  assert.deepEqual([...ring].sort((a, b) => a - b).filter((v, i, arr) => arr.indexOf(v) === i), [1, 2, 3, 4, 5]);

  const open = assembleRings([[1, 2, 3], [4, 5, 6]]);
  assert.equal(open.rings.length, 0);
  assert.equal(open.dropped, 2);
});

test("locality index resolves boundaries with holes and place fallbacks", () => {
  const index = createLocalityIndex();
  const square = (minLat, minLon, maxLat, maxLon) => [
    [minLat, minLon], [minLat, maxLon], [maxLat, maxLon], [maxLat, minLon], [minLat, minLon]
  ];
  assert.ok(index.addBoundary({
    id: 1,
    name: "Testville",
    adminLevel: 8,
    rings: [square(45.0, -73.2, 45.2, -73.0), square(45.08, -73.12, 45.12, -73.08)]
  }));
  // A larger level-7 parent: level 8 must win inside Testville.
  assert.ok(index.addBoundary({
    id: 2,
    name: "Test County",
    adminLevel: 7,
    rings: [square(44.5, -73.7, 45.7, -72.5)]
  }));
  assert.ok(index.addPlace({ name: "Holeburg", type: "village", lat: 45.1, lon: -73.1 }));
  assert.ok(index.addPlace({ name: "Fallbackton", type: "hamlet", lat: 47.0, lon: -70.0 }));
  assert.ok(index.addPlace({ name: "Bigcity", type: "city", lat: 47.05, lon: -70.05 }));
  index.finalize();

  assert.deepEqual(index.resolve(45.05, -73.15), { city: "Testville", source: "boundary" });
  // Inside the hole: not Testville; the county still contains it… but the
  // hole is only cut from Testville, so the level-7 parent claims it.
  assert.deepEqual(index.resolve(45.1, -73.1), { city: "Test County", source: "boundary" });
  // Outside every boundary, close to the hamlet: distance/radius scoring
  // prefers the near hamlet over the bigger-but-farther city.
  assert.deepEqual(index.resolve(47.001, -70.001), { city: "Fallbackton", source: "place" });
  // Far from everything: no locality.
  assert.equal(index.resolve(10, 10), null);
});

test("enrichDocLocality stamps city and search text without touching addresses", () => {
  const bare = { name: "Jean Coutu", category: "amenity", type: "pharmacy" };
  assert.ok(enrichDocLocality(bare, "Rosemère"));
  assert.equal(bare.city, "Rosemère");
  assert.equal(bare.address_search, "Rosemère");
  assert.equal(bare.address, undefined);

  const addressed = { name: "Osteria", address: "2 Rue Haute", address_search: "2 Rue Haute" };
  assert.ok(enrichDocLocality(addressed, "Dudelange"));
  assert.equal(addressed.address_search, "2 Rue Haute, Dudelange");
  assert.equal(addressed.address, "2 Rue Haute");

  const already = { name: "X", city: "Mapperville", address_search: "1 Main, Mapperville" };
  assert.equal(enrichDocLocality(already, "Elsewhere"), false);
  assert.equal(already.city, "Mapperville");

  const place = { name: "Rosemère", category: "place", type: "town" };
  assert.equal(enrichDocLocality(place, "Rosemère"), false);
  assert.equal(place.city, undefined);

  const containing = { name: "Y", address_search: "3 Rue de Dudelange" };
  assert.ok(enrichDocLocality(containing, "Dudelange"));
  assert.equal(containing.address_search, "3 Rue de Dudelange");
  assert.equal(containing.city, "Dudelange");
});

test("extraction enriches documents from boundaries and place nodes end to end", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-osm-locality-"));
  const { nodes, ways, relations } = fixtureElements();
  const strings = collectStrings([nodes, ways, relations]);
  const pbf = join(root, "test.osm.pbf");
  await writeFile(pbf, encodePbf([encodeBlock(strings, { nodes, ways, relations })]));

  const meta = await extractOsmPlaces({ root, pbf, region: "testville" });
  assert.equal(meta.locality.boundaries, 1);
  assert.ok(meta.locality.enrichedFromBoundaries >= 1);
  assert.ok(meta.locality.enrichedFromPlaces >= 2);

  const docs = (await readFile(join(root, "data", "osm-places.jsonl"), "utf8"))
    .trim().split("\n").map(line => JSON.parse(line));
  const byName = new Map(docs.map(doc => [doc.name, doc]));

  // Inside the boundary: stamped Testville.
  assert.equal(byName.get("Pharmacie Test").city, "Testville");
  assert.equal(byName.get("Pharmacie Test").address_search, "Testville");
  // Inside the hole: the boundary excludes it; the village node claims it.
  assert.equal(byName.get("Cafe Hole").city, "Holeburg");
  // Outside all boundaries: hamlet radius fallback.
  assert.equal(byName.get("Boulangerie Loin").city, "Fallbackton");
  // Mapper-provided addr:city always wins.
  assert.equal(byName.get("Chez Tagged").city, "Mapperville");
  // Place documents never get stamped with themselves.
  assert.equal(byName.get("Testville").city, undefined);
});
