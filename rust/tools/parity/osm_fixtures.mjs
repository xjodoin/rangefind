#!/usr/bin/env node
// OSM query layer parity fixtures (rust/rangefind/tests/osm_parity.rs).
//
//   node rust/tools/parity/osm_fixtures.mjs            # everything
//   node rust/tools/parity/osm_fixtures.mjs --cases    # reuse built indexes
//
// Local data only; nothing is downloaded. Writes under rust/target/osm-parity/:
//
//   lux/public/rangefind          Luxembourg OSM index (scripts/osm_fixture.mjs,
//                                 built from examples/osm-geo/data/luxembourg-latest.osm.pbf)
//   luxsharded/public/rangefind   the same corpus split into 3 longitude shards
//                                 (sharded root with text + suggest routing and
//                                 the embedded category lexicon)
//   testville/, twoshards/        the small synthetic indexes the JS tests build
//   cases/<index>.json.gz         recorded cases: every query.js call with its
//                                 cold output (fresh engine), warm output (same
//                                 engine, second call) and every engine call made
//   replay/<test>.json.gz         the mock-engine unit tests, recorded through
//                                 osm_record_hooks.mjs
//
// Each case records the engine calls (method, params, response) so the Rust
// test can replay the OSM layer in isolation, and the index path so it can
// also run the full Rust stack live.

import { execFileSync } from "node:child_process";
import { createReadStream, existsSync, mkdirSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath, pathToFileURL } from "node:url";
import { gzipSync } from "node:zlib";
import { performance } from "node:perf_hooks";

import * as query from "../../../src/integrations/osm/query.js";
import { createRangefindMapsAdapter } from "../../../src/integrations/osm/google_maps_adapter.js";
import { createNodeSearch } from "../../../src/runtime.node.js";
import { buildOsmIndex, buildOsmShardedIndex } from "../../../src/integrations/osm/node/builder.js";
import { createRecorder, ENGINE_FUNCTIONS, PURE_FUNCTIONS } from "./osm_record.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, "../../..");
const out = resolve(repo, "rust/target/osm-parity");
const args = new Set(process.argv.slice(2));
const casesOnly = args.has("--cases");
const only = [...args].find(arg => arg.startsWith("--only="))?.slice(7).split(",") || null;

function log(line) {
  console.log(`[osm-parity] ${line}`);
}

// ---------------------------------------------------------------- indexes

function ensureLuxembourg() {
  const root = join(out, "lux");
  const index = join(root, "public", "rangefind");
  if (existsSync(join(index, "manifest.json"))) return index;
  if (casesOnly) throw new Error(`missing ${index}; run without --cases`);
  const pbf = join(repo, "examples/osm-geo/data/luxembourg-latest.osm.pbf");
  if (!existsSync(pbf)) throw new Error(`missing ${pbf}; this script never downloads data`);
  mkdirSync(join(root, "data"), { recursive: true });
  const link = join(root, "data", "luxembourg-latest.osm.pbf");
  if (!existsSync(link)) symlinkSync(pbf, link);
  log("building the Luxembourg index (scripts/osm_fixture.mjs)");
  execFileSync(process.execPath, [
    join(repo, "scripts/osm_fixture.mjs"), "all", "--region=luxembourg", "--no-rqa", `--root=${root}`
  ], { cwd: repo, stdio: "inherit" });
  return index;
}

async function splitCorpus(input, outputs, pick) {
  const { createWriteStream } = await import("node:fs");
  const streams = outputs.map(path => createWriteStream(path));
  const lines = createInterface({ input: createReadStream(input), crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line.trim()) continue;
    const doc = JSON.parse(line);
    streams[pick(doc)].write(`${line}\n`);
  }
  await Promise.all(streams.map(stream => new Promise(done => stream.end(done))));
}

async function ensureLuxSharded() {
  const root = join(out, "luxsharded");
  const index = join(root, "public", "rangefind");
  if (existsSync(join(index, "manifest.json"))) return index;
  if (casesOnly) throw new Error(`missing ${index}; run without --cases`);
  mkdirSync(join(root, "data"), { recursive: true });
  const ids = ["west", "center", "east"];
  const inputs = ids.map(id => join(root, "data", `${id}.jsonl`));
  log("splitting the Luxembourg corpus into 3 longitude shards");
  await splitCorpus(join(out, "lux", "data", "osm-places.jsonl"), inputs, doc => {
    const lon = Number(doc.lon);
    return lon < 6.05 ? 0 : lon < 6.2 ? 1 : 2;
  });
  log("building the sharded Luxembourg index");
  await buildOsmShardedIndex({
    output: index,
    region: "luxembourg",
    workerCount: 6,
    buildProgressLogMs: 0,
    shards: ids.map((id, i) => ({ id, input: inputs[i] }))
  });
  return index;
}

const TESTVILLE_DOCS = [
  { id: "node/1", name: "Testville", search_name: "Testville", body: "city place", category: "place", type: "city", population: 120000, lat: 45.5, lon: -73.6, geo_lat: 45.5, geo_lon: -73.6 },
  { id: "node/2", name: "Testville Cinema", search_name: "Testville Cinema", aliases: ["Picture House", "Salle obscure"], body: "cinema amenity", category: "amenity", type: "cinema", lat: 45.51, lon: -73.61, geo_lat: 45.51, geo_lon: -73.61 },
  { id: "way/3", name: "Test Road", search_name: "Test Road", body: "primary highway", category: "highway", type: "primary", street: "Test Road", city: "Testville", street_authority: "Test Road, Testville", prominence: 0.05, lat: 45.505, lon: -73.605, geo_lat: 45.505, geo_lon: -73.605 },
  { id: "way/4", name: "Second Road", search_name: "Second Road", body: "secondary highway", category: "highway", type: "secondary", street: "Second Road", city: "Testville", street_authority: "Second Road, Testville", prominence: 0.05, lat: 45.5051, lon: -73.6051, geo_lat: 45.5051, geo_lon: -73.6051 },
  { id: "node/5", name: "Tim Hortons", search_name: "Tim Hortons", body: "fast food amenity coffee", category: "amenity", type: "fast_food", details: { opening_hours: "24/7", wheelchair: "yes", payment_contactless: "yes" }, lat: 45.502, lon: -73.6, geo_lat: 45.502, geo_lon: -73.6 },
  { id: "node/6", name: "Tim Hortons", search_name: "Tim Hortons", body: "fast food amenity coffee", category: "amenity", type: "fast_food", details: { opening_hours: "24/7", wheelchair: "yes", payment_contactless: "yes" }, lat: 45.56, lon: -73.6, geo_lat: 45.56, geo_lon: -73.6 },
  { id: "node/7", name: "Tim Hortons", search_name: "Tim Hortons", body: "fast food amenity coffee", category: "amenity", type: "fast_food", details: { opening_hours: "Mo-Su 00:00-24:00", wheelchair: "yes", payment_contactless: "yes" }, lat: 45.515, lon: -73.578, geo_lat: 45.515, geo_lon: -73.578 },
  { id: "node/8", name: "Pharmacie Centrale", search_name: "Pharmacie Centrale", body: "pharmacy amenity", category: "amenity", type: "pharmacy", house_number: "12", street: "Test Road", city: "Testville", postcode: "H2X 1Y4", lat: 45.5049, lon: -73.6049, geo_lat: 45.5049, geo_lon: -73.6049 },
  { id: "node/9", name: "12 Test Road", search_name: "12 Test Road", body: "address", category: "address", type: "civic_address", house_number: "12", street: "Test Road", city: "Testville", postcode: "H2X 1Y4", lat: 45.50491, lon: -73.60491, geo_lat: 45.50491, geo_lon: -73.60491 },
  { id: "node/10", name: "Test Road & Second Road", search_name: "Test Road & Second Road", body: "intersection highway", category: "highway", type: "intersection", city: "Testville", lat: 45.50505, lon: -73.60505, geo_lat: 45.50505, geo_lon: -73.60505 },
  { id: "node/11", name: "H2X 1Y4", search_name: "H2X 1Y4", body: "postal code", category: "place", type: "postal_code", postcode: "H2X 1Y4", lat: 45.504, lon: -73.604, geo_lat: 45.504, geo_lon: -73.604 },
  { id: "node/12", name: "Brunet", search_name: "Brunet", body: "village place", category: "place", type: "village", lat: 43.75, lon: 5.86, geo_lat: 43.75, geo_lon: 5.86 },
  { id: "node/13", name: "Garage Marcel Brunet", search_name: "Garage Marcel Brunet", body: "car repair shop", category: "shop", type: "car_repair", lat: 45.507, lon: -73.603, geo_lat: 45.507, geo_lon: -73.603 },
  { id: "node/14", name: "Bar Harbor", search_name: "Bar Harbor", body: "town place", category: "place", type: "town", population: 5000, lat: 44.39, lon: -68.2, geo_lat: 44.39, geo_lon: -68.2 },
  { id: "node/15", name: "Café Olimpico", search_name: "Café Olimpico", body: "cafe amenity", category: "amenity", type: "cafe", details: { opening_hours: "Mo-Fr 07:00-19:00", outdoor_seating: "yes", internet_access: "wlan" }, lat: 45.5065, lon: -73.6022, geo_lat: 45.5065, geo_lon: -73.6022 }
];

async function ensureTestville() {
  const root = join(out, "testville");
  const index = join(root, "public", "rangefind");
  if (existsSync(join(index, "manifest.json"))) return index;
  if (casesOnly) throw new Error(`missing ${index}; run without --cases`);
  mkdirSync(join(root, "data"), { recursive: true });
  writeFileSync(join(root, "data", "osm-places.jsonl"), TESTVILLE_DOCS.map(doc => JSON.stringify(doc)).join("\n"));
  await buildOsmIndex({
    root,
    region: "luxembourg",
    rqa: false,
    workerCount: 1,
    buildProgressLogMs: 0,
    config: { filterBitmapMaxFacetValues: 1 },
    runtimeBundlePath: join(root, "missing-runtime.js"),
    osmBundlePath: join(root, "missing-osm.js")
  });
  return index;
}

async function ensureTwoShards() {
  const root = join(out, "twoshards");
  const index = join(root, "public", "rangefind");
  if (existsSync(join(index, "manifest.json"))) return index;
  if (casesOnly) throw new Error(`missing ${index}; run without --cases`);
  mkdirSync(join(root, "data"), { recursive: true });
  const doc = (id, fields) => `${JSON.stringify({ id, ...fields })}\n`;
  writeFileSync(join(root, "data", "quebec.jsonl"),
    doc("node/1", { name: "Cinéma Beaubien", search_name: "Cinéma Beaubien", body: "cinema amenity", category: "amenity", type: "cinema", lat: 45.535, lon: -73.58, geo_lat: 45.535, geo_lon: -73.58 })
    + doc("node/2", { name: "Montréal", search_name: "Montréal", body: "city place", category: "place", type: "city", population: 1704694, lat: 45.5032, lon: -73.5698, geo_lat: 45.5032, geo_lon: -73.5698 })
    + doc("node/4", { name: "Laval", search_name: "Laval", body: "city place", category: "place", type: "city", population: 438366, lat: 45.57, lon: -73.69, geo_lat: 45.57, geo_lon: -73.69 })
    + doc("node/5", { name: "Cinéma Guzzo Laval", search_name: "Cinéma Guzzo Laval", body: "cinema amenity", category: "amenity", type: "cinema", city: "Laval", lat: 45.56, lon: -73.71, geo_lat: 45.56, geo_lon: -73.71 })
    + doc("node/6", { name: "Rue Sainte-Catherine", search_name: "Rue Sainte-Catherine", body: "primary highway", category: "highway", type: "primary", street: "Rue Sainte-Catherine", city: "Montréal", street_authority: "Rue Sainte-Catherine, Montréal", lat: 45.508, lon: -73.567, geo_lat: 45.508, geo_lon: -73.567 })
    + doc("node/7", { name: "Boulevard Saint-Laurent", search_name: "Boulevard Saint-Laurent", body: "primary highway", category: "highway", type: "primary", street: "Boulevard Saint-Laurent", city: "Montréal", street_authority: "Boulevard Saint-Laurent, Montréal", lat: 45.5085, lon: -73.5675, geo_lat: 45.5085, geo_lon: -73.5675 })
    + doc("node/8", { name: "Tim Hortons", search_name: "Tim Hortons", body: "fast food amenity coffee", category: "amenity", type: "fast_food", city: "Laval", lat: 45.571, lon: -73.692, geo_lat: 45.571, geo_lon: -73.692 }));
  writeFileSync(join(root, "data", "uganda.jsonl"),
    doc("node/3", { name: "Cinema", search_name: "Cinema", body: "village place", category: "place", type: "village", lat: -0.3408, lon: 31.739, geo_lat: -0.3408, geo_lon: 31.739 })
    + doc("node/9", { name: "Laval", search_name: "Laval", body: "hamlet place", category: "place", type: "hamlet", lat: -0.35, lon: 31.75, geo_lat: -0.35, geo_lon: 31.75 }));
  await buildOsmShardedIndex({
    output: index,
    workerCount: 1,
    buildProgressLogMs: 0,
    shards: [
      { id: "quebec", input: join(root, "data", "quebec.jsonl") },
      { id: "uganda", input: join(root, "data", "uganda.jsonl") }
    ]
  });
  return index;
}

// ---------------------------------------------------------------- corpus samples

function rng(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6D2B79F5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const random = rng(20261123);
const pick = items => items[Math.floor(random() * items.length)];
const round = (value, digits = 5) => Math.round(value * 10 ** digits) / 10 ** digits;

async function sampleCorpus(path) {
  const buckets = { places: [], streets: [], addresses: [], pois: [], postcodes: [] };
  const seen = { streets: 0, addresses: 0, pois: 0 };
  const reservoir = (bucket, item, limit) => {
    seen[bucket]++;
    if (buckets[bucket].length < limit) buckets[bucket].push(item);
    else {
      const slot = Math.floor(random() * seen[bucket]);
      if (slot < limit) buckets[bucket][slot] = item;
    }
  };
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line.trim()) continue;
    const doc = JSON.parse(line);
    const item = {
      name: doc.name, type: doc.type, category: doc.category, city: doc.city, street: doc.street,
      house_number: doc.house_number, postcode: doc.postcode, lat: doc.lat, lon: doc.lon
    };
    if (!Number.isFinite(item.lat) || !Number.isFinite(item.lon)) continue;
    if (doc.category === "place" && ["city", "town", "village", "hamlet", "suburb"].includes(doc.type) && doc.name) buckets.places.push(item);
    else if (doc.category === "highway" && doc.name && doc.city && ["residential", "primary", "secondary", "tertiary", "unclassified", "living_street"].includes(doc.type)) reservoir("streets", item, 2000);
    else if (doc.house_number && doc.street && doc.city) reservoir("addresses", item, 2000);
    else if (doc.name && ["amenity", "shop", "tourism", "leisure", "historic"].includes(doc.category)) reservoir("pois", item, 2000);
    if (doc.postcode && buckets.postcodes.length < 200 && !buckets.postcodes.includes(doc.postcode)) buckets.postcodes.push(doc.postcode);
  }
  return buckets;
}

// ---------------------------------------------------------------- case generation

const ANCHORS = {
  luxembourg: { lat: 49.6116, lon: 6.1319 },
  esch: { lat: 49.4958, lon: 5.9806 },
  ettelbruck: { lat: 49.8475, lon: 6.1043 },
  clervaux: { lat: 50.0547, lon: 6.0313 },
  remich: { lat: 49.5447, lon: 6.3669 },
  montreal: { lat: 45.5017, lon: -73.5673 },
  laval: { lat: 45.6066, lon: -73.7124 },
  tokyo: { lat: 35.6812, lon: 139.7671 }
};
const LUX_BOX = { minLat: 49.45, maxLat: 50.18, minLon: 5.74, maxLon: 6.52 };

const CATEGORY_WORDS = [
  "restaurant", "restaurants", "resto", "café", "cafe", "coffee", "bar", "pub", "pharmacy", "pharmacie", "pharmacies",
  "boulangerie", "bakery", "supermarket", "supermarché", "hotel", "hôtel", "museum", "musée", "cinema", "cinéma", "parking",
  "fuel", "gas station", "station essence", "bank", "banque", "atm", "school", "école", "church", "église", "park", "parc",
  "hospital", "hôpitaux", "playground", "toilets", "post office", "library", "fast food", "ice cream", "doctors", "dentist",
  "restaurnt", "pharmcy", "cinma", "hotle", "boulangeri"
];
const NEARBY_PHRASES = ["near me", "nearby", "around me", "close by", "pres de moi", "près de moi", "autour de moi", "à proximité", "proche"];
const CONNECTORS = ["in", "near", "à", "a", "de", "dans", "around", "pres", "du"];
const PLANET_QUERIES = [
  ["Laval", null], ["Park City", null], ["Bar Harbor", null], ["McGill University", "montreal"],
  ["parc larochelle repentigny", null], ["parc lafontaine montréal", null], ["parc larochelle toronto", null],
  ["Montréal Trudeau Airport", "montreal"], ["Berri-UQAM", "montreal"], ["cinema laval", null], ["restaurant montreal", null],
  ["cinéma laval", null], ["cinema new york", null], ["restaurant saint jean sur richelieu", null], ["gas station near me", "montreal"],
  ["restaurant", "montreal"], ["pharmacy", "montreal"], ["parking", "montreal"], ["Tim Hortons", "laval"], ["Tim Horton", "laval"],
  ["845 rue Sherbrooke Ouest Montréal", null], ["rue saint denis montreal", null], ["H2X 1Y4", null],
  ["boulevard Saint-Laurent and rue Sainte-Catherine Montréal", null], ["McGil University", "montreal"], ["cinma laval", null],
  ["東京駅", "tokyo"], ["Calgary Tower", null], ["Berlin Hauptbahnhof", null], ["hauptbanhof berlin", null], ["SW1A 1AA", null],
  ["350 5th Avenue New York", null], ["Hotel Plaza New York", null], ["garage brunet", "montreal"], ["Miami Beach", null],
  ["Market Harborough", null], ["pharmacy near me", null], ["cafés nearby", "luxembourg"], ["restaurants autour de moi", "esch"]
];

function viewportAround(anchor, span) {
  return { minLat: round(anchor.lat - span), maxLat: round(anchor.lat + span), minLon: round(anchor.lon - span * 1.5), maxLon: round(anchor.lon + span * 1.5) };
}

function randomPoint(box = LUX_BOX) {
  return { lat: round(box.minLat + random() * (box.maxLat - box.minLat)), lon: round(box.minLon + random() * (box.maxLon - box.minLon)) };
}

function encodePolyline(points, precision = 5) {
  const factor = 10 ** precision;
  let lastLat = 0;
  let lastLon = 0;
  let out = "";
  const encode = value => {
    let v = value < 0 ? ~(value << 1) : value << 1;
    let chunk = "";
    while (v >= 0x20) {
      chunk += String.fromCharCode((0x20 | (v & 0x1f)) + 63);
      v >>= 5;
    }
    return chunk + String.fromCharCode(v + 63);
  };
  for (const [lat, lon] of points) {
    const la = Math.round(lat * factor);
    const lo = Math.round(lon * factor);
    out += encode(la - lastLat) + encode(lo - lastLon);
    lastLat = la;
    lastLon = lo;
  }
  return out;
}

function luxCases(samples, { full = true } = {}) {
  const cases = [];
  const add = (fn, ...callArgs) => cases.push({ fn, args: callArgs });
  const search = params => add("searchOsmQuery", params);
  const places = samples.places;
  const majorPlaces = places.filter(place => ["city", "town"].includes(place.type));
  const scale = full ? 1 : 0.3;
  const n = count => Math.max(1, Math.round(count * scale));
  const anchorNames = Object.keys(ANCHORS).filter(name => !["montreal", "laval", "tokyo"].includes(name));

  // Coordinates and reverse geocoding.
  for (let i = 0; i < n(50); i++) {
    const point = randomPoint();
    search({ q: i % 2 ? `${point.lat}, ${point.lon}` : `${point.lat} ${point.lon}`, size: 5 });
  }
  for (let i = 0; i < n(8); i++) {
    const point = randomPoint({ minLat: 48.5, maxLat: 49.3, minLon: 4, maxLon: 5.5 });
    search({ q: `${point.lat},${point.lon}`, size: 5 });
  }
  search({ q: "45.5019, -73.5674", reverseGeocode: false, size: 18 });
  search({ q: "49.6116, 6.1319", reverseGeocode: { radiusMeters: 250, size: 3 } });
  search({ q: "49.6116 6.1319", reverseGeocode: { resultTypes: ["locality"] } });
  search({ q: "91, 6", size: 5 });
  search({ q: "+49.5 +6.1", size: 5 });
  search({ q: ".5, -.5", size: 5 });
  const reverseVariants = [
    {}, { radiusMeters: 100 }, { radiusMeters: 1500, size: 3 }, { size: 40 }, { size: 0 }, { resultTypes: "street_address" },
    { resultType: "route" }, { resultTypes: ["locality", "village"] }, { locationTypes: ["ROOFTOP"] }, { locationType: "approximate" },
    { radiusMeters: 50, localityRadiusMeters: 8000 }, { radiusMeters: 20, resultTypes: ["town"] }, { shards: ["nowhere"] },
    { filters: { facets: { type: ["address"] } } }, { page: 3, q: "ignored", near: ANCHORS.luxembourg, trace: true }
  ];
  for (let i = 0; i < n(70); i++) {
    const point = i % 7 === 6 ? randomPoint({ minLat: 49.3, maxLat: 50.3, minLon: 5.5, maxLon: 6.8 }) : randomPoint();
    add("reverseGeocodeOsm", { ...point, ...reverseVariants[i % reverseVariants.length] });
  }
  add("reverseGeocodeOsm", { coordinate: { lat: 49.61, lon: 6.13 }, size: 2 });
  add("reverseGeocodeOsm", { lat: 95, lon: 6 });
  add("reverseGeocodeOsm", { lat: 49.6, lon: 6.1, radiusMeters: -1 });
  add("reverseGeocodeOsm", { lat: 49.6, lon: 6.1, localityRadiusMeters: "x" });
  add("reverseGeocodeOsm", {});

  // Bare place names: unanchored, anchored, viewport.
  for (const place of places.slice(0, n(140))) search({ q: place.name, size: 10 });
  for (let i = 0; i < n(50); i++) {
    const place = pick(places);
    search({ q: place.name, size: 8, near: ANCHORS[pick(anchorNames)] });
  }
  for (let i = 0; i < n(20); i++) {
    const place = pick(places);
    search({ q: place.name.toLowerCase(), size: 8, geo: { box: viewportAround(ANCHORS[pick(anchorNames)], 0.05) } });
  }

  // Category + locality.
  for (let i = 0; i < n(170); i++) {
    const category = pick(CATEGORY_WORDS);
    const place = pick(i % 3 ? majorPlaces : places);
    const form = i % 5;
    const q = form === 0 ? `${category} ${place.name}`
      : form === 1 ? `${place.name} ${category}`
        : form === 2 ? `${category} ${pick(CONNECTORS)} ${place.name}`
          : form === 3 ? `${category} ${place.name.toLowerCase()}`
            : `${category} ${pick(["Hotel", "Café", "Le", "Am"])} ${place.name}`;
    const anchor = i % 4 === 0 ? { near: ANCHORS[pick(Object.keys(ANCHORS))] } : {};
    search({ q, size: 10, ...anchor });
  }

  // Nearby categories.
  for (let i = 0; i < n(60); i++) {
    const category = pick(CATEGORY_WORDS);
    const q = i % 2 ? `${category} ${pick(NEARBY_PHRASES)}` : category;
    const anchor = ANCHORS[pick(Object.keys(ANCHORS))];
    search({ q, size: 10, near: i % 9 === 0 ? randomPoint() : anchor });
  }
  for (let i = 0; i < n(20); i++) search({ q: `${pick(CATEGORY_WORDS)} ${pick(NEARBY_PHRASES)}`, size: 6 });
  for (let i = 0; i < n(20); i++) {
    search({ q: pick(CATEGORY_WORDS), size: 10, geo: { box: viewportAround(ANCHORS[pick(anchorNames)], pick([0.01, 0.05, 0.2])) } });
  }

  // Streets and civic addresses with a locality.
  for (let i = 0; i < n(110); i++) {
    const street = pick(samples.streets);
    const form = i % 4;
    search({
      q: form === 0 ? `${street.name} ${street.city}` : form === 1 ? `${street.name}, ${street.city}`
        : form === 2 ? `${street.name.toLowerCase()} ${street.city.toLowerCase()}` : `${street.name} ${pick(places).name}`,
      size: 10
    });
  }
  for (let i = 0; i < n(80); i++) {
    const address = pick(samples.addresses);
    const form = i % 3;
    search({
      q: form === 0 ? `${address.house_number} ${address.street} ${address.city}`
        : form === 1 ? `${address.house_number} ${address.street}, ${address.city}`
          : `${address.street} ${address.house_number}, ${address.postcode || ""} ${address.city}`,
      size: 10
    });
  }

  // Intersections.
  for (let i = 0; i < n(60); i++) {
    const left = pick(samples.streets);
    const sameCity = samples.streets.filter(street => street.city === left.city && street.name !== left.name);
    const right = sameCity.length && i % 4 ? pick(sameCity) : pick(samples.streets);
    const connector = pick([" & ", " and ", " at ", " et ", " x ", "/", " / ", ", and "]);
    search({ q: `${left.name}${connector}${right.name} ${left.city}`, size: 10, ...(i % 5 === 0 ? { near: ANCHORS.luxembourg } : {}) });
  }

  // Named places (POIs).
  for (let i = 0; i < n(120); i++) search({ q: pick(samples.pois).name, size: 10 });
  for (let i = 0; i < n(80); i++) {
    const poi = pick(samples.pois);
    const offset = i % 2 ? 0.02 : 0.2;
    search({ q: poi.name, size: 10, near: { lat: round(poi.lat + offset), lon: round(poi.lon - offset) } });
  }
  for (let i = 0; i < n(40); i++) {
    const poi = pick(samples.pois.filter(item => item.city));
    search({ q: i % 2 ? `${poi.name} ${poi.city}` : `${poi.city} ${poi.name}`, size: 10 });
  }
  for (let i = 0; i < n(30); i++) {
    const poi = pick(samples.pois);
    search({ q: poi.name, size: 10, geo: { box: viewportAround({ lat: poi.lat, lon: poi.lon }, 0.03) } });
  }

  // Postal codes.
  for (let i = 0; i < n(20); i++) {
    const code = pick(samples.postcodes);
    search({ q: i % 2 ? code : `restaurant ${code}`, size: 10 });
  }
  search({ q: "L-1234", size: 10 });
  search({ q: "H2X 1Y4", size: 10 });
  search({ q: "h2x1y4", size: 10 });

  // Planet-scale benchmark wording (mostly misses on Luxembourg).
  for (const [q, anchor] of PLANET_QUERIES) search({ q, size: 18, ...(anchor ? { near: ANCHORS[anchor] } : {}) });

  // Edge inputs.
  for (const q of ["", "   ", "a", "é", "12", "rue", "near me", "nearby", "Rue", "rue de", "😀 café", "東京", "a b c d e f g h i j",
    "restaurant", "Luxembourg Luxembourg", "the", "1 2 3", "Rue , Luxembourg", ", ,", "&", "/", "x"]) search({ q, size: 5 });
  search({ q: "Luxembourg", size: "7" });
  search({ q: "Esch", limit: 3 });
  search({ q: "Esch", size: 500 });
  search({ q: "Esch", shards: ["anything"] });
  search({ q: "restaurant Esch", page: 2 });
  search({ q: "restaurant", near: { lat: "49.61", lon: "6.13" } });
  search({ q: "restaurant", near: { lat: "x", lon: 6 } });

  // Traces (merged runtime traces are normalized in the live comparison).
  for (let i = 0; i < n(25); i++) {
    const q = i % 3 === 0 ? pick(places).name : i % 3 === 1 ? `${pick(CATEGORY_WORDS)} ${pick(majorPlaces).name}` : `${pick(samples.streets).name} ${pick(places).name}`;
    search({ q, size: 5, trace: true, ...(i % 4 === 0 ? { near: ANCHORS.esch } : {}) });
  }

  // Constraints.
  const constraintQueries = [
    "restaurant open now", "cafe with wifi", "wheelchair accessible restaurant", "takeaway pizza", "restaurant with outdoor seating",
    "free museum", "pharmacie ouverte maintenant", "restaurant avec terrasse", "bar with contactless", "drive-through", "patio",
    "restaurant open now with takeaway", "restaurant luxembourg open now", "wheelchair-accessible toilets", "cafe open now near me"
  ];
  for (let i = 0; i < n(40); i++) {
    const q = pick(constraintQueries);
    search({
      q, size: 10, at: "2026-08-03T15:00:00Z", timeZone: i % 2 ? "Europe/Luxembourg" : "UTC",
      ...(i % 3 === 0 ? { near: ANCHORS.luxembourg } : {}), ...(i % 5 === 0 ? { includeUnknownOpenNow: true } : {})
    });
  }
  search({ q: "restaurant", constraints: { openNow: true, wheelchair: true }, at: "2026-08-03T11:00:00Z", timeZone: "UTC", near: ANCHORS.luxembourg });
  search({ query: "cafe", constraints: { internet: true }, near: ANCHORS.esch, limit: 4 });

  // Route corridors.
  const routes = [
    [ANCHORS.luxembourg, ANCHORS.esch], [ANCHORS.luxembourg, ANCHORS.ettelbruck], [ANCHORS.ettelbruck, ANCHORS.clervaux],
    [ANCHORS.luxembourg, ANCHORS.remich], [ANCHORS.esch, ANCHORS.remich]
  ];
  const routeQueries = ["restaurant", "fuel", "cafe", "pharmacy", "Shell", "parking", "open now restaurant", "bakery", "toilets"];
  for (let i = 0; i < n(40); i++) {
    const [from, to] = pick(routes);
    const mid = { lat: (from.lat + to.lat) / 2 + 0.01, lon: (from.lon + to.lon) / 2 };
    const coordinates = [[from.lon, from.lat], [mid.lon, mid.lat], [to.lon, to.lat]];
    const route = i % 2
      ? { type: "LineString", coordinates }
      : encodePolyline(coordinates.map(([lon, lat]) => [lat, lon]));
    add(i % 4 === 0 ? "searchOsmQuery" : "searchAlongRouteOsm", {
      route,
      query: pick(routeQueries),
      corridorMeters: pick([500, 1500, 3000, undefined]),
      limit: pick([5, 10, 20]),
      ...(i % 3 === 0 ? { routeDirection: "reverse" } : {}),
      at: "2026-08-03T15:00:00Z",
      timeZone: "UTC"
    });
  }
  add("searchAlongRouteOsm", { query: "fuel" });

  // Suggestions.
  const prefixes = [];
  for (const source of [samples.places, samples.streets, samples.pois]) {
    for (let i = 0; i < n(50); i++) {
      const name = pick(source).name;
      prefixes.push(name.slice(0, 1 + Math.floor(random() * Math.min(10, name.length))));
    }
  }
  for (let i = 0; i < n(40); i++) {
    const address = pick(samples.addresses);
    prefixes.push(`${address.house_number} ${address.street.slice(0, 3 + Math.floor(random() * 6))}`);
  }
  for (let i = 0; i < n(40); i++) prefixes.push(`${pick(CATEGORY_WORDS)} ${pick(majorPlaces).name.slice(0, 2 + Math.floor(random() * 4))}`);
  for (const [i, q] of prefixes.entries()) {
    const variant = i % 6;
    add("suggestOsmQuery", {
      q,
      size: pick([5, 8, 12]),
      ...(variant === 1 ? { near: ANCHORS[pick(anchorNames)] } : {}),
      ...(variant === 2 ? { hydrate: true } : {}),
      ...(variant === 3 ? { geo: { box: viewportAround(ANCHORS.luxembourg, 0.05) } } : {}),
      ...(variant === 4 ? { inputOffset: Math.max(1, q.length - 2) } : {})
    });
  }
  for (const q of ["rue de la", "rue de la gare", "route d", "avenue", "12 rue", "mon", "lux", "esch", "", "  "]) {
    add("suggestOsmQuery", { q, size: 8 });
    add("suggestOsmQuery", { q, size: 8, hydrate: true });
  }
  add("suggestOsmQuery", { q: "luxembxurg", inputOffset: 5, size: 8 });
  add("suggestOsmQuery", { q: "esch", inputOffset: "x" });
  add("suggestOsmQuery", { q: "esch", size: "abc" });
  return cases;
}

function mapsCases(samples) {
  const cases = [];
  const add = (method, request) => cases.push({ fn: `maps.${method}`, args: [request] });
  const circle = (anchor, radius) => ({ circle: { center: { latitude: anchor.lat, longitude: anchor.lon }, radius } });
  for (let i = 0; i < 12; i++) {
    const place = pick(samples.places);
    add("autocomplete", { input: place.name.slice(0, 3 + (i % 4)), ...(i % 2 ? { locationBias: circle(ANCHORS.luxembourg, 5000) } : {}) });
    add("textSearch", { textQuery: i % 3 ? `${pick(CATEGORY_WORDS)} ${place.name}` : place.name, maxResultCount: 5, ...(i % 2 ? { locationBias: circle(ANCHORS.esch, 3000) } : {}) });
    add("nearbySearch", { includedTypes: [pick(["restaurant", "cafe", "pharmacy", "gas_station", "coffee_shop", "bakery"])], maxResultCount: 5, locationRestriction: circle(ANCHORS[pick(["luxembourg", "esch", "ettelbruck"])], pick([500, 2000, 10000])), ...(i % 3 === 0 ? { rankPreference: "POPULARITY" } : {}) });
    const address = pick(samples.addresses);
    add("geocode", { address: `${address.house_number} ${address.street}, ${address.city}` });
    const point = randomPoint();
    add("reverseGeocode", { location: { latitude: point.lat, longitude: point.lon }, maxResultCount: 3 });
  }
  add("textSearch", { textQuery: "restaurant", includedType: "restaurant", openNow: true, timeZone: "UTC", locationBias: circle(ANCHORS.luxembourg, 2000) });
  add("searchAlongRoute", { route: { type: "LineString", coordinates: [[ANCHORS.luxembourg.lon, ANCHORS.luxembourg.lat], [ANCHORS.esch.lon, ANCHORS.esch.lat]] }, query: "fuel", corridorMeters: 1000 });
  add("placeDetails", { placeId: "node/141612" });
  return cases;
}

function smallCases(kind) {
  const cases = [];
  const search = params => cases.push({ fn: "searchOsmQuery", args: [params] });
  const suggest = params => cases.push({ fn: "suggestOsmQuery", args: [params] });
  const near = { lat: 45.5, lon: -73.57 };
  const queries = kind === "testville"
    ? ["Testville", "cinema Testville", "Testville cinema", "Test Road & Second Road Testville", "Test Road and Second Road, Testville",
      "Test Road Testville", "12 Test Road Testville", "12 Test Road, Testville", "H2X 1Y4", "pharmacy H2X 1Y4", "Tim Hortons",
      "Tim Horton", "Picture House", "garage brunet", "Bar Harbor", "bar harbor", "pharmacie", "cafe", "café open now",
      "Testville Cinema", "Pharmacie Centrale", "Second Road / Test Road Testville", "Brunet", "cinema near me", "restaurants nearby"]
    : ["cinema", "cinéma près de moi", "cinema laval", "cinéma laval", "Laval", "Montréal", "Cinema", "Tim Hortons",
      "Rue Sainte-Catherine Montréal", "boulevard Saint-Laurent and rue Sainte-Catherine Montréal", "cinema montreal",
      "Cinéma Beaubien", "laval cinema", "fast food laval", "cinema in laval", "Guzzo Laval"];
  for (const q of queries) {
    search({ q, size: 5 });
    search({ q, size: 5, near });
    search({ q, size: 5, near: { lat: 45.56, lon: -73.7 } });
    search({ q, size: 5, geo: { box: { minLat: 45.45, maxLat: 45.6, minLon: -73.75, maxLon: -73.5 } } });
    if (kind !== "testville") search({ q, size: 5, near: { lat: -0.34, lon: 31.74 } });
  }
  for (const q of ["Test", "tim", "cin", "pict", "12 te", "cinema tes", "lav", "mon", "Rue", "cinéma la"]) {
    suggest({ q, size: 8 });
    suggest({ q, size: 8, near, hydrate: true });
  }
  for (const point of [{ lat: 45.505, lon: -73.605 }, { lat: 45.6, lon: -73.6 }, { lat: -0.34, lon: 31.74 }, { lat: 10, lon: 10 }]) {
    cases.push({ fn: "reverseGeocodeOsm", args: [point] });
    cases.push({ fn: "reverseGeocodeOsm", args: [{ ...point, radiusMeters: 50000, resultTypes: ["locality"] }] });
    search({ q: `${point.lat}, ${point.lon}`, size: 5 });
  }
  cases.push({
    fn: "searchAlongRouteOsm",
    args: [{
      route: { type: "LineString", coordinates: [[-73.62, 45.49], [-73.57, 45.52]] },
      query: "wheelchair-accessible Tim Hortons open now with contactless",
      corridorMeters: 1000, limit: 20, at: "2026-08-03T15:00:00Z", timeZone: "UTC", trace: true
    }]
  });
  cases.push({ fn: "searchAlongRouteOsm", args: [{ route: { type: "LineString", coordinates: [[-73.62, 45.49], [-73.57, 45.52]] }, query: "fast food" }] });
  return cases;
}

function pureCases(samples) {
  const cases = [];
  const strings = [];
  for (let i = 0; i < 150; i++) {
    const place = pick(samples.places);
    const category = pick(CATEGORY_WORDS);
    strings.push(pick([
      `${category} ${place.name}`, `${place.name} ${category}`, `${category} ${pick(CONNECTORS)} ${place.name}`,
      `${category} ${pick(NEARBY_PHRASES)}`, place.name, `${category}`, `${category} ${pick(CONNECTORS)}`,
      `${pick(CONNECTORS)} ${place.name}`, `${round(49 + random(), 4)}, ${round(6 + random(), 4)}`
    ]));
  }
  strings.push("", " ", "45.5019, -73.5674", "45.5019 -73.5674", "91, -73", "45.5, -181", "845 Sherbrooke", "1.5,2.5",
    "-0.0 , +0.", ".5 .5", "1 2 3", "4551", "  45  ,  -73  ", "45,-73,", "cinema near me", "near me", "café nearby",
    "restaurants autour de moi", "pharmacie près de moi", "cinma laval", "Quebec City", "city hall montreal", "hôpitaux paris",
    "parcs lyon", "gas station near me", "a proximite", "coffee shop", "Park City", "McGill University");
  for (const value of strings) {
    cases.push({ fn: "parseCoordinateIntent", args: [value] });
    cases.push({ fn: "parseOsmQueryIntent", args: [value] });
    cases.push({ fn: "parseNearbyCategoryIntent", args: [value] });
  }
  return cases;
}

// ---------------------------------------------------------------- recording

function groupRecords(records, start) {
  return records.slice(start);
}

async function runCases(name, indexPath, cases) {
  const recorder = createRecorder();
  const wrapped = Object.fromEntries([
    ...ENGINE_FUNCTIONS.map(fn => [fn, recorder.wrapEngineFunction(fn, query[fn])]),
    ...PURE_FUNCTIONS.map(fn => [fn, recorder.wrapPureFunction(fn, query[fn])])
  ]);
  const outCases = [];
  const started = performance.now();
  const record = async (fn, args, engine) => {
    const before = recorder.records.length;
    try {
      if (PURE_FUNCTIONS.includes(fn)) wrapped[fn](...args);
      else await wrapped[fn](engine, ...args);
    } catch {
      // recorded
    }
    const [entry] = groupRecords(recorder.records, before);
    return entry;
  };
  const pending = [...cases];
  const runMaps = async (fn, args, adapter) => {
    try {
      return { output: JSON.parse(JSON.stringify(await adapter[fn.slice(5)](...args)) ?? "null") };
    } catch (error) {
      return { error: { message: String(error?.message ?? error) } };
    }
  };
  const suggestionsWithDocs = [];
  for (let index = 0; index < pending.length; index++) {
    const { fn, args } = pending[index];
    if (PURE_FUNCTIONS.includes(fn)) {
      const entry = await record(fn, args, null);
      outCases.push({ id: outCases.length, fn, args: entry.args, cold: { output: entry.output ?? null, error: entry.error } });
      continue;
    }
    const engine = await createNodeSearch({ source: indexPath });
    if (fn.startsWith("maps.")) {
      const adapter = createRangefindMapsAdapter(engine);
      const coldMaps = await runMaps(fn, args, adapter);
      const warmMaps = await runMaps(fn, args, adapter);
      outCases.push({ id: outCases.length, fn, args, cold: { ...coldMaps, calls: [] }, warm: { ...warmMaps, calls: [] } });
      continue;
    }
    const cold = await record(fn, args, engine);
    const warm = await record(fn, args, engine);
    const strip = entry => ({
      ...(entry.error ? { error: entry.error } : { output: entry.output ?? null }),
      calls: entry.calls,
      engine: entry.engine
    });
    outCases.push({ id: outCases.length, fn, args: cold.args, cold: strip(cold), warm: strip(warm) });
    if (fn === "suggestOsmQuery" && cold.output?.suggestions) {
      for (const item of cold.output.suggestions) {
        if (item?.selection?.doc && suggestionsWithDocs.length < 120) suggestionsWithDocs.push(item);
      }
    }
    if (index === pending.length - 1 && suggestionsWithDocs.length) {
      // Second pass: resolve recorded entity suggestions, then exercise the
      // pure street collapser on raw engine suggest responses.
      for (const suggestion of suggestionsWithDocs.splice(0, 60)) pending.push({ fn: "resolveOsmSuggestion", args: [suggestion] });
      pending.push({ fn: "resolveOsmSuggestion", args: [{ selection: { query: "x" } }] });
      pending.push({ fn: "resolveOsmSuggestion", args: [{ selection: { doc: { index: -1 } } }] });
      pending.push({ fn: "hydrateOsmSuggestions", args: [outCases.filter(item => item.fn === "suggestOsmQuery").slice(0, 1).flatMap(item => (item.cold.output?.suggestions || []).map(({ result, ...rest }) => rest))] });
      for (const item of outCases.filter(entry => entry.fn === "suggestOsmQuery").slice(0, 120)) {
        for (const call of item.cold.calls.filter(call => call.method === "suggest" && call.response)) {
          pending.push({ fn: "collapseStreetSuggestions", args: [call.response, { q: item.args[0]?.q, size: item.args[0]?.size }] });
        }
      }
    }
    if ((index + 1) % 100 === 0) log(`${name}: ${index + 1}/${pending.length} cases (${((performance.now() - started) / 1000).toFixed(1)}s)`);
  }
  const file = join(out, "cases", `${name}.json.gz`);
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, gzipSync(JSON.stringify({ name, index: relative(out, indexPath), cases: outCases })));
  log(`${name}: wrote ${outCases.length} cases to ${relative(repo, file)} (${(statSync(file).size / 1e6).toFixed(1)} MB, ${((performance.now() - started) / 1000).toFixed(1)}s)`);
}

function recordUnitTests() {
  mkdirSync(join(out, "replay"), { recursive: true });
  for (const file of ["osm_query", "osm_integration", "osm_constraints"]) {
    const target = join(out, "replay", `${file}.json.gz`);
    rmSync(target, { force: true });
    try {
      execFileSync(process.execPath, ["--import", pathToFileURL(join(here, "osm_record_hooks.mjs")).href, join(repo, "test", `${file}.test.js`)], {
        cwd: repo,
        env: { ...process.env, OSM_RECORD_OUT: target },
        stdio: "ignore"
      });
    } catch {
      // A failing JS test still records its calls.
    }
    log(`replay: recorded test/${file}.test.js`);
  }
}

// ---------------------------------------------------------------- main

const wants = name => !only || only.includes(name);
if (wants("replay")) recordUnitTests();
// The Luxembourg indexes need the local extract (examples/osm-geo/data);
// CI runs only the unit-test replay and the synthetic indexes.
if (wants("lux") || wants("luxsharded")) {
  const lux = ensureLuxembourg();
  const samples = await sampleCorpus(join(out, "lux", "data", "osm-places.jsonl"));
  log(`corpus samples: ${Object.entries(samples).map(([key, items]) => `${key}=${items.length}`).join(", ")}`);
  if (wants("lux")) await runCases("lux", lux, [...luxCases(samples), ...mapsCases(samples), ...pureCases(samples)]);
  if (wants("luxsharded")) {
    const sharded = await ensureLuxSharded();
    await runCases("luxsharded", sharded, luxCases(samples, { full: false }));
  }
}
if (wants("testville")) await runCases("testville", await ensureTestville(), smallCases("testville"));
if (wants("twoshards")) await runCases("twoshards", await ensureTwoShards(), smallCases("twoshards"));
