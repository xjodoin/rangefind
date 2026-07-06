#!/usr/bin/env node

// OpenStreetMap geo fixture for Rangefind.
//
// Converts a Geofabrik-style .osm.pbf extract into a normalized place JSONL
// corpus, builds a geo-enabled Rangefind index from it, and serves it for
// benchmarking. Nodes carry their own coordinates; tagged ways (buildings,
// parks, shops mapped as areas) are anchored at their first node coordinate,
// which is close enough for search-and-rank benchmarking without resolving
// full geometry.
//
// Usage:
//   node scripts/osm_fixture.mjs jsonl --pbf=examples/osm-geo/data/luxembourg-latest.osm.pbf
//   node scripts/osm_fixture.mjs build
//   node scripts/osm_fixture.mjs all --region=luxembourg
//   node scripts/osm_fixture.mjs all --region=quebec

import { createWriteStream, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { availableParallelism } from "node:os";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { scanPbf } from "./osm_pbf.mjs";
import { build } from "../src/builder.js";

const REGIONS = {
  luxembourg: "https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf",
  quebec: "https://download.geofabrik.de/north-america/canada/quebec-latest.osm.pbf",
  switzerland: "https://download.geofabrik.de/europe/switzerland-latest.osm.pbf"
};

// Primary OSM keys that make a feature a searchable place, in ranking order.
const CATEGORY_KEYS = [
  "place",
  "amenity",
  "shop",
  "tourism",
  "leisure",
  "historic",
  "healthcare",
  "office",
  "craft",
  "man_made",
  "natural",
  "railway",
  "aeroway",
  "emergency",
  "sport"
];

// Values that describe map plumbing rather than searchable places.
const NOISE_TYPES = new Set([
  "yes",
  "no",
  "tree",
  "rock",
  "stone",
  "survey_point",
  "switch",
  "signal",
  "milestone",
  "level_crossing",
  "crossing",
  "buffer_stop",
  "fire_hydrant",
  "street_lamp",
  "waste_basket",
  "grit_bin",
  "parking_space",
  "tree_row"
]);

const ALIAS_KEYS = [
  "alt_name",
  "old_name",
  "short_name",
  "official_name",
  "loc_name",
  "brand",
  "operator",
  "ref"
];

function parseArgs(argv) {
  const args = {
    command: argv[0] || "all",
    region: "luxembourg",
    pbf: "",
    root: "examples/osm-geo",
    limit: 0,
    force: false,
    buildProgressLogMs: Number(process.env.OSM_BUILD_PROGRESS_MS || 15000)
  };
  for (const arg of argv.slice(1)) {
    if (arg.startsWith("--region=")) args.region = arg.slice("--region=".length);
    else if (arg.startsWith("--pbf=")) args.pbf = arg.slice("--pbf=".length);
    else if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--limit=")) args.limit = Number(arg.slice("--limit=".length)) || 0;
    else if (arg === "--force") args.force = true;
  }
  if (!args.pbf) args.pbf = `${args.root}/data/${args.region}-latest.osm.pbf`;
  return args;
}

function ensurePbf(args) {
  if (existsSync(args.pbf)) return;
  const url = REGIONS[args.region];
  if (!url) throw new Error(`No PBF at ${args.pbf} and unknown region "${args.region}".`);
  mkdirSync(resolve(args.root, "data"), { recursive: true });
  console.log(`[osm] downloading ${url}`);
  execFileSync("curl", ["-sSL", "-o", args.pbf, url], { stdio: "inherit" });
}

function firstCategory(tags) {
  for (const key of CATEGORY_KEYS) {
    const value = tags.get(key);
    if (value && !NOISE_TYPES.has(value)) return { category: key, type: value };
  }
  return null;
}

function collectAliases(tags, name) {
  const aliases = [];
  for (const [key, rawValue] of tags) {
    const value = cleanText(rawValue);
    if (!value || value === name) continue;
    if (ALIAS_KEYS.includes(key) || key.startsWith("name:")) {
      if (!aliases.includes(value) && aliases.length < 8) aliases.push(value);
    }
  }
  return aliases;
}

function typeLabel(type) {
  return String(type || "").replaceAll(/[_;]+/gu, " ").trim();
}

// OSM tag values can contain raw newlines and U+2028/U+2029 line separators,
// which JSON.stringify leaves unescaped and line-based JSONL readers split on.
function cleanText(value) {
  return String(value || "").replaceAll(/[\u0000-\u001f\u0085\u2028\u2029]+/gu, " ").trim();
}

function placeDoc(osmType, osmId, lat, lon, tags) {
  const name = cleanText(tags.get("name"));
  const category = firstCategory(tags);
  if (!name && !category) return null;
  const label = typeLabel(category?.type);
  const aliases = name ? collectAliases(tags, name) : [];
  const street = tags.get("addr:street") || "";
  const housenumber = tags.get("addr:housenumber") || "";
  const city = tags.get("addr:city") || "";
  const addressParts = [housenumber && street ? `${housenumber} ${street}` : street, city].filter(Boolean);
  const cuisine = typeLabel(tags.get("cuisine"));
  const bodyParts = [label, category ? category.category : "", cuisine, ...addressParts];
  const doc = {
    id: `${osmType}/${osmId}`,
    url: `https://www.openstreetmap.org/${osmType}/${osmId}`,
    name: name || label,
    body: bodyParts.filter(Boolean).join(" "),
    lat: Number(lat.toFixed(7)),
    lon: Number(lon.toFixed(7))
  };
  if (aliases.length) doc.aliases = aliases;
  if (category) {
    doc.category = category.category;
    doc.type = category.type;
  }
  if (tags.get("place")) {
    const population = Number(tags.get("population"));
    if (Number.isFinite(population) && population > 0) doc.population = population;
  }
  return doc;
}

async function writeJsonl(args) {
  const outPath = resolve(args.root, "data", "osm-places.jsonl");
  const metaPath = resolve(args.root, "data", "osm-places.meta.json");
  if (!args.force && existsSync(outPath) && existsSync(metaPath)) {
    const meta = JSON.parse(readFileSync(metaPath, "utf8"));
    if (meta.pbf === args.pbf && meta.limit === args.limit) {
      console.log(`[osm] reusing ${outPath} (${meta.docs} docs)`);
      return outPath;
    }
  }
  ensurePbf(args);
  const t0 = performance.now();
  console.log(`[osm] pass 1/2: scanning ways in ${args.pbf}`);
  // Tagged ways keep only their first node ref; pass 2 resolves it to coords.
  const wayAnchors = new Map();
  const ways = [];
  scanPbf(args.pbf, {
    onWay(id, refs, tags) {
      if (!refs.length) return;
      if (!tags.get("name") && !firstCategory(tags)) return;
      ways.push({ id, ref: refs[0], tags });
      wayAnchors.set(refs[0], -1);
    }
  });
  console.log(`[osm] pass 2/2: scanning nodes (${ways.length} candidate ways)`);
  const stream = createWriteStream(outPath);
  let docs = 0;
  let truncated = false;
  const writeDoc = doc => {
    if (!doc) return false;
    if (args.limit && docs >= args.limit) {
      truncated = true;
      return true;
    }
    stream.write(`${JSON.stringify(doc)}\n`);
    docs += 1;
    return false;
  };
  const anchorLat = new Map();
  const anchorLon = new Map();
  scanPbf(args.pbf, {
    onNode(id, lat, lon, tags) {
      if (wayAnchors.has(id)) {
        anchorLat.set(id, lat);
        anchorLon.set(id, lon);
      }
      if (!tags || truncated) return;
      writeDoc(placeDoc("node", id, lat, lon, tags));
    }
  });
  for (const way of ways) {
    if (truncated) break;
    const lat = anchorLat.get(way.ref);
    const lon = anchorLon.get(way.ref);
    if (lat === undefined || lon === undefined) continue;
    writeDoc(placeDoc("way", way.id, lat, lon, way.tags));
  }
  await new Promise((resolvePromise, reject) => {
    stream.end(err => (err ? reject(err) : resolvePromise()));
  });
  const meta = {
    pbf: args.pbf,
    limit: args.limit,
    docs,
    ways: ways.length,
    seconds: Math.round((performance.now() - t0) / 100) / 10
  };
  writeFileSync(metaPath, JSON.stringify(meta, null, 2));
  console.log(`[osm] wrote ${docs} docs to ${outPath} in ${meta.seconds}s`);
  return outPath;
}

function writeSite(args) {
  const configPath = resolve(args.root, "rangefind.config.json");
  const config = {
    input: "data/osm-places.jsonl",
    output: "public/rangefind",
    scanWorkers: Math.max(1, availableParallelism() - 2),
    builderWorkerCount: Math.max(1, availableParallelism() - 2),
    fields: [
      { name: "title", path: "name", weight: 6.0, b: 0.4, phrase: true },
      { name: "aliases", path: "aliases", weight: 4.0, b: 0.5 },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: [
      { name: "category", path: "category" },
      { name: "type", path: "type" }
    ],
    numbers: [
      { name: "population", path: "population", type: "int" }
    ],
    geo: [
      { name: "location", latPath: "lat", lonPath: "lon" }
    ],
    suggest: [
      { path: "name", weightPath: "population" },
      { path: "aliases" }
    ],
    display: ["name", "url", "category", "type", "lat", "lon"],
    buildProgressLogMs: args.buildProgressLogMs
  };
  mkdirSync(resolve(args.root, "public"), { recursive: true });
  writeFileSync(configPath, JSON.stringify(config, null, 2));
  return configPath;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (!["jsonl", "build", "all"].includes(args.command)) {
    throw new Error(`Unknown command "${args.command}" (expected jsonl, build, or all).`);
  }
  if (args.command === "jsonl" || args.command === "all") await writeJsonl(args);
  if (args.command === "build" || args.command === "all") {
    const configPath = writeSite(args);
    const t0 = performance.now();
    await build({ configPath });
    console.log(`[osm] build finished in ${Math.round((performance.now() - t0) / 100) / 10}s`);
  }
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
