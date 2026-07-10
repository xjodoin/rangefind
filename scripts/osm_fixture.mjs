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

import {
  closeSync,
  copyFileSync,
  createReadStream,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
  writeSync
} from "node:fs";
import { execFileSync } from "node:child_process";
import { availableParallelism } from "node:os";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createInterface } from "node:readline";
import { scanPbf } from "./osm_pbf.mjs";
import {
  coordinateStoreExists,
  createAnchorRefWriter,
  createCoordinateStore,
  openSortedAnchorRefs,
  sortUniqueAnchorRefs
} from "./osm_anchor_store.mjs";
import { build } from "../src/builder.js";

const REGIONS = {
  luxembourg: "https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf",
  quebec: "https://download.geofabrik.de/north-america/canada/quebec-latest.osm.pbf",
  switzerland: "https://download.geofabrik.de/europe/switzerland-latest.osm.pbf",
  us: "https://download.geofabrik.de/north-america/us-latest.osm.pbf"
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
  let rootExplicit = false;
  for (const arg of argv.slice(1)) {
    if (arg.startsWith("--region=")) args.region = arg.slice("--region=".length);
    else if (arg.startsWith("--pbf=")) args.pbf = arg.slice("--pbf=".length);
    else if (arg.startsWith("--root=")) {
      args.root = arg.slice("--root=".length);
      rootExplicit = true;
    }
    else if (arg.startsWith("--limit=")) args.limit = Number(arg.slice("--limit=".length)) || 0;
    else if (arg === "--force") args.force = true;
  }
  if (args.region === "us" && !rootExplicit) {
    args.root = process.env.RANGEFIND_OSM_US_ROOT || ".cache/osm-us";
  }
  if (!args.pbf) args.pbf = `${args.root}/data/${args.region}-latest.osm.pbf`;
  return args;
}

function ensurePbf(args) {
  if (existsSync(args.pbf)) return;
  const url = REGIONS[args.region];
  if (!url) throw new Error(`No PBF at ${args.pbf} and unknown region "${args.region}".`);
  mkdirSync(dirname(resolve(args.pbf)), { recursive: true });
  console.log(`[osm] downloading ${url}`);
  const partial = `${args.pbf}.partial`;
  execFileSync("curl", ["--fail", "--location", "--continue-at", "-", "--output", partial, url], { stdio: "inherit" });
  renameSync(partial, args.pbf);
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

const WAY_DOC_KEYS = new Set([
  "name",
  ...CATEGORY_KEYS,
  ...ALIAS_KEYS,
  "addr:street",
  "addr:housenumber",
  "addr:city",
  "cuisine",
  "population"
]);

function wayDocTagEntries(tags) {
  const entries = [];
  for (const [key, value] of tags) {
    if (WAY_DOC_KEYS.has(key) || key.startsWith("name:")) entries.push([key, value]);
  }
  return entries;
}

const JSONL_WRITE_BUFFER_BYTES = 8 * 1024 * 1024;

function createBufferedJsonlWriter(path) {
  const fd = openSync(path, "w");
  let pending = [];
  let pendingBytes = 0;
  let bytes = 0;
  let closed = false;

  function flush() {
    if (!pending.length) return;
    const buffer = Buffer.from(pending.join(""));
    let offset = 0;
    while (offset < buffer.length) offset += writeSync(fd, buffer, offset, buffer.length - offset);
    bytes += buffer.length;
    pending = [];
    pendingBytes = 0;
  }

  return {
    write(value) {
      const line = `${JSON.stringify(value)}\n`;
      pending.push(line);
      pendingBytes += Buffer.byteLength(line);
      if (pendingBytes >= JSONL_WRITE_BUFFER_BYTES) flush();
    },
    writeLine(value) {
      const line = `${value}\n`;
      pending.push(line);
      pendingBytes += Buffer.byteLength(line);
      if (pendingBytes >= JSONL_WRITE_BUFFER_BYTES) flush();
    },
    close() {
      if (closed) return;
      flush();
      closeSync(fd);
      closed = true;
    },
    get bytes() {
      return bytes + pendingBytes;
    }
  };
}

async function eachJsonLine(path, handler) {
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    if (line && await handler(JSON.parse(line))) break;
  }
}

async function eachRawLine(path, handler) {
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    if (line && await handler(line)) break;
  }
}

function pbfIdentity(path) {
  const absolute = resolve(path);
  const stat = statSync(absolute);
  return { pbf: absolute, pbfBytes: stat.size, pbfMtimeMs: Math.floor(stat.mtimeMs) };
}

function matchesPbf(meta, identity) {
  return meta?.pbf === identity.pbf
    && meta?.pbfBytes === identity.pbfBytes
    && meta?.pbfMtimeMs === identity.pbfMtimeMs;
}

function progressLogger(label) {
  let last = performance.now();
  return (detail) => {
    const now = performance.now();
    if (now - last < 30000) return;
    last = now;
    console.log(`[osm] ${label}: ${detail()}`);
  };
}

function elapsedSeconds(start) {
  return Math.round((performance.now() - start) / 100) / 10;
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
  const outPartialPath = `${outPath}.partial`;
  const metaPath = resolve(args.root, "data", "osm-places.meta.json");
  const wayPath = resolve(args.root, "data", "osm-way-candidates.jsonl");
  const wayPartialPath = `${wayPath}.partial`;
  const wayMetaPath = resolve(args.root, "data", "osm-way-candidates.meta.json");
  const anchorRawPath = resolve(args.root, "data", "osm-way-anchors.bin");
  const anchorSortedPath = resolve(args.root, "data", "osm-way-anchors.sorted.bin");
  const anchorMetaPath = resolve(args.root, "data", "osm-way-anchors.meta.json");
  const anchorScratchPath = resolve(args.root, "data", "osm-way-anchor-sort");
  const nodePath = resolve(args.root, "data", "osm-node-docs.jsonl");
  const nodePartialPath = `${nodePath}.partial`;
  const nodeMetaPath = resolve(args.root, "data", "osm-node-docs.meta.json");
  const coordPath = resolve(args.root, "data", "osm-way-anchor-coords.sqlite");
  const coordMetaPath = resolve(args.root, "data", "osm-way-anchor-coords.meta.json");
  mkdirSync(dirname(outPath), { recursive: true });
  ensurePbf(args);
  const identity = pbfIdentity(args.pbf);
  if (!args.force && existsSync(outPath) && existsSync(metaPath)) {
    const meta = JSON.parse(readFileSync(metaPath, "utf8"));
    if (matchesPbf(meta, identity) && meta.limit === args.limit) {
      console.log(`[osm] reusing ${outPath} (${meta.docs} docs)`);
      return outPath;
    }
  }
  const t0 = performance.now();
  const stageSeconds = {};
  let ways = 0;
  let reusableWays = false;
  if (!args.force && existsSync(wayPath) && existsSync(wayMetaPath)) {
    const meta = JSON.parse(readFileSync(wayMetaPath, "utf8"));
    if (matchesPbf(meta, identity)) {
      console.log(`[osm] reusing disk-backed candidate-way spool (${meta.ways} ways, ${meta.bytes} bytes)`);
      ways = meta.ways;
      reusableWays = true;
    }
  }
  if (!reusableWays) {
    const stageStart = performance.now();
    console.log(`[osm] stage 1/4: spooling candidate ways and anchor references from ${args.pbf}`);
    const writer = createBufferedJsonlWriter(wayPartialPath);
    const anchorWriter = createAnchorRefWriter(anchorRawPath);
    const progress = progressLogger("candidate ways");
    try {
      scanPbf(args.pbf, {
        onWay(id, refs, tags) {
          if (!refs.length) return;
          if (!tags.get("name") && !firstCategory(tags)) return;
          const ref = refs[0];
          writer.write([id, ref, wayDocTagEntries(tags)]);
          anchorWriter.write(ref);
          ways++;
          progress(() => `${ways.toLocaleString()} ways, ${(writer.bytes / 2 ** 30).toFixed(2)} GiB spooled`);
        }
      });
    } finally {
      writer.close();
      anchorWriter.close();
    }
    renameSync(wayPartialPath, wayPath);
    writeFileSync(wayMetaPath, JSON.stringify({ ...identity, ways, bytes: statSync(wayPath).size }, null, 2));
    stageSeconds.candidateWays = elapsedSeconds(stageStart);
  }

  let anchorMeta = null;
  if (!args.force && existsSync(anchorSortedPath) && existsSync(anchorMetaPath)) {
    const meta = JSON.parse(readFileSync(anchorMetaPath, "utf8"));
    if (matchesPbf(meta, identity) && meta.ways === ways) anchorMeta = meta;
  }
  if (!anchorMeta) {
    if (!existsSync(anchorRawPath) || reusableWays) {
      console.log(`[osm] reconstructing anchor references from ${ways.toLocaleString()} spooled ways`);
      const anchorWriter = createAnchorRefWriter(anchorRawPath);
      let read = 0;
      const progress = progressLogger("anchor references");
      try {
        await eachJsonLine(wayPath, ([, ref]) => {
          anchorWriter.write(ref);
          read++;
          progress(() => `${read.toLocaleString()}/${ways.toLocaleString()} references`);
        });
      } finally {
        anchorWriter.close();
      }
    }
    console.log("[osm] stage 2/4: externally sorting and deduplicating way anchors");
    const stageStart = performance.now();
    rmSync(anchorScratchPath, { recursive: true, force: true });
    const sorted = sortUniqueAnchorRefs(anchorRawPath, anchorSortedPath, anchorScratchPath);
    rmSync(anchorRawPath, { force: true });
    rmSync(anchorScratchPath, { recursive: true, force: true });
    anchorMeta = { ...identity, ways, anchors: sorted.count, bytes: sorted.bytes, runs: sorted.runs };
    writeFileSync(anchorMetaPath, JSON.stringify(anchorMeta, null, 2));
    stageSeconds.anchorSort = elapsedSeconds(stageStart);
  } else {
    console.log(`[osm] reusing ${anchorMeta.anchors.toLocaleString()} sorted way anchors`);
  }
  const uniqueAnchors = anchorMeta.anchors;

  let nodeMeta = null;
  if (!args.force && existsSync(nodePath) && existsSync(nodeMetaPath)
      && coordinateStoreExists(coordPath) && existsSync(coordMetaPath)) {
    const candidateNodeMeta = JSON.parse(readFileSync(nodeMetaPath, "utf8"));
    const candidateCoordMeta = JSON.parse(readFileSync(coordMetaPath, "utf8"));
    if (matchesPbf(candidateNodeMeta, identity) && candidateNodeMeta.anchors === uniqueAnchors
        && matchesPbf(candidateCoordMeta, identity) && candidateCoordMeta.anchors === uniqueAnchors) {
      nodeMeta = candidateNodeMeta;
    }
  }
  if (!nodeMeta) {
    const stageStart = performance.now();
    console.log(`[osm] stage 3/4: scanning nodes (${ways.toLocaleString()} candidate ways, ${uniqueAnchors.toLocaleString()} unique anchors)`);
    const nodeWriter = createBufferedJsonlWriter(nodePartialPath);
    const anchors = openSortedAnchorRefs(anchorSortedPath);
    const coords = createCoordinateStore(coordPath, { reset: true });
    let nodeDocs = 0;
    let anchorsResolved = 0;
    let lastNodeId = -1;
    let storedCoords = 0;
    const nodeProgress = progressLogger("nodes");
    try {
      scanPbf(args.pbf, {
        onNode(id, lat, lon, tags) {
          if (id < lastNodeId) throw new Error("OSM node IDs are not ordered; bounded anchor resolution requires a sorted Geofabrik PBF.");
          lastNodeId = id;
          while (anchors.current != null && anchors.current < id) anchors.advance();
          if (anchors.current === id) {
            coords.put(id, lat, lon);
            anchorsResolved++;
            anchors.advance();
          }
          if (tags) {
            const doc = placeDoc("node", id, lat, lon, tags);
            if (doc) {
              nodeWriter.write(doc);
              nodeDocs++;
            }
          }
          nodeProgress(() => `${nodeDocs.toLocaleString()} node documents, ${anchorsResolved.toLocaleString()}/${uniqueAnchors.toLocaleString()} anchors resolved`);
        }
      });
      storedCoords = coords.count();
    } finally {
      nodeWriter.close();
      coords.close();
      anchors.close();
    }
    renameSync(nodePartialPath, nodePath);
    nodeMeta = { ...identity, nodeDocs, anchors: uniqueAnchors, anchorsResolved: storedCoords, bytes: statSync(nodePath).size };
    writeFileSync(nodeMetaPath, JSON.stringify(nodeMeta, null, 2));
    writeFileSync(coordMetaPath, JSON.stringify({ ...identity, anchors: uniqueAnchors, anchorsResolved: storedCoords, bytes: statSync(coordPath).size }, null, 2));
    stageSeconds.nodes = elapsedSeconds(stageStart);
  } else {
    console.log(`[osm] reusing ${nodeMeta.nodeDocs.toLocaleString()} node documents and ${nodeMeta.anchorsResolved.toLocaleString()} anchor coordinates`);
  }

  console.log("[osm] stage 4/4: materializing searchable node and way documents");
  const outputStart = performance.now();
  const writer = createBufferedJsonlWriter(outPartialPath);
  const coords = createCoordinateStore(coordPath);
  let docs = 0;
  let nodeDocs = 0;
  let waysRead = 0;
  let wayDocs = 0;
  const outputProgress = progressLogger("output documents");
  try {
    await eachRawLine(nodePath, line => {
      if (args.limit && docs >= args.limit) return true;
      writer.writeLine(line);
      docs++;
      nodeDocs++;
      outputProgress(() => `${docs.toLocaleString()} documents`);
      return Boolean(args.limit && docs >= args.limit);
    });
    if (!args.limit || docs < args.limit) {
      await eachJsonLine(wayPath, ([id, ref, entries]) => {
        waysRead++;
        if (!args.limit || docs < args.limit) {
          const point = coords.get(ref);
          if (point) {
            const doc = placeDoc("way", id, point.lat, point.lon, new Map(entries));
            if (doc) {
              writer.write(doc);
              docs++;
              wayDocs++;
            }
          }
        }
        outputProgress(() => `${waysRead.toLocaleString()}/${ways.toLocaleString()} ways, ${docs.toLocaleString()} total documents`);
        return Boolean(args.limit && docs >= args.limit);
      });
    }
  } finally {
    writer.close();
    coords.close();
  }
  renameSync(outPartialPath, outPath);
  stageSeconds.output = elapsedSeconds(outputStart);
  const meta = {
    ...identity,
    limit: args.limit,
    docs,
    nodeDocs,
    wayDocs,
    ways,
    anchors: uniqueAnchors,
    bytes: statSync(outPath).size,
    stageSeconds,
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
  if (args.region === "us") {
    // National builds run from large external workspaces. Sequential doc-id
    // packing avoids millions of random spool reads, level-3 posting gzip
    // preserves query layout while reducing reducer CPU, and the high merge
    // fan-in is now safely bounded by decoded directory bytes.
    config.docLayoutStrategy = "doc-id";
    config.postingGzipLevel = 3;
    config.segmentMergeFanIn = 512;
    config.codeStorePreloadMaxBytes = 2304 * 1024 * 1024;
    config.buildTelemetryPath = "osm-us-build-telemetry.json";
  }
  mkdirSync(resolve(args.root, "public"), { recursive: true });
  writeFileSync(configPath, JSON.stringify(config, null, 2));
  const bundle = resolve("dist/runtime.browser.js");
  if (existsSync(bundle)) copyFileSync(bundle, resolve(args.root, "public", "runtime.browser.js"));
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
