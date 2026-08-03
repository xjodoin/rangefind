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
//   node scripts/osm_fixture.mjs all --region=quebec --no-rqa

import {
  closeSync,
  createReadStream,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  realpathSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
  writeSync
} from "node:fs";
import { execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { scanPbf } from "./osm_pbf.mjs";
import {
  addressFromTags,
  enrichDocLocality,
  geometryForWay,
  interpolationRangeDocs,
  placeDoc,
  retainedAddressTagEntries,
  searchablePlaceTags,
  shouldRetainPlaceGeometry,
  wayDocTagEntries
} from "../src/integrations/osm/documents.js";
import {
  assembleRings,
  createLocalityIndex,
  LOCALITY_ADMIN_LEVELS,
  PLACE_RADII_METERS
} from "../src/integrations/osm/locality_index.js";
import {
  coordinateStoreExists,
  createAnchorRefWriter,
  createCoordinateStore,
  openSortedAnchorRefs,
  sortUniqueAnchorRefs
} from "./osm_anchor_store.mjs";
import { augmentOsmWithRqa, RQA_CSV_URL } from "../src/integrations/osm/node/rqa.js";
import { buildOsmIndex } from "../src/integrations/osm/node/builder.js";

export { addressFromTags, interpolationRangeDocs, placeDoc } from "../src/integrations/osm/documents.js";

const REGIONS = {
  luxembourg: "https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf",
  quebec: "https://download.geofabrik.de/north-america/canada/quebec-latest.osm.pbf",
  switzerland: "https://download.geofabrik.de/europe/switzerland-latest.osm.pbf",
  us: "https://download.geofabrik.de/north-america/us-latest.osm.pbf"
};

// Included in every reusable extraction-stage identity. Bump this whenever
// document selection or normalized output changes so a large PBF can never
// silently reuse an older corpus shape.
// v10: compact render geometry and true area centroids for useful POI ways.
const OSM_FIXTURE_SCHEMA_VERSION = 10;

function parseArgs(argv) {
  const args = {
    command: argv[0] || "all",
    region: "luxembourg",
    pbf: "",
    root: "examples/osm-geo",
    limit: 0,
    force: false,
    rqa: null,
    rqaArchive: "",
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
    else if (arg.startsWith("--rqa-archive=")) args.rqaArchive = arg.slice("--rqa-archive=".length);
    else if (arg === "--rqa") args.rqa = true;
    else if (arg === "--no-rqa") args.rqa = false;
    else if (arg === "--force") args.force = true;
  }
  if (args.region === "us" && !rootExplicit) {
    args.root = process.env.RANGEFIND_OSM_US_ROOT || ".cache/osm-us";
  }
  if (!args.pbf) args.pbf = `${args.root}/data/${args.region}-latest.osm.pbf`;
  if (args.rqa == null) args.rqa = args.region === "quebec";
  if (!args.rqaArchive) args.rqaArchive = process.env.RANGEFIND_RQA_ARCHIVE || `${args.root}/data/RQA_CSV.zip`;
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

function ensureRqa(args) {
  if (!args.rqa || existsSync(args.rqaArchive)) return;
  mkdirSync(dirname(resolve(args.rqaArchive)), { recursive: true });
  console.log(`[rqa] downloading ${RQA_CSV_URL}`);
  const partial = `${args.rqaArchive}.partial`;
  execFileSync("curl", ["--fail", "--location", "--continue-at", "-", "--retry", "5", "--output", partial, RQA_CSV_URL], { stdio: "inherit" });
  renameSync(partial, args.rqaArchive);
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

function pbfIdentity(path) {
  const absolute = resolve(path);
  const stat = statSync(absolute);
  return {
    schemaVersion: OSM_FIXTURE_SCHEMA_VERSION,
    pbf: absolute,
    pbfBytes: stat.size,
    pbfMtimeMs: Math.floor(stat.mtimeMs)
  };
}

function matchesPbf(meta, identity) {
  return meta?.schemaVersion === identity.schemaVersion
    && meta?.pbf === identity.pbf
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


async function finishOsmCorpus(args, outPath, meta) {
  if (!args.rqa) return outPath;
  ensureRqa(args);
  return (await augmentOsmWithRqa({
    root: args.root,
    osmPath: outPath,
    archivePath: args.rqaArchive,
    osmDocs: meta.docs,
    force: args.force
  })).path;
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
  const boundaryRelationsPath = resolve(args.root, "data", "osm-boundary-relations.jsonl");
  const boundaryRelationsPartialPath = `${boundaryRelationsPath}.partial`;
  const boundaryRelationsMetaPath = resolve(args.root, "data", "osm-boundary-relations.meta.json");
  const boundaryWaysPath = resolve(args.root, "data", "osm-boundary-ways.jsonl");
  const boundaryWaysPartialPath = `${boundaryWaysPath}.partial`;
  const placeNodesPath = resolve(args.root, "data", "osm-place-nodes.jsonl");
  const placeNodesPartialPath = `${placeNodesPath}.partial`;
  mkdirSync(dirname(outPath), { recursive: true });
  ensurePbf(args);
  const identity = pbfIdentity(args.pbf);
  if (!args.force && existsSync(outPath) && existsSync(metaPath)) {
    const meta = JSON.parse(readFileSync(metaPath, "utf8"));
    if (matchesPbf(meta, identity) && meta.limit === args.limit) {
      console.log(`[osm] reusing ${outPath} (${meta.docs} docs)`);
      return finishOsmCorpus(args, outPath, meta);
    }
  }
  const t0 = performance.now();
  const stageSeconds = {};

  // Stage 1/5: administrative boundary relations (municipality polygons for
  // locality enrichment). Relations sit after ways in the PBF, so knowing
  // which ways are boundary members before the way scan requires this
  // dedicated pass; node and way groups are skipped undecoded.
  let boundaryRelations = 0;
  let reusableBoundaries = false;
  if (!args.force && existsSync(boundaryRelationsPath) && existsSync(boundaryRelationsMetaPath)) {
    const meta = JSON.parse(readFileSync(boundaryRelationsMetaPath, "utf8"));
    if (matchesPbf(meta, identity)) {
      console.log(`[osm] reusing ${meta.boundaryRelations.toLocaleString()} boundary relations`);
      boundaryRelations = meta.boundaryRelations;
      reusableBoundaries = true;
    }
  }
  if (!reusableBoundaries) {
    const stageStart = performance.now();
    console.log(`[osm] stage 1/5: scanning boundary relations from ${args.pbf}`);
    const writer = createBufferedJsonlWriter(boundaryRelationsPartialPath);
    const adminLevels = new Set(LOCALITY_ADMIN_LEVELS.map(String));
    scanPbf(args.pbf, {
      onRelation(id, members, tags) {
        if (tags.get("boundary") !== "administrative") return;
        if (!adminLevels.has(tags.get("admin_level"))) return;
        const name = tags.get("name");
        if (!name) return;
        const wayIds = [];
        for (const member of members) {
          // Inner rings join the even-odd containment test alongside outers,
          // so both roles spool; other roles (admin_centre, label) are nodes.
          if (member.type === "way" && (member.role === "outer" || member.role === "inner" || member.role === "")) {
            wayIds.push(member.ref);
          }
        }
        if (!wayIds.length) return;
        writer.write([id, name, Number(tags.get("admin_level")), wayIds]);
        boundaryRelations++;
      }
    });
    writer.close();
    renameSync(boundaryRelationsPartialPath, boundaryRelationsPath);
    writeFileSync(boundaryRelationsMetaPath, JSON.stringify({ ...identity, boundaryRelations }, null, 2));
    stageSeconds.boundaryRelations = elapsedSeconds(stageStart);
  }
  const boundaryWayIds = new Set();
  await eachJsonLine(boundaryRelationsPath, ([, , , wayIds]) => {
    for (const wayId of wayIds) boundaryWayIds.add(wayId);
  });

  let ways = 0;
  let interpolationWays = 0;
  let geometryWays = 0;
  let boundaryWays = 0;
  let reusableWays = false;
  if (!args.force && existsSync(wayPath) && existsSync(wayMetaPath) && existsSync(boundaryWaysPath)) {
    const meta = JSON.parse(readFileSync(wayMetaPath, "utf8"));
    if (matchesPbf(meta, identity)) {
      console.log(`[osm] reusing disk-backed candidate-way spool (${meta.ways} ways, ${meta.bytes} bytes)`);
      ways = meta.ways;
      interpolationWays = meta.interpolationWays || 0;
      geometryWays = meta.geometryWays || 0;
      boundaryWays = meta.boundaryWays || 0;
      reusableWays = true;
    }
  }
  if (!reusableWays) {
    const stageStart = performance.now();
    console.log(`[osm] stage 2/5: spooling candidate ways and anchor references from ${args.pbf}`);
    const writer = createBufferedJsonlWriter(wayPartialPath);
    const boundaryWriter = createBufferedJsonlWriter(boundaryWaysPartialPath);
    const anchorWriter = createAnchorRefWriter(anchorRawPath);
    const progress = progressLogger("candidate ways");
    try {
      scanPbf(args.pbf, {
        onWay(id, refs, tags) {
          if (!refs.length) return;
          if (boundaryWayIds.has(id)) {
            // Boundary geometry rides the anchor store: every vertex needs a
            // coordinate for ring assembly, resolved by the same sorted-anchor
            // pass that resolves way anchors.
            boundaryWriter.write([id, refs]);
            for (const ref of refs) anchorWriter.write(ref);
            boundaryWays++;
          }
          if (!searchablePlaceTags(tags)) return;
          const interpolation = Boolean(tags.get("addr:interpolation"));
          const geometry = shouldRetainPlaceGeometry(tags, refs);
          const descriptor = {
            anchor: refs[0],
            ...(interpolation ? { interpolation: refs } : {}),
            ...(geometry ? { geometry: refs } : {})
          };
          writer.write([id, descriptor, wayDocTagEntries(tags)]);
          if (interpolation) interpolationWays++;
          if (geometry) geometryWays++;
          if (interpolation || geometry) {
            for (const ref of refs) anchorWriter.write(ref);
          } else {
            anchorWriter.write(refs[0]);
          }
          ways++;
          progress(() => `${ways.toLocaleString()} ways, ${(writer.bytes / 2 ** 30).toFixed(2)} GiB spooled`);
        }
      });
    } finally {
      writer.close();
      boundaryWriter.close();
      anchorWriter.close();
    }
    renameSync(wayPartialPath, wayPath);
    renameSync(boundaryWaysPartialPath, boundaryWaysPath);
    writeFileSync(wayMetaPath, JSON.stringify({ ...identity, ways, interpolationWays, geometryWays, boundaryWays, bytes: statSync(wayPath).size }, null, 2));
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
        await eachJsonLine(wayPath, ([, descriptor]) => {
          const refs = descriptor?.interpolation || descriptor?.geometry || [descriptor?.anchor ?? descriptor];
          for (const item of refs) anchorWriter.write(item);
          read++;
          progress(() => `${read.toLocaleString()}/${ways.toLocaleString()} references`);
        });
        await eachJsonLine(boundaryWaysPath, ([, refs]) => {
          for (const item of refs) anchorWriter.write(item);
        });
      } finally {
        anchorWriter.close();
      }
    }
    console.log("[osm] stage 3/5: externally sorting and deduplicating way anchors");
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
  if (!args.force && existsSync(nodePath) && existsSync(nodeMetaPath) && existsSync(placeNodesPath)
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
    console.log(`[osm] stage 4/5: scanning nodes (${ways.toLocaleString()} candidate ways, ${uniqueAnchors.toLocaleString()} unique anchors)`);
    const nodeWriter = createBufferedJsonlWriter(nodePartialPath);
    const placeNodeWriter = createBufferedJsonlWriter(placeNodesPartialPath);
    const anchors = openSortedAnchorRefs(anchorSortedPath);
    const coords = createCoordinateStore(coordPath, { reset: true });
    let nodeDocs = 0;
    let placeNodes = 0;
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
            const entries = retainedAddressTagEntries(tags);
            coords.put(id, lat, lon, entries.length ? JSON.stringify(entries) : null);
            anchorsResolved++;
            anchors.advance();
          }
          if (tags) {
            // Locality fallback candidates for documents outside any
            // assembled boundary: place nodes sit at settlement centers.
            const placeType = tags.get("place");
            if (placeType && PLACE_RADII_METERS[placeType] && tags.get("name")) {
              placeNodeWriter.write([tags.get("name"), placeType, lat, lon]);
              placeNodes++;
            }
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
      placeNodeWriter.close();
      coords.close();
      anchors.close();
    }
    renameSync(nodePartialPath, nodePath);
    renameSync(placeNodesPartialPath, placeNodesPath);
    nodeMeta = { ...identity, nodeDocs, placeNodes, anchors: uniqueAnchors, anchorsResolved: storedCoords, bytes: statSync(nodePath).size };
    writeFileSync(nodeMetaPath, JSON.stringify(nodeMeta, null, 2));
    writeFileSync(coordMetaPath, JSON.stringify({ ...identity, anchors: uniqueAnchors, anchorsResolved: storedCoords, bytes: statSync(coordPath).size }, null, 2));
    stageSeconds.nodes = elapsedSeconds(stageStart);
  } else {
    console.log(`[osm] reusing ${nodeMeta.nodeDocs.toLocaleString()} node documents and ${nodeMeta.anchorsResolved.toLocaleString()} anchor coordinates`);
  }

  console.log("[osm] stage 5/5: materializing searchable node and way documents");
  const outputStart = performance.now();
  const writer = createBufferedJsonlWriter(outPartialPath);
  const coords = createCoordinateStore(coordPath);
  let docs = 0;
  let nodeDocs = 0;
  let waysRead = 0;
  let wayDocs = 0;
  let interpolationRangeDocsWritten = 0;
  const enriched = { boundary: 0, place: 0 };
  let localityStats = { boundaries: 0, boundariesDropped: 0, ringsDropped: 0, places: 0 };
  const outputProgress = progressLogger("output documents");
  try {
    // Locality index: assemble municipality polygons from the boundary
    // spools (vertex coordinates come from the anchor store) and load the
    // place-node fallback candidates.
    const locality = createLocalityIndex();
    const boundaryWayRefs = new Map();
    await eachJsonLine(boundaryWaysPath, ([id, refs]) => {
      boundaryWayRefs.set(id, refs);
    });
    await eachJsonLine(boundaryRelationsPath, ([, name, adminLevel, wayIds]) => {
      const memberWays = wayIds.map(wayId => boundaryWayRefs.get(wayId)).filter(Boolean);
      if (!memberWays.length) {
        localityStats.boundariesDropped++;
        return;
      }
      const assembly = assembleRings(memberWays);
      localityStats.ringsDropped += assembly.dropped;
      const rings = [];
      for (const ring of assembly.rings) {
        const points = [];
        for (const nodeId of ring) {
          const point = coords.get(nodeId);
          if (!point) {
            points.length = 0;
            break;
          }
          points.push([point.lat, point.lon]);
        }
        if (points.length) rings.push(points);
      }
      if (rings.length && locality.addBoundary({ id: 0, name, adminLevel, rings })) {
        localityStats.boundaries++;
      } else {
        localityStats.boundariesDropped++;
      }
    });
    await eachJsonLine(placeNodesPath, ([name, type, lat, lon]) => {
      if (locality.addPlace({ name, type, lat, lon })) localityStats.places++;
    });
    locality.finalize();
    boundaryWayRefs.clear();
    console.log(`[osm] locality index: ${localityStats.boundaries.toLocaleString()} boundaries (${localityStats.boundariesDropped} dropped, ${localityStats.ringsDropped} open rings), ${localityStats.places.toLocaleString()} place nodes`);

    const enrich = doc => {
      if (doc.city || doc.category === "place") return;
      const resolved = locality.resolve(doc.lat, doc.lon);
      if (resolved && enrichDocLocality(doc, resolved.city)) enriched[resolved.source]++;
    };

    await eachJsonLine(nodePath, doc => {
      if (args.limit && docs >= args.limit) return true;
      enrich(doc);
      writer.write(doc);
      docs++;
      nodeDocs++;
      outputProgress(() => `${docs.toLocaleString()} documents`);
      return Boolean(args.limit && docs >= args.limit);
    });
    if (!args.limit || docs < args.limit) {
      await eachJsonLine(wayPath, ([id, descriptor, entries]) => {
        waysRead++;
        if (!args.limit || docs < args.limit) {
          const interpolationRefs = descriptor?.interpolation || null;
          const geometryRefs = descriptor?.geometry || null;
          const refs = interpolationRefs || geometryRefs || [descriptor?.anchor ?? descriptor];
          const points = refs.map(item => {
            const point = coords.get(item);
            if (!point) return null;
            return {
              lat: point.lat,
              lon: point.lon,
              tags: point.data ? new Map(JSON.parse(point.data)) : new Map()
            };
          });
          const point = points[0];
          const tags = new Map(entries);
          if (point) {
            const shape = geometryRefs ? geometryForWay(points) : null;
            const center = shape?.center || point;
            const doc = placeDoc("way", id, center.lat, center.lon, tags, { geometry: shape?.geometry });
            if (doc) {
              enrich(doc);
              writer.write(doc);
              docs++;
              wayDocs++;
            }
          }
          if ((!args.limit || docs < args.limit) && interpolationRefs) {
            for (const doc of interpolationRangeDocs(id, interpolationRefs, points, tags)) {
              if (args.limit && docs >= args.limit) break;
              enrich(doc);
              writer.write(doc);
              docs++;
              interpolationRangeDocsWritten++;
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
    interpolationWays,
    geometryWays,
    interpolationRangeDocs: interpolationRangeDocsWritten,
    ways,
    boundaryWays,
    anchors: uniqueAnchors,
    locality: { ...localityStats, enrichedFromBoundaries: enriched.boundary, enrichedFromPlaces: enriched.place },
    bytes: statSync(outPath).size,
    stageSeconds,
    seconds: Math.round((performance.now() - t0) / 100) / 10
  };
  writeFileSync(metaPath, JSON.stringify(meta, null, 2));
  console.log(`[osm] wrote ${docs} docs to ${outPath} in ${meta.seconds}s (locality: ${enriched.boundary.toLocaleString()} from boundaries, ${enriched.place.toLocaleString()} from place nodes)`);
  return finishOsmCorpus(args, outPath, meta);
}


// Programmatic extraction API, published as `rangefind/osm/extract`: PBF →
// normalized places JSONL under `${root}/data/`, reusing the fingerprinted
// skip logic (a second call with an unchanged PBF returns immediately).
// Returns the corpus meta ({ docs, bytes, seconds, … }).
export async function extractOsmPlaces(options = {}) {
  const root = String(options.root || "examples/osm-geo");
  const region = String(options.region || "custom");
  const args = {
    command: "jsonl",
    region,
    pbf: options.pbf ? String(options.pbf) : `${root}/data/${region}-latest.osm.pbf`,
    root,
    limit: Math.max(0, Math.floor(Number(options.limit || 0))),
    force: Boolean(options.force),
    rqa: Boolean(options.rqa),
    rqaArchive: options.rqaArchive ? String(options.rqaArchive) : `${root}/data/RQA_CSV.zip`,
    buildProgressLogMs: Number(options.buildProgressLogMs ?? 15000)
  };
  await writeJsonl(args);
  return JSON.parse(readFileSync(resolve(root, "data", "osm-places.meta.json"), "utf8"));
}

async function run() {
  const args = parseArgs(process.argv.slice(2));
  if (!["jsonl", "build", "all"].includes(args.command)) {
    throw new Error(`Unknown command "${args.command}" (expected jsonl, build, or all).`);
  }
  if (args.command === "jsonl" || args.command === "all") await writeJsonl(args);
  if (args.command === "build" || args.command === "all") {
    const result = await buildOsmIndex(args);
    console.log(`[osm] build finished in ${result.seconds}s`);
  }
}

// realpath both sides: when invoked through a symlinked node_modules (the
// script ships in the npm package), argv[1] is the symlink while
// import.meta.url is already resolved.
if (process.argv[1] && (() => {
  try {
    return realpathSync(resolve(process.argv[1])) === fileURLToPath(import.meta.url);
  } catch {
    return false;
  }
})()) {
  run().catch(err => {
    console.error(err);
    process.exit(1);
  });
}
