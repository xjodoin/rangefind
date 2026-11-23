// Parity fixtures for the Rust `geo` module group.
//
// Runs the real JS geo modules (src/geo_tree.js, src/geo_cells.js,
// src/geo_route.js, src/geo_shard_routing.js) over deterministic inputs and
// writes their outputs to rust/target/fixtures/geo/*.json. Binary inputs are
// produced with the JS encoders and stored as base64.
//
// Usage (from the repo root): node rust/tools/fixtures/geo.mjs

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  boxContainsBoxE7,
  boxContainsPointE7,
  boxIntersectsE7,
  boxesForRadiusE7,
  buildGeoTreeLeaves,
  decodeGeoBranchPage,
  decodeGeoLeafPage,
  encodeGeoBranchPage,
  encodeGeoLeafPage,
  encodeGeoTreeRoot,
  haversineMeters,
  haversineMetersE7,
  latToE7,
  lonToE7,
  mergeBlockFilterSummaries,
  parseGeoTreeRoot,
  pointToBoxDistanceMetersE7,
  pointToBoxMaxDistanceMetersE7
} from "../../../src/geo_tree.js";
import {
  decodeGeoCellBlock,
  encodeGeoCellBlock,
  geoCellBlock,
  geoCellBlockKey,
  geoCellForE7,
  geoCellRouteKey,
  geoCellsForBoxes,
  geoCellsForRoute
} from "../../../src/geo_cells.js";
import { encodePolyline, matchPointToRoute, prepareRoute, routeMatchPublic } from "../../../src/geo_route.js";
import { shardBboxDistanceMeters, shardBoxIntersects } from "../../../src/geo_shard_routing.js";

const here = dirname(fileURLToPath(import.meta.url));
const outDir = join(here, "..", "..", "target", "fixtures", "geo");
mkdirSync(outDir, { recursive: true });

// ---------------------------------------------------------------- helpers

function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const random = mulberry32(0x5eed1234);
const pick = items => items[Math.floor(random() * items.length)];
const randInt = (lo, hi) => lo + Math.floor(random() * (hi - lo + 1));

// Floats that JSON cannot carry exactly are written as strings.
function f(value) {
  if (typeof value !== "number") return value;
  if (Number.isNaN(value)) return "NaN";
  if (value === Infinity) return "Infinity";
  if (value === -Infinity) return "-Infinity";
  if (Object.is(value, -0)) return "-0";
  return value;
}

function deepF(value) {
  if (typeof value === "number") return f(value);
  if (Array.isArray(value)) return value.map(deepF);
  if (ArrayBuffer.isView(value)) return [...value].map(f);
  if (value && typeof value === "object") {
    const out = {};
    for (const [key, item] of Object.entries(value)) out[key] = deepF(item);
    return out;
  }
  return value;
}

const b64 = bytes => Buffer.from(bytes).toString("base64");

function capture(fn) {
  try {
    return { ok: fn() };
  } catch (error) {
    return { error: error.message };
  }
}

function write(name, data) {
  writeFileSync(join(outDir, `${name}.json`), JSON.stringify(data));
  return data;
}

const counts = {};

// ------------------------------------------------------------- primitives
// Raw Math.* results, so the Rust test can report exactly which libm
// functions differ from V8 on this machine.
{
  const cases = [];
  const angle = () => (random() * 2 - 1) * Math.PI;
  const wide = () => (random() * 2 - 1) * 10 ** randInt(-8, 6);
  for (let i = 0; i < 20000; i++) {
    const x = i % 3 === 0 ? wide() : angle();
    const y = i % 2 === 0 ? wide() : angle();
    const half = (random() * 2 - 1) * Math.PI / 2 * 0.9999;
    const unit = random();
    cases.push({
      x: f(x),
      y: f(y),
      half: f(half),
      unit: f(unit),
      sin: f(Math.sin(x)),
      cos: f(Math.cos(x)),
      tan: f(Math.tan(half)),
      atan: f(Math.atan(x)),
      atan2: f(Math.atan2(y, x)),
      asin: f(Math.asin(unit)),
      asinh: f(Math.asinh(x)),
      sqrt: f(Math.sqrt(Math.abs(x))),
      pow2: f(x ** 2),
      mul2: f(x * x)
    });
  }
  counts.primitives = cases.length;
  write("primitives", cases);
}

// ------------------------------------------------------------------- math
{
  const toE7Inputs = [
    null, "", 0, -0, 1, -1, 45.5017, -73.5673, 90, -90, 90.00000001, -90.00000004, 90.00000005, -90.00000005,
    180, -180, 180.00000004, 180.00000005, -180.00000005, 179.99999995, -179.99999995, 1e-8, 5e-8, -5e-8,
    1.5e-7, -1.5e-7, 2.5e-7, -2.5e-7, 0.00000005, 12.34567895, -12.34567895, 0.049999999999999996,
    "45.5", " 12.25 ", "abc", "1e1", "0x10", "Infinity", "-Infinity", "NaN", true, false, [], [12.5], ["7"], [1, 2], {},
    1e300, -1e300, 4.9e-324, 89.9999999499999, 179.99999994999
  ];
  for (let i = 0; i < 3000; i++) {
    const kind = i % 4;
    if (kind === 0) toE7Inputs.push((random() * 2 - 1) * 200);
    else if (kind === 1) toE7Inputs.push(Math.round((random() * 2 - 1) * 1.8e9) / 1e7 + (random() < 0.5 ? 5e-8 : -5e-8));
    else if (kind === 2) toE7Inputs.push(Math.round((random() * 2 - 1) * 1.8e9) / 1e7);
    else toE7Inputs.push(String((random() * 2 - 1) * 100));
  }
  const toE7 = toE7Inputs.map(input => ({ input, lat: latToE7(input), lon: lonToE7(input) }));

  const randLatE7 = () => {
    const r = random();
    if (r < 0.05) return pick([900000000, -900000000, 0]);
    return Math.round((random() * 2 - 1) * 900000000);
  };
  const randLonE7 = () => {
    const r = random();
    if (r < 0.05) return pick([1800000000, -1800000000, 0, 1799999999, -1799999999]);
    return Math.round((random() * 2 - 1) * 1800000000);
  };
  const randBox = () => {
    const r = random();
    if (r < 0.3) {
      // small local box
      const lat = randLatE7();
      const lon = randLonE7();
      const dLat = randInt(0, 5000000);
      const dLon = randInt(0, 5000000);
      return {
        minLatE7: Math.max(-900000000, lat - dLat),
        maxLatE7: Math.min(900000000, lat + dLat),
        minLonE7: Math.max(-1800000000, lon - dLon),
        maxLonE7: Math.min(1800000000, lon + dLon)
      };
    }
    const a = randLatE7();
    const b = randLatE7();
    const c = randLonE7();
    const d = randLonE7();
    // Occasionally keep min > max on longitude (wrapped box).
    const wrap = r > 0.9;
    return {
      minLatE7: Math.min(a, b),
      maxLatE7: Math.max(a, b),
      minLonE7: wrap ? Math.max(c, d) : Math.min(c, d),
      maxLonE7: wrap ? Math.min(c, d) : Math.max(c, d)
    };
  };

  const haversine = [];
  for (let i = 0; i < 4000; i++) {
    const near = i % 3 === 0;
    const lat1 = (random() * 2 - 1) * 90;
    const lon1 = (random() * 2 - 1) * 180;
    const lat2 = near ? lat1 + (random() - 0.5) * 0.01 : (random() * 2 - 1) * 90;
    const lon2 = near ? lon1 + (random() - 0.5) * 0.01 : (random() * 2 - 1) * 180;
    const latE7A = randLatE7();
    const lonE7A = randLonE7();
    const latE7B = near ? latE7A + randInt(-100000, 100000) : randLatE7();
    const lonE7B = near ? lonE7A + randInt(-100000, 100000) : randLonE7();
    haversine.push({
      deg: [lat1, lon1, lat2, lon2],
      degOut: f(haversineMeters(lat1, lon1, lat2, lon2)),
      e7: [latE7A, lonE7A, latE7B, lonE7B],
      e7Out: f(haversineMetersE7(latE7A, lonE7A, latE7B, lonE7B))
    });
  }
  // antipodal / identical extremes
  for (const [a, b, c, d] of [[0, 0, 0, 1800000000], [900000000, 0, -900000000, 0], [0, 0, 0, 0], [450000000, 1799999999, 450000000, -1799999999]]) {
    haversine.push({ deg: [a / 1e7, b / 1e7, c / 1e7, d / 1e7], degOut: f(haversineMeters(a / 1e7, b / 1e7, c / 1e7, d / 1e7)), e7: [a, b, c, d], e7Out: f(haversineMetersE7(a, b, c, d)) });
  }

  const radius = [];
  const radii = [0, -5, 1, 10, 100, 999.5, 1000, 25000, 100000, 500000, 2e6, 5e6, 1e7, 2.1e7, 4e7, Infinity, NaN];
  for (let i = 0; i < 4000; i++) {
    const latE7 = i % 10 === 0 ? pick([900000000, -900000000, 899990000, -899990000, 0]) : randLatE7();
    const lonE7 = i % 7 === 0 ? pick([1800000000, -1800000000, 1799990000, -1799990000, 0]) : randLonE7();
    const r = i < radii.length * 5 ? radii[i % radii.length] : (random() < 0.8 ? random() * 50000 : random() * 5e6);
    radius.push({ latE7, lonE7, radius: f(r), out: boxesForRadiusE7(latE7, lonE7, r) });
  }
  // radius passed as non-number values
  for (const r of ["500", "abc", null, ""]) {
    radius.push({ latE7: 455000000, lonE7: -735000000, radius: r, raw: true, out: boxesForRadiusE7(455000000, -735000000, r) });
  }

  const boxes = [];
  for (let i = 0; i < 4000; i++) {
    const a = randBox();
    const b = i % 4 === 0 ? { ...a, minLatE7: a.minLatE7 + randInt(0, 100), maxLatE7: a.maxLatE7 - randInt(0, 100) } : randBox();
    const latE7 = i % 5 === 0 ? pick([a.minLatE7, a.maxLatE7]) : randLatE7();
    const lonE7 = i % 6 === 0 ? pick([a.minLonE7, a.maxLonE7]) : randLonE7();
    boxes.push({
      a,
      b,
      latE7,
      lonE7,
      intersects: boxIntersectsE7(a, b),
      contains: boxContainsBoxE7(a, b),
      containsPoint: boxContainsPointE7(a, latE7, lonE7),
      minDist: f(pointToBoxDistanceMetersE7(latE7, lonE7, a)),
      maxDist: f(pointToBoxMaxDistanceMetersE7(latE7, lonE7, a))
    });
  }
  counts.toE7 = toE7.length;
  counts.haversine = haversine.length;
  counts.radius = radius.length;
  counts.boxes = boxes.length;
  write("math", { toE7, haversine, radius: deepF(radius), boxes });
}

// ------------------------------------------------------------------- tree
{
  const blockFilters = [
    { name: "category", kind: "facet", words: 2 },
    { name: "rating", kind: "number", type: "float" },
    { name: "open", kind: "boolean", type: "boolean" },
    { name: "kind", kind: "facet", words: 1 }
  ];
  const packTable = ["0000.aaa.bin", "0001.bbb.bin", "0002.ééé.bin"];
  const packIndexes = new Map(packTable.map((pack, index) => [pack, index]));

  function makePoints(count, seedShift) {
    const lats = new Int32Array(count);
    const lons = new Int32Array(count);
    const docs = new Uint32Array(count);
    for (let i = 0; i < count; i++) {
      const r = random();
      let lat;
      let lon;
      if (r < 0.03) {
        lat = pick([90, -90, 89.9999999, -89.9999999]);
        lon = (random() * 2 - 1) * 180;
      } else if (r < 0.06) {
        lat = (random() * 2 - 1) * 80;
        lon = pick([180, -180, 179.9999999, -179.9999999]);
      } else if (r < 0.5) {
        lat = 45.5 + (random() - 0.5) * 0.5;
        lon = -73.6 + (random() - 0.5) * 0.5;
      } else {
        lat = (random() * 2 - 1) * 90;
        lon = (random() * 2 - 1) * 180;
      }
      lats[i] = latToE7(lat);
      lons[i] = lonToE7(lon);
      docs[i] = r < 0.2 ? i + seedShift : Math.floor(random() * (r < 0.3 ? 200 : 4294967295));
    }
    // duplicates
    for (let i = 0; i + 1 < count; i += 37) {
      lats[i + 1] = lats[i];
      lons[i + 1] = lons[i];
    }
    return { lats, lons, docs };
  }

  function randomSummary() {
    const roll = random();
    if (roll < 0.15) return undefined;
    return {
      category: { words: [randInt(0, 2 ** 32 - 1), randInt(0, 255)] },
      rating: roll < 0.3 ? { min: null, max: null } : { min: Math.round(random() * 50) / 10, max: 5 + random() },
      open: { min: randInt(0, 1), max: randInt(0, 1) },
      kind: roll < 0.4 ? { words: [1, 2, 3] } : { words: [randInt(0, 1000000)] }
    };
  }

  function randomEntry(leaf) {
    const roll = random();
    return {
      pack: pick(packTable),
      offset: leaf.start * 13 + randInt(0, 1000) + (roll < 0.1 ? 2 ** 40 : 0),
      length: randInt(1, 100000),
      physicalLength: roll < 0.5 ? randInt(1, 100000) : undefined,
      logicalLength: roll < 0.3 ? 0 : randInt(1, 1000000),
      checksum: roll < 0.25 ? null : roll < 0.4 ? { algorithm: "", value: "ab".repeat(32) } : { algorithm: "sha256", value: randInt(0, 1e9).toString(16).padStart(64, "0") }
    };
  }

  const capsuleValue = (row, field) => {
    const roll = random();
    if (field === "title") return pick(["Café Olimpico", "北京烤鸭", "𝒜 astral 😀", "", "plain", `Row ${row}`, " line"]);
    if (field === "rating") return roll < 0.3 ? null : roll < 0.6 ? Math.round(random() * 50) / 10 : roll < 0.8 ? randInt(-5, 1e9) : random() * 1e-5;
    if (field === "tags") return roll < 0.3 ? undefined : roll < 0.6 ? ["a", "b€", ""] : [];
    if (field === "open") return roll < 0.3 ? true : roll < 0.6 ? false : null;
    if (field === "geo") {
      return roll < 0.3
        ? { b: 1, a: [1, "x", null], 10: "ten", 2: "two", nested: { z: 1.5, 1: true } }
        : roll < 0.6 ? [1, 2.5, "three", { k: false }] : roll < 0.8 ? { big: 1e21, small: 1e-7, neg: -12.75 } : undefined;
    }
    if (field === "index") return roll < 0.5 ? `idx-${row}` : undefined;
    if (field === "7") return roll < 0.5 ? "seven" : undefined;
    if (field === "02") return roll < 0.5 ? "zero-two" : undefined;
    return roll < 0.5 ? `v${row}` : undefined;
  };

  function capsulesFor(leaf, fields) {
    const rows = [];
    for (let row = 0; row < leaf.count; row++) {
      const doc = {};
      for (const field of fields) {
        const value = capsuleValue(row, field);
        if (value !== undefined) doc[field] = value;
      }
      rows.push(doc);
    }
    return rows;
  }

  const roots = [];
  const branchPages = [];
  const leafPages = [];

  const configs = [
    { count: 3000, leafSize: 64, filters: blockFilters, levels: 1, capsuleFields: null },
    { count: 2500, leafSize: 32, filters: [], levels: 1, capsuleFields: ["title", "rating"] },
    { count: 4000, leafSize: 48, filters: blockFilters, levels: 2, branchSize: 8, capsuleFields: ["title", "rating", "tags", "open", "geo"] },
    { count: 1500, leafSize: 16, filters: [], levels: 2, branchSize: 5, capsuleFields: ["7", "title", "index", "02", "title"] },
    { count: 10, leafSize: 16, filters: blockFilters.slice(1, 3), levels: 1, capsuleFields: ["title"] },
    { count: 0, leafSize: 16, filters: [], levels: 1, capsuleFields: null }
  ];

  configs.forEach((config, configIndex) => {
    const field = configIndex === 3 ? "lieu_géo" : "location";
    const { lats, lons, docs } = makePoints(config.count, configIndex * 1000);
    const leaves = buildGeoTreeLeaves(lats, lons, docs, config.leafSize);
    const withEntries = leaves.map(leaf => ({ ...leaf, entry: randomEntry(leaf), summary: randomSummary() }));
    const bbox = leaves.length
      ? {
          minLatE7: Math.min(...leaves.map(leaf => leaf.minLatE7)),
          maxLatE7: Math.max(...leaves.map(leaf => leaf.maxLatE7)),
          minLonE7: Math.min(...leaves.map(leaf => leaf.minLonE7)),
          maxLonE7: Math.max(...leaves.map(leaf => leaf.maxLonE7))
        }
      : { minLatE7: -5, maxLatE7: 7, minLonE7: -9, maxLonE7: 11 };

    // leaf pages (sample up to 25 per config to keep fixtures compact)
    const step = Math.max(1, Math.floor(leaves.length / 25));
    for (let i = 0; i < leaves.length; i += step) {
      const leaf = leaves[i];
      const useCapsules = config.capsuleFields && i % (2 * step) === 0;
      const options = useCapsules ? { capsuleFields: config.capsuleFields, capsules: capsulesFor(leaf, config.capsuleFields) } : {};
      const buffer = encodeGeoLeafPage(field, lats, lons, docs, leaf, options);
      const decoded = decodeGeoLeafPage(buffer, { name: field });
      leafPages.push({
        bytes: b64(buffer),
        expected: field,
        out: {
          ...decoded,
          latsE7: [...decoded.latsE7],
          lonsE7: [...decoded.lonsE7],
          docs: [...decoded.docs]
        }
      });
    }

    let rootBuffer;
    if (config.levels === 1) {
      rootBuffer = encodeGeoTreeRoot({
        field,
        total: config.count,
        leafSize: config.leafSize,
        bbox,
        leaves: withEntries,
        packTable,
        packIndexes,
        blockFilters: config.filters
      }).buffer;
    } else {
      const branches = [];
      for (let start = 0; start < withEntries.length; start += config.branchSize) {
        const slice = withEntries.slice(start, start + config.branchSize);
        const branchBox = {
          minLatE7: Math.min(...slice.map(leaf => leaf.minLatE7)),
          maxLatE7: Math.max(...slice.map(leaf => leaf.maxLatE7)),
          minLonE7: Math.min(...slice.map(leaf => leaf.minLonE7)),
          maxLonE7: Math.max(...slice.map(leaf => leaf.maxLonE7))
        };
        const branchIndex = branches.length;
        const page = encodeGeoBranchPage({
          field,
          branchIndex,
          firstLeafIndex: start,
          bbox: branchBox,
          leaves: slice,
          packIndexes,
          blockFilters: config.filters
        });
        branchPages.push({
          bytes: b64(page),
          filters: config.filters,
          packTable,
          expected: field,
          out: decodeGeoBranchPage(page, packTable, config.filters, { name: field })
        });
        branches.push({
          ...branchBox,
          count: slice.reduce((sum, leaf) => sum + leaf.count, 0),
          firstLeafIndex: start,
          leafCount: slice.length,
          entry: randomEntry({ start: branchIndex }),
          summary: config.filters.length ? mergeBlockFilterSummaries(config.filters, slice.map(leaf => leaf.summary)) : null
        });
      }
      rootBuffer = encodeGeoTreeRoot({
        field,
        total: config.count,
        leafSize: config.leafSize,
        leafCount: withEntries.length,
        bbox,
        branches,
        packTable,
        packIndexes,
        blockFilters: config.filters
      }).buffer;
    }
    roots.push({ bytes: b64(rootBuffer), filters: config.filters, out: parseGeoTreeRoot(rootBuffer, config.filters) });
  });

  // Error cases: corrupt magic, versions, levels, trailing bytes, truncation,
  // field mismatch, missing filter manifest.
  const errors = [];
  const sampleRoot = Buffer.from(roots[0].bytes, "base64");
  const sampleRoot2 = Buffer.from(roots[2].bytes, "base64");
  const sampleBranch = Buffer.from(branchPages[0].bytes, "base64");
  const sampleLeaf = Buffer.from(leafPages[0].bytes, "base64");
  const capsuleLeaf = Buffer.from(leafPages.find(page => page.out.capsules).bytes, "base64");
  const mutate = (buffer, index, value) => {
    const copy = Buffer.from(buffer);
    copy[index] = value;
    return copy;
  };
  const rootCase = (bytes, filters) => errors.push({ kind: "root", bytes: b64(bytes), filters, out: capture(() => { parseGeoTreeRoot(bytes, filters); return true; }) });
  const branchCase = (bytes, filters, expected) => errors.push({ kind: "branch", bytes: b64(bytes), filters, packTable, expected, out: capture(() => { decodeGeoBranchPage(bytes, packTable, filters, { name: expected }); return true; }) });
  const leafCase = (bytes, expected) => errors.push({ kind: "leaf", bytes: b64(bytes), expected, out: capture(() => { decodeGeoLeafPage(bytes, { name: expected }); return true; }) });

  rootCase(mutate(sampleRoot, 0, 0x00), blockFilters);
  rootCase(mutate(sampleRoot, 4, 9), blockFilters);
  rootCase(Buffer.concat([sampleRoot, Buffer.from([0])]), blockFilters);
  rootCase(sampleRoot, []);
  rootCase(sampleRoot2, []);
  rootCase(Buffer.from([0x52, 0x46]), []);
  // levels byte corrupted: field "location" (len 8) → pos 5..14, then varints.
  {
    // Build a tiny root and patch its levels byte.
    const tiny = encodeGeoTreeRoot({ field: "f", total: 0, leafSize: 16, bbox: { minLatE7: 0, maxLatE7: 0, minLonE7: 0, maxLonE7: 0 }, leaves: [], packTable: [], packIndexes: new Map() }).buffer;
    const levelsAt = tiny.length - 3; // levels, hasSummaries, leafCount
    rootCase(tiny, []);
    rootCase(mutate(tiny, levelsAt, 3), []);
    rootCase(mutate(tiny, levelsAt, 0), []);
    rootCase(tiny.subarray(0, levelsAt), []);
  }
  for (let cut = 4; cut < sampleRoot.length; cut += Math.max(1, Math.floor(sampleRoot.length / 40))) {
    rootCase(sampleRoot.subarray(0, cut), blockFilters);
  }
  branchCase(mutate(sampleBranch, 3, 0x41), branchPages[0].filters, branchPages[0].expected);
  branchCase(mutate(sampleBranch, 4, 2), branchPages[0].filters, branchPages[0].expected);
  branchCase(sampleBranch, branchPages[0].filters, "other");
  branchCase(sampleBranch, [], branchPages[0].expected);
  branchCase(Buffer.concat([sampleBranch, Buffer.from([1, 2])]), branchPages[0].filters, branchPages[0].expected);
  for (let cut = 4; cut < sampleBranch.length; cut += Math.max(1, Math.floor(sampleBranch.length / 30))) {
    branchCase(sampleBranch.subarray(0, cut), branchPages[0].filters, branchPages[0].expected);
  }
  leafCase(mutate(sampleLeaf, 0, 0x51), "location");
  leafCase(mutate(sampleLeaf, 4, 3), "location");
  leafCase(sampleLeaf, "elsewhere");
  leafCase(sampleLeaf, "");
  leafCase(Buffer.concat([sampleLeaf, Buffer.from([0])]), "location");
  for (let cut = 20; cut < sampleLeaf.length; cut += Math.max(1, Math.floor(sampleLeaf.length / 30))) {
    leafCase(sampleLeaf.subarray(0, cut), "location");
  }
  leafCase(Buffer.concat([capsuleLeaf, Buffer.from([0])]), "");
  leafCase(capsuleLeaf.subarray(0, capsuleLeaf.length - 1), "");

  // Capsule payload edge cases built by hand: wrong row count, bad value
  // type, bad payload version, field-count mismatch.
  {
    const lats = new Int32Array([1, 2]);
    const lons = new Int32Array([3, 4]);
    const docs = new Uint32Array([5, 6]);
    const leaf = { start: 0, end: 2, count: 2, minLatE7: 1, maxLatE7: 2, minLonE7: 3, maxLonE7: 4 };
    const good = encodeGeoLeafPage("f", lats, lons, docs, leaf, { capsuleFields: ["a"], capsules: [{ a: 1 }, { a: "x" }] });
    leafCase(good, "f");
    // Payload starts after header+body; locate RFPC magic.
    const at = good.indexOf(Buffer.from([0x52, 0x46, 0x50, 0x43]));
    leafCase(mutate(good, at + 4, 2), "f"); // payload version
    leafCase(mutate(good, at + 5, 1), "f"); // row count 1 → mismatch / trailing
    leafCase(mutate(good, at + 5, 3), "f"); // row count 3
    leafCase(mutate(good, at + 6, 2), "f"); // field count mismatch
    leafCase(mutate(good, at + 7, 9), "f"); // unknown value type
    leafCase(mutate(good, at + 3, 0x00), "f"); // payload magic
  }

  counts.roots = roots.length;
  counts.branchPages = branchPages.length;
  counts.leafPages = leafPages.length;
  counts.treeErrors = errors.length;
  write("tree", { roots, branchPages, leafPages, errors });
}

// ------------------------------------------------------------------ cells
{
  const cellForE7 = [];
  for (let i = 0; i < 3000; i++) {
    const latE7 = i % 9 === 0 ? pick([900000000, -900000000, 850511287, -850511288, 0]) : Math.round((random() * 2 - 1) * 900000000);
    const lonE7 = i % 11 === 0 ? pick([1800000000, -1800000000, 0, 1799999999]) : Math.round((random() * 2 - 1) * 1800000000);
    const zoom = i % 13 === 0 ? pick([-3, 0, 1.7, 22, 23, 30, NaN]) : randInt(0, 22);
    cellForE7.push({ latE7, lonE7, zoom: f(zoom), out: geoCellForE7(latE7, lonE7, zoom) });
  }

  const cellBlock = [];
  for (let i = 0; i < 2000; i++) {
    const cell = cellForE7[i % cellForE7.length].out;
    const blockZoom = i % 7 === 0 ? pick([-1, 0, 2.9, 25, NaN]) : randInt(0, 16);
    cellBlock.push({ cell, blockZoom: f(blockZoom), out: geoCellBlock(cell, blockZoom) });
  }

  const blockKeys = [];
  const names = ["location", "lieu géo", "a|b", "100%", "x y/z?&=#", "😀", "tag-_.!~*'()", "", "Ünïcödé"];
  for (let i = 0; i < 1500; i++) {
    const field = pick(names);
    const facet = pick(names);
    const group = i % 10 === 0 ? pick([-4, 0, 999999, 1234567]) : randInt(0, 5000);
    const block = cellBlock[i % cellBlock.length].out;
    blockKeys.push({ field, facet, group, block, out: geoCellBlockKey(field, facet, group, block) });
  }
  const routeKeys = [];
  for (let i = 0; i < 500; i++) {
    const args = [randInt(0, 22), randInt(0, 4194303), randInt(0, 4194303), randInt(0, 100000)];
    routeKeys.push({ args, out: geoCellRouteKey(...args) });
  }

  const forBoxes = [];
  for (let i = 0; i < 1500; i++) {
    const latE7 = Math.round((random() * 2 - 1) * 900000000);
    const lonE7 = Math.round((random() * 2 - 1) * 1800000000);
    const radius = pick([50, 500, 2000, 10000, 80000, 400000, 3e6]);
    let boxes = boxesForRadiusE7(latE7, lonE7, radius);
    if (i % 5 === 0) boxes = boxes.concat(boxesForRadiusE7(latE7 + 1000000, lonE7 - 2000000, radius));
    if (i % 17 === 0) boxes = [];
    const zoom = randInt(0, 16);
    const limit = pick([Infinity, Infinity, 1, 4, 48, 200, 1000, 0, -3, NaN]);
    let out;
    // Unlimited queries are only safe for small coverings.
    if (limit === Infinity) {
      const probe = geoCellsForBoxes(boxes, zoom, 5000);
      out = probe === null ? "skip" : geoCellsForBoxes(boxes, zoom);
    } else {
      out = geoCellsForBoxes(boxes, zoom, limit);
    }
    if (out === "skip") continue;
    forBoxes.push({ boxes, zoom, limit: f(limit), out });
  }

  const blocks = [];
  for (let i = 0; i < 300; i++) {
    const routes = [];
    const routeCount = i === 0 ? 0 : randInt(1, 12);
    for (let r = 0; r < routeCount; r++) {
      const members = [];
      const memberCount = randInt(0, 8);
      for (let m = 0; m < memberCount; m++) {
        const ordinals = [];
        const ordinalCount = randInt(0, 6);
        for (let o = 0; o < ordinalCount; o++) ordinals.push(randInt(0, random() < 0.1 ? 1e7 : 600));
        members.push({ leaf: randInt(0, random() < 0.2 ? 5 : 1e6), ordinals });
      }
      routes.push({
        zoom: randInt(0, 22),
        x: randInt(0, 4194303),
        y: randInt(0, 4194303),
        code: randInt(0, 300),
        members
      });
    }
    // duplicate route keys overwrite earlier entries
    if (i % 10 === 1 && routes.length) routes.push({ ...routes[0], members: [{ leaf: 1, ordinals: [9] }] });
    const buffer = encodeGeoCellBlock({ blockZoom: randInt(0, 16), blockX: randInt(0, 65535), blockY: randInt(0, 65535), routes });
    const decoded = decodeGeoCellBlock(buffer);
    blocks.push({
      bytes: b64(buffer),
      out: {
        ...decoded,
        routes: [...decoded.routes.entries()].map(([key, members]) => [key, [...members.entries()]])
      }
    });
  }
  const blockErrors = [];
  {
    const sample = Buffer.from(blocks[5].bytes, "base64");
    const cases = [
      Buffer.from(sample).fill(0, 0, 1),
      Buffer.concat([sample.subarray(0, 4), Buffer.from([2]), sample.subarray(5)]),
      Buffer.concat([sample, Buffer.from([7])]),
      sample.subarray(0, 3)
    ];
    for (let cut = 5; cut < sample.length; cut += 3) cases.push(sample.subarray(0, cut));
    for (const bytes of cases) {
      blockErrors.push({ bytes: b64(bytes), out: capture(() => { decodeGeoCellBlock(bytes); return true; }) });
    }
  }

  counts.cellForE7 = cellForE7.length;
  counts.cellBlock = cellBlock.length;
  counts.blockKeys = blockKeys.length;
  counts.routeKeys = routeKeys.length;
  counts.forBoxes = forBoxes.length;
  counts.cellBlocks = blocks.length;
  counts.cellBlockErrors = blockErrors.length;
  write("cells", deepF({ cellForE7, cellBlock, blockKeys, routeKeys, forBoxes, blocks, blockErrors }));
}

// ------------------------------------------------------------------ route
{
  function randomPath() {
    const roll = random();
    const n = randInt(2, 25);
    let lat;
    let lon;
    if (roll < 0.15) {
      lat = (random() * 2 - 1) * 60;
      lon = pick([179.5, -179.5, 179.99]);
    } else if (roll < 0.25) {
      lat = pick([89.5, -89.5, 88]);
      lon = (random() * 2 - 1) * 180;
    } else if (roll < 0.7) {
      lat = 45.5 + (random() - 0.5);
      lon = -73.6 + (random() - 0.5);
    } else {
      lat = (random() * 2 - 1) * 70;
      lon = (random() * 2 - 1) * 180;
    }
    const scale = pick([0.0005, 0.01, 0.1, 0.5, 2]);
    const points = [];
    for (let i = 0; i < n; i++) {
      points.push({ lat: Math.round(lat * 1e6) / 1e6, lon: Math.round(lon * 1e6) / 1e6 });
      if (random() < 0.08) points.push({ ...points.at(-1) });
      lat = Math.max(-90, Math.min(90, lat + (random() - 0.5) * scale));
      lon += (random() - 0.5) * scale;
      if (lon > 180) lon -= 360;
      if (lon < -180) lon += 360;
    }
    return points;
  }

  const routeInputs = [];
  const corridorChoices = [undefined, 50, 400, 1000, 1500, 25000, 100000];
  for (let i = 0; i < 260; i++) {
    const points = randomPath();
    const form = i % 8;
    let route;
    let options = {};
    if (form === 0) route = points;
    else if (form === 1) route = points.map(point => [point.lon, point.lat]);
    else if (form === 2) route = points.map(point => ({ lat: point.lat, lng: point.lon }));
    else if (form === 3) {
      const precision = pick([5, 6, 7]);
      route = encodePolyline(points, precision);
      options = precision === 5 ? {} : random() < 0.5 ? { precision } : { polylinePrecision: precision };
    } else if (form === 4) route = { type: "LineString", coordinates: points.map(point => [point.lon, point.lat]) };
    else if (form === 5) {
      const mid = Math.max(1, Math.floor(points.length / 2));
      route = {
        type: "Feature",
        properties: {},
        geometry: { type: "MultiLineString", coordinates: [points.slice(0, mid), points.slice(mid)].map(line => line.map(point => [point.lon, point.lat])) }
      };
    } else if (form === 6) {
      route = {
        type: "FeatureCollection",
        features: [
          { type: "Feature", geometry: { type: "LineString", coordinates: points.map(point => [point.lon, point.lat]) } },
          { type: "Feature", geometry: { type: "GeometryCollection", geometries: [{ type: "LineString", coordinates: [[points.at(-1).lon, points.at(-1).lat], [points[0].lon, points[0].lat]] }] } }
        ]
      };
    } else route = points.map(point => ({ lat: String(point.lat), lon: point.lon }));
    const corridor = pick(corridorChoices);
    if (corridor !== undefined) options.corridorMeters = corridor;
    routeInputs.push({ route, options });
  }
  // Error and edge inputs.
  const edgeInputs = [
    { route: [{ lat: 45, lon: -73 }], options: {} },
    { route: [{ lat: 45, lon: -73 }, { lat: 45, lon: -73 }], options: {} },
    { route: [{ lat: 90, lon: 0 }, { lat: 90, lon: 50 }], options: {} },
    { route: [{ lat: 90, lon: 0 }, { lat: 90, lon: 50 }, { lat: 89, lon: 50 }], options: {} },
    { route: [{ lat: 91, lon: 0 }, { lat: 0, lon: 0 }], options: {} },
    { route: [{ lat: 0, lon: 181 }, { lat: 0, lon: 0 }], options: {} },
    { route: [{ lat: null, lon: 1 }, { lat: 1, lon: 1 }], options: {} },
    { route: [{ lon: 1 }, { lat: 1, lon: 1 }], options: {} },
    { route: [{ lat: 1, lon: null, lng: 2 }, { lat: 1, lon: 1 }], options: {} },
    { route: [{ lat: 1, lng: 2 }, { lat: "1.5", lon: [3] }], options: {} },
    { route: [[1, 2], [3]], options: {} },
    { route: [[1, 2], [3, 4, 5]], options: {} },
    { route: [[1, 2], [3, 4]], options: { corridorMeters: 0 } },
    { route: [[1, 2], [3, 4]], options: { corridorMeters: -5 } },
    { route: [[1, 2], [3, 4]], options: { corridorMeters: 100001 } },
    { route: [[1, 2], [3, 4]], options: { corridorMeters: "abc" } },
    { route: [[1, 2], [3, 4]], options: { corridorMeters: "750" } },
    { route: [[1, 2], [3, 4]], options: { corridorMeters: null } },
    { route: [[179.9, 10], [-179.9, 10.1]], options: { corridorMeters: 5000 } },
    { route: [[-179.9, 10], [179.9, 10.1]], options: { corridorMeters: 5000 } },
    { route: [[180, 0], [-180, 0], [-179, 0]], options: {} },
    { route: "_p~iF~ps|U_ulLnnqC_mqNvxq`@", options: {} },
    { route: "_p~iF~ps|U_ulLnnqC_mqNvxq`@", options: { precision: 6 } },
    { route: "_p~iF~ps|U_ulLnnqC_mqNvxq`@", options: { precision: 0 } },
    { route: "_p~iF~ps|U_ulLnnqC_mqNvxq`@", options: { precision: 12 } },
    { route: "_p~iF~ps|U_ulLnnqC_mqNvxq`@", options: { precision: null, polylinePrecision: 6 } },
    { route: "_p~iF~ps|U_ulLnnqC_mqNvxq", options: {} },
    { route: "_p~iF~ps|U_ulLnnqC_mqNvxq`", options: {} },
    { route: "_p~iF ~ps|U", options: {} },
    { route: "_p~iF~ps|Ué", options: {} },
    { route: "~~~~~~~~~~~?", options: {} },
    { route: "", options: {} },
    { route: { type: "Point", coordinates: [1, 2] }, options: {} },
    { route: null, options: {} },
    { route: 42, options: {} },
    { route: { type: "Feature", geometry: null }, options: {} },
    { route: { type: "MultiLineString", coordinates: [[[1, 2], [1, 2]], [[1, 2], [3, 4]]] }, options: {} },
    { route: { type: "GeometryCollection", geometries: [] }, options: {} }
  ];
  const routes = [];
  const allInputs = [...edgeInputs, ...routeInputs];
  let matchCount = 0;
  let cellsCount = 0;
  for (const input of allInputs) {
    let prepared;
    let error = null;
    try {
      prepared = prepareRoute(input.route, input.options);
    } catch (e) {
      error = e.message;
    }
    const entry = { input, error };
    if (!error) {
      entry.prepared = prepared;
      const matches = [];
      const anchor = prepared.points[Math.floor(random() * prepared.points.length)];
      for (let q = 0; q < 40; q++) {
        let lat;
        let lon;
        if (q < 20) {
          lat = anchor.lat + (random() - 0.5) * pick([0.001, 0.05, 1]);
          lon = anchor.lon + (random() - 0.5) * pick([0.001, 0.05, 1]);
          lat = Math.max(-90, Math.min(90, lat));
          if (lon > 180) lon -= 360;
          if (lon < -180) lon += 360;
        } else if (q < 36) {
          lat = (random() * 2 - 1) * 90;
          lon = (random() * 2 - 1) * 180;
        } else if (q === 36) {
          lat = prepared.points[0].lat;
          lon = prepared.points[0].lon;
        } else if (q === 37) {
          lat = 95;
          lon = 0;
        } else if (q === 38) {
          lat = NaN;
          lon = 0;
        } else {
          lat = prepared.points.at(-1).lat;
          lon = prepared.points.at(-1).lon;
        }
        let match;
        let matchError = null;
        try {
          match = matchPointToRoute(prepared, lat, lon);
        } catch (e) {
          matchError = e.message;
        }
        matches.push({ lat: f(lat), lon: f(lon), error: matchError, match: matchError ? null : match, public: matchError ? null : routeMatchPublic(match) });
        matchCount++;
      }
      entry.matches = matches;
      const cells = [];
      for (const zoom of [2, 6, 10, 13, 16]) {
        for (const limit of [48, 400, Infinity]) {
          if (limit === Infinity && geoCellsForRoute(prepared, zoom, 600) === null) continue;
          cells.push({ zoom, limit: f(limit), out: geoCellsForRoute(prepared, zoom, limit) });
          cellsCount++;
        }
      }
      entry.cells = cells;
    }
    routes.push(entry);
  }
  counts.routes = routes.length;
  counts.routeMatches = matchCount;
  counts.routeCells = cellsCount;
  write("route", deepF(routes));
}

// ------------------------------------------------------------------ shard
{
  const bboxes = [
    [37.3, 127, 82.1, -169],
    [-90, -180, 90, 180],
    [45, -74, 46, -73],
    [10, 170, 20, -170],
    [0, 0, 0, 0],
    [-60, 100, -10, 179.9999]
  ];
  for (let i = 0; i < 60; i++) {
    const a = (random() * 2 - 1) * 90;
    const b = (random() * 2 - 1) * 90;
    const c = (random() * 2 - 1) * 180;
    const d = (random() * 2 - 1) * 180;
    bboxes.push([Math.min(a, b), c, Math.max(a, b), d]);
  }
  const distance = [];
  const intersects = [];
  for (let i = 0; i < 4000; i++) {
    const bbox = bboxes[i % bboxes.length];
    const lat = i % 50 === 0 ? pick([90, -90, 95, NaN]) : (random() * 2 - 1) * 90;
    const lon = i % 60 === 0 ? pick([180, -180, 200, -0.0000001]) : (random() * 2 - 1) * 180;
    distance.push({ bbox, lat: f(lat), lon: f(lon), out: f(shardBboxDistanceMeters(bbox, lat, lon)) });
    const box = {
      minLat: (random() * 2 - 1) * 90,
      maxLat: (random() * 2 - 1) * 90,
      minLon: (random() * 2 - 1) * 180,
      maxLon: (random() * 2 - 1) * 180
    };
    if (i % 3 === 0) box.maxLon = box.minLon + random() * 5;
    if (i % 40 === 0) box.minLat = NaN;
    intersects.push({ bbox, box: deepF(box), out: shardBoxIntersects(bbox, box) });
  }
  counts.shardDistance = distance.length;
  counts.shardIntersects = intersects.length;
  write("shard", { distance, intersects });
}

write("counts", counts);
console.log(`geo fixtures written to ${outDir}`);
console.log(JSON.stringify(counts));
