import assert from "node:assert/strict";
import test from "node:test";
import { closeSync, existsSync, mkdtempSync, openSync, readFileSync, readdirSync, rmSync, writeFileSync, writeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import {
  MinHeap,
  bucketWeight,
  decodeRouteCell,
  decodeRouteOverlay,
  decodeRouteTopSlice,
  encodeRouteCell,
  encodeRouteOverlay,
  encodeRouteTopSlice
} from "../src/route_graph.js";
import { IntegerRadixHeap, adaptiveTopMaxCells, buildRouteGraph } from "../src/route_graph_build.js";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { decodeRoutePortalIds, decodeRoutePortalRecords } from "../src/route_portals.js";

// Deterministic LCG so the synthetic graph is stable across runs.
function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

test("integer radix heap orders duplicate and 53-bit monotone distances", () => {
  const heap = new IntegerRadixHeap();
  for (const [weight, value] of [
    [2 ** 40 + 7, "large"],
    [5, "five-a"],
    [2 ** 32 + 1, "wide"],
    [5, "five-b"],
    [2 ** 48 + 3, "largest"]
  ]) heap.push(weight, value);

  const weights = [];
  while (heap.size) {
    weights.push(heap.peekWeight());
    heap.pop();
  }
  assert.deepEqual(weights, [5, 5, 2 ** 32 + 1, 2 ** 40 + 7, 2 ** 48 + 3]);

  heap.clear();
  heap.push(20, "twenty");
  assert.equal(heap.peekWeight(), 20);
  heap.pop();
  assert.throws(() => heap.push(19, "past"), /monotone safe-integer/);
});

test("country hierarchy widens only when cell working sets would exceed the mobile target", () => {
  assert.equal(adaptiveTopMaxCells({ nodeCount: 672, leafCount: 14, fanout: 4, configured: 6 }), 6);
  assert.equal(adaptiveTopMaxCells({ nodeCount: 30_642_545, leafCount: 32_768, fanout: 8, configured: 8 }), 512);
  assert.equal(adaptiveTopMaxCells({ nodeCount: 30_642_545, leafCount: 32_768, fanout: 8, configured: 1024 }), 1024);
});

// Grid road network around Montreal-ish coordinates. Every neighbor pair is
// connected in both directions with independent random weights, which keeps
// the graph strongly connected while still exercising directedness.
function syntheticGraph(width, height, seed = 42) {
  const random = lcg(seed);
  const nodeCount = width * height;
  const nodeLat = new Int32Array(nodeCount);
  const nodeLon = new Int32Array(nodeCount);
  const baseLat = 45.5e7;
  const baseLon = -73.6e7;
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const index = y * width + x;
      nodeLat[index] = baseLat + y * 9000 + Math.floor(random() * 2000);
      nodeLon[index] = baseLon + x * 12000 + Math.floor(random() * 2000);
    }
  }
  const edgeFrom = [];
  const edgeTo = [];
  const edgeWeightDs = [];
  const edgeDistDm = [];
  const edgeName = [];
  const geomOffsets = [0];
  const geomBytes = [];
  const addEdge = (from, to) => {
    edgeFrom.push(from);
    edgeTo.push(to);
    edgeWeightDs.push(10 + Math.floor(random() * 600));
    edgeDistDm.push(500 + Math.floor(random() * 8000));
    edgeName.push(1 + ((from + to) % 3));
    geomBytes.push(0); // varint 0: no interior geometry points
    geomOffsets.push(geomBytes.length);
  };
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const index = y * width + x;
      if (x + 1 < width) {
        addEdge(index, index + 1);
        addEdge(index + 1, index);
      }
      if (y + 1 < height) {
        addEdge(index, index + width);
        addEdge(index + width, index);
      }
    }
  }
  return {
    nodeLat,
    nodeLon,
    edgeFrom: Uint32Array.from(edgeFrom),
    edgeTo: Uint32Array.from(edgeTo),
    edgeWeightDs: Uint32Array.from(edgeWeightDs),
    edgeDistDm: Uint32Array.from(edgeDistDm),
    edgeName: Uint32Array.from(edgeName),
    edgeClass: Uint8Array.from(edgeFrom.map((_, i) => i % 2)),
    geomOffsets: Uint32Array.from(geomOffsets),
    geomBytes: Uint8Array.from(geomBytes),
    names: ["", "Rue A", "Rue B", "Rue C"],
    profile: "car",
    classes: ["arterial", "local"]
  };
}

const PEAK_BUCKET = {
  name: "peak",
  rules: [{ dayMask: 0b0111110, startHour: 7, endHour: 9 }],
  classFactors: { arterial: 1.5, local: 1.2 }
};

function buildCsr(nodeCount, from, to) {
  const rowStart = new Uint32Array(nodeCount + 1);
  for (let i = 0; i < from.length; i++) rowStart[from[i] + 1]++;
  for (let i = 0; i < nodeCount; i++) rowStart[i + 1] += rowStart[i];
  const targets = new Uint32Array(from.length);
  const edgeIds = new Uint32Array(from.length);
  const cursor = Uint32Array.from(rowStart.subarray(0, nodeCount));
  for (let i = 0; i < from.length; i++) {
    const slot = cursor[from[i]]++;
    targets[slot] = to[i];
    edgeIds[slot] = i;
  }
  return { rowStart, targets, edgeIds };
}

// Reference: plain Dijkstra over the raw graph from multi-seed sources.
function referenceSeconds(graph, csr, forwardSeeds, backwardSeeds, sameEdgeWeight) {
  const dist = new Float64Array(graph.nodeLat.length).fill(Infinity);
  const heap = new MinHeap();
  for (const seed of forwardSeeds) {
    if (seed.weight < dist[seed.node]) {
      dist[seed.node] = seed.weight;
      heap.push(seed.weight, seed.node);
    }
  }
  while (heap.size) {
    const weight = heap.peekWeight();
    const node = heap.pop();
    if (weight !== dist[node]) continue;
    for (let e = csr.rowStart[node]; e < csr.rowStart[node + 1]; e++) {
      const next = weight + graph.edgeWeightDs[csr.edgeIds[e]];
      if (next < dist[csr.targets[e]]) {
        dist[csr.targets[e]] = next;
        heap.push(next, csr.targets[e]);
      }
    }
  }
  let best = sameEdgeWeight ?? Infinity;
  for (const seed of backwardSeeds) {
    if (dist[seed.node] + seed.weight < best) best = dist[seed.node] + seed.weight;
  }
  return best / 10;
}

test("road reference preparation sorts once and marks each junction once", async () => {
  const {
    createMonotonicLookup,
    sortedUniqueAndDuplicates
  } = await import("../scripts/osm_road_graph.mjs");
  const duplicates = [];
  const unique = sortedUniqueAndDuplicates(
    Float64Array.from([9, 2, 5, 2, 9, 9, 12, 5]),
    { push: value => duplicates.push(value) }
  );
  assert.deepEqual([...unique], [2, 5, 9, 12]);
  assert.deepEqual(duplicates, [2, 5, 9]);

  const find = createMonotonicLookup(unique);
  assert.deepEqual(
    [find(1), find(2), find(4), find(5), find(12), find(20)],
    [-1, 0, -1, 1, 3, -1]
  );
  // An out-of-order PBF block resets through lower-bound lookup rather than
  // inheriting the cursor from the preceding higher-id block.
  assert.deepEqual([find(2), find(9), find(8), find(9)], [0, 2, -1, 2]);
});

test("road graph writer replaces atomically without concatenating sections", async (t) => {
  const graph = syntheticGraph(4, 3, 71);
  const edgeCount = graph.edgeFrom.length;
  graph.edgeJunction = new Uint8Array(edgeCount);
  graph.edgeSpeed = new Uint8Array(edgeCount);
  graph.edgeCond = new Uint8Array(edgeCount);
  graph.edgeSign = new Uint32Array(edgeCount);
  graph.edgeFlags = new Uint8Array(edgeCount);
  graph.laneOffsets = new Uint32Array(edgeCount + 1);
  graph.laneBytes = new Uint8Array(0);
  graph.condRules = [];
  graph.signs = [];
  graph.portals = { neighbor: [123, 455000000, -736000000] };

  const dir = mkdtempSync(join(tmpdir(), "rangefind-road-write-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "graph.bin");
  writeFileSync(path, "incomplete old graph");

  const { readRoadGraph, writeRoadGraph } = await import("../scripts/osm_road_graph.mjs");
  writeRoadGraph(path, graph);
  assert.deepEqual(readdirSync(dir), ["graph.bin"], "temporary output is renamed, not left behind");

  const decoded = await readRoadGraph(path);
  assert.equal(decoded.profile, graph.profile);
  assert.deepEqual(decoded.names, graph.names);
  assert.deepEqual([...decoded.portals.neighbor.ids], [123]);
  assert.deepEqual([...decoded.portals.neighbor.latE7], [455000000]);
  assert.deepEqual([...decoded.portals.neighbor.lonE7], [-736000000]);
  const header = JSON.parse(readFileSync(path, "utf8").split("\n", 1)[0]);
  assert.equal(header.portalColumns[0].count, 1);
  assert.equal(header.sections.some(section => section.name === "portalsBytes"), false);
  for (const key of [
    "nodeLat", "nodeLon", "edgeFrom", "edgeTo", "edgeWeightDs", "edgeDistDm",
    "edgeName", "edgeClass", "edgeJunction", "edgeSpeed", "edgeCond", "edgeSign",
    "edgeFlags", "laneOffsets", "laneBytes", "geomOffsets", "geomBytes"
  ]) {
    assert.deepEqual(decoded[key], graph[key], `${key} round trips`);
  }
});

test("road graph reader keeps legacy v8 JSON portal sections readable", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "rangefind-road-legacy-portals-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "graph.bin");
  const portals = Buffer.from(JSON.stringify({ neighbor: [123, 455000000, -736000000] }));
  const header = Buffer.from(`${JSON.stringify({
    format: "rfroutesrc-v8",
    profile: "car",
    sections: [{ name: "portalsBytes", bytes: portals.length, type: "Uint8Array" }]
  })}\n`);
  writeFileSync(path, Buffer.concat([header, portals]));
  const { readRoadGraph } = await import("../scripts/osm_road_graph.mjs");
  const decoded = await readRoadGraph(path);
  assert.deepEqual(decoded.portals, { neighbor: [123, 455000000, -736000000] });
});

test("route builds publish immutable compact federation portal sidecars", (t) => {
  const graph = syntheticGraph(4, 4, 72);
  graph.portals = { neighbor: [123, graph.nodeLat[0], graph.nodeLon[0]] };
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-portals-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 16, fanout: 4, topMaxCells: 4 });
  const manifest = JSON.parse(readFileSync(join(dir, "manifest.json"), "utf8"));
  assert.equal(manifest.portals.format, "rfrouteportals-v2");
  assert.match(manifest.portals.file, /^portals\.[a-f0-9]{24}\.bin$/u);
  assert.equal(manifest.portalCandidates, 1);
  const pack = readFileSync(join(dir, manifest.portals.file));
  const entry = manifest.portals.neighbors.neighbor;
  const ids = decodeRoutePortalIds(gunzipSync(pack.subarray(entry.ids.offset, entry.ids.offset + entry.ids.length)));
  const records = decodeRoutePortalRecords(gunzipSync(pack.subarray(entry.records.offset, entry.records.offset + entry.records.length)));
  assert.deepEqual([...ids], [123]);
  assert.deepEqual([...records.ids], [123]);
  assert.deepEqual([...records.latE7], [graph.nodeLat[0]]);
  assert.deepEqual([...records.lonE7], [graph.nodeLon[0]]);
  assert.equal(entry.ids.checksum.length, 64);
  assert.equal(entry.records.checksum.length, 64);
});

test("federation portal extraction keeps only valid neighboring OSM junction ids", async () => {
  const { collectFederationPortals } = await import("../scripts/osm_road_graph.mjs");
  const portals = collectFederationPortals({
    junctionIds: new Float64Array([11, 22, 33, 44]),
    usedIds: new Float64Array([11, 22, 33, 44]),
    graphNodeByUsed: new Int32Array([0, 1, -1, 2]),
    found: new Uint8Array([1, 1, 1, 0]),
    latE7: new Int32Array([45e7, 46e7, 47e7, 48e7]),
    lonE7: new Int32Array([-75e7, -74e7, -73e7, -72e7]),
    portalRegions: [
      { id: "west", bbox: [44, -76, 45.5, -74.5] },
      { id: "east", bbox: [45.5, -74.5, 47.5, -72.5] }
    ]
  });
  assert.deepEqual(portals.west, [11, 45e7, -75e7]);
  assert.deepEqual(portals.east, [22, 46e7, -74e7]);
});

test("road graph reader uses positional reads beyond Node's 2 GiB whole-file limit", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "rangefind-road-large-read-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "sparse.graph.bin");
  const position = 2 ** 31 + 123;
  const expected = new Uint8Array([17, 34, 51, 68]);
  const fd = openSync(path, "w+");
  try {
    writeSync(fd, expected, 0, expected.byteLength, position);
    const { readRoadGraphBytes } = await import("../scripts/osm_road_graph.mjs");
    const actual = readRoadGraphBytes(fd, new Uint8Array(expected.byteLength), position, 2);
    assert.deepEqual(actual, expected);
  } finally {
    closeSync(fd);
  }
});

test("SCC filtering writes retained edges directly into exact typed columns", async () => {
  const { filterLargestScc } = await import("../scripts/osm_road_graph.mjs");
  const graph = {
    nodeLat: new Int32Array([100, 200, 300]),
    nodeLon: new Int32Array([-100, -200, -300]),
    edgeFrom: new Uint32Array([0, 1, 2]),
    edgeTo: new Uint32Array([1, 0, 2]),
    edgeWeightDs: new Uint32Array([10, 20, 30]),
    edgeDistDm: new Uint32Array([11, 21, 31]),
    edgeName: new Uint32Array([1, 2, 3]),
    edgeClass: new Uint8Array([4, 5, 6]),
    edgeJunction: new Uint8Array([7, 8, 9]),
    edgeSpeed: new Uint8Array([40, 50, 60]),
    edgeCond: new Uint8Array([1, 2, 3]),
    edgeSign: new Uint32Array([10, 20, 30]),
    edgeFlags: new Uint8Array([0, 1, 0]),
    geomOffsets: new Uint32Array([0, 2, 3, 5]),
    geomBytes: new Uint8Array([1, 2, 3, 4, 5]),
    laneOffsets: new Uint32Array([0, 1, 3, 4]),
    laneBytes: new Uint8Array([6, 7, 8, 9]),
    names: ["", "A", "B", "isolated"],
    profile: "car",
    classes: ["road"],
    condRules: [],
    signs: []
  };
  const sourceBuffers = {
    nodeLat: graph.nodeLat.buffer,
    edgeFrom: graph.edgeFrom.buffer,
    geomBytes: graph.geomBytes.buffer,
    laneBytes: graph.laneBytes.buffer
  };
  const messages = [];
  const filtered = filterLargestScc(graph, message => messages.push(message));
  assert.deepEqual(filtered.nodeLat, new Int32Array([100, 200]));
  assert.deepEqual(filtered.edgeFrom, new Uint32Array([0, 1]));
  assert.deepEqual(filtered.edgeTo, new Uint32Array([1, 0]));
  assert.deepEqual(filtered.edgeWeightDs, new Uint32Array([10, 20]));
  assert.deepEqual(filtered.geomOffsets, new Uint32Array([0, 2, 3]));
  assert.deepEqual(filtered.geomBytes, new Uint8Array([1, 2, 3]));
  assert.deepEqual(filtered.laneOffsets, new Uint32Array([0, 1, 3]));
  assert.deepEqual(filtered.laneBytes, new Uint8Array([6, 7, 8]));
  assert.equal(filtered.nodeLat.buffer, sourceBuffers.nodeLat, "node columns compact in place");
  assert.equal(filtered.edgeFrom.buffer, sourceBuffers.edgeFrom, "edge columns compact in place");
  assert.equal(filtered.geomBytes.buffer, sourceBuffers.geomBytes, "geometry compacts in place");
  assert.equal(filtered.laneBytes.buffer, sourceBuffers.laneBytes, "lane payload compacts in place");
  assert.match(messages.at(-1), /largest 2 of 3 nodes/);
});

// Mirrors the engine's seed construction so reference and engine agree on
// snapping by construction.
function seedsFromSnap(snapResult, side) {
  return snapResult.matches.map(match => side === "forward"
    ? { node: match.toNode, weight: Math.round(match.weight * (1 - match.ratio)) }
    : { node: match.fromNode, weight: Math.round(match.weight * match.ratio) });
}

function sameEdgeShortcut(snapFrom, snapTo) {
  let best = null;
  for (const from of snapFrom.matches) {
    for (const to of snapTo.matches) {
      if (from.leaf === to.leaf && from.edgeIndex === to.edgeIndex && to.ratio >= from.ratio) {
        const weight = Math.round(from.weight * (to.ratio - from.ratio));
        if (best == null || weight < best) best = weight;
      }
    }
  }
  return best;
}

test("turn restrictions expand via nodes and cut forbidden turns", async () => {
  const { applyTurnRestrictions } = await import("../scripts/osm_road_graph.mjs");
  // Cross junction: node 4 is the center; ways 100 (west-east through 0-4-1)
  // and 200 (south-north through 2-4-3). Two-way everywhere, one edge each
  // direction per arm.
  const edges = [
    [0, 4, 100], [4, 0, 100], [4, 1, 100], [1, 4, 100],
    [2, 4, 200], [4, 2, 200], [4, 3, 200], [3, 4, 200]
  ];
  const context = {
    restrictions: [
      // Coming from the west arm (way 100), turning to the north arm
      // (way 200) is forbidden.
      { kind: "no_left_turn", fromWay: 100, toWay: 200, viaNode: 9004, only: false }
    ],
    nodeIndex: new Map([[9004, 4]]),
    nodeLat: [0, 0, 0, 0, 0],
    nodeLon: [0, 0, 0, 0, 0],
    edgeFrom: edges.map(edge => edge[0]),
    edgeTo: edges.map(edge => edge[1]),
    edgeWeightDs: edges.map(() => 10),
    edgeDistDm: edges.map(() => 100),
    edgeName: edges.map(() => 0),
    edgeWay: edges.map(edge => edge[2]),
    geomOffsets: [0, ...edges.map((_, i) => i + 1)],
    geomBytes: {
      data: new Uint8Array(edges.map(() => 0)),
      length: edges.length,
      ensure(extra) {
        if (this.length + extra <= this.data.length) return;
        const next = new Uint8Array((this.length + extra) * 2);
        next.set(this.data.subarray(0, this.length));
        this.data = next;
      }
    },
    log: () => {}
  };
  applyTurnRestrictions(context);
  // Both approaches on way 100 (0->4 and 1->4) get redirected to copies.
  const copies = context.nodeLat.length - 5;
  assert.ok(copies >= 2, `expected copies for both way-100 approaches, got ${copies}`);
  const westApproachTarget = context.edgeTo[0];
  assert.ok(westApproachTarget >= 5, "west approach redirected to a copy node");
  // The copy keeps way-100 continuation and way-200 southbound... except the
  // forbidden way-200 edges: no outgoing copy may reach node 3 or 2 via way
  // 200? The restriction bans way 200 entirely from this approach.
  for (let e = 8; e < context.edgeFrom.length; e++) {
    if (context.edgeFrom[e] === westApproachTarget) {
      assert.notEqual(context.edgeWay[e], 200, "no way-200 turn from the restricted approach");
    }
  }
  // Unrestricted approaches (way 200) still reach everything directly.
  assert.equal(context.edgeTo[4], 4, "south approach unchanged");
});

test("only_ restrictions keep just the mandated turn", async () => {
  const { applyTurnRestrictions } = await import("../scripts/osm_road_graph.mjs");
  const edges = [
    [0, 4, 100], [4, 0, 100], [4, 1, 100], [1, 4, 100],
    [2, 4, 200], [4, 2, 200], [4, 3, 200], [3, 4, 200]
  ];
  const context = {
    restrictions: [
      { kind: "only_straight_on", fromWay: 200, toWay: 200, viaNode: 9004, only: true }
    ],
    nodeIndex: new Map([[9004, 4]]),
    nodeLat: [0, 0, 0, 0, 0],
    nodeLon: [0, 0, 0, 0, 0],
    edgeFrom: edges.map(edge => edge[0]),
    edgeTo: edges.map(edge => edge[1]),
    edgeWeightDs: edges.map(() => 10),
    edgeDistDm: edges.map(() => 100),
    edgeName: edges.map(() => 0),
    edgeWay: edges.map(edge => edge[2]),
    geomOffsets: [0, ...edges.map((_, i) => i + 1)],
    geomBytes: {
      data: new Uint8Array(edges.map(() => 0)),
      length: edges.length,
      ensure() {}
    },
    log: () => {}
  };
  context.geomBytes.ensure = function (extra) {
    if (this.length + extra <= this.data.length) return;
    const next = new Uint8Array((this.length + extra) * 2);
    next.set(this.data.subarray(0, this.length));
    this.data = next;
  };
  applyTurnRestrictions(context);
  const southApproachTarget = context.edgeTo[4]; // edge 2->4 on way 200
  assert.ok(southApproachTarget >= 5, "restricted approach redirected");
  for (let e = 8; e < context.edgeFrom.length; e++) {
    if (context.edgeFrom[e] === southApproachTarget) {
      assert.equal(context.edgeWay[e], 200, "only way-200 edges remain from an only_ approach");
    }
  }
});

test("via-way restrictions keep path memory through the via chain", async () => {
  const { applyTurnRestrictions } = await import("../scripts/osm_road_graph.mjs");
  // 0 -(way100)- 1=entry -(way300)- 2=mid -(way300)- 3=exit -(way200)- 4,
  // side street way400 at mid (2<->5), second approach way500 at entry (6<->1).
  const edges = [
    [0, 1, 100], [1, 0, 100],
    [1, 2, 300], [2, 3, 300],
    [3, 4, 200], [4, 3, 200],
    [2, 5, 400], [5, 2, 400],
    [6, 1, 500], [1, 6, 500]
  ];
  const context = {
    restrictions: [
      { kind: "no_u_turn", fromWay: 100, viaWays: [300], toWay: 200, only: false }
    ],
    nodeIndex: new Map(),
    nodeLat: [0, 0, 0, 0, 0, 0, 0],
    nodeLon: [0, 0, 0, 0, 0, 0, 0],
    edgeFrom: edges.map(edge => edge[0]),
    edgeTo: edges.map(edge => edge[1]),
    edgeWeightDs: edges.map(() => 10),
    edgeDistDm: edges.map(() => 100),
    edgeName: edges.map(() => 0),
    edgeWay: edges.map(edge => edge[2]),
    geomOffsets: [0, ...edges.map((_, i) => i + 1)],
    geomBytes: {
      data: new Uint8Array(64),
      length: edges.length,
      ensure(extra) {
        if (this.length + extra <= this.data.length) return;
        const next = new Uint8Array((this.length + extra) * 2);
        next.set(this.data.subarray(0, this.length));
        this.data = next;
      }
    },
    log: () => {}
  };
  applyTurnRestrictions(context);
  // The from-way approach 0->1 is redirected into the copy chain.
  const entryCopy = context.edgeTo[0];
  assert.ok(entryCopy >= 7, "from-way approach redirected to entry copy");
  // Walk the copy chain: entryCopy -> midCopy via way 300.
  const outOf = (node) => {
    const out = [];
    for (let e = 0; e < context.edgeFrom.length; e++) {
      if (context.edgeFrom[e] === node) out.push(e);
    }
    return out;
  };
  const chain1 = outOf(entryCopy).filter(e => context.edgeWay[e] === 300);
  assert.equal(chain1.length, 1, "entry copy continues along the via way");
  const midCopy = context.edgeTo[chain1[0]];
  assert.ok(midCopy >= 7, "via chain stays on copies through intermediate junctions");
  const midOut = outOf(midCopy);
  assert.ok(midOut.some(e => context.edgeWay[e] === 400), "side exits stay legal mid-chain for no_ restrictions");
  const chain2 = midOut.filter(e => context.edgeWay[e] === 300);
  assert.equal(chain2.length, 1);
  const exitCopy = context.edgeTo[chain2[0]];
  assert.ok(exitCopy >= 7);
  for (const e of outOf(exitCopy)) {
    assert.notEqual(context.edgeWay[e], 200, "restricted exit removed at the end of the via chain");
  }
  // Traffic entering the via way from elsewhere is untouched.
  assert.equal(context.edgeTo[8], 1, "other approach still reaches the original entry");
  assert.equal(context.edgeTo[2], 2, "original via edge unchanged");
  assert.equal(context.edgeTo[4], 4, "original exit turn unchanged");
});

test("junction expansion prices turns by bearing and filters restrictions", async () => {
  const { expandTurnCosts } = await import("../scripts/osm_road_graph.mjs");
  // Cross at the equator: 4 = center, 0 = west, 1 = east, 2 = south, 3 = north.
  const nodeLat = [0, 0, -100000, 100000, 0];
  const nodeLon = [-100000, 100000, 0, 0, 0];
  const edges = [
    [0, 4, 100], [4, 0, 100], [4, 1, 100], [1, 4, 100],
    [2, 4, 200], [4, 2, 200], [4, 3, 200], [3, 4, 200]
  ];
  const makeContext = (restrictions) => ({
    restrictions,
    nodeIndex: new Map([[9004, 4]]),
    nodeLat,
    nodeLon,
    edgeFrom: edges.map(edge => edge[0]),
    edgeTo: edges.map(edge => edge[1]),
    edgeWeightDs: edges.map(() => 100),
    edgeDistDm: edges.map(() => 1000),
    edgeName: edges.map(() => 0),
    edgeWay: edges.map(edge => edge[2]),
    edgeClass: edges.map(() => 0),
    edgeJunction: edges.map(() => 0),
    geomOffsets: [0, ...edges.map((_, i) => i + 1)],
    geomBytes: { data: new Uint8Array(edges.map(() => 0)), length: edges.length },
    log: () => {}
  });
  const costs = { uturn: 150, left: 40, right: 15, slightLeft: 20, slightRight: 8 };
  const expanded = expandTurnCosts(makeContext([]), costs);
  assert.ok(expanded.edgeFrom instanceof Uint32Array);
  assert.ok(expanded.edgeClass instanceof Uint8Array);
  assert.ok(expanded.geomOffsets instanceof Uint32Array);
  assert.ok(expanded.geomBytes instanceof Uint8Array);
  // From the west approach (edge 0: 0->4), find each turn's added cost.
  const added = new Map();
  for (let i = 0; i < expanded.edgeFrom.length; i++) {
    if (expanded.edgeFrom[i] !== 0) continue;
    added.set(expanded.edgeTo[i], expanded.edgeWeightDs[i] - 100);
  }
  assert.equal(added.get(2), 0, "straight through (west->east) is free");
  assert.equal(added.get(6), costs.left, "west->north is a left turn");
  assert.equal(added.get(5), costs.right, "west->south is a right turn");
  assert.equal(added.get(1), costs.uturn, "west->west back is a u-turn");

  // A no-turn restriction removes the movement entirely in expansion mode.
  const restricted = expandTurnCosts(makeContext([
    { kind: "no_left_turn", fromWay: 100, toWay: 200, viaNode: 9004, only: false }
  ]), costs);
  for (let i = 0; i < restricted.edgeFrom.length; i++) {
    if (restricted.edgeFrom[i] !== 0) continue;
    assert.notEqual(restricted.edgeTo[i], 6, "restricted left turn filtered");
    assert.notEqual(restricted.edgeTo[i], 5, "restricted way-200 turn filtered");
  }
});

test("high-degree turn expansion allocates exact typed columns", async () => {
  const { expandTurnCosts } = await import("../scripts/osm_road_graph.mjs");
  const spokes = 256;
  const nodeLat = new Int32Array(spokes + 1);
  const nodeLon = new Int32Array(spokes + 1);
  const edgeFrom = new Uint32Array(spokes * 2);
  const edgeTo = new Uint32Array(spokes * 2);
  const edgeWay = new Float64Array(spokes * 2);
  for (let i = 0; i < spokes; i++) {
    const leaf = i + 1;
    nodeLat[leaf] = Math.round(Math.sin(i) * 100000);
    nodeLon[leaf] = Math.round(Math.cos(i) * 100000);
    edgeFrom[i * 2] = leaf;
    edgeTo[i * 2] = 0;
    edgeFrom[i * 2 + 1] = 0;
    edgeTo[i * 2 + 1] = leaf;
    edgeWay[i * 2] = i + 1;
    edgeWay[i * 2 + 1] = i + 1;
  }
  const edgeCount = edgeFrom.length;
  const expanded = expandTurnCosts({
    restrictions: [],
    nodeIndex: new Map(),
    nodeLat,
    nodeLon,
    edgeFrom,
    edgeTo,
    edgeWeightDs: new Uint32Array(edgeCount).fill(10),
    edgeDistDm: new Uint32Array(edgeCount).fill(100),
    edgeName: new Uint32Array(edgeCount),
    edgeWay,
    edgeClass: new Uint8Array(edgeCount),
    edgeJunction: new Uint8Array(edgeCount),
    edgeSpeed: new Uint8Array(edgeCount),
    edgeCond: new Uint8Array(edgeCount),
    edgeSign: new Uint32Array(edgeCount),
    edgeFlags: new Uint8Array(edgeCount),
    geomOffsets: Uint32Array.from({ length: edgeCount + 1 }, (_, i) => i),
    geomBytes: { data: new Uint8Array(edgeCount), length: edgeCount },
    laneOffsets: Uint32Array.from({ length: edgeCount + 1 }, (_, i) => i),
    laneBytes: { data: new Uint8Array(edgeCount), length: edgeCount, view() { return this.data; } },
    log: () => {}
  }, { uturn: 150, left: 40, right: 15, slightLeft: 20, slightRight: 8 });
  assert.equal(expanded.edgeFrom.length, spokes * spokes + spokes);
  for (const key of ["edgeFrom", "edgeTo", "edgeWeightDs", "edgeDistDm", "edgeName", "edgeSign", "geomOffsets", "laneOffsets"]) {
    assert.ok(expanded[key] instanceof Uint32Array, `${key} is compact`);
  }
  for (const key of ["edgeClass", "edgeJunction", "edgeSpeed", "edgeCond", "edgeFlags", "geomBytes", "laneBytes"]) {
    assert.ok(expanded[key] instanceof Uint8Array, `${key} is compact`);
  }
  assert.equal(expanded.geomBytes.length, expanded.edgeFrom.length);
  assert.equal(expanded.laneBytes.length, expanded.edgeFrom.length);
});

test("multi-via-way restrictions resolve through the way union", async () => {
  const { applyTurnRestrictions } = await import("../scripts/osm_road_graph.mjs");
  // 0 -(100)- 1 -(301)- 2 -(302)- 3 -(200)- 4: two via ways chained.
  const edges = [
    [0, 1, 100], [1, 0, 100],
    [1, 2, 301], [2, 1, 301],
    [2, 3, 302], [3, 2, 302],
    [3, 4, 200], [4, 3, 200]
  ];
  const context = {
    restrictions: [
      { kind: "no_u_turn", fromWay: 100, viaWays: [302, 301], toWay: 200, only: false }
    ],
    nodeIndex: new Map(),
    nodeLat: [0, 0, 0, 0, 0],
    nodeLon: [0, 0, 0, 0, 0],
    edgeFrom: edges.map(edge => edge[0]),
    edgeTo: edges.map(edge => edge[1]),
    edgeWeightDs: edges.map(() => 10),
    edgeDistDm: edges.map(() => 100),
    edgeName: edges.map(() => 0),
    edgeWay: edges.map(edge => edge[2]),
    geomOffsets: [0, ...edges.map((_, i) => i + 1)],
    geomBytes: {
      data: new Uint8Array(64),
      length: edges.length,
      ensure(extra) {
        if (this.length + extra <= this.data.length) return;
        const next = new Uint8Array((this.length + extra) * 2);
        next.set(this.data.subarray(0, this.length));
        this.data = next;
      }
    },
    log: () => {}
  };
  applyTurnRestrictions(context);
  // Approach 0->1 is redirected into a copy chain spanning both via ways.
  const entryCopy = context.edgeTo[0];
  assert.ok(entryCopy >= 5, "approach redirected despite shuffled via member order");
  const outOf = (node) => {
    const out = [];
    for (let e = 0; e < context.edgeFrom.length; e++) {
      if (context.edgeFrom[e] === node) out.push(e);
    }
    return out;
  };
  let current = entryCopy;
  for (let hops = 0; hops < 2; hops++) {
    const chain = outOf(current).filter(e => context.edgeWay[e] === 301 || context.edgeWay[e] === 302);
    const forward = chain.filter(e => context.edgeTo[e] >= 5);
    assert.equal(forward.length, 1, `chain hop ${hops} continues on a copy`);
    current = context.edgeTo[forward[0]];
  }
  for (const e of outOf(current)) {
    assert.notEqual(context.edgeWay[e], 200, "exit onto the to-way removed at chain end");
  }
  assert.equal(context.edgeTo[2], 2, "direct via traffic untouched");
});

test("route cell, geometry, and overlay codecs round-trip", async () => {
  const { encodeRouteGeometry, decodeRouteGeometry, edgePolyline } = await import("../src/route_graph.js");
  const cell = {
    cellId: 7,
    firstNode: 1000,
    nodeCount: 3,
    latE7: Int32Array.from([455000000, 455000500, 455001000]),
    lonE7: Int32Array.from([-736000000, -735999000, -735998000]),
    rowStart: Uint32Array.from([0, 2, 3, 3]),
    targets: Uint32Array.from([1001, 5, 1000]),
    weights: Uint32Array.from([120, 999, 60]),
    distsDm: Uint32Array.from([800, 12000, 400]),
    nameIds: Uint32Array.from([1, 0, 2]),
    classes: Uint8Array.from([0, 3, 1]),
    extLat: Int32Array.from([0, 454900000, 0]),
    extLon: Int32Array.from([0, -736100000, 0]),
    geomRefs: Uint32Array.from([0, 3, 1])
  };
  const decoded = decodeRouteCell(encodeRouteCell(cell));
  assert.equal(decoded.cellId, 7);
  assert.equal(decoded.firstNode, 1000);
  assert.deepEqual([...decoded.latE7], [...cell.latE7]);
  assert.deepEqual([...decoded.targets], [...cell.targets]);
  assert.deepEqual([...decoded.weights], [...cell.weights]);
  assert.deepEqual([...decoded.geomRefs], [0, 3, 1]);
  assert.deepEqual([...decoded.classes], [0, 3, 1]);

  const geometryBlock = decodeRouteGeometry(encodeRouteGeometry({
    cellId: 7,
    polylines: [
      Int32Array.from([455000000, -736000000, 455000200, -735999800, 455000500, -735999000]),
      Int32Array.from([454900000, -736100000, 455001000, -735998000])
    ]
  }));
  assert.equal(geometryBlock.cellId, 7);
  assert.deepEqual([...geometryBlock.polylines[0]], [455000000, -736000000, 455000200, -735999800, 455000500, -735999000]);
  const forward = edgePolyline(0, geometryBlock);
  const reversed = edgePolyline(1, geometryBlock);
  assert.deepEqual([...forward], [...geometryBlock.polylines[0]]);
  assert.deepEqual([...reversed], [455000500, -735999000, 455000200, -735999800, 455000000, -736000000]);

  const overlay = {
    level: 2,
    cellId: 3,
    nodes: Uint32Array.from([10, 55, 300]),
    rowStart: Uint32Array.from([0, 2, 2, 3]),
    targetIndex: Uint32Array.from([1, 2, 0]),
    weights: Uint32Array.from([500, 1200, 70]),
    isClique: Uint8Array.from([1, 0, 1]),
    limits: Uint32Array.from([0, 2, 0]),
    pathFlags: Uint8Array.from([1, 0, 4])
  };
  const encodedOverlay = encodeRouteOverlay(overlay);
  const legacyBytes = [0x52, 0x46, 0x52, 0x4f];
  const pushVarint = value => {
    let number = Math.max(0, Math.floor(value));
    while (number >= 0x80) {
      legacyBytes.push((number % 0x80) | 0x80);
      number = Math.floor(number / 0x80);
    }
    legacyBytes.push(number);
  };
  const pushZigzag = value => pushVarint(value < 0 ? -value * 2 - 1 : value * 2);
  pushVarint(3);
  pushVarint(overlay.level);
  pushVarint(overlay.cellId);
  pushVarint(overlay.nodes.length);
  let previousNode = 0;
  for (const node of overlay.nodes) {
    pushVarint(node - previousNode);
    previousNode = node;
  }
  pushVarint(overlay.targetIndex.length);
  for (let node = 0; node < overlay.nodes.length; node++) {
    pushVarint(overlay.rowStart[node + 1] - overlay.rowStart[node]);
    for (let edge = overlay.rowStart[node]; edge < overlay.rowStart[node + 1]; edge++) {
      pushZigzag(overlay.targetIndex[edge] - node);
      pushVarint(overlay.weights[edge] * 2 + overlay.isClique[edge]);
      pushVarint(overlay.limits[edge]);
      pushVarint(overlay.pathFlags[edge]);
    }
  }
  assert.deepEqual([...encodedOverlay], legacyBytes, "pre-sized encoder must retain the published RFRO bytes");
  const decodedOverlay = decodeRouteOverlay(encodedOverlay);
  assert.equal(decodedOverlay.level, 2);
  assert.deepEqual([...decodedOverlay.nodes], [10, 55, 300]);
  assert.deepEqual([...decodedOverlay.weights], [500, 1200, 70]);
  assert.deepEqual([...decodedOverlay.isClique], [1, 0, 1]);
  assert.deepEqual([...decodedOverlay.limits], [0, 2, 0]);
  assert.deepEqual([...decodedOverlay.pathFlags], [1, 0, 4]);

  const topSlice = decodeRouteTopSlice(encodeRouteTopSlice(overlay, 0, overlay.nodes.length, 9));
  assert.equal(topSlice.level, 2);
  assert.equal(topSlice.cellId, 9);
  assert.deepEqual([...topSlice.nodes], [10, 300]);
  assert.deepEqual([...topSlice.targets], [55, 300, 10]);
  assert.deepEqual([...topSlice.weights], [500, 1200, 70]);
  assert.deepEqual([...topSlice.isClique], [1, 0, 1]);
  assert.deepEqual([...topSlice.limits], [0, 2, 0]);
  assert.deepEqual([...topSlice.pathFlags], [1, 0, 4]);
});

test("multilevel routes match reference Dijkstra on a synthetic city", async (t) => {
  const graph = syntheticGraph(28, 24);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const summary = buildRouteGraph(graph, dir, { leafNodes: 48, fanout: 4, topMaxCells: 6 });
  assert.ok(summary.levelFanouts.length >= 1, "expected at least one overlay level");
  const engine = await openRouteGraphDir(dir);
  // The engine's node ids follow the builder's KD order; remap the source
  // edges so the reference Dijkstra speaks the same ids as engine.snap.
  const newId = new Uint32Array(graph.nodeLat.length);
  for (let i = 0; i < summary.nodeOrder.length; i++) newId[summary.nodeOrder[i]] = i;
  const renumberedFrom = Uint32Array.from(graph.edgeFrom, node => newId[node]);
  const renumberedTo = Uint32Array.from(graph.edgeTo, node => newId[node]);
  const csr = buildCsr(graph.nodeLat.length, renumberedFrom, renumberedTo);
  const random = lcg(7);
  let compared = 0;
  for (let i = 0; i < 40; i++) {
    const fromNode = Math.floor(random() * graph.nodeLat.length);
    const toNode = Math.floor(random() * graph.nodeLat.length);
    const from = { lat: graph.nodeLat[fromNode] / 1e7, lon: graph.nodeLon[fromNode] / 1e7 };
    const to = { lat: graph.nodeLat[toNode] / 1e7, lon: graph.nodeLon[toNode] / 1e7 };
    const [snapFrom, snapTo] = await Promise.all([engine.snap(from), engine.snap(to)]);
    const expected = referenceSeconds(
      graph,
      csr,
      seedsFromSnap(snapFrom, "forward"),
      seedsFromSnap(snapTo, "backward"),
      sameEdgeShortcut(snapFrom, snapTo)
    );
    const result = await engine.route({ from, to });
    assert.equal(result.seconds, expected, `pair ${i}: engine ${result.seconds}s vs reference ${expected}s`);
    assert.ok(result.geometry.length >= 1, "route geometry present");
    compared++;
  }
  assert.equal(compared, 40);
});

test("sharded build routes identically to the monolithic build", async (t) => {
  const graph = syntheticGraph(20, 20, 99);
  const monoDir = mkdtempSync(join(tmpdir(), "rangefind-route-mono-"));
  const shardDir = mkdtempSync(join(tmpdir(), "rangefind-route-shard-"));
  t.after(() => {
    rmSync(monoDir, { recursive: true, force: true });
    rmSync(shardDir, { recursive: true, force: true });
  });
  buildRouteGraph(graph, monoDir, { leafNodes: 40, fanout: 4, topMaxCells: 5 });
  buildRouteGraph(graph, shardDir, { leafNodes: 40, fanout: 4, topMaxCells: 5, shards: 3 });
  const mono = await openRouteGraphDir(monoDir);
  const sharded = await openRouteGraphDir(shardDir);
  assert.ok(sharded.root.shards.length >= 4, "expected shard directories plus the top shard");
  const random = lcg(3);
  for (let i = 0; i < 15; i++) {
    const fromNode = Math.floor(random() * graph.nodeLat.length);
    const toNode = Math.floor(random() * graph.nodeLat.length);
    const from = { lat: graph.nodeLat[fromNode] / 1e7, lon: graph.nodeLon[fromNode] / 1e7 };
    const to = { lat: graph.nodeLat[toNode] / 1e7, lon: graph.nodeLon[toNode] / 1e7 };
    const monoResult = await mono.route({ from, to, geometry: false });
    const shardResult = await sharded.route({ from, to, geometry: false });
    assert.equal(shardResult.seconds, monoResult.seconds, `pair ${i} sharded vs monolithic`);
  }
});

test("production builds can release consumed source columns without changing output", (t) => {
  const retained = syntheticGraph(12, 10, 123);
  const releasable = syntheticGraph(12, 10, 123);
  const retainedDir = mkdtempSync(join(tmpdir(), "rangefind-route-retained-source-"));
  const releasedDir = mkdtempSync(join(tmpdir(), "rangefind-route-released-source-"));
  t.after(() => {
    rmSync(retainedDir, { recursive: true, force: true });
    rmSync(releasedDir, { recursive: true, force: true });
  });
  const options = { leafNodes: 32, fanout: 4, topMaxCells: 4 };
  const baseline = buildRouteGraph(retained, retainedDir, options);
  const collected = [];
  const compact = buildRouteGraph(releasable, releasedDir, {
    ...options,
    releaseSource: true,
    collectGarbage: phase => collected.push(phase)
  });
  assert.equal(compact.rootFile, baseline.rootFile);
  assert.deepEqual(collected, ["topology", "leaf-packing", "overlays"]);
  for (const column of [
    "nodeLat", "nodeLon", "edgeFrom", "edgeTo", "edgeWeightDs",
    "edgeDistDm", "edgeName", "edgeClass", "edgeJunction", "edgeSpeed",
    "edgeCond", "edgeSign", "edgeFlags", "geomOffsets", "geomBytes",
    "laneOffsets", "laneBytes"
  ]) {
    assert.equal(releasable[column], null, `${column} should be released`);
  }
});

test("far-off points fail with a coded snap error", async (t) => {
  const graph = syntheticGraph(8, 8, 11);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-snap-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 24, fanout: 4, topMaxCells: 4 });
  const engine = await openRouteGraphDir(dir);
  await assert.rejects(
    engine.route({ from: { lat: 44.0, lon: -75.0 }, to: { lat: 45.5, lon: -73.6 } }),
    error => error.code === "RANGEFIND_ROUTE_SNAP_TOO_FAR"
  );
  await assert.rejects(
    engine.route({ from: { lat: Number.NaN, lon: -73.6 }, to: { lat: 45.5, lon: -73.6 } }),
    error => error.code === "RANGEFIND_ROUTE_BAD_POINT"
  );
});

test("time buckets stay exact and departure times select them", async (t) => {
  const graph = syntheticGraph(20, 18, 31);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-bucket-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const summary = buildRouteGraph(graph, dir, {
    leafNodes: 48, fanout: 4, topMaxCells: 6,
    timeBuckets: [PEAK_BUCKET]
  });
  assert.deepEqual(summary.buckets, ["base", "peak"]);
  const engine = await openRouteGraphDir(dir);
  const newId = new Uint32Array(graph.nodeLat.length);
  for (let i = 0; i < summary.nodeOrder.length; i++) newId[summary.nodeOrder[i]] = i;
  const renumberedFrom = Uint32Array.from(graph.edgeFrom, node => newId[node]);
  const renumberedTo = Uint32Array.from(graph.edgeTo, node => newId[node]);
  const csr = buildCsr(graph.nodeLat.length, renumberedFrom, renumberedTo);
  // Peak factors in build order (arterial, local), scaled by 1000.
  const peakFactors = [1500, 1200];
  const peakGraph = {
    ...graph,
    edgeWeightDs: Uint32Array.from(graph.edgeWeightDs, (weight, i) => bucketWeight(weight, graph.edgeClass[i], peakFactors))
  };
  const random = lcg(13);
  for (let i = 0; i < 15; i++) {
    const fromNode = Math.floor(random() * graph.nodeLat.length);
    const toNode = Math.floor(random() * graph.nodeLat.length);
    const from = { lat: graph.nodeLat[fromNode] / 1e7, lon: graph.nodeLon[fromNode] / 1e7 };
    const to = { lat: graph.nodeLat[toNode] / 1e7, lon: graph.nodeLon[toNode] / 1e7 };
    const [snapFrom, snapTo] = await Promise.all([engine.snap(from), engine.snap(to)]);
    const scaledSeeds = (snapResult, side) => snapResult.matches.map(match => {
      const weight = bucketWeight(match.weight, match.classCode, peakFactors);
      return side === "forward"
        ? { node: match.toNode, weight: Math.round(weight * (1 - match.ratio)) }
        : { node: match.fromNode, weight: Math.round(weight * match.ratio) };
    });
    let sameEdgeWeight = null;
    for (const fromMatch of snapFrom.matches) {
      for (const toMatch of snapTo.matches) {
        if (fromMatch.leaf === toMatch.leaf && fromMatch.edgeIndex === toMatch.edgeIndex && toMatch.ratio >= fromMatch.ratio) {
          const weight = Math.round(bucketWeight(fromMatch.weight, fromMatch.classCode, peakFactors) * (toMatch.ratio - fromMatch.ratio));
          if (sameEdgeWeight == null || weight < sameEdgeWeight) sameEdgeWeight = weight;
        }
      }
    }
    const expected = referenceSeconds(peakGraph, csr, scaledSeeds(snapFrom, "forward"), scaledSeeds(snapTo, "backward"), sameEdgeWeight);
    const result = await engine.route({ from, to, bucket: "peak" });
    assert.equal(result.seconds, expected, `peak pair ${i}`);
    assert.equal(result.bucket, "peak");
  }
  // departureTime rule matching: Tuesday 08:00 is peak, Sunday 08:00 is base.
  const tuesday = await engine.route({
    from: { lat: graph.nodeLat[0] / 1e7, lon: graph.nodeLon[0] / 1e7 },
    to: { lat: graph.nodeLat[300] / 1e7, lon: graph.nodeLon[300] / 1e7 },
    departureTime: new Date(2026, 7, 4, 8, 0),
    geometry: false
  });
  assert.equal(tuesday.bucket, "peak");
  const sunday = await engine.route({
    from: { lat: graph.nodeLat[0] / 1e7, lon: graph.nodeLon[0] / 1e7 },
    to: { lat: graph.nodeLat[300] / 1e7, lon: graph.nodeLon[300] / 1e7 },
    departureTime: new Date(2026, 7, 2, 8, 0),
    geometry: false
  });
  assert.equal(sunday.bucket, "base");
  assert.ok(tuesday.seconds >= sunday.seconds, "peak metric is never faster than base");
});

test("alternatives diverge and live weights re-rank them", async (t) => {
  const graph = syntheticGraph(22, 20, 63);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-alt-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 48, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);
  const corner = (x, y) => ({ lat: graph.nodeLat[y * 22 + x] / 1e7, lon: graph.nodeLon[y * 22 + x] / 1e7 });
  const result = await engine.route({ from: corner(1, 1), to: corner(20, 18), alternatives: 2 });
  assert.ok(Array.isArray(result.alternatives), "alternatives returned");
  assert.ok(result.alternatives.length >= 1, "at least one alternative survives filtering");
  for (const alternative of result.alternatives) {
    assert.ok(alternative.seconds >= result.seconds - 1e-9, "primary is fastest on the static metric");
    assert.ok(alternative.geometry.length > 1, "alternatives carry geometry");
    const shared = new Set(result.edges.map(edge => `${edge.leaf}/${edge.edge}`));
    const overlap = alternative.edges.filter(edge => shared.has(`${edge.leaf}/${edge.edge}`)).length / alternative.edges.length;
    assert.ok(overlap < 0.9, `alternative should diverge (overlap ${overlap.toFixed(2)})`);
  }
  // Live re-rank: jam every edge of the primary route.
  const factors = {};
  for (const edge of result.edges) factors[`${edge.leaf}/${edge.edge}`] = 3;
  const reranked = await engine.route({
    from: corner(1, 1),
    to: corner(20, 18),
    alternatives: 2,
    liveWeights: { epoch: engine.root.sourceHash, factors }
  });
  assert.ok(reranked.adjustedSeconds != null, "adjusted seconds reported");
  const primaryKey = JSON.stringify(result.geometry[Math.floor(result.geometry.length / 2)]);
  const rerankedKey = JSON.stringify(reranked.geometry[Math.floor(reranked.geometry.length / 2)]);
  assert.notEqual(rerankedKey, primaryKey, "jamming the primary promotes an alternative");
  await assert.rejects(
    engine.route({ from: corner(1, 1), to: corner(20, 18), liveWeights: { epoch: "stale", factors } }),
    error => error.code === "RANGEFIND_ROUTE_STALE_LIVE"
  );
});

test("http adapter routes identically over Range requests", async (t) => {
  const { createServer } = await import("node:http");
  const { readFileSync, statSync } = await import("node:fs");
  const { openRouteGraphUrl } = await import("../src/route_graph_query.js");
  const graph = syntheticGraph(14, 14, 77);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-http-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 32, fanout: 4, topMaxCells: 4 });

  const server = createServer((req, res) => {
    const file = join(dir, decodeURIComponent(new URL(req.url, "http://x").pathname));
    let size;
    try {
      size = statSync(file).size;
    } catch {
      res.writeHead(404).end();
      return;
    }
    const range = req.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
    const body = readFileSync(file);
    if (range) {
      const start = Number(range[1]);
      const end = Math.min(Number(range[2]), size - 1);
      res.writeHead(206, { "content-range": `bytes ${start}-${end}/${size}` });
      res.end(body.subarray(start, end + 1));
      return;
    }
    res.writeHead(200).end(body);
  });
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  t.after(() => server.close());
  const baseUrl = `http://127.0.0.1:${server.address().port}`;

  const local = await openRouteGraphDir(dir);
  const remote = await openRouteGraphUrl(baseUrl);
  const random = lcg(21);
  for (let i = 0; i < 6; i++) {
    const fromNode = Math.floor(random() * graph.nodeLat.length);
    const toNode = Math.floor(random() * graph.nodeLat.length);
    const from = { lat: graph.nodeLat[fromNode] / 1e7, lon: graph.nodeLon[fromNode] / 1e7 };
    const to = { lat: graph.nodeLat[toNode] / 1e7, lon: graph.nodeLon[toNode] / 1e7 };
    const [localResult, remoteResult] = await Promise.all([
      local.route({ from, to }),
      remote.route({ from, to })
    ]);
    assert.equal(remoteResult.seconds, localResult.seconds, `pair ${i} http vs file`);
    assert.deepEqual(remoteResult.geometry, localResult.geometry, `pair ${i} geometry http vs file`);
  }
  assert.ok(remote.io.counters.requests > 0, "http adapter actually fetched");
});

test("dedicated geometry packs and adaptive ranges collapse long-route fetches", async (t) => {
  const { openRouteGraph } = await import("../src/route_graph_query.js");
  const graph = syntheticGraph(64, 40, 91);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-range-plan-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 16, fanout: 4, topMaxCells: 4, packBytes: 4 * 1024 * 1024 });
  const makeIo = () => {
    const counters = { requests: 0, bytes: 0 };
    return {
      counters,
      async readFile(path) { return new Uint8Array(readFileSync(join(dir, path))); },
      async readRange(path, offset, length) {
        counters.requests++;
        counters.bytes += length;
        return new Uint8Array(readFileSync(join(dir, path)).subarray(offset, offset + length));
      }
    };
  };
  const exactIo = makeIo();
  const batchedIo = makeIo();
  const [exact, batched] = await Promise.all([
    openRouteGraph({
      io: exactIo,
      rangeMergeGapBytes: 0,
      rangeMaxMergedBytes: 1,
      rangeMaxOverfetchBytes: 0,
      rangeMaxOverfetchRatio: 1
    }),
    openRouteGraph({ io: batchedIo })
  ]);
  const from = { lat: graph.nodeLat[0] / 1e7, lon: graph.nodeLon[0] / 1e7 };
  const last = graph.nodeLat.length - 1;
  const to = { lat: graph.nodeLat[last] / 1e7, lon: graph.nodeLon[last] / 1e7 };
  const [exactResult, batchedResult] = await Promise.all([
    exact.route({ from, to }),
    batched.route({ from, to })
  ]);
  assert.equal(batchedResult.seconds, exactResult.seconds);
  assert.deepEqual(batchedResult.geometry, exactResult.geometry);
  assert.ok(
    batchedIo.counters.requests <= exactIo.counters.requests * 0.5,
    `expected at least 2x fewer ranges (${exactIo.counters.requests} -> ${batchedIo.counters.requests})`
  );
  assert.ok(
    batchedIo.counters.bytes <= exactIo.counters.bytes * 2.5,
    `adaptive overfetch stayed bounded (${exactIo.counters.bytes} -> ${batchedIo.counters.bytes})`
  );
});

test("one-to-many matrix equals pairwise routes", async (t) => {
  const graph = syntheticGraph(18, 16, 44);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-matrix-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 40, fanout: 4, topMaxCells: 5 });
  const engine = await openRouteGraphDir(dir);
  const point = (node) => ({ lat: graph.nodeLat[node] / 1e7, lon: graph.nodeLon[node] / 1e7 });
  const stops = [point(5), point(200), point(150), point(287), point(96)];
  const fast = await engine.matrix({ points: stops });
  const slow = await engine.matrix({ points: stops, pairwise: true });
  assert.deepEqual(fast.seconds, slow.seconds, "shared-context matrix matches pairwise exactly");
});

test("live traffic providers reroute, close, and degrade gracefully", async (t) => {
  const { createStaticLiveProvider } = await import("../src/route_graph_query.js");
  const graph = syntheticGraph(20, 18, 77);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-live-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 48, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);
  const point = (node) => ({ lat: graph.nodeLat[node] / 1e7, lon: graph.nodeLon[node] / 1e7 });
  const from = point(21);
  const to = point(20 * 18 - 25);

  const base = await engine.route({ from, to });
  assert.ok(base.edges.every(edge => typeof edge.segment === "string"), "edges expose physical segment ids");
  const baseSegments = base.edges.map(edge => edge.segment);

  // Jamming every segment of the base route must push the search onto a
  // different corridor, and the live estimate must exceed the static one.
  const jam = createStaticLiveProvider(baseSegments.map(segment => ({
    segment,
    factor: 6,
    confidence: 1,
    observedAt: Date.now()
  })));
  const jammed = await engine.route({ from, to, live: jam });
  assert.ok(jammed.live.applied > 0, "jam states applied to corridor edges");
  const overlap = jammed.edges.filter(edge => baseSegments.includes(edge.segment)).length / jammed.edges.length;
  assert.ok(overlap < 0.6, `route diverges from the jammed corridor (overlap ${overlap.toFixed(2)})`);
  assert.ok(jammed.adjustedSeconds >= base.seconds, "live estimate is no faster than free flow");

  // A verified closure removes the segment outright.
  const closure = createStaticLiveProvider([{ segment: baseSegments[Math.floor(baseSegments.length / 2)], closed: true }]);
  const rerouted = await engine.route({ from, to, live: closure });
  assert.ok(!rerouted.edges.some(edge => edge.segment === baseSegments[Math.floor(baseSegments.length / 2)]),
    "closed segment is avoided");

  // Unknown epoch: provider contract returns nothing, route is static.
  const staleEpoch = createStaticLiveProvider([{ segment: baseSegments[0], factor: 6 }], { epoch: "not-this-build" });
  const stale = await engine.route({ from, to, live: staleEpoch });
  assert.equal(stale.seconds, base.seconds, "stale-epoch provider leaves the static route untouched");
  assert.equal(stale.live.applied, 0);

  // A throwing provider degrades to the static metric instead of failing.
  const broken = { name: "broken", fetch() { throw new Error("mesh unreachable"); } };
  const degraded = await engine.route({ from, to, live: broken });
  assert.equal(degraded.seconds, base.seconds, "provider failure degrades to static");
  assert.ok(degraded.live.error, "provider failure is reported");

  // Low confidence only nudges the cost: same route, milder estimate.
  const mild = createStaticLiveProvider(baseSegments.map(segment => ({
    segment,
    factor: 6,
    confidence: 0.1,
    observedAt: Date.now()
  })));
  const nudged = await engine.route({ from, to, live: mild });
  assert.ok(nudged.adjustedSeconds < jammed.adjustedSeconds, "confidence blending softens the live estimate");
});

test("itinerary orders stops and concatenates legs", async (t) => {
  const graph = syntheticGraph(16, 16, 5);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-trip-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 40, fanout: 4, topMaxCells: 5 });
  const engine = await openRouteGraphDir(dir);
  const nodeAt = (x, y) => y * 16 + x;
  const point = (node) => ({ lat: graph.nodeLat[node] / 1e7, lon: graph.nodeLon[node] / 1e7 });
  const stops = [point(nodeAt(0, 0)), point(nodeAt(14, 14)), point(nodeAt(2, 13)), point(nodeAt(13, 1)), point(nodeAt(7, 7))];
  const trip = await engine.itinerary({ stops, geometry: false });
  assert.equal(trip.order[0], 0, "itinerary starts at the first stop");
  assert.equal(trip.order[trip.order.length - 1], stops.length - 1, "itinerary ends at the last stop");
  assert.equal(new Set(trip.order).size, stops.length, "every stop visited once");
  assert.equal(trip.legs.length, stops.length - 1);
  const naiveSeconds = [];
  for (let i = 0; i + 1 < stops.length; i++) {
    const leg = await engine.route({ from: stops[i], to: stops[i + 1], geometry: false });
    naiveSeconds.push(leg.seconds);
  }
  const naiveTotal = naiveSeconds.reduce((a, b) => a + b, 0);
  assert.ok(trip.totalSeconds <= naiveTotal + 1e-9, "optimized order is no worse than input order");
});

test("an open-ended itinerary is free to finish at the last delivery", async (t) => {
  const graph = syntheticGraph(20, 20, 11);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-open-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 40, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);
  const nodeAt = (x, y) => y * 20 + x;
  const point = (node) => ({ lat: graph.nodeLat[node] / 1e7, lon: graph.nodeLon[node] / 1e7 });

  // The depot, three far corners, and — typed last, as a dispatcher would —
  // a drop two blocks from the depot. Pinning that last address as the
  // terminus forces a long haul home; letting the end float does not.
  const stops = [
    point(nodeAt(0, 0)),
    point(nodeAt(18, 1)),
    point(nodeAt(18, 18)),
    point(nodeAt(1, 18)),
    point(nodeAt(2, 2))
  ];

  const fixed = await engine.itinerary({ stops, geometry: false });
  const open = await engine.itinerary({ stops, openEnd: true, geometry: false });

  assert.equal(open.order[0], 0, "an open-ended trip still starts at stop 0");
  assert.deepEqual([...open.order].sort((a, b) => a - b), [0, 1, 2, 3, 4], "every stop visited exactly once");
  assert.equal(open.legs.length, stops.length - 1);
  // Any fixed-end order is a legal open-ended order, so this direction is
  // structural; the crafted layout makes it strict.
  assert.ok(open.totalSeconds <= fixed.totalSeconds + 1e-9, "a free end is never worse than a pinned one");
  assert.ok(open.totalSeconds < fixed.totalSeconds, "the free end skips the haul back to the last-typed address");
  assert.notEqual(open.order[open.order.length - 1], stops.length - 1, "the last-typed stop is no longer the terminus");

  // Round trips are untouched by the new mode.
  const loop = await engine.itinerary({ stops, roundTrip: true, geometry: false });
  assert.equal(loop.order.length, stops.length + 1, "a round trip has one extra hop");
  assert.equal(loop.order[0], 0);
  assert.equal(loop.order[loop.order.length - 1], 0, "a round trip comes home");
  assert.equal(new Set(loop.order).size, stops.length, "a round trip visits every stop once");

  await assert.rejects(
    () => engine.itinerary({ stops, openEnd: true, roundTrip: true, geometry: false }),
    /cannot both come back to the start and end anywhere/,
    "the two end modes are mutually exclusive"
  );
});

test("an open-ended itinerary past the exact bound falls back to 2-opt", async (t) => {
  const graph = syntheticGraph(20, 20, 29);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-open-heuristic-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 40, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);
  const nodeAt = (x, y) => y * 20 + x;
  const point = (node) => ({ lat: graph.nodeLat[node] / 1e7, lon: graph.nodeLon[node] / 1e7 });

  // Twelve stops: eleven interior under `openEnd`, one past the Held-Karp
  // bound, entered in a deliberately bad zig-zag so the heuristic has
  // something to fix.
  const cells = [
    [0, 0], [17, 2], [2, 5], [15, 7], [4, 10], [13, 12],
    [6, 15], [11, 17], [8, 3], [3, 14], [16, 16], [9, 9]
  ];
  const stops = cells.map(([x, y]) => point(nodeAt(x, y)));
  const trip = await engine.itinerary({ stops, openEnd: true, geometry: false });

  assert.equal(trip.order[0], 0, "the heuristic still starts at stop 0");
  assert.deepEqual(
    [...trip.order].sort((a, b) => a - b),
    cells.map((_, index) => index),
    "the heuristic order is a permutation of the stops"
  );
  assert.equal(trip.legs.length, stops.length - 1);
  assert.ok(Number.isFinite(trip.totalSeconds) && trip.totalSeconds > 0, "the trip has a finite cost");

  const { seconds } = await engine.matrix({ points: stops });
  let identityTotal = 0;
  for (let i = 0; i + 1 < stops.length; i++) identityTotal += seconds[i][i + 1];
  assert.ok(trip.totalSeconds <= identityTotal + 1e-6, "the heuristic beats the order the stops were typed in");
});

test("a reported heading prices turning around at the origin", async (t) => {
  const graph = syntheticGraph(14, 14, 73);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-route-heading-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 40, fanout: 4, topMaxCells: 5 });
  const engine = await openRouteGraphDir(dir);
  const nodeAt = (x, y) => y * 14 + x;
  const point = (node) => ({ lat: graph.nodeLat[node] / 1e7, lon: graph.nodeLon[node] / 1e7 });

  // Sit the driver between two nodes of one east-west street so both
  // directions of it are near-equal snap candidates, and send them somewhere
  // the search has to leave that street to reach.
  const west = point(nodeAt(4, 6));
  const east = point(nodeAt(5, 6));
  const from = { lat: (west.lat + east.lat) / 2, lon: (west.lon + east.lon) / 2 };
  const to = point(nodeAt(9, 10));

  const plain = await engine.route({ from, to, geometry: false });
  const eastward = await engine.route({ from, to, geometry: false, fromHeading: 90 });
  const westward = await engine.route({ from, to, geometry: false, fromHeading: 270 });

  // Driving with the edge the route already wanted costs nothing extra.
  assert.equal(eastward.seconds, plain.seconds, "aligned heading leaves the route untouched");
  // Driving against it has to pay for the U-turn it implies.
  assert.ok(
    westward.seconds > plain.seconds,
    `opposed heading should cost more than ${plain.seconds}s, got ${westward.seconds}s`
  );
  // The charge is the configured one, not an arbitrary detour.
  const free = await engine.route({ from, to, geometry: false, fromHeading: 270, headingPenaltySeconds: 0 });
  assert.equal(free.seconds, plain.seconds, "a zero penalty restores the unbiased route");
});

test("step.at marks the junction a turn happens at, not a point down the street", async (t) => {
  // A step's `at` is the geometry index where that street begins. Reading it
  // after the first edge's points were appended put it part-way along the new
  // street, where the road is straight by construction — so every maneuver
  // arrow pointed straight ahead and spoken guidance said "continue" into
  // ninety-degree turns. On a grid city the corners are unmistakable.
  const graph = syntheticGraph(20, 18);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-step-at-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 48, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);

  const toRad = value => (value * Math.PI) / 180;
  const bearing = (a, b) => {
    const y = Math.sin(toRad(b[1] - a[1])) * Math.cos(toRad(b[0]));
    const x = Math.cos(toRad(a[0])) * Math.sin(toRad(b[0])) -
      Math.sin(toRad(a[0])) * Math.cos(toRad(b[0])) * Math.cos(toRad(b[1] - a[1]));
    return ((Math.atan2(y, x) * 180) / Math.PI + 360) % 360;
  };
  const signedDelta = (from, to) => {
    let delta = to - from;
    while (delta > 180) delta -= 360;
    while (delta < -180) delta += 360;
    return delta;
  };

  const random = lcg(11);
  let turnsSeen = 0;
  let stepsSeen = 0;
  for (let i = 0; i < 25; i++) {
    const fromNode = Math.floor(random() * graph.nodeLat.length);
    const toNode = Math.floor(random() * graph.nodeLat.length);
    const from = { lat: graph.nodeLat[fromNode] / 1e7, lon: graph.nodeLon[fromNode] / 1e7 };
    const to = { lat: graph.nodeLat[toNode] / 1e7, lon: graph.nodeLon[toNode] / 1e7 };
    const route = await engine.route({ from, to });
    if (!route.steps?.length || route.geometry.length < 3) continue;

    // The first street is not turned onto; it is simply where the trip starts.
    assert.equal(route.steps[0].at, 0, "the first step begins at the first point");

    for (let s = 0; s < route.steps.length; s++) {
      const at = route.steps[s].at;
      assert.ok(Number.isInteger(at) && at >= 0, `step ${s} has an index`);
      assert.ok(at < route.geometry.length, `step ${s} indexes inside the geometry`);
      // Steps march forward through the polyline; a step that begins before
      // its predecessor would make any slice of per-street geometry wrong.
      if (s > 0) assert.ok(at >= route.steps[s - 1].at, `step ${s} does not go backwards`);
      stepsSeen++;
      if (at <= 0 || at >= route.geometry.length - 1) continue;
      const delta = Math.abs(signedDelta(
        bearing(route.geometry[at - 1], route.geometry[at]),
        bearing(route.geometry[at], route.geometry[at + 1])
      ));
      if (delta > 45) turnsSeen++;
    }
  }

  assert.ok(stepsSeen > 0, "expected the sample to produce steps");
  // On a grid, changing street means turning a corner. Before the fix this
  // count was zero for every route in the sample.
  assert.ok(turnsSeen > 0, `expected real turns at step boundaries, saw ${turnsSeen}`);
});

test("lane movements survive the cell codec, jagged and mostly absent", async () => {
  const { encodeRouteCell: encode, decodeRouteCell: decode } = await import("../src/route_graph.js");
  const cell = {
    cellId: 3,
    firstNode: 0,
    nodeCount: 2,
    latE7: Int32Array.from([455000000, 455000500]),
    lonE7: Int32Array.from([-736000000, -735999000]),
    rowStart: Uint32Array.from([0, 2, 3]),
    targets: Uint32Array.from([1, 2, 0]),
    weights: Uint32Array.from([10, 20, 30]),
    distsDm: Uint32Array.from([100, 200, 300]),
    nameIds: Uint32Array.from([0, 1, 2]),
    classes: Uint8Array.from([0, 0, 0]),
    speeds: Uint8Array.from([50, 0, 90]),
    // A two-lane approach, an untagged edge, and a three-lane one. Most roads
    // are the middle case, which is why the column is jagged.
    lanes: [[4 | 16, 64], null, [4, 16, 16 | 64]],
    extLat: Int32Array.from([0, 0, 0]),
    extLon: Int32Array.from([0, 0, 0]),
    geomRefs: Uint32Array.from([0, 1, 2])
  };
  const decoded = decode(encode(cell));
  const lanesOf = edge =>
    Array.from(decoded.laneMasks.subarray(decoded.laneOffsets[edge], decoded.laneOffsets[edge + 1]));
  assert.deepEqual(lanesOf(0), [20, 64], "left+through then right");
  assert.deepEqual(lanesOf(1), [], "an untagged edge carries no lanes");
  assert.deepEqual(lanesOf(2), [4, 16, 80]);
  // Posted speeds share the same rows and must not be disturbed by the
  // variable-length column sitting beside them.
  assert.deepEqual([...decoded.speeds], [50, 0, 90]);
});

test("turn:lanes tags parse into per-direction movement bits", async () => {
  const { wayLanes } = await import("../scripts/osm_road_graph.mjs");
  const tags = pairs => new Map(pairs);
  const LEFT = 4, THROUGH = 16, RIGHT = 64, SLIGHT_RIGHT = 32, REVERSE = 1;

  // The shape OSM actually uses: one entry per lane, ";" for a lane that
  // serves more than one movement.
  assert.deepEqual(
    wayLanes(tags([["turn:lanes", "left|through;right"]]), true).forward,
    [LEFT, THROUGH | RIGHT]
  );
  // An empty entry is a lane with no movement tagged, not a missing lane.
  assert.deepEqual(
    wayLanes(tags([["turn:lanes", "left||right"]]), true).forward,
    [LEFT, 0, RIGHT]
  );
  // Suffixed tags win, and each direction gets its own approach.
  const twoWay = wayLanes(tags([
    ["turn:lanes:forward", "through|right"],
    ["turn:lanes:backward", "reverse;left"]
  ]), false);
  assert.deepEqual(twoWay.forward, [THROUGH, RIGHT]);
  assert.deepEqual(twoWay.backward, [REVERSE | LEFT]);
  // A one-way's unsuffixed tag describes its only direction; nothing flows
  // backwards along it.
  const oneWay = wayLanes(tags([["turn:lanes", "through|right"]]), true);
  assert.deepEqual(oneWay.backward, []);
  // Merges read as the gentle turns they are.
  assert.deepEqual(
    wayLanes(tags([["turn:lanes", "merge_to_right"]]), true).forward,
    [SLIGHT_RIGHT]
  );
  // A lane count with no movements still says how many lanes there are.
  assert.deepEqual(wayLanes(tags([["lanes", "3"]]), true).forward, [0, 0, 0]);
  // Nonsense is dropped rather than guessed at.
  assert.deepEqual(wayLanes(tags([["turn:lanes", "sideways|"]]), true).forward, [0, 0]);
  assert.deepEqual(wayLanes(tags([]), true).forward, []);
});

test("one intersection reports one junction, however many nodes describe it", async () => {
  // A wide signalised crossroads is tagged in OSM as a signal node on each
  // approach, with a pedestrian crossing either side of it: separate nodes,
  // metres apart, all describing the one intersection a driver is about to
  // cross. Reported as they are, the map draws two sets of lights for it.
  const { mergesWithPreviousJunction } = await import("../src/route_graph_query.js");
  const at = (lat, lon, kind) => ({ lat, lon, kind });
  const SIGNAL = 1, STOP = 2, CROSSING = 5;

  // Roughly 9 m north of 45.5, -73.6: the far approach of one crossroads.
  assert.equal(mergesWithPreviousJunction(at(45.5, -73.6, SIGNAL), SIGNAL, 455000800, -736000000), true);
  // Crossings on either side of the same intersection, likewise.
  assert.equal(mergesWithPreviousJunction(at(45.5, -73.6, CROSSING), CROSSING, 455001500, -736000000), true);
  // A different kind is a different thing, however close: a stop line beside
  // a crossing is two facts about the road, not one drawn twice.
  assert.equal(mergesWithPreviousJunction(at(45.5, -73.6, SIGNAL), STOP, 455000200, -736000000), false);
  // Far enough apart to be the next intersection along, roughly 90 m.
  assert.equal(mergesWithPreviousJunction(at(45.5, -73.6, SIGNAL), SIGNAL, 455008000, -736000000), false);
  // The first junction of a drive has nothing to merge with.
  assert.equal(mergesWithPreviousJunction(null, SIGNAL, 455000000, -736000000), false);

  // The width that started this: Chemin de la Grande-Côte crosses Boulevard
  // Labelle in Rosemère with a carriageway and a turn lane on each side, and
  // its two signal markers sit 31 m apart. One left turn, one red light — the
  // driver was shown two.
  const wide = at(45.6243114, -73.8025515, SIGNAL);
  wide.atMeters = 278.7;
  assert.equal(mergesWithPreviousJunction(wide, SIGNAL, 456240382, -738026403, 330.5), true);

  // Same two coordinates, met 300 m apart along the route: a block circled,
  // not a crossroads crossed. Distance alone cannot tell those apart, which is
  // why the radius may only be this wide with the along-route bound beside it.
  assert.equal(mergesWithPreviousJunction(wide, SIGNAL, 456240382, -738026403, 590.0), false);
});

test("one intersection costs one wait, however many nodes describe it", async () => {
  // The cost counterpart of the junction merge above, and the one that was
  // missing. A divided boulevard carries a signal on each carriageway and a
  // pedestrian crossing on each arm; charged in turn, crossing one road cost
  // two or three signal waits. In Rosemère that priced the twelve metres of
  // Chemin de la Grande-Côte between Boulevard Labelle's carriageways at
  // twenty-six seconds, and the router answered by leaving the road, running
  // a hundred metres up the boulevard and coming back — a detour on a road
  // the driver could see was straight.
  const { chargeEachIntersectionOnce } = await import("../scripts/osm_road_graph.mjs");
  const SIGNAL = 1, CROSSING = 5;
  const quiet = () => {};

  // Four nodes of one intersection: a signal on each carriageway of a divided
  // road, and a crossing beside each. Spread over about 20 m.
  const ids = new Float64Array([1, 2, 3, 4]);
  const lat = new Int32Array([455000000, 455000900, 455001800, 455000400]);
  const lon = new Int32Array([-736000000, -736000000, -736000000, -736000000]);
  const penalty = new Uint16Array([100, 100, 50, 50]);
  const kind = new Uint8Array([SIGNAL, SIGNAL, CROSSING, CROSSING]);
  chargeEachIntersectionOnce(ids, lat, lon, penalty, kind, quiet);

  const charged = [...penalty].filter(value => value > 0);
  assert.deepEqual(charged, [100], "one intersection should cost one signal wait");
  // The heaviest node is the one kept: a driver waits for the light, not for
  // whichever node the scan reached first.
  assert.equal(penalty[0], 100);

  // The next intersection along is its own charge, not part of this one.
  const farIds = new Float64Array([1, 2]);
  const farLat = new Int32Array([455000000, 455008000]);   // ~90 m apart
  const farLon = new Int32Array([-736000000, -736000000]);
  const farPenalty = new Uint16Array([100, 100]);
  chargeEachIntersectionOnce(farIds, farLat, farLon, farPenalty, new Uint8Array([SIGNAL, SIGNAL]), quiet);
  assert.deepEqual([...farPenalty], [100, 100], "separate intersections each cost a wait");

  // Kind codes are the query layer's business and must survive untouched, or
  // the map stops drawing lights it should draw.
  assert.deepEqual([...kind], [SIGNAL, SIGNAL, CROSSING, CROSSING]);
});

/**
 * Boulevard Labelle through Rosemère, to the metre, as OSM has it: two
 * `oneway=yes` lines 10.4 m apart, both named, both tagged `lanes:both_ways=1`
 * and `turn:lanes:both_ways=left` — one undivided street with a painted centre
 * turn lane, drawn as two. `a*` is the south-east-bound line the driver is on,
 * `b*` the north-west-bound one; `e1` is the business being driven to, reached
 * by a driveway off `b2`, and `a2`–`b0` is the one driveway near here that is
 * mapped straight across the middle.
 */
const LABELLE = {
  s: [45.623340, -73.801530],
  a0: [45.622897, -73.800975],
  a1: [45.622461, -73.800347],
  a2: [45.622405, -73.800266],
  b0: [45.622470, -73.800170],
  b1: [45.622527, -73.800253],
  b2: [45.622771, -73.800604],
  b3: [45.622957, -73.800872],
  e1: [45.622998, -73.800302]
};

class ByteSpool {
  constructor() {
    this.data = new Uint8Array(4096);
    this.length = 0;
  }
  ensure(extra) {
    if (this.length + extra <= this.data.length) return;
    const next = new Uint8Array((this.length + extra) * 2);
    next.set(this.data.subarray(0, this.length));
    this.data = next;
  }
  push(value) {
    this.ensure(1);
    this.data[this.length++] = value;
  }
  view() {
    return this.data.subarray(0, this.length);
  }
}

function labelleGraph() {
  const order = Object.keys(LABELLE);
  const nodeLat = order.map(key => Math.round(LABELLE[key][0] * 1e7));
  const nodeLon = order.map(key => Math.round(LABELLE[key][1] * 1e7));
  const id = key => order.indexOf(key);
  const graph = {
    nodeLat, nodeLon,
    edgeFrom: [], edgeTo: [], edgeWeightDs: [], edgeDistDm: [], edgeName: [],
    edgeWay: [], edgeClass: [], edgeJunction: [], edgeSpeed: [], edgeCond: [],
    edgeSign: [], edgeFlags: [],
    geomOffsets: [0], geomBytes: new ByteSpool(),
    laneOffsets: [0], laneBytes: new ByteSpool()
  };
  const meters = (from, to) => {
    const toRad = Math.PI / 180 / 1e7;
    const dLat = (nodeLat[to] - nodeLat[from]) * toRad;
    const dLon = (nodeLon[to] - nodeLon[from]) * toRad;
    const a = Math.sin(dLat / 2) ** 2 +
      Math.cos(nodeLat[from] * toRad) * Math.cos(nodeLat[to] * toRad) * Math.sin(dLon / 2) ** 2;
    return 2 * 6371008.7714 * Math.asin(Math.min(1, Math.sqrt(a)));
  };
  const add = (fromKey, toKey, way, name, kmh = 60) => {
    const from = id(fromKey);
    const to = id(toKey);
    const length = meters(from, to);
    graph.edgeFrom.push(from);
    graph.edgeTo.push(to);
    graph.edgeWeightDs.push(Math.max(1, Math.round((length / ((kmh * 1000) / 3600)) * 10)));
    graph.edgeDistDm.push(Math.max(1, Math.round(length * 10)));
    graph.edgeName.push(name);
    graph.edgeWay.push(way);
    graph.edgeClass.push(0);
    graph.edgeJunction.push(0);
    graph.edgeSpeed.push(kmh);
    graph.edgeCond.push(0);
    graph.edgeSign.push(0);
    graph.edgeFlags.push(0);
    graph.geomBytes.push(0);
    graph.geomOffsets.push(graph.geomBytes.length);
    graph.laneBytes.push(0);
    graph.laneOffsets.push(graph.laneBytes.length);
  };
  const NAMED = 1;
  for (const pair of [["s", "a0"], ["a0", "a1"], ["a1", "a2"]]) add(...pair, 1090282700, NAMED);
  for (const pair of [["b0", "b1"], ["b1", "b2"], ["b2", "b3"]]) add(...pair, 1090282672, NAMED);
  add("a2", "b0", 1090502896, 0, 20);
  add("b0", "a2", 1090502896, 0, 20);
  add("b2", "e1", 1090508941, 0, 20);
  add("e1", "b2", 1090508941, 0, 20);
  return graph;
}

async function labelleRoute(graph) {
  const dir = mkdtempSync(join(tmpdir(), "rangefind-centre-"));
  try {
    await buildRouteGraph({
      nodeLat: Int32Array.from(graph.nodeLat),
      nodeLon: Int32Array.from(graph.nodeLon),
      edgeFrom: Uint32Array.from(graph.edgeFrom),
      edgeTo: Uint32Array.from(graph.edgeTo),
      edgeWeightDs: Uint32Array.from(graph.edgeWeightDs),
      edgeDistDm: Uint32Array.from(graph.edgeDistDm),
      edgeName: Uint32Array.from(graph.edgeName),
      edgeClass: Uint8Array.from(graph.edgeClass),
      edgeSpeed: Uint8Array.from(graph.edgeSpeed),
      geomOffsets: Uint32Array.from(graph.geomOffsets),
      geomBytes: Uint8Array.from(graph.geomBytes.view()),
      names: ["", "Boulevard Labelle"],
      profile: "car",
      classes: ["primary"]
    }, dir, { leafSize: 8, log: () => {} });
    const engine = await openRouteGraphDir(dir);
    return await engine.route({
      from: { lat: LABELLE.s[0], lon: LABELLE.s[1] },
      to: { lat: LABELLE.e1[0], lon: LABELLE.e1[1] }
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("a painted centre turn lane is a left turn, not a hundred-metre dogleg", async () => {
  const { centreTurnLane, linkCentreTurnLanes } = await import("../scripts/osm_road_graph.mjs");
  const tags = pairs => new Map(pairs);

  // What the tag looks like on the road it came from, and what it does not.
  assert.equal(centreTurnLane(tags([["lanes:both_ways", "1"], ["lanes", "3"]])), true);
  assert.equal(centreTurnLane(tags([["turn:lanes:both_ways", "left"]])), true);
  assert.equal(centreTurnLane(tags([["lanes", "3"], ["oneway", "yes"]])), false);

  // Read literally, the two lines have no way across, and the router answers
  // the way the driver was shown: past the door, over at the one driveway that
  // is mapped through the middle, and back up the other side.
  const asDrawn = await labelleRoute(labelleGraph());
  assert.ok(
    asDrawn.distanceMeters > 120,
    `the dogleg should be the long way round, got ${asDrawn.distanceMeters} m`
  );
  assert.equal(asDrawn.steps.length, 3, "past the destination, across, and back");

  const linked = labelleGraph();
  const opened = linkCentreTurnLanes({
    ...linked,
    centreTurnWays: new Set([1090282700, 1090282672]),
    log: () => {}
  });
  assert.equal(opened, 1, "one crossing, opposite the one driveway there is to turn into");

  // The crossing lands opposite the driveway rather than at either end of the
  // block, and is one carriageway wide.
  const crossings = [];
  for (let e = 0; e < linked.edgeFrom.length; e++) {
    if (linked.edgeWay[e] === 0) crossings.push(e);
  }
  assert.equal(crossings.length, 2, "a left turn is available from both sides");
  for (const e of crossings) {
    assert.ok(
      linked.edgeDistDm[e] > 80 && linked.edgeDistDm[e] < 120,
      `a crossing should be the 10.4 m between the lines, got ${linked.edgeDistDm[e] / 10} m`
    );
    // Still Boulevard Labelle: a driver waiting in the centre lane has not
    // left the street, so the maneuver is the turn out of it, not onto it.
    assert.equal(linked.edgeName[e], 1);
  }

  const direct = await labelleRoute(linked);
  assert.ok(
    direct.distanceMeters < asDrawn.distanceMeters - 60,
    `the left turn should be far shorter than the dogleg, got ${direct.distanceMeters} m`
  );
  assert.equal(direct.steps.length, 1, "one street, one turn off it");
  // And it must not have gone past the destination to get there: every point
  // stays north-west of the driveway the dogleg used.
  for (const [, lon] of direct.geometry) {
    assert.ok(lon < LABELLE.b0[1], `route reached ${lon}, past the far driveway`);
  }
});

test("the cycling profile prefers cycle infrastructure over arterials", async () => {
  const { PROFILES } = await import("../scripts/osm_road_graph.mjs");
  const bike = PROFILES.bike;
  const tags = pairs => new Map(pairs);

  // A cycleway has to be enough quicker than a main road to survive being
  // longer. At a few percent apart, any cycleway that detours at all loses,
  // and riders get sent down arterials to save seconds.
  const cycleway = bike.speeds.cycleway;
  const primary = bike.speeds.primary;
  assert.ok(
    cycleway >= primary * 1.5,
    `a cycleway at ${cycleway} against a primary at ${primary} is not a preference`
  );
  assert.ok(bike.speeds.residential > bike.speeds.primary, "a quiet street beats an arterial");
  assert.ok(bike.speeds.secondary > bike.speeds.primary, "and a smaller road beats a bigger one");

  // Infrastructure painted on an ordinary road counts too — OSM records it on
  // the road itself, which the profile used to read only for contraflow.
  const plain = bike.adjustSpeed(tags([]), 15);
  assert.equal(plain, 15, "a road with nothing tagged is unchanged");
  assert.ok(bike.adjustSpeed(tags([["cycleway", "lane"]]), 15) > plain, "a marked lane is better");
  assert.ok(bike.adjustSpeed(tags([["cycleway:right", "track"]]), 15) > plain, "so is one side of it");
  assert.ok(bike.adjustSpeed(tags([["bicycle", "designated"]]), 15) > plain, "so is a designated way");
  // "no" is a statement that there is none, and must not read as one.
  assert.equal(bike.adjustSpeed(tags([["cycleway", "no"]]), 15), plain);
  assert.equal(bike.adjustSpeed(tags([["cycleway", "none"]]), 15), plain);
  // Preference is not licence to invent a speed nobody rides at.
  assert.ok(bike.adjustSpeed(tags([["cycleway", "track"]]), 18) <= 20);
});

test("roads a rider or walker may not use are absent, not merely avoided", async () => {
  const { PROFILES } = await import("../scripts/osm_road_graph.mjs");
  const tags = pairs => new Map(pairs);
  const { bike, foot, car } = PROFILES;

  // Motorways and trunk roads are excluded by never being emitted, which is
  // stronger than pricing them badly: no weighting mistake can put a cyclist
  // on one, because the edge does not exist in that graph.
  for (const highway of ["motorway", "motorway_link", "trunk", "trunk_link"]) {
    const way = tags([["highway", highway]]);
    assert.equal(bike.allowed(way), false, `${highway} must not exist for cycling`);
    assert.equal(foot.allowed(way), false, `${highway} must not exist for walking`);
    assert.equal(car.allowed(way), true, `${highway} is exactly where a car belongs`);
    assert.ok(!(highway in bike.speeds), `${highway} must not be in the cycling speed table`);
    assert.ok(!(highway in foot.speeds), `${highway} must not be in the walking speed table`);
  }

  // The dangerous case is the one the highway class does not reveal:
  // motorroad=yes carries motorway rules onto an ordinary road.
  const expressway = tags([["highway", "primary"], ["motorroad", "yes"]]);
  assert.equal(bike.allowed(expressway), false);
  assert.equal(foot.allowed(expressway), false);
  assert.equal(car.allowed(expressway), true);
  // Without it, the same road is an ordinary primary and perfectly usable.
  const ordinary = tags([["highway", "primary"]]);
  assert.equal(bike.allowed(ordinary), true);
  assert.equal(foot.allowed(ordinary), true);

  // An explicit permission outranks the ban: a bridge carrying a cycle track
  // over an expressway says so on itself.
  assert.equal(bike.allowed(tags([["highway", "primary"], ["motorroad", "yes"], ["bicycle", "yes"]])), true);
  assert.equal(foot.allowed(tags([["highway", "primary"], ["motorroad", "yes"], ["foot", "yes"]])), true);
  // And an explicit ban is still a ban.
  assert.equal(bike.allowed(tags([["highway", "residential"], ["bicycle", "no"]])), false);
  assert.equal(foot.allowed(tags([["highway", "residential"], ["foot", "no"]])), false);
});

test("a car park you are being sent to is reachable, but not a shortcut", async () => {
  const { PROFILES } = await import("../scripts/osm_road_graph.mjs");
  const tags = pairs => new Map(pairs);
  const { car } = PROFILES;

  // How a shopping centre is tagged. "customers" means you may drive here if
  // you are one — which is exactly what somebody being routed to the centre
  // is. Reading it as a refusal made every aisle on the site invisible: no
  // route in, nothing to say about which entrance, and a driver crossing the
  // car park permanently off-route.
  assert.equal(car.allowed(tags([["highway", "service"], ["service", "parking_aisle"], ["access", "customers"]])), true);
  assert.equal(car.allowed(tags([["highway", "service"], ["access", "customers"]])), true);
  assert.equal(car.allowed(tags([["highway", "service"], ["service", "drive-through"], ["access", "customers"]])), true);
  assert.equal(car.allowed(tags([["highway", "unclassified"], ["access", "destination"]])), true);

  // What is genuinely closed stays closed.
  assert.equal(car.allowed(tags([["highway", "service"], ["service", "driveway"], ["access", "private"]])), false);
  assert.equal(car.allowed(tags([["highway", "residential"], ["access", "no"]])), false);
  assert.equal(car.allowed(tags([["highway", "service"], ["access", "delivery"]])), false);
  assert.equal(car.allowed(tags([["highway", "service"], ["motor_vehicle", "no"]])), false);

  // Reachable is not the same as worth driving through. A destination-only
  // way is slowed so it never wins as a shortcut past a queue on the road
  // outside, while still being available when it is the destination.
  const { waySpeeds } = await import("../scripts/osm_road_graph.mjs");
  const open = waySpeeds(tags([["highway", "service"]]), car).forward;
  const restricted = waySpeeds(tags([["highway", "service"], ["access", "customers"]]), car).forward;
  assert.ok(
    restricted < open,
    `a customers-only way at ${restricted} should be slower than an open one at ${open}`
  );
  assert.ok(restricted > 0, "slowed, not closed");
});

test("conditional speed limits are read the way they are actually tagged", async () => {
  const { parseConditionalMaxspeed } = await import("../scripts/osm_road_graph.mjs");

  // The four syntaxes Québec actually uses for the same idea. Two of them are
  // not valid opening-hours syntax, which is why this parses what is written
  // rather than what the wiki specifies.
  const monToFri = 0b0011111;
  assert.deepEqual(parseConditionalMaxspeed("30 @ Mo-Fr 07:00-19:00"), {
    speedKmh: 30, days: monToFri, startMinute: 420, endMinute: 1140, monthStart: 1, monthEnd: 12
  });
  assert.deepEqual(parseConditionalMaxspeed("30 @ (Sep-Jun AND Mo-Fr 07:00-17:00)"), {
    speedKmh: 30, days: monToFri, startMinute: 420, endMinute: 1020, monthStart: 9, monthEnd: 6
  });
  assert.deepEqual(parseConditionalMaxspeed("30 @ (Sep-Jun: Mo-Fr 07:00-17:00)"), {
    speedKmh: 30, days: monToFri, startMinute: 420, endMinute: 1020, monthStart: 9, monthEnd: 6
  });
  // No day range means every day, which is what a bare time range says.
  assert.deepEqual(parseConditionalMaxspeed("30 @ (07:00-21:00)"), {
    speedKmh: 30, days: 0b1111111, startMinute: 420, endMinute: 1260, monthStart: 1, monthEnd: 12
  });

  // Sep-Jun is a school year, not a mistake: the range wraps the new year and
  // must survive as written rather than being normalised into nothing.
  const school = parseConditionalMaxspeed("30 @ (Sep-Jun AND Mo-Fr 07:00-17:00)");
  assert.equal(school.monthStart, 9);
  assert.equal(school.monthEnd, 6);

  // A day range may wrap the week too.
  assert.equal(parseConditionalMaxspeed("30 @ Fr-Mo 07:00-19:00").days, 0b1110001);
  assert.equal(parseConditionalMaxspeed("30 @ Mo,We,Fr 07:00-19:00").days, 0b0010101);

  // Anything not understood keeps the posted limit rather than inventing one.
  // A limit the app made up is worse than no limit: the driver learns to
  // distrust the sign, and then it is worthless when it is right.
  for (const unknown of [
    "",
    null,
    "wet 80",                 // condition is weather, not time
    "30 @ flashing",          // depends on a beacon we cannot see
    "30 @ (May 15-Oct 15)",   // seasonal, no time of day
    "@ Mo-Fr 07:00-19:00",    // no speed
    "30 @ Mo-Fr",             // no times
    "30 @ Mo-Fr 19:00-07:00"  // reversed; overnight windows are not modelled
  ]) {
    assert.equal(parseConditionalMaxspeed(unknown), null, `should refuse: ${unknown}`);
  }
});

test("a posted limit that changes mid-street is reported where it changes", async (t) => {
  // One street, three limits: the shape of an autoroute that drops through an
  // interchange and climbs back. Reported per step, this is a single number —
  // the one covering the most metres — so the sign reads 90 while the driver
  // is in the 70. Reported over distance, it is three answers in the right
  // places, and the app can ask which one applies where the car actually is.
  const graph = syntheticGraph(12, 3);
  // Every edge on one name, so the whole route collapses into one step and the
  // per-step answer has nowhere left to hide.
  graph.edgeName = Uint32Array.from(graph.edgeName, () => 1);
  graph.edgeSpeed = Uint8Array.from(graph.edgeFrom, (_, i) => {
    const third = Math.floor((i / graph.edgeFrom.length) * 3);
    return third === 1 ? 70 : 90;
  });

  const dir = mkdtempSync(join(tmpdir(), "rangefind-limits-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 24, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);

  const from = { lat: graph.nodeLat[0] / 1e7, lon: graph.nodeLon[0] / 1e7 };
  const last = graph.nodeLat.length - 1;
  const to = { lat: graph.nodeLat[last] / 1e7, lon: graph.nodeLon[last] / 1e7 };
  const route = await engine.route({ from, to });

  assert.ok(Array.isArray(route.speedLimits), "a route reports where its limits change");
  assert.ok(route.speedLimits.length >= 1, "at least one limit entry");
  assert.equal(route.speedLimits[0].atMeters, 0, "the first entry starts at the trip's start");

  // Entries march forward and never repeat a limit back to back, or the app
  // would be told the limit changed when it did not.
  for (let i = 1; i < route.speedLimits.length; i++) {
    assert.ok(
      route.speedLimits[i].atMeters > route.speedLimits[i - 1].atMeters,
      "limit changes are ordered by distance"
    );
    assert.notEqual(
      route.speedLimits[i].limitKmh,
      route.speedLimits[i - 1].limitKmh,
      "consecutive entries differ, or the run was not collapsed"
    );
  }

  // The limit in force at a distance is the last entry at or before it, which
  // is the lookup the app performs. Every edge's own limit must agree with it,
  // because that is the whole promise being made.
  const limitAt = (meters) => {
    let answer = 0;
    for (const entry of route.speedLimits) {
      if (entry.atMeters <= meters + 1e-6) answer = entry.limitKmh;
      else break;
    }
    return answer;
  };
  let travelled = 0;
  for (const edge of route.edges) {
    // Mid-edge, so the reading is unambiguous at a boundary.
    assert.equal(
      limitAt(travelled + edge.meters / 2),
      edge.speedLimitKmh,
      `the limit at ${Math.round(travelled)} m should match the edge under it`
    );
    travelled += edge.meters;
  }
});

test("a conditional limit survives the index and says when it applies", async (t) => {
  const graph = syntheticGraph(10, 3);
  graph.edgeName = Uint32Array.from(graph.edgeName, () => 1);
  graph.edgeSpeed = Uint8Array.from(graph.edgeFrom, () => 50);
  // A school-hours window on the middle third, the way an extract would carry
  // it: the posted limit stays 50 and the conditional rides beside it.
  graph.condRules = [
    { speedKmh: 30, days: 0b0011111, startMinute: 7 * 60, endMinute: 17 * 60, monthStart: 9, monthEnd: 6 }
  ];
  graph.edgeCond = Uint8Array.from(graph.edgeFrom, (_, i) =>
    Math.floor((i / graph.edgeFrom.length) * 3) === 1 ? 1 : 0
  );

  const dir = mkdtempSync(join(tmpdir(), "rangefind-cond-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 24, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);

  assert.equal(engine.root.condRules.length, 1, "the root carries the window");
  assert.equal(engine.root.condRules[0].speedKmh, 30);
  assert.equal(engine.root.condRules[0].monthStart, 9, "a school year starts in September");
  assert.equal(engine.root.condRules[0].monthEnd, 6, "and ends in June, wrapping the new year");

  const from = { lat: graph.nodeLat[0] / 1e7, lon: graph.nodeLon[0] / 1e7 };
  const last = graph.nodeLat.length - 1;
  const to = { lat: graph.nodeLat[last] / 1e7, lon: graph.nodeLon[last] / 1e7 };
  const route = await engine.route({ from, to });

  const conditioned = route.speedLimits.filter(entry => entry.conditional);
  assert.ok(conditioned.length >= 1, "the route reports where a conditional limit begins");
  const entry = conditioned[0];
  assert.equal(entry.limitKmh, 50, "the posted limit is still the posted limit");
  assert.equal(entry.conditional.limitKmh, 30, "and the conditional one rides beside it");
  assert.equal(entry.conditional.startMinute, 420);
  assert.equal(entry.conditional.endMinute, 1020);

  // The index must not resolve the window itself. It is static and the answer
  // depends on the clock, so an index that picked one would be wrong for
  // every hour it was not rebuilt in.
  assert.notEqual(entry.limitKmh, entry.conditional.limitKmh);

  // An arrival time is a promise, so the hours spent at the lower limit have
  // to be in it. The route is otherwise identical — 757 edges in a province
  // cannot change which way is quickest — so only the clock moves the number.
  const schoolMorning = await engine.route({ from, to, departAt: "2026-10-08T08:15:00" });
  const sameEvening = await engine.route({ from, to, departAt: "2026-10-08T20:15:00" });
  const summer = await engine.route({ from, to, departAt: "2026-07-08T08:15:00" });
  const saturday = await engine.route({ from, to, departAt: "2026-10-10T08:15:00" });

  assert.ok(
    schoolMorning.seconds > sameEvening.seconds,
    `a school morning (${schoolMorning.seconds.toFixed(1)}s) should cost more than the same evening (${sameEvening.seconds.toFixed(1)}s)`
  );
  assert.ok(schoolMorning.conditionalDelaySeconds > 0, "and say how much of it was the zone");
  assert.equal(summer.seconds, sameEvening.seconds, "July is not the school year");
  assert.equal(saturday.seconds, sameEvening.seconds, "Saturday is not a school day");
  assert.equal(
    (await engine.route({ from, to })).seconds,
    sameEvening.seconds,
    "with no departure given, the posted limit stands"
  );
});

test("a stop sign is charged and drawn only to the approach it faces", async () => {
  // OSM tags 36,704 of Québec's stop nodes `direction=forward` and 31,468
  // `direction=backward`, against 4,882 with no direction at all — the
  // direction is the rule, not the exception. Reading the node without it put
  // a stop sign in front of every driver who passed, including the ones on
  // the road with right of way: a drive down Rue Main drew three stops the
  // driver never had to make, all of them belonging to the side streets.
  const { chargeEachIntersectionOnce, signFacing, FACES_FORWARD, FACES_BACKWARD, FACES_BOTH } =
    await import("../scripts/osm_road_graph.mjs");
  const tags = pairs => new Map(pairs);

  assert.equal(signFacing(tags([["direction", "forward"]])), FACES_FORWARD);
  assert.equal(signFacing(tags([["direction", "backward"]])), FACES_BACKWARD);
  assert.equal(signFacing(tags([["direction", "both"]])), FACES_BOTH);
  // A bearing or a cardinal cannot be resolved without the way's heading at
  // that node, so it stays visible to both. A stop shown that is not there is
  // a smaller fault than a stop hidden that is.
  assert.equal(signFacing(tags([["direction", "225"]])), FACES_BOTH);
  assert.equal(signFacing(tags([])), FACES_BOTH);

  const quiet = () => {};
  const STOP = 2;
  // Two stop signs at one crossroads, facing opposite ways and close enough
  // that the intersection merge would otherwise treat them as one.
  const ids = new Float64Array([1, 2]);
  const lat = new Int32Array([455000000, 455000900]);
  const lon = new Int32Array([-736000000, -736000000]);
  const kind = new Uint8Array([STOP, STOP]);
  const faces = new Uint8Array([FACES_FORWARD, FACES_BACKWARD]);

  const forward = new Uint16Array([20, 20]);
  chargeEachIntersectionOnce(ids, lat, lon, forward, kind, quiet, faces, FACES_FORWARD);
  assert.deepEqual([...forward], [20, 0], "only the sign facing this driver is charged");

  const backward = new Uint16Array([20, 20]);
  chargeEachIntersectionOnce(ids, lat, lon, backward, kind, quiet, faces, FACES_BACKWARD);
  assert.deepEqual([...backward], [0, 20], "and the opposite driver pays their own");

  // The merge must not collapse across directions, or one carriageway would
  // absorb the other's charge and the other would pay nothing at all.
  const both = new Uint16Array([20, 20]);
  chargeEachIntersectionOnce(ids, lat, lon, both, kind, quiet, faces, FACES_BOTH);
  assert.deepEqual(
    [...both].filter(Boolean).length, 1,
    "two signs facing one driver at one intersection are still one wait"
  );
});

test("what the signs say survives the index — number, exit, and where it leads", async (t) => {
  // A motorway's name is the one thing never written on a sign. Guiding by it
  // announced roads by a label the driver could not see, and announced the
  // exit — the instruction on a motorway that has to be right — as a nameless
  // ramp. Québec tags `ref` on 12,605 of 12,866 motorway ways and
  // `destination:ref` on 5,739 slip roads, which is where "40 Ouest" is.
  const { makeSignTable, waySigns } = await import("../scripts/osm_road_graph.mjs");
  const tags = pairs => new Map(pairs);

  const ramp = waySigns(tags([
    ["junction:ref", "32"],
    ["destination:ref", "20 Est;30"],
    ["destination", "Québec;Sorel-Tracy"]
  ]));
  assert.equal(ramp.forward.exit, "32");
  assert.equal(ramp.forward.destRef, "20 Est;30", "semicolon lists stay as OSM writes them");
  assert.equal(ramp.forward.dest, "Québec;Sorel-Tracy");

  // A two-way road signed differently at each end: taking the unsuffixed
  // value for both would point half its traffic at the wrong city.
  const split = waySigns(tags([
    ["destination:ref", "20 Est"],
    ["destination:ref:backward", "20 Ouest"]
  ]));
  assert.equal(split.forward.destRef, "20 Est");
  assert.equal(split.backward.destRef, "20 Ouest");

  // `destination:street` is the same promise in words where the target is a
  // street rather than a numbered route.
  assert.equal(waySigns(tags([["destination:street", "Boulevard Labelle"]])).forward.dest, "Boulevard Labelle");
});

test("the same number spelled two ways is one number, and knows its scheme", async () => {
  const { mergeRefs, refKey } = await import("../scripts/osm_road_graph.mjs");

  // A Luxembourg motorway carries `ref=A 1` on the way and `ref=A1` on the
  // route relation that collects it. Read as two roads, the sign came back
  // "A 1;A1;E44" — the driver's own number, said twice, in front of the one
  // they were actually looking for.
  assert.equal(mergeRefs("A 1", ["A1", "E44"]), "A 1;E44");
  assert.equal(refKey("A 1"), refKey("a1"));
  assert.equal(refKey("CR 191B"), "CR191B");

  // The relation often drops the letter as well: the way says "N 4" and the
  // relation says "4". A bare number repeating one already listed with its
  // letter is that same road again.
  assert.equal(mergeRefs("N 4", ["4"]), "N 4");
  assert.equal(mergeRefs("N 11", ["11", "E29"]), "N 11;E29");

  // Numbers that genuinely differ all survive, which is the whole point of
  // reading the relations in the first place.
  assert.equal(mergeRefs("A 4", ["E 50"]), "A 4;E 50");
  assert.equal(mergeRefs("", ["E 25"]), "E 25");
});

test("a sign table gives one entry per distinct face", async (t) => {
  const { makeSignTable } = await import("../scripts/osm_road_graph.mjs");
  const table = makeSignTable();
  assert.equal(table.idFor({ ref: "", exit: "", destRef: "", dest: "" }), 0, "a blank sign is no sign");
  const first = table.idFor({ ref: "40", exit: "", destRef: "", dest: "" });
  assert.equal(table.idFor({ ref: "40", exit: "", destRef: "", dest: "" }), first, "identical faces share an entry");
  assert.notEqual(table.idFor({ ref: "40", exit: "28", destRef: "", dest: "" }), first);

  // End to end: an edge names a sign face by index and a route's steps read
  // it back without a further fetch.
  const graph = syntheticGraph(8, 8, 7);
  graph.signs = [
    { ref: "40", exit: "", destRef: "", dest: "" },
    { ref: "", exit: "28", destRef: "40 Ouest", dest: "Montréal" }
  ];
  graph.edgeSign = Uint32Array.from(graph.edgeFrom, (_, i) => (i % 5 === 0 ? 2 : 1));
  // Every fifth edge is a roundabout arc, so the flag has to survive too.
  graph.edgeFlags = Uint8Array.from(graph.edgeFrom, (_, i) => (i % 5 === 0 ? 1 : 0));

  const dir = mkdtempSync(join(tmpdir(), "rangefind-signs-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 16, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);

  assert.equal(engine.root.signs.length, 2, "the root carries the sign table");
  assert.equal(engine.root.signs[1].destRef, "40 Ouest");

  const route = await engine.route({
    from: { lat: graph.nodeLat[0] / 1e7, lon: graph.nodeLon[0] / 1e7 },
    to: { lat: graph.nodeLat.at(-1) / 1e7, lon: graph.nodeLon.at(-1) / 1e7 }
  });
  assert.ok(route.steps.length, "the route has steps to sign");
  for (const step of route.steps) {
    // Flattened onto the step: every consumer wants the strings and none of
    // them wants to know a sign table exists.
    assert.equal(typeof step.ref, "string");
    assert.equal(typeof step.exitRef, "string");
    assert.equal(typeof step.destinationRef, "string");
    assert.equal(typeof step.destination, "string");
    assert.equal(typeof step.roundabout, "boolean");
  }
  assert.ok(
    route.steps.some(step => step.ref === "40" || step.exitRef === "28"),
    "and at least one of them says what its sign says"
  );
});

test("a roundabout is one maneuver with an exit to count, not three nameless arcs", async (t) => {
  // A circle arrives from the extract as two or three unnamed forty-metre
  // ways whose turn angles describe the curve rather than where the driver
  // ends up — so a roundabout followed by a left turn was drawn, and spoken,
  // as "bear right". The exit number is the instruction, and it is the one
  // thing geometry can never supply.
  const width = 6;
  const graph = syntheticGraph(width, 6, 11);
  // Turn a run of consecutive edges into a roundabout, all sharing a name
  // with neither neighbour so the collapse cannot be an artefact of naming.
  graph.edgeFlags = Uint8Array.from(graph.edgeFrom, () => 0);
  const circle = [];
  for (let i = 0; i < graph.edgeFrom.length; i++) {
    if (graph.edgeFrom[i] + 1 === graph.edgeTo[i] && graph.edgeFrom[i] < width - 1) {
      graph.edgeFlags[i] = 1;
      circle.push(i);
    }
  }
  assert.ok(circle.length >= 2, "the fixture needs a run of arcs to collapse");
  // Distinct names per arc: if the collapse were really name-based rather
  // than flag-based, this would produce one line per arc.
  graph.names = [...graph.names, "Arc A", "Arc B", "Arc C"];
  circle.forEach((edge, i) => {
    graph.edgeName[edge] = graph.names.length - 3 + (i % 3);
  });
  // Make the ring the cheap way across, so the route certainly uses it.
  for (const edge of circle) graph.edgeWeightDs[edge] = 1;

  const dir = mkdtempSync(join(tmpdir(), "rangefind-circle-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 12, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);

  const route = await engine.route({
    from: { lat: graph.nodeLat[0] / 1e7, lon: graph.nodeLon[0] / 1e7 },
    to: { lat: graph.nodeLat[width - 1] / 1e7, lon: graph.nodeLon[width - 1] / 1e7 }
  });
  const arcs = route.steps.filter(step => step.roundabout);
  assert.equal(arcs.length, 1, "however many arcs the circle is built from, it is one step");
  assert.ok(arcs[0].roundaboutExit >= 1, "and it says which exit to take");
  // The arc never merges with the road either side of it: entering a
  // roundabout and driving down a street are not the same instruction.
  const index = route.steps.indexOf(arcs[0]);
  for (const neighbour of [route.steps[index - 1], route.steps[index + 1]]) {
    if (neighbour) assert.equal(neighbour.roundabout, false);
  }
});

test("a fix's own error bar decides how wide the snap looks, and distance is charged", async (t) => {
  // The seed cost of a snap match was the cost of the rest of its own edge
  // and nothing else, so which road a reroute started from was decided by how
  // fast the road was rather than how close: twenty metres of autoroute costs
  // less than twenty metres of boulevard. A driver beside the A-13 was handed
  // a route down the desserte and declared off-route eleven times in four
  // minutes.
  const graph = syntheticGraph(10, 10, 23);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-snap-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 20, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);

  const from = { lat: graph.nodeLat[0] / 1e7, lon: graph.nodeLon[0] / 1e7 };
  const to = { lat: graph.nodeLat.at(-1) / 1e7, lon: graph.nodeLon.at(-1) / 1e7 };

  // A wider band is more candidates, never fewer, and never a worse route:
  // the search still picks the cheapest, it simply gets to see the road the
  // car is actually on.
  const tight = await engine.route({ from, to });
  const wide = await engine.route({ from, to, accuracyMeters: 45 });
  assert.ok(Number.isFinite(wide.seconds));
  assert.ok(
    wide.seconds >= tight.seconds - 1e-6,
    "widening the band cannot make the answer cheaper than the unpenalised one"
  );

  // Every candidate knows how much further off it is than the nearest, which
  // is what the seed charge is computed from. Charged on the excess, not the
  // absolute distance: a trip that genuinely starts eighty metres from any
  // road must not have all its candidates taxed for the same eighty metres.
  const snapped = await engine.snap(from);
  assert.ok(snapped.matches.length >= 1);
  assert.equal(snapped.matches[0].extraMeters, 0, "the nearest road is charged nothing");
  for (const match of snapped.matches) {
    assert.ok(match.extraMeters >= 0);
    assert.ok(match.extraMeters <= 25 + 1e-6, "and the default band is the ordinary GPS error");
  }
});

test("a ferry is a road the router can take, priced by its timetable", async () => {
  // A ferry has no `highway` tag — it is `route=ferry` — so every class
  // lookup keyed on `highway` treated the crossing as unroutable and dropped
  // it. On a coastline that is not a missing edge but a missing road: the
  // router sends a driver the long way round a river, or refuses the trip,
  // with nothing on screen to say a boat was the answer.
  const { PROFILES, wayClass, FERRY_CLASS, parseDuration, ferryWaitSeconds } =
    await import("../scripts/osm_road_graph.mjs");
  const tags = pairs => new Map(pairs);

  assert.equal(wayClass(tags([["route", "ferry"]])), FERRY_CLASS);
  assert.equal(wayClass(tags([["highway", "primary"]])), "primary");

  // OSM writes `duration` as a clock or as an ISO period, and two fields are
  // hours:minutes — a twelve-minute crossing is "00:12", not twelve seconds.
  assert.equal(parseDuration("00:12"), 12 * 60);
  assert.equal(parseDuration("PT10M"), 10 * 60);
  assert.equal(parseDuration("1:30:00"), 90 * 60);
  assert.equal(parseDuration(""), 0);
  assert.equal(parseDuration("not a duration"), 0);

  // What a ferry costs is mostly the wait for the next sailing, and pricing
  // it as a road is what makes a router send somebody to sit at a slip for
  // forty minutes to save four.
  assert.equal(ferryWaitSeconds(tags([["interval", "01:00"]])), 30 * 60);
  assert.equal(ferryWaitSeconds(tags([["interval", "04:00"]])), 30 * 60, "capped, not unbounded");
  assert.ok(ferryWaitSeconds(tags([])) > 0, "an untagged interval is not a zero wait");

  // A car ferry carries cars; a foot-and-bicycle one says so on itself.
  const carry = tags([["route", "ferry"], ["motor_vehicle", "yes"]]);
  const walkOn = tags([["route", "ferry"], ["motor_vehicle", "no"], ["foot", "yes"]]);
  assert.equal(PROFILES.car.allowed(carry), true);
  assert.equal(PROFILES.car.allowed(walkOn), false);
  assert.equal(PROFILES.foot.allowed(walkOn), true);
  // And a ferry is never one-way, whatever the default for its class.
  assert.equal(PROFILES.car.oneway(carry), 0);
});

test("a Y is a fork, and the road keeping its name is what hid it", async (t) => {
  // A stem running north into a junction, and two prongs leaving it about
  // twenty degrees either side. Every angle here is shallow, so the turn
  // reported onto either prong is "carrying straight on" — the same reading a
  // street renamed at a boundary produces. Two things differ, and neither is
  // in the line: a second road left the junction going much the same way, and
  // the road never changed its name, so the step would have folded and taken
  // the junction with it.
  const lat = [45.500, 45.505, 45.510, 45.515, 45.520, 45.515];
  const lon = [-73.600, -73.600, -73.600, -73.6027, -73.6055, -73.5973];
  const pairs = [[0, 1], [1, 2], [2, 3], [3, 4], [2, 5]];
  const from = [];
  const to = [];
  for (const [a, b] of pairs) { from.push(a, b); to.push(b, a); }
  const graph = {
    nodeLat: Int32Array.from(lat, v => Math.round(v * 1e7)),
    nodeLon: Int32Array.from(lon, v => Math.round(v * 1e7)),
    edgeFrom: Uint32Array.from(from),
    edgeTo: Uint32Array.from(to),
    edgeWeightDs: Uint32Array.from(from, () => 100),
    edgeDistDm: Uint32Array.from(from, () => 5500),
    // One name throughout: the fork must not be an artefact of the road being
    // called something new past the junction. This is the case that used to
    // vanish entirely.
    edgeName: Uint32Array.from(from, () => 1),
    edgeClass: Uint8Array.from(from, () => 0),
    geomOffsets: Uint32Array.from(from.map((_, i) => i).concat([from.length])),
    geomBytes: Uint8Array.from(from, () => 0),
    names: ["", "Rue A"],
    profile: "car",
    classes: ["arterial", "local"]
  };

  const dir = mkdtempSync(join(tmpdir(), "rangefind-fork-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 8, fanout: 4, topMaxCells: 4 });
  const engine = await openRouteGraphDir(dir);

  const route = await engine.route({
    from: { lat: lat[0], lon: lon[0] },
    to: { lat: lat[4], lon: lon[4] }
  });
  assert.ok(route.steps.length > 1, "the Y is a step boundary, not swallowed by the name");
  assert.ok(
    route.steps.some(step => step.forkArms >= 1),
    "the other prong is reported as carrying on beside the route"
  );
  // Nothing precedes the first step, so there is no junction to describe and
  // the count says so rather than claiming a plain road.
  assert.equal(route.steps[0].forkArms, 0, "unknown at the start of the route");
});

test("a square crossroads is not a fork, however many ways lead out of it", async (t) => {
  // Every arm of a grid leaves at a right angle to its neighbours, so nothing
  // here carries on beside the route. A client counting arms rather than
  // measuring them would announce a keep-left at every intersection in the
  // city — which is why the count alone was not enough to ship.
  const graph = syntheticGraph(6, 6, 17);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-crossroads-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 12, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);

  const route = await engine.route({
    from: { lat: graph.nodeLat[0] / 1e7, lon: graph.nodeLon[0] / 1e7 },
    to: { lat: graph.nodeLat.at(-1) / 1e7, lon: graph.nodeLon.at(-1) / 1e7 }
  });
  assert.ok(route.steps.length, "the grid routes");
  for (const step of route.steps) {
    assert.equal(typeof step.forkArms, "number");
    assert.equal(step.forkArms, 0, "a square crossroads offers no second way on");
  }
});

test("a barrier is a road that is not there, and who it stops depends on what it is", async () => {
  const { barrierBlocks } = await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));

  // The overwhelmingly common node: no barrier, nothing to decide.
  assert.equal(barrierBlocks("car", tags({ highway: "traffic_signals" })), false);
  assert.equal(barrierBlocks("car", null), false);

  // Bollards at the end of an alley. A perfectly good footpath, and a road a
  // van has never been able to enter — which is what routing one down it
  // amounts to when nothing reads the tag.
  assert.equal(barrierBlocks("car", tags({ barrier: "bollard" })), true);
  assert.equal(barrierBlocks("bike", tags({ barrier: "bollard" })), false);
  assert.equal(barrierBlocks("foot", tags({ barrier: "bollard" })), false);

  // A cycle barrier is built to slow a bicycle down, not to stop one.
  assert.equal(barrierBlocks("bike", tags({ barrier: "cycle_barrier" })), false);
  assert.equal(barrierBlocks("car", tags({ barrier: "cycle_barrier" })), true);

  // A stile is for people and stops the wheels.
  assert.equal(barrierBlocks("foot", tags({ barrier: "stile" })), false);
  assert.equal(barrierBlocks("bike", tags({ barrier: "stile" })), true);
  assert.equal(barrierBlocks("car", tags({ barrier: "stile" })), true);

  // A wall is a wall.
  for (const profile of ["car", "bike", "foot"]) {
    assert.equal(barrierBlocks(profile, tags({ barrier: "wall" })), true);
  }

  // A gate ordinarily stands open — most of them do, and refusing every one
  // would cut up farm tracks and car parks everywhere. Locked or refused by
  // its own access tag, it is a wall with hinges.
  assert.equal(barrierBlocks("car", tags({ barrier: "gate" })), false);
  assert.equal(barrierBlocks("car", tags({ barrier: "gate", locked: "yes" })), true);
  assert.equal(barrierBlocks("car", tags({ barrier: "gate", access: "private" })), true);

  // The node's own access tag outranks the table in both directions.
  assert.equal(barrierBlocks("car", tags({ barrier: "bollard", motor_vehicle: "yes" })), false);
  assert.equal(barrierBlocks("foot", tags({ barrier: "gate", foot: "no" })), true);
  // Reaching a place is not passing through it; the way-level rules already
  // price a car park, so the barrier itself does not refuse a customer.
  assert.equal(barrierBlocks("car", tags({ barrier: "gate", access: "customers" })), false);
});

test("a posted limit is read the way the sign is written, in either system", async () => {
  const { parseLengthCm, parseWeightKg, wayLimits } = await import("../scripts/osm_road_graph.mjs");

  // Metres, which is what most of the world posts and most mappers copy.
  assert.equal(parseLengthCm("3.5"), 350);
  assert.equal(parseLengthCm("3.5 m"), 350);
  assert.equal(parseLengthCm("3,5"), 350);
  // Feet and inches, which is what North America posts. An unparsed limit
  // reads as no limit, so failing here sends a van under every low bridge on
  // the continent — the exact failure this exists to prevent.
  assert.equal(parseLengthCm("12'6\""), 381);
  assert.equal(parseLengthCm("12'"), 366);
  assert.equal(parseLengthCm("12 ft"), 366);
  // Values that measure nothing must not be mistaken for a limit of zero,
  // which would refuse the road to everything including a bicycle.
  for (const nothing of ["none", "default", "unsigned", "", null, "below_default"]) {
    assert.equal(parseLengthCm(nothing), 0);
  }

  assert.equal(parseWeightKg("7.5"), 7500);
  assert.equal(parseWeightKg("7.5 t"), 7500);
  assert.equal(parseWeightKg("3500 kg"), 3500);
  assert.equal(parseWeightKg("none"), 0);

  const tags = (entries) => new Map(Object.entries(entries));
  assert.equal(wayLimits(tags({ highway: "residential" })), null);
  // The physical arch outranks the legal posting where both exist: one is
  // what you are allowed to do and the other is what will happen.
  assert.equal(wayLimits(tags({ maxheight: "4.0", "maxheight:physical": "3.2" })).heightCm, 320);
  assert.equal(wayLimits(tags({ maxweight: "7.5" })).weightKg, 7500);
});

test("a van is turned away by a bridge a car drives under", async (t) => {
  // A straight road north with a 3.2 m arch across the middle of it, and a
  // longer way round that has none. A car takes the arch. A van 3.4 m tall
  // cannot, and sending it under is not a slower route — it is a wedged
  // vehicle under a bridge and a road closed behind it.
  const lat = [45.500, 45.505, 45.510, 45.515, 45.520, 45.512, 45.518];
  const lon = [-73.600, -73.600, -73.600, -73.600, -73.600, -73.5920, -73.5920];
  const pairs = [[0, 1], [1, 2], [2, 3], [3, 4], [2, 5], [5, 6], [6, 4]];
  const from = [];
  const to = [];
  const weight = [];
  const limitId = [];
  for (const [a, b] of pairs) {
    from.push(a, b);
    to.push(b, a);
    const isArch = a === 2 && b === 3;
    // The detour is deliberately dearer, so a van found on it can only be
    // there because the arch refused it rather than because it was quicker.
    const detour = (a === 2 && b === 5) || (a === 5 && b === 6) || (a === 6 && b === 4);
    const w = detour ? 600 : 100;
    weight.push(w, w);
    limitId.push(isArch ? 1 : 0, isArch ? 1 : 0);
  }
  const graph = {
    nodeLat: Int32Array.from(lat, v => Math.round(v * 1e7)),
    nodeLon: Int32Array.from(lon, v => Math.round(v * 1e7)),
    edgeFrom: Uint32Array.from(from),
    edgeTo: Uint32Array.from(to),
    edgeWeightDs: Uint32Array.from(weight),
    edgeDistDm: Uint32Array.from(from, () => 5000),
    edgeName: Uint32Array.from(from, () => 1),
    edgeClass: Uint8Array.from(from, () => 0),
    edgeLimit: Uint32Array.from(limitId),
    geomOffsets: Uint32Array.from(from.map((_, i) => i).concat([from.length])),
    geomBytes: Uint8Array.from(from, () => 0),
    names: ["", "Rue A"],
    profile: "car",
    classes: ["arterial", "local"],
    limits: [{ heightCm: 320, weightKg: 0, widthCm: 0, lengthCm: 0 }]
  };

  const dir = mkdtempSync(join(tmpdir(), "rangefind-maxheight-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 8, fanout: 4, topMaxCells: 4 });
  const engine = await openRouteGraphDir(dir);

  const ends = { from: { lat: lat[0], lon: lon[0] }, to: { lat: lat[4], lon: lon[4] } };
  const car = await engine.route(ends);
  const van = await engine.route({ ...ends, vehicle: { heightM: 3.4 } });

  assert.ok(car.distanceMeters > 0, "the car routes");
  assert.ok(
    van.distanceMeters > car.distanceMeters,
    "the van is sent the long way round rather than under the arch"
  );
  // A vehicle that fits is not punished for having said so, and equality
  // passes: a sign reading 3.2 admits a vehicle declared 3.2. Inventing a
  // safety margin here would silently refuse roads that are posted to fit.
  const fits = await engine.route({ ...ends, vehicle: { heightM: 3.0 } });
  assert.equal(fits.distanceMeters, car.distanceMeters, "a vehicle that fits keeps the short way");
  const exact = await engine.route({ ...ends, vehicle: { heightM: 3.2 } });
  assert.equal(exact.distanceMeters, car.distanceMeters, "a posted 3.2 admits a vehicle of 3.2");
  // A limit the vehicle says nothing about restricts nothing: a weight-only
  // van must not be refused by a height it never claimed.
  const weightOnly = await engine.route({ ...ends, vehicle: { weightT: 3.5 } });
  assert.equal(weightOnly.distanceMeters, car.distanceMeters, "an unrelated limit does not refuse");
  const unstated = await engine.route({ ...ends, vehicle: {} });
  assert.equal(unstated.distanceMeters, car.distanceMeters, "an unstated vehicle is an ordinary car");
});

test("a posted limit survives every stage between the tag and the edge", async (t) => {
  // The bug this guards was silent in the worst way: the tags parsed, the
  // limit table was written with 82 entries, the index built, the query
  // filtered — and not one edge in the country pointed at a limit, because
  // the column was dropped three separate times on the way through. Nothing
  // failed. Every route was simply the car's route.
  //
  // So this asserts the join rather than the parts: a limit reaches an edge,
  // survives the shortcut that replaces it, and still refuses a vehicle on
  // the far side of the whole pipeline.
  const fixture = "examples/osm-geo/public/route-graph";
  if (!existsSync(join(fixture, "manifest.json"))) {
    t.skip(`route graph fixture missing at ${fixture}`);
    return;
  }
  const engine = await openRouteGraphDir(fixture);
  const root = engine.root ?? engine._root;
  assert.ok(root, "the fixture opens");
  assert.ok(
    (root.limits || []).length > 0,
    "the index carries the distinct limit sets its edges point at"
  );

  // Across a spread of the country, a vehicle too big for the posted limits
  // must be routed differently somewhere — and always the long way round
  // rather than through, since a refusal can only ever add distance.
  const points = [
    [49.6116, 6.1319], [49.5000, 6.1000], [49.7500, 6.1000],
    [49.6000, 5.9500], [49.8500, 6.1000], [49.6300, 6.1700]
  ];
  let routed = 0;
  let differed = 0;
  let faster = 0;
  for (let i = 0; i < points.length; i++) {
    for (let j = i + 1; j < points.length; j++) {
      const ends = {
        from: { lat: points[i][0], lon: points[i][1] },
        to: { lat: points[j][0], lon: points[j][1] }
      };
      let car;
      try { car = await engine.route(ends); } catch { continue; }
      routed++;
      const oversize = await engine.route({ ...ends, vehicle: { heightM: 6, weightT: 60 } });
      if (Math.round(oversize.seconds) !== Math.round(car.seconds)) {
        differed++;
        if (oversize.seconds < car.seconds) faster++;
      }
    }
  }
  assert.ok(routed > 0, "the fixture routes at all");
  assert.ok(differed > 0, "an oversize vehicle is turned off at least one posted road");
  assert.equal(faster, 0, "a refusal can only ever cost time, never save it");
});

test("an unmarked crossing is a routing connector, not a feature of the road", async () => {
  // Most `highway=crossing` nodes in OSM exist to join a sidewalk to the
  // roadway so pedestrian routing works at all. Nothing is painted there and
  // a driver would not know one had been passed — but the map drew a marker
  // for each, every few metres, all the way down the street.
  const { nodeKindCode, JUNCTION_KINDS } = await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));

  assert.equal(nodeKindCode(tags({ highway: "crossing", crossing: "unmarked" })), 0);
  assert.equal(nodeKindCode(tags({ highway: "crossing" })), 0, "no marking stated is not a marking");
  assert.equal(nodeKindCode(tags({ highway: "crossing", "crossing:markings": "no" })), 0);

  // Painted ones are real, and a driver reads them off the road surface.
  assert.equal(nodeKindCode(tags({ highway: "crossing", crossing: "marked" })), JUNCTION_KINDS.crossing);
  assert.equal(nodeKindCode(tags({ highway: "crossing", crossing: "zebra" })), JUNCTION_KINDS.crossing);
  // The North American spelling of "marked but not signalised".
  assert.equal(nodeKindCode(tags({ highway: "crossing", crossing: "uncontrolled" })), JUNCTION_KINDS.crossing);
  assert.equal(
    nodeKindCode(tags({ highway: "crossing", "crossing:markings": "zebra" })),
    JUNCTION_KINDS.crossing
  );
  // A signal-controlled crossing is a signal, whatever else it says.
  assert.equal(
    nodeKindCode(tags({ highway: "crossing", crossing: "traffic_signals" })),
    JUNCTION_KINDS.traffic_signals
  );
  // And the things that were never crossings are untouched.
  assert.equal(nodeKindCode(tags({ highway: "traffic_signals" })), JUNCTION_KINDS.traffic_signals);
  assert.equal(nodeKindCode(tags({ highway: "stop" })), JUNCTION_KINDS.stop);
});

test("a crossing beside the light that governs it is not drawn twice", async () => {
  const { mergesWithPreviousJunction } = await import("../src/route_graph_query.js");
  const at = (lat, lon, kind) => ({ lat, lon, kind });
  const SIGNAL = 1, STOP = 2, CROSSING = 5;

  // A signalised crossroads carries a signal node and a painted crossing on
  // each approach. The driver obeys one thing there, and was shown two.
  assert.equal(mergesWithPreviousJunction(at(45.5, -73.6, SIGNAL), CROSSING, 455000200, -736000000), true);
  assert.equal(mergesWithPreviousJunction(at(45.5, -73.6, STOP), CROSSING, 455000200, -736000000), true);
  // Only the crossing gives way. A stop line beside a light is two facts.
  assert.equal(mergesWithPreviousJunction(at(45.5, -73.6, SIGNAL), STOP, 455000200, -736000000), false);
  assert.equal(mergesWithPreviousJunction(at(45.5, -73.6, CROSSING), SIGNAL, 455000200, -736000000), false);
  // And a crossing far enough away is its own crossing.
  assert.equal(mergesWithPreviousJunction(at(45.5, -73.6, SIGNAL), CROSSING, 455008000, -736000000), false);
});

test("a mini-roundabout is a junction the driver can see, not a nameless crossroads", async () => {
  // A painted circle on an ordinary crossroads: no arcs to collapse, no exits
  // to count, and nothing in the geometry that says it is there. The turn
  // through it is the real instruction and stays one — this only makes sure
  // the thing the driver is turning at is on the map and costs what it costs.
  const { nodeKindCode, JUNCTION_KINDS, PROFILES } = await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));

  assert.equal(nodeKindCode(tags({ highway: "mini_roundabout" })), JUNCTION_KINDS.mini_roundabout);
  assert.equal(JUNCTION_KINDS.mini_roundabout, 6, "a new kind, appended so old readers keep their meanings");
  // Yielding costs more than a give-way triangle and far less than a signal.
  const car = PROFILES.car.nodePenalties;
  assert.ok(car.mini_roundabout > car.give_way);
  assert.ok(car.mini_roundabout < car.traffic_signals);
});

test("a turn banned at rush hour is shut then and open the rest of the day", async (t) => {
  // A straight run north with a left at the top, and a longer way round that
  // needs no left. The left is signed "no left turn, Mo-Fr 07:00-09:00" —
  // one sign with two meanings, and the router used to read it as none: the
  // whole relation was thrown away and the turn stood open at eight in the
  // morning along with every other hour.
  //
  // The bucket here carries no speed factors at all, so nothing but the ban
  // can move the route: if base and peak differ, it is the sign doing it.
  const lat = [45.500, 45.505, 45.510, 45.515, 45.520, 45.512, 45.518];
  const lon = [-73.600, -73.600, -73.600, -73.594, -73.588, -73.610, -73.606];
  const pairs = [[0, 1], [1, 2], [2, 3], [3, 4], [2, 5], [5, 6], [6, 4]];
  const from = [];
  const to = [];
  const weight = [];
  const ban = [];
  for (const [a, b] of pairs) {
    from.push(a, b);
    to.push(b, a);
    const isBannedTurn = a === 2 && b === 3;
    const detour = (a === 2 && b === 5) || (a === 5 && b === 6) || (a === 6 && b === 4);
    const w = detour ? 400 : 100;
    weight.push(w, w);
    // Only the forward direction is banned; the sign faces one way.
    ban.push(isBannedTurn ? 1 : 0, 0);
  }
  const graph = {
    nodeLat: Int32Array.from(lat, v => Math.round(v * 1e7)),
    nodeLon: Int32Array.from(lon, v => Math.round(v * 1e7)),
    edgeFrom: Uint32Array.from(from),
    edgeTo: Uint32Array.from(to),
    edgeWeightDs: Uint32Array.from(weight),
    edgeDistDm: Uint32Array.from(from, () => 5000),
    edgeName: Uint32Array.from(from, () => 1),
    edgeClass: Uint8Array.from(from, () => 0),
    edgeBan: Uint32Array.from(ban),
    geomOffsets: Uint32Array.from(from.map((_, i) => i).concat([from.length])),
    geomBytes: Uint8Array.from(from, () => 0),
    names: ["", "Rue A"],
    profile: "car",
    classes: ["arterial", "local"],
    banRules: [{ days: 0b0111110, startMinute: 420, endMinute: 540, monthStart: 1, monthEnd: 12 }]
  };

  const dir = mkdtempSync(join(tmpdir(), "rangefind-timed-turn-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, {
    leafNodes: 8,
    fanout: 4,
    topMaxCells: 4,
    // Exactly the window on the sign, and no speed factors: the only thing
    // this bucket changes is whether that turn exists.
    timeBuckets: [{ name: "peak", rules: [{ dayMask: 0b0111110, startHour: 7, endHour: 9 }], classFactors: {} }]
  });
  const engine = await openRouteGraphDir(dir);

  const ends = { from: { lat: lat[0], lon: lon[0] }, to: { lat: lat[4], lon: lon[4] } };
  const offPeak = await engine.route({ ...ends, bucket: "base" });
  const rushHour = await engine.route({ ...ends, bucket: "peak" });

  assert.ok(offPeak.distanceMeters > 0, "the road routes at all");
  assert.ok(
    rushHour.distanceMeters > offPeak.distanceMeters,
    "at rush hour the banned left is refused and the route goes the long way"
  );
  // And the ban is not a tax: the rest of the day the turn is simply there.
  const midnight = await engine.route({ ...ends, departureTime: "2026-08-10T02:30:00" });
  assert.equal(
    Math.round(midnight.distanceMeters),
    Math.round(offPeak.distanceMeters),
    "outside the window the sign says nothing and the short way is taken"
  );
  // A departure inside the window picks the same answer as the bucket does,
  // which is what makes the clock and the metric agree.
  const eightAm = await engine.route({ ...ends, departureTime: "2026-08-10T08:00:00" });
  assert.equal(
    Math.round(eightAm.distanceMeters),
    Math.round(rushHour.distanceMeters),
    "a departure at eight is routed the way rush hour is"
  );
});

test("a toll and a ferry are refusable, and the refusal survives a shortcut", async (t) => {
  // A short way across a tolled bridge and a long way round. A toll is not
  // slow — it is a decision, and one driver pays it daily while another will
  // drive twenty minutes to save two dollars. So it is refused rather than
  // priced into the time, and the refusal has to hold on the shortcut that
  // replaces the bridge as well as on the bridge itself.
  const { EDGE_FLAG_TOLL, EDGE_FLAG_FERRY } = await import("../scripts/osm_road_graph.mjs");
  const lat = [45.500, 45.505, 45.510, 45.515, 45.520, 45.512, 45.518];
  const lon = [-73.600, -73.600, -73.600, -73.600, -73.600, -73.5920, -73.5920];
  const pairs = [[0, 1], [1, 2], [2, 3], [3, 4], [2, 5], [5, 6], [6, 4]];

  const build = (flagOn) => {
    const from = [];
    const to = [];
    const weight = [];
    const flags = [];
    for (const [a, b] of pairs) {
      from.push(a, b);
      to.push(b, a);
      const crossing = a === 2 && b === 3;
      const detour = (a === 2 && b === 5) || (a === 5 && b === 6) || (a === 6 && b === 4);
      const w = detour ? 600 : 100;
      weight.push(w, w);
      flags.push(crossing ? flagOn : 0, crossing ? flagOn : 0);
    }
    return {
      nodeLat: Int32Array.from(lat, v => Math.round(v * 1e7)),
      nodeLon: Int32Array.from(lon, v => Math.round(v * 1e7)),
      edgeFrom: Uint32Array.from(from),
      edgeTo: Uint32Array.from(to),
      edgeWeightDs: Uint32Array.from(weight),
      edgeDistDm: Uint32Array.from(from, () => 5000),
      edgeName: Uint32Array.from(from, () => 1),
      edgeClass: Uint8Array.from(from, () => 0),
      edgeFlags: Uint8Array.from(flags),
      edgeCharge: Uint32Array.from(from, (_, i) => (flags[i] ? 1 : 0)),
      geomOffsets: Uint32Array.from(from.map((_, i) => i).concat([from.length])),
      geomBytes: Uint8Array.from(from, () => 0),
      names: ["", "Rue A"],
      profile: "car",
      classes: ["arterial", "local"],
      charges: [{ cents: 250, currency: "CAD" }]
    };
  };

  for (const [what, flag, avoid] of [
    ["toll", EDGE_FLAG_TOLL, { tolls: true }],
    ["ferry", EDGE_FLAG_FERRY, { ferries: true }]
  ]) {
    const dir = mkdtempSync(join(tmpdir(), `rangefind-${what}-`));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    buildRouteGraph(build(flag), dir, { leafNodes: 8, fanout: 4, topMaxCells: 4 });
    const engine = await openRouteGraphDir(dir);
    const ends = { from: { lat: lat[0], lon: lon[0] }, to: { lat: lat[4], lon: lon[4] } };

    const straight = await engine.route(ends);
    const avoided = await engine.route({ ...ends, avoid });
    assert.ok(
      avoided.distanceMeters > straight.distanceMeters,
      `avoiding the ${what} takes the long way round`
    );
    // Asking to avoid the other thing must not move the route: a preference
    // that refuses more than it was asked to is worse than none.
    const other = what === "toll" ? { ferries: true } : { tolls: true };
    const unrelated = await engine.route({ ...ends, avoid: other });
    assert.equal(
      Math.round(unrelated.distanceMeters),
      Math.round(straight.distanceMeters),
      "an unrelated preference leaves the route alone"
    );
    // And the route says what it used, and what that cost.
    assert.equal(straight.charges?.[what], true, `the route reports the ${what}`);
    assert.equal(straight.charges?.cents, 250);
    assert.equal(straight.charges?.currency, "CAD");
    assert.equal(avoided.charges, undefined, "the avoided route costs nothing to report");
  }
});

test("the panel over each lane says where that lane goes", async () => {
  const { laneDestinations } = await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));

  // The arrows say which movements a lane allows. Only this says where any of
  // them goes, and on a five-lane approach that is the entire decision.
  assert.equal(
    laneDestinations(tags({ "destination:lanes": "Montréal|Montréal|Laval" }), 1).forward,
    "Montréal|Montréal|Laval"
  );
  // The number outranks the name, the way the panel is painted: "40 Ouest" is
  // above the lane and "Autoroute Félix-Leclerc" is nowhere a driver can see.
  assert.equal(
    laneDestinations(tags({
      "destination:lanes": "Autoroute Félix-Leclerc|Local",
      "destination:ref:lanes": "40 Ouest|"
    }), 1).forward,
    "40 Ouest|Local"
  );
  // A lane listing three places is read for the first; a chip has room for one.
  assert.equal(
    laneDestinations(tags({ "destination:lanes": "Montréal;Québec;Trois-Rivières|Local" }), 1).forward,
    "Montréal|Local"
  );
  // Nothing tagged is nothing to draw, and a two-way street does not hand the
  // forward panel to traffic coming the other way.
  assert.equal(laneDestinations(tags({ highway: "primary" }), 1).forward, "");
  assert.equal(laneDestinations(tags({ "destination:lanes:forward": "Laval|Laval" }), 1).backward, "");
});

test("a car-carrying train is a road, and one a driver may refuse", async () => {
  const { wayClass, SHUTTLE_CLASS, isBoarded, FERRY_CLASS } = await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));

  // The Chunnel and the Alpine tunnels: you drive on, you wait, you drive off.
  // Unroutable without this, which cuts a country in half wherever one is the
  // only way through a mountain.
  assert.equal(wayClass(tags({ route: "shuttle_train" })), SHUTTLE_CLASS);
  assert.equal(wayClass(tags({ route: "shuttle" })), SHUTTLE_CLASS);
  assert.equal(isBoarded(SHUTTLE_CLASS), true);
  assert.equal(isBoarded(FERRY_CLASS), true);
  assert.equal(isBoarded("motorway"), false);
});

test("a ford, a winter road and a bus lane are not a courier's road", async () => {
  const { drivableSurface, reservedFor, EDGE_FLAG_PSV_ONLY, EDGE_FLAG_HOV_ONLY } =
    await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));

  // A stream running over the road: passable in a pickup in July and not in a
  // van in April, and the map cannot tell you which day it is.
  assert.equal(drivableSurface(tags({ ford: "yes" })), false);
  assert.equal(drivableSurface(tags({ ford: "no" })), true);
  // The same claim about the calendar.
  assert.equal(drivableSurface(tags({ seasonal: "winter" })), false);
  assert.equal(drivableSurface(tags({ winter_road: "yes" })), false);
  // A perfectly good road that this driver is not allowed on. Being sent
  // down one is a fine rather than a detour — but it is somebody's road, so
  // it is flagged for them rather than deleted for everyone. Dropping it at
  // extract time made the decision unrecoverable: no option at query time
  // can reach a way that was never written, which is why a bus could not be
  // routed down its own route.
  assert.equal(drivableSurface(tags({ access: "psv" })), true);
  assert.equal(reservedFor(tags({ access: "psv" })), EDGE_FLAG_PSV_ONLY);
  assert.equal(reservedFor(tags({ access: "bus" })), EDGE_FLAG_PSV_ONLY);
  assert.equal(reservedFor(tags({ motor_vehicle: "psv" })), EDGE_FLAG_PSV_ONLY);
  assert.equal(drivableSurface(tags({ hov: "designated" })), true);
  assert.equal(reservedFor(tags({ hov: "designated" })), EDGE_FLAG_HOV_ONLY);
  assert.equal(reservedFor(tags({ access: "hov" })), EDGE_FLAG_HOV_ONLY);
  // Where cars are spelled out, that is the more specific claim and the
  // road belongs to everyone again.
  assert.equal(reservedFor(tags({ hov: "designated", motorcar: "yes" })), 0);
  assert.equal(reservedFor(tags({ highway: "residential" })), 0);
  // And an ordinary street is left alone.
  assert.equal(drivableSurface(tags({ highway: "residential" })), true);
});

test("a guide sign at a junction is read for the turn it points at", async () => {
  const { parseDestinationSign } = await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));
  const members = (extra = []) => [
    { role: "from", type: "way", ref: 100 },
    { role: "to", type: "way", ref: 200 },
    ...extra
  ];

  // The panel itself: which approach faces it, which turning it points at,
  // and what is written on it. Without this an ordinary junction announces a
  // turn by a street name the driver will not find on any sign.
  const sign = parseDestinationSign(
    tags({ destination: "Montréal", "destination:ref": "40 Ouest" }),
    members([{ role: "intersection", type: "node", ref: 7 }])
  );
  assert.deepEqual(sign, { fromWay: 100, toWay: 200, viaNode: 7, ref: "40 Ouest", text: "Montréal" });

  // Mappers who know the restriction shape write `via`, and mean the same node.
  assert.equal(
    parseDestinationSign(tags({ destination: "Laval" }), members([{ role: "via", type: "node", ref: 9 }]))?.viaNode,
    9
  );
  // A relation with nothing written on it describes no sign.
  assert.equal(parseDestinationSign(tags({}), members([{ role: "intersection", type: "node", ref: 7 }])), null);
  // And one with no junction cannot be attached to a turn.
  assert.equal(parseDestinationSign(tags({ destination: "Laval" }), members()), null);
});

test("connectivity says which lanes reach a turning, where arrows only imply it", async () => {
  const { parseConnectivity } = await import("../scripts/osm_road_graph.mjs");
  const tags = (spec) => new Map(Object.entries({ connectivity: spec }));
  const members = [
    { role: "from", type: "way", ref: 100 },
    { role: "to", type: "way", ref: 200 },
    { role: "via", type: "node", ref: 7 }
  ];

  // "lane 1 to lane 1, lane 2 to lanes 2 and 3, lane 3 to nothing" — the
  // third lane is exactly the one a driver must not be in, and no arrow
  // painted on the road says so.
  assert.equal(parseConnectivity(tags("1:1|2:2,3|3:"), members).mask, 0b011);
  assert.equal(parseConnectivity(tags("1:1|2:2|3:3"), members).mask, 0b111);
  assert.equal(parseConnectivity(tags("1:none|2:2"), members).mask, 0b010);
  // Nothing usable is not a claim worth carrying.
  assert.equal(parseConnectivity(tags("1:none|2:none"), members), null);
  assert.equal(parseConnectivity(new Map(), members), null);
});

test("a road number carried by a route relation reaches the sign", async () => {
  const { mergeRefs, waySigns } = await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));

  // The way's own number leads and the relation's follows, which is the
  // order the gantry stacks them.
  assert.equal(mergeRefs("A 4", ["E 50"]), "A 4;E 50");
  // A number the way already carries is not said twice because a relation
  // repeats it.
  assert.equal(mergeRefs("A 4", ["E 50", "A 4"]), "A 4;E 50");
  // The case this exists for: the way carries no number at all, and every
  // number the driver can see is on the relation.
  assert.equal(mergeRefs("", ["E 25"]), "E 25");
  assert.equal(mergeRefs(" ", []), "");
  // Semicolon lists on either side are one list, deduplicated across both.
  assert.equal(mergeRefs("N 5;N 5", ["E 411"]), "N 5;E 411");

  assert.equal(waySigns(tags({ ref: "A 4" }), ["E 50"]).forward.ref, "A 4;E 50");
  // Absent relations change nothing, which is every way in North America.
  assert.equal(waySigns(tags({ ref: "A 40" })).forward.ref, "A 40");
});

test("E-road numbers reach the index from the relations that carry them", async (t) => {
  // Luxembourg is the case North America cannot exercise: its motorways are
  // signed `A 4` on the way and `E 44` only on a `type=route` relation, so a
  // router reading ways alone announces half of what is on the gantry — and
  // in Europe the half it drops is often the one being followed.
  const fixture = "examples/osm-geo/public/route-graph";
  if (!existsSync(join(fixture, "manifest.json"))) {
    t.skip(`route graph fixture missing at ${fixture}`);
    return;
  }
  const engine = await openRouteGraphDir(fixture);
  const root = engine.root ?? engine._root;
  const refs = (root.signs || []).map(sign => sign?.ref || "").filter(Boolean);
  assert.ok(refs.length > 0, "the index carries road numbers at all");
  // An E-number can only have come from a relation: no Luxembourg way is
  // tagged with one.
  const eRoads = refs.filter(ref => /(^|;)\s*E\s?\d+/.test(ref));
  assert.ok(eRoads.length > 0, `no E-road number survived to the index (${refs.length} refs)`);
  // And it arrived alongside the national number rather than replacing it.
  assert.ok(
    eRoads.some(ref => ref.includes(";")),
    `E-numbers replaced the national number instead of joining it: ${eRoads.slice(0, 5).join(" | ")}`
  );
});

test("a lorry is refused a street signed against it", async (t) => {
  // `hgv=no` is the most-mapped restriction this router could not read, and
  // it is aimed at exactly the vehicle a delivery app carries.
  const fixture = "examples/osm-geo/public/route-graph";
  if (!existsSync(join(fixture, "manifest.json"))) {
    t.skip(`route graph fixture missing at ${fixture}`);
    return;
  }
  const engine = await openRouteGraphDir(fixture);
  const root = engine.root ?? engine._root;
  assert.ok(root, "the fixture opens");

  // Ordinary points across the country rather than points planted on a ban:
  // what matters is how the country routes, not how a contrived pair does.
  let seed = 12345;
  const rnd = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 0x100000000);
  const bounds = { minLat: 49.45, maxLat: 50.15, minLon: 5.75, maxLon: 6.50 };
  const point = () => ({
    lat: bounds.minLat + rnd() * (bounds.maxLat - bounds.minLat),
    lon: bounds.minLon + rnd() * (bounds.maxLon - bounds.minLon)
  });
  const go = async (params) => {
    try {
      return await engine.route(params);
    } catch (err) {
      return err.code;
    }
  };

  let compared = 0;
  let diverted = 0;
  for (let attempt = 0; attempt < 220 && compared < 60; attempt++) {
    const from = point();
    const to = point();
    const car = await go({ from, to });
    if (typeof car === "string") continue;
    const lorry = await go({ from, to, vehicle: { hgv: true } });
    if (typeof lorry === "string") continue;

    // A 2.4 t van is not a goods vehicle and must not be made to pay for
    // having declared its weight.
    const van = await go({ from, to, vehicle: { weightT: 2.4 } });
    assert.ok(typeof van !== "string", "a 2.4 t van failed to route at all");
    assert.equal(van.seconds, car.seconds, "a 2.4 t van was treated as a goods vehicle");

    // Weight alone makes a vehicle one, because a driver declaring 7.5 t
    // will not also think to say "hgv" — and the sign applies to them anyway.
    const heavy = await go({ from, to, vehicle: { weightT: 7.5 } });
    assert.ok(typeof heavy !== "string", "a 7.5 t vehicle failed to route at all");
    // At least as restricted, not identically so: 7.5 t also meets every
    // posted weight limit on the way, which a bare `hgv` declaration does
    // not. What is being asserted is the implication — that the weight alone
    // was enough to make it a goods vehicle — and a vehicle that were not
    // one would come back with the car's route, well under the lorry's.
    assert.ok(
      heavy.seconds >= lorry.seconds - 1,
      "a 7.5 t vehicle was let down roads a goods vehicle may not use"
    );

    // A refusal is never a shortcut — but only where the two are answering
    // the same question. A lorry refused the road at the door is snapped to
    // the nearest one it may use and finishes the last few metres on foot,
    // and that route is legitimately shorter than the car's because it stops
    // short of the same doorstep. Compare only the pairs that ran door to
    // identical door.
    const sameEnds =
      car.from.snapDistanceMeters === lorry.from.snapDistanceMeters &&
      car.to.snapDistanceMeters === lorry.to.snapDistanceMeters;
    if (!sameEnds) continue;
    compared++;
    assert.ok(
      lorry.seconds >= car.seconds - 1,
      "a refused road produced a quicker route, which means the refusal was a bug"
    );
    if (lorry.seconds > car.seconds + 1) diverted++;
  }

  assert.ok(compared > 20, `too few comparable pairs to conclude anything (${compared})`);
  assert.ok(
    diverted > 0,
    "no lorry anywhere in the country was diverted by an hgv sign, which is what a dropped column looks like"
  );
});

test("a doorstep a lorry may not use is a corner to stop at, not a refusal", async (t) => {
  // The regression this guards is one the feature itself introduced: reading
  // `hgv=no` at all means a depot on such a street snaps the search onto an
  // edge the lorry cannot leave, and the honest-looking answer — "no route
  // this vehicle fits through" — is the one answer a courier cannot act on.
  const fixture = "examples/osm-geo/public/route-graph";
  if (!existsSync(join(fixture, "manifest.json"))) {
    t.skip(`route graph fixture missing at ${fixture}`);
    return;
  }
  const engine = await openRouteGraphDir(fixture);
  let seed = 99; 
  const rnd = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 0x100000000);
  const point = () => ({ lat: 49.45 + rnd() * 0.70, lon: 5.75 + rnd() * 0.75 });
  const go = async (params) => {
    try {
      return await engine.route(params);
    } catch (err) {
      return err.code;
    }
  };

  let routed = 0;
  let refused = 0;
  for (let attempt = 0; attempt < 200 && routed + refused < 80; attempt++) {
    const from = point();
    const to = point();
    const car = await go({ from, to });
    if (typeof car === "string") continue;
    const lorry = await go({ from, to, vehicle: { hgv: true } });
    if (lorry === "RANGEFIND_ROUTE_VEHICLE_NO_PATH") refused++;
    else if (typeof lorry !== "string") routed++;
  }

  assert.ok(routed > 0, "no lorry route succeeded anywhere");
  // Some neighbourhoods genuinely have no road a lorry may use, and saying so
  // is right. Most do, and a country where a lorry is refused more often than
  // not is a snap that gave up rather than a country that is closed.
  assert.ok(
    refused * 4 < routed,
    `a lorry was refused ${refused} of ${routed + refused} ordinary journeys, which is a snapping failure rather than a road network`
  );
});

test("a lane reserved for buses is read as theirs, and a lane buses may use is not", async () => {
  const { wayLanes, wayLaneAccess, LANE_BUS, LANE_HOV, LANE_HOV3, LANE_NO_MOTOR, LANE_BICYCLE } =
    await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));
  const access = (entries) => {
    const map = tags(entries);
    return wayLaneAccess(map, wayLanes(map, entries.oneway === "yes" ? 1 : 0));
  };

  // The Tunnel Albert Bousser, which is why this distinction exists. Buses
  // are *permitted* in lane 1 and the lane is *for* them in lane 2, and the
  // first cut read both as reserved — leaving a car with no lane at all on a
  // road it was being routed down.
  assert.deepEqual(
    access({
      highway: "trunk",
      lanes: "3",
      "lanes:backward": "2",
      "lanes:forward": "1",
      "access:lanes:backward": "yes|no",
      "bus:lanes:backward": "yes|designated"
    }).backward,
    [0, LANE_BUS | LANE_NO_MOTOR]
  );

  // A lane every bus may use is nobody's lane in particular, and claiming it
  // is reserved would grey out most of the road network.
  assert.equal(access({ highway: "primary", lanes: "2", "bus:lanes": "yes|yes" }).forward, null);
  // And a road with no lane access tags at all says nothing, which is almost
  // all of them.
  assert.equal(access({ highway: "primary", lanes: "2" }).forward, null);

  // The reason is kept, not just the fact: "bus lane" and "HOV lane" are
  // different instructions to a driver.
  // One-way, so `lanes=2` really is two lanes forward: on a two-way street
  // the same tag means one lane each way, and the reasons follow the lanes.
  assert.deepEqual(
    access({ highway: "primary", oneway: "yes", lanes: "2", "hov:lanes": "designated|no" }).forward,
    [LANE_HOV, 0]
  );
  // Not every car-pool lane asks the same carful: Québec signs 22 ways at
  // three. A lane that wants three is a different claim from one that wants
  // two, so it is a different value rather than the same one with a note.
  assert.deepEqual(
    access({
      highway: "primary",
      oneway: "yes",
      lanes: "2",
      "hov:lanes": "designated|no",
      "hov:minimum": "3"
    }).forward,
    [LANE_HOV3, 0]
  );
  // An unreadable or absent minimum is two, which is what the sign reads
  // where nobody has written a number down.
  assert.deepEqual(
    access({
      highway: "primary",
      oneway: "yes",
      lanes: "2",
      "hov:lanes": "designated|no",
      "hov:minimum": "boom"
    }).forward,
    [LANE_HOV, 0]
  );
  assert.deepEqual(
    access({ highway: "primary", oneway: "yes", lanes: "2", "bicycle:lanes": "designated|no" }).forward,
    [LANE_BICYCLE, 0]
  );

  // Alignment is the point: the reasons index the same lanes the arrows do,
  // so a short access tag pads rather than shifting everything left.
  const wide = tags({
    highway: "primary",
    "turn:lanes": "left|through|through;right",
    "bus:lanes": "no|no|designated"
  });
  const lanes = wayLanes(wide, 0);
  const reasons = wayLaneAccess(wide, lanes).forward;
  assert.equal(reasons.length, lanes.forward.length, "a reason per lane, or the labels slide");
  assert.deepEqual(reasons, [0, 0, LANE_BUS]);

  // An unsuffixed tag is not applied backwards. Lane order reverses with
  // direction, so borrowing it would put the bus lane on the wrong side —
  // a missing label is recoverable, a mirrored one is not.
  assert.equal(access({ highway: "primary", lanes: "2", "bus:lanes": "designated|no" }).backward, null);
});

test("a reserved lane survives every stage between the tag and the step", async (t) => {
  // The join, not the parts. This pipeline has silently dropped a column
  // three times, and each time the tags parsed, the table filled, the index
  // built and every route came back looking perfectly reasonable.
  const fixture = "examples/osm-geo/public/route-graph";
  if (!existsSync(join(fixture, "manifest.json"))) {
    t.skip(`route graph fixture missing at ${fixture}`);
    return;
  }
  const { LANE_BUS, LANE_NO_MOTOR } = await import("../scripts/osm_road_graph.mjs");
  const engine = await openRouteGraphDir(fixture);

  // Journeys that actually cross Luxembourg City's reserved lanes, read off
  // the built index rather than guessed: the first two run the bus lane on
  // the northbound approach, the last a marked cycle lane on a three-lane
  // one.
  const seen = [];
  const spread = [
    [{ lat: 49.58353, lon: 6.13060 }, { lat: 49.59987, lon: 6.13612 }],
    [{ lat: 49.62717, lon: 6.09669 }, { lat: 49.62652, lon: 6.10736 }],
    [{ lat: 49.62761, lon: 6.10258 }, { lat: 49.62531, lon: 6.10489 }],
    [{ lat: 49.60705, lon: 6.10867 }, { lat: 49.60596, lon: 6.10495 }]
  ];
  for (const [from, to] of spread) {
    let route;
    try {
      route = await engine.route({ from, to });
    } catch {
      continue;
    }
    for (const step of route.steps || []) {
      if (!Array.isArray(step.laneAccess) || !step.laneAccess.some(Boolean)) continue;
      seen.push(step);
      // A reason per lane and no more: a list that has drifted out of step
      // with the arrows labels the wrong lane, which is worse than silence.
      assert.equal(
        step.laneAccess.length,
        step.lanes.length,
        "the reasons and the arrows disagree about how many lanes there are"
      );
    }
  }

  assert.ok(
    seen.length > 0,
    "no step anywhere carried a reserved lane, which is what a dropped column looks like"
  );
  // And the reason survived intact rather than arriving as a bare 1.
  assert.ok(
    seen.some(step => step.laneAccess.some(reason => (reason & LANE_BUS) !== 0)),
    "a reserved lane reached the step with no reason attached"
  );
  assert.ok(
    seen.some(step => step.laneAccess.some(reason => (reason & LANE_NO_MOTOR) !== 0)),
    "the closed-to-motor-traffic bit did not survive"
  );
});

test("a bus is routed down the road its job is on", async (t) => {
  // Reserved roads used to be deleted at extract time, so no option at query
  // time could reach them: a bus or taxi driver could not be routed down the
  // very roads their work is on. They are flagged now instead, which means
  // the ordinary car has to still be refused them — silence must keep
  // meaning "not for me", or every car inherits the bus lanes.
  const fixture = "examples/osm-geo/public/route-graph";
  if (!existsSync(join(fixture, "manifest.json"))) {
    t.skip(`route graph fixture missing at ${fixture}`);
    return;
  }
  const engine = await openRouteGraphDir(fixture);
  const root = engine.root ?? engine._root;
  assert.ok(root, "the fixture opens");

  const go = async (params) => {
    try {
      return await engine.route(params);
    } catch (err) {
      return err.code;
    }
  };
  // A stretch of Boulevard Franklin D. Roosevelt reserved for public service
  // vehicles, entered from each end.
  const from = { lat: 49.6073113, lon: 6.1331524 };
  const to = { lat: 49.6066773, lon: 6.1336198 };
  const car = await go({ from, to });
  const bus = await go({ from, to, vehicle: { psv: true } });
  assert.ok(typeof car !== "string", `an ordinary car could not route at all: ${car}`);
  assert.ok(typeof bus !== "string", `a bus could not route at all: ${bus}`);
  assert.ok(
    bus.seconds < car.seconds,
    "a bus was given the same route as a car past a road reserved for buses"
  );
  // And the car really is going round rather than merely being charged for it.
  assert.ok(
    car.distanceMeters > bus.distanceMeters,
    "the car was sent down a road reserved for public service vehicles"
  );
});

test("a coach's weight is not read as freight", async (t) => {
  // The defect the vehicle class fixes. `weight >= 3.5 t` implies a goods
  // vehicle, which is right for the van it was written for and wrong for a
  // coach: a driver entering their real weight was refused every street
  // signed against lorries, including the ones that name buses as welcome,
  // and the routes just came back longer with nothing said.
  const fixture = "examples/osm-geo/public/route-graph";
  if (!existsSync(join(fixture, "manifest.json"))) {
    t.skip(`route graph fixture missing at ${fixture}`);
    return;
  }
  const engine = await openRouteGraphDir(fixture);
  let seed = 4242;
  const rnd = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 0x100000000);
  const point = () => ({ lat: 49.45 + rnd() * 0.70, lon: 5.75 + rnd() * 0.75 });
  const go = async (params) => {
    try {
      return await engine.route(params);
    } catch (err) {
      return err.code;
    }
  };

  let compared = 0;
  let lorryDiverted = 0;
  let coachDiverted = 0;
  for (let attempt = 0; attempt < 260 && compared < 60; attempt++) {
    const from = point();
    const to = point();
    const car = await go({ from, to });
    if (typeof car === "string") continue;
    // Same fourteen tonnes both times; only the licence differs.
    const lorry = await go({ from, to, vehicle: { weightT: 14 } });
    const coach = await go({ from, to, vehicle: { weightT: 14, psv: true } });
    if (typeof lorry === "string" || typeof coach === "string") continue;
    compared++;
    if (lorry.seconds > car.seconds + 1) lorryDiverted++;
    if (coach.seconds > car.seconds + 1) coachDiverted++;
    // A coach is still stopped by a posted weight limit — it really does
    // weigh fourteen tonnes — so with the same ends it is never worse off
    // than the lorry, only less constrained by the signs aimed at freight.
    //
    // Only with the same ends: a coach may use a reserved road, so its
    // doorstep can snap somewhere the lorry's could not, and two routes
    // between different points are not comparable in either direction.
    const sameEnds =
      lorry.from.snapDistanceMeters === coach.from.snapDistanceMeters &&
      lorry.to.snapDistanceMeters === coach.to.snapDistanceMeters;
    if (sameEnds) {
      assert.ok(
        coach.seconds <= lorry.seconds + 1,
        "a coach was refused something the same weight of lorry was allowed"
      );
    }
  }

  assert.ok(compared > 20, `too few comparable pairs to conclude anything (${compared})`);
  assert.ok(lorryDiverted > 0, "no lorry was diverted at all, so this proves nothing");
  assert.ok(
    coachDiverted < lorryDiverted,
    `the coach was diverted as often as the lorry (${coachDiverted} vs ${lorryDiverted}): ` +
    "its weight is still being read as freight"
  );
});

test("a car-pool road asking three aboard turns away a car carrying two", async () => {
  const { reservedFor, hovMinimum, EDGE_FLAG_HOV_ONLY, EDGE_FLAG_HOV3_ONLY, EDGE_FLAG_PSV_ONLY } =
    await import("../scripts/osm_road_graph.mjs");
  const tags = (entries) => new Map(Object.entries(entries));

  // Two where nobody wrote a number down, which is what the sign reads.
  assert.equal(hovMinimum(tags({})), 2);
  assert.equal(hovMinimum(tags({ "hov:minimum": "3" })), 3);
  // A minimum below two is not a car-pool lane at all, and an unreadable one
  // is not a licence to invent a number.
  assert.equal(hovMinimum(tags({ "hov:minimum": "1" })), 2);
  assert.equal(hovMinimum(tags({ "hov:minimum": "" })), 2);
  assert.equal(hovMinimum(tags({ "hov:minimum": "three" })), 2);

  assert.equal(reservedFor(tags({ access: "hov" })), EDGE_FLAG_HOV_ONLY);
  assert.equal(reservedFor(tags({ access: "hov", "hov:minimum": "3" })), EDGE_FLAG_HOV3_ONLY);
  assert.equal(
    reservedFor(tags({ hov: "designated", "hov:minimum": "3" })),
    EDGE_FLAG_HOV3_ONLY
  );
  // The two are alternatives, never both: a road asks for one number.
  assert.equal(
    reservedFor(tags({ access: "hov", "hov:minimum": "3" })) & EDGE_FLAG_HOV_ONLY,
    0
  );
  // A minimum on a road reserved for buses says nothing about the buses.
  assert.equal(reservedFor(tags({ access: "psv", "hov:minimum": "3" })), EDGE_FLAG_PSV_ONLY);
});

test("occupancy decides which car-pool road a driver is given", async (t) => {
  // Built by hand rather than read off an extract: Luxembourg tags no
  // `hov:minimum` at all, and a rule about thresholds has to be provable on
  // a graph where every threshold is present.
  //
  // A straight road north whose middle segment is a car-pool road asking
  // three aboard, and a dearer way round that asks nothing. A driver alone
  // or with one passenger takes the long way; a third passenger opens the
  // short one.
  const lat = [45.500, 45.505, 45.510, 45.515, 45.520, 45.512, 45.518];
  const lon = [-73.600, -73.600, -73.600, -73.600, -73.600, -73.5920, -73.5920];
  const pairs = [[0, 1], [1, 2], [2, 3], [3, 4], [2, 5], [5, 6], [6, 4]];
  const from = [];
  const to = [];
  const weight = [];
  const flags = [];
  const EDGE_FLAG_HOV3_ONLY = 128;
  for (const [a, b] of pairs) {
    from.push(a, b);
    to.push(b, a);
    const reserved = a === 2 && b === 3;
    // The detour is deliberately dearer, so a car found on it can only be
    // there because the short way refused it rather than because it was quicker.
    const detour = (a === 2 && b === 5) || (a === 5 && b === 6) || (a === 6 && b === 4);
    const w = detour ? 600 : 100;
    weight.push(w, w);
    flags.push(reserved ? EDGE_FLAG_HOV3_ONLY : 0, reserved ? EDGE_FLAG_HOV3_ONLY : 0);
  }
  const graph = {
    nodeLat: Int32Array.from(lat, v => Math.round(v * 1e7)),
    nodeLon: Int32Array.from(lon, v => Math.round(v * 1e7)),
    edgeFrom: Uint32Array.from(from),
    edgeTo: Uint32Array.from(to),
    edgeWeightDs: Uint32Array.from(weight),
    edgeDistDm: Uint32Array.from(from, () => 5000),
    edgeName: Uint32Array.from(from, () => 1),
    edgeClass: Uint8Array.from(from, () => 0),
    edgeFlags: Uint8Array.from(flags),
    geomOffsets: Uint32Array.from(from.map((_, i) => i).concat([from.length])),
    geomBytes: Uint8Array.from(from, () => 0),
    names: ["", "Rue A"],
    profile: "car",
    classes: ["arterial", "local"]
  };

  const dir = mkdtempSync(join(tmpdir(), "rangefind-hov3-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 8, fanout: 4, topMaxCells: 4 });
  const engine = await openRouteGraphDir(dir);
  const ends = { from: { lat: lat[0], lon: lon[0] }, to: { lat: lat[4], lon: lon[4] } };
  const meters = async (vehicle) =>
    (await engine.route(vehicle ? { ...ends, vehicle } : ends)).distanceMeters;

  const round = await meters(null);
  const through = await meters({ occupancy: 3 });
  assert.ok(round > through, "the reserved road is not the short way, so this proves nothing");

  // Short of the number on the sign, the road is not theirs.
  assert.equal(await meters({ occupancy: 1 }), round, "a driver alone was let into a 3+ lane");
  assert.equal(await meters({ occupancy: 2 }), round, "two aboard was let into a 3+ lane");
  // At it and past it, it is.
  assert.equal(await meters({ occupancy: 3 }), through, "three aboard was refused a 3+ lane");
  assert.equal(await meters({ occupancy: 4 }), through, "four aboard was refused a 3+ lane");
  // A bus is entitled by being a bus, whatever number the sign asks for.
  assert.equal(await meters({ psv: true }), through, "a bus was refused a car-pool road");
  // And a vehicle that says nothing at all is a driver alone.
  assert.equal(await meters({}), round, "an unstated vehicle was given a reserved road");
});

test("Montréal's three-aboard reserved lane reaches the step, and gates on occupancy", async (t) => {
  // Luxembourg tags no `hov:minimum` anywhere, so the only real-data proof
  // of the two-versus-three distinction is Québec's. Skipped where that
  // index has not been built, which is why the synthetic graph above carries
  // the routing rule and this carries the tagging.
  const fixture = "bench/route/quebec-index";
  if (!existsSync(join(fixture, "manifest.json"))) {
    t.skip(`quebec index missing at ${fixture}`);
    return;
  }
  const { LANE_BUS, LANE_HOV, LANE_HOV3 } = await import("../scripts/osm_road_graph.mjs");
  const engine = await openRouteGraphDir(fixture);

  // The Autoroute du Souvenir corridor through Dorval, swept deterministically.
  let seed = 7;
  const rnd = () => ((seed = (seed * 1664525 + 1013904223) >>> 0) / 0x100000000);
  const reasons = new Set();
  for (let attempt = 0; attempt < 400; attempt++) {
    const from = { lat: 45.42 + rnd() * 0.06, lon: -73.80 + rnd() * 0.12 };
    const to = { lat: 45.42 + rnd() * 0.06, lon: -73.80 + rnd() * 0.12 };
    let route;
    try {
      route = await engine.route({ from, to });
    } catch {
      continue;
    }
    for (const step of route.steps || []) {
      for (const reason of step.laneAccess || []) if (reason) reasons.add(reason);
    }
  }

  const threePlus = [...reasons].filter(reason => (reason & LANE_HOV3) !== 0);
  assert.ok(
    threePlus.length > 0,
    `no three-aboard lane reached a step; reasons seen: ${[...reasons].join(", ")}`
  );
  // Montréal signs that lane for buses and for car-pools of three together,
  // which is why the reason is a set rather than a single class.
  assert.ok(
    threePlus.some(reason => (reason & LANE_BUS) !== 0),
    "the three-aboard lane arrived without the bus it is also signed for"
  );
  // Two and three are alternatives on any one lane, never both at once.
  assert.ok(
    threePlus.every(reason => (reason & LANE_HOV) === 0),
    "a lane claimed to ask for two and three aboard at the same time"
  );
  // And the ordinary two-aboard lane is still out there, or this would be
  // proving that everything got relabelled rather than that the split works.
  assert.ok(
    [...reasons].some(reason => (reason & LANE_HOV) !== 0),
    "no two-aboard lane survived at all"
  );
});
