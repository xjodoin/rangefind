import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  MinHeap,
  bucketWeight,
  decodeRouteCell,
  decodeRouteOverlay,
  encodeRouteCell,
  encodeRouteOverlay
} from "../src/route_graph.js";
import { buildRouteGraph } from "../src/route_graph_build.js";
import { openRouteGraphDir } from "../src/route_graph_node.js";

// Deterministic LCG so the synthetic graph is stable across runs.
function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

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
    geomOffsets: [0, ...edges.map((_, i) => i + 1)],
    geomBytes: { data: new Uint8Array(edges.map(() => 0)), length: edges.length },
    log: () => {}
  });
  const costs = { uturn: 150, left: 40, right: 15, slightLeft: 20, slightRight: 8 };
  const expanded = expandTurnCosts(makeContext([]), costs);
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
    isClique: Uint8Array.from([1, 0, 1])
  };
  const decodedOverlay = decodeRouteOverlay(encodeRouteOverlay(overlay));
  assert.equal(decodedOverlay.level, 2);
  assert.deepEqual([...decodedOverlay.nodes], [10, 55, 300]);
  assert.deepEqual([...decodedOverlay.weights], [500, 1200, 70]);
  assert.deepEqual([...decodedOverlay.isClique], [1, 0, 1]);
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
