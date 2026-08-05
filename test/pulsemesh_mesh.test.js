// M2 — loopback mesh: real protocol bytes end-to-end. Three contributor
// nodes drive the base route of a synthetic graph and report a jam; a
// consumer node receives the PMB1 gossip, recomputes aggregates, and its
// provider makes engine.route({ live }) choose a different corridor. A
// late joiner converges through anti-entropy with the §11.3 padded fetch.
// This closes phase 1 of pulsemesh.md.

import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildRouteGraph } from "../src/route_graph_build.js";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { DEFAULT_CONSTANTS, detailCellForE7, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode, canonicalJson, createLoopbackNetwork, signBootstrap, verifyBootstrap } from "../src/pulsemesh/node.js";
import { createContributor } from "../src/pulsemesh/contribute.js";
import { decodePMC1, encodePMC1, encodePMD1, parseSegment } from "../src/pulsemesh/codec.js";
import { fromHex, sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

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
    geomBytes.push(0);
    geomOffsets.push(geomBytes.length);
  };
  for (let y = 0; y < height; y++) {
    for (let x = 0; x < width; x++) {
      const index = y * width + x;
      if (x + 1 < width) { addEdge(index, index + 1); addEdge(index + 1, index); }
      if (y + 1 < height) { addEdge(index, index + width); addEdge(index + width, index); }
    }
  }
  return {
    nodeLat, nodeLon,
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

test("M2: contribute → gossip → aggregate → route reroutes around the jam", async (t) => {
  const graph = syntheticGraph(20, 18, 77);
  const dir = mkdtempSync(join(tmpdir(), "rangefind-pulsemesh-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  buildRouteGraph(graph, dir, { leafNodes: 48, fanout: 4, topMaxCells: 6 });
  const engine = await openRouteGraphDir(dir);
  const epochHex = engine.root.sourceHash;
  assert.match(epochHex, /^[0-9a-f]{64}$/, "epoch is the route-graph sourceHash");

  const point = node => ({ lat: graph.nodeLat[node] / 1e7, lon: graph.nodeLon[node] / 1e7 });
  const from = point(21);
  const to = point(20 * 18 - 25);
  const base = await engine.route({ from, to });
  const baseSegments = base.edges.map(edge => edge.segment);
  const metersBySegKey = new Map();
  for (const edge of base.edges) {
    const { leafCell, geomRef } = parseSegment(edge.segment);
    metersBySegKey.set(`${leafCell}/${geomRef}`, edge.meters);
  }

  // Deterministic shared cell placement: the z15 cell of the leaf's bbox
  // center — a pure function of the record and the static index.
  const cellOf = record => {
    const bbox = engine.root.leaves[record.leafCell]?.bbox;
    if (!bbox) return null;
    return detailCellForE7((bbox.minLat + bbox.maxLat) / 2, (bbox.minLon + bbox.maxLon) / 2);
  };

  // The engine blends confidence against real wall-clock age, so the
  // virtual clock must be anchored at real now (bucket-aligned).
  let now = Math.floor(Date.now() / 15000) * 15000;
  const clock = () => now;
  const constants = { ...DEFAULT_CONSTANTS, EMIT_INTERVAL: 1 };
  const network = createLoopbackNetwork({ clock });
  const nodeOptions = { epochHex, constants, cellOf, network, clock, transport: "loopback" };
  const consumer = new MeshNode({ id: "consumer", ...nodeOptions });
  const contributors = ["car-1", "car-2", "car-3"].map(id => new MeshNode({ id, ...nodeOptions }));

  // The consumer computes its corridor locally and subscribes to the z9
  // zones the corridor crosses.
  const zones = new Map();
  for (const segment of baseSegments) {
    const { leafCell, geomRef } = parseSegment(segment);
    const cell = cellOf({ leafCell, geomRef });
    if (cell) {
      const zone = zoneOfDetailCell(cell);
      zones.set(`${zone.x}/${zone.y}`, zone);
    }
  }
  consumer.subscribeZones([...zones.values()], now);

  // Three vehicles crawl the base corridor together at ~7 km/h, one PMC1
  // per vehicle per segment through the full contributor state machine.
  const drivers = contributors.map((meshNode, index) => ({
    index,
    contributor: createContributor({
      epoch32: meshNode.epoch32,
      epochPrefix8: meshNode.epochPrefix8,
      snap: async fix => fix.match,
      publish: async result => meshNode.publishRecord(result.record, { nowMillis: now }),
      proofType: 0,
      clock,
      constants,
      metersOf: segKey => metersBySegKey.get(segKey) || 0
    })
  }));
  for (const segment of baseSegments) {
    now += 1200;
    for (const { index, contributor } of drivers) {
      await contributor.handleFix({
        match: { segment, distMeters: 2 + index, ratio: 0.5, snappedLatE7: 0, snappedLonE7: 0 },
        speedMps: 2
      });
    }
  }

  assert.ok(consumer.stats.gossipAccepted >= baseSegments.length * 2, "consumer received the corridor's gossip");

  // The provider makes the router avoid the jammed corridor.
  const jammed = await engine.route({ from, to, live: consumer.provider });
  assert.ok(jammed.live.applied > 0, "live states applied");
  assert.equal(jammed.live.provider, "pulsemesh");
  const overlap = jammed.edges.filter(edge => baseSegments.includes(edge.segment)).length / jammed.edges.length;
  assert.ok(overlap < 0.6, `route diverges from the jammed corridor (overlap ${overlap.toFixed(2)})`);
  assert.ok(jammed.adjustedSeconds > base.seconds, "live estimate reflects the jam");

  // Unknown epoch: the provider returns [] and the route stays static.
  const stale = await engine.route({
    from, to,
    live: { name: "pulsemesh", fetch: query => consumer.provider.fetch({ ...query, epoch: "0".repeat(64) }) }
  });
  assert.equal(stale.seconds, base.seconds, "epoch mismatch degrades to the static metric");
  assert.equal(stale.live.applied, 0);

  // A late joiner converges via anti-entropy: digest diff, padded/split
  // PMQ1 fetch, set-union merge — identical digests afterwards.
  // Each padded fetch splits cells across random peers (§11.3) and the
  // car nodes hold only their own records, so one round can be partial —
  // anti-entropy is a repair loop, and convergence is eventual.
  const late = new MeshNode({ id: "late", ...nodeOptions });
  const digestsMatch = zone => {
    const a = encodePMD1({ epochPrefix8: late.epochPrefix8, ...late.store.digestForZone(zone, now) });
    const b = encodePMD1({ epochPrefix8: consumer.epochPrefix8, ...consumer.store.digestForZone(zone, now) });
    return toHex(a) === toHex(b);
  };
  let rounds = 0;
  for (; rounds < 25 && ![...zones.values()].every(digestsMatch); rounds++) {
    for (const zone of zones.values()) {
      await late.antiEntropyWith("consumer", zone, now);
    }
  }
  for (const zone of zones.values()) {
    assert.ok(digestsMatch(zone), `zone ${zone.x}/${zone.y} digests converge (after ${rounds} rounds)`);
  }
  assert.ok(late.stats.cellsRequested >= late.stats.cellsWanted, "every fetch was padded");
  const lateRoute = await engine.route({ from, to, live: late.provider });
  assert.ok(lateRoute.live.applied > 0, "late joiner routes on converged data");

  // TTL: after 90 s of silence the mesh forgets and routing degrades to
  // static — an empty region simply forgets.
  now += 95 * 1000;
  await consumer.tick(now);
  assert.equal(consumer.store.size(), 0, "receiver-enforced TTL emptied the store");
  const faded = await engine.route({ from, to, live: consumer.provider });
  assert.equal(faded.seconds, base.seconds, "mesh silence degrades to the static route");
});

test("node: gossip size cap, epoch overlap window, and unsubscribe linger", async () => {
  let now = Math.floor(Date.now() / 15000) * 15000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });
  const currentEpoch = "a".repeat(64);
  const previousEpoch = "b".repeat(64);
  const cellOf = () => ({ x: 9216, y: 11520 });
  const zone = { x: 9216 >> 6, y: 11520 >> 6 };
  const node = new MeshNode({
    id: "consumer",
    epochHex: currentEpoch,
    previousEpochHex: previousEpoch,
    cellOf,
    network,
    clock,
    rng: () => 0.5,
    transport: "loopback"
  });

  // MAX_GOSSIP_BYTES: an oversized payload is dropped before decoding.
  node.subscribeZones([zone], now);
  const topic = [...node.subscribedTopics].find(name => name.includes(currentEpoch.slice(0, 16)));
  node.onGossip(topic, new Uint8Array(DEFAULT_CONSTANTS.MAX_GOSSIP_BYTES + 1), "peer", now);
  assert.equal(node.stats.dropsByRule.oversize, 1, "oversized gossip is dropped, not parsed");

  // §11.5: during EPOCH_OVERLAP a consumer also subscribes to the previous
  // epoch's topics, so records in flight across a republish are not lost.
  const previousTopics = [...node.subscribedTopics].filter(name => name.includes(previousEpoch.slice(0, 16)));
  assert.ok(previousTopics.length > 0, "previous-epoch topics joined during the overlap");

  // After the overlap they stop being accepted — segment ids are
  // meaningful for exactly one graph epoch.
  now += DEFAULT_CONSTANTS.EPOCH_OVERLAP * 1000 + 1000;
  const before = node.stats.gossipDropped;
  node.onGossip(previousTopics[0], new Uint8Array(8), "peer", now);
  assert.equal(node.stats.gossipDropped, before, "a stale-epoch topic is ignored outright, not counted as a drop");
  node.subscribeZones([zone], now);
  assert.equal(
    [...node.subscribedTopics].filter(name => name.includes(previousEpoch.slice(0, 16))).length,
    0,
    "previous-epoch topics are released once the overlap expires"
  );

  // §11.4 UNSUB_LINGER: a zone that leaves the corridor is held before it
  // is dropped, so a brief detour is not announced as unsub/resub.
  const heldTopics = new Set(node.subscribedTopics);
  node.subscribeZones([{ x: zone.x + 8, y: zone.y + 8 }], now);
  const stillHeld = [...heldTopics].filter(name => node.subscribedTopics.has(name)).length;
  assert.ok(stillHeld > 0, "old zone lingers rather than unsubscribing immediately");
  now += (DEFAULT_CONSTANTS.UNSUB_LINGER + 30) * 1000;
  node.subscribeZones([{ x: zone.x + 8, y: zone.y + 8 }], now);
  assert.equal(
    [...heldTopics].filter(name => node.subscribedTopics.has(name)).length,
    0,
    "the linger expires and the zone is released"
  );
});

test("anti-entropy elides the digest once two peers agree", async () => {
  // Digest exchange measures at 4–5× the gossip cost, and in a converged
  // zone every byte of it confirms what the requester already knows. The
  // requester sends a 12-byte zone fold; a matching responder answers
  // with 12 bytes instead of the whole digest. Round cadence is
  // untouched — an adaptive backoff would have saved more bytes and cost
  // convergence, which is the wrong trade for a mesh whose entire
  // guarantee is that peers agree.
  let now = Math.floor(Date.now() / 15000) * 15000;
  const clock = () => now;
  const cell = { x: 144 * 64 + 5, y: 180 * 64 + 9 };
  const zone = zoneOfDetailCell(cell);
  const cellOf = () => cell;
  const network = createLoopbackNetwork({ clock });
  const epochHex = toHex(sha256Utf8("fold-gating"));
  const prefix8 = fromHex(epochHex).subarray(0, 8);
  const options = { epochHex, constants: DEFAULT_CONSTANTS, cellOf, network, clock, transport: "loopback" };
  const a = new MeshNode({ id: "a", ...options });
  const b = new MeshNode({ id: "b", ...options });
  a.subscribeZones([zone], now);
  b.subscribeZones([zone], now);

  let counter = 0;
  const seed = node => {
    const reportId = new Uint8Array(16);
    reportId[0] = ++counter;
    const record = decodePMC1(encodePMC1({
      epochPrefix8: prefix8, leafCell: counter % 8, geomRef: counter % 40,
      timeBucket: Math.floor(now / 15000), speedBin: 5, qualityBin: 7, meters: 400,
      ttlSeconds: 90, reportId, proofType: 0, proof: new Uint8Array(0)
    }).bytes);
    node.store.addContribution(record, { nowMillis: now });
  };

  // While records keep arriving the folds differ, so the digest is sent.
  for (let round = 0; round < 5; round++) {
    seed(a);
    seed(b);
    await a.antiEntropyWith("b", zone, now);
    await b.antiEntropyWith("a", zone, now);
    now += 10000;
  }
  assert.equal(a.stats.antiEntropyAgreed, 0, "peers that genuinely differ still exchange digests");
  assert.equal(a.store.size(), b.store.size(), "and converge by doing so");

  // Converged: every subsequent round elides.
  const before = network.counters("a").streamIn;
  for (let round = 0; round < 5; round++) {
    await a.antiEntropyWith("b", zone, now);
    now += 10000;
  }
  assert.equal(a.stats.antiEntropyAgreed, 5, "a converged zone agrees every round");
  assert.equal(b.stats.antiEntropyElided, 5, "and the responder never builds the digest");
  const bytesPerRound = (network.counters("a").streamIn - before) / 5;
  assert.ok(bytesPerRound <= 16, `the agreement answer is tiny (${bytesPerRound} bytes/round)`);

  // A new record breaks the agreement, and repair resumes immediately.
  seed(b);
  await a.antiEntropyWith("b", zone, now);
  assert.equal(a.store.size(), b.store.size(), "divergence is repaired on the very next round");
});

test("signed bootstrap verifies and rejects tampering", async () => {
  const { generateKeyPairSync } = await import("node:crypto");
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const publicRaw = publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("hex");
  const seed = privateKey.export({ format: "der", type: "pkcs8" }).subarray(-32).toString("hex");

  const unsigned = {
    format: "pulsemesh-bootstrap-v1",
    epoch: "f".repeat(64),
    previousEpoch: null,
    constants: { POW_DIFFICULTY: 18 },
    incidentPolicy: { suppressedTypes: [5] },
    bootstrapPeers: [],
    relays: [],
    authorityKeys: [],
    publicKey: publicRaw
  };
  const signed = await signBootstrap(unsigned, seed);
  assert.deepEqual(await verifyBootstrap(signed, publicRaw), { ok: true });

  const tampered = { ...signed, constants: { POW_DIFFICULTY: 1 } };
  assert.equal((await verifyBootstrap(tampered, publicRaw)).ok, false, "tampered constants fail");
  assert.equal((await verifyBootstrap(signed, "00".repeat(32))).ok, false, "unexpected key fails");

  // Canonical JSON is key-sorted at every level, no whitespace.
  assert.equal(canonicalJson({ b: 1, a: { d: 2, c: [3, null] } }), '{"a":{"c":[3,null],"d":2},"b":1}');
});
