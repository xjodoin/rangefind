// §12.1 — a fleet's own seed, bridged. Real js-libp2p TCP throughout,
// because every claim here is about what actually crosses a socket:
//
//   driver    an island phone that has NEVER minted a §5.4 bond. It dials
//             the seed and nothing else, exactly as a device that got the
//             address out of a sealed ticket does.
//   seed      two hosts in one process: the island-facing one the driver
//             dialled, and a listen-less upstream one that dials the
//             keeper. The bridge joins them.
//   upstream  an ordinary bonded keeper standing in for the wider mesh.
//
// The three policies are asserted as three separate worlds because a
// policy is fixed when the bridge is built — which is the point: a fleet
// decides once whether it is contributing, receiving, or neither.
//
// Skips cleanly when the optional libp2p peer dependencies are absent.

import assert from "node:assert/strict";
import test from "node:test";
import { DEFAULT_CONSTANTS, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode } from "../src/pulsemesh/node.js";
import { ADMIT_CONNECTED, createFleetBridge } from "../src/pulsemesh/bridge.js";
import { createContributor } from "../src/pulsemesh/contribute.js";
import { PROOF_BOND, decodePMC1, encodePMC1 } from "../src/pulsemesh/codec.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH_HEX = toHex(sha256Utf8("pulsemesh-fleet-bridge-test"));
// B=16 keeps every real mint at ~1 ms while exercising the whole §5.4
// path; EMIT_INTERVAL is effectively disabled — cadence is covered
// elsewhere and this test is about what crosses.
const CONSTANTS = { ...DEFAULT_CONSTANTS, BOND_BIRTHDAY_BITS: 16, EMIT_INTERVAL: 0.01 };

// The same tiny static world the wire tests use: every leaf maps into one
// z15 cell in one zone, and the leaf context makes rules 10–12 fireable.
const CELL = { x: 144 * 64 + 5, y: 180 * 64 + 9 };
const cellOf = record => (record.leafCell < 64 ? { x: CELL.x + (record.leafCell % 8), y: CELL.y } : null);
const cellContext = leafCell => (leafCell < 64
  ? { polylineCount: 64, classOf: () => "secondary", metersOf: () => 400 }
  : null);
const ZONE = zoneOfDetailCell(CELL);

async function waitFor(check, { timeoutMs = 20000, stepMs = 100, label = "condition" } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise(resolve => setTimeout(resolve, stepMs));
  }
}

async function loadLibp2p(t) {
  try {
    const module = await import("../src/pulsemesh/libp2p.js");
    await import("libp2p");
    return module;
  } catch {
    t.skip("libp2p optional dependencies not installed");
    return null;
  }
}

/**
 * Builds the three-party world for one policy and returns its handles.
 * Everything is torn down through `t.after`.
 */
async function buildWorld(t, { createPulseMeshHost, createLibp2pNetwork }, policy) {
  const hosts = [];
  const networks = [];
  const bridges = [];
  t.after(async () => {
    for (const bridge of bridges) bridge.close();
    for (const network of networks) await network.close();
    await Promise.all(hosts.map(host => host.stop().catch(() => {})));
  });
  const makePeer = async (options = {}) => {
    const host = await createPulseMeshHost(options);
    const network = createLibp2pNetwork(host);
    hosts.push(host);
    networks.push(network);
    await network.ready;
    return { host, network };
  };
  const makeNode = ({ host, network }, options = {}) => {
    const node = new MeshNode({
      id: host.peerId.toString(),
      epochHex: EPOCH_HEX,
      constants: CONSTANTS,
      cellOf,
      cellContext,
      network,
      transport: "wire",
      ...options
    });
    node.subscribeZones([ZONE]);
    return node;
  };

  const upstreamPeer = await makePeer();
  const upstreamNode = makeNode(upstreamPeer);
  assert.ok(await upstreamPeer.network.mintBond(), "the wider mesh's keeper mints");

  // The seed: island-facing host (the one a driver dials) plus, when
  // bridging, a listen-less upstream host that dials the keeper.
  const seedIsland = await makePeer();
  const seedIslandNode = makeNode(seedIsland);
  assert.ok(await seedIsland.network.mintBond(), "the seed's downward bond");

  let seedUpstream = null;
  let seedUpstreamNode = null;
  if (policy !== "off") {
    seedUpstream = await makePeer({
      listen: [],
      bootstrapPeers: [upstreamPeer.host.getMultiaddrs()[0].toString()]
    });
    seedUpstreamNode = makeNode(seedUpstream, { readOnly: policy === "in" });
    if (policy === "both") {
      assert.ok(await seedUpstream.network.mintBond(), "the seed's upstream bond");
    }
  }

  const bridge = createFleetBridge({
    island: seedIslandNode,
    upstream: seedUpstreamNode,
    policy,
    zones: [ZONE],
    admit: ADMIT_CONNECTED,
    islandPeers: () => seedIsland.network.peersOf()
  });
  bridges.push(bridge);

  // The island driver. It mints nothing, ever.
  const driver = await makePeer();
  const driverNode = makeNode(driver);
  await driver.host.dial(seedIsland.host.getMultiaddrs()[0]);
  await waitFor(() => seedIsland.network.peersOf().includes(driverNode.id), {
    label: "driver connected to the seed"
  });
  bridge.admitConnected();
  // The driver mints nothing, but it does verify the SEED's bond — that
  // is what lets rule 5 accept anything the seed delivers downward.
  await waitFor(() => driverNode.isBonded(seedIslandNode.id), {
    label: "the seed's bond registered on the driver"
  });

  return {
    upstreamPeer, upstreamNode,
    seedIsland, seedIslandNode,
    seedUpstream, seedUpstreamNode,
    driver, driverNode,
    bridge
  };
}

/** Drives a contributor through `count` segments on the given node. */
async function drive(node, { count = 4, offset = 0, vehicle = 0 } = {}) {
  const contributor = createContributor({
    epoch32: node.epoch32,
    epochPrefix8: node.epochPrefix8,
    snap: async fix => fix.match,
    publish: async result => node.publishRecord(result.record),
    constants: CONSTANTS,
    proofType: PROOF_BOND,
    metersOf: () => 400
  });
  for (let segment = 0; segment < count; segment++) {
    await contributor.handleFix({
      match: {
        segment: `${offset + segment}/${vehicle}/0`,
        distMeters: 3,
        ratio: 0.5,
        snappedLatE7: 0,
        snappedLonE7: 0
      },
      speedMps: 2,
      nowMillis: Date.now()
    });
    await new Promise(resolve => setTimeout(resolve, 50));
  }
}

const meshFormed = (peer, node) => () => [...node.subscribedTopics]
  .some(topic => peer.host.services.pubsub.getSubscribers(topic).length >= 1);

test("§12.1 both: the seed vouches for an unbonded driver, and the wider mesh answers", async t => {
  const libp2p = await loadLibp2p(t);
  if (!libp2p) return;
  const world = await buildWorld(t, libp2p, "both");

  // The admission: directly connected, nothing minted.
  assert.ok(world.bridge.isAdmitted(world.driverNode.id), "the driver is admitted by connection");
  assert.equal(world.driver.network.stats.bondsSent, 0, "the driver minted no bond");
  assert.ok(world.driverNode.stats.bondsAccepted >= 1, "but it did verify the seed's");

  await waitFor(meshFormed(world.driver, world.driverNode), { label: "island gossip mesh" });
  await waitFor(meshFormed(world.seedUpstream, world.seedUpstreamNode), { label: "upstream gossip mesh" });

  await drive(world.driverNode, { count: 4, vehicle: 1 });

  await waitFor(() => world.seedIslandNode.store.size() === 4, {
    label: `seed accepted the island's records (${world.seedIslandNode.store.size()}/4)`
  });
  // The claim: they reached the wider mesh, and they got there because
  // the seed republished them under its own bond — the driver has none,
  // and the upstream keeper has never heard of the driver at all.
  await waitFor(() => world.upstreamNode.store.size() === 4, {
    label: `records crossed upstream (${world.upstreamNode.store.size()}/4)`
  });
  assert.equal(world.bridge.stats.upCrossed, 4, "four records crossed outward");
  assert.equal(world.upstreamNode.isBonded(world.driverNode.id), false, "upstream never bonded the driver");
  assert.ok(world.upstreamNode.isBonded(world.seedUpstreamNode.id), "it bonded the seed, which is what vouched");
  for (const entry of world.upstreamNode.store.contributionsForSegment("0/2")) {
    assert.equal(entry.deliveredBy, world.seedUpstreamNode.id, "delivered by the seed, not the driver");
  }

  // And the other direction: the wider mesh reaches the island driver.
  await drive(world.upstreamNode, { count: 3, offset: 20, vehicle: 2 });
  await waitFor(() => world.driverNode.store.size() >= 7, {
    label: `upstream records reached the island driver (${world.driverNode.store.size()}/7)`
  });
  assert.ok(world.bridge.stats.downCrossed >= 3, "records crossed inward");
});

test("§12.1 in: the island receives, and publishes nothing outward", async t => {
  const libp2p = await loadLibp2p(t);
  if (!libp2p) return;
  const world = await buildWorld(t, libp2p, "in");

  // §11.6 upstream, bonded downward — the asymmetry, asserted.
  assert.equal(world.seedUpstreamNode.subscribedTopics.size, 0, "read-only upstream joins no gossip topic");
  assert.equal(world.seedUpstream.network.stats.bondsSent, 0, "and mints nothing");
  assert.ok(world.seedIslandNode.subscribedTopics.size > 0, "while the island side is a normal gossip peer");
  assert.ok(world.bridge.isAdmitted(world.driverNode.id), "and still admits its own drivers");

  await waitFor(meshFormed(world.driver, world.driverNode), { label: "island gossip mesh" });
  await drive(world.driverNode, { count: 3, vehicle: 1 });
  await waitFor(() => world.seedIslandNode.store.size() === 3, {
    label: `the island itself still works (${world.seedIslandNode.store.size()}/3)`
  });

  // Upstream publishes its own; the read-only side pulls it on tick.
  await drive(world.upstreamNode, { count: 3, offset: 20, vehicle: 2 });
  await waitFor(async () => {
    await world.seedUpstreamNode.tick();
    return world.seedUpstreamNode.store.size() >= 3;
  }, { label: `read-only upstream pulled the wider mesh (${world.seedUpstreamNode.store.size()}/3)`, stepMs: 250 });

  await waitFor(() => world.driverNode.store.size() >= 6, {
    label: `upstream records reached the island driver (${world.driverNode.store.size()}/6)`
  });
  assert.ok(world.bridge.stats.downCrossed >= 3, "records crossed inward");

  // The whole point of `in`: nothing the fleet published is upstream.
  assert.equal(world.bridge.stats.upCrossed, 0, "nothing crossed outward");
  assert.equal(world.upstreamNode.store.size(), 3, "the keeper holds only its own three records");
  for (const entry of world.upstreamNode.store.contributionsForSegment("0/2")) {
    assert.fail(`an island record reached the wider mesh: ${entry.idHex}`);
  }
});

test("§12.1 off: an island stays an island in both directions", async t => {
  const libp2p = await loadLibp2p(t);
  if (!libp2p) return;
  const world = await buildWorld(t, libp2p, "off");
  assert.equal(world.seedUpstreamNode, null, "no upstream side exists at all");

  await waitFor(meshFormed(world.driver, world.driverNode), { label: "island gossip mesh" });
  await drive(world.driverNode, { count: 3, vehicle: 1 });
  await waitFor(() => world.seedIslandNode.store.size() === 3, {
    label: `the island works on its own (${world.seedIslandNode.store.size()}/3)`
  });

  await drive(world.upstreamNode, { count: 3, offset: 20, vehicle: 2 });
  // Give both directions a generous chance to leak before asserting they did not.
  for (let i = 0; i < 5; i++) {
    await world.upstreamNode.tick();
    await new Promise(resolve => setTimeout(resolve, 200));
  }
  assert.equal(world.upstreamNode.store.size(), 3, "no island record reached the wider mesh");
  assert.equal(world.driverNode.store.size(), 3, "and no upstream record reached the island");
  assert.equal(world.bridge.stats.upCrossed, 0);
  assert.equal(world.bridge.stats.downCrossed, 0);
});

test("§12.1: a driver the seed's own map refuses is never republished, and is muted", async t => {
  const libp2p = await loadLibp2p(t);
  if (!libp2p) return;
  const world = await buildWorld(t, libp2p, "both");
  await waitFor(meshFormed(world.driver, world.driverNode), { label: "island gossip mesh" });
  await waitFor(meshFormed(world.seedUpstream, world.seedUpstreamNode), { label: "upstream gossip mesh" });

  // A record whose segment does not exist in the seed's leaf: geomRef
  // 200 >> 1 = 100, and the leaf holds 64 polylines. Rule 11, provable,
  // trust-penalizing. It rides its OWN cell's topic (publishRecord
  // derives that from cellOf) — rule 8 runs before rules 10–12, so a
  // record delivered on the wrong shard would never reach the rule the
  // test is about.
  const lie = () => {
    const reportId = new Uint8Array(16);
    globalThis.crypto.getRandomValues(reportId);
    return encodePMC1({
      epochPrefix8: world.driverNode.epochPrefix8,
      leafCell: 3,
      geomRef: 200,
      timeBucket: Math.floor(Date.now() / 15000),
      speedBin: 5,
      qualityBin: 7,
      meters: 400,
      ttlSeconds: 90,
      reportId,
      proofType: PROOF_BOND,
      proof: new Uint8Array(0)
    });
  };

  // Two provable violations floor the peer's trust (1000 → 500 → 250) and
  // the node forfeits it: bond revoked, re-registration refused for the
  // bucket, PMX1 testimony gossiped. That is the mute — no second strike
  // counter exists here, unlike the LoRa bridge, because on IP the
  // delivering peer is an authenticated peerId and §8.4 already applies.
  for (let i = 0; i < 2; i++) {
    world.driverNode.publishRecord(lie());
    await new Promise(resolve => setTimeout(resolve, 150));
  }
  await waitFor(() => (world.seedIslandNode.stats.dropsByRule.rule11 ?? 0) >= 2, {
    label: `the seed's own map refused the records (${world.seedIslandNode.stats.dropsByRule.rule11 ?? 0}/2)`
  });
  assert.equal(world.seedIslandNode.store.size(), 0, "nothing invalid entered the seed's store");
  assert.equal(world.bridge.stats.upCrossed, 0, "and nothing invalid was republished under the seed's bond");
  assert.equal(world.upstreamNode.store.size(), 0, "the wider mesh saw none of it");

  // Muted: forfeited locally, and the bridge does not hand the admission
  // straight back on the next tick.
  assert.ok(world.seedIslandNode.locallyBanned.has(world.driverNode.id), "§8.4 forfeiture fired");
  assert.deepEqual(world.bridge.mutedPeers(), [world.driverNode.id]);
  world.bridge.tick();
  assert.equal(world.bridge.isAdmitted(world.driverNode.id), false, "re-admission refused");
  assert.equal(world.seedIslandNode.isBonded(world.driverNode.id), false);

  // A now-honest record from the same peer gets no free ride either: with
  // the admission gone, rule 5 is what drops it.
  const before = world.seedIslandNode.stats.dropsByRule.rule5 ?? 0;
  await drive(world.driverNode, { count: 2, vehicle: 4 });
  await waitFor(() => (world.seedIslandNode.stats.dropsByRule.rule5 ?? 0) > before, {
    label: "a muted peer's valid records are dropped by rule 5"
  });
  assert.equal(world.upstreamNode.store.size(), 0, "still nothing crossed");
});

test("§12.1: bridging is confined to the pinned zones", async t => {
  const libp2p = await loadLibp2p(t);
  if (!libp2p) return;
  const world = await buildWorld(t, libp2p, "both");

  // A record that validates fine but lands outside the pinned zone: the
  // bridge is the layer that refuses it, so a depot's uplink never
  // carries a continent it did not ask for. Fed straight into the tap
  // (there is no island topic for a zone the seed does not subscribe to,
  // which is itself the first line of the same defence).
  const foreignZone = { x: 1, y: 1 };
  const narrow = createFleetBridge({
    island: world.seedIslandNode,
    upstream: world.seedUpstreamNode,
    policy: "both",
    zones: [foreignZone],
    admit: ADMIT_CONNECTED,
    islandPeers: () => world.seedIsland.network.peersOf()
  });
  t.after(() => narrow.close());

  const reportId = new Uint8Array(16);
  globalThis.crypto.getRandomValues(reportId);
  const record = decodePMC1(encodePMC1({
    epochPrefix8: world.seedIslandNode.epochPrefix8,
    leafCell: 1,
    geomRef: 4,
    timeBucket: Math.floor(Date.now() / 15000),
    speedBin: 5,
    qualityBin: 7,
    meters: 400,
    ttlSeconds: 90,
    reportId,
    proofType: PROOF_BOND,
    proof: new Uint8Array(0)
  }).bytes);
  world.seedIslandNode.onRecordAccepted(record, { fromPeer: world.driverNode.id, nowMillis: Date.now() });
  assert.equal(narrow.stats.upOutOfZone, 1, "an out-of-zone record is refused by the bridge");
  assert.equal(narrow.stats.upCrossed, 0);
});
