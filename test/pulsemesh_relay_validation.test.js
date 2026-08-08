// §5.1 — relaying implies validating. Real js-libp2p TCP throughout,
// because the whole claim is about what a GossipSub relay puts back on
// the wire.
//
// GossipSub forwards a message to its mesh peers on receipt, before the
// application sees it. A node that validates only on delivery therefore
// spends its §5.4 bond vouching, at the transport layer, for bytes it
// never checked — and rule 5 has the far side accept them, because rule 5
// asks only whether the *delivering* peer is bonded. Any peer could
// launder invalid records through an honest relay. The fix is a GossipSub
// topic validator: the §6 pipeline runs before the forward decision.
//
// The topology is a line, so the observer can only hear through the relay:
//
//   attacker ── relay (bonded, honest) ── observer
//
// Skips cleanly when the optional libp2p peer dependencies are absent.

import assert from "node:assert/strict";
import test from "node:test";
import { DEFAULT_CONSTANTS, topicWindowFromMillis, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode, createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { PROOF_BOND, encodePMC1, encodePMQ1 } from "../src/pulsemesh/codec.js";
import { topicForCell } from "../src/pulsemesh/topics.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH_HEX = toHex(sha256Utf8("pulsemesh-relay-validation-test"));
// B=16 keeps every real mint at ~1 ms while exercising the whole §5.4
// path; EMIT_INTERVAL is irrelevant here (records are hand-encoded).
const CONSTANTS = { ...DEFAULT_CONSTANTS, BOND_BIRTHDAY_BITS: 16 };

// The same tiny static world the other wire tests use. `residential`
// caps rule 10 at 70 km/h × 1.15 = 80.5, so speedBin 44 (222.5 km/h) is
// a provable, map-checkable lie and speedBin 5 (27.5 km/h) is fine.
const CELL = { x: 144 * 64 + 5, y: 180 * 64 + 9 };
const cellOf = record => (record.leafCell < 64 ? { x: CELL.x + (record.leafCell % 8), y: CELL.y } : null);
const cellContext = leafCell => (leafCell < 64
  ? { polylineCount: 64, classOf: () => "residential", metersOf: () => 400 }
  : null);
const ZONE = zoneOfDetailCell(CELL);

async function waitFor(check, { timeoutMs = 20000, stepMs = 50, label = "condition" } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise(resolve => setTimeout(resolve, stepMs));
  }
}

/** A settle window long enough for a forward to have crossed if it were going to. */
async function settle(millis = 600) {
  await new Promise(resolve => setTimeout(resolve, millis));
}

/** One PMC1 plus the reportId it carries (the encoder does not hand it back). */
function record({ node, leafCell = 0, speedBin = 5, reportId = null }) {
  const id = reportId ?? new Uint8Array(16);
  if (!reportId) globalThis.crypto.getRandomValues(id);
  const encoded = encodePMC1({
    epochPrefix8: node.epochPrefix8,
    leafCell,
    geomRef: 4,
    timeBucket: Math.floor(Date.now() / 15000),
    speedBin,
    qualityBin: 7,
    meters: 400,
    ttlSeconds: 90,
    reportId: id,
    proofType: PROOF_BOND,
    proof: new Uint8Array(0)
  });
  return { encoded, reportId: id };
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
 * attacker ── relay ── observer, with no edge between the ends: whatever
 * the observer receives, it received from the relay. Both the attacker
 * and the relay mint real bonds, so rule 5 is satisfied at every hop and
 * the only thing that can stop a record is validation itself.
 */
async function buildLine(t, { createPulseMeshHost, createLibp2pNetwork }) {
  const hosts = [];
  const networks = [];
  t.after(async () => {
    for (const network of networks) await network.close();
    await Promise.all(hosts.map(host => host.stop().catch(() => {})));
  });
  const makePeer = async () => {
    const host = await createPulseMeshHost();
    const network = createLibp2pNetwork(host);
    hosts.push(host);
    networks.push(network);
    await network.ready;
    const node = new MeshNode({
      id: host.peerId.toString(),
      epochHex: EPOCH_HEX,
      constants: CONSTANTS,
      cellOf,
      cellContext,
      network,
      transport: "wire"
    });
    node.subscribeZones([ZONE]);
    return { host, network, node };
  };

  const attacker = await makePeer();
  const relay = await makePeer();
  const observer = await makePeer();

  // A line, dialled in one direction only. Nothing here introduces the
  // ends to each other: no DHT, and GossipSub peer exchange is off.
  await attacker.host.dial(relay.host.getMultiaddrs()[0]);
  await relay.host.dial(observer.host.getMultiaddrs()[0]);

  // The attacker holds a real admission bond — the point of the test is
  // that a *bonded* peer still cannot launder an invalid record.
  assert.ok(await attacker.network.mintBond(), "the attacker mints its own bond");
  assert.ok(await relay.network.mintBond(), "the honest relay mints");
  await waitFor(() => relay.node.isBonded(attacker.node.id), { label: "relay registers the attacker's bond" });
  await waitFor(() => observer.node.isBonded(relay.node.id), { label: "observer registers the relay's bond" });

  const topic = topicForCell({
    epochPrefix16hex: attacker.node.epochPrefix16hex,
    cell: cellOf({ leafCell: 0 }),
    window: topicWindowFromMillis(Date.now())
  });
  await waitFor(
    () => attacker.host.services.pubsub.getSubscribers(topic).length >= 1
      && relay.host.services.pubsub.getSubscribers(topic).length >= 2,
    { label: "subscriptions propagate along the line" }
  );
  // Subscriptions are announced immediately; the mesh a relay actually
  // *forwards* on is grafted on a heartbeat. Wait for the real thing, or
  // the first publish races the graft and nothing crosses the second hop
  // for reasons that have nothing to do with validation.
  const meshed = (peer, want) => (peer.host.services.pubsub.mesh.get(topic)?.size ?? 0) >= want;
  await waitFor(() => meshed(attacker, 1) && meshed(relay, 2) && meshed(observer, 1), {
    label: "GossipSub mesh grafted on the publish topic"
  });

  assert.equal(
    observer.host.getConnections().filter(c => c.remotePeer.toString() === attacker.node.id).length,
    0,
    "the observer must have no direct path to the attacker"
  );
  return { attacker, relay, observer };
}

test("§5.1: a relay forwards only what it validated, and still forwards what it did", async t => {
  const libp2p = await loadLibp2p(t);
  if (!libp2p) return;
  const { attacker, relay, observer } = await buildLine(t, libp2p);

  // --- 1. The laundering attempt --------------------------------------
  // 222.5 km/h on a residential road: a rule 10 failure, provable against
  // the map the relay holds, and one the trust ledger treats as a lie.
  attacker.node.publishRecord(record({ node: attacker.node, speedBin: 44 }).encoded);
  await waitFor(() => (relay.node.stats.dropsByRule.rule10 ?? 0) >= 1, {
    label: "the relay evaluates rule 10 before forwarding"
  });
  await settle();

  assert.equal(observer.node.store.size(), 0, "the observer never received the record");
  assert.equal(observer.node.stats.gossipAccepted, 0, "and accepted nothing");
  assert.equal(
    observer.network.stats.gossipIn,
    0,
    "the relay did not forward the bytes at all — not merely 'the observer dropped them'"
  );
  assert.ok(relay.network.stats.gossipRejected >= 1, "a provable lie is Rejected, which scores the source down");
  assert.equal(relay.node.store.size(), 0, "the relay did not store it either");

  // --- 2. The failure mode that matters most --------------------------
  // The same topology, a valid record: propagation must still work, or
  // the fix would be indistinguishable from breaking the mesh.
  attacker.node.publishRecord(record({ node: attacker.node, speedBin: 5 }).encoded);
  await waitFor(() => observer.node.store.size() === 1, {
    label: `a valid record still reaches the observer through the relay (have ${observer.node.store.size()})`
  });
  assert.equal(relay.node.store.size(), 1, "the relay validated and kept it");
  assert.ok(relay.network.stats.gossipRelayed >= 1, "and vouched for it onward");
  assert.ok(observer.network.stats.gossipIn > 0, "the observer heard it via the relay");
});

test("§5.1: a replay is Ignored rather than Rejected, and is validated exactly once", async t => {
  const libp2p = await loadLibp2p(t);
  if (!libp2p) return;
  const { attacker, relay, observer } = await buildLine(t, libp2p);

  // --- 1. Validate once ------------------------------------------------
  // rule 7's token bucket and the §8.4 trust path both mutate state, so a
  // record validated in the topic validator and again on delivery would
  // charge one peer twice for one record. Count the calls.
  let validations = 0;
  const realValidate = relay.node.validator.validateContribution;
  relay.node.validator.validateContribution = (...args) => {
    validations++;
    return realValidate(...args);
  };

  const first = record({ node: attacker.node, speedBin: 5 });
  attacker.node.publishRecord(first.encoded);
  await waitFor(() => observer.node.store.size() === 1, { label: "the record traverses the gossip path" });
  await settle();
  assert.equal(validations, 1, "the relay validated the relayed record exactly once");

  // --- 2. A replay is not misbehaviour --------------------------------
  // Same reportId, different bytes — so GossipSub's own duplicate cache
  // does not swallow it and the §6 rule 6 window is what fires. A relay
  // handing us something we already hold is doing its job; Rejecting it
  // would let an attacker score down honest peers by replaying their own
  // traffic back at them.
  const rejectedBefore = relay.network.stats.gossipRejected;
  const trustBefore = relay.node.trust.get(attacker.node.id);
  attacker.node.publishRecord(record({
    node: attacker.node,
    speedBin: 6,
    reportId: first.reportId
  }).encoded);
  await waitFor(() => (relay.node.stats.dropsByRule.rule6 ?? 0) >= 1, { label: "the relay sees the replay" });
  await settle();

  assert.equal(relay.network.stats.gossipRejected, rejectedBefore, "a replay never Rejects");
  assert.ok(relay.network.stats.gossipIgnored >= 1, "it Ignores instead");
  assert.equal(relay.node.trust.get(attacker.node.id), trustBefore, "and the sender's trust is untouched");
  assert.equal(observer.node.store.size(), 1, "the replay went no further");
});

test("§5.1: transports with no relay step keep validating on delivery", async t => {
  // Loopback hands a message straight to its one local node — there is no
  // forward to gate, and nothing was vouched for on the way in — so the
  // §6 pipeline must still run exactly where it always did.
  const network = createLoopbackNetwork();
  const makeNode = id => {
    const node = new MeshNode({
      id,
      epochHex: EPOCH_HEX,
      constants: CONSTANTS,
      cellOf,
      cellContext,
      network,
      transport: "loopback"
    });
    node.subscribeZones([ZONE]);
    return node;
  };
  const sender = makeNode("sender");
  const receiver = makeNode("receiver");

  sender.publishRecord(record({ node: sender, speedBin: 5 }).encoded);
  assert.equal(receiver.store.size(), 1, "loopback delivery still validates and stores");
  assert.equal(receiver.stats.gossipAccepted, 1);

  sender.publishRecord(record({ node: sender, speedBin: 44 }).encoded);
  assert.equal(receiver.store.size(), 1, "the implausible record is still dropped");
  assert.ok((receiver.stats.dropsByRule.rule10 ?? 0) >= 1, "by rule 10, on the delivery path");

  // §11.6 pull: a snapshot is solicited, not relayed, so it never touches
  // the gossip verdict path — and must still be validated on merge.
  const puller = makeNode("puller");
  // PMQ1 batches are padded to 8/16/32 cells by design (§11.3).
  const cell = cellOf({ leafCell: 0 });
  const snapshot = sender.onStream(
    encodePMQ1({ epochPrefix8: sender.epochPrefix8, cells: Array.from({ length: 8 }, () => cell) }),
    sender.id
  );
  assert.ok(snapshot, "the sender answered with a PMS1");
  // The sender holds two records: locally produced ones are stored
  // without validation, so its own implausible one is in the snapshot.
  const merged = puller.mergeSnapshot(snapshot, sender.id);
  assert.equal(merged, 1, "the merge path validates too — only the plausible record is taken");
  assert.equal(puller.store.size(), 1);
});
