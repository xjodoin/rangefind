// §8.4 forfeiture and ban propagation. The property under test is the
// asymmetry: FIRST-HAND evidence (provable rule 10–12 failures) revokes
// a bond; REMOTE testimony (PMX1), however corroborated, only lowers
// trust weight — bounded, once per target, recoverable. Three colluding
// bonds can make a mesh distrust an honest peer for a while; they can
// never silence it. That ceiling is the design, and these tests pin it.

import assert from "node:assert/strict";
import test from "node:test";
import { DEFAULT_CONSTANTS, timeBucketFromMillis, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode, banPeerHash16, createLoopbackNetwork } from "../src/pulsemesh/node.js";
import {
  BAN_REASON_INVALID_RECORDS,
  PROOF_BOND,
  decodePMX1,
  encodePMB1,
  encodePMC1,
  encodePMX1
} from "../src/pulsemesh/codec.js";
import { shardOfCell } from "../src/pulsemesh/topics.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH_HEX = toHex(sha256Utf8("pulsemesh-ban-test"));
const PREFIX8 = sha256Utf8("pulsemesh-ban-test").slice(0, 8);
const CELL = { x: 144 * 64 + 5, y: 180 * 64 + 9 };
const ZONE = zoneOfDetailCell(CELL);
const cellOf = record => (record.leafCell < 64 ? { x: CELL.x + (record.leafCell % 8), y: CELL.y } : null);
// polylineCount 10: any geomRef ≥ 20 provably names a segment that does
// not exist — rule 11, the trust-penalty class.
const cellContext = leafCell => (leafCell < 64
  ? { polylineCount: 10, classOf: () => "secondary", metersOf: () => 400 }
  : null);

function makeWorld({ now = Date.now() } = {}) {
  let millis = now;
  const clock = () => millis;
  const network = createLoopbackNetwork({ clock });
  const makeNode = id => {
    const node = new MeshNode({
      id,
      epochHex: EPOCH_HEX,
      constants: DEFAULT_CONSTANTS,
      cellOf,
      cellContext,
      network,
      clock,
      transport: "wire"
    });
    node.subscribeZones([ZONE], millis);
    return node;
  };
  return { clock, network, makeNode, tick: ms => { millis += ms; } };
}

let reportCounter = 0;
function record({ geomRef = 4, nowMillis }) {
  return encodePMB1([encodePMC1({
    epochPrefix8: PREFIX8,
    leafCell: 5,
    geomRef,
    timeBucket: timeBucketFromMillis(nowMillis),
    speedBin: 6,
    qualityBin: 5,
    meters: 400,
    ttlSeconds: 90,
    reportId: sha256Utf8(`ban-r-${reportCounter++}`).slice(0, 16),
    proofType: PROOF_BOND,
    proof: new Uint8Array(0)
  }).bytes]);
}

function topicOf(node, nowMillis) {
  void node;
  // A legal current-window topic whose shard matches the test records'
  // cell (leafCell 5) — rule 8 runs before the trust-penalty rules, so a
  // wrong shard would shield the violation this file exists to punish.
  const shard = shardOfCell({ x: CELL.x + 5, y: CELL.y });
  return `/rangefind/pulsemesh/1/${EPOCH_HEX.slice(0, 16)}/${ZONE.x}/${ZONE.y}/${Math.floor(nowMillis / 1000 / 300)}/${shard}`;
}

function banBytes({ target, reportLabel, nowMillis }) {
  return encodePMX1({
    epochPrefix8: PREFIX8,
    targetHash16: banPeerHash16(target),
    reason: BAN_REASON_INVALID_RECORDS,
    timeBucket: timeBucketFromMillis(nowMillis),
    reportId: sha256Utf8(`ban-x-${reportLabel}`).slice(0, 16)
  });
}

test("PMX1 encodes and decodes byte-exactly", () => {
  const bytes = banBytes({ target: "mallory", reportLabel: "codec", nowMillis: 1754265600000 });
  const decoded = decodePMX1(bytes);
  assert.equal(decoded.kind, "ban");
  assert.equal(decoded.reason, BAN_REASON_INVALID_RECORDS);
  assert.deepEqual(Array.from(decoded.targetHash16), Array.from(banPeerHash16("mallory")));
  assert.throws(() => decodePMX1(Uint8Array.from([...bytes, 0])), /trailing/);
  assert.ok(bytes.length < 64, `an announcement is small (${bytes.length} B)`);
});

test("first-hand evidence forfeits the bond and publishes testimony", () => {
  const { clock, makeNode } = makeWorld();
  const victim = makeNode("victim");
  const witness = makeNode("witness");
  // Both nodes have met the attacker's bond; the witness has also met
  // the victim's (testimony is only accepted from bonded deliverers).
  const far = clock() + 86400 * 1000;
  victim.bondedPeers.set("mallory", far);
  witness.bondedPeers.set("mallory", far);
  witness.bondedPeers.set("victim", far);
  witness.registerBond; // (bond hash index is filled via registerBond in real flow)
  witness.peerHashIndex.set(toHex(banPeerHash16("mallory")), "mallory");

  const now = clock();
  const topic = topicOf(victim, now);

  // Two provable rule-11 violations: 1000 → 500 → 250 (floor).
  victim.onGossip(topic, record({ geomRef: 40, nowMillis: now }), "mallory", now);
  assert.ok(victim.isBonded("mallory"), "one violation is not a ban");
  victim.onGossip(topic, record({ geomRef: 42, nowMillis: now }), "mallory", now);

  assert.equal(victim.isBonded("mallory"), false, "trust floor revokes the bond");
  assert.equal(victim.stats.bansForfeited, 1);
  assert.equal(victim.stats.bansPublished, 1, "testimony went out");

  // Re-presenting the same (valid!) bond is refused for its lifetime.
  const refused = victim.registerBond(new Uint8Array(4), "mallory", now);
  assert.match(refused.reason, /forfeited/);

  // Subsequent records from the forfeited peer die on rule 5.
  victim.onGossip(topic, record({ geomRef: 4, nowMillis: now }), "mallory", now);
  assert.ok((victim.stats.dropsByRule.rule5 ?? 0) >= 1, "proofless records need a live bond");

  // The witness — subscribed to the same zone over the loopback — got
  // the PMX1 and counted it as one accuser: testimony, not a verdict.
  assert.equal(witness.stats.bansAccepted, 1);
  assert.equal(witness.stats.bansCorroborated, 0);
  assert.equal(witness.trust.get("mallory"), DEFAULT_CONSTANTS.TRUST_INIT, "one accuser changes nothing");
});

test("corroborated testimony lowers weight once and never revokes", () => {
  const { clock, makeNode } = makeWorld();
  const node = makeNode("observer");
  const now = clock();
  const far = now + 86400 * 1000;
  // Mallory and three accusers are all bonded here.
  for (const peer of ["mallory", "a1", "a2", "a3"]) node.bondedPeers.set(peer, far);
  node.peerHashIndex.set(toHex(banPeerHash16("mallory")), "mallory");
  const topic = topicOf(node, now);

  node.onGossip(topic, banBytes({ target: "mallory", reportLabel: 1, nowMillis: now }), "a1", now);
  node.onGossip(topic, banBytes({ target: "mallory", reportLabel: 2, nowMillis: now }), "a2", now);
  assert.equal(node.trust.get("mallory"), 1000, "below BAN_MIN_SOURCES nothing applies");

  node.onGossip(topic, banBytes({ target: "mallory", reportLabel: 3, nowMillis: now }), "a3", now);
  assert.equal(node.stats.bansCorroborated, 1);
  assert.equal(node.trust.get("mallory"), 1000 - DEFAULT_CONSTANTS.BAN_REMOTE_PENALTY, "one bounded penalty");
  assert.ok(node.isBonded("mallory"), "REMOTE testimony never revokes");

  // More testimony about the same target applies nothing further.
  node.onGossip(topic, banBytes({ target: "mallory", reportLabel: 4, nowMillis: now }), "a1", now);
  assert.equal(node.trust.get("mallory"), 625, "the penalty is once per corroborated target");

  // Mallory's records are still accepted — down-weighted, not silenced.
  node.onGossip(topic, record({ geomRef: 4, nowMillis: now }), "mallory", now);
  assert.equal(node.stats.gossipAccepted, 1, "defamation cannot silence an honest peer");

  // But testimony plus ONE first-hand provable violation reaches the
  // floor: 625 − 500 → 250. That synergy is the point of propagation.
  node.onGossip(topic, record({ geomRef: 40, nowMillis: now }), "mallory", now);
  assert.equal(node.isBonded("mallory"), false, "first-hand evidence completes the forfeiture");
  assert.equal(node.stats.bansForfeited, 1);
});

test("testimony is gated: unbonded deliverers, replays, and rate", () => {
  const { clock, makeNode } = makeWorld();
  const node = makeNode("observer");
  const now = clock();
  const topic = topicOf(node, now);
  node.bondedPeers.set("a1", now + 86400 * 1000);

  // Unbonded deliverer: dropped before it counts.
  node.onGossip(topic, banBytes({ target: "x", reportLabel: "u1", nowMillis: now }), "stranger", now);
  assert.equal(node.stats.bansAccepted, 0);
  assert.ok((node.stats.dropsByRule.banRule5 ?? 0) >= 1);

  // Replay of the same reportId: counted once.
  const dup = banBytes({ target: "x", reportLabel: "dup", nowMillis: now });
  node.onGossip(topic, dup, "a1", now);
  node.onGossip(topic, dup, "a1", now);
  assert.equal(node.stats.bansAccepted, 1);
  assert.ok((node.stats.dropsByRule.banReplay ?? 0) >= 1);

  // Rate: BAN_PEER_RATE per window per deliverer.
  for (let i = 0; i < DEFAULT_CONSTANTS.BAN_PEER_RATE + 2; i++) {
    node.onGossip(topic, banBytes({ target: `t${i}`, reportLabel: `r${i}`, nowMillis: now }), "a1", now);
  }
  assert.ok((node.stats.dropsByRule.banRule7 ?? 0) >= 1, "testimony volume is rate-bounded");
});

test("a forfeited peer is re-admissible after its bond bucket ends", () => {
  const { clock, makeNode, tick } = makeWorld();
  const node = makeNode("victim");
  const now = clock();
  const until = now + 3600 * 1000; // the bond we held expires in an hour
  node.bondedPeers.set("mallory", until);
  const topic = topicOf(node, now);
  node.onGossip(topic, record({ geomRef: 40, nowMillis: now }), "mallory", now);
  node.onGossip(topic, record({ geomRef: 42, nowMillis: now }), "mallory", now);
  assert.equal(node.isBonded("mallory"), false);
  assert.match(node.registerBond(new Uint8Array(4), "mallory", clock()).reason, /forfeited/);

  // After the forfeited bond's own lifetime the refusal lapses — the
  // deterrent is the re-mint, not permanent exile of a peerId.
  tick(3600 * 1000 + 1000);
  const afterwards = node.registerBond(new Uint8Array(4), "mallory", clock());
  assert.doesNotMatch(afterwards.reason ?? "", /forfeited/, "refusal expires with the bucket");
});
