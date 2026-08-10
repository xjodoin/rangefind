// §16 — PulseMesh over LoRa. A simulated radio hub with Meshtastic
// physics (237-byte payload cap, broadcast flooding with duplicates, no
// streams) carries real protocol bytes between a phone-side node, a
// bonded bridge, and the IP mesh. The claims under test: records fit
// and flow, the bridge vouches only what it validated, airtime buys
// incidents before statistics, and a sealed thread crosses both worlds
// without the bridge being able to read it.

import assert from "node:assert/strict";
import test from "node:test";
import { timeBucketFromMillis, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode, createLoopbackNetwork } from "../src/pulsemesh/node.js";
import {
  LORA_CONSTANTS,
  LORA_MAX_PAYLOAD,
  LORA_PROFILE_OVERRIDES,
  createLoraBridge,
  createLoraNetwork
} from "../src/pulsemesh/lora.js";
import { PROOF_BOND, encodePMB1, encodePMC1, encodePMI1 } from "../src/pulsemesh/codec.js";
import { sha256Utf8, toHex, fromHex } from "../src/pulsemesh/sha256.js";

const EPOCH_HEX = toHex(sha256Utf8("pulsemesh-lora-test"));
const EPOCH32 = fromHex(EPOCH_HEX);
const PREFIX8 = EPOCH32.slice(0, 8);
const CELL = { x: 144 * 64 + 5, y: 180 * 64 + 9 };
const ZONE = zoneOfDetailCell(CELL);
const cellOf = record => (record.leafCell < 64 ? { x: CELL.x + (record.leafCell % 8), y: CELL.y } : null);
const cellContext = leafCell => (leafCell < 64
  ? { polylineCount: 64, classOf: () => "secondary", metersOf: () => 400 }
  : null);

/** A radio hub with Meshtastic physics: broadcast, cap, duplicates. */
function makeHub({ maxPayload = LORA_MAX_PAYLOAD } = {}) {
  const radios = [];
  function makeRadio(id) {
    const listeners = [];
    const radio = {
      id,
      maxPayload,
      sent: [],
      send(bytes) {
        if (bytes.length > maxPayload) throw new Error(`frame ${bytes.length} B exceeds the radio payload cap`);
        radio.sent.push(bytes);
        for (const other of radios) {
          if (other === radio) continue;
          for (const cb of other.listeners) cb(bytes, id);
        }
      },
      onReceive(cb) { listeners.push(cb); },
      listeners,
      peers: () => radios.filter(r => r !== radio).map(r => r.id)
    };
    radios.push(radio);
    return radio;
  }
  return { makeRadio };
}

let reportCounter = 0;
function contribution({ nowMillis, geomRef = 6, speedBin = 4 }) {
  return encodePMC1({
    epochPrefix8: PREFIX8,
    leafCell: 5,
    geomRef,
    timeBucket: timeBucketFromMillis(nowMillis),
    speedBin,
    qualityBin: 6,
    meters: 400,
    ttlSeconds: 90,
    reportId: sha256Utf8(`lora-r-${reportCounter++}`).slice(0, 16),
    proofType: PROOF_BOND,
    proof: new Uint8Array(0)
  }).bytes;
}

function incident({ nowMillis }) {
  return encodePMI1({
    epochPrefix8: PREFIX8,
    leafCell: 5,
    geomRef: 6,
    ratioQ12: 2048,
    timeBucket: timeBucketFromMillis(nowMillis),
    type: 1,
    polarity: 1,
    ttlSeconds: 600,
    reportId: sha256Utf8(`lora-i-${reportCounter++}`).slice(0, 16),
    proofType: PROOF_BOND,
    proof: new Uint8Array(0)
  }).bytes;
}

test("the LoRa profile stays inside the bootstrap's tunable envelope", () => {
  assert.equal(LORA_CONSTANTS.MAX_AGE_RECEIPT, 90);
  assert.equal(LORA_CONSTANTS.MAX_FUTURE_SKEW, 45);
  assert.equal(LORA_CONSTANTS.EMIT_INTERVAL, 60);
  // Everything not overridden is untouched — same protocol, one profile.
  assert.equal(LORA_CONSTANTS.CONTRIB_TTL, 90);
  assert.equal(LORA_CONSTANTS.BOND_BIRTHDAY_BITS, 44);
  for (const key of Object.keys(LORA_PROFILE_OVERRIDES)) {
    assert.ok(LORA_CONSTANTS[key] === LORA_PROFILE_OVERRIDES[key], `${key} applied`);
  }
});

test("phone-side transport: radio gossip reaches the node, physics enforced", () => {
  let now = Date.now();
  const clock = () => now;
  const hub = makeHub();
  const carRadio = hub.makeRadio("car");
  const neighborRadio = hub.makeRadio("neighbor");

  const network = createLoraNetwork(carRadio, { clock, bytesPerMinute: 240 });
  const node = new MeshNode({
    id: "car-phone",
    epochHex: EPOCH_HEX,
    constants: LORA_CONSTANTS,
    cellOf,
    cellContext,
    network,
    clock,
    transport: "wire"
  });
  node.subscribeZones([ZONE], now);
  network.admitRadioPeer("neighbor");

  // A neighbor's batch arrives off the air: no topics existed on the
  // channel, yet the record lands validated in the store.
  neighborRadio.send(encodePMB1([contribution({ nowMillis: now })]));
  assert.equal(node.store.size(), 1, "radio gossip validated and stored");

  // An unadmitted radio sender is dropped by rule 5 — same rule, new air.
  const strangerRadio = hub.makeRadio("stranger");
  strangerRadio.send(encodePMB1([contribution({ nowMillis: now })]));
  assert.equal(node.store.size(), 1);
  assert.ok((node.stats.dropsByRule.rule5 ?? 0) >= 1, "spoofable senders earn nothing by default");

  // Outbound: the node publishes; the frame waits for the airtime budget.
  node.publishRecord({ bytes: contribution({ nowMillis: now }) }, { nowMillis: now });
  assert.equal(carRadio.sent.length, 0, "nothing is aired before flush");
  network.flush(now);
  assert.equal(carRadio.sent.length, 1, "one 45-byte frame after flush");
  assert.ok(carRadio.sent[0].length <= LORA_MAX_PAYLOAD);

  // Garbage on the channel is counted, not crashed on.
  neighborRadio.send(Uint8Array.of(1, 2, 3, 4, 5));
  assert.equal(network.stats.badFrames, 1);
});

test("airtime buys incidents before statistics, and the budget holds", () => {
  let now = Date.now();
  const hub = makeHub();
  const radio = hub.makeRadio("solo");
  const network = createLoraNetwork(radio, { clock: () => now, bytesPerMinute: 100, queueCap: 8 });
  new MeshNode({
    id: "solo-phone", epochHex: EPOCH_HEX, constants: LORA_CONSTANTS,
    cellOf, cellContext, network, clock: () => now, transport: "wire"
  });

  // Fill with contributions, then one incident arrives last.
  for (let i = 0; i < 3; i++) network.publish("t", encodePMB1([contribution({ nowMillis: now })]));
  network.publish("t", incident({ nowMillis: now }));

  // Budget of 100 B/min drains the incident (44 B) then ONE batch (49 B);
  // the rest wait even though they were queued first.
  network.flush(now);
  assert.equal(radio.sent.length, 2);
  assert.equal(String.fromCharCode(...radio.sent[0].subarray(0, 4)), "PMI1", "the hazard flies first");

  // Next minute, the budget refills.
  now += 61_000;
  network.flush(now);
  assert.equal(radio.sent.length, 4, "queue drains across budget windows");

  // Oversize and IP-domain frames never reach the air.
  assert.equal(network.publish("t", new Uint8Array(LORA_MAX_PAYLOAD + 50).fill(65)), undefined);
  assert.ok(network.stats.air.dropOversize >= 1);
});

test("the bonded bridge vouches only what it validated, and mutes liars", async () => {
  let now = Date.now();
  const clock = () => now;
  const hub = makeHub();
  const busRadio = hub.makeRadio("bus");
  const bridgeRadio = hub.makeRadio("bridge");

  // IP side: bridge + consumer over the loopback, mutually bonded.
  const ip = createLoopbackNetwork({ clock });
  const bridgeNode = new MeshNode({
    id: "bridge", epochHex: EPOCH_HEX, constants: LORA_CONSTANTS,
    cellOf, cellContext, network: ip, clock, transport: "wire"
  });
  const consumer = new MeshNode({
    id: "consumer", epochHex: EPOCH_HEX, constants: LORA_CONSTANTS,
    cellOf, cellContext, network: ip, clock, transport: "wire"
  });
  bridgeNode.subscribeZones([ZONE], now);
  consumer.subscribeZones([ZONE], now);
  const far = now + 86400 * 1000;
  consumer.bondedPeers.set("bridge", far);
  bridgeNode.bondedPeers.set("consumer", far);

  const bridge = await createLoraBridge({ node: bridgeNode, radio: bridgeRadio, cellContext, clock });

  // Uplink: the bus's contribution crosses the air, the bridge validates
  // it against its own map, and vouches it onto the IP mesh — where the
  // consumer accepts it because the BRIDGE is bonded, not the bus.
  busRadio.send(encodePMB1([contribution({ nowMillis: now })]));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(bridge.stats.upVouched, 1);
  assert.equal(consumer.store.size(), 1, "the IP mesh never needed to know LoRa exists");

  // Garbage: a record naming a segment that does not exist is dropped at
  // the bridge — its bond never vouches for it — and repeated provable
  // lies mute the radio sender locally.
  for (let i = 0; i < 3; i++) {
    busRadio.send(encodePMB1([contribution({ nowMillis: now, geomRef: 200 })]));
    await new Promise(resolve => setImmediate(resolve));
  }
  assert.equal(consumer.store.size(), 1, "nothing invalid was vouched");
  assert.equal(bridge.stats.muted, 1, "three strikes mute the sender");
  busRadio.send(encodePMB1([contribution({ nowMillis: now })]));
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(consumer.store.size(), 1, "a muted sender is not even validated");

  // Downlink: an incident accepted on the IP side is queued for the air
  // — a hazard matters most where there is no coverage.
  consumer.publishRecord({ bytes: incident({ nowMillis: now }) }, { nowMillis: now });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(bridge.stats.downIncidents, 1);
  await bridge.tick(now);
  assert.equal(bridgeRadio.sent.length, 1);
  assert.equal(String.fromCharCode(...bridgeRadio.sent[0].subarray(0, 4)), "PMI1");
  bridge.close();
});

test("a sealed thread crosses the air and the bridge cannot read it", async () => {
  let now = Date.now();
  const clock = () => now;
  const hub = makeHub();
  const busRadio = hub.makeRadio("bus");
  const bridgeRadio = hub.makeRadio("bridge");

  const { generateThreadKeypair, threadTopic } = await import("../src/pulsemesh/thread_crypto.js");
  const { encodeThreadLink, decodeThreadLink, THREAD_MODE } = await import("../src/pulsemesh/thread_codec.js");
  const { createThreadPublisher } = await import("../src/pulsemesh/thread_publish.js");
  const { createThreadSubscriber } = await import("../src/pulsemesh/thread_consume.js");

  const ip = createLoopbackNetwork({ clock });
  const bridgeNode = new MeshNode({
    id: "bridge", epochHex: EPOCH_HEX, constants: LORA_CONSTANTS,
    cellOf, cellContext, network: ip, clock, transport: "wire"
  });
  const bridge = await createLoraBridge({ node: bridgeNode, radio: bridgeRadio, cellContext, clock });

  // A parent's stub subscriber on the IP side captures thread topics.
  const captured = [];
  ip.register({ id: "parent", clock, onGossip: (topic, payload) => captured.push({ topic, payload }) });

  // The bus — out of coverage — publishes its thread over the radio.
  const keypair = await generateThreadKeypair();
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32: EPOCH32,
    mode: THREAD_MODE.FINE,
    clock,
    publish: async emitted => busRadio.send(emitted.bytes)
  });
  const link = decodeThreadLink(encodeThreadLink({
    threadSecret: publisher.threadSecret, rootPublicKey: publisher.rootPublicKey,
    epochPrefix8: PREFIX8,
    notAfter: Math.floor(now / 1000) + 3600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32: EPOCH32, clock });
  for (const topic of await subscriber.topics(now)) ip.subscribe("parent", topic);

  const result = await publisher.handleFix({
    lat: 45.5, lon: -73.6, speedMps: 9, nowMillis: now,
    match: { segment: "3/2/0", distMeters: 3, ratio: 0.4, snappedLatE7: 455000000, snappedLonE7: -736000000 }
  });
  assert.ok(result.published, "the bus emitted");
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(bridge.stats.upThreads, 1, "the bridge shuttled the sealed frame");
  assert.equal(captured.length, 1, "it reached the thread topic on the IP mesh");
  const verdict = await subscriber.accept(captured[0].payload, { nowMillis: now });
  assert.ok(verdict.ok, "the parent decrypts and verifies under the thread key");
  assert.ok(Number.isFinite(verdict.update.seq));

  // Downlink with an operator-held link: IP-side thread gossip is aired.
  const bridge2 = await createLoraBridge({
    node: bridgeNode, radio: bridgeRadio, cellContext, clock, threadLinks: [link]
  });
  await bridge2.tick(now); // derives and subscribes the thread topics
  const emitted = await publisher.handleFix({
    lat: 45.501, lon: -73.6, speedMps: 9, nowMillis: (now += 20_000),
    match: { segment: "3/2/0", distMeters: 3, ratio: 0.5, snappedLatE7: 455010000, snappedLonE7: -736000000 }
  });
  assert.ok(emitted.published);
  const topic = threadTopic(EPOCH_HEX.slice(0, 16), emitted.record.tag);
  assert.ok(bridge2.onIpGossip(topic, emitted.record.bytes), "held-link thread gossip queues for the air");
  await bridge2.tick(now);
  assert.ok(bridgeRadio.sent.some(frame => String.fromCharCode(...frame.subarray(0, 4)) === "PMT1"));
  bridge.close();
  bridge2.close();
});
