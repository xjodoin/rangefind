import assert from "node:assert/strict";
import test from "node:test";
import { Record as Libp2pRecord } from "@libp2p/kad-dht";
import { MemoryDatastore } from "datastore-core/memory";
import { Key } from "interface-datastore/key";
import { pushVarint } from "../src/binary.js";
import { MeshNode } from "../src/pulsemesh/node.js";
import {
  createPeerPresentationLedger,
  frameAssembler,
  hardenDhtDatastore
} from "../src/pulsemesh/libp2p.js";
import { THREAD_MODE, decodeThreadLink, encodeThreadLink, encodeThreadRecord } from "../src/pulsemesh/thread_codec.js";
import { generateThreadKeypair, threadTopic } from "../src/pulsemesh/thread_crypto.js";
import { createThreadPublisher } from "../src/pulsemesh/thread_publish.js";
import { createThreadChannel } from "../src/pulsemesh/thread_session.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";
import { GOSSIP_ACCEPT, GOSSIP_IGNORE } from "../src/pulsemesh/validate.js";

const text = new TextEncoder();

function framed(payload) {
  const header = [];
  pushVarint(header, payload.length);
  return Uint8Array.from([...header, ...payload]);
}

test("fragmented frames allocate once and copy each payload byte once", () => {
  const payload = Uint8Array.from({ length: 65536 }, (_, index) => index & 0xff);
  const received = [];
  const assemble = frameAssembler(frame => received.push(frame));
  const bytes = framed(payload);
  for (const byte of bytes) assemble(Uint8Array.of(byte));

  assert.equal(received.length, 1);
  assert.deepEqual(received[0], payload);
  assert.deepEqual(assemble.stats, { allocations: 1, copiedBytes: payload.length },
    "fragment count does not cause reallocations or prefix copies");

  const limited = frameAssembler(() => assert.fail("oversize frame must not complete"), { maxFrameBytes: 1024 });
  assert.throws(() => limited(framed(new Uint8Array(1025)).subarray(0, 2)), /too large/,
    "the declared protocol cap is enforced before payload allocation");
  assert.equal(limited.stats.allocations, 0);
});

test("DHT storage rejects the advisory exploit shape and remains bounded", () => {
  const base = new MemoryDatastore();
  const guarded = hardenDhtDatastore(base, {
    decodeRecord: value => Libp2pRecord.deserialize(value),
    maxRecords: 2,
    maxRecordBytes: 256
  });
  const storeKey = suffix => new Key(`/pulsemesh-dht/record/${suffix}`);
  const record = (key, value = Uint8Array.of(1)) =>
    new Libp2pRecord(text.encode(key), value, new Date()).serialize();

  assert.throws(
    () => guarded.put(storeKey("attack"), record("attacker-controlled-key")),
    /no supported namespace/,
    "an unnamespaced PUT_VALUE never reaches the datastore"
  );
  assert.equal(base.has(storeKey("attack")), false);
  assert.throws(
    () => guarded.put(storeKey("large"), new Uint8Array(257)),
    /too large/,
    "the four-megabyte advisory payload is rejected at the write boundary"
  );

  const first = storeKey("one");
  const second = storeKey("two");
  const third = storeKey("three");
  guarded.put(first, record("/pk/one"));
  guarded.put(second, record("/pk/two"));
  guarded.put(third, record("/pk/three"));
  assert.equal(guarded.pulseMeshDhtRecordCount, 2);
  assert.equal(base.has(first), false, "oldest valid-shaped record is evicted at the cap");
  assert.equal(base.has(second), true);
  assert.equal(base.has(third), true);
});

test("bond presentation identity memory is LRU-, TTL-, and disconnect-bounded", () => {
  let now = 1000;
  const ledger = createPeerPresentationLedger({ maxPeers: 3, ttlMillis: 100, clock: () => now });
  ledger.add("a");
  ledger.add("b");
  ledger.add("c");
  ledger.add("d");
  assert.equal(ledger.size, 3);
  assert.equal(ledger.has("a"), false, "identity churn evicts oldest-first");
  assert.equal(ledger.delete("b"), true, "peer:disconnect removes the identity immediately");
  assert.equal(ledger.size, 2);
  now += 101;
  assert.equal(ledger.size, 0, "missed disconnects expire by TTL");
});

test("thread gossip is authenticated and rate-limited before forwarding", async t => {
  const epoch32 = sha256Utf8("pulsemesh-thread-validator-security");
  const epochHex = toHex(epoch32);
  const now = Math.floor(Date.now() / 1000) * 1000;
  let node;
  const network = {
    register(value) { node = value; },
    subscribe() {}, unsubscribe() {}, publish() {},
    peersOf() { return []; },
    schedule(fn, delay) { return setTimeout(fn, delay); }
  };
  const mesh = new MeshNode({
    id: "security-viewer",
    epochHex,
    network,
    cellOf: () => null,
    clock: () => now
  });
  const channel = createThreadChannel({ node: mesh, network, epochHex, clock: () => now });
  t.after(() => channel.close());

  const keypair = await generateThreadKeypair();
  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32,
    mode: THREAD_MODE.FINE,
    clock: () => now,
    publish: record => { emitted.push(record); }
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: keypair.publicKey,
    epochPrefix8: epoch32.subarray(0, 8),
    notAfter: Math.floor(now / 1000) + 3600
  }));
  const follow = await channel.follow(link);
  await publisher.handleFix({
    lat: 45.5, lon: -73.6, speedMps: 8, nowMillis: now,
    match: { segment: "5/1/0", ratio: 0.5 }
  });
  const honest = emitted[0];
  const topic = threadTopic(epochHex.slice(0, 16), honest.tag);

  assert.equal(await mesh.judgeGossip(topic, honest.bytes, "publisher", now), GOSSIP_ACCEPT);
  assert.equal(follow.subscriber.highestSeq, honest.seq,
    "the subscriber validates and commits the update in the pre-forward hook");

  let cryptoAttempts = 0;
  const realAccept = follow.subscriber.accept;
  follow.subscriber.accept = async (...args) => {
    cryptoAttempts++;
    return realAccept(...args);
  };
  for (let index = 0; index < 5; index++) {
    const junk = encodeThreadRecord({
      epochPrefix8: epoch32.subarray(0, 8),
      tag: honest.tag,
      seq: 100 + index,
      ciphertext: new Uint8Array(48)
    });
    assert.equal(await mesh.judgeGossip(topic, junk.bytes, "link-holder", now), GOSSIP_IGNORE);
  }
  assert.equal(cryptoAttempts, 4,
    "the startup burst is bounded and further garbage is dropped before AEAD work");
});
