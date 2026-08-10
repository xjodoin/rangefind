// T3 — discovery and catch-up. The sleep/wake test is the point: a
// subscriber offline for four minutes rejoins and pulls the gap from
// *another subscriber's* cache, with no designated host anywhere in the
// test. Availability scales with audience size instead of costing a
// server.

import assert from "node:assert/strict";
import test from "node:test";
import {
  applyThreadResponse,
  buildThreadRequest,
  createThreadCache,
  encodeThreadCacheResponse
} from "../src/pulsemesh/thread_cache.js";
import { createThreadPublisher, THREAD_CONSTANTS } from "../src/pulsemesh/thread_publish.js";
import { createThreadSubscriber } from "../src/pulsemesh/thread_consume.js";
import {
  THREAD_MODE,
  decodeThreadRecord,
  decodeThreadLink,
  decodeThreadRequest,
  encodeThreadLink,
  encodeThreadRecord
} from "../src/pulsemesh/thread_codec.js";
import {
  generateThreadKeypair,
  threadRendezvous,
  threadTag,
  threadTopic,
  threadWindow
} from "../src/pulsemesh/thread_crypto.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH32 = sha256Utf8("pulsemesh-thread-catchup");
const EPOCH_PREFIX8 = EPOCH32.subarray(0, 8);

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

async function newRun(clock) {
  const keypair = await generateThreadKeypair();
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32: EPOCH32,
    mode: THREAD_MODE.FINE,
    clock
  });
  const link = decodeThreadLink(encodeThreadLink({
    threadSecret: publisher.threadSecret, rootPublicKey: publisher.rootPublicKey,
    epochPrefix8: EPOCH_PREFIX8,
    notAfter: Math.floor(clock() / 1000) + 7200
  }));
  return { publisher, link };
}

test("T3: a late joiner recovers the gap from another subscriber, with no host", async () => {
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const { publisher, link } = await newRun(clock);

  // Parent A is awake through the whole run and caches what it renders.
  const awake = await createThreadSubscriber({ link, epoch32: EPOCH32, clock });
  const awakeCache = createThreadCache({ clock });
  const wire = [];
  const publish = async record => {
    wire.push(record);
    const verdict = await awake.accept(record.bytes, { nowMillis: now });
    if (verdict.ok) awakeCache.admit(record.bytes, { openable: true, nowMillis: now });
  };

  // Four minutes of the run, one fine-mode update every 5 s.
  for (let tick = 0; tick < 48; tick++) {
    now += THREAD_CONSTANTS.THREAD_UPDATE_FINE * 1000;
    const emitted = await publisher.handleFix({
      lat: 45.5 + tick * 0.0004, lon: -76.5, speedMps: 12, nowMillis: now,
      match: { segment: `5/${tick}/0`, ratio: 0.5 }
    });
    if (emitted.published) await publish(emitted.record);
  }
  assert.equal(awake.stats.accepted, 48, "the awake parent saw the whole run");
  assert.ok(awakeCache.size >= 1);

  // Parent B's phone was asleep and missed everything. It wakes, derives
  // the same tags from the same link — no directory, no address — and
  // asks whoever is providing that rendezvous key.
  const late = await createThreadSubscriber({ link, epoch32: EPOCH32, clock });
  assert.equal(late.latest(), null, "the late joiner starts with nothing");

  const wantedTags = await late.currentTags(now);
  const lateTopics = await late.topics(now);
  const awakeTopics = await awake.topics(now);
  assert.deepEqual(lateTopics, awakeTopics, "both derive identical topics from the link alone");
  const lateRendezvous = (await late.rendezvousKeys(now)).map(toHex);
  const awakeRendezvous = (await awake.rendezvousKeys(now)).map(toHex);
  assert.deepEqual(lateRendezvous, awakeRendezvous, "and identical rendezvous keys");

  // The request is padded to a legal size with CSPRNG decoys.
  const request = buildThreadRequest({
    epochPrefix8: EPOCH_PREFIX8,
    wanted: wantedTags.map(tag => ({ tag, sinceSeq: 0 })),
    rng: lcg(7)
  });
  const decodedRequest = decodeThreadRequest(request.bytes);
  assert.equal(decodedRequest.entries.length, 4, "3 real tags padded to the 4-tag batch");
  assert.equal(request.realCount, 3);

  // Parent A answers — it is a peer, not a server.
  const answered = awakeCache.answer(decodedRequest, { nowMillis: now });
  const response = encodeThreadCacheResponse(EPOCH_PREFIX8, answered);
  const accepted = await applyThreadResponse(late, response, { nowMillis: now, wantedTags });
  assert.ok(accepted > 0, `the late joiner recovered ${accepted} updates from another subscriber`);
  assert.ok(late.latest(), "and now has a position");
  assert.equal(late.latest().seq, awake.latest().seq, "caught all the way up to the awake parent");
  assert.equal(late.stats.forgeries, 0, "nothing the relay handed over was forged");

  // The relaying peer never had to be trusted: the records it passed on
  // are byte-identical to what the publisher signed. (A run crossing a
  // 5-minute boundary is cached under more than one tag, so look across
  // every tag the relay holds.)
  const cachedBytes = new Set(
    wantedTags.flatMap(tag => awakeCache.recordsFor(tag)).map(bytes => toHex(bytes))
  );
  const original = toHex(wire[wire.length - 1].bytes);
  assert.ok(cachedBytes.has(original), "records travel verbatim through the relay");
});

test("T3: a responder cannot be probed for which threads it follows", async () => {
  const now = Math.floor(Date.now() / 1000) * 1000;
  const cache = createThreadCache({ clock: () => now });
  const { publisher, link } = await newRun(() => now);
  const subscriber = await createThreadSubscriber({ link, epoch32: EPOCH32, clock: () => now });
  const emitted = await publisher.handleFix({
    lat: 45.5, lon: -76.5, speedMps: 10, nowMillis: now, match: { segment: "5/1/0", ratio: 0.2 }
  });
  cache.admit(emitted.record.bytes, { openable: true, nowMillis: now });

  const held = (await subscriber.currentTags(now))[1];
  const strangerTag = new Uint8Array(8).fill(0xab);
  const request = decodeThreadRequest(buildThreadRequest({
    epochPrefix8: EPOCH_PREFIX8,
    wanted: [
      { tag: strangerTag, sinceGeneration: 0, sinceSeq: 0 },
      { tag: held, sinceGeneration: 1, sinceSeq: 999 }
    ],
    rng: lcg(3)
  }).bytes);
  const answered = cache.answer(request, { nowMillis: now });

  // A tag the peer does not hold and a tag it holds with nothing newer
  // are answered identically: an empty record list.
  const forStranger = answered.entries.find(entry => toHex(entry.tag) === toHex(strangerTag));
  const forHeld = answered.entries.find(entry => toHex(entry.tag) === toHex(held));
  assert.deepEqual(forStranger.records, [], "unknown tag: empty");
  assert.deepEqual(forHeld.records, [], "held tag with nothing newer: also empty");
  assert.equal(answered.entries.length, 4, "and every tag in the padded batch is answered");
});

test("T3: catch-up cursors cannot skip a rotated generation whose sequence restarted", () => {
  const now = Math.floor(Date.now() / 1000) * 1000;
  const cache = createThreadCache({ clock: () => now });
  const tag = new Uint8Array(8).fill(0x41);
  const makeRecord = (generation, seq) => encodeThreadRecord({
    epochPrefix8: EPOCH_PREFIX8,
    tag,
    generation,
    seq,
    previousHash: new Uint8Array(16),
    ciphertext: new Uint8Array(28),
    admissionTag: new Uint8Array(16)
  });

  assert.ok(cache.admit(makeRecord(1, 50).bytes, { nowMillis: now }).admitted);
  assert.ok(cache.admit(makeRecord(2, 1).bytes, { nowMillis: now }).admitted,
    "generation and sequence together form the cache identity");

  const answered = cache.answer({
    entries: [{ tag, sinceGeneration: 1, sinceSeq: 50 }]
  }, { nowMillis: now });
  assert.deepEqual(
    answered.entries[0].records.map(bytes => {
      const record = decodeThreadRecord(bytes);
      return [record.generation, record.seq];
    }),
    [[2, 1]],
    "a newer generation compares after the complete older cursor even when its sequence is lower"
  );
});

test("T3: relay caching is bounded, and its own threads are never evicted", async () => {
  let now = Math.floor(Date.now() / 1000) * 1000;
  const constants = { ...THREAD_CONSTANTS, THREAD_CACHE_TAGS: 8, THREAD_TAG_BUDGET: 4, THREAD_CACHE_RING: 3 };
  const cache = createThreadCache({ constants, clock: () => now });
  const { publisher, link } = await newRun(() => now);
  const subscriber = await createThreadSubscriber({ link, epoch32: EPOCH32, clock: () => now });

  // Our own thread: admitted as openable, exempt from the relay budget.
  const mine = await publisher.handleFix({
    lat: 45.5, lon: -76.5, speedMps: 10, nowMillis: now, match: { segment: "5/1/0", ratio: 0.1 }
  });
  assert.ok(cache.admit(mine.record.bytes, { openable: true, nowMillis: now }).admitted);
  const myTag = mine.record.tag;

  // A hostile peer inventing tags it knows we cannot verify.
  const { encodeThreadRecord } = await import("../src/pulsemesh/thread_codec.js");
  let rejectedForBudget = 0;
  for (let i = 0; i < 20; i++) {
    const junk = encodeThreadRecord({
      epochPrefix8: EPOCH_PREFIX8,
      tag: Uint8Array.from({ length: 8 }, () => i + 1),
      generation: 1,
      seq: 1,
      previousHash: new Uint8Array(16),
      ciphertext: new Uint8Array(48),
      admissionTag: new Uint8Array(16)
    });
    const verdict = cache.admit(junk.bytes, { fromPeer: "flooder", nowMillis: now });
    if (!verdict.admitted && verdict.reason === "tag-budget") rejectedForBudget++;
  }
  assert.ok(rejectedForBudget > 0, "new tags per source peer are budgeted");
  assert.ok(cache.size <= constants.THREAD_CACHE_TAGS + 1, `LRU keeps the cache bounded (${cache.size})`);
  assert.ok(cache.has(myTag), "and never evicts a thread we actually follow");

  // Per-tag admission rate.
  const same = Uint8Array.from({ length: 8 }, () => 0x55);
  const junkRecord = seq => encodeThreadRecord({
    epochPrefix8: EPOCH_PREFIX8, tag: same, generation: 1, seq,
    previousHash: new Uint8Array(16), ciphertext: new Uint8Array(48), admissionTag: new Uint8Array(16)
  });
  const first = junkRecord(1);
  const second = junkRecord(2);
  cache.admit(first.bytes, { fromPeer: "peer-b", nowMillis: now });
  const tooSoon = cache.admit(second.bytes, { fromPeer: "peer-b", nowMillis: now + 100 });
  assert.equal(tooSoon.reason, "rate", "per-tag admission is rate limited");

  // TTL sweep clears relay-held records.
  now += (constants.THREAD_CACHE_TTL + 10) * 1000;
  cache.sweep(now);
  assert.equal(cache.recordsFor(same).length, 0, "relayed records expire");
  void subscriber;
});

test("T3: tags rotate, and a stale-window tag stops being addressable", async () => {
  const base = Math.floor(Date.now() / 1000) * 1000;
  const { publisher, link } = await newRun(() => base);
  const subscriber = await createThreadSubscriber({ link, epoch32: EPOCH32, clock: () => base });
  const window = threadWindow(base);

  const tagNow = await threadTag(publisher.keys, EPOCH32, window);
  const tagLater = await threadTag(publisher.keys, EPOCH32, window + 12); // an hour on
  assert.notEqual(toHex(tagNow), toHex(tagLater), "an observer cannot follow the thread by address");

  // The subscriber listens on the current window and its neighbours, so a
  // record published across a rotation boundary still lands.
  const tags = (await subscriber.currentTags(base)).map(toHex);
  assert.ok(tags.includes(toHex(tagNow)));
  assert.ok(tags.includes(toHex(await threadTag(publisher.keys, EPOCH32, window + 1))));
  assert.ok(!tags.includes(toHex(tagLater)), "but not an hour ahead");

  // The topic and rendezvous key are pure functions of the tag: no zone,
  // no route number, no operator, nothing enumerable.
  const topic = threadTopic(toHex(EPOCH32).slice(0, 16), tagNow);
  assert.match(topic, /^\/rangefind\/pulsemesh\/1\/t\/[0-9a-f]{16}\/[0-9a-f]{16}$/);
  assert.equal(threadRendezvous(topic).length, 32);
});

test("T3: padded requests reject illegal sizes and pad every batch", () => {
  const random = lcg(11);
  for (const count of [1, 3, 4]) {
    const built = buildThreadRequest({
      epochPrefix8: EPOCH_PREFIX8,
      wanted: Array.from({ length: count }, (_, i) => ({ tag: Uint8Array.from({ length: 8 }, () => i + 1), sinceSeq: 0 })),
      rng: random
    });
    assert.equal(decodeThreadRequest(built.bytes).entries.length, 4, `${count} wanted → a 4-tag batch`);
  }
  const eight = buildThreadRequest({
    epochPrefix8: EPOCH_PREFIX8,
    wanted: Array.from({ length: 6 }, (_, i) => ({ tag: Uint8Array.from({ length: 8 }, () => i + 1), sinceSeq: 0 })),
    rng: random
  });
  assert.equal(decodeThreadRequest(eight.bytes).entries.length, 8);
  assert.throws(() => buildThreadRequest({ epochPrefix8: EPOCH_PREFIX8, wanted: [], rng: random }), /at least one/);
  assert.throws(
    () => buildThreadRequest({
      epochPrefix8: EPOCH_PREFIX8,
      wanted: Array.from({ length: 17 }, () => ({ tag: new Uint8Array(8), sinceSeq: 0 })),
      rng: random
    }),
    /at most 16/
  );
});
