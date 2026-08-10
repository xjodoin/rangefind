// PulseMesh thread channel benchmarks (threads §15, §18).
//
//   node scripts/pulsemesh_thread_bench.mjs [--json]
//
// Measures the crypto and codec costs a phone actually pays, the
// bandwidth claims of §15, and the §18 open question the spec could not
// answer from a design document: how many concurrent subscribers a run
// needs before a late joiner reliably finds the gap. That last one
// decides whether small audiences need a cache incentive.

import { performance } from "node:perf_hooks";
import {
  deriveThreadKeys,
  generateThreadKeypair,
  openThreadBody,
  sealThreadBody,
  signThread,
  threadAdmissionTag,
  threadRecordSigningMessage,
  threadRendezvous,
  threadTag,
  threadTopic,
  threadWindow,
  verifyThread
} from "../src/pulsemesh/thread_crypto.js";
import {
  THREAD_MODE,
  THREAD_TRAVEL_MODE,
  THREAD_STATE,
  decodeThreadBody,
  decodeThreadLink,
  decodeThreadRecord,
  encodeThreadBody,
  encodeThreadBodyPreimage,
  encodeThreadLink,
  encodeThreadRecord,
  threadRecordAad
} from "../src/pulsemesh/thread_codec.js";
import { createThreadPublisher, THREAD_CONSTANTS } from "../src/pulsemesh/thread_publish.js";
import { createThreadSubscriber } from "../src/pulsemesh/thread_consume.js";
import {
  applyThreadResponse,
  buildThreadRequest,
  createThreadCache,
  encodeThreadCacheResponse
} from "../src/pulsemesh/thread_cache.js";
import { decodeThreadRequest } from "../src/pulsemesh/thread_codec.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const AS_JSON = process.argv.includes("--json");
const EPOCH32 = sha256Utf8("pulsemesh-thread-bench");
const EPOCH_PREFIX8 = EPOCH32.subarray(0, 8);
const results = {};

function report(section, rows) {
  results[section] = rows;
  if (AS_JSON) return;
  console.log(`\n## ${section}`);
  for (const [label, value] of Object.entries(rows)) console.log(`  ${label.padEnd(46)} ${value}`);
}

async function timeAsync(fn, iterations) {
  await fn(0); // warm
  const start = performance.now();
  for (let i = 0; i < iterations; i++) await fn(i);
  const elapsed = performance.now() - start;
  return { perSecond: iterations / (elapsed / 1000), msPerOp: elapsed / iterations };
}

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

const VECTOR_BODY = {
  unixSeconds: 1754265600,
  state: THREAD_STATE.EN_ROUTE,
  mode: THREAD_MODE.FINE,
  travelMode: THREAD_TRAVEL_MODE.CAR,
  leafCell: 3181,
  geomRef: 885,
  ratioQ12: 2048,
  speedBin: 7,
  stopIndex: 8,
  planRef: sha256Utf8("plan").subarray(0, 8),
  // The floor: a run with no plan still spends three bytes saying it has
  // no outcome map, no last mark and no note. A delivery round's own
  // cost is measured separately below.
  outcomes: [],
  lastOutcome: null,
  note: new Uint8Array(0)
};

/** What a delivery round adds to every record (§5.2.1). */
const OUTCOME_PLAN_SIZES = [50, 200];

// --- 1. Crypto and codec cost --------------------------------------------

{
  const keypair = await generateThreadKeypair();
  const keys = await deriveThreadKeys(keypair.threadSecret);
  const tag = await threadTag(keys, EPOCH32, threadWindow(Date.now()));
  const preimage = encodeThreadBodyPreimage(VECTOR_BODY);
  const previousHash = new Uint8Array(16);
  const aad = threadRecordAad(EPOCH_PREFIX8, tag, 1, 1, previousHash);
  const signedMessage = threadRecordSigningMessage(aad, preimage);
  const signature = await signThread(signedMessage, keypair.privateSeed);
  const body = encodeThreadBody(VECTOR_BODY, signature);
  const ciphertext = await sealThreadBody(keys, 1, aad, body);
  const admissionTag = await threadAdmissionTag(keys, aad, ciphertext);
  const record = encodeThreadRecord({
    epochPrefix8: EPOCH_PREFIX8, tag, generation: 1, seq: 1,
    previousHash, ciphertext, admissionTag
  });

  const derive = await timeAsync(() => deriveThreadKeys(keypair.threadSecret), 500);
  const tagCost = await timeAsync(i => threadTag(keys, EPOCH32, 5847552 + i), 2000);
  const sign = await timeAsync(() => signThread(signedMessage, keypair.privateSeed), 500);
  const verify = await timeAsync(() => verifyThread(signedMessage, signature, keypair.publicKey), 500);
  const seal = await timeAsync(i => sealThreadBody(keys, i + 2, aad, body), 2000);
  const open = await timeAsync(() => openThreadBody(keys, 1, aad, ciphertext), 2000);

  // The full receive path: decode, open, parse, verify — what a phone
  // does per update.
  const receive = await timeAsync(async () => {
    const decoded = decodeThreadRecord(record.bytes);
    const opened = await openThreadBody(keys, decoded.seq, decoded.aad, decoded.ciphertext);
    const parsed = decodeThreadBody(opened);
    return verifyThread(
      threadRecordSigningMessage(decoded.aad, parsed.preimage),
      parsed.signature,
      keypair.publicKey
    );
  }, 500);

  const link = encodeThreadLink({ threadSecret: keypair.threadSecret, rootPublicKey: keypair.publicKey, epochPrefix8: EPOCH_PREFIX8, notAfter: 1754294400 });
  // What a delivery round pays for carrying the whole day's outcomes in
  // every record — the price of a follower who joins at lunchtime still
  // learning that stop 7 was skipped at ten (§5.2.1).
  const outcomeCost = OUTCOME_PLAN_SIZES.map(stops => {
    const withPlan = encodeThreadBodyPreimage({
      ...VECTOR_BODY,
      outcomes: new Array(stops).fill(1),
      lastOutcome: { stopIndex: stops, outcome: 2, reasonCode: 1 }
    });
    return `${stops} stops: +${withPlan.length - preimage.length} B`;
  }).join(", ");
  report("crypto and codec", {
    "PMT1 record size (fine mode, no plan)": `${record.bytes.length} bytes`,
    "PMTP body size": `${body.length} bytes (${preimage.length}-byte preimage + 64-byte signature)`,
    "cumulative outcome map, per record": outcomeCost,
    "link size": `${link.length} bytes`,
    "key schedule from the capability": `${derive.msPerOp.toFixed(3)} ms (once per thread)`,
    "topic tag (HMAC)": `${Math.round(tagCost.perSecond).toLocaleString()} /s`,
    "sign (publisher, per update)": `${sign.msPerOp.toFixed(3)} ms`,
    "verify (subscriber, per update)": `${verify.msPerOp.toFixed(3)} ms`,
    "seal (AES-256-GCM)": `${Math.round(seal.perSecond).toLocaleString()} /s`,
    "open (AES-256-GCM)": `${Math.round(open.perSecond).toLocaleString()} /s`,
    "full receive path (decode+open+parse+verify)": `${receive.msPerOp.toFixed(3)} ms → ${Math.round(receive.perSecond).toLocaleString()} updates/s`
  });
}

// --- 2. Validation cost of a hostile flood --------------------------------

{
  // §7: an unknown tag is dropped at step 3, before any crypto. That is
  // the entire cost of a flood from someone without the capability, and
  // it is the number that decides whether a phone can be swamped.
  const keypair = await generateThreadKeypair();
  const link = decodeThreadLink(encodeThreadLink({
    threadSecret: keypair.threadSecret, rootPublicKey: keypair.publicKey, epochPrefix8: EPOCH_PREFIX8,
    notAfter: Math.floor(Date.now() / 1000) + 3600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32: EPOCH32 });
  const knownTags = await subscriber.currentTags();

  const junk = [];
  for (let i = 0; i < 256; i++) {
    junk.push(encodeThreadRecord({
      epochPrefix8: EPOCH_PREFIX8,
      tag: Uint8Array.from({ length: 8 }, () => (i * 31 + 7) & 0xff),
      generation: 1,
      seq: i + 1,
      previousHash: new Uint8Array(16),
      ciphertext: new Uint8Array(108),
      admissionTag: new Uint8Array(16)
    }).bytes);
  }
  const flood = await timeAsync(i => subscriber.accept(junk[i % junk.length], { knownTags }), 20000);
  report("hostile flood (§7 step 3)", {
    "records with an unguessable tag rejected": `${Math.round(flood.perSecond).toLocaleString()} /s on one core`,
    "crypto performed on them": "none — the tag check precedes the AEAD",
    "what an attacker needs to get past it": "the capability itself, which is the thing they do not have"
  });
}

// --- 3. Bandwidth, against the §15 claims ---------------------------------

{
  const keypair = await generateThreadKeypair();
  let now = Math.floor(Date.now() / 1000) * 1000;
  const fineEmitted = [];
  const finePublisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32: EPOCH32, mode: THREAD_MODE.FINE,
    clock: () => now, publish: async record => fineEmitted.push(record)
  });
  for (let i = 0; i < 120; i++) {
    now += THREAD_CONSTANTS.THREAD_UPDATE_FINE * 1000;
    await finePublisher.handleFix({
      lat: 45.5 + i * 0.0002, lon: -76.5, speedMps: 12, nowMillis: now,
      match: { segment: `3181/${400 + i}/1`, ratio: 0.5 }
    });
  }
  const fineBytes = fineEmitted.reduce((sum, record) => sum + record.bytes.length, 0);
  const fineMinutes = (120 * THREAD_CONSTANTS.THREAD_UPDATE_FINE) / 60;

  // Coarse: stop events plus a heartbeat, over the same wall-clock.
  const coarseEmitted = [];
  const stops = Array.from({ length: 12 }, (_, i) => ({ index: i + 1, lat: 45.5 + i * 0.002, lon: -76.5 }));
  let coarseNow = Math.floor(Date.now() / 1000) * 1000;
  const coarsePublisher = await createThreadPublisher({
    privateSeed: (await generateThreadKeypair()).privateSeed, epoch32: EPOCH32,
    mode: THREAD_MODE.COARSE, plan: { planRef: new Uint8Array(8), stops, dwellSeconds: 20 },
    clock: () => coarseNow, publish: async record => coarseEmitted.push(record)
  });
  for (let i = 0; i < 120; i++) {
    coarseNow += THREAD_CONSTANTS.THREAD_UPDATE_FINE * 1000;
    const stop = stops[Math.floor(i / 10)];
    const atStop = i % 10 === 0;
    await coarsePublisher.handleFix({
      lat: atStop ? stop.lat : stop.lat + 0.001, lon: -76.5,
      speedMps: atStop ? 0 : 12, nowMillis: coarseNow
    });
  }
  const coarseBytes = coarseEmitted.reduce((sum, record) => sum + record.bytes.length, 0);

  const perThreadHourFine = (fineBytes / fineMinutes) * 60;
  report("bandwidth (§15)", {
    "fine mode, per thread": `${(fineBytes / fineMinutes / 60).toFixed(1)} B/s → ${(perThreadHourFine / 1024).toFixed(0)} KB/thread-hour`,
    "coarse mode, same wall-clock": `${coarseEmitted.length} updates vs ${fineEmitted.length} (${(coarseBytes / Math.max(1, fineBytes) * 100).toFixed(1)}% of the bytes)`,
    "500-bus fleet, one morning hour (fine)": `${(perThreadHourFine * 500 / 1024 / 1024).toFixed(0)} MB across the whole mesh`,
    "500-bus fleet, one morning hour (coarse)": `${(perThreadHourFine * 500 * (coarseBytes / Math.max(1, fineBytes)) / 1024 / 1024).toFixed(1)} MB`,
    "subscriber catch-up cache per thread": `${(THREAD_CONSTANTS.THREAD_CACHE_RING * (fineEmitted[0]?.bytes.length || 130) / 1024).toFixed(0)} KiB`,
    "relay at THREAD_CACHE_TAGS capacity": `${(THREAD_CONSTANTS.THREAD_CACHE_TAGS * THREAD_CONSTANTS.THREAD_CACHE_RING * (fineEmitted[0]?.bytes.length || 320) / 1024 / 1024).toFixed(1)} MB`
  });
}

// --- 4. §18: catch-up availability vs audience size -----------------------

{
  // The spec's first open question. Model a school run whose subscribers
  // sleep and wake independently: each is awake with probability p at any
  // moment, and a late joiner can only recover what *somebody* was awake
  // to cache. Every peer here runs the real cache and the real validator.
  const rows = [];
  const random = lcg(90210);

  const UPDATES = 48;                 // four minutes of fine-mode run
  const AWAKE_PROBABILITY = 0.25;     // a phone in a pocket, screen off
  const TRIALS = 40;
  // §7 step 8 caps what is recoverable at all: an update older than
  // THREAD_MAX_AGE fails validation no matter who cached it. So catch-up
  // means "the last two minutes", never "the whole run", and the honest
  // denominator is the recoverable window rather than the run length.
  const recoverable = Math.floor(THREAD_CONSTANTS.THREAD_MAX_AGE / THREAD_CONSTANTS.THREAD_UPDATE_FINE);

  for (const { audience, PEERS_ASKED } of [
    { audience: 1, PEERS_ASKED: 1 }, { audience: 2, PEERS_ASKED: 1 },
    { audience: 5, PEERS_ASKED: 1 }, { audience: 10, PEERS_ASKED: 1 },
    { audience: 30, PEERS_ASKED: 1 }, { audience: 100, PEERS_ASKED: 1 },
    { audience: 5, PEERS_ASKED: 3 }, { audience: 10, PEERS_ASKED: 3 },
    { audience: 30, PEERS_ASKED: 3 }, { audience: 100, PEERS_ASKED: 3 },
    { audience: 30, PEERS_ASKED: 8 }, { audience: 100, PEERS_ASKED: 8 }
  ]) {
    let foundAnyone = 0;
    let totalFraction = 0;
    for (let trial = 0; trial < TRIALS; trial++) {
      // A fresh run per trial: the clock must not drift past the link's
      // own expiry, which is what an earlier version of this harness did
      // — reporting 0% for every audience above one.
      let now = Math.floor(Date.now() / 1000) * 1000;
      const keypair = await generateThreadKeypair();
      const publisher = await createThreadPublisher({
        privateSeed: keypair.privateSeed, epoch32: EPOCH32, mode: THREAD_MODE.FINE, clock: () => now
      });
      const link = decodeThreadLink(encodeThreadLink({
        threadSecret: publisher.threadSecret, rootPublicKey: publisher.rootPublicKey, epochPrefix8: EPOCH_PREFIX8,
        notAfter: Math.floor(now / 1000) + 7200
      }));
      const caches = Array.from({ length: audience }, () => createThreadCache({ clock: () => now }));
      const awake = caches.map(() => random() < AWAKE_PROBABILITY);
      for (let i = 0; i < UPDATES; i++) {
        now += THREAD_CONSTANTS.THREAD_UPDATE_FINE * 1000;
        const emitted = await publisher.handleFix({
          lat: 45.5 + i * 0.0002, lon: -76.5, speedMps: 12, nowMillis: now,
          match: { segment: `3181/${500 + i}/1`, ratio: 0.5 }
        });
        if (!emitted.published) continue;
        // Each subscriber independently flips between asleep and awake.
        for (let s = 0; s < audience; s++) {
          if (random() < 0.08) awake[s] = random() < AWAKE_PROBABILITY;
          if (awake[s]) caches[s].admit(emitted.record.bytes, { openable: true, nowMillis: now });
        }
      }
      // The late joiner asks one randomly chosen peer — it has no way to
      // know which peers were awake.
      const late = await createThreadSubscriber({ link, epoch32: EPOCH32, clock: () => now });
      const wantedTags = await late.currentTags(now);
      const request = decodeThreadRequest(buildThreadRequest({
        epochPrefix8: EPOCH_PREFIX8,
        wanted: wantedTags.map(tag => ({ tag, sinceSeq: 0 })),
        rng: random
      }).bytes);
      // Asking ONE peer cannot benefit from a large audience: the odds
      // that peer happened to be awake are the same whether the run has
      // 2 subscribers or 200. §8's "availability scales with audience
      // size" only materialises if the joiner queries several providers,
      // which is the actionable half of this measurement.
      const shuffled = [...caches].sort(() => random() - 0.5);
      for (const [index, peer] of shuffled.slice(0, PEERS_ASKED).entries()) {
        const response = encodeThreadCacheResponse(EPOCH_PREFIX8, peer.answer(request, { nowMillis: now }));
        await applyThreadResponse(late, response, { nowMillis: now, wantedTags });
        void index;
      }
      const accepted = late.stats.accepted;
      if (accepted > 0) foundAnyone++;
      totalFraction += Math.min(1, accepted / recoverable);
    }
    rows.push({
      audience,
      peersAsked: PEERS_ASKED,
      foundACache: Math.round((foundAnyone / TRIALS) * 100),
      meanFractionOfRecoverableWindow: Number((totalFraction / TRIALS).toFixed(2))
    });
  }
  report("§18 catch-up availability vs audience size", {
    "recoverable window": `${THREAD_CONSTANTS.THREAD_MAX_AGE} s (${recoverable} fine-mode updates) — older records fail §7 step 8 regardless of who cached them`,
    ...Object.fromEntries(rows.map(row => [
      `audience ${String(row.audience).padStart(3)}, ${row.peersAsked} peer(s) asked`,
      `found a cache ${String(row.foundACache).padStart(3)}% of the time, recovering ${row.meanFractionOfRecoverableWindow} of that window`
    ]))
  });
  results.catchupDetail = rows;
}

if (AS_JSON) console.log(JSON.stringify(results, null, 2));
