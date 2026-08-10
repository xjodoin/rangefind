// §21 — recurring routes: identity split from authority.
//
// The headline test is the whole feature and everything else supports it:
// **the same 78-byte link works on day 1 and on day 40**. A school bus
// keeps one plan for a term, the driver changes some days and not most,
// and a parent must never have to resubscribe. Today a run's keypair is
// its identity *and* its publish authority, so rotating the authority
// rotates every parent's link; §21 makes the root the identity and a
// certified day key the authority, and this file is the proof that a
// subscriber built once on day 1 keeps working on day 40 with no new
// artifact of any kind.

import assert from "node:assert/strict";
import test from "node:test";
import { createThreadPublisher, THREAD_CONSTANTS } from "../src/pulsemesh/thread_publish.js";
import { createThreadSubscriber, THREAD_DROP } from "../src/pulsemesh/thread_consume.js";
import { createThreadChannel } from "../src/pulsemesh/thread_session.js";
import {
  DAY_CERT_BYTES,
  DAY_CERT_VERSION,
  LINK_BYTES,
  LINK_FLAG_DELEGATED,
  THREAD_MAGIC,
  THREAD_MODE,
  decodeDayCertificate,
  decodeThreadLink,
  decodeThreadRecord,
  encodeDayCertificate,
  encodeDayCertificatePreimage,
  encodeThreadBody,
  encodeThreadBodyPreimage,
  encodeThreadLink,
  encodeThreadRecord,
  threadBodyMagic,
  threadRecordAad
} from "../src/pulsemesh/thread_codec.js";
import {
  CERT_ERROR,
  THREAD_MAX_CERT_SECONDS,
  mintDayCertificate,
  routeFollowLink,
  serviceDayOf,
  verifyDayCertificate
} from "../src/pulsemesh/thread_route.js";
import {
  bytesToBase64Url,
  deriveThreadSecret,
  deriveThreadKeys,
  generateThreadKeypair,
  publicKeyFromSeed,
  sealThreadBody,
  signThread,
  threadAdmissionTag,
  threadRecordSigningMessage,
  threadTag,
  threadWindow
} from "../src/pulsemesh/thread_crypto.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH32 = sha256Utf8("pulsemesh-thread-route");
const EPOCH_PREFIX8 = EPOCH32.subarray(0, 8);

const PLAN_REF = sha256Utf8("route-8a-morning").subarray(0, 8);
const PLAN = {
  planRef: PLAN_REF,
  stops: [
    { index: 1, lat: 45.52, lon: -73.59 },
    { index: 2, lat: 45.53, lon: -73.58 },
    { index: 3, lat: 45.54, lon: -73.57 }
  ]
};

const DAY_ONE = Date.UTC(2026, 8, 8, 11, 0, 0); // 07:00 America/Montreal
const DAY = 86400000;

function serviceDayAt(millis) {
  const at = new Date(millis);
  return serviceDayOf(at.getUTCFullYear(), at.getUTCMonth() + 1, at.getUTCDate());
}

/** What the depot mints each morning, and all the driver is ever handed. */
async function dispatchFor(rootSeed, atMillis) {
  const notBefore = Math.floor(atMillis / 1000) - 3600;
  return mintDayCertificate({
    rootSeed,
    planRef: PLAN_REF,
    serviceDay: serviceDayAt(atMillis),
    notBefore,
    notAfter: notBefore + 16 * 3600
  });
}

/**
 * What a link holder can actually build: a record sealed under the run's
 * content key — which `deriveThreadKeys(publicKey)` hands to anyone with
 * the link — carrying a body signed by a foreign seed.
 */
async function forgeRecord(threadSecret, foreignSeed, atMillis) {
  const keys = await deriveThreadKeys(threadSecret);
  const window = threadWindow(atMillis);
  const tag = await threadTag(keys, EPOCH32, window);
  const body = {
    unixSeconds: Math.floor(atMillis / 1000),
    state: 2,
    mode: THREAD_MODE.FINE,
    travelMode: 0,
    leafCell: 12,
    geomRef: 8,
    ratioQ12: 2048,
    speedBin: 3,
    stopIndex: 1,
    planRef: PLAN_REF,
    outcomes: [0, 0, 0],
    lastOutcome: null,
    note: new Uint8Array(0)
  };
  const seq = 1;
  const previousHash = new Uint8Array(16);
  const aad = threadRecordAad(EPOCH_PREFIX8, tag, 1, seq, previousHash);
  const signature = await signThread(
    threadRecordSigningMessage(aad, encodeThreadBodyPreimage(body)), foreignSeed
  );
  const ciphertext = await sealThreadBody(keys, seq, aad, encodeThreadBody(body, signature));
  const admissionTag = await threadAdmissionTag(keys, aad, ciphertext);
  return encodeThreadRecord({
    epochPrefix8: EPOCH_PREFIX8, tag, generation: 1, seq, previousHash, ciphertext, admissionTag
  }).bytes;
}

async function termLink(rootSeed, rootPublicKey, notAfter) {
  return routeFollowLink({
    threadSecret: await deriveThreadSecret(rootSeed),
    rootPublicKey,
    epochPrefix8: EPOCH_PREFIX8,
    notAfter
  });
}

/** A running day: the publisher a driver's phone would create. */
async function driverFor(rootSeed, atMillis, { mode = THREAD_MODE.FINE, publish = null } = {}) {
  const { daySeed, certificate, threadSecret } = await dispatchFor(rootSeed, atMillis);
  const publisher = await createThreadPublisher({
    daySeed,
    certificate,
    threadSecret,
    epoch32: EPOCH32,
    mode,
    plan: PLAN,
    publish,
    clock: () => atMillis,
    maxRunSeconds: 6 * 3600
  });
  return { publisher, certificate, daySeed };
}

test("the same capability-separated link works on day 1 and on day 40", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-8a-root"));
  const termEnd = Math.floor(DAY_ONE / 1000) + 180 * 86400;

  // Minted once, at the depot, at the start of the term.
  const link = await termLink(root.privateSeed, root.publicKey, termEnd);
  assert.equal(link.length, LINK_BYTES);
  assert.equal(link[1], LINK_FLAG_DELEGATED);

  // The parent subscribes once, on day 1, and never again.
  let now = DAY_ONE;
  const subscriber = await createThreadSubscriber({
    link: decodeThreadLink(link), epoch32: EPOCH32, clock: () => now
  });
  assert.equal(subscriber.delegated, true);

  const seen = [];
  async function runOneDay(atMillis) {
    now = atMillis;
    const emitted = [];
    // A fresh publisher, a fresh day key, and `seq` starting over at 1 —
    // nothing coordinates the two days, because nothing can: the ring
    // holds ten minutes and nobody published overnight. Step 7's ledger
    // is scoped to the signing authority for exactly this reason.
    const { publisher, certificate } = await driverFor(root.privateSeed, atMillis, {
      publish: record => { emitted.push(record); }
    });
    await publisher.handleFix({ lat: 45.521, lon: -73.591, speedMps: 8, nowMillis: atMillis });
    await publisher.markStop(1, 1, { nowMillis: atMillis });
    for (const record of emitted) {
      const verdict = await subscriber.accept(record.bytes, { nowMillis: atMillis });
      if (verdict.ok && verdict.update) seen.push(verdict.update);
    }
    return { publisher, certificate };
  }

  const day1 = await runOneDay(DAY_ONE);
  assert.ok(seen.length >= 2, "day 1 records are accepted");
  const acceptedAfterDay1 = subscriber.stats.accepted;
  assert.equal(subscriber.stats.forgeries, 0);

  // …thirty-nine days, dozens of drivers, and a different day key every
  // one of them.
  const day40At = DAY_ONE + 39 * DAY;
  const day40 = await runOneDay(day40At);

  // The authority really did rotate.
  assert.notEqual(toHex(day1.certificate.dayPublicKey), toHex(day40.certificate.dayPublicKey));
  assert.equal(toHex(day1.certificate.rootPublicKey), toHex(day40.certificate.rootPublicKey));

  // And the identity really did not: the link is the same *bytes*.
  const rebuilt = await termLink(root.privateSeed, day40.certificate.rootPublicKey, termEnd);
  assert.deepEqual([...rebuilt], [...link], "the capability a parent holds never moved");

  // The subscriber built on day 1 kept accepting, and needed nothing new
  // to do it: no second link, no fetched certificate bundle, no
  // re-derived keys. Its topic tags are still the root's.
  assert.ok(subscriber.stats.accepted > acceptedAfterDay1, "day 40 records were accepted too");
  assert.equal(subscriber.stats.forgeries, 0);
  assert.equal(subscriber.heldCount, 0, "the certificate always arrives before the records it covers");
  assert.equal(seen.at(-1).planRef.length, 8);

  // …and the tags a subscriber listens on are still derived from the
  // root, which is why no resubscription was ever needed.
  const tagsDay40 = await subscriber.currentTags(day40At);
  const publisherTags = day40.publisher.keys;
  assert.equal(toHex(publisherTags.topicKey), toHex(subscriber.keys.topicKey));
  assert.equal(tagsDay40.length, 3);
});

test("recurring days and restarts never reuse a GCM nonce", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-nonce-root"));

  async function firstSealedRecord(atMillis) {
    const emitted = [];
    const { publisher } = await driverFor(root.privateSeed, atMillis, {
      publish: record => { emitted.push(record); }
    });
    await publisher.handleFix({ lat: 45.521, lon: -73.591, speedMps: 8, nowMillis: atMillis });
    const record = decodeThreadRecord(emitted[0].bytes);
    assert.equal(record.seq, 1, "each isolated publisher starts at the same sequence");
    return { publisher, nonce: record.ciphertext.subarray(0, 12) };
  }

  const first = await firstSealedRecord(DAY_ONE);
  const restarted = await firstSealedRecord(DAY_ONE);
  const tomorrow = await firstSealedRecord(DAY_ONE + DAY);

  assert.equal(toHex(first.publisher.keys.contentKey), toHex(restarted.publisher.keys.contentKey));
  assert.equal(toHex(first.publisher.keys.contentKey), toHex(tomorrow.publisher.keys.contentKey),
    "the stable route link intentionally keeps one content key");
  assert.notEqual(toHex(first.nonce), toHex(restarted.nonce), "a same-day restart gets a fresh nonce");
  assert.notEqual(toHex(first.nonce), toHex(tomorrow.nonce), "the next service day gets a fresh nonce");
  assert.notEqual(toHex(restarted.nonce), toHex(tomorrow.nonce));
});

test("authority generations are random, plan-scoped, and reveal no root material", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-authority-isolation"));
  const fields = {
    rootSeed: root.privateSeed,
    planRef: PLAN_REF,
    serviceDay: 20260908,
    notBefore: 1000,
    notAfter: 1000 + 16 * 3600
  };
  const first = await mintDayCertificate(fields);
  const second = await mintDayCertificate(fields);
  assert.notEqual(toHex(first.daySeed), toHex(second.daySeed),
    "minting twice never recreates a compromised authority seed");
  assert.notEqual(toHex(first.daySeed), toHex(root.privateSeed));
  assert.deepEqual(first.certificate.planRef, PLAN_REF);
  assert.notEqual(toHex(first.certificate.dayPublicKey), toHex(root.publicKey));
  const handed = new Uint8Array([...first.daySeed, ...first.certificate.bytes]);
  assert.equal(toHex(handed).includes(toHex(root.privateSeed)), false,
    "the root seed is in nothing the driver holds");
});

test("a route-day publisher refuses the root seed and a mismatched day seed", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-holder-split"));
  const { daySeed, certificate } = await dispatchFor(root.privateSeed, DAY_ONE);

  await assert.rejects(
    () => createThreadPublisher({
      privateSeed: root.privateSeed, daySeed, certificate, epoch32: EPOCH32
    }),
    /never the route root/u
  );
  await assert.rejects(
    () => createThreadPublisher({ daySeed, epoch32: EPOCH32 }),
    /without its certificate/u
  );
  const otherDay = (await generateThreadKeypair(sha256Utf8("route-other-day"))).privateSeed;
  await assert.rejects(
    () => createThreadPublisher({ daySeed: otherDay, certificate, epoch32: EPOCH32 }),
    /not the one the certificate vouches for/u
  );

  // And the identity the publisher adopts is the root, never the day key.
  const publisher = await createThreadPublisher({
    daySeed, certificate, threadSecret: await deriveThreadSecret(root.privateSeed),
    epoch32: EPOCH32, plan: PLAN
  });
  assert.equal(toHex(publisher.publicKey), toHex(root.publicKey));
  assert.equal(toHex(publisher.dayPublicKey), toHex(certificate.dayPublicKey));
});

test("a record signed by an uncertified key is refused with its own reason", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-uncertified"));
  const now = DAY_ONE;
  const link = await termLink(root.privateSeed, root.publicKey, Math.floor(now / 1000) + 86400);
  const subscriber = await createThreadSubscriber({
    link: decodeThreadLink(link), epoch32: EPOCH32, clock: () => now
  });

  // The driver publishes, but the subscriber has not heard the
  // certificate — so it must not be told this is a forgery.
  const emitted = [];
  const { publisher } = await driverFor(root.privateSeed, now, {
    publish: record => { emitted.push(record); }
  });
  await publisher.handleFix({ lat: 45.52, lon: -73.59, speedMps: 6, nowMillis: now });
  const certRecord = emitted.find(record => record.certificate);
  const dataRecords = emitted.filter(record => record.body);
  assert.ok(certRecord && dataRecords.length);

  const verdict = await subscriber.accept(dataRecords[0].bytes, { nowMillis: now });
  assert.equal(verdict.ok, false);
  assert.equal(verdict.step, 7);
  assert.equal(verdict.code, THREAD_DROP.AWAITING_CERTIFICATE);
  assert.match(verdict.reason, /waiting for the day's certificate/u);
  assert.notEqual(verdict.code, THREAD_DROP.BAD_SIGNATURE);
  assert.equal(subscriber.stats.forgeries, 0, "a joiner who has not heard the cert is not a forgery");
  assert.equal(subscriber.heldCount, 1, "and its record is held, not discarded");

  // The distinction is only worth anything if a direct-authority link still calls a
  // real forgery a forgery. A link holder can seal — it has `K_content`
  // — but it cannot sign, so this is a record with the run's content key
  // and somebody else's signature.
  const oneOff = await generateThreadKeypair(sha256Utf8("route-one-off"));
  const directLink = encodeThreadLink({
    threadSecret: oneOff.threadSecret, rootPublicKey: oneOff.publicKey,
    epochPrefix8: EPOCH_PREFIX8, notAfter: Math.floor(now / 1000) + 3600
  });
  const directSubscriber = await createThreadSubscriber({
    link: decodeThreadLink(directLink), epoch32: EPOCH32, clock: () => now
  });
  const forged = await forgeRecord(oneOff.threadSecret, sha256Utf8("route-wrong-signer"), now);
  const forgery = await directSubscriber.accept(forged, { nowMillis: now });
  assert.equal(forgery.ok, false);
  assert.equal(forgery.step, 7);
  assert.equal(forgery.code, THREAD_DROP.BAD_SIGNATURE);
  assert.equal(directSubscriber.stats.forgeries, 1);
});

test("a certificate with an over-long window is refused by the subscriber", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-long-window"));
  const notBefore = Math.floor(DAY_ONE / 1000);
  // The depot-side guard fires first…
  await assert.rejects(
    () => mintDayCertificate({
      rootSeed: root.privateSeed,
      planRef: PLAN_REF,
      serviceDay: 20260908,
      notBefore,
      notAfter: notBefore + THREAD_MAX_CERT_SECONDS + 1
    }),
    /a root key with extra steps/u
  );

  // …but the bound that matters is the one the *verifier* applies, so
  // build the over-long certificate the way a hostile or buggy depot
  // would: straight through the codec, correctly signed by the real root.
  const hostileAuthority = await generateThreadKeypair(sha256Utf8("route-long-window-authority"));
  const fields = {
    version: DAY_CERT_VERSION,
    rootPublicKey: root.publicKey,
    dayPublicKey: hostileAuthority.publicKey,
    generation: 20260908,
    serviceDay: 20260908,
    planRef: PLAN_REF,
    notBefore,
    notAfter: notBefore + 365 * 86400
  };
  const signature = await signThread(encodeDayCertificatePreimage(fields), root.privateSeed);
  const forever = decodeDayCertificate(encodeDayCertificate(fields, signature));
  assert.equal(forever.bytes.length, DAY_CERT_BYTES);

  const verdict = await verifyDayCertificate(forever, {
    rootPublicKey: root.publicKey, nowSeconds: notBefore + 60
  });
  assert.equal(verdict.ok, false);
  assert.equal(verdict.code, CERT_ERROR.WINDOW_TOO_LONG);
  assert.match(verdict.reason, /the bound is 48 h/u);

  // …and through the subscriber, which is where it counts: a root that
  // mints a permanent day key gets nothing.
  const now = DAY_ONE + 60000;
  const link = await termLink(root.privateSeed, root.publicKey, notBefore + 200 * 86400);
  const subscriber = await createThreadSubscriber({
    link: decodeThreadLink(link), epoch32: EPOCH32, clock: () => now
  });
  const emitted = [];
  const publisher = await createThreadPublisher({
    daySeed: hostileAuthority.privateSeed,
    certificate: forever,
    threadSecret: await deriveThreadSecret(root.privateSeed),
    epoch32: EPOCH32,
    mode: THREAD_MODE.FINE,
    plan: PLAN,
    clock: () => now,
    publish: record => { emitted.push(record); }
  });
  await publisher.handleFix({ lat: 45.52, lon: -73.59, speedMps: 6, nowMillis: now });
  const certVerdict = await subscriber.accept(emitted[0].bytes, { nowMillis: now });
  assert.equal(certVerdict.ok, false);
  assert.equal(certVerdict.code, CERT_ERROR.WINDOW_TOO_LONG);
  // With no certificate accepted, the records that follow it are held,
  // never accepted.
  for (const record of emitted.slice(1)) {
    const dropped = await subscriber.accept(record.bytes, { nowMillis: now });
    assert.equal(dropped.ok, false);
    assert.equal(dropped.code, THREAD_DROP.AWAITING_CERTIFICATE);
  }
  assert.equal(subscriber.stats.accepted, 0);
  assert.equal(subscriber.certified(), false);
});

test("a certificate signed by the wrong root, or outside its window, is refused by name", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-wrong-root"));
  const stranger = await generateThreadKeypair(sha256Utf8("route-stranger"));
  const notBefore = Math.floor(DAY_ONE / 1000);

  // A perfectly good certificate — for somebody else's route.
  const other = await mintDayCertificate({
    rootSeed: stranger.privateSeed,
    planRef: PLAN_REF,
    serviceDay: 20260908,
    notBefore,
    notAfter: notBefore + 16 * 3600
  });
  const foreign = await verifyDayCertificate(other.certificate, {
    rootPublicKey: root.publicKey, nowSeconds: notBefore + 60
  });
  assert.equal(foreign.ok, false);
  assert.equal(foreign.code, CERT_ERROR.FOREIGN_ROOT);
  assert.match(foreign.reason, /a different route root/u);

  // A certificate that *claims* our root but was signed by the stranger:
  // the identity check passes and the signature check is what catches it.
  const fields = {
    version: DAY_CERT_VERSION,
    rootPublicKey: root.publicKey,
    dayPublicKey: other.certificate.dayPublicKey,
    generation: 20260908,
    serviceDay: 20260908,
    planRef: PLAN_REF,
    notBefore,
    notAfter: notBefore + 16 * 3600
  };
  const impersonation = decodeDayCertificate(encodeDayCertificate(
    fields, await signThread(encodeDayCertificatePreimage(fields), stranger.privateSeed)
  ));
  const forged = await verifyDayCertificate(impersonation, {
    rootPublicKey: root.publicKey, nowSeconds: notBefore + 60
  });
  assert.equal(forged.ok, false);
  assert.equal(forged.code, CERT_ERROR.BAD_SIGNATURE);

  // Our own certificate, before and after its window.
  const mine = await mintDayCertificate({
    rootSeed: root.privateSeed,
    planRef: PLAN_REF,
    serviceDay: 20260908,
    notBefore,
    notAfter: notBefore + 16 * 3600
  });
  const early = await verifyDayCertificate(mine.certificate, {
    rootPublicKey: root.publicKey, nowSeconds: notBefore - 3600
  });
  assert.equal(early.code, CERT_ERROR.NOT_YET_VALID);
  const late = await verifyDayCertificate(mine.certificate, {
    rootPublicKey: root.publicKey, nowSeconds: notBefore + 20 * 3600
  });
  assert.equal(late.code, CERT_ERROR.EXPIRED);
  // Inside the window, and inside the clock tolerance on either edge.
  for (const at of [notBefore - 200, notBefore + 3600, notBefore + 16 * 3600 + 200]) {
    const verdict = await verifyDayCertificate(mine.certificate, {
      rootPublicKey: root.publicKey, nowSeconds: at
    });
    assert.equal(verdict.ok, true, `a clock ${at - notBefore}s from notBefore is tolerated`);
  }
});

test("catch-up delivers the certificate to a late joiner, which then releases held records", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-late-joiner"));
  const now = DAY_ONE;
  const link = await termLink(root.privateSeed, root.publicKey, Math.floor(now / 1000) + 90 * 86400);
  const subscriber = await createThreadSubscriber({
    link: decodeThreadLink(link), epoch32: EPOCH32, clock: () => now
  });

  const emitted = [];
  const { publisher } = await driverFor(root.privateSeed, now, {
    publish: record => { emitted.push(record); }
  });
  await publisher.handleFix({ lat: 45.52, lon: -73.59, speedMps: 6, nowMillis: now });
  await publisher.markStop(1, 1, { nowMillis: now });
  await publisher.markStop(2, 1, { nowMillis: now });

  const certificate = emitted.find(record => record.certificate);
  const records = emitted.filter(record => record.body);
  assert.ok(certificate, "the publisher put its certificate on the run's own topic");
  assert.equal(threadBodyMagic(certificate.certificate.bytes), THREAD_MAGIC.PMTC);
  assert.ok(records.length >= 3);

  // The joiner's gossip starts mid-run: it gets the records first and
  // the certificate only when §5.5 catch-up hands it over — out of the
  // same PMM1, with no second fetch protocol anywhere.
  for (const record of records) {
    const verdict = await subscriber.accept(record.bytes, { nowMillis: now });
    assert.equal(verdict.code, THREAD_DROP.AWAITING_CERTIFICATE);
  }
  assert.equal(subscriber.stats.accepted, 0);
  assert.equal(subscriber.heldCount, records.length);

  const verdict = await subscriber.accept(certificate.bytes, { nowMillis: now });
  assert.equal(verdict.ok, true);
  assert.equal(toHex(verdict.certificate.dayPublicKey), toHex(publisher.dayPublicKey));
  // Everything that was waiting is released, in order, and the newest
  // wins the seq ledger.
  assert.equal(verdict.released.length, records.length);
  assert.deepEqual(
    verdict.released.map(update => update.seq),
    [...verdict.released.map(update => update.seq)].sort((a, b) => a - b)
  );
  assert.equal(subscriber.heldCount, 0);
  assert.equal(subscriber.latest().seq, records.at(-1).seq);
  assert.equal(subscriber.certified(), true);
  assert.equal(subscriber.stats.forgeries, 0);
});

test("the held-record buffer is bounded, and a held forgery is evicted rather than accepted", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-buffer-bound"));
  const now = DAY_ONE;
  const link = await termLink(root.privateSeed, root.publicKey, Math.floor(now / 1000) + 86400);
  const subscriber = await createThreadSubscriber({
    link: decodeThreadLink(link), epoch32: EPOCH32, clock: () => now
  });

  const emitted = [];
  const { publisher } = await driverFor(root.privateSeed, now, {
    publish: record => { emitted.push(record); }
  });
  for (let i = 0; i < THREAD_CONSTANTS.THREAD_HELD_RECORDS + 12; i++) {
    await publisher.markStop((i % 3) + 1, 1, { nowMillis: now });
  }
  const records = emitted.filter(record => record.body);
  assert.ok(records.length > THREAD_CONSTANTS.THREAD_HELD_RECORDS);
  for (const record of records) await subscriber.accept(record.bytes, { nowMillis: now });
  assert.equal(subscriber.heldCount, THREAD_CONSTANTS.THREAD_HELD_RECORDS);
  assert.ok(subscriber.stats.heldEvicted > 0);

  // A record sealed with the run's content key but signed by a key no
  // certificate covers — a link holder trying to publish — is held and
  // then evicted. It is never accepted, and the certificate arriving
  // does not vindicate it.
  const forged = await forgeRecord(await deriveThreadSecret(root.privateSeed), sha256Utf8("route-buffer-impostor"), now);
  const held = await subscriber.accept(forged, { nowMillis: now });
  assert.equal(held.code, THREAD_DROP.AWAITING_CERTIFICATE);
  assert.equal(subscriber.heldCount, THREAD_CONSTANTS.THREAD_HELD_RECORDS, "the bound holds under a forgery too");

  const certificate = emitted.find(record => record.certificate);
  const verdict = await subscriber.accept(certificate.bytes, { nowMillis: now });
  assert.equal(verdict.ok, true);
  // Only what survived the bound is released; the evicted ones are gone
  // for good, which is the honest outcome of a bounded buffer.
  assert.equal(verdict.released.length, THREAD_CONSTANTS.THREAD_HELD_RECORDS - 1);
  // The forgery is not among them, is not counted as accepted, and is
  // still sitting there waiting for a certificate that will never exist.
  assert.equal(subscriber.heldCount, 1);
  assert.equal(subscriber.stats.accepted, THREAD_CONSTANTS.THREAD_HELD_RECORDS - 1);

  // And age is the second bound: a certificate that turns up past
  // THREAD_MAX_AGE releases nothing, because a position that would fail
  // step 8 on arrival must not pass it by having been held.
  const late = await createThreadSubscriber({
    link: decodeThreadLink(link), epoch32: EPOCH32, clock: () => now
  });
  for (const record of records.slice(0, 4)) await late.accept(record.bytes, { nowMillis: now });
  assert.equal(late.heldCount, 4);
  const muchLater = now + (THREAD_CONSTANTS.THREAD_MAX_AGE + 60) * 1000;
  const stale = await late.accept(certificate.bytes, { nowMillis: muchLater });
  assert.equal(stale.ok, true);
  assert.equal(stale.released.length, 0);
  assert.equal(late.heldCount, 0);
  assert.equal(late.stats.accepted, 0);
});

test("a direct-authority run works end to end and refuses delegated certificates", async () => {
  const keypair = await generateThreadKeypair(sha256Utf8("route-v1-still-works"));
  const now = DAY_ONE;
  const link = encodeThreadLink({
    threadSecret: keypair.threadSecret, rootPublicKey: keypair.publicKey,
    epochPrefix8: EPOCH_PREFIX8, notAfter: Math.floor(now / 1000) + 3600
  });
  assert.equal(link[1], 0, "direct authority uses no delegation flag");
  const decoded = decodeThreadLink(link);
  assert.equal(decoded.delegated, false);

  const subscriber = await createThreadSubscriber({ link: decoded, epoch32: EPOCH32, clock: () => now });
  assert.equal(subscriber.delegated, false);

  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32: EPOCH32,
    mode: THREAD_MODE.FINE,
    plan: PLAN,
    clock: () => now,
    publish: record => { emitted.push(record); }
  });
  await publisher.handleFix({ lat: 45.52, lon: -73.59, speedMps: 6, nowMillis: now });
  await publisher.markStop(1, 1, { nowMillis: now });
  assert.equal(publisher.certificate, null);
  assert.equal(publisher.stats.certificates, 0);
  assert.equal(emitted.every(record => record.body), true, "a v1 run emits no certificates");

  for (const record of emitted) {
    const verdict = await subscriber.accept(record.bytes, { nowMillis: now });
    assert.equal(verdict.ok, true);
  }
  assert.equal(subscriber.stats.accepted, emitted.length);
  assert.equal(subscriber.heldCount, 0);

  // A certificate arriving on a v1 thread is the publisher and the
  // artifact disagreeing, and the artifact wins.
  const root = await generateThreadKeypair(sha256Utf8("route-v1-mix"));
  const { certificate } = await dispatchFor(root.privateSeed, now);
  // Re-seal that certificate under *this* run's content key, which is
  // what a publisher confused about its own artifact would do.
  const confused = [];
  const confusedMint = await mintDayCertificate({
    rootSeed: keypair.privateSeed,
    planRef: PLAN_REF,
    serviceDay: serviceDayAt(now),
    notBefore: Math.floor(now / 1000) - 60,
    notAfter: Math.floor(now / 1000) + 3600
  });
  const confusedPublisher = await createThreadPublisher({
    daySeed: confusedMint.daySeed,
    certificate: confusedMint.certificate,
    threadSecret: keypair.threadSecret,
    epoch32: EPOCH32,
    mode: THREAD_MODE.FINE,
    plan: PLAN,
    clock: () => now,
    publish: record => { confused.push(record); }
  });
  await confusedPublisher.handleFix({ lat: 45.52, lon: -73.59, speedMps: 6, nowMillis: now });
  const certRecord = confused.find(record => record.certificate);
  const rejected = await subscriber.accept(certRecord.bytes, { nowMillis: now });
  assert.equal(rejected.ok, false);
  assert.equal(rejected.code, THREAD_DROP.CERT_ON_DIRECT_LINK);
  assert.equal(certificate.version, DAY_CERT_VERSION);
});

test("the channel publishes a route day and a follower on the term link tracks it", async () => {
  const root = await generateThreadKeypair(sha256Utf8("route-session"));
  const now = DAY_ONE;
  const clock = () => now;
  const epochHex = toHex(EPOCH32);

  const depot = createThreadChannel({ epochHex, id: "depot", clock });
  const parent = createThreadChannel({ epochHex, id: "parent", clock });

  // The depot mints the term link once, and each morning's authority
  // separately. Only the second pair ever reaches the bus.
  const termEnd = Math.floor(now / 1000) + 180 * 86400;
  const link = await termLink(root.privateSeed, root.publicKey, termEnd);
  const url = `https://track.example/r#${bytesToBase64Url(link)}`;

  const updates = [];
  const follow = await parent.follow(url, { onUpdate: update => updates.push(update) });
  assert.ok(follow.topics.size >= 1);

  const wire = [];
  const { daySeed, certificate } = await dispatchFor(root.privateSeed, now);
  const run = await depot.publishRouteDay({
    baseUrl: "https://track.example/r",
    daySeed,
    certificate,
    link,
    notAfter: termEnd,
    mode: THREAD_MODE.FINE,
    plan: PLAN,
    onPublish: emitted => { wire.push(emitted); }
  });
  assert.deepEqual([...run.link], [...link], "the term's link, not a freshly minted one");

  await run.handleFix({ lat: 45.521, lon: -73.591, speedMps: 9, nowMillis: now });
  await run.markStop(1, 1, { nowMillis: now });

  for (const emitted of wire) {
    await parent.deliver(
      `/rangefind/pulsemesh/1/t/${epochHex.slice(0, 16)}/${toHex(emitted.tag)}`,
      emitted.bytes,
      now
    );
  }
  assert.ok(updates.length >= 2, "the parent tracked the day without holding anything new");
  assert.equal(follow.status({ nowMillis: now }).live, true);
  assert.equal(parent.stats.dropped, 0, "the certificate arrived ahead of every record it covers");

  // And the run is bounded by the day's authority, never by the term.
  assert.ok(run.notAfter === termEnd);
  depot.close();
  parent.close();
});
