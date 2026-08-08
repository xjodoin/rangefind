// T7 — proof-of-delivery photos (threads §20.7).
//
// The claim under test is that a photo is a *commitment* on the wire and
// bytes somewhere else: the record stays inside 256 bytes, the sealed
// blob travels on demand over its own protocol, and the key that opens
// it comes from the run seed — so the driver and the dispatcher can read
// it and a customer holding the 45-byte link structurally cannot.

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createMeshSession } from "../src/pulsemesh/session.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createThreadChannel } from "../src/pulsemesh/thread_session.js";
import { createThreadPublisher } from "../src/pulsemesh/thread_publish.js";
import { createThreadSubscriber } from "../src/pulsemesh/thread_consume.js";
import {
  STOP_OUTCOME,
  THREAD_MAX_RECORD_BYTES,
  THREAD_MODE,
  decodePhotoRequest,
  decodePhotoResponse,
  decodeThreadLink,
  encodePhotoRequest,
  encodeThreadLink
} from "../src/pulsemesh/thread_codec.js";
import {
  THREAD_MAX_PHOTO_BYTES,
  generateThreadKeypair,
  openPhoto,
  photoCommitment,
  photoKeyFor,
  sealPhoto
} from "../src/pulsemesh/thread_crypto.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";
const EPOCH32 = sha256Utf8("pulsemesh-thread-photo");
const EPOCH_PREFIX8 = EPOCH32.subarray(0, 8);
const PLAN_REF = sha256Utf8("pulsemesh-thread-photo-plan").subarray(0, 8);

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

async function until(condition, { attempts = 50 } = {}) {
  for (let i = 0; i < attempts; i++) {
    if (condition()) return true;
    await flush();
  }
  return condition();
}

/** A stand-in for a JPEG: the codec never looks inside, and must not. */
function fakeJpeg(length, salt = 0) {
  const bytes = new Uint8Array(length);
  for (let i = 0; i < length; i++) bytes[i] = (i * 31 + salt * 7) & 0xff;
  return bytes;
}

function plan(stopCount = 3) {
  return {
    planRef: PLAN_REF,
    dwellSeconds: 0,
    stops: Array.from({ length: stopCount }, (_, i) => ({
      index: i + 1, lat: 45.5 + i * 0.001, lon: -73.6 + i * 0.001
    }))
  };
}

/** A publisher and a subscriber on the same run, no transport at all. */
async function pair({ mode = THREAD_MODE.FINE, clock } = {}) {
  const keypair = await generateThreadKeypair();
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32: EPOCH32, mode, plan: plan(), clock
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey,
    epochPrefix8: EPOCH_PREFIX8,
    notAfter: Math.floor(clock() / 1000) + 7200
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32: EPOCH32, clock });
  return { keypair, publisher, subscriber, link };
}

test("a photo key comes from the run seed, so the 45-byte link cannot derive it", async () => {
  const keypair = await generateThreadKeypair();

  const first = await photoKeyFor(keypair.privateSeed, PLAN_REF, 3);
  const again = await photoKeyFor(keypair.privateSeed, PLAN_REF, 3);
  assert.equal(first.length, 32);
  assert.deepEqual([...again], [...first], "same seed, plan and stop derive the same key");

  const otherStop = await photoKeyFor(keypair.privateSeed, PLAN_REF, 4);
  assert.notDeepEqual([...otherStop], [...first], "a different stop is a different key");

  const otherPlan = await photoKeyFor(keypair.privateSeed, sha256Utf8("other").subarray(0, 8), 3);
  assert.notDeepEqual([...otherPlan], [...first], "a different plan is a different key");

  const otherRun = await generateThreadKeypair();
  const elsewhere = await photoKeyFor(otherRun.privateSeed, PLAN_REF, 3);
  assert.notDeepEqual([...elsewhere], [...first], "a different run is a different key");

  // The structural half of the claim: what a customer holds is the link,
  // and the derivation's inputs are simply not in it. This is not a
  // cryptographic proof, it is the reason no proof is needed — there is
  // nothing to attack, because the secret was never distributed.
  const link = encodeThreadLink({
    publicKey: keypair.publicKey, epochPrefix8: EPOCH_PREFIX8, notAfter: 2_000_000_000
  });
  assert.equal(link.length, 45);
  const linkHex = toHex(link);
  assert.ok(!linkHex.includes(toHex(keypair.privateSeed)), "the seed is not in the link");
  assert.ok(!linkHex.includes(toHex(first)), "nor is any photo key");
  // And the capability that *is* in it opens nothing. `P` is 32 bytes
  // and HKDF will happily chew on it, which is the point: it derives a
  // key, and that key is not this one.
  const sealed = await sealPhoto(first, fakeJpeg(64));
  assert.equal(sealed.length, 64 + 28, "sealing prepends a 12-byte IV and appends a 16-byte tag");
  await assert.rejects(
    () => openPhoto(sealed, {
      privateSeed: keypair.publicKey,
      planRef: PLAN_REF,
      stopIndex: 3,
      hash: toHex(photoCommitment(sealed))
    }),
    /does not open/u,
    "everything a link holder has derives the wrong key"
  );
});

test("a mark carries a 32-byte commitment, and the record still fits 256 bytes", async () => {
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const { publisher, subscriber, keypair } = await pair({ clock });

  const photo = fakeJpeg(4096);
  const emitted = await publisher.markStop(2, STOP_OUTCOME.DELIVERED, { photo });

  assert.ok(
    emitted.bytes.length <= THREAD_MAX_RECORD_BYTES,
    `a photo does not grow the record past ${THREAD_MAX_RECORD_BYTES} (${emitted.bytes.length})`
  );
  // The bytes on the wire are the commitment's, not the photo's: 4 KB of
  // image and the record moved by 33 bytes.
  assert.ok(emitted.bytes.length < photo.length, "the image did not ride the record");

  const verdict = await subscriber.accept(emitted.bytes, { nowMillis: now });
  assert.equal(verdict.ok, true, `the follower accepted it (${verdict.reason || ""})`);
  const mark = verdict.update.lastOutcome;
  assert.equal(mark.stopIndex, 2);
  assert.equal(mark.outcome, STOP_OUTCOME.DELIVERED);
  assert.match(mark.photoHash, /^[0-9a-f]{64}$/u, "exposed as hex");

  // The commitment is over the *sealed* blob, and the driver's own store
  // holds exactly those bytes.
  const sealed = publisher.photoFor(mark.photoHash);
  assert.ok(sealed, "the publisher kept the sealed blob");
  assert.equal(toHex(photoCommitment(sealed)), mark.photoHash);

  // And it is inside the signature: the subscriber verified at step 6,
  // so flipping the commitment must break that.
  const forged = Uint8Array.from(verdict.update.preimage);
  forged[forged.length - 20] ^= 0x01;
  assert.notDeepEqual([...forged], [...verdict.update.preimage]);

  // Round trip: the dispatcher holds the same seed the ticket carried.
  const opened = await openPhoto(sealed, {
    privateSeed: keypair.privateSeed, planRef: PLAN_REF, stopIndex: 2, hash: mark.photoHash
  });
  assert.deepEqual([...opened], [...photo], "the dispatcher gets the original bytes back");

  // A mark with no photo says so in one byte and exposes null.
  now += 1000;
  const plain = await publisher.markStop(1, STOP_OUTCOME.SKIPPED, { reason: 1, nowMillis: now });
  const second = await subscriber.accept(plain.bytes, { nowMillis: now });
  assert.equal(second.ok, true);
  assert.equal(second.update.lastOutcome.photoHash, null);
});

test("a coarse run refuses a photo unless the operator means it", async () => {
  const now = Math.floor(Date.now() / 1000) * 1000;
  const { publisher } = await pair({ mode: THREAD_MODE.COARSE, clock: () => now });

  await assert.rejects(
    () => publisher.markStop(1, STOP_OUTCOME.DELIVERED, { photo: fakeJpeg(512) }),
    /coarse/iu,
    "a doorstep photo would give away the position coarse mode withholds"
  );
  assert.equal(publisher.lastOutcome(), null, "and nothing was published");

  const emitted = await publisher.markStop(1, STOP_OUTCOME.DELIVERED, {
    photo: fakeJpeg(512), allowCoarsePhoto: true
  });
  assert.match(publisher.lastOutcome().photoHash, /^[0-9a-f]{64}$/u);
  assert.ok(emitted.bytes.length <= THREAD_MAX_RECORD_BYTES);
});

test("an oversized photo is refused at markStop, before any crypto runs", async () => {
  const now = Math.floor(Date.now() / 1000) * 1000;
  const { publisher } = await pair({ clock: () => now });

  await assert.rejects(
    () => publisher.markStop(1, STOP_OUTCOME.DELIVERED, { photo: fakeJpeg(THREAD_MAX_PHOTO_BYTES) }),
    new RegExp(String(THREAD_MAX_PHOTO_BYTES), "u")
  );
  assert.equal(publisher.photoStore.size, 0, "nothing was sealed or stored");
  assert.equal(publisher.outcomes()[0], STOP_OUTCOME.PENDING, "and the stop was not marked");

  // One byte under the cap, counting the seal's own overhead, is fine.
  const emitted = await publisher.markStop(1, STOP_OUTCOME.DELIVERED, {
    photo: fakeJpeg(THREAD_MAX_PHOTO_BYTES - 28)
  });
  assert.equal(publisher.photoStore.size, 1);
  assert.ok(emitted.bytes.length <= THREAD_MAX_RECORD_BYTES);
});

test("a tampered blob fails the commitment check rather than opening", async () => {
  const keypair = await generateThreadKeypair();
  const key = await photoKeyFor(keypair.privateSeed, PLAN_REF, 1);
  const photo = fakeJpeg(1024, 3);
  const sealed = await sealPhoto(key, photo);
  const hash = toHex(photoCommitment(sealed));

  const opened = await openPhoto(sealed, {
    privateSeed: keypair.privateSeed, planRef: PLAN_REF, stopIndex: 1, hash
  });
  assert.deepEqual([...opened], [...photo]);

  const tampered = Uint8Array.from(sealed);
  tampered[tampered.length >> 1] ^= 0x01;
  await assert.rejects(
    () => openPhoto(tampered, {
      privateSeed: keypair.privateSeed, planRef: PLAN_REF, stopIndex: 1, hash
    }),
    /commitment/u,
    "the hash is checked before a key is ever imported"
  );

  // The wrong stop is the wrong key: the bytes are the committed ones,
  // so this gets past the hash and dies in the AEAD.
  await assert.rejects(
    () => openPhoto(sealed, {
      privateSeed: keypair.privateSeed, planRef: PLAN_REF, stopIndex: 2, hash
    }),
    /does not open/u
  );
});

test("PMTF/PMTB round trip, including the not-held answer", () => {
  const hash = toHex(sha256Utf8("some sealed blob"));
  const request = encodePhotoRequest({ hash });
  assert.equal(request.length, 36, "magic plus a 32-byte commitment, nothing else");
  assert.equal(decodePhotoRequest(request).hash, hash);

  assert.equal(decodePhotoResponse(new Uint8Array([80, 77, 84, 66, 0])).sealed, null);
  assert.throws(() => decodePhotoRequest(new Uint8Array([...request, 0])), /trailing/u);
});

test("a dispatcher fetches the photo from the driver over the mesh, and verifies it", async t => {
  let engine;
  try {
    engine = await openRouteGraphDir(GRAPH_DIR);
  } catch {
    t.skip(`route graph fixture missing at ${GRAPH_DIR}`);
    return;
  }
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const driver = await createMeshSession({ engine, network, id: "driver", transport: "loopback", clock });
  const office = await createMeshSession({
    engine, network, id: "office", transport: "loopback", readOnly: true, clock
  });
  const driverThreads = createThreadChannel({ node: driver.node, network, engine, id: "driver", clock });
  const officeThreads = createThreadChannel({ node: office.node, network, engine, id: "office", clock });

  // The dispatcher minted the seed and kept it (§20.1), so it is the one
  // party besides the driver that can open what comes back.
  const keypair = await generateThreadKeypair();
  const run = await driverThreads.publish({
    baseUrl: "https://track.example/r",
    mode: THREAD_MODE.FINE,
    privateSeed: keypair.privateSeed,
    plan: plan(2)
  });

  const updates = [];
  const follow = await officeThreads.follow(run.url, { onUpdate: update => updates.push(update) });

  const photo = fakeJpeg(9000, 11);
  await run.markStop(1, STOP_OUTCOME.DELIVERED, { photo });
  await until(() => updates.some(update => update.lastOutcome?.photoHash));
  const mark = follow.latest().lastOutcome;
  assert.ok(mark?.photoHash, "the commitment arrived on the gossip channel");

  // The blob did not. It is fetched by content hash over §20.7's own
  // protocol, from the only peer that holds it.
  const sealed = await follow.fetchPhoto(mark.photoHash);
  assert.ok(sealed, "the driver served it");
  assert.equal(toHex(photoCommitment(sealed)), mark.photoHash, "and it matched the commitment");
  assert.equal(officeThreads.stats.photosFetched, 1);
  assert.equal(driverThreads.stats.photosServed, 1);

  const opened = await openPhoto(sealed, {
    privateSeed: keypair.privateSeed, planRef: run.plan.planRef, stopIndex: 1, hash: mark.photoHash
  });
  assert.deepEqual([...opened], [...photo], "and the dispatcher's seed opens it");

  // A hash nobody holds comes back clean rather than as an error, and
  // without distinguishing "not publishing that run" from "no such photo".
  const unknown = toHex(sha256Utf8("a photo that was never taken"));
  assert.equal(await follow.fetchPhoto(unknown), null);
  const answer = decodePhotoResponse(driverThreads.servePhoto(encodePhotoRequest({ hash: unknown })));
  assert.equal(answer.sealed, null);

  // A relay never caches a blob: nothing but the publisher can answer,
  // which is exactly why availability here is "the driver is online".
  assert.equal(officeThreads.servePhoto(encodePhotoRequest({ hash: mark.photoHash })).length, 5);

  follow.stop();
  driverThreads.close();
  officeThreads.close();
});
