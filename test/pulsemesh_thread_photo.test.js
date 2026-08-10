// T7 — proof-of-delivery photos (threads §20.7).
//
// The claim under test is that a photo is a *commitment* on the wire and
// bytes somewhere else: the record stays inside 320 bytes, the sealed
// blob travels on demand over its own protocol, and the key that opens
// it comes from the run seed — so the driver and the dispatcher can read
// it and a customer holding the 78-byte link structurally cannot.
//
// And §20.7.1: the commitments survive a dead zone. One 32-byte
// accumulator in every signed record binds every commitment the run has
// published, so a holder that heard none of an outage recovers the lot
// from the publisher's list and checks it against a value the publisher
// signed at the door. The tests below are the two halves of that — the
// recovery, and the four ways a publisher could lie about the list and
// be caught.

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
  THREAD_MAX_PHOTO_LIST,
  THREAD_MAX_RECORD_BYTES,
  THREAD_RECORD_OVERHEAD,
  THREAD_MODE,
  decodePhotoListRequest,
  decodePhotoListResponse,
  decodePhotoRequest,
  decodePhotoResponse,
  decodeThreadBody,
  decodeThreadLink,
  encodePhotoListRequest,
  encodePhotoListResponse,
  encodePhotoRequest,
  encodeThreadBodyPreimage,
  encodeThreadLink
} from "../src/pulsemesh/thread_codec.js";
import {
  PHOTO_CHAIN_ZERO,
  THREAD_MAX_PHOTO_BYTES,
  generateThreadKeypair,
  openPhoto,
  photoChainOf,
  photoCommitment,
  photoKeyFor,
  sealPhoto,
  verifyPhotoChain
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
async function pair({ mode = THREAD_MODE.FINE, clock, stops = 3 } = {}) {
  const keypair = await generateThreadKeypair();
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32: EPOCH32, mode, plan: plan(stops), clock
  });
  const link = decodeThreadLink(encodeThreadLink({
    threadSecret: publisher.threadSecret, rootPublicKey: publisher.rootPublicKey,
    epochPrefix8: EPOCH_PREFIX8,
    notAfter: Math.floor(clock() / 1000) + 7200
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32: EPOCH32, clock });
  return { keypair, publisher, subscriber, link };
}

test("a photo key comes from the run seed, so the 78-byte link cannot derive it", async () => {
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
    threadSecret: keypair.threadSecret, rootPublicKey: keypair.publicKey, epochPrefix8: EPOCH_PREFIX8, notAfter: 2_000_000_000
  });
  assert.equal(link.length, 78);
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

test("a mark carries a signed accumulator, and the record still fits 320 bytes", async () => {
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const { publisher, subscriber, keypair } = await pair({ clock });

  const photo = fakeJpeg(4096);
  const emitted = await publisher.markStop(2, STOP_OUTCOME.DELIVERED, { photo });

  assert.ok(
    emitted.bytes.length <= THREAD_MAX_RECORD_BYTES,
    `a photo does not grow the record past ${THREAD_MAX_RECORD_BYTES} (${emitted.bytes.length})`
  );
  // The bytes on the wire are the accumulator's, not the photo's: 4 KB of
  // image and the record moved by 33.
  assert.ok(emitted.bytes.length < photo.length, "the image did not ride the record");

  const verdict = await subscriber.accept(emitted.bytes, { nowMillis: now });
  assert.equal(verdict.ok, true, `the follower accepted it (${verdict.reason || ""})`);
  const mark = verdict.update.lastOutcome;
  assert.equal(mark.stopIndex, 2);
  assert.equal(mark.outcome, STOP_OUTCOME.DELIVERED);
  assert.equal(mark.hasPhoto, true, "the mark says a proof exists, in one signed byte");
  assert.equal(mark.photoHash, undefined, "and does not say which — that is the chain's job");
  assert.match(verdict.update.photoChain, /^[0-9a-f]{64}$/u, "the accumulator, exposed as hex");

  // §20.7.1: the head is the fold of one entry over A₀, and it binds the
  // stop as well as the commitment.
  const [entry] = publisher.photoChainEntries();
  assert.deepEqual(entry, { stopIndex: 2, commitment: publisher.lastOutcome().photoHash });
  assert.equal(verdict.update.photoChain, photoChainOf([entry]));
  assert.notEqual(
    photoChainOf([{ ...entry, stopIndex: 3 }]), verdict.update.photoChain,
    "the same blob claimed for another door is a different chain"
  );

  // The commitment is over the *sealed* blob, and the driver's own store
  // holds exactly those bytes.
  const sealed = publisher.photoFor(entry.commitment);
  assert.ok(sealed, "the publisher kept the sealed blob");
  assert.equal(toHex(photoCommitment(sealed)), entry.commitment);

  // And the accumulator is inside the signature: the subscriber verified
  // at step 6, so flipping it must break that.
  const forged = Uint8Array.from(verdict.update.preimage);
  forged[forged.length - 20] ^= 0x01;
  assert.notDeepEqual([...forged], [...verdict.update.preimage]);

  // Round trip: the dispatcher holds the same seed the ticket carried.
  const opened = await openPhoto(sealed, {
    privateSeed: keypair.privateSeed, planRef: PLAN_REF, stopIndex: 2, hash: entry.commitment
  });
  assert.deepEqual([...opened], [...photo], "the dispatcher gets the original bytes back");

  // A mark with no photo says so in one byte — and the accumulator does
  // not move, because nothing was committed to.
  now += 1000;
  const plain = await publisher.markStop(1, STOP_OUTCOME.SKIPPED, { reason: 1, nowMillis: now });
  const second = await subscriber.accept(plain.bytes, { nowMillis: now });
  assert.equal(second.ok, true);
  assert.equal(second.update.lastOutcome.hasPhoto, false);
  assert.equal(second.update.photoChain, verdict.update.photoChain,
    "every record re-states the head, marked or not — that is what makes it recoverable");
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

test("PMTL/PMTA round trip, the empty answer, and the caps", () => {
  const accumulator = toHex(sha256Utf8("a run's photo chain"));
  const request = encodePhotoListRequest({ accumulator });
  assert.equal(request.length, 36, "magic plus a 32-byte accumulator, nothing else");
  assert.equal(decodePhotoListRequest(request).accumulator, accumulator);

  // A₀ names every run at once and no run in particular, so it is not a
  // question this protocol will ask or answer.
  assert.throws(() => encodePhotoListRequest({ accumulator: PHOTO_CHAIN_ZERO }), /non-zero/u);
  assert.throws(
    () => decodePhotoListRequest(Uint8Array.from([...request.subarray(0, 4), ...new Uint8Array(32)])),
    /empty accumulator/u
  );

  const entries = [
    { stopIndex: 2, commitment: toHex(sha256Utf8("one")) },
    { stopIndex: 137, commitment: toHex(sha256Utf8("two")) }
  ];
  const response = encodePhotoListResponse({ entries });
  assert.equal(response.length, 4 + 1 + (1 + 32) + (2 + 32), "a varint stop index and 32 bytes each");
  assert.deepEqual(decodePhotoListResponse(response).entries, entries);
  // Order is the chain's order and the encoder does not touch it: a
  // responder that sorted this would produce a list verifying against
  // nothing.
  assert.deepEqual(decodePhotoListResponse(encodePhotoListResponse({
    entries: [...entries].reverse()
  })).entries, [...entries].reverse());

  // "Not held" and "no such run" are the same answer, as with PMTB.
  assert.deepEqual(decodePhotoListResponse(encodePhotoListResponse({})).entries, []);
  assert.throws(() => decodePhotoListResponse(new Uint8Array([...response, 0])), /trailing/u);

  // The cap bounds what a responder can make a holder chew on: 1024
  // entries is 34 KB, a third of one photo.
  const many = Array.from({ length: THREAD_MAX_PHOTO_LIST + 1 }, (_, i) => ({
    stopIndex: i + 1, commitment: PHOTO_CHAIN_ZERO
  }));
  assert.throws(() => encodePhotoListResponse({ entries: many }), /at most 1024/u);
  const overclaimed = Uint8Array.from([...encodePhotoListResponse({}).subarray(0, 4), 0x81, 0x10]);
  assert.throws(() => decodePhotoListResponse(overclaimed), /cap is 1024/u);
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
  await until(() => updates.some(update => update.photoChain));
  const mark = follow.latest().lastOutcome;
  assert.equal(mark?.hasPhoto, true, "the record says a proof exists");
  assert.ok(follow.latest().photoChain, "and the accumulator arrived on the gossip channel");

  // The commitment did not, and does not need to: it is recovered from
  // the publisher's list and checked against the accumulator the record
  // already carried.
  const list = await follow.fetchPhotoList();
  assert.equal(list.verified.length, 1);
  assert.equal(list.unverified.length, 0);
  assert.deepEqual(list.verified[0].stopIndex, 1);
  assert.equal(officeThreads.stats.photoListsFetched, 1);
  assert.equal(driverThreads.stats.photoListsServed, 1);
  const commitment = list.verified[0].commitment;

  // The blob is fetched by content hash over §20.7's own protocol, from
  // the only peer that holds it.
  const sealed = await follow.fetchPhoto(commitment);
  assert.ok(sealed, "the driver served it");
  assert.equal(toHex(photoCommitment(sealed)), commitment, "and it matched the commitment");
  assert.equal(officeThreads.stats.photosFetched, 1);
  assert.equal(driverThreads.stats.photosServed, 1);

  const opened = await openPhoto(sealed, {
    privateSeed: keypair.privateSeed, planRef: run.plan.planRef, stopIndex: 1, hash: commitment
  });
  assert.deepEqual([...opened], [...photo], "and the dispatcher's seed opens it");

  // A hash nobody holds comes back clean rather than as an error, and
  // without distinguishing "not publishing that run" from "no such photo".
  const unknown = toHex(sha256Utf8("a photo that was never taken"));
  assert.equal(await follow.fetchPhoto(unknown), null);
  const answer = decodePhotoResponse(driverThreads.servePhoto(encodePhotoRequest({ hash: unknown })));
  assert.equal(answer.sealed, null);

  // The same rule for a list: an accumulator this peer does not recognise
  // gets an empty answer, never "that is not my run".
  const strangeChain = toHex(sha256Utf8("some other run's accumulator"));
  assert.deepEqual(
    decodePhotoListResponse(driverThreads.servePhotoList(
      encodePhotoListRequest({ accumulator: strangeChain })
    )).entries,
    []
  );
  assert.deepEqual(await follow.fetchPhotoList(strangeChain),
    { matchedAt: 0, verified: [], unverified: [] });

  // A relay never caches a blob nor a list: nothing but the publisher can
  // answer, which is exactly why availability here is "the driver is online".
  assert.equal(officeThreads.servePhoto(encodePhotoRequest({ hash: commitment })).length, 5);
  assert.deepEqual(
    decodePhotoListResponse(officeThreads.servePhotoList(
      encodePhotoListRequest({ accumulator: follow.latest().photoChain })
    )).entries,
    []
  );

  follow.stop();
  driverThreads.close();
  officeThreads.close();
});

/**
 * A network whose behaviour a test can change mid-run: gossip can be cut,
 * and one method can be replaced to make a peer lie.
 */
function tapped(network, hooks) {
  return new Proxy(network, {
    get(target, property, receiver) {
      if (Object.hasOwn(hooks, property)) return hooks[property];
      const value = Reflect.get(target, property, receiver);
      return typeof value === "function" ? value.bind(target) : value;
    }
  });
}

test("three proofs marked in a dead zone are recovered and verified by a holder that heard none of it", async t => {
  // The headline claim of §20.7.1. Before the accumulator, two of these
  // three commitments were gone the moment the next stop was marked: the
  // photos stayed on the device and nothing could name them.
  let engine;
  try {
    engine = await openRouteGraphDir(GRAPH_DIR);
  } catch {
    t.skip(`route graph fixture missing at ${GRAPH_DIR}`);
    return;
  }
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const base = createLoopbackNetwork({ clock });
  const hooks = {};
  const network = tapped(base, hooks);

  const driver = await createMeshSession({ engine, network, id: "driver", transport: "loopback", clock });
  const office = await createMeshSession({
    engine, network, id: "office", transport: "loopback", readOnly: true, clock
  });
  const driverThreads = createThreadChannel({ node: driver.node, network, engine, id: "driver", clock });
  const officeThreads = createThreadChannel({ node: office.node, network, engine, id: "office", clock });

  const keypair = await generateThreadKeypair();
  const run = await driverThreads.publish({
    baseUrl: "https://track.example/r",
    mode: THREAD_MODE.FINE,
    privateSeed: keypair.privateSeed,
    plan: plan(6)
  });
  const updates = [];
  const follow = await officeThreads.follow(run.url, { onUpdate: update => updates.push(update) });

  // The morning is ordinary and the board is current.
  await run.markStop(1, STOP_OUTCOME.DELIVERED);
  await until(() => updates.length > 0);
  const morning = updates.length;

  // Into the dead zone: the radio takes every record and delivers none.
  hooks.publish = () => {};
  const photos = [fakeJpeg(3000, 2), fakeJpeg(3100, 3), fakeJpeg(3200, 4)];
  await run.markStop(2, STOP_OUTCOME.FAILED, { reason: 1, photo: photos[0] });
  now += 20_000;
  await run.markStop(3, STOP_OUTCOME.SKIPPED, { reason: 3, photo: photos[1] });
  now += 20_000;
  await run.markStop(4, STOP_OUTCOME.DELIVERED, { photo: photos[2] });
  now += 20_000;
  await flush();
  assert.equal(updates.length, morning, "the office heard nothing at all of the three marks");

  // Coverage returns. One ordinary fix — not a replay of anything.
  delete hooks.publish;
  await run.handleFix({ lat: 45.503, lon: -73.603, speedMps: 8 });
  await until(() => updates.length > morning);

  const update = follow.latest();
  assert.equal(update.lastOutcome.stopIndex, 4, "only the newest mark is in this record");
  assert.ok(update.photoChain, "but it carries a head over all three");

  // The recovery. The publisher restates its list; nothing here trusts
  // it — the chain is recomputed from A₀ and must reach the head that is
  // inside the signature on the record the office just accepted.
  const list = await follow.fetchPhotoList();
  assert.equal(list.matchedAt, 3);
  assert.equal(list.unverified.length, 0);
  assert.deepEqual(list.verified.map(entry => entry.stopIndex), [2, 3, 4],
    "all three commitments, at the doors they were taken at, in publication order");
  assert.equal(photoChainOf(list.verified), update.photoChain);

  // Including stop 2's, whose record reached nobody and whose mark was
  // superseded twice before coverage came back. Its blob fetches and
  // verifies against a commitment the office never received directly.
  const lost = list.verified[0];
  const sealed = await follow.fetchPhoto(lost.commitment);
  assert.ok(sealed, "the driver still holds it — the publisher evicts nothing");
  assert.equal(toHex(photoCommitment(sealed)), lost.commitment);
  const opened = await openPhoto(sealed, {
    privateSeed: keypair.privateSeed, planRef: run.plan.planRef, stopIndex: 2, hash: lost.commitment
  });
  assert.deepEqual([...opened], [...photos[0]],
    "the proof for the stop that failed in the dead zone, opened by the dispatcher");

  // The three sit inside a 256-byte record like everything else.
  assert.ok(follow.latest().preimage.length + 64 + THREAD_RECORD_OVERHEAD <= THREAD_MAX_RECORD_BYTES);

  follow.stop();
  driverThreads.close();
  officeThreads.close();
});

test("a publisher that inserts, drops, reorders or substitutes is caught by the chain", async () => {
  const now = Math.floor(Date.now() / 1000) * 1000;
  const { publisher } = await pair({ clock: () => now, stops: 6 });
  for (const [index, salt] of [[1, 21], [2, 22], [3, 23]]) {
    await publisher.markStop(index, STOP_OUTCOME.DELIVERED, { photo: fakeJpeg(600, salt) });
  }
  const honest = publisher.photoChainEntries();
  const head = publisher.photoChain();
  assert.equal(photoChainOf(honest), head);
  assert.equal(verifyPhotoChain(honest, head).matchedAt, 3);

  const stranger = { stopIndex: 2, commitment: toHex(sha256Utf8("a doorstep photographed at 18:00")) };
  const lies = {
    // The one the whole section exists to stop: a photo taken later,
    // served as the proof for a stop it was never at.
    substituted: [honest[0], stranger, honest[2]],
    inserted: [honest[0], stranger, honest[1], honest[2]],
    dropped: [honest[0], honest[2]],
    reordered: [honest[1], honest[0], honest[2]],
    // Same blobs, same order, one moved to another door — the stop index
    // is inside the hash, so this breaks too.
    redoored: [honest[0], { ...honest[1], stopIndex: 5 }, honest[2]]
  };
  for (const [what, entries] of Object.entries(lies)) {
    const checked = verifyPhotoChain(entries, head);
    assert.equal(checked.matchedAt, 0, `${what} does not reproduce the signed head`);
    assert.equal(checked.verified.length, 0, `${what} verifies nothing at all`);
    assert.equal(checked.unverified.length, entries.length,
      `${what} is returned in full and believed in no part`);
  }

  // Appending is the one edit that is not a lie: it is what taking
  // another photo does, and it leaves the signed prefix intact.
  await publisher.markStop(5, STOP_OUTCOME.SKIPPED, { reason: 2, photo: fakeJpeg(600, 24) });
  assert.equal(verifyPhotoChain(publisher.photoChainEntries(), head).matchedAt, 3,
    "the old head still vouches for the first three and says nothing about the fourth");
});

test("a liar over the wire is refused, and the honest peer after it is not", async t => {
  let engine;
  try {
    engine = await openRouteGraphDir(GRAPH_DIR);
  } catch {
    t.skip(`route graph fixture missing at ${GRAPH_DIR}`);
    return;
  }
  const now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const base = createLoopbackNetwork({ clock });
  const hooks = {};
  const network = tapped(base, hooks);
  const driver = await createMeshSession({ engine, network, id: "driver", transport: "loopback", clock });
  const office = await createMeshSession({
    engine, network, id: "office", transport: "loopback", readOnly: true, clock
  });
  const driverThreads = createThreadChannel({ node: driver.node, network, engine, id: "driver", clock });
  const officeThreads = createThreadChannel({ node: office.node, network, engine, id: "office", clock });

  const keypair = await generateThreadKeypair();
  const run = await driverThreads.publish({
    baseUrl: "https://track.example/r", mode: THREAD_MODE.FINE,
    privateSeed: keypair.privateSeed, plan: plan(3)
  });
  const follow = await officeThreads.follow(run.url);
  await run.markStop(1, STOP_OUTCOME.DELIVERED, { photo: fakeJpeg(700, 31) });
  await run.markStop(2, STOP_OUTCOME.DELIVERED, { photo: fakeJpeg(700, 32) });
  await until(() => follow.latest()?.photoChain === run.photoChain());

  // One peer answers with a list that drops the first entry — a plausible
  // list, of real commitments, that simply is not what was signed.
  const honest = run.photoChainEntries();
  const requestPhoto = base.requestPhoto.bind(base);
  const isListRequest = payload => String.fromCharCode(...payload.subarray(0, 4)) === "PMTL";
  hooks.requestPhoto = (from, to, payload) => (
    isListRequest(payload)
      ? encodePhotoListResponse({ entries: honest.slice(1) })
      : requestPhoto(from, to, payload)
  );
  assert.deepEqual(await follow.fetchPhotoList(), { matchedAt: 0, verified: [], unverified: [] });
  assert.equal(officeThreads.stats.photoListsRefused, 1,
    "counted rather than thrown: one lying peer must not end the fetch");
  assert.equal(officeThreads.stats.photoListsFetched, 0);

  // And the honest publisher, once it gets a turn, is believed.
  delete hooks.requestPhoto;
  const list = await follow.fetchPhotoList();
  assert.equal(list.matchedAt, 2);
  assert.deepEqual(list.verified.map(entry => entry.stopIndex), [1, 2]);

  follow.stop();
  driverThreads.close();
  officeThreads.close();
});

test("a stale accumulator verifies its prefix and refuses the rest by name", async () => {
  // The routine case, not the exceptional one: a holder's newest record
  // is older than the newest photo more often than not. It must neither
  // reject the list it can vouch for nor accept the part it cannot.
  let now = Math.floor(Date.now() / 1000) * 1000;
  const { publisher, subscriber } = await pair({ clock: () => now, stops: 6 });

  await publisher.markStop(1, STOP_OUTCOME.DELIVERED, { photo: fakeJpeg(800, 41) });
  const first = await publisher.markStop(2, STOP_OUTCOME.FAILED, { reason: 1, photo: fakeJpeg(800, 42) });
  const heard = await subscriber.accept(first.bytes, { nowMillis: now });
  assert.equal(heard.ok, true);
  const stale = heard.update.photoChain;

  // Two more photos the holder never hears about.
  now += 1000;
  await publisher.markStop(3, STOP_OUTCOME.SKIPPED, { reason: 3, photo: fakeJpeg(800, 43), nowMillis: now });
  now += 1000;
  await publisher.markStop(5, STOP_OUTCOME.DELIVERED, { photo: fakeJpeg(800, 44), nowMillis: now });

  const checked = verifyPhotoChain(publisher.photoChainEntries(), stale);
  assert.equal(checked.matchedAt, 2, "the prefix its own record signed");
  assert.deepEqual(checked.verified.map(entry => entry.stopIndex), [1, 2]);
  assert.deepEqual(checked.unverified.map(entry => entry.stopIndex), [3, 5],
    "the rest is named, so a board can say which proofs it cannot yet vouch for");
  assert.ok(checked.unverified.every(entry => /^[0-9a-f]{64}$/u.test(entry.commitment)));
  assert.equal(
    checked.verified.length + checked.unverified.length, publisher.photoChainEntries().length,
    "nothing is silently dropped — honest about both halves"
  );

  // The verified prefix is verified because it folds to the held head.
  assert.equal(photoChainOf(checked.verified), stale);

  // One more record, and the same list is fully verified — the publisher
  // did not have to do anything, the holder simply caught up.
  now += 10_000;
  const later = await publisher.handleFix({
    lat: 45.5, lon: -73.6, speedMps: 5, nowMillis: now, match: { segment: "3181/885/1", ratio: 0.2 }
  });
  const caught = await subscriber.accept(later.record.bytes, { nowMillis: now });
  assert.equal(verifyPhotoChain(publisher.photoChainEntries(), caught.update.photoChain).matchedAt, 4);
});

test("the run that takes no photo pays nothing, to the byte", async () => {
  // §16.4's vector is the floor and this is the rule above it: a record
  // from a run that has never taken a photo must encode to exactly the
  // bytes it did before the accumulator existed — flag bit clear, field
  // absent, not one byte spent on a mechanism it never used.
  const now = Math.floor(Date.now() / 1000) * 1000;
  const { publisher, subscriber } = await pair({ clock: () => now });
  const emitted = await publisher.markStop(2, STOP_OUTCOME.SKIPPED, { reason: 2 });
  const verdict = await subscriber.accept(emitted.bytes, { nowMillis: now });
  assert.equal(verdict.ok, true);

  const body = verdict.update;
  assert.equal(body.photoChain, null);
  assert.equal(body.lastOutcome.hasPhoto, false);
  const preimage = encodeThreadBodyPreimage(body);
  assert.deepEqual([...preimage], [...body.preimage], "re-encodes to the bytes on the wire");
  // Explicitly stating "no photos" cannot cost anything either.
  assert.deepEqual(
    [...encodeThreadBodyPreimage({ ...body, photoChain: PHOTO_CHAIN_ZERO })], [...body.preimage]
  );
  // And the run that *does* take one pays 32 for all of them, which is
  // what the single commitment used to cost for the newest alone.
  assert.equal(
    encodeThreadBodyPreimage({ ...body, photoChain: "d3".repeat(32) }).length - preimage.length, 32
  );

  // The flag bit is what says "this run has photographed something", so
  // A₀ behind a set bit is not a record any encoder makes and is refused
  // rather than tolerated — a holder must never be handed an accumulator
  // that would name every run at once.
  const withChain = encodeThreadBodyPreimage({ ...body, photoChain: "d3".repeat(32) });
  const zeroed = Uint8Array.from(withChain);
  zeroed.fill(0, withChain.length - 33, withChain.length - 1);
  assert.throws(
    () => decodeThreadBody(Uint8Array.from([...zeroed, ...new Uint8Array(64)])),
    /empty accumulator/u
  );
});
