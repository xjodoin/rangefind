// Dispatch tickets (threads §20): the run's identity belongs to the job.
//
// The claim under test is the one that makes dispatch possible at all:
// the customer's follow link exists at composition time, before anybody
// has been assigned the job — and when the job changes hands mid-run,
// the follower keeps watching the same thread instead of being handed a
// second link and a dead first one.

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createMeshSession } from "../src/pulsemesh/session.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createThreadChannel } from "../src/pulsemesh/thread_session.js";
import {
  STOP_OUTCOME,
  STOP_REASON,
  THREAD_MODE,
  THREAD_TRAVEL_MODE,
  decodeThreadLink,
  encodeThreadBodyPreimage
} from "../src/pulsemesh/thread_codec.js";
import { fromHex, sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";
import { bytesToBase64Url, publicKeyFromSeed } from "../src/pulsemesh/thread_crypto.js";
import { encodeQr } from "../src/qr.js";
import {
  THREAD_MAX_CONTACT_BYTES,
  THREAD_MAX_INSTRUCTIONS_BYTES,
  THREAD_MAX_LABEL_BYTES,
  THREAD_MAX_ORDER_REF_BYTES,
  THREAD_MAX_OFFER_LABEL_BYTES,
  THREAD_STOP_FIELD,
  awardMatchesOffer,
  classifyThreadArtifact,
  decodeJobOffer,
  decodeThreadPlan,
  decodeThreadTicket,
  encodeThreadPlan,
  issueJobOffer,
  issueTicket,
  jobIdOf,
  planRefOf,
  ticketFollowLink,
  verifyThreadTicket
} from "../src/pulsemesh/thread_ticket.js";
import { utf8Bytes } from "../src/pulsemesh/codec.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";
const EPOCH32 = fromHex("f44796c8cc1f3fa797104e925812ff052717f3052b5dbcadb0a36db776e0a4d1");
const ISSUER_SEED = sha256Utf8("pulsemesh-test-dispatcher");
const OTHER_ISSUER_SEED = sha256Utf8("pulsemesh-test-other-dispatcher");
const RUN_SEED = sha256Utf8("pulsemesh-test-run");

const DELIVERY = {
  dwellSeconds: 90,
  stops: [
    { lat: 45.5019, lon: -73.5674, plannedUnixSeconds: 1754265600, label: "Chez Lise" },
    { lat: 45.5088, lon: -73.554, label: "3e étage, sonner" }
  ]
};

async function fixture(t) {
  let engine;
  try {
    engine = await openRouteGraphDir(GRAPH_DIR);
  } catch {
    t.skip(`route graph fixture missing at ${GRAPH_DIR}`);
    return null;
  }
  const centre = leaf => {
    const bbox = engine.root.leaves[leaf].bbox;
    return { lat: (bbox.minLat + bbox.maxLat) / 2 / 1e7, lon: (bbox.minLon + bbox.maxLon) / 2 / 1e7 };
  };
  for (let attempt = 0; attempt < 40; attempt++) {
    try {
      const from = centre((attempt * 7) % engine.root.leaves.length);
      const to = centre((attempt * 7 + 11) % engine.root.leaves.length);
      const route = await engine.route({ from, to });
      if ((route.edges || []).length >= 20 && route.geometry?.length > 20) return { engine, route, from, to };
    } catch {
      continue;
    }
  }
  t.skip("no suitable route in the fixture graph");
  return null;
}

const flush = () => new Promise(resolve => setTimeout(resolve, 0));

async function until(condition, { attempts = 50 } = {}) {
  for (let i = 0; i < attempts; i++) {
    if (condition()) return true;
    await flush();
  }
  return condition();
}

/** A ticket for the plan, bound to whatever epoch the channel is running. */
async function ticketFor(epochPrefix8, { notAfterSeconds, mode = THREAD_MODE.FINE } = {}) {
  const planBytes = encodeThreadPlan(DELIVERY);
  const issued = await issueTicket({
    issuerSeed: ISSUER_SEED,
    epochPrefix8,
    planBytes,
    notAfter: notAfterSeconds ?? Math.floor(Date.now() / 1000) + 3600,
    mode
  });
  return { issued, planBytes };
}

test("the run plan encodes to the same bytes twice, and its ref is the binding", () => {
  const bytes = encodeThreadPlan(DELIVERY);
  assert.deepEqual(bytes, encodeThreadPlan(DELIVERY), "a plan is deterministic — jobId hashes it");

  const decoded = decodeThreadPlan(bytes);
  assert.equal(decoded.dwellSeconds, 90);
  assert.equal(decoded.stops.length, 2);
  assert.equal(decoded.stops[0].index, 1, "visit order is 1-based, as plan.stops[].index");
  assert.equal(decoded.stops[1].index, 2);
  assert.equal(decoded.stops[0].label, "Chez Lise");
  assert.equal(decoded.stops[1].label, "3e étage, sonner", "labels are UTF-8, not ASCII");
  assert.equal(decoded.stops[0].plannedUnixSeconds, 1754265600);
  assert.equal(decoded.stops[1].plannedUnixSeconds, null, "0 on the wire means no planned time");

  // Coordinates are E7 integers on the wire — a float would make two
  // hosts disagree about planRef, which is the whole binding.
  assert.equal(decoded.stops[0].latE7, 455019000);
  assert.equal(decoded.stops[1].lonE7, -735540000, "and negative longitudes zigzag");
  assert.ok(Math.abs(decoded.stops[0].lat - 45.5019) < 1e-9);
  assert.deepEqual(encodeThreadPlan(decoded), bytes, "a decoded plan re-encodes byte-for-byte");

  assert.equal(decoded.planRef.length, 8);
  assert.deepEqual(decoded.planRef, planRefOf(bytes));
  assert.notDeepEqual(planRefOf(encodeThreadPlan({ stops: DELIVERY.stops.slice(0, 1) })), decoded.planRef);

  assert.throws(
    () => encodeThreadPlan({ stops: [{ lat: 1, lon: 2, label: "x".repeat(THREAD_MAX_LABEL_BYTES + 1) }] }),
    /48 bytes/
  );
  assert.throws(() => decodeThreadPlan(Uint8Array.from([...bytes, 0])), /trailing bytes/);

  // How the job is to be made belongs to the plan, not to the driver's
  // app: the dispatcher who routed and priced a bike round decided it.
  assert.equal(decoded.travelMode, THREAD_TRAVEL_MODE.UNSPECIFIED, "0 when nobody said");
  const byBike = encodeThreadPlan({ ...DELIVERY, travelMode: THREAD_TRAVEL_MODE.BIKE });
  assert.equal(decodeThreadPlan(byBike).travelMode, THREAD_TRAVEL_MODE.BIKE);
  assert.deepEqual(encodeThreadPlan(decodeThreadPlan(byBike)), byBike, "and it re-encodes byte-for-byte");
  assert.notDeepEqual(planRefOf(byBike), decoded.planRef,
    "a different travel mode is a different plan, and so a different job");
  assert.throws(() => encodeThreadPlan({ ...DELIVERY, travelMode: 9 }), /travelMode/u);
});

test("a bike round stays a bike round from the ticket to the follower's ETA", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const customer = await createMeshSession({
    engine, network, id: "customer", transport: "loopback", readOnly: true, clock
  });
  const courier = await createMeshSession({ engine, network, id: "courier", transport: "loopback", clock });
  const customerThreads = createThreadChannel({ node: customer.node, network, engine, id: "customer", clock });
  const courierThreads = createThreadChannel({ node: courier.node, network, engine, id: "courier", clock });

  const end = route.geometry[route.geometry.length - 1];
  const mid = route.geometry[Math.floor(route.geometry.length / 2)];
  const planBytes = encodeThreadPlan({
    dwellSeconds: 60,
    travelMode: THREAD_TRAVEL_MODE.BIKE,
    stops: [
      { lat: mid[0], lon: mid[1], label: "porte 1" },
      { lat: end[0], lon: end[1], label: "porte 3" }
    ]
  });
  const issued = await issueTicket({
    issuerSeed: ISSUER_SEED,
    epochPrefix8: courier.node.epochPrefix8,
    planBytes,
    notAfter: Math.floor(now / 1000) + 3600,
    mode: THREAD_MODE.FINE
  });
  assert.equal(issued.ticket.plan.travelMode, THREAD_TRAVEL_MODE.BIKE, "PMP1 → PMK1");

  const updates = [];
  const follow = await customerThreads.follow(issued.link, { onUpdate: update => updates.push(update) });
  const run = await courierThreads.publishTicket(issued.bytes);
  assert.equal(run.travelMode, THREAD_TRAVEL_MODE.BIKE, "PMK1 → publisher, without the driver being asked");

  for (let i = 0; i < route.geometry.length && updates.length < 2; i += Math.ceil(route.geometry.length / 6)) {
    const [lat, lon] = route.geometry[i];
    now += 6000;
    await run.handleFix({ lat, lon, speedMps: 5, nowMillis: now });
    await flush();
  }
  await until(() => updates.length >= 1);
  assert.equal(follow.latest().travelMode, THREAD_TRAVEL_MODE.BIKE, "publisher → wire → subscriber");
  assert.equal(follow.status().travelMode, THREAD_TRAVEL_MODE.BIKE);

  // The demo's own graph is a car graph, and the honest ETA says so
  // rather than quietly promising a car's arrival for a bike.
  const plan = { stops: issued.ticket.plan.stops, dwellSeconds: issued.ticket.plan.dwellSeconds };
  const eta = await follow.eta({ plan, myStopIndex: 2 });
  assert.equal(eta.travelModeName, "bike");
  assert.equal(eta.profileBasis, "unstated", "the host never named its graph, so nothing is claimed");

  // The outcome the driver marks reaches the follower on the same thread.
  now += 1000;
  await run.markStop(1, STOP_OUTCOME.SKIPPED, { reason: STOP_REASON.INACCESSIBLE });
  await until(() => follow.latest()?.lastOutcome != null);
  assert.deepEqual(follow.latest().lastOutcome, {
    stopIndex: 1, outcome: STOP_OUTCOME.SKIPPED, reasonCode: STOP_REASON.INACCESSIBLE, hasPhoto: false
  });
  assert.deepEqual(follow.latest().outcomes, [STOP_OUTCOME.SKIPPED, STOP_OUTCOME.PENDING]);

  follow.stop();
  customerThreads.close();
  courierThreads.close();
});

test("a ticket survives base64url, and nothing in it is malleable", async () => {
  const notAfter = Math.floor(Date.now() / 1000) + 3600;
  const planBytes = encodeThreadPlan(DELIVERY);
  const issued = await issueTicket({
    issuerSeed: ISSUER_SEED,
    epoch32: EPOCH32,
    planBytes,
    notAfter,
    mode: THREAD_MODE.FINE,
    privateSeed: RUN_SEED
  });

  const ticket = decodeThreadTicket(issued.base64url);
  assert.deepEqual(ticket.bytes, issued.bytes, "base64url is the transport, not a second format");
  assert.equal(ticket.version, 1);
  assert.equal(ticket.seedPresent, true);
  assert.equal(ticket.mode, THREAD_MODE.FINE, "the issuer decides what the link is worth (§11)");
  assert.equal(ticket.notAfter, notAfter);
  assert.deepEqual(ticket.privateSeed, RUN_SEED);
  assert.deepEqual(ticket.planBytes, planBytes);
  assert.equal(ticket.plan.stops.length, 2);
  assert.deepEqual(ticket.issuerPublicKey, await publicKeyFromSeed(ISSUER_SEED));

  const epochPrefix8 = EPOCH32.subarray(0, 8);
  assert.deepEqual(await verifyThreadTicket(ticket, { epochPrefix8 }), { ok: true });

  // Every field the signature covers, one flipped bit at a time. The
  // preimage runs from the magic through the seed precisely so that none
  // of these is a survivable edit.
  const seedStart = issued.bytes.length - 96;
  const planStart = seedStart - planBytes.length;
  const fields = {
    flags: 5, mode: 6, epoch: 9, notAfter: 17, issuer: 25, plan: planStart + 9, seed: seedStart + 7
  };
  for (const [field, offset] of Object.entries(fields)) {
    const tampered = Uint8Array.from(issued.bytes);
    tampered[offset] ^= 0x01;
    const verdict = await verifyThreadTicket(tampered, { epochPrefix8 });
    assert.equal(verdict.ok, false, `a flipped bit in ${field} is refused`);
  }

  const expired = await issueTicket({
    issuerSeed: ISSUER_SEED, epoch32: EPOCH32, planBytes, notAfter: Math.floor(Date.now() / 1000) - 10
  });
  assert.match((await verifyThreadTicket(expired.ticket, { epochPrefix8 })).reason, /expired/);

  const wrongEpoch = await verifyThreadTicket(ticket, { epochPrefix8: new Uint8Array(8) });
  assert.match(wrongEpoch.reason, /different graph epoch/);

  // Issuer pinning: a perfectly valid ticket from someone this host does
  // not work for is still refused.
  const stranger = await issueTicket({
    issuerSeed: OTHER_ISSUER_SEED, epoch32: EPOCH32, planBytes, notAfter
  });
  const pinned = toHex(await publicKeyFromSeed(ISSUER_SEED));
  assert.deepEqual(await verifyThreadTicket(ticket, { epochPrefix8, expectedIssuerHex: pinned }), { ok: true });
  const refused = await verifyThreadTicket(stranger.ticket, { epochPrefix8, expectedIssuerHex: pinned });
  assert.equal(refused.ok, false);
  assert.match(refused.reason, /different issuer/);
});

test("an offer and the ticket that fulfils it are the same job", async () => {
  const notAfter = Math.floor(Date.now() / 1000) + 3600;
  const planBytes = encodeThreadPlan(DELIVERY);
  const issued = await issueTicket({ issuerSeed: ISSUER_SEED, epoch32: EPOCH32, planBytes, notAfter });

  // (b) ticket first, offer broadcast from it (§20.4 PMJ1).
  const derived = await issueJobOffer({ issuerSeed: ISSUER_SEED, ticket: issued.ticket, totalMeters: 4200 });
  assert.equal(derived.jobIdHex, issued.jobIdHex, "same job: jobId hashes neither the seed nor the signature");
  assert.deepEqual(derived.offer.planRef, planRefOf(planBytes), "and the offer commits to the plan");
  assert.equal(derived.offer.stopCount, DELIVERY.stops.length);

  // (a) offer first, awarded later — the marketplace order of events.
  const offered = await issueJobOffer({
    issuerSeed: ISSUER_SEED, epoch32: EPOCH32, planBytes, notAfter, totalMeters: 4200
  });
  const awarded = await issueTicket({ issuerSeed: ISSUER_SEED, epoch32: EPOCH32, planBytes, notAfter });
  assert.equal(awarded.jobIdHex, offered.jobIdHex, "the awarded ticket joins back to the offer");
  assert.notDeepEqual(awarded.ticket.privateSeed, issued.ticket.privateSeed, "on a freshly minted run seed");
  assert.equal(offered.offer.jobId.length, 16);
  assert.deepEqual(awardMatchesOffer(offered.offer, awarded.ticket), { ok: true, reason: null });

  const elsewhere = await issueTicket({
    issuerSeed: ISSUER_SEED,
    epoch32: EPOCH32,
    planBytes: encodeThreadPlan({ ...DELIVERY, stops: DELIVERY.stops.slice(0, 1) }),
    notAfter
  });
  assert.notEqual(elsewhere.jobIdHex, issued.jobIdHex, "a different plan is a different job");

  await assert.rejects(
    () => issueJobOffer({ issuerSeed: OTHER_ISSUER_SEED, ticket: issued.ticket }),
    /the ticket's own issuer/
  );
});

// The seedless PMK1 is no longer an offer — §20.4 gives offers their own
// artifact — but the codec can still express one, and the publisher's
// refusal of it is the guard that must not rot.
test("a seedless ticket is not a publish capability", async () => {
  const epochPrefix8 = EPOCH32.subarray(0, 8);
  const notAfter = Math.floor(Date.now() / 1000) + 3600;
  const offered = await issueTicket({
    issuerSeed: ISSUER_SEED, epoch32: EPOCH32, plan: DELIVERY, notAfter, seedless: true
  });
  // It is a *valid* artifact; what it is not is a run.
  assert.deepEqual(await verifyThreadTicket(offered.ticket, { epochPrefix8 }), { ok: true });
  await assert.rejects(() => ticketFollowLink(offered.ticket), /no run seed/);

  const channel = createThreadChannel({ epochHex: toHex(EPOCH32) });
  await assert.rejects(() => channel.publishTicket(offered.bytes), /not a publish capability/);

  const expired = await issueTicket({
    issuerSeed: ISSUER_SEED, epoch32: EPOCH32, plan: DELIVERY, notAfter: Math.floor(Date.now() / 1000) - 1
  });
  await assert.rejects(() => channel.publishTicket(expired.bytes), /expired/);

  const elsewhere = await issueTicket({
    issuerSeed: ISSUER_SEED, epochPrefix8: new Uint8Array(8), plan: DELIVERY, notAfter
  });
  await assert.rejects(() => channel.publishTicket(elsewhere.bytes), /different graph epoch/);

  const forged = Uint8Array.from((await issueTicket({
    issuerSeed: ISSUER_SEED, epoch32: EPOCH32, plan: DELIVERY, notAfter
  })).bytes);
  forged[forged.length - 70] ^= 0x02;
  await assert.rejects(() => channel.publishTicket(forged), /signature does not verify/);

  channel.close();
});

// A delivery day is eight to ten hours. The publisher's own
// THREAD_MAX_RUN_SECONDS is six, which is the right bound for a run
// somebody started by pressing "share this drive" and may well forget to
// stop — and the wrong one for a dispatched day, which carries a bound
// the dispatcher set on purpose.
test("a dispatched day runs to the ticket's expiry; a self-started drive still stops at six hours", async () => {
  // Anchored to the wall clock: `notAfter` is verified against real unix
  // seconds, so a virtual clock starting at zero is a ticket that
  // expired in 1970.
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const startSeconds = Math.floor(now / 1000);
  const channel = createThreadChannel({ epochHex: toHex(EPOCH32), clock });
  const epochPrefix8 = EPOCH32.subarray(0, 8);
  // Far from either planned stop, so nothing here is stop suppression.
  const fix = { lat: 45.4, lon: -73.9, speedMps: 12 };

  const shift = await ticketFor(epochPrefix8, { notAfterSeconds: startSeconds + 10 * 3600 });
  const dispatched = await channel.publishTicket(shift.issued.bytes, { catchUp: false });

  // A ticket a dispatcher fat-fingered four days out. `notAfter` is a
  // uint32 and nothing stops it naming next year; the run bound is
  // clamped at 24 h, because no run should outlive a shift by that much.
  const fatFingered = await ticketFor(epochPrefix8, { notAfterSeconds: startSeconds + 100 * 3600 });
  const clamped = await channel.publishTicket(fatFingered.issued.bytes, { catchUp: false });

  const mine = await channel.publish({ ttlSeconds: 10 * 3600, mode: THREAD_MODE.FINE });

  now += 7 * 3600 * 1000;
  assert.equal(
    (await dispatched.handleFix({ ...fix, nowMillis: now })).published, true,
    "mid-afternoon on a dispatched day, the run is still publishing"
  );
  assert.equal((await clamped.handleFix({ ...fix, nowMillis: now })).published, true);

  const late = await mine.handleFix({ ...fix, nowMillis: now });
  assert.equal(late.published, false, "a run this device started for itself is over at six hours");
  assert.equal(late.reason, "run-expired");

  now += 3.5 * 3600 * 1000; // 10.5 h in: past the ticket's own expiry
  const done = await dispatched.handleFix({ ...fix, nowMillis: now });
  assert.equal(done.published, false, "and the ticket's expiry is the bound, not an extension without end");
  assert.equal(done.reason, "run-expired");

  now = Math.floor(Date.now() / 1000) * 1000 + 25 * 3600 * 1000;
  const overClamp = await clamped.handleFix({ ...fix, nowMillis: now });
  assert.equal(overClamp.published, false, "24 h is the ceiling however far out notAfter points");
  assert.equal(overClamp.reason, "run-expired");

  channel.close();
});

test("a capability that came through a mail client is still a capability", async () => {
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const channel = createThreadChannel({ epochHex: toHex(EPOCH32), clock });
  const { issued } = await ticketFor(EPOCH32.subarray(0, 8), {
    notAfterSeconds: Math.floor(now / 1000) + 3600
  });

  // What a hard-wrapping mail client does to base64url: a newline in the
  // middle of the fragment. That is a broken paste, not a broken
  // capability, and the follow path has to see the difference.
  const encoded = bytesToBase64Url(issued.link);
  const wrapped = `${encoded.slice(0, 24)}\n${encoded.slice(24, 48)}\r\n  ${encoded.slice(48)}`;
  assert.notEqual(wrapped, encoded);

  const bare = await channel.follow(wrapped);
  assert.deepEqual(bare.link.publicKey, await publicKeyFromSeed(issued.ticket.privateSeed));
  bare.stop();

  const fromUrl = await channel.follow(`https://order.example/t#${wrapped}`);
  assert.deepEqual(fromUrl.link.publicKey, bare.link.publicKey, "and the same through a URL");
  fromUrl.stop();

  // The ticket decoder gets the same treatment from the demo's
  // `fragmentOf`, which strips whitespace before handing it over.
  const ticketText = bytesToBase64Url(issued.bytes);
  const wrappedTicket = `wayfind://ticket#${ticketText.slice(0, 40)}\n${ticketText.slice(40)}\n`;
  const fragment = wrappedTicket.slice(wrappedTicket.lastIndexOf("#") + 1).replace(/\s+/gu, "");
  assert.deepEqual(decodeThreadTicket(fragment).bytes, issued.bytes);

  channel.close();
});

test("an unreadable ticket is reported as a ticket, not as a broken link", async () => {
  // The bug this pins: hosts used to try the ticket decoder, swallow its
  // error, try the link decoder, and surface *that* one — so a driver
  // holding a ticket minted before a wire change was told "a thread link
  // is 45 bytes", which describes nothing they have. Identity comes from
  // the magic; readability is a separate answer carried beside it.
  const now = Math.floor(Date.now() / 1000);
  const { issued } = await ticketFor(EPOCH32.subarray(0, 8), { notAfterSeconds: now + 3600 });

  assert.deepEqual(
    classifyThreadArtifact(bytesToBase64Url(issued.bytes)),
    // §21.11: an ordinary job is a job, and says so — `routeDay: false`
    // here is a read answer, not the absence of one.
    { kind: "ticket", reason: null, sealed: false, routeDay: false, serviceDay: null }
  );
  assert.deepEqual(
    classifyThreadArtifact(bytesToBase64Url(issued.link)),
    { kind: "link", reason: null, sealed: false, routeDay: false, serviceDay: null },
    "and 45 bytes is a link"
  );
  assert.equal(issued.link.length, 45);

  // A PMK1 blob this build cannot read: the magic still says what it is.
  const truncated = issued.bytes.subarray(0, issued.bytes.length - 40);
  const stale = classifyThreadArtifact(truncated);
  assert.equal(stale.kind, "ticket", "the magic is the identity, decoding is not");
  assert.equal(stale.routeDay, null, "and which kind of ticket it is, unread, is not knowable");
  assert.match(stale.reason, /ticket/iu);
  assert.match(stale.reason, /dispatcher/iu, "and it says what to do about it");
  assert.doesNotMatch(stale.reason, /45 bytes/u, "never the other artifact's complaint");

  // Neither magic, and neither width: nothing to claim.
  assert.deepEqual(
    classifyThreadArtifact("not-a-capability-at-all"),
    { kind: null, reason: null, sealed: false, routeDay: false, serviceDay: null }
  );
  assert.deepEqual(
    classifyThreadArtifact(""),
    { kind: null, reason: null, sealed: false, routeDay: false, serviceDay: null }
  );
  assert.deepEqual(
    classifyThreadArtifact(null),
    { kind: null, reason: null, sealed: false, routeDay: false, serviceDay: null }
  );

  // Same three forms every host takes, wrapped by a mail client.
  const text = bytesToBase64Url(issued.bytes);
  assert.equal(
    classifyThreadArtifact(`wayfind://ticket#${text.slice(0, 40)}\n${text.slice(40)}\n`).kind,
    "ticket"
  );
});

test("a full delivery day does not fit a camera, and does not have to", async () => {
  // 100 drops with short labels: an ordinary suburban route sheet, and
  // several kilobytes of signed ticket.
  const stops = [];
  for (let i = 0; i < 100; i++) {
    stops.push({
      lat: 45.4 + (i * 0.0013),
      lon: -73.7 + (i * 0.0011),
      label: `Drop ${i + 1}`
    });
  }
  const issued = await issueTicket({
    issuerSeed: ISSUER_SEED,
    epoch32: EPOCH32,
    plan: { dwellSeconds: 60, stops },
    notAfter: Math.floor(Date.now() / 1000) + 10 * 3600
  });
  assert.ok(issued.bytes.length > 2000, `a 100-stop ticket is ${issued.bytes.length} bytes`);

  const url = `wayfind://ticket#${issued.base64url}`;
  assert.throws(
    () => encodeQr(url, { maxVersion: 25 }),
    /within version 25/,
    "past the scannability bound the answer is no QR, never an unscannable one"
  );

  // The primitive does not care which carrier brought it: the file holds
  // the identical signed bytes, and they verify the same.
  const asFile = `${url}\n`;
  const fragment = asFile.slice(asFile.lastIndexOf("#") + 1).replace(/\s+/gu, "");
  const decoded = decodeThreadTicket(fragment);
  assert.deepEqual(decoded.bytes, issued.bytes);
  assert.equal(decoded.plan.stops.length, 100);
  assert.equal(decoded.plan.stops[99].label, "Drop 100");
  assert.deepEqual(
    await verifyThreadTicket(decoded, { epochPrefix8: EPOCH32.subarray(0, 8) }),
    { ok: true }
  );
  assert.equal(jobIdOf(decoded).length, 16);
});

test("the customer gets the link before the courier gets the job", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const customer = await createMeshSession({
    engine, network, id: "customer", transport: "loopback", readOnly: true, clock
  });
  const courier = await createMeshSession({ engine, network, id: "courier", transport: "loopback", clock });
  const customerThreads = createThreadChannel({ node: customer.node, network, engine, id: "customer", clock });
  const courierThreads = createThreadChannel({ node: courier.node, network, engine, id: "courier", clock });

  // Composition: the restaurant knows the drop, not the driver.
  const end = route.geometry[route.geometry.length - 1];
  const planBytes = encodeThreadPlan({
    dwellSeconds: 0,
    stops: [{ lat: end[0], lon: end[1], label: "porte 3" }]
  });
  const issued = await issueTicket({
    issuerSeed: ISSUER_SEED,
    epochPrefix8: courier.node.epochPrefix8,
    planBytes,
    notAfter: Math.floor(now / 1000) + 3600,
    mode: THREAD_MODE.FINE,
    baseUrl: "https://order.example/t"
  });
  assert.equal(issued.link.length, 45, "the follow link exists at composition time");
  assert.match(issued.url, /#[A-Za-z0-9_-]{60}/);
  assert.deepEqual(decodeThreadLink(issued.link).publicKey, await publicKeyFromSeed(issued.ticket.privateSeed));

  // The customer starts following a run that has no publisher at all.
  const updates = [];
  const follow = await customerThreads.follow(issued.url, { onUpdate: update => updates.push(update) });
  assert.equal(follow.latest(), null, "nothing to see yet — nobody has been dispatched");

  // Only now is the job handed to a courier.
  const run = await courierThreads.publishTicket(issued.bytes, { baseUrl: "https://order.example/t" });
  assert.equal(run.jobIdHex, issued.jobIdHex);
  assert.equal(run.url, issued.url, "which publishes onto the link already in the customer's hands");

  for (let i = 0; i < route.geometry.length && updates.length < 4; i += Math.ceil(route.geometry.length / 6)) {
    const [lat, lon] = route.geometry[i];
    now += 6000;
    await run.handleFix({ lat, lon, speedMps: 9, nowMillis: now });
    await flush();
  }
  await until(() => updates.length >= 3);
  assert.ok(updates.length >= 3, `the customer is watching the delivery (${updates.length})`);
  assert.deepEqual(
    follow.latest().planRef, planRefOf(planBytes),
    "and every record binds to the plan the ticket carried"
  );

  const position = await follow.position();
  assert.ok(position && Number.isFinite(position.lat), "a fine ticket is a live locator");

  follow.stop();
  customerThreads.close();
  courierThreads.close();
});

test("a job changes hands mid-run and the follower never sees a gap", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const first = await createMeshSession({ engine, network, id: "first", transport: "loopback", clock });
  const second = await createMeshSession({ engine, network, id: "second", transport: "loopback", clock });
  const customer = await createMeshSession({ engine, network, id: "customer", transport: "loopback", clock });
  const firstThreads = createThreadChannel({ node: first.node, network, engine, id: "first", clock });
  const secondThreads = createThreadChannel({ node: second.node, network, engine, id: "second", clock });
  const customerThreads = createThreadChannel({ node: customer.node, network, engine, id: "customer", clock });

  const { issued } = await ticketFor(first.node.epochPrefix8, { notAfterSeconds: Math.floor(now / 1000) + 3600 });

  const seqs = [];
  const follow = await customerThreads.follow(issued.link, { onUpdate: update => seqs.push(update.seq) });

  const runA = await firstThreads.publishTicket(issued.bytes);
  assert.equal(runA.seq, 0, "the first holder starts at the beginning");
  for (let i = 0; i < route.geometry.length && seqs.length < 3; i += 3) {
    const [lat, lon] = route.geometry[i];
    now += 6000;
    await runA.handleFix({ lat, lon, speedMps: 12, nowMillis: now });
    await flush();
  }
  await until(() => seqs.length >= 3);
  const handoverSeq = runA.seq;
  assert.ok(handoverSeq >= 3, `the first courier published ${handoverSeq} records`);

  // The bike breaks. The dispatcher hands the same ticket to someone
  // else; the customer's link is untouched.
  firstThreads.close();

  const runB = await secondThreads.publishTicket(issued.bytes);
  assert.equal(
    runB.seq, handoverSeq,
    "the second courier resumed from the audience's own cache, not from a server"
  );
  assert.deepEqual(runB.link, issued.link, "onto the same thread");

  for (let i = 12; i < route.geometry.length && seqs.length < handoverSeq + 2; i += 3) {
    const [lat, lon] = route.geometry[i];
    now += 6000;
    await runB.handleFix({ lat, lon, speedMps: 12, nowMillis: now });
    await flush();
  }
  await until(() => seqs.length > handoverSeq);

  assert.ok(seqs.length > handoverSeq, `the customer kept receiving across the handover (${seqs.join(",")})`);
  for (let i = 1; i < seqs.length; i++) {
    assert.ok(seqs[i] > seqs[i - 1], "and every accepted record advanced the sequence");
  }
  assert.equal(
    follow.stats.dropsByStep.step7, undefined,
    "no record was ever refused for a sequence regression"
  );
  assert.equal(follow.stats.forgeries, 0);

  follow.stop();
  secondThreads.close();
  customerThreads.close();
});

// --- §20.8 per-stop delivery metadata --------------------------------------

/**
 * The reference stop the §20.8 size table is measured against: an
 * ordinary suburban drop with a 20-byte label, a planned time, and
 * planet-scale coordinates (7 significant digits, negative longitude).
 */
function referenceStop(i, extra = {}) {
  return {
    lat: 45.4 + (i * 0.0013),
    lon: -73.7 + (i * 0.0011),
    plannedUnixSeconds: 1754265600 + (i * 300),
    label: `12 rue Sainte-Cath ${i}`.slice(0, 20),
    ...extra
  };
}

/** Does a scannable QR (§20.5's maxVersion 25 policy) hold this ticket? */
function fitsScannableQr(base64url) {
  try {
    encodeQr(`wayfind://ticket#${base64url}`, { ecLevel: "M", maxVersion: 25 });
    return true;
  } catch {
    return false;
  }
}

async function ticketOf(stops) {
  return issueTicket({
    issuerSeed: ISSUER_SEED,
    epoch32: EPOCH32,
    plan: { dwellSeconds: 60, stops },
    notAfter: Math.floor(Date.now() / 1000) + 10 * 3600
  });
}

test("a stop's delivery details round-trip, and a stated 0 parcels is not the same as silence", () => {
  const plan = {
    dwellSeconds: 90,
    stops: [
      // Everything set.
      {
        lat: 45.5019,
        lon: -73.5674,
        plannedUnixSeconds: 1754265600,
        label: "Chez Lise",
        orderRef: "4471-B",
        parcels: 3,
        instructions: "porte de côté, sonner deux fois",
        contact: "+15145550134"
      },
      // Nothing set: the ordinary address-only stop.
      { lat: 45.5088, lon: -73.554, label: "3e étage" },
      // A *stated* zero. This is the whole reason the fields are flagged.
      { lat: 45.51, lon: -73.55, label: "Ramassage", parcels: 0, orderRef: "4472" }
    ]
  };
  const bytes = encodeThreadPlan(plan);
  assert.deepEqual(bytes, encodeThreadPlan(plan), "still deterministic — jobId hashes these bytes");

  const decoded = decodeThreadPlan(bytes);
  assert.equal(decoded.stops[0].orderRef, "4471-B");
  assert.equal(decoded.stops[0].parcels, 3);
  assert.equal(decoded.stops[0].instructions, "porte de côté, sonner deux fois");
  assert.equal(decoded.stops[0].contact, "+15145550134");

  // Null throughout means unstated — never "" and never 0.
  assert.equal(decoded.stops[1].orderRef, null);
  assert.equal(decoded.stops[1].parcels, null, "nobody said how many parcels");
  assert.equal(decoded.stops[1].instructions, null);
  assert.equal(decoded.stops[1].contact, null);

  // The distinction the flags byte exists for: 0 is a claim ("nothing to
  // carry" — a collection, a survey), and a UI must not render the stop
  // above as if the dispatcher had said this.
  assert.equal(decoded.stops[2].parcels, 0);
  assert.notEqual(decoded.stops[2].parcels, decoded.stops[1].parcels);
  assert.ok(Number.isFinite(decoded.stops[2].parcels));
  assert.ok(!Number.isFinite(decoded.stops[1].parcels));

  assert.deepEqual(
    encodeThreadPlan(decoded), bytes,
    "encode(decode(x)) === x — including the 0 and including the absences"
  );

  // An empty string is not a field. It encodes as absent, decodes as
  // null, and re-encodes identically, so there is only ever one encoding.
  assert.deepEqual(
    encodeThreadPlan({ stops: [{ lat: 1, lon: 2, label: "x", orderRef: "", instructions: "" }] }),
    encodeThreadPlan({ stops: [{ lat: 1, lon: 2, label: "x" }] }),
    "\"\" is unstated, and the flags byte is the only place presence lives"
  );

  // Details are part of the plan, so they are part of the job.
  const without = encodeThreadPlan({ ...plan, stops: plan.stops.map(({ orderRef, ...rest }) => rest) });
  assert.notDeepEqual(planRefOf(without), decoded.planRef, "a different plan is a different job");
});

test("each oversized stop detail is refused by name, never truncated", () => {
  const at = extra => () => encodeThreadPlan({ stops: [{ lat: 45.5, lon: -73.5, label: "x", ...extra }] });

  // Truncating any of these is worse than refusing: a shortened order
  // reference matches the wrong parcel and a shortened number does not
  // ring, so the cap is named and the encode fails.
  assert.throws(at({ orderRef: "x".repeat(THREAD_MAX_ORDER_REF_BYTES + 1) }), /order reference is at most 24 bytes/u);
  assert.throws(at({ instructions: "y".repeat(THREAD_MAX_INSTRUCTIONS_BYTES + 1) }), /instruction is at most 64 bytes/u);
  assert.throws(at({ contact: "z".repeat(THREAD_MAX_CONTACT_BYTES + 1) }), /contact is at most 24 bytes/u);
  assert.throws(at({ parcels: 65536 }), /0 to 65535/u);
  assert.throws(at({ parcels: -1 }), /0 to 65535/u);
  assert.throws(at({ parcels: 1.5 }), /whole number/u);

  // The caps are UTF-8 **bytes**, not characters: 24 accented characters
  // is 48 bytes, and the encoder counts what goes on the wire.
  assert.throws(at({ orderRef: "é".repeat(THREAD_MAX_ORDER_REF_BYTES) }), /at most 24 bytes; this one is 48/u);
  assert.doesNotThrow(at({ orderRef: "é".repeat(THREAD_MAX_ORDER_REF_BYTES / 2) }));

  // And each cap is reachable — a limit nothing can hit is a bug.
  assert.doesNotThrow(at({
    orderRef: "x".repeat(THREAD_MAX_ORDER_REF_BYTES),
    parcels: 65535,
    instructions: "y".repeat(THREAD_MAX_INSTRUCTIONS_BYTES),
    contact: "z".repeat(THREAD_MAX_CONTACT_BYTES)
  }));
});

test("a stop flagging a field this reader does not know is refused, not skipped", () => {
  // A stop with no metadata ends in its flags byte, so the last byte of
  // the plan is the one to forge.
  const bytes = encodeThreadPlan({ stops: [{ lat: 45.5, lon: -73.5, label: "Chez Lise" }] });
  assert.equal(bytes[bytes.length - 1], 0, "an address-only stop spends exactly one byte saying so");

  for (const bit of [0x10, 0x20, 0x40, 0x80]) {
    const forged = Uint8Array.from(bytes);
    forged[forged.length - 1] = bit;
    // Skipping the bit and reading on is how a driver ends up at a door
    // with the instruction that mattered silently removed.
    assert.throws(() => decodeThreadPlan(forged), /reserved stop flag bits must be zero/u, `bit ${bit}`);
  }
  // A known bit still decodes, so the refusal is about the reserved range
  // and not about any flags byte being set at all.
  const known = Uint8Array.from([...bytes.subarray(0, bytes.length - 1), THREAD_STOP_FIELD.PARCELS, 7]);
  assert.equal(decodeThreadPlan(known).stops[0].parcels, 7);

  // A set bit with a zero-length value would be a second encoding of one
  // plan, and planRef hashes plan bytes.
  const empty = Uint8Array.from([...bytes.subarray(0, bytes.length - 1), THREAD_STOP_FIELD.ORDER_REF, 0]);
  assert.throws(() => decodeThreadPlan(empty), /order reference flag is set with no order reference/u);

  // Trailing bytes stay refused with metadata in play, as everywhere (§5).
  assert.throws(() => decodeThreadPlan(Uint8Array.from([...known, 0])), /trailing bytes/u);
});

test("delivery details reach the driver and cannot reach a follower", async t => {
  const found = await fixture(t);
  if (!found) return;
  const { engine, route } = found;
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const network = createLoopbackNetwork({ clock });

  const customer = await createMeshSession({
    engine, network, id: "customer", transport: "loopback", readOnly: true, clock
  });
  const courier = await createMeshSession({ engine, network, id: "courier", transport: "loopback", clock });
  const customerThreads = createThreadChannel({ node: customer.node, network, engine, id: "customer", clock });
  const courierThreads = createThreadChannel({ node: courier.node, network, engine, id: "courier", clock });

  const mid = route.geometry[Math.floor(route.geometry.length / 2)];
  const end = route.geometry[route.geometry.length - 1];
  const ORDER_REF = "4471-B";
  const CONTACT = "+15145550134";
  const INSTRUCTIONS = "porte de côté, sonner deux fois";
  const planBytes = encodeThreadPlan({
    dwellSeconds: 60,
    travelMode: THREAD_TRAVEL_MODE.BIKE,
    stops: [
      {
        lat: mid[0],
        lon: mid[1],
        label: "porte 1",
        orderRef: ORDER_REF,
        parcels: 2,
        instructions: INSTRUCTIONS,
        contact: CONTACT
      },
      { lat: end[0], lon: end[1], label: "porte 3", parcels: 0 }
    ]
  });
  const issued = await issueTicket({
    issuerSeed: ISSUER_SEED,
    epochPrefix8: courier.node.epochPrefix8,
    planBytes,
    notAfter: Math.floor(now / 1000) + 3600,
    mode: THREAD_MODE.FINE
  });

  // issueTicket → decodeThreadTicket: the ticket carries the plan whole.
  const reread = decodeThreadTicket(issued.base64url);
  assert.equal(reread.plan.stops[0].orderRef, ORDER_REF);
  assert.equal(reread.plan.stops[0].parcels, 2);
  assert.equal(reread.plan.stops[0].instructions, INSTRUCTIONS);
  assert.equal(reread.plan.stops[0].contact, CONTACT);
  assert.equal(reread.plan.stops[1].parcels, 0, "a stated 0 survives the ticket");
  assert.equal(reread.plan.stops[1].contact, null);

  // …and the public offer of the same job carries **none** of it: it is
  // the same job (`jobId` joins) committed to by hash, and the metadata
  // reaches only the device that is finally awarded the ticket (§20.4).
  const offer = await issueJobOffer({ issuerSeed: ISSUER_SEED, ticket: reread, totalMeters: 3100 });
  assert.equal(offer.jobIdHex, issued.jobIdHex);
  assert.deepEqual(awardMatchesOffer(offer.offer, reread), { ok: true, reason: null });

  const updates = [];
  const follow = await customerThreads.follow(issued.link, { onUpdate: update => updates.push(update) });

  // → publishTicket: the host can read the details back off the run for
  // display, which is what a driver app renders at the door.
  const emitted = [];
  const run = await courierThreads.publishTicket(issued.bytes, { onPublish: e => emitted.push(e) });
  assert.equal(run.plan.stops[0].orderRef, ORDER_REF, "the publisher's plan still has it");
  assert.equal(run.plan.stops[0].parcels, 2);
  assert.equal(run.plan.stops[0].instructions, INSTRUCTIONS);
  assert.equal(run.plan.stops[0].contact, CONTACT);
  assert.equal(run.plan.stops[1].parcels, 0);

  for (let i = 0; i < route.geometry.length && updates.length < 3; i += Math.ceil(route.geometry.length / 6)) {
    const [lat, lon] = route.geometry[i];
    now += 6000;
    await run.handleFix({ lat, lon, speedMps: 5, nowMillis: now });
    await flush();
  }
  now += 1000;
  await run.markStop(1, STOP_OUTCOME.DELIVERED);
  await until(() => updates.length >= 2 && follow.latest()?.lastOutcome != null);
  assert.ok(emitted.length >= 2, `the run published ${emitted.length} records`);

  // --- the privacy claim, asserted rather than argued -------------------
  //
  // A plan is driver-side only. The body carries the 8-byte planRef hash
  // and there is no field on the wire for any of these values, so this is
  // structural: no configuration turns it on and no UI bug leaks it.
  const needles = [ORDER_REF, CONTACT, INSTRUCTIONS, "porte 1", "porte 3"].map(utf8Bytes);
  const contains = (haystack, needle) => {
    outer: for (let i = 0; i + needle.length <= haystack.length; i++) {
      for (let j = 0; j < needle.length; j++) if (haystack[i + j] !== needle[j]) continue outer;
      return true;
    }
    return false;
  };

  for (const record of emitted) {
    // The signed *plaintext* body, not just the sealed record: the point
    // is that the value is never encoded, not that it is encrypted.
    const plaintext = encodeThreadBodyPreimage(record.body);
    for (const needle of needles) {
      assert.equal(contains(plaintext, needle), false, "no stop detail is in the signed body");
      assert.equal(contains(record.bytes, needle), false, "and none is in the record on the wire");
    }
    assert.deepEqual(record.body.planRef, planRefOf(planBytes), "only the 8-byte ref binds to the plan");
    assert.equal(record.body.planRef.length, 8);
    for (const field of ["orderRef", "parcels", "instructions", "contact", "label", "stops"]) {
      assert.equal(field in record.body, false, `a body has no ${field} field at all`);
    }
  }

  // The same claim for the broadcast artifact, which is the one that goes
  // to strangers rather than to one enrolled device (§20.4).
  for (const needle of needles) {
    assert.equal(contains(offer.bytes, needle), false, "no stop detail is in the public offer either");
  }

  // What the follower actually holds: the ref, and nothing behind it.
  const latest = follow.latest();
  assert.deepEqual(latest.planRef, planRefOf(planBytes));
  for (const field of ["orderRef", "parcels", "instructions", "contact"]) {
    assert.equal(latest[field], undefined, `a subscriber's record has no ${field}`);
  }
  for (const update of follow.history()) {
    assert.equal(contains(update.bytes ?? new Uint8Array(0), needles[0]), false);
    assert.equal(update.orderRef, undefined);
    assert.equal(update.contact, undefined);
  }
  // The outcome still gets through — it is the run's business, and it is
  // on the wire by design (§5.2.1).
  assert.equal(latest.lastOutcome.outcome, STOP_OUTCOME.DELIVERED);

  follow.stop();
  customerThreads.close();
  courierThreads.close();
});

test("what a stop's details cost, in bytes the encoder actually produces", async () => {
  // The metadata columns of the §20.8 size table, asserted against the
  // encoder rather than computed by hand, which is the point: a doc
  // table nobody measures drifts from the code within one change.
  //
  // The table's *QR* column lives in pulsemesh_sealed_ticket.test.js,
  // because since §20.9 what goes in a QR is the sealed envelope and
  // never these bytes — measuring the plaintext artifact against a
  // scannability policy would be measuring something that no longer
  // travels.
  const NONE = {};
  const TYPICAL = { orderRef: "4471", parcels: 2 };
  const MAX = {
    orderRef: "x".repeat(THREAD_MAX_ORDER_REF_BYTES),
    parcels: 65535,
    instructions: "y".repeat(THREAD_MAX_INSTRUCTIONS_BYTES),
    contact: "z".repeat(THREAD_MAX_CONTACT_BYTES)
  };

  // The metadata's own cost, isolated: one stop with it, minus the same
  // stop without, plus the flags byte the bare stop already spends.
  const bare = encodeThreadPlan({ stops: [referenceStop(0)] }).length;
  const metadataCost = extra =>
    (encodeThreadPlan({ stops: [referenceStop(0, extra)] }).length - bare) + 1;
  assert.equal(metadataCost(NONE), 1, "an address-only stop spends one byte on the flags");
  assert.equal(metadataCost(TYPICAL), 7, "order \"4471\" and 2 parcels: 1 flags + 1+4 + 1");
  assert.equal(metadataCost(MAX), 119, "all four at their caps: 1 + 25 + 3 + 65 + 25");
  assert.equal(metadataCost({ parcels: 0 }), 2, "and a stated 0 costs a byte, which is why it fits");

  // The marginal cost of one more stop, which is what a round's size is.
  const marginal = extra =>
    encodeThreadPlan({ stops: [referenceStop(0, extra), referenceStop(1, extra)] }).length
    - encodeThreadPlan({ stops: [referenceStop(0, extra)] }).length;
  assert.equal(marginal(NONE), 37, "the reference stop, whole");
  assert.equal(marginal(TYPICAL), 43);
  assert.equal(marginal(MAX), 155);

  // A hundred-drop day, which never fitted a camera and never will: the
  // same signed bytes, hard-wrapped by a mail client, still decode and
  // still verify (§20.5's file carrier, now sealed by §20.9).
  const stopsFor = (count, extra) =>
    Array.from({ length: count }, (unused, i) => referenceStop(i, extra));
  const day = await ticketOf(stopsFor(100, TYPICAL));
  assert.equal(fitsScannableQr(day.base64url), false, "a day is far past any scannable symbol");
  const asFile = `wayfind://ticket#${day.base64url}\n`;
  const decoded = decodeThreadTicket(asFile.slice(asFile.lastIndexOf("#") + 1).replace(/\s+/gu, ""));
  assert.equal(decoded.plan.stops.length, 100);
  assert.equal(decoded.plan.stops[99].orderRef, "4471");
  assert.equal(decoded.plan.stops[99].parcels, 2);
  assert.deepEqual(
    await verifyThreadTicket(decoded, { epochPrefix8: EPOCH32.subarray(0, 8) }),
    { ok: true }
  );
});
