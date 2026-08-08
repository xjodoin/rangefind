// Job offers (threads §20.4): what a dispatcher may broadcast to people
// it has never met.
//
// The claim under test is a privacy claim and it is asserted by scanning
// bytes rather than by reading the encoder: an offer goes to a *pool* of
// couriers, most of whom will not win, so anything in it is disclosed to
// strangers for free. The rule is that it contains a commitment and
// coarse descriptors — never an address, never a label, never a byte of
// §20.8 metadata — and that the commitment is strong enough that the
// winner's device can refuse a job that is not the one advertised.

import assert from "node:assert/strict";
import test from "node:test";
import { THREAD_MODE, THREAD_TRAVEL_MODE } from "../src/pulsemesh/thread_codec.js";
import { fromHex, sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";
import { publicKeyFromSeed, signThread } from "../src/pulsemesh/thread_crypto.js";
import { utf8Bytes } from "../src/pulsemesh/codec.js";
import { encodeQr } from "../src/qr.js";
import {
  OFFER_GRID_E7,
  OFFER_SPREAD,
  THREAD_MAX_OFFER_LABEL_BYTES,
  awardMatchesOffer,
  classifyThreadArtifact,
  decodeJobOffer,
  encodeThreadPlan,
  issueJobOffer,
  issueTicket,
  jobOfferUrl,
  planRefOf,
  verifyJobOffer
} from "../src/pulsemesh/thread_ticket.js";

const EPOCH32 = fromHex("f44796c8cc1f3fa797104e925812ff052717f3052b5dbcadb0a36db776e0a4d1");
const OTHER_EPOCH32 = fromHex("1111111111111111111111111111111111111111111111111111111111111111");
const ISSUER_SEED = sha256Utf8("pulsemesh-test-dispatcher");
const OTHER_ISSUER_SEED = sha256Utf8("pulsemesh-test-other-dispatcher");

// Deliberately distinctive UTF-8: an accented label, an order reference
// nothing else could produce, an instruction and a phone number. If any
// of these turns up in an offer, a byte scan finds it.
const ORDER_REF = "CMD-Ω-4471";
const CONTACT = "+1 514 555 0134";
const INSTRUCTIONS = "portail latéral, chez le voisin";
const LABEL_A = "Épicerie Şükran";
const LABEL_B = "3e étage, sonner";

const ROUND = {
  dwellSeconds: 90,
  travelMode: THREAD_TRAVEL_MODE.BIKE,
  stops: [
    {
      lat: 45.501947, lon: -73.567413, label: LABEL_A,
      orderRef: ORDER_REF, parcels: 2, instructions: INSTRUCTIONS, contact: CONTACT
    },
    { lat: 45.508831, lon: -73.554029, label: LABEL_B, parcels: 0 },
    { lat: 45.516204, lon: -73.575112, label: "Dépôt Villeray" }
  ]
};

function contains(haystack, needle) {
  outer: for (let i = 0; i + needle.length <= haystack.length; i++) {
    for (let j = 0; j < needle.length; j++) if (haystack[i + j] !== needle[j]) continue outer;
    return true;
  }
  return false;
}

function concat(...parts) {
  const out = new Uint8Array(parts.reduce((n, part) => n + part.length, 0));
  let at = 0;
  for (const part of parts) { out.set(part, at); at += part.length; }
  return out;
}

const soon = () => Math.floor(Date.now() / 1000) + 3600;

async function offerFor(plan, extra = {}) {
  return issueJobOffer({
    issuerSeed: ISSUER_SEED,
    epoch32: EPOCH32,
    plan,
    notAfter: soon(),
    mode: THREAD_MODE.FINE,
    totalMeters: 8123,
    ...extra
  });
}

// --- 1. the privacy claim, byte-scanned ------------------------------------

test("a broadcast offer carries no address, no label and no §20.8 metadata", async () => {
  const issued = await offerFor(ROUND, { label: "12 parcels, city centre", payMinor: 2450, currency: "CAD" });
  const planBytes = encodeThreadPlan(ROUND);

  // The needles are in the plan — that is the control. If the plan did
  // not contain them the scan below would pass for the wrong reason.
  for (const needle of [ORDER_REF, CONTACT, INSTRUCTIONS, LABEL_A, LABEL_B].map(utf8Bytes)) {
    assert.equal(contains(planBytes, needle), true, "the plan does carry it");
    assert.equal(contains(issued.bytes, needle), false, "and the offer does not");
    assert.equal(contains(utf8Bytes(issued.base64url), needle), false);
  }

  // No coordinate survives either. The centroid is a *gridded* point, so
  // it lands on a multiple of 0.01° and is not any stop's own position:
  // there is no arithmetic from an offer back to a door.
  const { centroid } = issued.offer;
  assert.equal(Math.abs(centroid.latE7 % OFFER_GRID_E7), 0, "the centroid sits on the grid");
  assert.equal(Math.abs(centroid.lonE7 % OFFER_GRID_E7), 0);
  assert.equal(centroid.gridDegrees, 0.01);
  for (const stop of ROUND.stops) {
    const latE7 = Math.round(stop.lat * 1e7);
    const lonE7 = Math.round(stop.lon * 1e7);
    assert.notEqual(centroid.latE7, latE7, "the centroid is not a stop");
    assert.notEqual(centroid.lonE7, lonE7);
    assert.equal(contains(issued.bytes, utf8Bytes(String(latE7))), false);
  }
  // It is the centroid, not the first stop: an origin is somebody's
  // doorstep, and publishing it would be the leak with extra steps.
  const meanLat = ROUND.stops.reduce((sum, stop) => sum + stop.lat, 0) / ROUND.stops.length;
  assert.ok(Math.abs(centroid.lat - meanLat) <= 0.01, "the point published is the mean of the stops");
  assert.notEqual(centroid.lat, Math.round(ROUND.stops[0].lat * 100) / 100);

  // What a bidder *does* learn, in full — this list is the doc table.
  assert.equal(issued.offer.stopCount, 3);
  assert.equal(issued.offer.totalMeters, 8123);
  assert.equal(issued.offer.travelMode, THREAD_TRAVEL_MODE.BIKE);
  assert.equal(issued.offer.mode, THREAD_MODE.FINE);
  assert.equal(issued.offer.payMinor, 2450);
  assert.equal(issued.offer.currency, "CAD");
  assert.equal(issued.offer.label, "12 parcels, city centre");
  assert.deepEqual(issued.offer.planRef, planRefOf(planBytes));
  assert.equal(issued.offer.jobIdHex.length, 32);

  // And it is its own kind of artifact, so a host cannot route it to the
  // driver's accept screen by accident.
  assert.deepEqual(
    classifyThreadArtifact(jobOfferUrl(issued.bytes)),
    { kind: "offer", reason: null, sealed: false }
  );
});

test("the spread bucket says how far the round ranges, and nothing sharper", async () => {
  // One stop pair per bucket edge, walked out from a fixed centre. The
  // buckets are coarse on purpose: an exact radius around an exact
  // centroid is a disc, and two offers from one depot trilaterate it.
  const at = (dLat) => ({
    travelMode: THREAD_TRAVEL_MODE.CAR,
    stops: [{ lat: 45.5 - dLat, lon: -73.6 }, { lat: 45.5 + dLat, lon: -73.6 }]
  });
  const buckets = [];
  for (const dLat of [0.002, 0.02, 0.05, 0.2, 0.6]) {
    buckets.push((await offerFor(at(dLat))).offer.spread);
  }
  assert.deepEqual(buckets, [0, 1, 2, 3, 4], "five buckets, edges at 1 / 3 / 10 / 30 km");
  assert.equal(OFFER_SPREAD.length, 5);
  assert.deepEqual(OFFER_SPREAD.map(entry => entry.maxMeters), [1000, 3000, 10000, 30000, null]);
  assert.match((await offerFor(at(0.002))).offer.spreadLabel, /1 km/u);
});

// --- 2. round-trip and signature -------------------------------------------

test("an offer round-trips, and a tampered or foreign one is refused", async () => {
  const epochPrefix8 = EPOCH32.subarray(0, 8);
  const issued = await offerFor(ROUND, { label: "12 parcels, city centre", payMinor: 2450, currency: "CAD" });

  const reread = decodeJobOffer(issued.base64url);
  assert.deepEqual(reread.bytes, issued.bytes);
  assert.deepEqual(reread.jobId, issued.offer.jobId);
  assert.deepEqual(reread.planRef, issued.offer.planRef);
  assert.equal(reread.label, "12 parcels, city centre");
  assert.deepEqual(await verifyJobOffer(issued.bytes, { epochPrefix8 }), { ok: true });

  // Signed over every field, so nothing in an offer is malleable: move a
  // byte of the advertised distance and the signature stops verifying.
  const tampered = Uint8Array.from(issued.bytes);
  tampered[tampered.length - 70] ^= 0x08;
  const bad = await verifyJobOffer(tampered, { epochPrefix8 });
  assert.equal(bad.ok, false);
  assert.match(bad.reason, /signature does not verify/u);

  // A stranger's offer verifies as *their* offer and is refused the
  // moment a courier pins the issuer it works for.
  const stranger = await issueJobOffer({
    issuerSeed: OTHER_ISSUER_SEED, epoch32: EPOCH32, plan: ROUND, notAfter: soon(), totalMeters: 8123
  });
  assert.deepEqual(await verifyJobOffer(stranger.bytes, { epochPrefix8 }), { ok: true });
  const pinned = toHex(await publicKeyFromSeed(ISSUER_SEED));
  const refused = await verifyJobOffer(stranger.bytes, { epochPrefix8, expectedIssuerHex: pinned });
  assert.equal(refused.ok, false);
  assert.match(refused.reason, /different issuer/u);

  const wrongEpoch = await verifyJobOffer(issued.bytes, { epochPrefix8: OTHER_EPOCH32.subarray(0, 8) });
  assert.match(wrongEpoch.reason, /different graph epoch/u);

  const stale = await offerFor(ROUND, { notAfter: Math.floor(Date.now() / 1000) - 10 });
  assert.match((await verifyJobOffer(stale.bytes, { epochPrefix8 })).reason, /expired/u);

  // Trailing bytes are refused here as everywhere else on this channel.
  assert.throws(() => decodeJobOffer(concat(issued.bytes, Uint8Array.of(0))), /trailing bytes/u);
});

// --- 3. the anti-bait-and-switch check -------------------------------------

// Field offsets inside a PMJ1 preimage, used to build the forgery a
// dishonest dispatcher would actually build. Layout: magic(4) version(1)
// mode(1) travelMode(1) epochPrefix8(8) notAfter(4) jobId(16) planRef(8).
const JOB_ID_AT = 19;

async function forgeOffer(bytes, mutate) {
  const forged = Uint8Array.from(bytes);
  mutate(forged);
  // Re-signed with the issuer's own key, because the issuer *is* the
  // attacker here: the check has to survive a valid signature.
  const preimage = forged.subarray(0, forged.length - 64);
  const signature = await signThread(concat(utf8Bytes("pulsemesh/offer/1"), preimage), ISSUER_SEED);
  return concat(preimage, signature);
}

test("an award is refused unless it is the job that was advertised", async () => {
  const notAfter = soon();
  const advertised = await offerFor(ROUND, { notAfter });
  const honest = await issueTicket({
    issuerSeed: ISSUER_SEED, epoch32: EPOCH32, plan: ROUND, notAfter, mode: THREAD_MODE.FINE
  });
  assert.deepEqual(awardMatchesOffer(advertised.offer, honest.ticket), { ok: true, reason: null });
  assert.deepEqual(awardMatchesOffer(advertised.bytes, honest.bytes), { ok: true, reason: null });

  // The bait and switch: "3 stops downtown", then thirty across the
  // province. `planRef` is a hash of the plan bytes, so this is
  // arithmetic rather than an argument the courier has to win.
  const province = {
    travelMode: THREAD_TRAVEL_MODE.CAR,
    stops: Array.from({ length: 30 }, (_, i) => ({ lat: 46 + (i * 0.1), lon: -71 - (i * 0.1) }))
  };
  const switched = await issueTicket({
    issuerSeed: ISSUER_SEED, epoch32: EPOCH32, plan: province, notAfter, mode: THREAD_MODE.FINE
  });
  const caught = awardMatchesOffer(advertised.offer, switched.ticket);
  assert.equal(caught.ok, false);
  assert.match(caught.reason, /planRef/u);

  // …and the same, with the dispatcher advertising the *awarded* job's
  // `jobId` so that a courier checking only the join key would be
  // satisfied. The plan commitment still catches it.
  const baited = await forgeOffer(advertised.bytes, forged => {
    forged.set(fromHex(switched.jobIdHex), JOB_ID_AT);
  });
  assert.deepEqual(await verifyJobOffer(baited), { ok: true }, "a dispatcher can sign its own forgery");
  assert.deepEqual(decodeJobOffer(baited).jobIdHex, switched.jobIdHex);
  const stillCaught = awardMatchesOffer(baited, switched.ticket);
  assert.equal(stillCaught.ok, false);
  assert.match(stillCaught.reason, /planRef/u);

  // Wrong issuer: the offer everyone saw was signed by one dispatcher and
  // the ticket arrived from another.
  const elsewhere = await issueTicket({
    issuerSeed: OTHER_ISSUER_SEED, epoch32: EPOCH32, plan: ROUND, notAfter, mode: THREAD_MODE.FINE
  });
  const wrongIssuer = awardMatchesOffer(advertised.offer, elsewhere.ticket);
  assert.equal(wrongIssuer.ok, false);
  assert.match(wrongIssuer.reason, /different issuer/u);

  // Wrong epoch: same plan, pointed at another graph.
  const otherGraph = await issueTicket({
    issuerSeed: ISSUER_SEED, epoch32: OTHER_EPOCH32, plan: ROUND, notAfter, mode: THREAD_MODE.FINE
  });
  const wrongEpoch = awardMatchesOffer(advertised.offer, otherGraph.ticket);
  assert.equal(wrongEpoch.ok, false);
  assert.match(wrongEpoch.reason, /different graph epoch/u);

  // A longer window than was advertised: the courier bid on a job that
  // ends at six and is being handed one that publishes them until ten.
  const longer = await issueTicket({
    issuerSeed: ISSUER_SEED, epoch32: EPOCH32, plan: ROUND, notAfter: notAfter + 4 * 3600
  });
  const overrun = awardMatchesOffer(advertised.offer, longer.ticket);
  assert.equal(overrun.ok, false);
  assert.match(overrun.reason, /notAfter/u);

  // Shorter is fine — nobody is harmed by being released early — but it
  // is a different `jobId`, and that is the check that names it.
  const shorter = await issueTicket({
    issuerSeed: ISSUER_SEED, epoch32: EPOCH32, plan: ROUND, notAfter: notAfter - 600
  });
  const rekeyed = awardMatchesOffer(advertised.offer, shorter.ticket);
  assert.equal(rekeyed.ok, false);
  assert.match(rekeyed.reason, /jobId/u);
});

// --- 4. flags -------------------------------------------------------------

test("an offer's optional fields round-trip, and a reserved flag bit is refused", async () => {
  const bare = await offerFor(ROUND);
  assert.equal(bare.offer.payMinor, null, "unstated pay is null, never 0");
  assert.equal(bare.offer.currency, null);
  assert.equal(bare.offer.label, null);

  const priced = await offerFor(ROUND, { payMinor: 0, currency: "eur" });
  assert.equal(priced.offer.payMinor, 0, "a stated 0 survives — an unpaid run is a statement");
  assert.equal(priced.offer.currency, "EUR", "and the code is normalized on the way in");

  await assert.rejects(() => offerFor(ROUND, { payMinor: 900, currency: "dollars" }), /ISO 4217/u);
  await assert.rejects(() => offerFor(ROUND, { currency: "CAD" }), /without a payMinor/u);
  await assert.rejects(() => offerFor(ROUND, { payMinor: 1.5, currency: "CAD" }), /minor units/u);
  await assert.rejects(
    () => offerFor(ROUND, { label: "x".repeat(THREAD_MAX_OFFER_LABEL_BYTES + 1) }),
    /at most 48 bytes/u
  );

  // Reserved bits throw rather than being skipped, exactly as PMK1's and
  // PMP1's do: an offer whose writer meant a term this reader cannot see
  // is an offer whose terms this reader does not know.
  const flagsAt = bare.bytes.length - 64 - 32 - 1;
  const reserved = Uint8Array.from(bare.bytes);
  reserved[flagsAt] |= 0x04;
  assert.throws(() => decodeJobOffer(reserved), /PMJ1 reserved flag bits/u);

  // A label flag with nothing behind it is a second encoding of one
  // offer, and the signature is over these bytes.
  const empty = Uint8Array.from(concat(bare.bytes.subarray(0, flagsAt + 1), Uint8Array.of(0), bare.bytes.subarray(flagsAt + 1)));
  empty[flagsAt] |= 0x02;
  assert.throws(() => decodeJobOffer(empty), /no label behind it/u);
});

// --- 5. size --------------------------------------------------------------

test("an offer fits a QR a phone can actually scan", async () => {
  // The ceiling is the camera, not the encoder (§20.5): past about
  // version 25 the modules are finer than a hand-held scan resolves. An
  // offer is a fixed-size artifact — it holds no plan — so unlike a
  // ticket it can never outgrow this, whatever the round looks like.
  const optional = { payMinor: 2450, currency: "CAD", label: "12 parcels, city centre" };
  const bare = await offerFor(ROUND);
  const full = await offerFor(ROUND, optional);
  const day = await offerFor({
    travelMode: THREAD_TRAVEL_MODE.CAR,
    stops: Array.from({ length: 120 }, (_, i) => ({
      lat: 45.5 + (i * 0.001), lon: -73.6 - (i * 0.001), label: `stop ${i}`, contact: CONTACT
    }))
  }, optional);

  assert.equal(bare.bytes.length, 148, "a bare offer is 148 bytes");
  assert.equal(full.bytes.length, 177, "with pay and a 23-byte label, 177");
  // The claim that matters: an offer holds no plan, so its size is a
  // function of its own fields and not of the round. A 120-drop day
  // advertises in the same bytes a 3-drop one does.
  assert.equal(day.bytes.length, full.bytes.length, "120 stops cost the same as 3");
  assert.equal(day.offer.stopCount, 120);

  for (const issued of [bare, full, day]) {
    const qr = encodeQr(jobOfferUrl(issued.bytes), { ecLevel: "M", maxVersion: 25 });
    // Measured, not guessed: 148 B → v11, 177 B → v12, both as a
    // `wayfind://offer#…` URL at EC level M. Less than half the
    // scannability ceiling, with nothing in the round that can push it.
    assert.ok(qr.version <= 12, `an offer QR is version ${qr.version}`);
  }
});
