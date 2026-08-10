import assert from "node:assert/strict";
import test from "node:test";

import { THREAD_MODE } from "../src/pulsemesh/thread_codec.js";
import { sha256Utf8 } from "../src/pulsemesh/sha256.js";
import {
  SEAL_ERROR,
  generateDeviceKeypair,
  openSealedTicket
} from "../src/pulsemesh/thread_seal.js";
import { encodeThreadPlan, issueTicket } from "../src/pulsemesh/thread_ticket.js";
import {
  awardFirstJobClaim,
  createJobClaim,
  decodeJobClaim,
  issueMultiRecipientJobOffer,
  openMultiRecipientJobOffer,
  verifyAssignmentOffer,
  verifyJobClaim
} from "../src/pulsemesh/thread_assignment.js";

const ISSUER_SEED = sha256Utf8("pulsemesh-assignment-issuer");
const EPOCH_PREFIX8 = sha256Utf8("pulsemesh-assignment-epoch").subarray(0, 8);
const NOW = 1_800_000_000;
const PLAN = {
  travelMode: 1,
  stops: [
    { lat: 45.5, lon: -73.6, label: "private customer" },
    { lat: 45.6, lon: -73.5, label: "private destination" }
  ]
};

async function fixture() {
  const candidates = await Promise.all([
    generateDeviceKeypair(sha256Utf8("candidate-a")),
    generateDeviceKeypair(sha256Utf8("candidate-b")),
    generateDeviceKeypair(sha256Utf8("candidate-c"))
  ]);
  const ticket = await issueTicket({
    issuerSeed: ISSUER_SEED,
    epochPrefix8: EPOCH_PREFIX8,
    planBytes: encodeThreadPlan(PLAN),
    notAfter: NOW + 3600,
    mode: THREAD_MODE.FINE
  });
  const offered = await issueMultiRecipientJobOffer({
    issuerSeed: ISSUER_SEED,
    recipients: candidates.map(candidate => candidate.publicKey),
    ticket: ticket.ticket,
    claimNotAfter: NOW + 300,
    totalMeters: 9000,
    payMinor: 2200,
    currency: "CAD",
    label: "two-stop delivery"
  });
  return { candidates, ticket, offered };
}

test("one private seedless offer opens for every invited candidate and nobody else", async () => {
  const { candidates, offered } = await fixture();
  for (const candidate of candidates) {
    const opened = await openMultiRecipientJobOffer(offered.sealed, candidate.privateKey, { nowMillis: NOW * 1000 });
    assert.equal(opened.assignmentIdHex, offered.assignment.assignmentIdHex);
    assert.equal(opened.offer.payMinor, 2200);
    assert.equal(opened.offer.planBytes, undefined, "the candidate pool receives no private run plan");
  }

  const stranger = await generateDeviceKeypair(sha256Utf8("candidate-stranger"));
  await assert.rejects(
    openMultiRecipientJobOffer(offered.sealed, stranger.privateKey, { nowMillis: NOW * 1000 }),
    error => error.code === SEAL_ERROR.NOT_ENROLLED
  );
  await assert.rejects(
    createJobClaim(offered.assignment, stranger.privateKey, { claimedAt: NOW }),
    /not invited/u
  );
});

test("claims prove an invited device key and are bound to exactly one assignment", async () => {
  const { candidates, offered } = await fixture();
  const claim = await createJobClaim(offered.assignment, candidates[0].privateKey, {
    claimedAt: NOW,
    nonce: new Uint8Array(16).fill(7)
  });
  assert.equal((await verifyJobClaim(
    offered.assignment, claim, ISSUER_SEED, { nowMillis: NOW * 1000 }
  )).ok, true);

  const tampered = Uint8Array.from(claim.bytes);
  tampered[tampered.length - 1] ^= 1;
  assert.match(
    (await verifyJobClaim(offered.assignment, decodeJobClaim(tampered), ISSUER_SEED, {
      nowMillis: NOW * 1000
    })).reason,
    /authentication code/u
  );

  const other = await issueMultiRecipientJobOffer({
    issuerSeed: ISSUER_SEED,
    recipients: candidates.map(candidate => candidate.publicKey),
    ticket: (await fixture()).ticket.ticket,
    claimNotAfter: NOW + 300,
    nonce: new Uint8Array(16).fill(9)
  });
  assert.match(
    (await verifyJobClaim(other.assignment, claim, ISSUER_SEED, { nowMillis: NOW * 1000 })).reason,
    /different assignment/u
  );
});

test("the dispatcher atomically awards one publishing ticket to the first accepted claim", async () => {
  const { candidates, ticket, offered } = await fixture();
  const [first, second] = await Promise.all([
    createJobClaim(offered.assignment, candidates[0].privateKey, { claimedAt: NOW }),
    createJobClaim(offered.assignment, candidates[1].privateKey, { claimedAt: NOW - 1 })
  ]);
  const winners = new Map();
  const claimOnce = async (assignmentId, winner) => {
    if (winners.has(assignmentId)) return false;
    winners.set(assignmentId, winner);
    return true;
  };

  const awarded = await awardFirstJobClaim({
    assignment: offered.assignment,
    claim: first,
    ticket: ticket.ticket,
    issuerSeed: ISSUER_SEED,
    claimOnce,
    nowMillis: NOW * 1000
  });
  assert.equal(awarded.ok, true);
  assert.deepEqual(await openSealedTicket(awarded.sealed, candidates[0].privateKey), ticket.bytes);
  await assert.rejects(
    openSealedTicket(awarded.sealed, candidates[1].privateKey),
    error => error.code === SEAL_ERROR.NOT_ENROLLED
  );

  const lost = await awardFirstJobClaim({
    assignment: offered.assignment,
    claim: second,
    ticket: ticket.ticket,
    issuerSeed: ISSUER_SEED,
    claimOnce,
    nowMillis: NOW * 1000
  });
  assert.equal(lost.ok, false);
  assert.equal(lost.code, "assignment-already-awarded");
  assert.equal(winners.size, 1);
  assert.equal(winners.values().next().value.claimIdHex, first.claimIdHex,
    "receipt/CAS order wins; a claimant-controlled earlier timestamp does not");
});

test("assignment signatures and claim deadlines are enforced", async () => {
  const { offered } = await fixture();
  const tampered = Uint8Array.from(offered.bytes);
  tampered[tampered.length - 1] ^= 1;
  assert.match((await verifyAssignmentOffer(tampered, { nowMillis: NOW * 1000 })).reason, /signature/u);
  assert.match(
    (await verifyAssignmentOffer(offered.assignment, { nowMillis: (NOW + 301) * 1000 })).reason,
    /claim window has expired/u
  );
});
