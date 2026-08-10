// Multi-recipient job assignment without multi-recipient publish authority.
//
// PMA1 seals one signed, seedless PMJ1 offer to an explicit set of enrolled
// devices. PMQ1 lets each intended recipient prove possession of its X25519
// device key. The dispatcher awards the existing PMK1 ticket only after an
// application-supplied atomic insert accepts one claim. Candidate timestamps
// never decide the winner: clocks are not a consensus mechanism.

import { pushVarint, readVarint } from "../binary.js";
import { utf8Bytes } from "./codec.js";
import { sha256, toHex } from "./sha256.js";
import {
  equalThreadAuth,
  hkdfBytes,
  publicKeyFromSeed,
  signThread,
  uint32be,
  verifyThread,
  x25519PublicKey,
  x25519SharedSecret
} from "./thread_crypto.js";
import {
  SEAL_MAX_RECIPIENTS,
  openSealedTicket,
  sealTicket
} from "./thread_seal.js";
import {
  awardMatchesOffer,
  decodeJobOffer,
  decodeThreadTicket,
  issueJobOffer,
  verifyJobOffer,
  verifyThreadTicket
} from "./thread_ticket.js";

export const ASSIGNMENT_MAGIC = Object.freeze({ PMA1: "PMA1", PMQ1: "PMQ1" });
export const ASSIGNMENT_VERSION = 1;
export const ASSIGNMENT_RECIPIENT_COMMITMENT_BYTES = 16;
export const ASSIGNMENT_ID_BYTES = 32;
export const JOB_CLAIM_BYTES = 121;
export const ASSIGNMENT_MAX_OFFER_BYTES = 2048;

const ASSIGNMENT_SIGNATURE_DOMAIN = "pulsemesh/assignment/signature/1";
const ASSIGNMENT_ID_DOMAIN = "pulsemesh/assignment/id/1";
const ASSIGNMENT_RECIPIENT_DOMAIN = "pulsemesh/assignment/recipient/1";
const ASSIGNMENT_PRIVATE_DOMAIN = "pulsemesh/assignment/claim-private/1";
const CLAIM_KEY_DOMAIN = "pulsemesh/assignment/claim-key/1";
const CLAIM_MAC_DOMAIN = "pulsemesh/assignment/claim-mac/1";

function subtle() {
  const api = globalThis.crypto?.subtle;
  if (!api) throw new Error("PulseMesh assignments require WebCrypto (globalThis.crypto.subtle).");
  return api;
}

function randomBytes(length) {
  return globalThis.crypto.getRandomValues(new Uint8Array(length));
}

function concat(...parts) {
  const out = new Uint8Array(parts.reduce((sum, part) => sum + part.length, 0));
  let offset = 0;
  for (const part of parts) { out.set(part, offset); offset += part.length; }
  return out;
}

function pushMagic(out, magic) {
  for (let i = 0; i < 4; i++) out.push(magic.charCodeAt(i));
}

function pushBytes(out, bytes) {
  for (const byte of bytes) out.push(byte);
}

function readBytes(bytes, state, length, what = "Assignment") {
  if (state.pos + length > bytes.length) throw new Error(`${what} is truncated.`);
  const value = bytes.subarray(state.pos, state.pos + length);
  state.pos += length;
  return value;
}

function readU32(bytes, state) {
  const value = readBytes(bytes, state, 4);
  return (value[0] * 2 ** 24) + (value[1] << 16) + (value[2] << 8) + value[3];
}

function assertNoTrailing(bytes, state, what) {
  if (state.pos !== bytes.length) throw new Error(`${what} has trailing bytes.`);
}

function normalizeRecipients(recipients) {
  const values = (recipients || []).map(value => value?.publicKey ?? value);
  if (!values.length || values.length > SEAL_MAX_RECIPIENTS) {
    throw new Error(`An assignment needs 1..${SEAL_MAX_RECIPIENTS} enrolled recipients.`);
  }
  for (const value of values) {
    if (!(value instanceof Uint8Array) || value.length !== 32) {
      throw new Error("An assignment recipient public key is 32 bytes.");
    }
  }
  const unique = new Map(values.map(value => [toHex(value), value]));
  if (unique.size !== values.length) throw new Error("An assignment recipient appears more than once.");
  return [...unique.values()];
}

function recipientCommitment(publicKey) {
  return sha256(concat(utf8Bytes(ASSIGNMENT_RECIPIENT_DOMAIN), publicKey))
    .subarray(0, ASSIGNMENT_RECIPIENT_COMMITMENT_BYTES);
}

function compareBytes(left, right) {
  for (let i = 0; i < Math.min(left.length, right.length); i++) {
    if (left[i] !== right[i]) return left[i] - right[i];
  }
  return left.length - right.length;
}

async function issuerClaimPrivateKey(issuerSeed) {
  return hkdfBytes(issuerSeed, utf8Bytes(ASSIGNMENT_PRIVATE_DOMAIN), 32);
}

function assignmentSigningMessage(preimage) {
  return concat(utf8Bytes(ASSIGNMENT_SIGNATURE_DOMAIN), preimage);
}

function assignmentIdOfBytes(bytes) {
  return sha256(concat(utf8Bytes(ASSIGNMENT_ID_DOMAIN), bytes));
}

function encodeAssignmentPreimage({ notAfter, nonce, claimPublicKey, recipientCommitments, offerBytes }) {
  if (!Number.isInteger(notAfter) || notAfter < 0 || notAfter > 0xffffffff) {
    throw new Error("An assignment notAfter is a uint32 unix timestamp.");
  }
  if (nonce?.length !== 16) throw new Error("An assignment nonce is 16 bytes.");
  if (claimPublicKey?.length !== 32) throw new Error("An assignment claim public key is 32 bytes.");
  if (!offerBytes?.length || offerBytes.length > ASSIGNMENT_MAX_OFFER_BYTES) {
    throw new Error(`An assignment offer is 1..${ASSIGNMENT_MAX_OFFER_BYTES} bytes.`);
  }
  const out = [];
  pushMagic(out, ASSIGNMENT_MAGIC.PMA1);
  out.push(ASSIGNMENT_VERSION);
  pushBytes(out, uint32be(notAfter));
  pushBytes(out, nonce);
  pushBytes(out, claimPublicKey);
  pushVarint(out, recipientCommitments.length);
  for (const commitment of recipientCommitments) pushBytes(out, commitment);
  pushVarint(out, offerBytes.length);
  pushBytes(out, offerBytes);
  return Uint8Array.from(out);
}

export function decodeAssignmentOffer(bytes) {
  if (!(bytes instanceof Uint8Array)) throw new Error("A PMA1 assignment is bytes.");
  const state = { pos: 0 };
  const magic = String.fromCharCode(...readBytes(bytes, state, 4));
  if (magic !== ASSIGNMENT_MAGIC.PMA1) throw new Error(`Expected PMA1, found ${JSON.stringify(magic)}.`);
  const version = readBytes(bytes, state, 1)[0];
  if (version !== ASSIGNMENT_VERSION) throw new Error(`Unsupported PMA1 version ${version}.`);
  const notAfter = readU32(bytes, state);
  const nonce = readBytes(bytes, state, 16);
  const claimPublicKey = readBytes(bytes, state, 32);
  const recipientCount = readVarint(bytes, state);
  if (!recipientCount || recipientCount > SEAL_MAX_RECIPIENTS) {
    throw new Error(`A PMA1 assignment carries 1..${SEAL_MAX_RECIPIENTS} recipient commitments.`);
  }
  const recipientCommitments = [];
  for (let i = 0; i < recipientCount; i++) {
    const commitment = readBytes(bytes, state, ASSIGNMENT_RECIPIENT_COMMITMENT_BYTES);
    if (i && compareBytes(recipientCommitments[i - 1], commitment) >= 0) {
      throw new Error("PMA1 recipient commitments must be unique and sorted.");
    }
    recipientCommitments.push(commitment);
  }
  const offerLength = readVarint(bytes, state);
  if (!offerLength || offerLength > ASSIGNMENT_MAX_OFFER_BYTES) {
    throw new Error(`A PMA1 offer is 1..${ASSIGNMENT_MAX_OFFER_BYTES} bytes.`);
  }
  const offerBytes = readBytes(bytes, state, offerLength);
  const preimageEnd = state.pos;
  const signature = readBytes(bytes, state, 64);
  assertNoTrailing(bytes, state, "PMA1 assignment");
  const offer = decodeJobOffer(offerBytes);
  return {
    version, notAfter, nonce, claimPublicKey, recipientCommitments,
    offerBytes, offer, signature, preimage: bytes.subarray(0, preimageEnd), bytes,
    assignmentId: assignmentIdOfBytes(bytes),
    assignmentIdHex: toHex(assignmentIdOfBytes(bytes))
  };
}

function asAssignment(value) {
  return value?.offer && value?.preimage ? value : decodeAssignmentOffer(value);
}

/** Creates one private, seedless offer for an explicit candidate set. */
export async function issueMultiRecipientJobOffer({
  recipients,
  issuerSeed,
  claimNotAfter = null,
  nonce = null,
  ...offerFields
} = {}) {
  if (issuerSeed?.length !== 32) throw new Error("An assignment issuer seed is 32 bytes.");
  const recipientKeys = normalizeRecipients(recipients);
  const issuedOffer = await issueJobOffer({ issuerSeed, ...offerFields });
  const notAfter = claimNotAfter ?? issuedOffer.offer.notAfter;
  if (!Number.isInteger(notAfter) || notAfter > issuedOffer.offer.notAfter) {
    throw new Error("An assignment claim deadline cannot outlive its job offer.");
  }
  const claimPrivateKey = await issuerClaimPrivateKey(issuerSeed);
  const claimPublicKey = await x25519PublicKey(claimPrivateKey);
  const recipientCommitments = recipientKeys.map(recipientCommitment).sort(compareBytes);
  const preimage = encodeAssignmentPreimage({
    notAfter,
    nonce: nonce ?? randomBytes(16),
    claimPublicKey,
    recipientCommitments,
    offerBytes: issuedOffer.bytes
  });
  const signature = await signThread(assignmentSigningMessage(preimage), issuerSeed);
  const bytes = concat(preimage, signature);
  const assignment = decodeAssignmentOffer(bytes);
  return {
    assignment,
    offer: assignment.offer,
    bytes,
    sealed: await sealTicket(bytes, recipientKeys)
  };
}

/** Verifies both the assignment wrapper and its signed seedless offer. */
export async function verifyAssignmentOffer(value, options = {}) {
  let assignment;
  try { assignment = asAssignment(value); } catch (error) { return { ok: false, reason: error.message }; }
  if (!(await verifyThread(
    assignmentSigningMessage(assignment.preimage),
    assignment.signature,
    assignment.offer.issuerPublicKey
  ))) return { ok: false, reason: "the assignment signature does not verify" };
  if (assignment.notAfter > assignment.offer.notAfter) {
    return { ok: false, reason: "the assignment outlives its job offer" };
  }
  const offerVerdict = await verifyJobOffer(assignment.offer, options);
  if (!offerVerdict.ok) return offerVerdict;
  if (Math.floor((options.nowMillis ?? Date.now()) / 1000) > assignment.notAfter) {
    return { ok: false, reason: "the assignment claim window has expired" };
  }
  return { ok: true, assignment };
}

/** Opens a candidate's PME1 envelope, then authenticates PMA1 and PMJ1. */
export async function openMultiRecipientJobOffer(sealed, devicePrivateKey, options = {}) {
  const assignment = decodeAssignmentOffer(await openSealedTicket(sealed, devicePrivateKey));
  const verdict = await verifyAssignmentOffer(assignment, options);
  if (!verdict.ok) throw new Error(verdict.reason);
  return assignment;
}

function claimPreimage({ assignmentId, devicePublicKey, claimedAt, nonce }) {
  const out = [];
  pushMagic(out, ASSIGNMENT_MAGIC.PMQ1);
  out.push(ASSIGNMENT_VERSION);
  pushBytes(out, assignmentId);
  pushBytes(out, devicePublicKey);
  pushBytes(out, uint32be(claimedAt));
  pushBytes(out, nonce);
  return Uint8Array.from(out);
}

async function claimKey(sharedSecret, assignmentId) {
  return hkdfBytes(sharedSecret, concat(utf8Bytes(CLAIM_KEY_DOMAIN), assignmentId), 32);
}

async function claimMac(keyBytes, preimage) {
  const key = await subtle().importKey("raw", keyBytes, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return new Uint8Array(await subtle().sign("HMAC", key, concat(utf8Bytes(CLAIM_MAC_DOMAIN), preimage)));
}

export function decodeJobClaim(bytes) {
  if (!(bytes instanceof Uint8Array) || bytes.length !== JOB_CLAIM_BYTES) {
    throw new Error(`A PMQ1 job claim is exactly ${JOB_CLAIM_BYTES} bytes.`);
  }
  const state = { pos: 0 };
  const magic = String.fromCharCode(...readBytes(bytes, state, 4));
  if (magic !== ASSIGNMENT_MAGIC.PMQ1) throw new Error(`Expected PMQ1, found ${JSON.stringify(magic)}.`);
  const version = readBytes(bytes, state, 1)[0];
  if (version !== ASSIGNMENT_VERSION) throw new Error(`Unsupported PMQ1 version ${version}.`);
  const assignmentId = readBytes(bytes, state, 32);
  const devicePublicKey = readBytes(bytes, state, 32);
  const claimedAt = readU32(bytes, state);
  const nonce = readBytes(bytes, state, 16);
  const preimageEnd = state.pos;
  const mac = readBytes(bytes, state, 32);
  assertNoTrailing(bytes, state, "PMQ1 claim");
  return {
    version, assignmentId, assignmentIdHex: toHex(assignmentId), devicePublicKey,
    claimedAt, nonce, mac, preimage: bytes.subarray(0, preimageEnd), bytes,
    claimIdHex: toHex(sha256(bytes))
  };
}

/** Creates a claim proving that an intended recipient controls its device key. */
export async function createJobClaim(value, devicePrivateKey, {
  claimedAt = Math.floor(Date.now() / 1000),
  nonce = null
} = {}) {
  const assignment = asAssignment(value);
  if (devicePrivateKey?.length !== 32) throw new Error("A claimant device private key is 32 bytes.");
  if (!Number.isInteger(claimedAt) || claimedAt < 0 || claimedAt > 0xffffffff) {
    throw new Error("A claim timestamp is a uint32 unix timestamp.");
  }
  const devicePublicKey = await x25519PublicKey(devicePrivateKey);
  const commitment = recipientCommitment(devicePublicKey);
  if (!assignment.recipientCommitments.some(held => equalThreadAuth(held, commitment))) {
    throw new Error("This device was not invited to claim the assignment.");
  }
  const preimage = claimPreimage({
    assignmentId: assignment.assignmentId,
    devicePublicKey,
    claimedAt,
    nonce: nonce ?? randomBytes(16)
  });
  const shared = await x25519SharedSecret(devicePrivateKey, assignment.claimPublicKey);
  const mac = await claimMac(await claimKey(shared, assignment.assignmentId), preimage);
  return decodeJobClaim(concat(preimage, mac));
}

/** Authenticates membership and key possession; receipt order is not decided here. */
export async function verifyJobClaim(value, claimValue, issuerSeed, {
  nowMillis = Date.now(),
  clockSkewSeconds = 300,
  ...offerOptions
} = {}) {
  const assignmentVerdict = await verifyAssignmentOffer(value, { nowMillis, ...offerOptions });
  if (!assignmentVerdict.ok) return assignmentVerdict;
  const assignment = assignmentVerdict.assignment;
  let claim;
  try { claim = claimValue?.preimage ? claimValue : decodeJobClaim(claimValue); }
  catch (error) { return { ok: false, reason: error.message }; }
  if (issuerSeed?.length !== 32) return { ok: false, reason: "an assignment issuer seed is required" };
  if (!equalThreadAuth(await publicKeyFromSeed(issuerSeed), assignment.offer.issuerPublicKey)) {
    return { ok: false, reason: "the claim was presented to a different assignment issuer" };
  }
  const issuerPrivate = await issuerClaimPrivateKey(issuerSeed);
  if (!equalThreadAuth(await x25519PublicKey(issuerPrivate), assignment.claimPublicKey)) {
    return { ok: false, reason: "the assignment claim key does not belong to its issuer" };
  }
  if (!equalThreadAuth(claim.assignmentId, assignment.assignmentId)) {
    return { ok: false, reason: "the claim belongs to a different assignment" };
  }
  if (!assignment.recipientCommitments.some(held => equalThreadAuth(held, recipientCommitment(claim.devicePublicKey)))) {
    return { ok: false, reason: "the claimant was not an intended assignment recipient" };
  }
  const nowSeconds = Math.floor(nowMillis / 1000);
  if (claim.claimedAt > assignment.notAfter || claim.claimedAt > nowSeconds + clockSkewSeconds) {
    return { ok: false, reason: "the claim timestamp is outside the assignment window" };
  }
  const shared = await x25519SharedSecret(issuerPrivate, claim.devicePublicKey);
  const expected = await claimMac(await claimKey(shared, assignment.assignmentId), claim.preimage);
  if (!equalThreadAuth(expected, claim.mac)) return { ok: false, reason: "the claim authentication code does not verify" };
  return { ok: true, assignment, claim };
}

/**
 * Verifies and seals an award to exactly one claimant. `claimOnce` MUST be a
 * durable atomic insert-if-absent keyed by assignmentIdHex. The first caller
 * for which it returns true wins; candidate timestamps never affect order.
 */
export async function awardFirstJobClaim({
  assignment: assignmentValue,
  claim: claimValue,
  ticket: ticketValue,
  issuerSeed,
  claimOnce,
  nowMillis = Date.now(),
  offerOptions = {}
} = {}) {
  if (typeof claimOnce !== "function") {
    throw new Error("Awarding requires an atomic claimOnce assignment store.");
  }
  const verdict = await verifyJobClaim(assignmentValue, claimValue, issuerSeed, { nowMillis, ...offerOptions });
  if (!verdict.ok) return verdict;
  let ticket;
  try { ticket = ticketValue?.plan ? ticketValue : decodeThreadTicket(ticketValue); }
  catch (error) { return { ok: false, reason: error.message }; }
  const ticketVerdict = await verifyThreadTicket(ticket, { nowMillis });
  if (!ticketVerdict.ok) return ticketVerdict;
  const match = awardMatchesOffer(verdict.assignment.offer, ticket);
  if (!match.ok) return match;
  const sealed = await sealTicket(ticket.bytes, [verdict.claim.devicePublicKey]);
  const won = await claimOnce(verdict.assignment.assignmentIdHex, {
    claimIdHex: verdict.claim.claimIdHex,
    devicePublicKey: Uint8Array.from(verdict.claim.devicePublicKey),
    receivedAt: nowMillis
  });
  if (!won) return { ok: false, code: "assignment-already-awarded", reason: "the assignment was already awarded" };
  return { ok: true, sealed, assignment: verdict.assignment, claim: verdict.claim };
}
