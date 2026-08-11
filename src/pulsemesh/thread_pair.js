// Mesh pairing (threads §16): enrolment with the scan flipped.
//
// Enrolment moves a device's PUBLIC card from the phone to the console.
// The card leaks nothing — it is public material — but the channel that
// carries it today is whatever is at hand: pasted text, a URL read
// aloud, a chat app that keeps a copy. And it is the wrong way round: the
// console has a screen, the phone has a camera.
//
// Pairing flips it. The console shows a short-lived offer as a QR; the
// phone scans it and replies OVER THE MESH with its card, sealed to the
// offer's key and proving it holds the private half. The one thing that
// still crosses the room by hand is the eight-character fingerprint two
// people already read to each other in a doorway (§16.4). The mesh
// replaces the clipboard, not the judgement — nothing here enrols a
// card, it only carries the proposal to where a human can confirm it.
//
// Three messages under one magic, PMP1:
//   kind 1  offer  console screen -> phone camera (a QR, not on the mesh)
//   kind 2  reply  phone -> mesh, the card sealed to the offer key + PoP
//   kind 3  ack    console -> mesh, sealed to the winner, after confirming
//
// This module is codec + crypto only. The session lifetime (subscribe,
// dedupe, expiry, teardown) lives in thread_session.js; the human
// confirmation lives in the app. Keeping those apart is deliberate: a
// module that could enrol on its own is a module that will, by accident,
// on the day somebody forgets the ceremony.

import { pushVarint, readVarint } from "../binary.js";
import { sha256, toHex, fromHex } from "./sha256.js";
import { utf8Bytes } from "./codec.js";
import {
  base64UrlToBytes,
  bytesToBase64Url,
  hkdfBytes,
  x25519SharedSecret
} from "./thread_crypto.js";
import {
  SEAL_MAX_RECIPIENTS,
  THREAD_MAX_DEVICE_NAME_BYTES,
  decodeDeviceCard,
  encodeDeviceCard,
  fingerprint,
  generateDeviceKeypair,
  openSealedTicket,
  sealTicket
} from "./thread_seal.js";

export const PAIR_MAGIC = "PMP1";
export const PAIR_VERSION = 1;

export const PAIR_KIND = Object.freeze({ OFFER: 1, REPLY: 2, ACK: 3 });

// §14 additions. Fifteen minutes, not a shift: a photographed offer can
// propose keys until it expires, and the ceremony filters them but the
// clock is what bounds them.
export const PAIR_MAX_SECONDS = 900;
export const PAIR_MAX_REPLY_BYTES = 512;
export const PAIR_MAX_CANDIDATES = SEAL_MAX_RECIPIENTS; // 16
export const PAIR_POP_SKEW = 120;

// A pairing address is a bootstrap hint, under the sealed ticket's own
// caps (§20.10): the offer carries the operator's OWN seed so a fresh
// phone has something to dial the moment it is paired.
export const PAIR_MAX_ADDRESSES = 3;
export const PAIR_MAX_ADDRESS_BYTES = 96;
export const PAIR_ID_BYTES = 16;

const TAG_DOMAIN = "wayfind-pair-v1/tag";
const KEY_DOMAIN = "wayfind-pair-v1/key";
const POP_DOMAIN = "wayfind-pair-v1/pop";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function pushMagic(out) {
  for (const ch of PAIR_MAGIC) out.push(ch.charCodeAt(0));
}

function readMagic(bytes, state) {
  const magic = String.fromCharCode(...bytes.subarray(state.pos, state.pos + 4));
  if (magic !== PAIR_MAGIC) throw new Error(`Expected ${PAIR_MAGIC}, found ${JSON.stringify(magic)}.`);
  state.pos += 4;
}

function readU8(bytes, state) {
  if (state.pos >= bytes.length) throw new Error("PMP1 ended early.");
  return bytes[state.pos++];
}

function readBytes(bytes, state, length) {
  if (state.pos + length > bytes.length) throw new Error("PMP1 ended early.");
  const out = bytes.subarray(state.pos, state.pos + length);
  state.pos += length;
  return out;
}

function assertNoTrailing(bytes, state, what) {
  if (state.pos !== bytes.length) throw new Error(`${what} has trailing bytes.`);
}

// --- Offer (kind 1): console screen -> phone camera -----------------------

/**
 * Mint a pairing offer. Returns the bytes to show as a QR, the ephemeral
 * keypair the console keeps until the pairing ends, and the pairing id.
 *
 * The keypair is FRESH per offer and dies with it (§16.1): reusing it
 * would make the QR a stable identifier of the operator and let an old
 * photograph open a new channel.
 */
export async function createPairingOffer({
  epochPrefix8,
  label = "",
  addresses = [],
  nowSeconds,
  maxSeconds = PAIR_MAX_SECONDS
} = {}) {
  if (!(epochPrefix8 instanceof Uint8Array) || epochPrefix8.length !== 8) {
    throw new Error("A pairing offer needs the 8-byte epoch prefix.");
  }
  if (!Number.isInteger(nowSeconds)) throw new Error("createPairingOffer needs nowSeconds.");
  const { privateKey, publicKey } = await generateDeviceKeypair();
  const pairingId = randomId();
  const notAfter = nowSeconds + Math.min(maxSeconds, PAIR_MAX_SECONDS);
  const bytes = encodePairingOffer({
    pairingId, pairingPub: publicKey, epochPrefix8, notAfter, label, addresses
  });
  return { bytes, privateKey, publicKey, pairingId, notAfter };
}

function randomId() {
  return globalThis.crypto.getRandomValues(new Uint8Array(PAIR_ID_BYTES));
}

export function encodePairingOffer({
  pairingId, pairingPub, epochPrefix8, notAfter, label = "", addresses = []
}) {
  if (pairingId?.length !== PAIR_ID_BYTES) throw new Error("A pairing id is 16 bytes.");
  if (pairingPub?.length !== 32) throw new Error("A pairing public key is 32 bytes.");
  if (epochPrefix8?.length !== 8) throw new Error("The epoch prefix is 8 bytes.");
  if (!Number.isInteger(notAfter) || notAfter < 0 || notAfter > 0xffffffff) {
    throw new Error("A pairing notAfter is a uint32 unix timestamp.");
  }
  const labelBytes = typeof label === "string" ? encoder.encode(label) : (label || new Uint8Array(0));
  if (labelBytes.length > THREAD_MAX_DEVICE_NAME_BYTES) {
    throw new Error(`A pairing label is at most ${THREAD_MAX_DEVICE_NAME_BYTES} bytes.`);
  }
  const addrs = (addresses || []).map(a => (typeof a === "string" ? encoder.encode(a) : a));
  if (addrs.length > PAIR_MAX_ADDRESSES) {
    throw new Error(`A pairing offer carries at most ${PAIR_MAX_ADDRESSES} addresses.`);
  }
  for (const a of addrs) {
    if (a.length > PAIR_MAX_ADDRESS_BYTES) {
      throw new Error(`A pairing address is at most ${PAIR_MAX_ADDRESS_BYTES} bytes.`);
    }
  }
  const out = [];
  pushMagic(out);
  out.push(PAIR_VERSION, PAIR_KIND.OFFER);
  for (const b of pairingId) out.push(b);
  for (const b of pairingPub) out.push(b);
  for (const b of epochPrefix8) out.push(b);
  pushVarint(out, notAfter);
  pushVarint(out, labelBytes.length);
  for (const b of labelBytes) out.push(b);
  pushVarint(out, addrs.length);
  for (const a of addrs) {
    pushVarint(out, a.length);
    for (const b of a) out.push(b);
  }
  return Uint8Array.from(out);
}

export function decodePairingOffer(bytesOrText) {
  const bytes = toBytes(bytesOrText);
  const state = { pos: 0 };
  readMagic(bytes, state);
  const version = readU8(bytes, state);
  if (version !== PAIR_VERSION) throw new Error(`Unsupported PMP1 version ${version}.`);
  const kind = readU8(bytes, state);
  if (kind !== PAIR_KIND.OFFER) throw new Error(`Expected a pairing offer, found kind ${kind}.`);
  const pairingId = Uint8Array.from(readBytes(bytes, state, PAIR_ID_BYTES));
  const pairingPub = Uint8Array.from(readBytes(bytes, state, 32));
  const epochPrefix8 = Uint8Array.from(readBytes(bytes, state, 8));
  const notAfter = readVarint(bytes, state);
  const labelLen = readVarint(bytes, state);
  if (labelLen > THREAD_MAX_DEVICE_NAME_BYTES) throw new Error("Pairing label too long.");
  const label = decoder.decode(readBytes(bytes, state, labelLen));
  const addrCount = readVarint(bytes, state);
  if (addrCount > PAIR_MAX_ADDRESSES) throw new Error("Too many pairing addresses.");
  const addresses = [];
  for (let i = 0; i < addrCount; i++) {
    const len = readVarint(bytes, state);
    if (len > PAIR_MAX_ADDRESS_BYTES) throw new Error("A pairing address is too long.");
    addresses.push(decoder.decode(readBytes(bytes, state, len)));
  }
  assertNoTrailing(bytes, state, "PMP1 offer");
  return { version, kind, pairingId, pairingPub, epochPrefix8, notAfter, label, addresses };
}

export function pairingOfferUrl(bytes, baseUrl = "wayfind://pair") {
  return `${baseUrl}#${bytesToBase64Url(bytes)}`;
}

export function parsePairingOfferUrl(text) {
  const fragment = String(text).split("#")[1] ?? String(text);
  return decodePairingOffer(base64UrlToBytes(fragment.trim().replace(/\s+/gu, "")));
}

// --- Proof of possession --------------------------------------------------
//
// Built exactly the way PMQ1 binds a job claim: an HMAC over the pairing
// context, keyed by an HKDF of the X25519 shared secret. A reply that
// carries a valid pop proves the sender holds the private half of the
// device key it is offering — which paste never proved. It does not prove
// the sender is the phone in front of you; that is §16.4, and it is human.

async function popKey(sharedSecret) {
  return hkdfBytes(sharedSecret, utf8Bytes(KEY_DOMAIN), 32);
}

function popPreimage({ pairingId, pairingPub, devicePub, timestamp }) {
  const out = [];
  for (const b of pairingId) out.push(b);
  for (const b of pairingPub) out.push(b);
  for (const b of devicePub) out.push(b);
  pushVarint(out, timestamp);
  return Uint8Array.from(out);
}

async function computePop(keyBytes, preimage) {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) throw new Error("PulseMesh pairing requires WebCrypto (globalThis.crypto.subtle).");
  const key = await subtle.importKey("raw", keyBytes, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const msg = new Uint8Array([...encoder.encode(POP_DOMAIN), ...preimage]);
  return new Uint8Array(await subtle.sign("HMAC", key, msg));
}

// --- Reply (kind 2): phone -> mesh ----------------------------------------

/**
 * The phone's answer to an offer: its device card and a proof of
 * possession, sealed with PME1 to the offer's public key. The plaintext
 * pairingId lets the console drop foreign replies without trial
 * decryption (§16.3).
 */
export async function createPairingReply({
  offer,
  devicePublicKey,
  devicePrivateKey,
  name = "",
  nowSeconds
}) {
  if (devicePrivateKey?.length !== 32) throw new Error("A device private key is 32 bytes.");
  if (devicePublicKey?.length !== 32) throw new Error("A device public key is 32 bytes.");
  if (!Number.isInteger(nowSeconds)) throw new Error("createPairingReply needs nowSeconds.");
  const shared = await x25519SharedSecret(devicePrivateKey, offer.pairingPub);
  const key = await popKey(shared);
  const pop = await computePop(key, popPreimage({
    pairingId: offer.pairingId,
    pairingPub: offer.pairingPub,
    devicePub: devicePublicKey,
    timestamp: nowSeconds
  }));

  const card = encodeDeviceCard({ publicKey: devicePublicKey, name });
  const inner = [];
  pushVarint(inner, card.length);
  for (const b of card) inner.push(b);
  pushVarint(inner, nowSeconds);
  for (const b of pop) inner.push(b);

  const sealed = await sealTicket(Uint8Array.from(inner), [offer.pairingPub]);

  const out = [];
  pushMagic(out);
  out.push(PAIR_VERSION, PAIR_KIND.REPLY);
  for (const b of offer.pairingId) out.push(b);
  for (const b of sealed) out.push(b);
  const bytes = Uint8Array.from(out);
  if (bytes.length > PAIR_MAX_REPLY_BYTES) {
    throw new Error(`A pairing reply is at most ${PAIR_MAX_REPLY_BYTES} bytes; this is ${bytes.length}.`);
  }
  return bytes;
}

/**
 * The console side. Opens a reply against the offer it minted and returns
 * the proposed card ONLY if the pop verifies and the timestamp is fresh —
 * a proposal, never an enrolment. The caller shows the fingerprint and
 * waits for a human. Returns null for a reply that is not for this offer.
 */
export async function openPairingReply({
  bytes, pairingId, pairingPrivateKey, pairingPublicKey, nowSeconds, popSkew = PAIR_POP_SKEW
}) {
  const reply = toBytes(bytes);
  if (reply.length > PAIR_MAX_REPLY_BYTES) return null;
  const state = { pos: 0 };
  try {
    readMagic(reply, state);
  } catch {
    return null;
  }
  const version = readU8(reply, state);
  if (version !== PAIR_VERSION) return null;
  const kind = readU8(reply, state);
  if (kind !== PAIR_KIND.REPLY) return null;
  const replyId = readBytes(reply, state, PAIR_ID_BYTES);
  // Foreign reply — for a different pairing. Drop without trial decrypt.
  if (toHex(replyId) !== toHex(pairingId)) return null;

  const sealed = reply.subarray(state.pos);
  let inner;
  try {
    inner = await openSealedTicket(sealed, pairingPrivateKey);
  } catch {
    return null;
  }

  const innerState = { pos: 0 };
  let card;
  let timestamp;
  let pop;
  try {
    const cardLen = readVarint(inner, innerState);
    const cardBytes = readBytes(inner, innerState, cardLen);
    card = decodeDeviceCard(cardBytes);
    timestamp = readVarint(inner, innerState);
    pop = readBytes(inner, innerState, 32);
    assertNoTrailing(inner, innerState, "PMP1 reply body");
  } catch {
    return null;
  }

  if (!Number.isInteger(timestamp) || Math.abs(nowSeconds - timestamp) > popSkew) return null;

  // Verify possession: the sender holds the private half of card.publicKey.
  const shared = await x25519SharedSecret(pairingPrivateKey, card.publicKey);
  const key = await popKey(shared);
  const expected = await computePop(key, popPreimage({
    pairingId,
    pairingPub: pairingPublicKey,
    devicePub: card.publicKey,
    timestamp
  }));
  if (!timingSafeEqual(expected, pop)) return null;

  return {
    publicKey: card.publicKey,
    publicKeyHex: toHex(card.publicKey),
    name: card.name,
    fingerprint: card.fingerprint,
    timestamp
  };
}

// --- Ack (kind 3): console -> mesh, after the human confirms ---------------

/**
 * Told to the chosen device, sealed to it, on the same topic, ONLY after
 * the dispatcher has confirmed a fingerprint (§16.5). The phone that finds
 * its own fingerprint inside shows "paired"; every other phone learns
 * only that it was not chosen.
 */
export async function createPairingAck({
  pairingId, devicePublicKey, label = "", chosenFingerprint, enrolledAt
}) {
  if (devicePublicKey?.length !== 32) throw new Error("A device public key is 32 bytes.");
  const fp = typeof chosenFingerprint === "string" ? fromHex(chosenFingerprint) : chosenFingerprint;
  if (fp?.length !== 4) throw new Error("A fingerprint is 4 bytes.");
  if (!Number.isInteger(enrolledAt)) throw new Error("An ack needs enrolledAt.");
  const labelBytes = typeof label === "string" ? encoder.encode(label) : (label || new Uint8Array(0));
  if (labelBytes.length > THREAD_MAX_DEVICE_NAME_BYTES) throw new Error("Ack label too long.");

  const inner = [];
  pushVarint(inner, labelBytes.length);
  for (const b of labelBytes) inner.push(b);
  pushVarint(inner, enrolledAt);
  for (const b of fp) inner.push(b);
  const sealed = await sealTicket(Uint8Array.from(inner), [devicePublicKey]);

  const out = [];
  pushMagic(out);
  out.push(PAIR_VERSION, PAIR_KIND.ACK);
  for (const b of pairingId) out.push(b);
  for (const b of sealed) out.push(b);
  return Uint8Array.from(out);
}

/**
 * The phone side of the ack: open it with the device key. Returns the
 * chosen fingerprint and label, or null when this is not our ack (a reply
 * we cannot open is one sealed to a different device — somebody else won).
 */
export async function openPairingAck({ bytes, pairingId, devicePrivateKey }) {
  const ack = toBytes(bytes);
  const state = { pos: 0 };
  try {
    readMagic(ack, state);
  } catch {
    return null;
  }
  if (readU8(ack, state) !== PAIR_VERSION) return null;
  if (readU8(ack, state) !== PAIR_KIND.ACK) return null;
  const ackId = readBytes(ack, state, PAIR_ID_BYTES);
  if (toHex(ackId) !== toHex(pairingId)) return null;
  const sealed = ack.subarray(state.pos);
  let inner;
  try {
    inner = await openSealedTicket(sealed, devicePrivateKey);
  } catch {
    return null;
  }
  const innerState = { pos: 0 };
  try {
    const labelLen = readVarint(inner, innerState);
    const label = decoder.decode(readBytes(inner, innerState, labelLen));
    const enrolledAt = readVarint(inner, innerState);
    const fp = readBytes(inner, innerState, 4);
    assertNoTrailing(inner, innerState, "PMP1 ack body");
    return { label, enrolledAt, fingerprint: toHex(fp) };
  } catch {
    return null;
  }
}

// --- Rendezvous -----------------------------------------------------------
//
// The pairing topic is the ordinary thread-topic namespace over the
// epoch, keyed by a tag derived from the pairing id and key — so pairing
// traffic is indistinguishable from thread traffic to everything carrying
// it, and the same seed relays it without learning anything (§16.2).

export async function pairingTag({ pairingId, pairingPub }) {
  const ikm = new Uint8Array([...pairingId, ...pairingPub]);
  return hkdfBytes(ikm, utf8Bytes(TAG_DOMAIN), 8);
}

export function pairingTopic(epochPrefix16hex, tag) {
  return `/rangefind/pulsemesh/1/t/${epochPrefix16hex}/${toHex(tag)}`;
}

/** What kind of PMP1 message this is, without decoding the rest. */
export function pairingKindOf(bytes) {
  const b = toBytes(bytes);
  if (b.length < 6) return null;
  if (String.fromCharCode(b[0], b[1], b[2], b[3]) !== PAIR_MAGIC) return null;
  if (b[4] !== PAIR_VERSION) return null;
  return b[5];
}

function toBytes(value) {
  if (value instanceof Uint8Array) return value;
  if (typeof value === "string") return base64UrlToBytes(value.trim().replace(/\s+/gu, ""));
  throw new Error("Expected bytes or a base64url string.");
}

function timingSafeEqual(a, b) {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
  return diff === 0;
}
