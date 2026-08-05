// PulseMesh thread wire formats (threads §5): PMT1 sealed update, PMTP
// plaintext body, PMR1/PMM1 catch-up, and the 45-byte link.
//
// Framing, varints, magic discipline, and the zero-trailing-bytes rule
// are the traffic channel's §1 conventions unchanged — the two channels
// share every convention and no records.

import { pushVarint, readVarint } from "../binary.js";
import { uint32be } from "./thread_crypto.js";
import { bytesToBase64Url, base64UrlToBytes } from "./thread_crypto.js";

export const THREAD_MAGIC = Object.freeze({
  PMT1: "PMT1",
  PMTP: "PMTP",
  PMR1: "PMR1",
  PMM1: "PMM1"
});

// §5.3 run states.
export const THREAD_STATE = Object.freeze({
  SCHEDULED: 1,
  EN_ROUTE: 2,
  DWELLING: 3,
  COMPLETED: 4,
  CANCELED: 5,
  OFF_PLAN: 6
});

// §11 granularity. Not a bandwidth setting: it decides what a leaked
// capability is worth.
export const THREAD_MODE = Object.freeze({ COARSE: 1, FINE: 2 });

export const THREAD_MAX_RECORD_BYTES = 256;
export const THREAD_MAX_NOTE_BYTES = 64;
export const LINK_VERSION = 1;
export const LINK_BYTES = 45;

/**
 * §5.2's "position withheld" test. The spec words it as `leafCell = 0`,
 * which collides with the real leaf 0; the triple being all-zero is the
 * wire-compatible reading that does not throw away one leaf's worth of
 * the map.
 */
export function isWithheldPosition(leafCell, geomRef, ratioQ12) {
  return leafCell === 0 && geomRef === 0 && ratioQ12 === 0;
}

function pushMagic(out, magic) {
  for (let i = 0; i < 4; i++) out.push(magic.charCodeAt(i));
}

function pushBytes(out, values) {
  for (const value of values) out.push(value);
}

function readMagic(bytes, state) {
  if (state.pos + 4 > bytes.length) throw new Error("Thread record truncated before magic.");
  const magic = String.fromCharCode(bytes[state.pos], bytes[state.pos + 1], bytes[state.pos + 2], bytes[state.pos + 3]);
  state.pos += 4;
  return magic;
}

function expectMagic(bytes, state, magic) {
  const found = readMagic(bytes, state);
  if (found !== magic) throw new Error(`Expected ${magic}, found ${JSON.stringify(found)}.`);
}

function readBytes(bytes, state, length) {
  if (state.pos + length > bytes.length) throw new Error("Thread record truncated.");
  const out = bytes.subarray(state.pos, state.pos + length);
  state.pos += length;
  return out;
}

function readU8(bytes, state) {
  if (state.pos >= bytes.length) throw new Error("Thread record truncated.");
  return bytes[state.pos++];
}

function assertNoTrailing(bytes, state, what) {
  if (state.pos !== bytes.length) throw new Error(`${what} has trailing bytes.`);
}

// --- §5.2 PMTP plaintext body ---------------------------------------------

/**
 * Encodes fields 1–11 — the signed preimage. The signature is appended
 * separately so a publisher signs exactly these bytes and a subscriber
 * verifies exactly these bytes.
 */
export function encodeThreadBodyPreimage(body) {
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMTP);
  pushVarint(out, body.unixSeconds);
  out.push(body.state & 0xff);
  out.push(body.mode & 0xff);
  pushVarint(out, body.leafCell);
  pushVarint(out, body.geomRef);
  pushVarint(out, body.ratioQ12);
  out.push(body.speedBin & 0xff);
  pushVarint(out, body.stopIndex);
  const planRef = body.planRef || new Uint8Array(8);
  if (planRef.length !== 8) throw new Error("planRef must be 8 bytes.");
  pushBytes(out, planRef);
  const note = body.note || new Uint8Array(0);
  if (note.length > THREAD_MAX_NOTE_BYTES) throw new Error("Thread note exceeds 64 bytes.");
  pushVarint(out, note.length);
  pushBytes(out, note);
  return Uint8Array.from(out);
}

export function encodeThreadBody(body, signature) {
  if (signature.length !== 64) throw new Error("Thread signature must be 64 bytes.");
  const preimage = encodeThreadBodyPreimage(body);
  const out = new Uint8Array(preimage.length + 64);
  out.set(preimage, 0);
  out.set(signature, preimage.length);
  return out;
}

export function decodeThreadBody(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, THREAD_MAGIC.PMTP);
  const unixSeconds = readVarint(bytes, state);
  const runState = readU8(bytes, state);
  const mode = readU8(bytes, state);
  const leafCell = readVarint(bytes, state);
  const geomRef = readVarint(bytes, state);
  const ratioQ12 = readVarint(bytes, state);
  const speedBin = readU8(bytes, state);
  const stopIndex = readVarint(bytes, state);
  const planRef = readBytes(bytes, state, 8);
  const noteLen = readVarint(bytes, state);
  const note = readBytes(bytes, state, noteLen);
  const preimageEnd = state.pos;
  const signature = readBytes(bytes, state, 64);
  assertNoTrailing(bytes, state, "PMTP body");
  return {
    magic: THREAD_MAGIC.PMTP,
    unixSeconds,
    state: runState,
    mode,
    leafCell,
    geomRef,
    ratioQ12,
    speedBin,
    stopIndex,
    planRef,
    note,
    signature,
    preimage: bytes.subarray(0, preimageEnd),
    // Position is (segment, ratio), never coordinates: snapped to the
    // road, smaller than a coordinate pair, and meaningless without the
    // map pack for this epoch.
    //
    // §5.2 makes `leafCell = 0` mean "withheld", but leaf 0 is an
    // ordinary leaf — a vehicle really can be on segment "0/95/1", and
    // reading the sentinel off leafCell alone silently discards the
    // position of everything in it. The whole triple is the sentinel
    // instead: a withheld record writes 0/0/0, and the publisher nudges
    // the one colliding real position (leaf 0, polyline 0, direction 0,
    // ratio exactly 0) off the sentinel. See the note in
    // encodeThreadBodyPreimage's caller.
    segment: isWithheldPosition(leafCell, geomRef, ratioQ12)
      ? null
      : `${leafCell}/${geomRef >>> 1}/${geomRef & 1}`,
    ratio: ratioQ12 / 4096
  };
}

// --- §5.1 PMT1 sealed update ----------------------------------------------

/** AAD = fields 1–4 exactly as encoded (magic, epoch, tag, seq). */
export function threadRecordAad(epochPrefix8, tag, seq) {
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMT1);
  pushBytes(out, epochPrefix8);
  pushBytes(out, tag);
  pushVarint(out, seq);
  return Uint8Array.from(out);
}

export function encodeThreadRecord({ epochPrefix8, tag, seq, ciphertext }) {
  if (epochPrefix8.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  if (tag.length !== 8) throw new Error("Thread tag must be 8 bytes.");
  const aad = threadRecordAad(epochPrefix8, tag, seq);
  const out = [...aad];
  pushVarint(out, ciphertext.length);
  pushBytes(out, ciphertext);
  const bytes = Uint8Array.from(out);
  return { bytes, aad };
}

export function readThreadRecord(bytes, state) {
  const start = state.pos;
  expectMagic(bytes, state, THREAD_MAGIC.PMT1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const tag = readBytes(bytes, state, 8);
  const seq = readVarint(bytes, state);
  const aadEnd = state.pos;
  const ctLen = readVarint(bytes, state);
  const ciphertext = readBytes(bytes, state, ctLen);
  return {
    magic: THREAD_MAGIC.PMT1,
    epochPrefix8,
    tag,
    seq,
    ciphertext,
    aad: bytes.subarray(start, aadEnd),
    bytes: bytes.subarray(start, state.pos)
  };
}

export function decodeThreadRecord(bytes) {
  const state = { pos: 0 };
  const record = readThreadRecord(bytes, state);
  assertNoTrailing(bytes, state, "PMT1 record");
  return record;
}

// --- §5.5 catch-up --------------------------------------------------------

export const THREAD_REQUEST_SIZES = Object.freeze([4, 8, 16]);

/**
 * PMR1. Tag counts are fixed at 4/8/16 and padded with CSPRNG decoys —
 * free here, because a tag is indistinguishable from uniform random
 * bytes, so even a peer holding the same thread cannot tell which of the
 * tags a requester actually came for.
 */
export function encodeThreadRequest({ epochPrefix8, entries }) {
  if (!THREAD_REQUEST_SIZES.includes(entries.length)) {
    throw new Error("PMR1 tag count must be exactly 4, 8, or 16.");
  }
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMR1);
  pushBytes(out, epochPrefix8);
  pushVarint(out, entries.length);
  for (const entry of entries) {
    if (entry.tag.length !== 8) throw new Error("Thread tag must be 8 bytes.");
    pushBytes(out, entry.tag);
    pushVarint(out, entry.sinceSeq);
  }
  return Uint8Array.from(out);
}

export function decodeThreadRequest(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, THREAD_MAGIC.PMR1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const count = readVarint(bytes, state);
  if (!THREAD_REQUEST_SIZES.includes(count)) throw new Error("PMR1 tag count must be exactly 4, 8, or 16.");
  const entries = [];
  for (let i = 0; i < count; i++) {
    const tag = readBytes(bytes, state, 8);
    const sinceSeq = readVarint(bytes, state);
    entries.push({ tag, sinceSeq });
  }
  assertNoTrailing(bytes, state, "PMR1 request");
  return { magic: THREAD_MAGIC.PMR1, epochPrefix8, entries };
}

/**
 * PMM1. Tags echo the request order, and an unknown tag returns count 0 —
 * a responder must not distinguish "tag I do not hold" from "tag with no
 * new data".
 */
export function encodeThreadResponse({ epochPrefix8, entries }) {
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMM1);
  pushBytes(out, epochPrefix8);
  pushVarint(out, entries.length);
  for (const entry of entries) {
    pushBytes(out, entry.tag);
    const records = entry.records || [];
    pushVarint(out, records.length);
    for (const record of records) pushBytes(out, record.bytes || record);
  }
  return Uint8Array.from(out);
}

export function decodeThreadResponse(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, THREAD_MAGIC.PMM1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const count = readVarint(bytes, state);
  const entries = [];
  for (let i = 0; i < count; i++) {
    const tag = readBytes(bytes, state, 8);
    const recordCount = readVarint(bytes, state);
    const records = [];
    for (let r = 0; r < recordCount; r++) records.push(readThreadRecord(bytes, state));
    entries.push({ tag, records });
  }
  assertNoTrailing(bytes, state, "PMM1 response");
  return { magic: THREAD_MAGIC.PMM1, epochPrefix8, entries };
}

// --- §5.4 the link --------------------------------------------------------

/**
 * 45 bytes: version, the capability `P`, the epoch it is bound to, and an
 * absolute expiry. No bootstrap address, no mailbox host, no plan URL —
 * a link is a key, not a location.
 */
export function encodeThreadLink({ publicKey, epochPrefix8, notAfter }) {
  if (publicKey.length !== 32) throw new Error("A thread capability is a 32-byte public key.");
  if (epochPrefix8.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  const out = new Uint8Array(LINK_BYTES);
  out[0] = LINK_VERSION;
  out.set(publicKey, 1);
  out.set(epochPrefix8, 33);
  out.set(uint32be(notAfter), 41);
  return out;
}

export function decodeThreadLink(bytes) {
  if (bytes.length !== LINK_BYTES) throw new Error(`A thread link is ${LINK_BYTES} bytes.`);
  if (bytes[0] !== LINK_VERSION) throw new Error(`Unsupported thread link version ${bytes[0]}.`);
  const notAfter = (bytes[41] * 2 ** 24) + (bytes[42] << 16) + (bytes[43] << 8) + bytes[44];
  return {
    version: bytes[0],
    publicKey: bytes.subarray(1, 33),
    epochPrefix8: bytes.subarray(33, 41),
    notAfter
  };
}

/**
 * The capability belongs in a URL **fragment**: browsers never transmit
 * it, so the page host — operator, CDN, app-store landing page — never
 * receives it and serves only inert code that derives keys locally.
 * There is no endpoint anywhere that sees a tracking key.
 */
export function threadLinkUrl(baseUrl, link) {
  return `${baseUrl}#${bytesToBase64Url(link)}`;
}

export function parseThreadLinkUrl(url) {
  const hash = String(url).indexOf("#");
  if (hash < 0) throw new Error("A thread link lives in the URL fragment.");
  return decodeThreadLink(base64UrlToBytes(String(url).slice(hash + 1)));
}
