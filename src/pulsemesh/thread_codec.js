// PulseMesh thread wire formats (threads §5): PMT1 sealed update, PMTP
// plaintext body, PMR1/PMM1 catch-up, and the 45-byte link.
//
// Framing, varints, magic discipline, and the zero-trailing-bytes rule
// are the traffic channel's §1 conventions unchanged — the two channels
// share every convention and no records.

import { pushVarint, readVarint } from "../binary.js";
import { uint32be } from "./thread_crypto.js";
import { bytesToBase64Url, base64UrlToBytes } from "./thread_crypto.js";
import { utf8Bytes } from "./codec.js";
import { fromHex, toHex } from "./sha256.js";

export const THREAD_MAGIC = Object.freeze({
  PMT1: "PMT1",
  PMTP: "PMTP",
  PMR1: "PMR1",
  PMM1: "PMM1",
  // §20.7 photo blobs. `PMF1`/`PMG1` — the obvious pair — are already the
  // traffic channel's cell fetch and digest, and every `PMx1` slot the
  // thread channel reserves (`PMT`, `PMP`, `PMR`, `PMM`) has its 1 taken.
  // So these follow `PMTP`'s precedent instead: a `PMT` prefix, which
  // traffic-only implementations already ignore, with a fourth letter
  // naming the record rather than a version digit.
  PMTF: "PMTF",
  PMTB: "PMTB",
  // §21 day certificate. Same reasoning as the two above, one step
  // further: this is not a record on the wire at all, it is an **inner
  // sealed body** — the exact shape PMTP is — so it belongs to the
  // `PMT` family rather than to the `PMx1` envelope namespace, where
  // every remaining letter would misrepresent what it is. `C` for
  // certificate; `PMTC` is unused, and unlike the `PMO1` §20.4 refuses,
  // `C` is confusable with nothing at the size these four characters get
  // read back at.
  PMTC: "PMTC"
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
// capability is worth. Not to be confused with THREAD_TRAVEL_MODE, which
// is how the vehicle moves.
export const THREAD_MODE = Object.freeze({ COARSE: 1, FINE: 2 });

/**
 * How the run is being made (§5.2 field 5). A follower routes from the
 * broadcast position to its own stop under whatever graph it holds, and
 * a car graph applied to a bike courier overstates the arrival badly —
 * so the run says what it is and the ETA says which profile it used.
 */
export const THREAD_TRAVEL_MODE = Object.freeze({
  UNSPECIFIED: 0, CAR: 1, BIKE: 2, FOOT: 3
});

/**
 * Per-stop delivery outcome (§5.2 field 13). Two bits per plan stop.
 *
 * `PENDING` is the only value dwell detection can produce, and it
 * produces it by never writing here at all: passing a stop is progress,
 * not an assertion about what happened at the door. Everything else is
 * driver-asserted through `markStop`.
 */
export const STOP_OUTCOME = Object.freeze({
  PENDING: 0, DELIVERED: 1, SKIPPED: 2, FAILED: 3
});

/** Why a stop was skipped or failed. Free text goes in the note. */
export const STOP_REASON = Object.freeze({
  NONE: 0,
  CUSTOMER_ABSENT: 1,
  REFUSED: 2,
  INACCESSIBLE: 3,
  PARCEL_MISSING: 4,
  OTHER: 5
});

export const THREAD_MAX_RECORD_BYTES = 256;
export const THREAD_MAX_NOTE_BYTES = 64;
/** The commitment a mark may carry (§20.7): SHA-256 of the sealed blob. */
export const THREAD_PHOTO_HASH_BYTES = 32;

/**
 * What a PMT1 record spends on framing around the sealed body: magic 4,
 * epochPrefix8 8, tag 8, `seq` varint ≤ 5, `ctLen` varint 2 (a body is
 * never under 128 nor over 16383 bytes), AEAD tag 16.
 *
 * Subtracting it gives the budget the *plaintext* body has, which is the
 * only number an encoder can check: the body is sealed before it is
 * framed, so a record that overflows 256 has already been signed.
 */
export const THREAD_RECORD_OVERHEAD = 43;
export const THREAD_MAX_BODY_BYTES = THREAD_MAX_RECORD_BYTES - THREAD_RECORD_OVERHEAD;

/**
 * A **v1** link: the run key that the link carries signs the run's own
 * records. §4.1's original shape, and still the right one for anything
 * one-off — a self-started drive, a single delivery, a dispatched job
 * that dies at the door.
 */
export const LINK_VERSION_SELF = 1;
/**
 * A **v2** link: records are signed by a *delegated day key*, and the
 * key in the link is the route **root** (§21). A holder must expect a
 * PMTC certificate on the thread and must refuse a record until one
 * covers the key that signed it.
 *
 * The version lives in the artifact rather than being guessed per
 * record on purpose: "this record verified under some key I was told
 * about" is not a security property. A holder has to know from the 45
 * bytes it was given whether the key in its hand is an authority or an
 * identity, or a downgrade is one forged record away.
 */
export const LINK_VERSION_DELEGATED = 2;
/** The highest link version this build understands. */
export const LINK_VERSION = LINK_VERSION_DELEGATED;
export const LINK_VERSIONS = Object.freeze([LINK_VERSION_SELF, LINK_VERSION_DELEGATED]);
export const LINK_BYTES = 45;

/** §21. The version byte of a PMTC day certificate. */
export const DAY_CERT_VERSION = 1;
/** magic 4, version 1, two keys 64, three uint32be 12, signature 64. */
export const DAY_CERT_BYTES = 145;

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

function readU32be(bytes, state) {
  const at = readBytes(bytes, state, 4);
  return (at[0] * 2 ** 24) + (at[1] << 16) + (at[2] << 8) + at[3];
}

function assertNoTrailing(bytes, state, what) {
  if (state.pos !== bytes.length) throw new Error(`${what} has trailing bytes.`);
}

// --- §5.2 PMTP plaintext body ---------------------------------------------

function varintLength(value) {
  let bytes = 1;
  let rest = Math.floor(Math.max(0, value) / 0x80);
  while (rest > 0) {
    bytes++;
    rest = Math.floor(rest / 0x80);
  }
  return bytes;
}

/**
 * The largest outcome map that still fits, given everything else this
 * body spends. Only ever called to write an error message, so the loop
 * costs nothing that matters.
 */
function stopsThatFit(fixedBytes) {
  let best = 0;
  for (let count = 0; count <= 8192; count++) {
    const size = fixedBytes + varintLength(count) + Math.ceil(count / 4) + 64;
    if (size > THREAD_MAX_BODY_BYTES) break;
    best = count;
  }
  return best;
}

/**
 * Packs the cumulative outcome map: two bits per plan stop, LSB-first,
 * `outcomes[i]` being stop `i + 1` (`stopIndex` is 1-based).
 *
 * Cumulative in *every* record, deliberately. The gossip channel is
 * lossy and catch-up recovers at most THREAD_MAX_AGE of history, so a
 * follower that joins late, or sleeps through the record where stop 7
 * was skipped, has no other way to learn it happened. A 200-stop day
 * costs 50 bytes to say the whole day, every time.
 */
function packOutcomes(outcomes) {
  const packed = new Uint8Array(Math.ceil(outcomes.length / 4));
  for (let i = 0; i < outcomes.length; i++) {
    packed[i >> 2] |= (outcomes[i] & 0x03) << ((i & 3) * 2);
  }
  return packed;
}

function unpackOutcomes(packed, count) {
  const outcomes = new Array(count);
  for (let i = 0; i < count; i++) {
    outcomes[i] = (packed[i >> 2] >> ((i & 3) * 2)) & 0x03;
  }
  return outcomes;
}

/**
 * A mark's photo commitment as bytes, from bytes or 64 hex characters.
 *
 * The wire carries bytes and `decodeThreadBody` hands back **hex**, so a
 * decoded body has to survive being re-encoded: accepting both here is
 * what makes that round trip work without the caller converting.
 */
function photoHashBytes(value) {
  if (value == null || value === "") return null;
  if (value instanceof Uint8Array) {
    if (value.length !== THREAD_PHOTO_HASH_BYTES) throw new Error("A photo commitment is 32 bytes.");
    return value;
  }
  const text = String(value).toLowerCase();
  if (!/^[0-9a-f]{64}$/u.test(text)) {
    throw new Error("A photo commitment is 32 bytes or 64 hex characters.");
  }
  return fromHex(text);
}

/**
 * Encodes fields 1–20 — the signed preimage. The signature is appended
 * separately so a publisher signs exactly these bytes and a subscriber
 * verifies exactly these bytes.
 */
export function encodeThreadBodyPreimage(body) {
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMTP);
  pushVarint(out, body.unixSeconds);
  out.push(body.state & 0xff);
  out.push(body.mode & 0xff);
  out.push(body.travelMode & 0xff);
  pushVarint(out, body.leafCell);
  pushVarint(out, body.geomRef);
  pushVarint(out, body.ratioQ12);
  out.push(body.speedBin & 0xff);
  pushVarint(out, body.stopIndex);
  const planRef = body.planRef || new Uint8Array(8);
  if (planRef.length !== 8) throw new Error("planRef must be 8 bytes.");
  pushBytes(out, planRef);

  const outcomes = body.outcomes || [];
  const packed = packOutcomes(outcomes);
  pushVarint(out, outcomes.length);
  pushBytes(out, packed);

  // The most recent explicit mark, so a follower can render "stop 7
  // skipped — customer absent" without diffing two bitmaps.
  const last = body.lastOutcome || null;
  if (!last) {
    out.push(0);
  } else {
    out.push(1);
    pushVarint(out, last.stopIndex);
    out.push(last.outcome & 0xff);
    out.push(last.reasonCode & 0xff);
    // §20.7: a *commitment*, never the image. 32 bytes of SHA-256 over
    // the sealed blob, inside the signed preimage, so the driver is
    // bound to one specific photo and the bytes travel on demand over
    // their own protocol. A record stays 256 bytes whether or not a
    // photo exists, which is the entire point.
    const photoHash = photoHashBytes(last.photoHash);
    out.push(photoHash ? 1 : 0);
    if (photoHash) pushBytes(out, photoHash);
  }

  const note = body.note || new Uint8Array(0);
  if (note.length > THREAD_MAX_NOTE_BYTES) throw new Error("Thread note exceeds 64 bytes.");
  pushVarint(out, note.length);
  pushBytes(out, note);

  // 256 bytes is a hard wire limit, and the body is signed and sealed
  // before it is ever framed — so a run whose plan is too large to
  // encode must be refused here, with the number that would fit.
  const preimageLength = out.length;
  if (preimageLength + 64 > THREAD_MAX_BODY_BYTES) {
    const fixed = preimageLength - varintLength(outcomes.length) - packed.length;
    throw new Error(
      `PMTP body is ${preimageLength + 64} bytes and a PMT1 record allows ${THREAD_MAX_BODY_BYTES}: `
      + `${outcomes.length} plan stops cost ${packed.length} bytes of outcome map `
      + `and this note costs ${note.length}. With a ${note.length}-byte note a run plan `
      + `may carry at most ${stopsThatFit(fixed)} stops.`
    );
  }
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
  const travelMode = readU8(bytes, state);
  const leafCell = readVarint(bytes, state);
  const geomRef = readVarint(bytes, state);
  const ratioQ12 = readVarint(bytes, state);
  const speedBin = readU8(bytes, state);
  const stopIndex = readVarint(bytes, state);
  const planRef = readBytes(bytes, state, 8);
  const stopCount = readVarint(bytes, state);
  const packed = readBytes(bytes, state, Math.ceil(stopCount / 4));
  const outcomes = unpackOutcomes(packed, stopCount);
  const lastPresent = readU8(bytes, state);
  let lastOutcome = null;
  if (lastPresent) {
    const lastStopIndex = readVarint(bytes, state);
    const outcome = readU8(bytes, state);
    const reasonCode = readU8(bytes, state);
    const photoPresent = readU8(bytes, state);
    // Hex, not bytes, and deliberately: this is a content address the
    // consuming side uses as a Map key, puts in a DOM dataset, compares
    // for equality and logs. Bytes would make every one of those a
    // conversion, and `photoHashBytes` on the encoder side keeps the
    // round trip working.
    const photoHash = photoPresent
      ? toHex(readBytes(bytes, state, THREAD_PHOTO_HASH_BYTES))
      : null;
    lastOutcome = { stopIndex: lastStopIndex, outcome, reasonCode, photoHash };
  }
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
    travelMode,
    leafCell,
    geomRef,
    ratioQ12,
    speedBin,
    stopIndex,
    planRef,
    // Cumulative and explicit-only: a 0 here means nobody said what
    // happened at that stop, never "the vehicle has not reached it".
    outcomes,
    lastOutcome,
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

// --- §21 PMTC day certificate ---------------------------------------------
//
// A sealed body like PMTP, on the run's own topic, in the run's own seq
// stream. That placement is the whole design: §5.5 catch-up carries it
// with no new message type, no new protocol id and no second fetch, so a
// parent who opens the link at 07:41 pulls the day's certificate out of
// the same PMM1 that carries the records it has to verify.
//
// It is the one place the capability `P` appears in a body — §4.1 says
// `P` must not go **on the wire**, and this does not: the body is sealed
// under `K_content = HKDF(P)`, so anyone who can read this field already
// had to hold `P` to get here, exactly as the §5.2 signature does.
// Carrying it buys the check that matters most: a certificate minted by
// some *other* route is refused by name rather than as a bad signature.

/**
 * Fields 1–7 — what the root signs. The signature is appended
 * separately, so a depot signs exactly these bytes and a subscriber
 * verifies exactly these bytes.
 */
export function encodeDayCertificatePreimage(certificate) {
  const { rootPublicKey, dayPublicKey } = certificate;
  if (rootPublicKey?.length !== 32) throw new Error("A route root is a 32-byte Ed25519 public key.");
  if (dayPublicKey?.length !== 32) throw new Error("A day key is a 32-byte Ed25519 public key.");
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMTC);
  out.push((certificate.version ?? DAY_CERT_VERSION) & 0xff);
  pushBytes(out, rootPublicKey);
  pushBytes(out, dayPublicKey);
  for (const value of [certificate.serviceDay, certificate.notBefore, certificate.notAfter]) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
      throw new Error(`A day certificate's serviceDay, notBefore and notAfter are uint32; got ${value}.`);
    }
    pushBytes(out, uint32be(value));
  }
  return Uint8Array.from(out);
}

export function encodeDayCertificate(certificate, signature) {
  if (signature.length !== 64) throw new Error("A day certificate's signature is 64 bytes.");
  const preimage = encodeDayCertificatePreimage(certificate);
  const out = new Uint8Array(preimage.length + 64);
  out.set(preimage, 0);
  out.set(signature, preimage.length);
  return out;
}

export function decodeDayCertificate(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, THREAD_MAGIC.PMTC);
  const version = readU8(bytes, state);
  const rootPublicKey = readBytes(bytes, state, 32);
  const dayPublicKey = readBytes(bytes, state, 32);
  const serviceDay = readU32be(bytes, state);
  const notBefore = readU32be(bytes, state);
  const notAfter = readU32be(bytes, state);
  const preimageEnd = state.pos;
  const signature = readBytes(bytes, state, 64);
  assertNoTrailing(bytes, state, "PMTC certificate");
  return {
    magic: THREAD_MAGIC.PMTC,
    version,
    rootPublicKey,
    dayPublicKey,
    serviceDay,
    notBefore,
    notAfter,
    signature,
    preimage: bytes.subarray(0, preimageEnd),
    bytes
  };
}

/**
 * The four magic characters an opened body starts with, or null when
 * there are not four bytes to read. A subscriber needs this *before*
 * committing to a decoder: one topic now carries two body shapes, and
 * `decodeThreadBody` throwing "Expected PMTP" on a perfectly good
 * certificate would be indistinguishable from a malformed record.
 */
export function threadBodyMagic(bytes) {
  if (!(bytes instanceof Uint8Array) || bytes.length < 4) return null;
  return String.fromCharCode(bytes[0], bytes[1], bytes[2], bytes[3]);
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

// --- §20.7 photo blobs ----------------------------------------------------

/**
 * PMTF. A request is a magic and a content hash, and nothing else.
 *
 * No epochPrefix8, unlike PMR1: a catch-up request names *tags*, which
 * only mean anything inside an epoch, while this names a SHA-256 of
 * ciphertext — globally unique by construction. An epoch prefix here
 * would be a fingerprint the responder does not need and cannot use.
 */
export function encodePhotoRequest({ hash }) {
  const bytes = photoHashBytes(hash);
  if (!bytes) throw new Error("A photo request needs a 32-byte commitment.");
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMTF);
  pushBytes(out, bytes);
  return Uint8Array.from(out);
}

export function decodePhotoRequest(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, THREAD_MAGIC.PMTF);
  const hash = readBytes(bytes, state, THREAD_PHOTO_HASH_BYTES);
  assertNoTrailing(bytes, state, "PMTF request");
  return { magic: THREAD_MAGIC.PMTF, hash: toHex(hash) };
}

/**
 * PMTB. A length and that many sealed bytes; **zero length means not
 * held**, which is also the answer to a hash this peer has never seen.
 * As with PMM1's unknown tags, a responder must not let a prober tell
 * "I do not have it" from "I will not give it to you".
 */
export function encodePhotoResponse({ sealed = null } = {}) {
  const body = sealed || new Uint8Array(0);
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMTB);
  pushVarint(out, body.length);
  pushBytes(out, body);
  return Uint8Array.from(out);
}

export function decodePhotoResponse(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, THREAD_MAGIC.PMTB);
  const length = readVarint(bytes, state);
  const sealed = readBytes(bytes, state, length);
  assertNoTrailing(bytes, state, "PMTB response");
  return { magic: THREAD_MAGIC.PMTB, sealed: length ? sealed : null };
}

// --- §5.4 the link --------------------------------------------------------

/**
 * 45 bytes: version, the capability `P`, the epoch it is bound to, and an
 * absolute expiry. No bootstrap address, no mailbox host, no plan URL —
 * a link is a key, not a location.
 *
 * `version` says what the key in field 2 **is**, not how new the writer
 * is, which is why the default did not move to 2 when §21 landed: a
 * one-off run's key really does sign its own records, and writing 2 over
 * it would be a lie about the artifact that a subscriber would then
 * enforce (it would sit waiting for a certificate that is never coming).
 * A recurring route passes `LINK_VERSION_DELEGATED` explicitly, and the
 * §16.3 vector — a v1 link — is byte-unchanged.
 */
export function encodeThreadLink({ publicKey, epochPrefix8, notAfter, version = LINK_VERSION_SELF }) {
  if (publicKey.length !== 32) throw new Error("A thread capability is a 32-byte public key.");
  if (epochPrefix8.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  if (!LINK_VERSIONS.includes(version)) {
    throw new Error(`A thread link is version ${LINK_VERSIONS.join(" or ")}; got ${version}.`);
  }
  const out = new Uint8Array(LINK_BYTES);
  out[0] = version;
  out.set(publicKey, 1);
  out.set(epochPrefix8, 33);
  out.set(uint32be(notAfter), 41);
  return out;
}

export function decodeThreadLink(bytes) {
  if (bytes.length !== LINK_BYTES) throw new Error(`A thread link is ${LINK_BYTES} bytes.`);
  if (!LINK_VERSIONS.includes(bytes[0])) throw new Error(`Unsupported thread link version ${bytes[0]}.`);
  const notAfter = (bytes[41] * 2 ** 24) + (bytes[42] << 16) + (bytes[43] << 8) + bytes[44];
  return {
    version: bytes[0],
    publicKey: bytes.subarray(1, 33),
    epochPrefix8: bytes.subarray(33, 41),
    notAfter,
    /**
     * True when field 2 is a route **root** and records are signed by a
     * delegated day key (§21). Read off the artifact, never inferred
     * from what happens to arrive on the topic.
     */
    delegated: bytes[0] === LINK_VERSION_DELEGATED
  };
}

// --- bootstrap hints: a location, beside the capability -------------------
//
// A capability tells you what a run *is*; it does not tell a cold device
// how to reach the mesh the run is published on. Joining needs at least
// one dialable peer, and a fleet that runs its own keeper (§12) has
// exactly one to hand out. These helpers are the shared validation and
// the URL placement rule for that address; the artifacts that carry one
// are the sealed ticket (§20.10) and the PMH1 seed card.

/** A multiaddr with a `/p2p/<peerId>` suffix is comfortably under this. */
export const THREAD_MAX_BOOTSTRAP_BYTES = 96;
/** How many a signed artifact may carry: enough for a depot pair plus a relay. */
export const THREAD_MAX_BOOTSTRAP_ADDRESSES = 3;
/**
 * How many a **URL** may hint at. Lower than the artifact's cap on
 * purpose: a link is typed, pasted, texted and printed, and two
 * addresses already cost more characters than the 45-byte capability
 * they are riding beside.
 */
export const THREAD_MAX_URL_BOOTSTRAP = 2;
/** The query parameter a hint travels in. Repeatable. */
export const BOOTSTRAP_QUERY_KEY = "b";

/**
 * The one syntactic check this repo makes on a bootstrap address.
 *
 * Not a multiaddr parser: `@multiformats/multiaddr` is an optional peer
 * dependency that only the libp2p transport pulls in, and a codec that
 * required it would make a ticket undecodable on a host that never dials
 * anything. A leading `/` catches the mistakes that actually happen — a
 * hostname, a `https://` URL, a peer id on its own — and the dial itself
 * is the authority on the rest.
 */
function looksLikeMultiaddr(address) {
  return typeof address === "string" && address.startsWith("/");
}

/**
 * Validates and normalizes bootstrap addresses, refusing rather than
 * truncating: an address silently cut at 96 bytes is an address that
 * dials nothing, and a fleet whose seed "sometimes works" is worse off
 * than one with no seed at all.
 */
export function normalizeBootstrapAddresses(value, {
  max = THREAD_MAX_BOOTSTRAP_ADDRESSES,
  what = "bootstrap address"
} = {}) {
  if (value == null || value === "") return [];
  const list = (Array.isArray(value) ? value : [value])
    .map(entry => String(entry ?? "").trim())
    .filter(Boolean);
  if (!list.length) return [];
  if (list.length > max) {
    // "address" → "addresses", "hint" → "hints".
    throw new Error(`At most ${max} ${what}${what.endsWith("s") ? "es" : "s"} fit here; this one has ${list.length}.`);
  }
  for (const address of list) {
    if (!looksLikeMultiaddr(address)) {
      throw new Error(
        `A ${what} is a multiaddr and starts with "/" (e.g. `
        + `/ip4/203.0.113.7/tcp/4001/p2p/12D3Koo…); this one is ${JSON.stringify(address)}.`
      );
    }
    const bytes = utf8Bytes(address).length;
    if (bytes > THREAD_MAX_BOOTSTRAP_BYTES) {
      throw new Error(`A ${what} is at most ${THREAD_MAX_BOOTSTRAP_BYTES} bytes; this one is ${bytes}.`);
    }
  }
  return list;
}

/** `/` is legal in a query and makes a multiaddr readable; nothing else is. */
function encodeAddress(address) {
  return encodeURIComponent(address).replaceAll("%2F", "/");
}

/**
 * Puts a bootstrap hint in the **query string**, never the fragment.
 *
 * The split is the whole point. The fragment is the capability and is
 * never transmitted — that is what keeps the page host from ever seeing
 * a tracking key (§5.4). The hint is not a capability: it is a public
 * address the recipient may dial, ignore, or already know better than
 * the sender does, and it is worth nothing to whoever serves the page.
 * So it goes where a hint belongs, in front of the `#`, and the
 * capability's bytes are byte-identical with or without it.
 */
export function withBootstrapHint(baseUrl, bootstrap) {
  const addresses = normalizeBootstrapAddresses(bootstrap, {
    max: THREAD_MAX_URL_BOOTSTRAP,
    what: "bootstrap hint"
  });
  if (!addresses.length) return String(baseUrl);
  const text = String(baseUrl);
  const hash = text.indexOf("#");
  const head = hash < 0 ? text : text.slice(0, hash);
  const tail = hash < 0 ? "" : text.slice(hash);
  const query = addresses.map(address => `${BOOTSTRAP_QUERY_KEY}=${encodeAddress(address)}`).join("&");
  return `${head}${head.includes("?") ? "&" : "?"}${query}${tail}`;
}

/**
 * Reads a hint back off an incoming URL, **leniently**.
 *
 * A malformed, oversized or over-long hint yields fewer addresses and
 * never an exception: the capability is in the fragment and must keep
 * working when the query is garbage, truncated by a messaging app, or
 * carrying somebody else's tracking parameters.
 */
export function parseBootstrapHint(url) {
  const text = String(url ?? "");
  const hash = text.indexOf("#");
  const head = hash < 0 ? text : text.slice(0, hash);
  const question = head.indexOf("?");
  if (question < 0) return [];
  const out = [];
  for (const pair of head.slice(question + 1).split("&")) {
    const equals = pair.indexOf("=");
    if (equals < 0 || pair.slice(0, equals) !== BOOTSTRAP_QUERY_KEY) continue;
    let value;
    try {
      value = decodeURIComponent(pair.slice(equals + 1).replaceAll("+", " "));
    } catch {
      continue; // a broken percent escape is a hint, still not an error
    }
    for (const part of value.split(",")) {
      const address = part.trim();
      if (!looksLikeMultiaddr(address)) continue;
      if (utf8Bytes(address).length > THREAD_MAX_BOOTSTRAP_BYTES) continue;
      if (!out.includes(address)) out.push(address);
      if (out.length === THREAD_MAX_URL_BOOTSTRAP) return out;
    }
  }
  return out;
}

/**
 * The capability belongs in a URL **fragment**: browsers never transmit
 * it, so the page host — operator, CDN, app-store landing page — never
 * receives it and serves only inert code that derives keys locally.
 * There is no endpoint anywhere that sees a tracking key.
 *
 * `bootstrap` is the optional §20.10 hint and lands in the query, in
 * front of the `#`: the fragment this returns is byte-identical whether
 * or not one was passed.
 */
export function threadLinkUrl(baseUrl, link, { bootstrap = null } = {}) {
  return `${withBootstrapHint(baseUrl, bootstrap)}#${bytesToBase64Url(link)}`;
}

export function parseThreadLinkUrl(url) {
  const hash = String(url).indexOf("#");
  if (hash < 0) throw new Error("A thread link lives in the URL fragment.");
  return decodeThreadLink(base64UrlToBytes(String(url).slice(hash + 1)));
}
