// PulseMesh thread wire formats (threads §5): PMT1 sealed update, PMTP
// plaintext body, PMR1/PMM1 catch-up, and the capability-separated link.
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
  // §20.7.1 the commitment list, on the same protocol id and by the same
  // naming: `L` asks for the list, `A` answers with it. They belong here
  // rather than beside PMR1/PMM1 because they have the photo protocol's
  // shape and not catch-up's — only the peer publishing the run can ever
  // answer one, and the answer is a prelude to fetching a blob.
  PMTL: "PMTL",
  PMTA: "PMTA",
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

export const THREAD_MAX_RECORD_BYTES = 320;
export const THREAD_MAX_NOTE_BYTES = 64;
export const THREAD_CHAIN_HASH_BYTES = 16;
export const THREAD_ADMISSION_TAG_BYTES = 16;
/** A photo commitment (§20.7) and the accumulator over them are both SHA-256. */
export const THREAD_PHOTO_HASH_BYTES = 32;

/**
 * How many entries a PMTA commitment list may carry (§20.7.1).
 *
 * A bound on what a *responder* can make a holder chew on, not on what a
 * run can take: a plan tops out at a few hundred stops (§5.2.2) and only
 * a re-mark adds an entry beyond one per stop, so no honest run comes
 * near this. 1024 entries is 34 KB — a third of one photo, and the list
 * is only ever fetched as the prelude to fetching those.
 */
export const THREAD_MAX_PHOTO_LIST = 1024;

/**
 * §5.2 field 14, the mark block's flags. Bit 0 is the byte that used to
 * be a plain `lastPresent` boolean, so a record that carries nothing new
 * is byte-identical to one this build's predecessor wrote — including
 * the §16.4 vector.
 */
export const THREAD_MARK_FLAG = Object.freeze({
  /** Fields 15–18: the most recent mark. */
  LAST: 0x01,
  /** Fields 19–20: the cumulative reason list (§5.2.1). */
  REASONS: 0x02,
  /** Field 21: the photo accumulator, over every commitment (§20.7.1). */
  PHOTO_CHAIN: 0x04
});
const THREAD_MARK_FLAGS_KNOWN = THREAD_MARK_FLAG.LAST | THREAD_MARK_FLAG.REASONS | THREAD_MARK_FLAG.PHOTO_CHAIN;

/**
 * How many per-stop reasons a record carries at most (§5.2.1).
 *
 * The list is cumulative, so it is bounded twice: by this count, and by
 * whatever is left of the 228-byte body — `fitStopReasons` applies both,
 * keeping the newest. 16 is the number that fits the day this channel is
 * sized for. A 200-stop plan leaves 57 bytes (§5.2.2), and 16 entries
 * cost 33 of them at one-byte stop indices and 49 at the widest a
 * 200-stop plan can produce, so the cap is reachable at any point in the
 * day without the reason list ever being the thing that does not fit. It
 * is also 8 % of a 200-stop day, several times the non-delivery rate a
 * courier fleet actually runs at.
 */
export const THREAD_MAX_REASONS = 16;
/** Bit 7 of a reason byte: this mark carried a photo commitment (§20.7). */
const REASON_PHOTO_BIT = 0x80;
const REASON_CODE_MASK = 0x7f;

/**
 * What a PMT1 record spends on framing around the sealed body: magic 4,
 * epochPrefix8 8, tag 8, authority generation and `seq` varints ≤ 5
 * each, predecessor hash 16, `ctLen` varint 2, random GCM nonce 12,
 * AEAD tag 16, and the outer admission MAC 16.
 *
 * Subtracting it gives the budget the *plaintext* body has, which is the
 * only number an encoder can check: the body is sealed before it is
 * framed, so a record that overflows 320 has already been signed.
 */
export const THREAD_RECORD_OVERHEAD = 92;
export const THREAD_MAX_BODY_BYTES = THREAD_MAX_RECORD_BYTES - THREAD_RECORD_OVERHEAD;

/**
 * The sole link version. Its flag distinguishes direct root authority
 * from short-lived authority delegated by a root certificate.
 */
export const LINK_VERSION = 1;
export const LINK_FLAG_DELEGATED = 0x01;
const LINK_FLAG_MASK = LINK_FLAG_DELEGATED;
/** version + flags + capability secret + root verification key + epoch + expiry. */
export const LINK_BYTES = 78;

/** §21. The version byte of a PMTC day certificate. */
export const DAY_CERT_VERSION = 1;
/** magic 4, version 1, two keys 64, four uint32be 16, planRef 8, signature 64. */
export const DAY_CERT_BYTES = 157;

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
 * A photo commitment or accumulator as bytes, from bytes or 64 hex
 * characters.
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

const PHOTO_CHAIN_ZERO_HEX = "0".repeat(64);

/**
 * The accumulator, or null for a run that has published no photos.
 *
 * A₀ is 32 zero bytes and a run sitting on it writes **nothing**: the
 * flag bit stays clear, and an all-delivered day pays not one byte for a
 * mechanism it never used (§5.2.2). Zeros passed in explicitly mean the
 * same thing rather than being an error — a publisher folding an empty
 * list arrives at them honestly.
 */
function photoChainField(value) {
  const bytes = photoHashBytes(value);
  if (!bytes) return null;
  return toHex(bytes) === PHOTO_CHAIN_ZERO_HEX ? null : bytes;
}

/**
 * Validates a cumulative reason list and drops it into a canonical shape.
 *
 * Input order is **mark order** — oldest first — because that is what the
 * cap evicts by. The wire order is by stop index, applied in the encoder.
 */
function normalizeStopReasons(value) {
  if (!value || !value.length) return [];
  const out = [];
  const seen = new Set();
  for (const entry of value) {
    const stopIndex = entry?.stopIndex;
    if (!Number.isInteger(stopIndex) || stopIndex < 1) {
      throw new Error(`A carried stop reason names a 1-based stop index; got ${stopIndex}.`);
    }
    if (seen.has(stopIndex)) {
      throw new Error(`Stop ${stopIndex} appears twice in the carried reason list.`);
    }
    seen.add(stopIndex);
    const reasonCode = entry.reasonCode ?? STOP_REASON.NONE;
    if (!Number.isInteger(reasonCode) || reasonCode < 0 || reasonCode > REASON_CODE_MASK) {
      throw new Error(`A carried reason code is 0..${REASON_CODE_MASK}; got ${reasonCode}.`);
    }
    out.push({ stopIndex, reasonCode, photo: entry.photo === true });
  }
  return out;
}

/** What fields 20–21 cost on the wire, without building them. */
function stopReasonBytes(reasons) {
  let bytes = varintLength(reasons.length);
  for (const entry of reasons) bytes += varintLength(entry.stopIndex) + 1;
  return bytes;
}

/**
 * Builds the signed preimage without checking that it fits, so the size
 * check and `fitStopReasons` can both measure the same bytes.
 */
function buildThreadBodyPreimage(body) {
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

  // Field 14 is a flag byte whose bit 0 is the boolean it used to be, so
  // a record with nothing new to say encodes to exactly the bytes it
  // always did.
  const last = body.lastOutcome || null;
  // Ascending by stop index on the wire — deterministic, and it lets the
  // decoder reject a duplicate or a reordering with one comparison. The
  // caller's ordering is mark order and matters only to the cap.
  const reasons = normalizeStopReasons(body.stopReasons).sort((a, b) => a.stopIndex - b.stopIndex);
  const photoChain = photoChainField(body.photoChain);
  let flags = 0;
  if (last) flags |= THREAD_MARK_FLAG.LAST;
  if (reasons.length) flags |= THREAD_MARK_FLAG.REASONS;
  if (photoChain) flags |= THREAD_MARK_FLAG.PHOTO_CHAIN;
  out.push(flags);

  // The most recent explicit mark, so a follower can render "stop 7
  // skipped — customer absent" without diffing two bitmaps.
  if (last) {
    pushVarint(out, last.stopIndex);
    out.push(last.outcome & 0xff);
    out.push(last.reasonCode & 0xff);
    // One byte: **whether** this mark carried a proof, not which one.
    // Inside the signature, so a photo cannot be attached to a record
    // after the fact nor stripped off it, and it is what lets a card say
    // "proof attached" from a single record with no fetch at all. The
    // commitment itself is in the accumulator below — see §20.7.1 for
    // why 32 bytes buys every commitment here rather than the newest.
    out.push(last.hasPhoto ? 1 : 0);
  }

  // §5.2.1 the cumulative reason list: one entry for **every** stop the
  // map says is skipped or failed, sparse because delivered and pending
  // stops have nothing to say. Carrying reason 0 explicitly is what makes
  // a gap in this list mean "I never learned why" rather than "no reason
  // was given" — see `stopReasonFor`.
  if (reasons.length) {
    pushVarint(out, reasons.length);
    for (const entry of reasons) {
      pushVarint(out, entry.stopIndex);
      // Bit 7 says the mark carried a photo commitment. Free — the code
      // itself needs six bits — and it is the whole of what a subscriber
      // can be told about a commitment it never received (§20.7).
      out.push((entry.reasonCode & REASON_CODE_MASK) | (entry.photo ? REASON_PHOTO_BIT : 0));
    }
  }

  // §20.7.1 the photo accumulator: 32 bytes binding **every** commitment
  // the run has published, in order, each to its stop. Re-stated in every
  // record exactly as the outcome map is, and for the same reason — a
  // holder that heard none of an outage needs a head it can check a list
  // against, and the only heads it will ever have are the ones in the
  // records it did receive.
  //
  // It costs what the single commitment it replaced cost, and it is the
  // whole record of the run's proofs rather than the newest one.
  if (photoChain) pushBytes(out, photoChain);

  const note = body.note || new Uint8Array(0);
  if (note.length > THREAD_MAX_NOTE_BYTES) throw new Error("Thread note exceeds 64 bytes.");
  pushVarint(out, note.length);
  pushBytes(out, note);
  return { bytes: out, outcomes, packed, note, reasons };
}

/**
 * Encodes fields 1–23 — the signed preimage. The signature is appended
 * separately so a publisher signs exactly these bytes and a subscriber
 * verifies exactly these bytes.
 */
export function encodeThreadBodyPreimage(body) {
  const { bytes: out, outcomes, packed, note, reasons } = buildThreadBodyPreimage(body);

  // 320 bytes is the hard wire limit, and the body is signed and sealed
  // before it is ever framed — so a run whose plan is too large to
  // encode must be refused here, with the number that would fit.
  const preimageLength = out.length;
  if (preimageLength + 64 > THREAD_MAX_BODY_BYTES) {
    const fixed = preimageLength - varintLength(outcomes.length) - packed.length;
    throw new Error(
      `PMTP body is ${preimageLength + 64} bytes and a PMT1 record allows ${THREAD_MAX_BODY_BYTES}: `
      + `${outcomes.length} plan stops cost ${packed.length} bytes of outcome map `
      + `and this note costs ${note.length}`
      + (reasons.length ? `, and ${reasons.length} carried reasons cost ${stopReasonBytes(reasons)}` : "")
      + `. With a ${note.length}-byte note a run plan `
      + `may carry at most ${stopsThatFit(fixed)} stops.`
    );
  }
  return Uint8Array.from(out);
}

/**
 * The most recent carried reasons that fit, oldest evicted first.
 *
 * **Bounded, never refused**, and that asymmetry with the plan-size check
 * above is deliberate. A plan's size is known before the run starts, so
 * refusing it is a configuration error caught once. A reason list grows
 * during the day, out of things the driver does at doors — refusing to
 * encode at 09:41 because the twelfth stop failed would take the outcome
 * map, the position and the whole rest of the record down with it, which
 * is a far worse loss than the reason it was protecting.
 *
 * **Oldest first** because the list is cumulative and self-healing. A
 * subscriber that has been listening has already received the early
 * reasons; the ones it is most likely still missing are the newest, so
 * the newest are what every record spends its bytes re-stating.
 *
 * `body.stopReasons` is in mark order — oldest first — since that is the
 * order this evicts by; the encoder sorts by stop index for the wire.
 */
export function fitStopReasons(body, { max = THREAD_MAX_REASONS } = {}) {
  const all = normalizeStopReasons(body.stopReasons);
  let kept = all.length > max ? all.slice(all.length - max) : all;
  while (kept.length
    && buildThreadBodyPreimage({ ...body, stopReasons: kept }).bytes.length + 64 > THREAD_MAX_BODY_BYTES) {
    kept = kept.slice(1);
  }
  return kept;
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
  const flags = readU8(bytes, state);
  // Reserved bits are refused rather than skipped: a record whose unknown
  // bit means "and then four more fields" would otherwise decode as a
  // short body with trailing bytes, or worse, not.
  if (flags & ~THREAD_MARK_FLAGS_KNOWN) {
    throw new Error(`PMTP mark flags ${flags} set a reserved bit.`);
  }
  let lastOutcome = null;
  if (flags & THREAD_MARK_FLAG.LAST) {
    const lastStopIndex = readVarint(bytes, state);
    const outcome = readU8(bytes, state);
    const reasonCode = readU8(bytes, state);
    // Whether a proof exists, signed. Which proof is the accumulator's
    // job (§20.7.1), and resolving it to a commitment takes one list
    // fetch — but "there is a photo for this stop" needs no fetch and is
    // the claim a card makes first.
    const hasPhoto = readU8(bytes, state) !== 0;
    lastOutcome = { stopIndex: lastStopIndex, outcome, reasonCode, hasPhoto };
  }
  const stopReasons = [];
  if (flags & THREAD_MARK_FLAG.REASONS) {
    const reasonCount = readVarint(bytes, state);
    let previous = 0;
    for (let i = 0; i < reasonCount; i++) {
      const entryStop = readVarint(bytes, state);
      // Strictly increasing, which is both the canonical order and the
      // duplicate check: two entries for one stop would let a link holder
      // craft a record whose meaning depends on which one a reader wins.
      if (entryStop <= previous) throw new Error("PMTP carried reasons must be strictly increasing by stop index.");
      previous = entryStop;
      const packedReason = readU8(bytes, state);
      stopReasons.push({
        stopIndex: entryStop,
        reasonCode: packedReason & REASON_CODE_MASK,
        photo: (packedReason & REASON_PHOTO_BIT) !== 0
      });
    }
  }
  // Hex, not bytes, and deliberately: an accumulator is compared for
  // equality, cached by value, keyed on and logged, exactly as a
  // commitment is — and `photoChainField` on the encoder side accepts it
  // back, so a decoded body re-encodes to the same bytes.
  const photoChain = (flags & THREAD_MARK_FLAG.PHOTO_CHAIN)
    ? toHex(readBytes(bytes, state, THREAD_PHOTO_HASH_BYTES))
    : null;
  // A₀ on the wire is not something an encoder can produce — the flag bit
  // is what says "this run has photographed something", and a run that
  // has not leaves it clear. Refused rather than tolerated, so a decoded
  // body re-encodes to the same bytes and a holder is never handed an
  // accumulator that names every run at once.
  if (photoChain === PHOTO_CHAIN_ZERO_HEX) {
    throw new Error("PMTP sets the photo-chain flag over an empty accumulator.");
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
    /**
     * §5.2.1. One entry per stop the map says is skipped or failed, in
     * stop order — reason code, and whether that mark carried a photo
     * commitment. Cumulative like the map, and capped like it is not:
     * a **gap** here against a skipped stop means the reason was lost,
     * which `stopReasonFor` reports rather than guessing at.
     */
    stopReasons,
    /**
     * §20.7.1. The hash chain over every photo commitment this run has
     * published, or null for a run that has published none. A holder
     * hands it to `fetchPhotoList` and the returned list is verified
     * against it — which is what makes a commitment published into a
     * dead zone recoverable rather than lost.
     */
    photoChain,
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

/**
 * What one stop's record says, and — the point of this function —
 * whether it says anything at all.
 *
 * "Stop 3 skipped, no reason given" and "stop 3 skipped, and I never
 * learned why" are different sentences to put in front of a dispatcher,
 * and before the carried reason list they were the same three bytes on
 * the wire. They are told apart by `reasonKnown`:
 *
 * - `reasonKnown: true, reasonCode: 0` — the driver marked it and stated
 *   no reason. That is a complete answer.
 * - `reasonKnown: false` — this stop is resolved-not-delivered and no
 *   record we hold carries its reason. Either the run marked it before
 *   this build's reason list existed, or the mark's reason aged out of
 *   the cap (§5.2.1). Missing, not empty.
 *
 * `hasPhoto` follows the same discipline: `true`/`false` when the reason
 * list covers the stop, `null` when nothing does — never a cheerful
 * `false` for a proof that may well exist (§20.7).
 */
export function stopReasonFor(body, stopIndex) {
  const outcome = body?.outcomes?.[stopIndex - 1] ?? STOP_OUTCOME.PENDING;
  // Pending and delivered stops have no reason to carry, so nothing is
  // missing and nothing is unknown.
  if (outcome !== STOP_OUTCOME.SKIPPED && outcome !== STOP_OUTCOME.FAILED) {
    return { outcome, reasonCode: null, reasonKnown: true, hasPhoto: null };
  }
  const entry = body?.stopReasons?.find(item => item.stopIndex === stopIndex) ?? null;
  if (entry) {
    return { outcome, reasonCode: entry.reasonCode, reasonKnown: true, hasPhoto: entry.photo };
  }
  // A record written before the list existed still answers for the one
  // stop it happens to be about.
  const last = body?.lastOutcome;
  if (last && last.stopIndex === stopIndex) {
    return { outcome, reasonCode: last.reasonCode, reasonKnown: true, hasPhoto: last.hasPhoto === true };
  }
  return { outcome, reasonCode: null, reasonKnown: false, hasPhoto: null };
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
  const { rootPublicKey, dayPublicKey, planRef } = certificate;
  if (rootPublicKey?.length !== 32) throw new Error("A route root is a 32-byte Ed25519 public key.");
  if (dayPublicKey?.length !== 32) throw new Error("A day key is a 32-byte Ed25519 public key.");
  if (planRef?.length !== 8) throw new Error("A day certificate planRef is 8 bytes.");
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMTC);
  out.push((certificate.version ?? DAY_CERT_VERSION) & 0xff);
  pushBytes(out, rootPublicKey);
  pushBytes(out, dayPublicKey);
  for (const value of [certificate.generation, certificate.serviceDay]) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
      throw new Error(`A day certificate's generation, serviceDay, notBefore and notAfter are uint32; got ${value}.`);
    }
    pushBytes(out, uint32be(value));
  }
  pushBytes(out, planRef);
  for (const value of [certificate.notBefore, certificate.notAfter]) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
      throw new Error(`A day certificate's notBefore and notAfter are uint32; got ${value}.`);
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
  const generation = readU32be(bytes, state);
  if (generation < 1) throw new Error("A day certificate generation must be positive.");
  const serviceDay = readU32be(bytes, state);
  const planRef = readBytes(bytes, state, 8);
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
    generation,
    serviceDay,
    planRef,
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

/** AAD = every clear authenticated field before the ciphertext length. */
export function threadRecordAad(epochPrefix8, tag, generation, seq, previousHash) {
  if (!Number.isInteger(generation) || generation < 1 || generation > 0xffffffff) {
    throw new Error("A PMT1 authority generation is a positive uint32.");
  }
  if (previousHash?.length !== THREAD_CHAIN_HASH_BYTES) {
    throw new Error(`A PMT1 predecessor hash is ${THREAD_CHAIN_HASH_BYTES} bytes.`);
  }
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMT1);
  pushBytes(out, epochPrefix8);
  pushBytes(out, tag);
  pushVarint(out, generation);
  pushVarint(out, seq);
  pushBytes(out, previousHash);
  return Uint8Array.from(out);
}

export function encodeThreadRecord({
  epochPrefix8, tag, generation, seq, previousHash, ciphertext, admissionTag
}) {
  if (epochPrefix8.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  if (tag.length !== 8) throw new Error("Thread tag must be 8 bytes.");
  if (ciphertext.length < 28) {
    throw new Error("PMT1 sealed body must include its 12-byte nonce and 16-byte authentication tag.");
  }
  if (admissionTag?.length !== THREAD_ADMISSION_TAG_BYTES) {
    throw new Error(`PMT1 admission tag must be ${THREAD_ADMISSION_TAG_BYTES} bytes.`);
  }
  const aad = threadRecordAad(epochPrefix8, tag, generation, seq, previousHash);
  const out = [...aad];
  pushVarint(out, ciphertext.length);
  pushBytes(out, ciphertext);
  pushBytes(out, admissionTag);
  const bytes = Uint8Array.from(out);
  return { bytes, aad };
}

export function readThreadRecord(bytes, state) {
  const start = state.pos;
  expectMagic(bytes, state, THREAD_MAGIC.PMT1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const tag = readBytes(bytes, state, 8);
  const generation = readVarint(bytes, state);
  if (generation < 1 || generation > 0xffffffff) throw new Error("PMT1 authority generation is invalid.");
  const seq = readVarint(bytes, state);
  const previousHash = readBytes(bytes, state, THREAD_CHAIN_HASH_BYTES);
  const aadEnd = state.pos;
  const ctLen = readVarint(bytes, state);
  if (ctLen < 28) throw new Error("PMT1 sealed body is too short.");
  const ciphertext = readBytes(bytes, state, ctLen);
  const admissionTag = readBytes(bytes, state, THREAD_ADMISSION_TAG_BYTES);
  return {
    magic: THREAD_MAGIC.PMT1,
    epochPrefix8,
    tag,
    generation,
    seq,
    previousHash,
    ciphertext,
    admissionTag,
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
    if (!Number.isInteger(entry.sinceGeneration) || entry.sinceGeneration < 0 || entry.sinceGeneration > 0xffffffff) {
      throw new Error("A PMR1 cursor generation is a uint32.");
    }
    pushBytes(out, entry.tag);
    pushVarint(out, entry.sinceGeneration);
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
    const sinceGeneration = readVarint(bytes, state);
    if (sinceGeneration > 0xffffffff) throw new Error("A PMR1 cursor generation is invalid.");
    const sinceSeq = readVarint(bytes, state);
    entries.push({ tag, sinceGeneration, sinceSeq });
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

// --- §20.7.1 the commitment list ------------------------------------------

/**
 * PMTL. A magic and an accumulator, and nothing else.
 *
 * The accumulator names the run, and that is the whole of the addressing.
 * No epochPrefix8 and no tag, for PMTF's reason and one more: a tag is a
 * stable-for-300-seconds name a prober could correlate, while an
 * accumulator is a SHA-256 that changes with every photo and can only be
 * obtained by opening a record — which takes `P`. Asking with one is
 * already proof of being in the audience.
 *
 * A₀ is refused: a run with no photos has no list, and zeros would match
 * every run at once.
 */
export function encodePhotoListRequest({ accumulator }) {
  const bytes = photoChainField(accumulator);
  if (!bytes) throw new Error("A photo list request needs a non-zero 32-byte accumulator.");
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMTL);
  pushBytes(out, bytes);
  return Uint8Array.from(out);
}

export function decodePhotoListRequest(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, THREAD_MAGIC.PMTL);
  const accumulator = toHex(readBytes(bytes, state, THREAD_PHOTO_HASH_BYTES));
  assertNoTrailing(bytes, state, "PMTL request");
  if (accumulator === PHOTO_CHAIN_ZERO_HEX) throw new Error("PMTL names the empty accumulator.");
  return { magic: THREAD_MAGIC.PMTL, accumulator };
}

/**
 * PMTA. The commitment list **in publication order**, which is the order
 * the chain folds them in — a responder that sorts or dedupes this has
 * produced a list that verifies against nothing.
 *
 * Count 0 means "not held", and is also the answer to an accumulator this
 * peer has never seen: the PMM1 rule again, so a prober cannot tell "I am
 * not publishing that run" from "no such run".
 */
export function encodePhotoListResponse({ entries = [] } = {}) {
  if (entries.length > THREAD_MAX_PHOTO_LIST) {
    throw new Error(`A photo list carries at most ${THREAD_MAX_PHOTO_LIST} entries; this one has ${entries.length}.`);
  }
  const out = [];
  pushMagic(out, THREAD_MAGIC.PMTA);
  pushVarint(out, entries.length);
  for (const entry of entries) {
    if (!Number.isInteger(entry.stopIndex) || entry.stopIndex < 1) {
      throw new Error(`A photo list entry names a 1-based stop index; got ${entry.stopIndex}.`);
    }
    pushVarint(out, entry.stopIndex);
    const commitment = photoHashBytes(entry.commitment);
    if (!commitment) throw new Error("A photo list entry needs a 32-byte commitment.");
    pushBytes(out, commitment);
  }
  return Uint8Array.from(out);
}

export function decodePhotoListResponse(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, THREAD_MAGIC.PMTA);
  const count = readVarint(bytes, state);
  if (count > THREAD_MAX_PHOTO_LIST) {
    throw new Error(`PMTA claims ${count} entries and the cap is ${THREAD_MAX_PHOTO_LIST}.`);
  }
  const entries = [];
  for (let i = 0; i < count; i++) {
    const stopIndex = readVarint(bytes, state);
    if (stopIndex < 1) throw new Error("A photo list entry names a 1-based stop index.");
    entries.push({ stopIndex, commitment: toHex(readBytes(bytes, state, THREAD_PHOTO_HASH_BYTES)) });
  }
  assertNoTrailing(bytes, state, "PMTA response");
  return { magic: THREAD_MAGIC.PMTA, entries };
}

// --- §5.4 the link --------------------------------------------------------

/**
 * The capability secret and root verification key are deliberately
 * separate fields. Publishing either one can no longer accidentally
 * publish the other, and authority can rotate without changing topic or
 * content keys.
 */
export function encodeThreadLink({ threadSecret, rootPublicKey, epochPrefix8, notAfter, delegated = false }) {
  if (threadSecret?.length !== 32) throw new Error("A thread capability secret is 32 bytes.");
  if (rootPublicKey?.length !== 32) throw new Error("A thread root verification key is 32 bytes.");
  if (epochPrefix8.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  const out = new Uint8Array(LINK_BYTES);
  out[0] = LINK_VERSION;
  out[1] = delegated ? LINK_FLAG_DELEGATED : 0;
  out.set(threadSecret, 2);
  out.set(rootPublicKey, 34);
  out.set(epochPrefix8, 66);
  out.set(uint32be(notAfter), 74);
  return out;
}

export function decodeThreadLink(bytes) {
  if (bytes.length !== LINK_BYTES) throw new Error(`A thread link is ${LINK_BYTES} bytes.`);
  if (bytes[0] !== LINK_VERSION) throw new Error(`Unsupported thread link version ${bytes[0]}.`);
  if (bytes[1] & ~LINK_FLAG_MASK) throw new Error("Thread link reserved flag bits must be zero.");
  const notAfter = (bytes[74] * 2 ** 24) + (bytes[75] << 16) + (bytes[76] << 8) + bytes[77];
  return {
    version: bytes[0],
    threadSecret: bytes.subarray(2, 34),
    rootPublicKey: bytes.subarray(34, 66),
    epochPrefix8: bytes.subarray(66, 74),
    notAfter,
    delegated: (bytes[1] & LINK_FLAG_DELEGATED) !== 0
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
 * addresses already cost more characters than the thread capability
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
