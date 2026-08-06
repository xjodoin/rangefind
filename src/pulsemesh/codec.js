// PulseMesh wire codecs (protocol §4) and proof-of-work (§5.3).
//
// Every wire object begins with a 4-byte ASCII magic and is
// self-describing. Encoders emit Uint8Array; strict decoders reject
// trailing bytes (mirroring the engine codecs' zero-trailing-bytes rule);
// embedded decoders consume exactly one record from a shared cursor so
// PMB1/PMS1/PMF1 can carry verbatim inner records.

import { pushVarint, readVarint } from "../binary.js";
import { sha256 } from "./sha256.js";

export const MAGIC = Object.freeze({
  PMC1: "PMC1",
  PMB1: "PMB1",
  PMI1: "PMI1",
  PMD1: "PMD1",
  PMG1: "PMG1",
  PMQ1: "PMQ1",
  PMS1: "PMS1",
  PMF1: "PMF1",
  PMN1: "PMN1",
  PMA1: "PMA1",
  PMX1: "PMX1"
});

// Reserved for the thread channel (pulsemesh-threads.md); a traffic-only
// implementation must ignore these, not error on them (§1).
export const RESERVED_MAGIC_PREFIXES = Object.freeze(["PMT", "PMP", "PMR", "PMM"]);

// Proof registry (§5.3). Value 1 was per-record proof-of-work in the
// drafts; §5.4 bonds made it redundant and the value stays burned so no
// old capture can ever validate.
export const PROOF_NONE = 0;        // loopback/local only, never on the wire
export const PROOF_BLIND_TOKEN = 2; // reserved, phase 4
// §5.4 identity bonds: the record carries no proof at all — the
// *delivering peer's* session bond vouches for it, so the admission cost
// is one bond per peer per day instead of a proof per record.
export const PROOF_BOND = 3;

const encoderScratch = new TextEncoder();

function pushMagic(out, magic) {
  for (let i = 0; i < 4; i++) out.push(magic.charCodeAt(i));
}

function pushBytes(out, bytes) {
  for (const byte of bytes) out.push(byte);
}

function readMagic(bytes, state) {
  if (state.pos + 4 > bytes.length) throw new Error("PulseMesh frame truncated before magic.");
  const magic = String.fromCharCode(bytes[state.pos], bytes[state.pos + 1], bytes[state.pos + 2], bytes[state.pos + 3]);
  state.pos += 4;
  return magic;
}

function expectMagic(bytes, state, magic) {
  const found = readMagic(bytes, state);
  if (found !== magic) throw new Error(`Expected ${magic} magic, found ${JSON.stringify(found)}.`);
}

function readBytes(bytes, state, length) {
  if (state.pos + length > bytes.length) throw new Error("PulseMesh frame truncated.");
  const out = bytes.subarray(state.pos, state.pos + length);
  state.pos += length;
  return out;
}

function readU8(bytes, state) {
  if (state.pos >= bytes.length) throw new Error("PulseMesh frame truncated.");
  return bytes[state.pos++];
}

function readBoundedVarint(bytes, state) {
  if (state.pos >= bytes.length) throw new Error("PulseMesh frame truncated.");
  return readVarint(bytes, state);
}

function assertNoTrailing(bytes, state, what) {
  if (state.pos !== bytes.length) throw new Error(`${what} has trailing bytes.`);
}

// --- §2.2 segment identity ----------------------------------------------

export function segmentString(leafCell, geomRef) {
  return `${leafCell}/${geomRef >>> 1}/${geomRef & 1}`;
}

export function parseSegment(segment) {
  const parts = String(segment).split("/");
  if (parts.length !== 3) throw new Error(`Bad segment id ${JSON.stringify(segment)}.`);
  const leafCell = Number(parts[0]);
  const polyline = Number(parts[1]);
  const direction = Number(parts[2]);
  if (!Number.isInteger(leafCell) || leafCell < 0 || !Number.isInteger(polyline) || polyline < 0 ||
      (direction !== 0 && direction !== 1)) {
    throw new Error(`Bad segment id ${JSON.stringify(segment)}.`);
  }
  return { leafCell, geomRef: polyline * 2 + direction };
}

// --- Framing (§4): stream messages travel as varint length ‖ payload. ---

export function pushFramed(out, payload) {
  pushVarint(out, payload.length);
  pushBytes(out, payload);
}

export function encodeFramed(payload) {
  const out = [];
  pushFramed(out, payload);
  return Uint8Array.from(out);
}

/** Reads one frame from `bytes` at `state.pos`; returns the payload. */
export function readFramed(bytes, state) {
  const length = readBoundedVarint(bytes, state);
  return readBytes(bytes, state, length);
}

// --- PMC1 contribution record (§4.1) ------------------------------------

export function encodePMC1(record) {
  const out = [];
  pushMagic(out, MAGIC.PMC1);
  if (record.epochPrefix8.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  pushBytes(out, record.epochPrefix8);
  pushVarint(out, record.leafCell);
  pushVarint(out, record.geomRef);
  pushVarint(out, record.timeBucket);
  out.push(record.speedBin & 0xff);
  out.push(record.qualityBin & 0xff);
  pushVarint(out, record.meters);
  out.push(record.ttlSeconds & 0xff);
  if (record.reportId.length !== 16) throw new Error("reportId must be 16 bytes.");
  pushBytes(out, record.reportId);
  const preimageLength = out.length;
  out.push(record.proofType & 0xff);
  const proof = record.proof || new Uint8Array(0);
  pushVarint(out, proof.length);
  pushBytes(out, proof);
  const bytes = Uint8Array.from(out);
  return { bytes, preimage: bytes.subarray(0, preimageLength) };
}

/** Decodes one embedded PMC1 starting at state.pos. */
export function readPMC1(bytes, state) {
  const start = state.pos;
  expectMagic(bytes, state, MAGIC.PMC1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const leafCell = readBoundedVarint(bytes, state);
  const geomRef = readBoundedVarint(bytes, state);
  const timeBucket = readBoundedVarint(bytes, state);
  const speedBin = readU8(bytes, state);
  const qualityBin = readU8(bytes, state);
  const meters = readBoundedVarint(bytes, state);
  const ttlSeconds = readU8(bytes, state);
  const reportId = readBytes(bytes, state, 16);
  const preimageEnd = state.pos;
  const proofType = readU8(bytes, state);
  const proofLen = readBoundedVarint(bytes, state);
  const proof = readBytes(bytes, state, proofLen);
  return {
    kind: "contribution",
    magic: MAGIC.PMC1,
    epochPrefix8, leafCell, geomRef, timeBucket, speedBin, qualityBin,
    meters, ttlSeconds, reportId, proofType, proof,
    segment: segmentString(leafCell, geomRef),
    bytes: bytes.subarray(start, state.pos),
    preimage: bytes.subarray(start, preimageEnd)
  };
}

export function decodePMC1(bytes) {
  const state = { pos: 0 };
  const record = readPMC1(bytes, state);
  assertNoTrailing(bytes, state, "PMC1 record");
  return record;
}

// --- PMB1 gossip batch (§4.2) -------------------------------------------

export function encodePMB1(records) {
  if (!records.length || records.length > 16) throw new Error("PMB1 carries 1..16 records.");
  const out = [];
  pushMagic(out, MAGIC.PMB1);
  pushVarint(out, records.length);
  for (const record of records) pushBytes(out, record.bytes || record);
  return Uint8Array.from(out);
}

export function decodePMB1(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, MAGIC.PMB1);
  const count = readBoundedVarint(bytes, state);
  if (count < 1 || count > 16) throw new Error("PMB1 count out of range.");
  const records = [];
  for (let i = 0; i < count; i++) records.push(readPMC1(bytes, state));
  assertNoTrailing(bytes, state, "PMB1 batch");
  return { kind: "batch", magic: MAGIC.PMB1, records };
}

// --- PMI1 incident record (§4.3) ----------------------------------------

export const POLARITY_REPORT = 1;
export const POLARITY_CONFIRM = 2;
export const POLARITY_REFUTE = 3;

export function encodePMI1(record) {
  const out = [];
  pushMagic(out, MAGIC.PMI1);
  if (record.epochPrefix8.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  pushBytes(out, record.epochPrefix8);
  pushVarint(out, record.leafCell);
  pushVarint(out, record.geomRef);
  pushVarint(out, record.ratioQ12);
  pushVarint(out, record.timeBucket);
  out.push(record.type & 0xff);
  out.push(record.polarity & 0xff);
  pushVarint(out, record.ttlSeconds);
  if (record.reportId.length !== 16) throw new Error("reportId must be 16 bytes.");
  pushBytes(out, record.reportId);
  const preimageLength = out.length;
  out.push(record.proofType & 0xff);
  const proof = record.proof || new Uint8Array(0);
  pushVarint(out, proof.length);
  pushBytes(out, proof);
  const bytes = Uint8Array.from(out);
  return { bytes, preimage: bytes.subarray(0, preimageLength) };
}

export function readPMI1(bytes, state) {
  const start = state.pos;
  expectMagic(bytes, state, MAGIC.PMI1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const leafCell = readBoundedVarint(bytes, state);
  const geomRef = readBoundedVarint(bytes, state);
  const ratioQ12 = readBoundedVarint(bytes, state);
  const timeBucket = readBoundedVarint(bytes, state);
  const type = readU8(bytes, state);
  const polarity = readU8(bytes, state);
  const ttlSeconds = readBoundedVarint(bytes, state);
  const reportId = readBytes(bytes, state, 16);
  const preimageEnd = state.pos;
  const proofType = readU8(bytes, state);
  const proofLen = readBoundedVarint(bytes, state);
  const proof = readBytes(bytes, state, proofLen);
  return {
    kind: "incident",
    magic: MAGIC.PMI1,
    epochPrefix8, leafCell, geomRef, ratioQ12, timeBucket, type, polarity,
    ttlSeconds, reportId, proofType, proof,
    segment: segmentString(leafCell, geomRef),
    bytes: bytes.subarray(start, state.pos),
    preimage: bytes.subarray(start, preimageEnd)
  };
}

export function decodePMI1(bytes) {
  const state = { pos: 0 };
  const record = readPMI1(bytes, state);
  assertNoTrailing(bytes, state, "PMI1 record");
  return record;
}

// --- PMD1 zone digest (§4.4) --------------------------------------------

export function encodePMD1(digest) {
  const out = [];
  pushMagic(out, MAGIC.PMD1);
  pushBytes(out, digest.epochPrefix8);
  pushVarint(out, digest.zoneX);
  pushVarint(out, digest.zoneY);
  pushVarint(out, digest.baseBucket);
  const entries = [...digest.entries].sort((a, b) => (a.localX - b.localX) || (a.localY - b.localY));
  pushVarint(out, entries.length);
  for (const entry of entries) {
    out.push(entry.localX & 0xff);
    out.push(entry.localY & 0xff);
    pushVarint(out, entry.count);
    pushVarint(out, entry.ageBuckets);
    if (entry.idFold.length !== 8) throw new Error("idFold must be 8 bytes.");
    pushBytes(out, entry.idFold);
  }
  return Uint8Array.from(out);
}

export function decodePMD1(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, MAGIC.PMD1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const zoneX = readBoundedVarint(bytes, state);
  const zoneY = readBoundedVarint(bytes, state);
  const baseBucket = readBoundedVarint(bytes, state);
  const entryCount = readBoundedVarint(bytes, state);
  const entries = [];
  for (let i = 0; i < entryCount; i++) {
    const localX = readU8(bytes, state);
    const localY = readU8(bytes, state);
    if (localX > 63 || localY > 63) throw new Error("PMD1 local coordinates out of range.");
    const count = readBoundedVarint(bytes, state);
    const ageBuckets = readBoundedVarint(bytes, state);
    const idFold = readBytes(bytes, state, 8);
    entries.push({ localX, localY, count, ageBuckets, idFold });
  }
  assertNoTrailing(bytes, state, "PMD1 digest");
  return { kind: "digest", magic: MAGIC.PMD1, epochPrefix8, zoneX, zoneY, baseBucket, entries };
}

// --- Sync stream messages (§4.5) ----------------------------------------

/**
 * PMG1, optionally carrying the requester's own zone fold. Benchmarks put
 * digest exchange at 4–5× the gossip cost, and almost all of it is
 * full-zone PMD1 bodies describing state the requester already has. When
 * `have` is present and matches, the responder answers with a 12-byte
 * PMN1 instead of a 48 KB digest — same round cadence, so convergence is
 * untouched, which an adaptive backoff could not manage.
 */
export function encodePMG1({ epochPrefix8, zoneX, zoneY, have = null }) {
  const out = [];
  pushMagic(out, MAGIC.PMG1);
  pushBytes(out, epochPrefix8);
  pushVarint(out, zoneX);
  pushVarint(out, zoneY);
  if (have) {
    if (have.fold.length !== 8) throw new Error("A zone fold is 8 bytes.");
    pushVarint(out, have.count);
    pushBytes(out, have.fold);
  }
  return Uint8Array.from(out);
}

export function decodePMG1(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, MAGIC.PMG1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const zoneX = readBoundedVarint(bytes, state);
  const zoneY = readBoundedVarint(bytes, state);
  let have = null;
  if (state.pos < bytes.length) {
    const count = readBoundedVarint(bytes, state);
    have = { count, fold: readBytes(bytes, state, 8) };
  }
  assertNoTrailing(bytes, state, "PMG1 request");
  return { kind: "getDigest", magic: MAGIC.PMG1, epochPrefix8, zoneX, zoneY, have };
}

/** PMN1 — "our zones already agree". The cheap answer to a matching fold. */
export function encodePMN1({ epochPrefix8 }) {
  const out = [];
  pushMagic(out, MAGIC.PMN1);
  pushBytes(out, epochPrefix8);
  return Uint8Array.from(out);
}

export function decodePMN1(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, MAGIC.PMN1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  assertNoTrailing(bytes, state, "PMN1 response");
  return { kind: "zonesAgree", magic: MAGIC.PMN1, epochPrefix8 };
}

export function encodePMQ1({ epochPrefix8, cells }) {
  if (![8, 16, 32].includes(cells.length)) throw new Error("PMQ1 cell count must be exactly 8, 16, or 32.");
  const out = [];
  pushMagic(out, MAGIC.PMQ1);
  pushBytes(out, epochPrefix8);
  pushVarint(out, cells.length);
  for (const cell of cells) {
    pushVarint(out, cell.x);
    pushVarint(out, cell.y);
  }
  return Uint8Array.from(out);
}

export function decodePMQ1(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, MAGIC.PMQ1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const cellCount = readBoundedVarint(bytes, state);
  if (![8, 16, 32].includes(cellCount)) throw new Error("PMQ1 cell count must be exactly 8, 16, or 32.");
  const cells = [];
  for (let i = 0; i < cellCount; i++) {
    const x = readBoundedVarint(bytes, state);
    const y = readBoundedVarint(bytes, state);
    cells.push({ x, y });
  }
  assertNoTrailing(bytes, state, "PMQ1 request");
  return { kind: "getCells", magic: MAGIC.PMQ1, epochPrefix8, cells };
}

export function encodePMS1({ epochPrefix8, cells }) {
  const out = [];
  pushMagic(out, MAGIC.PMS1);
  pushBytes(out, epochPrefix8);
  pushVarint(out, cells.length);
  for (const cell of cells) {
    pushVarint(out, cell.x);
    pushVarint(out, cell.y);
    const records = cell.records || [];
    pushVarint(out, records.length);
    for (const record of records) pushBytes(out, record.bytes || record);
    const incidents = cell.incidents || [];
    pushVarint(out, incidents.length);
    for (const incident of incidents) pushBytes(out, incident.bytes || incident);
  }
  return Uint8Array.from(out);
}

export function decodePMS1(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, MAGIC.PMS1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const cellCount = readBoundedVarint(bytes, state);
  const cells = [];
  for (let i = 0; i < cellCount; i++) {
    const x = readBoundedVarint(bytes, state);
    const y = readBoundedVarint(bytes, state);
    const recordCount = readBoundedVarint(bytes, state);
    const records = [];
    for (let r = 0; r < recordCount; r++) records.push(readPMC1(bytes, state));
    const incidentCount = readBoundedVarint(bytes, state);
    const incidents = [];
    for (let r = 0; r < incidentCount; r++) incidents.push(readPMI1(bytes, state));
    cells.push({ x, y, records, incidents });
  }
  assertNoTrailing(bytes, state, "PMS1 snapshot");
  return { kind: "cellSnapshots", magic: MAGIC.PMS1, epochPrefix8, cells };
}

export function encodePMF1({ epochPrefix8, delayMs, payload }) {
  const out = [];
  pushMagic(out, MAGIC.PMF1);
  pushBytes(out, epochPrefix8);
  pushVarint(out, delayMs);
  pushBytes(out, payload);
  return Uint8Array.from(out);
}

export function decodePMF1(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, MAGIC.PMF1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const delayMs = readBoundedVarint(bytes, state);
  const payload = bytes.subarray(state.pos);
  if (!payload.length) throw new Error("PMF1 carries no inner payload.");
  return { kind: "forward", magic: MAGIC.PMF1, epochPrefix8, delayMs, payload };
}

/**
 * Decodes any known stream/gossip payload by magic. Unknown magics are
 * skippable data, not errors (§1): returns { kind: "unknown", magic }.
 */
export function decodeAny(bytes) {
  if (bytes.length < 4) throw new Error("PulseMesh payload shorter than a magic.");
  const magic = String.fromCharCode(bytes[0], bytes[1], bytes[2], bytes[3]);
  switch (magic) {
    case MAGIC.PMC1: return decodePMC1(bytes);
    case MAGIC.PMB1: return decodePMB1(bytes);
    case MAGIC.PMI1: return decodePMI1(bytes);
    case MAGIC.PMD1: return decodePMD1(bytes);
    case MAGIC.PMG1: return decodePMG1(bytes);
    case MAGIC.PMQ1: return decodePMQ1(bytes);
    case MAGIC.PMS1: return decodePMS1(bytes);
    case MAGIC.PMF1: return decodePMF1(bytes);
    case MAGIC.PMN1: return decodePMN1(bytes);
    case MAGIC.PMA1: return decodePMA1(bytes);
    case MAGIC.PMX1: return decodePMX1(bytes);
    default: return { kind: "unknown", magic };
  }
}

// --- §5.1 gossip message id: first 20 bytes of SHA-256 of the payload. --

export function gossipMessageId(payload) {
  return sha256(payload).subarray(0, 20);
}

// --- bit utilities shared with the §5.4 bond puzzle ----------------------

export function leadingZeroBits(bytes) {
  let bits = 0;
  for (const byte of bytes) {
    if (byte === 0) { bits += 8; continue; }
    for (let mask = 0x80; mask; mask >>= 1) {
      if (byte & mask) return bits;
      bits++;
    }
  }
  return bits;
}

// --- §8.4 ban announcement (PMX1) ---------------------------------------
//
// Gossiped when a peer's bond is forfeited on FIRST-HAND evidence (a
// trust-floor from provable validation failures). The target travels as
// a peerId hash, never the id itself; receivers match it against peers
// they have actually met. An announcement is testimony, not a verdict —
// receivers apply corroborated announcements as a trust penalty only,
// and revocation always requires their own first-hand evidence.

export const BAN_REASON_INVALID_RECORDS = 1;

export function encodePMX1({ epochPrefix8, targetHash16, reason, timeBucket, reportId }) {
  const out = [];
  pushMagic(out, MAGIC.PMX1);
  if (epochPrefix8.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  pushBytes(out, epochPrefix8);
  if (targetHash16.length !== 16) throw new Error("targetHash16 must be 16 bytes.");
  pushBytes(out, targetHash16);
  out.push(reason & 0xff);
  pushVarint(out, timeBucket);
  if (reportId.length !== 16) throw new Error("reportId must be 16 bytes.");
  pushBytes(out, reportId);
  return Uint8Array.from(out);
}

export function decodePMX1(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, MAGIC.PMX1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const targetHash16 = readBytes(bytes, state, 16);
  const reason = readU8(bytes, state);
  const timeBucket = readBoundedVarint(bytes, state);
  const reportId = readBytes(bytes, state, 16);
  assertNoTrailing(bytes, state, "PMX1 ban announcement");
  return { kind: "ban", magic: MAGIC.PMX1, epochPrefix8, targetHash16, reason, timeBucket, reportId };
}

// --- §5.4 identity bond admission (PMA1) --------------------------------
//
// A bond is presented once per session on its own stream, never inside a
// record, so its size is irrelevant to MAX_RECORD_BYTES. The peerId it
// binds is deliberately absent from the wire form: the verifier takes it
// from the connection, which is the only place it cannot be lied about.

export function encodePMA1({ epochPrefix8, dayBucket, birthdayBits, pairDifficulty, salt, i, j }) {
  const out = [];
  pushMagic(out, MAGIC.PMA1);
  if (epochPrefix8.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  pushBytes(out, epochPrefix8);
  pushVarint(out, dayBucket);
  out.push(birthdayBits & 0xff);
  out.push(pairDifficulty & 0xff);
  out.push(salt & 0xff);
  for (const nonce of [i, j]) {
    out.push((nonce >>> 24) & 0xff, (nonce >>> 16) & 0xff, (nonce >>> 8) & 0xff, nonce & 0xff);
  }
  return Uint8Array.from(out);
}

export function decodePMA1(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, MAGIC.PMA1);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const dayBucket = readBoundedVarint(bytes, state);
  const birthdayBits = readU8(bytes, state);
  const pairDifficulty = readU8(bytes, state);
  const salt = readU8(bytes, state);
  const readU32 = () => {
    const raw = readBytes(bytes, state, 4);
    return ((raw[0] << 24) | (raw[1] << 16) | (raw[2] << 8) | raw[3]) >>> 0;
  };
  const i = readU32();
  const j = readU32();
  assertNoTrailing(bytes, state, "PMA1 bond");
  return { kind: "bond", magic: MAGIC.PMA1, epochPrefix8, dayBucket, birthdayBits, pairDifficulty, salt, i, j };
}

export function utf8Bytes(text) {
  return encoderScratch.encode(text);
}
