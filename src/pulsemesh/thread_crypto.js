// PulseMesh thread cryptography (threads §4, §5.1): symmetric capability
// material and signing identity are separate. A link holder receives a
// random-looking thread secret for discovery/decryption/admission and a
// root public key for verification. Publishing either value never
// silently publishes the other.
//
// WebCrypto throughout, so one implementation covers Node, browsers, and
// any mobile host with a compliant `crypto.subtle`. AES-GCM, HKDF and
// HMAC-SHA-256 are on every target host. Ed25519 is not — Chrome only
// enabled it in WebCrypto in 137, so Android WebView 133 and plenty of
// shipping phones throw from `importKey({ name: "Ed25519" })`, and
// Hermes has none at all. Those hosts get ed25519.js, a pure-JS RFC 8032
// implementation, chosen once by a probe at first use rather than
// discovered by an exception on every call. setThreadCryptoImplementation
// still overrides everything for a host that would rather bring its own.

import { pushVarint } from "../binary.js";
import { fromHex, sha256, toHex } from "./sha256.js";
import { utf8Bytes } from "./codec.js";
import { ed25519PublicKeyFromSeed, ed25519Sign, ed25519Verify } from "./ed25519.js";
import { x25519, x25519Base } from "./x25519.js";

export const THREAD_TOPIC_PREFIX = "/rangefind/pulsemesh/1/t";
const TAG_INFO = "pulsemesh/thread/topic/1";
const CONTENT_INFO = "pulsemesh/thread/content/1";
const ADMISSION_INFO = "pulsemesh/thread/admission/1";
const CAPABILITY_INFO = "pulsemesh/thread/capability/1";
const TAG_DOMAIN = "pulsemesh/thread/1";
const ADMISSION_DOMAIN = "pulsemesh/thread/admission/1";
const SIGNED_RECORD_DOMAIN = "pulsemesh/thread/record-signature/1";
const PHOTO_INFO = "pulsemesh/photo/1";

let injected = null;

/**
 * Overrides Ed25519 sign/verify for hosts whose WebCrypto lacks it.
 * `{ sign(message, privateSeed) -> Uint8Array(64),
 *    verify(message, signature, publicKey) -> boolean }`
 */
export function setThreadCryptoImplementation(implementation) {
  injected = implementation;
}

function subtle() {
  const api = globalThis.crypto?.subtle;
  if (!api) throw new Error("PulseMesh threads require WebCrypto (globalThis.crypto.subtle).");
  return api;
}

function bytes(buffer) {
  return new Uint8Array(buffer);
}

export function uint64be(value) {
  const out = new Uint8Array(8);
  let remaining = BigInt(value);
  for (let i = 7; i >= 0; i--) {
    out[i] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
  return out;
}

export function uint32be(value) {
  const out = new Uint8Array(4);
  out[0] = (value >>> 24) & 0xff;
  out[1] = (value >>> 16) & 0xff;
  out[2] = (value >>> 8) & 0xff;
  out[3] = value & 0xff;
  return out;
}

// --- §4.1 key schedule ----------------------------------------------------

/**
 * HKDF-SHA-256 with an empty salt over raw `info` bytes. Exported for
 * thread_seal.js, whose `info` is a string followed by a public key and
 * therefore cannot go through the string-only `hkdf` below.
 */
export async function hkdfBytes(ikm, info, lengthBytes) {
  const key = await subtle().importKey("raw", ikm, "HKDF", false, ["deriveBits"]);
  const derived = await subtle().deriveBits(
    { name: "HKDF", hash: "SHA-256", salt: new Uint8Array(0), info },
    key,
    lengthBytes * 8
  );
  return bytes(derived);
}

async function hkdf(publicKey, info, lengthBytes) {
  return hkdfBytes(publicKey, utf8Bytes(info), lengthBytes);
}

/**
 * The whole key schedule from the capability `P`. A subscriber runs this
 * on the 32 bytes it got in a link and can then find, open, and verify
 * the thread — and do nothing else.
 */
export async function deriveThreadKeys(threadSecret) {
  if (threadSecret?.length !== 32) throw new Error("A thread capability secret is a 32-byte value.");
  const [topicKey, contentKey, admissionKey] = await Promise.all([
    hkdf(threadSecret, TAG_INFO, 32),
    hkdf(threadSecret, CONTENT_INFO, 32),
    hkdf(threadSecret, ADMISSION_INFO, 32)
  ]);
  return { threadSecret, topicKey, contentKey, admissionKey };
}

/**
 * Derives capability material from a root seed without reusing its
 * Ed25519 public key as a symmetric secret. The link carries this value
 * beside the root verification key; neither can substitute for the
 * other after compromise or accidental publication.
 */
export async function deriveThreadSecret(rootSeed) {
  if (rootSeed?.length !== 32) throw new Error("A thread root seed is 32 bytes.");
  return hkdfBytes(rootSeed, utf8Bytes(CAPABILITY_INFO), 32);
}

// --- §4.2 finding the thread ----------------------------------------------

export function threadWindow(unixMillis) {
  return Math.floor(unixMillis / 1000 / 300);
}

/**
 * The rotating 8-byte topic tag. Pseudorandom — it carries no zone, route
 * number, or operator — so threads cannot be enumerated, and successive
 * windows are unlinkable without `K_topic`.
 */
export async function threadTag(keys, epoch32, window) {
  const key = await subtle().importKey("raw", keys.topicKey, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const message = new Uint8Array([...utf8Bytes(TAG_DOMAIN), ...epoch32, ...uint64be(window)]);
  const mac = bytes(await subtle().sign("HMAC", key, message));
  return mac.subarray(0, 8);
}

/** Cheap capability check over a sealed record, before AEAD/signatures. */
export async function threadAdmissionTag(keys, aad, ciphertext, length = 16) {
  const key = await subtle().importKey("raw", keys.admissionKey, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const message = new Uint8Array(utf8Bytes(ADMISSION_DOMAIN).length + aad.length + ciphertext.length);
  let offset = 0;
  const domain = utf8Bytes(ADMISSION_DOMAIN);
  message.set(domain, offset); offset += domain.length;
  message.set(aad, offset); offset += aad.length;
  message.set(ciphertext, offset);
  return bytes(await subtle().sign("HMAC", key, message)).subarray(0, length);
}

/** Constant-time comparison for short authentication fields. */
export function equalThreadAuth(left, right) {
  if (!(left instanceof Uint8Array) || !(right instanceof Uint8Array) || left.length !== right.length) return false;
  let difference = 0;
  for (let i = 0; i < left.length; i++) difference |= left[i] ^ right[i];
  return difference === 0;
}

/**
 * Binds the inner signed state to generation, sequence, predecessor,
 * epoch and rotating topic. A link holder can re-encrypt a body but can
 * no longer transplant a valid signature into a new envelope.
 */
export function threadRecordSigningMessage(aad, bodyPreimage) {
  const domain = utf8Bytes(SIGNED_RECORD_DOMAIN);
  return new Uint8Array([...domain, ...sha256(aad), ...bodyPreimage]);
}

export function threadTopic(epochPrefix16hex, tag) {
  return `${THREAD_TOPIC_PREFIX}/${epochPrefix16hex}/${toHex(tag)}`;
}

/**
 * The DHT key holders advertise as providers for. This is what makes the
 * link self-sufficient: derive the tag, derive this, ask who provides it,
 * connect. No directory, no host, no address in the link.
 */
export function threadRendezvous(topic) {
  return sha256(utf8Bytes(topic));
}

/** Tags for the windows a subscriber should currently be listening on. */
export async function threadTagsForWindows(keys, epoch32, windows) {
  return Promise.all(windows.map(window => threadTag(keys, epoch32, window)));
}

// --- §5.1 sealing ---------------------------------------------------------

/** The NIST-recommended size for a randomly generated AES-GCM nonce. */
export const THREAD_NONCE_BYTES = 12;

/**
 * AES-256-GCM with a fresh 96-bit nonce prepended and the tag appended.
 * The nonce travels inside PMT1's sealed-body field. This is deliberately
 * random per record: route links keep one content key across service days,
 * and a publisher can restart without a peer from which to recover `seq`.
 * Neither event can therefore repeat an IV.
 */
export async function sealThreadBody(keys, _seq, aad, plaintext, { nonce = null } = {}) {
  const key = await subtle().importKey("raw", keys.contentKey, "AES-GCM", false, ["encrypt"]);
  const iv = nonce || globalThis.crypto.getRandomValues(new Uint8Array(THREAD_NONCE_BYTES));
  if (iv.length !== THREAD_NONCE_BYTES) throw new Error("A thread record nonce is 12 bytes.");
  const sealed = await subtle().encrypt(
    { name: "AES-GCM", iv, additionalData: aad, tagLength: 128 },
    key,
    plaintext
  );
  const ciphertext = bytes(sealed);
  const out = new Uint8Array(iv.length + ciphertext.length);
  out.set(iv, 0);
  out.set(ciphertext, iv.length);
  return out;
}

/** Returns null on any AEAD failure — a wrong key, a tampered record. */
export async function openThreadBody(keys, _seq, aad, ciphertext) {
  try {
    if (ciphertext.length < THREAD_NONCE_BYTES + 16) return null;
    const iv = ciphertext.subarray(0, THREAD_NONCE_BYTES);
    const sealed = ciphertext.subarray(THREAD_NONCE_BYTES);
    const key = await subtle().importKey("raw", keys.contentKey, "AES-GCM", false, ["decrypt"]);
    const opened = await subtle().decrypt(
      { name: "AES-GCM", iv, additionalData: aad, tagLength: 128 },
      key,
      sealed
    );
    return bytes(opened);
  } catch {
    return null;
  }
}

// --- §20.7 proof-of-delivery photos ---------------------------------------

/**
 * The largest sealed blob this channel will carry. A photo is not a
 * record: it never rides gossip, so this bounds one on-demand transfer
 * rather than the flood, and 128 KiB is a generous 1024 px JPEG.
 */
export const THREAD_MAX_PHOTO_BYTES = 131072;

/** Every seal prepends its own IV; that plus the GCM tag is the overhead. */
export const PHOTO_SEAL_OVERHEAD = 12 + 16;

/**
 * The per-stop photo key, and the whole privacy model in one derivation.
 *
 * It comes from the run's **private seed**, not from the capability `P`.
 * A customer holds the 78-byte link (§5.4), which carries a distinct
 * thread secret and verification key plus an epoch
 * and an expiry — no seed — so a customer structurally cannot derive
 * this, whatever they do with the bytes they can see. The two parties
 * that can are the driver, who was handed the seed in the ticket, and
 * the **dispatcher**, who kept it when minting that ticket (§20.1's
 * named inversion of §4.1). Photos are therefore dispatcher-only in v1,
 * by construction rather than by policy.
 *
 * `planRef` and `stopIndex` are in the info string, so one leaked photo
 * key opens one stop of one job.
 */
export async function photoKeyFor(privateSeed, planRef, stopIndex) {
  if (privateSeed?.length !== 32) throw new Error("A photo key derives from the run's 32-byte private seed.");
  if (!Number.isInteger(stopIndex) || stopIndex < 1) {
    throw new Error(`A photo belongs to a 1-based plan stop, not ${stopIndex}.`);
  }
  const ref = planRef && planRef.length === 8 ? planRef : new Uint8Array(8);
  const info = new Uint8Array([...utf8Bytes(PHOTO_INFO), ...ref, ...uint32be(stopIndex)]);
  return hkdfBytes(privateSeed, info, 32);
}

/**
 * AES-256-GCM with a fresh CSPRNG IV **prepended** to the ciphertext.
 *
 * The same §5.1 convention as a PMT1 record: a fresh random nonce travels
 * with the ciphertext. A photo has no sequence number, is not addressed
 * by one, and is fetched out of band by content hash.
 * There is no AAD for the same reason: the binding a record needs comes
 * from the signed accumulator over the commitment (§5.2 field 21), not
 * from the blob's own framing.
 */
export async function sealPhoto(key, plaintext, { iv = null } = {}) {
  const nonce = iv || globalThis.crypto.getRandomValues(new Uint8Array(12));
  if (nonce.length !== 12) throw new Error("A photo IV is 12 bytes.");
  const aes = await subtle().importKey("raw", key, "AES-GCM", false, ["encrypt"]);
  const sealed = bytes(await subtle().encrypt({ name: "AES-GCM", iv: nonce, tagLength: 128 }, aes, plaintext));
  const out = new Uint8Array(nonce.length + sealed.length);
  out.set(nonce, 0);
  out.set(sealed, nonce.length);
  return out;
}

/** The commitment that goes on the wire: SHA-256 of the *sealed* bytes. */
export function photoCommitment(sealed) {
  return sha256(sealed);
}

function normalizeHash(hash) {
  if (hash instanceof Uint8Array) {
    if (hash.length !== 32) throw new Error("A photo commitment is 32 bytes.");
    return toHex(hash);
  }
  const text = String(hash || "").toLowerCase();
  if (!/^[0-9a-f]{64}$/u.test(text)) throw new Error("A photo commitment is 32 bytes or 64 hex characters.");
  return text;
}

/**
 * Opens a fetched blob for the dispatcher (or the driver's own device).
 *
 * `hash` is **required** and checked first, against the sealed bytes,
 * before a key is even imported. The commitment is the only thing the
 * signature covers, so bytes that do not hash to it are not this photo
 * no matter how well they decrypt — and refusing them here is what
 * stops a peer substituting a different image under a valid record.
 * Throws rather than returning null: a caller rendering a proof of
 * delivery must not be able to ignore the failure by accident.
 */
export async function openPhoto(sealed, { privateSeed, planRef, stopIndex, hash }) {
  const wanted = normalizeHash(hash);
  if (!(sealed instanceof Uint8Array) || sealed.length <= PHOTO_SEAL_OVERHEAD) {
    throw new Error("That is not a sealed photo.");
  }
  if (toHex(photoCommitment(sealed)) !== wanted) {
    throw new Error("These bytes do not match the commitment the driver signed.");
  }
  const key = await photoKeyFor(privateSeed, planRef, stopIndex);
  const aes = await subtle().importKey("raw", key, "AES-GCM", false, ["decrypt"]);
  try {
    return bytes(await subtle().decrypt(
      { name: "AES-GCM", iv: sealed.subarray(0, 12), tagLength: 128 },
      aes,
      sealed.subarray(12)
    ));
  } catch {
    // The hash matched, so these are the committed bytes — the key is
    // wrong. A viewer holding only the link lands here, and so does a
    // seed for the wrong job or the wrong stop.
    throw new Error("This photo does not open with that run seed, plan and stop.");
  }
}

/** Bytes for a commitment given as hex or bytes. */
export function photoHashBytes(hash) {
  return hash instanceof Uint8Array ? hash : fromHex(normalizeHash(hash));
}

/** Hex for a commitment given as hex or bytes. */
export function photoHashHex(hash) {
  return normalizeHash(hash);
}

// --- §20.7.1 the photo accumulator ----------------------------------------
//
// One 32-byte field in the signed body binds **every** commitment the run
// has published, in order, each to the stop it was taken at. The record
// carries the head; the list is fetched from the publisher and checked
// against a head the publisher already signed and gossiped.
//
// That is what keeps the evidence property §20.7 exists for. A publisher
// restating its own list is the party who might lie — but it is restating
// it against a value it committed to at the door, inside a record whose
// `unixSeconds` and `seq` place it there. Insert, drop, reorder or
// backdate anything and the chain no longer reproduces that value.

const PHOTO_CHAIN_INFO = "pulsemesh/photo-chain/1";

/** A₀: the accumulator of a run that has published no photos. */
export const PHOTO_CHAIN_ZERO = "0".repeat(64);

/**
 * Aᵢ = SHA-256( "pulsemesh/photo-chain/1" ‖ Aᵢ₋₁ ‖ varint(stopIndex) ‖ commitmentᵢ ).
 *
 * The stop index is inside the hash, not merely alongside it: a
 * commitment names a blob, and which door that blob was taken at is
 * exactly what a dispute is about. It is also the second half of the
 * photo key (§20.7), so binding it here means a list entry cannot be
 * re-pointed at another stop without the chain breaking first.
 */
export function photoChainStep(previous, stopIndex, commitment) {
  if (!Number.isInteger(stopIndex) || stopIndex < 1) {
    throw new Error(`A photo chain entry names a 1-based plan stop, not ${stopIndex}.`);
  }
  const head = photoHashBytes(previous ?? PHOTO_CHAIN_ZERO);
  const varint = [];
  pushVarint(varint, stopIndex);
  const info = utf8Bytes(PHOTO_CHAIN_INFO);
  const commitmentBytes = photoHashBytes(commitment);
  const message = new Uint8Array(info.length + 32 + varint.length + 32);
  message.set(info, 0);
  message.set(head, info.length);
  message.set(varint, info.length + 32);
  message.set(commitmentBytes, info.length + 32 + varint.length);
  return toHex(sha256(message));
}

/** The head after folding `entries` — `[{ stopIndex, commitment }]` — into A₀. */
export function photoChainOf(entries) {
  let head = PHOTO_CHAIN_ZERO;
  for (const entry of entries || []) head = photoChainStep(head, entry.stopIndex, entry.commitment);
  return head;
}

/**
 * Splits a publisher's list at the point a signed accumulator vouches for
 * it (§20.7.1).
 *
 * A holder's accumulator comes from whatever record it last accepted, so
 * it is routinely *older* than the list it is handed. That is not a
 * failure and it is not all-or-nothing: the prefix that reproduces the
 * held head is exactly as good as evidence as it ever was, and everything
 * past it is a claim the holder has not yet seen signed. They are
 * returned apart, with stop indices, so a caller can neither accept the
 * unverified ones by accident nor be told nothing about them.
 *
 * `matchedAt === 0` with a non-empty list means no prefix reproduces the
 * head: the list belongs to another run, or the publisher rewrote it.
 * Nothing in it is verified.
 */
export function verifyPhotoChain(entries, accumulator) {
  const wanted = accumulator == null ? PHOTO_CHAIN_ZERO : normalizeHash(accumulator);
  const list = [...(entries || [])];
  let head = PHOTO_CHAIN_ZERO;
  let matchedAt = wanted === PHOTO_CHAIN_ZERO ? 0 : -1;
  for (let i = 0; i < list.length && matchedAt < 0; i++) {
    head = photoChainStep(head, list[i].stopIndex, list[i].commitment);
    // First match wins. A second position holding the same head would be
    // a SHA-256 collision, not an ambiguity worth resolving.
    if (head === wanted) matchedAt = i + 1;
  }
  const cut = matchedAt < 0 ? 0 : matchedAt;
  return {
    matchedAt: cut,
    /** Bound by a head the publisher signed. Safe to fetch and to believe. */
    verified: list.slice(0, cut).map(entry => ({
      stopIndex: entry.stopIndex,
      commitment: photoHashHex(entry.commitment)
    })),
    /** Published after the newest record this holder has. Named, never accepted. */
    unverified: list.slice(cut).map(entry => ({
      stopIndex: entry.stopIndex,
      commitment: photoHashHex(entry.commitment)
    }))
  };
}

// --- Ed25519 --------------------------------------------------------------

const PKCS8_PREFIX = new Uint8Array([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20
]);

// A host either has WebCrypto Ed25519 or it does not, and that never
// changes mid-process — so probe once, on first use, and remember. The
// probe runs the whole round trip the three functions below need
// (pkcs8 import, jwk export, sign, raw import, verify), because a host
// can have the import and still refuse the operation, and a half-present
// algorithm discovered on the third call is worse than none.
let nativeEd25519Probe = null;

function nativeEd25519Available() {
  if (!nativeEd25519Probe) {
    nativeEd25519Probe = (async () => {
      try {
        const seed = new Uint8Array(32);
        const key = await subtle().importKey(
          "pkcs8", new Uint8Array([...PKCS8_PREFIX, ...seed]), { name: "Ed25519" }, true, ["sign"]
        );
        const jwk = await subtle().exportKey("jwk", key);
        const probe = utf8Bytes("pulsemesh/thread/ed25519-probe");
        const signature = await subtle().sign({ name: "Ed25519" }, key, probe);
        const publicKey = await subtle().importKey(
          "raw", base64UrlToBytes(jwk.x), { name: "Ed25519" }, false, ["verify"]
        );
        return await subtle().verify({ name: "Ed25519" }, publicKey, signature, probe);
      } catch {
        return false;
      }
    })();
  }
  return nativeEd25519Probe;
}

/** Generates a run's keypair. Fresh per run, from a CSPRNG (§4.1). */
export async function generateThreadKeypair(seed = null) {
  const privateSeed = seed || globalThis.crypto.getRandomValues(new Uint8Array(32));
  const [publicKey, threadSecret] = await Promise.all([
    publicKeyFromSeed(privateSeed),
    deriveThreadSecret(privateSeed)
  ]);
  return { privateSeed, publicKey, threadSecret };
}

export async function publicKeyFromSeed(privateSeed) {
  if (injected?.publicKeyFromSeed) return injected.publicKeyFromSeed(privateSeed);
  if (!(await nativeEd25519Available())) return ed25519PublicKeyFromSeed(privateSeed);
  const key = await subtle().importKey(
    "pkcs8",
    new Uint8Array([...PKCS8_PREFIX, ...privateSeed]),
    { name: "Ed25519" },
    true,
    ["sign"]
  );
  // WebCrypto cannot export a public key from a private one, so derive it
  // by signing nothing and reading it back is not possible either — go
  // through JWK, which carries the public half.
  const jwk = await subtle().exportKey("jwk", key);
  return base64UrlToBytes(jwk.x);
}

export async function signThread(message, privateSeed) {
  if (injected?.sign) return injected.sign(message, privateSeed);
  if (!(await nativeEd25519Available())) return ed25519Sign(message, privateSeed);
  const key = await subtle().importKey(
    "pkcs8",
    new Uint8Array([...PKCS8_PREFIX, ...privateSeed]),
    { name: "Ed25519" },
    false,
    ["sign"]
  );
  return bytes(await subtle().sign({ name: "Ed25519" }, key, message));
}

export async function verifyThread(message, signature, publicKey) {
  if (injected?.verify) return injected.verify(message, signature, publicKey);
  if (!(await nativeEd25519Available())) return ed25519Verify(message, signature, publicKey);
  try {
    const key = await subtle().importKey("raw", publicKey, { name: "Ed25519" }, false, ["verify"]);
    // A `false` here is a real answer — this key did not sign this
    // message — and must never be retried against the fallback. Only a
    // throw is ambiguous, and by this point the probe has already ruled
    // out a missing algorithm, so a throw means a malformed key or
    // signature: also a failed verification, never an exception at the
    // caller, who reads this as "the issuer's signature does not verify".
    return await subtle().verify({ name: "Ed25519" }, key, signature, message);
  } catch {
    return false;
  }
}

// --- X25519 (§20.9 device sealing) ----------------------------------------

// Same PKCS#8 wrapper as Ed25519's, with X25519's OID: 1.3.101.110
// (0x2b 0x65 0x6e) where Ed25519 has 1.3.101.112 (0x2b 0x65 0x70).
const X25519_PKCS8_PREFIX = new Uint8Array([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x6e, 0x04, 0x22, 0x04, 0x20
]);

// RFC 7748 §6.1, Bob's private key and Alice's public key, and the
// shared secret they agree on. The probe below computes it.
const X25519_PROBE_PRIVATE = fromHex("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
const X25519_PROBE_PUBLIC = fromHex("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
const X25519_PROBE_SHARED = "4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742";

// One probe, once, exactly as Ed25519's above — and for a sharper
// reason. WebCrypto X25519 is *newer* than WebCrypto Ed25519 on every
// engine that has either (Chrome 133, Safari 17.4), so this fallback is
// reached more often, and a host that half-supports the algorithm must
// be found here rather than on the third call. The probe runs the whole
// round trip the two functions below need — pkcs8 import, jwk export of
// the public half, raw import of a peer key, deriveBits — and then
// checks the answer against RFC 7748 §6.1 rather than merely observing
// that nothing threw. Past this point a rejection from deriveBits is a
// *real answer* about those keys (a small-order peer point, a malformed
// key) and propagates to the caller; it never silently reroutes to the
// pure implementation, because two code paths disagreeing about whether
// a device can open a ticket is worse than either answer.
let nativeX25519Probe = null;

export function nativeX25519Available() {
  if (!nativeX25519Probe) {
    nativeX25519Probe = (async () => {
      try {
        const key = await subtle().importKey(
          "pkcs8",
          new Uint8Array([...X25519_PKCS8_PREFIX, ...X25519_PROBE_PRIVATE]),
          { name: "X25519" },
          true,
          ["deriveBits"]
        );
        const jwk = await subtle().exportKey("jwk", key);
        if (toHex(base64UrlToBytes(jwk.x)) !== toHex(x25519Base(X25519_PROBE_PRIVATE))) return false;
        const peer = await subtle().importKey("raw", X25519_PROBE_PUBLIC, { name: "X25519" }, false, []);
        const shared = bytes(await subtle().deriveBits({ name: "X25519", public: peer }, key, 256));
        return toHex(shared) === X25519_PROBE_SHARED;
      } catch {
        return false;
      }
    })();
  }
  return nativeX25519Probe;
}

/** The 32-byte X25519 public key for a 32-byte private key. */
export async function x25519PublicKey(privateKey) {
  if (privateKey?.length !== 32) throw new Error("An X25519 private key is 32 bytes.");
  if (!(await nativeX25519Available())) return x25519Base(privateKey);
  const key = await subtle().importKey(
    "pkcs8",
    new Uint8Array([...X25519_PKCS8_PREFIX, ...privateKey]),
    { name: "X25519" },
    true,
    ["deriveBits"]
  );
  // WebCrypto will not hand back a public key from a private one, so go
  // through JWK, which carries it — the same detour publicKeyFromSeed
  // takes for Ed25519.
  return base64UrlToBytes((await subtle().exportKey("jwk", key)).x);
}

/**
 * The raw X25519 shared secret. Never used as a key directly — §20.9
 * runs it through HKDF with the recipient's public key in the info, so
 * two recipients of one ticket derive unrelated wrapping keys.
 */
export async function x25519SharedSecret(privateKey, peerPublicKey) {
  if (privateKey?.length !== 32) throw new Error("An X25519 private key is 32 bytes.");
  if (peerPublicKey?.length !== 32) throw new Error("An X25519 public key is 32 bytes.");
  if (!(await nativeX25519Available())) return x25519(privateKey, peerPublicKey);
  const key = await subtle().importKey(
    "pkcs8",
    new Uint8Array([...X25519_PKCS8_PREFIX, ...privateKey]),
    { name: "X25519" },
    false,
    ["deriveBits"]
  );
  const peer = await subtle().importKey("raw", peerPublicKey, { name: "X25519" }, false, []);
  return bytes(await subtle().deriveBits({ name: "X25519", public: peer }, key, 256));
}

// --- base64url (links travel in URL fragments) ----------------------------

export function bytesToBase64Url(input) {
  let binary = "";
  for (const byte of input) binary += String.fromCharCode(byte);
  const base64 = typeof btoa === "function" ? btoa(binary) : Buffer.from(input).toString("base64");
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function base64UrlToBytes(text) {
  const base64 = String(text).replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
  if (typeof atob === "function") {
    const binary = atob(padded);
    const out = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
    return out;
  }
  return new Uint8Array(Buffer.from(padded, "base64"));
}
