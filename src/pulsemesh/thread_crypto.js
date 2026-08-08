// PulseMesh thread cryptography (threads §4, §5.1): everything derives
// from one Ed25519 public key `P`, which is the entire capability.
//
//   private key -> write. Only the publisher can sign, so no link holder
//                  can move the bus — and every subscriber holds the link.
//   public key  -> read + find + verify. Confidentiality is AES-256-GCM
//                  under a key derived from P; authenticity is the
//                  signature verified with P.
//
// This treats a public key as a secret, which is unusual and load-bearing:
// `P` is distributed, never published. It must not be logged, registered,
// reused across runs, used as a peer identity, or put on the wire — the
// topic tag is an HMAC under HKDF(P), the body is sealed, and the
// signature lives inside the sealed body rather than the envelope.
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

import { fromHex, sha256, toHex } from "./sha256.js";
import { utf8Bytes } from "./codec.js";
import { ed25519PublicKeyFromSeed, ed25519Sign, ed25519Verify } from "./ed25519.js";
import { x25519, x25519Base } from "./x25519.js";

export const THREAD_TOPIC_PREFIX = "/rangefind/pulsemesh/1/t";
const TAG_INFO = "pulsemesh/thread/topic/1";
const CONTENT_INFO = "pulsemesh/thread/content/1";
const NONCE_INFO = "pulsemesh/thread/nonce/1";
const TAG_DOMAIN = "pulsemesh/thread/1";
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
export async function deriveThreadKeys(publicKey) {
  if (publicKey.length !== 32) throw new Error("A thread capability is a 32-byte Ed25519 public key.");
  const [topicKey, contentKey, noncePrefix] = await Promise.all([
    hkdf(publicKey, TAG_INFO, 32),
    hkdf(publicKey, CONTENT_INFO, 32),
    hkdf(publicKey, NONCE_INFO, 4)
  ]);
  return { publicKey, topicKey, contentKey, noncePrefix };
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

export function threadNonce(noncePrefix, seq) {
  return new Uint8Array([...noncePrefix, ...uint64be(seq)]);
}

/**
 * AES-256-GCM with the tag appended. The nonce is never transmitted —
 * it is `noncePrefix ‖ uint64be(seq)`, unique by construction — and the
 * AAD binds the ciphertext to its epoch, tag, and sequence number, so a
 * record cannot be replayed into another thread or another window.
 */
export async function sealThreadBody(keys, seq, aad, plaintext) {
  const key = await subtle().importKey("raw", keys.contentKey, "AES-GCM", false, ["encrypt"]);
  const sealed = await subtle().encrypt(
    { name: "AES-GCM", iv: threadNonce(keys.noncePrefix, seq), additionalData: aad, tagLength: 128 },
    key,
    plaintext
  );
  return bytes(sealed);
}

/** Returns null on any AEAD failure — a wrong key, a tampered record. */
export async function openThreadBody(keys, seq, aad, ciphertext) {
  try {
    const key = await subtle().importKey("raw", keys.contentKey, "AES-GCM", false, ["decrypt"]);
    const opened = await subtle().decrypt(
      { name: "AES-GCM", iv: threadNonce(keys.noncePrefix, seq), additionalData: aad, tagLength: 128 },
      key,
      ciphertext
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
 * A customer holds the 45-byte link (§5.4), which is `P` plus an epoch
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
 * Deliberately not the §5.1 convention, where the nonce is derived from
 * `seq` and never transmitted: a photo has no sequence number, is not
 * addressed by one, and is fetched out of band by content hash — so
 * there is nothing to derive a nonce from and the IV has to travel.
 * There is no AAD for the same reason: the binding a record needs comes
 * from the signed commitment on the wire (§5.2 field 18), not from the
 * blob's own framing.
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
  const publicKey = await publicKeyFromSeed(privateSeed);
  return { privateSeed, publicKey };
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
