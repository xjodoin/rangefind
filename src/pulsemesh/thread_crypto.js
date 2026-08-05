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
// any mobile host with a compliant `crypto.subtle`. Hosts without
// Ed25519 (older Hermes) inject one through setThreadCryptoImplementation,
// mirroring the engine's existing `inflate` hook.

import { sha256, toHex } from "./sha256.js";
import { utf8Bytes } from "./codec.js";

export const THREAD_TOPIC_PREFIX = "/rangefind/pulsemesh/1/t";
const TAG_INFO = "pulsemesh/thread/topic/1";
const CONTENT_INFO = "pulsemesh/thread/content/1";
const NONCE_INFO = "pulsemesh/thread/nonce/1";
const TAG_DOMAIN = "pulsemesh/thread/1";

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

async function hkdf(publicKey, info, lengthBytes) {
  const key = await subtle().importKey("raw", publicKey, "HKDF", false, ["deriveBits"]);
  const derived = await subtle().deriveBits(
    { name: "HKDF", hash: "SHA-256", salt: new Uint8Array(0), info: utf8Bytes(info) },
    key,
    lengthBytes * 8
  );
  return bytes(derived);
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

// --- Ed25519 --------------------------------------------------------------

const PKCS8_PREFIX = new Uint8Array([
  0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20
]);

/** Generates a run's keypair. Fresh per run, from a CSPRNG (§4.1). */
export async function generateThreadKeypair(seed = null) {
  const privateSeed = seed || globalThis.crypto.getRandomValues(new Uint8Array(32));
  const publicKey = await publicKeyFromSeed(privateSeed);
  return { privateSeed, publicKey };
}

export async function publicKeyFromSeed(privateSeed) {
  if (injected?.publicKeyFromSeed) return injected.publicKeyFromSeed(privateSeed);
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
  try {
    const key = await subtle().importKey("raw", publicKey, { name: "Ed25519" }, false, ["verify"]);
    return await subtle().verify({ name: "Ed25519" }, key, signature, message);
  } catch {
    return false;
  }
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
