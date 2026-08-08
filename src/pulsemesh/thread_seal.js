// Sealed dispatch tickets and device enrolment (threads §20.9): a
// ticket is **encrypted at all times**, including while it is being
// handed from one driver to the next.
//
// §20.2 made a plan private *on the wire* — records carry an 8-byte
// `planRef` and nothing else, so a follower or a relay is structurally
// incapable of seeing a stop. That left one artifact in the clear, and
// it is the worst one: the ticket itself. Two things are wrong with a
// plaintext ticket.
//
//   - It carries the run's **private seed**, so it is a publish
//     capability. §20.5 states plainly that the protocol cannot tell two
//     seed-holders apart — a photographed QR on a counter is a stranger
//     who can move the vehicle, and no amount of validation downstream
//     can distinguish them from the driver.
//   - Since §20.8 it also carries order references, delivery
//     instructions and **customer phone numbers**. A leaked ticket is a
//     customer list, with the data-protection consequences that follow.
//
// So there is no plaintext ticket form for a host to hand around. A
// ticket is minted (issueTicket, producing the signed PMK1 bytes) and
// sealed to specific devices in one step (`issueSealedTicket`), and what
// leaves the process is PME1: ciphertext that a photograph of a QR code
// turns into nothing at all.
//
// **Enrolment is the gate on transfer.** A device can only receive a job
// if the sender already holds its public key, which means the second
// driver has to have been enrolled — scanned or pasted, in person or by
// whatever channel the operator trusts — *before* the bike breaks. That
// is a deliberate operational cost: it replaces "anyone holding these
// bytes" with "one device the dispatcher named", and there is no way to
// have the second property while keeping the first.
//
// The design is a content key with per-recipient wraps rather than
// per-recipient encryption of the ticket. One random 32-byte key
// encrypts the ticket once; each recipient gets that key wrapped under a
// key agreed with them. Sealing to the driver *and* the issuer therefore
// costs one extra 64-byte wrap, not a second copy of a 600-byte ticket —
// which matters because the QR budget (§20.5) is the binding constraint
// on how many stops a round can have.

import { pushVarint, readVarint } from "../binary.js";
import { sha256, toHex } from "./sha256.js";
import { utf8Bytes } from "./codec.js";
import {
  base64UrlToBytes,
  bytesToBase64Url,
  hkdfBytes,
  x25519PublicKey,
  x25519SharedSecret
} from "./thread_crypto.js";
import { withBootstrapHint } from "./thread_codec.js";

/**
 * `PME1` — the sealed **e**nvelope around a PMK1 ticket, and `PMV1` —
 * the de**v**ice card. The PulseMesh family gives each artifact one free
 * letter (PMC1 contribution, PMB1 batch, PMK1 ticket, PMP1 plan…), and
 * these are the two that were left with a mnemonic: `E` for envelope,
 * which is exactly what this record is, and `V` for de*v*ice, because
 * `D` has belonged to the protocol channel's PMD1 since §4.
 */
export const SEAL_MAGIC = Object.freeze({ PME1: "PME1", PMV1: "PMV1" });
export const SEAL_VERSION = 1;
export const DEVICE_CARD_VERSION = 1;

/** A device name is for a human to recognise, not to store a record in. */
export const THREAD_MAX_DEVICE_NAME_BYTES = 32;
/** A device can hold a job sealed to at most this many recipients. */
export const SEAL_MAX_RECIPIENTS = 16;

export const SEAL_HINT_BYTES = 4;
export const SEAL_NONCE_BYTES = 12;
export const SEAL_TAG_BYTES = 16;
export const SEAL_CONTENT_KEY_BYTES = 32;
/** hint ‖ nonce ‖ wrapped content key ‖ tag. */
export const SEAL_RECIPIENT_BYTES =
  SEAL_HINT_BYTES + SEAL_NONCE_BYTES + SEAL_CONTENT_KEY_BYTES + SEAL_TAG_BYTES;
/** magic ‖ version ‖ ephemeral public key ‖ varint(1) ‖ body nonce ‖ tag. */
export const SEAL_FIXED_BYTES = 4 + 1 + 32 + 1 + SEAL_NONCE_BYTES + SEAL_TAG_BYTES;

const SEAL_INFO = "pulsemesh/seal/1";
const DEVICE_FINGERPRINT_BYTES = 4;

/**
 * Why an `open` failed, because the two answers need different words in
 * front of a driver. `NOT_ENROLLED` means these are a perfectly good
 * sealed ticket for somebody else — the dispatcher sealed it before this
 * device was enrolled, or sealed it to the wrong one, and the fix is a
 * fresh seal. `UNREADABLE` means the bytes are not a sealed ticket this
 * version can parse at all.
 */
export const SEAL_ERROR = Object.freeze({
  NOT_ENROLLED: "sealed-for-another-device",
  UNREADABLE: "unreadable"
});

const NOT_ENROLLED_MESSAGE =
  "This job was sealed for another device. Ask the dispatcher to enrol this "
  + "device and send it again.";
const UNREADABLE_MESSAGE =
  "These bytes are not a sealed job this version can read — they may have "
  + "been issued before a protocol change; ask the dispatcher for a fresh one.";

function sealError(code, message) {
  const error = new Error(message);
  error.code = code;
  return error;
}

function subtle() {
  const api = globalThis.crypto?.subtle;
  if (!api) throw new Error("PulseMesh threads require WebCrypto (globalThis.crypto.subtle).");
  return api;
}

function randomBytes(length) {
  return globalThis.crypto.getRandomValues(new Uint8Array(length));
}

function pushMagic(out, magic) {
  for (let i = 0; i < 4; i++) out.push(magic.charCodeAt(i));
}

function pushBytes(out, values) {
  for (const value of values) out.push(value);
}

function hasMagic(bytes, magic) {
  if (!bytes || bytes.length < 4) return false;
  for (let i = 0; i < 4; i++) if (bytes[i] !== magic.charCodeAt(i)) return false;
  return true;
}

function readBytes(bytes, state, length) {
  if (length < 0 || state.pos + length > bytes.length) throw new Error("Sealed record truncated.");
  const out = bytes.subarray(state.pos, state.pos + length);
  state.pos += length;
  return out;
}

function readU8(bytes, state) {
  if (state.pos >= bytes.length) throw new Error("Sealed record truncated.");
  return bytes[state.pos++];
}

function toBytes(value, what) {
  if (value instanceof Uint8Array) return value;
  if (typeof value === "string") return base64UrlToBytes(value.trim().replace(/\s+/gu, ""));
  throw new Error(`${what} is bytes or a base64url string.`);
}

function sameBytes(a, b) {
  if (!a || !b || a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
  return diff === 0;
}

// --- device identity and enrolment -----------------------------------------

/**
 * A device's long-lived X25519 keypair — the thing enrolment enrols.
 *
 * Deliberately **not** the run seed and not the issuer seed. A run seed
 * is per-job and travels; an issuer seed signs. This one never leaves
 * the device and never signs anything: its only job is to be the address
 * a ticket can be sealed to. `seed` is for tests and for a host that
 * derives device keys from its own secure storage; leave it out and the
 * key comes from the CSPRNG.
 *
 * Key storage is the host's obligation, and it is a real one. This
 * library holds no keychain: an Android host should put `privateKey` in
 * the Keystore, an iOS host in the Secure Enclave-backed keychain, and a
 * browser host should understand that it cannot do either. A device that
 * loses this key cannot open any job already sealed to it, and must
 * enrol again (§20.9).
 */
export async function generateDeviceKeypair(seed = null) {
  const privateKey = seed ? Uint8Array.from(seed) : randomBytes(32);
  if (privateKey.length !== 32) throw new Error("A device private key is 32 bytes.");
  return { privateKey, publicKey: await x25519PublicKey(privateKey) };
}

/**
 * The short hex a human compares aloud before trusting a card.
 *
 * Enrolment over a QR code is only as good as the enroller's ability to
 * tell they scanned the right one — a card is just bytes on a screen,
 * and a screen in a depot can be showing anybody's. Four bytes is not a
 * cryptographic binding and is not claimed to be one; it is eight
 * characters two people can read to each other in three seconds, which
 * is the only comparison that actually happens in a doorway.
 */
export function fingerprint(publicKey) {
  const bytes = toBytes(publicKey, "A device public key");
  if (bytes.length !== 32) throw new Error("A device public key is 32 bytes.");
  return toHex(sha256(bytes).subarray(0, DEVICE_FINGERPRINT_BYTES));
}

/** PMV1: magic ‖ version ‖ publicKey(32) ‖ varint(nameLen) ‖ name. */
export function encodeDeviceCard({ publicKey, name = "" } = {}) {
  const key = toBytes(publicKey, "A device public key");
  if (key.length !== 32) throw new Error("A device public key is 32 bytes.");
  const nameBytes = typeof name === "string" ? utf8Bytes(name) : (name || new Uint8Array(0));
  if (nameBytes.length > THREAD_MAX_DEVICE_NAME_BYTES) {
    throw new Error(
      `A device name is at most ${THREAD_MAX_DEVICE_NAME_BYTES} bytes; this one is ${nameBytes.length}.`
    );
  }
  const out = [];
  pushMagic(out, SEAL_MAGIC.PMV1);
  out.push(DEVICE_CARD_VERSION);
  pushBytes(out, key);
  pushVarint(out, nameBytes.length);
  pushBytes(out, nameBytes);
  return Uint8Array.from(out);
}

export function decodeDeviceCard(bytesOrText) {
  const bytes = toBytes(bytesOrText, "A device card");
  if (!hasMagic(bytes, SEAL_MAGIC.PMV1)) throw new Error("Expected PMV1, found something else.");
  const state = { pos: 4 };
  const version = readU8(bytes, state);
  if (version !== DEVICE_CARD_VERSION) throw new Error(`Unsupported PMV1 device card version ${version}.`);
  const publicKey = readBytes(bytes, state, 32);
  const nameLen = readVarint(bytes, state);
  if (nameLen > THREAD_MAX_DEVICE_NAME_BYTES) {
    throw new Error(`A device name is at most ${THREAD_MAX_DEVICE_NAME_BYTES} bytes; this one is ${nameLen}.`);
  }
  const name = new TextDecoder().decode(readBytes(bytes, state, nameLen));
  if (state.pos !== bytes.length) throw new Error("PMV1 device card has trailing bytes.");
  return { version, publicKey, name, fingerprint: fingerprint(publicKey), bytes };
}

/**
 * The card as a link the other device can scan or be sent, following
 * `threadLinkUrl`'s convention exactly: base64url in the **fragment**,
 * which browsers never transmit and servers never log.
 */
export function deviceCardUrl(card, baseUrl = "wayfind://device") {
  const bytes = card instanceof Uint8Array ? card : encodeDeviceCard(card);
  return `${baseUrl}#${bytesToBase64Url(bytes)}`;
}

/** The inverse, tolerating whatever a mail client wrapped into it. */
export function parseDeviceCardUrl(url) {
  const raw = String(url).trim();
  const hash = raw.lastIndexOf("#");
  return decodeDeviceCard((hash >= 0 ? raw.slice(hash + 1) : raw).replace(/\s+/gu, ""));
}

// --- §20.9 PME1 sealed ticket ----------------------------------------------

/**
 * The 4-byte recipient hint: the head of SHA-256 over the recipient's
 * public key.
 *
 * A **convenience, never an access control.** It is there so a device
 * holding one key does not have to run a key agreement and an AEAD open
 * against every wrap in the record, which on the pure-JS X25519 path is
 * a visible pause on a phone. It proves nothing: anybody can compute the
 * hint for a public key they have seen, and a forged or copied hint
 * simply produces a wrap that does not open. Authentication is the
 * successful open and nothing else — which is why `openSealedTicket`
 * falls back to trying every wrap when no hint matches, rather than
 * concluding from the hints that the ticket is not for this device.
 */
function recipientHint(publicKey) {
  return sha256(publicKey).subarray(0, SEAL_HINT_BYTES);
}

/**
 * The AAD every wrap and the body are bound to: magic ‖ version ‖
 * ephemeral public key.
 *
 * The ephemeral key is what makes this load-bearing. Every wrap key
 * derives from an agreement with *that* key, so a wrap lifted out of one
 * sealed ticket and pasted into another is bound to the wrong AAD and
 * fails the tag check rather than silently handing the second ticket's
 * content key to the first ticket's recipient. Recipients cannot be
 * swapped between messages, and the body cannot be moved under a
 * different recipient set.
 */
function sealAad(ephemeralPublicKey) {
  const out = new Uint8Array(4 + 1 + 32);
  for (let i = 0; i < 4; i++) out[i] = SEAL_MAGIC.PME1.charCodeAt(i);
  out[4] = SEAL_VERSION;
  out.set(ephemeralPublicKey, 5);
  return out;
}

async function wrappingKey(sharedSecret, recipientPublicKey) {
  const label = utf8Bytes(SEAL_INFO);
  const info = new Uint8Array(label.length + 32);
  info.set(label, 0);
  info.set(recipientPublicKey, label.length);
  return hkdfBytes(sharedSecret, info, 32);
}

async function aesGcm(keyBytes, usage) {
  return subtle().importKey("raw", keyBytes, "AES-GCM", false, [usage]);
}

/**
 * Seals signed PMK1 ticket bytes to one or more enrolled devices.
 *
 * `recipientPublicKeys` are X25519 device public keys — from a scanned
 * PMV1 card, or from the issuer's own device. Order is preserved but
 * carries no meaning; a recipient finds itself by hint, or by trying.
 *
 * v1 callers seal to the awarded device and, when the issuer wants to
 * keep the ability to read back what it dispatched, to themselves. That
 * second recipient is an **explicit choice**, not a default: a
 * dispatcher that does not add itself has minted a ticket it can no
 * longer open, which is sometimes exactly right and is never an accident
 * this function makes for it.
 */
export async function sealTicket(ticketBytes, recipientPublicKeys, { ephemeralPrivateKey = null } = {}) {
  const inner = toBytes(ticketBytes, "A dispatch ticket");
  if (!inner.length) throw new Error("A dispatch ticket is required.");
  const recipients = (recipientPublicKeys || []).map(value => {
    const key = value?.publicKey ? toBytes(value.publicKey, "A device public key") : toBytes(value, "A device public key");
    if (key.length !== 32) throw new Error("A device public key is 32 bytes.");
    return key;
  });
  if (!recipients.length) {
    throw new Error("A sealed ticket needs at least one enrolled recipient device.");
  }
  if (recipients.length > SEAL_MAX_RECIPIENTS) {
    throw new Error(`A sealed ticket carries at most ${SEAL_MAX_RECIPIENTS} recipients.`);
  }

  const ephemeralPrivate = ephemeralPrivateKey || randomBytes(32);
  const ephemeralPublic = await x25519PublicKey(ephemeralPrivate);
  const aad = sealAad(ephemeralPublic);
  // One content key per sealing, never derived from anything and never
  // reused: it is what makes a second recipient cost 64 bytes.
  const contentKey = randomBytes(SEAL_CONTENT_KEY_BYTES);

  const wraps = [];
  for (const recipient of recipients) {
    const shared = await x25519SharedSecret(ephemeralPrivate, recipient);
    const nonce = randomBytes(SEAL_NONCE_BYTES);
    const wrapped = new Uint8Array(await subtle().encrypt(
      { name: "AES-GCM", iv: nonce, additionalData: aad, tagLength: 128 },
      await aesGcm(await wrappingKey(shared, recipient), "encrypt"),
      contentKey
    ));
    wraps.push({ hint: recipientHint(recipient), nonce, wrapped });
  }

  const bodyNonce = randomBytes(SEAL_NONCE_BYTES);
  const body = new Uint8Array(await subtle().encrypt(
    { name: "AES-GCM", iv: bodyNonce, additionalData: aad, tagLength: 128 },
    await aesGcm(contentKey, "encrypt"),
    inner
  ));

  const out = [];
  pushMagic(out, SEAL_MAGIC.PME1);
  out.push(SEAL_VERSION);
  pushBytes(out, ephemeralPublic);
  pushVarint(out, wraps.length);
  for (const wrap of wraps) {
    pushBytes(out, wrap.hint);
    pushBytes(out, wrap.nonce);
    pushBytes(out, wrap.wrapped);
  }
  pushBytes(out, bodyNonce);
  pushBytes(out, body);
  return Uint8Array.from(out);
}

/** True when these bytes announce themselves as a sealed ticket. */
export function isSealedTicket(bytes) {
  return hasMagic(bytes instanceof Uint8Array ? bytes : null, SEAL_MAGIC.PME1);
}

/**
 * The framing, without any key. This is what lets a host with no device
 * key say something true about a ticket it cannot open — see
 * `classifyThreadArtifact`.
 */
export function decodeSealedTicketHeader(bytesOrText) {
  const bytes = toBytes(bytesOrText, "A sealed ticket");
  if (!hasMagic(bytes, SEAL_MAGIC.PME1)) throw new Error("Expected PME1, found something else.");
  const state = { pos: 4 };
  const version = readU8(bytes, state);
  if (version !== SEAL_VERSION) throw new Error(`Unsupported PME1 sealed ticket version ${version}.`);
  const ephemeralPublicKey = readBytes(bytes, state, 32);
  const count = readVarint(bytes, state);
  if (!count || count > SEAL_MAX_RECIPIENTS) {
    throw new Error(`A sealed ticket carries 1..${SEAL_MAX_RECIPIENTS} recipients, not ${count}.`);
  }
  const recipients = [];
  for (let i = 0; i < count; i++) {
    recipients.push({
      hint: readBytes(bytes, state, SEAL_HINT_BYTES),
      nonce: readBytes(bytes, state, SEAL_NONCE_BYTES),
      wrapped: readBytes(bytes, state, SEAL_CONTENT_KEY_BYTES + SEAL_TAG_BYTES)
    });
  }
  const bodyNonce = readBytes(bytes, state, SEAL_NONCE_BYTES);
  // The remainder is the body. There is no length prefix and no trailing
  // slack: an AEAD ciphertext is exactly as long as it is, and the tag
  // check is what refuses anything appended — so the zero-trailing-bytes
  // rule (§5) is enforced by the AEAD here rather than by a comparison.
  const ciphertext = bytes.subarray(state.pos);
  if (ciphertext.length <= SEAL_TAG_BYTES) throw new Error("Sealed record truncated.");
  return { version, ephemeralPublicKey, recipients, bodyNonce, ciphertext, bytes };
}

/**
 * Opens a sealed ticket, returning the inner signed PMK1 bytes verbatim.
 *
 * Throws `SEAL_ERROR.NOT_ENROLLED` when the record parses and none of
 * its wraps opens with this device's key — a real, ordinary situation
 * with an action attached (get enrolled, ask for a fresh seal) — and
 * `SEAL_ERROR.UNREADABLE` when the bytes are not a sealed ticket at all.
 * A host must be able to tell a driver which of those happened, because
 * "ask the dispatcher to re-send" fixes one and nothing about the other.
 *
 * The caller still has to `verifyThreadTicket` what comes out. Opening
 * proves who the ticket was *sealed to*; the issuer's signature is what
 * proves who it came *from*, and sealing deliberately does not replace
 * it — the sealer and the issuer need not be the same party.
 */
export async function openSealedTicket(sealed, devicePrivateKey) {
  if (devicePrivateKey?.length !== 32) throw new Error("A device private key is 32 bytes.");
  let header;
  try {
    header = decodeSealedTicketHeader(sealed);
  } catch {
    throw sealError(SEAL_ERROR.UNREADABLE, UNREADABLE_MESSAGE);
  }

  const devicePublicKey = await x25519PublicKey(devicePrivateKey);
  const hint = recipientHint(devicePublicKey);
  // Hinted wraps first, then the rest. Trying the rest is not paranoia:
  // the hint is unauthenticated (see `recipientHint`), so a record whose
  // hints were rewritten in transit would otherwise be reported as "not
  // for this device" when it is.
  const order = [
    ...header.recipients.filter(entry => sameBytes(entry.hint, hint)),
    ...header.recipients.filter(entry => !sameBytes(entry.hint, hint))
  ];

  let shared;
  try {
    shared = await x25519SharedSecret(devicePrivateKey, header.ephemeralPublicKey);
  } catch {
    // A small-order or malformed ephemeral key. Nothing here can open,
    // and the bytes are not a well-formed seal.
    throw sealError(SEAL_ERROR.UNREADABLE, UNREADABLE_MESSAGE);
  }
  const key = await wrappingKey(shared, devicePublicKey);
  const aad = sealAad(header.ephemeralPublicKey);

  for (const entry of order) {
    let contentKey;
    try {
      contentKey = new Uint8Array(await subtle().decrypt(
        { name: "AES-GCM", iv: entry.nonce, additionalData: aad, tagLength: 128 },
        await aesGcm(key, "decrypt"),
        entry.wrapped
      ));
    } catch {
      continue;
    }
    try {
      return new Uint8Array(await subtle().decrypt(
        { name: "AES-GCM", iv: header.bodyNonce, additionalData: aad, tagLength: 128 },
        await aesGcm(contentKey, "decrypt"),
        header.ciphertext
      ));
    } catch {
      // The wrap opened, so this device *is* a recipient — the body is
      // damaged. That is corruption, not an enrolment problem, and
      // saying "sealed for another device" here would send the driver
      // chasing the wrong fix.
      throw sealError(SEAL_ERROR.UNREADABLE, UNREADABLE_MESSAGE);
    }
  }
  throw sealError(SEAL_ERROR.NOT_ENROLLED, NOT_ENROLLED_MESSAGE);
}

/**
 * The driver URL for a sealed ticket — `wayfind://ticket#<base64url>`,
 * the same carrier as before (§20.5), now carrying ciphertext. Hosts
 * keep the `.wayfindjob` file and the QR unchanged; what changed is that
 * photographing either one gets you nothing.
 *
 * `bootstrap` adds the §20.10 query hint. It is redundant on this URL
 * whenever the sealed ticket already carries one — and useful exactly
 * when it does not, because a device that cannot reach the mesh also
 * cannot be told anything by the ticket until it has opened it. The
 * fragment is unchanged either way.
 */
export function sealedTicketUrl(sealed, baseUrl = "wayfind://ticket", { bootstrap = null } = {}) {
  return `${withBootstrapHint(baseUrl, bootstrap)}#${bytesToBase64Url(toBytes(sealed, "A sealed ticket"))}`;
}
