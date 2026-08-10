// Managed thread subscriptions: selective capability distribution and rotation.
//
// A public follow link is intentionally a bearer capability. It is excellent
// for a short-lived delivery, but a copied bearer cannot be selectively
// revoked. PMS1 is the managed alternative: a root-signed capability grant is
// sealed to enrolled X25519 device identities. Rotating to a fresh generation
// and omitting a device revokes that device from all future traffic without
// changing the thread's stable Ed25519 verification identity.

import { utf8Bytes } from "./codec.js";
import { uint32be, publicKeyFromSeed, signThread, verifyThread } from "./thread_crypto.js";
import { encodeThreadLink } from "./thread_codec.js";
import { openSealedTicket, sealTicket } from "./thread_seal.js";

export const SUBSCRIPTION_MAGIC = "PMS1";
export const SUBSCRIPTION_VERSION = 1;
export const SUBSCRIPTION_FLAG_DELEGATED = 0x01;
const SUBSCRIPTION_FLAG_MASK = SUBSCRIPTION_FLAG_DELEGATED;
const SUBSCRIPTION_DOMAIN = "pulsemesh/subscription/1";
export const SUBSCRIPTION_BYTES = 154;

function pushMagic(out) {
  for (let i = 0; i < 4; i++) out.push(SUBSCRIPTION_MAGIC.charCodeAt(i));
}

function pushBytes(out, bytes) {
  for (const byte of bytes) out.push(byte);
}

function readBytes(bytes, state, length) {
  if (state.pos + length > bytes.length) throw new Error("PMS1 subscription grant is truncated.");
  const value = bytes.subarray(state.pos, state.pos + length);
  state.pos += length;
  return value;
}

function readU32(bytes, state) {
  const value = readBytes(bytes, state, 4);
  return (value[0] * 2 ** 24) + (value[1] << 16) + (value[2] << 8) + value[3];
}

function sameBytes(left, right) {
  if (!left || !right || left.length !== right.length) return false;
  let difference = 0;
  for (let i = 0; i < left.length; i++) difference |= left[i] ^ right[i];
  return difference === 0;
}

function signingMessage(preimage) {
  const domain = utf8Bytes(SUBSCRIPTION_DOMAIN);
  const message = new Uint8Array(domain.length + preimage.length);
  message.set(domain, 0);
  message.set(preimage, domain.length);
  return message;
}

function assertUint32(value, name, { positive = false } = {}) {
  if (!Number.isInteger(value) || value < (positive ? 1 : 0) || value > 0xffffffff) {
    throw new Error(`A subscription ${name} is ${positive ? "a positive " : "a "}uint32.`);
  }
}

/** Encodes the root-signed portion of a PMS1 subscription grant. */
export function encodeManagedSubscriptionPreimage({
  generation,
  epochPrefix8,
  notBefore,
  notAfter,
  rootPublicKey,
  threadSecret,
  delegated = false
} = {}) {
  assertUint32(generation, "generation", { positive: true });
  assertUint32(notBefore, "notBefore");
  assertUint32(notAfter, "notAfter");
  if (notAfter <= notBefore) throw new Error("A subscription validity window is empty.");
  if (epochPrefix8?.length !== 8) throw new Error("A subscription graph epoch prefix is 8 bytes.");
  if (rootPublicKey?.length !== 32) throw new Error("A subscription root public key is 32 bytes.");
  if (threadSecret?.length !== 32) throw new Error("A subscription thread secret is 32 bytes.");
  const out = [];
  pushMagic(out);
  out.push(SUBSCRIPTION_VERSION);
  out.push(delegated ? SUBSCRIPTION_FLAG_DELEGATED : 0);
  pushBytes(out, uint32be(generation));
  pushBytes(out, epochPrefix8);
  pushBytes(out, uint32be(notBefore));
  pushBytes(out, uint32be(notAfter));
  pushBytes(out, rootPublicKey);
  pushBytes(out, threadSecret);
  return Uint8Array.from(out);
}

/** Encodes a complete PMS1 grant. The result must be sealed before transport. */
export function encodeManagedSubscription(fields, signature) {
  if (signature?.length !== 64) throw new Error("A subscription signature is 64 bytes.");
  const preimage = encodeManagedSubscriptionPreimage(fields);
  const bytes = new Uint8Array(preimage.length + signature.length);
  bytes.set(preimage, 0);
  bytes.set(signature, preimage.length);
  return bytes;
}

/** Strict PMS1 decoder: one version, known flags only, and no trailing bytes. */
export function decodeManagedSubscription(bytes) {
  if (!(bytes instanceof Uint8Array) || bytes.length !== SUBSCRIPTION_BYTES) {
    throw new Error(`A PMS1 subscription grant is exactly ${SUBSCRIPTION_BYTES} bytes.`);
  }
  const state = { pos: 0 };
  const magic = String.fromCharCode(...readBytes(bytes, state, 4));
  if (magic !== SUBSCRIPTION_MAGIC) throw new Error(`Expected PMS1, found ${JSON.stringify(magic)}.`);
  const version = readBytes(bytes, state, 1)[0];
  if (version !== SUBSCRIPTION_VERSION) throw new Error(`Unsupported PMS1 version ${version}.`);
  const flags = readBytes(bytes, state, 1)[0];
  if (flags & ~SUBSCRIPTION_FLAG_MASK) throw new Error("PMS1 subscription grant has unknown flags.");
  const generation = readU32(bytes, state);
  if (!generation) throw new Error("A subscription generation must be positive.");
  const epochPrefix8 = readBytes(bytes, state, 8);
  const notBefore = readU32(bytes, state);
  const notAfter = readU32(bytes, state);
  if (notAfter <= notBefore) throw new Error("A subscription validity window is empty.");
  const rootPublicKey = readBytes(bytes, state, 32);
  const threadSecret = readBytes(bytes, state, 32);
  const preimageEnd = state.pos;
  const signature = readBytes(bytes, state, 64);
  return {
    version,
    delegated: Boolean(flags & SUBSCRIPTION_FLAG_DELEGATED),
    generation,
    epochPrefix8,
    notBefore,
    notAfter,
    rootPublicKey,
    threadSecret,
    signature,
    preimage: bytes.subarray(0, preimageEnd),
    bytes
  };
}

/**
 * Creates and seals one capability generation to the devices that remain
 * authorized. `threadSecret` is optional so a rotation is fresh by default.
 */
export async function issueManagedSubscription({
  rootSeed,
  recipients,
  generation,
  epochPrefix8,
  notBefore,
  notAfter,
  delegated = false,
  threadSecret = null
} = {}) {
  if (rootSeed?.length !== 32) throw new Error("A subscription root seed is 32 bytes.");
  const secret = threadSecret ?? globalThis.crypto.getRandomValues(new Uint8Array(32));
  const rootPublicKey = await publicKeyFromSeed(rootSeed);
  const fields = {
    generation, epochPrefix8, notBefore, notAfter, rootPublicKey, threadSecret: secret, delegated
  };
  const preimage = encodeManagedSubscriptionPreimage(fields);
  const signature = await signThread(signingMessage(preimage), rootSeed);
  const grant = decodeManagedSubscription(encodeManagedSubscription(fields, signature));
  const sealed = await sealTicket(grant.bytes, recipients);
  return {
    grant,
    sealed,
    link: encodeThreadLink({
      threadSecret: secret,
      rootPublicKey,
      epochPrefix8,
      notAfter,
      delegated
    })
  };
}

/**
 * Opens, authenticates, and time-checks a managed grant. `minimumGeneration`
 * is the caller's persisted rollback guard and must be stored per root.
 */
export async function openManagedSubscription(sealed, devicePrivateKey, {
  expectedRootPublicKey = null,
  minimumGeneration = 0,
  nowSeconds = Math.floor(Date.now() / 1000)
} = {}) {
  const grant = decodeManagedSubscription(await openSealedTicket(sealed, devicePrivateKey));
  if (!(await verifyThread(signingMessage(grant.preimage), grant.signature, grant.rootPublicKey))) {
    throw new Error("The managed subscription signature does not verify.");
  }
  if (expectedRootPublicKey && !sameBytes(expectedRootPublicKey, grant.rootPublicKey)) {
    throw new Error("The managed subscription belongs to a different thread root.");
  }
  if (!Number.isInteger(minimumGeneration) || minimumGeneration < 0) {
    throw new Error("minimumGeneration is a non-negative integer.");
  }
  if (grant.generation < minimumGeneration) {
    throw new Error("The managed subscription is an older generation than this device has accepted.");
  }
  if (nowSeconds < grant.notBefore) throw new Error("The managed subscription is not valid yet.");
  if (nowSeconds > grant.notAfter) throw new Error("The managed subscription has expired.");
  return {
    grant,
    link: encodeThreadLink({
      threadSecret: grant.threadSecret,
      rootPublicKey: grant.rootPublicKey,
      epochPrefix8: grant.epochPrefix8,
      notAfter: grant.notAfter,
      delegated: grant.delegated
    })
  };
}
