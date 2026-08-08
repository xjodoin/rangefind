// PMH1 seed cards (threads §20.10): how a fleet hands out the one peer
// its devices can dial.
//
// Everything else in this directory is a **capability** — 45 bytes that
// let you follow a run, or a sealed ticket that lets you publish one.
// None of them say how to *reach* the mesh. That is not an oversight
// (§5.4: "a link is a key, not a location"), but it leaves a real gap: a
// cold device with a perfect capability and no dialable peer joins
// nothing, falls back to whatever local transport the host has, and
// shows a customer an empty map.
//
// A seed card closes it. It is a **location**, not a capability: one to
// three multiaddrs and an optional label, and holding one grants exactly
// nothing — every thread on the far side still needs its own key, and
// the seed cannot read a record, forge one, or enumerate a topic.
//
// Which is why it is **not signed**. A signature would be a claim about
// authority, and there is no authority here to claim: the `/p2p/<peerId>`
// suffix of a multiaddr already authenticates the peer at the Noise
// handshake, so a card naming the wrong address does not impersonate a
// seed — it fails to connect. Signing it would only invite a host to
// treat "signed by the dispatcher" as "safe to trust", which is the one
// conclusion a bootstrap address must never license (availability, never
// authority — §12).
//
// The magic is `PMH1`, `H` for **h**ost: what the card names is a host
// to dial. Every other letter that reads as "seed" is spoken for —
// `PMS1` is the traffic channel's, `PMD1` and `PMB1` are taken, `PMO1`
// is the one §20.4 deliberately refuses because `O` and `0` are one
// glyph apart in the fonts these four characters get read back over a
// phone in. `H` is unused, and it is not confusable with `8`, `0`, `1`,
// `5`, `2` or `6` in any terminal font, which is the property that
// matters when the error message is "expected PMH1, found…".

import { pushVarint, readVarint } from "../binary.js";
import { utf8Bytes } from "./codec.js";
import { base64UrlToBytes, bytesToBase64Url } from "./thread_crypto.js";
import {
  THREAD_MAX_BOOTSTRAP_ADDRESSES,
  THREAD_MAX_BOOTSTRAP_BYTES,
  normalizeBootstrapAddresses,
  withBootstrapHint
} from "./thread_codec.js";

export const SEED_MAGIC = Object.freeze({ PMH1: "PMH1" });
export const SEED_CARD_VERSION = 1;
/** A label is for a human in a depot ("Depot seed"), never for a machine. */
export const THREAD_MAX_SEED_LABEL_BYTES = 32;
/** Which optional fields a card's flags byte says are present. */
export const SEED_FIELD = Object.freeze({ LABEL: 0x01 });
const SEED_FIELD_MASK = 0x01;

function pushMagic(out, magic) {
  for (let i = 0; i < 4; i++) out.push(magic.charCodeAt(i));
}

function pushBytes(out, values) {
  for (const value of values) out.push(value);
}

function expectMagic(bytes, state, magic) {
  if (state.pos + 4 > bytes.length) throw new Error(`Expected ${magic}, found a truncated record.`);
  const found = String.fromCharCode(bytes[state.pos], bytes[state.pos + 1], bytes[state.pos + 2], bytes[state.pos + 3]);
  state.pos += 4;
  if (found !== magic) throw new Error(`Expected ${magic}, found ${JSON.stringify(found)}.`);
}

function readBytes(bytes, state, length) {
  if (state.pos + length > bytes.length) throw new Error("PMH1 seed card truncated.");
  const out = bytes.subarray(state.pos, state.pos + length);
  state.pos += length;
  return out;
}

function readU8(bytes, state) {
  if (state.pos >= bytes.length) throw new Error("PMH1 seed card truncated.");
  return bytes[state.pos++];
}

/**
 * A seed card as bytes.
 *
 * `addresses` is 1–3 multiaddrs; `label` is optional and capped at 32
 * bytes, refused rather than truncated for the same reason a device name
 * is (§20.9) — a seed enrolled under a shortened name is a seed somebody
 * mistakes for another depot's.
 */
export function encodeSeedCard({ addresses = [], label = "" } = {}) {
  const list = normalizeBootstrapAddresses(addresses, { what: "seed address" });
  if (!list.length) throw new Error("A seed card names at least one bootstrap address.");
  const labelBytes = label ? utf8Bytes(String(label)) : new Uint8Array(0);
  if (labelBytes.length > THREAD_MAX_SEED_LABEL_BYTES) {
    throw new Error(
      `A seed label is at most ${THREAD_MAX_SEED_LABEL_BYTES} bytes; this one is ${labelBytes.length}.`
    );
  }
  const out = [];
  pushMagic(out, SEED_MAGIC.PMH1);
  out.push(SEED_CARD_VERSION);
  out.push(labelBytes.length ? SEED_FIELD.LABEL : 0);
  pushVarint(out, list.length);
  for (const address of list) {
    const bytes = utf8Bytes(address);
    pushVarint(out, bytes.length);
    pushBytes(out, bytes);
  }
  if (labelBytes.length) {
    pushVarint(out, labelBytes.length);
    pushBytes(out, labelBytes);
  }
  return Uint8Array.from(out);
}

export function decodeSeedCard(bytesOrText) {
  const bytes = typeof bytesOrText === "string"
    ? base64UrlToBytes(bytesOrText.trim().replace(/\s+/gu, ""))
    : bytesOrText;
  const state = { pos: 0 };
  expectMagic(bytes, state, SEED_MAGIC.PMH1);
  const version = readU8(bytes, state);
  if (version !== SEED_CARD_VERSION) throw new Error(`Unsupported PMH1 seed card version ${version}.`);
  const flags = readU8(bytes, state);
  // Reserved bits refused, as everywhere else on this channel: a card
  // whose writer meant a field this reader cannot see is a card this
  // reader does not fully understand, and dialling it anyway is how a
  // host ends up ignoring the half that mattered.
  if (flags & ~SEED_FIELD_MASK) throw new Error("PMH1 reserved flag bits must be zero.");
  const count = readVarint(bytes, state);
  if (!count || count > THREAD_MAX_BOOTSTRAP_ADDRESSES) {
    throw new Error(`A seed card names 1 to ${THREAD_MAX_BOOTSTRAP_ADDRESSES} addresses; this one names ${count}.`);
  }
  const decoder = new TextDecoder();
  const addresses = [];
  for (let i = 0; i < count; i++) {
    const length = readVarint(bytes, state);
    if (!length) throw new Error("A seed card address is empty.");
    if (length > THREAD_MAX_BOOTSTRAP_BYTES) {
      throw new Error(`A seed address is at most ${THREAD_MAX_BOOTSTRAP_BYTES} bytes; this one is ${length}.`);
    }
    addresses.push(decoder.decode(readBytes(bytes, state, length)));
  }
  let label = "";
  if (flags & SEED_FIELD.LABEL) {
    const length = readVarint(bytes, state);
    // A set bit with nothing behind it is a second encoding of one card.
    if (!length) throw new Error("A PMH1 label flag is set with no label behind it.");
    if (length > THREAD_MAX_SEED_LABEL_BYTES) {
      throw new Error(`A seed label is at most ${THREAD_MAX_SEED_LABEL_BYTES} bytes; this one is ${length}.`);
    }
    label = decoder.decode(readBytes(bytes, state, length));
  }
  if (state.pos !== bytes.length) throw new Error("PMH1 seed card has trailing bytes.");
  // The same shape the encoder takes, so re-encoding what was decoded is
  // byte-identical — and refuses the same addresses.
  normalizeBootstrapAddresses(addresses, { what: "seed address" });
  return { version, addresses, label, bytes };
}

/**
 * The card as a link, fragment-carried like every other artifact here.
 *
 * The fragment is not about secrecy in this one case — a seed address is
 * public by nature — it is about **uniformity**: every artifact a host
 * can be handed arrives as `wayfind://<thing>#<base64url>`, one paste
 * field takes all of them, and `classifyThreadArtifact` decides which is
 * which from the magic. A card that arrived in a query would be the one
 * exception a host has to special-case.
 */
export function seedCardUrl(card, baseUrl = "wayfind://seed") {
  const bytes = card instanceof Uint8Array ? card : encodeSeedCard(card);
  return `${baseUrl}#${bytesToBase64Url(bytes)}`;
}

export function parseSeedCardUrl(url) {
  const raw = String(url ?? "").trim();
  const hash = raw.lastIndexOf("#");
  return decodeSeedCard((hash >= 0 ? raw.slice(hash + 1) : raw).replace(/\s+/gu, ""));
}

/**
 * A public link with the card's addresses hinted in its query (§20.10).
 *
 * Convenience over `withBootstrapHint` for the one caller that has a
 * card in hand and a customer link to send; the URL cap (2) is lower
 * than the card's (3), and the extra address is dropped rather than
 * refused — a hint is a hint.
 */
export function seedHintUrl(baseUrl, card) {
  const decoded = card instanceof Uint8Array || typeof card === "string" ? decodeSeedCard(card) : card;
  return withBootstrapHint(baseUrl, (decoded?.addresses || []).slice(0, 2));
}

/**
 * Is this an address a stranger's device could actually dial?
 *
 * Loopback and unspecified addresses are the two that look fine in a
 * terminal and reach nobody: `/ip4/0.0.0.0/tcp/4001` is what the host
 * was *told to listen on*, not where it is, and `/ip4/127.0.0.1/…` is
 * only ever reachable by the machine that printed it. A seed card built
 * from either is a card that fails in the depot, so the tooling that
 * prints one filters with this.
 */
export function isDialableAddress(address) {
  if (typeof address !== "string" || !address.startsWith("/")) return false;
  const parts = address.split("/");
  for (let i = 1; i + 1 < parts.length; i += 2) {
    const protocol = parts[i];
    const value = parts[i + 1];
    if (protocol === "ip4") {
      if (value === "0.0.0.0" || value.startsWith("127.")) return false;
    } else if (protocol === "ip6") {
      if (value === "::" || value === "::1") return false;
    }
  }
  return true;
}
