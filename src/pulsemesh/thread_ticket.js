// Dispatch tickets (threads §20): the run's identity belongs to the
// **job**, not to whoever ends up driving it.
//
// Everywhere else on this channel the private seed never leaves the
// device that minted it (§4.1), and that rule is right whenever the
// publisher is also the origin of the run. A dispatched job inverts the
// order of events: a restaurant composes a delivery and hands the
// customer a follow link at composition time, before a courier has been
// found — possibly before the job has been offered to anyone. The link
// is a function of the run key (§5.4), so a key minted by the driver
// means the customer can be told nothing until a driver exists, and a
// reassignment invalidates every link already sent.
//
// So a ticket carries the run's Ed25519 private seed, deliberately,
// under the issuer's signature. What that buys:
//
//   - **Links before drivers.** `ticketFollowLink` derives the 45-byte
//     link from the ticket alone, so the customer's link is printed on
//     the order confirmation while the job is still unassigned.
//   - **The dispatcher keeps cancel and reassign.** It holds the seed,
//     so it can close the run out itself, or hand the same ticket to a
//     second courier without reissuing one customer link.
//   - **Mid-run handover.** Driver B resumes the same thread above
//     driver A's last `seq` (`publishTicket` in thread_session.js), so
//     followers see one continuous run rather than a dead link.
//
// The cost, stated plainly: a ticket is a *publish* capability, and
// anyone holding these bytes can move the vehicle — §20.5 says the
// protocol cannot tell two seed-holders apart. Which is why **these
// bytes never leave the process**: since §20.9 a ticket is sealed to
// enrolled devices before it travels (thread_seal.js), so a photographed
// QR is ciphertext rather than a stranger publishing as that vehicle.
// `notAfter` still bounds what an opened one is worth — one job, not a
// shift. `mode` is the issuer's §11 decision, not the driver's: the
// party that decides who gets the link is the party that should decide
// what the link is worth.
//
// Marketplace-forward, without a marketplace in v1: `issueJobOffer`
// mints a **separate artifact** (PMJ1, §20.4) that a dispatcher can
// broadcast to couriers it has never met. It is deliberately *not* a
// seedless ticket. A seedless ticket is the whole plan minus one field —
// every stop coordinate, every label, every order reference and every
// customer phone number — and broadcasting that to a pool of bidders
// hands the day's customer book to everyone who does not win. A PMJ1
// carries a **commitment** (`planRef`, `jobId`) plus coarse descriptors:
// how many stops, how far, what travel mode, roughly where, and nothing
// that names a door.
//
// The asymmetry is deliberate and runs the other way from §20.9. A
// ticket goes to *one* device and is therefore always sealed; an offer
// goes to strangers and is therefore never sealed. An offer is safe to
// post in a group chat because there is nothing in it worth stealing,
// not because it is encrypted.
//
// `jobId` is the join key both artifacts share, and `awardMatchesOffer`
// is what makes the commitment worth having: when the awarded ticket
// finally arrives, the courier's device checks that the plan it was
// handed is the plan that was advertised, so a dispatcher cannot offer
// "3 stops downtown" and award thirty across the country. Claims,
// bidding and payment are **not** in protocol v1 (§20).

import { pushVarint, readVarint } from "../binary.js";
import { sha256, toHex } from "./sha256.js";
import { utf8Bytes } from "./codec.js";
import {
  DAY_CERT_BYTES,
  DAY_CERT_VERSION,
  LINK_BYTES,
  THREAD_MAX_BOOTSTRAP_ADDRESSES,
  THREAD_MAX_BOOTSTRAP_BYTES,
  THREAD_MODE,
  THREAD_TRAVEL_MODE,
  decodeDayCertificate,
  decodeThreadLink,
  encodeThreadLink,
  normalizeBootstrapAddresses,
  threadLinkUrl
} from "./thread_codec.js";
import { deriveDaySeed, routeFollowLink, verifyDayCertificate } from "./thread_route.js";
import {
  base64UrlToBytes,
  bytesToBase64Url,
  generateThreadKeypair,
  publicKeyFromSeed,
  signThread,
  uint32be,
  verifyThread
} from "./thread_crypto.js";
import {
  SEAL_MAGIC,
  decodeSealedTicketHeader,
  sealTicket,
  sealedTicketUrl
} from "./thread_seal.js";
import { SEED_MAGIC, decodeSeedCard } from "./thread_seed.js";

// `PMJ1` is the **j**ob offer (§20.4). `O` for "offer" was the obvious
// letter and is the one to avoid: `PMO1` and `PM01` differ by a single
// glyph in most terminal and QR-debug fonts, and these four characters
// appear verbatim in the error messages a human reads back over a phone
// ("expected PMJ1, found…"). `J` is unused, unambiguous, and reads as
// the job the offer advertises — the same job `jobId` names.
export const TICKET_MAGIC = Object.freeze({ PMP1: "PMP1", PMK1: "PMK1", PMJ1: "PMJ1" });
export const TICKET_VERSION = 1;
export const PLAN_VERSION = 1;
export const JOB_OFFER_VERSION = 1;
/** flags bit0: the run seed is present (a ticket) rather than absent (an offer). */
export const TICKET_FLAG_SEED = 0x01;
/** flags bit1: the issuer named bootstrap addresses for reaching the mesh (§20.10). */
export const TICKET_FLAG_BOOTSTRAP = 0x02;
/**
 * flags bit2: this ticket hands over one **service day** of a recurring
 * route (§21.11), and the field it adds is the PMTC day certificate. The
 * seed in field 9 is then the *day* seed rather than a run seed.
 *
 * The certificate is what makes reusing PMK1 sufficient rather than
 * merely convenient. It already carries `rootPublicKey` (the route's
 * permanent identity), `dayPublicKey` (what the seed must match),
 * `serviceDay` and the validity window — so one sealed artifact gives the
 * driver identity, authority, and the proof to publish alongside its
 * records, with no second file to lose.
 */
export const TICKET_FLAG_DAY = 0x04;
const TICKET_FLAG_MASK = TICKET_FLAG_SEED | TICKET_FLAG_BOOTSTRAP | TICKET_FLAG_DAY;

/**
 * Why a route-day ticket was refused. Stable strings, alongside the
 * `CERT_ERROR` codes `verifyDayCertificate` returns — a host shows a
 * different sentence for "the depot sent the wrong day" than for "this
 * certificate expired last night".
 */
export const TICKET_ERROR = Object.freeze({
  DAY_SEED_MISMATCH: "ticket-day-seed-mismatch",
  DAY_FOREIGN_ROOT: "ticket-day-foreign-root",
  DAY_AFTER_TICKET_EXPIRY: "cert-after-link-expiry",
  DAY_OUTLIVES_TICKET: "cert-outlives-link"
});
export const THREAD_MAX_LABEL_BYTES = 48;
/** §20.8 per-stop delivery metadata. Driver-side only — never on the wire. */
export const THREAD_MAX_ORDER_REF_BYTES = 24;
export const THREAD_MAX_INSTRUCTIONS_BYTES = 64;
export const THREAD_MAX_CONTACT_BYTES = 24;
/** Which optional per-stop fields a stop's flags byte says are present. */
export const THREAD_STOP_FIELD = Object.freeze({
  ORDER_REF: 0x01,
  PARCELS: 0x02,
  INSTRUCTIONS: 0x04,
  CONTACT: 0x08
});
const THREAD_STOP_FIELD_MASK = 0x0f;

const PLAN_DOMAIN = "pulsemesh/plan/1";
const TICKET_DOMAIN = "pulsemesh/ticket/1";
const OFFER_DOMAIN = "pulsemesh/offer/1";
const JOB_DOMAIN = "pulsemesh/job/1";
const JOB_ID_BYTES = 16;

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
  if (state.pos + length > bytes.length) throw new Error("Ticket record truncated.");
  const out = bytes.subarray(state.pos, state.pos + length);
  state.pos += length;
  return out;
}

function readU8(bytes, state) {
  if (state.pos >= bytes.length) throw new Error("Ticket record truncated.");
  return bytes[state.pos++];
}

function assertNoTrailing(bytes, state, what) {
  if (state.pos !== bytes.length) throw new Error(`${what} has trailing bytes.`);
}

function concat(...parts) {
  let length = 0;
  for (const part of parts) length += part.length;
  const out = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function sameBytes(a, b) {
  if (!a || !b || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

// Zigzag over the existing unsigned varint, by arithmetic rather than by
// shifts: lonE7 reaches 1.8e9, which `<< 1` would wrap.
function pushZigzag(out, value) {
  const n = Math.trunc(value);
  pushVarint(out, n < 0 ? (-n * 2) - 1 : n * 2);
}

function readZigzag(bytes, state) {
  const raw = readVarint(bytes, state);
  return raw % 2 === 0 ? raw / 2 : -((raw + 1) / 2);
}

// --- §20.1 PMP1 run plan ---------------------------------------------------
//
// A plan is **driver-side only**. It travels inside the ticket, from the
// dispatcher to the one device that was awarded the job, and it stops
// there: `thread_publish.js` reads the stops for dwell detection and to
// size the outcome map, and the PMTP body it seals carries an 8-byte
// `planRef` hash of the plan bytes — never a coordinate, never a label,
// and now never an order reference, a parcel count, an instruction or a
// phone number. So per-stop metadata (§20.8) reaches the driver and
// cannot reach a follower or a relay through gossip, by construction
// rather than by policy.
//
// What that does *not* protect is the ticket itself. Adding metadata
// does not widen what a ticket can do — it is already a publish
// capability — but it raises what a leaked or photographed one is
// **worth**: order references and parcel counts are commercially
// sensitive, and `contact` is a customer's personal data. See §20.8.

/**
 * Coordinates go on the wire as E7 integers, never floats: two hosts
 * must agree on the plan bytes to the bit, because `planRef` and `jobId`
 * are hashes of them. A caller that already holds E7 (everything inside
 * the engine does) passes it through untouched.
 */
function coordE7(e7, degrees) {
  if (Number.isFinite(e7)) return Math.trunc(e7);
  if (!Number.isFinite(degrees)) throw new Error("A plan stop needs lat/lon in degrees or latE7/lonE7.");
  return Math.round(degrees * 1e7);
}

/**
 * A stop's optional text field, capped and refused rather than truncated:
 * a silently shortened phone number is a phone number that does not
 * ring, and a shortened order reference matches the wrong parcel.
 */
function optionalTextBytes(value, max, what) {
  if (value == null) return null;
  const bytes = typeof value === "string" ? utf8Bytes(value) : value;
  if (!bytes.length) return null;
  if (bytes.length > max) {
    throw new Error(`A stop ${what} is at most ${max} bytes; this one is ${bytes.length}.`);
  }
  return bytes;
}

function readOptionalText(bytes, state, decoder, max, what) {
  const length = readVarint(bytes, state);
  // A set bit with nothing behind it is not a shorter way of saying
  // absent: it is a second encoding of the same plan, and `planRef`
  // hashes plan bytes, so two encodings of one plan is two jobs.
  if (!length) throw new Error(`A stop ${what} flag is set with no ${what} behind it.`);
  if (length > max) throw new Error(`A stop ${what} is at most ${max} bytes; this one is ${length}.`);
  return decoder.decode(readBytes(bytes, state, length));
}

/**
 * The per-stop metadata flags byte (§20.8).
 *
 * A flags byte rather than four always-present zero-length fields, for
 * two reasons. It costs **1 byte instead of 4** on the ordinary
 * address-only stop, which is most stops on most days and the case a
 * 100-drop plan's size is decided by. And it distinguishes **"0 parcels,
 * stated"** from **"parcel count unspecified"**: a dispatcher who wrote
 * 0 has told the driver something (nothing to carry — a collection, a
 * survey, a failed-delivery retry), and a dispatcher who left the field
 * empty has told them nothing. A zero-length varint cannot hold that
 * difference, and a UI that renders "unspecified" as "0 parcels" invents
 * a claim the dispatcher never made. So `parcels` decodes to
 * `number | null` and the three text fields to `string | null`, with
 * null meaning unstated throughout — one rule for all four.
 */
function pushStopMetadata(out, stop) {
  const orderRef = optionalTextBytes(stop.orderRef, THREAD_MAX_ORDER_REF_BYTES, "order reference");
  const instructions = optionalTextBytes(stop.instructions, THREAD_MAX_INSTRUCTIONS_BYTES, "instruction");
  const contact = optionalTextBytes(stop.contact, THREAD_MAX_CONTACT_BYTES, "contact");
  // `Number.isFinite`, never truthiness: `parcels: 0` is the whole
  // reason this field is flagged rather than always present.
  const hasParcels = stop.parcels != null && stop.parcels !== "";
  let parcels = 0;
  if (hasParcels) {
    parcels = Number(stop.parcels);
    if (!Number.isInteger(parcels) || parcels < 0 || parcels > 0xffff) {
      throw new Error(`A stop parcel count is a whole number from 0 to 65535; this one is ${stop.parcels}.`);
    }
  }
  let flags = 0;
  if (orderRef) flags |= THREAD_STOP_FIELD.ORDER_REF;
  if (hasParcels) flags |= THREAD_STOP_FIELD.PARCELS;
  if (instructions) flags |= THREAD_STOP_FIELD.INSTRUCTIONS;
  if (contact) flags |= THREAD_STOP_FIELD.CONTACT;
  out.push(flags);
  // Ascending bit order, so a plan has exactly one encoding.
  if (orderRef) {
    pushVarint(out, orderRef.length);
    pushBytes(out, orderRef);
  }
  if (hasParcels) pushVarint(out, parcels);
  if (instructions) {
    pushVarint(out, instructions.length);
    pushBytes(out, instructions);
  }
  if (contact) {
    pushVarint(out, contact.length);
    pushBytes(out, contact);
  }
}

function readStopMetadata(bytes, state, decoder) {
  const flags = readU8(bytes, state);
  // Reserved bits are refused, exactly as PMK1's are (§20.3): a stop
  // whose writer meant a field this reader cannot see is an undecodable
  // stop, and skipping it silently would hand a driver a door with the
  // instruction that mattered removed.
  if (flags & ~THREAD_STOP_FIELD_MASK) {
    throw new Error("PMP1 reserved stop flag bits must be zero.");
  }
  const orderRef = (flags & THREAD_STOP_FIELD.ORDER_REF)
    ? readOptionalText(bytes, state, decoder, THREAD_MAX_ORDER_REF_BYTES, "order reference")
    : null;
  const parcels = (flags & THREAD_STOP_FIELD.PARCELS) ? readVarint(bytes, state) : null;
  const instructions = (flags & THREAD_STOP_FIELD.INSTRUCTIONS)
    ? readOptionalText(bytes, state, decoder, THREAD_MAX_INSTRUCTIONS_BYTES, "instruction")
    : null;
  const contact = (flags & THREAD_STOP_FIELD.CONTACT)
    ? readOptionalText(bytes, state, decoder, THREAD_MAX_CONTACT_BYTES, "contact")
    : null;
  return { orderRef, parcels, instructions, contact };
}

export function encodeThreadPlan(plan) {
  const stops = plan?.stops || [];
  const out = [];
  pushMagic(out, TICKET_MAGIC.PMP1);
  out.push(PLAN_VERSION);
  // How the job is to be made. It belongs to the plan rather than to the
  // driver's app: a dispatcher who routed a bike round and priced it as
  // one has decided this, and the follower's ETA is only honest if it
  // knows which profile the run is actually on.
  const travelMode = plan?.travelMode ?? THREAD_TRAVEL_MODE.UNSPECIFIED;
  if (!Number.isInteger(travelMode) || travelMode < 0 || travelMode > THREAD_TRAVEL_MODE.FOOT) {
    throw new Error(`Unknown plan travelMode ${plan?.travelMode}.`);
  }
  out.push(travelMode);
  pushVarint(out, Math.max(0, Math.round(plan?.dwellSeconds || 0)));
  pushVarint(out, stops.length);
  for (const stop of stops) {
    pushZigzag(out, coordE7(stop.latE7, stop.lat));
    pushZigzag(out, coordE7(stop.lonE7, stop.lon));
    pushVarint(out, Math.max(0, Math.floor(stop.plannedUnixSeconds || 0)));
    const label = utf8Bytes(stop.label || "");
    if (label.length > THREAD_MAX_LABEL_BYTES) {
      throw new Error(`A stop label is at most ${THREAD_MAX_LABEL_BYTES} bytes.`);
    }
    pushVarint(out, label.length);
    pushBytes(out, label);
    pushStopMetadata(out, stop);
  }
  return Uint8Array.from(out);
}

export function decodeThreadPlan(bytes) {
  const state = { pos: 0 };
  expectMagic(bytes, state, TICKET_MAGIC.PMP1);
  const version = readU8(bytes, state);
  if (version !== PLAN_VERSION) throw new Error(`Unsupported PMP1 plan version ${version}.`);
  const travelMode = readU8(bytes, state);
  if (travelMode > THREAD_TRAVEL_MODE.FOOT) throw new Error(`Unknown plan travelMode ${travelMode}.`);
  const dwellSeconds = readVarint(bytes, state);
  const stopCount = readVarint(bytes, state);
  const decoder = new TextDecoder();
  const stops = [];
  for (let i = 0; i < stopCount; i++) {
    const latE7 = readZigzag(bytes, state);
    const lonE7 = readZigzag(bytes, state);
    const plannedUnixSeconds = readVarint(bytes, state);
    const labelLen = readVarint(bytes, state);
    const label = decoder.decode(readBytes(bytes, state, labelLen));
    const metadata = readStopMetadata(bytes, state, decoder);
    stops.push({
      // Visit order is the encoding order, and `index` is 1-based to
      // match `plan.stops[].index` in thread_publish.js and thread_eta.js.
      index: i + 1,
      lat: latE7 / 1e7,
      lon: lonE7 / 1e7,
      latE7,
      lonE7,
      plannedUnixSeconds: plannedUnixSeconds || null,
      label,
      // §20.8. Null throughout means *unstated* — never 0, never "".
      // This is driver-side data: it is in the plan, and the plan is not
      // on the wire (only its 8-byte `planRef` is), so none of it can
      // reach a follower.
      ...metadata
    });
  }
  assertNoTrailing(bytes, state, "PMP1 plan");
  return { version, travelMode, stops, dwellSeconds, planRef: planRefOf(bytes), bytes };
}

/**
 * The 8-byte `planRef` every PMT1 body carries (§5.2). It is the binding
 * between a plan a follower was given out of band and the run publishing
 * against it: a courier who swaps the plan changes the ref, and the
 * follower sees a run for a different job rather than a silent
 * substitution.
 */
export function planRefOf(planBytes) {
  return sha256(concat(utf8Bytes(PLAN_DOMAIN), planBytes)).subarray(0, 8);
}

// --- §20.2 PMK1 ticket -----------------------------------------------------

function ticketPreimage({
  flags, mode, epochPrefix8, notAfter, issuerPublicKey, planBytes, privateSeed,
  bootstrap = [], dayCertificateBytes = null
}) {
  const out = [];
  pushMagic(out, TICKET_MAGIC.PMK1);
  out.push(TICKET_VERSION);
  out.push(flags & 0xff);
  out.push(mode & 0xff);
  pushBytes(out, epochPrefix8);
  pushBytes(out, uint32be(notAfter));
  pushBytes(out, issuerPublicKey);
  pushVarint(out, planBytes.length);
  pushBytes(out, planBytes);
  if (privateSeed) pushBytes(out, privateSeed);
  // Ascending flag-bit order, as the stop metadata block is (§20.8), so
  // a ticket has exactly one encoding.
  if (bootstrap.length) {
    pushVarint(out, bootstrap.length);
    for (const address of bootstrap) {
      const bytes = utf8Bytes(address);
      pushVarint(out, bytes.length);
      pushBytes(out, bytes);
    }
  }
  // §21.11. Fixed width and no length prefix: a PMTC is exactly
  // DAY_CERT_BYTES and says so in its own magic and version byte, so a
  // length here would be a second statement of a constant — and every
  // byte of it is charged to the QR budget (§20.8).
  //
  // Inside the preimage, for the same reason a bootstrap address is
  // (§20.10) and a sharper one: the certificate is *which day of which
  // route* the issuer granted. A certificate that could be swapped in
  // flight would let anyone who can rewrite an envelope point a driver at
  // another route's authority, or extend yesterday's into tomorrow.
  if (dayCertificateBytes) pushBytes(out, dayCertificateBytes);
  return Uint8Array.from(out);
}

/**
 * Signed over everything from the magic through the bootstrap block, so
 * flags, mode, epoch, expiry, plan, seed and addresses are all
 * immutable: an offer and the ticket that fulfils it carry different
 * signatures and the same job.
 *
 * The addresses are inside the preimage deliberately. A bootstrap
 * address is a *claim the dispatcher is making* — dial this machine —
 * and a claim that can be edited in flight is a claim worth nothing: an
 * unsigned hint on a sealed ticket would let anyone who could rewrite
 * the envelope point a fleet's phones at a peer of their choosing. It
 * costs nothing to sign it, and it is the only field here whose whole
 * purpose is to make a device connect somewhere.
 */
function ticketSigningMessage(preimage) {
  return concat(utf8Bytes(TICKET_DOMAIN), preimage);
}

/** A ticket as bytes, a base64url string, or already decoded. */
function toTicket(value) {
  if (!value) throw new Error("A dispatch ticket is required.");
  if (value instanceof Uint8Array || typeof value === "string") return decodeThreadTicket(value);
  if (value.signature && value.issuerPublicKey && value.preimage) return value;
  throw new Error("Not a dispatch ticket.");
}

function normalizeEpochPrefix8(epochPrefix8, epoch32) {
  const source = epochPrefix8 || epoch32;
  if (!source) throw new Error("A dispatch ticket is bound to a graph epoch.");
  const prefix = source.subarray(0, 8);
  if (prefix.length !== 8) throw new Error("epochPrefix8 must be 8 bytes.");
  return prefix;
}

/**
 * The day seed a route-day ticket must carry, checked against the one
 * §21.2 says it is — and re-derived when the depot did not pass one.
 *
 * This is the mint-time half of the `planRef` reconciliation. §21.2
 * derives a day seed from `(rootSeed, planRef, serviceDay)`, but the
 * certificate carries no `planRef`, so **nothing downstream can check
 * that the plan in the ticket is the plan the day key was scoped to** —
 * a driver's device holds neither the root nor an inverse of HKDF. The
 * one party that *can* check is the issuer, and by the issuer-equals-root
 * rule the issuer always holds the root seed. So the check happens here,
 * once, where it is possible: the dispatched plan's `planRef` must be the
 * `planRef` the day seed was derived under, or the depot minted a
 * certificate for one round and dispatched another.
 *
 * The consequence is worth stating: a route-day ticket must carry the
 * **term's plan byte for byte**. A depot that wants to hand one driver a
 * subset of the stops has changed `planRef`, which is a different day key
 * and needs its own certificate.
 */
async function dayTicketSeed({ cert, issuerSeed, issuerPublicKey, planBytes, privateSeed }) {
  if (cert.version !== DAY_CERT_VERSION) {
    throw new Error(`Unsupported PMTC day certificate version ${cert.version}.`);
  }
  // Only the route's owner grants a day of it. This is not a tightening
  // for its own sake: minting the certificate and deriving the day seed
  // both need the root seed, so any party able to issue this ticket
  // already holds the root — signing as anyone else would only let a
  // holder of one day's authority re-dispatch it under a name of its own
  // choosing, which is exactly the accountability the ticket signature
  // exists to provide.
  if (!sameBytes(cert.rootPublicKey, issuerPublicKey)) {
    throw new Error(
      "A route-day ticket is issued by the route root itself: this certificate names a different root."
    );
  }
  const expected = await deriveDaySeed({
    rootSeed: issuerSeed, planRef: planRefOf(planBytes), serviceDay: cert.serviceDay
  });
  const seed = privateSeed || expected;
  if (!sameBytes(seed, expected)) {
    throw new Error(
      "This day seed is not the one this plan and service day derive (§21.2): the certificate was "
      + "minted for a different planRef."
    );
  }
  // The authoritative form of the same check, and the one that catches a
  // depot that minted for another `planRef` without passing a seed at
  // all: the seed this ticket will carry has to be the key the
  // certificate vouches for, or the driver holds a day they cannot sign.
  if (!sameBytes(await publicKeyFromSeed(seed), cert.dayPublicKey)) {
    throw new Error(
      "This certificate does not vouch for the day seed this plan and service day derive (§21.2): "
      + "it was minted for a different planRef."
    );
  }
  return seed;
}

/**
 * Issues a ticket, or — with `seedless` — a runless PMK1.
 *
 * **The bytes this returns MUST NOT leave the process unsealed.** They
 * are the inner signed artifact and nothing else: a plaintext PMK1
 * carries the run's private seed (so it is a publish capability, and
 * §20.5 states the protocol cannot tell two seed-holders apart) and,
 * since §20.8, the customers' phone numbers. A host that shows one in a
 * QR, writes one to a `.wayfindjob` file, or puts one on any channel has
 * published both. `issueSealedTicket` is the entry point hosts use; this
 * one exists to make the artifact that gets sealed, and is public only
 * so that tests and internal callers can reach it (§20.9).
 *
 * `privateSeed` is the *run's* seed and is generated fresh here when
 * absent; `issuerSeed` is the dispatcher's long-lived identity and is
 * only ever used to sign. `seedless` mints no run seed at all and comes
 * back with `link: null`; it is **not** an offer — an offer is a PMJ1
 * (§20.4) and carries no plan — and the publisher refuses it, which is
 * the only reason the codec still expresses it.
 *
 * `bootstrap` is the fleet's own seed (§20.10): up to three multiaddrs
 * the awarded device may dial to reach the mesh at all. It rides inside
 * the ticket, which is sealed, so it reaches that device and nobody
 * else — **and it deliberately does not go in `url`**, the customer's
 * public follow link. Putting it there is a decision the dispatcher
 * makes per link with `threadLinkUrl(baseUrl, link, { bootstrap })`, not
 * a side effect of issuing a job.
 *
 * `dayCertificate` makes this a **route-day ticket** (§21.11): one
 * service day of a recurring route, handed to whoever is driving it
 * today. `issuerSeed` is then the route **root** seed — it has to be,
 * because only the root can derive the day seed and sign the certificate
 * in the first place — and `privateSeed` is the *day* seed, re-derived
 * here when the caller does not pass one. Everything a driver needs
 * travels in the one sealed artifact, and `link` comes back as the
 * **term's** v2 link over the root, never a link over the day key.
 */
export async function issueTicket({
  issuerSeed,
  epoch32 = null,
  epochPrefix8 = null,
  plan = null,
  planBytes = null,
  notAfter,
  mode = THREAD_MODE.FINE,
  privateSeed = null,
  seedless = false,
  bootstrap = null,
  dayCertificate = null,
  baseUrl = null
} = {}) {
  if (!issuerSeed || issuerSeed.length !== 32) throw new Error("An issuer seed is 32 bytes.");
  if (!Number.isInteger(notAfter) || notAfter < 0 || notAfter > 0xffffffff) {
    throw new Error("notAfter is an absolute expiry in unix seconds (uint32).");
  }
  if (mode !== THREAD_MODE.COARSE && mode !== THREAD_MODE.FINE) {
    throw new Error("A ticket's mode is THREAD_MODE.COARSE or THREAD_MODE.FINE.");
  }
  if (seedless && privateSeed) throw new Error("A seedless ticket carries no run seed.");
  if (seedless && dayCertificate) {
    throw new Error("A route-day ticket is the day seed plus its certificate; a seedless one publishes nothing.");
  }
  const prefix = normalizeEpochPrefix8(epochPrefix8, epoch32);
  const encodedPlan = planBytes || encodeThreadPlan(plan);
  if (!encodedPlan?.length) throw new Error("A dispatch ticket needs a run plan.");
  const issuerPublicKey = await publicKeyFromSeed(issuerSeed);
  const cert = dayCertificate
    ? (dayCertificate instanceof Uint8Array ? decodeDayCertificate(dayCertificate) : dayCertificate)
    : null;
  const runSeed = cert
    ? await dayTicketSeed({ cert, issuerSeed, issuerPublicKey, planBytes: encodedPlan, privateSeed })
    : (seedless ? null : (privateSeed || (await generateThreadKeypair()).privateSeed));
  const addresses = normalizeBootstrapAddresses(bootstrap, { what: "ticket bootstrap address" });
  const preimage = ticketPreimage({
    flags: (seedless ? 0 : TICKET_FLAG_SEED)
      | (addresses.length ? TICKET_FLAG_BOOTSTRAP : 0)
      | (cert ? TICKET_FLAG_DAY : 0),
    mode,
    epochPrefix8: prefix,
    notAfter,
    issuerPublicKey,
    planBytes: encodedPlan,
    privateSeed: runSeed,
    bootstrap: addresses,
    dayCertificateBytes: cert?.bytes || null
  });
  const signature = await signThread(ticketSigningMessage(preimage), issuerSeed);
  const bytes = concat(preimage, signature);
  const ticket = decodeThreadTicket(bytes);
  const link = seedless ? null : await ticketFollowLink(ticket);
  return {
    bytes,
    base64url: bytesToBase64Url(bytes),
    ticket,
    link,
    url: link && baseUrl ? threadLinkUrl(baseUrl, link) : null,
    jobIdHex: jobIdHexOf(ticket)
  };
}

/**
 * Mints a ticket and seals it to enrolled devices in one step — the only
 * ticket-minting entry point a host should call (§20.9).
 *
 * `recipients` are X25519 device public keys, or decoded PMV1 device
 * cards, or anything with a `publicKey`: whatever a dispatcher got when
 * it enrolled the device. **Enrolment is the gate on transfer.** There
 * is no argument that means "seal to whoever ends up holding this",
 * because that is precisely the property §20.9 removes — a job goes to a
 * device the dispatcher has already met, or it does not go.
 *
 * What comes back deliberately has **no plaintext ticket in it**. There
 * is `sealed` (and its base64url and driver URL), and there is `ticket`,
 * the decoded artifact — an in-memory object the dispatcher needs,
 * because it holds the run seed that derives proof-of-delivery photo
 * keys (§20.7) and it can cancel the run. There is no field carrying
 * encodable plaintext bytes, so a host cannot reach one by accident.
 *
 * `link` and `url` are the customer's 45-byte follow link, which is
 * *not* secret in the same way and is not sealed: it is a read
 * capability the customer is meant to have, and §5.4 already bounds what
 * it discloses.
 */
export async function issueSealedTicket({ recipients = [], ...args } = {}) {
  if (args.seedless) {
    // A seedless PMK1 is not an offer and never was: it is the whole
    // plan with one field removed, so broadcasting it publishes every
    // address. Sealing it would contradict the other half — an offer
    // goes to couriers who have not been chosen yet, so there is no
    // device to seal to. Both halves point at the same replacement.
    throw new Error(
      "An offer is broadcast, not sealed, and is not a seedless ticket: use issueJobOffer (PMJ1), "
      + "which advertises the job without publishing a single address."
    );
  }
  const issued = await issueTicket(args);
  const sealed = await sealTicket(issued.bytes, recipients);
  return {
    sealed,
    base64url: bytesToBase64Url(sealed),
    driverUrl: sealedTicketUrl(sealed),
    ticket: issued.ticket,
    link: issued.link,
    url: issued.url,
    jobIdHex: issued.jobIdHex,
    // What the awarded device may dial to reach the mesh (§20.10). It is
    // reported here because the dispatcher already knows it; the point is
    // that the *sealed* bytes are the only place it travels.
    bootstrap: issued.ticket.bootstrap,
    recipientCount: decodeSealedTicketHeader(sealed).recipients.length
  };
}

export function decodeThreadTicket(bytesOrBase64url) {
  const bytes = typeof bytesOrBase64url === "string"
    ? base64UrlToBytes(bytesOrBase64url.trim())
    : bytesOrBase64url;
  const state = { pos: 0 };
  expectMagic(bytes, state, TICKET_MAGIC.PMK1);
  const version = readU8(bytes, state);
  if (version !== TICKET_VERSION) throw new Error(`Unsupported PMK1 ticket version ${version}.`);
  const flags = readU8(bytes, state);
  // Forward compatibility is the version byte's job. A reserved bit set
  // means the writer meant something this reader cannot see, and reading
  // the rest anyway is how a capability gets silently widened.
  if (flags & ~TICKET_FLAG_MASK) throw new Error("PMK1 reserved flag bits must be zero.");
  const mode = readU8(bytes, state);
  if (mode !== THREAD_MODE.COARSE && mode !== THREAD_MODE.FINE) {
    throw new Error(`Unsupported ticket mode ${mode}.`);
  }
  const epochPrefix8 = readBytes(bytes, state, 8);
  const expiry = readBytes(bytes, state, 4);
  const notAfter = (expiry[0] * 2 ** 24) + (expiry[1] << 16) + (expiry[2] << 8) + expiry[3];
  const issuerPublicKey = readBytes(bytes, state, 32);
  const planLen = readVarint(bytes, state);
  const planBytes = readBytes(bytes, state, planLen);
  const seedPresent = (flags & TICKET_FLAG_SEED) !== 0;
  const privateSeed = seedPresent ? readBytes(bytes, state, 32) : null;
  const bootstrap = [];
  if (flags & TICKET_FLAG_BOOTSTRAP) {
    const count = readVarint(bytes, state);
    // A set bit with nothing behind it is a second encoding of one
    // ticket, and the signature is over these bytes.
    if (!count || count > THREAD_MAX_BOOTSTRAP_ADDRESSES) {
      throw new Error(
        `A ticket carries 1 to ${THREAD_MAX_BOOTSTRAP_ADDRESSES} bootstrap addresses; this one carries ${count}.`
      );
    }
    const decoder = new TextDecoder();
    for (let i = 0; i < count; i++) {
      const length = readVarint(bytes, state);
      if (!length) throw new Error("A ticket bootstrap address is empty.");
      if (length > THREAD_MAX_BOOTSTRAP_BYTES) {
        throw new Error(
          `A ticket bootstrap address is at most ${THREAD_MAX_BOOTSTRAP_BYTES} bytes; this one is ${length}.`
        );
      }
      bootstrap.push(decoder.decode(readBytes(bytes, state, length)));
    }
    // The same syntactic gate the encoder applies, so a host that dials
    // what it decoded never dials something the issuer could not have
    // written.
    normalizeBootstrapAddresses(bootstrap, { what: "ticket bootstrap address" });
  }
  // §21.11. Everything a route day needs beyond the seed: the root this
  // run publishes *as*, the day key the seed must be, the service day and
  // the window. Decoded here rather than left as bytes, because the two
  // checks below are what stop a ticket that cannot possibly work from
  // looking like one that can.
  let dayCertificate = null;
  if (flags & TICKET_FLAG_DAY) {
    if (!seedPresent) {
      throw new Error("A route-day ticket carries the day seed its certificate vouches for; this one has none.");
    }
    dayCertificate = decodeDayCertificate(readBytes(bytes, state, DAY_CERT_BYTES));
    if (dayCertificate.version !== DAY_CERT_VERSION) {
      throw new Error(`Unsupported PMTC day certificate version ${dayCertificate.version}.`);
    }
    // Only the route's owner grants a day of it (§21.11). A byte
    // comparison, so it belongs here rather than in the async verify: a
    // ticket whose issuer is not the certificate's root is not a
    // well-formed route-day ticket at all, and refusing it by name beats
    // handing a host something it will discover is unusable later.
    if (!sameBytes(dayCertificate.rootPublicKey, issuerPublicKey)) {
      throw new Error(
        "A route-day ticket is issued by the route root itself: this one's issuer is not the "
        + "certificate's root."
      );
    }
  }
  const preimageEnd = state.pos;
  const signature = readBytes(bytes, state, 64);
  assertNoTrailing(bytes, state, "PMK1 ticket");
  return {
    version,
    seedPresent,
    mode,
    epochPrefix8,
    notAfter,
    issuerPublicKey,
    planBytes,
    plan: decodeThreadPlan(planBytes),
    privateSeed,
    // §20.10. Empty when the issuer named none — a device with its own
    // remembered peers needs nothing here, and most do after day one.
    bootstrap,
    // §21.11. Null on an ordinary job. When present, `privateSeed` above
    // is the **day** seed and this is the proof that goes on the wire
    // with the run's records.
    dayCertificate,
    routeDay: dayCertificate !== null,
    serviceDay: dayCertificate?.serviceDay ?? null,
    signature,
    preimage: bytes.subarray(0, preimageEnd),
    bytes
  };
}

/**
 * Signature, expiry, epoch, and — when the host pins one — the issuer.
 * `expectedIssuerHex` is how a driver app refuses a ticket that is
 * perfectly well-formed and came from someone it does not work for.
 *
 * A **route-day** ticket (§21.11) is checked twice over, because the two
 * artifacts it fuses can disagree. The certificate must stand up on its
 * own — signed by the root it names, a sane window, inside
 * `THREAD_MAX_CERT_SECONDS`, live now — and the seed must actually be the
 * one it vouches for. A seed that is not the certificate's `dayPublicKey`
 * is a ticket that cannot sign a single record, and it is refused here,
 * **by name**, rather than discovered at the first `handleFix` with a
 * driver already sitting in the bus.
 */
export async function verifyThreadTicket(ticket, {
  epochPrefix8 = null,
  expectedIssuerHex = null,
  nowMillis = Date.now()
} = {}) {
  let decoded;
  try {
    decoded = toTicket(ticket);
  } catch (error) {
    return { ok: false, reason: error.message };
  }
  const verified = await verifyThread(
    ticketSigningMessage(decoded.preimage),
    decoded.signature,
    decoded.issuerPublicKey
  );
  if (!verified) return { ok: false, reason: "the issuer signature does not verify" };
  if (Math.floor(nowMillis / 1000) > decoded.notAfter) return { ok: false, reason: "the ticket has expired" };
  if (epochPrefix8 && !sameBytes(epochPrefix8.subarray(0, 8), decoded.epochPrefix8)) {
    return { ok: false, reason: "the ticket is bound to a different graph epoch" };
  }
  if (expectedIssuerHex && toHex(decoded.issuerPublicKey) !== String(expectedIssuerHex).toLowerCase()) {
    return { ok: false, reason: "the ticket was issued by a different issuer" };
  }
  if (decoded.dayCertificate) {
    const verdict = await verifyRouteDayTicket(decoded, nowMillis);
    if (!verdict.ok) return verdict;
  }
  return { ok: true };
}

async function verifyRouteDayTicket(decoded, nowMillis) {
  const cert = decoded.dayCertificate;
  // Re-checked here as well as at decode, because `toTicket` accepts an
  // already-decoded object from an internal caller and this rule is not
  // one a caller may hold wrong (§21.11).
  if (!sameBytes(cert.rootPublicKey, decoded.issuerPublicKey)) {
    return {
      ok: false,
      code: TICKET_ERROR.DAY_FOREIGN_ROOT,
      reason: "the day certificate names a different route root than the ticket's issuer"
    };
  }
  const certVerdict = await verifyDayCertificate(cert, {
    rootPublicKey: cert.rootPublicKey, nowSeconds: Math.floor(nowMillis / 1000)
  });
  if (!certVerdict.ok) {
    return { ok: false, code: certVerdict.code, reason: `the day certificate ${certVerdict.reason}` };
  }
  // §21.5 rule 7, with the ticket's own expiry standing in for the link's:
  // a day that starts after the artifact carrying it dies is nobody's day.
  if (cert.notBefore > decoded.notAfter) {
    return {
      ok: false,
      code: TICKET_ERROR.DAY_AFTER_TICKET_EXPIRY,
      reason: "the day certificate begins after the ticket expires"
    };
  }
  // The other half of the same rule, and the one that fails quietly. A
  // ticket that dies before its own day is over is accepted by everything
  // here and works all afternoon: the driver is already publishing, and
  // the seed in hand keeps signing. It fails the next time the artifact is
  // opened — a phone restarted mid-shift, a day resumed after the app was
  // killed — and by then the run has stopped, every parent's link has gone
  // quiet, and nothing anywhere said why. Refuse it at the only moment
  // somebody can still fix it.
  if (cert.notAfter > decoded.notAfter) {
    return {
      ok: false,
      code: TICKET_ERROR.DAY_OUTLIVES_TICKET,
      reason: "the ticket expires before the day it carries is over"
    };
  }
  if (!sameBytes(await publicKeyFromSeed(decoded.privateSeed), cert.dayPublicKey)) {
    return {
      ok: false,
      code: TICKET_ERROR.DAY_SEED_MISMATCH,
      reason: "the seed in this ticket is not the day key its certificate vouches for"
    };
  }
  return { ok: true };
}

/**
 * The 45-byte follow link this ticket's run publishes under.
 *
 * For an ordinary job that is the run key's own link, derivable the
 * moment the ticket exists and before any driver does — the property the
 * whole primitive is for.
 *
 * For a **route-day** ticket it is emphatically *not* the seed's link,
 * and that difference is the entire reason §21.11 exists. Sealing a bare
 * day seed into an ordinary PMK1 makes every downstream derivation —
 * topic tag, `K_content`, these 45 bytes — come off the **day** public
 * key, so the run publishes onto a topic no parent is subscribed to. It
 * looks perfectly healthy at the driver's end and every existing link
 * goes silent. So this reads the identity out of the certificate and
 * returns the route's own v2 link (§21.4): one branch, in the one place
 * every caller already goes through, rather than a rule for each of them
 * to remember.
 */
export async function ticketFollowLink(ticket) {
  const decoded = toTicket(ticket);
  if (!decoded.privateSeed) {
    throw new Error("This ticket carries no run seed, so it has no follow link yet.");
  }
  if (decoded.dayCertificate) {
    return routeFollowLink({
      rootPublicKey: decoded.dayCertificate.rootPublicKey,
      epochPrefix8: decoded.epochPrefix8,
      notAfter: decoded.notAfter
    });
  }
  const publicKey = await publicKeyFromSeed(decoded.privateSeed);
  return encodeThreadLink({
    publicKey,
    epochPrefix8: decoded.epochPrefix8,
    notAfter: decoded.notAfter
  });
}

/**
 * 16 bytes identifying the job rather than the artifact. It hashes
 * neither the seed nor the signature, so an offer and the ticket that
 * fulfils it share one id — which is what a future claim or award record
 * would join on (§20, not in protocol v1).
 */
export function jobIdOf(ticket) {
  const decoded = toTicket(ticket);
  return jobIdFrom(decoded);
}

function jobIdFrom({ epochPrefix8, notAfter, issuerPublicKey, planBytes }) {
  return sha256(concat(
    utf8Bytes(JOB_DOMAIN),
    epochPrefix8,
    uint32be(notAfter),
    issuerPublicKey,
    planBytes
  )).subarray(0, JOB_ID_BYTES);
}

export function jobIdHexOf(ticket) {
  return toHex(jobIdOf(ticket));
}

// --- §20.4 PMJ1 job offer --------------------------------------------------
//
// What a dispatcher broadcasts. Unlike everything else in this file it
// is **public and unsealed by design**: its whole job is to reach
// couriers the dispatcher has never enrolled, so there is nobody to seal
// it to. Safety comes from its contents, which are a commitment and a
// handful of coarse descriptors — never a stop, never a label, never a
// byte of §20.8 metadata, never a seed.
//
// The tension a marketplace actually has: **a bidder cannot decide
// whether to bid on nothing.** "12 parcels somewhere" is not an offer, it
// is a lottery ticket, and a channel that publishes nothing useful gets
// replaced within a week by a WhatsApp group that publishes everything.
// So the offer has to say roughly where — and "roughly" has to be a
// number, not a promise.
//
// The compromise, in three parts:
//
//   - the **centroid** of the stops, not the first stop. The origin of a
//     round is somebody's doorstep (or the depot, which then leaks the
//     first customer by adjacency); the centroid of a round is usually a
//     point with nothing at it.
//   - rounded to a **0.01° grid** — about 1.1 km north-south, and
//     1.1 km × cos(latitude) east-west, so ~0.8 km at 45°. One grid cell
//     holds a few thousand addresses in a city, which is the whole point:
//     the courier learns which part of town, and no arithmetic gets them
//     from there to a door.
//   - a **bucketed** radius rather than an exact one. An exact centroid
//     plus an exact radius is a disc, and two or three offers from the
//     same depot trilaterate it. Five buckets cannot.
//
// `totalMeters` is exact because the size of the work is not a location:
// it is what a courier prices against, and knowing a round is 41 km tells
// you nothing about where the 41 km are.
//
// **An offer carries no bootstrap address** (§20.10), and the ticket
// does. Three reasons, all of them the same asymmetry:
//
//   - an offer is *public* and broadcast to strangers, so an address in
//     one is published to everyone who ever sees it. For a two-van
//     operator whose seed is the machine in the back office, that is a
//     home address with an open port on it — a doxxing vector, not a
//     convenience.
//   - a courier does not need the mesh to evaluate an offer.
//     `describeOffer` is entirely local: verify the signature, read the
//     stop count, the distance, the gridded centroid. Nothing dials.
//   - the ticket is sealed (§20.9), so the same address put there
//     reaches the one device that was actually awarded the job — which
//     is precisely the device that has to join.

/** Which optional fields an offer's flags byte says are present. */
export const OFFER_FIELD = Object.freeze({ PAY: 0x01, LABEL: 0x02 });
const OFFER_FIELD_MASK = 0x03;
export const THREAD_MAX_OFFER_LABEL_BYTES = 48;

/** The locality grid, in E7 units: 0.01° ≈ 1.1 km. */
export const OFFER_GRID_E7 = 100000;

/**
 * How far the round ranges from its centroid, in five buckets. The edges
 * are the ones a courier actually decides on: one block, a
 * neighbourhood, across town, the whole metro, out of town.
 */
export const OFFER_SPREAD = Object.freeze([
  Object.freeze({ bucket: 0, maxMeters: 1000, label: "within about 1 km of that point" }),
  Object.freeze({ bucket: 1, maxMeters: 3000, label: "within about 3 km of that point" }),
  Object.freeze({ bucket: 2, maxMeters: 10000, label: "within about 10 km of that point" }),
  Object.freeze({ bucket: 3, maxMeters: 30000, label: "within about 30 km of that point" }),
  Object.freeze({ bucket: 4, maxMeters: null, label: "more than 30 km from that point" })
]);

const METERS_PER_DEGREE = 111320;

function roughMeters(aLat, aLon, bLat, bLon) {
  // Equirectangular: the answer only has to pick one of five buckets, and
  // the error at these distances is far inside a bucket edge.
  const mid = ((aLat + bLat) / 2) * Math.PI / 180;
  return Math.hypot(
    (bLat - aLat) * METERS_PER_DEGREE,
    (bLon - aLon) * METERS_PER_DEGREE * Math.cos(mid)
  );
}

/**
 * The coarse locality of a plan: a gridded centroid and a spread bucket.
 *
 * The mean is arithmetic and in degrees. A round that crosses the
 * antimeridian would average to the wrong side of the planet — and is
 * not a delivery round, so this does not special-case it; a dispatcher
 * with such a plan gets a nonsense centroid rather than a silently
 * plausible one.
 */
function offerLocality(stops) {
  if (!stops.length) throw new Error("An offer needs a plan with at least one stop.");
  let latSum = 0;
  let lonSum = 0;
  for (const stop of stops) {
    latSum += stop.latE7;
    lonSum += stop.lonE7;
  }
  const latE7 = latSum / stops.length;
  const lonE7 = lonSum / stops.length;
  // The spread is measured from the *exact* centroid, then bucketed. The
  // grid is what the courier is told; the bucket is a fact about the
  // round, and rounding it twice would only make it less honest.
  let radius = 0;
  for (const stop of stops) {
    radius = Math.max(radius, roughMeters(latE7 / 1e7, lonE7 / 1e7, stop.latE7 / 1e7, stop.lonE7 / 1e7));
  }
  let spread = OFFER_SPREAD.length - 1;
  for (const entry of OFFER_SPREAD) {
    if (entry.maxMeters != null && radius <= entry.maxMeters) { spread = entry.bucket; break; }
  }
  return {
    latCenti: Math.round(latE7 / OFFER_GRID_E7),
    lonCenti: Math.round(lonE7 / OFFER_GRID_E7),
    spread
  };
}

function offerCurrencyBytes(currency) {
  const text = String(currency || "").toUpperCase();
  if (!/^[A-Z]{3}$/u.test(text)) {
    throw new Error(`An offer's currency is a 3-letter ISO 4217 code; this one is ${JSON.stringify(currency)}.`);
  }
  return utf8Bytes(text);
}

function jobOfferPreimage(fields) {
  const out = [];
  pushMagic(out, TICKET_MAGIC.PMJ1);
  out.push(JOB_OFFER_VERSION);
  out.push(fields.mode & 0xff);
  out.push(fields.travelMode & 0xff);
  pushBytes(out, fields.epochPrefix8);
  pushBytes(out, uint32be(fields.notAfter));
  pushBytes(out, fields.jobId);
  pushBytes(out, fields.planRef);
  pushVarint(out, fields.stopCount);
  pushZigzag(out, fields.latCenti);
  pushZigzag(out, fields.lonCenti);
  out.push(fields.spread & 0xff);
  pushVarint(out, fields.totalMeters);
  // Flags immediately before the optional block, as §20.8's stop
  // metadata does, so a reader never holds a bit it cannot yet spend.
  out.push(fields.flags & 0xff);
  if (fields.flags & OFFER_FIELD.PAY) {
    pushVarint(out, fields.payMinor);
    pushBytes(out, fields.currencyBytes);
  }
  if (fields.flags & OFFER_FIELD.LABEL) {
    pushVarint(out, fields.labelBytes.length);
    pushBytes(out, fields.labelBytes);
  }
  pushBytes(out, fields.issuerPublicKey);
  return Uint8Array.from(out);
}

function jobOfferSigningMessage(preimage) {
  return concat(utf8Bytes(OFFER_DOMAIN), preimage);
}

/**
 * Mints the public advertisement of a job.
 *
 * Pass `ticket` to advertise a job that already exists (the ordinary
 * case: the dispatcher composed the round, so it has the plan), or the
 * plan and binding fields directly to advertise before any ticket is
 * minted. Either way the plan is read for `stopCount`, `travelMode`,
 * `planRef`, `jobId` and the coarse locality, and **none of its bytes
 * are copied into the offer**.
 *
 * `totalMeters` is the dispatcher's own routed distance and is not in
 * the plan, so it is an argument. `payMinor`/`currency` and `label` are
 * the two optional fields; `label` is a *public* line ("12 parcels, city
 * centre") and the caller owns the promise that it stays public — the
 * codec caps its length and cannot read it.
 */
export async function issueJobOffer({
  issuerSeed,
  ticket = null,
  epoch32 = null,
  epochPrefix8 = null,
  plan = null,
  planBytes = null,
  notAfter = null,
  mode = THREAD_MODE.FINE,
  totalMeters = 0,
  payMinor = null,
  currency = null,
  label = null,
  baseUrl = null
} = {}) {
  if (!issuerSeed || issuerSeed.length !== 32) throw new Error("An issuer seed is 32 bytes.");
  const issuerPublicKey = await publicKeyFromSeed(issuerSeed);
  let prefix = null;
  let encodedPlan = planBytes;
  let expiry = notAfter;
  let offerMode = mode;
  if (ticket) {
    const decoded = toTicket(ticket);
    if (!sameBytes(issuerPublicKey, decoded.issuerPublicKey)) {
      // A different issuer hashes to a different jobId, which silently
      // breaks the one join the offer exists to make.
      throw new Error("An offer must be signed by the ticket's own issuer.");
    }
    prefix = decoded.epochPrefix8;
    encodedPlan = decoded.planBytes;
    expiry = decoded.notAfter;
    offerMode = decoded.mode;
  } else {
    prefix = normalizeEpochPrefix8(epochPrefix8, epoch32);
    encodedPlan = encodedPlan || encodeThreadPlan(plan);
  }
  if (!Number.isInteger(expiry) || expiry < 0 || expiry > 0xffffffff) {
    throw new Error("notAfter is an absolute expiry in unix seconds (uint32).");
  }
  if (offerMode !== THREAD_MODE.COARSE && offerMode !== THREAD_MODE.FINE) {
    throw new Error("An offer's mode is THREAD_MODE.COARSE or THREAD_MODE.FINE.");
  }
  if (!encodedPlan?.length) throw new Error("An offer needs a run plan to describe.");
  const decodedPlan = decodeThreadPlan(encodedPlan);
  const { latCenti, lonCenti, spread } = offerLocality(decodedPlan.stops);
  const meters = Math.max(0, Math.round(Number(totalMeters) || 0));
  if (!Number.isFinite(meters)) throw new Error("An offer's totalMeters is a whole number of metres.");

  let flags = 0;
  let pay = 0;
  let currencyBytes = null;
  if (payMinor != null && payMinor !== "") {
    pay = Number(payMinor);
    if (!Number.isInteger(pay) || pay < 0) {
      throw new Error(`An offer's payMinor is a whole number of minor units; this one is ${payMinor}.`);
    }
    currencyBytes = offerCurrencyBytes(currency);
    flags |= OFFER_FIELD.PAY;
  } else if (currency != null && currency !== "") {
    throw new Error("An offer's currency means nothing without a payMinor.");
  }
  let labelBytes = null;
  if (label != null && label !== "") {
    labelBytes = typeof label === "string" ? utf8Bytes(label) : label;
    if (labelBytes.length > THREAD_MAX_OFFER_LABEL_BYTES) {
      throw new Error(
        `An offer label is at most ${THREAD_MAX_OFFER_LABEL_BYTES} bytes; this one is ${labelBytes.length}.`
      );
    }
    flags |= OFFER_FIELD.LABEL;
  }

  const preimage = jobOfferPreimage({
    mode: offerMode,
    travelMode: decodedPlan.travelMode,
    epochPrefix8: prefix,
    notAfter: expiry,
    jobId: jobIdFrom({ epochPrefix8: prefix, notAfter: expiry, issuerPublicKey, planBytes: encodedPlan }),
    planRef: planRefOf(encodedPlan),
    stopCount: decodedPlan.stops.length,
    latCenti,
    lonCenti,
    spread,
    totalMeters: meters,
    flags,
    payMinor: pay,
    currencyBytes,
    labelBytes,
    issuerPublicKey
  });
  const signature = await signThread(jobOfferSigningMessage(preimage), issuerSeed);
  const bytes = concat(preimage, signature);
  const offer = decodeJobOffer(bytes);
  return {
    bytes,
    base64url: bytesToBase64Url(bytes),
    offer,
    jobIdHex: offer.jobIdHex,
    url: jobOfferUrl(bytes, baseUrl || undefined)
  };
}

/** The offer as a link. Fragment, like every other capability here. */
export function jobOfferUrl(bytes, baseUrl = "wayfind://offer") {
  return `${baseUrl}#${bytesToBase64Url(bytes)}`;
}

export function decodeJobOffer(bytesOrBase64url) {
  const bytes = typeof bytesOrBase64url === "string"
    ? base64UrlToBytes(bytesOrBase64url.trim())
    : bytesOrBase64url;
  const state = { pos: 0 };
  expectMagic(bytes, state, TICKET_MAGIC.PMJ1);
  const version = readU8(bytes, state);
  if (version !== JOB_OFFER_VERSION) throw new Error(`Unsupported PMJ1 offer version ${version}.`);
  const mode = readU8(bytes, state);
  if (mode !== THREAD_MODE.COARSE && mode !== THREAD_MODE.FINE) {
    throw new Error(`Unsupported offer mode ${mode}.`);
  }
  const travelMode = readU8(bytes, state);
  if (travelMode > THREAD_TRAVEL_MODE.FOOT) throw new Error(`Unknown offer travelMode ${travelMode}.`);
  const epochPrefix8 = readBytes(bytes, state, 8);
  const expiry = readBytes(bytes, state, 4);
  const notAfter = (expiry[0] * 2 ** 24) + (expiry[1] << 16) + (expiry[2] << 8) + expiry[3];
  const jobId = readBytes(bytes, state, JOB_ID_BYTES);
  const planRef = readBytes(bytes, state, 8);
  const stopCount = readVarint(bytes, state);
  const latCenti = readZigzag(bytes, state);
  const lonCenti = readZigzag(bytes, state);
  const spread = readU8(bytes, state);
  if (spread >= OFFER_SPREAD.length) throw new Error(`Unknown offer spread bucket ${spread}.`);
  const totalMeters = readVarint(bytes, state);
  const flags = readU8(bytes, state);
  // Reserved bits are refused, as PMK1's and PMP1's are: an offer whose
  // writer meant a field this reader cannot see is an offer whose terms
  // this reader does not actually know, and bidding on it blind is worse
  // than not bidding.
  if (flags & ~OFFER_FIELD_MASK) throw new Error("PMJ1 reserved flag bits must be zero.");
  const payMinor = (flags & OFFER_FIELD.PAY) ? readVarint(bytes, state) : null;
  const currency = (flags & OFFER_FIELD.PAY)
    ? new TextDecoder().decode(readBytes(bytes, state, 3))
    : null;
  let label = null;
  if (flags & OFFER_FIELD.LABEL) {
    const length = readVarint(bytes, state);
    // A set bit with nothing behind it is a second encoding of one
    // offer, and the signature is over these bytes.
    if (!length) throw new Error("A PMJ1 label flag is set with no label behind it.");
    if (length > THREAD_MAX_OFFER_LABEL_BYTES) {
      throw new Error(`An offer label is at most ${THREAD_MAX_OFFER_LABEL_BYTES} bytes; this one is ${length}.`);
    }
    label = new TextDecoder().decode(readBytes(bytes, state, length));
  }
  const issuerPublicKey = readBytes(bytes, state, 32);
  const preimageEnd = state.pos;
  const signature = readBytes(bytes, state, 64);
  assertNoTrailing(bytes, state, "PMJ1 offer");
  return {
    version,
    mode,
    travelMode,
    epochPrefix8,
    notAfter,
    jobId,
    jobIdHex: toHex(jobId),
    planRef,
    stopCount,
    // The gridded centroid, given back both as the grid cell it is and as
    // the degrees that cell's corner sits at. There is no "precise"
    // reading of this to reach for: `latE7` is a multiple of the grid.
    centroid: {
      latCenti,
      lonCenti,
      latE7: latCenti * OFFER_GRID_E7,
      lonE7: lonCenti * OFFER_GRID_E7,
      lat: (latCenti * OFFER_GRID_E7) / 1e7,
      lon: (lonCenti * OFFER_GRID_E7) / 1e7,
      gridDegrees: OFFER_GRID_E7 / 1e7
    },
    spread,
    spreadLabel: OFFER_SPREAD[spread].label,
    spreadMaxMeters: OFFER_SPREAD[spread].maxMeters,
    totalMeters,
    payMinor,
    currency,
    label,
    issuerPublicKey,
    signature,
    preimage: bytes.subarray(0, preimageEnd),
    bytes
  };
}

/** An offer as bytes, a base64url string, or already decoded. */
function toJobOffer(value) {
  if (!value) throw new Error("A job offer is required.");
  if (value instanceof Uint8Array || typeof value === "string") return decodeJobOffer(value);
  if (value.signature && value.issuerPublicKey && value.preimage && value.planRef) return value;
  throw new Error("Not a job offer.");
}

/**
 * Signature, expiry, epoch and — when the host pins one — the issuer.
 * Same shape and same verdict style as `verifyThreadTicket`, because a
 * courier deciding whether to bid is making the same kind of decision as
 * a driver deciding whether to accept.
 */
export async function verifyJobOffer(offer, {
  epochPrefix8 = null,
  expectedIssuerHex = null,
  nowMillis = Date.now()
} = {}) {
  let decoded;
  try {
    decoded = toJobOffer(offer);
  } catch (error) {
    return { ok: false, reason: error.message };
  }
  const verified = await verifyThread(
    jobOfferSigningMessage(decoded.preimage),
    decoded.signature,
    decoded.issuerPublicKey
  );
  if (!verified) return { ok: false, reason: "the issuer signature does not verify" };
  if (Math.floor(nowMillis / 1000) > decoded.notAfter) return { ok: false, reason: "the offer has expired" };
  if (epochPrefix8 && !sameBytes(epochPrefix8.subarray(0, 8), decoded.epochPrefix8)) {
    return { ok: false, reason: "the offer is bound to a different graph epoch" };
  }
  if (expectedIssuerHex && toHex(decoded.issuerPublicKey) !== String(expectedIssuerHex).toLowerCase()) {
    return { ok: false, reason: "the offer was issued by a different issuer" };
  }
  return { ok: true };
}

/**
 * Is the job that was awarded the job that was advertised?
 *
 * This is what the commitment is *for*. Without it an offer is a
 * marketing line: a dispatcher advertises "3 stops downtown, 8 km",
 * every courier in the pool bids on that, and the winner opens a ticket
 * with thirty drops across the province in it — by which point they have
 * agreed, the customers have been told, and the argument is theirs to
 * lose. `planRef` is a hash of the exact plan bytes, so the swap is
 * arithmetic rather than a matter of opinion, and the refusal happens on
 * the courier's device before a wheel turns.
 *
 * Takes the **opened** ticket: the award arrives sealed (§20.9) and
 * opening it is the host's business. Signature checking is likewise not
 * this function's job — run `verifyJobOffer` on the offer and
 * `verifyThreadTicket` on the award first. This answers exactly one
 * question, and answers it about content, so it stays true even against
 * a dispatcher who re-signs its own forgery.
 */
export function awardMatchesOffer(offer, ticket) {
  let decodedOffer;
  let decodedTicket;
  try {
    decodedOffer = toJobOffer(offer);
  } catch (error) {
    return { ok: false, reason: error.message };
  }
  try {
    decodedTicket = toTicket(ticket);
  } catch (error) {
    return { ok: false, reason: error.message };
  }
  if (!sameBytes(decodedOffer.issuerPublicKey, decodedTicket.issuerPublicKey)) {
    return { ok: false, reason: "the award was issued by a different issuer than the offer" };
  }
  if (!sameBytes(decodedOffer.epochPrefix8, decodedTicket.epochPrefix8)) {
    return { ok: false, reason: "the award is bound to a different graph epoch than the offer" };
  }
  // Ordered so the *bait and switch* is what gets named. A swapped plan
  // moves both `planRef` and `jobId`, and "planRef" tells the courier
  // which lie was told; "jobId" would only tell them the ids differ.
  if (!sameBytes(decodedOffer.planRef, planRefOf(decodedTicket.planBytes))) {
    return { ok: false, reason: "the awarded plan is not the plan the offer committed to (planRef)" };
  }
  if (decodedTicket.notAfter > decodedOffer.notAfter) {
    return { ok: false, reason: "the award runs later than the offer advertised (notAfter)" };
  }
  if (!sameBytes(decodedOffer.jobId, jobIdFrom(decodedTicket))) {
    return { ok: false, reason: "the award is a different job from the one offered (jobId)" };
  }
  return { ok: true, reason: null };
}

// --- what an incoming artifact is -----------------------------------------

const UNREADABLE_TICKET =
  "This ticket cannot be read by this version — it may have been issued "
  + "before a protocol change; ask the dispatcher for a fresh one.";
const SEALED_TICKET =
  "This job is sealed for a specific device. If it is not this one, ask the "
  + "dispatcher to enrol this device and send the job again.";
const UNREADABLE_LINK =
  "This follow link cannot be read by this version — it may have been "
  + "issued before a protocol change; ask for a fresh one.";
const UNREADABLE_OFFER =
  "This job offer cannot be read by this version — it may have been "
  + "broadcast before a protocol change; ask the dispatcher to post a fresh one.";
const UNREADABLE_SEED =
  "This seed card cannot be read by this version — ask whoever runs the "
  + "seed to show it again from an up-to-date build.";

/**
 * What an incoming capability *is*, before asking whether it still works.
 *
 * A ticket and a follow link arrive the same way — base64url in a URL
 * fragment, which browsers never transmit — and they are worth wildly
 * different things: one lets you watch a run, the other lets you move
 * the vehicle. Every host therefore has to branch on the artifact.
 *
 * The reason this is one shared function rather than a try/catch ladder
 * in each host: a ladder decides what something is by whether it decodes,
 * so a ticket minted before a wire change falls through the ticket arm
 * and is reported with the *link* decoder's complaint — a driver holding
 * a stale ticket is told "a thread link is 45 bytes", which is true of
 * nothing they are holding. Identity comes from the magic, which is
 * exactly what a magic is for; readability is a separate answer carried
 * beside it.
 *
 * Returns `{ kind: "ticket" | "offer" | "link" | "seed" | null, reason,
 * sealed }`, where `reason` is null when the artifact decodes and a
 * sentence a user can act on when it does not. The four are worth wildly
 * different things — publish the vehicle, bid on a job, watch a run, dial
 * a peer — and no host may guess between them.
 *
 * A sealed ticket (§20.9) is still `kind: "ticket"` — it is one, and a
 * host routing on kind must route it to the driver's job screen, not to
 * "this is not a capability". What it cannot be is *read* here, because
 * reading needs the device key this function does not take. So it comes
 * back with `sealed: true` and the honest sentence: this is a job, it is
 * sealed for a device, and if this is not that device the fix is
 * enrolment. That is a different sentence from "unreadable", and it is
 * the one a host with no key must show.
 *
 * A **route-day** ticket (§21.11) is likewise still `kind: "ticket"`,
 * because it goes to the same screen and is opened by the same key — but
 * it is a different sentence again, and `routeDay` plus `serviceDay` are
 * what let a host say "tomorrow's run of a route" instead of "a job".
 * `routeDay` is `null`, never `false`, whenever the artifact is a ticket
 * this function could not read: a sealed PME1 hides which kind it is by
 * design, and answering `false` there would be an invented claim of
 * exactly the sort §20.8's flags byte exists to avoid.
 */
export function classifyThreadArtifact(fragmentOrBytes) {
  const nothing = { kind: null, reason: null, sealed: false, routeDay: false, serviceDay: null };
  let bytes = null;
  if (fragmentOrBytes instanceof Uint8Array) {
    bytes = fragmentOrBytes;
  } else if (typeof fragmentOrBytes === "string") {
    // A URL, a bare fragment, or a bare capability, with whatever a mail
    // client wrapped into it: the same three forms every host already
    // accepts, normalized here so they cannot drift apart.
    const raw = fragmentOrBytes.trim();
    const hash = raw.lastIndexOf("#");
    const fragment = (hash >= 0 ? raw.slice(hash + 1) : raw).replace(/\s+/gu, "");
    if (!fragment) return { ...nothing };
    try {
      bytes = base64UrlToBytes(fragment);
    } catch {
      return { ...nothing };
    }
  }
  if (!bytes?.length) return { ...nothing };

  if (hasMagic(bytes, SEAL_MAGIC.PME1)) {
    // Which kind of job is inside is ciphertext, and saying so is the
    // point: `routeDay: null` means "not knowable without the key".
    try {
      decodeSealedTicketHeader(bytes);
      return { kind: "ticket", reason: SEALED_TICKET, sealed: true, routeDay: null, serviceDay: null };
    } catch {
      return { kind: "ticket", reason: UNREADABLE_TICKET, sealed: true, routeDay: null, serviceDay: null };
    }
  }
  if (hasMagic(bytes, TICKET_MAGIC.PMK1)) {
    try {
      const ticket = decodeThreadTicket(bytes);
      return {
        kind: "ticket",
        reason: null,
        sealed: false,
        routeDay: ticket.routeDay,
        serviceDay: ticket.serviceDay
      };
    } catch {
      return { kind: "ticket", reason: UNREADABLE_TICKET, sealed: false, routeDay: null, serviceDay: null };
    }
  }
  // An offer is its own kind, not a ticket that happens to be missing
  // things. A host that routed it to the ticket arm would offer a driver
  // an "accept" button for a job it cannot publish and has no plan for;
  // one that ignored it would drop the only artifact on this channel
  // that is *meant* to be posted in public.
  if (hasMagic(bytes, TICKET_MAGIC.PMJ1)) {
    try {
      decodeJobOffer(bytes);
      return { ...nothing, kind: "offer" };
    } catch {
      return { ...nothing, kind: "offer", reason: UNREADABLE_OFFER };
    }
  }
  // A seed card is the one artifact here that is a **location** rather
  // than a capability (§20.10): holding it grants nothing, and a host
  // must branch on it precisely because it does — it goes to "add this
  // peer", never to a job screen or a map.
  if (hasMagic(bytes, SEED_MAGIC.PMH1)) {
    try {
      decodeSeedCard(bytes);
      return { ...nothing, kind: "seed" };
    } catch {
      return { ...nothing, kind: "seed", reason: UNREADABLE_SEED };
    }
  }
  // A link has no magic — it is 45 bytes opening with a version byte, and
  // that fixed width is its identity (§5.4).
  if (bytes.length === LINK_BYTES) {
    try {
      decodeThreadLink(bytes);
      return { ...nothing, kind: "link" };
    } catch {
      return { ...nothing, kind: "link", reason: UNREADABLE_LINK };
    }
  }
  return { ...nothing };
}

function hasMagic(bytes, magic) {
  if (bytes.length < 4) return false;
  for (let i = 0; i < 4; i++) if (bytes[i] !== magic.charCodeAt(i)) return false;
  return true;
}
