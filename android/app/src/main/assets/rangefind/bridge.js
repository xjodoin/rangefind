// Bridge between the Android app and the rangefind browser runtime.
//
// Contract: Kotlin evaluates `__rfCall(id, method, argsJson)`; every call
// answers exactly once through `AndroidBridge.onResult(id, json)` with
// `{ ok: true, payload }` or `{ ok: false, error }`. Binary never crosses
// this boundary — the runtime fetches byte ranges itself over https, and
// only small JSON results come back.

import { createSearch } from "./runtime.browser.js";
import {
  reverseGeocodeOsm,
  searchOsmQuery,
  suggestOsmQuery
} from "./osm.browser.js";
import { openRouteGraphUrl } from "./route.browser.js";
import {
  DEFAULT_CONSTANTS,
  INCIDENT_TYPES,
  createCorridorTraffic,
  createLoopbackNetwork,
  createMobileMesh
} from "./pulsemesh.mobile.js";

const ROUTE_PROBE_TIMEOUT_MS = 6000;
const MANIFEST_CACHE_PREFIX = "rangefind.manifest:";

let searchEngine = null;
let searchUnavailable = null;
let routeEngine = null;
let routeUnavailable = null;

function readCachedManifest(baseUrl) {
  try {
    const raw = localStorage.getItem(MANIFEST_CACHE_PREFIX + baseUrl);
    return raw ? JSON.parse(raw) : null;
  } catch (err) {
    return null;
  }
}

function writeCachedManifest(baseUrl, manifest) {
  if (!manifest) return;
  try {
    localStorage.setItem(MANIFEST_CACHE_PREFIX + baseUrl, JSON.stringify(manifest));
  } catch (err) {
    // Quota, or storage turned off. The cache is an optimisation for the
    // offline case, never a requirement for the online one.
  }
}

/**
 * Opens the search index, falling back to the manifest cached from a previous
 * run when the network is gone.
 *
 * Search reads still need the network, so this does not make search work
 * offline. What it does is keep the failure local: a device holding a
 * downloaded region can navigate with no connectivity at all, and letting an
 * unreachable search host throw out of init would take that away over
 * something routing never needed.
 */
async function openSearch(baseUrl) {
  try {
    searchEngine = await createSearch({ baseUrl });
    writeCachedManifest(baseUrl, searchEngine.manifest);
    return;
  } catch (err) {
    searchUnavailable = String(err?.message || err);
  }
  const manifest = readCachedManifest(baseUrl);
  if (!manifest) return;
  try {
    searchEngine = await createSearch({ baseUrl, manifest });
  } catch (err) {
    // Keep the original failure: it describes why the network is unusable.
  }
}

function post(id, ok, body) {
  const message = ok ? { ok: true, payload: body } : { ok: false, error: String(body?.message || body) };
  try {
    AndroidBridge.onResult(id, JSON.stringify(message));
  } catch (err) {
    // The activity went away mid-flight; nothing to deliver to.
  }
}

function near(anchor) {
  return anchor && Number.isFinite(anchor.lat) && Number.isFinite(anchor.lon)
    ? { near: { lat: anchor.lat, lon: anchor.lon } }
    : {};
}

// OSM documents carry display fields directly on the result. Normalize the
// handful the app renders so the Kotlin models stay small.
function toPlace(result) {
  const locality = [result.city, result.state, result.country].filter(Boolean).join(", ");
  const street = [result.house_number, result.street].filter(Boolean).join(" ");
  return {
    id: String(result.id ?? ""),
    name: result.name || result.title || street || "Unnamed place",
    address: result.address || [street, locality].filter(Boolean).join(", "),
    locality,
    category: result.category || "",
    type: result.type || "",
    lat: result.lat,
    lon: result.lon,
    distanceMeters: Number.isFinite(result.distanceMeters) ? result.distanceMeters : null
  };
}

function placesOf(response) {
  return (response.results || [])
    .filter(item => Number.isFinite(item.lat) && Number.isFinite(item.lon))
    .map(toPlace);
}

function trimRoute(route, live = route?.live) {
  if (!route) return null;
  return {
    seconds: route.seconds,
    // The total under the live metric, when the mesh had anything to say
    // about this corridor. `liveApplied` counts edges re-weighted, not
    // states consumed — one observed segment fans out to every edge
    // sharing the leaf's canonical polyline.
    adjustedSeconds: route.adjustedSeconds ?? null,
    liveApplied: live?.applied ?? 0,
    distanceMeters: route.distanceMeters ?? null,
    bucket: route.bucket || "",
    geometry: route.geometry || [],
    steps: (route.steps || []).map(step => ({
      name: step.name || "",
      meters: step.meters,
      seconds: step.seconds,
      at: step.at ?? 0,
      // Posted limit in km/h; 0 when the way carries no maxspeed tag.
      speedLimitKmh: step.speedLimitKmh ?? 0,
      // Movements allowed from each lane of the approach to this step's
      // junction, left to right. Empty when the way carried no lane tags.
      lanes: Array.isArray(step.lanes) ? step.lanes : [],
      // The kind of road this step runs on, resolved here so the app never
      // needs the index's class table. A ramp is unnamed far more often than
      // not, and its class is the only thing that says what it is.
      roadClass: routeEngine?.root?.classes?.[step.roadClass] || "",
      // What the signs say, which on a motorway is the only thing a driver
      // can act on: the road's number, the exit's number, and what the slip
      // road leads to. Semicolon-separated lists as OSM writes them.
      ref: step.ref || "",
      exitRef: step.exitRef || "",
      destinationRef: step.destinationRef || "",
      destination: step.destination || "",
      // A roundabout is one maneuver, and the exit is the instruction.
      roundabout: Boolean(step.roundabout),
      roundaboutExit: step.roundaboutExit ?? 0
    })),
    // Where the posted limit changes, as a step function over distance
    // travelled. A step is one street and a street routinely carries several
    // limits, so a single number per step shows the wrong one for however much
    // of the street disagrees with it.
    speedLimits: (route.speedLimits || []).map(entry => ({
      atMeters: entry.atMeters,
      limitKmh: entry.limitKmh,
      // A window the limit drops inside, answered against the device clock
      // rather than here: the index is static and cannot know the hour.
      conditional: entry.conditional || null
    })),
    junctions: (route.junctions || []).map(j => ({
      kind: j.kind,
      lat: j.lat,
      lon: j.lon,
      atMeters: j.atMeters
    })),
    from: route.from || null,
    to: route.to || null,
    stats: {
      httpRequests: route.stats?.httpRequests ?? 0,
      bytesFetched: route.stats?.bytesFetched ?? 0
    }
  };
}

// The route graph covers whatever region it was built for, while search and
// the basemap are worldwide. Publishing its extent lets the app say "outside
// the routable area" instead of failing a snap 5 km from a road. A single
// rectangle around the whole graph is too generous — it spans neighbouring
// countries the extract never included. The leaf boxes tile the actual node
// distribution, so shipping them tells "3 km past the border" from "in a
// covered town".
function routingInfo() {
  let routeBounds = null;
  const leaves = routeEngine?.root?.leaves;
  if (leaves?.length) {
    let minLat = Infinity, maxLat = -Infinity, minLon = Infinity, maxLon = -Infinity;
    const cells = [];
    for (const leaf of leaves) {
      const box = leaf.bbox;
      if (!box) continue;
      if (box.minLat < minLat) minLat = box.minLat;
      if (box.maxLat > maxLat) maxLat = box.maxLat;
      if (box.minLon < minLon) minLon = box.minLon;
      if (box.maxLon > maxLon) maxLon = box.maxLon;
      cells.push(box.minLat / 1e7, box.maxLat / 1e7, box.minLon / 1e7, box.maxLon / 1e7);
    }
    if (Number.isFinite(minLat)) {
      routeBounds = {
        minLat: minLat / 1e7,
        maxLat: maxLat / 1e7,
        minLon: minLon / 1e7,
        maxLon: maxLon / 1e7,
        cells
      };
    }
  }
  return {
    routing: Boolean(routeEngine),
    routingError: routeUnavailable,
    profile: routeEngine?.root?.profile || "",
    routeBounds
  };
}

// --- PulseMesh -------------------------------------------------------------
//
// The phone is the contributor the design counts on: it has background
// location and a screen that can be off, which is exactly what a browser
// tab cannot offer. Contribution stays off until the user turns it on —
// publishing where you drive is a decision, not a default.
//
// Three transports, chosen by what the deployment actually has:
//
//   "mesh" — libp2p over WSS to a configured keeper. The real thing. The
//            transport bundle is 2.6 MB and loads on demand, so a phone
//            that never joins pays nothing for it.
//   "demo" — a loopback network with simulated vehicles on the corridor
//            you are actually driving. Every byte still goes through the
//            codec, the twelve rules and the aggregate; only the drivers
//            are invented. This is what makes the feature testable on a
//            device before any peers exist, and the app says so on screen.
//   "off"  — no mesh. The router runs the static metric, which is the
//            degradation contract and not an error state.

let mesh = null;
let meshSim = null;
let meshThreads = null;
let meshNetwork = null;
let meshHost = null;
let meshMode = "off";
let meshError = null;
let meshBootstrap = "";
// Pushed from Kotlin with each fix: §10.1 rule 5 pauses contribution below
// 20% unless charging, because a traffic layer that flattens a phone loses
// the contributor permanently.
let meshBattery = null;
let meshCharging = null;
// The routes the driver is actually on, so the mesh can scope to them.
let meshRoutes = [];
let meshRun = null;
let meshFollow = null;
// The dispatch ticket behind `meshRun`, base64url, when the run came from
// one (threads §20). It is a *publish* capability and never leaves this
// file except as the QR the driver shows to the next driver's camera —
// and since §20.9 what is held here is the **sealed** PME1 form, exactly
// as it arrived, so what gets photographed is ciphertext.
let meshTicket = null;

// --- this device's identity (threads §20.9) --------------------------------
//
// The X25519 key a ticket can be sealed *to*. It is none of the other
// keys in this protocol: not the run seed (per-job, and it travels), not
// the issuer seed (which signs). It never signs anything and its only job
// is to be an address.
//
// It lives here and in the host's own storage, and it crosses the bridge
// exactly once — upward, at the moment it is minted, so the host can
// persist it. Nothing above the storage seam ever sees it again: the
// host hands it back at startup and asks this file to do the opening.
let devicePrivateKey = null;
let devicePublicKey = null;
let deviceName = "";

/** §20.9: a device name is at most 32 bytes, because the card says so. */
const MAX_DEVICE_NAME_BYTES = 32;

function threadsModule() {
  return import("./pulsemesh-threads.browser.js");
}

/**
 * A device name cut to what a PMV1 card can carry.
 *
 * Cut on a code-point boundary, never mid-character: "Léa — Pixel 8"
 * truncated through the middle of an em dash reaches the other phone as a
 * replacement glyph, and the name exists so a human recognises it. The
 * card encoder throws above 32 bytes, so doing this here is what keeps a
 * long name from taking the whole enrolment with it.
 */
function capDeviceName(value) {
  const text = String(value ?? "").trim();
  if (!text) return "";
  const encoder = new TextEncoder();
  if (encoder.encode(text).length <= MAX_DEVICE_NAME_BYTES) return text;
  let kept = "";
  let bytes = 0;
  for (const ch of text) {
    const cost = encoder.encode(ch).length;
    if (bytes + cost > MAX_DEVICE_NAME_BYTES) break;
    kept += ch;
    bytes += cost;
  }
  return kept.trim();
}

/** What the app is allowed to know about this device. Never the private key. */
function deviceIdentity(mod) {
  if (!devicePublicKey) return null;
  return {
    publicKey: mod.bytesToBase64Url(devicePublicKey),
    // The eight hex characters two people read to each other in a doorway.
    // Not a cryptographic binding, and §20.9 does not claim it is one.
    fingerprint: mod.fingerprint(devicePublicKey),
    name: deviceName
  };
}

/**
 * Adopts the host's stored key, or mints one on first run.
 *
 * `privateKey` comes back in the answer **only** when it was just minted:
 * that is the one crossing, and it exists because a device that loses this
 * key cannot open any job already sealed to it (§20.9) — so the host has
 * to be able to keep it.
 */
async function loadDevice({ privateKey, name }) {
  const mod = await threadsModule();
  let stored = null;
  if (privateKey) {
    try {
      const bytes = mod.base64UrlToBytes(String(privateKey).trim());
      if (bytes.length === 32) stored = bytes;
    } catch {
      // A key this build cannot read is a key this device does not have.
      // Minting a fresh one is the honest recovery, and the host is told.
    }
  }
  const minted = !stored;
  devicePrivateKey = stored || (await mod.generateDeviceKeypair()).privateKey;
  devicePublicKey = await mod.x25519PublicKey(devicePrivateKey);
  deviceName = capDeviceName(name);
  return {
    ...deviceIdentity(mod),
    minted,
    privateKey: minted ? mod.bytesToBase64Url(devicePrivateKey) : null
  };
}

/**
 * Another device's card, when that is what these bytes announce.
 *
 * Identity comes from the PMV1 magic rather than from a clean decode, for
 * the same reason it does everywhere else in this file: a card written by
 * a newer build is still a *card*, and its holder is owed "this app
 * cannot read that card" rather than "that is not a capability". Returns
 * null when the bytes are not a card at all.
 */
function readDeviceCardFragment(fragment, mod) {
  let bytes;
  try {
    bytes = mod.base64UrlToBytes(fragment);
  } catch {
    return null;
  }
  const magic = mod.SEAL_MAGIC.PMV1;
  if (bytes.length < 4) return null;
  for (let i = 0; i < 4; i++) if (bytes[i] !== magic.charCodeAt(i)) return null;
  try {
    const card = mod.decodeDeviceCard(bytes);
    return {
      kind: "device",
      reason: null,
      sealed: false,
      unreadable: false,
      device: {
        publicKey: mod.bytesToBase64Url(card.publicKey),
        name: card.name,
        fingerprint: card.fingerprint
      }
    };
  } catch (error) {
    return {
      kind: "device",
      reason: String(error?.message || error),
      sealed: false,
      unreadable: true,
      device: null
    };
  }
}

/**
 * The bytes a ticket artifact actually carries, opened when they are
 * sealed (§20.9).
 *
 * The two failures are kept apart all the way up because they have
 * different actions attached: `sealed-for-another-device` is a perfectly
 * good ticket for somebody else and the fix is enrolment plus a fresh
 * seal, while `unreadable` is not a ticket this build can parse at all
 * and no amount of re-sending helps.
 */
async function openTicketBytes(fragment) {
  const mod = await threadsModule();
  let raw;
  try {
    raw = mod.base64UrlToBytes(fragment);
  } catch {
    return { bytes: null, code: mod.SEAL_ERROR.UNREADABLE, error: "not a capability" };
  }
  // §20.9's rule runs in both directions. Emitting only sealed tickets
  // would still leave this phone willing to take a job whose plan — the
  // addresses, the order refs, the customers' phone numbers — travelled
  // in the clear, and to publish a vehicle on the strength of a seed
  // anyone who saw that QR also has. A plaintext ticket is refused as
  // what it is, not decoded and quietly honoured.
  if (!mod.isSealedTicket(raw)) {
    return { bytes: null, sealed: false, code: "unsealed", error: "this job arrived unsealed" };
  }
  if (!devicePrivateKey) {
    return { bytes: null, sealed: true, code: "no-device-key", error: "this device has no identity yet" };
  }
  try {
    return { bytes: await mod.openSealedTicket(raw, devicePrivateKey), sealed: true };
  } catch (error) {
    return {
      bytes: null,
      sealed: true,
      code: error?.code || mod.SEAL_ERROR.UNREADABLE,
      error: String(error?.message || error)
    };
  }
}

/**
 * How the app's travel mode reaches the wire (threads §5.2).
 *
 * The app names its modes after routing profiles and the wire numbers
 * them, so the mapping lives here rather than as a bare integer crossing
 * the bridge — the two vocabularies are allowed to drift and this is the
 * one place that has to know they have not.
 */
const TRAVEL_MODE_CODES = { car: 1, bike: 2, foot: 3 };

function travelModeCode(name) {
  return TRAVEL_MODE_CODES[String(name || "").toLowerCase()] || 0;
}

/**
 * The other direction, for a plan the app is reading rather than
 * writing. Null for 0 — "unspecified" is not a mode, and naming it one
 * would put a travel-mode line on a job whose dispatcher never stated it.
 */
function travelModeNameOf(code) {
  return Object.keys(TRAVEL_MODE_CODES).find(name => TRAVEL_MODE_CODES[name] === code) || null;
}

/** The fragment of a URL, a bare fragment, or a bare capability string. */
function fragmentOf(text) {
  const raw = String(text || "").trim();
  const hash = raw.lastIndexOf("#");
  return (hash >= 0 ? raw.slice(hash + 1) : raw).trim();
}

function bytesToHex(bytes) {
  let out = "";
  for (const byte of bytes) out += byte.toString(16).padStart(2, "0");
  return out;
}

/**
 * Standard base64 to bytes, which is how a photo crosses the bridge.
 *
 * Base64 rather than a number array because a 60 KB JPEG as JSON integers
 * is a third of a megabyte of text for the WebView to parse, for the same
 * pixels. `atob` throws on anything that is not base64, and the caller
 * turns that into a refusal rather than a crash.
 */
function bytesFromBase64(text) {
  const binary = atob(text);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

/**
 * Which of §20.7's photo refusals a thrown mark was, or null when the
 * mark failed for a reason that has nothing to do with the picture.
 *
 * Matching on the message is the only handle the library offers — it
 * throws `Error`, not codes — so this matches the parts of those
 * sentences that carry the meaning, and falls through to the raw message
 * when nothing matches. Being wrong here costs a less specific notice,
 * never a wrong outcome.
 */
function photoRefusal(message) {
  const text = String(message || "");
  if (/cap is/.test(text) && /seals to/.test(text)) return "photo-too-big";
  if (/coarse/.test(text)) return "photo-coarse";
  if (/photo is empty/.test(text)) return "photo-empty";
  if (/Uint8Array/.test(text)) return "photo-unreadable";
  return null;
}

/**
 * The graph epoch a ticket is bound to (§20.6), as the 8 bytes
 * `verifyThreadTicket` compares against.
 *
 * Read from the open route index rather than from the mesh, so a driver
 * can be shown what a ticket says with live traffic switched off:
 * decoding and verifying are local, and only *taking* the job needs a
 * channel to publish onto.
 */
function meshEpochPrefix8() {
  const hex = mesh?.node?.epochHex || routeEngine?.root?.sourceHash;
  if (!hex) return null;
  const text = String(hex).slice(0, 16);
  const out = new Uint8Array(8);
  for (let i = 0; i < 8; i++) out[i] = Number.parseInt(text.substr(i * 2, 2), 16);
  return out;
}

/**
 * What a ticket says, plus the verdict on whether it can be taken.
 *
 * A driver is being handed a publish capability and told to drive
 * somewhere; being shown where, until when and by whom before pressing
 * accept is the whole difference between an offer and an instruction.
 * The verdict comes from `verifyThreadTicket` rather than from a
 * well-formed decode: a ticket for last week's epoch parses perfectly
 * and cannot be published.
 *
 * The per-stop delivery metadata (§20.8) comes up with the stops, and
 * this is as far as it ever travels: the plan is driver-side, the wire
 * carries only its 8-byte `planRef` hash, and nothing the app publishes
 * — a mark's note, a cancel reason, a shared plan — is built from these
 * fields. `null` is the one form of "the dispatcher did not say", for
 * all four: `""` would be a stated empty reference and `0` a stated
 * parcel count, and both are claims nobody made.
 */
function ticketJob(ticket, verdict, jobIdHex) {
  return {
    jobIdHex,
    stops: (ticket.plan?.stops || []).map(stop => ({
      index: stop.index,
      lat: stop.lat,
      lon: stop.lon,
      label: stop.label || "",
      // `?? null` rather than a bare read: JSON.stringify drops an
      // undefined value, and an absent key on the far side is one more
      // thing for the app to have an opinion about.
      orderRef: stop.orderRef ?? null,
      parcels: stop.parcels ?? null,
      instructions: stop.instructions ?? null,
      contact: stop.contact ?? null
    })),
    mode: ticket.mode,
    // How the run moves (§5.2 field 5), as a profile name. The driver is
    // being handed a day's work and "by bike" is part of what the job is.
    travelModeName: travelModeNameOf(ticket.plan?.travelMode ?? 0),
    notAfter: ticket.notAfter,
    issuerHex: bytesToHex(ticket.issuerPublicKey),
    seedPresent: ticket.seedPresent,
    ok: Boolean(verdict.ok) && ticket.seedPresent,
    reason: verdict.ok
      ? (ticket.seedPresent ? null : "this is an offer, not a publish capability")
      : verdict.reason
  };
}


/**
 * Whether an awarded ticket is any of the jobs this device bid on.
 *
 * `awardMatchesOffer` is the whole point of the commitment: it recomputes
 * `planRef` from the awarded plan's bytes and names the check that
 * failed, so "the plan was swapped" is arithmetic rather than an opinion.
 * It is deliberately run per held offer rather than after a lookup by
 * `jobId` — under a swap the awarded `jobId` is exactly what does not
 * match, so a lookup would find nothing and wave the swap through.
 *
 * Signatures are not this function's business (the ticket's own verdict
 * and `verifyJobOffer` cover those); this answers only the content
 * question, which is what makes it hold against a dispatcher who
 * re-signs its own forgery.
 */
function awardChecks(mod, offers, ticket) {
  if (!Array.isArray(offers)) return [];
  return offers.map((held, index) => {
    const text = fragmentOf(held);
    if (!text) return { index, ok: false, reason: "this bid is empty" };
    try {
      const match = mod.awardMatchesOffer(mod.decodeJobOffer(text), ticket);
      return { index, ok: Boolean(match.ok), reason: match.reason ?? null };
    } catch (error) {
      // A held bid this build cannot read cannot vouch for anything. It
      // stays in the list as a failed check rather than disappearing:
      // dropping it would turn "I bid on something and this is not it"
      // into "I never bid at all".
      return { index, ok: false, reason: String(error?.message || error) };
    }
  });
}

/**
 * What a dispatcher's *offer* says (§20.4), plus the verdict on it.
 *
 * A different artifact from a ticket and not a lesser one: it carries a
 * commitment to a plan and coarse descriptors, and there is no stop in it
 * to report. Nothing here invents one — no coordinate on this object is a
 * doorstep, `centroidLat`/`centroidLon` are a 0.01° grid cell the whole
 * round sits somewhere inside, and `spread` is a bucket rather than a
 * radius.
 *
 * The offer travels back up as `offer` so the host can *hold* it: the
 * bid it records has to be re-checkable against an award that turns up
 * hours later, and `awardMatchesOffer` compares bytes rather than
 * summaries.
 */
function jobOfferSummary(offer, verdict, fragment) {
  return {
    offer: fragment,
    jobIdHex: offer.jobIdHex,
    planRefHex: bytesToHex(offer.planRef),
    stopCount: offer.stopCount,
    travelModeName: travelModeNameOf(offer.travelMode),
    mode: offer.mode,
    // The gridded centroid as degrees. `gridDegrees` travels with it
    // because a number without its resolution reads as a position.
    centroidLat: offer.centroid.lat,
    centroidLon: offer.centroid.lon,
    gridDegrees: offer.centroid.gridDegrees,
    spread: offer.spread,
    // Null in the top bucket, where the honest statement is "further than
    // the last edge" rather than a number.
    spreadMaxMeters: offer.spreadMaxMeters ?? null,
    totalMeters: offer.totalMeters,
    // `0` is a stated pay of nothing and null is silence, so `?? null`
    // rather than a falsy test — the same rule §20.8's fields follow.
    payMinor: offer.payMinor ?? null,
    currency: offer.currency ?? null,
    label: offer.label ?? null,
    notAfter: offer.notAfter,
    issuerHex: bytesToHex(offer.issuerPublicKey),
    ok: Boolean(verdict.ok),
    reason: verdict.ok ? null : verdict.reason
  };
}

/**
 * Anything a phone's camera is meant to read, as a packed module matrix.
 *
 * Version 25 is a ceiling on what a phone can be *shown*, not on what the
 * encoder can draw: past it the modules are finer than a hand-held camera
 * resolves across a 6" screen, so a symbol that encodes perfectly is one
 * no driver can scan. Something that large says so rather than rendering
 * an unreadable tile, and travels as a file instead.
 *
 * The rows pack the symbol MSB-first, one hex digit per four modules,
 * because a JSON array of 30 000 booleans across the bridge for something
 * the screen redraws is absurd.
 */
async function qrOf(text) {
  const { encodeQr } = await import("./qr.browser.js");
  let qr;
  try {
    qr = encodeQr(text, { ecLevel: "M", maxVersion: 25 });
  } catch {
    return { size: 0, rows: [], tooBig: true };
  }
  const rows = [];
  for (let y = 0; y < qr.size; y++) {
    let hex = "";
    let acc = 0;
    let bits = 0;
    for (let x = 0; x < qr.size; x++) {
      acc = (acc << 1) | (qr.modules[y * qr.size + x] ? 1 : 0);
      bits++;
      if (bits === 4) {
        hex += acc.toString(16);
        acc = 0;
        bits = 0;
      }
    }
    // A row is `size` bits wide and size is never a multiple of 4, so the
    // tail nibble is left-aligned like every other one.
    if (bits) hex += (acc << (4 - bits)).toString(16);
    rows.push(hex);
  }
  return { size: qr.size, rows, tooBig: false };
}

function meshFailure(error) {
  meshError = String(error?.message || error);
  // Headless: without this the reason a mesh refused to start exists only
  // inside a WebView nobody is looking at.
  console.warn("[pulsemesh]", meshError);
  return null;
}

async function startMeshTransport(mode) {
  if (mode === "mesh" && meshBootstrap) {
    const { createPulseMeshHost, createLibp2pNetwork } = await import("./pulsemesh-libp2p.browser.js");
    meshHost = await createPulseMeshHost({ profile: "browser", bootstrapPeers: [meshBootstrap] });
    meshNetwork = createLibp2pNetwork(meshHost);
    await meshNetwork.ready;
    return "mesh";
  }
  if (mode === "demo") {
    meshNetwork = createLoopbackNetwork();
    return "demo";
  }
  return "off";
}

/**
 * Brings the mesh up in `mode`. A keeper that is unreachable falls back to
 * the demo transport rather than failing: the point of the fallback is
 * that the map keeps showing something honest, labelled as what it is.
 *
 * `simulated` only bites in demo mode, and it is the user's decision:
 * where the demo vehicles are and how fast they go is the one thing on
 * this device that is invented, so it is refusable — at the price of a
 * traffic layer with nothing in it until real drivers are nearby.
 */
async function startMesh(mode, simulated = true) {
  if (mesh) return meshMode;
  if (!routeEngine) return meshFailure(new Error("no route index"));
  let achieved = "off";
  try {
    achieved = await startMeshTransport(mode);
  } catch (error) {
    meshError = `keeper unreachable (${error?.message || error})`;
    achieved = mode === "mesh" ? await startMeshTransport("demo").catch(() => "off") : "off";
  }
  if (achieved === "off") return meshFailure(new Error(meshError || "no transport configured"));
  try {
    mesh = await createMobileMesh({
      engine: routeEngine,
      network: meshNetwork,
      id: meshHost ? meshHost.peerId.toString() : "wayfind",
      profile: "reticent",
      transport: achieved === "mesh" ? "wire" : "loopback",
      batteryLevel: () => meshBattery,
      charging: () => meshCharging
    });
    // Deliberately NOT mesh.start(): the page owns no usable clock here.
    // A headless WebView parked at 1x1 is a hidden page to Chromium, which
    // clamps its timers — measured on a Pixel emulator, one setInterval
    // fired once and then never again, so anti-entropy stopped and the
    // simulated corridor froze after a single round. Kotlin drives the
    // cadence through the "tick" action instead.
    meshMode = achieved;
    if (meshRoutes.length) await followMeshRoutes(meshRoutes);
    if (achieved === "demo" && simulated) await startMeshSim();
    return meshMode;
  } catch (error) {
    return meshFailure(error);
  }
}

/** The demo corridor's vehicles, on the transport the mesh already has. */
async function startMeshSim() {
  if (!mesh || meshSim || meshMode !== "demo") return;
  meshSim = await createCorridorTraffic({
    engine: routeEngine,
    network: meshNetwork,
    transport: "loopback",
    // The simulated vehicles skip the emission cadence, and only them:
    // EMIT_INTERVAL is a contributor-side gate, so shortening it changes
    // what these three publish and nothing about how any record is
    // judged. At the protocol's real 15 s a demo corridor takes several
    // minutes to reach the two reports an aggregate needs, which is a
    // long time to look at an empty map.
    constants: { ...DEFAULT_CONSTANTS, EMIT_INTERVAL: 0.01 }
  });
  if (meshRoutes.length) await meshSim.follow(meshRoutes);
}

/**
 * Starts or stops the demo vehicles with the mesh left up.
 *
 * Stopping withdraws the source and nothing else: every record already in
 * the store was validated against the static index on arrival, and
 * discarding evidence because the vehicle that produced it has gone is not
 * something the protocol does anywhere else. What ends is the invention,
 * not what was concluded from it — the aggregates age out on TTL like any
 * others.
 */
async function setMeshSimulated(enabled) {
  if (!mesh) return;
  if (enabled) {
    await startMeshSim();
    return;
  }
  meshSim?.stop();
  meshSim = null;
}

async function stopMesh() {
  meshThreads?.close();
  meshSim?.stop();
  if (mesh) await mesh.close().catch(() => {});
  if (meshHost?.stop) await meshHost.stop().catch(() => {});
  mesh = null; meshSim = null; meshThreads = null; meshNetwork = null; meshHost = null;
  meshRun = null; meshFollow = null; meshTicket = null; meshMode = "off";
}

/**
 * One maintenance round, driven by the host: the node's TTL sweep and
 * anti-entropy, the thread channel's tag rotation, and — in demo mode —
 * one more stretch of simulated driving.
 */
async function meshTick() {
  if (!mesh) return;
  await mesh.tick();
  if (meshSim) await meshSim.tick();
  if (meshThreads) await meshThreads.tick();
}

async function followMeshRoutes(routes) {
  meshRoutes = routes;
  if (!mesh) return;
  await mesh.followRoute(routes);
  if (meshSim) await meshSim.follow(routes);
}

/** Everything the app renders about the mesh, in one call. */
function meshStatus() {
  if (!mesh) {
    return {
      available: false,
      mode: meshMode,
      simulated: false,
      error: meshError,
      types: incidentTypeList()
    };
  }
  return {
    available: true,
    mode: meshMode,
    // Whether anything on this map was invented here. The app labels the
    // map from this rather than from `mode`: demo mode with the vehicles
    // switched off draws only what real peers said.
    simulated: Boolean(meshSim),
    error: meshError,
    types: incidentTypeList(),
    thread: meshRun
      ? {
        publishing: true,
        url: meshRun.url,
        seq: meshRun.seq,
        // A dispatched run belongs to the job, not to this device: it
        // carries a job id, and it can be handed on (§20.5). A drive
        // this phone started owns its own seed and can do neither.
        jobIdHex: meshRun.jobIdHex ?? null,
        fromTicket: Boolean(meshTicket),
        // What the run says about itself, cumulatively: one entry per
        // plan stop, and the mode the followers' ETAs are computed for.
        // Only `markStop` ever writes an outcome — the app holds its own
        // copy across a process death, and this is what it reconciles
        // against.
        outcomes: meshRun.outcomes(),
        travelMode: meshRun.travelMode
      }
      : null,
    following: meshFollow ? meshFollow.status() : null,
    ...mesh.snapshot()
  };
}

function incidentTypeList() {
  return Object.entries(INCIDENT_TYPES).map(([type, info]) => ({
    type: Number(type),
    name: info.name,
    // Informational types never change a route at any score (§9); a UI
    // that implied otherwise would be promising something the protocol
    // deliberately refuses to do.
    informational: info.penaltySeconds === 0
  }));
}

async function meshThreadChannel() {
  if (meshThreads) return meshThreads;
  if (!mesh?.node) return null;
  const { createThreadChannel } = await import("./pulsemesh-threads.browser.js");
  meshThreads = createThreadChannel({
    node: mesh.node,
    network: meshNetwork,
    engine: routeEngine,
    // What this device's one graph actually is, read from the index it
    // opened rather than assumed. Without it every followed ETA reports
    // `profileBasis: "unstated"`, and with it wrong a bike courier's
    // arrival would be quoted off a car graph with no caveat.
    engineMode: routeEngine?.root?.profile || null,
    id: mesh.node.id
  });
  return meshThreads;
}

const handlers = {
  async init({ searchBase, routeBase, pulseMeshBootstrap }) {
    searchUnavailable = null;
    // A keeper multiaddr, when the deployment has one. Empty means the
    // only mesh available is the on-device demo transport.
    meshBootstrap = String(pulseMeshBootstrap || "");
    await openSearch(searchBase);
    const meta = searchEngine?.manifest?.meta || {};

    // Directions are optional: with no reachable route index the app still
    // does search, exactly like the web demo degrades.
    if (routeBase) {
      try {
        // Bounded for the same reason as useRouteBase, and more urgently: this
        // is the startup path, so an index that hangs holds the whole app on
        // its loading screen rather than degrading to "no routing".
        const probe = await fetch(new URL("manifest.json", routeBase).toString(), {
          signal: AbortSignal.timeout(ROUTE_PROBE_TIMEOUT_MS)
        });
        if (!probe.ok) throw new Error(`manifest ${probe.status}`);
        routeEngine = await openRouteGraphUrl(routeBase);
      } catch (err) {
        const timedOut = err?.name === "TimeoutError" || err?.name === "AbortError";
        const message = String(err?.message || err);
        // A stored region built by an older version reads as a version error,
        // which is true and useless: the driver cannot act on a version
        // number. The fix is always the same and is one tap away, so say
        // that instead. The marker lets the app show it in the user's
        // language rather than parsing this sentence.
        const outdated = /Unsupported route graph|re-run the extractor/i.test(message);
        routeUnavailable = timedOut
          ? `No answer from ${routeBase}`
          : outdated
            ? "region-outdated"
            : message;
        console.warn("[route] init failed", routeBase, routeUnavailable);
      }
    } else {
      routeUnavailable = "No route index configured";
    }

    return {
      attribution: meta.attribution || "© OpenStreetMap contributors",
      license: meta.license || "ODbL-1.0",
      total: searchEngine?.manifest?.total ?? 0,
      searchUnavailable,
      ...routingInfo()
    };
  },

  /**
   * Point routing at a different index — a freshly preloaded region served
   * from local storage, or back at the network base. Only the route engine is
   * rebuilt; search is unrelated and stays warm.
   */
  async useRouteBase({ routeBase }) {
    routeEngine = null;
    routeUnavailable = null;
    if (!routeBase) {
      routeUnavailable = "No route index configured";
      return routingInfo();
    }
    try {
      // A base that has gone away answers by hanging, not by refusing, and
      // the UI cannot show anything until this returns. Bound the wait so an
      // unreachable index degrades to "no routing" instead of a dead screen.
      const probe = await fetch(new URL("manifest.json", routeBase).toString(), {
        signal: AbortSignal.timeout(ROUTE_PROBE_TIMEOUT_MS)
      });
      if (!probe.ok) throw new Error(`manifest ${probe.status}`);
      routeEngine = await openRouteGraphUrl(routeBase);
    } catch (err) {
      const timedOut = err?.name === "TimeoutError" || err?.name === "AbortError";
      routeUnavailable = timedOut
        ? `No answer from ${routeBase}`
        : String(err?.message || err);
      console.warn("[route] useRouteBase failed", routeBase, routeUnavailable);
    }
    return routingInfo();
  },

  async search({ q, anchor, size, shards }) {
    if (!searchEngine) throw new Error(searchUnavailable || "Search index unavailable");
    const response = await searchOsmQuery(searchEngine, {
      q,
      size: size || 20,
      // A picked suggestion carries its home shard, which turns a fuzzy text
      // search into a direct lookup of the place the user actually chose.
      ...(Array.isArray(shards) && shards.length ? { shards } : {}),
      ...near(anchor)
    });
    return { places: placesOf(response), total: response.total ?? 0 };
  },

  async suggest({ q, anchor, inputOffset }) {
    if (!searchEngine) throw new Error(searchUnavailable || "Search index unavailable");
    const response = await suggestOsmQuery(searchEngine, {
      q,
      size: 8,
      inputOffset: inputOffset ?? q.length,
      ...near(anchor)
    });
    return {
      suggestions: (response.suggestions || []).map(s => ({
        text: s.text || "",
        mainText: s.mainText || s.text || "",
        secondaryText: s.secondaryText || "",
        kind: s.kind || "",
        // The prediction's own resolution hints: its canonical query text and
        // the shard that owns it.
        selectionQuery: String(s.selection?.query || s.text || ""),
        selectionShards: s.selection?.shards || s.shards || []
      }))
    };
  },

  async reverse({ lat, lon }) {
    if (!searchEngine) throw new Error(searchUnavailable || "Search index unavailable");
    const response = await reverseGeocodeOsm(searchEngine, { lat, lon, size: 1 });
    const places = placesOf(response);
    return { place: places[0] || null };
  },

  async route({ from, to, alternatives, departureTime, fromHeading, accuracyMeters }) {
    if (!routeEngine) throw new Error(routeUnavailable || "Routing unavailable");
    // Live traffic when the mesh has any, the static metric otherwise —
    // the router's degradation contract means this needs no branch.
    const live = mesh?.provider() ?? null;
    const result = await routeEngine.route({
      from,
      to,
      alternatives: alternatives || 0,
      ...(live ? { live } : {}),
      ...(departureTime ? { departureTime } : {}),
      // Which limit is in force is a question about the sign the driver will
      // be looking at, so it is answered on the device's own clock and in the
      // device's own timezone. A server's idea of the hour is the wrong hour
      // for anyone who is not standing next to it, and a phone that has
      // crossed a timezone is right about which one it is in.
      departAt: new Date().toISOString(),
      // Only sent while actually moving: a heading from a standing vehicle is
      // noise, and biasing the snap with it would invent a U-turn penalty for
      // a driver who is free to pull away in either direction.
      ...(Number.isFinite(fromHeading) ? { fromHeading } : {}),
      // How good the fix that produced `from` actually was. The snap widens
      // its candidate band to match, because a band narrower than the error
      // throws away the road the car is on before anything can weigh it —
      // which is how a driver on a service road beside the A-13 was handed a
      // route down the motorway, over and over.
      ...(Number.isFinite(accuracyMeters) ? { accuracyMeters } : {})
    });
    // Scope the mesh to the roads this driver is actually on: subscribe to
    // their zones, pull their cells, warm their leaves. Only the z9 zone
    // (≈55 km) is ever visible to a peer, and the cell requests leave
    // padded, shuffled and split. Fire-and-forget — a route must not wait
    // on the network for a layer that is decoration until it arrives.
    const routes = [result, ...(result.alternatives || [])];
    followMeshRoutes(routes).catch(() => {});
    return {
      primary: trimRoute(result),
      // Alternatives are searched under the same live metric, but only the
      // primary carries the summary, so it is handed to each of them.
      alternatives: (result.alternatives || []).map(route => trimRoute(route, result.live)),
      // What the live metric did to the answer, or nulls when there is no
      // mesh. `applied` counts edges re-weighted, not states consumed.
      live: result.live
        ? {
          provider: result.live.provider,
          states: result.live.states,
          applied: result.live.applied,
          adjustedSeconds: result.adjustedSeconds ?? null
        }
        : null
    };
  },

  /**
   * Live traffic: one call, an action, and the whole status back. Kotlin
   * polls `status` for the UI and pushes `location` as the location
   * provider produces fixes; everything else is a user action.
   */
  async pulseMesh(args) {
    const { action } = args;
    switch (action) {
      case "start":
        await startMesh(
          args.mode || (meshBootstrap ? "mesh" : "demo"),
          args.simulated !== false
        );
        break;
      case "stop":
        await stopMesh();
        break;
      // The demo vehicles, without taking the mesh down with them: turning
      // invented traffic off is not the same decision as turning live
      // traffic off, and a driver who wants the second one has a switch
      // for it already.
      case "simulate":
        await setMeshSimulated(Boolean(args.enabled));
        break;
      case "contribute":
        mesh?.setContributing(Boolean(args.enabled));
        break;
      case "location":
        if (mesh && Number.isFinite(args.lat) && Number.isFinite(args.lon)) {
          if (Number.isFinite(args.batteryLevel)) meshBattery = args.batteryLevel;
          if (typeof args.charging === "boolean") meshCharging = args.charging;
          // The thread channel gets the fix first, because it is the one
          // that can veto: threads §10 rule 4 stops *traffic* contribution
          // near a run's planned stops, so a dwelling vehicle cannot
          // publish a standstill onto a road that is flowing.
          let mayContribute = true;
          if (meshRun) {
            const result = await meshRun.handleFix({
              lat: args.lat, lon: args.lon, speedMps: args.speedMps ?? 0
            }).catch(() => null);
            if (result) mayContribute = result.contributeTraffic;
          }
          if (mayContribute) {
            await mesh.onLocation({
              lat: args.lat, lon: args.lon, speedMps: args.speedMps, courseDeg: args.courseDeg
            });
          }
        }
        break;
      case "report": {
        if (!mesh) break;
        const result = await mesh.reportIncident({
          type: Number(args.type),
          // §10.4: the protocol will not mint a report on an
          // unacknowledged disclosure. Kotlin shows the sheet that says
          // a report is public and locates you, and asserts it here.
          acknowledgedPublic: Boolean(args.acknowledgedPublic)
        });
        return { ...meshStatus(), lastAction: { emitted: result.emitted, reason: result.reason ?? null } };
      }
      case "answer": {
        if (!mesh) break;
        const result = await mesh.answerIncident({
          key: String(args.key),
          polarity: Number(args.polarity),
          acknowledgedPublic: Boolean(args.acknowledgedPublic)
        });
        return { ...meshStatus(), lastAction: { emitted: result.emitted, reason: result.reason ?? null } };
      }
      case "traffic": {
        if (!mesh) return { segments: [] };
        try {
          return { segments: await mesh.traffic({ limit: args.limit || 240 }) };
        } catch (error) {
          // A traffic layer that silently draws nothing is the hardest
          // failure there is to see, so the reason travels with the
          // (empty) answer instead of disappearing into a catch.
          console.warn("[pulsemesh] traffic failed", String(error?.message || error));
          return { segments: [], error: String(error?.message || error) };
        }
      }
      case "incidents":
        return { incidents: mesh ? await mesh.incidents() : [] };
      case "shareDrive": {
        const channel = await meshThreadChannel();
        if (!channel) return { ...meshStatus(), lastAction: { emitted: false, reason: "no-mesh" } };
        const { THREAD_MODE } = await import("./pulsemesh-threads.browser.js");
        const stops = (args.stops || []).map((stop, index) => ({
          index: index + 1, lat: stop.lat, lon: stop.lon
        }));
        if (meshRun) await meshRun.finish().catch(() => {});
        // A drive this device started owns its own seed (§4.1); there is
        // no ticket behind it and nothing to hand over.
        meshTicket = null;
        meshRun = await channel.publish({
          baseUrl: args.baseUrl || "https://rangefind.dev/t",
          // §11 is a harm decision, not a bandwidth one: coarse carries
          // no position at all, so a leaked link is worth what a printed
          // timetable is. The app asks; this only obeys.
          mode: args.fine === false ? THREAD_MODE.COARSE : THREAD_MODE.FINE,
          plan: stops.length ? { stops, dwellSeconds: 0 } : null,
          ttlSeconds: Number(args.ttlSeconds) || 3 * 3600,
          // §5.2, and a different question from §11's `mode`: one is what
          // the link reveals, the other is what graph a follower should
          // compute the arrival on. A cyclist's ETA taken off a car graph
          // is wrong by a factor, quietly.
          travelMode: travelModeCode(args.travelMode)
        });
        break;
      }
      /**
       * What happened at a stop, asserted by the driver (§5.2).
       *
       * Nothing infers this. Dwell detection knows the vehicle stopped
       * somewhere, which is not the same claim as knowing the parcel
       * changed hands — so an unmarked stop stays pending on the wire,
       * and this is the only thing that writes an outcome.
       */
      case "markStop": {
        if (!meshRun) return { ...meshStatus(), lastAction: { emitted: false, reason: "no-run" } };
        // Proof of delivery, when the driver took one (§20.7). The bytes
        // arrive already compressed and already stripped of EXIF: the app
        // re-encodes the camera bitmap to JPEG before it gets here, which
        // is the host obligation the library documents and cannot itself
        // discharge. Nothing here resizes or inspects them.
        let photo = null;
        if (args.photoBase64) {
          try {
            photo = bytesFromBase64(String(args.photoBase64));
          } catch {
            return {
              ...meshStatus(),
              lastAction: { emitted: false, reason: "photo-unreadable", photoRefused: true }
            };
          }
          if (!photo.length) {
            return {
              ...meshStatus(),
              lastAction: { emitted: false, reason: "photo-empty", photoRefused: true }
            };
          }
        }
        try {
          await meshRun.markStop(Number(args.index), Number(args.outcome), {
            reason: Number(args.reason) || 0,
            note: args.note ? String(args.note) : null,
            // Deliberately not `allowCoarsePhoto`. A coarse run withholds
            // the vehicle's position on purpose and a doorstep photo hands
            // it back with a house number on it; opting out of that is the
            // dispatcher's call to make when issuing the ticket, not this
            // phone's to make on a tap.
            ...(photo ? { photo } : {})
          });
        } catch (error) {
          const message = String(error?.message || error);
          // The photo is sealed *before* the outcome is recorded or
          // emitted, so a refused photo refuses the whole mark — there is
          // no half-marked stop to explain away. Say which refusal it was
          // so the app can offer marking again without the picture.
          const refusal = photo ? photoRefusal(message) : null;
          return {
            ...meshStatus(),
            lastAction: {
              emitted: false,
              reason: refusal || message,
              photoRefused: Boolean(refusal)
            }
          };
        }
        return { ...meshStatus(), lastAction: { emitted: true, reason: null, photoRefused: false } };
      }
      /**
       * Closes out this device's run, and says which of the three endings
       * this is.
       *
       * Default is COMPLETED, which is a claim: every customer holding the
       * link and every dispatcher watching is told the run arrived. So an
       * abandoned day passes `canceled` and gets §20's CANCELED state
       * instead, with the driver's reason as the ≤64-byte note.
       *
       * `silent` is the handover case and the only one with no final
       * record at all. §20.5: two devices holding one seed are
       * indistinguishable on the wire, so a goodbye from the phone that
       * handed the job on would tell every follower the run had ended
       * while the new driver is still delivering.
       */
      case "endDrive":
        if (meshRun && !args.silent) {
          const canceled = Boolean(args.canceled);
          const note = args.note ? String(args.note) : null;
          // The record outranks the words on it: the codec refuses a note
          // over 64 bytes by throwing, and swallowing that would leave
          // followers watching a vehicle that never said goodbye.
          try {
            await meshRun.finish({ canceled, note });
          } catch {
            await meshRun.finish({ canceled }).catch(() => {});
          }
        }
        meshRun = null;
        meshTicket = null;
        break;
      /**
       * What an incoming capability is: a job to take, a drive to watch,
       * or neither. Both live in the same place — a URL fragment, which
       * browsers never transmit — and they are worth wildly different
       * things, so the app branches on the artifact rather than on how
       * it arrived.
       *
       * Identity comes from the magic, not from whether the artifact
       * decodes: a ticket issued before a wire change is an unreadable
       * *ticket*, and telling its holder that a thread link is 45 bytes
       * describes nothing they are holding.
       */
      case "classify": {
        const fragment = fragmentOf(args.link);
        if (!fragment) return { kind: null, reason: null };
        const mod = await threadsModule();
        // Three kinds come back from here since §20.4 — ticket, offer,
        // link — and the app routes on all three. An offer is never a
        // ticket that happens to be missing things: routing it to the
        // ticket arm would put an accept button on a job with no plan
        // behind it.
        const artifact = mod.classifyThreadArtifact(fragment);
        if (!artifact.kind) {
          // The third thing a phone's camera can be pointed at (§20.9):
          // another device's PMV1 card. Identity comes from the magic
          // here too, so a card this build cannot parse is still a
          // *card* rather than "not a capability".
          const card = readDeviceCardFragment(fragment, mod);
          if (card) return card;
          return { kind: null, reason: null, sealed: false, unreadable: false };
        }
        // A well-formed seal carries a reason — "this is a job, sealed to
        // a device" — which is information rather than a fault, and the
        // app must not show it as one. Only the header failing to parse
        // makes a sealed ticket unreadable.
        let unreadable = Boolean(artifact.reason);
        if (artifact.sealed) {
          try {
            mod.decodeSealedTicketHeader(fragment);
            unreadable = false;
          } catch {
            unreadable = true;
          }
        }
        return {
          kind: artifact.kind,
          reason: artifact.reason,
          sealed: Boolean(artifact.sealed),
          unreadable
        };
      }
      /**
       * What an *offer* says (§20.4), and whether it checks out.
       *
       * The courier is deciding whether to bid, which is a different
       * decision from a driver's accept and rests on different facts:
       * how much work, how far, roughly where, for how much, until when.
       * There is no stop here to report and this never pretends there
       * is — the offer carries none.
       *
       * Verified for signature and expiry, and deliberately **not**
       * against this device's graph epoch. §20.6's binding is checked
       * where it bites, on the award, which cannot be published under a
       * mismatched epoch anyway; applying it here would refuse a
       * perfectly good offer because the courier's stored region was
       * built from a different extract, and refusing to let someone bid
       * is not a safety property.
       */
      case "describeOffer": {
        const mod = await threadsModule();
        const fragment = fragmentOf(args.link);
        const artifact = mod.classifyThreadArtifact(fragment);
        if (artifact.kind !== "offer") {
          return { offer: null, kind: artifact.kind, error: artifact.reason };
        }
        let offer;
        try {
          offer = mod.decodeJobOffer(fragment);
        } catch (error) {
          return { offer: null, kind: "offer", error: String(error?.message || error) };
        }
        const verdict = await mod.verifyJobOffer(offer);
        return { offer: jobOfferSummary(offer, verdict, fragment), kind: "offer", error: null };
      }
      /**
       * What a ticket says, without taking it. Works with the mesh off,
       * and since §20.9 it is what actually opens the seal: a sealed
       * ticket is opened with *this* device's private key, and one sealed
       * to somebody else's says exactly that rather than reading as
       * corruption.
       */
      case "describeJob": {
        const mod = await threadsModule();
        const fragment = fragmentOf(args.link);
        const artifact = mod.classifyThreadArtifact(fragment);
        if (artifact.kind !== "ticket") {
          return { job: null, kind: artifact.kind, error: artifact.reason, code: null };
        }
        const opened = await openTicketBytes(fragment);
        if (!opened.bytes) {
          return { job: null, kind: "ticket", error: opened.error, code: opened.code };
        }
        let ticket;
        try {
          ticket = mod.decodeThreadTicket(opened.bytes);
        } catch (error) {
          return {
            job: null,
            kind: "ticket",
            error: String(error?.message || error),
            code: mod.SEAL_ERROR.UNREADABLE
          };
        }
        const verdict = await mod.verifyThreadTicket(ticket, { epochPrefix8: meshEpochPrefix8() });
        return {
          job: ticketJob(ticket, verdict, mod.jobIdHexOf(ticket)),
          kind: "ticket",
          sealed: Boolean(opened.sealed),
          // The award against every bid the host is holding (§20.4). One
          // answer per offer handed down, in the order they arrived, so
          // a bid this build can no longer decode still occupies its
          // place instead of shifting every verdict after it onto the
          // wrong job.
          offerChecks: awardChecks(mod, args.offers, ticket),
          error: null,
          code: null
        };
      }
      /**
       * Takes the job: publishes the run the ticket names.
       *
       * `publishTicket` verifies signature, expiry and epoch, then
       * resumes above whatever `seq` the audience has already seen
       * (§20.5), so a handover continues the customer's link instead of
       * orphaning it. The run then becomes the ordinary `meshRun`, which
       * is what makes the `location` fix path, `endDrive` and the status
       * snapshot work unchanged.
       */
      case "acceptJob": {
        const channel = await meshThreadChannel();
        if (!channel) return { ...meshStatus(), lastAction: { emitted: false, reason: "no-mesh" } };
        const { bytesToBase64Url, decodeThreadTicket, jobIdHexOf, ticketFollowLink } =
          await import("./pulsemesh-threads.browser.js");
        const baseUrl = args.baseUrl || "https://rangefind.dev/t";
        const fragment = fragmentOf(args.link);
        // §20.9: the ticket is ciphertext until this device's own key
        // opens it. Everything below is unchanged — the seal is
        // confidentiality and addressing, and the issuer's signature is
        // still the only thing that says the job is real.
        const opened = await openTicketBytes(fragment);
        if (!opened.bytes) {
          return {
            ...meshStatus(),
            lastAction: { emitted: false, reason: opened.error, code: opened.code }
          };
        }
        let ticket;
        try {
          ticket = decodeThreadTicket(opened.bytes);
        } catch (error) {
          return { ...meshStatus(), lastAction: { emitted: false, reason: String(error?.message || error) } };
        }
        if (meshRun) await meshRun.finish().catch(() => {});
        try {
          // The ticket's own travel mode wins — the dispatcher decided it
          // — and the app's is the fallback for a plan that left the
          // field at 0, which would otherwise leave every follower's ETA
          // computed on an unnamed profile.
          meshRun = await channel.publishTicket(ticket, {
            baseUrl, travelMode: travelModeCode(args.travelMode)
          });
        } catch (error) {
          meshRun = null;
          meshTicket = null;
          return { ...meshStatus(), lastAction: { emitted: false, reason: String(error?.message || error) } };
        }
        // Kept exactly as it arrived, which since §20.9 means sealed: the
        // app persists this across a process death and a plaintext copy
        // on disk would undo the whole point of a ciphertext QR.
        meshTicket = fragment;
        // The customer's link is a function of the ticket alone, so it
        // is the same 45 bytes the dispatcher printed on the order
        // confirmation before any driver existed (§20.1).
        const follows = await ticketFollowLink(ticket);
        return {
          ...meshStatus(),
          lastAction: { emitted: true, reason: null },
          job: ticketJob(ticket, { ok: true }, jobIdHexOf(ticket)),
          customerUrl: `${baseUrl}#${bytesToBase64Url(follows)}`,
          // The capability itself, handed up so the app can hold the run
          // across a process death and hand it on later. This copy dies
          // with the WebView; a delivery day does not.
          ticket: meshTicket
        };
      }
      /**
       * The active ticket as a QR matrix, for the next driver's camera.
       *
       * Deliberately the **driver** URL: handover means passing the
       * publish capability on, and the phone-to-phone form depends on no
       * domain at all. The rows pack the symbol MSB-first, one hex digit
       * per four modules, because a JSON array of 30 000 booleans across
       * the bridge for something the screen redraws is absurd.
       */
      case "ticketQr": {
        // `args.ticket` is the app's own persisted copy and it wins when
        // it is offered: this module's `meshTicket` dies with the WebView
        // while a delivery day outlives the process several times over,
        // and a handover that stops working after a process death is one
        // the driver cannot rely on at the moment they need it.
        const ticket = args.ticket ? String(args.ticket) : meshTicket;
        if (!ticket) return { size: 0, rows: [] };
        return await qrOf(`wayfind://ticket#${ticket}`);
      }
      case "followDrive": {
        const channel = await meshThreadChannel();
        if (!channel) return { ...meshStatus(), lastAction: { emitted: false, reason: "no-mesh" } };
        try {
          meshFollow = await channel.follow(String(args.link), { cellContext: mesh.cellContext });
        } catch (error) {
          return { ...meshStatus(), lastAction: { emitted: false, reason: String(error?.message || error) } };
        }
        break;
      }
      case "stopFollowing":
        meshFollow?.stop();
        meshFollow = null;
        break;
      case "followed": {
        if (!meshFollow) return { following: null };
        const point = await meshFollow.position().catch(() => null);
        // §12's claim depends on whether *this* device also has live
        // traffic, which the thread subscriber cannot know — passing it
        // is what stops the card saying "static-metric ETA" over a
        // live-traffic one. `outcomes`, `lastOutcome` and `travelMode`
        // ride along on the same status: they are cumulative in every
        // record, so the newest update carries the whole day.
        const status = meshFollow.status({ hasTraffic: (mesh?.node?.store.size() ?? 0) > 0 });
        const { travelModeName } = await import("./pulsemesh-threads.browser.js");
        return {
          following: {
            ...status,
            // The name rather than the number, because the app renders a
            // sentence and the number is this file's business.
            travelModeName: travelModeName(status.travelMode),
            lat: point?.lat ?? null,
            lon: point?.lon ?? null
          }
        };
      }
      case "followedEta": {
        if (!meshFollow || !Number.isFinite(args.lat) || !Number.isFinite(args.lon)) {
          return { eta: null };
        }
        // §9: arrival is a local route query from the position they
        // broadcast to the stop *this* device cares about, under this
        // device's live metric. The publisher never learns which.
        const eta = await meshFollow.eta({
          plan: { stops: [{ index: 1, lat: args.lat, lon: args.lon }], dwellSeconds: 0 },
          myStopIndex: 1,
          live: mesh?.provider() ?? null
        }).catch(() => null);
        return { eta };
      }
      case "tick":
        await meshTick();
        break;
      case "status":
      default:
        break;
    }
    return meshStatus();
  },

  /**
   * This device's identity, the cards it enrols, and the sealing that
   * both of them exist for (threads §20.9).
   *
   * Deliberately outside `pulseMesh`: a device has an identity whether or
   * not live traffic is running, and a driver showing their card at a
   * depot counter should not have to have joined a mesh first.
   */
  async device(args) {
    const mod = await threadsModule();
    switch (args.action) {
      // Adopt the host's stored key, or mint one. The only path on which
      // a private key ever crosses this bridge, and only upward.
      case "load":
        return await loadDevice(args);
      case "name": {
        if (!devicePrivateKey) throw new Error("This device has no identity yet.");
        deviceName = capDeviceName(args.name);
        return deviceIdentity(mod);
      }
      /** The card this phone shows: a link, and the same link as a QR. */
      case "card": {
        if (!devicePublicKey) return { identity: null, url: "", size: 0, rows: [] };
        const url = mod.deviceCardUrl({ publicKey: devicePublicKey, name: deviceName });
        return { ...deviceIdentity(mod), url, ...(await qrOf(url)) };
      }
      /** A card that arrived, decoded for the confirmation sheet. */
      case "read": {
        const card = readDeviceCardFragment(fragmentOf(args.link), mod);
        return { device: card?.device ?? null, error: card?.reason ?? null };
      }
      /**
       * Re-seals a held ticket to an enrolled device — the handover
       * (§20.5) as §20.9 now requires it to happen.
       *
       * The inner PMK1 bytes are unchanged: the same signed job, the same
       * run seed, the same plan. What changes is who can open it, which
       * is the entire content of "enrolment is the gate on transfer".
       * There is no path here that emits the ticket in the clear.
       */
      case "reseal": {
        const source = fragmentOf(args.ticket) || meshTicket;
        if (!source) return { ticket: null, error: "no-ticket", code: null };
        const recipients = (args.recipients || [])
          .map(key => String(key || "").trim())
          .filter(Boolean);
        if (!recipients.length) {
          return { ticket: null, error: "no-recipient", code: "not-enrolled" };
        }
        const opened = await openTicketBytes(source);
        if (!opened.bytes) return { ticket: null, error: opened.error, code: opened.code };
        try {
          const sealed = await mod.sealTicket(opened.bytes, recipients);
          return { ticket: mod.bytesToBase64Url(sealed), error: null, code: null };
        } catch (error) {
          return { ticket: null, error: String(error?.message || error), code: null };
        }
      }
      default:
        return deviceIdentity(mod);
    }
  },

  /**
   * Every file that makes up a route index, for offline preloading. The root
   * enumerates its own shards and names file, so the download list is exact
   * rather than guessed from a directory listing HTTP does not provide.
   */
  async regionFiles({ baseUrl }) {
    const manifestUrl = new URL("manifest.json", baseUrl).toString();
    const response = await fetch(manifestUrl);
    if (!response.ok) throw new Error(`manifest ${response.status}`);
    const manifest = await response.json();

    const probe = await openRouteGraphUrl(baseUrl);
    const root = probe.root;
    const files = ["manifest.json", manifest.root];
    if (root.namesFile) files.push(root.namesFile);
    for (const shard of root.shards || []) {
      for (const pack of shard.packs || []) {
        files.push(shard.dir ? `${shard.dir}/${pack}` : pack);
      }
    }
    return {
      files,
      profile: root.profile || "",
      nodes: manifest.nodes ?? 0,
      leaves: manifest.leaves ?? 0
    };
  },

  async snap({ lat, lon }) {
    if (!routeEngine) throw new Error(routeUnavailable || "Routing unavailable");
    const result = await routeEngine.snap({ lat, lon });
    const best = (result.matches || [])[0];
    return best
      ? {
        lat: best.snappedLatE7 / 1e7,
        lon: best.snappedLonE7 / 1e7,
        distMeters: best.distMeters,
        segment: best.segment || ""
      }
      : null;
  }
};

window.__rfCall = async function (id, method, argsJson) {
  const handler = handlers[method];
  if (!handler) {
    post(id, false, `Unknown method ${method}`);
    return;
  }
  try {
    post(id, true, await handler(argsJson ? JSON.parse(argsJson) : {}));
  } catch (err) {
    post(id, false, err);
  }
};

// Tell the host the module graph loaded; init() is a separate call so the
// app controls when network work starts.
try {
  AndroidBridge.onReady();
} catch (err) {
  // Standalone browser preview — nothing to notify.
}
