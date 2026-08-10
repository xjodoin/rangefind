// Thread publisher (threads §5.3, §10 rule 4, §11): turns GPS fixes into
// sealed PMT1 updates, and decides — per §11 — how much of the vehicle's
// position the audience is told.
//
// `mode` is not a bandwidth setting. Coarse publishes stop events and a
// heartbeat with no position at all, so a leaked capability reveals
// roughly what a printed timetable does. Fine publishes a live locator.
// The recommended default follows the harm: coarse for anything carrying
// children, fine for couriers, whose position is the product.

import { speedBinFromMps } from "./bins.js";
import {
  STOP_OUTCOME,
  STOP_REASON,
  THREAD_MAX_NOTE_BYTES,
  THREAD_MAX_REASONS,
  THREAD_MODE,
  THREAD_STATE,
  THREAD_TRAVEL_MODE,
  decodeDayCertificate,
  encodeThreadBody,
  encodeThreadBodyPreimage,
  encodeThreadRecord,
  fitStopReasons,
  threadRecordAad
} from "./thread_codec.js";
import {
  PHOTO_CHAIN_ZERO,
  PHOTO_SEAL_OVERHEAD,
  THREAD_MAX_PHOTO_BYTES,
  deriveThreadKeys,
  photoChainStep,
  photoCommitment,
  photoKeyFor,
  publicKeyFromSeed,
  sealPhoto,
  sealThreadBody,
  signThread,
  threadTag,
  threadWindow
} from "./thread_crypto.js";
import { parseSegment, utf8Bytes } from "./codec.js";
import { toHex } from "./sha256.js";

export const THREAD_CONSTANTS = Object.freeze({
  THREAD_UPDATE_FINE: 5,
  THREAD_COARSE_HEARTBEAT: 60,
  THREAD_MAX_RECORD_BYTES: 256,
  THREAD_MAX_AGE: 120,
  THREAD_MAX_FUTURE_SKEW: 15,
  THREAD_STALE: 90,
  THREAD_CACHE_TTL: 600,
  THREAD_CACHE_RING: 240,
  THREAD_CACHE_TAGS: 256,
  THREAD_PROVIDE_INTERVAL: 120,
  THREAD_POLL_INTERVAL: 10,
  THREAD_MAX_RUN_SECONDS: 21600,
  THREAD_TAG_BUDGET: 32,
  THREAD_CACHE_RATE: 3,
  // §10 rule 4 stop suppression, in metres and seconds.
  THREAD_STOP_RADIUS: 40,
  THREAD_STOP_LINGER: 10,
  /**
   * §21. How often a route-day publisher re-emits its PMTC certificate.
   *
   * 60 s, matching `THREAD_COARSE_HEARTBEAT`, for two reasons that meet
   * at the same number. It bounds how long a late joiner sits holding
   * records it cannot yet verify — and one minute is inside
   * `THREAD_STALE` (90 s), so a subscriber never spends longer waiting
   * for authority than the UI would spend claiming "live" anyway. And it
   * costs almost nothing: a coarse run doubles its record rate to two
   * records a minute (~6 B/s), and a fine run pays one certificate per
   * twelve records. Cheaper would mostly buy re-verifying a certificate
   * that has not changed; dearer starts to matter inside the 240-record
   * `THREAD_CACHE_RING`, which is what makes §5.5 catch-up carry the
   * certificate for free.
   */
  THREAD_CERT_INTERVAL: 60,
  /**
   * §21. How many records a subscriber holds while it waits for the
   * day's certificate. See the note on the buffer in thread_consume.js.
   */
  THREAD_HELD_RECORDS: 32
});

/**
 * Creates a run publisher.
 *
 * Two credential shapes, and exactly one of them per publisher:
 *
 * - **One-off (§4.1).** `privateSeed` is the run's Ed25519 seed. Fresh
 *   per run; it never leaves the device, it signs the records, and the
 *   link carries its public key. This is a v1 link.
 * - **A route day (§21).** `daySeed` plus the `certificate` that vouches
 *   for it. The *root* public key comes out of the certificate and is
 *   what derives the topic tag and the content key — so the run
 *   publishes to the route's permanent address under a key that is good
 *   for one day. This is a v2 link, and it is the same 45 bytes on day 1
 *   and day 40.
 *
 * Passing `privateSeed` alongside a certificate is refused rather than
 * ignored. The root seed has no business on a driver's phone: it is the
 * term, and the only reason day keys exist is that a phone gets lost.
 *
 * - plan: optional run plan `{ planRef, stops: [{ lat, lon, index }],
 *   dwellSeconds }`. Drives coarse stop events and §10 rule 4.
 * - publish({ bytes, tag, seq }): transport callback.
 * - maxRunSeconds: how long this run may keep publishing. The 6 h
 *   default is sized for a self-started social run — someone shares a
 *   drive and forgets to stop it, and a link that outlives the reason it
 *   was shared is the harm. A **dispatched** day is not that run: it
 *   carries its own bound in the ticket's `notAfter`, which a dispatcher
 *   set deliberately, so `publishTicket` passes that instead (§20.6).
 * - travelMode: how the vehicle moves. Defaults to the plan's, because a
 *   dispatcher who wrote "bike" into a ticket decided that; 0 when
 *   nobody said.
 */
export async function createThreadPublisher({
  privateSeed = null,
  daySeed = null,
  certificate = null,
  epoch32,
  mode = THREAD_MODE.COARSE,
  plan = null,
  publish = null,
  constants = THREAD_CONSTANTS,
  clock = Date.now,
  snap = null,
  startSeq = 0,
  maxRunSeconds = constants.THREAD_MAX_RUN_SECONDS,
  travelMode = null
} = {}) {
  const dayCertificate = certificate
    ? (certificate instanceof Uint8Array ? decodeDayCertificate(certificate) : certificate)
    : null;
  if (dayCertificate && privateSeed) {
    throw new Error(
      "A route-day publisher takes the day seed and its certificate, never the route root: "
      + "a device holding the root holds the whole term, which is what day keys exist to prevent."
    );
  }
  if (dayCertificate && daySeed?.length !== 32) {
    throw new Error("A route-day publisher needs the 32-byte day seed its certificate covers.");
  }
  if (!dayCertificate && daySeed) {
    throw new Error("A day seed publishes nothing without its certificate: subscribers refuse an uncertified key.");
  }
  // The seed that **signs**, which on a route day is not the seed the
  // identity belongs to. Everything else in this file that was reaching
  // for `privateSeed` wants this one — including the §20.7 photo key, so
  // a leaked day seed opens that day's photos and no others, and the
  // depot re-derives the day seed from the root to open them later.
  const secretSeed = dayCertificate ? daySeed : privateSeed;
  if (secretSeed?.length !== 32) throw new Error("A thread publisher needs a 32-byte Ed25519 seed.");
  if (dayCertificate) {
    const dayPublicKey = await publicKeyFromSeed(daySeed);
    for (let i = 0; i < 32; i++) {
      if (dayPublicKey[i] !== dayCertificate.dayPublicKey[i]) {
        throw new Error("This day seed is not the one the certificate vouches for.");
      }
    }
  }
  // Identity, not authority: on a route day the topic tag, the content
  // key and the link all keep deriving from the **root** public key, so
  // a parent's 45 bytes are the same on day 1 and on day 40.
  const publicKey = dayCertificate ? dayCertificate.rootPublicKey : await publicKeyFromSeed(privateSeed);
  const keys = await deriveThreadKeys(publicKey);
  const epochPrefix8 = epoch32.subarray(0, 8);
  const planRef = plan?.planRef || new Uint8Array(8);
  const runTravelMode = travelMode ?? plan?.travelMode ?? THREAD_TRAVEL_MODE.UNSPECIFIED;

  // Ticket handover (§20): emit() increments before sealing, so the
  // second holder's first record is startSeq + 1 and no follower ever
  // sees a sequence regression.
  let seq = startSeq;
  let state = THREAD_STATE.SCHEDULED;
  // The last stop the run has *dealt with*, in plan order: visited and
  // resolved, or passed. Monotonic, and never a high-water mark of the
  // paperwork — an outcome recorded for a stop the vehicle has not
  // reached yet lives in the outcome map and leaves this alone (§5.2.1).
  let stopIndex = 0;
  let lastEmitMillis = -Infinity;
  let lastStateEmitted = null;
  let dwellDepartedMillis = -Infinity;
  // -Infinity, so the first thing any route-day run puts on its topic is
  // its certificate — before the record that needs it. That is also the
  // whole of "re-emits on rotation": a new day is a new publisher, and a
  // new publisher leads with the new day's certificate.
  let lastCertMillis = -Infinity;
  const startedAt = clock();
  const stats = { published: 0, suppressedTraffic: 0, stopEvents: 0, marked: 0, certificates: 0 };

  // The cumulative outcome map, one entry per plan stop, index i holding
  // the 1-based stop i + 1. Only `markStop` ever writes it: dwell
  // detection knows the vehicle stopped somewhere, which is not the same
  // claim as knowing the parcel was handed over.
  const outcomes = new Array(plan?.stops?.length || 0).fill(STOP_OUTCOME.PENDING);
  let lastOutcome = null;

  /**
   * §5.2.1. Why each unresolved-but-not-delivered stop ended that way,
   * keyed by stop index, in **mark order** — a `Map` iterates by
   * insertion, and re-marking deletes before it sets, so the most recent
   * mark is always last. That order is what the cap evicts by, and it is
   * the reason this is not simply derived from `outcomes` at emit time:
   * a bitmap knows which stops failed, not which failed most recently.
   *
   * Only skipped and failed stops are in here. A delivered stop needs no
   * reason and a pending one has nothing to say, so a normal day carries
   * an empty list and pays nothing at all for it (§5.2.2).
   */
  const reasonMarks = new Map();

  // §20.7. Sealed proof-of-delivery blobs this device holds, keyed by the
  // commitment that went on the wire. Unbounded within a run on purpose:
  // a delivery day is a few hundred photos of ~100 KB, and evicting one
  // is deleting the only copy of the evidence the run committed to —
  // there is no server holding a second one. The bound that matters is
  // per photo (THREAD_MAX_PHOTO_BYTES) and the run's own lifetime.
  const photos = new Map();

  /**
   * §20.7.1. Every commitment this run has published, in publication
   * order, and the accumulator over them.
   *
   * `photos` is keyed by commitment and so knows *what* the run holds;
   * this knows the **order** it committed to them in, which is the thing
   * the chain binds and the thing a publisher would have to lie about to
   * pass off a photo taken later as a proof from earlier. The list is
   * served to holders on request (PMTL) and is only useful because they
   * can check it against a head that is inside a signature.
   *
   * `chainHeads` is every intermediate head, so a request naming an
   * accumulator from *any* record this run ever published is recognised —
   * not merely one naming the current head.
   */
  const photoChainEntries = [];
  const chainHeads = new Set();
  let photoChain = PHOTO_CHAIN_ZERO;

  function metersBetween(a, b) {
    const toRad = Math.PI / 180;
    const dLat = (b.lat - a.lat) * toRad;
    const dLon = (b.lon - a.lon) * toRad;
    const lat = (a.lat + b.lat) / 2 * toRad;
    const x = dLon * Math.cos(lat);
    return Math.sqrt(dLat * dLat + x * x) * 6371008.8;
  }

  /** Nearest planned stop within `radius` metres, or null. */
  function stopNear(point, radius = constants.THREAD_STOP_RADIUS) {
    if (!plan?.stops?.length || !point) return null;
    let best = null;
    for (const stop of plan.stops) {
      const distance = metersBetween(point, stop);
      if (distance <= radius && (!best || distance < best.distance)) best = { stop, distance };
    }
    return best;
  }

  /** Seals one already-encoded body into a nonce-safe PMT1 and hands it to the transport. */
  async function emitPlaintext(plaintext, nowMillis, extra) {
    const window = threadWindow(nowMillis);
    const tag = await threadTag(keys, epoch32, window);
    seq += 1;
    const aad = threadRecordAad(epochPrefix8, tag, seq);
    const ciphertext = await sealThreadBody(keys, seq, aad, plaintext);
    const record = encodeThreadRecord({ epochPrefix8, tag, seq, ciphertext });
    if (record.bytes.length > constants.THREAD_MAX_RECORD_BYTES) {
      throw new Error(`PMT1 record exceeds ${constants.THREAD_MAX_RECORD_BYTES} bytes.`);
    }
    const emitted = { bytes: record.bytes, tag, seq, window, ...extra };
    if (publish) await publish(emitted);
    return emitted;
  }

  /**
   * §21. Puts the day certificate on the run's own topic, sealed under
   * the same content key as everything else.
   *
   * A record rather than a side channel, deliberately: §5.5 catch-up
   * already serves whatever is in the ring for a tag, so a parent who
   * opens the link mid-morning pulls the certificate out of the same
   * PMM1 as the records, with no new protocol id, no new message type
   * and no fetch that could fail separately from the one it depends on.
   */
  async function emitCertificate(nowMillis = clock()) {
    if (!dayCertificate) return null;
    lastCertMillis = nowMillis;
    stats.certificates++;
    return emitPlaintext(dayCertificate.bytes, nowMillis, { certificate: dayCertificate });
  }

  async function emit(body, nowMillis) {
    // Ahead of the record, never after it: a subscriber that has the
    // certificate first never has to hold anything.
    if (dayCertificate && nowMillis - lastCertMillis >= constants.THREAD_CERT_INTERVAL * 1000) {
      await emitCertificate(nowMillis);
    }
    const preimage = encodeThreadBodyPreimage(body);
    const signature = await signThread(preimage, secretSeed);
    const plaintext = encodeThreadBody(body, signature);
    const emitted = await emitPlaintext(plaintext, nowMillis, { body });
    lastEmitMillis = nowMillis;
    lastStateEmitted = body.state;
    stats.published++;
    return emitted;
  }

  /** A note as bytes, whether the caller had a string or already had bytes. */
  function noteBytes(note) {
    if (!note) return new Uint8Array(0);
    const bytes = typeof note === "string" ? utf8Bytes(note) : note;
    if (bytes.length > THREAD_MAX_NOTE_BYTES) {
      throw new Error(`A thread note is at most ${THREAD_MAX_NOTE_BYTES} bytes; this one is ${bytes.length}.`);
    }
    return bytes;
  }

  function bodyFor({ nowMillis, match, speedMps, runState, note }) {
    const coarse = mode === THREAD_MODE.COARSE;
    // §11: coarse withholds position entirely — leafCell 0 — so a leaked
    // coarse thread is a schedule, not a live child locator.
    let leafCell = 0;
    let geomRef = 0;
    let ratioQ12 = 0;
    if (!coarse && match?.segment) {
      const parsed = parseSegment(match.segment);
      leafCell = parsed.leafCell;
      geomRef = parsed.geomRef;
      ratioQ12 = Math.max(0, Math.min(4095, Math.round((match.ratio || 0) * 4095)));
      // The one real position that collides with the withheld sentinel:
      // leaf 0, polyline 0, direction 0, exactly at the segment start.
      // One quantum along the segment (about 10 cm on a 500 m road) is
      // cheaper than losing the position entirely.
      if (leafCell === 0 && geomRef === 0 && ratioQ12 === 0) ratioQ12 = 1;
    }
    const draft = {
      unixSeconds: Math.floor(nowMillis / 1000),
      state: runState,
      mode,
      travelMode: runTravelMode,
      leafCell,
      geomRef,
      ratioQ12,
      speedBin: speedBinFromMps(Math.max(0, speedMps || 0)) ?? 0,
      stopIndex,
      planRef,
      // A copy: the map keeps changing and an emitted body is a record
      // of one moment.
      outcomes: [...outcomes],
      lastOutcome: lastOutcome ? { ...lastOutcome } : null,
      stopReasons: [...reasonMarks.values()],
      // §20.7.1. The head of the photo chain as of this record. Written
      // into every record, not just the ones that follow a photo: the
      // whole point is that a holder which heard *none* of an outage can
      // check a commitment list against something it holds, and the only
      // heads it will ever hold are the ones it received.
      photoChain,
      note: noteBytes(note)
    };
    // Trimmed against this body's own remaining budget rather than a
    // fixed number alone, because a long plan and a long note are what
    // actually spend it. Oldest first: see `fitStopReasons`. Done here so
    // that a mark can never fail to encode because of the reason it was
    // carrying — losing the record would lose the outcome map with it.
    draft.stopReasons = fitStopReasons(draft);
    return draft;
  }

  /**
   * One GPS fix. Returns { published, record?, reason? } and, separately,
   * `contributeTraffic` — whether the traffic channel may take this fix
   * (§10 rule 4). A dwelling bus reports 0 km/h on a flowing road, and
   * several buses on one corridor corroborate each other into a
   * convincing, entirely false standstill.
   */
  async function handleFix({ lat, lon, speedMps = 0, nowMillis = clock(), match = null, note = null }) {
    if (nowMillis - startedAt > maxRunSeconds * 1000) {
      return { published: false, reason: "run-expired", contributeTraffic: false };
    }
    const point = Number.isFinite(lat) && Number.isFinite(lon) ? { lat, lon } : null;
    const resolved = match || (snap && point ? await snap(point) : null);

    // Run state from the plan: dwelling when stopped at a planned stop.
    const near = stopNear(point);
    const stopped = speedMps < 0.5;
    let runState = state;
    if (state === THREAD_STATE.SCHEDULED) runState = THREAD_STATE.EN_ROUTE;
    if (near && stopped) {
      runState = THREAD_STATE.DWELLING;
      // Passing or dwelling at a stop is progress through the plan, but
      // only ever forwards: a van that swings back past stop 2 on its way
      // to stop 9 has not undone stops 3 to 8.
      stopIndex = Math.max(stopIndex, near.stop.index);
    } else if (runState === THREAD_STATE.DWELLING) {
      runState = THREAD_STATE.EN_ROUTE;
      dwellDepartedMillis = nowMillis;
    }
    const wasDwelling = state === THREAD_STATE.DWELLING;
    state = runState;

    // §10 rule 4: suppress traffic contribution while dwelling, within
    // STOP_RADIUS of a planned stop, and for STOP_LINGER after departing.
    const contributeTraffic = !(
      runState === THREAD_STATE.DWELLING ||
      near !== null ||
      nowMillis - dwellDepartedMillis < constants.THREAD_STOP_LINGER * 1000
    );
    if (!contributeTraffic) stats.suppressedTraffic++;

    const stateChanged = runState !== lastStateEmitted || (wasDwelling && runState === THREAD_STATE.EN_ROUTE);
    const cadenceSeconds = mode === THREAD_MODE.COARSE
      ? constants.THREAD_COARSE_HEARTBEAT
      : constants.THREAD_UPDATE_FINE;
    const dueByCadence = nowMillis - lastEmitMillis >= cadenceSeconds * 1000;
    if (!stateChanged && !dueByCadence) {
      return { published: false, reason: "cadence", contributeTraffic };
    }
    if (stateChanged) stats.stopEvents++;
    const record = await emit(
      bodyFor({ nowMillis, match: resolved, speedMps, runState, note }),
      nowMillis
    );
    return { published: true, record, contributeTraffic };
  }

  /** Ends the run: one final update, after which the thread is dead. */
  async function finish({ nowMillis = clock(), canceled = false, note = null } = {}) {
    state = canceled ? THREAD_STATE.CANCELED : THREAD_STATE.COMPLETED;
    return emit(bodyFor({ nowMillis, match: null, speedMps: 0, runState: state, note }), nowMillis);
  }

  /** Publishes an operator note without changing the run state (§5.3 #6). */
  async function announce(note, { nowMillis = clock() } = {}) {
    state = THREAD_STATE.OFF_PLAN;
    return emit(bodyFor({ nowMillis, match: null, speedMps: 0, runState: state, note }), nowMillis);
  }

  /**
   * Seals one proof-of-delivery photo and returns its commitment (§20.7).
   *
   * The size check is on the plaintext plus the seal's own overhead, so
   * an oversized photo is refused before any crypto runs — the caller
   * gets the number it exceeded rather than a stall.
   */
  async function attachPhoto(index, photo, allowCoarsePhoto) {
    if (!(photo instanceof Uint8Array)) {
      throw new Error("A proof-of-delivery photo is a Uint8Array of already-compressed image bytes.");
    }
    if (!photo.length) throw new Error("That photo is empty.");
    if (mode === THREAD_MODE.COARSE && !allowCoarsePhoto) {
      throw new Error(
        "This run publishes coarse, which withholds the vehicle's position — and a doorstep photo "
        + "gives it away with a house number on it. Pass { allowCoarsePhoto: true } to mean it."
      );
    }
    const sealedLength = photo.length + PHOTO_SEAL_OVERHEAD;
    if (sealedLength > THREAD_MAX_PHOTO_BYTES) {
      throw new Error(
        `That photo seals to ${sealedLength} bytes and the cap is ${THREAD_MAX_PHOTO_BYTES}: `
        + "compress it further before marking the stop."
      );
    }
    const key = await photoKeyFor(secretSeed, planRef, index);
    const sealed = await sealPhoto(key, photo);
    const hash = toHex(photoCommitment(sealed));
    photos.set(hash, sealed);
    // Appended, never rewritten. A re-mark adds an entry rather than
    // replacing one: the chain is the history of what this run committed
    // to, and a history that can be edited is not evidence (§20.7.1).
    photoChainEntries.push({ stopIndex: index, commitment: hash });
    photoChain = photoChainStep(photoChain, index, hash);
    chainHeads.add(photoChain);
    return hash;
  }

  /**
   * What actually happened at a stop, asserted by the driver.
   *
   * This is the only writer of the outcome map. Dwell detection keeps
   * advancing `stopIndex` — that inference is fine, it only claims the
   * vehicle went past — but "delivered" is a different claim and nothing
   * infers it. An unmarked stop the vehicle drove past stays PENDING on
   * the wire, which is the honest answer.
   *
   * Marking records the outcome for **any** stop, and moves `stopIndex`
   * only over the contiguous run of resolved stops in front of it. A
   * dispatcher pre-marking stop 7 while the van is at stop 3 must not
   * make stops 4 to 6 look already dealt with, because every follower
   * between them would then compute "your delivery is behind us" and be
   * told nothing at all (§5.2.1, §9.1).
   *
   * Emits immediately, bypassing the cadence: a customer waiting to hear
   * their parcel was left with a neighbour should not wait out a 60 s
   * coarse heartbeat for it. Re-marking overwrites, because a failed
   * retry that is later delivered is an ordinary day.
   *
   * `photo` is an optional proof of delivery: **already-compressed image
   * bytes**, a `Uint8Array` of a JPEG, WebP or PNG the host produced.
   * This function does not resize, re-encode or strip anything, and it
   * cannot — it has no image decoder and no business having one. So
   * recompressing to a sane size and, above all, **removing EXIF (which
   * on a phone camera contains the GPS fix the run may be deliberately
   * withholding)** is the host's obligation, not this library's. See
   * §20.7; a canvas re-encode does both at once and is what the demo
   * does.
   *
   * The photo never rides gossip. It is sealed under a key derived from
   * the run's private seed (so only the driver and the dispatcher can
   * open it — a link holder cannot), kept here, and named on the wire by
   * a 32-byte commitment; the bytes travel on request over §20.7's own
   * protocol.
   *
   * `allowCoarsePhoto` defaults to **false** and refuses a photo on a
   * coarse run. §11 coarse means the operator decided this audience is
   * not entitled to the vehicle's position; a doorstep photo hands over
   * a position with a doormat and a house number on it, out of band and
   * without the granularity control ever being consulted. Opting in is
   * possible because a dispatcher who holds both the seed and the plan
   * already knows where the stop is — but it has to be said out loud.
   */
  async function markStop(index, outcome, {
    reason = STOP_REASON.NONE,
    note = null,
    photo = null,
    allowCoarsePhoto = false,
    nowMillis = clock()
  } = {}) {
    const stops = plan?.stops || [];
    if (!stops.length) throw new Error("This run has no plan, so it has no stops to mark.");
    if (!Number.isInteger(index) || index < 1 || index > stops.length) {
      throw new Error(`Stop ${index} is not on this run's plan (1..${stops.length}).`);
    }
    if (outcome !== STOP_OUTCOME.DELIVERED
      && outcome !== STOP_OUTCOME.SKIPPED
      && outcome !== STOP_OUTCOME.FAILED) {
      throw new Error("A stop is marked delivered, skipped, or failed — never back to pending.");
    }
    const reasonCode = Number.isInteger(reason) ? reason : STOP_REASON.NONE;
    if (reasonCode < 0 || reasonCode > STOP_REASON.OTHER) {
      throw new Error(`Unknown stop reason code ${reason}.`);
    }
    const photoHash = photo ? await attachPhoto(index, photo, allowCoarsePhoto) : null;
    outcomes[index - 1] = outcome;
    // `photoHash` is kept for this device's own use — it is the key into
    // `photos` — but only `hasPhoto` goes on the wire. Which commitment
    // is the accumulator's job, and 32 bytes there buys every one of them
    // rather than the newest (§20.7.1).
    lastOutcome = { stopIndex: index, outcome, reasonCode, photoHash, hasPhoto: photoHash != null };
    // §5.2.1. `lastOutcome` holds one mark and the next one replaces it,
    // so a reason published into a dead zone used to be gone the moment
    // the next stop was marked. Every resolved-not-delivered stop keeps
    // its reason here instead, and every record re-states them, so a
    // subscriber that heard none of the outage recovers the lot from the
    // first record after it.
    reasonMarks.delete(index);
    if (outcome !== STOP_OUTCOME.DELIVERED) {
      // Reason `NONE` is recorded rather than omitted: a stop that is in
      // this list with reason 0 was marked and no reason was given, which
      // a subscriber must be able to tell from a stop that is missing
      // from it because the cap dropped it (§5.2.1, `stopReasonFor`).
      reasonMarks.set(index, { stopIndex: index, reasonCode, photo: photoHash != null });
      // Delete-then-set already moved this stop to the end of the
      // insertion order; trimming from the front drops the oldest mark,
      // which is the one a listening subscriber is least likely to still
      // be missing.
      while (reasonMarks.size > THREAD_MAX_REASONS) {
        reasonMarks.delete(reasonMarks.keys().next().value);
      }
    }
    // Monotonic and contiguous. Marking stop 3 after stop 5 — the
    // paperwork catching up with the driving — records the outcome
    // without teleporting the vehicle backwards down the plan; marking
    // the stop the run is actually on advances by one, and hops over
    // whatever was already marked immediately after it.
    while (stopIndex < stops.length && outcomes[stopIndex] !== STOP_OUTCOME.PENDING) stopIndex += 1;
    // A run with a marked stop is not "scheduled, not started".
    if (state === THREAD_STATE.SCHEDULED) state = THREAD_STATE.EN_ROUTE;
    stats.marked++;
    return emit(bodyFor({ nowMillis, match: null, speedMps: 0, runState: state, note }), nowMillis);
  }

  return {
    /** The run's **identity**: on a route day, the root, not the day key. */
    publicKey,
    keys,
    handleFix,
    finish,
    announce,
    markStop,
    /** §21. The certificate this run publishes under, or null on a one-off run. */
    certificate: dayCertificate,
    /** The key that actually signs. Null on a one-off run, where it is `publicKey`. */
    dayPublicKey: dayCertificate?.dayPublicKey ?? null,
    /** Push the certificate now — for a host that knows it has new listeners. */
    emitCertificate,
    stats,
    /** The cumulative map, copied — callers must go through markStop. */
    outcomes: () => [...outcomes],
    lastOutcome: () => (lastOutcome ? { ...lastOutcome } : null),
    /**
     * §5.2.1. Every skipped or failed stop's reason, in mark order —
     * what the wire carries as much of as fits, newest first.
     */
    stopReasons: () => [...reasonMarks.values()].map(entry => ({ ...entry })),
    /**
     * The sealed blobs this run committed to (§20.7), for whatever
     * transport answers PMTF. Keyed by commitment hex; the values are
     * ciphertext, so handing this to a transport hands it nothing it
     * could read.
     */
    photoStore: photos,
    /** One sealed blob by commitment (hex), or null. */
    photoFor: hash => photos.get(String(hash || "").toLowerCase()) ?? null,
    /** §20.7.1. The accumulator as of now — what the next record carries. */
    photoChain: () => photoChain,
    /** Every commitment published, in order. Copied; the chain binds this order. */
    photoChainEntries: () => photoChainEntries.map(entry => ({ ...entry })),
    /**
     * The list, if `accumulator` is a head this run actually published.
     *
     * Any head, not only the current one: a holder's newest record may be
     * several photos old, and answering only the current head would make
     * recovery depend on being up to date — which is precisely the state
     * an outage leaves a holder out of. Verification is the holder's, and
     * it works from any head (§20.7.1).
     *
     * An unknown accumulator gets null, which the transport turns into
     * the same empty answer an unknown photo hash gets.
     */
    photoListFor: accumulator => (
      chainHeads.has(String(accumulator || "").toLowerCase())
        ? photoChainEntries.map(entry => ({ ...entry }))
        : null
    ),
    get seq() { return seq; },
    get state() { return state; },
    get stopIndex() { return stopIndex; },
    get mode() { return mode; },
    get travelMode() { return runTravelMode; }
  };
}
