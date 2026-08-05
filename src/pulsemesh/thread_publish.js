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
  THREAD_MODE,
  THREAD_STATE,
  encodeThreadBody,
  encodeThreadBodyPreimage,
  encodeThreadRecord,
  threadRecordAad
} from "./thread_codec.js";
import {
  deriveThreadKeys,
  publicKeyFromSeed,
  sealThreadBody,
  signThread,
  threadTag,
  threadWindow
} from "./thread_crypto.js";
import { parseSegment } from "./codec.js";

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
  THREAD_STOP_LINGER: 10
});

/**
 * Creates a run publisher.
 *
 * - privateSeed: the run's Ed25519 seed. Fresh per run (§4.1); it never
 *   leaves the device and nothing derived from it goes on the wire.
 * - plan: optional run plan `{ planRef, stops: [{ lat, lon, index }],
 *   dwellSeconds }`. Drives coarse stop events and §10 rule 4.
 * - publish({ bytes, tag, seq }): transport callback.
 */
export async function createThreadPublisher({
  privateSeed,
  epoch32,
  mode = THREAD_MODE.COARSE,
  plan = null,
  publish = null,
  constants = THREAD_CONSTANTS,
  clock = Date.now,
  snap = null
} = {}) {
  const publicKey = await publicKeyFromSeed(privateSeed);
  const keys = await deriveThreadKeys(publicKey);
  const epochPrefix8 = epoch32.subarray(0, 8);
  const planRef = plan?.planRef || new Uint8Array(8);

  let seq = 0;
  let state = THREAD_STATE.SCHEDULED;
  let stopIndex = 0;
  let lastEmitMillis = -Infinity;
  let lastStateEmitted = null;
  let dwellDepartedMillis = -Infinity;
  const startedAt = clock();
  const stats = { published: 0, suppressedTraffic: 0, stopEvents: 0 };

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

  async function emit(body, nowMillis) {
    const window = threadWindow(nowMillis);
    const tag = await threadTag(keys, epoch32, window);
    const preimage = encodeThreadBodyPreimage(body);
    const signature = await signThread(preimage, privateSeed);
    const plaintext = encodeThreadBody(body, signature);
    seq += 1;
    const aad = threadRecordAad(epochPrefix8, tag, seq);
    const ciphertext = await sealThreadBody(keys, seq, aad, plaintext);
    const record = encodeThreadRecord({ epochPrefix8, tag, seq, ciphertext });
    if (record.bytes.length > constants.THREAD_MAX_RECORD_BYTES) {
      throw new Error(`PMT1 record exceeds ${constants.THREAD_MAX_RECORD_BYTES} bytes.`);
    }
    lastEmitMillis = nowMillis;
    lastStateEmitted = body.state;
    stats.published++;
    const emitted = { bytes: record.bytes, tag, seq, window, body };
    if (publish) await publish(emitted);
    return emitted;
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
    return {
      unixSeconds: Math.floor(nowMillis / 1000),
      state: runState,
      mode,
      leafCell,
      geomRef,
      ratioQ12,
      speedBin: speedBinFromMps(Math.max(0, speedMps || 0)) ?? 0,
      stopIndex,
      planRef,
      note: note || new Uint8Array(0)
    };
  }

  /**
   * One GPS fix. Returns { published, record?, reason? } and, separately,
   * `contributeTraffic` — whether the traffic channel may take this fix
   * (§10 rule 4). A dwelling bus reports 0 km/h on a flowing road, and
   * several buses on one corridor corroborate each other into a
   * convincing, entirely false standstill.
   */
  async function handleFix({ lat, lon, speedMps = 0, nowMillis = clock(), match = null, note = null }) {
    if (nowMillis - startedAt > constants.THREAD_MAX_RUN_SECONDS * 1000) {
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
      stopIndex = near.stop.index;
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

  return {
    publicKey,
    keys,
    handleFix,
    finish,
    announce,
    stats,
    get seq() { return seq; },
    get state() { return state; },
    get mode() { return mode; }
  };
}
