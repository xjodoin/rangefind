// The reticent profile (protocol §10.2): four gates that replace cadence
// with data minimization. A contributor must pass all four to emit.
// Gate 2 (surprise) is the load-bearing one — silence becomes a signal,
// surprises are shared, and a lone unsurprising report was always
// worthless below the corroboration minimum anyway.

import { DEFAULT_CONSTANTS } from "./bins.js";

const RESIDENTIAL_CLASSES = new Set(["residential", "unclassified"]);

/**
 * Creates the reticent gate evaluator.
 *
 * - expectedBinOf(segKey): the live aggregate's speedBin when one exists
 *   with confidence ≥ SURPRISE_CONFIDENCE, else the time-bucket static
 *   free-flow bin (the contributor is also a consumer, so it holds both).
 * - companyCountOf(segKey, nowMillis): live contributions for the segment
 *   in the contributor's own store within COMPANY_WINDOW, excluding its
 *   own.
 * - isNearOwnStop(latE7, lonE7): trip endpoints and service stops within
 *   STOP_RADIUS (gate 1 is contributor-side and unenforceable by
 *   receivers — a violator harms only itself, §10.3).
 * - forwarderPool(): current candidate forwarder peer ids (gate 4
 *   rotates through them per record).
 */
export function createReticentProfile({
  constants = DEFAULT_CONSTANTS,
  expectedBinOf,
  companyCountOf,
  isNearOwnStop = null,
  forwarderPool = null,
  rng = Math.random,
  clock = Date.now
} = {}) {
  let lastEmitMillis = -Infinity;
  let dwellStartMillis = null;
  let dwellSuppressUntil = -Infinity;
  let reportSuppressUntil = -Infinity;
  let forwarderCursor = 0;
  const stats = {
    evaluated: 0, gate1: 0, gate2Surprise: 0, gate3Pass: 0, gate3Blocked: 0,
    gate4Blocked: 0, reportSuppressed: 0, emitted: 0
  };

  /**
   * Evaluates all four gates for one qualifying observation. Returns
   * { emit, surprise, gate, forwarder } — `gate` names the gate that
   * blocked emission when emit is false.
   */
  function evaluate({ segKey, speedBin, roadClass = null, snappedLatE7 = null, snappedLonE7 = null, nowMillis = clock() }) {
    stats.evaluated++;

    // Dwell tracking for gate 1: a stop of STOP_SECONDS suppresses for
    // STOP_SECONDS around it.
    if (speedBin === 0) {
      if (dwellStartMillis == null) dwellStartMillis = nowMillis;
      if (nowMillis - dwellStartMillis >= constants.STOP_SECONDS * 1000) {
        dwellSuppressUntil = nowMillis + constants.STOP_SECONDS * 1000;
      }
    } else {
      dwellStartMillis = null;
    }

    // §10.4: after filing an incident report, measurements are suppressed
    // for RETICENT_GAP so the report and the measurement stream cannot be
    // trivially joined. This outranks the surprise bypass — a surprise is
    // exactly what a reporter is most likely to be sitting in, which is
    // precisely the pairing the rule exists to break.
    if (nowMillis < reportSuppressUntil) { stats.reportSuppressed++; return blocked("report:suppression"); }

    // Gate 1 — place.
    if (roadClass && RESIDENTIAL_CLASSES.has(roadClass)) { stats.gate1++; return blocked("place:class"); }
    if (nowMillis < dwellSuppressUntil) { stats.gate1++; return blocked("place:dwell"); }
    if (isNearOwnStop && snappedLatE7 != null && isNearOwnStop(snappedLatE7, snappedLonE7)) {
      stats.gate1++;
      return blocked("place:stop");
    }

    // Gate 2 — surprise. A surprise bypasses gate 3 entirely (the first
    // witness of a new jam has no company by construction).
    const expected = expectedBinOf(segKey);
    const surprise = Number.isFinite(expected) && Math.abs(speedBin - expected) >= constants.SURPRISE_BINS;

    if (!surprise) {
      // Gate 3 — company: sampled, spaced, and never alone.
      if (nowMillis - lastEmitMillis < constants.RETICENT_GAP * 1000) { stats.gate3Blocked++; return blocked("company:gap"); }
      if (rng() >= constants.BASE_SAMPLE_RATE) { stats.gate3Blocked++; return blocked("company:sample"); }
      if (companyCountOf(segKey, nowMillis) < 1) { stats.gate3Blocked++; return blocked("company:alone"); }
      stats.gate3Pass++;
    } else {
      stats.gate2Surprise++;
    }

    // Gate 4 — forwarding, rotated per record. A contributor that selected
    // this profile asked not to publish directly, so no available
    // forwarder means no emission: falling back to a direct publish would
    // silently downgrade the protection the profile was chosen for, and a
    // skipped observation costs nothing (that is gate 3's whole argument).
    let forwarder = null;
    if (forwarderPool) {
      const pool = forwarderPool();
      if (!pool || !pool.length) { stats.gate4Blocked++; return blocked("forward:no-pool"); }
      forwarder = pool[forwarderCursor % Math.min(pool.length, constants.FORWARD_POOL)];
      forwarderCursor++;
    }

    lastEmitMillis = nowMillis;
    stats.emitted++;
    return { emit: true, surprise, gate: null, forwarder };
  }

  function blocked(gate) {
    return { emit: false, surprise: false, gate, forwarder: null };
  }

  /** §10.4: a filed report suppresses PMC1 emission for RETICENT_GAP. */
  function suppressAfterReport(nowMillis = clock()) {
    lastEmitMillis = nowMillis;
    reportSuppressUntil = nowMillis + constants.RETICENT_GAP * 1000;
  }

  return { evaluate, suppressAfterReport, stats };
}
