// PulseMesh contributor pipeline (protocol §10.1, §10.4): the per-fix
// state machine that turns GPS fixes into PMC1 records via the engine's
// snap(), and the manual incident-reporting rules. The codec having no
// fields for coordinates, trajectories, identities, or precise timestamps
// is what enforces rule 6 — this module only decides *whether* and *what
// bin* to emit.

import {
  DEFAULT_CONSTANTS,
  qualityBinFromSnap,
  speedBinFromMps,
  timeBucketFromMillis
} from "./bins.js";
import { INCIDENT_TYPES } from "./incidents.js";
import { PROOF_BOND, encodePMC1, encodePMI1, parseSegment } from "./codec.js";

function defaultRandomBytes(length) {
  const bytes = new Uint8Array(length);
  globalThis.crypto.getRandomValues(bytes);
  return bytes;
}

/**
 * Creates a contributor bound to one epoch.
 *
 * - snap(fix): async map-match; null or a match with { segment,
 *   distMeters, bearingDeg, ratio, snappedLatE7, snappedLonE7 }.
 * - publish({ bytes, record, forwarder }): emit callback (node.js wires
 *   this to PMB1 gossip or PMF1).
 * - profile: "cadence" (§10.1 rule 5) or a reticent profile object from
 *   reticent.js (§10.2).
 * - metersOf(segKey): static segment length for field 8.
 * - classOf(segKey): highway class, feeding the reticent place gate.
 * - proofType: PROOF_BOND (the default) on any network transport — the
 *   record carries no proof; the transport's §5.4 admission bond vouches
 *   for it, so emission and incident reports are instant. 0 only in
 *   loopback.
 * - batteryLevel(): 0..1 or null; contribution pauses below 20% unless
 *   charging().
 */
export function createContributor({
  epoch32,
  epochPrefix8,
  snap,
  publish,
  constants = DEFAULT_CONSTANTS,
  profile = "cadence",
  metersOf = null,
  classOf = null,
  proofType = PROOF_BOND,
  randomBytes = defaultRandomBytes,
  clock = Date.now,
  batteryLevel = null,
  charging = null,
  suppressedTypes = []
} = {}) {
  const suppressed = new Set(suppressedTypes);
  const reticent = profile !== "cadence" && profile !== null ? profile : null;
  const speedWindow = [];              // last 3 fix speeds (m/s)
  let lastFixMillis = -Infinity;       // smoothing resets across fix gaps
  let lastEmitMillis = -Infinity;
  let lastMoving = null;               // { segKey, adjacent:Set, atMillis }
  let lastMatch = null;                // most recent qualifying snap match
  const incidentEmits = new Map();     // type -> last emit millis
  const answeredIncidents = new Set(); // keys this device already confirmed/refuted
  const stats = { fixes: 0, offRoad: 0, suppressed: 0, emitted: 0, incidents: 0 };

  function smoothedSpeedMps(fixSpeed) {
    speedWindow.push(fixSpeed);
    if (speedWindow.length > 3) speedWindow.shift();
    let sum = 0;
    for (const value of speedWindow) sum += value;
    return sum / speedWindow.length;
  }

  function buildRecord({ segKey, leafCell, geomRef, speedBin, qualityBin, nowMillis }) {
    const meters = metersOf ? Math.round(metersOf(segKey) || 0) : 0;
    const fields = {
      epochPrefix8,
      leafCell,
      geomRef,
      timeBucket: timeBucketFromMillis(nowMillis),
      speedBin,
      qualityBin,
      meters,
      ttlSeconds: constants.CONTRIB_TTL,
      reportId: randomBytes(16),
      proofType,
      proof: new Uint8Array(0)
    };
    return encodePMC1(fields);
  }

  /**
   * Map-matches a fix and remembers it, without any of the emission
   * gates. This is step 1 of handleFix, exposed on its own because
   * reporting an incident is a different act from publishing
   * measurements: a device that contributes nothing may still report the
   * crash it is looking at, and §10.4 will only file that report for a
   * *recently snapped* position. Without this, turning contribution off
   * would silently turn incident reporting off with it.
   */
  async function observe(fix) {
    const nowMillis = fix.nowMillis ?? clock();
    const match = await snap(fix);
    if (!match || match.distMeters > 50) {
      lastMatch = null;
      return null;
    }
    lastMatch = { ...match, nowMillis };
    return lastMatch;
  }

  /**
   * §10.1 per-fix state machine. fix: { lat, lon, speedMps, courseDeg,
   * nowMillis? }. Returns { emitted, reason, record? }.
   */
  async function handleFix(fix) {
    stats.fixes++;
    const nowMillis = fix.nowMillis ?? clock();

    if (batteryLevel && batteryLevel() < 0.2 && !(charging && charging())) {
      return skip("battery");
    }

    // 1. Match.
    const match = await observe({ ...fix, nowMillis });
    if (!match) {
      stats.offRoad++;
      return skip("off-road");
    }

    // 2. Quality.
    const headingDelta = Number.isFinite(fix.courseDeg) && Number.isFinite(match.bearingDeg)
      ? Math.abs(fix.courseDeg - match.bearingDeg)
      : 0;
    const qualityBin = qualityBinFromSnap(match.distMeters, headingDelta);
    if (qualityBin === 0) return skip("quality");

    // 3. Speed, smoothed over the last 3 fixes. A gap in the fix stream
    // (tunnel, app background, parking) resets the window — smoothing
    // across a discontinuity would smear stale motion into a fresh state.
    if (nowMillis - lastFixMillis > 10 * 1000) speedWindow.length = 0;
    lastFixMillis = nowMillis;
    const speedBin = speedBinFromMps(smoothedSpeedMps(Math.max(0, fix.speedMps || 0)));
    if (speedBin == null) return skip("speed-range");

    const { leafCell, geomRef } = parseSegment(match.segment);
    const segKey = `${leafCell}/${geomRef}`;

    // 4. Standstill rule: bin 0 is congestion only if we were recently
    // moving on this or an adjacent segment; otherwise it is parking.
    if (speedBin > 0) {
      lastMoving = { segKey, atMillis: nowMillis, adjacent: adjacentKeys(leafCell, geomRef) };
    } else {
      const recentlyMoving = lastMoving
        && nowMillis - lastMoving.atMillis <= 60 * 1000
        && (lastMoving.segKey === segKey || lastMoving.adjacent.has(segKey));
      if (!recentlyMoving) return skip("stationary-off-road");
    }

    // 5. Cadence or the reticent gates.
    let forwarder = null;
    if (reticent) {
      const verdict = reticent.evaluate({
        segKey,
        speedBin,
        roadClass: classOf ? classOf(segKey) : null,
        snappedLatE7: match.snappedLatE7,
        snappedLonE7: match.snappedLonE7,
        nowMillis
      });
      if (!verdict.emit) return skip(`reticent:${verdict.gate}`);
      forwarder = verdict.forwarder;
    } else if (nowMillis - lastEmitMillis < constants.EMIT_INTERVAL * 1000) {
      return skip("cadence");
    }

    const encoded = buildRecord({ segKey, leafCell, geomRef, speedBin, qualityBin, nowMillis });
    lastEmitMillis = nowMillis;
    stats.emitted++;
    const result = { emitted: true, record: encoded, forwarder, segKey, speedBin };
    if (publish) await publish(result);
    return result;
  }

  function adjacentKeys(leafCell, geomRef) {
    // Adjacency for the standstill rule: the opposite direction of the
    // same polyline and the polyline neighbors in the same leaf — a
    // deliberately loose local notion, never serialized anywhere.
    const polyline = geomRef >>> 1;
    const keys = new Set();
    for (const p of [polyline - 1, polyline, polyline + 1]) {
      if (p < 0) continue;
      keys.add(`${leafCell}/${p * 2}`);
      keys.add(`${leafCell}/${p * 2 + 1}`);
    }
    return keys;
  }

  function skip(reason) {
    stats.suppressed++;
    return { emitted: false, reason };
  }

  /**
   * §10.4 manual incident report. Filed only for the reporter's own
   * snapped position — there is no "report anywhere" affordance, and a
   * device with no recent snap match cannot report. At most one per
   * (type, 5 min). polarity 2/3 answer a displayed incident, once.
   */
  function reportIncident({ type, polarity = 1, incidentKey = null, nowMillis = clock(), acknowledgedPublic = false }) {
    const typeInfo = INCIDENT_TYPES[type];
    if (!typeInfo) return { emitted: false, reason: "unknown-type" };
    // §10.4: the deployment's suppressed types must not be emitted, not
    // merely dropped by everyone else — a client that mints them is
    // publishing something the deployment says is unlawful there.
    if (suppressed.has(type)) return { emitted: false, reason: "suppressed-by-policy" };
    // §10.4: the user MUST be told, at the point of reporting, that a
    // report is public and locates them. The caller asserts it showed
    // that; the protocol will not mint a report on an unacknowledged
    // disclosure, so the requirement cannot be satisfied by a settings
    // page nobody read.
    if (!acknowledgedPublic) return { emitted: false, reason: "disclosure-not-acknowledged" };
    if (!lastMatch || nowMillis - lastMatch.nowMillis > 30 * 1000) {
      return { emitted: false, reason: "no-position" };
    }
    if (polarity === 1) {
      const last = incidentEmits.get(type);
      if (last != null && nowMillis - last < 5 * 60 * 1000) return { emitted: false, reason: "rate" };
    } else {
      if (!incidentKey) return { emitted: false, reason: "no-incident" };
      if (answeredIncidents.has(incidentKey)) return { emitted: false, reason: "already-answered" };
    }
    const { leafCell, geomRef } = parseSegment(lastMatch.segment);
    const fields = {
      epochPrefix8,
      leafCell,
      geomRef,
      ratioQ12: Math.max(0, Math.min(4095, Math.round((lastMatch.ratio || 0) * 4095))),
      timeBucket: timeBucketFromMillis(nowMillis),
      type,
      polarity,
      ttlSeconds: typeInfo.defaultTtlSeconds,
      reportId: randomBytes(16),
      proofType,
      proof: new Uint8Array(0)
    };
    if (polarity === 1) incidentEmits.set(type, nowMillis);
    else answeredIncidents.add(incidentKey);
    // A reticent contributor that reports must suppress measurements in
    // that cell for RETICENT_GAP so report and stream cannot be joined.
    if (reticent) reticent.suppressAfterReport(nowMillis);
    stats.incidents++;
    return { emitted: true, record: encodePMI1(fields) };
  }

  return { handleFix, observe, reportIncident, stats };
}
