// PulseMesh ingest — external traffic sources become mesh records.
//
// The mesh's cold-start problem is that traffic needs contributors and
// contributors need traffic worth opening the app for. An ingest node
// bootstraps a region from infrastructure that already measures it:
// municipal detector feeds, camera analytics APIs, 511-style event
// feeds. It is a bonded peer like any other — its records carry the same
// proofless PMC1/PMI1 bytes, pass the same §6 validation at every
// receiver, and cost the same trust when they are wrong. Ingest adds
// availability the way a keeper does, never authority.
//
// What this module deliberately is NOT:
//
// - It is not a contributor (§10.1). A phone's pipeline exists to
//   protect a person — movement gates, battery, reticence. A fixed
//   sensor has no person to protect and no trajectory to leak, so it
//   builds records directly and the protections that remain are the
//   receivers': rules 1–12, the rate buckets, the trust ledger.
// - It is not a back door around corroboration. A flow observation is
//   published as `copies` records (default AGG_MIN_REPORTS) so that a
//   receiver aggregates it at all — that is this one operator saying
//   the same thing three times, not three witnesses, and every copy
//   rides the same bond. Incidents get no copies: §8.5 scores
//   `min(raw, distinct deliverers)`, so a single ingest peer can only
//   ever produce a *hint*-tier incident, and multiplying records would
//   change nothing. The speed records are what carry the authority,
//   exactly as §8.5 intends ("a PMI1 is a claim constrained by
//   nothing — so a claim can change a route only when independent
//   measurements ratify it").
// - It is not a firehose. Receivers refill RATE_SUSTAINED tokens/s per
//   delivering peer (rule 7); records past the bucket are dropped as a
//   flood and waste everyone's bytes. The publisher therefore paces
//   itself under the same bucket with a safety margin, publishes jams
//   before free flow (a free road is what the static metric already
//   says), and sheds backlog visibly in stats rather than silently.
//
// Sources are plain objects: { id, intervalSeconds, fetch(ctx) } where
// fetch resolves to an array of observations —
//
//   { kind: "flow", lat, lon, speedKmh, bearingDeg?, bothDirections?,
//     observedAtMillis? }
//   { kind: "flow", lat, lon, congestionRatio, ... } — for sources that
//     observe congestion rather than speed (camera analytics): the ratio
//     (0 stopped .. 1 free) is converted to a speed against the matched
//     segment's own free-flow after snapping, so "slow" on a motorway
//     and "slow" on a residential street mean different km/h.
//   { kind: "flow", lat, lon, vehicleCount, visibleMeters?, lanes?, ... }
//     — for sources that count vehicles (camera pixel analysis). The
//     count is extrapolated against the road's own capacity: density =
//     count / (lanes × visible km), with lanes from the matched
//     segment's class when not given, then Greenshields' fundamental
//     diagram (v/vf = 1 − k/k_jam) against the segment's free-flow.
//     Twelve cars filling a village street and twelve cars on the A-40
//     produce different, honest speeds.
//   { kind: "incident", lat, lon, type, closedRoad?, observedAtMillis? }
//
// `type` is a §2.6 code or its exact name ("crash", "road works", …).
// Everything feed-specific (URLs, auth, parsing) stays in the source's
// own fetch; this module owns map-matching, gating, encoding and pacing.
//
// `closedRoad: true` is the one place a claim crosses into the
// measurement channel, and it is deliberate. Protocol v1 has no hard
// closure (`closed` is always false in §9) and an incident pin from one
// peer is a hint that never routes — a road authority's "physically
// closed" would otherwise be invisible to the router. So a closedRoad
// incident also maintains a near-zero speed state (closureSpeedKmh,
// `copies` records refreshed inside CONTRIB_TTL) on the matched
// approach(es), which is exactly how v1 represents a closure: the
// router prices the segment as impassable and routes around it. This
// synthesizes a measurement nobody drove — the operator's bond carries
// that assertion, so sources must set it ONLY for full closures stated
// by the authority (never "lane closed", never weather), and the state
// stops being refreshed the moment the feed stops asserting it (TTL
// clears it within CONTRIB_TTL).

import {
  DEFAULT_CONSTANTS,
  qualityBinFromSnap,
  speedBinFromKmh,
  timeBucketFromMillis,
  zoneOfDetailCell,
  zoneKey
} from "./bins.js";
import { CLASS_SPEED_CAP_KMH, DENIED_CLASSES } from "./validate.js";
import { INCIDENT_TYPES } from "./incidents.js";
import { PROOF_BOND, encodePMC1, encodePMI1, parseSegment } from "./codec.js";

const INCIDENT_CODE_BY_NAME = new Map(
  Object.entries(INCIDENT_TYPES).map(([code, type]) => [type.name, Number(code)])
);

/** Fallback free-flow speed by class, for a leaf whose facts lack one. */
const CLASS_FREEFLOW_KMH = Object.freeze({
  motorway: 110, motorway_link: 70, trunk: 95, trunk_link: 60,
  primary: 75, primary_link: 50, secondary: 60, secondary_link: 45,
  tertiary: 50, tertiary_link: 40, residential: 35, unclassified: 40
});
const DEFAULT_CLASS_CAP_KMH = 50;

// Capacity extrapolation for vehicle-count observations. Lanes per
// direction by class — the road graph does not carry lane counts, so
// this is the honest approximation a camera config can override per
// camera. Jam density is the traffic-engineering standard ~140 vehicles
// per lane-kilometre; Greenshields' linear fundamental diagram
// (v/vf = 1 − k/k_jam) then prices a density as a fraction of the
// segment's own free-flow. Crude by research standards, exactly right
// for a 5 km/h-binned coarse ratio.
const CLASS_LANES = Object.freeze({
  motorway: 3, motorway_link: 1, trunk: 2, trunk_link: 1,
  primary: 2, primary_link: 1, secondary: 1, secondary_link: 1,
  tertiary: 1, residential: 1, unclassified: 1
});
const JAM_DENSITY_PER_LANE_KM = 140;
const DEFAULT_VISIBLE_METERS = 200;

function defaultRandomBytes(length) {
  const out = new Uint8Array(length);
  globalThis.crypto.getRandomValues(out);
  return out;
}

function wrappedHeadingDelta(a, b) {
  const delta = Math.abs(Number(a) - Number(b)) % 360;
  return delta > 180 ? 360 - delta : delta;
}

/** §2.6 type from a code or an exact name; null when unknown. */
export function incidentCodeOf(type) {
  if (INCIDENT_TYPES[type]) return Number(type);
  const code = INCIDENT_CODE_BY_NAME.get(String(type));
  return code ?? null;
}

/**
 * Binds external sources to a mesh session.
 *
 * - `engine` / `session`: an open route graph and a `createMeshSession`
 *   on it. The session must not be read-only, and on a wire transport
 *   its network must have minted a bond — receivers drop an unbonded
 *   peer's records at rule 5, silently from here.
 * - `copies`: PMC1 records per accepted flow observation, 1..4. Default
 *   AGG_MIN_REPORTS (3): the count where a receiver's aggregate stops
 *   being confidence-capped as a hint. This is stated corroboration by
 *   one operator, not independence; see the header.
 * - `publishFreeFlow`: false (default) publishes only congestion —
 *   ratio below `congestedRatio` — plus recovery readings on segments
 *   this node recently published as congested, so a cleared jam decays
 *   by fresh evidence rather than only by TTL. True publishes every
 *   accepted observation.
 * - `maxFeedAgeSeconds`: observations whose feed timestamp is older are
 *   dropped as stale. Records are stamped at publish time — a feed
 *   value is "current per the feed", and MAX_AGE_RECEIPT (45 s) would
 *   reject most polling cadences otherwise — so this bound is what
 *   keeps that stamp honest.
 */
export function createIngestPublisher({
  engine,
  session,
  sources = [],
  constants = DEFAULT_CONSTANTS,
  copies = DEFAULT_CONSTANTS.AGG_MIN_REPORTS,
  publishFreeFlow = false,
  congestedRatio = 0.85,
  recoverySeconds = 270,
  maxFeedAgeSeconds = 300,
  maxSnapMeters = 40,
  maxQueueSeconds = 90,
  maxQueueLength = 4096,
  incidentReEmitSeconds = 300,
  closureSpeedKmh = 2,
  closureRefreshSeconds = 40,
  clock = Date.now,
  randomBytes = defaultRandomBytes
} = {}) {
  if (!engine?.snap) throw new Error("Ingest needs an open route graph with snap().");
  if (!session?.node) throw new Error("Ingest needs a mesh session with a network — records must go somewhere.");
  const node = session.node;
  copies = Math.max(1, Math.min(4, Math.floor(copies)));

  // Publisher-side pacing, mirroring the receiver's rule 7 bucket with a
  // margin so honest jitter never lands on the exact limit.
  const bucket = {
    capacity: Math.max(1, Math.floor(constants.RATE_BURST * 0.8)),
    refillPerSecond: constants.RATE_SUSTAINED * 0.9,
    tokens: Math.max(1, Math.floor(constants.RATE_BURST * 0.8)),
    lastMillis: clock()
  };
  const incidentTimes = []; // acceptMillis of our own recent PMI1 publishes
  const incidentBudget = Math.max(1, constants.INCIDENT_PEER_RATE - 1);

  const queue = [];                    // pending intents
  const zones = new Map();             // zoneKey -> zone, accumulated
  let zonesDirty = false;
  const recentlyCongested = new Map(); // segKey -> last congested publish millis
  const incidentEmittedAt = new Map(); // segKey|type -> millis
  // segKey -> { leafCell, geomRef, qualityBin, lastAssertedMillis,
  // holdMillis, lastEmitMillis } for closedRoad assertions being kept
  // alive as near-zero speed states.
  const activeClosures = new Map();
  const closureBin = closureSpeedKmh > 0 ? speedBinFromKmh(closureSpeedKmh) : null;

  const stats = {
    fetches: 0, fetchErrors: 0, observations: 0, malformed: 0, stale: 0,
    offRoad: 0, deniedClass: 0, skippedFree: 0, noQuality: 0, noFreeflow: 0,
    queued: 0, shed: 0, publishedRecords: 0, publishedSegments: 0,
    publishedIncidents: 0, incidentDeferred: 0, incidentDeduped: 0,
    closuresAsserted: 0, closureFlowEmitted: 0
  };

  const sourceStates = sources.map(source => ({
    source,
    intervalSeconds: Math.max(5, Number(source.intervalSeconds) || 60),
    nextDueMillis: 0,
    running: false,
    lastFetchMillis: null,
    lastCount: 0,
    lastError: null
  }));

  function refill(nowMillis) {
    bucket.tokens = Math.min(
      bucket.capacity,
      bucket.tokens + ((nowMillis - bucket.lastMillis) / 1000) * bucket.refillPerSecond
    );
    bucket.lastMillis = nowMillis;
  }

  function takeIncidentSlot(nowMillis) {
    const windowMs = constants.INCIDENT_PEER_RATE_WINDOW * 1000;
    while (incidentTimes.length && nowMillis - incidentTimes[0] > windowMs) incidentTimes.shift();
    if (incidentTimes.length >= incidentBudget) return false;
    incidentTimes.push(nowMillis);
    return true;
  }

  function noteZone(leafCell) {
    const cell = node.cellOf({ leafCell, geomRef: 0 });
    if (!cell) return;
    const zone = zoneOfDetailCell(cell);
    const key = zoneKey(zone);
    if (!zones.has(key)) {
      zones.set(key, zone);
      zonesDirty = true;
    }
  }

  /** Best snap match for an observation: nearest, tie-broken by heading. */
  function pickMatch(matches, bearingDeg) {
    const near = matches.filter(match => match.distMeters <= maxSnapMeters);
    if (!near.length) return null;
    if (!Number.isFinite(bearingDeg)) return near[0];
    let best = null;
    let bestDelta = Infinity;
    for (const match of near) {
      const delta = Number.isFinite(match.bearingDeg)
        ? wrappedHeadingDelta(bearingDeg, match.bearingDeg)
        : 90;
      if (delta < bestDelta) { best = match; bestDelta = delta; }
    }
    // Past 60° the quality bin would be 0 anyway (§2.5): the feed's
    // stated direction and every nearby road disagree, so there is no
    // honest segment to put this on.
    return bestDelta > 60 ? null : best;
  }

  /** The same polyline's opposite-direction match, when snap saw one. */
  function oppositeOf(matches, chosen) {
    const { leafCell, geomRef } = parseSegment(chosen.segment);
    const wanted = `${leafCell}/${geomRef >>> 1}/${(geomRef & 1) ^ 1}`;
    return matches.find(match => match.segment === wanted) ?? null;
  }

  async function processFlow(obs, nowMillis) {
    const speedKmh = Number(obs.speedKmh);
    const hasSpeed = Number.isFinite(speedKmh) && speedKmh >= 0;
    const ratio = Number(obs.congestionRatio);
    const hasRatio = Number.isFinite(ratio) && ratio >= 0;
    const count = Number(obs.vehicleCount);
    const hasCount = Number.isFinite(count) && count >= 0;
    if ((!hasSpeed && !hasRatio && !hasCount) || !Number.isFinite(obs.lat) || !Number.isFinite(obs.lon)) {
      stats.malformed++;
      return;
    }
    if (obs.observedAtMillis != null && nowMillis - obs.observedAtMillis > maxFeedAgeSeconds * 1000) {
      stats.stale++;
      return;
    }
    let matches;
    try {
      matches = (await engine.snap({ lat: obs.lat, lon: obs.lon }, { maxSnapMeters })).matches;
    } catch {
      stats.offRoad++;
      return;
    }
    const match = pickMatch(matches, obs.bearingDeg);
    if (!match) {
      stats.offRoad++;
      return;
    }
    const targets = [match];
    if (obs.bothDirections) {
      // Only when snap itself saw the opposite approach: inventing a
      // direction copy for a one-way road would be junk a receiver holds
      // the leaf to catch.
      const opposite = oppositeOf(matches, match);
      if (opposite) targets.push(opposite);
    }
    // A vehicle count is extensive: N cars seen across a roadway that
    // carries both directions is N in total, not N each way. Splitting
    // keeps the density arithmetic honest. A speed or a congestion ratio
    // is intensive — it describes the flow, so it applies to each
    // approach unchanged.
    const shared = hasCount && targets.length > 1
      ? { ...obs, vehicleCount: count / targets.length }
      : obs;
    for (const target of targets) {
      await enqueueFlow(target, shared, hasSpeed ? speedKmh : null, nowMillis);
    }
  }

  async function enqueueFlow(match, obs, speedKmh, nowMillis) {
    const { leafCell, geomRef } = parseSegment(match.segment);
    const segKey = `${leafCell}/${geomRef}`;
    await session.warmLeaves([leafCell]);
    noteZone(leafCell);

    const context = session.cellContext(leafCell);
    const roadClass = context?.classOf(geomRef) ?? null;
    if (roadClass && DENIED_CLASSES.has(roadClass)) {
      stats.deniedClass++;
      return;
    }
    const capKmh = (roadClass && CLASS_SPEED_CAP_KMH[roadClass]) || DEFAULT_CLASS_CAP_KMH;
    const freeflowKmh = session.freeflowKmhOf(segKey)
      ?? (roadClass ? CLASS_FREEFLOW_KMH[roadClass] : null);

    // Ratio and count observations have no speed of their own — they
    // mean "this fraction / this many cars of whatever normal is HERE",
    // so without a known free-flow there is no honest number to publish.
    let observedKmh = speedKmh;
    if (observedKmh == null) {
      if (!(freeflowKmh > 0)) {
        stats.noFreeflow++;
        return;
      }
      let ratio = Number(obs.congestionRatio);
      if (!Number.isFinite(ratio)) {
        // Count → density → speed: the road graph supplies the class
        // (lanes) and free-flow; the camera supplies the count and how
        // much road it sees.
        const lanes = Number.isFinite(obs.lanes) && obs.lanes > 0
          ? obs.lanes
          : (roadClass && CLASS_LANES[roadClass]) || 1;
        const visibleKm = (Number.isFinite(obs.visibleMeters) && obs.visibleMeters > 0
          ? obs.visibleMeters
          : DEFAULT_VISIBLE_METERS) / 1000;
        const density = Number(obs.vehicleCount) / (lanes * visibleKm);
        ratio = 1 - density / JAM_DENSITY_PER_LANE_KM;
      }
      observedKmh = Math.max(0, Math.min(1.2, ratio)) * freeflowKmh;
    }
    // Rule 10 clamp: a feed value above the class cap would cost this
    // node −500 trust at every leaf-holding receiver. The observation is
    // still worth publishing — "at least this fast" — at the cap.
    const clampedKmh = Math.min(observedKmh, capKmh);
    const speedBin = speedBinFromKmh(clampedKmh);
    if (speedBin == null) {
      stats.malformed++;
      return;
    }

    const ratio = freeflowKmh > 0 ? clampedKmh / freeflowKmh : null;
    const congested = ratio == null || ratio < congestedRatio;
    if (congested) {
      recentlyCongested.set(segKey, nowMillis);
    } else if (!publishFreeFlow) {
      const congestedAt = recentlyCongested.get(segKey);
      if (congestedAt == null || nowMillis - congestedAt > recoverySeconds * 1000) {
        stats.skippedFree++;
        return;
      }
      // A recovery reading: this node said "jam" recently, so it owes
      // the mesh the "flowing again" that moves the median back up.
      recentlyCongested.delete(segKey);
    }

    const headingDelta = Number.isFinite(obs.bearingDeg) && Number.isFinite(match.bearingDeg)
      ? wrappedHeadingDelta(obs.bearingDeg, match.bearingDeg)
      : 0;
    const qualityBin = qualityBinFromSnap(match.distMeters, headingDelta);
    if (qualityBin === 0) {
      stats.noQuality++;
      return;
    }

    push({
      kind: "flow",
      leafCell,
      geomRef,
      segKey,
      speedBin,
      qualityBin,
      ratio: ratio ?? 0,
      enqueuedAtMillis: nowMillis
    });
  }

  async function processIncident(obs, nowMillis, intervalSeconds = 60) {
    const type = incidentCodeOf(obs.type);
    if (type == null || !Number.isFinite(obs.lat) || !Number.isFinite(obs.lon)) {
      stats.malformed++;
      return;
    }
    if (obs.observedAtMillis != null && nowMillis - obs.observedAtMillis > maxFeedAgeSeconds * 1000) {
      stats.stale++;
      return;
    }
    let matches;
    try {
      matches = (await engine.snap({ lat: obs.lat, lon: obs.lon }, { maxSnapMeters })).matches;
    } catch {
      stats.offRoad++;
      return;
    }
    const match = pickMatch(matches, obs.bearingDeg);
    if (!match) {
      stats.offRoad++;
      return;
    }
    // A closedRoad assertion is re-registered on EVERY fetch that still
    // lists it — before the pin dedupe below, because the speed state's
    // liveness depends on the feed still asserting the closure, not on
    // whether the pin was recently republished.
    if (obs.closedRoad && closureBin != null) {
      const targets = [match];
      if (obs.bothDirections) {
        const opposite = oppositeOf(matches, match);
        if (opposite) targets.push(opposite);
      }
      for (const target of targets) await assertClosure(target, nowMillis, intervalSeconds);
    }
    const { leafCell, geomRef } = parseSegment(match.segment);
    const segKey = `${leafCell}/${geomRef}`;
    const dedupeKey = `${segKey}|${type}`;
    const last = incidentEmittedAt.get(dedupeKey);
    if (last != null && nowMillis - last < incidentReEmitSeconds * 1000) {
      stats.incidentDeduped++;
      return;
    }
    await session.warmLeaves([leafCell]);
    noteZone(leafCell);
    const roadClass = session.cellContext(leafCell)?.classOf(geomRef) ?? null;
    if (roadClass && DENIED_CLASSES.has(roadClass)) {
      stats.deniedClass++;
      return;
    }
    push({
      kind: "incident",
      leafCell,
      geomRef,
      segKey,
      dedupeKey,
      type,
      ratioQ12: Math.max(0, Math.min(4095, Math.round((match.ratio || 0) * 4095))),
      ttlSeconds: INCIDENT_TYPES[type].defaultTtlSeconds,
      enqueuedAtMillis: nowMillis
    });
  }

  /**
   * Registers (or re-asserts) one approach of a closed road. The state
   * is kept alive by refreshClosures until the feed stops asserting it:
   * the hold window is two fetch intervals, so one missed poll does not
   * flicker the closure, and a delisted closure dies within a poll plus
   * CONTRIB_TTL.
   */
  async function assertClosure(match, nowMillis, intervalSeconds) {
    const { leafCell, geomRef } = parseSegment(match.segment);
    const segKey = `${leafCell}/${geomRef}`;
    const holdMillis = (2 * intervalSeconds + 60) * 1000;
    const existing = activeClosures.get(segKey);
    if (existing) {
      existing.lastAssertedMillis = nowMillis;
      existing.holdMillis = holdMillis;
      return;
    }
    await session.warmLeaves([leafCell]);
    noteZone(leafCell);
    const roadClass = session.cellContext(leafCell)?.classOf(geomRef) ?? null;
    if (roadClass && DENIED_CLASSES.has(roadClass)) {
      stats.deniedClass++;
      return;
    }
    const qualityBin = qualityBinFromSnap(match.distMeters, 0);
    if (qualityBin === 0) {
      stats.noQuality++;
      return;
    }
    activeClosures.set(segKey, {
      leafCell, geomRef, qualityBin,
      lastAssertedMillis: nowMillis,
      holdMillis,
      lastEmitMillis: 0
    });
    stats.closuresAsserted++;
  }

  /** Keeps every live closure's speed state fresh inside CONTRIB_TTL. */
  function refreshClosures(nowMillis) {
    if (closureBin == null) return;
    for (const [segKey, entry] of activeClosures) {
      if (nowMillis - entry.lastAssertedMillis > entry.holdMillis) {
        activeClosures.delete(segKey);
        continue;
      }
      if (nowMillis - entry.lastEmitMillis < closureRefreshSeconds * 1000) continue;
      entry.lastEmitMillis = nowMillis;
      recentlyCongested.set(segKey, nowMillis);
      push({
        kind: "flow",
        leafCell: entry.leafCell,
        geomRef: entry.geomRef,
        segKey,
        speedBin: closureBin,
        qualityBin: entry.qualityBin,
        ratio: 0,
        enqueuedAtMillis: nowMillis
      });
      stats.closureFlowEmitted++;
    }
  }

  function push(intent) {
    queue.push(intent);
    stats.queued++;
    if (queue.length > maxQueueLength) {
      // Shed oldest first: a backlog this deep is already past
      // maxQueueSeconds by the time it would drain.
      queue.sort(drainOrder);
      stats.shed += queue.length - maxQueueLength;
      queue.length = maxQueueLength;
    }
  }

  // Incidents first (rare, bounded by their own budget), then jams
  // before free flow: when the token budget bites, what gets dropped is
  // what the static metric already said. Within incidents, the budget
  // goes to the highest-impact types first — §2.6's own penalty
  // ordering, so a closure outranks a crash outranks road works.
  function drainOrder(a, b) {
    if (a.kind !== b.kind) return a.kind === "incident" ? -1 : 1;
    if (a.kind === "incident") {
      const impact = (INCIDENT_TYPES[b.type]?.penaltySeconds || 0)
        - (INCIDENT_TYPES[a.type]?.penaltySeconds || 0);
      if (impact) return impact;
    }
    if (a.kind === "flow" && a.ratio !== b.ratio) return a.ratio - b.ratio;
    return a.enqueuedAtMillis - b.enqueuedAtMillis;
  }

  function buildFlowRecord(intent, nowMillis) {
    return encodePMC1({
      epochPrefix8: node.epochPrefix8,
      leafCell: intent.leafCell,
      geomRef: intent.geomRef,
      timeBucket: timeBucketFromMillis(nowMillis),
      speedBin: intent.speedBin,
      qualityBin: intent.qualityBin,
      meters: Math.round(session.cellContext(intent.leafCell)?.metersOf(intent.geomRef) || 0),
      ttlSeconds: constants.CONTRIB_TTL,
      reportId: randomBytes(16),
      proofType: PROOF_BOND,
      proof: new Uint8Array(0)
    });
  }

  function buildIncidentRecord(intent, nowMillis) {
    return encodePMI1({
      epochPrefix8: node.epochPrefix8,
      leafCell: intent.leafCell,
      geomRef: intent.geomRef,
      ratioQ12: intent.ratioQ12,
      timeBucket: timeBucketFromMillis(nowMillis),
      type: intent.type,
      polarity: 1,
      ttlSeconds: intent.ttlSeconds,
      reportId: randomBytes(16),
      proofType: PROOF_BOND,
      proof: new Uint8Array(0)
    });
  }

  function drain(nowMillis) {
    refill(nowMillis);
    if (zonesDirty) {
      node.subscribeZones([...zones.values()], nowMillis);
      zonesDirty = false;
    }
    queue.sort(drainOrder);
    const held = [];
    while (queue.length) {
      const intent = queue[0];
      if (nowMillis - intent.enqueuedAtMillis > maxQueueSeconds * 1000) {
        queue.shift();
        stats.shed++;
        continue;
      }
      if (intent.kind === "incident") {
        if (bucket.tokens < 1) break;
        queue.shift();
        if (!takeIncidentSlot(nowMillis)) {
          // The incident budget is its own window, not the byte bucket:
          // hold the intent for a later drain instead of burning it.
          held.push(intent);
          stats.incidentDeferred++;
          continue;
        }
        bucket.tokens -= 1;
        node.publishRecord(buildIncidentRecord(intent, nowMillis), { nowMillis });
        incidentEmittedAt.set(intent.dedupeKey, nowMillis);
        stats.publishedIncidents++;
        continue;
      }
      // All copies or none: a lone record aggregates to nothing at the
      // receiver, so partial copies would spend tokens on silence.
      if (bucket.tokens < copies) break;
      queue.shift();
      bucket.tokens -= copies;
      for (let copy = 0; copy < copies; copy++) {
        node.publishRecord(buildFlowRecord(intent, nowMillis), { nowMillis });
        stats.publishedRecords++;
      }
      stats.publishedSegments++;
    }
    if (held.length) queue.unshift(...held);
  }

  async function runSource(state, nowMillis) {
    state.running = true;
    stats.fetches++;
    try {
      const observations = await state.source.fetch({ nowMillis, engine, stats });
      state.lastFetchMillis = nowMillis;
      state.lastCount = Array.isArray(observations) ? observations.length : 0;
      state.lastError = null;
      for (const obs of observations || []) {
        stats.observations++;
        if (obs?.kind === "incident") await processIncident(obs, nowMillis, state.intervalSeconds);
        else if (obs?.kind === "flow") await processFlow(obs, nowMillis);
        else stats.malformed++;
      }
    } catch (error) {
      stats.fetchErrors++;
      state.lastError = String(error?.message || error);
    } finally {
      state.running = false;
    }
  }

  /**
   * One beat: poll every due source, then publish what the budget
   * allows. Call it about once a second — the drain is what needs the
   * cadence; sources fire on their own intervals inside it.
   */
  async function tick(nowMillis = clock()) {
    const due = [];
    for (const state of sourceStates) {
      if (state.running || nowMillis < state.nextDueMillis) continue;
      // Scheduled before the fetch resolves so a slow feed cannot pile
      // overlapping requests onto itself.
      state.nextDueMillis = nowMillis + state.intervalSeconds * 1000;
      due.push(runSource(state, nowMillis));
    }
    if (due.length) await Promise.all(due);
    refreshClosures(clock());
    drain(clock());
  }

  return {
    tick,
    stats,
    get queueLength() { return queue.length; },
    get closureCount() { return activeClosures.size; },
    sources: () => sourceStates.map(state => ({
      id: state.source.id,
      intervalSeconds: state.intervalSeconds,
      lastFetchMillis: state.lastFetchMillis,
      lastCount: state.lastCount,
      lastError: state.lastError
    }))
  };
}
