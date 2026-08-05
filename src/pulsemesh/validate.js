// PulseMesh validation pipeline (protocol §6): rules applied in order,
// dropping on first failure. Rules 1–9 are cheap and unconditional; rules
// 10–12 run only when the receiver holds the leaf cell (via the injected
// cellContext) and their failures carry a trust penalty for the
// delivering peer.

import { DEFAULT_CONSTANTS, MAX_SPEED_BIN, bucketAgeSeconds, bucketStartMillis } from "./bins.js";
import { INCIDENT_TYPES } from "./incidents.js";
import { MAGIC, PROOF_POW, decodePMC1, decodePMI1, verifyPow } from "./codec.js";
import { shardOfCell } from "./topics.js";
import { toHex } from "./sha256.js";

// §6 rule 10 class plausibility caps, km/h.
export const CLASS_SPEED_CAP_KMH = Object.freeze({
  motorway: 140,
  trunk: 130,
  primary: 110,
  secondary: 100,
  tertiary: 90,
  residential: 70,
  unclassified: 70,
  service: 50
});
const DEFAULT_CLASS_CAP_KMH = 50;

// §6 rule 12: classes on which contributions are dropped unconditionally.
export const DENIED_CLASSES = Object.freeze(new Set([
  "service", "driveway", "parking_aisle", "track", "living_street", "pedestrian", "footway"
]));

function fail(rule, reason, { trustPenalty = false } = {}) {
  return { ok: false, rule, reason, trustPenalty };
}

/**
 * Creates a stateful validator bound to one epoch (and optionally the
 * previous epoch during the consumer overlap window, §11.5).
 *
 * - epoch32 / previousEpoch32: full 32-byte binary epochs.
 * - cellOf(record): deterministic z15 cell (rule 8 topic consistency).
 * - cellContext(leafCell): null when the leaf is not loaded, else
 *   { polylineCount, classOf(geomRef), metersOf(geomRef) } (rules 10–12).
 * - transport: "wire" (default) rejects proofType 0; "loopback" accepts
 *   it, for phase-1 tests and the demo (§4.1).
 * - suppressedTypes: incidentPolicy.suppressedTypes from the signed
 *   bootstrap (§4.6, §10.4).
 */
export function createValidator({
  constants = DEFAULT_CONSTANTS,
  epoch32,
  previousEpoch32 = null,
  previousEpochUntilMillis = null,
  cellOf = null,
  cellContext = null,
  transport = "wire",
  suppressedTypes = [],
  clock = Date.now
} = {}) {
  if (!epoch32 || epoch32.length !== 32) throw new Error("Validator requires the full 32-byte epoch.");
  const suppressed = new Set(suppressedTypes);
  const peerBuckets = new Map();     // peerId -> { tokens, lastMillis }
  const incidentRates = new Map();   // peerId -> [acceptMillis...]
  // §11.5: the previous epoch is accepted only for EPOCH_OVERLAP after
  // handover. Segment ids are meaningful for exactly one graph epoch, so
  // an unbounded overlap would keep applying ids that no longer denote
  // the roads they denoted.
  const overlapUntil = previousEpoch32
    ? previousEpochUntilMillis ?? clock() + constants.EPOCH_OVERLAP * 1000
    : null;

  function epochPrefixMatches(prefix8, nowMillis) {
    const matches = epoch => {
      for (let i = 0; i < 8; i++) if (epoch[i] !== prefix8[i]) return false;
      return true;
    };
    if (matches(epoch32)) return epoch32;
    if (previousEpoch32 && nowMillis < overlapUntil && matches(previousEpoch32)) return previousEpoch32;
    return null;
  }

  // §6 rule 7 token bucket: RATE_SUSTAINED tokens/s, burst RATE_BURST.
  function takeToken(peerId, nowMillis) {
    if (peerId == null) return true; // locally produced or snapshot-merged
    let bucket = peerBuckets.get(peerId);
    if (!bucket) {
      bucket = { tokens: constants.RATE_BURST, lastMillis: nowMillis };
      peerBuckets.set(peerId, bucket);
    }
    bucket.tokens = Math.min(
      constants.RATE_BURST,
      bucket.tokens + ((nowMillis - bucket.lastMillis) / 1000) * constants.RATE_SUSTAINED
    );
    bucket.lastMillis = nowMillis;
    if (bucket.tokens < 1) return false;
    bucket.tokens -= 1;
    return true;
  }

  function takeIncidentToken(peerId, nowMillis) {
    if (peerId == null) return true;
    const windowMs = constants.INCIDENT_PEER_RATE_WINDOW * 1000;
    let times = incidentRates.get(peerId);
    if (!times) { times = []; incidentRates.set(peerId, times); }
    while (times.length && nowMillis - times[0] > windowMs) times.shift();
    if (times.length >= constants.INCIDENT_PEER_RATE) return false;
    times.push(nowMillis);
    return true;
  }

  function checkProof(record, isIncident) {
    if (record.proofType === PROOF_POW) {
      const epoch = epochPrefixMatches(record.epochPrefix8);
      const difficulty = constants.POW_DIFFICULTY + (isIncident ? constants.INCIDENT_POW_MULTIPLIER : 0);
      if (!verifyPow(record.preimage, epoch, record.proof, difficulty)) {
        return fail(5, "invalid proof of work");
      }
      return null;
    }
    if (record.proofType === 0 && transport === "loopback") return null;
    return fail(5, `proofType ${record.proofType} rejected on this transport`);
  }

  function checkTopic(record, topic) {
    if (!topic || !cellOf) return null;
    const cell = cellOf(record);
    if (!cell) return fail(8, "record cell unresolvable for topic check");
    if ((cell.x >> 6) !== topic.zoneX || (cell.y >> 6) !== topic.zoneY || shardOfCell(cell) !== topic.shard) {
      return fail(8, "record cell does not map to the delivery topic");
    }
    return null;
  }

  function checkLeafRules(record, isIncident) {
    const context = cellContext ? cellContext(record.leafCell) : null;
    if (!context) return null; // rules 10–12 are probabilistic across peers
    // Rule 11: segment existence.
    if ((record.geomRef >>> 1) >= context.polylineCount) {
      return fail(11, "segment does not exist in leaf", { trustPenalty: true });
    }
    // Rule 12: reportable class.
    const roadClass = context.classOf ? context.classOf(record.geomRef) : null;
    if (roadClass && DENIED_CLASSES.has(roadClass)) {
      return fail(12, `class ${roadClass} is never reportable`, { trustPenalty: true });
    }
    if (!isIncident) {
      // Rule 10: class plausibility.
      const capKmh = (roadClass && CLASS_SPEED_CAP_KMH[roadClass]) || DEFAULT_CLASS_CAP_KMH;
      const representativeKmh = 5 * record.speedBin + 2.5;
      if (representativeKmh > capKmh * 1.15) {
        return fail(10, `speed implausible for class ${roadClass || "unknown"}`, { trustPenalty: true });
      }
      const staticMeters = context.metersOf ? context.metersOf(record.geomRef) : null;
      if (record.meters > 0 && Number.isFinite(staticMeters) && staticMeters > 0) {
        if (record.meters < staticMeters * 0.8 || record.meters > staticMeters * 1.2) {
          return fail(10, "meters disagrees with static segment length", { trustPenalty: true });
        }
      }
    }
    return null;
  }

  /**
   * Validates one PMC1. `payload` may be bytes or an already-decoded
   * record (whose verbatim bytes are still attached). `store` provides
   * the rule-6 replay window. `topic` is the parsed delivery topic for
   * gossip arrivals (rule 8). Returns { ok: true, record } or a §6
   * failure with its rule number.
   */
  function validateContribution(payload, { store = null, fromPeer = null, topic = null, nowMillis = clock() } = {}) {
    let record;
    try {
      record = payload instanceof Uint8Array ? decodePMC1(payload) : payload;
    } catch (error) {
      return fail(1, error.message);
    }
    if (record.bytes.length > constants.MAX_RECORD_BYTES) return fail(1, "record exceeds MAX_RECORD_BYTES");
    if (!epochPrefixMatches(record.epochPrefix8)) return fail(2, "unknown epoch");
    if (record.speedBin > MAX_SPEED_BIN) return fail(3, "speedBin out of range");
    if (record.qualityBin < 1 || record.qualityBin > 7) return fail(3, "qualityBin out of range");
    if (record.ttlSeconds < 1 || record.ttlSeconds > 90) return fail(3, "ttlSeconds out of range");
    if (record.meters > 100000) return fail(3, "meters out of range");
    const age = bucketAgeSeconds(record.timeBucket, nowMillis);
    if (age > constants.MAX_AGE_RECEIPT) return fail(4, "record too old at receipt");
    if (bucketStartMillis(record.timeBucket) > nowMillis + constants.MAX_FUTURE_SKEW * 1000) {
      return fail(4, "record from the future");
    }
    const proofFailure = checkProof(record, false);
    if (proofFailure) return proofFailure;
    if (store && store.hasReport(toHex(record.reportId))) return fail(6, "replayed reportId");
    if (!takeToken(fromPeer, nowMillis)) return fail(7, "per-peer rate exceeded");
    const topicFailure = checkTopic(record, topic);
    if (topicFailure) return topicFailure;
    const leafFailure = checkLeafRules(record, false);
    if (leafFailure) return leafFailure;
    return { ok: true, record };
  }

  /** Validates one PMI1 (§6 with the incident-specific rows of rule 3/5/7). */
  function validateIncident(payload, { store = null, fromPeer = null, topic = null, nowMillis = clock() } = {}) {
    let record;
    try {
      record = payload instanceof Uint8Array ? decodePMI1(payload) : payload;
    } catch (error) {
      return fail(1, error.message);
    }
    if (record.bytes.length > constants.MAX_RECORD_BYTES) return fail(1, "record exceeds MAX_RECORD_BYTES");
    if (!epochPrefixMatches(record.epochPrefix8)) return fail(2, "unknown epoch");
    const type = INCIDENT_TYPES[record.type];
    if (!type) return fail(3, "unknown incident type");
    if (suppressed.has(record.type)) return fail(3, "incident type suppressed by deployment policy");
    if (record.polarity < 1 || record.polarity > 3) return fail(3, "polarity out of range");
    if (record.ratioQ12 > 4095) return fail(3, "ratioQ12 out of range");
    if (record.ttlSeconds < constants.INCIDENT_TTL_MIN || record.ttlSeconds > type.defaultTtlSeconds) {
      return fail(3, "ttlSeconds outside the type's window");
    }
    const age = bucketAgeSeconds(record.timeBucket, nowMillis);
    if (age > constants.MAX_AGE_RECEIPT) return fail(4, "record too old at receipt");
    if (bucketStartMillis(record.timeBucket) > nowMillis + constants.MAX_FUTURE_SKEW * 1000) {
      return fail(4, "record from the future");
    }
    const proofFailure = checkProof(record, true);
    if (proofFailure) return proofFailure;
    if (store && store.hasReport(toHex(record.reportId))) return fail(6, "replayed reportId");
    if (!takeToken(fromPeer, nowMillis)) return fail(7, "per-peer rate exceeded");
    if (!takeIncidentToken(fromPeer, nowMillis)) return fail(7, "per-peer incident rate exceeded");
    const topicFailure = checkTopic(record, topic);
    if (topicFailure) return topicFailure;
    const leafFailure = checkLeafRules(record, true);
    if (leafFailure) return leafFailure;
    return { ok: true, record };
  }

  return { validateContribution, validateIncident, constants };
}
