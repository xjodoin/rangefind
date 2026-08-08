// PulseMesh validation pipeline (protocol §6): rules applied in order,
// dropping on first failure. Rules 1–9 are cheap and unconditional; rules
// 10–12 run only when the receiver holds the leaf cell (via the injected
// cellContext) and their failures carry a trust penalty for the
// delivering peer.

import { DEFAULT_CONSTANTS, MAX_SPEED_BIN, bucketAgeSeconds, bucketStartMillis } from "./bins.js";
import { INCIDENT_TYPES } from "./incidents.js";
import { BAN_REASON_INVALID_RECORDS, MAGIC, PROOF_BOND, decodePMC1, decodePMI1, decodePMX1 } from "./codec.js";
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

// §5.1 gossip verdicts. A GossipSub topic validator runs *before* the
// message is forwarded, so these three words decide both what this node
// keeps and what it relays — and relaying is vouching (rule 5 asks only
// whether the delivering peer is bonded). The strings are exactly
// libp2p's `TopicValidatorResult` values, spelled out here so this module
// stays importable without the optional libp2p dependency.
//
//   ACCEPT  delivered here and forwarded to our mesh peers.
//   IGNORE  neither delivered nor forwarded, and no peer is scored down.
//   REJECT  as IGNORE, plus GossipSub scores down the peer that handed us
//           the bytes — reserved for provable misbehaviour, which is
//           exactly the set the trust ledger already treats as a lie
//           (`verdict.trustPenalty`, rules 10–12).
export const GOSSIP_ACCEPT = "accept";
export const GOSSIP_IGNORE = "ignore";
export const GOSSIP_REJECT = "reject";

/** The stricter of two verdicts: reject beats ignore beats accept. */
export function worseGossipVerdict(a, b) {
  if (a === GOSSIP_REJECT || b === GOSSIP_REJECT) return GOSSIP_REJECT;
  if (a === GOSSIP_IGNORE || b === GOSSIP_IGNORE) return GOSSIP_IGNORE;
  return GOSSIP_ACCEPT;
}

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
 *   bootstrap (§4.7, §10.4).
 * - isBonded(peerId): §5.4 — whether the peer holds a live admission
 *   bond. When absent, proofType 3 is rejected on the wire: a mesh that
 *   has not deployed bonds must not accept proofless records.
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
  isBonded = null,
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

  // §8.4 ban-announcement rate: BAN_PEER_RATE per BAN_PEER_RATE_WINDOW
  // per delivering peer — testimony is cheap to relay, so its volume is
  // bounded the same way incidents are.
  const banRates = new Map(); // peerId -> [acceptMillis...]
  function takeBanToken(peerId, nowMillis) {
    if (peerId == null) return true;
    const windowMs = constants.BAN_PEER_RATE_WINDOW * 1000;
    let times = banRates.get(peerId);
    if (!times) { times = []; banRates.set(peerId, times); }
    while (times.length && nowMillis - times[0] > windowMs) times.shift();
    if (times.length >= constants.BAN_PEER_RATE) return false;
    times.push(nowMillis);
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

  function checkProof(record, vouchPeer) {
    if (record.proofType === PROOF_BOND) {
      // §5.4: the record is proofless; the delivering peer's session bond
      // vouches for it, hop by hop — the same hop the trust ledger
      // penalizes and rule 7 rate-limits, which is the realignment bonds
      // exist for. A null vouchPeer is a locally produced record: our own
      // contributor minted it, nothing external is vouching or needs to.
      if (record.proof && record.proof.length !== 0) return fail(5, "proofType 3 carries no proof bytes");
      if (vouchPeer == null) return null;
      // The loopback transport is one trusted process; there is no
      // stranger to admit, so requiring a bond would only test the mock.
      if (transport === "loopback") return null;
      if (isBonded && isBonded(vouchPeer)) return null;
      return fail(5, "delivering peer holds no admission bond");
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

  /**
   * Rules 10–12. Returns `{ judged, failure }`: `judged` is false when
   * this receiver does not hold the leaf and therefore could not apply
   * them at all. The distinction matters beyond bookkeeping — §5.1's
   * topic validator refuses to *forward* a record whose map-dependent
   * rules it could not evaluate, because relaying is vouching and a peer
   * may only vouch for what it actually checked.
   */
  function checkLeafRules(record, isIncident) {
    const context = cellContext ? cellContext(record.leafCell) : null;
    if (!context) return { judged: false, failure: null }; // rules 10–12 are probabilistic across peers
    // Rule 11: segment existence.
    if ((record.geomRef >>> 1) >= context.polylineCount) {
      return { judged: true, failure: fail(11, "segment does not exist in leaf", { trustPenalty: true }) };
    }
    // Rule 12: reportable class.
    const roadClass = context.classOf ? context.classOf(record.geomRef) : null;
    if (roadClass && DENIED_CLASSES.has(roadClass)) {
      return { judged: true, failure: fail(12, `class ${roadClass} is never reportable`, { trustPenalty: true }) };
    }
    if (!isIncident) {
      // Rule 10: class plausibility.
      const capKmh = (roadClass && CLASS_SPEED_CAP_KMH[roadClass]) || DEFAULT_CLASS_CAP_KMH;
      const representativeKmh = 5 * record.speedBin + 2.5;
      if (representativeKmh > capKmh * 1.15) {
        return {
          judged: true,
          failure: fail(10, `speed implausible for class ${roadClass || "unknown"}`, { trustPenalty: true })
        };
      }
      const staticMeters = context.metersOf ? context.metersOf(record.geomRef) : null;
      if (record.meters > 0 && Number.isFinite(staticMeters) && staticMeters > 0) {
        if (record.meters < staticMeters * 0.8 || record.meters > staticMeters * 1.2) {
          return {
            judged: true,
            failure: fail(10, "meters disagrees with static segment length", { trustPenalty: true })
          };
        }
      }
    }
    return { judged: true, failure: null };
  }

  /**
   * Validates one PMC1. `payload` may be bytes or an already-decoded
   * record (whose verbatim bytes are still attached). `store` provides
   * the rule-6 replay window. `topic` is the parsed delivery topic for
   * gossip arrivals (rule 8). Returns { ok: true, record } or a §6
   * failure with its rule number.
   */
  function validateContribution(payload, { store = null, fromPeer = null, vouchPeer = fromPeer, topic = null, nowMillis = clock() } = {}) {
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
    const proofFailure = checkProof(record, vouchPeer);
    if (proofFailure) return proofFailure;
    if (store && store.hasReport(toHex(record.reportId))) return fail(6, "replayed reportId");
    if (!takeToken(fromPeer, nowMillis)) return fail(7, "per-peer rate exceeded");
    const topicFailure = checkTopic(record, topic);
    if (topicFailure) return topicFailure;
    const leaf = checkLeafRules(record, false);
    if (leaf.failure) return leaf.failure;
    return { ok: true, record, judged: leaf.judged };
  }

  /** Validates one PMI1 (§6 with the incident-specific rows of rule 3/5/7). */
  function validateIncident(payload, { store = null, fromPeer = null, vouchPeer = fromPeer, topic = null, nowMillis = clock() } = {}) {
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
    const proofFailure = checkProof(record, vouchPeer);
    if (proofFailure) return proofFailure;
    if (store && store.hasReport(toHex(record.reportId))) return fail(6, "replayed reportId");
    if (!takeToken(fromPeer, nowMillis)) return fail(7, "per-peer rate exceeded");
    if (!takeIncidentToken(fromPeer, nowMillis)) return fail(7, "per-peer incident rate exceeded");
    const topicFailure = checkTopic(record, topic);
    if (topicFailure) return topicFailure;
    const leaf = checkLeafRules(record, true);
    if (leaf.failure) return leaf.failure;
    return { ok: true, record, judged: leaf.judged };
  }

  /**
   * §8.4: one PMX1 ban announcement. Testimony, not a verdict — this
   * only decides whether the bytes are worth counting; what a
   * corroborated count *does* is the node's decision, and it is never
   * revocation. Only bonded deliverers may testify: an accusation must
   * cost its bearer an admission.
   */
  function validateBan(payload, { fromPeer = null, nowMillis = clock() } = {}) {
    let record;
    try {
      record = payload instanceof Uint8Array ? decodePMX1(payload) : payload;
    } catch (error) {
      return fail(1, error.message);
    }
    if (!epochPrefixMatches(record.epochPrefix8, nowMillis)) return fail(2, "unknown epoch");
    if (record.reason !== BAN_REASON_INVALID_RECORDS) return fail(3, "unknown ban reason");
    const age = bucketAgeSeconds(record.timeBucket, nowMillis);
    if (age > constants.BAN_TTL) return fail(4, "ban announcement expired");
    if (bucketStartMillis(record.timeBucket) > nowMillis + constants.MAX_FUTURE_SKEW * 1000) {
      return fail(4, "ban announcement from the future");
    }
    if (fromPeer != null && !(isBonded && isBonded(fromPeer))) {
      return fail(5, "ban testimony from an unbonded deliverer");
    }
    if (!takeBanToken(fromPeer, nowMillis)) return fail(7, "per-peer ban rate exceeded");
    // A PMX1 names a peer hash, not a road: nothing here depends on
    // holding a leaf, so testimony is always fully judged.
    return { ok: true, record, judged: true };
  }

  return { validateContribution, validateIncident, validateBan, constants };
}
