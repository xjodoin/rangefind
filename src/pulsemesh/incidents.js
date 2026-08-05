// PulseMesh manual incidents (protocol §2.6, §8.5): the fixed taxonomy,
// distinct-peer-capped scoring, contradiction decay, and the speed-anchored
// routing penalty. The design premise: a PMC1 is a measurement constrained
// by physics; a PMI1 is a claim constrained by nothing — so a claim can
// change a route only when independent measurements ratify it.

import { DEFAULT_CONSTANTS } from "./bins.js";
import { POLARITY_REFUTE } from "./codec.js";
import { congestionRatio } from "./aggregate.js";

// §2.6 taxonomy. `appliesBoth` stores/displays for both directions of the
// physical segment; `penaltySeconds` is the §9 anchored routing penalty
// (0 = informational: never affects routing under any score).
export const INCIDENT_TYPES = Object.freeze({
  1: { name: "crash", appliesBoth: false, defaultTtlSeconds: 900, penaltySeconds: 120 },
  2: { name: "hazard on road", appliesBoth: false, defaultTtlSeconds: 600, penaltySeconds: 60 },
  3: { name: "closure report", appliesBoth: false, defaultTtlSeconds: 1800, penaltySeconds: 300 },
  4: { name: "standstill", appliesBoth: false, defaultTtlSeconds: 600, penaltySeconds: 0 },
  5: { name: "police", appliesBoth: false, defaultTtlSeconds: 1800, penaltySeconds: 0 },
  6: { name: "road works", appliesBoth: false, defaultTtlSeconds: 1800, penaltySeconds: 45 },
  7: { name: "stopped vehicle", appliesBoth: false, defaultTtlSeconds: 600, penaltySeconds: 45 },
  8: { name: "object on road", appliesBoth: false, defaultTtlSeconds: 900, penaltySeconds: 60 },
  9: { name: "slippery surface", appliesBoth: true, defaultTtlSeconds: 1800, penaltySeconds: 45 },
  10: { name: "poor visibility", appliesBoth: true, defaultTtlSeconds: 1800, penaltySeconds: 0 },
  11: { name: "animal on road", appliesBoth: false, defaultTtlSeconds: 600, penaltySeconds: 0 },
  12: { name: "signal outage", appliesBoth: true, defaultTtlSeconds: 1800, penaltySeconds: 60 },
  13: { name: "hazard on shoulder", appliesBoth: false, defaultTtlSeconds: 600, penaltySeconds: 0 }
});

export const MAX_SEGMENT_PENALTY_SECONDS = 300;

/**
 * §8.5 scoring for one (segment, type) key's live entries within
 * INCIDENT_WINDOW:
 *
 *   raw     = Σ trust-weighted reports+confirms − REFUTE_WEIGHT × refutes
 *   sources = distinct delivering peers
 *   score   = min(raw, sources)
 *
 * The min is the defense: five records down one connection score 1.
 * Forwarded records keep their measurement-privacy but lose claim-weight —
 * a key corroborated only through forwarders is capped at the hint score
 * (forwarder rotation makes one reporter look like several peers, which is
 * precisely the wrong property for claims).
 */
export function scoreIncidentKey(entries, { nowMillis, trustOf = null, constants = DEFAULT_CONSTANTS } = {}) {
  const windowMs = constants.INCIDENT_WINDOW * 1000;
  let raw = 0;
  const sources = new Set();
  let hasDirect = false;
  let newestBucket = 0;
  let reports = 0, confirms = 0, refutes = 0;
  const live = [];
  for (const entry of entries) {
    const record = entry.record || entry;
    const observedAt = record.timeBucket * 15000;
    if (nowMillis - observedAt > windowMs) continue;
    live.push(entry);
    const trust = entry.deliveredBy != null && trustOf ? trustOf(entry.deliveredBy) : constants.TRUST_INIT;
    const weight = trust / 1000;
    if (record.polarity === POLARITY_REFUTE) { raw -= constants.REFUTE_WEIGHT * weight; refutes++; }
    else { raw += weight; record.polarity === 1 ? reports++ : confirms++; }
    if (entry.deliveredBy != null) sources.add(entry.deliveredBy);
    else sources.add("self");
    if (!entry.viaForward) hasDirect = true;
    if (record.timeBucket > newestBucket) newestBucket = record.timeBucket;
  }
  if (!live.length) return null;
  let score = Math.min(raw, sources.size);
  if (!hasDirect) score = Math.min(score, constants.INCIDENT_HINT_SCORE);
  score = Math.max(0, score);
  const record = (live[0].record || live[0]);
  return {
    segment: record.segment,
    segKey: `${record.leafCell}/${record.geomRef}`,
    type: record.type,
    typeName: INCIDENT_TYPES[record.type].name,
    ratioQ12: record.ratioQ12,
    raw,
    sources: sources.size,
    score,
    reports,
    confirms,
    refutes,
    newestBucket,
    displayed: score >= constants.INCIDENT_SHOW_SCORE,
    hint: score >= constants.INCIDENT_HINT_SCORE && score < constants.INCIDENT_SHOW_SCORE,
    entries: live
  };
}

/**
 * §8.5 mechanism 3, contradiction decay: when an anchored-penalty
 * incident is live on a segment whose speed aggregate (n ≥ 3) shows a
 * congestion ratio above the anchor — traffic flowing normally — its
 * remaining TTL is divided by CONTRADICTION_DECAY. Applied once per new
 * aggregate observation (keyed by the aggregate's newest bucket) so a
 * single flowing aggregate doesn't collapse the TTL exponentially.
 */
export function applyContradictionDecay(scored, aggregate, freeflowKmh, { nowMillis, constants = DEFAULT_CONSTANTS } = {}) {
  if (!scored || !aggregate || aggregate.n < constants.AGG_MIN_REPORTS) return false;
  const type = INCIDENT_TYPES[scored.type];
  if (!type || type.penaltySeconds === 0) return false;
  const ratio = congestionRatio(aggregate, freeflowKmh);
  if (ratio == null || ratio <= constants.INCIDENT_ANCHOR_RATIO) return false;
  let decayed = false;
  for (const entry of scored.entries) {
    if (entry.contradictedAtBucket === aggregate.newestBucket) continue;
    entry.contradictedAtBucket = aggregate.newestBucket;
    const remaining = entry.expiresAt - nowMillis;
    if (remaining > 0) {
      entry.expiresAt = nowMillis + remaining / constants.CONTRADICTION_DECAY;
      decayed = true;
    }
  }
  return decayed;
}

/**
 * §9 routing penalty for one segment: gated entirely on the speed anchor.
 * No independent aggregate at or below INCIDENT_ANCHOR_RATIO means no
 * penalty, at any score, ever. Informational types contribute 0. Capped
 * at 300 s.
 */
export function incidentPenaltySeconds(scoredIncidents, aggregate, freeflowKmh, { constants = DEFAULT_CONSTANTS } = {}) {
  if (!aggregate || aggregate.n < constants.AGG_MIN_REPORTS) return 0;
  const ratio = congestionRatio(aggregate, freeflowKmh);
  if (ratio == null || ratio > constants.INCIDENT_ANCHOR_RATIO) return 0;
  let penalty = 0;
  for (const scored of scoredIncidents || []) {
    if (!scored || scored.score < constants.INCIDENT_SHOW_SCORE) continue; // hints never route
    penalty += INCIDENT_TYPES[scored.type]?.penaltySeconds || 0;
  }
  return Math.min(penalty, MAX_SEGMENT_PENALTY_SECONDS);
}
