// Deterministic aggregation and the local trust ledger (protocol §8).
//
// All weight arithmetic is exact integer math: Q (0..1000) × FRESHNESS
// (0..1000) × trust (250..2000) stays below 2^31 per record and below
// 2^53 summed over a segment's capped contribution set, so two conforming
// implementations holding the same records compute bit-identical
// aggregates. Confidence alone is computed in floating point, at the very
// end, after the bin is already fixed (§8.3 step 4).

import {
  DEFAULT_CONSTANTS,
  FRESHNESS,
  QUALITY_WEIGHT,
  bucketAgeSeconds,
  bucketStartMillis,
  speedBinKmh,
  speedBinMps
} from "./bins.js";

/**
 * §8.2: W(c) = Q[qualityBin] × FRESHNESS[age] × T[peer]. Ages past 90 use
 * freshness 0 per the §8.1 table note — such records are expired anyway.
 */
export function contributionWeight(record, ageSeconds, trustMilli = 1000) {
  const age = Math.max(0, Math.floor(ageSeconds));
  const freshness = age > 90 ? 0 : FRESHNESS[age];
  return QUALITY_WEIGHT[record.qualityBin] * freshness * trustMilli;
}

function compareReportIds(a, b) {
  for (let i = 0; i < 16; i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return 0;
}

/**
 * §8.3 weighted median + confidence over one (segment, direction)'s live
 * store entries ({ record, deliveredBy }). Returns null below the n ≥ 2
 * hint minimum. trustOf(peerId) → milli trust; snapshot-fetched records
 * (deliveredBy null) use TRUST_INIT.
 */
export function aggregateSegment(entries, { nowMillis, trustOf = null, constants = DEFAULT_CONSTANTS } = {}) {
  if (!entries || entries.length < 2) return null;
  const rows = entries.map(entry => {
    const record = entry.record || entry;
    const trust = entry.deliveredBy != null && trustOf ? trustOf(entry.deliveredBy) : constants.TRUST_INIT;
    const age = bucketAgeSeconds(record.timeBucket, nowMillis);
    return { record, weight: contributionWeight(record, age, trust) };
  });
  rows.sort((a, b) => (a.record.speedBin - b.record.speedBin) || compareReportIds(a.record.reportId, b.record.reportId));

  let total = 0;
  for (const row of rows) total += row.weight;
  let cum = 0;
  let aggBin = rows[rows.length - 1].record.speedBin;
  for (const row of rows) {
    cum += row.weight;
    if (2 * cum >= total) { aggBin = row.record.speedBin; break; }
  }

  const n = rows.length;
  let agreementNum = 0;
  let newestBucket = 0;
  const metersVotes = new Map();
  for (const row of rows) {
    if (Math.abs(row.record.speedBin - aggBin) <= 1) agreementNum += row.weight;
    if (row.record.timeBucket > newestBucket) newestBucket = row.record.timeBucket;
    if (row.record.meters > 0) metersVotes.set(row.record.meters, (metersVotes.get(row.record.meters) || 0) + 1);
  }
  let meters = 0;
  let bestVotes = 0;
  for (const [value, votes] of metersVotes) {
    if (votes > bestVotes || (votes === bestVotes && value < meters)) { meters = value; bestVotes = votes; }
  }

  const agreement = total > 0 ? agreementNum / total : 0;
  const diversity = Math.min(n, 6) / 6;
  const newestAge = Math.min(bucketAgeSeconds(newestBucket, nowMillis), 90);
  const freshnessFactor = FRESHNESS[newestAge] / 1000;
  let confidence = diversity * freshnessFactor * agreement;
  const hint = n < constants.AGG_MIN_REPORTS;
  if (hint) confidence = Math.min(confidence, 0.30);
  const confidenceBin = Math.round(7 * confidence);

  return {
    speedBin: aggBin,
    speedKmh: speedBinKmh(aggBin),
    speedMps: speedBinMps(aggBin),
    n,
    hint,
    totalWeight: total,
    agreementNum,
    agreement,
    diversity,
    freshnessFactor,
    confidence,
    confidenceBin,
    newestBucket,
    observedAt: bucketStartMillis(newestBucket),
    meters
  };
}

/** Congestion ratio = observed speed / static free-flow speed (§ summary). */
export function congestionRatio(aggregate, freeflowKmh) {
  if (!aggregate || !Number.isFinite(freeflowKmh) || freeflowKmh <= 0) return null;
  return aggregate.speedKmh / freeflowKmh;
}

// --- §8.4 trust ledger (local, never gossiped) ---------------------------

export class TrustLedger {
  constructor({ constants = DEFAULT_CONSTANTS, clock = Date.now } = {}) {
    this.constants = constants;
    this.clock = clock;
    this.peers = new Map(); // peerId -> { value, updatedAt }
  }

  #entry(peerId) {
    let entry = this.peers.get(peerId);
    if (!entry) {
      entry = { value: this.constants.TRUST_INIT, updatedAt: this.clock() };
      this.peers.set(peerId, entry);
    }
    return entry;
  }

  #decay(entry, nowMillis) {
    const minutes = Math.floor((nowMillis - entry.updatedAt) / 60000);
    if (minutes <= 0) return;
    for (let i = 0; i < minutes; i++) {
      entry.value += Math.round((this.constants.TRUST_INIT - entry.value) * 0.01);
    }
    entry.updatedAt += minutes * 60000;
  }

  get(peerId) {
    if (peerId == null) return this.constants.TRUST_INIT;
    const entry = this.#entry(peerId);
    this.#decay(entry, this.clock());
    return entry.value;
  }

  #adjust(peerId, delta) {
    if (peerId == null) return;
    const entry = this.#entry(peerId);
    this.#decay(entry, this.clock());
    entry.value = Math.max(this.constants.TRUST_MIN, Math.min(this.constants.TRUST_MAX, entry.value + delta));
  }

  /** −500 for any rule 10–11 validation failure. */
  penalizeValidation(peerId) {
    this.#adjust(peerId, -500);
  }

  /**
   * §8.4 corroborated remote testimony: a bounded penalty, clamped at
   * TRUST_MIN like everything else. Deliberately weaker than first-hand
   * evidence — remote input can floor a peer's *weight*, never revoke
   * its bond; revocation is reserved for what this node saw itself.
   */
  penalizeRemoteBan(peerId) {
    this.#adjust(peerId, -this.constants.BAN_REMOTE_PENALTY);
  }

  /** True when the peer sits at the floor — the §8.4 forfeiture trigger. */
  isFloored(peerId, nowMillis = this.clock()) {
    void nowMillis;
    return this.get(peerId) <= this.constants.TRUST_MIN;
  }

  /**
   * Post-aggregate feedback: +25 for landing within ±1 bin of an n ≥ 3
   * aggregate (at most one credit per aggregate per peer), −100 for ≥ 3
   * bins away.
   */
  applyAggregateFeedback(aggregate, entries) {
    if (!aggregate || aggregate.n < 3) return;
    const credited = new Set();
    for (const entry of entries) {
      const peer = entry.deliveredBy;
      if (peer == null) continue;
      const delta = Math.abs((entry.record || entry).speedBin - aggregate.speedBin);
      if (delta <= 1) {
        if (!credited.has(peer)) { this.#adjust(peer, 25); credited.add(peer); }
      } else if (delta >= 3) {
        this.#adjust(peer, -100);
      }
    }
  }
}
