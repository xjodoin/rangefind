// PulseMesh bins, normative tables, time helpers, and cell arithmetic
// (protocol §2, §3, §8.1). Everything here is deterministic integer math:
// two implementations holding the same records must compute identical
// aggregates, so the freshness table is carried verbatim rather than
// derived from a platform-dependent exp() at runtime.

import { geoCellForE7 } from "../geo_cells.js";

// --- §3 normative constants (defaults; bootstrap may override tunables
// within ±4×, enforced by applyBootstrapConstants below). ----------------

export const DEFAULT_CONSTANTS = Object.freeze({
  MAX_AGE_RECEIPT: 45,
  MAX_FUTURE_SKEW: 15,
  CONTRIB_TTL: 90,
  DISPLAY_MAX_AGE: 120,
  BUCKET_SECONDS: 15,
  RETAINED_BUCKETS: 8,
  TOPIC_WINDOW: 300,
  TOPIC_OVERLAP: 30,
  ANTI_ENTROPY_SECONDS: 10,
  SHARDS: 16,
  EMIT_INTERVAL: 15,
  RATE_SUSTAINED: 2,
  RATE_BURST: 40,
  MAX_RECORD_BYTES: 96,
  MAX_GOSSIP_BYTES: 8192,
  CELL_CONTRIB_CAP: 4096,
  STORE_CONTRIB_CAP: 262144,
  SEG_CONTRIB_CAP: 64,
  BATCH_SIZES: [8, 16, 32],
  DECOY_FRACTION: 0.3,
  SPLIT_PEERS: 3,
  ENDPOINT_RINGS: 2,
  UNSUB_LINGER: 75,
  EPOCH_OVERLAP: 600,
  INCIDENT_TTL_MAX: 1800,
  TRUST_INIT: 1000,
  TRUST_MIN: 250,
  TRUST_MAX: 2000,
  AGG_MIN_REPORTS: 3,
  AGG_HINT_REPORTS: 2,
  // Reticent profile (§10.2)
  SURPRISE_BINS: 3,
  SURPRISE_CONFIDENCE: 0.4,
  BASE_SAMPLE_RATE: 0.05,
  RETICENT_GAP: 120,
  COMPANY_WINDOW: 60,
  STOP_RADIUS: 150,
  STOP_SECONDS: 90,
  FORWARD_POOL: 4,
  FORWARD_MAX_DELAY: 10,
  FORWARD_RATE: 4,
  // Incidents (§4.3, §8.5, §10.4)
  INCIDENT_SHOW_SCORE: 3,
  INCIDENT_HINT_SCORE: 1,
  INCIDENT_ANCHOR_RATIO: 0.65,
  INCIDENT_WINDOW: 600,
  INCIDENT_TTL_MIN: 300,
  INCIDENT_CELL_CAP: 24,
  INCIDENT_PEER_RATE: 6,
  INCIDENT_PEER_RATE_WINDOW: 600,
  REFUTE_WEIGHT: 2,
  CONTRADICTION_DECAY: 4,
  // Identity bonds (§5.4). BOND_BIRTHDAY_BITS 44 is the phone-safe point
  // measured in benchmarks §14.5: a 256 MiB table, ~2 s of desktop solve
  // (~8 s phone, chunked, once per BOND_LIFETIME), verification three
  // hashes. 48 (1 GiB) quarters the attacker's per-GPU parallelism again
  // and is the right setting where miners are desktop-class.
  BOND_BIRTHDAY_BITS: 44,
  BOND_PAIR_DIFFICULTY: 0,
  BOND_LIFETIME: 86400,
  BOND_OVERLAP: 3600,
  // Ban propagation (§8.4). Announcements are testimony, never verdicts:
  // BAN_MIN_SOURCES distinct bonded deliverers must corroborate before a
  // receiver applies BAN_REMOTE_PENALTY — once per corroborated target —
  // to its local trust. Nothing is ever revoked on remote input: the
  // penalty is sized so corroborated testimony plus ONE first-hand
  // violation reaches the forfeiture floor (1000−375−500 ≤ 250), while
  // testimony alone leaves an honest peer above it and recovering
  // through the ledger's ordinary decay.
  BAN_MIN_SOURCES: 3,
  BAN_REMOTE_PENALTY: 375,
  BAN_PEER_RATE: 4,
  BAN_PEER_RATE_WINDOW: 600,
  BAN_TTL: 86400,
  BAN_TARGET_CAP: 256
});

// Rows a signed bootstrap may override (§3 "tunable" column), and only
// within ±4× of the default. Everything else is fixed for protocol v1.
const TUNABLE = new Set([
  "MAX_AGE_RECEIPT", "MAX_FUTURE_SKEW", "CONTRIB_TTL", "DISPLAY_MAX_AGE",
  "RETAINED_BUCKETS", "TOPIC_OVERLAP", "ANTI_ENTROPY_SECONDS",
  "EMIT_INTERVAL", "RATE_SUSTAINED", "RATE_BURST",
  "CELL_CONTRIB_CAP", "STORE_CONTRIB_CAP", "SEG_CONTRIB_CAP",
  "DECOY_FRACTION", "SPLIT_PEERS", "ENDPOINT_RINGS", "UNSUB_LINGER",
  "EPOCH_OVERLAP", "AGG_MIN_REPORTS",
  "SURPRISE_BINS", "SURPRISE_CONFIDENCE", "BASE_SAMPLE_RATE",
  "RETICENT_GAP", "COMPANY_WINDOW", "STOP_RADIUS", "STOP_SECONDS",
  "FORWARD_POOL", "FORWARD_MAX_DELAY", "FORWARD_RATE",
  "INCIDENT_SHOW_SCORE", "INCIDENT_HINT_SCORE", "INCIDENT_ANCHOR_RATIO",
  "INCIDENT_WINDOW", "INCIDENT_CELL_CAP", "INCIDENT_PEER_RATE",
  "REFUTE_WEIGHT", "CONTRADICTION_DECAY",
  "BOND_BIRTHDAY_BITS", "BOND_PAIR_DIFFICULTY", "BOND_LIFETIME", "BOND_OVERLAP",
  "BAN_MIN_SOURCES", "BAN_REMOTE_PENALTY", "BAN_PEER_RATE", "BAN_PEER_RATE_WINDOW",
  "BAN_TTL", "BAN_TARGET_CAP"
]);

export function applyBootstrapConstants(overrides, base = DEFAULT_CONSTANTS) {
  const merged = { ...base };
  for (const [key, value] of Object.entries(overrides || {})) {
    if (!TUNABLE.has(key)) continue;
    const fallback = base[key];
    if (typeof fallback !== "number" || typeof value !== "number" || !Number.isFinite(value)) continue;
    if (value < fallback / 4 || value > fallback * 4) continue;
    merged[key] = value;
  }
  return Object.freeze(merged);
}

// --- §8.1 normative tables, verbatim. -----------------------------------

// FRESHNESS[a] = round(1000·e^(−a/60)) for a = 0..90.
export const FRESHNESS = Object.freeze([
  1000, 983, 967, 951, 936, 920, 905, 890, 875, 861, 846, 832, 819, 805, 792, 779, 766,
  753, 741, 729, 717, 705, 693, 682, 670, 659, 648, 638, 627, 617, 607, 597, 587, 577,
  567, 558, 549, 540, 531, 522, 513, 505, 497, 488, 480, 472, 465, 457, 449, 442, 435,
  427, 420, 413, 407, 400, 393, 387, 380, 374, 368, 362, 356, 350, 344, 338, 333, 327,
  322, 317, 311, 306, 301, 296, 291, 287, 282, 277, 273, 268, 264, 259, 255, 251, 247,
  243, 239, 235, 231, 227, 223
]);

// Quality weight by qualityBin 0..7.
export const QUALITY_WEIGHT = Object.freeze([0, 250, 400, 550, 700, 800, 900, 1000]);

export function freshnessAt(ageSeconds) {
  const age = Math.max(0, Math.floor(ageSeconds));
  return age > 90 ? 0 : FRESHNESS[age];
}

// --- §2.5 speed bins. ---------------------------------------------------

export const MAX_SPEED_BIN = 44;

export function speedBinFromKmh(kmh) {
  if (!Number.isFinite(kmh) || kmh < 0) return null;
  const bin = Math.floor(kmh / 5);
  return bin > MAX_SPEED_BIN ? null : bin;
}

export function speedBinFromMps(mps) {
  return speedBinFromKmh(mps * 3.6);
}

/** Representative (midpoint) speed of a bin, in km/h. */
export function speedBinKmh(bin) {
  return 5 * bin + 2.5;
}

/** Representative (midpoint) speed of a bin, in m/s. */
export function speedBinMps(bin) {
  return (5 * bin + 2.5) / 3.6;
}

// --- §2.5 quality bins from the contributor's own snap match. -----------

/**
 * qualityBin from snap distance and heading delta. Returns 1..7, or 0
 * meaning "suppress — do not emit" (0 is never legitimate on the wire).
 */
export function qualityBinFromSnap(distMeters, headingDeltaDeg = 0) {
  const d = Number(distMeters);
  if (!Number.isFinite(d) || d > 50) return 0;
  let q;
  if (d <= 5) q = 7;
  else if (d <= 10) q = 6;
  else if (d <= 15) q = 5;
  else if (d <= 20) q = 4;
  else if (d <= 30) q = 3;
  else if (d <= 40) q = 2;
  else q = 1;
  const delta = Math.abs(Number(headingDeltaDeg) || 0);
  const wrapped = delta > 180 ? 360 - delta : delta;
  if (wrapped > 60) return 0;
  if (wrapped > 30) q = Math.max(q - 2, 1);
  return q;
}

// --- §2.4 time. ---------------------------------------------------------

export function timeBucketFromMillis(unixMillis) {
  return Math.floor(unixMillis / 15000);
}

export function bucketStartMillis(bucket) {
  return bucket * 15000;
}

export function topicWindowFromMillis(unixMillis) {
  return Math.floor(unixMillis / 1000 / 300);
}

/** Whole-second age of a bucket at `nowMillis`, clamped to ≥ 0 (§2.4). */
export function bucketAgeSeconds(bucket, nowMillis) {
  return Math.max(0, Math.floor(nowMillis / 1000) - bucket * 15);
}

// --- §2.3 cells. --------------------------------------------------------

export const ZONE_ZOOM = 9;
export const DETAIL_ZOOM = 15;

export function detailCellForE7(latE7, lonE7) {
  const cell = geoCellForE7(latE7, lonE7, DETAIL_ZOOM);
  return { x: cell.x, y: cell.y };
}

export function zoneOfDetailCell(cell) {
  return { x: cell.x >> 6, y: cell.y >> 6 };
}

export function localOfDetailCell(cell) {
  return { x: cell.x & 63, y: cell.y & 63 };
}

export function detailCellKey(cell) {
  return `${cell.x}/${cell.y}`;
}

export function zoneKey(zone) {
  return `${zone.x}/${zone.y}`;
}

/** All detail cells within `rings` Chebyshev rings of `cell`, inclusive. */
export function detailCellRings(cell, rings) {
  const out = [];
  const r = Math.max(0, Math.floor(rings));
  const max = (1 << DETAIL_ZOOM) - 1;
  for (let dy = -r; dy <= r; dy++) {
    for (let dx = -r; dx <= r; dx++) {
      const x = cell.x + dx;
      const y = cell.y + dy;
      if (x >= 0 && x <= max && y >= 0 && y <= max) out.push({ x, y });
    }
  }
  return out;
}
