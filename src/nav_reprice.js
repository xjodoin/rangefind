// When a live re-price should change a drive, and — much more of the
// work — when it should not.
//
// A route is priced once, when it is computed: `route({ live })` calls
// the provider's `fetch()` and nothing re-prices the answer afterwards.
// That is right for a query and wrong for a drive, because the jam that
// matters is the one that forms after you set off. A host that closes
// that gap recomputes the best path from here, every so often and
// whenever the mesh says the corridor got worse — and then has to decide
// what to do with the answer.
//
// That decision is the whole of this module, and it is deliberately
// separate from any host. It touches no DOM, no route engine and no
// mesh: it takes two routes' segment ids, two durations and a policy,
// and returns one of three verdicts. The reason it is its own file is
// that it is the part with the interesting edge cases — a different path
// that is somehow *slower*, a re-price on a route already finished, an
// engine that returned nothing — and none of those are reachable from a
// browser demo's inline code.
//
// The verdicts:
//
//   "switch"  — a genuinely different way, fast enough to be worth
//               interrupting the driver for.
//   "refresh" — the same way, at a materially different cost. Re-install
//               so the ETA is honest, and say nothing about turns.
//   "keep"    — leave the drive alone.
//
// Adoption is reluctant on purpose. The router already returns the best
// path from here under current traffic, so a *different* answer is by
// construction no worse than the one being driven — the temptation is
// therefore to take it every time. But switching costs the driver
// attention, and a corridor that flickers across a threshold would spend
// that attention continuously. So a new road has to clear both an
// absolute floor and a share of what is left: 60 s saved matters on a
// ten-minute drive and is noise on a three-hour one.

// The router's own blend, imported rather than restated: pricing the
// path already being driven differently from the way the alternatives
// to it are priced would manufacture savings out of the discrepancy.
import { resolveLiveFactor } from "./live_blend.js";

/** The default policy. Every field is seconds except `*Share`, a ratio. */
export const DEFAULT_REPRICE_POLICY = Object.freeze({
  /** A different path must save at least this long to be worth taking. */
  minGainSeconds: 60,
  /** …and at least this share of the journey still ahead. */
  gainShare: 0.06,
  /** The same path is re-installed when its cost moves by this much. */
  etaShiftSeconds: 45,
  /**
   * Share of the candidate — by distance where the caller supplies it —
   * that must already be on the route being driven for the two to count
   * as the same way.
   */
  samePathShare: 0.9,
  /**
   * …and an absolute floor underneath it: a candidate that leaves the
   * current route for less than this is not a different way, whatever
   * the share says. A re-price starts at the driver's fix and re-snaps,
   * so a couple of short joining edges are always new — and near the end
   * of a drive those few edges are a large *share* of what little is
   * left. Without this floor the last kilometre of every journey looks
   * like a detour.
   */
  minDetourMeters: 150
});

/** A route's segment ids, in order, skipping edges that carry none. */
export function segmentsOf(route) {
  const out = [];
  for (const edge of route?.edges || []) {
    if (edge?.segment) out.push(edge.segment);
  }
  return out;
}

/**
 * A route as `{ segment, meters }`, which is what the comparison below
 * would rather have: a detour is a distance, not a number of edges.
 */
export function pathOf(route) {
  const out = [];
  for (const edge of route?.edges || []) {
    if (edge?.segment) out.push({ segment: edge.segment, meters: Number(edge.meters) });
  }
  return out;
}

/**
 * How much of `candidate` runs on road that `current` already covers,
 * as a share 0..1, plus the distance that does not.
 *
 * Weighted by metres when every candidate entry carries them (`pathOf`),
 * and by edge count otherwise (`segmentsOf`). The distinction matters:
 * two 30 m joining edges are 10% of a twenty-edge candidate and 1% of
 * its distance, and only the second number describes whether the driver
 * is being sent a different way.
 *
 * Deliberately asymmetric — the question is "is the candidate a way I am
 * already going", not "do these two overlap". A candidate that is a
 * short prefix of a long current route is the same way, the driver
 * simply has not got there yet; a current route that is a short prefix
 * of a long candidate is a detour.
 *
 * With nothing to compare, reports a perfect match: the honest reading
 * of no evidence is "no evidence of a different path", and the caller's
 * response to that is to leave the drive alone.
 */
export function pathOverlap(currentSegments, candidate) {
  const current = currentSegments instanceof Set
    ? currentSegments
    : new Set((currentSegments || []).map(entry => (typeof entry === "string" ? entry : entry?.segment)));
  const entries = candidate || [];
  if (!current.size || !entries.length) return { share: 1, unsharedMeters: 0, weighted: false };

  const normalised = entries.map(entry => (typeof entry === "string"
    ? { segment: entry, meters: NaN }
    : { segment: entry?.segment, meters: Number(entry?.meters) }));
  // All or nothing: a run where some edges carry metres and some do not
  // would weight the two kinds against each other, which means nothing.
  const weighted = normalised.every(entry => Number.isFinite(entry.meters) && entry.meters >= 0);

  let total = 0;
  let shared = 0;
  let unsharedMeters = 0;
  for (const entry of normalised) {
    const weight = weighted ? entry.meters : 1;
    total += weight;
    if (current.has(entry.segment)) shared += weight;
    else if (weighted) unsharedMeters += entry.meters;
  }
  return {
    share: total > 0 ? shared / total : 1,
    unsharedMeters: weighted ? unsharedMeters : NaN,
    weighted
  };
}

/** The share alone, for callers that only want the number. */
export function sharedShare(currentSegments, candidate) {
  return pathOverlap(currentSegments, candidate).share;
}

/**
 * The part of a route still ahead of a driver `progressMeters` along it,
 * as `{ segment, meters, seconds }`. The edge the driver is on is
 * prorated rather than dropped or kept whole.
 */
export function remainingPath(route, progressMeters = 0) {
  const passed = Math.max(0, Number(progressMeters) || 0);
  const out = [];
  let travelled = 0;
  for (const edge of route?.edges || []) {
    if (!edge?.segment) continue;
    const meters = Number(edge.meters) || 0;
    const seconds = Number(edge.seconds) || 0;
    const end = travelled + meters;
    travelled = end;
    if (end <= passed) continue;
    const share = meters > 0 ? Math.min(1, (end - passed) / meters) : 1;
    out.push({ segment: edge.segment, meters: meters * share, seconds: seconds * share });
  }
  return out;
}

/**
 * What the path already being driven costs under the live states the
 * candidate was priced with.
 *
 * This is the number the whole decision turns on, and getting it wrong
 * is not a rounding error — it inverts the outcome. Compare a diversion
 * against what the drive *believed* and the diversion always looks
 * worse, because the jam that prompted the re-price made everything
 * slower than that belief; the navigator then keeps its driver pointed
 * into the jam, every time, which is precisely the failure it exists to
 * prevent. Compare it against the same road priced under the same
 * observations and the saving is real.
 *
 * The blend is `resolveLiveFactor`, imported rather than restated, so
 * this and the router cannot drift apart. An edge with no state keeps
 * its static cost — that is what the router does with it too. A closed
 * road makes the whole path Infinity, which is the honest cost of a way
 * that is shut.
 */
export function livePathSeconds(edges, states, { nowMillis = Date.now() } = {}) {
  const bySegment = states instanceof Map
    ? states
    : new Map((states || []).filter(Boolean).map(state => [state.segment, state]));
  let total = 0;
  for (const edge of edges || []) {
    const seconds = Number(edge?.seconds);
    if (!Number.isFinite(seconds) || seconds < 0) continue;
    const state = edge?.segment != null ? bySegment.get(edge.segment) : null;
    const resolved = state ? resolveLiveFactor(state, seconds, nowMillis) : null;
    if (!resolved) { total += seconds; continue; }
    if (!Number.isFinite(resolved.factor)) return Infinity;
    total += seconds * resolved.factor + resolved.penaltyDs / 10;
  }
  return total;
}

/**
 * What a re-price should do with the answer it just computed.
 *
 * - `remainingSeconds`: what the drive currently believes is left.
 * - `candidateSeconds`: the re-priced best route from the driver's
 *   position, under current traffic.
 * - `currentSegments` / `candidateSegments`: segment ids, from
 *   `segmentsOf`. A Set is accepted for the current side.
 *
 * `gain` is positive when the candidate is faster. Note that it is
 * measured against what the drive *believed*, which may itself be stale
 * — that is exactly why "keep" is the default and why a different path
 * that comes back slower is never taken.
 */
export function repriceDecision({
  remainingSeconds,
  candidateSeconds,
  currentLiveSeconds = null,
  candidateLiveSeconds = null,
  currentSegments = [],
  candidateSegments = [],
  policy = DEFAULT_REPRICE_POLICY
} = {}) {
  const settings = { ...DEFAULT_REPRICE_POLICY, ...policy };
  const remaining = Number(remainingSeconds);
  const candidate = Number(candidateSeconds);
  // `null` must not become 0 here — a missing live figure has to fall
  // back, not read as "this route is free".
  const currentLive = currentLiveSeconds == null ? NaN : Number(currentLiveSeconds);
  const candidateLive = candidateLiveSeconds == null ? NaN : Number(candidateLiveSeconds);
  const overlap = pathOverlap(currentSegments, candidateSegments);
  const share = overlap.share;
  // Either test can make it the same way: mostly-shared, or barely
  // leaving. The second is what keeps the tail of a drive — where a
  // handful of joining edges is a big share of very little road — from
  // reading as a detour.
  const samePath = share >= settings.samePathShare
    || (Number.isFinite(overlap.unsharedMeters) && overlap.unsharedMeters < settings.minDetourMeters);
  const blank = {
    action: "keep", gain: 0, etaShift: 0, samePath, comparedAgainst: "belief",
    sharedShare: share, unsharedMeters: overlap.unsharedMeters, threshold: Infinity
  };

  // An engine that returned nothing, or a drive that is already over,
  // has nothing to decide.
  if (!Number.isFinite(remaining) || !Number.isFinite(candidate) || candidate < 0) return blank;
  if (remaining <= 0) return blank;

  // Two different questions, and they need two different numbers — and
  // each has to be measured in one unit, not one of each.
  //
  //   gain — what switching would really save. Both halves are the two
  //     paths priced through `livePathSeconds` under the same states.
  //   etaShift — how wrong the figure on the driver's screen has become:
  //     what they are being shown, against what the road ahead now
  //     actually costs.
  //
  // Note that `route.seconds` is the *static* total of the path chosen —
  // live weights steer the search, they do not appear in the reported
  // duration. So comparing a live-priced current path against
  // `candidate.seconds` mixes two units and manufactures a saving out of
  // the difference; every jam then reads as a reason to divert. With
  // either live figure missing this falls back to the reported pair,
  // which errs the other way — it refuses real diversions — so a host
  // that can price both should.
  const currentUsable = !Number.isNaN(currentLive) && currentLive >= 0;
  const candidateUsable = Number.isFinite(candidateLive) && candidateLive >= 0;
  const bothLive = currentUsable && candidateUsable;
  const basis = bothLive ? currentLive : remaining;
  const etaShift = remaining - (candidateUsable ? candidateLive : candidate);
  // A closed road on the path being driven is Infinity, and that is the
  // strongest reason to switch there is — not a missing measurement.
  const gain = bothLive ? currentLive - candidateLive : etaShift;
  const threshold = Math.max(
    settings.minGainSeconds,
    (Number.isFinite(basis) ? basis : remaining) * settings.gainShare
  );
  const measured = {
    samePath, sharedShare: share, unsharedMeters: overlap.unsharedMeters,
    gain, etaShift, threshold, comparedAgainst: bothLive ? "live" : "belief"
  };

  if (samePath) {
    // The way ahead is unchanged; only what it costs has moved. Worth
    // re-installing so the ETA is honest — in both directions, because
    // a drive silently getting longer is the thing a driver most wants
    // to be told.
    return Math.abs(etaShift) >= settings.etaShiftSeconds
      ? { action: "refresh", ...measured }
      : { action: "keep", ...measured };
  }
  return gain >= threshold
    ? { action: "switch", ...measured }
    : { action: "keep", ...measured };
}

/**
 * The voice state a re-install may carry across, or null to start clean.
 *
 * Re-planning from the driver's current position renumbers every
 * boundary, so the index a host tracks "already announced" against is
 * meaningless afterwards. The instruction is not: "turn left onto Rue
 * Notre-Dame" is the same thing to say, and saying it twice because the
 * route object changed underneath is the tell of a navigator that
 * re-plans behind the driver's back.
 */
export function carriedVoice({ previousPhrase, upcomingPhrase, previousLevels = 0, boundary = 0 } = {}) {
  if (!previousPhrase || !upcomingPhrase) return null;
  if (previousPhrase !== upcomingPhrase) return null;
  return { boundary, levels: previousLevels, phrase: previousPhrase };
}

/**
 * Whether a corridor change is worth re-pricing for immediately.
 *
 * Only a corridor getting worse interrupts. An improvement is still
 * worth acting on — a jam that cleared may reopen a faster way — but it
 * can wait for the host's ordinary timer, where it costs nobody their
 * attention and cannot be triggered in a loop by a segment flickering
 * back and forth across one level boundary.
 */
export function shouldRepriceNow(change) {
  return Boolean(change?.worsened?.length);
}
