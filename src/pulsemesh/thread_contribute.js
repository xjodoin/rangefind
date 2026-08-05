// Rules between the two channels (threads §10).
//
// A fleet on the road all day is the best sustained probe the traffic
// channel could ask for, so the goal is to let thread publishers
// contribute *safely* rather than to forbid it. Three rules do the work:
//
//   1. A thread record never enters a traffic aggregate. Absolute. A
//      signed single-source record inside a corroborated multi-source
//      aggregate would turn one fleet key into a traffic authority — the
//      exact property the traffic channel is built not to have. A fleet
//      contributes by emitting ordinary PMC1s, with the same proof of
//      work, corroboration minimums, and anonymity as anyone else.
//   3. Contribution is gated on whether the route is already public. The
//      test is not vehicle type: would an observer learn anything from
//      correlating a thread with its contributions that a published
//      document does not already tell them?
//   4. A contributing publisher suppresses at its own stops. This is a
//      data-quality rule and it is easy to miss — a bus dwelling at every
//      stop reports 0 km/h on roads that are flowing perfectly, and
//      several buses on one corridor corroborate each other into a
//      convincing, entirely false standstill.

import { THREAD_CONSTANTS } from "./thread_publish.js";

/** §10 rule 3: what kind of route this thread is running. */
export const ROUTE_PUBLICITY = Object.freeze({
  /** Timetabled and published as a static asset: transit, school runs,
   *  snow clearing, waste collection. Correlation reveals a route anyone
   *  could already read off the timetable. */
  PUBLISHED: "published",
  /** Couriers, field service, anything ad hoc. The route is not merely
   *  private — it is a list of customer addresses. */
  UNPUBLISHED: "unpublished"
});

/**
 * Decides whether a thread publisher may contribute to the traffic
 * channel, and under which profile.
 *
 * Contribution MUST be an explicit per-thread setting, MUST default to
 * off, and MUST NOT be inferred from vehicle class — so this takes the
 * operator's explicit choice and refuses to guess.
 */
export function resolveContributionPolicy({ enabled = false, publicity = null } = {}) {
  if (!enabled) {
    return { contribute: false, profile: null, reason: "off by default; contribution is an explicit per-thread choice" };
  }
  if (publicity !== ROUTE_PUBLICITY.PUBLISHED && publicity !== ROUTE_PUBLICITY.UNPUBLISHED) {
    // Refusing to guess is the whole point of rule 3's last paragraph.
    return { contribute: false, profile: null, reason: "route publicity must be stated, never inferred" };
  }
  if (publicity === ROUTE_PUBLICITY.PUBLISHED) {
    return {
      contribute: true,
      profile: "cadence",
      reason: "a published timetable already reveals this route, so there is no anonymity to lose"
    };
  }
  return {
    contribute: true,
    profile: "reticent",
    reason: "an unpublished route is a customer list; the reticent profile removes the trajectory rather than the contributor"
  };
}

/**
 * §10 rule 4 in isolation, so a contributor can be gated without a
 * publisher object: suppress while dwelling, within STOP_RADIUS of a
 * planned stop, and for STOP_LINGER after departing one.
 *
 * A vehicle stopped for *traffic* between stops reports normally — that
 * is real congestion and precisely what the channel wants.
 */
export function createStopSuppressor({ stops = [], constants = THREAD_CONSTANTS, clock = Date.now } = {}) {
  let departedAt = -Infinity;
  let wasAtStop = false;
  const stats = { evaluated: 0, suppressed: 0, byReason: {} };

  function metersBetween(a, b) {
    const toRad = Math.PI / 180;
    const dLat = (b.lat - a.lat) * toRad;
    const dLon = (b.lon - a.lon) * toRad;
    const lat = (a.lat + b.lat) / 2 * toRad;
    const x = dLon * Math.cos(lat);
    return Math.sqrt(dLat * dLat + x * x) * 6371008.8;
  }

  function suppress(reason) {
    stats.suppressed++;
    stats.byReason[reason] = (stats.byReason[reason] || 0) + 1;
    return { contribute: false, reason };
  }

  /**
   * @returns {{contribute: boolean, reason?: string}}
   */
  function evaluate({ lat, lon, speedMps = 0, dwelling = false, nowMillis = clock() }) {
    stats.evaluated++;
    const point = Number.isFinite(lat) && Number.isFinite(lon) ? { lat, lon } : null;
    const nearStop = point
      ? stops.some(stop => metersBetween(point, stop) <= constants.THREAD_STOP_RADIUS)
      : false;

    if (nearStop && !wasAtStop) wasAtStop = true;
    if (!nearStop && wasAtStop) {
      wasAtStop = false;
      departedAt = nowMillis;
    }

    if (dwelling) return suppress("dwelling");
    if (nearStop) return suppress("at-stop");
    if (nowMillis - departedAt < constants.THREAD_STOP_LINGER * 1000) return suppress("just-departed");
    // A courier stopped to hand over a package on a free-flowing street
    // is the same failure without a plan to detect it, so a standstill
    // with no recent movement is treated as a service stop too.
    if (speedMps < 0.5 && !stops.length) return suppress("stationary-no-plan");
    return { contribute: true };
  }

  return { evaluate, stats };
}

/**
 * §10 rule 1, enforced rather than documented: a thread update carries no
 * shape a traffic store will accept. This exists so the boundary is a
 * function someone has to deliberately defeat, not a paragraph.
 */
export function assertNeverBridged(threadUpdate) {
  if (!threadUpdate) return;
  const looksLikeContribution = threadUpdate
    && typeof threadUpdate === "object"
    && "qualityBin" in threadUpdate
    && "reportId" in threadUpdate;
  if (looksLikeContribution) {
    throw new Error(
      "A thread update must never be shaped into a traffic contribution: " +
      "a signed single-source record inside a corroborated aggregate turns one fleet key into a traffic authority."
    );
  }
}
