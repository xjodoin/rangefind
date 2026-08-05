// Arrival time is a local route query (threads §9).
//
// This is the integration the whole channel exists for. A thread carries
// position only; the arrival time is computed on the subscriber's device
// by routing from that position to their stop under the *traffic*
// channel's live metric. The publisher broadcasts one position and never
// learns which stop anyone cares about — unlike every server-side ETA,
// which necessarily knows each recipient's address.

import { THREAD_STATE } from "./thread_codec.js";
import { THREAD_CONSTANTS } from "./thread_publish.js";

/**
 * Estimates arrival at `myStopIndex` from the newest thread update.
 *
 * - engine: the route graph (needs `locate` and `matrix`).
 * - update: a validated PMTP body from the subscriber.
 * - plan: `{ stops: [{ index, lat, lon }], dwellSeconds }`.
 * - live: the traffic channel's provider, or null for the static metric.
 *
 * Returns null when the run cannot reach the stop (completed, canceled,
 * or already past it), so a caller never renders an arrival for a bus
 * that has gone.
 */
export async function estimateArrival({
  engine,
  update,
  plan,
  myStopIndex,
  live = null,
  constants = THREAD_CONSTANTS,
  nowMillis = Date.now()
}) {
  if (!update || !plan?.stops?.length) return null;
  if (update.state === THREAD_STATE.COMPLETED || update.state === THREAD_STATE.CANCELED) return null;

  const remaining = plan.stops
    .filter(stop => stop.index > update.stopIndex && stop.index <= myStopIndex)
    .sort((a, b) => a.index - b.index);
  if (!remaining.length) return null; // already served, or not on this run

  // Where the vehicle is. Fine mode gives a snapped position; coarse mode
  // withholds it, so fall back to the last stop it reported serving —
  // which is exactly the accuracy trade §11 describes.
  let origin = null;
  let originIsExact = false;
  // `segment` is already null when the position was withheld, and unlike
  // `leafCell > 0` it does not mistake the real leaf 0 for a sentinel.
  if (update.segment) {
    origin = await engine.locate(update.segment, update.ratio);
    originIsExact = true;
  } else {
    const lastServed = plan.stops.find(stop => stop.index === update.stopIndex);
    origin = lastServed ? { lat: lastServed.lat, lon: lastServed.lon } : null;
  }
  if (!origin) return null;

  // §9: matrix(), never itinerary(). itinerary() *reorders* stops — it
  // solves a travelling salesman problem, which is exactly wrong for a
  // run whose sequence is fixed by the plan, and would silently produce
  // optimistic ETAs by shortcutting the route the bus will actually drive.
  const points = [origin, ...remaining.map(stop => ({ lat: stop.lat, lon: stop.lon }))];
  const result = await engine.matrix({ points, live, geometry: false });
  const dwellSeconds = plan.dwellSeconds ?? 0;

  let seconds = 0;
  for (let i = 0; i + 1 < points.length; i++) {
    const leg = result.seconds[i][i + 1];
    if (!Number.isFinite(leg)) return null;
    seconds += leg;
    // Dwell at every intermediate stop, but not at the subscriber's own:
    // the bus arriving is the event, not it pulling away again.
    if (i + 2 < points.length) seconds += dwellSeconds;
  }

  const observedAtMillis = update.unixSeconds * 1000;
  const ageSeconds = Math.max(0, Math.floor((nowMillis - observedAtMillis) / 1000));
  return {
    arrivalMillis: observedAtMillis + seconds * 1000,
    secondsFromObservation: seconds,
    secondsFromNow: Math.max(0, seconds - ageSeconds),
    stopsAway: remaining.length,
    // Honest provenance for the UI, mirroring §12's four rows.
    basis: live ? "live-traffic" : "static-metric",
    positionBasis: originIsExact ? "reported-position" : "last-stop",
    observationAgeSeconds: ageSeconds,
    stale: ageSeconds > constants.THREAD_STALE
  };
}

/**
 * A plan-only prediction, for §12's lower rows: no thread at all, so the
 * answer is the published timetable, marked as scheduled.
 */
export function scheduledArrival({ plan, myStopIndex }) {
  const stop = plan?.stops?.find(candidate => candidate.index === myStopIndex);
  if (!stop || stop.plannedUnixSeconds == null) return null;
  return {
    arrivalMillis: stop.plannedUnixSeconds * 1000,
    basis: "schedule",
    positionBasis: "none",
    stale: false
  };
}
