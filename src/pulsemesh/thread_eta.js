// Arrival time is a local route query (threads §9).
//
// This is the integration the whole channel exists for. A thread carries
// position only; the arrival time is computed on the subscriber's device
// by routing from that position to their stop under the *traffic*
// channel's live metric. The publisher broadcasts one position and never
// learns which stop anyone cares about — unlike every server-side ETA,
// which necessarily knows each recipient's address.

import { STOP_OUTCOME, THREAD_STATE, THREAD_TRAVEL_MODE } from "./thread_codec.js";
import { THREAD_CONSTANTS } from "./thread_publish.js";

const TRAVEL_MODE_NAMES = Object.freeze({
  [THREAD_TRAVEL_MODE.CAR]: "car",
  [THREAD_TRAVEL_MODE.BIKE]: "bike",
  [THREAD_TRAVEL_MODE.FOOT]: "foot"
});

const OUTCOME_NAMES = Object.freeze({
  [STOP_OUTCOME.PENDING]: "pending",
  [STOP_OUTCOME.DELIVERED]: "delivered",
  [STOP_OUTCOME.SKIPPED]: "skipped",
  [STOP_OUTCOME.FAILED]: "failed"
});

/** The name of a wire travel mode, or null when the run did not say. */
export function travelModeName(travelMode) {
  return TRAVEL_MODE_NAMES[travelMode] ?? null;
}

/**
 * Which graph to route on, and what the answer is allowed to claim.
 *
 * Two designs were possible here and this is both: a host that holds
 * several profiles passes `engines`, a host with one graph passes
 * `engine` and names it with `engineMode`. The invariant either way is
 * that the result states the profile it actually used — a follower told
 * "12 min" for a bike courier, computed on a car graph, has been
 * misinformed, and a host with only a car graph is not wrong to answer,
 * only wrong to answer silently.
 */
function resolveProfile({ engine, engines, engineMode, travelMode }) {
  const wanted = travelModeName(travelMode);
  if (engines) {
    if (wanted && engines[wanted]) {
      return { engine: engines[wanted], profile: wanted, wanted };
    }
    const fallbackName = (engineMode && engines[engineMode] ? engineMode : null)
      ?? (engines.car ? "car" : Object.keys(engines).find(name => engines[name]) ?? null);
    if (fallbackName) return { engine: engines[fallbackName], profile: fallbackName, wanted };
  }
  return { engine, profile: engineMode ?? null, wanted };
}

function profileFacts({ profile, wanted }) {
  // "unstated" is the honest answer when the host never said what its
  // one graph is: we cannot claim a match and must not claim a mismatch.
  const profileBasis = !wanted || !profile
    ? "unstated"
    : (profile === wanted ? "matched" : "mismatched");
  return {
    profile,
    profileBasis,
    travelModeName: wanted,
    travelModeMismatch: profileBasis === "mismatched"
  };
}

/**
 * Estimates arrival at `myStopIndex` from the newest thread update.
 *
 * - engine: the route graph (needs `locate` and `matrix`).
 * - engines: optionally `{ car, bike, foot }`, when the host holds more
 *   than one profile. The run's `travelMode` picks one.
 * - engineMode: what `engine` is, when there is only one. Without it the
 *   result says `profileBasis: "unstated"` rather than guessing.
 * - update: a validated PMTP body from the subscriber.
 * - plan: `{ stops: [{ index, lat, lon }], dwellSeconds }`.
 * - live: the traffic channel's provider, or null for the static metric.
 *
 * Returns null when the run cannot reach the stop (completed, canceled,
 * already dealt with at or before `update.stopIndex`, or already
 * delivered there — wherever it sits in the plan), so a caller never
 * renders an arrival for a bus that has gone. A stop the driver marked
 * **skipped or failed** returns a result with `arrivalMillis: null` and
 * `basis: "marked"` instead: it is not an arrival, and there must be no
 * shape a caller can accidentally render a time out of.
 */
export async function estimateArrival({
  engine,
  engines = null,
  engineMode = null,
  update,
  plan,
  myStopIndex,
  live = null,
  constants = THREAD_CONSTANTS,
  nowMillis = Date.now()
}) {
  if (!update || !plan?.stops?.length) return null;
  if (update.state === THREAD_STATE.COMPLETED || update.state === THREAD_STATE.CANCELED) return null;

  const outcomes = update.outcomes || [];
  const outcomeOf = index => outcomes[index - 1] ?? STOP_OUTCOME.PENDING;
  const observedAtMillis = update.unixSeconds * 1000;
  const ageSeconds = Math.max(0, Math.floor((nowMillis - observedAtMillis) / 1000));
  const resolved = resolveProfile({ engine, engines, engineMode, travelMode: update.travelMode });
  const facts = profileFacts(resolved);

  // The subscriber's own stop, first. A stop the driver explicitly
  // skipped or failed is not going to be visited, and answering it with
  // any number at all — even a stale one — is the wrong answer.
  const mine = outcomeOf(myStopIndex);
  if (mine === STOP_OUTCOME.DELIVERED) return null; // already served
  if (mine === STOP_OUTCOME.SKIPPED || mine === STOP_OUTCOME.FAILED) {
    return {
      arrivalMillis: null,
      secondsFromObservation: null,
      secondsFromNow: null,
      stopsAway: null,
      outcome: mine,
      outcomeName: OUTCOME_NAMES[mine],
      // The wire carries one reason, for the most recent mark. When that
      // mark was not this stop the reason is genuinely unknown here
      // rather than inferrable, and null says so.
      reasonCode: update.lastOutcome?.stopIndex === myStopIndex
        ? update.lastOutcome.reasonCode
        : null,
      basis: "marked",
      positionBasis: "none",
      observationAgeSeconds: ageSeconds,
      stale: false,
      ...facts
    };
  }

  const remaining = plan.stops
    // `stopIndex` is the last stop the run has dealt with in plan order,
    // never a high-water mark of the paperwork: a stop the dispatcher
    // pre-marked further down the plan does not put this subscriber
    // behind the vehicle, so the position test is the only thing the
    // index decides.
    .filter(stop => stop.index > update.stopIndex && stop.index <= myStopIndex)
    // A stop the vehicle is not going to visit — or has already been
    // resolved at, wherever it sits in the plan — must leave the chain.
    // Left in, one skipped delivery inflates every downstream customer's
    // ETA by a leg and a dwell, for the rest of the day.
    .filter(stop => outcomeOf(stop.index) === STOP_OUTCOME.PENDING)
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
    origin = await resolved.engine.locate(update.segment, update.ratio);
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
  const result = await resolved.engine.matrix({ points, live, geometry: false });
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

  return {
    arrivalMillis: observedAtMillis + seconds * 1000,
    secondsFromObservation: seconds,
    secondsFromNow: Math.max(0, seconds - ageSeconds),
    stopsAway: remaining.length,
    outcome: STOP_OUTCOME.PENDING,
    outcomeName: OUTCOME_NAMES[STOP_OUTCOME.PENDING],
    reasonCode: null,
    // Honest provenance for the UI, mirroring §12's four rows.
    basis: live ? "live-traffic" : "static-metric",
    positionBasis: originIsExact ? "reported-position" : "last-stop",
    observationAgeSeconds: ageSeconds,
    stale: ageSeconds > constants.THREAD_STALE,
    ...facts
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
