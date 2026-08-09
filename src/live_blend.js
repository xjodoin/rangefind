// How an observed live state becomes a multiplier over a static edge
// weight — the one place that arithmetic is written down.
//
// It used to live inside the query engine's closure, which was fine
// while the router was the only thing that priced an edge. It is not
// fine now: deciding whether a re-priced route is worth taking means
// pricing *the path already being driven* under the same observations,
// and a second implementation of this blend would drift from the first
// silently, producing a saving that exists only in the comparison.
//
// The blend itself (PulseMesh §14): confidence decays with age, and a
// low-confidence report only nudges the cost toward the observation
// rather than replacing it.

/**
 * `{ factor, penaltyDs }` for a live state over an edge of
 * `staticSeconds`, or null when the state says nothing usable.
 * `factor` is Infinity for a closed road.
 */
export function resolveLiveFactor(state, staticSeconds, nowMs) {
  if (state.closed === true) return { factor: Infinity, penaltyDs: 0 };
  let factor = Number(state.factor);
  if (!Number.isFinite(factor) && Number.isFinite(state.speedMps) && state.speedMps > 0 && staticSeconds > 0) {
    const meters = Number(state.meters);
    // Without meters we cannot turn a speed into a time; providers that
    // send speedMps should send meters too, or precompute `factor`.
    factor = Number.isFinite(meters) && meters > 0
      ? (meters / Math.max(1.4, state.speedMps)) / staticSeconds
      : NaN;
  }
  if (!Number.isFinite(factor) || factor <= 0) return null;
  let confidence = Number.isFinite(state.confidence) ? Math.max(0, Math.min(1, state.confidence)) : 1;
  if (Number.isFinite(state.observedAt)) {
    const ageSeconds = Math.max(0, (nowMs - state.observedAt) / 1000);
    confidence *= Math.exp(-ageSeconds / 60);
  }
  const blended = confidence * factor + (1 - confidence);
  const clamped = Math.max(0.25, Math.min(10, blended));
  const penaltyDs = Number.isFinite(state.penaltySeconds) && state.penaltySeconds > 0
    ? Math.round(Math.min(600, state.penaltySeconds) * 10)
    : 0;
  return { factor: clamped, penaltyDs };
}
