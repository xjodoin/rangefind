// PMF1 forwarding (protocol §4.5): accept, validate, hold, republish as
// our own. Forwarding is cheap and safe precisely because records carry
// no sender identity — there is nothing to strip or substitute, and an
// altered byte only destroys the record's proof. What forwarding changes
// is which peer id the mesh observes publishing.

import { DEFAULT_CONSTANTS } from "./bins.js";
import { MAGIC, decodeAny } from "./codec.js";

/**
 * - validator: createValidator() output — the inner record is validated
 *   exactly as a gossip arrival (a forwarder that skips this becomes an
 *   amplifier).
 * - publishAsOwn(payload, meta): republish callback; meta.records carries
 *   the decoded inner records so the caller can mark them viaForward in
 *   its own store (§8.5 hint-weighting).
 * - schedule(fn, delayMs): timer injection (virtual time in simulation).
 */
export function createForwardHandler({
  validator,
  publishAsOwn,
  store = null,
  constants = DEFAULT_CONSTANTS,
  schedule = (fn, delayMs) => setTimeout(fn, delayMs),
  clock = Date.now
} = {}) {
  const sourceRates = new Map(); // peerId -> [acceptMillis...]
  const stats = { accepted: 0, dropped: 0, republished: 0 };

  function takeRate(peerId, nowMillis) {
    let times = sourceRates.get(peerId);
    if (!times) { times = []; sourceRates.set(peerId, times); }
    while (times.length && nowMillis - times[0] > 60000) times.shift();
    if (times.length >= constants.FORWARD_RATE) return false;
    times.push(nowMillis);
    return true;
  }

  /**
   * Handles one PMF1 stream message. There is no response. Returns a
   * report of what happened, for tests and stats.
   */
  function handle(message, { fromPeer, nowMillis = clock() } = {}) {
    if (message.delayMs > constants.FORWARD_MAX_DELAY * 1000) return drop("delay-too-long");
    if (!takeRate(fromPeer, nowMillis)) return drop("rate");
    let inner;
    try {
      inner = decodeAny(message.payload);
    } catch (error) {
      return drop(`malformed: ${error.message}`);
    }
    // One hop only, no chains; only measurement batches and incidents.
    if (inner.magic === MAGIC.PMF1) return drop("chained-forward");
    let records;
    // Validation passes `store` so rule 6 applies here too: without it a
    // forwarder happily re-publishes a replay, and re-publishing is the
    // one thing a forwarder must not be usable for.
    if (inner.magic === MAGIC.PMB1) {
      records = inner.records;
      for (const record of records) {
        const verdict = validator.validateContribution(record, { store, fromPeer, nowMillis });
        if (!verdict.ok) return drop(`invalid-inner:rule${verdict.rule}`);
      }
    } else if (inner.magic === MAGIC.PMI1) {
      const verdict = validator.validateIncident(inner, { store, fromPeer, nowMillis });
      if (!verdict.ok) return drop(`invalid-inner:rule${verdict.rule}`);
      records = [inner];
    } else {
      return drop(`unforwardable:${inner.magic}`);
    }
    stats.accepted++;
    schedule(() => {
      stats.republished++;
      // Bytes republished verbatim, as our own publication.
      publishAsOwn(message.payload, { records, viaForward: true });
    }, message.delayMs);
    return { forwarded: true };
  }

  function drop(reason) {
    stats.dropped++;
    return { forwarded: false, reason };
  }

  return { handle, stats };
}
