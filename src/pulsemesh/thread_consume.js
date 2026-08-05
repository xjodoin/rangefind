// Thread subscriber (threads §7, §12): validation in order, a sequence
// ledger, and the staleness rule.
//
// The validation order matters for cost as much as correctness. An
// unknown tag is dropped at step 3 — a cheap byte comparison — which is
// the entire cost of a hostile flood on the gossip path, because an
// attacker who cannot derive the tag cannot make a subscriber do any
// crypto at all.

import { THREAD_CONSTANTS } from "./thread_publish.js";
import { THREAD_STATE, decodeThreadBody, decodeThreadRecord } from "./thread_codec.js";
import {
  deriveThreadKeys,
  openThreadBody,
  threadTagsForWindows,
  threadTopic,
  threadRendezvous,
  threadWindow,
  verifyThread
} from "./thread_crypto.js";
import { toHex } from "./sha256.js";

const CLASS_CAP_KMH = 140; // §7 step 9 upper bound, absent leaf context.

function fail(step, reason) {
  return { ok: false, step, reason };
}

/**
 * Creates a subscriber from a decoded link.
 *
 * - link: { publicKey, epochPrefix8, notAfter } from decodeThreadLink.
 * - epoch32: the full epoch the subscriber is running.
 * - cellContext(leafCell): optional, enables the §7 step 9 segment check.
 */
export async function createThreadSubscriber({
  link,
  epoch32,
  constants = THREAD_CONSTANTS,
  clock = Date.now,
  cellContext = null
} = {}) {
  const keys = await deriveThreadKeys(link.publicKey);
  const epochPrefix8 = epoch32.subarray(0, 8);
  for (let i = 0; i < 8; i++) {
    if (epochPrefix8[i] !== link.epochPrefix8[i]) {
      throw new Error("This link is bound to a different graph epoch.");
    }
  }
  const epochPrefix16hex = toHex(epoch32).slice(0, 16);

  let highestSeq = 0;
  let latest = null;
  const history = [];
  const stats = { accepted: 0, dropped: 0, dropsByStep: {}, forgeries: 0 };

  function drop(step, reason) {
    stats.dropped++;
    const key = `step${step}`;
    stats.dropsByStep[key] = (stats.dropsByStep[key] || 0) + 1;
    return fail(step, reason);
  }

  /** Tags for the current window and its neighbours (rotation overlap). */
  async function currentTags(nowMillis = clock()) {
    const window = threadWindow(nowMillis);
    return threadTagsForWindows(keys, epoch32, [window - 1, window, window + 1]);
  }

  async function topics(nowMillis = clock()) {
    const tags = await currentTags(nowMillis);
    return tags.map(tag => threadTopic(epochPrefix16hex, tag));
  }

  async function rendezvousKeys(nowMillis = clock()) {
    return (await topics(nowMillis)).map(topic => threadRendezvous(topic));
  }

  /**
   * §7, in order. Returns { ok: true, update } or a numbered failure.
   * `knownTags` may be supplied to avoid re-deriving them per record.
   */
  async function accept(payload, { nowMillis = clock(), knownTags = null } = {}) {
    // 1. Frame, magic, size, no trailing bytes.
    let record;
    try {
      record = payload instanceof Uint8Array ? decodeThreadRecord(payload) : payload;
    } catch (error) {
      return drop(1, error.message);
    }
    if (record.bytes.length > constants.THREAD_MAX_RECORD_BYTES) return drop(1, "record too large");

    // 2. Epoch.
    for (let i = 0; i < 8; i++) {
      if (record.epochPrefix8[i] !== epochPrefix8[i]) return drop(2, "unknown epoch");
    }

    // 3. Tag. Cheap, and the whole cost of a flood from someone without
    // the capability: they cannot compute an address we listen on.
    const tags = knownTags || (await currentTags(nowMillis));
    const tagHex = toHex(record.tag);
    if (!tags.some(tag => toHex(tag) === tagHex)) return drop(3, "unknown tag");

    // 4. AEAD.
    const opened = await openThreadBody(keys, record.seq, record.aad, record.ciphertext);
    if (!opened) return drop(4, "AEAD failed");

    // 5. Inner body well-formed.
    let body;
    try {
      body = decodeThreadBody(opened);
    } catch (error) {
      return drop(5, error.message);
    }

    // 6. Signature. A record that decrypts but does not verify means a
    // link holder tried to publish — an attack, not an error.
    if (!(await verifyThread(body.preimage, body.signature, link.publicKey))) {
      stats.forgeries++;
      return drop(6, "signature does not verify (a link holder tried to publish)");
    }

    // 7. Sequence: strictly increasing — replay and rollback protection.
    if (record.seq <= highestSeq) return drop(7, "seq not strictly increasing");

    // 8. Time window, and never past the link's expiry.
    const nowSeconds = Math.floor(nowMillis / 1000);
    if (body.unixSeconds < nowSeconds - constants.THREAD_MAX_AGE) return drop(8, "update too old");
    if (body.unixSeconds > nowSeconds + constants.THREAD_MAX_FUTURE_SKEW) return drop(8, "update from the future");
    if (body.unixSeconds > link.notAfter) return drop(8, "past the link's notAfter");

    // 9. Plausibility: implied speed between accepted updates, and the
    // segment existing in its leaf when we hold the cell.
    if (body.segment && cellContext) {
      const context = cellContext(body.leafCell);
      if (context && (body.geomRef >>> 1) >= context.polylineCount) {
        return drop(9, "segment does not exist in leaf");
      }
    }
    if (latest && body.segment && latest.segment && body.segment !== latest.segment) {
      const seconds = body.unixSeconds - latest.unixSeconds;
      if (seconds > 0 && typeof constants.impliedMetersBetween === "function") {
        const meters = constants.impliedMetersBetween(latest, body);
        if (meters != null && (meters / seconds) * 3.6 > CLASS_CAP_KMH * 1.15) {
          return drop(9, "implied speed implausible");
        }
      }
    }

    highestSeq = record.seq;
    const update = { ...body, seq: record.seq, receivedAt: nowMillis };
    latest = update;
    history.push(update);
    if (history.length > constants.THREAD_CACHE_RING) history.shift();
    stats.accepted++;
    return { ok: true, update };
  }

  /**
   * §12 degradation. `live` is false past THREAD_STALE — the UI must
   * stop presenting a stale position as live, because "bus expected
   * 07:44" and "bus is here, arriving 07:44" are different claims.
   */
  function status({ nowMillis = clock(), hasTraffic = false } = {}) {
    const ageSeconds = latest ? Math.floor(nowMillis / 1000) - latest.unixSeconds : Infinity;
    const live = latest != null && ageSeconds <= constants.THREAD_STALE;
    const hasPosition = live && latest.segment != null;
    let row;
    if (live && hasTraffic) row = "thread+traffic";
    else if (live) row = "thread-only";
    else if (hasTraffic) row = "traffic-only";
    else row = "neither";
    return {
      row,
      live,
      hasPosition,
      ageSeconds: Number.isFinite(ageSeconds) ? ageSeconds : null,
      state: latest?.state ?? THREAD_STATE.SCHEDULED,
      stopIndex: latest?.stopIndex ?? 0,
      // The claim the UI is allowed to make, spelled out so it cannot be
      // accidentally upgraded.
      claim: row === "thread+traffic" ? "live position, live-traffic ETA"
        : row === "thread-only" ? "live position, static-metric ETA"
          : row === "traffic-only" ? "scheduled prediction, position unknown"
            : "scheduled prediction only"
    };
  }

  function expired(nowMillis = clock()) {
    return Math.floor(nowMillis / 1000) > link.notAfter;
  }

  return {
    keys,
    accept,
    currentTags,
    topics,
    rendezvousKeys,
    status,
    expired,
    stats,
    latest: () => latest,
    history: () => [...history],
    get highestSeq() { return highestSeq; }
  };
}
