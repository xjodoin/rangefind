// Thread subscriber (threads §7, §12): validation in order, a sequence
// ledger, and the staleness rule.
//
// The validation order matters for cost as much as correctness. An
// unknown tag is dropped at step 3 — a cheap byte comparison — which is
// the entire cost of a hostile flood on the gossip path, because an
// attacker who cannot derive the tag cannot make a subscriber do any
// crypto at all.

import { THREAD_CONSTANTS } from "./thread_publish.js";
import {
  LINK_VERSION_DELEGATED,
  THREAD_MAGIC,
  THREAD_STATE,
  decodeDayCertificate,
  decodeThreadBody,
  decodeThreadRecord,
  threadBodyMagic
} from "./thread_codec.js";
import {
  deriveThreadKeys,
  openThreadBody,
  threadTagsForWindows,
  threadTopic,
  threadRendezvous,
  threadWindow,
  verifyThread
} from "./thread_crypto.js";
import {
  THREAD_CERT_SKEW,
  THREAD_MAX_CERT_SECONDS,
  verifyDayCertificate
} from "./thread_route.js";
import { toHex } from "./sha256.js";

const CLASS_CAP_KMH = 140; // §7 step 9 upper bound, absent leaf context.

/**
 * How many day certificates a subscriber keeps at once (§21). A route
 * has one live day key and, across a service-day boundary, briefly two;
 * four is that with room for a depot that pre-publishes tomorrow's. It
 * is a bound rather than a Map because only the root can mint these and
 * the root already owns the route — but "the attacker owns the route" is
 * not a licence to let it own the subscriber's heap as well.
 */
const MAX_CERTIFICATES = 4;

/** Stable codes for the failures a host branches on, alongside the prose. */
export const THREAD_DROP = Object.freeze({
  BAD_SIGNATURE: "bad-signature",
  AWAITING_CERTIFICATE: "awaiting-certificate",
  CERT_ON_V1_LINK: "cert-on-self-signed-link",
  CERT_AFTER_LINK_EXPIRY: "cert-after-link-expiry"
});

function fail(step, reason, code = null) {
  return { ok: false, step, reason, code };
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

  /**
   * §21. Whether this artifact says records are signed by a delegated
   * day key. Read off the link's version byte — never inferred from
   * whether a certificate happens to turn up, because "I saw a
   * certificate, so I will now accept delegated signatures" is a
   * downgrade an attacker performs by sending one.
   */
  const delegated = link.version === LINK_VERSION_DELEGATED;
  /**
   * A host may tighten the certificate lifetime bound, never loosen it.
   * The 48 h cap is the reason the split is worth anything: without it a
   * root mints one day key valid until 2038 and a lost driver phone is
   * back to costing the whole term.
   */
  const maxCertSeconds = Math.min(
    THREAD_MAX_CERT_SECONDS,
    Number.isFinite(constants.THREAD_MAX_CERT_SECONDS) ? constants.THREAD_MAX_CERT_SECONDS : Infinity
  );
  const certSkew = Number.isFinite(constants.THREAD_CERT_SKEW) ? constants.THREAD_CERT_SKEW : THREAD_CERT_SKEW;

  let highestSeq = 0;
  let latest = null;
  const history = [];
  /** Live day certificates, by day public key hex. */
  const certificates = new Map();
  /**
   * §21. Records that opened and decoded but that no certificate we hold
   * yet verifies. A joiner who has not heard the day's certificate is
   * not a forgery, and dropping its records would mean the first thing a
   * parent sees is nothing.
   *
   * Bounded twice over. By **count** — `THREAD_HELD_RECORDS`, 32,
   * oldest-first: at the fine 5 s cadence that is 160 s of records, more
   * than the 60 s `THREAD_CERT_INTERVAL` a joiner can possibly wait, and
   * 32 × 256 B is 8 KiB, so the buffer cannot become an amplifier. And
   * by **age** on release — anything already past `THREAD_MAX_AGE` is
   * dropped rather than replayed, because a position that would fail
   * step 8 on arrival must not pass it by having been held.
   */
  const heldRecords = [];
  const stats = {
    accepted: 0,
    dropped: 0,
    dropsByStep: {},
    forgeries: 0,
    certificates: 0,
    held: 0,
    heldEvicted: 0,
    heldReleased: 0
  };

  function drop(step, reason, code = null) {
    stats.dropped++;
    const key = `step${step}`;
    stats.dropsByStep[key] = (stats.dropsByStep[key] || 0) + 1;
    return fail(step, reason, code);
  }

  /** Drops certificates whose window has closed. Cheap; runs per record. */
  function pruneCertificates(nowSeconds) {
    for (const [key, cert] of certificates) {
      if (nowSeconds - certSkew > cert.notAfter) certificates.delete(key);
    }
  }

  /**
   * §7 step 6 under a v2 link: which certified day key signed this?
   *
   * Every live certificate is tried. There are at most a handful, and a
   * record does not name its signer — deliberately, since a 256-byte
   * record has no room for 32 bytes of key it would only be repeating.
   */
  async function signerFor(body) {
    for (const cert of certificates.values()) {
      if (await verifyThread(body.preimage, body.signature, cert.dayPublicKey)) return cert;
    }
    return null;
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

    // 5. Inner body well-formed. One topic now carries two body shapes
    // (§21), so the magic decides the decoder before either runs.
    const bodyMagic = threadBodyMagic(opened);
    if (bodyMagic === THREAD_MAGIC.PMTC) return acceptCertificate(opened, nowMillis);
    let body;
    try {
      body = decodeThreadBody(opened);
    } catch (error) {
      return drop(5, error.message);
    }
    return finishRecord(record, body, nowMillis, { mayHold: true });
  }

  /**
   * §7 step 7's ledger, scoped to **whoever signed**.
   *
   * Under v1 there is one signer for the life of the run and this is the
   * counter that has always been here. Under v2 each certified day key
   * gets its own, and that is not a convenience — it is forced. A run
   * ends at 16:40 and the next one starts at 07:00 the following
   * morning from a different device with a different day key; nothing
   * has been published in between, so §5.5 catch-up has nothing to
   * resume from (the ring holds ten minutes) and the new publisher
   * cannot possibly know what number yesterday's device reached. A
   * global counter would mean every parent whose phone stayed awake
   * overnight rejects the whole of day 40 for `seq` regression.
   *
   * Nothing is lost by scoping it. Rollback and replay *within* an
   * authority are what step 7 defends, and that is unchanged. Replaying
   * yesterday's records under today's ledger fails anyway, twice over:
   * their signature needs yesterday's certificate, which is outside its
   * ≤48 h window, and their `unixSeconds` is a day past
   * `THREAD_MAX_AGE`. The counter was never the cross-day defence.
   *
   * The per-signer counter lives on the certificate object, so it is
   * bounded and pruned by exactly the same rule the certificates are.
   */
  function ledgerFor(signer) {
    return signer ? (signer.highestSeq || 0) : highestSeq;
  }

  /**
   * §21 step 5a — a day certificate arriving on the run's own topic.
   *
   * Certificates do **not** touch the `seq` ledger. Step 7 orders
   * *positions*, and a certificate is not one; letting it advance
   * `highestSeq` would mean a held record — received before the
   * certificate that authorises it, which is the entire situation the
   * buffer exists for — could never be released without failing step 7.
   * Replay is a non-issue here for the same reason it is elsewhere: the
   * `seq` is inside the AEAD's AAD and the nonce, so only the holder of
   * `K_content` can produce one at all, and a verbatim duplicate of a
   * certificate changes nothing.
   */
  async function acceptCertificate(opened, nowMillis) {
    if (!delegated) {
      // A v1 link says the key in the link signs its own records. A
      // certificate on that thread is the publisher and the artifact
      // disagreeing about what the run is, and the artifact wins.
      return drop(5, "this link is self-signed and carries no delegated authority", THREAD_DROP.CERT_ON_V1_LINK);
    }
    let cert;
    try {
      cert = decodeDayCertificate(opened);
    } catch (error) {
      return drop(5, error.message);
    }
    const nowSeconds = Math.floor(nowMillis / 1000);
    const verdict = await verifyDayCertificate(cert, {
      rootPublicKey: link.publicKey,
      nowSeconds,
      maxSeconds: maxCertSeconds,
      skew: certSkew
    });
    if (!verdict.ok) return drop(6, verdict.reason, verdict.code);
    // The link's own expiry still bounds everything. A certificate that
    // only starts after the term is over authorises nothing.
    if (cert.notBefore > link.notAfter) {
      return drop(8, "the certificate begins after the link expires", THREAD_DROP.CERT_AFTER_LINK_EXPIRY);
    }

    pruneCertificates(nowSeconds);
    const key = toHex(cert.dayPublicKey);
    const known = certificates.has(key);
    certificates.set(key, cert);
    while (certificates.size > MAX_CERTIFICATES) {
      certificates.delete(certificates.keys().next().value);
    }
    if (!known) stats.certificates++;

    // Whatever was waiting on authority now gets it.
    const released = await releaseHeld(nowMillis);
    return { ok: true, certificate: cert, released };
  }

  /** §7 steps 6–9, shared by a freshly arrived record and a released one. */
  async function finishRecord(record, body, nowMillis, { mayHold }) {
    // 6. Signature. A record that decrypts but does not verify means a
    // link holder tried to publish — an attack, not an error.
    let signer = null;
    if (delegated) {
      signer = await signerFor(body);
      if (!signer) {
        // Under delegation these two are genuinely indistinguishable: a
        // record from today's driver that arrived before today's
        // certificate looks exactly like a forgery by a link holder,
        // because neither verifies under anything we hold. So it is
        // never called a forgery and never accepted — it waits, and is
        // evicted if the certificate that would vindicate it never
        // comes. A joiner is not an attacker; an attacker is not
        // admitted either.
        if (mayHold) hold(record, body, nowMillis);
        return drop(
          6,
          "waiting for the day's certificate — no certified day key covers this signature",
          THREAD_DROP.AWAITING_CERTIFICATE
        );
      }
    } else if (!(await verifyThread(body.preimage, body.signature, link.publicKey))) {
      stats.forgeries++;
      return drop(
        6,
        "signature does not verify (a link holder tried to publish)",
        THREAD_DROP.BAD_SIGNATURE
      );
    }

    // 7. Sequence: strictly increasing — replay and rollback protection.
    if (record.seq <= ledgerFor(signer)) return drop(7, "seq not strictly increasing");

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

    if (signer) signer.highestSeq = record.seq;
    // The global figure stays a high-water mark across the whole thread:
    // it is what a §20.5 mid-run handover resumes above, and a
    // replacement device must not restart under the number the audience
    // has already seen today.
    highestSeq = Math.max(highestSeq, record.seq);
    const update = { ...body, seq: record.seq, receivedAt: nowMillis };
    latest = update;
    history.push(update);
    if (history.length > constants.THREAD_CACHE_RING) history.shift();
    stats.accepted++;
    return { ok: true, update };
  }

  function hold(record, body, nowMillis) {
    heldRecords.push({ record, body, nowMillis });
    stats.held++;
    while (heldRecords.length > constants.THREAD_HELD_RECORDS) {
      heldRecords.shift();
      stats.heldEvicted++;
    }
  }

  /**
   * Re-runs the held records now that a certificate has arrived, oldest
   * `seq` first so the ledger advances in order.
   *
   * A record that still fails the signature stays held: it may belong to
   * a day key whose certificate is the *next* one to arrive. A record
   * that gets past the signature has had its turn — accepted, or dropped
   * by steps 7–9 on its own merits — and leaves the buffer either way.
   */
  async function releaseHeld(nowMillis) {
    if (!heldRecords.length) return [];
    const pending = heldRecords.splice(0, heldRecords.length).sort((a, b) => a.record.seq - b.record.seq);
    const nowSeconds = Math.floor(nowMillis / 1000);
    const released = [];
    for (const entry of pending) {
      if (entry.body.unixSeconds < nowSeconds - constants.THREAD_MAX_AGE) {
        stats.heldEvicted++;
        continue;
      }
      const verdict = await finishRecord(entry.record, entry.body, nowMillis, { mayHold: false });
      if (verdict.ok) {
        stats.heldReleased++;
        released.push(verdict.update);
      } else if (verdict.code === THREAD_DROP.AWAITING_CERTIFICATE) {
        heldRecords.push(entry);
      }
    }
    while (heldRecords.length > constants.THREAD_HELD_RECORDS) {
      heldRecords.shift();
      stats.heldEvicted++;
    }
    return released;
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
    // The top two rows are the ones that say "live position", so they are
    // gated on actually having one — not merely on the thread being live.
    // A coarse run (§11) publishes state and a heartbeat with the position
    // withheld by design: it is live, it is useful, and claiming a live
    // position for it would be the upgrade §12 forbids in the one case
    // where the publisher deliberately withheld it.
    let row;
    if (hasPosition && hasTraffic) row = "thread+traffic";
    else if (hasPosition) row = "thread-only";
    else if (hasTraffic) row = "traffic-only";
    else row = "neither";
    return {
      row,
      live,
      hasPosition,
      ageSeconds: Number.isFinite(ageSeconds) ? ageSeconds : null,
      state: latest?.state ?? THREAD_STATE.SCHEDULED,
      stopIndex: latest?.stopIndex ?? 0,
      // Outcomes are cumulative in every record, so the newest update
      // carries the whole day. They are also *not* part of the §12
      // rows: a stop marked delivered stays delivered whether or not
      // this device still has a live position, so no claim moves.
      outcomes: latest?.outcomes ?? [],
      lastOutcome: latest?.lastOutcome ?? null,
      // Cumulative for the same reason the map is, and read the same way:
      // an entry is why a stop was skipped or failed, and a *gap* against
      // a skipped stop is a reason this device will never learn. Use
      // `stopReasonFor` rather than searching this by hand — it is the
      // thing that keeps "no reason given" and "reason lost" apart.
      stopReasons: latest?.stopReasons ?? [],
      // §20.7. How many photo commitments the run says it has published.
      // Fewer distinct commitments held than this is exactly how many
      // proofs of delivery this device can never fetch.
      photoCount: latest?.photoCount ?? 0,
      travelMode: latest?.travelMode ?? 0,
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
    /** §21. True when this link delegates signing to certified day keys. */
    delegated,
    /** The live day certificates this subscriber holds (§21). */
    certificates: () => [...certificates.values()],
    /** Whether anything currently held can authorise a record. */
    certified: () => certificates.size > 0,
    /** How many records are waiting on the day's certificate. */
    get heldCount() { return heldRecords.length; },
    get highestSeq() { return highestSeq; }
  };
}
