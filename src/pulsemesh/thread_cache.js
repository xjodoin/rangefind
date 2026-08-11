// Thread catch-up (threads §5.5, §8): every subscriber already caches
// the thread's recent records to render it, so any subscriber can answer
// a late joiner. There is no mailbox and no designated host.
//
// The inversion this produces is the point: availability scales **with
// audience size**, which is exactly backwards from a server, where more
// viewers cost more. A parent whose phone slept for four minutes pulls
// the gap from another parent.
//
// A relay caches sealed bytes it may not be able to open. It does not
// need to be trusted: records travel verbatim, so a relay that tampers
// produces an AEAD failure, one that forges produces a signature
// failure, and one that replays produces a `seq` failure. Because it
// cannot open them, though, it also cannot tell real records from
// garbage addressed to invented tags — hence the admission caps.

import { THREAD_CONSTANTS } from "./thread_publish.js";
import {
  THREAD_REQUEST_SIZES,
  decodeThreadRequest,
  encodeThreadRequest,
  encodeThreadResponse,
  decodeThreadResponse,
  decodeThreadRecord
} from "./thread_codec.js";
import { toHex } from "./sha256.js";

const SOURCE_BUDGET_MAX = 1024;
const OPENABLE_BURST = 4;

function randomBytes(length) {
  const out = new Uint8Array(length);
  globalThis.crypto.getRandomValues(out);
  return out;
}

/**
 * A ring buffer of sealed records per tag, with LRU eviction across tags
 * and per-source admission limits.
 *
 * `openable` marks tags this peer actually subscribes to; those are kept
 * regardless of the relay eviction budget, because dropping your own
 * thread to make room for someone else's is never right. Remote records
 * remain rate-limited even when openable: possession of a follow link is
 * not permission to make every subscriber decrypt an unlimited flood.
 */
export function createThreadCache({ constants = THREAD_CONSTANTS, clock = Date.now, rng = Math.random } = {}) {
  const tags = new Map();       // tagHex -> { records: [], lastUsed, openable }
  const sourceBudget = new Map(); // peerId -> { window, newTags, admitted: Map(tagHex -> lastMillis) }
  const globalBudget = new Map(); // tagHex -> host-wide token bucket
  const stats = { admitted: 0, rejected: 0, evictedTags: 0, served: 0, requests: 0 };

  function entryFor(tagHex, { openable = false, nowMillis = clock() } = {}) {
    let entry = tags.get(tagHex);
    if (!entry) {
      entry = { records: [], lastUsed: nowMillis, openable };
      tags.set(tagHex, entry);
    }
    entry.lastUsed = nowMillis;
    if (openable) entry.openable = true;
    return entry;
  }

  function evictIfNeeded() {
    if (tags.size <= constants.THREAD_CACHE_TAGS) return;
    // LRU across relay-only tags; a subscriber's own threads are exempt.
    let oldest = null;
    for (const [tagHex, entry] of tags) {
      if (entry.openable) continue;
      if (!oldest || entry.lastUsed < oldest.entry.lastUsed) oldest = { tagHex, entry };
    }
    if (oldest) {
      tags.delete(oldest.tagHex);
      stats.evictedTags++;
    }
  }

  /**
   * Admits a sealed record. Locally produced records have no `fromPeer`
   * and bypass network quotas. Remote openable records get a small burst
   * for certificate+position startup, then the ordinary per-tag refill;
   * relay-only traffic gets no burst and also pays the new-tag budget.
   */
  function admit(record, { fromPeer = null, openable = false, retain = true, nowMillis = clock(), retainUntilMillis = null } = {}) {
    const decoded = record instanceof Uint8Array ? decodeThreadRecord(record) : record;
    if (decoded.bytes.length > constants.THREAD_MAX_RECORD_BYTES) {
      stats.rejected++;
      return { admitted: false, reason: "oversize" };
    }
    const tagHex = toHex(decoded.tag);
    const known = tags.get(tagHex);

    if (fromPeer != null) {
      const globalBurst = Math.max(1, constants.THREAD_GLOBAL_BURST ?? 32);
      const globalRate = Math.max(1, constants.THREAD_GLOBAL_RATE ?? 16);
      const global = globalBudget.get(tagHex) ?? { tokens: globalBurst, lastMillis: nowMillis };
      const globalElapsed = Math.max(0, nowMillis - global.lastMillis);
      global.tokens = Math.min(globalBurst, global.tokens + (globalElapsed / 1000) * globalRate);
      global.lastMillis = nowMillis;
      if (global.tokens < 1) {
        globalBudget.set(tagHex, global);
        stats.rejected++;
        return { admitted: false, reason: "global-rate" };
      }
      global.tokens -= 1;
      globalBudget.delete(tagHex);
      globalBudget.set(tagHex, global);
      while (globalBudget.size > constants.THREAD_CACHE_TAGS) {
        globalBudget.delete(globalBudget.keys().next().value);
      }

      const window = Math.floor(nowMillis / 1000 / 300);
      let budget = sourceBudget.get(fromPeer);
      if (!budget || budget.window !== window) {
        for (const [peer, held] of sourceBudget) {
          if (held.window !== window) sourceBudget.delete(peer);
        }
        while (sourceBudget.size >= SOURCE_BUDGET_MAX) {
          sourceBudget.delete(sourceBudget.keys().next().value);
        }
        budget = { window, newTags: 0, admitted: new Map() };
        sourceBudget.set(fromPeer, budget);
      }
      if (!openable && !known && budget.newTags >= constants.THREAD_TAG_BUDGET) {
        stats.rejected++;
        return { admitted: false, reason: "tag-budget" };
      }
      const burst = openable ? OPENABLE_BURST : 1;
      const refillMillis = constants.THREAD_CACHE_RATE * 1000;
      const previous = budget.admitted.get(tagHex);
      const elapsed = previous ? Math.max(0, nowMillis - previous.lastMillis) : 0;
      const available = previous
        ? Math.min(burst, previous.tokens + elapsed / refillMillis)
        : burst;
      if (available < 1) {
        stats.rejected++;
        return { admitted: false, reason: "rate" };
      }
      if (!openable && !known) budget.newTags++;
      budget.admitted.set(tagHex, { tokens: available - 1, lastMillis: nowMillis });
    }

    // Validation-only mode for a channel configured not to relay. The
    // framing and source quota above still apply before forwarding, but
    // bytes this host neither follows nor republishes do not occupy its
    // catch-up cache.
    if (!retain) return { admitted: true, retained: false };

    const entry = entryFor(tagHex, { openable, nowMillis });
    if (entry.records.some(held => held.generation === decoded.generation && held.seq === decoded.seq)) {
      return { admitted: false, reason: "duplicate" };
    }
    entry.records.push({
      generation: decoded.generation,
      seq: decoded.seq,
      bytes: decoded.bytes,
      receivedAt: nowMillis,
      // An explicit deadline, for records this device published itself.
      // Everything else here is somebody else's traffic held on a relay
      // budget, and ten minutes is right for that. A run of one's own is
      // the thing a customer comes back and asks for.
      retainUntilMillis: Number.isFinite(retainUntilMillis) ? retainUntilMillis : null
    });
    entry.records.sort((a, b) => a.generation - b.generation || a.seq - b.seq);
    if (entry.records.length > constants.THREAD_CACHE_RING) entry.records.shift();
    stats.admitted++;
    evictIfNeeded();
    return { admitted: true, retained: true };
  }

  /** Drops records past THREAD_CACHE_TTL, and tags left empty. */
  function sweep(nowMillis = clock()) {
    const cutoff = nowMillis - constants.THREAD_CACHE_TTL * 1000;
    for (const [tagHex, entry] of [...tags]) {
      // A record carrying its own deadline outlives the relay window: it
      // is this device's own run, kept so a follower arriving after the
      // last stop still has somebody to ask. Everything else ages out on
      // THREAD_CACHE_TTL exactly as before.
      entry.records = entry.records.filter(record => (
        record.retainUntilMillis != null
          ? nowMillis < record.retainUntilMillis
          : record.receivedAt > cutoff
      ));
      if (!entry.records.length && !entry.openable) tags.delete(tagHex);
    }
  }

  /**
   * Answers a PMR1. Unknown tags return count 0 — a responder must not
   * distinguish "tag I do not hold" from "tag with no new data", because
   * doing so would tell a prober which threads this peer follows.
   */
  function answer(request, { nowMillis = clock() } = {}) {
    // Not a duck-check: a Uint8Array carries an `entries` *method*, so
    // `request.entries ? …` silently takes the wrong branch.
    const decoded = request instanceof Uint8Array ? decodeThreadRequest(request) : request;
    stats.requests++;
    const entries = decoded.entries.map(({ tag, sinceGeneration, sinceSeq }) => {
      const entry = tags.get(toHex(tag));
      if (!entry) return { tag, records: [] };
      entry.lastUsed = nowMillis;
      const records = entry.records
        .filter(record => record.generation > sinceGeneration
          || (record.generation === sinceGeneration && record.seq > sinceSeq))
        .map(record => record.bytes);
      stats.served += records.length;
      return { tag, records };
    });
    return { entries };
  }

  function has(tag) {
    return tags.has(toHex(tag));
  }

  function recordsFor(tag) {
    return (tags.get(toHex(tag))?.records || []).map(record => record.bytes);
  }

  return {
    admit,
    answer,
    sweep,
    has,
    recordsFor,
    stats,
    get size() { return tags.size; },
    get tagHexes() { return [...tags.keys()]; }
  };
}

/**
 * Builds a padded PMR1. Padding is *free* on this channel: a tag is
 * indistinguishable from uniform random bytes, so a decoy costs one
 * CSPRNG call and is perfectly indistinguishable from a real tag. Even a
 * peer holding the same thread cannot tell which of the tags a requester
 * actually came for.
 */
export function buildThreadRequest({ epochPrefix8, wanted, rng = Math.random }) {
  if (!wanted.length) throw new Error("A catch-up request needs at least one wanted tag.");
  const size = THREAD_REQUEST_SIZES.find(candidate => candidate >= wanted.length);
  if (!size) throw new Error(`A catch-up request carries at most ${THREAD_REQUEST_SIZES.at(-1)} tags.`);
  const entries = wanted.map(entry => ({
    tag: entry.tag,
    sinceGeneration: entry.sinceGeneration ?? 0,
    sinceSeq: entry.sinceSeq ?? 0
  }));
  while (entries.length < size) {
    entries.push({
      tag: randomBytes(8),
      sinceGeneration: Math.floor(rng() * 64),
      sinceSeq: Math.floor(rng() * 64)
    });
  }
  // Shuffle so position does not reveal which entries are real.
  for (let i = entries.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [entries[i], entries[j]] = [entries[j], entries[i]];
  }
  return { bytes: encodeThreadRequest({ epochPrefix8, entries }), entries, realCount: wanted.length };
}

export function encodeThreadCacheResponse(epochPrefix8, answered) {
  return encodeThreadResponse({ epochPrefix8, entries: answered.entries });
}

/**
 * Feeds a PMM1 response through a subscriber, in sequence order. Returns
 * how many updates were accepted — the late joiner validates everything
 * itself, so a relaying peer cannot alter, forge, or substitute anything.
 */
export async function applyThreadResponse(subscriber, payload, { nowMillis = Date.now(), wantedTags = null } = {}) {
  const response = payload instanceof Uint8Array ? decodeThreadResponse(payload) : payload;
  const wanted = wantedTags ? new Set(wantedTags.map(toHex)) : null;
  const records = [];
  for (const entry of response.entries) {
    if (wanted && !wanted.has(toHex(entry.tag))) continue; // our decoys
    records.push(...entry.records);
  }
  records.sort((a, b) => a.generation - b.generation || a.seq - b.seq);
  let accepted = 0;
  for (const record of records) {
    const verdict = await subscriber.accept(record, { nowMillis });
    if (verdict.ok) accepted++;
  }
  return accepted;
}
