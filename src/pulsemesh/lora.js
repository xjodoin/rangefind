// PulseMesh over LoRa (§16): the Meshtastic-class transport profile and
// the bonded bridge that joins a radio mesh to the IP mesh.
//
// The fit is not a coincidence. A PMC1 is 43 bytes because the codec
// refuses to carry coordinates, trajectories, or identities — and that
// same refusal is exactly what a duty-cycled 0.3–20 kbps radio needs.
// Five contributions batch into one 220-byte frame under Meshtastic's
// ~237-byte payload cap; an incident is 44 bytes; a thread update is
// ≤ 256 (typically ~100). What does NOT fit is everything bulk: zone
// digests, PMQ1/PMS1 snapshots, the whole sync stream family. So a LoRa
// segment is gossip-only by construction, and the airtime budget — not
// a bond — is its admission: you cannot flood a channel you are only
// allowed to whisper on.
//
// The module speaks to a DUCK RADIO, not to Meshtastic directly:
//
//   radio.maxPayload            // bytes per frame (Meshtastic: ~237)
//   radio.send(bytes)           // broadcast one frame on the channel
//   radio.onReceive(cb)         // cb(bytes, fromRadioId) per frame
//   radio.peers?()              // optional: known radio node ids
//
// so the real Meshtastic binding is a ~20-line shim over @meshtastic/js
// (see docs/pulsemesh-lora.md), and the physics — payload caps, duplicate
// flood delivery, airtime budgets — is testable without hardware.

import { applyBootstrapConstants } from "./bins.js";
import { MAGIC, RESERVED_MAGIC_PREFIXES, decodeAny, encodePMB1 } from "./codec.js";
import { topicForCell } from "./topics.js";
import { topicWindowFromMillis } from "./bins.js";
import { createValidator } from "./validate.js";

// --- §16.1 the LoRa constants profile ------------------------------------
//
// Every override sits inside the bootstrap's ±4× tunable envelope, so a
// LoRa deployment is expressible as an ordinary signed bootstrap.
// MAX_AGE_RECEIPT stays at 90 s deliberately: the FRESHNESS table zeroes
// at 90 s and CONTRIB_TTL is wire-capped at 90, so accepting older
// contributions would admit dead weight — multi-hop radio latency is
// real, which is exactly why LoRa is primarily an INCIDENT and THREAD
// medium (whose TTLs are minutes), with contributions best-effort.

export const LORA_PROFILE_OVERRIDES = Object.freeze({
  MAX_AGE_RECEIPT: 90,   // 2×: allow multi-hop radio latency
  MAX_FUTURE_SKEW: 45,   // 3×: radio nodes keep worse clocks
  EMIT_INTERVAL: 60      // 4×: airtime is the scarce resource
});

export const LORA_CONSTANTS = applyBootstrapConstants(LORA_PROFILE_OVERRIDES);

/** Meshtastic's usable payload after its own headers. */
export const LORA_MAX_PAYLOAD = 237;

// Outbound priority: what airtime buys first. A hazard report matters
// more off-grid than anywhere else; a thread update is someone's bus; a
// contribution is statistics. PMA1/PMX1 never cross the air — bonds and
// ban testimony are IP-domain artifacts (§16.3).
const PRIORITY = { [MAGIC.PMI1]: 0, PMT: 1, PMR: 1, [MAGIC.PMB1]: 2, [MAGIC.PMC1]: 2 };
const NEVER_AIRED = new Set([MAGIC.PMA1, MAGIC.PMX1, MAGIC.PMG1, MAGIC.PMD1, MAGIC.PMQ1, MAGIC.PMS1, MAGIC.PMN1, MAGIC.PMF1]);

function magicOf(bytes) {
  if (bytes.length < 4) return null;
  return String.fromCharCode(bytes[0], bytes[1], bytes[2], bytes[3]);
}

function priorityOf(magic) {
  if (magic in PRIORITY) return PRIORITY[magic];
  const prefix = magic.slice(0, 3);
  if (prefix in PRIORITY) return PRIORITY[prefix];
  return 3;
}

/**
 * A shared outbound scheduler: frames queue with priorities and drain
 * inside a bytes-per-minute budget. `flush(nowMillis)` is explicit — a
 * deployment calls it on its own tick; tests call it deterministically.
 * When the queue overflows, the *lowest* priority is evicted first: an
 * incident is never dropped to make room for a contribution.
 */
function createAirQueue({ radio, bytesPerMinute, queueCap }) {
  const queue = []; // { bytes, priority, seq }
  let seq = 0;
  let windowStart = 0;
  let windowSpent = 0;
  const stats = { queued: 0, sent: 0, sentBytes: 0, dropOversize: 0, dropOverflow: 0, dropNeverAired: 0 };

  function offer(bytes) {
    const magic = magicOf(bytes);
    if (magic == null || NEVER_AIRED.has(magic)) { stats.dropNeverAired++; return false; }
    if (bytes.length > radio.maxPayload) { stats.dropOversize++; return false; }
    queue.push({ bytes, priority: priorityOf(magic), seq: seq++ });
    queue.sort((a, b) => a.priority - b.priority || a.seq - b.seq);
    stats.queued++;
    while (queue.length > queueCap) {
      queue.pop(); // sorted: the tail is the lowest priority, newest last
      stats.dropOverflow++;
    }
    return true;
  }

  function flush(nowMillis) {
    if (nowMillis - windowStart >= 60000) {
      windowStart = nowMillis;
      windowSpent = 0;
    }
    while (queue.length && windowSpent + queue[0].bytes.length <= bytesPerMinute) {
      const frame = queue.shift();
      windowSpent += frame.bytes.length;
      stats.sent++;
      stats.sentBytes += frame.bytes.length;
      radio.send(frame.bytes);
    }
    return queue.length;
  }

  return { offer, flush, stats, get pending() { return queue.length; } };
}

// --- §16.2 the phone-side transport ---------------------------------------

/**
 * Implements the five-verb MeshNetwork over a duck radio, for the phone
 * that IS on the radio mesh (paired over BLE with its node, typically).
 *
 * Topics do not exist on a radio channel — the RF footprint is the
 * scope — so the adapter derives each inbound record's topic from its
 * own cell, exactly as a publisher would have. Rule 8 therefore holds by
 * construction on this transport; its work is done by physics.
 *
 * Admission: radio sender ids are spoofable broadcast fields, so each is
 * admitted as a `lora:<id>` pseudo-peer — rate-limited and trust-tracked
 * individually, revocable individually, but never treated as a §5.4
 * identity. The honest statement, documented in §16.3: an RF-adjacent
 * attacker who could abuse this could equally jam the channel, so the
 * pseudo-peer is a bookkeeping unit, not a security boundary. The
 * security boundary is the bridge.
 */
export function createLoraNetwork(radio, {
  bytesPerMinute = 240,   // ~1% duty at LongFast-class rates; operator-tuned
  queueCap = 32,
  clock = Date.now
} = {}) {
  let meshNode = null;
  const air = createAirQueue({ radio, bytesPerMinute, queueCap });
  const stats = { framesIn: 0, recordsIn: 0, badFrames: 0, air: air.stats };

  function deliver(bytes, fromRadioId) {
    stats.framesIn++;
    if (!meshNode) return;
    let message;
    try {
      message = decodeAny(bytes);
    } catch {
      stats.badFrames++;
      return;
    }
    const nowMillis = meshNode.clock();
    const pseudoPeer = `lora:${fromRadioId ?? "air"}`;
    const records = message.kind === "batch" ? message.records
      : message.kind === "contribution" || message.kind === "incident" ? [message]
      : null;
    if (!records) {
      // Thread-family frames are someone else's sealed mail — ignored,
      // not bad. Anything else unknown on the channel is noise.
      const magic = magicOf(bytes) ?? "";
      if (!RESERVED_MAGIC_PREFIXES.some(prefix => magic.startsWith(prefix)) && !(magic in MAGIC)) {
        stats.badFrames++;
      }
      return;
    }
    for (const record of records) {
      const cell = meshNode.cellOf(record);
      if (!cell) continue;
      stats.recordsIn++;
      const topic = topicForCell({
        epochPrefix16hex: meshNode.epochPrefix16hex,
        cell,
        window: topicWindowFromMillis(nowMillis)
      });
      const payload = record.kind === "contribution" ? encodePMB1([record.bytes]) : record.bytes;
      meshNode.onGossip(topic, payload, pseudoPeer, nowMillis);
    }
  }

  radio.onReceive(deliver);

  return {
    register(node) {
      if (meshNode && meshNode !== node) throw new Error("One MeshNode per radio.");
      meshNode = node;
    },
    /**
     * The radio's pseudo-peers hold no §5.4 bonds — their admission is
     * airtime. Marking them bonded is the adapter's explicit, visible
     * decision, and the node's ordinary forfeiture still revokes any of
     * them individually on first-hand evidence.
     */
    admitRadioPeer(fromRadioId, untilMillis = Number.MAX_SAFE_INTEGER) {
      if (!meshNode) throw new Error("register(node) first.");
      meshNode.bondedPeers.set(`lora:${fromRadioId ?? "air"}`, untilMillis);
    },
    subscribe() {},   // the channel is the subscription
    unsubscribe() {},
    publish(_topic, payload) {
      air.offer(payload);
    },
    async request() {
      return null;    // no streams on a radio: the sync family is absent
    },
    peersOf() {
      return radio.peers ? radio.peers().map(id => `lora:${id}`) : [];
    },
    /** Drain the outbound queue inside the airtime budget. Call on a tick. */
    flush(nowMillis = clock()) {
      return air.flush(nowMillis);
    },
    stats
  };
}

// --- §16.3 the bonded bridge ----------------------------------------------

/**
 * Joins a radio segment to the IP mesh. `node` is an ordinary bonded
 * MeshNode on libp2p; `radio` is the duck radio. The bridge validates
 * every record arriving off the air against its OWN static map — rules
 * 1–12 are transport-independent — and republishes survivors under its
 * own §5.4 vouching, staking its bond and trust on them exactly as the
 * §4.5 forwarder and the snapshot provider already do. Hop-vouched
 * admission is what makes dissimilar meshes composable: the IP side
 * never needs to know LoRa exists.
 *
 * Thread frames (the PMT- and PMR-families) shuttle OPAQUELY:
 * end-to-end authenticated
 * and sealed to their audience, so the bridge can carry them without
 * being able to read them — uplink always, downlink only for thread
 * links the operator explicitly holds (its own fleet's buses, say).
 *
 * Downlink policy: incidents always (a hazard matters most where there
 * is no coverage), contributions never (dead weight at radio latency for
 * anyone not already adjacent), everything else never.
 */
export async function createLoraBridge({
  node,
  radio,
  bytesPerMinute = 240,
  queueCap = 32,
  strikeLimit = 3,
  muteMillis = 10 * 60 * 1000,
  threadLinks = [],
  cellContext = null,
  clock = null
} = {}) {
  const now = clock ?? node.clock;
  const air = createAirQueue({ radio, bytesPerMinute, queueCap });
  const stats = {
    upValidated: 0, upVouched: 0, upDropped: 0, upThreads: 0,
    downIncidents: 0, downThreads: 0, muted: 0, air: air.stats
  };

  // The bridge's own gate for air arrivals. isBonded is true by
  // construction: what stands behind an uplinked record is not the radio
  // sender's identity (spoofable) but the bridge's willingness to vouch
  // after validating — its bond is the one on the line.
  const validator = createValidator({
    constants: node.constants,
    epoch32: node.epoch32,
    previousEpoch32: node.previousEpoch32,
    cellOf: record => node.cellOf(record),
    cellContext,
    transport: "wire",
    isBonded: () => true,
    clock: now
  });

  // Radio senders that keep failing provable rules get muted locally —
  // cheap bookkeeping against a noisy or hostile neighbor, with the same
  // honest caveat as §16.2: the real recourse against an RF-adjacent
  // attacker is the trust the IP side places in THIS bridge, not in them.
  const strikes = new Map(); // radioId -> { count, mutedUntil }

  function struck(radioId, nowMillis) {
    const entry = strikes.get(radioId) ?? { count: 0, mutedUntil: 0 };
    entry.count += 1;
    if (entry.count >= strikeLimit) {
      entry.mutedUntil = nowMillis + muteMillis;
      entry.count = 0;
      stats.muted++;
    }
    strikes.set(radioId, entry);
  }

  function isMuted(radioId, nowMillis) {
    const entry = strikes.get(radioId);
    return entry != null && nowMillis < entry.mutedUntil;
  }

  // Optional thread downlink: only for links the operator holds. Tags
  // rotate per window, so topics are re-derived on each maintenance tick.
  let threadSubscribers = [];
  let threadTopics = new Set();
  if (threadLinks.length) {
    const { createThreadSubscriber } = await import("./thread_consume.js");
    threadSubscribers = await Promise.all(
      threadLinks.map(link => createThreadSubscriber({ link, epoch32: node.epoch32, clock: now }))
    );
  }

  async function refreshThreadTopics(nowMillis) {
    const wanted = new Set();
    for (const subscriber of threadSubscribers) {
      for (const topic of await subscriber.topics(nowMillis)) wanted.add(topic);
    }
    for (const topic of wanted) if (!threadTopics.has(topic)) node.network.subscribe(node.id, topic);
    for (const topic of threadTopics) if (!wanted.has(topic)) node.network.unsubscribe(node.id, topic);
    threadTopics = wanted;
  }

  // --- uplink: air → validated → vouched onto the IP mesh ---------------
  radio.onReceive(async (bytes, fromRadioId) => {
    const nowMillis = now();
    const radioId = String(fromRadioId ?? "air");
    if (isMuted(radioId, nowMillis)) { stats.upDropped++; return; }
    const magic = magicOf(bytes);
    if (magic && (magic.startsWith("PMT") || magic.startsWith("PMR"))) {
      // Opaque shuttle: sealed to its audience, verifiable by them alone.
      const { decodeThreadRecord } = await import("./thread_codec.js");
      const { threadTopic } = await import("./thread_crypto.js");
      try {
        const record = decodeThreadRecord(bytes);
        node.network.publish(threadTopic(node.epochPrefix16hex, record.tag), bytes, node.id);
        stats.upThreads++;
      } catch {
        stats.upDropped++;
        struck(radioId, nowMillis);
      }
      return;
    }
    let message;
    try {
      message = decodeAny(bytes);
    } catch {
      stats.upDropped++;
      struck(radioId, nowMillis);
      return;
    }
    const records = message.kind === "batch" ? message.records
      : message.kind === "contribution" || message.kind === "incident" ? [message]
      : null;
    if (!records) { stats.upDropped++; return; }
    for (const record of records) {
      const verdict = record.kind === "incident"
        ? validator.validateIncident(record, { store: node.store, fromPeer: `lora:${radioId}`, nowMillis })
        : validator.validateContribution(record, { store: node.store, fromPeer: `lora:${radioId}`, nowMillis });
      if (!verdict.ok) {
        stats.upDropped++;
        if (verdict.trustPenalty) struck(radioId, nowMillis);
        continue;
      }
      stats.upValidated++;
      // Republish as our own: our bond vouches, our trust is at stake.
      node.publishRecord({ bytes: record.bytes }, { nowMillis });
      stats.upVouched++;
    }
  });

  // --- downlink: accepted incidents (and held-link threads) → air --------
  const previousTap = node.onRecordAccepted;
  node.onRecordAccepted = (record, meta) => {
    previousTap?.(record, meta);
    if (record.kind === "incident") {
      if (air.offer(record.bytes)) stats.downIncidents++;
    }
  };

  return {
    stats,
    /** One maintenance beat: rotate thread topics, drain the air queue. */
    async tick(nowMillis = now()) {
      if (threadSubscribers.length) await refreshThreadTopics(nowMillis);
      return air.flush(nowMillis);
    },
    /** Feed IP-side gossip that may belong on the air (thread downlink). */
    onIpGossip(topic, payload) {
      if (!threadTopics.has(topic)) return false;
      if (air.offer(payload)) { stats.downThreads++; return true; }
      return false;
    },
    close() {
      node.onRecordAccepted = previousTap;
      for (const topic of threadTopics) node.network.unsubscribe(node.id, topic);
      threadTopics = new Set();
    }
  };
}
