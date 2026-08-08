// PulseMesh mesh-node wiring (protocol §5.1, §11, §12): ties the codec,
// store, validator, trust ledger, forwarder, sync, and provider together
// behind a transport-agnostic network interface. The same node runs as a
// browser consumer, a mobile contributor, or a headless keeper (same
// store, same validation, same TTLs — a keeper just doesn't contribute
// and holds more connections and a raised store cap).
//
// The network interface is deliberately tiny so a js-libp2p host, the
// in-process loopback used by tests (phase 1), and the virtual-time
// simulation harness (phase 2) are interchangeable:
//
//   network.register(node)
//   network.subscribe(nodeId, topic) / unsubscribe(nodeId, topic)
//   network.publish(topic, payload, fromId)          // gossip
//   network.request(fromId, toId, payload) -> bytes  // sync stream
//   network.requestThread(fromId, toId, payload) -> bytes  // §5.5 catch-up
//   network.requestPhoto(fromId, toId, payload) -> bytes   // §20.7 blobs
//   network.peersOf(nodeId) -> peerId[]
//
// A transport that forwards before the application sees a message —
// GossipSub does — additionally calls `node.judgeGossip(...)` in place of
// `node.onGossip(...)`, ahead of its forward decision, and acts on the
// verdict it returns. Transports that hand a message straight to their
// one local node (loopback, LoRa) keep calling `onGossip`: there is no
// relay step to gate, and nothing was vouched for on the way in.

import { DEFAULT_CONSTANTS, detailCellKey, timeBucketFromMillis, topicWindowFromMillis } from "./bins.js";
import {
  MAGIC,
  decodeAny,
  encodePMB1,
  encodePMD1,
  encodePMF1,
  encodePMG1,
  encodePMN1,
  encodePMQ1,
  encodePMS1,
  decodePMA1,
  encodePMX1,
  BAN_REASON_INVALID_RECORDS,
  parseSegment,
  utf8Bytes
} from "./codec.js";
import { verifyBond } from "./bond.js";
import { PulseMeshStore } from "./store.js";
import { TrustLedger } from "./aggregate.js";
import {
  GOSSIP_ACCEPT,
  GOSSIP_IGNORE,
  GOSSIP_REJECT,
  createValidator,
  worseGossipVerdict
} from "./validate.js";
import { createForwardHandler } from "./forward.js";
import { createPulseMeshProvider } from "./provider.js";
import { scoreIncidentKey } from "./incidents.js";
import { buildCellRequests, buildDecoyPool, corridorCells, diffDigest } from "./sync.js";
import { parseTopic, topicForCell, topicName, topicsForZones, windowAcceptable } from "./topics.js";
import { fromHex, sha256, toHex } from "./sha256.js";
import { signThread, verifyThread } from "./thread_crypto.js";

const BAN_HASH_TAG = "rangefind-ban-v1:";

/** The 16-byte peer handle a PMX1 names — never the peerId itself. */
export function banPeerHash16(peerId) {
  return sha256(utf8Bytes(BAN_HASH_TAG + String(peerId))).subarray(0, 16);
}

function foldsEqual(a, b) {
  for (let i = 0; i < 8; i++) if (a[i] !== b[i]) return false;
  return true;
}

export class MeshNode {
  constructor({
    id,
    epochHex,
    previousEpochHex = null,
    constants = DEFAULT_CONSTANTS,
    cellOf,
    cellContext = null,
    freeflowKmhOf = null,
    network,
    clock = Date.now,
    rng = Math.random,
    transport = "wire",
    suppressedTypes = [],
    keeper = false,
    // §11.6 read-only consumers: no gossip membership, no publishing, no
    // §5.4 bond — the node pulls everything it shows through the padded
    // sync path (PMG1 digests → PMQ1 cell fetches → PMS1 snapshots) from
    // bonded peers on tick(). This is the browser-at-home mode: a peer
    // that will never contribute should not pay a 256 MiB mint, and it
    // should not sit in the gossip mesh as a relay whose deliveries
    // every bonded receiver would ignore.
    readOnly = false
  }) {
    this.id = id;
    this.epochHex = epochHex;
    this.epochPrefix16hex = epochHex.slice(0, 16);
    this.epoch32 = fromHex(epochHex);
    this.epochPrefix8 = this.epoch32.subarray(0, 8);
    this.constants = constants;
    this.cellOf = cellOf;
    this.network = network;
    this.clock = clock;
    this.rng = rng;
    this.keeper = keeper;
    this.readOnly = readOnly;
    // §11.5 consumer overlap: for EPOCH_OVERLAP after handover a consumer
    // also subscribes to, and accepts, the previous epoch's topics. It
    // emits on the current epoch only — contributors switch immediately.
    this.previousEpochPrefix16hex = previousEpochHex ? previousEpochHex.slice(0, 16) : null;
    this.epochOverlapUntil = previousEpochHex ? clock() + constants.EPOCH_OVERLAP * 1000 : null;
    this.trust = new TrustLedger({ constants, clock });
    this.store = new PulseMeshStore({ constants, cellOf, trustOf: peer => this.trust.get(peer) });
    // §5.4: peers whose admission bond this node has verified, and until
    // when. The validator consults it for proofType 3; the transport
    // fills it via registerBond.
    this.bondedPeers = new Map(); // peerId -> expiresMillis
    // §8.4 forfeiture and propagation. `locallyBanned` holds first-hand
    // revocations (peerId -> untilMillis). `banLedger` holds remote
    // testimony (targetHashHex -> { accusers, firstMillis, corroborated,
    // appliedTo }); it can lower a peer's trust weight, never revoke.
    this.locallyBanned = new Map();
    this.banLedger = new Map();
    this.banReportIds = new Map();   // reportIdHex -> millis, replay window
    this.peerHashIndex = new Map();  // banPeerHash16 hex -> peerId
    this.previousEpoch32 = previousEpochHex ? fromHex(previousEpochHex) : null;
    this.validator = createValidator({
      constants,
      epoch32: this.epoch32,
      previousEpoch32: this.previousEpoch32,
      previousEpochUntilMillis: this.epochOverlapUntil,
      cellOf,
      cellContext,
      transport,
      suppressedTypes,
      isBonded: peerId => this.isBonded(peerId),
      clock
    });
    this.forwarder = createForwardHandler({
      validator: this.validator,
      store: this.store,
      constants,
      clock,
      schedule: (fn, delayMs) => network.schedule ? network.schedule(fn, delayMs) : setTimeout(fn, delayMs),
      publishAsOwn: (payload, meta) => this.#republishForwarded(payload, meta)
    });
    this.provider = createPulseMeshProvider({
      epochHex,
      store: this.store,
      trust: this.trust,
      constants,
      freeflowKmhOf,
      clock,
      // §9 step 2 / §11.3: a provider-triggered refresh is a cell fetch
      // like any other, so it goes through the padded/decoy/split path.
      // Cells already covered by a recent corridor fill are skipped —
      // refetching them would be pure overhead, not extra privacy.
      fetchCells: cells => this.refreshCells(cells)
    });
    this.subscribedTopics = new Set();
    this.topicDroppedAt = new Map();    // topic -> millis the linger expires
    this.cellRefreshedAt = new Map();   // cellKey -> millis of last fetch
    this.zones = [];
    this.visitedCells = new Map(); // key -> cell, for the decoy pool
    this.stats = {
      gossipAccepted: 0, gossipDropped: 0, dropsByRule: {},
      bondsAccepted: 0, bondsRejected: 0,
      bansForfeited: 0, bansPublished: 0, bansAccepted: 0, bansCorroborated: 0,
      snapshotsMerged: 0, snapshotRecordsAccepted: 0,
      antiEntropyRounds: 0, antiEntropyElided: 0, antiEntropyAgreed: 0,
      cellsRequested: 0, cellsWanted: 0
    };
    network.register(this);
  }

  // --- Subscription management (§11.4) ----------------------------------

  #acceptsEpochTopic(epochPrefix16hex, nowMillis) {
    if (epochPrefix16hex === this.epochPrefix16hex) return true;
    return this.previousEpochPrefix16hex === epochPrefix16hex && nowMillis < this.epochOverlapUntil;
  }

  subscribeZones(zones, nowMillis = this.clock()) {
    this.zones = zones;
    // §11.6: a read-only node tracks its zones — tick() targets its
    // anti-entropy pulls and the provider its cell fetches with them —
    // but never joins a gossip topic. Joining would make it a mesh
    // relay, and rule 5 has every bonded receiver ignore what an
    // unbonded relay delivers, so its membership would only punch holes
    // in other peers' delivery paths.
    if (this.readOnly) return;
    const wanted = new Set(topicsForZones({
      epochPrefix16hex: this.epochPrefix16hex,
      zones,
      nowMillis,
      overlapSeconds: this.constants.TOPIC_OVERLAP
    }));
    if (this.previousEpochPrefix16hex && nowMillis < this.epochOverlapUntil) {
      for (const topic of topicsForZones({
        epochPrefix16hex: this.previousEpochPrefix16hex,
        zones,
        nowMillis,
        overlapSeconds: this.constants.TOPIC_OVERLAP
      })) wanted.add(topic);
    }
    for (const topic of wanted) {
      this.topicDroppedAt.delete(topic);
      if (!this.subscribedTopics.has(topic)) {
        this.network.subscribe(this.id, topic);
        this.subscribedTopics.add(topic);
      }
    }
    // §11.4 UNSUB_LINGER: a topic that leaves the corridor is held for a
    // jittered linger before being dropped, so a brief detour, a reroute,
    // or a corridor recomputed one cell to the side does not announce
    // itself as an unsubscribe/resubscribe pair.
    for (const topic of [...this.subscribedTopics]) {
      if (wanted.has(topic)) continue;
      // Linger absorbs corridor jitter, so it only makes sense for a
      // topic that could still carry something we would accept. A topic
      // whose epoch has expired never will — release it at once.
      const parsed = parseTopic(topic);
      const stale = !parsed || parsed.reserved || !this.#acceptsEpochTopic(parsed.epochPrefix16hex, nowMillis);
      const droppedAt = this.topicDroppedAt.get(topic);
      if (!stale && droppedAt == null) {
        const linger = (this.constants.UNSUB_LINGER + (this.rng() - 0.5) * 30) * 1000;
        this.topicDroppedAt.set(topic, nowMillis + linger);
        continue;
      }
      if (stale || nowMillis >= droppedAt) {
        this.network.unsubscribe(this.id, topic);
        this.subscribedTopics.delete(topic);
        this.topicDroppedAt.delete(topic);
      }
    }
  }

  // --- Publishing --------------------------------------------------------

  /** Publishes one encoded PMC1 (as a PMB1 of 1) or PMI1 to its topic. */
  publishRecord(encoded, { forwarder = null, nowMillis = this.clock() } = {}) {
    if (this.readOnly) throw new Error("A read-only PulseMesh node never publishes (§11.6).");
    const record = decodeAny(encoded.bytes);
    const payload = record.kind === "contribution" ? encodePMB1([encoded.bytes]) : encoded.bytes;
    if (forwarder != null) {
      const wrapped = encodePMF1({
        epochPrefix8: this.epochPrefix8,
        delayMs: Math.floor(this.rng() * this.constants.FORWARD_MAX_DELAY * 1000),
        payload
      });
      this.network.request(this.id, forwarder, wrapped); // no response (§4.5)
      this.#storeOwn(record, nowMillis, true);
      return;
    }
    this.#storeOwn(record, nowMillis, false);
    this.#publishPayload(payload, record, nowMillis);
  }

  #publishPayload(payload, record, nowMillis) {
    const inner = record.kind === "batch" ? record.records[0] : record;
    const cell = this.cellOf(inner);
    if (!cell) return;
    const topic = topicForCell({
      epochPrefix16hex: this.epochPrefix16hex,
      cell,
      window: topicWindowFromMillis(nowMillis)
    });
    this.network.publish(topic, payload, this.id);
  }

  #storeOwn(record, nowMillis, viaForward) {
    const records = record.kind === "batch" ? record.records : [record];
    for (const one of records) {
      if (one.kind === "contribution") {
        this.store.addContribution(one, { nowMillis, deliveredBy: null, viaForward });
      } else if (one.kind === "incident") {
        this.store.addIncident(one, { nowMillis, deliveredBy: null, viaForward, scoreOf: key => this.#scoreOf(key, nowMillis) });
      }
    }
  }

  #republishForwarded(payload, meta) {
    const nowMillis = this.clock();
    for (const record of meta.records) this.#storeOwnForwarded(record, nowMillis);
    const decoded = decodeAny(payload);
    this.#publishPayload(payload, decoded, nowMillis);
  }

  #storeOwnForwarded(record, nowMillis) {
    if (record.kind === "contribution") {
      this.store.addContribution(record, { nowMillis, deliveredBy: null, viaForward: true });
    } else if (record.kind === "incident") {
      this.store.addIncident(record, { nowMillis, deliveredBy: null, viaForward: true, scoreOf: key => this.#scoreOf(key, nowMillis) });
    }
  }

  #scoreOf(key, nowMillis) {
    const scored = scoreIncidentKey(this.store.incidentsForKey(key), {
      nowMillis,
      trustOf: peer => this.trust.get(peer),
      constants: this.constants
    });
    return scored ? scored.score : 0;
  }

  // --- Admission bonds (§5.4) -------------------------------------------

  /**
   * Verifies a PMA1 presented by `fromPeer` and, when valid, marks the
   * peer bonded until the bond's bucket (plus overlap) ends. The peerId
   * MUST come from the live connection — it is the one input the sender
   * cannot choose, and the entire binding rests on it.
   */
  registerBond(payload, fromPeer, nowMillis = this.clock()) {
    // A forfeited peer is refused before its bytes are even decoded —
    // re-admission costs a fresh mint in a fresh bucket, not a retry.
    const bannedUntil = this.locallyBanned.get(fromPeer);
    if (bannedUntil != null) {
      if (nowMillis < bannedUntil) {
        this.stats.bondsRejected++;
        return { ok: false, reason: "bond forfeited here until its bucket ends" };
      }
      this.locallyBanned.delete(fromPeer);
    }
    let bond;
    try {
      bond = payload instanceof Uint8Array ? decodePMA1(payload) : payload;
    } catch (error) {
      this.stats.bondsRejected++;
      return { ok: false, reason: error.message };
    }
    const verdict = verifyBond(bond, {
      epoch32: this.epoch32,
      previousEpoch32: this.previousEpoch32,
      peerId: fromPeer,
      constants: this.constants,
      nowMillis
    });
    if (!verdict.ok) {
      this.stats.bondsRejected++;
      return verdict;
    }
    const existing = this.bondedPeers.get(fromPeer);
    this.bondedPeers.set(fromPeer, Math.max(existing ?? 0, verdict.expiresMillis));
    this.stats.bondsAccepted++;
    const hashHex = toHex(banPeerHash16(fromPeer));
    this.peerHashIndex.set(hashHex, fromPeer);
    // Testimony that arrived before we met this peer applies on arrival.
    const entry = this.banLedger.get(hashHex);
    if (entry && entry.corroborated && !entry.appliedTo.has(fromPeer)) {
      this.trust.penalizeRemoteBan(fromPeer);
      entry.appliedTo.add(fromPeer);
    }
    return verdict;
  }

  isBonded(peerId, nowMillis = this.clock()) {
    const expires = this.bondedPeers.get(peerId);
    if (expires == null) return false;
    if (nowMillis >= expires) {
      this.bondedPeers.delete(peerId);
      return false;
    }
    return true;
  }

  // --- Gossip receipt (§6) ----------------------------------------------

  /**
   * Delivery-time receipt: the loopback network, the LoRa bridge, and any
   * transport with no validator seam of its own land here. On GossipSub
   * the message has normally already been judged by `judgeGossip` before
   * it was forwarded, and the transport consumes that verdict rather than
   * calling this a second time (rule 7's token bucket and the §8.4 trust
   * path both mutate state — validating twice would charge one peer twice
   * for one record and forfeit honest peers).
   */
  onGossip(topicName, payload, fromPeer, nowMillis = this.clock()) {
    const topic = parseTopic(topicName);
    if (!topic || topic.reserved) {
      // The thread channel's key-derived topics (§5.2's reserved `t`
      // namespace) ride the same host and the same gossip mesh. They
      // are end-to-end authenticated by the thread key and mean nothing
      // to the traffic channel, so they are handed to whoever wired the
      // other channel up rather than silently dropped — otherwise every
      // host has to bypass this node to receive its own threads.
      this.onOtherTopic?.(topicName, payload, fromPeer, nowMillis);
      return;
    }
    this.#ingestGossip(topic, payload, fromPeer, nowMillis);
  }

  /**
   * §5.1: the GossipSub topic-validator seam — **relaying implies
   * validating**. GossipSub forwards a message to its mesh peers on
   * receipt, so a node that validates only on delivery spends its bond
   * vouching, at the transport layer, for bytes it never checked; any
   * peer could then launder invalid records through an honest relay.
   * Registered as a topic validator, this runs the real §6 pipeline
   * *before* the forward decision and returns what the transport should
   * do with the message:
   *
   *   GOSSIP_REJECT  a record failed a rule the trust ledger treats as a
   *                  provable lie (rules 10–12, `verdict.trustPenalty`).
   *                  Reject scores down the peer that handed us the
   *                  bytes, so nothing weaker may use it.
   *   GOSSIP_IGNORE  everything else that must not travel further: a
   *                  replay, a stale window, a foreign epoch, an
   *                  out-of-zone record — and, crucially, anything this
   *                  node could not actually judge (no leaf loaded for
   *                  the area). Ignore penalizes nobody, which is what
   *                  keeps an attacker from scoring down honest peers by
   *                  replaying their own traffic back at them.
   *   GOSSIP_ACCEPT  checked in full, stored, safe to vouch for.
   *   null           not this node's channel to judge (the reserved
   *                  thread namespace, or a topic that is not ours at
   *                  all). The caller falls back to its previous
   *                  behaviour; nothing is consumed and no side effect
   *                  has happened here.
   *
   * A message carrying several records is forwarded only if *every* one
   * of them was accepted and fully judged: the message is the unit the
   * transport relays, so it is the unit this node vouches for.
   */
  judgeGossip(topicName, payload, fromPeer, nowMillis = this.clock()) {
    const topic = parseTopic(topicName);
    if (!topic || topic.reserved) return null;
    return this.#ingestGossip(topic, payload, fromPeer, nowMillis);
  }

  #ingestGossip(topic, payload, fromPeer, nowMillis) {
    if (!this.#acceptsEpochTopic(topic.epochPrefix16hex, nowMillis)) return GOSSIP_IGNORE;
    if (payload.length > this.constants.MAX_GOSSIP_BYTES) { this.#drop("oversize"); return GOSSIP_IGNORE; }
    if (!windowAcceptable(topic.window, nowMillis)) { this.#drop("window"); return GOSSIP_IGNORE; }
    let message;
    try {
      message = decodeAny(payload);
    } catch {
      this.#drop("rule1");
      return GOSSIP_IGNORE;
    }
    if (message.kind === "batch") {
      // An empty batch is nothing to vouch for.
      let action = message.records.length ? GOSSIP_ACCEPT : GOSSIP_IGNORE;
      for (const record of message.records) {
        action = worseGossipVerdict(action, this.#acceptContribution(record, fromPeer, topic, nowMillis));
      }
      return action;
    }
    if (message.kind === "incident") return this.#acceptIncident(message, fromPeer, topic, nowMillis);
    if (message.kind === "ban") return this.#acceptBan(message, fromPeer, nowMillis);
    this.#drop("unexpected-" + (message.magic || message.kind));
    return GOSSIP_IGNORE;
  }

  #acceptContribution(record, fromPeer, topic, nowMillis) {
    const verdict = this.validator.validateContribution(record, { store: this.store, fromPeer, topic, nowMillis });
    if (!verdict.ok) {
      this.#drop(`rule${verdict.rule}`);
      if (verdict.trustPenalty) {
        this.trust.penalizeValidation(fromPeer);
        this.#maybeForfeit(fromPeer, nowMillis);
        return GOSSIP_REJECT;
      }
      return GOSSIP_IGNORE;
    }
    const result = this.store.addContribution(record, { nowMillis, deliveredBy: fromPeer });
    if (!result.added) {
      this.#drop(result.reason);
      return GOSSIP_IGNORE;
    }
    this.stats.gossipAccepted++;
    // Optional tap for gateways (LoRa bridges §16, fleet seeds §12.1):
    // fires only for records that passed every rule and entered the
    // store, on this path and on the snapshot merge below. That is what
    // makes it usable as a bridge's gate — a gateway republishing from
    // here is republishing what this node already staked its bond on.
    this.onRecordAccepted?.(record, { fromPeer, nowMillis });
    // Stored, but relayed only if rules 10–12 could actually be applied:
    // a peer whose map does not cover this leaf has checked nothing about
    // where the record claims to be, and vouching for it would make
    // "relay implies validation" nominal. It keeps the record for its own
    // use — that is its own risk to take — and does not pass it on.
    return verdict.judged ? GOSSIP_ACCEPT : GOSSIP_IGNORE;
  }

  #acceptIncident(record, fromPeer, topic, nowMillis) {
    const verdict = this.validator.validateIncident(record, { store: this.store, fromPeer, topic, nowMillis });
    if (!verdict.ok) {
      this.#drop(`rule${verdict.rule}`);
      if (verdict.trustPenalty) {
        this.trust.penalizeValidation(fromPeer);
        this.#maybeForfeit(fromPeer, nowMillis);
        return GOSSIP_REJECT;
      }
      return GOSSIP_IGNORE;
    }
    const result = this.store.addIncident(record, {
      nowMillis,
      deliveredBy: fromPeer,
      scoreOf: key => this.#scoreOf(key, nowMillis)
    });
    if (!result.added) {
      this.#drop(result.reason);
      return GOSSIP_IGNORE;
    }
    this.stats.gossipAccepted++;
    this.onRecordAccepted?.(record, { fromPeer, nowMillis });
    return verdict.judged ? GOSSIP_ACCEPT : GOSSIP_IGNORE;
  }

  #drop(reason) {
    this.stats.gossipDropped++;
    this.stats.dropsByRule[reason] = (this.stats.dropsByRule[reason] || 0) + 1;
  }

  // --- §8.4 forfeiture and ban propagation -------------------------------

  /**
   * First-hand evidence only: called after a provable validation failure
   * (rules 10–12) has just been penalized. When the peer's trust reaches
   * the floor, its bond is revoked here, re-registration is refused for
   * the bond's remaining lifetime, and a PMX1 announcement goes out —
   * testimony for other peers to corroborate, never a verdict they must
   * accept.
   */
  #maybeForfeit(fromPeer, nowMillis) {
    if (fromPeer == null || this.locallyBanned.has(fromPeer)) return;
    if (!this.trust.isFloored(fromPeer, nowMillis)) return;
    const until = this.bondedPeers.get(fromPeer)
      ?? nowMillis + this.constants.BOND_LIFETIME * 1000;
    this.bondedPeers.delete(fromPeer);
    this.locallyBanned.set(fromPeer, until);
    this.stats.bansForfeited++;
    this.#publishBan(fromPeer, nowMillis);
  }

  #publishBan(fromPeer, nowMillis) {
    if (this.readOnly || !this.zones.length) return;
    const targetHash16 = banPeerHash16(fromPeer);
    const reportId = new Uint8Array(16);
    globalThis.crypto.getRandomValues(reportId);
    const bytes = encodePMX1({
      epochPrefix8: this.epochPrefix8,
      targetHash16,
      reason: BAN_REASON_INVALID_RECORDS,
      timeBucket: timeBucketFromMillis(nowMillis),
      reportId
    });
    // One deterministic shard per zone: everyone subscribed to the zone
    // holds all its shards, so one topic reaches them all.
    const shard = targetHash16[0] % this.constants.SHARDS;
    const window = topicWindowFromMillis(nowMillis);
    for (const zone of this.zones) {
      this.network.publish(topicName({
        epochPrefix16hex: this.epochPrefix16hex,
        zoneX: zone.x,
        zoneY: zone.y,
        window,
        shard
      }), bytes, this.id);
    }
    this.stats.bansPublished++;
  }

  /**
   * Remote testimony. Corroboration by BAN_MIN_SOURCES distinct bonded
   * deliverers lowers the target's local trust weight — bounded, and
   * recoverable through the ledger's ordinary decay. It never revokes:
   * three colluding bonds can make a mesh distrust an honest peer's
   * weight for a while, which is the designed cost ceiling of defamation
   * here; they cannot silence it.
   *
   * Returns a §5.1 gossip verdict. Testimony never rejects: a PMX1 is an
   * accusation, and the ledger's own answer to a false one is
   * corroboration thresholds, not GossipSub scoring — a replayed or
   * over-rate announcement is a relay doing its job badly, not a lie.
   */
  #acceptBan(record, fromPeer, nowMillis) {
    const idHex = toHex(record.reportId);
    for (const [id, seen] of this.banReportIds) {
      if (nowMillis - seen > this.constants.BAN_TTL * 1000) this.banReportIds.delete(id);
    }
    if (this.banReportIds.has(idHex)) { this.#drop("banReplay"); return GOSSIP_IGNORE; }
    const verdict = this.validator.validateBan(record, { fromPeer, nowMillis });
    if (!verdict.ok) { this.#drop(`banRule${verdict.rule}`); return GOSSIP_IGNORE; }
    this.banReportIds.set(idHex, nowMillis);
    const hashHex = toHex(record.targetHash16);
    // Testimony about us decides nothing here — and must not be relayed
    // by its own target, who is the last peer with a neutral view of it.
    if (hashHex === toHex(banPeerHash16(this.id))) return GOSSIP_IGNORE;
    let entry = this.banLedger.get(hashHex);
    if (!entry) {
      if (this.banLedger.size >= this.constants.BAN_TARGET_CAP) {
        let oldestKey = null;
        let oldestAt = Infinity;
        for (const [key, candidate] of this.banLedger) {
          if (candidate.firstMillis < oldestAt) { oldestAt = candidate.firstMillis; oldestKey = key; }
        }
        if (oldestKey) this.banLedger.delete(oldestKey);
      }
      entry = { accusers: new Map(), firstMillis: nowMillis, corroborated: false, appliedTo: new Set() };
      this.banLedger.set(hashHex, entry);
    }
    for (const [accuser, at] of entry.accusers) {
      if (nowMillis - at > this.constants.BAN_TTL * 1000) entry.accusers.delete(accuser);
    }
    entry.accusers.set(fromPeer ?? "local", nowMillis);
    this.stats.bansAccepted++;
    if (entry.accusers.size >= this.constants.BAN_MIN_SOURCES) {
      if (!entry.corroborated) {
        entry.corroborated = true;
        this.stats.bansCorroborated++;
      }
      const target = this.peerHashIndex.get(hashHex);
      if (target && !entry.appliedTo.has(target)) {
        this.trust.penalizeRemoteBan(target);
        entry.appliedTo.add(target);
      }
    }
    return GOSSIP_ACCEPT;
  }

  // --- Sync stream server (§4.5) ----------------------------------------

  onStream(payload, fromPeer, nowMillis = this.clock()) {
    let message;
    try {
      message = decodeAny(payload);
    } catch {
      return null;
    }
    switch (message.kind) {
      case "getDigest": {
        const zone = { x: message.zoneX, y: message.zoneY };
        // If the requester told us its fold and ours matches, the whole
        // digest would say nothing — answer in 12 bytes instead of tens
        // of kilobytes. This is where the measured 4–5× anti-entropy
        // cost actually goes.
        if (message.have) {
          const mine = this.store.zoneFold(zone);
          if (mine.count === message.have.count && foldsEqual(mine.fold, message.have.fold)) {
            this.stats.antiEntropyElided++;
            return encodePMN1({ epochPrefix8: this.epochPrefix8 });
          }
        }
        const digest = this.store.digestForZone(zone, nowMillis);
        return encodePMD1({ epochPrefix8: this.epochPrefix8, ...digest });
      }
      case "getCells": {
        // Unknown/empty cells answer with zero counts — a responder never
        // distinguishes "not tracked" from "no data" (§4.5).
        const cells = this.store.snapshotForCells(message.cells);
        return encodePMS1({ epochPrefix8: this.epochPrefix8, cells });
      }
      case "forward": {
        this.forwarder.handle(message, { fromPeer, nowMillis });
        return null;
      }
      default:
        // §1 says unknown magics are ignored rather than errors — but the
        // thread channel's PMR1 arrives here on transports that do not
        // separate the two protocols, and dropping it would mean no
        // catch-up at all. Offered to whoever wired that channel up; still
        // ignored when nobody has.
        return this.onOtherStream?.(payload, fromPeer, nowMillis) ?? null;
    }
  }

  // --- Snapshot fetching with §11.3 privacy batching ---------------------

  /** Fetches cells from the mesh with padding/decoys/shuffle/split. */
  async fetchCells(wanted, { nowMillis = this.clock() } = {}) {
    const peers = this.network.peersOf(this.id);
    if (!wanted.length || !peers.length) return 0;
    for (const cell of wanted) this.visitedCells.set(`${cell.x}/${cell.y}`, cell);
    if (this.visitedCells.size > 4096) {
      for (const key of [...this.visitedCells.keys()].slice(0, this.visitedCells.size - 2048)) {
        this.visitedCells.delete(key);
      }
    }
    const requests = buildCellRequests({
      wanted,
      peers,
      decoyPool: buildDecoyPool(wanted, [...this.visitedCells.values()], this.constants.ENDPOINT_RINGS),
      constants: this.constants,
      rng: this.rng
    });
    this.stats.cellsWanted += wanted.length;
    // Requests are already split across distinct peers, so issuing them
    // concurrently is both faster and closer to the privacy intent: the
    // point of splitting is that no peer sees the ordered corridor, and
    // serializing would have handed one peer its slice, waited a full
    // round trip, then handed the next — turning a route query on a real
    // WAN into seconds of blocking inside provider.fetch().
    const responses = await Promise.all(requests.map(request => {
      this.stats.cellsRequested += request.cells.length;
      return Promise.resolve(this.network.request(
        this.id,
        request.peer,
        encodePMQ1({ epochPrefix8: this.epochPrefix8, cells: request.cells })
      )).catch(() => null);
    }));
    let merged = 0;
    for (const [index, response] of responses.entries()) {
      if (response) merged += this.mergeSnapshot(response, requests[index].peer, nowMillis);
    }
    for (const cell of wanted) this.cellRefreshedAt.set(detailCellKey(cell), nowMillis);
    return merged;
  }

  /**
   * Provider-triggered refresh: the same padded fetch, minus cells whose
   * snapshot is younger than one aggregation bucket. The privacy rules
   * apply to what is actually requested; suppressing a redundant request
   * removes bytes, not cover (decoys still pad whatever remains).
   */
  async refreshCells(cells, { nowMillis = this.clock() } = {}) {
    const stale = cells.filter(cell => {
      const last = this.cellRefreshedAt.get(detailCellKey(cell));
      return last == null || nowMillis - last >= this.constants.BUCKET_SECONDS * 1000;
    });
    if (!stale.length) return 0;
    if (this.cellRefreshedAt.size > 8192) this.cellRefreshedAt.clear();
    return this.fetchCells(stale, { nowMillis });
  }

  /**
   * §11.2: adopt locally-computed candidate routes as the corridor cache
   * target — subscribe to the z9 zones they cross, then fill their z15
   * cells through the padded fetch. `routes` are routes as
   * `engine.route({ alternatives })` returns them.
   *
   * Cells come from `cellOf` over the routes' own edges whenever the
   * routes carry them, and only fall back to rasterizing the geometry
   * when they do not. The two are not the same set: `cellOf` is the cell
   * a *record* lands in, and every shipped host derives it from the
   * leaf's centre rather than from where the road runs, so a corridor
   * rasterized from geometry names cells nothing is stored under and
   * zones nobody publishes to. Whatever cellOf says is, by construction,
   * where to look.
   */
  async followCorridor(routes, { nowMillis = this.clock(), corridorMeters = 250 } = {}) {
    const list = (Array.isArray(routes) ? routes : [routes]).filter(Boolean);
    const byEdge = new Map();
    for (const route of list) {
      for (const edge of route.edges || []) {
        if (!edge?.segment) continue;
        const { leafCell, geomRef } = parseSegment(edge.segment);
        const cell = this.cellOf({ leafCell, geomRef });
        if (cell) byEdge.set(detailCellKey(cell), cell);
      }
    }
    const cells = byEdge.size
      ? [...byEdge.values()]
      : corridorCells({ routes: list, corridorMeters, constants: this.constants });
    if (!cells.length) return 0;
    const zones = new Map();
    for (const cell of cells) {
      const zone = { x: cell.x >> 6, y: cell.y >> 6 };
      zones.set(`${zone.x}/${zone.y}`, zone);
    }
    this.subscribeZones([...zones.values()], nowMillis);
    return this.fetchCells(cells, { nowMillis });
  }

  /** Merges a PMS1 (solicited, so per-peer token buckets do not apply). */
  mergeSnapshot(payload, fromPeer, nowMillis = this.clock()) {
    let snapshot;
    try {
      snapshot = decodeAny(payload);
    } catch {
      return 0;
    }
    if (snapshot.kind !== "cellSnapshots") return 0;
    let accepted = 0;
    for (const cell of snapshot.cells) {
      for (const record of cell.records) {
        // fromPeer null skips the rate limiter (a snapshot is one pulled
        // response, not a stream to throttle), but the §5.4 vouch is the
        // provider: a proofless record in a snapshot is only as good as
        // the bond of the peer serving it.
        const verdict = this.validator.validateContribution(record, { store: this.store, fromPeer: null, vouchPeer: fromPeer, nowMillis });
        if (!verdict.ok) continue;
        const result = this.store.addContribution(record, { nowMillis, deliveredBy: fromPeer });
        if (result.added) {
          accepted++;
          // Same contract as the gossip path: every rule passed and the
          // record entered the store. A §11.6 read-only node has no other
          // way in — it joins no topic — so a gateway whose upstream side
          // is read-only (the `--bridge=in` fleet seed, §12.1) would
          // otherwise have nothing to hand its island.
          this.onRecordAccepted?.(record, { fromPeer, nowMillis, viaSnapshot: true });
        }
      }
      for (const record of cell.incidents) {
        const verdict = this.validator.validateIncident(record, { store: this.store, fromPeer: null, vouchPeer: fromPeer, nowMillis });
        if (!verdict.ok) continue;
        const result = this.store.addIncident(record, {
          nowMillis,
          deliveredBy: fromPeer,
          scoreOf: key => this.#scoreOf(key, nowMillis)
        });
        if (result.added) {
          accepted++;
          this.onRecordAccepted?.(record, { fromPeer, nowMillis, viaSnapshot: true });
        }
      }
    }
    this.stats.snapshotsMerged++;
    this.stats.snapshotRecordsAccepted += accepted;
    return accepted;
  }

  // --- Anti-entropy (§11.4) ----------------------------------------------

  async antiEntropyWith(peerId, zone, nowMillis = this.clock()) {
    this.stats.antiEntropyRounds++;
    const response = await this.network.request(
      this.id,
      peerId,
      encodePMG1({
        epochPrefix8: this.epochPrefix8,
        zoneX: zone.x,
        zoneY: zone.y,
        have: this.store.zoneFold(zone)
      })
    );
    if (!response) return 0;
    let remote;
    try {
      remote = decodeAny(response);
    } catch {
      return 0;
    }
    // The peer folded to the same 12 bytes we did: nothing to repair.
    if (remote.kind === "zonesAgree") {
      this.stats.antiEntropyAgreed++;
      return 0;
    }
    if (remote.kind !== "digest") return 0;
    const local = this.store.digestForZone(zone, nowMillis);
    const wanted = diffDigest(local, remote);
    if (!wanted.length) return 0;
    return this.fetchCells(wanted, { nowMillis });
  }

  /** One maintenance tick: TTL sweep plus one jittered anti-entropy round. */
  async tick(nowMillis = this.clock()) {
    this.store.sweep(nowMillis);
    if (this.zones.length) this.subscribeZones(this.zones, nowMillis);
    const peers = this.network.peersOf(this.id);
    if (peers.length && this.zones.length) {
      const peer = peers[Math.floor(this.rng() * peers.length)];
      const zone = this.zones[Math.floor(this.rng() * this.zones.length)];
      await this.antiEntropyWith(peer, zone, nowMillis);
    }
  }
}

// --- In-process loopback network (phase 1 / M2) --------------------------

/**
 * Synchronous in-memory transport implementing the network interface:
 * gossip topics with full fan-out, direct request/response streams, and
 * per-node byte accounting. The M4 simulation harness provides the
 * latency/churn/loss variant with a virtual clock; this one is for tests
 * and the loopback demo.
 */
export function createLoopbackNetwork({ clock = Date.now } = {}) {
  const nodes = new Map();
  const topics = new Map(); // topic -> Set(nodeId)
  const bytes = new Map();  // nodeId -> { gossipIn, gossipOut, streamIn, streamOut, messages }

  function counters(id) {
    let entry = bytes.get(id);
    if (!entry) {
      entry = { gossipIn: 0, gossipOut: 0, streamIn: 0, streamOut: 0, messages: 0 };
      bytes.set(id, entry);
    }
    return entry;
  }

  return {
    register(node) {
      nodes.set(node.id, node);
      counters(node.id);
    },
    subscribe(nodeId, topic) {
      let set = topics.get(topic);
      if (!set) { set = new Set(); topics.set(topic, set); }
      set.add(nodeId);
    },
    unsubscribe(nodeId, topic) {
      const set = topics.get(topic);
      if (set) { set.delete(nodeId); if (!set.size) topics.delete(topic); }
    },
    publish(topic, payload, fromId) {
      counters(fromId).gossipOut += payload.length;
      counters(fromId).messages++;
      const set = topics.get(topic);
      if (!set) return;
      for (const nodeId of set) {
        if (nodeId === fromId) continue;
        counters(nodeId).gossipIn += payload.length;
        nodes.get(nodeId)?.onGossip(topic, payload, fromId, clock());
      }
    },
    async request(fromId, toId, payload) {
      const target = nodes.get(toId);
      if (!target) return null;
      counters(fromId).streamOut += payload.length;
      counters(toId).streamIn += payload.length;
      const response = target.onStream(payload, fromId, clock());
      if (response) {
        counters(toId).streamOut += response.length;
        counters(fromId).streamIn += response.length;
      }
      return response;
    },
    /**
     * The thread channel's own protocol (threads §5.5). In one process
     * there is nothing to separate, so it lands on the same node — but
     * the seam exists here too, because a responder on this protocol
     * serves sealed bytes it cannot open and never consults a bond,
     * which is a different contract from the traffic sync stream.
     */
    async requestThread(fromId, toId, payload) {
      const target = nodes.get(toId);
      if (!target?.onOtherStream) return null;
      counters(fromId).streamOut += payload.length;
      counters(toId).streamIn += payload.length;
      const response = target.onOtherStream(payload, fromId, clock());
      if (response) {
        counters(toId).streamOut += response.length;
        counters(fromId).streamIn += response.length;
      }
      return response;
    },
    /**
     * Proof-of-delivery blobs (threads §20.7): PMTF in, PMTB out. A
     * third seam rather than a magic on `onOtherStream`, because a
     * catch-up responder answers for anyone's thread out of a shared
     * relay cache and a photo responder answers only for runs it is
     * itself publishing.
     */
    async requestPhoto(fromId, toId, payload) {
      const target = nodes.get(toId);
      if (!target?.onPhotoStream) return null;
      counters(fromId).streamOut += payload.length;
      counters(toId).streamIn += payload.length;
      const response = target.onPhotoStream(payload, fromId, clock());
      if (response) {
        counters(toId).streamOut += response.length;
        counters(fromId).streamIn += response.length;
      }
      return response;
    },
    peersOf(nodeId) {
      return [...nodes.keys()].filter(id => id !== nodeId);
    },
    schedule(fn, delayMs) {
      return setTimeout(fn, delayMs);
    },
    counters(nodeId) {
      return counters(nodeId);
    },
    nodes
  };
}

// --- §4.7 signed bootstrap ------------------------------------------------

/**
 * Canonical JSON: UTF-8, object keys sorted lexicographically at every
 * level, no whitespace. The Ed25519 signature covers the canonical
 * encoding of the object with `signature` removed.
 */
export function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value && typeof value === "object") {
    const keys = Object.keys(value).sort();
    return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

/**
 * Verifies mesh-bootstrap.json. Ed25519 through WebCrypto, the same path
 * the thread channel uses, so one implementation covers Node, browsers,
 * and mobile hosts — and a browser bundle of this module pulls in no
 * Node built-ins.
 *
 * A bootstrap whose key or signature does not verify MUST be discarded
 * and the mesh treated as unavailable; the router keeps working on the
 * static metric by contract.
 */
export async function verifyBootstrap(bootstrap, expectedPublicKeyHex) {
  if (!bootstrap || bootstrap.format !== "pulsemesh-bootstrap-v1") return { ok: false, reason: "format" };
  if (!/^[0-9a-f]{64}$/.test(bootstrap.epoch || "")) return { ok: false, reason: "epoch" };
  if (bootstrap.publicKey !== expectedPublicKeyHex) return { ok: false, reason: "unexpected key" };
  const { signature, ...unsigned } = bootstrap;
  if (!/^[0-9a-f]{128}$/.test(signature || "")) return { ok: false, reason: "signature shape" };
  const ok = await verifyThread(
    utf8Bytes(canonicalJson(unsigned)),
    fromHex(signature),
    fromHex(bootstrap.publicKey)
  );
  return ok ? { ok: true } : { ok: false, reason: "bad signature" };
}

/** Signs a bootstrap object with a raw 32-byte Ed25519 seed (tests, ops). */
export async function signBootstrap(unsigned, privateSeedHex) {
  const signature = await signThread(utf8Bytes(canonicalJson(unsigned)), fromHex(privateSeedHex));
  return { ...unsigned, signature: toHex(signature) };
}
