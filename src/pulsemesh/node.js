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
//   network.peersOf(nodeId) -> peerId[]

import { DEFAULT_CONSTANTS, detailCellKey, topicWindowFromMillis } from "./bins.js";
import {
  MAGIC,
  decodeAny,
  encodePMB1,
  encodePMD1,
  encodePMF1,
  encodePMG1,
  encodePMN1,
  encodePMQ1,
  encodePMS1
} from "./codec.js";
import { PulseMeshStore } from "./store.js";
import { TrustLedger } from "./aggregate.js";
import { createValidator } from "./validate.js";
import { createForwardHandler } from "./forward.js";
import { createPulseMeshProvider } from "./provider.js";
import { scoreIncidentKey } from "./incidents.js";
import { buildCellRequests, buildDecoyPool, corridorCells, diffDigest } from "./sync.js";
import { parseTopic, topicForCell, topicsForZones, windowAcceptable } from "./topics.js";
import { fromHex } from "./sha256.js";

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
    keeper = false
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
    // §11.5 consumer overlap: for EPOCH_OVERLAP after handover a consumer
    // also subscribes to, and accepts, the previous epoch's topics. It
    // emits on the current epoch only — contributors switch immediately.
    this.previousEpochPrefix16hex = previousEpochHex ? previousEpochHex.slice(0, 16) : null;
    this.epochOverlapUntil = previousEpochHex ? clock() + constants.EPOCH_OVERLAP * 1000 : null;
    this.trust = new TrustLedger({ constants, clock });
    this.store = new PulseMeshStore({ constants, cellOf, trustOf: peer => this.trust.get(peer) });
    this.validator = createValidator({
      constants,
      epoch32: this.epoch32,
      previousEpoch32: previousEpochHex ? fromHex(previousEpochHex) : null,
      previousEpochUntilMillis: this.epochOverlapUntil,
      cellOf,
      cellContext,
      transport,
      suppressedTypes,
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

  // --- Gossip receipt (§6) ----------------------------------------------

  onGossip(topicName, payload, fromPeer, nowMillis = this.clock()) {
    const topic = parseTopic(topicName);
    if (!topic || topic.reserved) return;
    if (!this.#acceptsEpochTopic(topic.epochPrefix16hex, nowMillis)) return;
    if (payload.length > this.constants.MAX_GOSSIP_BYTES) { this.#drop("oversize"); return; }
    if (!windowAcceptable(topic.window, nowMillis)) { this.#drop("window"); return; }
    let message;
    try {
      message = decodeAny(payload);
    } catch {
      this.#drop("rule1");
      return;
    }
    if (message.kind === "batch") {
      for (const record of message.records) this.#acceptContribution(record, fromPeer, topic, nowMillis);
    } else if (message.kind === "incident") {
      this.#acceptIncident(message, fromPeer, topic, nowMillis);
    } else {
      this.#drop("unexpected-" + (message.magic || message.kind));
    }
  }

  #acceptContribution(record, fromPeer, topic, nowMillis) {
    const verdict = this.validator.validateContribution(record, { store: this.store, fromPeer, topic, nowMillis });
    if (!verdict.ok) {
      if (verdict.trustPenalty) this.trust.penalizeValidation(fromPeer);
      this.#drop(`rule${verdict.rule}`);
      return;
    }
    const result = this.store.addContribution(record, { nowMillis, deliveredBy: fromPeer });
    if (result.added) this.stats.gossipAccepted++;
    else this.#drop(result.reason);
  }

  #acceptIncident(record, fromPeer, topic, nowMillis) {
    const verdict = this.validator.validateIncident(record, { store: this.store, fromPeer, topic, nowMillis });
    if (!verdict.ok) {
      if (verdict.trustPenalty) this.trust.penalizeValidation(fromPeer);
      this.#drop(`rule${verdict.rule}`);
      return;
    }
    const result = this.store.addIncident(record, {
      nowMillis,
      deliveredBy: fromPeer,
      scoreOf: key => this.#scoreOf(key, nowMillis)
    });
    if (result.added) this.stats.gossipAccepted++;
    else this.#drop(result.reason);
  }

  #drop(reason) {
    this.stats.gossipDropped++;
    this.stats.dropsByRule[reason] = (this.stats.dropsByRule[reason] || 0) + 1;
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
        return null; // unknown magics are ignored, not errors (§1)
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
   * cells through the padded fetch. `routes` are geometries as
   * `engine.route({ alternatives })` returns them.
   */
  async followCorridor(routes, { nowMillis = this.clock(), corridorMeters = 250 } = {}) {
    const cells = corridorCells({ routes, corridorMeters, constants: this.constants });
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
        const verdict = this.validator.validateContribution(record, { store: this.store, fromPeer: null, nowMillis });
        if (!verdict.ok) continue;
        const result = this.store.addContribution(record, { nowMillis, deliveredBy: fromPeer });
        if (result.added) accepted++;
      }
      for (const record of cell.incidents) {
        const verdict = this.validator.validateIncident(record, { store: this.store, fromPeer: null, nowMillis });
        if (!verdict.ok) continue;
        const result = this.store.addIncident(record, {
          nowMillis,
          deliveredBy: fromPeer,
          scoreOf: key => this.#scoreOf(key, nowMillis)
        });
        if (result.added) accepted++;
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

// --- §4.6 signed bootstrap ------------------------------------------------

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

const ED25519_SPKI_PREFIX = "302a300506032b6570032100";
const ED25519_PKCS8_PREFIX = "302e020100300506032b657004220420";

/**
 * Verifies mesh-bootstrap.json (Node hosts; browsers use WebCrypto with
 * the same canonical bytes). A bootstrap whose key or signature does not
 * verify MUST be discarded and the mesh treated as unavailable — the
 * router keeps working on the static metric by contract.
 */
export async function verifyBootstrap(bootstrap, expectedPublicKeyHex) {
  if (!bootstrap || bootstrap.format !== "pulsemesh-bootstrap-v1") return { ok: false, reason: "format" };
  if (!/^[0-9a-f]{64}$/.test(bootstrap.epoch || "")) return { ok: false, reason: "epoch" };
  if (bootstrap.publicKey !== expectedPublicKeyHex) return { ok: false, reason: "unexpected key" };
  const { signature, ...unsigned } = bootstrap;
  if (!/^[0-9a-f]{128}$/.test(signature || "")) return { ok: false, reason: "signature shape" };
  const { createPublicKey, verify } = await import("node:crypto");
  const key = createPublicKey({
    key: Buffer.from(ED25519_SPKI_PREFIX + bootstrap.publicKey, "hex"),
    format: "der",
    type: "spki"
  });
  const ok = verify(null, Buffer.from(canonicalJson(unsigned), "utf8"), key, Buffer.from(signature, "hex"));
  return ok ? { ok: true } : { ok: false, reason: "bad signature" };
}

/** Signs a bootstrap object with a raw 32-byte Ed25519 seed (tests, ops). */
export async function signBootstrap(unsigned, privateSeedHex) {
  const { createPrivateKey, sign } = await import("node:crypto");
  const key = createPrivateKey({
    key: Buffer.from(ED25519_PKCS8_PREFIX + privateSeedHex, "hex"),
    format: "der",
    type: "pkcs8"
  });
  const signature = sign(null, Buffer.from(canonicalJson(unsigned), "utf8"), key);
  return { ...unsigned, signature: signature.toString("hex") };
}
