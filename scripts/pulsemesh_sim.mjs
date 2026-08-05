// PulseMesh phase-2 simulation harness (protocol §14, milestones M4–M5).
//
// A virtual-time, event-driven mesh: N MeshNode peers over a simulated
// gossip/stream transport with latency, loss, clock skew, and churn;
// vehicles driving a synthetic road world through the real contributor
// pipeline; every byte on the wire is a real protocol payload through the
// real validator, store, and aggregation.
//
//   node scripts/pulsemesh_sim.mjs --scenario=all [--fast] [--json]
//   node scripts/pulsemesh_sim.mjs --scenario=convergence|bandwidth|churn|flood|reticent|scale
//
// Scenarios and what they measure:
//   convergence  time until every peer holds identical zone digests
//   bandwidth    bytes/peer/minute by message type vs contributor density
//   churn        record survival when peers vanish mid-TTL
//   flood        hostile load: acceptance (must be 0) and defender CPU
//   reticent     M5 — jam-detection latency and coverage vs the cadence
//                profile, plus the trajectory-reconstruction attack
//                (recovered-route fraction, anonymity set per record)
//   scale        per-peer cost as the mesh grows (the flat-cost claim)

import { performance } from "node:perf_hooks";
import { DEFAULT_CONSTANTS, detailCellKey, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode } from "../src/pulsemesh/node.js";
import { createContributor } from "../src/pulsemesh/contribute.js";
import { createReticentProfile } from "../src/pulsemesh/reticent.js";
import { aggregateSegment } from "../src/pulsemesh/aggregate.js";
import { encodePMB1, encodePMC1, encodePMD1, decodePMC1, minePow } from "../src/pulsemesh/codec.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

// --- CLI ------------------------------------------------------------------

const args = Object.fromEntries(process.argv.slice(2).map(arg => {
  const [key, value] = arg.replace(/^--/, "").split("=");
  return [key, value ?? true];
}));
const SCENARIO = args.scenario || "all";
const FAST = Boolean(args.fast);
const AS_JSON = Boolean(args.json);
const EPOCH_HEX = toHex(sha256Utf8("pulsemesh-sim-epoch"));
const BASE_MS = 1754265600000;
const results = {};

function log(...parts) {
  if (!AS_JSON) console.log(...parts);
}

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

// --- Virtual-time scheduler ----------------------------------------------

class Scheduler {
  constructor() {
    this.now = BASE_MS;
    this.heap = [];
    this.seq = 0;
  }
  at(timeMs, fn) {
    const event = { time: timeMs, seq: this.seq++, fn };
    const heap = this.heap;
    heap.push(event);
    let i = heap.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (heap[parent].time < event.time || (heap[parent].time === event.time && heap[parent].seq < event.seq)) break;
      heap[i] = heap[parent];
      i = parent;
    }
    heap[i] = event;
  }
  after(delayMs, fn) {
    this.at(this.now + Math.max(0, delayMs), fn);
  }
  #pop() {
    const heap = this.heap;
    const top = heap[0];
    const last = heap.pop();
    if (heap.length) {
      const before = (a, b) => a.time < b.time || (a.time === b.time && a.seq < b.seq);
      let i = 0;
      for (;;) {
        const left = 2 * i + 1;
        const right = left + 1;
        let best = -1;
        let bestEvent = last;
        if (left < heap.length && before(heap[left], bestEvent)) { best = left; bestEvent = heap[left]; }
        if (right < heap.length && before(heap[right], bestEvent)) { best = right; bestEvent = heap[right]; }
        if (best === -1) break;
        heap[i] = heap[best];
        i = best;
      }
      heap[i] = last;
    }
    return top;
  }
  async run(untilMs) {
    let processed = 0;
    while (this.heap.length && this.heap[0].time <= untilMs) {
      const event = this.#pop();
      this.now = Math.max(this.now, event.time);
      event.fn();
      // Let promise chains resolved by this event advance before the next.
      if (++processed % 32 === 0) await new Promise(resolve => setImmediate(resolve));
      else await Promise.resolve();
    }
    this.now = Math.max(this.now, untilMs);
    await new Promise(resolve => setImmediate(resolve));
  }
}

// --- Simulated transport --------------------------------------------------

const GOSSIP_MESH_DEGREE = 8;

class SimNetwork {
  constructor({ sim, rng, latencyMeanMs = 60, latencyJitterMs = 40, lossRate = 0 }) {
    this.sim = sim;
    this.rng = rng;
    this.latencyMeanMs = latencyMeanMs;
    this.latencyJitterMs = latencyJitterMs;
    this.lossRate = lossRate;
    this.nodes = new Map();
    this.topics = new Map();
    this.bytes = new Map();
    this.dead = new Set();
  }
  #latency() {
    return Math.max(2, this.latencyMeanMs + (this.rng() - 0.5) * 2 * this.latencyJitterMs);
  }
  counters(id) {
    let entry = this.bytes.get(id);
    if (!entry) {
      entry = { gossipIn: 0, gossipOut: 0, streamIn: 0, streamOut: 0, relayOut: 0, messagesIn: 0 };
      this.bytes.set(id, entry);
    }
    return entry;
  }
  register(node) {
    this.nodes.set(node.id, node);
    this.counters(node.id);
  }
  kill(nodeId) {
    this.dead.add(nodeId);
    for (const set of this.topics.values()) set.delete(nodeId);
  }
  subscribe(nodeId, topic) {
    if (this.dead.has(nodeId)) return;
    let set = this.topics.get(topic);
    if (!set) { set = new Set(); this.topics.set(topic, set); }
    set.add(nodeId);
  }
  unsubscribe(nodeId, topic) {
    const set = this.topics.get(topic);
    if (set) { set.delete(nodeId); if (!set.size) this.topics.delete(topic); }
  }
  publish(topic, payload, fromId) {
    if (this.dead.has(fromId)) return;
    const set = this.topics.get(topic);
    const receivers = set ? [...set].filter(id => id !== fromId && !this.dead.has(id)) : [];
    // GossipSub cost model: each receiver ingests one copy; the sender
    // pays mesh-degree copies and relays share the rest evenly.
    const senderCopies = Math.min(GOSSIP_MESH_DEGREE, receivers.length);
    this.counters(fromId).gossipOut += payload.length * senderCopies;
    const relayCopies = Math.max(0, receivers.length - senderCopies);
    receivers.forEach((id, index) => {
      if (relayCopies && index < relayCopies) this.counters(id).relayOut += payload.length;
    });
    const latency = this.#latency();
    this.sim.after(latency, () => {
      for (const id of receivers) {
        if (this.dead.has(id)) continue;
        if (this.rng() < this.lossRate) continue;
        const node = this.nodes.get(id);
        const counters = this.counters(id);
        counters.gossipIn += payload.length;
        counters.messagesIn++;
        node?.onGossip(topic, payload, fromId, node.clock());
      }
    });
  }
  request(fromId, toId, payload) {
    if (this.dead.has(fromId) || this.dead.has(toId)) return Promise.resolve(null);
    const target = this.nodes.get(toId);
    if (!target) return Promise.resolve(null);
    this.counters(fromId).streamOut += payload.length;
    this.counters(toId).streamIn += payload.length;
    const latency = this.#latency();
    return new Promise(resolve => {
      this.sim.after(latency, () => {
        if (this.dead.has(toId)) { resolve(null); return; }
        const response = target.onStream(payload, fromId, target.clock());
        if (!response) { resolve(null); return; }
        this.counters(toId).streamOut += response.length;
        this.counters(fromId).streamIn += response.length;
        this.sim.after(this.#latency(), () => resolve(this.dead.has(fromId) ? null : response));
      });
    });
  }
  peersOf(nodeId) {
    return [...this.nodes.keys()].filter(id => id !== nodeId && !this.dead.has(id) && !this.dead.has(nodeId));
  }
  schedule(fn, delayMs) {
    this.sim.after(delayMs, fn);
  }
}

// --- Synthetic road world -------------------------------------------------

// Intersection lattice; 500 m segments; each undirected segment lives in
// the z15 cell of its midpoint, one leaf per cell. Zone-aligned at
// (144·64, 180·64) so the whole world is one z9 zone.
const CELL_BASE_X = 144 * 64;
const CELL_BASE_Y = 180 * 64;
const SEGMENT_METERS = 500;
const INTERSECTIONS_PER_CELL = 2;
const FREEFLOW_KMH = 50;
const FREEFLOW_BIN = Math.floor(FREEFLOW_KMH / 5);
const JAM_KMH = 6;

class World {
  constructor({ gridSize = 12 }) {
    this.gridSize = gridSize;
    this.segments = [];
    this.byKey = new Map();
    this.atIntersection = new Map(); // "gx/gy" -> segment list
    const leafOfCell = new Map();
    this.leaves = [];
    const addToLeaf = cellKey => {
      let leaf = leafOfCell.get(cellKey);
      if (leaf == null) {
        leaf = this.leaves.length;
        leafOfCell.set(cellKey, leaf);
        this.leaves.push({ cellKey, polylines: 0 });
      }
      return leaf;
    };
    const addSegment = (ax, ay, bx, by) => {
      const cellX = CELL_BASE_X + Math.floor((ax + bx) / 2 / INTERSECTIONS_PER_CELL);
      const cellY = CELL_BASE_Y + Math.floor((ay + by) / 2 / INTERSECTIONS_PER_CELL);
      const cellKey = `${cellX}/${cellY}`;
      const leafCell = addToLeaf(cellKey);
      const polyline = this.leaves[leafCell].polylines++;
      const segment = {
        leafCell, polyline,
        cell: { x: cellX, y: cellY },
        a: { x: ax, y: ay }, b: { x: bx, y: by },
        keys: [`${leafCell}/${polyline * 2}`, `${leafCell}/${polyline * 2 + 1}`],
        jammed: false
      };
      this.segments.push(segment);
      for (const key of segment.keys) this.byKey.set(key, segment);
      for (const end of [segment.a, segment.b]) {
        const endKey = `${end.x}/${end.y}`;
        if (!this.atIntersection.has(endKey)) this.atIntersection.set(endKey, []);
        this.atIntersection.get(endKey).push(segment);
      }
    };
    for (let y = 0; y < gridSize; y++) {
      for (let x = 0; x < gridSize; x++) {
        if (x + 1 < gridSize) addSegment(x, y, x + 1, y);
        if (y + 1 < gridSize) addSegment(x, y, x, y + 1);
      }
    }
    this.cellOf = record => {
      const leaf = this.leaves[record.leafCell];
      if (!leaf) return null;
      const [x, y] = leaf.cellKey.split("/").map(Number);
      return { x, y };
    };
    this.cellContext = leafCell => {
      const leaf = this.leaves[leafCell];
      if (!leaf) return null;
      return {
        polylineCount: leaf.polylines,
        classOf: () => "secondary",
        metersOf: () => SEGMENT_METERS
      };
    };
    this.zone = zoneOfDetailCell({ x: CELL_BASE_X, y: CELL_BASE_Y });
  }
  speedKmhOn(segment) {
    return segment.jammed ? JAM_KMH : FREEFLOW_KMH;
  }
  adjacentKeys(segKey) {
    const segment = this.byKey.get(segKey);
    if (!segment) return [];
    const out = new Set(segment.keys);
    for (const end of [segment.a, segment.b]) {
      for (const other of this.atIntersection.get(`${end.x}/${end.y}`) || []) {
        for (const key of other.keys) out.add(key);
      }
    }
    return [...out];
  }
}

// --- Vehicles -------------------------------------------------------------

class Vehicle {
  constructor({ id, world, rng, route = null, stops = [] }) {
    this.id = id;
    this.world = world;
    this.rng = rng;
    this.route = route;       // fixed segment list (courier) or null (random walk)
    this.routeIndex = 0;
    this.stops = stops;       // intersections where the courier dwells
    this.dwellUntil = 0;
    this.segment = route ? route[0] : world.segments[Math.floor(rng() * world.segments.length)];
    this.direction = 0;
    this.progress = 0;
    this.driven = [];         // ground truth: [{ segKey, enterMs }]
    this.#enter(this.segment, this.direction, 0);
  }
  #enter(segment, direction, nowMs) {
    this.segment = segment;
    this.direction = direction;
    this.progress = 0;
    this.driven.push({ segKey: segment.keys[direction].split("/").slice(0, 2).join("/"), segment, direction, enterMs: nowMs });
  }
  segKey() {
    return `${this.segment.leafCell}/${this.segment.polyline * 2 + this.direction}`;
  }
  speedMps(nowMs) {
    if (nowMs < this.dwellUntil) return 0;
    return this.world.speedKmhOn(this.segment) / 3.6;
  }
  step(dtSeconds, nowMs) {
    if (nowMs < this.dwellUntil) return;
    this.progress += this.speedMps(nowMs) * dtSeconds;
    if (this.progress < SEGMENT_METERS) return;
    // Arrived at the far intersection; dwell if it is a stop, then move on.
    const arrived = this.direction === 0 ? this.segment.b : this.segment.a;
    if (this.stops.some(stop => stop.x === arrived.x && stop.y === arrived.y)) {
      this.dwellUntil = nowMs + 60 * 1000;
    }
    if (this.route) {
      this.routeIndex = (this.routeIndex + 1) % this.route.length;
      const next = this.route[this.routeIndex];
      const direction = (next.a.x === arrived.x && next.a.y === arrived.y) ? 0 : 1;
      this.#enter(next, direction, nowMs);
      return;
    }
    const options = (this.world.atIntersection.get(`${arrived.x}/${arrived.y}`) || [])
      .filter(candidate => candidate !== this.segment);
    const next = options.length ? options[Math.floor(this.rng() * options.length)] : this.segment;
    const direction = (next.a.x === arrived.x && next.a.y === arrived.y) ? 0 : 1;
    this.#enter(next, direction, nowMs);
  }
  snapMatch() {
    return {
      segment: `${this.segment.leafCell}/${this.segment.polyline}/${this.direction}`,
      distMeters: 2 + this.rng() * 4,
      ratio: this.progress / SEGMENT_METERS,
      snappedLatE7: 0,
      snappedLonE7: 0,
      bearingDeg: undefined
    };
  }
}

// --- Mesh assembly --------------------------------------------------------

function buildMesh({
  sim, world, rng,
  consumers = 10,
  vehicles = 10,
  profile = "cadence",
  powDifficulty = 8,
  lossRate = 0.05,
  skewMs = 2000,
  latencyMeanMs = 60
}) {
  const constants = { ...DEFAULT_CONSTANTS, POW_DIFFICULTY: powDifficulty };
  const network = new SimNetwork({ sim, rng, lossRate, latencyMeanMs });
  const tap = []; // ground truth for every published record
  const nodes = [];
  const makeNode = (id, keeper = false) => {
    const skew = (rng() - 0.5) * 2 * skewMs;
    const node = new MeshNode({
      id,
      epochHex: EPOCH_HEX,
      constants,
      cellOf: world.cellOf,
      cellContext: world.cellContext,
      network,
      clock: () => Math.round(sim.now + skew),
      rng,
      transport: "wire",
      keeper
    });
    node.subscribeZones([world.zone], node.clock());
    nodes.push(node);
    return node;
  };
  const consumerNodes = Array.from({ length: consumers }, (_, i) => makeNode(`peer-${i}`));

  const cars = [];
  for (let i = 0; i < vehicles; i++) {
    const node = makeNode(`car-${i}`);
    const entry = { node, vehicle: new Vehicle({ id: `car-${i}`, world, rng }), contributor: null };
    const reticent = profile === "reticent"
      ? createReticentProfile({
          constants,
          clock: node.clock,
          rng,
          expectedBinOf: segKey => {
            const aggregate = aggregateSegment(node.store.contributionsForSegment(segKey), {
              nowMillis: node.clock(), constants
            });
            return aggregate && aggregate.confidence >= constants.SURPRISE_CONFIDENCE
              ? aggregate.speedBin
              : FREEFLOW_BIN;
          },
          companyCountOf: segKey => node.store.contributionsForSegment(segKey).length,
          forwarderPool: () => network.peersOf(node.id).filter(id => id !== node.id)
        })
      : "cadence";
    const contributor = createContributor({
      epoch32: node.epoch32,
      epochPrefix8: node.epochPrefix8,
      snap: async fix => fix.match,
      publish: async result => {
        const decoded = decodePMC1(result.record.bytes);
        tap.push({
          vehicle: entry.vehicle.id,
          segKey: `${decoded.leafCell}/${decoded.geomRef}`,
          bucket: decoded.timeBucket,
          reportId: toHex(decoded.reportId),
          publishedAt: sim.now,
          viaForwarder: result.forwarder != null
        });
        node.publishRecord(result.record, { forwarder: result.forwarder, nowMillis: node.clock() });
      },
      proofType: 1,
      clock: node.clock,
      constants,
      profile: reticent,
      metersOf: () => SEGMENT_METERS,
      classOf: () => "secondary"
    });
    entry.contributor = contributor;
    cars.push(entry);
  }
  return { constants, network, tap, consumerNodes, cars, nodes };
}

// Drives the world: vehicle fixes every 5 s, node ticks (TTL sweep +
// anti-entropy) every ANTI_ENTROPY_SECONDS with jitter.
function scheduleWorld({ sim, mesh, rng, minutes, fixSeconds = 5 }) {
  const endMs = BASE_MS + minutes * 60 * 1000;
  const tickNode = node => {
    if (mesh.network.dead.has(node.id)) return;
    node.tick(node.clock()).catch(() => {});
    sim.after((mesh.constants.ANTI_ENTROPY_SECONDS + (rng() - 0.5) * 6) * 1000, () => tickNode(node));
  };
  for (const node of mesh.nodes) sim.after(rng() * 10000, () => tickNode(node));
  const driveCar = car => {
    if (sim.now >= endMs) return;
    car.vehicle.step(fixSeconds, sim.now);
    car.contributor.handleFix({
      match: car.vehicle.snapMatch(),
      speedMps: car.vehicle.speedMps(sim.now),
      nowMillis: car.node.clock()
    }).catch(() => {});
    sim.after(fixSeconds * 1000, () => driveCar(car));
  };
  for (const car of mesh.cars) sim.after(rng() * fixSeconds * 1000, () => driveCar(car));
  return endMs;
}

function digestHex(node, zone) {
  return toHex(encodePMD1({ epochPrefix8: node.epochPrefix8, ...node.store.digestForZone(zone, node.clock()) }));
}

// --- Scenario: convergence ------------------------------------------------

async function runConvergence({ peers, vehicles, minutes = 2 }) {
  const sim = new Scheduler();
  const rng = lcg(1234 + peers);
  const world = new World({ gridSize: FAST ? 8 : 12 });
  const mesh = buildMesh({ sim, world, rng, consumers: peers, vehicles, lossRate: 0.15 });
  const endMs = scheduleWorld({ sim, mesh, rng, minutes });
  await sim.run(endMs);

  // Emissions stop; measure how long until all live consumers agree.
  const holdouts = () => {
    const reference = digestHex(mesh.consumerNodes[0], world.zone);
    return mesh.consumerNodes.filter(node => digestHex(node, world.zone) !== reference).length;
  };
  const stopMs = sim.now;
  let convergedAt = null;
  for (let step = 0; step < 120 && convergedAt == null; step++) {
    await sim.run(sim.now + 1000);
    if (holdouts() === 0) convergedAt = sim.now;
  }
  const records = mesh.consumerNodes[0].store.size();
  return {
    peers,
    vehicles,
    liveRecords: records,
    convergenceSeconds: convergedAt == null ? null : (convergedAt - stopMs) / 1000,
    lossRate: 0.15
  };
}

// --- Scenario: bandwidth --------------------------------------------------

async function runBandwidth({ peers, vehicles, minutes = 3 }) {
  const sim = new Scheduler();
  const rng = lcg(99 + vehicles);
  const world = new World({ gridSize: FAST ? 8 : 12 });
  const mesh = buildMesh({ sim, world, rng, consumers: peers, vehicles });
  const endMs = scheduleWorld({ sim, mesh, rng, minutes });
  await sim.run(endMs);
  const perConsumer = mesh.consumerNodes.map(node => mesh.network.counters(node.id));
  const mean = selector => perConsumer.reduce((sum, c) => sum + selector(c), 0) / perConsumer.length / minutes;
  const published = mesh.tap.length;
  return {
    peers,
    vehicles,
    minutes,
    recordsPublished: published,
    perPeerPerMinute: {
      gossipInBytes: Math.round(mean(c => c.gossipIn)),
      relayOutBytes: Math.round(mean(c => c.relayOut)),
      antiEntropyBytes: Math.round(mean(c => c.streamIn + c.streamOut)),
      totalKiB: ((mean(c => c.gossipIn + c.relayOut + c.streamIn + c.streamOut)) / 1024).toFixed(1)
    }
  };
}

// --- Scenario: churn ------------------------------------------------------

async function runChurn({ peers = 20, vehicles = 12, killFraction = 0.5 }) {
  const sim = new Scheduler();
  const rng = lcg(777);
  const world = new World({ gridSize: FAST ? 8 : 10 });
  const mesh = buildMesh({ sim, world, rng, consumers: peers, vehicles });
  const endMs = scheduleWorld({ sim, mesh, rng, minutes: 1.5 });
  await sim.run(endMs);

  // Union of live reportIds across all peers, then kill a fraction of
  // consumers AND stop all contributors (their nodes go too).
  const liveIds = new Set();
  for (const node of mesh.nodes) {
    for (const idHex of node.store.byId.keys()) liveIds.add(idHex);
  }
  const killed = mesh.consumerNodes.slice(0, Math.floor(peers * killFraction));
  for (const node of killed) mesh.network.kill(node.id);
  for (const { node } of mesh.cars) mesh.network.kill(node.id);
  const survivors = mesh.consumerNodes.slice(Math.floor(peers * killFraction));

  // A record survives if some survivor still holds it while it is within
  // TTL. Sample immediately after the churn event.
  await sim.run(sim.now + 5000);
  const nowMs = sim.now;
  let withinTtl = 0;
  let survived = 0;
  for (const idHex of liveIds) {
    let inTtl = false;
    let held = false;
    for (const node of survivors) {
      const entry = node.store.byId.get(idHex);
      if (entry) {
        held = true;
        if (entry.expiresAt > nowMs) inTtl = true;
      }
    }
    if (inTtl || held) withinTtl++;
    if (held) survived++;
  }

  // And a late joiner pulls everything back from survivors alone.
  const late = new MeshNode({
    id: "late-joiner",
    epochHex: EPOCH_HEX,
    constants: mesh.constants,
    cellOf: world.cellOf,
    cellContext: world.cellContext,
    network: mesh.network,
    clock: () => Math.round(sim.now),
    rng,
    transport: "wire"
  });
  late.subscribeZones([world.zone], late.clock());
  // Anti-entropy is a repair loop: each padded fetch splits cells across
  // random peers, so several rounds converge on the survivor union.
  // The rounds await simulated round-trips, so the clock has to keep
  // advancing until they settle — otherwise the last in-flight request
  // has no event left to deliver it.
  let done = false;
  const tickPromise = (async () => {
    for (let round = 0; round < 6; round++) {
      for (const survivor of survivors) {
        await late.antiEntropyWith(survivor.id, world.zone, late.clock());
      }
    }
    done = true;
  })();
  for (let step = 0; step < 600 && !done; step++) await sim.run(sim.now + 1000);
  await tickPromise;
  const survivorUnion = new Set();
  for (const node of survivors) {
    for (const idHex of node.store.byId.keys()) {
      if (node.store.byId.get(idHex).expiresAt > sim.now) survivorUnion.add(idHex);
    }
  }
  let lateHeld = 0;
  for (const idHex of survivorUnion) if (late.store.byId.has(idHex)) lateHeld++;

  return {
    peers,
    killedPeers: killed.length + mesh.cars.length,
    liveRecordsBeforeChurn: liveIds.size,
    survivedOnRemainingPeers: survived,
    survivalFraction: Number((survived / Math.max(1, liveIds.size)).toFixed(3)),
    lateJoinerRecovered: lateHeld,
    lateJoinerRecoveredFraction: Number((lateHeld / Math.max(1, survivorUnion.size)).toFixed(3))
  };
}

// --- Scenario: flood ------------------------------------------------------

async function runFlood({ peers = 10, vehicles = 6, hostilePerSecond = 200, minutes = 1 }) {
  const sim = new Scheduler();
  const rng = lcg(4242);
  const world = new World({ gridSize: 8 });
  const mesh = buildMesh({ sim, world, rng, consumers: peers, vehicles, lossRate: 0 });
  const target = mesh.consumerNodes[0];

  // The attacker publishes through the real gossip path: malformed
  // records, replays, missing/invalid PoW, and (the expensive case)
  // records with VALID proof-of-work at full spam rate.
  const attacker = new MeshNode({
    id: "attacker",
    epochHex: EPOCH_HEX,
    constants: mesh.constants,
    cellOf: world.cellOf,
    cellContext: world.cellContext,
    network: mesh.network,
    clock: () => Math.round(sim.now),
    rng,
    transport: "wire"
  });
  const epoch32 = attacker.epoch32;
  let validSpam = null;
  const makeHostile = kind => {
    const fields = {
      epochPrefix8: attacker.epochPrefix8,
      leafCell: Math.floor(rng() * world.leaves.length),
      geomRef: Math.floor(rng() * 8),
      timeBucket: Math.floor(sim.now / 15000),
      speedBin: 10,
      qualityBin: 7,
      meters: SEGMENT_METERS,
      ttlSeconds: 90,
      reportId: Uint8Array.from({ length: 16 }, () => Math.floor(rng() * 256)),
      proofType: 1,
      proof: new Uint8Array(8)
    };
    switch (kind) {
      case "malformed":
        fields.speedBin = 200;
        return encodePMC1(fields).bytes;
      case "no-pow":
        return encodePMC1(fields).bytes;
      case "replay":
        if (!validSpam) return null;
        return validSpam;
      case "valid-pow": {
        const { preimage } = encodePMC1(fields);
        fields.proof = minePow(preimage, epoch32, mesh.constants.POW_DIFFICULTY).nonce;
        const bytes = encodePMC1(fields).bytes;
        validSpam = bytes;
        return bytes;
      }
    }
  };
  const kinds = ["malformed", "no-pow", "replay", "valid-pow"];
  const endMs = scheduleWorld({ sim, mesh, rng, minutes });
  let hostileSent = 0;
  const attack = () => {
    if (sim.now >= endMs) return;
    for (let i = 0; i < hostilePerSecond; i++) {
      const bytes = makeHostile(kinds[i % kinds.length]);
      if (!bytes) continue;
      hostileSent++;
      // Deliver straight to the target's gossip handler on a legal topic,
      // batched exactly as a real publisher would send it.
      target.onGossip(
        `/rangefind/pulsemesh/1/${EPOCH_HEX.slice(0, 16)}/${world.zone.x}/${world.zone.y}/${Math.floor(sim.now / 1000 / 300)}/0`,
        encodePMB1([bytes]),
        "attacker",
        target.clock()
      );
    }
    sim.after(1000, attack);
  };
  sim.after(1000, attack);

  const cpuStart = performance.now();
  await sim.run(endMs);
  const cpuMs = performance.now() - cpuStart;

  const honest = new Set(mesh.tap.map(entry => entry.reportId));
  let hostileStored = 0;
  for (const idHex of target.store.byId.keys()) {
    if (!honest.has(idHex)) hostileStored++;
  }
  return {
    hostileSent,
    hostileAcceptedBeyondRateLimit: hostileStored,
    hostileAcceptanceNote: "valid-PoW spam is bounded by the per-peer token bucket; everything else is rejected outright",
    rateLimitCeiling: `${mesh.constants.RATE_SUSTAINED}/s sustained, burst ${mesh.constants.RATE_BURST}`,
    dropsByRule: target.stats.dropsByRule,
    honestAcceptedDuringAttack: target.stats.gossipAccepted - hostileStored,
    defenderCpuMsTotal: Math.round(cpuMs),
    attackerPowCostPerRecordMs: "2^difficulty hashes each (see pulsemesh_bench.mjs)"
  };
}

// --- Scenario: reticent (M5) ---------------------------------------------

function buildCourierRoute(world) {
  // A serpentine over the grid — the worst case for trajectory privacy.
  const route = [];
  const size = world.gridSize;
  for (let y = 0; y < size - 1; y += 2) {
    for (let x = 0; x + 1 < size; x++) route.push(findSegment(world, x, y, x + 1, y));
    route.push(findSegment(world, size - 1, y, size - 1, y + 1));
    for (let x = size - 1; x > 0; x--) route.push(findSegment(world, x, y + 1, x - 1, y + 1));
    if (y + 2 < size) route.push(findSegment(world, 0, y + 1, 0, y + 2));
  }
  return route.filter(Boolean);
}

function findSegment(world, ax, ay, bx, by) {
  return (world.atIntersection.get(`${ax}/${ay}`) || []).find(segment =>
    (segment.a.x === ax && segment.a.y === ay && segment.b.x === bx && segment.b.y === by) ||
    (segment.a.x === bx && segment.a.y === by && segment.b.x === ax && segment.b.y === ay)
  );
}

// The trajectory-reconstruction attack (§14 M5): chain records by segment
// adjacency and bucket timing; a step is taken only when it is
// unambiguous (exactly one candidate continuation). Returns the longest
// correctly-recovered run of the courier's emissions and the mean
// anonymity set per courier record.
function trajectoryAttack({ tap, world, courierId }) {
  const byBucket = new Map();
  for (const record of tap) {
    if (!byBucket.has(record.bucket)) byBucket.set(record.bucket, []);
    byBucket.get(record.bucket).push(record);
  }
  // The adversary steps to the *earliest* bucket holding a spatially
  // plausible continuation, and is blocked only when that bucket offers
  // more than one — this is what makes a lone contributor's chain unique
  // and a crowd's chain ambiguous.
  const nextStep = (record, used) => {
    const nearKeys = new Set(world.adjacentKeys(record.segKey));
    for (let bucket = record.bucket + 1; bucket <= record.bucket + 8; bucket++) {
      const candidates = (byBucket.get(bucket) || [])
        .filter(other => other !== record && !used.has(other) && nearKeys.has(other.segKey));
      if (candidates.length) return { candidates, bucket };
    }
    return { candidates: [], bucket: null };
  };

  const courierRecords = tap.filter(record => record.vehicle === courierId);
  const courierOrder = new Map(courierRecords.map((record, index) => [record, index]));
  let bestRun = 0;
  let bestRunSegments = new Set();
  let bestRunSeconds = 0;
  let anonymitySum = 0;
  for (const start of courierRecords) {
    anonymitySum += Math.max(1, nextStep(start, new Set()).candidates.length);
    const used = new Set([start]);
    const segments = new Set([start.segKey]);
    let run = 1;
    let current = start;
    for (;;) {
      const { candidates } = nextStep(current, used);
      if (candidates.length !== 1) break; // ambiguous, or the trail ends
      const next = candidates[0];
      if (next.vehicle !== courierId || courierOrder.get(next) !== courierOrder.get(current) + 1) break;
      used.add(next);
      segments.add(next.segKey);
      run++;
      current = next;
    }
    if (segments.size > bestRunSegments.size) {
      bestRunSegments = segments;
      bestRunSeconds = (current.publishedAt - start.publishedAt) / 1000;
    }
    bestRun = Math.max(bestRun, run);
  }
  return {
    courierEmissions: courierRecords.length,
    longestRecoveredRun: bestRun,
    recoveredSegments: bestRunSegments.size,
    recoveredSpanSeconds: Math.round(bestRunSeconds),
    meanAnonymitySet: courierRecords.length ? Number((anonymitySum / courierRecords.length).toFixed(2)) : null
  };
}

async function runReticent({ profile, background = 24, minutes = 6, seed = 31337 }) {
  const sim = new Scheduler();
  const rng = lcg(seed);
  const world = new World({ gridSize: FAST ? 8 : 10 });
  const mesh = buildMesh({ sim, world, rng, consumers: 6, vehicles: background, profile, lossRate: 0.05 });

  // The courier: fixed serpentine route with dwell stops, always through
  // the profile under test.
  const courierNode = mesh.cars[0].node;
  const route = buildCourierRoute(world);
  const stops = [route[3], route[9], route[15]].filter(Boolean).map(segment => segment.b);
  const courier = new Vehicle({ id: "courier", world, rng, route, stops });
  mesh.cars[0].vehicle = courier;

  // Jam a corridor mid-run and measure detection latency at a consumer.
  const jamSegments = [];
  const mid = Math.floor(world.gridSize / 2);
  for (let x = 0; x + 1 < world.gridSize; x++) {
    const segment = findSegment(world, x, mid, x + 1, mid);
    if (segment) jamSegments.push(segment);
  }
  const jamStartMs = BASE_MS + Math.floor(minutes * 60 * 1000 * 0.4);
  sim.at(jamStartMs, () => { for (const segment of jamSegments) segment.jammed = true; });

  const observer = mesh.consumerNodes[0];
  // Two thresholds, because they are not the same event. The provider
  // hands a state to the router at AGG_HINT_REPORTS (n = 2, confidence
  // capped at 0.30) — that is when a jam first *changes a route*.
  // AGG_MIN_REPORTS (n = 3) is when it becomes a full-confidence
  // aggregate. Measuring only the latter, as an earlier version of this
  // harness did, reports the channel as slower than the router it feeds.
  let detectedAt = null;      // n >= AGG_HINT_REPORTS: reaches the router
  let confirmedAt = null;     // n >= AGG_MIN_REPORTS: full confidence
  const watchJam = () => {
    if (sim.now >= jamStartMs && (detectedAt == null || confirmedAt == null)) {
      for (const segment of jamSegments) {
        for (const key of segment.keys) {
          const aggregate = aggregateSegment(observer.store.contributionsForSegment(key), {
            nowMillis: observer.clock(), constants: mesh.constants
          });
          if (!aggregate || aggregate.speedBin > 2) continue;
          if (detectedAt == null && aggregate.n >= mesh.constants.AGG_HINT_REPORTS) detectedAt = sim.now;
          if (confirmedAt == null && aggregate.n >= mesh.constants.AGG_MIN_REPORTS) confirmedAt = sim.now;
        }
      }
    }
    if (detectedAt == null || confirmedAt == null) sim.after(1000, watchJam);
  };
  sim.after(1000, watchJam);

  const endMs = scheduleWorld({ sim, mesh, rng, minutes });
  await sim.run(endMs);

  // Coverage: driven segments (last 90 s) that have a publishable aggregate.
  const drivenRecently = new Set();
  for (const car of mesh.cars) {
    for (const entry of car.vehicle.driven) {
      if (sim.now - entry.enterMs <= 90 * 1000) {
        for (const key of entry.segment.keys) drivenRecently.add(key);
      }
    }
  }
  let covered = 0;
  for (const key of drivenRecently) {
    const aggregate = aggregateSegment(observer.store.contributionsForSegment(key), {
      nowMillis: observer.clock(), constants: mesh.constants
    });
    if (aggregate && aggregate.n >= mesh.constants.AGG_HINT_REPORTS) covered++;
  }

  const attack = trajectoryAttack({ tap: mesh.tap, world, courierId: "courier" });
  const courierMinutes = minutes;
  // Ground truth: the distinct directed segments the courier actually drove.
  const courierSegments = new Set(courier.driven.map(entry => entry.segKey));
  return {
    profile,
    backgroundVehicles: background,
    recordsPublished: mesh.tap.length,
    emissionsPerVehiclePerMinute: Number((mesh.tap.length / mesh.cars.length / courierMinutes).toFixed(2)),
    courierEmissions: attack.courierEmissions,
    jamDetectionSeconds: detectedAt == null ? null : (detectedAt - jamStartMs) / 1000,
    jamConfirmedSeconds: confirmedAt == null ? null : (confirmedAt - jamStartMs) / 1000,
    coverageFraction: drivenRecently.size ? Number((covered / drivenRecently.size).toFixed(3)) : null,
    // Privacy (§14 M5). The comparable number across profiles is the
    // fraction of the courier's actually-driven route an adversary
    // reconstructs from the record set alone — not the fraction of its
    // emissions, whose denominator shrinks with the profile.
    courierSegmentsDriven: courierSegments.size,
    longestRecoveredRun: attack.longestRecoveredRun,
    recoveredSegments: attack.recoveredSegments,
    recoveredSpanSeconds: attack.recoveredSpanSeconds,
    recoveredRouteFraction: courierSegments.size
      ? Number((attack.recoveredSegments / courierSegments.size).toFixed(3))
      : null,
    meanAnonymitySet: attack.meanAnonymitySet
  };
}

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function summarizeReticentRuns(runs, { profile, background, seeds }) {
  const detections = runs.map(run => run.jamDetectionSeconds).filter(value => value != null);
  const confirmations = runs.map(run => run.jamConfirmedSeconds).filter(value => value != null);
  const numeric = key => runs.map(run => run[key]).filter(value => value != null);
  const round = (value, places = 3) => (value == null ? null : Number(value.toFixed(places)));
  return {
    profile,
    backgroundVehicles: background,
    seeds,
    recordsPublished: Math.round(median(numeric("recordsPublished"))),
    emissionsPerVehiclePerMinute: round(median(numeric("emissionsPerVehiclePerMinute")), 2),
    // A run where the jam was never detected is reported rather than
    // averaged away — "usually 20 s, sometimes never" is a different
    // claim from "20 s".
    jamDetectedInRuns: `${detections.length}/${runs.length}`,
    jamDetectionSecondsMedian: median(detections),
    jamDetectionSecondsRange: detections.length ? [Math.min(...detections), Math.max(...detections)] : null,
    jamConfirmedInRuns: `${confirmations.length}/${runs.length}`,
    jamConfirmedSecondsMedian: median(confirmations),
    coverageFraction: round(median(numeric("coverageFraction"))),
    courierSegmentsDriven: Math.round(median(numeric("courierSegmentsDriven"))),
    recoveredRouteFractionMedian: round(median(numeric("recoveredRouteFraction"))),
    recoveredRouteFractionWorst: round(Math.max(...numeric("recoveredRouteFraction"))),
    meanAnonymitySet: round(median(numeric("meanAnonymitySet")), 2)
  };
}

// --- Runner ---------------------------------------------------------------

async function main() {
  const started = performance.now();

  if (SCENARIO === "convergence" || SCENARIO === "all") {
    log("\n== convergence (15% gossip loss; anti-entropy repairs) ==");
    const rows = [];
    for (const peers of FAST ? [10, 25] : [10, 25, 50]) {
      const row = await runConvergence({ peers, vehicles: 10 });
      rows.push(row);
      log(`  peers=${row.peers} liveRecords=${row.liveRecords} convergence=${row.convergenceSeconds}s`);
    }
    results.convergence = rows;
  }

  if (SCENARIO === "bandwidth" || SCENARIO === "all") {
    log("\n== bandwidth per peer per minute vs contributor density ==");
    const rows = [];
    for (const vehicles of FAST ? [5, 20] : [5, 20, 60]) {
      const row = await runBandwidth({ peers: 15, vehicles });
      rows.push(row);
      log(`  vehicles=${vehicles} records=${row.recordsPublished} perPeer/min=${JSON.stringify(row.perPeerPerMinute)}`);
    }
    results.bandwidth = rows;
  }

  if (SCENARIO === "churn" || SCENARIO === "all") {
    log("\n== churn: half the mesh vanishes mid-TTL ==");
    const row = await runChurn({});
    results.churn = row;
    log(`  ${JSON.stringify(row, null, 2)}`);
  }

  if (SCENARIO === "flood" || SCENARIO === "all") {
    log("\n== flood: hostile records at 200/s against one peer ==");
    const row = await runFlood({});
    results.flood = row;
    log(`  ${JSON.stringify(row, null, 2)}`);
  }

  if (SCENARIO === "reticent" || SCENARIO === "all") {
    log("\n== M5: cadence vs reticent (utility and the trajectory attack) ==");
    const rows = [];
    // Density 1 is the spec's quiet street — the case where a cadence
    // chain is supposed to be unique and therefore fully reconstructible.
    // Jam-detection latency is noisy at a single seed and the profile
    // recommendation rests on it, so each cell is repeated and reported
    // as a median with its spread.
    const seeds = FAST ? [31337, 4242] : [31337, 4242, 90210, 5150, 777, 8675309, 271828, 141421, 161803, 302144, 998244, 604800];
    for (const background of FAST ? [1, 24] : [1, 4, 12, 32]) {
      for (const profile of ["cadence", "reticent"]) {
        const runs = [];
        for (const seed of seeds) {
          runs.push(await runReticent({ profile, background, minutes: FAST ? 6 : 10, seed }));
        }
        rows.push(summarizeReticentRuns(runs, { profile, background, seeds: seeds.length }));
        log(`  ${JSON.stringify(rows[rows.length - 1])}`);
      }
    }
    results.reticent = rows;
  }

  if (SCENARIO === "cost" || SCENARIO === "all") {
    log("\n== cost: what a centralized equivalent would have to serve ==");
    // Measured per-peer traffic at a realistic urban density, projected to
    // fleet sizes. The centralized column is the same live-traffic service
    // delivered by a server: every client polls for its corridor, so the
    // operator carries bytes × users, while the mesh operator carries the
    // static CDN assets it already publishes and nothing else.
    const measured = await runBandwidth({ peers: 30, vehicles: 30, minutes: 2 });
    const perPeerBytesPerMin = Number(measured.perPeerPerMinute.gossipInBytes)
      + Number(measured.perPeerPerMinute.relayOutBytes)
      + Number(measured.perPeerPerMinute.antiEntropyBytes);
    const rows = [];
    for (const users of [1e4, 1e6, 1e8]) {
      // Centralized: one corridor refresh per user per 30 s, ~40 KiB of
      // segment states each (a mid-size corridor) — the conventional
      // design PulseMesh replaces.
      const centralizedBytesPerMin = users * 2 * 40 * 1024;
      rows.push({
        users,
        meshOperatorEgressGiBPerMonth: 0,
        meshPerUserBytesPerMinute: Math.round(perPeerBytesPerMin),
        centralizedOperatorEgressGiBPerMonth: Math.round(centralizedBytesPerMin * 60 * 24 * 30 / 1024 ** 3),
        note: "mesh operator egress is zero because live state never touches an operator's server"
      });
    }
    results.cost = { measuredAt: measured, projections: rows };
    for (const row of rows) {
      log(`  users=${row.users.toExponential(0)} mesh=0 GiB/mo egress; centralized=${row.centralizedOperatorEgressGiBPerMonth.toLocaleString()} GiB/mo`);
    }
  }

  if (SCENARIO === "scale" || SCENARIO === "all") {
    log("\n== scale: per-peer cost as the mesh grows ==");
    const rows = [];
    for (const peers of FAST ? [10, 50] : [10, 50, 150]) {
      const row = await runBandwidth({ peers, vehicles: 20, minutes: 2 });
      rows.push(row);
      log(`  peers=${peers} perPeer/min=${JSON.stringify(row.perPeerPerMinute)}`);
    }
    results.scale = rows;
  }

  log(`\nsimulated in ${((performance.now() - started) / 1000).toFixed(1)} s wall-clock`);
  if (AS_JSON) console.log(JSON.stringify(results, null, 2));
}

await main();
