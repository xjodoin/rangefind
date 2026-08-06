// PulseMesh in the browser, for the OSM demo.
//
// Two modes, both real protocol bytes through the real store, validator,
// aggregation and provider:
//
//   "local"  — peers inside this tab over an in-memory network, fed by
//              simulated vehicles crawling a corridor you pick. No
//              infrastructure at all, so the demo works from `file://`
//              and from a static host. This is a demonstration of the
//              mechanism, and the UI says so.
//   "keeper" — a real libp2p host over WebSockets to a keeper, joining
//              the actual mesh. Needs a keeper multiaddr and the
//              transport bundle; falls back to "local" when absent.
//
// What is *not* faked in either mode: contributions go through
// createContributor (snap → quality → speed bin → cadence), cross a
// MeshNode's gossip path, get validated by the twelve rules, aggregate by
// weighted median, and reach the router as an ordinary
// LiveTrafficProvider. The router does not know which mode it is in.

import {
  DEFAULT_CONSTANTS,
  MeshNode,
  createContributor,
  createLoopbackNetwork,
  detailCellForE7,
  parseSegment,
  zoneOfDetailCell
} from "./pulsemesh.browser.js";

const CONTRIBUTOR_COUNT = 3;
// Proof-of-work is a per-emission cost; 20 bits is ~0.6 s of one core and
// would stall a UI thread. The demo runs the loopback transport, where
// proofType 0 is legal (protocol §4.1) and no admission bond is needed.
// EMIT_INTERVAL is effectively disabled so a simulated drive can cover a
// corridor in one tick; cadence is the contributor's own rule and is
// covered by its tests, not by this demo.
const DEMO_CONSTANTS = { ...DEFAULT_CONSTANTS, EMIT_INTERVAL: 0.01, BOND_BIRTHDAY_BITS: 24 };

/**
 * Wraps a route-graph engine with a PulseMesh instance.
 *
 * `engine` is the already-open route graph; its `root.sourceHash` is the
 * mesh epoch, so live states are bound to exactly this index build.
 */
export function createPulseMeshDemo({ engine, mode = "local", keeperAddress = null } = {}) {
  const epochHex = engine.root.sourceHash;
  // Deterministic, shared cell placement: the z15 cell of the leaf's bbox
  // centre. A pure function of the record and the static index, so every
  // peer derives the same cell without coordinating.
  const cellOf = record => {
    const bbox = engine.root.leaves[record.leafCell]?.bbox;
    if (!bbox) return null;
    return detailCellForE7((bbox.minLat + bbox.maxLat) / 2, (bbox.minLon + bbox.maxLon) / 2);
  };
  const cellContext = leafCell => (engine.root.leaves[leafCell]
    ? { polylineCount: 1 << 20, classOf: () => "secondary", metersOf: () => null }
    : null);

  let network = null;
  let host = null;
  let consumer = null;
  let contributors = [];
  let activeMode = null;
  // Static length per wire segment key. A contribution carries `meters`
  // so a receiver can turn a speed bin back into a time; without it the
  // engine has a speed and no way to cost it, and silently applies
  // nothing (`states: 15, applied: 0`).
  const metersBySegKey = new Map();
  const state = { records: 0, segments: 0, peers: 0, error: null };

  async function startLocal() {
    network = createLoopbackNetwork();
    const options = { epochHex, constants: DEMO_CONSTANTS, cellOf, cellContext, network, transport: "loopback" };
    consumer = new MeshNode({ id: "you", ...options });
    contributors = Array.from({ length: CONTRIBUTOR_COUNT }, (_, i) =>
      new MeshNode({ id: `vehicle-${i + 1}`, ...options }));
    activeMode = "local";
  }

  async function startKeeper(address) {
    // The transport bundle is large and only needed for this path, so it
    // loads on demand — a page that never joins the real mesh never pays
    // for libp2p.
    const { createPulseMeshHost, createLibp2pNetwork } = await import("./pulsemesh-libp2p.browser.js");
    host = await createPulseMeshHost({ profile: "browser", bootstrapPeers: [address] });
    network = createLibp2pNetwork(host);
    await network.ready;
    consumer = new MeshNode({
      id: host.peerId.toString(), epochHex, constants: DEFAULT_CONSTANTS,
      cellOf, cellContext, network, transport: "wire"
    });
    contributors = [];
    activeMode = "keeper";
  }

  /** Starts the mesh. Returns the mode actually achieved. */
  async function start() {
    if (activeMode) return activeMode;
    if (mode === "keeper" && keeperAddress) {
      try {
        await startKeeper(keeperAddress);
        return activeMode;
      } catch (error) {
        // A keeper that is down must not break the page: the whole
        // contract is that the router keeps working without live data.
        state.error = `keeper unreachable (${error?.message || error}); using in-tab peers`;
      }
    }
    await startLocal();
    return activeMode;
  }

  /**
   * Subscribes every peer to the z9 zones a route crosses. The corridor
   * is computed here, in the page — it never leaves the device in order,
   * and only the zone (≈55 km) is ever visible to a peer.
   */
  function followRoute(route) {
    if (!consumer || !route?.edges?.length) return [];
    const zones = new Map();
    for (const edge of route.edges) {
      const { leafCell, geomRef } = parseSegment(edge.segment);
      if (Number.isFinite(edge.meters)) metersBySegKey.set(`${leafCell}/${geomRef}`, Math.round(edge.meters));
      const cell = cellOf({ leafCell, geomRef });
      if (!cell) continue;
      const zone = zoneOfDetailCell(cell);
      zones.set(`${zone.x}/${zone.y}`, zone);
    }
    const list = [...zones.values()];
    for (const node of [consumer, ...contributors]) node.subscribeZones(list);
    return list;
  }

  /**
   * Simulates vehicles crawling `segments`, one PMC1 per vehicle per
   * segment, through the real contributor pipeline. Local mode only —
   * on the real mesh the traffic comes from real drivers.
   */
  async function simulateJam(segments, { speedMps = 2.2 } = {}) {
    if (activeMode !== "local" || !segments?.length) return 0;
    let published = 0;
    for (const [index, node] of contributors.entries()) {
      const contributor = createContributor({
        epoch32: node.epoch32,
        epochPrefix8: node.epochPrefix8,
        snap: async fix => fix.match,
        publish: async result => {
          published++;
          node.publishRecord(result.record);
        },
        proofType: 0,
        constants: DEMO_CONSTANTS,
        metersOf: key => metersBySegKey.get(key) || 0
      });
      let fixMillis = Date.now();
      for (const segment of segments) {
        // Advance enough to clear the cadence gate but stay inside
        // MAX_FUTURE_SKEW, or receivers reject the records as future-dated.
        fixMillis += 20;
        await contributor.handleFix({
          match: { segment, distMeters: 2 + index, ratio: 0.5, snappedLatE7: 0, snappedLonE7: 0 },
          speedMps,
          nowMillis: fixMillis
        });
      }
    }
    refreshState();
    return published;
  }

  function refreshState() {
    if (!consumer) return state;
    state.records = consumer.store.size();
    state.segments = consumer.provider.aggregates().size;
    state.peers = network?.peersOf ? network.peersOf(consumer.id).length : 0;
    return state;
  }

  /** The live-traffic provider to hand to `engine.route({ live })`. */
  function provider() {
    return consumer?.provider ?? null;
  }

  /** Aggregated segments a caller can paint on the map. */
  function jammedSegments({ maxSpeedKmh = 20 } = {}) {
    if (!consumer) return [];
    const out = [];
    for (const [segKey, aggregate] of consumer.provider.aggregates()) {
      if (aggregate.speedKmh > maxSpeedKmh) continue;
      const [leafCell, geomRef] = segKey.split("/").map(Number);
      out.push({
        segment: `${leafCell}/${geomRef >>> 1}/${geomRef & 1}`,
        speedKmh: aggregate.speedKmh,
        reports: aggregate.n,
        confidence: aggregate.confidence
      });
    }
    return out;
  }

  async function stop() {
    if (network?.close) await network.close();
    if (host?.stop) await host.stop().catch(() => {});
    network = null; host = null; consumer = null; contributors = []; activeMode = null;
    state.records = 0; state.segments = 0; state.peers = 0;
  }

  return {
    start, stop, followRoute, simulateJam, provider, jammedSegments, refreshState,
    get mode() { return activeMode; },
    get state() { return state; },
    get epoch() { return epochHex; }
  };
}
