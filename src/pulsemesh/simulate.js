// Simulated corridor traffic, for demos and for exercising a host.
//
// A live-traffic layer is hard to see working: it needs other drivers.
// This produces them — a handful of vehicles crawling a real route on the
// real graph — so a page or an app can be driven end to end with no
// keeper, no peers, and no network at all.
//
// Nothing here is a shortcut through the protocol. Each vehicle is a full
// mesh session with its own contributor; every observation is encoded,
// gossiped, validated against the static index and aggregated by weighted
// median before it reaches anything. The only thing simulated is where
// the vehicles are and how fast they are going, which is exactly the part
// a real deployment gets from real phones.

import { DEFAULT_CONSTANTS } from "./bins.js";
import { parseSegment } from "./codec.js";
import { createMeshSession } from "./session.js";

/** Free-flow speed of a class, when the index has nothing better. */
const CLASS_FREEFLOW_KMH = Object.freeze({
  motorway: 110, motorway_link: 70, trunk: 95, trunk_link: 60,
  primary: 75, primary_link: 50, secondary: 60, secondary_link: 45,
  tertiary: 50, tertiary_link: 40, residential: 35, unclassified: 40, service: 20
});

/**
 * Vehicles crawling a corridor.
 *
 * - `network`: the transport the vehicles publish on — a loopback network
 *   for a self-contained demo, any other for a testbed.
 * - `span`: records each vehicle emits per tick. Rule 7's token bucket
 *   refills at RATE_SUSTAINED (2/s) per delivering peer, so a vehicle that
 *   dumped a whole corridor every tick would have half its records dropped
 *   by its neighbours as a flood — which is the rule working correctly,
 *   and a waste of everyone's time. Eight over a four-second tick is
 *   exactly sustainable.
 * - `samples`: how finely the route is sampled. A long route is thousands
 *   of edges and an aggregate needs dozens, not thousands.
 */
export async function createCorridorTraffic({
  engine,
  network,
  constants = DEFAULT_CONSTANTS,
  count = 3,
  span = 8,
  samples = 48,
  jamStart = 0.32,
  jamLength = 0.14,
  idPrefix = "vehicle",
  clock = Date.now,
  transport = "loopback"
} = {}) {
  const vehicles = [];
  for (let i = 0; i < count; i++) {
    vehicles.push(await createMeshSession({
      engine,
      network,
      constants,
      transport,
      clock,
      id: `${idPrefix}-${i + 1}`,
      profile: "cadence",
      contribute: true,
      // The simulation already knows which segment each vehicle is on, so
      // it hands the match over instead of round-tripping through snap().
      // Everything after the match is the real pipeline.
      snap: async fix => fix.match
    }));
  }

  let corridor = [];
  let cursors = [];

  /** Samples a route into the stretch the vehicles will drive. */
  async function follow(route) {
    corridor = [];
    const routes = (Array.isArray(route) ? route : [route]).filter(Boolean);
    for (const vehicle of vehicles) await vehicle.followRoute(routes);
    const edges = routes[0]?.edges || [];
    if (!edges.length) return 0;
    const step = Math.max(1, Math.floor(edges.length / samples));
    const jamFrom = Math.floor(edges.length * jamStart);
    const jamTo = Math.floor(edges.length * (jamStart + jamLength));
    for (let i = 0; i < edges.length; i += step) {
      const edge = edges[i];
      if (!edge?.segment) continue;
      let point = null;
      try {
        point = await engine.locate(edge.segment, 0.5);
      } catch {
        continue;
      }
      const { leafCell, geomRef } = parseSegment(edge.segment);
      const facts = vehicles[0]?.cellContext(leafCell) ?? null;
      const roadClass = facts?.classOf(geomRef) || "";
      corridor.push({
        segment: edge.segment,
        lat: point.lat,
        lon: point.lon,
        freeflowKmh: facts?.freeflowKmhOf(geomRef) || CLASS_FREEFLOW_KMH[roadClass] || 50,
        jammed: i >= jamFrom && i < jamTo
      });
    }
    // Spread the vehicles around the corridor so every stretch is observed
    // by all of them within a couple of ticks, well inside CONTRIB_TTL —
    // corroboration is what turns observations into an aggregate the
    // router will act on.
    cursors = vehicles.map((_, index) => Math.floor(corridor.length * index / Math.max(1, vehicles.length)));
    return corridor.length;
  }

  /**
   * Advances every vehicle by one stretch and has it report what it drove
   * through: free-flowing where nothing is wrong, crawling through the jam.
   */
  async function tick({ nowMillis = clock() } = {}) {
    if (!corridor.length) return 0;
    let published = 0;
    for (const [index, vehicle] of vehicles.entries()) {
      let millis = nowMillis - (vehicles.length - index) * 300;
      const stretch = [];
      for (let step = 0; step < span; step++) {
        stretch.push(corridor[(cursors[index] + step) % corridor.length]);
      }
      cursors[index] = (cursors[index] + span) % corridor.length;
      for (const sample of stretch) {
        millis += 30;
        // Vehicles disagree slightly, which is what the weighted median is
        // for: three identical numbers would prove nothing.
        const speedKmh = sample.jammed
          ? Math.max(2, 6 * (0.85 + index * 0.11))
          : sample.freeflowKmh * (0.82 + index * 0.06);
        const result = await vehicle.onLocation({
          lat: sample.lat,
          lon: sample.lon,
          speedMps: speedKmh / 3.6,
          nowMillis: millis,
          match: {
            segment: sample.segment,
            distMeters: 3 + index,
            ratio: 0.5,
            snappedLatE7: Math.round(sample.lat * 1e7),
            snappedLonE7: Math.round(sample.lon * 1e7)
          }
        });
        if (result.emitted) published++;
      }
    }
    return published;
  }

  function stop() {
    for (const vehicle of vehicles) vehicle.stop();
    corridor = [];
    cursors = [];
  }

  return {
    vehicles,
    follow,
    tick,
    stop,
    get corridor() { return corridor; },
    get jamCount() { return corridor.filter(sample => sample.jammed).length; }
  };
}
