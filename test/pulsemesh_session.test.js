// The engine-bound mesh session: the integration boundary every host
// (the Android app, the web demo, a keeper) sits on.
//
// What is worth pinning here is exactly what each host used to get wrong
// on its own — the static index answering the validator instead of a
// stand-in, contributions carrying a real length, incidents landing where
// they were reported, and a traffic layer that only draws what the router
// itself would believe.

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createMeshSession, congestionLevel } from "../src/pulsemesh/session.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";
import { CLASS_SPEED_CAP_KMH } from "../src/pulsemesh/validate.js";
import { decodePMC1 } from "../src/pulsemesh/codec.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";
// Cadence out of the way: these tests drive a corridor in one tick, and
// the emission cadence is the contributor's own rule with its own tests.
const FAST = Object.freeze({ ...DEFAULT_CONSTANTS, EMIT_INTERVAL: 0.001 });

async function graphFixture(t) {
  try {
    return await openRouteGraphDir(GRAPH_DIR);
  } catch {
    t.skip(`route graph fixture missing at ${GRAPH_DIR}`);
    return null;
  }
}

const centreOf = (engine, leaf) => {
  const bbox = engine.root.leaves[leaf].bbox;
  return { lat: (bbox.minLat + bbox.maxLat) / 2 / 1e7, lon: (bbox.minLon + bbox.maxLon) / 2 / 1e7 };
};

/** A route long enough to have a corridor, from the real fixture graph. */
async function someRoute(engine, minEdges = 12) {
  for (let attempt = 0; attempt < 40; attempt++) {
    const from = centreOf(engine, (attempt * 7) % engine.root.leaves.length);
    const to = centreOf(engine, (attempt * 7 + 11) % engine.root.leaves.length);
    try {
      const route = await engine.route({ from, to });
      if ((route.edges || []).length >= minEdges && route.geometry?.length) return { route, from, to };
    } catch {
      continue;
    }
  }
  return null;
}

test("the static index answers rules 10-12, instead of a stand-in", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const session = await createMeshSession({
    engine, network: createLoopbackNetwork(), id: "reader", transport: "loopback"
  });

  const found = await someRoute(engine);
  assert.ok(found, "the fixture graph has a routable pair");
  await session.followRoute(found.route);

  const [edge] = found.route.edges;
  const [leaf, polyline, direction] = edge.segment.split("/").map(Number);
  const context = session.cellContext(leaf);
  assert.ok(context, "following a corridor warms its leaves' facts");

  // The stand-in this replaces claimed 1<<20 polylines, which made rule
  // 11 unfireable, and a class of "secondary" for every road on earth.
  assert.ok(context.polylineCount > 0 && context.polylineCount < 1 << 20);
  assert.equal(
    (polyline >>> 0) < context.polylineCount, true,
    "a segment the router just used exists in its own leaf"
  );

  const geomRef = polyline * 2 + direction;
  const roadClass = context.classOf(geomRef);
  assert.ok(typeof roadClass === "string" && roadClass.length > 0, `class resolved (${roadClass})`);
  assert.ok(
    Object.prototype.hasOwnProperty.call(CLASS_SPEED_CAP_KMH, roadClass) || roadClass.endsWith("_link") ||
      ["residential", "unclassified", "living_street", "track", "service"].includes(roadClass),
    `the class is one the validator's table knows about (${roadClass})`
  );

  // Length and free-flow speed both come from the same edge the router
  // costed, so a contribution's `meters` cannot disagree with rule 10.
  const staticMeters = context.metersOf(geomRef);
  assert.ok(staticMeters > 0, "the segment has a static length");
  const geometry = await engine.geometryOf(edge.segment);
  assert.ok(
    Math.abs(geometry.meters - staticMeters) <= staticMeters * 0.05,
    `drawn geometry and costed length agree (${geometry.meters.toFixed(1)} vs ${staticMeters})`
  );
  const freeflow = context.freeflowKmhOf(geomRef);
  assert.ok(freeflow > 0 && freeflow < 200, `free-flow speed is plausible (${freeflow?.toFixed(1)} km/h)`);
});

test("contributions carry the segment's real length, so receivers can cost them", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const network = createLoopbackNetwork();
  const found = await someRoute(engine);
  assert.ok(found);

  const emitted = [];
  const driver = await createMeshSession({
    engine, network, id: "driver", profile: "cadence", contribute: true,
    constants: FAST, transport: "loopback",
    onEmit: emission => { emitted.push(emission); }
  });
  const reader = await createMeshSession({
    engine, network, id: "reader", constants: FAST, transport: "loopback"
  });
  await driver.followRoute(found.route);
  await reader.followRoute(found.route);

  // Drive the corridor. The first fix in a leaf may still carry meters 0
  // — the facts warm behind it — so walk far enough to leave that behind.
  const geometry = found.route.geometry;
  let now = Date.now();
  for (let i = 0; i < geometry.length && emitted.length < 8; i++) {
    const [lat, lon] = geometry[i];
    now += 1200;
    await driver.onLocation({ lat, lon, speedMps: 3.5, nowMillis: now });
  }
  assert.ok(emitted.length > 0, `the driver published (${emitted.length})`);

  const withLength = emitted
    .map(emission => decodePMC1(emission.record.bytes))
    .filter(record => record.meters > 0);
  assert.ok(
    withLength.length > 0,
    "records carry a length — without one a receiver has a speed it cannot turn into a time"
  );
  for (const record of withLength) {
    const context = reader.cellContext(record.leafCell);
    if (!context) continue;
    const staticMeters = context.metersOf(record.geomRef);
    assert.ok(
      record.meters >= staticMeters * 0.8 && record.meters <= staticMeters * 1.2,
      `the length a peer publishes survives its neighbours' rule 10 (${record.meters} vs ${staticMeters})`
    );
  }
  assert.equal(reader.snapshot().gossip.dropsByRule.rule10 ?? 0, 0, "nothing was dropped as implausible");
  assert.ok(reader.node.store.size() > 0, "and the reader holds what was published");
});

test("a jam shows up on the map and in the route, or in neither", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const network = createLoopbackNetwork();
  const found = await someRoute(engine, 20);
  assert.ok(found);

  const reader = await createMeshSession({
    engine, network, id: "reader", constants: FAST, transport: "loopback"
  });
  await reader.followRoute(found.route);

  // Three vehicles crawling the same stretch: below AGG_MIN_REPORTS
  // there is no aggregate to draw, which is the point.
  const stretch = found.route.edges.slice(4, 12);
  const drivers = [];
  for (let i = 0; i < 3; i++) {
    drivers.push(await createMeshSession({
      engine, network, id: `car-${i}`, profile: "cadence", contribute: true,
      constants: FAST, transport: "loopback"
    }));
    await drivers[i].followRoute(found.route);
  }
  let now = Date.now();
  for (const driver of drivers) {
    for (const edge of stretch) {
      now += 20;
      const point = await engine.locate(edge.segment, 0.5);
      await driver.onLocation({ ...point, speedMps: 1.4, nowMillis: now });
    }
  }

  const traffic = await reader.traffic({ nowMillis: now });
  assert.ok(traffic.length > 0, `the reader can draw the jam (${traffic.length} segments)`);
  for (const entry of traffic) {
    assert.ok(entry.points.length >= 2, "every drawn segment is a line, not a point");
    assert.ok(entry.reports >= DEFAULT_CONSTANTS.AGG_HINT_REPORTS,
      "nothing is drawn that the router would not believe");
    assert.equal(entry.level, congestionLevel(entry.ratio));
  }
  const slow = traffic.filter(entry => entry.level === "stopped" || entry.level === "heavy");
  assert.ok(slow.length > 0, `the crawl reads as congestion (${traffic.map(e => e.level).join(",")})`);
  assert.ok(slow[0].freeflowKmh > slow[0].speedKmh, "against the static free-flow speed of that road");

  // The same store, through the ordinary provider contract, moves a route.
  const live = await engine.route({ from: found.from, to: found.to, live: reader.provider() });
  assert.equal(live.live.provider, "pulsemesh");
  assert.ok(live.live.applied > 0, `live states reached the router (applied ${live.live.applied})`);
});

test("a reader can still report an incident, and it lands where it was reported", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const network = createLoopbackNetwork();
  const found = await someRoute(engine);
  assert.ok(found);

  const reporter = await createMeshSession({
    engine, network, id: "reporter", constants: FAST, transport: "loopback"
  });
  const reader = await createMeshSession({
    engine, network, id: "reader", constants: FAST, transport: "loopback"
  });
  await reporter.followRoute(found.route);
  await reader.followRoute(found.route);

  // Contribution is off: this device publishes no measurements at all.
  assert.equal(reporter.contributing, false);
  const where = await engine.locate(found.route.edges[3].segment, 0.4);
  const now = Date.now();
  await reporter.onLocation({ ...where, speedMps: 8, nowMillis: now });
  assert.equal(reporter.stats.emitted, 0, "and it stays that way");

  // §10.4: a report the user was never told is public is not minted.
  const refused = await reporter.reportIncident({ type: 1, nowMillis: now });
  assert.equal(refused.emitted, false);
  assert.match(refused.reason, /disclosure/);

  const filed = await reporter.reportIncident({ type: 1, acknowledgedPublic: true, nowMillis: now });
  assert.equal(filed.emitted, true, `the report was minted (${filed.reason ?? ""})`);

  const shown = await reader.incidents({ nowMillis: now });
  assert.equal(shown.length, 1, "the neighbour sees it");
  assert.equal(shown[0].typeName, "crash");
  assert.equal(shown[0].tier, "hint", "one report is a hint, not a fact — §8.5 needs corroboration");
  const metres = Math.hypot(
    (shown[0].lat - where.lat) * 111_320,
    (shown[0].lon - where.lon) * 111_320 * Math.cos(where.lat * Math.PI / 180)
  );
  assert.ok(metres < 60, `it is pinned where it was reported (${metres.toFixed(0)} m away)`);
});

test("a read-only session consumes without ever publishing (§11.6)", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const network = createLoopbackNetwork();
  const session = await createMeshSession({
    engine, network, id: "home", readOnly: true, transport: "loopback"
  });
  assert.equal(session.snapshot().readOnly, true);
  assert.throws(() => session.setContributing(true), /read-only/);
  const refused = await session.reportIncident({ type: 1, acknowledgedPublic: true });
  assert.equal(refused.emitted, false);
  assert.match(refused.reason, /read-only/);
  await assert.rejects(
    () => createMeshSession({ engine, network, id: "x", readOnly: true, contribute: true }),
    /mutually exclusive/
  );
});
