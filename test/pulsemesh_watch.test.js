// The corridor watch: when does the mesh changing under a drive justify
// re-planning it, and — much more of the work — when does it not.
//
// A route is priced once, at query time. The signal this adds is the
// missing half: the jam that forms after you set off. Everything worth
// pinning here is a refusal to fire — on a road merely confirmed clear,
// on records expiring, on jitter inside a level, or twice inside the
// debounce. A navigator that re-plans on any of those thrashes.

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createMeshSession } from "../src/pulsemesh/session.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createIngestPublisher } from "../src/pulsemesh/ingest.js";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";
import { parseSegment } from "../src/pulsemesh/codec.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";

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

async function someRoute(engine, minEdges = 20) {
  for (let attempt = 0; attempt < 40; attempt++) {
    const from = centreOf(engine, (attempt * 7) % engine.root.leaves.length);
    const to = centreOf(engine, (attempt * 7 + 11) % engine.root.leaves.length);
    try {
      const route = await engine.route({ from, to });
      if ((route.edges || []).length >= minEdges && route.geometry?.length) return route;
    } catch {
      continue;
    }
  }
  return null;
}

/** A session, a route on it, and the edges a reading can honestly land on. */
async function corridorFixture(engine, clock) {
  const network = createLoopbackNetwork({ clock });
  const session = await createMeshSession({
    engine, network, id: "driver", transport: "loopback", clock
  });
  const route = await someRoute(engine);
  if (!route) return null;
  await session.followRoute(route);

  const usable = [];
  const seen = new Set();
  for (const edge of route.edges || []) {
    if (!edge?.segment) continue;
    const { leafCell, geomRef } = parseSegment(edge.segment);
    const segKey = `${leafCell}/${geomRef}`;
    if (seen.has(segKey)) continue;
    const freeflowKmh = session.freeflowKmhOf(segKey);
    if (!(freeflowKmh > 0)) continue;
    let point;
    try {
      point = await engine.locate(edge.segment, 0.5);
    } catch {
      continue;
    }
    seen.add(segKey);
    usable.push({ segment: edge.segment, segKey, freeflowKmh, lat: point.lat, lon: point.lon });
    if (usable.length >= 4) break;
  }
  return { session, route, usable };
}

/** Publishes one reading and returns the publisher that did it. */
async function publish(engine, session, clock, observations, options = {}) {
  const ingest = createIngestPublisher({
    engine, session, clock, ...options,
    sources: [{ id: "seed", intervalSeconds: 1, fetch: async () => observations }]
  });
  await ingest.tick();
  return ingest;
}

test("the first check only establishes a baseline; it never announces the corridor", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const fixture = await corridorFixture(engine, clock);
  assert.ok(fixture, "the fixture graph has a usable corridor");
  const { session, route, usable } = fixture;
  assert.ok(usable.length >= 2);

  // A jam already present before the watch starts is the state of the
  // world, not a change in it.
  const jam = usable[0];
  await publish(engine, session, clock, [
    { kind: "flow", lat: jam.lat, lon: jam.lon, speedKmh: Math.max(3, jam.freeflowKmh * 0.1) }
  ]);

  const fired = [];
  const watch = session.watchRoute(route, { onChange: change => fired.push(change) });
  assert.equal(watch.check(now), null, "the baseline pass is silent");
  assert.equal(fired.length, 0);
  assert.equal(watch.check(now), null, "and nothing has changed since");
});

test("a jam forming on the corridor fires, with the segment that caused it", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { session, route, usable } = await corridorFixture(engine, clock);
  assert.ok(usable.length >= 1);

  const fired = [];
  const watch = session.watchRoute(route, { onChange: change => fired.push(change), debounceSeconds: 0 });
  watch.check(now);   // baseline: nothing known yet

  const jam = usable[0];
  await publish(engine, session, clock, [
    { kind: "flow", lat: jam.lat, lon: jam.lon, speedKmh: Math.max(3, jam.freeflowKmh * 0.1) }
  ]);

  const change = watch.check(now);
  assert.ok(change, "the forming jam is reported");
  assert.equal(fired.length, 1, "and the handler saw it once");
  assert.ok(change.worsened.length >= 1);
  assert.equal(change.improved.length, 0);
  const first = change.worsened[0];
  assert.equal(first.from, "unknown", "it is the first thing the mesh has said here");
  assert.ok(["heavy", "stopped"].includes(first.to), `a jam, not a ${first.to} road`);
});

test("a road merely confirmed clear is not news", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { session, route, usable } = await corridorFixture(engine, clock);
  assert.ok(usable.length >= 1);

  const fired = [];
  const watch = session.watchRoute(route, { onChange: change => fired.push(change), debounceSeconds: 0 });
  watch.check(now);

  // publishFreeFlow so the reading survives the publisher's own gate:
  // most of a corridor is uncovered most of the time, and the mesh
  // filling in "this road is fine" must never re-plan a drive.
  const clear = usable[0];
  const ingest = await publish(engine, session, clock, [
    { kind: "flow", lat: clear.lat, lon: clear.lon, speedKmh: clear.freeflowKmh * 0.98 }
  ], { publishFreeFlow: true });
  assert.ok(ingest.stats.publishedSegments > 0, "the free reading really was published");

  assert.equal(watch.check(now), null, "no-data to free is not a change worth acting on");
  assert.equal(fired.length, 0);
});

test("records expiring is not the jam clearing", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { session, route, usable } = await corridorFixture(engine, clock);
  assert.ok(usable.length >= 1);

  const jam = usable[0];
  await publish(engine, session, clock, [
    { kind: "flow", lat: jam.lat, lon: jam.lon, speedKmh: Math.max(3, jam.freeflowKmh * 0.1) }
  ]);

  const fired = [];
  const watch = session.watchRoute(route, { onChange: change => fired.push(change), debounceSeconds: 0 });
  watch.check(now);   // baseline with the jam known

  // Let it age past DISPLAY_MAX_AGE without anyone measuring anything.
  now += (DEFAULT_CONSTANTS.DISPLAY_MAX_AGE + 30) * 1000;

  assert.equal(watch.check(now), null, "expiry is silence, not recovery");
  assert.equal(fired.length, 0, "nobody is told good news nobody measured");
});

test("a jam clearing is reported as an improvement", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { session, route, usable } = await corridorFixture(engine, clock);
  assert.ok(usable.length >= 1);

  const road = usable[0];
  await publish(engine, session, clock, [
    { kind: "flow", lat: road.lat, lon: road.lon, speedKmh: Math.max(3, road.freeflowKmh * 0.1) }
  ]);

  const fired = [];
  const watch = session.watchRoute(route, { onChange: change => fired.push(change), debounceSeconds: 0 });
  watch.check(now);   // baseline: jammed

  // Fresh measurements say it is moving again.
  now += 20_000;
  await publish(engine, session, clock, [
    { kind: "flow", lat: road.lat, lon: road.lon, speedKmh: road.freeflowKmh * 0.95 }
  ], { publishFreeFlow: true });

  const change = watch.check(now);
  assert.ok(change, "the clearing is reported");
  assert.ok(change.improved.length >= 1);
  assert.equal(change.worsened.length, 0);
  assert.equal(fired.length, 1);
});

test("the debounce is a floor a flapping corridor cannot get under", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { session, route, usable } = await corridorFixture(engine, clock);
  assert.ok(usable.length >= 2);

  const fired = [];
  const watch = session.watchRoute(route, { onChange: change => fired.push(change), debounceSeconds: 60 });
  watch.check(now);

  await publish(engine, session, clock, [
    { kind: "flow", lat: usable[0].lat, lon: usable[0].lon, speedKmh: Math.max(3, usable[0].freeflowKmh * 0.1) }
  ]);
  assert.ok(watch.check(now), "the first crossing fires");
  assert.equal(fired.length, 1);

  now += 5000;
  await publish(engine, session, clock, [
    { kind: "flow", lat: usable[1].lat, lon: usable[1].lon, speedKmh: Math.max(3, usable[1].freeflowKmh * 0.1) }
  ]);
  assert.equal(watch.check(now), null, "a second jam five seconds later is held");
  assert.equal(fired.length, 1);

  now += 61_000;
  await publish(engine, session, clock, [
    { kind: "flow", lat: usable[1].lat, lon: usable[1].lon, speedKmh: usable[1].freeflowKmh * 0.95 }
  ], { publishFreeFlow: true });
  assert.ok(watch.check(now), "past the floor it speaks again");
  assert.equal(fired.length, 2);
});

test("a stopped watch is dropped by the maintenance loop", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { session, route, usable } = await corridorFixture(engine, clock);

  const fired = [];
  const watch = session.watchRoute(route, { onChange: change => fired.push(change), debounceSeconds: 0 });
  await session.tick(now);           // baseline, via the session's own beat

  watch.stop();
  assert.equal(watch.watching, false);

  await publish(engine, session, clock, [
    { kind: "flow", lat: usable[0].lat, lon: usable[0].lon, speedKmh: Math.max(3, usable[0].freeflowKmh * 0.1) }
  ]);
  await session.tick(now);
  assert.equal(fired.length, 0, "a stopped watch says nothing");
  assert.equal(watch.check(now), null, "and cannot be driven directly either");
});

test("the session's own beat drives a watch with no extra wiring", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { session, route, usable } = await corridorFixture(engine, clock);

  const fired = [];
  session.watchRoute(route, { onChange: change => fired.push(change), debounceSeconds: 0 });
  await session.tick(now);           // baseline

  await publish(engine, session, clock, [
    { kind: "flow", lat: usable[0].lat, lon: usable[0].lon, speedKmh: Math.max(3, usable[0].freeflowKmh * 0.1) }
  ]);
  await session.tick(now);
  assert.equal(fired.length, 1, "tick() found the change without the host asking");
});

test("a handler that throws does not take the maintenance loop with it", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { session, route, usable } = await corridorFixture(engine, clock);

  session.watchRoute(route, {
    onChange: () => { throw new Error("the host's handler is broken"); },
    debounceSeconds: 0
  });
  await session.tick(now);

  await publish(engine, session, clock, [
    { kind: "flow", lat: usable[0].lat, lon: usable[0].lon, speedKmh: Math.max(3, usable[0].freeflowKmh * 0.1) }
  ]);
  await session.tick(now);           // must not reject
  assert.ok(session.snapshot().available, "the session is still running");
});
