// The ingest publisher: external feeds become mesh records that real
// receivers actually aggregate, and nothing more than that.
//
// What is worth pinning: the congestion gate (a free road is what the
// static metric already says — publishing it is noise), the copies rule
// (fewer than AGG_MIN_REPORTS aggregates only as a capped hint), the
// incident ceiling (one delivering peer can never exceed hint tier, by
// §8.5 construction), and the self-pacing (records past the receiver's
// rule 7 bucket are dropped as a flood, so the publisher must never
// send them).

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createMeshSession } from "../src/pulsemesh/session.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createIngestPublisher, incidentCodeOf } from "../src/pulsemesh/ingest.js";
import { CONGESTION_RATIO, createCameraTrafficSource } from "../src/pulsemesh/ingest_camera.js";
import { createPixelCameraAnalyzer } from "../src/pulsemesh/ingest_camera_pixels.js";
import { createGalleryImageFetcher } from "../src/pulsemesh/ingest_camera_browser.js";
import { DEFAULT_CONSTANTS, speedBinFromKmh } from "../src/pulsemesh/bins.js";
import { DENIED_CLASSES } from "../src/pulsemesh/validate.js";
import { aggregateSegment } from "../src/pulsemesh/aggregate.js";
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

async function someRoute(engine, minEdges = 12) {
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

/**
 * Route edges an ingest observation can honestly land on: reportable
 * class, known free-flow speed, and a locatable midpoint.
 */
async function usableEdges(engine, session, route, count) {
  const out = [];
  const seen = new Set();
  for (const edge of route.edges || []) {
    if (!edge?.segment) continue;
    const { leafCell, geomRef } = parseSegment(edge.segment);
    const segKey = `${leafCell}/${geomRef}`;
    if (seen.has(segKey)) continue;
    await session.warmLeaves([leafCell]);
    const context = session.cellContext(leafCell);
    if (!context) continue;
    const roadClass = context.classOf(geomRef);
    if (!roadClass || DENIED_CLASSES.has(roadClass)) continue;
    const freeflowKmh = session.freeflowKmhOf(segKey);
    if (!(freeflowKmh > 0)) continue;
    let point;
    try {
      point = await engine.locate(edge.segment, 0.5);
    } catch {
      continue;
    }
    // The edge's own heading, so a camera in these tests can state which
    // direction it watches — the pipeline now requires it. `twoWay` marks
    // the edges that carry traffic both ways, which the direction tests
    // need and a one-way ramp cannot provide.
    let bearingDeg = null;
    let twoWay = false;
    try {
      const snapped = await engine.snap({ lat: point.lat, lon: point.lon });
      bearingDeg = snapped.matches.find(match => match.segment === edge.segment)?.bearingDeg ?? null;
      const sibling = `${leafCell}/${geomRef >>> 1}/${(geomRef & 1) ^ 1}`;
      twoWay = snapped.matches.some(match => match.segment === sibling);
    } catch {
      continue;
    }
    if (!Number.isFinite(bearingDeg)) continue;
    seen.add(segKey);
    out.push({ segment: edge.segment, segKey, freeflowKmh, bearingDeg, twoWay, lat: point.lat, lon: point.lon });
    if (out.length >= count) break;
  }
  return out;
}

/** A fixture pair: an ingest session and a reader on one loopback mesh. */
async function meshFixture(engine, clock) {
  const network = createLoopbackNetwork({ clock });
  const ingestSession = await createMeshSession({
    engine, network, id: "ingest", transport: "loopback", clock
  });
  const reader = await createMeshSession({
    engine, network, id: "reader", transport: "loopback", clock
  });
  return { network, ingestSession, reader };
}

test("a jam from a feed reaches a receiver as a full aggregate; free flow is not published", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  // Anchored virtual clock: freshness decays against wall-clock age, so
  // a clock in the past would silently zero every record.
  let now = Date.now();
  const clock = () => now;
  const { ingestSession, reader } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route, "the fixture graph has a routable pair");
  await reader.followRoute(route);

  const edges = await usableEdges(engine, ingestSession, route, 2);
  assert.ok(edges.length >= 2, "the route has two usable edges");
  const [jamEdge, freeEdge] = edges;
  const jamKmh = Math.max(3, jamEdge.freeflowKmh * 0.1);

  const ingest = createIngestPublisher({
    engine,
    session: ingestSession,
    clock,
    sources: [{
      id: "detectors",
      intervalSeconds: 60,
      fetch: async () => [
        { kind: "flow", lat: jamEdge.lat, lon: jamEdge.lon, speedKmh: jamKmh },
        { kind: "flow", lat: freeEdge.lat, lon: freeEdge.lon, speedKmh: freeEdge.freeflowKmh * 0.95 }
      ]
    }]
  });
  await ingest.tick();

  assert.equal(ingest.stats.publishedSegments, 1, "only the jam is published");
  assert.equal(ingest.stats.skippedFree, 1, "the free reading is gated");
  assert.equal(ingest.stats.publishedRecords, DEFAULT_CONSTANTS.AGG_MIN_REPORTS);

  // The observation may snap to either approach of the physical edge;
  // what matters is that ONE segment reached the reader with enough
  // records to aggregate above the hint cap.
  const expectedBin = speedBinFromKmh(jamKmh);
  let aggregated = null;
  for (const segKey of reader.node.store.liveSegmentKeys()) {
    const entries = reader.node.store.contributionsForSegment(segKey);
    const aggregate = aggregateSegment(entries, { nowMillis: clock() });
    if (aggregate) aggregated = aggregate;
  }
  assert.ok(aggregated, "the reader aggregates the jam");
  assert.equal(aggregated.n, DEFAULT_CONSTANTS.AGG_MIN_REPORTS);
  assert.equal(aggregated.hint, false, "AGG_MIN_REPORTS records clear the hint cap");
  assert.equal(aggregated.speedBin, expectedBin);
});

test("a feed incident lands as a hint and never as displayed fact", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession, reader } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);
  await reader.followRoute(route);

  const [edge] = await usableEdges(engine, ingestSession, route, 1);
  assert.ok(edge);
  assert.equal(incidentCodeOf("crash"), 1);
  assert.equal(incidentCodeOf("no such thing"), null);

  const source = {
    id: "events",
    intervalSeconds: 60,
    fetch: async () => [{ kind: "incident", lat: edge.lat, lon: edge.lon, type: "crash" }]
  };
  const ingest = createIngestPublisher({ engine, session: ingestSession, clock, sources: [source] });
  await ingest.tick();

  assert.equal(ingest.stats.publishedIncidents, 1);
  assert.equal(reader.node.store.incidentCount(), 1, "the incident reached the reader");
  const incidents = await reader.incidents({ nowMillis: clock() });
  assert.equal(incidents.length, 1);
  assert.equal(incidents[0].tier, "hint", "one delivering peer scores min(raw, 1) — never shown as fact");

  // The same feed listing the same event next poll must not republish
  // inside the re-emit window.
  now += 61 * 1000;
  await ingest.tick();
  assert.equal(ingest.stats.incidentDeduped, 1);
  assert.equal(ingest.stats.publishedIncidents, 1);
});

test("a closedRoad incident maintains a near-zero speed state the router prices", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession, reader } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);
  await reader.followRoute(route);

  const [edge] = await usableEdges(engine, ingestSession, route, 1);
  assert.ok(edge);
  // The feed asserts the closure once, then stops listing it — the
  // state must live exactly as long as the assertion does.
  let calls = 0;
  const ingest = createIngestPublisher({
    engine,
    session: ingestSession,
    clock,
    sources: [{
      id: "closures",
      intervalSeconds: 60,
      fetch: async () => (++calls === 1
        ? [{ kind: "incident", lat: edge.lat, lon: edge.lon, type: "closure report", closedRoad: true }]
        : [])
    }]
  });
  await ingest.tick();

  assert.equal(ingest.closureCount, 1, "the closure is registered");
  assert.equal(ingest.stats.publishedIncidents, 1, "the pin still publishes");
  assert.ok(ingest.stats.closureFlowEmitted >= 1, "the speed state is emitted");

  // What the router actually consumes: a live state at walking pace.
  const states = await reader.node.provider.fetch({ epoch: engine.root.sourceHash, areas: [] });
  assert.ok(states.length >= 1, "the reader's provider hands the router a state");
  const slowest = states.reduce((a, b) => (a.speedMps < b.speedMps ? a : b));
  assert.ok(slowest.speedMps < 1, `the closed segment reads as impassable (${slowest.speedMps.toFixed(2)} m/s)`);

  // Refreshed inside CONTRIB_TTL while the assertion holds.
  const emittedOnce = ingest.stats.closureFlowEmitted;
  now += 45 * 1000;
  await ingest.tick();
  assert.ok(ingest.stats.closureFlowEmitted > emittedOnce, "the state is re-emitted before it expires");

  // The feed stopped asserting it: past the hold window (two fetch
  // intervals + grace) the closure dies and nothing re-emits.
  now += 200 * 1000;
  await ingest.tick();
  assert.equal(ingest.closureCount, 0, "a delisted closure expires");
  const emittedFinal = ingest.stats.closureFlowEmitted;
  now += 60 * 1000;
  await ingest.tick();
  assert.equal(ingest.stats.closureFlowEmitted, emittedFinal, "no state outlives its assertion");
});

test("a camera glance becomes a speed the road's own free-flow defines", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession, reader } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);
  await reader.followRoute(route);

  const edges = await usableEdges(engine, ingestSession, route, 3);
  assert.ok(edges.length >= 3);
  const [jammed, dark, unsure] = edges;

  // Three cameras: one sees stopped traffic, one is unusable (night,
  // fogged lens), one answers below the confidence floor. Only the
  // first may publish anything.
  const verdicts = new Map([
    ["cam-jam", { congestion: "stopped", vehicles: 24, confidence: 0.9 }],
    ["cam-dark", { congestion: "unusable", confidence: 0.9 }],
    ["cam-unsure", { congestion: "heavy", confidence: 0.2 }]
  ]);
  const source = createCameraTrafficSource({
    id: "test-cameras",
    intervalSeconds: 60,
    cameras: [
      { id: "cam-jam", lat: jammed.lat, lon: jammed.lon, bearingDeg: jammed.bearingDeg },
      { id: "cam-dark", lat: dark.lat, lon: dark.lon, bearingDeg: dark.bearingDeg },
      { id: "cam-unsure", lat: unsure.lat, lon: unsure.lon, bearingDeg: unsure.bearingDeg }
    ],
    fetchImage: async () => ({ base64: "ZmFrZQ==", mediaType: "image/jpeg" }),
    analyze: async ({ camera }) => verdicts.get(camera.id)
  });

  const ingest = createIngestPublisher({ engine, session: ingestSession, clock, sources: [source] });
  await ingest.tick();

  assert.equal(source.stats.frames, 3);
  assert.equal(source.stats.unusable, 1, "an unusable frame is dropped, never guessed from");
  assert.equal(source.stats.lowConfidence, 1);
  assert.equal(source.stats.observations, 1);
  assert.equal(ingest.stats.publishedSegments, 1, "only the stopped camera publishes");

  // The published speed is the segment's OWN free-flow scaled by the
  // stopped ratio — not a hardcoded km/h.
  const expectedKmh = jammed.freeflowKmh * CONGESTION_RATIO.stopped;
  let aggregated = null;
  for (const segKey of reader.node.store.liveSegmentKeys()) {
    const aggregate = aggregateSegment(reader.node.store.contributionsForSegment(segKey), { nowMillis: clock() });
    if (aggregate) aggregated = aggregate;
  }
  assert.ok(aggregated, "the reader aggregates the camera observation");
  assert.ok(
    Math.abs(aggregated.speedKmh - expectedKmh) <= 2.5 + 1e-9,
    `aggregate ${aggregated.speedKmh} km/h is the stopped fraction of free-flow ${jammed.freeflowKmh.toFixed(1)}`
  );
});

test("the pixel analyzer counts blobs, refuses bad frames, and spots standing queues", async () => {
  const WIDTH = 64;
  const HEIGHT = 48;
  const frame = (base, blobs = []) => {
    const gray = new Uint8Array(WIDTH * HEIGHT).fill(base);
    for (const { x, y, w, h, value } of blobs) {
      for (let dy = 0; dy < h; dy++) {
        for (let dx = 0; dx < w; dx++) gray[(y + dy) * WIDTH + (x + dx)] = value;
      }
    }
    return { width: WIDTH, height: HEIGHT, gray };
  };
  // Cars are dark 4x3 rectangles on a 120-luma road, inside the default
  // ROI (which skips the top 35% of the frame).
  const cars = count => Array.from({ length: count }, (_, i) => ({
    x: 2 + (i % 8) * 7, y: 20 + Math.floor(i / 8) * 6, w: 4, h: 3, value: 40
  }));

  const frames = [
    frame(120), frame(120), frame(120),          // background warm-up
    frame(120, cars(6)),                          // separated vehicles
    frame(10),                                    // night
    frame(120, cars(20)),                         // dense traffic, first sight
    frame(120, cars(20)),                         // same vehicles next poll
    frame(200)                                    // PTZ preset change
  ];
  const analyze = createPixelCameraAnalyzer({ decode: async () => frames.shift() });
  const next = () => analyze({ imageBase64: "Zg==", camera: { id: "cam" } });

  assert.equal((await next()).congestion, "unusable"); // init
  assert.equal((await next()).congestion, "unusable"); // warming
  assert.equal((await next()).congestion, "unusable"); // warming

  const counted = await next();
  assert.equal(counted.congestion, undefined, "a countable frame classifies nothing");
  assert.equal(counted.vehicles, 6, "six separated vehicles count as six");

  assert.match((await next()).reason, /dark/, "night frames are refused, not guessed");

  const dense = await next();
  assert.ok(dense.vehicles >= 18, `dense traffic counts high (${dense.vehicles})`);
  const standing = await next();
  assert.equal(standing.congestion, "stopped", "the same foreground minutes later is a standing queue");

  const moved = await next();
  assert.match(moved.reason, /scene changed/, "a preset change resets the background");
});

test("a frozen camera feed is refused, not analyzed into an empty road", async () => {
  // Found in the wild: an offline public endpoint serving its cached
  // last frame forever. Byte-identical consecutive frames must be
  // dropped before analysis — a live camera never repeats exactly.
  let analyzed = 0;
  const source = createCameraTrafficSource({
    id: "frozen",
    intervalSeconds: 60,
    cameras: [{ id: "cam", lat: 0, lon: 0, bearingDeg: 0 }],
    fetchImage: async () => ({ base64: "c3RhbGUtZnJhbWU=", mediaType: "image/jpeg" }),
    analyze: async () => {
      analyzed++;
      return { congestion: "free", confidence: 0.9 };
    }
  });
  const first = await source.fetch({ nowMillis: 0 });
  const second = await source.fetch({ nowMillis: 60000 });
  const third = await source.fetch({ nowMillis: 120000 });
  assert.equal(first.length, 1, "the first sighting of a frame is analyzed");
  assert.equal(second.length + third.length, 0, "repeats of the same bytes produce nothing");
  assert.equal(analyzed, 1, "the analyzer is not even called on frozen frames");
  assert.equal(source.stats.frozen, 2);
});

test("a vehicle count becomes a speed via the road's own lanes and free-flow", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession, reader } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);
  await reader.followRoute(route);

  const [edge] = await usableEdges(engine, ingestSession, route, 1);
  assert.ok(edge);

  // The analyzer only counts; the camera carries its calibration; the
  // segment's free-flow prices the density (Greenshields, k_jam 140).
  const VEHICLES = 20;
  const LANES = 2;
  const VISIBLE_METERS = 200;
  const source = createCameraTrafficSource({
    id: "counting-camera",
    intervalSeconds: 60,
    cameras: [{
      id: "cam", lat: edge.lat, lon: edge.lon,
      bearingDeg: edge.bearingDeg, lanes: LANES, visibleMeters: VISIBLE_METERS
    }],
    fetchImage: async () => ({ base64: "Zg==", mediaType: "image/jpeg" }),
    analyze: async () => ({ vehicles: VEHICLES, confidence: 0.8 })
  });
  const ingest = createIngestPublisher({ engine, session: ingestSession, clock, sources: [source] });
  await ingest.tick();

  assert.equal(source.stats.observations, 1);
  assert.equal(ingest.stats.publishedSegments, 1, "a dense count publishes as congestion");

  const density = VEHICLES / (LANES * (VISIBLE_METERS / 1000));
  const expectedKmh = Math.max(0, 1 - density / 140) * edge.freeflowKmh;
  let aggregated = null;
  for (const segKey of reader.node.store.liveSegmentKeys()) {
    const aggregate = aggregateSegment(reader.node.store.contributionsForSegment(segKey), { nowMillis: clock() });
    if (aggregate) aggregated = aggregate;
  }
  assert.ok(aggregated, "the reader aggregates the counted observation");
  assert.ok(
    Math.abs(aggregated.speedKmh - expectedKmh) <= 2.5 + 1e-9,
    `aggregate ${aggregated.speedKmh} km/h ≈ Greenshields ${expectedKmh.toFixed(1)} of free-flow ${edge.freeflowKmh.toFixed(1)}`
  );
});

test("each carriageway is counted and priced separately", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession, reader } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);
  await reader.followRoute(route);

  const edge = (await usableEdges(engine, ingestSession, route, 16)).find(candidate => candidate.twoWay);
  assert.ok(edge, "the route has a road carrying traffic both ways");

  // One camera looking at both directions of the road: the near half of
  // the frame is one carriageway, the far half the other. Each declares
  // its own region, heading and lane count. Both are busy enough to
  // publish — the point is that they publish *different* speeds.
  const HEAVY = 30;
  const LIGHT = 15;
  const counts = new Map([["A15#out", HEAVY], ["A15#back", LIGHT]]);
  const seenRois = [];
  const source = createCameraTrafficSource({
    id: "two-way-camera",
    intervalSeconds: 60,
    cameras: [{
      id: "A15",
      lat: edge.lat,
      lon: edge.lon,
      visibleMeters: 200,
      directions: [
        { name: "out", roi: [0, 0.5, 1, 1], bearingDeg: edge.bearingDeg, lanes: 2 },
        { name: "back", roi: [0, 0.2, 1, 0.5], bearingDeg: (edge.bearingDeg + 180) % 360, lanes: 2 }
      ]
    }],
    fetchImage: async () => ({ base64: "Zg==", mediaType: "image/jpeg" }),
    analyze: async ({ camera }) => {
      seenRois.push(camera.roi.join(","));
      return { vehicles: counts.get(camera.id), confidence: 0.8 };
    }
  });
  const ingest = createIngestPublisher({ engine, session: ingestSession, clock, sources: [source] });
  await ingest.tick();

  assert.equal(source.stats.frames, 1, "one frame is fetched, not one per direction");
  assert.equal(source.stats.observations, 2, "each carriageway observes separately");
  assert.deepEqual(seenRois, ["0,0.5,1,1", "0,0.2,1,0.5"], "each view analyzes its own region");

  // Each observation must land on the approach whose heading it declared
  // — the whole point of stating a direction.
  const outbound = edge.segKey;
  const inbound = `${outbound.split("/")[0]}/${Number(outbound.split("/")[1]) ^ 1}`;
  const published = reader.node.store.liveSegmentKeys();
  assert.deepEqual(
    [...published].sort(), [outbound, inbound].sort(),
    "both approaches of the road carry a state, and only those"
  );

  // Each carriageway is priced on its own count against its own
  // free-flow. Those free-flows can differ sharply between directions
  // (the fixture has 89 km/h one way and 29 the other), so the busy side
  // is not necessarily the slower one — which is exactly why the count
  // must not be shared between them.
  const speedOf = segKey => aggregateSegment(
    reader.node.store.contributionsForSegment(segKey), { nowMillis: clock() }
  )?.speedKmh;
  const priced = (count, segKey) =>
    Math.max(0, 1 - (count / (2 * 0.2)) / 140) * ingestSession.freeflowKmhOf(segKey);

  for (const [segKey, count, other] of [[outbound, HEAVY, LIGHT], [inbound, LIGHT, HEAVY]]) {
    const expected = priced(count, segKey);
    assert.ok(
      Math.abs(speedOf(segKey) - expected) <= 2.6,
      `${segKey} priced on its own ${count} vehicles (${speedOf(segKey)} vs ${expected.toFixed(1)})`
    );
    assert.ok(
      Math.abs(priced(count, segKey) - priced(other, segKey)) > 2.6,
      "and the two counts are far enough apart that this is not a coincidence"
    );
  }
});

test("a camera that never says which way it looks is skipped, not guessed", async () => {
  let analyzed = 0;
  const source = createCameraTrafficSource({
    id: "undirected",
    intervalSeconds: 60,
    cameras: [
      { id: "silent", lat: 45.5, lon: -73.5 },
      { id: "stated", lat: 45.5, lon: -73.5, bearingDeg: 90 }
    ],
    fetchImage: async () => ({ base64: "Zg==", mediaType: "image/jpeg" }),
    analyze: async () => {
      analyzed++;
      return { vehicles: 5, confidence: 0.9 };
    }
  });
  const observations = await source.fetch({ nowMillis: 0 });

  assert.equal(source.stats.undirected, 1, "the silent camera is counted, not published");
  assert.equal(analyzed, 1, "and never analyzed — no frame is even fetched for it");
  assert.equal(source.stats.frames, 1);
  assert.equal(observations.length, 1);
  assert.equal(observations[0].bearingDeg, 90);
});

test("a count on a shared roadway is split between directions, not doubled", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession, reader } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);
  await reader.followRoute(route);

  const edge = (await usableEdges(engine, ingestSession, route, 16)).find(candidate => candidate.twoWay);
  assert.ok(edge, "the route has an undivided road");

  const TOTAL = 24;
  const LANES = 1;
  const VISIBLE_METERS = 200;
  const source = createCameraTrafficSource({
    id: "undivided",
    intervalSeconds: 60,
    cameras: [{
      id: "village", lat: edge.lat, lon: edge.lon,
      bothDirections: true, lanes: LANES, visibleMeters: VISIBLE_METERS
    }],
    fetchImage: async () => ({ base64: "Zg==", mediaType: "image/jpeg" }),
    analyze: async () => ({ vehicles: TOTAL, confidence: 0.8 })
  });
  const ingest = createIngestPublisher({ engine, session: ingestSession, clock, sources: [source] });
  await ingest.tick();

  const published = reader.node.store.liveSegmentKeys();
  assert.equal(published.length, 2, "an undivided road carries both approaches");

  // Half the vehicles per approach — not TOTAL on each. Each approach is
  // priced against the free-flow of the segment it actually matched.
  const halfDensity = (TOTAL / 2) / (LANES * (VISIBLE_METERS / 1000));
  const wholeDensity = TOTAL / (LANES * (VISIBLE_METERS / 1000));
  for (const segKey of published) {
    const aggregate = aggregateSegment(
      reader.node.store.contributionsForSegment(segKey), { nowMillis: clock() }
    );
    const freeflowKmh = ingestSession.freeflowKmhOf(segKey);
    const expectedKmh = Math.max(0, 1 - halfDensity / 140) * freeflowKmh;
    const doubledKmh = Math.max(0, 1 - wholeDensity / 140) * freeflowKmh;
    assert.ok(
      Math.abs(aggregate.speedKmh - expectedKmh) <= 2.6,
      `${segKey} priced on half the count (${aggregate.speedKmh} vs ${expectedKmh.toFixed(1)})`
    );
    assert.ok(
      Math.abs(aggregate.speedKmh - doubledKmh) > 2.6,
      "and not on the whole count applied to each approach"
    );
  }
});

/**
 * A stand-in for Playwright: a browser whose page records which URLs it
 * was asked to open and replays a fixed set of image responses, so the
 * gallery reader can be tested without a network or a real browser.
 */
function fakeChromium({ images = {}, onGoto = null } = {}) {
  const visited = [];
  let listener = null;
  const page = {
    isClosed: () => false,
    setDefaultNavigationTimeout() {},
    on(event, handler) { if (event === "response") listener = handler; },
    off() { listener = null; },
    async waitForTimeout() {},
    async goto(url) {
      visited.push(url);
      if (onGoto) await onGoto(url, visited.length);
      for (const [path, body] of Object.entries(images[url] ?? {})) {
        await listener?.({
          url: () => `https://example.test${path}`,
          ok: () => true,
          headers: () => ({ "content-type": "image/jpeg" }),
          body: async () => Buffer.from(body)
        });
      }
    }
  };
  return {
    visited,
    launch: async () => ({
      newContext: async () => ({ newPage: async () => page }),
      close: async () => {}
    })
  };
}

test("the gallery reader keeps what the operator's own page loaded", async () => {
  const GALLERY = "https://example.test/cameras?route=15";
  const chromium = fakeChromium({
    images: {
      [GALLERY]: {
        "/Images/Cameras/Montreal/cam/0155179.jpg": "first-camera-bytes",
        "/Images/Cameras/Montreal/cam/0155189.jpg": "second-camera-bytes"
      }
    }
  });
  const gallery = createGalleryImageFetcher({
    origin: "https://example.test",
    chromium,
    galleryUrlFor: () => GALLERY,
    fileOf: camera => camera.file,
    refreshMillis: 60_000
  });

  const first = await gallery.fetchImage({ id: "a", file: "0155179.jpg" });
  const second = await gallery.fetchImage({ id: "b", file: "0155189.jpg" });
  assert.equal(Buffer.from(first.base64, "base64").toString(), "first-camera-bytes");
  assert.equal(Buffer.from(second.base64, "base64").toString(), "second-camera-bytes");
  assert.equal(first.mediaType, "image/jpeg");

  // One page view served both cameras — the whole point of reading the
  // gallery instead of requesting each picture.
  assert.equal(chromium.visited.length, 1, "the gallery is opened once, not once per camera");
  assert.equal(gallery.stats.pageViews, 1);
  assert.equal(gallery.stats.imagesSeen, 2);

  // A camera the page did not show is a miss, not a wrong picture.
  await assert.rejects(
    () => gallery.fetchImage({ id: "c", file: "9999999.jpg" }),
    /was not among the 2 pictures/
  );
  assert.equal(gallery.stats.misses, 1);
  await gallery.close();
});

test("the gallery reader reopens the page only when its pictures go stale", async () => {
  const GALLERY = "https://example.test/cameras?route=20";
  const chromium = fakeChromium({
    images: { [GALLERY]: { "/Images/Cameras/Quebec/cam/5413.jpg": "bytes" } }
  });
  const gallery = createGalleryImageFetcher({
    origin: "https://example.test",
    chromium,
    galleryUrlFor: () => GALLERY,
    fileOf: () => "5413.jpg",
    refreshMillis: 50,
    minIntervalMillis: 0
  });

  await gallery.fetchImage({ id: "a" });
  await gallery.fetchImage({ id: "a" });
  assert.equal(chromium.visited.length, 1, "a fresh page is reused");

  await new Promise(resolve => setTimeout(resolve, 60));
  await gallery.fetchImage({ id: "a" });
  assert.equal(chromium.visited.length, 2, "a stale page is opened again");
  await gallery.close();
});

test("a gallery that shows nothing is left alone, not hammered", async () => {
  // Measured against Québec 511: a throttled gallery answers with no
  // pictures. Re-opening it for each of its cameras would turn one
  // throttled read into a burst — the opposite of what it is asking for.
  const GALLERY = "https://example.test/cameras?route=540";
  const chromium = fakeChromium({ images: { [GALLERY]: {} } });
  const gallery = createGalleryImageFetcher({
    origin: "https://example.test",
    chromium,
    galleryUrlFor: () => GALLERY,
    fileOf: camera => camera.file,
    minIntervalMillis: 0,
    emptyCooldownMillis: 10_000
  });

  const cameras = Array.from({ length: 11 }, (_, i) => ({ id: `c${i}`, file: `${i}.jpg` }));
  for (const camera of cameras) {
    await assert.rejects(() => gallery.fetchImage(camera));
  }
  assert.equal(chromium.visited.length, 1, "eleven cameras cost one page view, not eleven");
  assert.equal(gallery.stats.emptyPages, 1);
  assert.equal(gallery.stats.misses, 11);
  await gallery.close();
});

test("an empty gallery is tried again once its cooldown passes", async () => {
  const GALLERY = "https://example.test/cameras?route=148";
  // Empty on the first view, populated on the second.
  const images = {};
  const chromium = fakeChromium({
    images,
    onGoto: (url, visit) => {
      images[url] = visit === 1 ? {} : { "/Images/Cameras/Quebec/cam/5413.jpg": "bytes" };
    }
  });
  const gallery = createGalleryImageFetcher({
    origin: "https://example.test",
    chromium,
    galleryUrlFor: () => GALLERY,
    fileOf: () => "5413.jpg",
    minIntervalMillis: 0,
    emptyCooldownMillis: 40
  });

  await assert.rejects(() => gallery.fetchImage({ id: "a" }));
  await assert.rejects(() => gallery.fetchImage({ id: "a" }), /was not among/);
  assert.equal(chromium.visited.length, 1, "still cooling down");

  await new Promise(resolve => setTimeout(resolve, 50));
  const image = await gallery.fetchImage({ id: "a" });
  assert.equal(Buffer.from(image.base64, "base64").toString(), "bytes");
  assert.equal(chromium.visited.length, 2, "and tried again after the cooldown");
  await gallery.close();
});

test("the publisher paces itself under the receiver's rule 7 bucket", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession, reader } = await meshFixture(engine, clock);
  const route = await someRoute(engine, 30);
  assert.ok(route);
  await reader.followRoute(route);

  const edges = await usableEdges(engine, ingestSession, route, 30);
  assert.ok(edges.length >= 15, `enough distinct edges to overflow the budget (${edges.length})`);

  const ingest = createIngestPublisher({
    engine,
    session: ingestSession,
    clock,
    sources: [{
      id: "burst",
      intervalSeconds: 60,
      fetch: async () => edges.map(edge => ({
        kind: "flow", lat: edge.lat, lon: edge.lon, speedKmh: Math.max(3, edge.freeflowKmh * 0.1)
      }))
    }]
  });
  await ingest.tick();

  const capacity = Math.floor(DEFAULT_CONSTANTS.RATE_BURST * 0.8);
  const firstWave = ingest.stats.publishedRecords;
  assert.ok(firstWave <= capacity, `first drain stays inside the burst margin (${firstWave} <= ${capacity})`);
  assert.ok(ingest.queueLength > 0, "the backlog is held, not flooded");
  assert.equal(reader.stats?.dropped ?? 0, 0);

  // The bucket refills with the clock; later drains work the backlog off
  // without a second fetch.
  const before = ingest.stats.publishedRecords;
  now += 20 * 1000;
  await ingest.tick();
  assert.ok(ingest.stats.publishedRecords > before, "the backlog drains as tokens refill");
  // The reader's own validator dropped nothing as a flood: every record
  // it received, it accepted.
  assert.equal(reader.node.stats.gossipDropped, 0, "no record was dropped by the receiver");
});
