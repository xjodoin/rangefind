// Lazy backfill: a paid traffic API asked only where the mesh cannot
// answer and the answer could change a route.
//
// What is worth pinning here is everything that costs money or trust:
// that assessing spends nothing, that a road the mesh already prices is
// never bought, that the same corridor is not bought twice inside a
// TTL, that a reading is never spread past the stretch it was taken on,
// and that a reading which does not agree with the segment it landed on
// is dropped rather than published under this operator's bond.

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createMeshSession } from "../src/pulsemesh/session.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createIngestPublisher } from "../src/pulsemesh/ingest.js";
import { createRouteBackfill, createTomTomProbe, DEFAULT_PROBE_CLASSES } from "../src/pulsemesh/ingest_tomtom.js";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";
import { aggregateSegment } from "../src/pulsemesh/aggregate.js";
import { parseSegment } from "../src/pulsemesh/codec.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";
const PROBE_CLASSES = new Set(DEFAULT_PROBE_CLASSES);

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

/** A route long enough to have several probe-class runs on it. */
async function someRoute(engine, minEdges = 40) {
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

/** A probe stand-in: records every call, answers from a fixed template. */
function fakeProbe(reply) {
  const calls = [];
  const probe = async point => {
    calls.push(point);
    return typeof reply === "function" ? reply(point, calls.length) : reply;
  };
  probe.calls = calls;
  return probe;
}

/** The reading a healthy jam looks like on a segment of this free-flow. */
const jamReading = freeflowKmh => ({
  currentSpeedKmh: Math.max(3, freeflowKmh * 0.15),
  freeFlowSpeedKmh: freeflowKmh,
  confidence: 0.95,
  roadClosure: false,
  frc: "FRC1"
});

/** The segment free-flow the backfill will compare a reading against. */
function freeflowOnRoute(session, plan) {
  const gap = plan.entries.find(entry => entry.gap);
  return gap ? gap.freeflowKmh : null;
}

test("assessing an itinerary spends nothing and only counts what the mesh cannot price", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route, "the fixture graph has a long routable pair");

  const probe = fakeProbe(() => {
    throw new Error("assess() must never issue a probe");
  });
  const backfill = createRouteBackfill({ engine, session: ingestSession, probe, clock });

  const plan = await backfill.assess(route);
  assert.equal(probe.calls.length, 0, "assessing is free");
  assert.ok(plan.entries.length > 0, "the route's edges were considered");
  assert.ok(plan.gapMeters > 0, "an empty mesh cannot price this route");
  assert.ok(plan.totalMeters >= plan.gapMeters);
  assert.ok(plan.probes.length > 0, "there is something worth buying");
  assert.ok(plan.probes.length <= 8, "the per-route probe cap holds");
});

test("only classes where congestion could change the route become gaps", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const backfill = createRouteBackfill({ engine, session: ingestSession, probe: fakeProbe(null), clock });
  const plan = await backfill.assess(route);

  const offClass = plan.entries.filter(entry => entry.roadClass && !PROBE_CLASSES.has(entry.roadClass));
  assert.ok(offClass.length > 0, "the fixture route mixes in residential/tertiary road");
  for (const entry of offClass) {
    assert.equal(entry.gap, false, `${entry.roadClass} must never be bought`);
    assert.equal(entry.reason, "off class");
  }
  for (const entry of plan.entries) {
    if (entry.gap) assert.ok(PROBE_CLASSES.has(entry.roadClass));
  }
});

test("a bought reading reaches a mesh reader as a real aggregate", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession, reader } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);
  await reader.followRoute(route);

  // Answer with the segment's own free-flow so the map-match check
  // passes, at a speed that is unambiguously a jam.
  const scout = createRouteBackfill({ engine, session: ingestSession, probe: fakeProbe(null), clock });
  const freeflowKmh = freeflowOnRoute(ingestSession, await scout.assess(route));
  assert.ok(freeflowKmh > 0, "the route has a gap edge with a known free-flow");

  const probe = fakeProbe(jamReading(freeflowKmh));
  const backfill = createRouteBackfill({
    engine, session: ingestSession, probe, clock,
    maxProbesPerRoute: 2, maxSegmentsPerRoute: 4,
    // Every gap edge on this route shares one free-flow only by luck;
    // widen the match tolerance so the test exercises publishing, not
    // the mismatch gate (which has its own test).
    maxFreeflowMismatch: 10
  });
  const ingest = createIngestPublisher({
    engine, session: ingestSession, sources: [backfill.source], clock
  });

  const report = await backfill.backfill(route);
  assert.ok(probe.calls.length > 0, "the gap was bought");
  assert.ok(report.publishedSegments > 0, "the reading was attributed to segments");
  assert.ok(report.states.length > 0, "and returned for immediate use on this route");
  for (const state of report.states) {
    assert.ok(state.speedMps > 0);
    assert.equal(state.closed, false, "protocol v1 has no hard closure in a speed state");
  }

  await ingest.tick();
  assert.ok(ingest.stats.publishedSegments > 0, "the publisher drained the backfill");

  let aggregated = null;
  for (const segKey of reader.node.store.liveSegmentKeys()) {
    const aggregate = aggregateSegment(reader.node.store.contributionsForSegment(segKey), { nowMillis: clock() });
    if (aggregate) aggregated = aggregate;
  }
  assert.ok(aggregated, "the reader aggregates what was bought");
  assert.equal(aggregated.n, DEFAULT_CONSTANTS.AGG_MIN_REPORTS);
  assert.equal(aggregated.hint, false, "AGG_MIN_REPORTS records clear the hint cap");
});

test("a segment the mesh already prices is never bought", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const scout = createRouteBackfill({ engine, session: ingestSession, probe: fakeProbe(null), clock });
  const before = await scout.assess(route);
  const gaps = before.entries.filter(entry => entry.gap).slice(0, 6);
  assert.ok(gaps.length >= 3, "the cold route has gaps to close");

  // Close some of them the way the mesh normally would: real records,
  // published as jams so the free-flow gate does not drop them.
  const points = [];
  for (const gap of gaps) {
    const at = await engine.locate(gap.segment, 0.5);
    points.push({ kind: "flow", lat: at.lat, lon: at.lon, speedKmh: Math.max(3, gap.freeflowKmh * 0.1) });
  }
  const seeder = createIngestPublisher({
    engine, session: ingestSession, clock,
    sources: [{ id: "drivers", intervalSeconds: 60, fetch: async () => points }]
  });
  await seeder.tick();
  assert.ok(seeder.stats.publishedSegments > 0, "the mesh now holds those segments");

  const after = await scout.assess(route);
  const covered = after.entries.filter(entry => entry.reason === "mesh");
  assert.ok(covered.length > 0, "what the mesh holds is read as covered");
  for (const entry of covered) {
    const aggregate = aggregateSegment(
      ingestSession.node.store.contributionsForSegment(entry.segKey),
      { nowMillis: clock() }
    );
    assert.ok(aggregate && aggregate.n >= DEFAULT_CONSTANTS.AGG_MIN_REPORTS,
      "covered means genuinely enough reports, not merely present");
  }
  assert.ok(after.gapMeters < before.gapMeters, "the unpriced share of the route shrank");
});

test("the same corridor is not bought twice inside the cache TTL", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const probe = fakeProbe({
    currentSpeedKmh: 20, freeFlowSpeedKmh: 60, confidence: 0.9, roadClosure: false, frc: "FRC2"
  });
  // A cap high enough to take the whole route in one pass, so the second
  // pass has nothing left but what the first already answered.
  const backfill = createRouteBackfill({
    engine, session: ingestSession, probe, clock,
    maxProbesPerRoute: 64, maxSegmentsPerRoute: 512, maxFreeflowMismatch: 10
  });

  await backfill.backfill(route);
  const first = probe.calls.length;
  assert.ok(first > 0);

  await backfill.backfill(route);
  assert.equal(probe.calls.length, first, "the second itinerary over the same road buys nothing");

  // Past the TTL the corridor is fair game again.
  now += (DEFAULT_CONSTANTS.CONTRIB_TTL + 5) * 1000;
  await backfill.backfill(route);
  assert.ok(probe.calls.length > first, "an expired answer is re-asked");
});

test("a reading that disagrees with the segment it landed on is dropped, not published", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  // A free-flow nothing on this route could plausibly have: the API
  // matched some other road at that point.
  const probe = fakeProbe({
    currentSpeedKmh: 4, freeFlowSpeedKmh: 7, confidence: 0.99, roadClosure: false, frc: "FRC6"
  });
  const backfill = createRouteBackfill({
    engine, session: ingestSession, probe, clock, maxProbesPerRoute: 4
  });
  const report = await backfill.backfill(route);

  assert.ok(probe.calls.length > 0, "the probes were issued");
  assert.equal(backfill.stats.freeflowMismatch, probe.calls.length, "every one was rejected");
  assert.equal(report.publishedSegments, 0, "nothing reached the mesh");
  assert.equal(backfill.queueLength, 0);
  assert.equal(report.states.length, 0);
});

test("a low-confidence reading is not published", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const probe = fakeProbe({
    currentSpeedKmh: 20, freeFlowSpeedKmh: 60, confidence: 0.2, roadClosure: false, frc: "FRC2"
  });
  const backfill = createRouteBackfill({
    engine, session: ingestSession, probe, clock,
    maxProbesPerRoute: 3, minConfidence: 0.5, maxFreeflowMismatch: 10
  });
  const report = await backfill.backfill(route);

  assert.ok(probe.calls.length > 0);
  assert.equal(backfill.stats.lowConfidence, probe.calls.length);
  assert.equal(report.publishedSegments, 0);
});

test("a vendor road closure becomes a closure the router can actually see", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const probe = fakeProbe({
    currentSpeedKmh: 0, freeFlowSpeedKmh: 60, confidence: 0.95, roadClosure: true, frc: "FRC1"
  });
  const backfill = createRouteBackfill({
    engine, session: ingestSession, probe, clock,
    maxProbesPerRoute: 1, maxSegmentsPerRoute: 2, maxFreeflowMismatch: 10
  });
  const ingest = createIngestPublisher({
    engine, session: ingestSession, sources: [backfill.source], clock
  });

  await backfill.backfill(route);
  assert.ok(backfill.stats.closures > 0, "the closure was queued alongside the speed");

  await ingest.tick();
  assert.ok(ingest.stats.closuresAsserted > 0, "the publisher holds it as a closure");
  assert.ok(ingest.closureCount > 0, "and keeps a near-zero speed state alive for it");
  assert.ok(ingest.stats.publishedIncidents > 0, "the incident pin is published too");
});

test("the daily budget is a hard ceiling on what a day can cost", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const probe = fakeProbe({
    currentSpeedKmh: 20, freeFlowSpeedKmh: 60, confidence: 0.9, roadClosure: false, frc: "FRC2"
  });
  const backfill = createRouteBackfill({
    engine, session: ingestSession, probe, clock,
    dailyProbeBudget: 2, maxProbesPerRoute: 8, maxFreeflowMismatch: 10,
    // No caching, so only the budget can stop the second pass.
    cacheSeconds: 0, failureCacheSeconds: 0
  });

  assert.equal(backfill.budgetRemaining, 2);
  await backfill.backfill(route);
  await backfill.backfill(route);
  assert.equal(probe.calls.length, 2, "the ceiling holds across itineraries");
  assert.equal(backfill.budgetRemaining, 0);
  assert.ok(backfill.stats.budgetBlocked > 0, "and says so rather than failing quietly");
});

test("when there is more gap than budget, the costliest road is bought first", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const scout = createRouteBackfill({
    engine, session: ingestSession, probe: fakeProbe(null), clock, maxProbesPerRoute: 1
  });
  const plan = await scout.assess(route);
  assert.ok(plan.chunks.length > 1, "there is a choice to make");
  assert.equal(plan.probes.length, 1, "and only one probe to make it with");

  const best = plan.probes[0];
  for (const chunk of plan.chunks) {
    assert.ok(best.weight >= chunk.weight, "the selected chunk outranks every other");
  }
  assert.ok(scout.stats.routeCapped > 0, "what was left unbought is counted, not hidden");
});

test("a reading never spreads past the stretch it was taken on", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const maxChunkMeters = 400;
  const scout = createRouteBackfill({
    engine, session: ingestSession, probe: fakeProbe(null), clock,
    maxChunkMeters, minChunkMeters: 0, maxProbesPerRoute: 64
  });
  const plan = await scout.assess(route);
  assert.ok(plan.chunks.length > 0);

  for (const chunk of plan.chunks) {
    // A chunk may exceed the bound only when a single edge already
    // does — there is nowhere shorter to put it.
    if (chunk.meters > maxChunkMeters) {
      assert.equal(chunk.edges.length, 1,
        "an over-long chunk is one indivisible edge, never an accumulation");
    }
    const classes = new Set(chunk.edges.map(entry => entry.roadClass));
    assert.equal(classes.size, 1, "a chunk is one class of road");
  }
});

test("a chunk too short to change a decision is not bought", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const backfill = createRouteBackfill({
    engine, session: ingestSession, probe: fakeProbe(null), clock,
    minChunkMeters: 100_000, maxProbesPerRoute: 64
  });
  const plan = await backfill.assess(route);
  assert.equal(plan.probes.length, 0, "nothing on this route clears an absurd length floor");
  assert.ok(backfill.stats.chunksTooShort > 0);
});

test("the caller's own policy gets the last word", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const route = await someRoute(engine);
  assert.ok(route);

  const seen = [];
  const backfill = createRouteBackfill({
    engine, session: ingestSession, probe: fakeProbe(null), clock,
    shouldProbe: ({ chunk }) => {
      seen.push(chunk.roadClass);
      return chunk.roadClass === "motorway";
    }
  });
  const plan = await backfill.assess(route);
  assert.ok(seen.length > 0, "the veto saw every candidate");
  for (const chunk of plan.probes) assert.equal(chunk.roadClass, "motorway");
  assert.ok(backfill.stats.vetoed > 0);
});

test("priceRoute answers the asking client from what it bought, not from the undrained mesh", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const from = centreOf(engine, 28);
  const to = centreOf(engine, 39);

  const probe = fakeProbe({
    currentSpeedKmh: 9, freeFlowSpeedKmh: 110, confidence: 0.95, roadClosure: false, frc: "FRC0"
  });
  const backfill = createRouteBackfill({
    engine, session: ingestSession, probe, clock,
    maxProbesPerRoute: 8, maxSegmentsPerRoute: 24, maxFreeflowMismatch: 10
  });

  const cold = await engine.route({ from, to, live: ingestSession.provider() });
  const { route, report, repriced } = await backfill.priceRoute({ from, to });

  assert.equal(repriced, true);
  assert.ok(report.states.length > 0, "gaps were bought");
  assert.ok(Math.round(route.seconds) !== Math.round(cold.seconds),
    "the jams that were bought changed this client's answer");

  // The trap this helper exists to close: nothing has been published
  // yet, so the mesh provider still knows nothing at all.
  const naive = await engine.route({ from, to, live: ingestSession.provider() });
  assert.equal(Math.round(naive.seconds), Math.round(cold.seconds),
    "re-routing off the mesh right after buying gets the pre-purchase answer");
  assert.ok(Math.round(route.seconds) > Math.round(naive.seconds),
    "priceRoute is the one that reflects the purchase");
});

test("priceRoute with nothing worth buying still returns a usable route", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  let now = Date.now();
  const clock = () => now;
  const { ingestSession } = await meshFixture(engine, clock);
  const from = centreOf(engine, 28);
  const to = centreOf(engine, 39);

  const probe = fakeProbe(null);   // the API has nothing for any point
  const backfill = createRouteBackfill({ engine, session: ingestSession, probe, clock });
  const { route, repriced } = await backfill.priceRoute({ from, to });

  assert.equal(repriced, false, "there was nothing to re-price on");
  assert.ok(route.seconds > 0, "and the caller still gets the route it asked for");
});

test("the TomTom probe reads the documented response and refuses to work around a block", async t => {
  const calls = [];
  const probe = createTomTomProbe({
    key: "test-key",
    minIntervalMillis: 0,
    fetchImpl: async url => {
      calls.push(url);
      if (calls.length === 1) {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            flowSegmentData: {
              frc: "FRC0", currentSpeed: 42, freeFlowSpeed: 100,
              currentTravelTime: 60, freeFlowTravelTime: 25,
              confidence: 0.97, roadClosure: false
            }
          })
        };
      }
      return { ok: false, status: 403, json: async () => ({}) };
    }
  });

  const reading = await probe({ lat: 46.8139, lon: -71.208 });
  assert.deepEqual(reading, {
    currentSpeedKmh: 42, freeFlowSpeedKmh: 100,
    confidence: 0.97, roadClosure: false, frc: "FRC0"
  });
  assert.match(calls[0], /flowSegmentData\/absolute\/10\/json/);
  assert.match(calls[0], /point=46\.813900,-71\.208000/);
  assert.match(calls[0], /unit=kmph/);

  // A 403 is an answer. It surfaces as an error the caller sees; there
  // is no retry, no second identity, no other route to the same bytes.
  await assert.rejects(() => probe({ lat: 46.8, lon: -71.2 }), /HTTP 403/);
  assert.equal(probe.stats.errors, 1);
  assert.equal(calls.length, 2, "a refusal is not retried");

  // The serializing chain survives the rejection.
  assert.equal(probe.stats.calls, 2);
});
