// T2 — loopback thread. A publisher and a subscriber in one process over
// an in-memory duplex, on the real OSM route graph.
//
// The assertion that matters is the last one: the subscriber's ETA moves
// when a jam is injected into the *traffic* channel's provider. That is
// the §9 claim — arrival time is a local route query under the other
// channel's live metric — and it is the entire reason this channel lives
// in rangefind rather than in a tracking product.

import assert from "node:assert/strict";
import test from "node:test";
import { openRouteGraphDir } from "../src/route_graph_node.js";
import { createStaticLiveProvider } from "../src/route_graph_query.js";
import { createThreadPublisher, THREAD_CONSTANTS } from "../src/pulsemesh/thread_publish.js";
import { createThreadSubscriber } from "../src/pulsemesh/thread_consume.js";
import { estimateArrival, scheduledArrival } from "../src/pulsemesh/thread_eta.js";
import {
  STOP_OUTCOME,
  STOP_REASON,
  THREAD_MODE,
  THREAD_STATE,
  THREAD_TRAVEL_MODE,
  decodeThreadLink,
  encodeThreadLink,
  encodeThreadRecord,
  threadRecordAad
} from "../src/pulsemesh/thread_codec.js";
import {
  deriveThreadKeys,
  generateThreadKeypair,
  sealThreadBody,
  signThread,
  threadTag,
  threadWindow
} from "../src/pulsemesh/thread_crypto.js";
import { encodeThreadBody, encodeThreadBodyPreimage } from "../src/pulsemesh/thread_codec.js";
import { fromHex, sha256Utf8 } from "../src/pulsemesh/sha256.js";

const GRAPH_DIR = "examples/osm-geo/public/route-graph";

// An in-memory duplex: the publisher writes, subscribers read. No
// networking at all — T3 covers discovery and catch-up.
function loopback() {
  const listeners = new Set();
  return {
    subscribe(fn) { listeners.add(fn); return () => listeners.delete(fn); },
    async publish(bytes) {
      for (const listener of [...listeners]) await listener(bytes);
    },
    get size() { return listeners.size; }
  };
}

async function graphFixture(t) {
  let engine;
  try {
    engine = await openRouteGraphDir(GRAPH_DIR);
  } catch {
    t.skip(`route graph fixture missing at ${GRAPH_DIR}`);
    return null;
  }
  return engine;
}

/** A short run along a real route: stops taken from an actual path. */
async function buildRun(engine) {
  const center = leaf => {
    const bbox = engine.root.leaves[leaf].bbox;
    return { lat: (bbox.minLat + bbox.maxLat) / 2 / 1e7, lon: (bbox.minLon + bbox.maxLon) / 2 / 1e7 };
  };
  for (let attempt = 0; attempt < 40; attempt++) {
    const from = center((attempt * 13) % engine.root.leaves.length);
    const to = center((attempt * 13 + 18) % engine.root.leaves.length);
    let route;
    try {
      route = await engine.route({ from, to });
    } catch {
      continue;
    }
    if (!route.edges || route.edges.length < 20 || !route.geometry?.length) continue;
    // Five stops spread along the driven geometry.
    const stops = [];
    for (let i = 0; i < 5; i++) {
      const point = route.geometry[Math.floor((route.geometry.length - 1) * (i / 4))];
      stops.push({
        index: i + 1, lat: point[0], lon: point[1],
        plannedUnixSeconds: Math.floor(Date.now() / 1000) + i * 300
      });
    }
    return { route, stops, from, to };
  }
  return null;
}

test("T2: publisher → subscriber over a loopback, with a live-traffic ETA", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const run = await buildRun(engine);
  if (!run) {
    t.skip("no suitable route in the fixture graph");
    return;
  }

  const epoch32 = fromHex(engine.root.sourceHash);
  const keypair = await generateThreadKeypair();
  const plan = { planRef: sha256Utf8("run-plan-1").subarray(0, 8), stops: run.stops, dwellSeconds: 20 };

  // Anchored to real time: the engine blends live-state confidence
  // against wall-clock age, so a virtual clock in the past would decay
  // every jam to nothing before it could move an ETA.
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const channel = loopback();

  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32,
    mode: THREAD_MODE.FINE,
    plan,
    clock,
    publish: async emitted => channel.publish(emitted.bytes)
  });

  // The link is the entire capability, and it is what a parent receives.
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey,
    epochPrefix8: epoch32.subarray(0, 8),
    notAfter: Math.floor(now / 1000) + 3600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32, clock });
  const received = [];
  channel.subscribe(async bytes => {
    const verdict = await subscriber.accept(bytes, { nowMillis: now });
    received.push(verdict);
  });

  // Drive the run: fixes along the real route geometry.
  const geometry = run.route.geometry;
  for (let i = 0; i < geometry.length; i += Math.ceil(geometry.length / 12)) {
    const [lat, lon] = geometry[i];
    const snapped = await engine.snap({ lat, lon });
    now += 6000;
    await publisher.handleFix({ lat, lon, speedMps: 11, nowMillis: now, match: snapped.matches[0] });
  }

  const accepted = received.filter(verdict => verdict.ok);
  assert.ok(accepted.length >= 5, `subscriber accepted updates (${accepted.length})`);
  assert.equal(subscriber.stats.dropped, 0, "no honest update was dropped");

  const latest = subscriber.latest();
  assert.ok(latest, "subscriber holds a position");
  assert.equal(latest.mode, THREAD_MODE.FINE);
  assert.ok(latest.segment, "fine mode reports a snapped segment");

  // The position decodes back to real coordinates through engine.locate.
  const at = await engine.locate(latest.segment, latest.ratio);
  assert.ok(Number.isFinite(at.lat) && Number.isFinite(at.lon));

  // §12: with a thread and no traffic, the honest claim is a
  // static-metric ETA.
  const status = subscriber.status({ nowMillis: now, hasTraffic: false });
  assert.equal(status.row, "thread-only");
  assert.equal(status.claim, "live position, static-metric ETA");
  assert.ok(status.live);

  // --- The thesis: the ETA responds to the *traffic* channel ------------
  const myStop = run.stops[run.stops.length - 1].index;
  const staticEta = await estimateArrival({
    engine, update: latest, plan, myStopIndex: myStop, live: null, nowMillis: now
  });
  assert.ok(staticEta, "an arrival estimate exists under the static metric");
  assert.equal(staticEta.basis, "static-metric");
  assert.equal(staticEta.positionBasis, "reported-position");
  assert.ok(staticEta.secondsFromObservation > 0);

  // Jam the remaining corridor in the traffic channel — a completely
  // separate channel the publisher knows nothing about.
  const remainingSegments = run.route.edges.map(edge => edge.segment);
  const jam = createStaticLiveProvider(
    remainingSegments.map(segment => ({
      segment, speedMps: 1.8, meters: 400, confidence: 1, observedAt: now
    })),
    { epoch: engine.root.sourceHash }
  );
  const liveEta = await estimateArrival({
    engine, update: latest, plan, myStopIndex: myStop, live: jam, nowMillis: now
  });
  assert.ok(liveEta, "an arrival estimate exists under the live metric");
  assert.equal(liveEta.basis, "live-traffic");
  assert.ok(
    liveEta.secondsFromObservation > staticEta.secondsFromObservation,
    `traffic on the remaining legs pushes the arrival later ` +
    `(${staticEta.secondsFromObservation.toFixed(0)}s → ${liveEta.secondsFromObservation.toFixed(0)}s)`
  );

  // And with both channels the subscriber may make the strongest claim.
  assert.equal(subscriber.status({ nowMillis: now, hasTraffic: true }).claim, "live position, live-traffic ETA");

  // Nothing the subscriber sent carries its stop: it sent nothing at all.
  // The ETA was computed entirely locally from a broadcast position.
  assert.equal(channel.size, 1, "the subscriber is a listener, not a participant");
});

test("T2: the delivery shape (one courier, one recipient) is live-aware", async t => {
  // §3.2 is an audience of one and a single destination, so the ETA is a
  // two-point query — a different code path inside matrix() from the
  // multi-stop school run. If it silently ignored live traffic, every
  // courier ETA would be a free-flow fantasy.
  const engine = await graphFixture(t);
  if (!engine) return;
  const run = await buildRun(engine);
  if (!run) { t.skip("no suitable route"); return; }

  const epoch32 = fromHex(engine.root.sourceHash);
  const keypair = await generateThreadKeypair();
  const doorstep = run.stops[run.stops.length - 1];
  const plan = { planRef: sha256Utf8("delivery-1").subarray(0, 8), stops: [doorstep], dwellSeconds: 0 };
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;

  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32, mode: THREAD_MODE.FINE, plan, clock,
    publish: async record => emitted.push(record)
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey, epochPrefix8: epoch32.subarray(0, 8), notAfter: Math.floor(now / 1000) + 1800
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32, clock });

  const start = run.route.geometry[0];
  const snapped = await engine.snap({ lat: start[0], lon: start[1] });
  now += 5000;
  await publisher.handleFix({
    lat: start[0], lon: start[1], speedMps: 9, nowMillis: now, match: snapped.matches[0]
  });
  assert.ok((await subscriber.accept(emitted[0].bytes, { nowMillis: now })).ok);

  const update = subscriber.latest();
  const staticEta = await estimateArrival({
    engine, update, plan, myStopIndex: doorstep.index, live: null, nowMillis: now
  });
  assert.ok(staticEta, "a two-point arrival estimate exists");
  assert.equal(staticEta.stopsAway, 1);

  const jam = createStaticLiveProvider(
    run.route.edges.map(edge => ({
      segment: edge.segment, speedMps: 1.8, meters: 400, confidence: 1, observedAt: now
    })),
    { epoch: engine.root.sourceHash }
  );
  const liveEta = await estimateArrival({
    engine, update, plan, myStopIndex: doorstep.index, live: jam, nowMillis: now
  });
  assert.ok(
    liveEta.secondsFromObservation > staticEta.secondsFromObservation,
    `the courier's ETA responds to traffic ` +
    `(${staticEta.secondsFromObservation.toFixed(0)}s → ${liveEta.secondsFromObservation.toFixed(0)}s)`
  );
});

test("T2: coarse mode withholds position and still estimates arrival", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const run = await buildRun(engine);
  if (!run) { t.skip("no suitable route"); return; }

  const epoch32 = fromHex(engine.root.sourceHash);
  const keypair = await generateThreadKeypair();
  const plan = { planRef: sha256Utf8("run-plan-2").subarray(0, 8), stops: run.stops, dwellSeconds: 20 };
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;

  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32,
    mode: THREAD_MODE.COARSE,
    plan,
    clock,
    publish: async record => emitted.push(record)
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey, epochPrefix8: epoch32.subarray(0, 8), notAfter: Math.floor(now / 1000) + 3600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32, clock });

  // Arrive at stop 2 and dwell — a coarse stop event.
  const stop = run.stops[1];
  now += 5000;
  await publisher.handleFix({ lat: stop.lat, lon: stop.lon, speedMps: 0, nowMillis: now });
  for (const record of emitted) {
    const verdict = await subscriber.accept(record.bytes, { nowMillis: now });
    assert.ok(verdict.ok, `coarse update accepted: ${verdict.reason || ""}`);
  }
  const latest = subscriber.latest();
  assert.equal(latest.mode, THREAD_MODE.COARSE);
  assert.equal(latest.leafCell, 0, "§11: a coarse record carries no position at all");
  assert.equal(latest.segment, null);
  assert.equal(latest.state, THREAD_STATE.DWELLING);
  assert.equal(latest.stopIndex, stop.index, "but it does say which stop");

  // Arrival still estimable — from the last stop rather than a position,
  // which is exactly the accuracy trade coarse mode makes.
  const eta = await estimateArrival({
    engine, update: latest, plan, myStopIndex: run.stops[4].index, live: null, nowMillis: now
  });
  assert.ok(eta, "coarse mode still produces an arrival estimate");
  assert.equal(eta.positionBasis, "last-stop");
  assert.equal(eta.stopsAway, 3);
});

test("T2: an outcome is asserted, never inferred, and reaches the follower at once", async () => {
  // Delivery is the one thing on this channel a vehicle's movement cannot
  // establish. Dwelling near a door says the van stopped; it does not say
  // the parcel was handed over, refused, or taken back to the depot. So
  // dwell detection moves `stopIndex` and nothing else, and every outcome
  // on the wire came from the driver pressing a button.
  const epoch32 = sha256Utf8("outcome-epoch");
  const keypair = await generateThreadKeypair();
  const stops = [
    { index: 1, lat: 45.5000, lon: -73.5500 },
    { index: 2, lat: 45.5100, lon: -73.5600 },
    { index: 3, lat: 45.5200, lon: -73.5700 },
    { index: 4, lat: 45.5300, lon: -73.5800 },
    { index: 5, lat: 45.5400, lon: -73.5900 }
  ];
  const plan = { planRef: sha256Utf8("outcome-plan").subarray(0, 8), stops, dwellSeconds: 30 };
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;

  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32, mode: THREAD_MODE.FINE, plan, clock,
    travelMode: THREAD_TRAVEL_MODE.BIKE,
    publish: async record => emitted.push(record)
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey, epochPrefix8: epoch32.subarray(0, 8), notAfter: Math.floor(now / 1000) + 3600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32, clock });
  const feed = async () => {
    while (emitted.length) {
      const record = emitted.shift();
      const verdict = await subscriber.accept(record.bytes, { nowMillis: now });
      assert.ok(verdict.ok, `record accepted: ${verdict.reason || ""}`);
    }
  };

  // Dwelling at stop 1 advances progress and writes no outcome at all.
  now += 1000;
  await publisher.handleFix({ ...stops[0], speedMps: 0, nowMillis: now });
  await feed();
  assert.equal(subscriber.latest().stopIndex, 1, "dwell detection still infers progress");
  assert.deepEqual(subscriber.latest().outcomes, [0, 0, 0, 0, 0],
    "…and asserts nothing about what happened at the door");
  assert.equal(subscriber.latest().lastOutcome, null);
  assert.equal(subscriber.latest().travelMode, THREAD_TRAVEL_MODE.BIKE);

  // Marking emits immediately, bypassing the cadence: no fix, no wait.
  const seqBefore = publisher.seq;
  now += 500; // far inside THREAD_UPDATE_FINE
  await publisher.markStop(1, STOP_OUTCOME.DELIVERED);
  assert.equal(publisher.seq, seqBefore + 1, "a mark publishes on the spot, not on the next heartbeat");
  await feed();
  assert.deepEqual(subscriber.latest().outcomes, [1, 0, 0, 0, 0]);
  assert.equal(subscriber.latest().stopIndex, 1);
  assert.deepEqual(subscriber.latest().lastOutcome,
    { stopIndex: 1, outcome: STOP_OUTCOME.DELIVERED, reasonCode: STOP_REASON.NONE, hasPhoto: false });

  // A delivery further down the run is an outcome, not an arrival. The
  // customer at stop 5 rang ahead and the parcel went to a neighbour; the
  // van is still standing at stop 1, and `stopIndex` says so. If it
  // jumped to 5 here, every follower at stops 2 to 4 would compute
  // "already served" and be shown nothing at all.
  now += 60_000;
  await publisher.markStop(5, STOP_OUTCOME.DELIVERED);
  await feed();
  assert.equal(subscriber.latest().stopIndex, 1,
    "pre-marking stop 5 records the outcome without moving the vehicle to it");
  assert.deepEqual(subscriber.latest().outcomes, [1, 0, 0, 0, 1]);

  // The paperwork catching up: stop 3, marked late. The outcome lands and
  // the vehicle does not teleport, forwards or backwards, down the plan.
  now += 5000;
  await publisher.markStop(3, STOP_OUTCOME.SKIPPED, { reason: STOP_REASON.CUSTOMER_ABSENT });
  await feed();
  const late = subscriber.latest();
  assert.deepEqual(late.outcomes, [1, 0, 2, 0, 1]);
  assert.equal(late.stopIndex, 1, "marking an earlier stop never regresses progress…");
  assert.deepEqual(late.lastOutcome,
    { stopIndex: 3, outcome: STOP_OUTCOME.SKIPPED, reasonCode: STOP_REASON.CUSTOMER_ABSENT, hasPhoto: false });

  // Resolving stop 2 — the one the run is actually on — advances by one
  // and then hops the stop already marked behind it. Progress is the
  // contiguous resolved prefix, which is exactly "dealt with in order".
  now += 5000;
  await publisher.markStop(2, STOP_OUTCOME.DELIVERED);
  await feed();
  assert.equal(subscriber.latest().stopIndex, 3,
    "…and resolving stop 2 advances over the already-marked stop 3");

  // Re-marking overwrites: a failed attempt that is later delivered is an
  // ordinary day, not a protocol violation.
  now += 5000;
  await publisher.markStop(3, STOP_OUTCOME.FAILED, {
    reason: STOP_REASON.OTHER, note: "gate code wrong"
  });
  now += 5000;
  await publisher.markStop(3, STOP_OUTCOME.DELIVERED);
  await feed();
  assert.deepEqual(subscriber.latest().outcomes, [1, 1, 1, 0, 1], "the retry overwrites the failure");
  assert.equal(subscriber.latest().lastOutcome.outcome, STOP_OUTCOME.DELIVERED);
  assert.equal(subscriber.latest().stopIndex, 3, "stop 4 is still pending, so the run is still at 3");

  // Outcomes are cumulative in *every* record, so the whole day is in the
  // newest one — a follower who joined late never has to replay history.
  assert.deepEqual(subscriber.status({ nowMillis: now }).outcomes, [1, 1, 1, 0, 1]);
  assert.equal(subscriber.status({ nowMillis: now }).travelMode, THREAD_TRAVEL_MODE.BIKE);

  await assert.rejects(() => publisher.markStop(0, STOP_OUTCOME.DELIVERED), /not on this run's plan/u);
  await assert.rejects(() => publisher.markStop(6, STOP_OUTCOME.DELIVERED), /not on this run's plan/u);
  await assert.rejects(() => publisher.markStop(2, STOP_OUTCOME.PENDING), /never back to pending/u);
  await assert.rejects(() => publisher.markStop(2, STOP_OUTCOME.SKIPPED, { reason: 99 }), /reason code/u);
});

test("T2: a skipped stop leaves the ETA chain, and never gets an arrival time", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const run = await buildRun(engine);
  if (!run) { t.skip("no suitable route"); return; }

  const epoch32 = fromHex(engine.root.sourceHash);
  const keypair = await generateThreadKeypair();
  // Three stops off the real driven geometry, renumbered 1..3.
  const stops = [run.stops[0], run.stops[2], run.stops[4]]
    .map((stop, i) => ({ ...stop, index: i + 1 }));
  const plan = { planRef: sha256Utf8("skip-plan").subarray(0, 8), stops, dwellSeconds: 120 };
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;

  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32, mode: THREAD_MODE.FINE, plan, clock,
    publish: async record => emitted.push(record)
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey, epochPrefix8: epoch32.subarray(0, 8), notAfter: Math.floor(now / 1000) + 3600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32, clock });

  // The customer at stop 2 rang ahead: not home today. The driver marks it
  // before setting off, then drives the route normally — so the vehicle's
  // reported progress is behind a stop that is already known to be dead.
  now += 1000;
  await publisher.markStop(2, STOP_OUTCOME.SKIPPED, { reason: STOP_REASON.CUSTOMER_ABSENT });
  now += 5000;
  const start = run.route.geometry[0];
  const snapped = await engine.snap({ lat: start[0], lon: start[1] });
  await publisher.handleFix({
    lat: start[0], lon: start[1], speedMps: 9, nowMillis: now, match: snapped.matches[0]
  });
  now += 1000;
  await publisher.handleFix({ ...stops[0], speedMps: 0, nowMillis: now });
  for (const record of emitted) {
    assert.ok((await subscriber.accept(record.bytes, { nowMillis: now })).ok);
  }

  const update = subscriber.latest();
  assert.equal(update.stopIndex, 1, "the vehicle is at stop 1…");
  assert.deepEqual(update.outcomes, [0, STOP_OUTCOME.SKIPPED, 0], "…with stop 2 already written off");

  // The customer at stop 3. Their van is not stopping at stop 2, so
  // neither the leg detour nor the 120 s dwell belongs in their wait.
  const skipping = await estimateArrival({
    engine, update, plan, myStopIndex: 3, live: null, nowMillis: now
  });
  const asIfPending = await estimateArrival({
    engine, update: { ...update, outcomes: [0, 0, 0] }, plan, myStopIndex: 3, live: null, nowMillis: now
  });
  assert.ok(skipping && asIfPending);
  assert.equal(skipping.stopsAway, 1, "one stop left, not two");
  assert.equal(asIfPending.stopsAway, 2);
  assert.ok(
    skipping.secondsFromObservation < asIfPending.secondsFromObservation - 100,
    "the skipped stop's leg and its 120 s dwell leave the chain "
    + `(${asIfPending.secondsFromObservation.toFixed(0)}s → ${skipping.secondsFromObservation.toFixed(0)}s)`
  );

  // The customer at stop 2 themselves. There is no arrival to render, and
  // the shape makes it impossible to render one anyway.
  const mine = await estimateArrival({
    engine, update, plan, myStopIndex: 2, live: null, nowMillis: now
  });
  assert.ok(mine, "a skipped stop gets an answer, not silence");
  assert.equal(mine.basis, "marked");
  assert.equal(mine.arrivalMillis, null, "there is no time at which this van arrives");
  assert.equal(mine.secondsFromNow, null);
  assert.equal(mine.outcome, STOP_OUTCOME.SKIPPED);
  assert.equal(mine.outcomeName, "skipped");
  assert.equal(mine.reasonCode, STOP_REASON.CUSTOMER_ABSENT);

  // A delivered stop is behind them: null, exactly as an already-served
  // stop has always been.
  now += 6000;
  await publisher.markStop(1, STOP_OUTCOME.DELIVERED);
  const delivered = emitted[emitted.length - 1];
  assert.ok((await subscriber.accept(delivered.bytes, { nowMillis: now })).ok);
  assert.equal(
    await estimateArrival({
      engine, update: subscriber.latest(), plan, myStopIndex: 1, live: null, nowMillis: now
    }),
    null
  );
});

/**
 * Stops on a north-south line, one minute of driving per degree. Fixture
 * free and exactly linear, because the claim under test is *which stops
 * end up in the chain*, not how long any road is.
 */
function lineEngine({ secondsPerDegree = 60 } = {}) {
  return {
    async locate() { throw new Error("this run reports no position"); },
    async matrix({ points }) {
      return {
        seconds: points.map(a => points.map(b => Math.abs(a.lat - b.lat) * secondsPerDegree))
      };
    }
  };
}

test("T2: pre-marking a far stop does not unserve the customers between", async () => {
  // The failure this exists to prevent: a dispatcher marks stop 3 while
  // the van is still at stop 1 — the customer rang ahead, or the parcel
  // never made it onto the van — and every follower between the two is
  // told their delivery is already behind the vehicle. `stopIndex` is a
  // position in the plan (§5.2), so it stays at 1 and the outcome for
  // stop 3 lives in the map where it belongs.
  const epoch32 = sha256Utf8("premark-epoch");
  const keypair = await generateThreadKeypair();
  const stops = [1, 2, 3, 4].map(index => ({ index, lat: index, lon: 0 }));
  const plan = { planRef: sha256Utf8("premark-plan").subarray(0, 8), stops, dwellSeconds: 120 };
  const engine = lineEngine();
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;

  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32, mode: THREAD_MODE.FINE, plan, clock,
    publish: async record => emitted.push(record)
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey,
    epochPrefix8: epoch32.subarray(0, 8),
    notAfter: Math.floor(now / 1000) + 3600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32, clock });

  now += 1000;
  await publisher.handleFix({ lat: 1, lon: 0, speedMps: 0, nowMillis: now });
  now += 1000;
  await publisher.markStop(3, STOP_OUTCOME.SKIPPED, { reason: STOP_REASON.CUSTOMER_ABSENT });
  for (const record of emitted) {
    assert.ok((await subscriber.accept(record.bytes, { nowMillis: now })).ok);
  }

  const update = subscriber.latest();
  assert.equal(update.stopIndex, 1, "the van is at stop 1, whatever the paperwork says about stop 3");
  assert.deepEqual(update.outcomes, [0, 0, STOP_OUTCOME.SKIPPED, 0]);

  // The customer between the van and the pre-marked stop. This is the
  // one that used to come back null.
  const between = await estimateArrival({
    engine, update, plan, myStopIndex: 2, live: null, nowMillis: now
  });
  assert.ok(between, "a customer in front of the vehicle still gets an arrival");
  assert.equal(between.stopsAway, 1, "one leg: the van's next stop is theirs");
  assert.equal(between.secondsFromObservation, 60);
  assert.equal(between.positionBasis, "last-stop");

  // The customer downstream of it. Their van is not stopping at stop 3,
  // so neither its dwell nor a second dwell belongs in their wait — but
  // stop 2's does.
  const downstream = await estimateArrival({
    engine, update, plan, myStopIndex: 4, live: null, nowMillis: now
  });
  const asIfPending = await estimateArrival({
    engine, update: { ...update, outcomes: [0, 0, 0, 0] }, plan, myStopIndex: 4, live: null, nowMillis: now
  });
  assert.equal(downstream.stopsAway, 2, "stops 2 and 4; stop 3 is not on the chain");
  assert.equal(downstream.secondsFromObservation, 60 + 120 + 120);
  assert.equal(asIfPending.stopsAway, 3);
  assert.equal(
    asIfPending.secondsFromObservation - downstream.secondsFromObservation,
    plan.dwellSeconds,
    "exactly the skipped stop's dwell leaves the chain — the legs are collinear"
  );

  // The pre-marked stop itself: the marked shape, with no time in it.
  const mine = await estimateArrival({
    engine, update, plan, myStopIndex: 3, live: null, nowMillis: now
  });
  assert.equal(mine.basis, "marked");
  assert.equal(mine.arrivalMillis, null);
  assert.equal(mine.outcome, STOP_OUTCOME.SKIPPED);
  assert.equal(mine.reasonCode, STOP_REASON.CUSTOMER_ABSENT);

  // And the shape of the bug, stated directly: had the mark jumped
  // `stopIndex` to 3, the customer at stop 2 would be behind the vehicle.
  assert.equal(
    await estimateArrival({
      engine, update: { ...update, stopIndex: 3 }, plan, myStopIndex: 2, live: null, nowMillis: now
    }),
    null,
    "a stop at or before stopIndex is dealt with — which is why marking must not move it"
  );
});

test("T2: an ETA says which routing profile it was computed under", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const run = await buildRun(engine);
  if (!run) { t.skip("no suitable route"); return; }

  // A car graph applied to a bike courier overstates every arrival, and
  // the follower cannot tell unless the answer says so. The rule is not
  // "refuse to answer" — a host with one graph is not wrong to answer —
  // it is "never answer silently".
  const stops = [run.stops[0], run.stops[4]].map((stop, i) => ({ ...stop, index: i + 1 }));
  const plan = { stops, dwellSeconds: 0 };
  const now = Math.floor(Date.now() / 1000) * 1000;
  const start = run.route.geometry[0];
  const match = (await engine.snap({ lat: start[0], lon: start[1] })).matches[0];
  const update = {
    unixSeconds: Math.floor(now / 1000),
    state: THREAD_STATE.EN_ROUTE,
    mode: THREAD_MODE.FINE,
    travelMode: THREAD_TRAVEL_MODE.BIKE,
    segment: match.segment,
    ratio: match.ratio || 0,
    stopIndex: 1,
    outcomes: [0, 0],
    lastOutcome: null
  };

  // One graph, unnamed: the honest answer is that nobody said.
  const unstated = await estimateArrival({ engine, update, plan, myStopIndex: 2, nowMillis: now });
  assert.equal(unstated.profileBasis, "unstated");
  assert.equal(unstated.profile, null);
  assert.equal(unstated.travelModeName, "bike");
  assert.equal(unstated.travelModeMismatch, false, "we cannot claim a mismatch we did not establish");

  // One graph, named: now the mismatch is a fact and the result says it.
  const mismatched = await estimateArrival({
    engine, engineMode: "car", update, plan, myStopIndex: 2, nowMillis: now
  });
  assert.equal(mismatched.profileBasis, "mismatched");
  assert.equal(mismatched.profile, "car");
  assert.equal(mismatched.travelModeMismatch, true);

  // A host holding several profiles routes the bike run on the bike graph.
  // (The fixture has one graph; passing it under both names proves the
  // selection, not the cycling speeds.)
  const matched = await estimateArrival({
    engines: { car: engine, bike: engine }, update, plan, myStopIndex: 2, nowMillis: now
  });
  assert.equal(matched.profileBasis, "matched");
  assert.equal(matched.profile, "bike");
  assert.equal(matched.travelModeMismatch, false);

  // A run that never said how it moves cannot mismatch anything.
  const silent = await estimateArrival({
    engine, engineMode: "car",
    update: { ...update, travelMode: THREAD_TRAVEL_MODE.UNSPECIFIED },
    plan, myStopIndex: 2, nowMillis: now
  });
  assert.equal(silent.travelModeName, null);
  assert.equal(silent.profileBasis, "unstated");
  assert.equal(silent.travelModeMismatch, false);
});

test("T2: subscriber rejects forgeries, replays, and expired links", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const epoch32 = fromHex(engine.root.sourceHash);
  const keypair = await generateThreadKeypair();
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;
  const plan = { planRef: new Uint8Array(8), stops: [], dwellSeconds: 0 };

  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32, mode: THREAD_MODE.FINE, plan, clock,
    publish: async record => emitted.push(record)
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey, epochPrefix8: epoch32.subarray(0, 8), notAfter: Math.floor(now / 1000) + 600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32, clock });

  now += 1000;
  await publisher.handleFix({ lat: 45.55, lon: -76.58, speedMps: 10, nowMillis: now });
  const first = emitted[0];
  assert.ok((await subscriber.accept(first.bytes, { nowMillis: now })).ok);

  // Replay of the same record: seq is no longer strictly increasing.
  const replay = await subscriber.accept(first.bytes, { nowMillis: now });
  assert.equal(replay.ok, false);
  assert.equal(replay.step, 7, "replay caught by the sequence ledger");

  // A link holder forging an update: seals correctly, signs with its own
  // key, and is caught at step 6.
  const keys = await deriveThreadKeys(publisher.publicKey);
  const forger = await generateThreadKeypair();
  const forgedBody = {
    unixSeconds: Math.floor(now / 1000), state: THREAD_STATE.EN_ROUTE, mode: THREAD_MODE.FINE,
    leafCell: 5, geomRef: 2, ratioQ12: 100, speedBin: 4, stopIndex: 3,
    planRef: new Uint8Array(8), note: new Uint8Array(0)
  };
  const preimage = encodeThreadBodyPreimage(forgedBody);
  const body = encodeThreadBody(forgedBody, await signThread(preimage, forger.privateSeed));
  const tag = await threadTag(keys, epoch32, threadWindow(now));
  const aad = threadRecordAad(epoch32.subarray(0, 8), tag, 500);
  const record = encodeThreadRecord({
    epochPrefix8: epoch32.subarray(0, 8), tag, seq: 500,
    ciphertext: await sealThreadBody(keys, 500, aad, body)
  });
  const forgery = await subscriber.accept(record.bytes, { nowMillis: now });
  assert.equal(forgery.ok, false);
  assert.equal(forgery.step, 6, "a sealed-but-unsigned record is caught at the signature");
  assert.equal(subscriber.stats.forgeries, 1, "and counted as an attack, not an error");
  assert.equal(subscriber.highestSeq, first.seq, "the forgery did not advance the sequence ledger");

  // An unknown tag is dropped before any crypto runs.
  const strangerRecord = encodeThreadRecord({
    epochPrefix8: epoch32.subarray(0, 8), tag: new Uint8Array(8).fill(9), seq: 900,
    ciphertext: new Uint8Array(32)
  });
  const unknown = await subscriber.accept(strangerRecord.bytes, { nowMillis: now });
  assert.equal(unknown.step, 3, "unknown tags are the cheap drop");

  // Past notAfter, the capability is over.
  now += 601 * 1000;
  await publisher.handleFix({ lat: 45.55, lon: -76.58, speedMps: 10, nowMillis: now });
  const late = await subscriber.accept(emitted[emitted.length - 1].bytes, { nowMillis: now });
  assert.equal(late.ok, false);
  assert.equal(late.step, 8, "updates past the link's expiry are refused");
  assert.ok(subscriber.expired(now));
});

test("T2: §12 degradation rows are distinguishable, and stale is not live", async t => {
  const engine = await graphFixture(t);
  if (!engine) return;
  const epoch32 = fromHex(engine.root.sourceHash);
  const keypair = await generateThreadKeypair();
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;

  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32, mode: THREAD_MODE.FINE,
    plan: { planRef: new Uint8Array(8), stops: [], dwellSeconds: 0 }, clock,
    publish: async record => emitted.push(record)
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey, epochPrefix8: epoch32.subarray(0, 8), notAfter: Math.floor(now / 1000) + 7200
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32, clock });

  assert.equal(subscriber.status({ nowMillis: now }).row, "neither", "before anything arrives");
  assert.equal(subscriber.status({ nowMillis: now, hasTraffic: true }).row, "traffic-only");

  // A real position, from the fixture graph: the top two rows are the
  // ones that say "live position", so reaching them requires actually
  // having one. Publishing coordinates the graph cannot match leaves the
  // position withheld, and a run with no position belongs in the third
  // row however live it is.
  const leaf = engine.root.leaves[3].bbox;
  const here = { lat: (leaf.minLat + leaf.maxLat) / 2 / 1e7, lon: (leaf.minLon + leaf.maxLon) / 2 / 1e7 };
  const match = (await engine.snap(here)).matches[0];

  now += 1000;
  await publisher.handleFix({ ...here, speedMps: 10, nowMillis: now, match });
  await subscriber.accept(emitted[0].bytes, { nowMillis: now });
  const located = subscriber.status({ nowMillis: now, hasTraffic: true });
  assert.equal(located.hasPosition, true);
  assert.equal(located.row, "thread+traffic");

  // Past THREAD_STALE the UI must stop claiming live.
  now += (THREAD_CONSTANTS.THREAD_STALE + 5) * 1000;
  const stale = subscriber.status({ nowMillis: now, hasTraffic: true });
  assert.equal(stale.live, false);
  assert.equal(stale.row, "traffic-only");
  assert.ok(!stale.claim.includes("live position"));

  // And with no live data at all, the plan is still an answer.
  const scheduled = scheduledArrival({
    plan: { stops: [{ index: 3, lat: 45.5, lon: -76.5, plannedUnixSeconds: 1754266000 }] },
    myStopIndex: 3
  });
  assert.equal(scheduled.basis, "schedule");
  assert.equal(scheduled.arrivalMillis, 1754266000 * 1000);
});

test("T2: a day that was abandoned says cancelled, and never says arrived", async () => {
  // The claim under test is the one the app used to get wrong: a run the
  // driver walked away from published COMPLETED, so every customer holding
  // a link was told their parcel had arrived. COMPLETED and CANCELED are
  // different sentences about the world and the wire has to keep them
  // apart — including the driver's reason, which is the only thing a
  // dispatcher can act on.
  const epoch32 = sha256Utf8("cancel-epoch");
  const keypair = await generateThreadKeypair();
  const stops = [
    { index: 1, lat: 45.5000, lon: -73.5500 },
    { index: 2, lat: 45.5100, lon: -73.5600 },
    { index: 3, lat: 45.5200, lon: -73.5700 }
  ];
  const plan = { planRef: sha256Utf8("cancel-plan").subarray(0, 8), stops, dwellSeconds: 30 };
  let now = Math.floor(Date.now() / 1000) * 1000;
  const clock = () => now;

  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed, epoch32, mode: THREAD_MODE.FINE, plan, clock,
    publish: async record => emitted.push(record)
  });
  const link = decodeThreadLink(encodeThreadLink({
    publicKey: publisher.publicKey, epochPrefix8: epoch32.subarray(0, 8), notAfter: Math.floor(now / 1000) + 3600
  }));
  const subscriber = await createThreadSubscriber({ link, epoch32, clock });
  const feed = async () => {
    while (emitted.length) {
      const verdict = await subscriber.accept(emitted.shift().bytes, { nowMillis: now });
      assert.ok(verdict.ok, `record accepted: ${verdict.reason || ""}`);
    }
  };

  // One delivery done, two doorsteps left, and then the van breaks down.
  now += 1000;
  await publisher.markStop(1, STOP_OUTCOME.DELIVERED);
  await feed();
  assert.equal(subscriber.latest().state, THREAD_STATE.EN_ROUTE);

  now += 60_000;
  await publisher.finish({ nowMillis: now, canceled: true, note: "van broke down" });
  await feed();

  const latest = subscriber.latest();
  assert.equal(latest.state, THREAD_STATE.CANCELED, "the run says cancelled");
  assert.notEqual(latest.state, THREAD_STATE.COMPLETED, "and never says arrived");
  assert.equal(new TextDecoder().decode(latest.note), "van broke down",
    "the driver's reason survives to the follower, which is what a dispatcher acts on");
  // The delivery that did happen is still on the record: cancelling a day
  // does not retract the parcels already handed over.
  assert.deepEqual(latest.outcomes, [STOP_OUTCOME.DELIVERED, STOP_OUTCOME.PENDING, STOP_OUTCOME.PENDING]);

  // §9: no arrival may be rendered for a run that is over, whichever way
  // it ended — a waiting customer must not be shown a time.
  const eta = await estimateArrival({
    engine: { locate: async () => ({ lat: 45.51, lon: -73.56 }), matrix: async () => ({ seconds: [[0, 60], [60, 0]] }) },
    update: latest, plan, myStopIndex: 3, nowMillis: now
  });
  assert.equal(eta, null, "a cancelled run offers no arrival time at all");
});
