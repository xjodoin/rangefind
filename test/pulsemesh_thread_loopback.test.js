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
  THREAD_MODE,
  THREAD_STATE,
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

  now += 1000;
  await publisher.handleFix({ lat: 45.55, lon: -76.58, speedMps: 10, nowMillis: now });
  await subscriber.accept(emitted[0].bytes, { nowMillis: now });
  assert.equal(subscriber.status({ nowMillis: now, hasTraffic: true }).row, "thread+traffic");

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
