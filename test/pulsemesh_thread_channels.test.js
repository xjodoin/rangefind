// T4 — the rules between the two channels (threads §10).
//
// The headline assertion is the dwell-bias one: a corridor served by
// buses must not end up systematically pessimistic in the traffic layer
// because every bus reports 0 km/h at every stop. That failure is
// self-consistent — several buses on one corridor corroborate each
// other's false standstill — so it would survive corroboration, which is
// exactly why it needs a rule of its own.

import assert from "node:assert/strict";
import test from "node:test";
import {
  ROUTE_PUBLICITY,
  assertNeverBridged,
  createStopSuppressor,
  resolveContributionPolicy
} from "../src/pulsemesh/thread_contribute.js";
import { createThreadPublisher, THREAD_CONSTANTS } from "../src/pulsemesh/thread_publish.js";
import { THREAD_MODE, THREAD_STATE } from "../src/pulsemesh/thread_codec.js";
import { generateThreadKeypair } from "../src/pulsemesh/thread_crypto.js";
import { PulseMeshStore } from "../src/pulsemesh/store.js";
import { aggregateSegment } from "../src/pulsemesh/aggregate.js";
import { decodePMC1, encodePMC1 } from "../src/pulsemesh/codec.js";
import { speedBinFromKmh } from "../src/pulsemesh/bins.js";
import { sha256Utf8 } from "../src/pulsemesh/sha256.js";

const EPOCH32 = sha256Utf8("pulsemesh-thread-channels");
const EPOCH_PREFIX8 = EPOCH32.subarray(0, 8);

test("§10 rule 3: contribution is off by default and never inferred", () => {
  assert.equal(resolveContributionPolicy().contribute, false, "off unless explicitly enabled");
  assert.equal(resolveContributionPolicy({ enabled: false, publicity: ROUTE_PUBLICITY.PUBLISHED }).contribute, false);

  // Enabled but with no stated publicity: the protocol refuses to guess,
  // because guessing wrong means publishing a courier's customer list.
  const unstated = resolveContributionPolicy({ enabled: true });
  assert.equal(unstated.contribute, false);
  assert.match(unstated.reason, /never inferred/);

  // A published timetable already reveals the route, so there is no
  // anonymity to protect and the ordinary profile is right.
  const bus = resolveContributionPolicy({ enabled: true, publicity: ROUTE_PUBLICITY.PUBLISHED });
  assert.equal(bus.contribute, true);
  assert.equal(bus.profile, "cadence");

  // An unpublished route is a customer list: reticent is mandatory.
  const courier = resolveContributionPolicy({ enabled: true, publicity: ROUTE_PUBLICITY.UNPUBLISHED });
  assert.equal(courier.contribute, true);
  assert.equal(courier.profile, "reticent");
});

test("§10 rule 1: a thread update can never be shaped into a contribution", () => {
  // A plain thread update passes through untouched.
  assertNeverBridged({ unixSeconds: 1, state: 2, segment: "5/1/0", ratio: 0.5 });
  // Anything wearing a traffic record's clothes is refused loudly.
  assert.throws(
    () => assertNeverBridged({ segment: "5/1/0", qualityBin: 7, reportId: new Uint8Array(16) }),
    /traffic authority/
  );
});

test("§10 rule 4: a bus dwelling at stops does not poison the corridor", async () => {
  // Ground truth: the corridor flows at 50 km/h. Ordinary drivers report
  // that. Buses serve stops along it and dwell 30 s at each.
  const nowMillis = Math.floor(Date.now() / 15000) * 15000;
  const bucket = Math.floor(nowMillis / 15000);
  const segKey = "5/12";
  const stops = [
    { index: 1, lat: 45.500, lon: -76.500 },
    { index: 2, lat: 45.505, lon: -76.500 },
    { index: 3, lat: 45.510, lon: -76.500 }
  ];

  let reportId = 0;
  const contribution = speedKmh => decodePMC1(encodePMC1({
    epochPrefix8: EPOCH_PREFIX8,
    leafCell: 5,
    geomRef: 12,
    timeBucket: bucket,
    speedBin: speedBinFromKmh(speedKmh),
    qualityBin: 7,
    meters: 400,
    ttlSeconds: 90,
    reportId: Uint8Array.from({ length: 16 }, (_, i) => (i === 0 ? ++reportId : 0)),
    proofType: 0,
    proof: new Uint8Array(0)
  }).bytes);

  // --- Without the rule: three buses report their dwell -----------------
  const naive = new PulseMeshStore({ cellOf: () => ({ x: 1, y: 1 }) });
  for (let i = 0; i < 3; i++) naive.addContribution(contribution(50), { nowMillis, deliveredBy: `car-${i}` });
  for (let i = 0; i < 3; i++) naive.addContribution(contribution(0), { nowMillis, deliveredBy: `bus-${i}` });
  const poisoned = aggregateSegment(naive.contributionsForSegment(segKey), { nowMillis });

  // --- With the rule: the buses suppress at their stops ------------------
  const guarded = new PulseMeshStore({ cellOf: () => ({ x: 1, y: 1 }) });
  for (let i = 0; i < 3; i++) guarded.addContribution(contribution(50), { nowMillis, deliveredBy: `car-${i}` });
  let suppressed = 0;
  for (let i = 0; i < 3; i++) {
    const suppressor = createStopSuppressor({ stops, clock: () => nowMillis });
    // The bus is dwelling at stop 2, exactly where a naive contributor
    // would report a standstill on a flowing road.
    const verdict = suppressor.evaluate({
      lat: stops[1].lat, lon: stops[1].lon, speedMps: 0, dwelling: true, nowMillis
    });
    if (!verdict.contribute) suppressed++;
    else guarded.addContribution(contribution(0), { nowMillis, deliveredBy: `bus-${i}` });
  }
  assert.equal(suppressed, 3, "every dwelling bus suppressed its contribution");
  const clean = aggregateSegment(guarded.contributionsForSegment(segKey), { nowMillis });

  // The failure this rule exists to prevent, made concrete.
  assert.equal(clean.speedKmh, 52.5, "the corridor reads as flowing");
  assert.ok(
    poisoned.speedKmh < clean.speedKmh,
    `unsuppressed dwell time biases the corridor downward ` +
    `(${poisoned.speedKmh} km/h vs ${clean.speedKmh} km/h)`
  );
  assert.ok(poisoned.n >= 3, "and it survives corroboration — three buses agree with each other");
});

test("§10 rule 4: stop suppression covers arrival, dwell, and departure", () => {
  let now = Math.floor(Date.now() / 1000) * 1000;
  const stops = [{ index: 1, lat: 45.5, lon: -76.5 }];
  const suppressor = createStopSuppressor({ stops, clock: () => now });

  // Well away from any stop and moving: contribute.
  assert.equal(suppressor.evaluate({ lat: 45.52, lon: -76.5, speedMps: 12, nowMillis: now }).contribute, true);

  // Within STOP_RADIUS, still rolling in: suppressed.
  assert.equal(
    suppressor.evaluate({ lat: 45.50025, lon: -76.5, speedMps: 3, nowMillis: now }).reason,
    "at-stop"
  );
  // Dwelling: suppressed.
  assert.equal(
    suppressor.evaluate({ lat: 45.5, lon: -76.5, speedMps: 0, dwelling: true, nowMillis: now }).reason,
    "dwelling"
  );
  // Just departed: still suppressed for STOP_LINGER.
  now += 1000;
  const departing = suppressor.evaluate({ lat: 45.52, lon: -76.5, speedMps: 8, nowMillis: now });
  assert.equal(departing.reason, "just-departed");
  now += (THREAD_CONSTANTS.THREAD_STOP_LINGER + 1) * 1000;
  assert.equal(suppressor.evaluate({ lat: 45.52, lon: -76.5, speedMps: 8, nowMillis: now }).contribute, true);

  // A vehicle stopped for *traffic* between stops reports normally: that
  // is real congestion and precisely what the channel wants.
  assert.equal(
    suppressor.evaluate({ lat: 45.53, lon: -76.5, speedMps: 0, dwelling: false, nowMillis: now }).contribute,
    true
  );
});

test("§10 rule 4: the publisher itself reports when traffic may take a fix", async () => {
  const now = Math.floor(Date.now() / 1000) * 1000;
  let clockNow = now;
  const keypair = await generateThreadKeypair();
  const stops = [{ index: 1, lat: 45.5, lon: -76.5 }, { index: 2, lat: 45.52, lon: -76.5 }];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32: EPOCH32,
    mode: THREAD_MODE.COARSE,
    plan: { planRef: new Uint8Array(8), stops, dwellSeconds: 30 },
    clock: () => clockNow
  });

  // Rolling into stop 1 and dwelling: the thread publishes a stop event,
  // and the traffic channel is told to stay out of it.
  clockNow += 1000;
  const atStop = await publisher.handleFix({ lat: 45.5, lon: -76.5, speedMps: 0, nowMillis: clockNow });
  assert.equal(atStop.contributeTraffic, false, "no traffic contribution while serving a stop");
  assert.equal(atStop.published, true, "but the thread still reports the stop event");
  assert.equal(atStop.record.body.state, THREAD_STATE.DWELLING);
  assert.equal(atStop.record.body.stopIndex, 1);

  // The first fix away from the stop is where departure is *observed*,
  // so that is when the linger starts — it is still suppressed.
  clockNow += 5000;
  const departing = await publisher.handleFix({ lat: 45.512, lon: -76.5, speedMps: 13, nowMillis: clockNow });
  assert.equal(departing.contributeTraffic, false, "the departure fix itself is still suppressed");

  // Once the linger has elapsed, a bus between stops is an ordinary probe.
  clockNow += (THREAD_CONSTANTS.THREAD_STOP_LINGER + 5) * 1000;
  const rolling = await publisher.handleFix({ lat: 45.512, lon: -76.5, speedMps: 13, nowMillis: clockNow });
  assert.equal(rolling.contributeTraffic, true, "a bus between stops is an ordinary probe");
  assert.ok(publisher.stats.suppressedTraffic >= 2);
});

test("a coarse thread publishes stop events without a heartbeat storm", async () => {
  let now = Math.floor(Date.now() / 1000) * 1000;
  const keypair = await generateThreadKeypair();
  const stops = [{ index: 1, lat: 45.5, lon: -76.5 }, { index: 2, lat: 45.6, lon: -76.5 }];
  const emitted = [];
  const publisher = await createThreadPublisher({
    privateSeed: keypair.privateSeed,
    epoch32: EPOCH32,
    mode: THREAD_MODE.COARSE,
    plan: { planRef: new Uint8Array(8), stops, dwellSeconds: 30 },
    clock: () => now,
    publish: async record => emitted.push(record)
  });

  // Ten fixes 5 s apart, all mid-route: coarse mode must not publish ten
  // updates — that is the fine-mode cadence and would leak a trajectory.
  for (let i = 0; i < 10; i++) {
    now += 5000;
    await publisher.handleFix({ lat: 45.55 + i * 0.0001, lon: -76.5, speedMps: 12, nowMillis: now });
  }
  assert.ok(emitted.length <= 2, `coarse mode stays quiet between events (${emitted.length} updates)`);

  // Arriving at a stop is an event, and it publishes immediately.
  const before = emitted.length;
  now += 5000;
  await publisher.handleFix({ lat: 45.6, lon: -76.5, speedMps: 0, nowMillis: now });
  assert.equal(emitted.length, before + 1, "a stop event publishes at once");
  assert.equal(emitted[emitted.length - 1].body.state, THREAD_STATE.DWELLING);
  assert.equal(emitted[emitted.length - 1].body.leafCell, 0, "and still carries no position");
});
