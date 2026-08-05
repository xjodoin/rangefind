// Contributor state machine (§10.1), reticent gates (§10.2), PMF1
// forwarding (§4.5), and the §11.3 privacy batching.

import assert from "node:assert/strict";
import test from "node:test";
import { DEFAULT_CONSTANTS, detailCellForE7 } from "../src/pulsemesh/bins.js";
import { createContributor } from "../src/pulsemesh/contribute.js";
import { createReticentProfile } from "../src/pulsemesh/reticent.js";
import { createForwardHandler } from "../src/pulsemesh/forward.js";
import { createValidator } from "../src/pulsemesh/validate.js";
import { buildCellRequests, buildDecoyPool, diffDigest, requestOverhead } from "../src/pulsemesh/sync.js";
import { PulseMeshStore } from "../src/pulsemesh/store.js";
import { corridorCells } from "../src/pulsemesh/sync.js";
import { decodeAny, decodePMC1, encodePMB1, encodePMC1, encodePMF1 } from "../src/pulsemesh/codec.js";
import { sha256Utf8 } from "../src/pulsemesh/sha256.js";

const EPOCH32 = sha256Utf8("pulsemesh-test-vector");
const PREFIX8 = EPOCH32.slice(0, 8);

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function makeContributor(overrides = {}) {
  let now = 1754265600000;
  const emitted = [];
  const random = lcg(3);
  const contributor = createContributor({
    epoch32: EPOCH32,
    epochPrefix8: PREFIX8,
    snap: async fix => fix.match,
    publish: async result => emitted.push(result),
    proofType: 0,
    randomBytes: length => Uint8Array.from({ length }, () => Math.floor(random() * 256)),
    clock: () => now,
    constants: { ...DEFAULT_CONSTANTS, EMIT_INTERVAL: 15 },
    metersOf: () => 200,
    ...overrides
  });
  return { contributor, emitted, tick: ms => { now += ms; }, nowOf: () => now };
}

const goodMatch = (segment = "5/6/0") => ({
  segment, distMeters: 3, bearingDeg: 90, ratio: 0.4, snappedLatE7: 455000000, snappedLonE7: -736000000
});

test("contributor: match, quality, cadence, standstill", async () => {
  const { contributor, emitted, tick } = makeContributor();

  assert.equal((await contributor.handleFix({ match: null, speedMps: 10 })).reason, "off-road");
  assert.equal((await contributor.handleFix({ match: { ...goodMatch(), distMeters: 60 }, speedMps: 10 })).reason, "off-road");
  assert.equal(
    (await contributor.handleFix({ match: goodMatch(), speedMps: 10, courseDeg: 200 })).reason,
    "quality",
    "heading 110° off the edge bearing suppresses"
  );

  const first = await contributor.handleFix({ match: goodMatch(), speedMps: 10, courseDeg: 92 });
  assert.ok(first.emitted);
  const decoded = decodePMC1(first.record.bytes);
  assert.equal(decoded.segment, "5/6/0");
  assert.equal(decoded.meters, 200);
  assert.ok(decoded.speedBin >= 2 && decoded.speedBin <= 7, "smoothed speed lands in a plausible bin");

  assert.equal((await contributor.handleFix({ match: goodMatch(), speedMps: 10 })).reason, "cadence");
  tick(16 * 1000);
  assert.ok((await contributor.handleFix({ match: goodMatch(), speedMps: 10 })).emitted);

  // Standstill: bin 0 needs recent movement nearby — congestion, not parking.
  tick(16 * 1000);
  const standstill = await contributor.handleFix({ match: goodMatch(), speedMps: 0 });
  assert.ok(standstill.emitted, "standstill right after moving on the same segment is congestion");
  tick(120 * 1000);
  const parked = await contributor.handleFix({ match: goodMatch("9/2/0"), speedMps: 0 });
  assert.equal(parked.reason, "stationary-off-road", "no recent movement nearby is parking");
  assert.equal(emitted.length, 3);
});

test("contributor: incident reports come only from the reporter's own position", async () => {
  const { contributor, tick } = makeContributor();
  const report = (params) => contributor.reportIncident({ acknowledgedPublic: true, ...params });

  // §10.4: the protocol will not mint a report on an unacknowledged
  // disclosure, so "tell the user it is public and locating" cannot be
  // satisfied by a settings page nobody read.
  assert.equal(
    contributor.reportIncident({ type: 1 }).reason,
    "disclosure-not-acknowledged"
  );
  assert.equal(report({ type: 1 }).reason, "no-position", "no snap match, no report");
  await contributor.handleFix({ match: goodMatch(), speedMps: 10 });
  const filed = report({ type: 1 });
  assert.ok(filed.emitted);
  const decoded = decodeAny(filed.record.bytes);
  assert.equal(decoded.segment, "5/6/0");
  assert.equal(decoded.ratioQ12, Math.round(0.4 * 4095));
  assert.equal(report({ type: 1 }).reason, "rate", "one per (type, 5 min)");
  tick(5 * 60 * 1000 + 1000);
  await contributor.handleFix({ match: goodMatch(), speedMps: 10 });
  assert.ok(report({ type: 1 }).emitted);
  assert.equal(report({ type: 99 }).reason, "unknown-type");
  assert.equal(report({ type: 1, polarity: 2 }).reason, "no-incident", "confirm needs an incident key");
  assert.ok(report({ type: 2, polarity: 2, incidentKey: "k1" }).emitted);
  assert.equal(report({ type: 2, polarity: 3, incidentKey: "k1" }).reason, "already-answered");
});

test("contributor honours the deployment's suppressed incident types on emit", async () => {
  // §10.4 jurisdiction: a deployment that suppresses police reporting must
  // not mint those records either — dropping them only on receipt would
  // still have the client publishing what the deployment says it may not.
  const { contributor } = makeContributor({ suppressedTypes: [5] });
  await contributor.handleFix({ match: goodMatch(), speedMps: 10 });
  assert.equal(
    contributor.reportIncident({ type: 5, acknowledgedPublic: true }).reason,
    "suppressed-by-policy"
  );
  assert.ok(contributor.reportIncident({ type: 1, acknowledgedPublic: true }).emitted);
});

test("reticent gates: place, surprise, company, forwarding rotation", () => {
  let now = 1754265600000;
  const random = lcg(9);
  let expected = 16;       // ~82 km/h free flow
  let company = 0;
  const profile = createReticentProfile({
    expectedBinOf: () => expected,
    companyCountOf: () => company,
    isNearOwnStop: latE7 => latE7 === 1,
    forwarderPool: () => ["f1", "f2", "f3", "f4"],
    rng: random,
    clock: () => now
  });

  // Gate 1: residential class and own stops suppress outright.
  assert.equal(profile.evaluate({ segKey: "5/12", speedBin: 4, roadClass: "residential" }).gate, "place:class");
  assert.equal(profile.evaluate({ segKey: "5/12", speedBin: 4, snappedLatE7: 1, snappedLonE7: 2 }).gate, "place:stop");

  // Gate 2: a jam where free flow is expected is a surprise — emits even
  // with zero company.
  const surprise = profile.evaluate({ segKey: "5/12", speedBin: 2 });
  assert.ok(surprise.emit && surprise.surprise, "surprise bypasses the company gate");
  assert.equal(surprise.forwarder, "f1");

  // Gate 2 the other way: free flow where the aggregate says jam is also a
  // surprise — clearance is detected.
  expected = 2;
  now += 130 * 1000;
  const clearance = profile.evaluate({ segKey: "5/12", speedBin: 16 });
  assert.ok(clearance.emit && clearance.surprise);
  assert.equal(clearance.forwarder, "f2", "forwarders rotate per record");

  // Gate 3: unsurprising observations are sampled, spaced, and need company.
  expected = 16;
  now += 130 * 1000;
  company = 0;
  let verdicts = [];
  for (let i = 0; i < 200; i++) {
    now += 130 * 1000;
    verdicts.push(profile.evaluate({ segKey: "5/12", speedBin: 16 }));
  }
  assert.ok(verdicts.every(v => !v.emit), "alone: never emits regardless of sampling");
  company = 2;
  verdicts = [];
  for (let i = 0; i < 400; i++) {
    now += 130 * 1000;
    verdicts.push(profile.evaluate({ segKey: "5/12", speedBin: 16 }));
  }
  const emittedCount = verdicts.filter(v => v.emit).length;
  assert.ok(emittedCount > 5 && emittedCount < 60, `sampling near BASE_SAMPLE_RATE (got ${emittedCount}/400)`);

  // RETICENT_GAP: an emission blocks the next unsurprising one.
  now += 130 * 1000;
  let emittedOnce = false;
  for (let i = 0; i < 400 && !emittedOnce; i++) {
    now += 130 * 1000;
    emittedOnce = profile.evaluate({ segKey: "5/12", speedBin: 16 }).emit;
  }
  assert.ok(emittedOnce);
  now += 10 * 1000; // inside the gap
  assert.equal(profile.evaluate({ segKey: "5/12", speedBin: 16 }).gate, "company:gap");

  // §10.4: filing a report suppresses measurements for RETICENT_GAP, and
  // that outranks the surprise bypass — a reporter is most likely sitting
  // in a surprise, which is exactly the pairing the rule breaks.
  now += 200 * 1000;
  profile.suppressAfterReport(now);
  expected = 16;
  assert.equal(
    profile.evaluate({ segKey: "5/12", speedBin: 2 }).gate,
    "report:suppression",
    "a surprise does not escape post-report suppression"
  );
  now += 121 * 1000;
  assert.ok(profile.evaluate({ segKey: "5/12", speedBin: 2 }).emit, "suppression lifts after RETICENT_GAP");
});

test("reticent gate 4 suppresses rather than silently publishing direct", () => {
  let now = 1754265600000;
  // A contributor that chose this profile asked not to publish directly.
  // With no forwarder available the correct answer is to emit nothing —
  // falling back to a direct publish would quietly drop the protection
  // the profile was selected for.
  const profile = createReticentProfile({
    expectedBinOf: () => 16,
    companyCountOf: () => 5,
    forwarderPool: () => [],
    rng: () => 0,
    clock: () => now
  });
  const verdict = profile.evaluate({ segKey: "5/12", speedBin: 2 });
  assert.equal(verdict.emit, false);
  assert.equal(verdict.gate, "forward:no-pool");
});

test("forwarder: validates, holds, republishes; refuses chains and floods", () => {
  let now = 1754265600000;
  const random = lcg(21);
  const constants = { ...DEFAULT_CONSTANTS };
  const validator = createValidator({
    constants, epoch32: EPOCH32, transport: "loopback", clock: () => now
  });
  const republished = [];
  const scheduled = [];
  const handler = createForwardHandler({
    validator,
    constants,
    clock: () => now,
    schedule: (fn, delayMs) => scheduled.push({ fn, delayMs }),
    publishAsOwn: (payload, meta) => republished.push({ payload, meta })
  });

  const record = encodePMC1({
    epochPrefix8: PREFIX8, leafCell: 5, geomRef: 12, timeBucket: Math.floor(now / 15000),
    speedBin: 6, qualityBin: 7, meters: 100, ttlSeconds: 90,
    reportId: Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256)),
    proofType: 0, proof: new Uint8Array(0)
  });
  const batch = encodePMB1([record.bytes]);

  const ok = handler.handle({ delayMs: 3000, payload: batch }, { fromPeer: "src" });
  assert.ok(ok.forwarded);
  assert.equal(scheduled[0].delayMs, 3000);
  scheduled[0].fn();
  assert.equal(republished.length, 1);
  assert.ok(republished[0].meta.viaForward);

  assert.equal(
    handler.handle({ delayMs: 60000, payload: batch }, { fromPeer: "src" }).reason,
    "delay-too-long"
  );
  const chained = encodePMF1({ epochPrefix8: PREFIX8, delayMs: 0, payload: batch });
  assert.equal(
    handler.handle({ delayMs: 0, payload: chained }, { fromPeer: "src" }).reason,
    "chained-forward",
    "one hop only, no chains"
  );
  const invalid = encodePMB1([encodePMC1({
    epochPrefix8: PREFIX8, leafCell: 5, geomRef: 12, timeBucket: Math.floor(now / 15000),
    speedBin: 45, qualityBin: 7, meters: 100, ttlSeconds: 90,
    reportId: Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256)),
    proofType: 0, proof: new Uint8Array(0)
  }).bytes]);
  assert.match(
    handler.handle({ delayMs: 0, payload: invalid }, { fromPeer: "src" }).reason,
    /invalid-inner:rule3/,
    "a forwarder never becomes an amplifier"
  );

  // FORWARD_RATE per source per minute.
  let refused = null;
  for (let i = 0; i < 10; i++) {
    const fresh = encodePMB1([encodePMC1({
      epochPrefix8: PREFIX8, leafCell: 5, geomRef: 12, timeBucket: Math.floor(now / 15000),
      speedBin: 6, qualityBin: 7, meters: 100, ttlSeconds: 90,
      reportId: Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256)),
      proofType: 0, proof: new Uint8Array(0)
    }).bytes]);
    const verdict = handler.handle({ delayMs: 0, payload: fresh }, { fromPeer: "flood" });
    if (!verdict.forwarded) { refused = verdict; break; }
  }
  assert.equal(refused?.reason, "rate");
});

test("forwarder rejects a replay it has already stored", () => {
  // Without the store the forwarder skips rule 6 and becomes a replay
  // amplifier: its own store dedups afterwards, but the gossip has gone.
  let now = 1754265600000;
  const random = lcg(31);
  const store = new PulseMeshStore({ cellOf: () => ({ x: 9000, y: 11000 }) });
  const validator = createValidator({ epoch32: EPOCH32, transport: "loopback", clock: () => now });
  const handler = createForwardHandler({
    validator, store, clock: () => now,
    schedule: fn => fn(),
    publishAsOwn: () => {}
  });
  const record = encodePMC1({
    epochPrefix8: PREFIX8, leafCell: 5, geomRef: 12, timeBucket: Math.floor(now / 15000),
    speedBin: 6, qualityBin: 7, meters: 100, ttlSeconds: 90,
    reportId: Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256)),
    proofType: 0, proof: new Uint8Array(0)
  });
  const batch = encodePMB1([record.bytes]);
  assert.ok(handler.handle({ delayMs: 0, payload: batch }, { fromPeer: "src" }).forwarded);
  store.addContribution(decodePMC1(record.bytes), { nowMillis: now, deliveredBy: "src" });
  assert.match(
    handler.handle({ delayMs: 0, payload: batch }, { fromPeer: "src2" }).reason,
    /invalid-inner:rule6/,
    "a forwarder is not a replay amplifier"
  );
});

test("§11.3 privacy batching: padded, shuffled, split, legal batch sizes", () => {
  const random = lcg(5);
  const wanted = Array.from({ length: 23 }, (_, i) => ({ x: 9000 + i, y: 11000 }));
  const peers = ["a", "b", "c", "d", "e"];
  const decoyPool = buildDecoyPool(wanted, [{ x: 8000, y: 10000 }]);
  assert.ok(decoyPool.length > 40, "2-ring neighborhoods plus visited cells");
  assert.ok(!decoyPool.some(cell => cell.y === 11000 && cell.x >= 9000 && cell.x <= 9022), "wanted cells are never decoys");

  const requests = buildCellRequests({ wanted, peers, decoyPool, rng: random });
  assert.ok(requests.length >= 2, "split across peers");
  const usedPeers = new Set(requests.map(request => request.peer));
  assert.ok(usedPeers.size >= 2 && usedPeers.size <= DEFAULT_CONSTANTS.SPLIT_PEERS);
  for (const request of requests) {
    assert.ok([8, 16, 32].includes(request.cells.length), `batch of ${request.cells.length} is a legal size`);
  }
  const fetchedKeys = new Set(requests.flatMap(request => request.cells.map(cell => `${cell.x}/${cell.y}`)));
  for (const cell of wanted) assert.ok(fetchedKeys.has(`${cell.x}/${cell.y}`), "every wanted cell is fetched");

  const overhead = requestOverhead(requests, wanted.length);
  assert.ok(overhead.decoys / overhead.fetched >= 0.2, `decoy share ${(overhead.decoys / overhead.fetched).toFixed(2)}`);

  // A single wanted cell still travels padded — endpoints never go bare.
  const single = buildCellRequests({ wanted: [{ x: 1, y: 1 }], peers, decoyPool, rng: random });
  assert.equal(single.length, 1);
  assert.equal(single[0].cells.length, 8);
});

test("§11.2 corridor rasterizes routes and broadens endpoints", () => {
  const route = [];
  for (let i = 0; i <= 40; i++) route.push({ lat: 45.5 + i * 0.002, lon: -73.6 + i * 0.003 });
  const cells = corridorCells({ routes: [route] });
  assert.ok(cells.length > 50, `corridor rasterizes to many cells (got ${cells.length})`);

  // Endpoints carry the most information about a query, so they are the
  // one place the request must never be narrow: ENDPOINT_RINGS worth of
  // cells around each end are present whether or not the corridor
  // touched them.
  const keys = new Set(cells.map(cell => `${cell.x}/${cell.y}`));
  const rings = DEFAULT_CONSTANTS.ENDPOINT_RINGS;
  for (const endpoint of [route[0], route[route.length - 1]]) {
    const cell = detailCellForE7(endpoint.lat * 1e7, endpoint.lon * 1e7);
    for (let dy = -rings; dy <= rings; dy++) {
      for (let dx = -rings; dx <= rings; dx++) {
        assert.ok(keys.has(`${cell.x + dx}/${cell.y + dy}`), `endpoint ring cell ${dx},${dy} present`);
      }
    }
  }
  assert.deepEqual(corridorCells({ routes: [] }), [], "no routes, no cells");
  assert.deepEqual(corridorCells({ routes: [[{ lat: 1, lon: 1 }]] }), [], "a single point is not a corridor");
});

test("digest diff finds missing, differing, and fresher cells", () => {
  const idFold = value => Uint8Array.from({ length: 8 }, () => value);
  const local = {
    zoneX: 144, zoneY: 179, baseBucket: 100,
    entries: [
      { localX: 1, localY: 1, count: 3, ageBuckets: 0, idFold: idFold(1) },
      { localX: 2, localY: 2, count: 5, ageBuckets: 1, idFold: idFold(2) }
    ]
  };
  const remote = {
    zoneX: 144, zoneY: 179, baseBucket: 100,
    entries: [
      { localX: 1, localY: 1, count: 3, ageBuckets: 0, idFold: idFold(1) },  // identical
      { localX: 2, localY: 2, count: 5, ageBuckets: 0, idFold: idFold(9) },  // fold differs
      { localX: 3, localY: 3, count: 2, ageBuckets: 0, idFold: idFold(3) }   // missing locally
    ]
  };
  const wanted = diffDigest(local, remote);
  assert.deepEqual(
    wanted.map(cell => `${cell.x}/${cell.y}`).sort(),
    [`${144 * 64 + 2}/${179 * 64 + 2}`, `${144 * 64 + 3}/${179 * 64 + 3}`].sort()
  );
});
