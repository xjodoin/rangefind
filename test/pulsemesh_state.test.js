// M1: store, deterministic aggregation, validation rules, incident
// scoring — including the milestone property test: two stores fed the
// same records in different orders emit identical digests and aggregates.

import assert from "node:assert/strict";
import test from "node:test";
import { DEFAULT_CONSTANTS, applyBootstrapConstants } from "../src/pulsemesh/bins.js";
import { PulseMeshStore } from "../src/pulsemesh/store.js";
import { TrustLedger, aggregateSegment, contributionWeight } from "../src/pulsemesh/aggregate.js";
import { createValidator } from "../src/pulsemesh/validate.js";
import {
  INCIDENT_TYPES,
  applyContradictionDecay,
  incidentPenaltySeconds,
  scoreIncidentKey
} from "../src/pulsemesh/incidents.js";
import { decodePMC1, encodePMC1, encodePMD1, minePow } from "../src/pulsemesh/codec.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const EPOCH32 = sha256Utf8("pulsemesh-test-vector");
const PREFIX8 = EPOCH32.slice(0, 8);

// Deterministic pseudo-random report ids.
function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function reportId(random) {
  return Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256));
}

const cellOf = record => ({ x: 9000 + (record.leafCell % 4), y: 11000 + (record.geomRef % 4) });

test("§13.3 aggregation vector is reproduced exactly", async () => {
  // bucketAgeSeconds(bucket, now) = floor(now/1000) − bucket·15, so a
  // bucket of (nowSec − age)/15 yields exactly `age` — fractional buckets
  // are fine for this arithmetic check (only the bin math matters here).
  const nowSec = 1754265600;
  const nowMillis = nowSec * 1000;
  const mk = (hexByte, speedBin, quality, age) => ({
    record: {
      reportId: Uint8Array.from({ length: 16 }, () => hexByte),
      speedBin,
      qualityBin: quality,
      timeBucket: (nowSec - age) / 15,
      meters: 184
    },
    deliveredBy: `peer-${hexByte}`
  });
  const entries = [mk(0xdd, 10, 3, 30), mk(0xee, 6, 6, 44), mk(0xaa, 7, 6, 5), mk(0xcc, 7, 5, 20), mk(0xbb, 8, 7, 12)];
  const agg = aggregateSegment(entries, { nowMillis });
  assert.equal(agg.speedBin, 7);
  assert.equal(agg.totalWeight, 2986450000);
  assert.equal(agg.agreementNum, 2652600000);
  assert.ok(Math.abs(agg.confidence - 0.680962) < 1e-5);
  assert.equal(agg.confidenceBin, 5);
  assert.equal(agg.speedKmh, 37.5);
  assert.equal(agg.meters, 184);
});

test("hint aggregates (n = 2) cap confidence at 0.30; n ≤ 1 produces nothing", () => {
  const nowMillis = 1754265600000;
  const bucket = nowMillis / 15000;
  const mk = idByte => ({
    record: { reportId: Uint8Array.from({ length: 16 }, () => idByte), speedBin: 4, qualityBin: 7, timeBucket: bucket, meters: 0 },
    deliveredBy: `p${idByte}`
  });
  assert.equal(aggregateSegment([mk(1)], { nowMillis }), null);
  const hint = aggregateSegment([mk(1), mk(2)], { nowMillis });
  assert.ok(hint.hint);
  assert.ok(hint.confidence <= 0.30 + 1e-12);
});

test("M1 property: identical record sets in different orders converge to identical digests and aggregates", () => {
  const nowMillis = 1754265600000;
  const random = lcg(7);
  const records = [];
  for (let i = 0; i < 400; i++) {
    const { bytes } = encodePMC1({
      epochPrefix8: PREFIX8,
      leafCell: 1 + Math.floor(random() * 6),
      geomRef: Math.floor(random() * 8),
      timeBucket: Math.floor(nowMillis / 15000) - Math.floor(random() * 3),
      speedBin: Math.floor(random() * 20),
      qualityBin: 1 + Math.floor(random() * 7),
      meters: 100 + Math.floor(random() * 100),
      ttlSeconds: 90,
      reportId: reportId(random),
      proofType: 0,
      proof: new Uint8Array(0)
    });
    records.push(bytes);
  }
  const storeA = new PulseMeshStore({ cellOf });
  const storeB = new PulseMeshStore({ cellOf });
  for (const bytes of records) storeA.addContribution(decodePMC1(bytes), { nowMillis });
  for (const bytes of [...records].reverse()) storeB.addContribution(decodePMC1(bytes), { nowMillis });

  const zones = [{ x: 9000 >> 6, y: 11000 >> 6 }];
  const digestA = encodePMD1({ epochPrefix8: PREFIX8, ...storeA.digestForZone(zones[0], nowMillis) });
  const digestB = encodePMD1({ epochPrefix8: PREFIX8, ...storeB.digestForZone(zones[0], nowMillis) });
  assert.equal(toHex(digestA), toHex(digestB), "digests are order-independent");

  assert.deepEqual(storeA.liveSegmentKeys().sort(), storeB.liveSegmentKeys().sort());
  for (const segKey of storeA.liveSegmentKeys()) {
    const a = aggregateSegment(storeA.contributionsForSegment(segKey), { nowMillis });
    const b = aggregateSegment(storeB.contributionsForSegment(segKey), { nowMillis });
    assert.deepEqual(
      a && { bin: a.speedBin, total: a.totalWeight, conf: a.confidenceBin, meters: a.meters },
      b && { bin: b.speedBin, total: b.totalWeight, conf: b.confidenceBin, meters: b.meters },
      `aggregate for ${segKey} is order-independent`
    );
  }
});

test("store: dedup, TTL expiry, per-segment cap eviction", () => {
  const nowMillis = 1754265600000;
  const random = lcg(11);
  const constants = { ...DEFAULT_CONSTANTS, SEG_CONTRIB_CAP: 4 };
  const store = new PulseMeshStore({ constants, cellOf });
  const mk = (speedBin, ttlSeconds = 90) => decodePMC1(encodePMC1({
    epochPrefix8: PREFIX8, leafCell: 5, geomRef: 12, timeBucket: Math.floor(nowMillis / 15000),
    speedBin, qualityBin: 7, meters: 100, ttlSeconds, reportId: reportId(random),
    proofType: 0, proof: new Uint8Array(0)
  }).bytes);

  const first = mk(6);
  assert.ok(store.addContribution(first, { nowMillis }).added);
  assert.equal(store.addContribution(first, { nowMillis }).reason, "duplicate");
  assert.ok(store.hasReport(toHex(first.reportId)));

  for (let i = 0; i < 6; i++) store.addContribution(mk(6), { nowMillis });
  assert.equal(store.contributionsForSegment("5/12").length, 4, "per-segment cap enforced");

  // Sender-shortened TTL expires early; receiver clamps to CONTRIB_TTL.
  const short = mk(6, 10);
  store.addContribution(short, { nowMillis });
  store.sweep(nowMillis + 11 * 1000);
  assert.ok(!store.hasReport(toHex(short.reportId)), "short TTL honoured");
  store.sweep(nowMillis + 91 * 1000);
  assert.equal(store.size(), 0, "everything gone after CONTRIB_TTL");
});

test("validation: rules fire in order with the right numbers", () => {
  const nowMillis = 1754265600000;
  const random = lcg(13);
  const constants = applyBootstrapConstants({ POW_DIFFICULTY: 8 });
  const store = new PulseMeshStore({ constants, cellOf });
  const cellContext = leafCell => leafCell === 5
    ? {
        polylineCount: 10,
        classOf: geomRef => ((geomRef >>> 1) === 9 ? "footway" : "secondary"),
        metersOf: () => 150
      }
    : null;
  const validator = createValidator({
    constants, epoch32: EPOCH32, cellOf, cellContext, transport: "wire", clock: () => nowMillis
  });

  function build(overrides = {}, mine = true) {
    const fields = {
      epochPrefix8: PREFIX8, leafCell: 5, geomRef: 12, timeBucket: Math.floor(nowMillis / 15000),
      speedBin: 10, qualityBin: 7, meters: 150, ttlSeconds: 90, reportId: reportId(random),
      proofType: 1, proof: new Uint8Array(8), ...overrides
    };
    if (mine) {
      const { preimage } = encodePMC1(fields);
      fields.proof = minePow(preimage, EPOCH32, constants.POW_DIFFICULTY).nonce;
    }
    return decodePMC1(encodePMC1(fields).bytes);
  }

  assert.ok(validator.validateContribution(build(), { store, fromPeer: "p1", nowMillis }).ok);

  const badEpoch = build({ epochPrefix8: sha256Utf8("other").slice(0, 8) });
  assert.equal(validator.validateContribution(badEpoch, { nowMillis }).rule, 2);

  assert.equal(validator.validateContribution(build({ speedBin: 45 }), { nowMillis }).rule, 3);
  assert.equal(validator.validateContribution(build({ qualityBin: 0 }), { nowMillis }).rule, 3);
  assert.equal(validator.validateContribution(build({ ttlSeconds: 0 }), { nowMillis }).rule, 3);

  const old = build({ timeBucket: Math.floor(nowMillis / 15000) - 4 });
  assert.equal(validator.validateContribution(old, { nowMillis }).rule, 4);
  const future = build({ timeBucket: Math.floor(nowMillis / 15000) + 2 });
  assert.equal(validator.validateContribution(future, { nowMillis }).rule, 4);

  const noProof = build({ proofType: 0 }, false);
  assert.equal(validator.validateContribution(noProof, { nowMillis }).rule, 5, "proofType 0 rejected on the wire");
  const badPow = build({ proof: new Uint8Array(8) }, false);
  assert.equal(validator.validateContribution(badPow, { nowMillis }).rule, 5);

  const replayed = build();
  store.addContribution(replayed, { nowMillis });
  assert.equal(validator.validateContribution(replayed, { store, nowMillis }).rule, 6);

  // Rule 10: 52.5 km/h representative on a secondary is fine; 200 km/h is not.
  const implausible = build({ speedBin: 40 });
  const r10 = validator.validateContribution(implausible, { nowMillis });
  assert.equal(r10.rule, 10);
  assert.ok(r10.trustPenalty);
  const badMeters = build({ meters: 400 });
  assert.equal(validator.validateContribution(badMeters, { nowMillis }).rule, 10);

  // Rule 11: segment beyond the leaf's polyline count.
  const missing = build({ geomRef: 25 });
  assert.equal(validator.validateContribution(missing, { nowMillis }).rule, 11);

  // Rule 12: denied class, whatever the sender claims.
  const denied = build({ geomRef: 18 });
  assert.equal(validator.validateContribution(denied, { nowMillis }).rule, 12);

  // Rule 7: burst bucket drains.
  const flooder = "flood-peer";
  let dropped = null;
  for (let i = 0; i < DEFAULT_CONSTANTS.RATE_BURST + 5; i++) {
    const verdict = validator.validateContribution(build(), { store, fromPeer: flooder, nowMillis });
    if (!verdict.ok) { dropped = verdict; break; }
  }
  assert.equal(dropped?.rule, 7);
});

test("bootstrap constants clamp to ±4× and ignore fixed rows", () => {
  const merged = applyBootstrapConstants({
    POW_DIFFICULTY: 8,           // 20/4 = 5 min, ok
    MAX_AGE_RECEIPT: 1000,       // > 45×4, ignored
    BUCKET_SECONDS: 60,          // fixed row, ignored
    SHARDS: 8                    // fixed row, ignored
  });
  assert.equal(merged.POW_DIFFICULTY, 8);
  assert.equal(merged.MAX_AGE_RECEIPT, 45);
  assert.equal(merged.BUCKET_SECONDS, 15);
  assert.equal(merged.SHARDS, 16);
});

test("§13.5 incident scoring: min(raw, sources), refutes, decay, anchor", () => {
  const nowMillis = 1754265600000;
  const bucket = Math.floor(nowMillis / 15000);
  const random = lcg(17);
  const mk = (peer, polarity, viaForward = false) => ({
    record: {
      reportId: reportId(random), timeBucket: bucket, polarity, type: 1,
      leafCell: 5, geomRef: 12, ratioQ12: 2048, segment: "5/6/0"
    },
    deliveredBy: peer,
    viaForward,
    expiresAt: nowMillis + 900 * 1000,
    contradictedAtBucket: -1
  });

  let scored = scoreIncidentKey([mk("A", 1), mk("A", 2), mk("A", 2), mk("B", 2)], { nowMillis });
  assert.equal(scored.raw, 4);
  assert.equal(scored.sources, 2);
  assert.equal(scored.score, 2, "peer A's three records buy exactly one point");
  assert.ok(!scored.displayed && scored.hint);

  scored = scoreIncidentKey([mk("A", 1), mk("A", 2), mk("A", 2), mk("B", 2), mk("C", 2)], { nowMillis });
  assert.equal(scored.score, 3);
  assert.ok(scored.displayed);

  scored = scoreIncidentKey([mk("A", 1), mk("A", 2), mk("A", 2), mk("B", 2), mk("C", 2), mk("D", 3)], { nowMillis });
  assert.equal(scored.raw, 3, "refute costs REFUTE_WEIGHT");
  assert.equal(scored.score, 3, "one dissent against three sources is not decisive");

  // Forwarded-only keys cap at the hint score.
  scored = scoreIncidentKey([mk("A", 1, true), mk("B", 2, true), mk("C", 2, true)], { nowMillis });
  assert.equal(scored.score, 1, "forwarded reports never reach the display threshold alone");

  // The speed anchor: flowing traffic → zero penalty at any score.
  const displayedIncident = scoreIncidentKey([mk("A", 1), mk("B", 2), mk("C", 2)], { nowMillis });
  const congested = { n: 5, speedKmh: 12.5, newestBucket: bucket, speedMps: 12.5 / 3.6 };
  const flowing = { n: 5, speedKmh: 87.5, newestBucket: bucket };
  assert.equal(incidentPenaltySeconds([displayedIncident], flowing, 90), 0, "no penalty on a flowing road, ever");
  assert.equal(incidentPenaltySeconds([displayedIncident], null, 90), 0, "no aggregate, no penalty");
  assert.equal(incidentPenaltySeconds([displayedIncident], congested, 90), 120, "crash penalty unlocks under congestion");
  const hintIncident = scoreIncidentKey([mk("A", 1), mk("B", 2)], { nowMillis });
  assert.equal(incidentPenaltySeconds([hintIncident], congested, 90), 0, "hints never route");

  // Informational types never route even when displayed and congested.
  const police = scoreIncidentKey(
    [{ ...mk("A", 1), record: { ...mk("A", 1).record, type: 5 } },
     { ...mk("B", 2), record: { ...mk("B", 2).record, type: 5 } },
     { ...mk("C", 2), record: { ...mk("C", 2).record, type: 5 } }],
    { nowMillis }
  );
  assert.ok(police.displayed);
  assert.equal(incidentPenaltySeconds([police], congested, 90), 0);

  // Contradiction decay quarters remaining TTL, once per aggregate bucket.
  const entries = [mk("A", 1), mk("B", 2), mk("C", 2)];
  const scoredForDecay = scoreIncidentKey(entries, { nowMillis });
  const before = entries[0].expiresAt;
  assert.ok(applyContradictionDecay(scoredForDecay, flowing, 90, { nowMillis }));
  const after = entries[0].expiresAt;
  assert.ok(Math.abs((after - nowMillis) - (before - nowMillis) / 4) < 1);
  assert.ok(!applyContradictionDecay(scoredForDecay, flowing, 90, { nowMillis }), "idempotent per aggregate observation");
});

test("an appliesBoth incident reported from both directions counts once", async () => {
  // Ice does not care which way you are driving, so a slippery-surface
  // report applies to both directions of the physical segment. Scoring
  // each direction as its own key would double both the score and the
  // penalty for what is one incident on one road.
  const { createPulseMeshProvider } = await import("../src/pulsemesh/provider.js");
  const { decodePMI1, encodePMI1 } = await import("../src/pulsemesh/codec.js");
  const nowMillis = Math.floor(Date.now() / 15000) * 15000;
  const random = lcg(19);
  const store = new PulseMeshStore({ cellOf: () => ({ x: 9000, y: 11000 }) });

  // geomRef 12 and 13 are the two directions of polyline 6 in leaf 5.
  const file = (geomRef, peer) => {
    const record = decodePMI1(encodePMI1({
      epochPrefix8: PREFIX8, leafCell: 5, geomRef, ratioQ12: 2048,
      timeBucket: Math.floor(nowMillis / 15000), type: 9, polarity: 1,
      ttlSeconds: 1800, reportId: reportId(random), proofType: 0, proof: new Uint8Array(0)
    }).bytes);
    store.addIncident(record, { nowMillis, deliveredBy: peer });
  };
  // Three peers report it heading one way, two report it heading back.
  file(12, "A"); file(12, "B"); file(12, "C");
  file(13, "D"); file(13, "E");

  const provider = createPulseMeshProvider({
    epochHex: "0".repeat(64),
    store,
    clock: () => nowMillis,
    freeflowKmhOf: () => 90
  });
  const displayed = provider.displayIncidents({ nowMillis });
  const slippery = displayed.filter(incident => incident.type === 9);
  assert.equal(slippery.length, 1, "one physical incident, one scored key");
  assert.equal(slippery[0].sources, 5, "all five peers corroborate the same incident");
  assert.equal(
    incidentPenaltySeconds(slippery, { n: 5, speedKmh: 20, newestBucket: Math.floor(nowMillis / 15000) }, 90),
    45,
    "slippery-surface penalty applied once, not once per direction"
  );
});

test("provider reports states outside the areas the engine has fetched", async () => {
  // The areas the engine passes are the leaves it has fetched *so far*.
  // Filtering output to them would hide a jam one leaf ahead — exactly
  // the case the engine's "leaves referenced by live states join the
  // context set, and their overlay shortcuts are suppressed" mechanism
  // exists to handle. Areas drive fetching, never filtering.
  const { createPulseMeshProvider } = await import("../src/pulsemesh/provider.js");
  const { decodePMC1 } = await import("../src/pulsemesh/codec.js");
  const nowMillis = Math.floor(Date.now() / 15000) * 15000;
  const random = lcg(23);
  const epochHex = "c".repeat(64);

  // Records live in a cell far from any area the engine will describe.
  const store = new PulseMeshStore({ cellOf: () => ({ x: 20000, y: 12000 }) });
  for (let i = 0; i < 3; i++) {
    const record = decodePMC1(encodePMC1({
      epochPrefix8: PREFIX8, leafCell: 77, geomRef: 4, timeBucket: Math.floor(nowMillis / 15000),
      speedBin: 1, qualityBin: 7, meters: 300, ttlSeconds: 90,
      reportId: reportId(random), proofType: 0, proof: new Uint8Array(0)
    }).bytes);
    store.addContribution(record, { nowMillis, deliveredBy: `peer-${i}` });
  }

  const fetchedCells = [];
  const provider = createPulseMeshProvider({
    epochHex,
    store,
    clock: () => nowMillis,
    fetchCells: async cells => { fetchedCells.push(...cells); }
  });
  const states = await provider.fetch({
    epoch: epochHex,
    // A small, entirely unrelated bbox on the other side of the world.
    areas: [{ leaf: 0, bbox: { minLat: 1.0, maxLat: 1.05, minLon: 2.0, maxLon: 2.05 } }],
    maxAgeSeconds: 120
  });
  assert.equal(states.length, 1, "the aggregate is reported even though its cell is nowhere near the areas");
  assert.equal(states[0].segment, "77/2/0");
  assert.ok(fetchedCells.length > 0, "the areas still drove a cell fetch");

  // Epoch mismatch remains an unconditional empty answer.
  assert.deepEqual(await provider.fetch({ epoch: "d".repeat(64), areas: [] }), []);
});

test("reading aggregates never moves the trust ledger", async () => {
  // §8.4 trust moves when a record is judged against a later aggregate —
  // an event tied to routing. A UI polling aggregates() to draw jams must
  // not walk every delivering peer toward a trust bound, because that
  // silently changes the weights the next route is computed from.
  const { createPulseMeshProvider } = await import("../src/pulsemesh/provider.js");
  const { decodePMC1 } = await import("../src/pulsemesh/codec.js");
  const nowMillis = Math.floor(Date.now() / 15000) * 15000;
  const random = lcg(41);
  const store = new PulseMeshStore({ cellOf: () => ({ x: 5, y: 5 }) });
  const trust = new TrustLedger({ clock: () => nowMillis });
  // Two peers agree, one is three bins out — enough for feedback to fire.
  for (const [index, speedBin] of [4, 4, 12].entries()) {
    const record = decodePMC1(encodePMC1({
      epochPrefix8: PREFIX8, leafCell: 3, geomRef: 2, timeBucket: Math.floor(nowMillis / 15000),
      speedBin, qualityBin: 7, meters: 300, ttlSeconds: 90,
      reportId: reportId(random), proofType: 0, proof: new Uint8Array(0)
    }).bytes);
    store.addContribution(record, { nowMillis, deliveredBy: `peer-${index}` });
  }
  const provider = createPulseMeshProvider({
    epochHex: "f".repeat(64), store, trust, clock: () => nowMillis
  });

  const before = ["peer-0", "peer-2"].map(peer => trust.get(peer));
  for (let i = 0; i < 20; i++) provider.aggregates(nowMillis);
  assert.deepEqual(["peer-0", "peer-2"].map(peer => trust.get(peer)), before,
    "twenty reads leave trust exactly where it was");

  // A real fetch — the routing path — still applies feedback once.
  await provider.fetch({ epoch: "f".repeat(64), areas: [], maxAgeSeconds: 120 });
  assert.equal(trust.get("peer-0"), before[0] + 25, "agreeing peer credited on the routing path");
  assert.equal(trust.get("peer-2"), before[1] - 100, "outlying peer penalized on the routing path");
});

test("provider bounds the cell fetch for enormous leaf bboxes", async () => {
  // Route-graph leaves vary hugely in extent; a quarter-degree rural leaf
  // rasterizes to thousands of z15 cells, which would become thousands of
  // snapshot requests for one route query.
  const { createPulseMeshProvider } = await import("../src/pulsemesh/provider.js");
  const store = new PulseMeshStore({ cellOf: () => ({ x: 1, y: 1 }) });
  let requested = 0;
  const provider = createPulseMeshProvider({
    epochHex: "e".repeat(64),
    store,
    clock: () => Date.now(),
    fetchCells: async cells => { requested = cells.length; }
  });
  await provider.fetch({
    epoch: "e".repeat(64),
    areas: [{ leaf: 0, bbox: { minLat: 45.52, maxLat: 45.60, minLon: -76.69, maxLon: -76.46 } }],
    maxAgeSeconds: 120
  });
  assert.ok(requested > 0 && requested <= 64, `oversized leaf raster is capped (got ${requested} cells)`);

  // Per-area capping alone is not enough: a long route touches many
  // leaves, and every wanted cell becomes part of a padded request that
  // fetch() awaits, so the total has to be bounded too.
  let manyAreas = 0;
  const wide = createPulseMeshProvider({
    epochHex: "e".repeat(64),
    store,
    clock: () => Date.now(),
    fetchCells: async cells => { manyAreas = cells.length; }
  });
  await wide.fetch({
    epoch: "e".repeat(64),
    areas: Array.from({ length: 200 }, (_, i) => ({
      leaf: i,
      bbox: { minLat: 45 + i * 0.01, maxLat: 45.05 + i * 0.01, minLon: -76 + i * 0.01, maxLon: -75.95 + i * 0.01 }
    })),
    maxAgeSeconds: 120
  });
  assert.ok(manyAreas > 0 && manyAreas <= 256, `200 areas are capped in total (got ${manyAreas} cells)`);

  // An antimeridian-crossing bbox yields a negative width; it must fall
  // back to the centre neighbourhood rather than silently contributing
  // nothing.
  let wrapped = 0;
  const across = createPulseMeshProvider({
    epochHex: "e".repeat(64),
    store,
    clock: () => Date.now(),
    fetchCells: async cells => { wrapped = cells.length; }
  });
  await across.fetch({
    epoch: "e".repeat(64),
    areas: [{ leaf: 0, bbox: { minLat: 60, maxLat: 61, minLon: 179.9, maxLon: -179.9 } }],
    maxAgeSeconds: 120
  });
  assert.ok(wrapped > 0, "a bbox crossing the antimeridian still yields cells");
});

test("trust ledger: penalties, credits, clamps, decay", () => {
  let now = 0;
  const ledger = new TrustLedger({ clock: () => now });
  assert.equal(ledger.get("p"), 1000);
  ledger.penalizeValidation("p");
  assert.equal(ledger.get("p"), 500);
  ledger.penalizeValidation("p");
  assert.equal(ledger.get("p"), 250, "clamped at TRUST_MIN");
  now = 60 * 60 * 1000; // an hour of decay pulls back toward 1000
  assert.ok(ledger.get("p") > 300);

  const aggregate = { n: 3, speedBin: 6 };
  const entries = [
    { record: { speedBin: 6 }, deliveredBy: "good" },
    { record: { speedBin: 6 }, deliveredBy: "good" },
    { record: { speedBin: 12 }, deliveredBy: "bad" }
  ];
  ledger.applyAggregateFeedback(aggregate, entries);
  assert.equal(ledger.get("good"), 1025, "one credit per aggregate, not per record");
  assert.equal(ledger.get("bad"), 900);
});

test("weights match the spec formula", () => {
  assert.equal(contributionWeight({ qualityBin: 6 }, 5, 1000), 900 * 920 * 1000);
  assert.equal(contributionWeight({ qualityBin: 7 }, 0, 2000), 1000 * 1000 * 2000);
  assert.equal(contributionWeight({ qualityBin: 1 }, 95, 250), 0, "ages past 90 weigh nothing");
});
