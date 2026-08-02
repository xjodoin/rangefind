import assert from "node:assert/strict";
import test from "node:test";
import {
  evaluateBudgets,
  evaluateExpectations,
  summarizeCases
} from "../scripts/osm_maps_bench_lib.mjs";

test("Maps benchmark expectations catch locality and viewport regressions", () => {
  const response = {
    results: [
      { name: "Tim Hortons", shard: "quebec", lat: 45.608, lon: -73.709, distanceMeters: 313.3 },
      { name: "Tim Hortons", shard: "quebec", lat: 45.61, lon: -73.73, distanceMeters: 1428.2 }
    ],
    stats: { plannerLane: "osmViewportExactGeo", shardsQueried: 1 }
  };
  const good = evaluateExpectations(response, {
    minResults: 2,
    topTextAny: ["Tim Hortons"],
    allTopShards: ["quebec"],
    firstDistanceMax: 1000,
    distanceAscending: true,
    viewportBox: { minLat: 45.55, maxLat: 45.66, minLon: -73.8, maxLon: -73.62 },
    lanes: ["osmViewportExactGeo"],
    maxShardsQueried: 1
  });
  assert.equal(good.passed, true);

  const namedLocality = evaluateExpectations({
    results: [{ name: "Parc Larochelle", type: "park", city: "Repentigny", shard: "quebec" }],
    stats: { plannerLane: "osmNamedCategoryLocality", shardsQueried: 1 }
  }, {
    topTextAny: ["Parc Larochelle"],
    topTypes: ["park"],
    topLocalityAny: ["Repentigny"],
    lanes: ["osmNamedCategoryLocality"],
    maxShardsQueried: 1
  });
  assert.equal(namedLocality.passed, true);

  const provenMiss = evaluateExpectations({
    results: [],
    stats: { plannerLane: "osmNamedCategoryLocality", shardsQueried: 1 }
  }, {
    maxResults: 0,
    lanes: ["osmNamedCategoryLocality"],
    maxShardsQueried: 1
  });
  assert.equal(provenMiss.passed, true);

  const apiShape = evaluateExpectations({
    results: [{
      id: "node/1",
      name: "Example",
      address: "1 Main Street",
      postcode: "H2X 1Y4",
      lat: 45.5,
      lon: -73.5,
      distanceMeters: 120
    }]
  }, {
    topHasCoordinates: true,
    topHasAddress: true,
    topHasId: true,
    topPostcodeAny: ["H2X 1Y4"],
    allDistancesMax: 500,
    uniqueIds: true
  });
  assert.equal(apiShape.passed, true);

  const foreign = evaluateExpectations({
    ...response,
    results: [{ ...response.results[0], shard: "ontario", lat: 43.65, lon: -79.38 }]
  }, {
    allTopShards: ["quebec"],
    viewportBox: { minLat: 45.55, maxLat: 45.66, minLon: -73.8, maxLon: -73.62 }
  });
  assert.equal(foreign.passed, false);
  assert.deepEqual(
    foreign.checks.filter(check => !check.pass).map(check => check.name),
    ["allTopShards", "viewportBox"]
  );
});

test("Maps benchmark summary is weighted and separates quality from budgets", () => {
  const fast = { ms: 100, requests: 2, bytes: 1000 };
  const slow = { ms: 1000, requests: 20, bytes: 10000 };
  const budget = { coldMs: 500, coldRequests: 10, warmMs: 50, warmRequests: 0 };
  const cases = [
    {
      id: "common",
      family: "poi",
      weight: 9,
      cold: fast,
      warm: { ...fast, ms: 10, requests: 0 },
      quality: { passed: true, checks: [] },
      budget: evaluateBudgets(fast, { ...fast, ms: 10, requests: 0 }, budget)
    },
    {
      id: "edge",
      family: "poi",
      weight: 1,
      cold: slow,
      warm: { ...slow, ms: 100, requests: 1 },
      quality: { passed: false, checks: [{ name: "topShard", pass: false, actual: "ontario" }] },
      budget: evaluateBudgets(slow, { ...slow, ms: 100, requests: 1 }, budget)
    }
  ];
  const summary = summarizeCases(cases);
  assert.equal(summary.overall.qualityPassRate, 0.9);
  assert.equal(summary.overall.budgetPassRate, 0.9);
  assert.equal(summary.overall.cold.weightedMeanMs, 190);
  assert.equal(summary.failures.length, 1);
  assert.equal(summary.failures[0].id, "edge");
});
