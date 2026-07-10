import assert from "node:assert/strict";
import test from "node:test";
import { parseOsmQueryIntent, searchOsmQuery } from "../examples/osm-geo/public/osm-query.js";

test("OSM query intents recognize pharmacy and locality in either order", () => {
  assert.deepEqual(parseOsmQueryIntent("Pharmacie Rosemère"), {
    category: { query: "pharmacy", label: "Pharmacie" },
    locality: "Rosemère",
    order: "category-locality"
  });
  assert.deepEqual(parseOsmQueryIntent("Rosemère pharmacy"), {
    category: { query: "pharmacy", label: "Pharmacy" },
    locality: "Rosemère",
    order: "locality-category"
  });
  assert.equal(parseOsmQueryIntent("Pharmacie"), null);
  assert.equal(parseOsmQueryIntent("Café Rosemère"), null);
});

test("OSM category-locality search resolves a place then searches nearby", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Rosemère", category: "place", type: "town", lat: 45.6323155, lon: -73.8052338 }]
        };
      }
      return {
        total: 2,
        results: [{ name: "Jean Coutu", type: "pharmacy", distanceMeters: 814.7 }],
        stats: { geoLane: "nearestText" }
      };
    }
  };
  const response = await searchOsmQuery(engine, { q: "Pharmacie Rosemère", size: 10 });
  assert.equal(calls.length, 2);
  assert.equal(calls[1].q, "pharmacy");
  assert.equal(calls[1].geo.sort, "distance");
  assert.equal(calls[1].geo.near.radiusMeters, 10000);
  assert.equal(response.resolvedQuery, "Pharmacie Rosemère");
  assert.equal(response.stats.plannerLane, "osmCategoryLocality");
  assert.equal(response.results[0].name, "Jean Coutu");
  await searchOsmQuery(engine, { q: "Rosemère pharmacie", size: 10 });
  assert.equal(calls.length, 3);
  assert.equal(calls[2].q, "pharmacy");
});

test("OSM query intent falls back when the locality cannot be resolved", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      return params.filters
        ? { total: 0, results: [] }
        : { total: 0, results: [], stats: { plannerLane: "fullFallback" } };
    }
  };
  const response = await searchOsmQuery(engine, { q: "Pharmacie Nowhere", size: 10 });
  assert.equal(calls.length, 2);
  assert.equal(calls[1].q, "Pharmacie Nowhere");
  assert.equal(response.stats.plannerLane, "fullFallback");
});
