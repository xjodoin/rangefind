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

test("OSM exact locality search returns the city instead of matching addresses", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      return {
        total: 10,
        results: [
          { name: "Laval", category: "place", type: "city", population: 438366, lat: 45.58, lon: -73.75 },
          { name: "Laval", category: "place", type: "neighbourhood", lat: 46, lon: -72 }
        ]
      };
    }
  };
  const response = await searchOsmQuery(engine, { q: "Laval", geo: { box: {} }, size: 30 });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].filters, { facets: { category: ["place"] } });
  assert.equal(calls[0].geo, undefined);
  assert.equal(response.total, 1);
  assert.equal(response.results[0].type, "city");
  assert.equal(response.stats.plannerLane, "osmLocalityExact");
});

test("OSM street-locality search avoids common road-designator posting exhaustion", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Rosemère", category: "place", type: "town", lat: 45.6323, lon: -73.8052 }]
        };
      }
      if (params.q === "Hector") {
        return {
          total: 30,
          results: [
            { id: "way/1", name: "Rue Hector", lat: 45.637, lon: -73.792, distanceMeters: 310 },
            { id: "node/2", name: "149 Rue Hector, Rosemère", type: "address", lat: 45.635, lon: -73.789 }
          ],
          stats: { plannerLane: "nearestText" }
        };
      }
      return { total: 0, results: [] };
    }
  };
  const response = await searchOsmQuery(engine, { q: "Rue Hector Rosemère", geo: { box: {} }, size: 10 });
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].filters, { facets: { category: ["place"] } });
  assert.equal(calls[0].q, "Rosemère");
  assert.equal(calls[1].q, "Hector");
  assert.equal(calls[1].geo.sort, "distance");
  assert.equal(response.total, 1);
  assert.equal(response.results[0].name, "Rue Hector");
  assert.equal(response.results[0].address, "Rue Hector, Rosemère");
  assert.equal(response.results[0].type, "street");
  assert.equal(response.stats.plannerLane, "osmStreetLocality");
});

test("OSM category search resolves a compact postal area before geo ranking", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.q === "J7A1V6") {
        return {
          total: 1,
          results: [{ name: "J7A 1V6, Rosemère", postcode: "J7A 1V6", type: "postal_code", lat: 45.64, lon: -73.8 }]
        };
      }
      return { total: 1, results: [{ name: "Pharmacy", type: "pharmacy" }], stats: {} };
    }
  };
  const response = await searchOsmQuery(engine, { q: "Pharmacie J7A1V6", size: 10 });
  assert.equal(calls.length, 2);
  assert.equal(calls[0].filters, undefined);
  assert.equal(calls[1].q, "pharmacy");
  assert.equal(calls[1].geo.near.radiusMeters, 5000);
  assert.equal(response.stats.osmIntentLocalityType, "postal_code");
});

test("OSM query display collapses an RQA civic duplicate behind a named place", async () => {
  const engine = {
    async search() {
      return {
        total: 2,
        results: [
          { name: "Jean Coutu", type: "pharmacy", house_number: "10", street: "Boulevard Test", postcode: "J7A 1V6", lat: 45.64, lon: -73.8 },
          { name: "10 Boulevard Test", type: "civic_address", house_number: "10", street: "Boulevard Test", postcode: "J7A 1V6", lat: 45.6401, lon: -73.8001 }
        ],
        stats: { plannerLane: "addressAuthorityExact" }
      };
    }
  };
  const response = await searchOsmQuery(engine, { q: "10 Boulevard Test J7A1V6", size: 10 });
  assert.equal(response.total, 1);
  assert.equal(response.results.length, 1);
  assert.equal(response.results[0].name, "Jean Coutu");
  assert.equal(response.stats.osmCivicDuplicatesCollapsed, 1);
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
