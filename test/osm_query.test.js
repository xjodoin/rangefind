import assert from "node:assert/strict";
import test from "node:test";
import {
  parseNearbyCategoryIntent,
  parseOsmQueryIntent,
  searchOsmQuery,
  suggestOsmQuery
} from "../src/integrations/osm/query.js";

test("OSM query intents recognize common categories and natural locality phrasing", () => {
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
  assert.deepEqual(parseOsmQueryIntent("coffee in Montreal"), {
    category: { query: "cafe", label: "Coffee" },
    locality: "Montreal",
    order: "category-locality"
  });
  assert.deepEqual(parseOsmQueryIntent("cafés près de Monaco"), {
    category: { query: "cafe", label: "Cafes" },
    locality: "Monaco",
    order: "category-locality"
  });
  assert.deepEqual(parseOsmQueryIntent("Monaco restaurants"), {
    category: { query: "restaurant", label: "Restaurants" },
    locality: "Monaco",
    order: "locality-category"
  });
  assert.equal(parseOsmQueryIntent("Pharmacie"), null);
  assert.deepEqual(parseOsmQueryIntent("Café Rosemère"), {
    category: { query: "cafe", label: "Cafe" },
    locality: "Rosemère",
    order: "category-locality"
  });
});

test("OSM autocomplete collapses civic matches into street-locality suggestions", async () => {
  const calls = [];
  const engine = {
    async suggest(params) {
      calls.push(params);
      return {
        suggestions: [
          { text: "200 Rue Libersan, Sainte-Thérèse", weight: 2, count: 2, shards: ["quebec"] },
          { text: "202 Rue Libersan, Sainte-Thérèse", weight: 1, count: 1, shards: ["quebec", "ontario"] },
          { text: "202–218 Rue Libersan, Sainte-Thérèse", weight: 1, count: 1 },
          { text: "3024 Rue Libersan, Sainte-Marthe-sur-le-Lac", weight: 1, count: 1 },
          { text: "3028 Rue Libersan, Sainte-Marthe-sur-le-Lac", weight: 1, count: 1 }
        ],
        stats: { suggestLane: "authority-lexicon" }
      };
    }
  };
  const response = await suggestOsmQuery(engine, { q: "Rue Libersan Saint", size: 8 });
  assert.equal(calls[0].size, 32);
  assert.deepEqual(response.suggestions.map(item => item.text), [
    "Rue Libersan, Sainte-Thérèse",
    "Rue Libersan, Sainte-Marthe-sur-le-Lac"
  ]);
  assert.equal(response.suggestions[0].type, "street");
  assert.equal(response.suggestions[0].count, 4);
  assert.deepEqual(response.suggestions[0].shards, ["ontario", "quebec"]);
  assert.equal(response.stats.osmStreetSuggestionsCollapsed, 2);

  await suggestOsmQuery(engine, { q: "214 Rue Libersan Saint", size: 8 });
  assert.equal(calls[1].size, 8);
});

test("OSM category-locality search resolves a place then searches nearby", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Rosemère", category: "place", type: "town", lat: 45.6323155, lon: -73.8052338 }],
          stats: {
            trace: {
              totalMs: 5,
              totalBytes: 100,
              spans: [{ name: "manifest.fetch", count: 1, totalMs: 4, maxMs: 4, bytes: 100 }]
            }
          }
        };
      }
      return {
        total: 2,
        results: [{ name: "Jean Coutu", type: "pharmacy", distanceMeters: 814.7 }],
        stats: {
          geoLane: "nearestText",
          trace: {
            totalMs: 7,
            totalBytes: 200,
            spans: [{ name: "terms.fetch", count: 2, totalMs: 6, maxMs: 3, bytes: 200 }]
          }
        }
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
  assert.equal(response.stats.trace.totalMs, 12);
  assert.equal(response.stats.trace.totalBytes, 300);
  assert.equal(response.stats.trace.spans.length, 2);
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

  await searchOsmQuery(engine, { q: "Laval", shards: ["quebec"], size: 30 });
  assert.deepEqual(calls[1].shards, ["quebec"]);
  const warm = await searchOsmQuery(engine, { q: "Laval", shards: ["quebec"], size: 30, trace: true });
  assert.equal(calls.length, 2, "warm locality should not reopen its shard");
  assert.equal(warm.stats.trace.totalBytes, 0);
  assert.deepEqual(warm.stats.trace.spans, []);
});

test("OSM locality resolution prefers the populous bearer of a common name", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.numbers?.population) {
        return {
          total: 2,
          results: [
            { name: "Laval", category: "place", type: "town", lat: 48.07, lon: -0.77, shard: "pays-de-la-loire" },
            { name: "Laval", category: "place", type: "city", lat: 45.58, lon: -73.75, shard: "quebec" }
          ]
        };
      }
      return {
        total: 260,
        results: [
          { name: "Laval", category: "place", type: "hamlet", lat: 44.1, lon: 1.4, shard: "midi-pyrenees" },
          { name: "Laval-sur-Luzège", category: "place", type: "village", lat: 45.3, lon: 2.1, shard: "limousin" }
        ]
      };
    }
  };
  const response = await searchOsmQuery(engine, { q: "Laval", size: 10 });
  assert.equal(calls.length, 2);
  assert.equal(calls[0].size, 32);
  assert.equal(calls[1].filters.numbers.population.min, 25000);
  assert.deepEqual(calls[1].filters.facets, { category: ["place"] });
  assert.equal(response.total, 1);
  assert.equal(response.results[0].type, "city");
  assert.equal(response.results[0].shard, "quebec");
  assert.equal(response.stats.plannerLane, "osmLocalityExact");

  // A small real place resolves on the first page and never pays the retry.
  const smallCalls = [];
  const smallEngine = {
    async search(params) {
      smallCalls.push(params);
      return {
        total: 1,
        results: [{ name: "Rosemère", category: "place", type: "town", lat: 45.63, lon: -73.8 }]
      };
    }
  };
  await searchOsmQuery(smallEngine, { q: "Rosemère", size: 10 });
  assert.equal(smallCalls.length, 1);
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
  const response = await searchOsmQuery(engine, { q: "Rue Hector, Rosemère", geo: { box: {} }, size: 10 });
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].filters, { facets: { category: ["place"] } });
  assert.equal(calls[0].q, "Rosemère");
  assert.equal(calls[0].size, 32);
  assert.equal(calls[1].q, "Hector");
  // Plain text relevance, no geo machinery: a distance sort decodes every
  // posting inside the radius (common street tokens exhaust the
  // geoTextSortMaxDf budget in dense shards) and a radius filter verifies
  // every candidate through doc-value chunks. The integration checks the
  // returned page against the locality radius itself.
  assert.equal(calls[1].geo, undefined);
  assert.equal(response.total, 1);
  assert.equal(response.results[0].name, "Rue Hector");
  assert.equal(response.results[0].address, "Rue Hector, Rosemère");
  assert.equal(response.results[0].type, "street");
  assert.equal(response.stats.plannerLane, "osmStreetLocality");
});

test("OSM street-locality search returns a matching civic address directly", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Calgary", category: "place", type: "city", population: 1306784, lat: 51.0456, lon: -114.0575, shard: "alberta" }]
        };
      }
      return {
        total: 1,
        results: [
          { id: "way/259692476", name: "Tower Centre", type: "address", house_number: "101", street: "9 Avenue SW", city: "Calgary", lat: 51.0447, lon: -114.0632, distanceMeters: 400 }
        ],
        stats: {}
      };
    }
  };
  const response = await searchOsmQuery(engine, { q: "101 9 avenue sw, calgary", size: 10 });
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[1].shards, ["alberta"]);
  assert.equal(response.total, 1);
  assert.equal(response.results[0].name, "Tower Centre");
  assert.equal(response.results[0].address, "101 9 Avenue SW, Calgary");
  assert.equal(response.stats.plannerLane, "osmStreetLocality");
  assert.equal(response.stats.osmIntentCivicAddress, true);
});

test("OSM street-locality search anchors a street through its civic addresses", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Montréal", category: "place", type: "city", population: 1704694, lat: 45.5032, lon: -73.5698, shard: "quebec" }]
        };
      }
      return {
        total: 30,
        results: [
          { id: "node/9", name: "1000 Rue Saint-Denis", type: "address", street: "Rue Saint-Denis", city: "Montréal", lat: 45.512, lon: -73.561, distanceMeters: 1100 }
        ],
        stats: {}
      };
    }
  };
  const response = await searchOsmQuery(engine, { q: "rue saint denis, montreal", size: 10 });
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[1].shards, ["quebec"]);
  assert.equal(response.total, 1);
  assert.equal(response.results[0].name, "rue saint denis");
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

test("OSM locality resolution scopes to root-authority shards when available", async () => {
  const calls = [];
  const engine = {
    async authorityLookup(surface) {
      return {
        surface,
        prefix: "rosemere",
        matches: [
          { text: "Rosemère", weight: 14294, count: 3, full: true, shards: ["quebec"] },
          { text: "Rue de Rosemère", weight: 12, count: 9, full: false, shards: ["ontario"] }
        ]
      };
    },
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Rosemère", category: "place", type: "town", lat: 45.6323155, lon: -73.8052338 }],
          stats: {}
        };
      }
      return { total: 1, results: [{ name: "Jean Coutu", type: "pharmacy" }], stats: {} };
    }
  };
  await searchOsmQuery(engine, { q: "Pharmacie Rosemère", size: 10 });
  // The locality search runs scoped to the shard the root lexicon named;
  // the street-token match must not widen the scope.
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.ok(calls[0].filters?.facets?.category?.includes("place"));
});

test("OSM locality resolution retries unscoped when the authority scope misses", async () => {
  const calls = [];
  const engine = {
    async authorityLookup(surface) {
      return {
        surface,
        prefix: "rosemere",
        matches: [{ text: "Rosemère", weight: 14294, count: 3, full: true, shards: ["wrong-shard"] }]
      };
    },
    async search(params) {
      calls.push(params);
      if (params.shards) return { total: 0, results: [], stats: {} };
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Rosemère", category: "place", type: "town", lat: 45.6323155, lon: -73.8052338 }],
          stats: {}
        };
      }
      return { total: 1, results: [{ name: "Jean Coutu", type: "pharmacy" }], stats: {} };
    }
  };
  const response = await searchOsmQuery(engine, { q: "Pharmacie Rosemère", size: 10 });
  assert.deepEqual(calls[0].shards, ["wrong-shard"]);
  assert.equal(calls[1].shards, undefined);
  assert.equal(response.stats.plannerLane, "osmCategoryLocality");
});

test("nearby category intents strip near-me phrasing in both languages", () => {
  assert.equal(parseNearbyCategoryIntent("pharmacy near me").query, "pharmacy");
  assert.equal(parseNearbyCategoryIntent("Pharmacies nearby").query, "pharmacy");
  assert.equal(parseNearbyCategoryIntent("cafés autour de moi").query, "cafe");
  assert.equal(parseNearbyCategoryIntent("restaurants").query, "restaurant");
  assert.equal(parseNearbyCategoryIntent("épicerie à proximité").query, "supermarket");
  assert.equal(parseNearbyCategoryIntent("near me"), null);
  assert.equal(parseNearbyCategoryIntent("jean coutu near me"), null);
  assert.equal(parseNearbyCategoryIntent("pharmacy in Birmingham"), null);
});

test("OSM search runs bare categories as nearest-first around the anchor", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      return { total: 3, results: [{ name: "Jean Coutu", type: "pharmacy", distanceMeters: 480 }], stats: {} };
    }
  };
  const response = await searchOsmQuery(engine, { q: "pharmacy near me", size: 10, near: { lat: 45.63, lon: -73.8 } });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].q, "pharmacy");
  assert.equal(calls[0].near, undefined);
  assert.equal(calls[0].geo.sort, "distance");
  assert.equal(calls[0].geo.near.radiusMeters, 10000);
  assert.equal(response.stats.plannerLane, "osmCategoryNearby");
  assert.equal(response.resolvedQuery, "Pharmacy nearby");

  // Rural anchor: an empty 10 km orbit widens once before giving up.
  const sparse = [];
  const ruralEngine = {
    async search(params) {
      sparse.push(params);
      return sparse.length === 1
        ? { total: 0, results: [], stats: {} }
        : { total: 1, results: [{ name: "Pharmacie du Village", type: "pharmacy" }], stats: {} };
    }
  };
  const rural = await searchOsmQuery(ruralEngine, { q: "pharmacie", size: 10, near: { lat: 48.1, lon: -79.0 } });
  assert.deepEqual(sparse.map(params => params.geo.near.radiusMeters), [10000, 50000]);
  assert.equal(rural.stats.osmIntentRadiusMeters, 50000);
});

test("OSM plain text tries the anchor's radius first and falls back globally", async () => {
  const calls = [];
  const localEngine = {
    async search(params) {
      calls.push(params);
      // The locality resolver probes the name as a place first; a zero-hit
      // page ends that cascade without a populous retry.
      if (params.filters) return { total: 0, results: [], stats: {} };
      return { total: 2, results: [{ name: "Jean Coutu", type: "pharmacy" }], stats: {} };
    }
  };
  const local = await searchOsmQuery(localEngine, { q: "jean coutu", size: 10, near: { lat: 45.63, lon: -73.8 } });
  const nearCall = calls.at(-1);
  assert.equal(nearCall.q, "jean coutu");
  assert.equal(nearCall.filters, undefined);
  assert.equal(nearCall.geo.near.radiusMeters, 50000);
  assert.ok(nearCall.geo.boost);
  assert.equal(nearCall.near, undefined);
  assert.equal(local.stats.plannerLane, "osmNearText");

  const fallbackCalls = [];
  const fallbackEngine = {
    async search(params) {
      fallbackCalls.push(params);
      if (params.filters) return { total: 0, results: [], stats: {} };
      return params.geo
        ? { total: 0, results: [], stats: {} }
        : { total: 1, results: [{ name: "Calgary Tower", type: "attraction" }], stats: { plannerLane: "fullFallback" } };
    }
  };
  const fallback = await searchOsmQuery(fallbackEngine, { q: "calgary tower", size: 10, near: { lat: 48.85, lon: 2.35 } });
  const nearAttempt = fallbackCalls.at(-2);
  const globalCall = fallbackCalls.at(-1);
  assert.equal(nearAttempt.geo.near.radiusMeters, 50000);
  assert.equal(globalCall.geo, undefined);
  assert.equal(globalCall.filters, undefined);
  assert.equal(fallback.results[0].name, "Calgary Tower");
  assert.equal(fallback.stats.osmNearFallback, true);
});

test("OSM search ignores the anchor when explicit geo or intents are present", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      return { total: 1, results: [{ name: "Cafe X" }], stats: {} };
    }
  };
  // Explicit geo (the demo's area toggle) outranks the anchor entirely.
  await searchOsmQuery(engine, {
    q: "cafe",
    geo: { box: { minLat: 52.49, maxLat: 52.55, minLon: 13.35, maxLon: 13.46 } },
    near: { lat: 45.63, lon: -73.8 },
    size: 10
  });
  assert.equal(calls.length, 1);
  assert.deepEqual(Object.keys(calls[0].geo), ["box"]);
  assert.equal(calls[0].near, undefined);

  // Category + locality still resolves the named place, not the anchor.
  const localityCalls = [];
  const localityEngine = {
    async search(params) {
      localityCalls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Rosemère", category: "place", type: "town", lat: 45.6323, lon: -73.8052 }],
          stats: {}
        };
      }
      return { total: 1, results: [{ name: "Jean Coutu", type: "pharmacy" }], stats: {} };
    }
  };
  const response = await searchOsmQuery(localityEngine, {
    q: "pharmacy in Rosemère",
    size: 10,
    near: { lat: 51.04, lon: -114.07 }
  });
  assert.equal(response.stats.plannerLane, "osmCategoryLocality");
  const categoryCall = localityCalls.at(-1);
  assert.equal(categoryCall.geo.near.lat, 45.6323);
});
