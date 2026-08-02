import assert from "node:assert/strict";
import test from "node:test";
import {
  parseCoordinateIntent,
  parseNearbyCategoryIntent,
  parseOsmQueryIntent,
  searchOsmQuery,
  suggestOsmQuery
} from "../src/integrations/osm/query.js";
import {
  buildCategoryLexicon,
  buildCategoryLexiconArtifact,
  lookupCategory
} from "../src/integrations/osm/category_lexicon.js";

test("OSM coordinate intents open decimal latitude and longitude without an index fan-out", async () => {
  assert.deepEqual(parseCoordinateIntent("45.5019, -73.5674"), { lat: 45.5019, lon: -73.5674 });
  assert.deepEqual(parseCoordinateIntent("45.5019 -73.5674"), { lat: 45.5019, lon: -73.5674 });
  assert.equal(parseCoordinateIntent("91, -73"), null);
  assert.equal(parseCoordinateIntent("45.5, -181"), null);
  assert.equal(parseCoordinateIntent("845 Sherbrooke"), null);

  const engine = {
    async search() {
      throw new Error("coordinate intent must not query the index");
    }
  };
  const response = await searchOsmQuery(engine, { q: "45.5019, -73.5674", size: 18 });
  assert.equal(response.stats.plannerLane, "osmCoordinates");
  assert.equal(response.stats.shardsQueried, 0);
  assert.equal(response.results[0].type, "coordinate");
  assert.equal(response.results[0].lat, 45.5019);
  assert.equal(response.results[0].lon, -73.5674);
});

test("OSM autocomplete routes through the shard covering the current map", async () => {
  const calls = [];
  const engine = {
    manifest: {
      shards: [
        { id: "ontario", bbox: [41.6, -95.2, 56.9, -74.3] },
        { id: "quebec", bbox: [45, -79.9, 62.7, -57] }
      ]
    },
    async suggest(params) {
      calls.push(params);
      return { suggestions: [{ text: "845 Rue Sherbrooke Ouest, Montréal" }], stats: {} };
    }
  };
  const response = await suggestOsmQuery(engine, {
    q: "845 sher",
    near: { lat: 45.5019, lon: -73.5674 },
    size: 8
  });
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.deepEqual(response.stats.osmSuggestCoverageShards, ["quebec"]);
});

test("OSM autocomplete composes a category with matching locality suggestions", async () => {
  const calls = [];
  const engine = {
    manifest: {
      features: { shards: true },
      category_lexicon: { types: ["cinema"], aliases: {} },
      shards: [{ id: "quebec", bbox: [45, -79.9, 62.7, -57] }]
    },
    async suggest(params) {
      calls.push(params);
      return {
        suggestions: [{ text: "Laval", weight: 400000, shards: ["quebec"] }],
        stats: {}
      };
    }
  };
  const response = await suggestOsmQuery(engine, {
    q: "cinema lav",
    near: { lat: 45.5019, lon: -73.5674 },
    size: 8
  });
  assert.equal(calls[0].q, "lav");
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.equal(response.suggestions[0].text, "Cinema Laval");
  assert.equal(response.suggestions[0].type, "category-locality");
  assert.equal(response.stats.plannerLane, "osmSuggestCategoryLocality");
});

test("OSM intersection intent resolves two exact street surfaces inside one locality shard", async () => {
  const calls = [];
  const engine = {
    manifest: {
      shards: [{ id: "quebec", bbox: [45, -79.9, 62.7, -57] }]
    },
    async authorityLookup(surface) {
      return surface === "Montréal"
        ? { matches: [{ text: "Montréal", weight: 1000, shards: ["quebec"] }] }
        : { matches: [] };
    },
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{
            name: "Montréal",
            type: "city",
            category: "place",
            shard: "quebec",
            lat: 45.5032,
            lon: -73.5698
          }],
          stats: {}
        };
      }
      if (params.q === "Saint-Laurent") {
        return {
          total: 1,
          results: [{
            name: "2027 Boulevard Saint-Laurent",
            street: "Boulevard Saint-Laurent",
            shard: "quebec",
            lat: 45.5116,
            lon: -73.5673
          }],
          stats: {}
        };
      }
      if (params.q === "Sainte-Catherine") {
        return {
          total: 1,
          results: [{
            name: "680 Rue Sainte-Catherine",
            street: "Rue Sainte-Catherine",
            shard: "quebec",
            lat: 45.5028,
            lon: -73.57
          }],
          stats: {}
        };
      }
      return { total: 0, results: [], stats: {} };
    }
  };
  const response = await searchOsmQuery(engine, {
    q: "boulevard Saint-Laurent and rue Sainte-Catherine Montréal",
    size: 18
  });
  assert.equal(response.stats.plannerLane, "osmIntersectionLocality");
  assert.equal(response.results[0].type, "intersection");
  assert.equal(response.results[0].shard, "quebec");
  assert.ok(calls.every(call => call.shards?.[0] === "quebec"));
});

test("OSM anchored named destinations stay inside the map's radius coverage", async () => {
  const calls = [];
  const engine = {
    manifest: {
      features: { shards: true, facetSummaryUint32: true },
      category_lexicon: { types: ["aerodrome"], aliases: {} },
      shards: [
        { id: "ontario", bbox: [41.6, -95.2, 56.9, -74.3] },
        { id: "quebec", bbox: [45, -79.9, 62.7, -57] }
      ]
    },
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return { total: 0, results: [], stats: {} };
      }
      return {
        total: 1,
        results: [{
          name: "Aéroport international Montréal-Trudeau",
          type: "aerodrome",
          shard: "quebec",
          lat: 45.4706,
          lon: -73.7408
        }],
        stats: {}
      };
    }
  };
  const response = await searchOsmQuery(engine, {
    q: "Montréal Trudeau Airport",
    near: { lat: 45.5019, lon: -73.5674 },
    size: 18
  });
  const destinationCall = calls.find(call => call.filters?.facets?.type?.includes("aerodrome"));
  assert.ok(destinationCall);
  assert.deepEqual(destinationCall.shards, ["quebec"]);
  assert.equal(destinationCall.q, "trudeau");
  assert.equal(response.stats.plannerLane, "osmNearFuzzyText");
  assert.deepEqual(response.stats.osmIntentCoverageShards, ["quebec"]);
});

test("OSM query intents recognize common categories and natural locality phrasing", () => {
  assert.deepEqual(parseOsmQueryIntent("Pharmacie Rosemère"), {
    category: { type: "pharmacy", query: "pharmacy", label: "Pharmacie" },
    locality: "Rosemère",
    order: "category-locality",
    connector: false
  });
  assert.deepEqual(parseOsmQueryIntent("Rosemère pharmacy"), {
    category: { type: "pharmacy", query: "pharmacy", label: "Pharmacy" },
    locality: "Rosemère",
    order: "locality-category"
  });
  assert.deepEqual(parseOsmQueryIntent("coffee in Montreal"), {
    category: { type: "cafe", query: "cafe", label: "Coffee" },
    locality: "Montreal",
    order: "category-locality",
    connector: true
  });
  assert.deepEqual(parseOsmQueryIntent("cafés près de Monaco"), {
    category: { type: "cafe", query: "cafe", label: "Cafes" },
    locality: "Monaco",
    order: "category-locality",
    connector: true
  });
  assert.deepEqual(parseOsmQueryIntent("Monaco restaurants"), {
    category: { type: "restaurant", query: "restaurant", label: "Restaurants" },
    locality: "Monaco",
    order: "locality-category"
  });
  assert.equal(parseOsmQueryIntent("Pharmacie"), null);
  assert.deepEqual(parseOsmQueryIntent("Café Rosemère"), {
    category: { type: "cafe", query: "cafe", label: "Cafe" },
    locality: "Rosemère",
    order: "category-locality",
    connector: false
  });
  // The lexicon vocabulary reaches far beyond the old hardcoded list.
  assert.deepEqual(parseOsmQueryIntent("cinema in Birmingham"), {
    category: { type: "cinema", query: "cinema", label: "Cinema" },
    locality: "Birmingham",
    order: "category-locality",
    connector: true
  });
  assert.equal(parseOsmQueryIntent("boulangeries à Québec").category.query, "bakery");
  assert.deepEqual(parseOsmQueryIntent("cinma laval"), {
    category: {
      type: "cinema",
      query: "cinema",
      label: "Cinma",
      correctedFrom: "cinma",
      correctedTo: "cinema"
    },
    locality: "laval",
    order: "category-locality",
    connector: false
  });
  assert.equal(parseOsmQueryIntent("bar laval").category.correctedFrom, undefined);
  assert.equal(parseOsmQueryIntent("xyz laval"), null);
});

test("OSM named category plus locality uses one authority-proven shard", async () => {
  const calls = [];
  const lookups = [];
  const engine = {
    manifest: {
      features: { shards: true },
      shards: [
        { id: "ile-de-france", features: { facetSummaryUint32: true } },
        { id: "ontario", features: { facetSummaryUint32: true } },
        { id: "quebec", features: { facetSummaryUint32: true } }
      ]
    },
    async authorityLookup(surface) {
      lookups.push(surface);
      if (String(surface).toLowerCase() === "repentigny") {
        return { matches: [{ text: "Repentigny", weight: 1000, count: 1, shards: ["quebec"] }] };
      }
      return { matches: [] };
    },
    async search(params) {
      calls.push(params);
      return {
        total: 3,
        results: [
          { name: "Parc Larochelle", type: "park", city: "Repentigny", shard: "quebec" },
          { name: "Parc Larochelle", type: "park", city: "Terrebonne", shard: "quebec" },
          { name: "Parc Israel-Larochelle", type: "park", city: "Farnham", shard: "quebec" }
        ],
        stats: { shardsQueried: 1 }
      };
    }
  };

  const response = await searchOsmQuery(engine, {
    q: "parc larochelle repentigny",
    size: 18
  });

  assert.deepEqual(lookups.sort(), ["larochelle repentigny", "repentigny"]);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].q, "larochelle");
  assert.equal(calls[0].typo, false);
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.deepEqual(calls[0].filters.facets.type, ["park"]);
  assert.equal(calls[0].geo, undefined);
  assert.deepEqual(response.results.map(result => result.city), ["Repentigny"]);
  assert.equal(response.total, 1);
  assert.equal(response.stats.plannerLane, "osmNamedCategoryLocality");
  assert.deepEqual(response.stats.osmIntentCoverageShards, ["quebec"]);
});

test("OSM named category locality tolerates name spacing and keeps proven misses scoped", async () => {
  const searches = [];
  const engine = {
    manifest: {
      features: { shards: true },
      shards: [
        { id: "ontario", features: { facetSummaryUint32: true } },
        { id: "quebec", features: { facetSummaryUint32: true } }
      ]
    },
    async authorityLookup(surface) {
      const normalized = String(surface).toLowerCase();
      if (normalized === "montréal") {
        return { matches: [{ text: "Montréal", count: 1, shards: ["quebec"] }] };
      }
      if (normalized === "toronto") {
        return { matches: [{ text: "Toronto", count: 1, shards: ["ontario"] }] };
      }
      return { matches: [] };
    },
    async search(params) {
      searches.push(params);
      if (params.shards[0] === "quebec") {
        return {
          total: 1,
          results: [{ name: "Parc La Fontaine", type: "park", city: "Montréal", shard: "quebec" }],
          stats: { shardsQueried: 1 }
        };
      }
      return { total: 0, results: [], stats: { shardsQueried: 1 } };
    }
  };

  const spaced = await searchOsmQuery(engine, { q: "parc lafontaine montréal", size: 8 });
  assert.deepEqual(spaced.results.map(result => result.name), ["Parc La Fontaine"]);
  assert.equal(spaced.stats.plannerLane, "osmNamedCategoryLocality");

  const miss = await searchOsmQuery(engine, { q: "parc larochelle toronto", size: 8 });
  assert.deepEqual(miss.results, []);
  assert.equal(miss.total, 0);
  assert.equal(miss.stats.plannerLane, "osmNamedCategoryLocality");
  assert.deepEqual(miss.stats.osmIntentCoverageShards, ["ontario"]);
  assert.equal(searches.length, 2);
});

test("OSM global exact landmarks use their authority shard", async () => {
  const calls = [];
  const engine = {
    async authorityLookup(surface) {
      if (String(surface).toLowerCase() === "calgary tower") {
        return { matches: [{ text: "Calgary Tower", weight: 1, shards: ["alberta"] }] };
      }
      return { matches: [] };
    },
    async search(params) {
      calls.push(params);
      return {
        total: 1,
        results: [{ id: "way/1", name: "Calgary Tower", type: "attraction", city: "Calgary", shard: "alberta" }],
        stats: { shardsQueried: 1 }
      };
    }
  };

  const response = await searchOsmQuery(engine, { q: "Calgary Tower", size: 10 });
  assert.equal(response.stats.plannerLane, "osmGlobalExactText");
  assert.equal(response.results[0].name, "Calgary Tower");
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].shards, ["alberta"]);
  assert.equal(calls[0].typo, false);
});

test("OSM unanchored typo plus locality stays inside the locality authority shard", async () => {
  const calls = [];
  const engine = {
    async authorityLookup(surface) {
      if (String(surface).toLowerCase() === "berlin") {
        return { matches: [{ text: "Berlin", weight: 1, shards: ["berlin"] }] };
      }
      return { matches: [] };
    },
    async search(params) {
      calls.push(params);
      return {
        total: 1,
        results: [{
          id: "node/1",
          name: "Berlin Hauptbahnhof",
          type: "station",
          city: "Berlin",
          shard: "berlin"
        }],
        stats: { shardsQueried: 1 }
      };
    }
  };

  const response = await searchOsmQuery(engine, { q: "hauptbanhof berlin", size: 10 });
  assert.equal(response.stats.plannerLane, "osmNamedTextLocality");
  assert.equal(response.results[0].name, "Berlin Hauptbahnhof");
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].shards, ["berlin"]);
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
  // The connectorless category-first form first probes the whole surface as
  // a locality (miss + populous retry), then resolves "Rosemère" and runs
  // the nearest-sorted category search.
  assert.equal(calls.length, 4);
  assert.equal(calls[3].q, "pharmacy");
  assert.equal(calls[3].geo.sort, "distance");
  assert.equal(calls[3].geo.near.radiusMeters, 10000);
  assert.equal(response.resolvedQuery, "Pharmacie Rosemère");
  assert.equal(response.stats.plannerLane, "osmCategoryLocality");
  assert.equal(response.stats.trace.totalMs, 12);
  assert.equal(response.stats.trace.totalBytes, 300);
  assert.equal(response.stats.trace.spans.length, 2);
  assert.equal(response.results[0].name, "Jean Coutu");
  // The locality-category form pays its own whole-surface probe (miss +
  // populous retry); "Rosemère" itself is already cached.
  await searchOsmQuery(engine, { q: "Rosemère pharmacie", size: 10 });
  assert.equal(calls.length, 7);
  assert.equal(calls[6].q, "pharmacy");
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

test("OSM street-locality search reuses exact root authority without resolving the place twice", async () => {
  const calls = [];
  const engine = {
    async authorityLookup(surface) {
      assert.equal(surface, "Montréal");
      return {
        matches: [{
          text: "Montréal",
          weight: 1704694,
          count: 1,
          shards: ["quebec"]
        }]
      };
    },
    async search(params) {
      calls.push(params);
      assert.equal(params.filters?.facets?.category, undefined);
      return {
        total: 30,
        results: [{
          id: "node/9",
          name: "1000 Rue Saint-Denis",
          type: "address",
          street: "Rue Saint-Denis",
          city: "Montréal",
          lat: 45.512,
          lon: -73.561
        }],
        stats: {}
      };
    }
  };
  const response = await searchOsmQuery(engine, {
    q: "Rue Saint-Denis, Montréal",
    size: 10
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].q, "Denis");
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.equal(response.total, 1);
  assert.equal(response.results[0].name, "Rue Saint-Denis");
  assert.equal(response.stats.plannerLane, "osmStreetLocality");
  assert.equal(response.stats.osmIntentLocalityAuthority, true);
  assert.equal(response.stats.osmIntentRadiusMeters, undefined);
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

test("OSM civic address routing continues past a valid shorter locality suffix", async () => {
  const calls = [];
  const engine = {
    async authorityLookup(surface) {
      const key = String(surface).toLowerCase();
      if (key === "york") {
        return { matches: [{ text: "York", weight: 150000, shards: ["great-britain"] }] };
      }
      if (key === "new york") {
        return { matches: [{ text: "New York", weight: 19000000, shards: ["new-york"] }] };
      }
      return { matches: [] };
    },
    async search(params) {
      calls.push(params);
      if (params.q === "350 5th Avenue" && params.shards?.[0] === "new-york") {
        return {
          total: 1,
          results: [{
            id: "node/350",
            name: "Empire State Building",
            type: "attraction",
            house_number: "350",
            street: "5th Avenue",
            city: "New York",
            lat: 40.7484,
            lon: -73.9857,
            shard: "new-york"
          }],
          stats: { shardsQueried: 1 }
        };
      }
      if (params.filters?.facets?.category?.includes("place")) {
        const york = params.shards?.[0] === "great-britain";
        return {
          total: 1,
          results: [{
            name: york ? "York" : "New York",
            category: "place",
            type: "city",
            lat: york ? 53.96 : 40.71,
            lon: york ? -1.08 : -74,
            shard: york ? "great-britain" : "new-york"
          }],
          stats: { shardsQueried: 1 }
        };
      }
      return { total: 0, results: [], stats: { shardsQueried: 1 } };
    }
  };

  const response = await searchOsmQuery(engine, { q: "350 5th Avenue New York", size: 10 });
  assert.equal(response.stats.plannerLane, "osmStreetLocality");
  assert.equal(response.results[0].city, "New York");
  assert.deepEqual(response.results[0].address, "350 5th Avenue, New York");
  assert.ok(calls.some(call => call.shards?.[0] === "great-britain"));
  assert.ok(calls.some(call => call.shards?.[0] === "new-york"));
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
  // The whole-surface locality probe pays place-filtered misses concurrently
  // with the postal-code resolution, which runs filterless.
  assert.equal(calls.length, 4);
  const postalCall = calls.find(params => params.q === "J7A1V6");
  assert.equal(postalCall.filters, undefined);
  const categoryCall = calls.at(-1);
  assert.equal(categoryCall.q, "pharmacy");
  assert.equal(categoryCall.geo.near.radiusMeters, 5000);
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
  assert.equal(calls.length, 3);
  assert.equal(calls[2].q, "Pharmacie Nowhere");
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
          { text: "Rosemere", weight: 12, count: 1, full: true, shards: ["france"] },
          { text: "Rue de Rosemère", weight: 12, count: 9, full: false, shards: ["ontario"] }
        ]
      };
    },
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{
            name: "Rosemère",
            category: "place",
            type: "town",
            lat: 45.6323155,
            lon: -73.8052338,
            shard: "quebec"
          }],
          stats: {}
        };
      }
      return { total: 1, results: [{ name: "Jean Coutu", type: "pharmacy" }], stats: {} };
    }
  };
  await searchOsmQuery(engine, { q: "Pharmacie Rosemère", size: 10 });
  // The root lexicon proves the whole phrase is not a locality without a
  // global place search. Both the locality resolution and the final category
  // search stay on the shard named by the exact Rosemère authority row.
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.ok(calls[0].filters?.facets?.category?.includes("place"));
  assert.deepEqual(calls[1].shards, ["quebec"]);
  assert.equal(calls[1].q, "pharmacy");
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
  // The authority-scoped locality attempt hits the wrong shard, then the
  // unscoped retry resolves. The whole-surface authority miss performs no
  // global place search.
  const scopedIndex = calls.findIndex(params => params.shards);
  assert.deepEqual(calls[scopedIndex].shards, ["wrong-shard"]);
  assert.ok(calls.slice(scopedIndex + 1).some(params => params.shards === undefined
    && params.filters?.facets?.category?.includes("place")));
  assert.equal(response.stats.plannerLane, "osmCategoryLocality");
});

test("OSM category-locality uses an authority miss and locality provenance to avoid federation fan-out", async () => {
  const calls = [];
  const lookups = [];
  const engine = {
    // Mixed root: Quebec has been rebuilt with unsigned facet summaries,
    // while another shard still requires the fail-open compatibility path.
    manifest: {
      features: { shards: true, facetSummaryUint32: false },
      shards: [
        { id: "quebec", features: { facetSummaryUint32: true } },
        { id: "legacy" }
      ]
    },
    async authorityLookup(surface) {
      lookups.push(surface);
      if (surface === "cinema laval") {
        return { surface, prefix: "cinema laval", matches: [] };
      }
      return {
        surface,
        prefix: "laval",
        matches: [{ text: "Laval", weight: 438366, count: 8, full: true, shards: ["quebec"] }]
      };
    },
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{
            name: "Laval",
            category: "place",
            type: "city",
            population: 438366,
            lat: 45.5833,
            lon: -73.75,
            shard: "quebec"
          }],
          stats: {}
        };
      }
      return {
        total: 1,
        results: [{ name: "Cinéma Cineplex Laval", type: "cinema", distanceMeters: 3200 }],
        stats: { shards: 310, shardsQueried: 1 }
      };
    }
  };

  const response = await searchOsmQuery(engine, { q: "cinema laval", size: 10 });

  assert.deepEqual(lookups.sort(), ["cinema laval", "laval"]);
  assert.equal(calls.length, 2);
  assert.equal(calls.some(call => call.q === "cinema laval"), false);
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.deepEqual(calls[1].shards, ["quebec"]);
  assert.equal(calls[1].q, "cinema");
  assert.deepEqual(calls[1].filters.facets.type, ["cinema"]);
  assert.equal(response.results[0].name, "Cinéma Cineplex Laval");
  assert.equal(response.stats.plannerLane, "osmCategoryLocality");
  assert.equal(response.stats.osmIntentShard, "quebec");
  assert.equal(response.stats.osmIntentCategoryFacet, true);
});

test("OSM whole-place ambiguity probe remains fail-open without a usable authority artifact", async () => {
  const calls = [];
  const engine = {
    async authorityLookup() {
      return null;
    },
    async search(params) {
      calls.push(params);
      if (params.q === "bar harbor" && params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{
            name: "Bar Harbor",
            category: "place",
            type: "town",
            population: 5089,
            lat: 44.39,
            lon: -68.2
          }]
        };
      }
      return { total: 0, results: [], stats: {} };
    }
  };

  const response = await searchOsmQuery(engine, { q: "bar harbor", size: 10 });

  assert.ok(calls.some(call => call.q === "bar harbor"
    && call.filters?.facets?.category?.includes("place")));
  assert.equal(response.stats.plannerLane, "osmLocalityExact");
  assert.equal(response.results[0].name, "Bar Harbor");
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

test("category lexicon artifact joins the corpus vocabulary with the alias table", () => {
  const artifact = buildCategoryLexiconArtifact([
    { value: "cinema", n: 120 },
    { value: "fast_food", n: 3400 },
    { value: "velodrome", n: 1200 },
    // The freeform tail must not gate: rare one-off values, long phrases,
    // and place/address types stay out of the artifact.
    { value: "church_tent", n: 3 },
    { value: "school_mother_touch_community_hall", n: 9999 },
    { value: "city", n: 999999 },
    { value: "address", n: 999999 }
  ]);
  assert.equal(artifact.facet, "type");
  assert.deepEqual(artifact.types, ["cinema", "fast_food", "velodrome"]);
  // Aliases only for types the corpus actually holds.
  assert.equal(artifact.aliases.cinema, "cinema");
  assert.equal(artifact.aliases["movie theater"], "cinema");
  assert.equal(artifact.aliases.boulangerie, undefined);

  const lexicon = buildCategoryLexicon(artifact);
  assert.equal(lookupCategory(lexicon, "cinéma").query, "cinema");
  assert.equal(lookupCategory(lexicon, "fast food").query, "fast food");
  assert.equal(lookupCategory(lexicon, "velodromes").query, "velodrome");
  assert.equal(lookupCategory(lexicon, "bakery"), null);
  assert.equal(lookupCategory(lexicon, "city"), null);
});

test("category lexicon covers the OSM type vocabulary, aliases, and plurals", () => {
  assert.equal(parseNearbyCategoryIntent("cinema").query, "cinema");
  assert.equal(parseNearbyCategoryIntent("cinémas près de moi").query, "cinema");
  assert.equal(parseNearbyCategoryIntent("movie theater near me").query, "cinema");
  assert.equal(parseNearbyCategoryIntent("boulangeries").query, "bakery");
  assert.equal(parseNearbyCategoryIntent("dépanneur").query, "convenience");
  assert.equal(parseNearbyCategoryIntent("fast food nearby").query, "fast food");
  assert.equal(parseNearbyCategoryIntent("hôpitaux").query, "hospital");
  assert.equal(parseNearbyCategoryIntent("churches close by").query, "place of worship");
  assert.equal(parseNearbyCategoryIntent("airports nearby").query, "aerodrome");
  // Locality names must never fold into category intents.
  assert.equal(parseNearbyCategoryIntent("paris"), null);
  assert.equal(parseNearbyCategoryIntent("tours"), null);
  assert.equal(parseNearbyCategoryIntent("nice"), null);
});

test("OSM bare category words no longer teleport to a same-named village", async () => {
  // Before the lexicon, "cinema" was not a known category, passed the
  // locality gate, and resolved to an actual village named Cinema — the
  // demo map then flew across the planet. With an anchor it must be a
  // nearest-first category search; without one, a plain text search. The
  // place-filtered locality probe must never run for a category word.
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Cinema", category: "place", type: "village", lat: -0.34, lon: 31.74 }]
        };
      }
      return { total: 4, results: [{ name: "Cinéma Kirkland", type: "cinema", distanceMeters: 900 }], stats: {} };
    }
  };
  const near = await searchOsmQuery(engine, { q: "cinema", size: 10, near: { lat: 45.63, lon: -73.8 } });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].q, "cinema");
  assert.equal(calls[0].geo.sort, "distance");
  assert.equal(near.stats.plannerLane, "osmCategoryNearby");
  assert.equal(near.results[0].name, "Cinéma Kirkland");

  const global = await searchOsmQuery(engine, { q: "cinema", size: 10 });
  assert.equal(calls.length, 2);
  assert.equal(calls[1].q, "cinema");
  assert.equal(calls[1].filters, undefined);
  assert.notEqual(global.stats.plannerLane, "osmLocalityExact");
});

test("OSM category-first place names still resolve as localities", async () => {
  // "Bar Harbor" opens with a category word ("bar"), but the whole surface
  // is a real town — the connectorless form probes the full name first.
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.q === "bar harbor" && params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Bar Harbor", category: "place", type: "town", population: 5089, lat: 44.39, lon: -68.2 }]
        };
      }
      return { total: 0, results: [], stats: {} };
    }
  };
  const response = await searchOsmQuery(engine, { q: "bar harbor", size: 10 });
  assert.equal(response.stats.plannerLane, "osmLocalityExact");
  assert.equal(response.results[0].name, "Bar Harbor");
  assert.equal(response.results[0].type, "town");
});

test("OSM authority-proven category-first localities never start the split search", async () => {
  const calls = [];
  const engine = {
    async authorityLookup(surface) {
      if (String(surface).toLowerCase() === "park city") {
        return { matches: [{ text: "Park City", weight: 8396, count: 25, shards: ["utah"] }] };
      }
      return { matches: [] };
    },
    async search(params) {
      calls.push(params);
      return {
        total: 1,
        results: [{ name: "Park City", category: "place", type: "town", population: 8396, shard: "utah", lat: 40.65, lon: -111.5 }],
        stats: { shardsQueried: 1 }
      };
    }
  };

  const response = await searchOsmQuery(engine, { q: "Park City", size: 10 });
  assert.equal(response.stats.plannerLane, "osmLocalityExact");
  assert.equal(response.results[0].name, "Park City");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].q, "Park City");
  assert.deepEqual(calls[0].shards, ["utah"]);
});

test("OSM category-last place names still resolve as localities", async () => {
  // "Miami Beach" ends with a corpus category word ("beach"); the whole
  // surface must resolve as the city, never as beaches around Miami.
  const engine = {
    manifest: {
      features: { shards: true },
      category_lexicon: { version: 1, facet: "type", types: ["beach"], aliases: {} }
    },
    async search(params) {
      if (params.q === "miami beach" && params.filters?.facets?.category?.includes("place")) {
        return {
          total: 1,
          results: [{ name: "Miami Beach", category: "place", type: "city", population: 82890, lat: 25.79, lon: -80.13 }]
        };
      }
      return { total: 0, results: [], stats: {} };
    }
  };
  const response = await searchOsmQuery(engine, { q: "miami beach", size: 10 });
  assert.equal(response.stats.plannerLane, "osmLocalityExact");
  assert.equal(response.results[0].name, "Miami Beach");
});

test("OSM category lexicon prefers the manifest artifact, then the facet dictionary", async () => {
  // A sharded root built with the artifact: its own vocabulary gates, even
  // for types the bundled fallback does not carry.
  const embedded = {
    manifest: {
      features: { shards: true },
      category_lexicon: {
        version: 1,
        facet: "type",
        types: ["velodrome", "cinema"],
        aliases: { "velodrome couvert": "velodrome" }
      }
    },
    async search(params) {
      this.calls = [...(this.calls || []), params];
      return { total: 1, results: [{ name: "Vélodrome", type: "velodrome" }], stats: {} };
    }
  };
  const artifact = await searchOsmQuery(embedded, { q: "velodrome couvert near me", size: 10, near: { lat: 45.5, lon: -73.6 } });
  assert.equal(artifact.stats.plannerLane, "osmCategoryNearby");
  assert.equal(embedded.calls[0].q, "velodrome");

  // A single index reads its lazy type facet dictionary — once per engine.
  let dictionaryReads = 0;
  const single = {
    manifest: { features: {} },
    async loadFacetValues(field) {
      dictionaryReads += 1;
      assert.equal(field, "type");
      return [{ value: "windmill", n: 3 }, { value: "cinema", n: 8 }];
    },
    async search(params) {
      this.calls = [...(this.calls || []), params];
      return { total: 1, results: [{ name: "De Gooyer", type: "windmill" }], stats: {} };
    }
  };
  const windmill = await searchOsmQuery(single, { q: "windmill", size: 10, near: { lat: 52.37, lon: 4.93 } });
  assert.equal(windmill.stats.plannerLane, "osmCategoryNearby");
  assert.equal(single.calls[0].q, "windmill");
  await searchOsmQuery(single, { q: "windmill", size: 10, near: { lat: 52.37, lon: 4.93 } });
  assert.equal(dictionaryReads, 1, "the facet dictionary is read once per engine");

  // A sharded root without the artifact must not fan out per shard for a
  // dictionary merge — the bundled vocabulary still covers common words.
  const bare = {
    manifest: { features: { shards: true } },
    async loadFacetValues() {
      throw new Error("sharded dictionary merge must not run for the lexicon");
    },
    async search(params) {
      this.calls = [...(this.calls || []), params];
      return { total: 1, results: [{ name: "Cineplex", type: "cinema" }], stats: {} };
    }
  };
  const fallback = await searchOsmQuery(bare, { q: "cinema", size: 10, near: { lat: 45.5, lon: -73.6 } });
  assert.equal(fallback.stats.plannerLane, "osmCategoryNearby");
});

test("OSM search runs bare categories as nearest-first around the anchor", async () => {
  const calls = [];
  const engine = {
    manifest: { features: { facetSummaryUint32: true } },
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
  assert.deepEqual(calls[0].filters.facets.type, ["pharmacy"]);
  assert.equal(response.stats.plannerLane, "osmCategoryNearby");
  assert.equal(response.stats.osmIntentCategoryFacet, true);
  assert.equal(response.resolvedQuery, "Pharmacy nearby");

  // Rural anchor: an empty 10 km orbit widens once before giving up.
  const sparse = [];
  const ruralEngine = {
    manifest: { features: { facetSummaryUint32: true } },
    async search(params) {
      sparse.push(params);
      return sparse.length === 1
        ? { total: 0, results: [], stats: {} }
        : { total: 1, results: [{ name: "Pharmacie du Village", type: "pharmacy" }], stats: {} };
    }
  };
  const rural = await searchOsmQuery(ruralEngine, { q: "pharmacie", size: 10, near: { lat: 48.1, lon: -79.0 } });
  assert.deepEqual(sparse.map(params => params.geo.near.radiusMeters), [10000, 50000]);
  assert.ok(sparse.every(params => params.filters.facets.type[0] === "pharmacy"));
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
  const nearAttempt = fallbackCalls.find(params => params.geo?.near);
  const globalCall = fallbackCalls.findLast(params => !params.geo && !params.filters);
  assert.equal(nearAttempt.geo.near.radiusMeters, 50000);
  assert.equal(globalCall.geo, undefined);
  assert.equal(globalCall.filters, undefined);
  assert.equal(fallback.results[0].name, "Calgary Tower");
  assert.equal(fallback.stats.osmNearFallback, true);
});

test("OSM anchored exact and one-edit landmark names bypass locality parsing", async () => {
  const calls = [];
  const engine = {
    manifest: {
      features: { shards: true },
      shards: [
        { id: "france", bbox: [41, -5, 51, 10] },
        { id: "quebec", bbox: [44.9, -79.9, 62.7, -57] }
      ]
    },
    async authorityLookup(surface) {
      return surface === "McGill University"
        ? {
            matches: [{
              text: "McGill University",
              weight: 100,
              shards: ["quebec"]
            }]
          }
        : { matches: [] };
    },
    async search(params) {
      calls.push(params);
      return {
        total: 1,
        results: [{
          name: "McGill University",
          type: "university",
          lat: 45.5048,
          lon: -73.5772
        }],
        stats: {}
      };
    }
  };
  const exact = await searchOsmQuery(engine, {
    q: "McGill University",
    size: 10,
    near: { lat: 45.5019, lon: -73.5674 }
  });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].filters, undefined);
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.equal(calls[0].geo, undefined);
  assert.equal(exact.stats.plannerLane, "osmNearExactText");

  const fuzzy = await searchOsmQuery(engine, {
    q: "McGil University",
    size: 10,
    near: { lat: 45.5019, lon: -73.5674 }
  });
  assert.equal(calls.length, 2);
  assert.deepEqual(calls[1].shards, ["quebec"]);
  assert.equal(fuzzy.stats.plannerLane, "osmNearIntentText");
  assert.equal(fuzzy.results[0].name, "McGill University");
});

test("OSM repeated brand names use true nearest order inside the anchor shard", async () => {
  const calls = [];
  const engine = {
    manifest: {
      features: { shards: true },
      shards: [
        { id: "ontario", bbox: [41.6, -95.2, 56.9, -74.3] },
        { id: "quebec", bbox: [44.9, -79.9, 62.7, -57] }
      ]
    },
    async authorityLookup(surface) {
      // The singular surface exists in Ontario, but the map anchor is in
      // Québec where the plural brand has hundreds of text matches.
      return {
        matches: surface === "Tim Hortons"
          ? [{ text: "Tim Hortons", weight: 1790, count: 4504, shards: ["ontario"] }]
          : [{ text: "Tim Horton", weight: 2, count: 4, shards: ["ontario"] }]
      };
    },
    async search(params) {
      calls.push(params);
      if (params.geo?.sort === "distance") {
        return {
          total: 2,
          results: [
            { name: "Tim Hortons", shard: "quebec", lat: 45.608, lon: -73.709, distanceMeters: 313.3 },
            { name: "Tim Hortons", shard: "quebec", lat: 45.61, lon: -73.73, distanceMeters: 1428.2 }
          ],
          stats: {}
        };
      }
      return {
        total: 597,
        results: Array.from({ length: 32 }, (_, index) => ({
          name: "Tim Hortons",
          shard: "quebec",
          lat: 45.7 + index / 1000,
          lon: -73.8
        })),
        stats: {}
      };
    }
  };

  const response = await searchOsmQuery(engine, {
    q: "Tim Horton",
    size: 10,
    near: { lat: 45.6066, lon: -73.7124 }
  });

  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.equal(calls[0].geo.sort, "distance");
  assert.equal(calls[0].geo.near.radiusMeters, 50000);
  assert.equal(response.stats.plannerLane, "osmNearExactGeo");
  assert.equal(response.stats.osmIntentLocalNameProof, true);
  assert.equal(response.results[0].distanceMeters, 313.3);
  assert.ok(response.results.every(result => result.shard === "quebec"));

  calls.length = 0;
  const exact = await searchOsmQuery(engine, {
    q: "Tim Hortons",
    size: 10,
    near: { lat: 45.6066, lon: -73.7124 }
  });
  assert.equal(calls.length, 1, "local whole-name proof should skip root authority");
  assert.equal(calls[0].geo.sort, "distance");
  assert.equal(exact.results[0].distanceMeters, 313.3);
});

test("OSM viewport brand search orders locally and never accepts a foreign authority hint", async () => {
  const calls = [];
  const box = { minLat: 45.55, maxLat: 45.66, minLon: -73.8, maxLon: -73.62 };
  const engine = {
    manifest: {
      features: { shards: true },
      shards: [
        { id: "ontario", bbox: [41.6, -95.2, 56.9, -74.3] },
        { id: "quebec", bbox: [44.9, -79.9, 62.7, -57] }
      ]
    },
    async authorityLookup() {
      return {
        matches: [{ text: "Tim Horton", weight: 2, count: 4, shards: ["ontario"] }]
      };
    },
    async search(params) {
      calls.push(params);
      return {
        total: 1,
        results: [{
          name: "Tim Hortons",
          shard: "quebec",
          lat: 45.608,
          lon: -73.709,
          distanceMeters: 313.3
        }],
        stats: {}
      };
    }
  };

  const response = await searchOsmQuery(engine, {
    q: "Tim Horton",
    size: 10,
    geo: { box }
  });

  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].shards, ["quebec"]);
  assert.deepEqual(calls[0].geo.box, box);
  assert.ok(Math.abs(calls[0].geo.near.lat - 45.605) < 1e-9);
  assert.ok(Math.abs(calls[0].geo.near.lon - -73.71) < 1e-9);
  assert.equal(calls[0].geo.sort, "distance");
  assert.equal(response.stats.plannerLane, "osmViewportExactGeo");
  assert.equal(response.results[0].shard, "quebec");
});

test("OSM search ignores the anchor when explicit geo or intents are present", async () => {
  const calls = [];
  const engine = {
    manifest: { features: { facetSummaryUint32: true } },
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
  assert.equal(calls[0].q, "");
  assert.deepEqual(Object.keys(calls[0].geo), ["box"]);
  assert.equal(calls[0].near, undefined);
  assert.deepEqual(calls[0].filters.facets.type, ["cafe"]);

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

test("OSM category-locality search degrades to a proximity boost when distance sort exceeds the posting budget", async () => {
  // Live shape of "garage brunet": the category-first intent resolves the
  // village of Brunet (Provence), and "car repair" blows through the dense
  // shard's geoTextSortMaxDf budget. The refusal must degrade to relevance +
  // proximity boost in the same radius, never surface as a failed query.
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.numbers?.population) {
        return { total: 0, results: [] };
      }
      if (params.filters?.facets?.category?.includes("place")) {
        if (params.q !== "brunet") return { total: 0, results: [] };
        return {
          total: 1,
          results: [{ name: "Brunet", category: "place", type: "village", lat: 43.8912, lon: 6.0303 }]
        };
      }
      if (params.geo?.sort === "distance") {
        const budgetError = new Error("Rangefind: text distance sort exceeds the geoTextSortMaxDf posting budget; narrow the query or rank by relevance with geo.boost.");
        budgetError.code = "RANGEFIND_GEO_TEXT_SORT_BUDGET";
        throw budgetError;
      }
      return {
        total: 2,
        results: [
          { name: "agricenter - val'agri", type: "car_repair", distanceMeters: 6815 },
          { name: "TAXIL", type: "car_repair", distanceMeters: 6764 }
        ],
        stats: { geoLane: "boostedText" }
      };
    }
  };
  const response = await searchOsmQuery(engine, { q: "garage brunet", size: 10 });
  const fallbackCall = calls.at(-1);
  assert.equal(fallbackCall.q, "car repair");
  assert.equal(fallbackCall.geo.sort, undefined);
  assert.equal(fallbackCall.geo.near.radiusMeters, 7000);
  assert.deepEqual(fallbackCall.geo.boost, { weight: 2, pivotMeters: 2000 });
  assert.equal(response.total, 2);
  // The degraded page is still presented nearest-first.
  assert.deepEqual(response.results.map(result => result.name), ["TAXIL", "agricenter - val'agri"]);
  assert.equal(response.stats.plannerLane, "osmCategoryLocality");
  assert.equal(response.stats.osmDistanceSortFallback, "geo-boost");
  assert.equal(response.resolvedQuery, "Garage Brunet");
});

test("OSM distance-sort fallback rethrows errors other than the posting budget", async () => {
  const engine = {
    async search(params) {
      if (params.filters?.numbers?.population) return { total: 0, results: [] };
      if (params.filters?.facets?.category?.includes("place")) {
        if (params.q !== "brunet") return { total: 0, results: [] };
        return {
          total: 1,
          results: [{ name: "Brunet", category: "place", type: "village", lat: 43.8912, lon: 6.0303 }]
        };
      }
      throw new Error("fetch failed");
    }
  };
  await assert.rejects(
    searchOsmQuery(engine, { q: "garage brunet", size: 10 }),
    /fetch failed/u
  );
});

test("OSM anchored search tries local text before a weak faraway locality interpretation", async () => {
  // "garage brunet" from Rosemère: the only locality named Brunet is a
  // no-population Provence village 6,000km away, while the actual Garage
  // Marcel Brunet sits 2km from the anchor. The anchored text lane gets the
  // first shot; the category interpretation stays the fallback.
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.numbers?.population) return { total: 0, results: [] };
      if (params.filters?.facets?.category?.includes("place")) {
        if (params.q !== "brunet") return { total: 0, results: [] };
        return {
          total: 1,
          results: [{ name: "Brunet", category: "place", type: "village", lat: 43.8912, lon: 6.0303 }]
        };
      }
      return {
        total: 1,
        results: [{ name: "Garage Marcel Brunet Filles et Fils Inc", type: "car_repair", lat: 45.6393, lon: -73.8267, distanceMeters: 2100 }],
        stats: { geoLane: "boostedText" }
      };
    }
  };
  const response = await searchOsmQuery(engine, {
    q: "garage brunet",
    size: 10,
    near: { lat: 45.636, lon: -73.805 }
  });
  const localCall = calls.at(-1);
  assert.equal(localCall.q, "garage brunet");
  assert.equal(localCall.geo.near.radiusMeters, 50000);
  assert.deepEqual(localCall.geo.boost, { weight: 2, pivotMeters: 2000 });
  assert.equal(localCall.near, undefined);
  assert.equal(response.results[0].name, "Garage Marcel Brunet Filles et Fils Inc");
  assert.equal(response.stats.plannerLane, "osmNearText");
  assert.equal(response.stats.osmWeakLocalityTextFirst, true);
});

test("OSM anchored search falls through to the category lane when local text is empty", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.numbers?.population) return { total: 0, results: [] };
      if (params.filters?.facets?.category?.includes("place")) {
        if (params.q !== "brunet") return { total: 0, results: [] };
        return {
          total: 1,
          results: [{ name: "Brunet", category: "place", type: "village", lat: 43.8912, lon: 6.0303 }]
        };
      }
      if (params.geo?.boost && !params.geo?.sort) return { total: 0, results: [] };
      return {
        total: 1,
        results: [{ name: "TAXIL", type: "car_repair", distanceMeters: 6764 }],
        stats: {}
      };
    }
  };
  const response = await searchOsmQuery(engine, {
    q: "garage brunet",
    size: 10,
    near: { lat: 45.636, lon: -73.805 }
  });
  const categoryCall = calls.at(-1);
  assert.equal(categoryCall.q, "car repair");
  assert.equal(categoryCall.geo.sort, "distance");
  assert.equal(response.stats.plannerLane, "osmCategoryLocality");
  assert.equal(response.results[0].name, "TAXIL");
});

test("OSM anchored search keeps the category lane for strong or nearby localities", async () => {
  const calls = [];
  const engine = {
    async search(params) {
      calls.push(params);
      if (params.filters?.facets?.category?.includes("place")) {
        if (params.q !== "laval") return { total: 0, results: [] };
        return {
          total: 1,
          results: [{ name: "Laval", category: "place", type: "city", population: 438366, lat: 45.5833, lon: -73.75 }]
        };
      }
      return {
        total: 1,
        results: [{ name: "Cinéma Cineplex", type: "cinema", distanceMeters: 3200 }],
        stats: {}
      };
    }
  };
  const response = await searchOsmQuery(engine, {
    q: "cinema laval",
    size: 10,
    near: { lat: 45.636, lon: -73.805 }
  });
  // No anchored-text probe: the city interpretation goes straight through.
  assert.equal(calls.filter(call => call.geo?.boost && !call.geo?.sort).length, 0);
  assert.equal(response.stats.plannerLane, "osmCategoryLocality");
});
