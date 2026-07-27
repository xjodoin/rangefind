import assert from "node:assert/strict";
import test from "node:test";
import {
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
  const nearAttempt = fallbackCalls.at(-2);
  const globalCall = fallbackCalls.at(-1);
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
  assert.equal(fuzzy.stats.plannerLane, "osmNearFuzzyText");
  assert.equal(fuzzy.results[0].name, "McGill University");
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
