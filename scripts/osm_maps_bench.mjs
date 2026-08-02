#!/usr/bin/env node

// Production benchmark for common interactive map-search use cases. Each
// case runs in its own Node process so a pathological cold query cannot
// retain caches or heap state that contaminates the next measurement.
//
// Usage:
//   node scripts/osm_maps_bench.mjs
//   node scripts/osm_maps_bench.mjs --profile=full
//   node scripts/osm_maps_bench.mjs --cases=category-near,landmark
//   node scripts/osm_maps_bench.mjs --list
//   node scripts/osm_maps_bench.mjs --out=/tmp/maps-bench.json

import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { createSearch } from "../src/runtime.js";
import { reverseGeocodeOsm, searchOsmQuery, suggestOsmQuery } from "../src/integrations/osm/query.js";
import { evaluateBudgets, evaluateExpectations, summarizeCases } from "./osm_maps_bench_lib.mjs";

const MONTREAL = { lat: 45.5019, lon: -73.5674 };
const LAVAL = { lat: 45.6066, lon: -73.7124 };
const CALGARY = { lat: 51.0447, lon: -114.0719 };
const BERLIN = { lat: 52.52, lon: 13.405 };
const TOKYO = { lat: 35.6812, lon: 139.7671 };
const MONTREAL_BOX = {
  minLat: 45.45,
  maxLat: 45.60,
  minLon: -73.70,
  maxLon: -73.50
};
const LAVAL_BOX = {
  minLat: 45.55,
  maxLat: 45.66,
  minLon: -73.80,
  maxLon: -73.62
};
const OLD_MONTREAL_BOX = {
  minLat: 45.495,
  maxLat: 45.515,
  minLon: -73.565,
  maxLon: -73.535
};
const BERLIN_BOX = {
  minLat: 52.49,
  maxLat: 52.55,
  minLon: 13.35,
  maxLon: 13.46
};

const MIB = 1024 * 1024;
const BUDGETS = {
  autocomplete: { coldMs: 2500, coldRequests: 30, coldBytes: 2 * MIB, warmMs: 100, warmRequests: 0 },
  addressAutocomplete: { coldMs: 2500, coldRequests: 35, coldBytes: 2 * MIB, warmMs: 100, warmRequests: 0 },
  direct: { coldMs: 4000, coldRequests: 60, coldBytes: 4 * MIB, warmMs: 100, warmRequests: 0 },
  discovery: { coldMs: 6000, coldRequests: 100, coldBytes: 5 * MIB, warmMs: 100, warmRequests: 0 },
  recovery: { coldMs: 7000, coldRequests: 130, coldBytes: 6 * MIB, warmMs: 150, warmRequests: 0 },
  journey: { coldMs: 5000, coldRequests: 75, coldBytes: 5 * MIB, warmMs: 150, warmRequests: 0 },
  reverseGeocode: { coldMs: 4000, coldRequests: 65, coldBytes: 4 * MIB, warmMs: 100, warmRequests: 0 }
};

const CASES = [
  {
    id: "suggest-locality",
    family: "autocomplete",
    scenario: "Type a city name",
    weight: 9,
    budget: BUDGETS.autocomplete,
    expect: { minResults: 1, anyTextAny: ["Montréal", "Montreal"] },
    run: engine => suggestOsmQuery(engine, { q: "mont", near: MONTREAL, size: 8 })
  },
  {
    id: "suggest-poi-locality",
    family: "autocomplete",
    scenario: "Type a category and locality",
    weight: 9,
    budget: BUDGETS.autocomplete,
    expect: { minResults: 1, anyTextAny: ["cinema", "cinéma", "Laval"] },
    run: engine => suggestOsmQuery(engine, { q: "cinema lav", near: MONTREAL, size: 8 })
  },
  {
    id: "suggest-address",
    family: "autocomplete",
    scenario: "Type a partial civic address",
    weight: 8,
    budget: BUDGETS.addressAutocomplete,
    expect: { minResults: 1, anyTextAny: ["Sherbrooke"] },
    run: engine => suggestOsmQuery(engine, { q: "845 sher", near: MONTREAL, size: 8 })
  },
  {
    id: "suggest-brand",
    family: "autocomplete",
    scenario: "Type a chain or brand",
    weight: 7,
    common: false,
    budget: BUDGETS.autocomplete,
    expect: { minResults: 1, anyTextAny: ["Tim Horton"] },
    run: engine => suggestOsmQuery(engine, { q: "tim hor", near: LAVAL, size: 8 })
  },
  {
    id: "locality-exact",
    family: "locality",
    scenario: "Open an exact city",
    weight: 8,
    budget: BUDGETS.direct,
    expect: { minResults: 1, topTextAny: ["Laval"], topShard: "quebec", maxShardsQueried: 1 },
    run: engine => searchOsmQuery(engine, { q: "Laval", size: 18 })
  },
  {
    id: "edge-category-word-locality-park-city",
    family: "locality",
    scenario: "Do not parse a category-looking city name as a category query",
    weight: 2,
    common: false,
    edge: true,
    budget: BUDGETS.recovery,
    expect: {
      minResults: 1,
      topTextAny: ["Park City"],
      topTypes: ["town"],
      topShard: "utah",
      lanes: ["osmLocalityExact"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "Park City", size: 18 })
  },
  {
    id: "edge-category-word-locality-bar-harbor",
    family: "locality",
    scenario: "Keep a category-looking multi-word locality intact",
    weight: 2,
    common: false,
    edge: true,
    budget: BUDGETS.direct,
    expect: {
      minResults: 1,
      topTextAny: ["Bar Harbor"],
      topTypes: ["town"],
      topShard: "maine",
      lanes: ["osmLocalityExact"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "Bar Harbor", size: 18 })
  },
  {
    id: "landmark",
    family: "poi",
    scenario: "Find a named landmark near the map",
    weight: 8,
    budget: BUDGETS.direct,
    expect: { minResults: 1, topTextAny: ["McGill University"], topShard: "quebec", firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "McGill University", near: MONTREAL, size: 18 })
  },
  {
    id: "named-category-locality",
    family: "poi",
    scenario: "Find a named venue by category and locality",
    weight: 8,
    budget: BUDGETS.direct,
    expect: {
      minResults: 1,
      topTextAny: ["Parc Larochelle"],
      topTypes: ["park"],
      topLocalityAny: ["Repentigny"],
      topShard: "quebec",
      lanes: ["osmNamedCategoryLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "parc larochelle repentigny", size: 18 })
  },
  {
    id: "edge-named-category-spacing",
    family: "poi",
    scenario: "Match a joined query token to a spaced venue name",
    weight: 2,
    common: false,
    edge: true,
    budget: BUDGETS.direct,
    expect: {
      minResults: 1,
      topTextAny: ["Parc La Fontaine"],
      topTypes: ["park"],
      topLocalityAny: ["Montréal", "Montreal"],
      topShard: "quebec",
      lanes: ["osmNamedCategoryLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "parc lafontaine montréal", size: 18 })
  },
  {
    id: "edge-named-category-same-name",
    family: "poi",
    scenario: "Disambiguate the same venue name by structured locality",
    weight: 2,
    common: false,
    edge: true,
    budget: BUDGETS.direct,
    expect: {
      minResults: 1,
      topTextAny: ["Parc Larochelle"],
      topTypes: ["park"],
      topLocalityAny: ["Terrebonne"],
      topShard: "quebec",
      lanes: ["osmNamedCategoryLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "parc larochelle terrebonne", size: 18 })
  },
  {
    id: "edge-named-category-proven-miss",
    family: "poi",
    scenario: "Keep an authority-proven venue miss from reopening global search",
    weight: 2,
    common: false,
    edge: true,
    budget: BUDGETS.direct,
    expect: {
      maxResults: 0,
      lanes: ["osmNamedCategoryLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "parc larochelle toronto", size: 18 })
  },
  {
    id: "airport",
    family: "poi",
    scenario: "Find an airport by name",
    weight: 5,
    budget: BUDGETS.direct,
    expect: { minResults: 1, anyTextAny: ["Trudeau", "Dorval", "YUL"], allTopShards: ["quebec"], checkTop: 5 },
    run: engine => searchOsmQuery(engine, { q: "Montréal Trudeau Airport", near: MONTREAL, size: 18 })
  },
  {
    id: "transit-station",
    family: "poi",
    scenario: "Find a transit station",
    weight: 6,
    common: false,
    budget: BUDGETS.direct,
    expect: { minResults: 1, topTextAny: ["Berri-UQAM", "Berri UQAM"], topShard: "quebec", firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "Berri-UQAM", near: MONTREAL, size: 18 })
  },
  {
    id: "category-locality",
    family: "category",
    scenario: "Find a place type in a city",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 3,
      topTypes: ["cinema"],
      allTopShards: ["quebec"],
      distanceAscending: true,
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "cinema laval", size: 18 })
  },
  {
    id: "category-locality-common",
    family: "category",
    scenario: "Find a dense place type in a city",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: { minResults: 10, allTopShards: ["quebec"], distanceAscending: true, maxShardsQueried: 1 },
    run: engine => searchOsmQuery(engine, { q: "restaurant montreal", size: 18 })
  },
  {
    id: "category-locality-french",
    family: "category",
    scenario: "Use the local language for category search",
    weight: 7,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, topTypes: ["cinema"], allTopShards: ["quebec"], distanceAscending: true },
    run: engine => searchOsmQuery(engine, { q: "cinéma laval", size: 18 })
  },
  {
    id: "edge-category-multiword-locality",
    family: "category",
    scenario: "Preserve a genuine multi-word locality after a category",
    weight: 2,
    common: false,
    edge: true,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 3,
      topTypes: ["cinema"],
      topLocalityAny: ["New York", "Manhattan"],
      allTopShards: ["new-york"],
      lanes: ["osmCategoryLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "cinema new york", size: 18 })
  },
  {
    id: "edge-category-hyphenated-locality",
    family: "category",
    scenario: "Resolve an unhyphenated query to a hyphenated locality",
    weight: 2,
    common: false,
    edge: true,
    // The four-word authority surface legitimately reads one extra term pack;
    // retain the discovery latency/request limits with a narrowly larger byte cap.
    budget: { ...BUDGETS.discovery, coldBytes: 5.5 * MIB },
    expect: {
      minResults: 3,
      topTypes: ["restaurant"],
      topLocalityAny: ["Saint-Jean-sur-Richelieu"],
      allTopShards: ["quebec"],
      lanes: ["osmCategoryLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "restaurant saint jean sur richelieu", size: 18 })
  },
  {
    id: "category-near-sparse",
    family: "near-me",
    scenario: "Find a sparse utility near me",
    weight: 8,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, allTopShards: ["quebec"], distanceAscending: true, firstDistanceMax: 10000 },
    run: engine => searchOsmQuery(engine, { q: "gas station near me", near: MONTREAL, size: 18 })
  },
  {
    id: "category-near-dense",
    family: "near-me",
    scenario: "Find a dense category around the current location",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: { minResults: 10, allTopShards: ["quebec"], distanceAscending: true, firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "restaurant", near: MONTREAL, size: 18 })
  },
  {
    id: "pharmacy-near",
    family: "near-me",
    scenario: "Find an essential service near me",
    weight: 6,
    common: false,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, allTopShards: ["quebec"], distanceAscending: true, firstDistanceMax: 10000 },
    run: engine => searchOsmQuery(engine, { q: "pharmacy", near: MONTREAL, size: 18 })
  },
  {
    id: "parking-near",
    family: "near-me",
    scenario: "Find parking near the destination",
    weight: 6,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, allTopShards: ["quebec"], distanceAscending: true, firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "parking", near: MONTREAL, size: 18 })
  },
  {
    id: "brand-near",
    family: "poi",
    scenario: "Find the nearest branch of a chain",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 5,
      topTextAny: ["Tim Hortons"],
      allTopShards: ["quebec"],
      firstDistanceMax: 1000,
      distanceAscending: true,
      lanes: ["osmNearExactGeo"]
    },
    run: engine => searchOsmQuery(engine, { q: "Tim Hortons", near: LAVAL, size: 18 })
  },
  {
    id: "brand-near-variant",
    family: "poi",
    scenario: "Resolve a singular or incomplete brand variant",
    weight: 5,
    common: false,
    budget: BUDGETS.recovery,
    expect: {
      minResults: 5,
      topTextAny: ["Tim Hortons"],
      allTopShards: ["quebec"],
      firstDistanceMax: 1000,
      distanceAscending: true,
      lanes: ["osmNearExactGeo"]
    },
    run: engine => searchOsmQuery(engine, { q: "Tim Horton", near: LAVAL, size: 18 })
  },
  {
    id: "civic-address",
    family: "address",
    scenario: "Find a complete civic address",
    weight: 10,
    budget: BUDGETS.direct,
    expect: { minResults: 1, anyTextAny: ["845", "Sherbrooke"], allTopShards: ["quebec"], checkTop: 5 },
    run: engine => searchOsmQuery(engine, { q: "845 rue Sherbrooke Ouest Montréal", size: 18 })
  },
  {
    id: "street-locality",
    family: "address",
    scenario: "Find a street within a locality",
    weight: 7,
    budget: BUDGETS.direct,
    expect: { minResults: 1, topTextAny: ["Saint-Denis", "Saint Denis"], topShard: "quebec" },
    run: engine => searchOsmQuery(engine, { q: "rue saint denis montreal", size: 18 })
  },
  {
    id: "postal-code",
    family: "address",
    scenario: "Find an area by postal code",
    weight: 5,
    common: false,
    budget: BUDGETS.direct,
    expect: { minResults: 1, anyTextAny: ["H2X 1Y4", "H2X"], allTopShards: ["quebec"], checkTop: 5 },
    run: engine => searchOsmQuery(engine, { q: "H2X 1Y4", size: 18 })
  },
  {
    id: "intersection",
    family: "address",
    scenario: "Find an intersection",
    weight: 6,
    budget: BUDGETS.recovery,
    expect: { minResults: 1, anyTextAny: ["Saint-Laurent", "Sainte-Catherine"], allTopShards: ["quebec"], checkTop: 8 },
    run: engine => searchOsmQuery(engine, {
      q: "boulevard Saint-Laurent and rue Sainte-Catherine Montréal",
      size: 18
    })
  },
  {
    id: "viewport-browse",
    family: "viewport",
    scenario: "Browse visible places after moving the map",
    weight: 4,
    common: false,
    budget: BUDGETS.discovery,
    expect: { minResults: 10, viewportBox: MONTREAL_BOX, maxShardsQueried: 1 },
    run: engine => engine.search({ q: "", geo: { box: MONTREAL_BOX }, size: 18 })
  },
  {
    id: "viewport-category",
    family: "viewport",
    scenario: "Search this area for a category",
    weight: 10,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 10,
      viewportBox: MONTREAL_BOX,
      allTopShards: ["quebec"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, {
      q: "restaurant",
      geo: { box: MONTREAL_BOX },
      size: 18
    })
  },
  {
    id: "viewport-brand",
    family: "viewport",
    scenario: "Search this area for a chain",
    weight: 9,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 5,
      viewportBox: LAVAL_BOX,
      topTextAny: ["Tim Hortons"],
      allTopShards: ["quebec"],
      distanceAscending: true,
      lanes: ["osmViewportExactGeo"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, {
      q: "Tim Horton",
      geo: { box: LAVAL_BOX },
      size: 18
    })
  },
  {
    id: "viewport-pan-category",
    family: "viewport",
    scenario: "Repeat search after panning into a smaller area",
    weight: 7,
    budget: BUDGETS.discovery,
    expect: { minResults: 3, viewportBox: OLD_MONTREAL_BOX, allTopShards: ["quebec"], maxShardsQueried: 1 },
    run: engine => searchOsmQuery(engine, {
      q: "cafe",
      geo: { box: OLD_MONTREAL_BOX },
      size: 18
    })
  },
  {
    id: "typo-landmark",
    family: "typo",
    scenario: "Recover a typo in a named place",
    weight: 5,
    budget: BUDGETS.recovery,
    expect: { minResults: 1, topTextAny: ["McGill University"], topShard: "quebec", firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "McGil University", near: MONTREAL, size: 18 })
  },
  {
    id: "typo-category-locality",
    family: "typo",
    scenario: "Recover a typo in category plus locality",
    weight: 4,
    common: false,
    budget: BUDGETS.recovery,
    expect: { minResults: 3, allTopShards: ["quebec"], distanceAscending: true },
    run: engine => searchOsmQuery(engine, { q: "cinma laval", size: 18 })
  },
  {
    id: "unicode-landmark",
    family: "international",
    scenario: "Find a landmark in its native script",
    weight: 5,
    common: false,
    budget: BUDGETS.discovery,
    expect: { minResults: 1, topTextAny: ["東京駅"], topShard: "japan", firstDistanceMax: 5000 },
    run: engine => searchOsmQuery(engine, { q: "東京駅", near: TOKYO, size: 18 })
  },
  {
    id: "production-suggest-select",
    family: "journey",
    scenario: "Autocomplete a locality and search the selected prediction",
    weight: 5,
    common: false,
    production: true,
    budget: { ...BUDGETS.journey, coldRequests: 90 },
    expect: {
      minResults: 1,
      topTextAny: ["Berlin"],
      topShard: "berlin",
      topHasCoordinates: true,
      topHasId: true,
      lanes: ["osmLocalityExact"],
      maxShardsQueried: 2
    },
    run: async engine => {
      const suggested = await suggestOsmQuery(engine, { q: "berl", size: 8 });
      const item = suggested.suggestions.find(candidate => candidate.text === "Berlin")
        || suggested.suggestions[0];
      return searchOsmQuery(engine, {
        q: item?.text || "Berlin",
        size: 18,
        ...(item?.shards?.length ? { shards: item.shards } : {})
      });
    }
  },
  {
    id: "production-suggest-native-script",
    family: "autocomplete",
    scenario: "Autocomplete a place using native-script input",
    weight: 3,
    common: false,
    production: true,
    budget: BUDGETS.autocomplete,
    expect: { minResults: 1, anyTextAny: ["東京"] },
    run: engine => suggestOsmQuery(engine, { q: "東京", near: TOKYO, size: 8 })
  },
  {
    id: "production-global-landmark",
    family: "poi",
    scenario: "Find a globally unique landmark without map context",
    weight: 4,
    common: false,
    production: true,
    budget: BUDGETS.direct,
    expect: {
      minResults: 1,
      topTextAny: ["Calgary Tower"],
      topShard: "alberta",
      topHasCoordinates: true,
      topHasId: true,
      lanes: ["osmGlobalExactText"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "Calgary Tower", size: 18 })
  },
  {
    id: "production-city-landmark",
    family: "poi",
    scenario: "Find a named transit landmark with its city in the query",
    weight: 4,
    common: false,
    production: true,
    budget: { ...BUDGETS.direct, coldBytes: 6.25 * MIB },
    expect: {
      minResults: 1,
      topTextAny: ["Berlin Hauptbahnhof", "Berlin Central Station", "Hauptbahnhof"],
      topShard: "berlin",
      topHasCoordinates: true,
      topHasId: true,
      lanes: ["osmGlobalExactText", "osmNamedTextLocality"],
      maxShardsQueried: 2
    },
    run: engine => searchOsmQuery(engine, { q: "Berlin Hauptbahnhof", size: 18 })
  },
  {
    id: "production-category-connector-locality",
    family: "category",
    scenario: "Resolve an explicit category-in-locality request",
    weight: 4,
    common: false,
    production: true,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 3,
      topTypes: ["pharmacy"],
      topLocalityAny: ["Birmingham"],
      allTopShards: ["great-britain"],
      distanceAscending: true,
      lanes: ["osmCategoryLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "pharmacy in Birmingham", size: 18 })
  },
  {
    id: "production-interpolated-address",
    family: "address",
    scenario: "Forward-geocode a house number through street interpolation",
    weight: 5,
    common: false,
    production: true,
    budget: BUDGETS.direct,
    expect: {
      minResults: 1,
      topTextAny: ["214 Rue Libersan"],
      topTypes: ["interpolated_address"],
      topLocalityAny: ["Sainte-Thérèse", "Sainte-Therese"],
      topHasCoordinates: true,
      topHasAddress: true,
      topHasId: true,
      topShard: "quebec",
      lanes: ["osmStreetLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "214 rue Libersan Sainte-Thérèse", size: 18 })
  },
  {
    id: "production-calgary-address",
    family: "address",
    scenario: "Forward-geocode a directional North American civic address",
    weight: 4,
    common: false,
    production: true,
    budget: BUDGETS.direct,
    expect: {
      minResults: 1,
      topLocalityAny: ["Calgary"],
      topHasCoordinates: true,
      topHasAddress: true,
      topHasId: true,
      topShard: "alberta",
      lanes: ["osmStreetLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "101 9 avenue sw, calgary", size: 18 })
  },
  {
    id: "production-multiword-locality-address",
    family: "address",
    scenario: "Forward-geocode an address whose locality has a valid suffix city",
    weight: 4,
    common: false,
    production: true,
    budget: BUDGETS.recovery,
    expect: {
      minResults: 1,
      topLocalityAny: ["New York"],
      topHasCoordinates: true,
      topHasAddress: true,
      topHasId: true,
      topPostcodeAny: ["10118"],
      topShard: "new-york",
      lanes: ["osmStreetLocality"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "350 5th Avenue New York", size: 18 })
  },
  {
    id: "production-nearest",
    family: "nearby",
    scenario: "Return the nearest places around a device location",
    weight: 4,
    common: false,
    production: true,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 10,
      allTopShards: ["alberta"],
      distanceAscending: true,
      firstDistanceMax: 1000,
      topHasCoordinates: true,
      topHasId: true,
      uniqueIds: true,
      maxShardsQueried: 4
    },
    run: engine => engine.search({ q: "", geo: { near: CALGARY, sort: "distance" }, size: 18 })
  },
  {
    id: "production-nearest-radius",
    family: "nearby",
    scenario: "Apply a hard radius restriction to nearby discovery",
    weight: 4,
    common: false,
    production: true,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 10,
      allTopShards: ["alberta"],
      distanceAscending: true,
      allDistancesMax: 2000,
      topHasCoordinates: true,
      uniqueIds: true,
      maxShardsQueried: 3
    },
    run: engine => engine.search({
      q: "",
      geo: { near: { ...CALGARY, radiusMeters: 2000 }, sort: "distance" },
      size: 18
    })
  },
  {
    id: "production-international-viewport",
    family: "viewport",
    scenario: "Search a category inside a non-Canadian map viewport",
    weight: 4,
    common: false,
    production: true,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 10,
      viewportBox: BERLIN_BOX,
      allTopShards: ["berlin", "brandenburg"],
      topHasCoordinates: true,
      uniqueIds: true,
      maxShardsQueried: 2
    },
    run: engine => searchOsmQuery(engine, { q: "cafe", geo: { box: BERLIN_BOX }, size: 18 })
  },
  {
    id: "production-discovery-orbit",
    family: "nearby",
    scenario: "Discover related places around a selected result",
    weight: 3,
    common: false,
    production: true,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 10,
      allTopShards: ["berlin"],
      distanceAscending: true,
      allDistancesMax: 2500,
      topHasCoordinates: true,
      uniqueIds: true,
      maxShardsQueried: 1
    },
    run: engine => engine.search({
      q: "cafe",
      geo: { near: { ...BERLIN, radiusMeters: 2500 }, sort: "distance" },
      shards: ["berlin"],
      size: 18
    })
  },
  {
    id: "production-near-boost",
    family: "nearby",
    scenario: "Bias text search toward the map without hard restricting it",
    weight: 3,
    common: false,
    production: true,
    budget: BUDGETS.discovery,
    expect: {
      minResults: 10,
      allTopShards: ["berlin", "brandenburg"],
      firstDistanceMax: 5000,
      topHasCoordinates: true,
      uniqueIds: true,
      maxShardsQueried: 3
    },
    run: engine => searchOsmQuery(engine, { q: "coffee", near: BERLIN, size: 18 })
  },
  {
    id: "production-global-typo",
    family: "typo",
    scenario: "Recover a zero-hit typo without a location anchor",
    weight: 3,
    common: false,
    production: true,
    budget: BUDGETS.recovery,
    expect: {
      minResults: 1,
      anyTextAny: ["Hauptbahnhof"],
      allTopShards: ["berlin"],
      checkTop: 5,
      topHasCoordinates: true,
      lanes: ["osmNamedTextLocality"],
      maxShardsQueried: 2
    },
    run: engine => searchOsmQuery(engine, { q: "hauptbanhof berlin", size: 18 })
  },
  {
    id: "production-empty-result",
    family: "negative",
    scenario: "Return a bounded empty result for an unknown place",
    weight: 2,
    common: false,
    production: true,
    budget: BUDGETS.direct,
    expect: { maxResults: 0, maxShardsQueried: 1 },
    run: engine => searchOsmQuery(engine, { q: "zzqxjkv nowhere", shards: ["alberta"], size: 18 })
  },
  {
    id: "coordinates",
    family: "reverse-geocode",
    scenario: "Reverse-geocode decimal coordinates typed into map search",
    weight: 5,
    common: false,
    production: true,
    budget: BUDGETS.reverseGeocode,
    expect: {
      minResults: 1,
      topTextAny: ["Robert-Bourassa", "René-Lévesque"],
      topTypes: ["address", "interpolated_address_range"],
      topLocalityAny: ["Montréal", "Montreal"],
      topHasCoordinates: true,
      topHasAddress: true,
      topHasId: true,
      firstDistanceMax: 100,
      lanes: ["osmReverseGeocode"],
      maxShardsQueried: 1
    },
    run: engine => searchOsmQuery(engine, { q: "45.5019, -73.5674", size: 8 })
  },
  {
    id: "production-reverse-interpolated",
    family: "reverse-geocode",
    scenario: "Reverse-geocode a point represented by an address interpolation range",
    weight: 3,
    common: false,
    production: true,
    budget: BUDGETS.reverseGeocode,
    expect: {
      minResults: 1,
      topTextAny: ["Libersan"],
      topTypes: ["address", "interpolated_address_range"],
      topLocalityAny: ["Sainte-Thérèse", "Sainte-Therese"],
      topHasCoordinates: true,
      topHasAddress: true,
      topHasId: true,
      firstDistanceMax: 100,
      lanes: ["osmReverseGeocode"],
      maxShardsQueried: 1
    },
    run: engine => reverseGeocodeOsm(engine, {
      lat: 45.647554,
      lon: -73.8311837,
      radiusMeters: 500,
      size: 8
    })
  },
  {
    id: "production-reverse-international",
    family: "reverse-geocode",
    scenario: "Reverse-geocode an international city coordinate",
    weight: 3,
    common: false,
    production: true,
    budget: BUDGETS.reverseGeocode,
    expect: {
      minResults: 1,
      topTextAny: ["Spandauer Straße", "Spandauer Strasse"],
      topTypes: ["address", "interpolated_address_range"],
      topLocalityAny: ["Berlin"],
      topHasCoordinates: true,
      topHasAddress: true,
      topHasId: true,
      firstDistanceMax: 100,
      lanes: ["osmReverseGeocode"],
      maxShardsQueried: 3
    },
    run: engine => reverseGeocodeOsm(engine, { ...BERLIN, radiusMeters: 1000, size: 8 })
  },
  {
    id: "production-reverse-rural",
    family: "reverse-geocode",
    scenario: "Return the closest bounded address in a sparse rural area",
    weight: 2,
    common: false,
    production: true,
    budget: BUDGETS.reverseGeocode,
    expect: {
      minResults: 1,
      topTextAny: ["Chemin d'Entrelacs"],
      topTypes: ["address", "interpolated_address_range"],
      topLocalityAny: ["Sainte-Marguerite-du-lac-Masson", "Sainte-Marguerite-du-Lac-Masson"],
      topHasCoordinates: true,
      topHasAddress: true,
      topHasId: true,
      firstDistanceMax: 2500,
      lanes: ["osmReverseGeocode"],
      maxShardsQueried: 1
    },
    run: engine => reverseGeocodeOsm(engine, {
      lat: 46.0737,
      lon: -74.0687,
      size: 8
    })
  },
  {
    id: "production-reverse-uncovered",
    family: "reverse-geocode",
    scenario: "Return bounded zero results for an uncovered ocean coordinate",
    weight: 2,
    common: false,
    production: true,
    budget: BUDGETS.reverseGeocode,
    expect: {
      maxResults: 0,
      lanes: ["osmReverseGeocode"],
      maxShardsQueried: 0
    },
    run: engine => reverseGeocodeOsm(engine, { lat: 0, lon: -140, size: 8 })
  }
];

function parseArgs(argv) {
  const out = {
    base: "https://osm.rangefind.dev/",
    cases: null,
    output: "",
    profile: "common",
    timeoutMs: 90_000,
    list: false,
    strict: false,
    summaryOnly: false
  };
  for (const arg of argv) {
    if (arg.startsWith("--base=")) out.base = arg.slice("--base=".length);
    else if (arg.startsWith("--cases=")) out.cases = new Set(arg.slice("--cases=".length).split(",").filter(Boolean));
    else if (arg.startsWith("--out=")) out.output = arg.slice("--out=".length);
    else if (arg.startsWith("--profile=")) out.profile = arg.slice("--profile=".length);
    else if (arg.startsWith("--timeout-ms=")) out.timeoutMs = Math.max(1000, Number(arg.slice("--timeout-ms=".length)) || out.timeoutMs);
    else if (arg === "--list") out.list = true;
    else if (arg === "--strict") out.strict = true;
    else if (arg === "--summary-only") out.summaryOnly = true;
  }
  return out;
}

function bucketFromUrl(value) {
  let path;
  try {
    path = new URL(String(value)).pathname;
  } catch {
    return "other";
  }
  if (/manifest[^/]*\.json/u.test(path)) return path.includes("/shards/") ? "shardManifest" : "rootManifest";
  if (path.includes("/terms/block-packs/")) return "postingBlocks";
  if (path.includes("/terms/packs/")) return "terms";
  if (path.includes("/authority/")) return "authority";
  if (path.includes("/doc-values/")) return "docValues";
  if (path.includes("/docs/pointers/")) return "docPointers";
  if (path.includes("/docs/pages/")) return "docPagePointers";
  if (path.includes("/docs/page-packs/")) return "docPages";
  if (path.includes("/docs/")) return "docs";
  if (path.includes("/geo/")) return "geo";
  if (path.includes("/facets/")) return "facets";
  if (path.includes("/filter-bitmaps/")) return "filterBitmaps";
  if (path.includes("/directory-")) return "directory";
  if (path.includes("/suggest/")) return "suggest";
  return "other";
}

function createFetchMeter({ concurrency = 32, attempts = 3 } = {}) {
  const nativeFetch = globalThis.fetch;
  let active = 0;
  const waiters = [];
  let state;
  const reset = () => {
    state = { requests: 0, bytes: 0, by: {}, shards: new Set() };
  };
  const acquire = () => active < concurrency
    ? (active++, Promise.resolve())
    : new Promise(resolve => waiters.push(resolve));
  const release = () => {
    active--;
    const next = waiters.shift();
    if (next) {
      active++;
      next();
    }
  };
  reset();
  globalThis.fetch = async (input, init) => {
    await acquire();
    try {
      let response;
      let lastError;
      for (let attempt = 0; attempt < attempts; attempt++) {
        try {
          response = await nativeFetch(input, init);
          if (attempt < attempts - 1 && [429, 500, 502, 503, 504].includes(response.status)) {
            await response.body?.cancel();
            continue;
          }
          break;
        } catch (error) {
          lastError = error;
        }
      }
      if (!response) throw lastError;
      const url = String(input?.url || input);
      const bucket = bucketFromUrl(url);
      const row = state.by[bucket] || (state.by[bucket] = { requests: 0, bytes: 0 });
      const bytes = Number(response.headers.get("content-length") || 0);
      state.requests++;
      row.requests++;
      if (Number.isFinite(bytes) && bytes > 0) {
        state.bytes += bytes;
        row.bytes += bytes;
      }
      const shard = /\/shards\/([^/]+)\//u.exec(url)?.[1];
      if (shard) state.shards.add(shard);
      return response;
    } finally {
      release();
    }
  };
  return {
    reset,
    snapshot: () => ({
      requests: state.requests,
      bytes: state.bytes,
      by: state.by,
      shards: [...state.shards].sort()
    }),
    restore: () => {
      globalThis.fetch = nativeFetch;
    }
  };
}

function resultSummary(response) {
  const rows = response.results || response.suggestions || [];
  return {
    total: Number(response.total ?? rows.length),
    lane: response.stats?.plannerLane || response.stats?.suggestLane || response.stats?.geoLane || null,
    shardsQueried: response.stats?.shardsQueried ?? null,
    items: rows.slice(0, 5).map(item => ({
      id: item.id || "",
      text: item.name || item.title || item.text || "",
      type: item.type || item.category || "",
      city: item.city || "",
      address: item.address || "",
      shard: item.shard || "",
      distanceMeters: Number.isFinite(item.distanceMeters) ? item.distanceMeters : null
    }))
  };
}

async function timed(run) {
  const started = performance.now();
  const response = await run();
  return { ms: performance.now() - started, response };
}

async function runWorker(caseId, base) {
  const definition = CASES.find(item => item.id === caseId);
  if (!definition) throw new Error(`Unknown Maps benchmark case: ${caseId}`);
  const meter = createFetchMeter();
  try {
    const engine = await createSearch({ baseUrl: base });
    const cold = await timed(() => definition.run(engine));
    const coldMeter = meter.snapshot();
    meter.reset();
    const warm = await timed(() => definition.run(engine));
    const coldRun = { ms: cold.ms, ...coldMeter };
    const warmRun = { ms: warm.ms, ...meter.snapshot() };
    return {
      id: definition.id,
      family: definition.family,
      scenario: definition.scenario,
      weight: definition.weight,
      cold: coldRun,
      warm: warmRun,
      quality: evaluateExpectations(cold.response, definition.expect),
      budget: evaluateBudgets(coldRun, warmRun, definition.budget),
      result: resultSummary(cold.response)
    };
  } finally {
    meter.restore();
  }
}

function runChild(definition, args) {
  return new Promise(resolve => {
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url)], {
      env: {
        ...process.env,
        RANGEFIND_MAPS_BENCH_CASE: definition.id,
        RANGEFIND_MAPS_BENCH_BASE: args.base
      },
      stdio: ["ignore", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, args.timeoutMs);
    child.stdout.on("data", chunk => {
      stdout += chunk;
    });
    child.stderr.on("data", chunk => {
      stderr += chunk;
    });
    child.on("close", code => {
      clearTimeout(timer);
      if (timedOut) {
        resolve({ id: definition.id, family: definition.family, error: `timeout after ${args.timeoutMs}ms` });
        return;
      }
      if (code !== 0) {
        resolve({ id: definition.id, family: definition.family, error: stderr.trim() || `worker exited ${code}` });
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch {
        resolve({ id: definition.id, family: definition.family, error: `invalid worker output: ${stdout.slice(0, 500)}` });
      }
    });
  });
}

const workerCase = process.env.RANGEFIND_MAPS_BENCH_CASE;
if (workerCase) {
  const result = await runWorker(workerCase, process.env.RANGEFIND_MAPS_BENCH_BASE || "https://osm.rangefind.dev/");
  process.stdout.write(JSON.stringify(result));
} else {
  const args = parseArgs(process.argv.slice(2));
  const knownProfiles = new Set(["common", "production", "full", "edge", ...CASES.map(item => item.family)]);
  if (!knownProfiles.has(args.profile)) {
    throw new Error(`Unknown Maps benchmark profile ${args.profile}; expected common, full, or a case family.`);
  }
  const selected = CASES.filter(item => {
    if (args.cases) return args.cases.has(item.id);
    if (args.profile === "full") return true;
    if (args.profile === "common") return item.common !== false;
    if (args.profile === "production") return item.common !== false || item.production === true;
    if (args.profile === "edge") return item.edge === true;
    return item.family === args.profile;
  });
  if (args.list) {
    console.log(JSON.stringify(CASES.map(item => ({
      id: item.id,
      family: item.family,
      scenario: item.scenario,
      weight: item.weight,
      common: item.common !== false,
      production: item.common !== false || item.production === true,
      edge: item.edge === true
    })), null, 2));
    process.exit(0);
  }
  const report = {
    base: args.base,
    at: new Date().toISOString(),
    profile: args.cases ? "custom" : args.profile,
    runtimeVersion: JSON.parse(await (await import("node:fs/promises")).readFile(
      new URL("../package.json", import.meta.url),
      "utf8"
    )).version,
    cases: []
  };
  for (let index = 0; index < selected.length; index++) {
    const definition = selected[index];
    const result = await runChild(definition, args);
    report.cases.push(result);
    if (result.error) {
      console.log(`[${index + 1}/${selected.length}] ${definition.id}: ERROR ${result.error}`);
    } else {
      console.log(
        `[${index + 1}/${selected.length}] ${definition.id}: `
        + `${Math.round(result.cold.ms)}ms, ${result.cold.requests} req, `
        + `${(result.cold.bytes / 1024).toFixed(0)} KiB, `
        + `${result.cold.shards.length} shard(s), ${result.result.total} result(s), `
        + `quality=${result.quality.passed ? "pass" : "FAIL"}, budget=${result.budget.passed ? "pass" : "MISS"}`
      );
    }
  }
  report.summary = summarizeCases(report.cases);
  if (args.output) writeFileSync(args.output, JSON.stringify(report, null, 2) + "\n");
  console.log(JSON.stringify(args.summaryOnly ? report.summary : report, null, 2));
  if (args.strict && report.summary.failures.length) process.exitCode = 1;
}
