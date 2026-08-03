import test from "node:test";
import assert from "node:assert/strict";
import {
  createRangefindMapsAdapter,
  toMigrationPlace
} from "../examples/osm-geo/google-maps-adapter.js";

test("Google Maps migration adapter shapes OSM results without hiding Rangefind metadata", () => {
  const place = toMigrationPlace({
    id: "node/42",
    name: "Cafe Range",
    type: "cafe",
    lat: 45.5,
    lon: -73.6,
    address: "42 Range Street, Montreal",
    distanceMeters: 123,
    details: {
      opening_hours: "24/7",
      wheelchair: "yes",
      phone: "+1-555-0100",
      website: "https://example.test"
    },
    openNow: true,
    openingHoursState: "open"
  });

  assert.equal(place.id, "node/42");
  assert.deepEqual(place.displayName, { text: "Cafe Range" });
  assert.deepEqual(place.location, { latitude: 45.5, longitude: -73.6 });
  assert.equal(place.openingHours.openNow, true);
  assert.equal(place.accessibilityOptions.wheelchair, "yes");
  assert.equal(place.rangefind.distanceMeters, 123);
  assert.equal(place.source.dataset, "OpenStreetMap");
});

test("Google Maps migration adapter preserves non-OSM dataset provenance", () => {
  const place = toMigrationPlace({
    id: "postal/CA/J7A1V6",
    name: "J7A 1V6, Rosemère",
    type: "postal_code",
    postcode: "J7A 1V6",
    source: "GeoNames",
    lat: 45.64,
    lon: -73.7971
  });
  assert.equal(place.source.dataset, "GeoNames");
  assert.equal(place.source.osmType, null);
  assert.equal(place.source.osmId, null);
});

test("Google Maps migration adapter translates common request surfaces", async () => {
  const calls = [];
  const result = {
    id: "node/1",
    name: "Range Cafe",
    type: "cafe",
    lat: 45.51,
    lon: -73.61,
    distanceMeters: 80,
    details: { opening_hours: "24/7" },
    openNow: true
  };
  const adapter = createRangefindMapsAdapter({ manifest: {} }, {
    defaults: { timeZone: "America/Toronto" },
    async searchOsmQuery(_engine, params) {
      calls.push(["search", params]);
      return { total: 1, results: [result], stats: { plannerLane: "test" } };
    },
    async suggestOsmQuery(_engine, params) {
      calls.push(["suggest", params]);
      return {
        suggestions: [{
          text: "Range Cafe, Montreal",
          mainText: "Range Cafe",
          secondaryText: "Montreal",
          types: ["cafe"],
          matchedRanges: [{ start: 0, end: 5 }],
          selection: { query: "Range Cafe", shards: ["quebec"] }
        }]
      };
    },
    async reverseGeocodeOsm(_engine, params) {
      calls.push(["reverse", params]);
      return { total: 1, results: [result] };
    },
    async searchAlongRouteOsm(_engine, params) {
      calls.push(["route", params]);
      return { total: 1, results: [{ ...result, routeDistanceMeters: 25, rejoinPoint: { lat: 45.5, lon: -73.6 } }] };
    }
  });

  const text = await adapter.textSearch({
    textQuery: "cafe Montreal",
    openNow: true,
    locationBias: { circle: { center: { latitude: 45.5, longitude: -73.6 }, radius: 5000 } }
  });
  assert.equal(text.status, "OK");
  assert.equal(text.places[0].id, "node/1");
  assert.equal(calls[0][1].constraints.openNow, true);
  assert.deepEqual(calls[0][1].near, { lat: 45.5, lon: -73.6 });

  const suggestions = await adapter.autocomplete({ input: "range caf", inputOffset: 9 });
  assert.equal(suggestions.suggestions[0].placePrediction.structuredFormat.mainText.text, "Range Cafe");
  assert.deepEqual(suggestions.suggestions[0].placePrediction.rangefindSelection.shards, ["quebec"]);

  const reverse = await adapter.reverseGeocode({
    location: { latitude: 45.5, longitude: -73.6 },
    resultTypes: ["street_address"]
  });
  assert.equal(reverse.results.length, 1);
  assert.deepEqual(calls.find(([kind]) => kind === "reverse")[1].resultTypes, ["street_address"]);

  const route = await adapter.searchAlongRoute({
    route: "encoded-route",
    query: "cafe",
    corridorMeters: 1200,
    openNow: true
  });
  assert.equal(route.places[0].rangefind.routeDistanceMeters, 25);
  assert.equal(calls.find(([kind]) => kind === "route")[1].timeZone, "America/Toronto");

  const details = await adapter.placeDetails({ placeId: "node/1" });
  assert.equal(details.place.displayName.text, "Range Cafe");
});

test("Google Maps migration nearby adapter unions types and applies a hard radius", async () => {
  const queries = [];
  const adapter = createRangefindMapsAdapter({ manifest: {} }, {
    async searchOsmQuery(_engine, params) {
      queries.push(params);
      if (params.query === "cafe") {
        return { results: [
          { id: "cafe/1", name: "Cafe", type: "cafe", lat: 45.5, lon: -73.6, distanceMeters: 90 },
          { id: "fuel/1", name: "Fuel", type: "fuel", lat: 45.5, lon: -73.6, distanceMeters: 40 }
        ] };
      }
      return { results: [
        { id: "restaurant/1", name: "Restaurant", type: "restaurant", lat: 45.5, lon: -73.6, distanceMeters: 20 },
        { id: "cafe/1", name: "Cafe", type: "cafe", lat: 45.5, lon: -73.6, distanceMeters: 90 }
      ] };
    }
  });

  const response = await adapter.nearbySearch({
    includedTypes: ["cafe", "restaurant"],
    excludedTypes: ["gas_station"],
    maxResultCount: 5,
    rankPreference: "DISTANCE",
    locationRestriction: {
      circle: {
        center: { latitude: 45.5, longitude: -73.6 },
        radius: 1500
      }
    }
  });

  assert.deepEqual(queries.map(item => item.query), ["cafe", "restaurant"]);
  assert.deepEqual(queries[0].geo, {
    near: { lat: 45.5, lon: -73.6, radiusMeters: 1500 },
    sort: "distance"
  });
  assert.deepEqual(response.places.map(item => item.id), ["restaurant/1", "cafe/1"]);
});
