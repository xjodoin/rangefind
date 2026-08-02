# OSM maps workload benchmark

`npm run bench:osm-maps` exercises a weighted set of common interactive map
searches against a deployed Rangefind OSM index. Each cold case runs in a new
process so caches and pathological queries cannot contaminate later cases; the
same engine then repeats the query to measure the warm path.

The common profile follows the public Google Maps search examples and nearby
discovery model: named businesses, categories near the user, categories within
a city, civic addresses, intersections, airports, incomplete input, map
viewport searches, and typo recovery. Google describes local ranking as a
combination of relevance, distance, and prominence, so the benchmark checks
result identity, locality, viewport containment, shard count, and nearest-first
ordering in addition to transport cost.

Autocomplete cases carry the current map center, matching an interactive maps
session rather than an unscoped global typeahead. Category-plus-locality input
is evaluated as a composed suggestion, while civic addresses, brands, and
localities remain direct authority lookups. Predictions are checked for target
rank, uniqueness, structured primary/secondary text, match ranges, cursor-edit
behavior, and a reusable selection payload. Decimal coordinates invoke the
same bounded reverse-geocoding API available to applications, and intersections
are validated as locality-scoped street pairs instead of broad text matches.

The production profile models the client-side replacement contract for the
Google Maps search APIs. It adds multi-step autocomplete selection, global and
city-qualified landmarks, native-script suggestions, explicit category-in-city
queries, exact and interpolated forward geocoding, hard-radius nearby search,
viewport restriction, discovery around a selected result, location-biased text,
unanchored typo recovery, empty results, inline place-detail fields, and urban,
interpolated, international, rural, locality-filtered, and uncovered
reverse-geocoding probes. The next-index profile adds strict checks for compact
OSM place details and exact named-road authority once those build-time fields
have been published.

## API replacement coverage

| Google Maps surface | Rangefind benchmark contract | Status |
| --- | --- | --- |
| Places Autocomplete | Locality, category, brand, address, native script, cursor edits, structured predictions, and suggestion-selection journey | Covered |
| Text Search | Named landmarks, category/locality, typo recovery, language variants, and bounded misses | Covered |
| Nearby Search | Nearest, hard radius, dense/sparse categories, and selected-place discovery orbit | Covered |
| Forward Geocoding | Civic, street, postal, intersection, and interpolated addresses | Covered |
| Place Details | Stable id, coordinates, address/locality, type, hours, contact, brand/operator, cuisine, accessibility, service/payment capabilities, and OSM references returned inline | Covered for indexed OSM fields |
| Reverse Geocoding | Address-first coordinate lookup with a hard radius, covering-shard routing, structured components, result/accuracy filters, locality fallback, interpolation ranges, international/rural results, and bounded zero results | Covered |
| Photos, reviews, live hours, traffic, and directions | Not present in the static OSM search index | Out of scope |

This is still not complete Google Maps API parity: dynamic place content,
photos, reviews, live hours, traffic, and directions remain out of scope. The
production score measures the supported static search and geocoding contract,
while the full profile also retains lower-volume and pathological routing
probes.

Applications can call reverse geocoding directly. Coordinate text passed to
`searchOsmQuery` uses this same implementation by default; pass
`reverseGeocode: false` only when a raw coordinate marker is desired.

```js
import { reverseGeocodeOsm } from "rangefind/osm";

const response = await reverseGeocodeOsm(engine, {
  lat: 45.5019,
  lon: -73.5674,
  radiusMeters: 5000,
  size: 8
});
```

## Commands

```sh
# Weighted 21-case workload representing frequent phone searches.
npm run bench:osm-maps

# Strict 43-case Google Maps search/geocoding replacement workload.
npm run bench:osm-maps:production

# Strict 45-case promotion gate for a freshly rebuilt schema-v2 OSM index.
npm run bench:osm-maps:next-index

# All 56 cases, including lower-frequency transit, postal-code, native-script,
# empty-viewport, brand-variant, reverse-geocoding, and routing edge probes.
npm run bench:osm-maps:full

# Six strict urban, interpolation, international, rural, locality, and uncovered
# reverse-geocoding cases.
npm run bench:osm-maps:reverse

# Seven ambiguity and locality-boundary cases used to stress routing shortcuts.
npm run bench:osm-maps:edge

# One family or an explicit regression subset.
npm run bench:osm-maps -- --profile=viewport
npm run bench:osm-maps -- --cases=brand-near,viewport-brand

# Fail the command when a quality check or phone budget misses.
npm run bench:osm-maps -- --strict

# Machine-readable report and case catalog.
npm run bench:osm-maps -- --out=/tmp/osm-maps.json
npm run bench:osm-maps -- --summary-only
npm run bench:osm-maps:list
```

## Report

Every case includes:

- a scenario name, family, and workload weight;
- cold and warm latency, requests, bytes, fetch buckets, and opened shards;
- declarative quality checks for expected text/type, locality, viewport,
  distance ordering, routing lane, shard fan-out, target rank, structured
  predictions, reverse semantics, and OSM detail coverage;
- a phone-oriented latency, request, and transfer budget.

The summary reports weighted quality and budget pass rates, weighted mean cold
cost, p50/p95 latency, mean reciprocal rank, family-level rollups, and exact failed checks. Budget
misses do not stop exploratory runs; use `--strict` in promotion or CI gates.

Current workload sources:

- [Places API overview](https://developers.google.com/maps/documentation/places/web-service/op-overview)
- [Places Autocomplete](https://developers.google.com/maps/documentation/places/web-service/place-autocomplete)
- [Text Search](https://developers.google.com/maps/documentation/places/web-service/text-search)
- [Geocoding API overview](https://developers.google.com/maps/documentation/geocoding/overview)
- [Reverse geocoding](https://developers.google.com/maps/documentation/geocoding/reverse-geocoding)
