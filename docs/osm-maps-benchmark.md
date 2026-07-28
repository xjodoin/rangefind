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
localities remain direct authority lookups. Decimal coordinates are handled as
navigation intents, and intersections are validated as locality-scoped street
pairs instead of broad text matches.

## Commands

```sh
# Weighted 20-case workload representing frequent phone searches.
npm run bench:osm-maps

# All 29 cases, including lower-frequency transit, postal-code, native-script,
# empty-viewport, brand-variant, and coordinate probes.
npm run bench:osm-maps:full

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
  distance ordering, routing lane, and shard fan-out;
- a phone-oriented latency, request, and transfer budget.

The summary reports weighted quality and budget pass rates, weighted mean cold
cost, p50/p95 latency, family-level rollups, and exact failed checks. Budget
misses do not stop exploratory runs; use `--strict` in promotion or CI gates.

Current workload sources:

- [Search locations on Google Maps](https://support.google.com/maps/answer/3092445?hl=en)
- [Search for nearby places and explore the area](https://support.google.com/maps/answer/4610185?hl=en)
- [Search by latitude and longitude](https://support.google.com/maps/answer/18539?hl=en)
