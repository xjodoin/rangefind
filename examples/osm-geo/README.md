# OSM Geo Search Example

This example builds a geo-enabled Rangefind index from an OpenStreetMap
regional extract and benchmarks Lucene-style geo queries (bounding box,
radius, nearest-neighbor distance sort, and text-plus-geo) against an
exhaustive oracle.

## Build

```bash
# Small region (Luxembourg, ~175k places, downloads ~450 MB PBF)
node scripts/osm_fixture.mjs all --region=luxembourg --root=examples/osm-geo

# Large region (Quebec, millions of places, downloads ~1.2 GB PBF)
node scripts/osm_fixture.mjs all --region=quebec --root=examples/osm-geo
```

The fixture converts tagged OSM nodes and ways (anchored at their first node)
into place documents with `name`, `aliases`, `category`, `type`, and `lat`/`lon`
fields, then builds an index with a `geo` field:

```json
{
  "geo": [{ "name": "location", "latPath": "lat", "lonPath": "lon" }]
}
```

## Benchmark

```bash
node scripts/osm_geo_bench.mjs --root=examples/osm-geo
```

Reports cold HTTP request counts, transfer KB, warm latency, and geo tree
traversal stats per query lane, then verifies bounding box, radius, nearest,
and text-plus-radius results against exhaustive Haversine scans of the corpus.

## Query API

```js
const engine = await createSearch({ baseUrl: "./rangefind/" });

// Places inside a map viewport.
await engine.search({ q: "", geo: { box: { minLat, maxLat, minLon, maxLon } } });

// Places within 5 km, unordered browse.
await engine.search({ q: "", geo: { near: { lat, lon, radiusMeters: 5000 } } });

// Exact nearest places, sorted by distance.
await engine.search({ q: "", geo: { near: { lat, lon }, sort: "distance" } });

// Text search restricted to a radius, with distance decay boost.
await engine.search({
  q: "bakery",
  geo: {
    near: { lat, lon, radiusMeters: 5000 },
    boost: { weight: 2, pivotMeters: 500 }
  }
});
```

Results carry `distanceMeters` whenever `geo.near` is present, and geo
responses expose `stats.geoLane`, `stats.geoLeavesVisited`,
`stats.geoPointsScanned`, and related counters.
