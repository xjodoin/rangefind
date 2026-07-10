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

# National scale (United States, 32.8M places, downloads ~11.2 GB PBF)
RANGEFIND_OSM_US_ROOT=/Volumes/large-disk/rangefind-osm-us \
  npm run bench:osm-geo:us
```

The US driver defaults to `.cache/osm-us`; set `RANGEFIND_OSM_US_ROOT` to a
volume with at least 100 GB free for the source PBF, resumable extraction
artifacts, build scratch space, and final index. Downloads use `curl -C -`
and publish atomically after completion.

Large extraction is a four-stage bounded pipeline: candidate ways are spooled
to JSONL, their node anchors are externally sorted/deduplicated, matching node
coordinates are stored in an indexed SQLite table while node documents are
spooled, and the final corpus is materialized sequentially. Each completed
stage has PBF size/mtime metadata and is reused on restart. The extractor does
not retain all ways or coordinates in JavaScript heap.

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
and text-plus-radius results against streaming exhaustive Haversine scans of
the corpus. The oracle retains only the densest-cell counters and bounded
top-20 nearest sets, so validation is safe at national scale.

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
