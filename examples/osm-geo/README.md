# OSM Geo Search Example

This example builds a geo-enabled place and address index from an OpenStreetMap
regional extract. It benchmarks canonical address lookup plus Lucene-style geo
queries (bounding box, radius, nearest-neighbor distance sort, and
text-plus-geo) against exhaustive oracles.

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

The fixture converts named places and complete `addr:housenumber` +
`addr:street`/`addr:place` nodes and ways (anchored at their first node) into
documents. Address results expose `address`, `house_number`, `street`, `unit`,
`city`, `state`, `postcode`, and `country` alongside `name`, `category`,
`type`, and `lat`/`lon`.

OSM `addr:interpolation=all|odd|even|N` ways are retained as compact range
documents. The extractor keeps their geometry and numeric address anchors,
splits at tagged intermediate anchors, rejects incompatible endpoint metadata,
and stores the polyline with 1e-6-degree delta encoding. It does **not** create
one document per inferred house. Instead, each range emits street-first
16-number bucket keys in the packed authority index. An exact query hydrates
only the matching range, verifies parity/step, and computes the coordinate by
distance along the OSM polyline. Explicitly tagged addresses are checked first
and therefore always win over an overlapping interpolation.

On the July 2026 Quebec extract, 442,979 interpolation ways produced 339,169
valid range documents covering about 9.15 million possible inferred addresses.
The compact representation grew the 5.76M-document corpus by 5.9%; naïve
materialization would have grown it by roughly 159%. `214 Rue Libersan,
Sainte-Thérèse` resolves through `addressInterpolationExact` with zero posting
blocks decoded.

Autocomplete recognizes a leading numeric house number once the active street
token has at least three characters. It completes the street/locality through
the existing lexicon, verifies a bounded candidate set through the same exact
range lane, and returns only addresses whose interpolation step actually
contains the requested number. No inferred address surfaces are added to the
lexicon.

The demo also recognizes category-plus-locality intents in either order. For
example, `Pharmacie Rosemère` resolves the exact `place=town` document for
Rosemère, maps the French category to the indexed `pharmacy` type, and performs
a distance-sorted geo text query within the town-scale radius. This finds POIs
such as Uniprix whose OSM node has coordinates and `amenity=pharmacy` but no
`addr:city`; resolved localities are cached for subsequent searches.

A small hierarchy of canonical address keys per document is stored in the
packed authority index: full address, house/street/locality,
house/street/postcode, and house/street. Equivalent forms such as `Fifth
Avenue`/`5th Ave`, `Northwest`/`NW`, reordered components, and those useful
partial forms use a zero-posting exact lane. Other incomplete or
place-plus-address queries fall back to the weighted inverted index.

The index also includes a `geo` field:

```json
{
  "geo": [{ "name": "location", "latPath": "lat", "lonPath": "lon" }]
}
```

## Benchmark

```bash
node scripts/osm_geo_bench.mjs --root=examples/osm-geo
node scripts/osm_address_bench.mjs --root=examples/osm-geo
```

Reports cold HTTP request counts, transfer KB, warm latency, and geo tree
traversal stats per query lane, then verifies bounding box, radius, nearest,
and text-plus-radius results against streaming exhaustive Haversine scans of
the corpus. The oracle retains only the densest-cell counters and bounded
top-20 nearest sets, so validation is safe at national scale.

## Query API

```js
const engine = await createSearch({ baseUrl: "./rangefind/" });

// Canonical exact-address lookup. Structured address fields are returned.
await engine.search({ q: "350 Fifth Ave. New York" });

// OSM interpolation range lookup. The result is marked `interpolated` and
// includes `address_accuracy` from addr:inclusion plus a computed lat/lon.
await engine.search({ q: "214 Rue Libersan, Sainte-Thérèse" });

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
