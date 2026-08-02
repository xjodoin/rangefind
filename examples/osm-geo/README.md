# OSM Geo Search Example

This example builds a geo-enabled place and address index from an OpenStreetMap
regional extract. It benchmarks canonical address lookup plus Lucene-style geo
queries (bounding box, radius, nearest-neighbor distance sort, and
text-plus-geo) against exhaustive oracles.

## Integration API

OSM behavior is a reusable integration rather than demo-only code. Browser-safe
document conversion, schema generation, search intents, and autocomplete are
exported from `rangefind/osm`; bounded PBF/RQA build helpers are exported from
`rangefind/osm/node`:

```js
import {
  createOsmIndexConfig,
  reverseGeocodeOsm,
  searchOsmQuery,
  suggestOsmQuery
} from "rangefind/osm";
import { buildOsmIndex, augmentOsmWithRqa } from "rangefind/osm/node";
```

Both APIs produce and query the normal Rangefind pack format. There is no OSM
sidecar or parallel runtime. `scripts/osm_fixture.mjs` remains a resumable CLI
for PBF extraction and delegates document shaping, RQA ingestion, schema
generation, and index publication to these modules.

`reverseGeocodeOsm(engine, { lat, lon, radiusMeters, size })` resolves nearest
indexed addresses inside a hard radius and routes only through shards whose
bounding boxes intersect that radius. Coordinate text passed to
`searchOsmQuery` uses the same address-first path automatically.

The hosted map client queries the rolling sharded index published by the
sibling [`osm-rangefind-index`](https://github.com/xjodoin/osm-rangefind-index)
project at `https://osm.rangefind.dev/`. The fixture commands below remain
available for local builds, benchmarks, and development against one regional
extract.

## Build

```bash
# Small region (Luxembourg, ~175k places, downloads ~450 MB PBF)
node scripts/osm_fixture.mjs all --region=luxembourg --root=examples/osm-geo

# Large region (Quebec, millions of places, downloads ~1.2 GB PBF)
# RQA civic/postal coverage is enabled by default for this region.
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

Québec builds also download the monthly, CC BY 4.0
[Référentiel québécois des adresses (RQA)](https://www.donneesquebec.ca/recherche/dataset/referentiel-quebecois-des-adresses).
The download is resumable and can be shared across workspaces with
`RANGEFIND_RQA_ARCHIVE=/path/to/RQA_CSV.zip`; pass `--no-rqa` for an OSM-only
comparison. Ingestion streams the zipped CSV, collapses apartment/unit rows to
one civic point, and uses an indexed on-disk canonical set to suppress only
full addresses already represented identically by OSM. Spatially nearby OSM
points are retained alongside the RQA authority record when their published
components differ, ensuring the official full address remains searchable. It
emits one additional aggregate record per postal-code/municipality pair.

RQA civic records intentionally have no BM25 title/body terms, geo-browse
point, or autocomplete surface. They contribute only compressed display data
and canonical authority keys, so a complete address still uses the bounded
zero-posting exact lane without making ordinary place search, map browse, or
autocomplete proportional to the residential corpus. Postal aggregates are
searchable and suggestible; a postal-only query returns the public area
centroid and civic-address count rather than enumerating private residences.

The July 2026 full Québec run read 5,307,418 RQA rows and emitted 3,635,146
unique civic records plus 221,677 postal-area aggregates; 12,863 addresses
already represented identically by OSM were suppressed. The merged corpus has
9,952,563 documents. RQA merge took 116.5 seconds and the complete resumable
index build took 925.1 seconds (15m25s). The published index is 8.39 GiB,
versus 5.52 GiB for the prior OSM-only/interpolation index. Despite adding
3.86M authority/display records, posting reduction remained 37.9 seconds with
the same 13,433 posting segments, and geo construction remained 5.4 seconds,
because residential civic points do not enter either structure.

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

An exact settlement query such as `Laval` uses the same cached place resolver
before ordinary BM25. It returns the canonical city/town/village record,
disables a stale map viewport, and centers the map instead of displaying the
many POIs and addresses that merely contain the locality name.

Street-plus-locality queries use a similarly bounded plan. `Rue Hector
Rosemère` first resolves the municipality, removes the high-frequency road
designator from the text plan, and searches the distinctive street name within
the town radius. Exact OSM road segments are collapsed to one canonical street
result. This avoids exhausting the posting budget on `rue` and requires no
per-street sidecar or duplicate street index.

Autocomplete applies the same street-level presentation without changing the
index. When no house number is present, a bounded candidate window groups
civic and interpolation titles by street and municipality, so `Rue Libersan
Saint` offers `Rue Libersan, Sainte-Thérèse` instead of spending every visible
slot on individual house numbers. Numeric prefixes keep the address-level and
interpolated suggestions unchanged.

A small hierarchy of canonical address keys per document is stored in the
packed authority index: full address, house/street/locality,
house/street/postcode, and house/street. Equivalent forms such as `Fifth
Avenue`/`5th Ave`, `Northwest`/`NW`, reordered components, and those useful
partial forms use a zero-posting exact lane. Other incomplete or
place-plus-address queries fall back to the weighted inverted index.
Canadian postal codes may be entered with or without their customary space:
`J7B 1Z5` and `J7B1Z5` share the same query plan and existing index data.

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
