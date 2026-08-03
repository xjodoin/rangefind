# OSM Geo Query Implementation Plan

> Historical design document. The point-tree milestones that started here have
> since expanded into production OSM search, address interpolation, reverse
> geocoding, prominence ranking, geo capsules, multi-resolution category/route
> cells, route-corridor search, display geometry, and planet-scale sharding.
> Use the current [feature guide](features.md), [reference](reference.md), and
> [OSM example](../examples/osm-geo/README.md) as the product source of truth.
> The unimplemented scope retained below is polygon filtering and a general
> spatial shape-relation index, not result geometry rendering.

## Goal

Give Rangefind full-featured geo query support so a single static index can
search OpenStreetMap-derived place data by text, place semantics, category
metadata, point geometry, area geometry, and geographic constraints without a
search server.

The target is the same class of capability Lucene gets from `LatLonPoint`,
`LatLonDocValuesField`, and `LatLonShape`: bounding-box queries, radius
queries, nearest-neighbor result pages, polygon and shape relations, reverse
lookup, address-aware place ranking, and distance-aware ranking — adapted to
range-addressed static files.

Planet-scale deployment sharding was explicitly out of scope for the original
plan and was subsequently delivered as the generic `rangefind/shards` layer.
The geo tree, packs, manifests, text routing, and suggestion routing remain
local to independently updateable regions beneath a small federated root.

## Status

The point subsystem is implemented and benchmarked (see
`docs/architecture.md` "Geo Point Trees" and `docs/osm-geo-benchmarks.md`):

- `geo` config fields with hidden lat/lon double doc-values.
- E7 coordinates, bulk-loaded static KD tree, range-packed leaf pages, and a
  two-level branch-paged tree root that stays a few KB at any point count.
- Per-leaf/per-branch filter summaries for cell pruning.
- Runtime lanes: bbox/radius browse, exact nearest-neighbor (with and without
  a text query), text-plus-geo filtering, distance boost, dateline and pole
  handling, and exhaustive-oracle test coverage.
- OSM fixture pipeline (`scripts/osm_pbf.mjs`, `scripts/osm_fixture.mjs`) and
  oracle-verified benchmarks (`scripts/osm_geo_bench.mjs`) for Luxembourg and
  Quebec extracts.

The remaining original geo scope is point-in-polygon filtering, a general
static shape-relation index, and the Lucene comparison bench. Reverse
geocoding, address search/interpolation, static OSM prominence, result geometry,
route corridors, and geographic sharding are implemented through later APIs.

## Product Scope

Implemented:

- Text plus radius: `q="bakery"` near a point within `N` meters.
- Text plus bounding box: matches inside a viewport.
- Category-only geo browse: `amenity=pharmacy` inside a radius or viewport.
- Distance sort: nearest places to a point, with optional text/category
  filters, exact via early-stop proofs.
- Distance boost: combine text relevance and distance decay.
- Address-aware forward search with canonical authority keys and compact
  `addr:interpolation` ranges.
- Address-first bounded reverse geocoding with structured components,
  accuracy/result filters, locality fallback, and shard routing.
- Static OSM prominence through generic `rankPrior`.
- Geo capsules and multi-resolution category cells, including wildcard
  occupancy for brand/text route queries.
- Route-corridor search with exact cross-track distance, progress, direction,
  viewport bias, and rejoin points.
- Simplified encoded polygon/line geometry for selected-result rendering.
- Independently built geographic shards with frozen scoring statistics and
  root text/suggestion routing.

Remaining:

- Polygon filters over point places.
- Indexed spatial area/line shapes for administrative boundaries, streets,
  parks, buildings, and other OSM ways/relations. Compact result geometry is
  already supported but is not queried as a shape tree.
- Shape-vs-shape relation queries aligned with Lucene relations where
  exactness is practical: `INTERSECTS`, `WITHIN`, `CONTAINS`, and `DISJOINT`.
- Containing-area reverse hierarchy over a future shape tree. Current reverse
  geocoding is nearest-address plus bounded locality fallback.
- Optional external enrichment hooks for TIGER/OpenAddresses-style house
  number data. Québec RQA enrichment is already implemented without making it
  mandatory.

Out of scope for the static search engine: route calculation, turn-by-turn
navigation, traffic-aware/network driving distance, and detour-time
recalculation. Searching along a caller-supplied route is implemented.

## Query API

Implemented runtime shape:

```js
const result = await engine.search({
  q: "bakery",
  geo: {
    field: "location",
    near: { lat: 45.5017, lon: -73.5673, radiusMeters: 2500 },
    boost: { weight: 0.4, pivotMeters: 1000 },
    sort: "distance"
  },
  filters: {
    facets: { category: ["shop"] }
  },
  size: 10
});
```

Equivalent structured form:

```js
geo: {
  field: "location",
  box: { minLat: 45.45, maxLat: 45.56, minLon: -73.70, maxLon: -73.50 }
}
```

Planned polygon filter form:

```js
geo: {
  field: "location",
  polygon: [[45.52, -73.62], [45.53, -73.55], [45.48, -73.56]]
}
```

Planned shape relation form:

```js
geo: {
  field: "shape",
  relation: "within",
  geometry: {
    type: "Polygon",
    coordinates: [[[-73.62, 45.52], [-73.55, 45.53], [-73.56, 45.48], [-73.62, 45.52]]]
  }
}
```

Planned reverse lookup form:

```js
const reverse = await engine.reverse({
  field: "location",
  lat: 45.5017,
  lon: -73.5673,
  layers: ["address", "poi", "street", "locality", "admin"],
  size: 10
});
```

Responses carry `distanceMeters` per result when `geo.near` is present, and
`stats` exposes the traversal (`geoLane`, `geoCandidateLeaves`,
`geoLeavesVisited`, `geoPointsScanned`, `geoPointsAccepted`, exactness
flags). Shape and reverse lanes should extend the same stats surface
(`geoShapeRelation`, `reverseHierarchy`).

## OSM Input Model

Rangefind keeps JSONL as the build input; OSM parsing lives in the fixture
builder (`scripts/osm_fixture.mjs`), not the core runtime.

Normalized JSONL fields (implemented subset first):

```json
{
  "id": "node/123",
  "url": "https://www.openstreetmap.org/node/123",
  "name": "Bakery Name",
  "aliases": ["Boulangerie Name", "old name"],
  "body": "bakery shop pastry ...",
  "category": "shop",
  "type": "bakery",
  "lat": 45.5017,
  "lon": -73.5673
}
```

Planned additions for ranking and shapes:

```json
{
  "displayName": "Bakery Name, Montreal, Quebec, Canada",
  "tags": ["shop=bakery", "cuisine=pastry"],
  "countryCode": "ca",
  "adminLevel": 8,
  "placeRank": 30,
  "addressRank": 30,
  "importance": 0.00001,
  "population": null,
  "bbox": [45.5016, -73.5675, 45.5018, -73.5671]
}
```

Extraction rules:

- Keep named places and searchable POIs: `place`, `amenity`, `shop`,
  `tourism`, `leisure`, `historic`, `office`, `craft`, `healthcare`, and
  admin boundaries.
- Preserve name variants: `name`, `name:*`, `alt_name`, `old_name`,
  `short_name`, `brand`, `operator`, `ref`.
- Keep principal OSM class/type facets separately from display labels.
- Keep raw OSM ids stable.

Nominatim remains the reference model for which concepts matter: place rank,
address rank, importance, address hierarchy, and the difference between
independent places and dependent rank-30 POIs.

## Schema

Implemented:

```json
{
  "geo": [
    { "name": "location", "latPath": "lat", "lonPath": "lon" }
  ]
}
```

Planned shape source:

```json
{
  "geo": [
    { "name": "location", "latPath": "lat", "lonPath": "lon" },
    { "name": "shape", "geometryPath": "geometry" }
  ]
}
```

Config with no `geo` keeps the current output shape exactly; geo support is
an additive feature path and non-geo builds emit no geo files (enforced by
tests).

## Format Direction

### Point index (implemented, `rfgeotreeroot-v1`)

```text
rangefind/
  geo/
    location.<hash>.bin.gz        # tree root: leaf table or branch table
    point-packs/
      0000.<hash>.bin             # leaf pages + branch pages, gzip members
```

- Leaf pages: cell bbox, delta-encoded fixed-width `latE7`/`lonE7`/`docId`
  columns.
- Root/branch entries: cell bbox, point count, object pointer, and
  posting-block-style filter summaries.
- Two-level roots when leaves exceed twice `geoBranchLeaves` (default 256).

### Shape index (planned)

A sibling shape index for OSM lines and polygons that need spatial relation
queries, enabled when a geometry source is configured:

```text
rangefind/
  geo/
    shape.<hash>.bin.gz
    shape-packs/
      0000.<hash>.bin             # shape summary rows (bbox, type, flags)
    geometry-packs/
      0000.<hash>.bin             # exact geometry payloads, fetched lazily
```

- Shape leaf rows: shape bbox, `docId`, geometry type, relation flags, and a
  pointer to exact geometry bytes for final verification.
- Optional simplified geometry for fast rejection.
- Store exact geometry as encoded lon/lat integer rings or line strings;
  API input/output stays GeoJSON-compatible.
- Split point and shape packs so point-only queries never pay for geometry.

Builder steps for shapes:

1. Normalize polygons, multipolygons, and lines into validated internal
   geometry.
2. Compute bbox, centroid, simplification, and tessellation records.
3. Bulk-load shape cells by bbox/centroid with page summaries.
4. Store exact geometry bytes only where exact relation verification needs
   them.

## Runtime Lanes

Implemented:

- `geoBrowse`: bbox/radius browse over candidate leaves with per-point
  verification and early stop.
- `nearest` / `nearestText`: best-first traversal by exact point-to-cell
  distance with a top-k boundary proof; the text variant filters by an exact
  posting-derived match set bounded by `geoTextSortMaxDf`.
- `textDocSet` / `textDocValues`: text search with the geo constraint as a
  filter, geo-first when selective, doc-value verification otherwise.
- `geoExactScan` equivalent: the exhaustive oracles in tests and benchmarks.

Planned:

- `geoShape`: polygon, line, and bbox geometry relations using shape tree
  bboxes and simplified geometry to prune, exact geometry only for candidates
  that page summaries cannot prove. Supports `INTERSECTS`, `WITHIN`,
  `CONTAINS`, `DISJOINT` where exact verification is available; unsupported
  combinations fail clearly rather than returning approximate results.
- `geoReverse`: nearest point candidates from the point tree plus containing
  admin/locality/street/building candidates from the shape tree, returned as
  a ranked hierarchy with layer/type metadata.

Exactness rules (implemented ones enforced by oracle tests):

- Bbox filters are exact after comparing encoded E7 values.
- Radius filters use bbox as a coarse prune and Haversine as final
  verification.
- Distance sort returns exact top-k once the tree proves no unvisited cell
  can beat the boundary distance.
- Polygon filters over points must be exact after point-in-polygon
  verification.
- Reverse lookup must distinguish nearest point matches from containing area
  matches in the response.

## Scoring

Final place score should compose:

```text
score =
  textScore
  + authorityBoost
  + categoryBoost
  + importanceBoost
  + rankBoost
  + distanceBoost
```

Distance boost follows Lucene's distance feature shape (implemented as a
page-window rerank):

```text
distanceBoost = weight * pivotMeters / (pivotMeters + distanceMeters)
```

OSM ranking guidance for the quality pass:

- `importance` is a strong tie-breaker for well-known places.
- Lower `placeRank` usually means larger or more globally significant
  features.
- `addressRank` distinguishes addressable places from non-address parts.
- For typed category queries, exact OSM `class/type` matches should beat
  vague body-text matches.
- For local POI queries, distance should matter more than global importance
  once a reasonable category/text match is established.

## Lucene Comparison Bench

Extend the existing Lucene quality bench with geo fields:

- `LatLonPoint("location", lat, lon)` for indexed geo queries.
- `LatLonDocValuesField("location", lat, lon)` for distance sort.
- Compare `LatLonPoint.newBoxQuery`, `newDistanceQuery`, `nearest`, and
  `newDistanceFeatureQuery` against the Rangefind lanes.

New scripts:

```text
scripts/osm_geo_lucene_quality.mjs
scripts/lucene_quality/src/main/java/rangefind/bench/LuceneOsmGeoQuality.java
```

Report metrics: Hit@1/Hit@3/Hit@10 and MRR@10 for known place-name queries,
geo filter correctness against exhaustive Haversine checks, nearest top-k
agreement with Lucene, cold request count and transfer KB per lane, and
leaf/branch traversal counters.

Starter judgments: known named places, category near point, viewport
queries, ambiguous names (`Springfield`, `Paris`), and address-like queries
where the fixture has `addr:*` coverage.

## Tests

Implemented (see `test/geo.test.js`):

- Coordinate validation, E7 bounds, Haversine, radius-to-bbox including the
  antimeridian and poles, exact point-to-box min/max distance bounds.
- Leaf/root/branch codec round trips.
- Exhaustive-oracle agreement for bbox, radius, nearest, filtered nearest,
  text+geo, text+distance-sort, dateline boxes, pagination, and both
  single-level and branch-paged trees.
- Non-geo builds emit no geo artifacts and reject geo queries clearly.
- Per-cell filter summary pruning.

Planned for shapes and reverse:

- Polygon winding, closure, bbox, simplification, and point-in-polygon
  checks.
- Shape relation verification for supported geometry pairs.
- Reverse hierarchy ranking and layer/type ordering.
- Exact agreement against in-test exhaustive implementations, matching the
  point-lane oracle style.

## Milestones

### Milestone 1: Point index, OSM fixture, and oracle benchmarks — DONE

Delivered: `src/geo_tree.js`, builder and runtime wiring, `geo` config,
hidden lat/lon doc-values, two-level roots, per-cell filter summaries, OSM
fixture and PBF parser, oracle-verified Luxembourg and Quebec benchmarks,
text+geo lanes, exact nearest with and without text.

### Milestone 2: Polygon filters over points

- Polygon normalization (winding, closure, bbox) and point-in-polygon
  verification.
- `geo.polygon` query form using the point tree with polygon bbox pruning
  and exact per-point verification.
- Oracle tests and a benchmark lane.

### Milestone 3: Static shape tree

- `src/geo_shape.js` codec and builder helpers.
- Shape tree pages, shape packs, and geometry packs.
- Indexed line/polygon bbox pruning and exact relation verification.
- Shape relation runtime lane and stats.

### Milestone 4: Reverse lookup and address hierarchy — PARTIALLY DELIVERED

- Delivered: address-first nearest-point reverse geocoding, interpolation,
  structured address components, accuracy/result filters, locality fallback,
  and geographic shard routing through `reverseGeocodeOsm()`.
- Remaining: containing-area hierarchy and shape-tree relations.

### Milestone 5: OSM ranking and quality pass — DELIVERED, BENCHMARKS ONGOING

- Delivered: generic static `rankPrior`, OSM prominence, multilingual category
  lexicon/intents, ambiguity/locality regression cases, and production phone
  workload benchmarks.
- Remaining from the original plan: a direct Lucene comparison bench for box,
  radius, nearest, and distance boost.

## Risks

- Address search quality needs hierarchy and interpolation logic; fixture
  expectations must distinguish OSM-only data from optional external
  house-number enrichment.
- Polygon support can add large geometry payloads. Keep point and shape
  packs split so point-only queries do not pay for geometry.
- Distance boost complicates exact top-k proofs for text queries. Exact
  filtering stays separate from approximate boost ranking (already enforced:
  the boost is a visible page-window rerank).
- Very wide geo constraints on text queries fall back to per-document
  doc-value verification; posting-block lat/lon summaries or spatial doc
  ordering are the known format-level follow-ups
  (`docs/osm-geo-benchmarks.md`, Known limitations).

## Source References

- Lucene `LatLonPoint`: point indexing, bbox, distance, polygon, geometry,
  nearest, and distance-feature query APIs.
  https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/document/LatLonPoint.html
- Lucene `LatLonDocValuesField`: distance sort and doc-value pairing guidance.
  https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/document/LatLonDocValuesField.html
- Lucene `PointValues`: KD-tree-backed point values for range, distance,
  nearest-neighbor, and point-in-polygon query families.
  https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/index/PointValues.html
- Lucene BKD package: block KD-tree implementation notes.
  https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/util/bkd/package-summary.html
- Procopiuc et al., "Bkd-Tree: A Dynamic Scalable kd-Tree".
  https://users.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf
- Nominatim search API, indexing, database layout, and ranking docs.
  https://nominatim.org/release-docs/latest/api/Search/
  https://nominatim.org/release-docs/latest/develop/Indexing/
  https://nominatim.org/release-docs/latest/develop/Database-Layout/
  https://nominatim.org/release-docs/latest/customize/Importance/
- Palacio, Derungs, and Purves, "Development and evaluation of a geographic
  information retrieval system using fine grained toponyms".
  https://josis.org/index.php/josis/article/view/61
