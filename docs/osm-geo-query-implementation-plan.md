# OSM Geo Query Implementation Plan

## Goal

Add full-featured geo query support to Rangefind so a static index can search
OpenStreetMap-derived place data by text, address/place semantics, category
metadata, point geometry, area geometry, and geographic constraints without a
search server. The design is shard-first for geo-enabled builds: every OSM/geo
build emits a global shard directory plus one or more shard-local Rangefind
indexes. A city extract may produce one shard; a country or full-planet build
produces many independently cacheable shards.

The target is not an incremental numeric-filter feature. The target is a
range-addressed and shard-addressed geospatial subsystem that gives Rangefind
the same class of capability Lucene gets from `LatLonPoint`,
`LatLonDocValuesField`, and `LatLonShape`: bounding-box queries, radius queries,
nearest-neighbor result pages, polygon and shape relations, reverse lookup,
address-aware place ranking, and distance-aware ranking.

## Current Rangefind Fit

Rangefind already has several pieces that should be reused:

- Text scoring uses weighted schema fields and BM25F-style impact posting lists.
- Authority sidecars can rescue exact canonical labels such as place names and
  aliases.
- Facets support OSM tags like `amenity`, `shop`, `tourism`, `place`, country,
  administrative level, and source class/type.
- Numeric/date/boolean doc-values already support range filtering, sorted
  browsing, and page-level min/max pruning.
- Filter bitmaps and posting-block summaries can short-circuit common metadata
  filters before document payload hydration.
- Query bundles can carry small, high-confidence top-k result sets with embedded
  filter summaries.

The missing primitive is a two-dimensional point tree where latitude and
longitude are pruned together. Independent `lat` and `lon` numeric pages can
prune a rough rectangle, but they cannot efficiently prove distance, polygon, or
nearest-neighbor results over large OSM extracts.

## Compatibility With Non-Geo Indexes

This plan must not force the OSM/geo layout onto existing non-geo collections
such as the Wikipedia/frwiki fixture. Geo support is an additive feature path.

Compatibility rules:

- A config with no `geo` and no `sharding` keeps the current Rangefind output
  shape: root `manifest.json`, term directories, doc packs, doc-values, facets,
  authority, typo sidecars, and query bundles as they work today.
- Non-geo unsharded builds do not emit `manifest.shards`, `shard-packs/`,
  `global/`, or `geo/` files.
- Non-geo sharded builds may emit `manifest.shards`, `shard-packs/`, and
  `global/`, but they must not emit `geo/`, point trees, shape trees, or
  geo-specific stats.
- The runtime dispatch is feature-driven:
  - If `manifest.shards` is absent and the query has no `geo`, use the current
    text/filter/sort path.
  - If `manifest.shards` is absent and the query has `geo`, fail clearly with an
    unsupported-feature error.
  - If `manifest.shards` is present, use the deployment-shard planner before
    opening shard-local manifests.
- The word "shard" already appears in the current text/authority/typo term
  directories. Geo planning should use names like `deploymentShard`,
  `geoShard`, or `shardDirectory` internally so the new planet-scale shard layer
  is not confused with existing term shards.
- The Wikipedia/frwiki fixture remains a regression gate. Its existing build and
  runtime benchmarks should continue to pass without producing geo artifacts,
  and its request/transfer stats should not regress just because geo support
  exists in the runtime bundle.

Deployment sharding should be generic from the start; geo is only one routing
strategy. The OSM plan should not make Wikipedia-style indexes pay for geo
routing, point trees, or shape trees, but the same root shard directory and
shard-local index layout can support very large text-only corpora.

## Generic Non-Geo Sharding

Non-geo sharding is compatible with Rangefind when it is modeled as deployment
partitioning, not as geo partitioning.

Supported non-geo shard strategies:

- `none`: the current single-index layout, used by existing basic and frwiki
  fixtures unless explicitly changed.
- `docIdRange`: deterministic contiguous document ranges. Good for large browse,
  filter, and sort workloads.
- `hash`: stable hash of external id. Good for balanced shard sizes and
  incremental/generational layouts.
- `fieldPrefix`: partition by a configured field such as title prefix, language,
  namespace, tenant, or collection id.
- `termRouted`: shard-local text indexes plus a global term-to-shard sketch or
  directory so text queries can avoid opening shards that cannot match.

For non-geo sharded indexes, each deployment shard contains the same logical
pieces Rangefind already emits today: terms, docs, doc-values, facets,
authority, typo, query bundles, and dense doc pages. The root manifest adds only
the shard directory, global scoring stats, shard summaries, and optional routing
sketches.

Non-geo shard directory records should contain:

- `deploymentShardId` and build hash.
- Shard manifest pointer.
- Document count and doc-id or external-id range.
- Field length totals and global BM25 inputs.
- Optional facet/filter summaries for shard pruning.
- Optional sort min/max summaries for sorted browse pruning.
- Optional term sketches, Bloom filters, or exact term-to-shard directory pages.
- Optional authority prefix summaries for exact title/entity lookups.

Runtime behavior:

- Exact text search must either use an exact term-to-shard directory or visit
  every shard that might contain a query term.
- If routing relies on approximate sketches, the runtime may over-fetch false
  positive shards but must not skip possible exact matches in exact mode.
- Auto mode may cap shard fanout for latency, but then it must expose
  `shardExact: false` and provide a continuation cursor.
- Cross-shard BM25 must use global document counts, document frequencies, and
  field length statistics where possible so shard-local scores are comparable.
- Cross-shard sort merges shard-local sorted pages using the same heap/upper
  bound approach as geo distance merge, but with field values instead of
  distances.

## Product Scope

The supported feature set should include:

- Text plus radius: `q="bakery"` near a point within `N` meters.
- Text plus bounding box: find places matching text/category inside a viewport.
- Category-only geo browse: `amenity=pharmacy` inside a radius or viewport.
- Distance sort: nearest places to a point, with optional text/category filters.
- Distance boost: combine text relevance and distance decay.
- OSM ranking: prefer more important, addressable, or better-scoped places when
  text and distance are otherwise close.
- Polygon filters over point places.
- Indexed area/line shapes for administrative boundaries, streets, parks,
  buildings, and other OSM ways/relations.
- Shape-vs-shape relation queries aligned with Lucene relations where exactness
  is practical: `INTERSECTS`, `WITHIN`, `CONTAINS`, and `DISJOINT`.
- Reverse lookup: find the nearest or containing place/address hierarchy for a
  coordinate.
- Address-aware search with inherited/attached address parts for POIs, house
  numbers, streets, localities, cities, regions, and countries.
- Planet-scale static deployment through a small global shard directory, spatial
  shard routing, shard-local point/text/shape indexes, and cross-shard top-k
  merge.
- Optional external enrichment hooks for TIGER/OpenAddresses-style house number
  data, without making those datasets mandatory for the core OSM path.

Out of scope for this engine layer: routing, turn-by-turn navigation, and
network-distance search. Those require graph semantics rather than a document
search index.

## Query API

Proposed runtime shape:

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
    facets: { osmType: ["node", "way"], category: ["shop"] }
  },
  shards: {
    mode: "auto",
    maxFanout: 16
  },
  size: 10
});
```

`shards` is optional. The default mode routes through the shard directory
automatically. It exists so tests and advanced hosts can cap fanout, force exact
multi-shard execution, or continue a paged search with an opaque shard cursor.

Equivalent structured forms:

```js
geo: {
  field: "location",
  box: {
    minLat: 45.45,
    maxLat: 45.56,
    minLon: -73.70,
    maxLon: -73.50
  }
}
```

```js
geo: {
  field: "location",
  polygon: [[45.52, -73.62], [45.53, -73.55], [45.48, -73.56]]
}
```

Shape relation form:

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

Reverse lookup form:

```js
const reverse = await engine.reverse({
  field: "location",
  lat: 45.5017,
  lon: -73.5673,
  layers: ["address", "poi", "street", "locality", "admin"],
  size: 10
});
```

Response additions:

```js
{
  results: [
    {
      id: "node/123",
      title: "Bakery Name",
      distanceMeters: 418.2,
      score: 12.4
    }
  ],
  stats: {
    plannerLane: "geoText",
    geoCandidateLeaves: 8,
    geoLeavesVisited: 3,
    geoPointsScanned: 421,
    geoPointsAccepted: 17,
    geoDistanceSorted: true,
    geoShapeRelation: "",
    reverseHierarchy: false,
    shardCandidates: 5,
    shardsVisited: 2,
    shardDirectoryPagesFetched: 1,
    shardExact: true,
    shardCursor: ""
  }
}
```

## OSM Input Model

Rangefind should keep JSONL as the build input and add a reusable OSM fixture
builder rather than making OSM parsing part of the core runtime.

Recommended normalized JSONL fields:

```json
{
  "id": "node/123",
  "osmType": "node",
  "osmId": 123,
  "url": "https://www.openstreetmap.org/node/123",
  "name": "Bakery Name",
  "aliases": ["Boulangerie Name", "old name"],
  "displayName": "Bakery Name, Montreal, Quebec, Canada",
  "body": "bakery pastry coffee ...",
  "category": "shop",
  "type": "bakery",
  "tags": ["shop=bakery", "cuisine=pastry"],
  "countryCode": "ca",
  "adminLevel": 8,
  "placeRank": 30,
  "addressRank": 30,
  "importance": 0.00001,
  "population": null,
  "lat": 45.5017,
  "lon": -73.5673,
  "bbox": [45.5016, -73.5675, 45.5018, -73.5671],
  "shardHint": "ca/qc/montreal"
}
```

Core OSM extraction rules:

- Keep named places and searchable POIs: `place`, `amenity`, `shop`, `tourism`,
  `leisure`, `historic`, `office`, `craft`, `healthcare`, `building` with
  useful address/name tags, and admin boundaries.
- Preserve all name variants: `name`, `name:*`, `alt_name`, `old_name`,
  `short_name`, `brand`, `operator`, `ref`.
- Generate text fields from name, aliases, category/type labels, address parts,
  and selected tags.
- Generate authority fields from canonical name, aliases, brand/operator, and
  address display strings.
- Keep principal OSM class/type facets separately from display labels.
- Keep raw OSM IDs stable; do not expose Nominatim internal `place_id` semantics.
- Treat `shardHint` as optional builder input. The builder may ignore it, refine
  it, or use it as a coarse seed for deterministic spatial sharding.

Nominatim should be used as the reference model for which concepts matter:
place rank, address rank, importance, address hierarchy, tokenizer behavior,
and the difference between independent places and dependent rank-30 POIs.

## Schema Additions

Add `geo` and `sharding` to geo-enabled `rangefind.config.json` files:

```json
{
  "geo": [
    {
      "name": "location",
      "latPath": "lat",
      "lonPath": "lon",
      "bboxPath": "bbox",
      "sort": true,
      "boost": true
    }
  ],
  "sharding": {
    "strategy": "geoAdaptive",
    "field": "location",
    "targetDocsPerShard": 250000,
    "maxGeometryBytesPerShard": 134217728,
    "replicateCoveringShapes": true,
    "globalTextDirectory": true,
    "defaultMaxFanout": 16,
    "maxExactFanout": 128
  }
}
```

`sharding` is part of the initial geo model, not a later scale retrofit. A
small index should still flow through the same code path with a single shard.
That keeps planner, scoring, and tests honest before country or planet-sized
data is attempted.

For a non-geo sharded index, the config should omit `geo` and use a non-geo
strategy:

```json
{
  "sharding": {
    "strategy": "termRouted",
    "targetDocsPerShard": 250000,
    "globalTextDirectory": true,
    "defaultMaxFanout": 32,
    "maxExactFanout": 256
  }
}
```

That emits deployment shards and global routing/scoring metadata, but no point
tree, shape tree, or `geo/` sidecar.

Recommended OSM example config:

```json
{
  "fields": [
    { "name": "name", "path": "name", "weight": 6.0, "b": 0.4, "phrase": true },
    { "name": "aliases", "path": "aliases", "weight": 4.0, "b": 0.5 },
    { "name": "body", "path": "body", "weight": 1.0, "b": 0.75 }
  ],
  "authority": [
    { "name": "name", "path": "name" },
    { "name": "aliases", "path": "aliases" },
    { "name": "displayName", "path": "displayName" }
  ],
  "facets": [
    { "name": "category", "path": "category" },
    { "name": "type", "path": "type" },
    { "name": "countryCode", "path": "countryCode" },
    { "name": "tags", "path": "tags" }
  ],
  "numbers": [
    { "name": "importance", "path": "importance", "type": "double" },
    { "name": "placeRank", "path": "placeRank", "type": "int" },
    { "name": "addressRank", "path": "addressRank", "type": "int" }
  ],
  "geo": [
    { "name": "location", "latPath": "lat", "lonPath": "lon", "bboxPath": "bbox" }
  ],
  "sharding": {
    "strategy": "geoAdaptive",
    "field": "location",
    "targetDocsPerShard": 250000,
    "maxGeometryBytesPerShard": 134217728,
    "replicateCoveringShapes": true,
    "globalTextDirectory": true,
    "defaultMaxFanout": 16,
    "maxExactFanout": 128
  }
}
```

## Shard-First Architecture

The first runtime decision for an OSM geo query is shard selection. The geo tree
and shape tree are local indexes inside each shard; the global layer is a small
routing and scoring layer that tells the runtime which shard-local indexes are
worth fetching.

Core rules:

- Every OSM build emits `manifest.shards`, even when there is only one shard.
- Shards are immutable, content-addressed deployment units with local doc ids,
  local text postings, local doc-values, local facets, local authority sidecars,
  local point trees, and optional local shape trees.
- The global manifest owns stable external ids, shard ids, shard bboxes, shard
  object pointers, global scoring statistics, and compact shard summaries.
- Point documents belong to exactly one home shard. Large lines and polygons
  have one home shard plus lightweight covering references in every shard whose
  bbox they intersect. Result merging deduplicates by stable external id.
- Very large administrative boundaries may live in a dedicated global/admin
  shard if replicating their geometry would dominate ordinary POI shards.
- Shards are sized by both document count and geometry bytes. A dense city core
  should split sooner than a sparse rural region, even if the geographic area is
  small.
- The sharding key should be spatial and deterministic. Start with an adaptive
  quadtree/geohash-like cell id; keep the implementation internal so a later S2
  or H3 mapping can be evaluated without changing the public query API.

Shard directory records should contain:

- `shardId` and generation/build hash.
- Geographic bbox and optional covering cells.
- Pointers to the shard manifest and hot shard metadata.
- Document count, point count, shape count, geometry byte count.
- Min/max `placeRank`, `addressRank`, and `importance`.
- Compact facet summaries for high-value filters such as country, category,
  type, admin level, and OSM class/type.
- Term sketches or Bloom filters for common text routing.
- Authority/name prefix summaries for exact place-name routing.
- Per-shard global BM25 inputs: document count, field length totals, and any
  normalized statistics required to make shard-local scores comparable.

Shard planning:

1. Convert radius, bbox, or polygon constraints into a shard covering set.
2. Intersect that set with metadata filter summaries when filters exist.
3. Use term sketches, authority summaries, and query-bundle hints to avoid
   fetching text shards that cannot match.
4. Rank candidate shards by distance-to-shard, overlap, term/name evidence,
   importance bounds, and filter selectivity.
5. Execute shard-local lanes in the highest-value shards first.
6. Merge shard-local top-k results by exact score or distance.
7. Stop early only when shard upper bounds prove that unvisited shards cannot
   beat the current boundary. Otherwise return `shardExact: false` or a
   `shardCursor` for continuation, depending on the requested mode.

Exactness expectations:

- Geo filters are exact only over the shards actually searched.
- `mode: "exact"` must visit every candidate shard or fail with a clear fanout
  error when the configured budget is exceeded.
- `mode: "auto"` may use bounded fanout for interactive latency, but it must
  expose `shardExact: false` and a continuation cursor when not all candidates
  were visited.
- `q=""` plus radius/distance sort can be exact with shard upper-bound proofs.
- Text plus geo ranking should be exact only when the global shard scorer has
  enough term and distance upper bounds to stop safely.

## Format Direction

### Full `rfgeo-v1`

Build the full format around a global shard directory and two sibling geo
indexes inside each shard:

- A point index for centroid and address/POI coordinates.
- A shape index for OSM lines and polygons that need spatial relation queries.

The point index is mandatory for any configured `geo` field. The shape index is
enabled when a `shapePath`, `bboxPath`, or OSM geometry source is configured.
The existing numeric doc-value path may be kept only as a builder/runtime
diagnostic and exhaustive correctness oracle; it is not the product lane.

Range-packed output:

```text
rangefind/
  manifest.json
  shards/
    directory-root.<hash>.bin.gz
    directory-pages/
      0000.<hash>.bin.gz
    manifests/
      0000.<hash>.json.gz
  global/
    text-sketch-root.<hash>.bin.gz
    authority-sketch-root.<hash>.bin.gz
    scoring-stats.<hash>.json.gz
  shard-packs/
    ca-qc-montreal/
      manifest.<hash>.json.gz
      terms/
        directory-root.<hash>.bin.gz
        directory-pages/
          0000.<hash>.bin.gz
        block-packs/
          0000.<hash>.bin
        packs/
          0000.<hash>.bin
      docs/
        ordinals/
          0000.<hash>.bin
        pointers/
          0000.<hash>.bin
        pages/
          0000.<hash>.bin
        page-packs/
          0000.<hash>.bin
        packs/
          0000.<hash>.bin
      doc-values/
        packs/
          0000.<hash>.bin
        sorted/
          importance.<hash>.bin.gz
        sorted-packs/
          0000.<hash>.bin
      facets/
        directory-root.<hash>.bin.gz
        directory-pages/
          0000.<hash>.bin.gz
        packs/
          0000.<hash>.bin
      authority/
        directory-root.<hash>.bin.gz
        directory-pages/
          0000.<hash>.bin.gz
        packs/
          0000.<hash>.bin
      geo/
        location/
          tree-root.<hash>.bin.gz
          tree-pages/
            0000.<hash>.bin.gz
          point-packs/
            0000.<hash>.bin
        shape/
          tree-root.<hash>.bin.gz
          tree-pages/
            0000.<hash>.bin.gz
          shape-packs/
            0000.<hash>.bin
          geometry-packs/
            0000.<hash>.bin
```

The legacy-looking single-index layout is still valid as the contents of one
shard. The root `manifest.json` should stay small: it points at the shard
directory, global scoring metadata, and feature flags. It should not carry
planet-wide terms, docs, geometry, or full facet dictionaries.

One-shard regional output is just a degenerate form:

```text
rangefind/
  manifest.json
  shards/
    directory-root.<hash>.bin.gz
  shard-packs/
    default/
      manifest.<hash>.json.gz
      terms/
      docs/
      doc-values/
      facets/
      authority/
      geo/
```

Point leaf page contents:

- Tree cell bbox: `minLatE7`, `maxLatE7`, `minLonE7`, `maxLonE7`.
- Point count.
- Packed rows sorted by a spatial order inside each leaf.
- Rows contain `latE7`, `lonE7`, `docId`.
- Per-leaf summaries for configured filter fields.
- `maxImportance`, `minPlaceRank`, and `minAddressRank` ranking bounds.

Point internal page contents:

- Child cell bboxes.
- Child pointer records using the existing object pointer shape.
- Child point-count summaries.
- Filter, rank, and importance summaries.

Shape leaf page contents:

- Shape bbox.
- Tessellated triangle or compact geometry reference rows.
- `docId`, geometry type, relation flags, and pointer to exact geometry bytes
  when needed for final verification.
- Optional simplified geometry for fast rejection.

Shape internal page contents:

- Child shape bboxes.
- Child geometry type summaries.
- Child relation/filter/rank summaries.

Encoding:

- Store latitude and longitude as Lucene-like fixed precision integers.
- Use signed fixed-width or delta encoding within leaf pages.
- Keep doc ids as fixed-width or delta-coded integers.
- Compress each page as an independent gzip member and pack it into
  content-addressed range packs.
- Store exact geometry in GeoJSON-compatible coordinate order internally as
  encoded lon/lat integer rings or line strings. API input/output can stay
  GeoJSON-compatible, while internal helpers can expose lat/lon convenience
  forms.

The builder constructs each shard-local point tree as a static bulk-loaded
kd-tree/BKD-like tree:

1. Stream documents and write valid `(lat, lon, docId)` records to a temp spool.
2. Assign each point to a deterministic home shard.
3. Sort or partition by alternating latitude/longitude splits until leaf target.
4. Emit leaf pages and then internal tree pages bottom-up for each shard.
5. Attach metadata to the shard manifest and aggregate bounds into the global
   shard directory.

The builder constructs each shard-local shape tree separately:

1. Stream OSM geometries from normalized JSONL or the OSM fixture builder.
2. Normalize polygons, multipolygons, and lines into validated internal geometry.
3. Compute bbox, centroid, simplification, and tessellation records.
4. Assign a home shard and compute covering shard references for spanning
   geometry.
5. Bulk-load shape cells by bbox/centroid with page summaries.
6. Store exact geometry bytes in geometry packs only when exact relation
   verification needs them.
7. Deduplicate replicated covering results with stable external ids.

The global shard directory is emitted after shard-local indexes are built:

1. Read shard manifests and collect bboxes, counts, filter summaries, rank
   bounds, term sketches, authority sketches, and global scoring stats.
2. Build a compact spatial directory that can route radius, bbox, polygon, and
   reverse queries before any shard manifest is fetched.
3. Build optional text and authority routing sketches so text-only and ambiguous
   name queries do not fan out to every shard.
4. Write a small root manifest that references only global routing/scoring
   metadata and lazy shard pointers.

## Runtime Planning

The runtime should add a shard planner plus five geo lanes. Every geo lane
starts with shard planning unless the host explicitly opens a known shard.

`shardPlan`:

- Loads the shard directory lazily on the first OSM geo or sharded text query.
- Resolves radius, bbox, polygon, reverse, or text-only routing constraints into
  candidate shard ids.
- Applies shard-level filter summaries before opening shard-local manifests.
- Orders shards by estimated value and estimated transfer cost.
- Tracks fanout, exactness, and continuation cursor state.
- Provides cross-shard score and distance upper bounds to the lane-specific
  executor.

`geoBrowse`:

- No text query.
- Runs inside each selected shard.
- Traverses shard-local geo tree cells ordered by distance-to-cell for
  nearest/radius, or bbox intersection for viewport.
- Fetches only candidate leaf pages.
- Maintains a shard-local top-k heap by distance, or doc order for unsorted
  browse.
- Merges shard-local results with global distance upper-bound checks.

`geoText`:

- Text query with geo filter or geo boost.
- Uses shard routing before either text or geo work starts.
- Uses shard-local geo tree to produce candidate docs when geo selectivity is
  high.
- Uses shard-local text posting scheduler first when text selectivity is high.
- For combined queries, choose the cheaper first phase from estimates:
  text posting block upper bounds, geo cell point counts, query radius/viewport,
  active metadata filters, and shard-level term/filter summaries.

`geoShape`:

- Handles polygon, line, and bbox geometry relations.
- Uses shard routing to select only shards intersecting the query geometry.
- Uses shard-local shape tree bboxes and simplified geometry to prune cells.
- Fetches exact geometry only for candidate shapes that cannot be proven by page
  summaries.
- Supports `INTERSECTS`, `WITHIN`, `CONTAINS`, and `DISJOINT` when exact
  verification is available for the configured geometry type.

`geoReverse`:

- Starts from the shard directory to find the nearest intersecting shards.
- Starts from shard-local point trees for nearest POI/address candidates.
- Uses shard-local shape trees to find containing admin/locality/street/building
  candidates.
- Returns a ranked hierarchy with layer/type metadata instead of pretending
  reverse geocoding is a normal text search.

`geoExactScan`:

- Diagnostic-only exhaustive lane for tests and benchmarks.
- May use numeric doc-values or decoded geometry packs to verify tree results.
- May run against one shard, all candidate shards, or all shards depending on
  the test.
- Should not be selected in production search unless explicitly requested with a
  debug/exact flag.

Required exactness rules:

- Bbox filters are exact after comparing encoded lat/lon values.
- Radius filters use bbox as a coarse prune and Haversine as final verification.
- Distance sort must return exact top-k by distance for `q=""` once the geo tree
  and shard directory prove no unvisited cell or shard can beat the boundary
  distance.
- Polygon filters over points are exact after point-in-polygon verification.
- Shape relation queries are exact only when exact geometry verification is
  available. Unsupported relation/geometry combinations must fail clearly rather
  than silently returning approximate results.
- Reverse lookup must distinguish nearest point matches from containing area
  matches in the response.
- Text plus distance boost may be approximate only if marked as approximate in
  stats; the preferred v1 should keep exact filtering and clearly separate
  distance boost from exact text top-k proof.
- Cross-shard result merging must use stable external ids and deterministic
  tie-breaking so replicated shapes cannot duplicate results.

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

Distance boost should follow Lucene's distance feature shape:

```text
distanceBoost = weight * pivotMeters / (pivotMeters + distanceMeters)
```

OSM ranking guidance:

- `importance` is a strong tie-breaker for well-known places.
- Lower `placeRank` usually means larger or more globally significant features.
- `addressRank` helps distinguish addressable places from non-address parts.
- For typed category queries, exact OSM `class/type` or special phrase matches
  should beat vague body-text matches.
- For local POI queries, distance should matter more than global importance
  after a reasonable category/text match is established.
- Shard-local text scores must be computed with global scoring statistics where
  possible. If a query falls back to shard-local statistics, the response should
  expose that in stats because cross-shard ordering becomes less comparable.

## Lucene Comparison Bench

Extend the existing Lucene quality bench with geo fields:

- Add `LatLonPoint("location", lat, lon)` for indexed geo queries.
- Add `LatLonDocValuesField("location", lat, lon)` for distance sort.
- Add stored `lat`, `lon`, `category`, `type`, and `importance`.
- Compare `LatLonPoint.newBoxQuery`.
- Compare `LatLonPoint.newDistanceQuery`.
- Compare `LatLonPoint.nearest`.
- Compare `LatLonPoint.newDistanceFeatureQuery` for distance boost.

Add a new script:

```text
scripts/osm_geo_lucene_quality.mjs
scripts/lucene_quality/src/main/java/rangefind/bench/LuceneOsmGeoQuality.java
```

Bench report metrics:

- Hit@1/Hit@3/Hit@10 and MRR@10 for known place-name queries.
- Geo filter correctness against exhaustive Haversine checks.
- Nearest-neighbor top-k agreement with Lucene.
- Cold request count and transfer KB per query lane.
- Leaf pages visited, points scanned, points accepted.
- Text-first versus geo-first planner choice.
- Shard candidates, shards visited, shard exactness, and cross-shard merge
  cutoff behavior.
- One-shard versus multi-shard agreement against the same exhaustive oracle.

Starter judgments:

- Known named places in a regional extract.
- Category near point, e.g. `bakery near downtown Montreal`.
- Viewport query, e.g. `pharmacy in visible map box`.
- Ambiguous place names, e.g. `Springfield`, `Paris`, `London`.
- Address-like queries if the fixture has useful `addr:*` coverage.

## Tests

Unit tests:

- Shard id assignment, shard bbox union, and deterministic split decisions.
- Shard directory encode/decode round trips.
- Shard candidate selection for radius, bbox, polygon, reverse, and text-only
  routing.
- Cross-shard top-k merge, deduplication, exactness flags, and cursor behavior.
- Coordinate validation and integer encoding bounds.
- Haversine distance and bbox-from-radius calculations.
- Dateline-crossing bbox handling.
- Geo tree page encode/decode round trips.
- Leaf page exact filtering for bbox and radius.
- Polygon winding, closure, bbox, simplification, and point-in-polygon checks.
- Shape relation verification for supported geometry pairs.
- Distance top-k heap ordering and tie-breaking.
- Reverse hierarchy ranking and layer/type ordering.
- Planner estimate decisions for text-first and geo-first paths.

Build/runtime integration tests:

- Extend `test/build-runtime.test.js` with a small place fixture.
- Add a non-geo compatibility assertion using the basic or frwiki-style fixture:
  no `geo`, no `sharding`, no `manifest.shards`, no `shard-packs/`, and the
  existing text/filter/sort runtime path still answers queries.
- Add a non-geo sharded assertion using the same fixture: no `geo`, yes
  `manifest.shards`, yes `shard-packs/`, no shard-local `geo/`, and exact-mode
  text/filter/sort results match the unsharded build.
- Validate `manifest.shards`, shard directory pages, shard manifests, and
  shard-local `geo/` packs.
- Build both one-shard and forced multi-shard versions of the same fixture.
- Query `q=""` with radius and distance sort.
- Query text plus radius.
- Query category-only plus bbox.
- Query polygon filters over point places.
- Query shape relations over polygons/lines.
- Query reverse lookup at a coordinate with containing admin/locality results.
- Assert exact agreement against an in-test exhaustive implementation.
- Assert one-shard and multi-shard outputs agree for exact-mode queries.
- Assert bounded fanout returns `shardExact: false` plus a cursor when it stops
  before all candidate shards.
- Assert lazy fetch behavior: no shard-local geo manifest until a geo query is
  requested.

Benchmark tests:

- Add OSM fixture smoke command with a small checked-in JSONL sample.
- Add an optional large regional extract path for local benchmarking.
- Record cold request and transfer counters as in the frwiki fixture.
- Compare unsharded frwiki and non-geo sharded frwiki for correctness, request
  count, transfer, and score ordering.
- Compare point, shape, reverse, and text-plus-geo lanes against Lucene and
  exhaustive checks.
- Compare one-shard, forced multi-shard, and adaptive multi-shard builds for the
  same extract.

## Milestones

### Milestone 1: Shard Directory, OSM Fixture, Schema, and Exhaustive Oracle

Deliverables:

- `scripts/osm_fixture.mjs` that converts regional OSM-derived JSONL into a
  Rangefind example site and benchmark fixture.
- A checked-in normalized mini OSM fixture with points, lines, polygons,
  ambiguous names, rank metadata, address metadata, and category metadata.
- Config support for `geo` fields.
- Config support for `sharding`.
- Config defaults that leave `geo` and `sharding` disabled for non-geo indexes.
- Non-geo sharding strategies for `docIdRange`, `hash`, `fieldPrefix`, and
  `termRouted`.
- Config support for optional shape geometry fields.
- `src/shard_directory.js` codec and builder helpers.
- Single-shard and forced multi-shard fixture builds using the same input.
- Shard assignment helpers for point home shards and shape covering shards.
- Global shard directory output with shard bboxes, counts, filter summaries,
  rank bounds, term sketches, authority sketches, and scoring stats.
- Runtime shard planner that can select candidate shards before opening a shard
  manifest.
- Exhaustive debug/oracle implementation for point, radius, bbox, polygon,
  shape relation, reverse, and cross-shard merge checks.
- OSM import normalization helpers for names, aliases, address parts,
  class/type facets, ranks, importance, centroids, bboxes, and geometry.

Acceptance:

- Small fixture builds with `npm run test:all`.
- Existing non-geo fixtures still build and query without geo files or shard
  directories.
- A non-geo sharded fixture builds with deployment shards but no geo sidecars.
- Non-geo sharded exact-mode text/filter/sort results match the equivalent
  unsharded fixture.
- The fixture can validate full geo behavior even before optimized tree
  traversal is wired.
- Exhaustive checks establish exact expected results for all supported query
  types.
- Single-shard and forced multi-shard exact-mode queries return the same
  results.
- The root manifest stays small and loads shard metadata lazily.
- Bounded fanout exposes `shardExact: false` and a continuation cursor.

### Milestone 2: Static Point Tree Format

Deliverables:

- `src/geo_tree.js` codec and builder helpers.
- Shard-local `manifest.geo` lazy sidecar.
- Range-packed `shard-packs/<shard-id>/geo/<field>/point-packs`.
- Runtime tree traversal for bbox and radius filters.
- Tree-based distance sort for `q=""`.
- Nearest-neighbor browse and distance-bound top-k proof.
- Dateline-aware query planning.
- Cross-shard nearest-neighbor merge with shard distance upper-bound proof.

Acceptance:

- Geo tree results match exhaustive checks.
- Radius queries fetch only relevant tree/leaf pages on selective queries.
- Distance sort stops once unvisited cells and unvisited candidate shards cannot
  beat the top-k boundary.

### Milestone 3: Static Shape Tree Format

Deliverables:

- `src/geo_shape.js` codec and builder helpers.
- Shard-local shape tree pages, shape packs, and geometry packs.
- Polygon filter support over point places.
- Indexed line/polygon bbox pruning.
- Exact geometry verification for supported relation queries.
- Shape relation runtime lane and stats.
- Covering-shard references for spanning ways and relations.

Acceptance:

- Polygon and shape relation results match exhaustive checks.
- Exact geometry payloads are fetched only for unresolved candidates.
- Point-only indexes are not forced to emit shape packs.
- Replicated covering records deduplicate to one result per stable external id.

### Milestone 4: Text Plus Geo Planner

Deliverables:

- Combined text/geo planner.
- Shard-first, geo-first, and text-first execution lanes.
- Metadata filter summaries on geo leaves where useful.
- Distance boost with explicit stats.
- Authority/name ranking tuned for OSM places.
- Shape-aware filtering inside text searches.
- Cross-shard result merge using global scoring statistics.
- Exact-mode fanout errors and auto-mode continuation cursor.

Acceptance:

- Text plus radius works without scanning all text hits or all geo points.
- Text plus polygon/shape relation works with exact filtering.
- Planner stats explain selected lane and candidate counts.
- Lucene comparison bench covers box, radius, nearest, and distance boost.
- One-shard and forced multi-shard text-plus-geo exact results agree.

### Milestone 5: Reverse Lookup and Address Hierarchy

Deliverables:

- Reverse lookup API.
- Shard-routed reverse lookup for nearest and containing features.
- Containing-area and nearest-point hierarchy ranking.
- Independent/dependent place modeling inspired by Nominatim.
- Address inheritance/attachment for POIs and house numbers where data exists.
- Layer-specific response shape for address, POI, street, locality, admin, and
  country levels.

Acceptance:

- Reverse results distinguish nearest features from containing areas.
- Reverse lookup searches neighboring shards until distance and containment
  bounds prove no better candidate remains.
- Address-like place results use hierarchy metadata rather than raw text only.
- Tests cover rank-30 POIs, streets, localities, and admin boundaries.

### Milestone 6: OSM Ranking and Quality Pass

Deliverables:

- OSM rank/importance score integration.
- Category/special phrase mapping for common OSM tags.
- Ambiguous-name test set.
- Regional benchmark report.
- Optional external enrichment hook for house-number datasets.

Acceptance:

- Known-name and category-near judgments have comparable top-k behavior to the
  Lucene baseline.
- Ambiguous names prefer nearby results when a location context is supplied.
- Globally important places still win when no local context is supplied.

### Milestone 7: Planet-Scale Shard Hardening

Deliverables:

- Large regional extract benchmark.
- Country-scale adaptive shard benchmark.
- Synthetic planet-scale shard directory stress test, even before a full planet
  build is practical locally.
- Public example site for OSM place search.
- Browser transfer/request budget report for every geo lane and shard fanout
  mode.
- Format docs in `docs/architecture.md`.
- User-facing README docs for OSM place search configuration.
- Lucene comparison report committed as a reproducible fixture output or
  documented local command.

Acceptance:

- Full geo test suite passes through `npm run test:all`.
- Large regional extract demonstrates bounded transfer for selective geo
  queries.
- Country-scale benchmark demonstrates that query cost scales with touched
  shards rather than total indexed OSM size.
- Synthetic planet-scale directory benchmark keeps root manifest and first
  routing fetch small enough for browser startup.
- Shape, reverse, text-plus-geo, and distance-sort lanes expose actionable
  runtime stats.

## Risks

- OSM full-planet data is too large for a single browser static index. The
  initial architecture must be shard-first, with a small global directory and
  shard-local indexes. Regional extracts are still the first validation target,
  but they must use the same sharded code path.
- Address search quality needs hierarchy and interpolation logic. The full
  feature plan includes hierarchy modeling, but fixture expectations must
  distinguish OSM-only data from optional external house-number enrichment.
- Independent `lat` and `lon` numeric filters can look acceptable on small data
  but degrade badly on large extracts. Keep them as a debug oracle only.
- Distance boost complicates exact top-k proof for text queries. Keep exact
  filtering separate from approximate ranking until proofs are implemented.
- Polygon support can add large geometry payloads. Split point and shape packs so
  point-only queries do not pay for full geometry.
- Cross-shard scoring can become inconsistent if shards compute BM25 statistics
  independently. Emit global scoring stats during the build and make fallback
  local scoring visible in runtime stats.
- Non-geo sharded indexes need exact term routing or conservative over-fetching
  to preserve recall. Approximate sketches are acceptable only when they create
  false positives, or when the response is explicitly marked non-exact.
- Replicated shape references can duplicate results or inflate transfer if they
  carry exact geometry into every covering shard. Replicate lightweight covering
  records, keep one home geometry payload when possible, and deduplicate by
  stable external id.
- Naming can become confusing because Rangefind already has term, authority, and
  typo shards. Keep planet-scale routing concepts named as deployment/geo shards
  in code, manifests, and stats.

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
- Nominatim search API: structured/free-form place search and output controls.
  https://nominatim.org/release-docs/latest/api/Search/
- Nominatim indexing: OSM import, tokenizer, address hierarchy, and search
  table flow.
  https://nominatim.org/release-docs/latest/develop/Indexing/
- Nominatim database layout: `placex`, search names, importance, ranks,
  centroid, and address helper tables.
  https://nominatim.org/release-docs/latest/develop/Database-Layout/
- Nominatim ranking and importance docs.
  https://nominatim.org/release-docs/latest/customize/Importance/
  https://nominatim.org/release-docs/develop/customize/Ranking/
- Palacio, Derungs, and Purves, "Development and evaluation of a geographic
  information retrieval system using fine grained toponyms".
  https://josis.org/index.php/josis/article/view/61
