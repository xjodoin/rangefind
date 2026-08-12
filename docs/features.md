# Rangefind feature guide

Rangefind is a build-time search engine whose query-time database is a set of
static, range-addressable files. It supports full-text, structured, geographic,
vector, and OpenStreetMap search without a search service or query-time
database. The same index can run in browsers, Node, mobile JavaScript hosts,
WebViews, and read-only MCP tools.

This guide is the feature map: it explains what Rangefind can do, which API
exposes each capability, which index structures must be built, and which
features compose. For every configuration property and query parameter, use
the [reference](reference.md). For the storage design, use the
[architecture guide](architecture.md).

## At a glance

| Area | Supported capabilities | Primary API or entry point |
| --- | --- | --- |
| Text retrieval | Weighted BM25F, phrases, proximity, exact top-k proofs, typo correction, authority rescue | `engine.search()` |
| Languages | Per-document language, 20+ language profiles, Unicode/script folding, CJK bigrams, cross-language query expansion | `analysis` config |
| Structured search | Facets, numeric/date/boolean filters, facet counts, field sorting, empty-query browse | `filters`, `facets`, `sort` |
| Autocomplete | Weighted prefix and mid-label token completion, hot prefixes, exact authority lookup | `engine.suggest()`, `authorityLookup()` |
| Geographic search | Bounding boxes, hard radii, exact nearest, distance sorting/boosting, viewport search | `search({ geo })` |
| Route search | Encoded polyline or GeoJSON corridors, multi-resolution cells, route progress/direction/rejoin ranking | `search({ geo: { route } })` |
| Semantic search | Int8 IVF cosine retrieval and text/vector reciprocal-rank fusion | `vectorSearch()`, `search({ vector })` |
| OSM search | Places, categories, localities, addresses, interpolation, reverse geocoding, constraints, open-now, geometry | `rangefind/osm` |
| Google Maps migration | Promise-based compatibility adapter for common Places and Geocoding request shapes | `createRangefindMapsAdapter()` |
| Publication | Immutable content-addressed objects, incremental generations, compaction, geographic sharding | CLI, `rangefind/shards` |
| Clients | Browser, Node, React Native/embedded JS, Web Component, static-site adapters, MCP | package entry points |
| Operations | Checksums, provenance, tracing, build telemetry, retrieval-quality and production workload benchmarks | `trace`, manifest metadata, benchmark scripts |

## Build and ingestion

### JSONL corpora

The generic builder accepts one JSON object per line. Dotted paths map source
properties into weighted text fields, result display fields, facets,
doc-values, geo points, authority labels, suggestions, and vectors. Indexed
content and returned content are independent: a long body can participate in
ranking while only a capped excerpt is stored in result packs.

```json
{
  "input": "documents.jsonl",
  "output": "public/rangefind",
  "fields": [
    { "name": "title", "path": "title", "weight": 4.5, "phrase": true },
    { "name": "body", "path": "body", "weight": 1 }
  ],
  "display": ["id", "url", "title", { "name": "summary", "path": "body", "maxChars": 320 }]
}
```

Build from the command line or Node:

```sh
npx rangefind build --config rangefind.config.json
```

```js
import { build } from "rangefind/builder";

await build({ configPath: "rangefind.config.json" });
```

The large-corpus pipeline is file-backed. It uses resumable spools, immutable
posting segments, external sorting, bounded reducers, compressed-object
deduplication, and sampled build telemetry instead of retaining the corpus or
global posting map in memory.

### Static-site crawling

`rangefind build ./dist` crawls generated HTML and writes an index beside the
site. It understands `<main>`, `<article>`, headings, titles, descriptions,
`<html lang>`, and these opt-in attributes:

| Attribute | Purpose |
| --- | --- |
| `data-rangefind-body` | Restrict indexed body content to marked regions. |
| `data-rangefind-ignore` | Exclude an element subtree or an entire page. |
| `data-rangefind-meta="name"` | Store text or an attribute as result metadata. |
| `data-rangefind-filter="name"` | Create a facet value. |
| `data-rangefind-sort="name"` | Create a sortable value. |

Internal links are resolved during crawling. When a link graph is present,
Rangefind computes a normalized PageRank-style `linkRank` and can use it as a
bounded relevance prior or explicit sort field. See [link-graph authority](link-graph.md).

### OpenStreetMap ingestion

`rangefind/osm/node` converts normalized OSM place documents into an ordinary
Rangefind index; `rangefind/osm/extract` provides bounded PBF extraction. The
extractor supports named nodes and ways, addresses, streets, useful closed-way
geometry, compact address interpolation ranges, locality enrichment, and
optional multi-provider civic/postal augmentation. The bundled RQA adapter is
one provider; the shared engine also accepts OpenAddresses, national address
registers, postal gazetteers, or application-owned authorities without
changing the index format or runtime.

```js
import { extractOsmPlaces } from "rangefind/osm/extract";
import { buildOsmIndex } from "rangefind/osm/node";

await extractOsmPlaces({ pbf: "quebec-latest.osm.pbf", root: "work", region: "quebec" });
await buildOsmIndex({ root: "work", region: "quebec" });
```

OSM attribution and source metadata are published in the manifest. The
integration does not create a second search runtime or OSM-specific database.

### Free public OpenStreetMap index

The already-generated rolling index at
[`https://osm.rangefind.dev/`](https://osm.rangefind.dev/) can be queried for
free from browsers or Node without an account, API key, or payment method:

```js
import { createSearch } from "rangefind";

const engine = await createSearch({
  baseUrl: "https://osm.rangefind.dev/"
});
```

The endpoint allows cross-origin range requests and its status page reports
current coverage, source freshness, and rebuild progress. It is a best-effort
public service without an availability SLA. Build or mirror the index under
your own domain for guaranteed capacity, version pinning, custom source data,
or custom OSM fields. Any result UI must retain the published OpenStreetMap
[attribution](https://www.openstreetmap.org/copyright).

## Text search and ranking

### Weighted lexical retrieval

Rangefind uses schema-driven BM25F-style scoring. Each text field controls its
weight and length normalization. Title and heading fields can emit phrase
signals; fields can also emit adjacent-term proximity signals. A bounded
reranker combines those dependency signals without changing the posting hot
loop.

```js
const response = await engine.search({
  q: "static range search",
  page: 1,
  size: 10,
  highlight: { fields: ["title", "body"], maxChars: 240 }
});
```

Results contain configured display fields, internal index id, score, and
optional analyzer-consistent highlight text/ranges. Highlighting returns
ranges rather than HTML so clients control rendering and escaping.

The main index uses adaptive posting segments, range-packed high-frequency
posting blocks, impact/doc-range pruning, and exact top-k stop proofs. A query
can be intentionally budgeted for predictable cold latency; `exact: true`
forces the exhaustive lane when required.

### Canonical authority and relevance priors

Authority fields provide exact title, entity, alias, slug, product, or place
rescue beside BM25. Diacritic-preserving, folded exact, and token keys are
ranked separately, so canonical labels win without inflating ordinary term
frequency.

`rankPrior` is a reusable normalized numeric prior for static quality signals
such as prominence, popularity, or editorial authority. The crawler's
`linkRank` is one producer of this generic mechanism. It reranks only a bounded
window and can be disabled or retuned per query.

### Typo correction

`typoMode: "main-index"` derives candidates from bounded vocabulary shard
probes; there is no corpus-sized spelling structure. Correction runs only when
the first page is empty or weak, and only the strongest corrected plans are
executed. Responses expose `correctedQuery` and token-level `corrections`.

### Multilingual analysis

The `multi-v1` analyzer supports an explicit per-document language or
deterministic language detection, per-language stopwords and light stemmers,
script-aware Unicode folding, and dictionary-free CJK bigrams. Query analysis
can expand across every configured language and selects a base plan using
actual index evidence. The complete analysis profile is frozen in the
manifest, so incremental generations and every runtime use identical tokens.

Light stemming is built in for English, French, German, Spanish, Italian,
Portuguese, Dutch, Swedish, Norwegian, Danish, Finnish, Russian, Greek,
Arabic, and Hindi. Additional stopword profiles cover Turkish, Polish, Czech,
Hungarian, Romanian, and Indonesian; other configured languages still receive
Unicode folding and tokenization.

## Structured retrieval

### Facets and facet counts

Facet fields accept one or many keyword values. They support filtering, lazy
dictionary enumeration, block pruning, and per-query counts.

```js
const response = await engine.search({
  q: "waterproof jacket",
  filters: { facets: { brand: ["Acme"], color: ["blue", "black"] } },
  facets: { fields: ["brand", "color"], size: 20 }
});
```

Each facet-count response includes `exact`. Small match sets are counted
exactly; large sets use bounded chunk sampling and are explicitly marked when
estimated. `engine.loadFacetValues(field)` loads the complete dictionary and
global counts for filter controls.

### Numeric, date, and boolean doc-values

Typed doc-values support minimum/maximum filters and sorting. Numeric types are
`int`, `float`, `double`, and `date`; boolean parsing accepts common boolean and
0/1 representations. Sorted doc-value trees prune irrelevant pages and prove
top-k completion. Optional sort replicas accelerate repeated text-plus-sort
workloads.

```js
await engine.search({
  q: "conference",
  filters: {
    numbers: { startsAt: { min: "2026-08-01", max: "2026-08-31" } },
    booleans: { accessible: true }
  },
  sort: { field: "startsAt", order: "asc" }
});
```

An empty query is valid, so filter-only, sorted browse, geo-only, and
vector-only experiences do not need a synthetic match-all term.

### Counts without result hydration

`engine.count({ q })` returns the text match count without fetching result
documents. `includeResults: false` keeps ranked ids/scores but skips display
payload hydration when another layer owns the documents.

## Autocomplete

`suggest` fields and authority fields share one packed lexicon. Suggestions
support diacritic-folded prefix matching, mid-label token matching, optional
custom weights, popularity fallback, concise-label tie breaks, and precomputed
hot lists for first-keystroke latency.

```js
const { suggestions } = await engine.suggest({ q: "eiff", size: 8 });
const exact = await engine.authorityLookup("Tour Eiffel", { size: 4 });
```

Each suggestion also names the best documents behind its surface: `doc` (the
winning doc ordinal) plus `docs` (all kept rows, `suggestMaxDocRows` per
surface, default 3) when the rank has one unambiguous owner — single-index
engines always, federated engines when exactly one shard/generation owns the
winning rank (`docShard` / `generation` name the owner). Selecting such a
suggestion should hydrate the document directly instead of re-running the
query as a search:

```js
const picked = suggestions[0];
if (picked.doc != null) {
  const [place] = await engine.hydrateRows(
    [[picked.doc, 0]],
    picked.docShard ? { shard: picked.docShard } : {}
  ); // 1-2 small reads
}
```

The [autocomplete guide](autocomplete.md) covers which pattern to use where,
with measured per-keystroke costs.

Or let the engine do it: `suggest({ q, hydrate: true })` resolves every
suggestion's doc rows into real search hits in one batched doc-page pass —
each suggestion gains `result` (best document) and `results` (all kept rows),
shaped exactly like `search()` results, so an autocomplete dropdown can render
the same cards the results list shows. Hydration is advisory: on failure the
text suggestions survive unchanged. The `<rangefind-search>` element uses this
to render doc-resolving suggestions as result cards (with per-completion
result counts) and speculatively warms the search for the top completion, so
accepting a suggestion renders instantly.

The OSM integration wraps the same contract as `selection.doc` plus
`resolveOsmSuggestion(engine, suggestion)`, which returns a one-result search
response (`plannerLane: "osmSuggestEntity"`). Since `rfsuggestroute-v2`,
root-routed suggestions on sharded planets carry the same `doc` + `docShard`
provenance (the artifact stamps the winning row's shard ordinal and doc), so
selection hydrates from the owning shard there too; v1 artifacts fall back to
the shard-scoped search.

`authorityLookup()` returns every canonical display whose normalized surface is
equal to the input. On sharded roots, root-level suggest routing supplies shard
provenance, allowing the selected prediction to scope its follow-up query
without probing every shard.

## Geographic search

Geo fields store one E7-precision latitude/longitude point per document in a
range-addressed static KD tree.

```js
// Visible map area.
await engine.search({ q: "cafe", geo: { box: { minLat, minLon, maxLat, maxLon } } });

// Hard radius.
await engine.search({
  q: "pharmacy",
  geo: { near: { lat: 45.5, lon: -73.6, radiusMeters: 5000 } }
});

// Exact nearest-first, with no hard radius.
await engine.search({
  q: "hospital",
  geo: { near: { lat: 45.5, lon: -73.6 }, sort: "distance" }
});
```

Bounding boxes and radii are exact after spatial pruning. Nearest search uses
best-first tree traversal and early-stop proofs. Text can be combined with
every geo mode, and `boost: { weight, pivotMeters }` biases relevance toward a
point without imposing a hard boundary. Results include `distanceMeters`.

### Geo capsules

`geoCapsules` embeds a compact, configured subset of display data in geo
leaves. Viewport and nearest queries can then return markers from the same
range read that finds their coordinates, avoiding a second document hydration
round trip. The authoritative result payload remains single-copy in document
packs; capsules are intentionally small map-result projections.

### Multi-resolution category cells

`geoCellIndexes` maps selected facet values to point ordinals at multiple grid
resolutions. A query chooses a resolution suited to its radius, viewport, or
route corridor and opens only cells that can contain matching categories.
This prevents dense planet-scale category queries from traversing unrelated
geo leaves.

An optional `includeAll` occupancy lane indexes all point ordinals under a
reserved cell code. It enables the same spatial routing for arbitrary names
and brands that do not have a known category facet. The cells route to the
existing geo payload; they do not duplicate result documents.

### Route-corridor search

Generic `engine.search()` accepts an encoded polyline, GeoJSON `LineString` or
`MultiLineString`, GeoJSON Feature/FeatureCollection, or coordinate array.

```js
const response = await engine.search({
  q: "coffee",
  geo: {
    field: "location",
    route: encodedPolylineOrGeoJSON,
    corridorMeters: 1500,
    polylinePrecision: 5,
    routePositionMeters: 12_400,
    routeDirection: "forward",
    viewport: { lat: 45.56, lon: -73.66 },
    sort: "route"
  },
  size: 20
});
```

The route is rasterized directly into configured multi-resolution cells;
overlapping segment boxes remain separate so a diagonal or winding route does
not become one enormous bounding rectangle. Candidate points are verified
against route segments and ranked using:

- perpendicular distance from the route;
- progress along the route;
- forward/behind position relative to `routePositionMeters`;
- route direction; and
- optional viewport proximity.

Results expose `routeDistanceMeters`, `routeProgressMeters`,
`routeProgressRatio`, `routeBearingDegrees`, `routeRank`, `routeMatch`, and the
closest `rejoinPoint`. This provides “search along this route” without a
routing engine. Exact driving detour time, turn-by-turn instructions, and route
recalculation remain routing-engine responsibilities.

## Vector and hybrid search

Vector fields build a static IVF index with int8 quantized candidate pages and
a fixed-width full-dimension refine store. Cosine similarity is supported.
Embeddings are generated by the application using the same model at build and
query time; Rangefind deliberately does not bundle a model.

```js
const semantic = await engine.vectorSearch({
  vector: queryEmbedding,
  field: "embedding",
  k: 10,
  nprobe: 8,
  refineFactor: 8,
  refine: true
});

const hybrid = await engine.search({
  q: "offline map search",
  vector: queryEmbedding,
  vectorField: "embedding",
  hybrid: { rrfK: 60 }
});
```

Hybrid search fuses text and vector ranks with reciprocal-rank fusion. Filters,
geo restrictions, sharding, and incremental generations compose with the
vector lane.

## OpenStreetMap application features

The browser-safe `rangefind/osm` integration layers maps-specific intent
parsing and result shaping over the generic runtime.

Applications migrating an existing Google Places/Geocoding integration can use
`createRangefindMapsAdapter(engine)` for familiar request and response names,
then progressively adopt the native APIs. See
[Replace Google Maps search APIs](google-maps-migration.md).

### Search and autocomplete intents

`searchOsmQuery()` supports:

- named places and globally unique landmarks;
- multilingual category aliases;
- category near a device/map anchor;
- category or named place within a locality;
- exact locality and named-street resolution;
- map viewport search and repeat search after panning;
- location-biased text without a hard radius;
- exact and partial civic addresses;
- Canadian postal-code normalization;
- intersections; and
- decimal-coordinate input routed to reverse geocoding.

`suggestOsmQuery()` supports locality, category/locality, brand, street, civic
address, interpolated address, native-script, and cursor-edit predictions.
Predictions include `mainText`, `secondaryText`, match ranges, types, and a
reusable `selection` carrying query text and shard hints.

### Forward and reverse geocoding

Complete civic addresses use a zero-posting canonical authority lane. Multiple
canonical forms cover component reorderings, common road abbreviations, postal
spacing, and useful partial forms. Queries additionally probe every plausible
reading of a pasted envelope form against keys already published by older
builds: abbreviation readings generated from libpostal's address dictionaries
for 14 languages (en, fr, es, de, pt, it, nl, ca, sv, da, nb, pl, cs, ro) —
"St" as Saint, Street, Straße, Strada, or Straat; "Trl", "Hwy", "Ch", "Rte",
"Vle", "Avda", "Ul"; "Ste" as Sainte or a suite — directionals ("311 A Bd
Cartier O, Laval, QC H7N 2J3" resolves like "311 Boulevard Cartier Ouest,
Laval"; "NW" like "Northwest"), Germanic/Scandinavian concatenated street
suffixes ("Marktstr 5" and "Markt Str 5" both resolve like "Marktstraße 5"),
single-token state/province names in both directions ("Ohio" ↔ "OH"),
detached or attached unit letters and "Apt N"/"#2F" designations, and
trailing province/state and postal-code tails. Candidates rank cheapest
interpretation first under a fixed probe cap, so unambiguous queries still
derive a single key. Explicit address points always win over interpolation.

OSM `addr:interpolation` ways remain compact ranges rather than becoming one
document per potential house number. The runtime checks range bounds,
odd/even/step semantics, and computes the coordinate along the stored street
polyline only after an authority hit.

```js
import { reverseGeocodeOsm } from "rangefind/osm";

const response = await reverseGeocodeOsm(engine, {
  lat: 45.5019,
  lon: -73.5674,
  radiusMeters: 5000,
  size: 8,
  resultTypes: ["street_address"],
  locationTypes: ["ROOFTOP", "RANGE_INTERPOLATED"]
});
```

Reverse geocoding uses a hard radius, queries only intersecting geographic
shards, and returns a formatted address, structured components, location type,
and accuracy. A bounded locality fallback is available when result filters ask
for administrative places instead of addresses.

### Typed place constraints

Natural-language constraints can be embedded in `query`, or passed explicitly
through `constraints`. Supported predicates are:

| Constraint | Representative natural phrases |
| --- | --- |
| `openNow` | `open now`, `currently open`, `ouvert maintenant` |
| `wheelchair` | `wheelchair accessible`, `accessible en fauteuil roulant` |
| `toiletsWheelchair` | `wheelchair-accessible toilets`, French equivalent |
| `contactless` | `contactless`, `tap payment`, `paiement sans contact` |
| `delivery` | `with delivery`, `avec livraison` |
| `takeaway` | `takeaway`, `takeout`, `pour emporter` |
| `driveThrough` | `drive-through`, `service au volant` |
| `outdoorSeating` | `outdoor seating`, `patio`, `avec terrasse` |
| `internet` | `wifi`, `wi-fi`, `internet` |
| `reservation` | `takes reservations` |
| `free` | `free admission`, `no fee` |

```js
const response = await searchOsmQuery(engine, {
  query: "wheelchair-accessible cafe open now with wifi",
  near: { lat: 45.5, lon: -73.6 },
  timeZone: "America/Toronto",
  limit: 20
});
```

Static constraints are pushed into indexed facets when the index exposes them,
then verified against hydrated OSM details. This two-stage contract keeps range
reads selective without trusting an incomplete payload. Results include
`constraintMatches`.

### Open-now evaluation

`evaluateOpeningHours()` evaluates common OSM `opening_hours` expressions
entirely on the client. It supports `24/7`, weekday lists/ranges, multiple
daily intervals, overnight intervals, `off`/`closed`, semicolon rules, and
later-rule overrides in an IANA timezone.

```js
import { evaluateOpeningHours } from "rangefind/osm";

evaluateOpeningHours("Mo-Fr 08:00-18:00; Sa 09:00-13:00", {
  at: "2026-08-03T14:00:00Z",
  timeZone: "America/Toronto"
});
```

The result is `{ state: "open"|"closed"|"unknown", isOpen, reason, ... }`.
Holiday calendars, named months/dates, sunrise/sunset, dawn/dusk, Easter, and
other unsupported expressions return `unknown`; they are never guessed open.
`includeUnknownOpenNow: true` lets an application retain unknown results while
still distinguishing them from verified-open places.

### OSM search along a route

`searchAlongRouteOsm()` combines OSM intent parsing, typed constraints, and
the generic route engine:

```js
import { searchAlongRouteOsm } from "rangefind/osm";

const response = await searchAlongRouteOsm(engine, {
  route: encodedPolylineOrGeoJSON,
  query: "Tim Hortons open now with contactless",
  corridorMeters: 1500,
  routePositionMeters: 12_400,
  routeDirection: "forward",
  viewport: { lat: 45.56, lon: -73.66 },
  timeZone: "America/Toronto",
  limit: 20
});
```

Known category queries use typed category cells. Brand and arbitrary text
queries use the wildcard occupancy lane when present. Adjacent byte ranges are
coalesced or sent as multipart ranges, and the final result set is verified
against the exact corridor before route-aware ranking.

### Place details and geometry

OSM results can carry compact details for opening hours, contact fields,
brand/operator, cuisine, wheelchair/accessibility, internet, seating,
takeaway/delivery/drive-through, payment methods, capacity, access, and OSM
knowledge references.

Useful closed ways such as parks, campuses, venues, and buildings can carry a
simplified encoded polygon or line plus precision and bounds. Decode it with
`decodePolyline(result.geometry.encoded, result.geometry.precision)`. The OSM
map example includes MapLibre fill and line layers that render result geometry
while retaining the point marker and true polygon centroid.

## Index publication and federation

### Immutable static publication

Posting packs, directories, document packs, doc-values, geo leaves, vectors,
and sidecars use content-addressed immutable names. Mutable root manifests are
small and revalidated; unchanged index bytes retain both their filenames and
CDN cache entries. Optional SHA-256 verification runs before decompression.

### Incremental generations

`rangefind build --update` adds a small generation containing new or replaced
documents plus tombstones. Every retrieval lane merges across generations:
text, authority, autocomplete, filters, sort, geo, route, vector, hybrid, and
facets. Frozen corpus-wide scoring statistics keep scores comparable.

`rangefind build --compact` rebuilds the full live corpus into one generation
and removes old generation directories only after coverage verification.

### Geographic sharding

`rangefind/shards` publishes a small root manifest over independently built
regional indexes. Shared frozen scoring statistics preserve monolithic-equivalent
ranking. The root runtime supports:

- explicit shard ids and hierarchical groups;
- bbox/radius/corridor routing by shard coverage;
- expanding nearest-first search;
- root text-to-shard routing;
- root suggestion routing with selection provenance; and
- merged search, count, suggest, vector, facet, and incremental results.

Term-set and suggestion-set sidecars allow root routing artifacts to be rebuilt
after bulky shard-local build artifacts have been reclaimed.

## Runtime targets

### Browser

Use `rangefind/browser` or `dist/runtime.browser.js`. The browser runtime uses
HTTP byte ranges, the browser cache, `DecompressionStream`, and optional
multipart byte ranges. It has no framework or DOM dependency.

### Node

`rangefind/node` reads local indexes with positional file reads or remote
indexes over HTTP(S). Remote immutable ranges use shared memory and disk
caches; manifests use ETag/Last-Modified revalidation. The engine adds
`cacheStats()` and `close()`. See [Node runtime](node-runtime.md).

### Mobile and offline

`rangefind/mobile` accepts an injected positional-read adapter, gzip inflater,
fetch implementation, and persistent cache. It runs the same search engine in
React Native/Hermes, JavaScriptCore, QuickJS, or another embedded JS host.
WebView/hybrid and native/Flutter recipes are documented in [Mobile](mobile.md).

### Search Web Component

`<rangefind-search>` is an unstyled, light-DOM, framework-agnostic WAI-ARIA
combobox with keyboard navigation, autocomplete, instant results, snippets,
events, CSS hooks, and an optional light/dark theme. It can be used directly or
inside React, Vue, Svelte, Angular, and static-site generators. Its dropdown
uses hydrated suggestions: completions show their result counts, suggestions
that resolve to a single document render as the same result cards the results
list uses (activating one navigates directly), and the search for the top
completion is speculatively warmed so accepting a suggestion renders from
cache.

### Static-site generator integrations

Rangefind ships integration recipes/packages for Astro, Eleventy, Docusaurus,
Hugo, and MkDocs. Each hooks the Rangefind crawl into the generator's completed
build and makes the Web Component assets available to the generated site.

### MCP

`rangefind-mcp` exposes configured local or remote indexes as read-only search,
suggest, count, info, and index-list tools. Single, generational, and sharded
indexes are supported without placing a search service behind the MCP server.

## Transport, caching, and deployment

The runtime fetches only the directory pages, posting blocks, doc-value pages,
geo cells/leaves, vector pages, and result ranges needed for a query. Nearby
ranges are merged under per-lane overfetch budgets; separated ranges targeting
the same object can share a multipart request when the transport supports it.
Unsupported multipart servers transparently fall back to ordinary single-range
reads.

A remote static host must:

- serve byte ranges as `206 Partial Content` with a correct `Content-Range`;
- preserve `Range` request semantics rather than rewriting them to a full
  range-ignoring `200` response;
- allow `.bin.gz` files without applying a second content encoding; and
- cache content-addressed files as immutable while revalidating manifests.

The runtime works without multipart byte ranges, but first-class multipart
support reduces request fan-out for scattered reads. See the
[deployment reference](reference.md#deployment-requirements).

## Observability and benchmarks

Every query can return lane-specific statistics. `trace: true` adds fetch
counts, transferred bytes, timing buckets, opened shards, and planner details.
Builds publish phase timings, memory samples, disk deltas, segment counters,
and optimizer diagnostics.

Repository benchmarks cover:

| Benchmark | Coverage |
| --- | --- |
| `bench:quality` | Known-item retrieval, Hit@k/MRR, typo recovery, filters and sort |
| `bench:performance` | Cold/warm latency, requests, and transferred bytes |
| `bench:directories` | Directory layout alternatives |
| `bench:suggest` | Prefix correctness and autocomplete transport cost |
| `bench:vectors` | IVF recall, refine behavior, and hybrid search |
| `bench:osm-geo` | Bbox, radius, nearest, and text-plus-geo against exhaustive oracles |
| `bench:osm-address` | Exact and interpolated address lookup |
| `bench:osm-shards` | Federated versus monolithic correctness/cost |
| `bench:osm-maps:*` | Production map-search journeys, reverse geocoding, constraints, route corridors, and geometry |
| `bench:frwiki:*` | Large multilingual corpus quality and scalability |
| `bench:link-rank:*` | Link-prior cost and ranking quality |

The maps promotion benchmark checks result identity and semantics in addition
to latency: locality, viewport containment, distance order, hard radius,
route-corridor distance/progress/rejoin, open-now truth, constraint satisfaction,
geometry coverage, shard fan-out, request count, and transfer size. See
[OSM maps workload benchmark](osm-maps-benchmark.md).

## Feature composition

Most capabilities are orthogonal and can be combined in one query:

| Combination | Supported | Notes |
| --- | --- | --- |
| Text + facets/numbers/booleans | Yes | Filters participate in planning and verification. |
| Text + sort | Yes | Optional sort replicas accelerate common fields. |
| Text + geo radius/box/nearest | Yes | Exact spatial verification; optional distance boost. |
| Text + route corridor | Yes | Exact segment-distance verification and route sorting. |
| Filters + geo/route | Yes | Geo-cell category routing can reduce both tree and payload reads. |
| Text + vector | Yes | Reciprocal-rank fusion; host supplies the query embedding. |
| Vector + filters/geo | Yes | Candidate results obey structured and geographic restrictions. |
| Facet counts + text/filter | Yes | Exact or explicitly marked estimated counts. |
| Incremental + all runtime lanes | Yes | Generations are merged with tombstones. |
| Sharding + all primary lanes | Yes | Root routing avoids avoidable shard fan-out. |
| OSM constraints + route | Yes | Static facets push down; details/opening hours verify afterward. |

## Deliberate boundaries

Rangefind is a static retrieval engine, not a source of dynamic data. It does
not provide live traffic, directions, turn-by-turn navigation, driving detour
time, reviews, photos, live occupancy, or server-maintained business status.
OSM `opening_hours` is evaluated from the indexed schedule and chosen time; it
is not a guarantee that a business made an exceptional same-day change.

The vector lane indexes supplied embeddings but does not generate them. The OSM
integration searches published OSM/RQA data but does not call external place,
geocoding, or routing services.

## Index compatibility

Runtime code can read older Rangefind indexes and falls back when newer
optional structures are absent. Features that depend on newly indexed fields
cannot appear until the corpus is rebuilt:

| Feature | Older-index behavior |
| --- | --- |
| Geo capsules | Correct results with ordinary document hydration. |
| Multi-resolution category cells | Correct geo-tree fallback with more reads. |
| Wildcard route occupancy | Correct route search through fallback geo traversal; potentially more cold I/O. |
| OSM typed constraints/details | No matching detail can be verified; rebuild with OSM schema v3. |
| OSM geometry | Markers still work; polygons/lines require schema-v4 capsules. |
| Root text/suggest routing | Query falls back to broader shard probing. |

This fallback policy preserves correctness where the old index contains enough
data and makes performance features independently deployable. Use
`npm run bench:osm-maps:next-index` as the promotion gate for a rebuilt OSM
schema.

## Documentation map

- [Autocomplete guide](autocomplete.md) — search-as-you-type patterns, measured
  costs per keystroke, previews, instant selection, and anti-patterns.
- [Reference](reference.md) — configuration, APIs, runtime tuning, UI component,
  incremental publishing, and deployment.
- [Architecture](architecture.md) — file formats and retrieval design.
- [Node runtime](node-runtime.md) and [mobile](mobile.md) — non-browser clients.
- [Sharded OSM](sharded-osm.md) — regional builds, routing, and score parity.
- [OSM example](../examples/osm-geo/README.md) — extraction, map UX, addresses,
  interpolation, constraints, routes, and geometry.
- [OSM maps benchmark](osm-maps-benchmark.md) — production workload contracts.
- [Google Maps migration](google-maps-migration.md) — compatibility adapter,
  complete map journeys, CDN setup, attribution, and parity boundaries.
- [Vector](vector-benchmarks.md), [autocomplete](suggest-benchmarks.md), and
  [geo](osm-geo-benchmarks.md) benchmark results.
- [Link-graph authority](link-graph.md) — static PageRank enrichment and tuning.
