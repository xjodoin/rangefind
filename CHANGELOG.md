# Changelog

## 0.2.0 — 2026-07-06

The "full search product" release: geo, autocomplete, semantic hybrid,
facet counts, highlighting, and incremental publishing — all static, all
over HTTP range requests.

### Added

- **Geo queries** (Lucene `LatLonPoint`-class, adapted to range requests):
  `geo` config fields build a static KD tree with a branch-paged root.
  Bounding-box and radius filters (exact, Haversine-verified), exact
  nearest-neighbor distance sort with early-stop proofs — with or without a
  text query — text+geo filtering, per-cell filter summaries, and distance
  boosts. Verified against exhaustive oracles at 175k and 4.27M points;
  cold nearest on 4.27M points ≈ 92 KB. (`docs/osm-geo-benchmarks.md`)
- **Search-as-you-type autocomplete**: `suggest` config fields build a
  prefix-sorted suggestion sidecar with per-page max-weight proofs and
  precomputed per-character hot pages. `engine.suggest({ q })` answers a
  first keystroke in ~2 requests / ~13 KB at 4.27M docs; later keystrokes
  are mostly cache hits. (`docs/suggest-benchmarks.md`)
- **Hybrid semantic search**: `vectors` config fields build an int8 IVF
  index (variance-permuted dimensions, coarse-prefix candidate pages, a
  fixed-width full-dimension refine store). `engine.vectorSearch()` and
  `search({ q, vector })` with reciprocal-rank fusion; filters apply to
  both lanes. Full-probe recall@10 is 0.98 against brute force.
  (`docs/vector-benchmarks.md`)
- **Per-query facet counts**: `search({ facets: [...] })` returns top
  values + counts with exact-or-flagged semantics (dictionary-backed
  global counts, exact counts over budgeted match sets, bounded
  chunk-sampled estimates).
- **Snippets and highlighting**: `search({ highlight })` returns
  analyzer-consistent match ranges over raw display text ("montreal"
  marks "Montréal"), with best-window passage selection.
- **Incremental publishing (Phase 1)**: `rangefind build --update` adds
  delta generations over an existing index. Unchanged pack bytes keep
  their content-addressed names (and CDN cache entries); replaced
  documents tombstone their old version; delta builds replicate the base
  generations' frozen statistics so scores stay exactly comparable across
  generations. (`docs/incremental-publishing-plan.md`)
- **Examples**: an OpenStreetMap map demo (`examples/osm-geo`, MapLibre +
  autocomplete + viewport search) and semantic search in the wiki example
  (`build.mjs --embed`, browser-side query embedding via transformers.js).
- New benchmarks: `bench:osm-geo`, `bench:suggest`, `bench:vectors`.

### Fixed

- The minimal example page now loads the bundled runtime instead of a
  source path that does not exist on static deployments.
- Posting shard parsing without a manifest could misalign on block-filter
  summaries; document frequency and physical row count are now cleanly
  separated in the posting encoder.

### Changed

- `count()` rejects `geo` parameters explicitly (previously ignored).
- Search results include the internal `index` of each hit.

## 0.1.0

Initial extraction: BM25F text search over range-packed static files,
typo correction, authority sidecar, facets, typed doc values, sorted
browse, query bundles, and the frwiki scalability fixture.
