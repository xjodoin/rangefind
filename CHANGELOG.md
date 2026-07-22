# Changelog

## Unreleased

## 0.3.9 — 2026-07-22

### Fixed

- Root suggest-routing finalization now reuses its partition metadata array
  when resolving content-addressed pack names instead of duplicating every
  directory entry at once. Planet-scale merges no longer exhaust the V8 heap
  after all sidecar streams and packs have already completed.

## 0.3.8 — 2026-07-20

### Added

- **Data-driven OSM category lexicon**: the query planner's category
  vocabulary now comes from the index instead of a hardcoded seven-word
  list. Sharded OSM builds merge every shard's `type` facet dictionary,
  join it with a bundled multilingual alias table (French forms, English
  synonyms, irregular plurals), and embed the result in the root manifest
  (`category_lexicon`); single indexes read their lazy `type` facet
  dictionary at query time; indexes without either fall back to a bundled
  canonical OSM vocabulary of ~180 common type values. Any type the corpus
  holds — "cinema", "bakery", "boulangerie", "dépanneur", "movie theater"
  — now gates as a category, so bare category words become nearest-first
  searches around the anchor instead of leaking into locality resolution.
  The artifact vocabulary is pruned to gateable values: a frequency floor
  (default 250) drops the freeform tail of one-off tag strings (a planet
  corpus holds ~37k distinct type values, ~1.2k of them real categories),
  and place/address types are excluded so "Quebec City" and "Miami Beach"
  keep resolving as the cities they are — backed by a whole-surface
  locality probe on every connectorless category/locality split.

### Fixed

- **Bare category words no longer teleport the map to a same-named
  village.** "cinema" was not in the old hardcoded category list, passed
  the locality gate, and resolved `osmLocalityExact` to an actual village
  named Cinema — the demo map then flew across the planet. Category words
  are now recognized via the lexicon and never enter locality resolution.
- **Category-first place names resolve as places again.** Connectorless
  category-first queries ("Bar Harbor", "Park City", "Market Harborough")
  give the whole surface one shot at resolving as a locality before being
  split into category + locality; "cinema in Nice" states its intent
  explicitly and skips straight to the split.

## 0.3.7 — 2026-07-20

### Fixed

- Suggest-set sidecars are now written as bounded concatenated gzip members,
  avoiding V8's maximum string length on multi-million-key OSM shards.

### Added

- **Location-anchored OSM search** (`params.near`): callers pass an advisory
  anchor (user location or map viewport center) and the query cascade uses
  it wherever the text itself names no place. Bare categories and near-me
  phrasing ("pharmacy near me", "restaurants", "cafés autour de moi")
  become a nearest-sorted search around the anchor — against the live
  planet index, "restaurant" drops from a ~4,700-request unroutable
  fan-out to ~95 requests / 2MB with walkable results first. Plain text
  tries a proximity-boosted search scoped to the anchor's 50 km radius
  (geo routing opens only the shards under the caller) and falls back to
  the global cascade when the local page is empty. Explicit intents —
  named localities, streets, an explicit `geo`, suggestion shard hints —
  always outrank the anchor. The map demo now feeds this anchor from
  browser geolocation (adopted silently when permission is already
  granted, one tap on the locate control otherwise) with the map center
  as fallback, and labels anchored results "near you" / "near map view".
  Dragging or zooming the map re-runs a view-anchored query for the new
  area (debounced, and only on genuine user gestures, never the app's own
  post-search recentring); resolved-place queries stay put.

## 0.3.6 — 2026-07-19

### Added

- **Root suggest routing for sharded indexes** (`rfsuggestroute-v1`): every
  shard's authority autocomplete lexicon merges into one root-level artifact
  (`writeSuggestRoutingIndex`, with `writeShardSuggestSet` sidecars for
  pipelines that reclaim shard files). A federated `suggest()` is answered
  from the root in a couple of small range reads instead of opening every
  shard's authority sidecar — on the live 310-shard planet index a cold
  keystroke was ~1,600 requests / 23MB (≈30s on a throttled 4G phone).
  Merged entries keep federation provenance: suggestion rows store shard
  ordinals (authority codec v3), so `suggestions[].shards` still names the
  region(s) that back each suggestion. Fail-open: any missing or broken
  artifact falls back to the per-shard fan-out.
- **`engine.authorityLookup(surface)`**: exact-surface lookup against the
  authority autocomplete lexicon (single or sharded root). The OSM
  integration uses it to resolve locality names ("Berlin", "montreal")
  straight from the root artifact and scope the follow-up search to the
  shards that own the name, replacing the global fan-out that opened ~200
  shards per cold locality query. Advisory: a scoped miss retries unscoped.
- **OSM locality enrichment**: extraction now stamps every document that
  lacks a mapper-provided `addr:city` with its actual municipality —
  administrative-boundary containment first (`boundary=administrative`
  relations at admin_level 8/7, assembled into polygons from a new relation
  pass in the PBF reader), nearest place node as fallback where boundaries
  are missing or clipped at extract edges. The derived name lands in the
  `city` display field and the indexed `address_search` text, so
  brand-plus-town queries ("jean coutu rosemère") match the POI even though
  the OSM node carries only name and amenity tags; the formatted address
  and the authority address lane stay untouched. Luxembourg: 99% of
  documents carry a city after enrichment. Extraction schema v9
  — cached corpora re-extract, and planet deployments should regenerate
  scoring stats alongside the rebuild (locality terms change document
  frequencies corpus-wide).

### Fixed

- **Phantom approximate totals**: early-terminated search lanes floored
  `total` at the requested page size, so a block-budget stop that found no
  eligible documents reported "5 results" with an empty page ("st hubert
  terrebonne" on a Quebec OSM index). Approximate totals now report the
  eligible documents actually seen — zero stays zero, which also lets the
  federated deferred-typo retry and the OSM locality cascade react to
  genuinely empty pages instead of trusting invented counts.

## 0.3.5 — 2026-07-18

### Added

- **Mobile browser benchmark**: `scripts/osm_mobile_bench.mjs` runs the real
  browser bundles in headless Chromium with CPU and network throttling
  against a live deployment, reporting cold/warm latency, requests, transfer,
  and JS heap per demo lane.
- **Demo-flow benchmark lanes**: `scripts/osm_remote_bench.mjs` now covers
  every OSM demo flow — category/street/civic intents, postal codes, map-area
  boxes, discovery orbit, zero-result typo, and address suggest — with a
  retrying, concurrency-capped fetch so large runs survive transient network
  failures.

### Changed

- **Sharded cold queries**: filtered text search skips the per-shard
  doc-values manifest when filter bitmaps cover the plan, and number/geo
  verification loads doc values lazily so only shards with real candidates
  pay for them; typo correction defers to a second fan-out that only runs
  when the merged page is empty; conjunction tails resolve by candidate-doc
  lookup once minShouldMatch can no longer be reached by unseen documents;
  the expanding nearest front queries wider batches and prefetches shard
  engines. On a live 310-shard planet index, locality queries dropped from
  19.2s/109MB to ~3s/23MB, two-term city queries from 8.6s to 1.5s cold
  (736ms to 9ms warm), and nearest-first from 9.3s to under 1s.
- **OSM locality and address resolution**: common locality names resolve to
  their populous bearer ("laval" → Laval, Québec) through a population-gated
  retry; street and civic-address queries resolve inside the locality's own
  shard instead of a full-index fan-out, and civic addresses match through
  their structured house-number and street fields (13.2s → ~2s cold).

### Fixed

- **Range-ignoring CDN responses**: `fetchRange` now tolerates servers that
  answer a Range request with 200 and the full object — small bodies are
  sliced, large ones aborted and retried — instead of failing the query or
  downloading a multi-hundred-MB file onto a phone.
- **Doc-range top-k proof bound**: the doc-range-aware stability proof capped
  remaining potential with the current block's max impact; doc-id-ordered
  postings could hide a higher-impact posting in a later block and wrongly
  prove a top-k missing the best documents. The bound now uses the
  remaining-suffix maximum.

## 0.3.4 — 2026-07-17

### Fixed

- **Byte-stable static hosting**: range-addressed packs and dense pointer
  tables now use a `.bin.gz` suffix so GitHub Pages and similar static hosts
  do not transparently gzip `.bin` responses and invalidate byte offsets.
  Existing manifests and `.bin` indexes remain readable.

## 0.3.3 — 2026-07-17

### Added

- **Prefix-aware sharded routing**: root text routing carries prefix matches
  into autocomplete and propagates shard hints through OSM locality search,
  avoiding unnecessary shard fan-out while preserving fail-open behavior.
- **OSM discovery diagnostics**: the map demo exposes query trace receipts and
  discovery-orbit status alongside improved geo intent parsing and navigation.

### Changed

- **Bounded routing finalization**: gzipped shard term sets are streamed and
  merged through a min-heap with bounded reusable buffers instead of eagerly
  inflating every shard vocabulary into memory. Large routing rebuilds now
  finalize without making heap use grow with the combined term count.
- **Recurring query plans**: short plans are admitted to a 128-entry LRU only
  after their second use, sharing in-flight multilingual analysis between
  concurrent searches. A 30,000-document benchmark improved 1,000 recurring
  searches by 16% (57.4 ms to 48.2 ms) while cold result-bearing queries stayed
  flat; long and one-off queries do not retain full plans.
- **Builder scoring metadata**: stable `alwaysIndexFields` metadata is reused
  across document analysis instead of rebuilding a set per field. OSM-shaped
  analysis benchmarks reduced lookup time and retained heap without changing
  full-build performance.

## 0.3.2 — 2026-07-16

### Added

- **Federated text routing for sharded roots**: `writeTextRoutingIndex`
  (`rangefind/shards`) builds a root-level term → shard-set directory
  (`text-routing/`, format `rftextroute-v1`) by enumerating every shard's
  term directory; `writeShardedRootManifest({ textRouting })` embeds it and
  `buildOsmShardedIndex` builds it automatically. The federated engine then
  opens only the shards that can satisfy `minShouldMatch` of a query plan's
  terms instead of fanning text queries out to every shard — on a 67-shard
  planet index this turns a ~1 200-request, 12–18 MB cold text query into a
  handful of shard opens. Fail-open by design: unknown shard ids are always
  searched, unroutable queries fall back to the full fan-out (per-shard typo
  correction keeps working), and routing errors never break a query.
  Responses report `stats.textRouting`.

### Changed

- **Suggest CPU**: autocomplete candidates are parsed once per authority
  shard and binary-searched per call (previously every suggest call re-walked
  and re-normalized every entry), comparisons no longer allocate, and the
  top-k pool is pruned as it grows. Warm federated suggest on a 67-shard OSM
  index dropped from ~2.5 s to ~0.1 s per call.
- **Text top-k selection**: early-terminated search lanes and the repeated
  top-k stability proofs now select the best k rows with a bounded heap
  instead of materializing and fully sorting every scored document.
- **Geo-filtered text transfer**: the geo doc-set prune now prices itself
  against the doc-value chunks the text lane would otherwise fetch (exact
  candidate leaf-page bytes vs estimated chunk bytes per verified match)
  instead of a fixed candidate-point cap, so city-radius text+near queries
  ride the well-merged geo tree pages — a live text+near+boost query dropped
  from 15.5 MB / 370 requests to 5.3 MB / 193 requests cold with identical
  results. Doc-value chunk fetches for filtered text queries also coalesce
  across each decoded posting-block batch instead of going out per block.

- **Mobile runtime** (`rangefind/mobile`): the full query engine on embedded
  JS hosts — React Native/Hermes, QuickJS, JavaScriptCore. Local indexes
  (bundled with the app or downloaded to device storage) are searched fully
  offline through positional reads on a caller-provided io adapter; http(s)
  indexes get the caching a browser would provide (bytes-bounded memory LRU
  plus an optional persistent cache adapter for content-addressed objects).
  `docs/mobile.md` covers React Native, Expo, WebView/Capacitor, Flutter, and
  native Swift/Kotlin integration paths.
- **`setInflateImplementation(fn)`** in the core runtime: injectable gzip
  inflation for hosts without `DecompressionStream` (e.g. `pako.ungzip` on
  Hermes), mirroring the existing injectable fetch transport.

## 0.3.1 — 2026-07-14

### Added

- **Per-query hook on `<rangefind-search>`**: `element.searchOptions.transform`
  — an async function receiving the params about to be sent and returning the
  params to use. Built for hybrid semantic search (embed the query, set
  `params.vector`); stale transforms are dropped when a newer keystroke wins.
- **Crawler enrichment**: `buildFromCrawl({ enrich })` runs an async hook on
  the crawled documents before indexing (embeddings, external metadata), and
  `buildFromCrawl({ config })` merges overrides into the generated config
  (e.g. a `vectors` declaration for enriched embeddings).
- **Uniform enrichment across every integration**: `enrich` accepts a
  function or a path to an ES module (default export = the hook, optional
  `config` export = overrides), so the CLI (`rangefind build <dir>
  --enrich ./enrich.mjs`), the Eleventy, Astro, and Docusaurus plugins
  (`config` + `enrich` options), and mkdocs-rangefind (`enrich:` setting)
  all expose the same capability. The Eleventy plugin also no longer warns
  on Nunjucks' internal `__keywords` shortcode marker.

- **Query CLI**: the `rangefind` binary now queries indexes as well as
  building them — `search` (facets, `--filter` facet/range/boolean filters,
  `--sort`, `--near`/`--box` geo, `--shards` scoping, `--json`), `suggest`,
  `count`, and `info` (totals, provenance, features, shard tree) against any
  local directory or http(s) index URL. Query-command failures print
  one-line messages with a failing exit code.
- **MCP server** (`rangefind-mcp`, new package): exposes any rangefind
  index as Model Context Protocol tools — `rangefind_search` (text + geo +
  facets + shard scoping), `rangefind_suggest`, `rangefind_count`,
  `rangefind_info`, and `rangefind_list_indexes` — over stdio via the
  official SDK, with structured content, read-only annotations, cached
  engines, and configured/open index access modes. Lives in its own package
  so the core keeps zero runtime dependencies.
- Crawled sites now build a title autocomplete lexicon by default, powering
  the component's `suggest` attribute and `rangefind suggest` on every
  plugin-indexed site.

### Changed

- Search params `filters` documentation and TypeScript types now describe
  the engine's actual shape (`{ facets, numbers: {field: {min, max}},
  booleans }`).
- Site crawls now index deep body vocabulary: the crawler's generated config
  sets `targetPostingsPerDoc: 128` (the corpus-scale default of 12 dropped
  most body terms on long pages, breaking multi-word site search).

## 0.3.0 — 2026-07-14

### Added

- **Release structure**: conditional package exports — node-only entries
  (`./node`, `./builder`, `./crawler`, `./config`, `./shards`,
  `./scoring-stats`, `./osm/node`, `./osm/extract`) resolve to a clear
  import-time error under browser bundler conditions instead of failing on
  `node:` built-ins. TypeScript declarations for the public surface
  (search params/responses including `shards` scoping and geo, builder,
  config, sharded roots, scoring stats, OSM integration). New
  `rangefind/osm/extract` entry exposes PBF → places JSONL extraction as a
  stable API (`extractOsmPlaces`) instead of a script path inside
  `node_modules`. `prepublishOnly` runs the full bundle + test + smoke
  pipeline.

- **Geographic index sharding** (`docs/sharded-osm.md`): a corpus can now be
  built as independently updated per-region shards that federate into one
  engine at query time. A frozen scoring-stats artifact
  (`rangefind/scoring-stats`, new `scoringStats` build config) collects
  corpus-wide document totals, average field lengths, and per-term document
  frequencies (sorted on-disk `rfdf-v1` table with spill-and-merge
  collection, resolved lazily inside parallel reduce workers), so every
  shard bakes exactly comparable BM25 impacts — a sharded index reproduces
  the monolithic build's rankings exactly. A tiny sharded root manifest
  (`rangefind/shards`) lists shard paths and coverage bboxes; the runtime
  opens shard engines lazily, routes radius/box queries to intersecting
  shards, answers nearest-first queries through an expanding shard front
  with an early-stop proof, and merges every lane (text, distance and sorted
  browse, facets, suggest, vector, hybrid). Shards compose with generational
  updates. OSM corpora get a one-call orchestrator
  (`buildOsmShardedIndex` in `rangefind/osm/node`) and a Québec-scale
  benchmark (`npm run bench:osm-shards`). Stats-frozen shards accept
  generational deltas (`build --update` with the same `scoringStats`
  artifact): impacts bake from the shared df table — no per-generation
  term-directory scan, parallel reducers retained — and a delta-updated
  shard is proven identical to a full rebuild of the final corpus, so
  region refreshes publish one small generation instead of re-shipping the
  shard. Queries scope to named shards with `shards: ["quebec"]`
  (search/count/suggest/vectorSearch; unknown names throw), resolve
  multi-level group labels from the shard entries' `groups` hierarchy
  (`shards: ["canada"]` expands to every province shard), or bypass
  federation entirely by opening a shard directory as a standalone index.
  Hierarchical roots compose: a shard entry may itself point at a sharded
  root — routing recurses, merges nest, results carry hierarchical shard
  paths, and rankings match the flat topology.

- **Manifest provenance** (`meta` config option): a free-form provenance
  block carried verbatim into every manifest (full, minimal, generational
  root, sharded root — the root defaults to the first shard's meta) next to
  the existing `built_at`. The OSM integration ships ODbL-compliant defaults
  (`© OpenStreetMap contributors`, `ODbL-1.0`, license URL, plus the RQA
  CC-BY-4.0 source when enabled) and merges caller fields — generator
  identity, source URL, upstream data version — on top via
  `createOsmIndexConfig({ meta })`.

### Changed

- **Reusable OSM integration**: OSM document normalization, compact address
  interpolation, index schema generation, map query intents, and autocomplete
  now live under the browser-safe `rangefind/osm` export. Node-only RQA
  ingestion and index publication are exposed through `rangefind/osm/node`.
  Fixture scripts and the map demo are thin consumers of those APIs, while the
  underlying Rangefind pack format stays unchanged and no OSM sidecar is added.

- **Québec civic and postal coverage**: Québec OSM fixtures now merge the
  monthly CC BY 4.0 Référentiel québécois des adresses through a resumable,
  zipped-CSV stream. A disk-indexed canonical pass collapses units and removes
  only full-address-identical OSM duplicates. Civic records stay out of BM25,
  geo browse, and autocomplete, while canonical addresses and one compact
  aggregate per postal-code/municipality pair use the zero-posting authority
  lane. A measured full run emitted 3.64M civic and 221.7k postal records into
  a 9.95M-document, 8.39 GiB index in 15m25s; posting segments and geo build
  time remained effectively unchanged from the OSM-heavy baseline.

- **Map locality intent and autocomplete overlay**: exact settlement queries
  such as `Laval` resolve the cached `place=city` record globally instead of
  ranking every address that contains the city name inside a stale viewport.
  The demo centers the locality and returns it alone. Street-plus-locality
  queries such as `Rue Hector Rosemère` resolve the town, search only the
  distinctive street token inside its radius, and collapse OSM road segments
  to one street result, avoiding common `rue` posting-budget exhaustion.
  Autocomplete overlays can escape the rounded panel without horizontal
  overflow, long labels wrap, and pending suggestion work is cancelled on
  Enter or Escape. Non-numeric street prefixes now group civic candidates by
  street and municipality, promoting canonical street suggestions without a
  new sidecar; numeric address autocomplete remains unchanged.

- **Canadian postal-code query normalization**: compact forms such as
  `J7B1Z5` are canonicalized to the already-indexed `J7B 1Z5` token form
  before search, count, autocomplete, and exact-address planning. This fixes
  postal-only, category-plus-postal, and full-address searches without an
  index rebuild, duplicate postings, or additional range requests.

- **Compact OSM address interpolation**: numeric `addr:interpolation` ways now
  become one range document per compatible anchor segment instead of millions
  of inferred documents. Street-first 16-number authority buckets locate a
  candidate with zero posting decodes; the runtime verifies range/parity and
  computes the inferred point by distance along a compact 1e-6-degree polyline.
  Explicit address objects always take precedence. On Quebec, 442,979 ways
  produced 339,169 range docs covering about 9.15M possible addresses, growing
  the corpus only 5.9%. The authority shard encoder now uses bounded byte chunks
  rather than a giant JavaScript array, and address-like demo searches bypass a
  stale map viewport so exact addresses cannot be hidden. Address autocomplete
  now resolves a typed house number against lexicon-completed street/locality
  tails after three street-token characters, reusing the compact range lane
  instead of publishing millions of inferred suggestion strings.

- **OSM category-locality search**: the map demo recognizes pharmacy queries
  such as `Pharmacie Rosemère`, `Rosemère pharmacie`, and `pharmacy rosemere`.
  It resolves the exact settlement through the place facet, maps the localized
  category to the indexed OSM term, and runs a distance-sorted geo query. This
  finds POIs whose OSM records omit `addr:city` without fabricating locality
  tags or rebuilding the index; locality resolutions are cached.

- **National-scale address search and posting reduction**: the OSM fixture now
  retains complete address-only nodes and ways, publishes structured address
  fields, and builds canonical full, locality, postcode, and street authority
  keys. Normalized, reordered, and useful partial address forms use a bounded
  zero-posting exact lane before BM25. A 66.8M-document / 700.8M-posting US
  build also exposed two reducer limits: final segment directories now merge
  through 64 KiB streaming cursors instead of decoding roughly 2 GiB into V8,
  and posting blocks are zero-copy typed-array views instead of allocating one
  JavaScript pair per posting. Large posting headers now grow through bounded
  64 KiB byte chunks instead of one giant JavaScript number array. The
  zero-copy change produced byte-identical output 2.17x faster with 62% lower
  RSS on a 1M-posting encoder benchmark.

- **National-scale OSM builds**: the geo fixture now supports the full United
  States Geofabrik extract and uses resumable downloads, disk-spooled candidate
  ways, externally sorted/deduplicated anchors, and an indexed on-disk
  coordinate store. Entity-selective PBF decoding skips unused node or way
  payloads. Quebec extraction dropped from 113.7 s / 2.71 GB peak RSS to
  77.5 s / 462 MiB with byte-identical JSONL. The exhaustive benchmark oracle
  is now a bounded two-pass stream instead of retaining every point and token
  set, and geo root bounding boxes no longer use argument-spread operations
  that can overflow at national leaf counts. Posting reducers share only the
  block-filter code columns they read; on the 32.8M-place US corpus this
  changed reduction from more than 29m35s with no output to 2m41s. Dense
  document pointers use 65,536-row sequential reads instead of two reads per
  document (4m33s to 2m44s for US document packing), and 10M+ builds may
  preload a 2.25 GiB code store so geo summaries and doc-value writers avoid
  random chunk reads. Large sidecars checkpoint independently, so an
  interrupted geo/vector phase no longer repeats authority, document, and
  doc-value work. Unified autocomplete now publishes bounded, adaptive hot
  lists for broad two- and three-letter Latin prefixes in addition to every
  one-character prefix; on the full-US index this reduced mean keystroke
  latency 69% and transfer 22% while adding 54 KiB to the one-time root.

- **Unified authority autocomplete**: `suggest` fields now stream into bounded
  authority runs and are encoded directly in `rfauth-v2` packs with exact
  weights, counts, display strings, token suffixes, per-shard max-rank
  proofs, and lazy one-character hot lists. This removes the scan-wide surface
  map, the second writer-wide map, the `suggest/` pack family, suggestion
  page/branch codecs, `manifest.suggest`, and the old runtime page lane. The
  public `engine.suggest()` and build configuration remain unchanged; legacy
  title-only `rfauth-v1` indexes retain a bounded compatibility path.

### Added

- **Full-Wikipedia build path**: the wiki fixture can place its complete
  workspace on another volume with `--root`, discover and concurrently extract
  Wikimedia's ordered multistream shards, retry throttled downloads, preserve
  completed shard work across retries, and concatenate the result
  deterministically. The full English/French npm scripts use multistream
  extraction and the bounded unified authority autocomplete path.

- **Incremental publishing (Phases 3 & 4 — complete)**: every query lane now
  merges across generations. Sorted browse and text + sort merge by the real
  doc-value keys (a new `loadDocValues` helper on the engine); geo merges in
  all three shapes (box browse, nearest-first by exact distance, radius or
  boosted text search); vector search merges by absolute similarity; and
  hybrid text + vector fuses reciprocal ranks at the *merged* level, so a
  small delta generation can never hand its documents inflated per-generation
  ranks. `rangefind build --compact` folds a generational index back into a
  single index — a full rebuild that verifies every live document id from the
  old generations is present in the input before deleting the `gen-NNNN/`
  directories (and cleans up leftovers from previously failed compactions).
  `build --update` now recommends compaction once an index crosses 8
  generations or 25% tombstoned documents.

- **Static site generator adapters**: real, independently installable
  packages for [Astro](packages/rangefind-astro) (`astro:build:done` +
  `<RangefindSearch />`), [Eleventy](packages/eleventy-plugin-rangefind)
  (`eleventy.after` + a universal `{% rangefindSearch %}` shortcode), and
  [Docusaurus](packages/docusaurus-plugin-rangefind) (`postBuild` +
  `injectHtmlTags`), each running the crawler against the generator's own
  build output and copying the search component's assets in automatically.
  [Hugo](integrations/hugo) (no plugin system — a documented
  `hugo && rangefind build public` recipe plus a `relURL`-based partial) and
  [MkDocs](integrations/mkdocs-rangefind) (a real pip-installable Python
  plugin on `on_post_build`/`on_post_page` that shells out to the Node CLI)
  round out the five. Every adapter is verified end to end against the real
  tool — a real Astro/Eleventy/Docusaurus build, a Homebrew-installed Hugo
  binary, and a pip-installed MkDocs — crawling a fixture site and confirming
  the index is actually searchable through Rangefind's own runtime.

- **Multilingual analysis** (`analysis` config block, `multi-v1` profile):
  per-document language via `languageField` or script + stopword detection;
  per-language light stemmers (en, fr, de, es, it, pt, nl, sv, no, da, fi,
  ru, el, ar, hi) and stopword lists (those plus tr, pl, cs, hu, ro, id);
  script-aware folding (ß→ss, ø→o, Greek tonos + final sigma, Arabic
  harakat/alef/alef-maqsura, Hebrew niqqud, ё→е, width folding); and
  dictionary-free CJK bigram tokenization (Han/Kana/Hangul/Thai-class),
  deterministic across Node and every browser by construction — no
  `Intl.Segmenter`, no ICU dictionaries. Queries analyze in every configured
  language: the detected language drives phrases/proximity/typo, all
  languages' stems join the retrieval union under the skip-search term
  budget, and the runtime swaps to an alternate base plan when the primary
  language's stems have no postings. The profile is stored in the manifest,
  so the browser reconstructs the exact builder analyzer; `build --update`
  refuses deltas whose profile differs from the existing generations.
  Highlighting matches across languages and marks exact CJK bigram spans.
  This is now the only analyzer; a config with no `analysis` block uses the
  default profile (English plus French). The previous Latin-only analyzer
  and its module (`src/analyzer.js`) were removed, and the language-agnostic
  phrase/proximity/bundle term helpers moved to `src/terms.js`.

### Fixed

- Auto posting-codec sampling no longer loops forever when a term spans more
  blocks than the sample budget. Wikipedia extraction now honors writable
  stream backpressure, and capped body storage retains the article's true
  `bodyLength` and length-derived tags.
- Short shard keys are no longer underscore-padded. Padding made a short term
  such as `ai` collide with the real expansion term `ai_`, producing duplicate
  directory keys that could hide one posting segment on large vocabularies.
- Large document-layout merges now use a k-way heap instead of scanning every
  sorted chunk for every document. Layout order uses a compact `Uint32Array`,
  and the document-pack preload fast path is capped at 256 MiB by default to
  avoid multi-gigabyte RSS spikes. The wiki profile restores linear-time
  impact-bucket posting order plus auto block/codec selection for substantially
  earlier broad-query top-k proofs. Posting gzip level is now configurable; the
  measured Wikipedia profile uses level 3 to reduce compression CPU with a
  small transfer-size tradeoff while the library default remains level 6.
  Multi-gigabyte document preloads are chunked to stay below Node's 2 GiB
  Buffer limit. A new `doc-id` document layout packs through bounded sequential
  read windows; the full Wikipedia profile uses it to avoid millions of tiny
  random reads and swap-heavy multi-gigabyte preloads on external volumes.
- Runtime top-k proof is now adaptively bounded to 128 decoded blocks for
  indexes with at least one million documents. This turns pathological broad
  multi-term queries into bounded approximate searches while preserving
  `topKBlockBudget: 0` and exact search for exhaustive callers.
- Authority run spooling now has its own `authorityRunFlushRecords` budget
  (100,000 by default). The old condition referenced a removed posting-run
  option and therefore retained every authority record in heap on large builds.

- Term order in posting segments, shard payloads, and the range directory
  now uses code-unit comparison instead of `localeCompare`. ICU collation
  disagrees with the runtime's binary-search key order outside ASCII, which
  made any index containing CJK terms unsearchable, and it made pack bytes
  depend on the build machine's ICU version. Resume schema bumped to v5 so
  stale collation-ordered intermediate stages cannot mix into new builds.
- Main-index typo correction now accepts candidate tokens in any script
  (previously Latin-only).

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
