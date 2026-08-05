# Rangefind Reference

Complete reference for configuring, tuning, building, deploying, and querying
a Rangefind index — configuration schema, build tuning, the runtime API, and
deployment. For the conceptual overview and format internals see
[`architecture.md`](architecture.md); for a capability-oriented overview see
the [feature guide](features.md); for feature-specific benchmarks see the
`*-benchmarks.md` docs.

- [Mental model](#mental-model)
- [Installation and entry points](#installation-and-entry-points)
- [The builder](#the-builder)
- [Configuration reference](#configuration-reference)
  - [Top-level keys](#top-level-keys)
  - [`fields`](#fields--text-search) · [`facets`](#facets) ·
    [`numbers`](#numbers) · [`booleans`](#booleans) ·
    [`sorts` / `sortReplicas`](#sorts-and-sortreplicas)
  - [`authority`](#authority) · [`geo`](#geo) · [`suggest`](#suggest) ·
    [`vectors`](#vectors)
  - [`rankPrior`](#rankprior) · [geo capsules/cells](#geo-capsules-and-multi-resolution-cell-indexes)
  - [Typo correction](#typo-correction)
- [Build tuning reference](#build-tuning-reference)
- [Runtime API](#runtime-api)
  - [`createSearch(options)`](#createsearchoptions)
  - [`engine.search(params)`](#enginesearchparams)
  - [`engine.count` / `suggest` / `vectorSearch`](#other-query-methods)
  - [Response `stats`](#response-stats)
- [OpenStreetMap integration](#openstreetmap-integration)
- [Static site generator adapters](#static-site-generator-adapters)
- [Incremental publishing](#incremental-publishing)
- [Deployment requirements](#deployment-requirements)
- [Tuning recipes](#tuning-recipes)

---

## Mental model

Rangefind has two phases with no shared runtime:

1. **Build** (Node) — `rangefind build` reads newline-delimited JSON, writes a
   directory of immutable, content-addressed static files (posting packs,
   doc-value packs, directories, sidecars, and a small manifest).
2. **Query** (browser or Node) — `createSearch({ baseUrl })` loads the tiny
   manifest, then answers queries by fetching byte ranges of the static files
   over HTTP. There is no server process and no database.

Everything you configure lives in one `rangefind.config.json`; everything you
tune at query time lives in the `createSearch` options and per-call params.

---

## Installation and entry points

```bash
npm install rangefind      # requires Node >= 22
```

Package exports:

| Import | Purpose |
| --- | --- |
| `rangefind` | Core `createSearch`, fetch/inflate injection, and the default ESM runtime. |
| `rangefind/browser` | Bundled ESM runtime for browsers. |
| `rangefind/node` | Local positional-read or cached remote Node runtime. |
| `rangefind/mobile` | Injected-I/O runtime for React Native and embedded JavaScript hosts. |
| `rangefind/element` / `rangefind/element.css` | Search Web Component and optional theme. |
| `rangefind/builder` | Node index builder: `build({ configPath, update?, compact? })`. |
| `rangefind/crawler` | Static HTML crawler used by `rangefind build ./dist`. |
| `rangefind/osm` | Browser-safe OSM documents, schema, intents, constraints, hours, route, and geometry helpers. |
| `rangefind/osm/node` | OSM/RQA build and sharded publication helpers. |
| `rangefind/osm/extract` | Bounded PBF-to-JSONL extraction. |
| `rangefind/shards` | Root manifests plus text/suggest routing and sidecar writers. |
| `rangefind/scoring-stats` | Frozen corpus-wide statistics for independently built shards/generations. |
| `rangefind/config` | Node config loading and normalization. |
| `rangefind/analysis` | Analyzer construction and normalization. |
| `rangefind/terms` | Term/query-bundle helpers. |
| `rangefind/link-graph` | PageRank-style graph scoring helpers. |
| `rangefind/package.json` | Package metadata. |
| `rangefind` bin | the `rangefind build` CLI |

In the browser, import the bundle so no source resolution is needed:

```js
import { createSearch } from "https://your-host/runtime.browser.js";
const engine = await createSearch({ baseUrl: new URL("./rangefind/", location.href).href });
```

---

## The builder

### CLI

```bash
rangefind build --config path/to/rangefind.config.json
rangefind build --config path/to/delta.config.json --update
rangefind build --config path/to/rangefind.config.json --compact
```

- `--config <path>` — required; the JSON config below.
- `--update` — treat the config's `input` as a delta (new or replaced
  documents) and add it as a new generation over the existing `output`
  (see [Incremental publishing](#incremental-publishing)).
- `--compact` — fold a generational index back into a single index: a full
  rebuild whose `input` must be the **full corpus**, followed by removal of
  the old `gen-NNNN/` directories. Fails (keeping the generation files) if
  any live document from the generational index is missing from the input.

### Programmatic

```js
import { build } from "rangefind/builder";
await build({ configPath: "rangefind.config.json" });
await build({ configPath: "delta.config.json", update: true });
```

### Crawling a static site

Instead of authoring JSONL, point Rangefind at a built static site and it
extracts and indexes the HTML directly:

```bash
rangefind build ./dist [--output <dir>] [--base-url <url>] [--root <dir>]
```

- `<dir>` — the directory to crawl (mutually exclusive with `--config`).
- `--output` — index output directory (default `<dir>/rangefind`). A nested
  output directory is pruned from the crawl automatically.
- `--base-url` — URL prefix or origin for result URLs (default `"/"`). A path
  like `/blog/` prefixes every URL; an absolute origin like
  `https://example.com/` produces absolute URLs.
- `--root` — directory whose relative paths define ids and URLs (default
  `<dir>`); useful when scanning a subtree of a larger site.

The crawler walks every `.html`/`.htm` file (skipping dotfiles and
`node_modules`), extracts each page, writes a JSONL corpus plus an inferred
config to a temp work dir, and runs the normal builder against them. The work
dir is removed on success and preserved (with its path logged) on failure.

**Extraction rules.** For each page:

- **Title** — first non-empty of a `data-rangefind-meta="title"` value, the
  first `<h1>`, then `<title>`.
- **Headings** — the text of `<h1>`–`<h4>`, space-joined, into a `headings`
  field.
- **Body** — if any element carries `data-rangefind-body`, only the text inside
  those; otherwise the text of `<main>`, else `<article>`, else `<body>`. The
  subtrees of `<script>`, `<style>`, `<template>`, `<noscript>`, `<nav>`,
  `<aside>`, and any element with `data-rangefind-ignore` or `hidden` are always
  excluded. `<header>`/`<footer>` are excluded only when the fallback `<body>`
  region is selected (no `<main>` or `<article>` present) — inside a real
  `<main>`/`<article>` region they are trusted as content, so an in-article
  post header and its `<h1>` are indexed. Whitespace is collapsed and HTML
  entities (named and numeric) are decoded.
- **Language** — `<html lang="fr-CA">` reduces to its primary subtag (`fr`) and
  becomes the per-document `lang` field.
- **Description** — `<meta name="description">` becomes a `description` field.
- **URL** — an `index.html` collapses to its directory URL with a trailing
  slash (`docs/guide/index.html` → `/docs/guide/`); any other page becomes a
  pretty, extension-less path (`about.html` → `/about`).
- **Id** — the collapsed URL slug (`docs/guide`), falling back to the raw
  relative path on collision so ids stay unique and deterministic.

**`data-rangefind-*` attributes** (the `rangefind` namespace mirrors Pagefind):

| Attribute | Effect |
| --- | --- |
| `data-rangefind-body` | Restrict body indexing to the marked elements only. |
| `data-rangefind-ignore` | Drop this element's subtree; on `<html>` or `<body>` the whole page is skipped. |
| `data-rangefind-meta="name"` | Capture the element text as metadata `name`. The `"name:attr"` form reads the named attribute instead (e.g. `data-rangefind-meta="date:datetime"`). First value wins. |
| `data-rangefind-filter="key"` | Add the element text as a facet value under `key`; the same key may repeat for multi-valued facets. |
| `data-rangefind-sort="key"` | Use the element text as a sort value under `key`. |

**Inferred config.** Only fields that actually appear are configured. `title`
(weight 4.5, phrase), `headings` (weight 2.0), and `body` (weight 1.0) become
text fields; each `data-rangefind-filter` key becomes a `facets` entry;
`display` is `["title", "url"]` plus `description` (when present) and a 300-char
`excerpt` from the body; `analysis` uses the union of discovered `lang` values
(or the `["en", "fr"]` default) with `languageField: "lang"`. Each
`data-rangefind-sort` key whose values all parse as numbers or dates becomes a
sortable `numbers` column; keys with non-numeric values are exposed as facets
instead. `resumeBuild` is disabled for crawl builds.

### Output

The `output` directory is what you deploy. Key files:

- `manifest.min.json` — the small manifest the runtime loads cold. (`manifest.json`
  is the full diagnostic manifest; `manifest.full.json` is a lazy sidecar.)
- `terms/`, `docs/`, `doc-values/`, `facets/` — the core index.
- `authority/`, `geo/`, `vectors/`, `filter-bitmaps/` — sidecars,
  present only when the corresponding config section is set.
- `debug/build-telemetry.json` — phase timings and counters.

Files under `_build/` are resume scratch and are not deployed.

---

## Configuration reference

A minimal config:

```json
{
  "input": "docs.jsonl",
  "output": "public/rangefind",
  "fields": [
    { "name": "title", "path": "title", "weight": 4.5, "b": 0.55, "phrase": true },
    { "name": "body", "path": "body", "weight": 1.0, "b": 0.75 }
  ],
  "display": ["title", "url"]
}
```

Paths in `input`, `output`, and `buildTelemetryPath` resolve relative to the
config file. `path` values in field definitions are dotted lookups into each
JSON document (`"a.b.c"`); array-valued paths are handled per field type.

### Top-level keys

| Key | Default | Meaning |
| --- | --- | --- |
| `input` | — | Path to the JSONL corpus (one JSON object per line). |
| `output` | `public/rangefind` | Directory the index is written to. |
| `idPath` | `id` | Dotted path to the stable external document id. |
| `urlPath` | `url` | Dotted path to a result URL (added to the display payload). |
| `display` | `["title","url"]` | Fields returned with each result (see below). |
| `indexProfile` | `static-large` | Build profile; affects resume defaults. |
| `buildTelemetryPath` | — | If set, writes build telemetry JSON here too. |

**`display`** entries are either a field name string, or an object
`{ "name": "bodySnippet", "path": "body", "maxChars": 640 }` that copies a
(optionally truncated) value into the result payload under `name`. Display
values are the only document data returned at query time — index long, return
short.

### `fields` — text search

Weighted BM25F text fields. Every entry:

| Key | Default | Meaning |
| --- | --- | --- |
| `name` | — | Field name (used by `alwaysIndexFields`, authority, etc.). |
| `path` | — | Dotted path to the text in each document. |
| `weight` | `1` | Field weight in the BM25F sum. |
| `b` | `0.75` | Length-normalization strength (0 = none). |
| `phrase` | `false` | Emit phrase n-gram signals (good for titles). |
| `phraseWeight` | `8` | Weight for phrase signals when `phrase` is on. |
| `proximity` | `false` | Emit adjacent-term proximity signals. |
| `proximityWeight` | `3.5` | Weight for proximity signals. |
| `proximityWindow` | `5` | Token window for proximity. |

Global text knobs: `bm25fK1` (default `1.2`), `targetPostingsPerDoc` (default
`12`, the per-document term budget), `bodyIndexChars` (default `6000`, caps
indexed body text), and `alwaysIndexFields` (default `["title","categories"]`,
indexed before the budget is applied).

### `analysis` — multilingual text analysis

Rangefind has one analyzer, the `multi-v1` profile. Omitting the `analysis`
block selects the default profile — English plus French (`["en", "fr"]`,
primary English) — reflecting this corpus's heritage; set the block to tune
languages and behavior:

```json
{
  "analysis": {
    "languages": ["fr", "en", "de", "ja"],
    "languageField": "lang"
  }
}
```

| Key | Default | Meaning |
| --- | --- | --- |
| `languages` | `["en", "fr"]` | ISO 639 codes the corpus contains (BCP47 tags reduce to their primary subtag). |
| `primary` | first language | Fallback language when detection is inconclusive. |
| `languageField` | — | Dotted path to a per-document language code; wins over detection. |
| `detect` | `true` | Detect each document's language by script and stopword profile. |
| `stemming` | `"light"` | `"light"` (per-language light stemmers) or `"off"`. |
| `stopwords` | `"default"` | `"default"` (per-language lists) or `"off"`. |
| `foldDiacritics` | `true` | Fold accents (`Montréal` → `montreal`); also folds ß→ss, ø→o, Greek tonos, Arabic harakat, ё→е. |
| `minLength` | `2` | Minimum token length for alphabetic tokens (never applies to CJK). |

How it works:

- **Tokenization is deterministic in every environment.** Unicode word runs
  split at script boundaries; Han, Kana, Hangul, and Thai-class runs become
  overlapping character bigrams (the Lucene CJKAnalyzer approach). No
  `Intl.Segmenter`, no ICU dictionaries — the browser must reproduce the
  builder's tokens exactly, and dictionary segmentation differs per engine.
- **Each document is analyzed in its own language** (explicit
  `languageField`, else script histogram + stopword voting, else `primary`),
  selecting that language's stopword list and light stemmer. Light stemmers
  ship for `en fr de es it pt nl sv no da fi ru el ar hi`; stopword lists
  additionally for `tr pl cs hu ro id`. Other codes fold and tokenize but
  pass through unstemmed.
- **Queries analyze in every configured language.** The detected query
  language provides the base plan (phrases, proximity, typo correction), all
  languages' stems join the retrieval term union (capped at the planner's
  skip-search budget), and the runtime swaps to an alternate language's base
  plan when the primary's stems have no postings — so a German query against
  a mostly-French index still ranks exactly.
- **The profile is frozen into the manifest.** The runtime reconstructs the
  identical analyzer from `manifest.analysis`; `build --update` refuses a
  delta whose profile differs from the existing generations.
- Highlighting is analyzer-aware: accented words highlight from unaccented
  queries in any configured language, and CJK matches highlight the exact
  bigram span, not the whole run.

### `facets`

Multi-value keyword metadata for filtering, counting, and block pruning.

```json
{ "name": "category", "path": "category", "labelPath": "categoryLabel" }
```

- `path` — dotted path; arrays become multiple facet values.
- `labelPath` — optional display label path (defaults to `path`).

### `numbers`

Typed numeric/date doc-values for range filters and sorting.

```json
{ "name": "year", "path": "year", "type": "int", "sortable": true }
```

- `type` — one of `int` (default), `float`, `double`, `date` (parsed to epoch
  ms). `int` values are rounded.
- `sortable` — default `true`; set `false` to build filter columns without the
  sorted tree.

### `booleans`

```json
{ "name": "featured", "path": "featured" }
```

Accepts `true`/`false`/`1`/`0`/`"true"`/`"false"`.

### `sorts` and `sortReplicas`

- `sorts` — declarative sort field metadata carried in the manifest.
- `sortReplicas` — precomputed rank-ordered posting replicas that make
  `sort` + text queries fast. Each entry is `"year"` or
  `{ "field": "year", "order": "desc" }`; the field must be a configured
  number or boolean.

### `rankPrior`

A generic normalized numeric relevance prior, useful for static prominence,
quality, popularity, or authority signals:

```json
{ "rankPrior": { "field": "prominence", "boost": 0.35, "overfetch": 4 } }
```

- `field` — a configured numeric field whose values are normalized to `[0, 1]`.
- `boost` — non-negative multiplier in `score *= 1 + boost * value`.
- `overfetch` — bounded candidate-window multiplier, clamped to 1–20.

The prior is applied outside the posting hot loop. Query parameters
`rankPrior: false`, `rankPriorBoost`, and `rankPriorOverfetch` can disable or
retune it. The static-site crawler's link graph is built on this mechanism; OSM
uses it for place prominence.

### `authority`

An exact title/entity/alias rescue index, layered over BM25 so canonical
labels win without distorting posting scores.

```json
{ "name": "title", "path": "title", "weight": 1000000,
  "exactWeight": 1000000, "tokenWeight": 800000 }
```

- `weight` — base weight; `surfaceWeight` (default `2×exactWeight`),
  `exactWeight` (default `weight`), and `tokenWeight` (default `0.8×weight`)
  tune the diacritic-exact, folded-exact, and token match tiers.
- `suggest` fields share the authority run/reduce pipeline and packs. The
  emitted `rfauth-v2` autocomplete namespace carries display text, counts, and
  weights without duplicating a scan-wide title map or separate pack family.
- Older title-only `rfauth-v1` indexes remain available through a bounded
  lexicographic fallback.

### `geo`

Lucene `LatLonPoint`-style geo point fields (bbox, radius, nearest,
text+geo). See [`osm-geo-benchmarks.md`](osm-geo-benchmarks.md).

```json
{ "name": "location", "latPath": "lat", "lonPath": "lon" }
```

- `latPath` / `lonPath` — dotted paths to numeric coordinates.
- Each geo field silently registers hidden `location.lat` / `location.lon`
  double doc-values (do not declare colliding number fields).

Tuning: `geoLeafSize` (default `512` points/leaf), `geoPackBytes`.

### Geo capsules and multi-resolution cell indexes

Geo capsules keep a small map-result projection in each geo leaf so viewport
and nearest queries can return display fields without opening document packs:

```json
{
  "geoCapsules": true,
  "geoCapsuleFields": ["id", "name", "type", "lat", "lon"],
  "geoCapsuleDocPageCachePages": 256,
  "geoCapsuleDocPageIndexPreloadMaxBytes": 67108864,
  "geoCodeStorePreloadMaxBytes": 1610612736,
  "geoLeafWorkerBatchLeaves": 16
}
```

The builder preloads the compact capsule page-entry table when it fits, reads
each leaf's source pages in monotonic order, and uses `builderWorkerCount` to
compress geo leaves concurrently while committing packs deterministically.
When the complete code store is larger than `codeStorePreloadMaxBytes`,
`geoCodeStorePreloadMaxBytes` is a separate best-effort budget for coordinates,
cell-index facets, and geo summary fields. This avoids random 16-byte reads from
dominating national builds. OSM's schema selects a 3 GiB complete-store budget
and a 4096-page capsule cache; lower these settings on memory-constrained hosts.

Multi-resolution cell indexes route a facet-constrained radius, viewport, or
route query directly to intersecting point ordinals:

```json
{
  "geoCellIndexes": [{
    "field": "location",
    "facet": "type",
    "levels": [8, 10, 12, 14],
    "blockZoom": 8,
    "codeGroupSize": 16,
    "maxCellsPerQuery": 48,
    "maxFacetValues": 256,
    "includeAll": true,
    "values": []
  }]
}
```

- `levels` — grid zoom levels available to the query planner.
- `blockZoom` — directory grouping zoom, from 0 through the lowest level.
- `codeGroupSize` — facet codes grouped into one packed block.
- `maxCellsPerQuery` — hard planning budget; the runtime chooses a coarser
  configured level when needed.
- `maxFacetValues` — maximum distinct facet values represented.
- `values` — optional allowlist; empty means eligible dictionary values.
- `includeAll` — add a reserved all-point occupancy lane. This lets arbitrary
  names and brands use route/viewport cells even when no category facet is
  known.

Cell indexes contain routing ordinals, not duplicate result payloads. Without
these optional structures the same query falls back to the geo tree.

### `suggest`

Search-as-you-type configuration for the unified authority lexicon. See
[`suggest-benchmarks.md`](suggest-benchmarks.md).

```json
{ "path": "name", "weightPath": "population", "tokenPrefixes": true }
```

- `path` — dotted path to the surface text (arrays produce multiple surfaces).
- `weightPath` — optional numeric ranking weight. Without it, direct surface
  prefixes rank before token-suffix matches, followed by popularity (number of
  documents sharing the surface) and concise-label tie breaks.
- `tokenPrefixes` — default `true`; emit mid-label token keys so "eiffel"
  completes "Tour Eiffel".

Tuning: `suggestMaxTokenKeys` (`4`), `suggestHotListSize` (`64`), and
`suggestMinKeyLength` (`1`). Authority pack, shard, directory, and run-buffer
settings control the shared physical layout.

### `vectors`

Hybrid semantic search via an int8 IVF index. See
[`vector-benchmarks.md`](vector-benchmarks.md).

```json
{ "name": "embedding", "path": "embedding", "dims": 384, "metric": "cosine" }
```

- `dims` — required, 2–4096; must match the query embedding.
- `metric` — only `cosine` is supported (vectors are L2-normalized).
- `path` — the value may be a JSON array of numbers or a base64-encoded
  little-endian Float32 buffer (compact for large corpora).

Tuning: `vectorClusterTargetDocs` (`512`, sets cluster count),
`vectorCoarseDims` (`0` = auto ≈ dims/4), `vectorTrainSample` (`20000`),
`vectorKmeansIterations` (`6`), `vectorPackBytes`.

Query embeddings are produced by the **host**, not the engine — e.g.
transformers.js in the browser with the same model used at build time. See
`examples/wiki-search/scripts/embed.mjs`.

### Typo correction

Main-index typo correction runs only when first-page results are empty or
weak, using bounded vocabulary shard probes.

| Key | Default | Clamp |
| --- | --- | --- |
| `typoMode` | `main-index` | `main-index` \| `off` |
| `typoTrigger` | `zero-or-weak` | `zero` \| `zero-or-weak` |
| `typoMaxEdits` | `2` | 1–3 |
| `typoMaxTokenCandidates` | `8` | 1–32 |
| `typoMaxQueryPlans` | `5` | 1–32 |
| `typoMaxCorrectedSearches` | `3` | 1–8 |
| `typoMaxShardLookups` | `12` | 1–64 |

---

## Build tuning reference

All keys below are top-level in the config. Sizes are bytes. Defaults are the
production `static-large` profile; you rarely need to change these.

### Parallelism and memory

| Key | Default | Effect |
| --- | --- | --- |
| `scanWorkers` | `1` | Worker threads that parse/analyze documents. Set near `cores-2` for large builds. |
| `builderWorkerCount` | `1` | Workers for doc-page, rank-map, and geo-leaf compression plus the reducer fallback. |
| `partitionReducerWorkers` | `0` | Reduce-phase workers (`0` = `builderWorkerCount`). |
| `blockFilterFields` | `null` | Optional doc-value names to include in posting/geo block summaries. Other fields remain exactly filterable and simply skip summary pruning. |
| `codeStoreWriteBufferBytes` | `1048576` | Per-column sequential write buffer used while building the temporary code store. |
| `codeStoreWorkerPreloadMaxBytes` | `1610612736` | Shared-memory budget for reducer doc-value columns. Hot fields are loaded best-effort in summary order. |
| `codeStoreWorkerFallbackCacheChunks` | `8` | Per-worker cache cap when reducer columns cannot all be shared within the preload budget. |
| `scanBatchDocs` | `128` | Documents per worker batch. |
| `partitionReducerInFlightBytes` | `1 GiB` | Backpressure budget for the reducer. |
| `builderMemoryBudgetBytes` | `0` | Advisory memory budget (0 = unbounded). |
| `authorityRunFlushRecords` | `5000` | Max buffered authority and autocomplete rows before they are appended to external runs. |
| `docPackSpoolPreloadMaxBytes` | `256 MiB` | Largest compressed document spool preloaded into RAM during final packing. |
| `docPackSpoolPreloadChunkBytes` | `256 MiB` | Buffer size used to preload large document spools without Node's 2 GiB single-buffer limit. |
| `docLayoutStrategy` | `locality` | `locality` clusters related result payloads; `doc-id` uses bounded sequential packing for very large or slow-volume builds. |
| `docPackSequentialReadBytes` | `64 MiB` | Read-window size used by `doc-id` document packing. |
| `docLayoutSortChunkDocs` | `100000` | Documents per external locality-sort chunk; chunks are merged with a bounded heap. |
| `resumeBuild` | `true` (static-large) | Resume interrupted builds from stage files. |

### Posting index

| Key | Default | Effect |
| --- | --- | --- |
| `targetPostingsPerDoc` | `12` | Per-document term budget (recall vs size). |
| `bodyIndexChars` | `6000` | Max body characters the indexer considers. |
| `postingBlockSize` | `128` | Postings per block (skip granularity). |
| `postingSuperblockSize` | `16` | Blocks per superblock. |
| `externalPostingBlocks` | `true` | Range-address high-df term blocks. |
| `externalPostingBlockMinBlocks` | `4` | Min blocks before a term goes external. |
| `postingGzipLevel` | `6` | Gzip level for posting shards and external blocks (0–9). |
| `packBytes` | `4 MiB` | Target size of each `terms/packs` file. |
| `baseShardDepth` / `maxShardDepth` | `3` / `5` | Term directory shard depth. |
| `targetShardPostings` | `30000` | Postings per term shard. |

### Segments (indexing memory vs merge cost)

`segmentMaxPostings` (`250000`), `segmentMaxBytes` (`64 MiB`),
`segmentMergePolicy` (`tiered-log`), `segmentMergeFanIn` (`128`),
`segmentMergeMaxTempBytes` (`512 MiB`), `segmentMergeMaxDirectoryBytes`
(`64 MiB`), `finalSegmentTargetCount` (`0` = auto). The directory-byte
budget bounds decoded term metadata even when a very high fan-in is selected.

### Documents and doc-values

`docPageSize` (`32`), `docPackBytes` / `docPagePackBytes` (`4 MiB`),
`docValueChunkSize` (`2048`), `docValueSortedPageSize` (`512`),
`filterBitmaps` (`true`), `filterBitmapMaxFacetValues` (`64`).

### Query bundles (optional precomputed top-k)

`queryBundles` (`false`), `queryBundleMaxKeys` (`20000`),
`queryBundleMaxRows` (`64`), `queryBundleMaxTerms` (`3`).

### Telemetry

`buildTelemetrySampleMs` (`1000`), `buildProgressLogMs` (`0` = off),
`buildTelemetryPath` (`""`).

---

## Runtime API

```js
import { createSearch } from "rangefind";           // or "rangefind/browser"
const engine = await createSearch({ baseUrl: "./rangefind/" });
const { results } = await engine.search({ q: "static search", size: 10 });
```

`createSearch` returns `{ manifest, search, count, suggest, vectorSearch,
hydrateRows, loadBuildTelemetry, loadIndexOptimizer, loadSegmentManifest,
loadFacetValues }`.

### `createSearch(options)`

| Option | Default | Clamp | Purpose |
| --- | --- | --- | --- |
| `baseUrl` | `./rangefind/` | — | Base URL the index is served from. |
| `manifestName` | auto | — | Override the manifest filename. |
| `verifyChecksums` | `true` | — | SHA-256 verify range objects before use. |
| `maxPageSize` | `100` | 1–1000 | Hard cap on `size`. |
| `trace` | `false` | — | Attach a per-query fetch/latency trace to `stats`. |
| `multiRangeRequests` | `true` in browsers | — | Batch separated grouped reads from the same immutable object; automatically falls back when unsupported. |
| `multiRangeMaxRanges` | `32` | 2–64 | Maximum byte ranges in one multipart request. |
| `geoCapsules` | manifest | — | Use compact result rows embedded in geo leaves; `false` forces ordinary document hydration. |
| `geoCellIndexes` | manifest | — | Use multi-resolution facet/corridor routing; `false` forces geo-tree traversal. |
| `rangePlans` | see below | — | Per-lane range-coalescing budgets. |
| `topKProofMaxK` | `100` | 1–1000 | Max k eligible for the exact top-k proof. |
| `postingBlockFrontier` | `4` | 1–16 | Posting blocks decoded per frontier batch. |
| `topKProofCheckInterval` | `1` | 1–4096 | Blocks between proof checks (start). |
| `topKProofCheckIntervalMax` | `32` | ≥interval, ≤4096 | Proof-check interval ceiling. |
| `topKProofCheckScoresPerBlock` | `2048` | ≥1 | Amortizes proof checks by volume. |
| `topKBlockBudget` | `128` at ≥1M docs; otherwise `0` | ≥0 | Cap on decoded blocks. Pass `0` for unbounded proof; exact search remains available. |
| `docValueSortPageBatchSize` | `16` | 1–64 | Sorted-tree pages fetched per batch. |
| `geoLeafPageBatchSize` | `16` | 1–64 | Geo leaf pages fetched per batch. |
| `geoTextMaxCandidatePoints` | `100000` | ≥0 | Above this, text+geo verifies via doc-values instead of a tree doc-set. |
| `geoTextSortMaxDf` | `200000` | ≥0 | Posting budget for exact text distance sort. |
| `facetCountMaxChunks` | `32` | ≥1 | Doc-value chunks scanned before facet-count sampling. |
| `docRangePlanner` | `true` | — | Enable doc-range block pruning. |
| `docRangeImpactPlanner` | `true` | — | Enable impact-ordered doc-range planning. |
| `queryBundles` | manifest | — | Use query-bundle top-k when present. |

The **`rangePlans`** map controls HTTP range coalescing per lane. Each entry is
`{ mergeGapBytes, maxOverfetchBytes, maxOverfetchRatio, maxMergedBytes? }`;
adjacent ranges within `mergeGapBytes` are merged into one request, bounded by
the overfetch limits. Remaining separated groups that target the same object
share a multipart byte-range request in capable browser transports. Node,
mobile, disabled multipart, and incompatible HTTP servers transparently use
exact single-range or positional reads instead. Keys and defaults:

| Lane | mergeGapBytes | maxOverfetchBytes |
| --- | --- | --- |
| `default` | 8 KB | 64 KB (ratio 4) |
| `docs` / `docPointers` / `docPagePointers` | 32 KB | 8–32 KB |
| `docPages` / `docValueSortPages` | 64 KB | 64 KB |
| `postingBlocks` | 256 KB | 512 KB (maxMerged 1 MB) |
| `postingBlockFrontier` / `postingDocRanges` | 512 KB | 1 MB (maxMerged 2 MB) |
| `geoLeafPages` | 64 KB | 64 KB |
| `vectorClusterPages` | 64 KB | 64 KB |
| `vectorRefine` | 16 KB | 32 KB (ratio 8) |

Pass a partial object to override individual lanes:
`createSearch({ rangePlans: { postingBlocks: { mergeGapBytes: 512*1024 } } })`.

### `engine.search(params)`

```js
await engine.search({
  q: "bakery",
  page: 1,
  size: 10,
  filters: {
    facets: { category: ["shop"] },
    numbers: { year: { min: 2015, max: 2024 } },
    booleans: { featured: true }
  },
  sort: { field: "year", order: "desc" },   // or "-year"
  geo: { field: "location", near: { lat: 45.5, lon: -73.6, radiusMeters: 2500 }, sort: "distance" },
  vector: queryEmbedding,                    // triggers hybrid fusion
  hybrid: { rrfK: 60 },
  facets: { fields: ["category"], size: 8 }, // per-query facet counts
  highlight: { fields: ["title", "body"], maxChars: 300 },
  exact: false,
  rerank: true,
  includeResults: true,
  trace: false
});
```

**Params:**

- `q` — query string (empty for browse/filter/sort/geo/vector-only).
- `page` / `size` — 1-based page and page size (`size` clamped to `maxPageSize`).
- `filters` — `{ facets: { field: [values] }, numbers: { field: { min, max } },
  booleans: { field: bool } }`. Numeric `min`/`max` accept dates when the field
  is `type: "date"`.
- `sort` — `"field"`, `"-field"`, or `{ field, order: "asc"|"desc" }`. Field
  must be a sortable number/boolean.
- `geo` — `{ field?, near, box, route, corridorMeters, routePositionMeters,
  routeDirection, viewport, boost, sort: "distance"|"route" }`. `route` accepts
  an encoded polyline, GeoJSON LineString/MultiLineString, or coordinate array.
  Route results gain cross-track distance, forward progress, bearing, and the
  closest `rejoinPoint`; corridor/category lookup uses the same multi-resolution
  cells and multipart range transport as box/radius search. `near`
  without `radiusMeters` plus `sort:"distance"` is exact nearest-neighbor; with
  `radiusMeters` it filters. `box` and `near` are mutually exclusive. `boost`
  applies `weight·pivot/(pivot+distance)` to the page window. Results gain
  `distanceMeters` whenever `near` is present.
- `vector` — a query embedding (number array, `Float32Array`, or base64
  float32). Presence switches `search` to hybrid text+vector (reciprocal-rank
  fusion); `q:""` + `vector` is a filtered pure vector search.
- `vectorField` — vector field name when more than one is configured.
- `hybrid` — `{ rrfK: 60, nprobe, refineFactor, refine }` passed to the vector
  lane and fusion.
- `facets` — `["field", ...]` or `{ fields: [...], size: 10 }`; returns
  `response.facets[field] = { values: [{ value, label, count }], exact }`.
- `highlight` — `true` or `{ fields: [...], maxChars: 240 }`; adds
  `result.highlights[field] = { text, ranges: [[start,end], ...] }` with
  analyzer-consistent match ranges (no HTML).
- `exact` — force the exhaustive scan lane instead of the top-k proof.
- `rerank` — dependency/proximity reranking of the top candidates (default on).
- `includeResults` — set `false` to get ranked `{ index, score }` rows without
  hydrating display payloads.
- `trace` — attach a fetch/latency trace to `stats.trace`.
- `shards` — on a sharded root, restrict the query to one or more shard ids or
  hierarchy group labels.
- `rankPrior`, `rankPriorBoost`, `rankPriorOverfetch` — disable or override a
  manifest-configured numeric relevance prior.

**Response:** `{ total, results, page, size, approximate?, correctedQuery?,
corrections?, facets?, stats }`. Each result carries the `display` fields plus
`index` (internal id), `score`, and — depending on the query — `distanceMeters`,
`highlights`, and `hybrid` (per-lane ranks). `approximate: true` marks
early-stopped totals and sampled facet counts.

### Other query methods

- **`engine.count({ q })`** — exact match count for a text-only query (no
  filters/sort/geo/vector). Returns `{ total, totalExact, approximate, stats }`.
- **`engine.suggest({ q, size = 8 })`** — autocomplete. Returns
  `{ q, prefix, suggestions: [{ text, weight, count }], stats }`. `size`
  clamped to 50. Sources are fixed at build time: the `suggest` config when
  present, otherwise a single `title` authority field when available.
- **`engine.vectorSearch({ vector, field?, k = 10, nprobe = 8, refineFactor = 8,
  refine = true, includeResults })`** — pure vector top-k with real cosine
  scores. `k` clamped to 200, `nprobe` to the cluster count, `refineFactor` to
  64.
- **`engine.loadFacetValues(field)`** — the full facet dictionary (all values
  with global document frequencies).
- **`engine.authorityLookup(surface, { size })`** — normalized exact-surface
  lookup in the authority autocomplete lexicon. Sharded-root matches include
  owning `shards`; returns `null` when no suitable lexicon exists.
- **`engine.hydrateRows(rows)`** — hydrate `[index, score]` rows into display
  results (used internally by the generational layer).
- **`engine.loadBuildTelemetry()`**, **`loadIndexOptimizer()`**, and
  **`loadSegmentManifest()`** — lazily load optional diagnostics. These are
  available on a single/generational engine when the corresponding artifacts
  exist and are primarily intended for tooling.

### Response `stats`

`stats` reports which lane ran and how much work it did — useful for tuning.
Notable fields:

- Text: `plannerLane` (`tailProof` | `fullFallback` | `blockBudget`),
  `exact`, `blocksDecoded`, `postingsDecoded`, `skippedBlocks`.
- Geo: `geoLane` (`browse` | `nearest` | `nearestText` | `textDocSet` |
  `textDocValues` | route/cell variants), `geoCandidateLeaves`,
  `geoLeavesVisited`, `geoPointsScanned`, `geoPointsAccepted`, plus route-cell
  and route-ranking counters when applicable.
- Suggest: `suggestLane` (`authority-hot` | `authority-lexicon` |
  `authority-title` for old indexes), `suggestShardsVisited`, and
  `suggestEntriesScanned`.
- Facet counts: `facetCountLane` (`dictionary` | `text-match-set` |
  `chunk-scan` | `global-fallback`).
- Vector/hybrid: `vectorClustersProbed`, `vectorCandidatesScanned`,
  `hybrid`, `hybridPool`.
- With `trace: true`: `stats.trace` with per-bucket fetch counts and bytes.

### Search UI component

`<rangefind-search>` is a drop-in Web Component that wraps `engine.search` /
`engine.suggest` in an accessible combobox. It ships in the package bundle at
`rangefind/element` (`dist/rangefind-search.js`) and registers the
`rangefind-search` custom element on import.

```html
<script type="module" src="/dist/rangefind-search.js"></script>
<rangefind-search src="/rangefind/"></rangefind-search>
```

Build it with `npm run build:element`, which emits `dist/rangefind-search.js`
(self-contained ESM, bundles the runtime, zero dependencies) and
`dist/rangefind-search.css` (the optional theme).

**Light DOM + headless.** The element renders into its own light DOM — no
shadow root — and injects no styling of its own. That is deliberate: the host
page's CSS (Tailwind utilities, global stylesheets, the optional theme) applies
directly to the rendered parts. The one exception is the visually-hidden
`aria-live` status region, whose inline `sr-only` styles are an accessibility
affordance, not theming.

#### Attributes

All optional except `src`.

| Attribute | Default | Purpose |
| --- | --- | --- |
| `src` | — (required) | Index base URL, passed to `createSearch({ baseUrl })`. |
| `placeholder` | `Search` | Input placeholder. |
| `label` | `Search` | `aria-label` for the combobox. |
| `page-size` | `8` | Results per query (`size`, 1–100). |
| `debounce` | `150` | Debounce in ms before searching (0–5000). |
| `min-length` | `1` | Minimum query length before searching. |
| `highlight` | on | `<mark>` matched terms. `highlight="false"` disables. |
| `suggest` | `auto` | Autocomplete. `auto` = on iff the authority index has an autocomplete lexicon or one reusable legacy `title` field; `true`/`false` force it. |
| `router` | off | Sync `?q=` to the URL (`history.replaceState`) and seed from it on load. |
| `open-on-focus` | off | Reopen results when the input regains focus. |
| `hotkey` | off | Press `/` anywhere (outside inputs) to focus the box. |
| `empty-text` | `No results` | Shown when a query has no matches. |
| `loading-text` | `Searching…` | Announced while a query is in flight. |
| `error-text` | `Search is unavailable.` | Shown on an engine/search failure. |

#### Class hooks

Every rendered part carries a stable namespaced class you can target with your
own CSS or the theme:

| Class | Element |
| --- | --- |
| `rf-search` | Host element (root). |
| `rf-search__input` | The `role="combobox"` input. |
| `rf-search__panel` | Popup container (has `hidden` when closed). |
| `rf-search__suggest` | Suggestions `role="listbox"`. |
| `rf-search__suggest-item` | A suggestion `role="option"`. |
| `rf-search__list` | Results `role="listbox"`. |
| `rf-search__option` | A result option (an `<a>` to `result.url`). |
| `rf-search__option-title` | Result title (highlighted). |
| `rf-search__option-snippet` | Result snippet (highlighted). |
| `rf-search__option-url` | Result URL line. |
| `rf-search__mark` | Highlight `<mark>` inside title/snippet. |
| `rf-search__empty` | No-results / error message. |
| `rf-search__status` | Visually-hidden `aria-live` status. |

#### Per-part class injection

Add classes to any part additively (the hook class is always kept and leads).
Two equivalent mechanisms:

- **`*-class` attributes** — one per part, ideal for Tailwind:
  `input-class`, `panel-class`, `suggest-class`, `suggest-item-class`,
  `list-class`, `option-class`, `option-title-class`, `option-snippet-class`,
  `option-url-class`, `mark-class`, `empty-class`, `status-class`, `root-class`.

  ```html
  <rangefind-search src="/rangefind/"
    input-class="w-full border rounded px-3 py-2"
    mark-class="bg-yellow-200"></rangefind-search>
  ```

- **`.classNames` JS property** — an object keyed by the camelCase part name
  (`input`, `panel`, `suggest`, `suggestItem`, `list`, `option`, `optionTitle`,
  `optionSnippet`, `optionUrl`, `mark`, `empty`, `status`, `root`) for
  framework/programmatic use:

  ```js
  document.querySelector("rangefind-search").classNames = {
    input: "w-full border rounded px-3 py-2",
    mark: "bg-yellow-200"
  };
  ```

Merging is deduplicated: `rf-search__input` + attribute classes +
`.classNames.input`, in that order.

#### Events

All `CustomEvent`s bubble and are `composed` so frameworks can listen anywhere:

| Event | `detail` | Fired when |
| --- | --- | --- |
| `rangefind:search` | `{ query, response }` | A search resolves. |
| `rangefind:select` | `{ result }` | A result is chosen (click or Enter). |
| `rangefind:error` | `{ error }` | The engine fails to load or a query throws. |

`.searchOptions` (JS property) merges extra params into every call:
`{ create: {...}, search: {...} }` pass through to `createSearch` and
`engine.search` respectively (e.g. filters, facets, sort).

#### Accessibility

Implements the [WAI-ARIA combobox pattern](https://www.w3.org/WAI/ARIA/apg/patterns/combobox/):
the input is `role="combobox"` with `aria-autocomplete="list"`,
`aria-expanded`, `aria-controls`, and `aria-activedescendant`; the popup lists
are `role="listbox"` and options are `role="option"` with unique ids and
`aria-selected`. Focus stays on the input while navigating. Keyboard map:

| Key | Action |
| --- | --- |
| ArrowDown / ArrowUp | Move the active option (wraps); opens a closed panel. |
| Home / End | Jump to first / last option. |
| Enter | Activate the active option (navigate or fill from a suggestion); with no active option, run the query now. |
| Escape | Close the panel; if already closed, clear the input. |
| Tab | Close the panel and move focus on. |
| `/` | Focus the box (only with the `hotkey` attribute). |

#### Default theme

`dist/rangefind-search.css` is an opt-in, framework-free stylesheet targeting
the hook classes, with light and dark palettes (`prefers-color-scheme`) driven
by CSS custom properties on `.rf-search` (e.g. `--rf-accent`, `--rf-radius`).
Link it only when you are not styling the component yourself:

```html
<link rel="stylesheet" href="/dist/rangefind-search.css">
```

#### React usage

Web Components work in React via plain JSX — set string attributes and attach
events with a ref:

```jsx
import "rangefind/element";

function Search() {
  const ref = useRef(null);
  useEffect(() => {
    const el = ref.current;
    const onSelect = e => console.log(e.detail.result.url);
    el.addEventListener("rangefind:select", onSelect);
    // Object config (Tailwind classes, extra search params) goes via properties:
    el.classNames = { input: "w-full border rounded px-3 py-2" };
    return () => el.removeEventListener("rangefind:select", onSelect);
  }, []);
  return <rangefind-search ref={ref} src="/rangefind/" placeholder="Search…" />;
}
```

---

## OpenStreetMap integration

`rangefind/osm` is browser-safe and operates on the normal Rangefind runtime.
`rangefind/osm/node` and `rangefind/osm/extract` are Node-only build surfaces.
See the [complete feature guide](features.md#openstreetmap-application-features)
and [OSM example](../examples/osm-geo/README.md) for the supported map journeys.

### Build APIs

```js
import { createOsmIndexConfig } from "rangefind/osm";
import {
  augmentOsmWithAddressSources,
  augmentOsmWithRqa,
  buildOsmIndex,
  buildOsmShardedIndex,
  createDelimitedAddressSource
} from "rangefind/osm/node";
import { extractOsmPlaces } from "rangefind/osm/extract";
```

- `createOsmIndexConfig(options)` — returns the schema-v4 generic Rangefind
  config, including display/details, typed constraint facets, geo capsules,
  category cells, wildcard occupancy, authority, suggestion, and prominence.
- `extractOsmPlaces(options)` — bounded, resumable PBF extraction into normalized
  place/address JSONL, including locality enrichment, interpolation, and useful
  closed-way geometry.
- `buildOsmIndex(options)` — build/publish one ordinary OSM index.
- `buildOsmShardedIndex({ shards, output, ... })` — independently build regions
  with frozen scoring stats and publish a federated root.
- `augmentOsmWithRqa(options)` — stream Québec RQA civic/postal data into an OSM
  corpus through the generic enrichment engine.
- `augmentOsmWithAddressSources(options)` — merge any number of normalized
  civic/postal authorities in source-priority order. It suppresses canonical
  duplicates against OSM and earlier providers, and emits one country-scoped
  postal aggregate with centroid, bounds, aliases, source list, sample count,
  and civic-address count.
- `createDelimitedAddressSource(options)` — adapt streaming CSV/TSV input,
  including plain, gzip, and ZIP sources, to the generic record contract. A
  provider can instead use `createJsonlAddressSource(options)`, or expose an
  async `records()` iterator and `normalize()` callback for JSON, GeoJSON,
  database, or API inputs.

```js
const postalSource = createDelimitedAddressSource({
  id: "country-postcodes",
  name: "National postal authority",
  path: "country-postcodes.tsv.gz",
  delimiter: "\t",
  header: false,
  defaults: { kind: "postal_code", country: "CA" },
  mapping: { postcode: 1, city: 2, state: 4, lat: 9, lon: 10 },
  includeAddresses: false,
  license: "CC-BY-4.0",
  attribution: "Postal authority"
});

await augmentOsmWithAddressSources({
  root: "work/quebec",
  osmPath: "work/quebec/data/osm-places.jsonl",
  outputPath: "work/quebec/data/osm-enriched-places.jsonl",
  sources: [postalSource]
});
```

Country-specific lifecycle rules, column names, postcode formatting, and
region filters belong to the adapter. The shared layer owns bounded
streaming, deterministic IDs, on-disk deduplication, aggregation, provenance,
and atomic output.

### Search, autocomplete, and reverse geocoding

```js
import {
  reverseGeocodeOsm,
  searchOsmQuery,
  suggestOsmQuery
} from "rangefind/osm";

const found = await searchOsmQuery(engine, {
  query: "pharmacie Rosemère",
  near: { lat: 45.63, lon: -73.80 },
  limit: 10,
  trace: true
});

const predictions = await suggestOsmQuery(engine, {
  query: "214 rue lib",
  inputOffset: 11,
  near: { lat: 45.64, lon: -73.83 },
  limit: 8
});

const addresses = await reverseGeocodeOsm(engine, {
  lat: 45.5019,
  lon: -73.5674,
  radiusMeters: 5000,
  size: 8,
  resultTypes: ["street_address"],
  locationTypes: ["ROOFTOP", "RANGE_INTERPOLATED"]
});
```

`searchOsmQuery` accepts the core `SearchParams` plus:

| Parameter | Purpose |
| --- | --- |
| `query` / `q` | Natural map query. `query` is convenient for integration callers. |
| `near` | Advisory device/map anchor used for intent resolution and location bias. |
| `limit` / `size` | Requested result count. |
| `reverseGeocode` | `false` keeps coordinate input as a raw marker; an object overrides reverse options. |
| `inputOffset` | Cursor position used by autocomplete. |
| `constraints` | Explicit typed OSM constraint object. |
| `at`, `timeZone` | Clock and IANA timezone for deterministic open-now evaluation. |
| `includeUnknownOpenNow` | Retain unsupported/missing schedules while marking them unknown. |
| `route`, `corridorMeters`, `polylinePrecision` | Route geometry and corridor. |
| `routePositionMeters`, `routeDirection`, `viewport` | Route-aware ranking context. |

`suggestOsmQuery` returns legacy `text`/`weight` fields plus structured
`mainText`, `secondaryText`, `matchedRanges`, `types`, and `selection`.
`selection.shards` can scope the selected follow-up search.

For existing Google Places/Geocoding clients, `rangefind/osm` also exports
`createRangefindMapsAdapter(engine, options)`. It provides promise-based
`autocomplete`, `textSearch`, `nearbySearch`, `geocode`, `reverseGeocode`,
`searchAlongRoute`, and cached `placeDetails` methods with Google-like request
names while preserving Rangefind metadata. It is a migration facade, not a
byte-for-byte Google response clone. See the
[Google Maps migration guide](google-maps-migration.md).

Reverse-geocode `resultTypes` restrict semantic address/place types;
`locationTypes` accepts `ROOFTOP`, `RANGE_INTERPOLATED`, `GEOMETRIC_CENTER`,
and `APPROXIMATE`. The default hard search radius is 5 km, result size is
capped at 25, and `localityRadiusMeters` controls only the bounded locality
fallback.

### Constraints and open-now

```js
const response = await searchOsmQuery(engine, {
  query: "cafe",
  constraints: {
    openNow: true,
    wheelchair: true,
    toiletsWheelchair: true,
    contactless: true,
    delivery: true,
    takeaway: true,
    driveThrough: false,
    outdoorSeating: true,
    internet: true,
    reservation: true,
    free: false
  },
  at: new Date(),
  timeZone: "America/Toronto"
});
```

Only `true` predicates are required; `false` means “do not require,” not a
negative filter. The same predicates can be extracted from supported English
and French phrases. Static predicates are pushed into available facets and
verified against `result.details` after hydration.

```js
import {
  annotateConstraintResult,
  compileOsmConstraintFilters,
  evaluateOpeningHours,
  evaluateOsmConstraints,
  parseOsmConstraints
} from "rangefind/osm";
```

Those lower-level helpers are exported for applications that want to reuse the
parser/evaluator around a custom retrieval flow. `evaluateOpeningHours()`
supports `24/7`, weekday lists/ranges, multiple and overnight intervals,
closures, and later-rule overrides. Calendar/holiday and solar-time syntax is
returned as `unknown` by design.

### Route corridors

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

`route` accepts encoded polyline (precision 5 by default), GeoJSON
LineString/MultiLineString/Feature/FeatureCollection, or coordinate arrays.
Results include:

| Field | Meaning |
| --- | --- |
| `routeDistanceMeters` | Perpendicular distance to the closest route segment. |
| `routeProgressMeters` / `routeProgressRatio` | Position along the complete line. |
| `routeBearingDegrees` | Bearing of the matched segment. |
| `routeRank` | Combined corridor/progress/direction/viewport rank. |
| `rejoinPoint` | Closest `{ lat, lon }` on the route. |
| `routeMatch` | Full segment index, segment progress, distance, bearing, and rejoin details. |

`decodePolyline`, `encodePolyline`, and the generic route helpers are also
exported by `rangefind/osm`. Route search does not calculate driving detours or
directions.

### Geometry and result details

Schema-v3 OSM builds can return a compact `details` object and useful area/line
geometry. Geometry has the form:

```js
{
  type: "Polygon",             // or "LineString"
  encoded: "...",
  precision: 5,
  bbox: [minLon, minLat, maxLon, maxLat]
}
```

Decode `encoded` with `decodePolyline`; polygon rings are closed. Extraction
limits, simplifies, and caps the retained point sequence so geometry remains a
small result capsule rather than a general-purpose vector-tile replacement.

### OSM schema compatibility

Runtime intent code degrades to generic text/geo routing when optional
artifacts are missing, but indexed data cannot be invented. Typed details,
constraint verification, wildcard brand-route cells, and result geometry need
an OSM schema-v4 rebuild. Use `npm run bench:osm-maps:next-index` before
promoting a rebuilt index.

---

## Static site generator adapters

The crawler (["Crawling a static site"](#crawling-a-static-site)) and the
[search component](#search-ui-component) compose with any static site
generator: an adapter runs the crawl right after the generator writes its
build output and copies the component's client assets in alongside it, so
adding search is a matter of installing a package rather than wiring a
post-build script by hand.

Three generators have a real, Node-based plugin system, so those adapters are
genuine npm packages under `packages/`, tested against the real tool's own
build API/CLI:

- **[Astro](packages/rangefind-astro)** — `rangefind-astro` hooks
  `astro:build:done`, and ships a `<RangefindSearch />` component that's a
  thin, faithful passthrough to `<rangefind-search>` (same props, same
  `*-class` injection, headless by default).
- **[Eleventy](packages/eleventy-plugin-rangefind)** — `eleventy-plugin-rangefind`
  hooks the `eleventy.after` event and registers a universal `{%
  rangefindSearch %}` shortcode (works across Nunjucks/Liquid/Markdown/11ty.js)
  that renders the same markup.
- **[Docusaurus](packages/docusaurus-plugin-rangefind)** — `docusaurus-plugin-rangefind`
  hooks the `postBuild(props)` lifecycle and `injectHtmlTags()` to load the
  script/theme site-wide; it deliberately does **not** auto-render a search
  box (that would duplicate it on every page) — drop `<rangefind-search>` into
  a navbar `html` item, an MDX page, or a swizzled component instead.

Two generators have no JS/Python plugin loader of their own, so their
"adapters" are the honest equivalent — a documented recipe plus enough
templating glue that nothing needs hand-editing beyond one include:

- **[Hugo](integrations/hugo)** — a single Go binary with no plugin system.
  The recipe is `hugo && rangefind build public` (Rangefind's crawler needs
  nothing more than a directory of built HTML) plus a copy-paste
  `layouts/partials/rangefind-search.html` that resolves every asset URL
  through Hugo's own `relURL` function, so it respects `baseURL` and subpath
  deploys.
- **[MkDocs](integrations/mkdocs-rangefind)** — a real, pip-installable Python
  plugin (`mkdocs-rangefind`, published to PyPI once released) registered on
  the standard `mkdocs.plugins` entry point. Its `on_post_build` hook shells
  out to the Node CLI (`npx rangefind build <site_dir> --output ... --base-url
  ...`) since there is no way to run Rangefind's Node indexing logic from pure
  Python, and its `on_post_page` hook injects the widget into every rendered
  page automatically (or, in `placement: manual` mode, injects just the
  script/theme tags so you hand-place `<rangefind-search>` in a theme
  override).

Every adapter is verified end to end against the real tool, not a mock: a
real `astro build` / `eleventy.write()` / `docusaurus build`, a
Homebrew-installed `hugo` binary, and a pip-installed `mkdocs` — each crawling
a small fixture site and then confirming, through Rangefind's own runtime
served over real HTTP Range requests, that a search actually returns the
right page. See each package's own README for its full option list; the
component options themselves (attributes, class hooks, events) are documented
once, under ["Search UI component"](#search-ui-component) above, rather than
duplicated per adapter.

---

## Incremental publishing

Add small deltas over an existing index instead of rebuilding it.

```bash
# initial full build
rangefind build --config rangefind.config.json
# later: a delta config whose input is only new/changed documents
rangefind build --config delta.config.json --update
```

Each `--update` writes a new `gen-NNNN/` directory; unchanged generations keep
their exact content-addressed filenames (and CDN cache entries). Documents that
share an external id with a delta document tombstone their old version. Delta
builds replicate the base generations' frozen statistics, so scores stay
comparable across generations.

The runtime detects a generational root automatically — no query changes are
needed. Every query lane merges across generations: text search (with
filters, highlights, facet counts), suggestions, `count`, sorted browse and
text + sort (merged by real doc-value keys), geo (box browse, nearest-first,
radius/boosted text), vector search, and hybrid text + vector (fused at the
merged level, so per-generation ranks never skew the fusion).

Every query pays one fan-out per generation, so fold generations periodically:

```bash
# full corpus as input; removes gen-NNNN/ after verifying coverage
rangefind build --config rangefind.config.json --compact
```

`--update` prints a compaction recommendation once an index reaches 8
generations or 25% tombstoned documents. See
[`incremental-publishing-plan.md`](incremental-publishing-plan.md).

---

## Deployment requirements

- **Static host with HTTP `Range` support** for `.bin.gz` files (GitHub Pages,
  Netlify, S3/CloudFront, nginx, etc. all qualify). The runtime relies on
  `206 Partial Content`.
- **MIME/encoding**: do **not** let the host gzip-transcode range-addressed
  objects (they contain independently compressed members). Current builds use
  the `.bin.gz` suffix so hosts such as GitHub Pages serve their bytes as-is;
  manifests and directories with the same suffix are also pre-gzipped.
- **Immutable caching**: pack and directory filenames are content-addressed, so
  serve them with long-lived `Cache-Control: immutable`. Only the small
  `manifest.min.json` changes between builds.
- **Browser API**: the runtime uses `fetch` with `Range` and
  `DecompressionStream`. For semantic search, the host loads the embedding
  model (e.g. transformers.js); the engine itself ships no model.

---

## Tuning recipes

**Faster builds on big corpora** — raise `scanWorkers` and `builderWorkerCount`
toward `cores-2`, and set `partitionReducerWorkers` > 0. Keep `resumeBuild` on
so interruptions don't restart from zero. `docPackSpoolPreloadMaxBytes`
defaults to 256 MiB; raise it only when the compressed document spool fits
comfortably in spare RAM. External layout merges use a bounded k-way heap, so
smaller `docLayoutSortChunkDocs` no longer causes a linear scan across every
chunk for every document.

**Smaller cold transfer** — lower `targetPostingsPerDoc` and `bodyIndexChars`;
keep `display` fields short with `maxChars`. Widen `rangePlans.postingBlocks`
gaps only if your host penalizes request count more than bytes.

**Better text recall** — raise `targetPostingsPerDoc`; add `phrase`/`proximity`
to title-like fields; add an `authority` sidecar for exact-name rescue.

**Better vector recall** — raise `nprobe` and `refineFactor` at query time
(more transfer), or lower `vectorClusterTargetDocs` at build time (more, smaller
clusters). Full-probe recall is ~0.98; see `vector-benchmarks.md`.

**Exact vs approximate facet counts** — small/filtered result sets count
exactly; very large ones sample. Raise `facetCountMaxChunks` to widen the exact
regime at the cost of more fetches; check `response.facets[f].exact`.

**Autocomplete latency** — the first keystroke costs the root + one hot page;
raise `suggestHotListSize` if you page deeper than the default 8 on
single-character prefixes.

**Cross-check what a query did** — pass `trace: true` and inspect `stats`
(`plannerLane`, `geoLane`, request counts, bytes) rather than guessing.
