# Rangefind

Rangefind is a static search engine for large sites that cannot or should not
run a search server.

It builds a sparse inverted index into static files, then lets the browser query
that index with HTTP `Range` requests. The core idea is simple: keep the logical
search shards small, but pack them into a small number of static files so deploys
and cold starts stay practical.

This repository is the standalone extraction of a production prototype built for
a large thesis corpus.

## What Is Implemented

- JSONL document input.
- Static-site crawler: `rangefind build ./dist` indexes built HTML directly,
  with `data-rangefind-*` attributes for body scoping, metadata, facets, and
  sorts.
- Schema-driven weighted fields.
- BM25F-style field scoring.
- Phrase signals for title/heading fields.
- Adaptive posting segments.
- Independently compressed posting segments packed into `terms/packs/*.bin`.
- Single ESM browser runtime bundle at `dist/runtime.browser.js`.
- Lazy paged binary range directories at `terms/directory-root.<hash>.bin.gz`
  and `terms/directory-pages/*.bin.gz`.
- ZFS-inspired object pointers with SHA-256 verification before decompression.
- Content-addressed immutable pack and directory filenames for CDN caching.
- Exact compressed-object deduplication during index construction.
- Locality-ordered doc packs with a tiny doc-id ordinal table and dense pointer
  records for low-transfer result fetching.
- Dense binary doc-page payload packs for browse, filter, and sort result pages
  where original document ids are clustered.
- Range-packed result payloads with capped display fields.
- Range-addressable posting-block sidecar for high-df terms.
- Optional authority sidecar for exact title/entity/alias rescue without
  changing the main inverted index.
- Range-packed binary facet dictionaries for high-cardinality metadata.
- File-backed build pipeline with immutable posting segments, selected-term
  spools, raw/compressed document spools, heap-based segment merge, and sampled
  build telemetry.
- Minimal runtime manifest with lazy full-manifest and telemetry sidecars.
- Browser runtime with adaptive HTTP `Range` coalescing and bounded overfetch.
- Main-index typo correction that uses bounded vocabulary shard probes and
  executes only the strongest corrected searches when first-page results are
  empty or weak.
- Multi-value keyword facets with lazy dictionary loading.
- Range-addressed typed numeric, date, and boolean doc-values for filters and
  sorting.
- Lazy binary sorted doc-value trees for range-pruned top-k sort and doc-order
  early-stop browsing.
- Lucene-style geo point fields with a range-addressed static KD tree:
  bounding-box and radius filters, exact nearest-neighbor distance sort with
  early-stop proofs (with or without a text query), text-plus-geo filtering,
  per-cell filter summaries, and distance boosts.
- Search-as-you-type autocomplete from a range-addressed suggestion sidecar:
  diacritic-folded prefix and mid-label token matching, popularity or custom
  weights, exact best-first top-k with per-page weight proofs, and
  precomputed hot pages so a first keystroke costs one small fetch.
- Hybrid semantic search from a range-addressed IVF vector index: int8
  quantized embeddings, coarse-dimension candidate pages plus a fixed-width
  full-dimension refine store, cosine top-k, and reciprocal-rank fusion with
  the text lane — no server, no vector database.
- Analyzer-consistent snippets and highlight ranges on search results.
- Multilingual analysis (`analysis` config): per-document language detection
  or an explicit language field, per-language light stemmers and stopwords
  for 20+ languages, script-aware Unicode folding (Latin, Greek, Cyrillic,
  Arabic, Hebrew, Devanagari), dictionary-free CJK bigram tokenization that
  is deterministic across Node and every browser, cross-language query
  expansion with index-evidence base-plan selection, and the whole profile
  frozen into the manifest so query analysis always matches the index.
- Incremental publishing: `rangefind build --update` adds small delta
  generations over an existing index — unchanged pack bytes keep their
  content-addressed names (and CDN cache entries), replaced documents
  tombstone their old version, and the runtime merges generations with
  scores that stay exactly comparable.
- Per-query facet counts with exact-or-flagged semantics: dictionary-backed
  global counts, exact counts over budgeted match sets, and bounded
  chunk-sampled estimates for very large result sets.
- OpenStreetMap example and benchmark fixture with exhaustive geo oracles.
- Tiny runnable example.

## Why This Exists

Most browser search libraries assume the browser downloads a whole index, or
they optimize for smaller sites. Rangefind is for the case where you want:

- no backend service,
- no hosted search provider,
- static hosting compatibility,
- measurable retrieval quality,
- low request count despite many logical shards,
- and an index format designed around top-k retrieval.

## Quick Start

Demos: https://xjodoin.github.io/rangefind/ — an OpenStreetMap map search
(175k places with autocomplete, viewport geo queries, and nearest-neighbor)
and the minimal example, both served as pure static files.

```bash
npm install
npm run build:example
npm run test:smoke
npm run serve:example
```

Open `http://localhost:5178/`.

## Crawl A Static Site

If you already have a built static site (from any generator), point Rangefind at
the output directory and it indexes the HTML directly — no JSONL to author:

```bash
npx rangefind build ./dist
```

This crawls every `.html`/`.htm` file under `./dist`, extracts the title,
headings, and main body text, and writes the index to `./dist/rangefind` (change
it with `--output`). Result URLs default to site-root paths (`--base-url` sets a
prefix or origin, e.g. `--base-url https://example.com/`). Deploy the site and
load the index with `createSearch({ baseUrl: "/rangefind/" })`.

Content selection follows the document: the crawler indexes `<main>`, else
`<article>`, else `<body>`, and always drops `<script>`, `<style>`, `<nav>`, and
`<aside>`. `<header>`/`<footer>` are dropped only when the fallback `<body>`
region is used (no `<main>`/`<article>`), so an in-article post header and its
`<h1>` are still indexed. Opt-in `data-rangefind-*` attributes
(mirroring Pagefind's vocabulary) tune it:

| Attribute | Effect |
| --- | --- |
| `data-rangefind-body` | Index only the text inside marked elements. |
| `data-rangefind-ignore` | Drop this subtree (on `<html>`/`<body>` skips the page). |
| `data-rangefind-meta="name"` | Capture element text as metadata `name` (`"name:attr"` reads an attribute). |
| `data-rangefind-filter="key"` | Add the element text as a facet value under `key`. |
| `data-rangefind-sort="key"` | Use the element text as a sortable value `key`. |

Per-document language comes from `<html lang="…">` and the page description from
`<meta name="description">`. See the
[reference guide](docs/reference.md#crawling-a-static-site) for the full rules.

## Full Wikipedia Search Site

The `examples/wiki-search` project is a fuller static search application for
Wikimedia article dumps. It defaults to the latest English Wikipedia articles
dump and can run on a bounded sample or the full dump:

```bash
npm run build:wiki-site -- --limit=50000
npm run serve:wiki-site
```

Use `npm run build:wiki-site:full` to build the full default dump with the
bounded body cap, or `npm run build:wiki-site:fr:full` for the full French
Wikipedia dump. The generated site lives at `examples/wiki-search/public/` and
keeps the same static hosting requirement as every Rangefind index: the host
must support HTTP `Range` requests for `.bin` files.

## French Wikipedia Scalability Fixture

Rangefind includes a reproducible French Wikipedia fixture that streams the
official Wikimedia article dump, builds a static site, and benchmarks query
latency, request count, and transfer size:

```bash
npm run build:browser
node scripts/frwiki_fixture.mjs all --limit=50000 --runs=3
```

For runtime-only changes, reuse the existing generated index and rerun just the
request/transfer benchmark:

```bash
node scripts/frwiki_fixture.mjs runtime-bench --limit=50000 --runs=3
```

Use `--limit=0` to run against the full dump. The generated site lives at
`examples/frwiki/public/`. The fixture validates text query top-k against the
exact retrieval path by default and records cold request counts, transfer bytes,
runtime posting-block stats, typed filter/sort validation, and scale reports
across multiple Wikipedia sample sizes.
Extracted JSONL is cached under `examples/frwiki/data/`, so later runs with the
same dump/body cap and an equal or smaller limit do not stream the dump again.
Each generated index includes `manifest.min.json`, `manifest.full.json`, and
`debug/build-telemetry.json`. The full diagnostic `manifest.json` is still
written for local inspection and records phase timings, sampled memory peaks,
CPU time, disk byte deltas, and segment counters.

## Build A Custom Index

> For the complete configuration schema, every tuning knob, the full runtime
> API, and deployment requirements, see the
> **[reference guide](docs/reference.md)**. The walkthrough below is the
> quick version.

Create newline-delimited JSON:

```json
{"id":"1","url":"/a","title":"Static search","body":"Search without a server","category":"docs","tags":["static","range"],"year":2026,"published":"2026-01-10","featured":true}
{"id":"2","url":"/b","title":"Range packs","body":"Use HTTP byte ranges","category":"index","tags":["range"],"year":2026,"published":"2026-02-01","featured":false}
```

Create `rangefind.config.json`:

```json
{
  "input": "docs.jsonl",
  "output": "public/rangefind",
  "idPath": "id",
  "urlPath": "url",
  "indexProfile": "static-large",
  "targetPostingsPerDoc": 12,
  "bodyIndexChars": 6000,
  "alwaysIndexFields": ["title", "category"],
  "typoMode": "main-index",
  "typoTrigger": "zero-or-weak",
  "typoMaxEdits": 2,
  "typoMaxTokenCandidates": 8,
  "typoMaxQueryPlans": 5,
  "typoMaxCorrectedSearches": 3,
  "typoMaxShardLookups": 12,
  "display": ["id", "url", "title", "body", "category", "tags", "year", "published", "featured"],
  "fields": [
    { "name": "title", "path": "title", "weight": 4.5, "b": 0.55, "phrase": true },
    { "name": "body", "path": "body", "weight": 1.0, "b": 0.75 }
  ],
  "authority": [
    { "name": "title", "path": "title" },
    { "name": "aliases", "path": "aliases" }
  ],
  "facets": [
    { "name": "category", "path": "category" },
    { "name": "tags", "path": "tags" }
  ],
  "numbers": [
    { "name": "year", "path": "year", "type": "int" },
    { "name": "published", "path": "published", "type": "date" }
  ],
  "booleans": [
    { "name": "featured", "path": "featured" }
  ]
}
```

`display` controls only the payload returned with search results. Indexed fields
can stay long while returned fields are capped, for example:

```json
{ "name": "body", "path": "body", "maxChars": 640 }
```

For large static corpora, `targetPostingsPerDoc` is the body-term budget.
`bodyIndexChars` caps only the text considered by the indexer, while display
payload size stays controlled by `display` entries. Terms from
`alwaysIndexFields` are indexed before the body budget is applied.

`typoMode: "main-index"` uses the normal term vocabulary for typo candidates
instead of building a separate typo sidecar. Use `typoMode: "off"` to disable
correction.

`authority` fields build a separate packed sidecar for canonical labels such as
titles, entity names, product names, slugs, and aliases. The runtime first tries
a diacritic-preserving surface-exact key, then falls back to folded exact and
token keys only when needed, so common title rescue stays precise and cheap
without forcing all label logic into the BM25 posting lists.

Build:

```bash
npx rangefind build --config rangefind.config.json
```

Query in the browser:

```js
import { createSearch } from "rangefind";

const engine = await createSearch({ baseUrl: "/rangefind/" });
const result = await engine.search({ q: "static search", size: 10 });
console.log(result.results);
```

Filters and sort use range-addressed doc-value columns:

```js
const result = await engine.search({
  q: "static search",
  filters: {
    facets: { tags: ["range"] },
    numbers: { published: { min: "2026-01-01" } },
    booleans: { featured: true }
  },
  sort: { field: "published", order: "desc" }
});
```

Geo fields index one point per document into a range-addressed static KD tree
(Lucene `LatLonPoint`-style, E7 fixed precision):

```json
{
  "geo": [{ "name": "location", "latPath": "lat", "lonPath": "lon" }]
}
```

```js
// Places inside a map viewport (empty-query browse).
await engine.search({ q: "", geo: { box: { minLat: 45.45, maxLat: 45.62, minLon: -73.7, maxLon: -73.45 } } });

// Exact nearest neighbors, sorted by distance with early-stop proofs.
await engine.search({ q: "", geo: { near: { lat: 45.5017, lon: -73.5673 }, sort: "distance" } });

// Exact nearest matches for a text query ("closest bakeries first").
await engine.search({ q: "bakery", geo: { near: { lat: 45.5017, lon: -73.5673 }, sort: "distance" } });

// Text search restricted to a radius, plus a Lucene-style distance boost.
await engine.search({
  q: "bakery",
  geo: {
    near: { lat: 45.5017, lon: -73.5673, radiusMeters: 5000 },
    boost: { weight: 2, pivotMeters: 500 }
  }
});
```

Results include `distanceMeters` whenever `geo.near` is present. Radius and
box filters are exact (bounding-box prune plus Haversine verification), and
`stats` reports the tree traversal (`geoLane`, `geoLeavesVisited`,
`geoPointsScanned`, ...). See `examples/osm-geo/` for an OpenStreetMap-scale
example and benchmark.

Search-as-you-type suggestions come from a dedicated static sidecar:

```json
{
  "suggest": [{ "path": "title" }]
}
```

```js
const { suggestions } = await engine.suggest({ q: "boul", size: 8 });
// [{ text: "Boulangerie Fischer", weight: 12, count: 12 }, ...]
```

Matching is prefix-based over diacritic-folded keys ("montre" finds
"Montréal") and covers mid-label tokens ("eiffel" finds "Tour Eiffel").
Ranking uses an optional `weightPath` (for example population or importance)
and falls back to popularity — how many documents share the surface. Each
keystroke costs at most a few small range requests; repeat keystrokes in a
session are usually served entirely from cache.

Facet counts for filter UIs come back with the search response:

```js
const result = await engine.search({ q: "static", facets: ["tags"] });
// result.facets.tags = { values: [{ value: "range", label: "range", count: 12 }, ...], exact: true }
```

Hybrid semantic search takes a query embedding from the host (for example
transformers.js in the browser) and fuses it with the text lane:

```js
const result = await engine.search({ q: "map search", vector: queryEmbedding });
const nearest = await engine.vectorSearch({ vector: queryEmbedding, k: 10 });
```

Typo fallback is automatic. For example, if `statik search` has no exact
first-page hits but `static search` does, the response includes:

```js
{
  correctedQuery: "static search",
  corrections: [{ from: "statik", to: "static", surface: "static" }]
}
```

## Static Hosting Requirement

The runtime expects the host to support HTTP `Range` requests for `.bin` files.
GitHub Pages supports this. The included local server also supports it.

## Development

```bash
npm run check
npm test
npm run test:smoke
npm run test:all
npm run bench:quality
npm run bench:performance
npm run bench:directories -- --index=/path/to/public/rangefind
```

The unit tests cover analyzer normalization, binary varint/fixed-width codecs,
config resolution, shard/range planning, term/code binary round-trips, and an
end-to-end build plus browser-runtime query against a local HTTP `Range` server.
The end-to-end build test also verifies emitted build telemetry and file-backed
spool counters in the manifest.

The benchmark scripts are dependency-free and run against the example static
site. `bench:quality` reports known-item and typo-recovery Hit@k/MRR plus
structured filter/sort checks for facets, dates, booleans, and signed numbers.
`bench:performance` reports query latency, HTTP request count, and transfer size.
`bench:directories` compares global, naive prefix, and paged range-directory
layouts against an existing built index.
`docs/performance-research.md` tracks the top-k retrieval papers currently
guiding format decisions.

## Project Direction

Planned next milestones:

- Pagefind/Lucene/SQLite benchmark package,
- build-time sparse expansion hooks,
- WASM-free and WASM-assisted runtime comparisons,
- CI release workflow + published npm package.

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE) © Xavier Jodoin
