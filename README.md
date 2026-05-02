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
- Schema-driven weighted fields.
- BM25F-style field scoring.
- Phrase signals for title/heading fields.
- Adaptive logical term shards.
- Independently compressed logical shards packed into `terms/packs/*.bin`.
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
- Parallel build-time shard reduction with deterministic final pack assembly.
- Browser runtime with adaptive HTTP `Range` coalescing and bounded overfetch.
- Optional typo-tolerance sidecar using delete-key shards and HTTP `Range`
  fetches only when an exact first-page query returns no results.
- Multi-value keyword facets with lazy dictionary loading.
- Range-addressed typed numeric, date, and boolean doc-values for filters and
  sorting.
- Lazy binary sorted doc-value trees for range-pruned top-k sort and doc-order
  early-stop browsing.
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

Demo: https://xjodoin.github.io/rangefind/

```bash
npm install
npm run build:example
npm run test:smoke
npm run serve:example
```

Open `http://localhost:5178/`.

## French Wikipedia Scalability Fixture

Rangefind includes a reproducible French Wikipedia fixture that streams the
official Wikimedia article dump, builds a static site, and benchmarks query
latency, request count, and transfer size:

```bash
npm run build:browser
node scripts/frwiki_fixture.mjs all --limit=50000 --runs=3 --reduce-workers=auto
```

Use `--limit=0` to run against the full dump. The generated site lives at
`examples/frwiki/public/`. The fixture validates text query top-k against the
exact retrieval path by default and records cold request counts, transfer bytes,
runtime posting-block stats, typed filter/sort validation, and scale reports
across multiple Wikipedia sample sizes.

## Build A Custom Index

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
  ],
  "typo": {
    "enabled": true,
    "maxEdits": 2
  }
}
```

`display` controls only the payload returned with search results. Indexed fields
can stay long while returned fields are capped, for example:

```json
{ "name": "body", "path": "body", "maxChars": 640 }
```

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

The benchmark scripts are dependency-free and run against the example static
site. `bench:quality` reports known-item and typo-recovery Hit@k/MRR plus
structured filter/sort checks for facets, dates, booleans, and signed numbers.
`bench:performance` reports query latency, HTTP request count, and transfer size.
`bench:directories` compares global, naive prefix, and paged range-directory
layouts against an existing built index.
`docs/performance-research.md` tracks the top-k retrieval papers currently
guiding format decisions.

## Project Direction

This is the first standalone extraction. The next milestones are:

- Pagefind/Lucene/SQLite benchmark package,
- build-time sparse expansion hooks,
- WASM-free and WASM-assisted runtime comparisons,
- CI release workflow,
- published npm package.
