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
- Lazy binary range directory at `terms/ranges.bin.gz`.
- Browser runtime with coalesced HTTP `Range` fetches.
- Optional typo-tolerance sidecar using delete-key shards and HTTP `Range`
  fetches only when an exact first-page query returns no results.
- Facet and numeric code table foundation.
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

## Build A Custom Index

Create newline-delimited JSON:

```json
{"id":"1","url":"/a","title":"Static search","body":"Search without a server","category":"docs","year":2026}
{"id":"2","url":"/b","title":"Range packs","body":"Use HTTP byte ranges","category":"index","year":2026}
```

Create `rangefind.config.json`:

```json
{
  "input": "docs.jsonl",
  "output": "public/rangefind",
  "idPath": "id",
  "urlPath": "url",
  "display": ["id", "url", "title", "body", "category", "year"],
  "fields": [
    { "name": "title", "path": "title", "weight": 4.5, "b": 0.55, "phrase": true },
    { "name": "body", "path": "body", "weight": 1.0, "b": 0.75 }
  ],
  "facets": [
    { "name": "category", "path": "category" }
  ],
  "numbers": [
    { "name": "year", "path": "year" }
  ],
  "typo": {
    "enabled": true,
    "maxEdits": 2
  }
}
```

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
```

The unit tests cover analyzer normalization, binary varint/fixed-width codecs,
config resolution, shard/range planning, term/code binary round-trips, and an
end-to-end build plus browser-runtime query against a local HTTP `Range` server.

## Project Direction

This is the first standalone extraction. The next milestones are:

- typo-tolerance sidecar extraction,
- Pagefind/Lucene/SQLite benchmark package,
- build-time sparse expansion hooks,
- WASM-free and WASM-assisted runtime comparisons,
- CI release workflow,
- published npm package.
