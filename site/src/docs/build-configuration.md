---
title: Build configuration
lede: One JSON file drives everything the index can do — fields and weights, facets, typed values, geo points, autocomplete, and vectors.
description: Reference for rangefind.config.json — text fields and BM25F weights, facets, numeric and boolean doc-values, geo fields, suggest, vectors, display payloads, and the tuning options that matter.
order: 4
---

Build with a config file:

```bash
npx rangefind build --config rangefind.config.json
```

Paths inside the config resolve relative to the config file. A complete
example, annotated below:

```json
{
  "input": "places.jsonl",
  "output": "public/rangefind",
  "scanWorkers": 8,
  "fields": [
    { "name": "title", "path": "name", "weight": 6.0, "b": 0.4, "phrase": true },
    { "name": "address", "path": "address_search", "weight": 10.0, "b": 0.2 },
    { "name": "body", "path": "description", "weight": 1.0, "b": 0.75 }
  ],
  "alwaysIndexFields": ["title", "address"],
  "facets": [{ "name": "category", "path": "category" }],
  "numbers": [{ "name": "population", "path": "population", "type": "int" }],
  "booleans": [{ "name": "open_now", "path": "open" }],
  "geo": [{ "name": "location", "latPath": "lat", "lonPath": "lon" }],
  "suggest": [{ "path": "name", "weightPath": "population" }],
  "vectors": [{ "name": "embedding", "path": "embedding", "dims": 384 }],
  "display": ["name", "category", "lat", "lon", "url"],
  "meta": { "attribution": "© Your Data Source", "license": "CC-BY-4.0" }
}
```

## Text fields

Each entry in `fields` maps a JSON path to a searchable field:

| Key | Meaning |
| --- | --- |
| `name` | field identifier used in scoring and stats |
| `path` | dot path into each JSONL document (arrays are joined) |
| `weight` | BM25F weight — how much a match here counts |
| `b` | length normalization (0 = ignore field length, 0.75 = classic BM25) |
| `phrase` | emit phrase/proximity signals for multi-word matches |

`alwaysIndexFields` lists fields whose terms are always kept. Other fields
compete for the per-document posting budget (`targetPostingsPerDoc`,
default 12): the highest-scoring terms win, which keeps huge documents from
bloating the index while short titles stay fully searchable.

## Facets, numbers, booleans

- `facets` — keyword dimensions with per-query counts. Use `labelPath` for
  a display label distinct from the value.
- `numbers` — typed columns (`int`, `float`, `date`) usable in range
  filters and sorting. Add `"sortable": true` semantics via
  [query-time sort](../query-api/#sorting).
- `booleans` — true/false columns for filters.

These become compact doc-value columns and filter bitmaps, range-read only
when a query filters or sorts.

## Geo fields

```json
"geo": [{ "name": "location", "latPath": "lat", "lonPath": "lon" }]
```

Each geo field builds a static KD tree plus hidden lat/lon doc-values so text
queries can verify geo filters exactly. Query with `near` (radius or
nearest-first), `box`, distance sort, and distance boosts — see
[Geo search](../geo-search/).

## Autocomplete

```json
"suggest": [{ "path": "name", "weightPath": "population" }]
```

Builds search-as-you-type suggestions into the authority sidecar:
diacritic-folded prefix and mid-token matching, ranked by the weight path
(or corpus frequency), with precomputed hot lists so the first keystroke is
one small fetch.

## Vectors

```json
"vectors": [{ "name": "embedding", "path": "embedding", "dims": 384 }]
```

Embeddings are quantized to int8 in an IVF layout for cosine top-k and
[hybrid search](../query-api/#vectors-and-hybrid). You bring the embedding
model; rangefind stores and searches the vectors.

## Display payloads

`display` lists the fields returned with each hit. Keep payloads lean —
they're what the runtime fetches for every result. String entries pass
through; object entries support `{ "name", "path", "maxChars" }` truncation.

## Provenance

`meta` is copied verbatim into the manifest next to `built_at` — attribution,
license, generator, data version. Runtimes expose it as `engine.manifest.meta`
so a UI can render required attribution (the OSM integration fills in
ODbL-compliant defaults automatically).

## Tuning that matters

| Option | Default | When to touch it |
| --- | --- | --- |
| `scanWorkers` | 1 | set to CPU count − 1 for large builds |
| `targetPostingsPerDoc` | 12 | raise for long documents that need deep recall |
| `analysis` | none | multilingual profile — see [Multilingual](../multilingual/) |
| `scoringStats` | — | frozen stats artifact for [sharded builds](../sharded-indexes/) |
| `resumeBuild` | on (large profile) | interrupted builds continue from checkpoints |

Everything else — pack sizes, codecs, segment merge policy — has measured
defaults; the build writes `debug/build-telemetry.json` if you want to see
exactly where time and bytes went.
