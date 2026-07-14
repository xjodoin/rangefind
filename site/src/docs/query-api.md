---
title: Query API
lede: One search() call covers text, filters, facets, sorting, pagination, geo, vectors, and hybrid — with TypeScript types included.
description: The rangefind runtime API — createSearch, search parameters and responses, suggest, count, filters, facets, sorting, geo queries, vector and hybrid search, and typo correction.
order: 7
---

```js
import { createSearch } from "rangefind";
const rf = await createSearch({ baseUrl: "/rangefind/" });
```

`createSearch` fetches the manifest and returns an engine. The same call
handles single, [generational](../incremental-updates/), and
[sharded](../sharded-indexes/) indexes. TypeScript declarations ship with
the package.

## search(params)

```js
const res = await rf.search({
  q: "harbor lights",
  page: 1,
  size: 10,
  facets: ["topic"],
  filters: {
    facets: { topic: ["ports"] },
    numbers: { year: { min: 1990 } }
  },
  sort: "-year",
  highlight: true
});
```

| Parameter | Notes |
| --- | --- |
| `q` | query text; analyzed with the index's frozen profile |
| `page`, `size` | pagination (size ≤ 100) |
| `facets` | facet names to count for this query |
| `filters` | `{ facets: {field: [values]}, numbers: {field: {min, max}}, booleans: {field: bool} }` |
| `sort` | `"field"` / `"-field"` over a number or boolean field |
| `geo` | see [geo queries](#geo-queries) |
| `shards` | sharded indexes: scope to shard ids or group labels |
| `vector`, `hybrid` | see [vectors](#vectors-and-hybrid) |
| `highlight` | analyzer-consistent snippets and match ranges |

The response:

```js
{
  total: 1284,            // may be approximate under early termination
  approximate: false,
  results: [{
    id, url, title,       // your display fields
    score,                // BM25F, comparable across generations & shards
    highlights,           // when requested
    distanceMeters,       // geo queries
    shard, generation     // when federated
  }],
  facets: { topic: { exact: true, values: [{ value, count }] } },
  correctedQuery,         // when typo correction rescued the query
  stats: { ... }
}
```

`total` under early termination is a fast lower bound flagged
`approximate`; use `count()` when you need the exact number.

## suggest(params)

Search-as-you-type over the [suggest configuration](../build-configuration/#autocomplete):

```js
const { suggestions } = await rf.suggest({ q: "harb", size: 8 });
// [{ text: "Harbor Lights", weight: 12040, count: 3 }, ...]
```

Exact best-first top-k with per-shard weight proofs — the first keystroke
costs one small fetch from a precomputed hot list.

## count(params)

```js
const { total, totalExact } = await rf.count({ q: "harbor" });
```

Exact totals for text queries without paying for result hydration.

## Geo queries

```js
// everything within 5 km, nearest first
await rf.search({ q: "café", geo: { near: { lat: 46.81, lon: -71.21, radiusMeters: 5000 }, sort: "distance" } });

// bounding box (a map viewport)
await rf.search({ q: "", geo: { box: { minLat: 46.7, maxLat: 46.9, minLon: -71.4, maxLon: -71.0 } } });

// keep relevance ranking, prefer nearby
await rf.search({ q: "pizza", geo: { near: { lat, lon, radiusMeters: 20000 }, boost: { weight: 2, pivotMeters: 2000 } } });
```

Nearest-neighbor sort is **exact**, with early-stop proofs — see
[Geo search](../geo-search/) for how few bytes that costs.

## Vectors and hybrid

```js
// pure ANN
await rf.vectorSearch({ vector: embedding, k: 10 });

// hybrid: BM25F text lane + vector lane, fused by reciprocal rank
await rf.search({ q: "cozy waterfront dinner", vector: embedding, hybrid: { rrfK: 60 } });
```

Bring your own embeddings (`dims` fixed at build time); similarities are
cosine over int8-quantized vectors with full-dimension refinement.

## Typo correction

When first-page results are empty or weak, the runtime probes a bounded set
of vocabulary shards for close spellings and retries the strongest corrected
plans. Successful corrections surface as `correctedQuery` + `corrections`
so your UI can show "showing results for…". Tune or disable with
`typoMode` / `typoTrigger` in the build config.

## Node

The identical engine runs server-side over local directories or remote
indexes with disk caching — see [Node runtime](../node-runtime/).
