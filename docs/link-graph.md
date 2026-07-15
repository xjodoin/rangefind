# Link-Graph Authority (`linkRank`)

Rangefind can fold a page's **link-graph authority** — a PageRank score over the
site's internal hyperlinks — into ranking and sorting. It is the graph feature
that fits a static, no-server engine: the expensive graph work happens **once at
build time**, and the answer ships as an ordinary numeric doc-value. There is no
query-time graph traversal, no adjacency fetches, and no change to the block-max
scoring loop.

## What you get

- A sortable numeric doc-value, `linkRank`, in `[0, 1]` where the most
  authoritative page is `1`.
- An optional query-time ranking prior, applied multiplicatively:

  ```
  score *= 1 + boost * linkRank
  ```

  It fires only on **relevance-ranked** queries (a text query with no explicit
  sort). To let a well-linked page win a near-tie that sits just outside the
  requested page, the engine ranks a bounded wider window, applies the prior,
  and returns the requested slice — so authority can surface a page from beyond
  the page boundary without overriding a clearly stronger textual match. It is
  opt-out per query.

## Crawled sites: automatic

`rangefind build ./dist` (and every framework integration that wraps the
crawler) turns it on with no configuration:

1. `html_extract` collects `<a href>` targets found **inside the indexed content
   region** — links in `nav`/`aside`/`header`/`footer` chrome are excluded by the
   same region logic that keeps their text out of the body.
2. The crawler resolves each href against the page's own URL, drops external and
   non-navigational targets (`mailto:`, `tel:`, fragments, other hosts), and maps
   the rest to internal document ordinals.
3. `src/link_graph.js` runs PageRank over that graph and normalizes the scores.
4. The crawler declares a `linkRank` number (`sortable`), adds a `linkRank desc`
   sort, and sets a `linkGraph` config block with a default `boost` of `0.5`.

Sites with no internal cross-links get no `linkRank` doc-value (a uniform signal
carries no information), so nothing is materialized and no query cost is added.

## Corpora with their own edges: turnkey

If your documents already know their out-links, use the ready-made enricher —
no graph code to write. Give each document an id and a `links` field (an array
of target ids) and point the build at the module:

```bash
rangefind build ./dist --enrich ./examples/link-graph-enrich.mjs
```

It computes PageRank over your edges, attaches `linkRank`, and declares the
`linkRank` number, its sort, and the `linkGraph` boost block for you (the same
shape the crawler emits). The field names and default boost are configurable via
`RANGEFIND_LINK_ID_FIELD`, `RANGEFIND_LINK_FIELD`, and `RANGEFIND_LINK_BOOST`.
Framework integrations that forward an `enrich` option accept the same module
path.

## Fully manual

For a plain `rangefind build config.json` with no enrich hook, precompute the
values yourself with the exported helper and declare them in the config:

```js
import { computeLinkRank } from "rangefind/link-graph";
const ranks = computeLinkRank(adjacency); // adjacency[i] = [target ordinals of doc i]
// write ranks[i] onto each document as `linkRank`, then:
```

```json
{
  "numbers": [{ "name": "linkRank", "path": "linkRank", "type": "double", "sortable": true }],
  "sorts":   [{ "field": "linkRank", "order": "desc" }],
  "linkGraph": { "field": "linkRank", "boost": 0.5 }
}
```

The `linkGraph` block is carried into the runtime manifest; `field` names the
doc-value to read and `boost` is the default query-time multiplier. Note this is
declaration-only — the builder does not compute PageRank itself; the crawler and
the enricher above are what populate `linkRank`.

## Query-time controls

```js
search.search({ q: "widgets" });                      // default boost from linkGraph.boost
search.search({ q: "widgets", linkRankBoost: 1.5 });  // override the weight
search.search({ q: "widgets", linkRankBoost: 0 });    // disable the prior
search.search({ q: "widgets", linkRank: false });     // disable the prior
search.search({ q: "widgets", linkRankOverfetch: 8 });// widen the reranked pool (default 4x)
search.search({ q: "widgets", sort: { field: "linkRank", order: "desc" } }); // sort by authority
```

When the prior fires, `stats.linkRankBoost` is `true`,
`stats.linkRankBoostPool` reports how many candidates were reranked, and
`stats.linkRankBoostWindow` is the returned page size. On a browse (`q: ""`) or
under an explicit `sort`, the prior is skipped entirely and neither stat is set.

## Cost

Measured with `npm run bench:link-rank`:

- **Build:** PageRank over 20k nodes / 228k edges runs in ~6ms — negligible
  against indexing. The computation is a CSR power iteration over typed arrays,
  fully deterministic (same graph in, same scores out), consistent with the
  build's reproducible-output contract.
- **Query:** zero overhead when an index has no `linkGraph` block, or on any
  browse/sorted query. When the prior fires it ranks a bounded candidate pool
  (`min(100, size·page·overfetch)`, default overfetch 4x) instead of just the
  page, plus one batched doc-value fetch over that pool and a resort. The extra
  document payloads are range-coalesced, so the pool is a modest, bounded cost
  paid only on boosted relevance queries.

The same benchmark tracks a related builder speedup: `html_extract` now
precompiles its raw-text-stripping regexes once at module load instead of per
document, making that stage ~2.8x faster on repeated pages.

## Scope and limits

- The boost reranks a **bounded** pool (`min(100, size·page·overfetch)`), so
  authority can surface a page from just outside the requested window but not
  from arbitrarily deep in the ranking — by design, authority sways near-ties,
  it does not resurrect weak matches. Raise `size` or `linkRankOverfetch` to
  widen the reach.
- Automatic `linkRank` is computed by the **crawler** over a full crawl, and the
  boost is wired on the monolithic query path. Generational (`--update`) and
  sharded indexes still carry and sort the doc-value, but do not yet apply the
  query-time prior automatically.
- This is graph-*aware ranking*, not a graph query engine: no multi-hop
  traversal, path queries, or edge lookups at query time — by design.
