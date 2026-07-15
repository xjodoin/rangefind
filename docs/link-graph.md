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

## Wikipedia

The Wikipedia example build (`examples/wiki-search/build.mjs`) turns linkRank on
by default: it extracts real `[[wikilink]]` targets from the article wikitext,
runs PageRank over the article graph, and ships `linkRank` as a sortable
doc-value plus the `linkGraph` boost. Disable with `--no-link-rank`.

Two benchmarks exercise this at scale:

- `npm run bench:wiki-link-rank` (`scripts/wiki_linkrank_real.mjs`) streams a
  bounded prefix of a real Wikimedia dump, extracts actual wikilinks, and reports
  the most authoritative articles. On frwiki this yields a textbook ranking —
  France, Europe, the World Wars, countries, languages — and PageRank over
  ~500k edges runs in ~20ms.
- `npm run bench:link-rank-query` (`scripts/link_rank_query_bench.mjs`) builds a
  large synthetic linked site and measures search latency with the boost off vs.
  on; the bounded overfetch adds no measurable cost (range-coalesced payloads).
- `npm run bench:link-rank-geo` (`scripts/link_rank_geo_bench.mjs`) synthesizes a
  corpus with both coordinates and a link graph to confirm the prior composes
  with geo bbox/radius filters (it reranks the geo-filtered relevance results,
  and the overfetch pool auto-shrinks to the filtered set) and is skipped under
  distance sort.

The prior fires under a geo **filter** (bbox/radius), where results are still
relevance-ranked, but never under `geo.sort: "distance"`, where the ordering is
geometric — see the `link_rank_geo` test.

`scripts/wiki_linkrank_bench.mjs` is a download-free variant that approximates
the graph by title mentions over the link-stripped fixtures — useful for a quick
scaling check, but noisier than real wikilinks (it can conflate a common word
with an article of the same name).

## Choosing the boost

The default `boost` of `0.5` is chosen from a sweep, not a guess
(`npm run bench:link-rank-quality`). Across a labeled synthetic corpus:

```
  boost   tieWin   nearTieToHub   relevancePreserved
  0         0%       0%              100%
  0.25    100%     100%              100%
  0.5     100%     100%              100%   <- default
  1       100%     100%                0%
```

`0.5` is the strongest setting that still keeps a clear relevance winner at #1
(`relevancePreserved` = 100%) while letting authority decide exact ties and
near-ties. At `1.0` the prior starts overriding clear relevance. Raise the boost
if you want authority to weigh more heavily; lower it toward `0` to make it a
pure tie-breaker.

## Scope and limits

- The boost reranks a **bounded** pool (`min(100, size·page·overfetch)`), so
  authority can surface a page from just outside the requested window but not
  from arbitrarily deep in the ranking — by design, authority sways near-ties,
  it does not resurrect weak matches. Raise `size` or `linkRankOverfetch` to
  widen the reach.
- The prior applies on all three index topologies — monolithic, generational
  (`--update`), and sharded. In the federated layers each generation/shard
  applies it to its own candidates before the merge, so `linkRank` is normalized
  per partition: "top authority in a delta/shard" is relative to that partition,
  not the whole corpus. For geographic shards that is usually what you want
  (local authority); for generations, `--compact` re-normalizes globally when
  deltas pile up. The merged response reports `stats.linkRankBoost` either way.
- This is graph-*aware ranking*, not a graph query engine: no multi-hop
  traversal, path queries, or edge lookups at query time — by design.
