# Sharded OSM indexes: planet-scale search without a search server

Rangefind can index all of OpenStreetMap as a set of independently built,
independently updated geographic shards that merge into one search engine at
query time. The result is a static, CDN-hostable replacement for paid
geocoding/places APIs: no server-side search runtime, range requests against
immutable files, and per-query transfer bounded by the shards a query can
actually touch.

## Why shards, not one giant index

A single planet index hits four walls:

1. **Rebuild wall-clock.** The US extract (32.8M places) builds in ~27
   minutes; a planet corpus is 10–20× that. One monolithic build means every
   OSM refresh pays the full price. Shards rebuild only where the map
   changed.
2. **Query-side transfer.** Facet counting and broad filter lanes pay
   whole-corpus doc-value/bitmap cost. Geographic shards bound that cost per
   shard.
3. **Builder memory ceilings.** The geo tree builder holds 12 bytes/point in
   typed arrays, and doc ids live in `Uint32Array`s: fine per shard, fatal at
   1–2B docs in one index.
4. **Update cadence.** OSM changes continuously. Per-region shards turn diffs
   into cheap, targeted rebuilds.

## Exact cross-shard score comparability

Shard merging is only correct if a document scores identically no matter
which shard indexed it. BM25 impacts are baked at build time from three
corpus-wide inputs, so those inputs are collected once over every shard's
corpus and frozen into a **scoring-stats artifact** every shard build
consumes (`src/scoring_stats.js`):

- **total document count** — the idf denominator,
- **average field lengths** — length normalization *and* per-doc term
  selection depend on them,
- **per-term document frequencies (df)** — collected under the frozen
  averages, stored as a sorted binary table (`rfdf-v1`: block index +
  gzipped blocks), spilled and merged on disk so the planet term space never
  has to fit in one heap.

Shard builds set `scoringStats` in their config. The builder freezes average
lengths after measure and overrides idf inputs during reduce; reduce workers
resolve terms lazily through the df file (binary search + block LRU), so
shard builds keep parallel reducers — unlike generational deltas, which pin
the reducer to the main thread to share an in-heap df map. An analysis-profile
guard refuses to build a shard whose analyzer differs from the stats
artifact, exactly like the generational guard: shards share one term space.

This is the sharded analogue of the generational frozen-stats invariant, and
it is *stronger*: because the stats pass covers the whole corpus, a sharded
index reproduces the monolithic build's rankings **exactly**
(`test/sharded_index.test.js` asserts score equality to 1e-9 against a
monolithic build of the same corpus).

## Build workflow

```js
import { buildOsmShardedIndex } from "rangefind/osm/node";

await buildOsmShardedIndex({
  output: "public/rangefind",
  shards: [
    { id: "quebec",  input: "data/quebec-places.jsonl" },
    { id: "ontario", input: "data/ontario-places.jsonl" }
  ]
});
```

This runs three stages:

1. **Stats pass** (`collectScoringStats`) — two worker-parallel streaming
   passes over every input: corpus totals (+ per-shard coverage bbox), then
   df counting under the frozen averages, spilling sorted runs and merging
   them into the df table.
2. **Shard builds** — ordinary `rangefind build` per region with
   `scoringStats` pointing at the artifact. Each shard is a complete,
   standalone index; the resumable-stage machinery applies per shard (the
   build fingerprint tracks the stats file, so a regenerated artifact
   invalidates stale checkpoints).
3. **Root manifest** (`writeShardedRootManifest` from `rangefind/shards`) —
   a tiny `manifest.json` listing shard paths, totals, and coverage bboxes.

Layout:

```
public/rangefind/
  manifest.json          sharded root (shard list + bboxes)
  shards/<id>/…          one complete index per region
  _scoring-stats/        build artifact — not needed at query time
```

Generic (non-OSM) corpora use the same pieces directly:
`collectScoringStats` → per-shard `build()` with `scoringStats` →
`writeShardedRootManifest`.

### Updating a region

Refresh the region's JSONL, rerun that one shard's build (same stats
artifact), rewrite the root manifest. Idf drifts slowly at corpus scale, so
the stats artifact only needs regenerating on major corpus shifts — and the
fingerprint machinery makes that safe. A shard may itself be generational
(`build --update` inside the shard) — `createSearch` dispatches per shard
manifest, so the layers compose.

## Query-time federation

`createSearch` (browser and node) detects `manifest.shards` and returns the
federated engine. Nothing changes for callers.

- **Lazy shard engines** — opening the index costs one small root-manifest
  fetch; shard engines open on first use, so a planet index with hundreds of
  shards only ever pays for the shards queries touch.
- **Geo routing** — `near` + radius and `box` queries go only to shards whose
  coverage bbox can match (with slack for the flat-earth routing
  approximation; exact haversine verification happens inside each shard).
  Nearest-first without a radius uses an **expanding front**: shards are
  visited in bbox-distance order and the scan stops once the next shard
  cannot beat the page's worst kept distance.
- **Merged lanes** — text top-k merges by score (exactly comparable, see
  above), distance sort by meters, sorted browse by the real key, facet
  counts add, suggest merges by weight, vector/hybrid fuse at the merged
  level (per-shard RRF ranks are not comparable; merged text scores and
  similarities are).
- Responses carry `stats.shards` / `stats.shardsQueried` and each result its
  `shard` id.
- **Explicit shard scoping, multi-level** — `search({ q, shards: ["quebec"] })`
  (also on `count`, `suggest`, `vectorSearch`) restricts a query to the
  named shards before geo routing applies. Names resolve as shard ids *or*
  group labels: shard entries carry a `groups` hierarchy
  (`quebec` → `["canada", "north-america"]`), so `shards: ["canada"]`
  expands to every province shard and `shards: ["europe"]` to a continent;
  ids and groups mix and dedupe. Unknown names throw. Results match a
  standalone engine opened directly on the shard. And since every shard is
  a complete index, you can also bypass federation entirely:
  `createSearch({ baseUrl: ".../shards/quebec/" })`.
- **Hierarchical roots compose** — a shard entry may point at another
  sharded root (`createSearch` dispatches per manifest, recursively): geo
  routing recurses through subtree bboxes, merges nest, and results carry
  the hierarchical shard path (`north-america/canada/quebec`). Scores stay
  identical to the flat topology (tested). Prefer the flat root + `groups`
  while one manifest stays small — hierarchy costs one extra sequential
  manifest fetch per level and earns its keep only with many thousands of
  leaf shards or independently administered/hosted subtrees (a country
  subtree on its own bucket, mounted into a world index).

- **Text routing** — when the root carries a `text_routing` block (built by
  `writeTextRoutingIndex` from `rangefind/shards`, automatic in
  `buildOsmShardedIndex`), text queries stop fanning out to every shard.
  The block is a root-level term → shard-set directory (`text-routing/`):
  the federated engine analyzes the query with the shards' own profile,
  looks up each base term (a couple of small cached range reads), and opens
  only shards that hold enough of the terms to satisfy `minShouldMatch`
  for some query plan (primary or alternate language). Routing is exact for
  well-spelled queries and *fail-open* everywhere else: shards whose ids the
  routing table does not know are always searched, a query no shard supports
  falls back to the full fan-out (so per-shard typo correction still runs),
  and any routing fetch/parse error disables routing for that query. Rebuild
  the artifact whenever shard contents change — terms added by delta
  generations are invisible to a stale table until then. Responses report
  `stats.textRouting` (`terms`, `selected` or `fallback`).

- **Suggest routing** — when the root carries a `suggest_routing` block
  (built by `writeSuggestRoutingIndex` from `rangefind/shards`, or from
  `writeShardSuggestSet` sidecars when shard files are reclaimed after
  upload), autocomplete stops fanning out entirely. The artifact is every
  shard's authority autocomplete lexicon merged into one standard authority
  sidecar at `<root>/authority/` — hot lists make early keystrokes one small
  fetch, longer prefixes cost a directory page plus a partition read, all
  edge-cacheable. Entry rows store federation shard ordinals (authority
  codec v3), so `suggestions[].shards` still reports the region(s) whose
  weight won — the same provenance the fan-out produced — and a selected
  suggestion can scope its follow-up search. `stats.suggestRouting:
  "root-authority"` marks the lane; hot-list answers carry no provenance.
  The root artifact also powers `engine.authorityLookup(surface)`: an
  exact-surface lexicon lookup that returns each matching display with its
  weight and home shards. The OSM integration resolves locality names
  through it ("pharmacy in Birmingham" no longer opens ~200 shards to find
  Birmingham) and falls back to the unscoped search when the scoped page
  misses. Everything is fail-open: no artifact, a broken artifact, or an
  explicit `shards:` scope falls back to the per-shard fan-out. Rebuild the
  artifact whenever shard contents change, alongside text routing.

Current limitations (v1): `count()` stays text-only, like the single engine;
vector and hybrid lanes fan out to every shard (vector neighbors can come
from anywhere), as does suggest when the root has no `suggest_routing`
artifact; root-served address suggestions come from the merged lexicon only
(per-shard address interpolation enrichment does not run at the root);
cross-shard `sort` browse reads the sort key from the hydrated payload, so
sharded sort fields belong in `display`; shard coverage bboxes do not model
antimeridian-crossing regions (split such regions into two shards).

### Incremental shard updates

A stats-frozen shard accepts generational deltas: `rangefind build --update`
with the same `scoringStats` artifact adds a small generation whose impacts
bake from the artifact's df table instead of a per-generation term-directory
scan (and, unlike plain generational deltas, keep parallel reducers). The
result is exactly comparable across generations *and* across shards —
`test/sharded_index.test.js` proves a delta-updated shard identical to a
full rebuild of the final corpus against the same artifact. This works
because any term absent from the artifact is also absent from the base
build, so both paths fall back to the same document frequency. Practical
consequence: a nightly region refresh uploads one small generation instead
of re-shipping the shard. `--compact` is not needed for frozen shards — a
fresh full build with the same artifact is the compaction.

## Planet workflow sketch

1. Partition by Geofabrik region granularity (split dense regions further —
   target 5–40M places per shard). The stats pass computes each shard's bbox.
2. Run the stats pass once over all regions, then build shards — across as
   many machines as you like; shard builds are independent given the
   artifact.
3. Publish to any static host/CDN. All artifacts are immutable and
   content-addressed except the small manifests; a region update uploads only
   that shard's changed packs.
4. Consume OSM replication diffs → mark dirty regions → rebuild those shards
   on a schedule.

### Cost note

A planet-scale sharded index is static storage plus CDN egress — roughly
object-storage pennies per GB-month plus transfer, with per-query transfer in
the tens-to-hundreds of KB (see benchmarks). Compare Google's Geocoding /
Places pricing (roughly $5 per 1,000 requests): a site doing 1M
geocode-style lookups a month pays ~$5,000 there versus CDN egress measured
in single-digit dollars here — and the index works offline, embeds in apps,
and has no key management or rate limits.

## Benchmarks

`scripts/osm_shard_bench.mjs` (`npm run bench:osm-shards`) splits the Québec
corpus (6,095,740 places) into 4 longitude-quantile shards, builds monolithic
and sharded indexes from the same corpus, verifies rankings, and compares
cold/warm latency and transfer per lane (cold = fresh engine, empty caches,
manifest fetches included). Measured 2026-07-13 on a laptop (14 cores):

**Build** (same machine, sequential): monolithic 421.5s; sharded **423.1s
total** including the whole-corpus stats pass and all 4 shard builds — the
frozen-stats protocol costs ~nothing even without distributing, and shard
builds parallelize across machines. Index size: monolithic 9.54 GB, sharded
9.49 GB (4 × ~2.37 GB).

**Correctness**: 7 of 8 lanes returned identical ids with **zero score
drift**. The eighth (broad single term `wood`, thousands of equal-score
matches) returned a *better* page: the sharded merge surfaced a
higher-scoring document the monolithic index's early termination missed,
followed by the same score ties.

**Queries** (cold ms / cold transfer / warm ms):

| Lane | Sharded | Monolithic | Shards queried |
|---|---|---|---|
| nearest (no radius) | 13.5ms / 301KB / 0.4ms | 7ms / 348KB / 0.2ms | 2 of 4 |
| nearest, 2km radius | 5ms / 165KB / 0.2ms | 3.9ms / 348KB / 0.1ms | 1 of 4 |
| nearest + facets | 4.9ms / 165KB / 0.2ms | 4ms / 348KB / 0.2ms | 1 of 4 |
| text + 50km near boost | 262ms / 10.1MB / 42ms | 389ms / 21.5MB / 24ms | 3 of 4 |
| address text | 131ms / 1.7MB / 35ms | 4.2ms / 258KB / 0.04ms | 4 of 4 |
| street text | 166ms / 3.9MB / 45ms | 98ms / 2.6MB / 11ms | 4 of 4 |
| suggest | 22.4ms / 514KB / 10ms | 17.8ms / 361KB / 11ms | all |

These measurements predate root text and suggestion routing. Geo-routed lanes
already transferred *less* than the monolithic index (per-shard
structures are smaller), and the text+geo lane cut cold transfer in half by
skipping a shard. At that point text-only queries paid the fan-out (4 manifests,
4 term directories). Current roots can publish `text_routing` and
`suggest_routing` artifacts built from retained per-shard term/suggest sidecars,
so text and autocomplete open only candidate shards while unknown routing keys
fail open for correctness.

## Provenance

Every manifest carries `built_at` plus a free-form `meta` block (`meta`
config option) with the data attribution and license — for OSM the defaults
satisfy ODbL: `© OpenStreetMap contributors`, `ODbL-1.0`, and the license
URL (plus the RQA CC-BY-4.0 source when enabled). Shard manifests built by
the pipeline additionally record the generator, publisher, Geofabrik source
URL, and the upstream data version (the PBF's `Last-Modified` — the data
vintage, distinct from `built_at`). The sharded root manifest carries the
corpus-level meta, so `engine.manifest.meta` gives a UI everything it needs
to render attribution.
