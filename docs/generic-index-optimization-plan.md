# Generic Index Optimization Plan

Date: 2026-05-03
Status: research-reviewed, core-first refactor

This plan covers the next generic optimization work for Rangefind after the
segment-based builder and posting-segment runtime refactor. It is intentionally
not a French Wikipedia benchmark plan. The target is a portable static-search
engine that scales across documentation sites, product catalogs, issue trackers,
knowledge bases, blogs, and other mostly-static datasets.

The direction is core-first. Adding narrow extra files for every slow query
shape can become a hack. The next implementation should improve the main index
format, scheduler, proof model, compression, and field summaries first. Extra
materialized structures are deferred until benchmark evidence shows that the
core format cannot cover a generic query class cleanly.

## Position

Sidecar-first optimization is rejected for this phase.

Rangefind may still split large immutable data into multiple physical files for
static hosting and range requests. That is a storage layout detail, not a query
hack. The architectural rule is:

```text
generic query behavior belongs in the core index format and runtime planner;
optional materializations must be derived, budgeted, auditable, and removable.
```

Start with core improvements:

- Stronger block-max/WAND-style proof.
- Multi-level superblocks inside posting metadata.
- Better range-fetch scheduling.
- Codec and doc-id layout optimization.
- Filter and sort summaries integrated into core blocks/pages.
- Generic benchmark matrix beyond wiki.

Defer until after those are measured:

- Champion/top-doc materializations.
- Extra phrase or n-gram materializations.
- Term-by-sort overlays.
- Learned-sparse import lanes.

## Research Review Verdict

The plan is aligned with current information-retrieval research if the core
retrieval algorithm is treated as the foundation. The strongest direction is a
block-max, upper-bound, and access-cost architecture where exact top-k proof is
the default. Optional accelerators can exist later, but only when they prove
exactness or clearly report approximation.

Research implications for Rangefind:

- Block-max/WAND and MaxScore-style pruning are production-proven core
  techniques, not optional shortcuts.
- Multi-level block and superblock summaries are a better first move than
  adding separate query-specific structures.
- Static browser search must cost range requests, transferred bytes, and
  JavaScript decode time, not only scoring CPU.
- Compression/layout choices must be optimizer decisions because they affect
  both transfer and random access.
- Champion lists are useful but application-dependent; they are not the first
  correctness path.

## Research Anchors

- [Efficient In-Memory Inverted Indexes: Theory and Practice, SIGIR 2025](https://jmmackenzie.io/publication/sigir25-tutorial/):
  modern high-performance inverted indexes are built from compression,
  traversal strategies, pruning, and learned-sparse/hybrid retrieval awareness.
- [Ding and Suel, Faster Top-k Document Retrieval Using Block-Max Indexes, SIGIR 2011](https://research.engineering.nyu.edu/~suel/papers/bmw.pdf):
  block-level maximum impact scores enable safe early termination for
  disjunctive top-k retrieval.
- [Grand et al., From MaxScore to Block-Max WAND, ECIR 2020](https://cs.uwaterloo.ca/~jimmylin/publications/978-3-030-45442-5_3.pdf):
  Lucene adopted block-max WAND because it produced multiple-fold real
  performance gains, but production systems need flexible upper-bound metadata.
- [PISA documentation](https://pisa.readthedocs.io/en/latest/index.html):
  state-of-the-art experimentation combines compression methods, query
  processing algorithms, and document reordering.
- [Ottaviano and Venturini, Partitioned Elias-Fano Indexes](https://openportal.isti.cnr.it/doc?id=people______%3A%3Aa8650eecc6f8d6bd1d56ec87eda3c522):
  partitioned Elias-Fano improves compression while preserving efficient random
  access and search over posting lists.
- [Crane et al., A Comparison of Document-at-a-Time and Score-at-a-Time Query Evaluation, WSDM 2017](https://jmmackenzie.io/publication/wsdm17/):
  WAND, BMW, and impact-ordered JASS have different tail-latency and retrieval
  depth behavior; Rangefind should benchmark traversal choices rather than
  assume one layout wins everywhere.
- [Lin and Trotman, Anytime Ranking for Impact-Ordered Indexes, ICTIR 2015](https://cs.uwaterloo.ca/~jimmylin/publications/Lin_Trotman_ICTIR2015.pdf):
  impact-ordered postings are useful for latency-bounded approximate lanes, but
  approximate behavior must be explicit.
- [Mallia et al., Faster Learned Sparse Retrieval with Block-Max Pruning, SIGIR 2024](https://arxiv.org/abs/2405.01117):
  block-max pruning remains relevant when sparse term distributions change under
  learned sparse retrieval.
- [Carlson et al., Dynamic Superblock Pruning for Fast Learned Sparse Retrieval, SIGIR 2025](https://arxiv.org/abs/2504.17045):
  superblock-level pruning can avoid visiting child blocks; this maps naturally
  to Rangefind range packs.
- [Bast et al., IO-Top-k, VLDB 2006](https://www.vldb.org/conf/2006/p475-bast.pdf):
  top-k systems need explicit scheduling between sequential scans and random
  accesses; in Rangefind that means request groups, byte ranges, and doc-value
  lookups must be costed.
- [Stanford IR book: Champion Lists](https://nlp.stanford.edu/IR-book/html/htmledition/champion-lists-1.html):
  champion lists are useful top-doc shortcuts, but their window size is
  application-dependent and can be too small for arbitrary top-k requests.

## Design Principles

- Core first: improve `rfsegpost` layout, posting metadata, scheduler,
  compression, and proof before adding derived materializations.
- Select optimizations from corpus statistics, field configuration, and measured
  cost models, not from benchmark query strings.
- Keep the default index static-hosting friendly: immutable files,
  range-addressable packs, deterministic browser execution, and no server cache
  requirement.
- Preserve exactness for default text search. Fast approximate lanes can exist
  only when the response explicitly reports approximation.
- Prefer one strong new format over compatibility bridges; this is a greenfield
  project and old readers do not need to be preserved.
- Enforce a byte budget for optional metadata. Budgeting applies inside the core
  format too, not only to deferred materializations.
- Make all plans auditable through `debug/index-optimizer.json`,
  `manifest.build`, and benchmark rows that show latency, transfer, requests,
  decoded blocks, proof lane, and fallback reason.

## Current Generic Gaps

The 50k/100k/500k benchmark matrix shows that query bundles and block-max
posting packs work, but generic broad-query and sorted-query lanes still have
costly fallbacks:

- High-df single-term queries can still decode many posting blocks.
- Larger top-k windows can exhaust current proof metadata and fall back to broad
  scans.
- Text queries sorted by doc-value fields can require large value fetches.
- Filtered broad terms can need full field-value data before proving a small
  page.
- Transfer and request grouping are not yet modeled as first-class optimizer
  costs.
- The plan does not yet benchmark enough non-wiki corpus families.

These are core format and planner gaps. They should be addressed before adding
new query-shape materializations.

## Progress

- [x] 2026-05-03: Added `rfoptimizer-v1` scaffold output at
  `debug/index-optimizer.json`, exposed it through `manifest.optimizer`, and
  kept it lazy in `manifest.min.json`.
- [x] 2026-05-03: Added runtime loading for the optimizer report through
  `loadIndexOptimizer()`.
- [x] 2026-05-03: Added top-k proof/fallback runtime stats:
  proof attempts, successes, candidate-count failures, score-bound failures,
  fallback reason, threshold, and max outside potential.
- [x] 2026-05-03: Added proof-stat regression coverage for high-df tail
  exhaustion, filtered text, multi-term text, and large-page planner fallback.
- [x] 2026-05-03: Added posting block `maxImpactDoc` metadata, now carried in
  `rfsegpost-v3`, and used it to prove equal-score tails by deterministic
  doc-id order.
- [x] 2026-05-03: Added tie-bound proof regression coverage showing a tie-heavy
  high-df term can stop at the requested top-k instead of exhausting.
- [x] 2026-05-03: Added term-level superblock metadata inside the `rfsegpost-v3`
  posting segment directory, including row counts, max-impact tie metadata,
  merged block-filter summaries, manifest stats, and optimizer report fields.
- [x] 2026-05-03: Refactored the runtime block frontier to schedule inside
  active superblock boundaries, prune whole superblocks by filter summaries, and
  report considered, skipped, and decoded superblocks.
- [x] 2026-05-03: Added remaining-term upper-bound proof stats and explicit
  filter-unknown metadata so omitted high-cardinality filter summaries fall back
  to doc-value filtering instead of unsafe block pruning.
- [x] 2026-05-03: Added optimizer-controlled posting layout auto mode for
  `postingBlockSize` and `postingSuperblockSize`, resolved from corpus scan
  statistics and reported through manifest stats plus `debug/index-optimizer.json`.
- [x] 2026-05-03: Added `rfsegpost-v3` per-block codec metadata and a measured
  compact impact-run posting block codec selected by auto mode only when it is
  smaller than pair-varint baseline bytes.
- [x] 2026-05-03: Added a measured dense impact-bitset posting block codec for
  clustered doc-id blocks, also selected only when it beats the baseline.
- [x] 2026-05-03: Added measured partitioned-delta doc-id block coding for
  sparse regular impact groups where fixed-width deltas beat both bitsets and
  varint impact runs.
- [x] 2026-05-03: Integrated block filter summaries deeper into the runtime
  proof path by counting blocks whose numeric/boolean summaries prove every row
  passes and skipping unnecessary doc-value filtering for those blocks.
- [x] 2026-05-03: Added a sort-aware text planner lane that scans sorted
  doc-value pages in sort order, stops when the next page cannot tie the
  boundary sort value, and reports `sortPageText` stats.
- [x] 2026-05-03: Added `npm run bench:matrix` with an `rfbenchmatrix-v1`
  report spanning built fixture families, runtime planner stats, request/byte
  metrics, and codec byte-saving counters; stale or missing fixtures are
  reported without blocking the rest of the matrix unless `--fail-missing` is
  used.
- [x] 2026-05-03: Added `rfbenchpromotion-v1` promotion gates inside the
  benchmark matrix. The gate checks fixture health, corpus-family coverage,
  query-shape coverage, planner exactness, codec/layout invariants, and optional
  `--baseline=<rfbenchmatrix-v1.json>` comparisons before any default `auto`
  threshold is considered promotable. Matrix reports can be saved with `--out`
  so baseline/current comparisons are reproducible without shell redirection.
- [x] 2026-05-03: Rebuilt the ignored 100k `frwiki` fixture from the reusable
  cached JSONL. The default two-family matrix now passes fixture health,
  family/query coverage, planner exactness, and codec/layout invariants; it
  remains `needs-baseline` until compared with a saved baseline report.
- [x] 2026-05-03: Split sorted doc-value metadata into
  `doc-values/sorted/manifest.json.gz` and lazy-loaded it for sort-only and
  q+sort paths. On the rebuilt 100k `frwiki` fixture, the default matrix
  sort-only browse row dropped from about 197 KB to 16.4 KB cold transfer, and
  q+sort stays on `sortPageText` at about 22.1 KB instead of fetching the full
  manifest.
- [x] 2026-05-03: Split facet dictionary metadata into
  `facets/manifest.json.gz` and changed filtered text/browse planning to load
  facet dictionaries plus the existing doc-value manifest directly, avoiding
  `manifest.full.json` for core filter paths.
- [x] 2026-05-03: Ran the refreshed two-family matrix against a saved
  pair-varint baseline built with the same `rfsegpost-v3` core format and
  `codecs.mode = "off"`. The promotion gate matched 13 rows, found zero
  material regressions after applying the 10 ms p95 noise floor, and returned
  `promote` with winning families `docs-small` and `encyclopedia`. The 100k
  `frwiki` auto build saved 27,251,885 posting-block bytes and reduced the
  generated index from about 219 MB in the pair-varint baseline to about 203 MB.
- [x] 2026-05-03: Added `rfbenchdeferred-v1` to the benchmark matrix so
  champion windows, phrase materialization, term-sort overlays, and
  learned-sparse import are reviewed from promoted-core evidence. The current
  two-family baseline comparison marks champion windows and phrase
  materialization `not-recommended`, learned-sparse import `deferred`, and
  term-sort materialization `watch-core-first` because one q+sort row still
  decodes many postings even though transfer stays low.
- [x] 2026-05-03: Improved the core q+sort posting scheduler by making the
  sorted-text lane page-driven. It now scans sorted doc-value pages first, uses
  existing posting block and superblock numeric/boolean summaries as exact
  overlap filters for the current sort page, and decodes only candidate posting
  blocks before classifying that page. On the 100k `frwiki` matrix row for
  `Paris` sorted by `revisionDate`, decoded posting blocks dropped from 101 to
  71 and decoded postings dropped from 12,869 to 9,088 while the promotion gate
  stayed `promote` with 13 matched rows, zero regressions, and cross-family
  wins.
- [ ] Next: evaluate an intra-block candidate lookup codec for page-driven
  q+sort so candidate sorted-page docs can be scored without materializing every
  row in each candidate impact block.

## Milestone 0: Core Optimizer Report

Goal: make optimizer decisions visible before changing the format.

Implementation:

- Add an optimizer phase after segment statistics are available.
- Report core candidates and projected cost:
  - superblock grouping,
  - proof metadata,
  - codec family,
  - block size,
  - doc-id layout,
  - filter summary shape,
  - sort summary shape.
- Estimate:
  - bytes added,
  - expected range requests saved,
  - posting blocks skipped,
  - field-value pages avoided,
  - JavaScript decode work avoided,
  - proof strength gained.
- Emit `debug/index-optimizer.json`:

```json
{
  "format": "rfoptimizer-v1",
  "budgetBytes": 50000000,
  "selectedBytes": 38123456,
  "core": [
    { "kind": "superblock", "term": "example", "bytes": 1200, "reason": "high_df_high_tail_cost" },
    { "kind": "proof-metadata", "scope": "posting-block", "reason": "large_page_fallback" }
  ],
  "deferred": [
    { "kind": "champion-window", "reason": "wait_for_core_benchmark" },
    { "kind": "term-sort-materialization", "reason": "wait_for_core_benchmark" }
  ],
  "rejected": [
    { "kind": "facet-summary", "reason": "high_cardinality" }
  ]
}
```

Acceptance:

- The report distinguishes core metadata from deferred materialization.
- The optimizer enforces byte budget at build time.
- Bench rows can tie runtime behavior back to selected optimizer decisions.

## Milestone 1: Safe Block-Max/WAND Proof Layer

Goal: make exact high-df and multi-term top-k queries stop earlier without
query-specific materialization.

Implementation:

- Keep current impact-ordered block-max traversal as the baseline.
- Extend posting metadata with explicit proof fields:
  - block `maxImpact`,
  - optional per-base-term upper bounds,
  - tie-bound doc id,
  - block row count,
  - remaining term upper bound,
  - filter-known/filter-unknown flags.
- Add a WAND/MaxScore-compatible proof function over the existing cursor model:

```text
current kth score > max possible unseen score
or
current kth score == max possible unseen score and tie order is settled
```

- Report proof failures by reason:
  - `score_bound`,
  - `tie_bound`,
  - `filter_unknown`,
  - `sort_unknown`,
  - `metadata_unknown`,
  - `budget_exhausted`.
- Keep all default rows exact. If an anytime lane is added later, it must set
  `approximate: true`.

Acceptance:

- Fewer `fullFallback` rows for broad text and larger page sizes.
- Exact top-k agreement remains 100% for validated rows.
- Runtime stats expose skipped upper-bound work and proof failure reasons.

## Milestone 2: Multi-Level Superblocks In The Core Posting Format

Goal: skip whole range-fetch groups before fetching child posting blocks.

Implementation:

- Group posting blocks into superblocks sized for static range fetches.
- Store superblock metadata inside the posting-segment term entry, not as an
  extra query-specific structure:

```json
{
  "firstBlock": 128,
  "blockCount": 32,
  "maxImpact": 1440,
  "range": { "pack": 3, "offset": 912384, "length": 84121 },
  "filters": { "category": { "words": [2, 5] } }
}
```

- Runtime first schedules superblocks by upper bound, then decodes child blocks
  only inside competitive superblocks.
- Use current external block prefetching as the physical fetch primitive, but
  let the superblock scheduler decide when a group is worth touching.
- Evaluate variable block sizes for very high-df terms so common terms do not
  create too many tiny cold range requests.

Acceptance:

- High-df terms fetch fewer block-pack ranges.
- Single-term common queries and large page-size queries improve at 100k and
  500k without wiki-specific rules.
- Stats report superblocks considered, skipped, fetched, and decoded.

## Milestone 3: Compression And Layout Optimizer

Goal: reduce transferred bytes and decode time without losing random access.

Implementation:

- Benchmark codecs per posting-list family:
  - current delta/int codec,
  - partitioned Elias-Fano for doc-id-like monotone lists,
  - bitset or Roaring-like containers for dense low-cardinality filters,
  - compact impact arrays when scores are low-cardinality.
- Decide codec per term/list from df, density, score entropy, and expected
  random access pattern.
- Evaluate doc-id reassignment as a build option:
  - cluster by source/site/category for compression and filter locality,
  - preserve a stable external id map,
  - measure impact on sort and field-value pages.
- Keep browser decode costs in the benchmark, not only compressed bytes.

Acceptance:

- Codec choice is deterministic and captured in optimizer reports.
- Decode CPU plus transfer bytes improve or preserve p95 across fixtures.
- Random access remains bounded; no codec requires full-list decompression for
  a small top-k query.

## Milestone 4: Filter-Integrated Core Top-k

Goal: apply filters before fetching postings and field values when core
summaries can prove a block or superblock is irrelevant.

Implementation:

- Move filter summaries into the same upper-bound proof path as block-max:
  - low-cardinality facet masks,
  - boolean all/any/mixed flags,
  - numeric/date min/max,
  - optional dense bitset containers for very common boolean/facet values.
- Keep high-cardinality facets out of hot summaries unless the optimizer proves
  the byte cost is worth it.
- Reuse the same summary shape for posting blocks, superblocks, and value pages.
- Report when a filter is unknown and forces value-page fetches.

Acceptance:

- Filtered broad text queries fetch fewer posting blocks and value pages.
- Summary bytes are reported separately.
- Exactness is preserved when summaries are incomplete.

## Milestone 5: Sort-Integrated Core Top-k

Goal: make `q + sort` generic and cheap for configured sortable fields without
starting from term-by-sort materializations.

Implementation:

- Strengthen sorted field-value pages:
  - doc ids in sort order,
  - page min/max values,
  - low-cardinality filter summaries,
  - optional term-membership sketches for high-df terms when byte budget allows.
- Runtime uses the sort lane when:
  - the sort field is configured,
  - filters can be checked from summaries or cheap value pages,
  - text proof can be obtained through posting superblocks or term-membership
    summaries.
- Exact proof for sorted text must be sort-aware:

```text
no earlier unseen sort-position can still match the query and filters
```

Acceptance:

- Sorted text queries avoid large field-value fetches when core sort pages can
  prove the result.
- Rows are exact or explicitly approximate.
- Runtime stats expose rows scanned, pages skipped, and fallback reason.

## Milestone 6: Generic Benchmark Matrix

Goal: prove the core changes are not wiki-only before adding derived
materializations.

Fixture families:

```text
docs/wiki-like       long text, headings, facets, dates
catalog/ecommerce   title, description, price, rating, category, availability
issues/tasks        title, body, status, priority, assignee, updated date
code/docs           identifiers, exact symbols, long pages, version filters
```

Required rows per fixture:

- common single-term query,
- specific multi-term phrase,
- typo query,
- filtered common-term query,
- sorted text query,
- facet browse,
- numeric/date browse,
- larger page size,
- exact identifier/symbol query where applicable.

Required metrics:

- p50/p95 cold latency,
- transferred bytes,
- request count and request groups,
- posting blocks/superblocks decoded,
- value pages fetched,
- proof lane and fallback reason,
- index byte growth,
- build time and peak RSS,
- exact top-k validation for all exact lanes.

Acceptance:

- No fixture-specific optimizer rules.
- Core changes improve or preserve p95 cold latency and transfer.
- Index byte growth stays within configured budget.
- All exact/proven rows keep exact validation.

## Deferred Milestone 7: Proven Champion Windows

Goal: only after core benchmarks, answer common page-1 broad queries from
compact top-doc rows when exact proof is possible.

Gate before implementation:

- Core superblocks and proof metadata are implemented.
- Common single-term queries still show expensive block decoding across multiple
  fixture families.
- The optimizer report shows champion windows beat equivalent core metadata
  under the byte budget.

Implementation:

- Store champion windows as derived packed term metadata, not as a special query
  allowlist.
- Store enough tail proof metadata to know when the window is exact for a
  requested `offset + size`.
- For multi-term queries, use champion rows as a candidate seed only unless all
  missing contribution bounds are proven.
- Runtime lanes:
  - `championExact`,
  - `championCandidate`,
  - `championFallback`.

Acceptance:

- Broad common page-1 queries can return with zero posting-block reads when the
  champion proof holds.
- Champion misses preserve existing exact behavior.
- Increasing `size` or `page` cannot silently return a truncated top-k.

## Deferred Milestone 8: Generic Phrase And N-Gram Materialization

Goal: only after core benchmarks, materialize generic phrase rows when the core
posting path cannot cheaply handle repeated corpus phrases.

Gate before implementation:

- Multi-term phrase rows remain slow after superblocks, proof metadata, and
  codec/layout changes.
- Candidate phrases are selected from corpus statistics, not query strings.
- The optimizer proves materialization beats core metadata for byte and request
  cost.

Implementation:

- Mine candidate n-grams during analysis and segment merge:
  - adjacent base terms with high co-occurrence,
  - title and heading phrases,
  - authority-label phrases,
  - phrases whose component postings are expensive and whose top-k head is
    stable.
- Score candidates with:

```text
utility = estimated_saved_requests * query_likelihood_proxy / bytes
```

- Use query-log-free proxies by default:
  - title frequency,
  - field boost,
  - df of component terms,
  - phrase df,
  - entropy/concentration of top scores.
- Add optional query-log input later as a separate build input, never as
  hardcoded source rules.

Acceptance:

- No query string allowlist exists in builder config or source.
- Selection is deterministic for a corpus and config.
- Bench reports hit rate by category and bytes added.

## Deferred Milestone 9: Learned-Sparse And Hybrid Readiness

Goal: keep Rangefind aligned with modern sparse retrieval research without
making neural inference a required runtime dependency.

Gate before implementation:

- Exact lexical core path is stable.
- The posting row model can represent non-negative quantized impacts without
  weakening BM25F behavior.
- Bench fixtures can compare quality, size, latency, and transfer.

Implementation:

- Generalize posting rows around non-negative quantized impacts so BM25F,
  corpus-expanded lexical terms, and precomputed learned-sparse vectors can use
  the same block-max proof machinery.
- Allow build-time import of external sparse vectors:

```json
{
  "id": "doc-1",
  "sparse": { "tokenA": 1.25, "tokenB": 0.42 }
}
```

- Keep model inference outside the browser runtime by default.
- Add optional corpus-specific vocabulary/expansion experiments only behind an
  explicit config flag and benchmark them against index size, latency, and
  quality.

Acceptance:

- Core block-max and superblock pruning work for both BM25F and imported sparse
  impacts.
- Learned-sparse fixtures can be benchmarked without changing the runtime query
  API.
- Optional approximate sparse lanes report approximation explicitly.

## Auto Mode After Core Validation

Initial config shape:

```json
{
  "optimizationBudgetRatio": 0.08,
  "optimizationBudgetMaxBytes": 50000000,
  "topKProof": {
    "enabled": true,
    "superblocks": true
  },
  "codecs": {
    "mode": "auto"
  },
  "filterSummaries": {
    "mode": "auto",
    "maxFacetCardinality": 64
  },
  "sortSummaries": {
    "mode": "auto",
    "maxFields": 3
  },
  "materializations": {
    "mode": "off_until_core_benchmarks"
  },
  "learnedSparse": {
    "mode": "off"
  }
}
```

Acceptance:

- Auto mode first selects core metadata and layout decisions.
- Derived materializations stay disabled until core benchmarks show a generic
  gap.
- Optimizer decisions are auditable and reproducible.

## Suggested Implementation Order

1. [x] Implement optimizer report scaffolding and runtime proof/fallback reason
   stats.
2. [x] Strengthen tie-bound exact top-k proof metadata and tests for high-df
   ties, filtered text, missing terms, larger pages, and multi-term text.
3. [x] Add remaining-term upper-bound and filter-unknown proof metadata.
4. [x] Add multi-level superblock metadata inside posting segments.
5. [x] Refactor the runtime scheduler to choose competitive superblocks before
   child posting blocks.
6. [x] Add measured codec/layout experiments behind optimizer-controlled auto
   mode, including auto posting block/superblock sizing, measured impact-run,
   dense impact-bitset, partitioned-delta block codecs, and benchmark promotion
   gates.
7. [x] Integrate filter summaries into the core proof path.
8. [x] Integrate sort summaries into the core proof path.
9. [x] Build the multi-fixture benchmark matrix.
10. [x] Promote only core wins that survive across fixtures into default `auto`.
    Completed: `rfbenchpromotion-v1` now blocks default promotion unless fixture
    health, coverage, exactness, codec/layout invariants, and baseline
    comparisons pass; `--out` writes reusable matrix reports. The pair-varint
    baseline comparison returned `promote`, so the current measured codec/layout
    `auto` defaults stay promoted.
11. [x] Re-evaluate champion windows, phrase materialization, and learned-sparse
    import only after core benchmarks identify a remaining generic gap.
    Completed: `rfbenchdeferred-v1` keeps champion/phrase materialization off for
    the current matrix, keeps learned-sparse import deferred until explicit
    sparse inputs and quality benchmarks exist, and routes the remaining q+sort
    decode pressure back to core scheduler work.

## Open Questions

- Is Rangefind's current impact-block scheduler enough after adding
  superblocks, or should it be replaced by a more standard WAND/MaxScore pivot
  loop?
- What is the smallest proof metadata shape that handles score ties without
  causing large manifest or segment growth?
- Which posting codec wins in JavaScript after measuring both compressed bytes
  and decode CPU?
- Should doc-id reassignment be global, per-source, or disabled when many sort
  summaries are configured?
- Can the core sort path prove enough `q + sort` queries without term-by-sort
  materialization?
- What benchmark threshold justifies turning a deferred materialization on?
