# Generic Index Optimization Plan

Date: 2026-05-03

This plan covers generic optimizations for Rangefind after the segment-based
builder and posting-segment runtime refactor. The goal is not to tune for the
French Wikipedia benchmark directly. The goal is to add corpus-statistics-driven
index structures that improve broad text queries, sorted text queries, filtered
text queries, and larger top-k pages across documentation sites, catalogs,
blogs, ecommerce, issue trackers, knowledge bases, and other static datasets.

## Design Principles

- Optimizations must be selected from corpus statistics and field configuration,
  not from hardcoded benchmark query strings.
- The default index must remain static-hosting friendly: immutable files,
  range-addressable packs, lazy sidecars, and deterministic browser execution.
- Extra structures must be byte-budgeted. The user should configure a total
  optimization budget, not hand-pick every term.
- Query quality must remain exact where the runtime reports exact proof. Fast
  approximate lanes can exist only when explicitly marked approximate.
- Optimizations should compose with the existing `rfsegpost-v1` posting
  segments, query bundles, doc-value sidecars, authority sidecars, and compact
  typo lexicon.

## Current Generic Gaps

The 50k/100k/500k benchmark matrix shows the optimized phrase/query-bundle lane
is good, but generic broad-query and sorted-query lanes still have costly
fallbacks:

- High-df single-term queries can still decode many posting blocks.
- Larger top-k windows can exhaust proof metadata and fall back to broad scans.
- Text queries sorted by doc-value fields can require large doc-value fetches.
- Filtered broad terms can need full manifest/doc-value sidecars before they can
  prove a small top-k.

These are generic problems. They will appear on any corpus with common terms,
sortable metadata, and broad filters.

## Milestone 1: Impact-Tiered Posting Blocks

Goal: make high-df terms cheap without requiring query-specific bundles.

Implementation:

- Split each large posting list into impact tiers during final posting assembly:

```text
term
  tier 0: highest-impact postings
  tier 1: medium-impact postings
  tier 2: tail postings
```

- Preserve doc-id ordering inside each tier only where needed for deterministic
  tie handling; otherwise keep the current impact-first block order.
- Store tier metadata in the posting-segment term entry:

```json
{
  "tiers": [
    { "firstBlock": 0, "blockCount": 4, "minImpact": 1234, "maxImpact": 9000 },
    { "firstBlock": 4, "blockCount": 16, "minImpact": 400, "maxImpact": 1233 },
    { "firstBlock": 20, "blockCount": 200, "minImpact": 1, "maxImpact": 399 }
  ]
}
```

- Runtime reads high tiers first and stops once top-k proof succeeds.
- Build telemetry reports tier counts, bytes, and how many terms were tiered.

Acceptance:

- High-df single-term queries decode fewer blocks at 100k and 500k.
- Existing exact top-k tests still pass.
- Runtime stats expose tier reads and skipped tiers.
- Index size increase stays within the configured optimization budget.

## Milestone 2: Per-Term Champion Lists

Goal: answer common page-1 broad queries from compact pre-ranked rows.

Implementation:

- For terms above a df threshold, write a bounded champion list:

```text
term -> top N docs by base text score and doc-id tie order
```

- Store champions in a lazy sidecar, not inline in the minimal manifest.
- Use corpus statistics to select terms:
  - high df,
  - high observed posting-block decode cost during synthetic validation,
  - enough score concentration in the head to prove useful top-k.
- Runtime tries champion rows before posting traversal when:
  - no sort is requested,
  - filters are absent or can be proven from champion row summaries,
  - requested page/size fits the champion window.
- If champion rows cannot prove top-k, fall back to impact-tiered postings.

Acceptance:

- Broad common terms can return page 1 with zero posting-block reads when the
  champion list proves top-k.
- Champion misses preserve existing behavior.
- Stats distinguish `championExact`, `championCandidate`, and fallback lanes.
- Champion sidecar can be disabled or capped by byte budget.

## Milestone 3: Generic Phrase And N-Gram Bundle Mining

Goal: create query bundles from corpus structure, not benchmark queries.

Implementation:

- Mine candidate n-grams during analysis/segment merge:
  - adjacent base terms with high co-occurrence,
  - title/heading phrases,
  - authority-label phrases,
  - phrases whose postings are expensive but whose top-k head is stable.
- Score candidates with a generic utility function:

```text
utility = estimated_saved_requests * query_likelihood_proxy / bundle_bytes
```

- Use proxies available without query logs:
  - title frequency,
  - field boost,
  - df of component terms,
  - phrase df,
  - entropy/concentration of top scores.
- Emit bundles until the configured phrase-bundle byte budget is exhausted.
- Keep row-group filter summaries so filtered text can use bundles where safe.

Acceptance:

- No query string allowlist exists in builder config or source.
- Bundle selection is deterministic for a corpus and config.
- Larger top-k phrase rows use bundles where byte budget allows.
- Bench reports bundle hit rate by category and bundle sidecar bytes.

## Milestone 4: Sort-Aware Term Overlays

Goal: make `q + sort` generic and cheap for configured sortable fields.

Implementation:

- For sortable numeric/date fields, select high-df terms for sort overlays.
- For each selected `(term, sortField, direction)` pair, store a compact top
  window of document ids ordered by the sort field with enough score/filter
  metadata to validate the text match.
- Selection is budgeted and generic:
  - term df above threshold,
  - field marked sortable,
  - doc-value distribution is useful,
  - estimated overlay bytes below budget.
- Runtime uses the overlay when:
  - query has one dominant term or a proven phrase bundle,
  - sort field matches an overlay,
  - filters can be checked from summaries or cheap doc-value chunks.

Acceptance:

- Sorted text queries do not need to fetch large doc-value ranges when a sort
  overlay exists.
- Overlay rows are exact or clearly marked approximate.
- Runtime stats expose overlay attempts, hits, rows scanned, and fallback reason.

## Milestone 5: Filter-Aware Posting Summaries

Goal: skip irrelevant posting blocks before fetching postings or doc-values.

Implementation:

- Extend posting block summaries with compact filter metadata:
  - boolean min/max or bit masks,
  - numeric/date min/max,
  - selected low-cardinality facet masks,
  - doc-value chunk references.
- Keep high-cardinality facets out of block summaries unless explicitly
  budgeted.
- Reuse existing query-bundle row-group summary semantics for consistency.
- Runtime applies block summaries before range-fetching posting blocks.

Acceptance:

- Filtered broad text queries fetch fewer posting blocks and fewer doc-value
  chunks.
- Summary bytes are reported separately.
- Builder automatically drops summaries that exceed byte/cardinality budgets.

## Milestone 6: Stronger Top-K Proof Metadata

Goal: avoid full fallback when the top-k is already stable but current metadata
cannot prove it.

Implementation:

- Add optional per-block proof metadata:
  - max impact,
  - top doc id,
  - max score by base term,
  - tie-bound doc id,
  - remaining tier upper bound.
- Teach the runtime to prove:

```text
current kth score > maximum possible unseen score
or
current kth score == maximum possible unseen score and tie doc order is settled
```

- Keep proof metadata compact and only write it for blocks/terms where the
  optimizer predicts value.

Acceptance:

- Fewer `fullFallback` rows for broad text and larger page sizes.
- Exact top-k agreement remains 100% for validated rows.
- Stats show proof failures by reason: score bound, tie bound, filter unknown,
  sort unknown, or missing sidecar.

## Milestone 7: Budgeted Index Optimizer

Goal: make all optional structures automatic and portable across datasets.

Implementation:

- Add a builder optimizer phase after segment statistics are available.
- Inputs:

```json
{
  "optimizationBudgetBytes": 50000000,
  "championLists": true,
  "impactTiers": true,
  "phraseBundles": "auto",
  "sortOverlays": ["publishedAt", "price", "rating"],
  "filterSummaries": "auto"
}
```

- The optimizer estimates benefit and byte cost for each candidate structure.
- It emits a ranked decision report:

```json
{
  "format": "rfoptimizer-v1",
  "budgetBytes": 50000000,
  "selectedBytes": 38123456,
  "rejected": [
    { "kind": "sort-overlay", "reason": "over-budget" },
    { "kind": "facet-summary", "reason": "high-cardinality" }
  ]
}
```

- Store the report in `debug/index-optimizer.json`.

Acceptance:

- The same algorithm works for wiki, product catalogs, docs, and issue-like
  datasets without query-specific config.
- Byte budget is enforced.
- Build reports make optimizer decisions auditable.

## Milestone 8: Generic Benchmark Matrix

Goal: prove this is not wiki-only.

Create or adapt at least three fixture families:

```text
docs/wiki-like       long text, headings, facets, dates
catalog/ecommerce   title, description, price, rating, category, availability
issues/tasks        title, body, status, priority, assignee, updated date
```

Required rows per fixture:

- common single-term query,
- specific multi-term phrase,
- typo query,
- filtered common-term query,
- sorted text query,
- facet browse,
- numeric/date browse,
- larger page size.

Acceptance:

- No fixture-specific optimizer rules.
- Optimized indexes improve or preserve p95 cold latency and transfer.
- Index byte growth stays within configured budget.
- All exact/proven rows keep exact validation.

## Suggested Implementation Order

1. Add optimizer report scaffolding and byte-budget accounting.
2. Implement impact-tiered postings behind automatic corpus-stat thresholds.
3. Add per-term champion lists for high-df terms.
4. Add runtime lanes and stats for tier/champion attempts and fallbacks.
5. Add generic n-gram bundle mining.
6. Add sort-aware term overlays for configured date/numeric fields.
7. Extend posting block filter summaries.
8. Add stronger top-k proof metadata.
9. Build the multi-fixture benchmark matrix.
10. Promote successful structures into default `auto` mode with conservative
    byte budgets.

## Initial Defaults

Conservative defaults for the first implementation:

```json
{
  "optimizationBudgetRatio": 0.08,
  "optimizationBudgetMaxBytes": 50000000,
  "impactTiers": {
    "enabled": true,
    "minDf": 2048,
    "maxTiers": 3
  },
  "championLists": {
    "enabled": true,
    "minDf": 4096,
    "rows": 128
  },
  "phraseBundles": {
    "mode": "auto",
    "maxBytesRatio": 0.03
  },
  "sortOverlays": {
    "mode": "auto",
    "rows": 256,
    "maxFields": 3
  },
  "filterSummaries": {
    "mode": "auto",
    "maxFacetCardinality": 64
  }
}
```

These defaults should be revised only from multi-fixture benchmark evidence.

## Open Questions

- Can champion lists be exact for enough query shapes, or should they be a
  candidate lane that still needs a proof pass?
- Should impact tiers replace the current block ordering or layer on top of it?
- What is the smallest sort-overlay row shape that can prove exact sorted text?
- How much optimizer output should be included in `manifest.full.json` versus
  debug-only sidecars?
- Should runtime learn from local usage and expose a query-log file for the next
  build, or should first-class optimization remain query-log-free by default?
