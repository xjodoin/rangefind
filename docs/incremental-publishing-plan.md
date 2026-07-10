# Incremental Publishing Plan

## Problem

Rangefind rebuilds an index from scratch. For a site where 50 of 4M documents
change daily, a full rebuild costs minutes of compute and — worse for static
hosting — re-uploads and re-warms multi-GB of pack files, invalidating every
CDN cache entry because doc ids shift and postings re-pack globally.

The goal is delta publishing: a small "generation" of new files per update,
with unchanged generations byte-identical (already-cached at the CDN), and a
runtime that reads across generations transparently.

## Why the current format is well positioned

- Packs are immutable and content-addressed; a generation that doesn't
  change keeps its exact file names and stays cached forever.
- Every subsystem already routes reads through per-object pointers
  (pack/offset/length/checksum), so pointing at packs in a different
  generation directory is a naming change, not a format change.
- The runtime already merges ranked lists from independent sources
  (segments, typo plans, sort replicas, hybrid lanes), so cross-generation
  top-k merge reuses existing patterns.
- BM25F statistics are global inputs the builder controls; a manifest-level
  stats override keeps scoring comparable across generations.

## Design

### Layout

```text
rangefind/
  manifest.json                # generation directory + tombstones pointer
  gen-000/                     # the original full build (unchanged bytes)
    terms/ docs/ doc-values/ ...
  gen-001/                     # delta: only new/updated documents
    terms/ docs/ doc-values/ ...
  tombstones.<hash>.bin.gz     # doc ids superseded or deleted, per generation
```

Each generation is a complete, self-consistent mini-index over its own
documents with a private doc-id space (`generation << shift | localId` at the
API surface; internally each generation keeps dense local ids so every
existing codec works unchanged). The root manifest lists generations with
their doc counts, global scoring statistics (merged term df / field length
totals), and a tombstone set.

> **Status**: All phases shipped. `rangefind build --update` adds a delta
> generation over an existing index; the runtime transparently merges every
> lane across generations — text search (with filters, highlights, facet
> counts), suggestions, counts, sorted browse (merged by real doc-value
> keys), geo (browse, nearest-first, radius/boosted text), vector search,
> and hybrid (RRF fused at the merged level). `rangefind build --compact`
> folds generations back into one index: a full rebuild from the full corpus
> that verifies id coverage before deleting the old generation directories;
> `--update` recommends it at 8 generations or 25% tombstones.
> One refinement over the original design below: instead of
> *merging* global statistics, delta builds **replicate the base
> generations' frozen statistics** (same total, same df for known
> vocabulary via a local scan of the base's term shards, same field-length
> averages) — a document added by a delta scores byte-identically to the
> same document in the base build, which is exactly the property
> cross-generation merging needs. Statistics refresh at compaction.

### Write path (`rangefind build --update`)

1. Load the previous root manifest and the external-id → (generation,
   localId) map (a new sidecar, range-addressed like doc pointers).
2. Partition input: unchanged docs (skip), new/updated docs (index into the
   new generation), missing docs (tombstone).
3. Build the new generation with the normal pipeline — it is just a small
   build.
4. Updated docs tombstone their old (generation, localId).
5. Merge global scoring stats and write the new root manifest.
6. Compaction policy: when the tombstone ratio or generation count crosses a
   threshold (for example 8 generations or 25% tombstoned), fold the smaller
   generations into one — a bounded rebuild, amortized like segment merges
   inside the current builder.

### Read path

- Term queries fan out to each generation's term directory (generations are
  few — bounded by the compaction policy), score with the merged global
  stats, drop tombstoned docs, and merge top-k. This mirrors how the block-max
  scheduler already merges multiple segments.
- Doc hydration, doc values, facets, geo, suggest, and vectors resolve within
  the owning generation. Facet counts sum per-generation tallies; suggestion
  and vector roots load per generation and merge (weights and similarities
  are absolute, so merging is a heap, not a rescore).
- Tombstones load once per session (a bitmap or sorted-id delta list; ~1 bit
  per doc worst case, usually far smaller).

### Costs to be honest about

- Every text query multiplies term-directory lookups by the generation count.
  With ≤4 typical generations and the request coalescer, this is one extra
  round-trip in practice, but it is not free — hence the aggressive
  compaction default.
- IDF drift: merged global stats keep scores comparable, but a term that
  exploded in the delta will be slightly mis-weighted in old generations
  until compaction. Acceptable and self-healing.
- Facet dictionaries assign codes per generation; cross-generation count
  merging must go through values, not codes.
- The authority lexicon's autocomplete weights and the vector index's centroids
  are trained per generation; quality degrades gracefully and compaction
  restores it.

## Phasing

1. **Generation-aware manifest + runtime multi-generation text search**
   (terms, docs, doc values, tombstones) with an exhaustive
   equivalence test: a two-generation build must return exactly the same
   results as the equivalent single build.
2. **`--update` builder mode** with the external-id map and tombstoning.
3. **Sidecars** (facets counts, suggest, geo, vectors) cross-generation
   merge.
4. **Compaction** policy and command.

Phase 1 is the architectural proof and the right first pull request; nothing
in later phases changes formats introduced there.
