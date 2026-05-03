# Builder Optimization Implementation Plan

Status: implementation complete; acceptance follow-up needed

This plan targets Rangefind's builder, not a Wikipedia-specific shortcut. The
goal is a generic static-search indexer that scales to larger corpora with
bounded memory, predictable write amplification, and runtime artifacts that
remain efficient for browser range fetching.

Compatibility note: this project is still greenfield. The implementation should
prefer the clean end-state format over compatibility bridges.

## Current Baseline

Recent frwiki runs show that runtime performance is now strong enough that the
builder is the next bottleneck.

- 100k index: 141 files, 218,128,615 bytes.
- 500k index: 371 files, 931,348,929 bytes.
- 500k full rebuild previously took about 40 minutes on this machine.
- Sampling during the long 500k build showed heavy JavaScript array growth and
  GC pressure during final assembly.
- The builder already has important primitives:
  - `scan-and-spool`
  - `_build/segments`
  - `reduce-postings`
  - `query-bundles`
  - `doc-packs`
  - `doc-pages`
  - `doc-values`
  - `filter-bitmaps`
  - build telemetry

The next improvement should therefore remove remaining whole-index pressure and
make segment flushing, merge policy, and streaming writers first-class.

## Research Anchors

- Lucene `IndexWriter`: buffers documents in memory, flushes by RAM/doc count,
  writes immutable segments, and delegates segment selection to a merge policy.
  Source: https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/index/IndexWriter.html
- Lucene/Solr segment model: written segments are immutable and periodically
  merged to prevent fragmentation.
  Source: https://solr.apache.org/guide/solr/latest/configuration-guide/index-segments-merging.html
- Tantivy indexing: `SegmentWriter` builds segment indexes, `IndexWriter`
  creates and merges segments, and `LogMergePolicy` merges similarly sized
  segments.
  Source: https://docs.rs/tantivy/latest/tantivy/indexer/index.html
- Tantivy writer budgets: `writer_with_num_threads` splits an overall memory
  budget across indexing threads.
  Source: https://docs.rs/tantivy/latest/tantivy/index/struct.Index.html
- SPIMI: build in memory until the block is full, write a block dictionary and
  postings to disk, then merge blocks. This avoids one global token sort and
  gives linear construction behavior under a disk-space assumption.
  Source: https://nlp.stanford.edu/IR-book/html/htmledition/single-pass-in-memory-indexing-1.html
- Block-Max WAND and block-max indexes: block-level score upper bounds enable
  safe top-k pruning. Builder work should emit this metadata while flushing
  blocks instead of recomputing it in a late pass.
  Source: https://research.engineering.nyu.edu/~suel/papers/bmw.pdf
- Inverted-index compression research: compression format and block layout are
  part of performance, not just disk savings. The builder should produce
  decode-friendly integer streams and stable block boundaries.
  Source: https://arxiv.org/abs/1908.10598

## Design Principles

- Core format first: no extra sidecar that hides builder weakness. If a new
  structure is generally useful, make it part of the index format.
- Bounded memory: every builder phase must have an explicit byte/doc budget and
  a flush path.
- Immutable output: published files stay content-addressed and cache-safe.
- Segment aware: the builder should be able to publish multiple optimized
  segments instead of force-merging everything into one logical monolith.
- Streaming writers: pack files, directory pages, doc-values, filters, and
  query bundles should be emitted from streams/readers, not from large JS object
  graphs.
- Parallel where independent: partition by term range, field, or segment when
  outputs do not conflict.
- Runtime-aware layout: builder optimizations cannot shift cost into browser
  cold queries. Segment fanout and merge policy must preserve low request count
  and low transferred bytes.

## Target Architecture

### Segment Manifest

Add a top-level segment manifest:

```text
manifest.json
segments/
  manifest.json.gz
  s000001/
    segment.meta.json.gz
    terms/
    docs/
    doc-values/
    filter-bitmaps/
    query-bundles/
```

Each segment has:

- doc id base and count
- term dictionary pointer
- block-pack pointers
- field dictionaries and doc-value manifests
- filter bitmap manifest
- query-bundle manifest
- checksum and byte counters
- build telemetry for that segment

The runtime can search a small list of segments and merge top-k candidates. For
static publish builds, the builder may still merge aggressively, but it should
not require one final segment.

### Memory-Budgeted Flush

Replace implicit growth limits with explicit budgets:

- `builderMemoryBudgetBytes`
- `segmentFlushDocs`
- `segmentFlushBytes`
- `segmentMergeFanIn`
- `segmentMaxDocs`
- `builderWorkerCount`
- per-thread arena budget derived from the global budget

The scan phase flushes segment writers when either doc or byte budget is hit.
Each segment writer owns compact binary buffers for postings, selected scoring
terms, doc-value rows, and document payload pointers.

### Tiered Merge Policy

Implement a log/tiered merge policy inspired by Lucene and Tantivy:

- Merge similarly sized segments first.
- Bound fan-in to avoid peak read/write amplification.
- Keep large segments stable.
- Support `finalSegmentTargetCount`, with `1` as an optional release-build
  setting rather than a required builder invariant.
- Record merge decisions in telemetry.

### Streaming Pack and Directory Writers

Make pack/directory output incremental:

- Write compressed object bytes directly to the current pack.
- Compute hashes/checksums while streaming.
- Spill directory entries to a typed binary row spool.
- Sort or merge directory rows by key using bounded chunks.
- Build directory pages from the row stream.
- Avoid storing all pack entries in large JS arrays unless explicitly required
  by a small output.

### Partitioned Reducers

Reduce postings by independent partitions:

- Partition term ranges by hash or lexicographic boundaries.
- Assign each partition to a worker with disjoint output packs.
- Merge per-segment term streams with a heap.
- Emit final posting blocks, term stats, block-max metadata, query-bundle seeds,
  typo seeds, and authority seeds in one pass over the merged term stream.

### Binary Builder Buffers

Replace high-volume JS arrays/objects in hot phases with typed buffers:

- varint/gap encoded doc ids
- float/impact quantized scores
- fixed-width row headers
- string table references for repeated terms/fields
- mmap-like file readers where Node can stream by offset

The goal is lower GC pressure and fewer large object graphs during 500k+ builds.

### Shared Column Pipeline

Unify builder handling for doc-values, sorted doc-values, and filter bitmaps:

- one field row spool from scan
- field-specific reducers consume the same spool
- numeric/date/boolean summaries emitted once
- low-cardinality bitmap packs built from sorted field rows
- sorted browse pages built from the same typed rows

This keeps future generic fields from adding more full-corpus passes.

## Implementation Checklist

- [x] Baseline telemetry
  - [x] Add a builder benchmark report section for phase wall time, peak RSS,
        GC-sensitive heap samples, output bytes, and temp bytes.
  - [x] Add optional live build progress logs for long-running phases, with
        elapsed time, RSS/heap, temp bytes, pack bytes, and sidecar bytes.
  - [x] Save baseline rows for 50k, 100k, and 500k builds.
  - [x] Add a `--builder-only` or equivalent bench mode that skips runtime
        queries when only indexing code changed.

- [x] Segment manifest format
  - [x] Define `rfsegmentmanifest-v1` in docs and config.
  - [x] Emit segment metadata from the current `_build/segments` outputs.
  - [x] Add tests for segment metadata checksums, doc id base/count, and field
        manifest consistency.

- [x] Runtime segment fanout
  - [x] Load top-level `segments/manifest.json.gz`.
  - [x] Search multiple segments and merge top-k candidates exactly.
  - [x] Apply filters and sort lanes across segment-local doc ids.
  - [x] Add tests proving multi-segment results match force-merged results.

- [x] Memory-budgeted segment writer
  - [x] Add explicit builder memory and flush config.
  - [x] Flush scan output by byte/doc budget.
  - [x] Record per-segment memory estimates and flush reason.
  - [x] Fail early when a single document exceeds safe segment limits.

- [x] Streaming pack writer
  - [x] Add an append-only pack writer that returns object pointers without
        retaining all object payloads.
  - [x] Add a binary directory-entry spool.
  - [x] Build paged directories from bounded sorted chunks.
  - [x] Convert terms and docs first, then doc-values/filter/query-bundles.

- [x] Tiered merge policy
  - [x] Add a merge planner that groups similarly sized segments.
  - [x] Bound fan-in and temp disk usage.
  - [x] Make final force-merge optional.
  - [x] Emit telemetry for merge levels, write amplification, and skipped
        merges.

- [x] Partitioned posting reducer
  - [x] Partition term streams into independent ranges.
  - [x] Run reducers in workers with disjoint output ownership.
  - [x] Emit block-max metadata during block construction.
  - [x] Reuse the same stream for query-bundle and typo seeds.
  - [x] Reuse typed posting-row buffers in segment merge and final posting
        encoding to reduce hot-path JS pair-object allocation and major GC
        pressure.
  - [x] Stream external posting-block chunks directly to block packs without
        concatenating full term posting byte streams first.
  - [x] Let reducer workers keep external posting blocks enabled by writing
        term packs and block packs through shared atomic pack-index counters.

- [x] Shared field row pipeline
  - [x] Create a typed field-row spool in scan.
  - [x] Build doc-values, sorted values, and filter bitmaps from that spool.
  - [x] Remove redundant per-field full-doc loops.
  - [x] Add field-type coverage for numeric, date, boolean, single facet, and
        multi facet.

- [x] Bench and acceptance
  - [x] `npm test`
  - [x] `npm run check`
  - [x] `npm run build:browser`
  - [x] 50k full build and full query bench
  - [x] 100k full build and full query bench
  - [x] 500k full build and full query bench
  - [x] runtime-only bench against reused 100k and 500k indexes

### Current Bench Notes

- 50k full bench completed on 2026-05-03: 114.8s build time, 1.54 GB peak RSS,
  188.2 MB index, average text cold cost 27.16 requests / 178.40 KB.
- 100k full bench completed on 2026-05-03: 265.4s build time, 1.67 GB peak RSS,
  351.3 MB index, average text cold cost 29.37 requests / 198.94 KB.
- 50k builder-only sanity run after typed row-buffer reuse completed on
  2026-05-03: 115.3s build time, 1.61 GB peak RSS, 46.0s reducer time, 1.50 GB
  reducer peak RSS.
- 50k builder-only sanity run after external block chunk streaming completed on
  2026-05-03: 113.6s build time, 1.66 GB peak RSS, 45.1s reducer time, 1.66 GB
  reducer peak RSS.
- 500k full bench after typed row-buffer reuse completed on 2026-05-03:
  2,277.4s build time, 3.96 GB peak RSS, 1.51 GB peak heap, 1.41 GB index,
  average text cold cost 32.84 requests / 290.08 KB, 16/19 exact-check text
  rows reported exact top-k agreement.
- The 500k reducer now recovers to sub-1 GB RSS / sub-500 MB heap during much
  of the long phase, but its final completion still peaks at 3.69 GB RSS /
  1.49 GB heap. The next reducer target is streaming final posting-block
  assembly/compression so completion does not build large transient buffers.
- Runtime-only reuse benches completed on 2026-05-03. Reused 100k index:
  average cold text cost 25.30 requests / 206.88 KB across 10 default text
  rows, 10/10 exact top-k agreement. Reused 500k index: average cold text cost
  28.10 requests / 271.01 KB across 10 default text rows, 10/10 exact top-k
  agreement.
- Worker-owned external block-pack validation completed on 2026-05-03:
  focused tests now cover partition reducers writing numeric immutable term
  packs and posting-block packs while preserving search correctness.
- 50k builder-only run after worker-safe external block packs completed on
  2026-05-03: 91.3s build time, 22.4s reducer time, 2.10 GB peak RSS, 188.2 MB
  index, 180 files, 11 pack files. This improves reducer wall time materially
  versus the previous 45.1s reducer note, while peak RSS needs follow-up tuning.

### Acceptance Follow-Up

- The implementation checklist is complete, but the 500k wall-time target is
  not met yet. The latest 500k full build is 2,277.4s versus the previous
  roughly 2,400s baseline, not the target 30 percent reduction.
- The latest generic reducer fix is worker-safe external block packs. The next
  acceptance step is rerunning reusable 100k and 500k frwiki builder/full
  benches to quantify throughput and memory beyond the 50k validation point.

## Acceptance Targets

These targets should be validated with the reusable frwiki dataset, but the
implementation must stay corpus-neutral.

- 500k build wall time: at least 30 percent faster than the current full
  rebuild baseline.
- Peak RSS: bounded and reported; target at least 40 percent lower during the
  reduce/final assembly phase.
- Runtime correctness: exact top-k agreement unchanged for all exact-check
  query rows.
- Runtime cold cost: no more than 10 percent regression in request count or KB
  for existing 100k and 500k benchmark rows.
- Output integrity: all range-pack pointers remain checksummed and
  content-addressed.
- Format clarity: no compatibility shim unless it is needed only inside a
  single migration commit and is removed before the plan is marked complete.

## First Implementation Order

1. Add baseline builder telemetry and bench reporting so improvements are
   measurable.
2. Define the segment manifest and runtime segment fanout tests.
3. Convert the current temporary segment output into publishable immutable
   segments.
4. Make force-merge optional and add tiered merge policy.
5. Replace final pack/directory assembly with streaming writers.
6. Partition reducers and field-row pipelines after the format is stable.

This order keeps the work focused on the core indexing architecture first. It
also avoids spending effort on extra sidecars before the builder format itself
can scale.
