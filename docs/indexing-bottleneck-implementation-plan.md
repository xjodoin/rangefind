# Rangefind Indexing Bottleneck Implementation Plan

Date: 2026-05-02

This plan captures the 500k French Wikipedia benchmark findings, the Lucene and
IR research takeaways, and the implementation path to make Rangefind's static
index builder scale like a production search engine while preserving the
browser/CDN query model.

## Compatibility Decision

Rangefind is a new standalone project. The implementation should not preserve
legacy index formats, legacy builder paths, or old runtime loading behavior when
they conflict with performance, scalability, or simplicity.

The rule for this plan is:

```text
keep the product goal
discard slow intermediate architecture
ship one best index format
ship one best browser runtime
```

That means the global-run builder, current monolithic manifest, current typo
sidecar layout, and any compatibility-only posting reader should be removed as
soon as their replacements are implemented and validated. There should be no
long-term v1/v2 dual reader, no hidden compatibility mode, and no opt-in
experimental flag for the faster path. The new format becomes the only format.

## Current Findings

The latest 500k French Wikipedia build used:

```bash
/usr/bin/time -p node scripts/frwiki_fixture.mjs build --limit=500000
/usr/bin/time -p node scripts/frwiki_fixture.mjs bench --limit=500000 --runs=3
```

Build result:

```text
Docs:             500,000
Logical shards:   40,672
Index files:      427
Index size:       1.1 GB
Wall time:        2,719.86s
Previous 500k:    3,642s
Improvement:      about 25%
```

Manifest phase telemetry:

| Phase | Seconds | Finding |
| --- | ---: | --- |
| `reduce-postings` | 1,017.5 | Biggest bottleneck. Still doing too much global run sorting/reducing. |
| `typo` | 837.3 | Second biggest bottleneck and still hurts typo-query cold paths. |
| `scan-and-spool` | 648.9 | Single-process document analysis/scoring is now a major long pole. |
| `query-bundles` | 66.4 | Acceptable, but should become segment-derived rather than corpus-rescanned. |
| `measure` | 48.7 | Acceptable but duplicates some analysis work. |
| `doc-value-sorted` | 38.1 | Acceptable for 500k. |
| `doc-packs` | 27.5 | Acceptable after raw/compressed spool split. |
| `doc-pages` | 18.3 | Acceptable after raw spool use. |
| `authority` | 10.2 | Good. |

Memory finding:

```text
Manifest peak RSS: 1.31 GB
Observed live RSS: about 2.36 GB
```

The manifest telemetry only samples phase boundaries. It misses in-phase peaks,
so the first implementation step must improve telemetry sampling before deeper
builder changes are benchmarked.

Query quality and performance findings:

```text
Rows validated:          25/25
Text exact top-k:        10/10
Init:                    19.0ms, 1 request, 700.1 KB
```

Good query paths:

```text
changement climatique    25.0ms, 28 requests, 76.8 KB, queryBundleExact
dense filter browse       2.0ms,  3 requests, 11.6 KB
typed dates sorted       16.7ms,  4 requests, 74.1 KB
```

Bad query paths:

```text
typo changement climatique          977.3ms, 120 requests, 1376.7 KB
filtered changement climatique     1013.6ms, 184 requests, 1418.7 KB
Révolution française size 25         77.2ms,  70 requests,  678.0 KB
```

The normal text ranking path is now good enough to protect. The remaining
problems are build scalability, typo construction/querying, filtered phrase-like
queries, and the cold manifest size.

## Research Takeaways

### Lucene IndexWriter

Lucene is fast because it does not build one global postings run and sort it at
the end. It buffers documents, flushes immutable segments by RAM pressure, and
merges those segments with a merge policy. Segment files are write-once. Merges
can run in the background and preserve bounded writer memory.

Rangefind should adopt this shape directly. The final output must remain
static-hosting friendly, but it does not need to keep the old shard format:

```text
parallel doc analysis
  -> worker-local immutable segments
  -> tiered merges
  -> final Rangefind range packs and directories
```

### Lucene Postings Format

Lucene postings use packed integer blocks, currently with fixed block size 128
for the common path, plus variable VInt tails and skip data. The term dictionary
stores per-term statistics and file pointers to postings streams.

Rangefind already has impact blocks and range-packed posting-segment sidecars,
but the builder still spends too much time encoding temporary run records and
then converting them. Segment-local postings should be written directly into the
new final block streams.

### SPIMI

Single-pass in-memory indexing builds term dictionaries and postings lists in a
bounded memory block, flushes the block, then merges blocks into the final
inverted index. This is exactly the missing builder architecture for Rangefind:
avoid huge global sort pressure by making local inverted segments first.

### Fuzzy Search

Lucene fuzzy query uses Levenshtein automata against the term dictionary.
Symmetric-delete typo lookup is fast for small dictionaries, but in Rangefind it
creates heavy static sidecars and expensive cold typo queries. Rangefind needs a
hybrid typo architecture:

```text
exact/confident query -> no typo sidecar
broken query          -> compact lexicon traversal + bounded candidate expansion
```

The typo index should be segment-built and merged like postings, not reduced as
a separate giant global job.

### Block-Max WAND And Impact-Ordered Retrieval

Rangefind's query model is already moving in the right direction: impact heads,
query bundles, proof flags, and range-packed posting blocks. The next quality
improvement is not to replace that path. It is to make query bundles and
residual bounds filter-aware so the good unfiltered phrase path survives common
facet/numeric filters.

## Target Architecture

The target builder is a proof-carrying static search compiler:

```text
JSONL input
  -> document partitioner
  -> analyzer/scorer workers
  -> segment writers
       terms
       postings
       impact heads
       query-bundle candidates
       authority keys
       typo lexicon entries
       doc values
       doc payload/page spools
  -> tiered segment merger
  -> final immutable object store
       minimal manifest
       range directories
       packed posting segments
       impact heads
       query bundles
       doc cards/pages
       doc values
       compact typo lexicon
```

The builder should never need all corpus terms, all postings, all typo keys, or
all document payloads in heap at once.

## Progress Tracker

Updated: 2026-05-02

| Milestone | Status | Notes |
| --- | --- | --- |
| 1. Better Build Telemetry | Done | Interval memory sampling, per-phase CPU/disk deltas, segment merge timing, manifest output, optional frwiki telemetry export, and test coverage are implemented. |
| 2. Segment Compiler For Postings | In progress | `src/segment_builder.js` now flushes bounded immutable `rfsegment-v1` postings segments, the main builder no longer writes global posting runs, and batch analyzer workers are wired for `scan-and-spool`. The final browser segment format remains. |
| 3. Tiered Segment Merge | In progress | `src/segment_merge.js` now premerges segments through fan-in tiers before final partitioning, records reduced-spool/partition timings, and failed builds clean partial `_build` scratch state. Background/resumable merge semantics remain. |
| 4. Direct Compressed Posting Writers | Done | Final browser postings now use the new-only `rfsegpost-v1` posting-segment format. The legacy term-shard reader/writers and build-parse-rewrite loop have been removed. |
| 5. Typo Lexicon Rebuild | In progress | Global final-term delete-key expansion was removed from `reduce-postings`; typo sidecar now expands analyzer-time surface pairs once per flush segment; short typos use bounded symmetric delete, and long typos use compact `rftermlex-v1` lexicon pages. Large-frwiki acceptance remains. |
| 6. Filter-Aware Query Bundles | In progress | Filtered searches can use complete/proven query bundles and avoid posting block decode when the filtered top-k is proven. Query bundles now carry row-group bounds and compact filter summaries; the 50k/100k/500k frwiki filtered cases are under the request/KB budget. |
| 7. Minimal Manifest Split | In progress | Builder writes `manifest.min.json`, `manifest.full.json`, compressed `doc-values/manifest.json.gz`, and `debug/build-telemetry.json`; runtime initializes from the minimal manifest and hydrates diagnostics/doc-value/full metadata lazily. Binary side manifests remain. |
| 8. Larger Bench Matrix | In progress | 50k, 100k, and 500k checkpoints are now measured with telemetry and runtime transfer breakdowns; optional 1M remains. |

Current benchmark checkpoint:

- 50k frwiki slice on 2026-05-02 after tiered-merge support, query-bundle
  row-group filter summaries, `4` scan analyzer workers, direct external
  posting-segment block writes, and chunk-LRU build code-store reads:
  `real 84.53s`, peak RSS `1,441,333,248` bytes.
- 50k phase timings: `measure 6.21s`, `scan-and-spool 31.15s`,
  `reduce-postings 15.59s`, `query-bundles 8.51s`, `authority 1.03s`,
  `doc-packs 2.30s`, `doc-pages 1.75s`, `typo 17.40s`,
  `doc-values 0.21s`, `doc-value-sorted 0.21s`,
  `facet-dictionaries 0.04s`.
- `scan-and-spool` improved from `40.66s` to `31.38s` with workerized
  analyzer batches.
- `reduce-postings` improved from `280.07s` to `15.59s` after direct
  external posting-segment block assembly and a chunk-LRU build code-store cache.
  The old bottleneck was not reduced-spool creation (`2.90s`, `41.4 MB`), but
  partition assembly/filter-summary reads (`270.66s` before the cache,
  `12.57s` after).
- 50k minimal manifest is `38,810` bytes; full manifest is `117,917`
  bytes; build telemetry sidecar is `13,294` bytes.
- A fan-in-8 tiered merge trial on the 50k slice regressed to `457.86s`;
  the default fan-in is now `512`, so the 50k/100k/500k slices avoid
  intermediate merge I/O while still allowing explicit lower-fan-in tests.
- A 500k fan-in-64 trial was stopped after 35 minutes because it was still in
  single-core segment merge/reduce work with `_build/segment-merge` at `782 MB`;
  500k validation should use the direct final merge fan-in before judging the
  post-cache architecture.
- A direct no-spool final partition streaming trial regressed to `775.56s`
  on the 50k slice because `reduce-postings` rose to `696.24s`; the faster
  single reduced-term spool remains the default until the direct compressed
  posting writer replaces this bridge.
- Earlier 50k checkpoints in this implementation pass were `440.87s` before
  removing final-term typo expansion and `381.42s` before per-segment typo
  surface dedupe/minimal-manifest slimming. The best pre-filter-summary
  checkpoint was `336.44s`; the worker-only checkpoint was `349.87s`, and the
  direct external writer checkpoint before fixing code-store cache thrash was
  `344.00s`.
- 50k runtime bench after query-bundle filter summaries and row-group proof:
  init is `1` request and `37.9 KB`; `filtered changement climatique long body`
  uses `queryBundleExact`, `queryBundleFilterProof: rowGroupSummary`, decodes
  `0` posting blocks, scans `0` doc-value rows, makes `0` doc-value requests,
  avoids `manifest.full.json`, and transfers `79.6 KB` cold.
- 100k frwiki checkpoint: `real 176.80s`, peak RSS `1,850,179,584` bytes.
  Phase timings: `measure 11.81s`, `scan-and-spool 63.54s`,
  `reduce-postings 36.22s`, `query-bundles 17.40s`, `authority 2.11s`,
  `doc-packs 4.13s`, `doc-pages 3.97s`, `typo 36.42s`,
  `doc-values 0.42s`, `doc-value-sorted 0.51s`,
  `facet-dictionaries 0.07s`. Reduced spool is `82.97 MB`; reduced-spool
  creation is `5.94s`; partition assembly is `30.02s`.
- 100k runtime bench: init is `1` request and `42.7 KB`. `filtered changement
  climatique long body` uses `queryBundleExact`, scans only `64` doc-value rows,
  decodes `0` posting blocks, avoids `manifest.full.json`, and transfers
  `338.4 KB` cold. The doc-value sidecar manifest is `124,027` bytes.
- 500k frwiki checkpoint: `real 823.15s`, peak RSS `3,220,865,024` bytes.
  Phase timings: `measure 48.11s`, `scan-and-spool 300.09s`,
  `reduce-postings 173.24s`, `query-bundles 62.84s`, `authority 9.76s`,
  `doc-packs 28.35s`, `doc-pages 21.09s`, `typo 173.74s`,
  `doc-values 1.94s`, `doc-value-sorted 3.15s`,
  `facet-dictionaries 0.24s`. Reduced spool is `379.09 MB`; reduced-spool
  creation is `31.09s`; partition assembly is `141.18s`.
- 500k runtime bench: init is `1` request and `79.9 KB`. `filtered changement
  climatique long body` uses `queryBundleExact`, scans only `64` doc-value rows,
  decodes `0` posting blocks, avoids `manifest.full.json`, and transfers
  `455.7 KB` cold with compressed doc-value metadata. `typo changement
  climatique` is `304.7ms` and `465.1 KB`; unfiltered `changement climatique`
  is `50.2ms` and `77.1 KB`.
- 50k typo runtime after staged primary delete-key lookup: `typo changement
  climatique` drops from `45` requests / `462.9 KB` to `39` requests /
  `321.8 KB`; typo shard transfer drops from `4` requests / `137.2 KB` to
  `1` request / `31.6 KB`.
- The frwiki fixture config now enables `4` batch analyzer workers with
  `128` docs per batch; local integration coverage runs the worker path with
  `2` workers while preserving deterministic output.

## Milestone 1: Better Build Telemetry

Goal: make every later benchmark trustworthy.

Implementation:

- [x] Add an interval sampler to `src/build_telemetry.js`.
- [x] Track per-phase max RSS, heap, external memory, and array-buffer memory.
- [x] Track CPU user/system time per phase with `process.cpuUsage`.
- [x] Track per-phase disk output bytes for `_build`, final packs, and sidecars.
- [x] Track worker counts and worker phase durations.
- [x] Write telemetry in the manifest and optionally to
  `examples/frwiki/frwiki-build-telemetry.json`.

Acceptance:

- [ ] Manifest peak RSS must be close to observed `ps` RSS during the 500k build.
- [x] Telemetry must identify the exact serial tail phase after parallel reduction.
- [x] Existing tests must verify telemetry shape.

## Milestone 2: Segment Compiler For Postings

Goal: replace global run sorting with Lucene/SPIMI-style local segment writes.

Implementation:

- [x] Add `src/segment_builder.js`.
- [x] Partition input into bounded immutable flush segments.
- [x] Add parallel document-range analyzer workers.
- [x] Each segment builds an in-memory term map:

```text
term -> append-only arrays of doc deltas and impacts
```

- [x] Flush a segment when memory budget is reached.
- [x] Write each segment as immutable files under `_build/segments/<segment-id>/`.
- Segment metadata:

```json
{
  "format": "rfsegment-v1",
  "docBase": 0,
  "docCount": 65536,
  "termCount": 123456,
  "postingCount": 3456789,
  "terms": "terms.bin",
  "postings": "postings.bin",
  "heads": "heads.bin",
  "summaries": "summaries.bin"
}
```

- [x] Emit the new final posting-segment format directly. Do not emit the old
  term-shard codec as a compatibility bridge.
- [x] Delete the global-run posting builder after the segment compiler is wired into
  the main build path.

Acceptance:

- [ ] 100k and 500k output quality remains identical to an exhaustive evaluation of
  the new scorer.
- [ ] 500k `reduce-postings` time drops materially.
- [ ] Peak RSS remains bounded by configured worker budgets.
- [x] No compatibility code path remains for the global-run posting builder.

## Milestone 3: Tiered Segment Merge

Goal: avoid one huge final merge and preserve predictable disk/memory behavior.

Implementation:

- [x] Add `src/segment_merge.js`.
- [x] Merge segments by tier, similar to Lucene's tiered merge policy:

```text
small segments -> medium segments -> final segments
```

- [x] Merge term streams with a heap by term.
- [x] Merge postings per term in doc-id order.
- [x] Emit prefix counts, df, block summaries, and bundle df during merge.
- [ ] Keep intermediate merged segments write-once and resumable.
- [ ] Write final segment outputs in retrieval order with the new block metadata,
  impact heads, and doc-card pointers needed by the browser runtime.

Acceptance:

- [x] Build can resume or at least fail with clear partial segment cleanup.
- [ ] 500k merge CPU uses multiple cores for most of the merge window.
- [x] No phase requires all terms or all postings in heap.
- [ ] The final index is emitted only in the new segment-derived format.

## Milestone 4: Direct Compressed Posting Writers

Goal: stop encoding temporary data that is not close to final query format.

Implementation:

- Add a near-final segment posting block format:

```text
rfsegpost-v1
  term id
  df
  impact head rows
  block count
  per-block max impact
  per-block filter summaries
  compressed doc delta block
  compressed impact block
```

- Use packed integer blocks for doc deltas and impacts.
- Keep a small VInt tail for non-full blocks.
- Store block metadata separately from compressed bodies so the final writer can
  build query proof metadata without decoding all postings.
- [x] Write external posting-segment block metadata directly during final
  posting assembly, avoiding the previous build-term-shard, parse-shard,
  rewrite-shard loop.
- [x] Cache build code-store doc-value chunks with a small per-field LRU so block
  filter summaries do not issue random reads per posting.
- Preserve Rangefind's impact-ordered and query-bundle lanes in the new format.
- [x] Remove the old logical shard object layout and legacy reader after the new
  posting segment reader is implemented.

Acceptance:

- [x] Lower `reduce-postings` CPU and sys time.
- [x] Lower intermediate `_build` bytes.
- [x] Runtime query agreement remains exact.
- [x] Runtime no longer contains a reader for the replaced term-shard/posting
  split.

## Milestone 5: Typo Lexicon Rebuild

Goal: cut the 837s typo build phase and fix expensive cold typo queries.

Implementation:

- [x] Generate typo terms during analyzer/scoring scan.
- [x] Deduplicate surfaces per document before expanding typo keys.
- [x] Deduplicate surfaces per segment before expanding typo keys.
- [x] Try exact-token delete-key lookup before full symmetric-delete fanout for
  common deletion-style typos.
- [x] Build a compact term lexicon:

```text
rftermlex-v1
  minimal trie/FST-like pages
  term id
  df
  authority/title score
  pointer to exact term metadata
```

- [x] Keep symmetric delete lookup only as a bounded candidate generator for short
  terms and common misspellings.
- [x] Add a Levenshtein-automaton-inspired traversal over the compact lexicon for
  longer typo terms, with hard candidate limits.
- [x] Rank typo candidates before touching postings:

```text
edit distance
surface authority
df prior
query coverage
term score upper bound
```

Runtime rule:

```text
if exact top-k is confident:
  skip typo completely
else:
  load minimal typo lexicon pages
  generate bounded candidates
  run corrected query only if candidate score can beat current result
```

Acceptance:

- [ ] 500k typo build phase drops substantially.
- [ ] `typo changement climatique` cold requests and KB drop by at least 50%.
- [x] Existing typo quality cases still pass.

## Milestone 6: Filter-Aware Query Bundles

Goal: make `filtered changement climatique` behave like the unfiltered bundle
path instead of falling back to heavy posting/doc-value work.

Implementation:

- [x] Extend query bundles with compact filter summaries:

```text
facet hit masks or dictionary ranges
boolean masks
numeric/date min-max by bundle row group
doc-page locality hints
```

- [x] Add bundle row group metadata:

```text
rowStart
rowCount
scoreMax
scoreMin
docMin
docMax
filterSummaryPointer
```

- [x] Planner behavior:

```text
load bundle
apply cheap filter summary
hydrate passing row groups
use bundle threshold as proof seed
fall back to exact tail only when bound fails
```

Acceptance:

- [x] `filtered changement climatique long body` no longer decodes 472 blocks or
  performs 124 doc-value requests in the common case.
- [x] Top-k agreement remains exact for filtered text cases covered by the local regression suite.

## Milestone 7: Minimal Manifest Split

Goal: reduce cold init from `700 KB`.

Implementation:

- [x] Write:

```text
manifest.min.json
terms/terms-manifest.bin
docs/docs-manifest.bin
bundles/bundles-manifest.bin
facets/facets-manifest.bin
typo/typo-manifest.bin
debug/build-telemetry.json
doc-values/manifest.json.gz
```

- [x] Default runtime loads only:

```text
manifest.min.json
terms minimal root
bundle minimal root
```

- [x] Load doc-values, facets, typo, and build telemetry lazily.

Acceptance:

- [x] Init transfer drops from `700 KB` to below `100 KB` on the 50k frwiki
  checkpoint.
- Runtime internals and index-loading behavior can change freely. Keep only the
  public search-call shape if it does not add compatibility cost.
- [x] Bench reports init and lazy manifest requests separately.

## Milestone 8: Larger Bench Matrix

Goal: prove scalability and quality, not just one faster build.

Required benchmark points:

```text
50k
100k
500k
optional 1M
```

Required metrics:

- Build wall time, CPU user/sys, and peak RSS.
- Phase timings and phase memory peaks.
- `_build` temporary bytes.
- Final index bytes and file count.
- Terms, postings, external posting blocks, query bundles, typo lexicon size.
- Runtime init requests/KB.
- Runtime cold requests/KB/ms by query class.
- Exact top-k agreement for text and filtered text.
- Typo Hit@k/MRR and typo request/KB budgets.
- Lucene comparison for quality where available.

Acceptance gates:

```text
Text exact top-k agreement:       100% on benchmark set
Structured validation:            100%
500k build wall time:             met, 823.15s versus current 2,719s
500k peak RSS:                    measured at 3,220,865,024 bytes
Init transfer:                    met, 79.9 KB at 500k
typo changement climatique:       met, 304.7ms and 465.1 KB at 500k
filtered changement climatique:   met, 35.9ms and 455.7 KB at 500k
```

## Proposed Implementation Order

1. Improve telemetry sampling.
2. Build the segment compiler as the only posting construction path.
3. Add the tiered segment merger and remove global run reduction.
4. Implement the new direct compressed posting-segment format.
5. Replace the browser posting reader with the new segment reader.
6. Move typo construction onto segment infrastructure.
7. Replace the typo sidecar with the compact lexicon and bounded fuzzy planner.
8. Add filter-aware query bundles.
9. Split the manifest and remove the monolithic manifest cold path.
10. Run 100k and 500k comparative benches.
11. Delete obsolete codecs, readers, config flags, docs, and tests for removed
    legacy paths.

## Risks

- Segment compiler can change tie ordering if doc ordering is not preserved
  exactly. Use deterministic `(score desc, docId asc)` tests.
- Direct compressed postings can improve build speed but complicate exact proof
  metadata. Keep exhaustive fallback tests.
- Typo lexicon changes may improve performance but reduce correction recall.
  Track typo quality separately from text ranking.
- Filter-aware query bundles can become storage-heavy. Use row groups and strict
  byte budgets.
- Manifest split can create more cold requests if the minimal root is too small
  or poorly planned. Bench init and first query together.
- Removing compatibility increases migration cost for existing generated
  indexes. This is acceptable for the standalone project; require users to
  rebuild indexes with the new builder.

## Source References

- Lucene `IndexWriter`: RAM-buffered flush, immutable segments, merge policy.
- Lucene postings format: packed integer blocks, VInt tails, skip data, term
  metadata pointers.
- SPIMI: bounded in-memory inverted blocks followed by block merge.
- Lucene `LevenshteinAutomata` and `FuzzyQuery`: automaton-based fuzzy term
  enumeration over a compact term dictionary.
- Block-Max WAND and impact-ordered retrieval: exact top-k proof with block
  upper bounds and high-impact early traversal.
