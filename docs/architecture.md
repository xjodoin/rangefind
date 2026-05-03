# Architecture

Rangefind separates logical retrieval units from physical deployment files.

## Build Output

The package ships one browser entrypoint:

```text
dist/runtime.browser.js
```

That file bundles the query runtime and browser-safe codecs into a single ESM
module. Hosts should import it directly instead of copying `src/*.js`; the
source modules remain the development and Node test surface.

```text
rangefind/
  manifest.json
  segments/
    manifest.json.gz
  doc-values/
    packs/
      0000.<hash>.bin
    sorted/
      bodyLength.<hash>.bin.gz
    sorted-packs/
      0000.<hash>.bin
  facets/
    directory-root.<hash>.bin.gz
    directory-pages/
      0000.<hash>.bin.gz
    packs/
      0000.<hash>.bin
  docs/
    ordinals/
      0000.<hash>.bin
    pointers/
      0000.<hash>.bin
    pages/
      0000.<hash>.bin
    page-packs/
      0000.<hash>.bin
    packs/
      0000.<hash>.bin
  terms/
    directory-root.<hash>.bin.gz
    directory-pages/
      0000.<hash>.bin.gz
    block-packs/
      0000.<hash>.bin
    packs/
      0000.<hash>.bin
      0001.<hash>.bin
  authority/
    directory-root.<hash>.bin.gz
    directory-pages/
      0000.<hash>.bin.gz
    packs/
      0000.<hash>.bin
  typo/
    manifest.<hash>.json
    directory-root.<hash>.bin.gz
    directory-pages/
      0000.<hash>.bin.gz
    packs/
      0000.<hash>.bin
    lexicon/
      directory-root.<hash>.bin.gz
      directory-pages/
        0000.<hash>.bin.gz
    lexicon-packs/
      0000.<hash>.bin
```

`manifest.json` is small enough for page initialization. It lists schema,
feature flags, an `object_store` descriptor, default results, compact facet
counts, and pointers to the paged range directories. Full facet dictionaries use
the same range-pack layout as terms and documents, so high-cardinality metadata
does not inflate cold start.

## Object Pointers

New builds use ZFS-inspired immutable object pointers. Every range-packed object
that can be fetched independently is described by the same pointer shape:

```json
{
  "pack": "0000.<hash>.bin",
  "offset": 12345,
  "length": 678,
  "physicalLength": 678,
  "logicalLength": 2048,
  "checksum": { "algorithm": "sha256", "value": "..." }
}
```

Paged directory files use `rfdir-v2`, which stores those pointers next to each
logical key. Layout-ordered doc pointer tables, doc-page pointer tables,
doc-value chunks, and external posting blocks embed the same pointer fields in
their owning metadata. The browser verifies the compressed range bytes against
the SHA-256 checksum before decompression, so stale CDN objects, partial uploads,
and corrupt deploys fail before a parser sees invalid bytes. Runtime
verification can be disabled with `createSearch({ verifyChecksums: false })` for
diagnostics, but the default is to verify when the index advertises
`features.checksummedObjects`.

Pack files, directory pages, and directory roots use content-addressed immutable
names. The numeric prefix keeps deterministic pack ordering, while the SHA-256
hash suffix changes whenever the bytes change. Directory pages keep compact
numeric pack indexes and resolve them through a `pack_table`, so filenames are
CDN-safe without bloating every logical entry. The optional typo sidecar
manifest is also content-addressed and referenced from the main manifest; it can
include both short-token delete-key shards and compact long-token lexicon pages.
Hosts can serve every hashed object with long-lived immutable cache headers;
only the main `manifest.json` needs revalidation or versioned publication.

The pack writer can perform a narrow ZFS-style deduplication pass at build time.
When enabled, it hashes each independently compressed object and reuses the same
pack/offset pointer for exact byte-identical objects. High-volume lanes that do
not benefit from dedupe, such as document payload packs, use append-only writers
that return object pointers without retaining a builder-wide entry map. Dedupe
is intentionally exact only: near-duplicate text or cross-block semantic dedup
would add runtime indirection and extra range fetches, which is usually a bad
trade for a latency-sensitive browser index.

`facets/packs/*.bin` store independently compressed binary facet dictionaries
addressed through `facets/directory-root.<hash>.bin.gz` and
`facets/directory-pages/*.bin.gz`. The runtime lazy-loads a dictionary only when
that facet is selected or an application asks for its values. The small facet
directory manifest is lazy at `facets/manifest.json.gz`, so filtered paths do
not need `manifest.full.json` just to resolve facet codes.

`doc-values/packs/*.bin` store range-addressed column chunks used by filters and
sorting. Keyword facets are encoded as per-document bitsets, so a facet can be
single-value or multi-value. Numeric and date fields are encoded as typed
range/sort values, and booleans use a compact tristate representation for
missing, false, and true. The manifest carries per-chunk summaries, so broad
filter and sort queries can fetch only the involved columns and skip chunks that
cannot match instead of downloading one global code table.

`doc-values/sorted/*.bin.gz` stores a lazy binary directory per numeric/date/
boolean field. Each directory points into `doc-values/sorted-packs/*.bin`, where
`rfdocvaluesortpage-v1` pages keep value-sorted `(value, docId)` rows plus
per-page min/max summaries for every sortable/filterable typed field. Sorted
top-k browse loads the directory for the requested sort field, fetches only the
next value page in sort order, and stops once the requested page is full.
The small sorted-field manifest is lazy at
`doc-values/sorted/manifest.json.gz`, so cold sort paths avoid
`manifest.full.json`.
Unsorted filter browsing keeps document order instead: it walks doc-value
chunks in doc-id order and stops after enough matches, preserving the dense
doc-page payload lane. This gives both value-order pruning for sorted views and
low-request dense browsing for broad filters.

`docs/ordinals/*.bin` is a tiny fixed-record table keyed directly by numeric
document id. It maps each document id to its retrieval-local layout ordinal.
`docs/pointers/*.bin` is a dense fixed-record pointer table in that layout order.
Result fetching no longer walks a generic string directory for documents. The
runtime range-fetches small ordinal records, uses them to fetch nearby pointer
records for text-local result sets, then range-fetches the referenced compressed
document payloads from `docs/packs/*.bin`.

`docs/pages/*.bin` is a second dense pointer table keyed by document-id page,
and `docs/page-packs/*.bin` stores `rfdocpagecols-v1` binary column pages in
original document-id order. The format stores the page field list once in the
manifest, writes typed column values for each page, and treats `index` as an
implicit document-id offset. This lane is optimized for browse, filter, and sort
result pages where returned ids are clustered. The runtime estimates page
overfetch before using it; sparse text top-k results stay on the retrieval-local
doc pack lane.

`terms/directory-root.<hash>.bin.gz` is loaded lazily on the first real term
query. It contains page bounds and a compact Bloom filter for adaptive shard
resolution. Only touched `terms/directory-pages/*.bin.gz` files are fetched, and
each page maps logical shard names to checksummed object pointers.

`terms/packs/*.bin` contain many independently compressed posting segments. The
browser requests exactly the byte span it needs and decompresses that one
segment. High-df posting lists can move their posting blocks into
`terms/block-packs/*.bin`; the posting segment then carries term metadata,
block-max scores, filter summaries, and byte ranges for external blocks.
The text top-k runtime schedules a small frontier of the highest-impact active
cursors, then batches external posting-block misses across those cursors before
issuing range requests. Each cursor keeps a contiguous cached posting window and
only refills the next window when the cached run is almost exhausted, so broad
terms behave like dynamic superblocks instead of one range request per logical
block. The same adaptive overfetch planner is used for external posting blocks
and result documents: nearby byte ranges are merged when the extra transfer is
bounded and materially reduces request count.

The builder writes postings into bounded immutable `rfsegment-v1` segment
directories under `_build/segments/`. Each segment contains a compact term
directory and append-only posting rows, so the main indexing pass no longer
builds one global posting run and sorts it at the end. The segment merger reads
term streams with a heap, merges postings per term in doc-id order, and reuses
that stream for prefix statistics, query-bundle df, typo index-term emission,
and final partition emission.

`segments/manifest.json.gz` stores `rfsegmentmanifest-v1`, a checksummed
summary of the immutable segments that produced the published index. The
builder publishes each segment's term directory and posting row file under
`segments/s*/`, then records file checksums, doc-id ranges, term and posting
counts, field lists, merge fan-in, and merge tier decisions. The runtime can
lazy-load that manifest and use a segment-fanout exact lane that searches
matching terms across published segments, applies the same global-id filters
and sort lanes, and merges the final top-k candidates exactly. The force-merged
posting layout remains the optimized default for block-max tail searches.

Segment flushing is controlled by explicit builder limits. `segmentFlushDocs`
and `segmentFlushBytes` set direct flush thresholds, while
`builderMemoryBudgetBytes` can derive a per-worker byte budget when a direct
byte threshold is not configured. Every segment manifest row records the flush
reason and estimated peak segment memory. If one document alone exceeds the byte
limit, the builder fails early instead of producing an unpredictable oversized
segment.

Segment merging uses an explicit `tiered-log` policy. Segments are ordered by
size, similarly sized batches are merged up to `segmentMergeFanIn`, and
`finalSegmentTargetCount` can request an optional force merge down to a target
count such as `1`. `segmentMergeMaxTempBytes` can block oversized merge batches;
blocked tiers are recorded instead of silently exceeding the temp budget. The
build telemetry and segment manifest record the merge target, skipped segment
count, intermediate bytes, write amplification, and whether a temp budget
prevented the requested target from being reached.

Term pack assembly uses the same append-only direction. As posting partitions
are compressed into `terms/packs/*.bin`, the builder writes compact binary
directory-entry records into `_build/terms-directory.run`. After immutable pack
renaming, those spooled rows are sorted through bounded chunks and streamed into
the paged term directory. This removes the old dependency on a retained
`packWriter.entries` map for the term directory path and avoids holding all
directory page payloads at once.

When `partitionReducerWorkers` or `builderWorkerCount` enables reducer workers,
posting partitions are represented as byte ranges into the binary reduced-term
spool. The main thread sends workers only partition descriptors: shard name,
byte range, term count, row count, and estimated input bytes. Workers stream
those ranges back from disk, so large posting arrays are not structured-cloned
between heaps and the builder does not duplicate the reduced term stream. A
reducer scheduler accounts active partition bytes with
`partitionReducerInFlightBytes`; if it is unset, the limit is derived from
`builderMemoryBudgetBytes`, or from reducer worker count and pack target size
when no global budget is configured. Workers write term packs and external
posting-block packs into the shared numeric pack directories through atomic
pack-index counters, so posting segment headers can keep stable numeric
block-pack indexes. Worker pack finalization is staggered to avoid simultaneous
completion peaks. Posting-segment objects are emitted as header/body chunks.
Small partitions use fast buffered gzip; partitions at or above
`postingSegmentStreamMinBytes` gzip directly into append-only packs while
hashing the compressed stream, which avoids retaining both a full raw segment
buffer and a full compressed buffer for large partitions. The main thread still
owns final directory assembly, which keeps range lookup deterministic while
moving partition encoding off the main event loop.

Large posting lists whose doc ids are already sorted use impact-bucket ordering
instead of a JavaScript comparator sort when the quantized impact range fits
`postingImpactBucketOrderMaxBuckets`. This keeps impact-ordered block-max output
but turns the common high-df reducer path into linear counting/bucket placement
rather than `O(n log n)` object-array sorting.

The first ingestion pass also writes two extra file-backed spools. A
selected-term spool stores each document's final selected scoring terms and
scaled impacts, so query-bundle construction can stream precomputed scoring
signals instead of re-reading and re-tokenizing the JSONL corpus. Document
payloads are stored in both compressed and raw spools: retrieval-local doc packs
reuse the compressed payload spool, while dense doc-page construction reads the
raw spool directly and avoids a build-time gzip/decompress loop.

Field rows are captured in the same scan pass through the typed
`rf-build-code-store-v1` spool. The build wraps that store as
`rffieldrows-v1`, and query bundles, doc-value chunks, sorted doc-value pages,
and filter bitmaps now consume the same typed row source. Numeric, date,
boolean, single facet, and multi-facet rows are therefore extracted once during
scan instead of re-reading the source corpus for every downstream artifact.
Reducer workers use `codeStoreWorkerCacheChunks` to cap their file-backed
field-row caches independently from the main build process. When it is `0`, the
builder chooses enough chunks to cover the corpus up to
`codeStoreWorkerMaxAutoCacheChunks`, which avoids repeated random re-reads on
large high-df posting lists while still bounding worker memory.

The final pack writer still emits immutable range-pack objects in deterministic
term order, and externalizes large posting blocks into `terms/block-packs/`.
Every build manifest includes `build.format = "rfbuildtelemetry-v1"` with
sampled per-phase memory peaks, CPU user/system time, disk byte deltas, segment
counters, and reduce/merge worker timing so large-corpus regressions can be
compared from the emitted index alone. When `buildProgressLogMs` is non-zero,
the same telemetry layer writes live phase start, heartbeat, and completion
lines to stderr with elapsed time, RSS, heap, temp bytes, pack bytes, and
sidecar bytes. This keeps long scan, reduce, typo, and segment-publish phases
observable before the final telemetry file exists.

Authority fields use the same file-backed run/reduce pattern as postings. Each
configured title, entity-name, slug, or alias value emits surface-exact,
folded-exact, and token authority keys into temporary run files. The reducer
writes `rfauth-v1` shards into immutable `authority/packs/*.bin` objects and a
paged range directory. Authority has its own shard budget
(`authorityTargetShardRows`) and deeper max shard depth
(`authorityMaxShardDepth`) and smaller directory pages
(`authorityDirectoryPageBytes`) because label lookups are point reads; they
should not inherit the larger posting-list segment and directory budgets used by
normal posting segments.
The browser probes the surface-exact key first, which keeps accent-sensitive
matches such as `Paris` and `Pâris` distinct. It only falls back to folded or
token keys when the stronger key cannot rescue the first page. Authority scores
are additive with the normal text score, but the sidecar is independent from the
main posting lists, so projects can add canonical-label quality without
bloating every text query.

Document packs contain independently compressed result-display payloads written
in retrieval-local order. The builder spools compressed payloads to disk during
ingestion, computes a compact locality record from each document's strongest
base terms, then assembles final packs by primary term and impact. The dense
ordinal table preserves direct lookup by original document id. Payloads contain
only configured display fields, not necessarily the full indexed text. A display
object can set `maxChars` to cap a returned string field while the corresponding
indexed field remains uncapped for scoring. This keeps random result-fetch
traffic bounded for long documents and avoids over-fetching a whole JSON chunk
for one result. The same spool is also read into fixed-size doc pages for dense
metadata browsing; that duplicates display payload bytes, but it removes the
ordinal and random pointer fan-out when a result page is contiguous enough.

## Retrieval Model

The builder computes weighted field term frequencies, normalizes field length,
then applies BM25F-style saturation before writing impact scores. Phrase terms
can be emitted for fields such as titles.

Each posting list is stored in impact order and split into blocks with max-impact
metadata. The runtime uses those block maxima for single-term and multi-term
top-k queries: it decodes the highest-potential blocks first and can stop once
no remaining block can change the requested top results.

For high-df terms, decoded blocks are fetched from `terms/block-packs/*.bin`
only when the block-max scheduler chooses them. The runtime prefetches a small
adjacent block window, so medium lists behave like range-addressed superblocks
while very large lists can still avoid downloading their full posting payload.
Very high-cardinality facets are omitted from posting-block summaries by
default; exact per-document filtering still uses doc-value chunks, while block
filters keep only compact summaries that are worth shipping on every term block.

For no-query metadata views, the runtime now uses doc-value pruning instead of
materializing every candidate chunk. Sort requests use the sorted doc-value tree
for the sort field and evaluate filters with page summaries before touching
per-document chunks. Filter-only browse requests use doc-id chunk summaries and
early stop as soon as `offset + size` matching documents are found, which keeps
the returned ids dense enough for binary doc pages.

## Why Custom Binary

The hot path is term-keyed inverted-list lookup, not columnar analytics. The
custom binary format keeps shard directories, block metadata, and postings in
the order the runtime reads them. Parquet or Arrow remain useful for ingestion
and benchmark artifacts, but not for the current browser posting-list hot path.

## Static Range Packs

Using thousands of tiny static files is expensive for deployment and filesystem
metadata. Using one giant compressed file prevents independent decompression.
Rangefind keeps each logical shard independently compressed, then concatenates
those compressed members into larger packs.
