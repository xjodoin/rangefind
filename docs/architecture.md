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
  typo/
    manifest.<hash>.json
    directory-root.<hash>.bin.gz
    directory-pages/
      0000.<hash>.bin.gz
    packs/
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
manifest is also content-addressed and referenced from the main manifest. Hosts
can serve every hashed object with long-lived immutable cache headers; only the
main `manifest.json` needs revalidation or versioned publication.

The pack writer also performs a narrow ZFS-style deduplication pass at build
time. It hashes each independently compressed object and reuses the same
pack/offset pointer for exact byte-identical objects. This is intentionally exact
deduplication only: near-duplicate text or cross-block semantic dedup would add
runtime indirection and extra range fetches, which is usually a bad trade for a
latency-sensitive browser index.

`facets/packs/*.bin` store independently compressed binary facet dictionaries
addressed through `facets/directory-root.<hash>.bin.gz` and
`facets/directory-pages/*.bin.gz`. The runtime lazy-loads a dictionary only when
that facet is selected or an application asks for its values.

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

`terms/packs/*.bin` contain many independently compressed logical shards. The
browser requests exactly the byte span it needs and decompresses that one shard.
High-df posting lists can move their posting blocks into
`terms/block-packs/*.bin`; the term shard then carries only term metadata,
block-max scores, filter summaries, and byte ranges for external blocks.
The runtime uses an adaptive overfetch planner for external posting blocks and
result documents: it merges nearby byte ranges when the extra transfer is bounded
and materially reduces request count.

The builder writes temporary posting and typo runs with a compact binary record
format instead of TSV. Runs are still partitioned by base shard, so reduction can
stream bounded shard groups without holding the whole corpus index in memory.
Base-shard reduction can run in worker threads. Workers write compressed logical
shard files and typo index-term run files into `_build/`; the parent then
assembles final range packs in sorted task order so pack offsets stay
deterministic. Set `reduceWorkers` in the config to control the worker count;
`1` is the default, while `0` or `"auto"` uses up to four workers.

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
