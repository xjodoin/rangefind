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
  codes.bin.gz
  docs/
    directory-root.bin.gz
    directory-pages/
      0000.bin.gz
    packs/
      0000.bin
  terms/
    directory-root.bin.gz
    directory-pages/
      0000.bin.gz
    block-packs/
      0000.bin
    packs/
      0000.bin
      0001.bin
```

`manifest.json` is small enough for page initialization. It lists schema,
dictionaries, default results, and pointers to the paged range directories.

`terms/directory-root.bin.gz` is loaded lazily on the first real term query. It
contains page bounds and a compact Bloom filter for adaptive shard resolution.
Only touched `terms/directory-pages/*.bin.gz` files are fetched, and each page
maps logical shard names to `[packIndex, offset, length]` tuples.

`terms/packs/*.bin` contain many independently compressed logical shards. The
browser requests exactly the byte span it needs and decompresses that one shard.
High-df posting lists can move their posting blocks into
`terms/block-packs/*.bin`; the term shard then carries only term metadata,
block-max scores, filter summaries, and byte ranges for external blocks.

The builder writes temporary posting and typo runs with a compact binary record
format instead of TSV. Runs are still partitioned by base shard, so reduction can
stream bounded shard groups without holding the whole corpus index in memory.
Base-shard reduction can run in worker threads. Workers write compressed logical
shard files and typo index-term run files into `_build/`; the parent then
assembles final range packs in sorted task order so pack offsets stay
deterministic. Set `reduceWorkers` in the config to control the worker count;
`1` is the default, while `0` or `"auto"` uses up to four workers.

Document packs contain independently compressed result-display payloads addressed
through the same paged range-directory pattern as term shards. They contain only
configured display fields, not necessarily the full indexed text. A display
object can set `maxChars` to cap a returned string field while the corresponding
indexed field remains uncapped for scoring. This keeps random result-fetch
traffic bounded for long documents and avoids over-fetching a whole JSON chunk
for one result.

## Retrieval Model

The builder computes weighted field term frequencies, normalizes field length,
then applies BM25F-style saturation before writing impact scores. Phrase terms
can be emitted for fields such as titles.

Each posting list is stored in impact order and split into blocks with max-impact
metadata. The runtime uses those block maxima for multi-term top-k queries: it
decodes the highest-potential blocks first and can stop once no remaining block
can change the requested top results. Single-token queries use the direct
posting decode path, which is faster when there is no cross-term pruning
opportunity.

For high-df terms, decoded blocks are fetched from `terms/block-packs/*.bin`
only when the block-max scheduler chooses them. The runtime prefetches a small
adjacent block window, so medium lists behave like range-addressed superblocks
while very large lists can still avoid downloading their full posting payload.

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
