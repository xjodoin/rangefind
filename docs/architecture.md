# Architecture

Rangefind separates logical retrieval units from physical deployment files.

## Build Output

```text
rangefind/
  manifest.json
  codes.bin.gz
  docs/
    0000.json
  terms/
    ranges.bin.gz
    packs/
      0000.bin
      0001.bin
```

`manifest.json` is small enough for page initialization. It lists logical shard
names, schema, dictionaries, and the default results.

`terms/ranges.bin.gz` is loaded lazily on the first real term query. It maps
logical shard names to `[packIndex, offset, length]` tuples.

`terms/packs/*.bin` contain many independently compressed logical shards. The
browser requests exactly the byte span it needs and decompresses that one shard.

The builder writes temporary posting and typo runs with a compact binary record
format instead of TSV. Runs are still partitioned by base shard, so reduction can
stream one bounded shard group at a time without holding the whole corpus index
in memory.

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
