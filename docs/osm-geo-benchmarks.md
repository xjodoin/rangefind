# OSM Geo Query Benchmarks

Cold numbers are a fresh runtime session (manifest boot measured separately)
running one query; warm numbers are repeat queries in the same session.
Every lane is verified against exhaustive Haversine scans of the source JSONL
(`scripts/osm_geo_bench.mjs`, oracle section), including exact
nearest-neighbor order for both empty-query and text queries. Queries run
from the densest 0.05° cell of the extract (the busiest urban core).
Recorded 2026-07-06 on an Apple Silicon laptop over localhost HTTP; expect
network latency, not transfer, to dominate real deployments.

Reproduce with:

```bash
npm run bench:osm-geo          # Luxembourg
npm run bench:osm-geo:quebec   # Quebec
RANGEFIND_OSM_US_ROOT=/Volumes/large-disk/osm-us npm run bench:osm-geo:us
```

The oracle is a two-pass bounded stream: the first pass locates the densest
0.05° cell and the second keeps only exact counters and the nearest 20 rows.
It never loads the corpus or per-document token sets into memory.

National builds selectively share category/type/population code columns
across posting-reducer workers; unrelated geo columns no longer push the
all-or-nothing preload over budget. On the 32.8M-place US corpus, reduction
completed in 2m41s versus an aborted baseline that had produced no pack bytes
after 29m35s. Batched dense-pointer assembly reduced document packing from
4m33s to 2m44s.

## Luxembourg (175,276 places, single-level tree, 512 leaves)

Index: 172 MB total, geo sidecar 1.5 MB. Build: 5 s end to end.

| lane                 | cold req | cold KB | cold ms | warm ms | leaves visited |
| -------------------- | -------- | ------- | ------- | ------- | -------------- |
| viewport browse      | 19       | 91      | 19.0    | 0.09    | 1/41 candidates |
| wide viewport        | 6        | 103     | 5.0     | 0.07    | 1/216 |
| radius 1 km          | 13       | 115     | 6.5     | 0.16    | 1/7 |
| radius 5 km          | 17       | 82      | 6.2     | 0.15    | 1/81 |
| nearest (exact)      | 15       | 81      | 10.2    | 0.30    | 1/512 |
| nearest + radius     | 15       | 81      | 9.6     | 0.16    | 1/81 |
| nearest + facet      | 23       | 159     | 10.7    | 0.34    | 3/510 |
| text nearest (exact) | 25       | 230     | 14.8    | 0.57    | 7/512 |
| text + radius 5 km   | 45       | 756     | 32.4    | 1.97    | doc-set lane |
| text + viewport      | 32       | 1010    | 32.5    | 2.91    | doc-set lane |
| text + boost         | 45       | 756     | 26.6    | 1.76    | doc-set lane |
| text only baseline   | 12       | 93      | 6.0     | 0.17    | — |

## Quebec (4,269,316 places, two-level tree, 16,384 leaves / 64 branches, 9.9 KB root)

Index: 3.4 GB total, geo sidecar 37 MB. Build: 139 s end to end (geo tree
phase with per-leaf filter summaries: ~4 s). The bounded PBF→JSONL pipeline
converts the 1.16 GB extract in 77.5 s at 462 MiB peak RSS; the former
in-memory anchor maps needed 113.7 s and 2.71 GB.

| lane                 | cold req | cold KB | cold ms | warm ms | leaves visited |
| -------------------- | -------- | ------- | ------- | ------- | -------------- |
| viewport browse      | 19       | 74      | 22.3    | 0.09    | 1 |
| wide viewport        | 11       | 73      | 7.8     | 0.08    | 1 |
| radius 1 km          | 12       | 69      | 6.7     | 0.13    | 1 |
| radius 5 km          | 19       | 58      | 8.1     | 0.11    | 1 |
| nearest (exact)      | 18       | 113     | 7.7     | 0.18    | 1 (1 branch page) |
| nearest + radius     | 18       | 113     | 7.2     | 0.21    | 1 |
| nearest + facet      | 16       | 890     | 29.3    | 0.24    | 2 (534 KB is the one-time category bitmap) |
| text nearest (exact) | 24       | 298     | 30.6    | 9.76    | 6/256 |
| text + radius 5 km   | 52       | 1805    | 78.4    | 4.95    | doc-set lane, 66k in-radius docs resolved |
| text + viewport      | 87       | 2921    | 65.2    | 0.46    | doc-value verify lane |
| text only baseline   | 11       | 215     | 10.6    | 0.12    | — |

`text nearest` is exact "closest matches first": the runtime resolves the
full text match set from postings (bounded by `geoTextSortMaxDf`), then the
tree orders it with the nearest early-stop proof.

## United States (32,809,763 places, 65,536 leaves / 256 branches, 29 KB root)

Recorded 2026-07-09 from the checksum-verified 11.2 GB Geofabrik US extract.
The published index is approximately 11 GB excluding resumable `_build`
scratch; the geo family is 285 MB. The measured optimized phase sum is about
27m25s (measurement 40s, scan 11m56s, reduction 2m41s, authority 6m48s,
document packing 2m49s, geo 27s, and the remaining phases about 2m04s).
Extraction produced 32,809,763 JSONL rows in 14m14s at 503 MiB peak RSS;
entity-selective PBF decoding added afterward removes unused node/way parsing
from future runs.

| lane | cold req | cold KB | cold ms | warm ms | leaves / points scanned |
| --- | ---: | ---: | ---: | ---: | ---: |
| viewport browse | 9 | 140 | 26.4 | 0.10 | 1 / 501 |
| radius 5 km | 18 | 81 | 14.1 | 0.14 | 1 / 501 |
| nearest (exact) | 23 | 154 | 16.3 | 0.23 | 1 / 500 |
| nearest + facet | 30 | 6,724 | 155.6 | 0.34 | 3 / 1,501 |
| text nearest (`bakery`) | 35 | 763 | 70.4 | 71.05 | 39 / 19,525 |
| text + radius 5 km | 64 | 6,255 | 189.4 | 3.62 | 95 / 47,562 |
| text-only baseline | 28 | 518 | 53.3 | 23.84 | — |

All four streaming-oracle checks passed over all 32,809,763 source points.
The common facet and broad text+geo lanes pay for whole-corpus bitmap or
doc-value verification; unfiltered exact nearest remains effectively
constant-cost and settles after one 500-point leaf.

## What the two-level root bought

At Quebec scale the initial flat root was 1 MB gzipped and every geo query
paid for it cold:

| lane            | flat root       | branch-paged root |
| --------------- | --------------- | ----------------- |
| nearest         | 1054 KB         | 113 KB            |
| radius 5 km     | 1710 KB         | 58 KB             |
| nearest + facet | 15 MB / 896 req / 272 ms | 890 KB / 16 req / 29 ms |

Geo query cost now scales with the constraint's selectivity (branch pages and
leaves touched), not with the total indexed point count. The exact
nearest-neighbor proof typically settles within one or two leaves.

## Per-cell filter summaries

Leaf and branch entries carry posting-block-style filter summaries (facet
words, numeric min/max, boolean bounds). Cells that provably contain no
matching document are pruned before any page fetch, so a nearest query for a
category that only exists in one region never walks the rest of the tree.
In the urban-core benchmark above every cell contains common amenities, so
the summaries mostly show up in sparse-category and rural queries (covered by
the oracle test suite).

## Known limitations

- Text queries over very wide geo constraints (hundreds of thousands of
  candidate points) fall back to per-document lat/lon doc-value verification;
  a broad viewport over a dense city can transfer a few MB. Posting-block
  lat/lon summaries or spatial doc ordering would be the format-level fix.
- Facet-filtered geo lanes load whole-corpus filter bitmaps (~130 KB per
  million docs per selected value, cached per session) for per-document
  verification inside partially matching cells.
- Text + distance sort decodes the full postings of the query terms, bounded
  by `geoTextSortMaxDf` (default 200k postings); broader queries get a clear
  budget error and can rank by relevance with `geo.boost` instead.
