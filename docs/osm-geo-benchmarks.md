# OSM Geo Query Benchmarks

Cold numbers are a fresh runtime session (manifest boot measured separately)
running one query; warm numbers are repeat queries in the same session.
Every lane is verified against exhaustive Haversine scans of the source JSONL
(`scripts/osm_geo_bench.mjs`, oracle section). Queries run from the densest
0.05° cell of the extract (the busiest urban core). Recorded 2026-07-06 on an
Apple Silicon laptop over localhost HTTP; expect network latency, not
transfer, to dominate real deployments.

Reproduce with:

```bash
npm run bench:osm-geo          # Luxembourg
npm run bench:osm-geo:quebec   # Quebec (needs ~16 GB RAM for the build)
```

## Luxembourg (175,276 places, single-level tree, 512 leaves, 33 KB root)

Index: 172 MB total, geo sidecar 1.5 MB. Build: 5 s end to end.

| lane                | cold req | cold KB | cold ms | warm ms | leaves visited |
| ------------------- | -------- | ------- | ------- | ------- | -------------- |
| viewport browse     | 19       | 56      | 18.2    | 0.08    | 1/41 candidates |
| wide viewport       | 6        | 67      | 5.0     | 0.07    | 1/216 |
| radius 1 km         | 13       | 79      | 5.4     | 0.15    | 1/7 |
| radius 5 km         | 17       | 46      | 5.4     | 0.16    | 1/81 |
| nearest (exact)     | 15       | 45      | 6.4     | 0.20    | 1/512 |
| nearest + radius    | 15       | 45      | 5.0     | 0.19    | 1/81 |
| nearest + facet     | 23       | 124     | 10.0    | 0.23    | 3/512 |
| text + radius 5 km  | 45       | 720     | 33.3    | 1.93    | doc-set lane |
| text + viewport     | 32       | 974     | 34.7    | 2.78    | doc-set lane |
| text + boost        | 45       | 720     | 30.6    | 2.31    | doc-set lane |
| text only baseline  | 12       | 93      | 6.3     | 0.17    | — |

## Quebec (4,269,316 places, two-level tree, 16,384 leaves / 64 branches, 4.8 KB root)

Index: 3.4 GB total, geo sidecar 37 MB. Build: 138 s end to end (geo tree
phase: 2.5 s). PBF→JSONL conversion (1.16 GB extract, pure Node): 103 s.

| lane                | cold req | cold KB | cold ms | warm ms | leaves visited |
| ------------------- | -------- | ------- | ------- | ------- | -------------- |
| viewport browse     | 19       | 53      | 20.2    | 0.09    | 1 |
| wide viewport       | 11       | 53      | 5.8     | 0.06    | 1 |
| radius 1 km         | 12       | 48      | 5.8     | 0.12    | 1 |
| radius 5 km         | 19       | 38      | 6.3     | 0.10    | 1 |
| nearest (exact)     | 18       | 92      | 7.1     | 0.17    | 1 (1 branch page) |
| nearest + radius    | 18       | 92      | 6.4     | 0.20    | 1 |
| nearest + facet     | 16       | 869     | 23.4    | 0.16    | 2 (534 KB is the one-time category bitmap) |
| text + radius 5 km  | 52       | 1709    | 73.0    | 5.72    | doc-set lane, 66k in-radius docs resolved |
| text + viewport     | 87       | 2826    | 60.5    | 0.38    | doc-value verify lane |
| text only baseline  | 11       | 215     | 10.5    | 0.11    | — |

## What the two-level root bought

At Quebec scale the initial flat root was 1 MB gzipped and every geo query
paid for it cold:

| lane            | flat root       | branch-paged root |
| --------------- | --------------- | ----------------- |
| nearest         | 1054 KB         | 92 KB             |
| radius 5 km     | 1710 KB         | 38 KB             |
| nearest + facet | 15 MB / 896 req / 272 ms | 869 KB / 16 req / 23 ms |

Geo query cost now scales with the constraint's selectivity (branch pages and
leaves touched), not with the total indexed point count. The exact
nearest-neighbor proof typically settles within one or two leaves.

## Known limitations

- Text queries over very wide geo constraints (hundreds of thousands of
  candidate points) fall back to per-document lat/lon doc-value verification;
  a broad viewport over a dense city can transfer a few MB. Posting-block
  lat/lon summaries are the planned fix.
- Facet-filtered geo lanes load whole-corpus filter bitmaps (~130 KB per
  million docs per selected value, cached per session). Per-leaf facet
  summaries would remove that cost.
- `geo.sort: "distance"` is exact but only supported for empty-query browse;
  text queries rank by relevance with optional distance boost instead.
