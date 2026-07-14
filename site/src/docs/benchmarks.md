---
title: Benchmarks
lede: Every number here comes from a committed script you can rerun — with relevance checked against Lucene and Tantivy oracles.
description: Measured rangefind performance — build times, index sizes, and query latency/transfer on French and English Wikipedia and OpenStreetMap corpora, plus sharding overhead and quality methodology.
order: 13
---

All figures below were measured on a 14-core laptop with the benchmark
scripts committed in the repository (`scripts/*_bench*.mjs`,
`npm run bench:*`); cold numbers reset every cache first. Corpora are public
so you can reproduce end to end.

## Build

| Corpus | Documents | Build time | Index size | Peak RSS |
| --- | --- | --- | --- | --- |
| French Wikipedia | 2,753,081 | ~10.5 min | 5.3 GB | 4.7 GiB |
| English Wikipedia | 7,194,531 | ~52 min | ~9 GB | 6.1 GiB |
| OSM Québec | 6,095,740 | ~7 min | 9.5 GB | — |
| OSM United States | 32,809,763 | ~27 min | 11 GB | — |

The builder is file-backed throughout — postings spill to bounded segments
and merge in tiers — so memory stays flat as corpora grow, and every phase
checkpoints for resumable builds.

## Query — text (English Wikipedia, 7.2M pages, cold)

| Query shape | Latency | Transfer |
| --- | --- | --- |
| single term | 7–66 ms | tens–hundreds of KB |
| `United States` (two high-frequency terms) | 128 ms | 2.3 MiB |
| autocomplete prefix (`mach`) | 49 ms | 417 KB |

Early termination is what makes the two-word case work: exact evaluation of
that query would read ~13 s of postings; impact-ordered blocks with a block
budget reach the same top-10 105× faster.

## Query — geo (OpenStreetMap, cold)

| Corpus | Points | Nearest (cold) | Warm |
| --- | --- | --- | --- |
| Québec | 6.1M | 5–13 ms | 0.1–0.4 ms |
| United States | 32.8M | 7–16 ms | ≤ 3 ms |

Cold nearest-neighbor touches 1–3 tree leaves regardless of corpus size —
the KD tree's cost scales with query selectivity, not point count.

## Sharding overhead: none

Splitting the Québec corpus into 4 geographic shards, including the
corpus-wide statistics pass ([how that works](../sharded-indexes/)):

| | Monolithic | Sharded (4) |
| --- | --- | --- |
| Build (same machine, sequential) | 421.5 s | 423.1 s |
| Index size | 9.54 GB | 9.49 GB |
| Nearest + facets (cold) | 4 ms / 348 KB | 4.9 ms / 165 KB |
| Text + geo boost (cold) | 389 ms / 21.5 MB | 262 ms / 10.1 MB |
| Rankings | — | identical (score drift 0.0) |

Geo-routed lanes transfer *less* than the monolith because per-shard
structures are smaller; text-only queries pay a fan-out that a locality
routing layer will remove.

## Quality methodology

Speed without relevance is easy. The repository carries quality harnesses
that replay query sets against **Lucene** and **Tantivy** as oracles and
compare ranked results, plus a search-benchmark-game harness for
apples-to-apples latency. Typo correction, phrase handling, and multilingual
analysis are all covered by the 220+ test suite that runs on every commit.

## Reproduce it

```bash
git clone https://github.com/xjodoin/rangefind && cd rangefind && npm install
npm run bench:frwiki        # French Wikipedia end-to-end
npm run bench:osm-geo       # Luxembourg geo suite (small, fast)
npm run bench:osm-shards    # Québec sharded-vs-monolithic comparison
```
