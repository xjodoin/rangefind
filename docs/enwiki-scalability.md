# Full English Wikipedia scalability validation

Validated against the July 2026 English Wikipedia articles dump on a 14-core,
24 GiB Apple Silicon machine. The extracted JSONL and build workspace lived on
an APFS sparsebundle backed by an external exFAT SSD.

## Corpus and final index

| Metric | Result |
| --- | ---: |
| Source pages read | 26,204,500 |
| Accepted main-namespace, non-redirect documents | 7,194,531 |
| Extracted JSONL | 19 GiB |
| Distinct terms | 11,919,561 |
| Postings | 176,405,203 |
| Unique posting shards | 77,433 |
| Posting term packs | 253 files, 758.5 MiB |
| External posting blocks | 292.4 MiB |
| Document payload packs | 4.59 GiB |
| Document page packs | 2.73 GiB |

The dump was extracted from 72 ordered multistream shards with three concurrent
downloads. Completed shard checkpoints were reused after throttled downloads,
and final concatenation preserved page order.

## Builder timings

Phase timings are assembled from successful checkpointed passes so individual
changes could be measured without redownloading or rescanning the corpus.

| Phase | Time | Peak RSS / note |
| --- | ---: | --- |
| Measure | 6m12s | 2.83 GiB |
| Scan and spool | 12m21s | 6.21 GiB before the authority-buffer fix |
| Reduce postings, gzip level 3 | 30m33s | 6.25 GiB |
| Segment manifest | 25s | — |
| Authority | 2m01s | about 3 GiB |
| Document packs, sequential doc-id | 1m34s | bounded 64 MiB read windows |
| Document pages | 41s | — |
| Doc values + sorted doc values | 37s | — |
| ID map + filters + facet dictionaries | 21s | — |

The initial level-6 reducer took 43m08s. Level 3 reduced that to 30m33s
(29% faster) while term-pack bytes increased 6.2%; external block bytes were
effectively unchanged. Auto block codecs saved 544 MiB relative to the
pair-varint baseline.

Locality-ordered document packing was unsuitable for this slow external-volume
path: millions of small random reads produced less than 1 MiB/s and projected
to hours. Doc-id packing with sequential windows wrote the 4.59 GiB spool in
1m34s. On the 100k control corpus, the full build improved from 25.8s to 20.0s;
cold-query bytes rose by only 0.04–2%, with unchanged top results.

## Cold local query traces

These numbers reset Rangefind's runtime caches for each query and use local
positional reads; the operating-system file cache may still be warm. The large
index default uses a 128-block approximate top-k budget.

| Query | Time | Top result | Blocks | Bytes read |
| --- | ---: | --- | ---: | ---: |
| `United States` | 128 ms | United States | 128 | 2.28 MiB |
| `Wikipedia` | 7 ms | Wikipedia | 1 | 316 KiB |
| `football` | 18 ms | Football | 1 | 874 KiB |
| `machine learning` | 18 ms | Machine learning | 15 | 566 KiB |
| `New York City` | 66 ms | New York City | 128 | 1.12 MiB |
| `climate change` | 40 ms | Climate change | 128 | 724 KiB |
| `medicine` | 32 ms | Medicine | 97 | 555 KiB |
| `Barack Obama` | 11 ms | Barack Obama | 3 | 550 KiB |

Without a block budget, `United States` decoded 6,527 blocks and 1.67 million
postings in 13.4 seconds. The 128-block lane kept the same top result in about
128 milliseconds (roughly 105 times faster). Callers can pass
`topKBlockBudget: 0` or request exact search when exhaustive proof matters.

## Scale-only defects found by the run

- Auto-codec sampling could loop forever after seeding both endpoints.
- Authority rows referenced a removed flush option and accumulated in heap.
- External document-layout merge scanned every reader for every document.
- Single-buffer document preload failed above Node's 2 GiB Buffer limit.
- Short shard keys padded with `_` collided with real underscore expansion
  terms. The first full reducer emitted 1,693 duplicate keys; the corrected run
  emitted 77,433 records and 77,433 unique keys.
- Extraction and JSONL prefix writes ignored stream backpressure.

The full external artifact is under
`/Volumes/RangefindWiki/rangefind/enwiki-full`; the served index is under its
`public/rangefind` directory.
