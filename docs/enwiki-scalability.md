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
| Measure | 5m24s | 2.82 GiB |
| Scan and spool with unified autocomplete | 14m35s | 4.20 GiB |
| Reduce postings, gzip level 3 | 30m42s | 6.10 GiB RSS, about 2.1–2.5 GiB V8 heap |
| Segment manifest | 15–24s | — |
| Authority + autocomplete | 4m17s–4m55s | 2.86 GiB first pass; 1.47 GiB resumed pass |
| Document packs, sequential doc-id | 1m44s–1m47s | bounded 64 MiB read windows |
| Document pages | 33s–47s | — |
| Doc values + sorted doc values | 36s–38s | — |
| ID map + filters + facet dictionaries | 21s | — |

The initial level-6 reducer took 43m08s. Level 3 reduced that to 30m33s
(29% faster) while term-pack bytes increased 6.2%; external block bytes were
effectively unchanged. Auto block codecs saved 544 MiB relative to the
pair-varint baseline.

The unified scan emits 18.95 million autocomplete rows in the same pass, so it
takes 2m14s longer than the former authority-only scan. Its peak RSS is 4.20
GiB, 32% below the former 6.21 GiB peak, because autocomplete records flush to
external runs every 5,000 rows instead of accumulating a corpus-sized map.

A fresh reducer run exposed a second scale boundary: opening 512 compact term
directories together inflated 457 MiB of binary metadata into roughly 4 GiB
of V8 objects. Merge admission is now bounded independently by encoded
directory bytes (64 MiB by default). The resumed reducer completed in 30m42s,
within nine seconds of the previous baseline, with identical 758.5 MiB term
packs and 292.4 MiB external blocks and without increasing Node's heap limit.

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

## Full-corpus autocomplete and typo recovery

The published build uses `rfauth-v2` and has no `suggest/` directory,
`manifest.suggest` branch, suggestion pack table, or suggestion page codecs.
Autocomplete lives in the authority packs as compact terminal summaries:

| Metric | Result |
| --- | ---: |
| Autocomplete keys | 18,717,147 |
| Source rows | 18,948,931 |
| Autocomplete authority shards | 56,066 |
| Referenced authority pack bytes (all authority lanes) | 424.1 MiB |
| Lexicon root | 177 KiB compressed |
| One-character hot lists | 479 prefixes, 50 KiB total |

Cold local traces reset Rangefind's caches before each request. Bytes exclude
the manifest opened while creating the engine but include the lexicon root,
authority directory data, and exact shard ranges.

| Prefix | Time | Bytes | Shards | First suggestion |
| --- | ---: | ---: | ---: | --- |
| `m` | 20 ms | 177 KiB | hot list | Ma' |
| `mach` | 49 ms | 417 KiB | 7 / 24 candidates | Macham |
| `artificial int` | 16 ms | 371 KiB | 1 | Artificial intimacy |
| `cafe` | 20 ms | 383 KiB | 1 | Café Frascati |
| `modern app` | 25 ms | 392 KiB | 1 | Artificial Intelligence: A Modern Approach |
| `mechanical scattering` | 18 ms | 399 KiB | 1 | Quantum mechanical scattering of photon and nucleus |

Unweighted indexes rank direct surface prefixes ahead of token-suffix matches;
within each class they keep duplicate-count popularity and prefer concise
labels. Weighted indexes keep the configured weight as the primary signal.
The root stores the same composite rank per shard, so best-first traversal still
has an exact stop proof. The last two traces prove mid-title recall without a
second suggestion index.

A full-corpus collision case also exposed a typo-planning weakness:
`artificial inteligence` had zero joint hits even though the rare misspelling
was itself an indexed term. Correcting tokens in query order spent the bounded
shard budget on variants of the common word `artificial`. Zero-hit conflict
recovery now probes tokens in ascending document-frequency order, producing
`artificial intelligence` in 115 ms while retaining the same 12-shard cap.

## Scale-only defects found by the run

- Auto-codec sampling could loop forever after seeding both endpoints.
- Authority rows referenced a removed flush option and accumulated in heap.
- A 512-way tier merge decoded 457 MiB of compact term directories into enough
  JavaScript objects to exhaust V8; directory-byte admission now bounds it.
- Scan checkpoints duplicated 67.7 MiB of facet dictionaries already stored in
  the measure checkpoint; resumed builds now reattach only the canonical copy.
- External document-layout merge scanned every reader for every document.
- Single-buffer document preload failed above Node's 2 GiB Buffer limit.
- Short shard keys padded with `_` collided with real underscore expansion
  terms. The first full reducer emitted 1,693 duplicate keys; the corrected run
  emitted 77,433 records and 77,433 unique keys.
- Extraction and JSONL prefix writes ignored stream backpressure.
- Zero-hit typo recovery could exhaust its shard budget on a common first token
  before examining a rare, valid-but-conflicting misspelling.
- Building a second full-title suggestion map was unnecessary: autocomplete
  terminals, shard maxima, and hot lists now share the authority reducer and
  packs while preserving weighted and token-suffix completion.

The full external artifact is under
`/Volumes/RangefindWiki/rangefind/enwiki-full`; the served index is under its
`public/rangefind` directory.
