---
title: How it works
lede: A search engine is mostly reads. Rangefind moves all the writing to build time, so query time is just a few small, cacheable range requests.
description: The architecture of a rangefind index — packed immutable files, range directories, impact-ordered postings, and a browser runtime that reads only the bytes a query touches.
order: 2
---

## The core idea

Classic search engines keep an inverted index in memory behind a server
because queries need random access into large data structures. Rangefind
keeps the same kind of data structures — an inverted index, document store,
facet dictionaries, geo trees — but lays them out in **flat, immutable
files designed for remote random access**. The browser becomes the search
server: HTTP `Range` requests are its `pread`.

A query for `harbor lights` costs roughly:

1. **Manifest** — one small JSON fetch (cached after the first query).
2. **Term directory pages** — binary-search into a paged directory to find
   where each term's postings live.
3. **Postings** — range-read the impact-ordered posting blocks for both
   terms; early termination stops as soon as the top-k is provably correct.
4. **Documents** — range-read the payloads for the ten winners.

On a 7.2M-page English Wikipedia index, that whole cold sequence is
~128 ms and 2.3 MiB for a two-word query — and single-term queries run
7–66 ms. Warm queries reuse cached directory pages and often touch one
request, or none.

## What's in the index directory

```
rangefind/
  manifest.min.json          entry point: schema, analysis profile, pointers
  terms/packs/*.bin          posting segments, impact-ordered, compressed
  terms/directory-*.bin.gz   paged binary range directory
  docs/packs/*.bin           locality-ordered document payloads
  docs/pages/*.bin           dense page payloads for browse/filter/sort
  doc-values/                typed numeric/date/boolean columns
  facets/                    range-packed facet dictionaries
  geo/                       static KD trees (when geo fields exist)
  vectors/                   int8 IVF index (when vector fields exist)
  authority/                 exact-match + autocomplete sidecar
  typo/                      vocabulary shards for correction probes
```

Three properties make this remote-friendly:

- **Content-addressed, immutable names.** Every pack embeds a hash of its
  content in its filename, so CDNs cache them forever and updates never
  serve stale bytes. Only the small manifests are mutable.
- **Impact-ordered postings.** Posting lists are sorted by score
  contribution, so the runtime can stop reading a list early once no
  remaining entry could reach the top-k — bounded transfer even for terms
  that match millions of documents.
- **Checksummed object pointers.** Every read is verified (SHA-256) before
  decompression, so a corrupted or truncated response fails loudly instead
  of silently corrupting results.

## The build pipeline

`rangefind build` runs a file-backed pipeline that never needs the corpus in
memory: a measure pass computes corpus statistics; a scan pass tokenizes
documents in worker threads and spills postings into bounded immutable
segments; a reduce pass merges segments into the final packs; sidecar passes
write facets, doc-values, geo trees, vectors, autocomplete, and typo
structures. Every phase checkpoints, so an interrupted build resumes instead
of restarting — 2.75M French Wikipedia pages build in about 10 minutes on a
laptop with peak memory under 5 GB.

## Relevance

Scoring is BM25F: fields carry weights and length normalization (`title`
counts more than `body`), phrase and proximity signals boost adjacent
matches in fields that opt in, and per-term scores are pre-baked into
integer impacts at build time. Query-time work is addition and a heap — no
floating-point scoring loops over millions of candidates.

When a query returns nothing (or only weak matches), [typo
correction](../query-api/#typo-correction) probes a few vocabulary shards
for close spellings and retries the strongest corrected plans — the fuzzy
cost is paid only by queries that need it.

## One runtime, three index shapes

`createSearch` inspects the manifest and returns the same engine interface
for:

- a **single index** — the common case;
- a **generational index** — a base plus small
  [delta generations](../incremental-updates/) published by `build --update`;
- a **sharded index** — many independent regional indexes
  [federated at query time](../sharded-indexes/) with exactly comparable
  scores.

Layers compose: a shard can itself be generational, and the runtime routes,
merges, and de-duplicates across all of it transparently.
