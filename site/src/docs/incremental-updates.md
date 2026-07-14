---
title: Incremental updates
lede: Publish what changed — a few kilobytes of delta — and keep every CDN cache entry you already earned.
description: Generational index updates with rangefind build --update — frozen corpus statistics, tombstones, compaction, and CDN-friendly publishing.
order: 6
---

Rebuilding a large index for every content change wastes build time and,
worse, invalidates every cached pack on your CDN. Generational updates fix
both:

```bash
# once: full build
npx rangefind build --config rangefind.config.json

# each update: feed ONLY the new + changed documents
npx rangefind build --config rangefind.config.json --update
```

`--update` writes a small, complete **delta generation** next to the base
index and rewrites only the root manifest. Unchanged packs keep their
content-addressed names — and their cache entries. Documents whose ids
reappear in a delta **replace** their old versions via tombstones.

## Scores stay exactly comparable

The trap in incremental search indexes is statistics drift: BM25 depends on
corpus-wide document frequencies and averages, so naively indexing a delta
produces scores that can't be merged with the base. Rangefind freezes the
base generations' statistics into each delta build — a document added by a
delta scores **identically** to the same document in a full rebuild of the
base corpus. The test suite asserts this to nine decimal places.

At query time, `createSearch` fans out across generations and merges every
lane — text top-k, facet counts, sorted browse (by real keys), geo, vectors,
suggest — behind the same engine interface. Callers never know generations
exist.

## Compaction

Each generation adds a small per-query fan-out. When they pile up (the
builder warns at 8 generations or 25% tombstones):

```bash
npx rangefind build --config rangefind.config.json --compact
```

folds everything back into a single fresh index with up-to-date statistics.
A sensible cadence: deltas continuously, compaction on a schedule.

## Limits worth knowing

- Deltas **cannot delete** documents that never reappear — deletions wait
  for the next compaction/full rebuild. Track them and compact when the
  stale fraction matters for your product.
- Deltas must use the **same analysis profile** as the base (enforced).
- `count()` over a generational index cannot subtract tombstoned matches;
  it over-counts by at most the tombstones and flags itself `approximate`.

For sharded corpora the same mechanism updates one region at a time — a
nightly OpenStreetMap region refresh ships as a single small generation.
See [Sharded indexes](../sharded-indexes/).
