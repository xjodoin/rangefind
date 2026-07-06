# Search-As-You-Type Benchmarks

`scripts/suggest_bench.mjs` replays realistic keystroke sequences against a
built index over localhost HTTP, measuring requests, transfer, and latency
per keystroke. Top-k exactness is verified against an exhaustive aggregation
of the source corpus (test suite plus a full-corpus oracle run at Quebec
scale). Recorded 2026-07-06 on an Apple Silicon laptop.

Reproduce with:

```bash
npm run bench:suggest                                 # Luxembourg
node scripts/suggest_bench.mjs --root=examples/osm-geo  # any built index
```

## Luxembourg (175k places → 37.6k surfaces → 82.3k keys)

Sidecar: 1.1 MB, 322 pages, single-level root 27 KB (including per-character
hot pages). Suggest build phase: well under a second.

| metric | value |
| ------ | ----- |
| first keystroke of a session | 2 requests, ~28 KB (root + hot page) |
| later keystrokes, mean | 0.57 ms, 5.1 KB |
| keystrokes served fully from cache | 59 / 75 |

## Quebec (4.27M places → 255k surfaces → 679k keys)

Sidecar: 8.1 MB, 2,651 pages, two-level root 11.6 KB, 11 branch pages.
Suggest build phase: 2.4 s of a 148 s total build (scan-side surface
aggregation adds ~10 s at this scale).

| metric | value |
| ------ | ----- |
| first keystroke of a session | 2 requests, ~12.8 KB |
| later keystrokes, mean | 1.11 ms, 10.7 KB |
| keystrokes served fully from cache | 47 / 75 |

Example completions (population-weighted, diacritic-folded input):
`m` → Montréal, `riviere` → Trois-Rivières, `tim` → Tim Hortons,
`ecole` → Rue de l'École, `boulang` → Rue du Boulanger.

## Why keystrokes are cheap

- The root (page `minKey` + `maxWeight` summaries, branch-paged above ~512
  pages) is fetched once per session and answers "which pages could match"
  entirely client-side.
- A prefix maps to one contiguous run of pages; the runtime visits them
  best-first by `maxWeight` and stops with a block-max proof, so exact top-k
  rarely needs more than a few pages even for wide prefixes.
- Single-character prefixes — the first keystroke of every session and the
  widest key ranges — are answered from precomputed per-character hot pages:
  one ~1 KB fetch, already ranked and deduplicated.
- Deeper keystrokes narrow the same key range, so they mostly re-read pages
  the session already cached (zero requests).

## Edge case worth knowing

When more entries share one key than fit in a page (hundreds of surfaces end
with the token "montreal" or "riviere"), consecutive pages carry equal
`minKey`s. The candidate-run lower bound covers the whole equal run; this is
regression-tested with a fixture whose shared token spans several pages and
was verified against the full-corpus oracle at 4.27M docs.
