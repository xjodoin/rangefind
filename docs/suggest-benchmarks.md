# Search-As-You-Type Benchmarks

`scripts/suggest_bench.mjs` replays realistic keystroke sequences against a
built index over localhost HTTP, measuring requests, transfer, and latency per
keystroke. Top-k exactness is also verified against an exhaustive source-corpus
oracle in the test suite.

```bash
npm run bench:suggest
node scripts/suggest_bench.mjs --root=examples/osm-geo
```

## Unified authority result

Recorded 2026-07-09 on the 175,276-document Luxembourg place corpus. The
configuration indexes names and aliases, uses population when present, and
falls back to duplicate count.

| Metric | Old dedicated suggest index | Unified `rfauth-v2` |
| --- | ---: | ---: |
| Autocomplete keys | 82,305 | 83,046 |
| Autocomplete storage | 1,241,445 bytes | 1,235,565 bytes |
| Root | 27,491 bytes | 6,749 bytes |
| Dedicated suggestion packs | 1 | 0 |
| Authority shards containing autocomplete | — | 1,013 |
| Build total, stripped benchmark config | 4.96 s | 5.75 s |
| Scan phase | 1.40 s | 1.29 s |
| Autocomplete reduction | 0.29 s | 1.43 s |
| Peak scan heap | 44 MiB | 21 MiB |

The small-corpus reducer is intentionally slower because it performs a bounded
external sort instead of retaining every surface in a process-wide map. That
cost buys a fixed 5,000-record scan buffer and makes millions of unique titles
buildable. Storage is slightly smaller even though autocomplete now shares the
authority pack family and retains exact weights, counts, and display strings.

Cold local reads for six representative prefixes averaged 53.4 KB with the
unified index versus 62.2 KB with the old sidecar (14% less). Examples:

| Prefix | Old bytes | Unified bytes | Top result |
| --- | ---: | ---: | --- |
| `m` | 48,250 | 28,665 | Mamer |
| `mont` | 51,296 | 49,370 | Rue de la Montagne |
| `saint` | 53,788 | 52,078 | Val Sainte-Croix |
| `rue` | 114,279 | 85,107 | Rue Principale |
| `hotel` | 50,883 | 50,665 | insect hotel |
| `cafe` | 54,758 | 54,220 | cafe |

Local decode time remained in the 2–5 ms range after warm filesystem caching;
HTTP deployments benefit more directly from the reduced byte totals.

## Why the unified path stays exact

- Suggestion surfaces stream into the authority run files; there is no global
  surface map in the scanner or a second in-memory map in a sidecar writer.
- Full folded keys and up to four token suffixes preserve prefix and mid-label
  completion, including diacritic-insensitive input.
- Each autocomplete authority shard publishes its maximum composite rank. The runtime
  visits matching shards best-first and stops only when no unvisited shard can
  change top-k.
- Unweighted indexes prefer direct surface prefixes before token-suffix
  matches; indexes with `weightPath` retain the configured weight as the
  primary signal. Both modes use the same exact shard-max proof.
- Single-character prefixes fetch a compact immutable hot list from
  `authority/hot/`; other queries do not pay for hot-list contents.
- `rfauth-v2` autocomplete entries store only normalized/display text, maximum
  weight, and count—no unnecessary document row list or payload hydration.

The former `suggest/` directory, page/branch codecs, pack table, manifest
branch, scan aggregation map, and runtime page lane have been removed.
