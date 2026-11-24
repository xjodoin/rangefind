# Rust engine (native + WebAssembly)

`rust/rangefind` is a Rust port of the query runtime (`createSearch()` in
`src/runtime.js`). It reads the same static, range-packed index files the
JavaScript builder writes, returns the same responses, and runs unchanged on
every platform: natively (macOS, Linux, Windows, iOS, Android, any host with a
C ABI) and as a WebAssembly module in browsers, workers, Node, Deno, Bun and
edge runtimes.

Everything `createSearch()` accepts works, including generational
(`build --update`) and sharded indexes, and so does the OSM query layer
(`rangefind/osm`: geocoding, reverse geocoding, "X near Y", constraints such
as open now, suggestions, route corridors and the Google Maps-style adapter).
The builder and OSM extraction stay in JavaScript; only querying was ported.

## Why

- **One engine everywhere.** Apps that cannot embed a JavaScript engine (native
  iOS/Android, desktop, servers in other languages, Flutter via Dart FFI) link
  the library directly instead of going through a WebView or a local range
  server.
- **Faster native queries.** See [Performance](#performance).
- **No platform dependencies.** gzip inflation and SHA-256 checksum
  verification are built in, so hosts without `DecompressionStream` or
  `crypto.subtle` work as is.

## How it fits together

```
          ┌──────────────── host ────────────────┐
 request  │  rf_open / rf_call   rf_poll → JSON  │  fetch with the platform's
 ───────► │ ┌──────────────────────────────────┐ │  own HTTP / file APIs
          │ │ Rust engine (single-threaded,    │ │ ───────────────────────►
          │ │ async, no I/O of its own)        │ │  bytes back via
          │ └──────────────────────────────────┘ │  rf_complete
          └──────────────────────────────────────┘
```

The engine never performs I/O. It asks for whole objects or byte ranges and
the host answers. That single protocol (`src/host.rs`, exposed as a C ABI in
`src/ffi.rs` and `include/rangefind.h`) is what the wasm glue, native apps and
tests all use, so every platform runs the same code path. Native Rust callers
can instead use `Engine` directly with a `Transport` (`FileTransport` for
local indexes, `MemoryTransport` for bundled ones).

## Using it

### Browsers, workers, Node, Deno, Bun (`rangefind/wasm`)

```js
import { createWasmSearch } from "rangefind/wasm";

const engine = await createWasmSearch({ baseUrl: "/rangefind/" });
const response = await engine.search({ q: "static search", size: 10 });
```

Same parameters and response shapes as `createSearch()`: `search`, `count`,
`suggest`, `vectorSearch`, `authorityLookup`, `loadFacetValues`,
`loadDocValues`, `hydrateRows`, plus `stats()` (requests and bytes per object
bucket), and `close()`, which releases the engine's wasm memory and file
handles once in-flight calls settle (engines that are garbage-collected are
released too). Options: `fetch` (a fetch-compatible function), `transport`
(`(url, ranges) => Uint8Array[] | null`) for fully custom I/O, and `wasm` to
supply the module yourself. `file://` base URLs work in Node with a bounded
pool of file handles per engine. The module bundles for browsers (esbuild,
webpack, Vite): Node built-ins are only imported for `file:` URLs.

Build it with `npm run build:wasm`, which writes
`dist/rangefind_wasm.{mjs,wasm}`.

### Rust

```rust
use std::rc::Rc;
use rangefind::engine::{Engine, EngineOptions};
use rangefind::transport::FileTransport;

let engine = Engine::open_blocking(Rc::new(FileTransport::new()), "/srv/site/rangefind/", EngineOptions::default())?;
let response = engine.call_blocking("search", serde_json::json!({ "q": "museum", "size": 10 }))?;
```

`Engine::open` and `Engine::call` are `async` for transports that complete
later. Dropping an in-flight call is safe: partially loaded objects are
evicted, not cached, so the next query refetches them. `FileTransport` keeps
a bounded set of open files (`with_max_open_files`, default 64) and clamps
reads to the file size. `prefetchShards` warm-up only runs when something
drives the engine's runtime (a host session or `block_on`). The `rangefind-rs` binary wraps this:

```bash
cargo run --release --bin rangefind-rs -- search path/to/rangefind '{"q":"museum"}'
```

### Swift, Kotlin, Dart, C

Link the `staticlib`/`cdylib` and drive `rangefind.h`:

1. `rf_session_new()`, then `rf_open(session, url, options_json)`.
2. Loop on `rf_poll(session)`. It returns JSON with `requests` (URL plus byte
   ranges to fetch) and `done` (finished operations).
3. Answer each request with `rf_complete` (bytes; the per-range lengths must
   sum exactly to the data length), `rf_complete_missing` (404) or
   `rf_complete_error`. Completion order does not matter.
4. `rf_call(session, engine, "search", params_json)` runs a query the same
   way.

### Options specific to this engine

| Option | Default | Purpose |
| --- | --- | --- |
| `maxInflateBytes` | 512 MiB | Upper bound for one decompressed object, so a corrupt or hostile index cannot exhaust memory (objects with a known logical length are capped near that length). |
| `cacheBudgetBytes` | 256 MiB | Memory for decoded objects kept cached between calls (posting segments and blocks, doc pages, doc values, geo pages, authority shards, the OSM layer's per-query caches, ...). One budget per opened engine, shared by a federation's shards. Least recently used objects are evicted when a call ends; `0` keeps everything (unbounded). See [Memory](#memory). |

### Memory

The engine caches the objects it decodes, as the JavaScript runtime does,
but bounds them: each cache entry is measured when it settles (vector and
buffer lengths), and when a call ends and the total exceeds `cacheBudgetBytes`, the least
recently used entries are evicted down to 80% of the budget. Eviction never
touches a load still in flight or anything a running call has used (calls
read back what they loaded), and only drops the cache's reference — the next
query that needs the object fetches it again. A single call can still use
more than the budget while it runs; the budget bounds what stays resident
between calls. `engine.stats()` reports it under `cache` (`usedBytes`,
`entries`, bytes per cache, eviction counters).

This matters for long-lived engines: wasm memory never shrinks and is capped
at 4 GiB. Replaying 1,307 Luxembourg OSM queries on one wasm engine grew
linear memory to 1,886 MB with unbounded caches (estimated 1.6 GB of cached
objects, 60% of it geo leaf pages); with the default budget it levels off at
465 MB after about 400 queries and stays there, and the replay ran in 18.7 s
instead of 42.8 s. Directory roots, lazy manifests, the query plan cache and
federation shard handles are small and stay unbounded.

## Compatibility with the JavaScript runtime

Correctness is defined as matching the JavaScript runtime, and it is tested
at three levels:

| Level | What is compared | Size |
| --- | --- | --- |
| Module parity (`tests/*_parity.rs`, `authority` lib tests) | Every decoder, analyzer, stemmer, highlighter, geo, vector, typo and address function, against outputs of the real JS encoders/functions | Hundreds of thousands of generated cases, including every codec variant and corrupt-input errors |
| Engine parity (`tests/engine_parity.rs`) | Complete query responses — every top-level key except internal `stats` must be identical in both directions (results, scores, totals, facets, suggestions, corrections, errors, lookups) — plus the planner lane and exactness flags, over 16 real indexes built with different configs (single, generational and sharded; two-level geo trees; address authority; legacy title suggest). Each index replays its cases in order on one engine, so lazy-manifest state carries between calls | 8,323 recorded JS queries |
| Date parsing (`tests/date_parse_parity.rs`) | The port of V8's `Date.parse` (ES date-time strings and the legacy fallback: month names, `/` and `.` dates, RFC 2822, AM/PM, zone names and offsets) used by date range filters | 783 strings |
| Platform parity (`rust/tools/wasm/*`) | The same 8,323 cases through the wasm module in Node and in headless Chromium over HTTP range requests | 8,323 × 2 |
| Host protocol (`tests/host_session.rs`) | The poll/complete loop and raw C ABI, with out-of-order completions, transport errors and recovery | |

Known, documented differences, all outside normal indexes:

- **Last-bit floating point.** `Math.sin/cos/atan2/...` in V8 and the
  platform libm can differ by 1–2 ulp. Ranking does not depend on these (geo
  distances are rounded before use). `Math.cos` and `Math.log1p`, where
  scores need bit-exact values, are ported from V8's fdlibm.
- **Unicode version.** The Rust crates use Unicode 18. Exclusion tables
  pin `\p{L}`/`\p{N}` to Unicode 16, matching Node 24.10. Node releases
  with a newer ICU (Unicode 17+) treat a few thousand newly assigned
  characters as letters, so only those can tokenize differently. CI pins
  Node 24.10.0 for the fixtures for that reason.
- **Lone surrogates** in strings become U+FFFD (Rust strings are valid
  UTF-8). Index keys built from them were already unreachable in JS.
- **Corrupt input** may fail with a different message where JS would have
  read `undefined`.
- **Local time in date filters.** `Date.parse` reads zone-less date-times
  (`"2010-06-15T10:00"`) and legacy date strings (`"Jan 1 2010"`) in the
  host's local time zone; the Rust engine reads them as UTC. Date-only ISO
  strings and strings with a zone agree everywhere. The JS fixtures are
  recorded with `TZ=UTC`.

Two pre-existing JS bugs were found while porting and reproduced faithfully
rather than fixed:

- Directory and text-routing keys whose front-coding prefix splits a
  surrogate pair decode to U+FFFD, so they can never be looked up (builder
  fix needed).
- `s|` authority keys without the `\0` separator desynchronize the shard
  parser (builder fix needed).

Bugs found by comparing the fast lanes against `exact: true` were fixed in
both engines together:

- Query bundles answered queries from a bundle over a 2- or 3-term window of
  the query (a different match set), and a complete bundle claimed a proven
  filtered top k after the first filter batch. Only a bundle over the full
  base-term list answers now, and a complete bundle proves the filtered top
  k once k rows passed or every candidate row was filtered.
- Loading the full manifest (`exact: true`) replaced already loaded facet
  dictionaries with summaries, so later facet filters were dropped.
- Sorted doc-value trees and sort replicas stored missing values as `0` and
  double-coded booleans (every boolean sorted the same), and the browse lane
  stopped inside ties that straddle pages. Builders now omit missing values
  from trees (`missing_values: "omitted"`) and rank them last in replicas
  (`missing_values: "last"`); the runtime ignores older trees and replicas,
  falls back to the exact lane when a tree runs out before the missing
  values are accounted for, and narrows boolean sort pages with a boolean
  block filter instead of a numeric range. The exact sort lane now orders
  boolean fields in Rust too.
- A non-numeric `page` or `size` produced NaN offsets that crashed the proof
  lanes, fractional pages crashed the sorted and nearest-first lanes, and
  `hydrateRows` read arbitrary documents for ids that are not document
  indexes; these now read as page 1 / size 10, page by fractional offsets,
  and throw, respectively.

## OSM query layer

The query-time half of `src/integrations/osm/` is ported in `src/osm`
(`query.js` and the category lexicon, constraints, opening hours, schema and
Google Maps adapter modules). Hosts reach it through the same engine:

```js
const engine = await createWasmSearch({ baseUrl: "/osm/rangefind/" });
await engine.osm.search({ q: "pharmacie ouverte maintenant", near: { lat: 49.61, lon: 6.13 } });
await engine.osm.suggest({ q: "rue de la g", size: 5 });
await engine.osm.reverseGeocode({ lat: 49.6116, lon: 6.1319 });
await engine.osm.maps.textSearch({ textQuery: "restaurant", openNow: true });
```

`osm.*` methods also go through `rf_call` (`"osm.search"`, `"osm.suggest"`,
`"osm.reverseGeocode"`, `"osm.searchAlongRoute"`, `"osm.resolveSuggestion"`,
`"osm.hydrateSuggestions"`, `"osm.collapseStreetSuggestions"`, the
`"osm.parse*"` helpers and `"osm.maps.*"`), and Rust callers use
`rangefind::osm::query` directly. The layer's per-engine caches (locality,
authority lookups) live on the engine, so sharded indexes keep them warm, and
share its `cacheBudgetBytes`.

On `wasm32-unknown-unknown` there is no clock or tz database, so the module
imports two functions from its host — `rangefind.host_now_ms` and
`rangefind.host_zone_offset_minutes` — which the glue implements with
`Date.now()` and `Intl`, keeping "open now" identical to the JS layer.
Embedders loading the module without the glue must provide both. Native
builds read the system tz database (`$TZDIR` or `/usr/share/zoneinfo`).
Windows has none, and Ubuntu 24.04 moved legacy aliases such as
`US/Eastern` and `EST` to the `tzdata-legacy` package. There, set `$TZDIR`
to an IANA zoneinfo directory (Python's `tzdata` package ships one) or
plug in `OpeningHoursOptions::zone_resolver`; otherwise named zones report
`invalid-time-zone`.

Parity: the OSM suite (`node rust/tools/parity/osm_fixtures.mjs`, then
`cargo test --release --test osm_parity` and `node rust/tools/wasm/osm_parity.mjs`)
records 3,223 calls on a Luxembourg index built from the local extract (single
and 3-shard), the synthetic indexes the JS tests use, and replays of the JS
unit tests — 10,871 comparisons, natively and in wasm. CI runs the parts that
need no OSM extract. The supporting modules have their own fixtures
(`rust/tools/fixtures/osm_modules.mjs`, about 90,000 cases).

| Luxembourg (cases) | Engine | Cold p50 | Cold p95 | Warm p50 | Warm p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| single index (1,591) | JS | 12.29 ms | 69.9 ms | 0.371 ms | 4.52 ms |
| | Rust native | **9.52 ms** | **55.7 ms** | 0.426 ms | **4.40 ms** |
| 3 shards (616) | JS | 11.95 ms | 60.5 ms | 0.282 ms | 4.28 ms |
| | Rust native | **9.50 ms** | **54.4 ms** | 0.307 ms | 5.25 ms |

Cold OSM queries are I/O-bound (about 578 KB per query, identical in both
engines), which caps the gain at 1.2–1.3×. Warm OSM calls are dominated by
JSON value handling in the layer; they are within 10–15% of JS.

Known differences: named time zones use the system tz database natively
(Node bundles its own ICU data; they can disagree, e.g. Africa/Casablanca),
and free-form date strings such as "Aug 3 2026" are not accepted for `at`.

Five behaviour issues found while porting were fixed in both engines
together (JS regression tests in `test/osm_query.test.js`):

- A failed root-authority read is no longer cached as "no authority": the
  next query retries instead of losing the exact-name shortcut for that
  surface for the engine's lifetime.
- Suggestion `matchedRanges` match word prefixes only. `"el"` on
  "Rue de la Gare" no longer marks "e l" across two words, and offsets are
  UTF-16 throughout (an astral character before the match used to drop the
  range's `end`).
- Tie-breaks between equally weighted street suggestions, merged trace spans
  and fuzzy category keys use UTF-16 code-unit order instead of
  `localeCompare`, so ordering no longer depends on the runtime's ICU data.
- When the root authority names a street's shard but the city check fails,
  the street resolver reuses that search instead of issuing the identical
  request again. Outputs are unchanged on every recorded case; 23 of 7,269
  engine calls fewer.
- Locality probes were measured, not changed: their bytes are the text lane's
  term-dictionary, directory and posting reads, not hydration (a page of 32,
  8 or 1 results costs the same ~169 KB; rows without documents save 1.3%),
  so no result-identical saving exists in the OSM layer.

## Running the tests

```bash
npm run test:rust:fixtures   # JS encoders → module fixtures; parity indexes + recorded responses
npm run test:rust            # unit, module parity, engine parity, host protocol
npm run test:wasm            # build wasm, replay every case in Node
npm run test:wasm:browser    # replay every case in headless Chromium
```

`RANGEFIND_PARITY_ONLY=geo,blocks` narrows engine parity,
`RANGEFIND_PARITY_DUMP=file` writes every failure,
`RANGEFIND_PARITY_STATS=1` reports internal stats keys that differ, and
`RANGEFIND_CACHE_BUDGET=bytes` sets `cacheBudgetBytes`. Engine parity also
replays every index under 1-byte and partial cache budgets, so answers
cannot depend on what happens to be cached. CI
(`.github/workflows/rust.yml`) runs the native suite on Linux, macOS and
Windows, and the wasm suite in Node and Chromium.

## Performance

Measured by replaying each index's parity cases (Apple M-series laptop,
Node 24). *Cold* opens a fresh engine per query, so it includes the manifest,
directory and posting fetches; *warm* repeats the query on the same engine
five times. Reproduce with `rangefind-rs bench` and
`rust/tools/parity/bench_js.mjs [--wasm]`.

| Index (cases) | Engine | Cold p50 | Cold p95 | Cold total | Warm p50 | Warm p95 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| blocks (505) | JS runtime | 2.60 ms | 7.56 ms | 1,475 ms | 0.128 ms | 0.87 ms |
| | Rust native | **0.83 ms** | **2.26 ms** | **454 ms** | **0.070 ms** | **0.49 ms** |
| | Rust wasm (Node) | 1.69 ms | 4.35 ms | 935 ms | 0.168 ms | 0.85 ms |
| codecs (505) | JS runtime | 1.65 ms | 3.98 ms | 882 ms | 0.080 ms | 0.40 ms |
| | Rust native | **0.50 ms** | **1.18 ms** | **278 ms** | **0.055 ms** | **0.26 ms** |
| | Rust wasm (Node) | 1.10 ms | 2.24 ms | 586 ms | 0.133 ms | 0.44 ms |
| geo2 (642) | JS runtime | 2.70 ms | 8.75 ms | 2,327 ms | 0.142 ms | 1.49 ms |
| | Rust native | **0.87 ms** | **2.91 ms** | **751 ms** | **0.113 ms** | **0.89 ms** |
| | Rust wasm (Node) | 1.84 ms | 5.44 ms | 1,538 ms | 0.231 ms | 1.51 ms |
| sharded (826) | JS runtime | 4.06 ms | 9.54 ms | 3,677 ms | 0.204 ms | 1.08 ms |
| | Rust native | **2.27 ms** | **5.74 ms** | **2,063 ms** | **0.198 ms** | **0.92 ms** |
| | Rust wasm (Node) | 3.73 ms | 8.97 ms | 3,337 ms | 0.390 ms | 1.63 ms |

- Native Rust is 1.8–3.3× faster on cold queries, which is where CPU time is
  spent decoding and planning.
- Wasm is 1.1–1.6× faster than JS cold (each cold wasm engine opens its own
  file handles, as the JS runtime does after a cache reset). It is slightly slower warm, because each
  call serializes parameters and the response as JSON across the boundary,
  which dominates sub-millisecond cached queries.
- **Size is the tradeoff in browsers.** The wasm module is 1.4 MB gzipped (engine plus OSM layer)
  (`npm run build:wasm -- --small` is about 24% smaller and 20% slower), against 128 KB
  for `dist/runtime.browser.js`. For a typical website the JS runtime remains
  the lighter choice. Use wasm when cold-query CPU matters (large indexes,
  low-end devices, workers) or the host lacks `DecompressionStream` or
  `crypto.subtle`.
- Two engine-level costs found while benchmarking are fixed. The executor
  polls only woken tasks, so host-completed fetches cost O(1) each. The wasm
  heap starts at 16 MiB instead of growing 64 KiB at a time; `memory.grow`
  had dominated cold wasm queries.

## Layout

| Path | Contents |
| --- | --- |
| `src/rt.rs`, `src/budget.rs` | Minimal single-threaded async runtime: shared promises, promise caches, join; the byte-budgeted LRU behind the caches |
| `src/transport.rs`, `src/host.rs`, `src/ffi.rs` | Transports, the host protocol, the C ABI |
| `src/analysis`, `src/terms.rs`, `src/highlight.rs` | `multi-v1` analyzer, stemmers, stopwords, term expansion, highlighting |
| `src/codec` | Every binary format: postings, directories, doc pointers and pages, doc values, sort trees, bitmaps, bundles, segments, text routing |
| `src/geo`, `src/authority`, `src/vector.rs`, `src/typo.rs` | Geo tree/cells/routes, authority and address logic, vector index, typo math |
| `src/engine` | The query engine: loaders, filters, hydration, text lanes, browse and sort, facets, geo, vectors, typo, authority and suggest, bundles, segments, sort replicas |
| `src/engine/federation` | Generational (`build --update`) and sharded indexes: shard routing (geo, groups, text routing), suggest routing, federated merges |
| `src/osm` | OSM query layer: `query.js`, category lexicon, constraints, opening hours (+ tz), schema, Google Maps adapter |
| `js/rangefind_wasm.mjs` | wasm glue (`createWasmSearch`, host clock and time zones) |
| `include/rangefind.h` | C header |
| `rust/tools` | Fixture generators, parity runners, wasm build, benchmarks |
