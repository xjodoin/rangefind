# French Wikipedia Scalability Fixture

Rangefind includes a reproducible scalability fixture for French Wikipedia.
The fixture streams the official Wikimedia article dump, converts pages to
JSONL, builds a static Rangefind index, writes a small browser search site, and
records a local request/transfer benchmark with cold-transfer breakdowns for
directory, term, posting-block, typo, doc-value, sorted doc-value, packed
document payload, and doc-page payload fetches.
The generated schema includes multi-value article tags, typed numeric fields,
revision dates, and booleans so the scalability run also validates the typed
filter/sort code paths.

## Data Source

Default dump:

```text
https://dumps.wikimedia.org/frwiki/latest/frwiki-latest-pages-articles.xml.bz2
```

The current `frwiki/latest` index listed this file as a multi-gigabyte
compressed article dump. The fixture reads it as a stream, so bounded samples do
not require downloading the full archive first.

The runner shells out to `curl` and the decompressor matching the dump extension
(`bzip2` for the default `.bz2` input, `gzip` for `.gz` inputs).

## Commands

Build and benchmark a bounded sample:

```bash
npm run build:browser
node scripts/frwiki_fixture.mjs all --limit=5000
```

Rerun only the runtime benchmark against the existing generated index:

```bash
node scripts/frwiki_fixture.mjs runtime-bench --limit=5000 --runs=3
# or keep the all command shape and skip extraction/build:
node scripts/frwiki_fixture.mjs all --limit=5000 --runs=3 --reuse-index
```

Run only the builder benchmark and skip runtime queries:

```bash
node scripts/frwiki_fixture.mjs builder-bench --limit=5000
# or keep the all command shape and stop after the build report:
node scripts/frwiki_fixture.mjs all --limit=5000 --builder-only
```

Run against the full dump:

```bash
node scripts/frwiki_fixture.mjs all --limit=0
```

Serve the generated site:

```bash
node scripts/serve.mjs examples/frwiki/public 5180
```

Useful options:

- `--body-chars=N`: indexes only the first `N` cleaned article characters.
  Default: `6000`. Use `0` for uncapped article text.
- `--dump-url=URL_OR_FILE`: reads another dump URL or local dump file.
- `--force`: regenerates JSONL even when the existing metadata matches the
  requested dump URL, limit, and body cap, and refreshes the reusable extracted
  JSONL cache.
- `--reuse-index`: skips JSONL extraction, site generation, and index building.
  Use this for runtime-only changes when `public/rangefind` already matches the
  requested limit.
- `--builder-only`: builds the index and writes the builder report while
  skipping runtime query rows. Use this when the change affects only indexing.
- `--build-progress-ms=N`: controls live builder progress logging. The frwiki
  fixture defaults to `15000`, so long builds print phase heartbeats with
  elapsed time, RSS, heap, temp bytes, pack bytes, and sidecar bytes.
- `--queries=a|b|c`: overrides benchmark queries.
- `--scale-limits=50000,100000`: controls the `scale` command document counts.
- `--no-exact-checks`: skips the exact top-k comparison for text queries.

The fixture keeps a source-data cache at `data/frwiki.cache.jsonl` with matching
metadata. Once a larger limit has been extracted, later smaller or equal limits
with the same dump URL and `--body-chars` are materialized by slicing that cache
instead of streaming and decompressing the Wikimedia dump again.

## Benchmark Coverage

The `bench` command now exercises a broader set of cold-query lanes instead of
only the default text queries. Each row records a `category`, the full request
shape, cold request/KB breakdowns, compact runtime stats, validation status, and
exact top-k agreement where an exact comparison is meaningful.

Every benchmark run writes into repo-level `benchmarks/frwiki/` instead of
overwriting flat JSON files in the fixture root or mixing generated history into
`examples/`. The layout keeps both latest results and historical raw reports:

```text
benchmarks/frwiki/
  latest/
    runtime/limit-50000.json
    builder/limit-50000.json
    scale/full-dump.json
  history/
    runtime/limit-50000/<timestamp>_<commit>.json
    builder/limit-50000/<timestamp>_<commit>.json
    scale/full-dump/<timestamp>_<commit>.json
  index.json
```

`index.json` is a compact progression ledger. It stores one summary per run,
the current latest report for each `kind:limit` pair, and numeric deltas against
the previous run with the same kind and limit so regressions can be inspected
without opening every raw report.

The builder report uses `rfbuilderbench-v1` and records phase wall time, peak
RSS, heap samples, temp bytes, output bytes, worker summaries, and write
amplification. Full runtime reports embed the latest builder report under the
top-level `builder` key, and `scale --builder-only` records builder-only scale
points without spending time on query execution.

Covered scenarios include:

- default text queries from `--queries`
- query-bundle phrase retrieval and page-2 pagination
- larger top-k windows
- zero-result exact queries
- typo recovery cases
- filtered text queries
- text sorted by typed doc values
- rerank-disabled text retrieval
- numeric/date browse filters
- sorted doc-value browse
- facet-only browse
- facet + boolean + numeric sorted browse

Rows with typo correction or external sort can skip exact top-k comparison when
the exact exhaustive scorer is not the same final behavior being measured, but
they still validate expected result counts, selected filters, sort order, and
runtime-lane flags.

The Lucene quality comparison builds a local Lucene BM25 index over the same
JSONL fixture and compares known-title and typo judgments against Rangefind:

```bash
npm run bench:frwiki:lucene-quality -- --root=/tmp/rangefind-frwiki-500k-current
```

The report is written to `frwiki-lucene-quality.json` in the fixture root. It
contains Rangefind, Lucene OR, Lucene AND, and Lucene exact-title-boosted rows
with Hit@1/Hit@3/Hit@10/MRR@10 metrics.

Latest 500k quality run with the authority sidecar:

```text
Rangefind known titles:       Hit@1 7/7, Hit@10 7/7, MRR@10 1.000
Lucene title-boost known:     Hit@1 7/7, Hit@10 7/7, MRR@10 1.000
Lucene BM25 OR/AND known:     Hit@1 6/7, Hit@10 7/7, MRR@10 0.893

Rangefind typo judgments:     Hit@1 2/4, Hit@10 3/4, MRR@10 0.550
Lucene title-boost typo:      Hit@1 1/4, Hit@10 1/4, MRR@10 0.250
```

The known-title improvement comes from a generic authority sidecar over
configured label fields, not from Wikipedia-specific rules. On the 500k fixture
it indexes the `title` field into 1.49M authority rows, split into 7,018
point-read shards with a 4,096-row target, 15.0 MB of authority packs, and a
359 KB paged directory. The runtime probes a diacritic-preserving surface key
first, so `Paris`, `Médecine`, `Victor Hugo`, and `Québec` rank above
homonymy/partial-title pages. Query bundles still short-circuit exact phrase
queries such as `changement climatique` without touching authority.

Latest 500k cold-query rows after authority sharding:

```text
Paris:                  Paris,        40 requests, 176.1 KB, authority 34.0 KB
Médecine:               Médecine,     26 requests, 171.2 KB, authority 18.3 KB
Victor Hugo:            Victor Hugo,  37 requests, 289.3 KB, authority  6.2 KB
Québec:                 Québec,       28 requests, 212.1 KB, authority 18.0 KB
changement climatique:  unchanged,    28 requests,  76.8 KB, authority skipped
fromage:                unchanged,    19 requests, 239.5 KB, authority miss 9.6 KB
```

An older full 500k build took 3,642 seconds wall time with the removed
base-shard worker reducer and peaked at about 2.39 GB RSS. The main remaining
build bottleneck was the
pre-existing typo-sidecar merge/reduction path; rebuilding only the authority
sidecar from the same 500k JSONL took about 14 seconds.

Newer indexes include `manifest.build` (`rfbuildtelemetry-v1`) with phase
timings, peak RSS, selected-term spool bytes, raw document spool bytes, and
compressed document spool bytes. Use those manifest counters alongside
`/usr/bin/time` when comparing builder changes, because they identify whether a
run moved time between ingestion, posting reduction, query bundles, typo
reduction, document packs, and doc pages.

Latest 50k builder-only worker-reducer sanity run, reusing the cached JSONL:
89.1 seconds total build time, 22.1 seconds in `reduce-postings`, 180 output
files, 188.2 MB index bytes, and 1.91 GB peak RSS. Reducer workers kept
external posting blocks enabled and emitted 11 term packs plus 4 block packs.
The run used bounded reducer worker code-store caches; the next validation
point is the same path at 100k and 500k.

Latest 100k builder-only validation on the same path: 190.9 seconds total build
time, 60.9 seconds in `reduce-postings`, 278 output files, 351.2 MB index
bytes, and 2.17 GB peak RSS. The remaining build-memory target is reducer
completion/worker aggregation before the next 500k run.

## Local 50k Run

Build command, reusing the cached 50k JSONL fixture:

```bash
/usr/bin/time -p node scripts/frwiki_fixture.mjs all --limit=50000 --runs=2
```

Latest cold-transfer bench command against that index:

```bash
node scripts/frwiki_fixture.mjs bench --limit=50000 --runs=2
```

The benchmark uses a fresh runtime per query row and resets the fetch meter
after manifest initialization. Cold query rows therefore exclude the one-request
init cost and do not share runtime caches with earlier rows. Warm columns are
still repeated runs inside the same query row.

Result:

```text
Docs indexed:        50,000
Dump pages read:     65,687
Body cap:            6,000 cleaned characters/article
Logical shards:      15,104
Term packs:          8
Index files:         85
Index bytes:         153.0 MB (145.9 MiB)
Manifest/init:       11.6 ms, 1 request, 104.7 KB
Pack tables:         8 term, 5 posting-block, 8 doc, 5 doc-page, 1 doc-value, 1 sorted doc-value, 1 facet, 13 typo
Doc pointer table:   2.0 MB, 41-byte fixed records
Doc ordinal table:   0.1 MB, 2-byte fixed records
Doc page table:      64.1 KB, 1,563 fixed records, 32 docs/page
Doc page packs:      17.1 MB, rfdocpagecols-v1 binary columns
Sorted doc-values:   29.2 KB directories, 0.7 MB packed value pages
Doc layout:          rflocal-doc-v1, 21,558 primary terms
```

Representative cold queries:

```text
Paris:                    44.0 ms, 14 requests,  90.5 KB, packed docs, 1 block / 128 postings
Révolution française:     44.1 ms, 28 requests, 244.5 KB, packed docs, 31 blocks / 3,744 postings
intelligence artificielle: 29.7 ms, 27 requests, 281.7 KB, packed docs, 3 blocks / 257 postings
Victor Hugo:              21.0 ms, 31 requests, 140.7 KB, packed docs, 7 blocks / 728 postings
football:                  9.4 ms, 21 requests,  71.3 KB, packed docs, 1 block / 128 postings
médecine:                 12.5 ms, 24 requests, 122.0 KB, packed docs, 1 block / 128 postings
changement climatique:    37.1 ms, 30 requests, 167.6 KB, packed docs, 60 blocks / 7,472 postings
fromage:                   6.3 ms,  7 requests,  87.7 KB, packed docs, 3 blocks / 285 postings
Québec:                    7.5 ms, 10 requests,  98.1 KB, packed docs, 8 blocks / 913 postings
Napoléon Bonaparte:       14.2 ms, 23 requests, 157.1 KB, packed docs, 3 blocks / 257 postings
typed dates sorted:       12.1 ms,  4 requests,  18.8 KB, sorted doc-value + doc page lane
dense filter browse:       3.2 ms,  3 requests,  11.6 KB, doc-value chunk early stop + doc page lane
multi facet boolean:       6.1 ms, 10 requests,  21.6 KB, sorted doc-value + facet/doc-value checks
```

All rows reported `valid: true`. Every text query also reported
`exactTopKMatch: true` against the exact retrieval path. Broad single-term
queries now use the same block-max top-k scheduler as multi-term queries:
`Paris` matched exact top 10 after decoding 1 of 49 posting blocks,
`football` matched exact top 10 after decoding 1 of 7 blocks, and
`changement climatique` matched exact top 10 after decoding 60 of 71 blocks.
High-df phrase-style rows now batch posting-block frontier refills:
`Révolution française` used 3 posting-block range requests instead of the
previous 20, and `changement climatique` used 4 instead of 27.

Warm repeated text queries were served from the runtime cache with zero
additional network requests and sub-millisecond to low-single-digit millisecond
latency. Broad sorted metadata views now use `rfdocvaluesortdir-v1` directories
and `rfdocvaluesortpage-v1` value pages: the 50k typed date/body-length sort
fetches one value page, scans 2,048 sorted rows, and accepts the first 10
matches. Broad unsorted range browsing keeps document order and stops after the
first matching doc-value chunk, so the result ids stay dense enough to use one
binary doc page.

High-cardinality facet dictionaries are stored with the same range-directory
and pack strategy as terms and documents. The 50k manifest is 104.7 KB; the
large category dictionary lives in `facets/packs/*.bin` and is fetched only when
that facet is selected or a UI asks for its values.

The current index uses `rfdir-v2` directory pages, a dense `rfdocord-v1`
document ordinal table, a layout-ordered `rfdocptr-v1` document pointer table,
and ZFS-inspired block pointers for every compressed object. Each pointer records
physical length, logical length, codec/kind metadata, and a SHA-256 checksum that
the browser runtime verifies before decompression. That adds directory/manifest
bytes versus an unchecked format, but it makes range-fetched objects
self-verifying and catches stale or corrupt CDN/object-store responses before
decoding.

Dense doc pointers replace the previous document directory for text results.
The ordinal table is keyed by original numeric document id and maps to a
retrieval-local pointer ordinal. Document payloads and pointer records are
written in the same locality order, derived from each document's strongest base
term and impact score. Dense doc pages add a second result-payload lane for
browse/filter/sort rows: the builder writes compressed binary column pages of 32
display payloads in original document-id order, and the runtime uses that lane
only when the requested result ids are dense enough to keep overfetch bounded.
The measured rows show that this keeps dense browse payload requests low without
hurting the text top-k lane, while sparse text results continue to use
retrieval-local packed docs.

## Scale Run

The scale command builds isolated fixtures for each requested limit and writes a
combined report to `benchmarks/frwiki/latest/scale/full-dump.json`, with
timestamped history under `benchmarks/frwiki/history/scale/`:

```bash
node scripts/frwiki_fixture.mjs scale --scale-limits=50000,100000 --runs=2
```

Latest scale result:

```text
Docs        Index      Files  Init             Bytes/doc  Text avg        Browse avg      Exact
50,000      145.9 MiB     85  1 req / 104.7 KB  3061 B    21.5 req / 146 KB  5.7 req / 17 KB  10/10
100,000     267.0 MiB    122  1 req / 169.3 KB  2800 B    22.1 req / 172 KB  5.7 req / 22 KB  10/10
```

Selected rows:

```text
Paris:                 50k 14 req /  90.5 KB, 100k 16 req / 101.3 KB
Révolution française:  50k 28 req / 244.5 KB, 100k 28 req / 278.0 KB
typed dates sorted:    50k  4 req /  18.8 KB, 100k  4 req /  24.6 KB
dense filter browse:   50k  3 req /  11.6 KB, 100k  3 req /  11.6 KB
```

The scale result is now strong for both text and metadata retrieval. Text
request counts and transfer grow slowly while exact top-k agreement remains
10/10 at both sizes. The selected high-df `Révolution française` row stays at
3 posting-block fetch groups and 28 total cold requests at both 50k and 100k.
Metadata browse is flat enough for a static browser index:
sorted top-k grows from 18.8 KB to 24.6 KB when the corpus doubles, and dense
unsorted range browse stays at 3 requests / 11.6 KB because it stops after one
matching doc-value chunk and one doc page.

Pack files, directory pages, directory roots, and the typo sidecar manifest are
content-addressed with 24-hex-character SHA-256 name suffixes. The 50k run
reported 114,972 exact compressed objects and no duplicate compressed objects to
deduplicate, which is expected for real article/search shards. The dedup table
remains useful for synthetic or repeated payloads, but the important CDN win here
is that every heavy object can be served as immutable; only the main manifest
needs revalidation.
