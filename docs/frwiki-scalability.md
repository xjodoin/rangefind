# French Wikipedia Scalability Fixture

Rangefind includes a reproducible scalability fixture for French Wikipedia.
The fixture streams the official Wikimedia article dump, converts pages to
JSONL, builds a static Rangefind index, writes a small browser search site, and
records a local request/transfer benchmark with cold-transfer breakdowns for
directory, term, posting-block, typo, doc-value, and document payload fetches.
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
  requested dump URL, limit, and body cap.
- `--queries=a|b|c`: overrides benchmark queries.
- `--reduce-workers=auto`: enables worker-based term reduction.
- `--no-exact-checks`: skips the exact top-k comparison for text queries.

## Local 50k Run

Command:

```bash
/usr/bin/time -p node scripts/frwiki_fixture.mjs all --limit=50000 --runs=3 --reduce-workers=auto
```

Result:

```text
Docs indexed:        50,000
Dump pages read:     65,687
Body cap:            6,000 cleaned characters/article
Build + bench time:  196.88 s real including dump streaming
Logical shards:      15,104
Term packs:          7
Index files:         54
Index bytes:         124.0 MB
Manifest/init:       10.9 ms, 1 request, 53.1 KB
```

Representative cold queries:

```text
Paris:                    55.6 ms, 20 requests, 332.1 KB, 1 block / 128 postings
Révolution française:     64.1 ms, 39 requests, 238.3 KB, 31 blocks / 3,744 postings
intelligence artificielle: 19.2 ms, 13 requests, 137.6 KB, 3 blocks / 257 postings
football:                  9.9 ms, 11 requests,  12.5 KB, 1 block / 128 postings
Québec:                    9.7 ms, 12 requests,  32.9 KB, 8 blocks / 913 postings
typed dates sorted:       37.9 ms,  3 requests, 228.7 KB, doc-value top-k selector
multi facet boolean:      39.5 ms,  7 requests, 148.9 KB, 0.13 KB facet dictionary range
```

All rows reported `valid: true`. Every text query also reported
`exactTopKMatch: true` against the exact retrieval path. Broad single-term
queries now use the same block-max top-k scheduler as multi-term queries:
`Paris` matched exact top 10 after decoding 1 of 49 posting blocks, and
`football` matched exact top 10 after decoding 1 of 7 blocks.

Warm repeated text queries were served from the runtime cache with zero
additional network requests and sub-millisecond to low-single-digit millisecond
latency. Broad sorted metadata views still scan doc-value chunks, but the
runtime keeps only the requested top-k page instead of sorting all matching
documents; the 50k typed date sort dropped from about 100 ms warm to about
21 ms warm on the same index.

High-cardinality facet dictionaries are stored with the same range-directory
and pack strategy as terms and documents. The 50k manifest is 53.1 KB; the
large category dictionary lives in `facets/packs/*.bin` and is fetched only when
that facet is selected or a UI asks for its values.
