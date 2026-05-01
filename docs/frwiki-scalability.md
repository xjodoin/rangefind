# French Wikipedia Scalability Fixture

Rangefind includes a reproducible scalability fixture for French Wikipedia.
The fixture streams the official Wikimedia article dump, converts pages to
JSONL, builds a static Rangefind index, writes a small browser search site, and
records a local request/transfer benchmark with cold-transfer breakdowns for
directory, term, typo, code, and document payload fetches.

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

## Local 5k Run

Command:

```bash
/usr/bin/time -p node scripts/frwiki_fixture.mjs all --limit=5000 --runs=3
```

Result:

```text
Docs indexed:        5,000
Dump pages read:     5,913
Body cap:            6,000 cleaned characters/article
Build + bench time:  12.62 s real with prepared JSONL reused
Logical shards:      7,565
Term packs:          2
Index files:         16
Index bytes:         18.4 MB
Init:                11.2 ms, 1 request, 238.3 KB
```

Representative cold queries:

```text
Paris:                    25.0 ms, 14 requests, 122.6 KB, top Paris (homonymie)
Révolution française:      9.3 ms, 10 requests,  32.3 KB, top Révolution française
intelligence artificielle: 6.1 ms, 11 requests,  44.7 KB, top Intelligence artificielle
football:                  4.5 ms, 11 requests,   5.1 KB, top Coupe du monde de football
Québec:                    3.7 ms, 10 requests,   8.8 KB, top Système éducatif au Québec
```

The cold-transfer breakdown shows term pack fetches in the 0.5-54.3 KB range
and result document fetches in the 3.5-11.2 KB range. The first query also pays
for the term and document range-directory pages, which were 86.4 KB in this run.

Warm repeated queries in this run were served from the runtime cache with zero
additional network requests.
