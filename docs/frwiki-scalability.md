# French Wikipedia Scalability Fixture

Rangefind includes a reproducible scalability fixture for French Wikipedia.
The fixture streams the official Wikimedia article dump, converts pages to
JSONL, builds a static Rangefind index, writes a small browser search site, and
records a local request/transfer benchmark.

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
Build + bench time:  25.18 s real
Logical shards:      7,565
Term packs:          2
Index files:         53
Index bytes:         42.1 MB
Init:                10.3 ms, 1 request, 338.8 KB
```

Representative cold queries:

```text
Paris:                    26.8 ms, 10 requests, 4.2 MB, top Paris (homonymie)
Révolution française:     15.6 ms, 10 requests, 5.0 MB, top Révolution française
intelligence artificielle:11.8 ms,  8 requests, 3.7 MB, top Intelligence artificielle
football:                  7.3 ms,  4 requests, 2.0 MB, top Coupe du monde de football
Québec:                    0.6 ms,  1 request, 4.2 KB, top Système éducatif au Québec
```

Warm repeated queries in this run were served from the runtime cache with zero
additional network requests.
