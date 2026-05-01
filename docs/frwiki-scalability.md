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
- `--scale-limits=50000,100000`: controls the `scale` command document counts.
- `--no-exact-checks`: skips the exact top-k comparison for text queries.

## Local 50k Run

Build command, reusing the cached 50k JSONL fixture:

```bash
/usr/bin/time -p node scripts/frwiki_fixture.mjs all --limit=50000 --runs=2 --reduce-workers=auto
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
Manifest/init:       11.2 ms, 1 request, 104.7 KB
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
Paris:                    47.3 ms, 14 requests,  90.5 KB, packed docs, 1 block / 128 postings
Révolution française:     62.9 ms, 45 requests, 245.3 KB, packed docs, 31 blocks / 3,744 postings
intelligence artificielle: 29.2 ms, 27 requests, 281.7 KB, packed docs, 3 blocks / 257 postings
Victor Hugo:              21.8 ms, 31 requests, 140.7 KB, packed docs, 7 blocks / 728 postings
football:                 11.8 ms, 21 requests,  71.3 KB, packed docs, 1 block / 128 postings
médecine:                 13.8 ms, 24 requests, 122.0 KB, packed docs, 1 block / 128 postings
changement climatique:    47.9 ms, 53 requests, 167.6 KB, packed docs, 38 blocks / 4,737 postings
fromage:                   5.2 ms,  7 requests,  87.7 KB, packed docs, 3 blocks / 285 postings
Québec:                    7.3 ms, 10 requests,  98.1 KB, packed docs, 8 blocks / 913 postings
Napoléon Bonaparte:       14.2 ms, 23 requests, 157.1 KB, packed docs, 3 blocks / 257 postings
typed dates sorted:        4.4 ms,  4 requests,  18.8 KB, sorted doc-value + doc page lane
dense filter browse:       2.2 ms,  3 requests,  11.6 KB, doc-value chunk early stop + doc page lane
multi facet boolean:       5.3 ms, 10 requests,  21.6 KB, sorted doc-value + facet/doc-value checks
```

All rows reported `valid: true`. Every text query also reported
`exactTopKMatch: true` against the exact retrieval path. Broad single-term
queries now use the same block-max top-k scheduler as multi-term queries:
`Paris` matched exact top 10 after decoding 1 of 49 posting blocks,
`football` matched exact top 10 after decoding 1 of 7 blocks, and
`changement climatique` matched exact top 10 after decoding 38 of 71 blocks.

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
combined report to `examples/frwiki/frwiki-scale-bench.json`:

```bash
node scripts/frwiki_fixture.mjs scale --scale-limits=50000,100000 --runs=2 --reduce-workers=auto
```

Latest scale result:

```text
Docs        Index      Files  Init             Bytes/doc  Text avg        Browse avg      Exact
50,000      145.9 MiB     85  1 req / 104.7 KB  3061 B    25.5 req / 146 KB  5.7 req / 17 KB  10/10
100,000     267.0 MiB    122  1 req / 169.3 KB  2800 B    31.1 req / 171 KB  5.7 req / 22 KB  10/10
```

Selected rows:

```text
Paris:                 50k 14 req /  90.5 KB, 100k 16 req / 101.3 KB
Révolution française:  50k 45 req / 245.3 KB, 100k 43 req / 278.0 KB
typed dates sorted:    50k  4 req /  18.8 KB, 100k  4 req /  24.6 KB
dense filter browse:   50k  3 req /  11.6 KB, 100k  3 req /  11.6 KB
```

The scale result is now strong for both text and metadata retrieval. Text
request counts and transfer grow slowly while exact top-k agreement remains
10/10 at both sizes. Metadata browse is flat enough for a static browser index:
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
