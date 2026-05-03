# French Wikipedia Scalability Fixture

This fixture builds a static Rangefind site from French Wikipedia article data.
It streams the official Wikimedia dump, converts pages to JSONL, builds the
index, and runs a local search benchmark with request and transfer counts. The
benchmark JSON also breaks cold-query transfer down by directory, term, typo,
posting-block, doc-value, sorted doc-value, and document payload fetches. The
generated schema includes typed article metadata, revision dates, booleans, and
bounded multi-value tags so the benchmark validates filter and sort behavior as
well as text retrieval.

Quick bounded run:

```bash
npm run build:browser
node scripts/frwiki_fixture.mjs all --limit=50000 --runs=3
node scripts/serve.mjs examples/frwiki/public 5180
```

Runtime-only rerun against an existing index:

```bash
node scripts/frwiki_fixture.mjs runtime-bench --limit=50000 --runs=3
# equivalent when using the all entrypoint:
node scripts/frwiki_fixture.mjs all --limit=50000 --runs=3 --reuse-index
```

Full dump run:

```bash
node scripts/frwiki_fixture.mjs all --limit=0
```

Requirements: `curl` plus the matching decompressor for the dump extension
(`bzip2` for the default `.bz2` file, `gzip` for `.gz` inputs).

Useful options:

- `--dump-url=URL_OR_FILE`: defaults to the official `frwiki/latest`
  `pages-articles.xml.bz2` dump.
- `--limit=N`: stops after `N` indexed articles. Use `0` for the full dump.
- `--body-chars=N`: indexes the first `N` cleaned characters per article.
  The default is `6000`; use `0` for uncapped article text.
- `--force`: rebuilds the JSONL even when the existing generated metadata
  matches the requested dump URL, limit, and body cap. It also refreshes the
  reusable extracted-data cache.
- `--reuse-index`: skips JSONL extraction, site generation, and index building;
  use it when only runtime/benchmark code changed and `public/rangefind`
  already matches the requested limit.
- `--queries=a|b|c`: overrides benchmark queries.
- `--scale-limits=50000,100000`: builds isolated scale points with the `scale`
  command.
- `--no-exact-checks`: skips exact top-k validation for text queries.

Generated data is cached in `examples/frwiki/data/frwiki.cache.jsonl` after the
first extraction. Later runs with the same dump URL and body cap reuse that cache
for equal or smaller `--limit` values by slicing the first `N` rows instead of
streaming the dump again. Generated data, public assets, config, and benchmark
JSON are intentionally ignored by git.
