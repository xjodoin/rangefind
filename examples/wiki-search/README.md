# Rangefind Wikipedia Search Site

This is a standalone static Wikipedia search site built on Rangefind. It can
build a small local sample for iteration or a full Wikimedia `pages-articles`
dump for production-style testing.

From the repository root:

```bash
npm install
npm run build:browser
npm run build:wiki-site -- --limit=50000
npm run serve:wiki-site
```

Open `http://localhost:5182/`.

To build the full English Wikipedia dump with the bounded body cap, use:

```bash
npm run build:wiki-site:full
```

The full-dump scripts include autocomplete. Suggestion records stream through
the bounded authority reducer, so millions of unique titles no longer require
a corpus-sized heap map or a separate suggestion index.

For the full French Wikipedia dump, use:

```bash
npm run build:wiki-site:fr:full
```

The generated Rangefind config uses the `static-large` profile with
`targetPostingsPerDoc: 12`, `bodyIndexChars: 6000`, impact-ordered posting
blocks, gzip level 3 posting compression, and title/category fields as
always-indexed fields. That keeps every
article in the document store while bounding body postings for browser-served
static search. The extracted body defaults to the same 6,000-character cap,
while `bodyLength` and length-derived tags retain the full cleaned article
length. The full-corpus profile keeps document payloads in doc-id order and
packs them through bounded sequential read windows, avoiding millions of
random reads (or a multi-gigabyte preload) on external volumes.
At query time, indexes with at least one million documents automatically cap
the approximate top-k lane at 128 decoded blocks. Exhaustive callers can pass
`topKBlockBudget: 0` or request exact search explicitly.

Useful options:

- `--dump-url=URL_OR_FILE`: Wikimedia XML dump URL or local `.xml`, `.xml.gz`,
  or `.xml.bz2` file. Defaults to the latest English Wikipedia articles dump.
- `--root=PATH`: write data, resumable scratch files, the index, and the demo
  site under another workspace (for example a large external drive).
- `--wiki=enwiki`: wiki id used for generated article URLs.
- `--limit=N`: number of articles to index. Use `0` for the full dump.
- `--body-chars=N`: extraction cap for article body text before the JSONL is
  written. The Rangefind `bodyIndexChars` config then applies a separate
  indexing-only cap, and result snippets remain controlled by `display`.
- `--no-suggest`: omit autocomplete records entirely. It is no longer needed
  for memory safety; use it only when the product does not expose completion.
- `--multistream[=N]`: discover Wikimedia's ordered pages-articles shards and
  extract `N` concurrently (default `3` when the flag is present). The full
  English npm script enables this because it avoids one slow multi-hour HTTP
  stream while concatenating extracted rows back in deterministic page order.
  Completed shards are cached under `.wikipedia-multistream-parts` until the
  final JSONL is assembled, so a retry does not redownload them. A nonzero
  `--limit` stays on the ordered single-stream path so the cap remains exact.
- `--jsonl=PATH`: build directly from an existing compatible JSONL file.
- `--force`: rebuild extracted JSONL even when metadata already matches.
- `--build-progress-ms=N`: Rangefind builder progress log interval.

The generated index is written to `examples/wiki-search/public/rangefind/`.
The local server in `scripts/serve.mjs` supports HTTP `Range` requests, which
the browser runtime needs for `.bin` index files.

See [`docs/enwiki-scalability.md`](../../docs/enwiki-scalability.md) for the
full 7.19-million-document build and cold-query measurements.
