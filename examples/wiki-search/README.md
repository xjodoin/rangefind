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

For the full French Wikipedia dump, use:

```bash
npm run build:wiki-site:fr:full
```

The generated Rangefind config uses the `static-large` profile with
`targetPostingsPerDoc: 12`, `bodyIndexChars: 6000`, and title/category fields
as always-indexed fields. That keeps every article in the document store while
bounding body postings for browser-served static search.

Useful options:

- `--dump-url=URL_OR_FILE`: Wikimedia XML dump URL or local `.xml`, `.xml.gz`,
  or `.xml.bz2` file. Defaults to the latest English Wikipedia articles dump.
- `--wiki=enwiki`: wiki id used for generated article URLs.
- `--limit=N`: number of articles to index. Use `0` for the full dump.
- `--body-chars=N`: extraction cap for article body text before the JSONL is
  written. The Rangefind `bodyIndexChars` config then applies a separate
  indexing-only cap, and result snippets remain controlled by `display`.
- `--jsonl=PATH`: build directly from an existing compatible JSONL file.
- `--force`: rebuild extracted JSONL even when metadata already matches.
- `--build-progress-ms=N`: Rangefind builder progress log interval.

The generated index is written to `examples/wiki-search/public/rangefind/`.
The local server in `scripts/serve.mjs` supports HTTP `Range` requests, which
the browser runtime needs for `.bin` index files.
