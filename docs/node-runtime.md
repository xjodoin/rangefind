# Node Runtime

`rangefind/node` runs the same search runtime the browser uses, but from Node —
for MCP servers, CLIs, server-side rendering, or scripts that need to query a
Rangefind index without a browser.

```js
import { createNodeSearch } from "rangefind/node";

// Local index directory (fastest: positional file reads, no HTTP).
const engine = await createNodeSearch({ source: "./public/rangefind" });

// Or a deployed index over HTTP(S).
const remote = await createNodeSearch({
  source: "https://example.com/rangefind/",
  cacheDir: "/var/cache/rangefind"   // optional; defaults to a tmpdir path
});

const { results } = await engine.search({ q: "sparse inverted index" });
const suggestions = await engine.suggest({ q: "spar" });
```

The returned engine is the standard runtime engine (`search`, `count`,
`suggest`, `vectorSearch`, `hydrateRows`, facets, sorting, generational
indexes — everything), plus:

- `cacheStats()` — hit/miss/byte counters for the transport layer.
- `close()` — releases pooled file handles.

## What the transport layer does

The browser runtime gets caching for free from the HTTP cache. Node's `fetch`
has none of that, so `createNodeSearch` installs a transport that restores the
same semantics:

| Source | Behavior |
| --- | --- |
| Local path / `file://` | Byte ranges are served by positional reads on a pooled set of open file handles. No HTTP, no copies of whole packs. |
| `http(s)://` | Content-addressed objects (hash in the filename, or `Cache-Control: immutable` / long `max-age`) are cached in a bytes-bounded memory LRU and on disk, keyed by URL + byte range — equivalent to the browser treating immutable pack fetches as cache hits forever. |
| Mutable files (manifests) | Stored with their `ETag`/`Last-Modified` and revalidated with `If-None-Match` / `If-Modified-Since`; a `304` reuses the cached body — the browser's revalidation flow. |

The cache is process-wide and URL-keyed, so any number of engines over any
number of indexes share it safely. Incremental publishing composes with this
naturally: `--update` deltas keep unchanged pack names, so a redeployed index
reuses every cached object it didn't change.

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `source` | `./rangefind/` | Index location: directory path, `file://`, or `http(s)://` base URL. |
| `cacheDir` | `<tmpdir>/rangefind-node-cache` | Disk cache root for HTTP sources. |
| `diskCache` | `true` | Set `false` for memory-only caching. |
| `memoryCacheBytes` | 64 MiB | Process-wide memory LRU budget. |
| `fileHandleLimit` | 32 | Pooled open handles for local sources. |

All other options (`manifestName`, `trace`, `verifyChecksums`, ...) pass
through to the core runtime.

`resetNodeRuntimeCaches({ disk })` clears the process-wide caches — useful in
tests or after replacing an index in place.
