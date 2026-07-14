---
title: Node runtime
lede: The same engine, server-side — for SSR, CLIs, MCP servers, and offline copies of an index.
description: rangefind/node — createNodeSearch over local directories or remote HTTP indexes, with browser-equivalent disk and memory caching.
order: 9
---

```js
import { createNodeSearch } from "rangefind/node";

// a local index directory (positional file reads, no HTTP)
const local = await createNodeSearch({ baseUrl: "./public/rangefind" });

// or a remote index (range requests + persistent caching)
const remote = await createNodeSearch({ baseUrl: "https://cdn.example.com/rangefind/" });

await local.search({ q: "harbor", size: 5 });
```

The engine interface is identical to the browser's
[query API](../query-api/) — search, suggest, count, vectors, geo, sharded
and generational indexes included.

## What the Node layer adds

- **Local directories** are read with pooled positional file reads —
  no server needed even locally.
- **Remote indexes** get browser-equivalent caching: immutable
  content-addressed objects cached in a byte-bounded memory LRU **and on
  disk**, manifests revalidated with ETags. A warm process answers most
  queries without touching the network.
- **One fetch router per process** serves any number of engines over
  different indexes — open a hundred shards or tenants cheaply.

```js
import { resetNodeRuntimeCaches } from "rangefind/node";
await resetNodeRuntimeCaches({ disk: true }); // e.g. between benchmark runs
```

## Where it fits

- **SSR / API routes** — render search results server-side against the same
  static index the browser uses; no drift between the two.
- **MCP servers and agents** — give an LLM tool-calling access to a corpus
  with zero infrastructure: the "database" is a directory.
- **CLIs and batch jobs** — grep-speed structured queries over large
  corpora, locally or from object storage.
- **Offline products** — ship a region's index with your app and query it
  with no connectivity at all.
