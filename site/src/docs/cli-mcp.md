---
title: CLI & MCP server
lede: Query any index from a shell — or hand it to an agent as tools. Same engine, no server either way.
description: The rangefind query CLI (search, suggest, count, info) and the rangefind-mcp Model Context Protocol server exposing any index as agent tools.
order: 10
---

Both work against **any** index — a local directory or an http(s) URL;
single, [generational](../incremental-updates/), or
[sharded](../sharded-indexes/).

## The query CLI

The `rangefind` binary queries indexes as well as building them:

```bash
# text search with facets and filters
npx rangefind search ./public/rangefind harbor lights --size 5 --facets topic
npx rangefind search ./public/rangefind study --filter topic=glacier --filter year=2004..2006

# geo: nearest-first, radius, or viewport
npx rangefind search https://cdn.example.com/rangefind/ --near 46.81,-71.21 --size 5
npx rangefind search ./index café --near 46.81,-71.21,5000
npx rangefind search ./index --box 46.7,-71.4,46.9,-71.0

# sharded indexes: scope to shard ids or group labels
npx rangefind search ./planet montreal --shards canada

# autocomplete, exact counts, metadata
npx rangefind suggest ./public/rangefind harb
npx rangefind count ./public/rangefind harbor
npx rangefind info ./planet
```

`--json` on any command returns the raw engine response for scripts.
`info` prints totals, build time, provenance (source, attribution,
license, data version), and — for sharded indexes — the shard list with
groups. Filters use `key=value` for facets, `key=min..max` for numeric
ranges, and `key=true|false` for booleans.

## The MCP server

`rangefind-mcp` (its own package, so the core stays dependency-free)
exposes the same engine as [Model Context
Protocol](https://modelcontextprotocol.io) tools for Claude and other
MCP clients:

```bash
npm install -g rangefind-mcp        # or use npx

# open mode: tools accept any index path/URL
rangefind-mcp

# configured mode: a fixed set of named indexes
rangefind-mcp --index docs=./public/rangefind --index osm=https://cdn.example.com/rangefind/
```

Claude Code:

```bash
claude mcp add rangefind -- npx rangefind-mcp --index docs=./public/rangefind
```

The tools — all read-only, all returning structured content:

| Tool | Purpose |
| --- | --- |
| `rangefind_search` | full-text + geo search with facets, filters, sorting, shard scoping |
| `rangefind_suggest` | search-as-you-type autocomplete |
| `rangefind_count` | exact match counts |
| `rangefind_info` | totals, provenance, features, shard tree — agents call this first |
| `rangefind_list_indexes` | configured index names |

An agent pointed at a sharded planet index can call `rangefind_info` to
discover the shard groups, then run `rangefind_search` scoped to
`["canada"]` with a `near` query — the whole flow costs a handful of range
requests against static files, with no API keys and no per-query billing.
