# rangefind-mcp

An [MCP](https://modelcontextprotocol.io) server over
[rangefind](https://github.com/xjodoin/rangefind) static search indexes.
Point it at any index — a local directory or an http(s) URL; single,
generational, or sharded — and agents get search as tools. No search server
behind it: the index is plain files, read with HTTP range requests and
cached on disk.

## Run

```bash
# open mode: tools accept any index path or URL
npx rangefind-mcp

# configured mode: named indexes only
npx rangefind-mcp --index docs=./public/rangefind --index osm=https://cdn.example.com/rangefind/
```

Claude Code:

```bash
claude mcp add rangefind -- npx rangefind-mcp --index docs=./public/rangefind
```

Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "rangefind": {
      "command": "npx",
      "args": ["rangefind-mcp", "--index", "docs=/path/to/rangefind"]
    }
  }
}
```

## Tools

| Tool | Purpose |
| --- | --- |
| `rangefind_search` | BM25F full-text search with typo correction, facets, filters, sorting, geo (radius / bounding box / nearest-first / distance boost), shard scoping, pagination |
| `rangefind_suggest` | search-as-you-type autocomplete; `hydrate: true` also returns each suggestion's resolved document, so a completion often answers the question without a follow-up `rangefind_search` |
| `rangefind_count` | exact match counts without fetching results |
| `rangefind_info` | index metadata: totals, build time, provenance/license, features, shard list with groups and coverage bboxes |
| `rangefind_list_indexes` | the configured index names (configured mode only) |

All tools are read-only and return structured content. Errors are
actionable tool results (unknown index → the available names; unknown shard
→ the available shards/groups), not protocol failures.

## Programmatic use

```js
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createRangefindMcpServer } from "rangefind-mcp";

const server = createRangefindMcpServer({
  indexes: { docs: "./public/rangefind" },
  open: false
});
await server.connect(new StdioServerTransport());
```

## Why this is interesting for agents

A rangefind index is a self-describing corpus: `rangefind_info` exposes
what it contains (including data provenance and, for sharded planet-scale
indexes, the geographic shard tree), and every query — text, geo, faceted —
runs against static files with no keys, no rate limits, and no per-query
cost. Offline corpora work too: point `--index` at a downloaded directory.
