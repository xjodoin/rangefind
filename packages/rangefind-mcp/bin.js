#!/usr/bin/env node

// CLI entry: `rangefind-mcp [--index name=path-or-url]... [--open]`
// Speaks MCP over stdio; all logging goes to stderr.

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createRequire } from "node:module";
import { createRangefindMcpServer } from "./index.js";

const { version } = createRequire(import.meta.url)("./package.json");

function usage() {
  console.error(`rangefind-mcp ${version} — MCP server over rangefind static search indexes

Usage:
  rangefind-mcp                                   # open mode: tools accept any index path/URL
  rangefind-mcp --index docs=./public/rangefind   # configured mode: named indexes only
  rangefind-mcp --index osm=https://cdn.example.com/rangefind/ --index docs=./dist/rangefind --open

Options:
  --index name=path-or-url   Register a named index (repeatable). Restricts
                             tools to the named set unless --open is given.
  --open                     Also accept arbitrary paths/URLs as the index
                             argument alongside configured names.
  --help                     Show this help.

Tools: rangefind_search, rangefind_suggest, rangefind_count, rangefind_info,
and rangefind_list_indexes (when indexes are configured).`);
}

const indexes = new Map();
let open;
for (let i = 2; i < process.argv.length; i++) {
  const arg = process.argv[i];
  const value = arg.startsWith("--index=") ? arg.slice(8) : arg === "--index" ? process.argv[++i] : null;
  if (value != null) {
    const eq = value.indexOf("=");
    if (eq <= 0) {
      console.error(`rangefind-mcp: --index expects name=path-or-url, got "${value}"`);
      process.exit(2);
    }
    indexes.set(value.slice(0, eq), value.slice(eq + 1));
  } else if (arg === "--open") {
    open = true;
  } else if (arg === "--help" || arg === "-h") {
    usage();
    process.exit(0);
  } else {
    console.error(`rangefind-mcp: unknown option "${arg}"`);
    usage();
    process.exit(2);
  }
}

const server = createRangefindMcpServer({ indexes, open, version });
await server.connect(new StdioServerTransport());
console.error(
  `rangefind-mcp ${version} ready (${indexes.size ? `indexes: ${[...indexes.keys()].join(", ")}` : "open mode"})`
);
