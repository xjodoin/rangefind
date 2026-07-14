#!/usr/bin/env node

import { build } from "../src/builder.js";
import { buildFromCrawl } from "../src/crawler.js";

function usage() {
  console.log(`rangefind

Usage:
  rangefind build <dir> [--output <dir>] [--base-url <url>] [--root <dir>]
  rangefind build --config path/to/rangefind.config.json
  rangefind build --config path/to/delta.config.json --update
  rangefind build --config path/to/rangefind.config.json --compact

Commands:
  build   Build a static range-packed search index.

          With a positional <dir>, crawl that built static site: extract
          text from every .html/.htm file and index it directly.
            --output    Output directory (default: <dir>/rangefind).
            --base-url  URL prefix or origin for result URLs (default: "/").
            --root      Directory whose relative paths define ids/URLs
                        (default: <dir>).
            --enrich    Path to an ES module whose default export is an
                        async function(docs) run on the crawled documents
                        before indexing (add embeddings, metadata, …).
                        The module may also export "config": overrides
                        merged into the generated build config.

          With --config, build from a JSONL corpus described by the config.
            --update    Treat the config's input as a delta (new or replaced
                        documents) added as a new generation over the output.
            --compact   Fold a generational index back into one index: a full
                        rebuild (the input must be the FULL corpus) that then
                        removes the old generation directories.
`);
}

function parseArgs(argv) {
  const args = { positionals: [] };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--config") args.config = argv[++i];
    else if (arg.startsWith("--config=")) args.config = arg.slice("--config=".length);
    else if (arg === "--output") args.output = argv[++i];
    else if (arg.startsWith("--output=")) args.output = arg.slice("--output=".length);
    else if (arg === "--base-url") args.baseUrl = argv[++i];
    else if (arg.startsWith("--base-url=")) args.baseUrl = arg.slice("--base-url=".length);
    else if (arg === "--root") args.root = argv[++i];
    else if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg === "--enrich") args.enrich = argv[++i];
    else if (arg.startsWith("--enrich=")) args.enrich = arg.slice("--enrich=".length);
    else if (arg === "--update") args.update = true;
    else if (arg === "--compact") args.compact = true;
    else if (arg === "--help" || arg === "-h") args.help = true;
    else if (arg.startsWith("--")) args.unknown = arg;
    else args.positionals.push(arg);
  }
  return args;
}

const [command, ...rest] = process.argv.slice(2);
const args = parseArgs(rest);

if (!command || args.help) {
  usage();
  process.exit(command ? 0 : 1);
}

if (command !== "build") {
  console.error(`Unknown command: ${command}`);
  usage();
  process.exit(1);
}

if (args.unknown) {
  console.error(`Unknown option: ${args.unknown}`);
  usage();
  process.exit(1);
}

const dir = args.positionals[0];

if (dir && args.config) {
  console.error("Pass either a directory to crawl or --config, not both.");
  usage();
  process.exit(1);
}

if (dir) {
  const output = args.output || `${dir.replace(/\/+$/, "")}/rangefind`;
  await buildFromCrawl({
    root: args.root || dir,
    scanDir: dir,
    output,
    baseUrl: args.baseUrl || "/",
    enrich: args.enrich
  });
} else if (args.config) {
  await build({ configPath: args.config, update: !!args.update, compact: !!args.compact });
} else {
  console.error("Missing directory to crawl or --config");
  usage();
  process.exit(1);
}
