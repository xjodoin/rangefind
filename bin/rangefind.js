#!/usr/bin/env node

import { build } from "../src/builder.js";

function usage() {
  console.log(`rangefind

Usage:
  rangefind build --config path/to/rangefind.config.json

Commands:
  build   Build a static range-packed search index from JSONL documents.
`);
}

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === "--config") args.config = argv[++i];
    else if (arg.startsWith("--config=")) args.config = arg.slice("--config=".length);
    else if (arg === "--help" || arg === "-h") args.help = true;
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

if (!args.config) {
  console.error("Missing --config");
  usage();
  process.exit(1);
}

await build({ configPath: args.config });
