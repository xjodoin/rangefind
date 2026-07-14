// Resolved in place of node-only entries (`rangefind/builder`,
// `rangefind/node`, `rangefind/shards`, …) when a bundler targets the
// browser: fails loudly at import time instead of choking on `node:fs`
// deep inside the module graph.
throw new Error(
  "Rangefind: this entry point requires Node.js. " +
  "Browser code should import \"rangefind\" (query runtime), \"rangefind/browser\", or \"rangefind/element\"."
);
