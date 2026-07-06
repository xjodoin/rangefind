#!/usr/bin/env node

// Adds sentence embeddings to a wiki-search JSONL so the index can serve
// hybrid semantic search. Uses the same MiniLM model the browser demo loads
// through transformers.js, so query and document vectors share a space.
//
// The model dependency is optional and heavyweight; install it on demand:
//   npm install @huggingface/transformers
//
// Usage:
//   node examples/wiki-search/scripts/embed.mjs                # data/wikipedia.jsonl in place
//   node examples/wiki-search/scripts/embed.mjs --input=... --output=...

import { createReadStream, createWriteStream, renameSync } from "node:fs";
import { createInterface } from "node:readline";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(SCRIPT_DIR, "..");

export const EMBED_MODEL = "Xenova/all-MiniLM-L6-v2";
export const EMBED_DIMS = 384;
const BATCH_SIZE = 32;
const EMBED_CHARS = 480;

function parseArgs(argv) {
  const args = {
    input: resolve(ROOT, "data", "wikipedia.jsonl"),
    output: "",
    batch: BATCH_SIZE
  };
  for (const arg of argv) {
    if (arg.startsWith("--input=")) args.input = resolve(arg.slice(8));
    else if (arg.startsWith("--output=")) args.output = resolve(arg.slice(9));
    else if (arg.startsWith("--batch=")) args.batch = Number(arg.slice(8)) || BATCH_SIZE;
  }
  if (!args.output) args.output = args.input;
  return args;
}

async function loadEmbedder() {
  let transformers;
  try {
    transformers = await import("@huggingface/transformers");
  } catch {
    throw new Error(
      "Embedding needs the optional model runtime. Install it with:\n  npm install @huggingface/transformers"
    );
  }
  const extractor = await transformers.pipeline("feature-extraction", EMBED_MODEL, { dtype: "q8" });
  return async (texts) => {
    const output = await extractor(texts, { pooling: "mean", normalize: true });
    const data = output.data;
    const rows = [];
    for (let i = 0; i < texts.length; i++) {
      rows.push(new Float32Array(data.buffer, data.byteOffset + i * EMBED_DIMS * 4, EMBED_DIMS).slice());
    }
    output.dispose?.();
    return rows;
  };
}

export function embeddingText(doc) {
  return `${doc.title || ""}. ${String(doc.body || "").slice(0, EMBED_CHARS)}`.trim();
}

export async function embedJsonl({ input, output, batch = BATCH_SIZE, log = console.error }) {
  const embed = await loadEmbedder();
  const tmp = `${output}.tmp`;
  const out = createWriteStream(tmp);
  const lines = createInterface({ input: createReadStream(input), crlfDelay: Infinity });
  let pending = [];
  let docs = 0;
  let skipped = 0;
  const started = performance.now();

  async function flush() {
    if (!pending.length) return;
    const rows = await embed(pending.map(item => embeddingText(item)));
    for (let i = 0; i < pending.length; i++) {
      const doc = pending[i];
      doc.embedding = Buffer.from(rows[i].buffer, rows[i].byteOffset, EMBED_DIMS * 4).toString("base64");
      if (!out.write(`${JSON.stringify(doc)}\n`)) {
        await new Promise(resolveDrain => out.once("drain", resolveDrain));
      }
    }
    docs += pending.length;
    if (docs % 512 < batch) {
      const perSecond = docs / ((performance.now() - started) / 1000);
      log(`[embed] ${docs} docs (${perSecond.toFixed(1)}/s)`);
    }
    pending = [];
  }

  for await (const line of lines) {
    if (!line) continue;
    const doc = JSON.parse(line);
    if (doc.embedding) {
      skipped += 1;
      if (!out.write(`${JSON.stringify(doc)}\n`)) {
        await new Promise(resolveDrain => out.once("drain", resolveDrain));
      }
      continue;
    }
    pending.push(doc);
    if (pending.length >= batch) await flush();
  }
  await flush();
  await new Promise((resolveEnd, reject) => out.end(err => (err ? reject(err) : resolveEnd())));
  renameSync(tmp, output);
  const seconds = Math.round((performance.now() - started) / 100) / 10;
  log(`[embed] wrote ${docs + skipped} docs (${docs} embedded, ${skipped} already had vectors) in ${seconds}s`);
  return { docs: docs + skipped, embedded: docs, seconds };
}

const invokedDirectly = process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (invokedDirectly) {
  const args = parseArgs(process.argv.slice(2));
  embedJsonl(args).catch(err => {
    console.error(err.message || err);
    process.exit(1);
  });
}
