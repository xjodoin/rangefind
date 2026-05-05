#!/usr/bin/env node

import { stdin, stdout } from "node:process";
import { createInterface } from "node:readline";
import { basename, dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(process.env.RANGEFIND_REPO || resolve(here, "../../.."));
const { createSearch } = await import(pathToFileURL(resolve(repoRoot, "src/runtime.js")).href);
const { analyzeTerms } = await import(pathToFileURL(resolve(repoRoot, "src/analyzer.js")).href);
const { serveStatic } = await import(pathToFileURL(resolve(repoRoot, "scripts/bench_support.mjs")).href);

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchWithRetries(nativeFetch, input, init, retries) {
  let attempt = 0;
  while (true) {
    try {
      return await nativeFetch(input, init);
    } catch (error) {
      if (attempt >= retries) throw error;
      attempt++;
      await delay(20 * attempt);
    }
  }
}

function installFetchLimit(limit, retries = 2) {
  const max = Math.max(1, Math.floor(Number(limit) || 64));
  const attempts = Math.max(0, Math.floor(Number(retries) || 0));
  const nativeFetch = globalThis.fetch;
  let active = 0;
  const queue = [];
  globalThis.fetch = (input, init) => new Promise((resolve, reject) => {
    const run = () => {
      active++;
      fetchWithRetries(nativeFetch, input, init, attempts)
        .then(resolve, reject)
        .finally(() => {
          active--;
          queue.shift()?.();
        });
    };
    if (active < max) run();
    else queue.push(run);
  });
}

function normalizeGameQuery(query) {
  return String(query || "")
    .replace(/([+\-])([\p{L}\p{N}_]+)/gu, "$2")
    .replace(/"/gu, " ")
    .replace(/\b[a-zA-Z_][\w.]*:/gu, " ")
    .replace(/\b(?:AND|OR|NOT)\b/giu, " ")
    .replace(/[()]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

function commandSize(command) {
  const match = /^TOP_(\d+)(?:_COUNT)?$/u.exec(command);
  if (match) return Number(match[1]);
  if (command === "COUNT" || command === "UNOPTIMIZED_COUNT") return 1;
  return null;
}

function commandCounts(command) {
  return command === "COUNT" || command === "UNOPTIMIZED_COUNT" || /^TOP_\d+_COUNT$/u.test(command);
}

const indexPath = resolve(process.argv[2] || "index/public/rangefind");
installFetchLimit(process.env.RANGEFIND_SBG_FETCH_CONCURRENCY, process.env.RANGEFIND_SBG_FETCH_RETRIES);
const server = await serveStatic(dirname(indexPath));
const engine = await createSearch({
  baseUrl: new URL(`${basename(indexPath)}/`, server.url),
  maxPageSize: 1000,
  topKProofMaxK: 1000,
  postingBlockFrontier: 16,
  topKProofCheckInterval: 1024,
  topKBlockBudget: 2048,
  typoMode: "off"
});

const rl = createInterface({ input: stdin, crlfDelay: Infinity });
try {
  for await (const line of rl) {
    const fields = line.split("\t");
    if (fields.length !== 2) {
      stdout.write("UNSUPPORTED\n");
      continue;
    }
    const [command, rawQuery] = fields;
    const size = commandSize(command);
    if (!size) {
      stdout.write("UNSUPPORTED\n");
      continue;
    }
    if (commandCounts(command)) {
      stdout.write("UNSUPPORTED\n");
      continue;
    }
    const q = normalizeGameQuery(rawQuery);
    if (!analyzeTerms(q).length) {
      stdout.write("1\n");
      continue;
    }
    await engine.search({ q, size, exact: false, rerank: false, includeResults: false, authority: false });
    stdout.write("1\n");
  }
} finally {
  await server.close();
}
