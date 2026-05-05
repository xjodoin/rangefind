#!/usr/bin/env node

import { stdin, stdout } from "node:process";
import { createInterface } from "node:readline";
import { basename, dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(process.env.RANGEFIND_REPO || resolve(here, "../../.."));
const { createSearch } = await import(pathToFileURL(resolve(repoRoot, "src/runtime.js")).href);
const { serveStatic } = await import(pathToFileURL(resolve(repoRoot, "scripts/bench_support.mjs")).href);

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
const server = await serveStatic(dirname(indexPath));
const engine = await createSearch({ baseUrl: new URL(`${basename(indexPath)}/`, server.url) });

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
    const response = await engine.search({ q: normalizeGameQuery(rawQuery), size, exact: commandCounts(command) });
    stdout.write(`${commandCounts(command) ? response.total : 1}\n`);
  }
} finally {
  await server.close();
}
