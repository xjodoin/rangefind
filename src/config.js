import { availableParallelism } from "node:os";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

export const DEFAULTS = {
  docChunkSize: 100,
  baseShardDepth: 3,
  maxShardDepth: 5,
  targetShardPostings: 30000,
  packBytes: 4 * 1024 * 1024,
  docPackBytes: 4 * 1024 * 1024,
  directoryPageBytes: 64 * 1024,
  docDirectoryPageBytes: 64 * 1024,
  reduceWorkers: 1,
  postingFlushLines: 100000,
  maxTermsPerDoc: 160,
  maxExpansionTermsPerDoc: 12,
  initialResultLimit: 20,
  postingBlockSize: 128,
  bm25fK1: 1.2
};

function configDir(configPath) {
  return dirname(resolve(configPath));
}

function resolveFrom(base, value) {
  return resolve(base, value || ".");
}

export async function readConfig(configPath) {
  const full = resolve(configPath);
  const base = configDir(full);
  const raw = JSON.parse(await readFile(full, "utf8"));
  const autoReduceWorkers = Math.max(1, Math.min(4, availableParallelism() - 1));
  const reduceWorkers = raw.reduceWorkers === "auto" || raw.reduceWorkers === 0
    ? autoReduceWorkers
    : Math.max(1, Number(raw.reduceWorkers ?? DEFAULTS.reduceWorkers) || DEFAULTS.reduceWorkers);
  return {
    ...DEFAULTS,
    ...raw,
    reduceWorkers,
    input: resolveFrom(base, raw.input),
    output: resolveFrom(base, raw.output || "public/rangefind"),
    fields: raw.fields || [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: raw.facets || [],
    numbers: raw.numbers || [],
    display: raw.display || ["title", "url"]
  };
}

export function getPath(object, path, fallback = "") {
  if (!path) return fallback;
  let value = object;
  for (const part of String(path).split(".")) {
    if (value == null) return fallback;
    value = value[part];
  }
  if (Array.isArray(value)) return value.join(" ");
  return value ?? fallback;
}
