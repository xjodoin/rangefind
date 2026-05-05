#!/usr/bin/env node

import { createWriteStream, mkdirSync, writeFileSync } from "node:fs";
import { stdin } from "node:process";
import { createInterface } from "node:readline";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = resolve(process.env.RANGEFIND_REPO || resolve(here, "../../.."));
const { build } = await import(pathToFileURL(resolve(repoRoot, "src/builder.js")).href);

const outDir = resolve(process.argv[2] || "index");
const inputPath = process.env.RANGEFIND_SBG_INPUT ? resolve(process.env.RANGEFIND_SBG_INPUT) : "";
const docsPath = inputPath || resolve(outDir, "docs.jsonl");
const configPath = resolve(outDir, "rangefind.config.json");
mkdirSync(outDir, { recursive: true });

function envInt(name, fallback) {
  const parsed = Math.floor(Number(process.env[name] ?? fallback));
  return Number.isFinite(parsed) ? parsed : fallback;
}

const mode = String(process.env.RANGEFIND_SBG_MODE || "full").toLowerCase();
const fullTextMode = mode === "full" || mode === "full-text";
const targetPostingsPerDoc = envInt("RANGEFIND_SBG_TARGET_POSTINGS", fullTextMode ? 0 : 24);
const bodyIndexChars = envInt("RANGEFIND_SBG_BODY_CHARS", fullTextMode ? 0 : 3000);
const workerCount = envInt("RANGEFIND_SBG_WORKERS", Math.max(1, Math.min(4, Math.floor((Number(process.env.npm_config_jobs) || 4)))));

if (!inputPath) {
  const docsOut = createWriteStream(docsPath);
  const rl = createInterface({ input: stdin, crlfDelay: Infinity });
  let ordinal = 0;
  for await (const line of rl) {
    if (!line.trim()) continue;
    const parsed = JSON.parse(line);
    const id = parsed.id == null || parsed.id === "" ? String(ordinal) : String(parsed.id);
    if (!docsOut.write(`${JSON.stringify({
      id,
      text: String(parsed.text || ""),
      sort_field: Number.isFinite(Number(parsed.sort_field)) ? Number(parsed.sort_field) : ordinal
    })}\n`)) {
      await new Promise(resolveDrain => docsOut.once("drain", resolveDrain));
    }
    ordinal++;
  }
  await new Promise((resolveFinish, rejectFinish) => {
    docsOut.on("error", rejectFinish);
    docsOut.end(resolveFinish);
  });
}

writeFileSync(configPath, `${JSON.stringify({
  input: docsPath,
  output: "public/rangefind",
  idPath: "id",
  urlPath: "id",
  display: ["id"],
  fields: [
    { name: "text", path: "text", weight: 1, b: 0.75 }
  ],
  numbers: [],
  targetPostingsPerDoc,
  bodyIndexChars,
  alwaysIndexFields: fullTextMode ? ["text"] : [],
  queryBundles: false,
  authority: [],
  typoMode: "off",
  scanWorkers: workerCount,
  builderWorkerCount: workerCount,
  partitionReducerWorkers: workerCount,
  buildProgressLogMs: 0
}, null, 2)}\n`);

await build({ configPath });
