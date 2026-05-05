#!/usr/bin/env node

import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync, copyFileSync } from "node:fs";
import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { build } from "../../../src/builder.js";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(SCRIPT_DIR, "..");
const REPO_ROOT = resolve(ROOT, "../..");
const DEFAULT_DUMP_URL = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2";
const SCHEMA_VERSION = 1;

function parseArgs(argv) {
  const args = {
    dumpUrl: process.env.WIKI_DUMP_URL || DEFAULT_DUMP_URL,
    wiki: process.env.WIKI_ID || "",
    limit: Number(process.env.WIKI_LIMIT || 50000),
    bodyChars: Number(process.env.WIKI_BODY_CHARS || 12000),
    jsonl: process.env.WIKI_JSONL || "",
    force: false,
    buildProgressLogMs: Number(process.env.WIKI_BUILD_PROGRESS_MS || 15000)
  };
  for (const arg of argv) {
    if (arg === "--force") args.force = true;
    else if (arg.startsWith("--dump-url=")) args.dumpUrl = arg.slice("--dump-url=".length);
    else if (arg.startsWith("--wiki=")) args.wiki = arg.slice("--wiki=".length);
    else if (arg.startsWith("--limit=")) args.limit = Number(arg.slice("--limit=".length)) || 0;
    else if (arg.startsWith("--body-chars=")) args.bodyChars = Number(arg.slice("--body-chars=".length)) || 0;
    else if (arg.startsWith("--jsonl=")) args.jsonl = arg.slice("--jsonl=".length);
    else if (arg.startsWith("--build-progress-ms=")) args.buildProgressLogMs = Number(arg.slice("--build-progress-ms=".length)) || 0;
  }
  args.wiki ||= inferWikiId(args.dumpUrl);
  return args;
}

function inferWikiId(dumpUrl) {
  const match = /\/([a-z][a-z0-9_-]*wiki)\//iu.exec(String(dumpUrl || ""));
  return match?.[1] || "enwiki";
}

function wikiLanguage(wiki) {
  const match = /^([a-z-]+)wiki$/iu.exec(String(wiki || ""));
  return match?.[1] || "en";
}

function articleUrl(wiki, title) {
  const language = wikiLanguage(wiki);
  return `https://${language}.wikipedia.org/wiki/${encodeURIComponent(String(title).replaceAll(" ", "_"))}`;
}

function decodeXml(value) {
  return String(value || "")
    .replace(/&lt;/gu, "<")
    .replace(/&gt;/gu, ">")
    .replace(/&amp;/gu, "&")
    .replace(/&quot;/gu, "\"")
    .replace(/&#039;|&apos;/gu, "'");
}

function tag(page, name) {
  const match = new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)</${name}>`, "u").exec(page);
  return decodeXml(match?.[1] || "");
}

function stripWikitext(text) {
  return String(text || "")
    .replace(/<ref\b[\s\S]*?<\/ref>/giu, " ")
    .replace(/<ref\b[^/]*\/>/giu, " ")
    .replace(/<!--[\s\S]*?-->/gu, " ")
    .replace(/\{\{[\s\S]{0,2500}?\}\}/gu, " ")
    .replace(/\{\|[\s\S]*?\|\}/gu, " ")
    .replace(/\[\[(?:File|Fichier|Image|Media):[^\]]+\]\]/giu, " ")
    .replace(/\[\[(?:Category|Cat[ée]gorie):[^\]]+\]\]/giu, " ")
    .replace(/\[\[[^\]|]+\|([^\]]+)\]\]/gu, "$1")
    .replace(/\[\[([^\]]+)\]\]/gu, "$1")
    .replace(/\[https?:\/\/[^\s\]]+\s*([^\]]*)\]/gu, "$1")
    .replace(/'{2,}/gu, "")
    .replace(/={2,}\s*([^=]+?)\s*={2,}/gu, " $1 ")
    .replace(/<[^>]+>/gu, " ")
    .replace(/[{}[\]|]/gu, " ")
    .replace(/\s+/gu, " ")
    .trim();
}

function categoriesFromWikitext(text) {
  const out = [];
  const re = /\[\[\s*(?:Category|Cat[ée]gorie)\s*:\s*([^\]|#]+)(?:[^\]]*)\]\]/giu;
  let match;
  while ((match = re.exec(text))) {
    const value = match[1].trim();
    if (value && out.length < 12) out.push(value);
  }
  return out;
}

function articleTags(title, body, categories) {
  return [
    categories.length ? "has-categories" : "uncategorized",
    categories.length >= 5 ? "many-categories" : "few-categories",
    body.length >= 8000 ? "long-body" : "short-body",
    title.length >= 32 ? "long-title" : "short-title"
  ];
}

function pageToDoc(page, index, args) {
  const ns = tag(page, "ns");
  if (ns && ns !== "0") return null;
  const title = tag(page, "title").trim();
  const id = tag(page, "id").trim() || String(index + 1);
  if (!title || /<redirect\b/iu.test(page)) return null;
  const raw = tag(page, "text");
  let body = stripWikitext(raw);
  if (args.bodyChars > 0 && body.length > args.bodyChars) body = body.slice(0, args.bodyChars);
  if (body.length < 80) return null;
  const timestamp = tag(page, "timestamp");
  const revisionTime = Date.parse(timestamp);
  const categories = categoriesFromWikitext(raw);
  return {
    id,
    articleId: Number(id) || index + 1,
    title,
    titleLength: title.length,
    url: articleUrl(args.wiki, title),
    body,
    bodyLength: body.length,
    categories: categories.join(" "),
    categoryList: categories,
    category: categories[0] || "",
    categoryCount: categories.length,
    articleTags: articleTags(title, body, categories),
    hasCategories: categories.length > 0,
    revisionDate: Number.isFinite(revisionTime) ? new Date(revisionTime).toISOString().slice(0, 10) : "",
    source: args.wiki
  };
}

function sourceCommand(url) {
  if (/^https?:\/\//u.test(url)) return { cmd: "curl", args: ["-L", "--fail", "--silent", "--show-error", url] };
  return { cmd: "cat", args: [resolve(url)] };
}

function decompressorCommand(url) {
  if (url.endsWith(".bz2")) return { cmd: "bzip2", args: ["-dc"] };
  if (url.endsWith(".gz")) return { cmd: "gzip", args: ["-dc"] };
  return null;
}

function waitForChild(child, name, allowSignal = false) {
  return new Promise((resolveWait, rejectWait) => {
    child.on("error", rejectWait);
    child.on("close", (code, signal) => {
      if (code === 0 || (allowSignal && signal)) resolveWait();
      else rejectWait(new Error(`${name} exited with code ${code}${signal ? ` signal ${signal}` : ""}`));
    });
  });
}

function finishStream(stream) {
  return new Promise((resolveFinish, rejectFinish) => {
    stream.on("error", rejectFinish);
    stream.end(resolveFinish);
  });
}

function tempPath(path) {
  return `${path}.tmp-${process.pid}-${Date.now()}`;
}

function readJson(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

function expectedMeta(args) {
  return {
    schemaVersion: SCHEMA_VERSION,
    dumpUrl: args.jsonl ? "" : args.dumpUrl,
    jsonl: args.jsonl ? resolve(args.jsonl) : "",
    wiki: args.wiki,
    limit: args.limit || null,
    bodyChars: args.bodyChars || null
  };
}

function jsonlMatches(args, docsPath, metaPath) {
  if (args.force || !existsSync(docsPath) || !existsSync(metaPath)) return false;
  const meta = readJson(metaPath);
  const expected = expectedMeta(args);
  return meta?.schemaVersion === expected.schemaVersion
    && meta.dumpUrl === expected.dumpUrl
    && meta.jsonl === expected.jsonl
    && meta.wiki === expected.wiki
    && (meta.limit ?? null) === expected.limit
    && (meta.bodyChars ?? null) === expected.bodyChars;
}

async function writeJsonlPrefix(sourcePath, out, limit) {
  const tmp = tempPath(out);
  const input = createReadStream(resolve(sourcePath), { encoding: "utf8" });
  const output = createWriteStream(tmp);
  let buffer = "";
  let docs = 0;
  try {
    for await (const chunk of input) {
      buffer += chunk;
      while (true) {
        const index = buffer.indexOf("\n");
        if (index < 0) break;
        const line = buffer.slice(0, index);
        buffer = buffer.slice(index + 1);
        if (!line.trim()) continue;
        output.write(`${line}\n`);
        docs++;
        if (limit && docs >= limit) {
          input.destroy();
          await finishStream(output);
          renameSync(tmp, out);
          return docs;
        }
      }
    }
    if (buffer.trim()) {
      output.write(`${buffer.trimEnd()}\n`);
      docs++;
    }
    await finishStream(output);
    renameSync(tmp, out);
    return docs;
  } catch (error) {
    rmSync(tmp, { force: true });
    throw error;
  }
}

async function extractDump(args, out) {
  const sourceSpec = sourceCommand(args.dumpUrl);
  const source = spawn(sourceSpec.cmd, sourceSpec.args, { stdio: ["ignore", "pipe", "inherit"] });
  const sourceDone = waitForChild(source, sourceSpec.cmd, true);
  const decompSpec = decompressorCommand(args.dumpUrl);
  const decomp = decompSpec ? spawn(decompSpec.cmd, decompSpec.args, { stdio: ["pipe", "pipe", "inherit"] }) : null;
  const decompDone = decomp ? waitForChild(decomp, decompSpec.cmd, true) : Promise.resolve();
  if (decomp) source.stdout.pipe(decomp.stdin);
  source.stdout.on("error", () => {});
  if (decomp) {
    decomp.stdin.on("error", () => {});
    decomp.stdout.on("error", () => {});
  }

  const input = decomp ? decomp.stdout : source.stdout;
  input.setEncoding("utf8");
  const output = createWriteStream(out);
  let buffer = "";
  let docs = 0;
  let pages = 0;
  const started = performance.now();

  async function finish(complete) {
    await finishStream(output);
    await Promise.allSettled([sourceDone, decompDone]);
    return { docs, pagesRead: pages, complete, builtAt: new Date().toISOString() };
  }

  for await (const chunk of input) {
    buffer += chunk;
    while (true) {
      const start = buffer.indexOf("<page>");
      const end = buffer.indexOf("</page>");
      if (start < 0 || end < start) {
        if (start > 0) buffer = buffer.slice(start);
        break;
      }
      const page = buffer.slice(start, end + "</page>".length);
      buffer = buffer.slice(end + "</page>".length);
      pages++;
      const doc = pageToDoc(page, docs, args);
      if (!doc) continue;
      output.write(`${JSON.stringify(doc)}\n`);
      docs++;
      if (docs % 1000 === 0) {
        const seconds = (performance.now() - started) / 1000;
        console.error(`wiki-search: ${docs.toLocaleString()} docs from ${pages.toLocaleString()} pages (${(docs / Math.max(1, seconds)).toFixed(0)} docs/s)`);
      }
      if (args.limit && docs >= args.limit) {
        input.destroy();
        source.kill("SIGTERM");
        if (decomp) decomp.kill("SIGTERM");
        return finish(false);
      }
    }
  }
  return finish(true);
}

async function writeDocs(args) {
  const dataDir = resolve(ROOT, "data");
  const docsPath = resolve(dataDir, "wikipedia.jsonl");
  const metaPath = resolve(dataDir, "wikipedia.meta.json");
  mkdirSync(dataDir, { recursive: true });
  if (jsonlMatches(args, docsPath, metaPath)) return docsPath;

  const tmp = tempPath(docsPath);
  const extracted = args.jsonl
    ? { docs: await writeJsonlPrefix(args.jsonl, tmp, args.limit || 0), pagesRead: null, complete: !args.limit, builtAt: new Date().toISOString() }
    : await extractDump(args, tmp);
  if (!args.jsonl) renameSync(tmp, docsPath);
  else renameSync(tmp, docsPath);

  writeFileSync(metaPath, JSON.stringify({
    ...expectedMeta(args),
    docs: extracted.docs,
    pagesRead: extracted.pagesRead,
    complete: extracted.complete,
    builtAt: extracted.builtAt
  }, null, 2));
  return docsPath;
}

function syncRuntimeBundle() {
  const publicDir = resolve(ROOT, "public");
  mkdirSync(publicDir, { recursive: true });
  copyFileSync(resolve(REPO_ROOT, "dist/runtime.browser.js"), resolve(publicDir, "runtime.browser.js"));
}

function writeConfig(args, docsPath) {
  const config = {
    input: docsPath,
    output: "public/rangefind",
    idPath: "id",
    urlPath: "url",
    indexProfile: "static-large",
    targetPostingsPerDoc: 12,
    bodyIndexChars: 6000,
    alwaysIndexFields: ["title", "categories"],
    queryBundles: false,
    typoMode: "main-index",
    typoTrigger: "zero-or-weak",
    typoMaxEdits: 2,
    typoMaxTokenCandidates: 8,
    typoMaxQueryPlans: 5,
    typoMaxCorrectedSearches: 3,
    typoMaxShardLookups: 12,
    targetShardPostings: 45000,
    buildTelemetryPath: "wiki-search-build-telemetry.json",
    buildProgressLogMs: args.buildProgressLogMs,
    scanWorkers: 4,
    scanBatchDocs: 128,
    builderWorkerCount: 4,
    fields: [
      { name: "title", path: "title", weight: 6.0, b: 0.25, phrase: true, phraseWeight: 10, proximity: true, proximityWeight: 3, proximityWindow: 5 },
      { name: "categories", path: "categories", weight: 2.0, b: 0.0 },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    authority: [
      { name: "title", path: "title", weight: 1000000, exactWeight: 1000000, tokenWeight: 800000 }
    ],
    facets: [
      { name: "category", path: "category" },
      { name: "articleTags", path: "articleTags" }
    ],
    numbers: [
      { name: "articleId", path: "articleId", type: "int" },
      { name: "titleLength", path: "titleLength", type: "int" },
      { name: "bodyLength", path: "bodyLength", type: "int" },
      { name: "categoryCount", path: "categoryCount", type: "int" },
      { name: "revisionDate", path: "revisionDate", type: "date" }
    ],
    booleans: [
      { name: "hasCategories", path: "hasCategories" }
    ],
    display: [
      "id",
      "articleId",
      "title",
      "titleLength",
      "url",
      { name: "body", path: "body", maxChars: 900 },
      "bodyLength",
      "category",
      "categoryList",
      "articleTags",
      "categoryCount",
      "hasCategories",
      "revisionDate",
      "source"
    ]
  };
  const configPath = resolve(ROOT, "rangefind.config.json");
  writeFileSync(configPath, JSON.stringify(config, null, 2));
  return configPath;
}

function writeSiteMeta(args) {
  const docsMeta = readJson(resolve(ROOT, "data", "wikipedia.meta.json")) || {};
  const siteMeta = {
    name: "Wikipedia Search",
    source: args.wiki,
    dumpUrl: args.jsonl ? "" : args.dumpUrl,
    docs: docsMeta.docs || 0,
    limit: args.limit || null,
    bodyChars: args.bodyChars || null,
    builtAt: new Date().toISOString()
  };
  writeFileSync(resolve(ROOT, "public", "site-meta.json"), `${JSON.stringify(siteMeta, null, 2)}\n`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  mkdirSync(resolve(ROOT, "public"), { recursive: true });
  syncRuntimeBundle();
  const docsPath = await writeDocs(args);
  const configPath = writeConfig(args, docsPath);
  await build({ configPath });
  writeSiteMeta(args);
  console.log(`Built ${args.wiki} search site in ${ROOT}`);
  console.log(`Serve with: node scripts/serve.mjs examples/wiki-search/public 5182`);
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
