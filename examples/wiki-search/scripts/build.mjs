#!/usr/bin/env node

import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync, copyFileSync } from "node:fs";
import { createInterface } from "node:readline";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { availableParallelism } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { build } from "../../../src/builder.js";
import { computeLinkRank } from "../../../src/link_graph.js";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(SCRIPT_DIR, "..");
const REPO_ROOT = resolve(ROOT, "../..");
const DEFAULT_DUMP_URL = "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2";
// Bump when the emitted document schema changes so cached JSONL is regenerated.
// v3 adds real [[wikilink]] extraction and the linkRank authority signal.
const SCHEMA_VERSION = 3;

// [[prefix:...]] links to these namespaces are not article-to-article edges.
const NON_ARTICLE_LINK = /^(File|Fichier|Image|Media|Média|Category|Cat[ée]gorie|Help|Aide|Template|Mod[èe]le|Wikipedia|Wikip[ée]dia|Portal|Portail|Project|Projet|User|Utilisateur|Special|Sp[ée]cial|Talk|Discussion|MediaWiki|Module)\s*:/iu;

// Wikipedia capitalizes the first letter of every article title, so links and
// titles collapse to the same key once the first letter is upper-cased.
function normalizeTitle(raw) {
  const t = String(raw || "").replace(/_/gu, " ").trim();
  return t ? t.charAt(0).toUpperCase() + t.slice(1) : "";
}

// Extract article-namespace [[Target]] / [[Target|label]] link titles from raw
// wikitext, before stripWikitext flattens them away. Deduplicated per article.
function extractLinks(wikitext) {
  const out = [];
  const seen = new Set();
  const re = /\[\[([^\]]+?)\]\]/gu;
  let match;
  while ((match = re.exec(wikitext))) {
    let target = match[1];
    const pipe = target.indexOf("|");
    if (pipe >= 0) target = target.slice(0, pipe);
    const hash = target.indexOf("#");
    if (hash >= 0) target = target.slice(0, hash);
    target = target.trim();
    if (!target || target.startsWith(":") || NON_ARTICLE_LINK.test(target)) continue;
    const title = normalizeTitle(target);
    if (title && !seen.has(title)) { seen.add(title); out.push(title); }
  }
  return out;
}

function parseArgs(argv) {
  const args = {
    root: resolve(process.env.WIKI_ROOT || ROOT),
    dumpUrl: process.env.WIKI_DUMP_URL || DEFAULT_DUMP_URL,
    wiki: process.env.WIKI_ID || "",
    limit: Number(process.env.WIKI_LIMIT || 50000),
    bodyChars: Number(process.env.WIKI_BODY_CHARS || 6000),
    jsonl: process.env.WIKI_JSONL || "",
    embed: process.env.WIKI_EMBED === "1",
    suggest: process.env.WIKI_SUGGEST !== "0",
    linkRank: process.env.WIKI_LINK_RANK !== "0",
    multistream: Math.max(0, Number(process.env.WIKI_MULTISTREAM || 0)),
    force: false,
    buildProgressLogMs: Number(process.env.WIKI_BUILD_PROGRESS_MS || 15000)
  };
  for (const arg of argv) {
    if (arg === "--force") args.force = true;
    else if (arg === "--embed") args.embed = true;
    else if (arg === "--no-suggest") args.suggest = false;
    else if (arg === "--no-link-rank") args.linkRank = false;
    else if (arg === "--link-rank") args.linkRank = true;
    else if (arg.startsWith("--root=")) args.root = resolve(arg.slice("--root=".length));
    else if (arg === "--multistream") args.multistream = 3;
    else if (arg.startsWith("--multistream=")) args.multistream = Math.max(1, Number(arg.slice("--multistream=".length)) || 3);
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

function articleTags(title, bodyLength, categories) {
  return [
    categories.length ? "has-categories" : "uncategorized",
    categories.length >= 5 ? "many-categories" : "few-categories",
    bodyLength >= 8000 ? "long-body" : "short-body",
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
  const bodyLength = body.length;
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
    // Real [[wikilink]] targets (article namespace) for the link graph. Removed
    // again after linkRank is computed so the raw list is never indexed.
    ...(args.linkRank ? { links: extractLinks(raw) } : {}),
    body,
    bodyLength,
    categories: categories.join(" "),
    categoryList: categories,
    category: categories[0] || "",
    categoryCount: categories.length,
    articleTags: articleTags(title, bodyLength, categories),
    hasCategories: categories.length > 0,
    revisionDate: Number.isFinite(revisionTime) ? new Date(revisionTime).toISOString().slice(0, 10) : "",
    source: args.wiki
  };
}

function sourceCommand(url) {
  if (/^https?:\/\//u.test(url)) return {
    cmd: "curl",
    args: [
      "-L", "--fail", "--silent", "--show-error",
      "--retry", "8", "--retry-all-errors", "--retry-delay", "5",
      "--user-agent", "rangefind-wikipedia-builder/0.2",
      url
    ]
  };
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

async function writeWithBackpressure(stream, chunk) {
  if (!stream.write(chunk)) await once(stream, "drain");
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
    bodyChars: args.bodyChars || null,
    linkRank: !!args.linkRank
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
    && (meta.bodyChars ?? null) === expected.bodyChars
    && (meta.linkRank ?? false) === expected.linkRank;
}

// Compute PageRank over the extracted [[wikilinks]] and fold a normalized
// `linkRank` doc-value into each record, dropping the raw `links` list. Three
// streaming passes keep memory bounded (a title->ordinal map plus the edge
// arrays) so this scales with the corpus, not with body text. Returns the node
// and edge counts, or null when the data carried no resolvable links (e.g. an
// external --jsonl without a links field), in which case linkRank is left off.
async function enrichLinkRank(path) {
  const streamLines = async function* () {
    const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
    for await (const line of rl) if (line) yield line;
  };

  // Pass 1: title -> ordinal (line order is the stable ordinal).
  const titleToOrdinal = new Map();
  let n = 0;
  for await (const line of streamLines()) {
    try {
      const title = JSON.parse(line).title;
      if (title != null && !titleToOrdinal.has(String(title))) titleToOrdinal.set(String(title), n);
    } catch { /* skip malformed */ }
    n++;
  }
  if (!n) return null;

  // Pass 2: resolve each record's links to ordinals -> adjacency.
  const adjacency = new Array(n);
  for (let i = 0; i < n; i++) adjacency[i] = [];
  let edges = 0;
  let ordinal = 0;
  for await (const line of streamLines()) {
    try {
      const links = JSON.parse(line).links;
      if (Array.isArray(links)) {
        const seen = new Set();
        const row = adjacency[ordinal];
        for (const target of links) {
          const t = titleToOrdinal.get(String(target));
          if (t === undefined || t === ordinal || seen.has(t)) continue;
          seen.add(t);
          row.push(t);
        }
        edges += row.length;
      }
    } catch { /* skip malformed */ }
    ordinal++;
  }
  if (edges === 0) return null;

  const ranks = computeLinkRank(adjacency);

  // Pass 3: write linkRank, drop the raw links list.
  const tmp = tempPath(path);
  const out = createWriteStream(tmp);
  ordinal = 0;
  for await (const line of streamLines()) {
    let doc;
    try {
      doc = JSON.parse(line);
    } catch {
      await writeWithBackpressure(out, `${line}\n`);
      ordinal++;
      continue;
    }
    delete doc.links;
    doc.linkRank = Math.round(ranks[ordinal] * 1e6) / 1e6;
    await writeWithBackpressure(out, `${JSON.stringify(doc)}\n`);
    ordinal++;
  }
  await finishStream(out);
  renameSync(tmp, path);
  return { nodes: n, edges };
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
        await writeWithBackpressure(output, `${line}\n`);
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
      await writeWithBackpressure(output, `${buffer.trimEnd()}\n`);
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
  // A child can fail before the streaming loop reaches finish(); attach a
  // handler immediately so Node does not treat that as an unhandled rejection.
  sourceDone.catch(() => {});
  const decompSpec = decompressorCommand(args.dumpUrl);
  const decomp = decompSpec ? spawn(decompSpec.cmd, decompSpec.args, { stdio: ["pipe", "pipe", "inherit"] }) : null;
  const decompDone = decomp ? waitForChild(decomp, decompSpec.cmd, true) : Promise.resolve();
  decompDone.catch(() => {});
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
    const children = await Promise.allSettled([sourceDone, decompDone]);
    const failure = children.find(result => result.status === "rejected");
    if (failure) throw failure.reason;
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
      await writeWithBackpressure(output, `${JSON.stringify(doc)}\n`);
      docs++;
      if (docs % Math.max(1000, args.progressEvery || 1000) === 0) {
        const seconds = (performance.now() - started) / 1000;
        console.error(`${args.progressPrefix || "wiki-search"}: ${docs.toLocaleString()} docs from ${pages.toLocaleString()} pages (${(docs / Math.max(1, seconds)).toFixed(0)} docs/s)`);
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

function multistreamStart(name) {
  const match = /-p(\d+)p\d+\.bz2$/u.exec(name);
  return Number(match?.[1] || Number.MAX_SAFE_INTEGER);
}

async function discoverMultistreamShards(dumpUrl) {
  const directoryUrl = new URL("./", dumpUrl);
  const response = await fetch(directoryUrl);
  if (!response.ok) throw new Error(`Unable to list Wikimedia dump shards at ${directoryUrl}`);
  const html = await response.text();
  const wiki = inferWikiId(dumpUrl).replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const pattern = new RegExp(`href="(${wiki}-latest-pages-articles-multistream\\d+\\.xml-p\\d+p\\d+\\.bz2)"`, "gu");
  const names = [...html.matchAll(pattern)]
    .map(match => match[1]);
  const unique = [...new Set(names)].sort((a, b) => multistreamStart(a) - multistreamStart(b) || a.localeCompare(b));
  if (!unique.length) throw new Error(`No Wikimedia pages-articles multistream shards found at ${directoryUrl}`);
  return unique.map(name => new URL(name, directoryUrl).href);
}

async function appendFileWithBackpressure(path, output) {
  for await (const chunk of createReadStream(path)) await writeWithBackpressure(output, chunk);
}

async function extractMultistreamDump(args, out) {
  const urls = await discoverMultistreamShards(args.dumpUrl);
  const concurrency = Math.max(1, Math.min(urls.length, Math.floor(args.multistream || 3)));
  const partsDir = resolve(dirname(out), ".wikipedia-multistream-parts");
  mkdirSync(partsDir, { recursive: true });
  const parts = urls.map((_, index) => resolve(partsDir, `part-${String(index).padStart(3, "0")}.jsonl`));
  const results = new Array(urls.length);
  let next = 0;
  let completedDocs = 0;
  let completedShards = 0;
  const started = performance.now();

  async function worker() {
    while (true) {
      const index = next++;
      if (index >= urls.length) return;
      const metaPath = `${parts[index]}.meta.json`;
      const cached = readJson(metaPath);
      if (
        existsSync(parts[index])
        && cached?.complete === true
        && cached.url === urls[index]
        && cached.schemaVersion === SCHEMA_VERSION
        && cached.bodyChars === (args.bodyChars || null)
      ) {
        results[index] = cached;
      } else {
        rmSync(parts[index], { force: true });
        rmSync(metaPath, { force: true });
        results[index] = await extractDump({
          ...args,
          dumpUrl: urls[index],
          limit: 0,
          progressEvery: 10000,
          progressPrefix: `wiki-search shard ${index + 1}/${urls.length}`
        }, parts[index]);
        writeFileSync(metaPath, JSON.stringify({
          ...results[index],
          url: urls[index],
          schemaVersion: SCHEMA_VERSION,
          bodyChars: args.bodyChars || null
        }));
      }
      completedDocs += results[index].docs;
      completedShards++;
      const seconds = (performance.now() - started) / 1000;
      console.error(`wiki-search: completed ${completedShards}/${urls.length} shards, ${completedDocs.toLocaleString()} docs (${(completedDocs / Math.max(1, seconds)).toFixed(0)} docs/s aggregate)`);
    }
  }

  const settled = await Promise.allSettled(Array.from({ length: concurrency }, worker));
  const failure = settled.find(result => result.status === "rejected");
  if (failure) throw failure.reason;
  const output = createWriteStream(out);
  try {
    for (const part of parts) await appendFileWithBackpressure(part, output);
    await finishStream(output);
  } catch (error) {
    output.destroy();
    throw error;
  }
  rmSync(partsDir, { recursive: true, force: true });
  return {
    docs: results.reduce((sum, result) => sum + (result?.docs || 0), 0),
    pagesRead: results.reduce((sum, result) => sum + (result?.pagesRead || 0), 0),
    complete: true,
    shards: urls.length,
    builtAt: new Date().toISOString()
  };
}

async function writeDocs(args) {
  const dataDir = resolve(args.root, "data");
  const docsPath = resolve(dataDir, "wikipedia.jsonl");
  const metaPath = resolve(dataDir, "wikipedia.meta.json");
  mkdirSync(dataDir, { recursive: true });
  if (jsonlMatches(args, docsPath, metaPath)) {
    // Cached JSONL is already enriched; reflect that so writeConfig declares it.
    args.linkRankApplied = !!(readJson(metaPath)?.linkRank);
    return docsPath;
  }

  const tmp = tempPath(docsPath);
  const extracted = args.jsonl
    ? { docs: await writeJsonlPrefix(args.jsonl, tmp, args.limit || 0), pagesRead: null, complete: !args.limit, builtAt: new Date().toISOString() }
    // A limited build must stop after the first N accepted pages in global
    // dump order. Concurrent shards cannot provide that guarantee without
    // downloading and assembling the full corpus, so keep limits on the
    // ordered single-stream path and reserve multistream for full dumps.
    : args.multistream > 0 && !args.limit
      ? await extractMultistreamDump(args, tmp)
      : await extractDump(args, tmp);

  // Fold the link-graph authority signal in before publishing, so the cached
  // JSONL and its meta reflect the enriched state (and a cache hit skips it).
  let linkRankStats = null;
  if (args.linkRank) {
    const started = performance.now();
    linkRankStats = await enrichLinkRank(tmp);
    if (linkRankStats) {
      console.error(`wiki-search: linkRank over ${linkRankStats.nodes.toLocaleString()} articles, `
        + `${linkRankStats.edges.toLocaleString()} edges (${((performance.now() - started) / 1000).toFixed(1)}s)`);
    } else {
      console.error("wiki-search: no resolvable [[wikilinks]] found; linkRank skipped.");
    }
  }
  args.linkRankApplied = !!linkRankStats;

  renameSync(tmp, docsPath);

  writeFileSync(metaPath, JSON.stringify({
    ...expectedMeta(args),
    docs: extracted.docs,
    pagesRead: extracted.pagesRead,
    shards: extracted.shards || 1,
    complete: extracted.complete,
    linkRank: args.linkRankApplied,
    linkRankEdges: linkRankStats?.edges || 0,
    builtAt: extracted.builtAt
  }, null, 2));
  return docsPath;
}

function syncRuntimeBundle(args) {
  const publicDir = resolve(args.root, "public");
  mkdirSync(publicDir, { recursive: true });
  copyFileSync(resolve(REPO_ROOT, "dist/runtime.browser.js"), resolve(publicDir, "runtime.browser.js"));
  if (args.root !== ROOT) {
    for (const file of ["index.html", "app.js", "styles.css"]) {
      copyFileSync(resolve(ROOT, "public", file), resolve(publicDir, file));
    }
  }
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
    // Impact ordering lets block-max traversal prove broad top-k queries
    // after the highest-scoring blocks instead of walking doc-id order.
    // Bucket ordering keeps the high-df builder path linear.
    postingOrder: "impact",
    postingImpactBucketOrderMinRows: 2048,
    postingImpactTiers: true,
    postingBlockSize: "auto",
    postingSuperblockSize: "auto",
    postingGzipLevel: 3,
    codecs: { mode: "auto" },
    typoMode: "main-index",
    typoTrigger: "zero-or-weak",
    typoMaxEdits: 2,
    typoMaxTokenCandidates: 8,
    typoMaxQueryPlans: 5,
    typoMaxCorrectedSearches: 3,
    typoMaxShardLookups: 12,
    targetShardPostings: 45000,
    segmentMergeFanIn: 512,
    // Full-corpus locality reordering causes millions of random spool reads.
    // Preserve input doc-id order and pack through bounded sequential windows.
    docLayoutStrategy: "doc-id",
    buildTelemetryPath: "wiki-search-build-telemetry.json",
    buildProgressLogMs: args.buildProgressLogMs,
    scanWorkers: Math.max(4, Math.min(10, availableParallelism() - 4)),
    scanBatchDocs: 128,
    builderWorkerCount: Math.max(4, Math.min(8, Math.floor(availableParallelism() / 2))),
    fields: [
      { name: "title", path: "title", weight: 6.0, b: 0.25, phrase: true, phraseWeight: 10, proximity: true, proximityWeight: 3, proximityWindow: 5 },
      { name: "categories", path: "categories", weight: 2.0, b: 0.0 },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    authority: [
      { name: "title", path: "title", weight: 1000000, exactWeight: 1000000, tokenWeight: 800000 }
    ],
    suggest: args.suggest ? [{ path: "title" }] : [],
    ...(args.embed
      ? { vectors: [{ name: "embedding", path: "embedding", dims: 384 }] }
      : {}),
    facets: [
      { name: "category", path: "category" },
      { name: "articleTags", path: "articleTags" }
    ],
    numbers: [
      { name: "articleId", path: "articleId", type: "int" },
      { name: "titleLength", path: "titleLength", type: "int" },
      { name: "bodyLength", path: "bodyLength", type: "int" },
      { name: "categoryCount", path: "categoryCount", type: "int" },
      { name: "revisionDate", path: "revisionDate", type: "date" },
      ...(args.linkRankApplied ? [{ name: "linkRank", path: "linkRank", type: "double", sortable: true }] : [])
    ],
    ...(args.linkRankApplied
      ? {
          sorts: [{ field: "linkRank", order: "desc" }],
          // Authority prior: well-linked articles win near-ties (score *= 1 +
          // boost*linkRank), applied as a bounded relevance-time rerank.
          linkGraph: { field: "linkRank", boost: 0.5 }
        }
      : {}),
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
      "source",
      ...(args.linkRankApplied ? ["linkRank"] : [])
    ]
  };
  const configPath = resolve(args.root, "rangefind.config.json");
  writeFileSync(configPath, JSON.stringify(config, null, 2));
  return configPath;
}

function writeSiteMeta(args) {
  const docsMeta = readJson(resolve(args.root, "data", "wikipedia.meta.json")) || {};
  const siteMeta = {
    name: "Wikipedia Search",
    source: args.wiki,
    dumpUrl: args.jsonl ? "" : args.dumpUrl,
    docs: docsMeta.docs || 0,
    limit: args.limit || null,
    bodyChars: args.bodyChars || null,
    builtAt: new Date().toISOString()
  };
  writeFileSync(resolve(args.root, "public", "site-meta.json"), `${JSON.stringify(siteMeta, null, 2)}\n`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  mkdirSync(resolve(args.root, "public"), { recursive: true });
  syncRuntimeBundle(args);
  const docsPath = await writeDocs(args);
  if (args.embed) {
    const { embedJsonl } = await import("./embed.mjs");
    await embedJsonl({ input: docsPath, output: docsPath });
  }
  const configPath = writeConfig(args, docsPath);
  await build({ configPath });
  writeSiteMeta(args);
  console.log(`Built ${args.wiki} search site in ${args.root}`);
  console.log(`Serve with: node scripts/serve.mjs ${resolve(args.root, "public")} 5182`);
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
