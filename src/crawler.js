// Static-site crawler and config inference for `rangefind build <dir>`.
//
// Rangefind's builder consumes a JSONL corpus plus a JSON config (see
// src/builder.js / src/config.js), and it re-reads `config.input` across
// passes. Rather than reinvent any of that, the crawler walks a built static
// site into a real JSONL file on disk plus an inferred config that points at
// it, then hands off to the unchanged `build()`.

import { mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, relative, resolve, sep } from "node:path";
import { build } from "./builder.js";
import { DEFAULT_ANALYSIS_LANGUAGES } from "./analysis.js";
import { extractHtml } from "./html_extract.js";

const HTML_RE = /\.html?$/i;

// Recursively list crawlable HTML files under `dir`. Dotfiles/dot-dirs,
// node_modules, and an optional `skipPath` (e.g. a nested output dir) are
// pruned. The list is sorted so ids and any collision fallbacks are stable.
async function listHtmlFiles(dir, skipPath) {
  const found = [];
  async function walk(current) {
    const entries = await readdir(current, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.name.startsWith(".")) continue;
      const full = join(current, entry.name);
      if (skipPath && (full === skipPath || full.startsWith(skipPath + sep))) continue;
      if (entry.isDirectory()) {
        if (entry.name === "node_modules") continue;
        await walk(full);
      } else if (entry.isFile() && HTML_RE.test(entry.name)) {
        found.push(full);
      }
    }
  }
  await walk(dir);
  found.sort();
  return found;
}

function toPosix(path) {
  return path.split(sep).join("/");
}

// A file's site-root-relative URL path: index files collapse to their
// directory URL (trailing slash), everything else becomes a pretty,
// extension-less, trailing-slashless path.
function urlPathFor(relPosix) {
  const base = relPosix.replace(/^\/+/, "");
  const stem = base.replace(HTML_RE, "");
  if (/(^|\/)index$/.test(stem)) {
    const dir = stem.replace(/index$/, "").replace(/\/$/, "");
    return dir ? `/${dir}/` : "/";
  }
  return `/${stem}`;
}

// Join a base URL (a path like "/" or "/blog/", or an absolute origin URL)
// with a site-root-relative path without doubling slashes.
function joinUrl(baseUrl, path) {
  const prefix = String(baseUrl || "/").replace(/\/+$/, "");
  if (!prefix) return path;
  return path === "/" ? `${prefix}/` : `${prefix}${path}`;
}

const RESERVED_KEYS = new Set(["id", "url", "title", "headings", "body", "lang", "description"]);

// Decide the numeric/date/string nature of a set of raw sort values. Sorting
// needs a typed number doc-value; anything non-numeric falls back to a facet.
function classifySort(values) {
  let allInt = true;
  for (const value of values) {
    const num = Number(value);
    if (!Number.isFinite(num)) {
      const time = Date.parse(value);
      if (!Number.isFinite(time)) return { kind: "string" };
      return { kind: "date" };
    }
    if (!Number.isInteger(num)) allInt = false;
  }
  return { kind: "number", type: allInt ? "int" : "double" };
}

// Crawl a built static site into documents plus an inferred build config.
// `root` defines the id/URL namespace; `scanDir` (default `root`) is the tree
// actually walked; `skipPath` prunes a nested output directory.
export async function crawlSite({ root, scanDir, baseUrl = "/", skipPath = "" }) {
  const rootAbs = resolve(root);
  const scanAbs = resolve(scanDir || root);
  const files = await listHtmlFiles(scanAbs, skipPath ? resolve(skipPath) : "");

  const docs = [];
  const ids = new Set();
  const usedFields = new Set();
  const filterKeys = new Set();
  const sortValues = new Map();
  const langs = new Set();
  let anyDescription = false;

  for (const file of files) {
    const html = await readFile(file, "utf8");
    const extracted = extractHtml(html);
    if (extracted.skip) continue;

    const relPosix = toPosix(relative(rootAbs, file));
    const path = urlPathFor(relPosix);
    // Prefer the collapsed URL path as the id; fall back to the raw relative
    // path on collision so ids stay unique and deterministic.
    let id = path === "/" ? "index" : path.replace(/^\/+/, "").replace(/\/+$/, "");
    if (ids.has(id)) id = relPosix.replace(HTML_RE, "");
    ids.add(id);

    const doc = { id, url: joinUrl(baseUrl, path) };
    if (extracted.title) { doc.title = extracted.title; usedFields.add("title"); }
    if (extracted.headings) { doc.headings = extracted.headings; usedFields.add("headings"); }
    if (extracted.body) { doc.body = extracted.body; usedFields.add("body"); }
    if (extracted.lang) { doc.lang = extracted.lang; langs.add(extracted.lang); }
    if (extracted.description) { doc.description = extracted.description; anyDescription = true; }

    for (const [key, value] of Object.entries(extracted.meta)) {
      if (!RESERVED_KEYS.has(key) && doc[key] === undefined) doc[key] = value;
    }
    for (const [key, values] of Object.entries(extracted.filters)) {
      if (RESERVED_KEYS.has(key)) continue;
      filterKeys.add(key);
      doc[key] = values;
    }
    for (const [key, value] of Object.entries(extracted.sorts)) {
      if (RESERVED_KEYS.has(key)) continue;
      doc[key] = value;
      if (!sortValues.has(key)) sortValues.set(key, []);
      sortValues.get(key).push(value);
    }

    docs.push(doc);
  }

  const config = inferConfig({ usedFields, filterKeys, sortValues, langs, anyDescription });
  return { docs, config, files: files.length, languages: [...langs] };
}

function inferConfig({ usedFields, filterKeys, sortValues, langs, anyDescription }) {
  const fields = [];
  if (usedFields.has("title")) fields.push({ name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true });
  if (usedFields.has("headings")) fields.push({ name: "headings", path: "headings", weight: 2.0, b: 0.5 });
  if (usedFields.has("body")) fields.push({ name: "body", path: "body", weight: 1.0, b: 0.75 });
  // A crawl with no title/heading/body would index nothing; keep a body field
  // so the resulting index is always well-formed.
  if (!fields.length) fields.push({ name: "body", path: "body", weight: 1.0, b: 0.75 });

  const facets = [...filterKeys].sort().map(name => ({ name, path: name }));
  const numbers = [];
  const sorts = [];
  for (const [key, values] of [...sortValues.entries()].sort()) {
    const kind = classifySort(values);
    if (kind.kind === "number") {
      numbers.push({ name: key, path: key, type: kind.type, sortable: true });
      sorts.push({ field: key, order: "desc" });
    } else if (kind.kind === "date") {
      numbers.push({ name: key, path: key, type: "date", sortable: true });
      sorts.push({ field: key, order: "desc" });
    } else if (!facets.some(facet => facet.name === key)) {
      // Non-numeric sort values cannot be range-sorted; expose them as a facet
      // rather than dropping the signal.
      facets.push({ name: key, path: key });
    }
  }

  const display = ["title", "url"];
  if (anyDescription) display.push("description");
  display.push({ name: "excerpt", path: "body", maxChars: 300 });

  const languages = langs.size ? [...langs].sort() : DEFAULT_ANALYSIS_LANGUAGES.slice();

  return {
    idPath: "id",
    urlPath: "url",
    resumeBuild: false,
    analysis: { languages, languageField: "lang" },
    fields,
    facets,
    numbers,
    sorts,
    display
  };
}

// Crawl `root`, materialize the JSONL + config in a temp work dir, and run the
// unchanged builder against them. The work dir uses absolute input/output
// paths so config.js's relative resolution is a no-op. On success the work dir
// is removed; on failure it is preserved and its path reported for debugging.
export async function buildFromCrawl({ root, output, baseUrl = "/", scanDir }) {
  const outputAbs = resolve(output);
  const { docs, config, files, languages } = await crawlSite({
    root,
    scanDir,
    baseUrl,
    skipPath: outputAbs
  });
  if (!docs.length) throw new Error(`Rangefind found no indexable HTML under ${resolve(scanDir || root)}.`);

  const workDir = await mkdtemp(join(tmpdir(), "rangefind-crawl-"));
  const inputPath = join(workDir, "docs.jsonl");
  const configPath = join(workDir, "rangefind.config.json");
  await writeFile(inputPath, docs.map(doc => JSON.stringify(doc)).join("\n") + "\n");
  await writeFile(configPath, JSON.stringify({ ...config, input: inputPath, output: outputAbs }, null, 2));

  try {
    await build({ configPath });
  } catch (error) {
    console.error(`Rangefind: build failed; crawl work dir preserved at ${workDir}`);
    throw error;
  }
  await rm(workDir, { recursive: true, force: true });

  console.log(
    `Rangefind: crawled ${files} file(s), indexed ${docs.length} doc(s), `
    + `languages [${languages.length ? languages.join(", ") : config.analysis.languages.join(", ")}] -> ${outputAbs}`
  );
  return { files, docs: docs.length, languages, output: outputAbs };
}
