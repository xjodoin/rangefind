// Link-graph authority on Wikipedia using REAL [[wikilinks]].
//
//   node scripts/wiki_linkrank_real.mjs [--wiki=frwiki] [--limit=20000] [--top=25]
//
// Unlike scripts/wiki_linkrank_bench.mjs (which approximates edges by title
// mentions over the link-stripped fixture), this streams a bounded prefix of a
// real Wikimedia dump, extracts the actual [[Target]] / [[Target|label]] links
// before the wikitext is flattened, builds the graph, and runs the same
// PageRank the builder ships. The parsed prefix (title, url, out-links) is
// cached so reruns do not re-download.
//
// Bounded by --limit: the download stops after N article-namespace pages, so
// only a prefix of the dump is transferred.

import { spawn } from "node:child_process";
import { createReadStream, createWriteStream, existsSync, mkdirSync, renameSync } from "node:fs";
import { createInterface } from "node:readline";
import { once } from "node:events";
import { resolve } from "node:path";
import { pageRank, normalizeRanks } from "../src/link_graph.js";

function argStr(name, fallback) {
  const hit = process.argv.find(a => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
}
function argNum(name, fallback) {
  const v = argStr(name, null);
  return v == null ? fallback : Number(v);
}

const WIKI = argStr("wiki", "frwiki");
const LIMIT = argNum("limit", 20000);
const TOP = argNum("top", 25);
const DUMP_URL = argStr("dump-url", `https://dumps.wikimedia.org/${WIKI}/latest/${WIKI}-latest-pages-articles.xml.bz2`);
const CACHE = resolve(`examples/frwiki/data/${WIKI}-links-${LIMIT}.jsonl`);
const ms = ns => Number(ns) / 1e6;

// Namespaces whose [[prefix:...]] links are not article edges.
const NON_ARTICLE = /^(File|Fichier|Image|Media|Média|Category|Cat[ée]gorie|Help|Aide|Template|Mod[èe]le|Wikipedia|Wikip[ée]dia|Portal|Portail|Project|Projet|User|Utilisateur|Special|Sp[ée]cial|Talk|Discussion|MediaWiki|Module)\s*:/iu;

function normTitle(raw) {
  let t = String(raw || "").replace(/_/gu, " ").trim();
  if (!t) return "";
  // Wikipedia capitalizes the first letter of every article title.
  return t.charAt(0).toUpperCase() + t.slice(1);
}

function extractLinks(wikitext) {
  const out = [];
  const seen = new Set();
  const re = /\[\[([^\]]+?)\]\]/gu;
  let m;
  while ((m = re.exec(wikitext))) {
    let target = m[1];
    const pipe = target.indexOf("|");
    if (pipe >= 0) target = target.slice(0, pipe);
    const hash = target.indexOf("#");
    if (hash >= 0) target = target.slice(0, hash);
    target = target.trim();
    if (!target || target.startsWith(":") || NON_ARTICLE.test(target)) continue;
    const title = normTitle(target);
    if (title && !seen.has(title)) { seen.add(title); out.push(title); }
  }
  return out;
}

function decodeXml(v) {
  return String(v || "").replace(/&lt;/gu, "<").replace(/&gt;/gu, ">").replace(/&amp;/gu, "&").replace(/&quot;/gu, "\"").replace(/&#039;|&apos;/gu, "'");
}
function tag(page, name) {
  const m = new RegExp(`<${name}(?:\\s[^>]*)?>([\\s\\S]*?)</${name}>`, "u").exec(page);
  return decodeXml(m?.[1] || "");
}

async function downloadPrefix() {
  mkdirSync(resolve("examples/frwiki/data"), { recursive: true });
  const tmp = `${CACHE}.tmp`;
  const curl = spawn("curl", ["-L", "--fail", "--silent", "--show-error", "--user-agent", "rangefind-linkrank/0.1", DUMP_URL], { stdio: ["ignore", "pipe", "inherit"] });
  const bz = spawn("bzip2", ["-dc"], { stdio: ["pipe", "pipe", "inherit"] });
  curl.stdout.pipe(bz.stdin);
  curl.stdout.on("error", () => {});
  bz.stdin.on("error", () => {});
  const out = createWriteStream(tmp);
  const input = bz.stdout;
  input.setEncoding("utf8");
  let buffer = "";
  let docs = 0;
  let pages = 0;
  const started = process.hrtime.bigint();
  for await (const chunk of input) {
    buffer += chunk;
    while (true) {
      const s = buffer.indexOf("<page>");
      const e = buffer.indexOf("</page>");
      if (s < 0 || e < s) { if (s > 0) buffer = buffer.slice(s); break; }
      const page = buffer.slice(s, e + 7);
      buffer = buffer.slice(e + 7);
      pages++;
      const ns = tag(page, "ns");
      if (ns && ns !== "0") continue;
      const title = tag(page, "title").trim();
      if (!title || /<redirect\b/iu.test(page)) continue;
      const raw = tag(page, "text");
      if (raw.length < 200) continue;
      const links = extractLinks(raw);
      const rec = { title: normTitle(title), links };
      if (!out.write(`${JSON.stringify(rec)}\n`)) await once(out, "drain");
      docs++;
      if (docs % 5000 === 0) {
        const secs = ms(process.hrtime.bigint() - started) / 1000;
        console.error(`  downloaded ${docs} articles from ${pages} pages (${(docs / secs).toFixed(0)}/s)`);
      }
      if (docs >= LIMIT) {
        input.destroy();
        curl.kill("SIGTERM");
        bz.kill("SIGTERM");
        break;
      }
    }
    if (docs >= LIMIT) break;
  }
  await new Promise((res, rej) => out.end(err => err ? rej(err) : res()));
  renameSync(tmp, CACHE);
  return docs;
}

async function* records() {
  const rl = createInterface({ input: createReadStream(CACHE), crlfDelay: Infinity });
  for await (const line of rl) {
    if (line) { try { yield JSON.parse(line); } catch { /* skip */ } }
  }
}

async function main() {
  console.log(`Real-wikilink linkRank — ${WIKI}, limit ${LIMIT}`);
  if (!existsSync(CACHE)) {
    console.log(`  streaming ${DUMP_URL} ...`);
    const dl = process.hrtime.bigint();
    const n = await downloadPrefix();
    console.log(`  parsed ${n} articles with links in ${(ms(process.hrtime.bigint() - dl) / 1000).toFixed(1)}s -> ${CACHE}`);
  } else {
    console.log(`  using cached ${CACHE}`);
  }

  const titles = [];
  const titleToOrdinal = new Map();
  for await (const rec of records()) {
    if (!titleToOrdinal.has(rec.title)) { titleToOrdinal.set(rec.title, titles.length); titles.push(rec.title); }
  }
  const n = titles.length;

  const adjacency = new Array(n);
  for (let i = 0; i < n; i++) adjacency[i] = [];
  let edges = 0;
  let resolved = 0;
  let rawLinks = 0;
  let ordinal = 0;
  for await (const rec of records()) {
    const self = titleToOrdinal.get(rec.title);
    const row = adjacency[self];
    const seen = new Set();
    for (const target of rec.links || []) {
      rawLinks++;
      const t = titleToOrdinal.get(target);
      if (t === undefined || t === self || seen.has(t)) continue;
      seen.add(t);
      row.push(t);
      resolved++;
    }
    edges += row.length;
    ordinal++;
  }
  console.log(`  ${n} articles, ${rawLinks} raw links, ${edges} in-set edges (avg out-degree ${(edges / n).toFixed(1)}, ${((resolved / Math.max(1, rawLinks)) * 100).toFixed(0)}% resolved in-set)`);

  const prStart = process.hrtime.bigint();
  const linkRank = normalizeRanks(pageRank(adjacency));
  console.log(`  PageRank: ${ms(process.hrtime.bigint() - prStart).toFixed(0)}ms\n`);

  const order = Array.from({ length: n }, (_, i) => i).sort((a, b) => linkRank[b] - linkRank[a]);
  console.log(`Top ${TOP} articles by real-link authority:`);
  for (let r = 0; r < Math.min(TOP, n); r++) {
    const i = order[r];
    console.log(`  ${String(r + 1).padStart(3)}. ${linkRank[i].toFixed(4)}  ${titles[i]}`);
  }
}

main().catch(err => { console.error(err); process.exitCode = 1; });
