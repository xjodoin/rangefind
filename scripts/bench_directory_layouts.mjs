#!/usr/bin/env node

import { existsSync, readFileSync, statSync } from "node:fs";
import { resolve } from "node:path";
import { gunzipSync, gzipSync } from "node:zlib";
import { queryTerms } from "../src/analyzer.js";
import { pushVarint } from "../src/binary.js";
import { findDirectoryPage, parseDirectoryPage, parseDirectoryRoot } from "../src/directory.js";
import { shardKey } from "../src/shards.js";
import {
  typoDeleteKeys,
  typoMaxEditsFor
} from "../src/typo_runtime.js";

const DEFAULT_QUERIES = [
  "sante",
  "education",
  "montreal",
  "intelligence artificielle",
  "paquet",
  "diabete type 1",
  "sante publique"
];

const DEFAULT_TYPOS = [
  "elecrtified",
  "toitpotent",
  "evotutif",
  "vectrs",
  "electrophysionogical",
  "taipi",
  "moderatprs"
];

const encoder = new TextEncoder();

function parseArgs(argv) {
  const args = {
    index: "examples/basic/public/rangefind",
    queries: DEFAULT_QUERIES,
    typoTokens: DEFAULT_TYPOS,
    pageBytes: [32 * 1024, 64 * 1024, 128 * 1024],
    prefixLength: 3,
    scales: [1, 10, 100],
    json: false
  };
  for (const arg of argv) {
    if (arg === "--json") args.json = true;
    else if (arg.startsWith("--index=")) args.index = arg.slice("--index=".length);
    else if (arg.startsWith("--queries=")) args.queries = arg.slice("--queries=".length).split("|").filter(Boolean);
    else if (arg.startsWith("--typos=")) args.typoTokens = arg.slice("--typos=".length).split("|").filter(Boolean);
    else if (arg.startsWith("--page-bytes=")) args.pageBytes = arg.slice("--page-bytes=".length).split(",").map(Number).filter(Boolean);
    else if (arg.startsWith("--prefix-length=")) args.prefixLength = Number(arg.slice("--prefix-length=".length)) || args.prefixLength;
    else if (arg.startsWith("--scales=")) args.scales = arg.slice("--scales=".length).split(",").map(Number).filter(Boolean);
  }
  return args;
}

function pushUtf8(out, value) {
  const bytes = encoder.encode(String(value || ""));
  pushVarint(out, bytes.length);
  for (const byte of bytes) out.push(byte);
}

function encodeEntry(entry) {
  const out = [];
  pushUtf8(out, entry.shard);
  pushVarint(out, entry.pack);
  pushVarint(out, entry.offset);
  pushVarint(out, entry.length);
  return Uint8Array.from(out);
}

function encodeRootPage(page, id) {
  const out = [];
  pushUtf8(out, page.first);
  pushUtf8(out, page.last);
  pushVarint(out, id);
  pushVarint(out, page.entries.length);
  return Uint8Array.from(out);
}

function gzipSize(chunks) {
  return gzipSync(Buffer.concat(chunks.map(chunk => Buffer.from(chunk))), { level: 9 }).length;
}

function mb(bytes) {
  return bytes / 1024 / 1024;
}

function packNumber(pack) {
  const match = /^(\d+)/u.exec(String(pack || "0"));
  return match ? Number(match[1]) || 0 : 0;
}

function loadDirectoryEntries(indexRoot, directory) {
  if (!directory?.root || !directory?.pages) throw new Error("Rangefind directory metadata is missing root/pages.");
  const rootPath = resolve(indexRoot, directory.root);
  const rootCompressed = readFileSync(rootPath);
  const root = parseDirectoryRoot(gunzipSync(rootCompressed));
  const pageBytes = new Map();
  const entries = [];
  for (const page of root.pages) {
    const pagePath = resolve(indexRoot, `${directory.pages.replace(/\/?$/u, "/")}${page.file}`);
    const compressed = readFileSync(pagePath);
    pageBytes.set(page.file, compressed.length);
    const parsed = parseDirectoryPage(gunzipSync(compressed));
    for (const [shard, entry] of parsed) {
      entries.push({
        shard,
        pack: packNumber(entry.pack),
        offset: entry.offset,
        length: entry.length
      });
    }
  }
  return {
    root,
    rootBytes: rootCompressed.length,
    pageBytes,
    entries: entries.sort((a, b) => a.shard.localeCompare(b.shard))
  };
}

function loadTermEntries(indexRoot, manifest) {
  return loadDirectoryEntries(indexRoot, manifest.directory);
}

function currentPagedDirectory(loaded) {
  const totalPageBytes = [...loaded.pageBytes.values()].reduce((sum, bytes) => sum + bytes, 0);
  return {
    name: "current-paged-directory",
    files: loaded.root.pages.length + 1,
    rootBytes: loaded.rootBytes,
    totalBytes: loaded.rootBytes + totalPageBytes,
    smallFiles: [...loaded.pageBytes.values()].filter(bytes => bytes < 4096).length,
    p50PageBytes: percentile([...loaded.pageBytes.values()], 0.5),
    locate(shards) {
      const files = new Map([["root", loaded.rootBytes]]);
      for (const shard of shards) {
        const page = findDirectoryPage(loaded.root, shard);
        if (page) files.set(`page:${page.file}`, loaded.pageBytes.get(page.file) || 0);
      }
      return {
        requests: files.size,
        bytes: [...files.values()].reduce((sum, bytes) => sum + bytes, 0),
        keys: [...files.entries()].map(([key, bytes]) => ({ key, bytes }))
      };
    }
  };
}

function loadTypoEntries(indexRoot, rootManifest) {
  if (!rootManifest.typo?.manifest) return null;
  const manifestPath = resolve(indexRoot, rootManifest.typo.manifest);
  if (!existsSync(manifestPath)) return null;
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  return { manifest, manifestBytes: statSync(manifestPath).size, ...loadDirectoryEntries(indexRoot, manifest.directory) };
}

function scaledEntries(entries, scale) {
  if (scale === 1) return entries;
  const out = [];
  for (let copy = 0; copy < scale; copy++) {
    const suffix = copy ? `~${copy.toString(36).padStart(3, "0")}` : "";
    for (const entry of entries) out.push({ ...entry, shard: `${entry.shard}${suffix}` });
  }
  out.sort((a, b) => a.shard.localeCompare(b.shard));
  return out;
}

function globalNamed(entries) {
  return {
    name: "global-named",
    files: 1,
    rootBytes: 0,
    totalBytes: gzipSize(entries.map(encodeEntry)),
    locate(shards) {
      return { requests: 1, bytes: this.totalBytes, keys: [{ key: "global", bytes: this.totalBytes }] };
    }
  };
}

function prefixFiles(entries, prefixLength) {
  const groups = new Map();
  for (const entry of entries) {
    const key = entry.shard.slice(0, prefixLength).padEnd(prefixLength, "_");
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(entry);
  }
  const pages = new Map();
  let totalBytes = 0;
  for (const [key, group] of groups) {
    const bytes = gzipSize(group.map(encodeEntry));
    pages.set(key, bytes);
    totalBytes += bytes;
  }
  return {
    name: `prefix-${prefixLength}`,
    files: pages.size,
    rootBytes: 0,
    totalBytes,
    smallFiles: [...pages.values()].filter(bytes => bytes < 4096).length,
    p50PageBytes: percentile([...pages.values()], 0.5),
    locate(shards) {
      const keys = new Set([...shards].map(shard => shard.slice(0, prefixLength).padEnd(prefixLength, "_")));
      let bytes = 0;
      const fetched = [];
      for (const key of keys) {
        const size = pages.get(key) || 0;
        bytes += size;
        fetched.push({ key: `prefix:${key}`, bytes: size });
      }
      return { requests: keys.size, bytes, keys: fetched };
    }
  };
}

function paged(entries, targetBytes) {
  const pages = [];
  let current = [];
  let rawBytes = 0;
  for (const entry of entries) {
    const encoded = encodeEntry(entry);
    if (current.length && rawBytes + encoded.length > targetBytes) {
      pages.push({ entries: current, first: current[0].shard, last: current[current.length - 1].shard });
      current = [];
      rawBytes = 0;
    }
    current.push({ ...entry, encoded });
    rawBytes += encoded.length;
  }
  if (current.length) pages.push({ entries: current, first: current[0].shard, last: current[current.length - 1].shard });

  const pageBytes = pages.map(page => gzipSize(page.entries.map(entry => entry.encoded)));
  const rootBytes = gzipSize(pages.map(encodeRootPage));
  const totalBytes = rootBytes + pageBytes.reduce((sum, bytes) => sum + bytes, 0);

  function pageFor(shard) {
    let lo = 0;
    let hi = pages.length - 1;
    while (lo <= hi) {
      const mid = Math.floor((lo + hi) / 2);
      const page = pages[mid];
      if (shard < page.first) hi = mid - 1;
      else if (shard > page.last) lo = mid + 1;
      else return mid;
    }
    return Math.max(0, Math.min(pages.length - 1, lo));
  }

  return {
    name: `paged-${Math.round(targetBytes / 1024)}k`,
    files: pages.length + 1,
    rootBytes,
    totalBytes,
    smallFiles: pageBytes.filter(bytes => bytes < 4096).length,
    p50PageBytes: percentile(pageBytes, 0.5),
    locate(shards) {
      const ids = new Set([...shards].map(pageFor));
      let bytes = rootBytes;
      const fetched = [{ key: "root", bytes: rootBytes }];
      for (const id of ids) {
        const size = pageBytes[id] || 0;
        bytes += size;
        fetched.push({ key: `page:${id}`, bytes: size });
      }
      return { requests: ids.size + 1, bytes, keys: fetched };
    }
  };
}

function percentile(values, q) {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * q)))];
}

function average(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function sequenceCost(layout, shardSets) {
  const seen = new Set();
  const fetched = new Set();
  let requests = 0;
  let bytes = 0;
  for (const shards of shardSets) {
    const located = layout.locate(shards);
    for (const item of located.keys || [{ key: `${layout.name}:${requests}`, bytes: located.bytes }]) {
      if (fetched.has(item.key)) continue;
      fetched.add(item.key);
      requests++;
      bytes += item.bytes;
    }
    for (const shard of shards) seen.add(shard);
  }
  return { requests, bytes, uniqueShards: seen.size };
}

function summarizeLayout(layout, shardSets) {
  const cold = shardSets.map(shards => layout.locate(shards));
  const sequence = sequenceCost(layout, shardSets);
  return {
    name: layout.name,
    files: layout.files,
    rootKB: layout.rootBytes / 1024,
    totalKB: layout.totalBytes / 1024,
    smallFiles: layout.smallFiles || 0,
    p50PageKB: (layout.p50PageBytes || 0) / 1024,
    coldAvgRequests: average(cold.map(item => item.requests)),
    coldAvgKB: average(cold.map(item => item.bytes)) / 1024,
    sequenceRequests: sequence.requests,
    sequenceKB: sequence.bytes / 1024
  };
}

function resolvedShard(value, depths, available) {
  for (let depth = depths.max; depth >= depths.base; depth--) {
    const candidate = shardKey(value, depth);
    if (available.has(candidate)) return candidate;
  }
  return null;
}

function termShardSets(queries, manifest, entries) {
  const available = new Set(entries.map(entry => entry.shard));
  const depths = {
    base: manifest.stats?.base_shard_depth || 3,
    max: manifest.stats?.max_shard_depth || manifest.stats?.base_shard_depth || 5
  };
  return queries.map(q => new Set(queryTerms(q).map(term => resolvedShard(term, depths, available)).filter(Boolean)));
}

function typoShardSets(tokens, manifest, entries) {
  const available = new Set(entries.map(entry => entry.shard));
  const depths = {
    base: manifest.base_shard_depth || 2,
    max: manifest.max_shard_depth || manifest.base_shard_depth || 3
  };
  return tokens.map((token) => {
    const maxEdits = typoMaxEditsFor(token, { maxEdits: manifest.max_edits || 2 });
    const keys = typoDeleteKeys(token, {
      minTermLength: manifest.min_term_length || 5,
      maxEdits: manifest.max_edits || 2
    }, maxEdits);
    return new Set([...keys].map(key => resolvedShard(key, depths, available)).filter(Boolean));
  });
}

function printTable(title, rows) {
  console.log(`\n## ${title}\n`);
  console.log("| Layout | Files | Root KB | Total KB | Small files | P50 page KB | Cold req | Cold KB | Sequence req | Sequence KB |");
  console.log("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
  for (const row of rows) {
    console.log(`| ${row.name} | ${row.files.toLocaleString("en-US")} | ${row.rootKB.toFixed(1)} | ${row.totalKB.toFixed(1)} | ${row.smallFiles.toLocaleString("en-US")} | ${row.p50PageKB.toFixed(1)} | ${row.coldAvgRequests.toFixed(1)} | ${row.coldAvgKB.toFixed(1)} | ${row.sequenceRequests.toLocaleString("en-US")} | ${row.sequenceKB.toFixed(1)} |`);
  }
}

function benchmark(entries, shardSets, args, current = {}) {
  const rowsByScale = {};
  for (const scale of args.scales) {
    const scaled = scaledEntries(entries, scale);
    const layouts = [
      globalNamed(scaled),
      prefixFiles(scaled, args.prefixLength),
      ...args.pageBytes.map(bytes => paged(scaled, bytes))
    ];
    rowsByScale[scale] = layouts.map(layout => summarizeLayout(layout, shardSets));
    if (scale === 1 && current.layout) rowsByScale[scale].unshift(summarizeLayout(current.layout, shardSets));
  }
  return rowsByScale;
}

const args = parseArgs(process.argv.slice(2));
const indexRoot = resolve(args.index);
const manifest = JSON.parse(readFileSync(resolve(indexRoot, "manifest.json"), "utf8"));
const terms = loadTermEntries(indexRoot, manifest);
const termSets = termShardSets(args.queries, manifest, terms.entries);
const typo = loadTypoEntries(indexRoot, manifest);
const typoSets = typo ? typoShardSets(args.typoTokens, typo.manifest, typo.entries) : [];

const report = {
  index: indexRoot,
  termShards: terms.entries.length,
  typoShards: typo?.entries.length || 0,
  queries: args.queries,
  typoTokens: args.typoTokens,
  terms: benchmark(terms.entries, termSets, args, { layout: currentPagedDirectory(terms) }),
  typo: typo ? benchmark(typo.entries, typoSets, args, { layout: currentPagedDirectory(typo) }) : null
};

if (args.json) {
  console.log(JSON.stringify(report, null, 2));
} else {
  console.log(`# Directory layout benchmark\n\nIndex: ${indexRoot}`);
  console.log(`Term shards: ${terms.entries.length.toLocaleString("en-US")}`);
  if (typo) console.log(`Typo shards: ${typo.entries.length.toLocaleString("en-US")}`);
  for (const [scale, rows] of Object.entries(report.terms)) printTable(`Term directory, ${scale}x shards`, rows);
  if (report.typo) for (const [scale, rows] of Object.entries(report.typo)) printTable(`Typo directory, ${scale}x shards`, rows);
  console.log("\nInterpretation: `global-named` is a fair global-directory replacement once shard names leave manifest.json. `prefix-N` is the naive many-small-files design. `paged-*` keeps file count bounded by target page size while only loading root plus touched pages.");
}
