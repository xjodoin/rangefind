import {
  closeSync,
  createReadStream,
  mkdirSync,
  openSync,
  readFileSync,
  readSync,
  renameSync,
  rmSync,
  writeFileSync,
  writeSync
} from "node:fs";
import { createHash } from "node:crypto";
import { dirname, resolve } from "node:path";
import { StringDecoder } from "node:string_decoder";
import { createGunzip, gunzipSync, gzipSync } from "node:zlib";
import { parseDirectoryPage, parseDirectoryRoot } from "./directory.js";
import { writeDirectoryFilesFromSortedEntries } from "./directory_writer.js";
import { createAppendOnlyPackWriter, finalizePackWriter, writePackedShard } from "./packs.js";
import { AUTHORITY_FORMAT, SUGGEST_ROUTING_FORMAT, buildAuthorityShard, parseAuthorityShard } from "./authority_codec.js";
import {
  AUTHORITY_LEXICON_FORMAT,
  autocompleteRank,
  compareAutocomplete,
  encodeAuthorityHotList,
  encodeAuthorityLexiconRoot,
  isAutocompleteKey,
  parseAutocompleteKey,
  suggestKey
} from "./authority_lexicon.js";
import { partitionEntries, shardKey } from "./shards.js";

// Suggest routing index for sharded roots: every shard's authority
// autocomplete lexicon merged into one root-level artifact. A keystroke that
// used to fan out to hundreds of shards (each pulling its own authority
// ranges) is answered from the root in a couple of small range reads, and the
// entry rows carry federation shard ordinals so a selected suggestion still
// knows which region produced it. The artifact is a standard authority
// lexicon living at `<root>/authority/`, so the runtime consumes it with the
// single-index suggest machinery unchanged — only the row payload differs
// (codec version 3, see authority_codec.js).

export { SUGGEST_ROUTING_FORMAT } from "./authority_codec.js";
const SUGGEST_ROUTING_VERSION = 1;
const SUGGEST_SET_FORMAT = "rfsuggestset-v1";
const DEFAULT_PACK_TARGET_BYTES = 8 * 1024 * 1024;
const DEFAULT_SUGGEST_SET_CHUNK_BYTES = 8 * 1024 * 1024;
// Root partitions group at depth 4 ("s|" plus two characters) instead of the
// per-shard depth 3: a planet-scale merge holds one group in memory at a
// time, and one-letter buckets would be gigabytes.
const DEFAULT_BASE_SHARD_DEPTH = 4;
const DEFAULT_MAX_SHARD_DEPTH = 10;
const DEFAULT_TARGET_SHARD_ROWS = 4096;
const DEFAULT_HOT_LIST_SIZE = 64;

function manifestAt(dir, name) {
  return JSON.parse(readFileSync(resolve(dir, name), "utf8"));
}

function generationsOf(dir, manifest) {
  if (Array.isArray(manifest.generations)) {
    return manifest.generations.map(generation => {
      const path = String(generation.path || "");
      const manifestName = String(generation.manifest || "manifest.min.json");
      const localName = path && manifestName.startsWith(path) ? manifestName.slice(path.length) : manifestName;
      const genDir = path ? resolve(dir, path.replace(/\/$/u, "")) : dir;
      return { dir: genDir, manifest: manifestAt(genDir, localName) };
    });
  }
  return [{ dir, manifest }];
}

function autocompleteWeightedOf(manifest) {
  return (manifest.authority?.autocomplete?.fields || []).some(field => field.weightPath);
}

// Yields `{ key, weight, count }` for every autocomplete lexicon entry of one
// built generation, in code-unit key order. Authority directory pages and the
// partitions inside them are written key sorted, so sequential iteration is a
// globally sorted stream.
function* generationSuggestEntries(genDir, genManifest) {
  const directory = genManifest.authority?.directory;
  if (!directory?.root) return;
  const root = parseDirectoryRoot(gunzipSync(readFileSync(resolve(genDir, directory.root))));
  const pagesDir = String(directory.pages || "authority/directory-pages/").replace(/\/?$/u, "/");
  const fds = new Map();
  try {
    for (const page of root.pages) {
      const pageBuffer = gunzipSync(readFileSync(resolve(genDir, `${pagesDir}${page.file}`)));
      const entries = parseDirectoryPage(pageBuffer, { packTable: directory.pack_table || [] });
      for (const entry of entries.values()) {
        let fd = fds.get(entry.pack);
        if (fd === undefined) {
          fd = openSync(resolve(genDir, "authority", "packs", entry.pack), "r");
          fds.set(entry.pack, fd);
        }
        const compressed = Buffer.allocUnsafe(entry.length);
        let read = 0;
        while (read < entry.length) {
          const got = readSync(fd, compressed, read, entry.length - read, entry.offset + read);
          if (!got) throw new Error(`Rangefind suggest routing: short read in ${entry.pack}.`);
          read += got;
        }
        const parsed = parseAuthorityShard(gunzipSync(compressed));
        for (const [key, data] of parsed.entries) {
          if (!isAutocompleteKey(key)) continue;
          yield { key, weight: data.autocompleteWeight || data.total || 0, count: data.total || 1 };
        }
      }
    }
  } finally {
    for (const fd of fds.values()) closeSync(fd);
  }
}

// Sorted autocomplete entries of a built shard directory (single or
// generational). Generations can repeat a key; duplicates merge with the
// same semantics as the federated fan-out (max weight, summed count).
function* indexSuggestEntries(dir) {
  const manifest = manifestAt(dir, "manifest.min.json");
  const generations = generationsOf(dir, manifest);
  if (generations.length === 1) {
    yield* generationSuggestEntries(generations[0].dir, generations[0].manifest);
    return;
  }
  const streams = generations.map(generation => generationSuggestEntries(generation.dir, generation.manifest));
  const heads = streams.map(stream => ({ stream, item: null, done: false }));
  const advance = head => {
    const next = head.stream.next();
    if (next.done) {
      head.done = true;
      head.item = null;
    } else {
      head.item = next.value;
    }
  };
  for (const head of heads) advance(head);
  while (true) {
    let min = null;
    for (const head of heads) {
      if (!head.done && (min === null || head.item.key < min)) min = head.item.key;
    }
    if (min === null) return;
    let weight = 0;
    let count = 0;
    for (const head of heads) {
      while (!head.done && head.item.key === min) {
        weight = Math.max(weight, head.item.weight);
        count += head.item.count;
        advance(head);
      }
    }
    yield { key: min, weight, count };
  }
}

// Suggest-set sidecar ("rfsuggestset-v1"): one shard's sorted autocomplete
// lexicon as a gzipped JSONL stream — header line, then [key, weight, count]
// rows. Pipelines that reclaim shard artifacts after upload keep this small
// file so the routing merge never needs the full shard on disk.
export function writeShardSuggestSet({ dir, outFile, chunkTargetBytes = DEFAULT_SUGGEST_SET_CHUNK_BYTES }) {
  const manifest = manifestAt(resolve(dir), "manifest.min.json");
  const generations = generationsOf(resolve(dir), manifest);
  const weighted = generations.some(generation => autocompleteWeightedOf(generation.manifest));
  const target = resolve(outFile);
  const temporary = `${target}.${process.pid}.${Date.now()}.tmp`;
  const targetBytes = Math.max(1, Math.floor(Number(chunkTargetBytes) || DEFAULT_SUGGEST_SET_CHUNK_BYTES));
  let fd;
  let lines = [];
  let lineBytes = 0;
  let keys = 0;

  const flush = () => {
    if (lines.length === 0) return;
    const compressed = gzipSync(Buffer.from(lines.join(""), "utf8"), { level: 9 });
    let offset = 0;
    while (offset < compressed.length) offset += writeSync(fd, compressed, offset, compressed.length - offset);
    lines = [];
    lineBytes = 0;
  };
  const appendLine = value => {
    const line = `${value}\n`;
    lines.push(line);
    lineBytes += Buffer.byteLength(line);
    if (lineBytes >= targetBytes) flush();
  };

  mkdirSync(dirname(target), { recursive: true });
  try {
    fd = openSync(temporary, "wx");
    appendLine(JSON.stringify({ format: SUGGEST_SET_FORMAT, weighted }));
    for (const item of indexSuggestEntries(resolve(dir))) {
      appendLine(JSON.stringify([item.key, item.weight, item.count]));
      keys++;
    }
    flush();
    closeSync(fd);
    fd = undefined;
    renameSync(temporary, target);
  } catch (error) {
    if (fd !== undefined) closeSync(fd);
    rmSync(temporary, { force: true });
    throw error;
  }
  return { keys, weighted };
}

async function* gzipLines(file) {
  const input = createReadStream(resolve(file));
  const gunzip = createGunzip();
  const decoder = new StringDecoder("utf8");
  let pending = "";
  const forwardInputError = error => gunzip.destroy(error);
  input.on("error", forwardInputError);
  input.pipe(gunzip);
  try {
    for await (const chunk of gunzip) {
      pending += decoder.write(chunk);
      let separator;
      while ((separator = pending.indexOf("\n")) >= 0) {
        const line = pending.slice(0, separator);
        pending = pending.slice(separator + 1);
        yield line;
      }
    }
    pending += decoder.end();
    if (pending) yield pending;
  } finally {
    input.off("error", forwardInputError);
    gunzip.destroy();
    input.destroy();
  }
}

function parseSuggestSetHeader(line, file) {
  if (line === undefined) throw new Error(`Invalid empty Rangefind suggest set ${file}.`);
  const header = JSON.parse(line);
  if (header.format !== SUGGEST_SET_FORMAT) throw new Error(`Unsupported Rangefind suggest set ${file} (${header.format}).`);
  return { weighted: header.weighted === true };
}

async function readSuggestSetHeader(file) {
  const lines = gzipLines(file);
  try {
    const first = await lines.next();
    return parseSuggestSetHeader(first.done ? undefined : first.value, file);
  } finally {
    await lines.return();
  }
}

async function* suggestSetEntries(file) {
  let first = true;
  for await (const line of gzipLines(file)) {
    if (first) {
      parseSuggestSetHeader(line, file);
      first = false;
      continue;
    }
    if (line === "") continue;
    const [key, weight, count] = JSON.parse(line);
    yield { key: String(key), weight: Number(weight) || 0, count: Number(count) || 1 };
  }
  if (first) parseSuggestSetHeader(undefined, file);
}

// K-way heap merge over per-shard sorted `{ key, weight, count }` streams.
// Emits each unique key once with the merged rows `[ordinal, weight]`,
// summed count, and max weight — the same aggregation the federated
// suggest fan-out applies at query time.
async function* mergeShardSuggestStreams(streams) {
  const heads = streams.map((stream, index) => ({
    stream: stream[Symbol.asyncIterator]?.() || stream[Symbol.iterator]?.(),
    index,
    item: null,
    done: false
  }));
  if (heads.some(head => !head.stream)) throw new TypeError("Rangefind suggest routing: entry source is not iterable.");

  async function advance(head) {
    const next = await head.stream.next();
    if (next.done) {
      head.done = true;
      head.item = null;
    } else {
      head.item = next.value;
    }
  }

  const heap = [];
  function compare(left, right) {
    return left.item.key < right.item.key ? -1 : left.item.key > right.item.key ? 1 : left.index - right.index;
  }
  function push(head) {
    heap.push(head);
    let index = heap.length - 1;
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (compare(heap[parent], heap[index]) <= 0) break;
      const value = heap[parent];
      heap[parent] = heap[index];
      heap[index] = value;
      index = parent;
    }
  }
  function pop() {
    const first = heap[0];
    const last = heap.pop();
    if (heap.length) {
      heap[0] = last;
      let index = 0;
      while (true) {
        const left = index * 2 + 1;
        if (left >= heap.length) break;
        const right = left + 1;
        const child = right < heap.length && compare(heap[right], heap[left]) < 0 ? right : left;
        if (compare(heap[index], heap[child]) <= 0) break;
        const value = heap[index];
        heap[index] = heap[child];
        heap[child] = value;
        index = child;
      }
    }
    return first;
  }

  try {
    for (const head of heads) {
      await advance(head);
      if (!head.done) push(head);
    }
    const matched = [];
    while (heap.length) {
      const key = heap[0].item.key;
      matched.length = 0;
      while (heap.length && heap[0].item.key === key) matched.push(pop());
      const rows = [];
      let count = 0;
      for (const head of matched) {
        rows.push([head.index, Math.max(0, Math.floor(Number(head.item.weight) || 0))]);
        count += head.item.count;
        await advance(head);
      }
      for (const head of matched) {
        if (!head.done) push(head);
      }
      yield { key, rows, count };
    }
  } finally {
    await Promise.allSettled(heads.map(head => head.done ? undefined : head.stream.return?.()));
  }
}

// Builds the root suggest artifact for a sharded root. `shards` mirrors the
// writeShardedRootManifest input: [{ id, dir }] where dir holds the built
// shard index, or [{ id, suggestSet }] pointing at a writeShardSuggestSet
// sidecar. Returns the manifest block to store as `suggest_routing`.
export async function writeSuggestRoutingIndex({
  outDir,
  shards,
  baseShardDepth = DEFAULT_BASE_SHARD_DEPTH,
  maxShardDepth = DEFAULT_MAX_SHARD_DEPTH,
  targetShardRows = DEFAULT_TARGET_SHARD_ROWS,
  maxRowsPerKey = 16,
  hotListSize = DEFAULT_HOT_LIST_SIZE,
  directoryPageBytes,
  packTargetBytes = DEFAULT_PACK_TARGET_BYTES
}) {
  if (!Array.isArray(shards) || !shards.length) throw new Error("Rangefind suggest routing: no shards.");
  const shardIds = shards.map((shard, index) => String(shard.id || `shard-${index}`));
  const sources = [];
  for (const shard of shards) {
    if (shard.suggestSet) {
      const header = await readSuggestSetHeader(shard.suggestSet);
      sources.push({ weighted: header.weighted, stream: () => suggestSetEntries(shard.suggestSet) });
      continue;
    }
    const manifest = manifestAt(resolve(shard.dir), "manifest.min.json");
    if (Array.isArray(manifest.shards)) {
      throw new Error("Rangefind suggest routing: nested sharded roots are not supported; build routing from leaf shards.");
    }
    const generations = generationsOf(resolve(shard.dir), manifest);
    sources.push({
      weighted: generations.some(generation => autocompleteWeightedOf(generation.manifest)),
      stream: () => indexSuggestEntries(resolve(shard.dir))
    });
  }
  // Ranks must be comparable across the whole artifact: a mixed fleet would
  // interleave population weights with popularity counts, so weighting is
  // all-or-nothing exactly like the per-shard lexicon.
  const weighted = sources.some(source => source.weighted);

  const authorityDir = resolve(outDir, "authority");
  mkdirSync(authorityDir, { recursive: true });
  const packWriter = createAppendOnlyPackWriter(resolve(authorityDir, "packs"), packTargetBytes);
  const shardConfig = { baseShardDepth, maxShardDepth, targetShardPostings: targetShardRows };
  // Keep one directory-metadata array throughout the merge. Planet builds
  // can produce millions of physical partitions; retaining `{ shard, entry }`
  // objects and then mapping them into a second `directoryEntries` array
  // briefly doubled the live heap after the expensive stream merge had
  // already completed.
  const directoryEntries = [];
  const autocompleteShards = [];
  const hotLimit = Math.max(8, Math.floor(Number(hotListSize)));
  const hot = new Map();
  const hotSeen = new Map();
  let keyCount = 0;
  let rowCount = 0;

  // Same hot-prefix policy as the per-shard builder (authority_index.js):
  // universal one-codepoint lists plus the fixed two/three-letter Latin
  // keyspace, so first keystrokes stay constant-cost at planet scale.
  function mergeHotCandidate(item) {
    const codepoints = Array.from(item.normalized);
    const prefixes = [codepoints[0] || ""];
    for (let length = 2; length <= 3; length++) {
      const latinPrefix = codepoints.slice(0, length).join("");
      if (latinPrefix.length === length && /^[a-z]+$/u.test(latinPrefix)) prefixes.push(latinPrefix);
    }
    for (const prefix of prefixes) {
      if (!prefix) continue;
      hotSeen.set(prefix, (hotSeen.get(prefix) || 0) + 1);
      if (!hot.has(prefix)) hot.set(prefix, new Map());
      const bucket = hot.get(prefix);
      const previous = bucket.get(item.display);
      if (!previous || compareAutocomplete(item, previous) < 0) bucket.set(item.display, item);
      if (bucket.size > hotLimit * 2) {
        const kept = [...bucket.values()].sort(compareAutocomplete).slice(0, hotLimit);
        bucket.clear();
        for (const entry of kept) bucket.set(entry.display, entry);
      }
    }
  }

  function flushGroup(group) {
    if (!group.length) return;
    for (const partition of partitionEntries(group, shardConfig)) {
      const buffer = buildAuthorityShard(partition.entries, { maxRows: maxRowsPerKey, ordinalRows: true });
      const entry = writePackedShard(packWriter, partition.name, gzipSync(buffer, { level: 9 }), {
        kind: "authority-shard",
        codec: AUTHORITY_FORMAT,
        logicalLength: buffer.length
      });
      let maxRank = 0;
      for (const item of partition.entries) {
        maxRank = Math.max(maxRank, item.rank);
      }
      directoryEntries.push({
        shard: partition.name,
        pack: entry.pack,
        offset: entry.offset,
        length: entry.length,
        logicalLength: entry.logicalLength || 0,
        checksum: entry.checksum
      });
      autocompleteShards.push({ shard: partition.name, maxRank, count: partition.entries.length });
    }
    group.length = 0;
  }

  let group = [];
  let groupKey = null;
  for await (const merged of mergeShardSuggestStreams(sources.map(source => source.stream()))) {
    const parsed = parseAutocompleteKey(merged.key);
    if (!parsed) continue;
    const base = shardKey(merged.key, baseShardDepth);
    if (groupKey !== null && base !== groupKey) flushGroup(group);
    groupKey = base;
    const entry = [merged.key, merged.rows, { total: merged.count }];
    // partitionEntries and the hot merge both want the summary item; rank it
    // once here with the artifact-wide weighting mode.
    let weight = 0;
    for (const row of merged.rows) weight = Math.max(weight, row[1]);
    const item = {
      key: merged.key,
      normalized: parsed.normalized,
      display: parsed.display,
      weight: weight || merged.rows.length,
      count: merged.count,
      full: suggestKey(parsed.display) === parsed.normalized
    };
    item.rank = autocompleteRank(item, weighted);
    entry.rank = item.rank;
    group.push(entry);
    mergeHotCandidate(item);
    keyCount++;
    rowCount += merged.rows.length;
  }
  flushGroup(group);
  finalizePackWriter(packWriter);

  const packTable = packWriter.packs.map(pack => pack.file);
  const packIndexByFile = new Map(packTable.map((file, index) => [file, index]));
  for (const item of directoryEntries) {
    const pack = packWriter.packNameMap?.get(item.pack) || item.pack;
    item.packIndex = packIndexByFile.get(pack);
    delete item.pack;
  }
  const directory = await writeDirectoryFilesFromSortedEntries(
    authorityDir,
    directoryEntries,
    directoryEntries.length,
    directoryPageBytes,
    "authority",
    { packTable }
  );

  const hotLists = new Map();
  for (const [prefix, bucket] of hot) {
    if (Array.from(prefix).length > 1 && (hotSeen.get(prefix) || 0) <= hotLimit * 4) continue;
    hotLists.set(prefix, [...bucket.values()].sort(compareAutocomplete).slice(0, hotLimit));
  }
  const hotObjects = new Map();
  let hotBytes = 0;
  mkdirSync(resolve(authorityDir, "hot"), { recursive: true });
  for (const [prefix, items] of hotLists) {
    const source = encodeAuthorityHotList(items);
    const compressed = gzipSync(source, { level: 9 });
    const hash = createHash("sha256").update(compressed).digest("hex");
    const file = `authority/hot/${hash.slice(0, 24)}.bin.gz`;
    writeFileSync(resolve(outDir, file), compressed);
    hotBytes += compressed.length;
    hotObjects.set(prefix, { file, bytes: compressed.length, count: items.length });
  }
  const lexiconRoot = encodeAuthorityLexiconRoot({ shards: autocompleteShards, hot: hotObjects, weighted });
  const lexiconCompressed = gzipSync(lexiconRoot, { level: 9 });
  const lexiconHash = createHash("sha256").update(lexiconCompressed).digest("hex");
  const lexiconFile = `authority/lexicon-root.${lexiconHash.slice(0, 24)}.bin.gz`;
  writeFileSync(resolve(outDir, lexiconFile), lexiconCompressed);

  return {
    format: SUGGEST_ROUTING_FORMAT,
    version: SUGGEST_ROUTING_VERSION,
    shard_ids: shardIds,
    weighted,
    keys: keyCount,
    rows: rowCount,
    authority: {
      storage: "range-pack-v1",
      compression: "gzip-member",
      format: AUTHORITY_FORMAT,
      // No legacy authority fields: the root artifact is autocomplete-only,
      // which switches every consumer to the lexicon lane.
      fields: [],
      max_rows_per_key: Math.max(1, Math.floor(Number(maxRowsPerKey))),
      base_shard_depth: baseShardDepth,
      max_shard_depth: maxShardDepth,
      target_shard_rows: targetShardRows,
      keys: keyCount,
      rows: rowCount,
      shards: autocompleteShards.length,
      autocomplete: {
        format: AUTHORITY_LEXICON_FORMAT,
        keys: keyCount,
        rows: rowCount,
        shards: autocompleteShards.length,
        hot_prefixes: hotLists.size,
        hot_list_size: hotLimit,
        hot_bytes: hotBytes,
        directory: { file: lexiconFile, bytes: lexiconCompressed.length, logical_bytes: lexiconRoot.length, immutable: true }
      },
      directory,
      packs: packWriter.packs,
      pack_bytes: packWriter.bytes,
      directory_bytes: directory.total_bytes
    }
  };
}
