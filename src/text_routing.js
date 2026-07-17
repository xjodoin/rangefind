import { closeSync, createReadStream, mkdirSync, openSync, readFileSync, readSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { StringDecoder } from "node:string_decoder";
import { createGunzip, gunzipSync, gzipSync } from "node:zlib";
import { parsePostingSegment } from "./codec.js";
import { parseDirectoryPage, parseDirectoryRoot } from "./directory.js";
import { writeDirectoryFilesFromSortedEntries } from "./directory_writer.js";
import { createAppendOnlyPackWriter, finalizePackWriter, writePackedShard } from "./packs.js";
import { encodeTextRoutingSegment, TEXT_ROUTING_FORMAT, TEXT_ROUTING_VERSION } from "./text_routing_codec.js";

export { encodeTextRoutingSegment, parseTextRoutingSegment, TEXT_ROUTING_FORMAT } from "./text_routing_codec.js";

// Text routing index for sharded roots: a root-level directory mapping every
// analyzed term to the set of shards whose postings contain it. Queries with
// text terms route to the shards that can actually satisfy minShouldMatch
// instead of fanning out to the whole fleet — the difference between opening
// 2 shards and opening 300 on a planet-scale index.
//
// Storage reuses the proven range machinery: term segments (front-coded
// sorted terms + delta-encoded shard ordinal sets, one gzip member each)
// packed into `text-routing/packs/`, addressed by an rfdir-v2 directory whose
// keys are each segment's first term (floor lookup).

const DEFAULT_SEGMENT_TERMS = 4096;
const DEFAULT_PACK_TARGET_BYTES = 8 * 1024 * 1024;

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

// Yields every term of one built generation in code-unit order. Directory
// pages and segment blobs are written sorted, so sequential iteration is a
// globally sorted stream (see directory_writer.js / codec.js ordering).
function* generationTerms(genDir, genManifest) {
  const directory = genManifest.directory;
  if (!directory?.root) throw new Error(`Rangefind text routing: no term directory in ${genDir}.`);
  const root = parseDirectoryRoot(gunzipSync(readFileSync(resolve(genDir, directory.root))));
  const pagesDir = String(directory.pages || "terms/directory-pages/").replace(/\/?$/u, "/");
  const fds = new Map();
  try {
    for (const page of root.pages) {
      const pageBuffer = gunzipSync(readFileSync(resolve(genDir, `${pagesDir}${page.file}`)));
      const entries = parseDirectoryPage(pageBuffer, { packTable: directory.pack_table || [] });
      for (const entry of entries.values()) {
        let fd = fds.get(entry.pack);
        if (fd === undefined) {
          fd = openSync(resolve(genDir, "terms", "packs", entry.pack), "r");
          fds.set(entry.pack, fd);
        }
        const compressed = Buffer.allocUnsafe(entry.length);
        let read = 0;
        while (read < entry.length) {
          const got = readSync(fd, compressed, read, entry.length - read, entry.offset + read);
          if (!got) throw new Error(`Rangefind text routing: short read in ${entry.pack}.`);
          read += got;
        }
        // Only the term keys matter here; parsePostingSegment needs the
        // generation manifest because per-term block-filter summaries are
        // sized by manifest.block_filters.
        yield* parsePostingSegment(gunzipSync(compressed), genManifest).terms.keys();
      }
    }
  } finally {
    for (const fd of fds.values()) closeSync(fd);
  }
}

// Sorted unique terms of a built index directory (single or generational).
function* indexTerms(dir) {
  const manifest = manifestAt(dir, "manifest.min.json");
  const generations = generationsOf(dir, manifest);
  if (generations.length === 1) {
    yield* generationTerms(generations[0].dir, generations[0].manifest);
    return;
  }
  yield* mergeSortedStreams(
    generations.map(generation => generationTerms(generation.dir, generation.manifest)),
    () => {}
  );
}

// K-way merge over sorted string streams; emits each unique value once and
// reports which streams contained it via onSources(value, sourceIndexes).
function* mergeSortedStreams(streams, onSources) {
  const heads = streams.map((stream, index) => ({ stream, index, value: null, done: false }));
  for (const head of heads) advance(head);
  function advance(head) {
    while (true) {
      const next = head.stream.next();
      if (next.done) {
        head.done = true;
        head.value = null;
        return;
      }
      // Generational streams can repeat a term (same term in consecutive
      // segments is impossible, but guard against equal neighbors anyway).
      if (next.value !== head.value) {
        head.value = next.value;
        return;
      }
    }
  }
  const sources = [];
  while (true) {
    let min = null;
    for (const head of heads) {
      if (!head.done && (min === null || head.value < min)) min = head.value;
    }
    if (min === null) return;
    sources.length = 0;
    for (const head of heads) {
      if (!head.done && head.value === min) {
        sources.push(head.index);
        advance(head);
      }
    }
    onSources(min, sources);
    yield min;
  }
}

// Async counterpart used by routing sidecars. Unlike readFile + gunzip +
// split, this keeps only one current value (plus bounded stream buffers) per
// shard, regardless of the total vocabulary size. A min-heap selects the next
// term in O(log shard count), avoiding a full shard scan for every term.
async function* mergeSortedAsyncStreams(streams, onSources) {
  const heads = streams.map((stream, index) => ({
    stream: stream[Symbol.asyncIterator]?.() || stream[Symbol.iterator]?.(),
    index,
    value: null,
    done: false
  }));
  if (heads.some(head => !head.stream)) throw new TypeError("Rangefind text routing: term source is not iterable.");

  async function advance(head) {
    while (true) {
      const next = await head.stream.next();
      if (next.done) {
        head.done = true;
        head.value = null;
        return;
      }
      if (next.value !== head.value) {
        head.value = next.value;
        return;
      }
    }
  }

  const heap = [];
  const nodeByValue = new Map();
  // Recycle hot-path containers. Their high-water mark is capped by the
  // number of source streams, so the pool cannot grow with term count.
  const freeNodes = [];
  const freeHeadLists = [];
  function compare(left, right) {
    if (left.value < right.value) return -1;
    if (left.value > right.value) return 1;
    return 0;
  }
  function push(node) {
    let index = heap.length;
    heap.push(node);
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (compare(heap[parent], node) <= 0) break;
      heap[index] = heap[parent];
      index = parent;
    }
    heap[index] = node;
  }
  function pop() {
    const first = heap[0];
    const last = heap.pop();
    if (heap.length) {
      let index = 0;
      while (true) {
        const left = index * 2 + 1;
        if (left >= heap.length) break;
        const right = left + 1;
        const child = right < heap.length && compare(heap[right], heap[left]) < 0 ? right : left;
        if (compare(last, heap[child]) <= 0) break;
        heap[index] = heap[child];
        index = child;
      }
      heap[index] = last;
    }
    nodeByValue.delete(first.value);
    return first;
  }
  function add(head) {
    const existing = nodeByValue.get(head.value);
    if (existing) {
      existing.heads.push(head);
      return;
    }
    const node = freeNodes.pop() || { value: null, heads: null };
    const nodeHeads = freeHeadLists.pop() || [];
    node.value = head.value;
    node.heads = nodeHeads;
    nodeHeads.push(head);
    nodeByValue.set(node.value, node);
    push(node);
  }
  function recycleNode(node) {
    node.value = null;
    node.heads = null;
    freeNodes.push(node);
  }
  function recycleHeadList(nodeHeads) {
    nodeHeads.length = 0;
    freeHeadLists.push(nodeHeads);
  }

  const sources = [];
  try {
    for (const head of heads) {
      await advance(head);
      if (!head.done) add(head);
    }
    while (heap.length) {
      const node = pop();
      const min = node.value;
      const matchingHeads = node.heads;
      matchingHeads.sort((left, right) => left.index - right.index);
      sources.length = 0;
      for (const head of matchingHeads) {
        sources.push(head.index);
      }
      for (const head of matchingHeads) await advance(head);
      recycleNode(node);
      for (const head of matchingHeads) {
        if (!head.done) add(head);
      }
      recycleHeadList(matchingHeads);
      onSources(min, sources);
      yield min;
    }
  } finally {
    await Promise.allSettled(heads.map(head => head.done ? undefined : head.stream.return?.()));
  }
}

function analysisOf(dir) {
  const manifest = manifestAt(dir, "manifest.min.json");
  if (Array.isArray(manifest.shards)) {
    throw new Error("Rangefind text routing: nested sharded roots are not supported; build routing from leaf shards.");
  }
  const generations = generationsOf(dir, manifest);
  return generations[0].manifest.analysis || null;
}

// Prefix routing is safe for autocomplete only when every suggest source is
// also an always-indexed text field. Otherwise a suggestion can exist in the
// authority lexicon without a corresponding posting in the term directory,
// and routing by term prefix could incorrectly exclude its shard.
function manifestSupportsSuggestPrefixRouting(manifest) {
  const suggestFields = manifest.authority?.autocomplete?.fields || [];
  if (!suggestFields.length) return false;
  const alwaysIndexed = new Set(manifest.stats?.always_index_fields || []);
  const textFields = manifest.fields || [];
  return suggestFields.every(suggest => textFields.some(field => (
    field.path
    && field.path === suggest.path
    && alwaysIndexed.has(field.name)
  )));
}

function suggestPrefixRoutingOf(dir) {
  const manifest = manifestAt(dir, "manifest.min.json");
  if (Array.isArray(manifest.shards)) return false;
  const generations = generationsOf(dir, manifest);
  return generations.length > 0
    && generations.every(generation => manifestSupportsSuggestPrefixRouting(generation.manifest));
}

// Term-set sidecar ("rftermset-v1"): one shard's sorted unique terms plus its
// analysis profile, as a gzipped newline stream with a JSON header line.
// Pipelines that reclaim shard artifacts after upload persist this small file
// instead, so the routing merge never needs the full shard on disk.
const TERM_SET_FORMAT = "rftermset-v1";

export function writeShardTermSet({ dir, outFile }) {
  const analysis = analysisOf(dir);
  const suggestPrefix = suggestPrefixRoutingOf(dir);
  const lines = [JSON.stringify({ format: TERM_SET_FORMAT, analysis, suggest_prefix: suggestPrefix })];
  let terms = 0;
  for (const term of indexTerms(dir)) {
    lines.push(term);
    terms++;
  }
  lines.push("");
  mkdirSync(dirname(resolve(outFile)), { recursive: true });
  writeFileSync(resolve(outFile), gzipSync(Buffer.from(lines.join("\n"), "utf8"), { level: 9 }));
  return { terms, analysis, suggestPrefix };
}

function parseTermSetHeader(line, file) {
  if (line === undefined) throw new Error(`Invalid empty Rangefind term set ${file}.`);
  const header = JSON.parse(line);
  if (header.format !== TERM_SET_FORMAT) throw new Error(`Unsupported Rangefind term set ${file} (${header.format}).`);
  return { analysis: header.analysis || null, suggestPrefix: header.suggest_prefix === true };
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
        yield line.endsWith("\r") ? line.slice(0, -1) : line;
      }
    }
    pending += decoder.end();
    if (pending) yield pending.endsWith("\r") ? pending.slice(0, -1) : pending;
  } finally {
    input.off("error", forwardInputError);
    gunzip.destroy();
    input.destroy();
  }
}

async function readTermSetHeader(file) {
  const lines = gzipLines(file);
  try {
    const first = await lines.next();
    return parseTermSetHeader(first.done ? undefined : first.value, file);
  } finally {
    await lines.return();
  }
}

async function* termSetTerms(file) {
  let first = true;
  let trailingEmptyLines = 0;
  for await (const line of gzipLines(file)) {
    if (first) {
      parseTermSetHeader(line, file);
      first = false;
      continue;
    }
    if (line === "") {
      trailingEmptyLines++;
      continue;
    }
    while (trailingEmptyLines > 0) {
      yield "";
      trailingEmptyLines--;
    }
    yield line;
  }
  if (first) parseTermSetHeader(undefined, file);
}

// Builds the routing directory for a sharded root. `shards` mirrors the
// writeShardedRootManifest input: [{ id, dir }] where dir holds the built
// shard index, or [{ id, termSet }] pointing at a writeShardTermSet sidecar.
// Returns the manifest block to pass as `textRouting`.
export async function writeTextRoutingIndex({ outDir, shards, segmentTerms = DEFAULT_SEGMENT_TERMS, packTargetBytes = DEFAULT_PACK_TARGET_BYTES, directoryPageBytes }) {
  if (!Array.isArray(shards) || !shards.length) throw new Error("Rangefind text routing: no shards.");
  const shardIds = shards.map((shard, index) => String(shard.id || `shard-${index}`));
  const sources = [];
  for (const shard of shards) {
    if (shard.termSet) {
      const parsed = await readTermSetHeader(shard.termSet);
      sources.push({ analysis: parsed.analysis, suggestPrefix: parsed.suggestPrefix, stream: () => termSetTerms(shard.termSet) });
      continue;
    }
    sources.push({
      analysis: analysisOf(shard.dir),
      suggestPrefix: suggestPrefixRoutingOf(shard.dir),
      stream: () => indexTerms(shard.dir)
    });
  }
  const analysis = sources[0].analysis;
  const analysisKey = JSON.stringify(analysis);
  for (let index = 1; index < sources.length; index++) {
    if (JSON.stringify(sources[index].analysis) !== analysisKey) {
      throw new Error(`Rangefind text routing: shard ${shards[index].id} uses a different analysis profile; routing requires a uniform analyzer.`);
    }
  }

  const routingDir = resolve(outDir, "text-routing");
  const packWriter = createAppendOnlyPackWriter(resolve(routingDir, "packs"), packTargetBytes);
  const segments = [];
  let termCount = 0;
  const batch = [];
  // At most `segmentTerms` entries are retained and reused after each flush.
  const freeBatchEntries = [];

  function flushSegment() {
    if (!batch.length) return;
    const firstTerm = batch[0].term;
    const encoded = encodeTextRoutingSegment(batch);
    const compressed = gzipSync(encoded, { level: 9 });
    const entry = writePackedShard(packWriter, firstTerm, compressed, { logicalLength: encoded.length });
    segments.push({ key: firstTerm, entry });
    for (const batchEntry of batch) {
      batchEntry.term = null;
      batchEntry.shards.length = 0;
      freeBatchEntries.push(batchEntry);
    }
    batch.length = 0;
  }

  let currentShards = null;
  const streamRecorder = (term, sources) => {
    currentShards = sources;
  };
  const merged = mergeSortedAsyncStreams(sources.map(source => source.stream()), streamRecorder);
  for await (const term of merged) {
    const batchEntry = freeBatchEntries.pop() || { term: null, shards: [] };
    batchEntry.term = term;
    for (const shard of currentShards) batchEntry.shards.push(shard);
    batch.push(batchEntry);
    termCount++;
    if (batch.length >= segmentTerms) flushSegment();
  }
  flushSegment();
  freeBatchEntries.length = 0;
  finalizePackWriter(packWriter);

  const packTable = packWriter.packs.map(pack => pack.file);
  const packIndexByFile = new Map(packTable.map((file, index) => [file, index]));
  const directoryEntries = segments.map(segment => {
    const pack = packWriter.packNameMap?.get(segment.entry.pack) || segment.entry.pack;
    return {
      shard: segment.key,
      packIndex: packIndexByFile.get(pack),
      offset: segment.entry.offset,
      length: segment.entry.length,
      logicalLength: segment.entry.logicalLength || 0,
      checksum: segment.entry.checksum
    };
  });
  const directory = await writeDirectoryFilesFromSortedEntries(
    routingDir,
    directoryEntries,
    directoryEntries.length,
    directoryPageBytes,
    "text-routing",
    { packTable }
  );

  return {
    format: TEXT_ROUTING_FORMAT,
    version: TEXT_ROUTING_VERSION,
    term_count: termCount,
    shard_ids: shardIds,
    suggest_prefix: sources.every(source => source.suggestPrefix),
    ...(analysis ? { analysis } : {}),
    directory
  };
}
