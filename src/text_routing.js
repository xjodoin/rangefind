import { closeSync, mkdirSync, openSync, readFileSync, readSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { gunzipSync, gzipSync } from "node:zlib";
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

function readTermSet(file) {
  const text = gunzipSync(readFileSync(resolve(file))).toString("utf8");
  const separator = text.indexOf("\n");
  const header = JSON.parse(separator < 0 ? text : text.slice(0, separator));
  if (header.format !== TERM_SET_FORMAT) throw new Error(`Unsupported Rangefind term set ${file} (${header.format}).`);
  const terms = separator < 0 ? [] : text.slice(separator + 1).split("\n");
  while (terms.length && terms[terms.length - 1] === "") terms.pop();
  return { analysis: header.analysis || null, suggestPrefix: header.suggest_prefix === true, terms };
}

function* arrayTerms(terms) {
  yield* terms;
}

// Builds the routing directory for a sharded root. `shards` mirrors the
// writeShardedRootManifest input: [{ id, dir }] where dir holds the built
// shard index, or [{ id, termSet }] pointing at a writeShardTermSet sidecar.
// Returns the manifest block to pass as `textRouting`.
export async function writeTextRoutingIndex({ outDir, shards, segmentTerms = DEFAULT_SEGMENT_TERMS, packTargetBytes = DEFAULT_PACK_TARGET_BYTES, directoryPageBytes }) {
  if (!Array.isArray(shards) || !shards.length) throw new Error("Rangefind text routing: no shards.");
  const shardIds = shards.map((shard, index) => String(shard.id || `shard-${index}`));
  const sources = shards.map(shard => {
    if (shard.termSet) {
      const parsed = readTermSet(shard.termSet);
      return { analysis: parsed.analysis, suggestPrefix: parsed.suggestPrefix, stream: () => arrayTerms(parsed.terms) };
    }
    return {
      analysis: analysisOf(shard.dir),
      suggestPrefix: suggestPrefixRoutingOf(shard.dir),
      stream: () => indexTerms(shard.dir)
    };
  });
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
  let batch = [];

  function flushSegment() {
    if (!batch.length) return;
    const encoded = encodeTextRoutingSegment(batch);
    const compressed = gzipSync(encoded, { level: 9 });
    const entry = writePackedShard(packWriter, batch[0].term, compressed, { logicalLength: encoded.length });
    segments.push({ key: batch[0].term, entry });
    batch = [];
  }

  let currentShards = null;
  const streamRecorder = (term, sources) => {
    currentShards = sources;
  };
  const merged = mergeSortedStreams(sources.map(source => source.stream()), streamRecorder);
  for (const term of merged) {
    batch.push({ term, shards: currentShards.slice() });
    termCount++;
    if (batch.length >= segmentTerms) flushSegment();
  }
  flushSegment();
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
