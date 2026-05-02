import { resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { writeDirectoryFiles } from "./directory_writer.js";
import { createPackWriter, finalizePackWriter, writePackedShard } from "./packs.js";
import { buildQueryBundle, QUERY_BUNDLE_FORMAT } from "./query_bundle_codec.js";

function compareResultRows(left, right) {
  return right.score - left.score || left.doc - right.doc;
}

function isBetterRow(left, right) {
  return compareResultRows(left, right) < 0;
}

function isWorseRow(left, right) {
  return left.score - right.score || right.doc - left.doc;
}

function heapSwap(heap, left, right) {
  const value = heap[left];
  heap[left] = heap[right];
  heap[right] = value;
}

function heapPush(heap, row) {
  heap.push(row);
  let index = heap.length - 1;
  while (index > 0) {
    const parent = Math.floor((index - 1) / 2);
    if (isWorseRow(heap[parent], heap[index]) <= 0) break;
    heapSwap(heap, parent, index);
    index = parent;
  }
}

function heapifyDown(heap, index = 0) {
  while (true) {
    const left = index * 2 + 1;
    const right = left + 1;
    let worst = index;
    if (left < heap.length && isWorseRow(heap[left], heap[worst]) < 0) worst = left;
    if (right < heap.length && isWorseRow(heap[right], heap[worst]) < 0) worst = right;
    if (worst === index) break;
    heapSwap(heap, index, worst);
    index = worst;
  }
}

function heapReplaceRoot(heap, row) {
  heap[0] = row;
  heapifyDown(heap);
}

function addTopRow(heap, limit, row) {
  if (limit <= 0) return;
  if (heap.length < limit) {
    heapPush(heap, row);
  } else if (isBetterRow(row, heap[0])) {
    heapReplaceRoot(heap, row);
  }
}

function bundleFromHeap(seed, heap, total, maxRows) {
  const sorted = heap.slice().sort(compareResultRows);
  const rows = sorted.slice(0, maxRows);
  const next = sorted[maxRows] || null;
  return {
    key: seed.key,
    baseTerms: seed.baseTerms,
    expandedTerms: seed.expandedTerms,
    total,
    complete: total <= maxRows,
    nextScoreBound: next?.score || 0,
    nextTieDoc: next?.doc ?? null,
    rows
  };
}

export function createQueryBundleCollector(seeds, maxRows) {
  return {
    seedMap: new Map(seeds.map(seed => [seed.key, seed])),
    states: new Map(),
    maxRows: Math.max(1, Math.floor(Number(maxRows || 64))),
    keepRows: Math.max(1, Math.floor(Number(maxRows || 64))) + 1
  };
}

export function addQueryBundleRow(collector, key, doc, score) {
  if (!key || score <= 0) return;
  let state = collector.states.get(key);
  if (!state) {
    const seed = collector.seedMap.get(key);
    if (!seed) return;
    state = { seed, total: 0, heap: [] };
    collector.states.set(key, state);
  }
  state.total++;
  addTopRow(state.heap, collector.keepRows, { doc, score });
}

export function queryBundleCollectorResults(collector) {
  return [...collector.states.values()]
    .filter(state => state.total > 0)
    .map(state => bundleFromHeap(state.seed, state.heap, state.total, collector.maxRows))
    .sort((a, b) => a.key.localeCompare(b.key));
}

export function writeQueryBundleObjects(options) {
  const {
    outDir,
    config,
    bundles,
    coverage = "all-base-docs"
  } = options;
  const packWriter = createPackWriter(resolve(outDir, "bundles", "packs"), config.queryBundlePackBytes || config.packBytes);
  const entries = [];
  for (const bundle of bundles || []) {
    const buffer = buildQueryBundle(bundle);
    const entry = writePackedShard(packWriter, bundle.key, gzipSync(buffer, { level: 6 }), {
      kind: "query-bundle",
      codec: QUERY_BUNDLE_FORMAT,
      logicalLength: buffer.length
    });
    entries.push({ shard: bundle.key, ...entry });
  }
  finalizePackWriter(packWriter);
  const packIndexes = new Map(packWriter.packs.map((pack, index) => [pack.file, index]));
  const directoryEntries = entries.map(entry => ({
    ...entry,
    packIndex: packIndexes.get(entry.pack)
  }));
  const directory = writeDirectoryFiles(resolve(outDir, "bundles"), directoryEntries, config.directoryPageBytes, "bundles", { packTable: packWriter.packs });
  return {
    storage: "range-pack-v1",
    compression: "gzip-member",
    format: QUERY_BUNDLE_FORMAT,
    coverage,
    max_rows: Math.max(1, Math.floor(Number(config.queryBundleMaxRows || 64))),
    keys: entries.length,
    directory,
    packs: packWriter.packs,
    pack_bytes: packWriter.bytes,
    directory_bytes: directory.total_bytes
  };
}
