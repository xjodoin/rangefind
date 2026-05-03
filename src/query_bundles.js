import { resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { summarizeBlockFilters } from "./codec.js";
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

function bundleRowGroups(rows, rowGroupSize, filters, codes) {
  const size = Math.max(1, Math.floor(Number(rowGroupSize || 16)));
  const groups = [];
  for (let rowStart = 0; rowStart < rows.length; rowStart += size) {
    const slice = rows.slice(rowStart, rowStart + size);
    let docMin = Infinity;
    let docMax = -Infinity;
    let scoreMin = Infinity;
    let scoreMax = -Infinity;
    for (const row of slice) {
      docMin = Math.min(docMin, row.doc);
      docMax = Math.max(docMax, row.doc);
      scoreMin = Math.min(scoreMin, row.score);
      scoreMax = Math.max(scoreMax, row.score);
    }
    groups.push({
      rowStart,
      rowCount: slice.length,
      scoreMax: Number.isFinite(scoreMax) ? scoreMax : 0,
      scoreMin: Number.isFinite(scoreMin) ? scoreMin : 0,
      docMin: Number.isFinite(docMin) ? docMin : 0,
      docMax: Number.isFinite(docMax) ? docMax : 0,
      filters: summarizeBlockFilters(filters || [], codes, slice.map(row => row.doc))
    });
  }
  return groups;
}

function facetCodes(value) {
  const values = Array.isArray(value) ? value : value?.codes || [];
  return [...new Set(values.map(Number).filter(Number.isFinite))].sort((a, b) => a - b);
}

function normalizedFilterValue(filter, value) {
  if (filter.kind === "facet") return { codes: facetCodes(value) };
  if (filter.kind === "boolean") return value == null ? null : value === true;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function bundleFilterValues(rows, filters, codes) {
  const out = {};
  for (const filter of (filters || []).filter(item => item.kind !== "facet")) {
    out[filter.name] = rows.map(row => normalizedFilterValue(filter, codes?.get(filter.name, row.doc)));
  }
  return out;
}

function docRuns(docs) {
  const runs = [];
  for (const doc of docs) {
    const last = runs[runs.length - 1];
    if (last && doc === last.start + last.count) last.count++;
    else runs.push({ start: doc, count: 1 });
  }
  return runs;
}

function createFilterCodeCache(filters, codes, bundles) {
  if (!filters?.length || !codes) return codes;
  const docs = [...new Set((bundles || []).flatMap(bundle => bundle.rows.map(row => row.doc)))].sort((a, b) => a - b);
  if (!docs.length) return codes;
  const runs = docRuns(docs);
  const byField = new Map();
  for (const filter of filters) {
    const values = new Map();
    for (const run of runs) {
      const rows = typeof codes.chunk === "function"
        ? codes.chunk(filter.name, run.start, run.count)
        : Array.from({ length: run.count }, (_, index) => codes.get(filter.name, run.start + index));
      rows.forEach((value, index) => values.set(run.start + index, value));
    }
    byField.set(filter.name, values);
  }
  return {
    get(name, doc) {
      return byField.get(name)?.get(doc) ?? null;
    }
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
    coverage = "all-base-docs",
    filters = [],
    codes = null
  } = options;
  const packWriter = createPackWriter(resolve(outDir, "bundles", "packs"), config.queryBundlePackBytes || config.packBytes);
  const filterCodes = createFilterCodeCache(filters, codes, bundles);
  const entries = [];
  for (const bundle of bundles || []) {
    const buffer = buildQueryBundle({
      ...bundle,
      rowGroups: bundleRowGroups(bundle.rows, config.queryBundleRowGroupSize, filters, filterCodes),
      filterValues: bundleFilterValues(bundle.rows, filters, filterCodes)
    }, { block_filters: filters });
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
    row_group_size: Math.max(1, Math.floor(Number(config.queryBundleRowGroupSize || 16))),
    row_group_filter_fields: filters.length,
    keys: entries.length,
    directory,
    packs: packWriter.packs,
    pack_bytes: packWriter.bytes,
    directory_bytes: directory.total_bytes
  };
}
