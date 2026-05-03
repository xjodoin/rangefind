import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

export const DEFAULTS = {
  docValueChunkSize: 2048,
  docValueLookupChunkSize: 2048,
  filterBitmaps: true,
  filterBitmapMaxFacetValues: 64,
  filterBitmapPackBytes: 4 * 1024 * 1024,
  baseShardDepth: 3,
  maxShardDepth: 5,
  targetShardPostings: 30000,
  packBytes: 4 * 1024 * 1024,
  externalPostingBlocks: true,
  externalPostingBlockMinBlocks: 4,
  externalPostingBlockMinBytes: 1024,
  postingBlockPackBytes: 4 * 1024 * 1024,
  docPackBytes: 4 * 1024 * 1024,
  docPageSize: 32,
  docPagePackBytes: 4 * 1024 * 1024,
  docPageMaxOverfetchDocs: 16,
  docLocalityTerms: 2,
  docValuePackBytes: 4 * 1024 * 1024,
  docValueSortedPageSize: 512,
  docValueSortedPackBytes: 4 * 1024 * 1024,
  directoryPageBytes: 64 * 1024,
  directorySortChunkEntries: 16384,
  queryBundles: true,
  queryBundleMaxKeys: 20000,
  queryBundleSeedCandidateFactor: 2,
  queryBundleMinSeedDocs: 2,
  queryBundleMaxRows: 64,
  queryBundleRowGroupSize: 16,
  queryBundleMaxTerms: 3,
  queryBundleSeedMaxFieldTokens: 160,
  queryBundlePackBytes: 4 * 1024 * 1024,
  authority: [],
  authorityMaxRowsPerKey: 16,
  authorityPackBytes: 4 * 1024 * 1024,
  authorityTargetShardRows: 4096,
  authorityMaxShardDepth: 8,
  authorityDirectoryPageBytes: 16 * 1024,
  facetDictionaryPackBytes: 4 * 1024 * 1024,
  blockFilterMaxFacetWords: 64,
  codeStoreCacheDocs: 16384,
  codeStoreCacheChunks: 64,
  codeStoreWorkerCacheChunks: 8,
  docLayoutSortChunkDocs: 100000,
  scanWorkers: 1,
  scanBatchDocs: 128,
  builderWorkerCount: 1,
  partitionReducerWorkers: 0,
  builderMemoryBudgetBytes: 0,
  segmentFlushDocs: 0,
  segmentFlushBytes: 0,
  segmentMaxDocs: 0,
  segmentMaxPostings: 250000,
  segmentMaxBytes: 64 * 1024 * 1024,
  segmentMergePolicy: "tiered-log",
  segmentMergeFanIn: 512,
  segmentMergeMaxTempBytes: 0,
  finalSegmentTargetCount: 0,
  maxTermsPerDoc: 160,
  maxExpansionTermsPerDoc: 12,
  initialResultLimit: 20,
  postingBlockSize: 128,
  postingSuperblockSize: 16,
  codecs: { mode: "auto" },
  optimizationBudgetRatio: 0.08,
  optimizationBudgetMaxBytes: 50 * 1024 * 1024,
  bm25fK1: 1.2,
  buildTelemetrySampleMs: 1000,
  buildProgressLogMs: 0,
  buildTelemetryPath: ""
};

function configDir(configPath) {
  return dirname(resolve(configPath));
}

function resolveFrom(base, value) {
  return resolve(base, value || ".");
}

export async function readConfig(configPath) {
  const full = resolve(configPath);
  const base = configDir(full);
  const raw = JSON.parse(await readFile(full, "utf8"));
  const activeRaw = { ...raw };
  for (const key of [
    "reduceWorkers",
    "reduceSortChunkRecords",
    "reduceSortChunkBytes",
    "reduceLargeRunBytes",
    "reduceWorkerHeapMb",
    "postingFlushLines"
  ]) {
    delete activeRaw[key];
  }
  return {
    ...DEFAULTS,
    ...activeRaw,
    codecs: { ...DEFAULTS.codecs, ...(raw.codecs || {}) },
    input: resolveFrom(base, raw.input),
    output: resolveFrom(base, raw.output || "public/rangefind"),
    buildTelemetryPath: raw.buildTelemetryPath ? resolveFrom(base, raw.buildTelemetryPath) : "",
    fields: raw.fields || [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: raw.facets || [],
    numbers: raw.numbers || [],
    booleans: raw.booleans || [],
    sorts: raw.sorts || [],
    display: raw.display || ["title", "url"],
    authority: raw.authority || DEFAULTS.authority
  };
}

export function getPath(object, path, fallback = "") {
  if (!path) return fallback;
  let value = object;
  for (const part of String(path).split(".")) {
    if (value == null) return fallback;
    value = value[part];
  }
  if (Array.isArray(value)) return value.join(" ");
  return value ?? fallback;
}
