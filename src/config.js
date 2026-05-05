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
  postingSegmentStreamMinBytes: 64 * 1024,
  docPackBytes: 4 * 1024 * 1024,
  docPageSize: 32,
  docPagePackBytes: 4 * 1024 * 1024,
  docPageMaxOverfetchDocs: 16,
  docLocalityTerms: 2,
  docValuePackBytes: 4 * 1024 * 1024,
  docValueSortedPageSize: 512,
  docValueSortedPackBytes: 4 * 1024 * 1024,
  sortReplicas: [],
  sortReplicaRankChunkSize: 4096,
  sortReplicaDocPageSize: 8,
  sortReplicaPackBytes: 4 * 1024 * 1024,
  sortReplicaDocPagePackBytes: 4 * 1024 * 1024,
  sortReplicaPostingBlockPackBytes: 4 * 1024 * 1024,
  directoryPageBytes: 64 * 1024,
  directorySortChunkEntries: 16384,
  queryBundles: false,
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
  codeStoreWorkerCacheChunks: 0,
  codeStoreWorkerMaxAutoCacheChunks: 64,
  docLayoutSortChunkDocs: 100000,
  scanWorkers: 1,
  scanBatchDocs: 128,
  builderWorkerCount: 1,
  partitionReducerWorkers: 0,
  partitionReducerInFlightBytes: 1024 * 1024 * 1024,
  builderMemoryBudgetBytes: 0,
  indexProfile: "static-large",
  targetPostingsPerDoc: 12,
  bodyIndexChars: 6000,
  alwaysIndexFields: ["title", "categories"],
  resumeBuild: true,
  resumeDir: "_build/resume",
  typoMode: "main-index",
  typoTrigger: "zero-or-weak",
  typoMaxEdits: 2,
  typoMaxTokenCandidates: 8,
  typoMaxQueryPlans: 5,
  typoMaxCorrectedSearches: 3,
  typoMaxShardLookups: 12,
  segmentFlushDocs: 0,
  segmentFlushBytes: 0,
  segmentMaxDocs: 0,
  segmentMaxPostings: 250000,
  segmentMaxBytes: 64 * 1024 * 1024,
  segmentMergePolicy: "tiered-log",
  segmentMergeFanIn: 128,
  segmentMergeMaxTempBytes: 512 * 1024 * 1024,
  finalSegmentTargetCount: 0,
  maxTermsPerDoc: 12,
  maxExpansionTermsPerDoc: 0,
  initialResultLimit: 20,
  postingOrder: "doc-id",
  postingBlockSize: 128,
  postingSuperblockSize: 16,
  postingImpactBucketOrderMinRows: Number.MAX_SAFE_INTEGER,
  postingImpactBucketOrderMaxBuckets: 65536,
  postingImpactTiers: false,
  postingImpactTierMinBlocks: 8,
  postingImpactTierMaxBlocks: 256,
  postingDocRangeBlockMax: false,
  postingDocRangeSize: 1024,
  postingDocRangeQuantizationBits: 8,
  codecs: { mode: "varint" },
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

function applyIndexProfile(config, raw) {
  config.indexProfile = String(config.indexProfile || DEFAULTS.indexProfile).toLowerCase();
  config.targetPostingsPerDoc = Math.max(0, Math.floor(Number(config.targetPostingsPerDoc ?? DEFAULTS.targetPostingsPerDoc)));
  config.maxTermsPerDoc = config.targetPostingsPerDoc;
  config.bodyIndexChars = Math.max(0, Math.floor(Number(config.bodyIndexChars ?? DEFAULTS.bodyIndexChars)));
  config.alwaysIndexFields = Array.isArray(config.alwaysIndexFields)
    ? config.alwaysIndexFields.map(String).filter(Boolean)
    : DEFAULTS.alwaysIndexFields.slice();
  if (raw.resumeBuild == null) config.resumeBuild = config.indexProfile === "static-large";
  config.resumeDir = String(config.resumeDir || DEFAULTS.resumeDir);
  config.typoMode = String(config.typoMode || DEFAULTS.typoMode).toLowerCase();
  if (!["main-index", "off"].includes(config.typoMode)) config.typoMode = DEFAULTS.typoMode;
  config.typoTrigger = String(config.typoTrigger || DEFAULTS.typoTrigger).toLowerCase();
  if (!["zero", "zero-or-weak"].includes(config.typoTrigger)) config.typoTrigger = DEFAULTS.typoTrigger;
  config.typoMaxEdits = clampInt(config.typoMaxEdits, DEFAULTS.typoMaxEdits, 1, 3);
  config.typoMaxTokenCandidates = clampInt(config.typoMaxTokenCandidates, DEFAULTS.typoMaxTokenCandidates, 1, 32);
  config.typoMaxQueryPlans = clampInt(config.typoMaxQueryPlans, DEFAULTS.typoMaxQueryPlans, 1, 32);
  config.typoMaxCorrectedSearches = clampInt(config.typoMaxCorrectedSearches, DEFAULTS.typoMaxCorrectedSearches, 1, 8);
  config.typoMaxShardLookups = clampInt(config.typoMaxShardLookups, DEFAULTS.typoMaxShardLookups, 1, 64);
  return config;
}

function clampInt(value, fallback, min, max) {
  const parsed = Math.floor(Number(value ?? fallback));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
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
    "postingFlushLines",
    "typo"
  ]) {
    delete activeRaw[key];
  }
  return applyIndexProfile({
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
    sortReplicas: raw.sortReplicas || [],
    display: raw.display || ["title", "url"],
    authority: raw.authority || DEFAULTS.authority
  }, raw);
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
