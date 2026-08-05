import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { normalizeAnalysisConfig } from "./analysis.js";

export const DEFAULTS = {
  analysis: null,
  scoringStats: "",
  meta: null,
  // Optional link-graph authority prior. When set (the static-site crawler
  // populates it automatically), `field` names a numeric doc-value in [0, 1]
  // and `boost` is the default query-time multiplier: score *= 1 + boost*rank.
  linkGraph: null,
  // Generic numeric relevance prior. The field must be a numeric doc-value
  // normalized to [0, 1]. This is the reusable form of the crawler's historic
  // linkGraph prior and is useful for static corpora with an intrinsic
  // prominence/quality signal (places, products, documentation, ...).
  rankPrior: null,
  geoLeafSize: 512,
  geoPackBytes: 4 * 1024 * 1024,
  // Optional spatial result capsules. When enabled, every geo leaf carries
  // the configured display-field subset beside its coordinates/doc ids, so
  // nearest and viewport lanes can return results from the same range read
  // instead of opening the document store afterward.
  geoCapsules: false,
  geoCapsuleFields: [],
  geoCapsuleDocPageCachePages: 256,
  // The page-entry table is tiny compared with capsule payloads and is read
  // randomly in geo-tree order. Keep it resident to avoid one positional
  // syscall for every page cache miss.
  geoCapsuleDocPageIndexPreloadMaxBytes: 64 * 1024 * 1024,
  // Hot geo columns get a second, field-selective preload budget when the
  // complete build code store is just too large for its all-or-nothing path.
  geoCodeStorePreloadMaxBytes: 1536 * 1024 * 1024,
  geoLeafWorkerBatchLeaves: 16,
  // Optional multi-resolution facet routing over geo cells. Cell blocks carry
  // exact leaf/point ordinals; result payloads remain single-copy in geo leaves.
  geoCellIndexes: [],
  geoCellPackBytes: 4 * 1024 * 1024,
  geoCellDirectoryPageBytes: 64 * 1024,
  geoCellSortChunkRecords: 262144,
  suggestMaxTokenKeys: 4,
  suggestMinKeyLength: 1,
  suggestHotListSize: 64,
  vectorPackBytes: 4 * 1024 * 1024,
  vectorTrainSample: 20000,
  vectorKmeansIterations: 6,
  vectorClusterTargetDocs: 512,
  vectorCoarseDims: 0,
  docValueChunkSize: 2048,
  docValueLookupChunkSize: 2048,
  filterBitmaps: true,
  filterBitmapMaxFacetValues: 64,
  filterBitmapFacetValues: null,
  filterBitmapMaxBytes: 256 * 1024 * 1024,
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
  postingGzipLevel: 6,
  docPackBytes: 4 * 1024 * 1024,
  docPageSize: 32,
  docPagePackBytes: 4 * 1024 * 1024,
  docPageMaxOverfetchDocs: 16,
  docLocalityTerms: 2,
  docLayoutStrategy: "locality",
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
  authorityRunFlushRecords: 5000,
  facetDictionaryPackBytes: 4 * 1024 * 1024,
  blockFilterMaxFacetWords: 64,
  codeStoreCacheDocs: 16384,
  codeStoreCacheChunks: 64,
  codeStoreWorkerCacheChunks: 0,
  codeStoreWorkerMaxAutoCacheChunks: 64,
  docLayoutSortChunkDocs: 100000,
  // Preloading the compressed document spool speeds small/medium builds, but
  // a multi-gigabyte preload creates a matching external-memory/RSS spike.
  // Keep the fast path bounded; larger corpora use positional reads.
  docPackSpoolPreloadMaxBytes: 256 * 1024 * 1024,
  docPackSpoolPreloadChunkBytes: 256 * 1024 * 1024,
  docPackSequentialReadBytes: 64 * 1024 * 1024,
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
  if (config.rankPrior != null) {
    const field = String(config.rankPrior?.field || "").trim();
    if (!field) throw new Error("Rangefind rankPrior needs a numeric field.");
    if (!(config.numbers || []).some(number => number.name === field)) {
      throw new Error(`Rangefind rankPrior references unknown numeric field "${field}".`);
    }
    const boost = Number(config.rankPrior.boost ?? 0);
    if (!Number.isFinite(boost) || boost < 0) {
      throw new Error("Rangefind rankPrior boost must be a non-negative number.");
    }
    config.rankPrior = {
      field,
      boost,
      overfetch: clampInt(config.rankPrior.overfetch, 4, 1, 20)
    };
  }
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
  config.postingGzipLevel = clampInt(config.postingGzipLevel, DEFAULTS.postingGzipLevel, 0, 9);
  config.docLayoutStrategy = String(config.docLayoutStrategy || DEFAULTS.docLayoutStrategy).toLowerCase();
  if (!["locality", "doc-id"].includes(config.docLayoutStrategy)) config.docLayoutStrategy = DEFAULTS.docLayoutStrategy;
  config.geoCapsules = config.geoCapsules === true;
  config.geoCapsuleDocPageCachePages = clampInt(
    config.geoCapsuleDocPageCachePages,
    DEFAULTS.geoCapsuleDocPageCachePages,
    1,
    4096
  );
  config.geoCapsuleDocPageIndexPreloadMaxBytes = clampInt(
    config.geoCapsuleDocPageIndexPreloadMaxBytes,
    DEFAULTS.geoCapsuleDocPageIndexPreloadMaxBytes,
    0,
    1024 * 1024 * 1024
  );
  config.geoCodeStorePreloadMaxBytes = clampInt(
    config.geoCodeStorePreloadMaxBytes,
    DEFAULTS.geoCodeStorePreloadMaxBytes,
    0,
    8 * 1024 * 1024 * 1024
  );
  config.geoLeafWorkerBatchLeaves = clampInt(
    config.geoLeafWorkerBatchLeaves,
    DEFAULTS.geoLeafWorkerBatchLeaves,
    1,
    1024
  );
  config.geoCapsuleFields = Array.isArray(config.geoCapsuleFields)
    ? [...new Set(config.geoCapsuleFields.map(String).map(field => field.trim()).filter(Boolean))]
    : [];
  if (config.geoCapsules && !config.geoCapsuleFields.length) {
    config.geoCapsuleFields = (config.display || [])
      .map(field => typeof field === "string" ? field : field?.name)
      .map(String)
      .map(field => field.trim())
      .filter(Boolean);
  }
  const geoNames = new Set((config.geo || []).map(field => field.name));
  const facetNames = new Set((config.facets || []).map(field => field.name));
  config.geoCellIndexes = (Array.isArray(config.geoCellIndexes) ? config.geoCellIndexes : []).map((item, index) => {
    const field = String(item?.field || item?.geoField || "").trim();
    const facet = String(item?.facet || "").trim();
    if (!geoNames.has(field)) {
      throw new Error(`Rangefind geoCellIndexes[${index}] references unknown geo field "${field}".`);
    }
    if (!facetNames.has(facet)) {
      throw new Error(`Rangefind geoCellIndexes[${index}] references unknown facet "${facet}".`);
    }
    const rawLevels = Array.isArray(item.levels) && item.levels.length ? item.levels : [9, 12, 15];
    const levels = [...new Set(rawLevels
      .map(value => Number(value))
      .filter(Number.isFinite)
      .map(value => Math.max(0, Math.min(22, Math.floor(value)))))]
      .sort((a, b) => a - b);
    if (!levels.length) {
      throw new Error(`Rangefind geoCellIndexes[${index}] needs at least one numeric level.`);
    }
    const blockZoom = clampInt(item.blockZoom, Math.min(9, levels[0]), 0, levels[0]);
    return {
      field,
      facet,
      levels,
      blockZoom,
      codeGroupSize: clampInt(item.codeGroupSize, 16, 1, 256),
      maxCellsPerQuery: clampInt(item.maxCellsPerQuery, 48, 1, 256),
      maxFacetValues: clampInt(item.maxFacetValues, 256, 1, 4096),
      includeAll: item.includeAll === true,
      values: Array.isArray(item.values)
        ? [...new Set(item.values.map(String).map(value => value.trim()).filter(Boolean))]
        : []
    };
  });
  const geoCellPairs = new Set();
  for (const item of config.geoCellIndexes) {
    const key = `${item.field}\u0000${item.facet}`;
    if (geoCellPairs.has(key)) {
      throw new Error(`Rangefind geoCellIndexes contains duplicate ${item.field} × ${item.facet} indexes.`);
    }
    geoCellPairs.add(key);
  }
  config.geoCellSortChunkRecords = clampInt(
    config.geoCellSortChunkRecords,
    DEFAULTS.geoCellSortChunkRecords,
    1,
    4 * 1024 * 1024
  );
  return config;
}

function clampInt(value, fallback, min, max) {
  const parsed = Math.floor(Number(value ?? fallback));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

export function geoComponentFieldNames(geoField) {
  return { lat: `${geoField.name}.lat`, lon: `${geoField.name}.lon` };
}

function normalizeVectorFields(raw) {
  const fields = Array.isArray(raw) ? raw : [];
  return fields.map(field => {
    const name = String(field?.name || "").trim();
    if (!name) throw new Error("Rangefind vector fields need a name.");
    const dims = Math.floor(Number(field.dims));
    if (!Number.isFinite(dims) || dims < 2 || dims > 4096) {
      throw new Error(`Rangefind vector field "${name}" needs dims between 2 and 4096.`);
    }
    const metric = String(field.metric || "cosine").toLowerCase();
    if (metric !== "cosine") throw new Error(`Rangefind vector field "${name}" supports only the cosine metric.`);
    return { name, path: String(field.path || name), dims, metric };
  });
}

function normalizeSuggestFields(raw) {
  const fields = Array.isArray(raw) ? raw : [];
  return fields.map(field => {
    const path = String(field?.path || field?.name || "").trim();
    if (!path) throw new Error("Rangefind suggest fields need a path.");
    return {
      path,
      weightPath: field.weightPath ? String(field.weightPath) : "",
      tokenPrefixes: field.tokenPrefixes !== false
    };
  });
}

function normalizeGeoFields(raw) {
  const fields = Array.isArray(raw) ? raw : [];
  return fields.map(field => {
    const name = String(field?.name || "").trim();
    if (!name) throw new Error("Rangefind geo fields need a name.");
    if (!field.latPath || !field.lonPath) throw new Error(`Rangefind geo field "${name}" needs latPath and lonPath.`);
    return { name, latPath: String(field.latPath), lonPath: String(field.lonPath) };
  });
}

// Every geo field also stores its coordinates as hidden double doc-values so
// text searches can verify geo filters per doc through the existing numeric
// filter machinery (including posting-block range pruning).
function appendGeoComponentNumbers(config) {
  const existing = new Set(config.numbers.map(number => number.name));
  for (const geoField of config.geo) {
    const components = geoComponentFieldNames(geoField);
    for (const [axis, name] of Object.entries(components)) {
      if (existing.has(name)) throw new Error(`Rangefind geo field "${geoField.name}" collides with number field "${name}".`);
      config.numbers.push({
        name,
        path: axis === "lat" ? geoField.latPath : geoField.lonPath,
        type: "double",
        sortable: false,
        geoComponent: geoField.name
      });
    }
  }
  return config;
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
  return appendGeoComponentNumbers(applyIndexProfile({
    ...DEFAULTS,
    ...activeRaw,
    analysis: normalizeAnalysisConfig(raw.analysis),
    codecs: { ...DEFAULTS.codecs, ...(raw.codecs || {}) },
    input: resolveFrom(base, raw.input),
    output: resolveFrom(base, raw.output || "public/rangefind"),
    scoringStats: raw.scoringStats ? resolveFrom(base, raw.scoringStats) : "",
    // Free-form provenance carried verbatim into the manifest (`meta`):
    // data attribution/license, generator identity, source versions, …
    meta: raw.meta && typeof raw.meta === "object" ? { ...raw.meta } : null,
    buildTelemetryPath: raw.buildTelemetryPath ? resolveFrom(base, raw.buildTelemetryPath) : "",
    fields: raw.fields || [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    facets: raw.facets || [],
    numbers: (raw.numbers || []).map(number => ({ ...number })),
    booleans: raw.booleans || [],
    sorts: raw.sorts || [],
    sortReplicas: raw.sortReplicas || [],
    geo: normalizeGeoFields(raw.geo),
    suggest: normalizeSuggestFields(raw.suggest),
    vectors: normalizeVectorFields(raw.vectors),
    display: raw.display || ["title", "url"],
    authority: raw.authority || DEFAULTS.authority
  }, raw));
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
