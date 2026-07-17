import { expandedTermsFromBaseTerms, proximityTerm, queryBundleKeysFromBaseTerms } from "./terms.js";
import { analyzerFromManifest } from "./analysis.js";
import { authorityAddressRangeKey, authorityKeysForQuery, authorityNormalizeSurface, parseAuthorityShard } from "./authority_codec.js";
import { decodePostingBlock, decodePostingBytes, decodePostings, lookupDecodedPostingRows, lookupPostingBlock, lookupPostingBytes, parseCodes, parseDocValueChunk, parseFacetDictionary, parsePostingSegment } from "./codec.js";
import { findDirectoryPage, parseDirectoryPage, parseDirectoryRoot } from "./directory.js";
import { floorDirectoryPageIndex, floorSortedKeyIndex, parseTextRoutingSegment, TEXT_ROUTING_FORMAT } from "./text_routing_codec.js";
import { DOC_PAGE_ENCODING, decodeDocPageColumns, decodeDocPagePointerRecord } from "./doc_pages.js";
import { decodeDocValueSortPage, parseDocValueSortDirectory } from "./doc_value_tree.js";
import {
  boxContainsBoxE7,
  boxContainsPointE7,
  boxIntersectsE7,
  boxesForRadiusE7,
  decodeGeoBranchPage,
  decodeGeoLeafPage,
  haversineMetersE7,
  latToE7,
  lonToE7,
  parseGeoTreeRoot,
  pointToBoxDistanceMetersE7,
  pointToBoxMaxDistanceMetersE7
} from "./geo_tree.js";
import { decodeDocPointerRecord } from "./doc_pointers.js";
import { applyHighlights, highlightTermSet } from "./highlight.js";
import {
  applyPermutation,
  decodeVectorClusterPage,
  dotInt8,
  normalizeVector,
  parseVectorRoot,
  vectorFromValue
} from "./vector_index.js";
import {
  AUTOCOMPLETE_PREFIX,
  autocompleteRank,
  compareAutocomplete,
  parseAutocompleteKey,
  parseAuthorityHotList,
  parseAuthorityLexiconRoot,
  suggestKey
} from "./authority_lexicon.js";
import { filterBitmapHas, parseFilterBitmap } from "./filter_bitmaps.js";
import { verifyBlockPointer } from "./object_store.js";
import { parseQueryBundle } from "./query_bundle_codec.js";
import {
  addressRangeContains,
  addressRangeQueryCandidates,
  interpolateAddressRangePoint,
  looksLikeAddressQuery,
  normalizePostalCodePrefixSpacing,
  normalizePostalCodeSpacing
} from "./address.js";
import { decodeSegmentRows, parseSegmentTerms } from "./segment_codec.js";
import { groupRanges, shardKey } from "./shards.js";
import {
  bestMainIndexTypoDistance,
  isTypoCorrectionToken,
  mainIndexTypoCandidateScore,
  mainIndexTypoProbeValues,
  ngramOverlap,
  normalizeMainIndexTypoOptions,
  typoMaxEditsFor
} from "./typo_main_index.js";

const RERANK_CANDIDATES = 30;
const DEPENDENCY_SCORE_SCALE = 0.12;
const SKIP_MAX_TERMS = 30;
const EXTERNAL_POSTING_BLOCK_PREFETCH = 16;
const POSTING_BLOCK_FRONTIER = 4;
const FILTER_BITMAP_SPARSE_DOC_LIMIT = 256;
const TYPO_CORRECTION_CANDIDATES_PER_TOKEN = 2;
const TYPO_CORRECTION_PLAN_LIMIT = 6;
const TYPO_CORRECTION_EXECUTION_PLAN_LIMIT = 3;
const TYPO_CORRECTION_RELATIVE_SCORE = 0.5;
const DOC_RANGE_PLANNER_MIN_CANDIDATE_RANGES = 2;
const DOC_RANGE_PLANNER_MAX_CANDIDATE_BLOCK_RATIO = 0.12;
const DOC_RANGE_BLOCK_PRUNE_BATCH_SIZE = 1024;
const DOC_RANGE_BLOCK_PRUNE_INITIAL_BATCH_SIZE = 32;
const DOC_VALUE_SORT_PAGE_BATCH_SIZE = 16;
const GEO_LEAF_PAGE_BATCH_SIZE = 16;
const GEO_LEAF_PAGE_FIRST_BATCH_SIZE = 4;
const GEO_E7_PRUNE_MARGIN_DEGREES = 1e-7;
const GEO_TEXT_MAX_CANDIDATE_POINTS = 100000;
// With a text query, the geo doc set may grow beyond the base cap when its
// exact leaf-page cost undercuts the doc-value chunks the text lane would
// otherwise fetch to verify matches. The hard cap bounds the in-memory Set.
const GEO_TEXT_DOC_SET_HARD_CAP = 1000000;
const GEO_TEXT_SORT_MAX_DF = 200000;
const FACET_COUNT_MAX_CHUNKS = 32;
const FACET_COUNT_SIZE = 10;
const textDecoder = new TextDecoder();
let activeRuntimeTrace = null;

function nowMs() {
  return globalThis.performance?.now?.() ?? Date.now();
}

function createRuntimeTrace() {
  return {
    started: nowMs(),
    spans: new Map()
  };
}

// Shared by the single engine and the sharded routing layer: queries up to
// four terms require every term, longer ones tolerate a single miss.
function minShouldMatchFor(baseTerms) {
  return baseTerms.length <= 4 ? baseTerms.length : baseTerms.length - 1;
}

function traceBucketFromPath(path) {
  if (path.endsWith("/manifest.min.json")) return "manifest";
  if (/\/manifest(?:\.[0-9a-f]+)?\.json(?:\.gz)?$/u.test(path)) return "manifest";
  if (path.includes("/sort-replicas/") && path.includes("/docs/pointers/")) return "sortReplicaDocPointers";
  if (path.includes("/sort-replicas/") && path.includes("/docs/packs/")) return "sortReplicaDocs";
  if (path.includes("/sort-replicas/") && path.includes("/docs/pages/")) return "sortReplicaDocPagePointers";
  if (path.includes("/sort-replicas/") && path.includes("/docs/page-packs/")) return "sortReplicaDocPages";
  if (path.includes("/sort-replicas/") && path.includes("/rank-packs/")) return "sortReplicaRankMaps";
  if (path.includes("/sort-replicas/") && path.includes("/terms/block-packs/")) return "sortReplicaPostingBlocks";
  if (path.includes("/sort-replicas/") && path.includes("/terms/packs/")) return "sortReplicaTerms";
  if (path.includes("/terms/block-packs/")) return "postingBlocks";
  if (path.includes("/terms/packs/")) return "terms";
  if (path.includes("/bundles/packs/")) return "queryBundles";
  if (path.includes("/authority/packs/")) return "authority";
  if (path.includes("/authority/lexicon-root.")) return "authorityLexicon";
  if (path.includes("/authority/hot/")) return "authorityHot";
  if (path.includes("/sort-replicas/")) return "sortReplicas";
  if (path.includes("/doc-values/sorted")) return "docValueSorted";
  if (path.includes("/doc-values/")) return "docValues";
  if (path.includes("/docs/pointers/")) return "docPointers";
  if (path.includes("/docs/pages/")) return "docPagePointers";
  if (path.includes("/docs/page-packs/")) return "docPages";
  if (path.includes("/docs/")) return "docs";
  if (path.includes("/facets/")) return "facetDictionaries";
  if (path.includes("/filter-bitmaps/")) return "filterBitmaps";
  if (path.includes("/text-routing/")) return "textRouting";
  if (path.includes("/directory-")) return "directory";
  if (path.endsWith("/codes.bin.gz")) return "codes";
  return "other";
}

function traceBucketFromUrl(url) {
  try {
    return traceBucketFromPath(new URL(String(url)).pathname);
  } catch {
    return "other";
  }
}

function traceLabelBucket(label) {
  const value = String(label || "");
  if (value.startsWith("posting block")) return "postingBlocks";
  if (value.startsWith("posting segment")) return "terms";
  if (value.startsWith("query bundle")) return "queryBundles";
  if (value.startsWith("authority shard")) return "authority";
  if (value.startsWith("sort replica doc pointer")) return "sortReplicaDocPointers";
  if (value.startsWith("sort replica doc page")) return "sortReplicaDocPages";
  if (value.startsWith("sort replica doc ")) return "sortReplicaDocs";
  if (value.startsWith("sort replica rank")) return "sortReplicaRankMaps";
  if (value.startsWith("sort replica segment")) return "sortReplicaTerms";
  if (value.startsWith("sort replica")) return "sortReplicas";
  if (value.startsWith("doc-value sort page")) return "docValueSorted";
  if (value.startsWith("doc-value")) return "docValues";
  if (value.startsWith("doc page")) return "docPages";
  if (value.startsWith("doc ")) return "docs";
  if (value.startsWith("facet dictionary")) return "facetDictionaries";
  if (value.startsWith("filter bitmap")) return "filterBitmaps";
  return "object";
}

function recordTraceSpan(trace, name, ms, bytes = 0) {
  if (!trace || !Number.isFinite(ms)) return;
  const current = trace.spans.get(name) || { name, count: 0, totalMs: 0, maxMs: 0, bytes: 0 };
  current.count++;
  current.totalMs += ms;
  current.maxMs = Math.max(current.maxMs, ms);
  if (Number.isFinite(bytes) && bytes > 0) current.bytes += bytes;
  trace.spans.set(name, current);
}

async function traceSpan(name, fn) {
  const trace = activeRuntimeTrace;
  if (!trace) return fn();
  const started = nowMs();
  try {
    return await fn();
  } finally {
    recordTraceSpan(trace, name, nowMs() - started);
  }
}

async function traceFetch(bucket, fn) {
  const trace = activeRuntimeTrace;
  if (!trace) return fn();
  const started = nowMs();
  let response;
  try {
    response = await fn();
    return response;
  } finally {
    const bytes = Number(response?.headers?.get?.("content-length") || 0);
    recordTraceSpan(trace, `${bucket}.fetch`, nowMs() - started, bytes);
  }
}

function traceSpanSync(name, fn) {
  const trace = activeRuntimeTrace;
  if (!trace) return fn();
  const started = nowMs();
  try {
    return fn();
  } finally {
    recordTraceSpan(trace, name, nowMs() - started);
  }
}

async function withRuntimeTrace(trace, fn) {
  const previous = activeRuntimeTrace;
  activeRuntimeTrace = trace || previous;
  try {
    return await fn();
  } finally {
    activeRuntimeTrace = previous;
  }
}

function finalizeRuntimeTrace(trace) {
  if (!trace) return null;
  const spans = [...trace.spans.values()]
    .map(span => ({
      name: span.name,
      count: span.count,
      totalMs: span.totalMs,
      maxMs: span.maxMs,
      ...(span.bytes > 0 ? { bytes: span.bytes } : {})
    }))
    .sort((left, right) => right.totalMs - left.totalMs || left.name.localeCompare(right.name));
  return {
    totalMs: nowMs() - trace.started,
    totalBytes: spans.reduce((sum, span) => sum + Number(span.bytes || 0), 0),
    spans
  };
}

// Injectable gzip inflation: browsers and Node use DecompressionStream, while
// hosts without it (React Native/Hermes, QuickJS, JavaScriptCore) install an
// implementation such as pako.ungzip via setInflateImplementation().
let inflateImpl = null;

export function setInflateImplementation(fn) {
  inflateImpl = typeof fn === "function" ? fn : null;
}

function viewToArrayBuffer(bytes) {
  if (bytes instanceof ArrayBuffer) return bytes;
  if (ArrayBuffer.isView(bytes)) return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  throw new Error("Inflate implementation must return an ArrayBuffer or a typed array.");
}

async function inflateGzip(responseOrBuffer) {
  if (inflateImpl) {
    const compressed = responseOrBuffer instanceof ArrayBuffer
      ? responseOrBuffer
      : await responseOrBuffer.arrayBuffer();
    return viewToArrayBuffer(await inflateImpl(compressed));
  }
  if (!("DecompressionStream" in globalThis)) {
    throw new Error("Rangefind requires DecompressionStream for compressed static index files. On hosts without it, provide one with setInflateImplementation().");
  }
  const stream = responseOrBuffer instanceof ArrayBuffer
    ? new Blob([responseOrBuffer]).stream()
    : responseOrBuffer.body;
  if (!stream) throw new Error("Response body is not streamable.");
  return new Response(stream.pipeThrough(new DecompressionStream("gzip"))).arrayBuffer();
}

// Injectable transport: the browser uses the platform fetch as-is, while the
// Node runtime (src/runtime.node.js) installs a scheme-routing fetch that adds
// file:// support and browser-equivalent HTTP caching.
let fetchImpl = (url, init) => fetch(url, init);

export function setFetchImplementation(fn) {
  fetchImpl = typeof fn === "function" ? fn : ((url, init) => fetch(url, init));
}

async function fetchGzipArrayBuffer(url) {
  const bucket = traceBucketFromUrl(url);
  const response = await traceFetch(bucket, () => fetchImpl(url));
  if (!response.ok) throw new Error(`Unable to fetch ${url}`);
  return traceSpan(`${bucket}.inflate`, () => inflateGzip(response));
}

async function fetchRange(url, offset, length) {
  const bucket = traceBucketFromUrl(url);
  const response = await traceFetch(bucket, () => fetchImpl(url, {
    headers: { Range: `bytes=${offset}-${offset + length - 1}` }
  }));
  if (response.status !== 206) throw new Error(`Range request failed for ${url}`);
  return traceSpan(`${bucket}.read`, () => response.arrayBuffer());
}

function selectedFacetCodes(manifest, field, selected) {
  if (!selected?.size) return null;
  const values = Array.isArray(manifest.facets?.[field]) ? manifest.facets[field] : [];
  const out = new Set();
  values.forEach((item, idx) => {
    if (selected.has(item.value) || selected.has(item.label)) out.add(idx);
  });
  return out;
}

function facetCodeMatches(words, selected) {
  if (!selected?.size) return true;
  if (Array.isArray(words?.codes)) {
    for (const code of words.codes) if (selected.has(code)) return true;
    return false;
  }
  if (Array.isArray(words)) {
    for (const value of selected) {
      const word = Math.floor(value / 32);
      const bit = value % 32;
      if ((words[word] || 0) & (2 ** bit)) return true;
    }
    return false;
  }
  return selected.has(words);
}

export async function createSearch(options = {}) {
  const baseUrl = options.baseUrl || "./rangefind/";
  async function fetchJsonIfOk(path) {
    const url = new URL(path, baseUrl);
    const response = await traceFetch(traceBucketFromUrl(url), () => fetchImpl(url));
    return response.ok ? response.json() : null;
  }

  async function fetchManifestJsonIfOk(path) {
    if (!path) return null;
    if (!String(path).endsWith(".gz")) return fetchJsonIfOk(path);
    const url = new URL(path, baseUrl);
    const response = await traceFetch(traceBucketFromUrl(url), () => fetchImpl(url));
    if (!response.ok) return null;
    const inflated = await traceSpan("manifest.inflate", () => inflateGzip(response));
    return traceSpanSync("manifest.parse", () => JSON.parse(textDecoder.decode(inflated)));
  }

  const manifest = options.manifestName
    ? await fetchJsonIfOk(options.manifestName)
    : await fetchJsonIfOk("manifest.min.json") || await fetchJsonIfOk("manifest.json");
  if (!manifest) throw new Error("Rangefind manifest could not be loaded.");
  if (Array.isArray(manifest.shards)) {
    return createShardedSearch(manifest, options, baseUrl);
  }
  if (Array.isArray(manifest.generations)) {
    return createGenerationalSearch(manifest, options, baseUrl);
  }
  // The manifest's analysis profile reconstructs the exact analyzer the
  // builder used, so query terms always live in the index's term space.
  const analyzer = analyzerFromManifest(manifest);
  const verifyChecksums = options.verifyChecksums !== false && !!(manifest.features?.checksummedObjects || manifest.object_store?.checksum);
  const termDirectory = createDirectoryState(manifest.directory);
  const queryBundleDirectory = manifest.query_bundles?.directory ? createDirectoryState(manifest.query_bundles.directory) : null;
  const authorityDirectory = manifest.authority?.directory ? createDirectoryState(manifest.authority.directory) : null;
  const docPointers = manifest.docs?.pointers;
  const docPages = manifest.docs?.pages || null;
  let facetDictionaries = manifest.facet_dictionaries || null;
  let facetDictionaryManifestPromise = null;
  let facetDirectory = facetDictionaries?.directory ? createDirectoryState(facetDictionaries.directory) : null;
  const shardCache = new Map();
  const queryBundleCache = new Map();
  const authorityShardCache = new Map();
  const segmentTermsCache = new Map();
  const segmentRowsCache = new Map();
  const docPointerCache = new Map();
  const packedDocCache = new Map();
  const docPagePointerCache = new Map();
  const docPageCache = new Map();
  const docValueCache = new Map();
  const docValueSortDirectoryCache = new Map();
  const docValueSortPageCache = new Map();
  const geoTreeRootCache = new Map();
  const geoBranchPageCache = new Map();
  const geoLeafPageCache = new Map();
  let authorityLexiconRootPromise = null;
  const authorityHotCache = new Map();
  const vectorRootCache = new Map();
  const vectorClusterPageCache = new Map();
  const sortReplicaDirectoryCache = new Map();
  const sortReplicaShardCache = new Map();
  const sortReplicaRankCache = new Map();
  const sortReplicaDocPointerCache = new Map();
  const sortReplicaPackedDocCache = new Map();
  const sortReplicaDocPagePointerCache = new Map();
  const sortReplicaDocPageCache = new Map();
  const facetDictionaryCache = new Map();
  let codes = null;
  let codesPromise = null;
  let fullManifestPromise = null;
  let fullManifestLoaded = !manifest.lazy_manifests?.full;
  let buildTelemetry = manifest.build || null;
  let buildTelemetryPromise = null;
  let indexOptimizer = null;
  let indexOptimizerPromise = null;
  let segmentManifest = null;
  let segmentManifestPromise = null;
  let docValues = manifest.doc_values || null;
  let docValuesPromise = null;
  let docValueSorted = manifest.doc_value_sorted || null;
  let docValueSortedPromise = null;
  let filterBitmaps = manifest.filter_bitmaps || null;
  let filterBitmapsPromise = null;
  const docValueStore = { _docValues: true };
  const maxPageSize = Math.max(1, Math.min(1000, Math.floor(Number(options.maxPageSize || 100))));
  const topKProofMaxK = Math.max(1, Math.min(1000, Math.floor(Number(options.topKProofMaxK || 100))));
  const filterBitmapCache = new Map();
  const numberFields = new Map((manifest.numbers || []).map(field => [field.name, field]));
  const booleanFields = new Map((manifest.booleans || []).map(field => [field.name, field]));
  let blockFilterFields = new Set((manifest.block_filters || []).map(filter => filter.name));
  const rangePlans = {
    default: { mergeGapBytes: 8 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: 4 },
    docPointers: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 32 * 1024, maxOverfetchRatio: Infinity },
    docs: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 8 * 1024, maxOverfetchRatio: Infinity },
    docPagePointers: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 32 * 1024, maxOverfetchRatio: Infinity },
    docPages: { mergeGapBytes: 64 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: Infinity },
    docValueSortPages: { mergeGapBytes: 64 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: Infinity },
    sortReplicaRankMaps: { mergeGapBytes: 64 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: Infinity },
    sortReplicaDocPointers: { mergeGapBytes: 4 * 1024, maxOverfetchBytes: 8 * 1024, maxOverfetchRatio: Infinity },
    sortReplicaDocs: { mergeGapBytes: 8 * 1024, maxOverfetchBytes: 16 * 1024, maxOverfetchRatio: Infinity },
    sortReplicaDocPagePointers: { mergeGapBytes: 4 * 1024, maxOverfetchBytes: 8 * 1024, maxOverfetchRatio: Infinity },
    sortReplicaDocPages: { mergeGapBytes: 8 * 1024, maxOverfetchBytes: 16 * 1024, maxOverfetchRatio: Infinity },
    authority: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 32 * 1024, maxOverfetchRatio: Infinity },
    postingBlocks: { mergeGapBytes: 256 * 1024, maxMergedBytes: 1024 * 1024, maxOverfetchBytes: 512 * 1024, maxOverfetchRatio: Infinity },
    postingBlockFrontier: { mergeGapBytes: 512 * 1024, maxMergedBytes: 2 * 1024 * 1024, maxOverfetchBytes: 1024 * 1024, maxOverfetchRatio: Infinity },
    postingDocRanges: { mergeGapBytes: 512 * 1024, maxMergedBytes: 2 * 1024 * 1024, maxOverfetchBytes: 1024 * 1024, maxOverfetchRatio: Infinity },
    geoLeafPages: { mergeGapBytes: 64 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: Infinity },
    vectorClusterPages: { mergeGapBytes: 64 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: Infinity },
    vectorRefine: { mergeGapBytes: 16 * 1024, maxOverfetchBytes: 32 * 1024, maxOverfetchRatio: 8 },
    ...(options.rangePlans || {})
  };

  async function ensureFullManifest() {
    if (fullManifestLoaded) return manifest;
    if (!fullManifestPromise) {
      fullManifestPromise = fetchJsonIfOk(manifest.lazy_manifests.full)
        .then(full => {
          if (!full) return manifest;
          Object.assign(manifest, full);
          docValues = manifest.doc_values || null;
          docValueSorted = manifest.doc_value_sorted || null;
          filterBitmaps = manifest.filter_bitmaps || null;
          facetDictionaries = manifest.facet_dictionaries || null;
          facetDirectory = facetDictionaries?.directory ? createDirectoryState(facetDictionaries.directory) : null;
          blockFilterFields = new Set((manifest.block_filters || []).map(filter => filter.name));
          fullManifestLoaded = true;
          return manifest;
        });
    }
    return fullManifestPromise;
  }

  async function ensureDocValuesManifest() {
    if (docValues) return docValues;
    const path = manifest.lazy_manifests?.doc_values;
    if (!path) return null;
    if (!docValuesPromise) {
      docValuesPromise = fetchManifestJsonIfOk(path).then(meta => {
        docValues = meta || null;
        if (docValues) manifest.doc_values = docValues;
        return docValues;
      });
    }
    return docValuesPromise;
  }

  async function ensureDocValueSortedManifest() {
    if (docValueSorted) return docValueSorted;
    const path = manifest.lazy_manifests?.doc_value_sorted;
    if (!path) return null;
    if (!docValueSortedPromise) {
      docValueSortedPromise = fetchManifestJsonIfOk(path).then(meta => {
        docValueSorted = meta || null;
        if (docValueSorted) manifest.doc_value_sorted = docValueSorted;
        return docValueSorted;
      });
    }
    return docValueSortedPromise;
  }

  async function ensureFilterBitmapManifest() {
    if (filterBitmaps) return filterBitmaps;
    const path = manifest.lazy_manifests?.filter_bitmaps;
    if (!path) return null;
    if (!filterBitmapsPromise) {
      filterBitmapsPromise = fetchManifestJsonIfOk(path).then(meta => {
        filterBitmaps = meta || null;
        if (filterBitmaps) manifest.filter_bitmaps = filterBitmaps;
        return filterBitmaps;
      });
    }
    return filterBitmapsPromise;
  }

  async function ensureFacetDictionaryManifest() {
    if (facetDictionaries) return facetDictionaries;
    const path = manifest.lazy_manifests?.facet_dictionaries;
    if (!path) return null;
    if (!facetDictionaryManifestPromise) {
      facetDictionaryManifestPromise = fetchManifestJsonIfOk(path).then(meta => {
        facetDictionaries = meta || null;
        if (facetDictionaries) {
          manifest.facet_dictionaries = facetDictionaries;
          facetDirectory = facetDictionaries.directory ? createDirectoryState(facetDictionaries.directory) : null;
        }
        return facetDictionaries;
      });
    }
    return facetDictionaryManifestPromise;
  }

  async function loadBuildTelemetry() {
    if (buildTelemetry) return buildTelemetry;
    const path = manifest.lazy_manifests?.build;
    if (!path) return null;
    if (!buildTelemetryPromise) buildTelemetryPromise = fetchJsonIfOk(path);
    buildTelemetry = await buildTelemetryPromise;
    return buildTelemetry;
  }

  async function loadIndexOptimizer() {
    if (indexOptimizer) return indexOptimizer;
    const path = manifest.lazy_manifests?.optimizer || manifest.optimizer?.path;
    if (!path) return null;
    if (!indexOptimizerPromise) indexOptimizerPromise = fetchJsonIfOk(path);
    indexOptimizer = await indexOptimizerPromise;
    return indexOptimizer;
  }

  async function loadSegmentManifest() {
    if (segmentManifest) return segmentManifest;
    const path = manifest.segments?.manifest;
    if (!path) return null;
    if (!segmentManifestPromise) segmentManifestPromise = fetchManifestJsonIfOk(path);
    segmentManifest = await segmentManifestPromise;
    return segmentManifest;
  }
  const postingBlockFrontier = Math.max(1, Math.min(16, Math.floor(Number(options.postingBlockFrontier || POSTING_BLOCK_FRONTIER))));
  const topKProofCheckInterval = Math.max(1, Math.min(4096, Math.floor(Number(options.topKProofCheckInterval || 1))));
  const topKProofCheckIntervalMax = Math.max(
    topKProofCheckInterval,
    Math.min(4096, Math.floor(Number(options.topKProofCheckIntervalMax || 32)))
  );
  const topKProofCheckScoresPerBlock = Math.max(1, Math.floor(Number(options.topKProofCheckScoresPerBlock || 2048)));
  // An unbounded proof can decode millions of postings for broad multi-term
  // queries. Large indexes default to a bounded approximate top-k lane; callers
  // can still pass 0 (or request exact search) when exhaustive proof matters.
  const defaultTopKBlockBudget = Number(manifest.total || 0) >= 1_000_000 ? 128 : 0;
  const topKBlockBudget = Math.max(0, Math.floor(Number(options.topKBlockBudget ?? defaultTopKBlockBudget)));
  const docValueSortPageBatchSize = Math.max(1, Math.min(
    64,
    Math.floor(Number(options.docValueSortPageBatchSize || DOC_VALUE_SORT_PAGE_BATCH_SIZE))
  ));
  const geoLeafPageBatchSize = Math.max(1, Math.min(
    64,
    Math.floor(Number(options.geoLeafPageBatchSize || GEO_LEAF_PAGE_BATCH_SIZE))
  ));
  const geoTextMaxCandidatePoints = Math.max(0, Math.floor(Number(
    options.geoTextMaxCandidatePoints ?? GEO_TEXT_MAX_CANDIDATE_POINTS
  )));
  const geoTextSortMaxDf = Math.max(0, Math.floor(Number(
    options.geoTextSortMaxDf ?? GEO_TEXT_SORT_MAX_DF
  )));
  const facetCountMaxChunks = Math.max(1, Math.floor(Number(
    options.facetCountMaxChunks ?? FACET_COUNT_MAX_CHUNKS
  )));
  const docRangePlannerEnabled = options.docRangePlanner !== false;
  const docRangeBlockPruneBatchSize = Math.max(1, Math.min(
    2048,
    Math.floor(Number(options.docRangeBlockPruneBatchSize || DOC_RANGE_BLOCK_PRUNE_BATCH_SIZE))
  ));
  const docRangeBlockPruneInitialBatchSize = Math.max(1, Math.min(
    docRangeBlockPruneBatchSize,
    Math.floor(Number(options.docRangeBlockPruneInitialBatchSize || DOC_RANGE_BLOCK_PRUNE_INITIAL_BATCH_SIZE))
  ));
  const docRangeImpactPlannerEnabled = options.docRangeImpactPlanner !== false;
  const runtimeTypo = normalizeMainIndexTypoOptions(options, manifest);
  const typoCorrectionExecutionPlanLimit = Math.max(1, Math.min(
    runtimeTypo.maxQueryPlans || TYPO_CORRECTION_PLAN_LIMIT,
    runtimeTypo.maxCorrectedSearches || TYPO_CORRECTION_EXECUTION_PLAN_LIMIT,
    Math.floor(Number(options.typoCorrectionExecutionPlans || runtimeTypo.maxCorrectedSearches || TYPO_CORRECTION_EXECUTION_PLAN_LIMIT))
  ));

  function rangeGroups(items, kind = "default") {
    return groupRanges(items, rangePlans[kind] || rangePlans.default);
  }

  function createDirectoryState(directory) {
    if (!directory?.root || !directory?.pages) throw new Error("Rangefind index is missing a range directory.");
    return {
      meta: {
        root: directory.root,
        pages: directory.pages,
        packTable: directory.pack_table || directory.packs || []
      },
      root: null,
      rootPromise: null,
      pages: new Map()
    };
  }

  function directoryPagePath(state, page) {
    return `${state.meta.pages.replace(/\/?$/u, "/")}${page.file}`;
  }

  async function loadDirectoryRoot(state) {
    if (state.root) return state.root;
    if (!state.rootPromise) {
      state.rootPromise = fetchGzipArrayBuffer(new URL(state.meta.root, baseUrl))
        .then(buffer => traceSpanSync("directory.parseRoot", () => parseDirectoryRoot(buffer)));
    }
    state.root = await state.rootPromise;
    return state.root;
  }

  async function loadDirectoryPage(state, page) {
    if (!state.pages.has(page.file)) {
      state.pages.set(page.file, fetchGzipArrayBuffer(new URL(directoryPagePath(state, page), baseUrl))
        .then(buffer => traceSpanSync("directory.parsePage", () => parseDirectoryPage(buffer, { packTable: state.meta.packTable }))));
    }
    return state.pages.get(page.file);
  }

  async function directoryEntryFromRoot(state, root, shard) {
    const page = findDirectoryPage(root, shard);
    if (!page) return null;
    const entries = await loadDirectoryPage(state, page);
    const entry = entries.get(shard);
    return entry ? { shard, entry } : null;
  }

  async function resolveDirectoryShard(value, state, baseDepth, maxDepth) {
    return traceSpan("directory.resolve", async () => {
      const root = await loadDirectoryRoot(state);
      for (let depth = maxDepth; depth >= baseDepth; depth--) {
        const resolved = await directoryEntryFromRoot(state, root, shardKey(value, depth));
        if (resolved) return resolved;
      }
      return null;
    });
  }

  // Returns the first leaf shards whose key ranges overlap `prefix`. Unlike
  // exact directory lookup this also handles a hot prefix whose base shard was
  // recursively split (for example `x|a` -> `x|aa`, `x|ab`, ...). Directory
  // pages are already key sorted, so autocomplete can stop as soon as it has
  // enough equal-weight authority rows without materializing the full tree.
  async function directoryEntriesForPrefix(state, prefix, maxEntries) {
    const root = await loadDirectoryRoot(state);
    const upper = prefix + String.fromCharCode(0xffff);
    let lo = 0;
    let hi = root.pages.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (root.pages[mid].last < prefix) lo = mid + 1;
      else hi = mid;
    }
    const start = Math.max(0, lo - 1); // an unsplit ancestor can sort before prefix
    const found = [];
    let pagesVisited = 0;
    let truncated = false;
    for (let index = start; index < root.pages.length; index++) {
      const page = root.pages[index];
      if (page.first > upper && !prefix.startsWith(page.first)) break;
      if (page.last < prefix && index !== start) continue;
      const entries = await loadDirectoryPage(state, page);
      pagesVisited++;
      for (const [shard, entry] of entries) {
        if (!prefix.startsWith(shard) && !shard.startsWith(prefix)) continue;
        if (found.length >= maxEntries) {
          truncated = true;
          break;
        }
        found.push({ shard, entry });
      }
      if (truncated) break;
    }
    return { entries: found, pagesVisited, truncated };
  }

  async function loadCodes() {
    if (codes) return codes;
    if (!codesPromise) codesPromise = fetchGzipArrayBuffer(new URL("codes.bin.gz", baseUrl)).then(parseCodes);
    codes = await codesPromise;
    return codes;
  }

  async function inflateObject(compressed, pointer, label) {
    if (verifyChecksums) await verifyBlockPointer(compressed, pointer, label);
    return traceSpan(`${traceLabelBucket(label)}.inflate`, () => inflateGzip(compressed));
  }

  async function inflateGroupItem(compressed, groupStart, item, label) {
    const start = item.entry.offset - groupStart;
    const end = start + item.entry.length;
    const slice = compressed.slice(start, end);
    return inflateObject(slice, item.entry, label);
  }

  function docValueField(field) {
    return docValues?.fields?.[field] || null;
  }

  function docValueSortField(field) {
    return docValueSorted?.fields?.[field] || null;
  }

  function docValueSortPageCacheKey(field, pageIndexValue) {
    return `${field}\u0000${pageIndexValue}`;
  }

  async function loadFacetDictionary(field) {
    if (Array.isArray(manifest.facets?.[field])) return manifest.facets[field];
    if (!facetDirectory && manifest.lazy_manifests?.facet_dictionaries) await ensureFacetDictionaryManifest();
    if (!facetDirectory && manifest.lazy_manifests?.full) await ensureFullManifest();
    if (!facetDirectory || !facetDictionaries?.fields?.[field]) return [];
    if (!facetDictionaryCache.has(field)) {
      const promise = (async () => {
        const root = await loadDirectoryRoot(facetDirectory);
        const resolved = await directoryEntryFromRoot(facetDirectory, root, field);
        if (!resolved) return [];
        const packs = facetDictionaries.packs || "facets/packs/";
        const buffer = await fetchRange(new URL(`${packs.replace(/\/?$/u, "/")}${resolved.entry.pack}`, baseUrl), resolved.entry.offset, resolved.entry.length);
        const inflated = await inflateObject(buffer, resolved.entry, `facet dictionary ${field}`);
        const values = traceSpanSync("facetDictionaries.parse", () => parseFacetDictionary(inflated));
        if (!manifest.facets) manifest.facets = {};
        manifest.facets[field] = values;
        return values;
      })();
      promise.catch(() => {
        facetDictionaryCache.delete(field);
      });
      facetDictionaryCache.set(field, promise);
    }
    return facetDictionaryCache.get(field);
  }

  async function ensureFacetDictionaries(filters) {
    const fields = Object.keys(filters?.facets || {});
    if (!fields.length) return;
    await Promise.all(fields.map(field => loadFacetDictionary(field)));
  }

  function chunkIndexForDoc(fieldMeta, doc) {
    const chunkSize = docValues?.chunk_size || manifest.total || 1;
    const index = Math.floor(doc / chunkSize);
    return fieldMeta?.chunks?.[index] ? index : -1;
  }

  function docValueLookupIndexForDoc(fieldMeta, doc) {
    const chunks = fieldMeta?.lookup_chunks || null;
    if (!chunks?.length) return -1;
    const chunkSize = docValues?.lookup_chunk_size || docValues?.chunk_size || manifest.total || 1;
    const index = Math.floor(doc / chunkSize);
    return chunks[index] ? index : -1;
  }

  function docValueChunkForRequest(fieldMeta, request) {
    return request.lookup ? fieldMeta?.lookup_chunks?.[request.index] : fieldMeta?.chunks?.[request.index];
  }

  function docValueCacheKey(field, index, lookup = false) {
    return `${field}\u0000${lookup ? "lookup" : "chunk"}\u0000${index}`;
  }

  async function loadDocValueChunks(requests) {
    return traceSpan("docValues.loadChunks", async () => {
      if (!docValues || !requests.length) return;
      const pending = [];
      for (const request of requests) {
        const fieldMeta = docValueField(request.field);
        const chunk = docValueChunkForRequest(fieldMeta, request);
        if (!chunk) continue;
        const key = docValueCacheKey(request.field, request.index, request.lookup);
        if (docValueCache.has(key)) continue;
        let resolve;
        let reject;
        const promise = new Promise((res, rej) => {
          resolve = res;
          reject = rej;
        });
        promise.catch(() => {});
        docValueCache.set(key, promise);
        pending.push({ field: request.field, index: request.index, lookup: Boolean(request.lookup), entry: chunk, resolve, reject });
      }
      await Promise.all(rangeGroups(pending).map(async (group) => {
        try {
          const compressed = await fetchRange(new URL(`doc-values/packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
          await Promise.all(group.items.map(async (item) => {
            const inflated = await inflateGroupItem(compressed, group.start, item, `doc-value ${item.field}:${item.index}`);
            const parsed = traceSpanSync("docValues.parse", () => parseDocValueChunk(inflated));
            docValueCache.set(docValueCacheKey(item.field, item.index, item.lookup), parsed);
            item.resolve(parsed);
          }));
        } catch (error) {
          for (const item of group.items) {
            docValueCache.delete(docValueCacheKey(item.field, item.index, item.lookup));
            item.reject(error);
          }
          throw error;
        }
      }));
    });
  }

  async function loadDocValueSortDirectory(field) {
    const meta = docValueSortField(field);
    if (!meta?.directory?.file) return null;
    if (!docValueSortDirectoryCache.has(field)) {
      const promise = fetchGzipArrayBuffer(new URL(meta.directory.file, baseUrl)).then(parseDocValueSortDirectory);
      promise.catch(() => {
        docValueSortDirectoryCache.delete(field);
      });
      docValueSortDirectoryCache.set(field, promise);
    }
    return docValueSortDirectoryCache.get(field);
  }

  async function loadDocValueSortPages(field, directory, pageIndexes, stats = null) {
    const wanted = [];
    const pending = [];
    for (const pageIndexValue of [...new Set(pageIndexes)]) {
      const page = directory.pages[pageIndexValue];
      if (!page) continue;
      wanted.push(pageIndexValue);
      const key = docValueSortPageCacheKey(field, pageIndexValue);
      if (docValueSortPageCache.has(key)) continue;
      let resolvePage;
      let rejectPage;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolvePage = resolvePromise;
        rejectPage = rejectPromise;
      });
      promise.catch(() => {});
      docValueSortPageCache.set(key, promise);
      pending.push({ field, pageIndex: pageIndexValue, entry: page, resolve: resolvePage, reject: rejectPage });
    }

    const groups = rangeGroups(pending, "docValueSortPages");
    if (stats) {
      stats.wanted += wanted.length;
      stats.fetched += pending.length;
      stats.groups += groups.length;
    }
    await Promise.all(groups.map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`doc-values/sorted-packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `doc-value sort page ${item.field}:${item.pageIndex}`);
          item.resolve(traceSpanSync("docValueSorted.decode", () => decodeDocValueSortPage(inflated, { name: item.field })));
        }));
      } catch (error) {
        for (const item of group.items) {
          docValueSortPageCache.delete(docValueSortPageCacheKey(item.field, item.pageIndex));
          item.reject(error);
        }
        throw error;
      }
    }));

    return Promise.all(wanted.map(pageIndexValue => docValueSortPageCache.get(docValueSortPageCacheKey(field, pageIndexValue))));
  }

  function geoFieldMeta(field) {
    return manifest.geo?.fields?.[field] || null;
  }

  function geoLeafPageCacheKey(field, leafIndex) {
    return `${field}\u0000${leafIndex}`;
  }

  async function loadGeoTreeRoot(field) {
    const meta = geoFieldMeta(field);
    if (!meta?.directory?.file) return null;
    if (!geoTreeRootCache.has(field)) {
      const promise = fetchGzipArrayBuffer(new URL(meta.directory.file, baseUrl))
        .then(buffer => parseGeoTreeRoot(buffer, manifest.block_filters || []));
      promise.catch(() => {
        geoTreeRootCache.delete(field);
      });
      geoTreeRootCache.set(field, promise);
    }
    return geoTreeRootCache.get(field);
  }

  // Generic batch loader for range-packed pages (geo leaf/branch). `entries`
  // carry their own object pointers and a stable
  // `index` used for caching.
  async function loadPackedPages({ field, entries, cache, cacheKey, decode, label, packDir, rangePlan, stats = null }) {
    const wanted = [];
    const pending = [];
    const seen = new Set();
    for (const entry of entries) {
      if (!entry || seen.has(entry.index)) continue;
      seen.add(entry.index);
      wanted.push(entry.index);
      const key = cacheKey(field, entry.index);
      if (cache.has(key)) continue;
      let resolvePage;
      let rejectPage;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolvePage = resolvePromise;
        rejectPage = rejectPromise;
      });
      promise.catch(() => {});
      cache.set(key, promise);
      pending.push({ field, pageIndex: entry.index, entry, resolve: resolvePage, reject: rejectPage });
    }

    const groups = rangeGroups(pending, rangePlan);
    if (stats) {
      stats.wanted += wanted.length;
      stats.fetched += pending.length;
      stats.groups += groups.length;
    }
    await Promise.all(groups.map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`${packDir}/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `${label} ${item.field}:${item.pageIndex}`);
          item.resolve(traceSpanSync(`${label}.decode`, () => decode(inflated)));
        }));
      } catch (error) {
        for (const item of group.items) {
          cache.delete(cacheKey(item.field, item.pageIndex));
          item.reject(error);
        }
        throw error;
      }
    }));

    return Promise.all(wanted.map(index => cache.get(cacheKey(field, index))));
  }

  async function loadGeoPages(field, entries, cache, cacheKey, decode, label, stats = null) {
    return loadPackedPages({
      field,
      entries,
      cache,
      cacheKey,
      decode,
      label,
      packDir: "geo/point-packs",
      rangePlan: "geoLeafPages",
      stats
    });
  }

  async function loadAuthorityLexiconRoot() {
    const meta = manifest.authority?.autocomplete;
    if (!meta?.directory?.file) return null;
    if (!authorityLexiconRootPromise) {
      authorityLexiconRootPromise = fetchGzipArrayBuffer(new URL(meta.directory.file, baseUrl)).then(parseAuthorityLexiconRoot);
      authorityLexiconRootPromise.catch(() => {
        authorityLexiconRootPromise = null;
      });
    }
    return authorityLexiconRootPromise;
  }

  async function loadAuthorityHotList(prefix, entry) {
    if (!entry?.file) return [];
    if (!authorityHotCache.has(prefix)) {
      const promise = fetchGzipArrayBuffer(new URL(entry.file, baseUrl)).then(parseAuthorityHotList);
      promise.catch(() => authorityHotCache.delete(prefix));
      authorityHotCache.set(prefix, promise);
    }
    return authorityHotCache.get(prefix);
  }

  async function loadGeoLeafPages(field, leaves, stats = null) {
    return loadGeoPages(
      field,
      leaves,
      geoLeafPageCache,
      geoLeafPageCacheKey,
      inflated => decodeGeoLeafPage(inflated, { name: field }),
      "geo leaf page",
      stats
    );
  }

  async function loadGeoBranchPages(field, root, branches, stats = null) {
    return loadGeoPages(
      field,
      branches,
      geoBranchPageCache,
      (fieldName, index) => `${fieldName} branch ${index}`,
      inflated => decodeGeoBranchPage(inflated, root.packTable, manifest.block_filters || [], { name: field }),
      "geo branch page",
      stats
    );
  }

  async function ensureDocValuesForDocs(fields, docs) {
    if (!docValues) await ensureDocValuesManifest();
    if (!docValues || !fields.length || !docs.length) return null;
    const requests = [];
    const seen = new Set();
    for (const field of fields) {
      const fieldMeta = docValueField(field);
      if (!fieldMeta) continue;
      for (const doc of docs) {
        const lookup = !!fieldMeta.lookup_chunks?.length;
        const index = lookup ? docValueLookupIndexForDoc(fieldMeta, doc) : chunkIndexForDoc(fieldMeta, doc);
        if (index < 0) continue;
        const key = docValueCacheKey(field, index, lookup);
        if (seen.has(key) || docValueCache.has(key)) continue;
        seen.add(key);
        requests.push({ field, index, lookup });
      }
    }
    await loadDocValueChunks(requests);
    return docValueStore;
  }

  async function ensureDocValueChunkIndexes(fields, indexes) {
    if (!docValues) await ensureDocValuesManifest();
    if (!docValues || !fields.length || !indexes.length) return null;
    const requests = [];
    for (const field of fields) {
      for (const index of indexes) requests.push({ field, index });
    }
    await loadDocValueChunks(requests);
    return docValueStore;
  }

  function filterBitmapField(field) {
    return filterBitmaps?.fields?.[field] || null;
  }

  function filterBitmapCacheKey(field, value) {
    return `${field}\u0000${value}`;
  }

  async function loadFilterBitmap(field, value) {
    if (!filterBitmaps) await ensureFilterBitmapManifest();
    const entry = filterBitmapField(field)?.values?.[String(value)];
    if (!entry) return null;
    const key = filterBitmapCacheKey(field, value);
    if (!filterBitmapCache.has(key)) {
      const promise = fetchRange(new URL(`filter-bitmaps/packs/${entry.pack}`, baseUrl), entry.offset, entry.length)
        .then(buffer => inflateObject(buffer, entry, `filter bitmap ${field}:${value}`))
        .then(buffer => traceSpanSync("filterBitmaps.parse", () => parseFilterBitmap(buffer)));
      promise.catch(() => {
        filterBitmapCache.delete(key);
      });
      filterBitmapCache.set(key, promise);
    }
    return filterBitmapCache.get(key);
  }

  async function filterBitmapStoreForPlan(filterPlan) {
    if (!filterPlan?.active) return null;
    if (!filterPlan.facets.length && !filterPlan.booleans.length) return null;
    if (!filterBitmaps) await ensureFilterBitmapManifest();
    if (!filterBitmaps?.fields) return null;
    const facets = new Map();
    const booleans = new Map();
    const covered = new Set();
    for (const [field, selected] of filterPlan.facets) {
      if (filterBitmapField(field)?.kind !== "facet") continue;
      const bitmaps = (await Promise.all([...selected].map(code => loadFilterBitmap(field, String(code))))).filter(Boolean);
      if (!bitmaps.length) continue;
      facets.set(field, bitmaps);
      covered.add(field);
    }
    for (const [field, expected] of filterPlan.booleans) {
      if (filterBitmapField(field)?.kind !== "boolean") continue;
      const bitmap = await loadFilterBitmap(field, expected ? "true" : "false");
      if (!bitmap) continue;
      booleans.set(field, bitmap);
      covered.add(field);
    }
    return covered.size ? { _filterBitmaps: true, facets, booleans, covered } : null;
  }

  function mergeValueStores(primary, bitmapStore) {
    if (!bitmapStore) return primary;
    if (!primary) return bitmapStore;
    return { ...primary, _filterBitmaps: bitmapStore };
  }

  async function valueStoreForFilterPlan(filterPlan, docs, omittedFields = []) {
    if (!filterPlan?.active) return null;
    const bitmapStore = docs.length <= FILTER_BITMAP_SPARSE_DOC_LIMIT
      ? await filterBitmapStoreForPlan(filterPlan)
      : null;
    const omitted = new Set(omittedFields);
    const bitmapCovered = bitmapStore?.covered || new Set();
    const fields = filterPlanFields(filterPlan).filter(field => !omitted.has(field) && !bitmapCovered.has(field));
    const store = mergeValueStores(await valueStoreForDocs(fields, docs), bitmapStore);
    // A resolved geo doc set needs no doc values, but callers treat a null
    // store as "nothing to filter", so hand back a marker store instead.
    if (!store && filterPlan.geo?.docSet) return { _geoDocSet: true };
    return store;
  }

  // Warms the doc-value chunk cache for a whole decoded block batch in one
  // pass, so the chunk range requests coalesce across blocks instead of
  // going out block by block (a filtered text query over scattered doc ids
  // otherwise pays one small fetch per posting block). Field selection
  // mirrors valueStoreForFilterPlan's sparse (bitmap-covered) case; the
  // per-block calls that follow keep their exact behavior and simply hit
  // the cache.
  async function prefetchFilterPlanDocValues(filterPlan, docsPerBlock) {
    if (!filterPlan?.active) return;
    const union = [];
    for (const docs of docsPerBlock) {
      if (docs) for (const doc of docs) union.push(doc);
    }
    if (!union.length) return;
    if (!docValues) await ensureDocValuesManifest();
    if (!docValues) return;
    const bitmapStore = await filterBitmapStoreForPlan(filterPlan);
    const bitmapCovered = bitmapStore?.covered || new Set();
    const fields = filterPlanFields(filterPlan).filter(field => !bitmapCovered.has(field));
    if (!fields.length) return;
    await ensureDocValuesForDocs(fields, union);
  }

  function docValue(field, doc) {
    const fieldMeta = docValueField(field);
    const lookupIndex = docValueLookupIndexForDoc(fieldMeta, doc);
    if (lookupIndex >= 0) {
      const lookupChunk = docValueCache.get(docValueCacheKey(field, lookupIndex, true));
      if (lookupChunk && typeof lookupChunk.then !== "function") return lookupChunk.values[doc - lookupChunk.start];
    }
    const index = chunkIndexForDoc(fieldMeta, doc);
    if (index < 0) return null;
    const chunk = docValueCache.get(docValueCacheKey(field, index));
    if (!chunk || typeof chunk.then === "function") return null;
    return chunk.values[doc - chunk.start];
  }

  function valueForDoc(valueStore, field, doc) {
    if (valueStore?._docValues) return docValue(field, doc);
    return valueStore?.[field]?.[doc];
  }

  function filterBitmapStore(valueStore) {
    if (valueStore?._filterBitmaps === true) return valueStore;
    return valueStore?._filterBitmaps || null;
  }

  function facetBitmapMatches(store, field, doc) {
    const bitmaps = store?.facets?.get(field);
    if (!bitmaps) return null;
    return bitmaps.some(bitmap => filterBitmapHas(bitmap, doc));
  }

  function booleanBitmapMatches(store, field, doc) {
    const bitmap = store?.booleans?.get(field);
    return bitmap ? filterBitmapHas(bitmap, doc) : null;
  }

  async function valueStoreForDocs(fields, docs) {
    if (!fields.length) return null;
    if (!docValues) await ensureDocValuesManifest();
    if (docValues) return ensureDocValuesForDocs(fields, docs);
    return loadCodes();
  }

  async function loadShards(shards) {
    const wanted = [];
    const pending = [];
    const unique = new Map();
    for (const item of shards) if (!unique.has(item.shard)) unique.set(item.shard, item);
    for (const { shard, entry } of unique.values()) {
      wanted.push(shard);
      if (shardCache.has(shard)) continue;
      if (!entry) continue;
      let resolve;
      let reject;
      const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
      });
      promise.catch(() => {});
      shardCache.set(shard, promise);
      pending.push({ shard, entry, resolve, reject });
    }

    await Promise.all(rangeGroups(pending).map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`terms/packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `posting segment ${item.shard}`);
          item.resolve(traceSpanSync("terms.parse", () => parsePostingSegment(inflated, manifest)));
        }));
      } catch (error) {
        for (const item of group.items) {
          shardCache.delete(item.shard);
          item.reject(error);
        }
        throw error;
      }
    }));

    const out = new Map();
    await Promise.all(wanted.map(async (shard) => {
      const data = await shardCache.get(shard);
      if (data) out.set(shard, data);
    }));
    return out;
  }

  async function loadAuthorityShards(shards) {
    const wanted = [];
    const pending = [];
    const unique = new Map();
    for (const item of shards) if (!unique.has(item.shard)) unique.set(item.shard, item);
    for (const { shard, entry } of unique.values()) {
      wanted.push(shard);
      if (authorityShardCache.has(shard)) continue;
      if (!entry) continue;
      let resolveAuthority;
      let rejectAuthority;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolveAuthority = resolvePromise;
        rejectAuthority = rejectPromise;
      });
      promise.catch(() => {});
      authorityShardCache.set(shard, promise);
      pending.push({ shard, entry, resolve: resolveAuthority, reject: rejectAuthority });
    }

    await Promise.all(rangeGroups(pending, "authority").map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`authority/packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `authority shard ${item.shard}`);
          item.resolve(traceSpanSync("authority.parse", () => parseAuthorityShard(inflated)));
        }));
      } catch (error) {
        for (const item of group.items) {
          authorityShardCache.delete(item.shard);
          item.reject(error);
        }
        throw error;
      }
    }));

    const out = new Map();
    await Promise.all(wanted.map(async (shard) => {
      const data = await authorityShardCache.get(shard);
      if (data) out.set(shard, data);
    }));
    return out;
  }

  async function termEntries(terms) {
    return traceSpan("terms.entries", async () => {
      const byShard = new Map();
      for (const term of terms) {
        const resolved = await resolveDirectoryShard(
          term,
          termDirectory,
          manifest.stats?.base_shard_depth || 3,
          manifest.stats?.max_shard_depth || manifest.stats?.base_shard_depth || 5
        );
        if (!resolved) continue;
        if (!byShard.has(resolved.shard)) byShard.set(resolved.shard, { shard: resolved.shard, entry: resolved.entry, terms: [] });
        byShard.get(resolved.shard).terms.push(term);
      }
      const loaded = await loadShards([...byShard.values()]);
      const out = [];
      for (const [shard, bucket] of byShard) {
        const data = loaded.get(shard);
        if (!data) continue;
        for (const term of bucket.terms) {
          const entry = data.terms.get(term);
          if (entry) out.push({ term, shard: data, shardName: shard, entry });
        }
      }
      return out;
    });
  }

  async function loadSegmentTerms(segment) {
    const key = segment.id || String(segment.ordinal);
    if (!segmentTermsCache.has(key)) {
      const path = segment.files?.terms?.path;
      segmentTermsCache.set(key, path
        ? traceFetch(traceBucketFromUrl(new URL(path, baseUrl)), () => fetchImpl(new URL(path, baseUrl))).then(response => {
            if (!response.ok) throw new Error(`Unable to fetch ${path}`);
            return response.arrayBuffer();
          }).then(buffer => traceSpanSync("segments.parseTerms", () => parseSegmentTerms(buffer)))
        : Promise.resolve(null));
    }
    return segmentTermsCache.get(key);
  }

  async function segmentTermEntries(terms) {
    const meta = await loadSegmentManifest();
    if (!meta?.published || !meta.segments?.length || options.segmentFanout === false) return null;
    const dictionaries = await Promise.all(meta.segments.map(segment => loadSegmentTerms(segment)));
    const entries = [];
    const dfs = new Map();
    for (const term of terms) {
      for (let ordinal = 0; ordinal < meta.segments.length; ordinal++) {
        const segment = meta.segments[ordinal];
        const entry = dictionaries[ordinal]?.terms?.get(term);
        if (!entry) continue;
        entries.push({ term, segment, segmentOrdinal: ordinal, entry });
        dfs.set(term, (dfs.get(term) || 0) + (entry.df || entry.count || 0));
      }
    }
    return { manifest: meta, entries, dfs };
  }

  async function decodeSegmentEntryPostings(item, df) {
    const path = item.segment.files?.postings?.path;
    if (!path) return new Int32Array(0);
    const key = `${item.segment.id || item.segmentOrdinal}\u0000${item.term}`;
    if (!segmentRowsCache.has(key)) {
      segmentRowsCache.set(key, fetchRange(new URL(path, baseUrl), item.entry.offset, item.entry.bytes)
        .then(buffer => decodeSegmentRows(buffer, item.entry, { df, total: manifest.total })));
    }
    return segmentRowsCache.get(key);
  }

  async function authorityEntries(keys) {
    if (!authorityDirectory || options.authority === false || !keys.length) return [];
    const byShard = new Map();
    for (const key of keys) {
      const resolved = await resolveDirectoryShard(
        key,
        authorityDirectory,
        manifest.authority?.base_shard_depth || manifest.stats?.base_shard_depth || 3,
        manifest.authority?.max_shard_depth || manifest.stats?.max_shard_depth || manifest.authority?.base_shard_depth || 5
      );
      if (!resolved) continue;
      if (!byShard.has(resolved.shard)) byShard.set(resolved.shard, { shard: resolved.shard, entry: resolved.entry, keys: [] });
      byShard.get(resolved.shard).keys.push(key);
    }
    const loaded = await loadAuthorityShards([...byShard.values()]);
    const out = [];
    for (const [shard, bucket] of byShard) {
      const data = loaded.get(shard);
      if (!data) continue;
      for (const key of bucket.keys) {
        const entry = data.entries.get(key);
        if (entry) out.push({ key, shardName: shard, entry });
      }
    }
    return out;
  }

  async function loadQueryBundle(key) {
    return traceSpan("queryBundles.load", async () => {
      if (!queryBundleDirectory || options.queryBundles === false) return null;
      if (!queryBundleCache.has(key)) {
        const promise = (async () => {
          const root = await loadDirectoryRoot(queryBundleDirectory);
          const resolved = await directoryEntryFromRoot(queryBundleDirectory, root, key);
          if (!resolved) return null;
          const buffer = await fetchRange(new URL(`bundles/packs/${resolved.entry.pack}`, baseUrl), resolved.entry.offset, resolved.entry.length);
          const inflated = await inflateObject(buffer, resolved.entry, `query bundle ${key}`);
          return {
            bundle: traceSpanSync("queryBundles.parse", () => parseQueryBundle(inflated, manifest)),
            bytes: resolved.entry.length
          };
        })();
        promise.catch(() => {
          queryBundleCache.delete(key);
        });
        queryBundleCache.set(key, promise);
      }
      return queryBundleCache.get(key);
    });
  }

  function bundleProvesTopK(bundle, k) {
    if (!bundle) return false;
    if (bundle.complete) return true;
    if (k > bundle.rows.length || !bundle.rows.length) return false;
    const boundary = bundle.rows[k - 1];
    if (!boundary) return false;
    if ((bundle.nextScoreBound || 0) < boundary[1]) return true;
    return (bundle.nextScoreBound || 0) === boundary[1]
      && bundle.nextTieDoc != null
      && bundle.nextTieDoc > boundary[0];
  }

  function queryBundleFilteredTopKProven(bundle, ranked, k) {
    if (bundle.complete) return true;
    if (ranked.length < k) return false;
    const boundary = ranked[k - 1];
    return !!boundary && (bundle.nextScoreBound || 0) < boundary[1];
  }

  function queryBundleFilterValueFields(bundle, filterPlan) {
    if (!filterPlan?.active || !bundle?.filterValues) return new Set();
    return new Set(filterPlanFields(filterPlan).filter(field => Object.prototype.hasOwnProperty.call(bundle.filterValues, field)));
  }

  function knownQueryBundleFilterValues(bundle, fields, doc) {
    const known = {};
    for (const field of fields) known[field] = bundle.filterValues[field]?.[doc];
    return known;
  }

  async function filterQueryBundleRowsWithEmbeddedValues({ bundle, candidateRows, docFilterPlan, k, embeddedFields }) {
    const batchSize = Math.max(8, Math.min(32, k));
    const ranked = [];
    let scanned = 0;
    let usedDocValues = false;
    const omittedFields = [...embeddedFields];
    for (let start = 0; start < candidateRows.length; start += batchSize) {
      const batch = candidateRows.slice(start, start + batchSize);
      const codeData = filterPlanFields(docFilterPlan).some(field => !embeddedFields.has(field))
        ? await valueStoreForFilterPlan(docFilterPlan, batch.map(row => row[0]), omittedFields)
        : null;
      if (codeData?._docValues) usedDocValues = true;
      scanned += batch.length;
      for (const row of batch) {
        if (passesFilterPlanWithKnown(row[0], codeData, docFilterPlan, knownQueryBundleFilterValues(bundle, embeddedFields, row[0]))) ranked.push(row);
      }
      if (queryBundleFilteredTopKProven(bundle, ranked, k)) {
        const valueSource = usedDocValues ? "queryBundleRows+docValues" : "queryBundleRows";
        return {
          ranked,
          topKProven: true,
          scanned,
          accepted: ranked.length,
          exhausted: scanned >= candidateRows.length,
          progressive: true,
          valueSource,
          usedDocValues,
          filterProof: valueSource
        };
      }
    }
    const valueSource = usedDocValues ? "queryBundleRows+docValues" : "queryBundleRows";
    return {
      ranked,
      topKProven: queryBundleFilteredTopKProven(bundle, ranked, k),
      scanned,
      accepted: ranked.length,
      exhausted: true,
      progressive: false,
      valueSource,
      usedDocValues,
      filterProof: ""
    };
  }

  async function filterQueryBundleRows({ bundle, candidateRows, docFilterPlan, k, summaryProvesFilters }) {
    if (summaryProvesFilters) {
      return {
        ranked: candidateRows,
        topKProven: queryBundleFilteredTopKProven(bundle, candidateRows, k),
        scanned: 0,
        accepted: candidateRows.length,
        exhausted: true,
        progressive: false,
        valueSource: "rowGroupSummary",
        filterProof: "rowGroupSummary"
      };
    }
    const embeddedFields = queryBundleFilterValueFields(bundle, docFilterPlan);
    if (embeddedFields.size) {
      return filterQueryBundleRowsWithEmbeddedValues({
        bundle,
        candidateRows,
        docFilterPlan,
        k,
        embeddedFields
      });
    }
    const batchSize = Math.max(8, Math.min(32, k));
    const ranked = [];
    let scanned = 0;
    for (let start = 0; start < candidateRows.length; start += batchSize) {
      const batch = candidateRows.slice(start, start + batchSize);
      const codeData = await valueStoreForFilterPlan(docFilterPlan, batch.map(row => row[0]));
      if (!codeData) {
        return {
          ranked,
          topKProven: false,
          scanned,
          accepted: ranked.length,
          exhausted: false,
          progressive: true,
          valueSource: "",
          usedDocValues: false,
          filterProof: ""
        };
      }
      scanned += batch.length;
      for (const row of batch) {
        if (passesFilterPlan(row[0], codeData, docFilterPlan)) ranked.push(row);
      }
      if (queryBundleFilteredTopKProven(bundle, ranked, k)) {
        return {
          ranked,
          topKProven: true,
          scanned,
          accepted: ranked.length,
          exhausted: scanned >= candidateRows.length,
          progressive: true,
          valueSource: "docValues",
          usedDocValues: true,
          filterProof: "progressiveDocValues"
        };
      }
    }
    return {
      ranked,
      topKProven: queryBundleFilteredTopKProven(bundle, ranked, k),
      scanned,
      accepted: ranked.length,
      exhausted: true,
      progressive: false,
      valueSource: "docValues",
      usedDocValues: true,
      filterProof: ""
    };
  }

  async function tryQueryBundleSearch({ page, size, baseTerms, filters, sortPlan, rerank, includeResults }) {
    return traceSpan("queryBundles.search", () => tryQueryBundleSearchInner({ page, size, baseTerms, filters, sortPlan, rerank, includeResults }));
  }

  async function tryQueryBundleSearchInner({ page, size, baseTerms, filters, sortPlan, rerank, includeResults }) {
    const offset = (page - 1) * size;
    const k = offset + size;
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    if (sortPlan || !baseTerms.length || k > (manifest.query_bundles?.max_rows || 0)) return null;
    if (manifest.query_bundles?.coverage !== "all-base-docs") return null;
    if (rerank !== false && dependencyTerms(baseTerms).length) return null;
    const hasFacetFilters = Object.keys(filters.facets || {}).length > 0;
    if (hasFacetFilters) await ensureFacetDictionaries(filters);
    const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
    const blockFilterPlan = hasFilters ? makeBlockFilterPlan(filters) : null;
    const filterFields = hasFilters ? filterPlanFields(docFilterPlan) : [];

    let lookups = 0;
    for (const plan of queryBundleKeysFromBaseTerms(baseTerms)) {
      lookups++;
      const loaded = await loadQueryBundle(plan.key);
      const bundle = loaded?.bundle;
      if (!bundle) continue;
      const candidateGroups = hasFilters && bundle.rowGroups?.length
        ? bundle.rowGroups.filter(group => blockMayPass({ filters: group.filters }, blockFilterPlan))
        : null;
      const candidateRows = candidateGroups
        ? candidateGroups.flatMap(group => bundle.rows.slice(group.rowStart, group.rowStart + group.rowCount))
        : bundle.rows;
      const summaryProvesFilters = hasFilters
        && candidateGroups
        && candidateGroups.length > 0
        && candidateGroups.every(group => blockDefinitelyPassesDocFilter({ filters: group.filters }, docFilterPlan));
      const filterResult = hasFilters
        ? await filterQueryBundleRows({ bundle, candidateRows, docFilterPlan, k, summaryProvesFilters })
        : null;
      const ranked = hasFilters ? filterResult.ranked : bundle.rows;
      if (hasFilters ? !filterResult.topKProven : !bundleProvesTopK(bundle, k)) continue;
      const rows = ranked.slice(offset, offset + size);
      const resultContext = { hasTextTerms: true, preferDocPages: "auto" };
      const results = await rowsToSearchResults(rows, resultContext, includeResults);
      const totalExact = !hasFilters || (bundle.complete && filterResult.exhausted);
      const total = !hasFilters
        ? bundle.total
        : totalExact
          ? ranked.length
          : Math.max(candidateRows.length, k);
      const filterValueSource = filterResult?.valueSource || "";
      const filterUsesDocValues = filterResult?.usedDocValues || filterValueSource === "docValues";
      return {
        total,
        page,
        size,
        approximate: !totalExact,
        results,
        stats: {
          exact: true,
          plannerLane: "queryBundleExact",
          topKProven: true,
          totalExact,
          tailExhausted: false,
          blocksDecoded: 0,
          postingsDecoded: 0,
          postingsAccepted: 0,
          skippedBlocks: 0,
          terms: plan.expandedTerms.length,
          shards: 0,
          queryBundleLookups: lookups,
          queryBundleHit: true,
          queryBundleFiltered: Boolean(hasFilters),
          queryBundleRows: bundle.rows.length,
          queryBundleRowGroups: bundle.rowGroups?.length || 0,
          queryBundleRowGroupsScanned: candidateGroups?.length ?? (bundle.rowGroups?.length || 0),
          queryBundleRowsAccepted: ranked.length,
          queryBundleTotal: hasFilters ? ranked.length : bundle.total,
          queryBundleBytes: loaded.bytes || 0,
          queryBundleComplete: bundle.complete,
          queryBundleFilterProof: filterResult?.filterProof || "",
          queryBundleFilterProgressive: Boolean(filterResult?.progressive),
          queryBundleFilterExhausted: filterResult?.exhausted ?? true,
          queryBundleFilterValueSource: filterValueSource,
          queryBundleFilterRowsScanned: hasFilters ? filterResult.scanned : 0,
          queryBundleFilterRowsAccepted: hasFilters ? filterResult.accepted : 0,
          docValueRowsScanned: hasFilters && filterUsesDocValues ? filterResult.scanned : 0,
          docValueRowsAccepted: hasFilters && filterUsesDocValues ? filterResult.accepted : 0,
          docPayloadLane: resultContext.docPayloadLane,
          docPayloadPages: resultContext.docPayloadPages,
          docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
          docPayloadAdaptive: resultContext.docPayloadAdaptive,
          docPayloadForced: resultContext.docPayloadForced,
          rerankCandidates: 0,
          dependencyFeatures: 0,
          dependencyTermsMatched: 0,
          dependencyPostingsScanned: 0,
          dependencyCandidateMatches: 0
        }
      };
    }
    return null;
  }

  function dependencyTerms(baseTerms) {
    if (baseTerms.length < 3) return [];
    const window = manifest.stats?.proximity_window || 5;
    const out = new Map();
    for (let i = 0; i < baseTerms.length; i++) {
      const end = Math.min(baseTerms.length, i + window + 1);
      for (let j = i + 1; j < end; j++) {
        const term = proximityTerm(baseTerms[i], baseTerms[j]);
        if (!term) continue;
        out.set(term, (out.get(term) || 0) + DEPENDENCY_SCORE_SCALE / Math.max(1, j - i));
      }
    }
    return [...out.entries()].map(([term, weight]) => ({ term, weight }));
  }

  async function rerankWithDependencies(ranked, baseTerms, candidateLimit = RERANK_CANDIDATES) {
    const features = dependencyTerms(baseTerms);
    const limit = Math.min(ranked.length, candidateLimit);
    const disabledStats = {
      rerankCandidates: limit,
      dependencyFeatures: features.length,
      dependencyTermsMatched: 0,
      dependencyPostingsScanned: 0,
      dependencyCandidateMatches: 0
    };
    if (!features.length || limit <= 1) return { ranked, stats: disabledStats };

    const head = ranked.slice(0, limit);
    const tail = ranked.slice(limit);
    const candidateScores = new Map(head.map(([doc, score], index) => [doc, { doc, score, originalRank: index }]));
    const featureWeights = new Map(features.map(feature => [feature.term, feature.weight]));
    let dependencyTermsMatched = 0;
    let dependencyPostingsScanned = 0;
    let dependencyCandidateMatches = 0;

    for (const { term, shard, entry } of await termEntries(features.map(feature => feature.term))) {
      const weight = featureWeights.get(term) || 0;
      if (!weight) continue;
      dependencyTermsMatched++;
      const postings = await decodeEntryPostings(shard, entry);
      dependencyPostingsScanned += postings.length / 2;
      for (let i = 0; i < postings.length; i += 2) {
        const candidate = candidateScores.get(postings[i]);
        if (candidate) {
          candidate.score += postings[i + 1] * weight;
          dependencyCandidateMatches++;
        }
      }
    }

    head.sort((a, b) => {
      const left = candidateScores.get(a[0]);
      const right = candidateScores.get(b[0]);
      return right.score - left.score || left.originalRank - right.originalRank || a[0] - b[0];
    });
    return {
      ranked: head.map(([doc]) => [doc, candidateScores.get(doc).score]).concat(tail),
      stats: {
        rerankCandidates: limit,
        dependencyFeatures: features.length,
        dependencyTermsMatched,
        dependencyPostingsScanned,
        dependencyCandidateMatches
      }
    };
  }

  function candidateLimitFor(baseTerms, k, rerank = true) {
    return rerank === false || !dependencyTerms(baseTerms).length
      ? k
      : Math.max(RERANK_CANDIDATES, k);
  }

  async function loadDocPointers(indexes) {
    if (!docPointers?.file) throw new Error("Rangefind index is missing dense doc pointers.");
    const order = docPointers.order || "doc-id";
    if (order !== "doc-id") throw new Error(`Unsupported Rangefind doc pointer order ${order}`);
    const pending = [];
    const unique = [...new Set(indexes)];
    for (const index of unique) {
      if (docPointerCache.has(index)) continue;
      let resolvePointer;
      let rejectPointer;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolvePointer = resolvePromise;
        rejectPointer = rejectPromise;
      });
      promise.catch(() => {});
      docPointerCache.set(index, promise);
      const offset = docPointers.dataOffset + index * docPointers.recordBytes;
      pending.push({
        index,
        entry: { pack: docPointers.file, offset, length: docPointers.recordBytes },
        resolve: resolvePointer,
        reject: rejectPointer
      });
    }

    await Promise.all(rangeGroups(pending, "docPointers").map(async (group) => {
      try {
        const buffer = await fetchRange(new URL(group.pack, baseUrl), group.start, group.end - group.start);
        for (const item of group.items) {
          const pointer = decodeDocPointerRecord(buffer, item.entry.offset - group.start, docPointers, docPointers.pack_table || []);
          item.resolve(pointer);
        }
      } catch (error) {
        for (const item of group.items) {
          docPointerCache.delete(item.index);
          item.reject(error);
        }
        throw error;
      }
    }));
  }

  async function loadPackedDocs(indexes) {
    const wanted = [];
    const pending = [];
    const unique = [...new Set(indexes)];
    await loadDocPointers(unique);
    for (const index of unique) {
      wanted.push(index);
      if (packedDocCache.has(index)) continue;
      let resolveDoc;
      let rejectDoc;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolveDoc = resolvePromise;
        rejectDoc = rejectPromise;
      });
      promise.catch(() => {});
      packedDocCache.set(index, promise);
      const entry = await docPointerCache.get(index);
      pending.push({ index, entry, resolve: resolveDoc, reject: rejectDoc });
    }

    await Promise.all(rangeGroups(pending, "docs").map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`docs/packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `doc ${item.index}`);
          item.resolve(traceSpanSync("docs.parse", () => JSON.parse(textDecoder.decode(new Uint8Array(inflated)))));
        }));
      } catch (error) {
        for (const item of group.items) {
          packedDocCache.delete(item.index);
          item.reject(error);
        }
        throw error;
      }
    }));

    return Promise.all(wanted.map(index => packedDocCache.get(index)));
  }

  function docPageSize() {
    return Math.max(1, Number(docPages?.page_size || 0));
  }

  function docPageIndex(index) {
    return Math.floor(index / docPageSize());
  }

  function decodeDocPagePayload(inflated, pageIndexValue) {
    if (docPages.encoding !== DOC_PAGE_ENCODING) throw new Error(`Unsupported Rangefind doc page encoding ${docPages.encoding || "unknown"}.`);
    return decodeDocPageColumns(inflated, docPages.fields || [], pageIndexValue * docPageSize());
  }

  function docPagePlan(indexes, context = {}) {
    const hardForced = context.preferDocPages === "force";
    const forced = hardForced || context.preferDocPages === true;
    const adaptive = options.textDocPageHydration !== false
      && (context.preferDocPages === "auto" || (context.preferDocPages == null && context.hasTextTerms));
    if (!docPages?.pointers?.file || (!forced && !adaptive)) return null;
    const unique = [...new Set(indexes)];
    if (!unique.length) return null;
    const pages = [...new Set(unique.map(docPageIndex))].sort((a, b) => a - b);
    const payloadDocs = pages.length * docPageSize();
    const configuredMaxOverfetchDocs = Math.max(1, Number(docPages.max_overfetch_docs || 16));
    const maxOverfetchDocs = forced
      ? configuredMaxOverfetchDocs
      : Math.max(1, Number(options.textDocPageMaxOverfetchDocs || configuredMaxOverfetchDocs));
    const maxPayloadDocs = Math.max(docPageSize(), unique.length * maxOverfetchDocs);
    if (!hardForced && payloadDocs > maxPayloadDocs) return null;
    if (!forced) {
      const pageFetchEstimate = pages.length * 2;
      const packedFetchEstimate = unique.length * 3;
      if (pageFetchEstimate >= packedFetchEstimate) return null;
    }
    return {
      pages,
      pageSize: docPageSize(),
      payloadDocs,
      uniqueDocs: unique.length,
      adaptive: !forced,
      forced: hardForced
    };
  }

  async function loadDocPagePointers(pageIndexes) {
    const pointerMeta = docPages?.pointers;
    if (!pointerMeta?.file) throw new Error("Rangefind index is missing dense doc page pointers.");
    const pending = [];
    const unique = [...new Set(pageIndexes)];
    for (const pageIndexValue of unique) {
      if (docPagePointerCache.has(pageIndexValue)) continue;
      if (pageIndexValue < 0 || pageIndexValue >= pointerMeta.count) throw new Error(`Rangefind doc page ${pageIndexValue} is outside the index.`);
      let resolvePointer;
      let rejectPointer;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolvePointer = resolvePromise;
        rejectPointer = rejectPromise;
      });
      promise.catch(() => {});
      docPagePointerCache.set(pageIndexValue, promise);
      const offset = pointerMeta.dataOffset + pageIndexValue * pointerMeta.recordBytes;
      pending.push({
        pageIndex: pageIndexValue,
        entry: { pack: pointerMeta.file, offset, length: pointerMeta.recordBytes },
        resolve: resolvePointer,
        reject: rejectPointer
      });
    }

    await Promise.all(rangeGroups(pending, "docPagePointers").map(async (group) => {
      try {
        const buffer = await fetchRange(new URL(group.pack, baseUrl), group.start, group.end - group.start);
        for (const item of group.items) {
          const pointer = decodeDocPagePointerRecord(buffer, item.entry.offset - group.start, pointerMeta, pointerMeta.pack_table || []);
          item.resolve(pointer);
        }
      } catch (error) {
        for (const item of group.items) {
          docPagePointerCache.delete(item.pageIndex);
          item.reject(error);
        }
        throw error;
      }
    }));
  }

  async function loadDocPages(indexes, plan) {
    await loadDocPagePointers(plan.pages);
    const pending = [];
    for (const pageIndexValue of plan.pages) {
      if (docPageCache.has(pageIndexValue)) continue;
      let resolvePage;
      let rejectPage;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolvePage = resolvePromise;
        rejectPage = rejectPromise;
      });
      promise.catch(() => {});
      docPageCache.set(pageIndexValue, promise);
      const entry = await docPagePointerCache.get(pageIndexValue);
      pending.push({ pageIndex: pageIndexValue, entry, resolve: resolvePage, reject: rejectPage });
    }

    await Promise.all(rangeGroups(pending, "docPages").map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`docs/page-packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `doc page ${item.pageIndex}`);
          item.resolve(traceSpanSync("docPages.decode", () => decodeDocPagePayload(inflated, item.pageIndex)));
        }));
      } catch (error) {
        for (const item of group.items) {
          docPageCache.delete(item.pageIndex);
          item.reject(error);
        }
        throw error;
      }
    }));

    return Promise.all(indexes.map(async (index) => {
      const pageIndexValue = docPageIndex(index);
      const page = await docPageCache.get(pageIndexValue);
      const doc = page[index - pageIndexValue * plan.pageSize];
      if (!doc) throw new Error(`Rangefind doc page ${pageIndexValue} is missing document ${index}.`);
      return doc;
    }));
  }

  async function loadDocs(indexes, context = {}) {
    const plan = docPagePlan(indexes, context);
    context.docPayloadLane = plan ? "docPages" : "packedDocs";
    context.docPayloadPages = plan?.pages.length || 0;
    context.docPayloadRows = indexes.length;
    context.docPayloadOverfetchDocs = plan?.payloadDocs || indexes.length;
    context.docPayloadAdaptive = Boolean(plan?.adaptive);
    context.docPayloadForced = Boolean(plan?.forced);
    return plan ? loadDocPages(indexes, plan) : loadPackedDocs(indexes);
  }

  async function rowsToResults(rows, context = {}) {
    return traceSpan("docs.hydrate", async () => {
      const docs = await loadDocs(rows.map(([index]) => index), context);
      return docs.map((doc, i) => ({ ...doc, score: rows[i][1], index: rows[i][0] }));
    });
  }

  async function rowsToSearchResults(rows, context = {}, includeResults = true) {
    if (includeResults === false) {
      context.docPayloadLane = "skipped";
      context.docPayloadPages = 0;
      context.docPayloadRows = rows.length;
      context.docPayloadOverfetchDocs = 0;
      context.docPayloadAdaptive = false;
      context.docPayloadForced = false;
      return rows.map(([index, score]) => ({ index, score }));
    }
    return rowsToResults(rows, context);
  }

  function normalizeRangeValue(value, field) {
    if (value == null || value === "") return null;
    if (field?.type === "date") {
      const time = typeof value === "number" ? value : Date.parse(String(value));
      return Number.isFinite(time) ? time : null;
    }
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function booleanCode(value) {
    if (value === true || value === 1 || value === "true" || value === "1") return 2;
    if (value === false || value === 0 || value === "false" || value === "0") return 1;
    return null;
  }

  async function loadPostingBlockBatch(requests, rangePlan = "postingBlocks") {
    return traceSpan("postingBlocks.load", async () => {
      const pending = [];
      let wanted = 0;
      for (const request of requests) {
        const owner = request.entry;
        const blockIndex = request.blockIndex;
        if (!owner.blockPostings) owner.blockPostings = new Map();
        wanted++;
        if (owner.blockPostings.has(blockIndex)) continue;
        const block = owner.blocks?.[blockIndex];
        if (!block?.range) {
          owner.blockPostings.set(blockIndex, Promise.resolve(new Int32Array(0)));
          continue;
        }
        let resolveBlock;
        let rejectBlock;
        const promise = new Promise((resolvePromise, rejectPromise) => {
          resolveBlock = resolvePromise;
          rejectBlock = rejectPromise;
        });
        promise.catch(() => {});
        owner.blockPostings.set(blockIndex, promise);
        pending.push({
          owner,
          blockIndex,
          entry: block.range,
          basePath: owner.blockPackBasePath || "terms/block-packs",
          resolve: resolveBlock,
          reject: rejectBlock
        });
      }

      const groups = [];
      const pendingByBasePath = new Map();
      for (const item of pending) {
        if (!pendingByBasePath.has(item.basePath)) pendingByBasePath.set(item.basePath, []);
        pendingByBasePath.get(item.basePath).push(item);
      }
      for (const [basePath, items] of pendingByBasePath) {
        for (const group of rangeGroups(items, rangePlan)) groups.push({ ...group, basePath });
      }
      await Promise.all(groups.map(async (group) => {
        try {
          const compressed = await fetchRange(new URL(`${group.basePath.replace(/\/?$/u, "/")}${group.pack}`, baseUrl), group.start, group.end - group.start);
          await Promise.all(group.items.map(async (item) => {
            const inflated = await inflateGroupItem(compressed, group.start, item, `posting block ${item.blockIndex}`);
            item.resolve(traceSpanSync("postingBlocks.decode", () => decodePostingBytes(
              inflated,
              item.owner.blocks?.[item.blockIndex]
            )));
          }));
        } catch (error) {
          for (const item of group.items) {
            item.owner.blockPostings.delete(item.blockIndex);
            item.reject(error);
          }
          throw error;
        }
      }));

      return { wanted, fetched: pending.length, groups: groups.length };
    });
  }

  async function loadPostingBlockByteBatch(requests, rangePlan = "postingBlocks") {
    return traceSpan("postingBlocks.loadBytes", async () => {
      const pending = [];
      let wanted = 0;
      for (const request of requests) {
        const owner = request.entry;
        const blockIndex = request.blockIndex;
        if (!owner.blockBytes) owner.blockBytes = new Map();
        wanted++;
        if (owner.blockBytes.has(blockIndex)) continue;
        const block = owner.blocks?.[blockIndex];
        if (!block?.range) {
          owner.blockBytes.set(blockIndex, Promise.resolve(new Uint8Array(0)));
          continue;
        }
        let resolveBlock;
        let rejectBlock;
        const promise = new Promise((resolvePromise, rejectPromise) => {
          resolveBlock = resolvePromise;
          rejectBlock = rejectPromise;
        });
        promise.catch(() => {});
        owner.blockBytes.set(blockIndex, promise);
        pending.push({ owner, blockIndex, entry: block.range, resolve: resolveBlock, reject: rejectBlock });
      }

      const groups = rangeGroups(pending, rangePlan);
      await Promise.all(groups.map(async (group) => {
        try {
          const compressed = await fetchRange(new URL(`terms/block-packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
          await Promise.all(group.items.map(async (item) => {
            item.resolve(await inflateGroupItem(compressed, group.start, item, `posting block ${item.blockIndex}`));
          }));
        } catch (error) {
          for (const item of group.items) {
            item.owner.blockBytes.delete(item.blockIndex);
            item.reject(error);
          }
          throw error;
        }
      }));

      return { wanted, fetched: pending.length, groups: groups.length };
    });
  }

  async function loadExternalPostingBlocks(entry, blockIndexes) {
    await loadPostingBlockBatch(blockIndexes.map(blockIndex => ({ entry, blockIndex })));
    return Promise.all(blockIndexes.map(blockIndex => entry.blockPostings.get(blockIndex)));
  }

  function postingBlockPrefetchIndexes(entry, blockIndex, prefetch, maxBlockExclusive = null) {
    const available = entry.blocks?.length || 0;
    const total = Math.min(available, maxBlockExclusive == null ? available : maxBlockExclusive);
    if (blockIndex < 0 || blockIndex >= total) return [];
    const blockPostings = entry.blockPostings;
    const out = [blockIndex];
    const addMissingRange = (start, count) => {
      const limit = Math.min(total, start + count);
      for (let i = start; i < limit; i++) {
        if (i !== blockIndex && !blockPostings?.has(i)) out.push(i);
      }
    };

    if (total <= prefetch * 2) {
      addMissingRange(blockIndex + 1, total - blockIndex - 1);
      return out;
    }

    if (!blockPostings?.has(blockIndex)) {
      addMissingRange(blockIndex + 1, prefetch - 1);
      return out;
    }

    let contiguousEnd = blockIndex + 1;
    while (contiguousEnd < total && blockPostings.has(contiguousEnd)) contiguousEnd++;
    const cachedAhead = contiguousEnd - blockIndex;
    const refillThreshold = Math.max(2, Math.floor(prefetch / 4));
    if (cachedAhead <= refillThreshold) addMissingRange(contiguousEnd, prefetch);
    return out;
  }

  async function decodeEntryBlock(shard, entry, blockIndex) {
    if (!entry.external) return decodePostingBlock(shard, entry, blockIndex);
    const blockIndexes = postingBlockPrefetchIndexes(entry, blockIndex, EXTERNAL_POSTING_BLOCK_PREFETCH);
    const rows = await loadExternalPostingBlocks(entry, blockIndexes);
    return rows[0] || new Int32Array(0);
  }

  async function decodeEntryPostings(shard, entry) {
    if (!entry.external) return decodePostings(shard, entry);
    if (entry.postings) return entry.postings;
    const blocks = await loadExternalPostingBlocks(entry, entry.blocks.map((_, index) => index));
    const length = blocks.reduce((sum, rows) => sum + rows.length, 0);
    const out = new Int32Array(length);
    let offset = 0;
    for (const rows of blocks) {
      out.set(rows, offset);
      offset += rows.length;
    }
    entry.postings = out;
    return out;
  }

  async function lookupEntryBlocks(shard, entry, blockIndexes, candidateDocs) {
    const indexes = [...new Set(blockIndexes || [])].filter(index => index >= 0 && index < (entry.blocks?.length || 0));
    if (!indexes.length || !candidateDocs?.size) return [];
    if (!entry.external) {
      return indexes.map(blockIndex => ({
        blockIndex,
        ...lookupPostingBlock(shard, entry, blockIndex, candidateDocs)
      }));
    }
    const uncached = indexes.filter(blockIndex => !entry.blockPostings?.has(blockIndex) && !entry.blockBytes?.has(blockIndex));
    await loadPostingBlockByteBatch(uncached.map(blockIndex => ({ entry, blockIndex })));
    return Promise.all(indexes.map(async blockIndex => {
      if (entry.blockPostings?.has(blockIndex)) {
        return {
          blockIndex,
          ...lookupDecodedPostingRows(await entry.blockPostings.get(blockIndex), candidateDocs)
        };
      }
      return {
        blockIndex,
        ...lookupPostingBytes(await entry.blockBytes.get(blockIndex), candidateDocs, entry.blocks?.[blockIndex])
      };
    }));
  }

  function normalizeGeoBoxE7(box) {
    const minLatE7 = latToE7(box.minLat);
    const maxLatE7 = latToE7(box.maxLat);
    const minLonE7 = lonToE7(box.minLon);
    const maxLonE7 = lonToE7(box.maxLon);
    if (minLatE7 == null || maxLatE7 == null || minLonE7 == null || maxLonE7 == null) {
      throw new Error("Rangefind: geo.box needs finite minLat, maxLat, minLon, and maxLon.");
    }
    if (minLatE7 > maxLatE7) throw new Error("Rangefind: geo.box has minLat above maxLat.");
    if (minLonE7 <= maxLonE7) return [{ minLatE7, maxLatE7, minLonE7, maxLonE7 }];
    // A box crossing the antimeridian splits into two straddling boxes.
    return [
      { minLatE7, maxLatE7, minLonE7, maxLonE7: 1800000000 },
      { minLatE7, maxLatE7, minLonE7: -1800000000, maxLonE7 }
    ];
  }

  function makeGeoPlan(geo) {
    if (!geo) return null;
    const fields = manifest.geo?.fields || {};
    const fieldNames = Object.keys(fields);
    if (!fieldNames.length) throw new Error("Rangefind: this index has no geo fields.");
    const field = geo.field || (fieldNames.length === 1 ? fieldNames[0] : "");
    const meta = fields[field];
    if (!meta) throw new Error(`Rangefind: unknown geo field "${geo.field || ""}".`);
    const plan = {
      field,
      meta,
      latField: meta.lat_component,
      lonField: meta.lon_component,
      boxes: [],
      near: null,
      sort: geo.sort === "distance",
      boost: null
    };
    if (geo.near) {
      const latE7 = latToE7(geo.near.lat);
      const lonE7 = lonToE7(geo.near.lon);
      if (latE7 == null || lonE7 == null) throw new Error("Rangefind: geo.near needs finite lat and lon.");
      const radiusMeters = Number(geo.near.radiusMeters);
      plan.near = {
        latE7,
        lonE7,
        radiusMeters: Number.isFinite(radiusMeters) && radiusMeters > 0 ? radiusMeters : null
      };
      if (plan.near.radiusMeters != null) {
        plan.boxes = boxesForRadiusE7(latE7, lonE7, plan.near.radiusMeters);
      }
    }
    if (geo.box) {
      if (plan.near) throw new Error("Rangefind: geo supports near or box, not both.");
      plan.boxes = normalizeGeoBoxE7(geo.box);
    }
    if (geo.boost) {
      const weight = Number(geo.boost.weight);
      const pivotMeters = Number(geo.boost.pivotMeters);
      plan.boost = {
        weight: Number.isFinite(weight) && weight > 0 ? weight : 1,
        pivotMeters: Number.isFinite(pivotMeters) && pivotMeters > 0 ? pivotMeters : 1000
      };
      if (!plan.near) throw new Error("Rangefind: geo.boost needs geo.near.");
    }
    if (plan.sort && !plan.near) throw new Error("Rangefind: geo.sort \"distance\" needs geo.near.");
    if (!plan.boxes.length && !plan.sort) throw new Error("Rangefind: geo needs near with radiusMeters, box, or sort \"distance\".");
    plan.filtered = plan.boxes.length > 0;
    return plan;
  }

  // Geo predicate over E7-rounded coordinates so the tree lane and the
  // doc-value verification lane accept exactly the same documents.
  function geoPointMatchesE7(geoPlan, latE7, lonE7) {
    if (geoPlan.near?.radiusMeters != null) {
      return haversineMetersE7(geoPlan.near.latE7, geoPlan.near.lonE7, latE7, lonE7) <= geoPlan.near.radiusMeters;
    }
    if (!geoPlan.boxes.length) return true;
    for (const box of geoPlan.boxes) {
      if (boxContainsPointE7(box, latE7, lonE7)) return true;
    }
    return false;
  }

  function geoLeafCandidate(geoPlan, leaf, blockFilterPlan = null) {
    // Per-leaf/per-branch filter summaries prove some cells cannot contain a
    // matching document, so their pages are never fetched.
    if (blockFilterPlan && leaf.filters && !blockMayPass(leaf, blockFilterPlan)) return null;
    const near = geoPlan.near;
    const radius = near?.radiusMeters ?? null;
    if (radius != null) {
      const minDist = pointToBoxDistanceMetersE7(near.latE7, near.lonE7, leaf);
      if (minDist > radius) return null;
      return {
        leaf,
        minDist,
        geoDefinite: pointToBoxMaxDistanceMetersE7(near.latE7, near.lonE7, leaf) <= radius
      };
    }
    if (geoPlan.boxes.length) {
      if (!geoPlan.boxes.some(box => boxIntersectsE7(box, leaf))) return null;
      return {
        leaf,
        minDist: near ? pointToBoxDistanceMetersE7(near.latE7, near.lonE7, leaf) : 0,
        geoDefinite: geoPlan.boxes.some(box => boxContainsBoxE7(box, leaf))
      };
    }
    return {
      leaf,
      minDist: near ? pointToBoxDistanceMetersE7(near.latE7, near.lonE7, leaf) : 0,
      geoDefinite: true
    };
  }

  // Streams `{ candidate, leafPage }` pairs for every tree leaf matching the
  // geo constraint. Single-level roots list leaves inline; two-level roots
  // route through lazily fetched branch pages. With `distanceSorted`, pairs
  // arrive in globally non-decreasing min-distance order (each leaf's min
  // distance is at least its branch's), which the nearest lane's early-stop
  // proof relies on.
  async function* geoCandidateLeafPages(geoPlan, root, distanceSorted, tracking, blockFilterPlan = null) {
    const { counters, leafFetchStats, branchFetchStats } = tracking;
    const firstBatch = Math.min(GEO_LEAF_PAGE_FIRST_BATCH_SIZE, geoLeafPageBatchSize);

    async function* yieldLeafBatches(candidates) {
      let batchSize = firstBatch;
      for (let position = 0; position < candidates.length;) {
        const batch = candidates.slice(position, position + batchSize);
        batchSize = Math.min(batchSize * 2, geoLeafPageBatchSize);
        const pages = await loadGeoLeafPages(geoPlan.field, batch.map(item => item.leaf), leafFetchStats);
        for (let i = 0; i < batch.length; i++) yield { candidate: batch[i], leafPage: pages[i] };
        position += batch.length;
      }
    }

    function leafCandidates(leaves) {
      const out = [];
      for (const leaf of leaves) {
        const candidate = geoLeafCandidate(geoPlan, leaf, blockFilterPlan);
        if (candidate) out.push(candidate);
      }
      counters.candidateLeaves += out.length;
      if (distanceSorted) out.sort((a, b) => a.minDist - b.minDist || a.leaf.index - b.leaf.index);
      return out;
    }

    if (root.levels === 1) {
      yield* yieldLeafBatches(leafCandidates(root.leaves));
      return;
    }

    const branchCandidates = [];
    for (const branch of root.branches) {
      const candidate = geoLeafCandidate(geoPlan, branch, blockFilterPlan);
      if (candidate) branchCandidates.push(candidate);
    }
    counters.candidateBranches = branchCandidates.length;

    if (!distanceSorted) {
      branchCandidates.sort((a, b) => a.leaf.index - b.leaf.index);
      let branchBatch = 1;
      for (let position = 0; position < branchCandidates.length;) {
        const batch = branchCandidates.slice(position, position + branchBatch);
        branchBatch = Math.min(branchBatch * 2, 8);
        const pages = await loadGeoBranchPages(geoPlan.field, root, batch.map(item => item.leaf), branchFetchStats);
        for (const page of pages) yield* yieldLeafBatches(leafCandidates(page.leaves));
        position += batch.length;
      }
      return;
    }

    // Best-first merge across branches: open branches lazily in min-distance
    // order and only emit leaves that no unopened branch could beat.
    branchCandidates.sort((a, b) => a.minDist - b.minDist || a.leaf.index - b.leaf.index);
    let nextBranch = 0;
    const pool = [];
    for (;;) {
      while (
        nextBranch < branchCandidates.length
        && (!pool.length || branchCandidates[nextBranch].minDist <= pool[0].minDist)
      ) {
        const [page] = await loadGeoBranchPages(
          geoPlan.field,
          root,
          [branchCandidates[nextBranch].leaf],
          branchFetchStats
        );
        for (const candidate of leafCandidates(page.leaves)) {
          let lo = 0;
          let hi = pool.length;
          while (lo < hi) {
            const mid = (lo + hi) >> 1;
            if (pool[mid].minDist < candidate.minDist) lo = mid + 1;
            else hi = mid;
          }
          pool.splice(lo, 0, candidate);
        }
        nextBranch++;
      }
      if (!pool.length) return;
      const limit = nextBranch < branchCandidates.length ? branchCandidates[nextBranch].minDist : Infinity;
      const emit = [];
      while (pool.length && pool[0].minDist <= limit && emit.length < geoLeafPageBatchSize) emit.push(pool.shift());
      if (!emit.length) continue;
      const pages = await loadGeoLeafPages(geoPlan.field, emit.map(item => item.leaf), leafFetchStats);
      for (let i = 0; i < emit.length; i++) yield { candidate: emit[i], leafPage: pages[i] };
    }
  }

  function geoTraversalTracking() {
    return {
      counters: { candidateLeaves: 0, candidateBranches: 0 },
      leafFetchStats: { wanted: 0, fetched: 0, groups: 0 },
      branchFetchStats: { wanted: 0, fetched: 0, groups: 0 }
    };
  }

  // For text queries with a selective geo filter, resolving the matching doc
  // ids from the tree once is far cheaper than verifying scattered text
  // candidates against lat/lon doc-value chunks.
  // Prices what the doc-value lane would transfer to verify `verifiedDocs`
  // text matches against the geo filter: scattered matches touch about one
  // chunk per doc per component field, saturating at the whole column.
  async function estimateGeoFilterDocValueBytes(geoPlan, verifiedDocs) {
    await ensureDocValuesManifest();
    if (!docValues) return null;
    let bytes = 0;
    for (const field of [geoPlan.latField, geoPlan.lonField]) {
      const chunks = docValueField(field)?.chunks || [];
      let chunkCount = 0;
      let chunkBytes = 0;
      for (const chunk of chunks) {
        if (!chunk) continue;
        chunkCount++;
        chunkBytes += chunk.length || 0;
      }
      if (!chunkCount) continue;
      bytes += Math.min(verifiedDocs, chunkCount) * (chunkBytes / chunkCount);
    }
    return bytes || null;
  }

  async function buildGeoDocSetIfCheap(geoPlan, textPostingsEstimate = null) {
    if (!geoPlan?.filtered) return;
    const root = await loadGeoTreeRoot(geoPlan.field);
    if (!root) return;
    const tracking = geoTraversalTracking();
    let candidatePoints = 0;
    let candidateLeafBytes = 0;
    if (root.levels === 1) {
      for (const leaf of root.leaves) {
        if (geoLeafCandidate(geoPlan, leaf)) {
          candidatePoints += leaf.count;
          candidateLeafBytes += leaf.length || 0;
        }
      }
    } else {
      // Branch counts alone are far too coarse an estimate (one branch can
      // cover a whole city). Branch pages cost a few KB each, so open the
      // candidates and estimate from leaf-level counts instead, unless the
      // constraint spans so many branches that it is clearly unselective.
      const candidateBranches = root.branches.filter(branch => geoLeafCandidate(geoPlan, branch));
      const maxBranchOpens = 32;
      if (candidateBranches.length > maxBranchOpens) return;
      const pages = await loadGeoBranchPages(geoPlan.field, root, candidateBranches, tracking.branchFetchStats);
      for (const page of pages) {
        for (const leaf of page.leaves) {
          if (geoLeafCandidate(geoPlan, leaf)) {
            candidatePoints += leaf.count;
            candidateLeafBytes += leaf.length || 0;
          }
        }
      }
    }
    if (candidatePoints > geoTextMaxCandidatePoints) {
      // Above the base cap the doc set can still be the cheaper verifier:
      // compare its exact leaf-page bytes against the doc-value chunks the
      // text lane would otherwise fetch.
      if (!(textPostingsEstimate > 0)) return;
      if (candidatePoints > GEO_TEXT_DOC_SET_HARD_CAP) return;
      const docValueBytes = await estimateGeoFilterDocValueBytes(geoPlan, textPostingsEstimate);
      if (!docValueBytes || candidateLeafBytes >= docValueBytes) return;
    }
    const docSet = new Set();
    for await (const { candidate, leafPage } of geoCandidateLeafPages(geoPlan, root, false, tracking)) {
      for (let i = 0; i < leafPage.count; i++) {
        if (candidate.geoDefinite || geoPointMatchesE7(geoPlan, leafPage.latsE7[i], leafPage.lonsE7[i])) {
          docSet.add(leafPage.docs[i]);
        }
      }
    }
    geoPlan.docSet = docSet;
    geoPlan.docSetStats = {
      candidateLeaves: tracking.counters.candidateLeaves,
      candidatePoints,
      candidateLeafBytes,
      matchedDocs: docSet.size,
      leafPagesFetched: tracking.leafFetchStats.fetched,
      leafPageFetchGroups: tracking.leafFetchStats.groups,
      branchPagesFetched: tracking.branchFetchStats.fetched
    };
  }

  function geoDocMatches(geoPlan, codeData, doc, known) {
    if (geoPlan.docSet) return geoPlan.docSet.has(doc);
    const lat = known && Object.prototype.hasOwnProperty.call(known, geoPlan.latField)
      ? known[geoPlan.latField]
      : valueForDoc(codeData, geoPlan.latField, doc);
    const lon = known && Object.prototype.hasOwnProperty.call(known, geoPlan.lonField)
      ? known[geoPlan.lonField]
      : valueForDoc(codeData, geoPlan.lonField, doc);
    const latE7 = latToE7(lat);
    const lonE7 = lonToE7(lon);
    if (latE7 == null || lonE7 == null) return false;
    return geoPointMatchesE7(geoPlan, latE7, lonE7);
  }

  function intersectNumberRange(current, min, max) {
    return {
      min: current?.min == null ? min : Math.max(Number(current.min), min),
      max: current?.max == null ? max : Math.min(Number(current.max), max)
    };
  }

  // Rewrites the filters object so geo constraints prune through the existing
  // numeric machinery (doc-value chunk summaries, sorted-page summaries, and
  // posting-block ranges). The exact predicate stays in the plan's geo clause.
  function withGeoFilters(filters, geoPlan) {
    if (!geoPlan?.filtered) {
      return geoPlan ? { ...filters, geo: geoPlan } : filters;
    }
    const numbers = { ...(filters.numbers || {}) };
    const margin = GEO_E7_PRUNE_MARGIN_DEGREES;
    const minLat = Math.min(...geoPlan.boxes.map(box => box.minLatE7)) / 1e7 - margin;
    const maxLat = Math.max(...geoPlan.boxes.map(box => box.maxLatE7)) / 1e7 + margin;
    numbers[geoPlan.latField] = intersectNumberRange(numbers[geoPlan.latField], minLat, maxLat);
    if (geoPlan.boxes.length === 1) {
      const box = geoPlan.boxes[0];
      numbers[geoPlan.lonField] = intersectNumberRange(
        numbers[geoPlan.lonField],
        box.minLonE7 / 1e7 - margin,
        box.maxLonE7 / 1e7 + margin
      );
    }
    return { ...filters, numbers, geo: geoPlan };
  }

  function makeDocFilterPlan(filters) {
    const facets = Object.entries(filters.facets || {})
      .map(([field, values]) => [field, selectedFacetCodes(manifest, field, new Set(values))])
      .filter(([, selected]) => selected?.size);
    const numbers = Object.entries(filters.numbers || {})
      .map(([field, range]) => [field, {
        min: normalizeRangeValue(range?.min, numberFields.get(field)),
        max: normalizeRangeValue(range?.max, numberFields.get(field))
      }])
      .filter(([, range]) => range.min != null || range.max != null);
    const booleans = Object.entries(filters.booleans || {})
      .map(([field, expected]) => {
        const code = booleanCode(expected);
        return [field, code === 2 ? true : code === 1 ? false : null];
      })
      .filter(([, value]) => value != null);
    const geo = filters.geo?.filtered ? filters.geo : null;
    // With a resolved geo doc set the per-doc lat/lon range checks are
    // redundant and would only force doc-value chunk fetches.
    const activeNumbers = geo?.docSet
      ? numbers.filter(([field]) => field !== geo.latField && field !== geo.lonField)
      : numbers;
    return {
      facets,
      numbers: activeNumbers,
      booleans,
      geo,
      active: facets.length > 0 || activeNumbers.length > 0 || booleans.length > 0 || !!geo
    };
  }

  function filterPlanFields(filterPlan) {
    if (!filterPlan?.active) return [];
    return [
      ...filterPlan.facets.map(([field]) => field),
      ...filterPlan.numbers.map(([field]) => field),
      ...filterPlan.booleans.map(([field]) => field),
      ...(filterPlan.geo && !filterPlan.geo.docSet ? [filterPlan.geo.latField, filterPlan.geo.lonField] : [])
    ];
  }

  function planFields(filterPlan, sortPlan) {
    return [...new Set([
      ...filterPlanFields(filterPlan),
      ...(sortPlan?.field ? [sortPlan.field] : [])
    ])];
  }

  function passesFilterPlan(doc, codeData, filterPlan) {
    if (!filterPlan?.active) return true;
    const bitmapStore = filterBitmapStore(codeData);
    for (const [field, selected] of filterPlan.facets) {
      const bitmapMatch = facetBitmapMatches(bitmapStore, field, doc);
      if (bitmapMatch != null) {
        if (!bitmapMatch) return false;
        continue;
      }
      if (!facetCodeMatches(valueForDoc(codeData, field, doc), selected)) return false;
    }
    for (const [field, range] of filterPlan.numbers) {
      const value = valueForDoc(codeData, field, doc);
      if (value == null) return false;
      if (range.min != null && value < range.min) return false;
      if (range.max != null && value > range.max) return false;
    }
    for (const [field, expected] of filterPlan.booleans) {
      const bitmapMatch = booleanBitmapMatches(bitmapStore, field, doc);
      if (bitmapMatch != null) {
        if (!bitmapMatch) return false;
        continue;
      }
      const value = valueForDoc(codeData, field, doc);
      if (value == null || value !== expected) return false;
    }
    if (filterPlan.geo && !geoDocMatches(filterPlan.geo, codeData, doc, null)) return false;
    return true;
  }

  function knownValueForDoc(known, field) {
    return Object.prototype.hasOwnProperty.call(known || {}, field) ? known[field] : undefined;
  }

  function valueForDocWithKnown(codeData, field, doc, known) {
    const knownValue = knownValueForDoc(known, field);
    return knownValue !== undefined ? knownValue : valueForDoc(codeData, field, doc);
  }

  function passesFilterPlanWithKnown(doc, codeData, filterPlan, known = {}) {
    if (!filterPlan?.active) return true;
    const bitmapStore = filterBitmapStore(codeData);
    for (const [field, selected] of filterPlan.facets) {
      const bitmapMatch = facetBitmapMatches(bitmapStore, field, doc);
      if (bitmapMatch != null) {
        if (!bitmapMatch) return false;
        continue;
      }
      if (!facetCodeMatches(valueForDocWithKnown(codeData, field, doc, known), selected)) return false;
    }
    for (const [field, range] of filterPlan.numbers) {
      const value = valueForDocWithKnown(codeData, field, doc, known);
      if (value == null) return false;
      if (range.min != null && value < range.min) return false;
      if (range.max != null && value > range.max) return false;
    }
    for (const [field, expected] of filterPlan.booleans) {
      const bitmapMatch = booleanBitmapMatches(bitmapStore, field, doc);
      if (bitmapMatch != null) {
        if (!bitmapMatch) return false;
        continue;
      }
      const value = valueForDocWithKnown(codeData, field, doc, known);
      if (value == null || value !== expected) return false;
    }
    if (filterPlan.geo && !geoDocMatches(filterPlan.geo, codeData, doc, known)) return false;
    return true;
  }

  function blockFacetMatches(summary, selected) {
    if (!selected?.size) return true;
    const words = summary?.words || [];
    for (const value of selected) {
      const word = Math.floor(value / 32);
      const bit = value % 32;
      if (words[word] & (2 ** bit)) return true;
    }
    return false;
  }

  function makeBlockFilterPlan(filters) {
    const facets = [];
    const numbers = [];
    const booleans = [];
    const unknownFields = [];
    for (const [field, values] of Object.entries(filters.facets || {})) {
      const selected = selectedFacetCodes(manifest, field, new Set(values));
      if (!selected?.size) continue;
      if (blockFilterFields.has(field)) facets.push([field, selected]);
      else unknownFields.push(field);
    }
    for (const [field, range] of Object.entries(filters.numbers || {})) {
      const normalized = {
        min: normalizeRangeValue(range?.min, numberFields.get(field)),
        max: normalizeRangeValue(range?.max, numberFields.get(field))
      };
      if (normalized.min == null && normalized.max == null) continue;
      if (blockFilterFields.has(field)) numbers.push([field, normalized]);
      else unknownFields.push(field);
    }
    for (const [field, value] of Object.entries(filters.booleans || {})) {
      const code = booleanCode(value);
      if (code == null) continue;
      if (blockFilterFields.has(field)) booleans.push([field, code]);
      else unknownFields.push(field);
    }
    return {
      facets,
      numbers,
      booleans,
      unknownFields,
      active: facets.length > 0 || numbers.length > 0 || booleans.length > 0
    };
  }

  function blockMayPass(block, filterPlan) {
    if (!filterPlan?.active) return true;
    for (const [field, selected] of filterPlan.facets) {
      const summary = block.filters?.[field];
      if (summary && !blockFacetMatches(summary, selected)) return false;
    }
    for (const [field, range] of filterPlan.numbers) {
      const summary = block.filters?.[field];
      if (!summary || summary.min == null || summary.max == null) continue;
      if (range.min != null && summary.max < range.min) return false;
      if (range.max != null && summary.min > range.max) return false;
    }
    for (const [field, value] of filterPlan.booleans) {
      const summary = block.filters?.[field];
      if (!summary || !summary.max) continue;
      if (summary.max < value || summary.min > value) return false;
    }
    return true;
  }

  function blockDefinitelyPassesDocFilter(block, filterPlan) {
    if (!filterPlan?.active) return true;
    if (filterPlan.facets.length) return false;
    // Geo predicates (doc set or haversine) always need per-doc checks.
    if (filterPlan.geo) return false;
    for (const [field, range] of filterPlan.numbers) {
      const summary = block.filters?.[field];
      if (!summary || summary.min == null || summary.max == null) return false;
      if (range.min != null && summary.min < range.min) return false;
      if (range.max != null && summary.max > range.max) return false;
    }
    for (const [field, expected] of filterPlan.booleans) {
      const summary = block.filters?.[field];
      const code = expected === true ? 2 : expected === false ? 1 : expected;
      if (!summary || summary.min !== code || summary.max !== code) return false;
    }
    return true;
  }

  // Multilingual base-plan selection: the detected query language may not
  // match the language the relevant documents were indexed in, and the base
  // terms drive minShouldMatch, phrase bundles, proximity, and typo
  // correction. When the primary plan's stems are missing from the term
  // directory, the alternate-language plan with the most indexed stems takes
  // over. Directory shard loads are cached, so the winning plan's lookups
  // are reused by the search itself.
  async function resolveQueryPlan(q) {
    const plan = analyzer.queryPlan(q);
    if (!plan.altPlans?.length || !plan.baseTerms.length) return plan;
    async function presentCount(baseTerms) {
      const entries = await termEntries(baseTerms);
      const found = new Set(entries.map(item => item.term));
      return baseTerms.filter(term => found.has(term)).length;
    }
    const primaryPresent = await presentCount(plan.baseTerms);
    if (primaryPresent >= Math.max(1, minShouldMatchFor(plan.baseTerms))) return plan;
    let best = plan;
    let bestPresent = primaryPresent;
    for (const alt of plan.altPlans) {
      const altPresent = await presentCount(alt.baseTerms);
      if (altPresent > bestPresent) {
        best = { ...plan, language: alt.language, analyzedTerms: alt.analyzedTerms, baseTerms: alt.baseTerms };
        bestPresent = altPresent;
      }
    }
    return best;
  }

  function collectEligibleScores(scores, hits, minShouldMatch) {
    return [...scores.entries()]
      .filter(([doc]) => (hits.get(doc) || 0) >= Math.max(1, minShouldMatch))
      .sort((a, b) => b[1] - a[1] || a[0] - b[0]);
  }

  // Bounded selection over the score map: the same ordering as
  // collectEligibleScores but keeping only the best k rows via a min-heap
  // (root = weakest kept row), so the repeated top-k proofs and the
  // early-terminated lanes never materialize or sort the full match set.
  // Returns { count, top } — count is the number of eligible rows seen.
  function topEligibleScores(scores, hits, minShouldMatch, k) {
    const need = Math.max(1, minShouldMatch);
    const limit = Math.max(1, Math.floor(k));
    const docs = [];
    const vals = [];
    let count = 0;
    // a "worse" than b under (score desc, doc asc): lower score, or same
    // score with a higher doc id.
    const worse = (aScore, aDoc, bScore, bDoc) => aScore < bScore || (aScore === bScore && aDoc > bDoc);
    for (const [doc, score] of scores) {
      if ((hits.get(doc) || 0) < need) continue;
      count++;
      if (docs.length < limit) {
        let i = docs.length;
        docs.push(doc);
        vals.push(score);
        while (i > 0) {
          const parent = (i - 1) >> 1;
          if (!worse(vals[i], docs[i], vals[parent], docs[parent])) break;
          [docs[i], docs[parent]] = [docs[parent], docs[i]];
          [vals[i], vals[parent]] = [vals[parent], vals[i]];
          i = parent;
        }
      } else if (worse(vals[0], docs[0], score, doc)) {
        docs[0] = doc;
        vals[0] = score;
        let i = 0;
        while (true) {
          const left = 2 * i + 1;
          const right = left + 1;
          let weakest = i;
          if (left < docs.length && worse(vals[left], docs[left], vals[weakest], docs[weakest])) weakest = left;
          if (right < docs.length && worse(vals[right], docs[right], vals[weakest], docs[weakest])) weakest = right;
          if (weakest === i) break;
          [docs[i], docs[weakest]] = [docs[weakest], docs[i]];
          [vals[i], vals[weakest]] = [vals[weakest], vals[i]];
          i = weakest;
        }
      }
    }
    const top = docs.map((doc, i) => [doc, vals[i]]).sort((a, b) => b[1] - a[1] || a[0] - b[0]);
    return { count, top };
  }

  function emptyTextSearchResponse({ page, size, terms, entries = [], missingBaseTerms = 0 }) {
    return {
      total: 0,
      page,
      size,
      results: [],
      approximate: false,
      stats: {
        exact: true,
        plannerLane: "empty",
        topKProven: true,
        totalExact: true,
        tailExhausted: true,
        blocksDecoded: 0,
        postingsDecoded: 0,
        postingsAccepted: 0,
        skippedBlocks: 0,
        terms: terms.length,
        shards: new Set(entries.map(item => item.shardName)).size,
        missingBaseTerms
      }
    };
  }

  function emptyTextCountResponse({ baseTerms = [], entries = [], missingBaseTerms = 0, lane = "countEmpty" }) {
    return {
      total: 0,
      totalExact: true,
      approximate: false,
      stats: {
        exact: true,
        plannerLane: lane,
        countLane: lane,
        totalExact: true,
        terms: baseTerms.length,
        baseTermCount: baseTerms.length,
        minShouldMatch: baseTerms.length ? minShouldMatchFor(baseTerms) : 0,
        termEntriesVisited: entries.length,
        shards: new Set(entries.map(item => item.shardName)).size,
        missingBaseTerms,
        blocksDecoded: 0,
        postingsDecoded: 0,
        postingsAccepted: 0
      }
    };
  }

  function counterArrayForBaseTerms(baseTerms) {
    if (baseTerms.length <= 255) return Uint8Array;
    if (baseTerms.length <= 65535) return Uint16Array;
    return Uint32Array;
  }

  async function runCountSearch({ baseTerms }) {
    return traceSpan("count.search", () => runCountSearchInner({ baseTerms }));
  }

  async function runCountSearchInner({ baseTerms }) {
    if (!baseTerms.length) return emptyTextCountResponse({ baseTerms, lane: "countTermless" });
    const minShouldMatch = minShouldMatchFor(baseTerms);
    const entries = await termEntries(baseTerms);
    const presentBaseTerms = new Set(entries.map(item => item.term));
    const missingBaseTerms = Math.max(0, baseTerms.length - presentBaseTerms.size);
    if (presentBaseTerms.size < Math.max(1, minShouldMatch)) {
      return emptyTextCountResponse({ baseTerms, entries, missingBaseTerms, lane: "countMissingTerms" });
    }

    const commonStats = {
      exact: true,
      totalExact: true,
      terms: baseTerms.length,
      baseTermCount: baseTerms.length,
      minShouldMatch,
      termEntriesVisited: entries.length,
      shards: new Set(entries.map(item => item.shardName)).size,
      missingBaseTerms
    };

    if (baseTerms.length === 1) {
      const total = entries.reduce((sum, item) => sum + (item.entry.df || item.entry.count || 0), 0);
      return {
        total,
        totalExact: true,
        approximate: false,
        stats: {
          ...commonStats,
          plannerLane: "countSingleTermDf",
          countLane: "countSingleTermDf",
          blocksDecoded: 0,
          postingsDecoded: 0,
          postingsAccepted: total
        }
      };
    }

    const CounterArray = counterArrayForBaseTerms(baseTerms);
    const counters = new CounterArray(Math.max(0, manifest.total || 0));
    let blocksDecoded = 0;
    let postingsDecoded = 0;
    await traceSpan("count.postings", async () => {
      for (const { shard, entry } of entries) {
        const postings = await decodeEntryPostings(shard, entry);
        blocksDecoded += entry.blocks?.length || 0;
        postingsDecoded += Math.floor(postings.length / 2);
        for (let index = 0; index < postings.length; index += 2) {
          const doc = postings[index];
          if (doc >= 0 && doc < counters.length) counters[doc]++;
        }
      }
    });

    const total = traceSpanSync("count.scan", () => {
      let matches = 0;
      for (let doc = 0; doc < counters.length; doc++) {
        if (counters[doc] >= minShouldMatch) matches++;
      }
      return matches;
    });

    return {
      total,
      totalExact: true,
      approximate: false,
      stats: {
        ...commonStats,
        plannerLane: "countPostingCounter",
        countLane: "countPostingCounter",
        blocksDecoded,
        postingsDecoded,
        postingsAccepted: total
      }
    };
  }

  // Exact set of docs matching the analyzed query terms under the same
  // minShouldMatch rule as count(). Used by text + distance sort, where the
  // geo tree orders results and text match is a membership predicate.
  // Returns null when the posting volume exceeds `geoTextSortMaxDf`.
  async function collectTextMatchDocs(baseTerms) {
    const minShouldMatch = minShouldMatchFor(baseTerms);
    const entries = await termEntries(baseTerms);
    const presentBaseTerms = new Set(entries.map(item => item.term));
    const stats = {
      terms: baseTerms.length,
      minShouldMatch,
      termEntriesVisited: entries.length,
      blocksDecoded: 0,
      postingsDecoded: 0
    };
    if (presentBaseTerms.size < Math.max(1, minShouldMatch)) {
      return { docs: new Set(), stats };
    }
    const totalDf = entries.reduce((sum, item) => sum + (item.entry.df || item.entry.count || 0), 0);
    if (totalDf > geoTextSortMaxDf) return null;
    const CounterArray = counterArrayForBaseTerms(baseTerms);
    const counters = new CounterArray(Math.max(0, manifest.total || 0));
    for (const { shard, entry } of entries) {
      const postings = await decodeEntryPostings(shard, entry);
      stats.blocksDecoded += entry.blocks?.length || 0;
      stats.postingsDecoded += Math.floor(postings.length / 2);
      for (let index = 0; index < postings.length; index += 2) {
        const doc = postings[index];
        if (doc >= 0 && doc < counters.length) counters[doc]++;
      }
    }
    const docs = new Set();
    for (let doc = 0; doc < counters.length; doc++) {
      if (counters[doc] >= minShouldMatch) docs.add(doc);
    }
    return { docs, stats };
  }

  function makeSortPlan(sort) {
    if (!sort) return null;
    const field = typeof sort === "string" ? sort.replace(/^-/, "") : sort.field;
    if (!field) return null;
    const order = typeof sort === "string" && sort.startsWith("-")
      ? "desc"
      : String(sort.order || sort.direction || "asc").toLowerCase();
    if (!numberFields.has(field) && !booleanFields.has(field)) return null;
    return { field, desc: order === "desc" };
  }

  function chunkFacetMayPass(chunk, selected) {
    if (!selected?.size || !chunk) return true;
    if (!Array.isArray(chunk.words)) return true;
    for (const value of selected) {
      const word = Math.floor(value / 32);
      const bit = value % 32;
      if ((chunk.words[word] || 0) & (2 ** bit)) return true;
    }
    return false;
  }

  function chunkMayPass(index, filterPlan) {
    if (!docValues || !filterPlan?.active) return true;
    for (const [field, selected] of filterPlan.facets) {
      if (!chunkFacetMayPass(docValueField(field)?.chunks?.[index], selected)) return false;
    }
    for (const [field, range] of filterPlan.numbers) {
      const chunk = docValueField(field)?.chunks?.[index];
      if (!chunk || chunk.min == null || chunk.max == null) return false;
      if (range.min != null && chunk.max < range.min) return false;
      if (range.max != null && chunk.min > range.max) return false;
    }
    for (const [field, expected] of filterPlan.booleans) {
      const chunk = docValueField(field)?.chunks?.[index];
      const code = expected ? 2 : 1;
      if (!chunk || chunk.min == null || chunk.max == null) return false;
      if (chunk.max < code || chunk.min > code) return false;
    }
    return true;
  }

  function candidateDocValueChunks(filterPlan) {
    if (!docValues) return [];
    const count = Math.ceil(manifest.total / Math.max(1, docValues.chunk_size || manifest.total || 1));
    const out = [];
    for (let index = 0; index < count; index++) if (chunkMayPass(index, filterPlan)) out.push(index);
    return out;
  }

  function booleanSummaryCode(value) {
    return value ? 2 : 1;
  }

  function pageSummaryForField(page, field, sortedField) {
    if (Object.prototype.hasOwnProperty.call(page.summaries || {}, field)) return page.summaries[field];
    return field === sortedField && Number.isFinite(page.min) && Number.isFinite(page.max) ? { min: page.min, max: page.max } : null;
  }

  function pageMayPassDocValueFilter(page, filterPlan, sortedField) {
    if (!filterPlan?.active) return true;
    for (const [field, range] of filterPlan.numbers) {
      const summary = pageSummaryForField(page, field, sortedField);
      if (!summary || summary.min == null || summary.max == null) return false;
      if (range.min != null && summary.max < range.min) return false;
      if (range.max != null && summary.min > range.max) return false;
    }
    for (const [field, expected] of filterPlan.booleans) {
      const summary = pageSummaryForField(page, field, sortedField);
      const code = booleanSummaryCode(expected);
      if (!summary || summary.min == null || summary.max == null) return false;
      if (summary.max < code || summary.min > code) return false;
    }
    return true;
  }

  function pageDefinitelyPassesDocValueFilter(page, filterPlan, sortedField) {
    if (!filterPlan?.active) return true;
    if (filterPlan.facets.length) return false;
    // Geo clauses need per-doc verification (haversine or E7 box rounding).
    if (filterPlan.geo) return false;
    for (const [field, range] of filterPlan.numbers) {
      const summary = pageSummaryForField(page, field, sortedField);
      if (!summary || summary.min == null || summary.max == null) return false;
      if (range.min != null && summary.min < range.min) return false;
      if (range.max != null && summary.max > range.max) return false;
    }
    for (const [field, expected] of filterPlan.booleans) {
      const summary = pageSummaryForField(page, field, sortedField);
      const code = booleanSummaryCode(expected);
      if (!summary || summary.min == null || summary.max == null) return false;
      if (summary.min !== code || summary.max !== code) return false;
    }
    return true;
  }

  function sortedDirectoryPages(directory, desc, filterPlan) {
    return directory.pages
      .filter(page => pageMayPassDocValueFilter(page, filterPlan, directory.field.name))
      .sort((a, b) => (
        desc
          ? b.max - a.max || a.rankStart - b.rankStart
          : a.min - b.min || a.rankStart - b.rankStart
      ));
  }

  function sortedPageRows(page, desc) {
    return page.rows.slice().sort((a, b) => (
      desc
        ? b.sortValue - a.sortValue || a.doc - b.doc
        : a.sortValue - b.sortValue || a.doc - b.doc
    ));
  }

  function mergeSortPageFilter(filters, field, page) {
    const numbers = { ...(filters.numbers || {}) };
    const current = numbers[field] || {};
    const fieldMeta = numberFields.get(field);
    const currentMin = normalizeRangeValue(current.min, fieldMeta);
    const currentMax = normalizeRangeValue(current.max, fieldMeta);
    const pageMin = Number.isFinite(page?.min) ? page.min : null;
    const pageMax = Number.isFinite(page?.max) ? page.max : null;
    const min = currentMin == null ? pageMin : pageMin == null ? currentMin : Math.max(currentMin, pageMin);
    const max = currentMax == null ? pageMax : pageMax == null ? currentMax : Math.min(currentMax, pageMax);
    numbers[field] = { min, max };
    return {
      facets: filters.facets || {},
      numbers,
      booleans: filters.booleans || {}
    };
  }

  function blockOverlapsDocSpan(block, minDoc, maxDoc) {
    if (minDoc == null || maxDoc == null) return true;
    if (!Number.isFinite(block?.docMin) || !Number.isFinite(block?.docMax)) return true;
    return block.docMax >= minDoc && block.docMin <= maxDoc;
  }

  function candidateDocsOverlapBlock(candidateDocs, block) {
    if (!candidateDocs?.size) return true;
    if (!Number.isFinite(block?.docMin) || !Number.isFinite(block?.docMax)) return true;
    for (const doc of candidateDocs) {
      if (doc >= block.docMin && doc <= block.docMax) return true;
    }
    return false;
  }

  function sortedTextCandidateBlockIndexes(entry, filterPlan, candidateDocs = null) {
    const indexes = [];
    let consideredBlocks = 0;
    let skippedBlocks = 0;
    let consideredSuperblocks = 0;
    let skippedSuperblocks = 0;
    const blocks = entry.blocks || [];
    const superblocks = entry.superblocks || [];
    if (superblocks.length) {
      for (const superblock of superblocks) {
        consideredSuperblocks++;
        const first = Math.max(0, superblock.firstBlock || 0);
        const end = Math.min(blocks.length, first + (superblock.blockCount || 0));
        if (!blockMayPass(superblock, filterPlan) || !candidateDocsOverlapBlock(candidateDocs, superblock)) {
          skippedSuperblocks++;
          skippedBlocks += Math.max(0, end - first);
          continue;
        }
        for (let blockIndex = first; blockIndex < end; blockIndex++) {
          consideredBlocks++;
          if (blockMayPass(blocks[blockIndex], filterPlan) && candidateDocsOverlapBlock(candidateDocs, blocks[blockIndex])) indexes.push(blockIndex);
          else skippedBlocks++;
        }
      }
      return { indexes, consideredBlocks, skippedBlocks, consideredSuperblocks, skippedSuperblocks };
    }
    for (let blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
      consideredBlocks++;
      if (blockMayPass(blocks[blockIndex], filterPlan) && candidateDocsOverlapBlock(candidateDocs, blocks[blockIndex])) indexes.push(blockIndex);
      else skippedBlocks++;
    }
    return { indexes, consideredBlocks, skippedBlocks, consideredSuperblocks, skippedSuperblocks };
  }

  function compareKnownSortRows(a, b, sortPlan, sortValues) {
    const left = sortValues.get(a[0]);
    const right = sortValues.get(b[0]);
    const leftMissing = left == null;
    const rightMissing = right == null;
    if (leftMissing || rightMissing) {
      if (leftMissing && rightMissing) return b[1] - a[1] || a[0] - b[0];
      return leftMissing ? 1 : -1;
    }
    if (left !== right) return sortPlan.desc ? right - left : left - right;
    return b[1] - a[1] || a[0] - b[0];
  }

  function nextSortPageCanTie(page, boundarySortValue, desc) {
    if (!page || boundarySortValue == null) return false;
    return desc ? page.max >= boundarySortValue : page.min <= boundarySortValue;
  }

  function sortReplicaPlanKey(sortPlan) {
    return sortPlan?.field ? `${sortPlan.field}:${sortPlan.desc ? "desc" : "asc"}` : "";
  }

  function sortReplicaForPlan(sortPlan) {
    const replicas = manifest.sort_replicas?.replicas || {};
    return replicas[sortReplicaPlanKey(sortPlan)] || null;
  }

  function sortReplicaDirectoryState(replica) {
    const key = replica?.id || replica?.key;
    if (!key || !replica?.terms?.directory) return null;
    if (!sortReplicaDirectoryCache.has(key)) sortReplicaDirectoryCache.set(key, createDirectoryState(replica.terms.directory));
    return sortReplicaDirectoryCache.get(key);
  }

  function sortReplicaTermsPath(replica) {
    return (replica.terms?.packs_path || `sort-replicas/${replica.id}/terms/packs`).replace(/\/?$/u, "/");
  }

  function sortReplicaBlockPath(replica) {
    return (replica.terms?.block_packs_path || `sort-replicas/${replica.id}/terms/block-packs`).replace(/\/?$/u, "/");
  }

  function sortReplicaRankPath(replica) {
    return (replica.rank_map?.packs_path || `sort-replicas/${replica.id}/rank-packs`).replace(/\/?$/u, "/");
  }

  function sortReplicaPostingManifest(replica) {
    return {
      ...manifest,
      block_filters: [],
      object_store: {
        ...(manifest.object_store || {}),
        pack_table: {
          ...(manifest.object_store?.pack_table || {}),
          postingBlocks: replica.terms?.block_pack_table || []
        }
      }
    };
  }

  async function loadSortReplicaShards(replica, shards) {
    const wanted = [];
    const pending = [];
    const unique = new Map();
    for (const item of shards) if (!unique.has(item.shard)) unique.set(item.shard, item);
    for (const { shard, entry } of unique.values()) {
      wanted.push(shard);
      const cacheKey = `${replica.id}\u0000${shard}`;
      if (sortReplicaShardCache.has(cacheKey)) continue;
      if (!entry) continue;
      let resolveShard;
      let rejectShard;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolveShard = resolvePromise;
        rejectShard = rejectPromise;
      });
      promise.catch(() => {});
      sortReplicaShardCache.set(cacheKey, promise);
      pending.push({ shard, cacheKey, entry, resolve: resolveShard, reject: rejectShard });
    }

    const parseManifest = sortReplicaPostingManifest(replica);
    const blockBasePath = sortReplicaBlockPath(replica);
    await Promise.all(rangeGroups(pending).map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`${sortReplicaTermsPath(replica)}${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `sort replica segment ${replica.id}:${item.shard}`);
          const parsed = traceSpanSync("sortReplicas.parseTerms", () => parsePostingSegment(inflated, parseManifest));
          for (const entry of parsed.terms.values()) {
            entry.blockPackBasePath = blockBasePath;
            entry.sortReplicaId = replica.id;
          }
          item.resolve(parsed);
        }));
      } catch (error) {
        for (const item of group.items) {
          sortReplicaShardCache.delete(item.cacheKey);
          item.reject(error);
        }
        throw error;
      }
    }));

    const out = new Map();
    await Promise.all(wanted.map(async (shard) => {
      const data = await sortReplicaShardCache.get(`${replica.id}\u0000${shard}`);
      if (data) out.set(shard, data);
    }));
    return out;
  }

  async function sortReplicaTermEntries(terms, replica) {
    return traceSpan("sortReplicas.entries", async () => {
      const directory = sortReplicaDirectoryState(replica);
      if (!directory) return [];
      const byShard = new Map();
      for (const term of terms) {
        const resolved = await resolveDirectoryShard(
          term,
          directory,
          replica.base_shard_depth || manifest.stats?.base_shard_depth || 3,
          replica.max_shard_depth || manifest.stats?.max_shard_depth || manifest.stats?.base_shard_depth || 5
        );
        if (!resolved) continue;
        if (!byShard.has(resolved.shard)) byShard.set(resolved.shard, { shard: resolved.shard, entry: resolved.entry, terms: [] });
        byShard.get(resolved.shard).terms.push(term);
      }
      const loaded = await loadSortReplicaShards(replica, [...byShard.values()]);
      const out = [];
      for (const [shard, bucket] of byShard) {
        const data = loaded.get(shard);
        if (!data) continue;
        for (const term of bucket.terms) {
          const entry = data.terms.get(term);
          if (entry) out.push({ term, shard: data, shardName: shard, entry });
        }
      }
      return out;
    });
  }

  function decodeSortReplicaRankChunk(buffer, meta) {
    const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const count = Math.max(0, Math.floor(Number(meta.count || 0)));
    const docs = new Uint32Array(count);
    const values = new Float64Array(count);
    for (let index = 0, offset = 0; index < count; index++, offset += 12) {
      docs[index] = view.getUint32(offset, true);
      values[index] = view.getFloat64(offset + 4, true);
    }
    return { start: meta.start || 0, count, docs, values };
  }

  function sortReplicaRankCacheKey(replica, chunkIndex) {
    return `${replica.id}\u0000${chunkIndex}`;
  }

  async function loadSortReplicaRankChunks(replica, chunkIndexes, stats = null) {
    const wanted = [];
    const pending = [];
    for (const chunkIndex of [...new Set(chunkIndexes)]) {
      const chunk = replica.rank_map?.chunks?.[chunkIndex];
      if (!chunk) continue;
      wanted.push(chunkIndex);
      const key = sortReplicaRankCacheKey(replica, chunkIndex);
      if (sortReplicaRankCache.has(key)) continue;
      let resolveChunk;
      let rejectChunk;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolveChunk = resolvePromise;
        rejectChunk = rejectPromise;
      });
      promise.catch(() => {});
      sortReplicaRankCache.set(key, promise);
      pending.push({ chunkIndex, key, entry: chunk, resolve: resolveChunk, reject: rejectChunk });
    }
    const groups = rangeGroups(pending, "sortReplicaRankMaps");
    if (stats) {
      stats.rankChunksWanted += wanted.length;
      stats.rankChunksFetched += pending.length;
      stats.rankChunkFetchGroups += groups.length;
    }
    await Promise.all(groups.map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`${sortReplicaRankPath(replica)}${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `sort replica rank ${replica.id}:${item.chunkIndex}`);
          item.resolve(traceSpanSync("sortReplicas.decodeRankMap", () => decodeSortReplicaRankChunk(inflated, item.entry)));
        }));
      } catch (error) {
        for (const item of group.items) {
          sortReplicaRankCache.delete(item.key);
          item.reject(error);
        }
        throw error;
      }
    }));
  }

  async function sortReplicaRankRows(replica, ranks, stats = null) {
    const total = Math.max(0, Number(replica.rank_map?.total ?? replica.total ?? 0));
    const chunkSize = Math.max(1, Number(replica.rank_map?.chunk_size || total || 1));
    const wantedRanks = [...new Set(ranks || [])].filter(rank => rank >= 0 && rank < total);
    if (stats) stats.rankLookups += wantedRanks.length;
    const chunkIndexes = wantedRanks.map(rank => Math.floor(rank / chunkSize));
    await loadSortReplicaRankChunks(replica, chunkIndexes, stats);
    const out = new Map();
    await Promise.all([...new Set(chunkIndexes)].map(async (chunkIndex) => sortReplicaRankCache.get(sortReplicaRankCacheKey(replica, chunkIndex))));
    for (const rank of wantedRanks) {
      const chunkIndex = Math.floor(rank / chunkSize);
      const chunk = await sortReplicaRankCache.get(sortReplicaRankCacheKey(replica, chunkIndex));
      if (!chunk) continue;
      const offset = rank - chunk.start;
      if (offset < 0 || offset >= chunk.count) continue;
      out.set(rank, { rank, doc: chunk.docs[offset], value: chunk.values[offset] });
    }
    return out;
  }

  function sortReplicaDocPacksMeta(replica) {
    return replica?.doc_packs || null;
  }

  function sortReplicaDocPackPath(replica) {
    const docs = sortReplicaDocPacksMeta(replica);
    return (docs?.packs_path || `sort-replicas/${replica.id}/docs/packs`).replace(/\/?$/u, "/");
  }

  function sortReplicaDocCacheKey(replica, rank) {
    return `${replica.id}\u0000${rank}`;
  }

  async function loadSortReplicaDocPointers(replica, ranks, stats = null) {
    const docs = sortReplicaDocPacksMeta(replica);
    const pointerMeta = docs?.pointers;
    if (!pointerMeta?.file) throw new Error(`Rangefind sort replica ${replica.id} is missing doc pointers.`);
    const pending = [];
    const total = Math.max(0, Number(replica.total || pointerMeta.count || 0));
    const wanted = [...new Set(ranks || [])].filter(rank => rank >= 0 && rank < total);
    for (const rank of wanted) {
      const key = sortReplicaDocCacheKey(replica, rank);
      if (sortReplicaDocPointerCache.has(key)) continue;
      let resolvePointer;
      let rejectPointer;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolvePointer = resolvePromise;
        rejectPointer = rejectPromise;
      });
      promise.catch(() => {});
      sortReplicaDocPointerCache.set(key, promise);
      const offset = pointerMeta.dataOffset + rank * pointerMeta.recordBytes;
      pending.push({
        rank,
        key,
        entry: { pack: pointerMeta.file, offset, length: pointerMeta.recordBytes },
        resolve: resolvePointer,
        reject: rejectPointer
      });
    }
    const groups = rangeGroups(pending, "sortReplicaDocPointers");
    if (stats) {
      stats.docPackPointerLookups += wanted.length;
      stats.docPackPointerFetches += pending.length;
      stats.docPackPointerFetchGroups += groups.length;
    }
    await Promise.all(groups.map(async (group) => {
      try {
        const buffer = await fetchRange(new URL(group.pack, baseUrl), group.start, group.end - group.start);
        for (const item of group.items) {
          const pointer = decodeDocPointerRecord(buffer, item.entry.offset - group.start, pointerMeta, pointerMeta.pack_table || []);
          item.resolve(pointer);
        }
      } catch (error) {
        for (const item of group.items) {
          sortReplicaDocPointerCache.delete(item.key);
          item.reject(error);
        }
        throw error;
      }
    }));
    return wanted;
  }

  async function loadSortReplicaPackedDocs(replica, ranks, stats = null) {
    if (!sortReplicaDocPacksMeta(replica)?.pointers?.file) return null;
    const wanted = await loadSortReplicaDocPointers(replica, ranks, stats);
    if (!wanted.length) return new Map();
    const pending = [];
    for (const rank of wanted) {
      const key = sortReplicaDocCacheKey(replica, rank);
      if (sortReplicaPackedDocCache.has(key)) continue;
      let resolveDoc;
      let rejectDoc;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolveDoc = resolvePromise;
        rejectDoc = rejectPromise;
      });
      promise.catch(() => {});
      sortReplicaPackedDocCache.set(key, promise);
      const entry = await sortReplicaDocPointerCache.get(key);
      pending.push({ rank, key, entry, resolve: resolveDoc, reject: rejectDoc });
    }
    const groups = rangeGroups(pending, "sortReplicaDocs");
    const plannedBytes = groups.reduce((sum, group) => sum + Math.max(0, group.end - group.start), 0);
    const maxGroups = Math.max(1, Number(options.sortReplicaDocMaxFetchGroups || 12));
    const maxBytes = Math.max(1, Number(options.sortReplicaDocMaxFetchBytes || 256 * 1024));
    if (groups.length > maxGroups || plannedBytes > maxBytes) {
      if (stats) {
        stats.docPackSkippedReason = groups.length > maxGroups ? "fetch_groups" : "fetch_bytes";
        stats.docPackPlannedFetchGroups = groups.length;
        stats.docPackPlannedFetchBytes = plannedBytes;
      }
      for (const item of pending) sortReplicaPackedDocCache.delete(item.key);
      return null;
    }
    if (stats) {
      stats.docPackPlannedFetchGroups = groups.length;
      stats.docPackPlannedFetchBytes = plannedBytes;
      stats.docPackFetches += pending.length;
      stats.docPackFetchGroups += groups.length;
    }
    await Promise.all(groups.map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`${sortReplicaDocPackPath(replica)}${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `sort replica doc ${replica.id}:${item.rank}`);
          item.resolve(traceSpanSync("sortReplicas.parseDoc", () => JSON.parse(textDecoder.decode(new Uint8Array(inflated)))));
        }));
      } catch (error) {
        for (const item of group.items) {
          sortReplicaPackedDocCache.delete(item.key);
          item.reject(error);
        }
        throw error;
      }
    }));
    const out = new Map();
    for (const rank of wanted) {
      const doc = await sortReplicaPackedDocCache.get(sortReplicaDocCacheKey(replica, rank));
      if (doc) out.set(rank, doc);
    }
    return out;
  }

  function sortReplicaDocPagesMeta(replica) {
    return replica?.doc_pages || null;
  }

  function sortReplicaDocPageSize(replica) {
    return Math.max(1, Number(sortReplicaDocPagesMeta(replica)?.page_size || 0));
  }

  function sortReplicaDocPageIndex(replica, rank) {
    return Math.floor(rank / sortReplicaDocPageSize(replica));
  }

  function sortReplicaDocPagePackPath(replica) {
    const pages = sortReplicaDocPagesMeta(replica);
    return (pages?.packs_path || `sort-replicas/${replica.id}/docs/page-packs`).replace(/\/?$/u, "/");
  }

  function sortReplicaDocPagePointerCacheKey(replica, pageIndex) {
    return `${replica.id}\u0000${pageIndex}`;
  }

  async function loadSortReplicaDocPagePointers(replica, pageIndexes, stats = null) {
    const pages = sortReplicaDocPagesMeta(replica);
    const pointerMeta = pages?.pointers;
    if (!pointerMeta?.file) throw new Error(`Rangefind sort replica ${replica.id} is missing doc page pointers.`);
    const pending = [];
    const wanted = [...new Set(pageIndexes)];
    for (const pageIndexValue of wanted) {
      const key = sortReplicaDocPagePointerCacheKey(replica, pageIndexValue);
      if (sortReplicaDocPagePointerCache.has(key)) continue;
      if (pageIndexValue < 0 || pageIndexValue >= pointerMeta.count) throw new Error(`Rangefind sort replica doc page ${pageIndexValue} is outside ${replica.id}.`);
      let resolvePointer;
      let rejectPointer;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolvePointer = resolvePromise;
        rejectPointer = rejectPromise;
      });
      promise.catch(() => {});
      sortReplicaDocPagePointerCache.set(key, promise);
      const offset = pointerMeta.dataOffset + pageIndexValue * pointerMeta.recordBytes;
      pending.push({
        pageIndex: pageIndexValue,
        key,
        entry: { pack: pointerMeta.file, offset, length: pointerMeta.recordBytes },
        resolve: resolvePointer,
        reject: rejectPointer
      });
    }
    const groups = rangeGroups(pending, "sortReplicaDocPagePointers");
    if (stats) {
      stats.docPagePointerPagesWanted += wanted.length;
      stats.docPagePointerPagesFetched += pending.length;
      stats.docPagePointerFetchGroups += groups.length;
    }
    await Promise.all(groups.map(async (group) => {
      try {
        const buffer = await fetchRange(new URL(group.pack, baseUrl), group.start, group.end - group.start);
        for (const item of group.items) {
          const pointer = decodeDocPagePointerRecord(buffer, item.entry.offset - group.start, pointerMeta, pointerMeta.pack_table || []);
          item.resolve(pointer);
        }
      } catch (error) {
        for (const item of group.items) {
          sortReplicaDocPagePointerCache.delete(item.key);
          item.reject(error);
        }
        throw error;
      }
    }));
  }

  function sortReplicaDocPageCacheKey(replica, pageIndex) {
    return `${replica.id}\u0000${pageIndex}`;
  }

  async function loadSortReplicaDocPages(replica, ranks, stats = null) {
    const pages = sortReplicaDocPagesMeta(replica);
    if (!pages?.pointers?.file) return null;
    if (pages.encoding !== DOC_PAGE_ENCODING) throw new Error(`Unsupported Rangefind sort replica doc page encoding ${pages.encoding || "unknown"}.`);
    const pageSize = sortReplicaDocPageSize(replica);
    const wantedRanks = [...new Set(ranks || [])].filter(rank => rank >= 0 && rank < (pages.total ?? replica.total ?? 0));
    if (!wantedRanks.length) return new Map();
    const pageIndexes = [...new Set(wantedRanks.map(rank => sortReplicaDocPageIndex(replica, rank)))].sort((a, b) => a - b);
    if (stats) {
      stats.docPageLookups += wantedRanks.length;
      stats.docPagesWanted += pageIndexes.length;
    }
    await loadSortReplicaDocPagePointers(replica, pageIndexes, stats);
    const pending = [];
    for (const pageIndexValue of pageIndexes) {
      const key = sortReplicaDocPageCacheKey(replica, pageIndexValue);
      if (sortReplicaDocPageCache.has(key)) continue;
      let resolvePage;
      let rejectPage;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolvePage = resolvePromise;
        rejectPage = rejectPromise;
      });
      promise.catch(() => {});
      sortReplicaDocPageCache.set(key, promise);
      const entry = await sortReplicaDocPagePointerCache.get(sortReplicaDocPagePointerCacheKey(replica, pageIndexValue));
      pending.push({ pageIndex: pageIndexValue, key, entry, resolve: resolvePage, reject: rejectPage });
    }
    const groups = rangeGroups(pending, "sortReplicaDocPages");
    const plannedBytes = groups.reduce((sum, group) => sum + Math.max(0, group.end - group.start), 0);
    const maxGroups = Math.max(1, Number(options.sortReplicaDocPageMaxFetchGroups || 12));
    const maxBytes = Math.max(1, Number(options.sortReplicaDocPageMaxFetchBytes || 192 * 1024));
    if (groups.length > maxGroups || plannedBytes > maxBytes) {
      if (stats) {
        stats.docPageSkippedReason = groups.length > maxGroups ? "fetch_groups" : "fetch_bytes";
        stats.docPagePlannedFetchGroups = groups.length;
        stats.docPagePlannedFetchBytes = plannedBytes;
      }
      for (const item of pending) sortReplicaDocPageCache.delete(item.key);
      return null;
    }
    if (stats) {
      stats.docPagePlannedFetchGroups = groups.length;
      stats.docPagePlannedFetchBytes = plannedBytes;
      stats.docPagesFetched += pending.length;
      stats.docPageFetchGroups += groups.length;
    }
    await Promise.all(groups.map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`${sortReplicaDocPagePackPath(replica)}${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `sort replica doc page ${replica.id}:${item.pageIndex}`);
          item.resolve(traceSpanSync("sortReplicas.decodeDocPage", () => decodeDocPageColumns(inflated, pages.fields || [], item.pageIndex * pageSize)));
        }));
      } catch (error) {
        for (const item of group.items) {
          sortReplicaDocPageCache.delete(item.key);
          item.reject(error);
        }
        throw error;
      }
    }));

    const out = new Map();
    for (const rank of wantedRanks) {
      const pageIndexValue = sortReplicaDocPageIndex(replica, rank);
      const page = await sortReplicaDocPageCache.get(sortReplicaDocPageCacheKey(replica, pageIndexValue));
      const doc = page?.[rank - pageIndexValue * pageSize];
      if (doc) out.set(rank, doc);
    }
    return out;
  }

  function applySortReplicaBlockRows(cursor, rows, scores, hits) {
    let accepted = 0;
    for (let i = 0; i < rows.length; i += 2) {
      const rank = rows[i];
      scores.set(rank, (scores.get(rank) || 0) + rows[i + 1]);
      if (cursor.isBase) hits.set(rank, (hits.get(rank) || 0) + 1);
      accepted++;
    }
    return accepted;
  }

  function sortReplicaNextRanks(cursors) {
    const ranks = [];
    for (const cursor of cursors) {
      const block = cursor.entry.blocks?.[cursor.blockIndex];
      if (block && Number.isFinite(block.docMin)) ranks.push(block.docMin);
    }
    return ranks;
  }

  async function sortReplicaRankedState(replica, scores, hits, minShouldMatch, sortPlan, stats) {
    const eligible = collectEligibleScores(scores, hits, minShouldMatch);
    const rankInfo = await sortReplicaRankRows(replica, eligible.map(([rank]) => rank), stats);
    const sortValues = new Map();
    for (const [rank, info] of rankInfo) sortValues.set(rank, info.value);
    eligible.sort((a, b) => compareKnownSortRows(a, b, sortPlan, sortValues));
    return { eligible, rankInfo, sortValues };
  }

  async function sortReplicaStopState(replica, cursors, scores, hits, minShouldMatch, k, sortPlan, stats) {
    const state = await sortReplicaRankedState(replica, scores, hits, minShouldMatch, sortPlan, stats);
    if (state.eligible.length < k) return { ...state, stop: false, exhausted: false, boundarySortValue: null };
    const boundarySortValue = state.sortValues.get(state.eligible[k - 1][0]);
    const nextRanks = sortReplicaNextRanks(cursors);
    if (!nextRanks.length) return { ...state, stop: true, exhausted: true, boundarySortValue };
    const nextRows = await sortReplicaRankRows(replica, nextRanks, stats);
    const canTieOrBeat = [...nextRows.values()].some(row => (
      sortPlan.desc ? row.value >= boundarySortValue : row.value <= boundarySortValue
    ));
    return { ...state, stop: !canTieOrBeat, exhausted: false, boundarySortValue };
  }

  function roundedDistanceMeters(distance) {
    return Math.round(distance * 10) / 10;
  }

  async function runGeoBrowse({ page, size, filters, geoPlan, hasFilters, textMatchDocs = null }) {
    return traceSpan("geo.browse", () => runGeoBrowseInner({ page, size, filters, geoPlan, hasFilters, textMatchDocs }));
  }

  async function runGeoBrowseInner({ page, size, filters, geoPlan, hasFilters, textMatchDocs = null }) {
    const root = await loadGeoTreeRoot(geoPlan.field);
    if (!root) return null;
    const offset = (page - 1) * size;
    const k = offset + size;
    const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
    const near = geoPlan.near;
    const radius = near?.radiusMeters ?? null;
    const distanceSorted = !!geoPlan.sort;

    // Filter bitmaps cover whole-corpus facet/boolean checks with one fetch,
    // no matter how many spatially clustered docs each leaf contributes.
    const bitmapStore = docFilterPlan?.active ? await filterBitmapStoreForPlan(docFilterPlan) : null;
    const bitmapCovered = bitmapStore?.covered || new Set();
    const residualFilterFields = docFilterPlan?.active
      ? [...new Set(filterPlanFields(docFilterPlan))].filter(field => !bitmapCovered.has(field))
      : [];

    const geoBlockFilterPlan = docFilterPlan?.active ? makeBlockFilterPlan(filters) : null;
    const best = [];
    const collected = [];
    const distances = new Map();
    let leavesVisited = 0;
    let pointsScanned = 0;
    let pointsAccepted = 0;
    let definiteLeaves = 0;
    let stoppedEarly = false;
    const tracking = geoTraversalTracking();
    const kthDistance = () => (best.length >= k ? best[k - 1].dist : Infinity);

    for await (const { candidate, leafPage } of geoCandidateLeafPages(geoPlan, root, distanceSorted, tracking, geoBlockFilterPlan)) {
      if (distanceSorted && best.length >= k && candidate.minDist > kthDistance()) {
        stoppedEarly = true;
        break;
      }
      leavesVisited++;
      if (candidate.geoDefinite) definiteLeaves++;
      pointsScanned += leafPage.count;
      const { latsE7, lonsE7, docs } = leafPage;
      let codeData = bitmapStore;
      if (residualFilterFields.length) {
        codeData = mergeValueStores(await valueStoreForDocs(residualFilterFields, Array.from(docs)), bitmapStore);
      }
      for (let i = 0; i < leafPage.count; i++) {
        const latE7 = latsE7[i];
        const lonE7 = lonsE7[i];
        if (!candidate.geoDefinite && !geoPointMatchesE7(geoPlan, latE7, lonE7)) continue;
        let dist = 0;
        if (near) {
          dist = haversineMetersE7(near.latE7, near.lonE7, latE7, lonE7);
          if (radius != null && dist > radius) continue;
        }
        if (distanceSorted && best.length >= k && dist > kthDistance()) continue;
        const doc = docs[i];
        if (textMatchDocs && !textMatchDocs.has(doc)) continue;
        if (docFilterPlan?.active && !passesFilterPlan(doc, codeData, docFilterPlan)) continue;
        pointsAccepted++;
        if (distanceSorted) {
          let lo = 0;
          let hi = best.length;
          while (lo < hi) {
            const mid = (lo + hi) >> 1;
            if (best[mid].dist < dist || (best[mid].dist === dist && best[mid].doc < doc)) lo = mid + 1;
            else hi = mid;
          }
          best.splice(lo, 0, { doc, dist });
          if (best.length > k) best.length = k;
        } else {
          collected.push([doc, 0]);
          if (near) distances.set(doc, dist);
          if (collected.length >= k) {
            stoppedEarly = true;
            break;
          }
        }
      }
      if (stoppedEarly) break;
    }

    const rows = distanceSorted
      ? best.slice(offset, offset + size).map(item => [item.doc, 0])
      : collected.slice(offset, offset + size);
    if (distanceSorted) {
      for (const item of best) distances.set(item.doc, item.dist);
    }
    const resultContext = { hasTextTerms: false, preferDocPages: true };
    const results = await rowsToResults(rows, resultContext);
    if (near) {
      for (const result of results) {
        const dist = distances.get(result.index);
        if (dist != null) result.distanceMeters = roundedDistanceMeters(dist);
      }
    }
    const matched = distanceSorted ? best.length : collected.length;
    const exactTotal = !stoppedEarly;
    return {
      total: exactTotal ? matched : Math.max(matched, k),
      results,
      page,
      size,
      approximate: !exactTotal,
      stats: {
        exact: distanceSorted ? true : exactTotal,
        geoLane: (distanceSorted ? "nearest" : "browse") + (textMatchDocs ? "Text" : ""),
        geoField: geoPlan.field,
        geoDistanceSorted: distanceSorted,
        geoTreeLevels: root.levels,
        geoDirectoryLeaves: root.leafCount,
        geoCandidateBranches: tracking.counters.candidateBranches,
        geoBranchPagesFetched: tracking.branchFetchStats.fetched,
        geoCandidateLeaves: tracking.counters.candidateLeaves,
        geoLeavesVisited: leavesVisited,
        geoDefiniteLeaves: definiteLeaves,
        geoLeafPagesPrefetched: tracking.leafFetchStats.wanted,
        geoLeafPagesFetched: tracking.leafFetchStats.fetched,
        geoLeafPageFetchGroups: tracking.leafFetchStats.groups,
        geoPointsScanned: pointsScanned,
        geoPointsAccepted: pointsAccepted,
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs
      }
    };
  }

  async function runDocValueBrowse({ page, size, filters, sortPlan, hasFilters }) {
    return traceSpan("docValues.sortedBrowse", () => runDocValueBrowseInner({ page, size, filters, sortPlan, hasFilters }));
  }

  async function runDocValueBrowseInner({ page, size, filters, sortPlan, hasFilters }) {
    const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
    const field = sortPlan?.field || null;
    if (field && !docValueSorted) await ensureDocValueSortedManifest();
    if (!field || !docValueSortField(field)) return null;
    const directory = await loadDocValueSortDirectory(field);
    if (!directory?.pages?.length) return null;
    const offset = (page - 1) * size;
    const k = offset + size;
    const desc = !!sortPlan?.desc;
    const pages = sortedDirectoryPages(directory, desc, docFilterPlan);
    const collected = [];
    const filterFields = filterPlanFields(docFilterPlan).filter(item => item !== field);
    let pagesVisited = 0;
    let rowsScanned = 0;
    let rowsAccepted = 0;
    let definitelyPassedPages = 0;
    let stoppedEarly = false;
    const sortPageFetchStats = { wanted: 0, fetched: 0, groups: 0 };

    for (let pageIndex = 0; pageIndex < pages.length && !stoppedEarly;) {
      const startPageIndex = pageIndex;
      const batchPages = pages.slice(startPageIndex, startPageIndex + docValueSortPageBatchSize);
      const loadedPages = await loadDocValueSortPages(field, directory, batchPages.map(item => item.index), sortPageFetchStats);
      for (let batchOffset = 0; batchOffset < batchPages.length; batchOffset++) {
        const candidatePage = batchPages[batchOffset];
        const loadedPage = loadedPages[batchOffset];
        pageIndex = startPageIndex + batchOffset;
        pagesVisited++;
        const rows = sortedPageRows(loadedPage, desc);
        rowsScanned += rows.length;
        const definite = pageDefinitelyPassesDocValueFilter(candidatePage, docFilterPlan, field);
        if (definite) definitelyPassedPages++;
        const codeData = definite || !filterFields.length
          ? null
          : await valueStoreForFilterPlan(docFilterPlan, rows.map(row => row.doc), [field]);
        for (const row of rows) {
          const known = { [field]: row.value };
          if (!definite && !passesFilterPlanWithKnown(row.doc, codeData, docFilterPlan, known)) continue;
          collected.push([row.doc, 0]);
          rowsAccepted++;
          if (collected.length >= k) {
            stoppedEarly = true;
            break;
          }
        }
        if (stoppedEarly) break;
      }
      pageIndex = startPageIndex + batchPages.length;
    }

    const resultContext = { hasTextTerms: false, preferDocPages: true };
    const results = await rowsToResults(collected.slice(offset, offset + size), resultContext);
    const exactTotal = !stoppedEarly;
    return {
      total: !hasFilters ? manifest.total : exactTotal ? collected.length : Math.max(collected.length, k),
      results,
      page,
      size,
      approximate: !exactTotal && hasFilters,
      stats: {
        exact: exactTotal,
        docValuePruning: true,
        docValuePruneField: field,
        docValueSortDirection: desc ? "desc" : "asc",
        docValueDirectoryPages: directory.pages.length,
        docValueCandidatePages: pages.length,
        docValuePagesPruned: directory.pages.length - pages.length,
        docValuePagesVisited: pagesVisited,
        docValueSortPageBatchSize,
        docValueSortPagesPrefetched: sortPageFetchStats.wanted,
        docValueSortPagesFetched: sortPageFetchStats.fetched,
        docValueSortPageFetchGroups: sortPageFetchStats.groups,
        docValueSortPageOverfetch: Math.max(0, sortPageFetchStats.wanted - pagesVisited),
        docValueRowsScanned: rowsScanned,
        docValueRowsAccepted: rowsAccepted,
        docValueDefinitePages: definitelyPassedPages,
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs
      }
    };
  }

  async function runSortReplicaTextSearch({ page, size, filters, sortPlan, baseTerms, terms, rerank = true }) {
    return traceSpan("sortReplicaText.search", () => runSortReplicaTextSearchInner({ page, size, filters, sortPlan, baseTerms, terms, rerank }));
  }

  async function runSortReplicaTextSearchInner({ page, size, filters, sortPlan, baseTerms, terms, rerank = true }) {
    const replica = sortReplicaForPlan(sortPlan);
    if (!replica || !baseTerms.length || terms.length > SKIP_MAX_TERMS) return null;
    if (rerank !== false && dependencyTerms(baseTerms).length) return null;
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    if (hasFilters) return null;

    const offset = (page - 1) * size;
    const k = offset + size;
    const entries = await sortReplicaTermEntries(terms, replica);
    const baseSet = new Set(baseTerms);
    const minShouldMatch = minShouldMatchFor(baseTerms);
    const presentBaseTerms = new Set(entries.filter(item => baseSet.has(item.term)).map(item => item.term));
    if (presentBaseTerms.size < Math.max(1, minShouldMatch)) {
      return emptyTextSearchResponse({
        page,
        size,
        terms,
        entries,
        missingBaseTerms: Math.max(0, baseTerms.length - presentBaseTerms.size)
      });
    }

    const cursors = entries.map((item, termIndex) => ({
      ...item,
      termIndex,
      isBase: baseSet.has(item.term),
      blockIndex: 0
    }));
    if (!cursors.length) return emptyTextSearchResponse({ page, size, terms });

    const scores = new Map();
    const hits = new Map();
    const rankStats = { rankLookups: 0, rankChunksWanted: 0, rankChunksFetched: 0, rankChunkFetchGroups: 0 };
    const docPackStats = {
      docPackPointerLookups: 0,
      docPackPointerFetches: 0,
      docPackPointerFetchGroups: 0,
      docPackFetches: 0,
      docPackFetchGroups: 0,
      docPackPlannedFetchGroups: 0,
      docPackPlannedFetchBytes: 0,
      docPackSkippedReason: ""
    };
    const docPageStats = {
      docPageLookups: 0,
      docPagesWanted: 0,
      docPagesFetched: 0,
      docPageFetchGroups: 0,
      docPagePlannedFetchGroups: 0,
      docPagePlannedFetchBytes: 0,
      docPageSkippedReason: "",
      docPagePointerPagesWanted: 0,
      docPagePointerPagesFetched: 0,
      docPagePointerFetchGroups: 0
    };
    const proofStats = createTopKProofStats({ sortPlan });
    let blocksDecoded = 0;
    let postingsDecoded = 0;
    let postingsAccepted = 0;
    let frontierBatches = 0;
    let frontierBlocks = 0;
    let frontierMax = 0;
    let fetchedBlocks = 0;
    let fetchGroups = 0;
    let wantedBlocks = 0;
    let stopChecks = 0;
    let stoppedBySortBound = false;
    let exhausted = false;
    let finalState = null;

    while (true) {
      const active = cursors.filter(cursor => cursor.blockIndex < (cursor.entry.blocks?.length || 0));
      stopChecks++;
      proofStats.attempts++;
      finalState = await sortReplicaStopState(replica, active, scores, hits, minShouldMatch, k, sortPlan, rankStats);
      if (finalState.stop) {
        exhausted = finalState.exhausted;
        stoppedBySortBound = !finalState.exhausted;
        recordTopKProofSuccess(proofStats, { threshold: finalState.boundarySortValue || 0, maxOutsidePotential: 0 });
        break;
      }
      if (!active.length) {
        exhausted = true;
        finalState = await sortReplicaRankedState(replica, scores, hits, minShouldMatch, sortPlan, rankStats);
        recordTopKProofSuccess(proofStats, { threshold: 0, maxOutsidePotential: 0 });
        break;
      }

      active.sort((a, b) => {
        const left = a.entry.blocks[a.blockIndex];
        const right = b.entry.blocks[b.blockIndex];
        return (left.docMin || 0) - (right.docMin || 0) || (right.maxImpact || 0) - (left.maxImpact || 0);
      });
      const frontier = active.slice(0, postingBlockFrontier);
      frontierBatches++;
      frontierBlocks += frontier.length;
      frontierMax = Math.max(frontierMax, frontier.length);
      await Promise.all(frontier.map(async (cursor) => {
        const blockIndex = cursor.blockIndex;
        const decoded = await decodeEntryBlockBatch(cursor.shard, cursor.entry, [blockIndex], "postingBlocks");
        fetchedBlocks += decoded.fetchedBlocks;
        fetchGroups += decoded.fetchGroups;
        wantedBlocks += decoded.wantedBlocks;
        cursor.blockIndex++;
        const block = decoded.blocks[0];
        const rows = block?.rows || new Int32Array(0);
        blocksDecoded++;
        postingsDecoded += rows.length / 2;
        postingsAccepted += applySortReplicaBlockRows(cursor, rows, scores, hits);
      }));
    }

    if (!finalState) finalState = await sortReplicaRankedState(replica, scores, hits, minShouldMatch, sortPlan, rankStats);
    const ranked = finalState.eligible || [];
    const rows = ranked.slice(offset, offset + size);
    const rowRanks = rows.map(([rank]) => rank);
    let rankDocs = await loadSortReplicaDocPages(replica, rowRanks, docPageStats);
    let rankDocLane = rankDocs ? "sortReplicaDocPages" : "";
    if (!rankDocs) {
      rankDocs = await loadSortReplicaPackedDocs(replica, rowRanks, docPackStats);
      rankDocLane = rankDocs ? "sortReplicaDocPacks" : "";
    }
    let results;
    const resultContext = {};
    if (rankDocs) {
      results = rows
        .map(([rank, score]) => {
          const doc = rankDocs.get(rank);
          return doc ? { ...doc, score } : null;
        })
        .filter(Boolean);
      resultContext.docPayloadLane = rankDocLane;
      resultContext.docPayloadPages = rankDocLane === "sortReplicaDocPages" ? docPageStats.docPagesWanted : 0;
      resultContext.docPayloadRows = rows.length;
      resultContext.docPayloadOverfetchDocs = rankDocLane === "sortReplicaDocPages"
        ? docPageStats.docPagesWanted * sortReplicaDocPageSize(replica)
        : rows.length;
      resultContext.docPayloadAdaptive = false;
      resultContext.docPayloadForced = false;
    } else {
      const rankInfo = await sortReplicaRankRows(replica, rows.map(([rank]) => rank), rankStats);
      const mappedRows = rows
        .map(([rank, score]) => {
          const info = rankInfo.get(rank) || finalState.rankInfo?.get(rank);
          return info ? [info.doc, score] : null;
        })
        .filter(Boolean);
      Object.assign(resultContext, { hasTextTerms: true, preferDocPages: true });
      results = await rowsToResults(mappedRows, resultContext);
    }
    const totalExact = exhausted;
    return {
      total: totalExact ? ranked.length : Math.max(ranked.length, k),
      page,
      size,
      results,
      approximate: !totalExact,
      stats: {
        exact: true,
        plannerLane: "sortReplicaText",
        topKProven: true,
        totalExact,
        tailExhausted: totalExact,
        terms: terms.length,
        shards: new Set(entries.map(item => item.shardName)).size,
        blocksDecoded,
        postingsDecoded,
        postingsAccepted,
        skippedBlocks: 0,
        sortReplicaText: true,
        sortReplicaId: replica.id,
        sortReplicaField: replica.field,
        sortReplicaDirection: sortPlan.desc ? "desc" : "asc",
        sortReplicaStopReason: stoppedBySortBound ? "sort_bound" : "exhausted",
        sortReplicaStopChecks: stopChecks,
        sortReplicaFrontier: postingBlockFrontier,
        sortReplicaFrontierBatches: frontierBatches,
        sortReplicaFrontierBlocks: frontierBlocks,
        sortReplicaFrontierMax: frontierMax,
        sortReplicaFetchedBlocks: fetchedBlocks,
        sortReplicaFetchGroups: fetchGroups,
        sortReplicaWantedBlocks: wantedBlocks,
        sortReplicaRankLookups: rankStats.rankLookups,
        sortReplicaRankChunksWanted: rankStats.rankChunksWanted,
        sortReplicaRankChunksFetched: rankStats.rankChunksFetched,
        sortReplicaRankChunkFetchGroups: rankStats.rankChunkFetchGroups,
        sortReplicaDocPackPointerLookups: docPackStats.docPackPointerLookups,
        sortReplicaDocPackPointerFetches: docPackStats.docPackPointerFetches,
        sortReplicaDocPackPointerFetchGroups: docPackStats.docPackPointerFetchGroups,
        sortReplicaDocPackFetches: docPackStats.docPackFetches,
        sortReplicaDocPackFetchGroups: docPackStats.docPackFetchGroups,
        sortReplicaDocPackPlannedFetchGroups: docPackStats.docPackPlannedFetchGroups,
        sortReplicaDocPackPlannedFetchBytes: docPackStats.docPackPlannedFetchBytes,
        sortReplicaDocPackSkippedReason: docPackStats.docPackSkippedReason,
        sortReplicaDocPageLookups: docPageStats.docPageLookups,
        sortReplicaDocPagesWanted: docPageStats.docPagesWanted,
        sortReplicaDocPagesFetched: docPageStats.docPagesFetched,
        sortReplicaDocPageFetchGroups: docPageStats.docPageFetchGroups,
        sortReplicaDocPagePlannedFetchGroups: docPageStats.docPagePlannedFetchGroups,
        sortReplicaDocPagePlannedFetchBytes: docPageStats.docPagePlannedFetchBytes,
        sortReplicaDocPageSkippedReason: docPageStats.docPageSkippedReason,
        sortReplicaDocPagePointerPagesWanted: docPageStats.docPagePointerPagesWanted,
        sortReplicaDocPagePointerPagesFetched: docPageStats.docPagePointerPagesFetched,
        sortReplicaDocPagePointerFetchGroups: docPageStats.docPagePointerFetchGroups,
        docValueSortText: false,
        docValuePruning: false,
        sortedTextBlockScheduler: false,
        sortedTextCandidateLookup: false,
        plannerFallbackReason: "",
        ...topKProofStatsObject(proofStats, ""),
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        docPayloadForced: resultContext.docPayloadForced,
        rerankCandidates: 0,
        dependencyFeatures: 0,
        dependencyTermsMatched: 0,
        dependencyPostingsScanned: 0,
        dependencyCandidateMatches: 0
      }
    };
  }

  async function runSortedTextSearch({ page, size, filters, sortPlan, baseTerms, terms, rerank = true }) {
    return traceSpan("sortPageText.search", () => runSortedTextSearchInner({ page, size, filters, sortPlan, baseTerms, terms, rerank }));
  }

  async function runSortedTextSearchInner({ page, size, filters, sortPlan, baseTerms, terms, rerank = true }) {
    const field = sortPlan?.field || null;
    const replicaResponse = await runSortReplicaTextSearch({ page, size, filters, sortPlan, baseTerms, terms, rerank });
    if (replicaResponse) return replicaResponse;
    if (field && !docValueSorted) await ensureDocValueSortedManifest();
    if (!field || !docValueSortField(field) || !baseTerms.length || terms.length > SKIP_MAX_TERMS) return null;
    if (rerank !== false && dependencyTerms(baseTerms).length) return null;

    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    if (hasFilters) await ensureDocValuesManifest();
    await ensureFacetDictionaries(filters);
    const directory = await loadDocValueSortDirectory(field);
    if (!directory?.pages?.length) return null;
    const entries = await termEntries(terms);
    const baseSet = new Set(baseTerms);
    const minShouldMatch = minShouldMatchFor(baseTerms);
    const presentBaseTerms = new Set(entries.filter(item => baseSet.has(item.term)).map(item => item.term));
    if (presentBaseTerms.size < Math.max(1, minShouldMatch)) {
      return emptyTextSearchResponse({
        page,
        size,
        terms,
        entries,
        missingBaseTerms: Math.max(0, baseTerms.length - presentBaseTerms.size)
      });
    }

    const offset = (page - 1) * size;
    const k = offset + size;
    const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
    const filterFields = filterPlanFields(docFilterPlan).filter(item => item !== field);
    const desc = !!sortPlan.desc;
    const candidatePages = sortedDirectoryPages(directory, desc, docFilterPlan);
    const collected = [];
    const sortValues = new Map();
    const decodedBlocks = new Set();
    let pagesVisited = 0;
    let rowsScanned = 0;
    let rowsAccepted = 0;
    let blocksDecoded = 0;
    let postingsDecoded = 0;
    let postingRowsScanned = 0;
    let postingLookupHits = 0;
    let candidatePostingBlocks = 0;
    let skippedPostingBlocks = 0;
    let consideredPostingBlocks = 0;
    let consideredPostingSuperblocks = 0;
    let skippedPostingSuperblocks = 0;
    let definitelyPassedPages = 0;
    let stoppedBySortBound = false;
    const sortPageFetchStats = { wanted: 0, fetched: 0, groups: 0 };

    for (let pageIndex = 0; pageIndex < candidatePages.length && !stoppedBySortBound;) {
      const startPageIndex = pageIndex;
      const batchPages = candidatePages.slice(startPageIndex, startPageIndex + docValueSortPageBatchSize);
      const loadedPages = await loadDocValueSortPages(field, directory, batchPages.map(item => item.index), sortPageFetchStats);
      for (let batchOffset = 0; batchOffset < batchPages.length; batchOffset++) {
        pageIndex = startPageIndex + batchOffset;
        const candidatePage = batchPages[batchOffset];
        const loadedPage = loadedPages[batchOffset];
        pagesVisited++;
        const rows = sortedPageRows(loadedPage, desc);
        rowsScanned += rows.length;
        const definite = pageDefinitelyPassesDocValueFilter(candidatePage, docFilterPlan, field);
        if (definite) definitelyPassedPages++;
        const codeData = definite || !filterFields.length
          ? null
          : await valueStoreForFilterPlan(docFilterPlan, rows.map(row => row.doc), [field]);
        const candidateDocs = new Set();
        const pageScores = new Map();
        const pageHits = new Map();

        for (const row of rows) {
          const known = { [field]: row.value };
          if (!definite && !passesFilterPlanWithKnown(row.doc, codeData, docFilterPlan, known)) continue;
          candidateDocs.add(row.doc);
        }

        if (candidateDocs.size) {
          const pageBlockFilterPlan = makeBlockFilterPlan(mergeSortPageFilter(filters, field, candidatePage));
          for (const { term, shard, shardName, entry } of entries) {
            const isBase = baseSet.has(term);
            const candidates = sortedTextCandidateBlockIndexes(entry, pageBlockFilterPlan, candidateDocs);
            candidatePostingBlocks += candidates.indexes.length;
            consideredPostingBlocks += candidates.consideredBlocks;
            skippedPostingBlocks += candidates.skippedBlocks;
            consideredPostingSuperblocks += candidates.consideredSuperblocks;
            skippedPostingSuperblocks += candidates.skippedSuperblocks;
            for (const { blockIndex, rows: postings, scanned } of await lookupEntryBlocks(shard, entry, candidates.indexes, candidateDocs)) {
              const blockKey = `${shardName}\u0000${term}\u0000${blockIndex}`;
              if (!decodedBlocks.has(blockKey)) {
                decodedBlocks.add(blockKey);
                blocksDecoded++;
              }
              postingRowsScanned += scanned || 0;
              postingLookupHits += postings.length / 2;
              postingsDecoded += postings.length / 2;
              for (let i = 0; i < postings.length; i += 2) {
                const doc = postings[i];
                if (!candidateDocs.has(doc)) continue;
                pageScores.set(doc, (pageScores.get(doc) || 0) + postings[i + 1]);
                if (isBase) pageHits.set(doc, (pageHits.get(doc) || 0) + 1);
              }
            }
          }
        }

        for (const row of rows) {
          const score = pageScores.get(row.doc);
          if (score == null || (pageHits.get(row.doc) || 0) < Math.max(1, minShouldMatch)) continue;
          sortValues.set(row.doc, row.sortValue);
          collected.push([row.doc, score]);
          rowsAccepted++;
        }

        collected.sort((a, b) => compareKnownSortRows(a, b, sortPlan, sortValues));
        if (collected.length >= k) {
          const boundarySortValue = sortValues.get(collected[k - 1][0]);
          if (!nextSortPageCanTie(candidatePages[pageIndex + 1], boundarySortValue, desc)) {
            stoppedBySortBound = true;
            break;
          }
        }
      }
      pageIndex = startPageIndex + batchPages.length;
    }

    collected.sort((a, b) => compareKnownSortRows(a, b, sortPlan, sortValues));
    const rows = collected.slice(offset, offset + size);
    const resultContext = { hasTextTerms: true, preferDocPages: true };
    const results = await rowsToResults(rows, resultContext);
    const totalExact = !stoppedBySortBound;
    return {
      total: totalExact ? collected.length : Math.max(collected.length, k),
      page,
      size,
      results,
      approximate: !totalExact,
      stats: {
        exact: true,
        plannerLane: "sortPageText",
        topKProven: true,
        totalExact,
        tailExhausted: totalExact,
        terms: terms.length,
        shards: new Set(entries.map(item => item.shardName)).size,
        blocksDecoded,
        postingsDecoded,
        postingsAccepted: rowsAccepted,
        skippedBlocks: skippedPostingBlocks,
        sortedTextBlockScheduler: true,
        sortedTextCandidateLookup: true,
        sortPagePostingBlocksConsidered: consideredPostingBlocks,
        sortPagePostingBlocksCandidate: candidatePostingBlocks,
        sortPagePostingBlocksSkipped: skippedPostingBlocks,
        sortPagePostingSuperblocksConsidered: consideredPostingSuperblocks,
        sortPagePostingSuperblocksSkipped: skippedPostingSuperblocks,
        sortPagePostingRowsScanned: postingRowsScanned,
        sortPagePostingLookupHits: postingLookupHits,
        docValueSortText: true,
        docValuePruning: true,
        docValuePruneField: field,
        docValueSortDirection: desc ? "desc" : "asc",
        docValueDirectoryPages: directory.pages.length,
        docValueCandidatePages: candidatePages.length,
        docValuePagesPruned: directory.pages.length - candidatePages.length,
        docValuePagesVisited: pagesVisited,
        docValueSortPageBatchSize,
        docValueSortPagesPrefetched: sortPageFetchStats.wanted,
        docValueSortPagesFetched: sortPageFetchStats.fetched,
        docValueSortPageFetchGroups: sortPageFetchStats.groups,
        docValueSortPageOverfetch: Math.max(0, sortPageFetchStats.wanted - pagesVisited),
        docValueRowsScanned: rowsScanned,
        docValueRowsAccepted: rowsAccepted,
        docValueDefinitePages: definitelyPassedPages,
        sortSummaryStopReason: stoppedBySortBound ? "sort_bound" : "exhausted",
        plannerFallbackReason: "",
        ...topKProofStatsObject(createTopKProofStats({ hasFilters, sortPlan }), ""),
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        docPayloadForced: resultContext.docPayloadForced,
        rerankCandidates: 0,
        dependencyFeatures: 0,
        dependencyTermsMatched: 0,
        dependencyPostingsScanned: 0,
        dependencyCandidateMatches: 0
      }
    };
  }

  async function runDocValueChunkBrowse({ page, size, filters, hasFilters }) {
    return traceSpan("docValues.chunkBrowse", () => runDocValueChunkBrowseInner({ page, size, filters, hasFilters }));
  }

  async function runDocValueChunkBrowseInner({ page, size, filters, hasFilters }) {
    if (!docValues || !hasFilters) return null;
    const docFilterPlan = makeDocFilterPlan(filters);
    if (!docFilterPlan?.active) return null;
    const fields = filterPlanFields(docFilterPlan);
    if (!fields.length) return null;
    const offset = (page - 1) * size;
    const k = offset + size;
    const chunkIndexes = candidateDocValueChunks(docFilterPlan);
    const chunkSize = Math.max(1, docValues.chunk_size || manifest.total || 1);
    const collected = [];
    let chunksVisited = 0;
    let rowsScanned = 0;
    let rowsAccepted = 0;
    let stoppedEarly = false;

    for (const chunkIndex of chunkIndexes) {
      const codeData = await ensureDocValueChunkIndexes(fields, [chunkIndex]);
      chunksVisited++;
      const start = chunkIndex * chunkSize;
      const end = Math.min(manifest.total, start + chunkSize);
      for (let index = start; index < end; index++) {
        rowsScanned++;
        if (!passesFilterPlan(index, codeData, docFilterPlan)) continue;
        collected.push([index, 0]);
        rowsAccepted++;
        if (collected.length >= k) {
          stoppedEarly = true;
          break;
        }
      }
      if (stoppedEarly) break;
    }

    const resultContext = { hasTextTerms: false, preferDocPages: true };
    const results = await rowsToResults(collected.slice(offset, offset + size), resultContext);
    const exactTotal = !stoppedEarly;
    return {
      total: exactTotal ? collected.length : Math.max(collected.length, k),
      results,
      page,
      size,
      approximate: !exactTotal,
      stats: {
        exact: exactTotal,
        docValueChunkPruning: true,
        docValueChunksTotal: Math.ceil(manifest.total / chunkSize),
        docValueCandidateChunks: chunkIndexes.length,
        docValueChunksPruned: Math.ceil(manifest.total / chunkSize) - chunkIndexes.length,
        docValueChunksVisited: chunksVisited,
        docValueRowsScanned: rowsScanned,
        docValueRowsAccepted: rowsAccepted,
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs
      }
    };
  }

  function sortRanked(ranked, codeData, sortPlan) {
    if (!sortPlan || !codeData) return ranked;
    return ranked.slice().sort((a, b) => compareRankedRows(a, b, codeData, sortPlan));
  }

  function compareRankedRows(a, b, codeData, sortPlan) {
    const left = valueForDoc(codeData, sortPlan.field, a[0]);
    const right = valueForDoc(codeData, sortPlan.field, b[0]);
    const leftMissing = left == null;
    const rightMissing = right == null;
    if (leftMissing || rightMissing) {
      if (leftMissing && rightMissing) return b[1] - a[1] || a[0] - b[0];
      return leftMissing ? 1 : -1;
    }
    if (left !== right) return sortPlan.desc ? Number(right) - Number(left) : Number(left) - Number(right);
    return b[1] - a[1] || a[0] - b[0];
  }

  function selectSortedTopK(candidates, codeData, sortPlan, k, filterPlan = null) {
    const top = [];
    let total = 0;
    for (const row of candidates) {
      if (filterPlan && !passesFilterPlan(row[0], codeData, filterPlan)) continue;
      total++;
      if (top.length < k) {
        top.push(row);
        if (top.length === k) top.sort((a, b) => compareRankedRows(a, b, codeData, sortPlan));
      } else if (compareRankedRows(row, top[top.length - 1], codeData, sortPlan) < 0) {
        top[top.length - 1] = row;
        top.sort((a, b) => compareRankedRows(a, b, codeData, sortPlan));
      }
    }
    if (top.length < k) top.sort((a, b) => compareRankedRows(a, b, codeData, sortPlan));
    return { total, ranked: top };
  }

  function bitIsSet(mask, bit) {
    return (mask & (2 ** bit)) !== 0;
  }

  function createTopKProofStats({ hasFilters = false, sortPlan = null, blockFilterPlan = null } = {}) {
    return {
      attempts: 0,
      successes: 0,
      failures: new Map(),
      lastFailureReason: "",
      lastThreshold: 0,
      lastMaxOutsidePotential: 0,
      lastRemainingTerms: 0,
      lastRemainingTermUpperBound: 0,
      filterAware: Boolean(hasFilters),
      sortAware: Boolean(sortPlan),
      docRangeAware: false,
      filterUnknown: Boolean(blockFilterPlan?.unknownFields?.length),
      unknownFilterFields: blockFilterPlan?.unknownFields || []
    };
  }

  function recordTopKProofFailure(stats, reason, detail = {}) {
    if (!stats) return;
    stats.failures.set(reason, (stats.failures.get(reason) || 0) + 1);
    stats.lastFailureReason = reason;
    if (detail.threshold != null) stats.lastThreshold = detail.threshold;
    if (detail.maxOutsidePotential != null) stats.lastMaxOutsidePotential = detail.maxOutsidePotential;
    if (detail.remainingTerms != null) stats.lastRemainingTerms = detail.remainingTerms;
    if (detail.remainingTermUpperBound != null) stats.lastRemainingTermUpperBound = detail.remainingTermUpperBound;
  }

  function recordTopKProofSuccess(stats, detail = {}) {
    if (!stats) return;
    stats.successes++;
    stats.lastFailureReason = "";
    if (detail.threshold != null) stats.lastThreshold = detail.threshold;
    if (detail.maxOutsidePotential != null) stats.lastMaxOutsidePotential = detail.maxOutsidePotential;
    if (detail.remainingTerms != null) stats.lastRemainingTerms = detail.remainingTerms;
    if (detail.remainingTermUpperBound != null) stats.lastRemainingTermUpperBound = detail.remainingTermUpperBound;
  }

  function topKProofStatsObject(stats, fallbackReason = "") {
    if (!stats) {
      return {
        topKProofAttempts: 0,
        topKProofSuccesses: 0,
        topKProofFailureReason: fallbackReason,
        topKProofFailureCandidateCount: 0,
        topKProofFailureScoreBound: 0,
        topKProofFailureTieBound: 0,
        topKProofThreshold: 0,
        topKProofMaxOutsidePotential: 0,
        topKProofRemainingTerms: 0,
        topKProofRemainingTermUpperBound: 0,
        topKProofFilterAware: false,
        topKProofSortAware: false,
        topKProofDocRangeAware: false,
        topKProofFilterUnknown: false,
        topKProofUnknownFilterFields: ""
      };
    }
    return {
      topKProofAttempts: stats.attempts,
      topKProofSuccesses: stats.successes,
      topKProofFailureReason: fallbackReason || stats.lastFailureReason || "",
      topKProofFailureCandidateCount: stats.failures.get("candidate_count") || 0,
      topKProofFailureScoreBound: stats.failures.get("score_bound") || 0,
      topKProofFailureTieBound: stats.failures.get("tie_bound") || 0,
      topKProofThreshold: stats.lastThreshold,
      topKProofMaxOutsidePotential: stats.lastMaxOutsidePotential,
      topKProofRemainingTerms: stats.lastRemainingTerms,
      topKProofRemainingTermUpperBound: stats.lastRemainingTermUpperBound,
      topKProofFilterAware: stats.filterAware,
      topKProofSortAware: stats.sortAware,
      topKProofDocRangeAware: stats.docRangeAware,
      topKProofFilterUnknown: stats.filterUnknown,
      topKProofUnknownFilterFields: stats.unknownFilterFields.join(",")
    };
  }

  function cursorSuperblock(cursor) {
    const superblocks = cursor.entry.superblocks || [];
    if (!superblocks.length) return null;
    let index = Math.max(0, Math.min(cursor.superblockIndex || 0, superblocks.length - 1));
    while (index < superblocks.length && cursor.blockIndex >= superblocks[index].firstBlock + superblocks[index].blockCount) index++;
    while (index > 0 && cursor.blockIndex < superblocks[index].firstBlock) index--;
    cursor.superblockIndex = index;
    const superblock = superblocks[index];
    return superblock && cursor.blockIndex >= superblock.firstBlock && cursor.blockIndex < superblock.firstBlock + superblock.blockCount
      ? superblock
      : null;
  }

  function markSuperblockConsidered(cursor, superblock) {
    if (!superblock) return;
    if (!cursor.superblocksSeen) cursor.superblocksSeen = new Set();
    if (!cursor.superblocksSeen.has(cursor.superblockIndex)) {
      cursor.superblocksSeen.add(cursor.superblockIndex);
      cursor.superblocksConsidered++;
    }
  }

  function markSuperblockDecoded(cursor) {
    const superblock = cursorSuperblock(cursor);
    if (!superblock) return false;
    if (!cursor.superblocksDecodedSet) cursor.superblocksDecodedSet = new Set();
    if (!cursor.superblocksDecodedSet.has(cursor.superblockIndex)) {
      cursor.superblocksDecodedSet.add(cursor.superblockIndex);
      cursor.superblocksDecoded++;
      return true;
    }
    return false;
  }

  function advanceCursor(cursor, filterPlan) {
    while (cursor.blockIndex < cursor.entry.blocks.length) {
      const superblock = cursorSuperblock(cursor);
      if (superblock) {
        markSuperblockConsidered(cursor, superblock);
        if (!blockMayPass(superblock, filterPlan)) {
          const end = Math.min(cursor.entry.blocks.length, superblock.firstBlock + superblock.blockCount);
          cursor.skippedBlocks += Math.max(0, end - cursor.blockIndex);
          cursor.skippedSuperblocks++;
          cursor.blockIndex = end;
          cursor.superblockIndex++;
          continue;
        }
      }
      if (blockMayPass(cursor.entry.blocks[cursor.blockIndex], filterPlan)) return true;
      cursor.skippedBlocks++;
      cursor.blockIndex++;
    }
    return false;
  }

  function entryDocRangeMaxByIndex(entry) {
    if (!entry?.docRanges?.ranges?.length) return null;
    if (!entry.docRangeMaxByIndex) {
      entry.docRangeMaxByIndex = new Map(entry.docRanges.ranges.map(range => [range.index, range.maxImpact || 0]));
    }
    return entry.docRangeMaxByIndex;
  }

  function docRangeMaxForDoc(entry, doc) {
    const docRanges = entry?.docRanges;
    if (!docRanges?.rangeSize) return null;
    const rangeIndex = Math.floor(doc / docRanges.rangeSize);
    return entryDocRangeMaxByIndex(entry)?.get(rangeIndex) || 0;
  }

  function docRangeMaxForIndex(entry, rangeIndex) {
    return entryDocRangeMaxByIndex(entry)?.get(rangeIndex) || 0;
  }

  function blockDocRangeMaxByIndex(block) {
    if (!block?.docRanges?.ranges?.length) return null;
    if (!block.docRangeMaxByIndex) {
      block.docRangeMaxByIndex = new Map(block.docRanges.ranges.map(range => [range.index, range.maxImpact || 0]));
    }
    return block.docRangeMaxByIndex;
  }

  function blockUpperBoundInDocRange(entry, block, rangeIndex) {
    const blockImpact = block?.maxImpact || 0;
    const blockRangeMax = blockDocRangeMaxByIndex(block);
    if (blockRangeMax) return blockRangeMax.get(rangeIndex) || 0;
    const rangeImpact = docRangeMaxForIndex(entry, rangeIndex);
    return rangeImpact ? Math.min(blockImpact, rangeImpact) : blockImpact;
  }

  function entryRemainingBlockMax(entry) {
    if (!entry?.remainingBlockMaxImpact) {
      const blocks = entry?.blocks || [];
      const impacts = new Array(blocks.length + 1).fill(0);
      const tieDocs = new Array(blocks.length + 1).fill(Infinity);
      for (let index = blocks.length - 1; index >= 0; index--) {
        const block = blocks[index] || {};
        const impact = block.maxImpact || 0;
        const tieDoc = block.maxImpactDoc ?? Infinity;
        const nextImpact = impacts[index + 1] || 0;
        const nextTieDoc = tieDocs[index + 1] ?? Infinity;
        if (impact > nextImpact) {
          impacts[index] = impact;
          tieDocs[index] = tieDoc;
        } else if (impact === nextImpact && impact > 0) {
          impacts[index] = impact;
          tieDocs[index] = Math.min(tieDoc, nextTieDoc);
        } else {
          impacts[index] = nextImpact;
          tieDocs[index] = nextTieDoc;
        }
      }
      entry.remainingBlockMaxImpact = impacts;
      entry.remainingBlockMaxTieDoc = tieDocs;
    }
    return {
      impacts: entry.remainingBlockMaxImpact,
      tieDocs: entry.remainingBlockMaxTieDoc
    };
  }

  function remainingBlockMaxImpact(cursor) {
    const index = Math.max(0, Math.min(cursor.blockIndex || 0, cursor.entry.blocks?.length || 0));
    return entryRemainingBlockMax(cursor.entry).impacts[index] || 0;
  }

  function remainingBlockMaxTieDoc(cursor) {
    const index = Math.max(0, Math.min(cursor.blockIndex || 0, cursor.entry.blocks?.length || 0));
    return entryRemainingBlockMax(cursor.entry).tieDocs[index] ?? Infinity;
  }

  function remainingImpactForCursor(cursor, block, doc = null) {
    const blockImpact = remainingBlockMaxImpact(cursor) || block?.maxImpact || 0;
    if (doc == null) return blockImpact;
    const rangeImpact = docRangeMaxForDoc(cursor.entry, doc);
    return rangeImpact == null ? blockImpact : Math.min(blockImpact, rangeImpact);
  }

  function remainingPotentialInfo(cursors, mask = 0, filterPlan = null, doc = null) {
    let potential = 0;
    let tieDocLowerBound = 0;
    let hasRemaining = false;
    let terms = 0;
    let baseTerms = 0;
    for (const cursor of cursors) {
      if (!advanceCursor(cursor, filterPlan) || bitIsSet(mask, cursor.termIndex)) continue;
      const block = cursor.entry.blocks[cursor.blockIndex];
      if (!block) continue;
      potential += remainingImpactForCursor(cursor, block, doc);
      tieDocLowerBound = Math.max(tieDocLowerBound, remainingBlockMaxTieDoc(cursor));
      hasRemaining = true;
      terms++;
      if (cursor.isBase) baseTerms++;
    }
    return {
      potential,
      tieDocLowerBound: hasRemaining ? tieDocLowerBound : Infinity,
      terms,
      baseTerms
    };
  }

  function remainingDocRangePotentialInfo(cursors, filterPlan = null) {
    const rangeBounds = new Map();
    let fallbackPotential = 0;
    let hasRangeBounds = false;
    let terms = 0;
    let baseTerms = 0;
    for (const cursor of cursors) {
      if (!advanceCursor(cursor, filterPlan)) continue;
      const block = cursor.entry.blocks[cursor.blockIndex];
      if (!block) continue;
      terms++;
      if (cursor.isBase) baseTerms++;
      const ranges = cursor.entry.docRanges?.ranges;
      if (!ranges?.length) {
        fallbackPotential += remainingBlockMaxImpact(cursor) || block.maxImpact || 0;
        continue;
      }
      hasRangeBounds = true;
      for (const range of ranges) {
        const impact = Math.min(block.maxImpact || 0, range.maxImpact || 0);
        if (!impact) continue;
        rangeBounds.set(range.index, (rangeBounds.get(range.index) || 0) + impact);
      }
    }
    if (!hasRangeBounds) return remainingPotentialInfo(cursors, 0, filterPlan);
    let potential = fallbackPotential;
    let bestRange = -1;
    for (const [range, bound] of rangeBounds) {
      if (bound + fallbackPotential > potential) {
        potential = bound + fallbackPotential;
        bestRange = range;
      }
    }
    return {
      potential,
      tieDocLowerBound: bestRange >= 0 ? 0 : Infinity,
      terms,
      baseTerms
    };
  }

  function stableTopK(scores, hits, masks, cursors, minShouldMatch, k, filterPlan, proofStats = null) {
    if (proofStats) proofStats.attempts++;
    const { count: eligibleCount, top } = topEligibleScores(scores, hits, minShouldMatch, k);
    if (eligibleCount < k) {
      recordTopKProofFailure(proofStats, "candidate_count");
      return null;
    }
    const topDocs = new Set(top.map(([doc]) => doc));
    const threshold = top[top.length - 1][1];
    const boundaryDoc = top[top.length - 1][0];
    const hasDocRangeBounds = cursors.some(cursor => (cursor.entry.docRanges?.ranges?.length || 0) >= 2);
    const baselineUnseen = remainingPotentialInfo(cursors, 0, filterPlan);
    const rangeUnseen = hasDocRangeBounds ? remainingDocRangePotentialInfo(cursors, filterPlan) : null;
    const docRangeAware = rangeUnseen && rangeUnseen.potential < baselineUnseen.potential;
    if (proofStats && docRangeAware) proofStats.docRangeAware = true;
    const unseen = docRangeAware ? rangeUnseen : baselineUnseen;
    let maxOutsidePotential = unseen.potential;
    let maxOutsideTieDoc = unseen.tieDocLowerBound;
    let maxRemainingTerms = unseen.terms;
    let maxRemainingTermUpperBound = unseen.potential;

    if (maxOutsidePotential > threshold) {
      recordTopKProofFailure(proofStats, "score_bound", {
        threshold,
        maxOutsidePotential,
        remainingTerms: maxRemainingTerms,
        remainingTermUpperBound: maxRemainingTermUpperBound
      });
      return null;
    }
    if (maxOutsidePotential === threshold && maxOutsideTieDoc <= boundaryDoc) {
      recordTopKProofFailure(proofStats, "tie_bound", {
        threshold,
        maxOutsidePotential,
        remainingTerms: maxRemainingTerms,
        remainingTermUpperBound: maxRemainingTermUpperBound
      });
      return null;
    }

    for (const [doc, score] of scores) {
      if (topDocs.has(doc)) continue;
      const remaining = remainingPotentialInfo(cursors, masks.get(doc) || 0, filterPlan, docRangeAware ? doc : null);
      const potential = score + remaining.potential;
      if (
        potential > maxOutsidePotential
        || (potential === maxOutsidePotential && doc < maxOutsideTieDoc)
      ) {
        maxOutsidePotential = potential;
        maxOutsideTieDoc = doc;
        maxRemainingTerms = remaining.terms;
        maxRemainingTermUpperBound = remaining.potential;
      }
      if (potential > threshold) {
        recordTopKProofFailure(proofStats, "score_bound", {
          threshold,
          maxOutsidePotential: potential,
          remainingTerms: remaining.terms,
          remainingTermUpperBound: remaining.potential
        });
        return null;
      }
      if (potential === threshold && doc < boundaryDoc) {
        recordTopKProofFailure(proofStats, "tie_bound", {
          threshold,
          maxOutsidePotential: potential,
          remainingTerms: remaining.terms,
          remainingTermUpperBound: remaining.potential
        });
        return null;
      }
    }

    if (maxOutsidePotential > threshold) {
      recordTopKProofFailure(proofStats, "score_bound", {
        threshold,
        maxOutsidePotential,
        remainingTerms: maxRemainingTerms,
        remainingTermUpperBound: maxRemainingTermUpperBound
      });
      return null;
    }
    if (maxOutsidePotential === threshold && maxOutsideTieDoc <= boundaryDoc) {
      recordTopKProofFailure(proofStats, "tie_bound", {
        threshold,
        maxOutsidePotential,
        remainingTerms: maxRemainingTerms,
        remainingTermUpperBound: maxRemainingTermUpperBound
      });
      return null;
    }

    recordTopKProofSuccess(proofStats, {
      threshold,
      maxOutsidePotential,
      remainingTerms: maxRemainingTerms,
      remainingTermUpperBound: maxRemainingTermUpperBound
    });
    return top;
  }

  function applyBlockRows(cursor, rows, codeData, filterPlan, scores, hits, masks) {
    let accepted = 0;
    const bit = cursor.termIndex < SKIP_MAX_TERMS ? 2 ** cursor.termIndex : 0;
    for (let i = 0; i < rows.length; i += 2) {
      const doc = rows[i];
      if (codeData && !passesFilterPlan(doc, codeData, filterPlan)) continue;
      scores.set(doc, (scores.get(doc) || 0) + rows[i + 1]);
      if (bit) masks.set(doc, (masks.get(doc) || 0) | bit);
      if (cursor.isBase) hits.set(doc, (hits.get(doc) || 0) + 1);
      accepted++;
    }
    return accepted;
  }

  function postingDocs(rows) {
    const docs = [];
    for (let i = 0; i < rows.length; i += 2) docs.push(rows[i]);
    return docs;
  }

  function cursorImpact(cursor) {
    return cursor.entry.blocks[cursor.blockIndex]?.maxImpact || 0;
  }

  function cursorSuperblockImpact(cursor) {
    return cursorSuperblock(cursor)?.maxImpact || cursorImpact(cursor);
  }

  function frontierPrefetchSize() {
    return EXTERNAL_POSTING_BLOCK_PREFETCH;
  }

  async function decodeCursorFrontier(frontier) {
    const prefetch = frontierPrefetchSize();
    const requests = [];
    const blocks = new Array(frontier.length);
    for (let index = 0; index < frontier.length; index++) {
      const cursor = frontier[index];
      if (!cursor.entry.external) {
        blocks[index] = { cursor, rows: decodePostingBlock(cursor.shard, cursor.entry, cursor.blockIndex) };
        continue;
      }
      const superblock = cursorSuperblock(cursor);
      const maxBlockExclusive = superblock ? superblock.firstBlock + superblock.blockCount : null;
      for (const blockIndex of postingBlockPrefetchIndexes(cursor.entry, cursor.blockIndex, prefetch, maxBlockExclusive)) {
        requests.push({ entry: cursor.entry, blockIndex });
      }
    }

    const batch = await loadPostingBlockBatch(requests, "postingBlockFrontier");
    for (let index = 0; index < frontier.length; index++) {
      const cursor = frontier[index];
      if (!cursor.entry.external) continue;
      const rows = await cursor.entry.blockPostings.get(cursor.blockIndex);
      blocks[index] = { cursor, rows: rows || new Int32Array(0) };
    }

    return {
      blocks: blocks.filter(Boolean),
      fetchedBlocks: batch.fetched,
      fetchGroups: batch.groups,
      wantedBlocks: batch.wanted
    };
  }

  async function decodeEntryBlockBatch(shard, entry, blockIndexes, rangePlan = "postingBlocks") {
    const indexes = [...new Set(blockIndexes || [])].filter(index => index >= 0 && index < (entry.blocks?.length || 0));
    if (!indexes.length) return { blocks: [], fetchedBlocks: 0, fetchGroups: 0, wantedBlocks: 0 };
    if (!entry.external) {
      return {
        blocks: indexes.map(blockIndex => ({ blockIndex, rows: decodePostingBlock(shard, entry, blockIndex) })),
        fetchedBlocks: 0,
        fetchGroups: 0,
        wantedBlocks: indexes.length
      };
    }
    const batch = await loadPostingBlockBatch(indexes.map(blockIndex => ({ entry, blockIndex })), rangePlan);
    const blocks = await Promise.all(indexes.map(async blockIndex => ({
      blockIndex,
      rows: await entry.blockPostings.get(blockIndex) || new Int32Array(0)
    })));
    return {
      blocks,
      fetchedBlocks: batch.fetched,
      fetchGroups: batch.groups,
      wantedBlocks: batch.wanted
    };
  }

  async function decodeCursorBlockBatch(items, rangePlan = "postingBlocks") {
    const unique = new Map();
    for (const item of items || []) {
      const blockIndex = item.blockIndex;
      if (!item?.cursor || blockIndex < 0 || blockIndex >= (item.cursor.entry.blocks?.length || 0)) continue;
      unique.set(postingBlockKey(item.cursor, blockIndex), { cursor: item.cursor, blockIndex });
    }
    const requests = [];
    for (const { cursor, blockIndex } of unique.values()) {
      if (cursor.entry.external) requests.push({ entry: cursor.entry, blockIndex });
    }
    const batch = await loadPostingBlockBatch(requests, rangePlan);
    const blocks = await Promise.all([...unique.values()].map(async ({ cursor, blockIndex }) => ({
      cursor,
      blockIndex,
      rows: cursor.entry.external
        ? await cursor.entry.blockPostings.get(blockIndex) || new Int32Array(0)
        : decodePostingBlock(cursor.shard, cursor.entry, blockIndex)
    })));
    return {
      blocks,
      fetchedBlocks: batch.fetched,
      fetchGroups: batch.groups,
      wantedBlocks: unique.size
    };
  }

  function postingBlockKey(cursor, blockIndex) {
    return `${cursor.shardName}\u0000${cursor.term}\u0000${blockIndex}`;
  }

  function impactTierRankForEntry(entry) {
    if (!docRangeImpactPlannerEnabled || !entry?.impactTiers?.blocks?.length) return null;
    if (!entry.impactTierRank) {
      entry.impactTierRank = new Map();
      for (let rank = 0; rank < entry.impactTiers.blocks.length; rank++) {
        entry.impactTierRank.set(entry.impactTiers.blocks[rank], rank);
      }
    }
    return entry.impactTierRank;
  }

  function docRangeTaskTieDocLowerBound(entry, block, rangeIndex, rangeStart, rangeEnd, upperBound) {
    if (
      block?.maxImpact === upperBound
      && block.maxImpactDoc >= rangeStart
      && block.maxImpactDoc < rangeEnd
    ) {
      return block.maxImpactDoc;
    }
    const blockMin = Number.isFinite(block?.docMin) ? block.docMin : rangeStart;
    return Math.max(rangeStart, Math.min(rangeEnd - 1, blockMin));
  }

  function compareDocRangeTasks(a, b) {
    return (
      b.upperBound - a.upperBound
      || a.impactRank - b.impactRank
      || a.tieDocLowerBound - b.tieDocLowerBound
      || a.blockIndex - b.blockIndex
    );
  }

  function docRangeCandidateBlockTasks(cursor, rangeStart, rangeEnd, rangeIndex, filterPlan) {
    const candidates = docRangeCandidateBlockIndexes(cursor.entry, rangeStart, rangeEnd, filterPlan);
    const rankByBlock = impactTierRankForEntry(cursor.entry);
    const tasks = candidates.indexes
      .map(blockIndex => {
        const block = cursor.entry.blocks?.[blockIndex];
        const upperBound = blockUpperBoundInDocRange(cursor.entry, block, rangeIndex);
        return {
          blockIndex,
          upperBound,
          tieDocLowerBound: docRangeTaskTieDocLowerBound(cursor.entry, block, rangeIndex, rangeStart, rangeEnd, upperBound),
          impactRank: rankByBlock?.get(blockIndex) ?? Number.MAX_SAFE_INTEGER
        };
      })
      .filter(task => task.upperBound > 0)
      .sort(compareDocRangeTasks);
    return { ...candidates, tasks };
  }

  function buildDocRangeUpperBoundPlan(entries, baseSet, minShouldMatch) {
    let rangeSize = null;
    let rangeCount = 0;
    let termRangeEntries = 0;
    const ranges = new Map();
    for (const item of entries) {
      const docRanges = item.entry.docRanges;
      if (!docRanges?.rangeSize || !docRanges.ranges?.length) return null;
      if (rangeSize == null) rangeSize = docRanges.rangeSize;
      if (docRanges.rangeSize !== rangeSize) return null;
      rangeCount = Math.max(rangeCount, docRanges.rangeCount || 0);
      const isBase = baseSet.has(item.term);
      for (const range of docRanges.ranges) {
        const current = ranges.get(range.index) || { index: range.index, upperBound: 0, baseHits: 0, termHits: 0 };
        current.upperBound += range.maxImpact || 0;
        current.termHits++;
        if (isBase) current.baseHits++;
        ranges.set(range.index, current);
        termRangeEntries++;
      }
    }
    const candidates = [...ranges.values()]
      .filter(range => range.upperBound > 0 && range.baseHits >= Math.max(1, minShouldMatch))
      .sort((a, b) => b.upperBound - a.upperBound || a.index - b.index);
    return { rangeSize, rangeCount, termRangeEntries, candidates };
  }

  function docRangeCandidateBlockIndexes(entry, rangeStart, rangeEnd, filterPlan) {
    const indexes = [];
    let consideredBlocks = 0;
    let skippedBlocks = 0;
    let consideredSuperblocks = 0;
    let skippedSuperblocks = 0;
    const blocks = entry.blocks || [];
    const superblocks = entry.superblocks || [];
    const maxDoc = Math.max(rangeStart, rangeEnd - 1);
    const rangeIndex = entry.docRanges?.rangeSize ? Math.floor(rangeStart / entry.docRanges.rangeSize) : -1;
    const blockMayContribute = block => rangeIndex < 0 || blockUpperBoundInDocRange(entry, block, rangeIndex) > 0;
    if (superblocks.length) {
      for (const superblock of superblocks) {
        consideredSuperblocks++;
        const first = Math.max(0, superblock.firstBlock || 0);
        const end = Math.min(blocks.length, first + (superblock.blockCount || 0));
        if (!blockMayPass(superblock, filterPlan) || !blockOverlapsDocSpan(superblock, rangeStart, maxDoc)) {
          skippedSuperblocks++;
          skippedBlocks += Math.max(0, end - first);
          continue;
        }
        for (let blockIndex = first; blockIndex < end; blockIndex++) {
          consideredBlocks++;
          const block = blocks[blockIndex];
          if (blockMayPass(block, filterPlan) && blockOverlapsDocSpan(block, rangeStart, maxDoc) && blockMayContribute(block)) indexes.push(blockIndex);
          else skippedBlocks++;
        }
      }
      return { indexes, consideredBlocks, skippedBlocks, consideredSuperblocks, skippedSuperblocks };
    }
    for (let blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
      consideredBlocks++;
      const block = blocks[blockIndex];
      if (blockMayPass(block, filterPlan) && blockOverlapsDocSpan(block, rangeStart, maxDoc) && blockMayContribute(block)) indexes.push(blockIndex);
      else skippedBlocks++;
    }
    return { indexes, consideredBlocks, skippedBlocks, consideredSuperblocks, skippedSuperblocks };
  }

  function docRangePlannerSelectivity(plan, entries, blockFilterPlan) {
    const sampleSize = Math.min(plan.candidates.length, Math.max(1, postingBlockFrontier));
    let candidateBlocks = 0;
    let availableBlocks = 0;
    for (const range of plan.candidates.slice(0, sampleSize)) {
      const rangeStart = range.index * plan.rangeSize;
      const rangeEnd = Math.min(manifest.total, rangeStart + plan.rangeSize);
      for (const item of entries) {
        availableBlocks += item.entry.blocks?.length || 0;
        candidateBlocks += docRangeCandidateBlockIndexes(item.entry, rangeStart, rangeEnd, blockFilterPlan).indexes.length;
      }
    }
    const ratio = availableBlocks ? candidateBlocks / availableBlocks : 1;
    return { sampleSize, candidateBlocks, availableBlocks, ratio };
  }

  function postingDocsInRange(rows, rangeStart, rangeEnd) {
    const docs = [];
    for (let i = 0; i < rows.length; i += 2) {
      const doc = rows[i];
      if (doc >= rangeStart && doc < rangeEnd) docs.push(doc);
    }
    return docs;
  }

  function applyBlockRowsInDocRange(cursor, rows, rangeStart, rangeEnd, codeData, filterPlan, scores, hits, masks) {
    let accepted = 0;
    const bit = cursor.termIndex < SKIP_MAX_TERMS ? 2 ** cursor.termIndex : 0;
    for (let i = 0; i < rows.length; i += 2) {
      const doc = rows[i];
      if (doc < rangeStart || doc >= rangeEnd) continue;
      if (codeData && !passesFilterPlan(doc, codeData, filterPlan)) continue;
      scores.set(doc, (scores.get(doc) || 0) + rows[i + 1]);
      if (bit) masks.set(doc, (masks.get(doc) || 0) | bit);
      if (cursor.isBase) hits.set(doc, (hits.get(doc) || 0) + 1);
      accepted++;
    }
    return accepted;
  }

  function remainingBlockPotential(remainingTermBounds, mask = 0, remainingTermTieDocs = null) {
    let potential = 0;
    let tieDocLowerBound = 0;
    let hasRemaining = false;
    let terms = 0;
    for (let termIndex = 0; termIndex < remainingTermBounds.length; termIndex++) {
      const bound = remainingTermBounds[termIndex] || 0;
      if (!bound) continue;
      if (termIndex < SKIP_MAX_TERMS && bitIsSet(mask, termIndex)) continue;
      potential += bound;
      tieDocLowerBound = Math.max(tieDocLowerBound, remainingTermTieDocs?.[termIndex] ?? 0);
      hasRemaining = true;
      terms++;
    }
    return { potential, tieDocLowerBound: hasRemaining ? tieDocLowerBound : Infinity, terms };
  }

  function rangeStatePotential(state) {
    if (!state || state.exhausted) return { potential: 0, tieDocLowerBound: Infinity, terms: 0 };
    if (!state.initialized) {
      return {
        potential: state.range.upperBound || 0,
        tieDocLowerBound: state.range.index * state.rangeSize,
        terms: state.range.termHits || 0
      };
    }
    return remainingBlockPotential(state.remainingTermBounds, 0, state.remainingTermTieDocs);
  }

  function compareRangeStatePotential(a, b) {
    if (a.potential !== b.potential) return b.potential - a.potential;
    if (a.tieDocLowerBound !== b.tieDocLowerBound) return a.tieDocLowerBound - b.tieDocLowerBound;
    return a.ordinal - b.ordinal;
  }

  function bestDocRangeState(states) {
    let best = null;
    let bestPotential = null;
    for (const state of states) {
      const potential = rangeStatePotential(state);
      if (potential.potential <= 0) continue;
      const candidate = { ...potential, ordinal: state.ordinal };
      if (!best || compareRangeStatePotential(candidate, bestPotential) < 0) {
        best = state;
        bestPotential = candidate;
      }
    }
    return best;
  }

  function maxDocRangeOutsidePotential(states) {
    let potential = 0;
    let tieDocLowerBound = Infinity;
    let terms = 0;
    for (const state of states) {
      const current = rangeStatePotential(state);
      if (current.potential > potential) {
        potential = current.potential;
        tieDocLowerBound = current.tieDocLowerBound;
        terms = current.terms;
      } else if (current.potential === potential && current.potential > 0) {
        tieDocLowerBound = Math.min(tieDocLowerBound, current.tieDocLowerBound);
        terms = Math.max(terms, current.terms);
      }
    }
    return { potential, tieDocLowerBound, terms };
  }

  function stableDocRangeGlobalTopK(scores, hits, masks, minShouldMatch, k, rangeSize, stateByRangeIndex, states, proofStats = null) {
    if (proofStats) {
      proofStats.attempts++;
      proofStats.docRangeAware = true;
    }
    const eligible = collectEligibleScores(scores, hits, minShouldMatch);
    const outside = maxDocRangeOutsidePotential(states);
    if (eligible.length < k) {
      recordTopKProofFailure(proofStats, "candidate_count", { maxOutsidePotential: outside.potential });
      return null;
    }

    const top = eligible.slice(0, k);
    const topDocs = new Set(top.map(([doc]) => doc));
    const threshold = top[top.length - 1][1];
    const boundaryDoc = top[top.length - 1][0];
    let maxOutsidePotential = outside.potential;
    let maxOutsideTieDoc = outside.tieDocLowerBound;
    let maxRemainingTerms = outside.terms;
    let maxRemainingTermUpperBound = outside.potential;

    if (maxOutsidePotential > threshold || (maxOutsidePotential === threshold && maxOutsideTieDoc <= boundaryDoc)) {
      recordTopKProofFailure(proofStats, maxOutsidePotential === threshold ? "tie_bound" : "score_bound", {
        threshold,
        maxOutsidePotential,
        remainingTerms: maxRemainingTerms,
        remainingTermUpperBound: maxRemainingTermUpperBound
      });
      return null;
    }

    for (const [doc, score] of scores) {
      if (topDocs.has(doc)) continue;
      const state = stateByRangeIndex.get(Math.floor(doc / rangeSize));
      const remaining = state?.initialized && !state.exhausted
        ? remainingBlockPotential(state.remainingTermBounds, masks.get(doc) || 0, state.remainingTermTieDocs)
        : { potential: 0, tieDocLowerBound: Infinity, terms: 0 };
      const potential = score + remaining.potential;
      if (
        potential > maxOutsidePotential
        || (potential === maxOutsidePotential && doc < maxOutsideTieDoc)
      ) {
        maxOutsidePotential = potential;
        maxOutsideTieDoc = doc;
        maxRemainingTerms = remaining.terms;
        maxRemainingTermUpperBound = remaining.potential;
      }
      if (potential > threshold || (potential === threshold && doc < boundaryDoc)) {
        recordTopKProofFailure(proofStats, potential === threshold ? "tie_bound" : "score_bound", {
          threshold,
          maxOutsidePotential: potential,
          remainingTerms: remaining.terms,
          remainingTermUpperBound: remaining.potential
        });
        return null;
      }
    }

    recordTopKProofSuccess(proofStats, {
      threshold,
      maxOutsidePotential,
      remainingTerms: maxRemainingTerms,
      remainingTermUpperBound: maxRemainingTermUpperBound
    });
    return top;
  }

  async function runDocRangeUpperBoundSearch({
    page,
    size,
    baseTerms,
    terms,
    entries,
    hasFilters,
    blockFilterPlan,
    docFilterPlan,
    fallbackCodeData,
    rerank,
    candidateK,
    minShouldMatch,
    includeResults
  }) {
    if (!docRangePlannerEnabled || !entries.length) return null;
    const baseSet = new Set(baseTerms);
    const plan = buildDocRangeUpperBoundPlan(entries, baseSet, minShouldMatch);
    if (!plan || plan.candidates.length < DOC_RANGE_PLANNER_MIN_CANDIDATE_RANGES) return null;
    const selectivity = docRangePlannerSelectivity(plan, entries, blockFilterPlan);
    const maxRatio = Number.isFinite(Number(options.docRangePlannerMaxCandidateBlockRatio))
      ? Number(options.docRangePlannerMaxCandidateBlockRatio)
      : DOC_RANGE_PLANNER_MAX_CANDIDATE_BLOCK_RATIO;
    if (selectivity.ratio > maxRatio) return null;

    const offset = (page - 1) * size;
    const cursors = entries.map((item, termIndex) => ({
      ...item,
      termIndex,
      isBase: baseSet.has(item.term)
    }));
    const impactTierTerms = cursors.reduce((sum, cursor) => sum + (cursor.entry.impactTiers?.blocks?.length ? 1 : 0), 0);
    const initialBatchLimit = Math.min(
      docRangeBlockPruneBatchSize,
      Math.max(
        docRangeBlockPruneInitialBatchSize,
        Math.ceil(candidateK / 16) * docRangeBlockPruneInitialBatchSize
      )
    );
    const rangeStates = plan.candidates.map((range, ordinal) => ({
      range,
      ordinal,
      rangeSize: plan.rangeSize,
      initialized: false,
      exhausted: false,
      queues: [],
      remainingTermBounds: new Array(cursors.length).fill(0),
      remainingTermTieDocs: new Array(cursors.length).fill(Infinity),
      batchLimit: initialBatchLimit
    }));
    const stateByRangeIndex = new Map(rangeStates.map(state => [state.range.index, state]));
    const scores = new Map();
    const hits = new Map();
    const masks = new Map();
    const decodedBlocks = new Set();
    const proofStats = createTopKProofStats({ hasFilters, blockFilterPlan });
    let stable = null;
    let exhausted = false;
    let rangesVisited = 0;
    let rangeRevisits = 0;
    let candidatePostingBlocks = 0;
    let consideredPostingBlocks = 0;
    let skippedPostingBlocks = 0;
    let consideredPostingSuperblocks = 0;
    let skippedPostingSuperblocks = 0;
    let impactTierTasks = 0;
    let blocksDecoded = 0;
    let blocksVisited = 0;
    let postingsDecoded = 0;
    let postingRowsScanned = 0;
    let postingsAccepted = 0;
    let fetchedBlocks = 0;
    let fetchGroups = 0;
    let wantedBlocks = 0;
    let filterSummaryProofBlocks = 0;
    let docRangeBlockBatches = 0;
    let docRangeInnerBlocksPruned = 0;
    let stopUpperBound = 0;

    const refreshQueueBound = (state, queue) => {
      const task = queue.tasks[queue.offset];
      state.remainingTermBounds[queue.termIndex] = task?.upperBound || 0;
      state.remainingTermTieDocs[queue.termIndex] = task?.tieDocLowerBound ?? Infinity;
    };
    const remainingQueueBlocks = (state) => state.queues.reduce((sum, queue) => sum + Math.max(0, queue.tasks.length - queue.offset), 0);
    const initializeRangeState = (state) => {
      if (state.initialized) {
        rangeRevisits++;
        return;
      }
      state.initialized = true;
      rangesVisited++;
      const range = state.range;
      const rangeStart = range.index * plan.rangeSize;
      const rangeEnd = Math.min(manifest.total, rangeStart + plan.rangeSize);
      for (const cursor of cursors) {
        const candidates = docRangeCandidateBlockTasks(cursor, rangeStart, rangeEnd, range.index, blockFilterPlan);
        candidatePostingBlocks += candidates.tasks.length;
        consideredPostingBlocks += candidates.consideredBlocks;
        skippedPostingBlocks += candidates.skippedBlocks;
        consideredPostingSuperblocks += candidates.consideredSuperblocks;
        skippedPostingSuperblocks += candidates.skippedSuperblocks;
        impactTierTasks += candidates.tasks.filter(task => task.impactRank !== Number.MAX_SAFE_INTEGER).length;
        const tasks = candidates.tasks;
        if (!tasks.length) continue;
        const queue = { cursor, termIndex: cursor.termIndex, tasks, offset: 0 };
        state.queues.push(queue);
        refreshQueueBound(state, queue);
      }
      if (!remainingQueueBlocks(state)) state.exhausted = true;
    };

    while (true) {
      stopUpperBound = maxDocRangeOutsidePotential(rangeStates).potential;
      stable = stableDocRangeGlobalTopK(
        scores,
        hits,
        masks,
        minShouldMatch,
        candidateK,
        plan.rangeSize,
        stateByRangeIndex,
        rangeStates,
        proofStats
      );
      if (stable) {
        docRangeInnerBlocksPruned += rangeStates.reduce((sum, state) => sum + (state.initialized ? remainingQueueBlocks(state) : 0), 0);
        break;
      }

      const state = bestDocRangeState(rangeStates);
      if (!state) break;
      initializeRangeState(state);
      if (state.exhausted) continue;

      const range = state.range;
      const rangeStart = range.index * plan.rangeSize;
      const rangeEnd = Math.min(manifest.total, rangeStart + plan.rangeSize);
      const batch = [];
      const batchLimit = Math.min(docRangeBlockPruneBatchSize, state.batchLimit);
      while (batch.length < batchLimit) {
        let bestQueue = null;
        let bestTask = null;
        for (const queue of state.queues) {
          const task = queue.tasks[queue.offset];
          if (!task) continue;
          if (!bestTask || compareDocRangeTasks(task, bestTask) < 0) {
            bestQueue = queue;
            bestTask = task;
          }
        }
        if (!bestQueue || !bestTask) break;
        bestQueue.offset++;
        refreshQueueBound(state, bestQueue);
        batch.push({ cursor: bestQueue.cursor, blockIndex: bestTask.blockIndex });
      }
      if (!batch.length) {
        state.exhausted = true;
        continue;
      }

      docRangeBlockBatches++;
      const decoded = await decodeCursorBlockBatch(batch, "postingDocRanges");
      fetchedBlocks += decoded.fetchedBlocks;
      fetchGroups += decoded.fetchGroups;
      wantedBlocks += decoded.wantedBlocks;
      const pendingBlocks = [];
      for (const { cursor, blockIndex, rows } of decoded.blocks) {
        const block = cursor.entry.blocks?.[blockIndex];
        const blockKey = postingBlockKey(cursor, blockIndex);
        if (!decodedBlocks.has(blockKey)) {
          decodedBlocks.add(blockKey);
          blocksDecoded++;
        }
        blocksVisited++;
        postingRowsScanned += rows.length / 2;
        const docsInRange = hasFilters ? postingDocsInRange(rows, rangeStart, rangeEnd) : null;
        if (hasFilters && !docsInRange.length) continue;
        const filterSummaryProvesBlock = hasFilters && blockDefinitelyPassesDocFilter(block, docFilterPlan);
        if (filterSummaryProvesBlock) filterSummaryProofBlocks++;
        pendingBlocks.push({ cursor, rows, docsInRange, filterSummaryProvesBlock });
      }
      // One chunk-cache warm-up for the whole batch: per-block filter
      // verification below then reads cached chunks instead of issuing one
      // small range request per block.
      if (hasFilters) {
        await prefetchFilterPlanDocValues(
          docFilterPlan,
          pendingBlocks.filter(item => !item.filterSummaryProvesBlock).map(item => item.docsInRange)
        );
      }
      for (const { cursor, rows, docsInRange, filterSummaryProvesBlock } of pendingBlocks) {
        const codeData = hasFilters && !filterSummaryProvesBlock && docValues
          ? await valueStoreForFilterPlan(docFilterPlan, docsInRange)
          : filterSummaryProvesBlock ? null : fallbackCodeData;
        postingsDecoded += rows.length / 2;
        postingsAccepted += applyBlockRowsInDocRange(
          cursor,
          rows,
          rangeStart,
          rangeEnd,
          codeData,
          filterSummaryProvesBlock ? null : docFilterPlan,
          scores,
          hits,
          masks
        );
      }
      state.exhausted = remainingQueueBlocks(state) === 0;
      state.batchLimit = Math.min(docRangeBlockPruneBatchSize, Math.max(1, state.batchLimit * 2));
    }
    exhausted = !stable && maxDocRangeOutsidePotential(rangeStates).potential <= 0;

    let ranked = exhausted
      ? collectEligibleScores(scores, hits, minShouldMatch)
      : stable || topEligibleScores(scores, hits, minShouldMatch, candidateK).top;
    const reranked = rerank === false
      ? { ranked, stats: { rerankCandidates: 0, dependencyFeatures: 0, dependencyTermsMatched: 0, dependencyPostingsScanned: 0, dependencyCandidateMatches: 0 } }
      : await rerankWithDependencies(ranked, baseTerms, candidateK);
    ranked = reranked.ranked;
    const rows = ranked.slice(offset, offset + size);
    const resultContext = { hasTextTerms: true, preferDocPages: "auto" };
    const results = await rowsToSearchResults(rows, resultContext, includeResults);
    return {
      total: exhausted ? ranked.length : Math.max(ranked.length, offset + size),
      page,
      size,
      approximate: !exhausted,
      results,
      stats: {
        exact: exhausted,
        plannerLane: exhausted ? "docRangeBlockMaxExhausted" : "docRangeBlockMax",
        topKProven: Boolean(stable || exhausted),
        totalExact: exhausted,
        tailExhausted: exhausted,
        blocksDecoded,
        postingsDecoded,
        postingsAccepted,
        skippedBlocks: skippedPostingBlocks,
        terms: terms.length,
        shards: new Set(entries.map(item => item.shardName)).size,
        docRangeBlockMax: true,
        docRangeSize: plan.rangeSize,
        docRangeCandidateRanges: plan.candidates.length,
        docRangeRangesVisited: rangesVisited,
        docRangeRangesPruned: Math.max(0, plan.candidates.length - rangesVisited),
        docRangeRangeRevisits: rangeRevisits,
        docRangeNextUpperBound: stopUpperBound,
        docRangeTermRangeEntries: plan.termRangeEntries,
        docRangeCandidateBlockRatio: selectivity.ratio,
        docRangeSelectivitySampleBlocks: selectivity.candidateBlocks,
        docRangeSelectivityAvailableBlocks: selectivity.availableBlocks,
        docRangeBlocksVisited: blocksVisited,
        docRangePostingRowsScanned: postingRowsScanned,
        docRangePostingBlocksConsidered: consideredPostingBlocks,
        docRangePostingBlocksCandidate: candidatePostingBlocks,
        docRangePostingBlocksProcessed: blocksVisited,
        docRangePostingBlocksSkipped: skippedPostingBlocks,
        docRangeInnerBlockBatches: docRangeBlockBatches,
        docRangeInnerBlocksPruned,
        docRangeInitialBatchLimit: initialBatchLimit,
        docRangeImpactPlanner: docRangeImpactPlannerEnabled && impactTierTerms > 0,
        docRangeImpactTierTerms: impactTierTerms,
        docRangeImpactTierTasks: impactTierTasks,
        docRangeImpactSeed: false,
        docRangeImpactSeedBlocks: 0,
        docRangeImpactSeedRowsScanned: 0,
        docRangeImpactSeedPostingsAccepted: 0,
        docRangeImpactSeedFetchedBlocks: 0,
        docRangeImpactSeedFetchGroups: 0,
        docRangeImpactSeedWantedBlocks: 0,
        docRangeImpactSeedIndexedTerms: impactTierTerms,
        docRangeImpactSeedScannedTerms: 0,
        docRangePostingSuperblocksConsidered: consideredPostingSuperblocks,
        docRangePostingSuperblocksSkipped: skippedPostingSuperblocks,
        docRangeFetchedBlocks: fetchedBlocks,
        docRangeFetchGroups: fetchGroups,
        docRangeWantedBlocks: wantedBlocks,
        filterSummaryProofBlocks,
        plannerFallbackReason: exhausted ? "range_exhausted" : "",
        ...topKProofStatsObject(proofStats, exhausted ? "range_exhausted" : ""),
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        docPayloadForced: resultContext.docPayloadForced,
        ...reranked.stats
      }
    };
  }

  async function runSkippedSearch({ q, page, size, filters, sort, baseTerms, terms, rerank = true, includeResults = true }) {
    return traceSpan("text.search", () => runSkippedSearchInner({ q, page, size, filters, sort, baseTerms, terms, rerank, includeResults }));
  }

  async function runSkippedSearchInner({ q, page, size, filters, sort, baseTerms, terms, rerank = true, includeResults = true }) {
    const offset = (page - 1) * size;
    const k = offset + size;
    const candidateK = candidateLimitFor(baseTerms, k, rerank);
    const sortPlan = makeSortPlan(sort);
    if (sortPlan) {
      const sortedText = await runSortedTextSearch({ page, size, filters, sortPlan, baseTerms, terms, rerank });
      if (sortedText) return sortedText;
      return runFullSearch({ q, page, size, filters, sort, baseTerms, terms, rerank, includeResults, plannerFallbackReason: "sort_requested" });
    }
    if (!baseTerms.length || terms.length > SKIP_MAX_TERMS || k > topKProofMaxK) {
      const plannerFallbackReason = !baseTerms.length
        ? "no_text_terms"
        : terms.length > SKIP_MAX_TERMS
          ? "too_many_terms"
          : "top_k_limit";
      return runFullSearch({ q, page, size, filters, sort, baseTerms, terms, rerank, includeResults, plannerFallbackReason });
    }

    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    const bundleResponse = await tryQueryBundleSearch({ page, size, baseTerms, filters, sortPlan, rerank, includeResults });
    if (bundleResponse) return bundleResponse;

    if (hasFilters) await ensureDocValuesManifest();
    await ensureFacetDictionaries(filters);
    const blockFilterPlan = hasFilters ? makeBlockFilterPlan(filters) : null;
    const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
    const filterFields = filterPlanFields(docFilterPlan);
    const fallbackCodeData = hasFilters && !docValues ? await loadCodes() : null;
    const entries = await termEntries(terms);
    const baseSet = new Set(baseTerms);
    const minShouldMatch = minShouldMatchFor(baseTerms);
    const presentBaseTerms = new Set(entries.filter(item => baseSet.has(item.term)).map(item => item.term));
    if (presentBaseTerms.size < Math.max(1, minShouldMatch)) {
      return emptyTextSearchResponse({
        page,
        size,
        terms,
        entries,
        missingBaseTerms: Math.max(0, baseTerms.length - presentBaseTerms.size)
      });
    }
    const docRangeResponse = await runDocRangeUpperBoundSearch({
      page,
      size,
      baseTerms,
      terms,
      entries,
      hasFilters,
      blockFilterPlan,
      docFilterPlan,
      fallbackCodeData,
      rerank,
      candidateK,
      minShouldMatch,
      includeResults
    });
    if (docRangeResponse) return docRangeResponse;
    const cursors = entries.map((item, termIndex) => ({
      ...item,
      termIndex,
      isBase: baseSet.has(item.term),
      blockIndex: 0,
      superblockIndex: 0,
      skippedBlocks: 0,
      skippedSuperblocks: 0,
      superblocksConsidered: 0,
      superblocksDecoded: 0
    }));
    if (!cursors.length) {
      return emptyTextSearchResponse({ page, size, terms });
    }

    const scores = new Map();
    const hits = new Map();
    const masks = new Map();
    let blocksDecoded = 0;
    let postingsDecoded = 0;
    let postingsAccepted = 0;
    let stable = null;
    let exhausted = false;
    let frontierBatches = 0;
    let frontierBlocks = 0;
    let frontierFetchedBlocks = 0;
    let frontierFetchGroups = 0;
    let frontierWantedBlocks = 0;
    let frontierMax = 0;
    let filterSummaryProofBlocks = 0;
    let currentProofInterval = topKProofCheckInterval;
    let blocksSinceProofCheck = topKProofCheckInterval;
    let budgetExhausted = false;
    const proofStats = createTopKProofStats({ hasFilters, blockFilterPlan });

    function proofFailed() {
      blocksSinceProofCheck = 0;
      currentProofInterval = Math.min(
        topKProofCheckIntervalMax,
        Math.max(topKProofCheckInterval, Math.floor(scores.size / topKProofCheckScoresPerBlock))
      );
    }

    while (true) {
      const active = cursors.filter(cursor => advanceCursor(cursor, blockFilterPlan));
      if (!active.length) {
        exhausted = true;
        break;
      }

      if (blocksSinceProofCheck >= currentProofInterval) {
        stable = stableTopK(scores, hits, masks, cursors, minShouldMatch, candidateK, blockFilterPlan, proofStats);
        if (stable) break;
        proofFailed();
      }

      active.sort((a, b) => cursorSuperblockImpact(b) - cursorSuperblockImpact(a) || cursorImpact(b) - cursorImpact(a));
      const frontier = active.slice(0, postingBlockFrontier);
      frontierBatches++;
      frontierBlocks += frontier.length;
      frontierMax = Math.max(frontierMax, frontier.length);
      const decoded = await decodeCursorFrontier(frontier);
      frontierFetchedBlocks += decoded.fetchedBlocks;
      frontierFetchGroups += decoded.fetchGroups;
      frontierWantedBlocks += decoded.wantedBlocks;
      // Each frontier cursor contributes exactly one block per batch, so the
      // block summaries can be read (without advancing cursors) to warm the
      // doc-value chunk cache for the whole batch at once — the per-block
      // verification below then reads cached chunks instead of issuing one
      // small range request per block. Cursor advancement and the top-k
      // proofs stay interleaved: a proof must still see unapplied blocks as
      // remaining potential.
      const batchBlocks = decoded.blocks.map(({ cursor, rows }) => ({
        cursor,
        rows,
        filterSummaryProvesBlock: hasFilters
          && blockDefinitelyPassesDocFilter(cursor.entry.blocks[cursor.blockIndex], docFilterPlan)
      }));
      if (hasFilters) {
        await prefetchFilterPlanDocValues(
          docFilterPlan,
          batchBlocks.filter(item => !item.filterSummaryProvesBlock).map(item => postingDocs(item.rows))
        );
      }
      for (const { cursor, rows, filterSummaryProvesBlock } of batchBlocks) {
        if (filterSummaryProvesBlock) filterSummaryProofBlocks++;
        markSuperblockDecoded(cursor);
        cursor.blockIndex++;
        const codeData = hasFilters && !filterSummaryProvesBlock && docValues
          ? await valueStoreForFilterPlan(docFilterPlan, postingDocs(rows))
          : filterSummaryProvesBlock ? null : fallbackCodeData;
        blocksDecoded++;
        postingsDecoded += rows.length / 2;
        postingsAccepted += applyBlockRows(cursor, rows, codeData, filterSummaryProvesBlock ? null : docFilterPlan, scores, hits, masks);
        blocksSinceProofCheck++;
        if (blocksSinceProofCheck >= currentProofInterval) {
          stable = stableTopK(scores, hits, masks, cursors, minShouldMatch, candidateK, blockFilterPlan, proofStats);
          if (stable) break;
          proofFailed();
        }
        if (topKBlockBudget > 0 && blocksDecoded >= topKBlockBudget) {
          budgetExhausted = true;
          break;
        }
      }
      if (stable || budgetExhausted) break;
    }

    let ranked = exhausted
      ? collectEligibleScores(scores, hits, minShouldMatch)
      : stable || topEligibleScores(scores, hits, minShouldMatch, k).top;
    const reranked = rerank === false
      ? { ranked, stats: { rerankCandidates: 0, dependencyFeatures: 0, dependencyTermsMatched: 0, dependencyPostingsScanned: 0, dependencyCandidateMatches: 0 } }
      : await rerankWithDependencies(ranked, baseTerms, candidateK);
    ranked = reranked.ranked;
    const rows = ranked.slice(offset, offset + size);
    const resultContext = { hasTextTerms: true, preferDocPages: "auto" };
    const results = await rowsToSearchResults(rows, resultContext, includeResults);
    return {
      total: exhausted ? ranked.length : Math.max(ranked.length, k),
      page,
      size,
      approximate: budgetExhausted || !exhausted,
      results,
      stats: {
        exact: exhausted && !budgetExhausted,
        plannerLane: budgetExhausted ? "blockBudget" : exhausted ? "fullFallback" : "tailProof",
        topKProven: !budgetExhausted && Boolean(stable || exhausted),
        totalExact: exhausted && !budgetExhausted,
        tailExhausted: exhausted,
        blocksDecoded,
        postingsDecoded,
        postingsAccepted,
        skippedBlocks: cursors.reduce((sum, cursor) => sum + cursor.skippedBlocks, 0),
        terms: terms.length,
        shards: new Set(entries.map(item => item.shardName)).size,
        postingBlockFrontier: postingBlockFrontier,
        postingBlockFrontierBatches: frontierBatches,
        postingBlockFrontierBlocks: frontierBlocks,
        postingBlockFrontierMax: frontierMax,
        postingBlockFrontierFetchedBlocks: frontierFetchedBlocks,
        postingBlockFrontierFetchGroups: frontierFetchGroups,
        postingBlockFrontierWantedBlocks: frontierWantedBlocks,
        topKProofCheckInterval,
        topKBlockBudget,
        topKBlockBudgetExhausted: budgetExhausted,
        postingSuperblockScheduler: cursors.some(cursor => cursor.entry.superblocks?.length),
        postingSuperblocks: entries.reduce((sum, item) => sum + (item.entry.superblocks?.length || 0), 0),
        postingSuperblocksConsidered: cursors.reduce((sum, cursor) => sum + cursor.superblocksConsidered, 0),
        postingSuperblocksSkipped: cursors.reduce((sum, cursor) => sum + cursor.skippedSuperblocks, 0),
        postingSuperblocksDecoded: cursors.reduce((sum, cursor) => sum + cursor.superblocksDecoded, 0),
        filterSummaryProofBlocks,
        plannerFallbackReason: budgetExhausted ? "block_budget" : exhausted ? "tail_exhausted" : "",
        ...topKProofStatsObject(proofStats, budgetExhausted ? "block_budget" : exhausted ? "tail_exhausted" : ""),
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        docPayloadForced: resultContext.docPayloadForced,
        ...reranked.stats
      }
    };
  }

  async function runSegmentFanoutSearch({ page, size, filters, sort, baseTerms, terms, rerank = true, includeResults = true, plannerFallbackReason = "exact_requested" }) {
    const offset = (page - 1) * size;
    const sortPlan = makeSortPlan(sort);
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    if (hasFilters || sortPlan) await ensureDocValuesManifest();
    await ensureFacetDictionaries(filters);
    const segmentSearch = await segmentTermEntries(terms);
    if (!segmentSearch) return null;
    const entries = segmentSearch.entries;
    const baseSet = new Set(baseTerms);
    const minShouldMatch = minShouldMatchFor(baseTerms);
    const presentBaseTerms = new Set(entries.filter(item => baseSet.has(item.term)).map(item => item.term));
    if (presentBaseTerms.size < Math.max(1, minShouldMatch)) {
      return emptyTextSearchResponse({
        page,
        size,
        terms,
        entries,
        missingBaseTerms: Math.max(0, baseTerms.length - presentBaseTerms.size)
      });
    }

    const decoded = [];
    const candidateDocs = new Set();
    let postingsDecoded = 0;
    for (const item of entries) {
      const rows = await decodeSegmentEntryPostings(item, segmentSearch.dfs.get(item.term));
      decoded.push({ ...item, rows });
      postingsDecoded += rows.length / 2;
      for (let i = 0; i < rows.length; i += 2) candidateDocs.add(rows[i]);
    }

    const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
    const fallbackCodeData = (hasFilters || sortPlan) && !docValues ? await loadCodes() : null;
    let codeData = fallbackCodeData;
    if (hasFilters && docValues) codeData = await valueStoreForFilterPlan(docFilterPlan, [...candidateDocs]);

    const scores = new Map();
    const hits = new Map();
    let postingsAccepted = 0;
    for (const { term, rows } of decoded) {
      const isBase = baseSet.has(term);
      for (let i = 0; i < rows.length; i += 2) {
        const doc = rows[i];
        if (codeData && !passesFilterPlan(doc, codeData, docFilterPlan)) continue;
        scores.set(doc, (scores.get(doc) || 0) + rows[i + 1]);
        if (isBase) hits.set(doc, (hits.get(doc) || 0) + 1);
        postingsAccepted++;
      }
    }

    let ranked = collectEligibleScores(scores, hits, minShouldMatch);
    const reranked = rerank === false || sortPlan
      ? { ranked, stats: { rerankCandidates: 0, dependencyFeatures: 0, dependencyTermsMatched: 0, dependencyPostingsScanned: 0, dependencyCandidateMatches: 0 } }
      : await rerankWithDependencies(ranked, baseTerms, candidateLimitFor(baseTerms, offset + size, rerank));
    if (sortPlan && docValues) codeData = await valueStoreForDocs([sortPlan.field], reranked.ranked.map(([doc]) => doc));
    ranked = sortRanked(reranked.ranked, codeData, sortPlan);
    const rows = ranked.slice(offset, offset + size);
    const resultContext = { hasTextTerms: !!baseTerms.length, preferDocPages: sortPlan ? true : "auto" };
    const results = await rowsToSearchResults(rows, resultContext, includeResults);
    return {
      total: ranked.length,
      page,
      size,
      results,
      approximate: false,
      stats: {
        exact: true,
        plannerLane: "segmentFanoutExact",
        topKProven: true,
        totalExact: true,
        tailExhausted: true,
        terms: terms.length,
        shards: new Set(entries.map(item => item.segment.id || item.segmentOrdinal)).size,
        segments: segmentSearch.manifest.segmentCount || segmentSearch.manifest.segments.length,
        segmentEntries: entries.length,
        segmentPublished: true,
        postings: postingsDecoded,
        blocksDecoded: entries.length,
        postingsDecoded,
        postingsAccepted,
        skippedBlocks: 0,
        plannerFallbackReason,
        ...topKProofStatsObject(null, plannerFallbackReason),
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        docPayloadForced: resultContext.docPayloadForced,
        ...reranked.stats
      }
    };
  }

  async function runFullSearch({ q, page, size, filters, sort, baseTerms, terms, rerank = true, includeResults = true, plannerFallbackReason = "full_scan" }) {
    if (plannerFallbackReason === "exact_requested" || options.segmentFanout === true) {
      const segmentResponse = await runSegmentFanoutSearch({ page, size, filters, sort, baseTerms, terms, rerank, includeResults, plannerFallbackReason });
      if (segmentResponse) return segmentResponse;
    }
    const offset = (page - 1) * size;
    const sortPlan = makeSortPlan(sort);
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    if (hasFilters || sortPlan) await ensureDocValuesManifest();
    await ensureFacetDictionaries(filters);
    const entries = await termEntries(terms);
    const baseSet = new Set(baseTerms);
    const minShouldMatch = minShouldMatchFor(baseTerms);
    const presentBaseTerms = new Set(entries.filter(item => baseSet.has(item.term)).map(item => item.term));
    if (presentBaseTerms.size < Math.max(1, minShouldMatch)) {
      return emptyTextSearchResponse({
        page,
        size,
        terms,
        entries,
        missingBaseTerms: Math.max(0, baseTerms.length - presentBaseTerms.size)
      });
    }
    const scores = new Map();
    const hits = new Map();
    const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
    const filterFields = filterPlanFields(docFilterPlan);
    const fallbackCodeData = (hasFilters || sortPlan) && !docValues ? await loadCodes() : null;
    let codeData = fallbackCodeData;

    if (hasFilters && docValues) {
      const docs = new Set();
      for (const { shard, entry } of entries) {
        const postings = await decodeEntryPostings(shard, entry);
        for (let i = 0; i < postings.length; i += 2) docs.add(postings[i]);
      }
      codeData = await valueStoreForFilterPlan(docFilterPlan, [...docs]);
    }

    for (const { term, shard, entry } of entries) {
      const postings = await decodeEntryPostings(shard, entry);
      const isBase = baseSet.has(term);
      for (let i = 0; i < postings.length; i += 2) {
        const doc = postings[i];
        if (codeData && !passesFilterPlan(doc, codeData, docFilterPlan)) continue;
        scores.set(doc, (scores.get(doc) || 0) + postings[i + 1]);
        if (isBase) hits.set(doc, (hits.get(doc) || 0) + 1);
      }
    }

    let ranked = collectEligibleScores(scores, hits, minShouldMatchFor(baseTerms));
    const reranked = rerank === false || sortPlan
      ? { ranked, stats: { rerankCandidates: 0, dependencyFeatures: 0, dependencyTermsMatched: 0, dependencyPostingsScanned: 0, dependencyCandidateMatches: 0 } }
      : await rerankWithDependencies(ranked, baseTerms, candidateLimitFor(baseTerms, offset + size, rerank));
    if (sortPlan && docValues) codeData = await valueStoreForDocs([sortPlan.field], reranked.ranked.map(([doc]) => doc));
    ranked = sortRanked(reranked.ranked, codeData, sortPlan);
    const rows = ranked.slice(offset, offset + size);
    const resultContext = { hasTextTerms: !!baseTerms.length, preferDocPages: sortPlan ? true : "auto" };
    const results = await rowsToSearchResults(rows, resultContext, includeResults);
    return {
      total: ranked.length,
      page,
      size,
      results,
      approximate: false,
      stats: {
        exact: true,
        plannerLane: "fullFallback",
        topKProven: true,
        totalExact: true,
        tailExhausted: true,
        terms: terms.length,
        shards: new Set(entries.map(item => item.shardName)).size,
        postings: entries.reduce((sum, item) => sum + item.entry.count, 0),
        blocksDecoded: entries.reduce((sum, item) => sum + (item.entry.blocks?.length || 0), 0),
        postingsDecoded: entries.reduce((sum, item) => sum + item.entry.count, 0),
        postingsAccepted: ranked.length,
        skippedBlocks: 0,
        plannerFallbackReason,
        ...topKProofStatsObject(null, plannerFallbackReason),
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        docPayloadForced: resultContext.docPayloadForced,
        ...reranked.stats
      }
    };
  }

  async function typoCandidatesForToken(item, debug) {
    const raw = String(item.raw || "");
    const token = String(item.term || raw);
    const scoringToken = isTypoCorrectionToken(raw) ? raw : token;
    if (!isTypoCorrectionToken(scoringToken) || !isTypoCorrectionToken(token)) return [];
    const maxEdits = typoMaxEditsFor(scoringToken, runtimeTypo);
    const probeValues = mainIndexTypoProbeValues(raw, token, {
      ...runtimeTypo,
      maxShardLookups: Math.max(runtimeTypo.maxShardLookups * 2, 24)
    });
    const byShard = new Map();
    for (const probe of probeValues) {
      const resolved = await resolveDirectoryShard(
        probe,
        termDirectory,
        manifest.stats?.base_shard_depth || 3,
        manifest.stats?.max_shard_depth || manifest.stats?.base_shard_depth || 5
      );
      if (!resolved || byShard.has(resolved.shard)) continue;
      if (!debug.shards.has(resolved.shard) && debug.shards.size + byShard.size >= runtimeTypo.maxShardLookups) break;
      byShard.set(resolved.shard, resolved);
    }

    const loaded = await loadShards([...byShard.values()]);
    const candidates = new Map();
    for (const [shard, resolved] of byShard) {
      debug.shards.add(shard);
      const data = loaded.get(resolved.shard);
      if (!data) continue;
      for (const [candidateTerm, entry] of data.terms) {
        debug.scanned++;
        if (candidateTerm === token || !isTypoCorrectionToken(candidateTerm)) continue;
        if (Math.abs(candidateTerm.length - token.length) > maxEdits + 4) continue;
        const overlap = Math.max(ngramOverlap(scoringToken, candidateTerm), ngramOverlap(token, candidateTerm));
        if (overlap < (scoringToken.length <= 5 ? 0.2 : 0.25) && scoringToken[0] !== candidateTerm[0]) continue;
        const bestDistance = bestMainIndexTypoDistance(scoringToken, candidateTerm, maxEdits);
        let distance = bestDistance.distance;
        let surface = bestDistance.surface;
        if (distance > maxEdits && token !== scoringToken) {
          const termDistance = bestMainIndexTypoDistance(token, candidateTerm, maxEdits);
          distance = termDistance.distance;
          surface = termDistance.surface;
        }
        if (distance <= 0 || distance > maxEdits) continue;
        const df = entry.df || 0;
        const score = mainIndexTypoCandidateScore(scoringToken, surface, df, distance);
        const candidate = { surface, term: candidateTerm, df, distance, score };
        const previous = candidates.get(candidate.term);
        if (!previous || candidate.score > previous.score || (candidate.score === previous.score && candidate.df > previous.df)) {
          candidates.set(candidate.term, candidate);
        }
      }
    }
    const verified = [...candidates.values()]
      .sort((a, b) => b.score - a.score || a.distance - b.distance || b.df - a.df || a.term.localeCompare(b.term))
      .slice(0, runtimeTypo.maxTokenCandidates);
    debug.candidates += verified.length;
    return verified;
  }

  async function correctedTypoQuery(baseTerms, analyzedTerms) {
    return traceSpan("typo.resolve", () => correctedTypoQueryInner(baseTerms, analyzedTerms));
  }

  async function correctedTypoQueryInner(baseTerms, analyzedTerms) {
    if (!baseTerms.length || baseTerms.length > 8) return null;
    const presentTerms = new Map((await termEntries(baseTerms)).map(item => [item.term, item.entry.df || 0]));
    const hasMissingTerms = baseTerms.some(term => !presentTerms.has(term));
    const plans = new Map();
    const debug = { shards: new Set(), candidates: 0, scanned: 0, tokens: [] };

    // If every token exists but their intersection is empty, a rare typo can
    // masquerade as a legitimate term. Probe the lowest-df token first so a
    // common leading word cannot consume the shared shard budget on obscure
    // variants before the conflicting token is examined.
    const candidateIndexes = analyzedTerms
      .map((_, index) => index)
      .filter(index => !hasMissingTerms || !presentTerms.has(analyzedTerms[index].term))
      .sort((left, right) => (
        (presentTerms.get(analyzedTerms[left].term) ?? -1) - (presentTerms.get(analyzedTerms[right].term) ?? -1)
        || left - right
      ));
    for (const index of candidateIndexes) {
      const item = analyzedTerms[index];
      const candidates = await typoCandidatesForToken(item, debug);
      if (options.typoDebug) debug.tokens.push({ token: item.term, candidates });
      const strongCandidates = candidates.filter(item => item.score >= 0.5);
      const bestScore = strongCandidates[0]?.score || 0;
      const minScore = bestScore >= 1 ? bestScore * TYPO_CORRECTION_RELATIVE_SCORE : 0.5;
      const perTokenLimit = runtimeTypo.maxTokenCandidates || TYPO_CORRECTION_CANDIDATES_PER_TOKEN;
      for (const candidate of strongCandidates.filter(item => item.score >= minScore).slice(0, perTokenLimit)) {
        const corrected = baseTerms.slice();
        corrected[index] = candidate.term;
        if (corrected[index] === item.term) continue;
        const plan = {
          q: corrected.join(" "),
          baseTerms: corrected,
          corrections: [{
            from: item.raw,
            to: candidate.term,
            surface: candidate.surface,
            distance: candidate.distance,
            df: candidate.df,
            score: Number(candidate.score.toFixed(3))
          }],
          score: candidate.score
        };
        const previous = plans.get(plan.q);
        if (!previous || plan.score > previous.score) plans.set(plan.q, plan);
      }
    }

    const sortedPlans = [...plans.values()].sort((a, b) => b.score - a.score || a.q.localeCompare(b.q));
    if (!sortedPlans.length) return null;
    const selectedPlans = await rankTypoCorrectionPlans(sortedPlans.slice(0, runtimeTypo.maxQueryPlans || TYPO_CORRECTION_PLAN_LIMIT));
    return {
      plans: selectedPlans,
      stats: {
        typoCandidateTerms: debug.candidates,
        typoCorrectionPlans: selectedPlans.length,
        typoCorrectionPlansEstimated: selectedPlans.filter(plan => Number.isFinite(plan.estimatedUpperBound)).length,
        typoCorrectionBestUpperBound: selectedPlans[0]?.estimatedUpperBound || 0,
        typoShardLookups: debug.shards.size,
        typoCandidateShardLookups: debug.shards.size,
        typoCandidateTermsScanned: debug.scanned,
        ...(options.typoDebug ? {
          typoDebugTokens: debug.tokens,
          typoDebugPlans: sortedPlans
        } : {})
      }
    };
  }

  async function rankTypoCorrectionPlans(plans) {
    const estimated = await Promise.all(plans.map(async (plan) => {
      const terms = expandedTermsFromBaseTerms(plan.baseTerms);
      const entries = await termEntries(terms);
      const baseSet = new Set(plan.baseTerms);
      const rangePlan = buildDocRangeUpperBoundPlan(entries, baseSet, minShouldMatchFor(plan.baseTerms));
      const rangeUpperBound = rangePlan?.candidates?.[0]?.upperBound || 0;
      const blockUpperBound = entries.reduce((sum, item) => {
        const blocks = item.entry.blocks || [];
        return sum + blocks.reduce((max, block) => Math.max(max, block.maxImpact || 0), 0);
      }, 0);
      const presentBaseTerms = entries.filter(item => baseSet.has(item.term)).length;
      return {
        ...plan,
        estimatedUpperBound: Math.max(rangeUpperBound, blockUpperBound),
        estimatedRangeUpperBound: rangeUpperBound,
        estimatedBlockUpperBound: blockUpperBound,
        estimatedPresentBaseTerms: presentBaseTerms,
        estimatedTerms: entries.length
      };
    }));
    return estimated.sort((a, b) => (
      b.estimatedUpperBound - a.estimatedUpperBound
      || b.estimatedPresentBaseTerms - a.estimatedPresentBaseTerms
      || b.score - a.score
      || a.q.localeCompare(b.q)
    ));
  }

  function rawSurfaceFallbackTerms(baseTerms, analyzedTerms, presentTerms) {
    let changed = false;
    const terms = baseTerms.map((term, index) => {
      const raw = analyzedTerms[index]?.raw;
      if (raw && raw !== term && presentTerms.has(raw)) {
        changed = true;
        return raw;
      }
      return term;
    });
    return changed ? terms : null;
  }

  async function maybeSurfaceExactFallback(params, response, baseTerms, analyzedTerms) {
    if (params.page !== 1 || response.total > 0) return null;
    if (!analyzedTerms.some(item => item.raw && item.raw !== item.term)) return null;
    const rawTerms = [...new Set(analyzedTerms.map(item => item.raw).filter(Boolean))];
    const presentTerms = new Map((await termEntries(rawTerms)).map(item => [item.term, item.entry.df || 0]));
    const fallbackBaseTerms = rawSurfaceFallbackTerms(baseTerms, analyzedTerms, presentTerms);
    if (!fallbackBaseTerms) return null;
    const fallback = await runSkippedSearch({
      ...params,
      q: fallbackBaseTerms.join(" "),
      baseTerms: fallbackBaseTerms,
      terms: expandedTermsFromBaseTerms(fallbackBaseTerms)
    });
    if (fallback.total <= response.total) return null;
    return {
      ...fallback,
      surfaceFallbackQuery: fallbackBaseTerms.join(" "),
      stats: {
        ...(fallback.stats || {}),
        surfaceFallbackAttempted: true,
        surfaceFallbackApplied: true,
        surfaceFallbackTerms: fallbackBaseTerms,
        typoAttempted: false,
        typoApplied: false,
        typoSkippedReason: "surface-exact"
      }
    };
  }

  async function maybeTypoFallback(params, response, baseTerms, analyzedTerms) {
    return traceSpan("typo.fallback", () => maybeTypoFallbackInner(params, response, baseTerms, analyzedTerms));
  }

  function shouldAttemptTypoFallback(params, response) {
    if (runtimeTypo.mode === "off") return false;
    if (params.page !== 1 || params.sort) return false;
    if (response.total === 0) return true;
    if (runtimeTypo.trigger !== "zero-or-weak") return false;
    return response.total <= (runtimeTypo.weakResultTotal || 0);
  }

  function typoCorrectionShouldReplace(original, corrected) {
    if ((original.total || 0) <= 0) return (corrected.total || 0) > 0;
    return (corrected.total || 0) >= Math.max((original.total || 0) + 2, (original.total || 0) * 1.5);
  }

  async function maybeTypoFallbackInner(params, response, baseTerms, analyzedTerms) {
    if (!shouldAttemptTypoFallback(params, response)) return response;
    const surfaceFallback = await maybeSurfaceExactFallback(params, response, baseTerms, analyzedTerms);
    if (surfaceFallback) return surfaceFallback;
    const correction = await correctedTypoQuery(baseTerms, analyzedTerms);
    if (!correction) {
      return { ...response, stats: { ...(response.stats || {}), typoAttempted: true, typoApplied: false } };
    }

    let best = null;
    const executionPlans = correction.plans.slice(0, typoCorrectionExecutionPlanLimit);
    for (const plan of executionPlans) {
      const corrected = await runSkippedSearch({
        ...params,
        q: plan.q,
        baseTerms: plan.baseTerms,
        terms: expandedTermsFromBaseTerms(plan.baseTerms)
      });
      if (corrected.total <= response.total) continue;
      const value = plan.score + Math.min(corrected.total, 20) * 0.05;
      if (!best || value > best.value) best = { value, plan, response: corrected };
    }

    if (!best) {
      return { ...response, stats: { ...(response.stats || {}), typoAttempted: true, typoApplied: false, ...correction.stats, typoCorrectionPlansExecuted: executionPlans.length } };
    }
    if (!typoCorrectionShouldReplace(response, best.response)) {
      return {
        ...response,
        suggestedQuery: best.plan.q,
        suggestions: [{ q: best.plan.q, corrections: best.plan.corrections }],
        stats: {
          ...(response.stats || {}),
          typoAttempted: true,
          typoApplied: false,
          typoSuggested: true,
          typoOriginalTotal: response.total,
          typoSuggestedQuery: best.plan.q,
          typoCorrectionPlansExecuted: executionPlans.length,
          typoCorrectedUpperBound: best.plan.estimatedUpperBound || 0,
          ...correction.stats
        }
      };
    }
    return {
      ...best.response,
      correctedQuery: best.plan.q,
      corrections: best.plan.corrections,
      stats: {
        ...(best.response.stats || {}),
        typoAttempted: true,
        typoApplied: true,
        typoOriginalTotal: response.total,
        typoCorrectedQuery: best.plan.q,
        typoCorrections: best.plan.corrections,
        typoCorrectionPlansExecuted: executionPlans.length,
        typoCorrectedUpperBound: best.plan.estimatedUpperBound || 0,
        ...correction.stats
      }
    };
  }

  function resultTitleMatchesQuery(result, query) {
    const title = authorityNormalizeSurface(result?.title || "");
    const surface = authorityNormalizeSurface(query);
    return !!title && !!surface && title === surface;
  }

  const addressAuthorityEnabled = Boolean(
    manifest.authority?.fields?.some(field => field.normalizer === "address")
  );
  const addressInterpolationEnabled = Boolean(
    manifest.authority?.fields?.some(field => field.normalizer === "address-range")
  );

  function interpolatedAddressLabel(result, houseNumber) {
    const base = [String(houseNumber), result.street].filter(Boolean).join(" ");
    const parts = [
      base,
      result.suburb,
      result.city,
      result.district,
      result.state,
      result.postcode,
      result.country
    ].map(value => String(value || "").trim()).filter(Boolean);
    return [...new Set(parts)].join(", ");
  }

  function synthesizeInterpolatedAddress(result, houseNumber) {
    if (!addressRangeContains(
      result._address_range_start,
      result._address_range_end,
      result._address_range_step,
      houseNumber
    )) return null;
    const point = interpolateAddressRangePoint(
      result._address_range_geometry,
      result._address_range_start,
      result._address_range_end,
      houseNumber
    );
    if (!point) return null;
    const address = interpolatedAddressLabel(result, houseNumber);
    const synthesized = {
      ...result,
      id: `${result.id}/${houseNumber}`,
      title: address,
      name: address,
      address,
      house_number: String(houseNumber),
      type: "interpolated_address",
      lat: Number(point.lat.toFixed(7)),
      lon: Number(point.lon.toFixed(7)),
      interpolated: true,
      address_accuracy: result._address_range_inclusion || "actual",
      interpolation: result._address_range_kind || ""
    };
    delete synthesized._address_range_start;
    delete synthesized._address_range_end;
    delete synthesized._address_range_step;
    delete synthesized._address_range_geometry;
    delete synthesized._address_range_kind;
    delete synthesized._address_range_inclusion;
    return synthesized;
  }

  async function tryAddressInterpolationFastPath({ q, page, size, includeResults }) {
    if (!addressInterpolationEnabled) return null;
    const candidates = addressRangeQueryCandidates(q);
    if (!candidates.length) return null;
    const housesByKey = new Map();
    for (const candidate of candidates) {
      const key = authorityAddressRangeKey(candidate.lookupValue);
      if (!key) continue;
      if (!housesByKey.has(key)) housesByKey.set(key, new Set());
      housesByKey.get(key).add(candidate.houseNumber);
    }
    const entries = await authorityEntries([...housesByKey.keys()]);
    const rowsByDoc = new Map();
    let complete = true;
    for (const { key, entry } of entries) {
      complete = complete && entry.complete !== false;
      for (const [doc, score] of entry.rows || []) {
        if (!rowsByDoc.has(doc)) rowsByDoc.set(doc, { doc, score, houses: new Set() });
        const row = rowsByDoc.get(doc);
        row.score = Math.max(row.score, score);
        for (const house of housesByKey.get(key) || []) row.houses.add(house);
      }
    }
    if (!rowsByDoc.size) return null;
    const ranked = [...rowsByDoc.values()].sort((left, right) => right.score - left.score || left.doc - right.doc);
    const resultContext = { hasTextTerms: true, preferDocPages: "auto" };
    const hydrated = includeResults === false
      ? []
      : await rowsToResults(ranked.map(row => [row.doc, row.score]), resultContext);
    const matches = [];
    if (includeResults !== false) {
      for (let index = 0; index < ranked.length; index++) {
        for (const houseNumber of ranked[index].houses) {
          const result = synthesizeInterpolatedAddress(hydrated[index], houseNumber);
          if (result) matches.push(result);
        }
      }
    } else {
      // Count-only callers still need the compact range payloads to prove
      // containment, so defer to the regular planner instead of guessing.
      return null;
    }
    if (!matches.length) {
      if (!complete) return null;
      return {
        total: 0,
        page,
        size,
        approximate: false,
        results: [],
        stats: {
          exact: true,
          plannerLane: "addressInterpolationExact",
          topKProven: true,
          totalExact: true,
          tailExhausted: true,
          addressInterpolationKeys: housesByKey.size,
          addressInterpolationRows: ranked.length,
          addressInterpolationMatches: 0,
          addressInterpolationComplete: true,
          blocksDecoded: 0,
          postingsDecoded: 0,
          docPayloadLane: resultContext.docPayloadLane,
          docPayloadPages: resultContext.docPayloadPages,
          docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
          docPayloadAdaptive: resultContext.docPayloadAdaptive,
          docPayloadForced: resultContext.docPayloadForced
        }
      };
    }
    matches.sort((left, right) => right.score - left.score || left.index - right.index || left.house_number.localeCompare(right.house_number));
    return {
      total: matches.length,
      page,
      size,
      approximate: !complete,
      results: matches.slice(0, size),
      stats: {
        exact: true,
        plannerLane: "addressInterpolationExact",
        topKProven: complete,
        totalExact: complete,
        tailExhausted: complete,
        addressInterpolationKeys: housesByKey.size,
        addressInterpolationRows: ranked.length,
        addressInterpolationMatches: matches.length,
        addressInterpolationComplete: complete,
        blocksDecoded: 0,
        postingsDecoded: 0,
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        docPayloadForced: resultContext.docPayloadForced
      }
    };
  }

  async function tryAddressAuthorityFastPath({ q, page, size, hasFilters, sortPlan, geoPlan, includeResults, authority }) {
    if (!addressAuthorityEnabled || options.authority === false || authority === false) return null;
    if (page !== 1 || hasFilters || sortPlan || geoPlan || !looksLikeAddressQuery(q)) return null;
    const keyPlans = authorityKeysForQuery(q, [], { analyzer, address: true });
    const addressKeys = [...new Set(keyPlans.filter(item => item.kind === "address").map(item => item.key))];
    if (!addressKeys.length) return null;
    const entries = await authorityEntries(addressKeys);
    const rowsByDoc = new Map();
    let total = 0;
    let complete = true;
    for (const { entry } of entries) {
      total = Math.max(total, entry.total || 0);
      complete = complete && entry.complete !== false;
      for (const [doc, score] of entry.rows || []) {
        rowsByDoc.set(doc, Math.max(rowsByDoc.get(doc) || 0, score));
      }
    }
    if (!rowsByDoc.size) {
      return tryAddressInterpolationFastPath({ q, page, size, includeResults });
    }
    const ranked = [...rowsByDoc].sort((left, right) => right[1] - left[1] || left[0] - right[0]);
    // Authority shards retain a bounded top list. Fall back to postings if a
    // caller requests more rows than the exact key retained.
    if (ranked.length < Math.min(size, total)) return null;
    const pageRows = ranked.slice(0, size);
    const resultContext = { hasTextTerms: true, preferDocPages: "auto" };
    const results = includeResults === false ? [] : await rowsToResults(pageRows, resultContext);
    return {
      total,
      page,
      size,
      approximate: false,
      results,
      stats: {
        exact: true,
        plannerLane: "addressAuthorityExact",
        topKProven: true,
        totalExact: true,
        tailExhausted: complete,
        addressAuthorityKeys: addressKeys.length,
        addressAuthorityRows: ranked.length,
        addressAuthorityComplete: complete,
        blocksDecoded: 0,
        postingsDecoded: 0,
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        docPayloadForced: resultContext.docPayloadForced
      }
    };
  }

  async function maybeAuthorityRerank(params, response) {
    return traceSpan("authority.rerank", () => maybeAuthorityRerankInner(params, response));
  }

  async function maybeAuthorityRerankInner(params, response) {
    const stats = response.stats || {};
    if (!authorityDirectory || options.authority === false || params.authority === false) return response;
    if (params.includeResults === false) return response;
    if (params.page !== 1 || params.sort) return response;
    const filters = params.filters || {};
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    if (hasFilters) return response;
    const authorityQuery = String(response.correctedQuery || response.surfaceFallbackQuery || params.q || "").trim();
    if (!authorityQuery || resultTitleMatchesQuery(response.results?.[0], authorityQuery)) return response;

    const authorityTerms = analyzer.queryPlan(authorityQuery).baseTerms;
    const keyPlans = authorityKeysForQuery(authorityQuery, authorityTerms, {
      analyzer,
      address: addressAuthorityEnabled && looksLikeAddressQuery(authorityQuery)
    }).filter(item => item.key);
    const addressKeys = [...new Set(keyPlans.filter(item => item.kind === "address").map(item => item.key))];
    const surfaceKeys = [...new Set(keyPlans.filter(item => item.kind === "surface").map(item => item.key))];
    const exactKeys = [...new Set(keyPlans.filter(item => item.kind === "exact").map(item => item.key))];
    const tokenKeys = [...new Set(keyPlans.filter(item => item.kind === "tokens").map(item => item.key))];
    if (!surfaceKeys.length && !exactKeys.length && !tokenKeys.length) return response;

    let loadedKeys = addressKeys.length ? addressKeys : surfaceKeys;
    let entries = await authorityEntries(loadedKeys);
    if (!authorityEntryRows(entries) && surfaceKeys.length && loadedKeys !== surfaceKeys) {
      loadedKeys = surfaceKeys;
      entries = await authorityEntries(surfaceKeys);
    }
    if (!authorityEntryRows(entries) && exactKeys.length) {
      loadedKeys = exactKeys;
      entries = await authorityEntries(exactKeys);
    }
    if (!authorityEntryRows(entries) && (response.total || 0) === 0 && tokenKeys.length) {
      loadedKeys = tokenKeys;
      entries = await authorityEntries(tokenKeys);
    }
    const rowsByDoc = new Map();
    for (const result of response.results || []) {
      if (result?.index == null) continue;
      rowsByDoc.set(result.index, {
        doc: result.index,
        score: Number(result.score || 0),
        baseline: true,
        authority: 0
      });
    }

    let authorityRows = 0;
    let authorityTotal = 0;
    let authorityComplete = true;
    for (const { entry } of entries) {
      authorityTotal = Math.max(authorityTotal, entry.total || 0);
      authorityComplete = authorityComplete && entry.complete !== false;
      for (const [doc, score] of entry.rows || []) {
        authorityRows++;
        const current = rowsByDoc.get(doc) || { doc, score: 0, baseline: false, authority: 0 };
        current.authority += score;
        current.score += score;
        rowsByDoc.set(doc, current);
      }
    }

    const ranked = [...rowsByDoc.values()]
      .sort((left, right) => right.score - left.score || left.doc - right.doc);
    const size = Math.max(1, Math.min(100, Number(params.size || response.size || 10)));
    const pageRows = ranked.slice(0, size).map(item => [item.doc, item.score]);
    const authorityInjected = pageRows.filter(([doc]) => !response.results?.some(result => result.index === doc)).length;

    if (!authorityRows || (!authorityInjected && sameDocOrder(pageRows, response.results || []))) {
      return {
        ...response,
        stats: {
          ...stats,
          authorityAttempted: true,
          authorityApplied: false,
          authorityKeys: loadedKeys.length,
          authorityRows
        }
      };
    }

    const resultContext = { hasTextTerms: true, preferDocPages: "auto" };
    const results = await rowsToResults(pageRows, resultContext);
    return {
      ...response,
      total: Math.max(response.total || 0, authorityTotal || pageRows.length),
      results,
      stats: {
        ...stats,
        topKProven: Boolean(stats.topKProven && authorityComplete),
        authorityAttempted: true,
        authorityApplied: true,
        authorityComplete,
        authorityKeys: loadedKeys.length,
        authorityEntries: entries.length,
        authorityRows,
        authorityInjected,
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        docPayloadForced: resultContext.docPayloadForced
      }
    };
  }

  function sameDocOrder(rows, results) {
    if (rows.length !== results.length) return false;
    for (let i = 0; i < rows.length; i++) {
      if (rows[i][0] !== results[i]?.index) return false;
    }
    return true;
  }

  function authorityEntryRows(entries) {
    let rows = 0;
    for (const { entry } of entries || []) rows += entry.rows?.length || 0;
    return rows;
  }

  async function executeSearch(params = {}) {
    if (params.vector) return executeHybridSearch(params);
    const q = String(params.q || "").trim();
    const page = Math.max(1, Number(params.page || 1));
    const size = Math.max(1, Math.min(maxPageSize, Math.floor(Number(params.size || 10))));
    const offset = (page - 1) * size;
    const userFilters = params.filters || {};
    const geoPlan = params.geo ? makeGeoPlan(params.geo) : null;
    const filters = geoPlan ? withGeoFilters(userFilters, geoPlan) : userFilters;
    const sort = params.sort || null;
    const sortPlan = makeSortPlan(sort);
    if (geoPlan?.sort && sortPlan) throw new Error("Rangefind: use either sort or geo.sort, not both.");
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;

    if (!q) {
      if (sortPlan) await ensureDocValueSortedManifest();
      await ensureFacetDictionaries(filters);
      if (geoPlan && !sortPlan) {
        // The tree lane verifies geo constraints from leaf pages directly, so
        // doc values are only needed for user-supplied filters.
        const hasUserFilters = Object.keys(userFilters.facets || {}).length
          || Object.keys(userFilters.numbers || {}).length
          || Object.keys(userFilters.booleans || {}).length;
        if (hasUserFilters) await ensureDocValuesManifest();
        const geoResponse = await runGeoBrowse({ page, size, filters: userFilters, geoPlan, hasFilters: hasUserFilters });
        if (geoResponse) return geoResponse;
      }
      if (hasFilters) await ensureDocValuesManifest();
      if (!sortPlan && !hasFilters && !geoPlan) {
        const docs = manifest.initial_results.slice(offset, offset + size);
        return { total: manifest.total, results: docs, page, size };
      }
      const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
      if (!sortPlan && hasFilters && docValues) {
        const chunkPruned = await runDocValueChunkBrowse({ page, size, filters, hasFilters });
        if (chunkPruned) return chunkPruned;
      }
      if (docValueSorted && sortPlan) {
        const pruned = await runDocValueBrowse({ page, size, filters, sortPlan, hasFilters });
        if (pruned) return pruned;
      }
      let codeData;
      let candidates;
      if (docValues) {
        const chunkIndexes = candidateDocValueChunks(docFilterPlan);
        codeData = await ensureDocValueChunkIndexes(planFields(docFilterPlan, sortPlan), chunkIndexes);
        candidates = [];
        const chunkSize = Math.max(1, docValues.chunk_size || manifest.total || 1);
        for (const chunkIndex of chunkIndexes) {
          const start = chunkIndex * chunkSize;
          const end = Math.min(manifest.total, start + chunkSize);
          for (let index = start; index < end; index++) candidates.push([index, 0]);
        }
      } else {
        codeData = await loadCodes();
        candidates = Array.from({ length: manifest.total }, (_, index) => [index, 0]);
      }
      const selected = sortPlan
        ? selectSortedTopK(candidates, codeData, sortPlan, offset + size, hasFilters ? docFilterPlan : null)
        : {
            total: candidates.filter(([index]) => !hasFilters || passesFilterPlan(index, codeData, docFilterPlan)).length,
            ranked: candidates.filter(([index]) => !hasFilters || passesFilterPlan(index, codeData, docFilterPlan))
          };
      const ranked = sortPlan ? selected.ranked : sortRanked(selected.ranked, codeData, sortPlan);
      const rows = ranked.slice(offset, offset + size);
      const resultContext = { hasTextTerms: false, preferDocPages: true };
      const results = await rowsToResults(rows, resultContext);
      return {
        total: selected.total,
        results,
        page,
        size,
        stats: {
          docPayloadLane: resultContext.docPayloadLane,
          docPayloadPages: resultContext.docPayloadPages,
          docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs
        }
      };
    }

    const addressResponse = await tryAddressAuthorityFastPath({
      q,
      page,
      size,
      hasFilters,
      sortPlan,
      geoPlan,
      includeResults: params.includeResults !== false,
      authority: params.authority
    });
    if (addressResponse) {
      if (params.highlight && addressResponse.results.length) {
        const highlightOptions = params.highlight === true ? {} : params.highlight;
        applyHighlights(addressResponse.results, highlightTermSet(q, "", analyzer), { ...highlightOptions, analyzer });
      }
      return addressResponse;
    }

    const queryAnalysis = await resolveQueryPlan(q);
    const analyzedTerms = queryAnalysis.analyzedTerms;
    const baseTerms = queryAnalysis.baseTerms;
    if (geoPlan?.sort) {
      // Exact text + distance sort: resolve the full text match set once,
      // then let the geo tree order it with the nearest early-stop proof.
      const match = await collectTextMatchDocs(baseTerms);
      if (!match) {
        throw new Error("Rangefind: text distance sort exceeds the geoTextSortMaxDf posting budget; narrow the query or rank by relevance with geo.boost.");
      }
      const hasUserFilters = Object.keys(userFilters.facets || {}).length
        || Object.keys(userFilters.numbers || {}).length
        || Object.keys(userFilters.booleans || {}).length;
      if (hasUserFilters) await ensureDocValuesManifest();
      await ensureFacetDictionaries(userFilters);
      const geoResponse = await runGeoBrowse({
        page,
        size,
        filters: userFilters,
        geoPlan,
        hasFilters: hasUserFilters,
        textMatchDocs: match.docs
      });
      if (!geoResponse) throw new Error("Rangefind: geo tree is unavailable for distance sort.");
      geoResponse.stats = {
        ...(geoResponse.stats || {}),
        textMatchDocs: match.docs.size,
        textMatchTerms: match.stats.terms,
        textMatchMinShouldMatch: match.stats.minShouldMatch,
        textMatchBlocksDecoded: match.stats.blocksDecoded,
        textMatchPostingsDecoded: match.stats.postingsDecoded
      };
      return geoResponse;
    }
    if (geoPlan?.filtered) {
      // Term entries are already cached by resolveQueryPlan, so the df sum
      // costs no extra fetches. It bounds the postings the doc-value lane
      // would have to verify, which prices the doc-set alternative.
      const textPostingsEstimate = q && baseTerms.length
        ? (await termEntries(baseTerms)).reduce((sum, item) => sum + (item.entry.df || 0), 0)
        : null;
      await buildGeoDocSetIfCheap(geoPlan, textPostingsEstimate);
    }
    if (params.exact) await ensureFullManifest();
    else if (sortPlan) await ensureDocValueSortedManifest();
    const searchFn = params.exact ? runFullSearch : runSkippedSearch;
    const response = await searchFn({
      q,
      page,
      size,
      filters,
      sort,
      baseTerms,
      terms: queryAnalysis.terms,
      rerank: params.rerank,
      includeResults: params.includeResults !== false,
      plannerFallbackReason: params.exact ? "exact_requested" : "full_scan"
    });
    const includeResults = params.includeResults !== false;
    const typoResponse = await maybeTypoFallback({ q, page, size, filters, sort, rerank: params.rerank, includeResults }, response, baseTerms, analyzedTerms);
    const rerankedResponse = await maybeAuthorityRerank({ q, page, size, filters, sort, rerank: params.rerank, includeResults, authority: params.authority }, typoResponse);
    if (params.highlight && includeResults && rerankedResponse?.results?.length) {
      const highlightOptions = params.highlight === true ? {} : params.highlight;
      applyHighlights(
        rerankedResponse.results,
        highlightTermSet(q, rerankedResponse.correctedQuery, analyzer),
        { ...highlightOptions, analyzer }
      );
    }
    if (geoPlan?.docSetStats) {
      rerankedResponse.stats = {
        ...(rerankedResponse.stats || {}),
        geoLane: "textDocSet",
        geoCandidateLeaves: geoPlan.docSetStats.candidateLeaves,
        geoPointsScanned: geoPlan.docSetStats.candidatePoints,
        geoPointsAccepted: geoPlan.docSetStats.matchedDocs,
        geoLeafPagesFetched: geoPlan.docSetStats.leafPagesFetched,
        geoLeafPageFetchGroups: geoPlan.docSetStats.leafPageFetchGroups
      };
    } else if (geoPlan?.filtered) {
      rerankedResponse.stats = {
        ...(rerankedResponse.stats || {}),
        geoLane: "textDocValues"
      };
    }
    return attachGeoDistances(rerankedResponse, geoPlan);
  }

  async function attachGeoDistances(response, geoPlan) {
    if (!geoPlan?.near || !response?.results?.length) return response;
    const near = geoPlan.near;
    const indexes = response.results.map(result => result.index).filter(Number.isInteger);
    if (!indexes.length) return response;
    const store = await valueStoreForDocs([geoPlan.latField, geoPlan.lonField], indexes);
    for (const result of response.results) {
      if (!Number.isInteger(result.index)) continue;
      const latE7 = latToE7(valueForDoc(store, geoPlan.latField, result.index));
      const lonE7 = lonToE7(valueForDoc(store, geoPlan.lonField, result.index));
      if (latE7 == null || lonE7 == null) continue;
      result.distanceMeters = roundedDistanceMeters(haversineMetersE7(near.latE7, near.lonE7, latE7, lonE7));
    }
    if (geoPlan.boost) {
      const { weight, pivotMeters } = geoPlan.boost;
      for (const result of response.results) {
        if (result.distanceMeters == null) continue;
        result.score += weight * pivotMeters / (pivotMeters + result.distanceMeters);
      }
      response.results.sort((a, b) => b.score - a.score || a.index - b.index);
      response.stats = {
        ...(response.stats || {}),
        geoBoost: true,
        // Boost reranks only the returned page window, not the full corpus.
        geoBoostWindow: response.results.length
      };
    }
    return response;
  }

  function vectorFieldMeta(name) {
    const fields = manifest.vectors?.fields || {};
    const fieldNames = Object.keys(fields);
    if (!fieldNames.length) throw new Error("Rangefind: this index has no vector fields (configure `vectors` at build time).");
    const field = name || (fieldNames.length === 1 ? fieldNames[0] : "");
    const meta = fields[field];
    if (!meta) throw new Error(`Rangefind: unknown vector field "${name || ""}".`);
    return meta;
  }

  async function loadVectorRoot(meta) {
    if (!vectorRootCache.has(meta.name)) {
      const promise = fetchGzipArrayBuffer(new URL(meta.directory.file, baseUrl)).then(parseVectorRoot);
      promise.catch(() => {
        vectorRootCache.delete(meta.name);
      });
      vectorRootCache.set(meta.name, promise);
    }
    return vectorRootCache.get(meta.name);
  }

  function normalizedQueryVector(value, dims) {
    const vector = value instanceof Float32Array
      ? (value.length === dims ? Float32Array.from(value) : null)
      : vectorFromValue(value, dims);
    const normalized = vector ? normalizeVector(vector) : null;
    if (!normalized) throw new Error(`Rangefind: vector queries need ${dims} finite dimensions.`);
    return normalized;
  }

  // IVF traversal: score int8 centroids, open the top nprobe cluster pages,
  // rank candidates on the coarse dimension prefix, then re-score the best
  // candidates against full-dimension int8 rows fetched from the fixed-width
  // refine store (addressed by ordinal — no per-document pointers).
  async function vectorTopK(params = {}) {
    const meta = vectorFieldMeta(params.field);
    const root = await loadVectorRoot(meta);
    // The index stores vectors with dimensions permuted variance-descending;
    // the query enters the same space once here.
    const query = applyPermutation(
      normalizedQueryVector(params.vector, root.dims),
      0,
      root.permutation,
      new Float32Array(root.dims)
    );
    const k = Math.max(1, Math.min(200, Math.floor(Number(params.k || 10))));
    const nprobe = Math.max(1, Math.min(root.clusterCount, Math.floor(Number(params.nprobe || 8))));
    const refineFactor = Math.max(1, Math.min(64, Math.floor(Number(params.refineFactor || 8))));

    const centroidOrder = [];
    for (let c = 0; c < root.clusterCount; c++) {
      centroidOrder.push([c, dotInt8(query, root.centroidCodes, c * root.dims, root.dims, root.centroidScales[c])]);
    }
    centroidOrder.sort((a, b) => b[1] - a[1]);
    const probed = centroidOrder.slice(0, nprobe).map(([c]) => root.clusters[c]);

    const pageFetchStats = { wanted: 0, fetched: 0, groups: 0 };
    const pages = await loadPackedPages({
      field: meta.name,
      entries: probed,
      cache: vectorClusterPageCache,
      cacheKey: (field, index) => `${field} ${index}`,
      decode: inflated => decodeVectorClusterPage(inflated, { name: meta.name }),
      label: "vector cluster page",
      packDir: meta.pack_dir,
      rangePlan: "vectorClusterPages",
      stats: pageFetchStats
    });

    const candidates = [];
    let scanned = 0;
    for (const page of pages) {
      for (let i = 0; i < page.count; i++) {
        scanned++;
        candidates.push([
          page.docs[i],
          page.ordinals[i],
          dotInt8(query, page.codes, i * page.coarseDims, page.coarseDims, page.scales[i])
        ]);
      }
    }
    candidates.sort((a, b) => b[2] - a[2] || a[0] - b[0]);
    const shortlist = candidates.slice(0, Math.max(k, Math.min(candidates.length, k * refineFactor)));

    let rows;
    let refineGroups = 0;
    if (params.refine === false || !shortlist.length) {
      rows = shortlist.slice(0, k).map(([doc, , score]) => [doc, score]);
    } else {
      const items = shortlist.map(([doc, ordinal, coarseScore]) => ({
        doc,
        coarseScore,
        entry: {
          pack: root.refine.packs[Math.floor(ordinal / root.refine.rowsPerPack)],
          offset: (ordinal % root.refine.rowsPerPack) * root.refine.rowBytes,
          length: root.refine.rowBytes
        }
      }));
      const groups = rangeGroups(items, "vectorRefine");
      refineGroups = groups.length;
      const scored = [];
      await Promise.all(groups.map(async (group) => {
        const bytes = new Uint8Array(await fetchRange(new URL(`${meta.refine_pack_dir}/${group.pack}`, baseUrl), group.start, group.end - group.start));
        const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
        const codes = new Int8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength);
        for (const item of group.items) {
          const at = item.entry.offset - group.start;
          const scale = view.getFloat32(at, true);
          scored.push([item.doc, dotInt8(query, codes, at + 4, root.dims, scale)]);
        }
      }));
      scored.sort((a, b) => b[1] - a[1] || a[0] - b[0]);
      rows = scored.slice(0, k);
    }

    return {
      rows,
      stats: {
        vectorField: meta.name,
        vectorClusters: root.clusterCount,
        vectorClustersProbed: nprobe,
        vectorClusterPagesFetched: pageFetchStats.fetched,
        vectorCandidatesScanned: scanned,
        vectorCandidatesRefined: params.refine === false ? 0 : shortlist.length,
        vectorRefineFetchGroups: refineGroups
      }
    };
  }

  async function executeVectorSearch(params = {}) {
    const { rows, stats } = await vectorTopK(params);
    const resultContext = { hasTextTerms: false, preferDocPages: false };
    const results = params.includeResults === false
      ? rows.map(([index, score]) => ({ index, score }))
      : await rowsToResults(rows, resultContext);
    return {
      total: rows.length,
      results,
      stats: { ...stats, exact: false, approximate: true }
    };
  }

  // Hybrid text + vector retrieval fused with reciprocal rank fusion; user
  // filters are enforced on both lanes (the vector lane verifies candidates
  // against doc values before fusion).
  async function executeHybridSearch(params) {
    const size = Math.max(1, Math.min(maxPageSize, Math.floor(Number(params.size || 10))));
    const page = Math.max(1, Number(params.page || 1));
    const poolSize = Math.max(size * 3, Math.min(100, size * 5));
    const rrfK = Math.max(1, Math.floor(Number(params.hybrid?.rrfK || 60)));
    const q = String(params.q || "").trim();
    const filters = params.filters || {};
    const hasFilters = Object.keys(filters.facets || {}).length
      || Object.keys(filters.numbers || {}).length
      || Object.keys(filters.booleans || {}).length;

    const [textResponse, vectorResponse] = await Promise.all([
      q
        ? executeSearch({ ...params, vector: undefined, hybrid: undefined, highlight: undefined, includeResults: false, size: poolSize, page: 1 })
        : null,
      vectorTopK({ ...(params.hybrid || {}), field: params.vectorField, vector: params.vector, k: poolSize })
    ]);

    let vectorRows = vectorResponse.rows;
    if (hasFilters && vectorRows.length) {
      await ensureFacetDictionaries(filters);
      const filterPlan = makeDocFilterPlan(filters);
      const codeData = await valueStoreForFilterPlan(filterPlan, vectorRows.map(([doc]) => doc));
      vectorRows = vectorRows.filter(([doc]) => passesFilterPlan(doc, codeData, filterPlan));
    }

    // Without a text lane this is a plain (optionally filtered) vector
    // search; return real similarity scores instead of fusion ranks.
    if (!q) {
      const offset = (page - 1) * size;
      const rows = vectorRows.slice(offset, offset + size);
      const results = await rowsToResults(rows, { hasTextTerms: false, preferDocPages: false });
      return {
        total: vectorRows.length,
        results,
        page,
        size,
        approximate: true,
        stats: { ...vectorResponse.stats, exact: false }
      };
    }

    const fused = new Map();
    const addRanked = (list, key) => {
      list.forEach((doc, rank) => {
        const entry = fused.get(doc) || { doc, score: 0, lanes: {} };
        entry.score += 1 / (rrfK + rank + 1);
        entry.lanes[key] = rank + 1;
        fused.set(doc, entry);
      });
    };
    addRanked((textResponse?.results || []).map(item => item.index), "text");
    addRanked(vectorRows.map(([doc]) => doc), "vector");

    const ranked = [...fused.values()].sort((a, b) => b.score - a.score || a.doc - b.doc);
    const offset = (page - 1) * size;
    const rows = ranked.slice(offset, offset + size).map(entry => [entry.doc, entry.score]);
    const resultContext = { hasTextTerms: !!q, preferDocPages: false };
    const results = await rowsToResults(rows, resultContext);
    for (const result of results) {
      const entry = fused.get(result.index);
      if (entry) result.hybrid = entry.lanes;
    }
    if (params.highlight && q && results.length) {
      const highlightOptions = params.highlight === true ? {} : params.highlight;
      applyHighlights(results, highlightTermSet(q, textResponse?.correctedQuery, analyzer), { ...highlightOptions, analyzer });
    }
    return {
      total: ranked.length,
      results,
      page,
      size,
      approximate: true,
      stats: {
        hybrid: true,
        hybridPool: poolSize,
        hybridTextResults: textResponse?.results?.length || 0,
        hybridVectorResults: vectorRows.length,
        ...vectorResponse.stats,
        textLane: textResponse?.stats?.plannerLane || (q ? "text" : "none")
      }
    };
  }

  function authoritySuggestField() {
    const fields = (manifest.authority?.fields || []).filter(field => field.exact !== false);
    if (fields.length !== 1) return null;
    const field = fields[0];
    // Authority rows intentionally store only doc ids and scores. The title is
    // guaranteed to be present in every hydrated payload, while arbitrary
    // authority fields may not be part of `display`; only use the lossless case.
    return field.path === "title" ? field : null;
  }

  async function executeLegacyAuthoritySuggest(params, q, prefix, size) {
    const field = authoritySuggestField();
    if (!authorityDirectory || options.authority === false || !field) return null;
    const keyPrefix = `x|${prefix}`;
    const maxShards = Math.max(1, Math.min(64, Math.floor(Number(
      params.authorityMaxShards ?? options.authoritySuggestMaxShards ?? 16
    ))));
    const range = await directoryEntriesForPrefix(authorityDirectory, keyPrefix, maxShards);
    const candidates = [];
    let shardsVisited = 0;
    let entriesScanned = 0;
    for (const shard of range.entries) {
      const loaded = await loadAuthorityShards([shard]);
      const data = loaded.get(shard.shard);
      if (!data) continue;
      shardsVisited++;
      for (const [key, entry] of data.entries) {
        if (key < keyPrefix) continue;
        if (!key.startsWith(keyPrefix)) break;
        entriesScanned++;
        const row = entry.rows?.[0];
        if (!row) continue;
        candidates.push({ key, doc: row[0], weight: row[1], count: entry.total || 1 });
        // A single title authority field gives every candidate the same
        // exactWeight. Key order therefore proves the first k suggestions.
        if (candidates.length >= size) break;
      }
      if (candidates.length >= size) break;
    }

    const resultContext = { hasTextTerms: false, preferDocPages: false };
    const docs = candidates.length
      ? await rowsToResults(candidates.map(item => [item.doc, item.weight]), resultContext)
      : [];
    const suggestions = docs.map((doc, index) => ({
      text: String(doc.title || ""),
      weight: candidates[index].weight,
      count: candidates[index].count
    })).filter(item => item.text).slice(0, size);
    const exact = candidates.length >= size || !range.truncated;
    return {
      q,
      prefix,
      suggestions,
      stats: {
        exact,
        suggestLane: "authority-title",
        suggestDirectoryPagesVisited: range.pagesVisited,
        suggestCandidateShards: range.entries.length,
        suggestShardsVisited: shardsVisited,
        suggestEntriesScanned: entriesScanned,
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs
      }
    };
  }

  function lexiconCandidateShards(shards, prefix) {
    return (shards || []).filter(item => prefix.startsWith(item.shard) || item.shard.startsWith(prefix));
  }

  function mergeAutocompleteCandidate(best, item) {
    const previous = best.get(item.display);
    if (!previous || compareAutocomplete(item, previous) < 0) best.set(item.display, item);
  }

  // Autocomplete candidates parsed once per authority shard and cached on the
  // (immutable, engine-cached) parsed shard: repeat suggest calls binary-search
  // the sorted candidate list instead of re-walking and re-normalizing every
  // entry in the shard.
  function autocompleteCandidatesForShard(shard, weighted) {
    if (shard.autocompleteCandidates) return shard.autocompleteCandidates;
    const list = [];
    for (const [key, entry] of shard.entries) {
      const parsed = parseAutocompleteKey(key);
      if (!parsed) continue;
      const candidate = {
        key,
        ...parsed,
        weight: entry.autocompleteWeight || entry.total || 0,
        count: entry.total || 1,
        full: suggestKey(parsed.display) === parsed.normalized
      };
      candidate.rank = autocompleteRank(candidate, weighted);
      list.push(candidate);
    }
    shard.autocompleteCandidates = list;
    return list;
  }

  function autocompleteLowerBound(candidates, keyPrefix) {
    let lo = 0;
    let hi = candidates.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (candidates[mid].key < keyPrefix) lo = mid + 1;
      else hi = mid;
    }
    return lo;
  }

  async function executeLexiconSuggest(q, prefix, size, root) {
    const codepoints = Array.from(prefix);
    if (root.hot?.size) {
      const hotEntry = root.hot.get(prefix);
      const completeMissingSingle = codepoints.length === 1 && !hotEntry;
      if (completeMissingSingle || (hotEntry && size <= hotEntry.count)) {
        const hot = hotEntry ? await loadAuthorityHotList(prefix, hotEntry) : [];
        return {
          q,
          prefix,
          suggestions: hot.slice(0, size).map(({ display, weight, count }) => ({ text: display, weight, count })),
          stats: {
            exact: true,
            suggestLane: "authority-hot",
            suggestCandidateShards: 0,
            suggestShardsVisited: 0,
            suggestEntriesScanned: hot.length
          }
        };
      }
    }

    const keyPrefix = `${AUTOCOMPLETE_PREFIX}${prefix}`;
    const candidates = lexiconCandidateShards(root.shards, keyPrefix);
    const ordered = candidates.slice().sort((left, right) => (
      right.maxRank - left.maxRank
      || (left.shard < right.shard ? -1 : left.shard > right.shard ? 1 : 0)
    ));
    const best = new Map();
    let entriesScanned = 0;
    let shardsVisited = 0;
    let directoryPagesVisited = 0;
    const directoryPages = new Set();
    const kthRank = () => {
      if (best.size < size) return -1;
      const sorted = [...best.values()].sort(compareAutocomplete);
      // Candidates below the boundary can never re-enter the top `size`
      // (duplicates merge to the max rank), so cap the pool to keep the
      // per-batch sort from growing with broad prefixes.
      if (sorted.length > size * 4) {
        best.clear();
        for (const item of sorted.slice(0, size * 2)) best.set(item.display, item);
      }
      return sorted[size - 1].rank;
    };
    const directoryRoot = await loadDirectoryRoot(authorityDirectory);
    let batch = 1;
    for (let position = 0; position < ordered.length;) {
      const boundary = kthRank();
      if (boundary >= 0 && ordered[position].maxRank < boundary) break;
      const slice = ordered.slice(position, position + batch);
      batch = Math.min(8, batch * 2);
      const resolved = [];
      for (const summary of slice) {
        const page = findDirectoryPage(directoryRoot, summary.shard);
        if (page) directoryPages.add(page.file);
        const item = await directoryEntryFromRoot(authorityDirectory, directoryRoot, summary.shard);
        if (item) resolved.push(item);
      }
      directoryPagesVisited = directoryPages.size;
      const loaded = await loadAuthorityShards(resolved);
      for (const item of resolved) {
        const shard = loaded.get(item.shard);
        if (!shard) continue;
        shardsVisited++;
        const shardCandidates = autocompleteCandidatesForShard(shard, root.weighted);
        for (let index = autocompleteLowerBound(shardCandidates, keyPrefix); index < shardCandidates.length; index++) {
          const candidate = shardCandidates[index];
          if (!candidate.key.startsWith(keyPrefix)) break;
          entriesScanned++;
          mergeAutocompleteCandidate(best, candidate);
        }
      }
      position += slice.length;
    }

    const ranked = [...best.values()].sort(compareAutocomplete).slice(0, size);
    return {
      q,
      prefix,
      suggestions: ranked.map(({ display, weight, count }) => ({ text: display, weight, count })),
      stats: {
        exact: true,
        suggestLane: "authority-lexicon",
        suggestCandidateShards: candidates.length,
        suggestShardsVisited: shardsVisited,
        suggestDirectoryPagesVisited: directoryPagesVisited,
        suggestEntriesScanned: entriesScanned
      }
    };
  }

  function addressSuggestionParts(q) {
    const match = String(q || "").trim().match(/^(\d+)\s+(.+)$/u);
    if (!match) return null;
    const houseNumber = Number(match[1]);
    if (!Number.isSafeInteger(houseNumber) || houseNumber < 0) return null;
    return { houseNumber, tail: match[2].trim() };
  }

  function replaceSuggestionHouseNumber(value, houseNumber) {
    const tail = String(value || "")
      .trim()
      .replace(/^\d+[\p{L}]?(?:\s*[\u2013-]\s*\d+[\p{L}]?)?\s+/u, "");
    return tail ? `${houseNumber} ${tail}` : "";
  }

  async function executeAddressSuggest(q, prefix, size, root) {
    if (!addressInterpolationEnabled || !root) return null;
    const parts = addressSuggestionParts(q);
    if (!parts?.tail) return null;
    const activeTailToken = authorityNormalizeSurface(parts.tail).split(" ").filter(Boolean).at(-1) || "";
    if (activeTailToken.length < 3) return null;
    const tailPrefix = suggestKey(parts.tail);
    if (!tailPrefix) return null;
    const tailSize = Math.min(32, Math.max(12, size * 3));
    const tailResponse = await executeLexiconSuggest(parts.tail, tailPrefix, tailSize, root);
    const suggestions = [];
    const seenQueries = new Set();
    const seenAddresses = new Set();
    const candidates = [];
    for (const candidate of tailResponse.suggestions) {
      const candidateQuery = replaceSuggestionHouseNumber(candidate.text, parts.houseNumber);
      const normalizedQuery = authorityNormalizeSurface(candidateQuery);
      if (!candidateQuery || seenQueries.has(normalizedQuery)) continue;
      seenQueries.add(normalizedQuery);
      candidates.push({ candidate, candidateQuery });
      if (candidates.length >= Math.min(16, Math.max(12, size * 2))) break;
    }
    // Directory and shard promises are cached, so concurrent probes coalesce
    // shared reads while broad street prefixes avoid serial range latency.
    const responses = await Promise.all(candidates.map(({ candidateQuery }) => tryAddressAuthorityFastPath({
        q: candidateQuery,
        page: 1,
        size: 1,
        hasFilters: false,
        sortPlan: null,
        geoPlan: null,
        includeResults: true,
        authority: true
      })));
    for (let index = 0; index < candidates.length; index++) {
      const candidate = candidates[index].candidate;
      const response = responses[index];
      const result = response?.results?.[0];
      const text = String(result?.address || result?.name || result?.title || "").trim();
      const normalizedAddress = authorityNormalizeSurface(text);
      if (!text || seenAddresses.has(normalizedAddress)) continue;
      seenAddresses.add(normalizedAddress);
      suggestions.push({
        text,
        weight: Math.max(1, Number(candidate.weight || 0)),
        count: 1,
        interpolated: Boolean(result.interpolated)
      });
      if (suggestions.length >= size) break;
    }
    if (!suggestions.length) return null;
    return {
      q,
      prefix,
      suggestions,
      stats: {
        exact: true,
        suggestLane: "address-authority",
        addressSuggestionTail: parts.tail,
        addressSuggestionCandidates: tailResponse.suggestions.length,
        addressSuggestionLookups: candidates.length,
        addressSuggestionMatches: suggestions.length,
        addressSuggestionTailLane: tailResponse.stats?.suggestLane || "",
        suggestCandidateShards: tailResponse.stats?.suggestCandidateShards || 0,
        suggestShardsVisited: tailResponse.stats?.suggestShardsVisited || 0,
        suggestDirectoryPagesVisited: tailResponse.stats?.suggestDirectoryPagesVisited || 0,
        suggestEntriesScanned: tailResponse.stats?.suggestEntriesScanned || 0
      }
    };
  }

  async function executeSuggest(params = {}) {
    const q = String(params.q || "");
    const size = Math.max(1, Math.min(50, Math.floor(Number(params.size || 8))));
    const prefix = suggestKey(q);
    if (!prefix) {
      return { q, prefix, suggestions: [], stats: { exact: true, suggestShardsVisited: 0, suggestEntriesScanned: 0 } };
    }
    const root = await loadAuthorityLexiconRoot();
    if (root) {
      const address = await executeAddressSuggest(q, prefix, size, root);
      if (address) return address;
      return executeLexiconSuggest(q, prefix, size, root);
    }
    const legacy = await executeLegacyAuthoritySuggest(params, q, prefix, size);
    if (legacy) return legacy;
    throw new Error("Rangefind: this index has no authority autocomplete lexicon (configure `suggest` fields at build time).");
  }

  function normalizeFacetsParam(param) {
    if (!param) return null;
    const fields = Array.isArray(param) ? param : param.fields;
    if (!Array.isArray(fields) || !fields.length) return null;
    const size = Math.max(1, Math.min(100, Math.floor(Number((Array.isArray(param) ? 0 : param.size) || FACET_COUNT_SIZE))));
    return { fields: fields.map(String), size };
  }

  async function facetTopValues(field, counts, size) {
    const dictionary = await loadFacetDictionary(field);
    const out = [];
    for (const [code, count] of counts) {
      const item = dictionary?.[code];
      if (!item || !item.value) continue;
      out.push({ value: item.value, label: item.label || item.value, count });
    }
    out.sort((a, b) => b.count - a.count || (a.value < b.value ? -1 : 1));
    return out.slice(0, size);
  }

  // Global distribution straight from the facet dictionaries (counted at
  // build time) — the unfiltered case costs zero extra fetches.
  async function facetCountsFromDictionary(fields, size) {
    const out = {};
    for (const field of fields) {
      const dictionary = await loadFacetDictionary(field);
      const values = (dictionary || [])
        .filter(item => item.value)
        .map(item => ({ value: item.value, label: item.label || item.value, count: item.n || 0 }))
        .sort((a, b) => b.count - a.count || (a.value < b.value ? -1 : 1))
        .slice(0, size);
      out[field] = { values, exact: true, source: "dictionary" };
    }
    return out;
  }

  // Counts facet codes over an enumeration grouped by doc-value chunk. When
  // the enumeration spans more chunks than the budget, a strided chunk
  // sample is counted and scaled — sampling whole chunks keeps fetch counts
  // bounded no matter how many documents match.
  async function facetCountsOverChunks({ fields, size, docsByChunk, verify }) {
    const chunkIndexes = [...docsByChunk.keys()].sort((a, b) => a - b);
    let sampledChunks = chunkIndexes;
    const sampled = chunkIndexes.length > facetCountMaxChunks;
    if (sampled) {
      const stride = chunkIndexes.length / facetCountMaxChunks;
      sampledChunks = Array.from({ length: facetCountMaxChunks }, (_, i) => chunkIndexes[Math.floor(i * stride)]);
    }
    const loadFields = verify ? [...new Set([...fields, ...verify.fields])] : fields;
    const codeData = await ensureDocValueChunkIndexes(loadFields, sampledChunks);
    const tallies = new Map(fields.map(field => [field, new Map()]));
    let counted = 0;
    let rawInSample = 0;
    for (const chunkIndex of sampledChunks) {
      for (const doc of docsByChunk.get(chunkIndex)) {
        rawInSample += 1;
        if (verify && !passesFilterPlan(doc, codeData, verify.plan)) continue;
        counted += 1;
        for (const field of fields) {
          const value = valueForDoc(codeData, field, doc);
          if (!value?.codes) continue;
          const tally = tallies.get(field);
          for (const code of value.codes) tally.set(code, (tally.get(code) || 0) + 1);
        }
      }
    }
    // Scale sampled counts by the fraction of the enumeration that was
    // inspected — unbiased under uniform verification rates, and never
    // dependent on early-stopped response totals.
    let totalEnumerated = 0;
    for (const docs of docsByChunk.values()) totalEnumerated += docs.length;
    const scale = sampled && rawInSample > 0 ? totalEnumerated / rawInSample : 1;
    const out = {};
    for (const field of fields) {
      const scaledCounts = new Map();
      for (const [code, count] of tallies.get(field)) scaledCounts.set(code, Math.round(count * scale));
      out[field] = {
        values: await facetTopValues(field, scaledCounts, size),
        exact: !sampled,
        sampled_docs: counted
      };
    }
    return out;
  }

  function groupDocsByChunk(docs) {
    const chunkSize = Math.max(1, docValues?.chunk_size || manifest.total || 1);
    const byChunk = new Map();
    for (const doc of docs) {
      const chunk = Math.floor(doc / chunkSize);
      let list = byChunk.get(chunk);
      if (!list) {
        list = [];
        byChunk.set(chunk, list);
      }
      list.push(doc);
    }
    return byChunk;
  }

  async function computeFacetCounts(params, response) {
    const facetsParam = normalizeFacetsParam(params.facets);
    if (!facetsParam) return;
    if (params.vector || params.geo) {
      response.stats = { ...(response.stats || {}), facetCountLane: "unsupported-lane" };
      return;
    }
    const q = String(params.q || "").trim();
    const filters = params.filters || {};
    const hasFilters = Object.keys(filters.facets || {}).length
      || Object.keys(filters.numbers || {}).length
      || Object.keys(filters.booleans || {}).length;
    await ensureFacetDictionaries(filters);

    if (!q && !hasFilters) {
      response.facets = await facetCountsFromDictionary(facetsParam.fields, facetsParam.size);
      response.stats = { ...(response.stats || {}), facetCountLane: "dictionary" };
      return;
    }

    await ensureDocValuesManifest();
    const filterPlan = hasFilters ? makeDocFilterPlan(filters) : null;

    if (q) {
      const match = await collectTextMatchDocs((await resolveQueryPlan(q)).baseTerms);
      if (!match) {
        // Posting volume above budget: fall back to the global distribution,
        // clearly flagged, instead of guessing from a biased top-k pool.
        response.facets = await facetCountsFromDictionary(facetsParam.fields, facetsParam.size);
        for (const field of facetsParam.fields) {
          if (response.facets[field]) response.facets[field].exact = false;
        }
        response.stats = { ...(response.stats || {}), facetCountLane: "global-fallback" };
        return;
      }
      response.facets = await facetCountsOverChunks({
        fields: facetsParam.fields,
        size: facetsParam.size,
        docsByChunk: groupDocsByChunk(match.docs),
        verify: filterPlan ? { plan: filterPlan, fields: filterPlanFields(filterPlan) } : null
      });
      response.stats = { ...(response.stats || {}), facetCountLane: "text-match-set" };
      return;
    }

    // Filtered browse: candidate chunks from the numeric/facet summaries,
    // every doc verified against the filter plan while tallying.
    const chunkSize = Math.max(1, docValues?.chunk_size || manifest.total || 1);
    const docsByChunk = new Map();
    for (const chunkIndex of candidateDocValueChunks(filterPlan)) {
      const start = chunkIndex * chunkSize;
      const end = Math.min(manifest.total, start + chunkSize);
      docsByChunk.set(chunkIndex, Array.from({ length: end - start }, (_, i) => start + i));
    }
    response.facets = await facetCountsOverChunks({
      fields: facetsParam.fields,
      size: facetsParam.size,
      docsByChunk,
      verify: { plan: filterPlan, fields: filterPlanFields(filterPlan) }
    });
    response.stats = { ...(response.stats || {}), facetCountLane: "chunk-scan" };
  }

  async function executeCount(params = {}) {
    const q = String(params.q || "").trim();
    const filters = params.filters || {};
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    if (hasFilters || params.sort || params.geo) {
      throw new Error("Rangefind count currently supports text-only queries without filters, sort, or geo.");
    }
    const baseTerms = (await resolveQueryPlan(q)).baseTerms;
    return runCountSearch({ q, baseTerms });
  }

  // Link-graph authority prior. A build-time PageRank score (manifest.linkGraph,
  // field values in [0, 1]) applied as a multiplicative document boost over the
  // returned result window: score *= 1 + weight * linkRank. This mirrors the
  // geo-boost window rerank — it reorders the current page rather than pulling
  // in new candidates, so it stays a bounded, cheap post-pass with no change to
  // the block-max scoring loop. Skipped entirely (no doc-value fetch) unless the
  // index carries a linkGraph block and the boost weight is positive.
  // Resolve the active link-graph boost weight for a query, or 0 when the prior
  // must not apply. The prior is a *relevance* prior: it only makes sense on a
  // text query ranked by score. A browse (no query) or an explicit sort has
  // zero/degenerate scores that multiplying-then-resorting would destroy, so
  // those are excluded outright.
  function linkRankBoostWeight(params) {
    const cfg = manifest.linkGraph;
    if (!cfg || !cfg.field) return 0;
    if (!params.q || params.sort || params.geo?.sort) return 0;
    if (params.linkRank === false || params.linkRankBoost === 0) return 0;
    const weight = Number(params.linkRankBoost ?? cfg.boost ?? 0);
    return weight > 0 ? weight : 0;
  }

  // Multiply each result's score by its authority prior (1 + weight*linkRank)
  // and resort. Mutates `results` in place; returns whether any score changed.
  async function applyLinkRankBoostToResults(results, weight) {
    const field = manifest.linkGraph?.field;
    if (!field || !results?.length) return false;
    const indexes = results.map(result => result.index).filter(Number.isInteger);
    if (!indexes.length) return false;
    await ensureDocValuesManifest();
    const store = await valueStoreForDocs([field], indexes);
    let touched = false;
    for (const result of results) {
      if (!Number.isInteger(result.index)) continue;
      const rank = valueForDoc(store, field, result.index);
      if (typeof rank === "number" && Number.isFinite(rank) && rank > 0) {
        result.score *= 1 + weight * rank;
        touched = true;
      }
    }
    if (touched) results.sort((a, b) => b.score - a.score || a.index - b.index);
    return touched;
  }

  // Relevance search with the authority prior. To let a well-linked page win a
  // near-tie that sits just outside the requested page, we rank a bounded wider
  // window through the normal (fully hydrated) path, apply the prior, then
  // return the requested slice. Over-materialization is capped by
  // LINKRANK_MAX_POOL and only happens on boosted relevance queries; facet
  // counts are unaffected (they are computed over the full match set, not this
  // window).
  const LINKRANK_OVERFETCH = 4;
  const LINKRANK_MAX_POOL = 100;
  async function executeBoostedSearch(params, weight) {
    const cfg = manifest.linkGraph;
    const size = Math.max(1, Math.floor(Number(params.size ?? 10)));
    const page = Math.max(1, Math.floor(Number(params.page ?? 1)));
    const factor = Math.max(1, Number(params.linkRankOverfetch ?? cfg.overfetch ?? LINKRANK_OVERFETCH));
    const windowSize = Math.min(LINKRANK_MAX_POOL, Math.max(size * page, Math.ceil(size * page * factor)));
    const wide = await executeSearch({ ...params, size: windowSize, page: 1 });
    const boosted = await applyLinkRankBoostToResults(wide.results, weight);
    const offset = (page - 1) * size;
    const results = (wide.results || []).slice(offset, offset + size);
    const response = {
      ...wide,
      results,
      page,
      size,
      stats: {
        ...(wide.stats || {}),
        linkRankBoost: boosted,
        linkRankBoostPool: wide.results?.length || 0,
        linkRankBoostWindow: results.length
      }
    };
    if (params.facets) await traceSpan("facets.count", () => computeFacetCounts(params, response));
    return response;
  }

  async function search(params = {}) {
    const surfaceQuery = String(params.q || "");
    const normalizedQuery = normalizePostalCodeSpacing(surfaceQuery);
    const activeParams = normalizedQuery === surfaceQuery ? params : { ...params, q: normalizedQuery };
    const trace = activeParams.trace || options.trace ? createRuntimeTrace() : null;
    const response = await withRuntimeTrace(trace, () => traceSpan("search.total", async () => {
      const boostWeight = linkRankBoostWeight(activeParams);
      if (boostWeight > 0) {
        return traceSpan("linkRank.boost", () => executeBoostedSearch(activeParams, boostWeight));
      }
      const searchResponse = await executeSearch(activeParams);
      if (activeParams.facets) await traceSpan("facets.count", () => computeFacetCounts(activeParams, searchResponse));
      return searchResponse;
    }));
    const normalizedResponse = normalizedQuery === surfaceQuery ? response : {
      ...response,
      normalizedQuery,
      stats: { ...(response.stats || {}), postalCodeNormalized: true }
    };
    if (!trace) return normalizedResponse;
    return {
      ...normalizedResponse,
      stats: {
        ...(normalizedResponse.stats || {}),
        trace: finalizeRuntimeTrace(trace)
      }
    };
  }

  async function count(params = {}) {
    const surfaceQuery = String(params.q || "");
    const normalizedQuery = normalizePostalCodeSpacing(surfaceQuery);
    const activeParams = normalizedQuery === surfaceQuery ? params : { ...params, q: normalizedQuery };
    const trace = activeParams.trace || options.trace ? createRuntimeTrace() : null;
    const response = await withRuntimeTrace(trace, () => traceSpan("count.total", () => executeCount(activeParams)));
    if (!trace) return response;
    const finalizedTrace = finalizeRuntimeTrace(trace);
    return {
      ...response,
      trace: finalizedTrace,
      stats: {
        ...(response.stats || {}),
        trace: finalizedTrace
      }
    };
  }

  async function suggest(params = {}) {
    const surfaceQuery = String(params.q || "");
    const normalizedQuery = normalizePostalCodePrefixSpacing(surfaceQuery);
    const activeParams = normalizedQuery === surfaceQuery ? params : { ...params, q: normalizedQuery };
    const trace = activeParams.trace || options.trace ? createRuntimeTrace() : null;
    const response = await withRuntimeTrace(trace, () => traceSpan("suggest.total", () => executeSuggest(activeParams)));
    if (normalizedQuery !== surfaceQuery) {
      response.q = surfaceQuery;
      response.normalizedQuery = normalizedQuery;
      response.stats = { ...(response.stats || {}), postalCodeNormalized: true };
    }
    if (!trace) return response;
    return {
      ...response,
      stats: {
        ...(response.stats || {}),
        trace: finalizeRuntimeTrace(trace)
      }
    };
  }

  async function vectorSearch(params = {}) {
    const trace = params.trace || options.trace ? createRuntimeTrace() : null;
    const response = await withRuntimeTrace(trace, () => traceSpan("vector.total", () => executeVectorSearch(params)));
    if (!trace) return response;
    return {
      ...response,
      stats: {
        ...(response.stats || {}),
        trace: finalizeRuntimeTrace(trace)
      }
    };
  }

  async function hydrateRows(rows, context = {}) {
    return rowsToResults(rows, context);
  }

  // Doc-value lookup for a handful of docs — the generational layer uses it
  // to merge sorted pages across generations by actual sort keys.
  async function loadDocValues(fields, indexes) {
    if (!indexes.length) return [];
    await ensureDocValuesManifest();
    const store = await valueStoreForDocs(fields, indexes);
    return indexes.map(index => Object.fromEntries(fields.map(field => [field, valueForDoc(store, field, index)])));
  }

  return {
    manifest,
    analyzer,
    search,
    count,
    suggest,
    vectorSearch,
    hydrateRows,
    loadDocValues,
    loadBuildTelemetry,
    loadIndexOptimizer,
    loadSegmentManifest,
    loadFacetValues: loadFacetDictionary
  };
}

// Generational indexes published by `rangefind build --update`: every
// generation is a complete single index; this layer fans queries out and
// merges results. Scores are comparable by construction — delta builds bake
// impact scores with corpus-wide document frequencies and totals.
async function createGenerationalSearch(root, options, baseUrl) {
  const generations = root.generations;
  const engines = await Promise.all(generations.map(generation => {
    const path = generation.path || "";
    const manifestName = generation.manifest?.startsWith(path) && path
      ? generation.manifest.slice(path.length)
      : generation.manifest || "manifest.min.json";
    return createSearch({
      ...options,
      baseUrl: new URL(path || "./", baseUrl).href,
      manifestName,
      maxPageSize: 1000
    });
  }));
  const tombstones = generations.map(generation => new Set(generation.tombstones || []));
  const tombstoneTotal = tombstones.reduce((sum, set) => sum + set.size, 0);

  const primaryManifest = engines[0]?.manifest || {};
  const sortableFields = new Set([
    ...(primaryManifest.numbers || []),
    ...(primaryManifest.booleans || [])
  ].map(field => field.name));

  // Mirrors the single-engine makeSortPlan: unknown fields fall back to the
  // unsorted path, exactly like they would inside one generation.
  function parseSortPlan(sort) {
    if (!sort) return null;
    const field = typeof sort === "string" ? sort.replace(/^-/, "") : sort.field;
    if (!field || !sortableFields.has(field)) return null;
    const order = typeof sort === "string" && sort.startsWith("-")
      ? "desc"
      : String(sort.order || sort.direction || "asc").toLowerCase();
    return { field, desc: order === "desc" };
  }

  function compareMergedTies(a, b) {
    return (b.score || 0) - (a.score || 0) || a.generation - b.generation || a.index - b.index;
  }

  // Hydrate merged rows per owning generation, then reassemble in merged
  // order; row extras (distanceMeters, hybrid lanes) survive onto results.
  async function hydrateMergedRows(pageRows) {
    const byGeneration = new Map();
    for (const row of pageRows) {
      if (!byGeneration.has(row.generation)) byGeneration.set(row.generation, []);
      byGeneration.get(row.generation).push(row);
    }
    const hydrated = new Map();
    await Promise.all([...byGeneration.entries()].map(async ([genIndex, rows]) => {
      const results = await engines[genIndex].hydrateRows(rows.map(row => [row.index, row.score]));
      rows.forEach((row, i) => {
        hydrated.set(`${genIndex}:${row.index}`, {
          ...results[i],
          generation: genIndex,
          ...(row.distanceMeters != null ? { distanceMeters: row.distanceMeters } : {}),
          ...(row.hybrid ? { hybrid: row.hybrid } : {})
        });
      });
    }));
    return pageRows
      .map(row => hydrated.get(`${row.generation}:${row.index}`))
      .filter(Boolean);
  }

  const mergeFacetResponses = mergeFederatedFacets;

  function applyMergedHighlights(params, q, results, correctedQuery) {
    if (!params.highlight || !results.length || !q) return;
    const highlightOptions = params.highlight === true ? {} : params.highlight;
    // Generations are built with one frozen analysis profile, so the
    // first engine's analyzer speaks for all of them.
    const genAnalyzer = engines[0]?.analyzer;
    applyHighlights(results, highlightTermSet(q, correctedQuery, genAnalyzer), { ...highlightOptions, analyzer: genAnalyzer });
  }

  function mergedResponseShell(responses, results, page, size) {
    const correctedQuery = responses.map(response => response.correctedQuery).find(Boolean);
    const total = responses.reduce((sum, response) => sum + (response.total || 0), 0) - tombstoneTotal;
    const approximate = tombstoneTotal > 0 || responses.some(response => response.approximate);
    return {
      total: Math.max(results.length, total),
      results,
      page,
      size,
      approximate,
      ...(correctedQuery ? { correctedQuery, corrections: responses.find(r => r.correctedQuery)?.corrections } : {}),
      stats: {
        generations: engines.length,
        generationTotals: responses.map(response => response.total || 0),
        tombstones: tombstoneTotal
      }
    };
  }

  async function search(params = {}) {
    const surfaceQuery = String(params.q || "");
    const normalizedQuery = normalizePostalCodeSpacing(surfaceQuery);
    const activeParams = normalizedQuery === surfaceQuery ? params : { ...params, q: normalizedQuery };
    if (activeParams.vector) return hybridSearch(activeParams);
    const q = String(activeParams.q || "").trim();
    const page = Math.max(1, Number(params.page || 1));
    const size = Math.max(1, Math.min(100, Math.floor(Number(params.size || 10))));
    const offset = (page - 1) * size;
    const sortPlan = parseSortPlan(params.sort);
    const geoDistanceSort = params.geo?.sort === "distance";
    if (sortPlan && geoDistanceSort) throw new Error("Rangefind: use either sort or geo.sort, not both.");

    // Ordered lanes (explicit sort, nearest-first geo, geo browse) return
    // hydrated pages per generation; the merge needs real keys, not scores.
    if (sortPlan || geoDistanceSort || (params.geo && !q)) {
      return orderedSearch(activeParams, { q, page, size, offset, sortPlan, geoDistanceSort });
    }

    const responses = await Promise.all(engines.map((engine, genIndex) => engine.search({
      ...activeParams,
      includeResults: false,
      highlight: undefined,
      page: 1,
      size: Math.min(1000, offset + size + tombstones[genIndex].size)
    })));

    const merged = [];
    for (let genIndex = 0; genIndex < responses.length; genIndex++) {
      for (const row of responses[genIndex].results || []) {
        if (tombstones[genIndex].has(row.index)) continue;
        merged.push({
          generation: genIndex,
          index: row.index,
          score: row.score || 0,
          ...(row.distanceMeters != null ? { distanceMeters: row.distanceMeters } : {})
        });
      }
    }
    if (q) merged.sort(compareMergedTies);
    const pageRows = merged.slice(offset, offset + size);
    const results = await hydrateMergedRows(pageRows);

    const response = mergedResponseShell(responses, results, page, size);
    // Each generation applies the authority prior to its own candidates before
    // the merge (linkRank is normalized per generation); surface that in the
    // merged stats so the federation lane is as observable as the monolithic one.
    if (responses.some(r => r?.stats?.linkRankBoost)) {
      response.stats.linkRankBoost = true;
      response.stats.linkRankBoostPool = responses.reduce((sum, r) => sum + (r?.stats?.linkRankBoostPool || 0), 0);
    }
    applyMergedHighlights(activeParams, q, results, response.correctedQuery);
    if (activeParams.facets) response.facets = mergeFacetResponses(responses);
    if (normalizedQuery !== surfaceQuery) {
      response.normalizedQuery = normalizedQuery;
      response.stats.postalCodeNormalized = true;
    }
    return response;
  }

  // Sorted browse, text + sort, geo browse, and nearest-first geo: each
  // generation returns its own correctly ordered hydrated page; the merge
  // re-orders by the actual key (doc value or meters), never by rank.
  async function orderedSearch(params, { q, page, size, offset, sortPlan, geoDistanceSort }) {
    const responses = await Promise.all(engines.map((engine, genIndex) => engine.search({
      ...params,
      highlight: undefined,
      facets: params.facets,
      page: 1,
      size: Math.min(1000, offset + size + tombstones[genIndex].size)
    })));

    const merged = [];
    for (let genIndex = 0; genIndex < responses.length; genIndex++) {
      for (const result of responses[genIndex].results || []) {
        if (Number.isInteger(result.index) && tombstones[genIndex].has(result.index)) continue;
        merged.push({ generation: genIndex, index: result.index, score: result.score || 0, result });
      }
    }

    if (sortPlan) {
      await Promise.all(engines.map(async (engine, genIndex) => {
        const rows = merged.filter(row => row.generation === genIndex);
        if (!rows.length) return;
        const values = await engine.loadDocValues([sortPlan.field], rows.map(row => row.index));
        rows.forEach((row, i) => {
          row.key = values[i]?.[sortPlan.field];
        });
      }));
      merged.sort((a, b) => {
        const leftMissing = a.key == null;
        const rightMissing = b.key == null;
        if (leftMissing || rightMissing) {
          if (leftMissing && rightMissing) return compareMergedTies(a, b);
          return leftMissing ? 1 : -1;
        }
        if (a.key !== b.key) return sortPlan.desc ? b.key - a.key : a.key - b.key;
        return compareMergedTies(a, b);
      });
    } else if (geoDistanceSort) {
      merged.sort((a, b) => {
        const left = a.result.distanceMeters;
        const right = b.result.distanceMeters;
        const leftMissing = left == null;
        const rightMissing = right == null;
        if (leftMissing || rightMissing) {
          if (leftMissing && rightMissing) return compareMergedTies(a, b);
          return leftMissing ? 1 : -1;
        }
        return left - right || compareMergedTies(a, b);
      });
    }
    // Plain geo browse keeps generation order — the single engine makes no
    // ordering promise there either.

    const results = merged
      .slice(offset, offset + size)
      .map(row => ({ ...row.result, generation: row.generation }));
    const response = mergedResponseShell(responses, results, page, size);
    applyMergedHighlights(params, q, results, response.correctedQuery);
    if (params.facets) response.facets = mergeFacetResponses(responses);
    return response;
  }

  // Hybrid text + vector: fuse at the MERGED level — per-generation RRF
  // ranks are not comparable (a small delta hands its docs top ranks), but
  // merged text scores and merged similarities both are.
  async function hybridSearch(params) {
    const q = String(params.q || "").trim();
    const page = Math.max(1, Number(params.page || 1));
    const size = Math.max(1, Math.min(100, Math.floor(Number(params.size || 10))));
    const offset = (page - 1) * size;

    // Vector lane through engine.search so per-engine doc-value filters
    // apply; similarities are absolute, so the merge is a plain sort.
    async function vectorLaneRows(need) {
      const responses = await Promise.all(engines.map((engine, genIndex) => engine.search({
        q: "",
        vector: params.vector,
        vectorField: params.vectorField,
        hybrid: params.hybrid,
        filters: params.filters,
        page: 1,
        size: Math.min(1000, need + tombstones[genIndex].size)
      })));
      const rows = [];
      for (let genIndex = 0; genIndex < responses.length; genIndex++) {
        for (const result of responses[genIndex].results || []) {
          if (tombstones[genIndex].has(result.index)) continue;
          rows.push({ generation: genIndex, index: result.index, score: result.score || 0, result });
        }
      }
      rows.sort(compareMergedTies);
      return { rows: rows.slice(0, need), responses };
    }

    if (!q) {
      const { rows, responses } = await vectorLaneRows(offset + size);
      const results = rows
        .slice(offset, offset + size)
        .map(row => ({ ...row.result, generation: row.generation }));
      const response = mergedResponseShell(responses, results, page, size);
      response.approximate = true;
      return response;
    }

    const poolSize = Math.max(size * 3, Math.min(100, size * 5));
    const rrfK = Math.max(1, Math.floor(Number(params.hybrid?.rrfK || 60)));
    const [textResponses, vectorLane] = await Promise.all([
      Promise.all(engines.map((engine, genIndex) => engine.search({
        ...params,
        vector: undefined,
        vectorField: undefined,
        hybrid: undefined,
        highlight: undefined,
        includeResults: false,
        page: 1,
        size: Math.min(1000, poolSize + tombstones[genIndex].size)
      }))),
      vectorLaneRows(poolSize)
    ]);

    const textRows = [];
    for (let genIndex = 0; genIndex < textResponses.length; genIndex++) {
      for (const row of textResponses[genIndex].results || []) {
        if (tombstones[genIndex].has(row.index)) continue;
        textRows.push({ generation: genIndex, index: row.index, score: row.score || 0 });
      }
    }
    textRows.sort(compareMergedTies);

    const fused = new Map();
    const addRanked = (rows, lane) => {
      rows.forEach((row, rank) => {
        const key = `${row.generation}:${row.index}`;
        const entry = fused.get(key) || { generation: row.generation, index: row.index, score: 0, hybrid: {} };
        entry.score += 1 / (rrfK + rank + 1);
        entry.hybrid[lane] = rank + 1;
        fused.set(key, entry);
      });
    };
    addRanked(textRows.slice(0, poolSize), "text");
    addRanked(vectorLane.rows, "vector");

    const ranked = [...fused.values()].sort(compareMergedTies);
    const pageRows = ranked.slice(offset, offset + size);
    const results = await hydrateMergedRows(pageRows);
    applyMergedHighlights(params, q, results, undefined);
    return {
      total: ranked.length,
      results,
      page,
      size,
      approximate: true,
      stats: {
        generations: engines.length,
        tombstones: tombstoneTotal,
        hybrid: true
      }
    };
  }

  // Standalone ANN queries: per-generation similarities share one metric
  // (normalized dot products), so cross-generation merge is a heap by score.
  async function vectorSearch(params = {}) {
    const k = Math.max(1, Math.min(200, Math.floor(Number(params.k || 10))));
    const responses = await Promise.all(engines.map((engine, genIndex) => engine.vectorSearch({
      ...params,
      includeResults: false,
      k: Math.min(200, k + tombstones[genIndex].size)
    })));
    const merged = [];
    for (let genIndex = 0; genIndex < responses.length; genIndex++) {
      for (const row of responses[genIndex].results || []) {
        if (tombstones[genIndex].has(row.index)) continue;
        merged.push({ generation: genIndex, index: row.index, score: row.score || 0 });
      }
    }
    merged.sort(compareMergedTies);
    const pageRows = merged.slice(0, k);
    const results = params.includeResults === false
      ? pageRows.map(row => ({ index: row.index, score: row.score, generation: row.generation }))
      : await hydrateMergedRows(pageRows);
    return {
      total: merged.length,
      results,
      stats: {
        generations: engines.length,
        tombstones: tombstoneTotal,
        perGeneration: responses.map(response => response.stats || {}),
        exact: false,
        approximate: true
      }
    };
  }

  async function suggest(params = {}) {
    const surfaceQuery = String(params.q || "");
    const normalizedQuery = normalizePostalCodePrefixSpacing(surfaceQuery);
    const activeParams = normalizedQuery === surfaceQuery ? params : { ...params, q: normalizedQuery };
    const size = Math.max(1, Math.min(50, Math.floor(Number(params.size || 8))));
    const responses = await Promise.all(engines.map(engine => engine.suggest(activeParams)));
    const merged = new Map();
    for (const response of responses) {
      for (const item of response.suggestions) {
        const existing = merged.get(item.text);
        if (existing) {
          existing.count += item.count;
          existing.weight = Math.max(existing.weight, item.weight);
        } else {
          merged.set(item.text, { ...item });
        }
      }
    }
    const suggestions = [...merged.values()]
      .sort((a, b) => b.weight - a.weight || (a.text < b.text ? -1 : 1))
      .slice(0, size);
    return {
      q: surfaceQuery,
      suggestions,
      ...(normalizedQuery !== surfaceQuery ? { normalizedQuery } : {}),
      stats: {
        generations: engines.length,
        ...(normalizedQuery !== surfaceQuery ? { postalCodeNormalized: true } : {})
      }
    };
  }

  async function count(params = {}) {
    const surfaceQuery = String(params.q || "");
    const normalizedQuery = normalizePostalCodeSpacing(surfaceQuery);
    const activeParams = normalizedQuery === surfaceQuery ? params : { ...params, q: normalizedQuery };
    const responses = await Promise.all(engines.map(engine => engine.count(activeParams)));
    return {
      total: responses.reduce((sum, response) => sum + (response.total || 0), 0),
      totalExact: tombstoneTotal === 0 && responses.every(response => response.totalExact),
      approximate: tombstoneTotal > 0 || responses.some(response => response.approximate),
      ...(normalizedQuery !== surfaceQuery ? { normalizedQuery } : {}),
      stats: {
        generations: engines.length,
        tombstones: tombstoneTotal,
        ...(normalizedQuery !== surfaceQuery ? { postalCodeNormalized: true } : {})
      }
    };
  }

  return {
    manifest: root,
    generations: engines,
    search,
    suggest,
    count,
    vectorSearch,
    loadFacetValues: field => engines[0].loadFacetValues(field)
  };
}

// Facet merging shared by the generational and sharded federation layers:
// counts add across sub-indexes, exactness survives only if every side was
// exact.
function mergeFederatedFacets(responses) {
  const facets = {};
  for (const response of responses) {
    for (const [field, data] of Object.entries(response.facets || {})) {
      const target = facets[field] || (facets[field] = { values: [], exact: true, byValue: new Map() });
      target.exact = target.exact && data.exact !== false;
      for (const item of data.values) {
        const existing = target.byValue.get(item.value);
        if (existing) existing.count += item.count;
        else target.byValue.set(item.value, { ...item });
      }
    }
  }
  for (const data of Object.values(facets)) {
    data.values = [...data.byValue.values()].sort((a, b) => b.count - a.count || (a.value < b.value ? -1 : 1));
    delete data.byValue;
  }
  return facets;
}

// Shard routing must never exclude a shard that could hold a matching
// document, so distance estimates are haversine to the coordinate-clamped
// bbox point (antimeridian-aware) and every exclusion decision applies
// SHARD_ROUTING_SLACK: the clamped point is not always the exact great-circle
// argmin (high latitudes + wide longitude gaps), but the deviation is far
// inside 5% + 10km. Per-shard geo lanes verify exact distances, so slack only
// costs an occasional extra shard query.
const SHARD_ROUTING_SLACK_RATIO = 1.05;
const SHARD_ROUTING_SLACK_METERS = 10000;
const TEXT_ROUTING_PREFIX_MIN_LENGTH = 3;
const TEXT_ROUTING_PREFIX_SEGMENT_LIMIT = 8;

function shardRoutingBudget(meters) {
  return meters * SHARD_ROUTING_SLACK_RATIO + SHARD_ROUTING_SLACK_METERS;
}

function wrapLonDelta(a, b) {
  return ((a - b + 540) % 360) - 180;
}

function shardBboxDistanceMeters(bbox, lat, lon) {
  const clampedLat = Math.min(Math.max(lat, bbox[0]), bbox[2]);
  let clampedLon = lon;
  if (!(lon >= bbox[1] && lon <= bbox[3])) {
    // Outside the range: snap to whichever edge is nearer around the globe.
    clampedLon = Math.abs(wrapLonDelta(lon, bbox[1])) <= Math.abs(wrapLonDelta(lon, bbox[3])) ? bbox[1] : bbox[3];
  }
  return haversineMetersE7(latToE7(lat), lonToE7(lon), latToE7(clampedLat), lonToE7(clampedLon));
}

function shardBoxIntersects(bbox, box) {
  const minLat = Math.min(Number(box.minLat), Number(box.maxLat));
  const maxLat = Math.max(Number(box.minLat), Number(box.maxLat));
  if (bbox[2] < minLat || bbox[0] > maxLat) return false;
  const minLon = Number(box.minLon);
  const maxLon = Number(box.maxLon);
  if (minLon <= maxLon) return bbox[3] >= minLon && bbox[1] <= maxLon;
  // Query box crossing the antimeridian: two longitude ranges.
  return bbox[3] >= minLon || bbox[1] <= maxLon;
}

// Sharded indexes: the root manifest lists independently built shard indexes
// (typically geographic regions) with coverage bboxes. Shard engines open
// lazily; geo queries route to the shards whose coverage can match, text
// queries fan out; merged scores are exactly comparable because every shard
// bakes impacts from one frozen scoring-stats artifact (see
// src/scoring_stats.js). A shard may itself be generational — createSearch
// dispatches per shard manifest, so the layers compose.
async function createShardedSearch(root, options, baseUrl) {
  const shards = (root.shards || []).map((shard, index) => ({
    id: String(shard.id || `shard-${index}`),
    index,
    path: shard.path || "",
    manifestName: shard.manifest || "manifest.min.json",
    total: shard.total || 0,
    bbox: Array.isArray(shard.bbox) && shard.bbox.length === 4 ? shard.bbox.map(Number) : null,
    groups: Array.isArray(shard.groups) ? shard.groups.map(String) : []
  }));
  if (!shards.length) throw new Error("Rangefind: sharded manifest has no shards.");

  const engines = new Array(shards.length).fill(null);
  function engineAt(index) {
    if (!engines[index]) {
      const shard = shards[index];
      const manifestName = shard.manifestName.startsWith(shard.path) && shard.path
        ? shard.manifestName.slice(shard.path.length)
        : shard.manifestName;
      engines[index] = createSearch({
        ...options,
        baseUrl: new URL(shard.path || "./", baseUrl).href,
        manifestName,
        maxPageSize: 1000,
        // The federated root owns one trace spanning every child. Giving each
        // concurrent shard its own module-level trace would fragment or mix
        // the receipt; child operations inherit the root's active trace.
        trace: false
      });
    }
    return engines[index];
  }

  const sortableFields = new Set([
    ...(root.numbers || []),
    ...(root.booleans || [])
  ].map(field => field.name));

  function parseSortPlan(sort) {
    if (!sort) return null;
    const field = typeof sort === "string" ? sort.replace(/^-/, "") : sort.field;
    if (!field || !sortableFields.has(field)) return null;
    const order = typeof sort === "string" && sort.startsWith("-")
      ? "desc"
      : String(sort.order || sort.direction || "asc").toLowerCase();
    return { field, desc: order === "desc" };
  }

  const shardIdIndex = new Map(shards.map(shard => [shard.id, shard.index]));
  const shardGroupIndex = new Map();
  for (const shard of shards) {
    for (const group of shard.groups) {
      if (!shardGroupIndex.has(group)) shardGroupIndex.set(group, []);
      shardGroupIndex.get(group).push(shard.index);
    }
  }

  // Explicit shard scoping: `params.shards: ["quebec"]` restricts a query
  // to the named shards before geo routing applies. Names resolve
  // multi-level: a shard id or a group label ("canada" expands to every
  // member shard). Unknown names throw — a typo silently searching the
  // wrong region is worse than an error.
  function candidateShards(params) {
    if (params?.shards == null) return shards;
    const names = Array.isArray(params.shards) ? params.shards : [params.shards];
    const picked = new Set();
    for (const name of names) {
      const id = String(name);
      const index = shardIdIndex.get(id);
      if (index !== undefined) {
        picked.add(index);
        continue;
      }
      const members = shardGroupIndex.get(id);
      if (!members) {
        throw new Error(`Rangefind: unknown shard or group "${id}" (shards: ${shards.map(shard => shard.id).join(", ")}${shardGroupIndex.size ? `; groups: ${[...shardGroupIndex.keys()].join(", ")}` : ""}).`);
      }
      for (const member of members) picked.add(member);
    }
    return [...picked].sort((a, b) => a - b).map(index => shards[index]);
  }

  // Text routing: the root-level term directory (text_routing.js) maps every
  // analyzed term to the shards containing it. A shard can only satisfy a
  // text query when it holds at least minShouldMatch of some query plan's
  // base terms, so shards below that support are skipped without opening
  // them. Fail-open everywhere: shards unknown to the routing table are
  // always searched, and a query no shard supports falls back to the full
  // fan-out so per-shard typo correction keeps working.
  const textRouting = root.text_routing?.format === TEXT_ROUTING_FORMAT && root.text_routing.directory?.root
    ? createTextRoutingState(root.text_routing)
    : null;

  function createTextRoutingState(block) {
    const routedIds = new Set((block.shard_ids || []).map(String));
    const shardIndexByOrdinal = (block.shard_ids || []).map(id => shardIdIndex.get(String(id)) ?? -1);
    const alwaysSelected = shards.filter(shard => !routedIds.has(shard.id)).map(shard => shard.index);
    let analyzer = null;
    let rootPromise = null;
    const pageCache = new Map();
    const segmentCache = new Map();

    function routingAnalyzer() {
      if (!analyzer) analyzer = analyzerFromManifest({ analysis: block.analysis || null });
      return analyzer;
    }

    function loadRoot() {
      if (!rootPromise) {
        rootPromise = fetchGzipArrayBuffer(new URL(block.directory.root, baseUrl)).then(parseDirectoryRoot);
      }
      return rootPromise;
    }

    function loadPage(page) {
      if (!pageCache.has(page.file)) {
        const path = `${String(block.directory.pages || "text-routing/directory-pages/").replace(/\/?$/u, "/")}${page.file}`;
        pageCache.set(page.file, fetchGzipArrayBuffer(new URL(path, baseUrl)).then(buffer => {
          const entries = parseDirectoryPage(buffer, { packTable: block.directory.pack_table || [] });
          return { entries, keys: [...entries.keys()] };
        }));
      }
      return pageCache.get(page.file);
    }

    function loadSegment(entry) {
      const key = `${entry.pack}:${entry.offset}`;
      if (!segmentCache.has(key)) {
        segmentCache.set(key, fetchRange(new URL(`text-routing/packs/${entry.pack}`, baseUrl), entry.offset, entry.length)
          .then(buffer => inflateGzip(buffer))
          .then(parseTextRoutingSegment));
      }
      return segmentCache.get(key);
    }

    // Root shard indexes containing `term` ([] when the term is unindexed).
    async function shardIndexesForTerm(term) {
      const directoryRoot = await loadRoot();
      const pageIndex = floorDirectoryPageIndex(directoryRoot, term);
      if (pageIndex < 0) return [];
      const page = await loadPage(directoryRoot.pages[pageIndex]);
      const keyIndex = floorSortedKeyIndex(page.keys, term);
      if (keyIndex < 0) return [];
      const segment = await loadSegment(page.entries.get(page.keys[keyIndex]));
      const ordinals = segment.get(term);
      if (!ordinals) return [];
      const indexes = [];
      for (const ordinal of ordinals) {
        const index = shardIndexByOrdinal[ordinal];
        if (index >= 0) indexes.push(index);
      }
      return indexes;
    }

    // Autocomplete needs a prefix range rather than an exact term. Start at
    // the segment that can own the prefix and walk forward until the sorted
    // term range ends. Broad prefixes are bounded and fail open so routing
    // can never turn a cheap suggestion into an unbounded directory scan.
    async function shardIndexesForPrefix(prefix) {
      const directoryRoot = await loadRoot();
      if (!directoryRoot.pages?.length) return { indexes: [], complete: true, segments: 0 };
      let pageIndex = floorDirectoryPageIndex(directoryRoot, prefix);
      if (pageIndex < 0) pageIndex = 0;
      const upper = `${prefix}\uffff`;
      const selected = new Set();
      let segments = 0;
      for (; pageIndex < directoryRoot.pages.length; pageIndex++) {
        const pageSummary = directoryRoot.pages[pageIndex];
        if (pageSummary.first > upper) break;
        const page = await loadPage(pageSummary);
        let keyIndex = floorSortedKeyIndex(page.keys, prefix);
        if (keyIndex < 0) keyIndex = 0;
        for (; keyIndex < page.keys.length; keyIndex++) {
          const key = page.keys[keyIndex];
          if (key > upper) return { indexes: [...selected], complete: true, segments };
          if (segments >= TEXT_ROUTING_PREFIX_SEGMENT_LIMIT) {
            return { indexes: [], complete: false, segments };
          }
          segments++;
          const segment = await loadSegment(page.entries.get(key));
          for (const [term, ordinals] of segment) {
            if (term < prefix) continue;
            if (term > upper) return { indexes: [...selected], complete: true, segments };
            if (!term.startsWith(prefix)) continue;
            for (const ordinal of ordinals) {
              const index = shardIndexByOrdinal[ordinal];
              if (index >= 0) selected.add(index);
            }
          }
        }
      }
      return { indexes: [...selected], complete: true, segments };
    }

    // Selection across every query plan (primary + alternate languages):
    // a shard qualifies when it supports minShouldMatch of any plan.
    async function selectShards(q) {
      const plan = routingAnalyzer().queryPlan(q);
      const plans = [plan, ...(plan.altPlans || [])];
      const uniqueTerms = new Set();
      for (const item of plans) for (const term of item.baseTerms) uniqueTerms.add(term);
      if (!uniqueTerms.size) return null;
      const byTerm = new Map(await Promise.all([...uniqueTerms].map(async term => [term, await shardIndexesForTerm(term)])));
      const selected = new Set(alwaysSelected);
      for (const item of plans) {
        const planTerms = [...new Set(item.baseTerms)];
        if (!planTerms.length) continue;
        const need = Math.min(planTerms.length, Math.max(1, minShouldMatchFor(item.baseTerms)));
        const support = new Map();
        for (const term of planTerms) {
          for (const index of byTerm.get(term) || []) support.set(index, (support.get(index) || 0) + 1);
        }
        for (const [index, count] of support) {
          if (count >= need) selected.add(index);
        }
      }
      return { selected, terms: uniqueTerms.size };
    }

    async function selectSuggestShards(q) {
      if (block.suggest_prefix !== true) return null;
      const plan = routingAnalyzer().queryPlan(q);
      const plans = [plan, ...(plan.altPlans || [])];
      const prefixes = new Set();
      for (const item of plans) {
        const prefix = item.baseTerms.at(-1) || "";
        if (Array.from(prefix).length >= TEXT_ROUTING_PREFIX_MIN_LENGTH) prefixes.add(prefix);
      }
      if (!prefixes.size) return null;
      const scans = await Promise.all([...prefixes].map(prefix => shardIndexesForPrefix(prefix)));
      const segments = scans.reduce((sum, scan) => sum + scan.segments, 0);
      if (scans.some(scan => !scan.complete)) {
        return { fallback: "prefix-scan-limit", prefixes: prefixes.size, segments };
      }
      const matched = new Set();
      for (const scan of scans) for (const index of scan.indexes) matched.add(index);
      return {
        selected: new Set([...alwaysSelected, ...matched]),
        matched: matched.size,
        prefixes: prefixes.size,
        segments
      };
    }

    return { selectShards, selectSuggestShards };
  }

  // Applies text routing to an already geo/scope-routed selection. Returns
  // the narrowed route plus stats, or the original route when routing does
  // not apply or no shard survives (typo fallback).
  async function withTextRoute(route, q) {
    if (!textRouting || !q) return { route, stats: null };
    let selection;
    try {
      selection = await textRouting.selectShards(q);
    } catch {
      // A broken or missing routing artifact must never break search.
      return { route, stats: { fallback: "error" } };
    }
    if (!selection) return { route, stats: null };
    const narrowed = route.expanding
      ? { selected: null, expanding: route.expanding.filter(item => selection.selected.has(item.index)) }
      : { selected: route.selected.filter(index => selection.selected.has(index)), expanding: null };
    const kept = route.expanding ? narrowed.expanding.length : narrowed.selected.length;
    if (!kept) return { route, stats: { terms: selection.terms, fallback: "no-shard-support" } };
    return { route: narrowed, stats: { terms: selection.terms, selected: kept } };
  }

  async function withSuggestRoute(params, q) {
    const candidates = candidateShards(params);
    if (!textRouting || !q) return { shards: candidates, stats: null };
    let selection;
    try {
      selection = await textRouting.selectSuggestShards(q);
    } catch {
      return { shards: candidates, stats: { fallback: "error" } };
    }
    if (!selection) return { shards: candidates, stats: null };
    if (selection.fallback) return { shards: candidates, stats: selection };
    const narrowed = candidates.filter(shard => selection.selected.has(shard.index));
    if (!selection.matched || !narrowed.length) {
      return {
        shards: candidates,
        stats: {
          prefixes: selection.prefixes,
          segments: selection.segments,
          fallback: "no-shard-support"
        }
      };
    }
    return {
      shards: narrowed,
      stats: {
        prefixes: selection.prefixes,
        segments: selection.segments,
        selected: narrowed.length
      }
    };
  }

  // Routing: which shards can contain a match for this geo context, within
  // the explicitly scoped candidates. Returns either a fixed selection or
  // an expanding front (nearest-first queries with no radius: shards
  // ordered by bbox distance, visited until the next shard cannot beat the
  // page's worst kept distance).
  function routeShards(geo, params) {
    const candidates = candidateShards(params);
    const all = candidates.map(shard => shard.index);
    if (!geo || !candidates.some(shard => shard.bbox)) return { selected: all, expanding: null };
    if (geo.near) {
      const lat = Number(geo.near.lat);
      const lon = Number(geo.near.lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return { selected: all, expanding: null };
      const distances = candidates.map(shard => ({
        index: shard.index,
        meters: shard.bbox ? shardBboxDistanceMeters(shard.bbox, lat, lon) : 0
      }));
      const radius = Number(geo.near.radiusMeters);
      if (Number.isFinite(radius) && radius > 0) {
        return {
          selected: distances.filter(item => item.meters <= shardRoutingBudget(radius)).map(item => item.index),
          expanding: null
        };
      }
      return { selected: null, expanding: distances.sort((a, b) => a.meters - b.meters) };
    }
    if (geo.box) {
      return {
        selected: candidates
          .filter(shard => !shard.bbox || shardBoxIntersects(shard.bbox, geo.box))
          .map(shard => shard.index),
        expanding: null
      };
    }
    return { selected: all, expanding: null };
  }

  // Results carry their shard id; when a shard is itself a sharded index
  // (hierarchical roots compose through createSearch dispatch), the inner
  // id is preserved as a path: "north-america/canada/quebec".
  function stampShard(result, shardIndex) {
    const id = shards[shardIndex].id;
    return { ...result, shard: result.shard != null ? `${id}/${result.shard}` : id };
  }

  async function searchShard(index, params) {
    const engine = await engineAt(index);
    return { index, response: await engine.search(params) };
  }

  // Expanding nearest-first: query shards in bbox-distance order and stop
  // once the merged page cannot improve — the next shard's minimum possible
  // distance already exceeds the page's worst kept result.
  async function expandingNearestQuery(front, params, need) {
    const queried = [];
    const distances = [];
    let cursor = 0;
    while (cursor < front.length) {
      if (distances.length >= need) {
        distances.sort((a, b) => a - b);
        // Same slack as routing: only stop when the next shard's minimum
        // possible distance clearly exceeds the page's worst kept result.
        if (front[cursor].meters > shardRoutingBudget(distances[need - 1])) break;
      }
      const batch = front.slice(cursor, cursor + 2);
      cursor += batch.length;
      const batchResults = await Promise.all(batch.map(item => searchShard(item.index, params)));
      for (const item of batchResults) {
        queried.push(item);
        for (const result of item.response.results || []) {
          if (result.distanceMeters != null) distances.push(result.distanceMeters);
        }
      }
    }
    return { queried, partial: cursor < front.length };
  }

  function compareByScore(a, b) {
    return (b.result.score || 0) - (a.result.score || 0)
      || a.shardIndex - b.shardIndex
      || (a.result.index || 0) - (b.result.index || 0);
  }

  function sortKeyNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function shardResponseShell(responses, results, page, size, meta = {}) {
    const correctedQuery = responses.map(response => response.correctedQuery).find(Boolean);
    const total = responses.reduce((sum, response) => sum + (response.total || 0), 0);
    return {
      total: Math.max(results.length, total),
      results,
      page,
      size,
      approximate: Boolean(meta.partial) || responses.some(response => response.approximate),
      ...(correctedQuery ? { correctedQuery, corrections: responses.find(r => r.correctedQuery)?.corrections } : {}),
      stats: {
        shards: shards.length,
        shardsQueried: responses.length,
        shardTotals: responses.map(response => response.total || 0),
        ...(meta.stats || {})
      }
    };
  }

  async function executeShardedSearch(params = {}) {
    const surfaceQuery = String(params.q || "");
    const normalizedQuery = normalizePostalCodeSpacing(surfaceQuery);
    const activeParams = normalizedQuery === surfaceQuery ? params : { ...params, q: normalizedQuery };
    if (activeParams.vector) return hybridSearch(activeParams);
    const q = String(activeParams.q || "").trim();
    const page = Math.max(1, Number(params.page || 1));
    const size = Math.max(1, Math.min(100, Math.floor(Number(params.size || 10))));
    const offset = (page - 1) * size;
    const need = offset + size;
    const sortPlan = parseSortPlan(params.sort);
    const geoDistanceSort = activeParams.geo?.sort === "distance";
    if (sortPlan && geoDistanceSort) throw new Error("Rangefind: use either sort or geo.sort, not both.");

    const routed = await withTextRoute(routeShards(activeParams.geo, activeParams), q);
    const route = routed.route;
    const perShardParams = { ...activeParams, shards: undefined, page: 1, size: Math.min(1000, need) };

    let queried;
    // Beyond each shard's 1000-row page cap the merge can no longer prove
    // completeness; the response is flagged approximate instead of silently
    // dropping rows.
    let partial = need > 1000;
    if (route.expanding) {
      const expanded = await expandingNearestQuery(route.expanding, perShardParams, need);
      queried = expanded.queried;
      partial = partial || expanded.partial;
    } else {
      queried = await Promise.all(route.selected.map(index => searchShard(index, perShardParams)));
    }

    const rows = [];
    for (const { index, response } of queried) {
      for (const result of response.results || []) {
        rows.push({ shardIndex: index, result });
      }
    }

    if (sortPlan) {
      // Merge by the real key. Shards return their pages already ordered;
      // the key rides on the hydrated payload (sort fields belong in
      // `display` for sharded browses).
      rows.sort((a, b) => {
        const left = sortKeyNumber(a.result[sortPlan.field]);
        const right = sortKeyNumber(b.result[sortPlan.field]);
        if (left == null || right == null) {
          if (left == null && right == null) return compareByScore(a, b);
          return left == null ? 1 : -1;
        }
        if (left !== right) return sortPlan.desc ? right - left : left - right;
        return compareByScore(a, b);
      });
    } else if (geoDistanceSort) {
      rows.sort((a, b) => {
        const left = a.result.distanceMeters;
        const right = b.result.distanceMeters;
        if (left == null || right == null) {
          if (left == null && right == null) return compareByScore(a, b);
          return left == null ? 1 : -1;
        }
        return left - right || compareByScore(a, b);
      });
    } else if (q) {
      rows.sort(compareByScore);
    }
    // Plain geo browse keeps shard order — the single engine makes no
    // ordering promise there either.

    const results = rows.slice(offset, need).map(row => stampShard(row.result, row.shardIndex));
    const responses = queried.map(item => item.response);
    const response = shardResponseShell(responses, results, page, size, {
      partial,
      ...(routed.stats ? { stats: { textRouting: routed.stats } } : {})
    });
    // Each shard applies the authority prior to its own (disjoint) candidates
    // before the merge; reflect that in the merged stats.
    if (responses.some(r => r?.stats?.linkRankBoost)) {
      response.stats.linkRankBoost = true;
      response.stats.linkRankBoostPool = responses.reduce((sum, r) => sum + (r?.stats?.linkRankBoostPool || 0), 0);
    }
    if (activeParams.facets) {
      response.facets = mergeFederatedFacets(responses);
      if (partial) {
        // An early-stopped expanding front merged only the shards it
        // visited — counts cover a subset of the corpus.
        for (const facet of Object.values(response.facets)) facet.exact = false;
      }
    }
    if (normalizedQuery !== surfaceQuery) {
      response.normalizedQuery = normalizedQuery;
      response.stats.postalCodeNormalized = true;
    }
    return response;
  }

  async function search(params = {}) {
    const trace = params.trace || options.trace ? createRuntimeTrace() : null;
    if (!trace) return executeShardedSearch(params);
    const response = await withRuntimeTrace(trace, () => traceSpan(
      "shards.searchTotal",
      () => executeShardedSearch({ ...params, trace: false })
    ));
    return {
      ...response,
      stats: {
        ...(response.stats || {}),
        trace: finalizeRuntimeTrace(trace)
      }
    };
  }

  // Hybrid text + vector across shards: fuse at the merged level, like the
  // generational layer — per-shard RRF ranks are not comparable, but merged
  // text scores and merged similarities both are.
  async function hybridSearch(params) {
    const q = String(params.q || "").trim();
    const page = Math.max(1, Number(params.page || 1));
    const size = Math.max(1, Math.min(100, Math.floor(Number(params.size || 10))));
    const offset = (page - 1) * size;
    const route = routeShards(params.geo, params);
    const selected = route.expanding ? route.expanding.map(item => item.index) : route.selected;
    const poolSize = Math.max(size * 3, Math.min(100, size * 5));

    async function lane(laneParams) {
      const queried = await Promise.all(selected.map(index => searchShard(index, { ...laneParams, shards: undefined, page: 1, size: Math.min(1000, poolSize) })));
      const rows = [];
      for (const { index, response } of queried) {
        for (const result of response.results || []) {
          rows.push({ shardIndex: index, result });
        }
      }
      rows.sort(compareByScore);
      return { rows: rows.slice(0, poolSize), responses: queried.map(item => item.response) };
    }

    const vectorLaneParams = {
      q: "",
      vector: params.vector,
      vectorField: params.vectorField,
      hybrid: params.hybrid,
      filters: params.filters,
      geo: params.geo
    };
    if (!q) {
      const { rows, responses } = await lane(vectorLaneParams);
      const results = rows.slice(offset, offset + size).map(row => stampShard(row.result, row.shardIndex));
      const response = shardResponseShell(responses, results, page, size);
      response.approximate = true;
      return response;
    }

    const rrfK = Math.max(1, Math.floor(Number(params.hybrid?.rrfK || 60)));
    const [textLane, vectorLane] = await Promise.all([
      lane({ ...params, vector: undefined, vectorField: undefined, hybrid: undefined }),
      lane(vectorLaneParams)
    ]);

    const fused = new Map();
    const addRanked = (rows, laneName) => {
      rows.forEach((row, rank) => {
        // Generational shards reuse `index` per generation, so the key
        // includes the generation to keep fused docs distinct.
        const key = `${row.shardIndex}:${row.result.generation ?? ""}:${row.result.index}`;
        const entry = fused.get(key) || { shardIndex: row.shardIndex, result: row.result, score: 0, hybrid: {} };
        entry.score += 1 / (rrfK + rank + 1);
        entry.hybrid[laneName] = rank + 1;
        fused.set(key, entry);
      });
    };
    addRanked(textLane.rows, "text");
    addRanked(vectorLane.rows, "vector");

    const ranked = [...fused.values()].sort((a, b) => b.score - a.score || a.shardIndex - b.shardIndex || (a.result.index || 0) - (b.result.index || 0));
    const results = ranked.slice(offset, offset + size).map(entry => ({
      ...stampShard(entry.result, entry.shardIndex),
      score: entry.score,
      hybrid: entry.hybrid
    }));
    return {
      total: ranked.length,
      results,
      page,
      size,
      approximate: true,
      stats: { shards: shards.length, shardsQueried: selected.length, hybrid: true }
    };
  }

  async function vectorSearch(params = {}) {
    const k = Math.max(1, Math.min(200, Math.floor(Number(params.k || 10))));
    const queried = await Promise.all(candidateShards(params).map(async shard => {
      const engine = await engineAt(shard.index);
      return { index: shard.index, response: await engine.vectorSearch({ ...params, shards: undefined, k }) };
    }));
    const rows = [];
    for (const { index, response } of queried) {
      for (const result of response.results || []) {
        rows.push({ shardIndex: index, result });
      }
    }
    rows.sort(compareByScore);
    const results = rows.slice(0, k).map(row => stampShard(row.result, row.shardIndex));
    return {
      total: rows.length,
      results,
      stats: {
        shards: shards.length,
        perShard: queried.map(item => item.response.stats || {}),
        exact: false,
        approximate: true
      }
    };
  }

  async function suggest(params = {}) {
    const surfaceQuery = String(params.q || "");
    const normalizedQuery = normalizePostalCodePrefixSpacing(surfaceQuery);
    const activeParams = normalizedQuery === surfaceQuery ? params : { ...params, q: normalizedQuery };
    const size = Math.max(1, Math.min(50, Math.floor(Number(params.size || 8))));
    const routed = await withSuggestRoute(activeParams, normalizedQuery.trim());
    const responses = await Promise.all(routed.shards.map(async shard => {
      const engine = await engineAt(shard.index);
      return { shard, response: await engine.suggest({ ...activeParams, shards: undefined }) };
    }));
    const merged = new Map();
    for (const { shard, response } of responses) {
      for (const item of response.suggestions) {
        const existing = merged.get(item.text);
        if (existing) {
          existing.count += item.count;
          if (item.weight > existing.weight) {
            existing.weight = item.weight;
            existing.shards = [shard.id];
          } else if (item.weight === existing.weight && !existing.shards.includes(shard.id)) {
            // Route a selected suggestion only to the shard(s) responsible
            // for its winning rank. Weak same-text rows elsewhere contribute
            // to `count` but should not reopen those regions on selection.
            existing.shards.push(shard.id);
          }
        } else {
          // Preserve the top-level shard provenance so a selected suggestion
          // can hand the following search directly to the region(s) that
          // produced it. This is additive metadata; callers that ignore it
          // retain the existing autocomplete contract.
          merged.set(item.text, { ...item, shards: [shard.id] });
        }
      }
    }
    const suggestions = [...merged.values()]
      .sort((a, b) => b.weight - a.weight || (a.text < b.text ? -1 : 1))
      .slice(0, size);
    return {
      q: surfaceQuery,
      suggestions,
      ...(normalizedQuery !== surfaceQuery ? { normalizedQuery } : {}),
      stats: {
        shards: shards.length,
        shardsQueried: routed.shards.length,
        ...(routed.stats ? { textRouting: routed.stats } : {}),
        ...(normalizedQuery !== surfaceQuery ? { postalCodeNormalized: true } : {})
      }
    };
  }

  async function count(params = {}) {
    const surfaceQuery = String(params.q || "");
    const normalizedQuery = normalizePostalCodeSpacing(surfaceQuery);
    const activeParams = normalizedQuery === surfaceQuery ? params : { ...params, q: normalizedQuery };
    const routed = await withTextRoute(routeShards(activeParams.geo, activeParams), String(activeParams.q || "").trim());
    const route = routed.route;
    const selected = route.expanding ? route.expanding.map(item => item.index) : route.selected;
    const responses = await Promise.all(selected.map(async index => {
      const engine = await engineAt(index);
      return engine.count({ ...activeParams, shards: undefined });
    }));
    return {
      total: responses.reduce((sum, response) => sum + (response.total || 0), 0),
      totalExact: responses.every(response => response.totalExact),
      approximate: responses.some(response => response.approximate),
      ...(normalizedQuery !== surfaceQuery ? { normalizedQuery } : {}),
      stats: {
        shards: shards.length,
        shardsQueried: selected.length,
        ...(routed.stats ? { textRouting: routed.stats } : {}),
        ...(normalizedQuery !== surfaceQuery ? { postalCodeNormalized: true } : {})
      }
    };
  }

  async function loadFacetValues(field) {
    const dictionaries = await Promise.all(shards.map(async shard => {
      const engine = await engineAt(shard.index);
      return engine.loadFacetValues(field);
    }));
    const merged = new Map();
    for (const values of dictionaries) {
      for (const item of values || []) {
        if (!item?.value) continue;
        const existing = merged.get(item.value);
        if (existing) existing.n += item.n || 0;
        else merged.set(item.value, { ...item });
      }
    }
    return [...merged.values()].sort((a, b) => (b.n || 0) - (a.n || 0) || (a.value < b.value ? -1 : 1));
  }

  return {
    manifest: root,
    shards: shards.map(shard => ({ id: shard.id, path: shard.path, total: shard.total, bbox: shard.bbox })),
    search,
    suggest,
    count,
    vectorSearch,
    loadFacetValues
  };
}

export default createSearch;
