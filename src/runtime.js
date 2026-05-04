import { analyzeTerms, expandedTermsFromBaseTerms, proximityTerm, queryBundleKeysFromBaseTerms, queryTerms, tokenize } from "./analyzer.js";
import { authorityKeysForQuery, authorityNormalizeSurface, parseAuthorityShard } from "./authority_codec.js";
import { decodePostingBlock, decodePostingBytes, decodePostings, lookupDecodedPostingRows, lookupPostingBlock, lookupPostingBytes, parseCodes, parseDocValueChunk, parseFacetDictionary, parsePostingSegment } from "./codec.js";
import { findDirectoryPage, parseDirectoryPage, parseDirectoryRoot } from "./directory.js";
import { DOC_PAGE_ENCODING, decodeDocPageColumns } from "./doc_pages.js";
import { decodeDocValueSortPage, parseDocValueSortDirectory } from "./doc_value_tree.js";
import { decodeDocOrdinalRecord, decodeDocPointerRecord } from "./doc_pointers.js";
import { filterBitmapHas, parseFilterBitmap } from "./filter_bitmaps.js";
import { verifyBlockPointer } from "./object_store.js";
import { parseQueryBundle } from "./query_bundle_codec.js";
import { decodeSegmentRows, parseSegmentTerms } from "./segment_codec.js";
import { groupRanges, shardKey } from "./shards.js";
import { parseTypoLexiconShard } from "./typo_lexicon.js";
import {
  boundedDamerauLevenshtein,
  parseTypoShard,
  typoCandidateScore,
  typoCandidatesForDeleteKey,
  typoDeleteKeys,
  typoMaxEditsFor
} from "./typo_runtime.js";

const RERANK_CANDIDATES = 30;
const DEPENDENCY_SCORE_SCALE = 0.12;
const SKIP_MAX_TERMS = 30;
const EXTERNAL_POSTING_BLOCK_PREFETCH = 16;
const POSTING_BLOCK_FRONTIER = 4;
const FILTER_BITMAP_SPARSE_DOC_LIMIT = 256;
const TYPO_CORRECTION_CANDIDATES_PER_TOKEN = 2;
const TYPO_CORRECTION_PLAN_LIMIT = 6;
const TYPO_CORRECTION_EXECUTION_PLAN_LIMIT = 1;
const TYPO_CORRECTION_RELATIVE_SCORE = 0.5;
const TYPO_LEXICON_MIN_SCAN_CANDIDATES = 64;
const DOC_RANGE_PLANNER_MIN_CANDIDATE_RANGES = 2;
const DOC_RANGE_PLANNER_MAX_CANDIDATE_BLOCK_RATIO = 0.75;
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

function traceBucketFromPath(path) {
  if (path.endsWith("/manifest.min.json")) return "manifest";
  if (/\/manifest(?:\.[0-9a-f]+)?\.json(?:\.gz)?$/u.test(path)) return "manifest";
  if (path.includes("/terms/block-packs/")) return "postingBlocks";
  if (path.includes("/terms/packs/")) return "terms";
  if (path.includes("/bundles/packs/")) return "queryBundles";
  if (path.includes("/authority/packs/")) return "authority";
  if (path.includes("/typo/lexicon-packs/") || path.includes("/typo/lexicon/")) return "typoLexicon";
  if (path.includes("/typo/")) return "typo";
  if (path.includes("/doc-values/sorted")) return "docValueSorted";
  if (path.includes("/doc-values/")) return "docValues";
  if (path.includes("/docs/ordinals/")) return "docOrdinals";
  if (path.includes("/docs/pointers/")) return "docPointers";
  if (path.includes("/docs/pages/")) return "docPagePointers";
  if (path.includes("/docs/page-packs/")) return "docPages";
  if (path.includes("/docs/")) return "docs";
  if (path.includes("/facets/")) return "facetDictionaries";
  if (path.includes("/filter-bitmaps/")) return "filterBitmaps";
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
  if (value.startsWith("typo lexicon")) return "typoLexicon";
  if (value.startsWith("typo shard")) return "typo";
  if (value.startsWith("doc-value sort page")) return "docValueSorted";
  if (value.startsWith("doc-value")) return "docValues";
  if (value.startsWith("doc page")) return "docPages";
  if (value.startsWith("doc ")) return "docs";
  if (value.startsWith("facet dictionary")) return "facetDictionaries";
  if (value.startsWith("filter bitmap")) return "filterBitmaps";
  return "object";
}

function recordTraceSpan(trace, name, ms) {
  if (!trace || !Number.isFinite(ms)) return;
  const current = trace.spans.get(name) || { name, count: 0, totalMs: 0, maxMs: 0 };
  current.count++;
  current.totalMs += ms;
  current.maxMs = Math.max(current.maxMs, ms);
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
  return {
    totalMs: nowMs() - trace.started,
    spans: [...trace.spans.values()]
      .map(span => ({
        name: span.name,
        count: span.count,
        totalMs: span.totalMs,
        maxMs: span.maxMs
      }))
      .sort((left, right) => right.totalMs - left.totalMs || left.name.localeCompare(right.name))
  };
}

async function inflateGzip(responseOrBuffer) {
  if (!("DecompressionStream" in globalThis)) {
    throw new Error("Rangefind requires DecompressionStream for compressed static index files.");
  }
  const stream = responseOrBuffer instanceof ArrayBuffer
    ? new Blob([responseOrBuffer]).stream()
    : responseOrBuffer.body;
  if (!stream) throw new Error("Response body is not streamable.");
  return new Response(stream.pipeThrough(new DecompressionStream("gzip"))).arrayBuffer();
}

async function fetchGzipArrayBuffer(url) {
  const bucket = traceBucketFromUrl(url);
  const response = await traceSpan(`${bucket}.fetch`, () => fetch(url));
  if (!response.ok) throw new Error(`Unable to fetch ${url}`);
  return traceSpan(`${bucket}.inflate`, () => inflateGzip(response));
}

async function fetchRange(url, offset, length) {
  const bucket = traceBucketFromUrl(url);
  const response = await traceSpan(`${bucket}.fetch`, () => fetch(url, {
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
    const response = await fetch(new URL(path, baseUrl));
    return response.ok ? response.json() : null;
  }

  async function fetchManifestJsonIfOk(path) {
    if (!path) return null;
    if (!String(path).endsWith(".gz")) return fetchJsonIfOk(path);
    const url = new URL(path, baseUrl);
    const response = await fetch(url);
    if (!response.ok) return null;
    const inflated = await traceSpan("manifest.inflate", () => inflateGzip(response));
    return traceSpanSync("manifest.parse", () => JSON.parse(textDecoder.decode(inflated)));
  }

  const manifest = await fetchJsonIfOk("manifest.min.json") || await fetchJsonIfOk("manifest.json");
  if (!manifest) throw new Error("Rangefind manifest could not be loaded.");
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
  const typoShardCache = new Map();
  const typoLexiconShardCache = new Map();
  const segmentTermsCache = new Map();
  const segmentRowsCache = new Map();
  const docOrdinalCache = new Map();
  const docPointerCache = new Map();
  const packedDocCache = new Map();
  const docPagePointerCache = new Map();
  const docPageCache = new Map();
  const docValueCache = new Map();
  const docValueSortDirectoryCache = new Map();
  const docValueSortPageCache = new Map();
  const facetDictionaryCache = new Map();
  let typoManifest = null;
  let typoManifestPromise = null;
  let typoDirectory = null;
  let typoLexiconDirectory = null;
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
  const filterBitmapCache = new Map();
  const numberFields = new Map((manifest.numbers || []).map(field => [field.name, field]));
  const booleanFields = new Map((manifest.booleans || []).map(field => [field.name, field]));
  let blockFilterFields = new Set((manifest.block_filters || []).map(filter => filter.name));
  const rangePlans = {
    default: { mergeGapBytes: 8 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: 4 },
    docOrdinals: { mergeGapBytes: 8 * 1024, maxOverfetchBytes: 4 * 1024, maxOverfetchRatio: Infinity },
    docPointers: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 32 * 1024, maxOverfetchRatio: Infinity },
    docs: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 8 * 1024, maxOverfetchRatio: Infinity },
    docPagePointers: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 32 * 1024, maxOverfetchRatio: Infinity },
    docPages: { mergeGapBytes: 64 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: Infinity },
    docValueSortPages: { mergeGapBytes: 64 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: Infinity },
    authority: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 32 * 1024, maxOverfetchRatio: Infinity },
    postingBlocks: { mergeGapBytes: 256 * 1024, maxMergedBytes: 1024 * 1024, maxOverfetchBytes: 512 * 1024, maxOverfetchRatio: Infinity },
    postingBlockFrontier: { mergeGapBytes: 512 * 1024, maxMergedBytes: 2 * 1024 * 1024, maxOverfetchBytes: 1024 * 1024, maxOverfetchRatio: Infinity },
    postingDocRanges: { mergeGapBytes: 512 * 1024, maxMergedBytes: 2 * 1024 * 1024, maxOverfetchBytes: 1024 * 1024, maxOverfetchRatio: Infinity },
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
  const docRangePlannerEnabled = options.docRangePlanner !== false;
  const typoCorrectionExecutionPlanLimit = Math.max(1, Math.min(
    TYPO_CORRECTION_PLAN_LIMIT,
    Math.floor(Number(options.typoCorrectionExecutionPlans || TYPO_CORRECTION_EXECUTION_PLAN_LIMIT))
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

  async function loadDocValueSortPages(field, directory, pageIndexes) {
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

    await Promise.all(rangeGroups(pending, "docValueSortPages").map(async (group) => {
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
    return mergeValueStores(await valueStoreForDocs(fields, docs), bitmapStore);
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

  async function loadTypoManifest() {
    if (typoManifest !== null) return typoManifest;
    if (!typoManifestPromise) {
      if (!manifest.typo?.manifest) {
        typoManifest = false;
        return typoManifest;
      }
      typoManifestPromise = fetch(new URL(manifest.typo.manifest, baseUrl))
        .then(response => response.ok ? response.json() : false)
        .catch(() => false);
    }
    typoManifest = await typoManifestPromise;
    if (typoManifest) typoDirectory = createDirectoryState(typoManifest.directory);
    if (typoManifest?.lexicon?.directory) typoLexiconDirectory = createDirectoryState(typoManifest.lexicon.directory);
    return typoManifest;
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

  async function loadTypoShards(shards) {
    const meta = await loadTypoManifest();
    if (!meta) return new Map();

    const wanted = [];
    const pending = [];
    const unique = new Map();
    for (const item of shards) if (!unique.has(item.shard)) unique.set(item.shard, item);
    for (const { shard, entry } of unique.values()) {
      wanted.push(shard);
      if (typoShardCache.has(shard)) continue;
      if (!entry) continue;
      let resolve;
      let reject;
      const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
      });
      promise.catch(() => {});
      typoShardCache.set(shard, promise);
      pending.push({ shard, entry, resolve, reject });
    }

    await Promise.all(rangeGroups(pending).map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`typo/packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `typo shard ${item.shard}`);
          item.resolve(traceSpanSync("typo.parse", () => parseTypoShard(inflated)));
        }));
      } catch (error) {
        for (const item of group.items) {
          typoShardCache.delete(item.shard);
          item.reject(error);
        }
        throw error;
      }
    }));

    const out = new Map();
    await Promise.all(wanted.map(async (shard) => {
      const data = await typoShardCache.get(shard);
      if (data) out.set(shard, data);
    }));
    return out;
  }

  async function loadTypoLexiconShards(shards) {
    const meta = await loadTypoManifest();
    if (!meta?.lexicon) return new Map();

    const wanted = [];
    const pending = [];
    const unique = new Map();
    for (const item of shards) if (!unique.has(item.shard)) unique.set(item.shard, item);
    for (const { shard, entry } of unique.values()) {
      wanted.push(shard);
      if (typoLexiconShardCache.has(shard)) continue;
      if (!entry) continue;
      let resolve;
      let reject;
      const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
      });
      promise.catch(() => {});
      typoLexiconShardCache.set(shard, promise);
      pending.push({ shard, entry, resolve, reject });
    }

    await Promise.all(rangeGroups(pending).map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`typo/lexicon-packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const inflated = await inflateGroupItem(compressed, group.start, item, `typo lexicon ${item.shard}`);
          item.resolve(traceSpanSync("typoLexicon.parse", () => parseTypoLexiconShard(inflated)));
        }));
      } catch (error) {
        for (const item of group.items) {
          typoLexiconShardCache.delete(item.shard);
          item.reject(error);
        }
        throw error;
      }
    }));

    const out = new Map();
    await Promise.all(wanted.map(async (shard) => {
      const data = await typoLexiconShardCache.get(shard);
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
        ? fetch(new URL(path, baseUrl)).then(response => {
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

  async function tryQueryBundleSearch({ page, size, baseTerms, filters, sortPlan, rerank }) {
    return traceSpan("queryBundles.search", () => tryQueryBundleSearchInner({ page, size, baseTerms, filters, sortPlan, rerank }));
  }

  async function tryQueryBundleSearchInner({ page, size, baseTerms, filters, sortPlan, rerank }) {
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
      const results = await rowsToResults(rows, resultContext);
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

  async function loadDocOrdinals(indexes) {
    const ordinalMeta = docPointers?.ordinals;
    if (!ordinalMeta?.file) throw new Error("Rangefind index is missing dense doc ordinals.");
    const pending = [];
    const unique = [...new Set(indexes)];
    for (const index of unique) {
      if (docOrdinalCache.has(index)) continue;
      let resolveOrdinal;
      let rejectOrdinal;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolveOrdinal = resolvePromise;
        rejectOrdinal = rejectPromise;
      });
      promise.catch(() => {});
      docOrdinalCache.set(index, promise);
      const offset = ordinalMeta.dataOffset + index * ordinalMeta.recordBytes;
      pending.push({
        index,
        entry: { pack: ordinalMeta.file, offset, length: ordinalMeta.recordBytes },
        resolve: resolveOrdinal,
        reject: rejectOrdinal
      });
    }

    await Promise.all(rangeGroups(pending, "docOrdinals").map(async (group) => {
      try {
        const buffer = await fetchRange(new URL(group.pack, baseUrl), group.start, group.end - group.start);
        for (const item of group.items) {
          item.resolve(decodeDocOrdinalRecord(buffer, item.entry.offset - group.start, ordinalMeta));
        }
      } catch (error) {
        for (const item of group.items) {
          docOrdinalCache.delete(item.index);
          item.reject(error);
        }
        throw error;
      }
    }));
  }

  async function loadDocPointers(indexes) {
    if (!docPointers?.file || docPointers.order !== "layout") throw new Error("Rangefind index is missing layout-ordered dense doc pointers.");
    await loadDocOrdinals(indexes);
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
      const ordinal = await docOrdinalCache.get(index);
      const offset = docPointers.dataOffset + ordinal * docPointers.recordBytes;
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
    const forced = context.preferDocPages === true;
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
    if (payloadDocs > maxPayloadDocs) return null;
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
      adaptive: !forced
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
          const pointer = decodeDocPointerRecord(buffer, item.entry.offset - group.start, pointerMeta, pointerMeta.pack_table || []);
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
    return plan ? loadDocPages(indexes, plan) : loadPackedDocs(indexes);
  }

  async function rowsToResults(rows, context = {}) {
    return traceSpan("docs.hydrate", async () => {
      const docs = await loadDocs(rows.map(([index]) => index), context);
      return docs.map((doc, i) => ({ ...doc, score: rows[i][1] }));
    });
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
        pending.push({ owner, blockIndex, entry: block.range, resolve: resolveBlock, reject: rejectBlock });
      }

      const groups = rangeGroups(pending, rangePlan);
      await Promise.all(groups.map(async (group) => {
        try {
          const compressed = await fetchRange(new URL(`terms/block-packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
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
    return { facets, numbers, booleans, active: facets.length > 0 || numbers.length > 0 || booleans.length > 0 };
  }

  function filterPlanFields(filterPlan) {
    if (!filterPlan?.active) return [];
    return [
      ...filterPlan.facets.map(([field]) => field),
      ...filterPlan.numbers.map(([field]) => field),
      ...filterPlan.booleans.map(([field]) => field)
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

  function minShouldMatchFor(baseTerms) {
    return baseTerms.length <= 4 ? baseTerms.length : baseTerms.length - 1;
  }

  function collectEligibleScores(scores, hits, minShouldMatch) {
    return [...scores.entries()]
      .filter(([doc]) => (hits.get(doc) || 0) >= Math.max(1, minShouldMatch))
      .sort((a, b) => b[1] - a[1] || a[0] - b[0]);
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

    for (const candidatePage of pages) {
      const [loadedPage] = await loadDocValueSortPages(field, directory, [candidatePage.index]);
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
        docValueRowsScanned: rowsScanned,
        docValueRowsAccepted: rowsAccepted,
        docValueDefinitePages: definitelyPassedPages,
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs
      }
    };
  }

  async function runSortedTextSearch({ page, size, filters, sortPlan, baseTerms, terms, rerank = true }) {
    return traceSpan("sortPageText.search", () => runSortedTextSearchInner({ page, size, filters, sortPlan, baseTerms, terms, rerank }));
  }

  async function runSortedTextSearchInner({ page, size, filters, sortPlan, baseTerms, terms, rerank = true }) {
    const field = sortPlan?.field || null;
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

    for (let pageIndex = 0; pageIndex < candidatePages.length; pageIndex++) {
      const candidatePage = candidatePages[pageIndex];
      const [loadedPage] = await loadDocValueSortPages(field, directory, [candidatePage.index]);
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

  function remainingImpactForCursor(cursor, block, doc = null) {
    const blockImpact = block?.maxImpact || 0;
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
      tieDocLowerBound = Math.max(tieDocLowerBound, block.maxImpactDoc ?? 0);
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
        fallbackPotential += block.maxImpact || 0;
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
    const eligible = collectEligibleScores(scores, hits, minShouldMatch);
    if (eligible.length < k) {
      recordTopKProofFailure(proofStats, "candidate_count");
      return null;
    }

    const top = eligible.slice(0, k);
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
          if (blockMayPass(block, filterPlan) && blockOverlapsDocSpan(block, rangeStart, maxDoc)) indexes.push(blockIndex);
          else skippedBlocks++;
        }
      }
      return { indexes, consideredBlocks, skippedBlocks, consideredSuperblocks, skippedSuperblocks };
    }
    for (let blockIndex = 0; blockIndex < blocks.length; blockIndex++) {
      consideredBlocks++;
      const block = blocks[blockIndex];
      if (blockMayPass(block, filterPlan) && blockOverlapsDocSpan(block, rangeStart, maxDoc)) indexes.push(blockIndex);
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

  function stableDocRangeTopK(scores, hits, minShouldMatch, k, nextRangeUpperBound, proofStats = null) {
    if (proofStats) proofStats.attempts++;
    const eligible = collectEligibleScores(scores, hits, minShouldMatch);
    if (eligible.length < k) {
      recordTopKProofFailure(proofStats, "candidate_count", { maxOutsidePotential: nextRangeUpperBound });
      return null;
    }
    const top = eligible.slice(0, k);
    const threshold = top[top.length - 1][1];
    if (nextRangeUpperBound < threshold) {
      recordTopKProofSuccess(proofStats, {
        threshold,
        maxOutsidePotential: nextRangeUpperBound,
        remainingTerms: 0,
        remainingTermUpperBound: nextRangeUpperBound
      });
      return top;
    }
    recordTopKProofFailure(proofStats, nextRangeUpperBound === threshold ? "tie_bound" : "score_bound", {
      threshold,
      maxOutsidePotential: nextRangeUpperBound,
      remainingTerms: 0,
      remainingTermUpperBound: nextRangeUpperBound
    });
    return null;
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
    minShouldMatch
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
    const scores = new Map();
    const hits = new Map();
    const masks = new Map();
    const decodedBlocks = new Set();
    const proofStats = createTopKProofStats({ hasFilters, blockFilterPlan });
    let stable = null;
    let exhausted = false;
    let rangesVisited = 0;
    let candidatePostingBlocks = 0;
    let consideredPostingBlocks = 0;
    let skippedPostingBlocks = 0;
    let consideredPostingSuperblocks = 0;
    let skippedPostingSuperblocks = 0;
    let blocksDecoded = 0;
    let blocksVisited = 0;
    let postingsDecoded = 0;
    let postingRowsScanned = 0;
    let postingsAccepted = 0;
    let fetchedBlocks = 0;
    let fetchGroups = 0;
    let wantedBlocks = 0;
    let filterSummaryProofBlocks = 0;
    let stopUpperBound = 0;

    for (let rangeIndex = 0; rangeIndex < plan.candidates.length; rangeIndex++) {
      const range = plan.candidates[rangeIndex];
      const rangeStart = range.index * plan.rangeSize;
      const rangeEnd = Math.min(manifest.total, rangeStart + plan.rangeSize);
      rangesVisited++;
      for (const cursor of cursors) {
        const candidates = docRangeCandidateBlockIndexes(cursor.entry, rangeStart, rangeEnd, blockFilterPlan);
        candidatePostingBlocks += candidates.indexes.length;
        consideredPostingBlocks += candidates.consideredBlocks;
        skippedPostingBlocks += candidates.skippedBlocks;
        consideredPostingSuperblocks += candidates.consideredSuperblocks;
        skippedPostingSuperblocks += candidates.skippedSuperblocks;
        if (!candidates.indexes.length) continue;
        const decoded = await decodeEntryBlockBatch(cursor.shard, cursor.entry, candidates.indexes, "postingDocRanges");
        fetchedBlocks += decoded.fetchedBlocks;
        fetchGroups += decoded.fetchGroups;
        wantedBlocks += decoded.wantedBlocks;
        for (const { blockIndex, rows } of decoded.blocks) {
          const block = cursor.entry.blocks?.[blockIndex];
          const blockKey = `${cursor.shardName}\u0000${cursor.term}\u0000${blockIndex}`;
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
      }
      stopUpperBound = plan.candidates[rangeIndex + 1]?.upperBound || 0;
      stable = stableDocRangeTopK(scores, hits, minShouldMatch, candidateK, stopUpperBound, proofStats);
      if (stable) break;
    }
    exhausted = !stable && rangesVisited >= plan.candidates.length;

    let ranked = exhausted
      ? collectEligibleScores(scores, hits, minShouldMatch)
      : stable || collectEligibleScores(scores, hits, minShouldMatch).slice(0, candidateK);
    const reranked = rerank === false
      ? { ranked, stats: { rerankCandidates: 0, dependencyFeatures: 0, dependencyTermsMatched: 0, dependencyPostingsScanned: 0, dependencyCandidateMatches: 0 } }
      : await rerankWithDependencies(ranked, baseTerms, candidateK);
    ranked = reranked.ranked;
    const rows = ranked.slice(offset, offset + size);
    const resultContext = { hasTextTerms: true, preferDocPages: "auto" };
    const results = await rowsToResults(rows, resultContext);
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
        docRangeNextUpperBound: stopUpperBound,
        docRangeTermRangeEntries: plan.termRangeEntries,
        docRangeCandidateBlockRatio: selectivity.ratio,
        docRangeSelectivitySampleBlocks: selectivity.candidateBlocks,
        docRangeSelectivityAvailableBlocks: selectivity.availableBlocks,
        docRangeBlocksVisited: blocksVisited,
        docRangePostingRowsScanned: postingRowsScanned,
        docRangePostingBlocksConsidered: consideredPostingBlocks,
        docRangePostingBlocksCandidate: candidatePostingBlocks,
        docRangePostingBlocksSkipped: skippedPostingBlocks,
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
        ...reranked.stats
      }
    };
  }

  async function runSkippedSearch({ q, page, size, filters, sort, baseTerms, terms, rerank = true }) {
    return traceSpan("text.search", () => runSkippedSearchInner({ q, page, size, filters, sort, baseTerms, terms, rerank }));
  }

  async function runSkippedSearchInner({ q, page, size, filters, sort, baseTerms, terms, rerank = true }) {
    const offset = (page - 1) * size;
    const k = offset + size;
    const candidateK = candidateLimitFor(baseTerms, k, rerank);
    const sortPlan = makeSortPlan(sort);
    if (sortPlan) {
      const sortedText = await runSortedTextSearch({ page, size, filters, sortPlan, baseTerms, terms, rerank });
      if (sortedText) return sortedText;
      return runFullSearch({ q, page, size, filters, sort, baseTerms, terms, rerank, plannerFallbackReason: "sort_requested" });
    }
    if (!baseTerms.length || terms.length > SKIP_MAX_TERMS || k > 100) {
      const plannerFallbackReason = !baseTerms.length
        ? "no_text_terms"
        : terms.length > SKIP_MAX_TERMS
          ? "too_many_terms"
          : "top_k_limit";
      return runFullSearch({ q, page, size, filters, sort, baseTerms, terms, rerank, plannerFallbackReason });
    }

    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    const bundleResponse = await tryQueryBundleSearch({ page, size, baseTerms, filters, sortPlan, rerank });
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
      minShouldMatch
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
    const proofStats = createTopKProofStats({ hasFilters, blockFilterPlan });

    while (true) {
      const active = cursors.filter(cursor => advanceCursor(cursor, blockFilterPlan));
      if (!active.length) {
        exhausted = true;
        break;
      }

      stable = stableTopK(scores, hits, masks, cursors, minShouldMatch, candidateK, blockFilterPlan, proofStats);
      if (stable) break;

      active.sort((a, b) => cursorSuperblockImpact(b) - cursorSuperblockImpact(a) || cursorImpact(b) - cursorImpact(a));
      const frontier = active.slice(0, postingBlockFrontier);
      frontierBatches++;
      frontierBlocks += frontier.length;
      frontierMax = Math.max(frontierMax, frontier.length);
      const decoded = await decodeCursorFrontier(frontier);
      frontierFetchedBlocks += decoded.fetchedBlocks;
      frontierFetchGroups += decoded.fetchGroups;
      frontierWantedBlocks += decoded.wantedBlocks;
      for (const { cursor, rows } of decoded.blocks) {
        const block = cursor.entry.blocks[cursor.blockIndex];
        const filterSummaryProvesBlock = hasFilters && blockDefinitelyPassesDocFilter(block, docFilterPlan);
        if (filterSummaryProvesBlock) filterSummaryProofBlocks++;
        markSuperblockDecoded(cursor);
        cursor.blockIndex++;
        const codeData = hasFilters && !filterSummaryProvesBlock && docValues
          ? await valueStoreForFilterPlan(docFilterPlan, postingDocs(rows))
          : filterSummaryProvesBlock ? null : fallbackCodeData;
        blocksDecoded++;
        postingsDecoded += rows.length / 2;
        postingsAccepted += applyBlockRows(cursor, rows, codeData, filterSummaryProvesBlock ? null : docFilterPlan, scores, hits, masks);
        stable = stableTopK(scores, hits, masks, cursors, minShouldMatch, candidateK, blockFilterPlan, proofStats);
        if (stable) break;
      }
      if (stable) break;
    }

    let ranked = exhausted
      ? collectEligibleScores(scores, hits, minShouldMatch)
      : stable || collectEligibleScores(scores, hits, minShouldMatch).slice(0, k);
    const reranked = rerank === false
      ? { ranked, stats: { rerankCandidates: 0, dependencyFeatures: 0, dependencyTermsMatched: 0, dependencyPostingsScanned: 0, dependencyCandidateMatches: 0 } }
      : await rerankWithDependencies(ranked, baseTerms, candidateK);
    ranked = reranked.ranked;
    const rows = ranked.slice(offset, offset + size);
    const resultContext = { hasTextTerms: true, preferDocPages: "auto" };
    const results = await rowsToResults(rows, resultContext);
    return {
      total: exhausted ? ranked.length : Math.max(ranked.length, k),
      page,
      size,
      approximate: !exhausted,
      results,
      stats: {
        exact: exhausted,
        plannerLane: exhausted ? "fullFallback" : "tailProof",
        topKProven: Boolean(stable || exhausted),
        totalExact: exhausted,
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
        postingSuperblockScheduler: cursors.some(cursor => cursor.entry.superblocks?.length),
        postingSuperblocks: entries.reduce((sum, item) => sum + (item.entry.superblocks?.length || 0), 0),
        postingSuperblocksConsidered: cursors.reduce((sum, cursor) => sum + cursor.superblocksConsidered, 0),
        postingSuperblocksSkipped: cursors.reduce((sum, cursor) => sum + cursor.skippedSuperblocks, 0),
        postingSuperblocksDecoded: cursors.reduce((sum, cursor) => sum + cursor.superblocksDecoded, 0),
        filterSummaryProofBlocks,
        plannerFallbackReason: exhausted ? "tail_exhausted" : "",
        ...topKProofStatsObject(proofStats, exhausted ? "tail_exhausted" : ""),
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        docPayloadAdaptive: resultContext.docPayloadAdaptive,
        ...reranked.stats
      }
    };
  }

  async function runSegmentFanoutSearch({ page, size, filters, sort, baseTerms, terms, rerank = true, plannerFallbackReason = "exact_requested" }) {
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
    const results = await rowsToResults(rows, resultContext);
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
        ...reranked.stats
      }
    };
  }

  async function runFullSearch({ q, page, size, filters, sort, baseTerms, terms, rerank = true, plannerFallbackReason = "full_scan" }) {
    if (plannerFallbackReason === "exact_requested" || options.segmentFanout === true) {
      const segmentResponse = await runSegmentFanoutSearch({ page, size, filters, sort, baseTerms, terms, rerank, plannerFallbackReason });
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
    const results = await rowsToResults(rows, resultContext);
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
        ...reranked.stats
      }
    };
  }

  async function typoCandidatesForToken(token, debug) {
    const meta = await loadTypoManifest();
    if (!meta) return [];
    const minLength = Math.max(2, (meta.min_surface_length || 4) - (meta.max_edits || 2));
    const maxLength = meta.max_surface_length || meta.max_term_length || 24;
    if (token.length < minLength || token.length > maxLength) return [];
    if (!/^[a-z][a-z0-9]*$/u.test(token) || /^\d+$/u.test(token)) return [];

    const maxEdits = typoMaxEditsFor(token, { maxEdits: meta.max_edits || 2 });
    const deleteEnabled = token.length <= Math.max(1, Number(meta.delete_max_surface_length || 8));
    const deleteKeys = deleteEnabled ? [...typoDeleteKeys(token, {
      minTermLength: meta.min_term_length || 4,
      maxEdits: meta.max_edits || 2
    }, maxEdits)] : [];

    async function candidatesForKeys(keys) {
      const byShard = new Map();
      for (const key of keys) {
        const resolved = await resolveDirectoryShard(
          key,
          typoDirectory,
          meta.base_shard_depth || 2,
          meta.max_shard_depth || meta.base_shard_depth || 3
        );
        if (!resolved) continue;
        if (!byShard.has(resolved.shard)) byShard.set(resolved.shard, { shard: resolved.shard, entry: resolved.entry, keys: [] });
        byShard.get(resolved.shard).keys.push(key);
      }

      const candidates = new Map();
      const loaded = await loadTypoShards([...byShard.values()]);
      for (const [shard, bucket] of byShard) {
        debug.shards.add(shard);
        const data = loaded.get(shard);
        if (!data) continue;
        for (const key of bucket.keys) {
          for (const candidate of typoCandidatesForDeleteKey(data, key)) {
            if (candidate.surface === token) continue;
            const candidateKey = `${candidate.surface}\u0001${candidate.term}`;
            const previous = candidates.get(candidateKey);
            if (!previous || candidate.df > previous.df) candidates.set(candidateKey, candidate);
          }
        }
      }
      return candidates;
    }

    function verifiedCandidates(candidates) {
      const verified = [];
      for (const candidate of candidates.values()) {
        const distance = boundedDamerauLevenshtein(token, candidate.surface, maxEdits);
        if (distance <= 0 || distance > maxEdits) continue;
        verified.push({
          ...candidate,
          distance,
          score: typoCandidateScore(token, candidate.surface, candidate.df, distance)
        });
      }
      return verified;
    }

    function mergeVerified(left, right) {
      const merged = new Map();
      for (const candidate of [...left, ...right]) {
        const key = `${candidate.surface}\u0001${candidate.term}`;
        const previous = merged.get(key);
        if (!previous || candidate.score > previous.score || (candidate.score === previous.score && candidate.df > previous.df)) {
          merged.set(key, candidate);
        }
      }
      return [...merged.values()];
    }

    function lexiconLowerBound(entries, value) {
      let low = 0;
      let high = entries.length;
      while (low < high) {
        const mid = Math.floor((low + high) / 2);
        if (entries[mid].surface.localeCompare(value) < 0) low = mid + 1;
        else high = mid;
      }
      return low;
    }

    function lexiconTrie(data) {
      if (data.trie) return data.trie;
      const root = { children: new Map(), entries: [] };
      for (const entry of data.entries || []) {
        let node = root;
        for (const char of entry.surface) {
          let child = node.children.get(char);
          if (!child) {
            child = { children: new Map(), entries: [] };
            node.children.set(char, child);
          }
          node = child;
        }
        node.entries.push(entry);
      }
      data.trie = root;
      return root;
    }

    function levenshteinAutomatonLexiconCandidates(data) {
      const root = lexiconTrie(data);
      const chars = [...token];
      const initial = Array.from({ length: chars.length + 1 }, (_, index) => index);
      const out = [];
      let visited = 0;
      const visit = (node, char, previousRow) => {
        const current = [previousRow[0] + 1];
        let rowMin = current[0];
        for (let column = 1; column <= chars.length; column++) {
          const insertCost = current[column - 1] + 1;
          const deleteCost = previousRow[column] + 1;
          const replaceCost = previousRow[column - 1] + (chars[column - 1] === char ? 0 : 1);
          const value = Math.min(insertCost, deleteCost, replaceCost);
          current[column] = value;
          rowMin = Math.min(rowMin, value);
        }
        if (node.entries.length && current[chars.length] <= maxEdits) {
          for (const entry of node.entries) {
            visited++;
            if (entry.surface === token || Math.abs(entry.surface.length - token.length) > maxEdits) continue;
            const distance = boundedDamerauLevenshtein(token, entry.surface, maxEdits);
            if (distance <= 0 || distance > maxEdits) continue;
            out.push({
              ...entry,
              distance,
              score: typoCandidateScore(token, entry.surface, entry.df, distance)
            });
          }
        }
        if (rowMin > maxEdits) return;
        for (const [nextChar, child] of node.children) visit(child, nextChar, current);
      };
      for (const [char, child] of root.children) visit(child, char, initial);
      return {
        candidates: out
          .sort((a, b) => b.score - a.score || a.distance - b.distance || b.df - a.df || a.term.localeCompare(b.term))
          .slice(0, 64),
        visited
      };
    }

    async function verifiedLexiconCandidates() {
      if (!meta.lexicon || !typoLexiconDirectory) return [];
      if (token.length < (meta.lexicon.min_surface_length || meta.min_surface_length || 4)) return [];
      const depth = meta.lexicon.shard_depth || 2;
      const resolved = await resolveDirectoryShard(token, typoLexiconDirectory, depth, depth);
      if (!resolved) return [];
      const loaded = await loadTypoLexiconShards([{ shard: resolved.shard, entry: resolved.entry }]);
      debug.lexiconShards.add(resolved.shard);
      const data = loaded.get(resolved.shard);
      const entries = data?.entries || [];
      if (data) {
        const automaton = levenshteinAutomatonLexiconCandidates(data);
        debug.lexiconScanned += automaton.visited;
        if (automaton.candidates.some(candidate => candidate.score >= 0.5)) return automaton.candidates;
      }
      const maxScan = Math.max(1, Number(meta.lexicon.max_scan_candidates || 2048));
      const minScan = Math.min(maxScan, TYPO_LEXICON_MIN_SCAN_CANDIDATES);
      const out = [];
      let scanned = 0;
      let left = lexiconLowerBound(entries, token) - 1;
      let right = left + 1;
      let scanRight = true;
      while (scanned < maxScan && (left >= 0 || right < entries.length)) {
        const entry = (scanRight && right < entries.length) || left < 0 ? entries[right++] : entries[left--];
        scanRight = !scanRight;
        scanned++;
        if (!entry || entry.surface === token || Math.abs(entry.surface.length - token.length) > maxEdits) continue;
        const distance = boundedDamerauLevenshtein(token, entry.surface, maxEdits);
        if (distance <= 0 || distance > maxEdits) continue;
        out.push({
          ...entry,
          distance,
          score: typoCandidateScore(token, entry.surface, entry.df, distance)
        });
        if (scanned >= minScan && out.some(candidate => candidate.score >= 0.5)) break;
      }
      debug.lexiconScanned += scanned;
      return out
        .sort((a, b) => b.score - a.score || a.distance - b.distance || b.df - a.df || a.term.localeCompare(b.term))
        .slice(0, 64);
    }

    const candidates = deleteEnabled ? await candidatesForKeys([token]) : new Map();
    let verified = verifiedCandidates(candidates);
    if (deleteEnabled && !verified.some(candidate => candidate.score >= 0.5)) {
      const remainingKeys = deleteKeys.filter(key => key !== token);
      const fallback = await candidatesForKeys(remainingKeys);
      for (const [key, candidate] of fallback) {
        const previous = candidates.get(key);
        if (!previous || candidate.df > previous.df) candidates.set(key, candidate);
      }
      verified = verifiedCandidates(candidates);
    }
    if (!verified.some(candidate => candidate.score >= 0.5)) {
      verified = mergeVerified(verified, await verifiedLexiconCandidates());
    }
    debug.candidates += verified.length;
    return verified
      .sort((a, b) => b.score - a.score || a.distance - b.distance || b.df - a.df || a.term.localeCompare(b.term))
      .slice(0, 64);
  }

  async function correctedTypoQuery(baseTerms, analyzedTerms) {
    return traceSpan("typo.resolve", () => correctedTypoQueryInner(baseTerms, analyzedTerms));
  }

  async function correctedTypoQueryInner(baseTerms, analyzedTerms) {
    if (!baseTerms.length || baseTerms.length > 8) return null;
    const presentTerms = new Map((await termEntries(baseTerms)).map(item => [item.term, item.entry.df || 0]));
    const hasMissingTerms = baseTerms.some(term => !presentTerms.has(term));
    const plans = new Map();
    const debug = { shards: new Set(), lexiconShards: new Set(), candidates: 0, lexiconScanned: 0 };

    for (let index = 0; index < analyzedTerms.length; index++) {
      const item = analyzedTerms[index];
      if (presentTerms.has(item.term) && hasMissingTerms) continue;
      const candidates = await typoCandidatesForToken(item.raw, debug);
      const strongCandidates = candidates.filter(item => item.score >= 0.5);
      const bestScore = strongCandidates[0]?.score || 0;
      const minScore = bestScore >= 1 ? bestScore * TYPO_CORRECTION_RELATIVE_SCORE : 0.5;
      for (const candidate of strongCandidates.filter(item => item.score >= minScore).slice(0, TYPO_CORRECTION_CANDIDATES_PER_TOKEN)) {
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
    const selectedPlans = await rankTypoCorrectionPlans(sortedPlans.slice(0, TYPO_CORRECTION_PLAN_LIMIT));
    return {
      plans: selectedPlans,
      stats: {
        typoCandidateTerms: debug.candidates,
        typoCorrectionPlans: selectedPlans.length,
        typoCorrectionPlansEstimated: selectedPlans.filter(plan => Number.isFinite(plan.estimatedUpperBound)).length,
        typoCorrectionBestUpperBound: selectedPlans[0]?.estimatedUpperBound || 0,
        typoShardLookups: debug.shards.size,
        typoLexiconShardLookups: debug.lexiconShards.size,
        typoLexiconCandidatesScanned: debug.lexiconScanned
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

  async function maybeTypoFallbackInner(params, response, baseTerms, analyzedTerms) {
    if (params.page !== 1 || response.total > 0) return response;
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

  async function maybeAuthorityRerank(params, response) {
    return traceSpan("authority.rerank", () => maybeAuthorityRerankInner(params, response));
  }

  async function maybeAuthorityRerankInner(params, response) {
    const stats = response.stats || {};
    if (!authorityDirectory || options.authority === false || params.authority === false) return response;
    if (params.page !== 1 || params.sort) return response;
    const filters = params.filters || {};
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    if (hasFilters) return response;
    const authorityQuery = String(response.correctedQuery || response.surfaceFallbackQuery || params.q || "").trim();
    if (!authorityQuery || resultTitleMatchesQuery(response.results?.[0], authorityQuery)) return response;

    const authorityTerms = analyzeTerms(authorityQuery).map(item => item.term);
    const keyPlans = authorityKeysForQuery(authorityQuery, authorityTerms).filter(item => item.key);
    const surfaceKeys = [...new Set(keyPlans.filter(item => item.kind === "surface").map(item => item.key))];
    const exactKeys = [...new Set(keyPlans.filter(item => item.kind === "exact").map(item => item.key))];
    const tokenKeys = [...new Set(keyPlans.filter(item => item.kind === "tokens").map(item => item.key))];
    if (!surfaceKeys.length && !exactKeys.length && !tokenKeys.length) return response;

    let loadedKeys = surfaceKeys;
    let entries = await authorityEntries(surfaceKeys);
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
        docPayloadAdaptive: resultContext.docPayloadAdaptive
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
    const q = String(params.q || "").trim();
    const page = Math.max(1, Number(params.page || 1));
    const size = Math.max(1, Math.min(100, Number(params.size || 10)));
    const offset = (page - 1) * size;
    const filters = params.filters || {};
    const sort = params.sort || null;
    const sortPlan = makeSortPlan(sort);
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;

    if (!q) {
      if (sortPlan) await ensureDocValueSortedManifest();
      if (hasFilters) await ensureDocValuesManifest();
      await ensureFacetDictionaries(filters);
      if (!sortPlan && !hasFilters) {
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

    const analyzedTerms = analyzeTerms(q);
    const baseTerms = analyzedTerms.map(item => item.term);
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
      terms: queryTerms(q),
      rerank: params.rerank,
      plannerFallbackReason: params.exact ? "exact_requested" : "full_scan"
    });
    const typoResponse = await maybeTypoFallback({ q, page, size, filters, sort, rerank: params.rerank }, response, baseTerms, analyzedTerms);
    return maybeAuthorityRerank({ q, page, size, filters, sort, rerank: params.rerank, authority: params.authority }, typoResponse);
  }

  async function search(params = {}) {
    const trace = params.trace || options.trace ? createRuntimeTrace() : null;
    const response = await withRuntimeTrace(trace, () => traceSpan("search.total", () => executeSearch(params)));
    if (!trace) return response;
    return {
      ...response,
      stats: {
        ...(response.stats || {}),
        trace: finalizeRuntimeTrace(trace)
      }
    };
  }

  return {
    manifest,
    search,
    loadBuildTelemetry,
    loadIndexOptimizer,
    loadSegmentManifest,
    loadFacetValues: loadFacetDictionary
  };
}

export default createSearch;
