import { analyzeTerms, expandedTermsFromBaseTerms, proximityTerm, queryTerms, tokenize } from "./analyzer.js";
import { decodePostingBlock, decodePostingBytes, decodePostings, parseCodes, parseDocValueChunk, parseFacetDictionary, parseShard } from "./codec.js";
import { findDirectoryPage, parseDirectoryPage, parseDirectoryRoot } from "./directory.js";
import { DOC_PAGE_ENCODING, decodeDocPageColumns } from "./doc_pages.js";
import { decodeDocValueSortPage, parseDocValueSortDirectory } from "./doc_value_tree.js";
import { decodeDocOrdinalRecord, decodeDocPointerRecord } from "./doc_pointers.js";
import { verifyBlockPointer } from "./object_store.js";
import { groupRanges, shardKey } from "./shards.js";
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
const textDecoder = new TextDecoder();

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
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Unable to fetch ${url}`);
  return inflateGzip(response);
}

async function fetchRange(url, offset, length) {
  const response = await fetch(url, {
    headers: { Range: `bytes=${offset}-${offset + length - 1}` }
  });
  if (response.status !== 206) throw new Error(`Range request failed for ${url}`);
  return response.arrayBuffer();
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
  const manifest = await fetch(new URL("manifest.json", baseUrl)).then(r => r.json());
  const verifyChecksums = options.verifyChecksums !== false && !!(manifest.features?.checksummedObjects || manifest.object_store?.checksum);
  const termDirectory = createDirectoryState(manifest.directory);
  const docPointers = manifest.docs?.pointers;
  const docPages = manifest.docs?.pages || null;
  const facetDirectory = manifest.facet_dictionaries?.directory ? createDirectoryState(manifest.facet_dictionaries.directory) : null;
  const shardCache = new Map();
  const typoShardCache = new Map();
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
  let codes = null;
  let codesPromise = null;
  const docValues = manifest.doc_values || null;
  const docValueSorted = manifest.doc_value_sorted || null;
  const docValueStore = { _docValues: true };
  const numberFields = new Map((manifest.numbers || []).map(field => [field.name, field]));
  const booleanFields = new Map((manifest.booleans || []).map(field => [field.name, field]));
  const rangePlans = {
    default: { mergeGapBytes: 8 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: 4 },
    docOrdinals: { mergeGapBytes: 8 * 1024, maxOverfetchBytes: 4 * 1024, maxOverfetchRatio: Infinity },
    docPointers: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 32 * 1024, maxOverfetchRatio: Infinity },
    docs: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 8 * 1024, maxOverfetchRatio: Infinity },
    docPagePointers: { mergeGapBytes: 32 * 1024, maxOverfetchBytes: 32 * 1024, maxOverfetchRatio: Infinity },
    docPages: { mergeGapBytes: 64 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: Infinity },
    docValueSortPages: { mergeGapBytes: 64 * 1024, maxOverfetchBytes: 64 * 1024, maxOverfetchRatio: Infinity },
    postingBlocks: { mergeGapBytes: 256 * 1024, maxMergedBytes: 1024 * 1024, maxOverfetchBytes: 512 * 1024, maxOverfetchRatio: Infinity },
    postingBlockFrontier: { mergeGapBytes: 512 * 1024, maxMergedBytes: 2 * 1024 * 1024, maxOverfetchBytes: 1024 * 1024, maxOverfetchRatio: Infinity },
    ...(options.rangePlans || {})
  };
  const postingBlockFrontier = Math.max(1, Math.min(16, Math.floor(Number(options.postingBlockFrontier || POSTING_BLOCK_FRONTIER))));

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
      state.rootPromise = fetchGzipArrayBuffer(new URL(state.meta.root, baseUrl)).then(parseDirectoryRoot);
    }
    state.root = await state.rootPromise;
    return state.root;
  }

  async function loadDirectoryPage(state, page) {
    if (!state.pages.has(page.file)) {
      state.pages.set(page.file, fetchGzipArrayBuffer(new URL(directoryPagePath(state, page), baseUrl)).then(buffer => parseDirectoryPage(buffer, { packTable: state.meta.packTable })));
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
    const root = await loadDirectoryRoot(state);
    for (let depth = maxDepth; depth >= baseDepth; depth--) {
      const resolved = await directoryEntryFromRoot(state, root, shardKey(value, depth));
      if (resolved) return resolved;
    }
    return null;
  }

  async function loadCodes() {
    if (codes) return codes;
    if (!codesPromise) codesPromise = fetchGzipArrayBuffer(new URL("codes.bin.gz", baseUrl)).then(parseCodes);
    codes = await codesPromise;
    return codes;
  }

  async function inflateObject(compressed, pointer, label) {
    if (verifyChecksums) await verifyBlockPointer(compressed, pointer, label);
    return inflateGzip(compressed);
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
    if (!facetDirectory || !manifest.facet_dictionaries?.fields?.[field]) return [];
    if (!facetDictionaryCache.has(field)) {
      const promise = (async () => {
        const root = await loadDirectoryRoot(facetDirectory);
        const resolved = await directoryEntryFromRoot(facetDirectory, root, field);
        if (!resolved) return [];
        const packs = manifest.facet_dictionaries.packs || "facets/packs/";
        const buffer = await fetchRange(new URL(`${packs.replace(/\/?$/u, "/")}${resolved.entry.pack}`, baseUrl), resolved.entry.offset, resolved.entry.length);
        const values = parseFacetDictionary(await inflateObject(buffer, resolved.entry, `facet dictionary ${field}`));
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

  function docValueCacheKey(field, index) {
    return `${field}\u0000${index}`;
  }

  async function loadDocValueChunks(requests) {
    if (!docValues || !requests.length) return;
    const pending = [];
    for (const request of requests) {
      const fieldMeta = docValueField(request.field);
      const chunk = fieldMeta?.chunks?.[request.index];
      if (!chunk) continue;
      const key = docValueCacheKey(request.field, request.index);
      if (docValueCache.has(key)) continue;
      let resolve;
      let reject;
      const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
      });
      promise.catch(() => {});
      docValueCache.set(key, promise);
      pending.push({ field: request.field, index: request.index, entry: chunk, resolve, reject });
    }
    await Promise.all(rangeGroups(pending).map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`doc-values/packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const parsed = parseDocValueChunk(await inflateGroupItem(compressed, group.start, item, `doc-value ${item.field}:${item.index}`));
          docValueCache.set(docValueCacheKey(item.field, item.index), parsed);
          item.resolve(parsed);
        }));
      } catch (error) {
        for (const item of group.items) {
          docValueCache.delete(docValueCacheKey(item.field, item.index));
          item.reject(error);
        }
        throw error;
      }
    }));
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
          item.resolve(decodeDocValueSortPage(inflated, { name: item.field }));
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
    if (!docValues || !fields.length || !docs.length) return null;
    const requests = [];
    const seen = new Set();
    for (const field of fields) {
      const fieldMeta = docValueField(field);
      if (!fieldMeta) continue;
      for (const doc of docs) {
        const index = chunkIndexForDoc(fieldMeta, doc);
        if (index < 0) continue;
        const key = docValueCacheKey(field, index);
        if (seen.has(key) || docValueCache.has(key)) continue;
        seen.add(key);
        requests.push({ field, index });
      }
    }
    await loadDocValueChunks(requests);
    return docValueStore;
  }

  async function ensureDocValueChunkIndexes(fields, indexes) {
    if (!docValues || !fields.length || !indexes.length) return null;
    const requests = [];
    for (const field of fields) {
      for (const index of indexes) requests.push({ field, index });
    }
    await loadDocValueChunks(requests);
    return docValueStore;
  }

  function docValue(field, doc) {
    const fieldMeta = docValueField(field);
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

  async function valueStoreForDocs(fields, docs) {
    if (!fields.length) return null;
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
          item.resolve(parseShard(await inflateGroupItem(compressed, group.start, item, `term shard ${item.shard}`), manifest));
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
          item.resolve(parseTypoShard(await inflateGroupItem(compressed, group.start, item, `typo shard ${item.shard}`)));
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

  async function termEntries(terms) {
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
          item.resolve(JSON.parse(textDecoder.decode(new Uint8Array(inflated))));
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
    if (!docPages?.pointers?.file || !context.preferDocPages) return null;
    const unique = [...new Set(indexes)];
    if (!unique.length) return null;
    const pages = [...new Set(unique.map(docPageIndex))].sort((a, b) => a - b);
    const payloadDocs = pages.length * docPageSize();
    const maxOverfetchDocs = Math.max(1, Number(docPages.max_overfetch_docs || 16));
    const maxPayloadDocs = Math.max(docPageSize(), unique.length * maxOverfetchDocs);
    if (payloadDocs > maxPayloadDocs) return null;
    return { pages, pageSize: docPageSize(), payloadDocs, uniqueDocs: unique.length };
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
          item.resolve(decodeDocPagePayload(inflated, item.pageIndex));
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
    return plan ? loadDocPages(indexes, plan) : loadPackedDocs(indexes);
  }

  async function rowsToResults(rows, context = {}) {
    const docs = await loadDocs(rows.map(([index]) => index), context);
    return docs.map((doc, i) => ({ ...doc, score: rows[i][1] }));
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
          item.resolve(decodePostingBytes(await inflateGroupItem(compressed, group.start, item, `posting block ${item.blockIndex}`)));
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
  }

  async function loadExternalPostingBlocks(entry, blockIndexes) {
    await loadPostingBlockBatch(blockIndexes.map(blockIndex => ({ entry, blockIndex })));
    return Promise.all(blockIndexes.map(blockIndex => entry.blockPostings.get(blockIndex)));
  }

  function postingBlockPrefetchIndexes(entry, blockIndex, prefetch) {
    const total = entry.blocks?.length || 0;
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
    for (const [field, selected] of filterPlan.facets) {
      if (!facetCodeMatches(valueForDoc(codeData, field, doc), selected)) return false;
    }
    for (const [field, range] of filterPlan.numbers) {
      const value = valueForDoc(codeData, field, doc);
      if (value == null) return false;
      if (range.min != null && value < range.min) return false;
      if (range.max != null && value > range.max) return false;
    }
    for (const [field, expected] of filterPlan.booleans) {
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
    for (const [field, selected] of filterPlan.facets) {
      if (!facetCodeMatches(valueForDocWithKnown(codeData, field, doc, known), selected)) return false;
    }
    for (const [field, range] of filterPlan.numbers) {
      const value = valueForDocWithKnown(codeData, field, doc, known);
      if (value == null) return false;
      if (range.min != null && value < range.min) return false;
      if (range.max != null && value > range.max) return false;
    }
    for (const [field, expected] of filterPlan.booleans) {
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
      .map(([field, value]) => [field, booleanCode(value)])
      .filter(([, value]) => value != null);
    return { facets, numbers, booleans, active: facets.length > 0 || numbers.length > 0 || booleans.length > 0 };
  }

  function blockMayPass(block, filterPlan) {
    if (!filterPlan?.active) return true;
    for (const [field, selected] of filterPlan.facets) {
      if (!blockFacetMatches(block.filters?.[field], selected)) return false;
    }
    for (const [field, range] of filterPlan.numbers) {
      const summary = block.filters?.[field];
      if (!summary || summary.min == null || summary.max == null) return false;
      if (range.min != null && summary.max < range.min) return false;
      if (range.max != null && summary.min > range.max) return false;
    }
    for (const [field, value] of filterPlan.booleans) {
      const summary = block.filters?.[field];
      if (!summary || !summary.max) return false;
      if (summary.max < value || summary.min > value) return false;
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

  async function runDocValueBrowse({ page, size, filters, sortPlan, hasFilters }) {
    const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
    const field = sortPlan?.field || null;
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
        : await valueStoreForDocs(filterFields, rows.map(row => row.doc));
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

  async function runDocValueChunkBrowse({ page, size, filters, hasFilters }) {
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

  function advanceCursor(cursor, filterPlan) {
    while (cursor.blockIndex < cursor.entry.blocks.length && !blockMayPass(cursor.entry.blocks[cursor.blockIndex], filterPlan)) {
      cursor.skippedBlocks++;
      cursor.blockIndex++;
    }
    return cursor.blockIndex < cursor.entry.blocks.length;
  }

  function remainingPotential(cursors, mask = 0, filterPlan = null) {
    let potential = 0;
    for (const cursor of cursors) {
      if (!advanceCursor(cursor, filterPlan) || bitIsSet(mask, cursor.termIndex)) continue;
      potential += cursor.entry.blocks[cursor.blockIndex].maxImpact;
    }
    return potential;
  }

  function stableTopK(scores, hits, masks, cursors, minShouldMatch, k, filterPlan) {
    const eligible = collectEligibleScores(scores, hits, minShouldMatch);
    if (eligible.length < k) return null;

    const top = eligible.slice(0, k);
    const topDocs = new Set(top.map(([doc]) => doc));
    const threshold = top[top.length - 1][1];
    let maxOutsidePotential = remainingPotential(cursors, 0, filterPlan);

    for (const [doc, score] of scores) {
      if (topDocs.has(doc)) continue;
      const potential = score + remainingPotential(cursors, masks.get(doc) || 0, filterPlan);
      if (potential > maxOutsidePotential) maxOutsidePotential = potential;
      if (maxOutsidePotential >= threshold) return null;
    }

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
      for (const blockIndex of postingBlockPrefetchIndexes(cursor.entry, cursor.blockIndex, prefetch)) {
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

  async function runSkippedSearch({ q, page, size, filters, sort, baseTerms, terms, rerank = true }) {
    const offset = (page - 1) * size;
    const k = offset + size;
    const candidateK = candidateLimitFor(baseTerms, k, rerank);
    const sortPlan = makeSortPlan(sort);
    if (sortPlan || !baseTerms.length || terms.length > SKIP_MAX_TERMS || k > 100) {
      return runFullSearch({ q, page, size, filters, sort, baseTerms, terms, rerank });
    }

    await ensureFacetDictionaries(filters);
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    const blockFilterPlan = hasFilters ? makeBlockFilterPlan(filters) : null;
    const docFilterPlan = hasFilters ? makeDocFilterPlan(filters) : null;
    const filterFields = filterPlanFields(docFilterPlan);
    const fallbackCodeData = hasFilters && !docValues ? await loadCodes() : null;
    const entries = await termEntries(terms);
    const baseSet = new Set(baseTerms);
    const cursors = entries.map((item, termIndex) => ({
      ...item,
      termIndex,
      isBase: baseSet.has(item.term),
      blockIndex: 0,
      skippedBlocks: 0
    }));
    if (!cursors.length) {
      return { total: 0, page, size, results: [], approximate: false, stats: { exact: true, blocksDecoded: 0, postingsDecoded: 0, postingsAccepted: 0, skippedBlocks: 0, terms: terms.length, shards: 0 } };
    }

    const scores = new Map();
    const hits = new Map();
    const masks = new Map();
    const minShouldMatch = minShouldMatchFor(baseTerms);
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

    while (true) {
      const active = cursors.filter(cursor => advanceCursor(cursor, blockFilterPlan));
      if (!active.length) {
        exhausted = true;
        break;
      }

      stable = stableTopK(scores, hits, masks, cursors, minShouldMatch, candidateK, blockFilterPlan);
      if (stable) break;

      active.sort((a, b) => cursorImpact(b) - cursorImpact(a));
      const frontier = active.slice(0, postingBlockFrontier);
      frontierBatches++;
      frontierBlocks += frontier.length;
      frontierMax = Math.max(frontierMax, frontier.length);
      const decoded = await decodeCursorFrontier(frontier);
      frontierFetchedBlocks += decoded.fetchedBlocks;
      frontierFetchGroups += decoded.fetchGroups;
      frontierWantedBlocks += decoded.wantedBlocks;
      for (const { cursor, rows } of decoded.blocks) {
        cursor.blockIndex++;
        const codeData = hasFilters && docValues
          ? await valueStoreForDocs(filterFields, postingDocs(rows))
          : fallbackCodeData;
        blocksDecoded++;
        postingsDecoded += rows.length / 2;
        postingsAccepted += applyBlockRows(cursor, rows, codeData, docFilterPlan, scores, hits, masks);
        stable = stableTopK(scores, hits, masks, cursors, minShouldMatch, candidateK, blockFilterPlan);
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
    const resultContext = { hasTextTerms: true, preferDocPages: false };
    const results = await rowsToResults(rows, resultContext);
    return {
      total: exhausted ? ranked.length : Math.max(ranked.length, k),
      page,
      size,
      approximate: !exhausted,
      results,
      stats: {
        exact: exhausted,
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
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
        ...reranked.stats
      }
    };
  }

  async function runFullSearch({ q, page, size, filters, sort, baseTerms, terms, rerank = true }) {
    const offset = (page - 1) * size;
    const sortPlan = makeSortPlan(sort);
    await ensureFacetDictionaries(filters);
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;
    const entries = await termEntries(terms);
    const scores = new Map();
    const hits = new Map();
    const baseSet = new Set(baseTerms);
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
      codeData = await valueStoreForDocs(filterFields, [...docs]);
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
    const resultContext = { hasTextTerms: !!baseTerms.length, preferDocPages: !!sortPlan };
    const results = await rowsToResults(rows, resultContext);
    return {
      total: ranked.length,
      page,
      size,
      results,
      approximate: false,
      stats: {
        exact: true,
        terms: terms.length,
        shards: new Set(entries.map(item => item.shardName)).size,
        postings: entries.reduce((sum, item) => sum + item.entry.count, 0),
        blocksDecoded: entries.reduce((sum, item) => sum + (item.entry.blocks?.length || 0), 0),
        postingsDecoded: entries.reduce((sum, item) => sum + item.entry.count, 0),
        postingsAccepted: ranked.length,
        skippedBlocks: 0,
        docPayloadLane: resultContext.docPayloadLane,
        docPayloadPages: resultContext.docPayloadPages,
        docPayloadOverfetchDocs: resultContext.docPayloadOverfetchDocs,
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
    const deleteKeys = [...typoDeleteKeys(token, {
      minTermLength: meta.min_term_length || 4,
      maxEdits: meta.max_edits || 2
    }, maxEdits)];
    const byShard = new Map();
    for (const key of deleteKeys) {
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
    debug.candidates += verified.length;
    return verified
      .sort((a, b) => b.score - a.score || a.distance - b.distance || b.df - a.df || a.term.localeCompare(b.term))
      .slice(0, 64);
  }

  async function correctedTypoQuery(baseTerms, analyzedTerms) {
    if (!baseTerms.length || baseTerms.length > 8) return null;
    const presentTerms = new Map((await termEntries(baseTerms)).map(item => [item.term, item.entry.df || 0]));
    const hasMissingTerms = baseTerms.some(term => !presentTerms.has(term));
    const plans = [];
    const debug = { shards: new Set(), candidates: 0 };

    for (let index = 0; index < analyzedTerms.length; index++) {
      const item = analyzedTerms[index];
      if (presentTerms.has(item.term) && hasMissingTerms) continue;
      const candidates = await typoCandidatesForToken(item.raw, debug);
      for (const candidate of candidates.filter(item => item.score >= 0.5).slice(0, 4)) {
        const corrected = baseTerms.slice();
        corrected[index] = candidate.term;
        if (corrected[index] === item.term) continue;
        plans.push({
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
        });
      }
    }

    if (!plans.length) return null;
    plans.sort((a, b) => b.score - a.score || a.q.localeCompare(b.q));
    return {
      plans: plans.slice(0, 12),
      stats: { typoCandidateTerms: debug.candidates, typoShardLookups: debug.shards.size }
    };
  }

  async function maybeTypoFallback(params, response, baseTerms, analyzedTerms) {
    if (params.page !== 1 || response.total > 0) return response;
    const correction = await correctedTypoQuery(baseTerms, analyzedTerms);
    if (!correction) {
      return { ...response, stats: { ...(response.stats || {}), typoAttempted: true, typoApplied: false } };
    }

    let best = null;
    for (const plan of correction.plans) {
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
      return { ...response, stats: { ...(response.stats || {}), typoAttempted: true, typoApplied: false, ...correction.stats } };
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
        ...correction.stats
      }
    };
  }

  async function search(params = {}) {
    const q = String(params.q || "").trim();
    const page = Math.max(1, Number(params.page || 1));
    const size = Math.max(1, Math.min(100, Number(params.size || 10)));
    const offset = (page - 1) * size;
    const filters = params.filters || {};
    const sort = params.sort || null;
    const sortPlan = makeSortPlan(sort);
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length || Object.keys(filters.booleans || {}).length;

    if (!q) {
      if (!sortPlan && !hasFilters) {
        const docs = manifest.initial_results.slice(offset, offset + size);
        return { total: manifest.total, results: docs, page, size };
      }
      await ensureFacetDictionaries(filters);
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
    const searchFn = params.exact ? runFullSearch : runSkippedSearch;
    const response = await searchFn({ q, page, size, filters, sort, baseTerms, terms: queryTerms(q), rerank: params.rerank });
    return maybeTypoFallback({ q, page, size, filters, sort, rerank: params.rerank }, response, baseTerms, analyzedTerms);
  }

  return {
    manifest,
    search
  };
}

export default createSearch;
