import { analyzeTerms, expandedTermsFromBaseTerms, proximityTerm, queryTerms, tokenize } from "./analyzer.js";
import { decodePostingBlock, decodePostingBytes, decodePostings, parseCodes, parseShard } from "./codec.js";
import { findDirectoryPage, parseDirectoryPage, parseDirectoryRoot } from "./directory.js";
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
const EXTERNAL_POSTING_BLOCK_PREFETCH = 4;
const DOC_INDEX_KEY_WIDTH = 8;
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
  const values = manifest.facets?.[field] || [];
  const out = new Set();
  values.forEach((item, idx) => {
    if (selected.has(item.value) || selected.has(item.label)) out.add(idx);
  });
  return out;
}

export async function createSearch(options = {}) {
  const baseUrl = options.baseUrl || "./rangefind/";
  const manifest = await fetch(new URL("manifest.json", baseUrl)).then(r => r.json());
  const termDirectory = createDirectoryState(manifest.directory, "terms");
  const docDirectory = manifest.docs?.directory ? createDirectoryState(manifest.docs.directory, "docs") : null;
  const shardCache = new Map();
  const typoShardCache = new Map();
  const docCache = new Map();
  const packedDocCache = new Map();
  let typoManifest = null;
  let typoManifestPromise = null;
  let typoDirectory = null;
  let codes = null;
  let codesPromise = null;

  function createDirectoryState(meta, fallbackDir) {
    const directory = meta || {};
    return {
      meta: {
        root: directory.root || `${fallbackDir}/directory-root.bin.gz`,
        pages: directory.pages || `${fallbackDir}/directory-pages/`
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
      state.pages.set(page.file, fetchGzipArrayBuffer(new URL(directoryPagePath(state, page), baseUrl)).then(parseDirectoryPage));
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

  function docIndexKey(index) {
    return String(index).padStart(DOC_INDEX_KEY_WIDTH, "0");
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

  async function loadTypoManifest() {
    if (typoManifest !== null) return typoManifest;
    if (!typoManifestPromise) {
      typoManifestPromise = fetch(new URL("typo/manifest.json", baseUrl))
        .then(response => response.ok ? response.json() : false)
        .catch(() => false);
    }
    typoManifest = await typoManifestPromise;
    if (typoManifest) typoDirectory = createDirectoryState(typoManifest.directory, "typo");
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
      shardCache.set(shard, promise);
      pending.push({ shard, entry, resolve, reject });
    }

    await Promise.all(groupRanges(pending).map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`terms/packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const start = item.entry.offset - group.start;
          const end = start + item.entry.length;
          item.resolve(parseShard(await inflateGzip(compressed.slice(start, end)), manifest));
        }));
      } catch (error) {
        for (const item of group.items) {
          shardCache.delete(item.shard);
          item.reject(error);
        }
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
      typoShardCache.set(shard, promise);
      pending.push({ shard, entry, resolve, reject });
    }

    await Promise.all(groupRanges(pending).map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`typo/packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const start = item.entry.offset - group.start;
          const end = start + item.entry.length;
          item.resolve(parseTypoShard(await inflateGzip(compressed.slice(start, end))));
        }));
      } catch (error) {
        for (const item of group.items) {
          typoShardCache.delete(item.shard);
          item.reject(error);
        }
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

  async function loadChunkDoc(index) {
    const chunk = Math.floor(index / manifest.doc_chunk_size);
    const local = index - chunk * manifest.doc_chunk_size;
    if (!docCache.has(chunk)) {
      const file = `docs/${String(chunk).padStart(4, "0")}.json`;
      docCache.set(chunk, fetch(new URL(file, baseUrl)).then(r => r.json()));
    }
    return (await docCache.get(chunk))[local];
  }

  async function resolvePackedDoc(index) {
    if (!docDirectory) return null;
    const key = docIndexKey(index);
    const root = await loadDirectoryRoot(docDirectory);
    const resolved = await directoryEntryFromRoot(docDirectory, root, key);
    return resolved ? { index, key, entry: resolved.entry } : null;
  }

  async function loadPackedDocs(indexes) {
    const wanted = [];
    const pending = [];
    const unique = [...new Set(indexes)];
    for (const index of unique) {
      wanted.push(index);
      if (packedDocCache.has(index)) continue;
      let resolveDoc;
      let rejectDoc;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolveDoc = resolvePromise;
        rejectDoc = rejectPromise;
      });
      packedDocCache.set(index, promise);
      const resolved = await resolvePackedDoc(index);
      if (resolved) pending.push({ ...resolved, resolve: resolveDoc, reject: rejectDoc });
      else resolveDoc(loadChunkDoc(index));
    }

    await Promise.all(groupRanges(pending).map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`docs/packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const start = item.entry.offset - group.start;
          const end = start + item.entry.length;
          const inflated = await inflateGzip(compressed.slice(start, end));
          item.resolve(JSON.parse(textDecoder.decode(new Uint8Array(inflated))));
        }));
      } catch (error) {
        for (const item of group.items) {
          packedDocCache.delete(item.index);
          item.reject(error);
        }
      }
    }));

    return Promise.all(wanted.map(index => packedDocCache.get(index)));
  }

  async function loadDocs(indexes) {
    if (docDirectory) return loadPackedDocs(indexes);
    return Promise.all(indexes.map(loadChunkDoc));
  }

  async function rowsToResults(rows) {
    const docs = await loadDocs(rows.map(([index]) => index));
    return docs.map((doc, i) => ({ ...doc, score: rows[i][1] }));
  }

  async function loadExternalPostingBlocks(entry, blockIndexes) {
    if (!entry.blockPostings) entry.blockPostings = new Map();
    const pending = [];
    const wanted = [];
    for (const blockIndex of blockIndexes) {
      wanted.push(blockIndex);
      if (entry.blockPostings.has(blockIndex)) continue;
      const block = entry.blocks?.[blockIndex];
      if (!block?.range) {
        entry.blockPostings.set(blockIndex, Promise.resolve(new Int32Array(0)));
        continue;
      }
      let resolveBlock;
      let rejectBlock;
      const promise = new Promise((resolvePromise, rejectPromise) => {
        resolveBlock = resolvePromise;
        rejectBlock = rejectPromise;
      });
      entry.blockPostings.set(blockIndex, promise);
      pending.push({ blockIndex, entry: block.range, resolve: resolveBlock, reject: rejectBlock });
    }

    await Promise.all(groupRanges(pending).map(async (group) => {
      try {
        const compressed = await fetchRange(new URL(`terms/block-packs/${group.pack}`, baseUrl), group.start, group.end - group.start);
        await Promise.all(group.items.map(async (item) => {
          const start = item.entry.offset - group.start;
          const end = start + item.entry.length;
          item.resolve(decodePostingBytes(await inflateGzip(compressed.slice(start, end))));
        }));
      } catch (error) {
        for (const item of group.items) {
          entry.blockPostings.delete(item.blockIndex);
          item.reject(error);
        }
      }
    }));

    return Promise.all(wanted.map(blockIndex => entry.blockPostings.get(blockIndex)));
  }

  async function decodeEntryBlock(shard, entry, blockIndex) {
    if (!entry.external) return decodePostingBlock(shard, entry, blockIndex);
    const prefetchLimit = entry.blocks.length <= EXTERNAL_POSTING_BLOCK_PREFETCH * 2
      ? entry.blocks.length
      : blockIndex + EXTERNAL_POSTING_BLOCK_PREFETCH;
    const blockIndexes = [];
    for (let i = blockIndex; i < Math.min(entry.blocks.length, prefetchLimit); i++) {
      blockIndexes.push(i);
    }
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

  function passesFilters(doc, codeData, filters) {
    for (const [field, values] of Object.entries(filters.facets || {})) {
      const selected = selectedFacetCodes(manifest, field, new Set(values));
      if (selected && !selected.has(codeData[field]?.[doc])) return false;
    }
    for (const [field, range] of Object.entries(filters.numbers || {})) {
      const value = codeData[field]?.[doc] || 0;
      if (range.min != null && value < range.min) return false;
      if (range.max != null && value > range.max) return false;
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
      .filter(([, range]) => range?.min != null || range?.max != null);
    return { facets, numbers, active: facets.length > 0 || numbers.length > 0 };
  }

  function blockMayPass(block, filterPlan) {
    if (!filterPlan?.active) return true;
    for (const [field, selected] of filterPlan.facets) {
      if (!blockFacetMatches(block.filters?.[field], selected)) return false;
    }
    for (const [field, range] of filterPlan.numbers) {
      const summary = block.filters?.[field];
      if (!summary || !summary.max) return false;
      if (range.min != null && summary.max < range.min) return false;
      if (range.max != null && summary.min > range.max) return false;
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

  function applyBlockRows(cursor, rows, codeData, filters, scores, hits, masks) {
    let accepted = 0;
    const bit = cursor.termIndex < SKIP_MAX_TERMS ? 2 ** cursor.termIndex : 0;
    for (let i = 0; i < rows.length; i += 2) {
      const doc = rows[i];
      if (codeData && !passesFilters(doc, codeData, filters)) continue;
      scores.set(doc, (scores.get(doc) || 0) + rows[i + 1]);
      if (bit) masks.set(doc, (masks.get(doc) || 0) | bit);
      if (cursor.isBase) hits.set(doc, (hits.get(doc) || 0) + 1);
      accepted++;
    }
    return accepted;
  }

  async function runSkippedSearch({ q, page, size, filters, baseTerms, terms, rerank = true }) {
    const offset = (page - 1) * size;
    const k = offset + size;
    const candidateK = rerank === false ? k : Math.max(RERANK_CANDIDATES, k);
    if (baseTerms.length < 2 || terms.length > SKIP_MAX_TERMS || k > 100) {
      return runFullSearch({ q, page, size, filters, baseTerms, terms, rerank });
    }

    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length;
    const filterPlan = hasFilters ? makeBlockFilterPlan(filters) : null;
    const codeData = hasFilters ? await loadCodes() : null;
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

    while (true) {
      const active = cursors.filter(cursor => advanceCursor(cursor, filterPlan));
      if (!active.length) {
        exhausted = true;
        break;
      }

      stable = stableTopK(scores, hits, masks, cursors, minShouldMatch, candidateK, filterPlan);
      if (stable) break;

      active.sort((a, b) => b.entry.blocks[b.blockIndex].maxImpact - a.entry.blocks[a.blockIndex].maxImpact);
      const cursor = active[0];
      const rows = await decodeEntryBlock(cursor.shard, cursor.entry, cursor.blockIndex);
      cursor.blockIndex++;
      blocksDecoded++;
      postingsDecoded += rows.length / 2;
      postingsAccepted += applyBlockRows(cursor, rows, codeData, filters, scores, hits, masks);
    }

    let ranked = exhausted
      ? collectEligibleScores(scores, hits, minShouldMatch)
      : stable || collectEligibleScores(scores, hits, minShouldMatch).slice(0, k);
    const reranked = rerank === false
      ? { ranked, stats: { rerankCandidates: 0, dependencyFeatures: 0, dependencyTermsMatched: 0, dependencyPostingsScanned: 0, dependencyCandidateMatches: 0 } }
      : await rerankWithDependencies(ranked, baseTerms, candidateK);
    ranked = reranked.ranked;
    const rows = ranked.slice(offset, offset + size);
    return {
      total: exhausted ? ranked.length : Math.max(ranked.length, k),
      page,
      size,
      approximate: !exhausted,
      results: await rowsToResults(rows),
      stats: {
        exact: exhausted,
        blocksDecoded,
        postingsDecoded,
        postingsAccepted,
        skippedBlocks: cursors.reduce((sum, cursor) => sum + cursor.skippedBlocks, 0),
        terms: terms.length,
        shards: new Set(entries.map(item => item.shardName)).size,
        ...reranked.stats
      }
    };
  }

  async function runFullSearch({ q, page, size, filters, baseTerms, terms, rerank = true }) {
    const offset = (page - 1) * size;
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length;
    const entries = await termEntries(terms);
    const scores = new Map();
    const hits = new Map();
    const baseSet = new Set(baseTerms);
    const codeData = hasFilters ? await loadCodes() : null;

    for (const { term, shard, entry } of entries) {
      const postings = await decodeEntryPostings(shard, entry);
      const isBase = baseSet.has(term);
      for (let i = 0; i < postings.length; i += 2) {
        const doc = postings[i];
        if (codeData && !passesFilters(doc, codeData, filters)) continue;
        scores.set(doc, (scores.get(doc) || 0) + postings[i + 1]);
        if (isBase) hits.set(doc, (hits.get(doc) || 0) + 1);
      }
    }

    let ranked = collectEligibleScores(scores, hits, minShouldMatchFor(baseTerms));
    const reranked = rerank === false
      ? { ranked, stats: { rerankCandidates: 0, dependencyFeatures: 0, dependencyTermsMatched: 0, dependencyPostingsScanned: 0, dependencyCandidateMatches: 0 } }
      : await rerankWithDependencies(ranked, baseTerms, Math.max(RERANK_CANDIDATES, offset + size));
    ranked = reranked.ranked;
    const rows = ranked.slice(offset, offset + size);
    return {
      total: ranked.length,
      page,
      size,
      results: await rowsToResults(rows),
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

    if (!q) {
      const docs = manifest.initial_results.slice(offset, offset + size);
      return { total: manifest.total, results: docs, page, size };
    }

    const analyzedTerms = analyzeTerms(q);
    const baseTerms = analyzedTerms.map(item => item.term);
    const searchFn = params.exact ? runFullSearch : runSkippedSearch;
    const response = await searchFn({ q, page, size, filters, baseTerms, terms: queryTerms(q), rerank: params.rerank });
    return maybeTypoFallback({ q, page, size, filters, rerank: params.rerank }, response, baseTerms, analyzedTerms);
  }

  return {
    manifest,
    search
  };
}

export default createSearch;
