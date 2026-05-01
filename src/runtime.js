import { analyzeTerms, queryTerms, tokenize } from "./analyzer.js";
import { decodePostings, parseCodes, parseRangeDirectory, parseShard } from "./codec.js";
import { groupRanges, shardFor } from "./shards.js";
import {
  boundedDamerauLevenshtein,
  parseTypoShard,
  typoCandidateScore,
  typoCandidatesForDeleteKey,
  typoDeleteKeys,
  typoMaxEditsFor,
  typoShardFor
} from "./typo_runtime.js";

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
  const availableShards = new Set(manifest.shards || []);
  const shardCache = new Map();
  const typoShardCache = new Map();
  const docCache = new Map();
  let rangeDirectory = null;
  let rangeDirectoryPromise = null;
  let typoManifest = null;
  let typoManifestPromise = null;
  let typoShardRanges = new Map();
  let availableTypoShards = new Set();
  let codes = null;
  let codesPromise = null;

  async function loadRanges() {
    if (rangeDirectory) return rangeDirectory;
    if (!rangeDirectoryPromise) {
      rangeDirectoryPromise = fetchGzipArrayBuffer(new URL("terms/ranges.bin.gz", baseUrl)).then(buffer => parseRangeDirectory(buffer, manifest));
    }
    rangeDirectory = await rangeDirectoryPromise;
    return rangeDirectory;
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
    availableTypoShards = new Set(typoManifest?.shards || []);
    typoShardRanges = new Map();
    const ranges = typoManifest?.shard_ranges || [];
    const packs = typoManifest?.packs || [];
    for (let i = 0; i < (typoManifest?.shards?.length || 0); i++) {
      const range = ranges[i];
      const pack = packs[range?.[0]];
      if (!range || !pack) continue;
      typoShardRanges.set(typoManifest.shards[i], {
        pack: pack.file,
        offset: range[1],
        length: range[2]
      });
    }
    return typoManifest;
  }

  async function loadShards(shards) {
    const ranges = await loadRanges();
    const wanted = [];
    const pending = [];
    for (const shard of new Set(shards)) {
      if (!availableShards.has(shard)) continue;
      wanted.push(shard);
      if (shardCache.has(shard)) continue;
      const entry = ranges.get(shard);
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
    for (const shard of new Set(shards)) {
      if (!availableTypoShards.has(shard)) continue;
      wanted.push(shard);
      if (typoShardCache.has(shard)) continue;
      const entry = typoShardRanges.get(shard);
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
      const shard = shardFor(term, manifest, availableShards);
      if (!byShard.has(shard)) byShard.set(shard, []);
      byShard.get(shard).push(term);
    }
    const loaded = await loadShards([...byShard.keys()]);
    const out = [];
    for (const [shard, shardTerms] of byShard) {
      const data = loaded.get(shard);
      if (!data) continue;
      for (const term of shardTerms) {
        const entry = data.terms.get(term);
        if (entry) out.push({ term, shard: data, entry });
      }
    }
    return out;
  }

  async function loadDoc(index) {
    const chunk = Math.floor(index / manifest.doc_chunk_size);
    const local = index - chunk * manifest.doc_chunk_size;
    if (!docCache.has(chunk)) {
      const file = `docs/${String(chunk).padStart(4, "0")}.json`;
      docCache.set(chunk, fetch(new URL(file, baseUrl)).then(r => r.json()));
    }
    return (await docCache.get(chunk))[local];
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

  async function runExactSearch({ q, page, size, filters, baseTerms, terms }) {
    const offset = (page - 1) * size;
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length;
    const entries = await termEntries(terms);
    const scores = new Map();
    const hits = new Map();
    const baseSet = new Set(baseTerms);
    const codeData = hasFilters ? await loadCodes() : null;

    for (const { term, shard, entry } of entries) {
      const postings = decodePostings(shard, entry);
      const isBase = baseSet.has(term);
      for (let i = 0; i < postings.length; i += 2) {
        const doc = postings[i];
        if (codeData && !passesFilters(doc, codeData, filters)) continue;
        scores.set(doc, (scores.get(doc) || 0) + postings[i + 1]);
        if (isBase) hits.set(doc, (hits.get(doc) || 0) + 1);
      }
    }

    const minShouldMatch = baseTerms.length <= 4 ? baseTerms.length : baseTerms.length - 1;
    const ranked = [...scores.entries()]
      .filter(([doc]) => (hits.get(doc) || 0) >= Math.max(1, minShouldMatch))
      .sort((a, b) => b[1] - a[1] || a[0] - b[0]);
    const rows = ranked.slice(offset, offset + size);
    return {
      total: ranked.length,
      page,
      size,
      results: await Promise.all(rows.map(async ([index, score]) => ({ ...(await loadDoc(index)), score }))),
      stats: { exact: true, terms: terms.length, shards: new Set(entries.map(item => item.shard)).size, postings: entries.reduce((sum, item) => sum + item.entry.count, 0) }
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
      const shard = typoShardFor(key, meta, availableTypoShards);
      if (!byShard.has(shard)) byShard.set(shard, []);
      byShard.get(shard).push(key);
    }

    const candidates = new Map();
    const loaded = await loadTypoShards([...byShard.keys()]);
    for (const [shard, keys] of byShard) {
      debug.shards.add(shard);
      const data = loaded.get(shard);
      if (!data) continue;
      for (const key of keys) {
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
      const corrected = await runExactSearch({
        ...params,
        q: plan.q,
        baseTerms: plan.baseTerms,
        terms: queryTerms(plan.q)
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
    const response = await runExactSearch({ q, page, size, filters, baseTerms, terms: queryTerms(q) });
    return maybeTypoFallback({ q, page, size, filters }, response, baseTerms, analyzedTerms);
  }

  return {
    manifest,
    search
  };
}

export default createSearch;
