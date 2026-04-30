import { queryTerms, tokenize } from "./analyzer.js";
import { decodePostings, parseCodes, parseRangeDirectory, parseShard } from "./codec.js";
import { groupRanges, shardFor } from "./shards.js";

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
  const docCache = new Map();
  let rangeDirectory = null;
  let rangeDirectoryPromise = null;
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

  async function search(params = {}) {
    const q = String(params.q || "").trim();
    const page = Math.max(1, Number(params.page || 1));
    const size = Math.max(1, Math.min(100, Number(params.size || 10)));
    const offset = (page - 1) * size;
    const filters = params.filters || {};
    const hasFilters = Object.keys(filters.facets || {}).length || Object.keys(filters.numbers || {}).length;

    if (!q) {
      const docs = manifest.initial_results.slice(offset, offset + size);
      return { total: manifest.total, results: docs, page, size };
    }

    const baseTerms = tokenize(q);
    const terms = queryTerms(q);
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
      stats: { terms: terms.length, shards: new Set(entries.map(item => item.shard)).size, postings: entries.reduce((sum, item) => sum + item.entry.count, 0) }
    };
  }

  return {
    manifest,
    search
  };
}

export default createSearch;
