import { queryTerms, tokenize } from "./analyzer.js";
import { CODE_MAGIC, TERM_RANGE_MAGIC, TERM_SHARD_MAGIC, readFixedInt, readVarint } from "./binary.js";

const RANGE_MERGE_GAP_BYTES = 8 * 1024;
const decoder = new TextDecoder();

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

function assertMagic(bytes, magic, message) {
  for (let i = 0; i < magic.length; i++) {
    if (bytes[i] !== magic[i]) throw new Error(message);
  }
}

function readUtf8(bytes, state) {
  const len = readVarint(bytes, state);
  const start = state.pos;
  state.pos += len;
  return decoder.decode(bytes.subarray(start, state.pos));
}

function parseShard(buffer, manifest) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, TERM_SHARD_MAGIC, "Unsupported Rangefind term shard");
  const state = { pos: TERM_SHARD_MAGIC.length };
  const termCount = readVarint(bytes, state);
  const terms = new Map();
  for (let i = 0; i < termCount; i++) {
    const term = readUtf8(bytes, state);
    const entry = {
      df: readVarint(bytes, state),
      count: readVarint(bytes, state),
      offset: readVarint(bytes, state),
      byteLength: readVarint(bytes, state),
      blockSize: readVarint(bytes, state),
      blocks: null,
      postings: null
    };
    const blockCount = readVarint(bytes, state);
    entry.blocks = new Array(blockCount);
    for (let j = 0; j < blockCount; j++) {
      const block = {
        offset: readVarint(bytes, state),
        maxImpact: readVarint(bytes, state),
        filters: {}
      };
      for (const filter of manifest.block_filters || []) {
        if (filter.kind === "facet") {
          const words = new Array(filter.words);
          for (let w = 0; w < filter.words; w++) words[w] = readVarint(bytes, state);
          block.filters[filter.name] = { words };
        } else {
          block.filters[filter.name] = {
            min: readVarint(bytes, state),
            max: readVarint(bytes, state)
          };
        }
      }
      entry.blocks[j] = block;
    }
    terms.set(term, entry);
  }
  return { bytes, dataStart: state.pos, terms };
}

function parseRangeDirectory(buffer, manifest) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, TERM_RANGE_MAGIC, "Unsupported Rangefind range directory");
  const state = { pos: TERM_RANGE_MAGIC.length };
  const count = readVarint(bytes, state);
  const ranges = new Map();
  for (let i = 0; i < count && i < manifest.shards.length; i++) {
    const pack = `${String(readVarint(bytes, state)).padStart(4, "0")}.bin`;
    ranges.set(manifest.shards[i], {
      pack,
      offset: readVarint(bytes, state),
      length: readVarint(bytes, state)
    });
  }
  return ranges;
}

function parseCodes(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, CODE_MAGIC, "Unsupported Rangefind code table");
  const state = { pos: CODE_MAGIC.length };
  const total = readVarint(bytes, state);
  const fieldCount = readVarint(bytes, state);
  const fields = [];
  for (let i = 0; i < fieldCount; i++) {
    fields.push({ name: readUtf8(bytes, state), width: bytes[state.pos++] });
  }
  const out = {};
  for (const field of fields) {
    const values = new Array(total);
    for (let i = 0; i < total; i++) values[i] = readFixedInt(bytes, state.pos + i * field.width, field.width);
    state.pos += total * field.width;
    out[field.name] = values;
  }
  return out;
}

function shardKey(term, depth) {
  return String(term || "").slice(0, depth).padEnd(depth, "_");
}

function shardFor(term, manifest, availableShards) {
  const maxDepth = manifest.stats?.max_shard_depth || 5;
  const baseDepth = manifest.stats?.base_shard_depth || 3;
  for (let depth = maxDepth; depth >= baseDepth; depth--) {
    const key = shardKey(term, depth);
    if (availableShards.has(key)) return key;
  }
  return shardKey(term, baseDepth);
}

function groupRanges(items) {
  const byPack = new Map();
  for (const item of items) {
    if (!byPack.has(item.entry.pack)) byPack.set(item.entry.pack, []);
    byPack.get(item.entry.pack).push(item);
  }
  const groups = [];
  for (const [pack, packItems] of byPack) {
    const sorted = packItems
      .map(item => ({ ...item, start: item.entry.offset, end: item.entry.offset + item.entry.length }))
      .sort((a, b) => a.start - b.start);
    let current = null;
    for (const item of sorted) {
      if (!current || item.start > current.end + RANGE_MERGE_GAP_BYTES) {
        current = { pack, start: item.start, end: item.end, items: [item] };
        groups.push(current);
      } else {
        current.items.push(item);
        current.end = Math.max(current.end, item.end);
      }
    }
  }
  return groups;
}

function decodePostings(shard, entry) {
  if (entry.postings) return entry.postings;
  const state = { pos: shard.dataStart + entry.offset };
  const out = new Int32Array(entry.count * 2);
  for (let i = 0; i < entry.count; i++) {
    out[i * 2] = readVarint(shard.bytes, state);
    out[i * 2 + 1] = readVarint(shard.bytes, state);
  }
  entry.postings = out;
  return out;
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
