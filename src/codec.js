import {
  CODE_MAGIC,
  TERM_RANGE_MAGIC,
  TERM_SHARD_MAGIC,
  fixedWidth,
  pushVarint,
  readFixedInt,
  readVarint,
  writeFixedInt
} from "./binary.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function assertMagic(bytes, magic, message) {
  for (let i = 0; i < magic.length; i++) {
    if (bytes[i] !== magic[i]) throw new Error(message);
  }
}

export function pushUtf8(out, value) {
  const bytes = textEncoder.encode(String(value || ""));
  pushVarint(out, bytes.length);
  for (const byte of bytes) out.push(byte);
}

export function readUtf8(bytes, state) {
  const len = readVarint(bytes, state);
  const start = state.pos;
  state.pos += len;
  return textDecoder.decode(bytes.subarray(start, state.pos));
}

export function buildBlockFilters(config, dicts) {
  return [
    ...config.facets.map(facet => ({
      name: facet.name,
      kind: "facet",
      words: Math.max(1, Math.ceil((dicts[facet.name]?.values?.length || 1) / 32))
    })),
    ...config.numbers.map(number => ({ name: number.name, kind: "number" }))
  ];
}

function addBit(words, value) {
  if (value < 0) return;
  const word = Math.floor(value / 32);
  const bit = value % 32;
  words[word] |= 2 ** bit;
}

function emptySummary(filters) {
  return filters.map(filter => filter.kind === "facet" ? { words: new Array(filter.words).fill(0) } : { min: 0, max: 0 });
}

function updateSummary(summary, filters, codes, doc) {
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    const value = codes[filter.name]?.[doc] || 0;
    if (filter.kind === "facet") addBit(summary[i].words, value);
    else if (value) {
      summary[i].min = summary[i].min ? Math.min(summary[i].min, value) : value;
      summary[i].max = Math.max(summary[i].max, value);
    }
  }
}

function encodePostings(rows, total, codes, filters, config) {
  const df = rows.length;
  const idf = Math.log(1 + (total - df + 0.5) / (df + 0.5));
  const encoded = rows
    .map(([doc, score]) => [doc, Math.max(1, Math.round(score * idf / 10))])
    .sort((a, b) => b[1] - a[1] || a[0] - b[0]);
  const bytes = [];
  const blocks = [];
  for (let i = 0; i < encoded.length; i++) {
    const [doc, impact] = encoded[i];
    if (i % config.postingBlockSize === 0) {
      blocks.push({ offset: bytes.length, maxImpact: impact, filters: emptySummary(filters) });
    }
    updateSummary(blocks[blocks.length - 1].filters, filters, codes, doc);
    pushVarint(bytes, doc);
    pushVarint(bytes, impact);
  }
  return { df, count: encoded.length, bytes: Uint8Array.from(bytes), blocks };
}

export function buildTermShard(entries, total, codes, filters, config) {
  const header = [...TERM_SHARD_MAGIC];
  const postingChunks = [];
  const directory = [];
  let postingOffset = 0;

  for (const [term, rows] of entries.sort((a, b) => a[0].localeCompare(b[0]))) {
    const postings = encodePostings(rows, total, codes, filters, config);
    directory.push({ term, postings, offset: postingOffset });
    postingChunks.push(postings.bytes);
    postingOffset += postings.bytes.length;
  }

  pushVarint(header, directory.length);
  for (const entry of directory) {
    pushUtf8(header, entry.term);
    pushVarint(header, entry.postings.df);
    pushVarint(header, entry.postings.count);
    pushVarint(header, entry.offset);
    pushVarint(header, entry.postings.bytes.length);
    pushVarint(header, config.postingBlockSize);
    pushVarint(header, entry.postings.blocks.length);
    for (const block of entry.postings.blocks) {
      pushVarint(header, block.offset);
      pushVarint(header, block.maxImpact);
      for (let i = 0; i < filters.length; i++) {
        const filter = filters[i];
        const summary = block.filters[i];
        if (filter.kind === "facet") for (const word of summary.words) pushVarint(header, word);
        else {
          pushVarint(header, summary.min);
          pushVarint(header, summary.max);
        }
      }
    }
  }

  return Buffer.concat([
    Buffer.from(Uint8Array.from(header)),
    ...postingChunks.map(chunk => Buffer.from(chunk))
  ]);
}

export function parseShard(buffer, manifest) {
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

export function decodePostings(shard, entry) {
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

export function decodePostingBlock(shard, entry, blockIndex) {
  const block = entry.blocks?.[blockIndex];
  if (!block) return new Int32Array(0);
  if (!entry.blockPostings) entry.blockPostings = new Map();
  if (entry.blockPostings.has(blockIndex)) return entry.blockPostings.get(blockIndex);
  const next = entry.blocks[blockIndex + 1];
  const end = shard.dataStart + entry.offset + (next ? next.offset : entry.byteLength);
  const state = { pos: shard.dataStart + entry.offset + block.offset };
  const rows = [];
  while (state.pos < end) {
    rows.push(readVarint(shard.bytes, state), readVarint(shard.bytes, state));
  }
  const out = Int32Array.from(rows);
  entry.blockPostings.set(blockIndex, out);
  return out;
}

export function buildRangeFile(ranges) {
  const out = [...TERM_RANGE_MAGIC];
  pushVarint(out, ranges.length);
  for (const [packIndex, offset, length] of ranges) {
    pushVarint(out, packIndex);
    pushVarint(out, offset);
    pushVarint(out, length);
  }
  return Buffer.from(Uint8Array.from(out));
}

export function parseRangeDirectory(buffer, manifest) {
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

export function buildCodesFile(config, total, codes) {
  const fields = [...config.facets.map(f => ({ name: f.name })), ...config.numbers.map(n => ({ name: n.name }))];
  const header = [...CODE_MAGIC];
  const chunks = [];
  pushVarint(header, total);
  pushVarint(header, fields.length);
  for (const field of fields) {
    const values = codes[field.name] || [];
    const width = fixedWidth(values);
    pushUtf8(header, field.name);
    header.push(width);
    const chunk = Buffer.alloc(total * width);
    for (let i = 0; i < total; i++) writeFixedInt(chunk, i * width, width, values[i] || 0);
    chunks.push(chunk);
  }
  return Buffer.concat([Buffer.from(Uint8Array.from(header)), ...chunks]);
}

export function parseCodes(buffer) {
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
