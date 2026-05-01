import {
  CODE_MAGIC,
  DOC_VALUE_MAGIC,
  FACET_DICT_MAGIC,
  TERM_SHARD_MAGIC,
  fixedWidth,
  pushVarint,
  readFixedInt,
  readVarint,
  writeFixedInt
} from "./binary.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const CODE_FORMAT_VERSION = 2;
const CODE_KIND = { facet: 1, number: 2, boolean: 3 };
const CODE_TYPE = { keyword: 1, int: 2, long: 3, float: 4, double: 5, date: 6, boolean: 7 };
const CODE_KIND_NAME = Object.fromEntries(Object.entries(CODE_KIND).map(([key, value]) => [value, key]));
const CODE_TYPE_NAME = Object.fromEntries(Object.entries(CODE_TYPE).map(([key, value]) => [value, key]));
const DOC_VALUE_FORMAT_VERSION = 1;
const FACET_DICT_FORMAT_VERSION = 1;
const MAX_SUMMARY_FACET_WORDS = 64;

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

function pushFloat64(out, value) {
  const buffer = new ArrayBuffer(8);
  new DataView(buffer).setFloat64(0, Number(value || 0), true);
  for (const byte of new Uint8Array(buffer)) out.push(byte);
}

function readFloat64(bytes, state) {
  const view = new DataView(bytes.buffer, bytes.byteOffset + state.pos, 8);
  state.pos += 8;
  return view.getFloat64(0, true);
}

function normalizedNumberType(field) {
  return String(field.type || "int").toLowerCase();
}

export function buildBlockFilters(config, dicts) {
  const maxFacetWords = Math.max(0, Number(config.blockFilterMaxFacetWords ?? MAX_SUMMARY_FACET_WORDS));
  return [
    ...config.facets.map(facet => ({
      name: facet.name,
      kind: "facet",
      words: Math.max(1, Math.ceil((dicts[facet.name]?.values?.length || 1) / 32))
    })).filter(filter => filter.words <= maxFacetWords),
    ...config.numbers.map(number => ({ name: number.name, kind: "number", type: normalizedNumberType(number) })),
    ...(config.booleans || []).map(boolean => ({ name: boolean.name, kind: "boolean", type: "boolean" }))
  ];
}

function addBit(words, value) {
  if (value < 0) return;
  const word = Math.floor(value / 32);
  const bit = value % 32;
  words[word] |= 2 ** bit;
}

function emptySummary(filters) {
  return filters.map(filter => filter.kind === "facet" ? { words: new Array(filter.words).fill(0) } : { min: null, max: null });
}

function updateSummary(summary, filters, codes, doc) {
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    const values = codes[filter.name];
    const value = values ? values[doc] : null;
    if (filter.kind === "facet") {
      if (Array.isArray(value)) {
        if (value.length === summary[i].words.length) {
          for (let w = 0; w < summary[i].words.length; w++) summary[i].words[w] |= value[w] || 0;
        } else {
          for (const item of value) addBit(summary[i].words, item);
        }
      } else {
        addBit(summary[i].words, value);
      }
    } else {
      const numeric = filter.kind === "boolean"
        ? (value === true ? 2 : value === false ? 1 : Number(value))
        : Number(value);
      if (Number.isFinite(numeric)) {
        summary[i].min = summary[i].min == null ? numeric : Math.min(summary[i].min, numeric);
        summary[i].max = summary[i].max == null ? numeric : Math.max(summary[i].max, numeric);
      }
    }
  }
}

function packIndexFromFile(file) {
  const match = /^(\d+)/u.exec(String(file || "0"));
  return match ? Number(match[1]) || 0 : 0;
}

function packFileFromIndex(index) {
  return `${String(index).padStart(4, "0")}.bin`;
}

function objectPackFileFromIndex(index, manifest, kind) {
  const table = manifest.object_store?.pack_table?.[kind] || manifest.object_store?.packs?.[kind] || [];
  return table[index] || packFileFromIndex(index);
}

function writeBlockFilterSummary(out, filters, summary) {
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    const item = Array.isArray(summary) ? summary[i] : summary?.[filter.name];
    if (filter.kind === "facet") {
      for (const word of item?.words || []) pushVarint(out, word);
    } else if (filter.kind === "number") {
      const hasValues = Number.isFinite(item?.min) && Number.isFinite(item?.max);
      out.push(hasValues ? 1 : 0);
      if (hasValues) {
        pushFloat64(out, item.min);
        pushFloat64(out, item.max);
      }
    } else {
      pushVarint(out, item?.min || 0);
      pushVarint(out, item?.max || 0);
    }
  }
}

function readBlockFilterSummary(bytes, state, manifest) {
  const filters = {};
  for (const filter of manifest.block_filters || []) {
    if (filter.kind === "facet") {
      const words = new Array(filter.words);
      for (let w = 0; w < filter.words; w++) words[w] = readVarint(bytes, state);
      filters[filter.name] = { words };
    } else if (filter.kind === "number") {
      const hasValues = bytes[state.pos++] === 1;
      filters[filter.name] = hasValues
        ? { min: readFloat64(bytes, state), max: readFloat64(bytes, state) }
        : { min: null, max: null };
    } else {
      filters[filter.name] = {
        min: readVarint(bytes, state),
        max: readVarint(bytes, state)
      };
    }
  }
  return filters;
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
      writeBlockFilterSummary(header, filters, block.filters);
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
  const externalBlockFormat = manifest.stats?.posting_block_storage === "range-pack-v1";
  const objectPointers = manifest.object_store?.pointer_format === "rfbp-v1" || manifest.features?.checksummedObjects;
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
    entry.external = externalBlockFormat && readVarint(bytes, state) === 1;
    entry.blocks = new Array(blockCount);
    for (let j = 0; j < blockCount; j++) {
      const block = {
        offset: readVarint(bytes, state),
        maxImpact: readVarint(bytes, state),
        filters: {}
      };
      block.filters = readBlockFilterSummary(bytes, state, manifest);
      if (entry.external) {
        const packIndex = readVarint(bytes, state);
        const range = {
          pack: objectPackFileFromIndex(packIndex, manifest, "postingBlocks"),
          offset: readVarint(bytes, state),
          length: readVarint(bytes, state)
        };
        range.physicalLength = range.length;
        if (objectPointers) {
          const logicalLength = readVarint(bytes, state);
          const algorithm = readUtf8(bytes, state);
          const value = readUtf8(bytes, state);
          range.logicalLength = logicalLength || null;
          range.checksum = value ? { algorithm: algorithm || "sha256", value } : null;
        }
        block.range = range;
      }
      entry.blocks[j] = block;
    }
    terms.set(term, entry);
  }
  return { bytes, dataStart: state.pos, terms };
}

export function decodePostings(shard, entry) {
  if (entry.external) throw new Error("External posting blocks require async runtime loading.");
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

export function decodePostingBytes(bytes) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const state = { pos: 0 };
  const rows = [];
  while (state.pos < source.length) {
    rows.push(readVarint(source, state), readVarint(source, state));
  }
  return Int32Array.from(rows);
}

export function decodePostingBlock(shard, entry, blockIndex) {
  const block = entry.blocks?.[blockIndex];
  if (!block) return new Int32Array(0);
  if (entry.external) throw new Error("External posting blocks require async runtime loading.");
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

export function rewriteTermShardForExternalBlocks(buffer, manifest, config, writeBlock) {
  const source = parseShard(buffer, { block_filters: manifest.block_filters || [] });
  const minBlocks = Math.max(1, Number(config.externalPostingBlockMinBlocks || 0));
  const minBytes = Math.max(0, Number(config.externalPostingBlockMinBytes || 0));
  const header = [...TERM_SHARD_MAGIC];
  const chunks = [];
  const directory = [];
  let postingOffset = 0;
  const stats = {
    externalBlocks: 0,
    externalTerms: 0,
    externalPostings: 0,
    externalPostingBytes: 0,
    inlinePostingBytes: 0
  };

  for (const [term, entry] of source.terms) {
    const external = !!writeBlock && entry.blocks.length >= minBlocks && entry.byteLength >= minBytes;
    const blocks = [];
    if (external) {
      stats.externalTerms++;
      for (let i = 0; i < entry.blocks.length; i++) {
        const block = entry.blocks[i];
        const next = entry.blocks[i + 1];
        const start = source.dataStart + entry.offset + block.offset;
        const end = source.dataStart + entry.offset + (next ? next.offset : entry.byteLength);
        const bytes = source.bytes.subarray(start, end);
        const range = writeBlock({ term, blockIndex: i, bytes });
        blocks.push({
          ...block,
          range: {
            packIndex: packIndexFromFile(range.pack),
            offset: range.offset,
            length: range.length,
            physicalLength: range.physicalLength || range.length,
            logicalLength: range.logicalLength || null,
            checksum: range.checksum || null
          }
        });
        stats.externalBlocks++;
        stats.externalPostings += Math.min(entry.blockSize, entry.count - i * entry.blockSize);
        stats.externalPostingBytes += bytes.length;
      }
      directory.push({ term, entry, offset: 0, byteLength: 0, external: true, blocks });
    } else {
      const bytes = source.bytes.subarray(source.dataStart + entry.offset, source.dataStart + entry.offset + entry.byteLength);
      chunks.push(bytes);
      stats.inlinePostingBytes += bytes.length;
      directory.push({ term, entry, offset: postingOffset, byteLength: bytes.length, external: false, blocks: entry.blocks });
      postingOffset += bytes.length;
    }
  }

  pushVarint(header, directory.length);
  for (const item of directory) {
    pushUtf8(header, item.term);
    pushVarint(header, item.entry.df);
    pushVarint(header, item.entry.count);
    pushVarint(header, item.offset);
    pushVarint(header, item.byteLength);
    pushVarint(header, item.entry.blockSize);
    pushVarint(header, item.blocks.length);
    pushVarint(header, item.external ? 1 : 0);
    for (const block of item.blocks) {
      pushVarint(header, block.offset);
      pushVarint(header, block.maxImpact);
      writeBlockFilterSummary(header, manifest.block_filters || [], block.filters);
      if (item.external) {
        pushVarint(header, block.range.packIndex);
        pushVarint(header, block.range.offset);
        pushVarint(header, block.range.length);
        if (block.range.checksum?.value) {
          pushVarint(header, block.range.logicalLength || 0);
          pushUtf8(header, block.range.checksum.algorithm || "sha256");
          pushUtf8(header, block.range.checksum.value);
        }
      }
    }
  }

  return {
    buffer: Buffer.concat([
      Buffer.from(Uint8Array.from(header)),
      ...chunks.map(chunk => Buffer.from(chunk))
    ]),
    stats
  };
}

function facetWords(value, words) {
  if (Array.isArray(value) && value.length === words) return value;
  const out = new Array(words).fill(0);
  const values = Array.isArray(value) ? value : [value];
  for (const item of values) addBit(out, Number(item) || 0);
  return out;
}

function finiteValues(values) {
  return values.filter(value => Number.isFinite(value));
}

function writeFloat64Array(total, values) {
  const chunk = Buffer.alloc(total * 8);
  for (let i = 0; i < total; i++) {
    const value = values[i] == null ? Number.NaN : Number(values[i]);
    chunk.writeDoubleLE(Number.isFinite(value) ? value : Number.NaN, i * 8);
  }
  return chunk;
}

function encodeIntegerField(total, values) {
  const present = finiteValues(values);
  const min = present.length ? Math.min(...present) : 0;
  const encoded = values.map(value => Number.isFinite(value) ? Math.max(1, Math.round(value - min + 1)) : 0);
  const width = fixedWidth(encoded);
  const chunk = Buffer.alloc(total * width);
  for (let i = 0; i < total; i++) writeFixedInt(chunk, i * width, width, encoded[i] || 0);
  return { chunk, width, min };
}

function fieldDescriptors(config, codes = {}) {
  return [
    ...config.facets.map(facet => ({
      name: facet.name,
      kind: "facet",
      type: "keyword",
      words: Math.max(1, Math.ceil(((codes._dicts?.[facet.name]?.values?.length) || 1) / 32))
    })),
    ...config.numbers.map(number => ({ name: number.name, kind: "number", type: normalizedNumberType(number), words: 0 })),
    ...(config.booleans || []).map(boolean => ({ name: boolean.name, kind: "boolean", type: "boolean", words: 0 }))
  ];
}

function summarizeFacetRows(rows, words) {
  const summary = new Array(words).fill(0);
  for (const value of rows) {
    const row = facetWords(value, words);
    for (let word = 0; word < words; word++) summary[word] |= row[word] || 0;
  }
  return words <= MAX_SUMMARY_FACET_WORDS ? { words: summary } : { words: null };
}

function summarizeNumericRows(rows) {
  const values = rows.filter(value => Number.isFinite(value));
  return values.length ? { min: Math.min(...values), max: Math.max(...values) } : { min: null, max: null };
}

function summarizeBooleanRows(rows) {
  const values = rows
    .map(value => normalizeBooleanValue(value))
    .filter(value => value != null)
    .map(value => value ? 2 : 1);
  return values.length ? { min: Math.min(...values), max: Math.max(...values) } : { min: null, max: null };
}

function encodeDocValueRows(field, rows) {
  if (field.kind === "facet") {
    const chunk = Buffer.alloc(rows.length * field.words * 4);
    for (let doc = 0; doc < rows.length; doc++) {
      const words = facetWords(rows[doc], field.words);
      for (let word = 0; word < field.words; word++) writeFixedInt(chunk, (doc * field.words + word) * 4, 4, words[word] || 0);
    }
    return { chunk, width: 4, min: 0, summary: summarizeFacetRows(rows, field.words) };
  }
  if (field.kind === "boolean") {
    const chunk = Buffer.alloc(rows.length);
    for (let doc = 0; doc < rows.length; doc++) {
      const value = normalizeBooleanValue(rows[doc]);
      chunk[doc] = value == null ? 0 : value ? 2 : 1;
    }
    return { chunk, width: 1, min: 0, summary: summarizeBooleanRows(rows) };
  }
  if (field.type === "float" || field.type === "double") {
    return { chunk: writeFloat64Array(rows.length, rows), width: 8, min: 0, summary: summarizeNumericRows(rows.map(Number)) };
  }
  const encoded = encodeIntegerField(rows.length, rows);
  return { ...encoded, summary: summarizeNumericRows(rows.map(Number)) };
}

export function docValueFields(config, codes = {}) {
  return fieldDescriptors(config, codes).map(field => ({ ...field }));
}

export function buildDocValueChunk(field, start, rows) {
  const encoded = encodeDocValueRows(field, rows);
  const header = [...DOC_VALUE_MAGIC];
  pushVarint(header, DOC_VALUE_FORMAT_VERSION);
  pushUtf8(header, field.name);
  header.push(CODE_KIND[field.kind]);
  header.push(CODE_TYPE[field.type] || CODE_TYPE.int);
  header.push(encoded.width);
  pushVarint(header, field.words || 0);
  pushVarint(header, start);
  pushVarint(header, rows.length);
  pushFloat64(header, encoded.min || 0);
  return {
    buffer: Buffer.concat([Buffer.from(Uint8Array.from(header)), encoded.chunk]),
    width: encoded.width,
    min: encoded.min || 0,
    summary: encoded.summary
  };
}

export function parseDocValueChunk(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, DOC_VALUE_MAGIC, "Unsupported Rangefind doc-value chunk");
  const state = { pos: DOC_VALUE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== DOC_VALUE_FORMAT_VERSION) throw new Error(`Unsupported Rangefind doc-value chunk version ${version}`);
  const field = {
    name: readUtf8(bytes, state),
    kind: CODE_KIND_NAME[bytes[state.pos++]] || "number",
    type: CODE_TYPE_NAME[bytes[state.pos++]] || "int",
    width: bytes[state.pos++],
    words: readVarint(bytes, state),
    start: readVarint(bytes, state),
    count: readVarint(bytes, state),
    min: readFloat64(bytes, state)
  };
  const values = new Array(field.count);
  if (field.kind === "facet") {
    for (let doc = 0; doc < field.count; doc++) {
      const words = new Array(field.words);
      for (let word = 0; word < field.words; word++) words[word] = readFixedInt(bytes, state.pos + (doc * field.words + word) * field.width, field.width);
      values[doc] = words;
    }
  } else if (field.kind === "boolean") {
    for (let doc = 0; doc < field.count; doc++) {
      const value = bytes[state.pos + doc];
      values[doc] = value === 0 ? null : value === 2;
    }
  } else if (field.type === "float" || field.type === "double") {
    for (let doc = 0; doc < field.count; doc++) {
      const value = new DataView(bytes.buffer, bytes.byteOffset + state.pos + doc * 8, 8).getFloat64(0, true);
      values[doc] = Number.isNaN(value) ? null : value;
    }
  } else {
    for (let doc = 0; doc < field.count; doc++) {
      const encoded = readFixedInt(bytes, state.pos + doc * field.width, field.width);
      values[doc] = encoded ? field.min + encoded - 1 : null;
    }
  }
  return { ...field, values };
}

function normalizeBooleanValue(value) {
  if (value === true || value === 1 || value === "true" || value === "1") return true;
  if (value === false || value === 0 || value === "false" || value === "0") return false;
  return null;
}

export function buildCodesFile(config, total, codes) {
  const fields = fieldDescriptors(config, codes);
  const descriptors = [];
  const chunks = [];

  for (const field of fields) {
    const values = codes[field.name] || [];
    if (field.kind === "facet") {
      const chunk = Buffer.alloc(total * field.words * 4);
      for (let doc = 0; doc < total; doc++) {
        const words = facetWords(values[doc], field.words);
        for (let word = 0; word < field.words; word++) writeFixedInt(chunk, (doc * field.words + word) * 4, 4, words[word] || 0);
      }
      descriptors.push({ ...field, width: 4, min: 0 });
      chunks.push(chunk);
    } else if (field.kind === "boolean") {
      const chunk = Buffer.alloc(total);
      for (let doc = 0; doc < total; doc++) {
        const value = normalizeBooleanValue(values[doc]);
        chunk[doc] = value == null ? 0 : value ? 2 : 1;
      }
      descriptors.push({ ...field, width: 1, min: 0 });
      chunks.push(chunk);
    } else if (field.type === "float" || field.type === "double") {
      descriptors.push({ ...field, width: 8, min: 0 });
      chunks.push(writeFloat64Array(total, values));
    } else {
      const encoded = encodeIntegerField(total, values);
      descriptors.push({ ...field, width: encoded.width, min: encoded.min });
      chunks.push(encoded.chunk);
    }
  }

  const header = [...CODE_MAGIC];
  pushVarint(header, CODE_FORMAT_VERSION);
  pushVarint(header, total);
  pushVarint(header, descriptors.length);
  for (const field of descriptors) {
    pushUtf8(header, field.name);
    header.push(CODE_KIND[field.kind]);
    header.push(CODE_TYPE[field.type] || CODE_TYPE.int);
    header.push(field.width);
    pushVarint(header, field.words || 0);
    pushFloat64(header, field.min || 0);
  }
  return Buffer.concat([Buffer.from(Uint8Array.from(header)), ...chunks]);
}

function parseLegacyCodes(bytes) {
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

export function parseCodes(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, CODE_MAGIC, "Unsupported Rangefind code table");
  const state = { pos: CODE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== CODE_FORMAT_VERSION) return parseLegacyCodes(bytes);
  const total = readVarint(bytes, state);
  const fieldCount = readVarint(bytes, state);
  const fields = [];
  for (let i = 0; i < fieldCount; i++) {
    fields.push({
      name: readUtf8(bytes, state),
      kind: CODE_KIND_NAME[bytes[state.pos++]] || "number",
      type: CODE_TYPE_NAME[bytes[state.pos++]] || "int",
      width: bytes[state.pos++],
      words: readVarint(bytes, state),
      min: readFloat64(bytes, state)
    });
  }
  const out = {};
  for (const field of fields) {
    const values = new Array(total);
    if (field.kind === "facet") {
      for (let doc = 0; doc < total; doc++) {
        const words = new Array(field.words);
        for (let word = 0; word < field.words; word++) words[word] = readFixedInt(bytes, state.pos + (doc * field.words + word) * field.width, field.width);
        values[doc] = words;
      }
      state.pos += total * field.words * field.width;
    } else if (field.kind === "boolean") {
      for (let doc = 0; doc < total; doc++) {
        const value = bytes[state.pos + doc];
        values[doc] = value === 0 ? null : value === 2;
      }
      state.pos += total;
    } else if (field.type === "float" || field.type === "double") {
      for (let doc = 0; doc < total; doc++) {
        const value = new DataView(bytes.buffer, bytes.byteOffset + state.pos + doc * 8, 8).getFloat64(0, true);
        values[doc] = Number.isNaN(value) ? null : value;
      }
      state.pos += total * 8;
    } else {
      for (let doc = 0; doc < total; doc++) {
        const encoded = readFixedInt(bytes, state.pos + doc * field.width, field.width);
        values[doc] = encoded ? field.min + encoded - 1 : null;
      }
      state.pos += total * field.width;
    }
    out[field.name] = values;
  }
  Object.defineProperty(out, "_meta", { value: { version, total, fields }, enumerable: false });
  return out;
}

export function buildFacetDictionary(values) {
  const out = [...FACET_DICT_MAGIC];
  pushVarint(out, FACET_DICT_FORMAT_VERSION);
  pushVarint(out, values.length);
  for (const item of values) {
    pushUtf8(out, item.value);
    pushUtf8(out, item.label);
    pushVarint(out, item.n || 0);
  }
  return Buffer.from(Uint8Array.from(out));
}

export function parseFacetDictionary(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, FACET_DICT_MAGIC, "Unsupported Rangefind facet dictionary");
  const state = { pos: FACET_DICT_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FACET_DICT_FORMAT_VERSION) throw new Error(`Unsupported Rangefind facet dictionary version ${version}`);
  const count = readVarint(bytes, state);
  const values = new Array(count);
  for (let i = 0; i < count; i++) {
    values[i] = {
      value: readUtf8(bytes, state),
      label: readUtf8(bytes, state),
      n: readVarint(bytes, state)
    };
  }
  return values;
}
