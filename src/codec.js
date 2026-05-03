import {
  CODE_MAGIC,
  DOC_VALUE_MAGIC,
  FACET_DICT_MAGIC,
  POSTING_SEGMENT_MAGIC,
  fixedWidth,
  pushVarint,
  readFixedInt,
  readVarint,
  writeFixedInt
} from "./binary.js";
import { postingRowCount, postingRowDoc, postingRowScore } from "./posting_rows.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const CODE_FORMAT_VERSION = 2;
const CODE_KIND = { facet: 1, number: 2, boolean: 3 };
const CODE_TYPE = { keyword: 1, int: 2, long: 3, float: 4, double: 5, date: 6, boolean: 7 };
const CODE_KIND_NAME = Object.fromEntries(Object.entries(CODE_KIND).map(([key, value]) => [value, key]));
const CODE_TYPE_NAME = Object.fromEntries(Object.entries(CODE_TYPE).map(([key, value]) => [value, key]));
const DOC_VALUE_FORMAT_VERSION = 2;
const DOC_VALUE_ENCODING_DENSE = 0;
const DOC_VALUE_ENCODING_SPARSE_FACET = 1;
const FACET_DICT_FORMAT_VERSION = 1;
export const POSTING_SEGMENT_FORMAT = "rfsegpost-v3";
const POSTING_SEGMENT_FORMAT_VERSION = 3;
const MAX_SUMMARY_FACET_WORDS = 64;
export const POSTING_BLOCK_CODEC_PAIR_VARINT = "pair-varint-v1";
export const POSTING_BLOCK_CODEC_IMPACT_RUNS = "impact-runs-v1";
export const POSTING_BLOCK_CODEC_IMPACT_BITSET = "impact-bitset-v1";
export const POSTING_BLOCK_CODEC_PARTITIONED_DELTAS = "partitioned-deltas-v1";
const POSTING_BLOCK_CODEC_CODES = {
  [POSTING_BLOCK_CODEC_PAIR_VARINT]: 0,
  [POSTING_BLOCK_CODEC_IMPACT_RUNS]: 1,
  [POSTING_BLOCK_CODEC_IMPACT_BITSET]: 2,
  [POSTING_BLOCK_CODEC_PARTITIONED_DELTAS]: 3
};
const POSTING_BLOCK_CODECS = Object.fromEntries(Object.entries(POSTING_BLOCK_CODEC_CODES).map(([name, code]) => [code, name]));

function postingBlockCodecCode(codec) {
  return POSTING_BLOCK_CODEC_CODES[codec] ?? POSTING_BLOCK_CODEC_CODES[POSTING_BLOCK_CODEC_PAIR_VARINT];
}

function postingBlockCodecName(code) {
  return POSTING_BLOCK_CODECS[code] || POSTING_BLOCK_CODEC_PAIR_VARINT;
}

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

function facetCodes(value) {
  if (value?.codes) return value.codes.map(Number).filter(Number.isFinite);
  return null;
}

function emptySummary(filters) {
  return filters.map(filter => filter.kind === "facet" ? { words: new Array(filter.words).fill(0) } : { min: null, max: null });
}

function codeValue(codes, name, doc) {
  if (codes && typeof codes.get === "function") return codes.get(name, doc);
  const values = codes?.[name];
  return values ? values[doc] : null;
}

function updateSummary(summary, filters, codes, doc) {
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    const value = codeValue(codes, filter.name, doc);
    if (filter.kind === "facet") {
      const codesForDoc = facetCodes(value);
      if (codesForDoc) {
        for (const item of codesForDoc) addBit(summary[i].words, item);
      } else if (Array.isArray(value)) {
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

function mergeBlockFilterSummary(target, filters, source) {
  for (let i = 0; i < filters.length; i++) {
    const filter = filters[i];
    const targetItem = target[i];
    const sourceItem = Array.isArray(source) ? source[i] : source?.[filter.name];
    if (!sourceItem) continue;
    if (filter.kind === "facet") {
      for (let w = 0; w < filter.words; w++) targetItem.words[w] |= sourceItem.words?.[w] || 0;
    } else {
      const min = sourceItem.min;
      const max = sourceItem.max;
      if (min != null) targetItem.min = targetItem.min == null ? min : Math.min(targetItem.min, min);
      if (max != null) targetItem.max = targetItem.max == null ? max : Math.max(targetItem.max, max);
    }
  }
}

function postingSuperblockSize(config) {
  return Math.max(1, Math.floor(Number(config.postingSuperblockSize || 16)));
}

function buildPostingSuperblocks(blocks, filters, config) {
  const size = postingSuperblockSize(config);
  const superblocks = [];
  for (let firstBlock = 0; firstBlock < blocks.length; firstBlock += size) {
    const blockCount = Math.min(size, blocks.length - firstBlock);
    const summary = emptySummary(filters);
    let rowCount = 0;
    let maxImpact = 0;
    let maxImpactDoc = 0;
    for (let i = firstBlock; i < firstBlock + blockCount; i++) {
      const block = blocks[i];
      rowCount += block.rowCount || 0;
      if (block.maxImpact > maxImpact) {
        maxImpact = block.maxImpact;
        maxImpactDoc = block.maxImpactDoc || 0;
      } else if (block.maxImpact === maxImpact && (block.maxImpactDoc || 0) < maxImpactDoc) {
        maxImpactDoc = block.maxImpactDoc || 0;
      }
      mergeBlockFilterSummary(summary, filters, block.filters);
    }
    superblocks.push({ firstBlock, blockCount, rowCount, maxImpact, maxImpactDoc, filters: summary });
  }
  return superblocks;
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

export function writeBlockFilterSummary(out, filters, summary) {
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

export function readBlockFilterSummary(bytes, state, manifest) {
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

export function summarizeBlockFilters(filters, codes, docs) {
  const summary = emptySummary(filters || []);
  for (const doc of docs || []) updateSummary(summary, filters || [], codes, doc);
  return Object.fromEntries((filters || []).map((filter, index) => [filter.name, summary[index]]));
}

function encodePostings(rows, total, codes, filters, config) {
  const df = postingRowCount(rows);
  const idf = Math.log(1 + (total - df + 0.5) / (df + 0.5));
  const docs = new Int32Array(df);
  const impacts = new Int32Array(df);
  let maxImpact = 0;
  let docsSorted = true;
  let previousDoc = -1;
  for (let i = 0; i < df; i++) {
    docs[i] = postingRowDoc(rows, i);
    impacts[i] = Math.max(1, Math.round(postingRowScore(rows, i) * idf / 10));
    maxImpact = Math.max(maxImpact, impacts[i]);
    if (docs[i] < previousDoc) docsSorted = false;
    previousDoc = docs[i];
  }
  const orderPlan = postingImpactOrder(docs, impacts, maxImpact, docsSorted, config);
  const order = orderPlan.order;
  const chunks = [];
  const blocks = [];
  let offset = 0;
  for (let start = 0; start < order.length; start += config.postingBlockSize) {
    const end = Math.min(order.length, start + config.postingBlockSize);
    const blockRows = new Array(end - start);
    for (let i = start; i < end; i++) {
      const row = order[i];
      blockRows[i - start] = [docs[row], impacts[row]];
    }
    if (!blockRows.length) continue;
    const [maxImpactDoc, maxImpact] = blockRows[0];
    const block = {
      offset,
      maxImpact,
      maxImpactDoc,
      rowCount: blockRows.length,
      filters: emptySummary(filters)
    };
    for (const [doc] of blockRows) updateSummary(block.filters, filters, codes, doc);
    const encodedBlock = encodePostingBlockRows(blockRows, config);
    block.codec = encodedBlock.codec;
    block.bytes = encodedBlock.bytes;
    block.baselineBytes = encodedBlock.baselineBytes;
    block.encodedBytes = encodedBlock.bytes.length;
    block.alternateBytes = encodedBlock.alternateBytes;
    block.impactRunBytes = encodedBlock.impactRunBytes;
    block.impactBitsetBytes = encodedBlock.impactBitsetBytes;
    block.partitionedDeltaBytes = encodedBlock.partitionedDeltaBytes;
    blocks.push(block);
    chunks.push(encodedBlock.bytes);
    offset += encodedBlock.bytes.length;
  }
  return { df, count: order.length, byteLength: offset, chunks, blocks, orderCodec: orderPlan.codec };
}

function postingImpactOrder(docs, impacts, maxImpact, docsSorted, config) {
  const count = docs.length;
  const minRows = Math.max(0, Math.floor(Number(config.postingImpactBucketOrderMinRows ?? 2048)));
  const maxBuckets = Math.max(1, Math.floor(Number(config.postingImpactBucketOrderMaxBuckets ?? 65536)));
  if (docsSorted && count >= minRows && maxImpact > 0 && maxImpact <= maxBuckets) {
    const counts = new Int32Array(maxImpact + 1);
    for (let i = 0; i < count; i++) counts[impacts[i]]++;
    const starts = new Int32Array(maxImpact + 1);
    let cursor = 0;
    for (let impact = maxImpact; impact >= 0; impact--) {
      starts[impact] = cursor;
      cursor += counts[impact];
    }
    const next = new Int32Array(starts);
    const order = new Int32Array(count);
    for (let i = 0; i < count; i++) order[next[impacts[i]]++] = i;
    return { order, codec: "impact-bucket" };
  }
  const order = new Array(count);
  for (let i = 0; i < count; i++) order[i] = i;
  order.sort((a, b) => impacts[b] - impacts[a] || docs[a] - docs[b]);
  return { order, codec: "impact-sort" };
}

function concatUint8(chunks, total) {
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

function encodePairVarintRows(rows) {
  const bytes = [];
  for (const [doc, impact] of rows) {
    pushVarint(bytes, doc);
    pushVarint(bytes, impact);
  }
  return Uint8Array.from(bytes);
}

function encodeImpactRunRows(rows) {
  const bytes = [];
  let groupCount = 0;
  const groups = [];
  for (let i = 0; i < rows.length;) {
    const impact = rows[i][1];
    const docs = [];
    while (i < rows.length && rows[i][1] === impact) {
      docs.push(rows[i][0]);
      i++;
    }
    groups.push({ impact, docs });
    groupCount++;
  }
  pushVarint(bytes, groupCount);
  for (const group of groups) {
    pushVarint(bytes, group.impact);
    pushVarint(bytes, group.docs.length);
    let previous = -1;
    for (const doc of group.docs) {
      pushVarint(bytes, doc - previous);
      previous = doc;
    }
  }
  return Uint8Array.from(bytes);
}

function impactGroups(rows) {
  const groups = [];
  for (let i = 0; i < rows.length;) {
    const impact = rows[i][1];
    const docs = [];
    while (i < rows.length && rows[i][1] === impact) {
      docs.push(rows[i][0]);
      i++;
    }
    groups.push({ impact, docs });
  }
  return groups;
}

function encodeImpactBitsetRows(rows) {
  const bytes = [];
  const groups = impactGroups(rows);
  pushVarint(bytes, groups.length);
  for (const group of groups) {
    const minDoc = group.docs[0] || 0;
    const maxDoc = group.docs[group.docs.length - 1] || minDoc;
    const span = maxDoc - minDoc + 1;
    const bitBytes = new Uint8Array(Math.ceil(span / 8));
    for (const doc of group.docs) {
      const bit = doc - minDoc;
      bitBytes[Math.floor(bit / 8)] |= 1 << (bit % 8);
    }
    pushVarint(bytes, group.impact);
    pushVarint(bytes, minDoc);
    pushVarint(bytes, span);
    pushVarint(bytes, bitBytes.length);
    for (const byte of bitBytes) bytes.push(byte);
  }
  return Uint8Array.from(bytes);
}

function bitWidth(value) {
  return value <= 0 ? 0 : Math.ceil(Math.log2(value + 1));
}

function pushPackedUnsigned(out, values, width) {
  if (!values.length || width <= 0) return;
  let current = 0;
  let filled = 0;
  for (const value of values) {
    let consumed = 0;
    while (consumed < width) {
      const take = Math.min(8 - filled, width - consumed);
      const chunk = Math.floor(value / (2 ** consumed)) % (2 ** take);
      current += chunk * (2 ** filled);
      filled += take;
      consumed += take;
      if (filled === 8) {
        out.push(current);
        current = 0;
        filled = 0;
      }
    }
  }
  if (filled > 0) out.push(current);
}

function readPackedUnsigned(source, state, count, width, byteLength) {
  const start = state.pos;
  state.pos += byteLength;
  if (!count) return [];
  if (width <= 0) return new Array(count).fill(0);
  const values = [];
  let byteOffset = start;
  let current = source[byteOffset++] || 0;
  let consumedFromByte = 0;
  for (let i = 0; i < count; i++) {
    let value = 0;
    let filled = 0;
    while (filled < width) {
      const available = 8 - consumedFromByte;
      const take = Math.min(available, width - filled);
      const chunk = Math.floor(current / (2 ** consumedFromByte)) % (2 ** take);
      value += chunk * (2 ** filled);
      consumedFromByte += take;
      filled += take;
      if (consumedFromByte === 8 && filled < width) {
        current = source[byteOffset++] || 0;
        consumedFromByte = 0;
      }
    }
    values.push(value);
    if (consumedFromByte === 8 && i < count - 1) {
      current = source[byteOffset++] || 0;
      consumedFromByte = 0;
    }
  }
  return values;
}

function encodePartitionedDeltaRows(rows) {
  const bytes = [];
  const groups = impactGroups(rows);
  pushVarint(bytes, groups.length);
  for (const group of groups) {
    const deltas = [];
    let previous = group.docs[0] || 0;
    for (let i = 1; i < group.docs.length; i++) {
      const delta = group.docs[i] - previous;
      deltas.push(delta);
      previous = group.docs[i];
    }
    const width = bitWidth(Math.max(0, ...deltas));
    if (width > 30) return null;
    const packed = [];
    pushPackedUnsigned(packed, deltas, width);
    pushVarint(bytes, group.impact);
    pushVarint(bytes, group.docs.length);
    pushVarint(bytes, group.docs[0] || 0);
    pushVarint(bytes, width);
    pushVarint(bytes, packed.length);
    for (const byte of packed) bytes.push(byte);
  }
  return Uint8Array.from(bytes);
}

function encodePostingBlockRows(rows, config) {
  const baseline = encodePairVarintRows(rows);
  const mode = String(config.codecs?.mode || "auto").toLowerCase();
  if (mode === "off" || mode === "pair" || mode === "pairs" || mode === "pair-varint" || mode === "varint") {
    return {
      codec: POSTING_BLOCK_CODEC_PAIR_VARINT,
      bytes: baseline,
      baselineBytes: baseline.length,
      alternateBytes: baseline.length
    };
  }
  const impactRuns = encodeImpactRunRows(rows);
  const impactBitset = encodeImpactBitsetRows(rows);
  const partitionedDeltas = encodePartitionedDeltaRows(rows);
  const candidates = [
    {
      codec: POSTING_BLOCK_CODEC_PAIR_VARINT,
      bytes: baseline
    },
    {
      codec: POSTING_BLOCK_CODEC_IMPACT_RUNS,
      bytes: impactRuns,
    },
    {
      codec: POSTING_BLOCK_CODEC_IMPACT_BITSET,
      bytes: impactBitset
    }
  ];
  if (partitionedDeltas) {
    candidates.push({
      codec: POSTING_BLOCK_CODEC_PARTITIONED_DELTAS,
      bytes: partitionedDeltas
    });
  }
  let selected = candidates[0];
  if (mode === "impact-runs" || mode === "compact-impact") selected = candidates[1];
  else if (mode === "impact-bitset" || mode === "dense-bitset") selected = candidates[2];
  else if ((mode === "partitioned-deltas" || mode === "partitioned-ef") && partitionedDeltas) selected = candidates[candidates.length - 1];
  else if (mode === "auto") {
    selected = candidates.reduce((best, candidate) => candidate.bytes.length < best.bytes.length ? candidate : best, candidates[0]);
  }
  return {
    codec: selected.codec,
    bytes: selected.bytes,
    baselineBytes: baseline.length,
    alternateBytes: Math.min(impactRuns.length, impactBitset.length, partitionedDeltas?.length || Infinity),
    impactRunBytes: impactRuns.length,
    impactBitsetBytes: impactBitset.length,
    partitionedDeltaBytes: partitionedDeltas?.length || 0
  };
}

function addPostingBlockCodecStats(stats, block) {
  if (block.codec === POSTING_BLOCK_CODEC_IMPACT_RUNS) stats.impactRunBlocks++;
  else if (block.codec === POSTING_BLOCK_CODEC_IMPACT_BITSET) stats.impactBitsetBlocks++;
  else if (block.codec === POSTING_BLOCK_CODEC_PARTITIONED_DELTAS) stats.partitionedDeltaBlocks++;
  else stats.pairVarintBlocks++;
  stats.blockCodecBaselineBytes += block.baselineBytes || block.encodedBytes || 0;
  stats.blockCodecSelectedBytes += block.encodedBytes || 0;
  stats.blockCodecImpactRunCandidateBytes += block.alternateBytes || 0;
  stats.blockCodecImpactBitsetCandidateBytes += block.impactBitsetBytes || 0;
  stats.blockCodecPartitionedDeltaCandidateBytes += block.partitionedDeltaBytes || 0;
}

export function buildPostingSegmentChunks(entries, total, codes, filters, config, writeBlock) {
  const minBlocks = Math.max(1, Number(config.externalPostingBlockMinBlocks || 0));
  const minBytes = Math.max(0, Number(config.externalPostingBlockMinBytes || 0));
  const header = [...POSTING_SEGMENT_MAGIC];
  const chunks = [];
  let chunkBytes = 0;
  const directory = [];
  let postingOffset = 0;
  const stats = {
    externalBlocks: 0,
    externalTerms: 0,
    externalPostings: 0,
    externalPostingBytes: 0,
    inlinePostingBytes: 0,
    superblocks: 0,
    superblockTerms: 0,
    superblockBlocks: 0,
    pairVarintBlocks: 0,
    impactRunBlocks: 0,
    impactBitsetBlocks: 0,
    partitionedDeltaBlocks: 0,
    blockCodecBaselineBytes: 0,
    blockCodecSelectedBytes: 0,
    blockCodecImpactRunCandidateBytes: 0,
    blockCodecImpactBitsetCandidateBytes: 0,
    blockCodecPartitionedDeltaCandidateBytes: 0,
    impactBucketOrderTerms: 0,
    impactBucketOrderPostings: 0
  };

  const orderedEntries = Array.isArray(entries)
    ? entries.slice().sort((a, b) => a[0].localeCompare(b[0]))
    : entries;
  for (const [term, rows] of orderedEntries) {
    const postings = encodePostings(rows, total, codes, filters, config);
    if (postings.orderCodec === "impact-bucket") {
      stats.impactBucketOrderTerms++;
      stats.impactBucketOrderPostings += postings.count;
    }
    const external = !!writeBlock && postings.blocks.length >= minBlocks && postings.byteLength >= minBytes;
    const blocks = [];

    if (external) {
      stats.externalTerms++;
      for (let i = 0; i < postings.blocks.length; i++) {
        const block = postings.blocks[i];
        const bytes = postings.chunks[i];
        const range = writeBlock({ term, blockIndex: i, bytes });
        blocks.push({
          ...block,
          rowCount: Math.min(config.postingBlockSize, postings.count - i * config.postingBlockSize),
          range: {
            packIndex: packIndexFromFile(range.pack),
            offset: range.offset,
            length: range.length,
            physicalLength: range.physicalLength || range.length,
            logicalLength: range.logicalLength || null,
            checksum: range.checksum || null
          }
        });
        addPostingBlockCodecStats(stats, block);
        stats.externalBlocks++;
        stats.externalPostings += Math.min(config.postingBlockSize, postings.count - i * config.postingBlockSize);
        stats.externalPostingBytes += bytes.length;
      }
      const superblocks = buildPostingSuperblocks(blocks, filters, config);
      stats.superblocks += superblocks.length;
      stats.superblockTerms += superblocks.length > 0 ? 1 : 0;
      stats.superblockBlocks += blocks.length;
      directory.push({ term, postings, offset: 0, byteLength: 0, external: true, blocks, superblocks });
    } else {
      const bytes = concatUint8(postings.chunks, postings.byteLength);
      chunks.push(bytes);
      chunkBytes += bytes.length;
      stats.inlinePostingBytes += bytes.length;
      for (let i = 0; i < postings.blocks.length; i++) {
        const block = postings.blocks[i];
        blocks.push({
          ...block,
          rowCount: Math.min(config.postingBlockSize, postings.count - i * config.postingBlockSize)
        });
        addPostingBlockCodecStats(stats, block);
      }
      const superblocks = buildPostingSuperblocks(blocks, filters, config);
      stats.superblocks += superblocks.length;
      stats.superblockTerms += superblocks.length > 0 ? 1 : 0;
      stats.superblockBlocks += blocks.length;
      directory.push({ term, postings, offset: postingOffset, byteLength: bytes.length, external: false, blocks, superblocks });
      postingOffset += bytes.length;
    }
  }

  pushVarint(header, POSTING_SEGMENT_FORMAT_VERSION);
  pushVarint(header, directory.length);
  for (const item of directory) {
    pushUtf8(header, item.term);
    pushVarint(header, item.postings.df);
    pushVarint(header, item.postings.count);
    pushVarint(header, item.offset);
    pushVarint(header, item.byteLength);
    pushVarint(header, config.postingBlockSize);
    pushVarint(header, item.blocks.length);
    pushVarint(header, item.external ? 1 : 0);
    for (const block of item.blocks) {
      pushVarint(header, block.offset);
      pushVarint(header, block.rowCount);
      pushVarint(header, block.maxImpact);
      pushVarint(header, block.maxImpactDoc || 0);
      pushVarint(header, postingBlockCodecCode(block.codec));
      writeBlockFilterSummary(header, filters, block.filters);
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
    pushVarint(header, item.superblocks.length);
    for (const superblock of item.superblocks) {
      pushVarint(header, superblock.firstBlock);
      pushVarint(header, superblock.blockCount);
      pushVarint(header, superblock.rowCount);
      pushVarint(header, superblock.maxImpact);
      pushVarint(header, superblock.maxImpactDoc || 0);
      writeBlockFilterSummary(header, filters, superblock.filters);
    }
  }

  const headerBuffer = Buffer.from(Uint8Array.from(header));
  return {
    format: POSTING_SEGMENT_FORMAT,
    chunks: [headerBuffer, ...chunks],
    logicalLength: headerBuffer.length + chunkBytes,
    stats
  };
}

export function buildPostingSegment(entries, total, codes, filters, config, writeBlock) {
  const segment = buildPostingSegmentChunks(entries, total, codes, filters, config, writeBlock);
  return {
    format: segment.format,
    buffer: Buffer.concat(segment.chunks.map(chunk => Buffer.from(chunk)), segment.logicalLength),
    stats: segment.stats
  };
}

function parsePostingSegmentBytes(bytes, manifest = {}) {
  assertMagic(bytes, POSTING_SEGMENT_MAGIC, "Unsupported Rangefind posting segment");
  const objectPointers = manifest.object_store?.pointer_format === "rfbp-v1" || manifest.features?.checksummedObjects;
  const state = { pos: POSTING_SEGMENT_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== POSTING_SEGMENT_FORMAT_VERSION) throw new Error("Unsupported Rangefind posting segment version");
  const termCount = readVarint(bytes, state);
  const terms = new Map();
  for (let i = 0; i < termCount; i++) {
    const term = readUtf8(bytes, state);
    const entry = {
      format: POSTING_SEGMENT_FORMAT,
      df: readVarint(bytes, state),
      count: readVarint(bytes, state),
      offset: readVarint(bytes, state),
      byteLength: readVarint(bytes, state),
      blockSize: readVarint(bytes, state),
      blocks: null,
      superblocks: null,
      postings: null
    };
    const blockCount = readVarint(bytes, state);
    entry.external = readVarint(bytes, state) === 1;
    entry.blocks = new Array(blockCount);
    for (let j = 0; j < blockCount; j++) {
      const block = {
        offset: readVarint(bytes, state),
        rowCount: readVarint(bytes, state),
        maxImpact: readVarint(bytes, state),
        maxImpactDoc: readVarint(bytes, state),
        codec: postingBlockCodecName(readVarint(bytes, state)),
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
    const superblockCount = readVarint(bytes, state);
    entry.superblocks = new Array(superblockCount);
    for (let j = 0; j < superblockCount; j++) {
      entry.superblocks[j] = {
        firstBlock: readVarint(bytes, state),
        blockCount: readVarint(bytes, state),
        rowCount: readVarint(bytes, state),
        maxImpact: readVarint(bytes, state),
        maxImpactDoc: readVarint(bytes, state),
        filters: readBlockFilterSummary(bytes, state, manifest)
      };
    }
    terms.set(term, entry);
  }
  return { format: POSTING_SEGMENT_FORMAT, bytes, dataStart: state.pos, terms };
}

export function parsePostingSegment(buffer, manifest = {}) {
  return parsePostingSegmentBytes(new Uint8Array(buffer), manifest);
}

export function decodePostings(shard, entry) {
  if (entry.external) throw new Error("External posting blocks require async runtime loading.");
  if (entry.postings) return entry.postings;
  const blocks = entry.blocks.map((_, index) => decodePostingBlock(shard, entry, index));
  const out = new Int32Array(blocks.reduce((sum, rows) => sum + rows.length, 0));
  let offset = 0;
  for (const rows of blocks) {
    out.set(rows, offset);
    offset += rows.length;
  }
  entry.postings = out;
  return out;
}

function decodePairVarintBytes(bytes) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const state = { pos: 0 };
  const rows = [];
  while (state.pos < source.length) {
    rows.push(readVarint(source, state), readVarint(source, state));
  }
  return Int32Array.from(rows);
}

function decodeImpactRunBytes(bytes) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const state = { pos: 0 };
  const rows = [];
  const groupCount = readVarint(source, state);
  for (let group = 0; group < groupCount; group++) {
    const impact = readVarint(source, state);
    const count = readVarint(source, state);
    let previous = -1;
    for (let i = 0; i < count; i++) {
      const doc = previous + readVarint(source, state);
      rows.push(doc, impact);
      previous = doc;
    }
  }
  return Int32Array.from(rows);
}

function decodeImpactBitsetBytes(bytes) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const state = { pos: 0 };
  const rows = [];
  const groupCount = readVarint(source, state);
  for (let group = 0; group < groupCount; group++) {
    const impact = readVarint(source, state);
    const minDoc = readVarint(source, state);
    const span = readVarint(source, state);
    const byteLength = readVarint(source, state);
    const start = state.pos;
    state.pos += byteLength;
    for (let bit = 0; bit < span; bit++) {
      if (source[start + Math.floor(bit / 8)] & (1 << (bit % 8))) rows.push(minDoc + bit, impact);
    }
  }
  return Int32Array.from(rows);
}

function decodePartitionedDeltaBytes(bytes) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const state = { pos: 0 };
  const rows = [];
  const groupCount = readVarint(source, state);
  for (let group = 0; group < groupCount; group++) {
    const impact = readVarint(source, state);
    const count = readVarint(source, state);
    let doc = readVarint(source, state);
    const width = readVarint(source, state);
    const byteLength = readVarint(source, state);
    rows.push(doc, impact);
    for (const delta of readPackedUnsigned(source, state, Math.max(0, count - 1), width, byteLength)) {
      doc += delta;
      rows.push(doc, impact);
    }
  }
  return Int32Array.from(rows);
}

function postingTargetSet(docs) {
  return docs instanceof Set ? docs : new Set(docs || []);
}

function sortedPostingTargets(targets) {
  return [...targets].filter(Number.isFinite).sort((a, b) => a - b);
}

function pushMatchedPosting(out, targets, doc, impact) {
  if (targets.has(doc)) out.push(doc, impact);
}

function lookupPairVarintBytes(bytes, targets) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const state = { pos: 0 };
  const rows = [];
  let scanned = 0;
  while (state.pos < source.length) {
    const doc = readVarint(source, state);
    const impact = readVarint(source, state);
    scanned++;
    pushMatchedPosting(rows, targets, doc, impact);
  }
  return { rows: Int32Array.from(rows), scanned };
}

function lookupImpactRunBytes(bytes, targets) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const state = { pos: 0 };
  const rows = [];
  let scanned = 0;
  const groupCount = readVarint(source, state);
  for (let group = 0; group < groupCount; group++) {
    const impact = readVarint(source, state);
    const count = readVarint(source, state);
    let previous = -1;
    for (let i = 0; i < count; i++) {
      const doc = previous + readVarint(source, state);
      scanned++;
      pushMatchedPosting(rows, targets, doc, impact);
      previous = doc;
    }
  }
  return { rows: Int32Array.from(rows), scanned };
}

function lookupImpactBitsetBytes(bytes, targets) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const targetDocs = sortedPostingTargets(targets);
  const state = { pos: 0 };
  const rows = [];
  let scanned = 0;
  const groupCount = readVarint(source, state);
  for (let group = 0; group < groupCount; group++) {
    const impact = readVarint(source, state);
    const minDoc = readVarint(source, state);
    const span = readVarint(source, state);
    const byteLength = readVarint(source, state);
    const start = state.pos;
    state.pos += byteLength;
    const maxDoc = minDoc + span - 1;
    for (const doc of targetDocs) {
      if (doc < minDoc) continue;
      if (doc > maxDoc) break;
      scanned++;
      const bit = doc - minDoc;
      if (source[start + Math.floor(bit / 8)] & (1 << (bit % 8))) rows.push(doc, impact);
    }
  }
  return { rows: Int32Array.from(rows), scanned };
}

function lookupPartitionedDeltaBytes(bytes, targets) {
  const source = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const state = { pos: 0 };
  const rows = [];
  let scanned = 0;
  const groupCount = readVarint(source, state);
  for (let group = 0; group < groupCount; group++) {
    const impact = readVarint(source, state);
    const count = readVarint(source, state);
    let doc = readVarint(source, state);
    const width = readVarint(source, state);
    const byteLength = readVarint(source, state);
    scanned++;
    pushMatchedPosting(rows, targets, doc, impact);
    for (const delta of readPackedUnsigned(source, state, Math.max(0, count - 1), width, byteLength)) {
      doc += delta;
      scanned++;
      pushMatchedPosting(rows, targets, doc, impact);
    }
  }
  return { rows: Int32Array.from(rows), scanned };
}

export function decodePostingBytes(bytes, block = null) {
  const codec = block?.codec || POSTING_BLOCK_CODEC_PAIR_VARINT;
  if (codec === POSTING_BLOCK_CODEC_PARTITIONED_DELTAS) return decodePartitionedDeltaBytes(bytes);
  if (codec === POSTING_BLOCK_CODEC_IMPACT_BITSET) return decodeImpactBitsetBytes(bytes);
  if (codec === POSTING_BLOCK_CODEC_IMPACT_RUNS) return decodeImpactRunBytes(bytes);
  return decodePairVarintBytes(bytes);
}

export function lookupPostingBytes(bytes, docs, block = null) {
  const targets = postingTargetSet(docs);
  if (!targets.size) return { rows: new Int32Array(0), scanned: 0 };
  const codec = block?.codec || POSTING_BLOCK_CODEC_PAIR_VARINT;
  if (codec === POSTING_BLOCK_CODEC_PARTITIONED_DELTAS) return lookupPartitionedDeltaBytes(bytes, targets);
  if (codec === POSTING_BLOCK_CODEC_IMPACT_BITSET) return lookupImpactBitsetBytes(bytes, targets);
  if (codec === POSTING_BLOCK_CODEC_IMPACT_RUNS) return lookupImpactRunBytes(bytes, targets);
  return lookupPairVarintBytes(bytes, targets);
}

export function lookupDecodedPostingRows(rows, docs) {
  const targets = postingTargetSet(docs);
  const out = [];
  let scanned = 0;
  for (let i = 0; i < (rows?.length || 0); i += 2) {
    scanned++;
    pushMatchedPosting(out, targets, rows[i], rows[i + 1]);
  }
  return { rows: Int32Array.from(out), scanned };
}

export function lookupPostingBlock(shard, entry, blockIndex, docs) {
  const block = entry.blocks?.[blockIndex];
  if (!block) return { rows: new Int32Array(0), scanned: 0 };
  if (entry.external) throw new Error("External posting blocks require async runtime loading.");
  const next = entry.blocks[blockIndex + 1];
  const end = shard.dataStart + entry.offset + (next ? next.offset : entry.byteLength);
  const start = shard.dataStart + entry.offset + block.offset;
  return lookupPostingBytes(shard.bytes.subarray(start, end), docs, block);
}

export function decodePostingBlock(shard, entry, blockIndex) {
  const block = entry.blocks?.[blockIndex];
  if (!block) return new Int32Array(0);
  if (entry.external) throw new Error("External posting blocks require async runtime loading.");
  if (!entry.blockPostings) entry.blockPostings = new Map();
  if (entry.blockPostings.has(blockIndex)) return entry.blockPostings.get(blockIndex);
  const next = entry.blocks[blockIndex + 1];
  const end = shard.dataStart + entry.offset + (next ? next.offset : entry.byteLength);
  const start = shard.dataStart + entry.offset + block.offset;
  const out = decodePostingBytes(shard.bytes.subarray(start, end), block);
  entry.blockPostings.set(blockIndex, out);
  return out;
}

function facetWords(value, words) {
  const codes = facetCodes(value);
  if (codes) {
    const out = new Array(words).fill(0);
    for (const item of codes) addBit(out, Number(item) || 0);
    return out;
  }
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
  if (Array.isArray(codes._fields)) return codes._fields.map(field => ({ ...field }));
  return [
    ...config.facets.map(facet => ({
      name: facet.name,
      kind: "facet",
      type: "keyword",
      encoding: "sparse-set",
      words: Math.max(1, Math.ceil(((codes._dicts?.[facet.name]?.values?.length) || 1) / 32))
    })),
    ...config.numbers.map(number => ({ name: number.name, kind: "number", type: normalizedNumberType(number), words: 0 })),
    ...(config.booleans || []).map(boolean => ({ name: boolean.name, kind: "boolean", type: "boolean", words: 0 }))
  ];
}

function summarizeFacetRows(rows, words) {
  if (words > MAX_SUMMARY_FACET_WORDS) return { words: null };
  const summary = new Array(words).fill(0);
  for (const value of rows) {
    const row = facetWords(value, words);
    for (let word = 0; word < words; word++) summary[word] |= row[word] || 0;
  }
  return { words: summary };
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
    const offsets = [];
    const data = [];
    for (const row of rows) {
      offsets.push(data.length);
      const codes = [...new Set(facetCodes(row) || [])].sort((a, b) => a - b);
      pushVarint(data, codes.length);
      let previous = 0;
      for (const code of codes) {
        pushVarint(data, code - previous);
        previous = code;
      }
    }
    offsets.push(data.length);
    const width = fixedWidth(offsets);
    const offsetBytes = Buffer.alloc(offsets.length * width);
    for (let i = 0; i < offsets.length; i++) writeFixedInt(offsetBytes, i * width, width, offsets[i]);
    return {
      chunk: Buffer.concat([offsetBytes, Buffer.from(Uint8Array.from(data))]),
      width,
      min: 0,
      encoding: DOC_VALUE_ENCODING_SPARSE_FACET,
      summary: summarizeFacetRows(rows, field.words)
    };
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
  header.push(encoded.encoding || DOC_VALUE_ENCODING_DENSE);
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
  if (version !== 1 && version !== DOC_VALUE_FORMAT_VERSION) throw new Error(`Unsupported Rangefind doc-value chunk version ${version}`);
  const field = {
    name: readUtf8(bytes, state),
    kind: CODE_KIND_NAME[bytes[state.pos++]] || "number",
    type: CODE_TYPE_NAME[bytes[state.pos++]] || "int",
    width: bytes[state.pos++],
    words: readVarint(bytes, state),
    encoding: version >= 2 ? bytes[state.pos++] : DOC_VALUE_ENCODING_DENSE,
    start: readVarint(bytes, state),
    count: readVarint(bytes, state),
    min: readFloat64(bytes, state)
  };
  const values = new Array(field.count);
  if (field.kind === "facet" && field.encoding === DOC_VALUE_ENCODING_SPARSE_FACET) {
    const offsets = new Array(field.count + 1);
    for (let doc = 0; doc <= field.count; doc++) offsets[doc] = readFixedInt(bytes, state.pos + doc * field.width, field.width);
    const dataStart = state.pos + offsets.length * field.width;
    for (let doc = 0; doc < field.count; doc++) {
      const cursor = { pos: dataStart + offsets[doc] };
      const end = dataStart + offsets[doc + 1];
      const codeCount = readVarint(bytes, cursor);
      const codes = [];
      let previous = 0;
      for (let i = 0; i < codeCount && cursor.pos <= end; i++) {
        previous += readVarint(bytes, cursor);
        codes.push(previous);
      }
      values[doc] = { codes };
    }
    state.pos = dataStart + offsets[field.count];
  } else if (field.kind === "facet") {
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
