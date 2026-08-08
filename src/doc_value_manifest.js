// Binary doc-values manifest (rfdvm-v1).
//
// The JSON doc-values manifest scales with fields × chunks: a planet-scale
// shard (21 fields × 6,914 chunks) inflates to ~46MB of JSON whose decode
// sits ~500ms on the cold critical path, and ~20MB of that is per-chunk
// checksum objects and repeated pack-name strings. This codec stores the
// same information columnar and binary:
//   - pack names once in a table, chunks carry a delta-coded table index;
//   - start/count derived from chunk_size + total (the writer emits strictly
//     regular chunk boundaries), so they are never stored;
//   - offsets delta-coded, min/max as raw float64 behind a presence bitmap,
//     facet word summaries as fixed-width uint32 rows;
//   - sha256 rows live OUTSIDE the manifest in an uncompressed fixed-width
//     sidecar (32 bytes per chunk, addressed by the chunk's emission
//     ordinal) so verifying engines range-read exactly the rows they check
//     while everyone else never downloads incompressible hex.
//
// The parser materializes the exact object shape the v1 JSON manifest
// produced ({fields: {name: {chunks: [{start, count, pack, offset, length,
// width, min, max, words}]}}, packs}), so every runtime consumer works
// unchanged whichever format served it.

import { pushVarint, readVarint } from "./binary.js";
import { assertMagic, pushUtf8, readUtf8 } from "./codec.js";

export const DOC_VALUE_MANIFEST_MAGIC = [0x52, 0x46, 0x56, 0x4d]; // RFVM
export const DOC_VALUE_MANIFEST_FORMAT = "rfdvm-v1";
const FORMAT_VERSION = 1;
const CHECKSUM_ROW_BYTES = 32;

function pushZigzag(out, value) {
  const n = Math.floor(value);
  pushVarint(out, n < 0 ? -2 * n - 1 : 2 * n);
}

function readZigzag(bytes, state) {
  const encoded = readVarint(bytes, state);
  return encoded % 2 === 1 ? -(encoded + 1) / 2 : encoded / 2;
}

function pushFloat64(out, value) {
  const buffer = new ArrayBuffer(8);
  new DataView(buffer).setFloat64(0, Number(value), true);
  const bytes = new Uint8Array(buffer);
  for (let i = 0; i < 8; i++) out.push(bytes[i]);
}

function pushUint32(out, value) {
  let n = Math.max(0, Math.floor(value)) >>> 0;
  for (let i = 0; i < 4; i++) {
    out.push(n & 0xff);
    n >>>= 8;
  }
}

function pushBitmap(out, bits) {
  for (let i = 0; i < bits.length; i += 8) {
    let byte = 0;
    for (let j = 0; j < 8 && i + j < bits.length; j++) {
      if (bits[i + j]) byte |= 1 << j;
    }
    out.push(byte);
  }
}

function readBitmap(bytes, state, count) {
  const bits = new Array(count);
  const byteCount = Math.ceil(count / 8);
  for (let i = 0; i < count; i++) {
    bits[i] = (bytes[state.pos + (i >> 3)] >> (i & 7)) & 1;
  }
  state.pos += byteCount;
  return bits;
}

function encodeChunkSection(out, chunks, packIndexByFile) {
  pushVarint(out, chunks.length);
  let prevPackIndex = 0;
  let prevOffset = 0;
  for (const chunk of chunks) {
    const packIndex = Number.isInteger(chunk.packIndex)
      ? chunk.packIndex
      : packIndexByFile.get(chunk.pack);
    if (!Number.isInteger(packIndex)) {
      throw new Error(`Rangefind doc-value manifest chunk references unknown pack "${chunk.pack}".`);
    }
    pushZigzag(out, packIndex - prevPackIndex);
    pushZigzag(out, chunk.offset - prevOffset);
    pushVarint(out, chunk.length);
    out.push(chunk.width & 0xff);
    prevPackIndex = packIndex;
    prevOffset = chunk.offset;
  }
  const hasMinMax = chunks.some(chunk => chunk.min != null || chunk.max != null);
  const wordsLen = chunks.reduce((max, chunk) => Math.max(max, Array.isArray(chunk.words) ? chunk.words.length : 0), 0);
  out.push((hasMinMax ? 1 : 0) | (wordsLen > 0 ? 2 : 0));
  if (hasMinMax) {
    pushBitmap(out, chunks.map(chunk => (chunk.min != null && chunk.max != null ? 1 : 0)));
    for (const chunk of chunks) {
      if (chunk.min == null || chunk.max == null) continue;
      pushFloat64(out, chunk.min);
      pushFloat64(out, chunk.max);
    }
  }
  if (wordsLen > 0) {
    pushVarint(out, wordsLen);
    pushBitmap(out, chunks.map(chunk => (Array.isArray(chunk.words) ? 1 : 0)));
    for (const chunk of chunks) {
      if (!Array.isArray(chunk.words)) continue;
      for (let i = 0; i < wordsLen; i++) pushUint32(out, chunk.words[i] || 0);
    }
  }
}

function decodeChunkSection(bytes, state, view, { chunkSize, total, packs, nextChecksumRow }) {
  const count = readVarint(bytes, state);
  const chunks = new Array(count);
  let packIndex = 0;
  let offset = 0;
  for (let i = 0; i < count; i++) {
    packIndex += readZigzag(bytes, state);
    offset += readZigzag(bytes, state);
    const length = readVarint(bytes, state);
    const width = bytes[state.pos++];
    const start = i * chunkSize;
    chunks[i] = {
      start,
      count: Math.min(chunkSize, total - start),
      pack: packs[packIndex],
      packIndex,
      offset,
      length,
      width,
      min: null,
      max: null,
      words: null,
      ...(nextChecksumRow ? { checksumRow: nextChecksumRow.row++ } : {})
    };
  }
  const flags = bytes[state.pos++];
  if (flags & 1) {
    const present = readBitmap(bytes, state, count);
    for (let i = 0; i < count; i++) {
      if (!present[i]) continue;
      chunks[i].min = view.getFloat64(state.pos, true);
      chunks[i].max = view.getFloat64(state.pos + 8, true);
      state.pos += 16;
    }
  }
  if (flags & 2) {
    const wordsLen = readVarint(bytes, state);
    const present = readBitmap(bytes, state, count);
    for (let i = 0; i < count; i++) {
      if (!present[i]) continue;
      const words = new Array(wordsLen);
      for (let j = 0; j < wordsLen; j++) {
        words[j] = view.getUint32(state.pos, true);
        state.pos += 4;
      }
      chunks[i].words = words;
    }
  }
  return chunks;
}

// `manifest` is the v1-shaped object writeDocValuePacks produces. `checksums`
// names the sidecar written by encodeDocValueChecksums ("" disables lazy
// checksum rows; verifying engines then fall back to the JSON manifest).
export function encodeDocValueManifest(manifest, { checksumsFile = "", checksumAlgorithm = "" } = {}, total) {
  const out = [...DOC_VALUE_MANIFEST_MAGIC];
  pushVarint(out, FORMAT_VERSION);
  pushVarint(out, manifest.chunk_size);
  pushVarint(out, manifest.lookup_chunk_size || manifest.chunk_size);
  pushVarint(out, total);
  pushUtf8(out, manifest.storage || "");
  pushUtf8(out, manifest.compression || "");
  pushUtf8(out, manifest.format || "");
  pushUtf8(out, checksumsFile);
  pushUtf8(out, checksumsFile ? checksumAlgorithm : "");
  const packs = (manifest.packs || []).map(pack => (typeof pack === "string" ? pack : pack.file));
  const packIndexByFile = new Map(packs.map((file, index) => [file, index]));
  pushVarint(out, packs.length);
  for (const file of packs) pushUtf8(out, file);
  const fields = Object.values(manifest.fields || {});
  pushVarint(out, fields.length);
  for (const field of fields) {
    pushUtf8(out, field.name);
    pushUtf8(out, field.kind || "");
    pushUtf8(out, field.type || "");
    pushVarint(out, field.words || 0);
    out.push(field.lookup_chunks ? 1 : 0);
    encodeChunkSection(out, field.chunks || [], packIndexByFile);
    if (field.lookup_chunks) encodeChunkSection(out, field.lookup_chunks, packIndexByFile);
  }
  return Uint8Array.from(out);
}

// Fixed-width sha256 rows in the same traversal order the encoder emits
// chunk sections (fields in manifest order, main chunks then lookup chunks).
// Uncompressed on purpose: hashes do not compress and rows must stay
// addressable as `ordinal * 32` for range reads.
export function encodeDocValueChecksums(manifest) {
  const rows = [];
  let algorithm = "";
  const collect = chunks => {
    for (const chunk of chunks || []) {
      const checksum = chunk.checksum;
      if (!checksum?.value || !/^[0-9a-f]{64}$/u.test(String(checksum.value))) return false;
      if (!algorithm) algorithm = checksum.algorithm || "sha256";
      else if (algorithm !== (checksum.algorithm || "sha256")) return false;
      rows.push(checksum.value);
    }
    return true;
  };
  for (const field of Object.values(manifest.fields || {})) {
    if (!collect(field.chunks)) return null;
    if (field.lookup_chunks && !collect(field.lookup_chunks)) return null;
  }
  if (!rows.length) return null;
  const buffer = new Uint8Array(rows.length * CHECKSUM_ROW_BYTES);
  rows.forEach((value, row) => {
    for (let i = 0; i < CHECKSUM_ROW_BYTES; i++) {
      buffer[row * CHECKSUM_ROW_BYTES + i] = parseInt(value.slice(i * 2, i * 2 + 2), 16);
    }
  });
  return { buffer, algorithm, rows: rows.length, rowBytes: CHECKSUM_ROW_BYTES };
}

export function parseDocValueManifest(buffer) {
  const bytes = buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
  assertMagic(bytes, DOC_VALUE_MANIFEST_MAGIC, "Unsupported Rangefind doc-value manifest");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const state = { pos: DOC_VALUE_MANIFEST_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== FORMAT_VERSION) {
    throw new Error(`Unsupported Rangefind doc-value manifest version ${version}`);
  }
  const chunkSize = readVarint(bytes, state);
  const lookupChunkSize = readVarint(bytes, state);
  const total = readVarint(bytes, state);
  const storage = readUtf8(bytes, state);
  const compression = readUtf8(bytes, state);
  const format = readUtf8(bytes, state);
  const checksumsFile = readUtf8(bytes, state);
  const checksumAlgorithm = readUtf8(bytes, state);
  const packCount = readVarint(bytes, state);
  const packs = new Array(packCount);
  for (let i = 0; i < packCount; i++) packs[i] = readUtf8(bytes, state);
  const nextChecksumRow = checksumsFile ? { row: 0 } : null;
  const fieldCount = readVarint(bytes, state);
  const fields = {};
  for (let i = 0; i < fieldCount; i++) {
    const name = readUtf8(bytes, state);
    const kind = readUtf8(bytes, state);
    const type = readUtf8(bytes, state);
    const words = readVarint(bytes, state);
    const hasLookup = bytes[state.pos++] === 1;
    const chunks = decodeChunkSection(bytes, state, view, { chunkSize, total, packs, nextChecksumRow });
    const lookupChunks = hasLookup
      ? decodeChunkSection(bytes, state, view, { chunkSize: lookupChunkSize, total, packs, nextChecksumRow })
      : null;
    fields[name] = {
      name,
      kind,
      type,
      words,
      chunks,
      ...(lookupChunks ? { lookup_chunks: lookupChunks } : {})
    };
  }
  if (state.pos !== bytes.length) throw new Error("Rangefind doc-value manifest has trailing bytes.");
  return {
    storage,
    compression,
    format,
    chunk_size: chunkSize,
    lookup_chunk_size: lookupChunkSize,
    total,
    fields,
    packs: packs.map(file => ({ file })),
    ...(checksumsFile
      ? { checksum_rows: { file: checksumsFile, algorithm: checksumAlgorithm, rowBytes: CHECKSUM_ROW_BYTES } }
      : {})
  };
}
