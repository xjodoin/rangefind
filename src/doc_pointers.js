import { DOC_ORDINAL_TABLE_MAGIC, DOC_POINTER_PAGE_MAGIC, fixedWidth, pushVarint, readFixedInt, readVarint, writeFixedInt } from "./binary.js";
import { assertMagic } from "./codec.js";

export const DOC_POINTER_FORMAT = "rfdocptr-v1";
export const DOC_ORDINAL_FORMAT = "rfdocord-v1";
const DOC_POINTER_VERSION = 1;
const DOC_ORDINAL_VERSION = 1;
const SHA256_BYTES = 32;
const SHA256_HEX = /^[0-9a-f]{64}$/iu;

function checksumToBytes(checksum) {
  const value = checksum?.value || "";
  if (!SHA256_HEX.test(value)) throw new Error("Rangefind doc pointer requires a SHA-256 checksum.");
  const out = new Uint8Array(SHA256_BYTES);
  for (let i = 0; i < SHA256_BYTES; i++) {
    out[i] = Number.parseInt(value.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function bytesToHex(bytes, offset) {
  let out = "";
  for (let i = 0; i < SHA256_BYTES; i++) {
    out += bytes[offset + i].toString(16).padStart(2, "0");
  }
  return out;
}

function headerBytes(meta, magic = DOC_POINTER_PAGE_MAGIC) {
  const out = [...magic];
  pushVarint(out, meta.version || DOC_POINTER_VERSION);
  pushVarint(out, meta.count);
  pushVarint(out, meta.widths.pack);
  pushVarint(out, meta.widths.offset);
  pushVarint(out, meta.widths.length);
  pushVarint(out, meta.widths.logicalLength);
  pushVarint(out, SHA256_BYTES);
  pushVarint(out, meta.recordBytes);
  return Uint8Array.from(out);
}

export function buildDocPointerTable(entries, packIndexes, options = {}) {
  const packValues = entries.map((entry) => {
    const packIndex = packIndexes.get(entry.pack);
    if (!Number.isFinite(packIndex)) throw new Error(`Rangefind doc pointer references unknown pack ${entry.pack}.`);
    return packIndex;
  });
  const widths = {
    pack: fixedWidth(packValues),
    offset: fixedWidth(entries.map(entry => entry.offset)),
    length: fixedWidth(entries.map(entry => entry.length)),
    logicalLength: fixedWidth(entries.map(entry => entry.logicalLength || 0))
  };
  const recordBytes = widths.pack + widths.offset + widths.length + widths.logicalLength + SHA256_BYTES;
  const meta = {
    format: options.format || DOC_POINTER_FORMAT,
    version: options.version || DOC_POINTER_VERSION,
    count: entries.length,
    checksum_bytes: SHA256_BYTES,
    recordBytes,
    widths
  };
  const header = headerBytes(meta, options.magic || DOC_POINTER_PAGE_MAGIC);
  meta.dataOffset = header.length;
  const buffer = Buffer.alloc(meta.dataOffset + entries.length * recordBytes);
  buffer.set(header, 0);
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    let offset = meta.dataOffset + i * recordBytes;
    writeFixedInt(buffer, offset, widths.pack, packValues[i]);
    offset += widths.pack;
    writeFixedInt(buffer, offset, widths.offset, entry.offset);
    offset += widths.offset;
    writeFixedInt(buffer, offset, widths.length, entry.length);
    offset += widths.length;
    writeFixedInt(buffer, offset, widths.logicalLength, entry.logicalLength || 0);
    offset += widths.logicalLength;
    buffer.set(checksumToBytes(entry.checksum), offset);
  }
  return { buffer, meta };
}

export function parseDocPointerHeader(buffer, options = {}) {
  const bytes = new Uint8Array(buffer);
  const magic = options.magic || DOC_POINTER_PAGE_MAGIC;
  const versionExpected = options.version || DOC_POINTER_VERSION;
  assertMagic(bytes, magic, "Unsupported Rangefind doc pointer table");
  const state = { pos: magic.length };
  const version = readVarint(bytes, state);
  if (version !== versionExpected) throw new Error(`Unsupported Rangefind doc pointer table version ${version}`);
  const count = readVarint(bytes, state);
  const widths = {
    pack: readVarint(bytes, state),
    offset: readVarint(bytes, state),
    length: readVarint(bytes, state),
    logicalLength: readVarint(bytes, state)
  };
  const checksumBytes = readVarint(bytes, state);
  const recordBytes = readVarint(bytes, state);
  if (checksumBytes !== SHA256_BYTES) throw new Error(`Unsupported Rangefind doc pointer checksum width ${checksumBytes}`);
  return {
    format: options.format || DOC_POINTER_FORMAT,
    version,
    count,
    checksum_bytes: checksumBytes,
    recordBytes,
    dataOffset: state.pos,
    widths
  };
}

export function decodeDocPointerRecord(buffer, offset, meta, packTable = []) {
  const bytes = new Uint8Array(buffer);
  let pos = offset;
  const packIndex = readFixedInt(bytes, pos, meta.widths.pack);
  pos += meta.widths.pack;
  const pointerOffset = readFixedInt(bytes, pos, meta.widths.offset);
  pos += meta.widths.offset;
  const length = readFixedInt(bytes, pos, meta.widths.length);
  pos += meta.widths.length;
  const logicalLength = readFixedInt(bytes, pos, meta.widths.logicalLength);
  pos += meta.widths.logicalLength;
  const value = bytesToHex(bytes, pos);
  const pack = packTable[packIndex];
  if (!pack) throw new Error(`Rangefind doc pointer references missing pack table index ${packIndex}.`);
  return {
    pack,
    offset: pointerOffset,
    length,
    physicalLength: length,
    logicalLength: logicalLength || null,
    checksum: { algorithm: "sha256", value }
  };
}

export function parseDocPointerPage(buffer, options = {}) {
  const packTable = options.packTable || options.pack_table || [];
  const meta = parseDocPointerHeader(buffer, options);
  const entries = new Array(meta.count);
  for (let i = 0; i < meta.count; i++) {
    entries[i] = decodeDocPointerRecord(buffer, meta.dataOffset + i * meta.recordBytes, meta, packTable);
  }
  return { ...meta, start: 0, entries };
}

function ordinalHeaderBytes(meta) {
  const out = [...DOC_ORDINAL_TABLE_MAGIC];
  pushVarint(out, DOC_ORDINAL_VERSION);
  pushVarint(out, meta.count);
  pushVarint(out, meta.width);
  return Uint8Array.from(out);
}

export function buildDocOrdinalTable(layoutOrder, total) {
  const ordinals = new Array(total);
  for (let ordinal = 0; ordinal < layoutOrder.length; ordinal++) {
    const doc = layoutOrder[ordinal];
    if (doc < 0 || doc >= total || ordinals[doc] != null) {
      throw new Error(`Rangefind doc ordinal table received invalid document id ${doc}.`);
    }
    ordinals[doc] = ordinal;
  }
  for (let doc = 0; doc < total; doc++) {
    if (ordinals[doc] == null) throw new Error(`Rangefind doc ordinal table is missing document ${doc}.`);
  }
  const width = fixedWidth([Math.max(0, total - 1)]);
  const meta = {
    format: DOC_ORDINAL_FORMAT,
    version: DOC_ORDINAL_VERSION,
    count: total,
    width,
    recordBytes: width
  };
  const header = ordinalHeaderBytes(meta);
  meta.dataOffset = header.length;
  const buffer = Buffer.alloc(meta.dataOffset + total * width);
  buffer.set(header, 0);
  for (let doc = 0; doc < total; doc++) {
    writeFixedInt(buffer, meta.dataOffset + doc * width, width, ordinals[doc]);
  }
  return { buffer, meta };
}

export function parseDocOrdinalHeader(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, DOC_ORDINAL_TABLE_MAGIC, "Unsupported Rangefind doc ordinal table");
  const state = { pos: DOC_ORDINAL_TABLE_MAGIC.length };
  const version = readVarint(bytes, state);
  if (version !== DOC_ORDINAL_VERSION) throw new Error(`Unsupported Rangefind doc ordinal table version ${version}`);
  const count = readVarint(bytes, state);
  const width = readVarint(bytes, state);
  return {
    format: DOC_ORDINAL_FORMAT,
    version,
    count,
    width,
    recordBytes: width,
    dataOffset: state.pos
  };
}

export function decodeDocOrdinalRecord(buffer, offset, meta) {
  return readFixedInt(new Uint8Array(buffer), offset, meta.width);
}

export function parseDocOrdinalTable(buffer) {
  const meta = parseDocOrdinalHeader(buffer);
  const entries = new Array(meta.count);
  for (let i = 0; i < meta.count; i++) {
    entries[i] = decodeDocOrdinalRecord(buffer, meta.dataOffset + i * meta.recordBytes, meta);
  }
  return { ...meta, entries };
}
