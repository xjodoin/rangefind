import { DOC_POINTER_PAGE_MAGIC, fixedWidth, pushVarint, readFixedInt, readVarint, writeFixedInt } from "./binary.js";
import { assertMagic } from "./codec.js";

export const DOC_POINTER_FORMAT = "rfdocptr-v1";
const DOC_POINTER_VERSION = 1;
const SHA256_BYTES = 32;
const SHA256_HEX = /^[0-9a-f]{64}$/iu;

function checksumToBytes(checksum) {
  const value = checksum?.value || "";
  if (ArrayBuffer.isView(value) && value.byteLength === SHA256_BYTES) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
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

function* pointerEntryBatches(count, readEntry, options, pass) {
  const batchSize = Math.max(1, Math.floor(Number(options.batchSize || 65536)));
  for (let start = 0; start < count; start += batchSize) {
    const expected = Math.min(batchSize, count - start);
    const entries = typeof options.readBatch === "function"
      ? options.readBatch(start, expected, pass)
      : Array.from({ length: expected }, (_, row) => readEntry(start + row));
    if (!Array.isArray(entries) || entries.length !== expected) {
      throw new Error(`Rangefind doc pointer batch at ${start} returned ${entries?.length ?? 0} of ${expected} entries.`);
    }
    yield { start, entries };
  }
}

export function streamDocPointerTableFromReader(count, packIndexes, readEntry, options = {}) {
  if (typeof options.write !== "function") throw new Error("Rangefind streamed doc pointer table needs a write callback.");
  let maxPack = 0;
  let maxOffset = 0;
  let maxLength = 0;
  let maxLogicalLength = 0;
  for (const { entries } of pointerEntryBatches(count, readEntry, options, "measure")) {
    for (const entry of entries) {
      const packIndex = packIndexes.get(entry.pack);
      if (!Number.isFinite(packIndex)) throw new Error(`Rangefind doc pointer references unknown pack ${entry.pack}.`);
      maxPack = Math.max(maxPack, packIndex);
      maxOffset = Math.max(maxOffset, entry.offset || 0);
      maxLength = Math.max(maxLength, entry.length || 0);
      maxLogicalLength = Math.max(maxLogicalLength, entry.logicalLength || 0);
    }
  }
  const widths = {
    pack: fixedWidth([maxPack]),
    offset: fixedWidth([maxOffset]),
    length: fixedWidth([maxLength]),
    logicalLength: fixedWidth([maxLogicalLength])
  };
  const recordBytes = widths.pack + widths.offset + widths.length + widths.logicalLength + SHA256_BYTES;
  const meta = {
    format: options.format || DOC_POINTER_FORMAT,
    version: options.version || DOC_POINTER_VERSION,
    count,
    checksum_bytes: SHA256_BYTES,
    recordBytes,
    widths
  };
  const header = headerBytes(meta, options.magic || DOC_POINTER_PAGE_MAGIC);
  meta.dataOffset = header.length;
  options.write(header);
  for (const { entries } of pointerEntryBatches(count, readEntry, options, "write")) {
    const buffer = Buffer.allocUnsafe(entries.length * recordBytes);
    for (let row = 0; row < entries.length; row++) {
      const entry = entries[row];
      let offset = row * recordBytes;
      writeFixedInt(buffer, offset, widths.pack, packIndexes.get(entry.pack));
      offset += widths.pack;
      writeFixedInt(buffer, offset, widths.offset, entry.offset);
      offset += widths.offset;
      writeFixedInt(buffer, offset, widths.length, entry.length);
      offset += widths.length;
      writeFixedInt(buffer, offset, widths.logicalLength, entry.logicalLength || 0);
      offset += widths.logicalLength;
      buffer.set(checksumToBytes(entry.checksum), offset);
    }
    options.write(buffer);
  }
  return { meta, bytes: meta.dataOffset + count * recordBytes };
}

export function buildDocPointerTableFromReader(count, packIndexes, readEntry, options = {}) {
  const chunks = [];
  const streamed = streamDocPointerTableFromReader(count, packIndexes, readEntry, {
    ...options,
    write: chunk => chunks.push(Buffer.from(chunk))
  });
  return { buffer: Buffer.concat(chunks, streamed.bytes), meta: streamed.meta };
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
