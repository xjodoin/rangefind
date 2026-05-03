import { closeSync, createReadStream, openSync, readSync, writeSync } from "node:fs";
import { createPostingRowBuffer, appendPostingRow, postingRowCount, postingRowDoc, postingRowScore } from "./posting_rows.js";
import { tryReadVarint, varintLength, writeVarint } from "./runs.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const DEFAULT_RANGE_READ_BYTES = 1024 * 1024;

export function encodeReducedTerm(term, rows, df = postingRowCount(rows)) {
  const termBytes = textEncoder.encode(String(term || ""));
  const rowCount = postingRowCount(rows);
  let bytes = varintLength(termBytes.length) + termBytes.length + varintLength(df) + varintLength(rowCount);
  for (let i = 0; i < rowCount; i++) {
    bytes += varintLength(postingRowDoc(rows, i)) + varintLength(postingRowScore(rows, i));
  }
  const out = Buffer.allocUnsafe(bytes);
  let pos = writeVarint(out, 0, termBytes.length);
  out.set(termBytes, pos);
  pos += termBytes.length;
  pos = writeVarint(out, pos, df);
  pos = writeVarint(out, pos, rowCount);
  for (let i = 0; i < rowCount; i++) {
    pos = writeVarint(out, pos, postingRowDoc(rows, i));
    pos = writeVarint(out, pos, postingRowScore(rows, i));
  }
  return out;
}

function reducedTermFromBytes(bytes, state, options = {}) {
  const includeRows = options.includeRows !== false;
  const start = state.pos;
  const termLength = tryReadVarint(bytes, state);
  if (termLength == null || state.pos + termLength > bytes.length) {
    state.pos = start;
    return null;
  }
  const term = textDecoder.decode(bytes.subarray(state.pos, state.pos + termLength));
  state.pos += termLength;
  const df = tryReadVarint(bytes, state);
  if (df == null) {
    state.pos = start;
    return null;
  }
  const rowCount = tryReadVarint(bytes, state);
  if (rowCount == null) {
    state.pos = start;
    return null;
  }
  const rows = includeRows ? createPostingRowBuffer(rowCount) : null;
  for (let i = 0; i < rowCount; i++) {
    const doc = tryReadVarint(bytes, state);
    const score = tryReadVarint(bytes, state);
    if (doc == null || score == null) {
      state.pos = start;
      return null;
    }
    if (includeRows) appendPostingRow(rows, doc, score);
  }
  return includeRows ? { term, df, rows } : { term, df, rowCount };
}

export async function* readReducedTermRanges(path, options = {}) {
  let pending = Buffer.alloc(0);
  let pendingOffset = 0;
  let fileOffset = 0;
  for await (const chunk of createReadStream(path)) {
    const chunkOffset = fileOffset;
    fileOffset += chunk.length;
    const bytes = pending.length ? Buffer.concat([pending, chunk]) : chunk;
    const baseOffset = pending.length ? pendingOffset : chunkOffset;
    const state = { pos: 0 };
    while (state.pos < bytes.length) {
      const start = state.pos;
      const item = reducedTermFromBytes(bytes, state, options);
      if (!item) break;
      yield { ...item, offset: baseOffset + start, length: state.pos - start };
    }
    if (state.pos < bytes.length) {
      pending = bytes.subarray(state.pos);
      pendingOffset = baseOffset + state.pos;
    } else {
      pending = Buffer.alloc(0);
      pendingOffset = baseOffset + bytes.length;
    }
  }
  if (pending.length) throw new Error(`Truncated Rangefind reduced term file: ${path}`);
}

export async function* readReducedTerms(path) {
  for await (const { term, df, rows } of readReducedTermRanges(path)) yield { term, df, rows };
}

export function* readReducedTermsFromRangeSync(path, offset = 0, length = Infinity, options = {}) {
  const fd = openSync(path, "r");
  const chunkBytes = Math.max(1024, Math.floor(Number(options.chunkBytes || DEFAULT_RANGE_READ_BYTES)));
  let cursor = 0;
  let pending = Buffer.alloc(0);
  try {
    while (cursor < length) {
      const remaining = Number.isFinite(length) ? length - cursor : chunkBytes;
      const target = Math.min(chunkBytes, remaining);
      if (target <= 0) break;
      const chunk = Buffer.allocUnsafe(target);
      const bytesRead = readSync(fd, chunk, 0, target, offset + cursor);
      if (bytesRead <= 0) {
        if (Number.isFinite(length) && cursor < length) {
          throw new Error(`Truncated Rangefind reduced term range: ${path}:${offset}+${length}`);
        }
        break;
      }
      cursor += bytesRead;
      const body = chunk.subarray(0, bytesRead);
      const bytes = pending.length ? Buffer.concat([pending, body]) : body;
      const state = { pos: 0 };
      while (state.pos < bytes.length) {
        const item = reducedTermFromBytes(bytes, state, options);
        if (!item) break;
        yield item;
      }
      pending = state.pos < bytes.length ? bytes.subarray(state.pos) : Buffer.alloc(0);
    }
    if (pending.length) throw new Error(`Truncated Rangefind reduced term range: ${path}:${offset}+${length}`);
  } finally {
    closeSync(fd);
  }
}

export function writeReducedTerm(fd, term, rows, df = postingRowCount(rows)) {
  const encoded = encodeReducedTerm(term, rows, df);
  writeSync(fd, encoded, 0, encoded.length);
  return encoded.length;
}

export function partitionInputBytes(partition) {
  if (!partition) return 0;
  if (Number.isFinite(partition.inputBytes)) return Math.max(0, Math.floor(partition.inputBytes));
  if (Number.isFinite(partition.length)) return Math.max(0, Math.floor(partition.length));
  if (Array.isArray(partition.entries)) {
    return partition.entries.reduce((sum, [term, rows]) => sum + String(term).length + postingRowCount(rows) * 8, 0);
  }
  return 0;
}

export function partitionTermCount(partition) {
  if (!partition) return 0;
  if (Number.isFinite(partition.terms)) return Math.max(0, Math.floor(partition.terms));
  return partition.entries?.length || 0;
}

export function partitionRowCount(partition) {
  if (!partition) return 0;
  if (Number.isFinite(partition.rows)) return Math.max(0, Math.floor(partition.rows));
  return partition.entries?.reduce((sum, [, rows]) => sum + postingRowCount(rows), 0) || 0;
}

export function* partitionTermEntries(partition, options = {}) {
  if (Array.isArray(partition?.entries)) {
    yield* partition.entries;
    return;
  }
  if (!partition?.path) throw new Error("Rangefind reducer partition is missing a binary spool descriptor.");
  for (const { term, rows } of readReducedTermsFromRangeSync(partition.path, partition.offset || 0, partition.length || 0, options)) {
    yield [term, rows];
  }
}
