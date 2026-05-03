import { createHash } from "node:crypto";
import { closeSync, mkdirSync, openSync, readFileSync, readSync, writeFileSync, writeSync } from "node:fs";
import { resolve } from "node:path";
import { appendPostingRow, isPostingRowBuffer, postingRowCount, postingRowDoc, postingRowScore } from "./posting_rows.js";
import { tryReadVarint, varintLength, writeVarint } from "./runs.js";

const TERMS_MAGIC = Uint8Array.from([0x52, 0x46, 0x53, 0x47, 0x54, 0x45, 0x52, 0x31]);
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function pushVarint(out, value) {
  const tmp = Buffer.allocUnsafe(varintLength(value));
  writeVarint(tmp, 0, value);
  out.push(...tmp);
}

function pushUtf8(out, value) {
  const bytes = textEncoder.encode(String(value || ""));
  pushVarint(out, bytes.length);
  out.push(...bytes);
}

function readVarint(bytes, state) {
  const value = tryReadVarint(bytes, state);
  if (value == null) throw new Error("Truncated Rangefind segment varint.");
  return value;
}

function readUtf8(bytes, state) {
  const length = readVarint(bytes, state);
  if (state.pos + length > bytes.length) throw new Error("Truncated Rangefind segment string.");
  const value = textDecoder.decode(bytes.subarray(state.pos, state.pos + length));
  state.pos += length;
  return value;
}

function assertMagic(bytes, magic, label) {
  for (let i = 0; i < magic.length; i++) {
    if (bytes[i] !== magic[i]) throw new Error(label);
  }
}

function normalizedScore(score) {
  return Math.max(1, Math.round(Number(score) || 0));
}

function positiveInteger(value, fallback = 0) {
  const numeric = Math.floor(Number(value) || 0);
  return numeric > 0 ? numeric : fallback;
}

function resolvedMaxBytes(config) {
  const flushBytes = positiveInteger(config.segmentFlushBytes, 0);
  if (flushBytes) return Math.max(1024, flushBytes);
  const budget = positiveInteger(config.builderMemoryBudgetBytes, 0);
  if (budget) {
    const workers = Math.max(1, positiveInteger(config.scanWorkers, 1));
    return Math.max(1024 * 1024, Math.floor(budget / workers / 2));
  }
  const explicit = positiveInteger(config.segmentMaxBytes, 0);
  if (explicit) return Math.max(1024, explicit);
  return 64 * 1024 * 1024;
}

function finishRows(rows) {
  if (isPostingRowBuffer(rows)) return rows;
  rows.sort((a, b) => a[0] - b[0]);
  const out = [];
  for (const [doc, score] of rows) {
    const last = out[out.length - 1];
    if (last && last[0] === doc) last[1] += normalizedScore(score);
    else out.push([doc, normalizedScore(score)]);
  }
  return out;
}

function encodeRows(rows) {
  const rowBytes = [];
  let docMin = null;
  let docMax = null;
  const count = postingRowCount(rows);
  for (let i = 0; i < count; i++) {
    const doc = postingRowDoc(rows, i);
    const score = postingRowScore(rows, i);
    pushVarint(rowBytes, doc);
    pushVarint(rowBytes, score);
    docMin = docMin == null ? doc : Math.min(docMin, doc);
    docMax = docMax == null ? doc : Math.max(docMax, doc);
  }
  return { buffer: Buffer.from(Uint8Array.from(rowBytes)), docMin, docMax };
}

function writeSegmentTermsFile(termsPath, terms) {
  const termBytes = [...TERMS_MAGIC];
  pushVarint(termBytes, terms.length);
  for (const item of terms) {
    pushUtf8(termBytes, item.term);
    pushVarint(termBytes, item.offset);
    pushVarint(termBytes, item.bytes);
    pushVarint(termBytes, item.df);
    pushVarint(termBytes, item.count);
  }
  const buffer = Buffer.from(Uint8Array.from(termBytes));
  writeFileSync(termsPath, buffer);
  return {
    bytes: buffer.length,
    checksum: checksum(buffer)
  };
}

function checksum(bytes) {
  return {
    algorithm: "sha256",
    value: createHash("sha256").update(bytes).digest("hex")
  };
}

function segmentMeta(id, terms, postingCount, termsBytes, postingBytes, docMin, docMax, extra = {}) {
  return {
    format: "rfsegment-v1",
    id,
    docBase: docMin ?? 0,
    docCount: docMax == null || docMin == null ? 0 : docMax - docMin + 1,
    termCount: terms.length,
    postingCount,
    termsBytes,
    postingBytes,
    terms: "terms.bin",
    postings: "postings.bin",
    files: {
      terms: {
        path: "terms.bin",
        bytes: termsBytes,
        checksum: extra.termsChecksum || null
      },
      postings: {
        path: "postings.bin",
        bytes: postingBytes,
        checksum: extra.postingsChecksum || null
      }
    },
    heads: null,
    summaries: null,
    ...extra
  };
}

export function createSegmentBuilder(outDir, config = {}) {
  mkdirSync(outDir, { recursive: true });
  return {
    outDir,
    config,
    nextId: 0,
    postings: new Map(),
    postingCount: 0,
    approxBytes: 0,
    docMin: null,
    docMax: null,
    docCount: 0,
    lastDoc: null,
    flushCounts: {},
    segments: [],
    maxPostings: Math.max(1, Math.floor(Number(config.segmentMaxPostings || 250000))),
    maxDocs: positiveInteger(config.segmentFlushDocs, 0) || positiveInteger(config.segmentMaxDocs, 0),
    maxBytes: resolvedMaxBytes(config),
    memoryBudgetBytes: positiveInteger(config.builderMemoryBudgetBytes, 0),
    pendingFlushReason: ""
  };
}

export function addSegmentPosting(builder, term, doc, score) {
  if (!term) return;
  if (builder.lastDoc !== doc) {
    builder.lastDoc = doc;
    builder.docCount++;
  }
  const key = String(term);
  let rows = builder.postings.get(key);
  if (!rows) {
    rows = [];
    builder.postings.set(key, rows);
    builder.approxBytes += key.length + 48;
  }
  rows.push([doc, normalizedScore(score)]);
  builder.postingCount++;
  builder.approxBytes += key.length + 16;
  builder.docMin = builder.docMin == null ? doc : Math.min(builder.docMin, doc);
  builder.docMax = builder.docMax == null ? doc : Math.max(builder.docMax, doc);
}

export function segmentFlushReason(builder) {
  if (builder.postingCount >= builder.maxPostings) return "postings";
  if (builder.maxDocs && builder.docCount >= builder.maxDocs) return "docs";
  if (builder.approxBytes >= builder.maxBytes) {
    if (builder.docCount <= 1) return "single-doc-bytes";
    return "bytes";
  }
  return "";
}

export function shouldFlushSegment(builder) {
  const reason = segmentFlushReason(builder);
  builder.pendingFlushReason = reason;
  return !!reason;
}

export function flushSegment(builder, reason = builder.pendingFlushReason || "finish") {
  if (!builder.postingCount) return null;
  if (reason === "single-doc-bytes") {
    throw new Error(`Rangefind segment flush limit exceeded by one document: approx ${builder.approxBytes} bytes > ${builder.maxBytes} bytes.`);
  }
  const id = `segment-${String(builder.nextId++).padStart(6, "0")}`;
  const dir = resolve(builder.outDir, id);
  mkdirSync(dir, { recursive: true });
  const postingsPath = resolve(dir, "postings.bin");
  const termsPath = resolve(dir, "terms.bin");
  const postingsFd = openSync(postingsPath, "w");
  const terms = [];
  const postingsHash = createHash("sha256");
  let postingOffset = 0;
  let postingCount = 0;
  let docMin = null;
  let docMax = null;
  try {
    for (const [term, sourceRows] of [...builder.postings.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
      const rows = finishRows(sourceRows);
      const { buffer, docMin: termDocMin, docMax: termDocMax } = encodeRows(rows);
      writeSync(postingsFd, buffer, 0, buffer.length, postingOffset);
      postingsHash.update(buffer);
      terms.push({ term, offset: postingOffset, bytes: buffer.length, df: rows.length, count: rows.length });
      postingOffset += buffer.length;
      postingCount += rows.length;
      docMin = termDocMin == null ? docMin : docMin == null ? termDocMin : Math.min(docMin, termDocMin);
      docMax = termDocMax == null ? docMax : docMax == null ? termDocMax : Math.max(docMax, termDocMax);
    }
  } finally {
    closeSync(postingsFd);
  }

  const termsFile = writeSegmentTermsFile(termsPath, terms);
  const meta = segmentMeta(id, terms, postingCount, termsFile.bytes, postingOffset, docMin, docMax, {
    flushReason: reason,
    approxBytes: builder.approxBytes,
    approxMemoryBytes: builder.approxBytes,
    sourceDocCount: builder.docCount,
    memoryBudgetBytes: builder.memoryBudgetBytes,
    maxPostings: builder.maxPostings,
    maxDocs: builder.maxDocs,
    maxBytes: builder.maxBytes,
    termsChecksum: termsFile.checksum,
    postingsChecksum: {
      algorithm: "sha256",
      value: postingsHash.digest("hex")
    }
  });
  const metaPath = resolve(dir, "segment.json");
  writeFileSync(metaPath, JSON.stringify(meta));
  const segment = { ...meta, dir, metaPath, termsPath, postingsPath };
  builder.segments.push(segment);
  builder.postings.clear();
  builder.postingCount = 0;
  builder.approxBytes = 0;
  builder.docMin = null;
  builder.docMax = null;
  builder.docCount = 0;
  builder.lastDoc = null;
  builder.pendingFlushReason = "";
  builder.flushCounts[reason] = (builder.flushCounts[reason] || 0) + 1;
  return segment;
}

export async function writeSegmentFromTermRows(outDir, id, termRows, extraMeta = {}) {
  const dir = resolve(outDir, id);
  mkdirSync(dir, { recursive: true });
  const postingsPath = resolve(dir, "postings.bin");
  const termsPath = resolve(dir, "terms.bin");
  const postingsFd = openSync(postingsPath, "w");
  const terms = [];
  const postingsHash = createHash("sha256");
  let postingOffset = 0;
  let postingCount = 0;
  let docMin = null;
  let docMax = null;
  try {
    for await (const item of termRows) {
      const term = Array.isArray(item) ? item[0] : item.term;
      const sourceRows = Array.isArray(item) ? item[1] : item.rows;
      if (!term || !sourceRows?.length) continue;
      const rows = finishRows(sourceRows);
      const { buffer, docMin: termDocMin, docMax: termDocMax } = encodeRows(rows);
      writeSync(postingsFd, buffer, 0, buffer.length, postingOffset);
      postingsHash.update(buffer);
      terms.push({ term, offset: postingOffset, bytes: buffer.length, df: rows.length, count: rows.length });
      postingOffset += buffer.length;
      postingCount += rows.length;
      docMin = termDocMin == null ? docMin : docMin == null ? termDocMin : Math.min(docMin, termDocMin);
      docMax = termDocMax == null ? docMax : docMax == null ? termDocMax : Math.max(docMax, termDocMax);
    }
  } finally {
    closeSync(postingsFd);
  }

  const termsFile = writeSegmentTermsFile(termsPath, terms);
  const meta = segmentMeta(id, terms, postingCount, termsFile.bytes, postingOffset, docMin, docMax, {
    flushReason: extraMeta.flushReason || "merge",
    approxBytes: (extraMeta.approxBytes || 0) || termsFile.bytes + postingOffset,
    approxMemoryBytes: (extraMeta.approxMemoryBytes || 0) || termsFile.bytes + postingOffset,
    ...extraMeta,
    termsChecksum: termsFile.checksum,
    postingsChecksum: {
      algorithm: "sha256",
      value: postingsHash.digest("hex")
    }
  });
  const metaPath = resolve(dir, "segment.json");
  writeFileSync(metaPath, JSON.stringify(meta));
  return { ...meta, dir, metaPath, termsPath, postingsPath };
}

export function finishSegmentBuilder(builder) {
  flushSegment(builder);
  return builder.segments.slice();
}

export function readSegmentTerms(segment) {
  const directory = readSegmentDirectory(segment);
  return { ...directory, postingsBytes: readFileSync(directory.postingsPath) };
}

export function readSegmentDirectory(segment) {
  const dir = segment.dir || segment.path || segment;
  const meta = typeof segment === "string"
    ? JSON.parse(readFileSync(resolve(segment, "segment.json"), "utf8"))
    : segment;
  const termsBytes = readFileSync(resolve(dir, meta.terms || "terms.bin"));
  const postingsPath = resolve(dir, meta.postings || "postings.bin");
  assertMagic(termsBytes, TERMS_MAGIC, "Unsupported Rangefind segment terms file.");
  const state = { pos: TERMS_MAGIC.length };
  const count = readVarint(termsBytes, state);
  const terms = new Array(count);
  for (let i = 0; i < count; i++) {
    terms[i] = {
      term: readUtf8(termsBytes, state),
      offset: readVarint(termsBytes, state),
      bytes: readVarint(termsBytes, state),
      df: readVarint(termsBytes, state),
      count: readVarint(termsBytes, state)
    };
  }
  return { meta, terms, postingsPath };
}

export function readSegmentRows(segmentData, entry) {
  const state = { pos: entry.offset };
  const end = entry.offset + entry.bytes;
  const rows = new Array(entry.count);
  for (let i = 0; i < rows.length; i++) {
    if (state.pos >= end) throw new Error(`Rangefind segment term ${entry.term} ended early.`);
    rows[i] = [readVarint(segmentData.postingsBytes, state), readVarint(segmentData.postingsBytes, state)];
  }
  if (state.pos !== end) throw new Error(`Rangefind segment term ${entry.term} has trailing bytes.`);
  return rows;
}

export function readSegmentRowsFromBytesInto(postingsBytes, entry, target) {
  const state = { pos: entry.offset };
  const end = entry.offset + entry.bytes;
  for (let i = 0; i < entry.count; i++) {
    if (state.pos >= end) throw new Error(`Rangefind segment term ${entry.term} ended early.`);
    appendPostingRow(target, readVarint(postingsBytes, state), readVarint(postingsBytes, state));
  }
  if (state.pos !== end) throw new Error(`Rangefind segment term ${entry.term} has trailing bytes.`);
  return target;
}

export function readSegmentRowsFromFd(fd, entry) {
  const bytes = Buffer.alloc(entry.bytes);
  const bytesRead = readSync(fd, bytes, 0, bytes.length, entry.offset);
  if (bytesRead !== bytes.length) throw new Error(`Rangefind segment term ${entry.term} ended early.`);
  return readSegmentRows({ postingsBytes: bytes }, { ...entry, offset: 0 });
}

export function readSegmentRowsFromFdInto(fd, entry, target, reusableBuffer = null) {
  const buffer = reusableBuffer && reusableBuffer.length >= entry.bytes
    ? reusableBuffer
    : Buffer.allocUnsafe(entry.bytes);
  const bytes = buffer.subarray(0, entry.bytes);
  const bytesRead = readSync(fd, bytes, 0, entry.bytes, entry.offset);
  if (bytesRead !== entry.bytes) throw new Error(`Rangefind segment term ${entry.term} ended early.`);
  readSegmentRowsFromBytesInto(bytes, { ...entry, offset: 0 }, target);
  return buffer;
}
