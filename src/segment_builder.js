import { closeSync, mkdirSync, openSync, readFileSync, readSync, writeFileSync, writeSync } from "node:fs";
import { resolve } from "node:path";
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

function finishRows(rows) {
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
  for (const [doc, score] of rows) {
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
  writeFileSync(termsPath, Buffer.from(Uint8Array.from(termBytes)));
  return termBytes.length;
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
    segments: [],
    maxPostings: Math.max(1, Math.floor(Number(config.segmentMaxPostings || 250000))),
    maxBytes: Math.max(1024, Math.floor(Number(config.segmentMaxBytes || 64 * 1024 * 1024)))
  };
}

export function addSegmentPosting(builder, term, doc, score) {
  if (!term) return;
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

export function shouldFlushSegment(builder) {
  return builder.postingCount >= builder.maxPostings || builder.approxBytes >= builder.maxBytes;
}

export function flushSegment(builder) {
  if (!builder.postingCount) return null;
  const id = `segment-${String(builder.nextId++).padStart(6, "0")}`;
  const dir = resolve(builder.outDir, id);
  mkdirSync(dir, { recursive: true });
  const postingsPath = resolve(dir, "postings.bin");
  const termsPath = resolve(dir, "terms.bin");
  const postingsFd = openSync(postingsPath, "w");
  const terms = [];
  let postingOffset = 0;
  let postingCount = 0;
  let docMin = null;
  let docMax = null;
  try {
    for (const [term, sourceRows] of [...builder.postings.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
      const rows = finishRows(sourceRows);
      const { buffer, docMin: termDocMin, docMax: termDocMax } = encodeRows(rows);
      writeSync(postingsFd, buffer, 0, buffer.length, postingOffset);
      terms.push({ term, offset: postingOffset, bytes: buffer.length, df: rows.length, count: rows.length });
      postingOffset += buffer.length;
      postingCount += rows.length;
      docMin = termDocMin == null ? docMin : docMin == null ? termDocMin : Math.min(docMin, termDocMin);
      docMax = termDocMax == null ? docMax : docMax == null ? termDocMax : Math.max(docMax, termDocMax);
    }
  } finally {
    closeSync(postingsFd);
  }

  const termsBytes = writeSegmentTermsFile(termsPath, terms);
  const meta = segmentMeta(id, terms, postingCount, termsBytes, postingOffset, docMin, docMax);
  const metaPath = resolve(dir, "segment.json");
  writeFileSync(metaPath, JSON.stringify(meta));
  const segment = { ...meta, dir, metaPath, termsPath, postingsPath };
  builder.segments.push(segment);
  builder.postings.clear();
  builder.postingCount = 0;
  builder.approxBytes = 0;
  builder.docMin = null;
  builder.docMax = null;
  return segment;
}

export async function writeSegmentFromTermRows(outDir, id, termRows, extraMeta = {}) {
  const dir = resolve(outDir, id);
  mkdirSync(dir, { recursive: true });
  const postingsPath = resolve(dir, "postings.bin");
  const termsPath = resolve(dir, "terms.bin");
  const postingsFd = openSync(postingsPath, "w");
  const terms = [];
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
      terms.push({ term, offset: postingOffset, bytes: buffer.length, df: rows.length, count: rows.length });
      postingOffset += buffer.length;
      postingCount += rows.length;
      docMin = termDocMin == null ? docMin : docMin == null ? termDocMin : Math.min(docMin, termDocMin);
      docMax = termDocMax == null ? docMax : docMax == null ? termDocMax : Math.max(docMax, termDocMax);
    }
  } finally {
    closeSync(postingsFd);
  }

  const termsBytes = writeSegmentTermsFile(termsPath, terms);
  const meta = segmentMeta(id, terms, postingCount, termsBytes, postingOffset, docMin, docMax, extraMeta);
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

export function readSegmentRowsFromFd(fd, entry) {
  const bytes = Buffer.alloc(entry.bytes);
  const bytesRead = readSync(fd, bytes, 0, bytes.length, entry.offset);
  if (bytesRead !== bytes.length) throw new Error(`Rangefind segment term ${entry.term} ended early.`);
  return readSegmentRows({ postingsBytes: bytes }, { ...entry, offset: 0 });
}
