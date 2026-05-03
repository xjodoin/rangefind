import { closeSync, createReadStream, mkdirSync, openSync, rmSync, statSync, writeSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { readSegmentDirectory, readSegmentRowsFromFd, writeSegmentFromTermRows } from "./segment_builder.js";
import { shardKey } from "./shards.js";
import { tryReadVarint, varintLength, writeVarint } from "./runs.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

class MinHeap {
  constructor(compare) {
    this.compare = compare;
    this.items = [];
  }

  push(item) {
    const items = this.items;
    items.push(item);
    let index = items.length - 1;
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (this.compare(items[parent], items[index]) <= 0) break;
      [items[parent], items[index]] = [items[index], items[parent]];
      index = parent;
    }
  }

  pop() {
    const items = this.items;
    if (!items.length) return null;
    const out = items[0];
    const last = items.pop();
    if (items.length) {
      items[0] = last;
      let index = 0;
      while (true) {
        const left = index * 2 + 1;
        const right = left + 1;
        let best = index;
        if (left < items.length && this.compare(items[left], items[best]) < 0) best = left;
        if (right < items.length && this.compare(items[right], items[best]) < 0) best = right;
        if (best === index) break;
        [items[index], items[best]] = [items[best], items[index]];
        index = best;
      }
    }
    return out;
  }

  get size() {
    return this.items.length;
  }
}

function encodeReducedTerm(term, rows, df) {
  const termBytes = textEncoder.encode(String(term || ""));
  let bytes = varintLength(termBytes.length) + termBytes.length + varintLength(df) + varintLength(rows.length);
  for (const [doc, score] of rows) bytes += varintLength(doc) + varintLength(score);
  const out = Buffer.allocUnsafe(bytes);
  let pos = writeVarint(out, 0, termBytes.length);
  out.set(termBytes, pos);
  pos += termBytes.length;
  pos = writeVarint(out, pos, df);
  pos = writeVarint(out, pos, rows.length);
  for (const [doc, score] of rows) {
    pos = writeVarint(out, pos, doc);
    pos = writeVarint(out, pos, score);
  }
  return out;
}

function reducedTermFromBytes(bytes, state) {
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
  const rows = new Array(rowCount);
  for (let i = 0; i < rowCount; i++) {
    const doc = tryReadVarint(bytes, state);
    const score = tryReadVarint(bytes, state);
    if (doc == null || score == null) {
      state.pos = start;
      return null;
    }
    rows[i] = [doc, score];
  }
  return { term, df, rows };
}

async function* readReducedTerms(path) {
  let pending = Buffer.alloc(0);
  for await (const chunk of createReadStream(path)) {
    const bytes = pending.length ? Buffer.concat([pending, chunk]) : chunk;
    const state = { pos: 0 };
    while (state.pos < bytes.length) {
      const item = reducedTermFromBytes(bytes, state);
      if (!item) break;
      yield item;
    }
    pending = state.pos < bytes.length ? bytes.subarray(state.pos) : Buffer.alloc(0);
  }
  if (pending.length) throw new Error(`Truncated Rangefind merged segment file: ${path}`);
}

function segmentReaders(segments) {
  return segments.map((segment, index) => {
    const data = readSegmentDirectory(segment);
    return {
      index,
      data,
      fd: openSync(data.postingsPath, "r"),
      position: 0,
      get current() {
        return data.terms[this.position] || null;
      }
    };
  });
}

async function* mergedSegmentTermRows(segments, onTerm) {
  const readers = segmentReaders(segments);
  const heap = new MinHeap((left, right) => left.term.localeCompare(right.term) || left.reader - right.reader);
  for (const reader of readers) {
    if (reader.current) heap.push({ reader: reader.index, term: reader.current.term });
  }

  try {
    while (heap.size) {
      const first = heap.pop();
      const term = first.term;
      const matches = [first.reader];
      while (heap.size && heap.items[0].term === term) matches.push(heap.pop().reader);
      const rows = [];
      for (const readerIndex of matches) {
        const reader = readers[readerIndex];
        rows.push(...readSegmentRowsFromFd(reader.fd, reader.current));
        reader.position++;
        if (reader.current) heap.push({ reader: reader.index, term: reader.current.term });
      }
      const merged = mergeRows(rows);
      onTerm?.(term, merged.length);
      yield { term, rows: merged };
    }
  } finally {
    for (const reader of readers) closeSync(reader.fd);
  }
}

function mergeRows(rows) {
  rows.sort((a, b) => a[0] - b[0]);
  const out = [];
  for (const [doc, score] of rows) {
    const last = out[out.length - 1];
    if (last && last[0] === doc) last[1] += score;
    else out.push([doc, score]);
  }
  return out;
}

function addPrefixCounts(prefixCounts, term, df, config) {
  const baseDepth = Math.max(1, Math.floor(Number(config.baseShardDepth || 1)));
  const maxDepth = Math.max(baseDepth, Math.floor(Number(config.maxShardDepth || baseDepth)));
  for (let depth = baseDepth; depth <= maxDepth; depth++) {
    const key = `${depth}\u0000${shardKey(term, depth)}`;
    prefixCounts.set(key, (prefixCounts.get(key) || 0) + df);
  }
}

function partitionNameForTerm(term, prefixCounts, config) {
  const target = Math.max(1, Math.floor(Number(config.targetShardPostings || 1)));
  const baseDepth = Math.max(1, Math.floor(Number(config.baseShardDepth || 1)));
  const maxDepth = Math.max(baseDepth, Math.floor(Number(config.maxShardDepth || baseDepth)));
  let depth = baseDepth;
  while (depth < maxDepth && (prefixCounts.get(`${depth}\u0000${shardKey(term, depth)}`) || 0) > target) depth++;
  return shardKey(term, depth);
}

async function mergeSegmentsToReducedSpool(segments, reducedPath, config, onTerm) {
  const fd = openSync(reducedPath, "w");
  const prefixCounts = new Map();
  let terms = 0;
  let postings = 0;
  try {
    for await (const { term, rows } of mergedSegmentTermRows(segments, onTerm)) {
      const df = rows.length;
      const encoded = encodeReducedTerm(term, rows, df);
      writeSync(fd, encoded, 0, encoded.length);
      terms++;
      postings += df;
      addPrefixCounts(prefixCounts, term, df, config);
    }
  } finally {
    closeSync(fd);
  }
  return { terms, postings, prefixCounts };
}

async function mergeSegmentBatchToSegment(segments, outDir, id, tier) {
  return writeSegmentFromTermRows(outDir, id, mergedSegmentTermRows(segments), {
    mergeTier: tier,
    sourceSegments: segments.map(segment => segment.id).filter(Boolean)
  });
}

async function tierMergeSegments(segments, scratchDir, config) {
  const fanIn = Math.max(2, Math.floor(Number(config.segmentMergeFanIn || config.segmentMergeTierSize || 8)));
  let current = segments.slice();
  const tiers = [];
  for (let tier = 1; current.length > fanIn; tier++) {
    const tierDir = resolve(scratchDir, `tier-${String(tier).padStart(2, "0")}`);
    mkdirSync(tierDir, { recursive: true });
    const next = [];
    const batches = [];
    for (let start = 0; start < current.length; start += fanIn) {
      const batch = current.slice(start, start + fanIn);
      if (batch.length === 1) {
        next.push(batch[0]);
        continue;
      }
      const segment = await mergeSegmentBatchToSegment(batch, tierDir, `merged-${String(tier).padStart(2, "0")}-${String(batches.length).padStart(6, "0")}`, tier);
      next.push(segment);
      batches.push({
        input_segments: batch.length,
        input_terms: batch.reduce((sum, item) => sum + (item.termCount || 0), 0),
        input_postings: batch.reduce((sum, item) => sum + (item.postingCount || 0), 0),
        output_terms: segment.termCount,
        output_postings: segment.postingCount,
        output_bytes: (segment.termsBytes || 0) + (segment.postingBytes || 0)
      });
    }
    tiers.push({
      tier,
      fan_in: fanIn,
      input_segments: current.length,
      output_segments: next.length,
      batches
    });
    current = next;
  }
  return { segments: current, tiers };
}

export async function mergeSegmentsToPartitions(options) {
  const {
    segments,
    scratchDir,
    config,
    onTerm,
    onPartition
  } = options;
  mkdirSync(scratchDir, { recursive: true });
  const reducedPath = resolve(scratchDir, "reduced-terms.run");
  let sequence = 0;
  try {
    const tierStart = performance.now();
    const tiered = await tierMergeSegments(segments, scratchDir, config);
    const tierMergeMs = performance.now() - tierStart;
    const spoolStart = performance.now();
    const reduced = await mergeSegmentsToReducedSpool(tiered.segments, reducedPath, config, onTerm);
    const reducedSpoolMs = performance.now() - spoolStart;
    const reducedSpoolBytes = statSync(reducedPath).size;
    const partitionStart = performance.now();
    const partitions = [];
    let currentName = null;
    let entries = [];
    for await (const { term, rows } of readReducedTerms(reducedPath)) {
      const name = partitionNameForTerm(term, reduced.prefixCounts, config);
      if (currentName && name !== currentName) {
        partitions.push(await onPartition({ name: currentName, entries }, sequence++));
        entries = [];
      }
      currentName = name;
      entries.push([term, rows]);
    }
    if (currentName) partitions.push(await onPartition({ name: currentName, entries }, sequence++));
    return {
      terms: reduced.terms,
      postings: reduced.postings,
      partitions,
      mergeTiers: tiered.tiers,
      timings: {
        tierMergeMs,
        reducedSpoolMs,
        partitionAssemblyMs: performance.now() - partitionStart
      },
      reducedSpoolBytes
    };
  } finally {
    rmSync(scratchDir, { recursive: true, force: true });
  }
}

export function segmentMergeSummary(segments) {
  return {
    format: "rfsegment-merge-v1",
    segments: segments.length,
    terms: segments.reduce((sum, segment) => sum + (segment.termCount || 0), 0),
    postings: segments.reduce((sum, segment) => sum + (segment.postingCount || 0), 0),
    bytes: segments.reduce((sum, segment) => sum + (segment.termsBytes || 0) + (segment.postingBytes || 0), 0)
  };
}
