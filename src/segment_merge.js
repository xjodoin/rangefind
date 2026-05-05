import { closeSync, mkdirSync, openSync, rmSync } from "node:fs";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { createPostingRowBuffer, appendPostingRow, copyPostingRows, postingRowCount, resetPostingRows } from "./posting_rows.js";
import { readSegmentDirectory, readSegmentRowsFromFdInto, writeSegmentFromTermRows } from "./segment_builder.js";
import { shardKey } from "./shards.js";

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

function segmentReaders(segments) {
  return segments.map((segment, index) => {
    const data = readSegmentDirectory(segment);
    return {
      index,
      data,
      fd: openSync(data.postingsPath, "r"),
      readBuffer: Buffer.alloc(0),
      rows: createPostingRowBuffer(0),
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
  const mergedRows = createPostingRowBuffer(0);
  for (const reader of readers) {
    if (reader.current) heap.push({ reader: reader.index, term: reader.current.term });
  }

  try {
    while (heap.size) {
      const first = heap.pop();
      const term = first.term;
      const matches = [first.reader];
      while (heap.size && heap.items[0].term === term) matches.push(heap.pop().reader);
      resetPostingRows(mergedRows);
      for (const readerIndex of matches) {
        const reader = readers[readerIndex];
        resetPostingRows(reader.rows);
        reader.readBuffer = readSegmentRowsFromFdInto(reader.fd, reader.current, reader.rows, reader.readBuffer);
      }
      mergeSortedReaderRows(readers, matches, mergedRows);
      for (const readerIndex of matches) {
        const reader = readers[readerIndex];
        reader.position++;
        if (reader.current) heap.push({ reader: reader.index, term: reader.current.term });
      }
      onTerm?.(term, mergedRows.length);
      yield { term, rows: mergedRows };
    }
  } finally {
    for (const reader of readers) closeSync(reader.fd);
  }
}

function mergeSortedReaderRows(readers, matches, out) {
  const rowHeap = new MinHeap((left, right) => left.doc - right.doc || left.reader - right.reader);
  for (const readerIndex of matches) {
    const rows = readers[readerIndex].rows;
    if (rows.length) rowHeap.push({ reader: readerIndex, index: 0, doc: rows.docs[0], score: rows.scores[0] });
  }
  while (rowHeap.size) {
    const first = rowHeap.pop();
    const doc = first.doc;
    let score = first.score;
    advanceReaderRow(readers, rowHeap, first);
    while (rowHeap.size && rowHeap.items[0].doc === doc) {
      const next = rowHeap.pop();
      score += next.score;
      advanceReaderRow(readers, rowHeap, next);
    }
    appendPostingRow(out, doc, score);
  }
  return out;
}

function advanceReaderRow(readers, rowHeap, item) {
  const rows = readers[item.reader].rows;
  item.index++;
  if (item.index >= rows.length) return;
  item.doc = rows.docs[item.index];
  item.score = rows.scores[item.index];
  rowHeap.push(item);
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

async function measureMergedSegmentTerms(segments, config, onTerm) {
  const prefixCounts = new Map();
  let terms = 0;
  let postings = 0;
  for await (const { term, rows } of mergedSegmentTermRows(segments, onTerm)) {
    const df = postingRowCount(rows);
    terms++;
    postings += df;
    addPrefixCounts(prefixCounts, term, df, config);
  }
  return { terms, postings, prefixCounts };
}

async function mergeSegmentBatchToSegment(segments, outDir, id, tier) {
  return writeSegmentFromTermRows(outDir, id, mergedSegmentTermRows(segments), {
    mergeTier: tier,
    mergePolicy: "tiered-log",
    mergeReason: "similarly-sized-tier",
    sourceSegments: segments.map(segment => segment.id).filter(Boolean),
    sourceBytes: segments.reduce((sum, segment) => sum + segmentBytes(segment), 0),
    sourceDocCount: segments.reduce((sum, segment) => sum + (segment.docCount || 0), 0)
  });
}

function segmentBytes(segment) {
  return (segment.termsBytes || 0) + (segment.postingBytes || 0);
}

function mergeFanIn(config) {
  return Math.max(2, Math.floor(Number(config.segmentMergeFanIn || config.segmentMergeTierSize || 8)));
}

function mergeTargetSegments(config, fanIn) {
  const target = Math.floor(Number(config.finalSegmentTargetCount || config.segmentMergeTargetCount || 0));
  return target > 0 ? Math.max(1, target) : fanIn;
}

function maxTempBytes(config) {
  return Math.max(0, Math.floor(Number(config.segmentMergeMaxTempBytes || 0)));
}

function mergePolicyName(config) {
  const policy = String(config.segmentMergePolicy || "tiered-log");
  if (policy === "none") return "none";
  return "tiered-log";
}

function orderedMergeCandidates(segments) {
  return segments
    .map((segment, index) => ({ segment, index, bytes: segmentBytes(segment) }))
    .sort((left, right) => left.bytes - right.bytes || String(left.segment.id || "").localeCompare(String(right.segment.id || "")) || left.index - right.index)
    .map(item => item.segment);
}

function mergeBatches(segments, config) {
  const fanIn = mergeFanIn(config);
  const cap = maxTempBytes(config);
  const ordered = orderedMergeCandidates(segments);
  const batches = [];
  for (let start = 0; start < ordered.length;) {
    const batch = [ordered[start++]];
    let bytes = segmentBytes(batch[0]);
    while (start < ordered.length && batch.length < fanIn) {
      const next = ordered[start];
      const nextBytes = segmentBytes(next);
      if (cap && bytes + nextBytes > cap) break;
      batch.push(next);
      bytes += nextBytes;
      start++;
    }
    batches.push(batch);
  }
  return batches;
}

function mergePolicySummary({ config, inputSegments, outputSegments, tiers }) {
  const inputBytes = inputSegments.reduce((sum, segment) => sum + segmentBytes(segment), 0);
  const intermediateBytes = tiers.reduce((sum, tier) => sum + (tier.output_bytes || 0), 0);
  const outputBytes = outputSegments.reduce((sum, segment) => sum + segmentBytes(segment), 0);
  const fanIn = mergeFanIn(config);
  const targetSegments = mergeTargetSegments(config, fanIn);
  return {
    policy: mergePolicyName(config),
    fanIn,
    targetSegments,
    forceMerge: targetSegments === 1,
    maxTempBytes: maxTempBytes(config),
    inputSegments: inputSegments.length,
    outputSegments: outputSegments.length,
    inputBytes,
    outputBytes,
    intermediateBytes,
    skippedSegments: tiers.reduce((sum, tier) => sum + (tier.skipped_segments || 0), 0),
    blockedByTempBudget: tiers.some(tier => tier.blocked_by_temp_budget),
    writeAmplification: inputBytes > 0 ? intermediateBytes / inputBytes : 0
  };
}

async function tierMergeSegments(segments, scratchDir, config) {
  const policyName = mergePolicyName(config);
  const fanIn = mergeFanIn(config);
  const targetSegments = mergeTargetSegments(config, fanIn);
  if (policyName === "none" || segments.length <= targetSegments) {
    const tiers = [];
    return {
      segments: segments.slice(),
      tiers,
      policy: mergePolicySummary({ config, inputSegments: segments, outputSegments: segments, tiers })
    };
  }
  let current = segments.slice();
  const tiers = [];
  for (let tier = 1; current.length > targetSegments; tier++) {
    const tierDir = resolve(scratchDir, `tier-${String(tier).padStart(2, "0")}`);
    mkdirSync(tierDir, { recursive: true });
    const next = [];
    const batches = [];
    let skippedSegments = 0;
    let inputBytes = 0;
    let outputBytes = 0;
    let mergedBatches = 0;
    for (const batch of mergeBatches(current, config)) {
      inputBytes += batch.reduce((sum, segment) => sum + segmentBytes(segment), 0);
      if (batch.length === 1) {
        next.push(batch[0]);
        skippedSegments++;
        continue;
      }
      const segment = await mergeSegmentBatchToSegment(batch, tierDir, `merged-${String(tier).padStart(2, "0")}-${String(batches.length).padStart(6, "0")}`, tier);
      next.push(segment);
      mergedBatches++;
      const batchInputBytes = batch.reduce((sum, item) => sum + segmentBytes(item), 0);
      const batchOutputBytes = segmentBytes(segment);
      outputBytes += batchOutputBytes;
      batches.push({
        policy: policyName,
        reason: "similarly-sized-tier",
        input_segments: batch.length,
        source_segments: batch.map(item => item.id).filter(Boolean),
        input_terms: batch.reduce((sum, item) => sum + (item.termCount || 0), 0),
        input_postings: batch.reduce((sum, item) => sum + (item.postingCount || 0), 0),
        input_bytes: batchInputBytes,
        output_terms: segment.termCount,
        output_postings: segment.postingCount,
        output_bytes: batchOutputBytes,
        write_amplification: batchInputBytes > 0 ? batchOutputBytes / batchInputBytes : 0
      });
    }
    tiers.push({
      tier,
      policy: policyName,
      fan_in: fanIn,
      target_segments: targetSegments,
      input_segments: current.length,
      output_segments: next.length,
      skipped_segments: skippedSegments,
      input_bytes: inputBytes,
      output_bytes: outputBytes,
      estimated_write_amplification: inputBytes > 0 ? outputBytes / inputBytes : 0,
      blocked_by_temp_budget: mergedBatches === 0 && next.length > targetSegments,
      batches
    });
    if (mergedBatches === 0) {
      current = next;
      break;
    }
    current = next;
  }
  return {
    segments: current,
    tiers,
    policy: mergePolicySummary({ config, inputSegments: segments, outputSegments: current, tiers })
  };
}

export async function mergeSegmentsToPartitions(options) {
  const {
    segments,
    scratchDir,
    config,
    onTerm,
    onPartition,
    partitionConcurrency = 1
  } = options;
  mkdirSync(scratchDir, { recursive: true });
  let sequence = 0;
  const maxConcurrency = Math.max(1, Math.floor(Number(partitionConcurrency || 1)));
  const creditLimitBytes = Math.max(0, Math.floor(Number(config.partitionReducerInFlightBytes || 0)));
  let partitionSpoolBytes = 0;
  let partitionSpoolEntries = 0;
  let activeInputBytes = 0;
  let maxActiveInputBytes = 0;
  let creditWaitMs = 0;
  let creditWaits = 0;
  let oversizedPartitions = 0;
  try {
    const tierStart = performance.now();
    const tiered = await tierMergeSegments(segments, scratchDir, config);
    const tierMergeMs = performance.now() - tierStart;
    const partitionStart = performance.now();
    const partitions = [];
    const pendingPartitions = [];
    const activePartitions = new Set();
    function hasCredit(inputBytes) {
      return !creditLimitBytes || activeInputBytes === 0 || activeInputBytes + inputBytes <= creditLimitBytes;
    }
    async function queuePartition(partition) {
      const inputBytes = partition.inputBytes || partition.length || 0;
      const started = performance.now();
      let waited = false;
      while (activePartitions.size >= maxConcurrency || !hasCredit(inputBytes)) {
        waited = true;
        await Promise.race(activePartitions);
      }
      if (waited) {
        creditWaits++;
        creditWaitMs += performance.now() - started;
      }
      if (creditLimitBytes && inputBytes > creditLimitBytes && activeInputBytes === 0) oversizedPartitions++;
      activeInputBytes += inputBytes;
      maxActiveInputBytes = Math.max(maxActiveInputBytes, activeInputBytes);
      const partitionSequence = sequence++;
      const promise = Promise.resolve(onPartition(partition, partitionSequence))
        .then(result => {
          partitions.push({ sequence: partitionSequence, result });
          return result;
        })
        .finally(() => {
          activeInputBytes -= inputBytes;
        });
      activePartitions.add(promise);
      pendingPartitions.push(promise);
      promise.finally(() => activePartitions.delete(promise));
    }
    function newStreamingPartition(name) {
      return {
        format: "rfreducerpartition-v1",
        name,
        entries: [],
        length: 0,
        terms: 0,
        rows: 0,
        inputBytes: 0,
        firstTerm: "",
        lastTerm: ""
      };
    }
    function addStreamingPartitionTerm(partition, term, rows) {
      const copiedRows = copyPostingRows(rows);
      const rowCount = postingRowCount(copiedRows);
      const inputBytes = String(term || "").length + rowCount * 8;
      partition.entries.push([term, copiedRows]);
      partition.length += inputBytes;
      partition.inputBytes += inputBytes;
      partition.terms++;
      partition.rows += rowCount;
      partitionSpoolEntries++;
      if (!partition.firstTerm) partition.firstTerm = term;
      partition.lastTerm = term;
    }
    const prefixStart = performance.now();
    const reduced = await measureMergedSegmentTerms(tiered.segments, config, onTerm);
    const prefixCountMs = performance.now() - prefixStart;
    let current = null;
    for await (const { term, rows } of mergedSegmentTermRows(tiered.segments)) {
      const name = partitionNameForTerm(term, reduced.prefixCounts, config);
      if (current && name !== current.name) {
        await queuePartition(current);
        partitionSpoolBytes += current.inputBytes;
        current = null;
      }
      if (!current) current = newStreamingPartition(name);
      addStreamingPartitionTerm(current, term, rows);
    }
    if (current) {
      await queuePartition(current);
      partitionSpoolBytes += current.inputBytes;
    }
    await Promise.all(pendingPartitions);
    return {
      terms: reduced.terms,
      postings: reduced.postings,
      partitions: partitions.sort((left, right) => left.sequence - right.sequence).map(item => item.result),
      mergeTiers: tiered.tiers,
      mergePolicy: tiered.policy,
      timings: {
        tierMergeMs,
        prefixCountMs,
        partitionAssemblyMs: performance.now() - partitionStart
      },
      partitionScheduler: {
        creditLimitBytes,
        maxActiveInputBytes,
        creditWaitMs,
        creditWaits,
        oversizedPartitions
      },
      partitionSpoolBytes,
      partitionSpoolEntries
    };
  } finally {
    rmSync(scratchDir, { recursive: true, force: true });
  }
}

export function segmentMergeSummary(segments) {
  const flushReasons = {};
  for (const segment of segments) {
    const reason = segment.flushReason || "unknown";
    flushReasons[reason] = (flushReasons[reason] || 0) + 1;
  }
  return {
    format: "rfsegment-merge-v1",
    segments: segments.length,
    terms: segments.reduce((sum, segment) => sum + (segment.termCount || 0), 0),
    postings: segments.reduce((sum, segment) => sum + (segment.postingCount || 0), 0),
    bytes: segments.reduce((sum, segment) => sum + (segment.termsBytes || 0) + (segment.postingBytes || 0), 0),
    approxMemoryBytes: segments.reduce((sum, segment) => Math.max(sum, segment.approxMemoryBytes || 0), 0),
    maxDocs: segments.reduce((sum, segment) => Math.max(sum, segment.sourceDocCount || segment.docCount || 0), 0),
    flushReasons
  };
}
