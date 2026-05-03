import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  addSegmentPosting,
  createSegmentBuilder,
  finishSegmentBuilder,
  flushSegment,
  readSegmentRows,
  readSegmentTerms,
  shouldFlushSegment
} from "../src/segment_builder.js";
import { mergeSegmentsToPartitions } from "../src/segment_merge.js";
import { postingRowCount, postingRowDoc, postingRowScore } from "../src/posting_rows.js";

function entriesToPairs(entries) {
  return entries.map(([term, rows]) => {
    const pairs = [];
    for (let i = 0; i < postingRowCount(rows); i++) pairs.push([postingRowDoc(rows, i), postingRowScore(rows, i)]);
    return [term, pairs];
  });
}

test("segment builder writes bounded immutable term segments", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-segments-"));
  const builder = createSegmentBuilder(root, { segmentMaxPostings: 3 });
  addSegmentPosting(builder, "beta", 2, 7);
  addSegmentPosting(builder, "alpha", 1, 3);
  addSegmentPosting(builder, "alpha", 1, 4);
  flushSegment(builder);
  const segments = finishSegmentBuilder(builder);

  assert.equal(segments.length, 1);
  assert.equal(segments[0].format, "rfsegment-v1");
  assert.equal(segments[0].termCount, 2);
  assert.equal(segments[0].postingCount, 2);
  assert.equal(segments[0].files.terms.checksum.algorithm, "sha256");
  assert.match(segments[0].files.terms.checksum.value, /^[0-9a-f]{64}$/u);
  assert.equal(segments[0].files.postings.checksum.algorithm, "sha256");
  assert.match(segments[0].files.postings.checksum.value, /^[0-9a-f]{64}$/u);

  const data = readSegmentTerms(segments[0]);
  assert.deepEqual(data.terms.map(item => item.term), ["alpha", "beta"]);
  assert.deepEqual(readSegmentRows(data, data.terms[0]), [[1, 7]]);
});

test("segment builder flushes by explicit doc and byte budgets", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-segment-budget-"));
  const byDocs = createSegmentBuilder(join(root, "docs"), { segmentFlushDocs: 2, segmentMaxPostings: 100 });
  addSegmentPosting(byDocs, "alpha", 1, 2);
  addSegmentPosting(byDocs, "beta", 2, 3);
  assert.equal(shouldFlushSegment(byDocs), true);
  const docSegment = flushSegment(byDocs);
  assert.equal(docSegment.flushReason, "docs");
  assert.equal(docSegment.sourceDocCount, 2);
  assert.ok(docSegment.approxMemoryBytes > 0);

  const byBytes = createSegmentBuilder(join(root, "bytes"), { segmentFlushBytes: 1024, segmentMaxPostings: 100 });
  for (let i = 0; i < 4; i++) {
    addSegmentPosting(byBytes, `unusually-long-token-${"x".repeat(240)}-${i}`, i % 2 === 0 ? 1 : 2, 2);
  }
  assert.equal(shouldFlushSegment(byBytes), true);
  const byteSegment = flushSegment(byBytes);
  assert.equal(byteSegment.flushReason, "bytes");
  assert.ok(byteSegment.approxMemoryBytes >= 1024);
});

test("segment builder derives byte budget from builder memory budget", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-segment-memory-budget-"));
  const builder = createSegmentBuilder(root, {
    builderMemoryBudgetBytes: 16 * 1024 * 1024,
    scanWorkers: 4,
    segmentMaxBytes: 64 * 1024 * 1024
  });

  assert.equal(builder.maxBytes, 2 * 1024 * 1024);
});

test("segment builder fails early when one document exceeds the byte limit", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-segment-oversize-"));
  const builder = createSegmentBuilder(root, { segmentFlushBytes: 1024, segmentMaxPostings: 100 });
  for (let i = 0; i < 4; i++) {
    addSegmentPosting(builder, `unusually-long-token-${"x".repeat(240)}-${i}`, 1, 2);
  }

  assert.equal(shouldFlushSegment(builder), true);
  assert.throws(
    () => flushSegment(builder),
    /Rangefind segment flush limit exceeded by one document/u
  );
});

test("segment merge combines terms and emits deterministic partitions", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-segment-merge-"));
  const builder = createSegmentBuilder(join(root, "segments"), { segmentMaxPostings: 2 });
  addSegmentPosting(builder, "alpha", 1, 2);
  addSegmentPosting(builder, "beta", 3, 5);
  flushSegment(builder);
  addSegmentPosting(builder, "alpha", 4, 7);
  addSegmentPosting(builder, "gamma", 5, 11);
  const segments = finishSegmentBuilder(builder);
  const seenTerms = [];
  const partitions = [];

  const stats = await mergeSegmentsToPartitions({
    segments,
    scratchDir: join(root, "merge"),
    config: { baseShardDepth: 1, maxShardDepth: 2, targetShardPostings: 10 },
    onTerm: (term, df) => seenTerms.push([term, df]),
    onPartition: (partition) => {
      partitions.push(partition);
      return partition.name;
    }
  });

  assert.deepEqual(seenTerms, [["alpha", 2], ["beta", 1], ["gamma", 1]]);
  assert.equal(stats.terms, 3);
  assert.equal(stats.postings, 4);
  assert.ok(stats.reducedSpoolBytes > 0);
  assert.ok(stats.timings.reducedSpoolMs >= 0);
  assert.ok(stats.timings.partitionAssemblyMs >= 0);
  assert.deepEqual(entriesToPairs(partitions.flatMap(partition => partition.entries)), [
    ["alpha", [[1, 2], [4, 7]]],
    ["beta", [[3, 5]]],
    ["gamma", [[5, 11]]]
  ]);
});

test("segment merge writes intermediate tiers before final partitioning", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-segment-tiered-"));
  const builder = createSegmentBuilder(join(root, "segments"), { segmentMaxPostings: 1 });
  for (const [term, doc, score] of [
    ["alpha", 1, 2],
    ["beta", 2, 3],
    ["alpha", 3, 5],
    ["gamma", 4, 7],
    ["beta", 5, 11]
  ]) {
    addSegmentPosting(builder, term, doc, score);
    flushSegment(builder);
  }
  const segments = finishSegmentBuilder(builder);
  const partitions = [];

  const stats = await mergeSegmentsToPartitions({
    segments,
    scratchDir: join(root, "merge"),
    config: { baseShardDepth: 1, maxShardDepth: 2, targetShardPostings: 10, segmentMergeFanIn: 2 },
    onPartition: (partition) => {
      partitions.push(partition);
      return partition.name;
    }
  });

  assert.equal(stats.mergeTiers.length, 2);
  assert.deepEqual(stats.mergeTiers.map(tier => tier.output_segments), [3, 2]);
  assert.equal(stats.mergePolicy.policy, "tiered-log");
  assert.equal(stats.mergePolicy.targetSegments, 2);
  assert.equal(stats.mergePolicy.forceMerge, false);
  assert.equal(stats.mergePolicy.inputSegments, 5);
  assert.equal(stats.mergePolicy.outputSegments, 2);
  assert.ok(stats.mergePolicy.writeAmplification > 0);
  assert.ok(stats.mergeTiers.every(tier => tier.input_bytes > 0));
  assert.ok(stats.mergeTiers.every(tier => tier.batches.every(batch => batch.input_bytes > 0 && batch.output_bytes > 0)));
  assert.equal(stats.terms, 3);
  assert.equal(stats.postings, 5);
  assert.deepEqual(entriesToPairs(partitions.flatMap(partition => partition.entries)), [
    ["alpha", [[1, 2], [3, 5]]],
    ["beta", [[2, 3], [5, 11]]],
    ["gamma", [[4, 7]]]
  ]);
});

test("segment merge can force merge to a target segment count", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-segment-force-merge-"));
  const builder = createSegmentBuilder(join(root, "segments"), { segmentMaxPostings: 1 });
  for (const [term, doc, score] of [
    ["alpha", 1, 2],
    ["beta", 2, 3],
    ["gamma", 3, 5],
    ["delta", 4, 7]
  ]) {
    addSegmentPosting(builder, term, doc, score);
    flushSegment(builder);
  }

  const stats = await mergeSegmentsToPartitions({
    segments: finishSegmentBuilder(builder),
    scratchDir: join(root, "merge"),
    config: {
      baseShardDepth: 1,
      maxShardDepth: 2,
      targetShardPostings: 10,
      segmentMergeFanIn: 2,
      finalSegmentTargetCount: 1
    },
    onPartition: partition => partition.name
  });

  assert.equal(stats.mergePolicy.forceMerge, true);
  assert.equal(stats.mergePolicy.targetSegments, 1);
  assert.equal(stats.mergePolicy.outputSegments, 1);
  assert.equal(stats.mergeTiers.at(-1).output_segments, 1);
});

test("segment merge records temp-budget blocked skipped merges", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-segment-temp-budget-"));
  const builder = createSegmentBuilder(join(root, "segments"), { segmentMaxPostings: 1 });
  for (const [term, doc, score] of [
    ["alpha", 1, 2],
    ["beta", 2, 3],
    ["gamma", 3, 5]
  ]) {
    addSegmentPosting(builder, term, doc, score);
    flushSegment(builder);
  }

  const stats = await mergeSegmentsToPartitions({
    segments: finishSegmentBuilder(builder),
    scratchDir: join(root, "merge"),
    config: {
      baseShardDepth: 1,
      maxShardDepth: 2,
      targetShardPostings: 10,
      segmentMergeFanIn: 3,
      finalSegmentTargetCount: 1,
      segmentMergeMaxTempBytes: 1
    },
    onPartition: partition => partition.name
  });

  assert.equal(stats.mergePolicy.blockedByTempBudget, true);
  assert.equal(stats.mergePolicy.skippedSegments, 3);
  assert.equal(stats.mergeTiers[0].blocked_by_temp_budget, true);
  assert.equal(stats.mergeTiers[0].batches.length, 0);
});
