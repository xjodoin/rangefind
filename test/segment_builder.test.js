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
  readSegmentTerms
} from "../src/segment_builder.js";
import { mergeSegmentsToPartitions } from "../src/segment_merge.js";

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

  const data = readSegmentTerms(segments[0]);
  assert.deepEqual(data.terms.map(item => item.term), ["alpha", "beta"]);
  assert.deepEqual(readSegmentRows(data, data.terms[0]), [[1, 7]]);
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
  assert.deepEqual(partitions.flatMap(partition => partition.entries), [
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
  assert.equal(stats.terms, 3);
  assert.equal(stats.postings, 5);
  assert.deepEqual(partitions.flatMap(partition => partition.entries), [
    ["alpha", [[1, 2], [3, 5]]],
    ["beta", [[2, 3], [5, 11]]],
    ["gamma", [[4, 7]]]
  ]);
});
