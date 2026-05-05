import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { getPath, readConfig } from "../src/config.js";

test("readConfig resolves input and output relative to the config file", async () => {
  const dir = await mkdtemp(join(tmpdir(), "rangefind-config-"));
  const configPath = join(dir, "rangefind.config.json");
  await writeFile(configPath, JSON.stringify({ input: "docs.jsonl", output: "public/search" }));
  const config = await readConfig(configPath);
  assert.equal(config.input, join(dir, "docs.jsonl"));
  assert.equal(config.output, join(dir, "public/search"));
  assert.equal(config.docValueChunkSize, 2048);
  assert.equal(config.docValueLookupChunkSize, 2048);
  assert.equal(config.filterBitmapMaxFacetValues, 64);
  assert.equal(config.directorySortChunkEntries, 16384);
  assert.equal(config.builderWorkerCount, 1);
  assert.equal(config.partitionReducerWorkers, 0);
  assert.equal(config.partitionReducerInFlightBytes, 1024 * 1024 * 1024);
  assert.equal(config.builderMemoryBudgetBytes, 0);
  assert.equal(config.indexProfile, "static-large");
  assert.equal(config.queryBundles, false);
  assert.equal(config.targetPostingsPerDoc, 12);
  assert.equal(config.maxTermsPerDoc, 12);
  assert.equal(config.bodyIndexChars, 6000);
  assert.deepEqual(config.alwaysIndexFields, ["title", "categories"]);
  assert.equal(config.resumeBuild, true);
  assert.equal(config.resumeDir, "_build/resume");
  assert.equal(config.maxExpansionTermsPerDoc, 0);
  assert.equal(config.postingOrder, "doc-id");
  assert.equal(config.postingSegmentStreamMinBytes, 64 * 1024);
  assert.equal(config.postingImpactBucketOrderMinRows, Number.MAX_SAFE_INTEGER);
  assert.equal(config.postingImpactBucketOrderMaxBuckets, 65536);
  assert.equal(config.postingImpactTiers, false);
  assert.equal(config.postingDocRangeBlockMax, false);
  assert.equal(config.postingDocRangeSize, 1024);
  assert.equal(config.postingDocRangeQuantizationBits, 8);
  assert.equal(config.codeStoreWorkerCacheChunks, 0);
  assert.equal(config.codeStoreWorkerMaxAutoCacheChunks, 64);
  assert.equal(config.segmentFlushDocs, 0);
  assert.equal(config.segmentFlushBytes, 0);
  assert.equal(config.segmentMergePolicy, "tiered-log");
  assert.equal(config.segmentMergeFanIn, 128);
  assert.equal(config.segmentMergeMaxTempBytes, 512 * 1024 * 1024);
  assert.equal(config.finalSegmentTargetCount, 0);
  assert.equal(config.codecs.mode, "varint");
  assert.equal(config.typo, false);
  assert.equal(config.buildProgressLogMs, 0);
});

test("readConfig keeps explicit overrides in static-large profile", async () => {
  const dir = await mkdtemp(join(tmpdir(), "rangefind-config-profile-"));
  const configPath = join(dir, "rangefind.config.json");
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/search",
    indexProfile: "static-large",
    queryBundles: true,
    targetPostingsPerDoc: 9,
    bodyIndexChars: 1200,
    alwaysIndexFields: ["title"],
    maxExpansionTermsPerDoc: 3,
    partitionReducerInFlightBytes: 16,
    typo: { enabled: true },
    codecs: { mode: "auto" }
  }));
  const config = await readConfig(configPath);

  assert.equal(config.indexProfile, "static-large");
  assert.equal(config.targetPostingsPerDoc, 9);
  assert.equal(config.maxTermsPerDoc, 9);
  assert.equal(config.bodyIndexChars, 1200);
  assert.deepEqual(config.alwaysIndexFields, ["title"]);
  assert.equal(config.maxExpansionTermsPerDoc, 3);
  assert.equal(config.queryBundles, true);
  assert.equal(config.postingOrder, "doc-id");
  assert.equal(config.postingImpactBucketOrderMinRows, Number.MAX_SAFE_INTEGER);
  assert.equal(config.postingDocRangeBlockMax, false);
  assert.equal(config.postingImpactTiers, false);
  assert.equal(config.segmentMergeFanIn, 128);
  assert.equal(config.segmentMergeMaxTempBytes, 512 * 1024 * 1024);
  assert.equal(config.partitionReducerInFlightBytes, 16);
  assert.deepEqual(config.typo, { enabled: true });
  assert.equal(config.codecs.mode, "auto");
});

test("getPath reads nested values, arrays, nulls, and fallbacks", () => {
  const object = { nested: { title: "Rangefind" }, tags: ["static", "search"], empty: null };
  assert.equal(getPath(object, "nested.title"), "Rangefind");
  assert.equal(getPath(object, "tags"), "static search");
  assert.equal(getPath(object, "empty", "fallback"), "fallback");
  assert.equal(getPath(object, "missing.value", "fallback"), "fallback");
});
