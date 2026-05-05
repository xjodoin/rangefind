import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readdir, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { gunzipSync } from "node:zlib";
import test from "node:test";
import { build } from "../src/builder.js";
import { parseDocPagePointerPage } from "../src/doc_pages.js";
import { parseDocOrdinalTable, parseDocPointerPage } from "../src/doc_pointers.js";
import { createSearch } from "../src/runtime.js";

async function serveStatic(root) {
  const requests = [];
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://localhost");
      requests.push({ pathname: url.pathname, range: request.headers.range || "" });
      const path = resolve(root, `.${decodeURIComponent(url.pathname)}`);
      if (!path.startsWith(resolve(root))) {
        response.writeHead(403).end();
        return;
      }
      const data = await readFile(path);
      const range = request.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (range) {
        const start = Number(range[1]);
        const end = Math.min(Number(range[2]), data.length - 1);
        response.writeHead(206, {
          "Accept-Ranges": "bytes",
          "Content-Length": String(end - start + 1),
          "Content-Range": `bytes ${start}-${end}/${data.length}`
        });
        response.end(data.subarray(start, end + 1));
        return;
      }
      response.writeHead(200, { "Content-Length": String(data.length) });
      response.end(data);
    } catch {
      response.writeHead(404).end();
    }
  });
  await new Promise(resolveListen => server.listen(0, "127.0.0.1", resolveListen));
  const { port } = server.address();
  return {
    baseUrl: `http://127.0.0.1:${port}/rangefind/`,
    requests,
    close: () => new Promise(resolveClose => server.close(resolveClose))
  };
}

async function resumeStageFile(output, stage) {
  const resumeRoot = join(output, "_build", "resume");
  const fingerprints = await readdir(resumeRoot);
  assert.equal(fingerprints.length, 1);
  return join(resumeRoot, fingerprints[0], "stages", `${stage}.json`);
}

test("builder output is searchable through the range-based runtime", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-build-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, [
    JSON.stringify({ id: "a", title: "Static range search", body: "Rangefind builds a static index with range requests.", category: "indexing", tags: ["static", "range"], year: 2026, temperature: -1, published: "2026-01-10", featured: true, url: "/a" }),
    JSON.stringify({ id: "b", title: "SQLite retrieval baseline", body: "A server-side SQLite benchmark compares retrieval quality.", aliases: ["authoritative alias"], category: "baseline", tags: ["sqlite", "quality"], year: 2025, temperature: -5, published: "2025-06-01", featured: false, url: "/b" }),
    JSON.stringify({ id: "c", title: "Client search runtime", body: "The runtime fetches packed posting segments lazily.", category: "runtime", tags: ["static", "runtime"], year: 2026, temperature: 0, published: "2026-03-15", featured: false, url: "/c" }),
    JSON.stringify({ id: "d", title: "Electrified winding insulation", body: "A corrected stem must not be stemmed a second time.", category: "runtime", tags: ["typo"], year: 2026, temperature: 7, published: "2026-05-01", featured: true, url: "/d" }),
    JSON.stringify({ id: "e", title: "Archived catalog entry", body: "A low impact search mention for block skipping coverage.", category: "archive", tags: ["filler"], year: 2024, temperature: 2, published: "2024-01-01", featured: false, url: "/e" }),
    JSON.stringify({ id: "f", title: "Collection note", body: "Another low impact search mention for block skipping coverage.", category: "archive", tags: ["filler"], year: 2024, temperature: 2, published: "2024-01-02", featured: false, url: "/f" }),
    JSON.stringify({ id: "g", title: "Dataset appendix", body: "A repeated low impact search mention for block skipping coverage.", category: "archive", tags: ["filler"], year: 2024, temperature: 2, published: "2024-01-03", featured: false, url: "/g" }),
    JSON.stringify({ id: "h", title: "Legacy material", body: "A final low impact search mention for block skipping coverage.", category: "archive", tags: ["filler"], year: 2024, temperature: 2, published: "2024-01-04", featured: false, url: "/h" }),
    JSON.stringify({ id: "i", title: "Pariser cannon", body: "Surface exact fallback should prefer indexed raw terms before typo lookup.", category: "archive", tags: ["filler"], year: 2023, temperature: 2, published: "2023-01-04", featured: false, url: "/i" })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    docValueChunkSize: 2,
    docValueLookupChunkSize: 1,
    docValueSortedPageSize: 2,
    buildTelemetrySampleMs: 5,
    buildTelemetryPath: "build-telemetry.json",
    scanWorkers: 2,
    scanBatchDocs: 2,
    baseShardDepth: 2,
    maxShardDepth: 3,
    targetShardPostings: 2,
    queryBundles: true,
    typo: { enabled: true },
    segmentMergeFanIn: 512,
    postingBlockSize: 2,
    postingDocRangeBlockMax: true,
    postingImpactTiers: true,
    postingImpactTierMinBlocks: 1,
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0,
    queryBundleMinSeedDocs: 1,
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    authority: [
      { name: "title", path: "title" },
      { name: "aliases", path: "aliases" }
    ],
	    facets: [{ name: "category", path: "category" }, { name: "tags", path: "tags" }],
	    numbers: [{ name: "year", path: "year" }, { name: "temperature", path: "temperature" }, { name: "published", path: "published", type: "date" }],
	    booleans: [{ name: "featured", path: "featured" }],
	    sortReplicas: [{ field: "year", order: "desc" }],
	    display: ["title", "url", "category", "tags", "year", "temperature", "published", "featured", { name: "bodySnippet", path: "body", maxChars: 16 }]
	  }));

  await build({ configPath });
  assert.ok(await readFile(join(output, "manifest.json"), "utf8"));
  const manifest = JSON.parse(await readFile(join(output, "manifest.json"), "utf8"));
  const minimalManifest = JSON.parse(await readFile(join(output, "manifest.min.json"), "utf8"));
  assert.equal(minimalManifest.lazy_manifests.full, "manifest.full.json");
  assert.equal(minimalManifest.lazy_manifests.build, "debug/build-telemetry.json");
  assert.equal(minimalManifest.lazy_manifests.optimizer, "debug/index-optimizer.json");
  assert.equal(minimalManifest.lazy_manifests.doc_values, "doc-values/manifest.json.gz");
  assert.equal(minimalManifest.lazy_manifests.doc_value_sorted, "doc-values/sorted/manifest.json.gz");
  assert.equal(minimalManifest.lazy_manifests.filter_bitmaps, "filter-bitmaps/manifest.json.gz");
  assert.equal(minimalManifest.lazy_manifests.facet_dictionaries, "facets/manifest.json.gz");
  assert.equal(minimalManifest.segments.manifest, "segments/manifest.json.gz");
  assert.ok(await readFile(join(output, "manifest.full.json"), "utf8"));
  assert.ok(await readFile(join(output, "doc-values", "manifest.json.gz")));
  assert.ok(await readFile(join(output, "doc-values", "sorted", "manifest.json.gz")));
  assert.ok(await readFile(join(output, "filter-bitmaps", "manifest.json.gz")));
  assert.ok(await readFile(join(output, "facets", "manifest.json.gz")));
  const segmentManifest = JSON.parse(gunzipSync(await readFile(join(output, "segments", "manifest.json.gz"))).toString("utf8"));
  assert.equal(segmentManifest.format, "rfsegmentmanifest-v1");
  assert.equal(segmentManifest.published, true);
  assert.equal(segmentManifest.storage, "static-segment-files-v1");
  assert.equal(segmentManifest.totalDocs, 9);
  assert.equal(segmentManifest.segmentCount, manifest.stats.segment_files);
  assert.deepEqual(segmentManifest.fields.text, ["title", "body"]);
  assert.deepEqual(segmentManifest.fields.facets, ["category", "tags"]);
  assert.deepEqual(segmentManifest.fields.numbers.map(field => field.name), ["year", "temperature", "published"]);
  assert.deepEqual(segmentManifest.fields.booleans, ["featured"]);
  assert.ok(segmentManifest.segments.every(segment => segment.docCount > 0));
  assert.ok(segmentManifest.segments.every(segment => segment.approxMemoryBytes > 0));
  assert.ok(segmentManifest.segments.every(segment => segment.flushReason));
  assert.ok(segmentManifest.segments.every(segment => segment.files.terms.checksum.value.length === 64));
  assert.ok(segmentManifest.segments.every(segment => segment.files.postings.checksum.value.length === 64));
  assert.ok(await readFile(join(output, segmentManifest.segments[0].files.terms.path)));
  assert.ok(await readFile(join(output, segmentManifest.segments[0].files.postings.path)));
  assert.ok(await readFile(join(output, "debug", "build-telemetry.json"), "utf8"));
  const optimizerReport = JSON.parse(await readFile(join(output, "debug", "index-optimizer.json"), "utf8"));
  assert.equal(optimizerReport.format, "rfoptimizer-v1");
  assert.equal(optimizerReport.policy, "core-first");
  assert.equal(optimizerReport.summary.path, "debug/index-optimizer.json");
  assert.ok(optimizerReport.budget.bytes >= 0);
  assert.ok(optimizerReport.core.some(item => item.kind === "top-k-proof" && item.status === "instrumented"));
  assert.ok(optimizerReport.core.some(item => item.kind === "posting-superblocks" && item.status === "current" && item.scheduler_status === "current"));
  assert.ok(optimizerReport.deferred.some(item => item.kind === "champion-window"));
  assert.equal(minimalManifest.build, undefined);
  assert.equal(minimalManifest.doc_values, undefined);
  assert.equal(minimalManifest.optimizer.path, "debug/index-optimizer.json");
  assert.ok(minimalManifest.block_filters);
  assert.equal(minimalManifest.typo.directory, undefined);
  assert.equal(minimalManifest.facets.category.count, 5);
  assert.equal(manifest.features.checksummedObjects, true);
  assert.equal(manifest.features.contentAddressedObjects, true);
  assert.equal(manifest.features.deduplicatedObjects, true);
  assert.equal(manifest.features.denseDocPointers, true);
  assert.equal(manifest.features.docLocalityLayout, true);
  assert.equal(manifest.features.docPages, true);
  assert.equal(manifest.features.fieldRowPipeline, true);
  assert.equal(manifest.features.docValueSorted, true);
  assert.equal(manifest.features.filterBitmaps, true);
  assert.equal(manifest.features.segmentManifest, true);
  assert.equal(manifest.features.queryBundles, true);
  assert.equal(manifest.features.authority, true);
  assert.equal(manifest.optimizer.format, "rfoptimizer-v1");
  assert.equal(manifest.optimizer.policy, "core-first");
  assert.equal(manifest.object_store.pointer_format, "rfbp-v1");
  assert.equal(manifest.object_store.immutable_names, true);
  assert.equal(manifest.docs.layout.format, "rflocal-doc-v1");
  assert.equal(manifest.docs.layout.strategy, "primary-base-term-impact");
  assert.ok(manifest.docs.layout.primary_terms > 0);
  assert.ok(manifest.docs.layout.raw_spool_bytes > 0);
  assert.ok(manifest.docs.layout.spool_bytes > 0);
  assert.equal(manifest.docs.pointers.format, "rfdocptr-v1");
  assert.equal(manifest.docs.pointers.order, "layout");
  assert.equal(manifest.docs.pointers.ordinals.format, "rfdocord-v1");
  assert.equal(manifest.docs.pages.format, "rfdocpage-v1");
  assert.equal(manifest.docs.pages.encoding, "rfdocpagecols-v1");
  assert.deepEqual(manifest.docs.pages.fields, ["id", "title", "url", "category", "tags", "year", "temperature", "published", "featured", "bodySnippet"]);
  assert.equal(manifest.docs.pages.pointers.format, "rfdocpageptr-v1");
  assert.equal(manifest.docs.pages.pointers.order, "doc-id-page");
  assert.equal(manifest.docs.pages.page_size, 32);
  assert.match(manifest.docs.pointers.file, /^docs\/pointers\/0000\.[0-9a-f]{24}\.bin$/u);
  assert.match(manifest.docs.pointers.ordinals.file, /^docs\/ordinals\/0000\.[0-9a-f]{24}\.bin$/u);
  assert.match(manifest.docs.pages.pointers.file, /^docs\/pages\/0000\.[0-9a-f]{24}\.bin$/u);
  assert.match(manifest.docs.pointers.pack_table[0], /^0000\.[0-9a-f]{24}\.bin$/u);
  assert.match(manifest.docs.pages.pointers.pack_table[0], /^0000\.[0-9a-f]{24}\.bin$/u);
  assert.match(manifest.object_store.pack_table.docPages[0], /^0000\.[0-9a-f]{24}\.bin$/u);
  assert.match(manifest.object_store.pack_table.postingBlocks[0], /^0000\.[0-9a-f]{24}\.bin$/u);
  assert.equal(manifest.directory.format, "rfdir-v2");
  assert.equal(manifest.build.format, "rfbuildtelemetry-v1");
  assert.ok(manifest.build.total_ms >= 0);
  assert.ok(manifest.build.peak_rss > 0);
  assert.ok(manifest.build.peak_memory.rss >= manifest.build.peak_rss);
  assert.ok(manifest.build.memory_samples.length >= manifest.build.phases.length);
  assert.ok(manifest.build.memory_samples.every(sample => sample.phase && sample.rss > 0));
  assert.ok(manifest.build.counters.selected_term_spool_bytes > 0);
  assert.ok(manifest.build.counters.selected_term_spool_terms > 0);
  assert.ok(manifest.build.counters.doc_raw_spool_bytes > 0);
  assert.ok(manifest.build.counters.doc_gzip_spool_bytes > 0);
  assert.ok(manifest.build.counters.segment_files > 0);
  assert.ok(manifest.build.counters.segment_postings > 0);
  assert.equal(manifest.build.counters.field_row_fields, 6);
  assert.equal(manifest.build.counters.field_row_facet_fields, 2);
  assert.equal(manifest.build.counters.field_row_numeric_fields, 3);
  assert.equal(manifest.build.counters.field_row_boolean_fields, 1);
  assert.equal(manifest.build.counters.field_row_date_fields, 1);
  assert.ok(manifest.build.phases.some(phase => phase.name === "scan-and-spool" && phase.ms >= 0 && phase.cpu.user_us >= 0 && phase.peakMemory.rss > 0 && phase.disk.delta.build >= 0));
  assert.ok(manifest.build.phases.some(phase => phase.name === "reduce-postings" && phase.ms >= 0 && phase.disk.delta.final_packs >= 0 && phase.disk.delta.sidecars >= 0));
  assert.ok(manifest.build.phases.some(phase => phase.name === "query-bundles" && phase.ms >= 0));
  assert.ok(manifest.build.workers.some(group => group.phase === "scan-and-spool" && group.count === 2 && group.workers.every(worker => worker.mode === "worker-thread")));
  assert.ok(manifest.build.workers.some(group => group.phase === "reduce-postings" && group.count >= 1 && group.workers[0].tasks >= 1));
  const telemetryFile = JSON.parse(await readFile(join(root, "build-telemetry.json"), "utf8"));
  assert.equal(telemetryFile.format, "rfbuildtelemetry-v1");
  assert.equal(telemetryFile.phases.length, manifest.build.phases.length);
  assert.ok(manifest.stats.build_total_ms >= 0);
  assert.ok(manifest.stats.build_peak_rss > 0);
  assert.ok(manifest.stats.selected_term_spool_bytes > 0);
  assert.ok(manifest.stats.selected_term_spool_terms > 0);
  assert.ok(manifest.stats.doc_raw_spool_bytes > 0);
  assert.ok(manifest.stats.doc_gzip_spool_bytes > 0);
  assert.equal(manifest.stats.posting_segment_format, "rfsegpost-v6");
  assert.equal(manifest.stats.posting_segment_storage, "range-pack-v1");
  assert.equal(manifest.stats.posting_segment_block_storage, "range-pack-v1");
  assert.ok(manifest.stats.posting_segment_superblocks > 0);
  assert.ok(manifest.stats.posting_segment_superblock_terms > 0);
  assert.ok(manifest.stats.posting_segment_superblock_blocks >= manifest.stats.posting_segment_superblocks);
  assert.equal(manifest.stats.posting_segment_superblock_size, 16);
  assert.ok(manifest.stats.posting_segment_impact_tier_terms > 0);
  assert.ok(manifest.stats.posting_segment_impact_tier_blocks > 0);
  assert.ok(manifest.stats.posting_segment_impact_tier_tiers > 0);
  assert.equal(manifest.stats.posting_segment_impact_tier_min_blocks, 1);
  assert.equal(manifest.stats.posting_segment_impact_tier_max_blocks, 256);
  assert.equal(manifest.stats.posting_segment_doc_range_block_max, true);
  assert.equal(manifest.stats.posting_segment_doc_range_size, 1024);
  assert.equal(manifest.stats.posting_segment_doc_range_quantization_bits, 8);
  assert.ok(manifest.stats.posting_segment_doc_range_terms > 0);
  assert.ok(manifest.stats.posting_segment_doc_range_entries > 0);
  assert.ok(manifest.stats.posting_segment_doc_range_blocks > 0);
  assert.ok(manifest.stats.posting_segment_doc_range_block_entries > 0);
  assert.ok(manifest.stats.posting_segment_block_codec_pair_varint_blocks > 0);
  assert.ok(manifest.stats.posting_segment_block_codec_baseline_bytes >= manifest.stats.posting_segment_block_codec_selected_bytes);
  assert.equal(manifest.stats.term_storage, undefined);
  assert.equal(manifest.stats.posting_block_storage, undefined);
  assert.equal(manifest.stats.segment_format, "rfsegment-v1");
  assert.equal(manifest.stats.segment_manifest_format, "rfsegmentmanifest-v1");
  assert.equal(manifest.stats.segment_manifest_path, "segments/manifest.json.gz");
  assert.equal(manifest.stats.segment_manifest_published, true);
  assert.equal(manifest.stats.segment_manifest_storage, "static-segment-files-v1");
  assert.ok(manifest.stats.segment_manifest_bytes > 0);
  assert.equal(manifest.stats.scan_workers, 2);
  assert.equal(manifest.stats.partition_reducer_workers, 1);
  assert.equal(manifest.stats.partition_reducer_worker_mode, "main-thread");
  assert.ok(manifest.stats.segment_peak_memory_bytes > 0);
  assert.ok(manifest.stats.segment_max_docs > 0);
  assert.ok(Object.keys(manifest.stats.segment_flush_reasons).length > 0);
  assert.ok(manifest.stats.segment_effective_flush_bytes > 0);
  assert.equal(manifest.stats.scan_batch_docs, 2);
  assert.equal(manifest.stats.segment_merge_fan_in, 512);
  assert.equal(manifest.stats.segment_merge_policy, "tiered-log");
  assert.equal(manifest.stats.segment_merge_target_segments, 512);
  assert.equal(manifest.stats.segment_merge_blocked_by_temp_budget, false);
  assert.ok(manifest.stats.segment_merge_intermediate_bytes >= 0);
  assert.ok(manifest.stats.segment_merge_write_amplification >= 0);
  assert.equal(manifest.stats.segment_merge_tiers, 0);
  assert.ok(manifest.stats.segment_directory_spool_bytes > 0);
  assert.equal(manifest.stats.segment_directory_spool_entries, manifest.directory.entries);
  assert.ok(manifest.stats.segment_prefix_count_ms >= 0);
  assert.ok(manifest.stats.segment_partition_assembly_ms >= 0);
  assert.ok(manifest.stats.segment_files > 0);
  assert.ok(manifest.stats.segment_terms > 0);
  assert.ok(manifest.stats.segment_postings > 0);
  assert.equal(manifest.field_rows.format, "rffieldrows-v1");
  assert.equal(manifest.field_rows.source, "rf-build-code-store-v1");
  assert.equal(manifest.field_rows.fields.length, 6);
  assert.equal(manifest.stats.field_row_format, "rffieldrows-v1");
  assert.equal(manifest.stats.field_row_fields, 6);
  assert.equal(manifest.stats.field_row_facet_fields, 2);
  assert.equal(manifest.stats.field_row_numeric_fields, 3);
  assert.equal(manifest.stats.field_row_boolean_fields, 1);
  assert.equal(manifest.stats.field_row_date_fields, 1);
  assert.ok(manifest.stats.typo_index_terms > 0);
  assert.ok(await readFile(join(output, manifest.docs.pointers.file)));
  assert.ok(await readFile(join(output, manifest.docs.pointers.ordinals.file)));
  assert.ok(await readFile(join(output, manifest.docs.pages.pointers.file)));
  assert.ok(await readFile(join(output, "docs", "packs", manifest.docs.pointers.pack_table[0])));
  assert.ok(await readFile(join(output, "docs", "page-packs", manifest.docs.pages.pointers.pack_table[0])));
  assert.ok(await readFile(join(output, manifest.directory.root)));
  assert.ok(await readFile(join(output, "terms", "block-packs", manifest.object_store.pack_table.postingBlocks[0])));
  assert.ok(manifest.query_bundles);
  assert.equal(manifest.query_bundles.format, "rfqbundle-v1");
  assert.equal(manifest.query_bundles.coverage, "all-base-docs");
  assert.equal(manifest.query_bundles.row_group_size, 16);
  assert.ok(manifest.query_bundles.row_group_filter_fields > 0);
  assert.ok(manifest.query_bundles.keys > 0);
  assert.ok(await readFile(join(output, manifest.query_bundles.directory.root)));
  assert.ok(await readFile(join(output, "bundles", "packs", manifest.object_store.pack_table.queryBundles[0])));
  assert.ok(manifest.authority);
  assert.equal(manifest.authority.format, "rfauth-v1");
  assert.equal(manifest.authority.fields.length, 2);
  assert.ok(manifest.authority.keys > 0);
  assert.ok(await readFile(join(output, manifest.authority.directory.root)));
  assert.ok(await readFile(join(output, "authority", "packs", manifest.object_store.pack_table.authority[0])));
  assert.equal(manifest.doc_values.storage, "range-pack-v1");
  assert.equal(manifest.doc_values.lookup_chunk_size, 1);
  assert.ok(manifest.doc_values.fields.tags);
  assert.ok(manifest.doc_values.fields.published);
  assert.ok(manifest.doc_values.fields.tags.chunks[0].checksum.value);
  assert.ok(manifest.doc_values.fields.tags.lookup_chunks[0].checksum.value);
  assert.match(manifest.doc_values.fields.tags.chunks[0].pack, /^0000\.[0-9a-f]{24}\.bin$/u);
  assert.ok(await readFile(join(output, "doc-values", "packs", manifest.doc_values.fields.tags.chunks[0].pack)));
  assert.equal(manifest.filter_bitmaps.storage, "range-pack-v1");
  const tagBitmap = Object.values(manifest.filter_bitmaps.fields.tags.values)[0];
  assert.ok(tagBitmap);
  assert.ok(await readFile(join(output, "filter-bitmaps", "packs", tagBitmap.pack)));
  assert.equal(manifest.doc_value_sorted.storage, "range-pack-v1");
  assert.equal(manifest.doc_value_sorted.directory_format, "rfdocvaluesortdir-v1");
  assert.equal(manifest.doc_value_sorted.page_format, "rfdocvaluesortpage-v1");
  assert.ok(manifest.doc_value_sorted.fields.published);
  assert.ok(manifest.doc_value_sorted.fields.featured);
  assert.match(manifest.doc_value_sorted.pack_table[0], /^0000\.[0-9a-f]{24}\.bin$/u);
  assert.ok(await readFile(join(output, manifest.doc_value_sorted.fields.published.directory.file)));
  assert.ok(await readFile(join(output, "doc-values", "sorted-packs", manifest.doc_value_sorted.pack_table[0])));
  assert.equal(manifest.sort_replicas.format, "rfsortreplicas-v1");
  assert.equal(manifest.sort_replicas.count, 1);
  assert.equal(manifest.sort_replicas.replicas["year:desc"].field, "year");
  assert.equal(manifest.sort_replicas.replicas["year:desc"].doc_pages.order, "sort-rank-page");
  assert.equal(manifest.sort_replicas.replicas["year:desc"].doc_pages.role, "display");
  assert.equal(manifest.sort_replicas.replicas["year:desc"].doc_pages.pointers.order, "sort-rank-page");
  assert.ok(await readFile(join(output, manifest.sort_replicas.replicas["year:desc"].terms.directory.root)));
  assert.ok(await readFile(join(output, "sort-replicas", "year_desc", "rank-packs", manifest.sort_replicas.replicas["year:desc"].rank_map.pack_table[0])));
  assert.ok(await readFile(join(output, manifest.sort_replicas.replicas["year:desc"].doc_pages.pointers.file)));
  assert.ok(await readFile(join(output, "sort-replicas", "year_desc", "docs", "page-packs", manifest.sort_replicas.replicas["year:desc"].doc_pages.pack_table[0])));
  assert.equal(manifest.facets.category.count, 5);
  assert.equal(manifest.facet_dictionaries.storage, "range-pack-v1");
  assert.equal(manifest.facet_dictionaries.directory.format, "rfdir-v2");
  assert.ok(manifest.facet_dictionaries.fields.category);
  assert.ok(await readFile(join(output, manifest.facet_dictionaries.directory.root)));
  assert.match(manifest.facet_dictionaries.directory.pack_table[0], /^0000\.[0-9a-f]{24}\.bin$/u);
  assert.ok(await readFile(join(output, "facets", "packs", manifest.facet_dictionaries.directory.pack_table[0])));
  assert.match(manifest.typo.manifest, /^typo\/manifest\.[0-9a-f]{24}\.json$/u);
  assert.ok(await readFile(join(output, manifest.typo.manifest)));
  assert.ok(await readFile(join(output, manifest.typo.directory.root)));

  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });
  assert.ok(server.requests.some(request => request.pathname.endsWith("/manifest.min.json")));
  assert.equal(server.requests.some(request => request.pathname.endsWith("/manifest.full.json")), false);
  assert.equal(server.requests.some(request => request.pathname.endsWith("/debug/build-telemetry.json")), false);
  assert.equal(server.requests.some(request => request.pathname.endsWith("/debug/index-optimizer.json")), false);
  const lazyTelemetry = await search.loadBuildTelemetry();
  assert.equal(lazyTelemetry.format, "rfbuildtelemetry-v1");
  assert.ok(server.requests.some(request => request.pathname.endsWith("/debug/build-telemetry.json")));
  assert.equal(server.requests.some(request => request.pathname.endsWith("/debug/index-optimizer.json")), false);
  const lazyOptimizer = await search.loadIndexOptimizer();
  assert.equal(lazyOptimizer.format, "rfoptimizer-v1");
  assert.ok(server.requests.some(request => request.pathname.endsWith("/debug/index-optimizer.json")));
  assert.equal(server.requests.some(request => request.pathname.endsWith("/manifest.full.json")), false);
  const lazySegments = await search.loadSegmentManifest();
  assert.equal(lazySegments.format, "rfsegmentmanifest-v1");
  assert.equal(lazySegments.totalDocs, 9);
  assert.ok(server.requests.some(request => request.pathname.endsWith("/segments/manifest.json.gz")));
  assert.equal(server.requests.some(request => request.pathname.endsWith("/manifest.full.json")), false);

  const sortOnlySearch = await createSearch({ baseUrl: server.baseUrl });
  const sortOnlyInitial = await sortOnlySearch.search({ q: "", sort: "-year", size: 2 });
  assert.deepEqual(sortOnlyInitial.results.map(result => result.id), ["a", "c"]);
  assert.ok(server.requests.some(request => request.pathname.endsWith("/doc-values/sorted/manifest.json.gz")));
  assert.equal(server.requests.some(request => request.pathname.endsWith("/manifest.full.json")), false);
  const sortOnlyText = await sortOnlySearch.search({ q: "search", sort: "-year", size: 2, rerank: false });
  assert.equal(sortOnlyText.stats.plannerLane, "sortReplicaText");
  assert.equal(server.requests.some(request => request.pathname.endsWith("/manifest.full.json")), false);

  const results = await search.search({ q: "static range search", size: 3 });
  assert.equal(results.results[0].title, "Static range search");
  assert.equal(results.results[0].bodySnippet, "Rangefind builds");
  assert.ok(results.stats.shards > 0);
  assert.equal(results.stats.docPayloadLane, "docPages");
  assert.equal(results.stats.docPayloadAdaptive, true);

  const exactResults = await search.search({ q: "static range search", size: 3, exact: true });
  assert.deepEqual(
    results.results.map(result => result.title),
    exactResults.results.map(result => result.title)
  );
  assert.equal(exactResults.stats.plannerFallbackReason, "exact_requested");
  assert.equal(exactResults.stats.plannerLane, "segmentFanoutExact");
  assert.ok(exactResults.stats.segmentEntries > 0);

  const singleTerm = await search.search({ q: "search", size: 2, rerank: false });
  const singleTermExact = await search.search({ q: "search", size: 2, exact: true, rerank: false });
  assert.deepEqual(
    singleTerm.results.map(result => result.title),
    singleTermExact.results.map(result => result.title)
  );
  assert.equal(singleTermExact.stats.plannerLane, "segmentFanoutExact");
  assert.ok(singleTermExact.stats.segmentEntries > 0);
  assert.ok(singleTerm.stats.topKProofAttempts > 0);
  assert.ok(singleTerm.stats.topKProofSuccesses > 0);
  assert.equal(singleTerm.stats.topKProofFailureReason, "");
  assert.equal(singleTerm.stats.topKProofSortAware, false);

  const segmentFilteredSorted = await search.search({
    q: "search",
    size: 3,
    exact: true,
    rerank: false,
    sort: "-year",
    filters: { facets: { tags: ["static"] }, numbers: { year: { min: 2026 } } }
  });
  const forceMergedSearch = await createSearch({ baseUrl: server.baseUrl, segmentFanout: false });
  const forceMergedFilteredSorted = await forceMergedSearch.search({
    q: "search",
    size: 3,
    exact: true,
    rerank: false,
    sort: "-year",
    filters: { facets: { tags: ["static"] }, numbers: { year: { min: 2026 } } }
  });
  assert.equal(segmentFilteredSorted.stats.plannerLane, "segmentFanoutExact");
  assert.equal(forceMergedFilteredSorted.stats.plannerLane, "fullFallback");
  assert.deepEqual(
    segmentFilteredSorted.results.map(result => result.id),
    forceMergedFilteredSorted.results.map(result => result.id)
  );

  const missingBaseTerm = await search.search({ q: "static zzzzzrangefindaucunresultatzzzzextra", size: 2, rerank: false });
  assert.equal(missingBaseTerm.total, 0);
  assert.equal(missingBaseTerm.stats.plannerLane, "empty");
  assert.equal(missingBaseTerm.stats.blocksDecoded, 0);
  assert.ok(missingBaseTerm.stats.missingBaseTerms > 0);

  const bundled = await search.search({ q: "static range", size: 2, rerank: false });
  assert.equal(bundled.stats.plannerLane, "queryBundleExact");
  assert.equal(bundled.stats.topKProven, true);
  assert.equal(bundled.stats.totalExact, true);
  assert.equal(bundled.stats.blocksDecoded, 0);
  assert.equal(bundled.stats.postingsDecoded, 0);

  const filteredBundled = await search.search({
    q: "static range",
    size: 2,
    rerank: false,
    filters: { facets: { tags: ["static"] }, booleans: { featured: true } }
  });
  assert.deepEqual(filteredBundled.results.map(result => result.id), ["a"]);
  assert.equal(filteredBundled.stats.plannerLane, "queryBundleExact");
  assert.equal(filteredBundled.stats.queryBundleFiltered, true);
  assert.ok(filteredBundled.stats.queryBundleRowGroups > 0);
  assert.ok(filteredBundled.stats.queryBundleRowGroupsScanned <= filteredBundled.stats.queryBundleRowGroups);
  assert.ok(filteredBundled.stats.docValueRowsScanned <= filteredBundled.stats.queryBundleRows);
  assert.equal(filteredBundled.stats.queryBundleFilterValueSource, "queryBundleRows");
  assert.equal(filteredBundled.stats.docValueRowsScanned, 0);
  assert.equal(filteredBundled.stats.blocksDecoded, 0);

  server.requests.length = 0;
  const fullRequestsBeforeLazyFilter = server.requests.filter(request => request.pathname.endsWith("/manifest.full.json")).length;
  const lazyFilterSearch = await createSearch({ baseUrl: server.baseUrl, queryBundles: false });
  const lazyFilteredBundled = await lazyFilterSearch.search({
    q: "static range",
    size: 2,
    rerank: false,
    filters: { facets: { tags: ["static"] }, booleans: { featured: true } }
  });
  assert.deepEqual(lazyFilteredBundled.results.map(result => result.id), ["a"]);
  assert.equal(server.requests.filter(request => request.pathname.endsWith("/manifest.full.json")).length, fullRequestsBeforeLazyFilter);
  assert.ok(server.requests.some(request => request.pathname.endsWith("/facets/manifest.json.gz")));
  assert.ok(server.requests.some(request => request.pathname.endsWith("/filter-bitmaps/manifest.json.gz")));
  assert.equal(server.requests.some(request => request.pathname.includes("/doc-values/packs/")), false);

  const bundledExact = await search.search({ q: "static range", size: 2, exact: true, rerank: false });
  assert.deepEqual(
    bundled.results.map(result => result.title),
    bundledExact.results.map(result => result.title)
  );

  const authority = await search.search({ q: "authoritative alias", size: 3 });
  assert.equal(authority.results[0].title, "SQLite retrieval baseline");
  assert.equal(authority.stats.authorityApplied, true);
  assert.equal(authority.stats.authorityInjected, 1);

  const typo = await search.search({ q: "statik range search", size: 3 });
  assert.equal(typo.results[0].title, "Static range search");
  assert.equal(typo.correctedQuery, "static range search");
  assert.deepEqual(typo.corrections.map(item => item.to), ["static"]);
  assert.equal(typo.stats.typoApplied, true);

  const deletionTypo = await search.search({ q: "statc range search", size: 3 });
  assert.equal(deletionTypo.results[0].title, "Static range search");
  assert.equal(deletionTypo.correctedQuery, "static range search");
  assert.equal(deletionTypo.stats.typoApplied, true);
  assert.equal(deletionTypo.stats.typoShardLookups, 1);

  const stemmedTypo = await search.search({ q: "elecrtified winding insulation", size: 3 });
  assert.equal(stemmedTypo.results[0].title, "Electrified winding insulation");
  assert.equal(stemmedTypo.stats.typoApplied, true);
  assert.ok(stemmedTypo.stats.typoLexiconShardLookups > 0);

  const surfaceFallback = await search.search({ q: "paris", size: 3 });
  assert.equal(surfaceFallback.results[0].title, "Pariser cannon");
  assert.equal(surfaceFallback.stats.surfaceFallbackApplied, true);
  assert.equal(surfaceFallback.stats.typoAttempted, false);

  const filtered = await search.search({
    q: "search",
    filters: {
      facets: { tags: ["static"] },
      numbers: { year: { min: 2026 } },
      booleans: { featured: true }
    }
  });
  assert.deepEqual(filtered.results.map(result => result.id), ["a"]);

  const signedNumericFiltered = await search.search({
    q: "client search runtime",
    filters: {
      numbers: { temperature: { min: -1, max: 0 } },
      booleans: { featured: false }
    }
  });
  assert.deepEqual(signedNumericFiltered.results.map(result => result.id), ["c"]);

  const dateFiltered = await search.search({
    q: "",
    filters: {
      facets: { category: ["runtime"] },
      numbers: { published: { min: "2026-03-01", max: "2026-12-31" } }
    },
    sort: { field: "published", order: "desc" }
  });
  assert.deepEqual(dateFiltered.results.map(result => result.id), ["d", "c"]);

  const sortedInitial = await search.search({ q: "", sort: "-year", size: 2 });
  assert.deepEqual(sortedInitial.results.map(result => result.id), ["a", "c"]);
  assert.equal(sortedInitial.stats.docPayloadLane, "docPages");
  assert.equal(sortedInitial.stats.docValuePruning, true);
  assert.equal(sortedInitial.stats.docValuePruneField, "year");

  const sortedText = await search.search({ q: "search", sort: "-year", size: 2, rerank: false });
  const sortedTextExact = await search.search({ q: "search", sort: "-year", size: 2, exact: true, rerank: false });
  assert.deepEqual(
    sortedText.results.map(result => result.id),
    sortedTextExact.results.map(result => result.id)
  );
  assert.equal(sortedText.stats.plannerLane, "sortReplicaText");
  assert.equal(sortedText.stats.sortReplicaText, true);
  assert.equal(sortedText.stats.sortReplicaField, "year");
  assert.equal(sortedText.stats.topKProofSortAware, true);
  assert.equal(sortedText.stats.docPayloadLane, "sortReplicaDocPages");
  assert.equal(sortedText.stats.docPayloadForced, false);
  assert.ok(sortedText.stats.blocksDecoded <= sortedTextExact.stats.blocksDecoded);
  assert.ok(sortedText.stats.sortReplicaRankChunksWanted >= 1);
  assert.ok(sortedText.stats.sortReplicaDocPagesFetched >= 1);
  assert.ok(sortedText.stats.sortReplicaWantedBlocks >= sortedText.stats.blocksDecoded);

  const pageTable = parseDocPagePointerPage(await readFile(join(output, manifest.docs.pages.pointers.file)), {
    packTable: manifest.docs.pages.pointers.pack_table
  });
  assert.equal(pageTable.entries.length, Math.ceil(manifest.total / manifest.docs.pages.page_size));

  const pointerTable = parseDocPointerPage(await readFile(join(output, manifest.docs.pointers.file)), {
    packTable: manifest.docs.pointers.pack_table
  });
  const ordinalTable = parseDocOrdinalTable(await readFile(join(output, manifest.docs.pointers.ordinals.file)));
  const firstDocPointer = pointerTable.entries[ordinalTable.entries[0]];
  const docPack = join(output, "docs", "packs", firstDocPointer.pack);
  const corrupted = Buffer.from(await readFile(docPack));
  corrupted[firstDocPointer.offset] ^= 0xff;
  await writeFile(docPack, corrupted);
  const corruptSearch = await createSearch({ baseUrl: server.baseUrl, textDocPageHydration: false });
  await assert.rejects(
    () => corruptSearch.search({ q: "static range search", size: 1 }),
    /checksum mismatch/
  );
});

test("builder can reduce posting partitions in worker-owned packs", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-worker-reduce-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, [
    JSON.stringify({ id: "a", title: "alpha beta", url: "/a" }),
    JSON.stringify({ id: "b", title: "alpha gamma", url: "/b" }),
    JSON.stringify({ id: "c", title: "delta epsilon", url: "/c" }),
    JSON.stringify({ id: "d", title: "zeta eta", url: "/d" })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    externalPostingBlocks: true,
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0,
    postingBlockSize: 1,
    partitionReducerWorkers: 2,
    queryBundles: false,
    typo: false,
    baseShardDepth: 1,
    maxShardDepth: 2,
    targetShardPostings: 1,
    fields: [{ name: "title", path: "title", weight: 1 }]
  }));

  await build({ configPath });
  const manifest = JSON.parse(await readFile(join(output, "manifest.json"), "utf8"));
  const reduceWorkers = manifest.build.workers.find(group => group.phase === "reduce-postings");
  assert.equal(reduceWorkers.count, 2);
  assert.ok(reduceWorkers.workers.every(worker => worker.mode === "worker-thread"));
  assert.ok(manifest.object_store.pack_table.terms.length <= 2);
  assert.ok(manifest.object_store.pack_table.terms.every(file => /^\d{4}\.[0-9a-f]{24}\.bin$/u.test(file)));
  assert.ok(manifest.object_store.pack_table.postingBlocks.length > 0);
  assert.ok(manifest.object_store.pack_table.postingBlocks.length <= 2);
  assert.ok(manifest.object_store.pack_table.postingBlocks.every(file => /^\d{4}\.[0-9a-f]{24}\.bin$/u.test(file)));
  assert.equal(manifest.stats.posting_segment_block_storage, "range-pack-v1");
  assert.ok(manifest.stats.external_posting_segment_blocks > 0);
  assert.equal(manifest.stats.partition_reducer_workers, 2);
  assert.equal(manifest.stats.partition_reducer_worker_mode, "worker-thread-owned-packs");
  assert.ok(manifest.stats.segment_partition_spool_bytes > 0);
  assert.ok(manifest.stats.segment_partition_spool_entries > 0);
  assert.ok(manifest.stats.partition_reducer_credit_limit_bytes > 0);
  assert.ok(manifest.stats.partition_reducer_max_active_input_bytes > 0);
  assert.equal(manifest.stats.partition_reducer_finish_mode, "staggered");
  assert.ok(manifest.stats.code_store_worker_cache_chunks > 0);

  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });
  const results = await search.search({ q: "alpha", size: 2, exact: true, rerank: false });
  assert.deepEqual(results.results.map(result => result.id), ["a", "b"]);
});

test("query bundles progressively verify numeric filters before doc-value exhaustion", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-query-bundle-filter-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  const docs = Array.from({ length: 80 }, (_, index) => JSON.stringify({
    id: String(index),
    title: index < 10 ? `Needle anchor priority ${index}` : `Body match ${index}`,
    body: index < 10 ? "priority body" : "Needle filler anchor background body",
    rank: index < 10 ? 1 : 0,
    url: `/${index}`
  }));
  await writeFile(docsPath, docs.join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    docValueChunkSize: 4,
    baseShardDepth: 1,
    maxShardDepth: 1,
    targetShardPostings: 1000,
    queryBundles: true,
    queryBundleMaxRows: 64,
    queryBundleRowGroupSize: 16,
    queryBundleMinSeedDocs: 2,
    fields: [
      { name: "title", path: "title", weight: 4.5, phrase: true },
      { name: "body", path: "body", weight: 1.0 }
    ],
    numbers: [{ name: "rank", path: "rank" }],
    display: ["title", "url", "rank"]
  }));

  await build({ configPath });
  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });

  const result = await search.search({
    q: "needle anchor",
    size: 10,
    rerank: false,
    filters: { numbers: { rank: { min: 1 } } }
  });

  assert.deepEqual(result.results.map(item => item.id), Array.from({ length: 10 }, (_, index) => String(index)));
  assert.equal(result.stats.plannerLane, "queryBundleExact");
  assert.equal(result.stats.queryBundleFiltered, true);
  assert.equal(result.stats.queryBundleFilterProgressive, true);
  assert.equal(result.stats.queryBundleFilterProof, "queryBundleRows");
  assert.equal(result.stats.queryBundleFilterValueSource, "queryBundleRows");
  assert.equal(result.stats.queryBundleFilterExhausted, false);
  assert.equal(result.stats.queryBundleFilterRowsScanned, 10);
  assert.equal(result.stats.queryBundleFilterRowsAccepted, 10);
  assert.equal(result.stats.docValueRowsScanned, 0);
  assert.equal(result.stats.docValueRowsAccepted, 0);
  assert.ok(result.stats.queryBundleFilterRowsScanned < result.stats.queryBundleRows);
  assert.equal(result.stats.blocksDecoded, 0);
  assert.equal(result.approximate, true);
});

test("failed builds write diagnostics and clean partial scratch state", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-build-failure-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, [
    JSON.stringify({ id: "a", title: "Valid document", body: "This row is fine.", url: "/a" }),
    "{ bad json"
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    fields: [{ name: "title", path: "title" }],
    display: ["title", "url"]
  }));

  await assert.rejects(() => build({ configPath }), /JSON|Unexpected/u);
  await stat(join(output, "_build"));
  const failure = JSON.parse(await readFile(join(output, "debug", "build-failure.json"), "utf8"));
  assert.equal(failure.status, "failed");
  assert.equal(failure.cleanup.preserved, "_build/resume");
  const failedTelemetry = JSON.parse(await readFile(join(output, "debug", "build-telemetry.failed.json"), "utf8"));
  assert.equal(failedTelemetry.status, "failed");
  assert.ok(failedTelemetry.error.message);
});

test("static-large budget keeps all docs, always indexes key fields, and caps body indexing", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-budget-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, [
    JSON.stringify({ id: "a", title: "RareTitle", categories: "KeptCategory", body: "earlybody lateunindexed retained display text", url: "/a" }),
    JSON.stringify({ id: "b", title: "Other article", categories: "OtherCategory", body: "lateunindexed only in body", url: "/b" })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    targetPostingsPerDoc: 0,
    bodyIndexChars: 10,
    alwaysIndexFields: ["title", "categories"],
    queryBundles: false,
    fields: [
      { name: "title", path: "title", weight: 5 },
      { name: "categories", path: "categories", weight: 3 },
      { name: "body", path: "body", weight: 1 }
    ],
    display: ["title", "url", { name: "bodySnippet", path: "body", maxChars: 12 }]
  }));

  await build({ configPath });
  const manifest = JSON.parse(await readFile(join(output, "manifest.json"), "utf8"));
  assert.equal(manifest.total, 2);
  assert.equal(manifest.stats.target_postings_per_doc, 0);
  assert.equal(manifest.stats.body_index_chars, 10);
  assert.deepEqual(manifest.stats.always_index_fields, ["title", "categories"]);

  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });
  const title = await search.search({ q: "raretitle", size: 2 });
  const category = await search.search({ q: "keptcategory", size: 2 });
  const body = await search.search({ q: "lateunindexed", size: 2 });
  assert.equal(title.results[0].id, "a");
  assert.equal(title.results[0].bodySnippet, "earlybody la");
  assert.equal(category.results[0].id, "a");
  assert.equal(body.total, 0);
});

test("resumable builds reuse scan and reduce stages and keep old manifest on failure", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-resume-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  const baseConfig = {
    input: "docs.jsonl",
    output: "public/rangefind",
    queryBundles: false,
    fields: [{ name: "title", path: "title", weight: 1 }],
    display: ["title", "url"]
  };
  await writeFile(docsPath, [
    JSON.stringify({ id: "a", title: "Alpha", url: "/a" }),
    JSON.stringify({ id: "b", title: "Beta", url: "/b" })
  ].join("\n"));

  await writeFile(configPath, JSON.stringify({ ...baseConfig, debugFailAfterStage: "scan" }));
  await assert.rejects(() => build({ configPath }), /debug failure after scan/u);
  const scanStage = await resumeStageFile(output, "scan");
  const scanBefore = await readFile(scanStage, "utf8");
  await writeFile(configPath, JSON.stringify(baseConfig));
  await build({ configPath });
  assert.equal(await readFile(scanStage, "utf8"), scanBefore);
  const firstManifest = JSON.parse(await readFile(join(output, "manifest.json"), "utf8"));
  assert.equal(firstManifest.total, 2);

  const reduceRoot = await mkdtemp(join(tmpdir(), "rangefind-resume-reduce-"));
  const reduceDocsPath = join(reduceRoot, "docs.jsonl");
  const reduceOutput = join(reduceRoot, "public", "rangefind");
  const reduceConfigPath = join(reduceRoot, "rangefind.config.json");
  const reduceConfig = { ...baseConfig, input: "docs.jsonl", output: "public/rangefind" };
  await writeFile(reduceDocsPath, [
    JSON.stringify({ id: "a", title: "Alpha", url: "/a" }),
    JSON.stringify({ id: "b", title: "Beta", url: "/b" })
  ].join("\n"));
  await writeFile(reduceConfigPath, JSON.stringify({ ...reduceConfig, debugFailAfterStage: "reduce" }));
  await assert.rejects(() => build({ configPath: reduceConfigPath }), /debug failure after reduce/u);
  const reduceStage = await resumeStageFile(reduceOutput, "reduce");
  const reduceBefore = await readFile(reduceStage, "utf8");
  await writeFile(reduceConfigPath, JSON.stringify(reduceConfig));
  await build({ configPath: reduceConfigPath });
  assert.equal(await readFile(reduceStage, "utf8"), reduceBefore);

  await writeFile(docsPath, [
    JSON.stringify({ id: "a", title: "Changed", url: "/a" }),
    JSON.stringify({ id: "b", title: "Beta", url: "/b" }),
    JSON.stringify({ id: "c", title: "Gamma", url: "/c" })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({ ...baseConfig, debugFailAfterStage: "scan" }));
  await assert.rejects(() => build({ configPath }), /debug failure after scan/u);
  const stillPublished = JSON.parse(await readFile(join(output, "manifest.json"), "utf8"));
  assert.equal(stillPublished.total, 2);
});

test("builder resolves auto posting layout into manifest and optimizer report", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-auto-layout-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, Array.from({ length: 12 }, (_, index) => JSON.stringify({
    id: String(index),
    title: `Auto layout document ${index}`,
    body: "common auto layout corpus statistics",
    url: `/${index}`
  })).join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    codecs: { mode: "auto" },
    postingBlockSize: "auto",
    postingSuperblockSize: "auto",
    queryBundles: false,
    fields: [
      { name: "title", path: "title", weight: 2.0 },
      { name: "body", path: "body", weight: 1.0 }
    ],
    display: ["title", "url"]
  }));

  await build({ configPath });
  const manifest = JSON.parse(await readFile(join(output, "manifest.json"), "utf8"));
  const optimizerReport = JSON.parse(await readFile(join(output, "debug", "index-optimizer.json"), "utf8"));
  const codecLayout = optimizerReport.core.find(item => item.kind === "codec-layout");
  assert.equal(manifest.stats.posting_segment_block_size_source, "auto");
  assert.equal(manifest.stats.posting_segment_superblock_size_source, "auto");
  assert.ok(manifest.stats.posting_segment_block_size > 0);
  assert.ok(manifest.stats.posting_segment_superblock_size > 0);
  assert.equal(codecLayout.mode, "auto");
  assert.equal(codecLayout.block_size_source, "auto");
  assert.equal(codecLayout.superblock_size_source, "auto");
  assert.equal(codecLayout.selected_codec, "term-sampled-auto-block-codec");
  assert.equal(manifest.stats.posting_segment_codec_planner_mode, "auto");
  assert.ok(manifest.stats.posting_segment_codec_planner_sampled_terms > 0);
  assert.ok(manifest.stats.posting_segment_codec_planner_sampled_blocks > 0);
});

test("runtime refills high-df posting block windows in batches", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-frontier-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  const docs = Array.from({ length: 40 }, (_, index) => JSON.stringify({
    id: String(index),
    title: `Common document ${String(index).padStart(2, "0")}`,
    body: "common marker text",
    category: index % 2 === 0 ? "even" : "odd",
    bucket: index < 20 ? "head" : "tail",
    unique: `u${index}`,
    tail: index >= 20,
    url: `/${index}`
  }));
  await writeFile(docsPath, docs.join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    baseShardDepth: 1,
    maxShardDepth: 1,
    targetShardPostings: 1000,
    postingBlockSize: 1,
    blockFilterMaxFacetWords: 1,
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0,
    queryBundles: false,
    fields: [
      { name: "title", path: "title", weight: 2.0 },
      { name: "body", path: "body", weight: 1.0 }
    ],
    facets: [{ name: "category", path: "category" }, { name: "bucket", path: "bucket" }, { name: "unique", path: "unique" }],
    booleans: [{ name: "tail", path: "tail" }],
    display: ["title", "url", "category", "bucket", "unique", "tail"]
  }));

  await build({ configPath });
  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });
  server.requests.length = 0;

  const results = await search.search({ q: "common", size: 40, rerank: false });
  const postingBlockRequests = server.requests.filter(request => request.pathname.includes("/terms/block-packs/"));
  assert.equal(results.results.length, 40);
  assert.equal(results.stats.blocksDecoded, 40);
  assert.equal(results.stats.postingBlockFrontierFetchedBlocks, 40);
  assert.equal(results.stats.postingBlockFrontierFetchGroups, 3);
  assert.equal(results.stats.postingSuperblockScheduler, true);
  assert.ok(results.stats.postingSuperblocks > 0);
  assert.equal(results.stats.postingSuperblocksSkipped, 0);
  assert.ok(results.stats.postingSuperblocksDecoded > 0);
  assert.equal(postingBlockRequests.length, 3);
  assert.equal(results.stats.plannerFallbackReason, "");
  assert.equal(results.stats.topKProofFailureReason, "");
  assert.ok(results.stats.topKProofSuccesses > 0);
  assert.ok(results.stats.topKProofFailureCandidateCount > 0);

  const tieProof = await search.search({ q: "common", size: 5, rerank: false });
  assert.equal(tieProof.results.length, 5);
  assert.deepEqual(tieProof.results.map(result => result.id), ["0", "1", "2", "3", "4"]);
  assert.equal(tieProof.stats.blocksDecoded, 5);
  assert.equal(tieProof.stats.topKProofFailureReason, "");
  assert.equal(tieProof.stats.topKProofFailureTieBound, 0);
  assert.equal(tieProof.stats.topKProofMaxOutsidePotential, tieProof.stats.topKProofThreshold);
  assert.ok(tieProof.stats.topKProofRemainingTerms > 0);
  assert.ok(tieProof.stats.topKProofRemainingTermUpperBound > 0);

  const exhausted = await search.search({ q: "common", size: 41, rerank: false });
  assert.equal(exhausted.stats.plannerFallbackReason, "tail_exhausted");
  assert.equal(exhausted.stats.topKProofFailureReason, "tail_exhausted");

  const multiTerm = await search.search({ q: "common marker", size: 5, rerank: false });
  assert.ok(multiTerm.stats.terms > 1);
  assert.ok(multiTerm.stats.topKProofAttempts > 0);
  assert.equal(multiTerm.stats.topKProofSortAware, false);

  const filtered = await search.search({
    q: "common",
    size: 5,
    rerank: false,
    filters: { facets: { category: ["even"] } }
  });
  assert.equal(filtered.results.length, 5);
  assert.ok(filtered.results.every(result => Number(result.id) % 2 === 0));
  assert.equal(filtered.stats.topKProofFilterAware, true);

  const tailFiltered = await search.search({
    q: "common",
    size: 5,
    rerank: false,
    filters: { facets: { bucket: ["tail"] } }
  });
  assert.equal(tailFiltered.results.length, 5);
  assert.ok(tailFiltered.results.every(result => result.bucket === "tail"));
  assert.ok(tailFiltered.stats.postingSuperblocksConsidered > 0);
  assert.ok(tailFiltered.stats.postingSuperblocksSkipped > 0);
  assert.ok(tailFiltered.stats.skippedBlocks >= 16);

  const booleanSummaryFiltered = await search.search({
    q: "common",
    size: 5,
    rerank: false,
    filters: { booleans: { tail: true } }
  });
  assert.equal(booleanSummaryFiltered.results.length, 5);
  assert.ok(booleanSummaryFiltered.results.every(result => result.tail === true));
  assert.ok(booleanSummaryFiltered.stats.filterSummaryProofBlocks > 0);

  const unknownFiltered = await search.search({
    q: "common",
    size: 5,
    rerank: false,
    filters: { facets: { unique: ["u22"] } }
  });
  assert.deepEqual(unknownFiltered.results.map(result => result.id), ["22"]);
  assert.equal(unknownFiltered.stats.topKProofFilterAware, true);
  assert.equal(unknownFiltered.stats.topKProofFilterUnknown, true);
  assert.equal(unknownFiltered.stats.topKProofUnknownFilterFields, "unique");

  const largePageFallback = await search.search({ q: "common", page: 2, size: 100, rerank: false });
  assert.equal(largePageFallback.stats.plannerLane, "fullFallback");
  assert.equal(largePageFallback.stats.plannerFallbackReason, "top_k_limit");
  assert.equal(largePageFallback.stats.topKProofFailureReason, "top_k_limit");
  assert.equal(largePageFallback.stats.topKProofAttempts, 0);
});

test("doc-range planner batches candidate blocks with inner proof stats", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-doc-range-inner-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  const docs = Array.from({ length: 60 }, (_, index) => {
    const head = index < 5;
    return JSON.stringify({
      id: String(index),
      title: head ? "alpha beta alpha beta alpha beta" : `document ${index}`,
      body: "alpha beta",
      url: `/${index}`
    });
  });
  await writeFile(docsPath, docs.join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    baseShardDepth: 1,
    maxShardDepth: 1,
    targetShardPostings: 1000,
    postingBlockSize: 1,
    postingDocRangeBlockMax: true,
    postingImpactTiers: true,
    postingDocRangeSize: 5,
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0,
    queryBundles: false,
    fields: [
      { name: "title", path: "title", weight: 4.0 },
      { name: "body", path: "body", weight: 1.0 }
    ],
    display: ["title", "url"]
  }));

  await build({ configPath });
  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({
    baseUrl: server.baseUrl,
    docRangeBlockPruneBatchSize: 2,
    docRangeBlockPruneInitialBatchSize: 1
  });

  const result = await search.search({ q: "alpha beta", size: 5, rerank: false });
  assert.equal(result.stats.plannerLane, "docRangeBlockMax");
  assert.deepEqual(result.results.map(row => row.id), ["0", "1", "2", "3", "4"]);
  assert.equal(result.stats.docRangeImpactPlanner, true);
  assert.equal(result.stats.docRangeImpactSeed, false);
  assert.ok(result.stats.docRangeImpactTierTerms > 0);
  assert.ok(result.stats.docRangeImpactTierTasks > 0);
  assert.ok(result.stats.docRangeInnerBlockBatches > 0);
  assert.ok(result.stats.docRangePostingBlocksProcessed <= result.stats.docRangePostingBlocksCandidate);
  assert.ok(result.stats.docRangeNextUpperBound < result.stats.topKProofThreshold);
});

test("doc-range planner falls back when sampled candidate blocks are too broad", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-doc-range-broad-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  const docs = Array.from({ length: 60 }, (_, index) => JSON.stringify({
    id: String(index),
    title: index < 5 ? "alpha alpha alpha alpha" : "alpha",
    body: `document ${index}`,
    url: `/${index}`
  }));
  await writeFile(docsPath, docs.join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    baseShardDepth: 1,
    maxShardDepth: 1,
    targetShardPostings: 1000,
    postingBlockSize: 1,
    postingDocRangeSize: 20,
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0,
    queryBundles: false,
    fields: [
      { name: "title", path: "title", weight: 4.0 },
      { name: "body", path: "body", weight: 1.0 }
    ],
    display: ["title", "url"]
  }));

  await build({ configPath });
  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });

  const result = await search.search({ q: "alpha", size: 5, rerank: false });
  assert.equal(result.stats.plannerLane, "tailProof");
  assert.deepEqual(result.results.map(row => row.id), ["0", "1", "2", "3", "4"]);
  assert.equal(Boolean(result.stats.docRangeBlockMax), false);
});
