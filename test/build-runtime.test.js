import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
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
    buildTelemetrySampleMs: 5,
    buildTelemetryPath: "build-telemetry.json",
    scanWorkers: 2,
    scanBatchDocs: 2,
    baseShardDepth: 2,
    maxShardDepth: 3,
    targetShardPostings: 2,
    postingBlockSize: 2,
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
    display: ["title", "url", "category", "tags", "year", "temperature", "published", "featured", { name: "bodySnippet", path: "body", maxChars: 16 }]
  }));

  await build({ configPath });
  assert.ok(await readFile(join(output, "manifest.json"), "utf8"));
  const manifest = JSON.parse(await readFile(join(output, "manifest.json"), "utf8"));
  const minimalManifest = JSON.parse(await readFile(join(output, "manifest.min.json"), "utf8"));
  assert.equal(minimalManifest.lazy_manifests.full, "manifest.full.json");
  assert.equal(minimalManifest.lazy_manifests.build, "debug/build-telemetry.json");
  assert.equal(minimalManifest.lazy_manifests.doc_values, "doc-values/manifest.json.gz");
  assert.ok(await readFile(join(output, "manifest.full.json"), "utf8"));
  assert.ok(await readFile(join(output, "doc-values", "manifest.json.gz")));
  assert.ok(await readFile(join(output, "debug", "build-telemetry.json"), "utf8"));
  assert.equal(minimalManifest.build, undefined);
  assert.equal(minimalManifest.doc_values, undefined);
  assert.ok(minimalManifest.block_filters);
  assert.equal(minimalManifest.typo.directory, undefined);
  assert.equal(minimalManifest.facets.category.count, 5);
  assert.equal(manifest.features.checksummedObjects, true);
  assert.equal(manifest.features.contentAddressedObjects, true);
  assert.equal(manifest.features.deduplicatedObjects, true);
  assert.equal(manifest.features.denseDocPointers, true);
  assert.equal(manifest.features.docLocalityLayout, true);
  assert.equal(manifest.features.docPages, true);
  assert.equal(manifest.features.docValueSorted, true);
  assert.equal(manifest.features.queryBundles, true);
  assert.equal(manifest.features.authority, true);
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
  assert.ok(manifest.build.counters.selected_term_spool_bytes > 0);
  assert.ok(manifest.build.counters.selected_term_spool_terms > 0);
  assert.ok(manifest.build.counters.doc_raw_spool_bytes > 0);
  assert.ok(manifest.build.counters.doc_gzip_spool_bytes > 0);
  assert.ok(manifest.build.counters.segment_files > 0);
  assert.ok(manifest.build.counters.segment_postings > 0);
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
  assert.equal(manifest.stats.posting_segment_format, "rfsegpost-v1");
  assert.equal(manifest.stats.posting_segment_storage, "range-pack-v1");
  assert.equal(manifest.stats.posting_segment_block_storage, "range-pack-v1");
  assert.equal(manifest.stats.term_storage, undefined);
  assert.equal(manifest.stats.posting_block_storage, undefined);
  assert.equal(manifest.stats.segment_format, "rfsegment-v1");
  assert.equal(manifest.stats.scan_workers, 2);
  assert.equal(manifest.stats.scan_batch_docs, 2);
  assert.equal(manifest.stats.segment_merge_fan_in, 512);
  assert.equal(manifest.stats.segment_merge_tiers, 0);
  assert.ok(manifest.stats.segment_reduced_spool_bytes > 0);
  assert.ok(manifest.stats.segment_reduced_spool_ms >= 0);
  assert.ok(manifest.stats.segment_partition_assembly_ms >= 0);
  assert.ok(manifest.stats.segment_files > 0);
  assert.ok(manifest.stats.segment_terms > 0);
  assert.ok(manifest.stats.segment_postings > 0);
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
  assert.ok(manifest.doc_values.fields.tags);
  assert.ok(manifest.doc_values.fields.published);
  assert.ok(manifest.doc_values.fields.tags.chunks[0].checksum.value);
  assert.match(manifest.doc_values.fields.tags.chunks[0].pack, /^0000\.[0-9a-f]{24}\.bin$/u);
  assert.ok(await readFile(join(output, "doc-values", "packs", manifest.doc_values.fields.tags.chunks[0].pack)));
  assert.equal(manifest.doc_value_sorted.storage, "range-pack-v1");
  assert.equal(manifest.doc_value_sorted.directory_format, "rfdocvaluesortdir-v1");
  assert.equal(manifest.doc_value_sorted.page_format, "rfdocvaluesortpage-v1");
  assert.ok(manifest.doc_value_sorted.fields.published);
  assert.ok(manifest.doc_value_sorted.fields.featured);
  assert.match(manifest.doc_value_sorted.pack_table[0], /^0000\.[0-9a-f]{24}\.bin$/u);
  assert.ok(await readFile(join(output, manifest.doc_value_sorted.fields.published.directory.file)));
  assert.ok(await readFile(join(output, "doc-values", "sorted-packs", manifest.doc_value_sorted.pack_table[0])));
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
  const lazyTelemetry = await search.loadBuildTelemetry();
  assert.equal(lazyTelemetry.format, "rfbuildtelemetry-v1");
  assert.ok(server.requests.some(request => request.pathname.endsWith("/debug/build-telemetry.json")));
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
  assert.ok(results.stats.blocksDecoded <= exactResults.stats.blocksDecoded);

  const singleTerm = await search.search({ q: "search", size: 2, rerank: false });
  const singleTermExact = await search.search({ q: "search", size: 2, exact: true, rerank: false });
  assert.deepEqual(
    singleTerm.results.map(result => result.title),
    singleTermExact.results.map(result => result.title)
  );
  assert.ok(singleTerm.stats.blocksDecoded < singleTermExact.stats.blocksDecoded);

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
  assert.equal(filteredBundled.stats.blocksDecoded, 0);

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
  await assert.rejects(() => stat(join(output, "_build")), /ENOENT/u);
  const failure = JSON.parse(await readFile(join(output, "debug", "build-failure.json"), "utf8"));
  assert.equal(failure.status, "failed");
  assert.equal(failure.cleanup.removed, "_build");
  const failedTelemetry = JSON.parse(await readFile(join(output, "debug", "build-telemetry.failed.json"), "utf8"));
  assert.equal(failedTelemetry.status, "failed");
  assert.ok(failedTelemetry.error.message);
});

test("runtime refills high-df posting block windows in batches", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-frontier-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  const docs = Array.from({ length: 40 }, (_, index) => JSON.stringify({
    id: String(index),
    title: `Common document ${String(index).padStart(2, "0")}`,
    body: "common marker text",
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
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0,
    fields: [
      { name: "title", path: "title", weight: 2.0 },
      { name: "body", path: "body", weight: 1.0 }
    ],
    display: ["title", "url"]
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
  assert.equal(postingBlockRequests.length, 3);
});
