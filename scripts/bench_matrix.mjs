#!/usr/bin/env node

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { performance } from "node:perf_hooks";
import { dirname, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { createSearch } from "../src/runtime.js";
import {
  createFetchMeter,
  dirStats,
  kb,
  mean,
  quantile,
  serveStatic
} from "./bench_support.mjs";

const BUILTIN_FIXTURES = {
  basic: {
    label: "basic",
    family: "docs-small",
    root: "examples/basic/public",
    basePath: "rangefind/"
  },
  frwiki: {
    label: "frwiki",
    family: "encyclopedia",
    root: "examples/frwiki/public",
    basePath: "rangefind/"
  }
};

const DEFAULT_PROMOTION_GATES = {
  minimumFamilies: 2,
  p95RegressionTolerance: 1.1,
  p95RegressionMinDeltaMs: 10,
  kbRegressionTolerance: 1.05,
  requestRegressionTolerance: 1.1
};

const DEFAULT_DEFERRED_REVIEW_LIMITS = {
  highTextP95Ms: 75,
  highTextKb: 128,
  highDecodedBlocks: 64,
  highDecodedPostings: 8192
};

function parseMatrixArgs(argv) {
  const args = {
    fixtures: ["basic", "frwiki"],
    customFixtures: [],
    runs: 3,
    size: 10,
    json: false,
    failMissing: false,
    baselinePath: "",
    outPath: "",
    gates: { ...DEFAULT_PROMOTION_GATES }
  };
  for (const arg of argv) {
    if (arg === "--json") args.json = true;
    else if (arg === "--fail-missing") args.failMissing = true;
    else if (arg.startsWith("--baseline=")) args.baselinePath = arg.slice("--baseline=".length);
    else if (arg.startsWith("--out=")) args.outPath = arg.slice("--out=".length);
    else if (arg.startsWith("--fixtures=")) args.fixtures = arg.slice("--fixtures=".length).split(",").map(item => item.trim()).filter(Boolean);
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--size=")) args.size = Number(arg.slice("--size=".length)) || args.size;
    else if (arg.startsWith("--fixture=")) args.customFixtures.push(parseCustomFixture(arg.slice("--fixture=".length)));
    else if (arg.startsWith("--gate-min-families=")) args.gates.minimumFamilies = Number(arg.slice("--gate-min-families=".length)) || args.gates.minimumFamilies;
    else if (arg.startsWith("--gate-p95-tolerance=")) args.gates.p95RegressionTolerance = Number(arg.slice("--gate-p95-tolerance=".length)) || args.gates.p95RegressionTolerance;
    else if (arg.startsWith("--gate-p95-min-delta-ms=")) args.gates.p95RegressionMinDeltaMs = Number(arg.slice("--gate-p95-min-delta-ms=".length)) || args.gates.p95RegressionMinDeltaMs;
    else if (arg.startsWith("--gate-kb-tolerance=")) args.gates.kbRegressionTolerance = Number(arg.slice("--gate-kb-tolerance=".length)) || args.gates.kbRegressionTolerance;
    else if (arg.startsWith("--gate-request-tolerance=")) args.gates.requestRegressionTolerance = Number(arg.slice("--gate-request-tolerance=".length)) || args.gates.requestRegressionTolerance;
  }
  return args;
}

function parseCustomFixture(value) {
  const [label, root, basePath = "rangefind/", family = "custom"] = value.split(":");
  if (!label || !root) throw new Error("--fixture expects label:root[:basePath[:family]]");
  return { label, root, basePath, family };
}

function fixtureManifestPath(fixture) {
  return resolve(fixture.root, fixture.basePath, "manifest.min.json");
}

function fieldNames(manifest, group) {
  return group === "facets"
    ? Object.keys(manifest.facets || {})
    : (manifest[group] || []).map(field => field.name);
}

function firstField(manifest, group, preferred = []) {
  const names = fieldNames(manifest, group);
  return preferred.find(name => names.includes(name)) || names[0] || "";
}

function fixtureCases(fixture, manifest, size) {
  const numberSort = firstField(manifest, "numbers", ["published", "revisionDate", "year", "bodyLength", "articleId"]);
  const booleanField = firstField(manifest, "booleans", ["featured", "hasCategories"]);
  const facetField = firstField(manifest, "facets", ["category", "tags", "namespace"]);
  const textQueries = fixture.label === "frwiki"
    ? ["Paris", "changement climatique"]
    : ["range static search", "facet numeric filters"];
  const cases = [
    { label: "empty browse", category: "browse", q: "" },
    { label: "broad text", category: "text", q: textQueries[0], options: { rerank: false } },
    { label: "multi-term text", category: "text", q: textQueries[1] || textQueries[0], options: { rerank: false } }
  ];
  if (numberSort) {
    cases.push({ label: "sorted browse", category: "sort", q: "", sort: { field: numberSort, order: "desc" } });
    cases.push({ label: "sorted text", category: "text-sort", q: textQueries[0], sort: { field: numberSort, order: "desc" }, options: { rerank: false } });
  }
  if (booleanField) {
    cases.push({ label: "boolean filtered text", category: "filter", q: textQueries[0], filters: { booleans: { [booleanField]: true } }, options: { rerank: false } });
  }
  if (facetField && fixture.label === "basic") {
    cases.push({ label: "facet filtered text", category: "filter", q: "range static search", filters: { facets: { [facetField]: ["filters", "indexing", "range"] } }, options: { rerank: false } });
  }
  return cases.map(item => ({ size, ...item }));
}

function compactStats(stats = {}) {
  return {
    plannerLane: stats.plannerLane || "",
    topKProven: Boolean(stats.topKProven),
    totalExact: Boolean(stats.totalExact),
    blocksDecoded: stats.blocksDecoded || 0,
    postingsDecoded: stats.postingsDecoded || 0,
    postingsAccepted: stats.postingsAccepted || 0,
    skippedBlocks: stats.skippedBlocks || 0,
    postingSuperblocksSkipped: stats.postingSuperblocksSkipped || 0,
    sortedTextBlockScheduler: Boolean(stats.sortedTextBlockScheduler),
    sortedTextCandidateLookup: Boolean(stats.sortedTextCandidateLookup),
    sortPagePostingBlocksConsidered: stats.sortPagePostingBlocksConsidered || 0,
    sortPagePostingBlocksCandidate: stats.sortPagePostingBlocksCandidate || 0,
    sortPagePostingBlocksSkipped: stats.sortPagePostingBlocksSkipped || 0,
    sortPagePostingSuperblocksConsidered: stats.sortPagePostingSuperblocksConsidered || 0,
    sortPagePostingSuperblocksSkipped: stats.sortPagePostingSuperblocksSkipped || 0,
    sortPagePostingRowsScanned: stats.sortPagePostingRowsScanned || 0,
    sortPagePostingLookupHits: stats.sortPagePostingLookupHits || 0,
    filterSummaryProofBlocks: stats.filterSummaryProofBlocks || 0,
    docValuePagesVisited: stats.docValuePagesVisited || 0,
    docValueSortPageBatchSize: stats.docValueSortPageBatchSize || 0,
    docValueSortPagesPrefetched: stats.docValueSortPagesPrefetched || 0,
    docValueSortPagesFetched: stats.docValueSortPagesFetched || 0,
    docValueSortPageFetchGroups: stats.docValueSortPageFetchGroups || 0,
    docValueSortPageOverfetch: stats.docValueSortPageOverfetch || 0,
    docValueRowsScanned: stats.docValueRowsScanned || 0,
    docValueRowsAccepted: stats.docValueRowsAccepted || 0,
    docValueSortText: Boolean(stats.docValueSortText),
    sortSummaryStopReason: stats.sortSummaryStopReason || "",
    queryBundleHit: Boolean(stats.queryBundleHit),
    typoApplied: Boolean(stats.typoApplied),
    plannerFallbackReason: stats.plannerFallbackReason || ""
  };
}

function optimizerStats(manifest) {
  const stats = manifest.stats || {};
  return {
    postingFormat: stats.posting_segment_format || "",
    blockSize: stats.posting_segment_block_size || 0,
    superblockSize: stats.posting_segment_superblock_size || 0,
    blockSizeSource: stats.posting_segment_block_size_source || "",
    superblockSizeSource: stats.posting_segment_superblock_size_source || "",
    pairVarintBlocks: stats.posting_segment_block_codec_pair_varint_blocks || 0,
    impactRunBlocks: stats.posting_segment_block_codec_impact_run_blocks || 0,
    impactBitsetBlocks: stats.posting_segment_block_codec_impact_bitset_blocks || 0,
    partitionedDeltaBlocks: stats.posting_segment_block_codec_partitioned_delta_blocks || 0,
    codecBaselineBytes: stats.posting_segment_block_codec_baseline_bytes || 0,
    codecSelectedBytes: stats.posting_segment_block_codec_selected_bytes || 0,
    codecBytesSaved: stats.posting_segment_block_codec_bytes_saved || 0
  };
}

function finite(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : 0;
}

function unique(values) {
  return [...new Set(values.filter(Boolean))].sort();
}

function rowSignature(row) {
  return `${row.category || ""}|${row.label || ""}|${JSON.stringify(row.request || {})}`;
}

function rowMap(report) {
  const rows = new Map();
  for (const fixture of report?.fixtures || []) {
    for (const row of fixture.rows || []) {
      rows.set(`${fixture.label}|${rowSignature(row)}`, { fixture, row });
    }
  }
  return rows;
}

function round(value, digits = 3) {
  const factor = 10 ** digits;
  return Math.round(finite(value) * factor) / factor;
}

function ratio(current, baseline) {
  const base = finite(baseline);
  if (base <= 0) return finite(current) <= 0 ? 1 : Infinity;
  return finite(current) / base;
}

function checkStatus(checks) {
  if (checks.some(check => check.status === "fail")) return "blocked";
  if (checks.some(check => check.status === "warn")) return "needs-baseline";
  return "promote";
}

function addCheck(checks, name, status, details, metrics = {}) {
  checks.push({ name, status, details, metrics });
}

function familySummaries(report) {
  return unique((report.fixtures || []).map(fixture => fixture.family)).map(family => {
    const fixtures = (report.fixtures || []).filter(fixture => fixture.family === family);
    const rows = fixtures.flatMap(fixture => fixture.rows || []);
    const categories = unique(rows.map(row => row.category));
    const lanes = unique(rows.map(row => row.coldStats?.plannerLane || ""));
    const fallbackRows = rows.filter(row => row.coldStats?.plannerFallbackReason).length;
    const codecBytesSaved = fixtures.reduce((sum, fixture) => sum + finite(fixture.optimizer?.codecBytesSaved), 0);
    return {
      family,
      fixtures: fixtures.map(fixture => fixture.label),
      cases: rows.length,
      categories,
      lanes,
      p95Ms: round(mean(rows.map(row => row.p95Ms || 0))),
      avgKb: round(mean(rows.map(row => row.avgKb || 0))),
      avgRequests: round(mean(rows.map(row => row.avgRequests || 0))),
      codecBytesSaved,
      fallbackRows
    };
  });
}

function compareToBaseline(report, baselineReport, gates) {
  const baselineRows = rowMap(baselineReport);
  const matched = [];
  const missing = [];
  const regressions = [];
  const improvements = [];

  for (const fixture of report.fixtures || []) {
    for (const row of fixture.rows || []) {
      const key = `${fixture.label}|${rowSignature(row)}`;
      const baseline = baselineRows.get(key);
      if (!baseline) {
        missing.push({ fixture: fixture.label, case: row.label, category: row.category });
        continue;
      }
      const p95Ratio = ratio(row.p95Ms, baseline.row.p95Ms);
      const kbRatio = ratio(row.avgKb, baseline.row.avgKb);
      const requestRatio = ratio(row.avgRequests, baseline.row.avgRequests);
      const p95DeltaMs = finite(row.p95Ms) - finite(baseline.row.p95Ms);
      const comparison = {
        fixture: fixture.label,
        family: fixture.family,
        case: row.label,
        category: row.category,
        p95Ratio: round(p95Ratio),
        p95DeltaMs: round(p95DeltaMs),
        kbRatio: round(kbRatio),
        requestRatio: round(requestRatio)
      };
      matched.push(comparison);
      if (p95Ratio > gates.p95RegressionTolerance && p95DeltaMs > gates.p95RegressionMinDeltaMs) regressions.push({ ...comparison, metric: "p95Ms" });
      if (kbRatio > gates.kbRegressionTolerance) regressions.push({ ...comparison, metric: "avgKb" });
      if (requestRatio > gates.requestRegressionTolerance) regressions.push({ ...comparison, metric: "avgRequests" });
      if (p95Ratio < 0.98 || kbRatio < 0.98 || requestRatio < 0.98) improvements.push(comparison);
    }
  }

  return {
    baselineFormat: baselineReport?.format || "",
    matchedRows: matched.length,
    missingRows: missing,
    regressions,
    improvements,
    winningFamilies: unique(improvements.map(item => item.family))
  };
}

function markdownCell(value) {
  return String(value ?? "").replace(/\|/gu, "\\|").replace(/\n/gu, " ");
}

export function createPromotionGates(report, baselineReport = null, options = {}) {
  const gates = { ...DEFAULT_PROMOTION_GATES, ...(options || {}) };
  const checks = [];
  const families = unique((report.fixtures || []).map(fixture => fixture.family));
  const summaries = familySummaries(report);
  const rows = (report.fixtures || []).flatMap(fixture => fixture.rows || []);
  const allCategories = unique(rows.map(row => row.category));
  const failed = report.failed || [];
  const skipped = report.skipped || [];

  addCheck(
    checks,
    "fixture-health",
    failed.length || skipped.length ? "fail" : "pass",
    failed.length || skipped.length
      ? `failed=${failed.length}, skipped=${skipped.length}`
      : "all requested fixtures completed"
  );
  addCheck(
    checks,
    "family-coverage",
    families.length >= gates.minimumFamilies ? "pass" : "warn",
    `families=${families.length}/${gates.minimumFamilies}: ${families.join(", ") || "none"}`,
    { families: families.length, required: gates.minimumFamilies }
  );
  addCheck(
    checks,
    "query-coverage",
    allCategories.includes("browse") && allCategories.includes("text") && allCategories.some(category => ["filter", "sort", "text-sort"].includes(category)) ? "pass" : "warn",
    `categories=${allCategories.join(", ") || "none"}`
  );

  const fallbackReasonRows = rows.filter(row => row.coldStats?.plannerFallbackReason);
  const fallbackRows = fallbackReasonRows.filter(row => {
    const stats = row.coldStats || {};
    return !(stats.totalExact || stats.topKProven || stats.docValueSortText);
  });
  const unsafeRows = rows.filter(row => {
    const stats = row.coldStats || {};
    if (!row.request?.q) return false;
    if (stats.totalExact || stats.topKProven || stats.docValueSortText) return false;
    if (stats.plannerFallbackReason) return true;
    return ["text", "filter", "text-sort"].includes(row.category);
  });
  addCheck(
    checks,
    "planner-exactness",
    fallbackRows.length || unsafeRows.length ? "fail" : "pass",
    fallbackRows.length || unsafeRows.length
      ? `fallback_rows=${fallbackRows.length}, unproven_rows=${unsafeRows.length}`
      : `no text query rows required an unproven broad fallback; fallback_reason_rows=${fallbackReasonRows.length}`
  );

  const badCodecFixtures = (report.fixtures || []).filter(fixture => {
    const optimizer = fixture.optimizer || {};
    return optimizer.postingFormat !== "rfsegpost-v6" ||
      finite(optimizer.blockSize) <= 0 ||
      finite(optimizer.superblockSize) <= 0 ||
      finite(optimizer.codecSelectedBytes) > finite(optimizer.codecBaselineBytes);
  });
  addCheck(
    checks,
    "codec-layout-invariants",
    badCodecFixtures.length ? "fail" : "pass",
    badCodecFixtures.length
      ? badCodecFixtures.map(fixture => fixture.label).join(", ")
      : "rfsegpost-v6 layout is valid and measured codecs never exceed baseline bytes"
  );

  let comparison = null;
  if (baselineReport) {
    comparison = compareToBaseline(report, baselineReport, gates);
    const status = !comparison.matchedRows
      ? "fail"
      : comparison.regressions.length
        ? "fail"
        : comparison.winningFamilies.length >= gates.minimumFamilies
          ? "pass"
          : "warn";
    addCheck(
      checks,
      "baseline-comparison",
      status,
      comparison.regressions.length
        ? `regressions=${comparison.regressions.length}`
        : `matched=${comparison.matchedRows}, winning_families=${comparison.winningFamilies.length}/${gates.minimumFamilies}`,
      {
        matchedRows: comparison.matchedRows,
        regressions: comparison.regressions.length,
        improvements: comparison.improvements.length,
        winningFamilies: comparison.winningFamilies.length
      }
    );
  } else {
    addCheck(
      checks,
      "baseline-comparison",
      "warn",
      "provide --baseline=<rfbenchmatrix-v1.json> before promoting default auto decisions"
    );
  }

  return {
    format: "rfbenchpromotion-v1",
    status: checkStatus(checks),
    policy: "core-wins-must-survive-across-families",
    gates,
    families,
    summaries,
    checks,
    comparison
  };
}

function evidenceRow(fixture, row) {
  return {
    fixture: fixture.label,
    family: fixture.family,
    case: row.label,
    category: row.category,
    lane: row.coldStats?.plannerLane || "",
    p95Ms: round(row.p95Ms || 0),
    avgKb: round(row.avgKb || 0),
    avgRequests: round(row.avgRequests || 0),
    blocksDecoded: row.coldStats?.blocksDecoded || 0,
    postingsDecoded: row.coldStats?.postingsDecoded || 0,
    sortPagePostingRowsScanned: row.coldStats?.sortPagePostingRowsScanned || 0,
    sortPagePostingLookupHits: row.coldStats?.sortPagePostingLookupHits || 0,
    sortedTextCandidateLookup: Boolean(row.coldStats?.sortedTextCandidateLookup),
    fallbackReason: row.coldStats?.plannerFallbackReason || ""
  };
}

function rowsWithFixtures(report, predicate) {
  const out = [];
  for (const fixture of report.fixtures || []) {
    for (const row of fixture.rows || []) if (predicate(row, fixture)) out.push(evidenceRow(fixture, row));
  }
  return out;
}

function hasMaterialTextCost(row, limits) {
  const stats = row.coldStats || {};
  return finite(row.p95Ms) >= limits.highTextP95Ms ||
    finite(row.avgKb) >= limits.highTextKb ||
    finite(stats.blocksDecoded) >= limits.highDecodedBlocks ||
    finite(stats.postingsDecoded) >= limits.highDecodedPostings;
}

function hasMaterialSortedTextCost(row, limits) {
  const stats = row.coldStats || {};
  return finite(row.p95Ms) >= limits.highTextP95Ms ||
    finite(row.avgKb) >= limits.highTextKb ||
    finite(stats.postingsDecoded) >= limits.highDecodedPostings ||
    (!stats.sortedTextCandidateLookup && finite(stats.blocksDecoded) >= limits.highDecodedBlocks);
}

export function createDeferredReview(report, promotion, options = {}) {
  const limits = { ...DEFAULT_DEFERRED_REVIEW_LIMITS, ...(options || {}) };
  const promotedCore = promotion?.status === "promote";
  const waitReason = "wait_for_promoted_core_benchmark";
  const textRows = rowsWithFixtures(report, row => row.request?.q && ["text", "filter"].includes(row.category));
  const highCostTextRows = rowsWithFixtures(report, row => row.request?.q && ["text", "filter"].includes(row.category) && hasMaterialTextCost(row, limits));
  const phraseFallbackRows = rowsWithFixtures(report, row => row.request?.q && row.category === "text" && row.coldStats?.plannerLane === "fullFallback" && hasMaterialTextCost(row, limits));
  const sortedDecodeRows = rowsWithFixtures(report, row => row.request?.q && row.category === "text-sort" && hasMaterialSortedTextCost(row, limits));

  const decisions = [
    {
      kind: "champion-window",
      status: !promotedCore ? "deferred" : highCostTextRows.length ? "candidate" : "not-recommended",
      reason: !promotedCore
        ? waitReason
        : highCostTextRows.length
          ? "some text rows still exceed generic transfer, latency, or decode thresholds"
          : "promoted core path has no high-cost generic text rows in this matrix",
      evidence: highCostTextRows.slice(0, 8)
    },
    {
      kind: "phrase-materialization",
      status: !promotedCore ? "deferred" : phraseFallbackRows.length ? "candidate" : "not-recommended",
      reason: !promotedCore
        ? waitReason
        : phraseFallbackRows.length
          ? "multi-term fallback rows remain material after core promotion"
          : "query bundles and core posting proof cover material multi-term rows in this matrix",
      evidence: phraseFallbackRows.slice(0, 8)
    },
    {
      kind: "term-sort-materialization",
      status: !promotedCore ? "deferred" : sortedDecodeRows.length ? "watch-core-first" : "not-recommended",
      reason: !promotedCore
        ? waitReason
        : sortedDecodeRows.length
          ? "q+sort still decodes many postings; prefer improving core sorted-posting scheduling before adding an overlay"
          : "sort summaries cover text-sort rows without material decode pressure",
      evidence: sortedDecodeRows.slice(0, 8)
    },
    {
      kind: "learned-sparse-import",
      status: "deferred",
      reason: "requires explicit sparse vector input and a quality benchmark; do not infer it from lexical latency rows",
      evidence: []
    }
  ];

  return {
    format: "rfbenchdeferred-v1",
    policy: "only-add-derived-structures-for-promoted-core-gaps",
    promotedCore,
    limits,
    textRows: textRows.length,
    decisions
  };
}

async function benchFixture(fixture, args) {
  const server = await serveStatic(fixture.root);
  const meter = createFetchMeter(/\/rangefind\//u);
  try {
    meter.reset();
    const initStart = performance.now();
    const engine = await createSearch({ baseUrl: new URL(fixture.basePath, server.url) });
    const initMs = performance.now() - initStart;
    const initNetwork = meter.snapshot();
    const cases = fixtureCases(fixture, engine.manifest, args.size);
    const rows = [];
    for (const item of cases) {
      const times = [];
      const requests = [];
      const bytes = [];
      let total = 0;
      let top = "";
      let coldStats = {};
      for (let run = 0; run < args.runs; run++) {
        meter.reset();
        const start = performance.now();
        const response = await engine.search({
          q: item.q,
          filters: item.filters,
          sort: item.sort,
          size: item.size,
          ...(item.options || {})
        });
        times.push(performance.now() - start);
        const network = meter.snapshot();
        requests.push(network.requests);
        bytes.push(network.bytes);
        total = response.total;
        top = response.results[0]?.title || response.results[0]?.id || "";
        if (run === 0) coldStats = compactStats(response.stats);
      }
      rows.push({
        label: item.label,
        category: item.category,
        request: { q: item.q, filters: item.filters || null, sort: item.sort || null, size: item.size },
        total,
        top,
        coldMs: times[0] || 0,
        coldRequests: requests[0] || 0,
        coldKb: kb(bytes[0] || 0),
        p50Ms: quantile(times, 0.5),
        p95Ms: quantile(times, 0.95),
        avgRequests: mean(requests),
        avgKb: kb(mean(bytes)),
        coldStats
      });
    }
    return {
      label: fixture.label,
      family: fixture.family,
      root: resolve(fixture.root),
      index: dirStats(resolve(fixture.root, fixture.basePath)),
      init: { ms: initMs, requests: initNetwork.requests, kb: kb(initNetwork.bytes) },
      optimizer: optimizerStats(engine.manifest),
      rows
    };
  } finally {
    meter.restore();
    await server.close();
  }
}

function markdown(report) {
  const lines = ["# Rangefind Benchmark Matrix", ""];
  for (const fixture of report.fixtures) {
    lines.push(`## ${fixture.label} (${fixture.family})`, "");
    lines.push(`Index: ${fixture.index.files} files, ${(fixture.index.bytes / 1024 / 1024).toFixed(2)} MB`);
    lines.push(`Init: ${fixture.init.ms.toFixed(1)} ms, ${fixture.init.requests} requests, ${fixture.init.kb.toFixed(1)} KB`);
    lines.push(`Codecs: pair=${fixture.optimizer.pairVarintBlocks}, runs=${fixture.optimizer.impactRunBlocks}, bitset=${fixture.optimizer.impactBitsetBlocks}, deltas=${fixture.optimizer.partitionedDeltaBlocks}, saved=${fixture.optimizer.codecBytesSaved} bytes`);
    lines.push("");
    lines.push("| Case | Lane | Total | P50 ms | P95 ms | Avg req | Avg KB | Blocks | Sort pages | Top |");
    lines.push("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |");
    for (const row of fixture.rows) {
      lines.push(`| ${row.label} | ${row.coldStats.plannerLane || ""} | ${row.total} | ${row.p50Ms.toFixed(1)} | ${row.p95Ms.toFixed(1)} | ${row.avgRequests.toFixed(1)} | ${row.avgKb.toFixed(1)} | ${row.coldStats.blocksDecoded} | ${row.coldStats.docValuePagesVisited} | ${row.top} |`);
    }
    lines.push("");
  }
  if (report.promotion) {
    lines.push("## Promotion Gates", "");
    lines.push(`Status: \`${report.promotion.status}\``);
    lines.push("");
    lines.push("| Gate | Status | Details |");
    lines.push("| --- | --- | --- |");
    for (const check of report.promotion.checks) {
      lines.push(`| ${markdownCell(check.name)} | ${markdownCell(check.status)} | ${markdownCell(check.details)} |`);
    }
    lines.push("");
    lines.push("| Family | Fixtures | Cases | Categories | Lanes | P95 ms | Avg KB | Avg req | Codec saved | Fallback rows |");
    lines.push("| --- | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |");
    for (const family of report.promotion.summaries) {
      lines.push(`| ${markdownCell(family.family)} | ${markdownCell(family.fixtures.join(", "))} | ${family.cases} | ${markdownCell(family.categories.join(", "))} | ${markdownCell(family.lanes.join(", "))} | ${family.p95Ms.toFixed(1)} | ${family.avgKb.toFixed(1)} | ${family.avgRequests.toFixed(1)} | ${family.codecBytesSaved} | ${family.fallbackRows} |`);
    }
    if (report.promotion.comparison) {
      lines.push("");
      lines.push(`Baseline rows matched: ${report.promotion.comparison.matchedRows}`);
      lines.push(`Baseline regressions: ${report.promotion.comparison.regressions.length}`);
      lines.push(`Winning families: ${report.promotion.comparison.winningFamilies.join(", ") || "none"}`);
    }
    lines.push("");
  }
  if (report.deferredReview) {
    lines.push("## Deferred Structure Review", "");
    lines.push(`Promoted core: ${report.deferredReview.promotedCore ? "yes" : "no"}`);
    lines.push("");
    lines.push("| Structure | Status | Reason | Evidence rows |");
    lines.push("| --- | --- | --- | ---: |");
    for (const decision of report.deferredReview.decisions) {
      lines.push(`| ${markdownCell(decision.kind)} | ${markdownCell(decision.status)} | ${markdownCell(decision.reason)} | ${decision.evidence.length} |`);
    }
    lines.push("");
  }
  if (report.skipped.length) lines.push(`Skipped missing fixtures: ${report.skipped.join(", ")}`);
  if (report.failed.length) {
    lines.push("");
    lines.push("Failed fixtures:");
    for (const item of report.failed) lines.push(`- ${item.fixture}: ${item.error}`);
  }
  return lines.join("\n");
}

function loadBaselineReport(path) {
  if (!path) return null;
  return JSON.parse(readFileSync(resolve(path), "utf8"));
}

function writeReport(path, report) {
  if (!path) return;
  const out = resolve(path);
  mkdirSync(dirname(out), { recursive: true });
  writeFileSync(out, `${JSON.stringify(report, null, 2)}\n`);
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseMatrixArgs(argv);
  const requested = [
    ...args.fixtures.map(name => BUILTIN_FIXTURES[name]).filter(Boolean),
    ...args.customFixtures
  ];
  const report = {
    format: "rfbenchmatrix-v1",
    generatedAt: new Date().toISOString(),
    runs: args.runs,
    size: args.size,
    fixtures: [],
    skipped: [],
    failed: []
  };

  for (const fixture of requested) {
    if (!existsSync(fixtureManifestPath(fixture))) {
      if (args.failMissing) throw new Error(`Missing fixture index: ${fixture.label} at ${fixtureManifestPath(fixture)}`);
      report.skipped.push(fixture.label);
      continue;
    }
    try {
      report.fixtures.push(await benchFixture(fixture, args));
    } catch (error) {
      if (args.failMissing) throw error;
      report.failed.push({ fixture: fixture.label, error: error.message });
    }
  }

  report.promotion = createPromotionGates(report, loadBaselineReport(args.baselinePath), args.gates);
  report.deferredReview = createDeferredReview(report, report.promotion);
  writeReport(args.outPath, report);

  if (args.json) console.log(JSON.stringify(report, null, 2));
  else console.log(markdown(report));
  return report;
}

if (import.meta.url === pathToFileURL(process.argv[1] || "").href) {
  await main();
}
