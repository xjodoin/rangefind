import assert from "node:assert/strict";
import test from "node:test";
import { createDeferredReview, createPromotionGates } from "../scripts/bench_matrix.mjs";

function fixture(label, family, p95Base) {
  return {
    label,
    family,
    optimizer: {
      postingFormat: "rfsegpost-v5",
      blockSize: 64,
      superblockSize: 16,
      codecBaselineBytes: 1000,
      codecSelectedBytes: 800,
      codecBytesSaved: 200
    },
    rows: [
      {
        label: "empty browse",
        category: "browse",
        request: { q: "", filters: null, sort: null, size: 10 },
        p95Ms: p95Base,
        avgKb: 5,
        avgRequests: 2,
        coldStats: { plannerLane: "browse", totalExact: true }
      },
      {
        label: "broad text",
        category: "text",
        request: { q: "alpha", filters: null, sort: null, size: 10 },
        p95Ms: p95Base + 2,
        avgKb: 8,
        avgRequests: 3,
        coldStats: { plannerLane: "blockMax", topKProven: true }
      },
      {
        label: "sorted text",
        category: "text-sort",
        request: { q: "alpha", filters: null, sort: { field: "year", order: "desc" }, size: 10 },
        p95Ms: p95Base + 4,
        avgKb: 9,
        avgRequests: 4,
        coldStats: { plannerLane: "sortPageText", docValueSortText: true }
      }
    ]
  };
}

function report(p95A = 10, p95B = 20) {
  return {
    format: "rfbenchmatrix-v1",
    fixtures: [fixture("basic", "docs-small", p95A), fixture("custom", "catalog", p95B)],
    failed: [],
    skipped: []
  };
}

test("promotion gates require a baseline before default auto promotion", () => {
  const gates = createPromotionGates(report());
  assert.equal(gates.status, "needs-baseline");
  assert.equal(gates.checks.find(check => check.name === "family-coverage").status, "pass");
  assert.equal(gates.checks.find(check => check.name === "baseline-comparison").status, "warn");
});

test("promotion gates promote cross-family wins without regressions", () => {
  const current = report(8, 18);
  const baseline = report(10, 20);
  const gates = createPromotionGates(current, baseline);
  assert.equal(gates.status, "promote");
  assert.equal(gates.comparison.matchedRows, 6);
  assert.deepEqual(gates.comparison.winningFamilies, ["catalog", "docs-small"]);
  assert.equal(gates.checks.find(check => check.name === "baseline-comparison").status, "pass");
});

test("promotion gates block runtime regressions", () => {
  const current = report(12, 35);
  const baseline = report(10, 20);
  const gates = createPromotionGates(current, baseline, { p95RegressionMinDeltaMs: 0 });
  assert.equal(gates.status, "blocked");
  assert.ok(gates.comparison.regressions.some(item => item.metric === "p95Ms"));
});

test("promotion gates ignore tiny timing noise when bytes and requests hold", () => {
  const current = report(11, 21);
  for (const fixture of current.fixtures) {
    for (const row of fixture.rows) row.avgKb *= 0.9;
  }
  const baseline = report(10, 20);
  const gates = createPromotionGates(current, baseline);
  assert.equal(gates.status, "promote");
  assert.equal(gates.comparison.regressions.length, 0);
});

test("deferred review watches sort overlays only after core promotion", () => {
  const current = report(8, 18);
  current.fixtures[1].rows[2].coldStats.blocksDecoded = 100;
  current.fixtures[1].rows[2].coldStats.postingsDecoded = 12000;
  const promoted = createPromotionGates(current, report(10, 20));
  const review = createDeferredReview(current, promoted);
  assert.equal(review.promotedCore, true);
  assert.equal(review.decisions.find(item => item.kind === "term-sort-materialization").status, "watch-core-first");
  assert.equal(review.decisions.find(item => item.kind === "champion-window").status, "not-recommended");
  assert.equal(review.decisions.find(item => item.kind === "learned-sparse-import").status, "deferred");
});

test("deferred review accepts sorted candidate lookup when materialized postings are low", () => {
  const current = report(8, 18);
  current.fixtures[1].rows[2].coldStats.blocksDecoded = 100;
  current.fixtures[1].rows[2].coldStats.postingsDecoded = 128;
  current.fixtures[1].rows[2].coldStats.sortedTextCandidateLookup = true;
  current.fixtures[1].rows[2].coldStats.sortPagePostingRowsScanned = 12000;
  const promoted = createPromotionGates(current, report(10, 20));
  const review = createDeferredReview(current, promoted);
  assert.equal(review.promotedCore, true);
  assert.equal(review.decisions.find(item => item.kind === "term-sort-materialization").status, "not-recommended");
});
