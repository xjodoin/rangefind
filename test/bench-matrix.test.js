import assert from "node:assert/strict";
import test from "node:test";
import { createPromotionGates } from "../scripts/bench_matrix.mjs";

function fixture(label, family, p95Base) {
  return {
    label,
    family,
    optimizer: {
      postingFormat: "rfsegpost-v3",
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
  const gates = createPromotionGates(current, baseline);
  assert.equal(gates.status, "blocked");
  assert.ok(gates.comparison.regressions.some(item => item.metric === "p95Ms"));
});
