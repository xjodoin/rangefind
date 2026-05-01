import assert from "node:assert/strict";
import test from "node:test";
import { docLayoutRecord, DOC_LAYOUT_FORMAT, orderDocIdsByLocality, summarizeDocLayout } from "../src/doc_layout.js";

test("doc layout prefers base terms over phrase and proximity terms", () => {
  const record = docLayoutRecord(7, [
    ["static_range", 9],
    ["n_range_static", 7],
    ["static", 4],
    ["range", 3]
  ], { baseShardDepth: 2, docLocalityTerms: 2 });

  assert.deepEqual(record, {
    index: 7,
    shard: "st",
    primary: "static",
    secondary: "range",
    score: 4000
  });
});

test("doc layout clusters primary terms by impact and leaves empty docs last", () => {
  const order = orderDocIdsByLocality([
    { index: 0, shard: "ra", primary: "range", secondary: "", score: 1000 },
    { index: 1, shard: "st", primary: "static", secondary: "", score: 1000 },
    { index: 2, shard: "ra", primary: "range", secondary: "", score: 3000 },
    { index: 3, shard: "", primary: "", secondary: "", score: 0 }
  ], 4);

  assert.deepEqual(order, [2, 0, 1, 3]);
});

test("doc layout summary is compact manifest metadata", () => {
  const summary = summarizeDocLayout([
    { index: 0, primary: "range" },
    { index: 1, primary: "range" },
    { index: 2, primary: "" }
  ], 3, { baseShardDepth: 3, docLocalityTerms: 2 });

  assert.deepEqual(summary, {
    format: DOC_LAYOUT_FORMAT,
    strategy: "primary-base-term-impact",
    terms: 2,
    shard_depth: 3,
    docs: 3,
    docs_without_terms: 1,
    primary_terms: 1
  });
});
