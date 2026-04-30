import assert from "node:assert/strict";
import test from "node:test";
import { fold, queryTerms, termCounts, tokenize } from "../src/analyzer.js";

test("fold normalizes accents, ligatures, and case", () => {
  assert.equal(fold("École CŒUR Æther"), "ecole coeur aether");
});

test("tokenize removes stopwords, stems words, and deduplicates by default", () => {
  assert.deepEqual(tokenize("The static static indexes are running quickly"), ["static", "index", "runn", "quickly"]);
});

test("termCounts keeps repeated tokens when scoring documents", () => {
  assert.deepEqual([...termCounts("static static search")], [["static", 2], ["search", 1]]);
});

test("queryTerms expands contiguous query phrases", () => {
  assert.deepEqual(queryTerms("static range search"), [
    "static",
    "range",
    "search",
    "static_range",
    "range_search",
    "static_range_search"
  ]);
});
