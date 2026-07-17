import assert from "node:assert/strict";
import test from "node:test";
import { analyzeDocumentForIndex, analyzeDocumentTerms, isAlwaysIndexField } from "../src/scoring.js";

test("always-index field metadata is cached per stable config", () => {
  const config = { alwaysIndexFields: ["title"] };
  const title = { name: "title", path: "name" };
  const body = { name: "body", path: "body" };

  assert.equal(isAlwaysIndexField(title, config), true);
  assert.equal(isAlwaysIndexField(body, config), false);
  // Replacing the serializable config field invalidates the cached metadata.
  config.alwaysIndexFields = ["body"];
  assert.equal(isAlwaysIndexField(title, config), false);
  assert.equal(isAlwaysIndexField(body, config), true);
});

test("document index analysis reuses capped field terms for query bundle seeds", () => {
  const config = {
    targetPostingsPerDoc: 8,
    maxExpansionTermsPerDoc: 0,
    queryBundleSeedMaxFieldTokens: 3,
    bm25fK1: 1.2,
    alwaysIndexFields: ["title"],
    bodyIndexChars: 1000,
    fields: [
      { name: "title", path: "title", weight: 4, b: 0.5, phrase: true },
      { name: "body", path: "body", weight: 1, b: 0.75, queryBundles: false }
    ]
  };
  const avgLens = { title: 3, body: 6 };
  const doc = {
    title: "Static range search runtime",
    body: "Rangefind builds static search with repeated static tokens"
  };

  const analysis = analyzeDocumentForIndex(doc, config, avgLens, { includeFieldTerms: true });

  assert.deepEqual(analysis.selectedTerms, analyzeDocumentTerms(doc, config, avgLens));
  assert.deepEqual(analysis.fieldTerms, [["static", "range", "search"], null]);
});
