import assert from "node:assert/strict";
import test from "node:test";
import {
  expandedTermsFromBaseTerms,
  proximityTerm,
  queryBundleKeyFromBaseTerms,
  queryBundleKeysFromBaseTerms
} from "../src/terms.js";

test("expandedTermsFromBaseTerms appends 2- and 3-gram phrases without re-analyzing", () => {
  assert.deepEqual(expandedTermsFromBaseTerms(["electrif", "wind", "insul"]), [
    "electrif",
    "wind",
    "insul",
    "electrif_wind",
    "wind_insul",
    "electrif_wind_insul"
  ]);
  // A single base term has no phrases.
  assert.deepEqual(expandedTermsFromBaseTerms(["solo"]), ["solo"]);
  // Duplicates collapse.
  assert.deepEqual(expandedTermsFromBaseTerms(["a", "a"]), ["a", "a_a"]);
});

test("proximityTerm names an order-independent pair for distinct terms", () => {
  assert.equal(proximityTerm("range", "search"), "n_range_search");
  assert.equal(proximityTerm("search", "range"), "n_range_search");
  assert.equal(proximityTerm("same", "same"), "");
  assert.equal(proximityTerm("", "x"), "");
});

test("queryBundleKeyFromBaseTerms only keys 2- and 3-term sets", () => {
  assert.equal(queryBundleKeyFromBaseTerms(["a", "b"]), "exact-expanded-v1|a b");
  assert.equal(queryBundleKeyFromBaseTerms(["a", "b", "c"]), "exact-expanded-v1|a b c");
  assert.equal(queryBundleKeyFromBaseTerms(["a"]), "");
  assert.equal(queryBundleKeyFromBaseTerms(["a", "b", "c", "d"]), "");
  // Duplicates are collapsed before counting.
  assert.equal(queryBundleKeyFromBaseTerms(["a", "a"]), "");
});

test("queryBundleKeysFromBaseTerms enumerates windows longest-first with expansions", () => {
  const plans = queryBundleKeysFromBaseTerms(["a", "b", "c"]);
  assert.deepEqual(plans.map(plan => plan.key), [
    "exact-expanded-v1|a b c",
    "exact-expanded-v1|a b",
    "exact-expanded-v1|b c"
  ]);
  assert.deepEqual(plans[0].baseTerms, ["a", "b", "c"]);
  assert.deepEqual(plans[0].expandedTerms, expandedTermsFromBaseTerms(["a", "b", "c"]));
});
