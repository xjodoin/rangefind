import assert from "node:assert/strict";
import test from "node:test";
import {
  bestMainIndexTypoDistance,
  boundedDamerauLevenshtein,
  mainIndexTypoCandidateScore,
  mainIndexTypoProbeValues,
  ngramOverlap,
  normalizeMainIndexTypoOptions,
  typoMaxEditsFor
} from "../src/typo_main_index.js";

test("main-index typo options normalize the static-large defaults", () => {
  const options = normalizeMainIndexTypoOptions({}, {
    search: {
      typo: {
        mode: "main-index",
        trigger: "zero-or-weak",
        maxEdits: 2,
        maxTokenCandidates: 8,
        maxQueryPlans: 5,
        maxCorrectedSearches: 3,
        maxShardLookups: 12
      }
    }
  });
  assert.equal(options.mode, "main-index");
  assert.equal(options.trigger, "zero-or-weak");
  assert.equal(options.maxEdits, 2);
  assert.equal(options.maxTokenCandidates, 8);
  assert.equal(options.maxQueryPlans, 5);
  assert.equal(options.maxCorrectedSearches, 3);
  assert.equal(options.maxShardLookups, 12);
});

test("main-index typo probes cover same-prefix and early substitution shards", () => {
  const probes = mainIndexTypoProbeValues("pxris", "pxri", { maxShardLookups: 12 });
  assert.ok(probes.includes("pxri"));
  assert.ok(probes.includes("paris") || probes.includes("pari"));
  assert.equal(probes.length <= 12, true);
});

test("main-index typo distance handles transposition and stem suffix surfaces", () => {
  assert.equal(boundedDamerauLevenshtein("elecrtif", "electrif", 2), 1);
  assert.equal(bestMainIndexTypoDistance("climatiqe", "climat", 2).surface, "climatique");
  assert.equal(bestMainIndexTypoDistance("climatiqe", "climat", 2).distance, 1);
  assert.equal(typoMaxEditsFor("short", { maxEdits: 2 }), 1);
  assert.equal(typoMaxEditsFor("longtoken", { maxEdits: 2 }), 2);
});

test("main-index typo candidate scoring favors close high-frequency terms", () => {
  const close = mainIndexTypoCandidateScore("statik", "static", 20, 1);
  const far = mainIndexTypoCandidateScore("statik", "sqlite", 20, 2);
  assert.ok(close > far);
  assert.ok(ngramOverlap("changement", "chang") > ngramOverlap("changement", "paris"));
});
