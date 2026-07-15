import assert from "node:assert/strict";
import test from "node:test";
import { pageRank, normalizeRanks, computeLinkRank, DEFAULT_ITERATIONS } from "../src/link_graph.js";

function sum(array) {
  let total = 0;
  for (const value of array) total += value;
  return total;
}

test("pageRank ranks a hub above the pages that link to it", () => {
  // 1, 2, 3 all point at 0; 0 is a dangling sink.
  const ranks = pageRank([[], [0], [0], [0]]);
  assert.ok(ranks[0] > ranks[1], "hub outranks its referrers");
  assert.ok(Math.abs(ranks[1] - ranks[2]) < 1e-9, "symmetric referrers tie");
  assert.ok(Math.abs(ranks[2] - ranks[3]) < 1e-9, "symmetric referrers tie");
});

test("pageRank distribution sums to 1 including dangling mass", () => {
  const ranks = pageRank([[1], [2], []]); // 2 is dangling
  assert.ok(Math.abs(sum(ranks) - 1) < 1e-9, `sum was ${sum(ranks)}`);
});

test("pageRank is deterministic across runs", () => {
  const graph = [[1, 2], [2], [0], [0, 1]];
  const a = pageRank(graph);
  const b = pageRank(graph);
  assert.deepEqual(Array.from(a), Array.from(b));
});

test("pageRank handles trivial graphs", () => {
  assert.deepEqual(Array.from(pageRank([])), []);
  assert.deepEqual(Array.from(pageRank([[]])), [1]);
});

test("normalizeRanks scales the top node to 1 and preserves ratios", () => {
  const normalized = normalizeRanks(Float64Array.of(0.1, 0.2, 0.4));
  assert.equal(normalized[2], 1);
  assert.ok(Math.abs(normalized[1] - 0.5) < 1e-12);
  assert.ok(Math.abs(normalized[0] - 0.25) < 1e-12);
});

test("normalizeRanks tolerates an all-zero vector", () => {
  const normalized = normalizeRanks(Float64Array.of(0, 0));
  assert.deepEqual(Array.from(normalized), [1, 1]);
});

test("computeLinkRank yields normalized authority for a clear hub", () => {
  const linkRank = computeLinkRank([[], [0], [0], [0]], { iterations: DEFAULT_ITERATIONS });
  assert.equal(linkRank[0], 1, "hub normalizes to 1");
  for (let i = 1; i < linkRank.length; i++) {
    assert.ok(linkRank[i] >= 0 && linkRank[i] < 1, "referrers are below the hub");
  }
});

test("more in-links means higher authority", () => {
  // Node 0 gets two referrers (2, 3); node 1 gets one (4).
  const linkRank = computeLinkRank([[], [], [0], [0], [1]]);
  assert.ok(linkRank[0] > linkRank[1], "the more-linked page wins");
});
