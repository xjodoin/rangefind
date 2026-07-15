// Build-time link-graph authority (PageRank) for Rangefind.
//
// Rangefind's static, no-server design says: do the expensive graph work once
// at build time and ship the answer as a plain numeric doc-value. A page's
// link-graph authority ("linkRank") is exactly that kind of prior — a global
// signal that cannot be recomputed cheaply per query but folds trivially into
// ranking and sorting once materialized.
//
// The input is a directed graph over dense node ordinals (0..n-1). Edges come
// from internal hyperlinks discovered while crawling a static site (see
// src/crawler.js), but nothing here is HTML-specific: any corpus that can
// express "document i points at documents [...]" can produce the same signal.
//
// The output is a per-node score in [0, 1] where the most authoritative page
// is 1. Keeping it normalized means the query-time boost (score prior) needs no
// runtime normalization pass and stays comparable to a plain multiplicative
// document boost.

// Standard PageRank defaults. 40 iterations converges comfortably for the
// modest graphs a static-site crawl produces (hundreds to tens of thousands of
// pages); the loop also early-exits once the L1 delta drops below `tolerance`.
export const DEFAULT_DAMPING = 0.85;
export const DEFAULT_ITERATIONS = 40;
export const DEFAULT_TOLERANCE = 1e-9;

// Compress an adjacency list (array of target-ordinal arrays) into CSR arrays.
// CSR (compressed sparse row) keeps the whole edge set in two flat typed arrays
// instead of n small JS arrays, so the power-iteration inner loop is a tight
// contiguous scan rather than a pointer chase — the same reason the rest of the
// builder favours typed-array layouts.
function toCsr(adjacency, n) {
  const rowStart = new Int32Array(n + 1);
  for (let i = 0; i < n; i++) {
    const row = adjacency[i];
    rowStart[i + 1] = rowStart[i] + (row ? row.length : 0);
  }
  const targets = new Int32Array(rowStart[n]);
  for (let i = 0; i < n; i++) {
    const row = adjacency[i];
    if (!row) continue;
    let at = rowStart[i];
    for (let k = 0; k < row.length; k++) targets[at++] = row[k];
  }
  return { rowStart, targets };
}

// Compute PageRank over a directed graph given as an adjacency list.
//
//   adjacency[i] = array of node ordinals that node i links to (out-links).
//
// Returns a `Float64Array` of length n summing to ~1. Dangling nodes (no
// out-links) redistribute their mass uniformly so no rank leaks out of the
// system. The computation is fully deterministic — same graph in, same ranks
// out, independent of platform — which the build's content-addressed,
// reproducible-output contract requires.
export function pageRank(adjacency, options = {}) {
  const n = adjacency.length;
  if (n === 0) return new Float64Array(0);
  if (n === 1) return Float64Array.of(1);

  const damping = clampDamping(options.damping);
  const maxIterations = Math.max(1, Math.floor(options.iterations ?? DEFAULT_ITERATIONS));
  const tolerance = Math.max(0, Number(options.tolerance ?? DEFAULT_TOLERANCE));

  const { rowStart, targets } = toCsr(adjacency, n);
  const outDeg = new Int32Array(n);
  for (let i = 0; i < n; i++) outDeg[i] = rowStart[i + 1] - rowStart[i];

  const initial = 1 / n;
  let rank = new Float64Array(n).fill(initial);
  let next = new Float64Array(n);
  const teleport = (1 - damping) / n;

  for (let iter = 0; iter < maxIterations; iter++) {
    // Mass held by dangling nodes is spread across every node, not lost.
    let dangling = 0;
    for (let i = 0; i < n; i++) if (outDeg[i] === 0) dangling += rank[i];
    const base = teleport + damping * dangling / n;
    next.fill(base);

    for (let i = 0; i < n; i++) {
      const deg = outDeg[i];
      if (deg === 0) continue;
      const contrib = damping * rank[i] / deg;
      const end = rowStart[i + 1];
      for (let k = rowStart[i]; k < end; k++) next[targets[k]] += contrib;
    }

    let delta = 0;
    for (let i = 0; i < n; i++) delta += Math.abs(next[i] - rank[i]);
    const swap = rank;
    rank = next;
    next = swap;
    if (delta <= tolerance) break;
  }

  return rank;
}

// Scale raw PageRank scores into [0, 1] with the most authoritative node at 1.
// A relative scale (divide by the max) rather than a min-max stretch keeps the
// signal proportional: a page with half the top page's authority reads as 0.5,
// which is what a multiplicative ranking prior wants.
export function normalizeRanks(rank) {
  const n = rank.length;
  const out = new Float64Array(n);
  let max = 0;
  for (let i = 0; i < n; i++) if (rank[i] > max) max = rank[i];
  if (max <= 0) {
    out.fill(n > 0 ? 1 : 0);
    return out;
  }
  for (let i = 0; i < n; i++) out[i] = rank[i] / max;
  return out;
}

function clampDamping(value) {
  const d = Number(value ?? DEFAULT_DAMPING);
  if (!Number.isFinite(d) || d < 0) return DEFAULT_DAMPING;
  return Math.min(0.99, d);
}

// Convenience: compute normalized linkRank scores in one call.
export function computeLinkRank(adjacency, options = {}) {
  return normalizeRanks(pageRank(adjacency, options));
}
