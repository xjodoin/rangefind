import { TYPO_SHARD_MAGIC, readVarint } from "./binary.js";
import { assertMagic, readUtf8 } from "./codec.js";

export function typoMaxEditsFor(term, options = {}) {
  const max = options.maxEdits ?? 2;
  return term.length >= 8 ? max : Math.min(1, max);
}

export function typoDeleteKeys(term, options = {}, maxEdits = typoMaxEditsFor(term, options)) {
  const minLength = (options.minTermLength ?? 4) - (options.maxEdits ?? 2);
  const keys = new Set([term]);
  let frontier = new Set([term]);
  for (let edits = 1; edits <= maxEdits; edits++) {
    const next = new Set();
    for (const value of frontier) {
      if (value.length <= 1) continue;
      for (let i = 0; i < value.length; i++) {
        const key = value.slice(0, i) + value.slice(i + 1);
        if (key.length < minLength) continue;
        keys.add(key);
        next.add(key);
      }
    }
    frontier = next;
  }
  return keys;
}

export function typoShardFor(deleteKey, manifest, availableShards) {
  const maxDepth = manifest?.max_shard_depth || manifest?.base_shard_depth || 3;
  const baseDepth = manifest?.base_shard_depth || 2;
  for (let depth = maxDepth; depth >= baseDepth; depth--) {
    const key = String(deleteKey || "").slice(0, depth).padEnd(depth, "_");
    if (availableShards.has(key)) return key;
  }
  return String(deleteKey || "").slice(0, baseDepth).padEnd(baseDepth, "_");
}

export function parseTypoShard(buffer) {
  const bytes = new Uint8Array(buffer);
  assertMagic(bytes, TYPO_SHARD_MAGIC, "Unsupported Rangefind typo shard");
  const state = { pos: TYPO_SHARD_MAGIC.length };
  const pairCount = readVarint(bytes, state);
  const pairs = new Array(pairCount);
  for (let i = 0; i < pairCount; i++) {
    pairs[i] = { surface: readUtf8(bytes, state), term: readUtf8(bytes, state), df: readVarint(bytes, state) };
  }
  const keyCount = readVarint(bytes, state);
  const keys = new Map();
  let previous = "";
  for (let i = 0; i < keyCount; i++) {
    const prefix = readVarint(bytes, state);
    const suffix = readUtf8(bytes, state);
    const key = previous.slice(0, prefix) + suffix;
    keys.set(key, { offset: readVarint(bytes, state), count: readVarint(bytes, state), candidates: null });
    previous = key;
  }
  return { bytes, dataStart: state.pos, pairs, keys };
}

export function typoCandidatesForDeleteKey(shard, key) {
  const entry = shard.keys?.get(key);
  if (!entry) return [];
  if (entry.candidates) return entry.candidates;
  const state = { pos: shard.dataStart + entry.offset };
  const candidates = new Array(entry.count);
  for (let i = 0; i < entry.count; i++) candidates[i] = shard.pairs[readVarint(shard.bytes, state)] || { surface: "", term: "", df: 0 };
  entry.candidates = candidates;
  return candidates;
}

export function boundedDamerauLevenshtein(a, b, maxDistance) {
  if (a === b) return 0;
  if (Math.abs(a.length - b.length) > maxDistance) return maxDistance + 1;
  let prevPrev = new Array(b.length + 1).fill(0);
  let prev = Array.from({ length: b.length + 1 }, (_, i) => i);
  let current = new Array(b.length + 1);
  for (let i = 1; i <= a.length; i++) {
    current[0] = i;
    let rowMin = current[0];
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      let value = Math.min(prev[j] + 1, current[j - 1] + 1, prev[j - 1] + cost);
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) {
        value = Math.min(value, prevPrev[j - 2] + 1);
      }
      current[j] = value;
      if (value < rowMin) rowMin = value;
    }
    if (rowMin > maxDistance) return maxDistance + 1;
    [prevPrev, prev, current] = [prev, current, prevPrev];
  }
  return prev[b.length];
}

function commonPrefixLength(a, b) {
  const max = Math.min(a.length, b.length);
  let i = 0;
  while (i < max && a[i] === b[i]) i++;
  return i;
}

function lcsLength(a, b) {
  const previous = new Array(b.length + 1).fill(0);
  const current = new Array(b.length + 1).fill(0);
  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      current[j] = a[i - 1] === b[j - 1] ? previous[j - 1] + 1 : Math.max(previous[j], current[j - 1]);
    }
    for (let j = 0; j <= b.length; j++) previous[j] = current[j];
  }
  return previous[b.length];
}

export function typoCandidateScore(token, surface, df, distance) {
  const prefix = commonPrefixLength(token, surface);
  const sequenceSimilarity = lcsLength(token, surface) / Math.max(1, token.length);
  const sameFirst = token[0] && token[0] === surface[0] ? 1.2 : -1.6;
  const sameLast = token[token.length - 1] === surface[surface.length - 1] ? 0.35 : 0;
  const lengthPenalty = Math.abs(token.length - surface.length) * 0.25;
  return Math.log1p(df) * 1.15
    + Math.min(prefix, 4) * 0.15
    + sequenceSimilarity * 4.0
    + sameFirst
    + sameLast
    - distance * 2.35
    - lengthPenalty;
}
