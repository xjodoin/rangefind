const COMMON_SUBSTITUTIONS = "aeiourstnlcmpdbfgvhqxyzkjw";
const COMMON_SUFFIXES = [
  "",
  "s",
  "e",
  "es",
  "er",
  "eur",
  "eurs",
  "euse",
  "ement",
  "ements",
  "ation",
  "ations",
  "ique",
  "iques",
  "ance",
  "ances",
  "ite",
  "ites"
];

export const MAIN_INDEX_TYPO_DEFAULTS = {
  mode: "main-index",
  trigger: "zero-or-weak",
  maxEdits: 2,
  maxTokenCandidates: 8,
  maxQueryPlans: 5,
  maxCorrectedSearches: 3,
  maxShardLookups: 12,
  weakResultTotal: 0
};

export function normalizeMainIndexTypoOptions(options = {}, manifest = {}) {
  const manifestOptions = manifest.search?.typo || {};
  const mode = String(options.typoMode ?? manifestOptions.mode ?? MAIN_INDEX_TYPO_DEFAULTS.mode).toLowerCase();
  const trigger = String(options.typoTrigger ?? manifestOptions.trigger ?? MAIN_INDEX_TYPO_DEFAULTS.trigger).toLowerCase();
  return {
    mode: mode === "off" || mode === "false" || mode === "none" ? "off" : "main-index",
    trigger: trigger === "zero" ? "zero" : "zero-or-weak",
    maxEdits: positiveInt(options.typoMaxEdits ?? manifestOptions.maxEdits, MAIN_INDEX_TYPO_DEFAULTS.maxEdits, 1, 3),
    maxTokenCandidates: positiveInt(options.typoMaxTokenCandidates ?? manifestOptions.maxTokenCandidates, MAIN_INDEX_TYPO_DEFAULTS.maxTokenCandidates, 1, 32),
    maxQueryPlans: positiveInt(options.typoMaxQueryPlans ?? manifestOptions.maxQueryPlans, MAIN_INDEX_TYPO_DEFAULTS.maxQueryPlans, 1, 32),
    maxCorrectedSearches: positiveInt(options.typoMaxCorrectedSearches ?? manifestOptions.maxCorrectedSearches, MAIN_INDEX_TYPO_DEFAULTS.maxCorrectedSearches, 1, 8),
    maxShardLookups: positiveInt(options.typoMaxShardLookups ?? manifestOptions.maxShardLookups, MAIN_INDEX_TYPO_DEFAULTS.maxShardLookups, 1, 64),
    weakResultTotal: positiveInt(options.typoWeakResultTotal ?? manifestOptions.weakResultTotal, MAIN_INDEX_TYPO_DEFAULTS.weakResultTotal, 0, 10)
  };
}

export function typoMaxEditsFor(term, options = MAIN_INDEX_TYPO_DEFAULTS) {
  const max = options.maxEdits ?? MAIN_INDEX_TYPO_DEFAULTS.maxEdits;
  return term.length >= 8 ? max : Math.min(1, max);
}

export function isTypoCorrectionToken(token) {
  return /^[a-z][a-z0-9]*$/u.test(token) && !/^\d+$/u.test(token) && token.length >= 3 && token.length <= 32;
}

export function mainIndexTypoProbeValues(raw, term, options = MAIN_INDEX_TYPO_DEFAULTS) {
  const max = Math.max(1, options.maxShardLookups || MAIN_INDEX_TYPO_DEFAULTS.maxShardLookups);
  const seeds = [term, raw].map(value => String(value || "")).filter(isTypoCorrectionToken);
  const out = new Set(seeds);
  const target = seeds.find(value => value.length >= 4) || seeds[0] || "";
  if (!target) return [];

  const priorityPositions = [...new Set([0, 1, 2, 3, target.length - 1].filter(index => index >= 0 && index < target.length))];
  for (const i of priorityPositions) {
    out.add(target.slice(0, i) + target.slice(i + 1));
    if (out.size >= max) return [...out].filter(isTypoCorrectionToken).slice(0, max);
  }
  for (const i of priorityPositions.filter(index => index < target.length - 1)) {
    out.add(target.slice(0, i) + target[i + 1] + target[i] + target.slice(i + 2));
    if (out.size >= max) return [...out].filter(isTypoCorrectionToken).slice(0, max);
  }
  const substitutionPositions = [...new Set([1, 0, 2, 3, target.length - 1].filter(index => index >= 0 && index < target.length))];
  for (const i of substitutionPositions) {
    for (const char of COMMON_SUBSTITUTIONS) {
      if (char === target[i]) continue;
      out.add(target.slice(0, i) + char + target.slice(i + 1));
      if (out.size >= max) break;
    }
    if (out.size >= max) break;
  }
  return [...out].filter(isTypoCorrectionToken).slice(0, max);
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

export function bestMainIndexTypoDistance(token, candidate, maxEdits) {
  let best = {
    surface: candidate,
    distance: boundedDamerauLevenshtein(token, candidate, maxEdits)
  };
  for (const suffix of COMMON_SUFFIXES) {
    if (!suffix) continue;
    const surface = `${candidate}${suffix}`;
    if (Math.abs(surface.length - token.length) > maxEdits) continue;
    const distance = boundedDamerauLevenshtein(token, surface, maxEdits);
    if (distance < best.distance) best = { surface, distance };
  }
  return best;
}

export function ngramOverlap(left, right) {
  const n = Math.min(left.length, right.length) <= 5 ? 2 : 3;
  const a = grams(left, n);
  const b = grams(right, n);
  if (!a.size || !b.size) return left[0] === right[0] ? 0.25 : 0;
  let shared = 0;
  for (const gram of a) if (b.has(gram)) shared++;
  return shared / Math.max(a.size, b.size);
}

export function mainIndexTypoCandidateScore(token, surface, df, distance) {
  const prefix = commonPrefixLength(token, surface);
  const sequenceSimilarity = lcsLength(token, surface) / Math.max(1, token.length);
  const sameFirst = token[0] && token[0] === surface[0] ? 1.2 : -1.2;
  const sameLast = token[token.length - 1] === surface[surface.length - 1] ? 0.8 : 0;
  const lengthPenalty = Math.abs(token.length - surface.length) * 0.35;
  return Math.log1p(df) * 0.35
    + Math.min(prefix, 4) * 0.25
    + sequenceSimilarity * 5.0
    + sameFirst
    + sameLast
    - distance * 2.6
    - lengthPenalty;
}

function positiveInt(value, fallback, min, max) {
  const parsed = Math.floor(Number(value ?? fallback));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function grams(value, n) {
  const out = new Set();
  for (let i = 0; i <= value.length - n; i++) out.add(value.slice(i, i + n));
  return out;
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
