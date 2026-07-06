import { analyzeTerms, fold, stem } from "./analyzer.js";

// Word scanning runs over the RAW display text (folding can change string
// length, so positions found in folded text would not map back). Combining
// marks stay inside words so decomposed accents highlight correctly.
const WORD_RE = /[\p{L}\p{M}\p{N}]+/gu;

export function highlightTermSet(query, correctedQuery = "") {
  const terms = new Set();
  for (const item of analyzeTerms(String(query || ""))) terms.add(item.term);
  for (const item of analyzeTerms(String(correctedQuery || ""))) terms.add(item.term);
  return terms;
}

function wordMatches(word, termSet) {
  const folded = fold(word);
  if (termSet.has(folded)) return true;
  const stemmed = stem(folded);
  return stemmed !== folded && termSet.has(stemmed);
}

// Finds [start, end) ranges of query-term occurrences in raw text using the
// same fold+stem normalization the indexer applies, so "montreal" highlights
// "Montréal" and "walk" highlights "walking".
export function findMatchRanges(text, termSet) {
  const ranges = [];
  if (!termSet?.size) return ranges;
  WORD_RE.lastIndex = 0;
  let match;
  while ((match = WORD_RE.exec(text))) {
    if (wordMatches(match[0], termSet)) {
      ranges.push([match.index, match.index + match[0].length]);
    }
  }
  return ranges;
}

function distinctTermsIn(text, ranges, from, to, termSet) {
  const seen = new Set();
  for (const [start, end] of ranges) {
    if (start < from || end > to) continue;
    seen.add(stem(fold(text.slice(start, end))));
  }
  return seen.size;
}

// Picks the window of at most maxChars covering the most distinct query
// terms (ties: most matches, then earliest), snaps it to word boundaries,
// and returns the snippet text with match ranges rebased onto it.
export function highlightText(text, termSet, options = {}) {
  const raw = String(text || "");
  if (!raw) return null;
  const maxChars = Math.max(40, Math.floor(Number(options.maxChars ?? 240)));
  const ranges = findMatchRanges(raw, termSet);
  if (!ranges.length) return null;

  let windowStart = 0;
  if (raw.length > maxChars) {
    let best = { distinct: -1, count: -1, start: 0 };
    // Slide over match positions: each candidate window starts a little
    // before one of the matches, so evaluation is O(matches^2) with small
    // constants instead of scanning every character offset.
    for (const [start] of ranges) {
      const candidate = Math.max(0, Math.min(start - 30, raw.length - maxChars));
      const to = candidate + maxChars;
      const distinct = distinctTermsIn(raw, ranges, candidate, to, termSet);
      const count = ranges.filter(([s, e]) => s >= candidate && e <= to).length;
      if (distinct > best.distinct || (distinct === best.distinct && count > best.count)) {
        best = { distinct, count, start: candidate };
      }
    }
    windowStart = best.start;
  }

  let from = windowStart;
  let to = Math.min(raw.length, from + maxChars);
  // Snap to word boundaries so the snippet never opens or closes mid-word.
  if (from > 0) {
    const nextSpace = raw.indexOf(" ", from);
    if (nextSpace !== -1 && nextSpace < from + 24) from = nextSpace + 1;
  }
  if (to < raw.length) {
    const previousSpace = raw.lastIndexOf(" ", to);
    if (previousSpace > from + maxChars / 2) to = previousSpace;
  }

  const prefix = from > 0 ? "… " : "";
  const suffix = to < raw.length ? " …" : "";
  const snippetRanges = [];
  for (const [start, end] of ranges) {
    if (start < from || end > to) continue;
    snippetRanges.push([start - from + prefix.length, end - from + prefix.length]);
  }
  if (!snippetRanges.length) return null;
  return {
    text: prefix + raw.slice(from, to) + suffix,
    ranges: snippetRanges,
    matches: snippetRanges.length
  };
}

// Adds `highlights: { field: { text, ranges } }` to hydrated results.
// Ranges are plain offsets so hosts can render <mark>s without any HTML
// passing through the engine.
export function applyHighlights(results, termSet, options = {}) {
  const wanted = Array.isArray(options.fields) && options.fields.length ? options.fields : null;
  const maxChars = options.maxChars;
  for (const result of results) {
    const highlights = {};
    let any = false;
    for (const [field, value] of Object.entries(result)) {
      if (typeof value !== "string" || !value) continue;
      if (wanted ? !wanted.includes(field) : field === "id" || field === "url") continue;
      const highlight = highlightText(value, termSet, { maxChars });
      if (highlight) {
        highlights[field] = highlight;
        any = true;
      }
    }
    if (any) result.highlights = highlights;
  }
  return results;
}
