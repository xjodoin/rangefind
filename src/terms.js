// Language-agnostic term combinatorics shared by the builder, the runtime,
// and the analyzer. These operate on already-analyzed base terms (folded,
// stemmed strings) and never touch text analysis themselves, so they live
// apart from the analyzer and carry no language state.

// Phrase n-gram expansion: base terms plus their contiguous 2- and 3-grams
// joined with "_". Deduplicated, original order preserved. Already-analyzed
// terms are never re-analyzed here.
export function expandedTermsFromBaseTerms(terms) {
  const expanded = [...terms];
  for (const n of [2, 3]) {
    for (let i = 0; i <= terms.length - n; i++) {
      expanded.push(terms.slice(i, i + n).join("_"));
    }
  }
  return [...new Set(expanded)];
}

// Order-independent proximity pair name for two distinct terms.
export function proximityTerm(left, right) {
  if (!left || !right || left === right) return "";
  const [a, b] = left < right ? [left, right] : [right, left];
  return `n_${a}_${b}`;
}

// Query-bundle key for a 2- or 3-term base set (deduplicated, order kept).
export function queryBundleKeyFromBaseTerms(baseTerms) {
  const terms = [...new Set((baseTerms || []).map(term => String(term || "")).filter(Boolean))];
  if (terms.length < 2 || terms.length > 3) return "";
  return `exact-expanded-v1|${terms.join(" ")}`;
}

// Every 2- and 3-term contiguous window of a base-term list, as bundle
// lookup plans (longest first).
export function queryBundleKeysFromBaseTerms(baseTerms) {
  const terms = [...new Set((baseTerms || []).map(term => String(term || "")).filter(Boolean))];
  const out = [];
  for (let n = Math.min(3, terms.length); n >= 2; n--) {
    for (let i = 0; i <= terms.length - n; i++) {
      const base = terms.slice(i, i + n);
      const key = queryBundleKeyFromBaseTerms(base);
      if (key) out.push({ key, baseTerms: base, expandedTerms: expandedTermsFromBaseTerms(base) });
    }
  }
  return out;
}
