export const DEFAULT_STOPWORDS = new Set([
  "a", "an", "and", "are", "as", "at", "be", "been", "being", "but", "by",
  "can", "could", "did", "do", "does", "for", "from", "had", "has", "have",
  "if", "in", "into", "is", "it", "its", "not", "of", "on", "or", "over",
  "than", "that", "the", "their", "then", "there", "these", "this", "those",
  "to", "under", "was", "were", "with", "within", "would",
  "au", "aux", "avec", "ce", "ces", "dans", "de", "des", "du", "elle", "en",
  "et", "eux", "il", "ils", "je", "la", "le", "les", "leur", "leurs", "lui",
  "ma", "mais", "me", "mes", "moi", "mon", "ne", "nos", "notre", "nous",
  "ou", "par", "pas", "pour", "qu", "que", "qui", "sa", "se", "ses", "son",
  "sur", "ta", "te", "tes", "toi", "ton", "tu", "un", "une", "vos", "votre",
  "vous"
]);

export function fold(text) {
  return String(text || "")
    .normalize("NFKD")
    .toLowerCase()
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/œ/g, "oe")
    .replace(/æ/g, "ae");
}

export function stem(token) {
  if (token.length < 5 || /^\d+$/u.test(token)) return token;
  return token
    .replace(/(ements|ement|ations|ation|iques|ique|ances|ance|ities|ity|ments|ment)$/u, "")
    .replace(/(issements|issement)$/u, "iss")
    .replace(/(euses|euse|eurs|eur|ives|ive|ifs|if)$/u, "")
    .replace(/(ies|ied|ing|ers|er|ed|es|s)$/u, "");
}

export function tokenize(text, options = {}) {
  const minLength = options.minLength ?? 2;
  const stopwords = options.stopwords || DEFAULT_STOPWORDS;
  const unique = options.unique !== false;
  const out = [];
  const seen = new Set();
  for (const raw of fold(text).split(/[^a-z0-9]+/u)) {
    if ((raw.length < minLength && !/^\d$/.test(raw)) || stopwords.has(raw)) continue;
    const token = stem(raw);
    if ((token.length < minLength && !/^\d$/.test(token)) || stopwords.has(token)) continue;
    if (unique && seen.has(token)) continue;
    seen.add(token);
    out.push(token);
  }
  return out;
}

export function termCounts(text, options = {}) {
  const counts = new Map();
  for (const token of tokenize(text, { ...options, unique: false })) {
    counts.set(token, (counts.get(token) || 0) + 1);
  }
  return counts;
}

export function surfaceStemPairs(text, options = {}) {
  const minLength = options.minLength ?? 2;
  const stopwords = options.stopwords || DEFAULT_STOPWORDS;
  const pairs = new Map();
  for (const raw of fold(text).split(/[^a-z0-9]+/u)) {
    if ((raw.length < minLength && !/^\d$/.test(raw)) || stopwords.has(raw)) continue;
    const token = stem(raw);
    if ((token.length >= minLength || /^\d$/.test(token)) && !stopwords.has(token)) {
      pairs.set(raw, token);
    }
  }
  return pairs;
}

export function analyzeTerms(text, options = {}) {
  const minLength = options.minLength ?? 2;
  const stopwords = options.stopwords || DEFAULT_STOPWORDS;
  const out = [];
  const seen = new Set();
  for (const raw of fold(text).split(/[^a-z0-9]+/u)) {
    if ((raw.length < minLength && !/^\d$/.test(raw)) || stopwords.has(raw)) continue;
    const term = stem(raw);
    if ((term.length < minLength && !/^\d$/.test(term)) || stopwords.has(term) || seen.has(term)) continue;
    seen.add(term);
    out.push({ raw, term });
  }
  return out;
}

export function queryTerms(text) {
  const terms = tokenize(text);
  return expandedTermsFromBaseTerms(terms);
}

export function queryBundleKeyFromBaseTerms(baseTerms) {
  const terms = [...new Set((baseTerms || []).map(term => String(term || "")).filter(Boolean))];
  if (terms.length < 2 || terms.length > 3) return "";
  return `exact-expanded-v1|${terms.join(" ")}`;
}

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

export function expandedTermsFromBaseTerms(terms) {
  const expanded = [...terms];
  for (const n of [2, 3]) {
    for (let i = 0; i <= terms.length - n; i++) {
      expanded.push(terms.slice(i, i + n).join("_"));
    }
  }
  return [...new Set(expanded)];
}

export function proximityTerm(left, right) {
  if (!left || !right || left === right) return "";
  const [a, b] = left < right ? [left, right] : [right, left];
  return `n_${a}_${b}`;
}
