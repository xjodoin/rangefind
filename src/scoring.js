import { proximityTerm, termCounts, tokenize } from "./analyzer.js";
import { getPath } from "./config.js";

function addWeighted(scores, term, weight) {
  if (!term || weight <= 0) return;
  scores.set(term, (scores.get(term) || 0) + weight);
}

function addCount(counts, term) {
  counts.set(term, (counts.get(term) || 0) + 1);
}

function termCountsFromTerms(terms) {
  const counts = new Map();
  for (const term of terms || []) addCount(counts, term);
  return counts;
}

function queryBundleSeedLimit(field, config = {}) {
  return Math.max(0, Math.floor(Number(field.queryBundleSeedMaxTokens ?? config.queryBundleSeedMaxFieldTokens ?? 512)));
}

export function fieldText(doc, field) {
  return String(getPath(doc, field.path, ""));
}

export function isAlwaysIndexField(field, config = {}) {
  const names = new Set((config.alwaysIndexFields || []).map(String));
  return names.has(String(field.name || "")) || names.has(String(field.path || ""));
}

export function fieldIndexText(doc, field, config = {}) {
  const text = fieldText(doc, field);
  const limit = Math.max(0, Math.floor(Number(field.indexChars ?? config.bodyIndexChars ?? 0)));
  return limit > 0 && !isAlwaysIndexField(field, config) && text.length > limit ? text.slice(0, limit) : text;
}

export function analyzeFieldText(doc, field, config = {}) {
  const text = fieldIndexText(doc, field, config);
  const terms = tokenize(text, { unique: false });
  return {
    text,
    terms,
    counts: termCountsFromTerms(terms),
    length: terms.length
  };
}

export function addFieldScores(doc, field, avgLen, scores, options = {}) {
  const analysis = options.analysis || null;
  const text = analysis ? analysis.text : options.text ?? fieldIndexText(doc, field, options.config || {});
  const terms = analysis?.terms || null;
  const counts = analysis?.counts || termCounts(text);
  const len = analysis?.length ?? [...counts.values()].reduce((sum, n) => sum + n, 0);
  const b = field.b ?? 0.75;
  const norm = 1 - b + b * (len / Math.max(1, avgLen));
  for (const [term, tf] of counts) {
    addWeighted(scores, term, (field.weight ?? 1) * tf / Math.max(0.2, norm));
  }

  if (field.phrase) {
    const phraseTerms = terms || tokenize(text, { unique: false });
    for (const n of [2, 3]) {
      for (let i = 0; i <= phraseTerms.length - n; i++) {
        addWeighted(scores, phraseTerms.slice(i, i + n).join("_"), field.phraseWeight ?? 8);
      }
    }
  }
}

export function addFieldExpansionScores(doc, field, scores, options = {}) {
  if (!field.proximity && !field.proximityWeight) return;
  const text = options.analysis ? options.analysis.text : options.text ?? fieldIndexText(doc, field, options.config || {});
  const terms = (options.analysis?.terms || tokenize(text, { unique: false })).slice(0, field.maxProximityTokens ?? 96);
  const window = field.proximityWindow ?? 5;
  const weight = field.proximityWeight ?? 3.5;
  const seen = new Set();
  for (let i = 0; i < terms.length; i++) {
    const end = Math.min(terms.length, i + window + 1);
    for (let j = i + 1; j < end; j++) {
      const term = proximityTerm(terms[i], terms[j]);
      if (!term || seen.has(term)) continue;
      seen.add(term);
      addWeighted(scores, term, weight / Math.max(1, j - i));
    }
  }
}

export function bm25fScores(weightedTf, k1) {
  const out = new Map();
  for (const [term, tf] of weightedTf) {
    out.set(term, ((k1 + 1) * tf) / (k1 + tf));
  }
  return out;
}

export function topTerms(scores, limit) {
  return [...scores.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, limit);
}

export function selectDocTerms(baseScores, expansionScores, baseLimit, expansionLimit) {
  const selected = new Map(topTerms(baseScores, baseLimit));
  for (const [term, score] of topTerms(expansionScores, expansionLimit)) {
    selected.set(term, Math.max(selected.get(term) || 0, score));
  }
  return [...selected.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

export function selectBudgetedDocTerms(alwaysScores, baseScores, expansionScores, baseLimit, expansionLimit) {
  const selected = new Map([...alwaysScores.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])));
  for (const [term, score] of topTerms(baseScores, baseLimit)) {
    selected.set(term, Math.max(selected.get(term) || 0, score));
  }
  for (const [term, score] of topTerms(expansionScores, expansionLimit)) {
    selected.set(term, Math.max(selected.get(term) || 0, score));
  }
  return [...selected.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

export function analyzeDocumentTerms(doc, config, avgLens) {
  return analyzeDocumentForIndex(doc, config, avgLens).selectedTerms;
}

export function analyzeDocumentForIndex(doc, config, avgLens, options = {}) {
  const always = new Map();
  const weighted = new Map();
  const expansion = new Map();
  const alwaysNames = new Set((config.alwaysIndexFields || []).map(String));
  const includeFieldTerms = Boolean(options.includeFieldTerms);
  const fieldTerms = includeFieldTerms ? new Array(config.fields.length).fill(null) : null;
  for (let index = 0; index < config.fields.length; index++) {
    const field = config.fields[index];
    const analysis = analyzeFieldText(doc, field, config);
    const alwaysField = alwaysNames.has(String(field.name || "")) || alwaysNames.has(String(field.path || ""));
    addFieldScores(doc, field, avgLens[field.name], alwaysField ? always : weighted, { analysis, config });
    if (!alwaysField) addFieldExpansionScores(doc, field, expansion, { analysis, config });
    if (includeFieldTerms && field.queryBundles !== false) {
      const limit = queryBundleSeedLimit(field, config);
      if (limit) fieldTerms[index] = analysis.terms.slice(0, limit);
    }
  }
  const selectedTerms = selectBudgetedDocTerms(
    bm25fScores(always, config.bm25fK1),
    bm25fScores(weighted, config.bm25fK1),
    expansion,
    config.targetPostingsPerDoc,
    config.maxExpansionTermsPerDoc
  );
  return fieldTerms ? { selectedTerms, fieldTerms } : { selectedTerms };
}
