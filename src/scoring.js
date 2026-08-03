import { proximityTerm } from "./terms.js";
import { analyzerForConfig } from "./analysis.js";
import { getPath } from "./config.js";

const EMPTY_ALWAYS_INDEX_FIELDS = new Set();
const alwaysIndexFieldsByConfig = new WeakMap();

function alwaysIndexFieldNames(config) {
  const source = config?.alwaysIndexFields;
  if (!Array.isArray(source) || !source.length) return EMPTY_ALWAYS_INDEX_FIELDS;
  const cached = alwaysIndexFieldsByConfig.get(config);
  if (cached?.source === source && cached.length === source.length) return cached.names;
  const names = new Set(source.map(String));
  alwaysIndexFieldsByConfig.set(config, { source, length: source.length, names });
  return names;
}

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

function rawFieldValue(doc, path, fallback = "") {
  if (!path) return fallback;
  let value = doc;
  for (const part of String(path).split(".")) {
    if (value == null) return fallback;
    value = value[part];
  }
  return value ?? fallback;
}

export function isAlwaysIndexField(field, config = {}) {
  const names = alwaysIndexFieldNames(config);
  return names.has(String(field.name || "")) || names.has(String(field.path || ""));
}

export function fieldIndexText(doc, field, config = {}) {
  const text = fieldText(doc, field);
  const limit = Math.max(0, Math.floor(Number(field.indexChars ?? config.bodyIndexChars ?? 0)));
  return limit > 0 && !isAlwaysIndexField(field, config) && text.length > limit ? text.slice(0, limit) : text;
}

export function analyzeFieldText(doc, field, config = {}, options = {}) {
  const analyzer = options.analyzer || analyzerForConfig(config);
  const lang = options.lang ?? analyzer.docLanguage(doc, config);
  const text = fieldIndexText(doc, field, config);
  const terms = analyzer.tokenize(text, { unique: false, lang });
  const rawValue = rawFieldValue(doc, field.path, "");
  let phraseRuns = null;
  if (field.phrase && Array.isArray(rawValue)) {
    let consumed = 0;
    phraseRuns = [];
    for (const rawItem of rawValue) {
      if (consumed >= text.length) break;
      const item = String(rawItem ?? "");
      const indexedItem = item.slice(0, Math.max(0, text.length - consumed));
      const run = analyzer.tokenize(indexedItem, { unique: false, lang });
      if (run.length) phraseRuns.push(run);
      // getPath joins array items with one space. Count it against a truncated
      // field without letting it join two independent phrase runs.
      consumed += item.length + 1;
    }
  }
  return {
    text,
    terms,
    counts: termCountsFromTerms(terms),
    length: terms.length,
    lang,
    ...(phraseRuns ? { phraseRuns } : {})
  };
}

export function addFieldScores(doc, field, avgLen, scores, options = {}) {
  const analysis = options.analysis || null;
  const analyzer = options.analyzer || analyzerForConfig(options.config || {});
  const text = analysis ? analysis.text : options.text ?? fieldIndexText(doc, field, options.config || {});
  const lang = analysis?.lang ?? options.lang ?? analyzer.docLanguage(doc, options.config || {});
  const terms = analysis?.terms || null;
  const counts = analysis?.counts || analyzer.termCounts(text, { lang });
  const len = analysis?.length ?? [...counts.values()].reduce((sum, n) => sum + n, 0);
  const b = field.b ?? 0.75;
  const norm = 1 - b + b * (len / Math.max(1, avgLen));
  for (const [term, tf] of counts) {
    addWeighted(scores, term, (field.weight ?? 1) * tf / Math.max(0.2, norm));
  }

  if (field.phrase) {
    const phraseTerms = terms || analyzer.tokenize(text, { unique: false, lang });
    const phraseRuns = analysis?.phraseRuns || [phraseTerms];
    for (const run of phraseRuns) {
      for (const n of [2, 3]) {
        for (let i = 0; i <= run.length - n; i++) {
          addWeighted(scores, run.slice(i, i + n).join("_"), field.phraseWeight ?? 8);
        }
      }
    }
  }
}

export function addFieldExpansionScores(doc, field, scores, options = {}) {
  if (!field.proximity && !field.proximityWeight) return;
  const analyzer = options.analyzer || analyzerForConfig(options.config || {});
  const text = options.analysis ? options.analysis.text : options.text ?? fieldIndexText(doc, field, options.config || {});
  const lang = options.analysis?.lang ?? options.lang ?? analyzer.docLanguage(doc, options.config || {});
  const terms = (options.analysis?.terms || analyzer.tokenize(text, { unique: false, lang })).slice(0, field.maxProximityTokens ?? 96);
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
  const alwaysNames = alwaysIndexFieldNames(config);
  const includeFieldTerms = Boolean(options.includeFieldTerms);
  const fieldTerms = includeFieldTerms ? new Array(config.fields.length).fill(null) : null;
  const analyzer = analyzerForConfig(config);
  const lang = analyzer.docLanguage(doc, config);
  for (let index = 0; index < config.fields.length; index++) {
    const field = config.fields[index];
    const analysis = analyzeFieldText(doc, field, config, { analyzer, lang });
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
