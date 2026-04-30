import { termCounts, tokenize } from "./analyzer.js";
import { getPath } from "./config.js";

function addWeighted(scores, term, weight) {
  if (!term || weight <= 0) return;
  scores.set(term, (scores.get(term) || 0) + weight);
}

export function fieldText(doc, field) {
  return String(getPath(doc, field.path, ""));
}

export function addFieldScores(doc, field, avgLen, scores) {
  const counts = termCounts(fieldText(doc, field));
  const len = [...counts.values()].reduce((sum, n) => sum + n, 0);
  const b = field.b ?? 0.75;
  const norm = 1 - b + b * (len / Math.max(1, avgLen));
  for (const [term, tf] of counts) {
    addWeighted(scores, term, (field.weight ?? 1) * tf / Math.max(0.2, norm));
  }

  if (field.phrase) {
    const terms = tokenize(fieldText(doc, field));
    for (const n of [2, 3]) {
      for (let i = 0; i <= terms.length - n; i++) {
        addWeighted(scores, terms.slice(i, i + n).join("_"), field.phraseWeight ?? 8);
      }
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
