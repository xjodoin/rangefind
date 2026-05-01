import { shardKey } from "./shards.js";

export const DOC_LAYOUT_FORMAT = "rflocal-doc-v1";

function isBaseTerm(term) {
  return !!term && !String(term).includes("_");
}

function layoutTermLimit(config) {
  return Math.max(1, Math.floor(Number(config.docLocalityTerms || 2) || 2));
}

function layoutShardDepth(config) {
  return Math.max(1, Math.floor(Number(config.docLocalityShardDepth || config.baseShardDepth || 1) || 1));
}

export function docLayoutRecord(index, selectedTerms, config = {}) {
  const limit = layoutTermLimit(config);
  const baseTerms = selectedTerms.filter(([term]) => isBaseTerm(term)).slice(0, limit);
  const terms = (baseTerms.length ? baseTerms : selectedTerms.slice(0, limit))
    .map(([term, score]) => [String(term || ""), Number(score) || 0]);
  const primary = terms[0]?.[0] || "";
  const secondary = terms[1]?.[0] || "";
  const score = Math.max(0, Math.round((terms[0]?.[1] || 0) * 1000));
  return {
    index,
    shard: primary ? shardKey(primary, layoutShardDepth(config)) : "",
    primary,
    secondary,
    score
  };
}

export function orderDocIdsByLocality(records, total) {
  const byIndex = new Map(records.map(record => [record.index, record]));
  const rows = [];
  for (let index = 0; index < total; index++) {
    rows.push(byIndex.get(index) || { index, shard: "", primary: "", secondary: "", score: 0 });
  }
  rows.sort((a, b) => {
    if (!!a.primary !== !!b.primary) return a.primary ? -1 : 1;
    return a.shard.localeCompare(b.shard)
      || a.primary.localeCompare(b.primary)
      || b.score - a.score
      || a.secondary.localeCompare(b.secondary)
      || a.index - b.index;
  });
  return rows.map(row => row.index);
}

export function summarizeDocLayout(records, total, config = {}) {
  const primary = new Set();
  let empty = 0;
  for (const record of records) {
    if (record.primary) primary.add(record.primary);
    else empty++;
  }
  return {
    format: DOC_LAYOUT_FORMAT,
    strategy: "primary-base-term-impact",
    terms: layoutTermLimit(config),
    shard_depth: layoutShardDepth(config),
    docs: total,
    docs_without_terms: empty,
    primary_terms: primary.size
  };
}
