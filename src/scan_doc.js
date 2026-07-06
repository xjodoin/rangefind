import { queryBundleKeyFromBaseTerms, tokenize } from "./analyzer.js";
import { getPath } from "./config.js";
import { fieldIndexText } from "./scoring.js";
import { varintLength, writeVarint } from "./runs.js";

const textEncoder = new TextEncoder();

export function rawPath(object, path, fallback = "") {
  if (!path) return fallback;
  let value = object;
  for (const part of String(path).split(".")) {
    if (value == null) return fallback;
    value = value[part];
  }
  return value ?? fallback;
}

export function valueList(value) {
  if (Array.isArray(value)) return value;
  if (value == null || value === "") return [];
  return [value];
}

export function normalizedNumberType(field) {
  return String(field.type || "int").toLowerCase();
}

export function numericValue(doc, field) {
  const value = rawPath(doc, field.path);
  if (value == null || value === "") return null;
  const type = normalizedNumberType(field);
  if (type === "date") {
    const time = value instanceof Date ? value.getTime() : Date.parse(String(value));
    return Number.isFinite(time) ? time : null;
  }
  const number = Number(value);
  if (!Number.isFinite(number)) return null;
  if (type === "float" || type === "double") return number;
  return Math.round(number);
}

export function booleanValue(doc, field) {
  const value = rawPath(doc, field.path);
  if (value === true || value === 1 || value === "true" || value === "1") return true;
  if (value === false || value === 0 || value === "false" || value === "0") return false;
  return null;
}

export function suggestEnabled(config) {
  return Array.isArray(config.suggest) && config.suggest.length > 0;
}

// Rows are [surface, weight, tokenPrefixes]; the caller aggregates duplicate
// surfaces before anything crosses a worker boundary.
export function suggestRowsForDoc(config, doc) {
  const rows = [];
  for (const field of config.suggest) {
    const weightValue = field.weightPath ? Number(rawPath(doc, field.weightPath)) : 0;
    const weight = Number.isFinite(weightValue) && weightValue > 0 ? Math.floor(weightValue) : 0;
    for (const value of valueList(rawPath(doc, field.path))) {
      const surface = String(value ?? "").trim();
      if (surface) rows.push([surface, weight, field.tokenPrefixes !== false]);
    }
  }
  return rows;
}

export function facetValues(doc, facet) {
  const labels = valueList(rawPath(doc, facet.labelPath || facet.path));
  return valueList(rawPath(doc, facet.path)).map((value, index) => ({
    value,
    label: labels[index] ?? value
  }));
}

export function docPayload(doc, config, index) {
  const payload = { id: String(getPath(doc, config.idPath || "id", index)), index };
  for (const item of config.display) {
    if (typeof item === "string") {
      payload[item] = getPath(doc, item);
    } else {
      let value = getPath(doc, item.path);
      const maxChars = Number(item.maxChars || 0);
      if (maxChars > 0 && typeof value === "string" && value.length > maxChars) {
        value = value.slice(0, maxChars).trimEnd();
      }
      payload[item.name] = value;
    }
  }
  if (!payload.title) payload.title = payload.id;
  if (!payload.url) payload.url = getPath(doc, config.urlPath || "url", "");
  return payload;
}

export function docPayloadFieldNames(config) {
  const fields = ["id"];
  for (const item of config.display) fields.push(typeof item === "string" ? item : item.name);
  fields.push("title", "url");
  return [...new Set(fields)].filter(field => field && field !== "index");
}

export function encodeSelectedTerms(selectedTerms) {
  const terms = selectedTerms.map(([term, score]) => [String(term || ""), Math.max(1, Math.round(score * 1000))]);
  let bytes = varintLength(terms.length);
  const encodedTerms = terms.map(([term, score]) => {
    const termBytes = textEncoder.encode(term);
    bytes += varintLength(termBytes.length) + termBytes.length + varintLength(score);
    return [termBytes, score];
  });
  const out = Buffer.allocUnsafe(bytes);
  let pos = writeVarint(out, 0, encodedTerms.length);
  for (const [termBytes, score] of encodedTerms) {
    pos = writeVarint(out, pos, termBytes.length);
    out.set(termBytes, pos);
    pos += termBytes.length;
    pos = writeVarint(out, pos, score);
  }
  return out;
}

export function queryBundlesEnabled(config) {
  return config.queryBundles !== false && Math.max(0, Number(config.queryBundleMaxKeys || 0)) > 0;
}

export function isBundlePhraseTerm(term, config) {
  if (!term || term.startsWith("n_") || !term.includes("_")) return false;
  const parts = term.split("_").filter(Boolean);
  const maxTerms = Math.max(2, Math.min(3, Math.floor(Number(config.queryBundleMaxTerms || 3))));
  return parts.length >= 2 && parts.length <= maxTerms && parts.every(part => part && !part.includes("_"));
}

export function queryBundleSeedCandidatesForDoc(config, selectedTerms, fieldTerms, doc) {
  const selected = new Set(selectedTerms.map(([term]) => term));
  const docKeys = new Set();
  const candidates = [];
  const maxTerms = Math.max(2, Math.min(3, Math.floor(Number(config.queryBundleMaxTerms || 3))));

  function addCandidate(baseTerms) {
    if (!baseTerms.every(base => selected.has(base))) return;
    const key = queryBundleKeyFromBaseTerms(baseTerms);
    if (!key || docKeys.has(key)) return;
    docKeys.add(key);
    candidates.push({ key, baseTerms });
  }

  for (const [term] of selectedTerms) {
    if (!isBundlePhraseTerm(term, config)) continue;
    addCandidate(term.split("_"));
  }
  for (let fieldIndex = 0; fieldIndex < config.fields.length; fieldIndex++) {
    const field = config.fields[fieldIndex];
    const limit = Math.max(0, Math.floor(Number(field.queryBundleSeedMaxTokens ?? config.queryBundleSeedMaxFieldTokens ?? 512)));
    if (!limit || field.queryBundles === false) continue;
    const terms = fieldTerms?.[fieldIndex] || tokenize(fieldIndexText(doc, field, config), { unique: false }).slice(0, limit);
    for (let n = 2; n <= maxTerms; n++) {
      for (let i = 0; i <= terms.length - n; i++) {
        const baseTerms = terms.slice(i, i + n);
        if (new Set(baseTerms).size !== baseTerms.length) continue;
        addCandidate(baseTerms);
      }
    }
  }
  return candidates;
}
