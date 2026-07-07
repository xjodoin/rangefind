import { parentPort } from "node:worker_threads";
import { analyzerForConfig } from "./analysis.js";
import { fieldIndexText } from "./scoring.js";

function rawPath(object, path, fallback = "") {
  if (!path) return fallback;
  let value = object;
  for (const part of String(path).split(".")) {
    if (value == null) return fallback;
    value = value[part];
  }
  return value ?? fallback;
}

function valueList(value) {
  if (Array.isArray(value)) return value;
  if (value == null || value === "") return [];
  return [value];
}

function facetValues(doc, facet) {
  const labels = valueList(rawPath(doc, facet.labelPath || facet.path));
  return valueList(rawPath(doc, facet.path)).map((value, index) => ({
    value,
    label: labels[index] ?? value
  }));
}

function addFacetValue(dict, value, label = value) {
  const key = String(value || "");
  if (!key) return null;
  let item = dict.ids.get(key);
  if (!item) {
    item = { value: key, label: String(label || key), n: 0 };
    dict.ids.set(key, item);
    dict.values.push(item);
  }
  return item;
}

function measureLines(lines, config) {
  const fieldTotals = Object.fromEntries(config.fields.map(field => [field.name, 0]));
  const facets = Object.fromEntries(config.facets.map(facet => [facet.name, { ids: new Map(), values: [] }]));
  let total = 0;
  let inputBytes = 0;
  for (const line of lines || []) {
    if (!String(line || "").trim()) continue;
    inputBytes += Buffer.byteLength(line) + 1;
    const doc = JSON.parse(line);
    total++;
    const analyzer = analyzerForConfig(config);
    const docLang = analyzer.docLanguage(doc, config);
    for (const field of config.fields) {
      fieldTotals[field.name] += analyzer.tokenize(fieldIndexText(doc, field, config), { unique: false, lang: docLang }).length;
    }
    for (const facet of config.facets) {
      const dict = facets[facet.name];
      for (const item of facetValues(doc, facet)) {
        const value = addFacetValue(dict, item.value, item.label);
        if (value) value.n++;
      }
    }
  }
  return {
    total,
    inputBytes,
    fieldTotals,
    facets: Object.fromEntries(Object.entries(facets).map(([name, dict]) => [name, dict.values]))
  };
}

parentPort.on("message", ({ id, lines, config }) => {
  try {
    parentPort.postMessage({
      id,
      ...measureLines(lines, config)
    });
  } catch (error) {
    parentPort.postMessage({
      id,
      error: error?.stack || error?.message || String(error)
    });
  }
});
