import { analyzerForConfig } from "./analysis.js";
import { fieldIndexText, analyzeDocumentTerms } from "./scoring.js";
import { rawPath } from "./scan_doc.js";

// Batch computation for the scoring-stats passes, shared by the worker entry
// (scoring_stats_worker.js) and the sequential path. Kept free of any
// parentPort wiring: this module is imported inside other worker threads
// (via scoring_stats.js), which must not gain message listeners.

function docBbox(doc, geoFields, bbox) {
  for (const geoField of geoFields) {
    const lat = Number(rawPath(doc, geoField.latPath, ""));
    const lon = Number(rawPath(doc, geoField.lonPath, ""));
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
    if (!bbox.length) {
      bbox.push(lat, lon, lat, lon);
    } else {
      bbox[0] = Math.min(bbox[0], lat);
      bbox[1] = Math.min(bbox[1], lon);
      bbox[2] = Math.max(bbox[2], lat);
      bbox[3] = Math.max(bbox[3], lon);
    }
  }
  return bbox;
}

export function computeScoringStatsBatch(lines, config, options = {}) {
  const mode = options.mode || "totals";
  const analyzer = analyzerForConfig(config);
  const geoFields = config.geo || [];
  let total = 0;
  let inputBytes = 0;

  if (mode === "totals") {
    const fieldTotals = Object.fromEntries(config.fields.map(field => [field.name, 0]));
    const bbox = [];
    for (const line of lines || []) {
      if (!String(line || "").trim()) continue;
      inputBytes += Buffer.byteLength(line) + 1;
      const doc = JSON.parse(line);
      total++;
      const docLang = analyzer.docLanguage(doc, config);
      for (const field of config.fields) {
        fieldTotals[field.name] += analyzer.tokenize(fieldIndexText(doc, field, config), { unique: false, lang: docLang }).length;
      }
      docBbox(doc, geoFields, bbox);
    }
    return { total, inputBytes, fieldTotals, bbox: bbox.length ? bbox : null };
  }

  // mode === "df": count each selected term once per document, under the
  // frozen global average field lengths — term selection depends on them.
  const avgLens = options.avgLens;
  const df = new Map();
  for (const line of lines || []) {
    if (!String(line || "").trim()) continue;
    inputBytes += Buffer.byteLength(line) + 1;
    const doc = JSON.parse(line);
    total++;
    for (const [term] of analyzeDocumentTerms(doc, config, avgLens)) {
      df.set(term, (df.get(term) || 0) + 1);
    }
  }
  const terms = new Array(df.size);
  const dfs = new Array(df.size);
  let index = 0;
  for (const [term, count] of df) {
    terms[index] = term;
    dfs[index] = count;
    index++;
  }
  return { total, inputBytes, terms, dfs };
}
