import { parentPort } from "node:worker_threads";
import { addFieldExpansionScores, addFieldScores, bm25fScores, fieldText, selectDocTerms } from "./scoring.js";
import { surfacePairsForFields } from "./typo.js";

function analyzeDoc(doc, index, config, avgLens) {
  const weighted = new Map();
  const expansion = new Map();
  for (const field of config.fields) addFieldScores(doc, field, avgLens[field.name], weighted);
  for (const field of config.fields) addFieldExpansionScores(doc, field, expansion);
  return {
    index,
    selectedTerms: selectDocTerms(
      bm25fScores(weighted, config.bm25fK1),
      expansion,
      config.maxTermsPerDoc,
      config.maxExpansionTermsPerDoc
    ),
    typoSurfacePairs: [...surfacePairsForFields(doc, config.fields, fieldText)]
  };
}

parentPort.on("message", ({ id, docs, config, avgLens }) => {
  try {
    parentPort.postMessage({
      id,
      docs: docs.map(({ doc, index }) => analyzeDoc(doc, index, config, avgLens))
    });
  } catch (error) {
    parentPort.postMessage({
      id,
      error: error?.stack || error?.message || String(error)
    });
  }
});
