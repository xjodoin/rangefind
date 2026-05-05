import { parentPort } from "node:worker_threads";
import { analyzeDocumentTerms, fieldIndexText } from "./scoring.js";
import { surfacePairsForFields } from "./typo.js";

function analyzeDoc(doc, index, config, avgLens) {
  return {
    index,
    selectedTerms: analyzeDocumentTerms(doc, config, avgLens),
    typoSurfacePairs: [...surfacePairsForFields(doc, config.fields, (source, field) => fieldIndexText(source, field, config))]
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
