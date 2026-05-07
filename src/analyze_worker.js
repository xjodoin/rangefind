import { parentPort } from "node:worker_threads";
import { analyzeDocumentForIndex } from "./scoring.js";

function analyzeDoc(doc, index, config, avgLens) {
  const analysis = analyzeDocumentForIndex(doc, config, avgLens, {
    includeFieldTerms: config.queryBundles !== false && Math.max(0, Number(config.queryBundleMaxKeys || 0)) > 0
  });
  return {
    index,
    selectedTerms: analysis.selectedTerms,
    fieldTerms: analysis.fieldTerms || null
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
