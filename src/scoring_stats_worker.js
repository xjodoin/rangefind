import { parentPort } from "node:worker_threads";
import { computeScoringStatsBatch } from "./scoring_stats_batch.js";

// Worker entry for the scoring-stats passes. Only ever loaded via
// `new Worker(...)` from scoring_stats.js — the batch logic lives in
// scoring_stats_batch.js so other modules can import it without this file's
// parentPort listener leaking into their worker threads.
//
// The config arrives once in an init message (not per batch): the analyzer
// memoizes per config object, so keeping one object alive per worker avoids
// rebuilding stemmer/stopword state on every batch.

let activeConfig = null;
let activeOptions = null;

parentPort.on("message", (message) => {
  try {
    if (message.kind === "init") {
      activeConfig = message.config;
      activeOptions = message.options || {};
      parentPort.postMessage({ id: message.id, ready: true });
      return;
    }
    parentPort.postMessage({
      id: message.id,
      ...computeScoringStatsBatch(message.lines, activeConfig, activeOptions)
    });
  } catch (error) {
    parentPort.postMessage({
      id: message.id,
      error: error?.stack || error?.message || String(error)
    });
  }
});
