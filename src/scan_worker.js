import { parentPort } from "node:worker_threads";
import { gzipSync } from "node:zlib";
import { authorityEnabled, authorityFields, authorityRecordsForDoc } from "./authority_index.js";
import { docLayoutRecord } from "./doc_layout.js";
import { encodeDocPageColumns } from "./doc_pages.js";
import {
  booleanValue,
  docPayload,
  docPayloadFieldNames,
  encodeSelectedTerms,
  facetValues,
  numericValue,
  queryBundlesEnabled,
  queryBundleSeedCandidatesForDoc,
  rawPath,
  suggestEnabled,
  suggestRowsForDoc,
  vectorsEnabled
} from "./scan_doc.js";
import { normalizeVector, vectorFromValue } from "./vector_index.js";
import { analyzeDocumentForIndex } from "./scoring.js";
import { addSegmentPosting, createSegmentBuilder, finishSegmentBuilder, flushSegment, shouldFlushSegment } from "./segment_builder.js";

let state = null;

function initState(message) {
  const config = message.config;
  state = {
    config,
    avgLens: message.avgLens,
    includeFieldTerms: queryBundlesEnabled(config),
    segmentBuilder: createSegmentBuilder(message.segmentsDir, config, { idPrefix: message.segmentIdPrefix || "segment" }),
    pageSize: Math.max(1, Math.floor(Number(config.docPageSize || 32))),
    pageFields: docPayloadFieldNames(config),
    authorityFields: authorityEnabled(config) ? authorityFields(config) : [],
    suggestEnabled: suggestEnabled(config),
    vectorsEnabled: vectorsEnabled(config),
    initialResultLimit: Math.max(0, Math.floor(Number(config.initialResultLimit || 0)))
  };
}

function processBatch({ id, baseIndex, lines }) {
  const { config, avgLens } = state;
  const started = performance.now();
  const n = lines.length;
  const payloadChunks = new Array(n);
  const payloadLengths = new Uint32Array(n);
  const payloadLogicalLengths = new Uint32Array(n);
  const termChunks = new Array(n);
  const termCounts = new Uint32Array(n);
  const layoutChunks = new Array(n);
  const pageColumns = [];
  const pageDocCounts = [];
  const initialPayloads = [];
  const facetsOut = config.facets.map(() => new Array(n));
  const numbersOut = config.numbers.map(() => new Float64Array(n));
  const booleansOut = (config.booleans || []).map(() => new Int8Array(n));
  const authority = { keys: [], docs: [], scores: [] };
  const bundleCandidates = state.includeFieldTerms ? new Array(n) : null;
  // Aggregated per batch so duplicate surfaces (chains, repeated street
  // names) collapse before crossing the worker boundary.
  const suggest = state.suggestEnabled ? new Map() : null;
  // One dense Float32 block per vector field per batch; a NaN in the first
  // component marks a missing vector.
  const vectorsOut = state.vectorsEnabled
    ? config.vectors.map(field => {
        const block = new Float32Array(n * field.dims);
        block.fill(Number.NaN);
        return block;
      })
    : null;
  let pagePayloads = [];

  for (let i = 0; i < n; i++) {
    const index = baseIndex + i;
    const doc = JSON.parse(lines[i]);
    const analysis = analyzeDocumentForIndex(doc, config, avgLens, { includeFieldTerms: state.includeFieldTerms });
    const selectedTerms = analysis.selectedTerms;

    for (const [term, score] of selectedTerms) {
      addSegmentPosting(state.segmentBuilder, term, index, Math.max(1, Math.round(score * 1000)));
    }
    if (shouldFlushSegment(state.segmentBuilder)) flushSegment(state.segmentBuilder);

    termChunks[i] = encodeSelectedTerms(selectedTerms);
    termCounts[i] = selectedTerms.length;
    layoutChunks[i] = Buffer.from(`${JSON.stringify(docLayoutRecord(index, selectedTerms, config))}\n`);

    const payload = docPayload(doc, config, index);
    const payloadBytes = Buffer.from(JSON.stringify(payload));
    const compressed = gzipSync(payloadBytes, { level: 6 });
    payloadChunks[i] = compressed;
    payloadLengths[i] = compressed.length;
    payloadLogicalLengths[i] = payloadBytes.length;
    if (index < state.initialResultLimit) initialPayloads.push(payload);

    pagePayloads.push(payload);
    if (pagePayloads.length >= state.pageSize) {
      pageColumns.push(encodeDocPageColumns(pagePayloads, state.pageFields));
      pageDocCounts.push(pagePayloads.length);
      pagePayloads = [];
    }

    for (let f = 0; f < config.facets.length; f++) {
      facetsOut[f][i] = facetValues(doc, config.facets[f]).map(item => [item.value, item.label]);
    }
    for (let f = 0; f < config.numbers.length; f++) {
      const value = numericValue(doc, config.numbers[f]);
      numbersOut[f][i] = value == null ? Number.NaN : value;
    }
    for (let f = 0; f < (config.booleans || []).length; f++) {
      const value = booleanValue(doc, (config.booleans || [])[f]);
      booleansOut[f][i] = value == null ? -1 : value ? 1 : 0;
    }
    for (const record of authorityRecordsForDoc(state.authorityFields, doc)) {
      authority.keys.push(record.key);
      authority.docs.push(index);
      authority.scores.push(record.score);
    }
    if (bundleCandidates) {
      bundleCandidates[i] = queryBundleSeedCandidatesForDoc(config, selectedTerms, analysis.fieldTerms, doc);
    }
    if (vectorsOut) {
      for (let f = 0; f < config.vectors.length; f++) {
        const field = config.vectors[f];
        const vector = vectorFromValue(rawPath(doc, field.path, null), field.dims);
        const normalized = vector ? normalizeVector(vector) : null;
        if (normalized) vectorsOut[f].set(normalized, i * field.dims);
      }
    }
    if (suggest) {
      for (const [surface, weight, tokenPrefixes] of suggestRowsForDoc(config, doc)) {
        const existing = suggest.get(surface);
        if (existing) {
          existing[0] += 1;
          if (weight > existing[1]) existing[1] = weight;
          if (tokenPrefixes) existing[2] = 1;
        } else {
          suggest.set(surface, [1, weight, tokenPrefixes ? 1 : 0]);
        }
      }
    }
  }
  if (pagePayloads.length) {
    pageColumns.push(encodeDocPageColumns(pagePayloads, state.pageFields));
    pageDocCounts.push(pagePayloads.length);
  }

  return {
    id,
    baseIndex,
    docs: n,
    payloadBytes: Buffer.concat(payloadChunks),
    payloadLengths,
    payloadLogicalLengths,
    termBytes: Buffer.concat(termChunks),
    termCounts,
    layoutBytes: Buffer.concat(layoutChunks),
    pageColumns,
    pageDocCounts,
    initialPayloads,
    facets: facetsOut,
    numbers: numbersOut,
    booleans: booleansOut,
    authority,
    bundleCandidates,
    suggest: suggest
      ? {
          surfaces: [...suggest.keys()],
          rows: [...suggest.values()]
        }
      : null,
    vectors: vectorsOut,
    analysisMs: performance.now() - started
  };
}

function finish(message) {
  const segments = finishSegmentBuilder(state.segmentBuilder);
  return { id: message.id, segments };
}

parentPort.on("message", (message) => {
  try {
    if (message.type === "init") {
      initState(message);
      parentPort.postMessage({ id: message.id });
      return;
    }
    if (message.type === "finish") {
      parentPort.postMessage(finish(message));
      return;
    }
    parentPort.postMessage(processBatch(message));
  } catch (error) {
    parentPort.postMessage({
      id: message.id,
      error: error?.stack || error?.message || String(error)
    });
  }
});
