import { mkdirSync } from "node:fs";
import { parentPort } from "node:worker_threads";
import { performance } from "node:perf_hooks";
import { gzipSync } from "node:zlib";
import { openCodeStore } from "./build_store.js";
import { buildPostingSegment, POSTING_SEGMENT_FORMAT } from "./codec.js";
import { createAppendOnlyPackWriter, finalizePackWriter, resolvePackEntry, writePackedShard } from "./packs.js";
import { postingRowCount } from "./posting_rows.js";

let activeCodes = null;
let activeCodesKey = "";
let activeTermWriter = null;
let activeBlockWriter = null;

function sharedCounter(buffer) {
  return buffer ? new Int32Array(buffer) : null;
}

function closeActiveCodes() {
  activeCodes?.close?.();
  activeCodes = null;
  activeCodesKey = "";
}

function descriptorKey(descriptor) {
  return JSON.stringify(descriptor || null);
}

function codeStoreForDescriptor(descriptor) {
  const key = descriptorKey(descriptor);
  if (activeCodes && activeCodesKey === key) return activeCodes;
  closeActiveCodes();
  activeCodes = openCodeStore(descriptor);
  activeCodesKey = key;
  return activeCodes;
}

function termWriterFor(outDir, targetBytes, indexCounter) {
  mkdirSync(outDir, { recursive: true });
  if (!activeTermWriter) activeTermWriter = createAppendOnlyPackWriter(outDir, targetBytes, { indexCounter });
  return activeTermWriter;
}

function blockWriterFor(config, outDir, targetBytes, indexCounter) {
  if (config.externalPostingBlocks === false) return null;
  mkdirSync(outDir, { recursive: true });
  if (!activeBlockWriter) activeBlockWriter = createAppendOnlyPackWriter(outDir, targetBytes, { indexCounter });
  return activeBlockWriter;
}

function writeBlockWith(writer) {
  if (!writer) return null;
  return ({ term, blockIndex, bytes }) => {
    const key = `${term}\u0000${blockIndex}\u0000${writer.bytes}`;
    return writePackedShard(writer, key, gzipSync(bytes, { level: 6 }), {
      kind: "posting-segment-block",
      codec: "rfsegpost-block-v1",
      logicalLength: bytes.length
    });
  };
}

function finishActiveWriters(id) {
  const started = performance.now();
  if (activeBlockWriter) finalizePackWriter(activeBlockWriter);
  if (activeTermWriter) finalizePackWriter(activeTermWriter);
  closeActiveCodes();
  return {
    id,
    packs: activeTermWriter?.packs || [],
    packBytes: activeTermWriter?.bytes || 0,
    blockPacks: activeBlockWriter?.packs || [],
    blockPackBytes: activeBlockWriter?.bytes || 0,
    ms: performance.now() - started
  };
}

async function reducePartition(message) {
  const started = performance.now();
  const {
    blockOutDir,
    blockPackCounter,
    blockTargetBytes,
    config,
    codesDescriptor,
    filters,
    id,
    partition,
    targetBytes,
    termPackCounter,
    termsOutDir,
    total
  } = message;
  const codes = codeStoreForDescriptor(codesDescriptor);
  const packWriter = termWriterFor(termsOutDir, targetBytes, sharedCounter(termPackCounter));
  const blockWriter = blockWriterFor(config, blockOutDir, blockTargetBytes, sharedCounter(blockPackCounter));
  const encoded = buildPostingSegment(partition.entries, total, codes, filters, config, writeBlockWith(blockWriter));
  const entry = writePackedShard(packWriter, partition.name, gzipSync(encoded.buffer, { level: 6 }), {
    kind: "posting-segment",
    codec: encoded.format || POSTING_SEGMENT_FORMAT,
    logicalLength: encoded.buffer.length
  });
  return {
    id,
    name: partition.name,
    entry: resolvePackEntry(packWriter, entry),
    stats: encoded.stats,
    rows: partition.entries.reduce((sum, [, rows]) => sum + postingRowCount(rows), 0),
    terms: partition.entries.length,
    ms: performance.now() - started
  };
}

parentPort.on("message", async (message) => {
  try {
    parentPort.postMessage(message.kind === "finish"
      ? finishActiveWriters(message.id)
      : await reducePartition(message));
  } catch (error) {
    parentPort.postMessage({
      id: message.id,
      error: error?.stack || error?.message || String(error)
    });
  }
});

process.once("exit", closeActiveCodes);
