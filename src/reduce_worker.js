import { mkdirSync, unlinkSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { parentPort, workerData } from "node:worker_threads";
import { gzipSync } from "node:zlib";
import { openCodeStore } from "./build_store.js";
import { buildTermShard } from "./codec.js";
import { createTypoRunBuffer, addTypoIndexTerm, flushTypoBuffer } from "./typo.js";
import { reduceRunToPartitions } from "./reduce_stream.js";

const {
  config,
  codes: codeDescriptor,
  filters,
  measuredTotal,
  runsOut,
  shardOut,
  sortOut,
  bundleTerms,
  typoOptions,
  typoRunsOut
} = workerData;

mkdirSync(shardOut, { recursive: true });
const codes = openCodeStore(codeDescriptor);
const typoBuffer = typoOptions?.enabled ? createTypoRunBuffer(typoRunsOut, typoOptions) : null;
const bundleTermSet = new Set(bundleTerms || []);

async function reduceBaseShard(baseShard) {
  const path = resolve(runsOut, `${baseShard}.run`);
  const bundleDfs = [];
  const stats = await reduceRunToPartitions({
    runPath: path,
    scratchDir: resolve(sortOut, encodeURIComponent(baseShard)),
    config,
    onTerm: (term, df) => {
      addTypoIndexTerm(typoBuffer, term, df, measuredTotal);
      if (bundleTermSet.has(term)) bundleDfs.push([term, df]);
    },
    onPartition: (partition, sequence) => {
      const encoded = buildTermShard(partition.entries, measuredTotal, codes, filters, config);
      const compressed = gzipSync(encoded, { level: 6 });
      const file = `${encodeURIComponent(partition.name)}.bin`;
      const out = resolve(shardOut, file);
      writeFileSync(out, compressed);
      return { shard: partition.name, path: out, length: compressed.length, sequence };
    }
  });
  unlinkSync(path);
  return { terms: stats.terms, postings: stats.postings, shards: stats.partitions, bundleDfs };
}

async function finish() {
  flushTypoBuffer(typoBuffer);
  codes?.close?.();
  return {
    typo: typoBuffer ? {
      runsOut: typoRunsOut,
      terms: typoBuffer.terms,
      deletePairs: typoBuffer.deletePairs,
      shards: [...typoBuffer.shards].sort()
    } : null
  };
}

parentPort.on("message", async (message) => {
  try {
    const value = message.type === "finish"
      ? await finish()
      : await reduceBaseShard(message.baseShard);
    parentPort.postMessage({ id: message.id, ok: true, value });
  } catch (error) {
    parentPort.postMessage({
      id: message.id,
      ok: false,
      error: error?.stack || error?.message || String(error)
    });
  }
});
