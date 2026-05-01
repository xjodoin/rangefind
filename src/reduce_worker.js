import { mkdirSync, unlinkSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { parentPort, workerData } from "node:worker_threads";
import { gzipSync } from "node:zlib";
import { buildTermShard } from "./codec.js";
import { createTypoRunBuffer, addTypoIndexTerm, flushTypoBuffer } from "./typo.js";
import { readRunRecords } from "./runs.js";
import { partitionEntries } from "./shards.js";

const {
  config,
  codes,
  filters,
  measuredTotal,
  runsOut,
  shardOut,
  typoOptions,
  typoRunsOut
} = workerData;

mkdirSync(shardOut, { recursive: true });
const typoBuffer = typoOptions?.enabled ? createTypoRunBuffer(typoRunsOut, typoOptions) : null;

async function reduceBaseShard(baseShard) {
  const path = resolve(runsOut, `${baseShard}.run`);
  const byTerm = new Map();
  for await (const [term, doc, score] of readRunRecords(path, ["string", "number", "number"])) {
    if (!term) continue;
    if (!byTerm.has(term)) byTerm.set(term, new Map());
    const rows = byTerm.get(term);
    rows.set(doc, (rows.get(doc) || 0) + score);
  }

  const entries = [...byTerm.entries()].map(([term, rows]) => [term, [...rows.entries()]]);
  const partitions = partitionEntries(entries, config);
  const shards = [];
  let postings = 0;
  let sequence = 0;
  for (const [term, rows] of byTerm) {
    postings += rows.size;
    addTypoIndexTerm(typoBuffer, term, rows.size, measuredTotal);
  }
  for (const partition of partitions) {
    const encoded = buildTermShard(partition.entries, measuredTotal, codes, filters, config);
    const compressed = gzipSync(encoded, { level: 6 });
    const file = `${encodeURIComponent(partition.name)}.bin`;
    const out = resolve(shardOut, file);
    writeFileSync(out, compressed);
    shards.push({ shard: partition.name, path: out, length: compressed.length, sequence: sequence++ });
  }
  unlinkSync(path);
  return { terms: byTerm.size, postings, shards };
}

async function finish() {
  flushTypoBuffer(typoBuffer);
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
