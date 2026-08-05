// PulseMesh micro-benchmarks: per-operation cost of every hot path in the
// protocol — codec, proof-of-work, validation, store, aggregation,
// digests, snapshots — plus store memory. These are the numbers that
// decide whether a phone can be a peer.
//
//   node scripts/pulsemesh_bench.mjs [--json]
//
// Everything measures the real modules with real wire bytes; no mocks.

import { performance } from "node:perf_hooks";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";
import {
  decodePMB1,
  decodePMC1,
  encodePMB1,
  encodePMC1,
  encodePMD1,
  encodePMQ1,
  encodePMS1,
  minePow,
  verifyPow
} from "../src/pulsemesh/codec.js";
import { sha256, sha256Utf8 } from "../src/pulsemesh/sha256.js";
import { PulseMeshStore } from "../src/pulsemesh/store.js";
import { aggregateSegment } from "../src/pulsemesh/aggregate.js";
import { createValidator } from "../src/pulsemesh/validate.js";
import { shardOfCell } from "../src/pulsemesh/topics.js";

const asJson = process.argv.includes("--json");
const EPOCH32 = sha256Utf8("pulsemesh-bench-epoch");
const PREFIX8 = EPOCH32.slice(0, 8);
const results = {};

function lcg(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function report(section, rows) {
  results[section] = rows;
  if (asJson) return;
  console.log(`\n## ${section}`);
  for (const [label, value] of Object.entries(rows)) console.log(`  ${label.padEnd(46)} ${value}`);
}

function bench(fn, { warmup = 2000, iterations = 20000 } = {}) {
  for (let i = 0; i < warmup; i++) fn(i);
  const start = performance.now();
  for (let i = 0; i < iterations; i++) fn(i);
  const elapsed = performance.now() - start;
  return { opsPerSec: iterations / (elapsed / 1000), usPerOp: (elapsed * 1000) / iterations };
}

const random = lcg(42);
function makeRecordFields(overrides = {}) {
  return {
    epochPrefix8: PREFIX8,
    leafCell: 1 + Math.floor(random() * 4000),
    geomRef: Math.floor(random() * 1000),
    timeBucket: Math.floor(Date.now() / 15000),
    speedBin: Math.floor(random() * 30),
    qualityBin: 1 + Math.floor(random() * 7),
    meters: 50 + Math.floor(random() * 900),
    ttlSeconds: 90,
    reportId: Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256)),
    proofType: 0,
    proof: new Uint8Array(0),
    ...overrides
  };
}

// --- Codec ---------------------------------------------------------------

{
  const fields = makeRecordFields();
  const encoded = encodePMC1(fields);
  const batch = encodePMB1(Array.from({ length: 16 }, () => encodePMC1(makeRecordFields()).bytes));
  const encode = bench(() => encodePMC1(fields));
  const decode = bench(() => decodePMC1(encoded.bytes));
  const batchDecode = bench(() => decodePMB1(batch), { iterations: 5000 });
  report("codec", {
    "PMC1 size (proofType 0 / PoW)": `${encoded.bytes.length} / ${encoded.bytes.length + 8} bytes`,
    "PMB1(16) size": `${batch.length} bytes`,
    "encode PMC1": `${Math.round(encode.opsPerSec).toLocaleString()} ops/s (${encode.usPerOp.toFixed(2)} µs)`,
    "decode PMC1": `${Math.round(decode.opsPerSec).toLocaleString()} ops/s (${decode.usPerOp.toFixed(2)} µs)`,
    "decode PMB1(16)": `${Math.round(batchDecode.opsPerSec).toLocaleString()} batches/s (${(batchDecode.opsPerSec * 16 / 1000).toFixed(0)}k records/s)`
  });
}

// --- SHA-256 and proof of work ------------------------------------------

{
  const message = new Uint8Array(81); // preimage(41) + epoch(32) + nonce(8)
  const hash = bench(() => sha256(message), { iterations: 200000 });
  const hashesPerSec = hash.opsPerSec;

  // Real mining runs at a few difficulties for measured (not extrapolated)
  // cost, then the 2^d model extends to production difficulties.
  const mineRows = {};
  for (const difficulty of [8, 12, 16]) {
    const times = [];
    let iterations = 0;
    for (let i = 0; i < (difficulty >= 16 ? 3 : 10); i++) {
      const fields = makeRecordFields();
      const { preimage } = encodePMC1(fields);
      const start = performance.now();
      const mined = minePow(preimage, EPOCH32, difficulty);
      times.push(performance.now() - start);
      iterations += mined.iterations;
    }
    const mean = times.reduce((a, b) => a + b, 0) / times.length;
    mineRows[`mine at ${difficulty} bits (measured mean)`] = `${mean.toFixed(1)} ms`;
  }
  const verifyBench = bench(
    (() => {
      const fields = makeRecordFields();
      const { preimage } = encodePMC1(fields);
      const nonce = minePow(preimage, EPOCH32, 8).nonce;
      return () => verifyPow(preimage, EPOCH32, nonce, 8);
    })(),
    { iterations: 100000 }
  );
  report("proof-of-work", {
    "sha256 (81-byte message)": `${Math.round(hashesPerSec).toLocaleString()} hashes/s`,
    ...mineRows,
    "projected mine at 20 bits (2^20 hashes)": `${(2 ** 20 / hashesPerSec).toFixed(2)} s`,
    "projected mine at 24 bits (incidents)": `${(2 ** 24 / hashesPerSec).toFixed(1)} s`,
    "verify (any difficulty)": `${Math.round(verifyBench.opsPerSec).toLocaleString()} ops/s (${verifyBench.usPerOp.toFixed(2)} µs)`,
    "attacker/defender cost ratio at 20 bits": `${Math.round(2 ** 20).toLocaleString()}:1 hashes`
  });
}

// --- Validation pipeline -------------------------------------------------

{
  const constants = { ...DEFAULT_CONSTANTS, RATE_BURST: 1e9, RATE_SUSTAINED: 1e9 };
  const cellOf = record => ({ x: 9216 + (record.leafCell % 8), y: 11520 + (record.geomRef % 8) });
  const cellContext = () => ({ polylineCount: 1000, classOf: () => "secondary", metersOf: () => null });
  const validator = createValidator({ constants, epoch32: EPOCH32, cellOf, cellContext, transport: "loopback" });
  const records = Array.from({ length: 2000 }, () => decodePMC1(encodePMC1(makeRecordFields({ speedBin: 10 })).bytes));
  const validate = bench(i => validator.validateContribution(records[i % records.length], { fromPeer: "p" }), { iterations: 100000 });

  // With PoW verification (the wire path).
  const powConstants = { ...constants, POW_DIFFICULTY: 8 };
  const powValidator = createValidator({ constants: powConstants, epoch32: EPOCH32, cellOf, cellContext, transport: "wire" });
  const powRecords = records.slice(0, 200).map(record => {
    const fields = makeRecordFields({ speedBin: 10, proofType: 1 });
    const { preimage } = encodePMC1(fields);
    fields.proof = minePow(preimage, EPOCH32, 8).nonce;
    return decodePMC1(encodePMC1(fields).bytes);
  });
  const validatePow = bench(i => powValidator.validateContribution(powRecords[i % powRecords.length], { fromPeer: "p" }), { iterations: 50000 });
  report("validation", {
    "rules 1–12, no proof (loopback)": `${Math.round(validate.opsPerSec).toLocaleString()} records/s`,
    "rules 1–12 + PoW verify (wire)": `${Math.round(validatePow.opsPerSec).toLocaleString()} records/s`,
    "flood-defense budget (1 core)": `${Math.round(validatePow.opsPerSec).toLocaleString()} hostile records/s rejected at line rate`
  });
}

// --- Store ---------------------------------------------------------------

{
  const cellOf = record => ({ x: 9216 + (record.leafCell % 64), y: 11520 + ((record.leafCell >> 6) % 64) });
  const nowMillis = Date.now();
  const store = new PulseMeshStore({ cellOf });
  const records = Array.from({ length: 100000 }, () => decodePMC1(encodePMC1(makeRecordFields()).bytes));
  const heapBefore = process.memoryUsage().heapUsed;
  const start = performance.now();
  for (const record of records) store.addContribution(record, { nowMillis });
  const insertMs = performance.now() - start;
  const heapAfter = process.memoryUsage().heapUsed;

  const zone = { x: 9216 >> 6, y: 11520 >> 6 };
  const digestBench = bench(() => store.digestForZone(zone, nowMillis), { warmup: 5, iterations: 50 });
  const digestBytes = encodePMD1({ epochPrefix8: PREFIX8, ...store.digestForZone(zone, nowMillis) }).length;
  const cells = Array.from({ length: 32 }, (_, i) => ({ x: 9216 + (i % 64), y: 11520 + (i >> 6) }));
  const snapshotBench = bench(() => store.snapshotForCells(cells), { warmup: 5, iterations: 100 });
  const snapshotBytes = encodePMS1({ epochPrefix8: PREFIX8, cells: store.snapshotForCells(cells) }).length;

  const sweepStart = performance.now();
  store.sweep(nowMillis + 200 * 1000);
  const sweepMs = performance.now() - sweepStart;

  report("store (100k records)", {
    "insert": `${Math.round(records.length / (insertMs / 1000)).toLocaleString()} records/s`,
    "memory": `${((heapAfter - heapBefore) / records.length).toFixed(0)} bytes/record (${((heapAfter - heapBefore) / 1048576).toFixed(1)} MB total)`,
    "zone digest (full zone)": `${digestBench.usPerOp.toFixed(0)} µs, ${digestBytes} bytes on the wire`,
    "32-cell snapshot": `${snapshotBench.usPerOp.toFixed(0)} µs, ${(snapshotBytes / 1024).toFixed(0)} KiB on the wire`,
    "full TTL sweep": `${sweepMs.toFixed(1)} ms`
  });
}

// --- Aggregation ---------------------------------------------------------

{
  const nowMillis = Date.now();
  const bucket = Math.floor(nowMillis / 15000);
  const makeEntries = count => Array.from({ length: count }, (_, i) => ({
    record: {
      reportId: Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256)),
      speedBin: 4 + Math.floor(random() * 6),
      qualityBin: 1 + Math.floor(random() * 7),
      timeBucket: bucket - Math.floor(random() * 4),
      meters: 200
    },
    deliveredBy: `peer-${i % 8}`
  }));
  const rows = {};
  for (const count of [3, 8, 64]) {
    const entries = makeEntries(count);
    const result = bench(() => aggregateSegment(entries, { nowMillis }), { iterations: 50000 });
    rows[`weighted median over n=${count}`] = `${Math.round(result.opsPerSec).toLocaleString()} segments/s (${result.usPerOp.toFixed(2)} µs)`;
  }
  const shard = bench(i => shardOfCell({ x: i, y: i * 3 }), { iterations: 100000 });
  rows["topic shard (sha256 of cell)"] = `${Math.round(shard.opsPerSec).toLocaleString()} ops/s`;
  report("aggregation", rows);
}

if (asJson) console.log(JSON.stringify(results, null, 2));
