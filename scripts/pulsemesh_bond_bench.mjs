// PulseMesh identity-bond benchmark (§5.4): the cost of admission, the
// cost of checking it, and what a farm can parallelise.
//
//   node scripts/pulsemesh_bond_bench.mjs [--json] [--workers=N]
//
// Three questions, measured rather than argued:
//
//   Q1 Mint and verify by table size. The design leans on one measured
//      property: verification is three hashes at ANY table size, so the
//      miner's memory burden never becomes the verifier's.
//
//   Q2 Farming. Memory-hardness failed at cache-sized tables (the
//      per-record drafts) because a table that fits in cache scales
//      across threads like plain hashing. At bond sizes the table is the
//      constraint: measure thread scale-up small vs large.
//
//   Q3 Amortization. What a contributor pays per day against what the
//      per-record drafts charged per trip.
//
// History: earlier drafts used per-record SHA-256 proof-of-work; the
// measurements that killed it (blocking, farm parallelism, and the
// record-cadence refutation of memory-hardness that §14.5 inverts) are
// recorded in docs/pulsemesh-benchmarks.md §14.

import { performance } from "node:perf_hooks";
import { availableParallelism } from "node:os";
import { Worker, isMainThread, parentPort, workerData } from "node:worker_threads";
import { fileURLToPath } from "node:url";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";
import { bondSeed, solveBondProof, verifyBondProof } from "../src/pulsemesh/bond.js";
import { sha256, sha256Utf8 } from "../src/pulsemesh/sha256.js";

const SELF = fileURLToPath(import.meta.url);

// --- worker: mint continuously for durationMs, report completed mints ----

if (!isMainThread && workerData?.role === "farm") {
  const { birthdayBits, durationMs, index } = workerData;
  const epoch32 = sha256Utf8(`bond-farm-${birthdayBits}`);
  const deadline = performance.now() + durationMs;
  let solved = 0;
  let bucket = 0;
  (async () => {
    while (performance.now() < deadline) {
      const seed = bondSeed(epoch32, bucket++, 0, `farm-${index}`);
      if (await solveBondProof(seed, birthdayBits, 0, { chunkMillis: 1000 })) solved++;
    }
    parentPort.postMessage({ solved });
  })();
}

// --- main ------------------------------------------------------------------

if (isMainThread) {
  const asJson = process.argv.includes("--json");
  const workerArg = process.argv.find(a => a.startsWith("--workers="));
  const maxWorkers = workerArg ? Number(workerArg.split("=")[1]) : Math.min(8, availableParallelism());
  const EPOCH32 = sha256Utf8("pulsemesh-bond-bench-epoch");
  const GIB = 1 << 30;
  const results = {};

  function report(section, rows) {
    results[section] = rows;
    if (asJson) return;
    console.log(`\n## ${section}`);
    for (const [label, value] of Object.entries(rows)) console.log(`  ${label.padEnd(44)} ${value}`);
  }

  const fmtBytes = b => (b >= GIB ? `${(b / GIB).toFixed(2)} GiB` : `${(b / (1 << 20)).toFixed(0)} MiB`);
  const fmtSec = ms => (ms >= 1000 ? `${(ms / 1000).toFixed(1)} s` : `${ms.toFixed(1)} ms`);

  // --- baseline ------------------------------------------------------------
  const probe = sha256Utf8("rate-probe");
  const RATE_ITERS = 200000;
  const rateStart = performance.now();
  for (let i = 0; i < RATE_ITERS; i++) sha256(probe);
  const hashesPerSec = RATE_ITERS / ((performance.now() - rateStart) / 1000);
  report("baseline", {
    "sha256 throughput (this machine, 1 core)": `${(hashesPerSec / 1e6).toFixed(2)} M hash/s`,
    "BOND_BIRTHDAY_BITS (default)": DEFAULT_CONSTANTS.BOND_BIRTHDAY_BITS,
    "BOND_LIFETIME": `${DEFAULT_CONSTANTS.BOND_LIFETIME} s (one mint per day)`
  });

  // --- Q1: mint and verify by table size ----------------------------------
  const mintRows = {};
  for (const B of [24, 32, 40, 44, 48]) {
    const runs = B >= 48 ? 2 : B >= 44 ? 3 : 5;
    let total = 0;
    let table = 0;
    let solution = null;
    let seed = null;
    for (let run = 0; run < runs; run++) {
      seed = bondSeed(EPOCH32, 20_000 + run, 0, `bench-${B}`);
      const start = performance.now();
      const solved = await solveBondProof(seed, B, 0, { chunkMillis: 100 });
      total += performance.now() - start;
      if (solved) { table = solved.tableBytes; solution = solved; }
    }
    let verifyUs = 0;
    if (solution) {
      const VERIFY_ITERS = 20000;
      const vStart = performance.now();
      for (let i = 0; i < VERIFY_ITERS; i++) verifyBondProof(seed, solution.i, solution.j, B, 0);
      verifyUs = ((performance.now() - vStart) * 1000) / VERIFY_ITERS;
    }
    const mean = total / runs;
    mintRows[`B=${B}${B === DEFAULT_CONSTANTS.BOND_BIRTHDAY_BITS ? " (default)" : ""}`] =
      `mint ${fmtSec(mean).padStart(8)} (phone ~${fmtSec(mean * 4)})  table ${fmtBytes(table).padStart(9)}  ` +
      `verify ${verifyUs.toFixed(2)} us  solvers/24GiB GPU: ${Math.floor((24 * GIB) / table)}`;
  }
  report("Q1 mint and verify by table size", mintRows);

  // --- Q2: farming scale-up, cache-sized vs bond-sized tables --------------
  async function farm(birthdayBits, workers, durationMs) {
    const jobs = Array.from({ length: workers }, (_, index) => new Promise((resolve, reject) => {
      const worker = new Worker(SELF, { workerData: { role: "farm", birthdayBits, durationMs, index } });
      worker.once("message", msg => { resolve(msg.solved); worker.terminate(); });
      worker.once("error", reject);
    }));
    const solved = await Promise.all(jobs);
    return solved.reduce((a, b) => a + b, 0) / (durationMs / 1000);
  }
  const scaleRows = {};
  for (const B of [28, 40]) {
    let base = 0;
    const parts = [];
    for (const workers of [1, maxWorkers]) {
      const rate = await farm(B, workers, 3000);
      if (workers === 1) base = rate;
      parts.push(`${workers}w ${rate.toFixed(1)}/s (${(rate / base).toFixed(2)}x)`);
    }
    scaleRows[`B=${B} (${fmtBytes(B === 28 ? 1 << 20 : 64 << 20)} table)`] = parts.join("   ");
  }
  // Honest cap: on a dev box whose RAM holds all eight tables, scale-up
  // stays linear — the constraint the design leans on is CAPACITY
  // (solvers <= RAM / table), which binds a 24 GiB GPU at 96 slots for
  // 256 MiB tables, not memory bandwidth on this machine.
  scaleRows["reading"] = "linear here because all tables fit this machine's RAM; the attacker's bound is slots = RAM/table";
  report(`Q2 farming scale-up (${maxWorkers} workers max)`, scaleRows);

  // --- Q3: amortization -----------------------------------------------------
  report("Q3 amortization: the admission economy", {
    "per contribution / per incident": "0 — records carry no proof (proofType 3)",
    "record on the wire": "42 B, no nonce",
    "verification per record": "none — one ~1 us bond check per session",
    "contributor pays": "one background mint per BOND_LIFETIME, ideally while charging",
    "attacker pays": "one mint per identity per day, re-paid at each receiver that forfeits it first-hand (§8.4); ~66k mints/core-day means this is a toll, not a wall",
    "historical (per-record PoW drafts)": "319 ms/record, 5.1 s/incident desktop; ~4x phone — see benchmarks §14"
  });

  if (asJson) console.log(JSON.stringify(results, null, 2));
}
