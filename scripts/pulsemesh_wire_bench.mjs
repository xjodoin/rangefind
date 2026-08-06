// PulseMesh wire benchmark (milestone M3): the real transport, measured.
//
// The M4 simulation models latency and gossip fan-out; this measures what
// actually happens over js-libp2p sockets — end-to-end record latency
// from publish to a peer's store, sync-stream round trips, snapshot
// throughput, and sustained record ingest — so the modeled numbers in
// pulsemesh-benchmarks.md have a real-socket counterpart.
//
//   node scripts/pulsemesh_wire_bench.mjs [--peers=6] [--records=300] [--json]
//
// Requires the optional libp2p peer dependencies; exits 0 with a notice
// when they are absent, so it is safe in a suite.

import { performance } from "node:perf_hooks";
import { DEFAULT_CONSTANTS, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode } from "../src/pulsemesh/node.js";
import { PROOF_BOND, encodePMC1, encodePMG1, encodePMQ1 } from "../src/pulsemesh/codec.js";
import { sha256Utf8, toHex } from "../src/pulsemesh/sha256.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => {
  const [key, value] = arg.replace(/^--/, "").split("=");
  return [key, value ?? true];
}));
const AS_JSON = Boolean(args.json);
const PEERS = Number(args.peers || 6);
const RECORDS = Number(args.records || 300);

let createPulseMeshHost;
let createLibp2pNetwork;
try {
  ({ createPulseMeshHost, createLibp2pNetwork } = await import("../src/pulsemesh/libp2p.js"));
  await import("libp2p");
} catch {
  console.log("pulsemesh_wire_bench: libp2p optional dependencies not installed — skipping.");
  process.exit(0);
}

const EPOCH_HEX = toHex(sha256Utf8("pulsemesh-wire-bench"));
// 8-bit PoW: mining cost is measured separately in pulsemesh_bench.mjs;
// here it must not dominate the transport measurement.
// BOND_BIRTHDAY_BITS 16 keeps the admission mint at ~1 ms; the bench
// measures the wire, not the puzzle (bench:pulsemesh:bond measures that).
const CONSTANTS = { ...DEFAULT_CONSTANTS, BOND_BIRTHDAY_BITS: 16, RATE_SUSTAINED: 1e6, RATE_BURST: 1e6 };
const CELL = { x: 144 * 64 + 5, y: 180 * 64 + 9 };
const ZONE = zoneOfDetailCell(CELL);
const cellOf = record => (record.leafCell < 64 ? { x: CELL.x + (record.leafCell % 8), y: CELL.y } : null);
const cellContext = leafCell => (leafCell < 64
  ? { polylineCount: 64, classOf: () => "secondary", metersOf: () => 400 }
  : null);

const results = {};
function report(section, rows) {
  results[section] = rows;
  if (AS_JSON) return;
  console.log(`\n## ${section}`);
  for (const [label, value] of Object.entries(rows)) console.log(`  ${label.padEnd(44)} ${value}`);
}

function percentile(values, p) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

async function waitFor(check, { timeoutMs = 30000, stepMs = 50, label = "condition" } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await check()) return;
    if (Date.now() > deadline) throw new Error(`Timed out waiting for ${label}.`);
    await new Promise(resolve => setTimeout(resolve, stepMs));
  }
}

// --- Build the mesh -------------------------------------------------------

const hosts = [];
const networks = [];
const nodes = [];
for (let i = 0; i < PEERS; i++) {
  const host = await createPulseMeshHost();
  const network = createLibp2pNetwork(host);
  const node = new MeshNode({
    id: host.peerId.toString(),
    epochHex: EPOCH_HEX,
    constants: CONSTANTS,
    cellOf,
    cellContext,
    network,
    transport: "wire"
  });
  node.subscribeZones([ZONE]);
  hosts.push(host);
  networks.push(network);
  nodes.push(node);
}
// Full mesh dial so gossip has a real fan-out to work with.
for (let i = 0; i < hosts.length; i++) {
  for (let j = i + 1; j < hosts.length; j++) {
    await hosts[i].dial(hosts[j].getMultiaddrs()[0]);
  }
}
const publisher = nodes[0];
await waitFor(
  () => [...publisher.subscribedTopics].some(topic => hosts[0].services.pubsub.getSubscribers(topic).length >= PEERS - 1),
  { label: "gossip mesh formation" }
);

// --- §5.4 admission: every peer presents its bond before publishing ------

for (const network of networks) {
  if (!(await network.mintBond())) throw new Error("bond mint failed");
}
await waitFor(
  () => nodes.every(node => node.stats.bondsAccepted >= PEERS - 1),
  { label: "bonds exchanged across the mesh" }
);

// --- Prepare records (proofless — the bond carries the session) ----------

const prepared = [];
for (let i = 0; i < RECORDS; i++) {
  const reportId = new Uint8Array(16);
  globalThis.crypto.getRandomValues(reportId);
  const fields = {
    epochPrefix8: publisher.epochPrefix8,
    leafCell: i % 8,
    geomRef: (i % 24) * 2,
    timeBucket: Math.floor(Date.now() / 15000),
    speedBin: 4 + (i % 6),
    qualityBin: 7,
    meters: 400,
    ttlSeconds: 90,
    reportId,
    proofType: PROOF_BOND,
    proof: new Uint8Array(0)
  };
  prepared.push(encodePMC1(fields));
}

// --- 1. End-to-end record latency: publish -> in a peer's store ----------

{
  const observer = nodes[1];
  const latencies = [];
  for (let i = 0; i < Math.min(120, prepared.length); i++) {
    const record = prepared[i];
    const idHex = toHex(record.bytes.subarray(record.bytes.length - 26, record.bytes.length - 10));
    const started = performance.now();
    publisher.publishRecord(record);
    try {
      await waitFor(() => observer.store.size() > i, { timeoutMs: 5000, stepMs: 1, label: "delivery" });
      latencies.push(performance.now() - started);
    } catch {
      break;
    }
    void idHex;
  }
  report("gossip delivery (publish → remote store, full validation)", {
    "samples": latencies.length,
    "p50": `${percentile(latencies, 50)?.toFixed(2)} ms`,
    "p95": `${percentile(latencies, 95)?.toFixed(2)} ms`,
    "max": `${Math.max(...latencies).toFixed(2)} ms`,
    "note": `${PEERS}-peer full mesh on loopback; includes PoW verify and rules 1–12`
  });
}

// --- 2. Sustained ingest: how fast a peer absorbs a burst ----------------

{
  const before = nodes[1].store.size();
  const remaining = prepared.slice(120);
  const started = performance.now();
  for (const record of remaining) publisher.publishRecord(record);
  await waitFor(() => nodes[1].store.size() >= before + remaining.length, {
    timeoutMs: 60000,
    label: `burst ingest (${nodes[1].store.size() - before}/${remaining.length})`
  });
  const elapsed = performance.now() - started;
  const fanoutRecords = remaining.length * (PEERS - 1);
  report("sustained ingest", {
    "records published": remaining.length,
    "wall clock to full delivery": `${elapsed.toFixed(0)} ms`,
    "publish→deliver rate": `${Math.round(remaining.length / (elapsed / 1000)).toLocaleString()} records/s`,
    "aggregate mesh deliveries": `${Math.round(fanoutRecords / (elapsed / 1000)).toLocaleString()} record-deliveries/s across ${PEERS - 1} peers`
  });
}

// --- 3. Sync stream round trips ------------------------------------------

{
  const client = nodes[2];
  const serverId = nodes[1].id;
  const digestLatencies = [];
  for (let i = 0; i < 50; i++) {
    const started = performance.now();
    const response = await networks[2].request(
      client.id,
      serverId,
      encodePMG1({ epochPrefix8: client.epochPrefix8, zoneX: ZONE.x, zoneY: ZONE.y })
    );
    if (!response) break;
    digestLatencies.push(performance.now() - started);
  }

  const cells = Array.from({ length: 32 }, (_, i) => ({ x: CELL.x + (i % 8), y: CELL.y }));
  const snapshotLatencies = [];
  let snapshotBytes = 0;
  for (let i = 0; i < 30; i++) {
    const started = performance.now();
    const response = await networks[2].request(
      client.id,
      serverId,
      encodePMQ1({ epochPrefix8: client.epochPrefix8, cells })
    );
    if (!response) break;
    snapshotBytes = response.length;
    snapshotLatencies.push(performance.now() - started);
  }
  report("sync streams (/rangefind/pulsemesh/1/sync)", {
    "PMG1 digest round trip p50": `${percentile(digestLatencies, 50)?.toFixed(2)} ms`,
    "PMG1 digest round trip p95": `${percentile(digestLatencies, 95)?.toFixed(2)} ms`,
    "PMQ1 32-cell snapshot p50": `${percentile(snapshotLatencies, 50)?.toFixed(2)} ms`,
    "PMQ1 32-cell snapshot p95": `${percentile(snapshotLatencies, 95)?.toFixed(2)} ms`,
    "snapshot response size": `${(snapshotBytes / 1024).toFixed(1)} KiB`,
    "streams per second (sequential)": `${Math.round(1000 / (percentile(digestLatencies, 50) || 1)).toLocaleString()}`
  });
}

// --- 4. Cold-start recovery: a fresh peer pulls the whole zone -----------

{
  const host = await createPulseMeshHost();
  const network = createLibp2pNetwork(host);
  const late = new MeshNode({
    id: host.peerId.toString(),
    epochHex: EPOCH_HEX,
    constants: CONSTANTS,
    cellOf,
    cellContext,
    network,
    transport: "wire"
  });
  hosts.push(host);
  networks.push(network);
  await host.dial(hosts[1].getMultiaddrs()[0]);
  late.subscribeZones([ZONE]);
  const target = nodes[1].store.size();
  const started = performance.now();
  let rounds = 0;
  while (late.store.size() < target && rounds < 40) {
    await late.antiEntropyWith(nodes[1].id, ZONE);
    rounds++;
  }
  const elapsed = performance.now() - started;
  report("cold-start recovery (anti-entropy over the wire)", {
    "records to recover": target,
    "recovered": late.store.size(),
    "anti-entropy rounds": rounds,
    "wall clock": `${elapsed.toFixed(0)} ms`,
    "cells requested / wanted": `${late.stats.cellsRequested} / ${late.stats.cellsWanted} (padding overhead ${(late.stats.cellsRequested / Math.max(1, late.stats.cellsWanted)).toFixed(2)}×)`
  });
}

// --- 5. Wire byte accounting ---------------------------------------------

{
  const publisherStats = networks[0].stats;
  const receiverStats = networks[1].stats;
  report("bytes on the wire", {
    "publisher gossip out": `${(publisherStats.gossipOut / 1024).toFixed(1)} KiB for ${prepared.length} records`,
    "per record published": `${Math.round(publisherStats.gossipOut / prepared.length)} bytes`,
    "receiver gossip in": `${(receiverStats.gossipIn / 1024).toFixed(1)} KiB`,
    "sync requests served by peer 1": receiverStats.served
  });
}

for (const network of networks) await network.close();
await Promise.all(hosts.map(host => host.stop().catch(() => {})));

if (AS_JSON) console.log(JSON.stringify(results, null, 2));
