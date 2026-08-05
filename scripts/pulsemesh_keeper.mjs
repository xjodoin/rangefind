// PulseMesh keeper node (protocol §12): a headless peer pinned to
// configured zones — same store, same validation, same TTLs, no
// contributions, more connections, and prompt PMG1/PMQ1 answers. Keepers
// add availability, never authority: their records carry the same proofs
// and get the same trust treatment as anyone's.
//
//   node scripts/pulsemesh_keeper.mjs --epoch=<64 hex> --graph=<route-graph dir>
//        [--listen=/ip4/0.0.0.0/tcp/4001] [--zones=x/y,x/y] [--store-cap=1048576]
//        [--bootstrap=<multiaddr>,...] [--pow=20] [--stats-seconds=30]
//
// For harness/tests: --test-world replaces --graph with a synthetic
// 64-leaf single-zone world (the same one the wire tests use).
//
// Emits JSON lines on stdout: {"event":"listening",...} once up, then
// periodic {"event":"stats",...}. SIGINT/SIGTERM stop cleanly.

import { DEFAULT_CONSTANTS, detailCellForE7, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode } from "../src/pulsemesh/node.js";
import { createLibp2pNetwork, createPulseMeshHost } from "../src/pulsemesh/libp2p.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => {
  const [key, value] = arg.replace(/^--/, "").split("=");
  return [key, value ?? true];
}));

function fail(message) {
  console.error(`pulsemesh-keeper: ${message}`);
  process.exit(1);
}

const epochHex = String(args.epoch || "");
if (!/^[0-9a-f]{64}$/.test(epochHex)) fail("--epoch must be the route graph's 64-hex sourceHash");

// --- Static world: leaf -> z15 cell -------------------------------------

let cellOf;
let cellContext = null;
let defaultZones = [];
if (args["test-world"]) {
  const BASE = { x: 144 * 64 + 5, y: 180 * 64 + 9 };
  cellOf = record => (record.leafCell < 64 ? { x: BASE.x + (record.leafCell % 8), y: BASE.y } : null);
  cellContext = leafCell => (leafCell < 64
    ? { polylineCount: 64, classOf: () => "secondary", metersOf: () => 400 }
    : null);
  defaultZones = [zoneOfDetailCell(BASE)];
} else if (args.graph) {
  const { openRouteGraphDir } = await import("../src/route_graph_node.js");
  const engine = await openRouteGraphDir(String(args.graph));
  if (engine.root.sourceHash !== epochHex) {
    fail(`graph epoch ${engine.root.sourceHash.slice(0, 16)}… does not match --epoch ${epochHex.slice(0, 16)}…`);
  }
  cellOf = record => {
    const bbox = engine.root.leaves[record.leafCell]?.bbox;
    if (!bbox) return null;
    return detailCellForE7((bbox.minLat + bbox.maxLat) / 2, (bbox.minLon + bbox.maxLon) / 2);
  };
  const zoneSet = new Map();
  for (let leaf = 0; leaf < engine.root.leaves.length; leaf++) {
    const cell = cellOf({ leafCell: leaf });
    if (!cell) continue;
    const zone = zoneOfDetailCell(cell);
    zoneSet.set(`${zone.x}/${zone.y}`, zone);
  }
  defaultZones = [...zoneSet.values()];
} else {
  fail("one of --graph=<dir> or --test-world is required (the keeper needs the shared static index)");
}

const zones = args.zones
  ? String(args.zones).split(",").map(part => {
      const [x, y] = part.split("/").map(Number);
      if (!Number.isInteger(x) || !Number.isInteger(y)) fail(`bad zone ${part}`);
      return { x, y };
    })
  : defaultZones;
if (!zones.length) fail("no zones to pin");

// --- Constants: keeper profile -------------------------------------------

const constants = {
  ...DEFAULT_CONSTANTS,
  STORE_CONTRIB_CAP: Number(args["store-cap"] || 1048576),
  ...(args.pow ? { POW_DIFFICULTY: Number(args.pow) } : {})
};

// --- Host + node ----------------------------------------------------------

const host = await createPulseMeshHost({
  listen: [String(args.listen || "/ip4/127.0.0.1/tcp/0")],
  bootstrapPeers: args.bootstrap ? String(args.bootstrap).split(",") : []
});
const network = createLibp2pNetwork(host);
const node = new MeshNode({
  id: host.peerId.toString(),
  epochHex,
  constants,
  cellOf,
  cellContext,
  network,
  transport: "wire",
  keeper: true
});
node.subscribeZones(zones);

// The sync protocol registers asynchronously. Announce only once it is
// actually handled: "listening" is the signal an orchestrator dials on,
// and a peer that dials a moment too early gets its first PMG1 rejected.
await network.ready;
if (network.registrationError) {
  fail(`could not register ${"/rangefind/pulsemesh/1/sync"}: ${network.registrationError.message}`);
}

console.log(JSON.stringify({
  event: "listening",
  peerId: host.peerId.toString(),
  multiaddrs: host.getMultiaddrs().map(String),
  zones: zones.map(zone => `${zone.x}/${zone.y}`),
  epoch: epochHex.slice(0, 16),
  storeCap: constants.STORE_CONTRIB_CAP,
  powDifficulty: constants.POW_DIFFICULTY
}));

// Keepers never contribute; they sweep, refresh subscriptions across
// topic rotations, and run anti-entropy like any consumer.
const tickTimer = setInterval(() => {
  node.tick().catch(() => {});
}, constants.ANTI_ENTROPY_SECONDS * 1000);

const statsSeconds = Number(args["stats-seconds"] || 30);
const statsTimer = setInterval(() => {
  console.log(JSON.stringify({
    event: "stats",
    records: node.store.size(),
    incidents: node.store.incidentCount(),
    peers: host.getPeers().length,
    gossipAccepted: node.stats.gossipAccepted,
    gossipDropped: node.stats.gossipDropped,
    served: network.stats.served,
    storeStats: node.store.stats
  }));
}, statsSeconds * 1000);
if (typeof tickTimer.unref === "function") tickTimer.unref();
if (typeof statsTimer.unref === "function") statsTimer.unref();

let stopping = false;
async function stop(signal) {
  if (stopping) return;
  stopping = true;
  console.log(JSON.stringify({ event: "stopping", signal }));
  clearInterval(tickTimer);
  clearInterval(statsTimer);
  clearInterval(keepAlive);
  await network.close();
  await host.stop().catch(() => {});
  // Not process.exit(): stdout writes to a pipe are asynchronous, and
  // exiting would truncate the JSON line a supervisor is parsing. With
  // every timer cleared and the host stopped, the loop drains on its own.
  process.exitCode = 0;
}
process.on("SIGINT", () => stop("SIGINT"));
process.on("SIGTERM", () => stop("SIGTERM"));

// Keep the event loop alive: libp2p's listeners already do, but this
// makes it explicit and gives stop() one handle to release.
const keepAlive = setInterval(() => {}, 1 << 30);
