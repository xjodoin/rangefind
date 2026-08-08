// PulseMesh keeper node (protocol §12): a headless peer pinned to
// configured zones — same store, same validation, same TTLs, no
// contributions, more connections, and prompt PMG1/PMQ1 answers. Keepers
// add availability, never authority: their records carry the same proofs
// and get the same trust treatment as anyone's.
//
//   node scripts/pulsemesh_keeper.mjs --epoch=<64 hex> --graph=<route-graph dir>
//        [--listen=/ip4/0.0.0.0/tcp/4001] [--zones=x/y,x/y] [--store-cap=1048576]
//        [--bootstrap=<multiaddr>,...] [--stats-seconds=30]
//        [--seed-card] [--seed-label="Depot seed"]
//        [--admit=connected|<peerId>,...] [--bridge=off|in|both]
//
// For harness/tests: --test-world replaces --graph with a synthetic
// 64-leaf single-zone world (the same one the wire tests use).
//
// **--seed-card** is the fleet-seed mode (threads §20.10): this keeper is
// the one dialable peer a fleet's own devices bootstrap from, and the
// card is how its address gets to them. It prints the dialable
// multiaddrs, a `wayfind://seed#…` link and that link as a scannable QR
// block, **on stderr** — see printSeedCard.
//
// **--admit and --bridge** are the fleet-seed pair (§12.1). `--admit`
// decides whether this seed vouches for its own bond-less devices;
// `--bridge` decides what crosses between their island and the wider
// mesh. They are independent axes and both default to off, because a
// plain §12 keeper on a public mesh must hand its bond to nobody.
//
// **Which addresses mean what.** With bridging on there are two sides and
// they are not symmetric:
//
//   --bootstrap  the WIDER NETWORK. Dialed by this process's upstream
//                host — other keepers, the public mesh, whatever this
//                fleet is contributing to and drawing from.
//   --listen     MY ISLAND. The address a fleet's own devices dial, and
//                the one --seed-card prints. This process never dials a
//                driver; drivers arrive here, carrying an address that
//                reached them inside a sealed ticket.
//
// With --bridge=off there is one host and one mesh, so --bootstrap keeps
// its plain meaning: peers this keeper joins. That is the correct setup
// for an ordinary keeper and the wrong one for a fleet seed that has
// admitted bond-less devices — see the refusal below.
//
// Emits JSON lines on stdout: {"event":"listening",...} once up, then
// periodic {"event":"stats",...}. SIGINT/SIGTERM stop cleanly.

import { DEFAULT_CONSTANTS, detailCellForE7, zoneOfDetailCell } from "../src/pulsemesh/bins.js";
import { MeshNode } from "../src/pulsemesh/node.js";
import { createLibp2pNetwork, createPulseMeshHost } from "../src/pulsemesh/libp2p.js";
import { ADMIT_CONNECTED, BRIDGE_POLICIES, createFleetBridge } from "../src/pulsemesh/bridge.js";
import { THREAD_MAX_BOOTSTRAP_ADDRESSES, THREAD_MAX_BOOTSTRAP_BYTES } from "../src/pulsemesh/thread_codec.js";
import { encodeSeedCard, isDialableAddress, seedCardUrl } from "../src/pulsemesh/thread_seed.js";
import { encodeQr, qrTerminal } from "../src/qr.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => {
  const [key, value] = arg.replace(/^--/, "").split("=");
  return [key, value ?? true];
}));

function fail(message) {
  console.error(`pulsemesh-keeper: ${message}`);
  process.exit(1);
}

const USAGE = `pulsemesh-keeper — a headless PulseMesh peer (§12), optionally a fleet's own seed (§12.1)

  node scripts/pulsemesh_keeper.mjs --epoch=<64 hex> --graph=<route-graph dir> [options]

Required
  --epoch=<64 hex>        the route graph's sourceHash; must match --graph
  --graph=<dir>           the static route graph this keeper validates against
  --test-world            harness substitute for --graph: a synthetic 64-leaf world

Mesh
  --listen=<multiaddr>    where THIS host listens (default /ip4/127.0.0.1/tcp/0).
                          With --bridge on, this is MY ISLAND: the address a
                          fleet's own devices dial, and what --seed-card prints.
  --bootstrap=<ma>,...    peers to dial. With --bridge on, this is THE WIDER
                          NETWORK, dialled by a second, listen-less host — never
                          by the island side. With --bridge=off it is the plain
                          keeper meaning: peers this one host joins.
  --zones=x/y,...         z9 zones to pin (default: every zone in the graph)
  --store-cap=<n>         STORE_CONTRIB_CAP (default 1048576)
  --stats-seconds=<n>     stats line period (default 30)

Fleet seed (§12.1)
  --seed-card             print dialable addresses, a wayfind://seed# link and a
                          QR block on stderr (stdout stays JSON lines)
  --seed-label=<text>     human label inside the card (≤32 bytes)
  --admit=connected       vouch for every directly connected peer: reaching this
                          seed required its address, and that address travels
                          only inside sealed tickets. This is what lets a fleet's
                          phones contribute WITHOUT minting a 256 MiB bond.
  --admit=<peerId>,...    the stricter variant: an explicit allowlist
  --bridge=off|in|both    off  (default) an island: nothing crosses
                          in   receive only — nothing this fleet publishes leaves
                               the seed; upstream is a §11.6 read-only node while
                               this side stays a normal bonded peer downward
                          both validated island records are republished upstream
                               UNDER THIS SEED'S OWN BOND, and upstream records
                               are gossiped down to the island

A bridged seed stakes its bond on its drivers: publish junk and remote peers
forfeit the seed, and the whole fleet loses reach. Bridge with --zones pinned to
the territory the fleet actually works; an unzoned bridge on a planet-scale mesh
pulls the planet through a depot's uplink.
`;
if (args.help || args.h) {
  console.log(USAGE);
  process.exit(0);
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
  ...(args["bond-bits"] ? { BOND_BIRTHDAY_BITS: Number(args["bond-bits"]) } : {})
};

// --- Fleet-seed policy (§12.1) --------------------------------------------

const bridgePolicy = args.bridge === true ? "both" : String(args.bridge ?? "off");
if (!BRIDGE_POLICIES.includes(bridgePolicy)) {
  fail(`--bridge must be one of ${BRIDGE_POLICIES.join("|")} (got ${bridgePolicy})`);
}
const bridging = bridgePolicy !== "off";

// --admit is what lets a fleet's bond-less phones contribute: being
// directly connected to this seed is their admission, because the seed's
// address travels only inside sealed tickets. Absent, this is an
// ordinary keeper and every peer must present its own PMA1.
let admit = null;
if (args.admit === true || args.admit === ADMIT_CONNECTED) {
  admit = ADMIT_CONNECTED;
} else if (typeof args.admit === "string" && args.admit.trim()) {
  admit = args.admit.split(",").map(part => part.trim()).filter(Boolean);
  if (!admit.length) fail("--admit=<peerId>,… listed no peers");
}

const bootstrap = args.bootstrap
  ? String(args.bootstrap).split(",").map(part => part.trim()).filter(Boolean)
  : [];
if (bridging && !bootstrap.length) {
  fail(`--bridge=${bridgePolicy} needs --bootstrap=<multiaddr>,… — the wider network this seed bridges to`);
}
// Defence in depth, not the last line of it. Since §5.1's GossipSub
// topic validators, this host validates every message before forwarding
// it, so a one-host seed no longer launders an admitted driver's records
// into the wider mesh. The shape is still refused, because the two-host
// split is what makes "validate, then vouch" a property of the transport
// instead of a property of this process staying correct — and because
// `in`/`both` are directions, which need an upstream node that is not
// also the island's publisher.
if (!bridging && admit && bootstrap.length) {
  fail("--admit with --bootstrap and no --bridge puts an admitted island and the wider mesh on one gossip host. Records are validated before this host forwards them (§5.1), so nothing crosses unchecked — but a bridge is how a seed states the direction it crosses in, and keeping the meshes physically separate is what makes that hold no matter what one process does. Use --bridge=both (validated republish upstream) or --bridge=in (receive only), or drop --bootstrap to stay an island.");
}

// --- Host + node ----------------------------------------------------------

const host = await createPulseMeshHost({
  listen: [String(args.listen || "/ip4/127.0.0.1/tcp/0")],
  // With bridging on, --bootstrap belongs to the upstream host below:
  // this one faces the island and dials nobody.
  bootstrapPeers: bridging ? [] : bootstrap
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

// §5.4: a keeper serves snapshots, and a snapshot's records are only as
// good as the bond of the peer serving them — so a keeper without a bond
// is a keeper nobody can use. Mint before announcing.
if (!(await network.mintBond())) fail("could not mint the keeper's admission bond");

// --- Upstream side of a bridged seed (§12.1) ------------------------------
//
// A second host, dial-only: it listens on nothing, because nobody
// upstream needs to reach a fleet's seed — the seed reaches out. Two
// hosts and not one so that the only path across is the bridge, whatever
// this process does with its gossip meshes (see the refusal above), and
// so that `in` can be a direction: an upstream node that publishes
// nothing cannot also be the island's publisher.

let upstreamHost = null;
let upstreamNetwork = null;
let upstreamNode = null;
if (bridging) {
  upstreamHost = await createPulseMeshHost({ listen: [], bootstrapPeers: bootstrap });
  upstreamNetwork = createLibp2pNetwork(upstreamHost);
  upstreamNode = new MeshNode({
    id: upstreamHost.peerId.toString(),
    epochHex,
    constants,
    cellOf,
    cellContext,
    network: upstreamNetwork,
    transport: "wire",
    keeper: true,
    // §11.6, one direction at a time: `in` means nothing this fleet
    // publishes leaves the seed, which is exactly a read-only node —
    // no bond, no gossip membership, everything pulled on tick. The
    // island-facing node above stays a normal bonded peer, so the seed
    // is read-only upstream and fully bonded downward.
    readOnly: bridgePolicy === "in"
  });
  upstreamNode.subscribeZones(zones);
  await upstreamNetwork.ready;
  if (upstreamNetwork.registrationError) {
    fail(`upstream host could not register the sync protocol: ${upstreamNetwork.registrationError.message}`);
  }
  if (bridgePolicy === "both" && !(await upstreamNetwork.mintBond())) {
    fail("could not mint the seed's upstream admission bond");
  }
}

// The bridge owns both fleet-seed jobs: who this seed vouches for, and
// what crosses. Either can be on without the other.
const bridge = (bridging || admit)
  ? createFleetBridge({
      island: node,
      upstream: upstreamNode,
      policy: bridgePolicy,
      zones,
      admit,
      islandPeers: () => network.peersOf()
    })
  : null;
if (bridge) {
  // Admit on connect, not only on the tick: a driver that dials and
  // publishes inside the anti-entropy period would otherwise have its
  // first records dropped by rule 5.
  host.addEventListener("peer:connect", () => {
    try {
      bridge.admitConnected();
    } catch {
      // A connect event racing shutdown decides nothing.
    }
  });
  bridge.admitConnected();
}

console.log(JSON.stringify({
  event: "listening",
  peerId: host.peerId.toString(),
  multiaddrs: host.getMultiaddrs().map(String),
  zones: zones.map(zone => `${zone.x}/${zone.y}`),
  epoch: epochHex.slice(0, 16),
  storeCap: constants.STORE_CONTRIB_CAP,
  bondBits: constants.BOND_BIRTHDAY_BITS,
  seedCard: Boolean(args["seed-card"]),
  bridge: bridgePolicy,
  admit: admit === ADMIT_CONNECTED ? "connected" : admit ? admit.length : null,
  upstreamPeerId: upstreamHost ? upstreamHost.peerId.toString() : null,
  upstreamReadOnly: bridgePolicy === "in"
}));

// --- Fleet-seed mode (threads §20.10) -------------------------------------

/**
 * Prints this keeper's seed card: its dialable addresses, the
 * `wayfind://seed#…` link, and that link as a QR block a phone in the
 * depot can scan off the terminal.
 *
 * **Everything here goes to stderr**, and the stdout JSON-lines contract
 * is untouched. Not a stylistic choice: stdout is parsed — by
 * supervisors, by the wire tests that wait for `{"event":"listening"}`,
 * by whatever an operator has piped this into — and a multi-line QR
 * block interleaved with those lines is a stream that no longer parses.
 * A card is for a human looking at a terminal; the event stream is for a
 * machine reading a pipe. They are two audiences, so they get two
 * streams, and `--seed-card` announces itself in the listening event so
 * a supervisor still knows the mode is on.
 *
 * The address filter is the part that matters operationally. libp2p
 * reports what it is bound to, which on a server is usually
 * `/ip4/0.0.0.0/tcp/4001` — the instruction it was given, not a place
 * anyone can dial — plus loopback. A card built from those is a card
 * that fails in the depot with no error anybody can see, so when nothing
 * routable is left this prints the reason and no card at all.
 */
function printSeedCard() {
  const all = host.getMultiaddrs().map(String);
  const dialable = all
    .filter(isDialableAddress)
    .filter(address => Buffer.byteLength(address, "utf8") <= THREAD_MAX_BOOTSTRAP_BYTES)
    .slice(0, THREAD_MAX_BOOTSTRAP_ADDRESSES);
  const label = args["seed-label"] === true ? "" : String(args["seed-label"] || "");
  if (!dialable.length) {
    console.error("");
    console.error("pulsemesh-keeper: no dialable address to put in a seed card.");
    console.error(`  listening on: ${all.join(", ") || "(nothing)"}`);
    console.error("  Those are loopback or unspecified addresses: they are where this process was told");
    console.error("  to listen, not somewhere another device can reach. A card built from them would");
    console.error("  scan cleanly in the depot and connect to nothing.");
    console.error("  Fixes, in the order they are usually right:");
    console.error("    - run this on a host with a public address (a small VPS sidesteps NAT entirely);");
    console.error("    - forward a port to this machine and pass --listen with the address that reaches it;");
    console.error("    - or put a circuit relay in front of it and hand out the relayed address.");
    return;
  }
  const card = encodeSeedCard({ addresses: dialable, label });
  const url = seedCardUrl(card);
  console.error("");
  console.error(`pulsemesh-keeper: fleet seed${label ? ` — ${label}` : ""}`);
  for (const address of dialable) console.error(`  ${address}`);
  if (dialable.length < all.length) {
    console.error(`  (${all.length - dialable.length} loopback/unspecified address(es) left out)`);
  }
  console.error("");
  console.error(`  ${url}`);
  console.error("");
  // §20.5's scannability policy, unchanged: a card is ~60–170 bytes, so
  // this is a version 6-ish symbol and comfortable to scan.
  console.error(qrTerminal(encodeQr(url, { ecLevel: "M", maxVersion: 25 })));
  console.error("");
  console.error("  Scan it with a driver's phone, or paste the link into the demo's \"enrol a seed\" field.");
  console.error("  A seed card is a location, not a capability: it grants nothing, and every thread on");
  console.error("  the far side still needs its own key.");
  console.error("");
  console.error(`  admission: ${admit === ADMIT_CONNECTED
    ? "every directly connected peer (they reached this address, so a sealed ticket carried it to them)"
    : admit
      ? `${admit.length} allowlisted peerId(s)`
      : "none — devices must present their own §5.4 bond"}`);
  console.error(`  bridge:    ${bridgePolicy}${bridgePolicy === "off"
    ? " — this island contributes nothing to, and receives nothing from, the wider mesh"
    : bridgePolicy === "in"
      ? " — receive only: nothing this fleet publishes leaves the seed"
      : " — validated records cross both ways, upstream ones under THIS seed's bond"}`);
}

if (args["seed-card"]) printSeedCard();

// Keepers never contribute; they sweep, refresh subscriptions across
// topic rotations, and run anti-entropy like any consumer.
const tickTimer = setInterval(() => {
  node.tick().catch(() => {});
  // The upstream node runs the same beat. In `in` mode it is the ONLY
  // thing that moves: a read-only node joins no topic, so its whole
  // downlink is the anti-entropy pull this tick performs.
  upstreamNode?.tick().catch(() => {});
  bridge?.tick();
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
    storeStats: node.store.stats,
    ...(bridge
      ? {
          bridge: bridgePolicy,
          admitted: bridge.admittedPeers().length,
          muted: bridge.mutedPeers().length,
          bridgeStats: bridge.stats,
          upstreamPeers: upstreamHost ? upstreamHost.getPeers().length : 0,
          upstreamRecords: upstreamNode ? upstreamNode.store.size() : 0
        }
      : {})
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
  bridge?.close();
  await network.close();
  if (upstreamNetwork) await upstreamNetwork.close();
  await host.stop().catch(() => {});
  if (upstreamHost) await upstreamHost.stop().catch(() => {});
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
