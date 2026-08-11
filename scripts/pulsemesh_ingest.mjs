// PulseMesh ingest node: a bonded peer that publishes traffic from
// external sources — municipal detector feeds, camera analytics APIs,
// 511-style event feeds — to bootstrap a region's live layer before it
// has organic contributors. See src/pulsemesh/ingest.js for what this
// deliberately is and is not.
//
//   node scripts/pulsemesh_ingest.mjs --epoch=<64 hex> --graph=<route-graph dir>
//        --config=<sources.mjs> [--listen=/ip4/0.0.0.0/tcp/4002]
//        [--bootstrap=<multiaddr>,...] [--copies=3] [--publish-free]
//        [--stats-seconds=30]
//
// The config module default-exports { sources: [...] } (or the array
// itself). A source is { id, intervalSeconds, fetch } — or declarative,
// { id, intervalSeconds, url, headers?, map }, which this script wraps
// in a JSON fetch. See examples/pulsemesh-ingest/sources.example.mjs.
//
// Emits JSON lines on stdout: {"event":"listening",...} once up, then
// periodic {"event":"stats",...}. SIGINT/SIGTERM stop cleanly.

import { pathToFileURL } from "node:url";
import { resolve } from "node:path";
import { DEFAULT_CONSTANTS } from "../src/pulsemesh/bins.js";
import { createMeshSession } from "../src/pulsemesh/session.js";
import { createIngestPublisher } from "../src/pulsemesh/ingest.js";
import { createLibp2pNetwork, createPulseMeshHost } from "../src/pulsemesh/libp2p.js";
import { datasetEpoch } from "../src/pulsemesh/dataset_epoch.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => {
  const [key, value] = arg.replace(/^--/, "").split("=");
  return [key, value ?? true];
}));

function fail(message) {
  console.error(`pulsemesh-ingest: ${message}`);
  process.exit(1);
}

const USAGE = `pulsemesh-ingest — publish traffic from external sources into the mesh

  node scripts/pulsemesh_ingest.mjs --epoch=<64 hex> --graph=<dir> --config=<sources.mjs> [options]

Required
  --epoch=<64 hex>        the route graph's sourceHash; must match --graph
  --dataset=<region/prof> name the epoch by dataset instead — "quebec/car".
                          Stable across rebuilds. Must match the keeper this
                          feeds, or these records land on a topic nobody is on.
  --graph=<dir|url>       the static route graph observations are matched against;
                          a directory or an http(s) base URL
  --config=<file.mjs>     module exporting { sources: [...] }; each source is
                          { id, intervalSeconds, fetch({nowMillis}) -> observations }
                          or { id, intervalSeconds, url, headers?, map(json) -> observations }.
                          Observations:
                            { kind: "flow", lat, lon, speedKmh, bearingDeg?,
                              bothDirections?, observedAtMillis? }
                            { kind: "incident", lat, lon, type, closedRoad?,
                              observedAtMillis? }
                          closedRoad: true (full closures stated by the road
                          authority ONLY) also maintains a near-zero speed
                          state on the segment so the router prices it as
                          impassable; it expires when the feed stops
                          asserting it.

Mesh
  --listen=<multiaddr>    where this host listens (default /ip4/127.0.0.1/tcp/0)
  --bootstrap=<ma>,...    peers to dial — a keeper, or any bonded peer

Publishing
  --copies=<1..4>         PMC1 records per flow observation (default 3, the
                          receiver-side AGG_MIN_REPORTS: fewer aggregate only
                          as a confidence-capped hint)
  --publish-free          publish free-flow readings too; default publishes
                          only congestion plus recovery on cleared jams
  --max-feed-age=<sec>    drop observations staler than this (default 300)
  --stats-seconds=<n>     stats line period (default 30)

An ingest node stakes its bond on its feeds: records failing the map-held
receivers' plausibility rules cost it -500 trust each, and a floored peer
forfeits the bond. Incidents from one peer only ever reach hint tier
(§8.5 scores min(raw, distinct deliverers)) — the speed records are what
carry the authority.
`;
if (args.help || args.h) {
  console.log(USAGE);
  process.exit(0);
}

// Same two ways to name the epoch as the keeper, and for the same reason:
// the epoch is the topic namespace, so an ingest node that derives it
// differently from the mesh it feeds publishes into a void — validly
// signed records on a topic nobody is listening to, with nothing
// anywhere reporting a problem. An operator running both must pass the
// same flag to both.
if (args.epoch && args.dataset) {
  fail("--epoch and --dataset both name the epoch; pass one. Use whichever the "
    + "keeper this feeds was given.");
}

const datasetId = args.dataset ? String(args.dataset).trim().toLowerCase() : null;
const epochHex = datasetId ? await datasetEpoch(datasetId) : String(args.epoch || "");
if (!/^[0-9a-f]{64}$/.test(epochHex)) {
  fail("name the epoch with --epoch=<64 hex sourceHash> or --dataset=<region/profile>");
}
if (!args.graph) fail("--graph=<dir|https://…> is required");
if (!args.config) fail("--config=<sources.mjs> is required");

// --- Sources ---------------------------------------------------------------

function fetchOf(source) {
  if (typeof source.fetch === "function") return source.fetch;
  if (!source.url) throw new Error(`source ${source.id || "?"} has neither fetch() nor url`);
  return async () => {
    const response = await fetch(source.url, {
      headers: source.headers,
      signal: AbortSignal.timeout(source.timeoutMillis ?? 15000)
    });
    if (!response.ok) throw new Error(`${source.url}: HTTP ${response.status}`);
    const body = source.format === "text" ? await response.text() : await response.json();
    return source.map ? source.map(body) : body;
  };
}

// Publisher options a region config may carry (`export default { options,
// sources }`); CLI flags override them. Allowlisted so a config cannot
// swap out the engine or session underneath the script.
const CONFIG_OPTIONS = [
  "copies", "publishFreeFlow", "congestedRatio", "recoverySeconds",
  "maxFeedAgeSeconds", "maxSnapMeters", "maxQueueSeconds", "maxQueueLength",
  "incidentReEmitSeconds", "closureSpeedKmh", "closureRefreshSeconds"
];

let sources;
let configOptions = {};
try {
  const loaded = (await import(pathToFileURL(resolve(String(args.config))))).default;
  const list = Array.isArray(loaded) ? loaded : loaded?.sources;
  if (!Array.isArray(list) || !list.length) throw new Error("config exports no sources");
  sources = list.map(source => ({
    id: String(source.id || source.url || "source"),
    intervalSeconds: source.intervalSeconds,
    fetch: fetchOf(source),
    // A source may own something that has to be shut down — a camera
    // source owns a browser. Carry it through so stop() can close it
    // rather than leaving the process wedged on an open window.
    close: typeof source.close === "function" ? () => source.close() : null
  }));
  for (const key of CONFIG_OPTIONS) {
    if (!Array.isArray(loaded) && loaded?.options?.[key] !== undefined) {
      configOptions[key] = loaded.options[key];
    }
  }
} catch (error) {
  fail(`could not load --config: ${error.message}`);
}

// --- Engine + mesh ---------------------------------------------------------

// A directory or a published URL, like the keeper. An ingest node matches
// observations to the graph continuously rather than reading the root
// once, so this one does pay for byte ranges as it runs — which is the
// right trade against mirroring a region, and the wrong one if the feed
// interval is seconds. It is stated rather than assumed.
const graphSource = String(args.graph);
let engine;
try {
  engine = /^https?:\/\//iu.test(graphSource)
    ? await (await import("../src/route_graph_query.js")).openRouteGraphUrl(graphSource)
    : await (await import("../src/route_graph_node.js")).openRouteGraphDir(graphSource);
} catch (error) {
  fail(`cannot open the route graph at ${graphSource}: ${error?.message ?? error}`);
}
// Only --epoch pins one exact build; a dataset epoch matches every build
// of its region on purpose.
if (!datasetId && engine.root.sourceHash !== epochHex) {
  fail(`graph epoch ${engine.root.sourceHash.slice(0, 16)}… does not match --epoch ${epochHex.slice(0, 16)}…`);
}

const bootstrap = args.bootstrap
  ? String(args.bootstrap).split(",").map(part => part.trim()).filter(Boolean)
  : [];
const host = await createPulseMeshHost({
  listen: [String(args.listen || "/ip4/127.0.0.1/tcp/0")],
  bootstrapPeers: bootstrap
});
const network = createLibp2pNetwork(host);
const session = await createMeshSession({
  engine,
  network,
  id: host.peerId.toString(),
  transport: "wire",
  // Explicitly, because the session's default is the graph's sourceHash.
  // That was the same value back when --epoch had to equal it; with
  // --dataset they differ on purpose, and leaving this out publishes
  // every record onto the content-hash topic while the whole mesh — and
  // this script's own `epoch` field — is on the dataset one. Nothing
  // errors: the feeds are read, the records are signed, and no listener
  // exists.
  epochHex
});
await network.ready;
if (network.registrationError) {
  fail(`could not register the sync protocol: ${network.registrationError.message}`);
}
// Rule 5: every receiver ignores records delivered by an unbonded peer.
// An ingest node that skipped this would publish into silence.
if (!(await network.mintBond())) fail("could not mint the ingest node's admission bond");

const ingest = createIngestPublisher({
  engine,
  session,
  sources,
  ...configOptions,
  ...(args.copies ? { copies: Number(args.copies) } : {}),
  ...(args["publish-free"] ? { publishFreeFlow: true } : {}),
  ...(args["max-feed-age"] ? { maxFeedAgeSeconds: Number(args["max-feed-age"]) } : {})
});

console.log(JSON.stringify({
  event: "listening",
  peerId: host.peerId.toString(),
  multiaddrs: host.getMultiaddrs().map(String),
  epoch: epochHex.slice(0, 16),
  sources: ingest.sources().map(source => source.id),
  copies: args.copies ? Number(args.copies) : DEFAULT_CONSTANTS.AGG_MIN_REPORTS,
  publishFree: Boolean(args["publish-free"])
}));

// The session's own maintenance (TTL sweep, anti-entropy, leaf warming)
// on the protocol beat; the ingest drain on a 1 s beat so the token
// bucket actually paces instead of bursting once per poll.
session.start();
const ingestTimer = setInterval(() => {
  ingest.tick().catch(() => {});
}, 1000);

const statsSeconds = Number(args["stats-seconds"] || 30);
const statsTimer = setInterval(() => {
  console.log(JSON.stringify({
    event: "stats",
    peers: host.getPeers().length,
    queue: ingest.queueLength,
    closures: ingest.closureCount,
    ...ingest.stats,
    sources: ingest.sources(),
    mesh: session.snapshot()
  }));
}, statsSeconds * 1000);
if (typeof ingestTimer.unref === "function") ingestTimer.unref();
if (typeof statsTimer.unref === "function") statsTimer.unref();

let stopping = false;
async function stop(signal) {
  if (stopping) return;
  stopping = true;
  console.log(JSON.stringify({ event: "stopping", signal }));
  clearInterval(ingestTimer);
  clearInterval(statsTimer);
  clearInterval(keepAlive);
  for (const source of sources) await source.close?.().catch?.(() => {});
  await session.close().catch(() => {});
  await host.stop().catch(() => {});
  process.exitCode = 0;
}
process.on("SIGINT", () => stop("SIGINT"));
process.on("SIGTERM", () => stop("SIGTERM"));

const keepAlive = setInterval(() => {}, 1 << 30);
