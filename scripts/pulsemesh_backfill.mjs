// PulseMesh lazy backfill: price one itinerary, and buy only the parts
// of it the mesh cannot answer.
//
//   node scripts/pulsemesh_backfill.mjs --graph=<route-graph dir> \
//        --from=<lat,lon> --to=<lat,lon> [--buy --key=<TomTom key>] \
//        [--listen=/ip4/0.0.0.0/tcp/4003] [--bootstrap=<multiaddr>,...]
//
// WITHOUT --buy this costs nothing and calls nothing: it routes, reads
// what the mesh already holds, and prints what it *would* buy. That is
// the mode to develop against, and the one to run before pointing a
// paid key at a new region.
//
// WITH --buy it probes the gaps, publishes the answers into the mesh as
// ordinary records, and re-prices the same itinerary against them so you
// can see what the money changed.
//
// Standalone (no --listen/--bootstrap) it runs on a loopback mesh: the
// records go into this process's own store and nowhere else. That is a
// real end-to-end exercise of the pipeline with no peers to bother.

import { createMeshSession } from "../src/pulsemesh/session.js";
import { createIngestPublisher } from "../src/pulsemesh/ingest.js";
import { createRouteBackfill, createTomTomProbe } from "../src/pulsemesh/ingest_tomtom.js";
import { createLoopbackNetwork } from "../src/pulsemesh/node.js";
import { createStaticLiveProvider } from "../src/route_graph_query.js";

const args = Object.fromEntries(process.argv.slice(2).map(arg => {
  const [key, value] = arg.replace(/^--/, "").split("=");
  return [key, value ?? true];
}));

function fail(message) {
  console.error(`pulsemesh-backfill: ${message}`);
  process.exit(1);
}

const USAGE = `pulsemesh-backfill — buy traffic only where the mesh cannot answer

  node scripts/pulsemesh_backfill.mjs --graph=<dir> --from=<lat,lon> --to=<lat,lon> [options]

Required
  --graph=<dir>           the static route graph the itinerary is computed on
  --from=<lat,lon>        origin
  --to=<lat,lon>          destination

Buying (all of this is free without --buy)
  --buy                   actually probe and publish; without it, assess only
  --key=<key>             TomTom API key (or set TOMTOM_KEY)
  --max-probes=<n>        probes for this itinerary (default 8)
  --daily-budget=<n>      rolling 24 h probe ceiling (default 2500)
  --zoom=<0..22>          road-detail level the API matches a point at
                          (default 10 — the single biggest lever on whether
                          it answers for the road you meant)
  --base-url=<origin>     API origin (default https://api.tomtom.com); for an
                          enterprise endpoint, a proxy, or a local stub

Mesh
  --listen=<multiaddr>    publish to a real mesh instead of a local loopback
  --bootstrap=<ma>,...    peers to dial

Records published here propagate to every receiver and are cached for
CONTRIB_TTL. That is redistribution of the vendor's traffic data; check
your agreement before running --buy against a mesh other people are on.
`;
if (args.help || args.h) {
  console.log(USAGE);
  process.exit(0);
}

if (!args.graph) fail("--graph=<route-graph dir> is required");
const point = (value, name) => {
  const parts = String(value || "").split(",").map(Number);
  if (parts.length !== 2 || !parts.every(Number.isFinite)) fail(`--${name}=<lat,lon> is required`);
  return { lat: parts[0], lon: parts[1] };
};
const from = point(args.from, "from");
const to = point(args.to, "to");

const buying = Boolean(args.buy);
const key = args.key === true ? null : (args.key || process.env.TOMTOM_KEY || null);
if (buying && !key) fail("--buy needs --key=<TomTom key> or TOMTOM_KEY in the environment");

// --- Engine + mesh ---------------------------------------------------------

const { openRouteGraphDir } = await import("../src/route_graph_node.js");
const engine = await openRouteGraphDir(String(args.graph));

let network;
let host = null;
if (args.listen || args.bootstrap) {
  const { createLibp2pNetwork, createPulseMeshHost } = await import("../src/pulsemesh/libp2p.js");
  host = await createPulseMeshHost({
    listen: [String(args.listen || "/ip4/127.0.0.1/tcp/0")],
    bootstrapPeers: args.bootstrap
      ? String(args.bootstrap).split(",").map(part => part.trim()).filter(Boolean)
      : []
  });
  network = createLibp2pNetwork(host);
  await network.ready;
  if (network.registrationError) fail(`could not register the sync protocol: ${network.registrationError.message}`);
  if (buying && !(await network.mintBond())) fail("could not mint the publishing bond");
} else {
  network = createLoopbackNetwork({});
}

const session = await createMeshSession({
  engine,
  network,
  id: host ? host.peerId.toString() : "backfill",
  transport: host ? "wire" : "loopback"
});

// --- Route, assess, maybe buy ---------------------------------------------

const route = await engine.route({ from, to, live: session.provider() });
const km = metres => (metres / 1000).toFixed(1);

const probe = buying
  ? createTomTomProbe({
      key,
      zoom: Number(args.zoom) || 10,
      ...(args["base-url"] ? { baseUrl: String(args["base-url"]) } : {})
    })
  : async () => null;

const backfill = createRouteBackfill({
  engine,
  session,
  probe,
  ...(args["max-probes"] ? { maxProbesPerRoute: Number(args["max-probes"]) } : {}),
  ...(args["daily-budget"] ? { dailyProbeBudget: Number(args["daily-budget"]) } : {})
});

const plan = await backfill.assess(route);
console.log(JSON.stringify({
  event: "assessed",
  buying,
  seconds: Math.round(route.seconds),
  km: Number(km(plan.totalMeters)),
  unpricedKm: Number(km(plan.gapMeters)),
  unpricedShare: plan.totalMeters ? Number((plan.gapMeters / plan.totalMeters).toFixed(3)) : 0,
  meshCovered: backfill.stats.meshCovered,
  offClass: backfill.stats.offClass,
  candidates: plan.chunks.length,
  wouldProbe: plan.probes.map(chunk => ({
    roadClass: chunk.roadClass,
    km: Number(km(chunk.meters)),
    edges: chunk.edges.length
  })),
  budgetRemaining: plan.budgetRemaining
}, null, 2));

if (!buying) {
  console.log(JSON.stringify({
    event: "dry-run",
    note: "nothing was called and nothing was spent; re-run with --buy --key=… to probe"
  }));
} else {
  const ingest = createIngestPublisher({ engine, session, sources: [backfill.source] });
  const report = await backfill.backfill(route);

  // The publisher paces itself under the receivers' rate bucket, so
  // draining a burst takes a few beats rather than one.
  for (let beat = 0; beat < 30 && (backfill.queueLength || ingest.queueLength); beat++) {
    await ingest.tick();
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  await ingest.tick();

  // Re-price the same itinerary against what was just bought. The mesh
  // copies are for whoever asks next; `states` is what this request paid
  // for and can use now.
  const repriced = report.states.length
    ? await engine.route({ from, to, live: createStaticLiveProvider(report.states) })
    : null;

  console.log(JSON.stringify({
    event: "bought",
    probesIssued: backfill.stats.probesIssued,
    published: report.publishedSegments,
    rejected: {
      freeflowMismatch: backfill.stats.freeflowMismatch,
      lowConfidence: backfill.stats.lowConfidence,
      empty: backfill.stats.probesEmpty,
      errors: backfill.stats.probeErrors
    },
    closures: backfill.stats.closures,
    staticSeconds: Math.round(route.seconds),
    liveSeconds: repriced ? Math.round(repriced.seconds) : null,
    publisher: {
      segments: ingest.stats.publishedSegments,
      records: ingest.stats.publishedRecords,
      incidents: ingest.stats.publishedIncidents,
      shed: ingest.stats.shed
    },
    budgetRemaining: backfill.budgetRemaining,
    lastError: backfill.stats.lastError
  }, null, 2));
}

await session.close().catch(() => {});
if (host) await host.stop().catch(() => {});
