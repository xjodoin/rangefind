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
import { readFileSync, writeFileSync } from "node:fs";
import { datasetEpoch } from "../src/pulsemesh/dataset_epoch.js";

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
  --graph=<dir|url>       the static route graph the itinerary is computed on;
                          a directory or an http(s) base URL
  --dataset=<region/prof> rendezvous on a dataset epoch — "quebec/car" — the
                          same one the mesh being bought for uses. Without it
                          the epoch is the graph's content hash, and the
                          answers are published where nobody is listening.
  --from=<lat,lon>        origin
  --to=<lat,lon>          destination
  --patrol[=n]            instead of one itinerary, drive n corridors across
                          this graph's own territory (default 3). The node
                          picks them itself, so nothing else has to know it
                          exists. Reads only what the mesh has published,
                          never what peers have asked for.

Buying (all of this is free without --buy)
  --buy                   actually probe and publish; without it, assess only
  --key=<key>             TomTom API key (or set TOMTOM_KEY)
  --max-probes=<n>        probes for this itinerary (default 8)
  --daily-budget=<n>      rolling 24 h probe ceiling (default 2500)
  --spend-log=<file>      persist the probe timestamps the ceiling counts.
                          WITHOUT IT THE CEILING IS PER-PROCESS: anything that
                          schedules this gets a full budget every invocation.
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

if (!args.graph) fail("--graph=<dir|https://…> is required");
// The epoch is the topic namespace, so a backfill that derives it
// differently from the mesh it is buying for publishes its answers where
// nobody is listening — having paid for them. Same flag the keeper and
// the ingest node take.
const datasetId = args.dataset ? String(args.dataset).trim().toLowerCase() : null;
const point = (value, name) => {
  const parts = String(value || "").split(",").map(Number);
  if (parts.length !== 2 || !parts.every(Number.isFinite)) fail(`--${name}=<lat,lon> is required`);
  return { lat: parts[0], lon: parts[1] };
};
// --patrol replaces --from/--to: instead of being handed one itinerary,
// the node drives its own territory.
//
// This is what makes an ingest node driver-shaped rather than a service
// the console has to know about. It picks corridors across the graph's
// own leaves — leaves exist where roads are, so their centres snap — and
// runs each through exactly the same gates a real itinerary gets. The
// mesh-sufficiency gate is what makes it taper: every corridor that
// drivers start covering drops out of the candidate set on its own, so
// this pays to bootstrap an empty region and stops paying as the region
// fills.
//
// It deliberately reads nothing about what other peers have ASKED for.
// Cell requests are padded with decoys and split across peers precisely
// so no peer can reconstruct a corridor from them; counting them here
// would be an attack on that, run by the one machine best placed to do
// it. What this uses instead is the node's own store — what the mesh
// has PUBLISHED — which is public by construction.
const patrolCount = args.patrol ? Math.max(1, Number(args.patrol === true ? 3 : args.patrol)) : 0;
const from = patrolCount ? null : point(args.from, "from");
const to = patrolCount ? null : point(args.to, "to");

const buying = Boolean(args.buy);
const key = args.key === true ? null : (args.key || process.env.TOMTOM_KEY || null);
if (buying && !key) fail("--buy needs --key=<TomTom key> or TOMTOM_KEY in the environment");

// --- Engine + mesh ---------------------------------------------------------

// A directory or a published URL, like the keeper's --graph.
const graphSource = String(args.graph);
let engine;
try {
  engine = /^https?:\/\//iu.test(graphSource)
    ? await (await import("../src/route_graph_query.js")).openRouteGraphUrl(graphSource)
    : await (await import("../src/route_graph_node.js")).openRouteGraphDir(graphSource);
} catch (error) {
  fail(`cannot open the route graph at ${graphSource}: ${error?.message ?? error}`);
}

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
} else {
  network = createLoopbackNetwork({});
}

const session = await createMeshSession({
  engine,
  network,
  id: host ? host.peerId.toString() : "backfill",
  transport: host ? "wire" : "loopback",
  ...(datasetId ? { epochHex: await datasetEpoch(datasetId) } : {})
});

// The bond is minted AFTER the session exists, not before.
//
// `mintBond()` needs the mesh node, and the node is what
// `createMeshSession` registers with the network — so minting first
// throws "register(node) before mintBond()" and the run dies before it
// has probed anything. It only ever showed up on a real mesh: the
// loopback path mints nothing, which is the path this script is usually
// developed against.
if (host && buying && !(await network.mintBond())) {
  fail("could not mint the publishing bond");
}

// --- Route, assess, maybe buy ---------------------------------------------

/**
 * Corridors across this graph's territory, as a driver would cover it.
 *
 * Endpoints come from leaf bounding boxes because a leaf exists where
 * road is — their centres snap, where an arbitrary lat/lon lands in a
 * river as often as not. Pairs are drawn far apart so the router returns
 * an arterial crossing rather than a lane change; which arterial it
 * picks is the router's judgement, not a corridor list this script would
 * have to keep current.
 *
 * Spread over the leaf set by a stride rather than sampled randomly, so
 * successive runs cover different ground instead of re-driving one
 * corner — the same reason the camera scheduler rotates.
 */
async function patrolRoutes(count) {
  // Leaf bboxes are E7 integers, so halving and scaling gives degrees.
  const points = engine.root.leaves.filter(leaf => leaf?.bbox).map(leaf => ({
    lat: (leaf.bbox.minLat + leaf.bbox.maxLat) / 2e7,
    lon: (leaf.bbox.minLon + leaf.bbox.maxLon) / 2e7
  }));
  const kmBetween = (a, b) => {
    const dLat = (b.lat - a.lat) * 111;
    const dLon = (b.lon - a.lon) * 111 * Math.cos((a.lat * Math.PI) / 180);
    return Math.hypot(dLat, dLon);
  };

  // A corridor, not a cross-province expedition. Pairing leaves by index
  // put endpoints hundreds of kilometres apart on a graph this size and
  // routed nothing; distance is the thing that actually matters, so pick
  // on distance.
  const MIN_KM = 8;
  const MAX_KM = 35;
  const routes = [];
  const stride = Math.max(1, Math.floor(points.length / (count * 8)));
  // Walk from a different place each hour so successive runs cover
  // different ground rather than re-driving one corner.
  const offset = Math.floor(Date.now() / 3600000) % Math.max(1, points.length);
  for (let step = 0; step < points.length && routes.length < count; step += stride) {
    const a = points[(offset + step) % points.length];
    const b = points.find(candidate => {
      const d = kmBetween(a, candidate);
      return d >= MIN_KM && d <= MAX_KM;
    });
    if (!b) continue;
    try {
      routes.push({ route: await engine.route({ from: a, to: b, live: session.provider() }), from: a, to: b });
    } catch {
      // Will not snap, or the two do not connect. There are thousands of
      // leaves; the patrol only needs `count` corridors.
    }
  }
  return routes;
}

// Each entry carries its own endpoints. A route RESULT reports `from`
// as { snapped, snapDistanceMeters } rather than a point, so re-pricing
// a patrol corridor from `corridor.from` fails on a bad point — after
// the probes have already been paid for.
const routes = patrolCount
  ? await patrolRoutes(patrolCount)
  : [{ route: await engine.route({ from, to, live: session.provider() }), from, to }];
if (!routes.length) fail("patrol found no routable corridor in this graph");
const km = metres => (metres / 1000).toFixed(1);

const probe = buying
  ? createTomTomProbe({
      key,
      zoom: Number(args.zoom) || 10,
      ...(args["base-url"] ? { baseUrl: String(args["base-url"]) } : {})
    })
  : async () => null;

// The daily cap has to outlive this process, or a scheduled patrol
// spends the whole ceiling on every invocation.
const spendLogPath = args["spend-log"] === true ? null : (args["spend-log"] || null);
const spendLog = spendLogPath ? {
  load() {
    try {
      return JSON.parse(readFileSync(spendLogPath, "utf8"));
    } catch (error) {
      if (error?.code !== "ENOENT") {
        // Loud, because the safe reading of an unreadable budget file is
        // "I do not know what has been spent", and continuing as though
        // nothing has is how a ceiling silently stops being one.
        console.error(`pulsemesh-backfill: cannot read --spend-log ${spendLogPath}: ${error.message}`);
      }
      return [];
    }
  },
  record(millis) {
    const day = Date.now() - 24 * 60 * 60 * 1000;
    const kept = [...spendLog.load().filter(t => t > day), millis];
    writeFileSync(spendLogPath, JSON.stringify(kept));
  }
} : null;

const backfill = createRouteBackfill({
  engine,
  session,
  probe,
  ...(spendLog ? { spendLog } : {}),
  ...(args["max-probes"] ? { maxProbesPerRoute: Number(args["max-probes"]) } : {}),
  ...(args["daily-budget"] ? { dailyProbeBudget: Number(args["daily-budget"]) } : {})
});

// One corridor or several — a patrol runs each through exactly the same
// gates a hand-supplied itinerary gets. The backfill's budget, cache and
// stats are shared across them, so the ceiling counts the whole run and a
// corridor overlapping the last one is answered from cache rather than
// bought twice.
for (const [index, { route: corridor, from: corridorFrom, to: corridorTo }] of routes.entries()) {
  if (patrolCount) {
    console.log(JSON.stringify({ event: "patrol", corridor: index + 1, of: routes.length }));
  }
  const plan = await backfill.assess(corridor);
  console.log(JSON.stringify({
    event: "assessed",
    buying,
    seconds: Math.round(corridor.seconds),
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
    const report = await backfill.backfill(corridor);

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
      ? await engine.route({
        from: corridorFrom, to: corridorTo,
        live: createStaticLiveProvider(report.states)
      })
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
      staticSeconds: Math.round(corridor.seconds),
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
}

await session.close().catch(() => {});
if (host) await host.stop().catch(() => {});
