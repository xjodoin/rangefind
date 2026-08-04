# Static routing and itinerary planning (rfroutegraph-v1)

Rangefind's route-graph lane computes driving routes, travel-time matrices,
and multi-stop itineraries from static files over HTTP range requests — the
same substrate as every other lane: content-addressed gzip packs, SHA-256
checksummed object pointers, a small root, and a provably bounded fetch set
per query. No routing server, no full-graph download.

This closes the loop that the [route-corridor feature](features.md) left
open: Rangefind previously consumed a route computed elsewhere; the
route-graph lane computes the route, and its output geometry feeds directly
into corridor search ("coffee along my trip"), which feeds place details,
constraints, and geocoding.

## How it works

The design is CRP/MLD (customizable route planning / multilevel Dijkstra)
adapted to range-addressed static objects:

1. **Extraction** (`scripts/osm_road_graph.mjs`, `--profile car|bike|foot`):
   allowed `highway=*` ways from an OSM PBF become a junction-collapsed
   directed graph per profile. The car profile caps class speeds by
   `maxspeed` (including `maxspeed:forward`/`:backward`) and applies
   `oneway`, access filters, and roundabouts; bike honors
   `oneway:bicycle=no` and opposite cycleways; foot ignores vehicular
   oneway and adds footways/paths/steps. All profiles degrade speeds on
   unpaved surfaces and bad smoothness, and fold junction penalties
   (traffic signals, stop, give-way, level crossings) into the edges
   entering the tagged node. Each edge carries its highway-class code for
   the time-bucket metrics below. Turn
   restrictions (`type=restriction` relations with a single via node,
   `no_*`/`only_*`, `except` handling, u-turn semantics) are compiled into
   the topology by via-node expansion: each restricted approach is
   redirected to a copy of the junction whose outgoing edges honor the
   restriction, so restrictions cost nothing at query time and every later
   stage sees plain edges. Via-way restrictions (dual-carriageway u-turn
   bans and their kin, including multi-via-way chains resolved through the
   union of the via ways) expand the whole via chain with path memory:
   only traffic that entered from the from-way sees the restricted exit,
   while side exits and other approaches stay untouched. Only the largest
   strongly connected component is kept. Per-edge polyline geometry and
   street names survive.

   **Turn costs** (car and bike by default; `--no-turn-costs` to disable):
   the graph is fully junction-expanded into an edge-based graph — every
   junction splits into one copy per incoming edge, and every outgoing
   edge is re-emitted per approach with a bearing-derived cost added
   (straight free; slight/full left and right turns priced separately,
   left higher under right-hand driving; u-turns heavily penalized but
   never forbidden, so dead ends stay reachable). In this mode via-node
   restrictions become exact per-approach turn filters, which also makes
   chained restrictions exact. The construction is the standard line graph
   expressed as node splitting, so edges keep their geometry, names, and
   distances and the partition/clique/query pipeline is untouched — the
   graph is ~2.3× larger and everything downstream just works.
2. **Partition** (`rangefind/route/build`): nodes are KD-partitioned into
   contiguous leaf cells (default ≤1280 nodes) over a locality-preserving
   order, then grouped by fanout into nested parent cells until the top
   level is small. Node ids are the KD order, so every cell at every level
   is one contiguous id range.
3. **Cliques, bottom-up**: each cell's boundary nodes (endpoints of edges
   that leave the cell) get an exact all-pairs shortest-path clique computed
   within the cell — leaves over raw edges, parents over their children's
   overlay graphs. This is CRP customization; changing the metric (new
   speeds, live weights) only recomputes cliques.
4. **Objects on disk**:
   - `cell` blocks (RFRC): a leaf's raw edges, weights (deciseconds),
     distances, street-name ids, and polyline geometry;
   - `overlay` blocks (RFRO): a parent cell's overlay graph — child cliques
     plus original edges crossing between children;
   - the top overlay: the same structure over the top-level cells, published
     as its own object (`top/`) so sharded deployments share one boundary
     artifact;
   - a root (RFRT): leaf bboxes and node ranges, per-level cell tables,
     shard/pack tables, and object pointers. Quebec's root is 33 KB.
5. **Query** (`rangefind/route`): snapping reads 1–4 leaf cells near each
   endpoint (bbox candidates from the root, exact polyline projection,
   one-way handled by seeding directed edges). The query then fetches a
   fixed object set computable from the endpoints alone — the snap leaves,
   one overlay per ancestor level per endpoint, and the top overlay — in a
   single parallel wave, and runs a bidirectional multilevel Dijkstra
   client-side. Because every clique edge's weight equals an exact
   shortest-path length, the union of all fetched objects is both complete
   and exact: results equal a flat Dijkstra over the full graph, verified
   edge-for-edge in tests and benchmarks.
6. **Path unpacking**: clique edges expand recursively through their child
   cells (local Dijkstra inside one object, with an exact weight assertion),
   fetching only the cells the route actually passes through — the route's
   own corridor. Geometry, distance meters, and named steps come out of the
   raw edges; street names load from one lazy sidecar.

Sharding is the same picture at a different granularity: a shard is a
contiguous group of top-level cells with its own pack files, and the top
overlay is the cross-shard boundary artifact. A query touches the source
shard, the target shard, and `top/` — never the rest of the planet. Sharded
and monolithic builds return identical results by construction.

## API

```js
import { openRouteGraphDir } from "rangefind/route/node"; // or openRouteGraph + io adapter

const engine = await openRouteGraphDir("./route-graph");

const route = await engine.route({
  from: { lat: 45.5088, lon: -73.554 },
  to: { lat: 46.8131, lon: -71.2075 }
});
// route.seconds, route.distanceMeters, route.geometry ([lat, lon] pairs),
// route.steps ([{ name, meters, seconds }]), route.stats (fetch budget)

const matrix = await engine.matrix({ points: stops }); // seconds[i][j]

const trip = await engine.itinerary({ stops, roundTrip: false });
// trip.order (Held-Karp exact ≤ ~12 stops, 2-opt beyond), trip.legs,
// trip.totalSeconds, trip.totalMeters

// Time-of-day metrics: indexes built with time buckets answer per-bucket,
// selected explicitly or by departure time against each bucket's rules.
const rush = await engine.route({ from, to, departureTime: "2026-08-04T08:15" });
// rush.bucket === "peak"; results are exact for that bucket's metric.

// Alternatives + live-weight re-ranking: k penalized re-searches over the
// same fetched objects, then fresh per-edge factors pick the winner.
const withAlternatives = await engine.route({
  from, to, alternatives: 2,
  liveWeights: { epoch: engine.root.sourceHash, factors: { "412/31": 2.5 } }
});
// withAlternatives.adjustedSeconds, withAlternatives.alternatives[],
// each route's edges expose stable "leaf/edgeIndex" ids for feeds.
```

Browsers (and any fetch-capable host) use the built-in HTTP adapter — all
objects are immutable and content-addressed, so CDN and browser caching
behave exactly like the search index, and servers that ignore `Range` and
answer 200 are tolerated by slicing client-side:

```js
import { openRouteGraphUrl } from "rangefind/route";

const engine = await openRouteGraphUrl("https://example.com/route-graph/");
const route = await engine.route({ from, to });
```

`route({ geometry: false })` skips path unpacking for matrix/ranking
workloads. TypeScript declarations ship at `rangefind/route`.

Build:

```bash
node scripts/osm_road_graph.mjs quebec-latest.osm.pbf quebec.graph.bin
node scripts/route_bench.mjs build quebec.graph.bin ./route-graph --shards 4
```

## Quebec benchmark

`scripts/route_bench.mjs bench` verifies exact equality against a reference
full-graph Dijkstra (identical snap seeds) and measures fetch budgets per
distance bucket; `compare` proves sharded == monolithic.

Quebec extract (2026-07 PBF), with full turn-cost junction expansion:
1,507,941 expanded nodes / 4,063,393 turn-priced edges for the car profile
(from 654 k junctions / 1.5 M road edges; 11,439 restricted turns filtered
exactly), extraction 46 s, index build 36 s, 82 MB on disk with two time
buckets (2,048 leaf cells, three overlay levels, top of 4). Measured on an
M-series laptop with a simulated 25 ms per-request RTT for cold queries:

| Bucket | Cold | Requests | Fetched | +Geometry | Warm |
| --- | --- | --- | --- | --- | --- |
| local <10 km | 65 ms | 12 | 3.3 MB | +66 ms | 16 ms |
| regional 10–100 km | 70 ms | 15 | 3.6 MB | +135 ms | 22 ms |
| long >100 km | 76 ms | 14 | 3.6 MB | +202 ms | 29 ms |

The exact turn-aware metric costs roughly 2.5× the transfer of the
turn-naive build (still available via `--no-turn-costs`). A 5-stop /
525 km itinerary (matrix + Held-Karp + 4 unpacked legs) completes in
~410 ms cold.

### Performance engineering, with attribution

Every optimization was A/B-benched on the same fixed-seed Quebec pair set
and kept only if it measurably improved; exactness was re-verified against
reference Dijkstra at every step. Starting point (turn-cost build, first
cut): cold 110–126 ms, 4.4–4.7 MB, warm 20–43 ms, 90 MB index.

1. **Correct bidirectional stop + membership caching** (engine): the
   textbook `topF + topB ≥ μ` stop and a per-node fetched-object
   resolution cache. Settled nodes −48–60 %, warm 43 → 29 ms (long).
2. **Within-leaf coordinate sort** (builder): junction-expansion copies
   share coordinates; making them byte-adjacent lets gzip collapse their
   near-identical rows. Index 90 → 84 MB.
3. **Geometry/topology split with canonical polyline dedup** (cell v3 +
   RFRP geometry objects): approach copies and two-way twins share one
   polyline; the search fetches topology only. Neutral alone — it exposed
   that the top overlay was ~80 % of per-query bytes — and enabler for 4
   and 6.
4. **Deeper hierarchy** (default `topMaxCells 8` → three levels, top of
   4 on Quebec): the always-fetched top overlay shrank 3.7 → 1.0 MB per
   bucket. Per-query transfer 4.5 → 3.3–3.6 MB, cold ≈ −15 ms.
5. **Speculative single-wave fetch**: snap candidate leaves are computable
   from root bboxes before any I/O, so context overlays launch in the same
   wave as snap cells (top-up only when a cross-cell seed escapes the
   plan). Cold ≈ −25 ms at 25 ms RTT.
6. **Same-file range coalescing**: reads issued in one microtask window
   merge into gap-bounded Range requests (snap topology+geometry pairs are
   byte-adjacent by construction). Requests 24 → 12–15.
7. **Progressive coarse geometry**: `coarseGeometry` / `onCoarseRoute`
   deliver a sketch polyline (snapped endpoints through traversed leaf
   centers) the moment the search finishes, before any unpack fetch.
8. **One-to-many matrices**: k stops fetch one shared context union and
   run k multi-target forward searches instead of k² bidirectional routes
   (equality with pairwise is unit-tested). 5-stop itinerary 747 → 408 ms.

- Correctness: 40/40 random pairs exactly equal to reference Dijkstra on
  the base metric and 15/15 on the peak bucket (scaled reference), with the
  unpack path's per-clique weight assertions on — turn restrictions,
  junction penalties, and turn costs included, since they are plain
  topology by build time. The bike profile (2.14 M expanded nodes, 119 MB)
  verifies 15/15 exact; foot (1.26 M nodes, unexpanded) likewise.
- Sharded (4 shards) vs monolithic: 60/60 identical routes; a query opens
  only the source/target shards plus the shared top overlay.
- Geometry unpack is breadth-batched: each hierarchy depth is one parallel
  request wave, so even a 250 km path unpacks in a handful of waves
  (~146 ms at 25 ms RTT, down from ~1.1 s with sequential fetches). Use
  `geometry: false` for matrices.
- Itinerary demo: 5 stops across 524 km (Montréal → Québec City region),
  travel-time matrix + Held-Karp ordering + 4 unpacked legs in 291 ms cold,
  109 object fetches / 6.5 MB total; the resulting polyline feeds
  `prepareRoute` for corridor search directly.
- The HTTP adapter is verified to produce byte-identical routes and
  geometry against the file adapter over real Range-request round trips.
- Baseline for scale: an in-memory full-graph Dijkstra takes ~57 ms per
  query and needs the entire 70 MB graph resident; the static engine
  answers from ~1.4 MB of range reads with no resident graph.

## Errors and limits

- `route`/`matrix`/`itinerary` throw coded errors: `RANGEFIND_ROUTE_BAD_POINT`
  for non-finite or out-of-range coordinates and
  `RANGEFIND_ROUTE_SNAP_TOO_FAR` when the nearest road exceeds
  `maxSnapMeters` (default 250, configurable per engine or per snap).
- Results report `from`/`to` snapped coordinates and snap distances, and
  per-query fetch stats.
- Snap results are cached per coordinate, so matrices re-snap each stop
  once, not once per pair.

## Speed limits

Cells carry a per-edge posted limit in km/h (0 when the way has no `maxspeed`
tag), and `route()` reports it on every edge plus a per-step summary — the
limit covering the most of that street:

```js
route.steps[0].speedLimitKmh   // 50
route.edges[0].speedLimitKmh   // 50
```

This is deliberately a separate column rather than something derived from
`seconds`. The travel weight also absorbs surface and smoothness degradation,
the profile cap, and junction penalties, so reading a speed back out of it
would report 35 km/h on a posted 50 road — wrong in a way that matters for a
display drivers trust. An untagged way reports 0 rather than the profile
default, because a guess is not a limit.

## Live traffic providers

Live data enters through a generic, pluggable provider contract — a P2P
mesh ([PulseMesh](pulsemesh.md) is the first design), a CDN-published
delta sidecar, a municipal feed, or an in-memory loopback all implement
the same interface:

```js
const provider = {
  name: "my-feed",
  async fetch({ epoch, areas, maxAgeSeconds }) {
    // epoch === root.sourceHash, or return [].
    return [
      { segment: "412/31/0", speedMps: 4.2, meters: 380, confidence: 0.8, observedAt: Date.now() },
      { segment: "413/7/1", closed: true } // verified closures only
    ];
  }
};

const result = await engine.route({ from, to, live: provider });
// result.adjustedSeconds — ETA under the live metric
// result.live — { provider, states, applied, error }
```

Segments are identified by the stable physical directed-segment id
`"leaf/polyline/direction"` (exposed on `result.edges[].segment` and
`snap()` matches) — every approach copy of a road edge from the turn-cost
expansion shares one id, so a single state fans out correctly. States
carry a time `factor` or `speedMps` + `meters`, an optional confidence
that decays with age and blends the cost toward the static model, and
`closed` / `penaltySeconds` for incidents.

The search itself runs under the live metric: leaves referenced by live
states join the context set like snap leaves, and overlay shortcuts
through live-adjusted subtrees are suppressed so the search descends to
the adjusted raw edges — closures and jams are exact wherever the
provider reports, and the far field falls back to the static metric
(which is where live data is sparse anyway). Clique unpacking and the
reported `seconds` stay on the exact static metric; `adjustedSeconds` is
the live estimate. Provider failures and unknown epochs degrade
gracefully to the static route. `createStaticLiveProvider(states)` ships
as the reference implementation for tests and loopbacks. In `matrix()`
the live metric applies to fetched corridor edges without shortcut
suppression (approximate); `route()` legs of an itinerary get the full
treatment.

`liveWeights` remains as the lower-level re-rank input (adjust ETAs of
already-computed alternatives without re-searching), now also accepting
physical segment ids as keys.

## Time-of-day and live traffic

Bucket metrics are the static half of traffic support: an index built with
`timeBuckets` (name, day/hour rules, per-class time factors — see the
`--peak` flag in `scripts/route_bench.mjs` for the heuristic default) gets
one exact overlay set per bucket. Cells store base weights plus a class
code, so bucket weights are recomputed identically at build and query time
and per-bucket results equal a reference Dijkstra on the scaled graph.
Real measured speed profiles plug into the same `classFactors` shape.

The live half is consumption-side by design: `alternatives: k` computes
diverging routes by penalized re-search over the already-fetched objects
(no extra requests), each route exposes its edges under stable
`leaf/edgeIndex` ids tied to the build's `sourceHash` epoch, and
`liveWeights` re-ranks the candidate set and adjusts ETAs with fresh
per-edge factors — from any source: a CDN-published delta sidecar or a
peer-to-peer feed. This is a re-rank, not a re-route; sub-minute
metric-exact rerouting remains a hosted-service capability.

## Boundaries

- Profiles are separate builds (car/bike/foot each get their own index
  directory); there is no multi-profile object sharing.
- Via-node and via-way restrictions (single or multi-via-way) are enforced
  exactly; the only fallbacks are via-way relations whose entry/exit
  junctions cannot be identified unambiguously and relations with more
  than four via ways, both counted in the extraction log. With turn costs
  enabled, via-node restrictions are exact per-approach filters (no chain
  depth limit); without them, chains deeper than 3 restricted junctions
  are truncated. Turn restrictions apply to the car profile.
- Turn costs are heuristic bands by turn geometry, not measured
  per-junction delays; junction penalties are fixed per node type. At
  snapped endpoints the turn-cost share of a partial edge is
  ratio-scaled with the rest of its weight.
- Bucket factors scale whole edge weights, including the folded junction
  penalty and turn-cost portions.
- `liveWeights` re-ranks computed candidates; it does not re-run the
  search under the adjusted metric.
- Distances inside cliques are exact in deciseconds; displayed seconds are
  quantized to 0.1 s.
