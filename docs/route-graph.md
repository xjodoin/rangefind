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
   `maxspeed` (including `maxspeed:forward`/`:backward`, and
   `maxspeed:conditional` carried separately as a time window) and applies
   `oneway`, access filters, and roundabouts; bike honors
   `oneway:bicycle=no` and opposite cycleways; foot ignores vehicular
   oneway and adds footways/paths/steps. All profiles degrade speeds on
   unpaved surfaces and bad smoothness, and fold junction penalties
   (traffic signals, stop, give-way, level crossings) into the edges
   entering the tagged node. One intersection is charged once, however
   many nodes describe it: a divided road carries a signal on each
   carriageway and a signalised junction a crossing on each arm, all of
   them one red light to a driver. Tagged nodes within 45 m of one already
   charged are absorbed into it, heaviest first, so the signal is what gets
   paid for and the crossings beside it fall in behind. (45 m rather than the
   30 m this started at: a signalised crossroads with a carriageway and a turn
   lane on each side spans a little over 30 m — Chemin de la Grande-Côte at
   Boulevard Labelle measures 31.2 m — and the old radius cut through the
   middle of one intersection rather than between two.) Each edge carries its
   highway-class code for the time-bucket metrics below. A one-way carriageway
   tagged `lanes:both_ways` or `turn:lanes:both_ways=left` shares a painted
   centre two-way left-turn lane with the line facing it: one undivided street
   drawn as two, which read literally has no way across. Wherever the far line
   carries a junction with anything else, the near line is split opposite it
   and the two are joined by a short crossing edge in both directions, priced
   at the wait for a gap in the oncoming traffic. Without them the router
   drives past a destination on the far side, crosses at whatever driveway
   happens to be mapped through the middle, and comes back. Turn
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

## Inter-region routing

`openRouteGraphUrl()` opens one regional graph. `openRouteCatalogUrl()` opens
the mutable `rangefind-route-catalog-v1` discovery root and federates those
graphs without introducing a routing service or a continent-sized download:

```js
import { openRouteCatalogUrl } from "rangefind/route";

const roads = await openRouteCatalogUrl(
  "https://osm.rangefind.dev/routes/catalog.json",
  { profile: "car" }
);
const route = await roads.route({
  from: { lat: 45.5019, lon: -73.5674 }, // Montreal
  to: { lat: 43.6532, lon: -79.3832 }    // Toronto
});
// route.federated === true
// route.regions: ["quebec", "ontario"]
// route.transitions[0].osmNodeId is the proven shared handoff
```

Every regional build publishes an immutable, content-addressed
`rfrouteportals-v1` gzip sidecar. It contains only junctions inside candidate
neighbor coverage. A transition is legal only when both independently built
sidecars contain the same original OSM node id at the same coordinate; nearby
roads are never joined by proximity. Coverage bboxes find candidates, not
connections. This uses Geofabrik's documented extract invariant that Osmium
keeps ways crossing an extract border complete
([technical details](https://download.geofabrik.de/technical.html)).

For a cross-region query the client:

1. resolves endpoint regions (ambiguous overlapping bboxes are verified by a
   real road snap);
2. enumerates a bounded number of catalog paths and intersects only the portal
   sidecars on those paths;
3. keeps a spatially diverse portal set per boundary (eight by default);
4. evaluates portal combinations with the actual regional road metric, not
   straight-line distance;
5. opens regional roots lazily, reuses their object caches, and unpacks geometry
   only for the winning legs;
6. merges geometry, steps, junctions, statistics, and region-qualified segment
   ids into one normal route result.

`maxPortalsPerBorder`, `maxRegionPaths`, `maxRegionHops`,
`maxRegionExpansions`, and `portalConcurrency` bound CPU and transfer for
unusual topology. Catalog BFS expands only portal-proven edges, so overlapping
bboxes with no shared road cannot crowd a real multi-region path out of the
candidate budget. `fromRegion`
and `toRegion` can pin endpoint coverage when an application already resolved
administrative membership. `matrix()` uses the same federated route planner.

This is different from an internal shard crossing. Internal shards share one
top overlay and are exact by construction. Regional graphs have independent
overlays and are connected only at verified OSM portals; a missing shared id
returns `RANGEFIND_ROUTE_REGIONS_DISCONNECTED` rather than inventing a bridge.

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
// trip.order (Held-Karp exact ≤ 10 interior stops, 2-opt beyond), trip.legs,
// trip.totalSeconds, trip.totalMeters

// Three end modes, mutually exclusive. By default stop 0 is the start and
// the last stop is a fixed end. `roundTrip: true` comes home to stop 0.
// `openEnd: true` pins only the start and lets the optimizer choose where
// the run finishes — a courier is rarely required to end at a particular
// address, and pinning whichever one was entered last distorts the whole
// order, not just the tail. `openEnd` leaves N−1 stops interior instead of
// N−2, so it reaches the exact-solver bound one stop sooner.
const run = await engine.itinerary({ stops, openEnd: true });

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

// Rerouting a moving vehicle: both directions of the road it is on snap
// equally well, so without a heading the search may seed the opposing edge
// for free and return a route that starts by driving back the way the
// driver came. `fromHeading` (degrees clockwise from north) charges the
// misaligned candidates, so turning around wins only when it saves more
// than it costs.
const continuing = await engine.route({
  from, to,
  fromHeading: 92,            // direction of travel, not the bearing to `to`
  headingPenaltySeconds: 60   // default; 0 disables the bias entirely
});
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

A reader requires the exact format version it was built for — cell v8, root
v5, source `rfroutesrc-v8` — and there are no compatibility shims. Carrying
older shapes means a branch per field per version, and every one of those is a
place to read a byte that is really the next field. An index is derived data
and reproducing it is two commands, so a format change is a rebuild, not an
archaeology problem. Clients holding an older copy are told to refresh it.

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

## What the signs say

A motorway's name is the one thing never written on a sign. Nobody in Québec
is looking for "Autoroute Félix-Leclerc"; they are looking for a green panel
reading **40 Ouest**, and above the slip road, **Sortie 32**. Guiding by
`name` alone announced roads by a label the driver could not see, and
announced the exit — the one instruction on a motorway that has to be right —
as a nameless ramp.

Distinct sign faces live in the root; an edge names one by index. Steps read
them back flattened:

```js
route.steps[4].ref              // "40"        the road's own number
route.steps[3].exitRef          // "32"        the exit number on the panel
route.steps[3].destinationRef   // "20 Est;30" where the ramp leads, with cardinal
route.steps[3].destination      // "Montréal;Québec"
```

All four come straight off the map, and the counts are why this works at all.
Across Québec, `ref` is on 12,605 of 12,866 motorway and trunk ways;
`destination:ref` on 5,739 slip roads; `junction:ref` on 1,442, backed by
1,323 numbered `motorway_junction` nodes. Direction is tagged on **41** route
relations in the entire province and is useless — but it is in
`destination:ref` on thousands of ramps, because that is what the sign says.
Semicolon lists are kept verbatim: which destination to lead with is a
presentation decision, and only the client knows how much room the banner has.

### Roundabouts

A circle arrives from the extract as two or three unnamed forty-metre ways
whose turn angles describe the curve rather than where the driver ends up — so
a roundabout followed by a left turn drew, and spoke, "bear right". The arcs
of one circle collapse into a single step, and the exits passed are counted as
the route goes round:

```js
route.steps[7].roundabout       // true
route.steps[7].roundaboutExit   // 2 — "take the second exit"
```

The exit number is the instruction, and it is the one thing geometry can never
supply. A roundabout that straddles a leaf boundary reports `0` rather than a
guess, and the client falls back to "at the roundabout, take the exit onto X".

### Stop signs belong to an approach, not to a junction

A stop sign is a property of one approach to an intersection, and OSM says so:
36,704 of Québec's stop nodes are tagged `direction=forward` and 31,468
`direction=backward`, against 4,882 with no direction at all. Reading the node
without its direction put a stop in front of every driver who passed it,
including the ones on the road with right of way — a recorded drive down Rue
Main drew three stops the driver never had to make, all of them belonging to
the side streets.

Junction kinds and junction penalties are both resolved per direction of
travel, and the intersection merge runs once per direction: two signals five
metres apart facing opposite ways are one red light to nobody, and merging
them across directions would leave one carriageway paying for a wait and the
other paying for nothing. Cardinal forms (`N`, `SSE`) and bearings (`225`) are
far down the tail and not resolvable without the way's heading at that node,
so they stay visible to both — a stop shown that is not there is a smaller
fault than a stop hidden that is.

### Ferries

A ferry has no `highway` tag — it is `route=ferry` — so every class lookup
keyed on `highway` treated the crossing as unroutable and dropped it. On a
coastline that is not a missing edge but a missing road: the router sent
drivers the long way round a river, or refused the trip, with nothing on
screen to say a boat was the answer. Québec has 65 of them.

A crossing is priced from its own `duration` rather than from a class speed,
plus half its `interval` as the expected wait (capped at 30 minutes, and 10
minutes where no interval is tagged). The wait is most of what a ferry costs,
and pricing one as a road is what makes a router send somebody to sit at a
slip for forty minutes to save four. Sorel-Tracy to Berthierville comes out at
9.2 km and 51 minutes by boat, against roughly 130 km by the nearest bridge.

## Speed limits

Cells carry a per-edge posted limit in km/h (0 when the way has no `maxspeed`
tag). `route()` reports it three ways: per edge, as a per-step summary, and —
the one a moving vehicle should read — as a step function over distance
travelled.

```js
route.edges[0].speedLimitKmh   // 50
route.steps[0].speedLimitKmh   // 50, the limit covering most of that street
route.speedLimits              // [{ atMeters: 0, limitKmh: 50 }, { atMeters: 1302, limitKmh: 70 }, …]
```

The per-step number is for the itinerary list, where one number per street is
the right answer and there is no position to be more precise about. It is the
wrong answer for a sign: a street is not a limit, and an autoroute drops from
100 to 70 through an interchange and climbs back without ever changing its
name. Measured on the published Québec index, Laval to Montréal is 22.4 km
over 17 steps with 10 limit changes, and 883 m of it — 3.9% — reads wrong if
the step's single number is used.

### Limits that apply only at certain times

`maxspeed:conditional` (school zones, in practice) rides beside the posted
limit rather than replacing it. Distinct windows live in the root; an edge
names one with a byte. Québec's 105 conditional ways use ten windows between
them, over 757 of 4,676,721 car edges.

```js
route.speedLimits[3]
// { atMeters: 120, limitKmh: 50,
//   conditional: { limitKmh: 30, days: 0b0011111,       // Mo-Fr, from Monday
//                  startMinute: 420, endMinute: 1020,   // 07:00-17:00
//                  monthStart: 9, monthEnd: 6 } }       // a school year, wrapping
```

The index does not resolve the window: it is static and the answer depends on
the clock, so an index that picked one would be wrong for every hour it was
not rebuilt in. The caller answers it, on local device time — the limit that
matters is the one on the sign the driver is looking at. Pass `departAt` (an
ISO local datetime or a `Date`) and the ETA includes the time spent at the
lower limit, reported as `route.conditionalDelaySeconds`. The search itself is
unchanged: 757 edges in 4.7 million cannot alter which way is quickest.

Month ranges are inclusive and may wrap, because a school year runs September
to June; read as a plain low-to-high span that range is empty, which silences
every school zone in the province and does so silently.

This is deliberately a separate column rather than something derived from
`seconds`. The travel weight also absorbs surface and smoothness degradation,
the profile cap, and junction penalties, so reading a speed back out of it
would report 35 km/h on a posted 50 road — wrong in a way that matters for a
display drivers trust. An untagged way reports 0 rather than the profile
default, because a guess is not a limit.

## Live traffic providers

Live data enters through a generic, pluggable provider contract — a P2P
mesh ([PulseMesh](pulsemesh.md) is the first implementation, shipping in
`src/pulsemesh/`), a CDN-published delta sidecar, a municipal feed, or an
in-memory loopback all implement the same interface:

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
  per-junction delays; junction penalties are fixed per node type, and
  charged once per intersection rather than once per tagged node. On
  Québec that merge absorbs roughly half the penalised nodes (car
  53,681 of 114,564; foot 45,333 against only 11,818 intersections
  charged — measured at the 30 m radius the merge started at, so the
  current 45 m absorbs somewhat more). Summing them instead priced
  crossing a divided boulevard at two or three signal waits, which was
  enough to route a car around a junction rather than straight through
  it. At snapped endpoints the turn-cost share of a partial edge is
  ratio-scaled with the rest of its weight.
- Centre-turn-lane crossings are synthesised from lane tags, so they are
  only as good as those tags: a road with the paint but without
  `lanes:both_ways` or `turn:lanes:both_ways` keeps the two lines it is
  drawn as, and a genuinely divided road that carries the tags by mistake
  gains a crossing it should not have. The 3–22 m width band and the
  requirement that the far side actually carry a junction are what bound
  the damage. A pair of crossings back to back is a u-turn across the
  centre lane; it is priced rather than forbidden, and at 2 x 5 s plus two
  left-turn charges it stays dearer than the u-turn cost the profile
  already carries.
- Bucket factors scale whole edge weights, including the folded junction
  penalty and turn-cost portions.
- `liveWeights` re-ranks computed candidates; it does not re-run the
  search under the adjusted metric.
- Distances inside cliques are exact in deciseconds; displayed seconds are
  quantized to 0.1 s.
