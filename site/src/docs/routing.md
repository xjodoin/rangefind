---
title: Routing & itineraries
lede: Driving routes, travel-time matrices, and optimized multi-stop itineraries — computed in the browser from static files, with no routing server.
description: Rangefind's route graph — CRP/MLD routing over range-addressed static objects, exact multi-stop itinerary ordering, turn restrictions and turn costs, sign-accurate steps, speed limits, time-of-day buckets, live traffic, and cross-region federation.
order: 14
---

The route-graph lane computes routes, travel-time matrices, and multi-stop
itineraries from the same substrate as every other lane: content-addressed
gzip packs, a small root, and a provably bounded fetch set per query.
Nothing is downloaded but the objects a query can prove it needs.

```bash
# 1. extract a profile graph from an OSM PBF
node scripts/osm_road_graph.mjs quebec-latest.osm.pbf quebec.graph.bin

# 2. partition, customize, and pack it
node scripts/route_bench.mjs build quebec.graph.bin ./route-graph --shards 4
```

```js
import { openRouteGraphUrl } from "rangefind/route";

const roads = await openRouteGraphUrl("https://example.com/route-graph/");

const route = await roads.route({
  from: { lat: 45.5088, lon: -73.554 },
  to:   { lat: 46.8131, lon: -71.2075 }
});
// route.seconds, route.distanceMeters, route.geometry ([lat, lon] pairs),
// route.steps, route.edges, route.stats (the fetch budget it spent)
```

`openRouteGraphDir` from `rangefind/route/node` reads a local directory
instead. Both return the same engine, and the HTTP adapter is verified to
produce byte-identical routes against the file adapter over real `Range`
round trips — including hosts that ignore `Range` and answer `200`, which
are tolerated by slicing client-side.

## Itinerary planning

A courier round is not a route: it is a set of stops whose *order* is the
answer. `itinerary()` builds the travel-time matrix, orders the stops, and
unpacks the legs.

```js
const trip = await roads.itinerary({ stops });
// trip.order        stop indices in visit order
// trip.legs         one full route result per leg, with geometry and steps
// trip.totalSeconds, trip.totalMeters, trip.stats
```

Ordering is **exact** — Held-Karp dynamic programming — up to 10 interior
stops, and falls back to 2-opt beyond that. Three end modes are mutually
exclusive:

| Call | Meaning |
| --- | --- |
| *(default)* | stop 0 starts, the last stop is a fixed end |
| `roundTrip: true` | come home to stop 0 |
| `openEnd: true` | pin only the start; the optimizer picks where the run finishes |

`openEnd` exists because a courier is rarely required to end at a
particular address. Pinning whichever stop happened to be entered last
distorts the whole order, not just the tail. It also leaves N−1 stops
interior instead of N−2, so it reaches the exact-solver bound one stop
sooner.

The matrix underneath is available on its own, and it is the primitive to
reach for whenever the order is already fixed:

```js
const { seconds } = await roads.matrix({ points: stops }); // seconds[i][j]
```

k stops fetch **one shared context union** and run k multi-target forward
searches, rather than k² bidirectional routes — equality with the pairwise
result is unit-tested, and it took a 5-stop itinerary from 747 ms to
408 ms. Pass `geometry: false` when you only want numbers; path unpacking
is the expensive half.

A finished itinerary's polyline feeds `prepareRoute` directly, so
"coffee along my trip" is [corridor search](../geo-search/) over the route
you just planned.

## What comes back with a route

**Steps that match the signs.** A motorway's name is the one thing never
written on one. Steps carry the road's own number, the exit number, and
where the ramp leads — all straight off the map:

```js
route.steps[4].ref              // "40"        the road's number
route.steps[3].exitRef          // "32"        the exit number on the panel
route.steps[3].destinationRef   // "20 Est;30" where the ramp goes
route.steps[7].roundabout       // true
route.steps[7].roundaboutExit   // 2 — "take the second exit"
```

**Speed limits as a step function over distance**, not one number per
street. An autoroute drops from 100 to 70 through an interchange and
climbs back without changing its name; on the published Québec index,
3.9% of a Laval → Montréal drive reads wrong if the per-step number is
used for a sign display.

```js
route.speedLimits   // [{ atMeters: 0, limitKmh: 50 }, { atMeters: 1302, limitKmh: 70 }, …]
```

`maxspeed:conditional` windows (school zones) ride *beside* the posted
limit rather than replacing it — a static index cannot resolve a window
that depends on the clock, so the caller answers it on local device time.
Pass `departAt` and the ETA includes the time spent at the lower limit,
reported as `route.conditionalDelaySeconds`.

**Continuing a drive, not reversing it.** Both directions of the road a
vehicle is on snap equally well, so a reroute without a heading can seed
the opposing edge for free and hand back a route that starts by driving
back the way the driver came. `fromHeading` charges the misaligned
candidates so turning around wins only when it saves more than it costs.

## Time of day, live traffic, alternatives

An index built with `timeBuckets` gets one exact overlay set per bucket:
per-bucket results equal a reference Dijkstra on the scaled graph, and a
query selects a bucket explicitly or by departure time.

```js
const rush = await roads.route({ from, to, departureTime: "2026-08-04T08:15" });
// rush.bucket === "peak"
```

Live data enters through a generic provider contract — a CDN-published
delta sidecar, a municipal feed, a private fleet feed, or
[PulseMesh](../pulsemesh/), which is the peer-to-peer implementation of it:

```js
const result = await roads.route({ from, to, live: provider });
// result.adjustedSeconds — the ETA under the live metric
// result.live — { provider, states, applied, error }
```

The search itself runs under the live metric: leaves referenced by live
states join the query's context set and overlay shortcuts through
live-adjusted subtrees are suppressed, so closures and jams are exact
wherever the provider reports. The far field falls back to the static
metric, which is where live data is sparse anyway. A provider failure or
an unknown graph epoch degrades to the static route — by contract, and
under test.

`alternatives: k` computes diverging routes by penalized re-search over
the objects already fetched, at no extra request, and `liveWeights`
re-ranks that candidate set with fresh per-edge factors. Every edge
exposes a stable physical `segment` id, so one report fans out to every
junction-expanded approach copy of that road.

## Across regions

`openRouteGraphUrl` opens one regional graph. `openRouteCatalogUrl` opens
a discovery root and federates several — without a routing service and
without a continent-sized download:

```js
import { openRouteCatalogUrl } from "rangefind/route";

const roads = await openRouteCatalogUrl("https://osm.rangefind.dev/routes/catalog.json", { profile: "car" });
const route = await roads.route({
  from: { lat: 45.5019, lon: -73.5674 },  // Montréal
  to:   { lat: 43.6532, lon: -79.3832 }   // Toronto
});
// route.federated === true; route.regions: ["quebec", "ontario"]
```

Regions are joined only at **proven portals**: a transition is legal only
when two independently built extracts contain the same original OSM node
id at the same coordinate. Coverage bounding boxes find candidates, not
connections; nearby roads are never joined by proximity, and a missing
shared id returns `RANGEFIND_ROUTE_REGIONS_DISCONNECTED` rather than
inventing a bridge. Portal data is an immutable range pack, so a query
fetches the two borders it needs and never the rest of the planet.

Sharding *within* a region is a different and stronger guarantee: shards
share one top overlay, so sharded and monolithic builds return identical
results by construction.

## Measured

Québec extract, car profile with full turn-cost junction expansion:
1,507,941 expanded nodes / 4,063,393 turn-priced edges, 46 s extraction,
36 s build, **82 MB on disk** with two time buckets. Cold numbers assume a
simulated 25 ms per-request RTT.

| Bucket | Cold | Requests | Fetched | +Geometry | Warm |
| --- | --- | --- | --- | --- | --- |
| local &lt;10 km | 65 ms | 12 | 3.3 MB | +66 ms | 16 ms |
| regional 10–100 km | 70 ms | 15 | 3.6 MB | +135 ms | 22 ms |
| long &gt;100 km | 76 ms | 14 | 3.6 MB | +202 ms | 29 ms |

A 5-stop / 525 km itinerary — matrix, Held-Karp ordering, and four
unpacked legs — completes in about 410 ms cold.

Exactness is the headline, not the latency: 40/40 random pairs are
**exactly equal** to a reference full-graph Dijkstra on the base metric
and 15/15 on the peak bucket, with turn restrictions, junction penalties,
and turn costs included; 60/60 sharded routes are identical to monolithic
ones. For scale, an in-memory full-graph Dijkstra answers in ~57 ms but
needs the entire 70 MB graph resident; the static engine answers from
~1.4 MB of range reads with nothing resident at all.

## Errors and limits

- `route`/`matrix`/`itinerary` throw coded errors:
  `RANGEFIND_ROUTE_BAD_POINT` for non-finite or out-of-range coordinates,
  `RANGEFIND_ROUTE_SNAP_TOO_FAR` when the nearest road exceeds
  `maxSnapMeters` (default 250). Results report snapped coordinates and
  snap distances.
- Profiles are separate builds — `car`, `bike`, and `foot` each get their
  own index directory; there is no cross-profile object sharing.
- Turn restrictions are enforced exactly (via-node and via-way, including
  multi-via-way chains) and apply to the car profile. Turn costs are
  heuristic bands by turn geometry, not measured per-junction delays.
- A reader requires the exact format version it was built for. An index is
  derived data and reproducing it is two commands, so a format change is a
  rebuild, not a compatibility shim.
- `liveWeights` re-ranks computed candidates; it does not re-run the
  search. Use `live` for that.
