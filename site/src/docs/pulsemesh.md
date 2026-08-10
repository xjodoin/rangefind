---
title: PulseMesh live traffic
lede: Peer-to-peer live traffic for the route graph — no traffic server, no accounts, and no trajectory anyone could reconstruct, including us.
description: PulseMesh, rangefind's peer-to-peer live traffic channel — anonymous corroborated speed observations, weighted-median aggregation, admission bonds, the reticent contributor profile, incident reports gated on physical corroboration, and the provider adapter into route().
order: 15
---

The [routing lane](../routing/) takes live traffic through a generic
provider contract. PulseMesh is the peer-to-peer implementation of that
contract: devices observe the roads they are already driving, publish
tiny anonymous records, and every peer recomputes the same aggregate
locally. There is no traffic server, no account, and no database that
outlives a 90-second TTL.

Status: **implemented and measured**. Protocol milestones M1–M5 are
complete, the wire transport runs on js-libp2p, and both shipped hosts —
the OSM web demo and the Wayfind Android app — sit on the same session
API. The byte-level specification lives in
[`docs/pulsemesh-protocol.md`](https://github.com/xjodoin/rangefind/blob/main/docs/pulsemesh-protocol.md).

## The five rules it is built on

1. **The static index is the shared truth.** Map packs, the route graph,
   and segment identity stay immutable content-addressed assets.
   PulseMesh carries only ephemeral state *about* them, bound to a graph
   epoch, and disappears entirely when unavailable.
2. **Local computation first.** GPS processing, map matching, route
   calculation, and rerouting happen on the device. Origin, destination,
   precise coordinates, and complete routes never enter a network
   request.
3. **Gossip summaries, fetch what matters.** Peers discover each other by
   coarse geographic zone, advertise small digests, and transfer only the
   changed nearby cell snapshots.
4. **Leaderless and ephemeral.** Contributions are immutable, deduped by
   one-use ids, merged by set union, and expired by receiver-enforced
   TTLs. A departing node removes nothing; an empty region simply
   forgets.
5. **Honest privacy language.** Transport encryption is not anonymity.
   Privacy comes from data minimization and query obfuscation, not from
   claims about relays.

## What a device actually emits

```
{ epoch, segment, timeBucket(15s), speedBin(5 km/h),
  qualityBin, reportId(one-use), expiresAt }
```

Roughly 42 bytes. Prohibited **by construction**: coordinates,
trajectories, accounts, stable keys, the previous or next segment, and
precise timestamps.

`segment` is the same physical directed-segment id the router speaks —
`"<leafCell>/<polylineIndex>/<direction>"`, valid for exactly one graph
epoch. Rangefind's turn-cost expansion creates several index edges per
physical road edge, and they all share one segment id, so one report
fans out to every approach copy automatically. In the end-to-end demo,
15 aggregated segments adjust **43 edges**.

Any subscriber reconstructs the same aggregate: union the contribution
set, weight by match quality × freshness × capped local trust, take the
**weighted median** per segment and direction, and publish only above
corroboration minimums — two reports for a low-confidence hint, three
for a normal aggregate. No consensus protocol is involved; peers
converge to byte-identical digests because the arithmetic is
deterministic.

## Cadence was the mistake

The obvious contribution rule — one record per device per interval while
driving — produces a **trajectory**. The records carry no identifier and
do not need one: a walk across adjacent segments in consecutive buckets
is reconstructible from the record set alone, by anyone, and on a quiet
street it is unique.

The correction is that the aggregate never wanted what cadence collects.
One vehicle reporting 120 consecutive segments and 120 vehicles
reporting one segment each produce *identical* aggregates; only the
first produces a trajectory.

The **reticent profile** replaces cadence with four gates: suppress near
your own stops and on residential streets; emit only what *surprises*
the current expectation; only when others are already reporting the same
segment; and through rotated forwarders. Surprise is the load-bearing
one, and it is not a privacy/utility trade — silence means reality
matches the static metric, and everyone in a jam reports the same jam,
so a surprise report's anonymity set is everyone in the jam.

Measured over 12 seeds: cadence leaks **84.6–100%** of a courier's
driven route up to moderate density and a median 27% even at 32
background vehicles, while reticence holds at **7.7%** — median and
worst case — everywhere. Cadence still detects a jam faster in dense
traffic (40.5 s against 116.5 s at density 12), so the rule is cadence
only where the route is already published and correlation reveals
nothing new, reticence everywhere else. It is the mobile default.

## Sybil resistance is a toll on identities, not on records

Per-record proof-of-work charged *disposable* records while peer
identities stayed free — the wrong end. It was deleted outright and
replaced by **admission bonds**: one memory-hard proof per peer per day
(a Momentum birthday puzzle, 256 MiB table, ~2 s to mint on a desktop,
three hashes to verify), presented once per session and bound to the
connection's peer id. Records from bonded peers are proofless, so a
hazard report publishes at the tap — and a trust-ledger ban forfeits the
bond, so ban evasion finally costs something.

Two client modes follow from that. A **contributor** mints its bond once
a day in the background. A **read-only consumer** — the browser at home
that only wants to see traffic — mints nothing, joins no gossip topic,
and pulls digests and snapshots over the padded sync path from bonded
peers.

**Relaying implies validating.** GossipSub forwards a message before the
application sees it, so every subscribed topic carries a topic validator
and the full validation pipeline runs before the forward decision. A
provable lie — a speed the road class cannot carry, a segment that does
not exist — is *rejected*, which scores the peer that handed it over
down. A replay or a stale window is merely *ignored*, because rejecting
a duplicate would let an attacker degrade honest peers by replaying
their own traffic back at them. The honest cost: a peer only vouches for
what it can check against its own map, so a device holding one corridor
relays almost nothing outside it.

## Incidents are claims, not measurements

Users tap to report crashes, hazards, road works, closures, ice, poor
visibility. A fabricated speed is constrained by physics; a fabricated
crash is constrained by nothing. Two mechanisms close that, and only the
second is load-bearing:

- **Score is capped by distinct delivering peers** — five records down
  one connection score 1.
- **Routing penalties require independent physical corroboration.** An
  incident changes a route *only* when the same segment carries a speed
  aggregate that independently shows congestion. A fake crash on a
  flowing road changes no route, ever. Informational types — police,
  visibility, animals — never affect routing under any score.

The same anchor fixes the ordinary failure this class of app has: an
incident whose segment is measurably flowing has its TTL cut, so a
cleared crash expires without anyone remembering to untag it. The honest
limit is that **incident display is best-effort and abusable at the
margins; incident routing is not.**

## Wiring it into a host

`createMeshSession` is the whole surface a host needs — route under live
traffic, draw the jams and the incident pins, contribute if the user
said so, report an incident, follow the corridor being driven, and know
what the mesh is doing:

```js
import { createMeshSession } from "rangefind/pulsemesh";

const session = await createMeshSession({ engine, network, id, contribute: false });

await engine.route({ from, to, live: session.provider() });
await session.followRoute(candidates);   // subscribe zones, fill cells, warm leaves
const jams  = await session.traffic();   // polylines, speeds, confidence, level
const pins  = await session.incidents(); // scored, positioned, shown/hint tiers
await session.reportIncident({ type: 1, acknowledgedPublic: true });
```

Everything mesh-specific lives behind the provider's `fetch()`. The
engine contributes epoch checks, confidence and age blending, closure
semantics, corridor-exact search, and graceful degradation.

**The host owns the clock.** `session.start()` schedules its own
maintenance, which is right in a browser tab and wrong in an app: a
headless WebView is a hidden page and Chromium clamps a hidden page's
timers, so a self-scheduling mesh ticks once and stops. An app host skips
`start()` and calls `session.tick()` on its own cadence.

Reporting is the one place the reticent profile's protections are
deliberately given up — a report is a precise, voluntary "I am here" —
and users must be told so at the moment of reporting.

## Receiver-enforced TTLs

Senders can only shorten these; receivers enforce them.

| Item | Value |
| --- | --- |
| Max observation age at receipt / future skew | 45 s / 15 s |
| Contribution TTL / display maximum | 90 s / 120 s |
| Aggregation bucket / retained buckets | 15 s / 8 |
| Topic epoch / rotation overlap | 5 min / 30 s |
| Anti-entropy interval | ~10 s jittered |
| Crowd incident (score ≥ 3) | 5–30 min by type, quartered when contradicted |

## Browsers read, apps write

A backgrounded tab loses `watchPosition` and screen-off ends reporting,
so web clients are consumers (foreground contributors at best) and the
sustained contributor is the mobile runtime.

| Bundle | Size | What it is |
| --- | --- | --- |
| `pulsemesh.browser.js` | 106 KB | codecs, store, aggregation, validation, incidents, provider |
| `pulsemesh-threads.browser.js` | 46 KB | the [thread channel](../pulsemesh-threads/), WebCrypto only |
| `pulsemesh-libp2p.browser.js` | 2.5 MB | the libp2p transport, loaded on demand |
| `rangefind/pulsemesh/mobile` | 102 KB | the app-side contributor, transport-free |

The 24× gap between the core and the transport is why they are separate
entry points: a page that consumes live traffic from peers it already has
never loads the third bundle. On mobile, contribution is off unless the
app explicitly asks for it, emission pauses below 20% battery when not
charging, and the reticent profile is the default rather than an opt-in.

## Measured end to end

`npm run demo:pulsemesh` runs the complete loop against the real Québec
route graph with nothing stubbed — four js-libp2p peers over TCP, three
vehicles driven through a jam by the real contributor pipeline:

| Stage | Result |
| --- | --- |
| Static route | 191 edges, 80.2 km, 59.0 min |
| Records published | 45 × 50 bytes (2.2 KiB total) |
| Delivered + validated at the driver | 45 records in 66 ms |
| Aggregates | 15 segments, all corroborated at n = 3 |
| Peer agreement | byte-identical digests, no consensus protocol |
| Live states → edges adjusted | 15 → 43 |
| ETA | 59.0 min static → 75.2 min live |
| Outcome | **rerouted** |
| After contributors leave + TTL | 0 records, route returns on the static metric |

That last row is the property everything else rests on: every
contributor is gone, every record has expired, and the router still
answers — on the static metric, with the same ETA it gave before the
mesh existed.

Per-peer bandwidth is flat in peer count and linear in contributor
density: 7.6 / 30.6 / 109 KiB per minute at 5 / 20 / 60 vehicles per
zone, with anti-entropy rather than gossip accounting for 4–5× of the
bill.

## Honest limits

- Incident *display* is abusable at the margins; incident *routing* is
  gated on measurements an attacker does not control.
- Admission bonds are a toll, not a cap: one core mints roughly 66k bonds
  a day, so corroboration remains the real defense.
- A ban forfeits a bond at the receiver that saw the evidence — the trust
  ledger is local by design. Ban propagation exists as corroborated
  testimony that down-weights but never revokes.
- Blind-signed contribution tokens are the planned path to per-identity
  rate limiting across sessions. That issuer would be the first
  non-static, per-user server in the rangefind story, and product copy
  has to say so.
- Circuit Relay is connectivity, never anonymity, and nothing here
  describes it otherwise.

Full rationale, deltas from the original concept, and the rejected
alternatives (RLN, memory-hard per-record proof-of-work, a kinematic
admission tier) are in
[`docs/pulsemesh.md`](https://github.com/xjodoin/rangefind/blob/main/docs/pulsemesh.md);
every number above is reproducible from
[`docs/pulsemesh-benchmarks.md`](https://github.com/xjodoin/rangefind/blob/main/docs/pulsemesh-benchmarks.md).
