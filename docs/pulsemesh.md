# PulseMesh: peer-to-peer live traffic for the route graph

PulseMesh is a proposed ephemeral peer-to-peer layer that distributes
live traffic observations for rangefind's static route graph without a
traffic server and without collecting user trajectories. It is the
first — but deliberately not the only — implementation of the engine's
generic [live-traffic provider contract](route-graph.md#live-traffic-providers):
a CDN-published delta sidecar, a municipal feed, or a private fleet feed
can implement the same interface, and the router does not care which is
plugged in.

This document is rangefind's adaptation of an externally drafted concept
("RangeFind PulseMesh", 2026-08). The architecture survives largely
intact; the sections below note where this version deliberately differs
and why. Status: **implemented** — the consumption side (provider
contract, segment identity, live-metric search), the full protocol v1
traffic channel (`src/pulsemesh/`), the js-libp2p wire transport and
keeper (`src/pulsemesh/libp2p.js`, `scripts/pulsemesh_keeper.mjs`), and
the phase-2 simulation are done, tested, and measured; all five protocol
milestones (M1–M5) are complete. The wire-level implementation
specification — byte layouts, topic grammar, bin tables, the
deterministic aggregation algorithm, validation rules, state machines,
and test vectors — is
[pulsemesh-protocol.md](pulsemesh-protocol.md); measured results are in
[pulsemesh-benchmarks.md](pulsemesh-benchmarks.md).

The anonymous traffic layer described here is one of **two channels**
sharing the same transport, cell vocabulary, segment identity and epoch
discipline. The second — authenticated tracking of a single vehicle for
a bounded audience, for school-bus and delivery use cases — inverts
almost every property below and is specified separately in
[pulsemesh-threads.md](pulsemesh-threads.md). See
[Second channel: threads](#second-channel-threads).

## Design rules

1. **The static index is the shared truth.** Map packs, the route graph,
   and segment identity remain immutable, content-addressed CDN assets.
   PulseMesh carries only ephemeral state about them, bound to a graph
   epoch, and disappears entirely when unavailable — the router degrades
   to the static metric by contract.
2. **Local computation first.** GPS processing, map matching, route
   calculation, corridor selection, and rerouting happen on the device.
   Origin, destination, precise coordinates, and complete routes never
   enter a network request.
3. **Gossip summaries, fetch what matters.** Peers discover each other
   by coarse geographic zone, advertise small digests, and transfer only
   changed nearby cell snapshots.
4. **Leaderless and ephemeral.** Contributions are immutable, deduped by
   one-use ids, merged by set union (CRDT-style convergence), and
   expired by receiver-enforced TTLs. A departing node removes nothing;
   an empty region simply forgets.
5. **Honest privacy language.** Transport encryption is not anonymity.
   Privacy comes from data minimization (what is sent) and query
   obfuscation (padding, decoys, splitting), not from claims about
   relays.

## Deltas from the original concept

These are the load-bearing changes; everything else (TTL tables,
weighted-median aggregation, anti-entropy, digest-before-download,
validation lists, two-hop forwarding option) is adopted as drafted.

### 1. Rangefind XYZ cells instead of H3

The original used H3 resolutions 6 and 8. Rangefind already ships a
deterministic, dependency-free hierarchical cell system — Web-Mercator
XYZ tiles with parent/neighbor/rasterize operations (`src/geo_cells.js`,
used by the geo lanes and corridor search). PulseMesh uses the same
vocabulary:

| Purpose | XYZ zoom | Approx. size at 45°N | H3 analogue |
| --- | --- | --- | --- |
| Discovery / privacy zone | z9 | ~55 km | r6 |
| Detailed traffic cell | z15 | ~0.9 km | r8 |

Corridors rasterize to cells with the existing `geoCellsForRoute`
machinery. Nothing in the design needs hexagons; one cell vocabulary
across the whole product beats two.

### 2. Physical segment identity from the geometry dedup

The original assumed a per-segment catalogue. Rangefind's turn-cost
junction expansion creates several index edges per physical road edge
(one per approach), so a naive edge id would under-apply a jam to one
approach. The index already contains the right identity for free: every
approach copy of a physical edge references its leaf's deduplicated
canonical polyline. The segment id is therefore

```
segment = "<leafCell>/<polylineIndex>/<directionFlag>"
```

valid for exactly one graph epoch (`root.sourceHash`), exposed by the
engine on `route().edges[].segment` and `snap()` matches, and consumed
by `route({ live })` with automatic fan-out to all approach copies.
Contributions and aggregates use this id; there is no separate segment
catalogue artifact.

### 3. Corridor-scoped exactness, stated plainly

The original's pseudocode assumed a local router with pluggable per-edge
costs. For a CRP/MLD engine that is the hard part: overlay shortcuts are
precomputed on the static metric. The implemented behavior is:

- Leaves referenced by live states join the query's context set (capped),
  and overlay shortcuts through live-adjusted subtrees are suppressed,
  forcing the search to descend to the adjusted raw edges. **Closures
  and jams are exact under the live metric wherever states exist**;
  unaffected sibling cells keep their shortcuts.
- The far field runs on the static (or time-bucket) metric — consistent
  with the concept's own detail horizon (detailed traffic for the next
  20–40 km, coarse beyond).
- Clique unpacking and the reported `seconds` stay on the exact static
  metric; `adjustedSeconds` is the live estimate.

No global pluggable-cost router is claimed or needed.

### 4. Platform reality: browsers read, apps write

Backgrounded browser tabs lose `watchPosition`; screen-off ends
reporting. Web clients are therefore treated as **consumers** (and
foreground contributors at best). The realistic sustained contributor is
the mobile runtime (`rangefind/mobile` — React Native/Hermes hosts with
background location permission). The contribution pipeline's primitive
is already in the engine: `snap()` map-matches a GPS fix to a segment id
with distance and match quality.

### 5. The credential service is named as a concession

Blind-signed contribution tokens (one token = one contribution,
unlinkable to issuance) are the recommended Sybil control. That issuer
is the first non-static, per-user server in the rangefind story. It sees
that an eligible client obtained a token batch — never which segment a
token was spent on — but it exists, and product copy must say so. The
fallback stack (proof-of-work, rate limits, corroboration minimums,
plausibility, peer scoring) trades weaker Sybil resistance for zero new
services.

## Architecture summary

```
Static CDN (immutable): route graph, segment identity, epoch, signed
                        mesh-bootstrap.json
        │
Connectivity (federated, connectivity-only): bootstrap, rendezvous,
                        Circuit Relay v2; WebRTC / WebTransport / WSS
        │
Control plane: GossipSub topics per (epoch, z9 zone, 5-min rotation,
                        shard 0..15) carrying small digests
        │
Data plane: negotiated request/response streams — digest-before-
                        snapshot, padded/shuffled/split cell batches
        │
Local state: in-memory TTL-bounded contribution sets, union merge,
                        weighted-median aggregates, confidence
        │
Router: engine.route({ live: pulseMeshProvider })
```

### Contribution (what a device emits)

At most one record per emission, gated by the reticent profile above
(the raw `EMIT_INTERVAL` cadence below is the ceiling, not the rate):

```
{ epoch, segment, timeBucket(15s), speedBin(5 km/h),
  qualityBin, reportId(one-use), expiresAt, proof }
```

Prohibited by construction: coordinates, trajectories, accounts, stable
keys, previous/next segment, precise timestamps. Suppressed when match
quality is poor, motion is implausible, or the device is stationary
off-road.

### Aggregation (what peers compute)

Any subscriber reconstructs the same aggregate: union the contribution
set, weight by match quality × freshness (`exp(-age/60)`) × capped local
trust, take the **weighted median** per segment/direction, and publish
only above corroboration minimums (≥2 reports for a low-confidence hint,
≥3 for a normal aggregate). Confidence = source diversity × freshness ×
agreement. Congestion ratio = observed / static free-flow.

### TTLs (receiver-enforced; senders can only shorten)

| Item | Value |
| --- | --- |
| Max observation age at receipt / future skew | 45 s / 15 s |
| Contribution TTL / display maximum | 90 s / 120 s |
| Aggregation bucket / retained buckets | 15 s / 8 |
| Topic epoch / rotation overlap | 5 min / 30 s |
| Anti-entropy interval | ~10 s jittered |
| Crowd incident (score ≥ 3) | 5–30 min by type, quartered when measurably contradicted |

### Manual reports (the Waze loop)

Users tap to report crashes, hazards, road works, closures, mobile
police, ice, poor visibility. Reports carry a type from a fixed
taxonomy, a segment, and a position along it — no coordinates, no free
text, no photos, no identity. Others confirm or refute the same
`(segment, type)` key as they pass. Fixed speed cameras are absent by
design: they are permanent road features and belong in the static
index, not in an ephemeral mesh.

**A report is a claim, not a measurement**, and that distinction is the
whole design. A fabricated speed is constrained by physics — it has to
survive a weighted median and a plausibility check. A fabricated crash
is constrained by nothing. Worse, the corroboration minimum that
protects speed aggregates does nothing here: reportIds are one-use
random values with no identity behind them, so one device mints three
of them in a second and has "three distinct reports".

Two mechanisms close that, and only the second is load-bearing:

- **Score is capped by distinct delivering peers** —
  `score = min(raw, sources)`. Five records down one connection score
  1. Reaching the display threshold costs three peer identities and
  three connections, bounded further by per-peer and per-cell caps.
- **Routing penalties require independent physical corroboration.** An
  incident changes a route *only* when the same segment carries a speed
  aggregate that independently shows congestion. A fake crash on a
  flowing road changes no route, ever, because the claim must be
  ratified by measurements the attacker does not control. The incident
  channel borrows the speed channel's Sybil resistance instead of
  needing its own. Informational types — police, visibility, animals —
  never affect routing under any score, so faking them wins nothing
  beyond display noise.

The same anchor fixes the ordinary failure Waze has: an incident whose
segment is measurably flowing has its TTL cut, so a cleared crash
expires without anyone remembering to untag it.

The honest limit: **incident display is best-effort and abusable at the
margins; incident routing is not.** Blind tokens (phase 4) are what
would make the first as sound as the second.

Reporting is also the one place the reticent profile's protections are
deliberately given up — a report is a precise, voluntary "I am here",
published directly rather than through a forwarder so that peer
counting means something. Users must be told that at the moment of
reporting, and a forwarded, hint-weight path exists for those who would
rather have the privacy than the weight. Police reporting is restricted
in some jurisdictions; the signed bootstrap file carries the
deployment's suppressed-type list, and clients must honour it.

### Route query privacy

The planner computes candidate routes locally, rasterizes their buffered
corridor to z15 cells, then fetches **whole-cell** snapshots in padded
fixed-size batches (8/16/32) with ~30% decoys, shuffled order, split
across 2–3 peers so no peer sees the ordered corridor, with endpoint
neighborhoods broadened by 2–3 rings. Responses return all segments in
each cell; the device filters locally. Subscription changes are batched,
jittered, and held ~60–90 s past cell exit.

## Provider adapter (the integration boundary)

The mesh terminates in an ordinary provider:

```js
const pulseMesh = {
  name: "pulsemesh",
  async fetch({ epoch, areas, maxAgeSeconds }) {
    if (epoch !== mesh.graphEpoch) return [];
    const cells = cellsForAreas(areas, /* + corridor cache + decoys */);
    const snapshots = await mesh.querySnapshots(cells);
    return snapshots.flatMap(cellStates => cellStates.map(s => ({
      segment: s.segment,
      speedMps: s.speedBin * (5 / 3.6),
      meters: s.meters,
      confidence: s.confidenceBin / 7,
      observedAt: s.observedBucket * 15_000,
      closed: s.verifiedClosure === true,
      penaltySeconds: s.incidentPenaltySeconds
    })));
  }
};

await engine.route({ from, to, live: pulseMesh });
```

Everything mesh-specific (peers, gossip, digests, TTL state, privacy
batching) lives behind `fetch()`. The engine contributes: epoch checks,
confidence/age blending, closure semantics, corridor-exact search,
graceful degradation, and the `result.live` application report.

## Second channel: threads

The traffic channel answers *"how fast is this road"* from many
anonymous, corroborated, unlinkable reports. A second class of product
asks the opposite question — *"where is this specific vehicle, and when
does it reach my stop"* — for parents following a school bus or a
recipient following a courier. That is a single authoritative publisher,
a bounded audience, end-to-end confidentiality, and a run that ends:
every property above, inverted. It is therefore a separate record type
with a separate trust model, not a mode of this one.
[pulsemesh-threads.md](pulsemesh-threads.md) is the specification;
three things about it belong here. It is implemented and measured
([benchmarks §9c](pulsemesh-benchmarks.md)).

**What it reuses.** Transport, XYZ cells, the 5-minute topic rotation,
epoch discipline, TTL philosophy, codec conventions, and — critically —
the segment id: a thread reports position as `(segment, ratio)` from
`snap()`, the same identity the router and the traffic aggregate speak.

**Why it belongs in rangefind.** A thread carries position only. The
arrival time is computed *on the subscriber's device*, by routing from
that position to their stop under this channel's live metric
(`matrix({ points, live })` over the planned order, already shipped —
*not* `itinerary()`, which reorders stops). The publisher never
learns which stop a subscriber cares about — unlike every server-side
ETA, which necessarily knows each recipient's address. Neither channel
is compelling alone; together they replace the tracking servers these
products run today, where continuous child or courier positions sit in
a vendor database indefinitely, joined to accounts.

**A link is a key, not a location.** A thread's whole capability is one
Ed25519 public key, 45 bytes in a URL fragment — SMS, QR code, order
email. The private key signs, so no one can impersonate the publisher;
the public key derives the content key, the rotating topic tag, and the
DHT rendezvous key, so holding the link is what lets you *find*,
decrypt and verify the thread. Nothing else is in the link: no host, no
mailbox, no bootstrap address. Late joiners catch up from other
subscribers' caches, so availability scales with audience size instead
of costing a server.

**How the two touch.** Threads never feed traffic aggregates — a signed
single-source record entering a corroborated aggregate would turn a
fleet key into a traffic authority, the exact property this channel is
built not to have. But fleets should absolutely contribute as ordinary
anonymous PMC1 peers, and working out how to let a *courier* do that
safely produced the reticent profile below, which is the more important
change of the two. The subtle rule is the data-quality one — a
contributing bus must suppress reports at its own stops, or several
buses on one corridor will corroborate each other into a convincing,
entirely false standstill.

## Cadence was the mistake

The contribution rule as originally drafted — one record per device per
`EMIT_INTERVAL` while driving — produces a **trajectory**. The records
carry no identifier, but they do not need one: a walk across adjacent
segments in consecutive buckets is reconstructible from the record set
alone, by anyone, and on a quiet street it is unique. Design rule 1
says privacy comes from data minimization, and cadence is not
minimization.

The correction is that **the aggregate never wanted what cadence
collects**. Aggregation wants breadth — many vehicles over many
segments — and discards anything below the corroboration minimum
anyway. One vehicle reporting 120 consecutive segments and 120 vehicles
reporting one segment each produce identical aggregates; only the first
produces a trajectory. So cadence buys depth the aggregate cannot use
and pays for it in the one currency this design refuses to spend.

The **reticent profile** ([protocol §10.2](pulsemesh-protocol.md))
replaces cadence with four gates — suppress near your own stops and on
residential streets; emit only what *surprises* the current
expectation; only when others are already reporting the same segment;
and through forwarders rotated per record. The load-bearing one is
surprise, and it is not a privacy/utility trade:

- **Silence becomes a signal.** No live state means reality matches the
  static metric, so degrading to static is the right answer rather than
  a degradation.
- **Surprises are shared.** Everyone in a jam reports the same jam, so
  a surprise report's anonymity set is everyone in the jam. Free-flow
  reports — the ones with a small, identifying anonymity set — are
  exactly what gets suppressed.
- **A lone report was always worthless.** Below the corroboration
  minimum it produces no aggregate at all, so suppressing it costs
  nothing and removes a pure privacy liability.

This is what lets a courier contribute at all. It is a candidate
default for *every* contributor, decided by measurement rather than
argument: milestone M5 runs a trajectory-reconstruction attack against
the record set and reports recovered-route fraction per gate.

## Phased plan

1. **Loopback (no networking).** ✅ **Done.** `createStaticLiveProvider` +
   contributions synthesized from a simulated drive — validates the full
   contribute→aggregate→route loop in one process. Milestones M1–M2 of
   [pulsemesh-protocol.md](pulsemesh-protocol.md) implement it with real
   protocol bytes (`src/pulsemesh/`, `test/pulsemesh_*.test.js`).
2. **Wire prototype.** ✅ **Done** (M3 + M4). The simulation
   (`scripts/pulsemesh_sim.mjs`) measures churn, convergence time, digest
   bandwidth by density, decoy/batch overhead, malformed/replay/Sybil
   floods, and clock skew over the real modules; results and the
   resulting tunable verdicts are in
   [pulsemesh-benchmarks.md](pulsemesh-benchmarks.md). The js-libp2p
   transport (`src/pulsemesh/libp2p.js`) binds the same `MeshNode` to
   real sockets — GossipSub, sync streams, a runnable §12 keeper — with
   two OS processes converging to byte-identical digests in
   `test/pulsemesh_wire.test.js`. Browser transports (WebRTC/WSS) and
   Circuit Relay are deployment wiring on the same adapter.
3. **Corridor pilot.** A handful of commuter corridors with the mobile
   runtime as contributor and browsers as consumers; keeper nodes for
   sparsity; read-only mode default.
4. **Credential service + private forwarding.** Blind-token issuance and
   the optional two-hop reporting mode, clearly labeled in product copy.
5. **Threads.** ✅ **Done through T4.** The tracking channel is
   implemented (`src/pulsemesh/thread_*.js`, exported as
   `rangefind/pulsemesh/threads`): crypto and codecs byte-identical to
   the [threads §16](pulsemesh-threads.md) vectors, the publisher and
   subscriber, host-free catch-up, and the cross-channel contribution
   rules. The local-ETA thesis is a test rather than a claim — a
   subscriber's arrival estimate moves when a jam is injected into the
   traffic channel, on the real OSM graph, with the subscriber sending
   nothing. Its pilot (T5) is a coarse-mode school run and remains
   open.

## Acceptance criteria (inherited, still binding)

- A route query never contains origin, destination, or an ordered full
  corridor; contributions never contain coordinates or stable ids.
- No accepted record outlives the receiver-enforced TTL; a departing
  node removes no replicated state; an empty region forgets.
- A single peer can neither force a closure nor a reliable estimate,
  **nor move a route with a manual report** — incident penalties are
  gated on independent speed measurements.
- **A contributor's route is not reconstructible from its
  contributions.** Absent from the original criteria, and the omission
  is what let cadence through: the record format was checked for
  identifiers, the record *sequence* was not.
- The router always returns a route; mesh unavailability degrades to the
  static metric (enforced by the provider contract and tested).
- Direct, relayed, and two-hop modes are distinguished honestly in
  product copy; Circuit Relay is never described as anonymous.

## Open questions

- Blind-token suite and issuance UX; who operates the issuer.
- Keeper-node incentives and abuse posture in sparse regions.
- Whether z15 is the right detail cell for dense downtowns (z16 halves
  the cell; measure in phase 2).
- Cross-epoch handover during index republishes (overlap window length).
- Bandwidth ceilings per zone under stadium-exit-grade density. Partially
  measured: per-peer cost is flat in peer count but linear in contributor
  density (7.6 / 30.6 / 109 KiB/min at 5 / 20 / 60 vehicles per zone), and
  anti-entropy — not gossip — is 4–5× the bill. Shard-subset subscription
  and a churn-gated digest exchange are the levers
  ([benchmarks §5](pulsemesh-benchmarks.md)).
- Whether a scheduled fleet's anonymous contributions leak anything
  about *deviations* from its published timetable, which is the one gap
  in the public-route argument that lets fleets contribute at all
  ([threads §10](pulsemesh-threads.md#10-rules-between-the-two-channels)).
- ~~Whether the reticent profile should simply be the only profile.~~
  **Answered** ([benchmarks §8](pulsemesh-benchmarks.md)): it should be
  the default. Cadence leaks 100% of a courier's driven route at low
  density (anonymity set 1) and a median 38.5% even at 32 background
  vehicles; reticence holds at 7.7% everywhere, worst case included. The
  latency picture is mixed rather than a clean win — reticence is slower
  at density 4, a tie at 12, faster at 32 — but cadence wins on latency
  only in the regime where it also leaks the entire route, which is not
  an argument for keeping it.
- Jam onset under universal reticence: the first witness of a new jam
  has no company by construction. A surprise bypasses the company gate
  for exactly this reason. **Measured, and it is the profile's one real
  weakness**: at 4 vehicles in the zone reticence took a median 84 s
  against cadence's 31.5 s, converging by ~12 and reversing by ~32.
  Candidate fixes to measure: keeper-supplied corroboration in sparse
  regions, or relaxing `AGG_MIN_REPORTS` to the hint threshold for
  surprise-flagged records specifically.
- Whether keepers should also cache thread records opportunistically:
  same blind, TTL-bounded, padded-request store, no keys, and it would
  cover the sparse-audience case threads are weakest at.
