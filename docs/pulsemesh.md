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
keeper (`src/pulsemesh/libp2p.js`, `scripts/pulsemesh_keeper.mjs`, with
the fleet-seed admission and bridge in `src/pulsemesh/bridge.js`), and
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

*Implemented.* `rangefind/pulsemesh` bundles for the browser at 141 KB
with the transport split into a separate 2.5 MB entry point loaded on
demand; `rangefind/pulsemesh/mobile` is the app-side surface, wired into
the Android app — live traffic on the map, incident reporting, thread
sharing and the settings that gate them. Measured in
[benchmarks §9d and §9e](pulsemesh-benchmarks.md).

The split holds in practice: the web demo joins the real mesh
**read-only** (§11.6) — no bond, no gossip membership, everything pulled
over the padded sync path — while the phone is the side that can
contribute, and does so only when its owner turns it on.

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
fallback stack (identity bonds, rate limits, corroboration minimums,
plausibility, peer scoring) trades weaker Sybil resistance for zero new
services.

**RLN was measured as an alternative and rejected**
([benchmarks §2b](pulsemesh-benchmarks.md)). Rate-limiting nullifiers
looked like they might supply anonymous rate limiting with no issuer at
all. Proving is genuinely 2.5× cheaper than proof-of-work (235 ms
against that run's 579 ms PoW sample), but verification is 9,130× more expensive — turning the
flood defence from 1.45 M rejected records per second into 159 — the
proof is 1,250 bytes against a 96-byte record cap, and the hypothesis
itself is false: verification checks a Merkle root, so a maintained
membership registry is required. RLN moves the issuer onto a registry or
a chain rather than removing it. Blind tokens remain the better route to
per-identity rate limiting, and the concession above stands as written.

**Memory-hard proof-of-work was measured as an alternative and rejected**
([benchmarks §14](pulsemesh-benchmarks.md)). Proof-of-work's real
weakness here is not its cost — under the reticent profile it amortises
to 1.4–2.5% of one core — but that SHA-256 parallelises perfectly, so a
farm scales linearly while the honest phone pays retail. A Momentum-style
birthday puzzle should have capped that, being memory-bound to solve and
three hashes to verify. The verification asymmetry held (0.9 µs at every
size); the Sybil cap did not. At the table size that matches the current
puzzle's cost, 64 MiB, a single consumer GPU still fits 384 concurrent
solvers and a rented box 4,096 — and at 4 MiB the table lives in cache,
where the measured thread scale-up is *better* than SHA-256's. Memory
binds only when core count exceeds RAM÷table, which is true of a GPU's
shader array and false of any CPU farm an attacker would rent.

A kinematic admission tier — pricing physically plausible movement over
the route graph rather than CPU — was designed and abandoned before it
was built, for a reason worth recording: PMC1 carries no contributor
identity, so successive records are unlinkable by construction, and
trajectory checking needs exactly the linkage §10.2 exists to prevent.
The static half of the idea is already validation rules 10–12, and the
road graph is public, so map plausibility costs a remote attacker
nothing.

What the search did find is that mining was blocking the caller's
thread — ~40 s on a phone at the incident difficulty, exactly when the
driver has just tapped "hazard ahead". Mining is now sliced
(`minePowChunked`), which costs under 1% of throughput and cuts the
longest unbroken block from seconds to ~13 ms.

**Identity bonds (§5.4) came out of taking that refutation seriously, and then replaced proof-of-work entirely**
([benchmarks §14.5](pulsemesh-benchmarks.md)). The deeper misalignment
was that every defense punishes a peer — trust ledger, rate limits,
`min(raw, sources)` — while proof-of-work charged disposable records,
and peer identities were free. A bond flips the charge to the identity:
one memory-hard admission proof per peer per day (Momentum birthday
puzzle, 256 MiB table at the default, ~2 s desktop mint, three-hash
verification), presented once per session and bound to the connection's
peerId. Records from bonded peers are proofless (proofType 3): the
42-byte record returns, contributions and hazard reports publish at the
tap, and — the structural win — a trust-ledger ban forfeits the bond,
so ban evasion finally costs something. And the memory-hardness that
failed at record cadence works at identity cadence: the once-daily table
can be 256 MiB, where a 24 GiB GPU fits 96 concurrent solvers instead of
the ~98,000 that cache-sized per-record tables allowed. Implemented and
wire-tested — and since this protocol has no deployed legacy to carry,
per-record proof-of-work was then deleted outright rather than kept as a
fallback: proofType 1 is burned, every wire record is proofType 3, and
the §3 PoW constants are gone. Two client modes remain. A *contributor*
mints its bond once a day in the background. A *read-only consumer*
(§11.6) — the browser at home that only wants to see traffic — mints
nothing and joins no gossip topic: it pulls digests, cells and snapshots
over the padded sync path from bonded peers on its maintenance tick,
which also keeps it from becoming an unbonded relay that rule 5 would
have everyone ignore. Blind tokens (phase 4) remain the path to
per-identity rate limiting *across* sessions; bonds deliver the
per-session version with zero new services.

Three corrections and one addition followed from re-examining the
claims (2026-08-06). Corrected: the Sybil bound is throughput, not
capacity — one core mints ~66k bonds/day, so admission is a toll and
corroboration remains the defense; "a ban forfeits the bond" holds at
the receiver that saw the evidence, because the trust ledger is local
by design; and bond buckets originally all rolled over at the same UTC
instant — now staggered by a per-peer phase hash. Added: **ban
propagation** ([protocol §8.4](pulsemesh-protocol.md)) as corroborated
testimony, engineered so it cannot become a defamation primitive.
First-hand provable evidence (rules 10–12, checkable against anyone's
own static map data) forfeits a bond and emits a 49-byte PMX1 naming
the peer only by hash. Remote receivers count testimony from bonded
deliverers, and at three distinct corroborators apply one bounded
trust penalty — down-weighting, never revoking, recoverable by decay.
The arithmetic is the contract: testimony alone leaves an honest peer
heard; testimony plus one first-hand violation forfeits. Three
colluding mints can make a mesh temporarily distrust an honest peer;
silencing it requires catching it lying about the map, which an honest
peer never does.

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

### Relaying implies validating

A peer's admission bond is what vouches for the proofless records it
hands over (§5.4, rule 5) — so the peer must actually have checked them.
GossipSub does not make that automatic: it forwards a message to its mesh
peers the moment it arrives, before the application sees it. Validating
on delivery therefore means every honest relay spends its bond on bytes
it never looked at, and anyone can launder invalid records through one.

The transport closes it rather than the application: every topic the
library subscribes to carries a GossipSub **topic validator**, so the
whole validation pipeline runs before the forward decision, exactly once
per message. A provable lie — a speed the road class cannot carry, a
segment that does not exist — is *rejected*, which scores the peer that
handed it over down in GossipSub's own accounting. Everything else that
must not travel is *ignored*: a replay, a stale window, a record for a
zone we are not in. Ignoring costs the sender nothing, which is the
point — rejecting a duplicate would let an attacker degrade honest peers
by replaying their own traffic back at them.

The honest cost: **a peer only vouches for what it can check.** The rules
that catch implausible records need the local map for the area. A peer
that does not hold that leaf still keeps the record — its own risk, and
plausibility was always enforced probabilistically by whoever does hold
the cell — but does not pass it on. So a device with a sparse map relays
less than one with a full one, and a browser holding one corridor relays
almost nothing outside it. At the limit, a node wired with no static map
at all forwards nothing — a pure sink, which is the honest description of
what its vouching was ever worth. Keepers, which hold their pinned zones in
full, carry the fan-out; that is what keepers are for.

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

### The session (what a host actually wires)

A host wants six things and nothing else: route under live traffic, see
where the jams and incidents are, contribute if the user said so, report
an incident, follow the corridor being driven, and know what the mesh is
doing. `createMeshSession({ engine, network, … })` in
[`src/pulsemesh/session.js`](../src/pulsemesh/session.js) is that
surface, and both shipped hosts — the Android app and the OSM web demo —
sit on it:

```js
const session = await createMeshSession({ engine, network, id, contribute: false });
await engine.route({ from, to, live: session.provider() });
await session.followRoute(candidates);   // subscribe zones, fill cells, warm leaves
const jams = await session.traffic();    // polylines, speeds, confidence, level
const pins = await session.incidents();  // scored, positioned, tiered shown/hint
await session.reportIncident({ type: 1, acknowledgedPublic: true });
```

Two things it gets right that a host writing its own wiring got wrong,
twice:

- **The static index answers rules 10–12.** `engine.cellFacts(leaf)`
  returns the leaf's real polyline count, per-geomRef road class, length
  and free-flow speed, warmed for the leaves the session is listening to.
  The stand-in it replaces (`polylineCount: 1 << 20`,
  `classOf: () => "secondary"`) made rule 11 unfireable and capped
  *every* road at 100 km/h, so an honest motorway contribution failed
  rule 10 and cost the peer that delivered it trust. Not holding a leaf
  is a legal state (§6 says 10–12 are probabilistic across peers);
  inventing its contents is not.
- **Contributions carry a real length.** `metersOf` comes from the same
  edge the router costed, so `record.meters` cannot disagree with a
  neighbour's rule 10, and a receiver can turn a speed bin back into a
  time. With 0 there, the familiar symptom is `states: 15, applied: 0`.

The session also holds the cell derivation every peer must agree on: the
z15 cell of the **leaf's bbox centre**. §2.3 words it as the cell of the
snapped coordinate, but a wire record carries `leafCell/geomRef` and no
coordinate, so a receiver has to derive the cell from the static index
alone — and the leaf bbox is in the route-graph root, which every peer
has, while the segment's geometry is not. It is coarser than the spec
reads and it is what interoperates.

**The host owns the clock.** `session.start()` schedules its own
maintenance, which is right in a browser tab and wrong in an app: a
headless WebView is a hidden page and Chromium clamps a hidden page's
timers, so a mesh that scheduled its own anti-entropy ticked once and
then stopped — measured on a Pixel emulator. An app host skips `start()`
and calls `session.tick()` on its own cadence.

### What the two shipped hosts do with it

| | Web demo (`examples/osm-geo`) | Android (Wayfind) |
|---|---|---|
| Role | consumer; read-only on the wire (§11.6) | consumer, contributor when asked |
| Transport | loopback with simulated vehicles, or libp2p/WSS to a keeper via `?keeper=` | loopback demo transport, or libp2p/WSS via `BuildConfig.PULSEMESH_BOOTSTRAP` |
| Clock | `session.start()` | Kotlin polls `tickMesh()` |
| Draws | traffic polylines, incident pins, followed vehicle | same three, as MapLibre layers |
| Reports | disclosure dialog → `reportIncident` | disclosure sheet → `reportIncident` |
| Threads | share link + follow field | share sheet + follow |

Both label the mode they are in. A host running simulated vehicles
because no keeper is configured must not look like a host on a real
mesh, and both say which on screen. Android goes one step further,
because a phone is where the claim is acted on: the simulation is a
switch the driver owns, it can be turned off with live traffic left up
(the store keeps what it already validated — the source going away is not
grounds for unlearning it), and the map itself carries a label for as long
as anything invented is being drawn.

### The thread channel, wired

`createThreadChannel({ node, network, engine, host })` in
[`thread_session.js`](../src/pulsemesh/thread_session.js) is the other
half: publishers, follows, and the three things between the sealing code
and a transport that every host would otherwise have written itself —
derive the topic from the rotating tag, follow the 5-minute rotation, and
route arriving payloads to the right subscriber. It rides the same
MeshNode: the traffic channel parses an incoming topic, finds §5.2's
reserved `t` namespace, and hands it over through `onOtherTopic`.

What it adds beyond delivery:

- **Catch-up (§5.5, §8 path 2), which is not an optimisation.** A phone
  that slept for four minutes has a hole in a thread, and the only
  alternative to filling it from other peers is a mailbox host — the
  exact server this design exists to avoid. Every follower caches the
  sealed bytes it sees and answers PMR1 on
  `/rangefind/pulsemesh/1/thread` for the tags it holds, so
  **availability scales with audience size**. A relay cannot open what it
  caches and does not need to be trusted: records travel verbatim, so
  tampering fails the AEAD, forgery fails the signature and replay fails
  `seq` — at the joiner, which validates everything itself. Requests are
  padded to 4/8/16 tags with CSPRNG decoys, which is free here because a
  tag is indistinguishable from random; a responder answers unknown tags
  with count 0 so a prober cannot learn which threads it follows.
- **Discovery (§4.2)**, when the host has a DHT: publishers *and*
  followers advertise the rendezvous key, so a bigger audience makes a
  thread easier to join rather than more expensive.
- **A fleet seed (threads §20.10), for when there is nothing to dial.**
  Discovery finds a *thread*; it does not find the *mesh*. A capability
  is a key, not a location (§5.4), so a cold device holding a perfect
  follow link and no `bootstrapPeers` connects to nothing. At fleet scale
  the answer is not a DHT — ten drivers who can all dial one machine have
  a working mesh — it is one keeper the operator runs:
  `pulsemesh_keeper.mjs --seed-card` prints its dialable multiaddrs, a
  `wayfind://seed#…` card (PMH1) and a scannable QR of it on **stderr**,
  leaving the stdout JSON-lines contract intact. The address then travels
  two ways and deliberately not a third: signed **inside the sealed
  ticket**, so it reaches the awarded device and no channel it crossed;
  optionally as a `?b=…` **query** hint on a public link, never the
  fragment, which stays byte-identical; and **never in a job offer**,
  which is broadcast to strangers and would publish a small operator's
  own machine to everyone who reads an ad. After first contact
  `network.knownPeers()` reports what the host is actually connected to
  and `createPulseMeshHost({ rememberedPeers })` dials it back, so the
  seed is a single point of failure for *joining* and for nothing else.
  The library reports and accepts; persisting is the host's business.
- **§10 rule 4 surfaced, not buried.** `handleFix` returns
  `contributeTraffic`, and a host that ignores it lets a dwelling vehicle
  publish a standstill onto a road that is flowing.

Both hosts carry the whole feature: mode choice (§11 is a harm decision,
so it is asked — coarse withholds position entirely), start and stop,
a followed-drive card whose sentence comes from the subscriber's own
`status()`, and an arrival computed locally from the broadcast position
under this device's live metric. Android also takes the link as a deep
link, so being sent one opens the app already following.

One trap worth naming: §12's claim is (live position?) × (live traffic?),
and the subscriber only knows the first half. Pass `hasTraffic` or the
card will say "static-metric ETA" directly above a live-traffic one —
the self-contradiction §12 exists to prevent.

## A fleet's island, and joining it to the rest (§12.1)

A seed card gives a fleet a mesh of its own. On its own that is an
**island**: the drivers see each other, and nothing they learn reaches
anybody else, nor anybody else's them. Two flags decide whether it stays
one, and they are independent axes:

```
node scripts/pulsemesh_keeper.mjs --graph=<dir> --epoch=<64 hex> \
     --listen=/ip4/0.0.0.0/tcp/4001 --zones=144/180 --seed-card \
     --admit=connected --bridge=both --bootstrap=/ip4/…/p2p/12D3Koo…
```

- `--admit` — whether this seed vouches for **its own devices**.
- `--bridge=off|in|both` — what crosses between them and the rest.

Which address means what stops being obvious once there are two sides, so
the keeper says it in `--help` and again here: **`--listen` is my
island** — the address a driver's phone dials, and the one `--seed-card`
prints — while **`--bootstrap` is the wider network**, dialled by a
second, listen-less host inside the same process. A fleet seed never
dials a driver; drivers arrive, carrying an address a sealed ticket
brought them.

### Why drivers do not mint bonds, and what admits them instead

A §5.4 bond is a 256 MiB memory-hard solve. It is affordable because it
is once per peer per day, and the cheap phone-sized variant was
implemented, measured and **refuted** as a Sybil defence (benchmarks
§14) — so "every courier phone mints one" was never on the table. The
codebase already had the answer for exactly this shape: §16.3's LoRa
bridge admits radio senders who minted nothing, because a *different*
admission applies to them and the security boundary is the bridge.

**Here, being directly connected to the seed is the admission.** Reaching
a fleet seed requires its address; that address travels only inside
sealed tickets, to devices the dispatcher awarded a job to. So the set of
peers able to dial the seed already *is* the fleet — enforced by the
seal, not by a roster the keeper would have to hold and keep current.
Fleets that want the stricter thing can pass `--admit=<peerId>,…`.

What this deliberately does **not** build is a binding between a driver's
device card and their libp2p peerId. It would be easy, and it would hand
the operator the one thing the contributor pipeline spends §10.2
preventing: the ability to join anonymous traffic records to a named
driver. The seal already establishes who may connect. A roster would add
de-anonymisation and nothing else.

### The fleet stakes its own bond on its drivers

An outward record is republished **as the seed's own**. Remote peers
therefore penalize the seed for what its drivers say, and at the trust
floor they forfeit it — at which point the whole fleet loses reach, not
one phone. That is the intended arrangement, not a wart: policing lands
on the party that can actually do it, the operator who knows which van is
which, instead of on strangers who can only see a peer id.

Inward, the seed validates every arrival against its own map before it
enters its store, and only what entered the store is eligible to cross —
so acceptance *is* the gate, and no second validation pass exists to
disagree with the first. A driver who publishes what the map refuses gets
the ordinary §8.4 treatment: two provable rule 10–12 failures floor its
trust, which revokes the admission, refuses re-admission for the bucket,
and gossips PMX1 testimony. Unlike the radio bridge, no bespoke strike
counter is needed — on IP the delivering peer is an authenticated peerId,
so the machinery that already existed applies.

### `in` is read-only upstream and bonded downward

§11.6 read-only means no bond and no gossip topics, which is the right
posture for a fleet that wants the world's traffic without publishing its
own. But a seed still has to serve its island, and read-only is a
whole-node property. The two-host split resolves it exactly: the upstream
node is read-only (mints nothing, joins no upstream topic, pulls
PMG1→PMQ1→PMS1 on tick), while the island node stays an ordinary bonded
peer that publishes and vouches downward. "Read-only" ends up naming a
*direction*, because a direction is what each node is.

The same split is what makes the bridge honest rather than incidental.
GossipSub forwards a message to its mesh peers **on receipt**, before the
application handler runs — so a node that validates on delivery relays
records it never checked, and rule 5 on the far side is satisfied by
bytes nobody looked at. That was true of every keeper from the beginning;
fleet seeds only turned it into an incentive problem. It is now closed
where it belongs, in the transport: every topic this library subscribes
to carries a GossipSub **topic validator**, so the §6 pipeline runs
before the forward decision and relaying implies validating everywhere
(§5.1).

The two-host split stays anyway. It makes "validate, then vouch" a
property of the transport rather than of one process staying correct, and
it is what lets `in` and `both` be *directions* at all. The keeper still
refuses the one-host laundering shape (`--admit` with `--bootstrap` and
no `--bridge`) — as defence in depth now, not as the only defence — and
says so.

### Pin the zones

`--zones` is not optional advice for a bridged seed. Bridging is confined
to the pinned zones in both directions, so a depot pins the territory its
drivers actually work; an unzoned bridge on a planet-scale mesh pulls the
planet through the depot's uplink and contributes a continent's worth of
nothing back.

### The honest costs

- **A bridged seed makes the fleet's territory weakly visible.** Whoever
  peers with it can see which zones it publishes into and roughly how
  much. It reveals no **driver**, no **vehicle** and no **job**: a
  traffic record carries no identity (`reportId` is fresh random per
  record) and thread contents are sealed to keys the mesh does not hold.
  "This operator works the south shore" is inferable; "this van was at
  this address" is not.
- **The seed is a single point of failure for *joining*, not for an
  established connection.** A mesh that has formed keeps working when the
  seed goes down — gossip, catch-up and the ETA are peer-to-peer, and
  `rememberedPeers` means a device that has connected once need not come
  back through it. What breaks is a *cold* device on a day the seed is
  down.
- **Admission is only as good as the seal.** A leaked ticket is a device
  that can contribute under the fleet's bond until it is caught by rule
  10–12 or removed from an allowlist. That is the same exposure the
  ticket already carried (it can publish as that vehicle, §20.5); the
  bridge does not widen it, and the allowlist narrows it for fleets that
  want the trade.

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

**A recurring route splits identity from authority.** A school bus keeps
one plan for a term and changes driver some days, so the run that
*publishes* it cannot be the thing that *identifies* it — rotating the
key for a new driver would rotate every parent's link. So the root key
stays the identity (topic tag, content key, the 45 bytes), and a
short-lived day key derived from it and certified by it carries the
publish authority. The driver's phone holds one day of authority and
never the root; the certificate rides the run's own topic, so host-free
catch-up delivers it to a late joiner for free; and a subscriber refuses
any certificate valid for more than 48 hours, so "short-lived" is a
property the verifier enforces rather than one the depot promises.
Details in [threads §21](pulsemesh-threads.md).

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
   nothing. Recurring routes (threads §21) are implemented too: a
   term-long link that survives a daily change of driver, verified
   through a root-signed day certificate carried on the run's own topic.
   Its pilot (T5) is a coarse-mode school run and remains open.

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
  **Answered — and the answer is no** ([benchmarks §8](pulsemesh-benchmarks.md),
  medians over 12 seeds). Privacy is decisive: cadence leaks 84.6–100% of
  a courier's driven route up to moderate density (anonymity set ~1) and
  a median 27% even at 32 background vehicles, while reticence holds at
  7.7% — median *and* worst case — everywhere. But cadence's latency
  argument survives: at density 12 it detects a jam in 40.5 s against
  reticence's 116.5 s, with every run detecting under both. So the right
  axis is the one [threads §10 rule 3](pulsemesh-threads.md) already
  named — cadence where the route is published and correlation reveals
  nothing new, reticence mandatory where it is not. An earlier five-seed
  run, measured against a stricter detection threshold than the router
  itself uses, pointed the other way; the fuller data does not.
- Jam onset under universal reticence: the first witness of a new jam
  has no company by construction. A surprise bypasses the company gate
  for exactly this reason. **Measured, and it is a reliability gap rather
  than a latency one** — the distinction a single-threshold measurement
  could not see. At 4 vehicles in the zone reticence detects *faster when
  it works* (44 s vs cadence's 214 s) but works less often (6 runs in 12
  against 9 in 12). Above ~12 vehicles both always detect and cadence is
  the quicker of the two. Candidate fixes still worth measuring:
  keeper-supplied corroboration in sparse regions, and letting a
  surprise-flagged record publish at the hint threshold rather than
  waiting for a third witness.

  One incidental result belongs here: for reticence the hint (n = 2) and
  full-confidence (n = 3) thresholds are always reached together, while
  for cadence they differ by 15 s or more. Reticent corroboration arrives
  simultaneously because surprises are shared events — everyone in the
  jam reports it at once — so n goes from 0 to 3 in a burst rather than
  trickling.
- Whether keepers should also cache thread records opportunistically:
  same blind, TTL-bounded, padded-request store, no keys, and it would
  cover the sparse-audience case threads are weakest at.
