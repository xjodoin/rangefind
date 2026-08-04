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
and why. Status: **design** — the consumption side (provider contract,
segment identity, live-metric search) is implemented and tested; the
mesh itself is not. The wire-level implementation specification —
byte layouts, topic grammar, bin tables, the deterministic aggregation
algorithm, validation rules, state machines, and test vectors — is
[pulsemesh-protocol.md](pulsemesh-protocol.md).

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

Per ~15 s of driving on a matched segment, at most one record:

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
| Crowd incident (3+ confirmations) | 5–10 min, ≤30 min unverified |

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

## Phased plan

1. **Loopback (no networking).** `createStaticLiveProvider` +
   contributions synthesized from the demo's own simulated drive —
   validates the full contribute→aggregate→route loop in one browser.
   The engine work for this phase is done; milestones M1–M2 of
   [pulsemesh-protocol.md](pulsemesh-protocol.md) implement it with
   real protocol bytes.
2. **Wire prototype.** js-libp2p simulation: churn, convergence time,
   digest bandwidth by density, decoy/batch-size trade-offs, malformed/
   replay/Sybil floods, clock skew. Produces measured defaults for the
   constants above (protocol spec milestones M3–M4).
3. **Corridor pilot.** A handful of commuter corridors with the mobile
   runtime as contributor and browsers as consumers; keeper nodes for
   sparsity; read-only mode default.
4. **Credential service + private forwarding.** Blind-token issuance and
   the optional two-hop reporting mode, clearly labeled in product copy.

## Acceptance criteria (inherited, still binding)

- A route query never contains origin, destination, or an ordered full
  corridor; contributions never contain coordinates or stable ids.
- No accepted record outlives the receiver-enforced TTL; a departing
  node removes no replicated state; an empty region forgets.
- A single peer can neither force a closure nor a reliable estimate.
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
- Bandwidth ceilings per zone under stadium-exit-grade density.
