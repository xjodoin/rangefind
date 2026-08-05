# PulseMesh protocol v1 — implementation specification

This is the normative wire-and-behavior specification for implementing
the PulseMesh live-traffic mesh described in [pulsemesh.md](pulsemesh.md).
That document explains *why*; this one pins every *what* an implementer
needs: byte layouts, identifier grammars, bin tables, the deterministic
aggregation algorithm, validation rules with numbers, state machines,
and test vectors. Two independent implementations following this
document must interoperate byte-for-byte and compute identical
aggregates from identical contribution sets.

Requirement words **MUST**, **MUST NOT**, **SHOULD**, **MAY** are used
in the RFC 2119 sense. Constants marked *(tunable)* may be overridden by
the signed bootstrap file (§4.6); everything else is fixed for protocol
version 1.

Status of each layer:

| Layer | Status |
| --- | --- |
| Engine consumption (`route({ live })`, segment ids, descend-where-dirty) | implemented, tested |
| §2–§9 codecs, store, aggregation, provider adapter | **implemented, tested** (`src/pulsemesh/`) |
| §10–§12 contributor/consumer/keeper behavior | **implemented, tested** |
| §5.1 wire transport (js-libp2p, GossipSub, sync streams) | **implemented, tested** (`src/pulsemesh/libp2p.js`; TCP profile — browser transports and Circuit Relay are deployment wiring on the same adapter) |
| Blind tokens (proof type 2) | reserved, phase 4 |
| Thread channel (PMT1/PMTP/PMR1/PMM1) | **implemented, tested**; separate spec, [pulsemesh-threads.md](pulsemesh-threads.md) |

The implementation lives in `src/pulsemesh/` per the §14 layout, is
covered by `test/pulsemesh_*.test.js` (§13 vectors byte-identical,
aggregation reproduced exactly, rules 1–12, incident scoring, an
end-to-end loopback mesh routing around a synthesized jam, and real
libp2p TCP nodes — including two OS processes — converging to
byte-identical digests), and is measured by
[pulsemesh-benchmarks.md](pulsemesh-benchmarks.md). All five milestones
(M1–M5) are complete. The libp2p packages are optional peer
dependencies: engine consumers that never touch the wire mesh install
nothing.

This document covers the **anonymous traffic channel** only. The
authenticated tracking channel (school bus, delivery) reuses §1's
conventions, §2's identifiers and §5.1's transport, but has its own
records, trust model and topic namespace; it is specified in
[pulsemesh-threads.md](pulsemesh-threads.md). The `PMT`, `PMP`, `PMR`
and `PMM` magic prefixes are reserved for it and MUST be ignored (not
treated as errors) by a traffic-only implementation.

## 1. Conventions

- **varint** — unsigned LEB128, identical to `pushVarint`/`readVarint`
  in `src/binary.js`: 7 value bits per byte, low bits first, high bit =
  continuation. No zigzag anywhere in this protocol (all fields are
  non-negative by construction).
- **u8** — one raw byte. Multi-byte fixed-width integers appear only in
  the PoW nonce (8 bytes, big-endian).
- **bytes(n)** — n raw bytes, no length prefix.
- All hashes are SHA-256. "hex" means lowercase hexadecimal.
- Every wire object begins with a 4-byte ASCII magic and is
  self-describing; unknown magics MUST be ignored (skipped via the
  framing length), not treated as errors.
- Trailing bytes after a well-formed record within its frame are a
  validation failure (mirrors the engine codecs' zero-trailing-bytes
  rule).

## 2. Identifiers

### 2.1 Epoch

`epoch` is the route-graph root's `sourceHash`: 64 lowercase hex
characters (SHA-256 over the graph's node/edge arrays, computed by the
builder). `epochPrefix8` is the first 8 bytes of the *binary* hash
(= first 16 hex characters). Wire records carry `epochPrefix8`; proofs
bind the full 32-byte epoch (§5.3). A record whose `epochPrefix8` does
not match the receiver's current (or overlapping previous, §11.5) epoch
MUST be dropped.

### 2.2 Segment

The physical directed segment, exactly as the engine defines it:

- engine string form: `"<leafCell>/<polylineIndex>/<direction>"` —
  exposed on `route().edges[].segment` and `snap()` matches.
- wire form: two varints, `leafCell` then
  `geomRef = polylineIndex * 2 + direction`.
- conversion: string → wire is `leafCell`, `poly*2+dir`; wire → string
  is `` `${leafCell}/${geomRef >>> 1}/${geomRef & 1}` ``.

Segment ids are meaningful for exactly one epoch. There is no segment
catalogue; the identity comes from the graph's geometry dedup.

### 2.3 Cells

Cells are rangefind Web-Mercator XYZ tiles, computed with
`geoCellForE7(latE7, lonE7, zoom)` from `src/geo_cells.js` (floor-based
XYZ with Web-Mercator latitude clamp).

- **Zone** — z9 cell. Discovery, topics, digests.
- **Detail cell** — z15 cell. Contribution storage, snapshot requests.
- A z15 cell's zone is `{x >> 6, y >> 6}`; its local coordinates within
  the zone are `localX = x & 63`, `localY = y & 63` (each 0..63).
- A contribution's cell is the z15 cell of its **snapped** coordinate
  (the `snappedLatE7/snappedLonE7` of the matched segment), never the
  raw GPS fix.

### 2.4 Time

- `timeBucket = floor(unixMillis / 15000)` — 15-second aggregation
  buckets, varint on the wire.
- `topicWindow = floor(unixSeconds / 300)` — 5-minute topic rotation.
- Ages are computed in whole seconds:
  `age = floor(nowMillis / 1000) - bucket * 15` clamped to ≥ 0, using
  the bucket's *start*.

### 2.5 Bins

**speedBin** (u8): `bin = floor(speedKmh / 5)`, valid range 0..44
(0..220 km/h). Bin `b` represents the half-open interval
`[5b, 5b+5) km/h`; its representative speed for reconstruction is the
midpoint `5b + 2.5 km/h = (5b + 2.5)/3.6 m/s`. Bin 0 is a legitimate
value meaning standstill (§10.1). Records with `speedBin > 44` MUST be
dropped.

**qualityBin** (u8, 0..7): derived by the *contributor* from its own
`snap()` match:

| snap `distMeters` d | base q |
| --- | --- |
| d ≤ 5 | 7 |
| d ≤ 10 | 6 |
| d ≤ 15 | 5 |
| d ≤ 20 | 4 |
| d ≤ 30 | 3 |
| d ≤ 40 | 2 |
| d ≤ 50 | 1 |
| d > 50 | suppress (do not emit) |

Then apply the heading rule: let Δ be the absolute angular difference
between the GPS course and the matched edge's travel bearing
(direction-aware). Δ ≤ 30°: keep q. 30° < Δ ≤ 60°: `q = max(q - 2, 1)`.
Δ > 60°: suppress. Receivers MUST drop records with `qualityBin = 0` or
`> 7` (0 is reserved: a contributor that would emit 0 suppresses
instead, so 0 on the wire indicates a non-conforming sender).

**confidenceBin** (u8, 0..7): appears only in the provider adapter
output (§9), never on the wire — confidence is *computed*, not
reported.

### 2.6 Incident types

Manual, user-tapped reports (§10.4). The type fixes three static
properties, so they are never wire fields:

| # | Type | Applies to | Default TTL | Routing |
| --- | --- | --- | --- | --- |
| 1 | crash | direction | 900 s | anchored penalty |
| 2 | hazard on road | direction | 600 s | anchored penalty |
| 3 | closure report | direction | 1800 s | anchored penalty |
| 4 | standstill | direction | 600 s | none |
| 5 | police | direction | 1800 s | **never** |
| 6 | road works | direction | 1800 s | anchored penalty |
| 7 | stopped vehicle | direction | 600 s | anchored penalty |
| 8 | object on road | direction | 900 s | anchored penalty |
| 9 | slippery surface | both | 1800 s | anchored penalty |
| 10 | poor visibility | both | 1800 s | none |
| 11 | animal on road | direction | 600 s | none |
| 12 | signal outage | both | 1800 s | anchored penalty |
| 13 | hazard on shoulder | direction | 600 s | none |

"Applies to: both" means the incident is stored and displayed for both
directions of the physical segment regardless of the reported
direction bit — ice does not care which way you are driving. Receivers
MUST drop unknown type values.

Two deliberate absences:

- **No fixed speed cameras.** A fixed camera is a permanent feature of
  the road, belongs in the static index (OSM tags it), and is served by
  the map pack with no mesh traffic, no TTL, and no corroboration
  problem. Only *mobile* enforcement is ephemeral enough to be mesh
  data, and that is type 5.
- **No free-text.** There is no comment, photo, or note field anywhere
  in PMI1. A text channel in an unmoderated, unattributed mesh is a
  harassment vector with no owner, and it would be the one field
  capable of carrying personal information about a *third* party. The
  taxonomy is the entire vocabulary.

## 3. Normative constants

| Name | Value | Tunable | Meaning |
| --- | --- | --- | --- |
| `MAX_AGE_RECEIPT` | 45 s | yes | oldest acceptable contribution at receipt |
| `MAX_FUTURE_SKEW` | 15 s | yes | max future timestamp tolerance |
| `CONTRIB_TTL` | 90 s | yes | contribution lifetime in the store |
| `DISPLAY_MAX_AGE` | 120 s | yes | oldest data a UI may render as "live" |
| `BUCKET_SECONDS` | 15 s | no | aggregation bucket size |
| `RETAINED_BUCKETS` | 8 | yes | buckets kept per segment |
| `TOPIC_WINDOW` | 300 s | no | topic rotation period |
| `TOPIC_OVERLAP` | 30 s | yes | dual-subscribe window around rotation |
| `ANTI_ENTROPY_SECONDS` | 10 s ± 3 jitter | yes | digest exchange interval |
| `SHARDS` | 16 | no | topic shards per zone |
| `POW_DIFFICULTY` | 20 bits | yes | proof-of-work leading zero bits |
| `EMIT_INTERVAL` | 15 s | yes | min seconds between emissions per device |
| `RATE_SUSTAINED` | 2 rec/s | yes | per-peer sustained acceptance rate |
| `RATE_BURST` | 40 | yes | per-peer burst bucket |
| `MAX_RECORD_BYTES` | 96 | no | max encoded PMC1 size |
| `MAX_GOSSIP_BYTES` | 8192 | no | max gossip payload |
| `CELL_CONTRIB_CAP` | 4096 | yes | stored contributions per z15 cell |
| `STORE_CONTRIB_CAP` | 262144 | yes | stored contributions total |
| `SEG_CONTRIB_CAP` | 64 | yes | stored contributions per (segment, direction) |
| `BATCH_SIZES` | 8 / 16 / 32 | no | padded snapshot request sizes |
| `DECOY_FRACTION` | 0.25–0.35 | yes | decoy share of each request |
| `SPLIT_PEERS` | 2–3 | yes | peers a corridor request is split across |
| `ENDPOINT_RINGS` | 2 | yes | z15 rings added around route endpoints |
| `UNSUB_LINGER` | 75 s ± 15 jitter | yes | subscription hold after cell exit |
| `EPOCH_OVERLAP` | 600 s | yes | dual-epoch consumer window |
| `INCIDENT_TTL_MAX` | 1800 s | no | absolute incident lifetime cap |
| `TRUST_INIT / MIN / MAX` | 1000 / 250 / 2000 | no | per-peer trust (milli) |
| `AGG_MIN_REPORTS` | 3 (2 for hint) | yes | corroboration minimums |

Reticent-profile constants (§10.2), applying to contributors that opt
into trajectory suppression:

| Name | Value | Tunable | Meaning |
| --- | --- | --- | --- |
| `SURPRISE_BINS` | 3 (15 km/h) | yes | deviation from expectation that unlocks emission |
| `SURPRISE_CONFIDENCE` | 0.4 | yes | aggregate confidence above which it, not the static metric, is the expectation |
| `BASE_SAMPLE_RATE` | 0.05 | yes | probability of emitting an unsurprising observation |
| `RETICENT_GAP` | 120 s | yes | min seconds between emissions, reticent profile |
| `COMPANY_WINDOW` | 60 s | yes | window for the "not alone" check |
| `STOP_RADIUS` | 150 m | yes | suppression radius around own stops and trip endpoints |
| `STOP_SECONDS` | 90 s | yes | suppression window around a dwell |
| `FORWARD_POOL` | 4 | yes | distinct forwarders rotated per record |
| `FORWARD_MAX_DELAY` | 10 s | yes | max hold a forwarder may add |
| `FORWARD_RATE` | 4 rec/min | yes | per-source forwarding rate at a forwarder |

Incident constants (§4.3, §8.5, §10.4):

| Name | Value | Tunable | Meaning |
| --- | --- | --- | --- |
| `INCIDENT_SHOW_SCORE` | 3 | yes | net score at which an incident is displayed |
| `INCIDENT_HINT_SCORE` | 1 | yes | net score for a low-confidence hint |
| `INCIDENT_ANCHOR_RATIO` | 0.65 | yes | congestion ratio below which a penalty is unlocked |
| `INCIDENT_WINDOW` | 600 s | yes | sliding window for scoring |
| `INCIDENT_TTL_MIN` | 300 s | no | floor on any incident lifetime |
| `INCIDENT_CELL_CAP` | 24 | yes | distinct live incidents stored per z15 cell |
| `INCIDENT_PEER_RATE` | 6 / 10 min | yes | incidents accepted per delivering peer |
| `INCIDENT_POW_MULTIPLIER` | +4 bits | yes | PoW difficulty above `POW_DIFFICULTY` |
| `REFUTE_WEIGHT` | 2 | yes | score cost of one refutation |
| `CONTRADICTION_DECAY` | 4× | yes | TTL shortening when the speed aggregate disagrees |

## 4. Wire formats

All stream messages are framed `varint length ‖ payload` (the standard
libp2p convention). Gossip messages are unframed payloads (the pubsub
layer frames them).

### 4.1 PMC1 — contribution record

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMC1"` | 4 bytes |
| 2 | epochPrefix8 | bytes(8) | §2.1 |
| 3 | leafCell | varint | §2.2 |
| 4 | geomRef | varint | `polyline*2 + direction` |
| 5 | timeBucket | varint | §2.4 |
| 6 | speedBin | u8 | 0..44 |
| 7 | qualityBin | u8 | 1..7 |
| 8 | meters | varint | static segment length, rounded; 0 = unknown |
| 9 | ttlSeconds | u8 | 1..90; receiver clamps to `CONTRIB_TTL` |
| 10 | reportId | bytes(16) | CSPRNG, one use, never reused |
| 11 | proofType | u8 | 0 none, 1 PoW, 2 blind token (reserved) |
| 12 | proofLen | varint | |
| 13 | proof | bytes(proofLen) | §5.3 |

The **preimage** is fields 1–10 exactly as encoded (everything before
`proofType`). `meters` is the contributor's knowledge of the segment's
static length (it has the leaf cell loaded to have snapped); it lets
receivers without that cell sanity-check speed×time plausibility and
lets the provider adapter fill `LiveSegmentState.meters`.

Proof type 0 MUST be rejected on any network transport; it exists so
the identical codec can run in loopback tests and the phase-1 demo.

### 4.2 PMB1 — gossip batch

`"PMB1"` ‖ varint count (1..16) ‖ count PMC1 records concatenated.
Gossip publishes PMB1 (a single record still travels as a batch of 1).
Total encoded size MUST be ≤ `MAX_GOSSIP_BYTES`.

### 4.3 PMI1 — incident record

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMI1"` | |
| 2 | epochPrefix8 | bytes(8) | |
| 3 | leafCell | varint | |
| 4 | geomRef | varint | |
| 5 | ratioQ12 | varint | 0..4095, position along the segment |
| 6 | timeBucket | varint | observation time |
| 7 | type | u8 | §2.6 |
| 8 | polarity | u8 | 1 report, 2 confirm, 3 refute |
| 9 | ttlSeconds | varint | ≤ the type's default TTL, ≥ `INCIDENT_TTL_MIN` |
| 10 | reportId | bytes(16) | |
| 11 | proofType / proofLen / proof | as PMC1 | preimage = fields 1–10 |

`ratioQ12` is what makes "police in 800 m" possible: the position along
the segment, from the reporter's own `snap()` match (`SnapMatch.ratio`).
Consumers interpolate with `engine.locate()`. It is quantized to 1/4096
of a segment and is the *only* positional precision in the record —
there is no coordinate field, here or anywhere.

`polarity` is the Waze loop: a **confirm** ("still there") and a
**refute** ("not there") are ordinary PMI1 records with the same
`(segment, type)` key, a fresh `reportId`, and their own proof. They
are not references — there is no incident id to point at, because a
stable id would be a linkable handle and because different reporters
must be able to corroborate without having seen each other's records.
Scoring is §8.5.

The PoW difficulty for PMI1 is `POW_DIFFICULTY + INCIDENT_POW_MULTIPLIER`.
An incident is a *claim*, not a measurement — nothing about the physical
world constrains it the way a plausible speed constrains a PMC1 — so it
should cost more to assert. This is a speed bump, not a defense; §8.5
carries the actual defense.

**A mesh incident never produces `closed: true`** in protocol v1 (§9);
closure-reports become a penalty. Verified closures are reserved for
authority-key-signed feeds (future protocol version; keys already have
a slot in the bootstrap file).

### 4.4 PMD1 — zone digest

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMD1"` | |
| 2 | epochPrefix8 | bytes(8) | |
| 3 | zoneX, zoneY | varint ×2 | z9 |
| 4 | baseBucket | varint | newest bucket known to sender |
| 5 | entryCount | varint | |
| 6 | entries | see below | sorted by (localX, localY) ascending |

Each entry: `localX` u8 (0..63) ‖ `localY` u8 ‖ `count` varint (live
contributions stored for that z15 cell) ‖ `ageBuckets` varint
(`baseBucket − newestBucketInCell`) ‖ `idFold` bytes(8) (XOR of the
first 8 bytes of every stored contribution's reportId in the cell —
order-independent, so equal sets fold equally).

A receiver requests a cell when the remote's `ageBuckets` implies newer
data than it holds, or when counts/folds differ (anti-entropy).

### 4.5 Sync stream messages

One libp2p protocol, magic-discriminated:

- **PMN1** (zones agree): `"PMN1"` ‖ epochPrefix8. The answer to a PMG1
  that carried a matching zone fold — 12 bytes instead of a full digest.
  Measured: in a converged zone this elides 100% of digest traffic; in an
  actively driven one it never fires, because there the digest is not
  waste but the cost of genuine disagreement
  ([benchmarks §5](pulsemesh-benchmarks.md)).

- **PMG1** (get digest): `"PMG1"` ‖ epochPrefix8 ‖ zoneX varint ‖ zoneY
  varint, optionally followed by the requester's own zone fold —
  `count` varint ‖ `fold` bytes(8), the XOR of its stored reportId
  prefixes for that zone. Response: one PMD1, or PMN1 when the fold
  matches.
- **PMQ1** (get cells): `"PMQ1"` ‖ epochPrefix8 ‖ cellCount varint
  (MUST be exactly 8, 16, or 32) ‖ per cell: x15 varint ‖ y15 varint.
  Cells MAY lie anywhere (decoys are indistinguishable by design).
- **PMS1** (cell snapshots), the response: `"PMS1"` ‖ epochPrefix8 ‖
  cellCount varint ‖ per cell: x15 ‖ y15 ‖ recordCount varint ‖ that
  many verbatim PMC1 records ‖ incidentCount varint ‖ that many
  verbatim PMI1 records. Cells echo the request order. Unknown/empty
  cells return counts of 0 — a responder MUST NOT distinguish "cell I
  don't track" from "cell with no data".

- **PMF1** (forward for me): `"PMF1"` ‖ epochPrefix8 ‖ delayMs varint
  (≤ `FORWARD_MAX_DELAY`) ‖ a verbatim PMB1 or PMI1. The receiver
  validates it exactly as if it had arrived over gossip (§6, all
  rules), holds it for `delayMs`, then publishes it to the correct
  topic **as its own** publication. There is no response.

  A forwarder MUST NOT forward a record that fails validation (it would
  become an amplifier), MUST NOT alter any byte (records are
  self-validating; altering one only destroys its proof), MUST NOT
  forward a PMF1 it received via PMF1 (one hop only, no chains), and
  MUST rate-limit per source peer at `FORWARD_RATE`. Forwarding is
  cheap and safe precisely because PMC1 carries no sender identity:
  there is nothing in the record for a forwarder to strip, substitute,
  or need to be trusted about. What forwarding changes is only *which
  peer id the mesh observes publishing it*.

Snapshots carry **raw contributions, not aggregates**: the merge is a
set union keyed by reportId, and every subscriber recomputes aggregates
locally (§8) — this is what makes the aggregate consensus-free.

### 4.6 mesh-bootstrap.json

Published on the static CDN next to the route-graph root, re-published
with each epoch:

```json
{
  "format": "pulsemesh-bootstrap-v1",
  "epoch": "<64 hex — MUST equal the route-graph root sourceHash>",
  "previousEpoch": "<64 hex or null>",
  "constants": { "POW_DIFFICULTY": 20, "MAX_AGE_RECEIPT": 45 },
  "incidentPolicy": { "suppressedTypes": [5] },
  "bootstrapPeers": ["/dns4/…/tcp/443/wss/p2p/12D3Koo…"],
  "relays": ["/dns4/…/p2p/12D3Koo…"],
  "authorityKeys": [],
  "publicKey": "<32-byte ed25519 pubkey, hex>",
  "signature": "<64-byte ed25519 signature, hex>"
}
```

The signature is Ed25519 over the canonical JSON encoding (UTF-8,
object keys sorted lexicographically at every level, no whitespace) of
the object with `signature` removed. The expected `publicKey` ships
inside the application; a bootstrap whose key or signature does not
verify MUST be discarded and the mesh treated as unavailable (the
router keeps working — provider degradation is the engine contract).
`constants` may override only the rows marked tunable in §3, and only
within ±4× of the defaults.

`incidentPolicy.suppressedTypes` lists §2.6 type numbers that clients in
this deployment MUST NOT emit and MUST drop on receipt (§10.4). It is
inside the signed object precisely so a deployment cannot have its
policy altered in transit; an absent field means no suppression.

## 5. Transport, topics, proofs

### 5.1 libp2p profile

- Transports: WebRTC (browser↔browser), WebTransport or WSS
  (browser↔keeper), TCP (keeper↔keeper). Circuit Relay v2 for NAT
  traversal — and product copy never calls relayed traffic anonymous.
- Pubsub: GossipSub v1.1. Message signing **disabled** (records are
  self-validating; peer identity is deliberately not bound to data).
  Message id = first 20 bytes of SHA-256 of the payload. Max payload
  `MAX_GOSSIP_BYTES`. Peer-scoring: use the library defaults plus the
  application-level trust ledger (§8.4) for local ranking only.
- Sync stream protocol id: `/rangefind/pulsemesh/1/sync`.

### 5.2 Topic grammar

```
/rangefind/pulsemesh/1/<epochPrefix16hex>/<zoneX>/<zoneY>/<topicWindow>/<shard>
```

- `epochPrefix16hex` — first 16 hex chars of the epoch.
- `zoneX`, `zoneY` — decimal z9 coordinates.
- `topicWindow` — decimal `floor(unixSeconds / 300)`.
- `shard` — decimal 0..15, of the record's z15 cell:
  `shard = SHA256(utf8("<x15>/<y15>"))[0] mod 16`.

The path element after the protocol version is a z9 `zoneX`, i.e. a
decimal number. The non-numeric element `t` is reserved for the thread
channel's key-derived topics (`…/1/t/<epochPrefix16hex>/<tagHex16>`); a
traffic-only implementation MUST NOT subscribe to it.

A publisher emits each PMB1/PMI1 to the topic of its z15 cell's shard
for the **current** window. Subscribers join current-window topics and,
within `TOPIC_OVERLAP` of a rotation boundary, the next window's
topics too. Messages arriving on a topic whose window differs from the
current or adjacent window MUST be dropped.

### 5.3 Proof of work (proofType 1)

`proof` is an 8-byte big-endian nonce. Requirement:

```
SHA256( preimage ‖ epoch32 ‖ nonce )
```

has ≥ `POW_DIFFICULTY` leading zero bits, where `epoch32` is the full
32-byte binary epoch (so a prefix collision cannot transplant proofs
across epochs) and `preimage` is defined per record type. The expected
cost at difficulty 20 is ~1M hashes (well under a second of one mobile
core, per ~15 s emission). Verification is one hash. Blind tokens
(proofType 2) will replace PoW in phase 4 with the same envelope;
until then receivers MUST reject proofType 2.

## 6. Validation pipeline

A receiver applies these rules **in order** to every record, dropping
on first failure. Rules 1–9 are cheap and unconditional; 10–12 need
context.

1. Frame/magic/size: well-formed, no trailing bytes, encoded size ≤
   `MAX_RECORD_BYTES`.
2. Epoch: `epochPrefix8` matches current epoch (or previous during the
   consumer overlap window, §11.5).
3. Fields in range: `speedBin ≤ 44`, `1 ≤ qualityBin ≤ 7`,
   `1 ≤ ttlSeconds ≤ 90` (PMC1), `meters ≤ 100000`. For PMI1:
   `type` known (§2.6) and not in `incidentPolicy.suppressedTypes`,
   `1 ≤ polarity ≤ 3`, `ratioQ12 ≤ 4095`, and `ttlSeconds` between
   `INCIDENT_TTL_MIN` and the type's default TTL.
4. Time: `age ≤ MAX_AGE_RECEIPT`; bucket start not more than
   `MAX_FUTURE_SKEW` in the future.
5. Proof: proofType 1 with valid PoW at the bootstrap difficulty —
   plus `INCIDENT_POW_MULTIPLIER` for PMI1 (proofType 0 and 2 rejected
   on the wire).
6. Replay: reportId not already in the store (the store *is* the dedup
   window — reportIds live exactly as long as their records).
7. Rate: per-remote-peer token bucket (`RATE_SUSTAINED`, `RATE_BURST`);
   for PMI1 additionally `INCIDENT_PEER_RATE` per delivering peer and
   `INCIDENT_CELL_CAP` distinct live incidents per z15 cell (on
   overflow, evict the lowest-scoring incident first).
8. Topic consistency (gossip only): the record's z15 cell must map to
   the topic's zone and shard.
9. Global speed cap: `speedBin ≤ 44` already enforces 220 km/h.
10. Class plausibility *(only if the receiver has the leaf cell
    loaded)*: representative speed ≤ class cap × 1.15, caps in km/h —
    motorway 140, trunk 130, primary 110, secondary 100, tertiary 90,
    residential/unclassified 70, service 50, everything else 50. Also:
    `meters` within ±20% of the static edge length when nonzero.
11. Segment existence *(same condition)*: `geomRef >>> 1` < the leaf's
    polyline count.
12. Reportable class *(same condition)*: the segment's highway class is
    **not** one of `service`, `driveway`, `parking_aisle`, `track`,
    `living_street`, `pedestrian`, or `footway`. Records on these are
    dropped unconditionally.

    These classes are where a contribution is least useful and most
    identifying: a driveway or a parking aisle carries no through
    traffic that anyone routes over, and a report from one is close to
    a statement about a single address. Excluding them at validation
    means the rule holds regardless of what the contributor intended,
    and it costs the routing layer nothing — the router degrades to the
    static metric on exactly the segments where the static metric was
    already right.

Failing 10–12 additionally applies a trust penalty to the delivering
peer (§8.4). Records that pass validation are stored even when rules
10–12 could not be evaluated — plausibility is enforced probabilistically
by whichever peers do hold the cell.

## 7. Local store

In-memory only. Keyed three ways over one record set:

- by reportId (dedup, rule 6),
- by z15 cell (digests, snapshots),
- by (segment, direction) — note direction is inside geomRef
  (aggregation).

Eviction: a record expires at
`min(bucketStart + ttlSeconds, receiptTime + CONTRIB_TTL)`; sweep at
least once per bucket (15 s). Capacity: per-(segment,direction) cap
`SEG_CONTRIB_CAP`, per-cell cap `CELL_CONTRIB_CAP`, global
`STORE_CONTRIB_CAP` — on overflow evict the lowest aggregation weight
(§8.2) first, oldest first among ties. A departing peer removes
nothing; an empty region forgets by TTL. Persistence to disk is
prohibited (contributions are ephemeral by design).

## 8. Deterministic aggregation

Aggregates are recomputed locally, per (segment, direction), from the
stored contribution set. All arithmetic below is exact integer
arithmetic; two conforming implementations holding the same set MUST
produce bit-identical aggregates.

### 8.1 Normative tables

Freshness by age in seconds, `FRESHNESS[a] = round(1000·e^(−a/60))`,
precomputed once; implementations MUST use this table verbatim (not a
runtime `exp`, whose rounding is platform-dependent). Ages > 90 use 0
(such records are expired anyway):

```
a=0..90:
1000 983 967 951 936 920 905 890 875 861 846 832 819 805 792 779 766
753 741 729 717 705 693 682 670 659 648 638 627 617 607 597 587 577
567 558 549 540 531 522 513 505 497 488 480 472 465 457 449 442 435
427 420 413 407 400 393 387 380 374 368 362 356 350 344 338 333 327
322 317 311 306 301 296 291 287 282 277 273 268 264 259 255 251 247
243 239 235 231 227 223
```

Quality weight `Q[q]` for q = 0..7: `0 250 400 550 700 800 900 1000`.

### 8.2 Weight

For contribution *c* delivered by peer *p*:

```
W(c) = Q[c.qualityBin] × FRESHNESS[min(age(c), 90)] × T[p]
```

with `T[p]` the local trust milli-value (§8.4; use `TRUST_INIT` = 1000
when unknown, e.g. for snapshot-fetched records). W is a plain integer
product — no normalization, since only ratios matter.

### 8.3 Weighted median and confidence

Over the live (unexpired) contributions for one (segment, direction):

1. Sort ascending by `(speedBin, reportId lexicographic)` — the
   reportId tie-break is what makes the fold deterministic.
2. `total = Σ W`. Walk the sorted list accumulating `cum`; the
   **aggregate bin** is the first contribution where `2·cum ≥ total`.
3. `n` = number of contributions. Publishable when `n ≥ 3`; when
   `n = 2` the aggregate is a *hint* and its confidence (below) is
   capped at 0.30; `n ≤ 1` produces nothing.
4. `agreement = (Σ W over contributions with |bin − aggBin| ≤ 1) / total`
   (rational; carry as numerator/denominator or compute in floating
   point only at this final step — the bin itself is already fixed).
5. `diversity = min(n, 6) / 6`;
   `freshnessFactor = FRESHNESS[age of newest contribution] / 1000`.
6. `confidence = diversity × freshnessFactor × agreement`, then the
   hint cap; `confidenceBin = round(7 × confidence)`.

The aggregate's representative speed is the bin midpoint (§2.5);
`observedAt = newestBucket × 15000`; `meters` = the plurality value of
the contributions' nonzero `meters` fields (ties → smallest), else 0.

### 8.4 Trust ledger (local, never gossiped)

Per remote peer, milli-integer clamped [250, 2000], start 1000:
**+25** when a peer's contribution lands within ±1 bin of a later
aggregate with `n ≥ 3` (at most one credit per aggregate); **−100**
when ≥ 3 bins away from such an aggregate; **−500** for any rule 10–11
validation failure; decay 1% of the distance back toward 1000 per
minute. Trust shapes weights and local peer ranking only — it is a
Sybil *damper*, not the Sybil defense (that is the proof layer).

### 8.5 Incident scoring

Manual reports need their own scoring, because they are a different
kind of statement. A PMC1 is a *measurement*, constrained by physics:
to forge a convincing jam you must produce several mutually consistent
speeds that survive a weighted median and a class plausibility check. A
PMI1 is a *claim*. Nothing about the world constrains it.

**The hole this closes.** As originally specified, an incident was
confirmed by "3 distinct reportIds within 10 minutes". But reportIds
are one-use CSPRNG values with no identity behind them — a single
device mints three of them, computes three proofs, and has "three
distinct reports" in under a second. The corroboration minimum that
makes speed aggregates trustworthy does nothing at all for incidents,
and this is exactly the abuse Waze-style reporting attracts: fabricate a
crash to divert traffic off your street, or to clear a lane ahead of you.

Four mechanisms, of which only the last actually holds.

**1. Score, don't count.** For key `(segment, type)` over records within
`INCIDENT_WINDOW`:

```
raw      = Σ trust-weighted reports and confirms
         − REFUTE_WEIGHT × Σ trust-weighted refutes
sources  = number of distinct peers that delivered those records
score    = min(raw, sources)
```

The `min` is the point: **a score can never exceed the number of
distinct peers that carried it**. Five records down one connection score
1, not 5. Reaching `INCIDENT_SHOW_SCORE` requires three peer identities
with three separate connections, and the per-peer `INCIDENT_PEER_RATE`
plus the per-cell `INCIDENT_CELL_CAP` bound how far that scales.

This is why **PMI1 SHOULD NOT be published through PMF1** (§4.5):
forwarder rotation makes one reporter look like several peers, which is
the privacy goal for measurements and precisely the wrong property for
claims. An implementation MAY offer forwarded reporting for users who
want it, but a forwarded PMI1 contributes at most `INCIDENT_HINT_SCORE`
and can never on its own reach the display threshold. Privacy or
weight — the protocol supports both, and the user picks per report.

**2. Display tiers.** `score ≥ INCIDENT_SHOW_SCORE` displays normally;
`≥ INCIDENT_HINT_SCORE` displays as unconfirmed and MUST be visually
distinct; below that, nothing is shown and nothing is routed.

**3. Contradiction decay.** When an anchored-penalty incident is live on
a segment whose speed aggregate has `n ≥ 3` and a congestion ratio
*above* `INCIDENT_ANCHOR_RATIO` — traffic is flowing normally — its
remaining TTL is divided by `CONTRADICTION_DECAY`. Physics refutes the
claim with nobody tapping anything, which also fixes the ordinary
non-malicious case: the crash cleared and no one thought to say so.

**4. The speed anchor — the one that actually defends.** A routing
penalty (§9) is applied **only** when the same segment carries an
independent speed aggregate with `n ≥ AGG_MIN_REPORTS` whose congestion
ratio is at or below `INCIDENT_ANCHOR_RATIO`. No aggregate, or an
aggregate showing free flow, means **no penalty, at any score**.

A fabricated crash on a flowing road therefore changes no route, ever.
The attacker's claim has to be ratified by measurements they do not
control and cannot cheaply forge, so the incident channel inherits the
Sybil resistance of the speed channel instead of needing its own. And
the informational types — police, visibility, animal, shoulder hazard —
never affect routing under any condition, so there is nothing to win by
faking them beyond display noise, which the caps bound.

Blind tokens (proofType 2, phase 4) are the real identity anchor and
will make `sources` mean something stronger. Until they exist, the
honest statement is: **incident display is best-effort and abusable at
the margins; incident *routing* is not, because it is gated on
measurement.** Product copy must not overstate the first.

## 9. Provider adapter (mesh → engine)

The mesh terminates in a `LiveTrafficProvider` (`types/route.d.ts`).
For `fetch({ epoch, areas, maxAgeSeconds })`:

1. If `epoch` ≠ the mesh's current epoch → return `[]` (the engine
   degrades; this is the contract).
2. Determine wanted z15 cells: rasterize each area bbox at z15, union
   with the active corridor cache (§11), fetch missing/stale cells via
   PMQ1 with full padding/decoy/split behavior (§11.3) — the privacy
   rules apply to *every* fetch, including this one.
3. For each aggregate with age ≤ `maxAgeSeconds` and `n ≥ 2`, emit:

```js
{
  segment: `${leafCell}/${geomRef >>> 1}/${geomRef & 1}`,
  speedMps: (5 * aggBin + 2.5) / 3.6,
  meters: aggMeters || undefined,        // engine falls back to factor-less
  confidence: confidenceBin / 7,
  observedAt: newestBucket * 15000,
  closed: false,                          // ALWAYS false in protocol v1
  penaltySeconds: incidentPenalty(segment) // §below, capped 300
}
```

`incidentPenalty` sums the segment's live incidents, and every term is
gated on the §8.5 speed anchor:

```js
function incidentPenalty(segment, aggregate) {
  // Rule 4 of §8.5: no independent measurement, no penalty. Ever.
  if (!aggregate || aggregate.n < AGG_MIN_REPORTS) return 0;
  if (aggregate.congestionRatio > INCIDENT_ANCHOR_RATIO) return 0;

  let penalty = 0;
  for (const inc of liveIncidents(segment)) {
    if (inc.score < INCIDENT_SHOW_SCORE) continue;   // hints never route
    penalty += PENALTY_SECONDS[inc.type] || 0;       // 0 for informational
  }
  return Math.min(penalty, 300);
}
```

with `PENALTY_SECONDS` — crash 120, hazard on road 60, closure report
300 (a strong penalty, not a closure), road works 45, stopped vehicle
45, object on road 60, slippery surface 45, signal outage 60, and
**0 for types 4, 5, 10, 11 and 13** (standstill is already in the speed
aggregate; police, visibility, animal and shoulder hazard are
informational and MUST NOT influence a route under any score).

The engine then applies its own confidence/age blend and clamps — the
adapter MUST NOT pre-blend.

Informational incidents are still returned to the application for
display, but through a separate accessor, never through
`LiveSegmentState`. Nothing that cannot change a route should be able
to reach the router at all.

## 10. Contributor pipeline (mobile runtime)

### 10.1 Per-fix state machine

State machine per GPS fix (target cadence 1 Hz):

1. **Match**: `engine.snap(fix)`; take the best match. No match, or
   `distMeters > 50` → state OFF_ROAD (emit nothing).
2. **Quality**: compute qualityBin (§2.5) with the fix's course; a
   suppress outcome → emit nothing this bucket.
3. **Speed**: smoothed over the last 3 fixes, converted to speedBin.
4. **Standstill rule**: `speedBin = 0` is emitted only if a moving
   matched fix existed within the previous 60 s on the same or an
   adjacent segment (that is congestion); otherwise suppress (that is
   parking).
5. **Cadence**: at most one PMC1 per `EMIT_INTERVAL` per device, on the
   segment where the most recent qualifying fix lies. Fresh CSPRNG
   reportId per record; compute PoW; publish as PMB1 to the segment's
   topic. Battery guard: contribution SHOULD pause below 20% battery
   unless charging.
6. **Never** in any record: raw coordinates, previous/next segment,
   device or session identifiers, precise timestamps (the 15 s bucket
   is the resolution). This is enforced by the codec having no fields
   for them.

Incident reports (PMI1) are user-initiated; §10.4 covers them.

### 10.2 The reticent profile

A contributor emitting on cadence produces a **trajectory**. Records
carry no identifier, but a walk on adjacent segments in consecutive
buckets is reconstructible from the record set alone, by anyone, with
no identifier needed: on a quiet street the chain is unique. For a
private driver that leaks a commute. For a **courier it leaks the
customer list**, which is why §10 rule 3 of
[pulsemesh-threads.md](pulsemesh-threads.md) originally forbade
couriers from contributing at all.

The fix is not to exclude them. It is that **the aggregate never wanted
what cadence collects**. Aggregation wants *breadth* — many vehicles
across many segments — and §8.3 discards everything below
`AGG_MIN_REPORTS` anyway. One vehicle reporting 120 consecutive
segments and 120 vehicles reporting one segment each produce the same
aggregates; only the first produces a trajectory. Cadence buys depth
the aggregate cannot use, and pays for it in exactly the currency this
design refuses to spend.

The reticent profile therefore replaces cadence with four gates. A
contributor MUST pass all four to emit. It is **mandatory for thread
publishers on unpublished routes** (couriers, field service) and
RECOMMENDED as the default for every contributor pending phase-2
measurement (§14, M4).

**Gate 1 — place.** Suppress within `STOP_RADIUS` of the device's own
trip origin or destination, within `STOP_RADIUS` of any of its own
service stops, and for `STOP_SECONDS` around any dwell. Suppress on
`residential` and `unclassified` classes entirely — the deny list of
rule 12 is the floor, not the target. A courier's addresses are on
those streets; its useful traffic observations are not.

**Gate 2 — surprise.** Let

```
expected = live aggregate speedBin for this segment, when one exists
           with confidence ≥ SURPRISE_CONFIDENCE
         = the time-bucket static free-flow bin otherwise
```

(the contributor is also a consumer, so it already holds both). Emit
when `|observedBin − expected| ≥ SURPRISE_BINS`. Otherwise fall through
to gate 3.

This is the load-bearing one, and it improves the channel rather than
compromising it:

- **Silence becomes a signal.** No live state means observations match
  the static metric, and the router's degrade-to-static behaviour is
  then not a degradation but the correct answer. The ambiguity between
  "matches static" and "nobody there" is harmless because both resolve
  the same way.
- **It self-corrects chronic error.** A segment whose time-bucket
  metric is wrong generates surprises until the aggregate fixes it,
  then goes quiet.
- **Clearance is detected.** Because `expected` follows the live
  aggregate when one exists, observing free flow where the aggregate
  says jam is itself a surprise, and gets reported.
- **Surprises are inherently shared.** Everyone in a jam observes the
  same jam and reports it, so the anonymity set for a surprise report
  is everyone in the jam. Free-flow reports — the ones with a small,
  identifying anonymity set — are exactly the ones now suppressed.

**Gate 3 — company.** Emit an unsurprising observation only with
probability `BASE_SAMPLE_RATE`, never within `RETICENT_GAP` of the
previous emission, and only when the contributor's own store already
holds ≥ 1 other live contribution for that segment within
`COMPANY_WINDOW`.

The company check costs nothing in utility: a record with no company
falls below `AGG_MIN_REPORTS` and **produces no aggregate at all**, so
a lone report is pure privacy cost against zero benefit. The one
exception is jam onset — the first witness of a new jam has no company
by definition — which is why a gate-2 surprise bypasses gate 3
entirely. A lone surprise still publishes nothing visible until two
more arrive, and if the jam is real they will.

**Gate 4 — forwarding.** Publish through PMF1 (§4.5) to a forwarder
drawn from a pool of `FORWARD_POOL` peers, **rotated per record**, not
directly to the topic.

State plainly what this does and does not buy, per design rule 5 of
[pulsemesh.md](pulsemesh.md#design-rules): it does not make a
contributor anonymous. It replaces the publishing peer id with the
forwarder's, so trajectory reconstruction from peer identity requires
the forwarders to collude — with rotation, all of them. A global
passive observer of the mesh, or a full-pool collusion, still sees the
record set. Forwarding narrows the observer set; the first three gates
are what shrink the data.

**Prohibited: decoy contributions.** Padding and decoys protect
*queries* (§11.3) and cost nothing because a query is a question. A
decoy contribution is a lie about the road, and it would be
indistinguishable from a Sybil attack — by the network and by the
aggregate. A contributor that cannot emit safely emits nothing.

### 10.3 What remains exposed

Honesty about the residual, since the gates do not reduce it to zero:

- A surprise report on an empty road at 03:00 can have an anonymity set
  of one. The gates are weakest at low density — where traffic data is
  also least valuable, but the trade is real.
- Gates 1–3 are contributor-side and unenforceable by receivers, unlike
  rule 12. That is acceptable here in a way it is not for Sybil
  controls: a contributor that violates a privacy gate harms only
  itself, so there is no incentive to defect and no need to police it.
- Sparse surprise reports still cluster along a courier's arterial
  path. Against an adversary that already knows the courier was working
  a given area, they narrow the search; against one that does not, they
  are a handful of unlinked points among everyone else's.

### 10.4 Manual incident reporting

A report is a deliberate act, so its rules are about the person, not
the sensor.

**Emission.** At most one PMI1 per `(type, 5 min)` per device. The
segment and `ratioQ12` come from `snap()` on the current fix, so a
report can only be filed for where the reporter actually is — there is
no "report ahead" or "report anywhere on the map" affordance, and an
implementation MUST NOT offer one. A user with no snap match (rule 1,
OFF_ROAD) cannot report. PoW at the incident difficulty (§4.3), fresh
reportId, published directly rather than forwarded (§8.5).

**Confirm and refute.** When a consumer traverses a segment carrying a
live displayed incident, it MAY prompt once ("still there?") and emit
polarity 2 or 3. It MUST NOT prompt for an incident it has already
answered, MUST NOT prompt while the vehicle is moving in a way that
makes answering unsafe, and MUST NOT auto-answer: a silent traversal is
not evidence, and the contradiction-decay rule (§8.5) already covers
what silence can be read to mean.

**Reporter privacy — the honest trade.** An incident report is a much
stronger self-disclosure than a speed contribution. It is precise in
time and space, it is deliberate, and per §8.5 it is published
*directly* so that distinct-peer counting works, which means the
reporting peer id is visible to the mesh at that place and moment. Note
the tension: the reticent profile (§10.2) exists to stop peer identity
from chaining a route together, and reporting punches a hole in it.

Consequently:

- An implementation MUST tell the user, at the point of reporting, that
  a report is public and locates them. Not in a settings page.
- A reticent contributor that files a report MUST suppress PMC1
  emission in that z15 cell for `RETICENT_GAP`, so the report and the
  measurement stream are not trivially joined.
- Reporting MUST be available without any account, and MUST NOT be
  gated on having contributed.
- The forwarded, hint-weight path (§8.5) MUST be offered to users who
  prefer it, since for some people and some report types the visibility
  is the risk.

**Jurisdiction.** Police reporting (type 5) is restricted or unlawful
in some jurisdictions, and its legality is not the protocol's judgment
to make. The signed bootstrap file therefore carries an
`incidentPolicy` object (§4.6) listing types that clients MUST NOT emit
or display in a given deployment. A client MUST honour it, and MUST
drop received records of a suppressed type rather than merely hiding
them. Deployments differ; the protocol stays neutral and the operator
configures.

Type 5 is also deliberately narrow: mobile enforcement presence, on a
segment, with a TTL under half an hour. It is not a facility for
tracking individual officers, and the taxonomy gives it no way to
become one — no identity, no description, no free text (§2.6), no
persistence beyond the TTL.

## 11. Consumer pipeline (any client)

1. **Bootstrap**: fetch and verify mesh-bootstrap.json; connect to
   1–2 bootstrap peers; discover zone peers via rendezvous on the z9
   zones of interest.
2. **Corridor**: compute candidate routes locally
   (`route({ alternatives })`), buffer the union geometry by 250 m,
   rasterize to z15 cells (`geoCellsForRoute` at z15 covers this),
   broaden each endpoint by `ENDPOINT_RINGS` rings. This cell set plus
   a ~30% overfetch of adjacent cells is the **corridor cache** target.
3. **Fetch privacy** (MUST, for every PMQ1 including provider-triggered
   ones): pad the wanted set with decoys (`DECOY_FRACTION`, drawn from
   the 2-ring neighborhood and previously visited cells) up to the next
   `BATCH_SIZES` size; shuffle with a CSPRNG; split across
   `SPLIT_PEERS` distinct peers round-robin so no peer sees the ordered
   corridor; never fetch endpoint cells unpadded.
4. **Subscribe**: join the gossip topics (all 16 shards, or the shard
   subset covering cached cells when bandwidth-constrained) for the
   zones the corridor crosses; unsubscribe `UNSUB_LINGER` after the
   corridor no longer touches a zone. Anti-entropy: every
   `ANTI_ENTROPY_SECONDS`, PMG1 a random connected zone peer and repair
   diffs.
5. **Epoch handover**: when the bootstrap advertises a new epoch,
   consumers run both epochs' topics/stores for `EPOCH_OVERLAP`;
   contributors switch immediately (single-epoch emit). The provider
   answers for whichever epoch the engine asks about and `[]`
   otherwise — during overlap an app typically holds one engine per
   epoch and swaps atomically.
6. **Routing**: `engine.route({ from, to, live: pulseMeshProvider })`.
   Everything downstream (context expansion, shortcut suppression,
   exactness, degradation) is the engine's implemented behavior.

## 12. Keeper nodes

A keeper is a headless peer pinned to configured zones: same store,
same validation, same TTLs, no contributions, higher connection limits,
and it answers PMG1/PMQ1 promptly. Keepers exist so sparse regions
retain data between commuter waves within TTL — they add availability,
never authority: a keeper's records carry the same proofs and get the
same trust treatment as anyone's. Run profile: Node ≥ 20, js-libp2p
with TCP+WSS listeners, `STORE_CONTRIB_CAP` raised (tunable), Circuit
Relay v2 service enabled.

## 13. Test vectors

Epoch (test only): `SHA256("pulsemesh-test-vector")` =
`f44796c8cc1f3fa797104e925812ff052717f3052b5dbcadb0a36db776e0a4d1`.

### 13.1 Contribution record

Fields: leafCell 3181, polyline 442 direction 1 (geomRef 885),
timeBucket 116951040 (= unixMillis 1754265600000), speedBin 7,
qualityBin 6, meters 184, ttl 90, reportId
`845e91831319e89c4d656bdb80c278ac` (= first 16 bytes of
`SHA256("report")`).

Preimage (41 bytes):

```
504d4331 f44796c8cc1f3fa7 ed18 f506 8090e237 07 06 b801 5a
845e91831319e89c4d656bdb80c278ac
```

PoW at difficulty 16 (test difficulty; production default 20): nonce
`000000000000e6d4`, digest
`0000c8ff4d3d25b2f2c430481bfd05b76b938aa1ac8bc6aff223c290c488c688`.

Full record (51 bytes): preimage ‖ `01` ‖ `08` ‖ nonce:

```
504d4331f44796c8cc1f3fa7ed18f5068090e2370706b8015a845e91831319e8
9c4d656bdb80c278ac0108000000000000e6d4
```

### 13.2 Topic

For a z15 cell (x 9256, y 11515) at unixSeconds 1754265600:
zone = (144, 179), window = 5847552,
shard = `SHA256("9256/11515")[0] mod 16` = 213 mod 16 = **5** →

```
/rangefind/pulsemesh/1/f44796c8cc1f3fa7/144/179/5847552/5
```

### 13.3 Aggregation

Five contributions on one (segment, direction), all from trust-1000
peers:

| id | speedBin | quality | age s | W = Q·F·T |
| --- | --- | --- | --- | --- |
| aa | 7 | 6 | 5 | 828 000 000 |
| bb | 8 | 7 | 12 | 819 000 000 |
| cc | 7 | 5 | 20 | 573 600 000 |
| dd | 10 | 3 | 30 | 333 850 000 |
| ee | 6 | 6 | 44 | 432 000 000 |

total = 2 986 450 000. Sorted: ee(6), aa(7), cc(7), bb(8), dd(10);
cumulative 432 000 000 → 1 260 000 000; `2·cum ≥ total` first holds at
**aa → aggregate bin 7** (37.5 km/h, 10.417 m/s). Agreement =
2 652 600 000 / 2 986 450 000 ≈ 0.888212; diversity 5/6; freshness
F[5]/1000 = 0.920; confidence ≈ 0.680962 → **confidenceBin 5**.

### 13.4 Incident record

Fields: leafCell 3181, geomRef 885 (polyline 442, direction 1),
ratioQ12 2048, timeBucket 116951040, type 5 (police), polarity 1
(report), ttl 1800, reportId `d4191834714542dcf3e5d8a6ab386c9b`
(= first 16 bytes of `SHA256("incident")`).

Preimage (42 bytes):

```
504d4931 f44796c8cc1f3fa7 ed18 f506 8010 8090e237 05 01 880e
d4191834714542dcf3e5d8a6ab386c9b
```

PoW at difficulty 16 (test difficulty; production is
`POW_DIFFICULTY + INCIDENT_POW_MULTIPLIER`): nonce
`000000000001731c`, digest
`00009229aff2876efb1697da51a3872513a2236ea653ca55a5ed5c6cf1d5ae58`.

Full record (52 bytes): preimage ‖ `01` ‖ `08` ‖ nonce:

```
504d4931f44796c8cc1f3fa7ed18f50680108090e2370501880ed41918347145
42dcf3e5d8a6ab386c9b0108000000000001731c
```

### 13.5 Incident scoring

One `(segment, type)` key, all peers at trust 1000, within
`INCIDENT_WINDOW`:

| record | polarity | delivered by |
| --- | --- | --- |
| r1 | report | peer A |
| r2 | confirm | peer A |
| r3 | confirm | peer A |
| r4 | confirm | peer B |

`raw = 4`, `sources = 2`, so `score = min(4, 2) = 2` — a **hint**, not
displayed, and routing-inert. Peer A's three records buy exactly one
point of score. Adding a confirm from peer C gives `raw = 5`,
`sources = 3`, `score = 3` → displayed. A subsequent refute from peer D
gives `raw = 5 − 2 = 3`, `sources = 4`, `score = 3` — still displayed,
since one dissent against three sources is not decisive.

If that segment's speed aggregate then reports a congestion ratio of
0.9 (flowing) with `n ≥ 3`, the incident's remaining TTL is divided by
`CONTRADICTION_DECAY`, and — for an anchored-penalty type — its routing
penalty is 0 throughout regardless of score.

## 14. Repository layout and milestones

```
src/pulsemesh/codec.js       PMC1/PMB1/PMI1/PMD1/PMG1/PMQ1/PMS1/PMF1 + PoW       (§4, §5.3)
src/pulsemesh/bins.js        speed/quality bins, FRESHNESS + Q tables, cells      (§2, §8.1)
src/pulsemesh/store.js       TTL store, three indexes, eviction, digests          (§6.6, §7)
src/pulsemesh/aggregate.js   weighted median, confidence, trust ledger            (§8)
src/pulsemesh/validate.js    rules 1–12, rate limiting                            (§6)
src/pulsemesh/topics.js      topic grammar, shards, rotation                      (§5.2)
src/pulsemesh/sync.js        anti-entropy + padded/split cell fetch               (§4.5, §11.3)
src/pulsemesh/provider.js    LiveTrafficProvider adapter                          (§9)
src/pulsemesh/contribute.js  contributor state machine (engine.snap-based)        (§10.1)
src/pulsemesh/reticent.js    the four emission gates + forwarder rotation         (§10.2)
src/pulsemesh/incidents.js   type table, scoring, contradiction decay, policy     (§2.6, §8.5)
src/pulsemesh/forward.js     PMF1 accept/validate/hold/republish                  (§4.5)
src/pulsemesh/node.js        js-libp2p host wiring, keeper profile                (§5.1, §12)
scripts/pulsemesh_sim.mjs    phase-2 simulation harness
test/pulsemesh_*.test.js     per-module + cross-implementation vectors (§13)
```

Milestones, each independently testable with `node --test`:

- **M1 — codecs + aggregation (no network).** codec/bins/store/
  aggregate/validate against the §13 vectors; property test: two stores
  fed the same records in different orders emit identical digests and
  aggregates.
- **M2 — loopback mesh.** Two in-process "peers" over an in-memory
  duplex; contributor pipeline fed by the demo's simulated drive;
  `route({ live: provider })` reroutes around a synthesized jam
  end-to-end. This closes phase 1 of pulsemesh.md with real protocol
  bytes.
- **M3 — wire.** js-libp2p transports, gossip, sync streams, bootstrap
  verification; two Node processes converge; churn test (kill a peer,
  data survives elsewhere until TTL).
  **Done** — `src/pulsemesh/libp2p.js` binds the transport-agnostic
  MeshNode to a libp2p host (GossipSub StrictNoSign with sha256 message
  ids, one framed request/response per sync stream);
  `scripts/pulsemesh_keeper.mjs` is the §12 keeper as a runnable process;
  `test/pulsemesh_wire.test.js` covers real-TCP convergence with PoW
  validation, contributor churn with zero record loss, padded
  late-joiner recovery, and a keeper child process converging with a
  contributor parent to byte-identical zone digests.
- **M4 — simulation harness (phase 2).** N simulated peers over
  mock transports with configurable density/churn/latency/clock-skew;
  measured outputs: convergence time to identical aggregates, digest
  bandwidth per peer per minute vs density, decoy/batch overhead,
  flood resilience (malformed/replayed/Sybil load vs PoW difficulty).
  These measurements finalize the §3 tunables.
  **Done** — `scripts/pulsemesh_sim.mjs`; results and the resulting
  tunable verdicts in [pulsemesh-benchmarks.md](pulsemesh-benchmarks.md).
  Headline: per-peer cost is flat in peer count (25 KiB/min at 10, 50 and
  150 peers) and linear in contributor density; anti-entropy is 4–5× the
  gossip cost and is the tuning lever; a 200/s hostile flood lands zero
  records for 461 ms of defender CPU.
- **M5 — reticent profile (§10.2).** The measurement that decides
  whether it becomes the default for everyone. Two axes, both against
  the cadence profile as baseline:

  *Utility.* Aggregate coverage and jam-detection latency under
  reticent emission at varying density. The question is how much later
  a jam is detected when only surprises are reported, and at what
  density the answer stops mattering. If detection latency is within a
  bucket or two of baseline, cadence has no remaining argument.

  *Privacy.* Run a trajectory-reconstruction attack directly against
  the record set: given all PMC1s from a simulated courier round plus
  background traffic, how much of the route can an adversary chain by
  adjacency and timing, with and without each gate? Report it as
  recovered-route fraction and as the size of the anonymity set per
  emitted record. This is the number that decides whether couriers can
  contribute, so it must be measured rather than argued.

  **Done** — [pulsemesh-benchmarks.md §8](pulsemesh-benchmarks.md),
  medians over 5 seeds. Privacy is decisive: cadence leaks **100%** of a
  courier's driven route at low density with a mean anonymity set of 1,
  and a median 38.5% even at 32 background vehicles; reticent holds at
  **7.7%**, median and worst case, at every density. Utility is mixed —
  reticence is slower at density 4 (84 s vs 31.5 s), a tie at 12 (95 s vs
  106 s), and faster at 32 (23 s vs 33 s) — at 4–11× fewer emissions.
  Recommendation: adopt reticent as the default and close the
  sparse-onset gap directly (keeper corroboration, or the hint threshold
  for surprise-flagged records), rather than keeping a profile whose only
  advantage appears in the regime where it also leaks the whole route.

## 15. Conformance checklist

An implementation is conformant when:

- [ ] Encodes/decodes every §4 format byte-identically to the §13
      vectors; rejects trailing bytes and oversized records.
- [ ] Enforces validation rules 1–9 unconditionally and 10–12 when the
      leaf cell is available; proofType 0/2 never accepted on the wire.
- [ ] Never accepts a record on a rule-12 denied class, whatever the
      sender claims.
- [ ] Store: reportId dedup, TTL expiry (senders can only shorten),
      caps with lowest-weight eviction, no disk persistence.
- [ ] Aggregation reproduces §13.3 exactly, uses the FRESHNESS/Q tables
      verbatim, ties broken by reportId.
- [ ] Incidents: §13.4 vector byte-identical; scoring reproduces §13.5,
      including `score = min(raw, sources)`; contradiction decay applied;
      `incidentPolicy.suppressedTypes` honoured on both emit and receive.
- [ ] No incident penalty without an independent speed aggregate at or
      below `INCIDENT_ANCHOR_RATIO`; informational types never reach
      `LiveSegmentState` at all.
- [ ] Reports are filed only for the reporter's own snapped position,
      never an arbitrary map point; the user is told at report time that
      a report is public and locating.
- [ ] Emits nothing but PMC1/PMI1/PMB1/PMD1/PMS1/PMF1 payloads;
      contribution cadence, standstill and suppression rules per §10.1.
- [ ] Reticent profile, where selected: all four gates of §10.2, in
      order, with forwarders rotated per record. Never emits a decoy
      contribution under any profile.
- [ ] As a forwarder: validates a PMF1's inner record fully before
      republishing, alters no byte, never chains PMF1 to PMF1, and
      rate-limits per source.
- [ ] Every cell fetch (including provider-triggered) is padded to
      8/16/32 with decoys, shuffled, and split across peers; endpoint
      cells never fetched unpadded.
- [ ] Provider returns `[]` on epoch mismatch, never sets
      `closed: true`, never pre-blends confidence.
- [ ] Router behavior under mesh failure = static routing (this is
      already guaranteed by the engine, but must not be broken by the
      adapter throwing synchronously outside `fetch`).
