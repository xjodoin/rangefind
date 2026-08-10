# PulseMesh benchmarks

Measured results for the PulseMesh implementation in `src/pulsemesh/`.
These are the numbers milestones M4 and M5 of
[pulsemesh-protocol.md §14](pulsemesh-protocol.md) exist to produce: they
finalize the §3 tunables, and they decide whether the reticent profile
(§10.2) becomes the only profile.

Reproduce with:

```bash
npm run bench:pulsemesh:all
```

or separately — `node scripts/pulsemesh_bench.mjs` for per-operation
cost, `node scripts/pulsemesh_sim.mjs --scenario=all` for the mesh
simulation, `node scripts/pulsemesh_wire_bench.mjs` for the real
js-libp2p transport (add `--json` for machine-readable output, `--fast`
for a quick simulation pass).

Everything below runs the real modules over real protocol bytes: the
simulation's peers are `MeshNode` instances, its vehicles drive the real
`createContributor` state machine, and every payload crosses the real
codec, validator, store, and aggregation. Nothing is mocked; in the
simulation the only synthetic parts are the clock, the transport's
latency/loss model, and the road world — and §9 replaces even those with
measurements over actual sockets.

Hardware: Apple Silicon, Node 24, single process. Absolute throughput will
differ elsewhere; the ratios and the scaling shapes are the point.

## 1. Per-operation cost

| Operation | Result |
| --- | --- |
| PMC1 encoded size | **42 bytes** (proofless — §5.4 bonds carry admission) |
| PMB1 batch of 16 | 688 bytes |
| encode PMC1 | 2.95 M ops/s (0.34 µs) |
| decode PMC1 | 3.34 M ops/s (0.30 µs) |
| decode PMB1(16) | 6.15 M records/s |
| SHA-256 (81-byte message) | 1.81 M hashes/s |
| bond verify (once per session, any table size) | 1.0 M ops/s (1.0 µs) |
| weighted median, n = 3 | 3.24 M segments/s (0.31 µs) |
| weighted median, n = 8 | 1.82 M segments/s |
| weighted median, n = 64 | 257 k segments/s (3.89 µs) |
| store insert | ~520 k records/s |
| store memory | **~1.1 KB/record** (~105 MB at the 100 k mark) |
| full TTL sweep, 100 k records | 45–75 ms |
| 32-cell snapshot response | ~10 µs to build, 32 KiB on the wire |

Throughput figures are single-run and drift a few percent between runs;
the ratios are what matter, and they are stable.

The load-bearing number is the record size. A contribution is 42 bytes
because the codec has no field for a coordinate, a trajectory, an
identity, or a precise timestamp — the privacy design and the wire
efficiency are the same decision.

**Store capacity in practice.** `STORE_CONTRIB_CAP` of 262 144 records at
~1.1 KB each is ~290 MB, which is too much for a phone. The measurement
says the default is set for keepers, not handsets; a mobile client should
lower it (it is tunable) to the corridor it actually routes over — a few
thousand records, single-digit MB.

## 2. Proof-of-work economics (historical)

> **Removed from the protocol.** Per-record proof-of-work was deleted
> when §5.4 identity bonds replaced it as the only wire admission
> (proofType 1 is burned). This section stays as the measurement record
> that motivated the change; nothing in it describes the current wire.

| Difficulty | Mining cost (measured) | Verify cost |
| --- | --- | --- |
| 8 bits | 0.2 ms | 0.69 µs |
| 12 bits | 2.7 ms | 0.69 µs |
| 16 bits | 40 ms | 0.69 µs |
| 18 bits | 246 ms mean / 897 ms max | 0.69 µs |
| 20 bits (`POW_DIFFICULTY`) | 719 ms mean / 1.87 s max | 0.69 µs |
| 22 bits | 4.50 s mean / 7.82 s max | 0.69 µs |
| 24 bits (incidents) | 10.0 s mean / 17.2 s max | 0.69 µs |

Mining at 8/12/16 bits comes from §1; the 18–24 bit rows are measured
directly by `bench:pulsemesh:bond` and confirm the 2^d model (each 4-bit
step costs ~16×). The **max** column matters as much as the mean: nonce
search is geometric, so the tail is long and a mean hides it.

The asymmetry is the defense: **~1 M hashes to create a record, one hash
to reject it**. At 20 bits an honest contributor spends ~0.7 s of one
core per emission here and ~2.9 s on a phone — acceptable against a 15 s
cadence, and amortising to 1.4–2.5% of one core under the reticent
profile's much lower emission rate (§8). An attacker pays the same per
forged record while a defender rejects 1.46 M/s.

The 24-bit incident difficulty costs a reporter 10 s on this desktop and
~40 s on a phone — see §14, which measures what that does to the UI
thread and what it takes to survive it. That is a deliberately felt cost
for a claim that nothing physical constrains, and
it is still only a speed bump — the protocol's own §8.5 distinct-peer cap
and speed anchor are the actual defense, and they are verified by the
conformance tests rather than benchmarked here: a fabricated incident on
a flowing road produces a zero penalty at any score, so there is no
throughput number to report.

## 2b. RLN measured against proof-of-work (historical)

The design concedes proof-of-work is a fallback and names blind tokens as
the recommended Sybil control — with a credential issuer as the honest
cost, the first per-user server in the whole story. RLN (rate-limiting
nullifiers, as used by Waku) looked like it might dodge that issuer
entirely, so it was measured rather than argued. Same machine, same run,
against the PoW baseline above:

| | Proof-of-work (20 bits) | RLN (tree depth 20) |
| --- | --- | --- |
| Prove / mine | 579 ms* | **235 ms** — 2.5× faster |
| **Verify** | **0.69 µs** | 6.3 ms — **9,130× slower** |
| Proof size | **8 bytes** | 1,250 bytes |
| Record on the wire | **50 bytes** | 1,292 bytes — 26× |
| Client artifacts | **none** | 5.7 MB of wasm + zkey |
| Rate limiting | per-connection token bucket | **per-identity, cryptographic** |

\* This run's PoW sample. Nonce search is geometric, so single-run means
scatter widely; §2's table gives 719 ms over more samples. The comparison
holds either way — the axis RLN loses on is verification, by four orders
of magnitude.

**RLN wins the comparison I said would decide it, and loses on
everything else.** Proving is 2.5× cheaper, which was the number I asked
for. But three consequences make it unusable here:

1. **It breaks the flood defence.** One core rejects 1.45 M hostile PoW
   records per second and 159 RLN ones. An attacker who cannot produce a
   valid proof still forces the verifier to *run* the verification to
   find out, so garbage proofs at 159/s saturate a core — a DoS vector
   proof-of-work simply does not have.
2. **It does not fit the wire format.** A 1,250-byte proof puts the
   record at 1,292 bytes against a `MAX_RECORD_BYTES` of 96, and would
   take per-peer bandwidth from a measured 25.5 KiB/min to roughly
   660 KiB/min. The 42-byte record is load-bearing for the whole
   bandwidth story.
3. **It does not remove the issuer.** This was the actual hypothesis, and
   it is false: `verifyProof` checks a Merkle root, so every verifier
   needs the current membership set of registered identities, and
   somebody maintains it. rlnjs ships exactly two registries — an
   in-memory one and a chain-backed one. RLN relocates the credential
   issuer onto a registry or a blockchain; it does not delete it.

**Verdict: proof-of-work stays.** The one thing RLN offers that we
genuinely lack is *per-identity* rate limiting — our token bucket is
per-connection, so it bounds a peer rather than a person — but that
property costs a 26× record, a 9,000× verification, a 5.7 MB client
download, and a registry. Blind tokens (phase 4) remain the better route
to the same property.

Reproduce: `npm i -D rlnjs`, then prove with `RLNProver` against
`getDefaultRLNParams(20)`. Note the Semaphore v3 API rlnjs depends on —
the RLN identity secret is `identity.secret` and its commitment is
`calculateIdentityCommitment(secret)`, not the Semaphore commitment. The
dependency is not kept in this repo: it was an experiment with a negative
result.

## 3. Validation throughput

| Path | Throughput |
| --- | --- |
| rules 1–12, no proof (loopback) | 13.9 M records/s |
| rules 1–12, bonded wire (proofType 3) | **12.7 M records/s** |

The wire path got 10× faster when per-record PoW verification became a
bond-set lookup: one core now rejects 12.7 million hostile records per
second, and the only way to be *accepted* at all is to have paid a §5.4
mint for the delivering identity — which a ledger ban then forfeits.
This is the ratio that makes a
leaderless mesh defensible without an authority.

## 4. Convergence

Every peer holds identical zone digests after emissions stop, at 15%
gossip loss:

| Peers | Live records | Convergence |
| --- | --- | --- |
| 10 | 48 | 12 s |
| 25 | 49 | 10 s |
| 50 | 50 | 12 s |

Convergence is **flat in peer count** — roughly one anti-entropy interval
(`ANTI_ENTROPY_SECONDS` = 10 s ± 3) plus a repair round. Adding peers adds
repair paths, not delay. The 15% loss rate is deliberately worse than a
real WebRTC mesh; anti-entropy absorbs it without a retransmit layer,
which is the whole reason digest-before-download exists.

## 5. Bandwidth and scale

**Per-peer cost as the mesh grows** (20 contributing vehicles, constant):

| Peers | Gossip in | Relay out | Anti-entropy | Total per peer |
| --- | --- | --- | --- | --- |
| 10 | 4.0 KB/min | 4.3 KB/min | 17.8 KB/min | **25.5 KiB/min** |
| 50 | 4.1 KB/min | 4.3 KB/min | 18.7 KB/min | **26.5 KiB/min** |
| 150 | 4.1 KB/min | 4.3 KB/min | 18.2 KB/min | **26.0 KiB/min** |

This is the result the architecture is built for: **per-peer cost is flat
in the number of peers.** A mesh of 150 costs each participant the same as
a mesh of 10, because a peer's traffic is set by how much is happening
*near it*, not by how many people are using the system. There is no
component whose load grows with the user base, which is precisely the
component a centralized service has to keep paying for.

**Per-peer cost vs contributor density** (15 peers, varying vehicles):

| Vehicles in the zone | Records/3 min | Total per peer |
| --- | --- | --- |
| 5 | 60 | 7.5 KiB/min |
| 20 | 240 | 31.5 KiB/min |
| 60 | 720 | 111.2 KiB/min |

Density is the real cost driver, and it scales linearly, as it must —
more traffic to describe means more bytes. At 60 vehicles in one z15
neighborhood a peer pays ~6.5 MB/hour, which is too much for a metered
phone. Two levers exist and are already in the protocol: subscribe to a
shard subset rather than all 16 (§11.4), and let the reticent profile cut
emissions (§8 below measures a 4–11× reduction). Raising
`ANTI_ENTROPY_SECONDS` looks like a third and is not — see the negative
result immediately below.

**Anti-entropy is 4–5× the gossip cost and dominates the bill**, and
chasing that number produced the most useful negative result in this
document.

The obvious fix — back off the interval when rounds find nothing — was
implemented and then reverted. It saved 27–39% of anti-entropy bytes and
cost the property the mesh exists for: convergence went from a flat ~12 s
to 10/26/41 s at 10/25/50 peers, because a peer that has gone quiet is
exactly the peer that will be slow to notice it is now behind. Adding a
"reset on new gossip" signal restored convergence and gave back nearly
all the savings. **A mesh whose guarantee is that peers agree cannot buy
bandwidth with agreement latency.**

What does work is gating the *content* rather than the cadence. The
requester now sends a 12-byte zone fold (a count plus the XOR of its
reportId prefixes) in PMG1; a responder whose fold matches answers with a
12-byte PMN1 instead of a full digest. Rounds happen just as often, so
convergence is untouched at a flat 12–13 s. Measured directly:

| Zone state | Elision rate | Bytes per round |
| --- | --- | --- |
| Records still arriving | 0% | full digest |
| Converged | 100% | ~37 bytes |

The 0% row is the honest one. **In an actively driven zone the digest is
not waste — it is the cost of genuine disagreement**, and no encoding
trick removes it. The saving is real but it lands in quiet zones and
quiet hours, which is why the aggregate bandwidth numbers above barely
move: in this simulation everybody is always driving. A deployment's
overnight and inter-peak hours are where it pays.

Remaining candidates for phase 3: restrict rounds to the shards a peer
actually subscribes to, and scope digests below the zone for very dense
areas.

## 6. Adversarial load

11 799 hostile records delivered to one peer at 200/s over a minute,
mixing malformed fields, missing proofs, replays, and valid-PoW spam:

| Outcome | Result |
| --- | --- |
| Hostile records accepted into the store | **0** |
| Honest records accepted during the attack | 24 (unaffected) |
| Defender CPU for the whole attack | 461 ms |
| Drops by rule | rule 3 (fields) 2 950, rule 5 (proof) 2 934, rule 7 (rate) 5 759, rule 8 (topic) 156 |

Nothing hostile reaches the store, honest traffic is unaffected, and the
defense costs under half a second of CPU for the entire flood. The
per-peer token bucket (rule 7) absorbs the majority — a single peer
identity simply cannot deliver more than `RATE_SUSTAINED` records/s no
matter how well-formed they are, which is what bounds a valid-PoW
attacker to the same ceiling as an honest one.

## 7. Churn

Half the consumer peers and *all* contributors are killed mid-TTL:

| Measure | Result |
| --- | --- |
| Live records before churn | 70 |
| Still held on surviving peers | 70 (**100%**) |
| Late joiner's recovery from survivors alone | 100% |

A departing node removes nothing, so records replicated by gossip survive
on every peer that heard them, and a peer joining after the contributors
are gone reconstructs the full live set from survivors via anti-entropy.
Availability comes from replication and TTL, not from anyone staying
online.

## 8. M5 — the reticent profile decides itself

The measurement the spec called for: a courier drives a fixed serpentine
route with dwell stops through background traffic, under each profile.
Utility is jam-detection latency and aggregate coverage; privacy is a
trajectory-reconstruction attack run directly against the record set,
chaining records by segment adjacency and bucket timing and stepping only
where the continuation is unambiguous.

Detection latency is noisy and the recommendation rests on it, so every
cell below is the **median of 12 seeds**, with the number of runs that
detected the jam at all reported alongside.

Two detection thresholds are reported, because they are different events.
The provider hands a state to the router at `AGG_HINT_REPORTS` (n = 2,
confidence capped at 0.30) — that is when a jam first *changes a route*.
`AGG_MIN_REPORTS` (n = 3) is when it becomes a full-confidence aggregate.
An earlier version of this harness measured only the latter and therefore
reported the channel as slower than the router it feeds.

| Background vehicles | Profile | Emissions /veh/min | Detected (n ≥ 2) | Confirmed (n ≥ 3) | **Route recovered** (median / worst) | Anonymity set |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | cadence | 4.0 | 0/12 | — | **100% / 100%** | 1.00 |
| 1 | reticent | 1.6 | 0/12 | — | **7.7% / 7.7%** | 1.56 |
| 4 | cadence | 4.0 | 9/12 — 214 s | 229 s | **84.6% / 100%** | 1.05 |
| 4 | reticent | 0.66 | 6/12 — **44 s** | 44 s | **7.7% / 7.7%** | 1.56 |
| 12 | cadence | 4.0 | 12/12 — **40.5 s** | 55.5 s | **84.6% / 100%** | 1.14 |
| 12 | reticent | 0.47 | 12/12 — 116.5 s | 116.5 s | **7.7% / 7.7%** | 1.56 |
| 32 | cadence | 4.0 | 12/12 — **16.5 s** | 31 s | 27% / 69.2% | 1.81 |
| 32 | reticent | 0.36 | 12/12 — 21.5 s | 21.5 s | **7.7% / 7.7%** | 1.56 |

"Route recovered" is the fraction of the courier's actually-driven
distinct segments an adversary reconstructs — not the fraction of its
emissions, whose denominator shrinks with the profile and would flatter
reticence unfairly. At density 1 neither profile detects the jam because
nobody drives the jammed corridor; that row is a privacy measurement only.

**The privacy result confirms the spec's own indictment of cadence, and
it is not close.** A cadence contributor's route is recoverable at
84.6–100% of driven segments up to density 12, with a mean anonymity set
of ~1, from records containing no identifier whatsoever. The design
document's claim that "a walk across adjacent segments in consecutive
buckets is reconstructible from the record set alone" is not a worry; it
is a measurement, and it reproduces across every seed. Even at 32
background vehicles cadence still leaks a median 27% with a worst case of
69%. The reticent profile holds at **7.7% — median *and* worst case, at
every density**: the adversary's best chain never leaves a single segment.

**The utility result is mixed, and it does not favour reticence.** At
density 12 cadence detects in 40.5 s against reticence's 116.5 s — nearly
3× faster, with every run detecting under both. At 32 it is 16.5 s vs
21.5 s, close. At density 4 the comparison inverts in an interesting way:
reticence is *faster when it works* (44 s vs 214 s) but works less often
(6/12 vs 9/12). That is the sparse-onset gap, and it is a **reliability**
gap rather than a latency one — a distinction the earlier single-threshold
measurement could not see.

One incidental finding worth keeping: for reticence the two thresholds
are always identical, while for cadence they differ by 15 s or more.
Reticent corroboration arrives simultaneously because surprises are
shared events — everyone in the jam reports it at once — so n goes from 0
to 3 together rather than trickling.

Coverage is far lower under reticence (0.00–0.03 vs 0.25–0.39) and that
is intended, not a regression: an unreported segment means "reality
matches the static metric", and the router's degrade-to-static path is
then the correct answer.

**Recommendation, revised by the fuller data: keep both profiles and
choose by route publicity — which is exactly what
[threads §13](pulsemesh-threads.md#13-cross-channel-isolation) prescribes.** The
measurement validates that rule rather than overturning it:

- **A published route** (transit, school runs, snow clearing) should use
  cadence. Correlating its contributions to its timetable reveals nothing
  a public document does not already say, so the 84.6–100% route recovery
  costs nothing — and cadence is the faster, more reliable detector at
  every density where detection matters.
- **An unpublished route** (couriers, field service) must use reticence.
  There the route *is* the customer list, the 11× privacy improvement is
  the whole point, and the latency cost is worth paying.

The earlier recommendation here — "make reticent the default for
everyone" — rested on a five-seed run and a detection criterion stricter
than the router's own. With twelve seeds and the corrected threshold,
cadence's latency argument survives, and the spec's publicity test turns
out to be the right axis after all.

## 9. The real wire (M3)

Sections 4–8 measure a simulated transport. This one measures actual
js-libp2p sockets: six peers in a full mesh over TCP with Noise
encryption, Yamux muxing, and GossipSub in the §5.1 profile (signing
disabled, message id = the first 20 bytes of SHA-256 of the payload),
plus the `/rangefind/pulsemesh/1/sync` stream protocol. Proof-of-work is
pre-mined at 8 bits so mining does not dominate the transport
measurement; verification still runs on every record.

| Measurement | Result |
| --- | --- |
| Gossip delivery, publish → remote store, p50 | **1.00 ms** |
| Gossip delivery, p95 / max | 2.23 ms / 3.72 ms |
| Sustained ingest, publish → all peers | 3 282 records/s (16 411 record-deliveries/s across 5 peers) |
| PMG1 digest round trip, p50 / p95 | 0.35 ms / 0.70 ms |
| PMQ1 32-cell snapshot, p50 / p95 | 1.10 ms / 2.22 ms |
| Snapshot response size | 57.6 KiB |
| Cold-start recovery, 300 records | **1 anti-entropy round, 4 ms** |
| Padding overhead on that recovery | 2.00× (16 cells requested for 8 wanted) |
| Publisher gossip out | 54 bytes/record |

These are the machine and the run that produced §9's narrative; §9a below
re-measures the ingest and latency rows against the §5.1 topic validators
in a paired before/after, which is the comparison to read for that
change.

The delivery figure is the one worth dwelling on. A contribution goes
from `publishRecord` on one host to *validated and stored* on another —
across a real socket, through Noise, through GossipSub, through
proof-of-work verification and all twelve validation rules, into the
three store indexes — with a median of **1.00 ms**. Live traffic is not a
latency problem for this design; it never was, because a jam is a 15-second
bucket and the transport resolves in one.

Cold-start recovery is the other notable result: a peer that has just
joined pulls an entire zone's 300 live records in **one anti-entropy
round and 4 ms**, because the digest tells it exactly which cells differ
and one padded PMQ1 batch covers them. The 2.00× padding overhead is the
privacy tax, paid in full and still cheap.

Caveats, stated plainly: this is loopback, so it measures protocol and
stack cost with the network removed rather than internet latency — add
the real RTT for a deployment estimate. It is TCP, the keeper-to-keeper
profile; browser peers will use WebRTC or WebSockets on the same adapter
and should be measured separately. And the 54 bytes/record publisher
figure counts one GossipSub copy per mesh peer, so it scales with mesh
degree, not with the network.

### 9a. What the §5.1 topic validators cost

Validation moved *ahead of* the forward decision — a GossipSub topic
validator now runs the whole §6 pipeline before a message is relayed, so
that relaying implies validating. The work per message is the same work
as before; what changed is where it sits, plus one SHA-256 to key the
validate-once verdict on each side of the forward decision.

Paired runs on one machine, five each, `--peers=6 --records=1200`
(1 080 records in the burst phase, 5 400 record-deliveries), medians:

| | Before | After |
| --- | --- | --- |
| Gossip delivery p50 | 0.99 ms | **1.00 ms** |
| Gossip delivery p95 | 2.20 ms | **2.20 ms** |
| Burst wall clock | 328 ms | **351 ms** |
| Sustained ingest | 3 297 records/s | **3 077 records/s** |

End-to-end delivery latency does not move: the pipeline runs once either
way, and running it a few hundred microseconds earlier in the same task
is invisible at the millisecond scale that matters. Peak burst throughput
gives up **~7%**, about **+4 µs per record-delivery**, and the run-to-run
spread (2 700–3 400 records/s after, 3 000–3 400 before) is wide enough
that the two distributions overlap — this is at the noise floor, not
above it. It buys a property no amount of throughput substitutes for: a
bonded peer no longer vouches for bytes it never read.

Reproduce with `npm run bench:pulsemesh:wire`.

## 9b. The whole thing, end to end

`npm run demo:pulsemesh` runs the complete loop against the real OSM
route graph shipped in this repo — 4096 leaves of Quebec roads — with
nothing stubbed:

1. Opens the graph and takes its `sourceHash` as the mesh epoch.
2. Finds a corridor that has a viable alternative (probing with an
   in-memory provider, so what the mesh demonstrates is the reroute
   rather than the search).
3. Starts four real js-libp2p peers over TCP and forms a GossipSub mesh.
4. Drives three vehicles through the jam with the real contributor
   pipeline — snap, quality bin, speed smoothing, cadence, PoW — emitting
   45 PMC1 records of ~50 bytes each.
5. Prints one record field by field, to show what a contribution
   actually contains.
6. Confirms every peer recomputed byte-identical state with no consensus
   protocol and no server.
7. Routes under the live metric and reports the outcome.
8. Stops every contributor, expires the records by TTL, and routes again.

A representative run:

| Stage | Result |
| --- | --- |
| Static route | 191 edges, 80.2 km, 59.0 min |
| Records published | 45 × 50 bytes (2.2 KiB total) |
| Delivered + validated at the driver | 45 records in 66 ms |
| Aggregates | 15 segments, all corroborated at n = 3 |
| Peer agreement | byte-identical digests |
| Live states → edges adjusted | 15 states → **43 edges** |
| ETA | 59.0 min static → 75.2 min live |
| Outcome | **rerouted** — 33% of the jammed stretch still used |
| After contributors leave + TTL | 0 records, route returns on the static metric |

The 15 → 43 line is the segment-identity design paying off: one physical
segment id fans out to every junction-expanded approach copy of that
road, so contributors report a road once and the router applies it
everywhere that road appears in the search graph.

The last row is the safety property the whole contract rests on. Every
contributor is gone, every record has expired, and the router still
answers — on the static metric, with the same ETA it gave before the
mesh existed.

## 9c. The thread channel

The second channel — authenticated tracking of one vehicle for a bounded
audience ([pulsemesh-threads.md](pulsemesh-threads.md)) — is implemented
through milestone T4 and measured by
`npm run bench:pulsemesh:threads`. Everything below runs the real
WebCrypto path, so the same numbers apply in a browser.

**Crypto and codec, per update:**

| Operation | Result |
| --- | --- |
| PMT1 record (fine mode, no run plan) | **178 bytes** |
| PMTP body | 95 bytes (31-byte preimage + 64-byte signature) |
| Cumulative outcome map, per record | +17 B at 50 stops, +56 B at 200 |
| Follow link — capability plus verification identity | **78 bytes** |
| Key schedule from the capability | 0.06 ms, once per thread |
| Sign (publisher) | 0.064 ms |
| Verify (subscriber) | 0.066 ms |
| Seal / open (AES-256-GCM) | 47 k / 50 k per second |
| **Full receive path** (decode + open + parse + verify) | **~0.09 ms → ~11 k updates/s** |

Throughput figures here drift a few percent between runs, as in §1; the
sizes are exact. A subscriber following one fine-mode thread at 5 s
cadence spends about 0.002% of one core on crypto. The cost is not the reason to prefer coarse
mode; safety is.

**What a flood costs a phone.** §7 drops an unknown tag at step 3, before
any crypto runs:

| Measure | Result |
| --- | --- |
| Records with an unguessable tag rejected | **~1.2–1.3 M/s on one core** |
| Crypto performed on them | none |

An attacker cannot make a subscriber do asymmetric work without the
capability, because they cannot compute an address it listens on. This is
the same shape as the traffic channel's proof-of-work asymmetry, reached
by a different route: there the defence is that forging is expensive,
here it is that addressing is impossible.

**Measured bandwidth:**

| Measure | Result |
| --- | --- |
| Fine thread bandwidth | **35.6 B/s** |
| Per thread-hour | **125 KB** |
| 500-bus fleet-hour | **61 MB** |
| Coarse "two orders of magnitude below" | **20.0% of fine** — one order, not two |
| Subscriber cache | 42 KiB at the default ring and measured record size |

Coarse costs about a fifth of fine. It remains the right default for child
transport because the decision is about what a leaked capability reveals,
not only bytes.

**Catch-up availability is controlled by peers asked, not audience size.**
The benchmark asks how many concurrent subscribers a run needs
before a late joiner reliably finds the gap. Modelling subscribers that
sleep and wake independently (awake ~25% of the time), with every peer
running the real cache and the real validator:

| Audience | Providers asked | Late joiner finds a cache | Fraction of the recoverable window |
| --- | --- | --- | --- |
| 1 | 1 | 45% | 0.20 |
| 10 | 1 | 53% | 0.19 |
| 100 | 1 | 53% | 0.25 |
| 5 | 3 | 83% | 0.37 |
| 10 | 3 | **95%** | 0.47 |
| 100 | 3 | 93% | 0.46 |
| 30 | 8 | **100%** | 0.73 |
| 100 | 8 | **100%** | 0.59 |

**Audience size barely matters; the number of providers a joiner asks
decides everything.** Asking one peer succeeds about half the time
whether the run has 2 subscribers or 100 — the odds that the peer you
picked happened to be awake do not improve with audience. Asking three
takes it to 83–95%, eight to 100%. The actionable rule is a floor on peers
asked, not a cache incentive for small audiences.

The same run pins a limit the design never stated: **catch-up can never
recover more than `THREAD_MAX_AGE` of history** — two minutes — because
the freshness validator rejects anything older regardless of who cached it. "Catch-up"
means the last two minutes, not the run so far, and a UI must not promise
otherwise.

**The §9 thesis, tested rather than argued.** `test/pulsemesh_thread_loopback.test.js`
runs a publisher and subscriber over the real OSM route graph and asserts
that the subscriber's ETA moves when a jam is injected into the *traffic*
channel's provider — in both shapes, the multi-stop school run and the
two-point delivery. The subscriber sends nothing at any point: it is a
listener, and its arrival estimate is computed entirely locally from a
broadcast position. That is the property no server-side ETA can have,
and it now has a test rather than a paragraph.

## 9d. In a browser

`npm run build:osm-demo` ships PulseMesh to the browser as three bundles,
split so a page pays only for what it uses:

| Bundle | Size | What it is |
| --- | --- | --- |
| `pulsemesh.browser.js` | **106 KB** | codecs, store, aggregation, validation, incidents, the provider — everything except transport |
| `pulsemesh-threads.browser.js` | **46 KB** | the thread channel; WebCrypto only, no polyfill |
| `pulsemesh-libp2p.browser.js` | **2.5 MB** | the libp2p transport, loaded on demand |

The 24× gap between the core and the transport is why they are separate
entry points. A page that consumes live traffic from peers it already has
never loads the third bundle at all, and the core has no static import
that could drag it in.

Getting there required moving bootstrap signature verification off
`node:crypto` and onto the same WebCrypto Ed25519 path the thread channel
uses. That was worth doing regardless: one implementation now covers
Node, browsers, and mobile hosts, and neither browser bundle references a
Node built-in.

**Verified in a real browser**, not asserted: with the OSM demo served
locally, three simulated vehicles published 45 contributions through the
real contributor pipeline into an in-tab mesh, the consumer validated and
stored all 45, aggregated 15 segments, and the router applied 26 edge
adjustments — moving the route off the jammed stretch completely (100% of
it used before, 0% after) and the ETA from 9.3 to 14.7 minutes. No
console errors, and no server involved in any of it.

`examples/osm-geo/public/pulsemesh-demo.js` runs in either of two modes.
`local` puts the peers inside the tab, so the demo works from a static
host with no infrastructure. `?keeper=<multiaddr>` joins the real mesh
over WebSockets instead, and falls back to `local` — with the reason
surfaced — when the keeper is unreachable, because a keeper being down
must never break the page.

## 9e. On a phone

`rangefind/pulsemesh/mobile` is the app-side contributor the design
counts on: browsers lose `watchPosition` when backgrounded, so the phone
is where sustained observations actually come from
([pulsemesh.md delta 4](pulsemesh.md)). It bundles to **102 KB** with no
transport — a host wires in whatever network it has, and the pipeline
runs locally without one.

Three behaviours are pinned by tests rather than left to an integrator:

- **Contribution is off unless the app asks for it.** A phone that reads
  traffic must never start publishing where its owner drives because a
  library defaulted it on. `setContributing(true)` is an explicit act.
- **A low battery stops it.** §10.1 rule 5: below 20% and not charging,
  emission pauses. A traffic layer that flattens a phone loses that
  contributor permanently.
- **The reticent profile is the mobile default** rather than an opt-in,
  because a phone's owner did not sign up to publish a trajectory.

It is wired into the Wayfind Android app, in its own repository: the engine interface gained
`pulseMeshStatus`, `setContributing` and `offerLocation`; the WebView
bridge exposes a `pulseMesh` method; `MapsViewModel` feeds every
`LocationManager` fix through it and routes under the live metric when
the mesh has anything to say. The Kotlin compiles clean.

One thing the integration made obvious, and which is worth stating
because it is easy to get wrong: **every peer must validate against the
same constants**. An early version of the test gave the contributing
phone a lower proof-of-work difficulty than the reading phone, and every
record failed rule 5 — correct behaviour, and exactly why §4.7 puts the
tunables inside a *signed* bootstrap rather than leaving them to each
client.

## 10. What this replaces

The per-peer flat-cost result (§5) is what makes the mesh an alternative
rather than an optimization. A conventional live-traffic service must
serve every client's corridor from its own infrastructure; its egress
grows linearly with users, forever. Taking a modest 40 KiB corridor
refresh twice a minute:

| Users | Mesh operator egress | Centralized operator egress |
| --- | --- | --- |
| 10 000 | **0** | 32 959 GiB/month |
| 1 000 000 | **0** | 3 295 898 GiB/month |
| 100 000 000 | **0** | 329 589 844 GiB/month |

The mesh operator's egress is zero because live state never touches an
operator's server: the only assets published are the immutable,
content-addressed route graph and map packs the engine already serves
from a CDN, plus a signed `mesh-bootstrap.json` of a few hundred bytes
per epoch. Live traffic costs the operator nothing at any scale, because
there is nothing in the live path to operate.

That is the substantive claim, and it should be stated without
overreaching. What genuinely disappears is the per-user server cost, the
trajectory database, and the account that ties a route to a person. What
does *not* disappear: the credential issuer of phase 4 is a real service
if blind tokens are adopted (§ pulsemesh.md delta 5), keeper nodes in
sparse regions need someone to run them, and connectivity infrastructure
— bootstrap peers, rendezvous, Circuit Relay — is federated but not free.
The honest framing is that PulseMesh removes the *data* service and its
per-user scaling, not every server in the world.

## 11. Status against the milestones

| Milestone | Status |
| --- | --- |
| M1 — codecs + aggregation, no network | **done**: §13 vectors byte-identical, order-independence property test |
| M2 — loopback mesh | **done**: `test/pulsemesh_mesh.test.js` closes the contribute→gossip→aggregate→route loop on the real engine |
| M3 — wire (js-libp2p transports) | **done**: `src/pulsemesh/libp2p.js` + `scripts/pulsemesh_keeper.mjs`; real-TCP convergence with PoW, churn without loss, padded late-joiner recovery, and a keeper child process converging with a contributor parent to byte-identical digests (`test/pulsemesh_wire.test.js`) |
| M4 — simulation harness | **done**: §4–§7 above |
| M5 — reticent profile measurement | **done**: §8 above; the answer validates threads §13 rather than overturning it |
| T1 — thread crypto, codecs, link | **done**: current vectors byte-identical through WebCrypto |
| T2 — loopback thread + local ETA | **done**: the ETA moves under a traffic-channel jam, on the real OSM graph |
| T3 — catch-up and discovery | **done**: a late joiner recovers from another subscriber with no host (§9c) |
| T4 — cross-channel contribution rules | **done**: including the dwell-bias failure rule 4 exists to prevent |
| T5 — coarse field pilot | not started: a deployment, not code |

The wire tests put a number on the transport too: three real libp2p TCP
nodes form a mesh, gossip 24 PoW-carrying records through full
validation, and converge — and separately a keeper in another OS process
converges with a contributor — in under 2 s each on localhost, mesh
formation included.

## 12. Tunables these measurements finalize

| Constant | Default | Verdict |
| --- | --- | --- |
| `RATE_SUSTAINED` / `RATE_BURST` | 2/s / 40 | keep — bounds a valid-PoW flooder to an honest peer's ceiling |
| `STORE_CONTRIB_CAP` | 262 144 | **lower on mobile** — ~1.1 KB/record makes the default ~290 MB |
| `AGG_MIN_REPORTS` | 3 | keep for aggregates; **consider 2 for surprise-flagged records** to close the sparse-onset gap (§8) |
| reticent profile | opt-in | **keep both; choose by route publicity** (§8) — mandatory for unpublished routes, cadence for timetabled ones |
| `ANTI_ENTROPY_SECONDS` | 10 | **do not back it off** (§5) — gate the digest content on a zone fold instead |
| `THREAD_MAX_AGE` | 120 s | keep, but it silently caps catch-up at two minutes (§9c) |
| providers asked per catch-up | unspecified | **specify a floor of 3, prefer 8** (§9c) |
| `BOND_BIRTHDAY_BITS` | 44 | keep — 256 MiB table, 1.8 s desktop mint, 96 solvers/24 GiB GPU (§14.5); raise to 48 only where miners are desktop-class |
| ~~`POW_DIFFICULTY`~~ / ~~`INCIDENT_POW_MULTIPLIER`~~ | — | **removed with per-record PoW** — §5.4 bonds are the only admission; incidents publish at the tap |

## 13. Defects these measurements and the conformance audit found

Building the harness and auditing the implementation's wire budget turned
up nine real defects, all now fixed and covered by tests. They are worth
recording because several are the kind that a passing test suite happily
hides:

1. **The provider was never wired to the padded fetch.** `MeshNode`
   constructed its provider without a `fetchCells` hook, so §9 step 2 was
   dead code: the store filled from gossip only and a route query never
   demand-fetched its corridor. The privacy machinery was correct and
   tested — it simply was not on the path.
2. **§11.2 corridor computation did not exist.** Nothing rasterized a
   route to z15 cells or broadened endpoints; `ENDPOINT_RINGS` was being
   used as a decoy-pool radius, a different §11.3 concept that happens to
   share the value 2. `corridorCells()` and `MeshNode.followCorridor()`
   now implement it.
3. **An `appliesBoth` incident was scored and penalized twice** — once
   per direction — so ice reported from both sides of a road produced
   double the score and double the routing penalty for one incident.
4. **The forwarder skipped the replay check**, validating without the
   store, which made it a replay amplifier: its own store deduped
   afterwards, but the gossip had already gone out.
5. **Post-report measurement suppression was bypassed by a surprise.**
   §10.4's "suppress PMC1 for `RETICENT_GAP` after filing a report" only
   set the gate-3 timer, and a surprise bypasses gate 3 — meaning it
   failed in exactly the case it exists for, since a reporter is usually
   sitting in the surprise they just reported.
6. **Gate 4 silently degraded to a direct publish** when no forwarder was
   available, dropping the protection the profile was selected for
   without saying so. It now suppresses instead.
7. **A deployment's `suppressedTypes` were honoured on receipt but not on
   emit**, so a deployment that suppresses police reporting still had its
   own clients minting and publishing those records.
8. **`MAX_GOSSIP_BYTES` was never enforced** — an arbitrarily large
   payload reached the decoder.
9. **`EPOCH_OVERLAP` and `UNSUB_LINGER` were unused constants.** The
   consumer never subscribed to the previous epoch's topics during
   handover, and left a zone the instant a corridor moved off it,
   announcing the change as an unsubscribe.
10. **The provider filtered its output by the engine's fetched areas** —
    found by the end-to-end demo, not by any test. The areas an engine
    passes are the leaves it has fetched *so far*; filtering states to
    them hid every jam outside that set, which defeats the engine's
    "leaves referenced by live states join the query's context set, and
    their overlay shortcuts are suppressed" mechanism — the thing that
    makes jams exact in the first place. In the demo it showed up as a
    fully corroborated 15-segment jam producing zero applied states.
    Areas drive *fetching*; they must never filter results.
11. **An oversized leaf bbox rasterized without bound.** A quarter-degree
    rural leaf becomes thousands of z15 cells, so one route query would
    have issued thousands of snapshot requests. Capped, with the area's
    centre neighbourhood fetched instead.

12. **The sync-stream framing misparsed a split length prefix.** The
    frame assembler used the engine's `readVarint`, which cannot
    distinguish "incomplete" from "complete": given the first byte of a
    two-byte prefix it returns the low seven bits as a finished number.
    A 300-byte frame arriving as `ac` then `02` reads as length 44 — and
    worse, a 128-byte payload's prefix `80 01` reads as length **0** on
    its first byte, framing an empty message, consuming the byte, and
    silently swallowing the real one. Every PMS1 snapshot response and
    every 32-cell PMQ1 has a multi-byte prefix, so this was a live
    correctness bug waiting on TCP segmentation. Fixed with a reader that
    returns null while the prefix is incomplete, and covered by a test
    that feeds frames one byte at a time (verified to fail against the
    old code, at exactly the 128-byte case).

13. **`provider.aggregates()` moved the trust ledger.** A read-only
    accessor applied §8.4 feedback on every call, so a UI polling it at
    1 Hz to draw jams drove every delivering peer to a trust bound —
    changing the weights the *next* route was computed from. Trust is
    supposed to move when a record is judged against a later aggregate,
    not when someone looks.
14. **The declared `libp2p` peer dependency was a major the code cannot
    run on.** The adapter is written against, and only ever tested
    against, the v2 duplex stream API; the manifest asked for v3.
    Everything green in CI, and broken for anyone who followed the
    manifest.
15. **A sync responder had no read deadline**, so a peer that opened a
    stream and went quiet pinned it indefinitely — cheapest against
    keepers, whose entire job is answering. **`network.close()` also
    leaked**: it left its gossip topics subscribed on a host that
    outlives it by contract, and left forwarder timers to fire into a
    stopped host.
16. **Cell fetching was bounded per area but not in total, and issued
    serially.** A long route touching 80 leaves could serialize 60+ round
    trips inside one `route()` call — seconds of blocking on a real WAN.

Defects 10 through 16 are the interesting group, and none came from the
test suite. Ten and eleven surfaced within minutes of running the whole
thing against a real 4096-leaf OSM graph. Twelve came from asking what
the framing does when a length prefix straddles two TCP segments — a
question no test asked, because loopback almost never splits there.
Thirteen through sixteen came from an adversarial review of code that was
already passing every test I had written for it.

Three different methods, three disjoint sets of bugs. Unit tests check
the parts you thought of; an end-to-end run checks the seams between
them; and an adversary reading the code finds what neither was looking
for. The uncomfortable one is fourteen: a dependency declaration that was
wrong in the only direction a test suite structurally cannot catch,
because the suite resolves the dev pins and never sees the range
published to everyone else.

The pattern in 1, 2 and 9 is the same: a constant defined, a mechanism
specified, and nothing calling it. Grepping for each §3 constant's use
sites found all three, and is worth repeating after any protocol change.

## 14. Is there a lighter puzzle than proof of work?

> Sections 14.1–14.4 are the investigation that ran while per-record
> proof-of-work was still on the wire; §14.5 is where it ends. The
> outcome shipped: per-record PoW is **removed** (proofType 1 burned),
> §5.4 bonds are the only admission, and read-only consumers (§11.6)
> need no admission at all.

`bench:pulsemesh:bond`. Two candidates were measured rather than argued,
after a survey of the alternatives the literature offers. One of them
was my own proposal and the measurement refuted it.

### 14.1 What the survey ruled out before measuring

Under this protocol's constraints — no issuer, every peer verifies
strangers' records at line rate, 42-byte records, must work on a phone
on an empty road — the shape of the field is stark:

| Approach | Needs an issuer? | Verify | Size | Works when sparse? |
| --- | --- | --- | --- | --- |
| PoW (this protocol, §5.3) | no | 0.69 µs | 0 B beyond the nonce | yes |
| RLN (measured, §2b) | yes, a registry | 6.3 ms | 26× | yes |
| ARC / anonymous credentials | yes, *privately* verifiable | 740 µs | 288 B | yes |
| Blind-RSA Privacy Pass | yes | ~30 µs | 256 B | yes |
| VDF (Wesolowski/Pietrzak) | RSA setup, or slow class groups | ~ms | 100s of B | yes |
| Proof-of-Location (witness quorum) | no | cheap | signatures | **no** |
| Proof-of-Stake | a chain and capital | cheap | small | yes |
| Hardware attestation | yes, Google/Apple roots | ~ms | ~KB | yes |

Everything cheaper than PoW buys its cheapness with an issuer. ARC is
the strongest of them and still disqualified: it is *privately*
verifiable, so issuer and verifier share a key — in a mesh where every
peer verifies, every peer could mint. Proof-of-Location inverts our
availability, needing peer density precisely where a hazard report
matters most, and is privacy-hostile in a way that fights §10.2 head on.

**A kinematic admission tier was designed and abandoned before it was
built.** The idea was to price movement rather than CPU: require a
contributor's trajectory to snap to roads and be traversable in the
elapsed time, verified with the route graph we already ship. It cannot
work here. PMC1 carries no contributor identity — `reportId` is 16 fresh
random bytes per record — so successive records are unlinkable by
construction, and trajectory checking needs exactly the linkage §10.2
exists to prevent. A per-epoch pseudonym would restore the linkage and be
a trip identifier in all but name. Separately, the static half of the
idea is already rule 10–12, and the road graph is public, so map
plausibility costs a remote attacker nothing.

### 14.2 Memory-hard PoW: measured, and refuted

SHA-256 PoW parallelises perfectly, so an attacker's throughput scales
with cores while the honest phone pays retail. An asymmetric memory-hard
puzzle should cap that: Momentum-style, a B-bit birthday collision over a
table of ~2^(B/2) entries, expensive and memory-bound to solve, three
hashes to verify.

| B | Solve | Table | Verify | Solvers in 24 GiB | in 256 GiB |
| --- | --- | --- | --- | --- | --- |
| 24 | 1.0 ms | 256 KiB | 0.93 µs | 98,304 | 1,048,576 |
| 28 | 6.2 ms | 1.0 MiB | 0.88 µs | 24,576 | 262,144 |
| 32 | 28.3 ms | 4.0 MiB | 0.87 µs | 6,144 | 65,536 |
| 36 | 196 ms | 16.0 MiB | 0.91 µs | 1,536 | 16,384 |
| 40 | 505 ms | 64.0 MiB | 0.87 µs | 384 | 4,096 |

The verification asymmetry holds beautifully — ~0.9 µs at every size,
within noise of the 0.69 µs SHA-256 path. The Sybil cap does not. B=40
matches the current puzzle's cost (505 ms vs 719 ms) and still leaves a
single consumer GPU room for 384 concurrent solvers and a rented box
4,096. Memory only binds once core count exceeds RAM÷table, which is true
of a GPU's shader array and false of every CPU farm an attacker would
actually rent.

Measured thread scale-up, at costs matched so the comparison is fair:

| Workers | SHA-256 (d=18) | Momentum (B=32) |
| --- | --- | --- |
| 1 | 6.0/s (1.00×) | 33.3/s (1.00×) |
| 4 | 21.3/s (3.56×) | 135.3/s (4.06×) |
| 8 | 31.3/s (5.22×) | 248.3/s (**7.45×**) |

The memory-hard puzzle scales *better* across threads than SHA-256 — a
4 MiB table lives in cache, so it never touches the memory bottleneck it
was chosen for. **Rejected.** At table sizes a phone can afford, memory
hardness caps nothing; at sizes that would cap something, no phone can
mine at all.

### 14.3 The defect the search actually found

The real problem was never the puzzle's cost. It was that `minePow` is
synchronous and never yields:

| | Desktop (3.5 M hash/s) | Phone (~4× slower) |
| --- | --- | --- |
| d=20 contribution, mean | 719 ms | ~2.9 s |
| d=20 contribution, max seen | 1.87 s | ~7.5 s |
| d=24 incident, mean | 10.0 s | ~40 s |
| d=24 incident, max seen | 17.2 s | ~69 s |
| `powMaxIterations` = 1<<26 worst case | 19.1 s | ~76 s |

Every one of those is an unbroken block. A driver taps "hazard ahead"
and the map freezes for forty seconds. The old `powMaxIterations` cap
made this worse by being device-relative: the same 67 M iterations meant
19 s on a laptop and 76 s on a phone.

`minePowChunked` mines in adaptively-sized slices that yield to the event
loop, bounded by a wall-clock budget rather than an iteration count so
the setting means the same thing on every device:

| Difficulty | Synchronous | Chunked | Cost | Longest block |
| --- | --- | --- | --- | --- |
| 20 | 474 ms | 475 ms | +0.2% | **12.5 ms** |
| 22 | 4.81 s | 4.84 s | +0.5% | **13.6 ms** |

A 38× and 354× reduction in the longest block for under 1% of
throughput. The contributor now uses it on both paths, which makes
`reportIncident` async. Total time to publish is unchanged — a hazard
report still takes ~40 s of phone CPU at d=24, and that is the finding
that outlives this section: **`INCIDENT_POW_MULTIPLIER: 4` is 16× the
work and deserves re-examination**, because a hazard report forty seconds
late is a much weaker signal than one issued at the tap.

### 14.4 Where this leaves §5.3

Proof of work stays as the bootstrap and fallback. Nothing surveyed
removes the issuer and keeps microsecond verification, and the one axis
where PoW is genuinely weak — parallel farming — is not fixable by a
memory-hard puzzle at phone-affordable *per-record* sizes. What changed
here: mining is no longer allowed to block the caller, and the tail of
the mining distribution is now measured rather than modelled from its
mean. §14.5 then takes the layer question seriously: charging the
identity instead of the record makes the memory-hard result invert.

### 14.5 The refutation inverts: identity bonds

§14.2 rejected memory-hard PoW *at record cadence* — and the qualifier
turned out to be the finding. The table there had to be phone-affordable
per emission, which makes it cache-sized, which is why RAM bound
nothing. One mint per peer per day (§5.4 identity bonds) can afford a
table three orders of magnitude larger. Same primitive, measured at bond
scale:

| B | Solve (desktop) | Phone (~4×) | Table | Verify | Solvers in 24 GiB GPU | in 256 GiB |
| --- | --- | --- | --- | --- | --- | --- |
| 40 | 0.2 s | ~1 s | 64 MiB | 0.93 µs | 384 | 4,096 |
| **44 (default)** | **1.8 s** | **~7 s** | **256 MiB** | **0.89 µs** | **96** | **1,024** |
| 48 (cap) | 11.7 s | ~47 s | 1 GiB | 0.87 µs | 24 | 256 |

Verification stays at three hashes — ~0.9 µs at every size, and paid
once per session rather than per record. The attacker's per-GPU
parallelism at the default is 96 concurrent solvers, against ~98,000 at
the cache-sized tables per-record mining forces. (Honest cap on the
claim, §5.4: Momentum admits van Oorschot–Wiener time–memory tradeoffs,
so 96 is an upper estimate of the constraint; corroboration and the
speed anchor remain the actual defense.)

What the record path stops paying once a bond carries the session:

| | Per-record PoW (§5.3) | Bonded (§5.4) |
| --- | --- | --- |
| Per contribution | 319 ms here, ~2.9 s phone | **0** |
| Per incident report | 5.1 s here, ~40 s phone, after the tap | **0 — publishes at the tap** |
| Record on the wire | 50 B | **42 B** |
| Verify per record | 0.69 µs | none (bond once per session) |
| Cadence, 30-min commute | 38 s of core | one background mint/day |
| Ban evasion cost | free (new keypair) | **a re-mint per forfeiting receiver** |

The last row is the structural point — stated with its honest scope.
Every sanction in §6/§8 attaches to a peer; per-record PoW attached cost
to records while peers stayed free, so a ban cost its target nothing. A
bond is the first artifact a ban actually forfeits — slashing without
stake — but the trust ledger is local by design, so first-hand
forfeiture bites at one receiver and spreads only as §8.4's corroborated
PMX1 testimony, which lowers weight elsewhere and never revokes. And the
Sybil bound is **throughput, not capacity**: the GPU-solver counts above
cap *concurrency*, while a bond lives a day — one desktop core mints
~66,000 default-difficulty bonds per day (~1.3 s each), so 1,000 Sybil
identities cost ~22 core-minutes. A toll, not a wall; corroboration,
plausibility, and the speed anchor remain the defense. One further
unmeasured caveat: salt-grinding with narrowed scan windows is itself a
time-memory tradeoff avenue against the Momentum table bound, on top of
the van Oorschot–Wiener route already noted — neither has an attack
implementation here, so the RAM numbers are upper estimates twice over.
It does resolve §14.3's open question about `INCIDENT_POW_MULTIPLIER`:
under bonds the multiplier's 16× tax leaves the incident path entirely
instead of being re-tuned.

Deployment status: bonds are the protocol. Per-record PoW is removed
(proofType 1 burned, §5.3), every wire record is proofType 3, keepers
mint at startup, the simulator models the all-bonded steady state, and
the wire tests cover mint → PMA1 → verification against the connection
peerId → rule-5 rejection of unbonded deliverers. Consumers that never
publish run read-only (§11.6): no bond, no gossip membership, pull-only
convergence — covered by its own real-TCP test. Open measurement: the
mint on real phone hardware — the ~7 s phone estimate above is the ×4
scaling rule, not a measurement, and a 256 MiB transient table on a
mid-range Android WebView is exactly the kind of claim §9e exists to
test rather than assume. `bench:pulsemesh:bond` reproduces the numbers
here.
