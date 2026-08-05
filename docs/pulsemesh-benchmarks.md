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
| PMC1 encoded size | **42 bytes** (50 with a PoW nonce) |
| PMB1 batch of 16 | 688 bytes |
| encode PMC1 | 2.95 M ops/s (0.34 µs) |
| decode PMC1 | 3.34 M ops/s (0.30 µs) |
| decode PMB1(16) | 6.15 M records/s |
| SHA-256 (81-byte message) | 1.81 M hashes/s |
| PoW verify | 1.46 M ops/s (0.69 µs) |
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

## 2. Proof-of-work economics

| Difficulty | Mining cost (measured) | Verify cost |
| --- | --- | --- |
| 8 bits | 0.2 ms | 0.69 µs |
| 12 bits | 2.7 ms | 0.69 µs |
| 16 bits | 40 ms | 0.69 µs |
| 20 bits (`POW_DIFFICULTY`) | ~0.6 s projected | 0.69 µs |
| 24 bits (incidents) | ~9.5 s projected | 0.69 µs |

Mining is measured at 8/12/16 bits and projected to production
difficulties by the 2^d model, which the measured points confirm (each
4-bit step costs ~16×).

The asymmetry is the defense: **~1 M hashes to create a record, one hash
to reject it**. At 20 bits an honest contributor spends about half a
second of one core per emission — acceptable against a 15 s cadence, and
nearly free under the reticent profile's much lower emission rate. An
attacker pays the same half-second per forged record while a defender
rejects 1.46 M/s.

The 24-bit incident difficulty costs a reporter ~9 s. That is a
deliberately felt cost for a claim that nothing physical constrains, and
it is still only a speed bump — the protocol's own §8.5 distinct-peer cap
and speed anchor are the actual defense, and they are verified by the
conformance tests rather than benchmarked here: a fabricated incident on
a flowing road produces a zero penalty at any score, so there is no
throughput number to report.

## 3. Validation throughput

| Path | Throughput |
| --- | --- |
| rules 1–12, no proof (loopback) | 12.3 M records/s |
| rules 1–12 + PoW verify (wire) | **1.24 M records/s** |

One core rejects 1.24 million hostile records per second. A flood that
saturates this needs to *produce* 1.24 M records/s, each costing ~1 M
hashes — about 10^12 hashes/s, or roughly the output of a small ASIC
farm, to inconvenience one phone. This is the ratio that makes a
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
phone. Three levers exist and are all already in the protocol: subscribe
to a shard subset rather than all 16 (§11.4), raise
`ANTI_ENTROPY_SECONDS`, and let the reticent profile cut emissions (§8
below measures a 4–11× reduction).

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
[threads §10 rule 3](pulsemesh-threads.md) already prescribes.** The
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
| PMT1 record (fine mode) | **130 bytes** |
| PMTP body | 92 bytes (28-byte preimage + 64-byte signature) |
| The link — the entire capability | **45 bytes** |
| Key schedule from the capability | 0.06 ms, once per thread |
| Sign (publisher) | 0.064 ms |
| Verify (subscriber) | 0.066 ms |
| Seal / open (AES-256-GCM) | 47 k / 50 k per second |
| **Full receive path** (decode + open + parse + verify) | **0.087 ms → 11 k updates/s** |

A subscriber following one fine-mode thread at 5 s cadence spends about
0.002% of one core on crypto. The cost is not the reason to prefer coarse
mode; safety is.

**What a flood costs a phone.** §7 drops an unknown tag at step 3, before
any crypto runs:

| Measure | Result |
| --- | --- |
| Records with an unguessable tag rejected | **1.22 M/s on one core** |
| Crypto performed on them | none |

An attacker cannot make a subscriber do asymmetric work without the
capability, because they cannot compute an address it listens on. This is
the same shape as the traffic channel's proof-of-work asymmetry, reached
by a different route: there the defence is that forging is expensive,
here it is that addressing is impossible.

**Bandwidth, against the §15 claims:**

| Claim in §15 | Measured |
| --- | --- |
| 26 B/s per fine thread | **26.0 B/s** |
| 94 KB per thread-hour | **91 KB** |
| ~47 MB for a 500-bus fleet-hour | **45 MB** |
| Coarse "two orders of magnitude below" | **19.5% of fine** — one order, not two |
| Subscriber cache ≈ 16 KB/thread | 30 KiB at the default ring |

The spec's arithmetic holds everywhere except the coarse-mode claim,
which was optimistic: coarse costs about a fifth of fine, not a
hundredth. Still the right default for child transport — §11's argument
was always about what a leaked capability is worth, not bytes — but the
bandwidth line in §15 should say 5×, not 100×.

**§18's first open question, answered — and it was aimed at the wrong
variable.** The spec asks how many concurrent subscribers a run needs
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
takes it to 83–95%, eight to 100%. So §8's "availability scales with
audience size" is only true for a joiner that queries several providers,
and the actionable rule is a floor on providers asked, not a cache
incentive for small audiences.

The same run pins a limit the design never stated: **catch-up can never
recover more than `THREAD_MAX_AGE` of history** — two minutes — because
§7 step 8 rejects anything older regardless of who cached it. "Catch-up"
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
| M5 — reticent profile measurement | **done**: §8 above; the answer validates threads §10 rule 3 rather than overturning it |
| T1 — thread crypto, codecs, link | **done**: §16 vectors byte-identical through WebCrypto |
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
| `POW_DIFFICULTY` | 20 | keep — ~0.6 s to mine, 0.69 µs to verify, 10^6:1 asymmetry |
| `INCIDENT_POW_MULTIPLIER` | +4 | keep — ~9 s per claim is a felt cost, and §8.5 carries the real defense |
| `RATE_SUSTAINED` / `RATE_BURST` | 2/s / 40 | keep — bounds a valid-PoW flooder to an honest peer's ceiling |
| `STORE_CONTRIB_CAP` | 262 144 | **lower on mobile** — ~1.1 KB/record makes the default ~290 MB |
| `AGG_MIN_REPORTS` | 3 | keep for aggregates; **consider 2 for surprise-flagged records** to close the sparse-onset gap (§8) |
| reticent profile | opt-in | **keep both; choose by route publicity** (§8) — mandatory for unpublished routes, cadence for timetabled ones |
| `ANTI_ENTROPY_SECONDS` | 10 | **do not back it off** (§5) — gate the digest content on a zone fold instead |
| `THREAD_MAX_AGE` | 120 s | keep, but it silently caps catch-up at two minutes (§9c) |
| providers asked per catch-up | unspecified | **specify a floor of 3, prefer 8** (§9c) |

## 13. Defects these measurements and the conformance audit found

Building the harness and auditing the implementation against §15 turned
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
