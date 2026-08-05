# PulseMesh Threads: authenticated tracking of one moving thing

PulseMesh's traffic channel answers *"how fast is this road right now"*
from many anonymous, corroborated, unlinkable observations. This
document specifies the **second channel**, which answers the opposite
question: *"where is this specific vehicle, and when does it reach my
stop"* — for an audience that is authorized to know, and for nobody
else.

The motivating cases are school-bus tracking for parents and
courier tracking for a delivery recipient. Both are the same shape: one
publisher, a bounded audience, a run that starts and ends, and an
arrival time that only matters because live traffic is in it.

Read [pulsemesh.md](pulsemesh.md) first for the traffic channel's
rationale and [pulsemesh-protocol.md](pulsemesh-protocol.md) for the
wire conventions this document reuses verbatim (varints, framing,
epochs, segment ids, cells, the 5-minute topic window). Requirement
words are RFC 2119.

Status: **implemented** (`src/pulsemesh/thread_*.js`, exported as
`rangefind/pulsemesh/threads`). Milestones T1–T4 are complete and
tested against the §16 vectors byte-for-byte; T5 is a field pilot and
remains open by nature. The engine addition §13 asks for —
`engine.locate()` — is implemented and is an exact inverse of `snap()`.
Measured results, including answers to two of §18's open questions, are
in [pulsemesh-benchmarks.md](pulsemesh-benchmarks.md).

## 1. Why this is a second channel, not a mode

Every load-bearing property of the traffic channel is inverted here.
Trying to serve both from one record format would weaken both.

| | Traffic channel (PMC1) | Thread channel (PMT1) |
| --- | --- | --- |
| Publishers per fact | many, required | exactly one, authoritative |
| Identity | none; stable ids prohibited | a per-run keypair, mandatory |
| Trust basis | corroboration ≥ 3 reports | a signature |
| Audience | everyone in the zone | capability holders only |
| Confidentiality | none needed (it is aggregate) | end-to-end, mandatory |
| Topic | derived from geography | derived from a secret |
| Lifetime | 90 s TTL | one run, then gone |
| Failure mode | degrade to the static metric | degrade to the schedule |

The two channels share the transport, the cell vocabulary, the segment
identity, the epoch discipline, the TTL philosophy, and the codec
conventions. They share no records and no trust.

**The synergy is the point.** A thread carries position only. The
arrival time is computed on the subscriber's device by routing from
that position to their stop *under the traffic channel's live metric*
(§9). Neither channel is very interesting alone; together they are a
private replacement for the tracking servers these products use today.

## 2. What this replaces

Today a school-bus tracker or a delivery tracker works by streaming
continuous positions to a vendor database, where they persist
indefinitely, are joined to accounts, and are readable by the vendor,
its subprocessors, and anyone who compromises it. Arrival times are
computed server-side, so the server also learns every parent's home
stop and every recipient's address.

The thread channel keeps: live position, arrival estimates, arrival
alerts. It removes: the plaintext server, the retention, the account
join, and the server-side knowledge of who is watching what. The
residual costs are named honestly in §10 and §14 — this design has
them, they are just much smaller.

## 3. The two shapes

### 3.1 School run (one → many, scheduled, repeating)

A run is one vehicle covering an ordered stop list on one date. The
operator publishes the **run plan** as an ordinary static asset next to
the map pack — stops, planned times, and the polyline — so a subscriber
can predict arrival with no live data at all. The thread then corrects
the prediction.

Audience is 30–500 families. Subscribers never see each other and never
see which stop another subscriber cares about; every arrival estimate
and every alert is computed locally from the same broadcast.

Default granularity is **coarse** (§11): stop events, not continuous
position. A parent asking "is the bus late" is answered exactly as well
by "departed stop 7 at 07:42, 4 stops to go" as by a dot on a map, and
a coarse thread that leaks reveals a schedule rather than a live child
locator.

### 3.2 Delivery (one → one or two, ad hoc, self-terminating)

The audience is the recipient, optionally the merchant. The capability
is delivered in the order confirmation and expires at `notAfter`. There
is nothing to revoke because there is no next run: the thread is dead
minutes after the doorbell.

Granularity is **fine** — a courier's position is the product — and
the recipient's ETA is the same local computation, which matters more
here because a courier is in the traffic that the other channel is
already measuring.

### 3.3 The general shape

Anything that is *one identified moving object, a bounded audience, and
a run with an end*: paratransit and medical transport, field service
windows, shared "I'm on my way" ETAs between two people, snow-clearing
and waste collection (public audience, empty capability), fleet
dispatch consoles. The protocol does not distinguish them; the
granularity mode and the audience size do.

## 4. Capability model

### 4.1 One key does everything

Per run, the publisher generates **one Ed25519 keypair** and nothing
else. The private key stays on the publishing device forever. The
32-byte public key `P` is the entire capability that travels in the
link: it is what lets a holder *find*, *decrypt*, and *authenticate* the
thread, and it is useless for publishing.

Everything derives from `P` by HKDF-SHA256 (empty salt, label as
`info`):

| Key | `info` label | Length | Purpose |
| --- | --- | --- | --- |
| `K_topic` | `pulsemesh/thread/topic/1` | 32 | topic tag derivation (§4.2) |
| `K_content` | `pulsemesh/thread/content/1` | 32 | AES-256-GCM content key |
| `noncePrefix` | `pulsemesh/thread/nonce/1` | 4 | nonce prefix (§5.1) |

So the split is:

- **Private key → write.** Only the publisher can sign, so only the
  publisher can move the bus. Impersonation is impossible for anyone
  holding the link, which matters because every subscriber holds it.
- **Public key → read + find + verify.** Confidentiality comes from
  AES-GCM under a key derived from `P`; authenticity comes from the
  signature verified with `P`.

The confidentiality here rests on `P` being **distributed, not
published** — it is a capability, not an identity. Therefore, normatively:

- The keypair MUST be generated fresh per run, from a CSPRNG.
- `P` MUST NOT be published, registered in any directory, reused across
  runs, or used as a libp2p peer identity. A publisher's mesh peer id is
  a *different*, unrelated key.
- `P` MUST NOT appear on the wire. Nothing transmitted reveals it: the
  topic tag is an HMAC under `HKDF(P)`, the payload is sealed, and the
  signature — which is verified with `P` but does not disclose it — is
  inside the sealed body (§5.2), not the envelope.

This is a deliberate departure from how public keys are normally
treated, and it is the whole reason one key in a QR code is enough. An
implementation that logs `P`, puts it in a URL query string, or reuses
it across runs has broken the confidentiality of every thread it ever
signed.

### 4.2 Finding the thread with the key

```
tag(window)  = HMAC-SHA256(K_topic,
                 "pulsemesh/thread/1" ‖ epoch32 ‖ uint64be(window))[0..8]
topic        = /rangefind/pulsemesh/1/t/<epochPrefix16hex>/<tagHex16>
rendezvous   = SHA-256(utf8(topic))
```

`window = floor(unixSeconds / 300)`, the same rotation as the traffic
channel, with the same `TOPIC_OVERLAP` dual-subscribe behaviour.

`rendezvous` is the DHT/rendezvous key that peers holding this thread
advertise as providers for. **This is what makes the link
self-sufficient**: a holder derives the tag, derives the rendezvous key,
asks the DHT who is providing it, connects to them, and subscribes.
No directory, no name server, no address in the link.

Four consequences worth stating:

1. The topic name is a pseudorandom 8 bytes — no zone, route number,
   operator, or anything else. Threads cannot be enumerated or browsed.
2. Finding a thread *requires* the capability. There is no "listen to
   all buses" position, even for a relay: without `P` you cannot compute
   the address, let alone open the payload.
3. Tags rotate every 5 minutes and are unlinkable across rotations
   without `K_topic`, so an observer cannot follow a thread through the
   day by its address — and neither can the DHT.
4. The DHT learns only that some peers provide an opaque, rotating key.

The cost is that thread topics carry no geography, so they cannot ride
the traffic channel's zone-scoped gossip mesh. §8 covers delivery.

### 4.3 Distribution and revocation

The capability travels as a **link** (§5.4) over a channel the operator
already has: an SMS, a QR code on a permission slip, the order
confirmation email, the school portal. The link is a bearer token —
anyone holding it can follow the thread until `notAfter`, exactly like
today's delivery-tracking URLs, but read-only, run-scoped,
time-bounded, and unable to reach a server that knows anything.

Revocation is **non-renewal**, which is why the keypair is per-run:

- Delivery: no revocation problem exists — the thread dies at the door.
- School: the operator issues each run's link to currently-entitled
  families, a day or two ahead. Removing a family means not sending
  tomorrow's link. A leaked link exposes one run, not a school year.

Group rekeying is thereby avoided entirely, and no revocation list
exists anywhere. Recurring subscriptions need a per-user authenticated
issuer, but that is the operator's existing portal or SMS pipeline, not
new rangefind infrastructure, and it learns only *who is entitled* —
never who watched, or where anyone lives.

Because the link is a bearer capability, treat it like one: SMS and
email are not confidential channels, and a forwarded link is a granted
capability. The mitigations are the ones already in the design — short
`notAfter`, per-run keys, and coarse mode as the default for child
transport (§11), so that the worst case of a forwarded link is a
schedule rather than a live locator.

## 5. Wire formats

Framing, varints, magic discipline, and the trailing-bytes rule are
`pulsemesh-protocol.md` §1 unchanged.

### 5.1 PMT1 — sealed thread update

The only thread record that touches the network.

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMT1"` | |
| 2 | epochPrefix8 | bytes(8) | |
| 3 | tag | bytes(8) | §4.2, current window |
| 4 | seq | varint | strictly increasing per thread |
| 5 | ctLen | varint | |
| 6 | ciphertext | bytes(ctLen) | AES-256-GCM, tag appended |

AEAD parameters: key `K_content`; nonce =
`noncePrefix(4) ‖ uint64be(seq)` (12 bytes — unique by construction, no
nonce is ever transmitted); AAD = fields 1–4 exactly as encoded, which
binds the ciphertext to its epoch, tag and sequence number.

AES-256-GCM rather than XChaCha20-Poly1305 because it is in WebCrypto
on every target host, and this project does not take crypto
dependencies. Ed25519 verification is in WebCrypto on current Node and
browsers; hosts without it (older Hermes) inject an implementation
through the same hook pattern the engine already uses for `inflate`.

### 5.2 PMTP — plaintext body

Never transmitted in the clear.

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMTP"` | |
| 2 | unixSeconds | varint | whole seconds; the audience is entitled to precision |
| 3 | state | u8 | §5.3 |
| 4 | mode | u8 | 1 coarse, 2 fine |
| 5 | leafCell | varint | 0 when position is withheld |
| 6 | geomRef | varint | `polyline*2 + direction` |
| 7 | ratioQ12 | varint | 0..4095, position along the segment |
| 8 | speedBin | u8 | 0..44, as traffic channel §2.5 |
| 9 | stopIndex | varint | 1-based index into the run plan, 0 = n/a |
| 10 | planRef | bytes(8) | SHA-256 prefix of the run plan, zeros = none |
| 11 | noteLen ‖ note | varint ‖ bytes | operator message, ≤ 64 bytes UTF-8 |
| 12 | signature | bytes(64) | Ed25519 over fields 1–11 |

Position is `(segment, ratio)`, not coordinates. The publisher gets
both directly from `snap()` — `SnapMatch.segment` and `SnapMatch.ratio`
— so the publish side needs no new engine surface. The payload is
snapped to the road (no GPS jitter, no driveway-level precision),
smaller than a coordinate pair, and meaningless without the map pack
for that epoch. It also composes: it is already the identity the
traffic channel and the router speak.

Prohibited by construction, as on the traffic channel: raw GPS,
accelerometer or device telemetry, passenger data, driver identity,
account ids. The codec has no fields for them.

### 5.3 States

| Value | Meaning |
| --- | --- |
| 1 | scheduled, not started |
| 2 | en route |
| 3 | dwelling at `stopIndex` |
| 4 | completed |
| 5 | canceled |
| 6 | off-plan (diverted, vehicle swapped — see `note`) |

There is deliberately no "approaching your stop" state and no "delayed"
field. Both are subscriber-side computations (§9): the publisher does
not know which stop a subscriber cares about, and must not learn it.

### 5.4 The link (out-of-band, never on the mesh)

The capability is 45 bytes, so it fits an SMS, a QR code, or a tap
target without a shortener:

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | version | u8 | 1 |
| 2 | publicKey | bytes(32) | `P` — the whole capability |
| 3 | epochPrefix8 | bytes(8) | binds the link to a graph epoch |
| 4 | notAfter | uint32be | absolute expiry, unix seconds |

base64url-encoded into a **URL fragment**:

```
https://track.example/r#Ac3wz7QizS-q8k64NDz_ZBpBykvko6pCnnIp1pBxl2G99EeWyMwfP6dokGiA
```

The fragment is deliberate and load-bearing: browsers never transmit it,
so the page host — which may be the operator, a CDN, or an app-store
landing page — never receives the capability. It serves inert static
code that then derives keys locally. There is no endpoint anywhere that
sees a tracking key.

The link carries no bootstrap address, mailbox host, or plan URL. Peers
are found through the mesh's normal bootstrap plus the §4.2 rendezvous
key; the run plan is fetched from the static index by `planRef` (§5.2)
once the first update is decrypted. **A link is a key, not a location.**

A subscriber MUST reject updates timestamped after `notAfter`, MUST
discard the key material at `notAfter`, and MUST reject a link whose
`epochPrefix8` matches no epoch it can load.

Where the distribution channel is especially untrusted, `P` MAY instead
be wrapped to a subscriber-held X25519 public key; nothing else in the
protocol changes.

### 5.5 Catch-up messages

There is no mailbox host and no designated server. Every subscriber
already caches the thread's recent records to render it, so **any
subscriber can answer a late joiner** over protocol id
`/rangefind/pulsemesh/1/thread`, magic-discriminated:

- **PMR1** (catch-up request): `"PMR1"` ‖ epochPrefix8 ‖ tagCount varint
  (MUST be exactly 4, 8, or 16) ‖ per tag: bytes(8) ‖ sinceSeq varint.
- **PMM1** (response): `"PMM1"` ‖ epochPrefix8 ‖ tagCount ‖ per tag:
  bytes(8) ‖ recordCount varint ‖ that many verbatim PMT1 records.
  Tags echo the request order. Unknown tags return count 0; a responder
  MUST NOT distinguish "tag I do not hold" from "tag with no new data".

A responder relays sealed bytes it may not even be able to open (a
subscriber caches records for tags it holds; a plain relay peer caches
opportunistically). Records are verbatim, so the signature and AEAD tag
travel intact and the late joiner validates them itself — a relaying
peer cannot alter, forge, or drop-and-substitute anything, and does not
need to be trusted.

The padded-request discipline is inherited from the traffic channel's
cell fetches, and here it is *free*: tags are indistinguishable from
uniform random bytes, so decoy tags cost one CSPRNG call and are
perfectly indistinguishable from real ones. Even a peer that holds the
same thread cannot tell which of 8 tags a given requester came for.

## 6. Constants

| Name | Value | Tunable | Meaning |
| --- | --- | --- | --- |
| `THREAD_UPDATE_FINE` | 5 s | yes | publish cadence, fine mode |
| `THREAD_UPDATE_COARSE` | on stop events + 60 s heartbeat | yes | coarse mode |
| `THREAD_MAX_RECORD_BYTES` | 256 | no | max encoded PMT1 |
| `THREAD_MAX_AGE` | 120 s | yes | oldest update a subscriber accepts |
| `THREAD_MAX_FUTURE_SKEW` | 15 s | yes | |
| `THREAD_STALE` | 90 s | yes | UI must stop claiming "live" past this |
| `THREAD_CACHE_TTL` | 600 s | yes | catch-up cache retention per tag |
| `THREAD_CACHE_RING` | 240 | yes | records cached per tag |
| `THREAD_CACHE_TAGS` | 256 | yes | tags a relay peer caches (LRU) |
| `THREAD_PROVIDE_INTERVAL` | 120 s | yes | DHT provider re-advertise |
| `THREAD_POLL_INTERVAL` | 10 s ± 3 jitter | yes | catch-up poll when gossip is cold |
| `THREAD_MAX_RUN_SECONDS` | 21600 (6 h) | no | absolute thread lifetime |
| `THREAD_TAG_BUDGET` | 32 / peer / window | yes | new cached tags per source peer |
| `THREAD_CACHE_RATE` | 1 rec / 3 s / tag | yes | relay cache admission rate |

`TOPIC_WINDOW` (300 s), `TOPIC_OVERLAP` (30 s) and `EPOCH_OVERLAP`
(600 s) are shared with the traffic channel unchanged.

## 7. Validation

A subscriber applies these in order, dropping on first failure.

1. Frame, magic, no trailing bytes, size ≤ `THREAD_MAX_RECORD_BYTES`.
2. `epochPrefix8` matches the current epoch, or the previous one during
   the consumer overlap window.
3. `tag` equals a tag the subscriber derived for the current or an
   adjacent window. (Unknown tags are dropped silently and cheaply —
   this is the entire cost of a hostile flood on the gossip path.)
4. AEAD opens with `K_content` under nonce `noncePrefix ‖ seq`.
5. Inner magic `"PMTP"`, well-formed, no trailing bytes.
6. Ed25519 signature verifies against `P` from the link. **A record
   that decrypts but does not verify is an attack, not an error** —
   it means a link holder tried to publish; log it and drop the
   delivering peer's standing.
7. `seq` strictly greater than the highest accepted `seq` for this
   thread — replay and rollback protection.
8. `unixSeconds` within `[now − THREAD_MAX_AGE, now + THREAD_MAX_FUTURE_SKEW]`
   and not after the link's `notAfter`.
9. Plausibility: implied speed between consecutive accepted updates ≤
   the class cap × 1.15 from traffic-channel §6 rule 10; `geomRef >>> 1`
   below the leaf's polyline count.

A **caching relay** (§8) validates only steps 1–3 — it holds no key and
therefore cannot check 4–9. It does not need to: it forwards verbatim
sealed records, and the late joiner runs the full pipeline itself. A
relay that tampers produces an AEAD failure; a relay that forges
produces a signature failure; a relay that replays produces a `seq`
failure. **Nothing in this channel requires trusting a relay**, which
is why the channel does not name one.

A relay caching for tags it cannot open is nevertheless a flood target,
since it cannot tell real records from garbage addressed to invented
tags. It is bounded by `THREAD_CACHE_RATE`, `THREAD_TAG_BUDGET`, and an
LRU over `THREAD_CACHE_TAGS`; caching is best-effort by definition, so
shedding under load is correct behaviour, not failure.

## 8. Delivery: no mailbox, no host

The link is a key, so discovery is a computation rather than an address
lookup. All paths carry identical bytes; a thread neither knows nor
cares which one reached a subscriber.

1. **Rendezvous + gossip (primary).** The subscriber derives
   `tag(window)` and `rendezvous` from `P` (§4.2), asks the DHT which
   peers provide that key, connects, and subscribes to the topic. The
   publisher advertises itself as a provider for the same key each
   window. This is the whole discovery mechanism: no bootstrap address
   in the link, no directory, no host.
2. **Audience caching (catch-up).** Every subscriber retains the
   thread's last `THREAD_CACHE_RING` records and answers PMR1 for tags
   it holds. A parent whose phone was asleep for four minutes rejoins,
   finds current providers through the same rendezvous key, and pulls
   the gap from *another parent* — or from any relay peer that cached
   the bytes opportunistically. Availability therefore scales **with
   audience size**, which is exactly backwards from a server, where a
   larger audience costs more.
3. **Direct.** Dispatch consoles and other always-on subscribers hold a
   stream to the publisher.

This removes the second non-static service from the design. What
remains is the traffic channel's optional keeper (pulsemesh.md §12) —
already an ordinary, blind, TTL-bounded mesh peer — which can cache
thread records too, without keys, without being named in any link, and
without becoming anything a thread depends on.

**Where this is weaker than a mailbox, stated plainly.** GossipSub is
not store-and-forward, so a record only survives if *someone* was
subscribed when it was published. The two shapes land differently:

- **School run** — 30–500 subscribers, many of them awake and in the
  same city. Someone is always holding the thread, and the catch-up
  path is well supplied. This is the good case, and it gets better the
  more families subscribe.
- **Delivery** — an audience of one. If the recipient's app is closed,
  nobody caches, and the position for those minutes is simply gone.
  That is acceptable *because* the recipient is actively watching a
  courier who is minutes away — the live connection is the product —
  but an implementation MUST NOT promise history it cannot hold, and
  the recipient's own device is the only reliable cache.

The design consequence: a run's early minutes may have no live position
for a subscriber who joined late and found no cache. §12's degradation
rows are not a corner case here, they are a normal Tuesday, and the UI
must handle them as such.

### 8.1 The wake-up problem

"Tell me when the bus is five minutes away" is the actual feature, and
a sleeping phone cannot compute it. Three options, weakest leak first:

- **Plan-scheduled local notification.** At subscribe time the device
  knows the run plan and its own stop, so it schedules a local alert
  for the planned time minus lead, and wakes shortly before to refresh
  against live data. Zero leak; correct for on-time runs; late runs are
  corrected only at the wake.
- **Wake on state change.** The device also wakes on the publisher's
  coarse stop events during the run window. Fewer wakes than polling,
  still no server-side knowledge.
- **Content-free push.** With no mailbox there is no natural place for
  this to live: it needs a service holding a push token, and pairing a
  token with a tag re-creates exactly the linkage the rest of the
  design removes. If an operator adds one, it MUST be opt-in, MUST be
  disclosed, and MUST carry no payload — and the honest framing is that
  it is a separate product decision layered on top of this protocol,
  not part of it.

## 9. Arrival time is a local route query

This is the integration, and it is why threads belong in rangefind
rather than in a tracking product.

```js
const u = thread.latest();                        // decrypted PMTP
const at = await engine.locate(u.segment, u.ratio / 4096);    // §13
const legs = plan.stopsBetween(u.stopIndex, myStop);          // fixed order
const { seconds } = await engine.matrix({
  points: [at, ...legs],
  live: pulseMeshTrafficProvider                  // the other channel
});
let eta = 0;                                      // sum consecutive pairs
for (let i = 0; i + 1 < seconds.length; i++) {
  eta += seconds[i][i + 1];
  if (i + 2 < seconds.length) eta += plan.dwellSeconds;   // not at my own stop
}
const arrivalMs = u.unixSeconds * 1000 + eta * 1000;
```

**Use `matrix()`, not `itinerary()`.** `itinerary()` *reorders* stops
(exact Held-Karp up to 12, 2-opt beyond) — it solves a travelling
salesman problem, which is exactly wrong for a bus run whose sequence
is fixed by the plan. `matrix()` gives all-pairs times from one
shared-context search; summing the consecutive pairs evaluates the
planned order and nothing else. An implementation that calls
`itinerary()` here will silently produce optimistic ETAs by shortcutting
the route the bus is actually going to drive.

Everything of value happens on the subscriber's device:

- **The publisher never learns which stop matters to anyone.** It
  broadcasts position; each device computes its own answer. Compare
  this with the server-side ETA it replaces, which necessarily knows
  every recipient's address.
- **Traffic is already there.** The subscriber is an ordinary consumer
  of the anonymous traffic channel over the corridor it just
  rasterized. A courier stuck on a jammed arterial produces a correct
  ETA because three other drivers on that arterial reported 12 km/h and
  corroborated each other — and neither the courier nor those drivers
  learned anything about each other.
- **Same numbers everywhere.** The operator's dispatch console runs the
  same engine over the same static index and gets the same estimate, so
  the parent and the dispatcher never argue about whose app is right.
- **The routing is already shipped.** Multi-point, live-aware,
  shared-context evaluation is existing engine behaviour; threads add no
  routing code at all — only the arithmetic above.

Confidence and staleness are UI obligations: past `THREAD_STALE` the
interface MUST stop presenting a position as live and fall back to §12.

## 10. Rules between the two channels

Thread publishers **should** contribute traffic — a fleet on the road
all day is the best sustained probe the traffic channel could ask for.
The rules below make that safe rather than forbidding it.

1. **A thread record never enters a traffic aggregate.** This one is
   absolute. Thread updates are authenticated single-source; traffic
   aggregates are corroborated multi-source with `AGG_MIN_REPORTS`.
   Admitting a signed fleet record into the aggregate would turn one
   fleet key into a traffic authority, the exact property the traffic
   channel is built not to have. A fleet contributes by emitting
   **ordinary PMC1 records** — same PoW, same corroboration minimums,
   same anonymity as everyone else. Two records, two channels, never a
   bridge between them.

2. **A thread consumes traffic aggregates freely.** §9. The subscriber
   is a normal traffic consumer, with the normal padding, decoy and
   split rules on every cell fetch.

3. **Contribution is gated on whether the route is already public.**

   The risk is correlation: an observer who sees a PMT1 and a PMC1
   leave one peer in the same window has de-anonymized the
   contribution, and can then follow the *encrypted* thread by watching
   its plaintext twin. Timing offsets do not fix this — the leak is the
   trajectory, not the clock. But what a trajectory costs depends
   entirely on whether it was ever secret:

   - **Scheduled vehicles on published routes SHOULD contribute.** A
     school bus's route and timetable are a public static asset — the
     run plan is literally published next to the map pack so
     subscribers can predict arrivals. Correlating a bus's
     contributions to its thread reveals a route anyone could already
     read off the timetable. There is no anonymity to lose, so the
     conservative rule protects nothing and costs the network its best
     contributor. Applies to transit, school runs, snow clearing, waste
     collection, scheduled shuttles.
   - **Couriers and unscheduled vehicles MUST contribute under the
     reticent profile** (protocol §10.2) or not at all. A courier's
     route is *not* public, and it is not merely private — it is a list
     of customer addresses, and reconstructing it from contributions is
     the worst outcome in this document.

     The reticent profile exists for this case: suppress near the
     device's own stops and on residential streets, emit only
     observations that *surprise* the current expectation, only when
     other contributors are already present, and publish through
     rotating forwarders. It removes the trajectory rather than the
     contributor, and because surprises are shared events, what a
     courier does emit is emitted simultaneously by everyone else stuck
     in the same jam. The residual exposure is stated honestly in
     protocol §10.3, and M5 measures it as a
     recovered-route fraction rather than asserting it.

   The distinguishing test is not vehicle type but a question:
   **would an observer learn anything from the correlation that a
   published document does not already tell them?** Where the answer is
   yes, the reticent profile is mandatory rather than the contribution
   being refused. Implementations MUST make contribution an explicit
   per-thread setting, MUST default it to off, and MUST NOT infer the
   profile from vehicle class.

4. **A contributing thread publisher MUST suppress contributions at its
   own stops.** This is a data-quality rule, not a privacy one, and it
   is easy to miss: a bus dwelling at every stop reports 0 km/h on
   roads that are flowing perfectly. Feeding that into a weighted
   median would make the traffic layer systematically pessimistic about
   exactly the arterials transit runs on — and worse, self-consistently
   so, since several buses on one corridor corroborate each other's
   false standstill.

   Therefore a publisher with a run plan MUST NOT emit PMC1 while
   `state` is dwelling, within 40 m of a planned stop, or for 10 s
   after departing one. The same applies to a courier stopped to hand
   over a package on a free-flowing street. The traffic channel's
   standstill rule (protocol §10.4) catches parking, not
   service stops — this rule is what catches service stops.

   A vehicle that stops for *traffic* between stops reports normally;
   that is real congestion and precisely what the channel wants.

## 11. Granularity is a safety control

`mode` is not a bandwidth setting. It decides what a leaked capability
is worth.

**Coarse** publishes stop events plus a heartbeat: `state`,
`stopIndex`, `unixSeconds`, no `leafCell`. A leaked coarse thread
reveals that a vehicle passed a published stop at a time — roughly what
a printed timetable and a bystander reveal. Arrival estimation still
works: the subscriber routes from the last stop's known position over
the remaining plan under live traffic, losing accuracy roughly in
proportion to inter-stop distance.

**Fine** publishes continuous position and is a live locator for
whoever holds the key.

The recommended defaults follow the harm, not the convenience: **coarse
for anything carrying children**, fine only as a deliberate operator
choice with the trade-off stated to families; fine for couriers, whose
position is the product and who are consenting adults on the clock.
Operators SHOULD publish coarse and offer fine per-family rather than
per-fleet.

A publisher MAY also withhold position (`leafCell = 0`) near
first and last stops — the segment of a run where a leak is most
directly a home address.

## 12. Degradation contract

Mirroring the router's degrade-to-static rule, which is the property
the whole project is organized around:

| Available | Behaviour |
| --- | --- |
| Thread + traffic | live position, live-traffic ETA |
| Thread only | live position, static-metric ETA |
| Traffic only | plan-based prediction, live traffic on the remaining legs, position marked *unknown* |
| Neither | the published run plan, marked *scheduled* |

An implementation MUST NOT present the lower rows as the upper ones.
"Bus expected 07:44" and "bus is here, arriving 07:44" are different
claims and the second one is the reason anyone installed the app.

## 13. Required engine addition

One, small — **now implemented** in `src/route_graph_query.js` and
exposed on every engine handle:

```
engine.locate(segment: string, ratio: number): Promise<{ lat, lon }>
```

The inverse of `snap()` — decode the leaf's canonical polyline for
`segment` and interpolate to `ratio`. The engine already decodes these
polylines during geometry unpacking, so this is a wrapper around
existing machinery, and it is independently useful (replaying a matched
trace, rendering a snapped marker, testing `snap()` round-trips).
Everything else threads need — `snap()` for publishing, `matrix({ live })`
and `route({ live })` for arrival, segment ids, the epoch — is shipped.

## 14. Legal and labour posture

Not legal advice, but the shape of the problem is clear enough to
design against, and it is the school case that carries it.

Tracking a school bus tracks a **driver**: an identifiable worker,
continuously, during employment. In Quebec, Law 25 requires informing
an employee of technology used to identify, locate or profile them; the
GDPR reaches the same place through purpose limitation and
proportionality. The design helps in the ways that matter — retention
is a TTL rather than a database, there is no historical trajectory
store anywhere by construction, and no vendor holds plaintext — but it
does not make the driver's position stop being personal data.

Practical obligations, which belong in the operator's deployment guide
rather than in the protocol:

- **Purpose limitation.** A thread exists to deliver children safely
  and to answer "where is the bus". It MUST NOT be repurposed into
  driver performance monitoring; that is a different lawful basis, a
  different notice, and usually a different bargaining conversation.
- **Driver-visible control.** The publishing device shows when a thread
  is live and can end it. A driver on a break is not a tracked object.
- **Where retention actually bites.** The mesh forgets. The operator's
  own dispatch console is a normal system of record and is where
  retention policy, access control and audit have to be real.
- **Children's data.** `stopIndex` plus a family's identity is, in
  effect, a child's location. §11's coarse default and the fact that no
  server ever learns which stop a subscriber watches are the two
  controls that matter most here.

The honest summary for product copy: this design removes the vendor
database and the server-side knowledge of who watches whom. It does not
remove the operator's own knowledge of where its own vehicles are, and
it should not pretend to.

## 15. Bandwidth

A fine-mode PMT1 is 130 bytes (§16). At 5 s cadence that is 26 B/s per
thread, 94 KB per thread-hour. A 500-bus fleet in a morning window
generates about 47 MB in total across the mesh. A subscriber's own
catch-up cache is 120 × 130 B ≈ 16 KB per thread — small enough that
audience caching costs a phone nothing, which is what makes §8's
host-free design practical. A relay peer caching the maximum
`THREAD_CACHE_TAGS` holds about 4 MB (measured: 7.6 MB at the default
ring). Coarse mode is **about 5× below** all of these — this document
originally claimed two orders of magnitude, and the measurement
([benchmarks §9c](pulsemesh-benchmarks.md)) puts it at 19.5% of fine
rather than 1%. Coarse remains the right default for child transport,
but on §11's safety argument, not on bandwidth.

The subscriber side is dominated not by the thread but by the traffic
cells its ETA query fetches — which is the existing corridor-fetch
budget, and the reason a thread subscriber should reuse one corridor
cache across refreshes rather than re-rasterizing per update.

## 16. Test vectors

Reproducible with Node's `crypto` and the definitions above. Epoch is
the traffic channel's test epoch,
`f44796c8cc1f3fa797104e925812ff052717f3052b5dbcadb0a36db776e0a4d1`
(= `SHA256("pulsemesh-test-vector")`).

### 16.1 Key schedule

Ed25519 keypair from seed `SHA256("pulsemesh-thread-publisher")` =
`0a0d7e721ae1571ceb2c444c3c661176175d0392d27040031a9f175f98de8ded`,
giving the capability `P` and everything derived from it:

```
P            = cdf0cfb422cd2faaf24eb8343cff641a41ca4be4a3aa429e7229d690719761bd
K_topic      = HKDF(P, "pulsemesh/thread/topic/1",   32)
             = 0a3e8b5df4d51e0ce7306d79a309b78c35c21f460ca07cdd5f6cfd7f30453c4b
K_content    = HKDF(P, "pulsemesh/thread/content/1", 32)
             = 39413c192f15a7068ac77abdf8a96eb9a769db3d226ed9731c7a201e88b8b3d7
noncePrefix4 = HKDF(P, "pulsemesh/thread/nonce/1",    4) = 576cd8f8
```

### 16.2 Topic tag and rendezvous

At unixSeconds 1754265600, window = 5847552:

```
tag        = cbfa3ae2fdc2cf0f
topic      = /rangefind/pulsemesh/1/t/f44796c8cc1f3fa7/cbfa3ae2fdc2cf0f
rendezvous = SHA-256(utf8(topic))
           = a6213aeb6a36ac6d3380b05b6181124601c2234197377c6a6a086f68f8620849
```

### 16.3 The link

Version 1, `P`, epochPrefix8, `notAfter` 1754294400 — 45 bytes:

```
Ac3wz7QizS-q8k64NDz_ZBpBykvko6pCnnIp1pBxl2G99EeWyMwfP6dokGiA
```

as delivered (60 characters after the `#`, SMS- and QR-sized):

```
https://track.example/r#Ac3wz7QizS-q8k64NDz_ZBpBykvko6pCnnIp1pBxl2G99EeWyMwfP6dokGiA
```

### 16.4 Sealed update

Body fields: unixSeconds 1754265600, state 2 (en route), mode 2 (fine),
leafCell 3181, geomRef 885 (polyline 442, direction 1), ratioQ12 2048,
speedBin 7, stopIndex 8, planRef `10c2d3bff3368b2e`, no note. Signed
preimage (28 bytes):

```
504d545080f0bfc4060202ed18f5068010070810c2d3bff3368b2e00
```

Ed25519 signature (64 bytes), giving a 92-byte PMTP body:

```
b0b3192ceefeb4be4a2addebc3a74409324ee4613b1715cb974a5841edc982e4
8500128fde33572ebaef2b639d34d283b1b68dcedfa7a092b86936c1bf783c07
```

Sealed at `seq` 42 — nonce `576cd8f8000000000000002a`, AAD
`504d5431f44796c8cc1f3fa7cbfa3ae2fdc2cf0f2a` — the full 130-byte PMT1
record is:

```
504d5431f44796c8cc1f3fa7cbfa3ae2fdc2cf0f2a6c7a8da4d85d9063cc5eb7
17fe848766a01c64b39e7051e5a88fc7d84c799bbaab39ea92bce979fdbb5412
f46e0a53c20d5243e25caceba50baea321a0a61066048684867b197163aa0633
0039f2730fcd49b1f3535d5d4ce4467d9de6e36c35429b0f6cc17c58ab3098ae
12d4
```

## 17. Repository layout and milestones

```
src/pulsemesh/thread_crypto.js   HKDF-from-P, tag/rendezvous, seal/open, Ed25519     (§4, §5.1)
src/pulsemesh/thread_codec.js    PMT1/PMTP/PMR1/PMM1 + link encode/decode            (§5)
src/pulsemesh/thread_publish.js  publisher state machine, snap-driven, modes         (§5.3, §11)
src/pulsemesh/thread_consume.js  validation, seq ledger, staleness                   (§7)
src/pulsemesh/thread_cache.js    catch-up ring buffers, padded PMR1, admission caps  (§5.5, §8)
src/pulsemesh/thread_eta.js      locate + fixed-order matrix arrival estimation      (§9)
src/pulsemesh/thread_contribute.js  the rules between the two channels               (§10)
src/pulsemesh/threads.js         public entry point (rangefind/pulsemesh/threads)
scripts/pulsemesh_thread_bench.mjs  crypto, bandwidth, catch-up availability        (§15, §18)
test/pulsemesh_thread_*.test.js  per-module + §16 vectors
```

- **T1 — crypto, codecs and the link (no network).** Reproduce §16 byte
  for byte; property test that a tampered AAD, a rolled-back `seq`, and
  a correctly-sealed-but-unsigned record (the link-holder-turned-forger
  case) are all rejected. Requires `engine.locate()` (§13).
  **Done** — `thread_crypto.js`, `thread_codec.js`,
  `test/pulsemesh_thread_codec.test.js`. Every §16 vector reproduces
  byte-for-byte, including the 130-byte sealed record, through WebCrypto
  so the same code runs in Node and browsers.
- **T2 — loopback thread.** Publisher and subscriber in one process
  over an in-memory duplex, publisher fed by the demo's simulated
  drive; subscriber renders a position and an ETA that moves when a
  synthesized jam is injected into the traffic provider. This proves
  the §9 claim, which is the whole thesis, with no networking at all.
  **Done** — `test/pulsemesh_thread_loopback.test.js`, on the real OSM
  route graph. The ETA moves under a jam in both shapes: the multi-stop
  school run and the two-point delivery.
- **T3 — discovery and catch-up.** Rendezvous derivation, DHT provider
  records, audience caching; sleep/wake test where a subscriber offline
  for 4 minutes rejoins from *another subscriber's* cache with no
  designated host anywhere in the test.
  **Done** — `thread_cache.js`, `test/pulsemesh_thread_catchup.test.js`.
  Rendezvous and topic derivation are covered; live DHT provider records
  are transport wiring on the traffic channel's libp2p adapter.
- **T4 — contribution.** A publisher emitting both PMT1 and PMC1 under
  §10 rule 3, with rule 4 stop suppression; assert the traffic
  aggregate for a corridor served by buses is not biased downward by
  dwell time — the failure this rule exists to prevent.
  **Done** — `thread_contribute.js`,
  `test/pulsemesh_thread_channels.test.js`. The dwell-bias test makes
  the failure concrete: three buses reporting their stops drag a
  50 km/h corridor down, and they corroborate each other while doing
  it, so corroboration alone would not have caught it.
- **T5 — coarse pilot.** One operator, one route, coarse mode, run
  plans as static assets, links by SMS. Measure: ETA error against
  observed arrival, wake budget and battery, catch-up hit rate as a
  function of how many families are subscribed, and whether families
  ever ask for fine mode once coarse works.

## 18. Open questions

- ~~Catch-up availability as a function of audience size.~~
  **Answered, and the question was aimed at the wrong variable**
  ([benchmarks §14](pulsemesh-benchmarks.md)). Audience size barely
  matters; **how many providers a late joiner asks** is what decides it.
  Asking one peer succeeds 40–53% of the time whether the run has 2
  subscribers or 100 — the odds that the one peer you picked was awake
  do not improve with audience. Asking three raises it to 83–95%, and
  asking eight to 100%. So §8's "availability scales with audience size"
  is true only for a joiner that queries several providers, and the
  actionable rule is a floor on providers asked (three minimum, eight
  for certainty) rather than a cache incentive for small audiences.

  The same measurement pins a limit the design never stated: **catch-up
  can never recover more than `THREAD_MAX_AGE` of history**, because
  §7 step 8 rejects anything older no matter who cached it. "Catch-up"
  means the last two minutes, not the run so far, and a UI must not
  promise otherwise.
- **A real position on leaf 0 collides with the withheld sentinel.**
  §5.2 words the sentinel as `leafCell = 0`, but leaf 0 is an ordinary
  leaf — a vehicle really can be on segment `0/95/1`, and reading the
  sentinel off `leafCell` alone silently discards the position of
  everything inside one leaf of the map. The implementation treats the
  whole triple (`leafCell`, `geomRef`, `ratioQ12`) being zero as the
  sentinel, which is wire-compatible and keeps the §16 vectors intact,
  and nudges the single colliding real position (leaf 0, polyline 0,
  direction 0, ratio exactly 0) one quantum along the segment. A v2
  should encode `leafCell + 1` and be done with it.
- Flood posture for relay peers caching tags they cannot open, given
  that no signature is verifiable without `P` and no proof-of-work
  envelope exists on this channel.
- Whether §10 rule 3's public-route test holds up in practice: a run
  plan is public, but does correlating contributions to it leak
  anything about *deviations* from the plan (a driver's detour, an
  unscheduled stop) that the timetable does not?
- Whether coarse mode's ETA error is small enough that fine mode is
  never needed for school runs — measurable in T5, and the answer
  decides a safety default rather than a feature.
- Cross-epoch handover mid-run, when the index republishes while a bus
  is halfway through its route: `EPOCH_OVERLAP` covers the consumer,
  but the run plan, `planRef` and the link's `epochPrefix8` are
  epoch-bound too. A link that dies mid-route is a bad failure.
- Multi-vehicle threads (a run continued by a replacement bus): a new
  keypair means a new link to every family mid-morning, which is
  unworkable. Chaining a successor key inside the sealed body of the
  final update is the obvious fix and needs its own analysis.

## 19. Conformance checklist

This implementation is conformant on every row below except the one
marked `[~]`, which is honest about a piece that is not built yet. Boxes
are ticked only where a test in `test/pulsemesh_thread_*.test.js` asserts
the behaviour, not where the code merely looks right.

- [x] Reproduces §16 byte-identically: key schedule from `P`, tag,
      rendezvous, link, signed preimage, sealed record.
- [x] Generates a fresh keypair per run; never publishes, reuses, or
      transmits `P`; never uses it as a peer identity. *(Not logging it
      is the embedding application's obligation — the library never
      writes it anywhere, and a test asserts `P` does not appear in a
      record's bytes.)*
- [x] Never transmits `K_content` or a nonce; derives every nonce from
      `noncePrefix ‖ seq`.
- [x] Carries the capability in a URL fragment, never a query string or
      path.
- [x] Rejects: bad AEAD tag, missing or invalid signature, non-increasing
      `seq`, out-of-window `unixSeconds`, records past `notAfter`,
      unknown tags.
- [~] Derivation of the tag, topic and rendezvous key from the link
      alone is implemented and tested, and the link carries no host,
      mailbox, or bootstrap address. **Live DHT provider lookup is not
      implemented** — it is transport wiring on the traffic channel's
      libp2p adapter, and until it lands a deployment must supply peers
      by the ordinary bootstrap path.
- [x] Every PMR1 padded to 4/8/16 tags with CSPRNG decoys; responders
      never distinguish unknown from empty tags.
- [x] Thread records never enter a traffic aggregate.
- [x] Anonymous contribution while a thread is active is off by default,
      settable only per-thread and never inferred; suppressed at stops
      per §10 rule 4; reticent profile (protocol §10.2) enforced for
      any thread whose route is not published.
- [x] ETA computed locally with `matrix()` over the planned order; no
      request carries the subscriber's stop, home, or identity.
- [x] The subscriber exposes all four rows of §12 as `status().row`
      with the exact claim each is allowed to make, and reports
      `live: false` past `THREAD_STALE`. *(Honouring it is the UI's
      obligation; the library makes the correct claim available and
      refuses to overstate it.)*
- [x] Coarse is the library default (`createThreadPublisher` without a
      `mode`); fine requires an explicit operator choice.
