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

Status: **design**. Nothing in this document is implemented. The
engine primitives it stands on (`snap()`, `route({ live })`,
`matrix()`, segment identity) are implemented and tested; one small
engine addition is required (§13).

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

### 4.1 Keys

Per run, the publisher generates:

- **`threadRoot`** — 32 CSPRNG bytes. The *read* capability. Never
  appears on the mesh in any form. From it, HKDF-SHA256 (empty salt,
  the label as `info`) derives:

  | Key | `info` label | Length |
  | --- | --- | --- |
  | `K_topic` | `pulsemesh/thread/topic/1` | 32 |
  | `K_content` | `pulsemesh/thread/content/1` | 32 |
  | `noncePrefix` | `pulsemesh/thread/nonce/1` | 4 |

- **publisher keypair** — Ed25519, per run. The *write* capability. The
  private key never leaves the publishing device.

Separating them is the non-obvious part and it is load-bearing: every
subscriber holds `K_content`, so without a signature any parent could
seal a well-formed update and move the bus. Confidentiality comes from
the AEAD; **authenticity comes only from the signature**.

### 4.2 Topic derivation

```
tag(window) = HMAC-SHA256(K_topic,
                "pulsemesh/thread/1" ‖ epoch32 ‖ uint64be(window))[0..8]
topic       = /rangefind/pulsemesh/1/t/<epochPrefix16hex>/<tagHex16>
```

`window = floor(unixSeconds / 300)`, the same rotation as the traffic
channel, with the same `TOPIC_OVERLAP` dual-subscribe behaviour.

Three consequences worth stating:

1. The topic name is a pseudorandom 8 bytes. It does not contain a
   zone, a route number, an operator, or anything else. Threads cannot
   be enumerated or browsed.
2. Subscribing requires the capability. There is no "listen to all
   buses" position, even for a relay.
3. Tags rotate every 5 minutes and are unlinkable across rotations
   without `K_topic`, so an observer cannot follow one thread through
   the day by its address.

The cost is that thread topics carry no geography, so they cannot use
the traffic channel's zone-scoped gossip mesh for discovery. §8 handles
delivery.

### 4.3 Distribution and revocation

The capability travels as a **thread invite** (§5.4) over a channel the
operator already has: a QR code, a deep link, the order confirmation,
the school portal. It is a bearer token — anyone holding it can read
the thread until `notAfter`, exactly like today's delivery-tracking
URLs, but read-only, run-scoped, and time-bounded.

Revocation is **non-renewal**, which is why roots are per-run:

- Delivery: no revocation problem exists.
- School: the operator's portal issues the root for each run to
  currently-entitled families, a day or two ahead. Removing a family
  means not serving tomorrow's root. A compromised root exposes one
  run, not a school year.

Group rekeying is thereby avoided entirely. The cost is a per-user
authenticated service — but it is the operator's existing portal, not
new rangefind infrastructure, and it learns only *who is entitled*,
never who watched or where anyone lives.

Where the distribution channel is not confidential (a logged email
pipeline, say), the invite MAY instead be sealed to a subscriber-held
X25519 public key; `threadRoot` is then wrapped per recipient and the
rest of the protocol is unchanged.

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

### 5.4 Thread invite (out-of-band, never on the mesh)

```json
{
  "format": "pulsemesh-thread-invite-v1",
  "epoch": "<64 hex — the route-graph root sourceHash>",
  "threadRoot": "<64 hex>",
  "publisherKey": "<32-byte ed25519 public key, hex>",
  "mode": "coarse",
  "notBefore": 1754265600,
  "notAfter": 1754294400,
  "planUrl": "https://cdn.example/runs/2026-08-04/r17.json",
  "mailboxes": ["/dns4/mailbox.example/tcp/443/wss/p2p/12D3Koo…"]
}
```

A subscriber MUST reject updates outside `[notBefore, notAfter]` and
MUST discard the whole invite at `notAfter`. `planUrl` is an ordinary
static asset; a subscriber that can reach it but not the mesh still has
a usable product (§12).

### 5.5 Mailbox messages

Protocol id `/rangefind/pulsemesh/1/thread`, magic-discriminated:

- **PMP1** (publish to mailbox): `"PMP1"` ‖ verbatim PMT1.
- **PMR1** (fetch): `"PMR1"` ‖ epochPrefix8 ‖ tagCount varint (MUST be
  exactly 4, 8, or 16) ‖ per tag: bytes(8) ‖ sinceSeq varint.
- **PMM1** (response): `"PMM1"` ‖ epochPrefix8 ‖ tagCount ‖ per tag:
  bytes(8) ‖ recordCount varint ‖ that many verbatim PMT1 records.
  Tags echo the request order. Unknown tags return count 0; a mailbox
  MUST NOT distinguish "tag I do not hold" from "tag with no new data".

The padded-request discipline is inherited from the traffic channel's
cell fetches, and here it is *free*: tags are indistinguishable from
uniform random bytes, so decoy tags cost one CSPRNG call and are
perfectly indistinguishable from real ones. A mailbox operator serving
a school's own buses still cannot tell which of 8 tags a given
subscriber came for.

## 6. Constants

| Name | Value | Tunable | Meaning |
| --- | --- | --- | --- |
| `THREAD_UPDATE_FINE` | 5 s | yes | publish cadence, fine mode |
| `THREAD_UPDATE_COARSE` | on stop events + 60 s heartbeat | yes | coarse mode |
| `THREAD_MAX_RECORD_BYTES` | 256 | no | max encoded PMT1 |
| `THREAD_MAX_AGE` | 120 s | yes | oldest update a subscriber accepts |
| `THREAD_MAX_FUTURE_SKEW` | 15 s | yes | |
| `THREAD_STALE` | 90 s | yes | UI must stop claiming "live" past this |
| `THREAD_MAILBOX_TTL` | 600 s | yes | mailbox retention per tag |
| `THREAD_MAILBOX_RING` | 240 | yes | records retained per tag |
| `THREAD_MAILBOX_PEERS` | 3 | yes | mailboxes a publisher pushes to |
| `THREAD_POLL_INTERVAL` | 10 s ± 3 jitter | yes | mailbox poll when gossip is cold |
| `THREAD_MAX_RUN_SECONDS` | 21600 (6 h) | no | absolute thread lifetime |
| `THREAD_TAG_BUDGET` | 32 / peer / window | yes | new mailbox tags per source peer |
| `THREAD_PUSH_RATE` | 1 rec / 3 s / tag | yes | mailbox write rate limit |

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
6. Ed25519 signature verifies against the invite's `publisherKey`.
   **A read-capable but unsigned record is an attack, not an error**;
   log it and drop the delivering peer's standing.
7. `seq` strictly greater than the highest accepted `seq` for this
   thread — replay and rollback protection.
8. `unixSeconds` within `[now − THREAD_MAX_AGE, now + THREAD_MAX_FUTURE_SKEW]`
   and within `[notBefore, notAfter]`.
9. Plausibility: implied speed between consecutive accepted updates ≤
   the class cap × 1.15 from traffic-channel §6 rule 10; `geomRef >>> 1`
   below the leaf's polyline count.

A **mailbox** validates only steps 1–3 and the rate limits — it holds
no keys and therefore cannot check 4–9. Write access is consequently an
operator-configured peer allowlist by default; open mailboxes are
opt-in and live entirely on `THREAD_PUSH_RATE` and
`THREAD_TAG_BUDGET`. This is a real difference from the traffic
channel, where proof-of-work makes open write access safe: the thread
channel has no equivalent because its records are opaque to the
relay.

## 8. Delivery

Three paths, in preference order. All three carry identical bytes, so a
thread does not know or care which one reached its subscriber.

1. **Gossip.** Publisher and subscriber both subscribe to `tag`'s
   topic. Works where the mesh is dense and both are online. Best case,
   no infrastructure.
2. **Mailbox keepers.** The publisher PMP1s every update to
   `THREAD_MAILBOX_PEERS` mailboxes from the invite; subscribers PMR1
   with padded tag sets. This is the default path and the one the
   product should be designed around, because a parent's phone is
   asleep and a courier's route may cross no peers at all.
3. **Direct.** Dispatch consoles and other always-on subscribers can
   hold a stream to the publisher's peer.

A **mailbox is blind**: opaque ciphertext, pseudorandom addresses that
rotate every 5 minutes, no way to count threads, tell them apart across
rotations, or learn who fetched which of 8 tags. And it is naturally
operated by the party that already knows the answer — a school board or
a delivery platform running its own mailbox learns nothing it did not
already have. That is the honest reason this is acceptable
infrastructure where a plaintext tracking server is not.

It is still, like the credential issuer in
[pulsemesh.md](pulsemesh.md#deltas-from-the-original-concept) §5, a
non-static service, and product copy must say so.

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
- **Content-free push.** A mailbox emits an empty "tag has news" push;
  the device wakes and decrypts. This links a push token to a tag at
  the mailbox — the one linkage in this design that a mailbox can
  actually accumulate. It MUST be opt-in and MUST be disclosed.

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

These three are normative, and the third is the uncomfortable one.

1. **A thread never feeds a traffic aggregate.** Thread updates are
   authenticated single-source; traffic aggregates are corroborated
   multi-source with `AGG_MIN_REPORTS`. Admitting signed fleet data
   into the aggregate would turn one fleet key into a traffic
   authority, which is precisely the property the traffic channel was
   designed not to have. A fleet that wants to contribute traffic emits
   ordinary PMC1 records under the same PoW and corroboration
   minimums as everyone else.

2. **A thread consumes traffic aggregates freely.** §9. The subscriber
   is a normal traffic consumer, with the normal padding, decoy and
   split rules on every cell fetch.

3. **A device MUST NOT publish a thread update and an anonymous
   contribution for the same (segment, time bucket).** A vehicle doing
   both is trivially correlatable — an observer who sees a PMT1 and a
   PMC1 leave the same peer in the same 15 s window has just
   de-anonymized the contribution, and worse, can now follow the
   *encrypted* thread by watching the plaintext one. The default is
   therefore: **a thread publisher does not contribute anonymously
   while a thread is active.**

   This is genuinely costly. Fleets are the best sustained contributors
   in the whole design — fixed routes, professional drivers, powered
   devices, all day — and this rule benches exactly them. A weaker rule
   worth measuring in the phase-2 simulation: contribute only on
   segments already carrying ≥ 3 other contributors in the current
   bucket, so the anonymity set is never one. Whether that survives a
   timing analysis is an open question (§17), and until it is measured
   the conservative rule stands.

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

One, small:

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
generates about 47 MB in total across the mesh, and a mailbox holding
`THREAD_MAILBOX_TTL` for all 500 threads needs roughly
500 × 120 × 130 B ≈ 7.8 MB resident. Coarse mode is two orders of
magnitude below that.

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

```
threadRoot   = SHA256("pulsemesh-thread-test-vector")
             = 91d993d38ed5d37e11f4a00726462ca3ebdecbdcbc7e3e9a1ef29a3430a3757f
K_topic      = 318fc2cc6de1de112a0e5651bdc2784feb2f5c647cbc267e9c246fcf29f6e7f1
K_content    = f538c14e179c82cf77bb3eb841b23112c791be2ecc684521924f4ce50a97a3d7
noncePrefix4 = fa427ba0
```

Publisher key from seed `SHA256("pulsemesh-thread-publisher")` =
`0a0d7e721ae1571ceb2c444c3c661176175d0392d27040031a9f175f98de8ded`:

```
publisherKey = cdf0cfb422cd2faaf24eb8343cff641a41ca4be4a3aa429e7229d690719761bd
```

### 16.2 Topic tag

At unixSeconds 1754265600, window = 5847552:

```
tag   = b3a426eff70cd207
topic = /rangefind/pulsemesh/1/t/f44796c8cc1f3fa7/b3a426eff70cd207
```

### 16.3 Sealed update

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

Sealed at `seq` 42 — nonce `fa427ba0000000000000002a`, AAD
`504d5431f44796c8cc1f3fa7b3a426eff70cd2072a` — the full 130-byte PMT1
record is:

```
504d5431f44796c8cc1f3fa7b3a426eff70cd2072a6cc033c740fac2cad2fc61
9bb21e325c8deff858cc135c6eea2af21bd84103e873a4fd8b321e6d6c48f847
847529e6b0618c60d3b82985967f0895652686f9c4070219d6b42cd3923de5a0
3b1486dd26645c801408fd1204619b82cfb57c79d29054d923e8d93893d376b3
7ae5
```

## 17. Repository layout and milestones

```
src/pulsemesh/thread_crypto.js   HKDF schedule, tag derivation, seal/open, Ed25519  (§4, §5.1)
src/pulsemesh/thread_codec.js    PMT1/PMTP/PMP1/PMR1/PMM1                            (§5)
src/pulsemesh/thread_publish.js  publisher state machine, snap-driven, modes         (§5.3, §11)
src/pulsemesh/thread_consume.js  validation, seq ledger, staleness                   (§7)
src/pulsemesh/thread_mailbox.js  blind ring-buffer keeper, rate limits               (§5.5, §8)
src/pulsemesh/thread_eta.js      locate + fixed-order matrix arrival estimation      (§9)
test/pulsemesh_thread_*.test.js  per-module + §16 vectors
```

- **T1 — crypto and codecs (no network).** Reproduce §16 byte for byte;
  property test that a tampered AAD, a rolled-back `seq`, and a
  correctly-sealed-but-unsigned record are all rejected. Requires
  `engine.locate()` (§13).
- **T2 — loopback thread.** Publisher and subscriber in one process
  over an in-memory duplex, publisher fed by the demo's simulated
  drive; subscriber renders a position and an ETA that moves when a
  synthesized jam is injected into the traffic provider. This proves
  the §9 claim, which is the whole thesis, with no networking at all.
- **T3 — mailbox.** Blind keeper with ring buffers, padded PMR1,
  rate limits and tag budgets; sleep/wake test where a subscriber
  offline for 4 minutes recovers the full sequence.
- **T4 — coarse pilot.** One operator, one route, coarse mode, run
  plans published as static assets, invites through the operator's
  existing portal. Measure: ETA error against observed arrival, wake
  budget and battery on the subscriber side, and whether families ever
  ask for fine mode once coarse works.

## 18. Open questions

- Whether a fleet can contribute anonymous traffic while publishing a
  thread without becoming correlatable (§10 rule 3). This is the most
  valuable open question in the design, because fleets are the best
  contributors the traffic channel could have.
- Mailbox abuse posture for open (non-allowlisted) mailboxes, given
  that a relay cannot verify a signature it has no key for.
- Whether coarse mode's ETA error is small enough that fine mode is
  never needed for school runs — measurable in T4, and the answer
  decides a safety default rather than a feature.
- Content-free push (§8.1): is the mailbox's token↔tag linkage
  acceptable, and can it be blinded (rotating tokens, an intermediary
  that sees one but not the other) without adding a fourth service?
- Cross-epoch handover mid-run, when the index republishes while a bus
  is halfway through its route: `EPOCH_OVERLAP` covers the consumer,
  but the run plan and `planRef` are epoch-bound too.
- Multi-vehicle threads (a run continued by a replacement bus): a new
  keypair under the same `threadRoot`, or a new invite?

## 19. Conformance checklist

- [ ] Reproduces §16 byte-identically: key schedule, tag, signed
      preimage, sealed record.
- [ ] Never transmits `threadRoot`, `K_content`, or a nonce; derives
      every nonce from `noncePrefix ‖ seq`.
- [ ] Rejects: bad AEAD tag, missing or invalid signature, non-increasing
      `seq`, out-of-window `unixSeconds`, records outside
      `[notBefore, notAfter]`, unknown tags.
- [ ] Mailbox is blind: no key material, no distinction between unknown
      and empty tags, enforced ring/TTL/rate/tag budgets.
- [ ] Every PMR1 padded to 4/8/16 tags with CSPRNG decoys.
- [ ] Thread records never enter a traffic aggregate; publisher does not
      emit PMC1 for a segment/bucket it published a PMT1 for.
- [ ] ETA computed locally; no request carries the subscriber's stop,
      home, or identity.
- [ ] UI distinguishes all four rows of §12 and stops claiming "live"
      past `THREAD_STALE`.
- [ ] Coarse is the default for child transport; fine requires an
      explicit operator choice.
