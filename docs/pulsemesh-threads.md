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

A **recurring route** (§21) keeps this answer and moves it one level
down. Its link is term-long and deliberately does not rotate, so
non-renewal cannot be what bounds a leak; what bounds it there is that
*publish authority* expires every day or two under a certificate, while
the read capability the link grants is unchanged in scope — one route,
read-only, at the §11 granularity the operator chose. There is still no
revocation list anywhere (§21.7).

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
dependencies. AES-GCM, HKDF and HMAC-SHA-256 really are everywhere.
Ed25519 is not: Chrome only enabled it in WebCrypto unflagged in 137, so
Android WebView 133 — the emulator the Wayfind app runs on, and a large
share of shipping phones — throws `NotSupportedError` from
`importKey({ name: "Ed25519" })`, and Hermes has none at all. That used
to surface as `verifyThread` returning false, an absent algorithm
disguised as a bad signature, which refused valid dispatch tickets on
the device. So `src/pulsemesh/ed25519.js` carries a dependency-free
RFC 8032 implementation (BigInt field arithmetic, SHA-512 in the same
file), and `thread_crypto.js` probes the host once at first use and
routes every Ed25519 operation to whichever path exists. The probe is
the only thing that decides: a `subtle.verify` that *resolves* false is
a genuinely bad signature and is never retried against the fallback.

The fallback is not constant time — it costs roughly 1.7 ms to sign and
1.9 ms to verify on a laptop, against microseconds natively — which is
affordable because threads sign at human cadence, and acceptable because
the key it handles is a per-run capability that expires with the job
rather than a device identity. `setThreadCryptoImplementation` still
overrides everything, for a host that would rather supply a native one.

### 5.2 PMTP — plaintext body

Never transmitted in the clear.

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMTP"` | |
| 2 | unixSeconds | varint | whole seconds; the audience is entitled to precision |
| 3 | state | u8 | §5.3 |
| 4 | mode | u8 | 1 coarse, 2 fine — what the link reveals (§11) |
| 5 | travelMode | u8 | 0 unspecified, 1 car, 2 bike, 3 foot — how the vehicle moves (§5.2.2) |
| 6 | leafCell | varint | 0 when position is withheld |
| 7 | geomRef | varint | `polyline*2 + direction` |
| 8 | ratioQ12 | varint | 0..4095, position along the segment |
| 9 | speedBin | u8 | 0..44, as traffic channel §2.5 |
| 10 | stopIndex | varint | 1-based; the last stop the run has **dealt with** in plan order, 0 = none yet |
| 11 | planRef | bytes(8) | SHA-256 prefix of the run plan, zeros = none |
| 12 | stopCount | varint | plan stops the outcome map covers, 0 = none |
| 13 | outcomes | bytes(⌈stopCount/4⌉) | 2 bits per stop, LSB-first (§5.2.1) |
| 14 | markFlags | u8 | bit 0 → fields 15–19, bit 1 → fields 20–21, bit 2 → field 22; other bits reserved and **refused** |
| 15 | lastStopIndex | varint | present iff bit 0 |
| 16 | lastOutcome | u8 | present iff bit 0 |
| 17 | lastReason | u8 | present iff bit 0 (§5.2.1) |
| 18 | lastPhoto | u8 | present iff bit 0; 0 none, 1 followed by field 19 (§20.7) |
| 19 | photoHash | bytes(32) | present iff field 18 is 1; SHA-256 of the **sealed** blob |
| 20 | reasonCount | varint | present iff bit 1; carried reasons (§5.2.1) |
| 21 | reasons | (varint ‖ u8) × reasonCount | stopIndex, then reason in bits 0–6 and "this mark had a photo" in bit 7. Strictly increasing by stopIndex |
| 22 | photoCount | varint | present iff bit 2; distinct photo commitments this run has published (§20.7) |
| 23 | noteLen ‖ note | varint ‖ bytes | operator message, ≤ 64 bytes UTF-8 |
| 24 | signature | bytes(64) | Ed25519 over fields 1–23 |

Field 14 was a plain `lastPresent` boolean and bit 0 is still exactly
that boolean, so a record with no carried reasons and no photos encodes
to the bytes it always did — the §16.4 vector included. That is not
nostalgia: it is what makes the day this channel is actually sized for
— every stop delivered, nothing to explain — pay **nothing at all** for
the fields below.

`stopIndex` is **the last stop the run has dealt with, in plan order** —
visited and resolved, or passed. It is monotonic and it is a position in
the plan, never a high-water mark of the paperwork: outcomes for stops
beyond it live in the outcome map (§5.2.1), and it advances past a
marked stop only when every stop up to that one has been resolved. Two
rules follow, and both are load-bearing:

- **Dwell detection** raises it to the nearest planned stop the vehicle
  is standing at, and never lowers it — a van that swings back past stop
  2 on its way to stop 9 has not undone stops 3 to 8.
- **Marking** records the outcome for any stop, and then advances
  `stopIndex` over the contiguous run of resolved stops in front of it.
  Marking the stop the run is on moves it by one, and hops over anything
  already marked immediately after. Pre-marking a far-future stop moves
  it not at all.

That second rule is what keeps the customers in between. A follower's
estimate treats every stop at or before `stopIndex` as already dealt
with (§9), so if pre-marking stop 7 jumped `stopIndex` to 7, every
follower at stops 4 to 6 would be told their delivery was behind the van
and shown nothing.

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

#### 5.2.1 Per-stop outcomes

**An outcome is asserted, never inferred.** Dwell detection (§10 rule 4)
keeps advancing `stopIndex`, because a vehicle standing at a planned
stop really is progress and movement really does establish it. It never
writes an outcome. The reverse also holds: marking a stop writes the
outcome map and moves `stopIndex` only as far as the contiguous resolved
run allows (§5.2), because an assertion about stop 7 is not a claim that
the vehicle has reached stop 7. A van parked outside a house for four minutes could
have delivered, been refused, or found nobody in, and those are three
different sentences to send a customer; the only party that knows which
is the driver. An unmarked stop the vehicle drove past stays `0`
(pending) on the wire, and `0` means *nobody has said*, not *the vehicle
has not arrived*.

| Value | Outcome |
| --- | --- |
| 0 | pending — no assertion has been made |
| 1 | delivered |
| 2 | skipped |
| 3 | failed |

`lastReason` gives the most recent mark a machine-readable why, so a
follower's app renders it in its own language rather than parsing free
text. Anything it cannot express goes in the note (field 23).

| Value | Reason |
| --- | --- |
| 0 | none |
| 1 | customer absent |
| 2 | refused |
| 3 | inaccessible |
| 4 | parcel missing or damaged |
| 5 | other — details in `note` |

The map is **cumulative in every record**, and that is deliberate rather
than lazy. This is a lossy gossip channel with no mailbox and no
retention past `THREAD_MAX_AGE` (§5.5): a follower who joins at 14:00,
or whose phone slept through the four minutes in which stop 7 was
skipped, can never recover that record from anywhere. A delta-encoded
map would leave them permanently and silently wrong about the day. Two
bits a stop is cheap enough to simply repeat the whole answer: a
200-stop day costs 50 bytes, in every record, forever.

`lastOutcome` is redundant with the map by construction, and carried
anyway: without it a follower would have to diff two bitmaps to discover
which stop just changed. It is what lets a card say "stop 7 skipped —
customer absent" from a single record.

##### Reasons are cumulative too, and for the same reason

`lastOutcome` holds **one** mark and the next mark replaces it. That is
a data-loss gap, and it is the one this channel is least able to afford.
A driver marks stop 3 skipped, customer absent, with a doorstep photo,
in a dead zone; marks stop 4 before coverage returns; and stop 3's
reason and photo commitment are gone from the wire permanently, because
nothing re-sends a record and §5.5 catch-up reaches back only
`THREAD_MAX_AGE`. The board self-heals to "stop 3 skipped" from the
cumulative map and can never say why. For a courier fleet that sentence
is the most valuable record of the day.

So fields 20–21 carry the reason for **every** stop the map says is
skipped or failed, sparsely, in every record:

```
reasonCount varint, then reasonCount × ( stopIndex varint, reason u8 )
```

Delivered stops need no reason and pending stops have nothing to say, so
a normal day's list is empty and costs zero bytes. Three failed stops on
a 200-stop day cost seven. It self-heals exactly as the outcome map
does: one record after the outage restates the lot.

The reason byte is not just the code. Bits 0–6 are the `lastReason`
value; **bit 7 says that mark carried a photo commitment** (§20.7). That
bit is free — the code needs six — and it is the whole of what a
subscriber can be told about a commitment that never reached it.

**Reason `0` is carried explicitly, not omitted.** That is what makes a
*gap* in the list mean something. Against a stop the map says is skipped:

| The list | What it means |
| --- | --- |
| entry, reason 1–5 | the driver said why |
| entry, reason 0 | the driver marked it and stated no reason — a complete answer |
| **no entry** | the reason is **lost**: it aged out of the cap, or the run predates this field |

`stopReasonFor(update, stopIndex)` is the accessor that keeps those
apart, returning `reasonKnown: false` for the third row rather than a
cheerful zero, and `hasPhoto: null` rather than `false` for a proof that
may well exist. `estimateArrival` reports the same pair, so a customer
whose parcel was refused at 09:12 is still told why at 16:00 — the old
code could only answer when their stop happened to be the most recent
mark in the record they were holding.

##### The cap: bounded, never refused

The list is bounded twice — by `THREAD_MAX_REASONS` (16), and by
whatever is left of the 213-byte body once the plan, the note and any
photo commitment have taken theirs. `fitStopReasons` applies both.

It is **bounded rather than refused**, and the asymmetry with §5.2.2's
plan-size refusal is deliberate. A plan's size is known before the run
starts, so refusing it is a configuration error caught once, at the
depot. A reason list grows during the day out of things a driver does at
doors; refusing to encode at 09:41 because the twelfth stop failed would
take the outcome map, the position, the state and the whole rest of the
record down with it. That is a far worse loss than the reason it was
protecting.

**The oldest entries are evicted, not the newest.** The list is
cumulative and self-healing, so what a record is buying with those bytes
is recovery for a subscriber that was not listening. The gap that
matters is a recent one: a subscriber that has been present already
received the early reasons, and the ones it is most likely still missing
are the ones just published. Losing the oldest therefore degrades
gracefully — those reasons were almost certainly delivered when they
were new — while losing the newest would lose exactly the ones the
mechanism exists for.

The 16 is sized on the day the channel is built for. A 200-stop plan
leaves 57 bytes (§5.2.2); 16 entries cost 33 of them at one-byte stop
indices and 49 at the widest a 200-stop plan can produce, so the cap is
reachable at any hour without the reason list ever being the field that
does not fit. It is also 8 % of a 200-stop day, several times the
non-delivery rate a courier fleet actually runs at.

A long **note** can squeeze the list, and is allowed to. Notes are
operator announcements on the occasional record, not a per-fix field, so
the next ordinary fix carries the full list again — the cumulative
property makes that self-correcting in one record rather than a loss.

Marking is a **stop event**, so it publishes immediately on both
granularities rather than waiting out the cadence (§11). A customer
whose parcel went back to the depot should not learn it 59 seconds after
the driver said so.

Re-marking a stop overwrites it. A failed attempt that is retried and
delivered is an ordinary day, not a protocol violation.

Field 19 is a **commitment, never content**: 32 bytes naming a
proof-of-delivery photo whose bytes travel elsewhere, on demand, over
§20.7's own protocol. Field 18 is written whenever bit 0 of field 14 is,
so a mark always says whether a photo exists and the answer is inside the
signature — a record cannot have a photo attached to it after the fact,
and one cannot be stripped off without breaking the signature. A
subscriber exposes it as `lastOutcome.photoHash`: lowercase hex, or
`null`. Hex rather than bytes because every consumer of it uses it as a
content address — a map key, a DOM attribute, an equality test — and
none of them wants a `Uint8Array`.

**Commitments are not carried cumulatively, and cannot be.** A
commitment is 32 bytes; a second one does not fit the day this channel
is sized for (§5.2.2 leaves 25 bytes beside the first). §20.7 works the
consequence through, including the fetch-by-stop design that was
rejected for it. What the record carries instead is field 22: how many
distinct commitments the run has published so far. A subscriber holding
fewer distinct hashes than that knows **exactly how many** proofs it can
never fetch, and bit 7 of the reason byte names which skipped or failed
stops they belong to. The loss is real and it is visible, which is the
most this budget can honestly buy.

#### 5.2.2 How large a plan can be

`THREAD_MAX_RECORD_BYTES` is 256 and the body is signed and sealed
*before* it is framed, so a record that would overflow has already been
signed. The encoder therefore refuses at encode time, with the number of
stops that would have fitted this particular body.

Subtracting the PMT1 framing — magic 4, epochPrefix8 8, tag 8, `seq`
varint ≤ 5, `ctLen` varint 2, AEAD tag 16 — leaves **213 bytes** for the
plaintext body, signature included. Against the widest position varints
a planet-scale leaf index can produce:

| Note | Max plan stops | …with a photo commitment (§20.7) |
| --- | --- | --- |
| none | 428 | 300 |
| 32 bytes | 300 | 172 |
| 64 bytes (the maximum) | 172 | 48 |

A 200-stop day — the size this feature is scaled for — fits with **57
bytes** of note left over, or **25** when the mark carries a photo
commitment. **A full 64-byte note and a 200-stop plan do not both fit**,
with or without a photo, and the encoder says so rather than truncating
either. That is the honest ceiling of a 256-byte record, and raising it
would mean giving up the fixed record size the whole channel's flood
resistance rests on.

Two of those 32 bytes' worth of arithmetic is worth stating plainly.
The photo flag (field 18) costs one byte on **every** mark, photo or
not, which is why the no-note figure moved from 432 to 428 and the
200-stop headroom from 58 to 57; and a commitment costs exactly as much
as 32 bytes of note, which is why each cell in the third column is the
row below it in the second. A dispatcher who wants both a full note and
a 200-stop day with photos has to give something up, and the encoder
tells them which.

**Every number above is unchanged by the carried reason list**, and that
is the point of putting it behind a flag bit rather than a count byte. A
run with nothing skipped or failed and no photos writes none of fields
20–22 and encodes to the bytes it did before they existed.

What they cost when a day does need them, measured out of the same
200-stop body, against its 57 bytes of headroom:

| Carried | Bytes | 200-stop note headroom |
| --- | --- | --- |
| nothing (an all-delivered day) | 0 | 57 |
| 3 reasons | 7 | 50 |
| 3 reasons + photo counter | 8 | 49 |
| 16 reasons (`THREAD_MAX_REASONS`), stops < 128 | 33 | 24 |
| 16 reasons, stops 128–16383 | 49 | 8 |
| 16 reasons + photo counter, widest | 50 | 7 |

One reason entry is a stop-index varint and one byte; the list adds a
count varint on top; the photo counter is one byte for any run under 128
photos. The outage this exists for — two stops marked in a dead zone,
both with reasons and both with photos — costs **six bytes**: a count,
two pairs, and the counter.

`fitStopReasons` spends only what is there. On a body with no room left
it keeps the newest entries that fit and drops the rest (§5.2.1); it
never refuses, and the plan-size refusal above is unchanged and still
the only thing that does.

### 5.3 States

| Value | Meaning |
| --- | --- |
| 1 | scheduled, not started |
| 2 | en route |
| 3 | dwelling at `stopIndex` — the last stop dealt with (§5.2) |
| 4 | completed |
| 5 | canceled |
| 6 | off-plan (diverted, vehicle swapped — see `note`) |

There is deliberately no "approaching your stop" state and no "delayed"
field. Both are subscriber-side computations (§9): the publisher does
not know which stop a subscriber cares about, and must not learn it.

There is also no "delivered" state, and that is a different reason.
`state` describes the vehicle at an instant and is replaced by the next
record; a delivery is a fact about a *stop* that has to survive every
record after it. Outcomes therefore live in their own cumulative field
(§5.2.1), and the two are independent: a van can be `en route` with
seven stops already delivered behind it.

### 5.4 The link (out-of-band, never on the mesh)

The capability is 45 bytes, so it fits an SMS, a QR code, or a tap
target without a shortener:

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | version | u8 | 1 self-signed; 2 delegated signing (§21) |
| 2 | publicKey | bytes(32) | `P` — the whole capability. On a v2 link, the route **root** |
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

Version 1 is the original and remains the default: the key in field 2
signs the run's own records. Version 2 says the key is a route **root**
and records are signed by a certified, short-lived day key, so a
recurring route's link can outlive its drivers (§21). A holder learns
which from the artifact, never by guessing per record.

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

§20.7 adds a sibling protocol, `/rangefind/pulsemesh/1/photo`, for
proof-of-delivery blobs. It is deliberately *not* a second magic on this
one: the two have opposite shapes — catch-up serves 256-byte records to
anyone from a shared relay cache and gets cheaper as the audience grows,
while a photo is one ~100 KB transfer that only the publisher can ever
answer.

## 6. Constants

| Name | Value | Tunable | Meaning |
| --- | --- | --- | --- |
| `THREAD_UPDATE_FINE` | 5 s | yes | publish cadence, fine mode |
| `THREAD_UPDATE_COARSE` | on stop events + 60 s heartbeat | yes | coarse mode |
| `THREAD_MAX_RECORD_BYTES` | 256 | no | max encoded PMT1 |
| `THREAD_RECORD_OVERHEAD` | 43 | no | PMT1 framing around the sealed body |
| `THREAD_MAX_BODY_BYTES` | 213 | no | derived: what a PMTP body may spend, signature included (§5.2.2) |
| `THREAD_MAX_AGE` | 120 s | yes | oldest update a subscriber accepts |
| `THREAD_MAX_FUTURE_SKEW` | 15 s | yes | |
| `THREAD_STALE` | 90 s | yes | UI must stop claiming "live" past this |
| `THREAD_CACHE_TTL` | 600 s | yes | catch-up cache retention per tag |
| `THREAD_CACHE_RING` | 240 | yes | records cached per tag |
| `THREAD_CACHE_TAGS` | 256 | yes | tags a relay peer caches (LRU) |
| `THREAD_PROVIDE_INTERVAL` | 120 s | yes | DHT provider re-advertise |
| `THREAD_POLL_INTERVAL` | 10 s ± 3 jitter | yes | catch-up poll when gossip is cold |
| `THREAD_MAX_RUN_SECONDS` | 21600 (6 h) | no | default thread lifetime; a ticket-born run uses its own `notAfter` instead (§20.6) |
| `THREAD_TAG_BUDGET` | 32 / peer / window | yes | new cached tags per source peer |
| `THREAD_CACHE_RATE` | 1 rec / 3 s / tag | yes | relay cache admission rate |
| `THREAD_MAX_REASONS` | 16 | no | carried per-stop reasons in one record; also bounded by whatever the body has left (§5.2.1) |
| `THREAD_MAX_PHOTO_BYTES` | 131072 | yes | largest sealed proof-of-delivery blob (§20.7) |
| `THREAD_CERT_INTERVAL` | 60 s | yes | how often a route-day publisher re-emits its PMTC (§21) |
| `THREAD_HELD_RECORDS` | 32 | yes | records held awaiting the day's certificate (§21) |
| `THREAD_MAX_CERT_SECONDS` | 172800 (48 h) | **tighten only** | longest certificate window a subscriber honours (§21.5). Unset in `THREAD_CONSTANTS`; a host that sets it lower wins, higher is ignored |
| `THREAD_CERT_SKEW` | 300 s | yes | clock tolerance on a certificate's window, both edges (§21.5). Unset in `THREAD_CONSTANTS`; the default lives in `thread_route.js` |

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
5. Inner magic well-formed, no trailing bytes. `"PMTP"` is a run update
   and continues below; `"PMTC"` is a §21 day certificate and takes
   §21.5's path instead. A v1 link refuses a `"PMTC"` outright.
6. Ed25519 signature verifies against `P` from the link. **A record
   that decrypts but does not verify is an attack, not an error** —
   it means a link holder tried to publish; log it and drop the
   delivering peer's standing. On a **v2** link the signature verifies
   against a *certified day key* instead, and a record no held
   certificate covers is refused as `awaiting-certificate` and held —
   never called a forgery, because a joiner who has not yet heard the
   day's certificate is not one (§21.5).
7. `seq` strictly greater than the highest accepted `seq` for this
   thread — replay and rollback protection. On a v2 link the ledger is
   per signing authority, because two service days cannot coordinate a
   counter across an overnight gap (§21.6).
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

**No admission bond on this channel.** The traffic channel's §5.4 bonds
exist because its records are anonymous and proofless — the delivering
peer must vouch. A thread record is the opposite: authenticated
end-to-end by the thread key's signature and sealed to the audience, so
*any* peer's delivery is verifiable and an unbonded relay is useful
rather than a hole. A home viewer therefore follows a thread with zero
bonds: a read-only traffic node (protocol §11.6) beside a plain
subscription to the thread's derived topics — the `t` namespace the
traffic validator ignores — verified per record by the subscriber.
Covered by a real-TCP test (`§11.6 + threads` in
`test/pulsemesh_wire.test.js`). Publishing a thread needs gossip, but
still no bond: possession of the private seed is the admission, and a
peer without it cannot produce one verifying byte.

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

### 9.1 Stops the vehicle is not going to visit

`stopsBetween` above is the *remaining* chain, and a stop is on it only
when it sits after `stopIndex` in the plan **and** is still pending. An
implementation MUST exclude from the matrix and from the dwell sum both
the stops marked skipped or failed (§5.2.1) and the stops already marked
delivered, wherever those sit in the plan. Left in, one skipped delivery
adds a leg and a dwell to every downstream customer's estimate for the
rest of the day — a systematic, invisible over-prediction that gets
worse as the round goes on.

The two tests are separate on purpose. `index <= stopIndex` says the run
has *been there*; the outcome map says the stop is *resolved*. A stop
the dispatcher marked ahead of the vehicle is resolved without having
been reached, and a follower sitting between the vehicle and that stop
is still owed a real arrival time.

For the subscriber's **own** stop the rule is stronger:

- marked **delivered** → no estimate at all, exactly as for a stop the
  vehicle has already passed;
- marked **skipped or failed** → a result that carries the outcome and
  its reason and has **no arrival time in it at all**. Not a stale one,
  not a large one, none: the vehicle is not coming, and a shape with an
  `arrivalMillis` field in it is a shape some caller will eventually
  render. `basis` is `"marked"`, so the distinction cannot be missed.

### 9.2 Which profile the estimate was computed under

The follower routes from the broadcast position on whatever graph *it*
holds, and the run says how it moves (§5.2 field 5). A courier on a bike
routed on a car graph gets an estimate that is wrong by a wide margin,
in the optimistic direction, and nothing about the number says so.

A host holding several profiles SHOULD pick the one the run names. A
host holding one is not wrong to answer — a car estimate is better than
no estimate — but it MUST NOT answer silently. Every estimate therefore
states the profile it used and whether that profile matched:

| `profileBasis` | Meaning |
| --- | --- |
| `matched` | the run's travel mode and the graph agree |
| `mismatched` | both are known and differ — the number is the graph's, not the run's |
| `unstated` | the run or the host did not say, so no claim is made either way |

`unstated` is not a weaker `matched`. A host that never names its graph
cannot establish a mismatch, and must not imply a match by omission.

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

**Outcomes are stop events, so coarse carries them.** That is by design
rather than by oversight. Coarse exists to withhold *position*, and "the
parcel was left with a neighbour" is not a position — it is the same
class of fact as "the bus called at stop 4", which coarse has always
published. A coarse thread that hid outcomes would withhold the one
thing its audience is following it for while still revealing the
timetable, which is the worst of both settings. Outcomes are also
published immediately on both granularities: waiting out a 60 s coarse
heartbeat to tell a customer their delivery failed is a cadence rule
applied to the wrong kind of record.

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

Per-stop outcomes (§5.2.1) sit **outside** this table and do not move a
row. They are assertions about the past, not claims about a live
position: a stop marked delivered stays delivered when the thread goes
stale, when the position was never published, and when this device has
no traffic at all. A UI may therefore keep showing them in every row —
including "neither", where the last thing known about the day is still
the last thing known about the day.

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

A fine-mode PMT1 with no run plan is 133 bytes (§16). At 5 s cadence
that is 26.6 B/s per thread, 94 KB per thread-hour. A 500-bus fleet in a
morning window generates about 46 MB in total across the mesh. A
delivery round pays for its outcome map in every record — measured at
**+16 bytes on a 50-stop van and +55 on a 200-stop day**, so 11 B/s at
fine cadence — and that repetition is exactly what makes the day's
outcomes recoverable by a follower who joins at lunchtime (§5.2.1). A
subscriber's own catch-up cache is 120 × 133 B ≈ 16 KB per thread —
small enough that
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
travelMode 1 (car), leafCell 3181, geomRef 885 (polyline 442, direction
1), ratioQ12 2048, speedBin 7, stopIndex 8, planRef `10c2d3bff3368b2e`,
no outcome map, no last mark, no note. Signed preimage (31 bytes):

```
504d545080f0bfc406020201ed18f5068010070810c2d3bff3368b2e000000
```

The tail `00 00 00` is stopCount 0, lastPresent 0, noteLen 0 — the
three empty fields, which a run with no plan still spends three bytes
saying it has none.

Ed25519 signature (64 bytes), giving a 95-byte PMTP body:

```
ab698bf41fc97ba4555c3a5a0ae790d039273eb0674f6be0ad61140d41edfe9f
0498a4b4f44a24fefe0b99eaf8953472ebd8b52fc2f89b1a4205943987e30f06
```

Sealed at `seq` 42 — nonce `576cd8f8000000000000002a`, AAD
`504d5431f44796c8cc1f3fa7cbfa3ae2fdc2cf0f2a` — the full 133-byte PMT1
record is:

```
504d5431f44796c8cc1f3fa7cbfa3ae2fdc2cf0f2a6f7a8da4d85d9063cc5eb7
1712716a95268c73bc86a24089e44a7a7d62c928a32cbe9fd21d6a288405cb8f
ea6ddf8df6554e6a47f074ca1de7227eae05ceeeeb8fc01365a1e9bab637909e
3842d6f1f0e84ea37c031daf6067fc919866eadfaf2e888b8ce54447d1e2bdf7
5792b13843
```

## 17. Repository layout and milestones

```
src/pulsemesh/thread_crypto.js   HKDF-from-P, tag/rendezvous, seal/open, Ed25519,
                                 the seed-derived photo key schedule       (§4, §5.1, §20.7)
src/pulsemesh/ed25519.js         RFC 8032 + SHA-512 fallback for hosts without it    (§5.1)
src/pulsemesh/x25519.js          RFC 7748 fallback for hosts without WebCrypto X25519  (§20.9)
src/pulsemesh/thread_codec.js    PMT1/PMTP/PMTC/PMR1/PMM1/PMTF/PMTB + link encode/decode
                                                                          (§5, §20.7, §21)
src/pulsemesh/thread_publish.js  publisher state machine, snap-driven, modes         (§5.3, §11)
src/pulsemesh/thread_consume.js  validation, seq ledger, staleness                   (§7)
src/pulsemesh/thread_cache.js    catch-up ring buffers, padded PMR1, admission caps  (§5.5, §8)
src/pulsemesh/thread_session.js  the channel wired to a transport: gossip tap, tag
                                 rotation, relaying, catch-up, discovery, photo
                                 fetch and serve                      (§4.2, §5.5, §6, §20.7)
src/pulsemesh/thread_ticket.js   PMP1 run plan, PMK1 ticket, PMJ1 job offer, jobId   (§20)
src/pulsemesh/thread_seal.js     PMV1 device card, PME1 sealed ticket, enrolment    (§20.9)
src/pulsemesh/thread_seed.js     PMH1 seed card, dialability filter               (§20.10)
src/pulsemesh/thread_route.js    day-seed derivation, PMTC minting and verification,
                                 the term-long v2 link — **depot-side**            (§21)
src/pulsemesh/thread_eta.js      locate + fixed-order matrix arrival estimation      (§9)
src/pulsemesh/thread_contribute.js  the rules between the two channels               (§10)
src/pulsemesh/thread_discovery.js   DHT rendezvous: provide + findProviders          (§4.2, §8)
src/pulsemesh/threads.js         public entry point (rangefind/pulsemesh/threads)
scripts/pulsemesh_thread_bench.mjs  crypto, bandwidth, catch-up availability        (§15, §18)
test/pulsemesh_thread_*.test.js  per-module + §16 vectors
test/pulsemesh_sealed_ticket.test.js  RFC 7748 vectors, seal/open, enrolment      (§20.9)
test/pulsemesh_seed_bootstrap.test.js  signed + sealed bootstrap, query hints,
                                       PMH1 round trip, the QR budget           (§20.10)
test/pulsemesh_thread_route.test.js   day 1 = day 40, the holder split, the
                                      certificate bounds, the held buffer          (§21)
```

- **T1 — crypto, codecs and the link (no network).** Reproduce §16 byte
  for byte; property test that a tampered AAD, a rolled-back `seq`, and
  a correctly-sealed-but-unsigned record (the link-holder-turned-forger
  case) are all rejected. Requires `engine.locate()` (§13).
  **Done** — `thread_crypto.js`, `thread_codec.js`,
  `test/pulsemesh_thread_codec.test.js`. Every §16 vector reproduces
  byte-for-byte, including the 133-byte sealed record, through WebCrypto
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
  **Done** — `thread_cache.js`, `thread_discovery.js`,
  `test/pulsemesh_thread_catchup.test.js`,
  `test/pulsemesh_thread_discovery.test.js`. Includes live DHT
  `provide`/`findProviders` on the derived rendezvous key: a second host
  finds the publisher with nothing but the link.
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

This implementation is conformant on every row below. Boxes are ticked
only where a test in `test/pulsemesh_thread_*.test.js` asserts the
behaviour, not where the code merely looks right.

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
- [x] Discovers peers from the derived rendezvous key alone — no host,
      mailbox, or bootstrap address in the link. Tag, topic and
      rendezvous derivation plus live DHT `provide`/`findProviders` are
      implemented in `src/pulsemesh/thread_discovery.js` and tested
      end-to-end between two hosts.
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
- [x] A dispatch ticket is refused unless the issuer signature verifies,
      the epoch matches and it has not expired; a seedless ticket is
      refused as a publish capability; reserved flag bits are rejected,
      not ignored (§20).
- [x] A broadcast job offer carries a commitment and coarse descriptors
      and nothing else: a byte scan of the offer finds no stop label, no
      order reference, no instruction and no phone number that the plan
      demonstrably contains, and the published centroid sits on the
      0.01° grid and is not any stop's position (§20.4).
- [x] An offer round-trips, refuses a tampered one and a foreign
      issuer's under a pin, refuses reserved flag bits and a set flag
      with nothing behind it, keeps a stated `payMinor: 0` distinct from
      unstated, and stays 148–177 bytes whether the round has 3 stops or
      120 — a QR at version 11–12 (§20.4).
- [x] `awardMatchesOffer` accepts the ticket that fulfils the offer and
      refuses a swapped plan, a foreign issuer, a foreign epoch, a later
      `notAfter` and a rekeyed job, **naming the check that failed** —
      including against an offer whose `jobId` field the dispatcher
      forged and re-signed, which `planRef` still catches (§20.4).
- [x] A handover resumes above the previous holder's last `seq` — out of
      the audience's own caches, with the first device offline — so no
      follower sees a regression and no second link is ever issued
      (§20.5).
- [x] No outcome is ever inferred: dwell detection advances `stopIndex`
      and never writes the outcome map, and an unmarked stop the vehicle
      passed stays pending on the wire (§5.2.1).
- [x] The outcome map is cumulative in every record, marking publishes
      immediately rather than on the next cadence tick, marking an
      earlier stop late never regresses `stopIndex`, and re-marking
      overwrites.
- [x] `stopIndex` is the last stop *dealt with* in plan order, not a
      high-water mark of the marks: pre-marking a far-future stop records
      its outcome and leaves `stopIndex` where the vehicle is, so a
      follower between the two still gets a real arrival (§5.2, §9.1).
- [x] A plan too large to encode is refused at encode time with the
      number of stops that would have fitted, rather than producing a
      record over `THREAD_MAX_RECORD_BYTES` (§5.2.2).
- [x] **A dead zone costs no reasons.** A publisher marks two stops with
      reasons and photos while its transport drops every record; on
      reconnection a subscriber that heard none of it recovers both
      stops' outcomes *and* both reasons from the next ordinary record,
      which is not a retransmission of anything (§5.2.1).
- [x] Reasons are carried for every skipped or failed stop and for no
      other, reason `0` included — so a gap in the list is a **lost**
      reason and `stopReasonFor` reports `reasonKnown: false` rather than
      a cheerful zero, and `estimateArrival` passes the same distinction
      to the customer (§5.2.1).
- [x] The list is capped rather than refused: a 60-stop day where every
      stop failed keeps the newest `THREAD_MAX_REASONS`, publishes every
      record, and leaves the outcome map complete for all 60; a body with
      no headroom left drops entries oldest-first rather than
      overflowing 213 bytes (§5.2.1).
- [x] The common case pays nothing: an all-delivered day and a run with
      nothing marked encode **byte-identically** to the same body without
      fields 20–22, the §16.4 vector's 31-byte preimage included, and the
      whole §5.2.2 stop table is unmoved. The outage day costs six bytes
      (§5.2.2).
- [x] Carried reasons are inside the signature, ascend strictly by stop
      index whatever order the driver marked in, and a duplicated entry,
      a zero stop index, an over-wide reason code and a reserved bit in
      the mark-flags byte are each refused rather than skipped (§5.2).
- [x] A superseded photo commitment is lost **visibly**: the record says
      how many commitments the run has published and which skipped or
      failed stops carried one, `hasPhoto` is `null` rather than `false`
      where nothing says, and no fetch-by-stop exists that would let a
      publisher restate a commitment after the fact (§20.7.1).
- [x] Stops marked skipped or failed leave the ETA chain, and the
      subscriber's own stop, so marked, yields a result with no arrival
      time in it at all (§9.1).
- [x] Every arrival estimate states the routing profile it was computed
      under and whether that profile matched the run's travel mode
      (§9.2).
- [x] Per-stop delivery metadata round-trips byte-identically, keeps
      `parcels: 0` distinguishable from unstated, refuses each oversized
      field by naming its cap, refuses reserved stop-flag bits rather
      than skipping the stop, and never reaches the wire — a subscriber's
      records carry the 8-byte `planRef` and no trace of an order
      reference, parcel count, instruction or phone number (§20.8).
- [x] A dispatch ticket never leaves the process in the clear: it is
      sealed to enrolled devices, the sealed bytes contain none of the
      plan's text, two devices open one record and a third gets a
      distinct sealed-for-another-device error, and flipping a byte in
      the ciphertext, a wrap or the ephemeral key fails to open (§20.9).
      X25519 matches RFC 7748 §5.2 and §6.1 and agrees with WebCrypto
      where the host has it.
- [x] A proof-of-delivery photo puts a 32-byte commitment inside the
      signed record and never a byte of image on gossip; the sealed bytes
      travel only on request; the key derives from the run seed, so a
      holder of the 45-byte link cannot open one; fetched bytes are
      refused unless they hash to the commitment; an oversized photo and
      a photo on a coarse run are both refused at `markStop` (§20.7).
- [x] A bootstrap address rides **inside the signed preimage**: it
      round-trips at 1 and 3 addresses, the count, byte and
      not-a-multiaddr caps are refused by name, a set flag with nothing
      behind it and reserved bits are refused, and flipping one character
      of an address fails the issuer signature (§20.10).
- [x] The sealed ticket hides it: a byte scan of the PME1 finds neither
      the address, its host, nor its peer id — and a PMJ1 offer has no
      field for one at all, so a byte scan of a broadcast offer finds
      nothing either (§20.10).
- [x] A bootstrap hint lands in the URL **query and never the fragment**:
      the fragment of a link built with a hint is byte-identical to the
      one built without, the hint parses back, and a malformed or
      over-long hint yields fewer addresses rather than an exception
      (§20.10).
- [x] A PMH1 seed card round-trips with and without a label, refuses the
      address count, the byte caps, reserved flag bits and trailing
      bytes, and `classifyThreadArtifact` reports `kind: "seed"` so a
      host routes a location somewhere other than a capability (§20.10).
- [x] **The same 45-byte link works on day 1 and on day 40**: the link
      bytes are byte-identical, a subscriber built once on day 1 keeps
      accepting records after the day key has rotated, and it needed no
      new artifact of any kind to do it — its topic tags are still the
      root's (§21).
- [x] A day seed is deterministic in `(rootSeed, planRef, serviceDay)`,
      differs for every other day, plan and root, and is structurally not
      the root: nothing the driver is handed contains the root seed
      (§21.2).
- [x] The holder split is enforced, not documented: `createThreadPublisher`
      refuses a root seed passed alongside a certificate, refuses a day
      seed with no certificate, refuses a day seed the certificate does
      not vouch for, and adopts the **root** as the run's identity
      (§21.2).
- [x] A record signed by an uncertified key is refused with its own
      distinct reason — `awaiting-certificate`, never `bad-signature` —
      and leaves `forgeries` at zero, while a genuine forgery on a v1
      link is still reported as one (§21.5).
- [x] A certificate with an over-long window is refused **by the
      subscriber**, not merely by the issuer: one signed correctly by the
      real root but valid for a year is refused as
      `cert-window-too-long`, and the records under it are never accepted
      (§21.5).
- [x] A certificate for a different root is refused by name before the
      signature is even checked; one that claims the right root but was
      signed by another fails the signature; and one outside its window
      is refused on each edge while `THREAD_CERT_SKEW` is tolerated on
      both (§21.5).
- [x] §5.5 catch-up delivers the certificate to a late joiner with no new
      protocol id, which then releases the records it had held — in `seq`
      order, with the newest winning the ledger (§21.3).
- [x] The held-record buffer is bounded by count *and* by age: it stops
      at `THREAD_HELD_RECORDS`, a certificate arriving past
      `THREAD_MAX_AGE` releases nothing, and a held forgery is evicted
      rather than accepted (§21.5).
- [x] A v1 one-off run still works end to end, emits no certificates,
      and refuses a PMTC arriving on its thread — the artifact wins over
      the publisher (§21.4).
- [x] `publishRouteDay` publishes a service day over a real channel from
      the day seed and its certificate alone, keeps the term's link
      rather than minting one, and a follower holding only those 45 bytes
      tracks the day with nothing dropped (§21).
- [x] A **route-day ticket** round-trips issue → seal → open → verify →
      publish and lands on the **root's** topic: a subscriber built from
      the term link before the day key existed accepts every record and
      drops none, and the link the run publishes under is byte-identical
      to the one minted in September. The same day seed sealed into an
      ordinary ticket reaches that subscriber not at all — the silent
      failure the artifact exists to make unrepresentable (§21.11).
- [x] A day ticket hands over: re-sealing the inner bytes to a second
      enrolled device works, the first device can no longer open them,
      and the spare publishes on the same root topic (§21.11, §20.9).
- [x] A day ticket whose seed is not the certificate's `dayPublicKey` is
      refused **by name** (`ticket-day-seed-mismatch`) at verification and
      again by the publisher, and cannot be minted at all — the issuer
      re-derives it from the plan's `planRef` and the service day
      (§21.11, §21.2).
- [x] Only the route root grants a day of it: a certificate naming
      another root is refused at issue and at decode, and one claiming
      this root but signed by another fails as `cert-bad-signature`
      (§21.11).
- [x] An expired certificate, a window over `THREAD_MAX_CERT_SECONDS`
      signed correctly by the real root, and one beginning after the
      ticket expires are each refused with their own §21.5 code — the
      ticket is a verifier, not merely a carrier (§21.11).
- [x] `classifyThreadArtifact` reports `routeDay` and `serviceDay`, says
      `false` for an ordinary job and **null** for a sealed one, and the
      flag is read from the artifact rather than inferred (§21.11).
- [x] The measured QR cost of a day certificate is 145 bytes and four
      stops — 8 address-only stops fit a scannable QR against 12 without,
      and one more does not, asserted with `encodeQr` at `maxVersion: 25`
      (§20.8, §21.11).

## 20. Dispatch tickets (handover)

### 20.1 The run belongs to the job, not to the driver

§4.1 says the private seed never leaves the device that generated it,
and that is right whenever the publisher is also the origin of the run:
a driver sharing their own drive, a bus starting its own route. A
**dispatched** job inverts the order of events. A restaurant composes a
delivery and hands the customer a follow link at order confirmation —
before a courier has accepted it, sometimes before it has been offered
to anyone. The link is a function of the run key (§5.4), so if the key
is minted by the driver:

- the customer can be told nothing until a driver exists, which is the
  wrong half of the wait to make opaque; and
- reassigning the job invalidates every link already sent.

So a **ticket** carries the run's Ed25519 private seed, signed by the
issuer. This is a deliberate, named inversion of §4.1 and it buys three
things: links before drivers, a dispatcher that keeps cancel and
reassign (it holds the seed too), and mid-run handover (§20.5).

The cost is stated rather than hidden: a ticket is a **publish**
capability. Anyone holding it can move the vehicle. It MUST travel over
a channel at least as protected as the one already carrying job
assignments, and `notAfter` is what bounds a leak — one job, not a
shift. `mode` is the issuer's §11 decision, not the driver's: whoever
decides who gets the link decides what the link is worth.

### 20.2 PMP1 — run plan

The plan is an artifact rather than a URL because `planRef` (§5.2) is a
hash of exactly these bytes, and two hosts must agree on them to the
bit.

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMP1"` | |
| 2 | version | u8 | 1 |
| 3 | travelMode | u8 | 0 unspecified, 1 car, 2 bike, 3 foot |
| 4 | dwellSeconds | varint | |
| 5 | stopCount | varint | |
| 6 | per stop: latE7 | zigzag varint | E7 integer, never a float |
| 7 | per stop: lonE7 | zigzag varint | |
| 8 | per stop: plannedUnixSeconds | varint | 0 = none |
| 9 | per stop: labelLen ‖ label | varint ‖ bytes | ≤ 48 bytes UTF-8 |
| 10 | per stop: flags | u8 | which of fields 11–14 follow; bits 4–7 reserved, MUST be 0 |
| 11 | per stop: orderRefLen ‖ orderRef | varint ‖ bytes | iff bit0. ≤ 24 bytes UTF-8 |
| 12 | per stop: parcels | varint | iff bit1. 0..65535 |
| 13 | per stop: instructionsLen ‖ instructions | varint ‖ bytes | iff bit2. ≤ 64 bytes UTF-8 |
| 14 | per stop: contactLen ‖ contact | varint ‖ bytes | iff bit3. ≤ 24 bytes UTF-8 |

Encoding order is visit order; a decoder assigns `index = i + 1`, which
is the 1-based `stopIndex` of §5.2 and the `plan.stops[].index` the
publisher and the ETA use.

Fields 10–14 are the per-stop delivery metadata of **§20.8**, which is
where the flags byte, the caps, the sizes and the privacy posture are
argued. Present fields appear in **ascending bit order**, so a plan has
exactly one encoding — `planRef` hashes plan bytes, and two encodings of
one plan would be two jobs.

`travelMode` belongs to the plan rather than to the driver's app for the
same reason `mode` belongs to the ticket: the dispatcher who routed a
bike round and priced it as one has already decided how it is made, and
the follower's estimate is only honest if it knows (§9.2). Being in the
plan puts it inside `planRef`, so it is part of the job's identity — the
same stops by bike and by van are two different jobs, and a courier who
changes it produces a visibly different `planRef` rather than a silent
substitution. The publisher copies it into every body's field 5.

A plan that leaves the field 0 has said *nothing*, which is not the same
as having said car. A host that knows which vehicle it is running on may
supply its own as a fallback — `publishTicket(ticket, { travelMode })` —
and it is used only when the plan is silent. It fills field 5 without
touching the plan bytes, so `planRef` is unchanged and the dispatcher's
answer still wins wherever there is one.

```
planRef = SHA-256(utf8("pulsemesh/plan/1") ‖ planBytes)[0..8]
```

That ref is the binding a follower can check: it holds the plan, the run
publishes the ref in every body, and a courier who swaps the plan
produces a run for a visibly different job rather than a silent
substitution.

Encoding order is visit order, so a plan SHOULD be minted in *optimized*
visit order rather than in the order a dispatcher typed the addresses:
`engine.itinerary({ stops, openEnd: true })` is the intended producer,
and its `order` — minus stop 0, which is the yard the vehicle leaves and
not a delivery — is the sequence the stops go into the plan in. That
choice has an honest cost at the other end. A customer is sent only
their own stop (§20.1: handing them the run hands them every other
customer's address), so the arrival they compute is a direct
vehicle-to-stop estimate, and every delivery scheduled ahead of theirs is
missing from it. The number is therefore a *lower bound*, not an ETA.
Full fidelity would need the prefix of the plan the vehicle still has to
drive, which is exactly the part being withheld; a dispatcher who wants
the customer to see a truthful arrival has to publish it themselves,
out of band.

### 20.3 PMK1 — the ticket

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMK1"` | |
| 2 | version | u8 | 1 |
| 3 | flags | u8 | bit0 = run seed present, bit1 = bootstrap addresses (§20.10), bit2 = day certificate (§21.11); bits 3–7 reserved, MUST be 0. A seedless PMK1 is **not** an offer (§20.4) |
| 4 | mode | u8 | 1 coarse, 2 fine — the issuer's §11 decision |
| 5 | epochPrefix8 | bytes(8) | |
| 6 | notAfter | uint32be | absolute expiry, unix seconds |
| 7 | issuerPublicKey | bytes(32) | Ed25519, the dispatcher's identity |
| 8 | planLen ‖ planBytes | varint ‖ bytes | §20.2 |
| 9 | privateSeed | bytes(32) | iff bit0. The **day** seed when bit2 is set (§21.11) |
| 10 | count ‖ per address: len ‖ bytes | varint ‖ varint ‖ bytes | iff bit1. 1–3 multiaddrs, each ≤96 bytes (§20.10) |
| 11 | dayCertificate | bytes(145) | iff bit2. A PMTC (§21.3), fixed width, no length prefix |
| 12 | signature | bytes(64) | Ed25519 over `utf8("pulsemesh/ticket/1")` ‖ fields 1–11 |

Optional fields appear in **ascending flag-bit order**, as §20.8's stop
metadata does, so a ticket has exactly one encoding.

Reserved flag bits are rejected rather than ignored: forward
compatibility on this channel is the version byte's job, and a reader
that skips a bit it does not understand is a reader that can have a
capability silently widened underneath it.

The preimage covers flags, mode, epoch, expiry, plan, seed, addresses
**and** certificate, so nothing in a ticket is malleable. A decoder MUST
reject trailing bytes, as everywhere else (§5).

A ticket and a 45-byte follow link (§5.4) arrive by the same carrier — a
URL fragment — so every host has to tell them apart before doing
anything with either. **That decision is made by the magic, never by
whether the artifact decodes.** A host that tries the ticket decoder,
swallows its error and surfaces the link decoder's instead tells a
driver holding a ticket from before a wire change that "a thread link is
45 bytes" — which is true of nothing they are holding.
`classifyThreadArtifact`
returns `{ kind, reason }`: `kind` from the leading magic — `PMK1` a
ticket, `PME1` a sealed one, `PMJ1` a job offer (§20.4) — or from the
link's fixed 45-byte width, and `reason` a sentence to show when that
known kind cannot be read: for a ticket, that the dispatcher should issue
a fresh one. An offer is its **own** kind and not a ticket missing
things, because a host that routed one to the driver's accept screen
would offer to publish a run that does not exist.

### 20.4 PMJ1 — the job offer

An offer is the one artifact on this channel that is **public and
unsealed by design**. Everything else here narrows: a ticket goes to one
enrolled device and is therefore always sealed (§20.9), a follow link
goes to the customers who were given it. An offer goes to *strangers* —
that is its entire job — so there is nobody to seal it to, and its safety
has to come from its contents rather than from a key.

Which is why it is **not** a seedless ticket. A ticket minus its seed is
the whole plan minus one field: every stop coordinate, every address
label, and since §20.8 every order reference, instruction and customer
phone number. Broadcasting that to a pool of bidders hands the day's
customer book to everyone who does not win — a marketplace built on it
would disclose more by losing bids than by running deliveries. So an
offer is a separate record carrying a **commitment** plus coarse
descriptors, and nothing else.

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMJ1"` | |
| 2 | version | u8 | 1 |
| 3 | mode | u8 | 1 coarse, 2 fine — what the winner would publish (§11) |
| 4 | travelMode | u8 | from the plan (§20.2) |
| 5 | epochPrefix8 | bytes(8) | graph binding, as the ticket's is (§20.6) |
| 6 | notAfter | uint32be | absolute expiry, unix seconds |
| 7 | jobId | bytes(16) | the join key |
| 8 | planRef | bytes(8) | **the commitment**: SHA-256 over the plan bytes |
| 9 | stopCount | varint | how many drops |
| 10 | centroidLat | zigzag varint | units of 0.01°, see below |
| 11 | centroidLon | zigzag varint | units of 0.01° |
| 12 | spread | u8 | bucket 0–4, see below |
| 13 | totalMeters | varint | the size of the work |
| 14 | flags | u8 | bit0 pay, bit1 label; other bits reserved, MUST be 0 |
| 15 | payMinor ‖ currency | varint ‖ bytes(3) | present iff bit0; ISO 4217, uppercase |
| 16 | labelLen ‖ label | varint ‖ bytes | present iff bit1; ≤ 48 bytes, public text |
| 17 | issuerPublicKey | bytes(32) | Ed25519, the dispatcher's identity |
| 18 | signature | bytes(64) | Ed25519 over `utf8("pulsemesh/offer/1")` ‖ fields 1–17 |

Reserved flag bits are rejected rather than ignored, as PMK1's and
PMP1's are: an offer whose writer meant a term this reader cannot see is
an offer whose terms this reader does not know, and bidding on it blind
is worse than not bidding. A set flag with a zero-length field behind it
is refused for the same reason §20.8 refuses one — it is a second
encoding of one offer, and the signature is over these bytes. Trailing
bytes are refused as everywhere else (§5).

**What a bidder learns, and what they do not.**

| Learns | Does not learn |
| --- | --- |
| how many stops | where any stop is |
| roughly where the round sits — a 0.01° grid cell (~1.1 km) | any street, any address label |
| roughly how far it ranges — one of five buckets | the exact extent, or any stop's bearing |
| the total distance, exactly | the route, or the order of the drops |
| the travel mode and the §11 granularity | any order reference, parcel count or instruction |
| the pay and currency, when the dispatcher states them | any customer's phone number |
| a short public label the dispatcher wrote | anything the label does not say |
| who is offering (issuer key), until when, which graph epoch | the run seed — there is no run yet, and no follow link |

**Why the coarse locality exists at all, and why it is a grid.** A
marketplace cannot publish nothing here. "12 parcels, somewhere" is not
an offer, it is a lottery ticket: no courier can decide whether to bid
without knowing whether the round is in their city, and a channel that
refuses to say gets replaced within a week by a group chat that says
everything. So the offer must name a place, and "roughly" has to be a
number rather than a promise. Three choices make it one:

- **The centroid, not the first stop.** An origin is somebody's doorstep
  — or the depot, which then leaks the first customer by adjacency. The
  centroid of a round is usually a point with nothing at it.
- **A 0.01° grid.** About 1.1 km north-south, and 1.1 km × cos(latitude)
  east-west, so roughly 0.8 km at 45°. One cell holds a few thousand
  addresses in a city: the courier learns which part of town, and no
  arithmetic gets them from there to a door. The published centroid is a
  multiple of the grid, so there is no "precise" reading to reach for.
- **A bucketed spread, not a radius.** An exact centroid plus an exact
  radius is a disc, and two or three offers from one depot trilaterate
  it. The five buckets are ≤ 1 km, ≤ 3 km, ≤ 10 km, ≤ 30 km and beyond,
  measured from the centroid to the furthest stop — one block, a
  neighbourhood, across town, the metro, out of town.

`totalMeters` is exact, because the size of the work is not a location:
it is what a courier prices against, and a 41 km round tells you nothing
about where the 41 km are. An offer is a fixed-size record — it holds no
plan — so a 120-drop day advertises in exactly the same bytes a 3-drop
one does: **148 bytes bare, 177 with pay and a 23-byte label**, a QR at
version 11–12 as a `wayfind://offer#…` URL, comfortably inside the
version-25 scannability ceiling (§20.5) that a real ticket blows past.

**The commitment, and what it is for.** `planRef` is the same 8-byte hash
every PMT1 body carries (§5.2), and `jobId` is the same 16 bytes both
artifacts share:

```
jobId = SHA-256(utf8("pulsemesh/job/1") ‖ epochPrefix8 ‖ uint32be(notAfter)
                ‖ issuerPublicKey ‖ planBytes)[0..16]
```

`jobId` hashes neither the seed nor the signature, precisely so that an
offer and the ticket that fulfils it are **the same job** in both
directions: offer first and award later, or mint the ticket and
advertise it.

Without a commitment an offer is a marketing line. A dispatcher
advertises "3 stops downtown, 8 km", the whole pool bids on that, and the
winner opens a ticket with thirty drops across the province in it — by
which point they have accepted, the customers have been told, and the
argument is theirs to lose. `awardMatchesOffer(offer, ticket)` closes
that on the courier's device before a wheel turns: it recomputes
`planRef` from the awarded plan's bytes and checks it against the offer,
then checks the issuer, the epoch, that the award does not run later than
was advertised, and finally `jobId`. It returns `{ ok, reason }` in
`verifyThreadTicket`'s style, **naming the check that failed** — a
courier told "planRef" knows the plan was swapped, where "this job does
not match" tells them nothing they can argue with. The check is on
content, not signatures (run `verifyJobOffer` and `verifyThreadTicket`
for those), so it holds against a dispatcher who re-signs its own
forgery — which is the actual threat, since the dispatcher owns the key.

**Bidding is out of scope.** There is no claim record, no bid message and
no transport for one in protocol v1. A courier who wants the job sends
the dispatcher their **PMV1 device card** (§20.9) by whatever channel the
two already use — the same card enrolment needs anyway — and the
dispatcher, if it picks them, seals the ticket to it. That is the whole
loop, and it is deliberately not a market: claims, awards, payments and
reputation all need an accountable party, and this protocol's posture is
that no such party exists on the wire (the same concession §4.3 makes for
credentials). A marketplace that wants them builds them where
accountability already lives — its own issuer — and uses `jobId` to tie
them back to the run. The mesh carries the job, not the money.

### 20.5 Mid-run handover

A courier's bike breaks. The dispatcher hands the *same* ticket to
someone else, and the customer's link must keep working.

`publishTicket` verifies the ticket against the channel's epoch, then
runs one round of §5.5 catch-up against the peers it already has to
learn the highest `seq` anyone has seen on this thread, and starts its
publisher at that number. The next record is `highestSeq + 1`, which is
strictly increasing, so §7 step 7 accepts it and no follower sees a
regression or a gap. Zero peers is not a failure — a first assignment
resumes at 0. Note what answers that catch-up: the **audience's** own
caches (§8), so the replacement courier resumes from the customers
watching the delivery, with no server involved and the original device
already offline.

Handover assumes the previous holder has stopped. Both devices hold the
same seed, so interleaved publishing is not corruption — every follower
still enforces strictly increasing `seq` and simply drops the loser of
each race — but it is nonsense, and stopping the old device is the
dispatcher's job, out of band. The protocol cannot enforce it: two
holders of one seed are indistinguishable by construction.

**Carriers.** The artifact is the same signed bytes whatever moves them
between the two devices. A QR code is the right carrier when the two
screens are in the same room, and the implementations cap it at about
version 25 — a *scannability* policy, not a QR limit: past that the
modules are finer than a hand-held camera resolves, and a technically
valid symbol nobody can scan is worse than none, because the driver
tries, fails and is told nothing. A full delivery day outgrows that
bound long before it outgrows the protocol, so a ticket also travels as
a **`.wayfindjob` file**: one line of text, the driver URL
(`wayfind://ticket#<base64url>`), named `job-<jobId first 12>.wayfindjob`.
Text rather than binary, so it survives mail clients, messengers and
copy-paste, and so a human who opens it sees a tappable link. Readers
MUST strip all whitespace from the fragment before decoding, because
that is what a hard-wrapping mail client does to base64url. Nothing
about the trust model changes with the channel: a file is a **publish
capability** exactly as the QR is, and travels under the same rule as
the ticket — a channel at least as protected as the one already carrying
job assignments (§20.1).

### 20.6 Epoch binding

A ticket dies with its graph epoch, exactly as a link does (§5.4): the
`epochPrefix8` inside it is signed, so it cannot be re-pointed at a new
index without reissuing. Keep `notAfter` well inside the index's
republish cadence. A ticket issued for a four-hour window against an
epoch that rolls in one is a job that becomes unpublishable halfway
through — the same failure §18 flags for links, arriving earlier
because a ticket is composed before the run rather than at its start.

`notAfter` is also what bounds the **run**. A ticket-born publisher runs
until the ticket expires, clamped to 24 hours — the clamp is there
because `notAfter` is a uint32 a dispatcher can fat-finger into next
year, and no run should outlive a shift by that much.
`THREAD_MAX_RUN_SECONDS` (§6, 6 h) now bounds only **self-started** runs:
someone shares their own drive and may well forget to stop it, and a
link that outlives the reason it was shared is the harm that constant
exists for. A dispatched delivery day is eight to ten hours and carries a
bound its dispatcher set deliberately; holding it to six would stop the
run mid-afternoon, mid-route, with customers still watching.

### 20.7 Proof-of-delivery photos

A courier photographs the parcel on the doorstep. The dispatcher wants
that photo against the job. Nothing about that requires putting an image
on a gossip mesh, and putting one there would end the channel.

**The rule: the record carries a commitment, the bytes travel
separately.** A mark's field 19 is `SHA-256` of the *sealed* blob, 32
bytes, inside the signed preimage (§5.2). The blob itself is fetched on
demand over `/rangefind/pulsemesh/1/photo`, and only by someone who
asks. That keeps every property the 256-byte record buys: flood
resistance rests on a fixed record size, `THREAD_CACHE_RING` caches 240
records per tag on devices that may be phones, and a relay forwards
records it cannot read. A 100 KB image inside any of those is not a
tuning problem, it is a different protocol. Bandwidth on the gossip path
stays flat whether a run attaches a photo to every stop or to none.

The commitment being *signed* is what makes it evidence. The driver is
bound to one specific set of bytes at the moment of the mark; a photo
substituted afterwards fails the hash, and a photo removed afterwards
fails the signature.

#### 20.7.1 A commitment published into a dead zone is lost, and says so

The record carries **one** commitment, for the most recent mark, and the
next mark replaces it. A driver who photographs stop 3 and then marks
stop 4 before regaining coverage has published stop 3's commitment to
nobody, and it is gone: the photo is still on the device, and no longer
addressable by anyone who wants it, because the thing that addressed it
was the commitment.

§5.2.1 fixes the *reason* half of that by carrying reasons cumulatively.
The commitment half cannot be fixed the same way, and the honest
statement of why is worth more than a mechanism that pretends otherwise.

**Carrying the most recent K commitments.** A commitment is 32 bytes.
§5.2.2 leaves a 200-stop day 25 bytes of headroom *with* the first one,
so K = 2 does not fit the day the feature exists for, let alone K = 4.
Made budget-adaptive it collapses back to K = 1 on exactly the days it
was supposed to help. Rejected: it buys nothing where it is needed and
costs 32 bytes where it is not.

**Making a photo addressable by `(planRef, stopIndex)` in the fetch
protocol.** Tempting — a dispatcher could then ask for "the photo for
stop 3" without holding its commitment — and rejected, because it
quietly trades away the property this whole section exists for. The
commitment is evidence *because a signed record committed to it before
it was fetched*. A fetch keyed on the stop has to return something the
requester can check, so the publisher would have to state the hash in
the response — and the publisher is the party who might lie. That is not
a verification, it is the publisher marking its own homework: the driver
could photograph a doorstep at 18:00 and serve it as stop 3's proof, and
the dispatcher would have no way to tell. A *signed* re-statement is no
better; it is still a statement made after the fact, and the temporal
binding — committed at the door, inside a record whose `unixSeconds` and
`seq` place it there — is the entire value in a dispute. **Convenience
is not worth the evidence property**, and a fetch-by-stop that returned
un-verifiable bytes would be worse than no fetch, because a dispatcher
would believe it.

**What is implemented instead: the loss is made countable and visible.**

- Field 22 carries **how many distinct commitments the run has
  published**. A subscriber that holds fewer distinct hashes than that
  knows exactly how many proofs it will never be able to fetch. One byte
  for any realistic day, and it is written only by a run that has taken
  a photo at all.
- Bit 7 of each carried reason byte (§5.2.1) says **that mark had a
  photo**, so a skipped or failed stop whose commitment was superseded
  is identifiable by name, not merely by a count.
- Nothing reports a missing proof as an absent one. `stopReasonFor`
  returns `hasPhoto: null` where nothing it holds says, never `false`.

So the residue is stated rather than hidden: **the commitment for a mark
that was superseded before its record reached anyone is unrecoverable,
and the driver's device should re-mark the stop.** Re-marking overwrites
(§5.2.1), re-seals under the same per-stop key and publishes a fresh
commitment, and the sealed blob it names is still on the device — the
publisher evicts nothing for the life of the run. That is a real
operational answer, and it is one the dispatcher can now know to ask
for, which before this it could not.

A UI that shows a delivery board SHOULD surface the shortfall rather
than a clean-looking page: "2 of 5 proofs unavailable" is the true
statement, and "no photo" is not.

**Wire.** Two records, magic-discriminated as everywhere else. `PMTF`
and `PMTB` rather than a `PMx1` pair, because the traffic channel
already owns `PMF1` (cell fetch) and `PMG1` (digest) and every `PMx1`
slot the thread channel reserves is taken; these follow `PMTP`'s
precedent instead — a `PMT` prefix a traffic-only implementation already
ignores (§1), with a fourth letter naming the record.

- **PMTF** (request): `"PMTF"` ‖ commitment bytes(32). No `epochPrefix8`,
  unlike PMR1: a tag only means something inside an epoch, while a
  SHA-256 of ciphertext is globally unique on its own, so an epoch prefix
  here would be a fingerprint the responder cannot use.
- **PMTB** (response): `"PMTB"` ‖ length varint ‖ sealed bytes. **A zero
  length means "not held"**, and is also the answer to a hash this peer
  has never seen — the PMM1 rule again: a responder MUST NOT let a prober
  distinguish "I am not publishing that run" from "no such photo".

**Sealing.** AES-256-GCM under a per-stop key, with a fresh CSPRNG
12-byte IV **prepended** to the ciphertext. This deliberately breaks the
§5.1 convention where the nonce is derived from `seq` and never
transmitted, and it has to: a photo has no sequence number and is
addressed by content hash, so there is nothing to derive a nonce from.
There is no AAD for the same reason — the binding a record needs comes
from the signed commitment, not from the blob's framing.

```
K_photo = HKDF-SHA256(ikm = privateSeed, salt = "",
                      info = utf8("pulsemesh/photo/1") ‖ planRef(8) ‖ uint32be(stopIndex))
```

**Photos are dispatcher-only in v1, and that falls out of §20.1 rather
than being a policy.** The key comes from the run's **private seed**,
not from the capability `P`. Two parties hold that seed: the driver, who
was handed it in the ticket, and the dispatcher, who kept it when
minting the ticket — the named inversion of §4.1. A customer holds the
45-byte link (§5.4), which is `P`, an epoch prefix and an expiry. The
seed is not in it, is not derivable from it, and never appears on the
wire. **A customer therefore cannot open a delivery photo, whatever they
do with the bytes.** A UI shown to a link-only holder should say a photo
is attached — the commitment is public and honest — and must not attempt
the fetch. Giving customers their own photos is a real product question
and a v2 one: it needs a second wrapping to a subscriber-held key, and
that is a key-distribution problem this protocol does not currently
have.

`planRef` and `stopIndex` are in the info string, so one leaked photo
key opens one stop of one job.

**EXIF and recompression are the HOST's obligation, and this is a MUST.**
`markStop` takes already-compressed image bytes and does not decode,
resize, rotate or strip anything — it has no image decoder and should
not acquire one. A phone camera's JPEG carries EXIF, and EXIF carries a
GPS fix: an unprocessed camera file attached to a run publishes the exact
coordinates of a customer's front door, out of band, past every
granularity control in §11. A host MUST re-encode before calling — a
canvas draw-and-`toBlob` does the downscale and drops all metadata in
one step, which is what the demo does — and MUST NOT pass a camera file
through.

**Size.** A sealed blob is capped at `THREAD_MAX_PHOTO_BYTES` (128 KiB),
checked on the plaintext plus the seal's 28 bytes of overhead so an
oversized photo is refused before any crypto runs. That is a generous
1024 px JPEG and a deliberate ceiling on one transfer: this path has no
chunking, no resume and no backpressure, and it should not grow them.

**Availability = the driver is online.** Relays do **not** cache photos.
A blob is three orders of magnitude over the thread cache's admission
caps and spending them on it would evict the records catch-up exists to
serve, so only the peer publishing the run ever answers a PMTF. The
consequence is stated rather than hidden: a dispatcher fetches photos as
the marks arrive, while both ends are on the mesh, and a photo whose
driver has gone home is gone. A dispatcher that needs them kept keeps
them — this channel gets them across, it is not an archive. The publisher
holds every photo of a run for the run's lifetime and evicts none: a run
is a few hundred photos of ~100 KB, and dropping one deletes the only
copy of evidence the run has already committed to.

**Coarse runs.** §11 coarse means the operator decided this audience is
not entitled to the vehicle's position. A doorstep photo hands over a
position with a doormat and a house number on it. Hosts SHOULD warn or
refuse, and this implementation refuses: `markStop` takes
`allowCoarsePhoto`, defaulting to **false**, and throws when a coarse run
is handed a photo. Opting in is possible — a dispatcher holds both the
seed and the plan, so it already knows where the stop is, and it is the
only party that can open the photo anyway — but it has to be said out
loud rather than happening because nobody thought about it.

**API.**

```js
await run.markStop(3, STOP_OUTCOME.DELIVERED, { photo });   // photo: Uint8Array, already compressed
// …on the dispatcher, holding the ticket's seed:
const sealed = await follow.fetchPhoto(update.lastOutcome.photoHash);
const bytes = await openPhoto(sealed, { privateSeed, planRef, stopIndex, hash });
```

`fetchPhoto` verifies the commitment before returning and skips a peer
that answers with anything else; `openPhoto` checks it again, first,
before importing a key, and throws rather than returning null — a caller
rendering a proof of delivery must not be able to ignore the failure by
accident.

### 20.8 Per-stop delivery metadata

A label and a coordinate are not enough to make a delivery. A driver at
a door needs the order reference to check against the box, how many
parcels the stop is, what to do when nobody answers, and a number to
ring. So a plan stop carries four optional fields (§20.2 fields 10–14).

| Bit | Field | Encoding | Cap |
| --- | --- | --- | --- |
| 0 | `orderRef` | varint len ‖ UTF-8 | 24 bytes |
| 1 | `parcels` | varint | 0..65535 |
| 2 | `instructions` | varint len ‖ UTF-8 | 64 bytes |
| 3 | `contact` | varint len ‖ UTF-8 | 24 bytes |
| 4–7 | reserved | — | MUST be 0 |

**Why a flags byte and not four always-present fields.** Two reasons,
and the second is the one that matters.

The cheap reason: a flags byte costs **1 byte** on the ordinary
address-only stop, against 4 for four zero-length fields. Most stops on
most days are an address and nothing else, and that case is what a
100-drop plan's size is decided by.

The real reason: a zero-length field cannot distinguish **"0 parcels,
stated"** from **"parcel count unspecified"**. Those are different
claims. A dispatcher who wrote 0 has told the driver something — nothing
to carry, so this is a collection, a survey, a signature, a
failed-delivery retry. A dispatcher who left the box empty has told them
nothing. A UI that renders the second as the first invents a claim
nobody made, and the driver acts on it. So the flag is what carries
presence, and a decoded stop exposes `parcels: number | null`.

**Null means unstated, for all four.** The three text fields decode to
`string | null` on the same rule — there is no `""`, because an empty
order reference is not a claim either, and one rule across four fields
is one thing to remember instead of two. The encoder skips a field that
is `null`, `undefined` or `""`, and the decoder **refuses** a set flag
bit with a zero-length value: that would be a second encoding of the
same plan, and `planRef` hashes plan bytes. Consequently
`encode(decode(x)) === x` byte-for-byte, with `parcels: 0` surviving as
`0` and an absent field surviving as absent.

**Reserved bits 4–7 are refused, not skipped.** Same rule as PMK1's
flags (§20.3), for a sharper reason: a stop whose writer set a bit this
reader does not know is a stop this reader cannot fully read, and
quietly dropping the unknown part hands a driver a door with the
instruction that mattered removed. The whole plan is refused.

**Sizes, measured against the encoder.** The reference stop is an
ordinary suburban drop: a 20-byte label, a planned time, and
planet-scale coordinates (7 significant digits, negative longitude).

| Stop carries | Metadata bytes | Whole stop | Stops in a scannable QR |
| --- | --- | --- | --- |
| nothing set | 1 | 37 | 12 |
| order `4471`, 2 parcels | 7 | 43 | 10 |
| all four at their caps | 119 | 155 | 2 |

The QR column is the existing §20.5 scannability policy — `maxVersion:
25` at EC level `M`, carrying `wayfind://ticket#<base64url>` — which
works out to a hard ceiling of **735 bytes of carrier payload** (980
payload characters, less the 17-character scheme prefix, at 3 bytes per
4 base64url characters). Since §20.9 what that payload carries is a
**sealed** ticket, and the seal costs 130 bytes for one recipient, so
the ceiling on the signed ticket inside is **605 bytes** — 541 with the
issuer as a second recipient, which is two stops fewer again in the
typical case. The encoder is the authority for these numbers and the
tests assert them; nothing here is arithmetic done by hand.

Metadata therefore costs two stops off a scannable round in the typical
case, and takes a maximal one down to two. Since §20.10 a ticket may also
carry the fleet's seed address, which costs 84 bytes and two to three
stops more; that table is in §20.10 and is measured the same way. Since
§21.11 it may instead carry a **day certificate**, which is a fixed 145
bytes and costs **four stops**:

| Ticket carries | Signed-ticket budget | Address-only stops in a scannable QR |
| --- | --- | --- |
| a plan (§20.8) | 605 | 12 |
| a plan + a day certificate (§21.11) | 460 | 8 |

**A day that exceeds the QR
ceiling travels as the `.wayfindjob` file** (§20.5) — the identical
sealed bytes, no protocol difference, and the only carrier once a plan
outgrows a camera. A hundred-drop day never fit a QR and still does not.

**This is private by construction, not by policy.** A plan is
**driver-side only**. It travels inside the ticket from the dispatcher to
the one device awarded the job, and it stops there: `thread_publish.js`
reads the stops for dwell detection (§10 rule 4) and to size the outcome
map, and the PMTP body it seals carries the 8-byte `planRef` **hash** of
the plan bytes — never a coordinate, never a label, and so never an
order reference, a parcel count, an instruction or a phone number. There
is no field on the wire for any of it. A follower holding the 45-byte
link, and a relay forwarding records it cannot read, are structurally
incapable of seeing this data; no configuration turns it on, and no bug
in a UI leaks it, because the bytes are not in the record.

**What does change is what a leaked ticket is worth.** Adding metadata
does not widen what a ticket can *do* — it was already a publish
capability, and §20.1 already says it MUST travel over a channel at
least as protected as the one carrying job assignments. It raises what a
ticket is worth to *read*:

- `orderRef` and `parcels` are **commercially sensitive**. A competitor
  holding a day's tickets has order volumes, drop densities and the
  reference scheme.
- `contact` is **personal data**, and that is a different category of
  harm from everything else in this protocol. A leaked or photographed
  ticket is now a list of customers' names-by-address and phone numbers.
  Data-protection obligations follow it: purpose limitation, retention,
  and the fact that a `.wayfindjob` file sent over a messenger is a
  disclosure of personal data to whatever holds that messenger.

**This is what §20.9 answers.** A QR on a counter is *photographable by
anyone else in the room*, which was a tolerable risk for a route sheet
and is not one for a customer list — so a ticket is no longer carried in
the clear at all: what goes in the QR, in the file, and over any channel
is the sealed PME1 envelope, and a photograph of it is ciphertext. Two
residual obligations survive sealing and hosts SHOULD take them
seriously. `notAfter` bounds the *capability* but not the *data*: an
expired ticket cannot publish and, once opened, can still be read, so a
device that keeps opened tickets is keeping personal data and should
stop. And a dispatcher SHOULD put in `contact` only what the driver
actually needs — 24 bytes is a phone number, deliberately not a field
anyone can fit a customer note into.

Hosts that do not want the exposure simply leave the fields unset: the
flags byte makes that the cheapest encoding there is, and a plan with no
metadata is byte-identical to what a pre-§20.8 dispatcher would have
produced apart from one zero byte per stop.

### 20.9 Sealed tickets and device enrolment

**A dispatch ticket is encrypted at all times, including while it is
being handed from one driver to the next.** There is no plaintext ticket
form for a host to pass around: `issueTicket` mints the signed PMK1
artifact and that artifact never leaves the process on its own.
`issueSealedTicket` is the entry point, and what comes out is PME1 —
ciphertext addressed to devices the dispatcher has already enrolled.

Two separate failures made this necessary, and the first is the serious
one.

- A ticket carries the run's **private seed**, so it is a publish
  capability, and §20.5 says plainly that the protocol cannot tell two
  seed-holders apart. A photographed QR on a counter was therefore a
  stranger who could move the vehicle, and nothing downstream could
  refuse them: they hold the same seed the driver does. Sealing turns
  that photograph into ciphertext. **This is a security fix, not only a
  privacy one.**
- Since §20.8 a ticket also carries order references, delivery
  instructions and **customer phone numbers**. A leaked ticket was a
  customer list.

**Enrolment is the gate on transfer.** A device can receive a job only
if the sender already holds its public key. That is a real operational
cost — the replacement courier has to have been enrolled *before* the
first one's bike breaks — and it is the entire point: it replaces
"whoever holds these bytes" with "one device the dispatcher named".
There is no way to have the second property while keeping the first, and
no flag that turns it off.

#### PMV1 — the device card

The artifact one device shows and another scans or pastes to enrol it.

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMV1"` | `V` for de*v*ice; `D` is the protocol channel's PMD1 |
| 2 | version | u8 | 1 |
| 3 | publicKey | bytes(32) | X25519, the device's long-lived enrolment key |
| 4 | nameLen ‖ name | varint ‖ UTF-8 | ≤ 32 bytes, for a human to recognise |

It travels as `wayfind://device#<base64url>`, the same carrier
convention as a follow link and a job (§5.4, §20.5): base64url in the
**fragment**, which browsers never transmit and servers never log.
Trailing bytes are rejected, as everywhere else (§5).

`fingerprint(publicKey)` is the first 4 bytes of `SHA-256(publicKey)` as
8 hex characters. **Compare it aloud before enrolling.** A card is bytes
on a screen and a screen in a depot can be showing anybody's; enrolment
over a QR is only as good as the enroller's ability to tell they scanned
the right one. Four bytes is not a cryptographic binding and is not
claimed to be — it is eight characters two people can read to each other
in three seconds, which is the comparison that actually happens in a
doorway.

The device key is X25519 and is **not** any of the other keys in this
protocol: not the run seed (per-job, and it travels), not the issuer
seed (which signs). It never leaves the device and never signs anything.
Its only job is to be the address a ticket can be sealed to.

#### PME1 — the sealed ticket

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PME1"` | `E` for *e*nvelope |
| 2 | version | u8 | 1 |
| 3 | ephemeralPublicKey | bytes(32) | X25519, fresh per sealing |
| 4 | recipientCount | varint | 1..16 |
| 5 | per recipient | bytes(4) ‖ bytes(12) ‖ bytes(48) | hint ‖ nonce ‖ wrapped content key + tag |
| 6 | bodyNonce | bytes(12) | |
| 7 | body | bytes | AES-256-GCM of the whole signed PMK1 ticket, tag appended |

```
contentKey  = CSPRNG(32)                        -- one per sealing
wrapKey_i   = HKDF-SHA256(X25519(ephemeralPriv, recipientPub_i),
                          info = utf8("pulsemesh/seal/1") ‖ recipientPub_i)[0..32]
wrap_i      = AES-256-GCM(wrapKey_i, nonce_i, aad, contentKey)
body        = AES-256-GCM(contentKey, bodyNonce, aad, PMK1 ticket bytes)
aad         = "PME1" ‖ version ‖ ephemeralPublicKey
hint_i      = SHA-256(recipientPub_i)[0..4]
```

**The content key is the design, not an implementation detail.** The
ticket is encrypted **once**; each recipient gets that key wrapped under
a key agreed with them. Sealing to the driver *and* the issuer therefore
costs one extra 64-byte wrap rather than a second 600-byte copy — which
is what makes multi-recipient affordable at all, because the QR budget
(§20.5) is the binding constraint on how many stops a round can carry.
v1 callers seal to the awarded device and, when the issuer wants to keep
the ability to read back what it dispatched, to themselves. That second
recipient is an explicit argument, never a default: a dispatcher that
does not add itself has minted a ticket it can no longer open, which is
sometimes exactly right and is never a choice the library makes for it.

**The AAD binds recipients to the message.** Every wrap and the body are
authenticated under `magic ‖ version ‖ ephemeralPublicKey`, and every
wrap key derives from an agreement with *that* ephemeral key. A wrap
lifted out of one sealed ticket and pasted into another fails its tag
check instead of handing the second ticket's content key to the first
one's recipient. Recipients cannot be swapped between messages, and a
body cannot be moved under a different recipient set.

**A hint is a convenience, never an access control.** It exists so a
device does not have to run a key agreement and an AEAD open against
every wrap — on the pure-JS X25519 path that is a visible pause on a
phone. It proves nothing: anyone can compute the hint for a public key
they have seen. A device must still authenticate by **successfully
opening**, which is why `openSealedTicket` falls back to trying the
unhinted wraps rather than concluding from the hints that a job is not
for it.

**Zero trailing bytes, enforced by the AEAD.** The body has no length
prefix: an AEAD ciphertext is exactly as long as it is, so anything
appended fails the tag check. That is the §5 rule, arrived at by the
same route the sealed record takes.

#### The API, and the no-plaintext rule

```js
const device = await generateDeviceKeypair();               // on the driver's phone
const card   = deviceCardUrl({ publicKey: device.publicKey, name: "Léa — Pixel 8" });
// …the dispatcher scans that card, compares the fingerprint aloud, and:
const job = await issueSealedTicket({
  issuerSeed, epoch32, plan, notAfter,
  recipients: [driverPublicKey, issuerPublicKey]            // enrolled devices
});
// job.driverUrl is wayfind://ticket#<base64url> — ciphertext.
// …on the phone:
const run = await threads.publishTicket(job.sealed, { devicePrivateKey: device.privateKey });
```

`openSealedTicket` throws two distinguishable errors, on `error.code`.
`sealed-for-another-device` means the record parses and none of its wraps
opens with this key — an ordinary situation with an action attached: get
enrolled, ask for a fresh seal. `unreadable` means the bytes are not a
sealed ticket this version can parse. A host must be able to tell a
driver which happened, because "ask the dispatcher to re-send" fixes one
and nothing at all about the other.

`classifyThreadArtifact` returns `{ kind: "ticket", sealed: true }` for
a PME1 blob, with the sealed-for-a-device sentence as its `reason`. A
host with no device key gets the honest answer — *this is a job, it is
sealed to a device, enrol this one if it is not that device* — and never
"this is not a capability", which was the wrong sentence and sent the
driver to the wrong screen. It also reports `routeDay` and `serviceDay`
(§21.11), and on a sealed blob `routeDay` is **null** rather than false:
which sort of job is inside is ciphertext, and answering "an ordinary
one" would be a claim nobody made.

`publishTicket` takes the sealed bytes and the device private key,
opens, and proceeds exactly as before: **the seal is confidentiality and
addressing, and the issuer's signature is still the only thing that says
the job is real**. Sealing does not replace verification and the sealer
need not be the issuer, so `verifyThreadTicket` runs on what comes out,
unchanged. An already-decoded inner ticket is still accepted for
internal callers and tests; a host that reaches for that path is passing
plaintext tickets around, which is the thing this section exists to
stop.

#### X25519, and the fallback's timing properties

`src/pulsemesh/x25519.js` is a dependency-free RFC 7748 implementation,
the sibling of `ed25519.js` and reached more often: WebCrypto X25519 is
*newer* than WebCrypto Ed25519 on every engine that has either (Chrome
133, Safari 17.4), and Hermes has no WebCrypto at all. `thread_crypto.js`
probes the host **once**, with a full round trip — pkcs8 import, jwk
export of the public half, raw import of a peer key, `deriveBits` —
checked against RFC 7748 §6.1 rather than merely observing that nothing
threw. Past that probe a rejection from `deriveBits` is a *real answer*
about those keys and propagates; it never silently reroutes to the pure
path, because two code paths disagreeing about whether a device can open
a job is worse than either answer.

The ladder is **constant-shaped**: all 255 iterations always, and the
conditional swap done with a BigInt mask rather than an `if`. It is not
constant *time* — BigInt multiplication allocates, and its cost varies
with operand size. What that does and does not mean: the attacker in
this model holds a photographed QR or an intercepted file, cannot ask a
victim's device to run the function on chosen input, and cannot time it
if they could. A co-resident adversary already inside the driver's app
with a stopwatch is not defended against here, and is not defended
against anywhere else either — such an adversary reads the key out of
storage.

#### What this does not protect against

Stated plainly, because a sealed ticket invites more confidence than it
earns.

- **The recipient can screenshot what they decrypt.** Sealing controls
  who can open a job, not what they do with it afterwards. A driver who
  photographs their own screen has a customer list, and the operator's
  answer to that is employment and data-protection policy, not
  cryptography.
- **A device that reinstalls loses its key** and must be enrolled again.
  Jobs already sealed to the old key are unopenable — by anybody,
  including the dispatcher, unless it sealed to itself as well. This is
  the cost of the enrolment gate showing up on an ordinary Tuesday, and
  hosts should expect to re-enrol rather than treat it as an incident.
- **Hints are not access control** (above). A record whose hints were
  rewritten in transit still opens for its real recipient and still
  opens for nobody else.
- **Key storage is the host's obligation.** The library holds no
  keychain. An Android host should put the device private key in the
  Keystore and an iOS host in the Secure Enclave-backed keychain; a
  browser host cannot do either and should know it. A stolen, unlocked
  phone is a driver's device, and sealing has nothing to say about that.
- **The seal says nothing about who issued the job.** It is the issuer
  signature, unchanged, that does — and a host that skips
  `verifyThreadTicket` because the bytes decrypted has confused
  confidentiality with authenticity.
- **Traffic analysis is unaffected.** A sealed ticket's length still
  tells an observer roughly how many stops the round has, and its
  recipient count is in the clear.

### 20.10 Reaching the mesh: a fleet's own seed

A 45-byte follow link and a sealed ticket are both **capabilities**. They
say what a run *is* — which key opens it, which epoch it is bound to,
when it expires — and §5.4 is emphatic that this is all they say: *a link
is a key, not a location.* That rule is right, and it leaves a hole this
section closes.

Joining a mesh needs at least one dialable peer. `createPulseMeshHost`
takes `bootstrapPeers` and dials them; with none configured a device
comes up connected to nothing, and the honest end state is a customer
opening a tracking link on a fresh phone and seeing an empty map. The
capability was perfect. There was nowhere to spend it.

**A capability and a location are different things, and confusing them is
the whole trap.** A capability must be secret, must expire, must be
narrow. A location is public, is long-lived, and grants nothing: an
address is not a key, and a peer that answers on it can still read no
thread it does not hold the capability for. Because they behave
differently they get carried differently, and the rest of this section is
that difference worked out.

#### A fleet does not need a DHT

The §4.2 rendezvous mechanism exists so that **strangers can find a
thread on a large network** — a peer holding a link derives a rotating
key, asks the DHT who provides it, and connects. That is the right
mechanism for an open network with no shared operator.

It is not what a fleet needs, and running it as though it were is the
kind of over-engineering that stops a small operator ever shipping. Ten
drivers who can all dial one known machine have a working mesh: gossip
finds the topics, catch-up finds the history, and there is nothing for a
DHT to discover because every participant already has one peer in common.
**The seed is that peer.** `scripts/pulsemesh_keeper.mjs` already *is*
it — headless, `--listen`, `--bootstrap`, the same store and the same
validation, availability and never authority (§12) — and what §20.10 adds
is a way for a fleet to run one as their own seed and hand its address
out.

#### Where an address may travel

Three carriers, one rule each.

**Inside the ticket, signed (PMK1 flags bit 1).** Up to 3 addresses, each
at most 96 bytes UTF-8, each validated to at least look like a multiaddr
(a leading `/`). They live in the **signed preimage**, after the run
seed, because a bootstrap address is a *claim the dispatcher is making* —
dial this machine — and an unsigned claim of that shape is one an
in-flight rewrite could point at any peer it liked. It costs nothing to
sign, and this is the only field on the channel whose entire purpose is
to make a device connect somewhere.

Since the ticket is sealed (§20.9), the address reaches the awarded
device and stops there: a byte scan of the PME1 finds neither the
address, nor its host, nor its peer id — the same test the plan-privacy
claims already carry.

**Not in the offer.** A PMJ1 has no field for one and never will. An
offer is *public and broadcast to couriers nobody has met*, so an address
in one is published to everyone who ever sees it — and for a two-van
operator whose seed is the machine in the back office, that is a home
address with an open port on it. A doxxing vector, not a convenience. Nor
is it needed: **a courier does not need the mesh to evaluate an offer.**
`describeOffer` is entirely local — verify the signature, read the stop
count, the distance, the gridded centroid — and nothing dials until a job
has actually been awarded, at which point the sealed ticket carries the
address to precisely the device that now has a reason to join.

**In a public link's query string, never its fragment.** The customer
case is the coldest start of all: a stranger's phone, a link from an SMS,
no prior contact with anything. `threadLinkUrl(baseUrl, link, {
bootstrap })` puts the hint in front of the `#`:

```
https://track.example/r?b=/ip4/203.0.113.7/tcp/4001/p2p/12D3Koo…#Ac3wz7Qi…
```

The split is load-bearing and is tested as such: **the fragment is
byte-identical with and without a hint.** The capability stays where
browsers never transmit it, so the page host still never receives it. The
hint is in the part that *is* transmitted, which is exactly right for a
public address and exactly wrong for a key. At most 2 addresses (lower
than the artifact's 3 — a link gets typed, texted and printed), repeated
as `?b=…&b=…`, and `parseBootstrapHint` reads them back **leniently**: a
malformed, oversized or over-long hint yields fewer addresses and never
an exception, because a hint the recipient may ignore must never be able
to stop a capability from parsing.

#### PMH1 — the seed card

A seed as an artifact a dispatcher can enrol by scanning the keeper's own
terminal, or hand a driver directly.

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | bytes(4) | `"PMH1"` |
| 2 | version | u8 | 1 |
| 3 | flags | u8 | bit0 label present; reserved bits refused |
| 4 | count | varint | 1..3 |
| 5 | addresses | count × (varint len ‖ UTF-8) | each ≤ 96 bytes, leading `/` |
| 6 | label | varint len ‖ UTF-8 | ≤ 32 bytes, only when bit0 is set |

`H` for **h**ost: what the card names is a host to dial. Every letter
that reads as "seed" is taken — `PMS1`, `PMD1` and `PMB1` are the traffic
channel's, and `PMO1` is the one §20.4 deliberately refuses because `O`
and `0` are one glyph apart in the fonts these four characters get read
back over a phone in. `H` is unused and is confusable with nothing.

**It carries no signature, deliberately.** A signature would be a claim
about authority, and there is none here to make: the `/p2p/<peerId>`
suffix of a multiaddr is what authenticates the far end at the Noise
handshake, so a card naming the wrong machine does not impersonate a
seed — it fails to connect. Signing it would only invite a host to read
"signed by the dispatcher" as "safe to trust", which is the one
conclusion a bootstrap address must never license.

It travels as `wayfind://seed#<base64url>`, in the fragment like every
other artifact — not for secrecy, which an address does not need, but for
**uniformity**: one paste field takes all of them and
`classifyThreadArtifact` decides which is which from the magic. It now
returns `kind: "seed"`, and hosts branch on it: a seed goes to "add this
peer", never to a job screen and never to a map.

#### Keeper: `--seed-card`

```
node scripts/pulsemesh_keeper.mjs --graph=<dir> --epoch=<64 hex> \
     --listen=/ip4/0.0.0.0/tcp/4001 --seed-card --seed-label="Depot seed"
```

Once listening, the keeper prints its dialable multiaddrs, the
`wayfind://seed#…` link, and that link as a scannable QR block rendered
with `▀` — two modules per character cell, so the symbol comes out square
rather than stretched to double height, with the four-module quiet zone
ISO/IEC 18004 requires and explicit black-on-white SGR colours so a
dark-themed terminal does not hand a phone camera an inverted symbol.

**All of it goes to stderr.** stdout is the JSON-lines event contract —
`{"event":"listening",…}` is what an orchestrator waits on and what the
wire tests parse — and a multi-line QR block interleaved into it is a
stream that no longer parses. Two audiences, two streams; the listening
event gains `"seedCard": true` so a supervisor still sees the mode is on.

A seed on its own gives a fleet an **island**: its drivers see each other
and nobody else. Whether that island contributes to and draws from the
wider mesh — and how a fleet's bond-less phones are admitted to
contribute at all — is `--admit` and `--bridge`, specified in
[pulsemesh-protocol.md §12.1](pulsemesh-protocol.md) and explained in
[pulsemesh.md](pulsemesh.md). The one thing to carry back here: the
address in a sealed ticket is what admits a device, so §20.9's seal is
load-bearing for the traffic channel too, not only for the plan.

The address filter is the operationally important part. libp2p reports
what it is *bound* to, which on a server is usually `/ip4/0.0.0.0/…` —
the instruction it was given, not a place anyone can reach — plus
loopback. A card built from those scans cleanly in the depot and connects
to nothing, so when nothing routable is left the keeper prints the reason
and **no card at all**, naming the three fixes: a host with a public
address, a forwarded port, or a relay in front.

#### Remembering peers

The seed should matter at first contact and at no other time. Once a
device has met the mesh it has met peers, and dialling one of those next
time is both faster and one less thing that has to still be running.

Two seams, both minimal:

- `createPulseMeshHost({ rememberedPeers })` dials them alongside
  `bootstrapPeers`, configured first, deduplicated.
- the libp2p network object exposes `knownPeers()`: the multiaddrs
  actually connected to, each with a `/p2p/<peerId>` suffix so it can be
  dialled again as-is.

**The library reports and accepts; the host persists.** There is no peer
store here and there is not going to be one — ageing, capping, and
whether a device is allowed to keep the list at all are decisions only
the embedding app can make, and they differ between a fleet's own phone
and a stranger's browser tab.

#### What the QR budget costs now

§20.5's scannability policy is unchanged: `maxVersion: 25` at EC level
`M` carrying `wayfind://ticket#<base64url>`, which is 735 bytes of
carrier payload. §20.9's seal takes 130 of them for one recipient and 194
for two, leaving 605 and 541 bytes of signed ticket. A realistic address
— `/ip4/203.0.113.7/tcp/4001/p2p/12D3Koo…`, 82 bytes — costs **84 more**
(the address, its length varint, and the count), so a single-recipient
ticket has 521 bytes left for the plan.

Measured against §20.8's reference stop (a 20-byte label, a planned time,
planet-scale coordinates), with the encoder as the authority and the
tests asserting that one more stop does *not* fit:

| Stop carries | seed | 1 recipient | 2 recipients |
| --- | --- | --- | --- |
| nothing set | — | 12 | 10 |
| nothing set | one address | 9 | 8 |
| order `4471`, 2 parcels | — | 10 | 8 |
| order `4471`, 2 parcels | one address | 8 | 6 |
| all four at their caps | — | 2 | 2 |
| all four at their caps | one address | 2 | 1 |

So a seed address costs **two to three stops** off a scannable round. A
day that exceeds the ceiling travels as the `.wayfindjob` file (§20.5) —
the identical sealed bytes, no protocol difference — exactly as a
hundred-drop day always has.

#### The honest costs

- **A seed is a single point of failure for *joining*, and for nothing
  else.** A mesh that has already formed keeps working when the seed goes
  down: gossip, catch-up and the ETA are all peer-to-peer, and remembered
  peers mean a device that has connected once does not need it again.
  What breaks is a *cold* device on a day the seed is down.
- **The seed sees connection metadata.** It learns which peers connect,
  when, and from which addresses. It does **not** see thread contents
  (records are sealed to a key it does not hold), it cannot enumerate
  topics (§4.2 tags are pseudorandom and rotate), and it learns nothing
  about traffic identities (§5.1 gossip is `StrictNoSign`). "Who was
  online at 07:40 from which IP" is real and is the price; "where was the
  van and what was in it" is not available to it.
- **An address in a public link is public.** The query hint is a
  disclosure to whoever hosts the page, whoever logs the URL, and whoever
  the customer forwards it to. That is acceptable for an address a fleet
  chose to publish and unacceptable for the dispatcher's back-office
  machine — which is why the offer carries none and why a hint is a
  per-link decision rather than a side effect of issuing a job.
- **NAT is the whole practical difficulty.** A seed behind a home router
  is not dialable: it needs a forwarded port, or a circuit relay in front
  of it, and the relayed address is what goes in the card. A small VPS
  sidesteps the problem entirely and is the recommendation for anyone who
  does not want to think about it. The keeper refuses to print a card
  built from loopback or `0.0.0.0` precisely so this failure surfaces in
  the depot rather than on the road.

## 21. Recurring routes: identity split from authority

### 21.1 What breaks

A school bus route 8A leaves the depot at 07:00, every school day, for a
term. The plan is fixed. The driver changes some days and not most —
sickness, holidays, the spare bus. And a parent must be able to open the
same link in September and in February, without ever resubscribing,
because the moment a link has to be reissued the operator is back to
running a subscription list, an SMS pipeline and a support queue for
"the tracking stopped working".

§4.1 makes that impossible, and it does so deliberately: one Ed25519
keypair is the run's identity *and* its publish authority at once. The
public key is the topic tag, the content key and the 45 bytes in the
link; the private key is the right to move the bus. For a delivery that
dies at the door those are the same thing and collapsing them is the
whole elegance of §4.1. For a route they are not. Handing tomorrow's
driver the authority means handing over the key, and rotating the key
rotates the link — so **every parent's subscription breaks every time
the depot changes who is driving.** Not rotating it means one seed
travels through a term's worth of phones, and the first one that is lost
owns the route until June.

### 21.2 The split

Identity and authority come apart the way they do in TLS, where a
certificate says "this ephemeral key speaks for this long-lived name".

- **Identity is the root key.** Topic tags, `K_content` and the link keep
  deriving from the *root* public key exactly as §4.1 says — so the tag
  rotation, the rendezvous key, the sealing and the 45 bytes are
  unchanged and a subscriber's link never moves for the life of the
  route. This is the requirement everything else serves.
- **Authority is a short-lived day key**, derived by the depot:

  ```
  daySeed = HKDF-SHA256(rootSeed,
              "pulsemesh/route-day/1" ‖ planRef(8) ‖ uint32be(serviceDay))
  ```

- **A day certificate binds them.** `{ rootPublicKey, dayPublicKey,
  serviceDay, notBefore, notAfter }`, signed by the **root**. Records are
  signed by the day key; a subscriber verifies the chain.

`planRef` is in the derivation so a root driving two rounds — a morning
loop and an afternoon loop under one route identity — does not hand both
to whoever holds one.

**Why derived rather than random.** A depot with 40 routes and a 180-day
term would otherwise be storing 7,200 secrets, backing them up, and
having an answer for what happens when the backup is lost. Derivation
replaces all of that with one root per route: any day, past or future,
regenerates from `(rootSeed, planRef, serviceDay)`. Recovery is
re-derivation, and there is no key store to breach that is not already
the root.

**Why the driver never holds the root.** `createThreadPublisher` takes a
day seed *plus its certificate*, and **refuses** a root seed passed
alongside one. This is not defensive tidiness. A driver's phone is a
device that gets lost, sold, seized at a border and handed to a repair
shop; if it holds the root, its compromise costs the whole term and
there is no recovery short of reissuing every parent's link — which is
precisely the failure the split exists to prevent. What is on the phone
is one day of authority, and it expires on its own.

### 21.3 PMTC — the day certificate

| # | Field | Encoding | Notes |
| --- | --- | --- | --- |
| 1 | magic | `"PMTC"` | |
| 2 | version | u8 | 1 |
| 3 | rootPublicKey | bytes(32) | the route identity |
| 4 | dayPublicKey | bytes(32) | the delegated signer |
| 5 | serviceDay | uint32be | the operator's civil date, `YYYYMMDD` |
| 6 | notBefore | uint32be | absolute unix seconds |
| 7 | notAfter | uint32be | absolute unix seconds |
| 8 | signature | bytes(64) | Ed25519 by the **root** over fields 1–7 |

145 bytes, sealed into an ordinary PMT1: 188 on the wire, inside the
256-byte record limit with room to spare.

**Why `PMTC`.** Every `PMx1` envelope slot that reads as "certificate" is
taken (`PMC1` is the traffic channel's), and `PMO1` is the one §20.4
deliberately refuses because `O` and `0` are one glyph apart in the fonts
these four characters get read back over a phone in. But the deeper point
is that this is **not an envelope at all** — it is an inner sealed body,
the same thing `PMTP` is, so it belongs to the `PMT` family that §20.7
already established for exactly this reason: a `PMT` prefix is one a
traffic-only implementation already ignores, and the fourth letter names
the record rather than a version digit. `C` for certificate is unused and
confusable with nothing.

**Why it is a record on the run's own topic.** Because §5.5 catch-up then
carries it for free. A parent who opens the link at 07:41, twenty minutes
into the run, pulls the certificate out of the same PMM1 as the records
it has to verify — no new protocol id, no new message type, no second
fetch that could fail independently of the one that depends on it, and
no host to fetch it *from*. It is sealed like every other body, so it is
readable only by someone who already holds the capability.

That placement is also the one point where `P` appears inside a body,
and §4.1 forbids putting `P` **on the wire**. It is not on the wire: the
body is sealed under `K_content = HKDF(P)`, so anyone who can read field
3 had to hold `P` to get there — the same argument that already puts the
§5.2 signature inside the sealed body rather than the envelope. It earns
its 32 bytes by making "this certificate belongs to another route"
refusable *by name* rather than as an anonymous signature failure.

**Cadence.** A route-day publisher emits its certificate before its first
record, and re-emits it every `THREAD_CERT_INTERVAL` (60 s). Rotation
needs no special case: a new service day is a new publisher, and a new
publisher leads with the new day's certificate. Sixty seconds is the
number because it matches `THREAD_COARSE_HEARTBEAT` and lands inside
`THREAD_STALE` (90 s) — a joiner never waits longer for authority than
the UI would spend claiming "live" anyway — while costing a coarse run
one extra record per heartbeat (~6 B/s) and a fine run one per twelve.
Anything much rarer starts to matter against the 240-record
`THREAD_CACHE_RING` that makes catch-up carry it in the first place.

### 21.4 The link says which

`LINK_VERSION` is bumped: **a v2 link means delegated signing**, and the
key in field 2 is a route root rather than the signer. One-off runs stay
v1 and are unchanged — `encodeThreadLink` still writes 1 by default,
because the version states what the artifact *is* rather than how new the
writer is, and the §16.3 vector is byte-identical.

The version has to be in the artifact rather than inferred per record.
"This record verified under some key I was told about" is not a security
property: if a subscriber decided to accept delegated signatures because
a certificate turned up, then sending a certificate would be the
downgrade. A holder knows from its 45 bytes whether the key in its hand
is an identity or an authority, and a v1 subscriber refuses a PMTC on its
thread outright — the publisher and the artifact disagreeing about what
the run is, with the artifact winning.

### 21.5 Subscriber verification

§7's step 5 gains a discriminator — one topic now carries two body
shapes, so the inner magic picks the decoder before either runs — and
step 6 splits by link version.

A **PMTC** is accepted only if all of these hold:

1. the link is v2 (a v1 link refuses it: `cert-on-self-signed-link`);
2. field 3 equals the root public key from the link
   (`cert-foreign-root`) — checked *before* the signature, so a depot
   that pasted the wrong route's certificate is told which mistake it
   made;
3. `notAfter > notBefore` (`cert-empty-window`);
4. `notAfter − notBefore ≤ THREAD_MAX_CERT_SECONDS` (48 h)
   (`cert-window-too-long`);
5. the signature verifies against the **root** (`cert-bad-signature`);
6. `now + THREAD_CERT_SKEW ≥ notBefore` and
   `now − THREAD_CERT_SKEW ≤ notAfter`
   (`cert-not-yet-valid` / `cert-expired`);
7. `notBefore ≤` the link's own `notAfter` (`cert-after-link-expiry`).

**Rule 4 is the one that carries the design.** It is a bound the
*verifier* applies, not merely one the issuer respects. If the 48 hours
lived only in `mintDayCertificate`, a root could mint `notAfter = 2038`,
the day key would be a permanent publish credential, and a lost driver
phone would cost the term again — the whole split evaporates. A host may
**tighten** it through `constants.THREAD_MAX_CERT_SECONDS`; the
subscriber takes the minimum of the two, so no configuration loosens it.

48 rather than 24 because a service day is not a calendar day: a run
landing at 01:20 needs a window that opened the previous morning, and a
depot minting tomorrow's certificate this afternoon wants it live before
the phone leaves the charger. `THREAD_CERT_SKEW` is 300 s on both edges
and is deliberately looser than `THREAD_MAX_FUTURE_SKEW` (15 s), which
bounds how fresh a *position* is. This bounds a window measured in hours,
so five minutes costs nothing against the 48-hour cap and covers the
phone that has had no NTP since Friday. Being mean here has exactly one
failure mode and it is the bad one: a parent whose clock drifted is told
the bus is not being tracked.

**A record with no valid certificate is not a forgery.** Under a v2 link
a record whose signature no held certificate covers is refused with its
own reason — `awaiting-certificate`, "waiting for the day's certificate"
— and never with `bad-signature`. A joiner who has not yet heard today's
certificate is the ordinary case, not an attack, and `stats.forgeries`
stays at zero for it.

Such records are **held** and re-checked when a certificate arrives. The
buffer is bounded twice: by count, `THREAD_HELD_RECORDS` = 32, evicting
oldest-first — at the fine 5 s cadence that is 160 s of records, well
past the 60 s a joiner can possibly wait, and 32 × 256 B is 8 KiB, so it
cannot become an amplifier — and by **age** on release, dropping anything
already past `THREAD_MAX_AGE`, because a position that would fail step 8
on arrival must not pass it by having been held.

Be honest about what this costs: under delegation a genuine forgery by a
link holder is *indistinguishable* from an uncertified-yet record, since
neither verifies under anything held. So it is held too — and then
evicted, because the certificate that would vindicate it never comes. It
is never accepted. The trade is that `stats.forgeries` no longer counts
that attack on a v2 link; `heldEvicted` is where it shows up instead.

### 21.6 `seq` is scoped to the signing authority

Step 7's ledger becomes per-signer under a v2 link. This is forced, not
chosen. Day 39 ends at 16:40; day 40 starts at 07:00 from a different
device with a different day key, and nothing published in between — the
catch-up ring holds ten minutes, so §20.5's resume round has nothing to
resume *from*, and the new publisher cannot know what number yesterday's
device reached. Under a global counter, every parent whose phone stayed
awake overnight would reject the whole of day 40 for `seq` regression.

Nothing is lost. Step 7 defends rollback and replay *within* an
authority, and that is unchanged. Replaying yesterday's records under
today's ledger fails twice over anyway: their signature needs yesterday's
certificate, which is outside its ≤48 h window, and their `unixSeconds`
is a day past `THREAD_MAX_AGE`. The counter was never the cross-day
defence. `subscriber.highestSeq` remains a thread-wide high-water mark,
because that is what a §20.5 *mid-run* handover resumes above.

### 21.7 Why short-lived certificates remove the revocation problem

§4.3 answers revocation with non-renewal, and that answer only works
because a key lives for one run. A term-long root would reopen the
question — and a revocation list is the one thing this channel cannot
have. There is no server to publish it, no directory to consult (§4.2 is
the whole point: threads cannot be enumerated), and a subscriber that had
to reach an endpoint before trusting a record would hand that endpoint
the tracking key it exists to hide.

So the answer is the same one, one level down: **authority expires
faster than a revocation could propagate.** A driver's phone is stolen at
09:00 Tuesday; the day seed on it is worthless by Wednesday evening at
the latest and in practice at that certificate's `notAfter`, which is the
end of Tuesday's service. The depot's response to a lost phone is to not
mint a certificate for that device again — the same shape as §4.3's "stop
sending tomorrow's link", and it costs a list of nothing. Nothing has to
be told, nothing has to be consulted, and no parent's link moves.

The residual window is real and stated: a leak is live until its
certificate's `notAfter`, up to 48 h and normally one shift. Shortening
`notAfter` shortens it; that is the only dial, and it is the operator's.

### 21.8 What a leaked seed is worth

| Leaked | Reveals | Lets the holder | Lives until |
| --- | --- | --- | --- |
| the 45-byte link | this route's whole term, read-only: find, decrypt, verify, and the §11 granularity the operator chose | nothing on the wire — it cannot sign | the link's `notAfter` (a term) |
| a **day seed** (+ its certificate) | the same reads, plus publish authority | move the bus, mark stops, and open that day's §20.7 photos | that certificate's `notAfter` — ≤48 h, normally one shift |
| the **root seed** | everything above, for every day of the term | mint any day, past or future; re-derive every day seed; forge any certificate | the term — and the only fix is a new root, which is a new link for every parent |

The middle row is what makes the design worth its complexity. Before
§21, the only publish credential *was* the bottom row. A leaked
**route-day ticket** (§21.11) is that middle row plus the plan it
carries, and nothing else — it cannot mint a day, re-derive one, or move
the route's identity.

The photo key (§20.7) derives from the signing seed, so on a route it is
the day seed: one leaked day opens one day of doorstep photos, and the
depot re-derives that day seed from the root when it needs to open them
later. A link holder still cannot, exactly as before.

### 21.9 The service day, and the timezone question

`serviceDay` is the operator's **civil date for the run**, packed as the
integer `YYYYMMDD` — 2026-09-08 becomes 20260908 — chosen **in the
route's own local timezone, by the operator.**

The question has to be answered out loud because a 07:00 local departure
straddles UTC midnight through much of the world, and a run from 23:10 to
01:20 straddles *local* midnight too. The rule: a run keeps the civil
date it **departed** on, so one service day is one contiguous block of
work and never two. This is not a new decision for anybody involved — the
depot's timetable already has a name for the day a run belongs to, and
that is the number that goes here.

Leaving it to the operator is safe because **`serviceDay` is never
compared against anybody's clock.** It is an HKDF input and a display
label, and nothing else. The only time-of-validity claim on the wire is
`notBefore`/`notAfter`, in absolute unix seconds, which is unambiguous
everywhere. A depot in Montréal and a parent's phone roaming in Lisbon
can disagree completely about what day it is and still agree, to the
second, about whether this certificate is live. The one rule an operator
must keep is uniqueness: two runs of one `planRef` on one `serviceDay`
would derive the same day seed, so a route with a morning and an
afternoon round gives them different plans, which the `planRef` in the
derivation already accounts for.

### 21.10 What this does not change

Everything in §4.2, §5.1, §5.2, §5.5, §7 steps 1–4 and 8–9, §8, §9, §10,
§11 and §12 is untouched. A route run is an ordinary run with an ordinary
plan, ordinary records and ordinary catch-up; the certificate is one more
sealed body on the same topic, and the only edits to the pipeline are a
magic check at step 5 and a signer lookup at step 6. It also does not
change §20 for a one-off job: a dispatched delivery is still a ticket,
still v1, and still self-signed.

What §21.11 adds is a *carrier*, not a second answer. Tickets answer "who
is driving this", and a route-day ticket answers it for one service day
of a route — the same PMK1, the same seal, the same enrolment, with the
day's certificate inside it and a flag saying so. The two artifacts share
everything except which key the run's identity comes from, and that one
difference is decided by the certificate rather than by the caller.

### 21.11 Handover: the route-day ticket

Everything above defines how to *run* a service day and nothing at all
about how a day reaches the person driving it. A dispatcher who has
minted tomorrow's seed and certificate is holding two loose values — 32
bytes and 145 bytes — with no artifact to hand over, no carrier, and no
refusal for the ways of getting it wrong. That gap is what this
subsection closes.

**Why sealing a bare day seed is wrong, and why it is the mistake
everyone makes first.** The obvious move is to put the day seed into an
ordinary PMK1 (§20.3) and seal it: it is a 32-byte Ed25519 seed, the
ticket takes one, the sealing and enrolment machinery all work. It even
runs. But a ticket's seed is the run's *identity* — `publishTicket`
derives the topic tag, `K_content` and the 45-byte link from **its**
public key — and §21.2's whole point is that on a route the identity
stays with the **root** so a parent's link never moves. So the bus
publishes, correctly signed and correctly sealed, onto a topic derived
from the **day** key: an address nobody is subscribed to and nobody ever
will be, because it changes again tomorrow.

Nothing errors. The driver's phone shows a healthy run with a rising
`seq`. The depot's dashboard, if it follows the link it just derived from
the same ticket, shows the bus moving. Every parent's link — the one
minted in September, the one the whole of §21 exists to keep still — goes
silent, and the only symptom is a support queue. It is the worst shape a
protocol failure can take: invisible on the side that would notice,
undiagnosable on the side that suffers.

So the day is carried by an artifact that cannot be published that way.

**The shape.** A route-day ticket is an ordinary PMK1 with `flags` bit2
set (§20.3 field 11), carrying the **PMTC day certificate** inside the
signed preimage, and whose `privateSeed` is the **day** seed. Reusing
PMK1 rather than inventing a parallel artifact is the point: sealing
(§20.9), device enrolment, the `.wayfindjob` file, the QR carrier,
handover by re-sealing and every refusal already work on it, unchanged.

The certificate is what makes that sufficient. It already carries
`rootPublicKey`, `dayPublicKey`, `serviceDay` and the window, so one
sealed artifact hands the driver all three of the things a day needs —
**identity** from the certificate's root, **authority** from the seed,
and the **proof** to publish beside its records — with no second file to
lose and no ordering problem between them. It costs a fixed 145 bytes,
inside the preimage for the same reason a bootstrap address is (§20.10)
and a sharper one: the certificate is *which day of which route* the
issuer granted, and one that could be edited in flight would let anyone
who can rewrite an envelope point a driver at another route's authority.

**`publishTicket` routes it, so no caller can forget to.** The branch is
made from the artifact rather than from an argument: a ticket carrying a
certificate opens a publisher with `daySeed` and `certificate` — the
§21.2 delegated path, deriving topics and sealing from the **root** and
signing with the day key — and one without carries on exactly as before.
`ticketFollowLink` is the single place a ticket's 45 bytes are derived,
and it returns the route's own v2 link (§21.4) for a day ticket, so
there is no arm of the library that can produce a link over a day key.
The run's bound comes from the certificate's `notAfter` rather than the
ticket's, because the ticket's is the *term*.

**Validation, and where each rule lives.** Structural checks are at
decode, so a ticket that cannot work never reaches a host as one that
can; the checks needing a key agreement or a clock are in
`verifyThreadTicket`, which `publishTicket` runs before it opens
anything.

| Rule | Refused as | Where |
| --- | --- | --- |
| bit2 without bit0 — a day with no seed | *carries the day seed its certificate vouches for; this one has none* | decode |
| the embedded PMTC does not parse, or is not v1 | the PMTC decoder's own complaint | decode |
| `issuerPublicKey ≠ certificate.rootPublicKey` | *issued by the route root itself* | decode **and** verify |
| the certificate fails §21.5 rules 3–6 | `cert-empty-window`, `cert-window-too-long`, `cert-bad-signature`, `cert-not-yet-valid`, `cert-expired` | verify |
| `notBefore >` the ticket's `notAfter` | `cert-after-link-expiry` | verify |
| the seed is not the certificate's `dayPublicKey` | `ticket-day-seed-mismatch` | verify |

The last one is worth its own sentence. A seed that is not the key the
certificate vouches for is a ticket that cannot sign a single record any
subscriber will accept, and the failure would otherwise surface as a
driver five stops into a round whose parents see nothing. It is refused
**by name**, before the publisher exists, and the publisher refuses it
again (§21.2) so neither layer is load-bearing alone.

**The issuer is the root, and that is not a tightening.** Only the route's
owner grants a day of it. It looks strict — everywhere else in §20 the
issuer is a dispatcher identity that need not be anything else — but it
costs nothing that was available anyway: minting the certificate and
deriving the day seed both require the root *seed*, so any party able to
issue a route-day ticket already holds the root by construction. What it
buys is that a holder of one day's authority cannot re-dispatch it under
a name of its own choosing, which is the accountability the ticket
signature exists for; and a driver's app that pins `expectedIssuerHex`
is pinning the route itself, the same key that is in the term link. The
accepted cost is that an operator who wanted a separate per-depot
signing identity on these tickets cannot have one.

**`planRef`, and what §21 leaves unverifiable.** §21.2 derives the day
seed from `(rootSeed, planRef, serviceDay)`, and a day ticket carries a
plan because the driver needs the stops — so the two must agree. They
are reconciled at **mint**: `issueTicket` re-derives the day seed from
the issuer's root seed, the dispatched plan's `planRef` and the
certificate's `serviceDay`, and refuses a ticket whose seed is not that
one (it will also derive it when the depot passes none). This is the
only place the check is possible. **The certificate does not carry
`planRef`**, and HKDF does not invert, so a driver's device — holding
neither the root nor the plan the certificate was minted against — cannot
check it at all. That is a real gap in §21.3: `planRef` is in the
derivation to keep a morning loop and an afternoon loop apart, but
nothing downstream can *verify* which loop a day key was scoped to. The
mint-time check is what keeps the two honest, and it has a consequence
worth stating out loud: **a route-day ticket carries the term's plan byte
for byte.** A depot that wants to hand one driver a subset of the stops
has changed `planRef`, which is a different day key needing its own
certificate.

**What a leaked route-day ticket is worth.** Exactly the middle row of
§21.8 plus the plan, and no more. It opens to one day's publish authority
over one route, expiring at that certificate's `notAfter` — ≤48 h, in
practice one shift — and it discloses the round's stops and whatever
§20.8 metadata the plan carries. It is **not** a root: it cannot mint
another day, cannot re-derive any other day's seed, and cannot move the
route's identity, so a lost driver phone still costs a day and never a
term. Against the alternative the gap forced — a day seed pasted into a
message, or an app-level file invented outside the protocol — the ticket
is strictly better: it is sealed to an enrolled device (§20.9), so a
photographed screen is ciphertext, and it is signed, so a driver can tell
which depot sent it.

**Size, measured.** The certificate is a flat 145 bytes against the
§20.8 QR budget, which leaves 460 bytes of a single-recipient sealed
ticket for the plan: **8 address-only stops in a scannable QR, against
12 without** — measured with `encodeQr` at `maxVersion: 25`, EC level
`M`, and asserted by the tests. So a school route, a milk run or a
morning of eight drops still travels as a QR code on a depot noticeboard.
Anything larger travels as the `.wayfindjob` file, which is the same
sealed bytes and no protocol difference — the answer a hundred-drop
delivery day has always had.

`classifyThreadArtifact` reports `routeDay` and `serviceDay` beside
`kind`, so a host can say *"route 8A, Tuesday's run"* rather than *"a
job"*. `kind` stays `"ticket"`, because it is one and it goes to the same
screen and opens with the same key. On a sealed PME1, `routeDay` is
`null` rather than `false`: which sort of job is inside is ciphertext,
and a UI that rendered "an ordinary job" there would be inventing a claim
nobody made — the same rule §20.8 applies to an unstated parcel count.
