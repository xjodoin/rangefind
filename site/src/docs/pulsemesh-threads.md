---
title: PulseMesh threads
lede: Authenticated tracking of one moving thing for a bounded audience — a school bus, a courier — where the whole capability is a 45-byte link and the arrival time is computed on your own device.
description: PulseMesh threads, rangefind's second channel — one Ed25519 capability per run, end-to-end sealed position updates, host-free catch-up, granularity as a safety control, and locally computed live-traffic ETAs no server ever learns.
order: 16
---

The [traffic channel](../pulsemesh/) answers *"how fast is this road"*
from many anonymous corroborated observations. Threads answer the
opposite question — *"where is **this** vehicle, and when does it reach
**my** stop"* — for an audience authorized to know, and for nobody else.

Every load-bearing property is inverted, which is why it is a second
channel rather than a mode:

| | Traffic channel | Thread channel |
| --- | --- | --- |
| Publishers per fact | many, required | exactly one, authoritative |
| Identity | none; stable ids prohibited | a per-run keypair, mandatory |
| Trust basis | corroboration ≥ 3 reports | a signature |
| Audience | everyone in the zone | capability holders only |
| Confidentiality | none needed (it is aggregate) | end-to-end, mandatory |
| Topic | derived from geography | derived from a secret |
| Lifetime | 90 s TTL | one run, then gone |
| Failure mode | degrade to the static metric | degrade to the schedule |

They share the transport, the cell vocabulary, the segment identity, the
epoch discipline, and the codec conventions. They share no records and
no trust: a signed single-source record never enters a corroborated
aggregate, because that would turn one fleet key into a traffic
authority.

Status: **implemented** (`rangefind/pulsemesh/threads`), milestones
T1–T4 complete and tested byte-for-byte against the specification's own
vectors. A field pilot is the open item.

## What it replaces

A school-bus or delivery tracker today streams continuous positions into
a vendor database, where they persist indefinitely, are joined to
accounts, and are readable by the vendor, its subprocessors, and anyone
who compromises it. Arrival times are computed server-side, so the
server also learns every parent's stop and every recipient's address.

Threads keep live position, arrival estimates, and arrival alerts. They
remove the plaintext server, the retention, the account join, and the
server-side knowledge of who is watching what.

## The link is a key, not a location

Per run the publisher generates **one Ed25519 keypair** and nothing else.
The private key never leaves the publishing device. The 32-byte public
key `P` is the entire capability, and everything else is HKDF-derived
from it:

| Derived | Purpose |
| --- | --- |
| `K_topic` | the rotating topic tag |
| `K_content` | the AES-256-GCM content key |
| `noncePrefix` | the record nonce prefix |

So the split is: **private key → write**, **public key → read, find and
verify**. Every subscriber holds the link, and none of them can move the
bus.

```
tag(window) = HMAC-SHA256(K_topic, "pulsemesh/thread/1" ‖ epoch ‖ window)[0..8]
topic       = /rangefind/pulsemesh/1/t/<epochPrefix>/<tag>
rendezvous  = SHA-256(topic)
```

`window` rotates every 5 minutes, the same rotation the traffic channel
uses. Four consequences:

1. The topic name is a pseudorandom 8 bytes — no zone, route number, or
   operator. Threads cannot be enumerated or browsed.
2. Finding a thread *requires* the capability. There is no "listen to all
   buses" position, even for a relay.
3. Tags are unlinkable across rotations without `K_topic`, so an observer
   cannot follow a thread through the day by its address — and neither
   can the DHT.
4. The DHT learns only that some peers provide an opaque, rotating key.

The whole capability travels as a **45-byte link** in a URL fragment —
SMS, QR code, order confirmation email — carrying `P`, an epoch, and a
`notAfter`. Because `P` is distributed rather than published, an
implementation that logs it, puts it in a query string, or reuses it
across runs has broken the confidentiality of every thread it ever
signed. Revocation is simply non-renewal: a delivery thread dies at the
door, and a school run's link is issued per run to currently-entitled
families. No group rekeying, no revocation list, anywhere.

## Arrival time is a local route query

This is the integration, and it is why threads belong in rangefind
rather than in a tracking product. A thread carries **position only**.
The subscriber routes from that position to *its own* stop, under the
traffic channel's live metric, on its own device:

```js
import { estimateArrival } from "rangefind/pulsemesh/threads";

const estimate = await estimateArrival({
  engine,                          // an open route graph
  update: subscriber.latest(),     // the decrypted position
  plan,                            // the published run plan
  myStopIndex: 7,
  live: trafficProvider            // the other channel
});
// estimate.arrivalMillis, estimate.basis, estimate.profileBasis
```

Under the hood it calls `engine.locate(segment, ratio)` — the exact
inverse of `snap()` — and then `matrix()` over the remaining stops.
**`matrix()`, not `itinerary()`**: `itinerary()` reorders stops, which
is exactly wrong for a run whose sequence is fixed by the plan and
silently produces optimistic ETAs by shortcutting the route the vehicle
is actually going to drive.

What that buys:

- **The publisher never learns which stop matters to anyone.** It
  broadcasts a position; each device computes its own answer. Compare
  the server-side ETA it replaces, which necessarily knows every
  recipient's address.
- **Traffic is already there.** A courier stuck on a jammed arterial
  produces a correct ETA because three other drivers reported 12 km/h
  and corroborated each other — and none of them learned anything about
  each other.
- **Same numbers everywhere.** The dispatch console runs the same engine
  over the same static index, so the parent and the dispatcher never
  argue about whose app is right.
- **Nothing new to route with.** Multi-point live-aware evaluation is
  existing engine behaviour; threads add arithmetic, not routing code.

Two details that are easy to get wrong and are therefore enforced:
stops marked skipped, failed, or already delivered leave the remaining
chain (left in, one skipped delivery inflates every downstream
customer's estimate for the rest of the day), and every estimate states
which routing profile it used — `matched`, `mismatched`, or `unstated`.
A courier on a bike estimated on a car graph is wrong by a wide margin,
in the optimistic direction, and nothing about the number says so unless
the result does.

## Granularity is a safety control

`mode` is not a bandwidth setting. It decides what a leaked capability
is worth.

- **Coarse** publishes stop events plus a heartbeat — no position. A
  leaked coarse thread reveals that a vehicle passed a published stop at
  a time: roughly what a printed timetable and a bystander reveal.
  Arrival estimation still works, from the last stop's known position
  over the remaining plan.
- **Fine** publishes continuous position and is a live locator for
  whoever holds the key.

The recommended defaults follow the harm, not the convenience: **coarse
for anything carrying children**, fine only as a deliberate operator
choice with the trade-off stated to families; fine for couriers, whose
position is the product. Publishers may also withhold position entirely
near first and last stops — the part of a run where a leak is most
directly a home address.

Per-stop outcomes are stop events, so coarse carries them: "the parcel
was left with a neighbour" is not a position.

## Degradation contract

| Available | Behaviour |
| --- | --- |
| Thread + traffic | live position, live-traffic ETA |
| Thread only | live position, static-metric ETA |
| Traffic only | plan-based prediction, live traffic on the remaining legs, position *unknown* |
| Neither | the published run plan, marked *scheduled* |

An implementation must not present the lower rows as the upper ones.
"Bus expected 07:44" and "bus is here, arriving 07:44" are different
claims, and the second one is why anyone installed the app.

## Delivery without a mailbox

Thread topics carry no geography, so they cannot ride the traffic
channel's zone-scoped gossip mesh. A phone that slept for four minutes
has a hole in a thread, and the only alternative to filling it from
other peers is a mailbox host — the exact server this design exists to
avoid.

So every follower caches the sealed bytes it sees and answers catch-up
requests for the tags it holds. A relay cannot open what it caches and
does not need to be trusted: records travel verbatim, so tampering fails
the AEAD, forgery fails the signature, and replay fails the sequence
number — at the joiner, which validates everything itself. Requests are
padded with random decoys, which is free here because a tag is
indistinguishable from random, and unknown tags are answered with count
zero so a prober cannot learn which threads a peer follows.

Measured, and it corrected the spec: **audience size barely matters; the
number of providers a joiner asks decides everything.**

| Audience | Providers asked | Late joiner finds a cache |
| --- | --- | --- |
| 1 | 1 | 45% |
| 100 | 1 | 53% |
| 10 | 3 | 95% |
| 30 | 8 | 100% |

The same run pinned a limit the design had never stated: catch-up can
never recover more than **two minutes** of history, because anything
older is rejected regardless of who cached it. "Catch-up" means the last
two minutes, not the run so far, and a UI must not promise otherwise.

## A term-long link that survives a change of driver

A school bus keeps one plan for a term and changes driver some days, so
the thing that *publishes* the run cannot be the thing that *identifies*
it — rotating the key for a new driver would rotate every parent's link.

The split: the root key stays the identity (topic tag, content key, the
45 bytes), and a short-lived **day key**, derived from it and certified
by it, carries the publish authority. The driver's phone holds one day
of authority and never the root. The certificate rides the run's own
topic, so host-free catch-up delivers it to a late joiner for free, and
a subscriber **refuses any certificate valid for more than 48 hours** —
"short-lived" is a property the verifier enforces, not one the depot
promises. There is still no revocation list anywhere.

The same machinery carries dispatch: a run belongs to the job rather
than to the driver, so a sealed ticket can be handed to whichever device
takes the round, a mid-run handover resumes above the sequence number
already published, and proof-of-delivery photos are sealed under a key
derived from the run's private seed — which the customer's 45-byte link
structurally cannot derive.

## Measured

Everything below runs the real WebCrypto path, so the same numbers apply
in a browser.

| Measure | Result |
| --- | --- |
| PMT1 record, fine mode | **133 bytes** |
| The link — the entire capability | **45 bytes** |
| Sign / verify | 0.064 ms / 0.066 ms |
| Full receive path (decode + open + parse + verify) | ~0.09 ms → ~11k updates/s |
| Records with an unguessable tag rejected | ~1.2–1.3M/s on one core, **no crypto performed** |
| Per fine thread | 26.6 B/s · 94 KB per thread-hour |
| A 500-bus fleet, one morning hour | ~46 MB across the whole mesh |
| Coarse mode | ~20% of fine |

An attacker cannot make a subscriber do asymmetric work without the
capability, because they cannot compute an address it listens on. And
the coarse-mode line is a correction: the specification originally
claimed two orders of magnitude below fine, and the measurement puts it
at one. Coarse is still the right default for child transport — on the
safety argument, never on bandwidth.

The central thesis is a test rather than a claim: a publisher and
subscriber run over the real Québec route graph, and the subscriber's
ETA moves when a jam is injected into the *traffic* channel — while the
subscriber sends nothing at any point.

## Legal posture, stated plainly

Tracking a school bus tracks a **driver**: an identifiable worker,
continuously, during employment. Québec's Law 25 requires informing an
employee of technology used to locate or profile them, and the GDPR
reaches the same place through purpose limitation.

This design helps where it can — retention is a TTL rather than a
database, no historical trajectory store exists by construction, and no
vendor holds plaintext — but it does not make a driver's position stop
being personal data. A thread must not be repurposed into performance
monitoring, the publishing device shows when a thread is live and can
end it, and the operator's own dispatch console remains a normal system
of record where retention and access control have to be real.

The honest summary for product copy: this removes the vendor database
and the server-side knowledge of who watches whom. It does not remove an
operator's knowledge of where its own vehicles are, and it should not
pretend to.

The full specification — wire formats, validation pipeline, dispatch
tickets, recurring routes, and test vectors — is
[`docs/pulsemesh-threads.md`](https://github.com/xjodoin/rangefind/blob/main/docs/pulsemesh-threads.md).
