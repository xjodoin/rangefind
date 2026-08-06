# PulseMesh over LoRa (§16)

How the mesh runs where there is no internet at all: over
Meshtastic-class LoRa radios, joined to the IP mesh by bonded bridges.
Implemented in `src/pulsemesh/lora.js`, tested against simulated radio
physics in `test/pulsemesh_lora.test.js`.

## 16.0 Why this works at all

The 42-byte record was designed so the codec *cannot* carry a
coordinate, a trajectory, an identity, or a precise timestamp — a
privacy decision. A duty-cycled 0.3–20 kbps radio wants exactly the same
thing. Measured against Meshtastic's ~237-byte usable payload:

| Payload | Size | Per LoRa frame |
| --- | --- | --- |
| PMC1 contribution | 43 B | 5 (PMB1×5 = 220 B) |
| PMI1 incident | 44 B | 1 |
| Thread update (PMT1) | ≤ 256 B cap, typically ~100 B | 1 |
| Zone digest / PMS1 snapshot | 48 KB / 32 KiB | **never** |

So a LoRa segment is **gossip-only by construction** — the sync family
(PMG1/PMD1/PMQ1/PMS1/PMN1) is structurally absent, `request()` resolves
null, and anti-entropy simply does not exist on the air. Ephemerality
completes the fit: nothing lives past its TTL, so there is no history to
transfer.

Capacity is the honest constraint. A duty-cycled channel sustains a few
hundred bytes per minute *shared*, which at reticent emission rates is
roughly **5–15 contributing vehicles per region**. Useless for a dense
city; exactly right for the places Meshtastic exists for — rural
corridors, mountain roads, disaster areas where every tower is down. And
there the centralized alternative is not expensive; it is **absent**.

## 16.1 The profile

`LORA_CONSTANTS` applies three overrides, all inside the signed
bootstrap's ±4× tunable envelope — a LoRa deployment is an ordinary
bootstrap, not a protocol fork:

| Constant | Default → LoRa | Why |
| --- | --- | --- |
| `MAX_AGE_RECEIPT` | 45 → 90 s | multi-hop radio latency is real |
| `MAX_FUTURE_SKEW` | 15 → 45 s | radio nodes keep worse clocks |
| `EMIT_INTERVAL` | 15 → 60 s | airtime is the scarce resource |

`MAX_AGE_RECEIPT` deliberately stops at 90 s: the FRESHNESS table zeroes
there and `CONTRIB_TTL` is wire-capped at 90, so older contributions are
dead weight. The consequence is stated rather than hidden: **LoRa is
primarily an incident and thread medium** (whose TTLs are minutes), with
traffic contributions best-effort.

## 16.2 The phone-side transport

`createLoraNetwork(radio)` implements the five-verb MeshNetwork over a
duck radio (`maxPayload`, `send`, `onReceive`, optional `peers`), so the
real Meshtastic binding is a thin shim over `@meshtastic/js`:

```js
// Sketch: bind a Meshtastic device as a duck radio.
const device = new MeshDevice(/* BLE/serial/HTTP connection */);
const radio = {
  maxPayload: 237,
  send: bytes => device.sendPacket(bytes, { portNum: PRIVATE_APP }),
  onReceive: cb => device.events.onFromRadio.subscribe(packet => {
    if (packet?.payloadVariant?.case === "packet") {
      const p = packet.payloadVariant.value;
      if (p.payloadVariant?.case === "decoded") cb(p.payloadVariant.value.payload, p.from);
    }
  })
};
const network = createLoraNetwork(radio, { bytesPerMinute: 240 });
const node = new MeshNode({ ..., constants: LORA_CONSTANTS, network });
```

Design decisions, each with its reason:

- **Topics are derived, not transmitted.** A radio channel has no
  topics — the RF footprint is the scope — so the adapter derives each
  inbound record's topic from its own cell, exactly as a publisher
  would. Rule 8 holds by construction here; its work is done by physics.
- **Radio senders are `lora:<id>` pseudo-peers, not §5.4 identities.**
  Meshtastic broadcast sender ids are spoofable, so no bond can bind to
  them. `admitRadioPeer(id)` marks them accepted; they are individually
  rate-limited, trust-tracked, and revocable by the ordinary forfeiture
  path. The honest scope: an RF-adjacent attacker who could abuse this
  could equally jam the channel — the pseudo-peer is bookkeeping, and
  the *security boundary is the bridge*.
- **Airtime buys incidents first.** Outbound frames queue with
  priorities — PMI1, then thread frames, then contributions — and drain
  inside a `bytesPerMinute` budget on `flush()`. Overflow evicts the
  lowest priority: a hazard is never dropped to make room for
  statistics. PMA1, PMX1, and the whole sync family never cross the air.
- **The budget is bytes, not modeled airtime.** LoRa time-on-air depends
  on spreading factor, bandwidth, and regional duty law; a physical
  model would be false precision. Operators tune `bytesPerMinute` to
  their preset (240 B/min ≈ 1% duty at LongFast-class rates).

## 16.3 The bonded bridge

`createLoraBridge({ node, radio })` joins a radio segment to the IP
mesh, and it is the §5.4 hop-vouching model doing exactly what it was
built for — composing dissimilar networks:

- **Uplink.** Every record arriving off the air is validated against the
  bridge's own static map (rules 1–12 are transport-independent).
  Survivors are republished under the bridge's own bond — its trust and
  admission are what stand behind them, precisely as the §4.5 forwarder
  and the snapshot provider already work. The IP side never needs to
  know LoRa exists. Radio senders that repeatedly fail *provable* rules
  are muted locally (`strikeLimit`, `muteMillis`).
- **Downlink.** Incidents accepted on the IP side are queued for the air
  — a hazard matters most where there is no coverage. Contributions are
  not downlinked: at radio latency they are dead weight for anyone not
  already adjacent.
- **Threads shuttle opaquely.** PMT-family frames are end-to-end
  authenticated and sealed to their audience, so the bridge carries mail
  it cannot read: uplink always (a bus out of coverage keeps its thread
  alive through any bridge), downlink only for thread links the operator
  explicitly holds (`threadLinks` — its own fleet, where holding the
  link is the operator's right anyway).

Notably, per-record proof-of-work would have made all of this
impossible: no microcontroller-adjacent device could mine, and no
self-contained proof could be checked without one. Deleting it in favour
of hop-vouched admission (§5.4) is what made heterogeneous meshes
composable.

## 16.4 What is proven and what is not

Proven, against simulated radio physics (payload cap, broadcast
flooding, budget windows): records flow phone→air→bridge→IP and
validate end to end; the bridge vouches nothing it did not validate and
mutes provable liars; incidents outrank statistics for airtime; a
sealed thread published off-grid reaches its audience on the IP side
and verifies under the thread key; the operator-held-link downlink
airs thread frames back to the radio side.

Not proven: anything against real radios. Time-on-air, flood-storm
behaviour on busy channels, BLE pairing latency, and Meshtastic's own
duplicate suppression are all assumptions until two actual devices sit
on a desk. The duck-radio seam exists precisely so that experiment is a
~20-line shim away.

## 16.5 Status

- [x] §16.1 profile inside the bootstrap envelope
- [x] §16.2 phone-side transport (gossip-only, priorities, budget)
- [x] §16.3 bonded bridge (validated vouching, incident downlink,
      opaque thread shuttle, held-link downlink)
- [ ] Real-hardware pilot: two Meshtastic devices + one bridge
      (the shim in §16.2 is the entire remaining integration)
