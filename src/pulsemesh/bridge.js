// PulseMesh fleet bridge (§12.1): joins one fleet's island to the wider
// mesh, and lets that fleet's bond-less devices contribute at all.
//
// The shape is §16.3's LoRa bridge with a different lower half. There,
// radio senders mint nothing because airtime is their admission and the
// bridge is the security boundary; here, courier phones mint nothing
// because a §5.4 bond is a 256 MiB memory-hard solve — affordable once
// per peer per day on a desktop, and explicitly refuted as a phone-side
// Sybil defence (benchmarks §14) — and the fleet seed is the security
// boundary. Both bridges then do the same three things: validate what
// arrives against their OWN static map, republish survivors under their
// OWN bond, and stop listening to a provable liar.
//
// **What admits a driver: being directly connected to the seed.**
// Reaching the seed requires its address, and that address travels only
// inside sealed tickets (threads §20.10, PMK1 flags bit 1) to devices the
// dispatcher awarded a job to. So the set of peers that can dial a fleet
// seed already *is* the fleet, enforced by the seal rather than by a
// roster this file would have to hold. Two consequences worth stating:
//
//   - There is NO binding here between a device card and a libp2p
//     peerId. Building one would let a fleet join its own drivers'
//     traffic records to their identities, which is exactly the linkage
//     §10.2 spends the whole contributor pipeline preventing. A roster
//     would buy the seed nothing it does not already get from the seal.
//   - Admission is per-connection and nothing else. `admitConnected`
//     writes directly-connected peers into `island.bondedPeers` — the
//     same explicit, visible move `admitRadioPeer` makes — and takes
//     them back out when they disconnect, when the node forfeits them
//     (§8.4), or when an allowlist excludes them.
//
// **The incentive is the design.** A record the seed republishes is
// vouched by the SEED's bond, so a driver publishing junk spends the
// fleet's admission, not their own: remote peers penalize the seed, and
// at the trust floor they revoke it and the whole fleet loses reach.
// Policing therefore sits with the party that can actually do it — the
// operator who knows which van is which — instead of with strangers who
// can only see a peer id.
//
// **Two nodes, not one.** A bridged seed runs two MeshNodes on two
// libp2p hosts: one facing the island (it listens; its address is the
// seed card) and one facing the wider network (it dials `--bootstrap`).
// That is not ceremony. GossipSub forwards a message to its mesh peers on
// receipt, before and regardless of any application-level validation, so
// a single host with a foot in both meshes launders whatever it relays
// under its own connection — the receiving peer sees `propagationSource`
// = the seed, which is bonded, and rule 5 is satisfied by a record the
// seed never checked. Splitting the hosts is what makes "validate, then
// vouch" a fact about the transport rather than a claim in a comment.

import { DEFAULT_CONSTANTS } from "./bins.js";

/** `off` — nothing crosses; `in` — receive only; `both` — full bridge. */
export const BRIDGE_POLICIES = Object.freeze(["off", "in", "both"]);

/** Admit every peer that is directly connected to the island host. */
export const ADMIT_CONNECTED = "connected";

function zoneKeyOf(cell) {
  return `${cell.x >> 6}/${cell.y >> 6}`;
}

/**
 * Wires a fleet seed.
 *
 * - `island`   — the MeshNode facing the fleet. Ordinary, bonded, and
 *                subscribed to the pinned zones: the island has to keep
 *                working whatever the bridge policy is.
 * - `upstream` — the MeshNode facing the wider mesh, or null for `off`.
 *                In `in` it is a §11.6 read-only node (see below).
 * - `policy`   — one of BRIDGE_POLICIES.
 * - `zones`    — the pinned zones. Bridging is confined to them in BOTH
 *                directions; an unzoned bridge on a planet-scale mesh
 *                would pull the planet through a depot's uplink.
 * - `admit`    — null (admit nobody: a plain §12 keeper on a public mesh
 *                must not hand its bond to anyone who dials it),
 *                ADMIT_CONNECTED, or an explicit array of peerIds for
 *                fleets that want the stricter thing.
 * - `islandPeers()` — the island host's currently connected peers.
 *
 * **How read-only-upstream composes with bonded-downward.** §11.6 is a
 * whole-node property — no bond, no gossip topics, publishRecord throws —
 * and a seed that adopted it wholesale would stop serving its own island,
 * which is the one thing it exists to do. Two nodes resolve it without
 * touching the mode: the upstream node carries `readOnly: true` (mints
 * nothing, joins no upstream topic, pulls PMG1→PMQ1→PMS1 on tick from
 * bonded upstream peers) while the island node stays a normal bonded
 * peer that publishes, gossips and vouches downward. "Read-only" then
 * describes a direction, because a direction is what each node is.
 */
export function createFleetBridge({
  island,
  upstream = null,
  policy = "off",
  zones = [],
  admit = null,
  islandPeers = null,
  constants = null,
  clock = null
} = {}) {
  if (!island) throw new Error("A fleet bridge needs its island-facing MeshNode.");
  if (!BRIDGE_POLICIES.includes(policy)) {
    throw new Error(`Unknown bridge policy ${policy} (expected ${BRIDGE_POLICIES.join("|")}).`);
  }
  if (policy !== "off" && !upstream) {
    throw new Error(`A --bridge=${policy} seed needs its upstream-facing MeshNode.`);
  }
  if (policy === "both" && upstream?.readOnly) {
    throw new Error("A --bridge=both seed publishes upstream, so its upstream node cannot be read-only (§11.6).");
  }
  const now = clock ?? island.clock;
  const limits = constants ?? island.constants ?? DEFAULT_CONSTANTS;
  const zoneKeys = new Set(zones.map(zone => `${zone.x}/${zone.y}`));
  const allowlist = Array.isArray(admit) ? new Set(admit.map(String)) : null;
  const admitAll = admit === ADMIT_CONNECTED || admit === true;
  // Peers WE admitted, so a disconnect never evicts a peer that presented
  // a real PMA1 of its own — those two live in the same map and only this
  // set can tell them apart.
  const admitted = new Set();
  const stats = {
    admitted: 0, revoked: 0, refusedMuted: 0,
    upCrossed: 0, upOutOfZone: 0, upRefused: 0,
    downCrossed: 0, downOutOfZone: 0, downRefused: 0
  };

  function admissible(peerId, nowMillis) {
    if (allowlist) return allowlist.has(peerId);
    if (!admitAll) return false;
    void nowMillis;
    return true;
  }

  /**
   * §8.4 forfeiture IS the mute. On a radio the bridge had to keep its own
   * strike map because a sender id is a spoofable broadcast field; here
   * the delivering peer is the connection's authenticated peerId, so the
   * node's ordinary machinery already does the whole job — two provable
   * rule 10–12 failures floor the peer's trust (1000 → 500 → 250), which
   * revokes its admission, refuses re-registration for the bond's bucket,
   * and gossips PMX1 testimony. All this function has to do is not undo
   * it on the next tick.
   */
  function muted(peerId, nowMillis) {
    const until = island.locallyBanned.get(peerId);
    if (until == null) return false;
    if (nowMillis >= until) {
      island.locallyBanned.delete(peerId);
      return false;
    }
    return true;
  }

  /**
   * Admits the island host's directly-connected peers and releases the
   * ones that left. Idempotent: call it on every peer:connect and on
   * every tick.
   */
  function admitConnected(nowMillis = now()) {
    if (!islandPeers) return 0;
    const connected = new Set(islandPeers().map(String));
    for (const peerId of connected) {
      if (!admissible(peerId, nowMillis)) continue;
      if (muted(peerId, nowMillis)) {
        if (admitted.delete(peerId)) {
          island.bondedPeers.delete(peerId);
          stats.revoked++;
        }
        stats.refusedMuted++;
        continue;
      }
      if (!admitted.has(peerId)) {
        admitted.add(peerId);
        stats.admitted++;
      }
      island.bondedPeers.set(peerId, nowMillis + limits.BOND_LIFETIME * 1000);
    }
    for (const peerId of [...admitted]) {
      if (connected.has(peerId)) continue;
      admitted.delete(peerId);
      island.bondedPeers.delete(peerId);
      stats.revoked++;
    }
    return admitted.size;
  }

  function inZones(node, record) {
    if (!zoneKeys.size) return true; // unzoned: documented as a mistake at scale
    const cell = node.cellOf(record);
    return Boolean(cell) && zoneKeys.has(zoneKeyOf(cell));
  }

  /**
   * Crossing one record. There is deliberately no second validation pass
   * here: the tap fires only from `#acceptContribution`/`#acceptIncident`
   * after rules 1–12 ran against this node's own static map and the
   * record entered the store, so acceptance IS the gate, and a record
   * that failed never reaches this function. Re-running the validator
   * would re-check a record against the same map with the same clock and
   * cost a second rule 6 lookup to reach the same verdict.
   *
   * The loop breaker is the same store: a record we push across is stored
   * on both sides, so when it comes back it is a rule 6 replay, is
   * dropped, and never fires the tap a second time.
   */
  function cross(source, target, record, direction) {
    if (!inZones(source, record)) {
      stats[`${direction}OutOfZone`]++;
      return;
    }
    try {
      target.publishRecord({ bytes: record.bytes });
      stats[`${direction}Crossed`]++;
    } catch {
      // A read-only or closed target refuses; a bridge is best-effort and
      // must never take the node down with it.
      stats[`${direction}Refused`]++;
    }
  }

  // --- uplink: island → validated → vouched under the seed's own bond ---
  const previousIslandTap = island.onRecordAccepted;
  if (policy === "both") {
    island.onRecordAccepted = (record, meta) => {
      previousIslandTap?.(record, meta);
      cross(island, upstream, record, "up");
    };
  }

  // --- downlink: the wider mesh → the fleet's own island ----------------
  const previousUpstreamTap = upstream ? upstream.onRecordAccepted : null;
  if (policy !== "off") {
    upstream.onRecordAccepted = (record, meta) => {
      previousUpstreamTap?.(record, meta);
      cross(upstream, island, record, "down");
    };
  }

  return {
    policy,
    stats,
    admitConnected,
    /** The peers this bridge is currently vouching for. */
    admittedPeers() {
      return [...admitted];
    },
    isAdmitted(peerId) {
      return admitted.has(String(peerId));
    },
    /** Peers this seed has forfeited on first-hand evidence (§8.4). */
    mutedPeers(nowMillis = now()) {
      return [...island.locallyBanned.keys()].filter(peerId => muted(peerId, nowMillis));
    },
    /** One maintenance beat: refresh admissions. Call beside node.tick(). */
    tick(nowMillis = now()) {
      return admitConnected(nowMillis);
    },
    close() {
      island.onRecordAccepted = previousIslandTap;
      if (upstream) upstream.onRecordAccepted = previousUpstreamTap;
      for (const peerId of admitted) island.bondedPeers.delete(peerId);
      admitted.clear();
    }
  };
}
