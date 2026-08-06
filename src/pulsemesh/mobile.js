// PulseMesh for mobile hosts (pulsemesh.md delta 4).
//
// The design is explicit that browsers are consumers and apps are the
// realistic sustained contributor: a backgrounded tab loses
// watchPosition, screen-off ends reporting, and neither produces the
// continuous fixes a traffic layer needs. This module is the app side —
// a GPS fix goes in, a validated contribution goes out, and the rules
// that decide *whether* to emit are the protocol's, not the app's.
//
// It deliberately owns no transport and no permissions. The host supplies
// fixes (from Core Location, Android's LocationManager, a React Native
// module, whatever), and supplies a network if it wants to gossip; this
// module is the pipeline between them, so the same code runs under
// Hermes, in a WebView, or in Node.

import { DEFAULT_CONSTANTS, detailCellForE7, zoneOfDetailCell } from "./bins.js";
import { MeshNode } from "./node.js";
import { createContributor } from "./contribute.js";
import { createReticentProfile } from "./reticent.js";
import { aggregateSegment } from "./aggregate.js";
import { parseSegment } from "./codec.js";
import { PROOF_BOND } from "./codec.js";

/** Capabilities a host must provide before any of this can run. */
export function checkMobileHost() {
  const missing = [];
  if (typeof globalThis.crypto?.getRandomValues !== "function") missing.push("crypto.getRandomValues");
  if (typeof TextEncoder === "undefined") missing.push("TextEncoder");
  // Only the thread channel needs subtle; the traffic channel does not,
  // so it is reported separately rather than as a hard requirement.
  const threadsAvailable = typeof globalThis.crypto?.subtle?.importKey === "function";
  return { ok: missing.length === 0, missing, threadsAvailable };
}

/**
 * A contributor bound to a running engine and (optionally) a mesh.
 *
 * - `engine`: an open route graph. Its `root.sourceHash` is the epoch,
 *   and its `snap()` is what turns a GPS fix into a segment id.
 * - `network`: any MeshNetwork (libp2p, loopback). Omit to run
 *   consume-only, or to collect emissions yourself through `onEmit`.
 * - `profile`: "reticent" (default) or "cadence". Reticent is mandatory
 *   for unpublished routes (threads §10 rule 3) and is the safer default
 *   for a phone, whose owner did not sign up to publish a trajectory.
 * - `batteryLevel` / `charging`: §10.1 rule 5 — contribution pauses below
 *   20% unless charging. A traffic layer that flattens a phone loses the
 *   contributor permanently.
 */
export async function createMobileMesh({
  engine,
  network = null,
  id = "mobile",
  profile = "reticent",
  constants = DEFAULT_CONSTANTS,
  batteryLevel = null,
  charging = null,
  onEmit = null,
  clock = Date.now,
  transport = "wire",
  contribute = false,
  // §11.6: consume without ever publishing — no gossip membership, no
  // §5.4 bond, everything pulled over the sync path on tick(). The right
  // mode for a browser at home that only wants to *see* traffic.
  readOnly = false
} = {}) {
  if (readOnly && contribute) throw new Error("readOnly and contribute are mutually exclusive.");
  const host = checkMobileHost();
  if (!host.ok) {
    throw new Error(`PulseMesh needs ${host.missing.join(", ")} on this host.`);
  }
  const epochHex = engine.root.sourceHash;

  // Deterministic, shared cell placement from the static index: the z15
  // cell of the leaf's bbox centre. Every peer derives the same cell for
  // the same record without coordinating.
  const cellOf = record => {
    const bbox = engine.root.leaves[record.leafCell]?.bbox;
    if (!bbox) return null;
    return detailCellForE7((bbox.minLat + bbox.maxLat) / 2, (bbox.minLon + bbox.maxLon) / 2);
  };
  const cellContext = leafCell => (engine.root.leaves[leafCell]
    ? { polylineCount: 1 << 20, classOf: () => "secondary", metersOf: () => null }
    : null);

  const node = network
    ? new MeshNode({ id, epochHex, constants, cellOf, cellContext, network, clock, transport, readOnly })
    : null;

  // The reticent profile needs to know what the mesh currently expects on
  // a segment and whether anyone else is reporting it — both of which the
  // node already holds, because a contributor is also a consumer.
  const reticent = profile === "reticent"
    ? createReticentProfile({
        constants,
        clock,
        expectedBinOf: segKey => {
          if (!node) return NaN;
          const aggregate = aggregateSegment(node.store.contributionsForSegment(segKey), {
            nowMillis: clock(), constants
          });
          return aggregate && aggregate.confidence >= constants.SURPRISE_CONFIDENCE
            ? aggregate.speedBin
            : NaN;
        },
        companyCountOf: segKey => (node ? node.store.contributionsForSegment(segKey).length : 0),
        forwarderPool: () => (network?.peersOf ? network.peersOf(id).filter(peer => peer !== id) : [])
      })
    : "cadence";

  const stats = { fixes: 0, emitted: 0, suppressed: 0, lastReason: null, zones: 0 };

  const contributor = createContributor({
    epoch32: node?.epoch32 ?? new Uint8Array(32),
    epochPrefix8: node?.epochPrefix8 ?? new Uint8Array(8),
    snap: async fix => {
      const result = await engine.snap({ lat: fix.lat, lon: fix.lon });
      return result?.matches?.[0] ?? null;
    },
    publish: async emission => {
      stats.emitted++;
      if (node) node.publishRecord(emission.record, { forwarder: emission.forwarder });
      if (onEmit) await onEmit(emission);
    },
    constants,
    profile: reticent,
    // §5.4: records are proofless; the transport's admission bond vouches
    // for them. A mobile host that contributes over a real network must
    // mint its bond on the transport (network.mintBond()) — records from
    // an unbonded peer are dropped by every receiver's rule 5.
    proofType: PROOF_BOND,
    clock,
    batteryLevel,
    charging,
    metersOf: segKey => {
      const [leafCell, geomRef] = segKey.split("/").map(Number);
      void leafCell; void geomRef;
      return 0; // filled by the caller's own map data when available
    }
  });

  /**
   * One GPS fix. Contribution is **off unless explicitly enabled**: a
   * phone that merely reads traffic must never start publishing because
   * a library defaulted it on.
   */
  async function onLocation(fix) {
    stats.fixes++;
    if (!contribute) {
      stats.suppressed++;
      stats.lastReason = "contribution disabled";
      return { emitted: false, reason: "contribution disabled" };
    }
    const result = await contributor.handleFix(fix);
    if (!result.emitted) {
      stats.suppressed++;
      stats.lastReason = result.reason;
    }
    return result;
  }

  /** Subscribes to the z9 zones a route crosses. */
  function followRoute(route) {
    if (!node || !route?.edges?.length) return [];
    const zones = new Map();
    for (const edge of route.edges) {
      const { leafCell, geomRef } = parseSegment(edge.segment);
      const cell = cellOf({ leafCell, geomRef });
      if (!cell) continue;
      const zone = zoneOfDetailCell(cell);
      zones.set(`${zone.x}/${zone.y}`, zone);
    }
    const list = [...zones.values()];
    node.subscribeZones(list);
    stats.zones = list.length;
    return list;
  }

  return {
    node,
    contributor,
    onLocation,
    followRoute,
    stats,
    host,
    /** Hand this to `engine.route({ live })`. Null when consume-only. */
    provider: () => node?.provider ?? null,
    /** Turns contribution on or off at runtime, e.g. from a settings toggle. */
    setContributing(value) { contribute = Boolean(value); return contribute; },
    get contributing() { return contribute; },
    get epoch() { return epochHex; }
  };
}
