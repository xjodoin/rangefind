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
//
// The pipeline itself now lives in ./session.js, which the web demo and
// keepers share. What stays here is the mobile *defaults*: the reticent
// profile, contribution off until asked, and the battery gate — the three
// decisions that are about phones rather than about the protocol.

import { DEFAULT_CONSTANTS } from "./bins.js";
import { checkMeshHost, createMeshSession } from "./session.js";

// An app host needs more than the contributor: the session it wires a map
// and a settings screen to, the incident taxonomy its report sheet is
// built from, a loopback network for running the whole thing with no
// infrastructure, and the corridor simulator that makes a live-traffic
// feature testable on a device before any peers exist.
export { DEFAULT_CONSTANTS } from "./bins.js";
export { checkMeshHost, congestionLevel, createMeshSession } from "./session.js";
export { createCorridorTraffic } from "./simulate.js";
export { createLoopbackNetwork } from "./node.js";
export { INCIDENT_TYPES } from "./incidents.js";

/** Capabilities a host must provide before any of this can run. */
export function checkMobileHost() {
  return checkMeshHost();
}

/**
 * A contributor bound to a running engine and (optionally) a mesh.
 *
 * - `engine`: an open route graph. Its `root.sourceHash` is the epoch,
 *   its `snap()` is what turns a GPS fix into a segment id, and its
 *   `cellFacts()` is what lets this device validate what it receives.
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
  readOnly = false,
  ...rest
} = {}) {
  // The session *is* the mobile surface: `node`, `contributor`,
  // `onLocation`, `followRoute`, `provider()`, `setContributing`,
  // `stats`, `host` and `epoch` are all on it, alongside the display and
  // incident calls a nav app needs.
  return createMeshSession({
    engine, network, id, profile, constants, batteryLevel, charging,
    onEmit, clock, transport, contribute, readOnly, ...rest
  });
}
