// Type declarations for rangefind/pulsemesh/bridge — the fleet seed
// (§12.1): who a seed vouches for, and what crosses between its island
// and the wider mesh.

import type { MeshCell, MeshNode, PulseMeshConstants } from "./pulsemesh.js";

/**
 * `off` — an island: nothing crosses in either direction.
 * `in`  — receive only: nothing the fleet publishes leaves the seed. The
 *         upstream node is a §11.6 read-only node (no bond, no gossip
 *         membership, pull-only) while the island node stays a normal
 *         bonded peer downward.
 * `both`— validated island records are republished upstream under the
 *         seed's own bond, and upstream records are gossiped down.
 */
export type BridgePolicy = "off" | "in" | "both";
export declare const BRIDGE_POLICIES: readonly ["off", "in", "both"];

/**
 * Admit every peer directly connected to the island host. Reaching a
 * fleet seed requires its address, and that address travels only inside
 * sealed tickets (threads §20.10) — so the connection IS the admission,
 * and no device-card ↔ peerId binding is created (which would let a
 * fleet de-anonymise its own drivers' traffic records).
 */
export declare const ADMIT_CONNECTED: "connected";

export interface FleetBridgeStats {
  /** Peers admitted by connection (or allowlist) since start. */
  admitted: number;
  /** Admissions withdrawn: disconnected, or forfeited under §8.4. */
  revoked: number;
  refusedMuted: number;
  upCrossed: number; upOutOfZone: number; upRefused: number;
  downCrossed: number; downOutOfZone: number; downRefused: number;
}

export interface FleetBridge {
  readonly policy: BridgePolicy;
  readonly stats: FleetBridgeStats;
  /** Admit currently connected island peers, release the ones that left. */
  admitConnected(nowMillis?: number): number;
  admittedPeers(): string[];
  isAdmitted(peerId: string): boolean;
  /** Peers this seed forfeited on first-hand evidence (rules 10–12, §8.4). */
  mutedPeers(nowMillis?: number): string[];
  /** One maintenance beat; call beside node.tick(). */
  tick(nowMillis?: number): number;
  close(): void;
}

export declare function createFleetBridge(options: {
  /** The MeshNode facing the fleet: ordinary, bonded, zone-subscribed. */
  island: MeshNode;
  /** The MeshNode facing the wider mesh; required unless policy is "off". */
  upstream?: MeshNode | null;
  policy?: BridgePolicy;
  /** Pinned zones. Bridging is confined to them in BOTH directions. */
  zones?: MeshCell[];
  /** null (admit nobody), ADMIT_CONNECTED, or an explicit peerId allowlist. */
  admit?: typeof ADMIT_CONNECTED | true | string[] | null;
  /** The island host's currently connected peers. */
  islandPeers?: (() => string[]) | null;
  constants?: Readonly<PulseMeshConstants> | null;
  clock?: (() => number) | null;
}): FleetBridge;
