// Type declarations for rangefind/pulsemesh/lora — PulseMesh over
// LoRa/Meshtastic-class radios (§16): the gossip-only transport profile,
// the phone-side adapter, and the bonded bridge to the IP mesh.

import type { MeshNode, MeshNetwork } from "./pulsemesh.js";
import type { PulseMeshConstants } from "./pulsemesh.js";

/** A minimal duck radio; the real Meshtastic binding is a thin shim. */
export interface DuckRadio {
  /** Usable payload bytes per frame (Meshtastic: ~237). */
  maxPayload: number;
  /** Broadcast one frame on the shared channel. */
  send(bytes: Uint8Array): void;
  /** Register a frame listener: (bytes, fromRadioId). */
  onReceive(cb: (bytes: Uint8Array, fromRadioId?: string | number) => void): void;
  /** Optional: known radio node ids. */
  peers?(): Array<string | number>;
}

/** Bootstrap overrides of the LoRa profile — all inside the ±4× envelope. */
export declare const LORA_PROFILE_OVERRIDES: Readonly<{
  MAX_AGE_RECEIPT: 90;
  MAX_FUTURE_SKEW: 45;
  EMIT_INTERVAL: 60;
}>;
export declare const LORA_CONSTANTS: Readonly<PulseMeshConstants>;
export declare const LORA_MAX_PAYLOAD: 237;

export interface AirStats {
  queued: number; sent: number; sentBytes: number;
  dropOversize: number; dropOverflow: number; dropNeverAired: number;
}

/**
 * Five-verb MeshNetwork over a duck radio, for the phone that is on the
 * radio mesh. Gossip-only: `request()` resolves null, so the sync family
 * is structurally absent. Outbound frames queue with priorities
 * (incidents > threads > contributions) and drain inside the airtime
 * budget on `flush()`.
 */
export declare function createLoraNetwork(radio: DuckRadio, options?: {
  bytesPerMinute?: number;
  queueCap?: number;
  clock?: () => number;
}): MeshNetwork & {
  /** Admit a radio sender as a `lora:<id>` pseudo-peer (airtime is its admission). */
  admitRadioPeer(fromRadioId: string | number | null, untilMillis?: number): void;
  /** Drain the outbound queue inside the budget; returns frames still pending. */
  flush(nowMillis?: number): number;
  stats: { framesIn: number; recordsIn: number; badFrames: number; air: AirStats };
};

/**
 * Joins a radio segment to the IP mesh: validates every record arriving
 * off the air against its own static map, republishes survivors under
 * its own §5.4 vouching, downlinks accepted incidents, and shuttles
 * sealed thread frames it cannot read (downlink only for links the
 * operator explicitly holds).
 */
export declare function createLoraBridge(options: {
  node: MeshNode;
  radio: DuckRadio;
  bytesPerMinute?: number;
  queueCap?: number;
  strikeLimit?: number;
  muteMillis?: number;
  threadLinks?: unknown[];
  cellContext?: unknown;
  clock?: (() => number) | null;
}): Promise<{
  stats: {
    upValidated: number; upVouched: number; upDropped: number; upThreads: number;
    downIncidents: number; downThreads: number; muted: number; air: AirStats;
  };
  /** One maintenance beat: rotate held-link thread topics, drain the air queue. */
  tick(nowMillis?: number): Promise<number>;
  /** Feed IP-side gossip that may belong on the air (held-link thread downlink). */
  onIpGossip(topic: string, payload: Uint8Array): boolean;
  close(): void;
}>;
