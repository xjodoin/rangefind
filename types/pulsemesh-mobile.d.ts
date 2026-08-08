import type { MeshNetwork, MeshNode, PulseMeshConstants, PulseMeshProvider } from "./pulsemesh.js";
import type { MeshSession } from "./pulsemesh-session.js";

// An app host needs the whole session, plus the incident taxonomy its
// report sheet is built from and the simulator that makes a live-traffic
// feature testable before any peers exist.
export * from "./pulsemesh-session.js";
export { INCIDENT_TYPES } from "./pulsemesh.js";
export { createLoopbackNetwork } from "./pulsemesh.js";

/** What a host must provide before PulseMesh can run on it. */
export declare function checkMobileHost(): {
  ok: boolean;
  missing: string[];
  /** The traffic channel does not need `crypto.subtle`; threads do. */
  threadsAvailable: boolean;
};

/** @deprecated The session is the surface; kept for older hosts. */
export interface MobileMesh {
  node: MeshNode | null;
  contributor: unknown;
  /** One GPS fix. Refuses unless contribution was explicitly enabled. */
  onLocation(fix: {
    lat: number; lon: number; speedMps?: number; courseDeg?: number; nowMillis?: number;
  }): Promise<{ emitted: boolean; reason?: string }>;
  /** Subscribes to the z9 zones a route crosses. */
  followRoute(route: { edges?: Array<{ segment: string }> }): Promise<Array<{ x: number; y: number }>>;
  stats: { fixes: number; emitted: number; suppressed: number; lastReason: string | null; zones: number };
  host: ReturnType<typeof checkMobileHost>;
  /** Hand to `engine.route({ live })`. Null when consume-only. */
  provider(): PulseMeshProvider | null;
  setContributing(value: boolean): boolean;
  readonly contributing: boolean;
  readonly epoch: string;
}

/**
 * The app-side contributor. Browsers are consumers; a phone with
 * background location is the realistic sustained contributor, so this is
 * where continuous fixes become validated contributions.
 *
 * Contribution defaults to **off** — a phone that only reads traffic must
 * never start publishing because a library defaulted it on.
 */
export declare function createMobileMesh(options: {
  engine: { root: { sourceHash: string; leaves: unknown[] }; snap(point: unknown): Promise<unknown> };
  network?: MeshNetwork | null;
  id?: string;
  /** "reticent" (default on mobile) or "cadence". */
  profile?: "reticent" | "cadence";
  constants?: Readonly<PulseMeshConstants>;
  /** §10.1 rule 5: contribution pauses below 20% unless charging. */
  batteryLevel?: (() => number) | null;
  charging?: (() => boolean) | null;
  onEmit?: ((emission: unknown) => unknown) | null;
  clock?: () => number;
  transport?: "wire" | "loopback";
  contribute?: boolean;
  /** §11.6: consume-only over the pull path; no bond, no gossip membership. */
  readOnly?: boolean;
}): Promise<MeshSession>;
