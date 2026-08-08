import type { MeshNetwork, MeshNode, PulseMeshConstants, PulseMeshProvider } from "./pulsemesh.js";

/** What a host must provide before PulseMesh can run on it. */
export declare function checkMeshHost(): {
  ok: boolean;
  missing: string[];
  /** The traffic channel does not need `crypto.subtle`; threads do. */
  threadsAvailable: boolean;
};

/** How a congestion ratio reads on a map. Display-only thresholds. */
export declare function congestionLevel(
  ratio: number | null
): "stopped" | "heavy" | "slow" | "free" | "unknown";

/** One segment of live traffic, ready to draw. */
export interface TrafficSegment {
  segment: string;
  segKey: string;
  /** The segment's canonical polyline, in travel order. */
  points: Array<[number, number]>;
  speedKmh: number;
  /** The static free-flow speed this is being compared against. */
  freeflowKmh: number | null;
  ratio: number | null;
  level: ReturnType<typeof congestionLevel>;
  /** Independent observations behind the aggregate. */
  reports: number;
  confidence: number;
  hint: boolean;
  ageSeconds: number;
}

/**
 * A scored incident with a position. `tier` is "shown" once enough
 * distinct peers have corroborated it to display as fact — rendering a
 * "hint" identically makes a claim the mesh did not.
 */
export interface MeshIncident {
  key: string;
  segment: string;
  segKey: string;
  type: number;
  typeName: string;
  appliesBoth: boolean;
  /** Informational types never change a route, at any score (§9). */
  informational: boolean;
  lat: number;
  lon: number;
  score: number;
  sources: number;
  reports: number;
  confirms: number;
  refutes: number;
  tier: "shown" | "hint";
  ageSeconds: number;
}

export interface MeshSessionSnapshot {
  available: true;
  epoch: string;
  contributing: boolean;
  readOnly: boolean;
  transport: string;
  profile: "reticent" | "cadence";
  peers: number;
  bondedPeers: number;
  zones: number;
  cells: number;
  warmedLeaves: number;
  records: number;
  segments: number;
  incidentRecords: number;
  fixes: number;
  emitted: number;
  suppressed: number;
  incidents: number;
  lastReason: string | null;
  gossip: {
    accepted: number;
    dropped: number;
    dropsByRule: Record<string, number>;
    snapshotsMerged: number;
    antiEntropyRounds: number;
    cellsRequested: number;
  } | null;
  threadsAvailable: boolean;
}

export interface MeshSession {
  node: MeshNode | null;
  network: MeshNetwork | null;
  contributor: unknown;
  host: ReturnType<typeof checkMeshHost>;
  stats: {
    fixes: number; emitted: number; suppressed: number; incidents: number;
    lastReason: string | null; zones: number; cells: number; warmedLeaves: number;
  };
  readonly epoch: string;

  /** One GPS fix. Publishes nothing unless contribution is enabled — but
   *  always keeps the snapped position fresh, which is what makes an
   *  incident report possible (§10.4). */
  onLocation(fix: {
    lat: number; lon: number; speedMps?: number; courseDeg?: number; nowMillis?: number;
    match?: unknown;
  }): Promise<{ emitted: boolean; reason?: string }>;

  /** Subscribe the corridor's zones, fetch its cells, warm its leaves. */
  followRoute(
    routes: unknown,
    options?: { fetch?: boolean }
  ): Promise<Array<{ x: number; y: number }>>;
  warmLeaves(leaves: number[]): Promise<number>;

  /** §10.4. Refuses unless the caller asserts it showed the disclosure. */
  reportIncident(options: {
    type: number; acknowledgedPublic?: boolean; nowMillis?: number;
  }): Promise<{ emitted: boolean; reason?: string }>;
  /** Confirm (2) or refute (3) an incident this device is passing. */
  answerIncident(options: {
    key: string; polarity: 2 | 3; acknowledgedPublic?: boolean; nowMillis?: number;
  }): Promise<{ emitted: boolean; reason?: string }>;

  traffic(options?: {
    nowMillis?: number; minReports?: number; maxAgeSeconds?: number;
    levels?: string[] | null; limit?: number;
  }): Promise<TrafficSegment[]>;
  incidents(options?: { nowMillis?: number; includeHints?: boolean }): Promise<MeshIncident[]>;

  /** One maintenance round: TTL sweep, anti-entropy, leaf warming. */
  tick(nowMillis?: number): Promise<void>;
  /** Schedules tick() itself. Wrong for an app host — see docs/mobile.md. */
  start(options?: { intervalSeconds?: number }): boolean;
  stop(): void;
  close(): Promise<void>;
  snapshot(): MeshSessionSnapshot | { available: false };

  cellContext(leafCell: number): {
    polylineCount: number;
    classOf(geomRef: number): string | null;
    metersOf(geomRef: number): number | null;
    freeflowKmhOf(geomRef: number): number | null;
  } | null;
  freeflowKmhOf(segKey: string): number | null;

  /** Hand to `engine.route({ live })`. Null when there is no mesh. */
  provider(): PulseMeshProvider | null;
  setContributing(value: boolean): boolean;
  readonly contributing: boolean;
  readonly running: boolean;
}

/**
 * Binds an open route graph to a mesh: the integration boundary every
 * host sits on. Contribution defaults to **off** — a device that only
 * reads traffic must never start publishing because a library defaulted
 * it on.
 */
export declare function createMeshSession(options: {
  engine: unknown;
  network?: MeshNetwork | null;
  id?: string;
  epochHex?: string | null;
  previousEpochHex?: string | null;
  constants?: Readonly<PulseMeshConstants>;
  profile?: "reticent" | "cadence";
  contribute?: boolean;
  /** §11.6: consume-only over the pull path; no bond, no gossip membership. */
  readOnly?: boolean;
  transport?: "wire" | "loopback";
  proofType?: number;
  keeper?: boolean;
  suppressedTypes?: number[];
  clock?: () => number;
  rng?: () => number;
  /** §10.1 rule 5: contribution pauses below 20% unless charging. */
  batteryLevel?: (() => number) | null;
  charging?: (() => boolean) | null;
  onEmit?: ((emission: unknown) => unknown) | null;
  onIncident?: ((result: unknown) => unknown) | null;
  /** Map-matching override; defaults to the engine's own snap(). */
  snap?: ((fix: unknown) => Promise<unknown>) | null;
}): Promise<MeshSession>;

/** Simulated vehicles crawling a real corridor, for demos and testbeds. */
export declare function createCorridorTraffic(options: {
  engine: unknown;
  network: MeshNetwork;
  constants?: Readonly<PulseMeshConstants>;
  count?: number;
  /** Records per vehicle per tick; rule 7 refills at 2/s per peer. */
  span?: number;
  samples?: number;
  jamStart?: number;
  jamLength?: number;
  idPrefix?: string;
  clock?: () => number;
  transport?: "wire" | "loopback";
}): Promise<{
  vehicles: MeshSession[];
  follow(route: unknown): Promise<number>;
  tick(options?: { nowMillis?: number }): Promise<number>;
  stop(): void;
  readonly corridor: unknown[];
  readonly jamCount: number;
}>;
