// Type declarations for rangefind/route (rfroutegraph-v1 query engine).

export interface RoutePoint {
  lat: number;
  lon?: number;
  lng?: number;
}

export interface RouteGraphIo {
  readFile(path: string): Promise<Uint8Array>;
  readRange(path: string, offset: number, length: number): Promise<Uint8Array>;
  counters?: { requests: number; bytes: number; files: Set<string> };
  resetCounters?(): void;
}

export interface OpenRouteGraphOptions {
  io: RouteGraphIo;
  /** Gzip inflate override; defaults to DecompressionStream. */
  inflate?: (bytes: Uint8Array) => Uint8Array | Promise<Uint8Array>;
  /** SHA-256 verification of fetched objects; defaults to on when crypto.subtle exists. */
  verifyChecksums?: boolean;
  /** Reject snaps whose nearest road is farther than this; default 250. */
  maxSnapMeters?: number;
}

export interface RouteFetchStats {
  objectFetches: number;
  bytesFetched: number;
  cellFetches: number;
  overlayFetches: number;
  unpackCellFetches: number;
  /** Actual merged Range requests after same-file coalescing. */
  httpRequests: number;
  shardsTouched: string[];
}

export interface RouteEndpointInfo {
  snapped: { lat: number; lon: number };
  snapDistanceMeters: number;
}

export interface RouteStep {
  name: string;
  meters: number;
  seconds: number;
  /** Index into `geometry` where this step begins (for per-street slices). */
  at?: number;
  /**
   * Posted limit in km/h for the majority of this step, 0 when the ways carry
   * no `maxspeed` tag. This is the sign, not the modelled speed: `seconds`
   * also absorbs surface degradation and junction penalties.
   */
  speedLimitKmh?: number;
}

/** 1 traffic signals, 2 stop, 3 give way, 4 level crossing, 5 crossing. */
export interface RouteJunction {
  kind: number;
  lat: number;
  lon: number;
  /** Distance from the route start, meters. */
  atMeters: number;
}

export interface RouteEdgeRef {
  /** Leaf cell id — with `edge`, a stable per-build edge identity. */
  leaf: number;
  edge: number;
  /**
   * Physical directed-segment id "leaf/polyline/direction": shared by all
   * approach copies of one road edge, stable for this build epoch. The
   * key live-traffic feeds should report against.
   */
  segment: string;
  seconds: number;
  meters: number;
  /** Posted limit in km/h, 0 when untagged. */
  speedLimitKmh?: number;
}

export interface RouteResult {
  seconds: number;
  /** Name of the time bucket this result was computed under. */
  bucket: string;
  settledNodes: number;
  from: RouteEndpointInfo;
  to: RouteEndpointInfo;
  stats: RouteFetchStats;
  /** Present unless `geometry: false`. */
  distanceMeters?: number;
  /** [lat, lon] pairs, present unless `geometry: false`. */
  geometry?: Array<[number, number]>;
  /** Consecutive same-street groups, present unless `geometry: false`. */
  steps?: RouteStep[];
  /** Stable per-edge refs, present unless `geometry: false`. */
  edges?: RouteEdgeRef[];
  /** Signals, stops, and crossings along the route, present with geometry. */
  junctions?: RouteJunction[];
  /**
   * Instant sketch polyline (snapped endpoints through traversed leaf-cell
   * centers), available before path unpacking; render it immediately and
   * replace with `geometry` when it arrives.
   */
  coarseGeometry?: Array<[number, number]>;
  /** ETA under the live metric (liveWeights re-rank or `live` search). */
  adjustedSeconds?: number;
  /** Application report when a `live` provider was consulted. */
  live?: { provider: string; states: number; applied: number; error: string | null };
  /** Diverging candidate routes, when `alternatives` was requested. */
  alternatives?: RouteResult[];
}

export interface LiveWeights {
  /** Must equal the index root's sourceHash when provided. */
  epoch?: string;
  /** Per-edge time multipliers keyed by "leaf/edgeIndex" or physical segment id. */
  factors: Record<string, number>;
}

/**
 * One ephemeral traffic state for a physical directed segment
 * ("leaf/polyline/direction", as exposed on RouteEdgeRef.segment and
 * SnapMatch.segment). Either `factor` (time multiplier over the static
 * weight) or `speedMps` + `meters` must be provided; `closed: true` makes
 * the segment impassable and must only be set for verified closures.
 */
export interface LiveSegmentState {
  segment: string;
  factor?: number;
  speedMps?: number;
  meters?: number;
  /** 0..1; decays with observedAt age and blends toward the static cost. */
  confidence?: number;
  observedAt?: number;
  closed?: boolean;
  penaltySeconds?: number;
}

/**
 * Pluggable source of ephemeral traffic states: a P2P mesh (PulseMesh), a
 * CDN-published delta sidecar, a municipal feed, or an in-memory test
 * loopback. `areas` is a hint (the query's endpoint contexts); providers
 * may return states anywhere — the engine fetches the referenced cells
 * (capped) and suppresses stale shortcuts through them, making closures
 * and jams exact under the live metric. Must return [] for unknown epochs.
 * Failures degrade the query to the static metric.
 */
export interface LiveTrafficProvider {
  name?: string;
  fetch(query: {
    epoch: string;
    areas: Array<{ leaf: number; bbox: { minLat: number; maxLat: number; minLon: number; maxLon: number } }>;
    maxAgeSeconds: number;
  }): LiveSegmentState[] | Promise<LiveSegmentState[]>;
}

export interface LiveProviderSpec {
  provider: LiveTrafficProvider;
  maxAgeSeconds?: number;
}

export interface RouteParams {
  from: RoutePoint;
  to: RoutePoint;
  /** Skip path unpacking (matrix/ranking workloads). Default true. */
  geometry?: boolean;
  /** Skip loading the street-name sidecar. */
  names?: boolean;
  /** Time bucket by name; overrides departureTime. */
  bucket?: string;
  /** Departure time matched against bucket rules. */
  departureTime?: string | number | Date;
  /** Compute up to this many diverging alternatives (max 3). */
  alternatives?: number;
  /** Re-rank candidates and adjust ETAs with fresh per-edge factors. */
  liveWeights?: LiveWeights;
  /**
   * Live-traffic source consulted during the search itself: the route is
   * chosen under the live metric (closures impassable, jams costed),
   * exact wherever the provider reports and static elsewhere.
   */
  live?: LiveTrafficProvider | LiveProviderSpec;
  /** Called with the coarse route as soon as the search finishes. */
  onCoarseRoute?: (coarse: { seconds: number; geometry: Array<[number, number]>; bucket: string }) => void;
}

export interface MatrixResult {
  /** seconds[i][j] = travel time from points[i] to points[j]. */
  seconds: number[][];
  stats: RouteFetchStats;
}

export interface ItineraryLeg extends RouteResult {
  fromStop: number;
  toStop: number;
}

export interface ItineraryResult {
  /** Stop indices in optimized visit order. */
  order: number[];
  legs: ItineraryLeg[];
  totalSeconds: number;
  totalMeters: number;
  stats: RouteFetchStats;
}

export interface SnapMatch {
  leaf: number;
  edgeIndex: number;
  /** Physical directed-segment id — the key contribution pipelines report. */
  segment: string;
  fromNode: number;
  toNode: number;
  weight: number;
  distMeters: number;
  ratio: number;
  snappedLatE7: number;
  snappedLonE7: number;
}

export interface SnapResult {
  latE7: number;
  lonE7: number;
  matches: SnapMatch[];
}

export interface RouteGraphEngine {
  root: unknown;
  io?: RouteGraphIo;
  route(params: RouteParams): Promise<RouteResult>;
  matrix(params: {
    points: RoutePoint[];
    bucket?: string;
    departureTime?: string | number | Date;
    live?: LiveTrafficProvider | LiveProviderSpec;
    /** Force k^2 point-to-point routes instead of shared-context one-to-many searches. */
    pairwise?: boolean;
  }): Promise<MatrixResult>;
  itinerary(params: {
    stops: RoutePoint[];
    roundTrip?: boolean;
    geometry?: boolean;
    bucket?: string;
    departureTime?: string | number | Date;
    live?: LiveTrafficProvider | LiveProviderSpec;
  }): Promise<ItineraryResult>;
  snap(point: RoutePoint, options?: {
    maxCandidates?: number;
    extraMeters?: number;
    maxSnapMeters?: number;
  }): Promise<SnapResult>;
  stats(): RouteFetchStats;
  resetStats(): void;
  clearCaches(): void;
}

export declare function openRouteGraph(options: OpenRouteGraphOptions): Promise<RouteGraphEngine>;

export declare function createRouteGraphHttpIo(baseUrl: string, options?: {
  fetch?: typeof fetch;
}): RouteGraphIo;

export declare function openRouteGraphUrl(baseUrl: string, options?: Partial<OpenRouteGraphOptions> & {
  fetch?: typeof fetch;
}): Promise<RouteGraphEngine>;

/**
 * Reference LiveTrafficProvider serving a fixed (or lazily computed) state
 * list — for tests, demo loopbacks, and pre-fetched sidecar payloads.
 */
export declare function createStaticLiveProvider(
  states: LiveSegmentState[] | ((query: { epoch: string }) => LiveSegmentState[]),
  options?: { name?: string; epoch?: string }
): LiveTrafficProvider;
