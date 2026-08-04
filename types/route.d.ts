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
  seconds: number;
  meters: number;
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
  /** ETA re-ranked with liveWeights, when provided. */
  adjustedSeconds?: number;
  /** Diverging candidate routes, when `alternatives` was requested. */
  alternatives?: RouteResult[];
}

export interface LiveWeights {
  /** Must equal the index root's sourceHash when provided. */
  epoch?: string;
  /** Per-edge time multipliers keyed by "leaf/edgeIndex". */
  factors: Record<string, number>;
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
    /** Force k^2 point-to-point routes instead of shared-context one-to-many searches. */
    pairwise?: boolean;
  }): Promise<MatrixResult>;
  itinerary(params: {
    stops: RoutePoint[];
    roundTrip?: boolean;
    geometry?: boolean;
    bucket?: string;
    departureTime?: string | number | Date;
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
