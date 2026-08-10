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
  /** Largest byte gap merged into one route-pack Range request; default 256 KiB. */
  rangeMergeGapBytes?: number;
  /** Hard cap for one merged route-pack Range request; default 4 MiB. */
  rangeMaxMergedBytes?: number;
  /** Hard cap for bytes fetched between requested objects; default 1 MiB. */
  rangeMaxOverfetchBytes?: number;
  /** Hard cap for merged bytes / exact object bytes; default 2.5. */
  rangeMaxOverfetchRatio?: number;
}

export interface RouteFetchStats {
  objectFetches: number;
  bytesFetched: number;
  /** Bytes transferred by merged pack ranges, including bounded gaps. */
  rangeBytesFetched: number;
  /** Portion of rangeBytesFetched intentionally fetched between objects. */
  rangeOverfetchBytes: number;
  cellFetches: number;
  overlayFetches: number;
  unpackCellFetches: number;
  /** Actual merged Range requests after same-file coalescing. */
  httpRequests: number;
  /** Per-neighbor portal range requests for a federated route. */
  portalRequests?: number;
  /** Actual compressed portal bytes transferred for a federated route. */
  portalBytesFetched?: number;
  shardsTouched: string[];
  /** Regional graph ids touched by a federated route. */
  regionsTouched?: string[];
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
  /**
   * The road's own number as written on the sign — "40", "A 13". A motorway's
   * *name* is the one thing never posted, so guidance that reads the name
   * announces roads by a label the driver cannot see.
   */
  ref?: string;
  /** Exit number off the green panel — "32", "89-N". */
  exitRef?: string;
  /**
   * Where a slip road leads, numbered and with its cardinal — "20 Est;30".
   * This is where a direction comes from in practice: OSM tags `direction` on
   * a handful of route relations and on thousands of ramps.
   */
  destinationRef?: string;
  /** The places named on the sign — "Montréal;Québec". Semicolon-separated. */
  destination?: string;
  /**
   * Whether this step runs inside a roundabout. The arcs of one circle
   * collapse into a single step: a roundabout is one maneuver, and its turn
   * angles describe the curve rather than where the driver ends up.
   */
  roundabout?: boolean;
  /** Which exit of that roundabout the route leaves by; 0 when unknown. */
  roundaboutExit?: number;
  /**
   * How many *other* ways carried on from the junction this step begins at,
   * setting off within about fifty degrees of the one the route takes.
   *
   * This is what identifies a fork, and no angle can: a motorway dividing
   * bends about ten degrees, and so does a boundary where a street is
   * renamed. Guidance reading only the geometry says nothing at either —
   * right for the name change, and silence at the one moment a driver is
   * looking at a Y with no way to tell which prong is theirs.
   *
   * A count of arms would not do it either. A crossroads driven straight
   * through has three ways on and needs no instruction, because only one of
   * them continues the road; measuring them against the route's own heading
   * is what separates the two. Slip roads are excluded when the route is not
   * itself on one, or every motorway exit passed would read as a fork.
   *
   * A junction with one of these is also never folded into the step before
   * it: a fork keeps the road's name, which is precisely what used to make it
   * disappear.
   *
   * **0 means unknown or none**, and is never grounds for saying anything.
   */
  forkArms?: number;
  /**
   * Nothing carries on the way the driver arrived: the road stops here and
   * this step turns off the end of it.
   *
   * "Turn left" and "at the end of the road, turn left" are different
   * instructions — one is a turning off a road that continues past it, the
   * other is a road that simply stops — and the difference is not in the
   * angle, which is identical.
   */
  endOfRoad?: boolean;
  /**
   * What the overhead panel says above each lane of the approach to this
   * step, left to right, aligned with `lanes`.
   *
   * The arrows say which movements a lane allows; only this says where any
   * of them goes. A driver on a five-lane approach with an interchange in
   * ninety seconds is reading the panel, not the road.
   */
  laneDestinations?: string[];
  /**
   * Why each lane of that approach is not the driver's, left to right and
   * aligned with `lanes`; 0 where it is theirs. Empty when the map never
   * said, which is most roads.
   *
   * Bits: 1 bus or other public service vehicle, 2 car-pool of two, 4 taxi,
   * 8 bicycle, 16 closed to motor traffic outright, 32 car-pool of three.
   * A lane asks for one car-pool number or the other, never both, so 2 and
   * 32 are alternatives rather than a value and a modifier. 16 is a marker
   * rather than a class — a bus lane is normally tagged as both 1 and 16 —
   * so a lane whose only bit is 16 is nobody's to drive in, and a lane that
   * names a class is open to a driver who belongs to it.
   *
   * The arrows never say a lane is somebody else's: a reserved bus lane
   * carries a straight-ahead arrow like every other lane, so guidance drawn
   * from `lanes` alone will happily point a driver into one.
   */
  laneAccess?: number[];
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
  /** True when independently published regional graphs were stitched. */
  federated?: boolean;
  /** Regional graph ids traversed in order. */
  regions?: string[];
  /** Exact shared-OSM-node handoffs between consecutive regions. */
  transitions?: Array<{
    osmNodeId: number;
    point: RoutePoint;
    fromRegion: string;
    toRegion: string;
  }>;
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
    /** Come back to stop 0 after the last one. Mutually exclusive with `openEnd`. */
    roundTrip?: boolean;
    /**
     * Start at stop 0, visit every other stop once, finish wherever the
     * last delivery is. Mutually exclusive with `roundTrip`.
     */
    openEnd?: boolean;
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

export interface RouteCatalogIndex {
  region: string;
  profile: string;
  base: string;
  bbox: [number, number, number, number];
  portals?: string | RoutePortalDirectory;
  neighbors?: string[];
  manifest?: { portals?: string | RoutePortalDirectory; [key: string]: unknown };
}

export interface RoutePortalPointer {
  offset: number;
  length: number;
  checksum?: string;
}

export interface RoutePortalDirectory {
  format: "rfrouteportals-v2";
  file: string;
  neighbors: Record<string, {
    count: number;
    ids: RoutePortalPointer;
    records: RoutePortalPointer;
  }>;
}

export interface FederatedRouteEngine {
  catalog: { format: "rangefind-route-catalog-v1"; indexes: RouteCatalogIndex[]; [key: string]: unknown };
  profile: string;
  route(params: RouteParams & { fromRegion?: string; toRegion?: string }): Promise<RouteResult>;
  matrix(params: { points: RoutePoint[]; bucket?: string; departureTime?: string | number | Date }): Promise<MatrixResult>;
  regionFor(point: RoutePoint): string;
  clearCaches(): void;
}

export declare function openRouteCatalogUrl(catalogUrl: string, options?: {
  profile?: "car" | "bike" | "foot";
  fetch?: typeof fetch;
  /** Gzip inflate override for immutable portal sidecars. */
  inflate?: (bytes: Uint8Array) => Uint8Array | Promise<Uint8Array>;
  graphOptions?: Partial<OpenRouteGraphOptions>;
  /** SHA-256 verification of portal range blocks; defaults to on when available. */
  verifyPortalChecksums?: boolean;
  maxPortalsPerBorder?: number;
  maxRegionPaths?: number;
  /** Prefer a proven direct handoff over exploring multi-region detours. Defaults to true. */
  preferDirectRegionPath?: boolean;
  maxRegionHops?: number;
  /** Maximum portal-proven regional nodes expanded while finding catalog paths. */
  maxRegionExpansions?: number;
  portalConcurrency?: number;
  openGraph?: (index: RouteCatalogIndex, baseUrl: string) => RouteGraphEngine | Promise<RouteGraphEngine>;
}): Promise<FederatedRouteEngine>;

/**
 * Reference LiveTrafficProvider serving a fixed (or lazily computed) state
 * list — for tests, demo loopbacks, and pre-fetched sidecar payloads.
 */
export declare function createStaticLiveProvider(
  states: LiveSegmentState[] | ((query: { epoch: string }) => LiveSegmentState[]),
  options?: { name?: string; epoch?: string }
): LiveTrafficProvider;
