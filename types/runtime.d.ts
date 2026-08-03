// Public query-runtime surface for `rangefind` and `rangefind/browser`.
// Pragmatic typings: the user-facing API is typed precisely; open-ended
// payloads stay indexable.

export interface GeoNear {
  lat: number;
  lon: number;
  /** Omit for pure nearest-first (requires sort: "distance"). */
  radiusMeters?: number;
}

export interface GeoBox {
  minLat: number;
  maxLat: number;
  minLon: number;
  maxLon: number;
}

export interface GeoParams {
  /** Geo field name; optional when the index has exactly one. */
  field?: string;
  near?: GeoNear;
  box?: GeoBox;
  /** Encoded polyline, GeoJSON LineString/MultiLineString, or coordinate array. */
  route?: string | Record<string, unknown> | Array<[number, number] | { lat: number; lon?: number; lng?: number }>;
  corridorMeters?: number;
  polylinePrecision?: number;
  routePositionMeters?: number;
  routeDirection?: "forward" | "reverse";
  viewport?: GeoBox | { lat: number; lon: number };
  sort?: "distance" | "route";
  boost?: { weight?: number; pivotMeters?: number };
}

export interface SearchParams {
  q?: string;
  page?: number;
  size?: number;
  /** "field" | "-field" | { field, order } over a number/boolean field. */
  sort?: string | { field: string; order?: "asc" | "desc"; direction?: "asc" | "desc" };
  facets?: string[];
  filters?: {
    facets?: Record<string, string[]>;
    numbers?: Record<string, { min?: number; max?: number }>;
    booleans?: Record<string, boolean>;
  };
  geo?: GeoParams;
  /**
   * Sharded indexes only: restrict to shard ids or group labels
   * ("quebec", "canada", "europe"). Unknown names throw.
   */
  shards?: string | string[];
  vector?: number[] | Float32Array;
  vectorField?: string;
  hybrid?: { rrfK?: number };
  highlight?: boolean | Record<string, unknown>;
  includeResults?: boolean;
  trace?: boolean;
  /** Disable or override a manifest-configured numeric relevance prior. */
  rankPrior?: boolean;
  rankPriorBoost?: number;
  rankPriorOverfetch?: number;
  [key: string]: unknown;
}

export interface SearchResult {
  id: string;
  index?: number;
  score?: number;
  title?: string;
  url?: string;
  distanceMeters?: number;
  routeDistanceMeters?: number;
  routeProgressMeters?: number;
  routeProgressRatio?: number;
  routeBearingDegrees?: number;
  routeRank?: number;
  rejoinPoint?: { lat: number; lon: number };
  routeMatch?: {
    distanceMeters: number;
    progressMeters: number;
    progressRatio: number;
    segmentIndex: number;
    segmentProgress: number;
    bearingDegrees: number;
    rejoinPoint: { lat: number; lon: number };
  };
  /** Sharded indexes: owning shard id (hierarchical path when roots nest). */
  shard?: string;
  /** Generational indexes: owning generation. */
  generation?: number;
  hybrid?: { text?: number; vector?: number };
  [field: string]: unknown;
}

export interface FacetValue {
  value: string;
  label?: string;
  count: number;
}

export interface SearchResponse {
  total: number;
  results: SearchResult[];
  page: number;
  size: number;
  approximate?: boolean;
  correctedQuery?: string;
  corrections?: unknown;
  normalizedQuery?: string;
  facets?: Record<string, { values: FacetValue[]; exact: boolean }>;
  stats?: Record<string, unknown>;
}

export interface Suggestion {
  text: string;
  count: number;
  weight: number;
  /** OSM integration: structured prediction fields. */
  description?: string;
  mainText?: string;
  secondaryText?: string;
  matchedRanges?: Array<{ start: number; end: number }>;
  kind?: string;
  types?: string[];
  selection?: { query: string; shards?: string[] };
  [key: string]: unknown;
}

export interface SuggestResponse {
  q: string;
  suggestions: Suggestion[];
  normalizedQuery?: string;
  stats?: Record<string, unknown>;
}

export interface CountResponse {
  total: number;
  totalExact?: boolean;
  approximate?: boolean;
  stats?: Record<string, unknown>;
}

export interface RangefindManifest {
  version?: number;
  engine?: string;
  total?: number;
  built_at?: string;
  /** Provenance: attribution, license, generator, data version, … */
  meta?: Record<string, unknown> | null;
  shards?: Array<Record<string, unknown>>;
  generations?: Array<Record<string, unknown>>;
  rankPrior?: { field: string; boost: number; overfetch?: number } | null;
  [key: string]: unknown;
}

export interface SearchEngine {
  manifest: RangefindManifest;
  search(params?: SearchParams): Promise<SearchResponse>;
  suggest(params?: { q?: string; size?: number; shards?: string | string[]; [key: string]: unknown }): Promise<SuggestResponse>;
  count(params?: { q?: string; shards?: string | string[]; [key: string]: unknown }): Promise<CountResponse>;
  /**
   * Exact-surface lookup against the authority autocomplete lexicon: every
   * display whose normalized form equals the surface's, best rank first.
   * Sharded roots serve it from the suggest-routing artifact and add
   * `shards` (federation provenance) to each match; returns null when the
   * index has no lexicon (or the sharded root has no artifact).
   */
  authorityLookup?(surface: string, params?: { size?: number }): Promise<{
    surface: string;
    prefix: string;
    matches: Array<{ text: string; weight: number; count: number; full: boolean; shards?: string[] }>;
  } | null>;
  vectorSearch(params?: Record<string, unknown>): Promise<SearchResponse>;
  loadFacetValues(field: string): Promise<FacetValue[]>;
  /** Sharded engines: the shard descriptors (id, path, total, bbox). */
  shards?: Array<{ id: string; path: string; total: number; bbox: number[] | null }>;
  [key: string]: unknown;
}

export interface CreateSearchOptions {
  /** Index root URL or path prefix (default "./rangefind/"). */
  baseUrl?: string;
  manifestName?: string;
  maxPageSize?: number;
  verifyChecksums?: boolean;
  trace?: boolean;
  /** Batch scattered byte ranges from one object into multipart HTTP requests. */
  multiRangeRequests?: boolean;
  /** Maximum byte ranges per multipart request (default 32, clamped to 2–64). */
  multiRangeMaxRanges?: number;
  /** Use display rows embedded in geo leaves; set false to force document hydration. */
  geoCapsules?: boolean;
  /** Use multi-resolution facet-to-geo-cell routing; set false to force tree traversal. */
  geoCellIndexes?: boolean;
  [key: string]: unknown;
}

export function createSearch(options?: CreateSearchOptions): Promise<SearchEngine>;
export function setFetchImplementation(
  fn: typeof fetch,
  capabilities?: { multiRange?: boolean }
): void;
/**
 * Install a gzip inflate implementation for hosts without DecompressionStream
 * (React Native/Hermes, QuickJS, JavaScriptCore). Receives the compressed
 * bytes and returns the inflated bytes (e.g. pako.ungzip). Pass null to
 * restore the default DecompressionStream path.
 */
export function setInflateImplementation(
  fn: ((compressed: ArrayBuffer) => ArrayBuffer | Uint8Array | Promise<ArrayBuffer | Uint8Array>) | null
): void;
export default createSearch;
