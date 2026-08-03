// `rangefind/osm`: browser-safe OSM integration (schema + query intents).
// Typings cover the primary surface; the module re-exports further
// document/query helpers from documents.js, query.js, and schema.js.

import type { SearchEngine, SearchParams, SearchResponse, SuggestResponse } from "./runtime.js";

export const OSM_INTEGRATION_SCHEMA_VERSION: number;
export const OSM_DISPLAY_FIELDS: readonly string[];

export interface OsmIndexConfigOptions {
  region?: string;
  rqa?: boolean;
  additionalSources?: Array<{
    source: string;
    attribution?: string;
    license?: string;
    license_url?: string;
    url?: string;
  }>;
  workerCount?: number;
  buildProgressLogMs?: number;
  input?: string;
  output?: string;
  /** Merged on top of the ODbL provenance defaults. */
  meta?: Record<string, unknown>;
  /** Merged into the final config (replaces same-named fields). */
  overrides?: Record<string, unknown>;
}

export function createOsmIndexConfig(options?: OsmIndexConfigOptions): Record<string, unknown>;

export const OSM_CATEGORY_LEXICON_VERSION: number;
export const OSM_CANONICAL_TYPES: readonly string[];
export const OSM_TYPE_ALIASES: Readonly<Record<string, readonly string[]>>;

export interface OsmCategoryLexiconArtifact {
  version: number;
  facet: "type";
  types: string[];
  /** folded alias surface → canonical type value */
  aliases: Record<string, string>;
}

export type OsmCategoryLexicon = Map<string, { type: string; query: string }>;

export function fold(value: unknown): string;
export function typeQueryText(type: unknown): string;
export function buildCategoryLexicon(
  typeValues?: OsmCategoryLexiconArtifact | Array<string | { value?: string; n?: number }> | null
): OsmCategoryLexicon;
export function buildCategoryLexiconArtifact(
  typeValues: Array<string | { value?: string; n?: number }>
): OsmCategoryLexiconArtifact;
export function lookupCategory(
  lexicon: OsmCategoryLexicon,
  surface: unknown
): { type: string; query: string; label: string } | null;
export function defaultCategoryLexicon(): OsmCategoryLexicon;

export interface OsmReverseGeocodeParams {
  lat: number;
  lon: number;
  /** Hard lookup radius; defaults to 5 km. */
  radiusMeters?: number;
  /** Maximum address candidates, capped at 25. */
  size?: number;
  shards?: string | string[];
  trace?: boolean;
  filters?: SearchParams["filters"];
  /** Restrict returned semantic result types, e.g. street_address or city. */
  resultTypes?: string[];
  /** Restrict accuracy classes: ROOFTOP, RANGE_INTERPOLATED, GEOMETRIC_CENTER, APPROXIMATE. */
  locationTypes?: string[];
  /** Radius used only by the locality fallback; defaults to 30 km. */
  localityRadiusMeters?: number;
  [key: string]: unknown;
}

export interface OsmQueryParams extends SearchParams {
  /** Advisory device/map anchor used by OSM intent routing. */
  near?: { lat: number; lon: number };
  /** Set false to retain a coordinate marker without reverse geocoding. */
  reverseGeocode?: boolean | Omit<OsmReverseGeocodeParams, "lat" | "lon">;
  /** Autocomplete cursor offset; only text before the cursor is predicted. */
  inputOffset?: number;
  query?: string;
  limit?: number;
  constraints?: OsmConstraints;
  route?: string | Record<string, unknown> | Array<[number, number] | { lat: number; lon?: number; lng?: number }>;
  corridorMeters?: number;
  polylinePrecision?: number;
  routePositionMeters?: number;
  routeDirection?: "forward" | "reverse";
  viewport?: { minLat: number; maxLat: number; minLon: number; maxLon: number } | { lat: number; lon: number };
  at?: Date | string | number;
  timeZone?: string;
  includeUnknownOpenNow?: boolean;
}

export interface OsmConstraints {
  openNow?: boolean;
  wheelchair?: boolean;
  toiletsWheelchair?: boolean;
  contactless?: boolean;
  delivery?: boolean;
  takeaway?: boolean;
  driveThrough?: boolean;
  outdoorSeating?: boolean;
  internet?: boolean;
  reservation?: boolean;
  free?: boolean;
}

export function reverseGeocodeOsm(
  engine: SearchEngine,
  params: OsmReverseGeocodeParams
): Promise<SearchResponse>;
export function searchOsmQuery(engine: SearchEngine, params?: OsmQueryParams): Promise<SearchResponse>;
export function searchAlongRouteOsm(engine: SearchEngine, params: OsmQueryParams & { route: NonNullable<OsmQueryParams["route"]> }): Promise<SearchResponse>;
export function evaluateOpeningHours(
  value: string,
  options?: { at?: Date | string | number; timeZone?: string; requireComplete?: boolean }
): { state: "open" | "closed" | "unknown"; isOpen: boolean | null; reason: string; [key: string]: unknown };
export function decodePolyline(value: string, precision?: number): Array<{ lat: number; lon: number }>;
export function encodePolyline(points: Array<[number, number] | { lat: number; lon?: number; lng?: number }>, precision?: number): string;
export function suggestOsmQuery(
  engine: SearchEngine,
  params?: OsmQueryParams
): Promise<SuggestResponse>;

export interface MigrationPlace {
  id?: string;
  displayName: { text: string };
  formattedAddress: string;
  location?: { latitude: number; longitude: number };
  primaryType?: string;
  types: string[];
  openingHours?: { osmExpression: string | null; openNow: boolean | null; state: string };
  accessibilityOptions?: { wheelchair: string };
  nationalPhoneNumber?: string;
  websiteUri?: string;
  geometry?: Record<string, unknown>;
  details: Record<string, unknown>;
  source: { dataset: string; osmType: string | null; osmId: unknown };
  rangefind: {
    score: number | null;
    shard: string | null;
    distanceMeters: number | null;
    routeDistanceMeters: number | null;
    routeProgressMeters: number | null;
    routeProgressRatio: number | null;
    routeBearingDegrees: number | null;
    routeRank: number | null;
    rejoinPoint: { lat: number; lon: number } | null;
    constraintMatches: Record<string, boolean> | null;
    locationType: string | null;
    reverseGeocodeAccuracy: string | null;
  };
}

export function toMigrationPlace(result: Record<string, unknown>): MigrationPlace;

export type MigrationLatLng =
  | { latitude: number; longitude: number }
  | { lat: number; lng: number }
  | { lat: number; lon: number };

export interface MigrationCircle {
  center: MigrationLatLng;
  radius?: number;
  radiusMeters?: number;
}

export type MigrationRectangle =
  | { low: MigrationLatLng; high: MigrationLatLng }
  | { north: number; south: number; east: number; west: number }
  | { minLat: number; minLon: number; maxLat: number; maxLon: number };

export interface MigrationRequestBase {
  maxResultCount?: number;
  shards?: string | string[];
  trace?: boolean;
}

export interface MigrationAutocompleteRequest extends MigrationRequestBase {
  input: string;
  inputOffset?: number;
  includedPrimaryTypes?: string[];
  locationBias?: { circle?: MigrationCircle; rectangle?: MigrationRectangle } | MigrationCircle | MigrationRectangle;
}

export interface MigrationTextSearchRequest extends MigrationRequestBase {
  textQuery?: string;
  query?: string;
  includedType?: string;
  locationBias?: { circle?: MigrationCircle; rectangle?: MigrationRectangle } | MigrationCircle | MigrationRectangle;
  locationRestriction?: { circle?: MigrationCircle; rectangle?: MigrationRectangle } | MigrationCircle | MigrationRectangle;
  openNow?: boolean;
  constraints?: OsmConstraints;
  at?: Date | string | number;
  timeZone?: string;
  pageSize?: number;
}

export interface MigrationNearbySearchRequest extends MigrationRequestBase {
  includedTypes?: string[];
  excludedTypes?: string[];
  /** DISTANCE is exact distance; POPULARITY uses the static Rangefind score/prior. */
  rankPreference?: "DISTANCE" | "POPULARITY";
  locationRestriction: { circle: MigrationCircle } | MigrationCircle;
  openNow?: boolean;
  constraints?: OsmConstraints;
  timeZone?: string;
}

export interface MigrationGeocodeRequest extends MigrationRequestBase {
  address: string;
  locationBias?: { circle?: MigrationCircle; rectangle?: MigrationRectangle } | MigrationCircle | MigrationRectangle;
}

export interface MigrationReverseGeocodeRequest extends MigrationRequestBase {
  location: MigrationLatLng;
  radiusMeters?: number;
  size?: number;
  resultTypes?: string[];
  locationTypes?: string[];
  localityRadiusMeters?: number;
}

export interface MigrationRouteSearchRequest extends MigrationRequestBase {
  route: NonNullable<OsmQueryParams["route"]>;
  query?: string;
  textQuery?: string;
  corridorMeters?: number;
  polylinePrecision?: number;
  routePositionMeters?: number;
  routeDirection?: "forward" | "reverse";
  viewport?: OsmQueryParams["viewport"];
  openNow?: boolean;
  constraints?: OsmConstraints;
  at?: Date | string | number;
  timeZone?: string;
  limit?: number;
}

export interface MigrationPlacesResponse {
  places: MigrationPlace[];
  status: "OK" | "ZERO_RESULTS";
  rangefind: Record<string, unknown>;
}

export interface MigrationGeocodeResponse {
  results: MigrationPlace[];
  status: "OK" | "ZERO_RESULTS";
  rangefind: Record<string, unknown>;
}

export interface RangefindMapsAdapter {
  autocomplete(request: MigrationAutocompleteRequest): Promise<{
    suggestions: Array<{ placePrediction: Record<string, unknown> }>;
    status: "OK" | "ZERO_RESULTS";
    rangefind: Record<string, unknown>;
  }>;
  textSearch(request: MigrationTextSearchRequest): Promise<MigrationPlacesResponse>;
  nearbySearch(request: MigrationNearbySearchRequest): Promise<MigrationPlacesResponse>;
  geocode(request: MigrationGeocodeRequest): Promise<MigrationGeocodeResponse>;
  reverseGeocode(request: MigrationReverseGeocodeRequest): Promise<MigrationGeocodeResponse>;
  searchAlongRoute(request: MigrationRouteSearchRequest): Promise<MigrationPlacesResponse>;
  placeDetails(request: { placeId?: string; id?: string }): Promise<{ place: MigrationPlace; status: "OK" }>;
  clearPlaceCache(): void;
}

export function createRangefindMapsAdapter(
  engine: SearchEngine,
  options?: {
    defaults?: { near?: { lat: number; lon: number }; timeZone?: string; trace?: boolean };
    [key: string]: unknown;
  }
): RangefindMapsAdapter;
