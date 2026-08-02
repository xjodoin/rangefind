// `rangefind/osm`: browser-safe OSM integration (schema + query intents).
// Typings cover the primary surface; the module re-exports further
// document/query helpers from documents.js, query.js, and schema.js.

import type { SearchEngine, SearchParams, SearchResponse, SuggestResponse } from "./runtime.js";

export const OSM_INTEGRATION_SCHEMA_VERSION: number;
export const OSM_DISPLAY_FIELDS: readonly string[];

export interface OsmIndexConfigOptions {
  region?: string;
  rqa?: boolean;
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
  [key: string]: unknown;
}

export interface OsmQueryParams extends SearchParams {
  /** Advisory device/map anchor used by OSM intent routing. */
  near?: { lat: number; lon: number };
  /** Set false to retain a coordinate marker without reverse geocoding. */
  reverseGeocode?: boolean | Omit<OsmReverseGeocodeParams, "lat" | "lon">;
}

export function reverseGeocodeOsm(
  engine: SearchEngine,
  params: OsmReverseGeocodeParams
): Promise<SearchResponse>;
export function searchOsmQuery(engine: SearchEngine, params?: OsmQueryParams): Promise<SearchResponse>;
export function suggestOsmQuery(
  engine: SearchEngine,
  params?: OsmQueryParams
): Promise<SuggestResponse>;
