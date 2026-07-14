// `rangefind/osm`: browser-safe OSM integration (schema + query intents).
// Typings cover the primary surface; the module re-exports further
// document/query helpers from documents.js, query.js, and schema.js.

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
