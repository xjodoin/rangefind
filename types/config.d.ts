// `rangefind/config`: config loading/normalization (node-only).

export interface RangefindConfig {
  input: string;
  output: string;
  fields: Array<Record<string, unknown>>;
  facets: Array<Record<string, unknown>>;
  numbers: Array<Record<string, unknown>>;
  booleans: Array<Record<string, unknown>>;
  geo: Array<{ name: string; latPath: string; lonPath: string }>;
  suggest: Array<Record<string, unknown>>;
  vectors: Array<Record<string, unknown>>;
  display: Array<string | Record<string, unknown>>;
  /** Frozen scoring-stats artifact path (sharded builds). */
  scoringStats?: string;
  /** Provenance copied verbatim into the manifest. */
  meta?: Record<string, unknown> | null;
  [key: string]: unknown;
}

export const DEFAULTS: Record<string, unknown>;
export function readConfig(configPath: string): Promise<RangefindConfig>;
export function getPath(object: unknown, path?: string, fallback?: unknown): unknown;
export function geoComponentFieldNames(geoField: { name: string }): { lat: string; lon: string };
