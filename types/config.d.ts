// `rangefind/config`: config loading/normalization (node-only).

export interface RangefindConfig {
  input: string;
  output: string;
  fields: Array<Record<string, unknown>>;
  facets: Array<Record<string, unknown>>;
  numbers: Array<Record<string, unknown>>;
  /** Optional normalized numeric relevance prior applied to text-score ties. */
  rankPrior?: { field: string; boost?: number; overfetch?: number } | null;
  booleans: Array<Record<string, unknown>>;
  geo: Array<{ name: string; latPath: string; lonPath: string }>;
  /** Embed compact display rows in geo leaves for one-range map results. */
  geoCapsules?: boolean;
  /** Display payload fields stored in each geo leaf capsule. */
  geoCapsuleFields?: string[];
  /** Maximum document pages retained while building geo capsules. */
  geoCapsuleDocPageCachePages?: number;
  /** Multi-resolution geo-cell routing indexes keyed by a facet. */
  geoCellIndexes?: Array<{
    field: string;
    facet: string;
    levels?: number[];
    blockZoom?: number;
    codeGroupSize?: number;
    maxCellsPerQuery?: number;
    maxFacetValues?: number;
    values?: string[];
  }>;
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
