// `rangefind/osm/node` and `rangefind/osm/extract`: node-side OSM index
// publication and PBF extraction.

export * from "./osm.js";

export interface OsmShardSpec {
  id: string;
  /** Places JSONL for this shard. */
  input: string;
  region?: string;
  overrides?: Record<string, unknown>;
}

export interface BuildOsmShardedIndexOptions {
  output?: string;
  shards: OsmShardSpec[];
  workerCount?: number;
  region?: string;
  rqa?: boolean;
  buildProgressLogMs?: number;
  overrides?: Record<string, unknown>;
}

export function buildOsmShardedIndex(options: BuildOsmShardedIndexOptions): Promise<{
  output: string;
  rootManifest: Record<string, unknown>;
  statsPath: string;
  seconds: number;
}>;

export interface BuildOsmIndexOptions {
  root?: string;
  region?: string;
  rqa?: boolean;
  workerCount?: number;
  input?: string;
  output?: string;
  buildProgressLogMs?: number;
  config?: Record<string, unknown>;
  [key: string]: unknown;
}

export function buildOsmIndex(options?: BuildOsmIndexOptions): Promise<Record<string, unknown>>;
export function writeOsmSite(options?: BuildOsmIndexOptions): Record<string, unknown>;
export const OSM_DEMO_VIEWS: Record<string, { center: [number, number]; zoom: number }>;

export interface ExtractOsmPlacesOptions {
  /** Path to the .osm.pbf extract. */
  pbf?: string;
  /** Work root; output lands under `${root}/data/`. */
  root?: string;
  region?: string;
  limit?: number;
  force?: boolean;
  rqa?: boolean;
  rqaArchive?: string;
  buildProgressLogMs?: number;
}

/**
 * PBF → normalized places JSONL with fingerprinted skip logic (an
 * unchanged PBF returns immediately). Returns the corpus meta.
 */
export function extractOsmPlaces(options?: ExtractOsmPlacesOptions): Promise<{
  docs: number;
  bytes?: number;
  seconds?: number;
  /** Locality enrichment: municipality boundaries assembled and documents stamped. */
  locality?: {
    boundaries: number;
    boundariesDropped: number;
    ringsDropped: number;
    places: number;
    enrichedFromBoundaries: number;
    enrichedFromPlaces: number;
  };
  [key: string]: unknown;
}>;
