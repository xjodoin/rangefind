// `rangefind/shards`: sharded root manifests (node-only).

export const SHARDED_INDEX_FORMAT: string;

export interface ShardDescriptor {
  id: string;
  /** Relative to the root manifest's directory; defaults to shards/<id>/. */
  path?: string;
  manifest?: string;
  /** Coverage [minLat, minLon, maxLat, maxLon] for geo routing. */
  bbox?: number[] | null;
  /** Hierarchy labels for multi-level query scoping ("canada", "europe"). */
  groups?: string[];
}

export interface WriteShardedRootManifestOptions {
  outDir: string;
  shards: ShardDescriptor[];
  scoringStats?: { total: number; df_terms?: number } | null;
  /** Block returned by writeTextRoutingIndex; enables federated text routing. */
  textRouting?: TextRoutingBlock | null;
  /** Extra root-manifest fields; `extra.meta` overrides the provenance block. */
  extra?: Record<string, unknown>;
}

export function writeShardedRootManifest(options: WriteShardedRootManifestOptions): Record<string, unknown>;

export const TEXT_ROUTING_FORMAT: string;

export interface TextRoutingBlock {
  format: string;
  version: number;
  term_count: number;
  /** Shard ids in routing-ordinal order; queries fail open for unknown ids. */
  shard_ids: string[];
  analysis?: Record<string, unknown>;
  directory: Record<string, unknown>;
}

export interface WriteTextRoutingIndexOptions {
  /** Sharded root directory; the artifact lands in <outDir>/text-routing/. */
  outDir: string;
  /**
   * In the sharded root's shard order: either the built shard index
   * directory or a term-set sidecar written by writeShardTermSet.
   */
  shards: Array<{ id: string; dir: string } | { id: string; termSet: string }>;
  segmentTerms?: number;
  packTargetBytes?: number;
  directoryPageBytes?: number;
}

/**
 * Persists one shard's sorted term set (plus analysis profile) as a small
 * gzipped sidecar, so routing can be rebuilt after the shard's local
 * artifacts are reclaimed. Regenerate whenever the shard changes.
 */
export function writeShardTermSet(options: { dir: string; outFile: string }): { terms: number; analysis: Record<string, unknown> | null };

/**
 * Builds the root-level term → shard-set routing directory by enumerating
 * every shard's term directory (single or generational). Rebuild whenever a
 * shard's contents change; stale routing can hide terms added by deltas.
 */
export function writeTextRoutingIndex(options: WriteTextRoutingIndexOptions): Promise<TextRoutingBlock>;
