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
  /** Extra root-manifest fields; `extra.meta` overrides the provenance block. */
  extra?: Record<string, unknown>;
}

export function writeShardedRootManifest(options: WriteShardedRootManifestOptions): Record<string, unknown>;
