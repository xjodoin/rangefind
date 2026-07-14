// `rangefind/scoring-stats`: frozen corpus statistics for sharded builds
// (node-only). See docs/sharded-osm.md.

export const SCORING_STATS_FORMAT: string;
export const SCORING_DF_FORMAT: string;

export interface ScoringStatsInput {
  id: string;
  /** Path to the shard's JSONL corpus. */
  input: string;
}

export interface ScoringStats {
  format: string;
  total: number;
  field_totals: Record<string, number>;
  avg_field_lengths: Record<string, number>;
  df_file: string;
  df_terms: number;
  analysis: unknown;
  inputs: Array<{ id: string; input: string; total: number; bbox: number[] | null }>;
  /** Absolute df table path (added by loadScoringStats). */
  dfPath?: string;
}

export interface CollectScoringStatsOptions {
  /** Resolved rangefind config (readConfig) — defines the term space. */
  config: Record<string, unknown>;
  inputs: Array<string | ScoringStatsInput>;
  outDir?: string;
  statsFileName?: string;
  dfFileName?: string;
  spillTerms?: number;
  blockTerms?: number;
  log?: (line: string) => void;
}

export function collectScoringStats(options: CollectScoringStatsOptions): Promise<{
  statsPath: string;
  dfPath: string;
  stats: ScoringStats;
}>;

export function loadScoringStats(statsPath: string): ScoringStats & { dfPath: string };

export function openDfFile(path: string, options?: { cacheBlocks?: number }): {
  terms: number;
  lookup(term: string): number | undefined;
  close(): void;
};

export function scoringDfLookup(path: string, term: string): number | undefined;
export function installScoringDfProvider(): void;
export function closeScoringDfReaders(): void;
