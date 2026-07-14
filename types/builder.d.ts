// `rangefind/builder`: build an index from a rangefind config file.

export interface BuildOptions {
  configPath: string;
  /** Publish a generational delta over the existing index. */
  update?: boolean;
  /** Fold generations back into a single index. */
  compact?: boolean;
}

export function build(options: BuildOptions): Promise<void>;
