// `rangefind/node`: the same query engine over local directories
// (positional reads) or HTTP(S) indexes with disk + memory caching.

import type { CreateSearchOptions, SearchEngine } from "./runtime.js";

export interface NodeSearchOptions extends CreateSearchOptions {
  /** Local directory, file:// URL, or http(s) URL. */
  baseUrl?: string;
  [key: string]: unknown;
}

export function createNodeSearch(options?: NodeSearchOptions): Promise<SearchEngine>;
export function resetNodeRuntimeCaches(options?: { disk?: boolean }): Promise<void>;
export default createNodeSearch;
