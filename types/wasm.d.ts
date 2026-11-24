// Rangefind query engine compiled from Rust to WebAssembly.
// Same request/response shapes as `createSearch()` from "rangefind".

export type WasmTransport = (url: string, ranges: Array<[number, number]>) => Promise<Uint8Array[] | null>;

export interface WasmSearchOptions {
  baseUrl?: string;
  /** Module bytes, URL, or a compiled WebAssembly.Module (defaults to the bundled file). */
  wasm?: BufferSource | URL | string | WebAssembly.Module;
  /** fetch-compatible function used for whole-object and Range requests. */
  fetch?: (url: string, init?: RequestInit) => Promise<Response>;
  /** Fully custom byte transport; `null` means the object does not exist. */
  transport?: WasmTransport;
  /**
   * Bytes of decoded objects kept cached between calls (default 256 MiB;
   * least recently used objects are evicted when a call ends; `0` = unbounded).
   */
  cacheBudgetBytes?: number;
  [option: string]: unknown;
}

export interface WasmSearch {
  summary: { total: number; features: Record<string, unknown> | null; facets: string[] | null; kind: "single" | "generational" | "sharded" };
  search(params: Record<string, unknown>): Promise<any>;
  count(params: Record<string, unknown>): Promise<any>;
  suggest(params: Record<string, unknown>): Promise<any>;
  vectorSearch(params: Record<string, unknown>): Promise<any>;
  authorityLookup(surface: string, params?: Record<string, unknown>): Promise<any>;
  searchAddressAuthority(params: Record<string, unknown>): Promise<any>;
  loadFacetValues(field: string): Promise<Array<{ value: string; label?: string; n?: number }>>;
  loadDocValues(fields: string[], indexes: number[]): Promise<Array<Record<string, unknown>>>;
  hydrateRows(rows: Array<[number, number]>, context?: Record<string, unknown>): Promise<any[]>;
  stats(): Promise<{
    transfer: Record<string, { requests: number; ranges: number; bytes: number }>;
    totals: { requests: number; bytes: number };
    /** Cache memory as of the end of the last call. */
    cache: {
      budgetBytes: number;
      usedBytes: number;
      entries: number;
      activeCalls: number;
      trims: number;
      evictedEntries: number;
      evictedBytes: number;
      caches: Record<string, { entries: number; bytes: number }>;
    };
  }>;
  /** OSM query layer (`rangefind/osm` functions bound to this engine). */
  osm: {
    search(params: Record<string, unknown>): Promise<any>;
    suggest(params: Record<string, unknown>): Promise<any>;
    reverseGeocode(params: Record<string, unknown>): Promise<any>;
    searchAlongRoute(params: Record<string, unknown>): Promise<any>;
    resolveSuggestion(suggestion: Record<string, unknown>): Promise<any>;
    /** Fills `result` on the given suggestion objects in place, like the JS function. */
    hydrateSuggestions<T extends Record<string, unknown>>(suggestions: T[]): Promise<T[]>;
    collapseStreetSuggestions(response: Record<string, unknown>, params?: Record<string, unknown>): Promise<any>;
    /** `lexicon: "default"` uses the built-in category lexicon instead of the index's own. */
    parseQueryIntent(q: string, options?: { lexicon?: "default" | "index" }): Promise<any>;
    parseNearbyCategoryIntent(q: string, options?: { lexicon?: "default" | "index" }): Promise<any>;
    parseCoordinateIntent(q: string): Promise<{ lat: number; lon: number } | null>;
    maps: {
      configure(defaults: Record<string, unknown>): Promise<null>;
      autocomplete(request: Record<string, unknown>): Promise<any>;
      textSearch(request: Record<string, unknown>): Promise<any>;
      nearbySearch(request: Record<string, unknown>): Promise<any>;
      geocode(request: Record<string, unknown>): Promise<any>;
      reverseGeocode(request: Record<string, unknown>): Promise<any>;
      searchAlongRoute(request: Record<string, unknown>): Promise<any>;
      placeDetails(request: Record<string, unknown>): Promise<any>;
      clearPlaceCache(): Promise<null>;
    };
  };
  /** Release the engine, its wasm memory and file handles; in-flight calls still settle. */
  close(): void;
}

export function loadRangefindWasm(source?: WasmSearchOptions["wasm"]): Promise<WebAssembly.Exports>;
export function fetchTransport(fetchFn?: WasmSearchOptions["fetch"]): WasmTransport & { close(): void };
export function createWasmSearch(options?: WasmSearchOptions): Promise<WasmSearch>;
export default createWasmSearch;
