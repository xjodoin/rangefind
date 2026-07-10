export const OSM_INTEGRATION_SCHEMA_VERSION = 1;

export const OSM_DISPLAY_FIELDS = Object.freeze([
  "name", "address", "house_number", "street", "unit", "suburb",
  "city", "district", "state", "postcode", "country",
  "url", "category", "type", "lat", "lon", "address_count",
  "_address_range_start", "_address_range_end", "_address_range_step",
  "_address_range_geometry", "_address_range_kind", "_address_range_inclusion"
]);

// Produce the canonical Rangefind schema for OSM-derived corpora. The output
// remains an ordinary Rangefind config: the integration adds domain behavior,
// not a second index format or query sidecar.
export function createOsmIndexConfig(options = {}) {
  const workerCount = Math.max(1, Math.floor(Number(options.workerCount || 1)));
  const config = {
    input: options.input || (options.rqa ? "data/osm-rqa-places.jsonl" : "data/osm-places.jsonl"),
    output: options.output || "public/rangefind",
    scanWorkers: workerCount,
    builderWorkerCount: workerCount,
    fields: [
      { name: "title", path: "search_name", weight: 6.0, b: 0.4, phrase: true },
      { name: "aliases", path: "aliases", weight: 4.0, b: 0.5 },
      { name: "address", path: "address_search", weight: 10.0, b: 0.2 },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    alwaysIndexFields: ["title", "aliases", "address"],
    authority: [
      {
        name: "address",
        path: "address",
        weight: 4000000,
        surface: false,
        exact: false,
        tokens: false,
        normalizer: "address",
        addressComponents: true
      },
      {
        name: "address_interpolation",
        path: "interpolation_keys",
        weight: 3000000,
        surface: false,
        exact: true,
        tokens: false,
        normalizer: "address-range"
      },
      {
        name: "postcode",
        path: "postal_lookup",
        weight: 5000000,
        surface: false,
        exact: false,
        tokens: false,
        normalizer: "address"
      }
    ],
    authorityMaxRowsPerKey: 64,
    facets: [
      { name: "category", path: "category" },
      { name: "type", path: "type" }
    ],
    numbers: [{ name: "population", path: "population", type: "int" }],
    geo: [{ name: "location", latPath: "geo_lat", lonPath: "geo_lon" }],
    suggest: [
      { path: "search_name", weightPath: "population" },
      { path: "aliases" }
    ],
    display: [...OSM_DISPLAY_FIELDS],
    buildProgressLogMs: Number(options.buildProgressLogMs ?? 15000)
  };
  if (options.region === "us") {
    config.docLayoutStrategy = "doc-id";
    config.postingGzipLevel = 3;
    config.segmentMergeFanIn = 512;
    config.codeStorePreloadMaxBytes = 2304 * 1024 * 1024;
    config.buildTelemetryPath = "osm-us-build-telemetry.json";
  }
  return { ...config, ...(options.overrides || {}) };
}
