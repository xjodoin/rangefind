import { OSM_CANONICAL_TYPES } from "./category_lexicon.js";

export const OSM_INTEGRATION_SCHEMA_VERSION = 2;

export const OSM_DISPLAY_FIELDS = Object.freeze([
  "name", "address", "house_number", "street", "unit", "suburb",
  "city", "district", "state", "postcode", "country",
  "url", "category", "type", "lat", "lon", "address_count",
  "prominence", "details",
  "_address_range_start", "_address_range_end", "_address_range_step",
  "_address_range_geometry", "_address_range_kind", "_address_range_inclusion"
]);

// Produce the canonical Rangefind schema for OSM-derived corpora. The output
// remains an ordinary Rangefind config: the integration adds domain behavior,
// not a second index format or query sidecar.
export function createOsmIndexConfig(options = {}) {
  const workerCount = Math.max(1, Math.floor(Number(options.workerCount || 1)));
  const config = {
    // Provenance published in the manifest. OSM's ODbL requires attribution
    // wherever the data is used; options.meta merges extra fields (generator,
    // data version, …) on top without losing the license block.
    meta: {
      source: "OpenStreetMap",
      attribution: "© OpenStreetMap contributors",
      license: "ODbL-1.0",
      license_url: "https://www.openstreetmap.org/copyright",
      ...(options.rqa ? {
        additional_sources: [{
          source: "Référentiel québécois des adresses (RQA)",
          attribution: "Gouvernement du Québec",
          license: "CC-BY-4.0"
        }]
      } : {}),
      ...(options.meta || {})
    },
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
      },
      {
        name: "street",
        path: "street_authority",
        weight: 2000000,
        surface: true,
        exact: true,
        tokens: false,
        normalizer: "address"
      }
    ],
    authorityMaxRowsPerKey: 64,
    facets: [
      { name: "category", path: "category" },
      { name: "type", path: "type" },
      { name: "brand", path: "details.brand" },
      { name: "cuisine", path: "details.cuisine" },
      { name: "wheelchair", path: "details.wheelchair" },
      { name: "access", path: "details.access" }
    ],
    // Keep curated type bitmaps for broad/text lanes that cannot use spatial
    // category routing. Category-cell geo queries prove their type from the
    // cell's point ordinals and do not fetch this global bitmap.
    filterBitmapFacetValues: { type: [...OSM_CANONICAL_TYPES] },
    numbers: [
      { name: "population", path: "population", type: "int" },
      { name: "prominence", path: "prominence", type: "double" }
    ],
    rankPrior: { field: "prominence", boost: 0.45, overfetch: 4 },
    geo: [{ name: "location", latPath: "geo_lat", lonPath: "geo_lon" }],
    // Route common category + viewport/radius queries directly to the small
    // set of geo leaves that contain that category. Three exact grid levels
    // share compact block objects; result rows remain single-copy in capsules.
    geoCellIndexes: [{
      field: "location",
      facet: "type",
      levels: [9, 12, 15],
      blockZoom: 9,
      codeGroupSize: 16,
      maxCellsPerQuery: 48,
      values: [...OSM_CANONICAL_TYPES]
    }],
    // Geo leaf capsules turn the spatial range read into a self-contained
    // result page for the fields used by the map UI. Less common metadata
    // remains available through the ordinary document store on non-geo lanes.
    geoCapsules: true,
    geoCapsuleFields: [
      "id", "name", "address", "house_number", "street", "unit", "suburb",
      "city", "district", "state", "postcode", "country", "url", "category",
      "type", "lat", "lon", "address_count", "prominence", "details"
    ],
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
