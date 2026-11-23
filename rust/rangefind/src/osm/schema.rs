//! Port of `src/integrations/osm/schema.js`: the OSM integration's schema
//! version, display fields, and the canonical index config
//! (`createOsmIndexConfig`, returned as the same JSON object the JS builds).

use serde_json::{json, Map, Value};

use crate::osm::category_lexicon::OSM_CANONICAL_TYPES;
use crate::osm::constraints::js::{is_truthy, js_number, nullish, prop, spread_into};
use crate::util::json::{num, truthy};

/// `OSM_INTEGRATION_SCHEMA_VERSION`.
pub const OSM_INTEGRATION_SCHEMA_VERSION: u32 = 5;

/// `OSM_DISPLAY_FIELDS`.
pub const OSM_DISPLAY_FIELDS: &[&str] = &[
    "name",
    "address",
    "house_number",
    "street",
    "unit",
    "suburb",
    "city",
    "district",
    "state",
    "postcode",
    "country",
    "url",
    "source",
    "category",
    "type",
    "lat",
    "lon",
    "bbox",
    "address_count",
    "sample_count",
    "prominence",
    "details",
    "geometry",
    "_address_range_start",
    "_address_range_end",
    "_address_range_step",
    "_address_range_geometry",
    "_address_range_kind",
    "_address_range_inclusion",
];

fn strings(values: &[&str]) -> Value {
    Value::Array(values.iter().map(|value| Value::String(value.to_string())).collect())
}

/// `createOsmIndexConfig(options)`.
pub fn create_osm_index_config(options: &Value) -> Value {
    let options = Some(options);
    let worker_count_raw = prop(options, "workerCount");
    let worker_count_input = if is_truthy(worker_count_raw) { js_number(worker_count_raw) } else { 1.0 };
    // `Math.max(1, Math.floor(n))` (NaN propagates).
    let worker_count = {
        let floored = worker_count_input.floor();
        if floored.is_nan() {
            f64::NAN
        } else {
            floored.max(1.0)
        }
    };
    let rqa = is_truthy(prop(options, "rqa"));
    let additional_sources: Vec<Value> = match prop(options, "additionalSources") {
        Some(Value::Array(items)) => items.iter().filter(|item| truthy(item)).cloned().collect(),
        _ if rqa => vec![json!({
            "source": "Référentiel québécois des adresses (RQA)",
            "attribution": "Gouvernement du Québec",
            "license": "CC-BY-4.0"
        })],
        _ => Vec::new(),
    };
    let mut meta = Map::new();
    meta.insert("source".into(), json!("OpenStreetMap"));
    meta.insert("attribution".into(), json!("© OpenStreetMap contributors"));
    meta.insert("license".into(), json!("ODbL-1.0"));
    meta.insert("license_url".into(), json!("https://www.openstreetmap.org/copyright"));
    if !additional_sources.is_empty() {
        meta.insert("additional_sources".into(), Value::Array(additional_sources));
    }
    let meta_option = prop(options, "meta");
    if is_truthy(meta_option) {
        spread_into(&mut meta, meta_option);
    }
    let or_default = |key: &str, fallback: &str| -> Value {
        let value = prop(options, key);
        if is_truthy(value) {
            value.cloned().unwrap_or(Value::Null)
        } else {
            Value::String(fallback.to_string())
        }
    };
    let input = or_default("input", if rqa { "data/osm-rqa-places.jsonl" } else { "data/osm-places.jsonl" });
    let output = or_default("output", "public/rangefind");
    let canonical = strings(OSM_CANONICAL_TYPES);
    let build_progress = js_number(nullish(prop(options, "buildProgressLogMs"), Some(&json!(15000))));
    let mut config = json!({
        "meta": meta,
        "input": input,
        "output": output,
        "scanWorkers": num(worker_count),
        "builderWorkerCount": num(worker_count),
        "codeStorePreloadMaxBytes": 3221225472u64,
        "geoCapsuleDocPageCachePages": 4096,
        "fields": [
            { "name": "title", "path": "search_name", "weight": 6, "b": 0.4, "phrase": true },
            { "name": "aliases", "path": "aliases", "weight": 4, "b": 0.5, "phrase": true },
            { "name": "address", "path": "address_search", "weight": 10, "b": 0.2 },
            { "name": "body", "path": "body", "weight": 1, "b": 0.75 }
        ],
        "alwaysIndexFields": ["title", "aliases", "address"],
        "authority": [
            {
                "name": "address", "path": "address", "weight": 4000000, "surface": false, "exact": false,
                "tokens": false, "normalizer": "address", "addressComponents": true
            },
            {
                "name": "address_interpolation", "path": "interpolation_keys", "weight": 3000000, "surface": false,
                "exact": true, "tokens": false, "normalizer": "address-range"
            },
            {
                "name": "postcode", "path": "postal_lookup", "weight": 5000000, "surface": false, "exact": false,
                "tokens": false, "normalizer": "address"
            },
            {
                "name": "street", "path": "street_authority", "weight": 2000000, "surface": true, "exact": true,
                "tokens": false, "normalizer": "address"
            }
        ],
        "authorityMaxRowsPerKey": 64,
        "facets": [
            { "name": "category", "path": "category" },
            { "name": "type", "path": "type" },
            { "name": "brand", "path": "details.brand" },
            { "name": "cuisine", "path": "details.cuisine" },
            { "name": "wheelchair", "path": "details.wheelchair" },
            { "name": "toilets_wheelchair", "path": "details.toilets_wheelchair" },
            { "name": "internet_access", "path": "details.internet_access" },
            { "name": "outdoor_seating", "path": "details.outdoor_seating" },
            { "name": "takeaway", "path": "details.takeaway" },
            { "name": "delivery", "path": "details.delivery" },
            { "name": "drive_through", "path": "details.drive_through" },
            { "name": "reservation", "path": "details.reservation" },
            { "name": "payment_cash", "path": "details.payment_cash" },
            { "name": "payment_cards", "path": "details.payment_cards" },
            { "name": "payment_contactless", "path": "details.payment_contactless" },
            { "name": "fee", "path": "details.fee" },
            { "name": "access", "path": "details.access" }
        ],
        "blockFilterFields": ["category", "type", "population", "prominence", "location.lat", "location.lon"],
        "filterBitmapFacetValues": { "type": canonical.clone() },
        "numbers": [
            { "name": "population", "path": "population", "type": "int" },
            { "name": "prominence", "path": "prominence", "type": "double" }
        ],
        "rankPrior": { "field": "prominence", "boost": 0.45, "overfetch": 4 },
        "geo": [{ "name": "location", "latPath": "geo_lat", "lonPath": "geo_lon" }],
        "geoCellIndexes": [{
            "field": "location",
            "facet": "type",
            "levels": [9, 12, 15],
            "blockZoom": 9,
            "codeGroupSize": 16,
            "maxCellsPerQuery": 48,
            "includeAll": true,
            "values": canonical
        }],
        "geoCapsules": true,
        "geoCapsuleFields": [
            "id", "name", "address", "house_number", "street", "unit", "suburb",
            "city", "district", "state", "postcode", "country", "url", "category",
            "source", "type", "lat", "lon", "bbox", "address_count", "sample_count",
            "prominence", "details", "geometry"
        ],
        "suggest": [
            { "path": "search_name", "weightPath": "population" },
            { "path": "aliases", "weightPath": "population" }
        ],
        "display": strings(OSM_DISPLAY_FIELDS),
        "buildProgressLogMs": num(build_progress)
    });
    let map = config.as_object_mut().expect("config object");
    if prop(options, "region") == Some(&json!("us")) {
        map.insert("docLayoutStrategy".into(), json!("doc-id"));
        map.insert("postingGzipLevel".into(), json!(3));
        map.insert("segmentMergeFanIn".into(), json!(512));
        map.insert("buildTelemetryPath".into(), json!("osm-us-build-telemetry.json"));
    }
    let overrides = prop(options, "overrides");
    if is_truthy(overrides) {
        spread_into(map, overrides);
    }
    config
}
