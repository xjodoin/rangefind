//! Port of `src/integrations/osm/google_maps_adapter.js`: a migration facade
//! that accepts Google Maps Places/Geocoding-style requests, calls the OSM
//! query functions, and maps results to Place-like objects.
//!
//! Requests and responses stay dynamic JSON, exactly like the JS. The four
//! query functions the JS imports from `query.js` (`searchOsmQuery`,
//! `suggestOsmQuery`, `reverseGeocodeOsm`, `searchAlongRouteOsm`) are reached
//! through the [`OsmQueryBackend`] trait, which also stands in for the JS
//! `options.searchOsmQuery`-style overrides: the backend owns (or borrows)
//! the engine the JS passes as the first argument. Parameters are built as
//! `JSON.stringify` would see them: keys whose JS value is `undefined` are
//! omitted.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::future::Future;

use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::osm::constraints::js::{
    is_present, is_truthy, js_number, js_string, js_template, js_trim, nullish, or, prop, put, spread_into,
};
use crate::util::json::{num, truthy};

/// The query entry points the adapter calls (`query.js`). Implementations
/// wrap an engine: `params` is the JSON params object the JS passes as the
/// second argument, and the result is the JSON response.
pub trait OsmQueryBackend {
    fn search_osm_query(&self, params: Value) -> impl Future<Output = Result<Value>>;
    fn suggest_osm_query(&self, params: Value) -> impl Future<Output = Result<Value>>;
    fn reverse_geocode_osm(&self, params: Value) -> impl Future<Output = Result<Value>>;
    fn search_along_route_osm(&self, params: Value) -> impl Future<Output = Result<Value>>;
}

/// Google Places primary types whose OSM text differs from the snake-case
/// spelling (`TYPE_ALIASES`).
pub const GOOGLE_TYPE_ALIASES: &[(&str, &str)] = &[
    ("amusement_park", "theme park"),
    ("coffee_shop", "cafe"),
    ("gas_station", "fuel"),
    ("grocery_store", "supermarket"),
    ("movie_theater", "cinema"),
    ("park_and_ride", "park and ride"),
    ("public_bath", "public bath"),
    ("shopping_mall", "mall"),
    ("tourist_attraction", "attraction"),
];

type Opt<'a> = Option<&'a Value>;

/// `clampSize(value, fallback, max)`.
fn clamp_size(value: Opt, fallback: f64, max: f64) -> f64 {
    let parsed = if is_present(value) { js_number(value) } else { fallback }.floor();
    if parsed.is_finite() {
        parsed.min(max).max(1.0)
    } else {
        fallback
    }
}

#[derive(Clone, Copy)]
struct LatLon {
    lat: f64,
    lon: f64,
}

impl LatLon {
    fn to_map(self) -> Map<String, Value> {
        let mut out = Map::new();
        out.insert("lat".into(), num(self.lat));
        out.insert("lon".into(), num(self.lon));
        out
    }

    fn to_json(self) -> Value {
        Value::Object(self.to_map())
    }
}

fn type_error(message: String) -> Error {
    Error::new(message)
}

/// `normalizeLocation(value, label)`.
fn normalize_location(value: Opt, label: &str) -> Result<LatLon> {
    let lat = js_number(nullish(prop(value, "latitude"), prop(value, "lat")));
    let lon = js_number(nullish(nullish(prop(value, "longitude"), prop(value, "lng")), prop(value, "lon")));
    if !lat.is_finite() || !lon.is_finite() {
        return Err(type_error(format!("{label} needs latitude/longitude or lat/lng.")));
    }
    Ok(LatLon { lat, lon })
}

fn rectangle_bounds(rectangle: Opt) -> Option<[f64; 4]> {
    let north = js_number(nullish(prop(rectangle, "north"), prop(rectangle, "maxLat")));
    let south = js_number(nullish(prop(rectangle, "south"), prop(rectangle, "minLat")));
    let east = js_number(nullish(prop(rectangle, "east"), prop(rectangle, "maxLon")));
    let west = js_number(nullish(prop(rectangle, "west"), prop(rectangle, "minLon")));
    [north, south, east, west].iter().all(|value| value.is_finite()).then_some([north, south, east, west])
}

fn low_high(rectangle: Opt) -> Result<Option<(LatLon, LatLon)>> {
    if is_truthy(prop(rectangle, "low")) && is_truthy(prop(rectangle, "high")) {
        let low = normalize_location(prop(rectangle, "low"), "rectangle.low")?;
        let high = normalize_location(prop(rectangle, "high"), "rectangle.high")?;
        return Ok(Some((low, high)));
    }
    Ok(None)
}

/// `centerOfRectangle(rectangle)`.
fn center_of_rectangle(rectangle: Opt) -> Result<Option<LatLon>> {
    if !is_truthy(rectangle) {
        return Ok(None);
    }
    if let Some((low, high)) = low_high(rectangle)? {
        return Ok(Some(LatLon { lat: (low.lat + high.lat) / 2.0, lon: (low.lon + high.lon) / 2.0 }));
    }
    Ok(rectangle_bounds(rectangle)
        .map(|[north, south, east, west]| LatLon { lat: (north + south) / 2.0, lon: (east + west) / 2.0 }))
}

/// `rectangleGeo(rectangle)`.
fn rectangle_geo(rectangle: Opt) -> Result<Option<Value>> {
    if !is_truthy(rectangle) {
        return Ok(None);
    }
    if let Some((low, high)) = low_high(rectangle)? {
        return Ok(Some(json!({
            "box": {
                "minLat": num(low.lat.min(high.lat)),
                "minLon": num(low.lon.min(high.lon)),
                "maxLat": num(low.lat.max(high.lat)),
                "maxLon": num(low.lon.max(high.lon))
            }
        })));
    }
    Ok(rectangle_bounds(rectangle).map(|[north, south, east, west]| {
        json!({ "box": { "minLat": num(south), "minLon": num(west), "maxLat": num(north), "maxLon": num(east) } })
    }))
}

struct Circle {
    center: LatLon,
    radius_meters: Option<f64>,
}

/// `circleFrom(value)`.
fn circle_from(value: Opt) -> Result<Option<Circle>> {
    let circle = if is_truthy(prop(value, "circle")) {
        prop(value, "circle")
    } else if is_truthy(prop(value, "center")) {
        value
    } else {
        None
    };
    if !is_truthy(circle) {
        return Ok(None);
    }
    let center = normalize_location(prop(circle, "center"), "circle.center")?;
    let radius = js_number(nullish(prop(circle, "radius"), prop(circle, "radiusMeters")));
    Ok(Some(Circle { center, radius_meters: (radius.is_finite() && radius > 0.0).then_some(radius) }))
}

/// `requestBias(request)`.
fn request_bias(request: &Value) -> Result<Option<LatLon>> {
    let bias = prop(Some(request), "locationBias");
    if let Some(circle) = circle_from(bias)? {
        return Ok(Some(circle.center));
    }
    center_of_rectangle(or(prop(bias, "rectangle"), bias))
}

/// `requestRestriction(request, { distanceSort })`.
fn request_restriction(request: &Value, distance_sort: bool) -> Result<Option<Value>> {
    let restriction = prop(Some(request), "locationRestriction");
    if let Some(circle) = circle_from(restriction)? {
        let Some(radius) = circle.radius_meters else {
            return Err(Error::new("A circular locationRestriction needs a positive radius."));
        };
        let mut near = circle.center.to_map();
        near.insert("radiusMeters".into(), num(radius));
        let mut out = Map::new();
        out.insert("near".into(), Value::Object(near));
        if distance_sort {
            out.insert("sort".into(), json!("distance"));
        }
        return Ok(Some(Value::Object(out)));
    }
    rectangle_geo(or(prop(restriction, "rectangle"), restriction))
}

/// `canonicalType(value)`.
pub fn canonical_type(value: &Value) -> String {
    let text = if truthy(value) { js_string(value) } else { String::new() };
    let type_ = js_trim(&text);
    match GOOGLE_TYPE_ALIASES.iter().find(|(key, _)| *key == type_) {
        Some((_, alias)) => alias.to_string(),
        None => type_.replace('_', " "),
    }
}

fn array_items(value: Opt<'_>) -> &[Value] {
    match value {
        Some(Value::Array(items)) => items,
        _ => &[],
    }
}

/// `resultTypes(result)`: distinct stringified types, categories and type.
fn result_types(result: &Value) -> Vec<String> {
    let result = Some(result);
    let category = prop(result, "category");
    let mut raw: Vec<&Value> = Vec::new();
    raw.extend(array_items(prop(result, "types")));
    raw.extend(array_items(category));
    if let Some(type_) = prop(result, "type") {
        raw.push(type_);
    }
    if let Some(category @ Value::String(_)) = category {
        raw.push(category);
    }
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for value in raw {
        if !truthy(value) {
            continue;
        }
        let text = js_string(value);
        if seen.insert(text.clone()) {
            out.push(text);
        }
    }
    out
}

/// `[...].filter(Boolean).join(sep)` over property values.
fn join_truthy(values: &[Opt], separator: &str) -> String {
    values.iter().filter(|value| is_truthy(**value)).map(|value| js_string(value.unwrap())).collect::<Vec<_>>().join(separator)
}

/// `formattedAddress(result)`.
fn formatted_address(result: &Value) -> String {
    let result = Some(result);
    if is_truthy(prop(result, "formattedAddress")) {
        return js_string(prop(result, "formattedAddress").unwrap());
    }
    if is_truthy(prop(result, "address")) {
        return js_string(prop(result, "address").unwrap());
    }
    let street = Value::String(join_truthy(&[prop(result, "house_number"), prop(result, "street")], " "));
    join_truthy(
        &[
            Some(&street),
            prop(result, "city"),
            prop(result, "state"),
            prop(result, "postcode"),
            prop(result, "country"),
        ],
        ", ",
    )
}

/// `sourceId(result)`.
fn source_id(result: &Value) -> Option<String> {
    let result = Some(result);
    if is_present(prop(result, "id")) {
        return Some(js_string(prop(result, "id").unwrap()));
    }
    if is_truthy(prop(result, "osm_type")) && is_present(prop(result, "osm_id")) {
        return Some(format!("{}/{}", js_template(prop(result, "osm_type")), js_template(prop(result, "osm_id"))));
    }
    None
}

fn or_null(value: Opt) -> Value {
    value.cloned().unwrap_or(Value::Null)
}

/// `value ?? null`.
fn nullish_null(value: Opt) -> Value {
    if is_present(value) {
        value.cloned().unwrap()
    } else {
        Value::Null
    }
}

/// `value || null`.
fn truthy_or_null(value: Opt) -> Value {
    if is_truthy(value) {
        value.cloned().unwrap()
    } else {
        Value::Null
    }
}

/// `toMigrationPlace(result)`: a migration-friendly Place object (not a
/// byte-for-byte Google Place).
pub fn to_migration_place(result: &Value) -> Value {
    let id = source_id(result);
    let r = Some(result);
    let lat = js_number(prop(r, "lat"));
    let lon = js_number(nullish(prop(r, "lon"), prop(r, "lng")));
    let types = result_types(result);
    let empty = Value::Object(Map::new());
    let details_value = if is_truthy(prop(r, "details")) { prop(r, "details").unwrap() } else { &empty };
    let details = Some(details_value);
    let mut out = Map::new();
    if let Some(id) = &id {
        if !id.is_empty() {
            out.insert("id".into(), Value::String(id.clone()));
        }
    }
    let formatted = formatted_address(result);
    let display = {
        let name = or(prop(r, "name"), prop(r, "title"));
        if is_truthy(name) {
            js_string(name.unwrap())
        } else if !formatted.is_empty() {
            formatted.clone()
        } else {
            id.clone().unwrap_or_default()
        }
    };
    out.insert("displayName".into(), json!({ "text": display }));
    out.insert("formattedAddress".into(), Value::String(formatted));
    if lat.is_finite() && lon.is_finite() {
        out.insert("location".into(), json!({ "latitude": num(lat), "longitude": num(lon) }));
    }
    if let Some(first) = types.first().filter(|first| !first.is_empty()) {
        out.insert("primaryType".into(), Value::String(first.clone()));
    }
    out.insert("types".into(), Value::Array(types.iter().cloned().map(Value::String).collect()));
    let opening = prop(details, "opening_hours");
    if is_truthy(opening) || is_present(prop(r, "openNow")) {
        let state = prop(r, "openingHoursState");
        out.insert(
            "openingHours".into(),
            json!({
                "osmExpression": truthy_or_null(opening),
                "openNow": nullish_null(prop(r, "openNow")),
                "state": if is_truthy(state) { state.cloned().unwrap() } else { json!("unknown") }
            }),
        );
    }
    if is_truthy(prop(details, "wheelchair")) {
        out.insert("accessibilityOptions".into(), json!({ "wheelchair": or_null(prop(details, "wheelchair")) }));
    }
    if is_truthy(prop(details, "phone")) {
        out.insert("nationalPhoneNumber".into(), or_null(prop(details, "phone")));
    }
    if is_truthy(prop(details, "website")) {
        out.insert("websiteUri".into(), or_null(prop(details, "website")));
    }
    if is_truthy(prop(r, "geometry")) {
        out.insert("geometry".into(), or_null(prop(r, "geometry")));
    }
    out.insert("details".into(), details_value.clone());
    let dataset = prop(r, "source");
    out.insert(
        "source".into(),
        json!({
            "dataset": if is_truthy(dataset) { js_string(dataset.unwrap()) } else { "OpenStreetMap".to_string() },
            "osmType": truthy_or_null(prop(r, "osm_type")),
            "osmId": nullish_null(prop(r, "osm_id"))
        }),
    );
    let mut rangefind = Map::new();
    for key in [
        "score",
        "shard",
        "distanceMeters",
        "routeDistanceMeters",
        "routeProgressMeters",
        "routeProgressRatio",
        "routeBearingDegrees",
        "routeRank",
        "rejoinPoint",
        "constraintMatches",
        "locationType",
        "reverseGeocodeAccuracy",
    ] {
        rangefind.insert(key.into(), nullish_null(prop(r, key)));
    }
    out.insert("rangefind".into(), Value::Object(rangefind));
    Value::Object(out)
}

/// `responseMeta(response)`.
fn response_meta(response: &Value) -> Value {
    let r = Some(response);
    let total = match prop(r, "total") {
        Some(total) if !total.is_null() => total.clone(),
        _ => match prop(r, "results") {
            Some(Value::Array(items)) => json!(items.len()),
            Some(Value::String(text)) => json!(text.encode_utf16().count()),
            _ => json!(0),
        },
    };
    json!({
        "total": total,
        "approximate": prop(r, "approximate") == Some(&Value::Bool(true)),
        "correctedQuery": truthy_or_null(prop(r, "correctedQuery")),
        "stats": truthy_or_null(prop(r, "stats"))
    })
}

fn status_for(items: &[Value]) -> &'static str {
    if items.is_empty() {
        "ZERO_RESULTS"
    } else {
        "OK"
    }
}

/// `createRangefindMapsAdapter(engine, options)` options, minus the
/// query-function overrides (see [`OsmQueryBackend`]).
#[derive(Clone, Debug, Default)]
pub struct MapsAdapterOptions {
    /// `options.defaults` (`near`, `trace`, `timeZone`).
    pub defaults: Value,
}

/// The adapter object `createRangefindMapsAdapter` returns.
pub struct RangefindMapsAdapter<B> {
    backend: B,
    defaults: Value,
    place_cache: RefCell<HashMap<String, Value>>,
}

/// `createRangefindMapsAdapter(engine, options)`.
pub fn create_rangefind_maps_adapter<B: OsmQueryBackend>(backend: B, options: MapsAdapterOptions) -> RangefindMapsAdapter<B> {
    RangefindMapsAdapter { backend, defaults: options.defaults, place_cache: RefCell::new(HashMap::new()) }
}

/// `{ ...(request.constraints || {}), ...(request.openNow === true ? { openNow: true } : {}) }`.
fn request_constraints(request: &Value) -> Value {
    let mut out = Map::new();
    let constraints = prop(Some(request), "constraints");
    if is_truthy(constraints) {
        spread_into(&mut out, constraints);
    }
    if prop(Some(request), "openNow") == Some(&Value::Bool(true)) {
        out.insert("openNow".into(), Value::Bool(true));
    }
    Value::Object(out)
}

impl<B: OsmQueryBackend> RangefindMapsAdapter<B> {
    pub fn backend(&self) -> &B {
        &self.backend
    }

    fn default(&self, key: &str) -> Opt<'_> {
        prop(Some(&self.defaults), key)
    }

    /// `request.trace ?? defaults.trace`.
    fn put_trace(&self, params: &mut Map<String, Value>, request: &Value) {
        put(params, "trace", nullish(prop(Some(request), "trace"), self.default("trace")));
    }

    /// `...(request.timeZone || defaults.timeZone ? { timeZone: … } : {})`.
    fn put_time_zone_if_set(&self, params: &mut Map<String, Value>, request: &Value) {
        let zone = or(prop(Some(request), "timeZone"), self.default("timeZone"));
        if is_truthy(zone) {
            put(params, "timeZone", zone);
        }
    }

    fn map_places(&self, response: &Value) -> Vec<Value> {
        let places: Vec<Value> = array_items(prop(Some(response), "results")).iter().map(to_migration_place).collect();
        let mut cache = self.place_cache.borrow_mut();
        for place in &places {
            if let Some(Value::String(id)) = place.get("id") {
                if !id.is_empty() {
                    cache.insert(id.clone(), place.clone());
                }
            }
        }
        places
    }

    /// `autocomplete(request)`.
    pub async fn autocomplete(&self, request: &Value) -> Result<Value> {
        let r = Some(request);
        let input = match nullish(prop(r, "input"), prop(r, "query")) {
            Some(value) if !value.is_null() => js_string(value),
            _ => String::new(),
        };
        let size = clamp_size(nullish(prop(r, "maxResultCount"), prop(r, "size")), 8.0, 50.0);
        let bias = request_bias(request)?.map(LatLon::to_json);
        let near = if bias.is_some() { bias.as_ref() } else { self.default("near") };
        let mut params = Map::new();
        params.insert("query".into(), Value::String(input.clone()));
        params.insert("q".into(), Value::String(input));
        params.insert("size".into(), num(size));
        params.insert("limit".into(), num(size));
        put(&mut params, "inputOffset", prop(r, "inputOffset"));
        if is_truthy(near) {
            put(&mut params, "near", near);
        }
        if is_truthy(prop(r, "shards")) {
            put(&mut params, "shards", prop(r, "shards"));
        }
        self.put_trace(&mut params, request);
        let response = self.backend.suggest_osm_query(Value::Object(params)).await?;
        let included: HashSet<String> = array_items(prop(r, "includedPrimaryTypes")).iter().map(canonical_type).collect();
        let empty = Vec::new();
        let suggestions: Vec<Value> = array_items(prop(Some(&response), "suggestions"))
            .iter()
            .filter(|item| {
                included.is_empty()
                    || array_items(prop(Some(item), "types"))
                        .iter()
                        .map(canonical_type)
                        .any(|type_| included.contains(&type_))
            })
            .map(|item| {
                let item = Some(item);
                let mut text = Map::new();
                put(&mut text, "text", or(prop(item, "description"), prop(item, "text")));
                let mut main = Map::new();
                put(&mut main, "text", or(prop(item, "mainText"), prop(item, "text")));
                let secondary = prop(item, "secondaryText");
                let selection = if is_truthy(prop(item, "selection")) {
                    prop(item, "selection").cloned().unwrap()
                } else {
                    let mut selection = Map::new();
                    put(&mut selection, "query", prop(item, "text"));
                    Value::Object(selection)
                };
                let types = prop(item, "types");
                let ranges = prop(item, "matchedRanges");
                json!({
                    "placePrediction": {
                        "text": text,
                        "structuredFormat": {
                            "mainText": main,
                            "secondaryText": { "text": if is_truthy(secondary) { secondary.cloned().unwrap() } else { json!("") } }
                        },
                        "types": if is_truthy(types) { types.cloned().unwrap() } else { Value::Array(empty.clone()) },
                        "matchedRanges": if is_truthy(ranges) { ranges.cloned().unwrap() } else { Value::Array(empty.clone()) },
                        "rangefindSelection": selection
                    }
                })
            })
            .collect();
        let status = status_for(&suggestions);
        Ok(json!({
            "suggestions": suggestions,
            "status": status,
            "rangefind": {
                "normalizedQuery": truthy_or_null(prop(Some(&response), "normalizedQuery")),
                "stats": truthy_or_null(prop(Some(&response), "stats"))
            }
        }))
    }

    /// `textSearch(request)`.
    pub async fn text_search(&self, request: &Value) -> Result<Value> {
        let r = Some(request);
        let base = match nullish(prop(r, "textQuery"), prop(r, "query")) {
            Some(value) if !value.is_null() => js_string(value),
            _ => String::new(),
        };
        let mut query = js_trim(&base).to_string();
        if is_truthy(prop(r, "includedType")) {
            query = js_trim(&format!("{} {query}", canonical_type(prop(r, "includedType").unwrap()))).to_string();
        }
        let size = clamp_size(nullish(prop(r, "maxResultCount"), prop(r, "pageSize")), 10.0, 50.0);
        let geo = request_restriction(request, false)?;
        let bias = if geo.is_some() { None } else { request_bias(request)?.map(LatLon::to_json) };
        let near = if geo.is_some() {
            None
        } else if bias.is_some() {
            bias.as_ref()
        } else {
            self.default("near")
        };
        let mut params = Map::new();
        params.insert("query".into(), Value::String(query.clone()));
        params.insert("q".into(), Value::String(query));
        params.insert("size".into(), num(size));
        params.insert("limit".into(), num(size));
        params.insert("constraints".into(), request_constraints(request));
        if let Some(geo) = &geo {
            params.insert("geo".into(), geo.clone());
        }
        if is_truthy(near) {
            put(&mut params, "near", near);
        }
        self.put_time_zone_if_set(&mut params, request);
        if is_truthy(prop(r, "at")) {
            put(&mut params, "at", prop(r, "at"));
        }
        if is_truthy(prop(r, "shards")) {
            put(&mut params, "shards", prop(r, "shards"));
        }
        self.put_trace(&mut params, request);
        let response = self.backend.search_osm_query(Value::Object(params)).await?;
        let places = self.map_places(&response);
        Ok(json!({ "places": places, "status": status_for(&places), "rangefind": response_meta(&response) }))
    }

    /// `nearbySearch(request)`.
    pub async fn nearby_search(&self, request: &Value) -> Result<Value> {
        let r = Some(request);
        let popularity = prop(r, "rankPreference") == Some(&json!("POPULARITY"));
        let geo = request_restriction(request, !popularity)?;
        let Some(geo) = geo.filter(|geo| is_truthy(geo.get("near"))) else {
            return Err(type_error("nearbySearch needs a circular locationRestriction.".into()));
        };
        let size = clamp_size(prop(r, "maxResultCount"), 10.0, 50.0);
        let included: Vec<String> =
            array_items(prop(r, "includedTypes")).iter().map(canonical_type).filter(|type_| !type_.is_empty()).collect();
        let queries = if included.is_empty() { vec![String::new()] } else { included.clone() };
        let fetch_size = num(50f64.min(size * 2.0));
        let calls = queries.iter().map(|query| {
            let mut params = Map::new();
            params.insert("query".into(), Value::String(query.clone()));
            params.insert("q".into(), Value::String(query.clone()));
            params.insert("geo".into(), geo.clone());
            params.insert("size".into(), fetch_size.clone());
            params.insert("limit".into(), fetch_size.clone());
            params.insert("constraints".into(), request_constraints(request));
            self.put_time_zone_if_set(&mut params, request);
            self.put_trace(&mut params, request);
            self.backend.search_osm_query(Value::Object(params))
        });
        let responses = crate::rt::try_join_all(calls).await?;
        let excluded: Vec<String> = array_items(prop(r, "excludedTypes")).iter().map(canonical_type).collect();
        let included_set: Vec<String> = {
            let mut seen = HashSet::new();
            included.iter().filter(|type_| seen.insert((*type_).clone())).cloned().collect()
        };
        // `new Map()` keyed by source id: insertion ordered, value replaced
        // in place by a higher score.
        let mut order: Vec<String> = Vec::new();
        let mut merged: HashMap<String, Value> = HashMap::new();
        for response in &responses {
            for result in array_items(prop(Some(response), "results")) {
                let types: HashSet<String> = result_types(result).iter().map(|t| canonical_type(&Value::String(t.clone()))).collect();
                if !included_set.is_empty() && !included_set.iter().any(|type_| types.contains(type_)) {
                    continue;
                }
                if excluded.iter().any(|type_| types.contains(type_)) {
                    continue;
                }
                let key = source_id(result).filter(|id| !id.is_empty()).unwrap_or_else(|| {
                    let rr = Some(result);
                    format!(
                        "{}:{}:{}",
                        js_template(prop(rr, "lat")),
                        js_template(prop(rr, "lon")),
                        js_template(or(prop(rr, "name"), prop(rr, "title")))
                    )
                });
                let score = |value: &Value| {
                    let s = prop(Some(value), "score");
                    if is_truthy(s) {
                        js_number(s)
                    } else {
                        0.0
                    }
                };
                match merged.get(&key) {
                    None => {
                        order.push(key.clone());
                        merged.insert(key, result.clone());
                    }
                    Some(prior) => {
                        if score(result) > score(prior) {
                            merged.insert(key, result.clone());
                        }
                    }
                }
            }
        }
        let mut rows: Vec<Value> = order.iter().map(|key| merged.remove(key).unwrap()).collect();
        let by = |delta: f64| if delta < 0.0 { Ordering::Less } else if delta > 0.0 { Ordering::Greater } else { Ordering::Equal };
        if !popularity {
            let distance = |value: &Value| {
                let d = prop(Some(value), "distanceMeters");
                if is_present(d) {
                    js_number(d)
                } else {
                    f64::INFINITY
                }
            };
            rows.sort_by(|a, b| by(distance(a) - distance(b)));
        } else {
            let score = |value: &Value| {
                let s = prop(Some(value), "score");
                if is_truthy(s) {
                    js_number(s)
                } else {
                    0.0
                }
            };
            rows.sort_by(|a, b| by(score(b) - score(a)));
        }
        let total = rows.len();
        rows.truncate(size as usize);
        let synthetic = json!({
            "results": rows,
            "total": total,
            "stats": { "queries": responses.iter().map(|item| or_null(prop(Some(item), "stats"))).collect::<Vec<_>>() }
        });
        let places = self.map_places(&synthetic);
        Ok(json!({ "places": places, "status": status_for(&places), "rangefind": response_meta(&synthetic) }))
    }

    /// `geocode(request)`.
    pub async fn geocode(&self, request: &Value) -> Result<Value> {
        let r = Some(request);
        let base = match nullish(prop(r, "address"), prop(r, "query")) {
            Some(value) if !value.is_null() => js_string(value),
            _ => String::new(),
        };
        let address = js_trim(&base).to_string();
        if address.is_empty() {
            return Err(type_error("geocode needs an address.".into()));
        }
        let size = clamp_size(prop(r, "maxResultCount"), 5.0, 25.0);
        let bias = request_bias(request)?.map(LatLon::to_json);
        let near = if bias.is_some() { bias.as_ref() } else { self.default("near") };
        let mut params = Map::new();
        params.insert("query".into(), Value::String(address.clone()));
        params.insert("q".into(), Value::String(address));
        params.insert("size".into(), num(size));
        params.insert("limit".into(), num(size));
        if is_truthy(near) {
            put(&mut params, "near", near);
        }
        self.put_trace(&mut params, request);
        let response = self.backend.search_osm_query(Value::Object(params)).await?;
        let results = self.map_places(&response);
        Ok(json!({ "results": results, "status": status_for(&results), "rangefind": response_meta(&response) }))
    }

    /// `reverseGeocode(request)`.
    pub async fn reverse_geocode(&self, request: &Value) -> Result<Value> {
        let r = Some(request);
        let location = normalize_location(or(prop(r, "location"), r), "reverseGeocode.location")?;
        let mut params = location.to_map();
        put(&mut params, "radiusMeters", prop(r, "radiusMeters"));
        params.insert("size".into(), num(clamp_size(nullish(prop(r, "maxResultCount"), prop(r, "size")), 8.0, 25.0)));
        for key in ["resultTypes", "locationTypes", "localityRadiusMeters", "shards"] {
            put(&mut params, key, prop(r, key));
        }
        self.put_trace(&mut params, request);
        let response = self.backend.reverse_geocode_osm(Value::Object(params)).await?;
        let results = self.map_places(&response);
        Ok(json!({ "results": results, "status": status_for(&results), "rangefind": response_meta(&response) }))
    }

    /// `searchAlongRoute(request)`.
    pub async fn search_along_route(&self, request: &Value) -> Result<Value> {
        let r = Some(request);
        if !is_truthy(prop(r, "route")) {
            return Err(type_error("searchAlongRoute needs route geometry.".into()));
        }
        let query = match nullish(prop(r, "query"), prop(r, "textQuery")) {
            Some(value) if !value.is_null() => js_string(value),
            _ => String::new(),
        };
        let mut params = Map::new();
        put(&mut params, "route", prop(r, "route"));
        params.insert("query".into(), Value::String(query));
        for key in ["corridorMeters", "polylinePrecision", "routePositionMeters", "routeDirection", "viewport"] {
            put(&mut params, key, prop(r, key));
        }
        params.insert("constraints".into(), request_constraints(request));
        put(&mut params, "timeZone", or(prop(r, "timeZone"), self.default("timeZone")));
        put(&mut params, "at", prop(r, "at"));
        params.insert("limit".into(), num(clamp_size(nullish(prop(r, "maxResultCount"), prop(r, "limit")), 20.0, 50.0)));
        put(&mut params, "shards", prop(r, "shards"));
        self.put_trace(&mut params, request);
        let response = self.backend.search_along_route_osm(Value::Object(params)).await?;
        let places = self.map_places(&response);
        Ok(json!({ "places": places, "status": status_for(&places), "rangefind": response_meta(&response) }))
    }

    /// `placeDetails(request)`: served from the places earlier calls
    /// returned.
    pub async fn place_details(&self, request: &Value) -> Result<Value> {
        let r = Some(request);
        let place_id = match nullish(prop(r, "placeId"), prop(r, "id")) {
            Some(value) if !value.is_null() => js_string(value),
            _ => String::new(),
        };
        if place_id.is_empty() {
            return Err(type_error("placeDetails needs placeId.".into()));
        }
        let Some(place) = self.place_cache.borrow().get(&place_id).cloned() else {
            return Err(Error::new(format!(
                "Place {place_id} is not in this adapter's result cache. Rangefind returns indexed details inline; retain the selected Place instead of issuing a second request."
            )));
        };
        Ok(json!({ "place": place, "status": "OK" }))
    }

    /// `clearPlaceCache()`.
    pub fn clear_place_cache(&self) {
        self.place_cache.borrow_mut().clear();
    }
}
