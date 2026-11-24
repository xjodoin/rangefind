//! `reverseGeocodeOsm`, `anchoredExactText`, `searchOsmQueryBase`,
//! `searchOsmQuery`, `searchAlongRouteOsm` and the suggestion entry points.

use std::cell::RefCell;
use std::cmp::Ordering;

use serde_json::{json, Value};

use super::core::*;
use super::deps::{
    compile_osm_constraint_filters, evaluate_and_annotate, fold, fold_value, lookup_category, lookup_category_fuzzy, parse_osm_constraints,
    within_one_edit, CategoryIntent,
};
use super::host::{Ctx, LocalFuture, Locality};
use super::js::*;
use super::locality::*;
use crate::error::{Error, Result};
use crate::osm::constraints::{ConstraintOptions, OsmConstraints};
use crate::rt::join_all;

/// Insert `key` when `value` is defined, remove it otherwise (JS
/// `{ ...obj, key: maybeUndefined }` serialization).
fn set_opt(target: &mut Obj, key: &str, value: Option<Value>) {
    match value {
        Some(value) => {
            target.insert(key.to_string(), value);
        }
        None => {
            target.shift_remove(key);
        }
    }
}

fn geo_box(params: &Value) -> Option<&Value> {
    field(params, "geo").and_then(|geo| field(geo, "box")).filter(|value| truthy_value(value))
}

fn shards_or(params: &Value, scope: &[String]) -> Value {
    match field(params, "shards") {
        None | Some(Value::Null) => strings(scope),
        Some(value) => value.clone(),
    }
}

fn near_geo(anchor: LatLon, radius: f64, sort_distance: bool) -> Value {
    if sort_distance {
        json!({ "near": anchor.near(Some(radius)), "sort": "distance" })
    } else {
        json!({ "near": anchor.near(Some(radius)), "boost": near_text_boost() })
    }
}

fn total_of(response: &Value) -> f64 {
    number(field(response, "total"))
}

/// `(left, right) => Number(left.distanceMeters ?? Infinity) - Number(right.distanceMeters ?? Infinity)`.
fn by_distance(left: &Value, right: &Value) -> Ordering {
    let value = |item: &Value| number_nullish(field(item, "distanceMeters"), f64::INFINITY);
    compare_numbers(value(left), value(right))
}

/// A JS comparator returning `a - b`.
pub fn compare_numbers(a: f64, b: f64) -> Ordering {
    let delta = a - b;
    if delta < 0.0 {
        Ordering::Less
    } else if delta > 0.0 {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

// ------------------------------------------------------------ reverse geocoding

const REVERSE_FIELDS: &[(&str, &str)] = &[
    ("house_number", "street_number"),
    ("street", "route"),
    ("unit", "subpremise"),
    ("suburb", "sublocality"),
    ("city", "locality"),
    ("district", "administrative_area_level_2"),
    ("state", "administrative_area_level_1"),
    ("postcode", "postal_code"),
    ("country", "country"),
];

fn reverse_address_components(result: &Value) -> Value {
    Value::Array(
        REVERSE_FIELDS
            .iter()
            .filter_map(|(key, type_)| {
                let value = trim(&js_string_or_empty(field(result, key))).to_string();
                (!value.is_empty()).then(|| json!({ "longText": value, "shortText": value, "types": [type_] }))
            })
            .collect(),
    )
}

fn first_truthy<'a>(result: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().map(|key| field(result, key)).find(|value| is_truthy(*value)).flatten()
}

fn reverse_address_result(result: &Value) -> Value {
    let address = trim(&js_string_or_empty(first_truthy(result, &["address", "name", "title"]))).to_string();
    let ranged = field(result, "type") == Some(&json!("interpolated_address_range"));
    let precise = is_truthy(field(result, "house_number"));
    let mut out = spread(result);
    if !address.is_empty() {
        out.insert("address".into(), Value::String(address.clone()));
        out.insert("formattedAddress".into(), Value::String(address));
    }
    out.insert("addressComponents".into(), reverse_address_components(result));
    out.insert("reverseGeocodeAccuracy".into(), json!(if ranged { "range" } else if precise { "address-point" } else { "address" }));
    out.insert("locationType".into(), json!(if ranged { "RANGE_INTERPOLATED" } else if precise { "ROOFTOP" } else { "GEOMETRIC_CENTER" }));
    out.insert("types".into(), json!([if ranged || precise { "street_address" } else { "route" }]));
    Value::Object(out)
}

fn reverse_locality_result(result: &Value) -> Value {
    let address = trim(&js_string_or_empty(first_truthy(result, &["name", "title"]))).to_string();
    let locality_type = if is_locality_type(field(result, "type")) { field(result, "type").cloned().unwrap() } else { json!("locality") };
    let mut out = spread(result);
    if !address.is_empty() {
        out.insert("address".into(), Value::String(address.clone()));
        out.insert("formattedAddress".into(), Value::String(address.clone()));
    }
    let city = value_or(field(result, "city"), &address);
    out.insert("addressComponents".into(), reverse_address_components(&with(result, vec![("city", city)])));
    out.insert("reverseGeocodeAccuracy".into(), json!("locality"));
    out.insert("locationType".into(), json!("APPROXIMATE"));
    out.insert("types".into(), json!([locality_type]));
    Value::Object(out)
}

fn string_set(raw: &Value, plural: &str, singular: &str, upper: bool) -> Vec<String> {
    let values = first_truthy(raw, &[plural, singular]);
    let items: Vec<&Value> = match values {
        Some(Value::Array(items)) => items.iter().collect(),
        Some(value) if truthy_value(value) => vec![value],
        _ => Vec::new(),
    };
    let mut out: Vec<String> = Vec::new();
    for item in items {
        let text = js_string(Some(item));
        let text = if upper { text.to_uppercase() } else { text };
        if !out.contains(&text) {
            out.push(text);
        }
    }
    out
}

fn matches_reverse_filters(result: &Value, result_types: &[String], location_types: &[String]) -> bool {
    let types_ok = result_types.is_empty()
        || field(result, "types").and_then(Value::as_array).map(|types| types.iter().any(|t| result_types.contains(&js_string(Some(t))))).unwrap_or(false);
    let location_ok = location_types.is_empty() || field(result, "locationType").map(|t| location_types.contains(&js_string(Some(t)))).unwrap_or(false);
    types_ok && location_ok
}

fn unique_reverse_address_results(results: Vec<Value>) -> Vec<Value> {
    let mut seen: Vec<String> = Vec::new();
    results
        .into_iter()
        .filter(|result| {
            let key = fold_value(field(result, "address"));
            if key.is_empty() || !seen.contains(&key) {
                if !key.is_empty() {
                    seen.push(key);
                }
                return true;
            }
            false
        })
        .collect()
}

/// `reverseGeocodeOsm(engine, rawParams)`.
pub async fn reverse_geocode(ctx: &Ctx<'_>, raw: &Value) -> Result<Value> {
    if !ctx.has("search") {
        return Err(Error::new("reverse geocoding requires a Rangefind search engine"));
    }
    let source = match field(raw, "coordinate") {
        Some(value) if truthy_value(value) => value,
        _ => raw,
    };
    let lat = number(field(source, "lat"));
    let lon = number(field(source, "lon"));
    if !lat.is_finite() || !lon.is_finite() || !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
        return Err(Error::new("reverse geocoding requires latitude in [-90, 90] and longitude in [-180, 180]"));
    }
    let coordinate = LatLon { lat, lon };
    let requested_radius = if nullish(field(raw, "radiusMeters")) { DEFAULT_REVERSE_GEOCODE_RADIUS_METERS } else { number(field(raw, "radiusMeters")) };
    if !requested_radius.is_finite() || requested_radius <= 0.0 {
        return Err(Error::new("reverse geocoding radiusMeters must be a positive number"));
    }
    let requested_size = number_nullish(field(raw, "size"), 10.0);
    let size = if requested_size.is_finite() && requested_size > 0.0 {
        js_min(MAX_REVERSE_GEOCODE_RESULTS, js_max(1.0, requested_size.floor()))
    } else {
        10.0
    };
    let explicit = explicit_shard_scope(raw);
    let coverage = if explicit.is_empty() { anchor_radius_coverage_shards(ctx, coordinate, requested_radius) } else { explicit.clone() };
    let sharded = at(&ctx.manifest, "shards").as_array().map(|shards| !shards.is_empty()).unwrap_or(false);
    let mut base_stats = Obj::new();
    base_stats.insert("plannerLane".into(), json!("osmReverseGeocode"));
    base_stats.insert("osmIntentCoordinate".into(), coordinate.to_json());
    base_stats.insert("osmIntentRadiusMeters".into(), num(requested_radius));
    if !coverage.is_empty() {
        base_stats.insert("osmIntentCoverageShards".into(), strings(&coverage));
    }
    let result_types = string_set(raw, "resultTypes", "resultType", false);
    let location_types = string_set(raw, "locationTypes", "locationType", true);
    if !nullish(field(raw, "localityRadiusMeters")) {
        let value = number(field(raw, "localityRadiusMeters"));
        if !value.is_finite() || value <= 0.0 {
            return Err(Error::new("reverse geocoding localityRadiusMeters must be a positive number"));
        }
    }
    if sharded && coverage.is_empty() {
        let mut stats = base_stats.clone();
        stats.insert("shardsQueried".into(), json!(0));
        return Ok(json!({
            "total": 0, "page": 1, "size": num(size), "approximate": false, "results": [],
            "resolvedQuery": coordinate.label(),
            "stats": Value::Object(stats)
        }));
    }
    let mut engine_options = spread(raw);
    for key in [
        "coordinate", "lat", "lon", "radiusMeters", "reverseGeocode", "resultTypes", "resultType", "locationTypes", "locationType",
        "localityRadiusMeters", "q", "near", "geo", "filters", "shards", "page", "size",
    ] {
        engine_options.shift_remove(key);
    }
    let engine_options = Value::Object(engine_options);
    let filters = field(raw, "filters").filter(|v| truthy_value(v)).cloned().unwrap_or(json!({}));
    let mut request = set(engine_options.clone(), vec![("q", json!(""))]);
    if !coverage.is_empty() {
        request = set(request, vec![("shards", strings(&coverage))]);
    }
    let address_filters = with_facet_filter(&json!({ "filters": filters }), vec![("category".into(), json!(["address"]))]);
    request = with(
        &request,
        vec![
            ("filters", address_filters["filters"].clone()),
            ("geo", json!({ "near": coordinate.near(Some(requested_radius)), "sort": "distance" })),
            ("page", json!(1)),
            ("size", num(size)),
        ],
    );
    let response = ctx.search(request).await?;
    let results = unique_reverse_address_results(
        results_of(&response).iter().map(reverse_address_result).filter(|result| matches_reverse_filters(result, &result_types, &location_types)).collect(),
    );
    let locality_requested = (result_types.is_empty() || result_types.iter().any(|t| LOCALITY_TYPES.contains(&t.as_str()) || t == "locality"))
        && (location_types.is_empty() || location_types.iter().any(|t| t == "APPROXIMATE"));
    if results.is_empty() && locality_requested {
        let locality_radius = js_max(requested_radius, number_or(field(raw, "localityRadiusMeters"), DEFAULT_REVERSE_LOCALITY_RADIUS_METERS));
        let locality_coverage = if explicit.is_empty() { anchor_radius_coverage_shards(ctx, coordinate, locality_radius) } else { explicit.clone() };
        let candidate_size = js_min(64.0, js_max(32.0, size * 4.0));
        let mut request = set(engine_options.clone(), vec![("q", json!(""))]);
        if !locality_coverage.is_empty() {
            request = set(request, vec![("shards", strings(&locality_coverage))]);
        }
        request = with(
            &request,
            vec![
                ("filters", json!({ "facets": { "category": ["place"] } })),
                ("geo", json!({ "near": coordinate.near(Some(locality_radius)), "sort": "distance" })),
                ("page", json!(1)),
                ("size", num(candidate_size)),
            ],
        );
        let locality_response = ctx.search(request).await?;
        let candidates = unique_reverse_address_results(
            results_of(&locality_response)
                .iter()
                .filter(|result| is_locality_type(field(result, "type")))
                .map(reverse_locality_result)
                .filter(|result| matches_reverse_filters(result, &result_types, &location_types))
                .collect(),
        );
        let total = candidates.len();
        let localities = slice_to(candidates, size);
        let resolved = localities.first().and_then(|first| field(first, "address")).filter(|v| truthy_value(v)).cloned().unwrap_or(json!(coordinate.label()));
        let mut stats = stats_of(&locality_response);
        for (key, value) in &base_stats {
            stats.insert(key.clone(), value.clone());
        }
        stats.insert("plannerLane".into(), json!("osmReverseGeocodeLocality"));
        stats.insert("osmIntentLocalityRadiusMeters".into(), num(locality_radius));
        if !locality_coverage.is_empty() {
            stats.insert("osmIntentCoverageShards".into(), strings(&locality_coverage));
        }
        return Ok(with(
            &locality_response,
            vec![
                ("total", num(total as f64)),
                ("page", json!(1)),
                ("size", num(size)),
                ("results", Value::Array(localities)),
                ("resolvedQuery", resolved),
                ("stats", Value::Object(stats)),
            ],
        ));
    }
    let resolved = results.first().and_then(|first| field(first, "address")).filter(|v| truthy_value(v)).cloned().unwrap_or(json!(coordinate.label()));
    let mut stats = stats_of(&response);
    for (key, value) in &base_stats {
        stats.insert(key.clone(), value.clone());
    }
    Ok(with(
        &response,
        vec![
            ("total", num(results.len() as f64)),
            ("page", json!(1)),
            ("size", num(size)),
            ("results", Value::Array(results)),
            ("resolvedQuery", resolved),
            ("stats", Value::Object(stats)),
        ],
    ))
}

// ------------------------------------------------------------ speculation

/// The JS `speculativeNearest` promise: started early, consumed at most
/// twice. Hosts that can run work in the background start it right away;
/// otherwise it runs on first use (identical result, the index is static).
struct Speculative<'a> {
    future: RefCell<Option<LocalFuture<'a, Result<Value>>>>,
    settled: RefCell<Option<Result<Value>>>,
}

impl<'a> Speculative<'a> {
    fn new(future: LocalFuture<'a, Result<Value>>) -> Self {
        Speculative { future: RefCell::new(Some(future)), settled: RefCell::new(None) }
    }

    async fn settle(&self) -> Result<Value> {
        if let Some(result) = self.settled.borrow().clone() {
            return result;
        }
        let future = self.future.borrow_mut().take();
        let result = match future {
            Some(future) => future.await,
            None => Err(Error::new("speculative search already consumed")),
        };
        *self.settled.borrow_mut() = Some(result.clone());
        result
    }
}

async fn search_nearest_with_budget_fallback(ctx: &Ctx<'_>, params: Value) -> Result<Value> {
    match ctx.search(params.clone()).await {
        Ok(response) => Ok(response),
        Err(error) => {
            let geo = field(&params, "geo");
            let sort_distance = geo.and_then(|geo| field(geo, "sort")) == Some(&json!("distance"));
            let near = geo.and_then(|geo| field(geo, "near"));
            if !sort_distance || !is_truthy(near) || !is_geo_text_sort_budget_error(&error) {
                return Err(error);
            }
            let mut geo = spread(geo.unwrap());
            geo.shift_remove("sort");
            geo.insert("boost".into(), near_text_boost());
            let response = ctx.search(set(params, vec![("geo", Value::Object(geo))])).await?;
            let meters = |item: &Value| finite_number(field(item, "distanceMeters")).unwrap_or(f64::INFINITY);
            let mut results = results_of(&response);
            results.sort_by(|left, right| compare_numbers(meters(left), meters(right)));
            let out = set(response, vec![("results", Value::Array(results))]);
            Ok(set_stats(out, vec![("osmDistanceSortFallback", json!("geo-boost"))]))
        }
    }
}

/// `{ ...params, filters: ..., facets: { type: [intent.type] } }` when safe.
fn with_category(params: Value, safe: bool, category: &CategoryIntent) -> Value {
    if safe {
        with_type_facet(&params, &category.type_)
    } else {
        params
    }
}

fn bears_name(result: &Value, q: &str) -> bool {
    let name = name_or_title(result);
    fold_value(name) == fold(q) || within_one_folded_edit(name, q)
}

fn with_trace(response: Value, trace: Option<Value>) -> Value {
    match trace {
        Some(trace) => set_stats(response, vec![("trace", trace)]),
        None => {
            // `{ ...resolved, stats: { ...(resolved.stats || {}) } }`
            let stats = stats_of(&response);
            set(response, vec![("stats", Value::Object(stats))])
        }
    }
}

/// `anchoredExactText(engine, q, params, anchor, { allowUngatedFuzzy, intent })`.
async fn anchored_exact_text(
    ctx: &Ctx<'_>,
    q: &str,
    params: &Value,
    anchor: LatLon,
    allow_ungated_fuzzy: bool,
    intent: Option<&QueryIntent>,
) -> Result<Option<Value>> {
    let coverage = anchor_coverage_shards(ctx, anchor);
    let explicit = explicit_shard_scope(params);
    let direct_scope = if explicit.is_empty() { coverage } else { explicit };
    if !direct_scope.is_empty() {
        ctx.prefetch_shards(&direct_scope, false);
    }
    let folded_q = fold(q);
    let compound_name = alnum_tokens(&folded_q).len() >= 2;
    let viewport = geo_box(params).cloned();

    if intent.is_none() && compound_name && !direct_scope.is_empty() {
        let geo = match &viewport {
            Some(viewport) => json!({ "box": viewport, "near": anchor.near(None), "sort": "distance" }),
            None => near_geo(anchor, NEAR_TEXT_RADIUS_METERS, true),
        };
        match ctx.search(with(params, vec![("shards", strings(&direct_scope)), ("geo", geo)])).await {
            Ok(direct) => {
                let all = results_of(&direct);
                let canonical: Vec<Value> = all.iter().filter(|result| bears_name(result, q)).cloned().collect();
                if !canonical.is_empty() {
                    let response = if canonical.len() == all.len() {
                        direct.clone()
                    } else {
                        set(direct, vec![("total", num(canonical.len() as f64)), ("approximate", json!(true)), ("results", Value::Array(canonical))])
                    };
                    let (radius, lane) = if viewport.is_some() { (0.0, "osmViewportExactGeo") } else { (NEAR_TEXT_RADIUS_METERS, "osmNearExactGeo") };
                    let resolved = exact_geo_response(&response, None, &direct_scope, radius, lane);
                    return Ok(Some(set_stats(resolved, vec![("osmIntentLocalNameProof", json!(true))])));
                }
            }
            Err(error) => {
                if !is_geo_text_sort_budget_error(&error) {
                    return Err(error);
                }
            }
        }
    }

    let speculative = if intent.is_some() && !direct_scope.is_empty() && viewport.is_none() {
        ctx.prefetch_shards(&direct_scope, true);
        let request = with(params, vec![("shards", shards_or(params, &direct_scope)), ("geo", near_geo(anchor, NEAR_TEXT_RADIUS_METERS, true))]);
        Some(Speculative::new(ctx.start_search(request)))
    } else {
        None
    };
    let mut intent_authority: Option<Option<super::host::ExactAuthority>> = None;
    if intent.is_some() && allow_ungated_fuzzy {
        intent_authority = Some(exact_authority_scope(ctx, q).await);
    }

    if let Some(intent) = intent {
        if allow_ungated_fuzzy
            && intent_authority.as_ref().map(Option::is_none).unwrap_or(true)
            && !direct_scope.is_empty()
            && !shard_scope_supports_feature(ctx, &direct_scope, "facetSummaryUint32")
        {
            let requested_size = js_max(1.0, number_or(field(params, "size"), 10.0));
            let request = without(
                &with(
                    params,
                    vec![
                        ("q", Value::String(q.to_string())),
                        ("shards", strings(&direct_scope)),
                        ("size", num(js_min(64.0, js_max(32.0, requested_size * 2.0)))),
                    ],
                ),
                &["geo"],
            );
            let local_text = ctx.search(request).await?;
            let folded_locality = fold(&intent.locality);
            let intent_tokens: Vec<&str> = split_non_alnum(&folded_locality).into_iter().filter(|token| len16(token) >= 3).collect();
            struct Row {
                result: Value,
                rank: usize,
                category_match: bool,
                distance: f64,
            }
            let mut rows: Vec<Row> = Vec::new();
            for (rank, result) in results_of(&local_text).into_iter().enumerate() {
                let lat = number(field(&result, "lat"));
                let lon = number(field(&result, "lon"));
                if !lat.is_finite() || !lon.is_finite() {
                    continue;
                }
                if let Some(viewport) = &viewport {
                    let values = [
                        number(field(viewport, "minLat")),
                        number(field(viewport, "minLon")),
                        number(field(viewport, "maxLat")),
                        number(field(viewport, "maxLon")),
                    ];
                    if !bbox_numbers_contain(values, lat, lon) {
                        continue;
                    }
                }
                let distance = haversine_meters(anchor.lat, anchor.lon, lat, lon);
                if viewport.is_none() && distance > NEAR_TEXT_RADIUS_METERS {
                    continue;
                }
                let name = fold_value(name_or_title(&result));
                let words = alnum_tokens(&name);
                let matches_intent = intent_tokens
                    .iter()
                    .all(|token| words.iter().any(|word| word.contains(token) || token.contains(word) || within_one_edit(word, token)));
                if !matches_intent {
                    continue;
                }
                let category_match = js_string_or_empty(field(&result, "type")) == intent.category.type_;
                rows.push(Row { result: set(result, vec![("distanceMeters", num(distance))]), rank, category_match, distance });
            }
            rows.sort_by(|left, right| {
                (right.category_match as i32)
                    .cmp(&(left.category_match as i32))
                    .then(left.rank.cmp(&right.rank))
                    .then_with(|| compare_numbers(left.distance, right.distance))
            });
            if !rows.is_empty() {
                let total = rows.len();
                let results: Vec<Value> = slice_to(rows.into_iter().map(|row| row.result).collect(), requested_size);
                let approximate = total_of(&local_text) > results_len(&local_text) as f64;
                let out = with(
                    &local_text,
                    vec![("total", num(total as f64)), ("size", num(requested_size)), ("approximate", json!(approximate)), ("results", Value::Array(results))],
                );
                return Ok(Some(collapse_civic_duplicates(with_stats(
                    &out,
                    vec![
                        ("plannerLane", json!(if viewport.is_some() { "osmViewportIntentText" } else { "osmNearIntentText" })),
                        ("osmIntentRadiusMeters", num(if viewport.is_some() { 0.0 } else { NEAR_TEXT_RADIUS_METERS })),
                        ("osmIntentCategoryHint", json!(intent.category.type_)),
                        ("osmIntentCoverageShards", strings(&direct_scope)),
                    ],
                ))));
            }
        }
    }

    let authority = match intent_authority {
        Some(authority) => authority,
        None => exact_authority_scope(ctx, q).await,
    };
    if authority.is_none() && !allow_ungated_fuzzy {
        return Ok(None);
    }
    let authority_coverage: Vec<String> = match &authority {
        Some(authority) if !authority.shards.is_empty() => direct_scope.iter().filter(|shard| authority.shards.contains(shard)).cloned().collect(),
        _ => Vec::new(),
    };
    let scope = if authority_coverage.is_empty() { direct_scope.clone() } else { authority_coverage };
    let requested_size = js_max(1.0, number_or(field(params, "size"), 10.0));
    let intent_name_tokens: Vec<String> = match intent {
        Some(intent) if authority.is_none() && allow_ungated_fuzzy && !intent.has_connector() => {
            fold(&intent.locality).split(' ').filter(|token| len16(token) >= 3).map(str::to_string).collect()
        }
        _ => Vec::new(),
    };
    let distinctive = intent_name_tokens.last().cloned();
    let facet_safe = shard_scope_supports_feature(ctx, &scope, "facetSummaryUint32");
    let probe_params = match (&distinctive, intent) {
        (Some(token), Some(intent)) => with_category(with(params, vec![("q", Value::String(token.clone()))]), facet_safe, &intent.category),
        _ => params.clone(),
    };
    if !scope.is_empty() && viewport.is_some() && authority.is_some() {
        let request = with(
            params,
            vec![("shards", shards_or(params, &scope)), ("geo", json!({ "box": viewport.clone().unwrap(), "near": anchor.near(None), "sort": "distance" }))],
        );
        let response = ctx.search(request).await?;
        return Ok(Some(exact_geo_response(&response, authority.as_ref(), &scope, 0.0, "osmViewportExactGeo")));
    }
    let nearest_exact = |lane: &'static str| {
        let request = set(probe_params.clone(), vec![("shards", shards_or(params, &scope)), ("geo", near_geo(anchor, NEAR_TEXT_RADIUS_METERS, true))]);
        let authority = authority.clone();
        let scope = scope.clone();
        async move {
            let response = match ctx.search(request).await {
                Ok(response) => response,
                Err(error) => {
                    if is_geo_text_sort_budget_error(&error) {
                        return Ok(None);
                    }
                    return Err(error);
                }
            };
            if intent.is_none() {
                let all = results_of(&response);
                let bearers: Vec<Value> = all.iter().filter(|result| bears_name(result, q)).cloned().collect();
                if bearers.is_empty() {
                    return Ok(None);
                }
                if bearers.len() != all.len() {
                    let narrowed = set(response, vec![("total", num(bearers.len() as f64)), ("approximate", json!(true)), ("results", Value::Array(bearers))]);
                    return Ok(Some(exact_geo_response(&narrowed, authority.as_ref(), &scope, NEAR_TEXT_RADIUS_METERS, lane)));
                }
            }
            Ok::<_, Error>(Some(exact_geo_response(&response, authority.as_ref(), &scope, NEAR_TEXT_RADIUS_METERS, lane)))
        }
    };
    let authority_count = authority.as_ref().map(|authority| if authority.count != 0.0 && !authority.count.is_nan() { authority.count } else { 0.0 }).unwrap_or(0.0);
    if !scope.is_empty() && authority_count >= js_max(32.0, requested_size * 2.0) {
        if let Some(speculative) = &speculative {
            match speculative.settle().await {
                Ok(response) => {
                    return Ok(Some(exact_geo_response(&response, authority.as_ref(), &direct_scope, NEAR_TEXT_RADIUS_METERS, "osmNearExactGeo")));
                }
                Err(error) => {
                    if !is_geo_text_sort_budget_error(&error) {
                        return Err(error);
                    }
                }
            }
        } else if let Some(nearest) = nearest_exact("osmNearExactGeo").await? {
            return Ok(Some(nearest));
        }
    }
    let scoped_text = !scope.is_empty();
    let request = if scoped_text {
        without(
            &with(
                &probe_params,
                vec![("shards", shards_or(params, &scope)), ("size", num(js_min(100.0, js_max(32.0, requested_size * 3.0))))],
            ),
            &["geo"],
        )
    } else {
        set(probe_params.clone(), vec![("geo", near_geo(anchor, NEAR_TEXT_RADIUS_METERS, false))])
    };
    let response = ctx.search(request).await?;
    let corrected = field(&response, "correctedQuery").filter(|value| truthy_value(value) && within_one_folded_edit(Some(value), q)).cloned();
    if authority.is_none() && corrected.is_none() && allow_ungated_fuzzy {
        let named_fuzzy = results_of(&response).iter().any(|result| {
            let name = fold_value(name_or_title(result));
            if intent_name_tokens.is_empty() {
                within_one_folded_edit(Some(&Value::String(name)), q)
            } else {
                intent_name_tokens.iter().all(|token| name.contains(token.as_str()))
            }
        });
        if !named_fuzzy {
            return Ok(None);
        }
    }
    if scoped_text && total_of(&response) > results_len(&response) as f64 {
        match (&speculative, &authority) {
            (Some(speculative), Some(_)) => match speculative.settle().await {
                Ok(settled) => {
                    return Ok(Some(exact_geo_response(&settled, authority.as_ref(), &direct_scope, NEAR_TEXT_RADIUS_METERS, "osmNearExactGeo")));
                }
                Err(error) => {
                    if !is_geo_text_sort_budget_error(&error) {
                        return Err(error);
                    }
                }
            },
            _ => {
                let lane = if authority.is_some() { "osmNearExactGeo" } else { "osmNearFuzzyGeo" };
                if let Some(nearest) = nearest_exact(lane).await? {
                    return Ok(Some(nearest));
                }
            }
        }
    }
    let folded_query = fold(&match &corrected {
        Some(value) => js_string(Some(value)),
        None => q.to_string(),
    });
    let mut exact: Vec<Value> = Vec::new();
    let mut fuzzy: Vec<Value> = Vec::new();
    let mut other: Vec<Value> = Vec::new();
    for result in results_of(&response) {
        let distance = match (finite_number(field(&result, "lat")), finite_number(field(&result, "lon"))) {
            (Some(lat), Some(lon)) => Some(Value::from(haversine_meters(anchor.lat, anchor.lon, lat, lon))),
            _ => field(&result, "distanceMeters").cloned(),
        };
        let finite_distance = finite_number(distance.as_ref());
        if scoped_text && finite_distance.map(|d| d > NEAR_TEXT_RADIUS_METERS).unwrap_or(true) {
            continue;
        }
        let folded_name = fold_value(name_or_title(&result));
        let ranked = match finite_distance {
            Some(distance) => set(result, vec![("distanceMeters", num(distance))]),
            None => result,
        };
        if folded_name == folded_query {
            exact.push(ranked);
        } else if folded_name.contains(folded_query.as_str())
            || folded_query.split(' ').any(|token| len16(token) >= 4 && folded_name.contains(token))
        {
            fuzzy.push(ranked);
        } else {
            other.push(ranked);
        }
    }
    if intent.is_none() && !exact.iter().any(|result| is_locality_type(field(result, "type"))) {
        let lexicon = engine_category_lexicon(ctx).await;
        if possible_locality_query(q, &lexicon) {
            if let Some(locality) = resolve_locality(ctx, q, params, LocalityOptions::final_miss()).await? {
                let resolved = locality_exact_response(&locality, params, q);
                let trace = merge_runtime_traces(&[trace_of(&response), trace_of(&resolved)]);
                return Ok(Some(with_trace(resolved, trace)));
            }
        }
    }
    if (authority.is_none() || intent.is_none()) && exact.is_empty() && fuzzy.is_empty() {
        return Ok(None);
    }
    if intent.is_none() {
        exact.sort_by(|left, right| {
            let locality = |item: &Value| is_locality_type(field(item, "type")) as i32;
            locality(right).cmp(&locality(left)).then_with(|| by_distance(left, right))
        });
    } else {
        exact.sort_by(by_distance);
    }
    fuzzy.sort_by(by_distance);
    other.sort_by(by_distance);
    let candidates: Vec<Value> = exact.into_iter().chain(fuzzy).chain(other).collect();
    let candidate_count = candidates.len();
    let local_results = slice_to(candidates, requested_size);
    if local_results.is_empty() {
        return Ok(None);
    }
    let mut out = spread(&response);
    if scoped_text {
        out.insert("total".into(), num(candidate_count as f64));
    }
    out.insert("size".into(), num(requested_size));
    if scoped_text {
        out.insert("approximate".into(), json!(total_of(&response) > results_len(&response) as f64));
    } else {
        let approximate = field(&response, "approximate").cloned();
        set_opt(&mut out, "approximate", approximate);
    }
    out.insert("results".into(), Value::Array(local_results));
    let mut stats = vec![
        ("plannerLane", json!(if authority.is_some() { "osmNearExactText" } else { "osmNearFuzzyText" })),
        ("osmIntentRadiusMeters", num(NEAR_TEXT_RADIUS_METERS)),
    ];
    if let Some(token) = &distinctive {
        stats.push(("osmIntentCategoryFacet", json!(facet_safe)));
        stats.push(("osmIntentNameToken", json!(token)));
    }
    if !scope.is_empty() {
        stats.push(("osmIntentCoverageShards", strings(&scope)));
    }
    if let Some(authority) = authority.as_ref().filter(|a| !a.shards.is_empty()) {
        stats.push(("osmIntentAuthorityShards", strings(&authority.shards)));
    }
    Ok(Some(collapse_civic_duplicates(set_stats(Value::Object(out), stats))))
}

// ------------------------------------------------------------ search cascade

fn locality_coordinate(locality: &Locality, key: &str) -> Option<Value> {
    locality.get(key).cloned()
}

/// `searchOsmQueryBase(engine, rawParams)`.
async fn search_osm_query_base(ctx: &Ctx<'_>, raw: &Value) -> Result<Value> {
    let anchor = if nullish(field(raw, "geo")) { near_anchor(raw) } else { None };
    let params = without(raw, &["near"]);
    let q = trim(&js_string_or_empty(field(&params, "q"))).to_string();
    if let Some(coordinate) = parse_coordinate_intent(&q) {
        if field(raw, "reverseGeocode") == Some(&Value::Bool(false)) {
            return Ok(coordinate_response(coordinate, &params, &Obj::new(), "osmCoordinates"));
        }
        let mut request = spread(&params);
        if let Some(Value::Object(options)) = field(raw, "reverseGeocode") {
            for (key, value) in options {
                request.insert(key.clone(), value.clone());
            }
        }
        request.insert("lat".into(), num(coordinate.lat));
        request.insert("lon".into(), num(coordinate.lon));
        let reversed = reverse_geocode(ctx, &Value::Object(request)).await?;
        if results_len(&reversed) > 0 {
            return Ok(reversed);
        }
        let stats = stats_of(&reversed);
        return Ok(coordinate_response(coordinate, &params, &stats, "osmReverseGeocodeFallback"));
    }
    let lexicon = engine_category_lexicon(ctx).await;
    let nearby = parse_nearby_category_intent(&q, &lexicon);
    let intent = if nearby.is_some() { None } else { parse_osm_query_intent(&q, &lexicon) };
    let map_anchor = anchor.or_else(|| viewport_anchor(field(&params, "geo").and_then(|geo| field(geo, "box"))));
    let viewport_whole_name = geo_box(&params).is_some() && (split_ws(&q).len() >= 2 || has_non_ascii(&q));
    if let Some(map_anchor) = map_anchor {
        if !q.is_empty() && !intent.as_ref().map(QueryIntent::has_connector).unwrap_or(false) && nearby.is_none() && (anchor.is_some() || viewport_whole_name) {
            let allow = intent.as_ref().map(|intent| intent.order == "locality-category").unwrap_or(false);
            if let Some(found) = anchored_exact_text(ctx, &q, &params, map_anchor, allow, intent.as_ref()).await? {
                return Ok(found);
            }
        }
    }
    let Some(intent) = intent else {
        return search_without_intent(ctx, &q, &params, anchor, map_anchor, nearby, &lexicon).await;
    };
    let exact_intent_candidate = intent.order == "locality-category" || split_ws(&intent.locality).len() < 2;
    if map_anchor.is_none() && !intent.has_connector() && exact_intent_candidate {
        if let Some(found) = resolve_global_named_text(ctx, &q, &params, true).await? {
            return Ok(collapse_civic_duplicates(found));
        }
    }
    if let Some(found) = resolve_named_category_locality(ctx, &intent, &params).await? {
        return Ok(collapse_civic_duplicates(found));
    }
    let anchor_scope = match map_anchor {
        Some(map_anchor) if nullish(field(&params, "shards")) => anchor_coverage_shards(ctx, map_anchor),
        _ => Vec::new(),
    };
    let routed_params = if anchor_scope.is_empty() { params.clone() } else { set(params.clone(), vec![("shards", strings(&anchor_scope))]) };
    let mut locality_probe_stats: Option<Value> = None;
    let mut locality = if !intent.has_connector() {
        let (whole, split) = resolve_ambiguous_category_locality(ctx, &q, &intent.locality, &routed_params).await?;
        if let Some(whole) = whole {
            return Ok(locality_exact_response(&whole, &params, &q));
        }
        split
    } else {
        resolve_locality(ctx, &intent.locality, &routed_params, LocalityOptions::default()).await?
    };
    if locality.is_none() && !q.is_empty() {
        if let Some(map_anchor) = map_anchor {
            let explicit = explicit_shard_scope(&params);
            let local_scope = if explicit.is_empty() { anchor_radius_coverage_shards(ctx, map_anchor, NEAR_TEXT_RADIUS_METERS) } else { explicit };
            let intent_name_tokens: Vec<String> = if !intent.has_connector() {
                fold(&intent.locality).split(' ').filter(|token| len16(token) >= 3).map(str::to_string).collect()
            } else {
                Vec::new()
            };
            let distinctive = intent_name_tokens.last().cloned();
            let facet_safe = shard_scope_supports_feature(ctx, &local_scope, "facetSummaryUint32");
            let mut request = params.clone();
            if !local_scope.is_empty() {
                request = set(request, vec![("shards", strings(&local_scope))]);
            }
            if let Some(token) = &distinctive {
                request = set(request, vec![("q", json!(token))]);
                request = with_category(request, facet_safe, &intent.category);
            }
            let size = number_or(field(&params, "size"), 10.0);
            request = with(
                &request,
                vec![("size", num(js_min(100.0, js_max(32.0, size * 3.0)))), ("geo", near_geo(map_anchor, NEAR_TEXT_RADIUS_METERS, false))],
            );
            let local = ctx.search(request).await?;
            locality_probe_stats = field(&local, "stats").filter(|stats| truthy_value(stats)).cloned();
            let local_results: Vec<Value> = if distinctive.is_some() {
                results_of(&local)
                    .into_iter()
                    .filter(|result| {
                        let name = fold_value(name_or_title(result));
                        intent_name_tokens.iter().all(|token| name.contains(token.as_str()))
                    })
                    .collect()
            } else {
                results_of(&local)
            };
            if !local_results.is_empty() {
                let mut out = spread(&local);
                if distinctive.is_some() {
                    out.insert("total".into(), num(local_results.len() as f64));
                } else {
                    let total = field(&local, "total").cloned();
                    set_opt(&mut out, "total", total);
                }
                out.insert("size".into(), num(size));
                out.insert("results".into(), Value::Array(slice_to(local_results, size)));
                let mut stats = vec![
                    ("plannerLane", json!("osmNearText")),
                    ("osmIntentRadiusMeters", num(NEAR_TEXT_RADIUS_METERS)),
                    ("osmAmbiguousIntentTextFirst", json!(true)),
                ];
                if let Some(token) = &distinctive {
                    stats.push(("osmIntentCategoryFacet", json!(facet_safe)));
                    stats.push(("osmIntentNameToken", json!(token)));
                }
                if !local_scope.is_empty() {
                    stats.push(("osmIntentCoverageShards", strings(&local_scope)));
                }
                return Ok(collapse_civic_duplicates(set_stats(Value::Object(out), stats)));
            }
            locality = if !intent.has_connector() {
                let (whole, split) = resolve_ambiguous_category_locality(ctx, &q, &intent.locality, &params).await?;
                if let Some(whole) = whole {
                    return Ok(locality_exact_response(&whole, &params, &q));
                }
                split
            } else {
                resolve_locality(ctx, &intent.locality, &params, LocalityOptions::default()).await?
            };
        }
    }
    let Some(locality) = locality else {
        let response = search_nearest_with_budget_fallback(ctx, params.clone()).await?;
        let trace = merge_runtime_traces(&[locality_probe_stats.as_ref().and_then(|stats| field(stats, "trace")), trace_of(&response)]);
        let mut entries = Vec::new();
        if let Some(trace) = trace {
            entries.push(("trace", trace));
        }
        if locality_probe_stats.is_some() {
            entries.push(("osmNearFallback", json!(true)));
        }
        return Ok(collapse_civic_duplicates(set_stats(response, entries)));
    };

    let weak_distant = match anchor {
        Some(anchor) => {
            let population = number_or(locality.get("population"), 0.0);
            type_priority(locality.get("type")) <= 2.0
                && (population == 0.0 || population.is_nan())
                && haversine_meters(anchor.lat, anchor.lon, number(locality.get("lat")), number(locality.get("lon"))) > WEAK_LOCALITY_ANCHOR_METERS
        }
        None => false,
    };
    let mut weak_probe: Option<Value> = None;
    if weak_distant {
        let anchor = anchor.unwrap();
        let probe = ctx.search(set(params.clone(), vec![("geo", near_geo(anchor, NEAR_TEXT_RADIUS_METERS, false))])).await?;
        if total_of(&probe) > 0.0 && results_len(&probe) > 0 {
            return Ok(collapse_civic_duplicates(with_stats(
                &probe,
                vec![
                    ("plannerLane", json!("osmNearText")),
                    ("osmIntentRadiusMeters", num(NEAR_TEXT_RADIUS_METERS)),
                    ("osmWeakLocalityTextFirst", json!(true)),
                ],
            )));
        }
        weak_probe = Some(probe);
    }
    let shard = {
        let raw = js_string_or_empty(locality.get("shard"));
        raw.split('/').next().unwrap_or("").to_string()
    };
    let explicit = explicit_shard_scope(&params);
    let locality_scope = if !explicit.is_empty() {
        explicit
    } else if !shard.is_empty() {
        vec![shard.clone()]
    } else {
        Vec::new()
    };
    let facet_safe = shard_scope_supports_feature(ctx, &locality_scope, "facetSummaryUint32");
    let mut request = params.clone();
    if nullish(field(&params, "shards")) && !shard.is_empty() {
        request = set(request, vec![("shards", json!([shard]))]);
    }
    request = set(request, vec![("q", json!(intent.category.query))]);
    request = with_category(request, facet_safe, &intent.category);
    let radius = locality_radius_meters(locality.get("type"));
    let mut near = Obj::new();
    set_opt(&mut near, "lat", locality_coordinate(&locality, "lat"));
    set_opt(&mut near, "lon", locality_coordinate(&locality, "lon"));
    near.insert("radiusMeters".into(), num(radius));
    request = set(request, vec![("geo", json!({ "near": Value::Object(near), "sort": "distance" }))]);
    let response = search_nearest_with_budget_fallback(ctx, request).await?;
    let trace = merge_runtime_traces(&[locality.trace(), weak_probe.as_ref().and_then(trace_of), trace_of(&response)]);
    let locality_name = value_or(locality.get("name"), &intent.locality);
    let mut stats = stats_of(&response);
    if let Some(trace) = trace {
        stats.insert("trace".into(), trace);
    }
    stats.insert("plannerLane".into(), json!("osmCategoryLocality"));
    stats.insert("osmIntentCategory".into(), json!(intent.category.query));
    stats.insert("osmIntentCategoryType".into(), json!(intent.category.type_));
    if facet_safe {
        stats.insert("osmIntentCategoryFacet".into(), json!(true));
    }
    stats.insert("osmIntentLocality".into(), locality_name.clone());
    stats.insert("osmIntentLocalityType".into(), value_or(locality.get("type"), ""));
    stats.insert("osmIntentRadiusMeters".into(), num(radius));
    if !shard.is_empty() {
        stats.insert("osmIntentShard".into(), json!(shard));
    }
    if weak_probe.is_some() {
        stats.insert("osmWeakLocalityTextProbe".into(), json!(true));
    }
    Ok(with(
        &response,
        vec![
            ("resolvedQuery", Value::String(format!("{} {}", intent.category.label, js_string(Some(&locality_name))))),
            ("stats", Value::Object(stats)),
        ],
    ))
}

/// The `if (!intent) { ... }` branch of `searchOsmQueryBase`.
async fn search_without_intent(
    ctx: &Ctx<'_>,
    q: &str,
    params: &Value,
    anchor: Option<LatLon>,
    map_anchor: Option<LatLon>,
    nearby: Option<CategoryIntent>,
    lexicon: &super::deps::CategoryLexicon,
) -> Result<Value> {
    if let (Some(nearby), Some(anchor)) = (&nearby, anchor) {
        let explicit = explicit_shard_scope(params);
        let mut response = Value::Null;
        let mut radius_meters = 0.0;
        let mut facet_safe = false;
        let mut scope: Vec<String> = Vec::new();
        for radius in NEARBY_CATEGORY_RADII_METERS {
            radius_meters = radius;
            scope = if explicit.is_empty() { anchor_radius_coverage_shards(ctx, anchor, radius) } else { explicit.clone() };
            facet_safe = shard_scope_supports_feature(ctx, &scope, "facetSummaryUint32");
            let mut request = params.clone();
            if nullish(field(params, "shards")) && !scope.is_empty() {
                request = set(request, vec![("shards", strings(&scope))]);
            }
            request = set(request, vec![("q", json!(nearby.query))]);
            request = with_category(request, facet_safe, nearby);
            request = set(request, vec![("geo", near_geo(anchor, radius, true))]);
            response = search_nearest_with_budget_fallback(ctx, request).await?;
            if total_of(&response) > 0.0 {
                break;
            }
        }
        let mut stats = vec![
            ("plannerLane", json!("osmCategoryNearby")),
            ("osmIntentCategory", json!(nearby.query)),
            ("osmIntentRadiusMeters", num(radius_meters)),
        ];
        if !scope.is_empty() {
            stats.push(("osmIntentCoverageShards", strings(&scope)));
        }
        if facet_safe {
            stats.push(("osmIntentCategoryFacet", json!(true)));
        }
        let out = set(response, vec![("resolvedQuery", Value::String(format!("{} nearby", nearby.label)))]);
        return Ok(set_stats(out, stats));
    }
    if let Some(nearby) = &nearby {
        let explicit = explicit_shard_scope(params);
        let viewport_scope = if explicit.is_empty() { viewport_coverage_shards(ctx, field(params, "geo").and_then(|geo| field(geo, "box"))) } else { explicit };
        let facet_safe = shard_scope_supports_feature(ctx, &viewport_scope, "facetSummaryUint32");
        let has_geo = is_truthy(field(params, "geo"));
        let mut request = params.clone();
        if nullish(field(params, "shards")) && !viewport_scope.is_empty() {
            request = set(request, vec![("shards", strings(&viewport_scope))]);
        }
        request = set(request, vec![("q", json!(if facet_safe && has_geo { "" } else { nearby.query.as_str() }))]);
        request = with_category(request, facet_safe, nearby);
        let response = search_nearest_with_budget_fallback(ctx, request).await?;
        let mut out = response.clone();
        if has_geo {
            out = set(out, vec![("resolvedQuery", json!(nearby.label))]);
        }
        let mut stats = Vec::new();
        if has_geo {
            stats.push(("plannerLane", json!("osmCategoryGeo")));
            stats.push(("osmIntentCategory", json!(nearby.query)));
            if facet_safe {
                stats.push(("osmIntentCategoryFacet", json!(true)));
            }
        }
        return Ok(collapse_civic_duplicates(set_stats(out, stats)));
    }
    if let Some(found) = resolve_intersection_locality(ctx, q, params, map_anchor).await? {
        return Ok(found);
    }
    if let Some(found) = resolve_street_locality(ctx, q, params).await? {
        return Ok(found);
    }
    if map_anchor.is_none() {
        if let Some(found) = resolve_global_named_text(ctx, q, params, false).await? {
            return Ok(collapse_civic_duplicates(found));
        }
    }
    let mut local_probe_stats: Option<Value> = None;
    if let Some(anchor) = anchor.filter(|_| !q.is_empty()) {
        let local_scope = if nullish(field(params, "shards")) { anchor_radius_coverage_shards(ctx, anchor, NEAR_TEXT_RADIUS_METERS) } else { Vec::new() };
        let mut request = params.clone();
        if !local_scope.is_empty() {
            request = set(request, vec![("shards", strings(&local_scope))]);
        }
        request = set(request, vec![("geo", near_geo(anchor, NEAR_TEXT_RADIUS_METERS, false))]);
        let local = ctx.search(request).await?;
        if total_of(&local) > 0.0 && results_len(&local) > 0 {
            let bears = results_of(&local).iter().any(|result| bears_name(result, q));
            if !bears && possible_locality_query(q, lexicon) {
                if let Some(locality) = resolve_locality(ctx, q, params, LocalityOptions::final_miss()).await? {
                    let response = locality_exact_response(&locality, params, q);
                    let trace = merge_runtime_traces(&[trace_of(&local), trace_of(&response)]);
                    return Ok(with_trace(response, trace));
                }
            }
            let mut stats = vec![("plannerLane", json!("osmNearText")), ("osmIntentRadiusMeters", num(NEAR_TEXT_RADIUS_METERS))];
            if !local_scope.is_empty() {
                stats.push(("osmIntentCoverageShards", strings(&local_scope)));
            }
            return Ok(collapse_civic_duplicates(set_stats(local, stats)));
        }
        local_probe_stats = Some(field(&local, "stats").cloned().unwrap_or(Value::Null));
    }
    if possible_locality_query(q, lexicon) {
        if let Some(locality) = resolve_locality(ctx, q, params, LocalityOptions::default()).await? {
            let response = locality_exact_response(&locality, params, q);
            let trace = merge_runtime_traces(&[local_probe_stats.as_ref().and_then(|stats| field(stats, "trace")), trace_of(&response)]);
            return Ok(with_trace(response, trace));
        }
    }
    if anchor.is_some() && !q.is_empty() {
        let global = ctx.search(params.clone()).await?;
        let trace = merge_runtime_traces(&[local_probe_stats.as_ref().and_then(|stats| field(stats, "trace")), trace_of(&global)]);
        let mut entries = Vec::new();
        if let Some(trace) = trace {
            entries.push(("trace", trace));
        }
        entries.push(("osmNearFallback", json!(true)));
        return Ok(collapse_civic_duplicates(set_stats(global, entries)));
    }
    Ok(collapse_civic_duplicates(search_nearest_with_budget_fallback(ctx, params.clone()).await?))
}

// ------------------------------------------------------------ constraints

fn with_constraint_filters(params: Value, constraints: &OsmConstraints, ctx: &Ctx<'_>) -> Value {
    let facets = compile_osm_constraint_filters(constraints, &available_facet_names(&ctx.manifest));
    if facets.is_empty() {
        return params;
    }
    with_facet_filter(&params, facets.into_iter().collect())
}

fn constrained_response(response: &Value, constraints: &OsmConstraints, raw: &Value, requested_size: f64) -> Result<Value> {
    let options = ConstraintOptions::from_params(raw);
    let mut evaluated: Vec<(bool, Value)> = Vec::new();
    for result in results_of(response) {
        evaluated.push(evaluate_and_annotate(&result, constraints, &options)?);
    }
    let candidates = evaluated.len();
    let results = slice_to(evaluated.into_iter().filter(|(matches, _)| *matches).map(|(_, result)| result).collect(), requested_size);
    let approximate = match field(response, "approximate") {
        Some(value) if truthy_value(value) => value.clone(),
        _ => {
            let size = number_or(field(response, "size"), candidates as f64);
            json!(candidates as f64 >= size)
        }
    };
    let matched = results.len();
    let out = with(
        response,
        vec![("total", num(matched as f64)), ("size", num(requested_size)), ("results", Value::Array(results)), ("approximate", approximate)],
    );
    Ok(with_stats(
        &out,
        vec![
            ("osmConstraints", constraints.to_json()),
            ("osmConstraintCandidates", num(candidates as f64)),
            ("osmConstraintMatches", num(matched as f64)),
        ],
    ))
}

/// `rawParams.query ?? rawParams.q ?? ""`.
fn query_param(raw: &Value) -> String {
    let value = match field(raw, "query") {
        Some(value) if !value.is_null() => Some(value),
        _ => match field(raw, "q") {
            Some(value) if !value.is_null() => Some(value),
            _ => None,
        },
    };
    js_string_or_empty(value)
}

/// `Math.max(1, Math.min(100, Math.floor(Number(rawParams.limit ?? rawParams.size ?? fallback))))`.
fn requested_limit(raw: &Value, fallback: f64) -> f64 {
    let value = match field(raw, "limit") {
        Some(value) if !value.is_null() => Some(value),
        _ => field(raw, "size").filter(|value| !value.is_null()),
    };
    js_max(1.0, js_min(100.0, number_nullish(value, fallback).floor()))
}

/// `searchAlongRouteOsm(engine, rawParams)`.
pub async fn search_along_route(ctx: &Ctx<'_>, raw: &Value) -> Result<Value> {
    if !ctx.has("search") {
        return Err(Error::new("route search requires a Rangefind search engine"));
    }
    let route = field(raw, "route").filter(|route| truthy_value(route)).cloned();
    let Some(route) = route else {
        return Err(Error::new("route search requires an encoded polyline or GeoJSON route"));
    };
    let requested_size = requested_limit(raw, 20.0);
    let parsed = parse_osm_constraints(&query_param(raw), field(raw, "constraints"));
    let lexicon = engine_category_lexicon(ctx).await;
    let category = lookup_category(&lexicon, &parsed.query).or_else(|| lookup_category_fuzzy(&lexicon, &parsed.query));
    let dynamic = parsed.constraints.open_now() || !parsed.constraints.is_empty();
    let candidate_size = js_min(100.0, if dynamic { js_max(32.0, requested_size * 4.0) } else { requested_size });
    let corridor = match field(raw, "corridorMeters") {
        None => json!(1500),
        Some(value) => value.clone(),
    };
    let mut engine_options = spread(raw);
    for key in [
        "route", "query", "q", "corridorMeters", "limit", "constraints", "at", "now", "timeZone", "includeUnknownOpenNow",
        "requireCompleteOpeningHours", "routePositionMeters", "routeDirection", "viewport", "polylinePrecision", "size", "geo",
    ] {
        engine_options.shift_remove(key);
    }
    let engine_options = Value::Object(engine_options);
    let mut params = with(
        &engine_options,
        vec![("q", json!(if category.is_some() { "" } else { parsed.query.as_str() })), ("size", num(candidate_size))],
    );
    if let Some(category) = &category {
        params = with_type_facet(&params, &category.type_);
    }
    let mut geo = match field(raw, "geo") {
        Some(value) if truthy_value(value) => spread(value),
        _ => Obj::new(),
    };
    geo.insert("route".into(), route);
    geo.insert("corridorMeters".into(), corridor.clone());
    for key in ["polylinePrecision", "routePositionMeters", "routeDirection", "viewport"] {
        set_opt(&mut geo, key, field(raw, key).cloned());
    }
    geo.insert("sort".into(), json!("route"));
    params = set(params, vec![("geo", Value::Object(geo))]);
    params = with_constraint_filters(params, &parsed.constraints, ctx);
    let response = ctx.search(params).await?;
    let constrained = constrained_response(&response, &parsed.constraints, raw, requested_size)?;
    let resolved = match &category {
        Some(category) if !category.label.is_empty() => category.label.clone(),
        _ => parsed.query.clone(),
    };
    let direction = if field(raw, "routeDirection") == Some(&json!("reverse")) { "reverse" } else { "forward" };
    let out = set(constrained, vec![("resolvedQuery", Value::String(resolved))]);
    Ok(with_stats(
        &out,
        vec![
            ("plannerLane", json!("osmRouteCorridor")),
            ("osmIntentRouteCategory", category.map(|category| json!(category.type_)).filter(|v| truthy_value(v)).unwrap_or(Value::Null)),
            ("osmIntentRouteCorridorMeters", num(crate::util::json::to_number(&corridor))),
            ("osmIntentRouteDirection", json!(direction)),
        ],
    ))
}

/// `searchOsmQuery(engine, rawParams)`.
pub async fn search_osm_query(ctx: &Ctx<'_>, raw: &Value) -> Result<Value> {
    if is_truthy(field(raw, "route")) {
        return search_along_route(ctx, raw).await;
    }
    let parsed = parse_osm_constraints(&query_param(raw), field(raw, "constraints"));
    let active = !parsed.constraints.is_empty();
    let requested_size = requested_limit(raw, 10.0);
    let candidate_size = if active { js_min(100.0, js_max(32.0, requested_size * 4.0)) } else { requested_size };
    let params = with(raw, vec![("q", Value::String(parsed.query.clone())), ("size", num(candidate_size))]);
    let params = with_constraint_filters(params, &parsed.constraints, ctx);
    let response = search_osm_query_base(ctx, &params).await?;
    if !active {
        return Ok(response);
    }
    constrained_response(&response, &parsed.constraints, raw, requested_size)
}

// ------------------------------------------------------------ suggestions

/// `resolveOsmSuggestion(engine, suggestion)`.
pub async fn resolve_osm_suggestion(ctx: &Ctx<'_>, suggestion: &Value) -> Result<Value> {
    let selection = match field(suggestion, "selection") {
        Some(value) if truthy_value(value) => value,
        _ => suggestion,
    };
    let doc = field(selection, "doc");
    let index = number(doc.and_then(|doc| field(doc, "index"))).floor();
    if !is_truthy(doc) || !index.is_finite() || index < 0.0 {
        return Err(Error::new("resolveOsmSuggestion requires a suggestion carrying selection.doc"));
    }
    if !ctx.has("hydrateRows") {
        return Err(Error::new("resolveOsmSuggestion requires an engine exposing hydrateRows"));
    }
    let doc = doc.unwrap();
    let shard = field(doc, "shard").filter(|shard| truthy_value(shard)).cloned();
    let context = match &shard {
        Some(shard) => json!({ "shard": shard }),
        None => json!({}),
    };
    let results = ctx.engine.call("hydrateRows", json!({ "rows": [[num(index), 0]], "context": context })).await?;
    let result = results.get(0).filter(|value| truthy_value(value)).cloned();
    let query = js_string_or_empty(first_truthy_of(&[field(selection, "query"), field(suggestion, "text")]));
    let resolved = if !query.is_empty() {
        Value::String(query)
    } else {
        result.as_ref().and_then(|result| field(result, "name")).filter(|name| !name.is_null()).cloned().unwrap_or(json!(""))
    };
    let mut stats = Obj::new();
    stats.insert("plannerLane".into(), json!("osmSuggestEntity"));
    stats.insert("osmSuggestEntityIndex".into(), num(index));
    if let Some(shard) = shard {
        stats.insert("osmSuggestEntityShard".into(), shard);
    }
    Ok(json!({
        "total": if result.is_some() { 1 } else { 0 },
        "page": 1,
        "size": 1,
        "approximate": false,
        "results": result.map(|result| vec![result]).unwrap_or_default(),
        "resolvedQuery": resolved,
        "stats": Value::Object(stats)
    }))
}

fn first_truthy_of<'a>(values: &[Option<&'a Value>]) -> Option<&'a Value> {
    values.iter().copied().find(|value| is_truthy(*value)).flatten()
}

/// `suggestionHydrationContext(count)` from `runtime.js`.
fn suggestion_hydration_context(count: usize) -> Obj {
    let mut out = Obj::new();
    out.insert("hasTextTerms".into(), json!(false));
    out.insert("preferDocPages".into(), if count > 1 { json!("force") } else { json!(false) });
    out
}

/// `hydrateOsmSuggestions(engine, suggestions)`: sets `result` on each
/// suggestion whose best document hydrated.
pub async fn hydrate_osm_suggestions(ctx: &Ctx<'_>, suggestions: &mut [Value]) {
    if !ctx.has("hydrateRows") {
        return;
    }
    struct Group {
        key: String,
        context: Obj,
        slots: Vec<(Value, Option<Value>)>,
    }
    let group_key = |item: &Value| {
        let nullish_or = |key: &str| field(item, key).cloned().unwrap_or(Value::Null);
        json_key(&[nullish_or("docShard"), nullish_or("generation")])
    };
    let mut groups: Vec<Group> = Vec::new();
    for item in suggestions.iter() {
        if nullish(field(item, "doc")) {
            continue;
        }
        let key = group_key(item);
        let position = match groups.iter().position(|group| group.key == key) {
            Some(position) => position,
            None => {
                let mut context = Obj::new();
                if is_truthy(field(item, "docShard")) {
                    context.insert("shard".into(), field(item, "docShard").cloned().unwrap());
                }
                if !nullish(field(item, "generation")) {
                    context.insert("generation".into(), field(item, "generation").cloned().unwrap());
                }
                groups.push(Group { key, context, slots: Vec::new() });
                groups.len() - 1
            }
        };
        let doc = field(item, "doc").cloned().unwrap();
        if !groups[position].slots.iter().any(|(seen, _)| same_value_zero(seen, &doc)) {
            groups[position].slots.push((doc, None));
        }
    }
    if groups.is_empty() {
        return;
    }
    let outcomes = join_all(groups.iter().map(|group| {
        let mut context = suggestion_hydration_context(group.slots.len());
        for (key, value) in &group.context {
            context.insert(key.clone(), value.clone());
        }
        let rows: Vec<Value> = group.slots.iter().map(|(ordinal, _)| json!([ordinal, 0])).collect();
        ctx.engine.call("hydrateRows", json!({ "rows": rows, "context": Value::Object(context) }))
    }))
    .await;
    for (group, outcome) in groups.iter_mut().zip(outcomes) {
        let Ok(Value::Array(results)) = outcome else { continue };
        for (index, slot) in group.slots.iter_mut().enumerate() {
            if let Some(hit) = results.get(index).filter(|hit| truthy_value(hit)) {
                slot.1 = Some(hit.clone());
            }
        }
    }
    for item in suggestions.iter_mut() {
        if nullish(field(item, "doc")) {
            continue;
        }
        let key = group_key(item);
        let doc = field(item, "doc").cloned().unwrap();
        let hit = groups
            .iter()
            .find(|group| group.key == key)
            .and_then(|group| group.slots.iter().find(|(seen, _)| same_value_zero(seen, &doc)))
            .and_then(|(_, hit)| hit.clone());
        if let (Some(hit), Some(map)) = (hit, item.as_object_mut()) {
            map.insert("result".into(), hit);
        }
    }
}

/// `suggestOsmQuery(engine, rawParams)`.
pub async fn suggest_osm_query(ctx: &Ctx<'_>, raw: &Value) -> Result<Value> {
    let original_q = js_string_or_empty(field(raw, "q"));
    let original_len = len16(&original_q) as f64;
    let parsed_offset = number(field(raw, "inputOffset"));
    let input_offset = if parsed_offset.is_finite() { js_max(0.0, js_min(original_len, parsed_offset.floor())) } else { original_len };
    let input = crate::util::jsstr::prefix16(&original_q, input_offset as usize);
    let mut params = spread(raw);
    params.shift_remove("inputOffset");
    params.shift_remove("near");
    params.insert("q".into(), Value::String(input.clone()));
    let hydrate_requested = params.get("hydrate") == Some(&Value::Bool(true));
    params.shift_remove("hydrate");
    let params = Value::Object(params);
    let requested_size = js_max(1.0, js_min(50.0, number_or(field(&params, "size"), 8.0).floor()));
    let anchor = near_anchor(raw).or_else(|| viewport_anchor(field(raw, "geo").and_then(|geo| field(geo, "box"))));
    let coverage = match anchor {
        Some(anchor) if nullish(field(&params, "shards")) => anchor_coverage_shards(ctx, anchor),
        _ => Vec::new(),
    };
    let q = trim(&input).to_string();
    let lexicon = engine_category_lexicon(ctx).await;
    let intent = parse_osm_query_intent(&q, &lexicon);
    if let Some(intent) = intent.filter(|intent| !intent.locality.is_empty() && intent.category.corrected_from.is_none()) {
        let mut request = set(params, vec![("q", Value::String(intent.locality.clone()))]);
        if !coverage.is_empty() {
            request = set(request, vec![("shards", strings(&coverage))]);
        }
        request = set(request, vec![("size", num(js_max(32.0, requested_size)))]);
        let locality_response = ctx.suggest(request).await?;
        let suggestions: Vec<Value> = match field(&locality_response, "suggestions") {
            Some(Value::Array(items)) => items
                .iter()
                .map(|item| {
                    let base = without(item, &["doc", "docs", "docShard", "generation", "result", "results"]);
                    let text = js_string(field(item, "text"));
                    let label = if intent.order == "locality-category" {
                        format!("{text} {}", intent.category.label)
                    } else {
                        format!("{} {text}", intent.category.label)
                    };
                    with(
                        &base,
                        vec![
                            ("text", Value::String(label)),
                            ("type", json!("category-locality")),
                            ("category", json!(intent.category.type_)),
                            ("locality", field(item, "text").cloned().unwrap_or(Value::Null)),
                        ],
                    )
                })
                .collect(),
            _ => Vec::new(),
        };
        let suggestions = slice_to(suggestions, requested_size);
        let mut stats = vec![("plannerLane", json!("osmSuggestCategoryLocality")), ("osmIntentCategory", json!(intent.category.query))];
        if !coverage.is_empty() {
            stats.push(("osmSuggestCoverageShards", strings(&coverage)));
        }
        let out = with(
            &locality_response,
            vec![("q", Value::String(original_q.clone())), ("inputOffset", num(input_offset)), ("suggestions", Value::Array(suggestions))],
        );
        return Ok(structured_suggestions(&set_stats(out, stats), &input));
    }
    let mut request = params.clone();
    if !coverage.is_empty() {
        request = set(request, vec![("shards", strings(&coverage))]);
    }
    let size = if has_house_number(&js_string_or_empty(field(&params, "q"))) { requested_size } else { js_max(32.0, requested_size) };
    request = set(request, vec![("size", num(size))]);
    let response = ctx.suggest(request).await?;
    let collapsed = collapse_street_suggestions(&response, &set(params, vec![("size", num(requested_size))]));
    let mut shaped = set(collapsed, vec![("q", Value::String(original_q)), ("inputOffset", num(input_offset))]);
    if !coverage.is_empty() {
        shaped = set_stats(shaped, vec![("osmSuggestCoverageShards", strings(&coverage))]);
    }
    let mut structured = structured_suggestions(&shaped, &input);
    if hydrate_requested {
        if let Some(Value::Array(items)) = structured.get_mut("suggestions") {
            hydrate_osm_suggestions(ctx, items).await;
        }
    }
    Ok(structured)
}
