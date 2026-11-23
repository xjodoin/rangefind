//! Pure helpers from `query.js`: intent parsing, shard geometry, response
//! shaping, street/civic collapsing and suggestion structuring.

use std::cmp::Ordering;

use serde_json::{json, Value};

use super::deps::{fold, fold_value, lookup_category, lookup_category_fuzzy, within_one_edit, CategoryIntent, CategoryLexicon};
use super::host::{shard_id, Ctx, ExactAuthority, Locality};
use super::js::*;
use crate::util::jsstr::cmp16;

pub const LOCALITY_CONNECTORS: &[&str] = &["a", "around", "dans", "de", "du", "in", "near", "pres"];
pub const POSTCODE_AUTHORITY_WEIGHT: f64 = 5_000_000.0;
pub const LOCALITY_TYPES: &[&str] = &["city", "town", "municipality", "village", "hamlet"];
pub const STREET_DESIGNATORS: &[&str] = &[
    "allee", "avenue", "boulevard", "chemin", "cote", "cour", "impasse", "montee", "parkway", "place", "rang", "route", "rue", "terrasse",
    "court", "drive", "highway", "lane", "road", "street",
];
pub const DEFAULT_REVERSE_GEOCODE_RADIUS_METERS: f64 = 5000.0;
pub const DEFAULT_REVERSE_LOCALITY_RADIUS_METERS: f64 = 30000.0;
pub const MAX_REVERSE_GEOCODE_RESULTS: f64 = 25.0;
pub const NEARBY_SUFFIXES: &[&str] =
    &["near me", "nearby", "around me", "close by", "close to me", "pres de moi", "autour de moi", "a proximite", "proche", "proches"];
pub const NEARBY_CATEGORY_RADII_METERS: [f64; 2] = [10000.0, 50000.0];
pub const NEAR_TEXT_RADIUS_METERS: f64 = 50000.0;
pub const WEAK_LOCALITY_ANCHOR_METERS: f64 = 100000.0;

pub fn near_text_boost() -> Value {
    json!({ "weight": 2, "pivotMeters": 2000 })
}

pub fn is_locality_type(value: Option<&Value>) -> bool {
    matches!(value, Some(Value::String(text)) if LOCALITY_TYPES.contains(&text.as_str()))
}

pub fn is_street_designator(token: &str) -> bool {
    STREET_DESIGNATORS.contains(&token)
}

/// `{ ...obj }` with the given entries assigned in order.
pub fn with(base: &Value, entries: Vec<(&str, Value)>) -> Value {
    let mut out = spread(base);
    for (key, value) in entries {
        out.insert(key.to_string(), value);
    }
    Value::Object(out)
}

/// `{ ...obj, ...entries }` reusing `base` (no copy of the untouched keys).
pub fn set(base: Value, entries: Vec<(&str, Value)>) -> Value {
    let mut out = match base {
        Value::Object(map) => map,
        _ => Obj::new(),
    };
    for (key, value) in entries {
        out.insert(key.to_string(), value);
    }
    Value::Object(out)
}

/// `{ ...response, stats: { ...(response.stats || {}), ...entries } }`
/// reusing `response`.
pub fn set_stats(response: Value, entries: Vec<(&str, Value)>) -> Value {
    let mut out = match response {
        Value::Object(map) => map,
        _ => Obj::new(),
    };
    let mut stats = match out.get_mut("stats") {
        Some(value) if truthy_value(value) => match std::mem::take(value) {
            Value::Object(map) => map,
            other => spread(&other),
        },
        _ => Obj::new(),
    };
    for (key, value) in entries {
        stats.insert(key.to_string(), value);
    }
    out.insert("stats".into(), Value::Object(stats));
    Value::Object(out)
}

/// `{ ...obj, key: undefined }` (the key disappears from JSON).
pub fn without(base: &Value, keys: &[&str]) -> Value {
    let mut out = spread(base);
    for key in keys {
        out.shift_remove(*key);
    }
    Value::Object(out)
}

/// `{ ...(response.stats || {}) }`.
pub fn stats_of(response: &Value) -> Obj {
    match field(response, "stats") {
        Some(value) if truthy_value(value) => spread(value),
        _ => Obj::new(),
    }
}

pub fn truthy_value(value: &Value) -> bool {
    crate::util::json::truthy(value)
}

/// `{ ...response, stats: { ...(response.stats || {}), ...entries } }`.
pub fn with_stats(response: &Value, entries: Vec<(&str, Value)>) -> Value {
    let mut stats = stats_of(response);
    for (key, value) in entries {
        stats.insert(key.to_string(), value);
    }
    with(response, vec![("stats", Value::Object(stats))])
}

/// `response.results || []`.
pub fn results_of(response: &Value) -> Vec<Value> {
    match field(response, "results") {
        Some(Value::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

/// `response.results.length` (0 when missing).
pub fn results_len(response: &Value) -> usize {
    field(response, "results").and_then(Value::as_array).map(Vec::len).unwrap_or(0)
}

pub fn strings(values: &[String]) -> Value {
    Value::Array(values.iter().map(|value| Value::String(value.clone())).collect())
}

pub fn sorted16(mut values: Vec<String>) -> Vec<String> {
    values.sort_by(|a, b| cmp16(a, b));
    values
}

/// `result.name || result.title`.
pub fn name_or_title(result: &Value) -> Option<&Value> {
    let name = field(result, "name");
    if is_truthy(name) {
        name
    } else {
        field(result, "title")
    }
}

// ------------------------------------------------------------ coordinates

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LatLon {
    pub lat: f64,
    pub lon: f64,
}

impl LatLon {
    pub fn to_json(self) -> Value {
        json!({ "lat": num(self.lat), "lon": num(self.lon) })
    }

    pub fn near(self, radius: Option<f64>) -> Value {
        let mut out = Obj::new();
        out.insert("lat".into(), num(self.lat));
        out.insert("lon".into(), num(self.lon));
        if let Some(radius) = radius {
            out.insert("radiusMeters".into(), num(radius));
        }
        Value::Object(out)
    }

    pub fn label(self) -> String {
        format!("{}, {}", to_fixed(self.lat, 6), to_fixed(self.lon, 6))
    }
}

/// `parseCoordinateIntent(value)`.
pub fn parse_coordinate_intent(value: &str) -> Option<LatLon> {
    let (lat, lon) = match_decimal_coordinates(value)?;
    let lat = parse_number_text(&lat);
    let lon = parse_number_text(&lon);
    if !lat.is_finite() || !lon.is_finite() || !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
        return None;
    }
    Some(LatLon { lat, lon })
}

pub fn coordinate_response(coordinate: LatLon, params: &Value, stats: &Obj, lane: &str) -> Value {
    let label = coordinate.label();
    let mut out_stats = stats.clone();
    out_stats.insert("plannerLane".into(), json!(lane));
    out_stats.insert("shardsQueried".into(), num(number_or(stats.get("shardsQueried"), 0.0)));
    json!({
        "total": 1,
        "page": 1,
        "size": num(number_or(field(params, "size"), 10.0)),
        "approximate": false,
        "results": [{
            "id": format!("coordinate:{},{}", number_to_string(coordinate.lat), number_to_string(coordinate.lon)),
            "name": label,
            "title": label,
            "address": label,
            "category": "place",
            "type": "coordinate",
            "lat": num(coordinate.lat),
            "lon": num(coordinate.lon),
            "distanceMeters": 0
        }],
        "resolvedQuery": label,
        "stats": Value::Object(out_stats)
    })
}

pub fn haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let rad = std::f64::consts::PI / 180.0;
    let d_lat = (lat2 - lat1) * rad;
    let d_lon = (lon2 - lon1) * rad;
    let a = (d_lat / 2.0).sin().powi(2) + (lat1 * rad).cos() * (lat2 * rad).cos() * (d_lon / 2.0).sin().powi(2);
    6371000.0 * 2.0 * a.sqrt().atan2((1.0 - a).sqrt())
}

/// `nearAnchor(params)`.
pub fn near_anchor(params: &Value) -> Option<LatLon> {
    let near = field(params, "near");
    let lat = number(near.and_then(|near| field(near, "lat")));
    let lon = number(near.and_then(|near| field(near, "lon")));
    (lat.is_finite() && lon.is_finite()).then_some(LatLon { lat, lon })
}

fn box_numbers(viewport: &Value) -> [f64; 4] {
    [
        number(field(viewport, "minLat")),
        number(field(viewport, "maxLat")),
        number(field(viewport, "minLon")),
        number(field(viewport, "maxLon")),
    ]
}

/// `viewportAnchor(box)`.
pub fn viewport_anchor(viewport: Option<&Value>) -> Option<LatLon> {
    let viewport = viewport.filter(|v| truthy_value(v))?;
    let [min_lat, max_lat, min_lon, max_lon] = box_numbers(viewport);
    if ![min_lat, max_lat, min_lon, max_lon].iter().all(|v| v.is_finite()) || min_lat > max_lat {
        return None;
    }
    let mut lon = if min_lon <= max_lon { (min_lon + max_lon) / 2.0 } else { (min_lon + max_lon + 360.0) / 2.0 };
    if lon > 180.0 {
        lon -= 360.0;
    }
    Some(LatLon { lat: (min_lat + max_lat) / 2.0, lon })
}

/// `shard.bbox.map(Number)` when it is a 4-element array of finite numbers.
pub fn bbox_numbers(bbox: Option<&Value>) -> Option<[f64; 4]> {
    let items = bbox?.as_array()?;
    if items.len() != 4 {
        return None;
    }
    let values = [to_n(&items[0]), to_n(&items[1]), to_n(&items[2]), to_n(&items[3])];
    values.iter().all(|v| v.is_finite()).then_some(values)
}

fn to_n(value: &Value) -> f64 {
    crate::util::json::to_number(value)
}

/// `bboxContainsPoint(bbox, lat, lon)`.
pub fn bbox_contains_point(bbox: Option<&Value>, lat: f64, lon: f64) -> bool {
    let Some(bbox) = bbox.and_then(Value::as_array) else { return false };
    if bbox.len() != 4 {
        return false;
    }
    let values = [to_n(&bbox[0]), to_n(&bbox[1]), to_n(&bbox[2]), to_n(&bbox[3])];
    bbox_numbers_contain(values, lat, lon)
}

pub fn bbox_numbers_contain(values: [f64; 4], lat: f64, lon: f64) -> bool {
    let [min_lat, min_lon, max_lat, max_lon] = values;
    if !values.iter().all(|v| v.is_finite()) {
        return false;
    }
    if lat < min_lat || lat > max_lat {
        return false;
    }
    if min_lon <= max_lon {
        lon >= min_lon && lon <= max_lon
    } else {
        lon >= min_lon || lon <= max_lon
    }
}

/// `anchorCoverageShards(engine, anchor)`.
pub fn anchor_coverage_shards(ctx: &Ctx<'_>, anchor: LatLon) -> Vec<String> {
    sorted16(
        ctx.shard_descriptors()
            .into_iter()
            .filter(|shard| bbox_contains_point(shard.get("bbox"), anchor.lat, anchor.lon))
            .map(shard_id)
            .collect(),
    )
}

/// `explicitShardScope(params)`.
pub fn explicit_shard_scope(params: &Value) -> Vec<String> {
    match field(params, "shards") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(items)) => items.iter().map(|item| js_string(Some(item))).collect(),
        Some(other) => vec![js_string(Some(other))],
    }
}

/// `shardScopeSupportsFeature(engine, shards, feature)`.
pub fn shard_scope_supports_feature(ctx: &Ctx<'_>, shards: &[String], feature: &str) -> bool {
    if ctx.manifest_features().get(feature) == Some(&Value::Bool(true)) {
        return true;
    }
    if shards.is_empty() {
        return false;
    }
    let descriptors = ctx.shard_descriptors();
    let mut selected: Vec<usize> = Vec::new();
    let mut resolved = 0;
    for scope in shards {
        if let Some(position) = descriptors.iter().position(|shard| &shard_id(shard) == scope) {
            if !selected.contains(&position) {
                selected.push(position);
            }
            resolved += 1;
            continue;
        }
        let mut matched = false;
        for (position, shard) in descriptors.iter().enumerate() {
            let groups = shard.get("groups").and_then(Value::as_array);
            if groups.map(|groups| groups.iter().any(|group| &js_string(Some(group)) == scope)).unwrap_or(false) {
                if !selected.contains(&position) {
                    selected.push(position);
                }
                matched = true;
            }
        }
        if matched {
            resolved += 1;
        }
    }
    resolved == shards.len()
        && !selected.is_empty()
        && selected.iter().all(|&position| at(at(descriptors[position], "features"), feature) == &Value::Bool(true))
}

/// `viewportCoverageShards(engine, box)`.
pub fn viewport_coverage_shards(ctx: &Ctx<'_>, viewport: Option<&Value>) -> Vec<String> {
    let Some(viewport) = viewport.filter(|v| truthy_value(v)) else { return Vec::new() };
    let [min_lat, max_lat, min_lon, max_lon] = box_numbers(viewport);
    if ![min_lat, max_lat, min_lon, max_lon].iter().all(|v| v.is_finite()) || min_lat > max_lat {
        return Vec::new();
    }
    let intervals = |left: f64, right: f64| if left <= right { vec![(left, right)] } else { vec![(left, 180.0), (-180.0, right)] };
    let requested = intervals(min_lon, max_lon);
    sorted16(
        ctx.shard_descriptors()
            .into_iter()
            .filter(|shard| {
                let Some([s_min_lat, s_min_lon, s_max_lat, s_max_lon]) = bbox_numbers(shard.get("bbox")) else { return false };
                if s_max_lat < min_lat || s_min_lat > max_lat {
                    return false;
                }
                requested.iter().any(|(left, right)| intervals(s_min_lon, s_max_lon).iter().any(|(sl, sr)| sr >= left && sl <= right))
            })
            .map(shard_id)
            .collect(),
    )
}

/// `anchorRadiusCoverageShards(engine, anchor, radiusMeters)`.
pub fn anchor_radius_coverage_shards(ctx: &Ctx<'_>, anchor: LatLon, radius_meters: f64) -> Vec<String> {
    let angular = |left: f64, right: f64| {
        let distance = (left - right).abs() % 360.0;
        js_min(distance, 360.0 - distance)
    };
    sorted16(
        ctx.shard_descriptors()
            .into_iter()
            .filter(|shard| {
                let Some(bbox) = bbox_numbers(shard.get("bbox")) else { return false };
                let [min_lat, min_lon, max_lat, max_lon] = bbox;
                let lat = js_max(min_lat, js_min(max_lat, anchor.lat));
                let mut lon = anchor.lon;
                if !bbox_contains_point(shard.get("bbox"), lat, anchor.lon) {
                    lon = if angular(anchor.lon, min_lon) <= angular(anchor.lon, max_lon) { min_lon } else { max_lon };
                }
                haversine_meters(anchor.lat, anchor.lon, lat, lon) <= radius_meters
            })
            .map(shard_id)
            .collect(),
    )
}

// ------------------------------------------------------------ intents

/// `parseOsmQueryIntent(value, lexicon)`.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryIntent {
    pub category: CategoryIntent,
    pub locality: String,
    /// `"category-locality"` or `"locality-category"`.
    pub order: &'static str,
    /// `connector` (only set for category-locality).
    pub connector: Option<bool>,
}

impl QueryIntent {
    pub fn has_connector(&self) -> bool {
        self.connector == Some(true)
    }

    pub fn to_json(&self) -> Value {
        let mut out = Obj::new();
        out.insert("category".into(), self.category.to_json());
        out.insert("locality".into(), Value::String(self.locality.clone()));
        out.insert("order".into(), json!(self.order));
        if let Some(connector) = self.connector {
            out.insert("connector".into(), Value::Bool(connector));
        }
        Value::Object(out)
    }
}

pub fn parse_osm_query_intent(value: &str, lexicon: &CategoryLexicon) -> Option<QueryIntent> {
    let surface = trim(value);
    let tokens = split_ws(surface);
    if tokens.len() < 2 {
        return None;
    }
    let first = lookup_category(lexicon, tokens[0]).or_else(|| lookup_category_fuzzy(lexicon, tokens[0]));
    if let Some(category) = first {
        let mut locality_tokens: Vec<&str> = tokens[1..].to_vec();
        let mut connector = false;
        while let Some(first) = locality_tokens.first() {
            if !LOCALITY_CONNECTORS.contains(&fold(first).as_str()) {
                break;
            }
            locality_tokens.remove(0);
            connector = true;
        }
        if locality_tokens.is_empty() {
            return None;
        }
        return Some(QueryIntent { category, locality: locality_tokens.join(" "), order: "category-locality", connector: Some(connector) });
    }
    let last_token = tokens[tokens.len() - 1];
    let last = lookup_category(lexicon, last_token).or_else(|| lookup_category_fuzzy(lexicon, last_token))?;
    Some(QueryIntent { category: last, locality: tokens[..tokens.len() - 1].join(" "), order: "locality-category", connector: None })
}

/// `parseNearbyCategoryIntent(value, lexicon)`.
pub fn parse_nearby_category_intent(value: &str, lexicon: &CategoryLexicon) -> Option<CategoryIntent> {
    let mut normalized = fold(value);
    if normalized.is_empty() {
        return None;
    }
    for suffix in NEARBY_SUFFIXES {
        if normalized == *suffix {
            return None;
        }
        if normalized.ends_with(&format!(" {suffix}")) {
            normalized = trim(&normalized[..normalized.len() - suffix.len() - 1]).to_string();
            break;
        }
    }
    lookup_category(lexicon, &normalized)
}

pub fn within_one_folded_edit(left: Option<&Value>, right: &str) -> bool {
    let left = fold_value(left);
    let right = fold(right);
    !left.is_empty() && !right.is_empty() && within_one_edit(&left, &right)
}

/// `exactAuthorityFromLookup(lookup, surface)`.
pub fn exact_authority_from_lookup(lookup: &Value, surface: &str) -> Option<ExactAuthority> {
    let key = fold(surface);
    let matches: Vec<&Value> = match field(lookup, "matches") {
        Some(Value::Array(items)) => items.iter().filter(|item| fold_value(field(item, "text")) == key).collect(),
        _ => Vec::new(),
    };
    if matches.is_empty() {
        return None;
    }
    let weight = |item: &Value| number_or(field(item, "weight"), 0.0);
    let best_weight = matches.iter().map(|item| weight(item)).fold(f64::NEG_INFINITY, js_max);
    let best: Vec<&Value> = matches.into_iter().filter(|item| weight(item) == best_weight).collect();
    let first = best.first()?;
    let mut unique: Vec<&Value> = Vec::new();
    for item in &best {
        if let Some(Value::Array(shards)) = field(item, "shards") {
            for shard in shards {
                if !unique.iter().any(|seen| same_value_zero(seen, shard)) {
                    unique.push(shard);
                }
            }
        }
    }
    let mut shards: Vec<String> = unique.into_iter().map(|shard| js_string(Some(shard))).collect();
    shards.sort_by(|a, b| cmp16(a, b));
    shards.truncate(8);
    Some(ExactAuthority {
        shards,
        text: field(first, "text").cloned().unwrap_or(Value::Null),
        count: best.iter().map(|item| number_or(field(item, "count"), 0.0)).fold(f64::NEG_INFINITY, js_max),
    })
}

/// `SameValueZero` for JSON values used as `Set`/`Map` keys (objects never
/// compare equal, as they are distinct identities in JS).
pub fn same_value_zero(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
        (Value::Object(_), _) | (_, Value::Object(_)) | (Value::Array(_), _) | (_, Value::Array(_)) => false,
        _ => left == right,
    }
}

pub fn result_matches_locality(result: &Value, surface: &str) -> bool {
    let expected = fold(surface);
    if expected.is_empty() {
        return false;
    }
    ["city", "district", "suburb", "county", "state"].iter().any(|key| fold_value(field(result, key)) == expected)
}

pub fn compact_fold(value: &str) -> String {
    fold(value).replace(' ', "")
}

pub fn compact_fold_value(value: Option<&Value>) -> String {
    compact_fold(&js_string_or_empty(value))
}

/// `resultNameMatchesSurface(result, surface, allowTypo)`.
pub fn result_name_matches_surface(result: &Value, surface: &str, allow_typo: bool) -> bool {
    let folded = fold(surface);
    let expected: Vec<&str> = split_non_alnum(&folded).into_iter().filter(|token| len16(token) >= 3).collect();
    if expected.is_empty() {
        return false;
    }
    let name = fold_value(name_or_title(result));
    let actual = alnum_tokens(&name);
    expected
        .iter()
        .all(|token| actual.iter().any(|word| word.contains(token) || token.contains(word) || (allow_typo && within_one_edit(word, token))))
}

pub fn is_geo_text_sort_budget_error(error: &crate::error::Error) -> bool {
    error.code() == Some("RANGEFIND_GEO_TEXT_SORT_BUDGET") || error.message().contains("geoTextSortMaxDf")
}

pub fn locality_radius_meters(type_: Option<&Value>) -> f64 {
    match type_.and_then(Value::as_str) {
        Some("postal_code") => 5000.0,
        Some("city") => 30000.0,
        Some("town") | Some("municipality") => 10000.0,
        Some("village") => 7000.0,
        _ => 5000.0,
    }
}

pub fn type_priority(type_: Option<&Value>) -> f64 {
    match type_.and_then(Value::as_str) {
        Some("city") => 5.0,
        Some("town") => 4.0,
        Some("municipality") => 3.0,
        Some("village") => 2.0,
        Some("hamlet") => 1.0,
        _ => 0.0,
    }
}

/// `possibleLocalityQuery(value, lexicon)`.
pub fn possible_locality_query(value: &str, lexicon: &CategoryLexicon) -> bool {
    let normalized = fold(trim(value));
    let tokens: Vec<&str> = normalized.split(' ').filter(|t| !t.is_empty()).collect();
    !tokens.is_empty() && tokens.len() <= 4 && !tokens.iter().any(|t| has_ascii_digit(t)) && lookup_category(lexicon, &normalized).is_none()
}

/// `hasHouseNumber(value)`.
pub fn has_house_number(value: &str) -> bool {
    split_house_number_tokens(trim(value)).into_iter().any(is_house_number_token)
}

pub struct StreetSurface {
    pub text: String,
    pub key: String,
}

/// `streetLocalitySurface(value)`.
pub fn street_locality_surface(value: &str) -> Option<StreetSurface> {
    let without_number = strip_leading_house_number(value);
    let comma = without_number.find(',')?;
    if comma == 0 || comma == without_number.len() - 1 {
        return None;
    }
    let street = trim(&without_number[..comma]);
    let locality = trim(&without_number[comma + 1..]);
    let folded = fold(street);
    if !folded.split(' ').filter(|t| !t.is_empty()).any(is_street_designator) {
        return None;
    }
    let text = format!("{street}, {locality}");
    let key = fold(&text);
    Some(StreetSurface { text, key })
}

// ------------------------------------------------------------ responses

/// `mergeRuntimeTraces(...values)`.
pub fn merge_runtime_traces(values: &[Option<&Value>]) -> Option<Value> {
    let traces: Vec<&Value> = values
        .iter()
        .flatten()
        .copied()
        .filter(|trace| field(trace, "spans").and_then(Value::as_array).map(|spans| !spans.is_empty()).unwrap_or(false))
        .collect();
    match traces.len() {
        0 => return None,
        1 => return Some(traces[0].clone()),
        _ => {}
    }
    struct Span {
        name: Value,
        count: f64,
        total_ms: f64,
        max_ms: f64,
        bytes: f64,
    }
    let mut spans: Vec<Span> = Vec::new();
    for trace in &traces {
        for span in field(trace, "spans").and_then(Value::as_array).into_iter().flatten() {
            let name = field(span, "name").cloned().unwrap_or(Value::Null);
            let position = match spans.iter().position(|current| same_value_zero(&current.name, &name)) {
                Some(position) => position,
                None => {
                    spans.push(Span { name, count: 0.0, total_ms: 0.0, max_ms: 0.0, bytes: 0.0 });
                    spans.len() - 1
                }
            };
            let current = &mut spans[position];
            current.count += number_or(field(span, "count"), 0.0);
            current.total_ms += number_or(field(span, "totalMs"), 0.0);
            current.max_ms = js_max(current.max_ms, number_or(field(span, "maxMs"), 0.0));
            current.bytes += number_or(field(span, "bytes"), 0.0);
        }
    }
    spans.sort_by(|left, right| {
        let delta = right.total_ms - left.total_ms;
        if delta != 0.0 && !delta.is_nan() {
            return if delta < 0.0 { Ordering::Less } else { Ordering::Greater };
        }
        cmp16(&js_string(Some(&left.name)), &js_string(Some(&right.name)))
    });
    let total_ms = traces.iter().map(|trace| number_or(field(trace, "totalMs"), 0.0)).sum::<f64>();
    let total_bytes = spans.iter().map(|span| if span.bytes > 0.0 { span.bytes } else { 0.0 }).sum::<f64>();
    let spans: Vec<Value> = spans
        .into_iter()
        .map(|span| {
            let mut out = Obj::new();
            out.insert("name".into(), span.name);
            out.insert("count".into(), num(span.count));
            out.insert("totalMs".into(), num(span.total_ms));
            out.insert("maxMs".into(), num(span.max_ms));
            if span.bytes > 0.0 {
                out.insert("bytes".into(), num(span.bytes));
            }
            Value::Object(out)
        })
        .collect();
    Some(json!({ "totalMs": num(total_ms), "totalBytes": num(total_bytes), "spans": spans }))
}

pub fn trace_of(response: &Value) -> Option<&Value> {
    field(response, "stats").and_then(|stats| field(stats, "trace"))
}

/// `collapseCivicDuplicates(response)`.
pub fn collapse_civic_duplicates(response: Value) -> Value {
    let civic_type = |result: &Value| field(result, "type") == Some(&Value::String("civic_address".into()));
    match field(&response, "results").and_then(Value::as_array) {
        Some(items) if items.iter().any(civic_type) && items.iter().any(|item| !civic_type(item)) => {}
        _ => return response,
    }
    let results = results_of(&response);
    let is_civic = |result: &Value| field(result, "type") == Some(&Value::String("civic_address".into()));
    let named: Vec<&Value> = results.iter().filter(|result| !is_civic(result)).collect();
    if named.is_empty() || !results.iter().any(is_civic) {
        return response;
    }
    let lat_lon = |result: &Value| (finite_number(field(result, "lat")), finite_number(field(result, "lon")));
    let duplicate = |civic: &Value| {
        named.iter().any(|result| {
            if !is_truthy(field(civic, "house_number")) || !is_truthy(field(civic, "street")) || !is_truthy(field(result, "house_number")) || !is_truthy(field(result, "street")) {
                return false;
            }
            let civic_key = fold(&format!("{} {}", js_string(field(civic, "house_number")), js_string(field(civic, "street"))));
            let result_key = fold(&format!("{} {}", js_string(field(result, "house_number")), js_string(field(result, "street"))));
            if civic_key != result_key {
                return false;
            }
            let same_postcode = is_truthy(field(civic, "postcode"))
                && is_truthy(field(result, "postcode"))
                && fold_value(field(civic, "postcode")) == fold_value(field(result, "postcode"));
            let nearby = match (lat_lon(civic), lat_lon(result)) {
                ((Some(clat), Some(clon)), (Some(rlat), Some(rlon))) => (clat - rlat).abs() <= 0.001 && (clon - rlon).abs() <= 0.0015,
                _ => false,
            };
            same_postcode || nearby
        })
    };
    let collapsed: Vec<Value> = results.iter().filter(|result| !is_civic(result) || !duplicate(result)).cloned().collect();
    let removed = results.len() - collapsed.len();
    if removed == 0 {
        return response;
    }
    let total = js_max(collapsed.len() as f64, number_or(field(&response, "total"), results.len() as f64) - removed as f64);
    let out = set(response, vec![("total", num(total)), ("results", Value::Array(collapsed))]);
    set_stats(out, vec![("osmCivicDuplicatesCollapsed", num(removed as f64))])
}

/// `exactGeoResponse(response, authority, scope, radiusMeters, lane)`.
pub fn exact_geo_response(response: &Value, authority: Option<&ExactAuthority>, scope: &[String], radius_meters: f64, lane: &str) -> Value {
    let mut entries = vec![("plannerLane", json!(lane))];
    if radius_meters != 0.0 && !radius_meters.is_nan() {
        entries.push(("osmIntentRadiusMeters", num(radius_meters)));
    }
    if !scope.is_empty() {
        entries.push(("osmIntentCoverageShards", strings(scope)));
    }
    if let Some(authority) = authority.filter(|a| !a.shards.is_empty()) {
        entries.push(("osmIntentAuthorityShards", strings(&authority.shards)));
    }
    collapse_civic_duplicates(set_stats(response.clone(), entries))
}

/// `localityExactResponse(locality, params, surface)`.
pub fn locality_exact_response(locality: &Locality, params: &Value, surface: &str) -> Value {
    let mut stats = locality.search_stats.as_ref().map(spread).unwrap_or_default();
    let name = locality.get("name");
    let name_or_surface = if is_truthy(name) { name.cloned().unwrap() } else { Value::String(surface.to_string()) };
    stats.insert("plannerLane".into(), json!("osmLocalityExact"));
    stats.insert("osmIntentLocality".into(), name_or_surface.clone());
    let type_ = locality.get("type");
    stats.insert("osmIntentLocalityType".into(), if is_truthy(type_) { type_.cloned().unwrap() } else { json!("") });
    json!({
        "total": 1,
        "page": 1,
        "size": num(number_or(field(params, "size"), 10.0)),
        "approximate": false,
        "results": [Value::Object(locality.doc.clone())],
        "resolvedQuery": name_or_surface,
        "stats": Value::Object(stats)
    })
}

// ------------------------------------------------------------ streets

pub struct ParsedStreet {
    pub text: String,
    pub key: String,
    pub core: String,
}

/// `parsedStreetSurface(value)`.
pub fn parsed_street_surface(value: &str) -> Option<ParsedStreet> {
    let tokens = split_ws(trim(value));
    if tokens.len() < 2 || !tokens.iter().any(|token| is_street_designator(&fold(token))) {
        return None;
    }
    let core: Vec<&str> = tokens.iter().copied().filter(|token| !is_street_designator(&fold(token))).collect();
    if core.is_empty() {
        return None;
    }
    let text = tokens.join(" ");
    Some(ParsedStreet { key: fold(&text), text, core: core.join(" ") })
}

/// `parseIntersectionSurface(value)`: `(left, right)`.
pub fn parse_intersection_surface(value: &str) -> Option<(ParsedStreet, String)> {
    let surface = collapse_commas(trim(value));
    let chars: Vec<char> = surface.chars().collect();
    let (start, end) = match_intersection_connector(&chars)?;
    let left = parsed_street_surface(&chars[..start].iter().collect::<String>());
    let right_text: String = chars[end..].iter().collect();
    let right = trim(&right_text).to_string();
    match left {
        Some(left) if !right.is_empty() => Some((left, right)),
        _ => None,
    }
}

// ------------------------------------------------------------ suggestions

/// `collapseStreetSuggestions(response, params)`.
pub fn collapse_street_suggestions(response: &Value, params: &Value) -> Value {
    let size = js_max(1.0, js_min(50.0, number_or(field(params, "size"), 8.0).floor()));
    let suggestions: Vec<Value> = match field(response, "suggestions") {
        Some(Value::Array(items)) => items.clone(),
        _ => Vec::new(),
    };
    if has_house_number(&js_string_or_empty(field(params, "q"))) {
        return with(response, vec![("suggestions", Value::Array(slice_to(suggestions, size)))]);
    }
    struct Group {
        key: String,
        text: String,
        weight: f64,
        count: f64,
        matches: usize,
        shards: Vec<Value>,
    }
    let mut grouped: Vec<Group> = Vec::new();
    let mut passthrough: Vec<Value> = Vec::new();
    for item in &suggestions {
        let Some(surface) = street_locality_surface(&js_string_or_empty(field(item, "text"))) else {
            passthrough.push(item.clone());
            continue;
        };
        let position = match grouped.iter().position(|group| group.key == surface.key) {
            Some(position) => position,
            None => {
                grouped.push(Group { key: surface.key.clone(), text: surface.text, weight: 0.0, count: 0.0, matches: 0, shards: Vec::new() });
                grouped.len() - 1
            }
        };
        let group = &mut grouped[position];
        group.weight += number_or(field(item, "weight"), 0.0);
        group.count += number_or(field(item, "count"), 1.0);
        group.matches += 1;
        if let Some(Value::Array(shards)) = field(item, "shards") {
            for shard in shards {
                if !group.shards.iter().any(|seen| same_value_zero(seen, shard)) {
                    group.shards.push(shard.clone());
                }
            }
        }
    }
    let streets: Vec<Value> = grouped
        .into_iter()
        .filter(|group| group.matches >= 2)
        .map(|group| {
            let mut out = Obj::new();
            out.insert("text".into(), Value::String(group.text));
            out.insert("weight".into(), num(group.weight));
            out.insert("count".into(), num(group.count));
            out.insert("type".into(), json!("street"));
            if !group.shards.is_empty() {
                let mut shards = group.shards;
                shards.sort_by(|a, b| cmp16(&js_string(Some(a)), &js_string(Some(b))));
                out.insert("shards".into(), Value::Array(shards));
            }
            Value::Object(out)
        })
        .collect();
    let collapsed_keys: Vec<String> = streets.iter().map(|item| fold_value(field(item, "text"))).collect();
    let mut remaining = passthrough;
    for item in &suggestions {
        if let Some(surface) = street_locality_surface(&js_string_or_empty(field(item, "text"))) {
            if !collapsed_keys.contains(&surface.key) {
                remaining.push(item.clone());
            }
        }
    }
    let street_count = streets.len();
    let mut merged: Vec<Value> = streets.into_iter().chain(remaining).collect();
    merged.sort_by(|left, right| {
        let street = |item: &Value| (field(item, "type") == Some(&json!("street"))) as i32 as f64;
        let delta = street(right) - street(left);
        if delta != 0.0 {
            return if delta < 0.0 { Ordering::Less } else { Ordering::Greater };
        }
        let delta = number_or(field(right, "weight"), 0.0) - number_or(field(left, "weight"), 0.0);
        if delta != 0.0 && !delta.is_nan() {
            return if delta < 0.0 { Ordering::Less } else { Ordering::Greater };
        }
        cmp16(&js_string(field(left, "text")), &js_string(field(right, "text")))
    });
    let out = with(response, vec![("suggestions", Value::Array(slice_to(merged, size)))]);
    with_stats(&out, vec![("osmStreetSuggestionsCollapsed", num(street_count as f64))])
}

/// `array.slice(0, n)` with JS number semantics.
pub fn slice_to(mut items: Vec<Value>, n: f64) -> Vec<Value> {
    let end = if n.is_nan() || n <= 0.0 { 0 } else { n.floor() as usize };
    items.truncate(end);
    items
}

/// A display word of a suggestion: the folded text plus, for every UTF-16
/// unit of it, the `[start, end)` UTF-16 span of the display character it
/// came from (`suggestionWords`).
struct SuggestionWord {
    key: Vec<u16>,
    starts: Vec<usize>,
    ends: Vec<usize>,
}

/// `suggestionWords(value)`: maximal runs of letters and numbers (combining
/// marks stay with the character they follow), folded per character.
fn suggestion_words(value: &str) -> Vec<SuggestionWord> {
    use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};
    let mut words: Vec<SuggestionWord> = Vec::new();
    let mut open = false;
    let mut last_start = 0usize;
    let mut offset = 0usize;
    for ch in value.chars() {
        let length = ch.len_utf16();
        if is_letter_or_number(ch) {
            if !open {
                words.push(SuggestionWord { key: Vec::new(), starts: Vec::new(), ends: Vec::new() });
                open = true;
            }
            let word = words.last_mut().unwrap();
            last_start = word.starts.len();
            for unit in fold(&ch.to_string()).encode_utf16() {
                word.key.push(unit);
                word.starts.push(offset);
                word.ends.push(offset + length);
            }
        } else if open && ch.general_category_group() == GeneralCategoryGroup::Mark {
            let word = words.last_mut().unwrap();
            for end in &mut word.ends[last_start..] {
                *end = offset + length;
            }
        } else {
            open = false;
        }
        offset += length;
    }
    words
}

/// Final sigma folds differently per character than inside a word.
fn sigma_insensitive(units: &[u16]) -> Vec<u16> {
    units.iter().map(|&unit| if unit == 0x03c2 { 0x03c3 } else { unit }).collect()
}

/// `suggestionMatchRanges(text, input)`: each folded input token, in order,
/// claims the first unclaimed display word whose folded form starts with
/// it; the range covers the display characters that fold to the token.
/// Ranges are `[start, end)` UTF-16 offsets into the display text, sorted.
pub fn suggestion_match_ranges(text: &str, input: &str) -> Vec<Value> {
    let words = suggestion_words(text);
    let keys: Vec<Vec<u16>> = words.iter().map(|word| sigma_insensitive(&word.key)).collect();
    let mut claimed = vec![false; words.len()];
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    let folded_input = fold(input);
    for token in folded_input.split(' ').filter(|token| !token.is_empty()) {
        let needle = sigma_insensitive(&token.encode_utf16().collect::<Vec<u16>>());
        let Some(index) = (0..words.len()).find(|&position| !claimed[position] && keys[position].starts_with(&needle)) else { continue };
        claimed[index] = true;
        ranges.push((words[index].starts[0], words[index].ends[needle.len() - 1]));
    }
    ranges.sort();
    ranges.into_iter().map(|(start, end)| json!({ "start": start, "end": end })).collect()
}

/// `structuredSuggestion(item, input)`.
pub fn structured_suggestion(item: &Value, input: &str) -> Value {
    let text = trim(&js_string_or_empty(field(item, "text"))).to_string();
    let comma = text.find(',').filter(|c| *c > 0);
    let category_locality = field(item, "type") == Some(&json!("category-locality"));
    let main_text = if category_locality {
        let category = field(item, "category");
        let base = if is_truthy(category) { js_string(category) } else { text.clone() };
        base.replace('_', " ")
    } else if let Some(comma) = comma {
        trim(&text[..comma]).to_string()
    } else {
        text.clone()
    };
    let secondary_text = if category_locality {
        js_string_or_empty(field(item, "locality"))
    } else if let Some(comma) = comma {
        trim(&text[comma + 1..]).to_string()
    } else {
        String::new()
    };
    let kind = match field(item, "type") {
        Some(value) if truthy_value(value) => value.clone(),
        _ => json!("place"),
    };
    let shards = field(item, "shards");
    let shards_len = shards.and_then(Value::as_array).map(Vec::len);
    let doc_shard = field(item, "docShard");
    let entity_shard = if is_truthy(doc_shard) {
        doc_shard.cloned()
    } else if shards_len == Some(1) {
        shards.and_then(Value::as_array).map(|items| items[0].clone())
    } else {
        None
    };
    let entity_doc = if kind == json!("place")
        && !nullish(field(item, "doc"))
        && number(field(item, "count")) == 1.0
        && (is_truthy(doc_shard) || nullish(shards) || shards_len == Some(1))
    {
        let mut doc = Obj::new();
        doc.insert("index".into(), field(item, "doc").cloned().unwrap());
        if let Some(shard) = entity_shard.filter(truthy_value) {
            doc.insert("shard".into(), shard);
        }
        Some(Value::Object(doc))
    } else {
        None
    };
    let category = field(item, "category");
    let types = if is_truthy(category) {
        json!([category.unwrap()])
    } else if kind == json!("street") {
        json!(["route"])
    } else {
        json!([kind.clone()])
    };
    let mut selection = Obj::new();
    selection.insert("query".into(), Value::String(text.clone()));
    if shards_len.unwrap_or(0) > 0 {
        selection.insert("shards".into(), shards.unwrap().clone());
    }
    if let Some(doc) = entity_doc {
        selection.insert("doc".into(), doc);
    }
    with(
        item,
        vec![
            ("description", Value::String(text.clone())),
            ("mainText", Value::String(main_text)),
            ("secondaryText", Value::String(secondary_text)),
            ("matchedRanges", Value::Array(suggestion_match_ranges(&text, input))),
            ("kind", kind),
            ("types", types),
            ("selection", Value::Object(selection)),
        ],
    )
}

/// `structuredSuggestions(response, input)`.
pub fn structured_suggestions(response: &Value, input: &str) -> Value {
    let suggestions: Vec<Value> = match field(response, "suggestions") {
        Some(Value::Array(items)) => items.iter().map(|item| structured_suggestion(item, input)).collect(),
        _ => Vec::new(),
    };
    with(response, vec![("suggestions", Value::Array(suggestions))])
}

// ------------------------------------------------------------ constraints

/// `availableFacetNames(manifest)` in insertion order.
pub fn available_facet_names(manifest: &Value) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let mut add = |facets: &Value| {
        if let Some(map) = facets.as_object() {
            for key in map.keys() {
                if !names.contains(key) {
                    names.push(key.clone());
                }
            }
        }
    };
    add(at(manifest, "facets"));
    for shard in at(manifest, "shards").as_array().into_iter().flatten() {
        add(at(shard, "facets"));
    }
    names
}

/// `{ ...params, filters: { ...(params.filters || {}), facets: { ...(params.filters?.facets || {}), [name]: values } } }`.
pub fn with_facet_filter(params: &Value, entries: Vec<(String, Value)>) -> Value {
    let filters = field(params, "filters").filter(|v| truthy_value(v)).cloned().unwrap_or(json!({}));
    let mut facets = match field(&filters, "facets") {
        Some(value) if truthy_value(value) => spread(value),
        _ => Obj::new(),
    };
    for (key, value) in entries {
        facets.insert(key, value);
    }
    let filters = with(&filters, vec![("facets", Value::Object(facets))]);
    with(params, vec![("filters", filters)])
}

pub fn with_type_facet(params: &Value, type_: &str) -> Value {
    with_facet_filter(params, vec![("type".to_string(), json!([type_]))])
}
