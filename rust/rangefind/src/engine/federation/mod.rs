//! Federated engines: `createGenerationalSearch` and `createShardedSearch`
//! from `src/runtime.js`, plus the helpers both layers share
//! (`mergeFederatedFacets`, the federation sort-plan parser and the JS
//! number/slice semantics the merge code relies on).

pub mod generational;
pub mod sharded;
pub mod text_routing;

use std::cmp::Ordering;
use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::util::json::{js_number_to_string, num, to_number, truthy, NULL};
use crate::util::jsstr::cmp16;

use super::options::EngineOptions;

pub use generational::GenerationalEngine;
pub use sharded::ShardedEngine;

/// JS `String(value)` for JSON values.
pub(crate) fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => js_number_to_string(number.as_f64().unwrap_or(f64::NAN)),
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| if item.is_null() { String::new() } else { js_string(item) })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

/// `Number(value)` where a missing property is `undefined` (NaN), unlike
/// `null` (0).
pub(crate) fn js_number(value: Option<&Value>) -> f64 {
    match value {
        None => f64::NAN,
        Some(value) => to_number(value),
    }
}

/// `Math.max(a, b)` (NaN-propagating).
pub(crate) fn js_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}

/// `Math.min(a, b)` (NaN-propagating).
pub(crate) fn js_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.min(b)
    }
}

/// `Number(value || fallback)`.
pub(crate) fn num_or_fallback(value: Option<&Value>, fallback: f64) -> f64 {
    match value {
        Some(value) if truthy(value) => to_number(value),
        _ => fallback,
    }
}

/// `Math.max(1, Number(params.page || 1))`.
pub(crate) fn page_param(params: &Value) -> f64 {
    js_max(1.0, num_or_fallback(params.get("page"), 1.0))
}

/// `Math.max(1, Math.min(max, Math.floor(Number(params.size || fallback))))`.
pub(crate) fn size_param(params: &Value, max: f64, fallback: f64) -> f64 {
    js_max(1.0, js_min(max, num_or_fallback(params.get("size"), fallback).floor()))
}

/// Index resolution of `Array.prototype.slice(start, end)`.
pub(crate) fn slice_bounds(len: usize, start: f64, end: f64) -> (usize, usize) {
    let resolve = |value: f64| -> usize {
        let value = if value.is_nan() { 0.0 } else { value.trunc() };
        if value < 0.0 {
            (len as f64 + value).max(0.0) as usize
        } else {
            value.min(len as f64) as usize
        }
    };
    let from = resolve(start);
    let to = resolve(end);
    (from, to.max(from))
}

/// `array.slice(start, end)` on an owned vector.
pub(crate) fn js_slice<T>(items: Vec<T>, start: f64, end: f64) -> Vec<T> {
    let (from, to) = slice_bounds(items.len(), start, end);
    items.into_iter().skip(from).take(to - from).collect()
}

/// `value || 0` as a number (`response.total || 0`, `row.score || 0`).
pub(crate) fn number_or_zero(value: Option<&Value>) -> f64 {
    match value {
        Some(value) if truthy(value) => to_number(value),
        _ => 0.0,
    }
}

/// A shallow copy of `params` as an object (`{ ...params }`).
pub(crate) fn spread(params: &Value) -> Map<String, Value> {
    params.as_object().cloned().unwrap_or_default()
}

/// `{ ...params, key: undefined }`: JSON drops undefined keys.
pub(crate) fn without(map: &mut Map<String, Value>, keys: &[&str]) {
    for key in keys {
        map.shift_remove(*key);
    }
}

/// `{ ...result, key: value }`: an existing key keeps its position.
pub(crate) fn with_key(result: &Value, key: &str, value: Value) -> Value {
    let mut map = spread(result);
    map.insert(key.to_string(), value);
    Value::Object(map)
}

/// `params.x != null` for a property that may be missing.
pub(crate) fn present(value: Option<&Value>) -> Option<&Value> {
    value.filter(|value| !value.is_null())
}

/// The JS `stats` object of a response (`response.stats`), or null.
pub(crate) fn stats_of(response: &Value) -> &Value {
    response.get("stats").unwrap_or(&NULL)
}

/// A federation sort plan (`parseSortPlan`).
#[derive(Clone, Debug)]
pub(crate) struct SortPlan {
    pub field: String,
    pub desc: bool,
}

/// `parseSortPlan(sort)`: unknown fields fall back to the unsorted path,
/// exactly like they would inside one sub-index.
pub(crate) fn parse_sort_plan(sort: Option<&Value>, sortable: &[String]) -> Option<SortPlan> {
    let sort = sort.filter(|value| truthy(value))?;
    let field = match sort {
        Value::String(text) => text.strip_prefix('-').unwrap_or(text).to_string(),
        other => match other.get("field") {
            Some(Value::String(text)) => text.clone(),
            _ => return None,
        },
    };
    if field.is_empty() || !sortable.iter().any(|name| *name == field) {
        return None;
    }
    let desc = match sort {
        Value::String(text) => text.starts_with('-'),
        other => {
            let order = [other.get("order"), other.get("direction")]
                .into_iter()
                .flatten()
                .find(|value| truthy(value))
                .map(js_string)
                .unwrap_or_else(|| "asc".to_string());
            order.to_lowercase() == "desc"
        }
    };
    Some(SortPlan { field, desc })
}

/// Sortable field names (`numbers` + `booleans`) of a manifest.
pub(crate) fn sortable_fields(manifest: &Value) -> Vec<String> {
    let mut out = Vec::new();
    for key in ["numbers", "booleans"] {
        if let Some(fields) = manifest.get(key).and_then(Value::as_array) {
            for field in fields {
                out.push(js_string(field.get("name").unwrap_or(&Value::Null)));
            }
        }
    }
    out
}

/// `geo.sort === "distance"` / `"route"`.
pub(crate) fn geo_sort_is(params: &Value, value: &str) -> bool {
    params.get("geo").and_then(|geo| geo.get("sort")).and_then(Value::as_str) == Some(value)
}

/// Options for a child engine (`{ ...options, baseUrl, manifestName,
/// ...extra }`). An injected `manifest` never propagates to children: it
/// would make every child reopen the federation root.
pub(crate) fn child_options(options: &EngineOptions, manifest_name: Option<String>, extra: &[(&str, Value)]) -> EngineOptions {
    let mut raw = options.raw.as_object().cloned().unwrap_or_default();
    raw.shift_remove("manifest");
    match manifest_name {
        Some(name) => {
            raw.insert("manifestName".into(), Value::String(name));
        }
        None => {
            raw.shift_remove("manifestName");
        }
    }
    for (key, value) in extra {
        raw.insert((*key).to_string(), value.clone());
    }
    EngineOptions::from_json(Value::Object(raw))
}

/// JS `a < b ? -1 : 1` on facet/suggestion values (strings compare by
/// UTF-16 code units; anything else through `String()`).
fn js_less(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64().unwrap_or(f64::NAN) < y.as_f64().unwrap_or(f64::NAN),
        _ => cmp16(&js_string(a), &js_string(b)) == Ordering::Less,
    }
}

/// `(a, b) => a < b ? -1 : 1` as an ordering.
pub(crate) fn less_or_greater(a: &Value, b: &Value) -> Ordering {
    if js_less(a, b) {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

/// Turn a JS comparator result into an `Ordering` (`0` and NaN are equal).
pub(crate) fn ordering_of(value: f64) -> Ordering {
    if value < 0.0 {
        Ordering::Less
    } else if value > 0.0 {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

/// `mergeFederatedFacets(responses)`: counts add across sub-indexes,
/// exactness survives only if every side was exact.
pub(crate) fn merge_federated_facets(responses: &[&Value]) -> Value {
    struct Target {
        exact: bool,
        values: Vec<Map<String, Value>>,
        by_value: HashMap<String, usize>,
    }
    let mut order: Vec<String> = Vec::new();
    let mut facets: HashMap<String, Target> = HashMap::new();
    for response in responses {
        let Some(map) = response.get("facets").and_then(Value::as_object) else { continue };
        for (field, data) in map {
            if !facets.contains_key(field) {
                order.push(field.clone());
                facets.insert(field.clone(), Target { exact: true, values: Vec::new(), by_value: HashMap::new() });
            }
            let target = facets.get_mut(field).unwrap();
            target.exact = target.exact && data.get("exact") != Some(&Value::Bool(false));
            for item in data.get("values").and_then(Value::as_array).into_iter().flatten() {
                let key = item.get("value").map(|value| value.to_string()).unwrap_or_default();
                match target.by_value.get(&key) {
                    Some(&slot) => {
                        let existing = &mut target.values[slot];
                        let sum = to_number(existing.get("count").unwrap_or(&Value::Null)) + js_number(item.get("count"));
                        existing.insert("count".into(), num(sum));
                    }
                    None => {
                        target.by_value.insert(key, target.values.len());
                        target.values.push(spread(item));
                    }
                }
            }
        }
    }
    let mut out = Map::new();
    for field in order {
        let mut target = facets.remove(&field).unwrap();
        target.values.sort_by(|a, b| {
            let count = js_number(b.get("count")) - js_number(a.get("count"));
            match ordering_of(count) {
                Ordering::Equal => less_or_greater(a.get("value").unwrap_or(&NULL), b.get("value").unwrap_or(&NULL)),
                other => other,
            }
        });
        let mut entry = Map::new();
        entry.insert("values".into(), Value::Array(target.values.into_iter().map(Value::Object).collect()));
        entry.insert("exact".into(), Value::Bool(target.exact));
        out.insert(field, Value::Object(entry));
    }
    Value::Object(out)
}

/// The `linkRankBoost` / `rankPriorBoost` merge both layers apply to the
/// merged stats: each sub-index boosts its own candidates before the merge.
pub(crate) fn merge_boost_stats(stats: &mut Map<String, Value>, responses: &[&Value]) {
    let flag = |key: &str| responses.iter().any(|response| truthy(stats_of(response).get(key).unwrap_or(&NULL)));
    let pool = |key: &str| responses.iter().map(|response| number_or_zero(stats_of(response).get(key))).sum::<f64>();
    if flag("linkRankBoost") {
        stats.insert("linkRankBoost".into(), Value::Bool(true));
        stats.insert("linkRankBoostPool".into(), num(pool("linkRankBoostPool")));
    }
    if flag("rankPriorBoost") {
        stats.insert("rankPriorBoost".into(), Value::Bool(true));
        stats.insert("rankPriorBoostPool".into(), num(pool("rankPriorBoostPool")));
        match responses.iter().find_map(|response| stats_of(response).get("rankPriorField").filter(|value| truthy(value))) {
            Some(field) => {
                stats.insert("rankPriorField".into(), field.clone());
            }
            None => {
                stats.shift_remove("rankPriorField");
            }
        }
    }
}

/// `correctedQuery` / `corrections` of the first response that corrected.
pub(crate) fn first_correction(responses: &[&Value]) -> Option<(Value, Option<Value>)> {
    let found = responses.iter().find(|response| truthy(response.get("correctedQuery").unwrap_or(&NULL)))?;
    Some((found["correctedQuery"].clone(), found.get("corrections").cloned()))
}

/// The merged `results` array of a response (`response.results || []`).
pub(crate) fn results_of(response: &Value) -> &[Value] {
    response.get("results").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[])
}

/// A tombstone set lookup on a result's `index` (`set.has(row.index)`).
pub(crate) fn index_key(value: Option<&Value>) -> Option<i64> {
    let number = value?.as_f64()?;
    if number.fract() == 0.0 && number.is_finite() {
        Some(number as i64)
    } else {
        None
    }
}
