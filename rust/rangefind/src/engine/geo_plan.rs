//! Geo request plans (`makeGeoPlan`, `geoPointMatchesE7`, `withGeoFilters`,
//! `geoDocMatches` in `src/runtime.js`).

use std::cell::RefCell;
use std::collections::HashSet;

use serde_json::{json, Map, Value};

use crate::codec::doc_values::DocValue;
use crate::error::{Error, Result};
use crate::geo::{
    box_contains_point_e7, boxes_for_radius_e7, haversine_meters_e7, lat_to_e7, lat_to_e7_f64, lon_to_e7, lon_to_e7_f64, match_point_to_route,
    prepare_route, GeoBox, PreparedRoute,
};
use crate::util::json::{num, to_number, truthy, NULL};

use super::filters::{Filters, ValueStore};
use super::index::IndexEngine;

const GEO_E7_PRUNE_MARGIN_DEGREES: f64 = 1e-7;

#[derive(Clone, Copy, Debug)]
pub struct NearPoint {
    pub lat_e7: i64,
    pub lon_e7: i64,
    pub radius_meters: Option<f64>,
}

#[derive(Clone, Copy, Debug)]
pub struct GeoBoost {
    pub weight: f64,
    pub pivot_meters: f64,
}

pub struct GeoPlan {
    pub field: String,
    pub meta: Value,
    pub lat_field: String,
    pub lon_field: String,
    pub boxes: Vec<GeoBox>,
    pub near: Option<NearPoint>,
    pub sort: bool,
    pub route_sort: bool,
    pub route: Option<PreparedRoute>,
    pub route_position_meters: f64,
    pub route_reverse: bool,
    pub viewport_anchor: Option<(f64, f64)>,
    pub boost: Option<GeoBoost>,
    pub filtered: bool,
    pub doc_set: RefCell<Option<HashSet<i32>>>,
    pub doc_set_stats: RefCell<Option<Map<String, Value>>>,
}

impl GeoPlan {
    pub fn has_doc_set(&self) -> bool {
        self.doc_set.borrow().is_some()
    }

    pub fn doc_set_stats(&self) -> Option<Map<String, Value>> {
        self.doc_set_stats.borrow().clone()
    }

    /// `geoPointMatchesE7`.
    pub fn point_matches_e7(&self, lat_e7: i64, lon_e7: i64) -> bool {
        if let Some(route) = &self.route {
            return match match_point_to_route(route, lat_e7 as f64 / 1e7, lon_e7 as f64 / 1e7) {
                Ok(Some(found)) => found.distance_meters <= route.corridor_meters,
                _ => false,
            };
        }
        if let Some(near) = self.near {
            if let Some(radius) = near.radius_meters {
                return haversine_meters_e7(near.lat_e7 as f64, near.lon_e7 as f64, lat_e7 as f64, lon_e7 as f64) <= radius;
            }
        }
        if self.boxes.is_empty() {
            return true;
        }
        self.boxes.iter().any(|b| box_contains_point_e7(b, lat_e7, lon_e7))
    }

    /// `geoDocMatches`.
    pub fn doc_matches(&self, engine: &IndexEngine, store: Option<&ValueStore>, doc: i32, known: Option<(&str, &DocValue)>) -> bool {
        if let Some(set) = self.doc_set.borrow().as_ref() {
            return set.contains(&doc);
        }
        let value = |field: &str| match known {
            Some((name, value)) if name == field => value.clone(),
            _ => engine.value_for_doc(store, field, doc),
        };
        let lat = value(&self.lat_field).as_number().and_then(lat_to_e7_f64);
        let lon = value(&self.lon_field).as_number().and_then(lon_to_e7_f64);
        match (lat, lon) {
            (Some(lat), Some(lon)) => self.point_matches_e7(lat, lon),
            _ => false,
        }
    }

    /// `withGeoFilters`: rewrite the bounding box into numeric range
    /// filters so block/chunk/page summaries prune for free.
    pub fn with_geo_filters(self: &std::rc::Rc<Self>, filters: &Filters) -> Filters {
        let mut out = filters.clone();
        out.geo = Some(self.clone());
        if !self.filtered {
            return out;
        }
        let margin = GEO_E7_PRUNE_MARGIN_DEGREES;
        let min_lat = self.boxes.iter().map(|b| b.min_lat_e7).min().unwrap_or(0) as f64 / 1e7 - margin;
        let max_lat = self.boxes.iter().map(|b| b.max_lat_e7).max().unwrap_or(0) as f64 / 1e7 + margin;
        intersect_range(&mut out.numbers, &self.lat_field, min_lat, max_lat);
        if self.boxes.len() == 1 {
            let b = &self.boxes[0];
            intersect_range(&mut out.numbers, &self.lon_field, b.min_lon_e7 as f64 / 1e7 - margin, b.max_lon_e7 as f64 / 1e7 + margin);
        }
        out
    }
}

/// `intersectNumberRange`, applied in place keeping key order.
fn intersect_range(numbers: &mut Vec<(String, Value)>, field: &str, min: f64, max: f64) {
    let current = numbers.iter().find(|(f, _)| f == field).map(|(_, v)| v.clone());
    let pick = |key: &str, fallback: f64, combine: fn(f64, f64) -> f64| -> Value {
        match current.as_ref().and_then(|c| c.get(key)).filter(|v| !v.is_null()) {
            Some(value) => num(combine(to_number(value), fallback)),
            None => num(fallback),
        }
    };
    let range = json!({ "min": pick("min", min, crate::geo::js_max), "max": pick("max", max, crate::geo::js_min) });
    match numbers.iter_mut().find(|(f, _)| f == field) {
        Some(item) => item.1 = range,
        None => numbers.push((field.to_string(), range)),
    }
}

/// `normalizeGeoBoxE7`.
fn normalize_box(value: &Value) -> Result<Vec<GeoBox>> {
    let get = |key: &str| value.get(key).unwrap_or(&NULL);
    let (Some(min_lat), Some(max_lat), Some(min_lon), Some(max_lon)) =
        (lat_to_e7(get("minLat")), lat_to_e7(get("maxLat")), lon_to_e7(get("minLon")), lon_to_e7(get("maxLon")))
    else {
        return Err(Error::new("Rangefind: geo.box needs finite minLat, maxLat, minLon, and maxLon."));
    };
    if min_lat > max_lat {
        return Err(Error::new("Rangefind: geo.box has minLat above maxLat."));
    }
    if min_lon <= max_lon {
        return Ok(vec![GeoBox { min_lat_e7: min_lat, max_lat_e7: max_lat, min_lon_e7: min_lon, max_lon_e7: max_lon }]);
    }
    Ok(vec![
        GeoBox { min_lat_e7: min_lat, max_lat_e7: max_lat, min_lon_e7: min_lon, max_lon_e7: 1_800_000_000 },
        GeoBox { min_lat_e7: min_lat, max_lat_e7: max_lat, min_lon_e7: -1_800_000_000, max_lon_e7: max_lon },
    ])
}

impl IndexEngine {
    /// `makeGeoPlan`.
    pub fn make_geo_plan(&self, geo: &Value) -> Result<GeoPlan> {
        let fields = self.manifest_at(&["geo", "fields"]);
        let names: Vec<String> = fields.as_object().map(|m| m.keys().cloned().collect()).unwrap_or_default();
        if names.is_empty() {
            return Err(Error::new("Rangefind: this index has no geo fields."));
        }
        let requested = geo.get("field").filter(|f| truthy(f)).map(crate::util::json::string_or_empty);
        let field = requested.clone().unwrap_or_else(|| if names.len() == 1 { names[0].clone() } else { String::new() });
        let Some(meta) = fields.get(&field).filter(|m| !m.is_null()).cloned() else {
            return Err(Error::new(format!("Rangefind: unknown geo field \"{}\".", requested.unwrap_or_default())));
        };
        let sort_value = geo.get("sort").and_then(Value::as_str).unwrap_or("");
        let mut plan = GeoPlan {
            field,
            lat_field: meta.get("lat_component").and_then(Value::as_str).unwrap_or("").to_string(),
            lon_field: meta.get("lon_component").and_then(Value::as_str).unwrap_or("").to_string(),
            meta,
            boxes: Vec::new(),
            near: None,
            sort: sort_value == "distance",
            route_sort: sort_value == "route",
            route: None,
            route_position_meters: 0.0,
            route_reverse: false,
            viewport_anchor: None,
            boost: None,
            filtered: false,
            doc_set: RefCell::new(None),
            doc_set_stats: RefCell::new(None),
        };
        if let Some(route_value) = geo.get("route").filter(|r| truthy(r)) {
            let options = json!({
                "corridorMeters": geo.get("corridorMeters").cloned().unwrap_or(Value::Null),
                "precision": geo.get("polylinePrecision").cloned().unwrap_or(Value::Null)
            });
            let route = prepare_route(route_value, &options)?;
            plan.boxes = route.boxes.clone();
            let position = to_number(geo.get("routePositionMeters").filter(|v| truthy(v)).unwrap_or(&json!(0)));
            plan.route_position_meters = if position.is_finite() { position.clamp(0.0, route.total_meters) } else { 0.0 };
            plan.route_reverse = geo.get("routeDirection").and_then(Value::as_str) == Some("reverse");
            if let Some(viewport) = geo.get("viewport").filter(|v| truthy(v)) {
                let field = |key: &str| to_number(viewport.get(key).unwrap_or(&NULL));
                let lat = match viewport.get("lat").filter(|v| !v.is_null()) {
                    Some(v) => to_number(v),
                    None => (field("minLat") + field("maxLat")) / 2.0,
                };
                let lon = match viewport.get("lon").or_else(|| viewport.get("lng")).filter(|v| !v.is_null()) {
                    Some(v) => to_number(v),
                    None => (field("minLon") + field("maxLon")) / 2.0,
                };
                if lat.is_finite() && lon.is_finite() {
                    plan.viewport_anchor = Some((lat, lon));
                }
            }
            plan.route = Some(route);
        }
        if let Some(near) = geo.get("near").filter(|n| truthy(n)) {
            let (Some(lat_e7), Some(lon_e7)) = (lat_to_e7(near.get("lat").unwrap_or(&NULL)), lon_to_e7(near.get("lon").unwrap_or(&NULL))) else {
                return Err(Error::new("Rangefind: geo.near needs finite lat and lon."));
            };
            let radius = to_number(near.get("radiusMeters").unwrap_or(&Value::Null));
            let radius = if near.get("radiusMeters").map(|v| !v.is_null()).unwrap_or(false) && radius.is_finite() && radius > 0.0 { Some(radius) } else { None };
            plan.near = Some(NearPoint { lat_e7, lon_e7, radius_meters: radius });
            if let Some(radius) = radius {
                if plan.route.is_some() {
                    return Err(Error::new("Rangefind: geo supports route or a near radius, not both."));
                }
                plan.boxes = boxes_for_radius_e7(lat_e7, lon_e7, radius);
            }
        }
        if let Some(bbox) = geo.get("box").filter(|b| truthy(b)) {
            if plan.near.and_then(|n| n.radius_meters).is_some() {
                return Err(Error::new("Rangefind: geo supports a radius or box, not both."));
            }
            if plan.route.is_some() {
                return Err(Error::new("Rangefind: geo supports route or box, not both."));
            }
            plan.boxes = normalize_box(bbox)?;
        }
        if let Some(boost) = geo.get("boost").filter(|b| truthy(b)) {
            let weight = to_number(boost.get("weight").unwrap_or(&NULL));
            let pivot = to_number(boost.get("pivotMeters").unwrap_or(&NULL));
            plan.boost = Some(GeoBoost {
                weight: if weight.is_finite() && weight > 0.0 { weight } else { 1.0 },
                pivot_meters: if pivot.is_finite() && pivot > 0.0 { pivot } else { 1000.0 },
            });
            if plan.near.is_none() {
                return Err(Error::new("Rangefind: geo.boost needs geo.near."));
            }
        }
        if plan.sort && plan.near.is_none() {
            return Err(Error::new("Rangefind: geo.sort \"distance\" needs geo.near."));
        }
        if plan.route_sort && plan.route.is_none() {
            return Err(Error::new("Rangefind: geo.sort \"route\" needs geo.route."));
        }
        if plan.boxes.is_empty() && !plan.sort {
            return Err(Error::new("Rangefind: geo needs near with radiusMeters, box, or sort \"distance\"."));
        }
        plan.filtered = !plan.boxes.is_empty();
        Ok(plan)
    }
}
