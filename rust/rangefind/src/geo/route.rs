//! Route corridors: geometry normalization (polyline, point arrays,
//! GeoJSON), corridor boxes and point-to-route matching. Mirrors
//! `src/geo_route.js` (query side only).

use std::collections::HashSet;

use serde_json::Value;

use super::tree::{boxes_for_radius_e7, haversine_meters, lat_to_e7_f64, lon_to_e7_f64, GeoBox};
use super::{js_max, js_min, js_round};
use crate::error::{Error, Result};
use crate::util::json::to_number;

const EARTH_RADIUS_METERS: f64 = 6371008.7714;
const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;

const COORDINATE_ERROR: &str =
    "Rangefind: route coordinates require latitude in [-90, 90] and longitude in [-180, 180].";

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct RoutePoint {
    pub lat: f64,
    pub lon: f64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RouteSegment {
    pub index: usize,
    pub start: RoutePoint,
    pub end: RoutePoint,
    pub start_meters: f64,
    pub length_meters: f64,
    pub bearing_degrees: f64,
}

/// `prepareRoute` result.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedRoute {
    pub points: Vec<RoutePoint>,
    pub segments: Vec<RouteSegment>,
    pub total_meters: f64,
    pub corridor_meters: f64,
    /// Overlapping radius boxes along the route (deduplicated, in order).
    pub boxes: Vec<GeoBox>,
}

/// `matchPointToRoute` result (and, rounded, `routeMatchPublic`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RouteMatch {
    pub distance_meters: f64,
    pub progress_meters: f64,
    pub progress_ratio: f64,
    pub segment_index: usize,
    pub segment_progress: f64,
    pub bearing_degrees: f64,
    pub rejoin_point: RoutePoint,
}

/// `Number(value)` where `None` is `undefined` (NaN).
fn js_number(value: Option<&Value>) -> f64 {
    value.map(to_number).unwrap_or(f64::NAN)
}

/// `value ?? next`: `None`/`null` fall through.
fn nullish<'a>(value: Option<&'a Value>) -> Option<&'a Value> {
    value.filter(|item| !item.is_null())
}

fn type_error_reading(target: &str, key: &str) -> Error {
    Error::new(format!("Cannot read properties of {target} (reading '{key}')"))
}

/// `value[key]` for the JSON shapes route inputs can take.
fn js_get<'a>(value: &'a Value, key: &str) -> Result<Option<&'a Value>> {
    match value {
        Value::Null => Err(type_error_reading("null", key)),
        Value::Object(map) => Ok(map.get(key)),
        Value::Array(items) => Ok(key.parse::<usize>().ok().filter(|_| !key.starts_with('+')).and_then(|i| items.get(i))),
        _ => Ok(None),
    }
}

/// `coordinate[index]` (strings index UTF-16 code units).
fn js_index(value: &Value, index: usize) -> Result<Option<Value>> {
    match value {
        Value::String(text) => Ok(text
            .encode_utf16()
            .nth(index)
            .map(|unit| Value::String(String::from_utf16_lossy(&[unit])))),
        other => js_get(other, &index.to_string()).map(|item| item.cloned()),
    }
}

/// `finiteCoordinate(lat, lon)` on already-`Number()`ed values.
fn finite_coordinate(latitude: f64, longitude: f64) -> Result<RoutePoint> {
    if !latitude.is_finite()
        || !longitude.is_finite()
        || !(-90.0..=90.0).contains(&latitude)
        || !(-180.0..=180.0).contains(&longitude)
    {
        return Err(Error::new(COORDINATE_ERROR));
    }
    Ok(RoutePoint { lat: latitude, lon: longitude })
}

/// A raw `[lon, lat]` array or `{lat, lon|lng}` object.
fn raw_coordinate(raw: &Value) -> Result<RoutePoint> {
    if raw.is_array() {
        let lat = js_index(raw, 1)?;
        let lon = js_index(raw, 0)?;
        return finite_coordinate(js_number(lat.as_ref()), js_number(lon.as_ref()));
    }
    let lat = js_get(raw, "lat")?;
    let lon = match nullish(js_get(raw, "lon")?) {
        Some(lon) => Some(lon),
        None => js_get(raw, "lng")?,
    };
    finite_coordinate(js_number(lat), js_number(lon))
}

const POWERS_OF_TEN: [f64; 8] = [1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 10000000.0];

/// `decodePolyline(value, precision)`; `precision` is `Number(precision)`
/// (NaN or 0 → 5, then floored and clamped to `0..=7`).
pub fn decode_polyline(value: &str, precision: f64) -> Result<Vec<RoutePoint>> {
    let encoded: Vec<u16> = value.encode_utf16().collect();
    let digits = if precision.is_nan() || precision == 0.0 { 5.0 } else { precision };
    let factor = POWERS_OF_TEN[js_max(0.0, js_min(7.0, digits.floor())) as usize];
    let mut points = Vec::new();
    let mut index = 0usize;
    let mut lat: i64 = 0;
    let mut lon: i64 = 0;
    let component = |index: &mut usize| -> Result<i64> {
        let mut result: i32 = 0;
        let mut shift: u32 = 0;
        while *index < encoded.len() {
            let byte = encoded[*index] as i32 - 63;
            *index += 1;
            if !(0..=63).contains(&byte) {
                return Err(Error::new("Rangefind: route is not a valid encoded polyline."));
            }
            result |= (byte & 0x1f).wrapping_shl(shift);
            if byte < 0x20 {
                return Ok(if result & 1 != 0 { !(result >> 1) } else { result >> 1 } as i64);
            }
            shift += 5;
            if shift > 30 {
                return Err(Error::new("Rangefind: encoded polyline component is too large."));
            }
        }
        Err(Error::new("Rangefind: encoded polyline ends inside a coordinate."))
    };
    while index < encoded.len() {
        lat += component(&mut index)?;
        lon += component(&mut index)?;
        points.push(finite_coordinate(lat as f64 / factor, lon as f64 / factor)?);
    }
    Ok(points)
}

fn not_a_function(what: &str) -> Error {
    Error::new(format!("{what} is not a function"))
}

/// `geoJsonCoordinateLines(value)`: the coordinate arrays of every line.
fn geo_json_coordinate_lines(value: &Value) -> Result<Vec<&Value>> {
    let value_type = value.get("type").and_then(Value::as_str);
    let geometry = if value_type == Some("Feature") { value.get("geometry").unwrap_or(&Value::Null) } else { value };
    if value_type == Some("FeatureCollection") {
        let Some(features) = value.get("features").and_then(Value::as_array) else {
            return Err(not_a_function("value.features.flatMap"));
        };
        let mut lines = Vec::new();
        for feature in features {
            lines.extend(geo_json_coordinate_lines(feature)?);
        }
        return Ok(lines);
    }
    match geometry.get("type").and_then(Value::as_str) {
        Some("LineString") => Ok(vec![geometry.get("coordinates").unwrap_or(&Value::Null)]),
        Some("MultiLineString") => match geometry.get("coordinates") {
            Some(Value::Array(lines)) => Ok(lines.iter().collect()),
            _ => Err(not_a_function("lines.map")),
        },
        Some("GeometryCollection") => {
            let Some(geometries) = geometry.get("geometries").and_then(Value::as_array) else {
                return Err(not_a_function("geometry.geometries.flatMap"));
            };
            let mut lines = Vec::new();
            for item in geometries {
                lines.extend(geo_json_coordinate_lines(item)?);
            }
            Ok(lines)
        }
        _ => Err(Error::new("Rangefind: route GeoJSON must contain a LineString or MultiLineString.")),
    }
}

/// `normalizeRouteGeometry(route, options)`: a polyline string, an array of
/// `[lon, lat]` / `{lat, lon|lng}` points, or GeoJSON lines, flattened with
/// consecutive duplicates removed.
pub fn normalize_route_geometry(route: &Value, options: &Value) -> Result<Vec<RoutePoint>> {
    let mut lines: Vec<Vec<RoutePoint>> = Vec::new();
    match route {
        Value::String(text) => {
            let precision = nullish(options.get("precision"))
                .or_else(|| nullish(options.get("polylinePrecision")))
                .map(to_number)
                .unwrap_or(5.0);
            lines.push(decode_polyline(text, precision)?);
        }
        Value::Array(items) => {
            let mut line = Vec::with_capacity(items.len());
            for raw in items {
                line.push(raw_coordinate(raw)?);
            }
            lines.push(line);
        }
        other => {
            let raw_lines = geo_json_coordinate_lines(other)?;
            for raw_line in raw_lines {
                let Value::Array(coordinates) = raw_line else {
                    return Err(not_a_function("line.map"));
                };
                let mut line = Vec::with_capacity(coordinates.len());
                for coordinate in coordinates {
                    let lat = js_index(coordinate, 1)?;
                    let lon = js_index(coordinate, 0)?;
                    line.push(finite_coordinate(js_number(lat.as_ref()), js_number(lon.as_ref()))?);
                }
                lines.push(line);
            }
        }
    }
    let mut points: Vec<RoutePoint> = Vec::new();
    for line in lines {
        for point in line {
            let point = finite_coordinate(point.lat, point.lon)?;
            match points.last() {
                Some(previous) if previous.lat == point.lat && previous.lon == point.lon => {}
                _ => points.push(point),
            }
        }
    }
    if points.len() < 2 {
        return Err(Error::new("Rangefind: a route requires at least two distinct coordinates."));
    }
    Ok(points)
}

fn initial_bearing(start: &RoutePoint, end: &RoutePoint) -> f64 {
    let phi1 = start.lat * DEG_TO_RAD;
    let phi2 = end.lat * DEG_TO_RAD;
    let lambda = (end.lon - start.lon) * DEG_TO_RAD;
    let y = lambda.sin() * phi2.cos();
    let x = phi1.cos() * phi2.sin() - phi1.sin() * phi2.cos() * lambda.cos();
    (y.atan2(x) / DEG_TO_RAD + 360.0) % 360.0
}

fn unique_boxes(boxes: Vec<GeoBox>) -> Vec<GeoBox> {
    let mut seen = HashSet::new();
    boxes.into_iter().filter(|b| seen.insert(*b)).collect()
}

/// `prepareRoute(route, options)`. `options` is the JS options object
/// (`corridorMeters`, `precision`, `polylinePrecision`); pass `Value::Null`
/// for none.
pub fn prepare_route(route: &Value, options: &Value) -> Result<PreparedRoute> {
    let points = normalize_route_geometry(route, options)?;
    let corridor_meters = nullish(options.get("corridorMeters")).map(to_number).unwrap_or(1000.0);
    if !corridor_meters.is_finite() || corridor_meters <= 0.0 || corridor_meters > 100000.0 {
        return Err(Error::new("Rangefind: corridorMeters must be a positive number no greater than 100000."));
    }
    let mut segments = Vec::new();
    let mut total_meters = 0.0;
    let mut boxes = Vec::new();
    for index in 1..points.len() {
        let start = points[index - 1];
        let end = points[index];
        let length_meters = haversine_meters(start.lat, start.lon, end.lat, end.lon);
        if length_meters <= 0.0 {
            continue;
        }
        segments.push(RouteSegment {
            index: segments.len(),
            start,
            end,
            start_meters: total_meters,
            length_meters,
            bearing_degrees: initial_bearing(&start, &end),
        });
        let steps = js_max(1.0, (length_meters / js_max(1000.0, corridor_meters * 1.25)).ceil());
        let mut step = 0.0;
        while step <= steps {
            let ratio = step / steps;
            let lat = start.lat + (end.lat - start.lat) * ratio;
            let mut lon_delta = end.lon - start.lon;
            if lon_delta > 180.0 {
                lon_delta -= 360.0;
            }
            if lon_delta < -180.0 {
                lon_delta += 360.0;
            }
            let mut lon = start.lon + lon_delta * ratio;
            if lon > 180.0 {
                lon -= 360.0;
            }
            if lon < -180.0 {
                lon += 360.0;
            }
            boxes.extend(boxes_for_radius_e7(
                lat_to_e7_f64(lat).unwrap_or(0),
                lon_to_e7_f64(lon).unwrap_or(0),
                corridor_meters,
            ));
            step += 1.0;
        }
        total_meters += length_meters;
    }
    if segments.is_empty() {
        return Err(Error::new("Rangefind: a route requires at least one non-zero segment."));
    }
    Ok(PreparedRoute { points, segments, total_meters, corridor_meters, boxes: unique_boxes(boxes) })
}

/// `matchPointToRoute(prepared, lat, lon)`: the closest route position
/// (`None` only for a route without segments).
pub fn match_point_to_route(prepared: &PreparedRoute, lat: f64, lon: f64) -> Result<Option<RouteMatch>> {
    let point = finite_coordinate(lat, lon)?;
    let mut best: Option<RouteMatch> = None;
    for segment in &prepared.segments {
        let reference_lat = (segment.start.lat + segment.end.lat + point.lat) / 3.0 * DEG_TO_RAD;
        let mut end_delta_lon = segment.end.lon - segment.start.lon;
        if end_delta_lon > 180.0 {
            end_delta_lon -= 360.0;
        }
        if end_delta_lon < -180.0 {
            end_delta_lon += 360.0;
        }
        let mut point_delta_lon = point.lon - segment.start.lon;
        if point_delta_lon > 180.0 {
            point_delta_lon -= 360.0;
        }
        if point_delta_lon < -180.0 {
            point_delta_lon += 360.0;
        }
        let ax = 0.0;
        let ay = 0.0;
        let bx = end_delta_lon * DEG_TO_RAD * reference_lat.cos() * EARTH_RADIUS_METERS;
        let by = (segment.end.lat - segment.start.lat) * DEG_TO_RAD * EARTH_RADIUS_METERS;
        let px = point_delta_lon * DEG_TO_RAD * reference_lat.cos() * EARTH_RADIUS_METERS;
        let py = (point.lat - segment.start.lat) * DEG_TO_RAD * EARTH_RADIUS_METERS;
        // `x ** 2` is exactly `x * x` in V8.
        let denominator = (bx - ax) * (bx - ax) + (by - ay) * (by - ay);
        let ratio = if denominator != 0.0 && !denominator.is_nan() {
            js_max(0.0, js_min(1.0, ((px - ax) * (bx - ax) + (py - ay) * (by - ay)) / denominator))
        } else {
            0.0
        };
        let mut route_lon = segment.start.lon + end_delta_lon * ratio;
        if route_lon > 180.0 {
            route_lon -= 360.0;
        }
        if route_lon < -180.0 {
            route_lon += 360.0;
        }
        let route_lat = segment.start.lat + (segment.end.lat - segment.start.lat) * ratio;
        let distance_meters = haversine_meters(point.lat, point.lon, route_lat, route_lon);
        let progress_meters = segment.start_meters + segment.length_meters * ratio;
        let better = match &best {
            None => true,
            Some(current) => {
                distance_meters < current.distance_meters
                    || (distance_meters == current.distance_meters && progress_meters < current.progress_meters)
            }
        };
        if better {
            let progress_ratio = if prepared.total_meters != 0.0 && !prepared.total_meters.is_nan() {
                progress_meters / prepared.total_meters
            } else {
                0.0
            };
            best = Some(RouteMatch {
                distance_meters,
                progress_meters,
                progress_ratio,
                segment_index: segment.index,
                segment_progress: ratio,
                bearing_degrees: segment.bearing_degrees,
                rejoin_point: RoutePoint { lat: route_lat, lon: route_lon },
            });
        }
    }
    Ok(best)
}

/// `routeMatchPublic(match)`: the rounded public shape.
pub fn route_match_public(found: Option<&RouteMatch>) -> Option<RouteMatch> {
    let found = found?;
    Some(RouteMatch {
        distance_meters: js_round(found.distance_meters * 10.0) / 10.0,
        progress_meters: js_round(found.progress_meters * 10.0) / 10.0,
        progress_ratio: js_round(found.progress_ratio * 1e6) / 1e6,
        segment_index: found.segment_index,
        segment_progress: js_round(found.segment_progress * 1e6) / 1e6,
        bearing_degrees: js_round(found.bearing_degrees * 10.0) / 10.0,
        rejoin_point: RoutePoint {
            lat: js_round(found.rejoin_point.lat * 1e7) / 1e7,
            lon: js_round(found.rejoin_point.lon * 1e7) / 1e7,
        },
    })
}
