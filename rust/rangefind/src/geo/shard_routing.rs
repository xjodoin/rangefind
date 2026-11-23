//! Shard bounding-box routing for sharded geo indexes. Mirrors
//! `src/geo_shard_routing.js`.
//!
//! A shard `bbox` is `[minLat, minLon, maxLat, maxLon]` in degrees; the
//! longitude range wraps across the antimeridian when `minLon > maxLon`.

use super::tree::{haversine_meters_e7, lat_to_e7_f64, lon_to_e7_f64};
use super::{js_max, js_min};

/// A query box in degrees (`{minLat, maxLat, minLon, maxLon}` after the JS
/// `Number(...)` coercion). Latitudes may come in either order.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LatLonBox {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

fn wrap_lon_delta(a: f64, b: f64) -> f64 {
    ((a - b + 540.0) % 360.0) - 180.0
}

fn longitude_intervals(min_lon: f64, max_lon: f64) -> Vec<[f64; 2]> {
    if min_lon <= max_lon {
        vec![[min_lon, max_lon]]
    } else {
        vec![[min_lon, 180.0], [-180.0, max_lon]]
    }
}

fn longitude_contains(min_lon: f64, max_lon: f64, lon: f64) -> bool {
    if min_lon <= max_lon {
        lon >= min_lon && lon <= max_lon
    } else {
        lon >= min_lon || lon <= max_lon
    }
}

/// `latToE7`/`lonToE7` results coerced the way `haversineMetersE7` sees
/// them (`null` → 0).
fn e7_or_zero(value: Option<i64>) -> f64 {
    value.unwrap_or(0) as f64
}

/// `shardBboxDistanceMeters(bbox, lat, lon)`: meters from the point to the
/// nearest point of the shard bbox (0 inside).
pub fn shard_bbox_distance_meters(bbox: &[f64; 4], lat: f64, lon: f64) -> f64 {
    let clamped_lat = js_min(js_max(lat, bbox[0]), bbox[2]);
    let mut clamped_lon = lon;
    if !longitude_contains(bbox[1], bbox[3], lon) {
        clamped_lon = if wrap_lon_delta(lon, bbox[1]).abs() <= wrap_lon_delta(lon, bbox[3]).abs() {
            bbox[1]
        } else {
            bbox[3]
        };
    }
    haversine_meters_e7(
        e7_or_zero(lat_to_e7_f64(lat)),
        e7_or_zero(lon_to_e7_f64(lon)),
        e7_or_zero(lat_to_e7_f64(clamped_lat)),
        e7_or_zero(lon_to_e7_f64(clamped_lon)),
    )
}

/// `shardBoxIntersects(bbox, box)`.
pub fn shard_box_intersects(bbox: &[f64; 4], query: &LatLonBox) -> bool {
    let min_lat = js_min(query.min_lat, query.max_lat);
    let max_lat = js_max(query.min_lat, query.max_lat);
    if bbox[2] < min_lat || bbox[0] > max_lat {
        return false;
    }
    let shard_intervals = longitude_intervals(bbox[1], bbox[3]);
    let query_intervals = longitude_intervals(query.min_lon, query.max_lon);
    shard_intervals
        .iter()
        .any(|shard| query_intervals.iter().any(|q| shard[1] >= q[0] && shard[0] <= q[1]))
}
