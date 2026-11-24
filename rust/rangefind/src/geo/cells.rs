//! Web-mercator category cells and `rfgeocategorycell-v1` blocks. Mirrors
//! `src/geo_cells.js` (query side only).

use std::collections::{HashMap, HashSet};

use super::route::PreparedRoute;
use super::tree::{boxes_for_radius_e7, lat_to_e7_f64, lon_to_e7_f64, GeoBox};
use super::{js_max, js_min};
use crate::error::{Error, Result};
use crate::util::bytes::{assert_magic, Reader};

const GEO_CELL_BLOCK_MAGIC: [u8; 4] = [0x52, 0x46, 0x47, 0x43]; // RFGC
const GEO_CELL_BLOCK_VERSION: u64 = 1;
const WEB_MERCATOR_MAX_LAT: f64 = 85.05112878;

pub const GEO_CATEGORY_CELL_FORMAT: &str = "rfgeocategorycell-v1";

/// A tile `{zoom, x, y}`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct GeoCell {
    pub zoom: u32,
    pub x: u32,
    pub y: u32,
}

/// One leaf's accepted point ordinals inside a cell route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeoCellMember {
    pub leaf: u64,
    pub ordinals: Vec<u64>,
}

/// One `routes` entry: `geoCellRouteKey(zoom, x, y, code)` → members (a JS
/// `Map` of leaf → ordinals, kept in insertion order).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeoCellRoute {
    pub key: String,
    pub members: Vec<GeoCellMember>,
}

/// `decodeGeoCellBlock` result. `routes` keeps the JS `Map` order; use
/// [`GeoCellBlock::route`] for `routes.get(key)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GeoCellBlock {
    pub format: &'static str,
    pub block_zoom: u64,
    pub block_x: u64,
    pub block_y: u64,
    pub routes: Vec<GeoCellRoute>,
    by_key: HashMap<String, usize>,
}

impl GeoCellBlock {
    /// `block.routes.get(key)`
    pub fn route(&self, key: &str) -> Option<&GeoCellRoute> {
        self.by_key.get(key).map(|&index| &self.routes[index])
    }
}

fn clamp(value: f64, min: f64, max: f64) -> f64 {
    js_max(min, js_min(max, value))
}

/// `Number(x) || 0` for an already-numeric argument.
fn or_zero(value: f64) -> f64 {
    if value.is_nan() {
        0.0
    } else {
        value
    }
}

fn pow2(exponent: u32) -> f64 {
    f64::from_bits(((1023 + exponent as u64) & 0x7ff) << 52)
}

/// `geoCellForE7(latE7, lonE7, zoom)`. `zoom` is floored and clamped to
/// `0..=22` (NaN → 0).
pub fn geo_cell_for_e7(lat_e7: f64, lon_e7: f64, zoom: f64) -> GeoCell {
    let z = clamp(or_zero(zoom).floor(), 0.0, 22.0) as u32;
    let scale = pow2(z);
    let lon = clamp(lon_e7 / 1e7, -180.0, 180.0);
    let lat = clamp(lat_e7 / 1e7, -WEB_MERCATOR_MAX_LAT, WEB_MERCATOR_MAX_LAT);
    let radians = lat * std::f64::consts::PI / 180.0;
    let x = clamp((((lon + 180.0) / 360.0) * scale).floor(), 0.0, scale - 1.0);
    let y = clamp(
        ((1.0 - radians.tan().asinh() / std::f64::consts::PI) / 2.0 * scale).floor(),
        0.0,
        scale - 1.0,
    );
    GeoCell { zoom: z, x: x as u32, y: y as u32 }
}

/// `geoCellBlock(cell, blockZoom)`: the coarser block a cell falls in.
pub fn geo_cell_block(cell: &GeoCell, block_zoom: f64) -> GeoCell {
    let wanted = js_max(0.0, or_zero(block_zoom).floor());
    let zoom = if (cell.zoom as f64) <= wanted { cell.zoom } else { wanted as u32 };
    let shift = cell.zoom - zoom;
    GeoCell { zoom, x: cell.x >> shift, y: cell.y >> shift }
}

/// `encodeURIComponent` for a well-formed string.
pub fn encode_uri_component(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(value.len());
    for &byte in value.as_bytes() {
        let keep = byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'!' | b'~' | b'*' | b'\'' | b'(' | b')');
        if keep {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 15) as usize] as char);
        }
    }
    out
}

/// `geoCellBlockKey(field, facet, group, block)`. Callers pass
/// `String(field)` / `String(facet)` (e.g. `"undefined"` for a missing facet).
pub fn geo_cell_block_key(field: &str, facet: &str, group: i64, block: &GeoCell) -> String {
    format!(
        "{}|{}|{:06}|{:02}|{:07}|{:07}",
        encode_uri_component(field),
        encode_uri_component(facet),
        group.max(0),
        block.zoom,
        block.y,
        block.x
    )
}

/// `geoCellRouteKey(zoom, x, y, code)`.
pub fn geo_cell_route_key(zoom: u64, x: u64, y: u64, code: u64) -> String {
    format!("{zoom}:{y}:{x}:{code}")
}

/// `Math.max(1, Math.floor(Number(limit) || 1))`; `f64::INFINITY` is the JS
/// default (no limit).
fn normalize_limit(limit: f64) -> f64 {
    if limit.is_nan() || limit == 0.0 {
        1.0
    } else {
        js_max(1.0, limit.floor())
    }
}

/// `geoCellsForBoxes(boxes, zoom, limit)`: distinct cells covering the boxes
/// in first-seen order, or `None` once more than `limit` cells are needed.
pub fn geo_cells_for_boxes(boxes: &[GeoBox], zoom: u32, limit: f64) -> Option<Vec<GeoCell>> {
    let max = normalize_limit(limit);
    let mut seen: HashSet<(u32, u32)> = HashSet::new();
    let mut cells = Vec::new();
    for b in boxes {
        let north_west = geo_cell_for_e7(b.max_lat_e7 as f64, b.min_lon_e7 as f64, zoom as f64);
        let south_east = geo_cell_for_e7(b.min_lat_e7 as f64, b.max_lon_e7 as f64, zoom as f64);
        for y in north_west.y..=south_east.y {
            for x in north_west.x..=south_east.x {
                if seen.insert((y, x)) {
                    cells.push(GeoCell { zoom: north_west.zoom, x, y });
                    if cells.len() as f64 > max {
                        return None;
                    }
                }
            }
        }
    }
    Some(cells)
}

/// `geoCellsForRoute(route, zoom, limit)`: rasterizes the buffered route at
/// `zoom`, or `None` once more than `limit` cells are needed.
pub fn geo_cells_for_route(route: &PreparedRoute, zoom: u32, limit: f64) -> Option<Vec<GeoCell>> {
    let max = normalize_limit(limit);
    let mut index: HashMap<(u32, u32), usize> = HashMap::new();
    let mut cells: Vec<GeoCell> = Vec::new();
    let mut add_boxes = |boxes: &[GeoBox], cells: &mut Vec<GeoCell>| -> bool {
        let Some(next) = geo_cells_for_boxes(boxes, zoom, js_max(1.0, max - cells.len() as f64)) else {
            return false;
        };
        for cell in next {
            match index.get(&(cell.y, cell.x)) {
                Some(&position) => cells[position] = cell,
                None => {
                    index.insert((cell.y, cell.x), cells.len());
                    cells.push(cell);
                }
            }
            if cells.len() as f64 > max {
                return false;
            }
        }
        true
    };
    let pi = std::f64::consts::PI;
    for segment in &route.segments {
        let mid_lat = (segment.start.lat + segment.end.lat) / 2.0 * pi / 180.0;
        let tile_meters = 40075016.686 * js_max(0.05, mid_lat.cos()) / pow2(zoom);
        let step_meters = js_max(50.0, js_min(route.corridor_meters, tile_meters / 2.0));
        let steps = js_max(1.0, (segment.length_meters / step_meters).ceil());
        let mut lon_delta = segment.end.lon - segment.start.lon;
        if lon_delta > 180.0 {
            lon_delta -= 360.0;
        }
        if lon_delta < -180.0 {
            lon_delta += 360.0;
        }
        let mut step = 0.0;
        while step <= steps {
            let ratio = step / steps;
            let lat = segment.start.lat + (segment.end.lat - segment.start.lat) * ratio;
            let mut lon = segment.start.lon + lon_delta * ratio;
            if lon > 180.0 {
                lon -= 360.0;
            }
            if lon < -180.0 {
                lon += 360.0;
            }
            let boxes = boxes_for_radius_e7(
                lat_to_e7_f64(lat).unwrap_or(0),
                lon_to_e7_f64(lon).unwrap_or(0),
                route.corridor_meters,
            );
            if !add_boxes(&boxes, &mut cells) {
                return None;
            }
            step += 1.0;
        }
    }
    Some(cells)
}

/// `decodeGeoCellBlock(buffer)`.
pub fn decode_geo_cell_block(bytes: &[u8]) -> Result<GeoCellBlock> {
    assert_magic(bytes, &GEO_CELL_BLOCK_MAGIC, "Unsupported Rangefind geo category-cell block")?;
    let mut reader = Reader::at(bytes, GEO_CELL_BLOCK_MAGIC.len());
    let version = reader.varint();
    if version != GEO_CELL_BLOCK_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind geo category-cell block version {version}")));
    }
    let block_zoom = reader.varint();
    let block_x = reader.varint();
    let block_y = reader.varint();
    let count = reader.varint();
    let mut routes: Vec<GeoCellRoute> = Vec::new();
    let mut by_key: HashMap<String, usize> = HashMap::new();
    for _ in 0..count {
        let zoom = reader.varint();
        let x = reader.varint();
        let y = reader.varint();
        let code = reader.varint();
        let member_count = reader.varint();
        let mut members: Vec<GeoCellMember> = Vec::new();
        let mut member_index: HashMap<u64, usize> = HashMap::new();
        let mut previous_leaf = 0u64;
        for _ in 0..member_count {
            previous_leaf = previous_leaf.wrapping_add(reader.varint());
            let ordinal_count = reader.varint();
            if ordinal_count > u32::MAX as u64 - 1 {
                return Err(Error::new("Invalid array length"));
            }
            let mut ordinals = Vec::with_capacity((ordinal_count as usize).min(bytes.len()));
            let mut previous_ordinal = 0u64;
            for _ in 0..ordinal_count {
                previous_ordinal = previous_ordinal.wrapping_add(reader.varint());
                ordinals.push(previous_ordinal);
            }
            match member_index.get(&previous_leaf) {
                Some(&position) => members[position].ordinals = ordinals,
                None => {
                    member_index.insert(previous_leaf, members.len());
                    members.push(GeoCellMember { leaf: previous_leaf, ordinals });
                }
            }
        }
        let key = geo_cell_route_key(zoom, x, y, code);
        match by_key.get(&key) {
            Some(&position) => routes[position].members = members,
            None => {
                by_key.insert(key.clone(), routes.len());
                routes.push(GeoCellRoute { key, members });
            }
        }
    }
    if reader.pos != bytes.len() {
        return Err(Error::new("Rangefind geo category-cell block has trailing bytes."));
    }
    Ok(GeoCellBlock { format: GEO_CATEGORY_CELL_FORMAT, block_zoom, block_x, block_y, routes, by_key })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uri_component_matches_js() {
        assert_eq!(encode_uri_component("a b|c/é😀-_.!~*'()"), "a%20b%7Cc%2F%C3%A9%F0%9F%98%80-_.!~*'()");
    }

    #[test]
    fn pow2_is_exact() {
        for exponent in 0..40 {
            assert_eq!(pow2(exponent), 2f64.powi(exponent as i32));
        }
    }
}
