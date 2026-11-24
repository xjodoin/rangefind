//! Geo point tree: E7 coordinate math and the `rfgeotreeroot-v1`,
//! `rfgeobranchpage-v1` and `rfgeoleafpage-v1`/`-v2` decoders. Mirrors
//! `src/geo_tree.js` (query side only).

use std::rc::Rc;

use serde_json::{Map, Value};

use super::{decode_utf8, js_max, js_min, js_round, read_utf8};
use crate::error::{Error, Result};
use crate::pointer::{Checksum, Pointer};
use crate::util::bytes::{assert_magic, read_fixed_int, Reader};
use crate::util::json::{num, to_number};

pub const GEO_TREE_ROOT_FORMAT: &str = "rfgeotreeroot-v1";
pub const GEO_BRANCH_PAGE_FORMAT: &str = "rfgeobranchpage-v1";
pub const GEO_LEAF_PAGE_FORMAT: &str = "rfgeoleafpage-v1";
pub const GEO_LEAF_CAPSULE_PAGE_FORMAT: &str = "rfgeoleafpage-v2";

pub const GEO_TREE_ROOT_MAGIC: [u8; 4] = [0x52, 0x46, 0x47, 0x52]; // RFGR
pub const GEO_BRANCH_PAGE_MAGIC: [u8; 4] = [0x52, 0x46, 0x47, 0x42]; // RFGB
pub const GEO_LEAF_PAGE_MAGIC: [u8; 4] = [0x52, 0x46, 0x47, 0x4c]; // RFGL
const DOC_PAGE_PAYLOAD_MAGIC: [u8; 4] = [0x52, 0x46, 0x50, 0x43]; // RFPC

const FORMAT_VERSION: u64 = 1;
const GEO_LEAF_CAPSULE_VERSION: u64 = 2;

pub const LAT_E7_MIN: i64 = -900_000_000;
pub const LAT_E7_MAX: i64 = 900_000_000;
pub const LON_E7_MIN: i64 = -1_800_000_000;
pub const LON_E7_MAX: i64 = 1_800_000_000;

pub const EARTH_RADIUS_METERS: f64 = 6371008.7714;
const E7: f64 = 1e7;
/// `Math.PI / (180 * E7)`
pub const RADIANS_PER_E7: f64 = std::f64::consts::PI / (180.0 * E7);

const LAT_MIN_F: f64 = LAT_E7_MIN as f64;
const LAT_MAX_F: f64 = LAT_E7_MAX as f64;
const LON_MIN_F: f64 = LON_E7_MIN as f64;
const LON_MAX_F: f64 = LON_E7_MAX as f64;

// ------------------------------------------------------------------ types

/// A lat/lon-aligned box in E7 units (`{minLatE7, maxLatE7, minLonE7,
/// maxLonE7}`). Longitude may wrap (`min_lon_e7 > max_lon_e7`) only where
/// the JS accepts it (`pointToBoxDistanceMetersE7`,
/// `pointToBoxMaxDistanceMetersE7`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct GeoBox {
    pub min_lat_e7: i64,
    pub max_lat_e7: i64,
    pub min_lon_e7: i64,
    pub max_lon_e7: i64,
}

impl GeoBox {
    pub const fn new(min_lat_e7: i64, max_lat_e7: i64, min_lon_e7: i64, max_lon_e7: i64) -> Self {
        GeoBox { min_lat_e7, max_lat_e7, min_lon_e7, max_lon_e7 }
    }
}

/// How a block filter's summary is laid out (`filter.kind`). Every kind
/// other than `facet` and `number` reads as a varint `min`/`max` pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockFilterKind {
    Facet,
    Number,
    Other,
}

/// One `manifest.block_filters` entry, as far as summary decoding needs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockFilter {
    pub name: String,
    pub kind: BlockFilterKind,
    /// Facet bitset words (`filter.words`); ignored for other kinds.
    pub words: usize,
}

impl BlockFilter {
    /// Reads `{name, kind, words}` from a manifest JSON entry.
    pub fn from_manifest(value: &Value) -> Self {
        let name = match value.get("name") {
            Some(Value::String(text)) => text.clone(),
            Some(Value::Null) | None => "undefined".to_string(),
            Some(other) => other.to_string(),
        };
        let kind = match value.get("kind").and_then(Value::as_str) {
            Some("facet") => BlockFilterKind::Facet,
            Some("number") => BlockFilterKind::Number,
            _ => BlockFilterKind::Other,
        };
        let words = value.get("words").map(to_number).filter(|n| n.is_finite() && *n > 0.0).unwrap_or(0.0) as usize;
        BlockFilter { name, kind, words }
    }

    /// Reads a whole `manifest.block_filters` array (missing → empty).
    pub fn list_from_manifest(value: &Value) -> Vec<BlockFilter> {
        value.as_array().map(|items| items.iter().map(BlockFilter::from_manifest).collect()).unwrap_or_default()
    }
}

/// One decoded block-filter summary (`readBlockFilterSummary` values).
#[derive(Clone, Debug, PartialEq)]
pub enum BlockFilterSummary {
    /// `{ words }`
    Facet { words: Vec<u64> },
    /// `{ min, max }` doubles, both `null` when the block had no values.
    Number { min: Option<f64>, max: Option<f64> },
    /// `{ min, max }` varints (boolean and other kinds).
    Range { min: u64, max: u64 },
}

/// `filters` object: `(filter name, summary)` in manifest order (a repeated
/// name overwrites in place, like the JS object).
pub type FilterSummaries = Vec<(String, BlockFilterSummary)>;

/// A leaf entry from a root (single-level) or branch page.
#[derive(Clone, Debug, PartialEq)]
pub struct GeoLeafEntry {
    /// Global leaf index.
    pub index: usize,
    pub bbox: GeoBox,
    pub count: u64,
    /// `{pack, offset, length, logicalLength, checksum}`. `pack` is `""` when
    /// the pack index is outside the pack table (JS: `undefined`).
    pub pointer: Pointer,
    pub physical_length: u64,
    pub filters: Option<FilterSummaries>,
}

/// A branch entry from a two-level root.
#[derive(Clone, Debug, PartialEq)]
pub struct GeoBranchEntry {
    pub index: usize,
    pub bbox: GeoBox,
    pub count: u64,
    pub first_leaf_index: u64,
    pub leaf_count: u64,
    pub pointer: Pointer,
    pub physical_length: u64,
    pub filters: Option<FilterSummaries>,
}

/// `parseGeoTreeRoot` result.
#[derive(Clone, Debug, PartialEq)]
pub struct GeoTreeRoot {
    pub format: &'static str,
    pub page_format: &'static str,
    pub field: String,
    pub total: u64,
    pub leaf_size: u64,
    pub leaf_count: u64,
    pub levels: u8,
    pub has_summaries: bool,
    pub bbox: GeoBox,
    pub pack_table: Vec<Rc<str>>,
    /// Set when `levels == 1`.
    pub leaves: Option<Vec<GeoLeafEntry>>,
    /// Set when `levels == 2`.
    pub branches: Option<Vec<GeoBranchEntry>>,
}

/// `decodeGeoBranchPage` result.
#[derive(Clone, Debug, PartialEq)]
pub struct GeoBranchPage {
    pub field: String,
    pub branch_index: u64,
    pub first_leaf_index: u64,
    pub bbox: GeoBox,
    pub leaves: Vec<GeoLeafEntry>,
}

/// `decodeGeoLeafPage` result.
#[derive(Clone, Debug, PartialEq)]
pub struct GeoLeafPage {
    pub field: String,
    pub count: usize,
    pub bbox: GeoBox,
    pub lats_e7: Vec<i32>,
    pub lons_e7: Vec<i32>,
    pub docs: Vec<u32>,
    pub capsule_fields: Vec<String>,
    /// v2 pages: one display row per point. Keys follow JS object order
    /// (array-index keys ascending first, then insertion order); `null`
    /// values are omitted.
    pub capsules: Option<Vec<Map<String, Value>>>,
}

// ------------------------------------------------------------ coordinates

/// `latToE7`: `null`/`undefined`/`""` and anything outside ±90° → `None`.
pub fn lat_to_e7(value: &Value) -> Option<i64> {
    match value {
        Value::Null => None,
        Value::String(text) if text.is_empty() => None,
        other => lat_to_e7_f64(to_number(other)),
    }
}

/// `latToE7` for a number argument.
pub fn lat_to_e7_f64(lat: f64) -> Option<i64> {
    let value = js_round(lat * E7);
    if !value.is_finite() || value < LAT_MIN_F || value > LAT_MAX_F {
        return None;
    }
    Some(value as i64)
}

/// `lonToE7`: like [`lat_to_e7`] for ±180°, mapping +180° to −180°.
pub fn lon_to_e7(value: &Value) -> Option<i64> {
    match value {
        Value::Null => None,
        Value::String(text) if text.is_empty() => None,
        other => lon_to_e7_f64(to_number(other)),
    }
}

/// `lonToE7` for a number argument.
pub fn lon_to_e7_f64(lon: f64) -> Option<i64> {
    let value = js_round(lon * E7);
    if !value.is_finite() || value < LON_MIN_F || value > LON_MAX_F {
        return None;
    }
    let value = value as i64;
    Some(if value == LON_E7_MAX { LON_E7_MIN } else { value })
}

pub fn e7_to_degrees(value: f64) -> f64 {
    value / E7
}

// ------------------------------------------------------------- distances

/// Great-circle distance in meters between two degree coordinates.
pub fn haversine_meters(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let deg = std::f64::consts::PI / 180.0;
    let phi1 = lat1 * deg;
    let phi2 = lat2 * deg;
    let d_phi = phi2 - phi1;
    let d_lambda = (lon2 - lon1) * deg;
    let sin_phi = (d_phi / 2.0).sin();
    let sin_lambda = (d_lambda / 2.0).sin();
    let a = sin_phi * sin_phi + phi1.cos() * phi2.cos() * sin_lambda * sin_lambda;
    2.0 * EARTH_RADIUS_METERS * js_min(1.0, a.sqrt()).asin()
}

/// Great-circle distance in meters between two E7 coordinates. Takes
/// doubles because the box-distance helpers pass fractional latitudes.
pub fn haversine_meters_e7(lat_e7_a: f64, lon_e7_a: f64, lat_e7_b: f64, lon_e7_b: f64) -> f64 {
    let phi1 = lat_e7_a * RADIANS_PER_E7;
    let phi2 = lat_e7_b * RADIANS_PER_E7;
    let d_phi = (lat_e7_b - lat_e7_a) * RADIANS_PER_E7;
    let d_lambda = (lon_e7_b - lon_e7_a) * RADIANS_PER_E7;
    let sin_phi = (d_phi / 2.0).sin();
    let sin_lambda = (d_lambda / 2.0).sin();
    let a = sin_phi * sin_phi + phi1.cos() * phi2.cos() * sin_lambda * sin_lambda;
    2.0 * EARTH_RADIUS_METERS * js_min(1.0, a.sqrt()).asin()
}

fn lon_delta_e7(a: f64, b: f64) -> f64 {
    let mut delta = (a - b).abs();
    if delta > LON_MAX_F {
        delta = 2.0 * LON_MAX_F - delta;
    }
    delta
}

fn lon_in_range(b: &GeoBox, lon: f64) -> bool {
    let (min, max) = (b.min_lon_e7 as f64, b.max_lon_e7 as f64);
    if min <= max {
        lon >= min && lon <= max
    } else {
        lon >= min || lon <= max
    }
}

/// Exact minimum great-circle distance from a point to a lat/lon box.
pub fn point_to_box_distance_meters_e7(lat_e7: i64, lon_e7: i64, b: &GeoBox) -> f64 {
    let (lat, lon) = (lat_e7 as f64, lon_e7 as f64);
    let (min_lat, max_lat) = (b.min_lat_e7 as f64, b.max_lat_e7 as f64);
    let in_lon = lon_in_range(b, lon);
    if in_lon && lat >= min_lat && lat <= max_lat {
        return 0.0;
    }
    if in_lon {
        let edge_lat = if lat < min_lat { min_lat } else { max_lat };
        return haversine_meters_e7(lat, lon, edge_lat, lon);
    }
    let near_lon = if lon_delta_e7(lon, b.min_lon_e7 as f64) <= lon_delta_e7(lon, b.max_lon_e7 as f64) {
        b.min_lon_e7 as f64
    } else {
        b.max_lon_e7 as f64
    };
    let mut best = js_min(
        haversine_meters_e7(lat, lon, min_lat, near_lon),
        haversine_meters_e7(lat, lon, max_lat, near_lon),
    );
    let d_lambda = lon_delta_e7(lon, near_lon) * RADIANS_PER_E7;
    if d_lambda.cos() > 0.0 {
        let phi = lat * RADIANS_PER_E7;
        let meridian_lat = (phi.tan() / d_lambda.cos()).atan() / RADIANS_PER_E7;
        let clamped_lat = js_max(min_lat, js_min(max_lat, meridian_lat));
        best = js_min(best, haversine_meters_e7(lat, lon, clamped_lat, near_lon));
    }
    best
}

/// Expands a radius query into one or two E7 boxes (two when the circle
/// crosses the antimeridian). `radius_meters` is the already-`Number()`ed
/// argument; NaN and non-positive radii act as 0.
pub fn boxes_for_radius_e7(lat_e7: i64, lon_e7: i64, radius_meters: f64) -> Vec<GeoBox> {
    let radius = js_max(0.0, if radius_meters.is_nan() || radius_meters == 0.0 { 0.0 } else { radius_meters });
    let lat = lat_e7 as f64;
    let lon = lon_e7 as f64;
    let lat_delta_e7 = ((radius / EARTH_RADIUS_METERS) / RADIANS_PER_E7).ceil();
    let min_lat_e7 = js_max(LAT_MIN_F, lat - lat_delta_e7);
    let max_lat_e7 = js_min(LAT_MAX_F, lat + lat_delta_e7);
    let sin_ratio = js_min(std::f64::consts::PI / 2.0, radius / EARTH_RADIUS_METERS).sin();
    let cos_lat = (lat * RADIANS_PER_E7).cos();
    let make = |min_lon: f64, max_lon: f64| GeoBox {
        min_lat_e7: min_lat_e7 as i64,
        max_lat_e7: max_lat_e7 as i64,
        min_lon_e7: min_lon as i64,
        max_lon_e7: max_lon as i64,
    };
    if lat - lat_delta_e7 < LAT_MIN_F || lat + lat_delta_e7 > LAT_MAX_F || sin_ratio >= cos_lat {
        return vec![make(LON_MIN_F, LON_MAX_F)];
    }
    let lon_delta = ((sin_ratio / cos_lat).asin() / RADIANS_PER_E7).ceil();
    let min_lon_e7 = lon - lon_delta;
    let max_lon_e7 = lon + lon_delta;
    if min_lon_e7 < LON_MIN_F && max_lon_e7 > LON_MAX_F {
        return vec![make(LON_MIN_F, LON_MAX_F)];
    }
    if min_lon_e7 < LON_MIN_F {
        return vec![make(LON_MIN_F, max_lon_e7), make(min_lon_e7 + 2.0 * LON_MAX_F, LON_MAX_F)];
    }
    if max_lon_e7 > LON_MAX_F {
        return vec![make(min_lon_e7, LON_MAX_F), make(LON_MIN_F, max_lon_e7 - 2.0 * LON_MAX_F)];
    }
    vec![make(min_lon_e7, max_lon_e7)]
}

pub fn box_intersects_e7(a: &GeoBox, b: &GeoBox) -> bool {
    if a.max_lat_e7 < b.min_lat_e7 || a.min_lat_e7 > b.max_lat_e7 {
        return false;
    }
    a.max_lon_e7 >= b.min_lon_e7 && a.min_lon_e7 <= b.max_lon_e7
}

pub fn box_contains_box_e7(outer: &GeoBox, inner: &GeoBox) -> bool {
    inner.min_lat_e7 >= outer.min_lat_e7
        && inner.max_lat_e7 <= outer.max_lat_e7
        && inner.min_lon_e7 >= outer.min_lon_e7
        && inner.max_lon_e7 <= outer.max_lon_e7
}

pub fn box_contains_point_e7(b: &GeoBox, lat_e7: i64, lon_e7: i64) -> bool {
    lat_e7 >= b.min_lat_e7 && lat_e7 <= b.max_lat_e7 && lon_e7 >= b.min_lon_e7 && lon_e7 <= b.max_lon_e7
}

/// Farthest distance from a point to anywhere inside a box.
pub fn point_to_box_max_distance_meters_e7(lat_e7: i64, lon_e7: i64, b: &GeoBox) -> f64 {
    let (lat, lon) = (lat_e7 as f64, lon_e7 as f64);
    let (min_lat, max_lat) = (b.min_lat_e7 as f64, b.max_lat_e7 as f64);
    let mut lon_candidates = vec![b.min_lon_e7 as f64, b.max_lon_e7 as f64];
    let mut antipode_lon = lon + 1_800_000_000.0;
    if antipode_lon >= LON_MAX_F {
        antipode_lon -= 2.0 * LON_MAX_F;
    }
    if lon_in_range(b, antipode_lon) {
        lon_candidates.push(antipode_lon);
    }
    let mut max = 0.0;
    for candidate_lon in lon_candidates {
        let d_lambda = lon_delta_e7(lon, candidate_lon) * RADIANS_PER_E7;
        let mut lat_candidates = vec![min_lat, max_lat];
        if d_lambda.cos() < 0.0 {
            let critical = ((lat * RADIANS_PER_E7).tan() / d_lambda.cos()).atan() / RADIANS_PER_E7;
            if critical >= min_lat && critical <= max_lat {
                lat_candidates.push(critical);
            }
        }
        for candidate_lat in lat_candidates {
            let distance = haversine_meters_e7(lat, lon, candidate_lat, candidate_lon);
            if distance > max {
                max = distance;
            }
        }
    }
    max
}

// --------------------------------------------------------------- decoders

fn read_zigzag(reader: &mut Reader<'_>) -> i64 {
    let raw = reader.varint();
    if raw % 2 == 0 {
        (raw / 2) as i64
    } else {
        -(((raw / 2) + 1) as i64)
    }
}

/// `readFloat64` through a `DataView`: out-of-bounds reads throw.
fn read_f64(reader: &mut Reader<'_>) -> Result<f64> {
    let pos = reader.pos;
    let len = reader.bytes.len();
    if pos > len {
        return Err(Error::new(format!("Start offset {pos} is outside the bounds of the buffer")));
    }
    if pos + 8 > len {
        return Err(Error::new("Invalid DataView length 8"));
    }
    Ok(reader.f64_le())
}

/// `readBlockFilterSummary`.
fn read_filter_summary(reader: &mut Reader<'_>, filters: Option<&[BlockFilter]>) -> Result<Option<FilterSummaries>> {
    let Some(filters) = filters else { return Ok(None) };
    let mut out: FilterSummaries = Vec::with_capacity(filters.len());
    for filter in filters {
        let summary = match filter.kind {
            BlockFilterKind::Facet => {
                let mut words = Vec::with_capacity(filter.words.min(reader.bytes.len() + 1));
                for _ in 0..filter.words {
                    words.push(reader.varint());
                }
                BlockFilterSummary::Facet { words }
            }
            BlockFilterKind::Number => {
                if reader.u8() == 1 {
                    let min = read_f64(reader)?;
                    let max = read_f64(reader)?;
                    BlockFilterSummary::Number { min: Some(min), max: Some(max) }
                } else {
                    BlockFilterSummary::Number { min: None, max: None }
                }
            }
            BlockFilterKind::Other => {
                let min = reader.varint();
                let max = reader.varint();
                BlockFilterSummary::Range { min, max }
            }
        };
        match out.iter_mut().find(|(name, _)| *name == filter.name) {
            Some(slot) => slot.1 = summary,
            None => out.push((filter.name.clone(), summary)),
        }
    }
    Ok(Some(out))
}

fn read_object_pointer(reader: &mut Reader<'_>, pack_table: &[Rc<str>]) -> (Pointer, u64) {
    let pack_index = reader.varint();
    let pack = usize::try_from(pack_index)
        .ok()
        .and_then(|index| pack_table.get(index).cloned())
        .unwrap_or_else(|| Rc::from(""));
    let offset = reader.varint();
    let length = reader.varint();
    let physical_length = reader.varint();
    let logical_length = Some(reader.varint()).filter(|value| *value != 0);
    let algorithm = read_utf8(reader);
    let value = read_utf8(reader);
    let checksum = if value.is_empty() {
        None
    } else {
        Some(Checksum {
            algorithm: Rc::from(if algorithm.is_empty() { "sha256" } else { algorithm.as_str() }),
            value: Rc::from(value.as_str()),
        })
    };
    (Pointer { pack, offset, length, logical_length, checksum, checksum_row: None }, physical_length)
}

fn read_leaf_entry(
    reader: &mut Reader<'_>,
    base: &GeoBox,
    pack_table: &[Rc<str>],
    index: usize,
    filters: Option<&[BlockFilter]>,
) -> Result<GeoLeafEntry> {
    let min_lat_e7 = base.min_lat_e7 + reader.varint() as i64;
    let max_lat_e7 = min_lat_e7 + reader.varint() as i64;
    let min_lon_e7 = base.min_lon_e7 + reader.varint() as i64;
    let max_lon_e7 = min_lon_e7 + reader.varint() as i64;
    let count = reader.varint();
    let (pointer, physical_length) = read_object_pointer(reader, pack_table);
    let filters = read_filter_summary(reader, filters)?;
    Ok(GeoLeafEntry {
        index,
        bbox: GeoBox { min_lat_e7, max_lat_e7, min_lon_e7, max_lon_e7 },
        count,
        pointer,
        physical_length,
        filters,
    })
}

fn read_bbox(reader: &mut Reader<'_>) -> GeoBox {
    let min_lat_e7 = read_zigzag(reader);
    let max_lat_e7 = min_lat_e7 + reader.varint() as i64;
    let min_lon_e7 = read_zigzag(reader);
    let max_lon_e7 = min_lon_e7 + reader.varint() as i64;
    GeoBox { min_lat_e7, max_lat_e7, min_lon_e7, max_lon_e7 }
}

/// `new Array(count)` guard plus an early exit once the cursor has run past
/// the end (the trailing-bytes check is then certain to fail).
fn check_array_length(count: u64) -> Result<usize> {
    if count > u32::MAX as u64 - 1 {
        return Err(Error::new("Invalid array length"));
    }
    Ok(count as usize)
}

/// `parseGeoTreeRoot(buffer, blockFilters)`.
pub fn parse_geo_tree_root(bytes: &[u8], block_filters: &[BlockFilter]) -> Result<GeoTreeRoot> {
    const TRAILING: &str = "Rangefind geo tree root has trailing bytes.";
    assert_magic(bytes, &GEO_TREE_ROOT_MAGIC, "Unsupported Rangefind geo tree root")?;
    let mut reader = Reader::at(bytes, GEO_TREE_ROOT_MAGIC.len());
    let version = reader.varint();
    if version != FORMAT_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind geo tree root version {version}")));
    }
    let field = read_utf8(&mut reader);
    let total = reader.varint();
    let leaf_size = reader.varint();
    let leaf_count = reader.varint();
    let bbox = read_bbox(&mut reader);
    let pack_count = check_array_length(reader.varint())?;
    let mut pack_table: Vec<Rc<str>> = Vec::with_capacity(pack_count.min(bytes.len()));
    for _ in 0..pack_count {
        if reader.pos > bytes.len() {
            return Err(Error::new(TRAILING));
        }
        pack_table.push(Rc::from(read_utf8(&mut reader).as_str()));
    }
    let levels = bytes.get(reader.pos).copied();
    reader.pos += 1;
    let has_summaries = bytes.get(reader.pos).copied() == Some(1);
    reader.pos += 1;
    if has_summaries && block_filters.is_empty() {
        return Err(Error::new(
            "Rangefind geo tree root has filter summaries but no block_filters manifest was provided.",
        ));
    }
    let summary_filters = if has_summaries { Some(block_filters) } else { None };
    let mut root = GeoTreeRoot {
        format: GEO_TREE_ROOT_FORMAT,
        page_format: GEO_LEAF_PAGE_FORMAT,
        field,
        total,
        leaf_size,
        leaf_count,
        levels: levels.unwrap_or(0),
        has_summaries,
        bbox,
        pack_table,
        leaves: None,
        branches: None,
    };
    match levels {
        Some(1) => {
            let count = check_array_length(reader.varint())?;
            let mut leaves = Vec::with_capacity(count.min(bytes.len()));
            for i in 0..count {
                if reader.pos > bytes.len() {
                    return Err(Error::new(TRAILING));
                }
                leaves.push(read_leaf_entry(&mut reader, &bbox, &root.pack_table, i, summary_filters)?);
            }
            root.leaves = Some(leaves);
        }
        Some(2) => {
            let count = check_array_length(reader.varint())?;
            let mut branches = Vec::with_capacity(count.min(bytes.len()));
            for i in 0..count {
                if reader.pos > bytes.len() {
                    return Err(Error::new(TRAILING));
                }
                let branch_min_lat = bbox.min_lat_e7 + reader.varint() as i64;
                let branch_max_lat = branch_min_lat + reader.varint() as i64;
                let branch_min_lon = bbox.min_lon_e7 + reader.varint() as i64;
                let branch_max_lon = branch_min_lon + reader.varint() as i64;
                let point_count = reader.varint();
                let first_leaf_index = reader.varint();
                let branch_leaf_count = reader.varint();
                let (pointer, physical_length) = read_object_pointer(&mut reader, &root.pack_table);
                let filters = read_filter_summary(&mut reader, summary_filters)?;
                branches.push(GeoBranchEntry {
                    index: i,
                    bbox: GeoBox::new(branch_min_lat, branch_max_lat, branch_min_lon, branch_max_lon),
                    count: point_count,
                    first_leaf_index,
                    leaf_count: branch_leaf_count,
                    pointer,
                    physical_length,
                    filters,
                });
            }
            root.branches = Some(branches);
        }
        Some(other) => return Err(Error::new(format!("Unsupported Rangefind geo tree root levels {other}"))),
        None => return Err(Error::new("Unsupported Rangefind geo tree root levels undefined")),
    }
    if reader.pos != bytes.len() {
        return Err(Error::new(TRAILING));
    }
    Ok(root)
}

/// `decodeGeoBranchPage(buffer, packTable, blockFilters, { name })`. An
/// empty or absent `expected_name` skips the field check, like the JS.
pub fn decode_geo_branch_page(
    bytes: &[u8],
    pack_table: &[Rc<str>],
    block_filters: &[BlockFilter],
    expected_name: Option<&str>,
) -> Result<GeoBranchPage> {
    const TRAILING: &str = "Rangefind geo branch page has trailing bytes.";
    assert_magic(bytes, &GEO_BRANCH_PAGE_MAGIC, "Unsupported Rangefind geo branch page")?;
    let mut reader = Reader::at(bytes, GEO_BRANCH_PAGE_MAGIC.len());
    let version = reader.varint();
    if version != FORMAT_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind geo branch page version {version}")));
    }
    let field = read_utf8(&mut reader);
    if let Some(expected) = expected_name.filter(|name| !name.is_empty()) {
        if expected != field {
            return Err(Error::new(format!("Rangefind geo branch page field mismatch: {field}")));
        }
    }
    let branch_index = reader.varint();
    let first_leaf_index = reader.varint();
    let bbox = read_bbox(&mut reader);
    let has_summaries = bytes.get(reader.pos).copied() == Some(1);
    reader.pos += 1;
    if has_summaries && block_filters.is_empty() {
        return Err(Error::new(
            "Rangefind geo branch page has filter summaries but no block_filters manifest was provided.",
        ));
    }
    let summary_filters = if has_summaries { Some(block_filters) } else { None };
    let count = check_array_length(reader.varint())?;
    let mut leaves = Vec::with_capacity(count.min(bytes.len()));
    for i in 0..count {
        if reader.pos > bytes.len() {
            return Err(Error::new(TRAILING));
        }
        let index = (first_leaf_index as usize).wrapping_add(i);
        leaves.push(read_leaf_entry(&mut reader, &bbox, pack_table, index, summary_filters)?);
    }
    if reader.pos != bytes.len() {
        return Err(Error::new(TRAILING));
    }
    Ok(GeoBranchPage { field, branch_index, first_leaf_index, bbox, leaves })
}

/// `decodeGeoLeafPage(buffer, { name })`.
pub fn decode_geo_leaf_page(bytes: &[u8], expected_name: Option<&str>) -> Result<GeoLeafPage> {
    const TRAILING: &str = "Rangefind geo leaf page has trailing bytes.";
    assert_magic(bytes, &GEO_LEAF_PAGE_MAGIC, "Unsupported Rangefind geo leaf page")?;
    let mut reader = Reader::at(bytes, GEO_LEAF_PAGE_MAGIC.len());
    let version = reader.varint();
    if version != FORMAT_VERSION && version != GEO_LEAF_CAPSULE_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind geo leaf page version {version}")));
    }
    let field = read_utf8(&mut reader);
    if let Some(expected) = expected_name.filter(|name| !name.is_empty()) {
        if expected != field {
            return Err(Error::new(format!("Rangefind geo leaf page field mismatch: {field}")));
        }
    }
    let count_raw = reader.varint();
    let bbox = read_bbox(&mut reader);
    let mut width = || {
        let value = bytes.get(reader.pos).copied();
        reader.pos += 1;
        value
    };
    let lat_width = width();
    let lon_width = width();
    let doc_width = width();
    let mut capsule_fields = Vec::new();
    if version >= GEO_LEAF_CAPSULE_VERSION {
        let field_count = check_array_length(reader.varint())?;
        for _ in 0..field_count {
            if reader.pos > bytes.len() {
                break;
            }
            capsule_fields.push(read_utf8(&mut reader));
        }
    }
    // Typed-array allocation limit, then the body walk. A missing width byte
    // makes every later JS position NaN: the v1 trailing check fails and a
    // v2 page hands the whole buffer to the capsule decoder.
    if count_raw > (u32::MAX as u64) {
        return Err(Error::new(format!("Invalid typed array length: {count_raw}")));
    }
    let count = count_raw as usize;
    let (Some(lat_width), Some(lon_width), Some(doc_width)) = (lat_width, lon_width, doc_width) else {
        if version >= GEO_LEAF_CAPSULE_VERSION {
            decode_doc_page_columns(bytes, &capsule_fields)?;
        }
        return Err(Error::new(TRAILING));
    };
    let (lat_width, lon_width, doc_width) = (lat_width as usize, lon_width as usize, doc_width as usize);
    let body_end = reader.pos as u128 + count as u128 * (lat_width + lon_width + doc_width) as u128;
    if body_end > bytes.len() as u128 {
        if version >= GEO_LEAF_CAPSULE_VERSION {
            decode_doc_page_columns(&[], &capsule_fields)?;
        }
        return Err(Error::new(TRAILING));
    }
    let mut pos = reader.pos;
    let mut lats_e7 = Vec::with_capacity(count);
    let mut lons_e7 = Vec::with_capacity(count);
    let mut docs = Vec::with_capacity(count);
    for _ in 0..count {
        lats_e7.push((bbox.min_lat_e7 + read_fixed_int(bytes, pos, lat_width) as i64) as i32);
        pos += lat_width;
    }
    for _ in 0..count {
        lons_e7.push((bbox.min_lon_e7 + read_fixed_int(bytes, pos, lon_width) as i64) as i32);
        pos += lon_width;
    }
    for _ in 0..count {
        docs.push(read_fixed_int(bytes, pos, doc_width) as u32);
        pos += doc_width;
    }
    let mut capsules = None;
    if version >= GEO_LEAF_CAPSULE_VERSION {
        let mut rows = decode_doc_page_columns(&bytes[pos..], &capsule_fields)?;
        if rows.len() != count {
            return Err(Error::new(format!(
                "Rangefind geo leaf capsule row count {} does not match point count {count}.",
                rows.len()
            )));
        }
        for row in &mut rows {
            row.shift_remove("index");
        }
        capsules = Some(rows);
        pos = bytes.len();
    }
    if pos != bytes.len() {
        return Err(Error::new(TRAILING));
    }
    Ok(GeoLeafPage { field, count, bbox, lats_e7, lons_e7, docs, capsule_fields, capsules })
}

// ---------------------------------------------------- capsule doc columns

const VALUE_NULL: u8 = 0;
const VALUE_FALSE: u8 = 1;
const VALUE_TRUE: u8 = 2;
const VALUE_NUMBER: u8 = 3;
const VALUE_STRING: u8 = 4;
const VALUE_STRING_ARRAY: u8 = 5;
const VALUE_JSON: u8 = 6;

fn doc_read_bytes<'a>(reader: &mut Reader<'a>) -> Result<&'a [u8]> {
    let length = reader.varint();
    let start = reader.pos;
    let end = (start as u128) + length as u128;
    if end > reader.bytes.len() as u128 {
        return Err(Error::new("Rangefind doc page payload ended inside a value."));
    }
    reader.pos = end as usize;
    Ok(&reader.bytes[start..reader.pos])
}

fn doc_decode_value(reader: &mut Reader<'_>) -> Result<Value> {
    let kind = reader.bytes.get(reader.pos).copied();
    reader.pos += 1;
    match kind {
        Some(VALUE_NULL) => Ok(Value::Null),
        Some(VALUE_FALSE) => Ok(Value::Bool(false)),
        Some(VALUE_TRUE) => Ok(Value::Bool(true)),
        Some(VALUE_NUMBER) => {
            if reader.pos + 8 > reader.bytes.len() {
                return Err(Error::new("Rangefind doc page payload ended inside a number."));
            }
            Ok(num(reader.f64_le()))
        }
        Some(VALUE_STRING) => Ok(Value::String(decode_utf8(doc_read_bytes(reader)?))),
        Some(VALUE_STRING_ARRAY) => {
            let count = reader.varint();
            if count > reader.bytes.len().saturating_sub(reader.pos) as u64 {
                // Every element needs at least its length byte.
                return Err(Error::new("Rangefind doc page payload ended inside a value."));
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push(Value::String(decode_utf8(doc_read_bytes(reader)?)));
            }
            Ok(Value::Array(items))
        }
        Some(VALUE_JSON) => {
            let text = decode_utf8(doc_read_bytes(reader)?);
            let parsed: Value = serde_json::from_str(&text)
                .map_err(|error| Error::new(format!("Rangefind doc page JSON value is invalid: {error}")))?;
            Ok(js_json_value(parsed))
        }
        Some(other) => Err(Error::new(format!("Unsupported Rangefind doc page value type {other}."))),
        None => Err(Error::new("Unsupported Rangefind doc page value type undefined.")),
    }
}

/// `decodeDocPageColumns(buffer, fields, 0)` (rows keep their `index` key;
/// the caller removes it).
fn decode_doc_page_columns(bytes: &[u8], fields: &[String]) -> Result<Vec<Map<String, Value>>> {
    assert_magic(bytes, &DOC_PAGE_PAYLOAD_MAGIC, "Unsupported Rangefind doc page payload")?;
    let mut reader = Reader::at(bytes, DOC_PAGE_PAYLOAD_MAGIC.len());
    let version = reader.varint();
    if version != 1 {
        return Err(Error::new(format!("Unsupported Rangefind doc page payload version {version}")));
    }
    let count = reader.varint();
    let field_count = reader.varint();
    if field_count != fields.len() as u64 {
        return Err(Error::new("Rangefind doc page field count does not match the manifest."));
    }
    let count = check_array_length(count)?;
    let new_row = |row: usize| {
        let mut map = Map::new();
        map.insert("index".to_string(), num(row as f64));
        map
    };
    let Some((first, rest)) = fields.split_first() else {
        return Ok((0..count).map(new_row).collect());
    };
    // Rows are materialized while the first column decodes: every value
    // consumes its type byte, so a corrupt count fails before it allocates.
    let mut docs: Vec<Map<String, Value>> = Vec::with_capacity(count.min(bytes.len()));
    for row in 0..count {
        let value = doc_decode_value(&mut reader)?;
        let mut doc = new_row(row);
        if !value.is_null() {
            js_object_set(&mut doc, first.clone(), value);
        }
        docs.push(doc);
    }
    for field in rest {
        for doc in docs.iter_mut() {
            let value = doc_decode_value(&mut reader)?;
            if !value.is_null() {
                js_object_set(doc, field.clone(), value);
            }
        }
    }
    if reader.pos != bytes.len() {
        return Err(Error::new("Rangefind doc page payload has trailing bytes."));
    }
    Ok(docs)
}

/// True for canonical array-index property names, which JS objects order
/// ascending before every other key.
fn is_array_index(key: &str) -> bool {
    if key.is_empty() || key.len() > 10 || !key.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    if key.len() > 1 && key.starts_with('0') {
        return false;
    }
    key.parse::<u64>().map(|n| n < u32::MAX as u64).unwrap_or(false)
}

/// `object[key] = value` with JS property order.
fn js_object_set(map: &mut Map<String, Value>, key: String, value: Value) {
    if let Some(slot) = map.get_mut(&key) {
        *slot = value;
        return;
    }
    if is_array_index(&key) {
        let n: u64 = key.parse().unwrap_or(0);
        let position = map
            .keys()
            .take_while(|existing| is_array_index(existing) && existing.parse::<u64>().unwrap_or(0) < n)
            .count();
        map.shift_insert(position, key, value);
    } else {
        map.insert(key, value);
    }
}

/// Re-shapes a parsed JSON value the way `JSON.parse` would hold it: object
/// keys in JS property order and numbers as doubles.
fn js_json_value(value: Value) -> Value {
    match value {
        Value::Number(number) => num(number.as_f64().unwrap_or(f64::NAN)),
        Value::Array(items) => Value::Array(items.into_iter().map(js_json_value).collect()),
        Value::Object(entries) => {
            let mut out = Map::new();
            for (key, item) in entries {
                js_object_set(&mut out, key, js_json_value(item));
            }
            Value::Object(out)
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn e7_rounding_matches_math_round() {
        assert_eq!(lat_to_e7(&json!(45.5017)), Some(455017000));
        assert_eq!(lat_to_e7(&json!("")), None);
        assert_eq!(lat_to_e7(&Value::Null), None);
        assert_eq!(lat_to_e7(&json!(90.00000004)), Some(LAT_E7_MAX));
        assert_eq!(lat_to_e7(&json!(90.00000005)), None);
        assert_eq!(lon_to_e7(&json!(180)), Some(LON_E7_MIN));
        assert_eq!(lat_to_e7_f64(-2.5e-7), Some(-2));
        assert_eq!(lat_to_e7_f64(2.5e-7), Some(3));
    }

    #[test]
    fn js_key_order() {
        let mut map = Map::new();
        js_object_set(&mut map, "index".into(), json!(0));
        js_object_set(&mut map, "b".into(), json!(1));
        js_object_set(&mut map, "10".into(), json!(2));
        js_object_set(&mut map, "2".into(), json!(3));
        js_object_set(&mut map, "02".into(), json!(4));
        let keys: Vec<_> = map.keys().cloned().collect();
        assert_eq!(keys, ["2", "10", "index", "b", "02"]);
    }
}
