//! Geo query subsystem: the decode and math side of the JS geo modules.
//!
//! - [`tree`] mirrors `src/geo_tree.js` (E7 coordinates, haversine, radius
//!   boxes, point/box distances, geo tree root / branch / leaf pages).
//! - [`cells`] mirrors `src/geo_cells.js` (web-mercator category cells and
//!   `rfgeocategorycell-v1` blocks).
//! - [`route`] mirrors `src/geo_route.js` (route normalization, corridor
//!   boxes, point-to-route matching).
//! - [`shard_routing`] mirrors `src/geo_shard_routing.js` (shard bbox
//!   distance and intersection).
//!
//! Floating-point code keeps the JS operation order so results are
//! bit-identical wherever the platform `sin`/`cos`/... agree with V8.

pub mod cells;
pub mod route;
pub mod shard_routing;
pub mod tree;

pub use cells::*;
pub use route::*;
pub use shard_routing::*;
pub use tree::*;

/// `Math.round`: nearest integer, ties toward +infinity, sign of zero kept
/// for inputs in `[-0.5, 0]`.
pub fn js_round(x: f64) -> f64 {
    if !x.is_finite() {
        return x;
    }
    let floor = x.floor();
    let rounded = if x - floor >= 0.5 { floor + 1.0 } else { floor };
    if rounded == 0.0 && (x < 0.0 || (x == 0.0 && x.is_sign_negative())) {
        -0.0
    } else {
        rounded
    }
}

/// `Math.max(a, b)`: NaN-propagating, `+0` beats `-0`.
pub fn js_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        return f64::NAN;
    }
    if a == b {
        return if a.is_sign_negative() { b } else { a };
    }
    if a > b {
        a
    } else {
        b
    }
}

/// `Math.min(a, b)`: NaN-propagating, `-0` beats `+0`.
pub fn js_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        return f64::NAN;
    }
    if a == b {
        return if a.is_sign_negative() { a } else { b };
    }
    if a < b {
        a
    } else {
        b
    }
}

/// `new TextDecoder().decode(bytes)`: lossy UTF-8 that also drops one
/// leading byte-order mark, like the WHATWG decoder's default.
pub(crate) fn decode_utf8(bytes: &[u8]) -> String {
    let bytes = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
    String::from_utf8_lossy(bytes).into_owned()
}

/// `readUtf8` with the JS decoder's BOM handling.
pub(crate) fn read_utf8(reader: &mut crate::util::bytes::Reader<'_>) -> String {
    let len = reader.varint_usize();
    decode_utf8(reader.take(len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_matches_math_round() {
        assert_eq!(js_round(0.5), 1.0);
        assert_eq!(js_round(-0.5).to_bits(), (-0.0f64).to_bits());
        assert_eq!(js_round(-1.5), -1.0);
        assert_eq!(js_round(2.5), 3.0);
        assert_eq!(js_round(0.49999999999999994), 0.0);
        assert_eq!(js_round(-0.2).to_bits(), (-0.0f64).to_bits());
        assert_eq!(js_round(4503599627370497.0), 4503599627370497.0);
    }

    #[test]
    fn min_max_follow_js() {
        assert!(js_max(0.0, f64::NAN).is_nan());
        assert_eq!(js_max(-0.0, 0.0).to_bits(), 0.0f64.to_bits());
        assert_eq!(js_min(0.0, -0.0).to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn utf8_drops_leading_bom_only() {
        assert_eq!(decode_utf8(&[0xef, 0xbb, 0xbf, b'A']), "A");
        assert_eq!(decode_utf8(&[b'A', 0xef, 0xbb, 0xbf]), "A\u{feff}");
    }
}
