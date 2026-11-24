//! Binary index formats read by the query runtime (decode side only).
//!
//! One submodule per JavaScript codec module:
//!
//! | Rust module          | JS module                    |
//! |----------------------|------------------------------|
//! | [`binary`]           | `src/binary.js` (magics)     |
//! | [`postings`]         | `src/codec.js` (rfsegpost-v6)|
//! | [`doc_values`]       | `src/codec.js` (doc values, codes, facet dictionaries) |
//! | [`directory`]        | `src/directory.js`           |
//! | [`doc_pointers`]     | `src/doc_pointers.js`        |
//! | [`doc_pages`]        | `src/doc_pages.js`           |
//! | [`doc_value_tree`]   | `src/doc_value_tree.js`      |
//! | [`doc_value_manifest`] | `src/doc_value_manifest.js` (+ JSON manifest) |
//! | [`filter_bitmaps`]   | `src/filter_bitmaps.js`      |
//! | [`query_bundle`]     | `src/query_bundle_codec.js`  |
//! | [`segment`]          | `src/segment_codec.js`       |
//! | [`text_routing`]     | `src/text_routing_codec.js`  |
//! | [`http_ranges`]      | `src/http_ranges.js`         |
//! | [`object_store`]     | `src/object_store.js`        |

pub mod binary;
pub mod directory;
pub mod doc_pages;
pub mod doc_pointers;
pub mod doc_value_manifest;
pub mod doc_value_tree;
pub mod doc_values;
pub mod filter_bitmaps;
pub mod http_ranges;
pub mod object_store;
pub mod postings;
pub mod query_bundle;
pub mod segment;
pub mod text_routing;

pub use object_store::verify_pointer;

use std::cmp::Ordering;
use std::rc::Rc;

use crate::util::bytes::Reader;

/// `readUtf8` with `TextDecoder` semantics: invalid sequences become U+FFFD
/// and a single leading byte-order mark is *consumed* (the WHATWG decoder
/// default, `ignoreBOM: false`). The shared `Reader::utf8` keeps the BOM.
pub(crate) fn read_utf8(reader: &mut Reader) -> String {
    let len = reader.varint_usize();
    decode_utf8(reader.take(len))
}

pub(crate) fn decode_utf8(bytes: &[u8]) -> String {
    let body = bytes.strip_prefix(&[0xef, 0xbb, 0xbf][..]).unwrap_or(bytes);
    String::from_utf8_lossy(body).into_owned()
}

/// `bytes[pos]` as JS sees it (`undefined` → None).
#[inline]
pub(crate) fn byte_at(bytes: &[u8], pos: usize) -> Option<u8> {
    bytes.get(pos).copied()
}

/// JS `readFixedInt`: little-endian, accumulated in a double. Reading past
/// the end yields NaN in JS (`undefined * n`); that is reported as None.
pub(crate) fn fixed_int(bytes: &[u8], offset: usize, width: usize) -> Option<u64> {
    if width > 0 && offset.saturating_add(width) > bytes.len() {
        return None;
    }
    let mut value = 0u64;
    for i in 0..width.min(8) {
        value |= (bytes[offset + i] as u64) << (8 * i);
    }
    if width > 8 {
        // JS keeps adding `byte * 2^(8i)` in doubles; anything above 2^64 is
        // beyond every real width (1/2/4/6/8). Saturate.
        if bytes[offset + 8..offset + width].iter().any(|b| *b != 0) {
            return Some(u64::MAX);
        }
    }
    Some(value)
}

/// `Math.round`: nearest integer, ties toward +Infinity.
pub(crate) fn js_round(x: f64) -> f64 {
    if !x.is_finite() {
        return x;
    }
    let floor = x.floor();
    if x - floor >= 0.5 {
        floor + 1.0
    } else {
        floor
    }
}

/// ECMAScript `ToInt32` (what storing into an `Int32Array` does).
pub(crate) fn to_int32(x: f64) -> i32 {
    if !x.is_finite() {
        return 0;
    }
    let t = x.trunc();
    let m = t.rem_euclid(4_294_967_296.0);
    (m as u64 as u32) as i32
}

/// A string key decoded from a front-coded (prefix-compressed) list.
///
/// Prefix lengths are UTF-16 code units, so a prefix may end between the two
/// halves of a surrogate pair. JS then holds a string with a lone surrogate:
/// it can never equal a well-formed lookup string, and it orders by its raw
/// code units. `text` is the lossy (U+FFFD) form; `units` keeps the exact
/// code units in that (rare) case so ordering and lookups stay faithful.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrontCodedKey {
    pub text: String,
    pub units: Option<Rc<[u16]>>,
}

impl FrontCodedKey {
    pub fn well_formed(&self) -> bool {
        self.units.is_none()
    }

    pub fn as_str(&self) -> &str {
        &self.text
    }

    pub fn units16(&self) -> Vec<u16> {
        match &self.units {
            Some(units) => units.to_vec(),
            None => self.text.encode_utf16().collect(),
        }
    }
}

/// UTF-16 code-unit comparison of a key against a (well-formed) string:
/// the JS `<` / `>` relational order.
pub trait JsKeyOrd {
    fn cmp_js(&self, other: &str) -> Ordering;
}

impl JsKeyOrd for str {
    fn cmp_js(&self, other: &str) -> Ordering {
        crate::util::jsstr::cmp16(self, other)
    }
}

impl JsKeyOrd for String {
    fn cmp_js(&self, other: &str) -> Ordering {
        crate::util::jsstr::cmp16(self, other)
    }
}

impl JsKeyOrd for &str {
    fn cmp_js(&self, other: &str) -> Ordering {
        crate::util::jsstr::cmp16(self, other)
    }
}

impl JsKeyOrd for Rc<str> {
    fn cmp_js(&self, other: &str) -> Ordering {
        crate::util::jsstr::cmp16(self, other)
    }
}

impl JsKeyOrd for FrontCodedKey {
    fn cmp_js(&self, other: &str) -> Ordering {
        match &self.units {
            None => crate::util::jsstr::cmp16(&self.text, other),
            Some(units) => units.iter().copied().cmp(other.encode_utf16()),
        }
    }
}

/// Decode a front-coded key: `previous.slice(0, prefix) + suffix`, where
/// `previous` is given in UTF-16 units. Returns the key and its units.
pub(crate) fn front_coded_key(previous: &[u16], prefix: usize, suffix: &str) -> (FrontCodedKey, Vec<u16>) {
    let mut units: Vec<u16> = previous[..prefix.min(previous.len())].to_vec();
    units.extend(suffix.encode_utf16());
    let key = match String::from_utf16(&units) {
        Ok(text) => FrontCodedKey { text, units: None },
        Err(_) => FrontCodedKey { text: String::from_utf16_lossy(&units), units: Some(Rc::from(units.clone())) },
    };
    (key, units)
}

/// Pack-table lookup from a manifest JSON array (`table[index]`), keeping
/// only string entries.
pub(crate) fn string_table(value: &serde_json::Value) -> Vec<String> {
    match value.as_array() {
        Some(items) => items.iter().map(|item| item.as_str().unwrap_or("").to_string()).collect(),
        None => Vec::new(),
    }
}

/// `${String(index).padStart(4, "0")}.bin`
pub(crate) fn default_pack_file(index: u64) -> String {
    format!("{index:04}.bin")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn js_numeric_helpers() {
        assert_eq!(js_round(2.5), 3.0);
        assert_eq!(js_round(-2.5), -2.0);
        assert_eq!(js_round(0.49999999999999994), 0.0);
        assert_eq!(to_int32(4_294_967_296.0 + 5.0), 5);
        assert_eq!(to_int32(2_147_483_648.0), i32::MIN);
        assert_eq!(to_int32(-1.0), -1);
        assert_eq!(decode_utf8(&[0xef, 0xbb, 0xbf, b'a']), "a");
        assert_eq!(fixed_int(&[1, 2], 0, 2), Some(0x0201));
        assert_eq!(fixed_int(&[1, 2], 1, 2), None);
        assert_eq!(fixed_int(&[], 5, 0), Some(0));
    }

    #[test]
    fn front_coding_keeps_lone_surrogates() {
        let previous: Vec<u16> = "😀".encode_utf16().collect();
        let (key, _) = front_coded_key(&previous, 1, "\u{fffd}");
        assert!(!key.well_formed());
        assert_eq!(key.text, "\u{fffd}\u{fffd}");
        assert_eq!(key.cmp_js("\u{e000}"), Ordering::Less);
    }
}

// ------------------------------------------------------------------ engine-facing adapters
//
// Short aliases and convenience accessors the query engine uses. They add no
// decoding logic of their own.

/// Sorted doc-value tree (`src/doc_value_tree.js`).
pub mod sort_tree {
    pub use super::doc_value_tree::*;

    impl DocValueSortDirectory {
        pub fn field_name(&self) -> &str {
            &self.field.name
        }
    }

    /// A row as the engine consumes it.
    #[derive(Clone, Debug)]
    pub struct EngineSortRow {
        pub doc: i32,
        pub value: super::doc_values::DocValue,
        pub sort_value: f64,
    }

    impl SortPage {
        /// `sortedPageRows(page, desc)`: by sort value, then doc id.
        pub fn sorted_rows(&self, desc: bool) -> Vec<EngineSortRow> {
            let mut rows: Vec<EngineSortRow> = self
                .rows
                .iter()
                .map(|row| EngineSortRow { doc: row.doc as i32, value: row.value.clone(), sort_value: row.sort_value })
                .collect();
            rows.sort_by(|a, b| {
                let order = if desc { b.sort_value.partial_cmp(&a.sort_value) } else { a.sort_value.partial_cmp(&b.sort_value) };
                order.unwrap_or(std::cmp::Ordering::Equal).then(a.doc.cmp(&b.doc))
            });
            rows
        }
    }
}

/// Filter bitmaps (`src/filter_bitmaps.js`).
pub mod filter_bitmap {
    pub use super::filter_bitmaps::*;
}

/// Facet dictionaries.
pub mod facet_dictionary {
    pub use super::doc_values::parse_facet_dictionary;
}

/// `{ pack, offset, length, logicalLength, checksum }` JSON → [`Pointer`].
pub fn pointer_from_json(value: &serde_json::Value) -> crate::pointer::Pointer {
    use crate::util::json::{to_number, truthy};
    let int = |key: &str| {
        let n = to_number(value.get(key).unwrap_or(&serde_json::Value::Null));
        if n.is_finite() && n > 0.0 { n as u64 } else { 0 }
    };
    let pack = value.get("pack").and_then(serde_json::Value::as_str).unwrap_or("");
    let length = if truthy(value.get("length").unwrap_or(&serde_json::Value::Null)) { int("length") } else { int("physicalLength") };
    let mut pointer = crate::pointer::Pointer::new(pack, int("offset"), length);
    let logical = int("logicalLength");
    pointer.logical_length = if logical > 0 { Some(logical) } else { None };
    if let Some(checksum) = value.get("checksum").filter(|c| truthy(c.get("value").unwrap_or(&serde_json::Value::Null))) {
        pointer.checksum = Some(crate::pointer::Checksum {
            algorithm: std::rc::Rc::from(checksum.get("algorithm").and_then(serde_json::Value::as_str).unwrap_or("sha256")),
            value: std::rc::Rc::from(checksum["value"].as_str().unwrap_or("")),
        });
    }
    pointer
}

impl doc_values::DocValue {
    /// Numeric value for range filters and sorting (booleans as 1/0 never
    /// reach here: the runtime compares booleans separately).
    pub fn as_number(&self) -> Option<f64> {
        self.as_f64()
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            doc_values::DocValue::Bool(flag) => Some(*flag),
            _ => None,
        }
    }
}

impl doc_values::FacetValue {
    /// `value?.codes` — only the sparse encoding carries an explicit list.
    pub fn codes(&self) -> Option<Vec<u32>> {
        match self {
            doc_values::FacetValue::Codes(codes) => Some(codes.iter().map(|c| *c as u32).collect()),
            doc_values::FacetValue::Words(_) => None,
        }
    }
}

impl doc_values::DocValueChunk {
    pub fn value_at(&self, doc: usize) -> doc_values::DocValue {
        self.value(doc).clone()
    }
}

impl doc_value_manifest::DocValuesMeta {
    /// `docValues.chunk_size || manifest.total || 1`.
    pub fn chunk_size_or(&self, total: usize) -> usize {
        if self.chunk_size > 0 {
            self.chunk_size
        } else if total > 0 {
            total
        } else {
            1
        }
    }
}
