//! Columnar doc pages (`rfdocpagecols-v1`) — the decode half of
//! `src/doc_pages.js`: `decodeDocPageColumns`, the doc-page pointer table
//! helpers and `DOC_PAGE_ENCODING`.
//!
//! Documents are `serde_json::Value` objects shaped exactly like the JS
//! objects: `index` first, then every non-null field in manifest order, with
//! JS property-order rules applied (array-index keys such as `"0"` or `"42"`
//! enumerate first, ascending).

use serde_json::{Map, Value};

use super::binary::{check_magic, DOC_PAGE_PAYLOAD_MAGIC};
use super::decode_utf8;
use super::postings::property_key;
use crate::error::{Error, Result};
use crate::util::bytes::Reader;
use crate::util::json::num;

pub use super::doc_pointers::{decode_doc_page_pointer_record, parse_doc_page_pointer_header, parse_doc_page_pointer_page};

pub const DOC_PAGE_FORMAT: &str = "rfdocpage-v1";
pub const DOC_PAGE_POINTER_FORMAT: &str = "rfdocpageptr-v1";
pub const DOC_PAGE_ENCODING: &str = "rfdocpagecols-v1";
const DOC_PAGE_VERSION: u64 = 1;

const VALUE_NULL: u8 = 0;
const VALUE_FALSE: u8 = 1;
const VALUE_TRUE: u8 = 2;
const VALUE_NUMBER: u8 = 3;
const VALUE_STRING: u8 = 4;
const VALUE_STRING_ARRAY: u8 = 5;
const VALUE_JSON: u8 = 6;

/// Is `key` an array index (canonical integer string below 2^32 - 1)?
pub fn is_array_index(key: &str) -> bool {
    if key.is_empty() || key.len() > 10 || !key.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    if key.len() > 1 && key.starts_with('0') {
        return false;
    }
    key.parse::<u64>().map(|n| n < 4_294_967_295).unwrap_or(false)
}

/// Reorder an object's keys the way JS enumerates them: array indexes
/// ascending first, then the remaining keys in insertion order.
pub fn js_property_order(map: Map<String, Value>) -> Map<String, Value> {
    if !map.keys().any(|key| is_array_index(key)) {
        return map;
    }
    let mut indexes: Vec<(u64, String, Value)> = Vec::new();
    let mut rest: Vec<(String, Value)> = Vec::new();
    for (key, value) in map {
        if is_array_index(&key) {
            indexes.push((key.parse().unwrap_or(0), key, value));
        } else {
            rest.push((key, value));
        }
    }
    indexes.sort_by_key(|item| item.0);
    let mut out = Map::new();
    for (_, key, value) in indexes {
        out.insert(key, value);
    }
    for (key, value) in rest {
        out.insert(key, value);
    }
    out
}

/// Normalize a parsed JSON value to what `JSON.parse` would produce in JS:
/// every number is a double (re-emitted like `JSON.stringify`) and object
/// keys follow JS enumeration order.
pub fn js_json_value(value: Value) -> Value {
    match value {
        Value::Number(number) => num(number.as_f64().unwrap_or(f64::NAN)),
        Value::Array(items) => Value::Array(items.into_iter().map(js_json_value).collect()),
        Value::Object(map) => {
            let map: Map<String, Value> = map.into_iter().map(|(key, value)| (key, js_json_value(value))).collect();
            Value::Object(js_property_order(map))
        }
        other => other,
    }
}

fn read_bytes<'a>(r: &mut Reader<'a>) -> Result<&'a [u8]> {
    let length = r.varint_usize();
    let start = r.pos;
    let end = start.saturating_add(length);
    r.pos = end;
    if end > r.bytes.len() {
        return Err(Error::new("Rangefind doc page payload ended inside a value."));
    }
    Ok(&r.bytes[start..end])
}

fn decode_string(r: &mut Reader) -> Result<String> {
    Ok(decode_utf8(read_bytes(r)?))
}

/// Decoded value plus whether JS would keep it (`value !== null`; NaN is a
/// kept number that serializes as null).
fn decode_value(r: &mut Reader) -> Result<Option<Value>> {
    let tag = r.bytes.get(r.pos).copied();
    r.pos += 1;
    match tag {
        Some(VALUE_NULL) => Ok(None),
        Some(VALUE_FALSE) => Ok(Some(Value::Bool(false))),
        Some(VALUE_TRUE) => Ok(Some(Value::Bool(true))),
        Some(VALUE_NUMBER) => {
            if r.pos + 8 > r.bytes.len() {
                return Err(Error::new("Rangefind doc page payload ended inside a number."));
            }
            Ok(Some(num(r.f64_le())))
        }
        Some(VALUE_STRING) => Ok(Some(Value::String(decode_string(r)?))),
        Some(VALUE_STRING_ARRAY) => {
            let count = r.varint_usize();
            let mut items = Vec::with_capacity(count.min(1 << 20));
            for _ in 0..count {
                items.push(Value::String(decode_string(r)?));
            }
            Ok(Some(Value::Array(items)))
        }
        Some(VALUE_JSON) => {
            let text = decode_string(r)?;
            let parsed: Value = serde_json::from_str(&text).map_err(|error| Error::new(format!("Rangefind doc page JSON value: {error}")))?;
            match parsed {
                Value::Null => Ok(None),
                other => Ok(Some(js_json_value(other))),
            }
        }
        Some(other) => Err(Error::new(format!("Unsupported Rangefind doc page value type {other}."))),
        None => Err(Error::new("Unsupported Rangefind doc page value type undefined.")),
    }
}

/// `decodeDocPageColumns(buffer, fields, startIndex)`.
pub fn decode_doc_page_columns(bytes: &[u8], fields: &[Value], start: usize) -> Result<Vec<Value>> {
    let names: Vec<String> = fields.iter().map(property_key).collect();
    decode_doc_page_columns_named(bytes, &names, start)
}

/// [`decode_doc_page_columns`] with field names already resolved.
pub fn decode_doc_page_columns_named(bytes: &[u8], fields: &[String], start: usize) -> Result<Vec<Value>> {
    check_magic(bytes, DOC_PAGE_PAYLOAD_MAGIC, "Unsupported Rangefind doc page payload")?;
    let mut r = Reader::at(bytes, DOC_PAGE_PAYLOAD_MAGIC.len());
    let version = r.varint();
    if version != DOC_PAGE_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind doc page payload version {version}")));
    }
    let count = r.varint_usize();
    let field_count = r.varint_usize();
    if field_count != fields.len() {
        return Err(Error::new("Rangefind doc page field count does not match the manifest."));
    }
    let mut docs: Vec<Map<String, Value>> = (0..count.min(1 << 24))
        .map(|row| {
            let mut map = Map::new();
            map.insert("index".into(), num((start + row) as f64));
            map
        })
        .collect();
    for field in fields {
        for doc in docs.iter_mut() {
            if let Some(value) = decode_value(&mut r)? {
                // Assigning `__proto__` sets the prototype (or is ignored);
                // it never creates an own property.
                if field != "__proto__" {
                    doc.insert(field.clone(), value);
                }
            }
        }
    }
    if r.pos != bytes.len() {
        return Err(Error::new("Rangefind doc page payload has trailing bytes."));
    }
    Ok(docs.into_iter().map(|doc| Value::Object(js_property_order(doc))).collect())
}

pub use super::doc_pointers::DocPointerMeta as DocPagePointerMeta;
