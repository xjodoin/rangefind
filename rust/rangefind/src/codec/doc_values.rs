//! Doc-value chunks (`RFVB`), the legacy/v2 code table (`RFCB`,
//! `codes.bin.gz`) and facet dictionaries (`RFFD`) — the decode half of the
//! doc-value parts of `src/codec.js` (`parseDocValueChunk`, `parseCodes`,
//! `parseFacetDictionary`) plus `facetCodeMatches` from `src/runtime.js`.

use std::collections::HashSet;

use serde_json::{Map, Value};

use super::binary::{check_magic, CODE_MAGIC, DOC_VALUE_MAGIC, FACET_DICT_MAGIC};
use super::postings::f64_view_at;
use super::{byte_at, fixed_int, read_utf8};
use crate::error::{Error, Result};
use crate::util::bytes::Reader;
use crate::util::json::num;

pub const DOC_VALUE_ENCODING_DENSE: u8 = 0;
pub const DOC_VALUE_ENCODING_SPARSE_FACET: u8 = 1;

/// `CODE_KIND_NAME[byte] || "number"`
pub fn code_kind_name(code: Option<u8>) -> &'static str {
    match code {
        Some(1) => "facet",
        Some(2) => "number",
        Some(3) => "boolean",
        _ => "number",
    }
}

/// `CODE_TYPE_NAME[byte] || "int"`
pub fn code_type_name(code: Option<u8>) -> &'static str {
    match code {
        Some(1) => "keyword",
        Some(2) => "int",
        Some(3) => "long",
        Some(4) => "float",
        Some(5) => "double",
        Some(6) => "date",
        Some(7) => "boolean",
        _ => "int",
    }
}

/// A facet value as stored per document.
#[derive(Clone, Debug, PartialEq)]
pub enum FacetValue {
    /// Sparse encoding: `{ codes: [...] }` (ascending, as stored).
    Codes(Vec<u64>),
    /// Dense encoding: bitset words (`[w0, w1, ...]`).
    Words(Vec<u64>),
}

/// One document's value (`chunk.values[i]` / `codes[field][doc]`).
#[derive(Clone, Debug, PartialEq)]
pub enum DocValue {
    /// `null`
    Missing,
    Number(f64),
    Bool(bool),
    Facet(FacetValue),
}

impl DocValue {
    pub fn is_missing(&self) -> bool {
        matches!(self, DocValue::Missing)
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            DocValue::Number(n) => Some(*n),
            _ => None,
        }
    }

    /// `facetCodeMatches(value, selected)` from `src/runtime.js`. A bare
    /// number (legacy code tables) is a single code: `selected.has(value)`.
    pub fn facet_matches(&self, selected: &HashSet<u32>) -> bool {
        if selected.is_empty() {
            return true;
        }
        match self {
            DocValue::Facet(facet) => facet.facet_matches(selected),
            DocValue::Number(n) => *n >= 0.0 && *n <= u32::MAX as f64 && *n == n.trunc() && selected.contains(&(*n as u32)),
            DocValue::Missing | DocValue::Bool(_) => false,
        }
    }

    /// The JS value as JSON (`null`, number, boolean, `{codes}` or words).
    pub fn to_json(&self) -> Value {
        match self {
            DocValue::Missing => Value::Null,
            DocValue::Number(n) => num(*n),
            DocValue::Bool(flag) => Value::Bool(*flag),
            DocValue::Facet(FacetValue::Codes(codes)) => {
                let mut map = Map::new();
                map.insert("codes".into(), Value::Array(codes.iter().map(|c| num(*c as f64)).collect()));
                Value::Object(map)
            }
            DocValue::Facet(FacetValue::Words(words)) => Value::Array(words.iter().map(|w| num(*w as f64)).collect()),
        }
    }
}

impl FacetValue {
    pub fn facet_matches(&self, selected: &HashSet<u32>) -> bool {
        if selected.is_empty() {
            return true;
        }
        match self {
            FacetValue::Codes(codes) => codes.iter().any(|&code| code <= u32::MAX as u64 && selected.contains(&(code as u32))),
            FacetValue::Words(words) => selected.iter().any(|&value| {
                let word = (value / 32) as usize;
                let bit = value % 32;
                // `(words[word] || 0) & (2 ** bit)` — ToInt32 keeps the low 32 bits.
                (words.get(word).copied().unwrap_or(0) as u32) & (1u32 << bit) != 0
            }),
        }
    }
}

/// A parsed doc-value chunk (`parseDocValueChunk`).
#[derive(Clone, Debug, PartialEq)]
pub struct DocValueChunk {
    pub name: String,
    pub kind: &'static str,
    pub type_: &'static str,
    pub width: u8,
    pub words: u64,
    pub encoding: u8,
    pub start: usize,
    pub count: usize,
    pub min: f64,
    pub values: Vec<DocValue>,
}

impl DocValueChunk {
    /// `chunk.values[doc - chunk.start]` (Missing outside the chunk).
    pub fn value(&self, doc: usize) -> &DocValue {
        static MISSING: DocValue = DocValue::Missing;
        doc.checked_sub(self.start).and_then(|i| self.values.get(i)).unwrap_or(&MISSING)
    }
}

fn f64_at(bytes: &[u8], pos: usize) -> Result<f64> {
    f64_view_at(bytes, pos)
}

fn read_f64(reader: &mut Reader) -> Result<f64> {
    let value = f64_at(reader.bytes, reader.pos)?;
    reader.pos += 8;
    Ok(value)
}

fn next_byte(reader: &mut Reader) -> Option<u8> {
    let value = byte_at(reader.bytes, reader.pos);
    reader.pos += 1;
    value
}

/// Integer column value: `encoded ? min + encoded - 1 : null`.
fn integer_value(min: f64, encoded: Option<u64>) -> DocValue {
    match encoded {
        Some(encoded) if encoded != 0 => DocValue::Number(min + encoded as f64 - 1.0),
        _ => DocValue::Missing,
    }
}

fn float_value(value: f64) -> DocValue {
    if value.is_nan() {
        DocValue::Missing
    } else {
        DocValue::Number(value)
    }
}

fn bool_value(byte: Option<u8>) -> DocValue {
    match byte {
        Some(0) => DocValue::Missing,
        Some(2) => DocValue::Bool(true),
        _ => DocValue::Bool(false),
    }
}

fn dense_words(bytes: &[u8], pos: usize, doc: usize, words: usize, width: usize) -> Vec<u64> {
    (0..words)
        // A word past the end is NaN in JS; it can never test as set.
        .map(|word| fixed_int(bytes, pos + (doc * words + word) * width, width).unwrap_or(0))
        .collect()
}

/// `parseDocValueChunk(buffer)`.
pub fn parse_doc_value_chunk(bytes: &[u8]) -> Result<DocValueChunk> {
    check_magic(bytes, DOC_VALUE_MAGIC, "Unsupported Rangefind doc-value chunk")?;
    let mut r = Reader::at(bytes, DOC_VALUE_MAGIC.len());
    let version = r.varint();
    if version != 1 && version != 2 {
        return Err(Error::new(format!("Unsupported Rangefind doc-value chunk version {version}")));
    }
    let name = read_utf8(&mut r);
    let kind = code_kind_name(next_byte(&mut r));
    let type_ = code_type_name(next_byte(&mut r));
    let width = next_byte(&mut r).unwrap_or(0);
    let words = r.varint();
    let encoding = if version >= 2 { next_byte(&mut r).unwrap_or(0) } else { DOC_VALUE_ENCODING_DENSE };
    let start = r.varint_usize();
    let count = r.varint_usize();
    let min = read_f64(&mut r)?;
    let pos = r.pos;
    let w = width as usize;
    let mut values = Vec::with_capacity(count.min(1 << 24));
    if kind == "facet" && encoding == DOC_VALUE_ENCODING_SPARSE_FACET {
        let offsets: Vec<Option<u64>> = (0..=count).map(|doc| fixed_int(bytes, pos + doc * w, w)).collect();
        let data_start = pos + offsets.len() * w;
        for doc in 0..count {
            let mut codes = Vec::new();
            if let (Some(begin), Some(end)) = (offsets[doc], offsets[doc + 1]) {
                let end = data_start as u64 + end;
                let mut cursor = Reader::at(bytes, (data_start as u64 + begin) as usize);
                let code_count = cursor.varint();
                let mut previous = 0u64;
                let mut i = 0;
                while i < code_count && cursor.pos as u64 <= end {
                    previous = previous.wrapping_add(cursor.varint());
                    codes.push(previous);
                    i += 1;
                }
            }
            values.push(DocValue::Facet(FacetValue::Codes(codes)));
        }
    } else if kind == "facet" {
        for doc in 0..count {
            values.push(DocValue::Facet(FacetValue::Words(dense_words(bytes, pos, doc, words as usize, w))));
        }
    } else if kind == "boolean" {
        for doc in 0..count {
            values.push(bool_value(byte_at(bytes, pos + doc)));
        }
    } else if type_ == "float" || type_ == "double" {
        for doc in 0..count {
            values.push(float_value(f64_at(bytes, pos + doc * 8)?));
        }
    } else {
        for doc in 0..count {
            values.push(integer_value(min, fixed_int(bytes, pos + doc * w, w)));
        }
    }
    Ok(DocValueChunk { name, kind, type_, width, words, encoding, start, count, min, values })
}

// ---------------------------------------------------------------------------
// Code table (codes.bin.gz)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct CodeField {
    pub name: String,
    pub kind: &'static str,
    pub type_: &'static str,
    pub width: u8,
    pub words: u64,
    pub min: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CodesMeta {
    pub version: u64,
    pub total: u64,
    pub fields: Vec<CodeField>,
}

/// `parseCodes(buffer)`: per-field value columns. Legacy tables (version
/// != 2) hold raw fixed-width integers, exposed as `DocValue::Number`, and
/// have no `meta` (the JS `_meta` is only defined for v2).
#[derive(Clone, Debug, PartialEq)]
pub struct Codes {
    /// Columns in object-key order (a repeated name keeps its first position
    /// and its last values).
    pub columns: Vec<(String, Vec<DocValue>)>,
    pub meta: Option<CodesMeta>,
}

impl Codes {
    pub fn column(&self, field: &str) -> Option<&[DocValue]> {
        self.columns.iter().find(|(name, _)| name == field).map(|(_, values)| values.as_slice())
    }

    /// `codes[field][doc]` (Missing when absent).
    pub fn value(&self, field: &str, doc: usize) -> DocValue {
        self.column(field).and_then(|values| values.get(doc)).cloned().unwrap_or(DocValue::Missing)
    }
}

fn set_column(columns: &mut Vec<(String, Vec<DocValue>)>, name: String, values: Vec<DocValue>) {
    match columns.iter_mut().find(|(key, _)| *key == name) {
        Some(slot) => slot.1 = values,
        None => columns.push((name, values)),
    }
}

fn parse_legacy_codes(bytes: &[u8]) -> Codes {
    let mut r = Reader::at(bytes, CODE_MAGIC.len());
    let total = r.varint_usize();
    let field_count = r.varint_usize();
    let mut fields = Vec::new();
    for _ in 0..field_count {
        let name = read_utf8(&mut r);
        let width = next_byte(&mut r).unwrap_or(0) as usize;
        fields.push((name, width));
    }
    let mut columns = Vec::new();
    for (name, width) in fields {
        let values = (0..total)
            .map(|i| match fixed_int(bytes, r.pos + i * width, width) {
                Some(raw) => DocValue::Number(raw as f64),
                None => DocValue::Number(f64::NAN),
            })
            .collect();
        r.pos += total * width;
        set_column(&mut columns, name, values);
    }
    Codes { columns, meta: None }
}

/// `parseCodes(buffer)`.
pub fn parse_codes(bytes: &[u8]) -> Result<Codes> {
    check_magic(bytes, CODE_MAGIC, "Unsupported Rangefind code table")?;
    let mut r = Reader::at(bytes, CODE_MAGIC.len());
    let version = r.varint();
    if version != 2 {
        return Ok(parse_legacy_codes(bytes));
    }
    let total = r.varint();
    let field_count = r.varint_usize();
    let mut fields = Vec::new();
    for _ in 0..field_count {
        let name = read_utf8(&mut r);
        let kind = code_kind_name(next_byte(&mut r));
        let type_ = code_type_name(next_byte(&mut r));
        let width = next_byte(&mut r).unwrap_or(0);
        let words = r.varint();
        let min = read_f64(&mut r)?;
        fields.push(CodeField { name, kind, type_, width, words, min });
    }
    let n = total as usize;
    let mut columns = Vec::new();
    for field in &fields {
        let w = field.width as usize;
        let pos = r.pos;
        let mut values = Vec::with_capacity(n.min(1 << 24));
        if field.kind == "facet" {
            for doc in 0..n {
                values.push(DocValue::Facet(FacetValue::Words(dense_words(bytes, pos, doc, field.words as usize, w))));
            }
            r.pos += n * field.words as usize * w;
        } else if field.kind == "boolean" {
            for doc in 0..n {
                values.push(bool_value(byte_at(bytes, pos + doc)));
            }
            r.pos += n;
        } else if field.type_ == "float" || field.type_ == "double" {
            for doc in 0..n {
                values.push(float_value(f64_at(bytes, pos + doc * 8)?));
            }
            r.pos += n * 8;
        } else {
            for doc in 0..n {
                values.push(integer_value(field.min, fixed_int(bytes, pos + doc * w, w)));
            }
            r.pos += n * w;
        }
        set_column(&mut columns, field.name.clone(), values);
    }
    Ok(Codes { columns, meta: Some(CodesMeta { version, total, fields }) })
}

// ---------------------------------------------------------------------------
// Facet dictionaries
// ---------------------------------------------------------------------------

/// One dictionary row (`{ value, label, n }`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FacetDictEntry {
    pub value: String,
    pub label: String,
    pub n: u64,
}

impl FacetDictEntry {
    pub fn to_json(&self) -> Value {
        let mut map = Map::new();
        map.insert("value".into(), Value::String(self.value.clone()));
        map.insert("label".into(), Value::String(self.label.clone()));
        map.insert("n".into(), num(self.n as f64));
        Value::Object(map)
    }
}

/// Typed `parseFacetDictionary`.
pub fn parse_facet_dictionary_entries(bytes: &[u8]) -> Result<Vec<FacetDictEntry>> {
    check_magic(bytes, FACET_DICT_MAGIC, "Unsupported Rangefind facet dictionary")?;
    let mut r = Reader::at(bytes, FACET_DICT_MAGIC.len());
    let version = r.varint();
    if version != 1 {
        return Err(Error::new(format!("Unsupported Rangefind facet dictionary version {version}")));
    }
    let count = r.varint_usize();
    let mut values = Vec::with_capacity(count.min(1 << 20));
    for _ in 0..count {
        let value = read_utf8(&mut r);
        let label = read_utf8(&mut r);
        let n = r.varint();
        values.push(FacetDictEntry { value, label, n });
    }
    Ok(values)
}

/// `parseFacetDictionary(buffer)`: the same `{ value, label, n }` objects.
pub fn parse_facet_dictionary(bytes: &[u8]) -> Result<Vec<Value>> {
    Ok(parse_facet_dictionary_entries(bytes)?.iter().map(FacetDictEntry::to_json).collect())
}

pub use super::doc_value_manifest::{doc_values_meta_from_json, parse_doc_value_manifest, DocValueChunkMeta, DocValuesMeta};
