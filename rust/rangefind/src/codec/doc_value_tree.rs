//! Doc-value sort trees — the decode half of `src/doc_value_tree.js`:
//! `parseDocValueSortDirectory` (`rfdocvaluesortdir-v1`) and
//! `decodeDocValueSortPage` (`rfdocvaluesortpage-v1`).

use std::rc::Rc;

use super::binary::{check_magic, DOC_VALUE_SORT_DIRECTORY_MAGIC, DOC_VALUE_SORT_PAGE_MAGIC};
use super::doc_values::DocValue;
use super::postings::f64_view_at;
use super::{fixed_int, read_utf8};
use crate::error::{Error, Result};
use crate::pointer::{Checksum, Pointer};
use crate::util::bytes::Reader;

pub const DOC_VALUE_SORT_DIRECTORY_FORMAT: &str = "rfdocvaluesortdir-v1";
pub const DOC_VALUE_SORT_PAGE_FORMAT: &str = "rfdocvaluesortpage-v1";
const FORMAT_VERSION: u64 = 1;
const VALUE_FLOAT64: u8 = 1;
const VALUE_INT_DELTA: u8 = 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortField {
    pub name: String,
    pub kind: String,
    pub type_: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SortSummary {
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// One page of the directory (`pages[i]`).
#[derive(Clone, Debug, PartialEq)]
pub struct SortPageMeta {
    pub index: usize,
    pub rank_start: u64,
    pub count: u64,
    pub min: f64,
    pub max: f64,
    /// `pack` (`packTable[packIndex]`; empty when the index is missing),
    /// `offset`, `length`, `logicalLength || null`, `checksum`.
    pub pointer: Pointer,
    pub physical_length: u64,
    /// `page.summaries` keyed by summary field name (object order).
    pub summaries: Vec<(String, SortSummary)>,
}

impl SortPageMeta {
    pub fn summary(&self, field: &str) -> Option<&SortSummary> {
        self.summaries.iter().find(|(name, _)| name == field).map(|(_, summary)| summary)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DocValueSortDirectory {
    pub field: SortField,
    pub page_size: u64,
    pub total: u64,
    pub pack_table: Vec<String>,
    pub summary_fields: Vec<SortField>,
    pub pages: Vec<SortPageMeta>,
}

impl DocValueSortDirectory {
    pub fn format(&self) -> &'static str {
        DOC_VALUE_SORT_DIRECTORY_FORMAT
    }

    pub fn page_format(&self) -> &'static str {
        DOC_VALUE_SORT_PAGE_FORMAT
    }
}

fn read_f64(r: &mut Reader) -> Result<f64> {
    if r.pos.saturating_add(8) > r.bytes.len() {
        return Err(Error::new("Rangefind doc-value sort payload ended inside a float64."));
    }
    Ok(r.f64_le())
}

fn read_field(r: &mut Reader) -> SortField {
    let name = read_utf8(r);
    let kind = read_utf8(r);
    let type_ = read_utf8(r);
    SortField { name, kind, type_ }
}

/// `parseDocValueSortDirectory(buffer)`.
pub fn parse_doc_value_sort_directory(bytes: &[u8]) -> Result<DocValueSortDirectory> {
    check_magic(bytes, DOC_VALUE_SORT_DIRECTORY_MAGIC, "Unsupported Rangefind doc-value sort directory")?;
    let mut r = Reader::at(bytes, DOC_VALUE_SORT_DIRECTORY_MAGIC.len());
    let version = r.varint();
    if version != FORMAT_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind doc-value sort directory version {version}")));
    }
    let field = read_field(&mut r);
    let page_size = r.varint();
    let total = r.varint();
    let pack_count = r.varint_usize();
    let mut pack_table = Vec::with_capacity(pack_count.min(1 << 20));
    for _ in 0..pack_count {
        pack_table.push(read_utf8(&mut r));
    }
    let summary_count = r.varint_usize();
    let mut summary_fields = Vec::with_capacity(summary_count.min(1 << 16));
    for _ in 0..summary_count {
        summary_fields.push(read_field(&mut r));
    }
    let page_count = r.varint_usize();
    let mut pages = Vec::with_capacity(page_count.min(1 << 20));
    for index in 0..page_count {
        let rank_start = r.varint();
        let count = r.varint();
        let min = read_f64(&mut r)?;
        let max = read_f64(&mut r)?;
        let pack_index = r.varint();
        let pack = pack_table.get(pack_index as usize).cloned().unwrap_or_default();
        let offset = r.varint();
        let length = r.varint();
        let physical_length = r.varint();
        let logical_length = r.varint();
        let algorithm = read_utf8(&mut r);
        let value = read_utf8(&mut r);
        let mut pointer = Pointer::new(pack, offset, length);
        pointer.logical_length = if logical_length != 0 { Some(logical_length) } else { None };
        pointer.checksum = if value.is_empty() {
            None
        } else {
            Some(Checksum {
                algorithm: Rc::from(if algorithm.is_empty() { "sha256" } else { algorithm.as_str() }),
                value: Rc::from(value.as_str()),
            })
        };
        let mut summaries: Vec<(String, SortSummary)> = Vec::with_capacity(summary_fields.len());
        for summary_field in &summary_fields {
            let has_values = r.bytes.get(r.pos).copied() == Some(1);
            r.pos += 1;
            let summary = if has_values {
                let min = read_f64(&mut r)?;
                let max = read_f64(&mut r)?;
                SortSummary { min: Some(min), max: Some(max) }
            } else {
                SortSummary { min: None, max: None }
            };
            match summaries.iter_mut().find(|(name, _)| *name == summary_field.name) {
                Some(slot) => slot.1 = summary,
                None => summaries.push((summary_field.name.clone(), summary)),
            }
        }
        pages.push(SortPageMeta { index, rank_start, count, min, max, pointer, physical_length, summaries });
    }
    if r.pos != bytes.len() {
        return Err(Error::new("Rangefind doc-value sort directory has trailing bytes."));
    }
    Ok(DocValueSortDirectory { field, page_size, total, pack_table, summary_fields, pages })
}

/// One sorted row: `{ doc, value, sortValue, rank }`.
#[derive(Clone, Debug, PartialEq)]
pub struct SortRow {
    pub doc: u64,
    /// `runtimeValue`: `Bool(sortValue === 2)` for boolean fields, else the number.
    pub value: DocValue,
    pub sort_value: f64,
    pub rank: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SortPage {
    pub field: SortField,
    pub rank_start: u64,
    pub count: u64,
    pub rows: Vec<SortRow>,
}

/// `decodeDocValueSortPage(buffer, { name })`.
pub fn decode_doc_value_sort_page(bytes: &[u8], expected_name: Option<&str>) -> Result<SortPage> {
    check_magic(bytes, DOC_VALUE_SORT_PAGE_MAGIC, "Unsupported Rangefind doc-value sort page")?;
    let mut r = Reader::at(bytes, DOC_VALUE_SORT_PAGE_MAGIC.len());
    let version = r.varint();
    if version != FORMAT_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind doc-value sort page version {version}")));
    }
    let field = read_field(&mut r);
    if let Some(expected) = expected_name {
        if !expected.is_empty() && expected != field.name {
            return Err(Error::new(format!("Rangefind doc-value sort page field mismatch: {}", field.name)));
        }
    }
    let rank_start = r.varint();
    let count = r.varint();
    let n = count as usize;
    let next = |r: &mut Reader| {
        let value = r.bytes.get(r.pos).copied();
        r.pos += 1;
        value
    };
    let value_encoding = next(&mut r);
    let value_width = next(&mut r).unwrap_or(0) as usize;
    let doc_width = next(&mut r).unwrap_or(0) as usize;
    let min = read_f64(&mut r)?;
    let mut values = Vec::with_capacity(n.min(1 << 24));
    match value_encoding {
        Some(VALUE_FLOAT64) => {
            for i in 0..n {
                values.push(f64_view_at(bytes, r.pos + i * 8)?);
            }
            r.pos += n * 8;
        }
        Some(VALUE_INT_DELTA) => {
            for i in 0..n {
                values.push(match fixed_int(bytes, r.pos + i * value_width, value_width) {
                    Some(delta) => min + delta as f64,
                    None => f64::NAN,
                });
            }
            r.pos += n * value_width;
        }
        other => {
            let shown = other.map(|code| code.to_string()).unwrap_or_else(|| "undefined".to_string());
            return Err(Error::new(format!("Unsupported Rangefind doc-value sort value encoding {shown}")));
        }
    }
    let boolean = field.kind == "boolean";
    let mut rows = Vec::with_capacity(values.len());
    for (i, sort_value) in values.into_iter().enumerate() {
        let doc = fixed_int(bytes, r.pos + i * doc_width, doc_width).unwrap_or(0);
        let value = if boolean { DocValue::Bool(sort_value == 2.0) } else { DocValue::Number(sort_value) };
        rows.push(SortRow { doc, value, sort_value, rank: rank_start + i as u64 });
    }
    r.pos += n * doc_width;
    if r.pos != bytes.len() {
        return Err(Error::new("Rangefind doc-value sort page has trailing bytes."));
    }
    Ok(SortPage { field, rank_start, count, rows })
}
