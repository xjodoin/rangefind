//! Precomputed query bundles (`rfqbundle-v1`) — `parseQueryBundle` from
//! `src/query_bundle_codec.js`.

use std::collections::BTreeMap;

use super::binary::{check_magic, QUERY_BUNDLE_MAGIC};
use super::postings::{read_block_filter_summary, read_f64_checked, BlockFilterDef, BlockFilters, PostingManifest};
use super::read_utf8;
use crate::error::{Error, Result};
use crate::util::bytes::Reader;

pub const QUERY_BUNDLE_FORMAT: &str = "rfqbundle-v1";
const QUERY_BUNDLE_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq)]
pub struct QueryBundleRowGroup {
    pub row_start: u64,
    pub row_count: u64,
    pub score_max: u64,
    pub score_min: u64,
    pub doc_min: u64,
    pub doc_max: u64,
    pub filters: BlockFilters,
}

/// A per-row filter value (`readFilterValue`).
#[derive(Clone, Debug, PartialEq)]
pub enum BundleFilterValue {
    /// `{ codes }`
    Facet(Vec<u64>),
    /// `null` / `true` / `false`
    Boolean(Option<bool>),
    /// `null` or the number (float64 for `type: "float"`, else a signed varint).
    Number(Option<f64>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryBundle {
    pub key: String,
    pub base_terms: Vec<String>,
    pub expanded_terms: Vec<String>,
    pub total: u64,
    pub complete: bool,
    pub next_score_bound: u64,
    pub next_tie_doc: Option<u64>,
    /// `[doc, score]` rows.
    pub rows: Vec<(u64, u64)>,
    pub row_groups: Vec<QueryBundleRowGroup>,
    /// `filterValues[field][doc]`, fields in object order; docs ascending
    /// (JS integer-keyed objects enumerate numerically; a repeated doc keeps
    /// the last value).
    pub filter_values: Vec<(String, BTreeMap<u64, BundleFilterValue>)>,
}

impl QueryBundle {
    pub fn filter_value(&self, field: &str, doc: u64) -> Option<&BundleFilterValue> {
        self.filter_values.iter().find(|(name, _)| name == field).and_then(|(_, values)| values.get(&doc))
    }
}

fn read_signed_varint(r: &mut Reader) -> f64 {
    let n = r.varint() as f64;
    if n % 2.0 != 0.0 {
        -((n + 1.0) / 2.0)
    } else {
        n / 2.0
    }
}

fn read_filter_value(r: &mut Reader, filter: &BlockFilterDef) -> Result<BundleFilterValue> {
    if filter.kind == "facet" {
        let count = r.varint_usize();
        let mut codes = Vec::with_capacity(count.min(1 << 16));
        for _ in 0..count {
            codes.push(r.varint());
        }
        return Ok(BundleFilterValue::Facet(codes));
    }
    if filter.kind == "boolean" {
        let code = r.varint();
        return Ok(BundleFilterValue::Boolean(if code == 0 { None } else { Some(code == 2) }));
    }
    if r.varint() == 0 {
        return Ok(BundleFilterValue::Number(None));
    }
    let type_ = if filter.type_.is_empty() { "int".to_string() } else { filter.type_.to_lowercase() };
    if type_ == "float" {
        Ok(BundleFilterValue::Number(Some(read_f64_checked(r)?)))
    } else {
        Ok(BundleFilterValue::Number(Some(read_signed_varint(r))))
    }
}

/// `parseQueryBundle(buffer, manifest)`.
pub fn parse_query_bundle(bytes: &[u8], manifest: &PostingManifest) -> Result<QueryBundle> {
    check_magic(bytes, QUERY_BUNDLE_MAGIC, "Unsupported Rangefind query bundle")?;
    let mut r = Reader::at(bytes, QUERY_BUNDLE_MAGIC.len());
    let version = r.varint();
    if version != QUERY_BUNDLE_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind query bundle version {version}")));
    }
    let key = read_utf8(&mut r);
    let base_count = r.varint_usize();
    let base_terms = (0..base_count).map(|_| read_utf8(&mut r)).collect();
    let expanded_count = r.varint_usize();
    let expanded_terms = (0..expanded_count).map(|_| read_utf8(&mut r)).collect();
    let total = r.varint();
    let complete = r.varint() == 1;
    let next_score_bound = r.varint();
    let next_tie_doc = if r.varint() == 1 { Some(r.varint()) } else { None };
    let row_count = r.varint_usize();
    let mut rows = Vec::with_capacity(row_count.min(1 << 20));
    for _ in 0..row_count {
        let doc = r.varint();
        let score = r.varint();
        rows.push((doc, score));
    }
    let defs = &manifest.block_filters;
    let group_count = if r.pos < bytes.len() { r.varint_usize() } else { 0 };
    let mut row_groups = Vec::with_capacity(group_count.min(1 << 20));
    for _ in 0..group_count {
        let row_start = r.varint();
        let row_count = r.varint();
        let score_max = r.varint();
        let score_min = r.varint();
        let doc_min = r.varint();
        let doc_max = r.varint();
        let filters = read_block_filter_summary(&mut r, defs)?;
        row_groups.push(QueryBundleRowGroup { row_start, row_count, score_max, score_min, doc_min, doc_max, filters });
    }
    let mut filter_values: Vec<(String, BTreeMap<u64, BundleFilterValue>)> = Vec::new();
    let field_count = if r.pos < bytes.len() { r.varint_usize() } else { 0 };
    for _ in 0..field_count {
        let filter = defs.get(r.varint_usize());
        let mut values = BTreeMap::new();
        for row in rows.iter() {
            if let Some(filter) = filter {
                let value = read_filter_value(&mut r, filter)?;
                values.insert(row.0, value);
            }
        }
        if let Some(filter) = filter {
            match filter_values.iter_mut().find(|(name, _)| *name == filter.name) {
                Some(slot) => slot.1 = values,
                None => filter_values.push((filter.name.clone(), values)),
            }
        }
    }
    Ok(QueryBundle { key, base_terms, expanded_terms, total, complete, next_score_bound, next_tie_doc, rows, row_groups, filter_values })
}
