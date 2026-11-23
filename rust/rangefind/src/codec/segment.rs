//! Published build segments (`rfsegmentterms-v1`) — `parseSegmentTerms` and
//! `decodeSegmentRows` from `src/segment_codec.js`.

use std::collections::HashMap;

use super::binary::{check_magic, SEGMENT_TERMS_MAGIC};
use super::{decode_utf8, js_round, to_int32};
use crate::error::{Error, Result};
use crate::util::bytes::Reader;

pub const SEGMENT_TERMS_FORMAT: &str = "rfsegmentterms-v1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentTermEntry {
    pub term: String,
    pub offset: u64,
    pub bytes: u64,
    pub df: u64,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SegmentTerms {
    /// Terms in file (Map insertion) order.
    pub terms: Vec<SegmentTermEntry>,
    index: HashMap<String, usize>,
}

impl SegmentTerms {
    pub fn format(&self) -> &'static str {
        SEGMENT_TERMS_FORMAT
    }

    pub fn get(&self, term: &str) -> Option<&SegmentTermEntry> {
        self.index.get(term).map(|&i| &self.terms[i])
    }
}

/// The strict `readVarint` of this module: truncation throws.
fn strict_varint(r: &mut Reader) -> Result<u64> {
    let mut value = 0f64;
    let mut multiplier = 1f64;
    while r.pos < r.bytes.len() {
        let byte = r.bytes[r.pos];
        r.pos += 1;
        value += (byte & 0x7f) as f64 * multiplier;
        if byte & 0x80 == 0 {
            return Ok(value as u64);
        }
        multiplier *= 128.0;
    }
    Err(Error::new("Truncated Rangefind segment varint."))
}

fn strict_utf8(r: &mut Reader) -> Result<String> {
    let length = strict_varint(r)? as usize;
    if r.pos.saturating_add(length) > r.bytes.len() {
        return Err(Error::new("Truncated Rangefind segment string."));
    }
    let value = decode_utf8(&r.bytes[r.pos..r.pos + length]);
    r.pos += length;
    Ok(value)
}

/// `parseSegmentTerms(buffer)`.
pub fn parse_segment_terms(bytes: &[u8]) -> Result<SegmentTerms> {
    check_magic(bytes, SEGMENT_TERMS_MAGIC, "Unsupported Rangefind segment terms file.")?;
    let mut r = Reader::at(bytes, SEGMENT_TERMS_MAGIC.len());
    let count = strict_varint(&mut r)? as usize;
    let mut terms: Vec<SegmentTermEntry> = Vec::with_capacity(count.min(1 << 20));
    let mut index: HashMap<String, usize> = HashMap::with_capacity(count.min(1 << 20));
    for _ in 0..count {
        let term = strict_utf8(&mut r)?;
        let offset = strict_varint(&mut r)?;
        let length = strict_varint(&mut r)?;
        let df = strict_varint(&mut r)?;
        let rows = strict_varint(&mut r)?;
        let entry = SegmentTermEntry { term, offset, bytes: length, df, count: rows };
        match index.get(&entry.term) {
            Some(&slot) => terms[slot] = entry,
            None => {
                index.insert(entry.term.clone(), terms.len());
                terms.push(entry);
            }
        }
    }
    Ok(SegmentTerms { terms, index })
}

/// `segmentImpact(score, df, total)`.
pub fn segment_impact(score: f64, df: f64, total: f64) -> f64 {
    let idf = (1.0 + (total - df + 0.5) / (df + 0.5)).ln();
    js_round(score * idf / 10.0).max(1.0)
}

/// `Math.max(1, Number(value || fallback))` for the `options` numbers.
fn at_least_one(value: Option<f64>, fallback: f64) -> f64 {
    let n = match value {
        Some(v) if v != 0.0 && !v.is_nan() => v,
        _ => fallback,
    };
    // Math.max(1, NaN) is NaN.
    if n.is_nan() {
        n
    } else {
        n.max(1.0)
    }
}

/// `decodeSegmentRows(buffer, entry, { df, total })`: rows of
/// `[doc, impact]` with impacts rescored by the given corpus statistics.
pub fn decode_segment_rows(bytes: &[u8], entry: &SegmentTermEntry, df: Option<f64>, total: Option<f64>) -> Result<Vec<i32>> {
    let total = at_least_one(total, 1.0);
    let entry_df = if entry.df != 0 { entry.df } else if entry.count != 0 { entry.count } else { 1 };
    let df = at_least_one(df, entry_df as f64);
    let mut r = Reader::new(bytes);
    let len = (entry.count as usize).saturating_mul(2);
    let mut rows = vec![0i32; len];
    let mut row = 0;
    while row < len {
        rows[row] = to_int32(strict_varint(&mut r)? as f64);
        rows[row + 1] = to_int32(segment_impact(strict_varint(&mut r)? as f64, df, total));
        row += 2;
    }
    if r.pos != bytes.len() {
        return Err(Error::new(format!("Rangefind segment term {} has trailing bytes.", entry.term)));
    }
    Ok(rows)
}
