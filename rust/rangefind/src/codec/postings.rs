//! Posting segments (`rfsegpost-v6`) and posting block codecs — the decode
//! half of `src/codec.js` (`parsePostingSegment`, `readBlockFilterSummary`,
//! `decodePostingBytes`, `lookupPostingBytes`, `lookupDecodedPostingRows`,
//! `decodePostingBlock`, `lookupPostingBlock`, `decodePostings`).
//!
//! Posting rows are flat `[doc, impact, doc, impact, ...]` vectors, exactly
//! like the JS `Int32Array`s (values go through `ToInt32`).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::rc::Rc;

use serde_json::Value;

use super::binary::{check_magic, POSTING_SEGMENT_MAGIC};
use super::{byte_at, default_pack_file, read_utf8, string_table, to_int32};
use crate::error::{Error, Result};
use crate::pointer::{Checksum, Pointer};
use crate::util::bytes::{Bytes, Reader};
use crate::util::json;

pub const POSTING_SEGMENT_FORMAT: &str = "rfsegpost-v6";
const POSTING_SEGMENT_FORMAT_VERSION: u64 = 6;

pub const POSTING_BLOCK_CODEC_PAIR_VARINT: &str = "pair-varint-v1";
pub const POSTING_BLOCK_CODEC_IMPACT_RUNS: &str = "impact-runs-v1";
pub const POSTING_BLOCK_CODEC_IMPACT_BITSET: &str = "impact-bitset-v1";
pub const POSTING_BLOCK_CODEC_PARTITIONED_DELTAS: &str = "partitioned-deltas-v1";

/// Posting block codec (`block.codec`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum BlockCodec {
    #[default]
    PairVarint,
    ImpactRuns,
    ImpactBitset,
    PartitionedDeltas,
}

impl BlockCodec {
    /// `postingBlockCodecName(code)`: unknown codes fall back to pair-varint.
    pub fn from_code(code: u64) -> BlockCodec {
        match code {
            1 => BlockCodec::ImpactRuns,
            2 => BlockCodec::ImpactBitset,
            3 => BlockCodec::PartitionedDeltas,
            _ => BlockCodec::PairVarint,
        }
    }

    /// Codec from its name (`block?.codec || pair-varint`).
    pub fn from_name(name: &str) -> BlockCodec {
        match name {
            POSTING_BLOCK_CODEC_IMPACT_RUNS => BlockCodec::ImpactRuns,
            POSTING_BLOCK_CODEC_IMPACT_BITSET => BlockCodec::ImpactBitset,
            POSTING_BLOCK_CODEC_PARTITIONED_DELTAS => BlockCodec::PartitionedDeltas,
            _ => BlockCodec::PairVarint,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            BlockCodec::PairVarint => POSTING_BLOCK_CODEC_PAIR_VARINT,
            BlockCodec::ImpactRuns => POSTING_BLOCK_CODEC_IMPACT_RUNS,
            BlockCodec::ImpactBitset => POSTING_BLOCK_CODEC_IMPACT_BITSET,
            BlockCodec::PartitionedDeltas => POSTING_BLOCK_CODEC_PARTITIONED_DELTAS,
        }
    }
}

// ---------------------------------------------------------------------------
// Block filter summaries
// ---------------------------------------------------------------------------

/// One entry of `manifest.block_filters`.
#[derive(Clone, Debug, PartialEq)]
pub struct BlockFilterDef {
    pub name: String,
    /// `"facet"`, `"number"`, anything else reads as min/max varints.
    pub kind: String,
    pub type_: String,
    /// Facet summary words (`filter.words`).
    pub words: usize,
}

impl BlockFilterDef {
    pub fn from_json(value: &Value) -> BlockFilterDef {
        BlockFilterDef {
            name: value.get("name").map(property_key).unwrap_or_else(|| "undefined".to_string()),
            kind: value.get("kind").and_then(Value::as_str).unwrap_or("").to_string(),
            type_: value.get("type").and_then(Value::as_str).unwrap_or("").to_string(),
            words: {
                let n = json::to_number(json::path(value, &["words"]));
                if n.is_finite() && n > 0.0 {
                    n.ceil() as usize
                } else {
                    0
                }
            },
        }
    }

    pub fn is_facet(&self) -> bool {
        self.kind == "facet"
    }

    pub fn is_number(&self) -> bool {
        self.kind == "number"
    }
}

/// `String(value)` as used for an object property key.
pub(crate) fn property_key(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => "null".to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => json::js_number_to_string(number.as_f64().unwrap_or(f64::NAN)),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

/// Parse `manifest.block_filters` (non-arrays read as empty).
pub fn block_filter_defs(manifest: &Value) -> Rc<[BlockFilterDef]> {
    match manifest.get("block_filters").and_then(Value::as_array) {
        Some(items) => items.iter().map(BlockFilterDef::from_json).collect(),
        None => Rc::from(Vec::new()),
    }
}

/// A block/superblock/row-group filter summary for one field.
#[derive(Clone, Debug, PartialEq)]
pub enum FilterSummary {
    /// Facet presence bitset words (`{ words }`).
    Facet { words: Vec<u64> },
    /// Numeric range (`{ min, max }`, null when the block has no values).
    Number { min: Option<f64>, max: Option<f64> },
    /// Boolean (and any other kind): min/max codes as varints (0 = none,
    /// 1 = false, 2 = true).
    Boolean { min: u64, max: u64 },
}

/// Summaries keyed by field name — the JS `filters` object. Values are kept
/// aligned with the manifest's filter list; [`BlockFilters::get`] resolves a
/// name the way JS property access does (a repeated name keeps its last
/// value).
#[derive(Clone, Debug, PartialEq)]
pub struct BlockFilters {
    pub defs: Rc<[BlockFilterDef]>,
    pub values: Vec<FilterSummary>,
}

impl BlockFilters {
    pub fn empty() -> BlockFilters {
        BlockFilters { defs: Rc::from(Vec::new()), values: Vec::new() }
    }

    pub fn get(&self, field: &str) -> Option<&FilterSummary> {
        (0..self.values.len()).rev().find(|&i| self.defs[i].name == field).map(|i| &self.values[i])
    }

    /// `Object.entries(filters)`: first-occurrence order, last value wins.
    pub fn entries(&self) -> Vec<(&str, &FilterSummary)> {
        let mut out: Vec<(&str, &FilterSummary)> = Vec::new();
        for (i, value) in self.values.iter().enumerate() {
            let name = self.defs[i].name.as_str();
            match out.iter_mut().find(|(key, _)| *key == name) {
                Some(slot) => slot.1 = value,
                None => out.push((name, value)),
            }
        }
        out
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

pub(crate) fn data_view_error() -> Error {
    Error::new("Offset is outside the bounds of the DataView")
}

/// `new DataView(buffer, pos, 8).getFloat64(0, true)`: the constructor
/// throws when the 8 bytes are not there.
pub(crate) fn f64_view_at(bytes: &[u8], pos: usize) -> Result<f64> {
    if pos > bytes.len() {
        return Err(Error::new(format!("Start offset {pos} is outside the bounds of the buffer")));
    }
    if pos + 8 > bytes.len() {
        return Err(Error::new("Invalid DataView length 8"));
    }
    Ok(f64::from_le_bytes(bytes[pos..pos + 8].try_into().expect("8 bytes")))
}

/// `readFloat64(bytes, state)` from `src/codec.js`.
pub(crate) fn read_f64_checked(reader: &mut Reader) -> Result<f64> {
    let value = f64_view_at(reader.bytes, reader.pos)?;
    reader.pos += 8;
    Ok(value)
}

/// `readBlockFilterSummary(bytes, state, manifest)`.
pub fn read_block_filter_summary(reader: &mut Reader, defs: &Rc<[BlockFilterDef]>) -> Result<BlockFilters> {
    let mut values = Vec::with_capacity(defs.len());
    for def in defs.iter() {
        if def.is_facet() {
            let mut words = Vec::with_capacity(def.words);
            for _ in 0..def.words {
                words.push(reader.varint());
            }
            values.push(FilterSummary::Facet { words });
        } else if def.is_number() {
            let has_values = byte_at(reader.bytes, reader.pos) == Some(1);
            reader.pos += 1;
            if has_values {
                let min = read_f64_checked(reader)?;
                let max = read_f64_checked(reader)?;
                values.push(FilterSummary::Number { min: Some(min), max: Some(max) });
            } else {
                values.push(FilterSummary::Number { min: None, max: None });
            }
        } else {
            let min = reader.varint();
            let max = reader.varint();
            values.push(FilterSummary::Boolean { min, max });
        }
    }
    Ok(BlockFilters { defs: defs.clone(), values })
}

// ---------------------------------------------------------------------------
// Segment structure
// ---------------------------------------------------------------------------

/// The parts of the index manifest `parsePostingSegment` reads.
#[derive(Clone, Debug, PartialEq)]
pub struct PostingManifest {
    pub block_filters: Rc<[BlockFilterDef]>,
    /// `object_store.pack_table.postingBlocks || object_store.packs.postingBlocks`.
    pub posting_block_packs: Vec<String>,
    /// `object_store.pointer_format === "rfbp-v1" || features.checksummedObjects`.
    pub object_pointers: bool,
}

impl Default for PostingManifest {
    fn default() -> Self {
        PostingManifest { block_filters: Rc::from(Vec::new()), posting_block_packs: Vec::new(), object_pointers: false }
    }
}

impl PostingManifest {
    pub fn from_manifest(manifest: &Value) -> PostingManifest {
        let store = json::path(manifest, &["object_store"]);
        let table = {
            let primary = json::path(store, &["pack_table", "postingBlocks"]);
            if json::truthy(primary) {
                primary
            } else {
                json::path(store, &["packs", "postingBlocks"])
            }
        };
        let object_pointers = json::path(store, &["pointer_format"]).as_str() == Some("rfbp-v1")
            || json::truthy(json::path(manifest, &["features", "checksummedObjects"]));
        PostingManifest { block_filters: block_filter_defs(manifest), posting_block_packs: string_table(table), object_pointers }
    }

    fn block_pack(&self, index: u64) -> String {
        match self.posting_block_packs.get(index as usize) {
            Some(file) if !file.is_empty() => file.clone(),
            _ => default_pack_file(index),
        }
    }
}

/// A doc-range impact bound (`{ index, maxImpact, quantizedMaxImpact }`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DocRangeBound {
    pub index: i64,
    pub max_impact: i64,
    pub quantized_max_impact: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocRanges {
    pub range_size: i64,
    pub scale: i64,
    pub range_count: i64,
    pub quantization_bits: i64,
    pub ranges: Vec<DocRangeBound>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ImpactTier {
    pub max_impact: i64,
    pub first: usize,
    pub count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImpactTiers {
    /// Block indexes in impact order (an `Int32Array` in JS).
    pub blocks: Vec<i32>,
    pub tiers: Vec<ImpactTier>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PostingBlock {
    pub offset: usize,
    pub row_count: u64,
    pub max_impact: i64,
    pub max_impact_doc: i64,
    pub doc_min: i64,
    pub doc_max: i64,
    pub codec: BlockCodec,
    pub filters: BlockFilters,
    /// External block location (`block.range`; `physicalLength` = `length`).
    pub range: Option<Pointer>,
    /// Per-block doc-range bounds; None unless the entry carries doc ranges.
    pub doc_ranges: Option<Vec<DocRangeBound>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Superblock {
    pub first_block: usize,
    pub block_count: usize,
    pub row_count: u64,
    pub max_impact: i64,
    pub max_impact_doc: i64,
    pub doc_min: i64,
    pub doc_max: i64,
    pub filters: BlockFilters,
}

/// A term's posting entry. Shared (`Rc`) and mutated by the runtime through
/// the interior caches, mirroring the JS object the runtime decorates.
#[derive(Debug)]
pub struct TermEntry {
    /// Process-unique identity, never reused. Caches of per-entry objects
    /// (external posting blocks) key on it: an entry's address is not an
    /// identity once its segment can be evicted and the allocation reused
    /// by another term's entry.
    pub uid: u64,
    pub df: u64,
    pub count: u64,
    pub offset: usize,
    pub byte_length: usize,
    pub block_size: u64,
    pub external: bool,
    pub blocks: Vec<PostingBlock>,
    pub superblocks: Vec<Superblock>,
    pub doc_ranges: Option<DocRanges>,
    pub impact_tiers: Option<ImpactTiers>,
    /// `entry.blockPostings` for inline blocks (filled by [`decode_posting_block`]).
    pub block_postings: RefCell<HashMap<usize, Rc<Vec<i32>>>>,
    /// `entry.postings` (filled by [`decode_postings`]).
    pub postings: RefCell<Option<Rc<Vec<i32>>>>,
}

impl TermEntry {
    pub fn format(&self) -> &'static str {
        POSTING_SEGMENT_FORMAT
    }
}

/// A parsed posting segment: `{ format, bytes, dataStart, terms }`.
#[derive(Debug)]
pub struct PostingSegment {
    pub bytes: Bytes,
    pub data_start: usize,
    /// Terms in file (Map insertion) order.
    pub terms: Vec<(String, Rc<TermEntry>)>,
    index: HashMap<String, usize>,
    /// Bytes of decoded rows memoised in the entries' interior caches
    /// (`block_postings` / `postings`) since parsing, for cache budgets.
    pub decoded_bytes: Cell<usize>,
}

impl PostingSegment {
    pub fn format(&self) -> &'static str {
        POSTING_SEGMENT_FORMAT
    }

    pub fn term(&self, term: &str) -> Option<Rc<TermEntry>> {
        self.index.get(term).map(|&i| self.terms[i].1.clone())
    }

    pub fn len(&self) -> usize {
        self.terms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }
}

fn i64v(reader: &mut Reader) -> i64 {
    reader.varint() as i64
}

/// `parsePostingSegment(buffer, manifest)`.
pub fn parse_posting_segment(bytes: Bytes, manifest: &PostingManifest) -> Result<PostingSegment> {
    check_magic(&bytes, POSTING_SEGMENT_MAGIC, "Unsupported Rangefind posting segment")?;
    let data = bytes.clone();
    let mut r = Reader::at(&data, POSTING_SEGMENT_MAGIC.len());
    if r.varint() != POSTING_SEGMENT_FORMAT_VERSION {
        return Err(Error::new("Unsupported Rangefind posting segment version"));
    }
    let defs = &manifest.block_filters;
    let term_count = r.varint_usize();
    let mut terms: Vec<(String, Rc<TermEntry>)> = Vec::with_capacity(term_count.min(1 << 20));
    let mut index: HashMap<String, usize> = HashMap::with_capacity(term_count.min(1 << 20));
    for _ in 0..term_count {
        let term = read_utf8(&mut r);
        let df = r.varint();
        let count = r.varint();
        let offset = r.varint_usize();
        let byte_length = r.varint_usize();
        let block_size = r.varint();
        let block_count = r.varint_usize();
        let external = r.varint() == 1;
        let mut blocks = Vec::with_capacity(block_count.min(1 << 20));
        for _ in 0..block_count {
            let offset = r.varint_usize();
            let row_count = r.varint();
            let max_impact = i64v(&mut r);
            let max_impact_doc = i64v(&mut r);
            let doc_min = i64v(&mut r);
            let doc_max = i64v(&mut r);
            let codec = BlockCodec::from_code(r.varint());
            let filters = read_block_filter_summary(&mut r, defs)?;
            let range = if external {
                let pack_index = r.varint();
                let mut pointer = Pointer::new(manifest.block_pack(pack_index), 0, 0);
                pointer.offset = r.varint();
                pointer.length = r.varint();
                if manifest.object_pointers {
                    let logical_length = r.varint();
                    let algorithm = read_utf8(&mut r);
                    let value = read_utf8(&mut r);
                    pointer.logical_length = if logical_length != 0 { Some(logical_length) } else { None };
                    pointer.checksum = if value.is_empty() {
                        None
                    } else {
                        Some(Checksum {
                            algorithm: Rc::from(if algorithm.is_empty() { "sha256" } else { algorithm.as_str() }),
                            value: Rc::from(value.as_str()),
                        })
                    };
                }
                Some(pointer)
            } else {
                None
            };
            let range_count = r.varint_usize();
            let doc_ranges = if range_count > 0 {
                let mut ranges = Vec::with_capacity(range_count.min(1 << 20));
                let mut previous: i64 = -1;
                for _ in 0..range_count {
                    let index = previous + 1 + i64v(&mut r);
                    let quantized = i64v(&mut r);
                    ranges.push(DocRangeBound { index, max_impact: 0, quantized_max_impact: quantized });
                    previous = index;
                }
                Some(ranges)
            } else {
                None
            };
            blocks.push(PostingBlock {
                offset,
                row_count,
                max_impact,
                max_impact_doc,
                doc_min,
                doc_max,
                codec,
                filters,
                range,
                doc_ranges,
            });
        }
        let superblock_count = r.varint_usize();
        let mut superblocks = Vec::with_capacity(superblock_count.min(1 << 20));
        for _ in 0..superblock_count {
            let first_block = r.varint_usize();
            let block_count = r.varint_usize();
            let row_count = r.varint();
            let max_impact = i64v(&mut r);
            let max_impact_doc = i64v(&mut r);
            let doc_min = i64v(&mut r);
            let doc_max = i64v(&mut r);
            let filters = read_block_filter_summary(&mut r, defs)?;
            superblocks.push(Superblock { first_block, block_count, row_count, max_impact, max_impact_doc, doc_min, doc_max, filters });
        }
        let doc_ranges = if r.varint() == 1 {
            let range_size = i64v(&mut r);
            let scale = i64v(&mut r);
            let range_count = i64v(&mut r);
            let quantization_bits = i64v(&mut r);
            let count = r.varint_usize();
            let mut ranges = Vec::with_capacity(count.min(1 << 20));
            let mut previous: i64 = -1;
            for _ in 0..count {
                let index = previous + 1 + i64v(&mut r);
                let quantized = i64v(&mut r);
                ranges.push(DocRangeBound { index, max_impact: scaled(quantized, scale), quantized_max_impact: quantized });
                previous = index;
            }
            for block in blocks.iter_mut() {
                if let Some(bounds) = block.doc_ranges.as_mut() {
                    for bound in bounds.iter_mut() {
                        bound.max_impact = scaled(bound.quantized_max_impact, scale);
                    }
                }
            }
            Some(DocRanges { range_size, scale, range_count, quantization_bits, ranges })
        } else {
            for block in blocks.iter_mut() {
                block.doc_ranges = None;
            }
            None
        };
        let tier_block_count = r.varint_usize();
        let impact_tiers = if tier_block_count > 0 {
            let tier_count = r.varint_usize();
            let mut tiers = Vec::with_capacity(tier_count.min(1 << 20));
            for _ in 0..tier_count {
                let max_impact = i64v(&mut r);
                let first = r.varint_usize();
                let count = r.varint_usize();
                tiers.push(ImpactTier { max_impact, first, count });
            }
            let mut tier_blocks = Vec::with_capacity(tier_block_count.min(1 << 20));
            for _ in 0..tier_block_count {
                tier_blocks.push(r.varint() as i32);
            }
            Some(ImpactTiers { blocks: tier_blocks, tiers })
        } else {
            None
        };
        let entry = Rc::new(TermEntry {
            uid: next_term_entry_uid(),
            df,
            count,
            offset,
            byte_length,
            block_size,
            external,
            blocks,
            superblocks,
            doc_ranges,
            impact_tiers,
            block_postings: RefCell::new(HashMap::new()),
            postings: RefCell::new(None),
        });
        match index.get(&term) {
            Some(&slot) => terms[slot].1 = entry,
            None => {
                index.insert(term.clone(), terms.len());
                terms.push((term, entry));
            }
        }
    }
    let data_start = r.pos;
    Ok(PostingSegment { bytes, data_start, terms, index, decoded_bytes: Cell::new(0) })
}

/// Next [`TermEntry::uid`]. Single counter for the process; 2^64 entries
/// never wrap.
pub fn next_term_entry_uid() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl PostingSegment {
    /// Account `rows` decoded into an entry's interior cache.
    pub fn note_decoded_rows(&self, rows: usize) {
        self.decoded_bytes.set(self.decoded_bytes.get() + rows * std::mem::size_of::<i32>() + 48);
    }
}

fn scaled(quantized: i64, scale: i64) -> i64 {
    (quantized as f64 * scale as f64) as i64
}

// ---------------------------------------------------------------------------
// Block codecs
// ---------------------------------------------------------------------------

/// Doc sets the lookup functions accept (`postingTargetSet(docs)`).
pub trait PostingTargets {
    fn target_count(&self) -> usize;
    /// `targets.has(doc)` for a decoded (non-negative) doc id.
    fn has_doc(&self, doc: i64) -> bool;
    /// `[...targets].filter(Number.isFinite).sort((a, b) => a - b)`.
    fn sorted_docs(&self) -> Vec<i64>;
}

macro_rules! posting_targets_for_set {
    ($set:ident, $t:ty) => {
        impl PostingTargets for $set<$t> {
            fn target_count(&self) -> usize {
                self.len()
            }
            fn has_doc(&self, doc: i64) -> bool {
                <$t>::try_from(doc).map(|value| self.contains(&value)).unwrap_or(false)
            }
            fn sorted_docs(&self) -> Vec<i64> {
                let mut docs: Vec<i64> = self.iter().map(|&doc| doc as i64).collect();
                docs.sort_unstable();
                docs
            }
        }
    };
}

posting_targets_for_set!(HashSet, i32);
posting_targets_for_set!(HashSet, u32);
posting_targets_for_set!(HashSet, i64);
posting_targets_for_set!(HashSet, usize);
posting_targets_for_set!(BTreeSet, i32);
posting_targets_for_set!(BTreeSet, u32);
posting_targets_for_set!(BTreeSet, i64);

#[inline]
fn wrap(value: i64) -> i32 {
    value as i32
}

fn decode_pair_varint(bytes: &[u8]) -> Vec<i32> {
    let mut r = Reader::new(bytes);
    let mut rows = Vec::with_capacity(bytes.len());
    while r.pos < bytes.len() {
        let doc = r.varint();
        let impact = r.varint();
        rows.push(doc as i32);
        rows.push(impact as i32);
    }
    rows
}

fn decode_impact_runs(bytes: &[u8]) -> Vec<i32> {
    let mut r = Reader::new(bytes);
    let mut rows = Vec::with_capacity(bytes.len() * 2);
    let groups = r.varint();
    for _ in 0..groups {
        let impact = r.varint() as i32;
        let count = r.varint();
        let mut previous: i64 = -1;
        for _ in 0..count {
            let doc = previous.wrapping_add(r.varint() as i64);
            rows.push(wrap(doc));
            rows.push(impact);
            previous = doc;
        }
    }
    rows
}

fn decode_impact_bitset(bytes: &[u8]) -> Vec<i32> {
    let mut r = Reader::new(bytes);
    let mut rows = Vec::new();
    let groups = r.varint();
    for _ in 0..groups {
        let impact = r.varint() as i32;
        let min_doc = r.varint() as i64;
        let span = r.varint();
        let byte_length = r.varint_usize();
        let start = r.pos;
        r.pos = r.pos.saturating_add(byte_length);
        // Bits past the end of the buffer read as `undefined & mask` = 0.
        let available = (bytes.len().saturating_sub(start) as u64).saturating_mul(8);
        for bit in 0..span.min(available) {
            let byte = bytes[start + (bit / 8) as usize];
            if byte & (1u8 << (bit % 8)) != 0 {
                rows.push(wrap(min_doc + bit as i64));
                rows.push(impact);
            }
        }
    }
    rows
}

/// `readPackedUnsigned`: `count` little-endian bit-packed values of `width`
/// bits starting at `pos`; the cursor advances by `byte_length` regardless of
/// what was consumed (reads continue into following bytes, zeros past the end).
fn read_packed_unsigned(source: &[u8], pos: &mut usize, count: u64, width: u64, byte_length: usize) -> Vec<f64> {
    let start = *pos;
    *pos = pos.saturating_add(byte_length);
    if count == 0 {
        return Vec::new();
    }
    if width == 0 {
        return vec![0.0; count as usize];
    }
    let mut values = Vec::with_capacity(count.min(1 << 24) as usize);
    let mut byte_offset = start;
    let next = |offset: &mut usize| -> u64 {
        let value = source.get(*offset).copied().unwrap_or(0) as u64;
        *offset += 1;
        value
    };
    let mut current = next(&mut byte_offset);
    let mut consumed: u64 = 0;
    let exact = width <= 53;
    for i in 0..count {
        let mut int_value: u64 = 0;
        let mut float_value = 0f64;
        let mut filled: u64 = 0;
        while filled < width {
            let available = 8 - consumed;
            let take = available.min(width - filled);
            let chunk = (current >> consumed) & ((1u64 << take) - 1);
            if exact {
                int_value |= chunk << filled;
            } else {
                float_value += chunk as f64 * 2f64.powi(filled.min(i32::MAX as u64) as i32);
            }
            consumed += take;
            filled += take;
            if consumed == 8 && filled < width {
                current = next(&mut byte_offset);
                consumed = 0;
            }
        }
        values.push(if exact { int_value as f64 } else { float_value });
        if consumed == 8 && i < count - 1 {
            current = next(&mut byte_offset);
            consumed = 0;
        }
    }
    values
}

fn decode_partitioned_deltas(bytes: &[u8]) -> Vec<i32> {
    let mut r = Reader::new(bytes);
    let mut rows = Vec::with_capacity(bytes.len() * 2);
    let groups = r.varint();
    for _ in 0..groups {
        let impact = r.varint() as i32;
        let count = r.varint();
        let mut doc = r.varint() as f64;
        let width = r.varint();
        let byte_length = r.varint_usize();
        rows.push(to_int32(doc));
        rows.push(impact);
        let mut pos = r.pos;
        for delta in read_packed_unsigned(bytes, &mut pos, count.saturating_sub(1), width, byte_length) {
            doc += delta;
            rows.push(to_int32(doc));
            rows.push(impact);
        }
        r.pos = pos;
    }
    rows
}

/// `decodePostingBytes(bytes, block)`.
pub fn decode_posting_bytes(bytes: &[u8], codec: BlockCodec) -> Vec<i32> {
    match codec {
        BlockCodec::PartitionedDeltas => decode_partitioned_deltas(bytes),
        BlockCodec::ImpactBitset => decode_impact_bitset(bytes),
        BlockCodec::ImpactRuns => decode_impact_runs(bytes),
        BlockCodec::PairVarint => decode_pair_varint(bytes),
    }
}

#[inline]
fn push_matched<T: PostingTargets + ?Sized>(rows: &mut Vec<i32>, targets: &T, doc: i64, impact: u64) {
    if targets.has_doc(doc) {
        rows.push(wrap(doc));
        rows.push(impact as i32);
    }
}

fn lookup_pair_varint<T: PostingTargets + ?Sized>(bytes: &[u8], targets: &T) -> (Vec<i32>, usize) {
    let mut r = Reader::new(bytes);
    let mut rows = Vec::new();
    let mut scanned = 0;
    while r.pos < bytes.len() {
        let doc = r.varint() as i64;
        let impact = r.varint();
        scanned += 1;
        push_matched(&mut rows, targets, doc, impact);
    }
    (rows, scanned)
}

fn lookup_impact_runs<T: PostingTargets + ?Sized>(bytes: &[u8], targets: &T) -> (Vec<i32>, usize) {
    let mut r = Reader::new(bytes);
    let mut rows = Vec::new();
    let mut scanned = 0;
    let groups = r.varint();
    for _ in 0..groups {
        let impact = r.varint();
        let count = r.varint();
        let mut previous: i64 = -1;
        for _ in 0..count {
            let doc = previous.wrapping_add(r.varint() as i64);
            scanned += 1;
            push_matched(&mut rows, targets, doc, impact);
            previous = doc;
        }
    }
    (rows, scanned)
}

fn lookup_impact_bitset<T: PostingTargets + ?Sized>(bytes: &[u8], targets: &T) -> (Vec<i32>, usize) {
    let target_docs = targets.sorted_docs();
    let mut r = Reader::new(bytes);
    let mut rows = Vec::new();
    let mut scanned = 0;
    let groups = r.varint();
    for _ in 0..groups {
        let impact = r.varint();
        let min_doc = r.varint() as i64;
        let span = r.varint() as i64;
        let byte_length = r.varint_usize();
        let start = r.pos;
        r.pos = r.pos.saturating_add(byte_length);
        let max_doc = min_doc + span - 1;
        for &doc in &target_docs {
            if doc < min_doc {
                continue;
            }
            if doc > max_doc {
                break;
            }
            scanned += 1;
            let bit = (doc - min_doc) as u64;
            let byte = bytes.get(start.saturating_add((bit / 8) as usize)).copied().unwrap_or(0);
            if byte & (1u8 << (bit % 8)) != 0 {
                rows.push(wrap(doc));
                rows.push(impact as i32);
            }
        }
    }
    (rows, scanned)
}

fn lookup_partitioned_deltas<T: PostingTargets + ?Sized>(bytes: &[u8], targets: &T) -> (Vec<i32>, usize) {
    let mut r = Reader::new(bytes);
    let mut rows = Vec::new();
    let mut scanned = 0;
    let groups = r.varint();
    for _ in 0..groups {
        let impact = r.varint();
        let count = r.varint();
        let mut doc = r.varint() as f64;
        let width = r.varint();
        let byte_length = r.varint_usize();
        scanned += 1;
        push_matched_f64(&mut rows, targets, doc, impact);
        let mut pos = r.pos;
        for delta in read_packed_unsigned(bytes, &mut pos, count.saturating_sub(1), width, byte_length) {
            doc += delta;
            scanned += 1;
            push_matched_f64(&mut rows, targets, doc, impact);
        }
        r.pos = pos;
    }
    (rows, scanned)
}

#[inline]
fn push_matched_f64<T: PostingTargets + ?Sized>(rows: &mut Vec<i32>, targets: &T, doc: f64, impact: u64) {
    if doc.is_finite() && doc == doc.trunc() && doc.abs() < 9.3e18 && targets.has_doc(doc as i64) {
        rows.push(to_int32(doc));
        rows.push(impact as i32);
    }
}

/// `lookupPostingBytes(bytes, docs, block)`: rows whose doc is in `targets`,
/// plus how many postings were inspected.
pub fn lookup_posting_bytes<T: PostingTargets + ?Sized>(bytes: &[u8], targets: &T, codec: BlockCodec) -> (Vec<i32>, usize) {
    if targets.target_count() == 0 {
        return (Vec::new(), 0);
    }
    match codec {
        BlockCodec::PartitionedDeltas => lookup_partitioned_deltas(bytes, targets),
        BlockCodec::ImpactBitset => lookup_impact_bitset(bytes, targets),
        BlockCodec::ImpactRuns => lookup_impact_runs(bytes, targets),
        BlockCodec::PairVarint => lookup_pair_varint(bytes, targets),
    }
}

/// `lookupDecodedPostingRows(rows, docs)`.
pub fn lookup_decoded_posting_rows<T: PostingTargets + ?Sized>(rows: &[i32], targets: &T) -> (Vec<i32>, usize) {
    let mut out = Vec::new();
    let mut scanned = 0;
    let mut i = 0;
    while i < rows.len() {
        scanned += 1;
        let doc = rows[i] as i64;
        if targets.has_doc(doc) {
            // An odd-length input reads `rows[i + 1]` as undefined → 0.
            out.push(rows[i]);
            out.push(rows.get(i + 1).copied().unwrap_or(0));
        }
        i += 2;
    }
    (out, scanned)
}

const EXTERNAL_BLOCKS_ERROR: &str = "External posting blocks require async runtime loading.";

fn inline_block_bytes(segment: &PostingSegment, entry: &TermEntry, block_index: usize) -> Bytes {
    let block = &entry.blocks[block_index];
    let base = segment.data_start + entry.offset;
    let end = base + entry.blocks.get(block_index + 1).map(|next| next.offset).unwrap_or(entry.byte_length);
    let start = base + block.offset;
    segment.bytes.slice(start, end)
}

/// `decodePostingBlock(shard, entry, blockIndex)`: decodes (and caches in
/// `entry.block_postings`) one inline block. A missing block decodes to an
/// empty row list; an external entry is an error, as in JS.
pub fn decode_posting_block(segment: &PostingSegment, entry: &TermEntry, block_index: usize) -> Result<Rc<Vec<i32>>> {
    if block_index >= entry.blocks.len() {
        return Ok(Rc::new(Vec::new()));
    }
    if entry.external {
        return Err(Error::new(EXTERNAL_BLOCKS_ERROR));
    }
    if let Some(rows) = entry.block_postings.borrow().get(&block_index) {
        return Ok(rows.clone());
    }
    let bytes = inline_block_bytes(segment, entry, block_index);
    let rows = Rc::new(decode_posting_bytes(&bytes, entry.blocks[block_index].codec));
    entry.block_postings.borrow_mut().insert(block_index, rows.clone());
    segment.note_decoded_rows(rows.len());
    Ok(rows)
}

/// `lookupPostingBlock(shard, entry, blockIndex, docs)`.
pub fn lookup_posting_block<T: PostingTargets + ?Sized>(
    segment: &PostingSegment,
    entry: &TermEntry,
    block_index: usize,
    targets: &T,
) -> Result<(Vec<i32>, usize)> {
    if block_index >= entry.blocks.len() {
        return Ok((Vec::new(), 0));
    }
    if entry.external {
        return Err(Error::new(EXTERNAL_BLOCKS_ERROR));
    }
    let bytes = inline_block_bytes(segment, entry, block_index);
    Ok(lookup_posting_bytes(&bytes, targets, entry.blocks[block_index].codec))
}

/// `decodePostings(shard, entry)`: every inline block concatenated (cached
/// in `entry.postings`).
pub fn decode_postings(segment: &PostingSegment, entry: &TermEntry) -> Result<Rc<Vec<i32>>> {
    if entry.external {
        return Err(Error::new(EXTERNAL_BLOCKS_ERROR));
    }
    if let Some(rows) = entry.postings.borrow().as_ref() {
        return Ok(rows.clone());
    }
    let mut out = Vec::new();
    for index in 0..entry.blocks.len() {
        out.extend_from_slice(&decode_posting_block(segment, entry, index)?);
    }
    let out = Rc::new(out);
    *entry.postings.borrow_mut() = Some(out.clone());
    segment.note_decoded_rows(out.len());
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_unsigned_round_trip() {
        // width 3: values 5, 2, 7 → bits 101 010 111 → 0b11_010_101, 0b1
        let source = [0b1101_0101u8, 0b0000_0001];
        let mut pos = 0;
        assert_eq!(read_packed_unsigned(&source, &mut pos, 3, 3, 2), vec![5.0, 2.0, 7.0]);
        assert_eq!(pos, 2);
    }

    #[test]
    fn codec_names() {
        assert_eq!(BlockCodec::from_code(9), BlockCodec::PairVarint);
        assert_eq!(BlockCodec::from_name("impact-runs-v1"), BlockCodec::ImpactRuns);
        assert_eq!(BlockCodec::PartitionedDeltas.name(), "partitioned-deltas-v1");
    }
}
