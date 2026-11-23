//! Autocomplete keys, ranking and the authority lexicon codecs — root,
//! paged segments and hot lists (`src/authority_lexicon.js`).
//!
//! Encoders and `autocompleteKeysForValue` (builder side) are not ported.

use std::cmp::Ordering;
use std::rc::Rc;

use super::text::{is_letter_or_number, replace_runs, trim_collapse};
use crate::error::{Error, Result};
use crate::pointer::{Checksum, Pointer};
use crate::util::bytes::{assert_magic, Reader};
use crate::util::fold::fold_multi;
use crate::util::jsstr::{cmp16, prefix16};

pub const AUTHORITY_LEXICON_FORMAT: &str = "rflexicon-v1";
pub const AUTHORITY_LEXICON_PAGED_FORMAT: &str = "rflexicon-v2";
pub const AUTHORITY_LEXICON_SEGMENT_FORMAT: &str = "rflexicon-segment-v1";
pub const AUTOCOMPLETE_PREFIX: &str = "s|";
pub const AUTOCOMPLETE_SEPARATOR: &str = "\u{0}";
pub const DEFAULT_AUTOCOMPLETE_DOC_ROWS: usize = 3;

pub const AUTHORITY_LEXICON_MAGIC: [u8; 4] = [0x52, 0x46, 0x4c, 0x58];
pub const AUTHORITY_LEXICON_SEGMENT_MAGIC: [u8; 4] = [0x52, 0x46, 0x4c, 0x50];
pub const AUTHORITY_HOT_MAGIC: [u8; 4] = [0x52, 0x46, 0x4c, 0x48];

const AUTHORITY_LEXICON_VERSION: u64 = 1;
const AUTHORITY_LEXICON_SEGMENT_VERSION: u64 = 1;
const AUTHORITY_HOT_DOC_VERSION: u64 = 2;
const AUTHORITY_HOT_DOC_ROWS_VERSION: u64 = 3;
const UNWEIGHTED_SURFACE_BONUS: f64 = 4_294_967_296.0;
const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// `suggestKey(value)`: folded, every script kept, non letter/number runs
/// collapsed to one space.
pub fn suggest_key(value: &str) -> String {
    trim_collapse(&replace_runs(&fold_multi(value, true), |c| !is_letter_or_number(c), " "))
}

/// `isAutocompleteKey(key)`.
pub fn is_autocomplete_key(key: &str) -> bool {
    key.starts_with(AUTOCOMPLETE_PREFIX)
}

/// `parseAutocompleteKey` result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedAutocompleteKey {
    pub normalized: String,
    pub display: String,
}

/// `parseAutocompleteKey(key)`: `s|<normalized>\0<display>`.
pub fn parse_autocomplete_key(key: &str) -> Option<ParsedAutocompleteKey> {
    if !is_autocomplete_key(key) {
        return None;
    }
    let body = &key[AUTOCOMPLETE_PREFIX.len()..];
    let separator = body.find(AUTOCOMPLETE_SEPARATOR)?;
    Some(ParsedAutocompleteKey {
        normalized: body[..separator].to_string(),
        display: body[separator + AUTOCOMPLETE_SEPARATOR.len()..].to_string(),
    })
}

/// `item.full ?? suggestKey(item.display) === item.normalized`, the
/// computed half.
pub fn autocomplete_full(display: &str, normalized: &str) -> bool {
    suggest_key(display) == normalized
}

/// `autocompleteRank(item, weighted)`. `weight` is `Number(item.weight)`;
/// `full` is the resolved `item.full ?? autocomplete_full(...)`.
pub fn autocomplete_rank(weight: f64, full: bool, weighted: bool) -> f64 {
    let floored = weight.floor();
    let weight = if floored.is_nan() || floored == 0.0 { 0.0 } else { floored.max(0.0) };
    if weighted {
        MAX_SAFE_INTEGER.min(weight * 2.0 + if full { 1.0 } else { 0.0 })
    } else {
        (if full { UNWEIGHTED_SURFACE_BONUS } else { 0.0 }) + (UNWEIGHTED_SURFACE_BONUS - 1.0).min(weight)
    }
}

/// What `compareAutocomplete` reads from an item.
pub trait AutocompleteOrder {
    /// `item.rank ?? item.weight`.
    fn rank_or_weight(&self) -> f64;
    fn display(&self) -> &str;
    fn normalized(&self) -> &str;
    /// Code-point length of `display` (implementors may cache it).
    fn display_length(&self) -> usize {
        self.display().chars().count()
    }
}

/// `compareAutocomplete(left, right)`: rank desc, shorter display, then
/// normalized and display in UTF-16 order.
pub fn compare_autocomplete<L: AutocompleteOrder + ?Sized, R: AutocompleteOrder + ?Sized>(left: &L, right: &R) -> Ordering {
    let diff = right.rank_or_weight() - left.rank_or_weight();
    if diff > 0.0 {
        return Ordering::Greater;
    }
    if diff < 0.0 {
        return Ordering::Less;
    }
    left.display_length()
        .cmp(&right.display_length())
        .then_with(|| cmp16(left.normalized(), right.normalized()))
        .then_with(|| cmp16(left.display(), right.display()))
}

/// One `{ doc, score }` of an autocomplete summary.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AutocompleteDoc {
    pub doc: f64,
    pub score: f64,
}

/// `autocompleteEntrySummary` result. `doc`/`docs` are present only with
/// `options.doc_rows` and non-empty rows.
#[derive(Clone, Debug, PartialEq)]
pub struct AutocompleteSummary {
    pub key: String,
    pub normalized: String,
    pub display: String,
    pub weight: f64,
    pub count: usize,
    pub full: bool,
    pub doc: Option<f64>,
    pub docs: Option<Vec<AutocompleteDoc>>,
    pub rank: f64,
}

impl AutocompleteOrder for AutocompleteSummary {
    fn rank_or_weight(&self) -> f64 {
        self.rank
    }
    fn display(&self) -> &str {
        &self.display
    }
    fn normalized(&self) -> &str {
        &self.normalized
    }
}

/// `autocompleteEntrySummary` options.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct AutocompleteSummaryOptions {
    pub doc_rows: bool,
    /// `options.maxDocs` (default 3).
    pub max_docs: Option<f64>,
    pub weighted: bool,
}

fn js_sign(value: f64) -> Ordering {
    if value > 0.0 {
        Ordering::Greater
    } else if value < 0.0 {
        Ordering::Less
    } else {
        Ordering::Equal
    }
}

/// `Number(x) || 0`.
fn or_zero(value: f64) -> f64 {
    if value.is_nan() {
        0.0
    } else {
        value
    }
}

/// `autocompleteEntrySummary(key, rows, options)` over `[doc, weight]` rows.
pub fn autocomplete_entry_summary(
    key: &str,
    rows: &[[f64; 2]],
    options: &AutocompleteSummaryOptions,
) -> Option<AutocompleteSummary> {
    let parsed = parse_autocomplete_key(key)?;
    let mut max_weight = 0f64;
    let mut top_doc: Option<f64> = None;
    for row in rows {
        let weight = or_zero(row[1]);
        let doc = row[0];
        if top_doc.is_none() || weight > max_weight || (weight == max_weight && doc < top_doc.unwrap_or(f64::NAN)) {
            top_doc = Some(doc);
        }
        max_weight = if weight.is_nan() || max_weight.is_nan() { f64::NAN } else { max_weight.max(weight) };
    }
    let docs = if options.doc_rows && !rows.is_empty() {
        let max_docs = options.max_docs.unwrap_or(DEFAULT_AUTOCOMPLETE_DOC_ROWS as f64).floor();
        // Math.max(1, NaN) is NaN, and slice(0, NaN) keeps nothing.
        let max_docs = if max_docs.is_nan() { 0.0 } else { max_docs.max(1.0) };
        let mut sorted = rows.to_vec();
        sorted.sort_by(|left, right| {
            let by_weight = right[1] - left[1];
            if by_weight != 0.0 && !by_weight.is_nan() {
                return js_sign(by_weight);
            }
            js_sign(left[0] - right[0])
        });
        Some(
            sorted
                .iter()
                .take(if max_docs >= sorted.len() as f64 { sorted.len() } else { max_docs as usize })
                .map(|row| AutocompleteDoc { doc: row[0], score: or_zero(row[1]) })
                .collect::<Vec<_>>(),
        )
    } else {
        None
    };
    let full = autocomplete_full(&parsed.display, &parsed.normalized);
    let weight = if max_weight != 0.0 && !max_weight.is_nan() { max_weight } else { rows.len() as f64 };
    let rank = autocomplete_rank(weight, full, options.weighted);
    Some(AutocompleteSummary {
        key: key.to_string(),
        normalized: parsed.normalized,
        display: parsed.display,
        weight,
        count: rows.len(),
        full,
        doc: if docs.is_some() { top_doc } else { None },
        docs,
        rank,
    })
}

fn read_front_coded(reader: &mut Reader<'_>, previous: &str) -> String {
    let prefix = reader.varint_usize();
    let mut value = prefix16(previous, prefix);
    value.push_str(&reader.utf8());
    value
}

/// A lexicon shard summary (`{ shard, maxRank, count }`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityLexiconShard {
    pub shard: String,
    pub max_rank: u64,
    pub count: u64,
}

/// A hot-list pointer (`root.hot.get(prefix)`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityHotRef {
    pub file: String,
    pub bytes: u64,
    pub count: u64,
}

/// `parseAuthorityLexiconRoot` result. `hot` keeps JS `Map` order (a
/// repeated prefix keeps its first position and takes the later value).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityLexiconRoot {
    pub format: &'static str,
    pub weighted: bool,
    pub shards: Vec<AuthorityLexiconShard>,
    pub hot: Vec<(String, AuthorityHotRef)>,
}

impl AuthorityLexiconRoot {
    /// `root.hot.get(prefix)`.
    pub fn hot_get(&self, prefix: &str) -> Option<&AuthorityHotRef> {
        self.hot.iter().find(|(key, _)| key == prefix).map(|(_, entry)| entry)
    }
}

fn trailing_check(reader: &Reader<'_>, message: &str) -> Result<()> {
    if reader.pos != reader.bytes.len() {
        return Err(Error::new(message));
    }
    Ok(())
}

/// `parseAuthorityLexiconRoot(buffer)`.
pub fn parse_authority_lexicon_root(bytes: &[u8]) -> Result<AuthorityLexiconRoot> {
    assert_magic(bytes, &AUTHORITY_LEXICON_MAGIC, "Unsupported Rangefind authority lexicon root")?;
    let mut reader = Reader::at(bytes, AUTHORITY_LEXICON_MAGIC.len());
    let version = reader.varint();
    if version != AUTHORITY_LEXICON_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind authority lexicon root version {version}")));
    }
    let weighted = reader.varint() == 1;
    let shard_count = reader.varint_usize();
    let mut shards = Vec::with_capacity(shard_count.min(bytes.len()));
    let mut previous = String::new();
    for _ in 0..shard_count {
        let shard = read_front_coded(&mut reader, &previous);
        let max_rank = reader.varint();
        let count = reader.varint();
        previous = shard.clone();
        shards.push(AuthorityLexiconShard { shard, max_rank, count });
    }
    let hot_count = reader.varint_usize();
    let mut hot: Vec<(String, AuthorityHotRef)> = Vec::new();
    for _ in 0..hot_count {
        let prefix = reader.utf8();
        let entry = AuthorityHotRef { file: reader.utf8(), bytes: reader.varint(), count: reader.varint() };
        match hot.iter_mut().find(|(key, _)| *key == prefix) {
            Some(slot) => slot.1 = entry,
            None => hot.push((prefix, entry)),
        }
    }
    trailing_check(&reader, "Rangefind authority lexicon root has trailing bytes.")?;
    Ok(AuthorityLexiconRoot { format: AUTHORITY_LEXICON_FORMAT, weighted, shards, hot })
}

/// One paged-lexicon segment summary with its direct authority pointer
/// (`entry.physicalLength` is always `entry.length`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityLexiconSegmentEntry {
    pub shard: String,
    pub max_rank: u64,
    pub count: u64,
    pub entry: Pointer,
}

/// `parseAuthorityLexiconSegment` result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityLexiconSegment {
    pub format: &'static str,
    pub entries: Vec<AuthorityLexiconSegmentEntry>,
    pub first: String,
    pub last: String,
}

/// `parseAuthorityLexiconSegment(buffer, { packTable })`.
pub fn parse_authority_lexicon_segment<S: AsRef<str>>(bytes: &[u8], pack_table: &[S]) -> Result<AuthorityLexiconSegment> {
    assert_magic(bytes, &AUTHORITY_LEXICON_SEGMENT_MAGIC, "Unsupported Rangefind authority lexicon segment")?;
    let mut reader = Reader::at(bytes, AUTHORITY_LEXICON_SEGMENT_MAGIC.len());
    let version = reader.varint();
    if version != AUTHORITY_LEXICON_SEGMENT_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind authority lexicon segment version {version}")));
    }
    let count = reader.varint_usize();
    let mut entries = Vec::with_capacity(count.min(bytes.len()));
    let mut previous = String::new();
    for _ in 0..count {
        let shard = read_front_coded(&mut reader, &previous);
        let max_rank = reader.varint();
        let entry_count = reader.varint();
        let pack_index = reader.varint();
        let offset = reader.varint();
        let length = reader.varint();
        let logical_length = reader.varint();
        let algorithm = reader.utf8();
        let algorithm = if algorithm.is_empty() { "sha256".to_string() } else { algorithm };
        let value = reader.utf8();
        if value.is_empty() {
            return Err(Error::new(format!("Rangefind authority lexicon segment entry {shard} is missing a checksum.")));
        }
        let pack = usize::try_from(pack_index)
            .ok()
            .and_then(|position| pack_table.get(position))
            .map(AsRef::as_ref)
            .filter(|name| !name.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{pack_index:04}.bin"));
        let pointer = Pointer {
            pack: Rc::from(pack.as_str()),
            offset,
            length,
            logical_length: if logical_length == 0 { None } else { Some(logical_length) },
            checksum: Some(Checksum { algorithm: Rc::from(algorithm.as_str()), value: Rc::from(value.as_str()) }),
            checksum_row: None,
        };
        previous = shard.clone();
        entries.push(AuthorityLexiconSegmentEntry { shard, max_rank, count: entry_count, entry: pointer });
    }
    trailing_check(&reader, "Rangefind authority lexicon segment has trailing bytes.")?;
    let first = entries.first().map(|entry| entry.shard.clone()).unwrap_or_default();
    let last = entries.last().map(|entry| entry.shard.clone()).unwrap_or_default();
    Ok(AuthorityLexiconSegment { format: AUTHORITY_LEXICON_SEGMENT_FORMAT, entries, first, last })
}

/// One `{ doc, score, ordinal? }` hot-list row (v3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HotListDoc {
    pub doc: u64,
    pub score: u64,
    /// Federation shard ordinal (root routing hot lists); `None` = local.
    pub ordinal: Option<u64>,
}

/// One hot-list entry. v2 lists set only `doc`; v3 lists with rows set
/// `doc` (= `docs[0].doc`) and `docs`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HotListEntry {
    pub normalized: String,
    pub display: String,
    pub weight: u64,
    pub count: u64,
    pub doc: Option<u64>,
    pub docs: Option<Vec<HotListDoc>>,
}

impl AutocompleteOrder for HotListEntry {
    fn rank_or_weight(&self) -> f64 {
        self.weight as f64
    }
    fn display(&self) -> &str {
        &self.display
    }
    fn normalized(&self) -> &str {
        &self.normalized
    }
}

/// `parseAuthorityHotList(buffer)` (versions 1–3).
pub fn parse_authority_hot_list(bytes: &[u8]) -> Result<Vec<HotListEntry>> {
    assert_magic(bytes, &AUTHORITY_HOT_MAGIC, "Unsupported Rangefind authority hot list")?;
    let mut reader = Reader::at(bytes, AUTHORITY_HOT_MAGIC.len());
    let version = reader.varint();
    if !(AUTHORITY_LEXICON_VERSION..=AUTHORITY_HOT_DOC_ROWS_VERSION).contains(&version) {
        return Err(Error::new(format!("Unsupported Rangefind authority hot-list version {version}")));
    }
    let count = reader.varint_usize();
    let mut entries = Vec::with_capacity(count.min(bytes.len()));
    for _ in 0..count {
        let mut entry = HotListEntry {
            normalized: reader.utf8(),
            display: reader.utf8(),
            weight: reader.varint(),
            count: reader.varint(),
            doc: None,
            docs: None,
        };
        if version == AUTHORITY_HOT_DOC_VERSION {
            let doc = reader.varint();
            if doc > 0 {
                entry.doc = Some(doc - 1);
            }
        } else if version >= AUTHORITY_HOT_DOC_ROWS_VERSION {
            let row_count = reader.varint_usize();
            if row_count > 0 {
                let mut docs = Vec::with_capacity(row_count.min(bytes.len()));
                for _ in 0..row_count {
                    let doc = reader.varint();
                    let score = reader.varint();
                    let ordinal = reader.varint();
                    docs.push(HotListDoc { doc, score, ordinal: if ordinal > 0 { Some(ordinal - 1) } else { None } });
                }
                entry.doc = Some(docs[0].doc);
                entry.docs = Some(docs);
            }
        }
        entries.push(entry);
    }
    trailing_check(&reader, "Rangefind authority hot list has trailing bytes.")?;
    Ok(entries)
}
