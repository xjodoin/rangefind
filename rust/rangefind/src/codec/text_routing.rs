//! Text routing segments (`rftextroute-v1`) — `parseTextRoutingSegment`,
//! `floorDirectoryPageIndex` and `floorSortedKeyIndex` from
//! `src/text_routing_codec.js`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use super::binary::{check_magic, TEXT_ROUTING_SEGMENT_MAGIC};
use super::directory::DirectoryRoot;
use super::{front_coded_key, read_utf8, FrontCodedKey, JsKeyOrd};
use crate::error::{Error, Result};
use crate::util::bytes::Reader;

pub const TEXT_ROUTING_FORMAT: &str = "rftextroute-v1";
pub const TEXT_ROUTING_VERSION: u64 = 1;

/// The JS `Map<term, shardOrdinals[]>`.
#[derive(Clone, Debug, PartialEq)]
pub struct TextRoutingSegment {
    /// Terms in insertion (sorted) order with their ascending shard ordinals.
    /// A term whose front coding split a surrogate pair keeps its exact
    /// UTF-16 form in `FrontCodedKey::units` and never matches [`Self::get`].
    pub terms: Vec<(FrontCodedKey, Vec<u64>)>,
    index: HashMap<String, usize>,
}

impl TextRoutingSegment {
    pub fn get(&self, term: &str) -> Option<&[u64]> {
        self.index.get(term).map(|&i| self.terms[i].1.as_slice())
    }

    pub fn len(&self) -> usize {
        self.terms.len()
    }

    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }
}

/// `parseTextRoutingSegment(buffer)`.
pub fn parse_text_routing_segment(bytes: &[u8]) -> Result<TextRoutingSegment> {
    check_magic(bytes, TEXT_ROUTING_SEGMENT_MAGIC, "Unsupported Rangefind text routing segment")?;
    let mut r = Reader::at(bytes, TEXT_ROUTING_SEGMENT_MAGIC.len());
    let version = r.varint();
    if version != TEXT_ROUTING_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind text routing segment version {version}")));
    }
    let count = r.varint_usize();
    let mut terms: Vec<(FrontCodedKey, Vec<u64>)> = Vec::with_capacity(count.min(1 << 20));
    let mut index: HashMap<String, usize> = HashMap::with_capacity(count.min(1 << 20));
    let mut exact: HashMap<Rc<[u16]>, usize> = HashMap::new();
    let mut previous: Vec<u16> = Vec::new();
    for _ in 0..count {
        let prefix = r.varint_usize();
        let suffix = read_utf8(&mut r);
        let (key, units) = front_coded_key(&previous, prefix, &suffix);
        let shard_count = r.varint_usize();
        let mut shards = Vec::with_capacity(shard_count.min(1 << 20));
        let mut last: i64 = -1;
        for _ in 0..shard_count {
            last += r.varint() as i64 + 1;
            shards.push(last as u64);
        }
        let existing = match &key.units {
            None => index.get(&key.text).copied(),
            Some(units) => exact.get(units).copied(),
        };
        match existing {
            Some(slot) => terms[slot].1 = shards,
            None => {
                match &key.units {
                    None => {
                        index.insert(key.text.clone(), terms.len());
                    }
                    Some(units) => {
                        exact.insert(units.clone(), terms.len());
                    }
                }
                terms.push((key, shards));
            }
        }
        previous = units;
    }
    Ok(TextRoutingSegment { terms, index })
}

/// `floorDirectoryPageIndex(root, term)`: the last page whose `first` key is
/// `<= term`, or -1.
pub fn floor_directory_page_index(root: &DirectoryRoot, term: &str) -> i64 {
    floor_sorted_key_index(&root.pages, term)
}

/// `floorSortedKeyIndex(keys, term)`: greatest key `<= term` (UTF-16 order),
/// or -1.
pub fn floor_sorted_key_index<K: JsKeyOrd>(keys: &[K], term: &str) -> i64 {
    if keys.is_empty() || keys[0].cmp_js(term) == Ordering::Greater {
        return -1;
    }
    let mut lo: usize = 0;
    let mut hi: usize = keys.len() - 1;
    while lo < hi {
        let mid = (lo + hi + 1) >> 1;
        if keys[mid].cmp_js(term) != Ordering::Greater {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo as i64
}
