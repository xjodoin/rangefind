//! Paged range directories (`rfdir-v2`) — the decode half of
//! `src/directory.js`: `parseDirectoryRoot`, `parseDirectoryPage`,
//! `findDirectoryPage`, `directoryMightContain`, `compareDirectoryKeys`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use super::binary::{check_magic, DIRECTORY_PAGE_MAGIC, DIRECTORY_ROOT_MAGIC};
use super::{default_pack_file, front_coded_key, read_utf8, FrontCodedKey, JsKeyOrd};
use crate::error::{Error, Result};
use crate::pointer::{Checksum, Pointer};
use crate::util::bytes::{Bytes, Reader};
use crate::util::jsstr::{cmp16, fnv1a16};

pub const DIRECTORY_FORMAT: &str = "rfdir-v2";
const DIRECTORY_VERSION: u64 = 2;

/// `{ first, last, file, count, rawBytes }`
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryPageRef {
    pub first: String,
    pub last: String,
    pub file: String,
    pub count: usize,
    pub raw_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct Bloom {
    pub bits: u64,
    pub hashes: u64,
    pub bytes: Bytes,
}

impl PartialEq for Bloom {
    fn eq(&self, other: &Bloom) -> bool {
        self.bits == other.bits && self.hashes == other.hashes && *self.bytes == *other.bytes
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DirectoryRoot {
    pub version: u64,
    pub entries: usize,
    pub pages: Vec<DirectoryPageRef>,
    pub bloom: Bloom,
}

impl DirectoryRoot {
    pub fn format(&self) -> &'static str {
        DIRECTORY_FORMAT
    }
}

/// `parseDirectoryRoot(buffer)`.
pub fn parse_directory_root(bytes: &[u8]) -> Result<DirectoryRoot> {
    check_magic(bytes, DIRECTORY_ROOT_MAGIC, "Unsupported Rangefind directory root")?;
    let mut r = Reader::at(bytes, DIRECTORY_ROOT_MAGIC.len());
    let version = r.varint();
    if version != DIRECTORY_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind directory root version {version}")));
    }
    let entries = r.varint_usize();
    let page_count = r.varint_usize();
    let bits = r.varint();
    let hashes = r.varint();
    let bloom_length = r.varint_usize();
    let bloom_bytes = Bytes::new(r.take(bloom_length).to_vec());
    let mut pages = Vec::with_capacity(page_count.min(1 << 20));
    for _ in 0..page_count {
        let first = read_utf8(&mut r);
        let last = read_utf8(&mut r);
        let file = read_utf8(&mut r);
        let count = r.varint_usize();
        let raw_bytes = r.varint_usize();
        pages.push(DirectoryPageRef { first, last, file, count, raw_bytes });
    }
    Ok(DirectoryRoot { version, entries, pages, bloom: Bloom { bits, hashes, bytes: bloom_bytes } })
}

/// A parsed directory page: the JS `Map<shard, pointer>`.
#[derive(Clone, Debug, PartialEq)]
pub struct DirectoryPage {
    /// Entries in insertion (sorted build) order. Keys are the decoded
    /// strings; see `keys` for the exact UTF-16 form.
    pub entries: Vec<(String, Pointer)>,
    /// Exact keys, aligned with `entries` (differs from the `String` only
    /// when front coding split a surrogate pair — such keys never match a
    /// lookup, exactly like the JS Map).
    pub keys: Vec<FrontCodedKey>,
    index: HashMap<String, usize>,
}

impl DirectoryPage {
    /// `entries.get(key)`
    pub fn get(&self, key: &str) -> Option<&Pointer> {
        self.index.get(key).map(|&i| &self.entries[i].1)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// `floorSortedKeyIndex([...entries.keys()], term)`.
    pub fn floor_key_index(&self, term: &str) -> i64 {
        super::text_routing::floor_sorted_key_index(&self.keys, term)
    }
}

/// `parseDirectoryPage(buffer, { packTable })`.
pub fn parse_directory_page(bytes: &[u8], pack_table: &[String]) -> Result<DirectoryPage> {
    check_magic(bytes, DIRECTORY_PAGE_MAGIC, "Unsupported Rangefind directory page")?;
    let mut r = Reader::at(bytes, DIRECTORY_PAGE_MAGIC.len());
    let version = r.varint();
    if version != DIRECTORY_VERSION {
        return Err(Error::new(format!("Unsupported Rangefind directory page version {version}")));
    }
    let count = r.varint_usize();
    let mut entries: Vec<(String, Pointer)> = Vec::with_capacity(count.min(1 << 20));
    let mut keys: Vec<FrontCodedKey> = Vec::with_capacity(count.min(1 << 20));
    let mut index: HashMap<String, usize> = HashMap::with_capacity(count.min(1 << 20));
    let mut exact: HashMap<Rc<[u16]>, usize> = HashMap::new();
    let mut previous: Vec<u16> = Vec::new();
    for _ in 0..count {
        let prefix = r.varint_usize();
        let suffix = read_utf8(&mut r);
        let (key, units) = front_coded_key(&previous, prefix, &suffix);
        let pack_index = r.varint();
        let pack = match pack_table.get(pack_index as usize) {
            Some(file) if !file.is_empty() => file.clone(),
            _ => default_pack_file(pack_index),
        };
        let mut pointer = Pointer::new(pack, 0, 0);
        pointer.offset = r.varint();
        pointer.length = r.varint();
        let logical_length = r.varint();
        let algorithm = read_utf8(&mut r);
        let value = read_utf8(&mut r);
        if value.is_empty() {
            return Err(Error::new(format!("Rangefind directory page entry {} is missing a checksum.", key.text)));
        }
        pointer.logical_length = if logical_length != 0 { Some(logical_length) } else { None };
        pointer.checksum = Some(Checksum {
            algorithm: Rc::from(if algorithm.is_empty() { "sha256" } else { algorithm.as_str() }),
            value: Rc::from(value.as_str()),
        });
        // Map.set: an existing key keeps its position and takes the new value.
        let existing = match &key.units {
            None => index.get(&key.text).copied(),
            Some(units) => exact.get(units).copied(),
        };
        match existing {
            Some(slot) => entries[slot].1 = pointer,
            None => {
                let slot = entries.len();
                match &key.units {
                    None => {
                        index.insert(key.text.clone(), slot);
                    }
                    Some(units) => {
                        exact.insert(units.clone(), slot);
                    }
                }
                entries.push((key.text.clone(), pointer));
                keys.push(key);
            }
        }
        previous = units;
    }
    Ok(DirectoryPage { entries, keys, index })
}

/// `hashString(value, seed)` — FNV-1a over UTF-16 code units.
pub fn hash_string(value: &str, seed: u32) -> u32 {
    fnv1a16(value, seed)
}

/// `bloomMightContain(bloom, key)`.
pub fn bloom_might_contain(bloom: &Bloom, key: &str) -> bool {
    if bloom.bits == 0 || bloom.hashes == 0 {
        return true;
    }
    let h1 = hash_string(key, 0);
    let h2 = hash_string(key, 0x9e37_79b9) | 1;
    for i in 0..bloom.hashes {
        let bit = (h1.wrapping_add((i as u32).wrapping_mul(h2)) as u64) % bloom.bits;
        // `bytes[bit >> 3]`: `>>` is a signed 32-bit shift, so bits at or
        // past 2^31 index a negative slot (undefined → not set).
        let slot = ((bit as u32 as i32) >> 3) as i64;
        let set = slot >= 0 && bloom.bytes.get(slot as usize).map(|byte| byte & (1u8 << (bit & 7)) != 0).unwrap_or(false);
        if !set {
            return false;
        }
    }
    true
}

/// `directoryMightContain(root, shard)`.
pub fn directory_might_contain(root: &DirectoryRoot, shard: &str) -> bool {
    bloom_might_contain(&root.bloom, shard)
}

/// `findDirectoryPage(root, shard)`, returning the page index.
pub fn find_directory_page_index(root: &DirectoryRoot, shard: &str) -> Option<usize> {
    if root.pages.is_empty() || !directory_might_contain(root, shard) {
        return None;
    }
    let mut lo: i64 = 0;
    let mut hi: i64 = root.pages.len() as i64 - 1;
    while lo <= hi {
        let mid = (lo + hi) / 2;
        let page = &root.pages[mid as usize];
        if cmp16(shard, &page.first) == Ordering::Less {
            hi = mid - 1;
        } else if cmp16(shard, &page.last) == Ordering::Greater {
            lo = mid + 1;
        } else {
            return Some(mid as usize);
        }
    }
    None
}

/// `findDirectoryPage(root, shard)`.
pub fn find_directory_page<'a>(root: &'a DirectoryRoot, shard: &str) -> Option<&'a DirectoryPageRef> {
    find_directory_page_index(root, shard).map(|index| &root.pages[index])
}

/// `compareDirectoryKeys(left, right)`: -1 / 0 / 1 in UTF-16 order.
pub fn compare_directory_keys(left: &str, right: &str) -> i32 {
    match cmp16(left, right) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

impl JsKeyOrd for DirectoryPageRef {
    fn cmp_js(&self, other: &str) -> Ordering {
        cmp16(&self.first, other)
    }
}
