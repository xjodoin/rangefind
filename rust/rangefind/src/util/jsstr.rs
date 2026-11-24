//! JavaScript string semantics the index formats depend on.
//!
//! The builder runs under Node, so every length, slice offset and ordering
//! baked into an index is in UTF-16 code units. These helpers reproduce
//! those semantics over Rust `&str` so shard names, directory keys and
//! token length gates resolve identically.

use std::cmp::Ordering;

/// `string.length`.
pub fn len16(value: &str) -> usize {
    value.chars().map(char::len_utf16).sum()
}

/// JS relational comparison (`<`, `>`): UTF-16 code-unit order. This differs
/// from Rust's byte order only between surrogate-encoded astral characters
/// and U+E000..U+FFFF.
pub fn cmp16(left: &str, right: &str) -> Ordering {
    if left.is_ascii() && right.is_ascii() {
        return left.as_bytes().cmp(right.as_bytes());
    }
    left.encode_utf16().cmp(right.encode_utf16())
}

pub fn to16(value: &str) -> Vec<u16> {
    value.encode_utf16().collect()
}

/// Lossy decode: lone surrogates become U+FFFD, the same replacement
/// `TextEncoder` applies when such a string is persisted.
pub fn from16(units: &[u16]) -> String {
    String::from_utf16_lossy(units)
}

/// `value.slice(0, end)` in code units.
pub fn prefix16(value: &str, end: usize) -> String {
    if value.is_ascii() {
        return value[..end.min(value.len())].to_string();
    }
    let units = to16(value);
    from16(&units[..end.min(units.len())])
}

/// `value.slice(start)` in code units.
pub fn suffix16(value: &str, start: usize) -> String {
    if value.is_ascii() {
        return value[start.min(value.len())..].to_string();
    }
    let units = to16(value);
    from16(&units[start.min(units.len())..])
}

/// `shardKey(term, depth)` from `src/shards.js`: the first `depth` code
/// units, extended by one when that would split a surrogate pair.
pub fn shard_key(term: &str, depth: usize) -> String {
    if term.is_ascii() {
        return term[..depth.min(term.len())].to_string();
    }
    let units = to16(term);
    let mut end = depth.min(units.len());
    if end > 0
        && end < units.len()
        && (0xd800..=0xdbff).contains(&units[end - 1])
        && (0xdc00..=0xdfff).contains(&units[end])
    {
        end += 1;
    }
    from16(&units[..end])
}

/// FNV-1a over UTF-16 code units (`hashString` in `src/directory.js`).
pub fn fnv1a16(value: &str, seed: u32) -> u32 {
    let mut hash = 0x811c_9dc5u32 ^ seed;
    for unit in value.encode_utf16() {
        hash ^= unit as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf16_lengths_and_order() {
        assert_eq!(len16("abc"), 3);
        assert_eq!(len16("é"), 1);
        assert_eq!(len16("😀"), 2);
        // U+FF5E sorts after an astral char in UTF-16 order, before it in UTF-8.
        assert_eq!(cmp16("\u{ff5e}", "😀"), Ordering::Greater);
        assert_eq!("\u{ff5e}".cmp("😀"), Ordering::Less);
    }

    #[test]
    fn shard_keys_keep_surrogate_pairs_whole() {
        assert_eq!(shard_key("hello", 3), "hel");
        assert_eq!(shard_key("ai", 3), "ai");
        assert_eq!(shard_key("a😀b", 2), "a😀");
        assert_eq!(shard_key("a😀b", 3), "a😀");
        assert_eq!(shard_key("a😀b", 4), "a😀b");
    }
}
