//! Authority keys and the authority shard codec (`src/authority_codec.js`).
//!
//! `buildAuthorityShard` (builder) is not ported. The analyzer is injected:
//! `authorityKeysForValue` only needs `analyzer.tokenize(value, { unique:
//! false, lang: analyzer.detectLanguage(value) })`, so callers pass that as a
//! `tokenize` closure and this module stays independent of `analysis`.

use std::collections::HashMap;

use unicode_normalization::UnicodeNormalization;

use super::address::{address_authority_query_keys, normalize_address_authority_key};
use super::lexicon::is_autocomplete_key;
use super::text::{is_letter_or_number, replace_runs, trim_collapse};
use crate::error::{Error, Result};
use crate::util::bytes::{assert_magic, Reader};
use crate::util::fold::fold_multi;
use crate::util::jsstr::prefix16;

pub const AUTHORITY_FORMAT: &str = "rfauth-v2";
pub const AUTHORITY_LEGACY_FORMAT: &str = "rfauth-v1";
pub const SUGGEST_ROUTING_FORMAT: &str = "rfsuggestroute-v2";
pub const SUGGEST_ROUTING_COMPAT_FORMATS: [&str; 2] = ["rfsuggestroute-v1", "rfsuggestroute-v2"];
pub const AUTHORITY_SHARD_MAGIC: [u8; 4] = [0x52, 0x46, 0x41, 0x55];

const AUTHORITY_VERSION: u64 = 2;
const AUTHORITY_LEGACY_VERSION: u64 = 1;
const AUTHORITY_ORDINAL_ROWS_VERSION: u64 = 3;
const AUTHORITY_DOC_ROWS_VERSION: u64 = 4;
const AUTHORITY_ORDINAL_DOC_ROWS_VERSION: u64 = 5;

pub const SURFACE_PREFIX: &str = "r|";
pub const EXACT_PREFIX: &str = "x|";
pub const TOKEN_PREFIX: &str = "t|";
pub const ADDRESS_PREFIX: &str = "a|";
pub const ADDRESS_RANGE_PREFIX: &str = "i|";

/// `authorityNormalizeSurface(value)`: folded, ASCII alphanumerics only.
pub fn authority_normalize_surface(value: &str) -> String {
    trim_collapse(&replace_runs(&fold_multi(value, true), |c| !(c.is_ascii_lowercase() || c.is_ascii_digit()), " "))
}

/// `authorityNormalizeRawSurface(value)`: NFKC + lowercase, every script
/// kept, non letter/number runs collapsed to one space.
pub fn authority_normalize_raw_surface(value: &str) -> String {
    let lowered = value.nfkc().collect::<String>().to_lowercase();
    trim_collapse(&replace_runs(&lowered, |c| !is_letter_or_number(c), " "))
}

/// `authorityAddressRangeKey(value)`.
pub fn authority_address_range_key(value: &str) -> String {
    let normalized = authority_normalize_surface(value);
    if normalized.is_empty() {
        String::new()
    } else {
        format!("{ADDRESS_RANGE_PREFIX}{normalized}")
    }
}

/// `authorityTokenKeyFromTerms(terms)`.
pub fn authority_token_key_from_terms<S: AsRef<str>>(terms: &[S]) -> String {
    let parts: Vec<&str> = terms.iter().map(AsRef::as_ref).filter(|term| !term.is_empty()).collect();
    if parts.is_empty() {
        String::new()
    } else {
        format!("{TOKEN_PREFIX}{}", parts.join(" "))
    }
}

/// The `kind` of an authority key plan item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorityKeyKind {
    Surface,
    Exact,
    Tokens,
    Address,
    AddressRange,
}

impl AuthorityKeyKind {
    /// The JS `kind` string.
    pub fn as_str(self) -> &'static str {
        match self {
            AuthorityKeyKind::Surface => "surface",
            AuthorityKeyKind::Exact => "exact",
            AuthorityKeyKind::Tokens => "tokens",
            AuthorityKeyKind::Address => "address",
            AuthorityKeyKind::AddressRange => "address-range",
        }
    }
}

/// `{ key, kind }`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityKey {
    pub key: String,
    pub kind: AuthorityKeyKind,
}

/// `options.normalizer`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorityNormalizer {
    Address,
    AddressRange,
}

/// `authorityKeysForValue` options (`surface`/`exact`/`tokens` default true).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityKeyOptions {
    pub surface: bool,
    pub exact: bool,
    pub tokens: bool,
    pub normalizer: Option<AuthorityNormalizer>,
}

impl Default for AuthorityKeyOptions {
    fn default() -> Self {
        AuthorityKeyOptions { surface: true, exact: true, tokens: true, normalizer: None }
    }
}

fn push_unique(out: &mut Vec<AuthorityKey>, key: String, kind: AuthorityKeyKind) {
    if !out.iter().any(|item| item.key == key) {
        out.push(AuthorityKey { key, kind });
    }
}

/// `authorityKeysForValue(value, options)`. `tokenize(value)` must return
/// `analyzer.tokenize(value, { unique: false, lang: analyzer.detectLanguage(value) })`;
/// it is only called when `options.tokens` is set.
pub fn authority_keys_for_value(
    value: &str,
    options: &AuthorityKeyOptions,
    tokenize: &dyn Fn(&str) -> Vec<String>,
) -> Vec<AuthorityKey> {
    if options.normalizer == Some(AuthorityNormalizer::AddressRange) {
        let key = authority_address_range_key(value);
        return if key.is_empty() { Vec::new() } else { vec![AuthorityKey { key, kind: AuthorityKeyKind::AddressRange }] };
    }
    let mut out = Vec::new();
    let surface = if options.surface { authority_normalize_raw_surface(value) } else { String::new() };
    if !surface.is_empty() {
        out.push(AuthorityKey { key: format!("{SURFACE_PREFIX}{surface}"), kind: AuthorityKeyKind::Surface });
    }
    let exact = if options.exact { authority_normalize_surface(value) } else { String::new() };
    if !exact.is_empty() {
        push_unique(&mut out, format!("{EXACT_PREFIX}{exact}"), AuthorityKeyKind::Exact);
    }
    if options.tokens {
        let token_key = authority_token_key_from_terms(&tokenize(value));
        if !token_key.is_empty() {
            push_unique(&mut out, token_key, AuthorityKeyKind::Tokens);
        }
    }
    if options.normalizer == Some(AuthorityNormalizer::Address) {
        let address = normalize_address_authority_key(value);
        if !address.is_empty() {
            push_unique(&mut out, format!("{ADDRESS_PREFIX}{address}"), AuthorityKeyKind::Address);
        }
    }
    out
}

/// `authorityKeysForQuery(query, baseTerms, { analyzer, address })`.
pub fn authority_keys_for_query<S: AsRef<str>>(
    query: &str,
    base_terms: &[S],
    tokenize: &dyn Fn(&str) -> Vec<String>,
    address: bool,
) -> Vec<AuthorityKey> {
    let mut out = authority_keys_for_value(query, &AuthorityKeyOptions::default(), tokenize);
    let token_key = authority_token_key_from_terms(base_terms);
    if !token_key.is_empty() {
        push_unique(&mut out, token_key, AuthorityKeyKind::Tokens);
    }
    if address {
        for key in address_authority_query_keys(query) {
            push_unique(&mut out, format!("{ADDRESS_PREFIX}{key}"), AuthorityKeyKind::Address);
        }
    }
    out
}

/// One decoded authority entry. `rows` are `[doc, score]` (or `[shard
/// ordinal, weight]` when `ordinal_rows`). Autocomplete entries of v2+
/// shards carry `autocomplete_weight` and are always `complete`; v5 entries
/// may name the winning row's `doc_ordinal`/`doc`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityEntry {
    pub key: String,
    pub total: u64,
    pub complete: bool,
    pub rows: Vec<[u64; 2]>,
    pub autocomplete_weight: Option<u64>,
    pub ordinal_rows: bool,
    pub doc_rows: bool,
    pub doc_ordinal: Option<u64>,
    pub doc: Option<u64>,
}

/// `parseAuthorityShard` result: entries in JS `Map` order (a repeated key
/// keeps its first position and takes the later value).
#[derive(Clone, Debug, Default)]
pub struct AuthorityShard {
    pub format: &'static str,
    pub entries: Vec<AuthorityEntry>,
    index: HashMap<String, usize>,
}

impl AuthorityShard {
    /// `entries.get(key)`.
    pub fn get(&self, key: &str) -> Option<&AuthorityEntry> {
        self.index.get(key).map(|position| &self.entries[*position])
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn set(&mut self, entry: AuthorityEntry) {
        match self.index.get(&entry.key) {
            Some(position) => self.entries[*position] = entry,
            None => {
                self.index.insert(entry.key.clone(), self.entries.len());
                self.entries.push(entry);
            }
        }
    }
}

fn read_rows(reader: &mut Reader<'_>) -> Vec<[u64; 2]> {
    let count = reader.varint_usize();
    let mut rows = Vec::with_capacity(count.min(reader.bytes.len()));
    for _ in 0..count {
        let doc = reader.varint();
        let score = reader.varint();
        rows.push([doc, score]);
    }
    rows
}

/// `parseAuthorityShard(buffer)` (versions 1–5).
pub fn parse_authority_shard(bytes: &[u8]) -> Result<AuthorityShard> {
    assert_magic(bytes, &AUTHORITY_SHARD_MAGIC, "Unsupported Rangefind authority shard")?;
    let mut reader = Reader::at(bytes, AUTHORITY_SHARD_MAGIC.len());
    let version = reader.varint();
    if !(AUTHORITY_LEGACY_VERSION..=AUTHORITY_ORDINAL_DOC_ROWS_VERSION).contains(&version) {
        return Err(Error::new(format!("Unsupported Rangefind authority shard version {version}")));
    }
    let ordinal_rows = version == AUTHORITY_ORDINAL_ROWS_VERSION || version == AUTHORITY_ORDINAL_DOC_ROWS_VERSION;
    let doc_provenance = version == AUTHORITY_ORDINAL_DOC_ROWS_VERSION;
    let doc_rows = version == AUTHORITY_DOC_ROWS_VERSION;
    let count = reader.varint();
    let mut shard = AuthorityShard {
        format: if version == AUTHORITY_LEGACY_VERSION { AUTHORITY_LEGACY_FORMAT } else { AUTHORITY_FORMAT },
        entries: Vec::new(),
        index: HashMap::new(),
    };
    let mut previous = String::new();
    for _ in 0..count {
        let prefix = reader.varint_usize();
        let suffix = reader.utf8();
        let mut key = prefix16(&previous, prefix);
        key.push_str(&suffix);
        let total = reader.varint();
        if version >= AUTHORITY_VERSION && is_autocomplete_key(&key) {
            let autocomplete_weight = reader.varint();
            let rows = if ordinal_rows || doc_rows { read_rows(&mut reader) } else { Vec::new() };
            let mut provenance = None;
            if doc_provenance {
                let ordinal_plus_one = reader.varint();
                if ordinal_plus_one > 0 {
                    provenance = Some((ordinal_plus_one - 1, reader.varint()));
                }
            }
            shard.set(AuthorityEntry {
                key: key.clone(),
                total,
                complete: true,
                rows,
                autocomplete_weight: Some(autocomplete_weight),
                ordinal_rows,
                doc_rows,
                doc_ordinal: provenance.map(|(ordinal, _)| ordinal),
                doc: provenance.map(|(_, doc)| doc),
            });
            previous = key;
            continue;
        }
        let rows = read_rows(&mut reader);
        shard.set(AuthorityEntry {
            key: key.clone(),
            total,
            complete: total == rows.len() as u64,
            rows,
            autocomplete_weight: None,
            ordinal_rows,
            doc_rows: false,
            doc_ordinal: None,
            doc: None,
        });
        previous = key;
    }
    Ok(shard)
}
