//! Port of `src/integrations/osm/category_lexicon.js`: the category gate the
//! OSM planner consults before treating a token as a locality.
//!
//! The lexicon is a folded-surface → `{type, query}` map in insertion order,
//! exactly like the JS `Map`. On top of it the Rust version keeps a hash
//! index (exact lookups) and a bucket of single-word keys per UTF-16 length,
//! so the one-edit fuzzy gate scans only keys of length n-1..n+1 instead of
//! the whole lexicon. Results are identical: the fuzzy match set does not
//! depend on scan order, and the winner is chosen by a total order.

mod data;

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use serde_json::{json, Map, Value};
use unicode_normalization::UnicodeNormalization;
use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};

pub use data::{CATEGORY_EXCLUDED_TYPES, OSM_CANONICAL_TYPES, OSM_TYPE_ALIASES};

use crate::osm::constraints::js::{is_word, js_string};
use crate::typo::{is_letter, is_number};
use crate::util::jsstr::{cmp16, len16};
use crate::util::json::{num, to_number, truthy};

/// `OSM_CATEGORY_LEXICON_VERSION`.
pub const OSM_CATEGORY_LEXICON_VERSION: u32 = 1;

/// `CATEGORY_EXCLUDED_TYPES.has(type)`.
pub fn is_category_excluded_type(type_: &str) -> bool {
    CATEGORY_EXCLUDED_TYPES.contains(&type_)
}

/// `OSM_TYPE_ALIASES[type]`.
pub fn osm_type_aliases(type_: &str) -> Option<&'static [&'static str]> {
    OSM_TYPE_ALIASES.iter().find(|(key, _)| *key == type_).map(|(_, aliases)| *aliases)
}

/// One lexicon value: the OSM type a surface gates to and its searchable
/// text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CategoryEntry {
    pub type_: String,
    pub query: String,
}

/// The intent `lookupCategory` / `lookupCategoryFuzzy` return. The two
/// `corrected_*` fields are only set by the fuzzy lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CategoryMatch {
    pub type_: String,
    pub query: String,
    pub label: String,
    pub corrected_from: Option<String>,
    pub corrected_to: Option<String>,
}

impl CategoryMatch {
    /// The JS object shape (`{type, query, label[, correctedFrom, correctedTo]}`).
    pub fn to_json(&self) -> Value {
        let mut out = Map::new();
        out.insert("type".into(), Value::String(self.type_.clone()));
        out.insert("query".into(), Value::String(self.query.clone()));
        out.insert("label".into(), Value::String(self.label.clone()));
        if let Some(from) = &self.corrected_from {
            out.insert("correctedFrom".into(), Value::String(from.clone()));
        }
        if let Some(to) = &self.corrected_to {
            out.insert("correctedTo".into(), Value::String(to.clone()));
        }
        Value::Object(out)
    }
}

/// The folded alias → entry map (`Map` in JS), insertion ordered.
#[derive(Clone, Debug, Default)]
pub struct CategoryLexicon {
    entries: Vec<(String, CategoryEntry)>,
    index: HashMap<String, usize>,
    /// Single-word keys bucketed by UTF-16 length, entry indexes ascending.
    by_length: HashMap<usize, Vec<usize>>,
}

impl CategoryLexicon {
    pub fn new() -> Self {
        Self::default()
    }

    /// `lexicon.get(key)`.
    pub fn get(&self, key: &str) -> Option<&CategoryEntry> {
        self.index.get(key).map(|&slot| &self.entries[slot].1)
    }

    /// `lexicon.has(key)`.
    pub fn contains_key(&self, key: &str) -> bool {
        self.index.contains_key(key)
    }

    /// `lexicon.size`.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Entries in insertion order (`for (const [key, entry] of lexicon)`).
    pub fn iter(&self) -> impl Iterator<Item = (&str, &CategoryEntry)> {
        self.entries.iter().map(|(key, entry)| (key.as_str(), entry))
    }

    /// `lexicon.set(key, entry)` when the key is new; an existing key keeps
    /// its value (the only way the JS builder writes).
    fn insert_new(&mut self, key: String, entry: CategoryEntry) {
        if self.index.contains_key(&key) {
            return;
        }
        let slot = self.entries.len();
        if !key.contains(' ') {
            self.by_length.entry(len16(&key)).or_default().push(slot);
        }
        self.index.insert(key.clone(), slot);
        self.entries.push((key, entry));
    }
}

fn is_mark(c: char) -> bool {
    c.general_category_group() == GeneralCategoryGroup::Mark
}

/// The planner's shared text folding: NFKD, strip marks, lowercase, and
/// collapse every run of non-letter/non-number code points to one space.
/// (Not `crate::util::fold::fold`: that one maps ß/œ/æ and keeps
/// punctuation.)
pub fn fold(value: &str) -> String {
    let stripped: String = value.nfkd().filter(|c| !is_mark(*c)).collect();
    let lowered = stripped.to_lowercase();
    let mut out = String::with_capacity(lowered.len());
    let mut gap = false;
    for c in lowered.chars() {
        if is_letter(c) || is_number(c) {
            if gap {
                out.push(' ');
                gap = false;
            }
            out.push(c);
        } else {
            gap = true;
        }
    }
    // `.trim()`: the only whitespace left is the collapsed separators, and a
    // trailing gap was never written; a leading one is dropped here.
    if out.starts_with(' ') {
        out.remove(0);
    }
    out
}

/// `typeQueryText(type)`: `_`/`;` runs become spaces ("fast_food" →
/// "fast food").
pub fn type_query_text(type_: &str) -> String {
    let mut out = String::with_capacity(type_.len());
    let mut in_run = false;
    for c in type_.chars() {
        if c == '_' || c == ';' {
            if !in_run {
                out.push(' ');
                in_run = true;
            }
        } else {
            out.push(c);
            in_run = false;
        }
    }
    crate::osm::constraints::js::js_trim(&out).to_string()
}

/// `labelize(folded)`: uppercase each letter that follows a `\b` (an ASCII
/// word boundary, so a non-ASCII letter right after an ASCII word char is
/// capitalized too — "café" → "CafÉ", exactly as the JS does).
fn labelize(folded: &str) -> String {
    let mut out = String::with_capacity(folded.len());
    let mut previous_word = false;
    for c in folded.chars() {
        let word = is_word(c);
        if is_letter(c) && word != previous_word {
            out.extend(c.to_uppercase());
        } else {
            out.push(c);
        }
        previous_word = word;
    }
    out
}

fn add_entry(lexicon: &mut CategoryLexicon, key: &str, type_: &str) {
    let folded = fold(key);
    if folded.is_empty() || lexicon.contains_key(&folded) {
        return;
    }
    lexicon.insert_new(folded, CategoryEntry { type_: type_.to_string(), query: type_query_text(type_) });
}

fn gateable_type(type_: &str) -> bool {
    if is_category_excluded_type(type_) {
        return false;
    }
    let folded = fold(&type_query_text(type_));
    !folded.is_empty() && len16(&folded) <= 32 && folded.split(' ').count() <= 3 && folded.chars().any(is_letter)
}

/// `String(value || "")`.
fn string_or_empty(value: &Value) -> String {
    if truthy(value) {
        js_string(value)
    } else {
        String::new()
    }
}

/// `String(item?.value ?? item ?? "")` for a facet dictionary row.
fn row_value(item: &Value) -> String {
    match item.get("value") {
        Some(value) if !value.is_null() => js_string(value),
        _ => {
            if item.is_null() {
                String::new()
            } else {
                js_string(item)
            }
        }
    }
}

/// `buildCategoryLexicon(typeValues)`. Accepts `null` (bundled vocabulary),
/// a type facet dictionary (array of strings or `{value}` rows), or an
/// embedded artifact `{ types: [...], aliases: { folded: type } }`.
pub fn build_category_lexicon(type_values: &Value) -> CategoryLexicon {
    let mut lexicon = CategoryLexicon::new();
    if let Some(types) = type_values.as_object().and_then(|map| map.get("types")).and_then(Value::as_array) {
        if let Some(aliases) = type_values.get("aliases").filter(|value| truthy(value)).and_then(Value::as_object) {
            for (alias, type_) in aliases {
                add_entry(&mut lexicon, alias, &string_or_empty(type_));
            }
        }
        // `new Set(typeValues.types)`: only string members can equal an
        // alias-table key.
        let present: HashSet<&str> = types.iter().filter_map(Value::as_str).collect();
        for (type_, aliases) in OSM_TYPE_ALIASES {
            if !present.contains(type_) {
                continue;
            }
            for alias in aliases.iter() {
                add_entry(&mut lexicon, alias, type_);
            }
        }
        for type_ in types {
            let text = string_or_empty(type_);
            add_entry(&mut lexicon, &type_query_text(&text), &text);
        }
        return lexicon;
    }
    let types: Vec<String> = match type_values.as_array() {
        Some(items) if !items.is_empty() => items.iter().map(row_value).filter(|value| gateable_type(value)).collect(),
        _ => OSM_CANONICAL_TYPES.iter().map(|value| value.to_string()).collect(),
    };
    let present: HashSet<&str> = types.iter().map(String::as_str).collect();
    for (type_, aliases) in OSM_TYPE_ALIASES {
        if !present.contains(type_) {
            continue;
        }
        for alias in aliases.iter() {
            add_entry(&mut lexicon, alias, type_);
        }
    }
    for type_ in &types {
        add_entry(&mut lexicon, &type_query_text(type_), type_);
    }
    lexicon
}

/// `buildCategoryLexiconArtifact(typeValues, { minCount })` (build side; the
/// artifact a sharded root manifest embeds). `min_count` defaults to 250.
pub fn build_category_lexicon_artifact(type_values: &Value, min_count: Option<f64>) -> Value {
    let min_count = min_count.unwrap_or(250.0);
    let curated: HashSet<&str> =
        OSM_CANONICAL_TYPES.iter().copied().chain(OSM_TYPE_ALIASES.iter().map(|(key, _)| *key)).collect();
    let empty = Vec::new();
    let items = type_values.as_array().unwrap_or(&empty);
    let types: Vec<String> = items
        .iter()
        .filter_map(|item| {
            let value = row_value(item);
            let n = match item.get("n") {
                Some(n) if truthy(n) => to_number(n),
                _ => 0.0,
            };
            (!value.is_empty() && gateable_type(&value) && (curated.contains(value.as_str()) || n >= min_count))
                .then_some(value)
        })
        .collect();
    let present: HashSet<&str> = types.iter().map(String::as_str).collect();
    let mut aliases = Map::new();
    for (type_, list) in OSM_TYPE_ALIASES {
        if !present.contains(type_) {
            continue;
        }
        for alias in list.iter() {
            let folded = fold(alias);
            if !folded.is_empty() && !aliases.contains_key(&folded) {
                aliases.insert(folded, Value::String(type_.to_string()));
            }
        }
    }
    json!({
        "version": num(OSM_CATEGORY_LEXICON_VERSION as f64),
        "facet": "type",
        "types": types,
        "aliases": aliases
    })
}

/// Singular candidates for a folded surface: the exact form, then the
/// regular plural strips.
fn lookup_keys(folded: &str) -> Vec<String> {
    let mut keys = vec![folded.to_string()];
    if let Some(stem) = folded.strip_suffix("aux") {
        keys.push(format!("{stem}al"));
    }
    if let Some(stem) = folded.strip_suffix("es") {
        keys.push(stem.to_string());
    }
    if folded.ends_with('s') || folded.ends_with('x') {
        keys.push(folded[..folded.len() - 1].to_string());
    }
    keys
}

/// `lookupCategory(lexicon, surface)`.
pub fn lookup_category(lexicon: &CategoryLexicon, surface: &str) -> Option<CategoryMatch> {
    let folded = fold(surface);
    if folded.is_empty() {
        return None;
    }
    for key in lookup_keys(&folded) {
        if let Some(entry) = lexicon.get(&key) {
            return Some(CategoryMatch {
                type_: entry.type_.clone(),
                query: entry.query.clone(),
                label: labelize(&folded),
                corrected_from: None,
                corrected_to: None,
            });
        }
    }
    None
}

fn within_one_edit16(left: &[u16], right: &[u16]) -> bool {
    if left == right {
        return true;
    }
    if left.len().abs_diff(right.len()) > 1 {
        return false;
    }
    if left.len() == right.len() {
        let mut differences = [0usize; 2];
        let mut count = 0;
        for index in 0..left.len() {
            if left[index] != right[index] {
                if count == 2 {
                    return false;
                }
                differences[count] = index;
                count += 1;
            }
        }
        if count == 1 {
            return true;
        }
        return count == 2
            && differences[1] == differences[0] + 1
            && left[differences[0]] == right[differences[1]]
            && left[differences[1]] == right[differences[0]];
    }
    let (shorter, longer) = if left.len() < right.len() { (left, right) } else { (right, left) };
    let mut short_index = 0;
    let mut long_index = 0;
    let mut edits = 0;
    while short_index < shorter.len() && long_index < longer.len() {
        if shorter[short_index] == longer[long_index] {
            short_index += 1;
            long_index += 1;
            continue;
        }
        edits += 1;
        if edits > 1 {
            return false;
        }
        long_index += 1;
    }
    true
}

/// `withinOneEdit(left, right)` over UTF-16 code units (one substitution,
/// insertion/deletion, or adjacent transposition).
pub fn within_one_edit(left: &str, right: &str) -> bool {
    if left.is_ascii() && right.is_ascii() {
        let to16 = |text: &str| text.bytes().map(u16::from).collect::<Vec<u16>>();
        return within_one_edit16(&to16(left), &to16(right));
    }
    within_one_edit16(&left.encode_utf16().collect::<Vec<_>>(), &right.encode_utf16().collect::<Vec<_>>())
}

/// The fuzzy-match tie-break between two lexicon keys: UTF-16 code-unit
/// order, as `lookupCategoryFuzzy` uses (deterministic in every runtime,
/// unlike `localeCompare`).
fn locale_compare(left: &str, right: &str) -> Ordering {
    cmp16(left, right)
}

/// `lookupCategoryFuzzy(lexicon, surface)`: one edit, words of four or more
/// code units, and every matching surface must resolve to the same type.
pub fn lookup_category_fuzzy(lexicon: &CategoryLexicon, surface: &str) -> Option<CategoryMatch> {
    let folded = fold(surface);
    let length = len16(&folded);
    if length < 4 || folded.contains(' ') {
        return None;
    }
    let folded16: Vec<u16> = folded.encode_utf16().collect();
    let mut matches: Vec<(&str, &CategoryEntry, usize)> = Vec::new();
    let mut first_type: Option<&str> = None;
    for candidate_length in [length - 1, length, length + 1] {
        for &slot in lexicon.by_length.get(&candidate_length).map(Vec::as_slice).unwrap_or(&[]) {
            let (key, entry) = &lexicon.entries[slot];
            let key16: Vec<u16> = key.encode_utf16().collect();
            if !within_one_edit16(&folded16, &key16) {
                continue;
            }
            match first_type {
                None => first_type = Some(entry.type_.as_str()),
                // Two types: the JS `types.size !== 1` gate rejects.
                Some(type_) if type_ != entry.type_ => return None,
                _ => {}
            }
            matches.push((key.as_str(), entry, candidate_length.abs_diff(length)));
        }
    }
    let (key, entry, _) = matches
        .into_iter()
        .min_by(|left, right| left.2.cmp(&right.2).then_with(|| locale_compare(left.0, right.0)))?;
    Some(CategoryMatch {
        type_: entry.type_.clone(),
        query: entry.query.clone(),
        label: labelize(&folded),
        corrected_from: Some(folded.clone()),
        corrected_to: Some(key.to_string()),
    })
}

/// `defaultCategoryLexicon()`: canonical vocabulary + aliases, built once.
pub fn default_category_lexicon() -> &'static CategoryLexicon {
    static LEXICON: OnceLock<CategoryLexicon> = OnceLock::new();
    LEXICON.get_or_init(|| build_category_lexicon(&Value::Null))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folds_and_labels_like_js() {
        assert_eq!(fold("  Crème-Glacée!! "), "creme glacee");
        assert_eq!(labelize("cafe"), "Cafe");
        assert_eq!(labelize("caf\u{e9}"), "Caf\u{c9}");
        assert_eq!(type_query_text("fast__food;x_"), "fast food x");
    }

    #[test]
    fn default_lexicon_gates_categories() {
        let lexicon = default_category_lexicon();
        let cinema = lookup_category(lexicon, "Cinémas").unwrap();
        assert_eq!((cinema.type_.as_str(), cinema.label.as_str()), ("cinema", "Cinemas"));
        assert_eq!(lookup_category(lexicon, "coffee").unwrap().type_, "cafe");
        let typo = lookup_category_fuzzy(lexicon, "cinma").unwrap();
        assert_eq!(typo.corrected_to.as_deref(), Some("cinema"));
        assert!(within_one_edit("abcd", "abdc"));
        assert!(!within_one_edit("abcd", "badc"));
    }
}
