//! Query-term highlighting (`src/highlight.js`).
//!
//! Word scanning runs over the raw display text; every offset (match
//! ranges, snippet windows, `maxChars`) is in UTF-16 code units, exactly as
//! the JS computes them, so hosts can apply ranges with JS string indexes.

use std::collections::HashSet;

use serde_json::{json, Map, Value};

use crate::analysis::{default_analyzer, word_matches, Analyzer, TermSet};
use crate::util::jsstr::{from16, to16};

/// `highlightTermSet(query, correctedQuery, analyzer)`.
pub fn highlight_term_set(query: &str, corrected_query: &str, analyzer: Option<&Analyzer>) -> TermSet {
    analyzer.unwrap_or_else(|| default_analyzer()).highlight_terms(query, corrected_query)
}

/// `findMatchRanges(text, termSet, analyzer)`: `[start, end)` UTF-16 ranges
/// of query-term occurrences in raw text.
pub fn find_match_ranges(text: &str, term_set: &TermSet, analyzer: Option<&Analyzer>) -> Vec<[usize; 2]> {
    let analyzer = analyzer.unwrap_or_else(|| default_analyzer());
    let mut ranges = Vec::new();
    if term_set.is_empty() {
        return ranges;
    }
    for found in word_matches(text) {
        for [start, end] in analyzer.word_match_ranges(found.word, term_set) {
            ranges.push([found.index16 + start, found.index16 + end]);
        }
    }
    ranges
}

/// A snippet: `{ text, ranges, matches }`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Highlight {
    pub text: String,
    pub ranges: Vec<[usize; 2]>,
    pub matches: usize,
}

impl Highlight {
    pub fn to_json(&self) -> Value {
        json!({ "text": self.text, "ranges": self.ranges, "matches": self.matches })
    }
}

/// `Math.max` / `Math.min` (NaN-propagating).
fn js_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}

fn js_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.min(b)
    }
}

/// `ToIntegerOrInfinity` clamped into `[0, length]` (`String.prototype.slice`
/// argument handling for the non-negative offsets used here).
fn slice_index(value: f64, length: usize) -> usize {
    let value = if value.is_nan() { 0.0 } else { value.trunc() };
    let length_f = length as f64;
    if value < 0.0 {
        (length_f + value).max(0.0) as usize
    } else {
        value.min(length_f) as usize
    }
}

fn slice16(units: &[u16], from: f64, to: f64) -> String {
    let start = slice_index(from, units.len());
    let end = slice_index(to, units.len());
    if start >= end {
        return String::new();
    }
    from16(&units[start..end])
}

fn distinct_terms_in(units: &[u16], ranges: &[[usize; 2]], from: f64, to: f64, analyzer: &Analyzer) -> usize {
    let mut seen = HashSet::new();
    for &[start, end] in ranges {
        if (start as f64) < from || (end as f64) > to {
            continue;
        }
        seen.insert(analyzer.canonical_term(&from16(&units[start..end])));
    }
    seen.len()
}

/// `raw.indexOf(" ", from)`.
fn index_of_space(units: &[u16], from: f64) -> Option<usize> {
    let start = slice_index(from, units.len());
    units[start..].iter().position(|unit| *unit == 0x20).map(|index| index + start)
}

/// `raw.lastIndexOf(" ", to)`.
fn last_index_of_space(units: &[u16], to: f64) -> Option<usize> {
    if units.is_empty() {
        return None;
    }
    let to = if to.is_nan() { f64::INFINITY } else { to.trunc() };
    if to < 0.0 {
        return None;
    }
    let last = (to.min((units.len() - 1) as f64)) as usize;
    units[..=last].iter().rposition(|unit| *unit == 0x20)
}

/// `highlightText(text, termSet, { maxChars, analyzer })`. `max_chars` is
/// the JS `options.maxChars` already passed through `Number()`; `None`
/// stands for `undefined`/`null` (default 240).
pub fn highlight_text(
    text: &str,
    term_set: &TermSet,
    max_chars: Option<f64>,
    analyzer: Option<&Analyzer>,
) -> Option<Highlight> {
    let analyzer = analyzer.unwrap_or_else(|| default_analyzer());
    if text.is_empty() {
        return None;
    }
    let max_chars = js_max(40.0, max_chars.unwrap_or(240.0).floor());
    let ranges = find_match_ranges(text, term_set, Some(analyzer));
    if ranges.is_empty() {
        return None;
    }
    let units = to16(text);
    let length = units.len() as f64;

    let mut window_start = 0.0f64;
    if length > max_chars {
        let (mut best_distinct, mut best_count, mut best_start) = (-1i64, -1i64, 0.0f64);
        // Slide over match positions: each candidate window starts a little
        // before one of the matches.
        for &[start, _] in &ranges {
            let candidate = js_max(0.0, js_min(start as f64 - 30.0, length - max_chars));
            let to = candidate + max_chars;
            let distinct = distinct_terms_in(&units, &ranges, candidate, to, analyzer) as i64;
            let count = ranges.iter().filter(|[s, e]| *s as f64 >= candidate && *e as f64 <= to).count() as i64;
            if distinct > best_distinct || (distinct == best_distinct && count > best_count) {
                best_distinct = distinct;
                best_count = count;
                best_start = candidate;
            }
        }
        window_start = best_start;
    }

    let mut from = window_start;
    let mut to = js_min(length, from + max_chars);
    // Snap to word boundaries so the snippet never opens or closes mid-word.
    if from > 0.0 {
        if let Some(next_space) = index_of_space(&units, from) {
            if (next_space as f64) < from + 24.0 {
                from = next_space as f64 + 1.0;
            }
        }
    }
    if to < length {
        // lastIndexOf returns -1 when absent, which never beats from + max/2.
        if let Some(previous_space) = last_index_of_space(&units, to) {
            if previous_space as f64 > from + max_chars / 2.0 {
                to = previous_space as f64;
            }
        }
    }

    let prefix = if from > 0.0 { "… " } else { "" };
    let prefix_length = if from > 0.0 { 2.0 } else { 0.0 };
    let suffix = if to < length { " …" } else { "" };
    let mut snippet_ranges = Vec::new();
    for &[start, end] in &ranges {
        if (start as f64) < from || (end as f64) > to {
            continue;
        }
        snippet_ranges.push([
            (start as f64 - from + prefix_length) as usize,
            (end as f64 - from + prefix_length) as usize,
        ]);
    }
    if snippet_ranges.is_empty() {
        return None;
    }
    let matches = snippet_ranges.len();
    Some(Highlight { text: format!("{prefix}{}{suffix}", slice16(&units, from, to)), ranges: snippet_ranges, matches })
}

/// Options for [`apply_highlights`] (`{ fields, maxChars, analyzer }`).
#[derive(Clone, Debug, Default)]
pub struct HighlightOptions<'a> {
    /// Fields to highlight; `None` or empty means every string field except
    /// `id` and `url`.
    pub fields: Option<Vec<String>>,
    /// `Number(options.maxChars)`; `None` for undefined/null.
    pub max_chars: Option<f64>,
    /// Defaults to the default analyzer.
    pub analyzer: Option<&'a Analyzer>,
}

/// JS array-index property key ("0".."4294967294" in canonical form).
fn array_index(key: &str) -> Option<u32> {
    if key.is_empty() || key.len() > 10 || !key.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if key.len() > 1 && key.starts_with('0') {
        return None;
    }
    key.parse::<u64>().ok().filter(|n| *n < 4_294_967_295).map(|n| n as u32)
}

/// Property keys in JS `Object.entries` order: array indexes ascending,
/// then string keys in insertion order.
fn js_key_order(map: &Map<String, Value>) -> Vec<&String> {
    let mut indexed: Vec<(u32, &String)> = Vec::new();
    let mut named: Vec<&String> = Vec::new();
    for key in map.keys() {
        match array_index(key) {
            Some(index) => indexed.push((index, key)),
            None => named.push(key),
        }
    }
    indexed.sort_by_key(|(index, _)| *index);
    indexed.into_iter().map(|(_, key)| key).chain(named).collect()
}

/// `applyHighlights(results, termSet, options)`: sets
/// `highlights: { field: { text, ranges, matches } }` on every result with
/// at least one highlighted field.
pub fn apply_highlights(results: &mut [Value], term_set: &TermSet, options: &HighlightOptions<'_>) {
    let wanted = options.fields.as_ref().filter(|fields| !fields.is_empty());
    let analyzer = options.analyzer.unwrap_or_else(|| default_analyzer());
    for result in results.iter_mut() {
        let Some(map) = result.as_object_mut() else { continue };
        let mut highlights = Map::new();
        for field in js_key_order(map) {
            let Some(value) = map.get(field).and_then(Value::as_str) else { continue };
            if value.is_empty() {
                continue;
            }
            let skip = match wanted {
                Some(fields) => !fields.iter().any(|wanted| wanted == field),
                None => field == "id" || field == "url",
            };
            if skip {
                continue;
            }
            if let Some(highlight) = highlight_text(value, term_set, options.max_chars, Some(analyzer)) {
                highlights.insert(field.clone(), highlight.to_json());
            }
        }
        if !highlights.is_empty() {
            map.insert("highlights".to_string(), Value::Object(highlights));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_follow_the_analyzer() {
        let terms = highlight_term_set("montreal walking", "", None);
        let text = "Walk from Montréal to MONTREAL-Est, walked far.";
        let ranges = find_match_ranges(text, &terms, None);
        let units = to16(text);
        let words: Vec<String> = ranges.iter().map(|[s, e]| from16(&units[*s..*e])).collect();
        assert_eq!(words, vec!["Walk", "Montréal", "MONTREAL", "walked"]);
    }

    #[test]
    fn no_match_is_none() {
        let terms = highlight_term_set("zebra", "", None);
        assert_eq!(highlight_text("Nothing to see", &terms, None, None), None);
        assert_eq!(highlight_text("", &terms, None, None), None);
    }
}
