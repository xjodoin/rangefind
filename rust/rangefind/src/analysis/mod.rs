//! Text analysis, "multi-v1" profile (`src/analysis.js`).
//!
//! One serializable profile (the manifest's `analysis` object) fully
//! determines analysis, so the analyzer rebuilt here produces the exact
//! terms the Node builder indexed. Tokenization: Unicode word runs split at
//! script boundaries; alphabetic runs become one folded, stopword-filtered,
//! light-stemmed token, Han/Kana/Hangul/Thai-class runs become overlapping
//! character bigrams.
//!
//! All lengths and offsets that the JS measures with `.length` / `.slice`
//! are UTF-16 code units here too.

pub mod stem;
pub mod stopwords;

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use serde_json::{json, Value};
use unicode_properties::general_category::{GeneralCategoryGroup, UnicodeGeneralCategory};
use unicode_script::{Script, UnicodeScript};

use crate::error::{Error, Result};
use crate::terms::expanded_terms_from_base_terms;
use crate::util::fold::fold_multi;
use crate::util::jsstr::{len16, prefix16};
use crate::util::json::to_number;

pub use stem::{stemmer_for, Stemmer, STEMMER_LANGUAGES};

/// `ANALYSIS_PROFILE_MULTI`.
pub const ANALYSIS_PROFILE_MULTI: &str = "multi-v1";

/// Mirrors the runtime's SKIP_MAX_TERMS: alternate-language expansion in
/// `queryPlan` never grows the term list past it.
pub const QUERY_TERM_BUDGET: usize = 30;

/// `DEFAULT_ANALYSIS_LANGUAGES`.
pub const DEFAULT_ANALYSIS_LANGUAGES: &[&str] = &["en", "fr"];

/// `DEFAULT_ANALYSIS_PROFILE` (= `normalizeAnalysisConfig(null)`).
pub fn default_analysis_profile() -> Value {
    json!({
        "profile": ANALYSIS_PROFILE_MULTI,
        "languages": DEFAULT_ANALYSIS_LANGUAGES,
        "primary": "en",
        "languageField": "",
        "detect": true,
        "stemming": "light",
        "stopwords": "default",
        "foldDiacritics": true,
        "minLength": 2
    })
}

/// An insertion-ordered string set: the JS `Set<string>` the analyzer
/// returns from `highlightTerms` and the highlighter consumes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TermSet {
    order: Vec<String>,
    members: HashSet<String>,
}

impl TermSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// `set.add(term)`; returns true when the term was new.
    pub fn insert(&mut self, term: impl Into<String>) -> bool {
        let term = term.into();
        if self.members.contains(&term) {
            return false;
        }
        self.members.insert(term.clone());
        self.order.push(term);
        true
    }

    pub fn contains(&self, term: &str) -> bool {
        self.members.contains(term)
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// Members in insertion order.
    pub fn iter(&self) -> std::slice::Iter<'_, String> {
        self.order.iter()
    }

    pub fn as_slice(&self) -> &[String] {
        &self.order
    }

    pub fn into_vec(self) -> Vec<String> {
        self.order
    }
}

impl<S: Into<String>> FromIterator<S> for TermSet {
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        let mut set = TermSet::new();
        for term in iter {
            set.insert(term);
        }
        set
    }
}

impl<'a> IntoIterator for &'a TermSet {
    type Item = &'a String;
    type IntoIter = std::slice::Iter<'a, String>;
    fn into_iter(self) -> Self::IntoIter {
        self.order.iter()
    }
}

/// `{ raw, term }` from `analyzeTerms`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnalyzedTerm {
    pub raw: String,
    pub term: String,
}

impl AnalyzedTerm {
    pub fn to_json(&self) -> Value {
        json!({ "raw": self.raw, "term": self.term })
    }
}

/// One alternate-language entry of `queryPlan().altPlans`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AltPlan {
    pub language: String,
    pub analyzed_terms: Vec<AnalyzedTerm>,
    pub base_terms: Vec<String>,
}

impl AltPlan {
    pub fn to_json(&self) -> Value {
        json!({
            "language": self.language,
            "analyzedTerms": self.analyzed_terms.iter().map(AnalyzedTerm::to_json).collect::<Vec<_>>(),
            "baseTerms": self.base_terms
        })
    }
}

/// `queryPlan(text)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryPlan {
    pub language: String,
    pub analyzed_terms: Vec<AnalyzedTerm>,
    pub base_terms: Vec<String>,
    pub terms: Vec<String>,
    pub alt_plans: Vec<AltPlan>,
}

impl QueryPlan {
    /// The JS object shape (`{ language, analyzedTerms, baseTerms, terms, altPlans }`).
    pub fn to_json(&self) -> Value {
        json!({
            "language": self.language,
            "analyzedTerms": self.analyzed_terms.iter().map(AnalyzedTerm::to_json).collect::<Vec<_>>(),
            "baseTerms": self.base_terms,
            "terms": self.terms,
            "altPlans": self.alt_plans.iter().map(AltPlan::to_json).collect::<Vec<_>>()
        })
    }
}

// ---------------------------------------------------------------- chars

/// `/[\p{L}\p{M}\p{N}ー々]/u`
pub fn is_word_char(ch: char) -> bool {
    ch == 'ー'
        || ch == '々'
        || matches!(
            ch.general_category_group(),
            GeneralCategoryGroup::Letter | GeneralCategoryGroup::Mark | GeneralCategoryGroup::Number
        )
}

/// `BIGRAM_CHAR_RE`: Han/Hiragana/Katakana/Hangul/Thai/Lao/Khmer/Myanmar
/// (Script property, not Script_Extensions), plus ー and 々.
pub fn is_bigram_char(ch: char) -> bool {
    ch == 'ー'
        || ch == '々'
        || matches!(
            ch.script(),
            Script::Han
                | Script::Hiragana
                | Script::Katakana
                | Script::Hangul
                | Script::Thai
                | Script::Lao
                | Script::Khmer
                | Script::Myanmar
        )
}

/// One `WORD_RE` match: the word and its start offset in UTF-16 units.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WordMatch<'a> {
    pub word: &'a str,
    pub index16: usize,
}

/// `text.matchAll(/[\p{L}\p{M}\p{N}ー々]+/gu)` with UTF-16 match indexes.
pub fn word_matches(text: &str) -> Vec<WordMatch<'_>> {
    let mut out = Vec::new();
    let mut start: Option<(usize, usize)> = None;
    let mut offset16 = 0usize;
    for (byte, ch) in text.char_indices() {
        if is_word_char(ch) {
            if start.is_none() {
                start = Some((byte, offset16));
            }
        } else if let Some((begin, begin16)) = start.take() {
            out.push(WordMatch { word: &text[begin..byte], index16: begin16 });
        }
        offset16 += ch.len_utf16();
    }
    if let Some((begin, begin16)) = start {
        out.push(WordMatch { word: &text[begin..], index16: begin16 });
    }
    out
}

/// `splitScriptRuns(word)`: maximal runs of bigram / non-bigram chars.
fn split_script_runs(word: &str) -> Vec<(&str, bool)> {
    let mut runs = Vec::new();
    let mut start = 0usize;
    let mut current: Option<bool> = None;
    for (byte, ch) in word.char_indices() {
        let bigram = is_bigram_char(ch);
        match current {
            Some(kind) if kind == bigram => {}
            Some(kind) => {
                runs.push((&word[start..byte], kind));
                start = byte;
                current = Some(bigram);
            }
            None => current = Some(bigram),
        }
    }
    if let Some(kind) = current {
        runs.push((&word[start..], kind));
    }
    runs
}

fn bigram_tokens(run: &str, out: &mut Vec<String>) {
    let chars: Vec<char> = run.chars().collect();
    if chars.len() == 1 {
        out.push(chars[0].to_string());
        return;
    }
    for pair in chars.windows(2) {
        let mut token = String::with_capacity(8);
        token.push(pair[0]);
        token.push(pair[1]);
        out.push(token);
    }
}

fn is_single_digit(value: &str) -> bool {
    value.len() == 1 && value.as_bytes()[0].is_ascii_digit()
}

fn script_for_language(language: &str) -> &'static str {
    match language {
        "ru" | "uk" | "bg" | "sr" | "be" | "mk" => "cyrillic",
        "el" => "greek",
        "ar" | "fa" | "ur" => "arabic",
        "he" | "yi" => "hebrew",
        "hi" | "mr" | "ne" => "devanagari",
        "th" => "thai",
        "zh" => "han",
        "ja" => "kana",
        "ko" => "hangul",
        _ => "latin",
    }
}

/// `SCRIPT_RES` key for a char, if any.
fn script_key(ch: char) -> Option<&'static str> {
    Some(match ch.script() {
        Script::Latin => "latin",
        Script::Cyrillic => "cyrillic",
        Script::Greek => "greek",
        Script::Arabic => "arabic",
        Script::Hebrew => "hebrew",
        Script::Devanagari => "devanagari",
        Script::Han => "han",
        Script::Hiragana | Script::Katakana => "kana",
        Script::Hangul => "hangul",
        Script::Thai => "thai",
        _ => return None,
    })
}

fn raw_stopwords(language: &str) -> &'static [&'static str] {
    stopwords::RAW_STOPWORDS
        .iter()
        .find(|(code, _)| *code == language)
        .map(|(_, words)| *words)
        .unwrap_or(&[])
}

// ---------------------------------------------------------------- analyzer

/// The analyzer `createAnalyzer(profile)` returns.
#[derive(Clone, Debug)]
pub struct Analyzer {
    /// The profile object this analyzer was built from (the default profile
    /// when the manifest had none).
    pub profile: Value,
    /// `profile.languages` (string entries).
    pub languages: Vec<String>,
    /// `profile.primary` ("" when absent).
    pub primary: String,
    /// `profile.stemming === "light"`.
    pub stemming: bool,
    /// `profile.stopwords !== "off"`.
    pub stopwords: bool,
    /// `profile.foldDiacritics !== false`.
    pub fold_diacritics: bool,
    /// `profile.minLength` as a JS number (NaN when absent: every length
    /// comparison against it is then false, like `x < undefined`).
    pub min_length: f64,
    /// Folded stopword lists for the profile languages and the primary.
    stopword_sets: HashMap<String, HashSet<String>>,
}

static EMPTY_SET: OnceLock<HashSet<String>> = OnceLock::new();

fn empty_set() -> &'static HashSet<String> {
    EMPTY_SET.get_or_init(HashSet::new)
}

/// `DEFAULT_ANALYZER`.
pub fn default_analyzer() -> &'static Analyzer {
    static DEFAULT: OnceLock<Analyzer> = OnceLock::new();
    DEFAULT.get_or_init(|| Analyzer::from_profile(default_analysis_profile()))
}

/// `createAnalyzer(profile)`: a falsy profile yields the default analyzer;
/// any profile other than "multi-v1" is an error.
pub fn create_analyzer(profile: &Value) -> Result<Analyzer> {
    if !crate::util::json::truthy(profile) {
        return Ok(default_analyzer().clone());
    }
    let name = profile.get("profile").unwrap_or(&Value::Null);
    if name.as_str() != Some(ANALYSIS_PROFILE_MULTI) {
        let shown = if profile.get("profile").is_none() { "undefined".to_string() } else { js_template_string(name) };
        return Err(Error::new(format!(
            "Rangefind analysis profile \"{shown}\" is not supported by this runtime; upgrade rangefind."
        )));
    }
    Ok(Analyzer::from_profile(profile.clone()))
}

/// `${value}` for a JSON value.
fn js_template_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => crate::util::json::js_number_to_string(number.as_f64().unwrap_or(f64::NAN)),
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| if item.is_null() { String::new() } else { js_template_string(item) })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_string(),
    }
}

/// `analyzerFromManifest(manifest)`: `createAnalyzer(manifest.analysis || null)`.
pub fn analyzer_from_manifest(manifest: &Value) -> Result<Analyzer> {
    if !manifest.is_object() {
        return Ok(default_analyzer().clone());
    }
    create_analyzer(manifest.get("analysis").unwrap_or(&Value::Null))
}

impl Analyzer {
    fn from_profile(profile: Value) -> Analyzer {
        let languages: Vec<String> = profile
            .get("languages")
            .and_then(Value::as_array)
            .map(|items| items.iter().filter_map(|item| item.as_str().map(str::to_string)).collect())
            .unwrap_or_default();
        let primary = profile.get("primary").and_then(Value::as_str).unwrap_or("").to_string();
        let stemming = profile.get("stemming").and_then(Value::as_str) == Some("light");
        let stopwords = profile.get("stopwords").and_then(Value::as_str) != Some("off");
        let fold_diacritics = profile.get("foldDiacritics") != Some(&Value::Bool(false));
        let min_length = match profile.get("minLength") {
            None => f64::NAN,
            Some(value) => to_number(value),
        };
        let mut analyzer = Analyzer {
            profile,
            languages,
            primary,
            stemming,
            stopwords,
            fold_diacritics,
            min_length,
            stopword_sets: HashMap::new(),
        };
        let mut wanted: Vec<String> = analyzer.languages.clone();
        wanted.push(analyzer.primary.clone());
        for language in wanted {
            if analyzer.stopword_sets.contains_key(&language) {
                continue;
            }
            // `.filter(Boolean)` drops nothing for the shipped lists, and
            // membership is only ever tested with non-empty words.
            let set: HashSet<String> = raw_stopwords(&language)
                .iter()
                .map(|word| analyzer.fold(word))
                .filter(|word| !word.is_empty())
                .collect();
            analyzer.stopword_sets.insert(language, set);
        }
        analyzer
    }

    /// `analyzer.fold(text)`: `foldMulti` with the profile's diacritics flag.
    pub fn fold(&self, text: &str) -> String {
        fold_multi(text, self.fold_diacritics)
    }

    fn has_language(&self, language: &str) -> bool {
        self.languages.iter().any(|item| item == language)
    }

    /// The folded raw list (used both for filtering and stopword votes).
    fn folded_list(&self, language: &str) -> &HashSet<String> {
        self.stopword_sets.get(language).unwrap_or_else(|| empty_set())
    }

    fn stopwords_for(&self, language: &str) -> &HashSet<String> {
        if !self.stopwords {
            return empty_set();
        }
        self.folded_list(language)
    }

    fn stem_for(&self, language: &str) -> Option<Stemmer> {
        if self.stemming {
            stemmer_for(language)
        } else {
            None
        }
    }

    fn below_min(&self, value: &str) -> bool {
        (len16(value) as f64) < self.min_length && !is_single_digit(value)
    }

    fn normalize_alpha_token(&self, raw: &str, stopwords: &HashSet<String>, stemmer: Option<Stemmer>) -> String {
        if self.below_min(raw) || stopwords.contains(raw) {
            return String::new();
        }
        let term = match stemmer {
            Some(stem) => stem(raw),
            None => raw.to_string(),
        };
        if self.below_min(&term) || stopwords.contains(&term) {
            return String::new();
        }
        term
    }

    /// `emitTokens`: `(raw, term)` pairs in text order.
    fn emit_tokens(&self, text: &str, language: &str, mut callback: impl FnMut(&str, &str)) {
        let lang = if self.has_language(language) { language } else { self.primary.as_str() };
        let stopwords = self.stopwords_for(lang);
        let stemmer = self.stem_for(lang);
        let folded = self.fold(text);
        let mut bigrams = Vec::new();
        for found in word_matches(&folded) {
            for (run, bigram) in split_script_runs(found.word) {
                if bigram {
                    bigrams.clear();
                    bigram_tokens(run, &mut bigrams);
                    for token in &bigrams {
                        callback(token, token);
                    }
                } else {
                    let term = self.normalize_alpha_token(run, stopwords, stemmer);
                    if !term.is_empty() {
                        callback(run, &term);
                    }
                }
            }
        }
    }

    /// `tokenize(text, { lang, unique })`. `lang` "" means unspecified (the
    /// primary language); `unique` is the JS default `true` unless the
    /// caller passed `unique: false`.
    pub fn tokenize(&self, text: &str, lang: &str, unique: bool) -> Vec<String> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        self.emit_tokens(text, lang, |_, term| {
            if unique && !seen.insert(term.to_string()) {
                return;
            }
            out.push(term.to_string());
        });
        out
    }

    /// `termCounts(text, { lang })` as `[term, count]` in first-seen order.
    pub fn term_counts(&self, text: &str, lang: &str) -> Vec<(String, usize)> {
        let mut out: Vec<(String, usize)> = Vec::new();
        let mut index: HashMap<String, usize> = HashMap::new();
        self.emit_tokens(text, lang, |_, term| match index.get(term) {
            Some(&slot) => out[slot].1 += 1,
            None => {
                index.insert(term.to_string(), out.len());
                out.push((term.to_string(), 1));
            }
        });
        out
    }

    /// `analyzeTerms(text, { lang })`.
    pub fn analyze_terms(&self, text: &str, lang: &str) -> Vec<AnalyzedTerm> {
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        self.emit_tokens(text, lang, |raw, term| {
            if seen.insert(term.to_string()) {
                out.push(AnalyzedTerm { raw: raw.to_string(), term: term.to_string() });
            }
        });
        out
    }

    /// Script histogram over the first 1600 code units, in first-seen
    /// script order (`Object.entries` insertion order).
    fn script_counts(text: &str) -> Vec<(&'static str, usize)> {
        let sample = prefix16(text, 1600);
        let mut counts: Vec<(&'static str, usize)> = Vec::new();
        for ch in sample.chars() {
            if let Some(key) = script_key(ch) {
                match counts.iter_mut().find(|(script, _)| *script == key) {
                    Some(entry) => entry.1 += 1,
                    None => counts.push((key, 1)),
                }
            }
        }
        counts
    }

    fn stopword_vote(&self, text: &str, candidates: &[&str]) -> String {
        if candidates.len() == 1 {
            return candidates[0].to_string();
        }
        let folded = self.fold(&prefix16(text, 1600));
        let words = word_matches(&folded);
        let mut best = "";
        let mut best_hits = 0usize;
        let mut second_hits = 0usize;
        for language in candidates {
            // With stopwords "off" the vote still uses the folded raw lists.
            let set = self.folded_list(language);
            let hits = words.iter().filter(|found| set.contains(found.word)).count();
            if hits > best_hits {
                second_hits = best_hits;
                best_hits = hits;
                best = language;
            } else if hits > second_hits {
                second_hits = hits;
            }
        }
        if best_hits >= 2 && best_hits > second_hits {
            best.to_string()
        } else {
            String::new()
        }
    }

    /// `detectLanguage(text)`: a profile language code or "".
    pub fn detect_language(&self, text: &str) -> String {
        let counts = Self::script_counts(text);
        let count_of = |key: &str| counts.iter().find(|(script, _)| *script == key).map(|(_, n)| *n).unwrap_or(0);
        let kana = count_of("kana");
        let han = count_of("han");
        let hangul = count_of("hangul");
        if kana > 0 && self.has_language("ja") {
            return "ja".into();
        }
        if hangul > 0 && self.has_language("ko") {
            return "ko".into();
        }
        if han > 0 {
            if self.has_language("zh") {
                return "zh".into();
            }
            if self.has_language("ja") {
                return "ja".into();
            }
        }
        let mut dominant = "";
        let mut dominant_count = 0usize;
        for (script, count) in &counts {
            if matches!(*script, "han" | "kana" | "hangul") {
                continue;
            }
            if *count > dominant_count {
                dominant = script;
                dominant_count = *count;
            }
        }
        if dominant.is_empty() {
            return String::new();
        }
        let candidates: Vec<&str> = self
            .languages
            .iter()
            .map(String::as_str)
            .filter(|language| script_for_language(language) == dominant)
            .collect();
        match candidates.len() {
            0 => String::new(),
            1 => candidates[0].to_string(),
            _ => self.stopword_vote(text, &candidates),
        }
    }

    /// `queryPlan(text)`.
    pub fn query_plan(&self, text: &str) -> QueryPlan {
        let detected = self.detect_language(text);
        let language = if detected.is_empty() { self.primary.clone() } else { detected };
        let analyzed_terms = self.analyze_terms(text, &language);
        let base_terms: Vec<String> = analyzed_terms.iter().map(|item| item.term.clone()).collect();
        let mut terms: TermSet = expanded_terms_from_base_terms(&base_terms).into_iter().collect();
        let mut alt_plans: Vec<AltPlan> = Vec::new();
        let mut seen_plans: HashSet<String> = HashSet::new();
        seen_plans.insert(base_terms.join("\0"));
        for alt in &self.languages {
            if *alt == language {
                continue;
            }
            let alt_analyzed = self.analyze_terms(text, alt);
            if alt_analyzed.is_empty() {
                continue;
            }
            let alt_base: Vec<String> = alt_analyzed.iter().map(|item| item.term.clone()).collect();
            if !seen_plans.insert(alt_base.join("\0")) {
                continue;
            }
            alt_plans.push(AltPlan { language: alt.clone(), analyzed_terms: alt_analyzed, base_terms: alt_base });
        }
        for alt in &alt_plans {
            for term in &alt.base_terms {
                if terms.len() >= QUERY_TERM_BUDGET {
                    break;
                }
                terms.insert(term.clone());
            }
        }
        for alt in &alt_plans {
            for term in expanded_terms_from_base_terms(&alt.base_terms) {
                if terms.len() >= QUERY_TERM_BUDGET {
                    break;
                }
                terms.insert(term);
            }
        }
        QueryPlan { language, analyzed_terms, base_terms, terms: terms.into_vec(), alt_plans }
    }

    /// `queryTerms(text)`.
    pub fn query_terms(&self, text: &str) -> Vec<String> {
        self.query_plan(text).terms
    }

    /// `highlightTerms(query, correctedQuery)`: every profile language's
    /// analysis of the query (and the correction, when non-empty).
    pub fn highlight_terms(&self, query: &str, corrected_query: &str) -> TermSet {
        let mut terms = TermSet::new();
        for language in &self.languages {
            for item in self.analyze_terms(query, language) {
                terms.insert(item.term);
            }
            if !corrected_query.is_empty() {
                for item in self.analyze_terms(corrected_query, language) {
                    terms.insert(item.term);
                }
            }
        }
        terms
    }

    /// `wordMatchRanges(word, termSet)`: `[start, end)` match ranges inside
    /// one raw display word, in UTF-16 units relative to the word start.
    pub fn word_match_ranges(&self, word: &str, term_set: &TermSet) -> Vec<[usize; 2]> {
        let mut ranges: Vec<[usize; 2]> = Vec::new();
        let mut offset = 0usize;
        for (run, bigram) in split_script_runs(word) {
            if bigram {
                let chars: Vec<char> = run.chars().collect();
                if chars.len() == 1 {
                    let mut buffer = [0u8; 4];
                    if term_set.contains(chars[0].encode_utf8(&mut buffer)) {
                        ranges.push([offset, offset + chars[0].len_utf16()]);
                    }
                } else {
                    let mut char_offset = offset;
                    let mut pair = String::with_capacity(8);
                    for i in 0..chars.len() - 1 {
                        pair.clear();
                        pair.push(chars[i]);
                        pair.push(chars[i + 1]);
                        if term_set.contains(&pair) {
                            let start = char_offset;
                            let end = char_offset + chars[i].len_utf16() + chars[i + 1].len_utf16();
                            match ranges.last_mut() {
                                Some(previous) if previous[1] >= start => previous[1] = end,
                                _ => ranges.push([start, end]),
                            }
                        }
                        char_offset += chars[i].len_utf16();
                    }
                }
                offset += len16(run);
            } else {
                let folded = self.fold(run);
                let mut matched = term_set.contains(&folded);
                if !matched && self.stemming {
                    for language in &self.languages {
                        if let Some(stem) = stemmer_for(language) {
                            if term_set.contains(&stem(&folded)) {
                                matched = true;
                                break;
                            }
                        }
                    }
                }
                let length = len16(run);
                if matched {
                    ranges.push([offset, offset + length]);
                }
                offset += length;
            }
        }
        ranges
    }

    /// `canonicalTerm(word)`: folded, then primary-language stem.
    pub fn canonical_term(&self, word: &str) -> String {
        let folded = self.fold(word);
        match self.stem_for(&self.primary) {
            Some(stem) => stem(&folded),
            None => folded,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_profile_tokenizes() {
        let analyzer = default_analyzer();
        assert_eq!(analyzer.tokenize("The quick brown foxes", "", true), vec!["quick", "brown", "foxe"]);
        assert_eq!(analyzer.tokenize("東京都", "", true), vec!["東京", "京都"]);
        assert_eq!(analyzer.detect_language("les chats et les chiens"), "fr");
    }

    #[test]
    fn unsupported_profile_errors() {
        let error = create_analyzer(&json!({ "profile": "multi-v2" })).unwrap_err();
        assert_eq!(
            error.message(),
            "Rangefind analysis profile \"multi-v2\" is not supported by this runtime; upgrade rangefind."
        );
    }

    #[test]
    fn word_offsets_are_utf16() {
        let found = word_matches("😀 ab é");
        assert_eq!(found.iter().map(|m| m.index16).collect::<Vec<_>>(), vec![3, 6]);
    }
}
