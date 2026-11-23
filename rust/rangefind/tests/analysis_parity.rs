//! Parity with the JS analyzer, stemmers, terms and highlighter. Fixtures
//! come from `node rust/tools/fixtures/analysis.mjs` (run from the repo root).

#![cfg(feature = "analysis")]

use std::fs;
use std::path::PathBuf;

use rangefind::analysis::stopwords::RAW_STOPWORDS;
use rangefind::analysis::{create_analyzer, analyzer_from_manifest, default_analyzer, stemmer_for, Analyzer, TermSet};
use rangefind::highlight::{apply_highlights, find_match_ranges, highlight_term_set, highlight_text, HighlightOptions};
use rangefind::terms::{
    expanded_terms_from_base_terms, proximity_term, query_bundle_key_from_base_terms, query_bundle_keys_from_base_terms,
};
use serde_json::{json, Value};

fn fixture(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/fixtures/analysis").join(name);
    let text = fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing fixture {}: run `node rust/tools/fixtures/analysis.mjs` from the repository root",
            path.display()
        )
    });
    serde_json::from_str(&text).expect("fixture JSON")
}

fn strings(value: &Value) -> Vec<String> {
    value.as_array().expect("array").iter().map(|item| item.as_str().expect("string").to_string()).collect()
}

fn ranges_json(ranges: &[[usize; 2]]) -> Value {
    json!(ranges)
}

/// Collects mismatches so one run reports many at once.
struct Report {
    name: &'static str,
    checked: usize,
    failures: Vec<String>,
}

impl Report {
    fn new(name: &'static str) -> Self {
        Report { name, checked: 0, failures: Vec::new() }
    }

    fn check<T: PartialEq + std::fmt::Debug>(&mut self, label: impl FnOnce() -> String, actual: T, expected: T) {
        self.checked += 1;
        if actual != expected && self.failures.len() < 40 {
            self.failures.push(format!("{}\n    rust: {:?}\n    js:   {:?}", label(), actual, expected));
        } else if actual != expected {
            self.failures.push(String::new());
        }
    }

    fn finish(self) {
        eprintln!("{}: {} checks", self.name, self.checked);
        if !self.failures.is_empty() {
            let shown: Vec<&String> = self.failures.iter().filter(|f| !f.is_empty()).collect();
            panic!(
                "{}: {} of {} checks differ from JS:\n{}",
                self.name,
                self.failures.len(),
                self.checked,
                shown.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n")
            );
        }
    }
}

fn analyzers_by_name() -> Vec<(String, Value)> {
    fixture("analyzer.json")
        .as_array()
        .unwrap()
        .iter()
        .map(|profile| (profile["name"].as_str().unwrap().to_string(), profile["profile"].clone()))
        .collect()
}

fn analyzer_named(name: &str, profiles: &[(String, Value)]) -> Option<Analyzer> {
    let (_, profile) = profiles.iter().find(|(candidate, _)| candidate == name).expect("profile name");
    if profile.is_null() {
        None
    } else {
        Some(create_analyzer(profile).unwrap())
    }
}

#[test]
fn stopword_table_matches_js() {
    let lists = fixture("stopwords.json");
    let lists = lists.as_object().unwrap();
    assert_eq!(lists.len(), RAW_STOPWORDS.len());
    for ((language, words), (rust_language, rust_words)) in lists.iter().zip(RAW_STOPWORDS) {
        assert_eq!(language, rust_language);
        assert_eq!(strings(words), rust_words.iter().map(|w| w.to_string()).collect::<Vec<_>>(), "{language}");
    }
}

#[test]
fn stemmers_match_js() {
    let data = fixture("stemmers.json");
    assert_eq!(
        strings(&data["languages"]),
        rangefind::analysis::STEMMER_LANGUAGES.iter().map(|l| l.to_string()).collect::<Vec<_>>()
    );
    let mut report = Report::new("stemmers");
    for (language, cases) in data["stemmers"].as_object().unwrap() {
        let stemmer = stemmer_for(language);
        for case in cases.as_array().unwrap() {
            let input = case[0].as_str().unwrap();
            let expected = case[1].as_str().map(str::to_string);
            report.check(|| format!("{language} {input:?}"), stemmer.map(|stem| stem(input)), expected);
        }
    }
    report.finish();
}

#[test]
fn analyzer_matches_js() {
    let data = fixture("analyzer.json");
    let mut report = Report::new("analyzer");
    for profile in data.as_array().unwrap() {
        let name = profile["name"].as_str().unwrap();
        let analyzer = create_analyzer(&profile["profile"]).unwrap();
        for case in profile["cases"].as_array().unwrap() {
            let text = case["text"].as_str().unwrap();
            let lang = case["lang"].as_str().unwrap();
            let corrected = case["corrected"].as_str().unwrap();
            let label = |what: &str| format!("[{name}] {what} {text:?} lang={lang:?}");
            report.check(|| label("fold"), analyzer.fold(text), case["fold"].as_str().unwrap().to_string());
            report.check(|| label("tokenize"), analyzer.tokenize(text, "", true), strings(&case["tokenize"]));
            report.check(|| label("tokenizeAll"), analyzer.tokenize(text, lang, false), strings(&case["tokenizeAll"]));
            let counts: Vec<Value> =
                analyzer.term_counts(text, lang).into_iter().map(|(term, count)| json!([term, count])).collect();
            report.check(|| label("termCounts"), Value::Array(counts), case["termCounts"].clone());
            let analyzed: Vec<Value> = analyzer.analyze_terms(text, lang).iter().map(|item| item.to_json()).collect();
            report.check(|| label("analyzeTerms"), Value::Array(analyzed), case["analyzeTerms"].clone());
            report.check(|| label("detect"), analyzer.detect_language(text), case["detect"].as_str().unwrap().to_string());
            report.check(|| label("queryPlan"), analyzer.query_plan(text).to_json(), case["plan"].clone());
            report.check(|| label("queryTerms"), analyzer.query_terms(text), strings(&case["queryTerms"]));
            let highlight = analyzer.highlight_terms(text, corrected);
            report.check(|| label("highlightTerms"), highlight.as_slice().to_vec(), strings(&case["highlight"]));
            let other_set: TermSet = strings(&case["otherSet"]).into_iter().collect();
            report.check(
                || label("canonical"),
                analyzer.canonical_term(text),
                case["canonical"].as_str().unwrap().to_string(),
            );
            let words = case["words"].as_array().unwrap();
            let found = rangefind::analysis::word_matches(text);
            report.check(
                || label("words"),
                found.iter().map(|m| m.word.to_string()).collect::<Vec<_>>(),
                words.iter().map(|w| w["word"].as_str().unwrap().to_string()).collect::<Vec<_>>(),
            );
            for word in words {
                let raw = word["word"].as_str().unwrap();
                report.check(
                    || label(&format!("wordMatchRanges {raw:?}")),
                    ranges_json(&analyzer.word_match_ranges(raw, &highlight)),
                    word["ranges"].clone(),
                );
                report.check(
                    || label(&format!("wordMatchRanges(other) {raw:?}")),
                    ranges_json(&analyzer.word_match_ranges(raw, &other_set)),
                    word["other"].clone(),
                );
                report.check(
                    || label(&format!("canonicalTerm {raw:?}")),
                    analyzer.canonical_term(raw),
                    word["canonical"].as_str().unwrap().to_string(),
                );
            }
        }
    }
    report.finish();
}

#[test]
fn errors_and_manifests_match_js() {
    let data = fixture("errors.json");
    for case in data["errors"].as_array().unwrap() {
        let result = create_analyzer(&case["profile"]);
        match case["error"].as_str() {
            Some(message) => assert_eq!(result.unwrap_err().message(), message),
            None => assert!(result.is_ok()),
        }
    }
    for case in data["manifests"].as_array().unwrap() {
        let analyzer = analyzer_from_manifest(&case["manifest"]).unwrap();
        assert_eq!(analyzer.languages, strings(&case["languages"]), "{}", case["manifest"]);
        if case["default"].as_bool().unwrap() {
            assert_eq!(analyzer.profile, default_analyzer().profile);
        }
    }
}

#[test]
fn terms_match_js() {
    let data = fixture("terms.json");
    let mut report = Report::new("terms");
    for case in data.as_array().unwrap() {
        let base = strings(&case["base"]);
        report.check(|| format!("expanded {base:?}"), expanded_terms_from_base_terms(&base), strings(&case["expanded"]));
        report.check(
            || format!("bundleKey {base:?}"),
            query_bundle_key_from_base_terms(&base),
            case["bundleKey"].as_str().unwrap().to_string(),
        );
        let keys: Vec<Value> = query_bundle_keys_from_base_terms(&base).iter().map(|key| key.to_json()).collect();
        report.check(|| format!("bundleKeys {base:?}"), Value::Array(keys), case["bundleKeys"].clone());
        let left = case["left"].as_str().unwrap();
        let right = case["right"].as_str().unwrap();
        report.check(
            || format!("proximity {left:?} {right:?}"),
            proximity_term(left, right),
            case["proximity"].as_str().unwrap().to_string(),
        );
    }
    report.finish();
}

fn max_chars(value: &Value) -> Option<f64> {
    match value {
        Value::Null => None,
        Value::String(text) if text == "NaN" => Some(f64::NAN),
        Value::String(text) if text == "Infinity" => Some(f64::INFINITY),
        other => Some(other.as_f64().expect("maxChars number")),
    }
}

#[test]
fn highlighting_matches_js() {
    let data = fixture("highlight.json");
    let profiles = analyzers_by_name();
    let mut report = Report::new("highlight");
    for case in data["highlight"].as_array().unwrap() {
        let analyzer = analyzer_named(case["analyzer"].as_str().unwrap(), &profiles);
        let text = case["text"].as_str().unwrap();
        let query = case["query"].as_str().unwrap();
        let corrected = case["corrected"].as_str().unwrap();
        let term_set = highlight_term_set(query, corrected, analyzer.as_ref());
        report.check(|| format!("termSet {query:?}"), term_set.as_slice().to_vec(), strings(&case["termSet"]));
        report.check(
            || format!("findMatchRanges {query:?} in {text:?}"),
            ranges_json(&find_match_ranges(text, &term_set, analyzer.as_ref())),
            case["ranges"].clone(),
        );
        let highlight = highlight_text(text, &term_set, max_chars(&case["maxChars"]), analyzer.as_ref());
        report.check(
            || format!("highlightText {query:?} max={} in {text:?}", case["maxChars"]),
            highlight.map(|h| h.to_json()).unwrap_or(Value::Null),
            case["highlight"].clone(),
        );
    }
    for case in data["apply"].as_array().unwrap() {
        let analyzer = analyzer_named(case["analyzer"].as_str().unwrap(), &profiles);
        let query = case["query"].as_str().unwrap();
        let term_set = highlight_term_set(query, "", analyzer.as_ref());
        let options = HighlightOptions {
            fields: case["fields"].as_array().map(|_| strings(&case["fields"])),
            max_chars: max_chars(&case["maxChars"]),
            analyzer: analyzer.as_ref(),
        };
        let mut results = case["input"].as_array().unwrap().clone();
        apply_highlights(&mut results, &term_set, &options);
        // Compared as serialized text so key order counts too.
        report.check(
            || format!("applyHighlights {query:?} fields={} max={}", case["fields"], case["maxChars"]),
            serde_json::to_string(&Value::Array(results)).unwrap(),
            serde_json::to_string(&case["output"]).unwrap(),
        );
    }
    report.finish();
}
