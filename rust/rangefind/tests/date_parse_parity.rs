#![cfg(feature = "engine")]
//! `Date.parse` parity: the Rust port of V8's date parser (used for date
//! range filters) against the strings and results recorded by
//! `node rust/tools/parity/build_fixtures.mjs` in `target/parity/dates.json`
//! (ES date-time strings, V8's legacy fallback forms and malformed input,
//! recorded with TZ=UTC).

use std::path::Path;

use serde_json::Value;

#[test]
fn date_parse_matches_v8() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/parity/dates.json");
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("missing {}: run `node rust/tools/parity/build_fixtures.mjs` from the repo root", path.display()));
    let fixture: Value = serde_json::from_str(&raw).expect("dates json");
    assert_eq!(fixture["tz"], "UTC", "date fixtures must be recorded with TZ=UTC");
    let cases = fixture["cases"].as_array().expect("cases");
    assert!(cases.len() >= 500, "expected hundreds of date cases, got {}", cases.len());
    let mut failures = Vec::new();
    for case in cases {
        let text = case[0].as_str().expect("text");
        let expected = case[1].as_f64();
        let actual = rangefind::engine::filters::parse_date(text);
        if actual != expected {
            failures.push(format!("{text:?}: js={expected:?} rust={actual:?}"));
        }
    }
    assert!(failures.is_empty(), "{} of {} Date.parse cases differ:\n{}", failures.len(), cases.len(), failures.join("\n"));
}
