#![cfg(feature = "engine")]
//! Engine-level parity: replay every recorded JavaScript query against the
//! same index files through the Rust engine and require the same answer.
//!
//! Fixtures come from `node rust/tools/parity/build_fixtures.mjs` (run from
//! the repo root). The comparison covers everything a caller can observe —
//! totals, approximation flags, every result document and score, typo
//! corrections, facet counts, suggestions, error messages — plus the
//! planner-lane stats that prove the same execution strategy was chosen.
//!
//! `RANGEFIND_PARITY_STATS=1` additionally diffs every stats key and prints
//! the mismatches (informational; lane-internal counters may legitimately
//! differ where the Rust port is more efficient).
//! `RANGEFIND_PARITY_ONLY=geo,blocks` limits the indexes replayed.
//! `RANGEFIND_CACHE_BUDGET=<bytes>` sets the engine's `cacheBudgetBytes`.
//!
//! A second test replays every index under small cache budgets (1 byte:
//! everything a call loaded is evicted when it ends; and partial budgets):
//! answers must not depend on what happens to be cached (eviction safety,
//! and cache keys that stay correct when evicted objects are reloaded at
//! new addresses).

use std::path::{Path, PathBuf};
use std::rc::Rc;

use rangefind::engine::{Engine, EngineOptions};
use rangefind::transport::FileTransport;
use serde_json::Value;

fn parity_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/parity")
}

/// Stats every response must agree on: they name the lane that answered and
/// the exactness guarantees it gave.
const LANE_STATS: &[&str] = &[
    "plannerLane",
    "countLane",
    "exact",
    "topKProven",
    "totalExact",
    "tailExhausted",
    "geoLane",
    "geoDistanceSorted",
    "docValuePruning",
    "docValueChunkPruning",
    "docValueSortText",
    "typoApplied",
    "postalCodeNormalized",
    "rankPriorBoost",
    "geoBoost",
    "facetCountLane",
    "vectorLane",
];

#[derive(Default)]
struct Report {
    cases: usize,
    failures: Vec<String>,
    stat_mismatches: std::collections::BTreeMap<String, usize>,
}

fn numbers_equal(left: &Value, right: &Value) -> bool {
    match (left.as_f64(), right.as_f64()) {
        (Some(a), Some(b)) => a == b || (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0),
        _ => false,
    }
}

/// Structural equality; numbers compare as doubles (`1` == `1.0`), with a
/// relative 1e-9 slack so last-ulp float differences in derived scores do
/// not mask real regressions elsewhere.
fn json_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(_), Value::Number(_)) => numbers_equal(left, right),
        (Value::Array(a), Value::Array(b)) => a.len() == b.len() && a.iter().zip(b).all(|(x, y)| json_equal(x, y)),
        (Value::Object(a), Value::Object(b)) => {
            a.len() == b.len() && a.iter().all(|(key, value)| b.get(key).map(|other| json_equal(value, other)).unwrap_or(false))
        }
        _ => left == right,
    }
}

fn first_difference(path: &str, left: &Value, right: &Value) -> Option<String> {
    match (left, right) {
        (Value::Object(a), Value::Object(b)) => {
            for (key, value) in a {
                match b.get(key) {
                    None => return Some(format!("{path}.{key}: js={} rust=<missing>", short(value))),
                    Some(other) => {
                        if let Some(diff) = first_difference(&format!("{path}.{key}"), value, other) {
                            return Some(diff);
                        }
                    }
                }
            }
            for (key, value) in b {
                if !a.contains_key(key) {
                    return Some(format!("{path}.{key}: js=<missing> rust={}", short(value)));
                }
            }
            None
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Some(format!("{path}: js len {} rust len {}", a.len(), b.len()));
            }
            a.iter().zip(b).enumerate().find_map(|(index, (x, y))| first_difference(&format!("{path}[{index}]"), x, y))
        }
        _ => {
            if json_equal(left, right) {
                None
            } else {
                Some(format!("{path}: js={} rust={}", short(left), short(right)))
            }
        }
    }
}

fn short(value: &Value) -> String {
    let text = value.to_string();
    if text.len() > 160 {
        format!("{}…", &text[..160])
    } else {
        text
    }
}

fn compare_case(index: &str, ordinal: usize, case: &Value, actual: Result<Value, rangefind::Error>, report: &mut Report, all_stats: bool) {
    report.cases += 1;
    let label = format!("{index}#{ordinal} {}({})", case["method"].as_str().unwrap_or("?"), short(&case["params"]));
    match (case.get("error"), actual) {
        (Some(error), Err(actual)) => {
            if error["message"].as_str() != Some(actual.message()) {
                report.failures.push(format!("{label}: error js={:?} rust={:?}", error["message"], actual.message()));
            }
        }
        (Some(error), Ok(_)) => report.failures.push(format!("{label}: js threw {:?}, rust answered", error["message"])),
        (None, Err(actual)) => report.failures.push(format!("{label}: rust threw {:?}", actual.message())),
        (None, Ok(actual)) => {
            let expected = &case["response"];
            // Arrays, null and scalars (authorityLookup, searchAddressAuthority)
            // are compared whole.
            if !expected.is_object() {
                if let Some(diff) = first_difference("response", expected, &actual) {
                    report.failures.push(format!("{label}: {diff}"));
                }
                return;
            }
            // Every top-level key except `stats` must match exactly, both ways
            // (a missing key is a mismatch, not null). Stats are compared
            // through LANE_STATS below because internal counters may differ.
            let empty = serde_json::Map::new();
            let expected_map = expected.as_object().unwrap_or(&empty);
            let actual_map = actual.as_object().unwrap_or(&empty);
            for (key, left) in expected_map {
                if key == "stats" {
                    continue;
                }
                let diff = match actual_map.get(key) {
                    None => Some(format!("{key}: js={} rust=<missing>", short(left))),
                    Some(right) => first_difference(key, left, right),
                };
                if let Some(diff) = diff {
                    report.failures.push(format!("{label}: {diff}"));
                    return;
                }
            }
            if let Some(key) = actual_map.keys().find(|key| *key != "stats" && !expected_map.contains_key(*key)) {
                report.failures.push(format!("{label}: {key}: js=<missing> rust={}", short(&actual_map[key])));
                return;
            }
            let expected_stats = expected.get("stats").cloned().unwrap_or(Value::Null);
            let actual_stats = actual.get("stats").cloned().unwrap_or(Value::Null);
            for key in LANE_STATS {
                let left = expected_stats.get(*key).unwrap_or(&Value::Null);
                let right = actual_stats.get(*key).unwrap_or(&Value::Null);
                if !json_equal(left, right) {
                    report.failures.push(format!("{label}: stats.{key} js={} rust={}", short(left), short(right)));
                    return;
                }
            }
            if all_stats {
                if let Some(map) = expected_stats.as_object() {
                    for (key, value) in map {
                        if key == "trace" {
                            continue;
                        }
                        if !json_equal(value, actual_stats.get(key).unwrap_or(&Value::Null)) {
                            *report.stat_mismatches.entry(key.clone()).or_default() += 1;
                        }
                    }
                }
            }
        }
    }
}

fn engine_options(budget: Option<u64>) -> EngineOptions {
    let budget = budget.or_else(|| std::env::var("RANGEFIND_CACHE_BUDGET").ok().and_then(|value| value.parse().ok()));
    match budget {
        Some(bytes) => EngineOptions::from_json(serde_json::json!({ "cacheBudgetBytes": bytes })),
        None => EngineOptions::default(),
    }
}

fn replay(index: &str, report: &mut Report, all_stats: bool, budget: Option<u64>) {
    let root = parity_root();
    let cases_path = root.join(format!("{index}.cases.json"));
    let raw = std::fs::read_to_string(&cases_path).unwrap_or_else(|_| {
        panic!("missing {}: run `node rust/tools/parity/build_fixtures.mjs` from the repo root", cases_path.display())
    });
    let fixture: Value = serde_json::from_str(&raw).expect("cases json");
    let base = format!("{}/", root.join(index).join("index").display());
    let transport = Rc::new(FileTransport::new());
    let engine = Engine::open_blocking(transport, &base, engine_options(budget)).expect("open engine");
    for (ordinal, case) in fixture["cases"].as_array().expect("cases").iter().enumerate() {
        let method = case["method"].as_str().unwrap_or("");
        let params = case["params"].clone();
        let actual = engine.call_blocking(method, params);
        compare_case(index, ordinal, case, actual, report, all_stats);
    }
    let cache = engine.cache_stats();
    if budget == Some(1) {
        // Everything a call loaded was evicted when it ended.
        assert!(cache["evictedEntries"].as_u64().unwrap_or(0) > 0, "{index}: nothing was evicted under a 1-byte budget: {cache}");
        assert_eq!(cache["usedBytes"].as_u64(), Some(0), "{index}: cached bytes survived a 1-byte budget: {cache}");
    }
    if std::env::var("RANGEFIND_CACHE_STATS").is_ok() {
        eprintln!("{index}: {cache}");
    }
}

#[test]
fn engine_matches_javascript_runtime() {
    run_parity(None, "");
}

/// Every index again under cache pressure: a 1-byte budget (each call
/// starts cold) and two partial budgets, where LRU eviction drops some
/// posting segments while blocks loaded through their term entries stay
/// cached. Cache keys derived from an entry's address (instead of its
/// `uid`) return another term's postings here once a reloaded segment's
/// entry lands at a freed address.
#[test]
fn engine_matches_javascript_runtime_under_cache_pressure() {
    for budget in [1, 100_000, 2_000_000] {
        run_parity(Some(budget), &format!(" ({budget}-byte cache budget)"));
    }
}

fn run_parity(budget: Option<u64>, label: &str) {
    let root = parity_root();
    let summary = std::fs::read_to_string(root.join("summary.json"))
        .expect("missing parity fixtures: run `node rust/tools/parity/build_fixtures.mjs` from the repo root");
    let summary: Value = serde_json::from_str(&summary).unwrap();
    let only: Option<Vec<String>> = std::env::var("RANGEFIND_PARITY_ONLY").ok().map(|v| v.split(',').map(str::to_string).collect());
    let all_stats = std::env::var("RANGEFIND_PARITY_STATS").is_ok();
    let mut report = Report::default();
    for item in summary.as_array().unwrap() {
        let index = item["index"].as_str().unwrap();
        if only.as_ref().map(|list| !list.iter().any(|name| name == index)).unwrap_or(false) {
            continue;
        }
        replay(index, &mut report, all_stats, budget);
    }
    if !report.stat_mismatches.is_empty() {
        eprintln!("stats keys differing (informational){label}: {:?}", report.stat_mismatches);
    }
    if let Ok(path) = std::env::var("RANGEFIND_PARITY_DUMP") {
        let path = match budget {
            Some(bytes) => format!("{path}.budget-{bytes}"),
            None => path,
        };
        std::fs::write(path, report.failures.join("\n")).ok();
    }
    if !report.failures.is_empty() {
        let shown: Vec<&String> = report.failures.iter().take(40).collect();
        panic!(
            "{} of {} parity cases failed{label}:\n{}",
            report.failures.len(),
            report.cases,
            shown.iter().map(|s| s.as_str()).collect::<Vec<_>>().join("\n")
        );
    }
    eprintln!("{} parity cases matched{label}", report.cases);
}
