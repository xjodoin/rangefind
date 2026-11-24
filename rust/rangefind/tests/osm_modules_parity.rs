//! Parity with the JS OSM query-time modules
//! (`src/integrations/osm/{category_lexicon,constraints,opening_hours,schema,
//! google_maps_adapter}.js`) over fixtures from
//! `node rust/tools/fixtures/osm_modules.mjs`.
#![cfg(feature = "osm")]

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use rangefind::osm::category_lexicon::*;
use rangefind::osm::constraints::*;
use rangefind::osm::google_maps_adapter::*;
use rangefind::osm::opening_hours::*;
use rangefind::osm::schema::*;
use rangefind::Result;
use serde_json::{json, Value};

fn load(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/fixtures/osm_modules").join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!("missing fixture {}: run `node rust/tools/fixtures/osm_modules.mjs` from the repo root", path.display())
    });
    serde_json::from_str(&text).expect("fixture json")
}

fn tables() -> Value {
    load("tables.json")
}

fn default_time_zone() -> String {
    tables()["defaultTimeZone"].as_str().unwrap().to_string()
}

fn strings(value: &Value) -> Vec<String> {
    value.as_array().unwrap().iter().map(|item| item.as_str().unwrap().to_string()).collect()
}

fn opt_json<T>(value: Option<T>, f: impl Fn(&T) -> Value) -> Value {
    value.as_ref().map(f).unwrap_or(Value::Null)
}

/// Fixture number: a JSON number or "NaN".
fn fnum(value: &Value) -> f64 {
    match value {
        Value::String(text) if text == "NaN" => f64::NAN,
        other => other.as_f64().unwrap(),
    }
}

fn same_num(left: f64, right: f64) -> bool {
    (left.is_nan() && right.is_nan()) || left == right
}

#[test]
fn tables_match_js() {
    let tables = tables();
    assert_eq!(strings(&tables["canonicalTypes"]), OSM_CANONICAL_TYPES.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    let aliases: Vec<Value> = OSM_TYPE_ALIASES.iter().map(|(key, list)| json!([key, list])).collect();
    assert_eq!(tables["typeAliases"], Value::Array(aliases));
    assert_eq!(strings(&tables["excludedTypes"]), CATEGORY_EXCLUDED_TYPES.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    assert_eq!(tables["lexiconVersion"], json!(OSM_CATEGORY_LEXICON_VERSION));
    assert_eq!(tables["schemaVersion"], json!(OSM_INTEGRATION_SCHEMA_VERSION));
    assert_eq!(strings(&tables["displayFields"]), OSM_DISPLAY_FIELDS.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    let definitions: Vec<Value> = OSM_CONSTRAINT_DEFINITIONS
        .iter()
        .map(|d| json!({ "key": d.key, "facet": d.facet, "detail": d.detail, "values": d.values }))
        .collect();
    assert_eq!(tables["constraintDefinitions"], Value::Array(definitions));
}

#[test]
fn fold_and_type_query_text() {
    let cases = load("fold.json");
    let mut failures = Vec::new();
    for case in cases.as_array().unwrap() {
        let input = case[0].as_str().unwrap();
        if fold(input) != case[1].as_str().unwrap() || type_query_text(input) != case[2].as_str().unwrap() {
            failures.push(format!("{input:?}: fold {:?} vs {:?}, typeQueryText {:?} vs {:?}", fold(input), case[1], type_query_text(input), case[2]));
        }
    }
    assert!(failures.is_empty(), "{} fold mismatches:\n{}", failures.len(), failures.join("\n"));
    assert!(cases.as_array().unwrap().len() > 1000);
}

#[test]
fn lexicons_and_lookups() {
    let lexicons = load("lexicons.json");
    let mut failures = Vec::new();
    let mut lookups = 0;
    for case in lexicons.as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let lexicon = if case["input"].is_null() { default_category_lexicon().clone() } else { build_category_lexicon(&case["input"]) };
        let entries: Vec<Value> = lexicon.iter().map(|(key, entry)| json!([key, entry.type_, entry.query])).collect();
        assert_eq!(Value::Array(entries), case["entries"], "lexicon {name} entries");
        for lookup in case["lookups"].as_array().unwrap() {
            let surface = lookup[0].as_str().unwrap();
            let exact = opt_json(lookup_category(&lexicon, surface), CategoryMatch::to_json);
            let fuzzy = opt_json(lookup_category_fuzzy(&lexicon, surface), CategoryMatch::to_json);
            if exact != lookup[1] || fuzzy != lookup[2] {
                failures.push(format!("{name} {surface:?}: exact {exact} vs {}, fuzzy {fuzzy} vs {}", lookup[1], lookup[2]));
            }
            lookups += 1;
        }
    }
    assert!(failures.is_empty(), "{} lookup mismatches:\n{}", failures.len(), failures.join("\n"));
    assert!(lookups > 10_000);
}

#[test]
fn lexicon_artifacts() {
    for case in load("artifacts.json").as_array().unwrap() {
        let min_count = case[1].as_f64();
        assert_eq!(build_category_lexicon_artifact(&case[0], min_count), case[2], "artifact for {}", case[0]);
    }
}

#[test]
fn within_one_edit_pairs() {
    let cases = load("edits.json");
    for case in cases.as_array().unwrap() {
        let (left, right) = (case[0].as_str().unwrap(), case[1].as_str().unwrap());
        assert_eq!(within_one_edit(left, right), case[2].as_bool().unwrap(), "{left:?} / {right:?}");
    }
}

#[test]
fn constraint_parsing() {
    let mut failures = Vec::new();
    for case in load("constraints_parse.json").as_array().unwrap() {
        let parsed = parse_osm_constraints(case[0].as_str().unwrap(), &case[1]);
        if parsed.to_json() != case[2] {
            failures.push(format!("{:?} {}: {} vs {}", case[0], case[1], parsed.to_json(), case[2]));
        }
    }
    assert!(failures.is_empty(), "{} parse mismatches:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn constraint_compilation() {
    for case in load("constraints_compile.json").as_array().unwrap() {
        let constraints = OsmConstraints::from_json(&case[0]);
        assert_eq!(constraints.to_json(), case[0]);
        let facets: HashSet<String> = strings(&case[1]).into_iter().collect();
        assert_eq!(Value::Object(compile_osm_constraint_filters(&constraints, &facets)), case[2], "{}", case[0]);
    }
}

#[test]
fn zoned_parts_match_intl() {
    let default_zone = default_time_zone();
    let mut failures = Vec::new();
    let cases = load("zones.json");
    for case in cases.as_array().unwrap() {
        let at = case[1].as_f64().unwrap();
        let zone = match &case[0] {
            Value::Null => None,
            Value::String(text) => Some(text.clone()),
            other => Some(other.to_string()),
        };
        let parts = zoned_parts(at, zone.as_deref(), if zone.is_none() { Some(default_zone.as_str()) } else { None });
        let actual = opt_json(parts, |p| json!({ "weekday": p.weekday, "minutes": p.minutes, "date": p.date }));
        if actual != case[2] {
            failures.push(format!("{} @ {at}: {actual} vs {}", case[0], case[2]));
        }
    }
    assert!(failures.is_empty(), "{} zone mismatches:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn date_values() {
    let default_zone = default_time_zone();
    for case in load("dates.json").as_array().unwrap() {
        let actual = js_date_ms(&case[0], Some(&default_zone));
        assert!(same_num(actual, fnum(&case[1])), "new Date({}) = {actual} vs {}", case[0], case[1]);
    }
}

fn hours_options(options: &Value, default_zone: &str) -> OpeningHoursOptions {
    OpeningHoursOptions {
        at: options.get("at").cloned().unwrap_or(Value::Null),
        time_zone: options.get("timeZone").cloned().unwrap_or(Value::Null),
        require_complete: options.get("requireComplete").cloned().unwrap_or(Value::Null),
        now_ms: None,
        default_time_zone: Some(default_zone.to_string()),
        zone_resolver: None,
    }
}

#[test]
fn opening_hours_evaluation() {
    let default_zone = default_time_zone();
    let mut failures = Vec::new();
    for case in load("opening_hours.json").as_array().unwrap() {
        let actual = match evaluate_opening_hours(&case[0], &hours_options(&case[1], &default_zone)) {
            Ok(evaluation) => evaluation.to_json(),
            Err(error) => json!({ "error": error.message() }),
        };
        if actual != case[2] {
            failures.push(format!("{} {}: {actual} vs {}", case[0], case[1], case[2]));
        }
    }
    assert!(failures.is_empty(), "{} opening-hours mismatches:\n{}", failures.len(), failures.join("\n"));
}

#[test]
fn opening_hours_now_is_pinnable() {
    let options = OpeningHoursOptions {
        time_zone: json!("UTC"),
        now_ms: Some(1_785_769_200_000.0), // Monday 2026-08-03 15:00 UTC
        ..Default::default()
    };
    let evaluation = evaluate_opening_hours(&json!("Mo-Fr 09:00-17:00"), &options).unwrap();
    assert_eq!(evaluation.state, "open");
    assert_eq!(evaluation.local_date.as_deref(), Some("2026-08-03"));
    let resolver: ZoneResolver = std::rc::Rc::new(|_, _| Some(ZonedParts { weekday: 0, minutes: 600, date: "x".into() }));
    let custom = OpeningHoursOptions { zone_resolver: Some(resolver), ..options };
    assert_eq!(evaluate_opening_hours(&json!("Mo-Fr 09:00-17:00"), &custom).unwrap().state, "closed");
}

#[test]
fn constraint_evaluation_and_annotation() {
    let default_zone = default_time_zone();
    let mut failures = Vec::new();
    for case in load("constraints_eval.json").as_array().unwrap() {
        let (result, constraints, options) = (&case[0], OsmConstraints::from_json(&case[1]), &case[2]);
        let mut constraint_options = ConstraintOptions::from_params(options);
        constraint_options.default_time_zone = Some(default_zone.clone());
        let (evaluation, annotated) = match evaluate_osm_constraints(result, &constraints, &constraint_options) {
            Ok(evaluation) => {
                let annotated = annotate_constraint_result(result, Some(&evaluation));
                (evaluation.to_json(), annotated)
            }
            Err(error) => (json!({ "error": error.message() }), Value::Null),
        };
        if evaluation != case[3] || annotated != case[4] {
            failures.push(format!("{result} {} {options}: {evaluation} vs {}; {annotated} vs {}", case[1], case[3], case[4]));
        }
    }
    assert!(failures.is_empty(), "{} evaluation mismatches:\n{}", failures.len(), failures.join("\n"));
    assert_eq!(annotate_constraint_result(&json!({ "a": 1 }), None), json!({ "a": 1 }));
}

#[test]
fn schema_config() {
    for case in load("schema.json").as_array().unwrap() {
        assert_eq!(create_osm_index_config(&case[0]), case[1], "options {}", case[0]);
    }
}

#[test]
fn migration_places() {
    for case in load("places.json").as_array().unwrap() {
        assert_eq!(to_migration_place(&case[0]), case[1], "result {}", case[0]);
    }
}

#[derive(Default)]
struct MockBackend {
    queues: RefCell<HashMap<&'static str, VecDeque<Value>>>,
    calls: RefCell<Vec<Value>>,
}

impl MockBackend {
    fn call(&self, kind: &'static str, params: Value) -> Result<Value> {
        self.calls.borrow_mut().push(json!([kind, params]));
        Ok(self.queues.borrow_mut().entry(kind).or_default().pop_front().unwrap_or_else(|| json!({ "results": [] })))
    }
}

impl OsmQueryBackend for MockBackend {
    fn search_osm_query(&self, params: Value) -> impl Future<Output = Result<Value>> {
        let out = self.call("search", params);
        async move { out }
    }
    fn suggest_osm_query(&self, params: Value) -> impl Future<Output = Result<Value>> {
        let out = self.call("suggest", params);
        async move { out }
    }
    fn reverse_geocode_osm(&self, params: Value) -> impl Future<Output = Result<Value>> {
        let out = self.call("reverse", params);
        async move { out }
    }
    fn search_along_route_osm(&self, params: Value) -> impl Future<Output = Result<Value>> {
        let out = self.call("route", params);
        async move { out }
    }
}

fn block_on<T>(future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("mock backend futures complete immediately"),
    }
}

#[test]
fn maps_adapter_sessions() {
    let sessions = load("adapter_sessions.json");
    let mut ops = 0;
    for (index, session) in sessions.as_array().unwrap().iter().enumerate() {
        let defaults = if session["defaults"].is_null() { Value::Null } else { session["defaults"].clone() };
        let adapter = create_rangefind_maps_adapter(MockBackend::default(), MapsAdapterOptions { defaults });
        for (op, expected) in session["ops"].as_array().unwrap().iter().zip(session["outputs"].as_array().unwrap()) {
            let backend = adapter.backend();
            backend.calls.borrow_mut().clear();
            for (kind, list) in op["responses"].as_object().unwrap() {
                let kind: &'static str = match kind.as_str() {
                    "search" => "search",
                    "suggest" => "suggest",
                    "reverse" => "reverse",
                    _ => "route",
                };
                backend.queues.borrow_mut().insert(kind, list.as_array().unwrap().iter().cloned().collect());
            }
            let request = &op["request"];
            let result = block_on(async {
                match op["method"].as_str().unwrap() {
                    "autocomplete" => adapter.autocomplete(request).await,
                    "textSearch" => adapter.text_search(request).await,
                    "nearbySearch" => adapter.nearby_search(request).await,
                    "geocode" => adapter.geocode(request).await,
                    "reverseGeocode" => adapter.reverse_geocode(request).await,
                    "searchAlongRoute" => adapter.search_along_route(request).await,
                    "placeDetails" => adapter.place_details(request).await,
                    other => panic!("unknown method {other}"),
                }
            });
            if op["clearAfter"] == json!(true) {
                adapter.clear_place_cache();
            }
            let mut actual = match result {
                Ok(value) => json!({ "result": value }),
                Err(error) => json!({ "error": error.message() }),
            };
            actual["calls"] = Value::Array(backend.calls.borrow().clone());
            assert_eq!(&actual, expected, "session {index} op {} request {request}", op["method"]);
            ops += 1;
        }
    }
    assert!(ops > 300);
}
