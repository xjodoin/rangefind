#![cfg(feature = "osm")]
//! OSM query layer parity (`src/integrations/osm/query.js`).
//!
//! Fixtures come from `node rust/tools/parity/osm_fixtures.mjs` (repo root)
//! and live in `rust/target/osm-parity/`. Two checks per recorded call:
//!
//! * **replay** — the Rust OSM layer runs against a replay engine that
//!   answers every engine call with the response the JS engine gave. Every
//!   call the Rust layer makes must match a recorded call (method and
//!   params), and the final output must equal the JS output exactly,
//!   stats included. This isolates the OSM port from engine differences
//!   and covers the mock-engine unit tests (`replay/*.json.gz`).
//! * **live** — the full Rust stack (OSM layer on the Rust engine) runs on
//!   the same index files; the response must match the JS response except
//!   for engine-internal stats and timing traces.
//!
//! Env: `OSM_PARITY_ONLY=lux,replay`, `OSM_PARITY_MODE=replay|live`,
//! `OSM_PARITY_DUMP=/path/failures.txt`, `OSM_PARITY_LIMIT=n` (cases per
//! file).

use std::cell::RefCell;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use rangefind::engine::{Engine, EngineOptions};
use rangefind::error::{Error, Result};
use rangefind::osm::query::{self as osm, LocalFuture, OsmCaches, OsmEngine};
use rangefind::transport::FileTransport;
use serde_json::{json, Value};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/osm-parity")
}

fn load_gz(path: &Path) -> Value {
    let file = std::fs::File::open(path).unwrap_or_else(|_| panic!("missing {}", path.display()));
    let mut text = String::new();
    flate2::read::GzDecoder::new(file).read_to_string(&mut text).expect("gzip");
    serde_json::from_str(&text).expect("json")
}

fn numbers_equal(left: &Value, right: &Value) -> bool {
    match (left.as_f64(), right.as_f64()) {
        (Some(a), Some(b)) => a == b || (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0),
        _ => false,
    }
}

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

fn short(value: &Value) -> String {
    let text = value.to_string();
    if text.chars().count() > 220 {
        format!("{}…", text.chars().take(220).collect::<String>())
    } else {
        text
    }
}

fn first_difference(path: &str, js: &Value, rust: &Value) -> Option<String> {
    match (js, rust) {
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
            b.iter().find(|(key, _)| !a.contains_key(*key)).map(|(key, value)| format!("{path}.{key}: js=<missing> rust={}", short(value)))
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Some(format!("{path}: js len {} rust len {} (js={} rust={})", a.len(), b.len(), short(js), short(rust)));
            }
            a.iter().zip(b).enumerate().find_map(|(index, (x, y))| first_difference(&format!("{path}[{index}]"), x, y))
        }
        _ => (!json_equal(js, rust)).then(|| format!("{path}: js={} rust={}", short(js), short(rust))),
    }
}

/// Key order differences (JSON object insertion order) anywhere.
fn first_order_difference(path: &str, js: &Value, rust: &Value) -> Option<String> {
    match (js, rust) {
        (Value::Object(a), Value::Object(b)) => {
            let ka: Vec<&String> = a.keys().collect();
            let kb: Vec<&String> = b.keys().collect();
            if ka != kb {
                return Some(format!("{path}: js keys {ka:?} rust keys {kb:?}"));
            }
            a.iter().find_map(|(key, value)| first_order_difference(&format!("{path}.{key}"), value, &b[key]))
        }
        (Value::Array(a), Value::Array(b)) => a.iter().zip(b).enumerate().find_map(|(i, (x, y))| first_order_difference(&format!("{path}[{i}]"), x, y)),
        _ => None,
    }
}

// ---------------------------------------------------------------- replay engine

struct RecordedCall {
    method: String,
    params: Value,
    response: Option<Value>,
    error: Option<Value>,
    used: bool,
}

struct ReplayEngine {
    manifest: Rc<Value>,
    has: Vec<String>,
    calls: RefCell<Vec<RecordedCall>>,
    caches: Rc<OsmCaches>,
    unexpected: RefCell<Vec<String>>,
}

impl ReplayEngine {
    fn new(engine: &Value, calls: &Value, caches: Rc<OsmCaches>) -> Self {
        let calls = calls
            .as_array()
            .map(|items| {
                items
                    .iter()
                    .map(|call| RecordedCall {
                        method: call["method"].as_str().unwrap_or("").to_string(),
                        params: call.get("params").cloned().unwrap_or(Value::Null),
                        response: call.get("response").cloned(),
                        error: call.get("error").cloned(),
                        used: false,
                    })
                    .collect()
            })
            .unwrap_or_default();
        ReplayEngine {
            manifest: Rc::new(engine.get("manifest").cloned().unwrap_or(Value::Null)),
            has: engine.get("has").and_then(Value::as_array).map(|items| items.iter().filter_map(|v| v.as_str().map(str::to_string)).collect()).unwrap_or_default(),
            calls: RefCell::new(calls),
            caches,
            unexpected: RefCell::new(Vec::new()),
        }
    }
}

impl OsmEngine for ReplayEngine {
    fn manifest(&self) -> Rc<Value> {
        self.manifest.clone()
    }

    fn has(&self, method: &str) -> bool {
        self.has.iter().any(|m| m == method)
    }

    fn call<'a>(&'a self, method: &'a str, params: Value) -> LocalFuture<'a, Result<Value>> {
        Box::pin(async move {
            let mut calls = self.calls.borrow_mut();
            let found = calls.iter_mut().find(|call| !call.used && call.method == method && json_equal(&call.params, &params));
            match found {
                Some(call) => {
                    call.used = true;
                    if let Some(error) = &call.error {
                        let message = error["message"].as_str().unwrap_or("").to_string();
                        return Err(match error.get("code").and_then(Value::as_str) {
                            Some(code) => Error::with_code(message, code),
                            None => Error::new(message),
                        });
                    }
                    Ok(call.response.clone().unwrap_or(Value::Null))
                }
                None => {
                    self.unexpected.borrow_mut().push(format!("{method}({})", short(&params)));
                    Err(Error::new(format!("replay: unrecorded {method} call")))
                }
            }
        })
    }

    fn caches(&self) -> Rc<OsmCaches> {
        self.caches.clone()
    }
}

// ---------------------------------------------------------------- running

fn run_with(engine: &dyn OsmEngine, function: &str, args: &[Value]) -> std::result::Result<Value, String> {
    let runtime = rangefind::rt::Runtime::new();
    let arg = args.first().cloned().unwrap_or(json!({}));
    // The OSM futures borrow the engine; drive them to completion here.
    let result: Rc<RefCell<Option<Result<Value>>>> = Rc::new(RefCell::new(None));
    let future: LocalFuture<'_, Result<Value>> = match function {
        "searchOsmQuery" => Box::pin(osm::search_osm_query_with(engine, &arg)),
        "searchAlongRouteOsm" => Box::pin(osm::search_along_route_osm_with(engine, &arg)),
        "reverseGeocodeOsm" => Box::pin(osm::reverse_geocode_osm_with(engine, &arg)),
        "suggestOsmQuery" => Box::pin(osm::suggest_osm_query_with(engine, &arg)),
        "resolveOsmSuggestion" => Box::pin(osm::resolve_osm_suggestion_with(engine, &arg)),
        "hydrateOsmSuggestions" => {
            let mut items = arg.as_array().cloned().unwrap_or_default();
            Box::pin(async move {
                osm::hydrate_osm_suggestions_with(engine, &mut items).await;
                Ok(Value::Array(items))
            })
        }
        other => return Err(format!("unknown function {other}")),
    };
    drive(&runtime, future, &result);
    let outcome = result.borrow_mut().take();
    match outcome {
        Some(Ok(value)) => Ok(value),
        Some(Err(error)) => Err(error.message().to_string()),
        None => Err("stalled".into()),
    }
}

/// Poll a borrowed future to completion on a fresh runtime (the replay
/// engine completes synchronously; spawned tasks run in between polls).
fn drive<'a>(runtime: &rangefind::rt::Runtime, mut future: LocalFuture<'a, Result<Value>>, out: &Rc<RefCell<Option<Result<Value>>>>) {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn raw() -> RawWaker {
        fn clone(_: *const ()) -> RawWaker {
            raw()
        }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    let waker = unsafe { Waker::from_raw(raw()) };
    let mut cx = Context::from_waker(&waker);
    for _ in 0..1_000_000 {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            *out.borrow_mut() = Some(value);
            return;
        }
        if runtime.run_until_stalled() == 0 {
            // Nothing left to run elsewhere; keep polling (replay is synchronous).
        }
    }
}

fn run_pure(function: &str, args: &[Value]) -> std::result::Result<Value, String> {
    let text = |value: &Value| match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    };
    let arg = args.first().cloned().unwrap_or(Value::Null);
    match function {
        "parseCoordinateIntent" => Ok(osm::parse_coordinate_intent(&text(&arg)).map(|c| c.to_json()).unwrap_or(Value::Null)),
        "parseOsmQueryIntent" => {
            if args.len() > 1 {
                return Err("custom lexicon".into());
            }
            Ok(osm::parse_osm_query_intent(&text(&arg), None).map(|i| i.to_json()).unwrap_or(Value::Null))
        }
        "parseNearbyCategoryIntent" => {
            if args.len() > 1 {
                return Err("custom lexicon".into());
            }
            Ok(osm::parse_nearby_category_intent(&text(&arg), None).map(|i| i.to_json()).unwrap_or(Value::Null))
        }
        "collapseStreetSuggestions" => Ok(osm::collapse_street_suggestions(&arg, args.get(1).unwrap_or(&json!({})))),
        other => Err(format!("unknown pure function {other}")),
    }
}

fn expected(record: &Value) -> std::result::Result<Value, String> {
    match record.get("error") {
        Some(error) => Err(error["message"].as_str().unwrap_or("").to_string()),
        None => Ok(record.get("output").cloned().unwrap_or(Value::Null)),
    }
}

#[derive(Default)]
struct Report {
    cases: usize,
    failures: Vec<String>,
    order: Vec<String>,
    unexpected_calls: usize,
    skipped: usize,
}

impl Report {
    fn compare(&mut self, label: &str, js: std::result::Result<Value, String>, rust: std::result::Result<Value, String>, normalize: fn(&Value) -> Value) {
        self.cases += 1;
        match (js, rust) {
            (Ok(js), Ok(rust)) => {
                let (js, rust) = (normalize(&js), normalize(&rust));
                if let Some(diff) = first_difference("$", &js, &rust) {
                    self.failures.push(format!("{label}: {diff}"));
                } else if let Some(diff) = first_order_difference("$", &js, &rust) {
                    self.order.push(format!("{label}: {diff}"));
                }
            }
            (Err(js), Err(rust)) => {
                if js != rust {
                    self.failures.push(format!("{label}: error js={js:?} rust={rust:?}"));
                }
            }
            (Ok(js), Err(rust)) => self.failures.push(format!("{label}: rust error {rust:?}, js={}", short(&js))),
            (Err(js), Ok(rust)) => self.failures.push(format!("{label}: js error {js:?}, rust={}", short(&rust))),
        }
    }
}

fn identity(value: &Value) -> Value {
    value.clone()
}

/// Live comparison: engine-internal stats are the engine parity suite's
/// business, and timing traces are never reproducible. Keep the planner
/// lane and every OSM-layer stat.
fn live_normalize(value: &Value) -> Value {
    fn strip(value: &Value, top: bool) -> Value {
        match value {
            Value::Object(map) => {
                let mut out = serde_json::Map::new();
                for (key, item) in map {
                    if key == "stats" && item.is_object() {
                        let mut stats = serde_json::Map::new();
                        for (name, stat) in item.as_object().unwrap() {
                            if name == "plannerLane" || name.starts_with("osm") {
                                stats.insert(name.clone(), stat.clone());
                            }
                        }
                        out.insert(key.clone(), Value::Object(stats));
                    } else if key == "trace" && !top {
                        continue;
                    } else {
                        out.insert(key.clone(), strip(item, false));
                    }
                }
                Value::Object(out)
            }
            Value::Array(items) => Value::Array(items.iter().map(|item| strip(item, false)).collect()),
            other => other.clone(),
        }
    }
    strip(value, true)
}

fn wanted(name: &str) -> bool {
    match std::env::var("OSM_PARITY_ONLY") {
        Ok(list) => list.split(',').any(|item| item == name),
        Err(_) => true,
    }
}

fn mode(name: &str) -> bool {
    match std::env::var("OSM_PARITY_MODE") {
        Ok(mode) => mode == name,
        Err(_) => true,
    }
}

fn limit() -> usize {
    std::env::var("OSM_PARITY_LIMIT").ok().and_then(|v| v.parse().ok()).unwrap_or(usize::MAX)
}

fn replay_cases(name: &str, file: &Value, report: &mut Report) {
    for case in file["cases"].as_array().unwrap().iter().take(limit()) {
        let function = case["fn"].as_str().unwrap();
        let args = case["args"].as_array().cloned().unwrap_or_default();
        let label = format!("{name}#{} {function}({})", case["id"], short(&Value::Array(args.clone())));
        if case.get("warm").is_none() {
            match run_pure(function, &args) {
                Err(reason) if reason == "custom lexicon" => report.skipped += 1,
                rust => report.compare(&format!("replay {label}"), expected(&case["cold"]), rust, identity),
            }
            continue;
        }
        if function.starts_with("maps.") {
            continue;
        }
        let caches = Rc::new(OsmCaches::new());
        for phase in ["cold", "warm"] {
            let record = &case[phase];
            let engine = ReplayEngine::new(&record["engine"], &record["calls"], caches.clone());
            let rust = run_with(&engine, function, &args);
            let unexpected = engine.unexpected.borrow().clone();
            report.unexpected_calls += unexpected.len();
            let mut label = format!("replay[{phase}] {label}");
            if !unexpected.is_empty() {
                label.push_str(&format!(" [unrecorded calls: {}]", unexpected.join("; ")));
            }
            report.compare(&label, expected(record), rust, identity);
        }
    }
}

fn replay_unit_tests(name: &str, file: &Value, report: &mut Report) {
    // One replay cache per JS engine object, in call order.
    let mut caches: Vec<(i64, Rc<OsmCaches>)> = Vec::new();
    for (ordinal, record) in file["records"].as_array().unwrap().iter().enumerate() {
        let function = record["fn"].as_str().unwrap();
        let args = record["args"].as_array().cloned().unwrap_or_default();
        let label = format!("replay {name}#{ordinal} {function}({})", short(&Value::Array(args.clone())));
        if record.get("engine").is_none() {
            match run_pure(function, &args) {
                Err(reason) if reason == "custom lexicon" => report.skipped += 1,
                rust => report.compare(&label, expected(record), rust, identity),
            }
            continue;
        }
        let id = record["engine"]["id"].as_i64().unwrap_or(-1);
        let cache = match caches.iter().find(|(seen, _)| *seen == id) {
            Some((_, cache)) => cache.clone(),
            None => {
                let cache = Rc::new(OsmCaches::new());
                caches.push((id, cache.clone()));
                cache
            }
        };
        let engine = ReplayEngine::new(&record["engine"], &record["calls"], cache);
        let rust = run_with(&engine, function, &args);
        let unexpected = engine.unexpected.borrow().clone();
        report.unexpected_calls += unexpected.len();
        let label = if unexpected.is_empty() { label } else { format!("{label} [unrecorded calls: {}]", unexpected.join("; ")) };
        report.compare(&label, expected(record), rust, identity);
    }
}

fn live_cases(name: &str, file: &Value, report: &mut Report) {
    let index = root().join(file["index"].as_str().unwrap());
    let base = format!("{}/", index.display());
    for case in file["cases"].as_array().unwrap().iter().take(limit()) {
        if case.get("warm").is_none() {
            continue;
        }
        let function = case["fn"].as_str().unwrap().to_string();
        let args = case["args"].as_array().cloned().unwrap_or_default();
        let label = format!("live {name}#{} {function}({})", case["id"], short(&Value::Array(args.clone())));
        let engine = Engine::open_blocking(Rc::new(FileTransport::new()), &base, EngineOptions::default()).expect("open index");
        for phase in ["cold", "warm"] {
            let rust = run_live(&engine, &function, &args);
            report.compare(&format!("{label} [{phase}]"), expected(&case[phase]), rust, live_normalize);
        }
    }
}

fn run_live(engine: &Engine, function: &str, args: &[Value]) -> std::result::Result<Value, String> {
    let method = match function {
        "searchOsmQuery" => "search",
        "searchAlongRouteOsm" => "searchAlongRoute",
        "reverseGeocodeOsm" => "reverseGeocode",
        "suggestOsmQuery" => "suggest",
        "resolveOsmSuggestion" => "resolveSuggestion",
        "hydrateOsmSuggestions" => "hydrateSuggestions",
        other if other.starts_with("maps.") => other,
        other => return Err(format!("unknown function {other}")),
    };
    let params = if function == "hydrateOsmSuggestions" { json!({ "suggestions": args[0] }) } else { args.first().cloned().unwrap_or(json!({})) };
    let handle = engine.clone();
    let method = method.to_string();
    engine.block_on(async move { osm::dispatch(&handle, &method, params).await }).map_err(|error| error.message().to_string())
}

#[test]
fn osm_query_parity() {
    let dir = root();
    if !dir.join("cases").exists() {
        panic!("OSM parity fixtures missing: run `node rust/tools/parity/osm_fixtures.mjs` from the repo root");
    }
    let mut report = Report::default();
    let mut names: Vec<PathBuf> = std::fs::read_dir(dir.join("cases")).unwrap().map(|e| e.unwrap().path()).collect();
    names.sort();
    for path in &names {
        let name = path.file_name().unwrap().to_string_lossy().trim_end_matches(".json.gz").to_string();
        if !wanted(&name) {
            continue;
        }
        let file = load_gz(path);
        if mode("replay") {
            replay_cases(&name, &file, &mut report);
        }
        if mode("live") {
            live_cases(&name, &file, &mut report);
        }
    }
    if wanted("replay") && mode("replay") && dir.join("replay").exists() {
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir.join("replay")).unwrap().map(|e| e.unwrap().path()).collect();
        files.sort();
        for path in files {
            let name = path.file_name().unwrap().to_string_lossy().trim_end_matches(".json.gz").to_string();
            replay_unit_tests(&name, &load_gz(&path), &mut report);
        }
    }
    eprintln!(
        "OSM parity: {} comparisons, {} failures, {} key-order differences, {} unrecorded engine calls, {} skipped",
        report.cases,
        report.failures.len(),
        report.order.len(),
        report.unexpected_calls,
        report.skipped
    );
    if let Ok(path) = std::env::var("OSM_PARITY_DUMP") {
        let mut text = report.failures.join("\n");
        text.push_str("\n\n# key order\n");
        text.push_str(&report.order.join("\n"));
        std::fs::write(path, text).unwrap();
    }
    for failure in report.failures.iter().take(25) {
        eprintln!("  {failure}");
    }
    for order in report.order.iter().take(5) {
        eprintln!("  order: {order}");
    }
    assert!(report.failures.is_empty(), "{} OSM parity failures", report.failures.len());
}

/// Rust side of the OSM benchmark (JS side: `rust/tools/parity/osm_bench_js.mjs`):
/// `OSM_BENCH=lux cargo test --release --test osm_parity osm_bench -- --ignored --nocapture`.
#[test]
#[ignore]
fn osm_bench() {
    use std::time::Instant;
    let name = std::env::var("OSM_BENCH").unwrap_or_else(|_| "lux".into());
    let repeat: usize = std::env::var("OSM_BENCH_REPEAT").ok().and_then(|v| v.parse().ok()).unwrap_or(3);
    let file = load_gz(&root().join("cases").join(format!("{name}.json.gz")));
    let base = format!("{}/", root().join(file["index"].as_str().unwrap()).display());
    let cases: Vec<&Value> = file["cases"].as_array().unwrap().iter().filter(|c| c.get("warm").is_some() && !c["fn"].as_str().unwrap().starts_with("maps.")).collect();
    let (mut cold, mut warm) = (Vec::new(), Vec::new());
    let mut by_fn: std::collections::BTreeMap<String, Vec<f64>> = Default::default();
    let (mut requests, mut ranges, mut bytes) = (0u64, 0u64, 0u64);
    for case in &cases {
        let function = case["fn"].as_str().unwrap();
        let args = case["args"].as_array().cloned().unwrap_or_default();
        let transport = Rc::new(FileTransport::new());
        let engine = Engine::open_blocking(transport.clone(), &base, EngineOptions::default()).expect("open");
        let start = Instant::now();
        let _ = run_live(&engine, function, &args);
        let elapsed = start.elapsed().as_secs_f64() * 1000.0;
        cold.push(elapsed);
        by_fn.entry(function.to_string()).or_default().push(elapsed);
        let stats = transport.stats();
        requests += stats.requests;
        ranges += stats.ranges;
        bytes += stats.bytes;
        for _ in 0..repeat {
            let start = Instant::now();
            let _ = run_live(&engine, function, &args);
            warm.push(start.elapsed().as_secs_f64() * 1000.0);
        }
    }
    let pct = |values: &mut Vec<f64>, p: f64| {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        values.get(((values.len() as f64 - 1.0) * p).round() as usize).copied().unwrap_or(0.0)
    };
    let sum: f64 = cold.iter().sum();
    let by_fn: serde_json::Map<String, Value> = by_fn.into_iter().map(|(name, mut values)| (name, json!(pct(&mut values, 0.5)))).collect();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "engine": "rust",
            "fixture": name,
            "cases": cases.len(),
            "cold_ms": { "p50": pct(&mut cold, 0.5), "p95": pct(&mut cold, 0.95), "max": pct(&mut cold, 1.0), "sum": sum },
            "warm_ms": { "p50": pct(&mut warm, 0.5), "p95": pct(&mut warm, 0.95), "max": pct(&mut warm, 1.0) },
            "cold_requests": requests, "cold_ranges": ranges, "cold_bytes_read": bytes,
            "by_function_cold_p50": by_fn
        }))
        .unwrap()
    );
}

struct TimedHost<'e> {
    inner: osm::EngineHost<'e>,
    spent: std::cell::Cell<f64>,
}

impl OsmEngine for TimedHost<'_> {
    fn manifest(&self) -> Rc<Value> {
        self.inner.manifest()
    }
    fn has(&self, method: &str) -> bool {
        self.inner.has(method)
    }
    fn call<'a>(&'a self, method: &'a str, params: Value) -> LocalFuture<'a, Result<Value>> {
        Box::pin(async move {
            let start = std::time::Instant::now();
            let out = self.inner.call(method, params).await;
            self.spent.set(self.spent.get() + start.elapsed().as_secs_f64() * 1000.0);
            out
        })
    }
    fn caches(&self) -> Rc<OsmCaches> {
        self.inner.caches()
    }
}

/// Warm-path split: time inside engine calls vs the OSM layer itself.
#[test]
#[ignore]
fn osm_warm_split() {
    let name = std::env::var("OSM_BENCH").unwrap_or_else(|_| "lux".into());
    let file = load_gz(&root().join("cases").join(format!("{name}.json.gz")));
    let base = format!("{}/", root().join(file["index"].as_str().unwrap()).display());
    let (mut total, mut engine_ms) = (0.0, 0.0);
    for case in file["cases"].as_array().unwrap().iter().filter(|c| c.get("warm").is_some() && !c["fn"].as_str().unwrap().starts_with("maps.")) {
        let engine = Engine::open_blocking(Rc::new(FileTransport::new()), &base, EngineOptions::default()).unwrap();
        let function = case["fn"].as_str().unwrap();
        let args = case["args"].as_array().cloned().unwrap_or_default();
        let persistent = Rc::new(OsmCaches::new());
        let host_for = |engine| if std::env::var("OSM_PERSISTENT").is_ok() { osm::EngineHost::with_caches(engine, persistent.clone()) } else { osm::EngineHost::new(engine) };
        let _ = run_with(&host_for(&engine), function, &args);
        for _ in 0..3 {
            let host = TimedHost { inner: host_for(&engine), spent: std::cell::Cell::new(0.0) };
            let start = std::time::Instant::now();
            let _ = run_with(&host, function, &args);
            total += start.elapsed().as_secs_f64() * 1000.0;
            engine_ms += host.spent.get();
        }
    }
    println!("warm total {total:.1} ms, inside engine calls {engine_ms:.1} ms, OSM layer {:.1} ms", total - engine_ms);
}
