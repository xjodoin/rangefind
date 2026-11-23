#![cfg(feature = "engine")]
//! Native test of the host protocol (`host::Session` and the C ABI in
//! `ffi.rs`): the exact poll → fetch → complete loop that the wasm glue,
//! Swift, Kotlin and Dart hosts run. The host side here answers fetches from
//! the filesystem, deliberately out of order, and the answers must equal the
//! direct (blocking) engine's.

use std::path::{Path, PathBuf};
use std::rc::Rc;

use rangefind::engine::{Engine, EngineOptions};
use rangefind::host::Session;
use rangefind::transport::FileTransport;
use serde_json::{json, Value};

fn index_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/parity").join(name).join("index")
}

fn base(name: &str) -> Option<String> {
    let dir = index_dir(name);
    if !dir.exists() {
        eprintln!("skipping: run `node rust/tools/parity/build_fixtures.mjs` first");
        return None;
    }
    Some(format!("{}/", dir.display()))
}

/// Serve one request from disk: whole file or the requested ranges.
fn serve(url: &str, ranges: &[Value]) -> Option<Vec<Vec<u8>>> {
    let data = std::fs::read(url).ok()?;
    if ranges.is_empty() {
        return Some(vec![data]);
    }
    Some(
        ranges
            .iter()
            .map(|range| {
                let offset = range[0].as_u64().unwrap() as usize;
                let length = range[1].as_u64().unwrap() as usize;
                data[offset.min(data.len())..(offset + length).min(data.len())].to_vec()
            })
            .collect(),
    )
}

/// Drive the session until `op` finishes; answers requests in reverse
/// order to prove nothing depends on completion order.
fn run(session: &mut Session, op: u32) -> Result<Value, String> {
    for _ in 0..10_000 {
        let events = session.poll();
        for done in events["done"].as_array().unwrap() {
            if done["op"].as_u64() == Some(op as u64) {
                return if done["ok"] == json!(true) { Ok(done["value"].clone()) } else { Err(done["error"].as_str().unwrap_or("").to_string()) };
            }
        }
        let mut requests = events["requests"].as_array().unwrap().clone();
        requests.reverse();
        for request in requests {
            let id = request["id"].as_u64().unwrap() as u32;
            match serve(request["url"].as_str().unwrap(), request["ranges"].as_array().unwrap()) {
                Some(parts) => session.complete(id, parts),
                None => session.complete_missing(id),
            }
        }
    }
    Err("session did not finish".into())
}

#[test]
fn session_matches_direct_engine() {
    let Some(base) = base("blocks") else { return };
    let mut session = Session::new();
    let open_op = session_open(&mut session, &base);
    let opened = run(&mut session, open_op).expect("open");
    let engine_id = opened["engine"].as_u64().unwrap() as u32;
    let direct = Engine::open_blocking(Rc::new(FileTransport::new()), &base, EngineOptions::default()).unwrap();
    for params in [
        json!({ "q": "museum", "size": 10 }),
        json!({ "q": "river city", "size": 5, "page": 2 }),
        json!({ "q": "", "size": 10, "sort": "-year" }),
        json!({ "q": "library", "size": 10, "filters": { "facets": { "category": ["culture"] } } }),
        json!({ "q": "quick brown fox", "size": 10, "facets": ["category"] }),
    ] {
        let op = session.call(engine_id, "search", params.clone());
        let via_session = run(&mut session, op).expect("session search");
        let via_direct = direct.call_blocking("search", params.clone()).expect("direct search");
        assert_eq!(via_session["results"], via_direct["results"], "{params}");
        assert_eq!(via_session["total"], via_direct["total"], "{params}");
    }
    let op = session.call(engine_id, "count", json!({ "q": "museum library" }));
    assert!(run(&mut session, op).unwrap()["total"].as_u64().is_some());
    assert!(session.close(engine_id));
    let op = session.call(engine_id, "search", json!({ "q": "x" }));
    assert!(run(&mut session, op).unwrap_err().contains("is not open"));
}

fn session_open(session: &mut Session, base: &str) -> u32 {
    session.open(base, json!({}))
}

#[test]
fn missing_index_fails_cleanly() {
    let mut session = Session::new();
    let op = session.open("/definitely/not/here/", json!({}));
    assert_eq!(run(&mut session, op).unwrap_err(), "Rangefind manifest could not be loaded.");
}

#[test]
fn transport_errors_propagate() {
    let Some(base) = base("basic") else { return };
    let mut session = Session::new();
    let open_op = session_open(&mut session, &base);
    let opened = run(&mut session, open_op).unwrap();
    let engine_id = opened["engine"].as_u64().unwrap() as u32;
    let op = session.call(engine_id, "search", json!({ "q": "museum" }));
    // Fail every request: the error must reach the operation, not hang.
    let mut error = None;
    for _ in 0..100 {
        let events = session.poll();
        if let Some(done) = events["done"].as_array().unwrap().iter().find(|d| d["op"].as_u64() == Some(op as u64)) {
            error = done["error"].as_str().map(str::to_string);
            break;
        }
        for request in events["requests"].as_array().unwrap() {
            session.complete_error(request["id"].as_u64().unwrap() as u32, "network down");
        }
    }
    assert_eq!(error.as_deref(), Some("network down"));
    // The engine recovers: failed loads are evicted and retried.
    let op = session.call(engine_id, "search", json!({ "q": "museum" }));
    assert!(run(&mut session, op).is_ok());
}

#[test]
fn c_abi_round_trip() {
    use rangefind::ffi::*;
    let Some(base) = base("basic") else { return };
    unsafe {
        let session = rf_session_new();
        let op = rf_open(session, base.as_ptr(), base.len(), b"{}".as_ptr(), 2);
        let mut engine = 0u32;
        let mut search_op = 0u32;
        for _ in 0..1000 {
            let ptr = rf_poll(session);
            let events: Value = serde_json::from_slice(std::slice::from_raw_parts(ptr, rf_result_len(session))).unwrap();
            for done in events["done"].as_array().unwrap() {
                if done["op"].as_u64() == Some(op as u64) {
                    engine = done["value"]["engine"].as_u64().unwrap() as u32;
                    let params = br#"{"q":"museum","size":3}"#;
                    search_op = rf_call(session, engine, b"search".as_ptr(), 6, params.as_ptr(), params.len());
                }
                if search_op != 0 && done["op"].as_u64() == Some(search_op as u64) {
                    assert_eq!(done["ok"], json!(true));
                    assert!(done["value"]["results"].as_array().unwrap().len() <= 3);
                    rf_session_free(session);
                    assert!(rf_version() > 0);
                    return;
                }
            }
            for request in events["requests"].as_array().unwrap() {
                let id = request["id"].as_u64().unwrap() as u32;
                match serve(request["url"].as_str().unwrap(), request["ranges"].as_array().unwrap()) {
                    None => rf_complete_missing(session, id),
                    Some(parts) => {
                        let lengths: Vec<u32> = parts.iter().map(|p| p.len() as u32).collect();
                        let data: Vec<u8> = parts.concat();
                        let count = if request["ranges"].as_array().unwrap().is_empty() { 0 } else { lengths.len() };
                        rf_complete(session, id, data.as_ptr(), data.len(), lengths.as_ptr(), count);
                    }
                }
            }
        }
        let _ = engine;
        panic!("C ABI session did not finish");
    }
}
