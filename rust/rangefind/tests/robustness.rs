#![cfg(feature = "engine")]
//! Robustness regressions: cancelled loads, concurrent load failures, the
//! file transport's descriptor bound and range clamping, the gzip inflate
//! cap, the C ABI's range-length validation and `JoinAll`'s per-child wakes.

use std::cell::Cell;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use rangefind::engine::io::{inflate_limited, inflate_object};
use rangefind::engine::{Engine, EngineOptions};
use rangefind::pointer::Pointer;
use rangefind::rt::{join_all, Promise, Runtime};
use rangefind::transport::{ByteRange, FetchRequest, FetchResponse, FileTransport, HostTransport, Transport};
use rangefind::util::bytes::Bytes;
use serde_json::{json, Value};

fn index_base(name: &str) -> Option<String> {
    let dir: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/parity").join(name).join("index");
    if !dir.exists() {
        eprintln!("skipping: run `node rust/tools/parity/build_fixtures.mjs` first");
        return None;
    }
    Some(format!("{}/", dir.display()))
}

/// Answer every queued host request from disk; `fail` picks requests to
/// answer with an error instead.
fn serve(transport: &HostTransport, fail: &mut dyn FnMut(&FetchRequest) -> bool) {
    for (id, request) in transport.take_requests() {
        if fail(&request) {
            transport.complete(id, Err(rangefind::error::Error::new("injected failure")));
            continue;
        }
        let response = match std::fs::read(&request.url) {
            Err(_) => Ok(FetchResponse::Missing),
            Ok(data) if request.ranges.is_empty() => Ok(FetchResponse::Ok(vec![Bytes::new(data)])),
            Ok(data) => Ok(FetchResponse::Ok(
                request
                    .ranges
                    .iter()
                    .map(|range| {
                        let start = (range.offset as usize).min(data.len());
                        let end = (start + range.length as usize).min(data.len());
                        Bytes::new(data[start..end].to_vec())
                    })
                    .collect(),
            )),
        };
        transport.complete(id, response);
    }
}

fn open_host_engine(base: &str) -> (Rc<HostTransport>, Runtime, Engine) {
    let transport = Rc::new(HostTransport::new());
    let runtime = Runtime::new();
    let dyn_transport: Rc<dyn Transport> = transport.clone();
    let inner = runtime.clone();
    let base = base.to_string();
    let opened = runtime.start(async move { Engine::open_with_runtime(dyn_transport, &base, EngineOptions::default(), inner).await });
    for _ in 0..1000 {
        runtime.run_until_stalled();
        serve(&transport, &mut |_| false);
        if opened.is_settled() {
            break;
        }
    }
    let engine = opened.peek().expect("open settled").expect("open ok");
    (transport, runtime, engine)
}

fn drive<T: Clone>(runtime: &Runtime, transport: &HostTransport, promise: &Promise<T>, fail: &mut dyn FnMut(&FetchRequest) -> bool) {
    for _ in 0..1000 {
        runtime.run_until_stalled();
        serve(transport, fail);
        if promise.is_settled() {
            return;
        }
    }
    panic!("operation never settled (in flight: {})", transport.in_flight());
}

fn reference(base: &str, params: Value) -> Value {
    let engine = Engine::open_blocking(Rc::new(FileTransport::new()), base, EngineOptions::default()).unwrap();
    engine.call_blocking("search", params).unwrap()
}

/// Finding 1: a search future dropped while its fetch is unanswered used to
/// leave "Rangefind request was cancelled." cached (directory root/pages)
/// or reserved promises pending forever (posting packs, blocks), so the
/// next query failed or hung. Every cancellation point must leave the
/// engine as good as new.
#[test]
fn dropped_search_does_not_poison_caches() {
    let Some(base) = index_base("blocks") else { return };
    let params = json!({ "q": "museum", "size": 10 });
    let expected = reference(&base, params.clone());
    for cancel_after in 1..=8 {
        let (transport, runtime, engine) = open_host_engine(&base);
        {
            let first = engine.clone();
            let first_params = params.clone();
            let mut future = Box::pin(async move { first.search(first_params).await });
            let mut cx = Context::from_waker(Waker::noop());
            for step in 0..cancel_after {
                if future.as_mut().poll(&mut cx).is_ready() {
                    break;
                }
                if step + 1 < cancel_after {
                    serve(&transport, &mut |_| false);
                }
            }
            // Dropped here with its last requests unanswered.
        }
        let _ = transport.take_requests();
        let second = engine.clone();
        let second_params = params.clone();
        let promise = runtime.start(async move { second.search(second_params).await });
        drive(&runtime, &transport, &promise, &mut |_| false);
        let got = promise.peek().unwrap().unwrap_or_else(|error| panic!("cancel after {cancel_after}: {}", error.message()));
        assert_eq!(got["results"], expected["results"], "cancel after {cancel_after}");
    }
}

/// Finding 6: a query that finds a key already in flight for another query
/// must surface that load's failure, not read the shard as missing.
#[test]
fn concurrent_load_failure_propagates() {
    let Some(base) = index_base("blocks") else { return };
    let (transport, runtime, engine) = open_host_engine(&base);
    let (a_engine, b_engine) = (engine.clone(), engine.clone());
    let a = runtime.start(async move { a_engine.search(json!({ "q": "museum", "size": 10 })).await });
    let b = runtime.start(async move { b_engine.search(json!({ "q": "museum library", "size": 10 })).await });
    // Fail only the first posting-pack request (query A's shard); B's own
    // shard loads succeed, but B also needs A's shard.
    let mut failed = false;
    let mut fail = |request: &FetchRequest| {
        if !failed && request.url.contains("terms/packs/") {
            failed = true;
            return true;
        }
        false
    };
    for _ in 0..1000 {
        runtime.run_until_stalled();
        serve(&transport, &mut fail);
        if a.is_settled() && b.is_settled() {
            break;
        }
    }
    assert!(failed, "the fixture issued no posting-pack request");
    assert!(matches!(a.peek(), Some(Err(_))), "A must fail: {:?}", a.peek().map(|r| r.map(|v| v["total"].clone())));
    assert!(matches!(b.peek(), Some(Err(_))), "B must see A's failure, got {:?}", b.peek().map(|r| r.map(|v| v["total"].clone())));
    // And nothing stays poisoned: the same queries succeed afterwards.
    let retry_engine = engine.clone();
    let retry = runtime.start(async move { retry_engine.search(json!({ "q": "museum library", "size": 10 })).await });
    drive(&runtime, &transport, &retry, &mut |_| false);
    assert!(matches!(retry.peek(), Some(Ok(_))));
}

/// Finding 1 (prefetch): an engine opened with `Engine::open` owns a runtime
/// nothing drives, so it must not count as driven (sharded prefetch is then
/// skipped instead of parking tasks that hold the engine forever).
#[test]
fn undriven_runtime_reports_it() {
    let runtime = Runtime::new();
    assert!(!runtime.is_driven());
    runtime.run_until_stalled();
    assert!(runtime.is_driven());
}

/// A stalled blocking call drops its future instead of leaving it parked.
#[test]
fn stalled_block_on_releases_its_future() {
    let runtime = Runtime::new();
    let held = Rc::new(());
    let never: Promise<u32> = Promise::pending();
    let witness = held.clone();
    let result = runtime.block_on(async move {
        let _keep = witness;
        never.await
    });
    assert!(result.is_err());
    assert_eq!(Rc::strong_count(&held), 1, "the stalled future was not dropped");
    assert_eq!(runtime.pending_tasks(), 0);
}

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rangefind-robustness-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn fetch_now(transport: &FileTransport, url: &str, ranges: Vec<ByteRange>) -> rangefind::error::Result<FetchResponse> {
    transport.fetch(FetchRequest { url: url.to_string(), ranges }).peek().expect("file transport settles synchronously")
}

/// Finding 3: handles used to be cached forever, so ~253 distinct files hit
/// EMFILE under `ulimit -n 256`. They now live in a bounded LRU.
#[test]
fn file_transport_bounds_open_handles() {
    let dir = scratch_dir("fds");
    let transport = FileTransport::with_max_open_files(4);
    for index in 0..300 {
        let path = dir.join(format!("f{index}"));
        std::fs::write(&path, format!("file {index}")).unwrap();
        match fetch_now(&transport, &path.display().to_string(), vec![ByteRange { offset: 0, length: 4 }]).unwrap() {
            FetchResponse::Ok(parts) => assert_eq!(&*parts[0], b"file"),
            FetchResponse::Missing => panic!("missing"),
        }
        assert!(transport.open_files() <= 4);
    }
    // Missing files are not remembered: one that appears later is found.
    let late = dir.join("late");
    let late_url = late.display().to_string();
    assert!(matches!(fetch_now(&transport, &late_url, vec![]), Ok(FetchResponse::Missing)));
    std::fs::write(&late, b"now").unwrap();
    assert!(matches!(fetch_now(&transport, &late_url, vec![]), Ok(FetchResponse::Ok(_))));
    std::fs::remove_dir_all(&dir).ok();
}

/// Finding 4: a corrupt pointer length (2^52) used to allocate the whole
/// length up front and abort. The read is clamped to what the file holds;
/// an offset past the end fails like an HTTP 416.
#[test]
fn file_transport_clamps_ranges() {
    let dir = scratch_dir("ranges");
    let path = dir.join("data.bin");
    std::fs::File::create(&path).unwrap().write_all(b"0123456789").unwrap();
    let url = path.display().to_string();
    let transport = FileTransport::new();
    match fetch_now(&transport, &url, vec![ByteRange { offset: 4, length: 1 << 52 }, ByteRange { offset: 10, length: 5 }]).unwrap() {
        FetchResponse::Ok(parts) => {
            assert_eq!(&*parts[0], b"456789");
            assert!(parts[1].is_empty());
        }
        FetchResponse::Missing => panic!("missing"),
    }
    let error = fetch_now(&transport, &url, vec![ByteRange { offset: 11, length: 1 }]).unwrap_err();
    assert!(error.message().contains("Range request failed"), "{}", error.message());
    let error = fetch_now(&transport, &url, vec![ByteRange { offset: u64::MAX, length: u64::MAX }]).unwrap_err();
    assert!(error.message().contains("Range request failed"));
    // Plain paths are used verbatim: `?`, `#` and `%` are file-name bytes.
    let odd = dir.join("a?b#c%20d.bin");
    std::fs::write(&odd, b"odd").unwrap();
    assert!(matches!(fetch_now(&transport, &odd.display().to_string(), vec![]), Ok(FetchResponse::Ok(_))));
    std::fs::remove_dir_all(&dir).ok();
}

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

/// Finding 5: a gzip bomb must fail with an error, not OOM-abort.
#[test]
fn inflate_is_capped() {
    let bomb = gzip(&vec![0u8; 8 << 20]);
    assert!(bomb.len() < 64 << 10);
    let error = inflate_limited(&bomb, 1 << 20).unwrap_err();
    assert!(error.message().contains("maxInflateBytes"), "{}", error.message());
    assert_eq!(inflate_limited(&bomb, 8 << 20).unwrap().len(), 8 << 20);
    // A pointer's logicalLength bounds its object to a small multiple.
    let mut pointer = Pointer::new("p", 0, bomb.len() as u64);
    pointer.logical_length = Some(1024);
    assert!(inflate_object(&Bytes::new(bomb.clone()), &pointer, "bomb", false).is_err());
    pointer.logical_length = Some(8 << 20);
    assert_eq!(inflate_object(&Bytes::new(bomb), &pointer, "bomb", false).unwrap().len(), 8 << 20);
}

/// Finding 5 (option): `maxInflateBytes` caps whole-object inflates.
#[test]
fn max_inflate_bytes_option_applies() {
    let Some(base) = index_base("blocks") else { return };
    let tiny = EngineOptions::from_json(json!({ "maxInflateBytes": 16 }));
    let engine = Engine::open_blocking(Rc::new(FileTransport::new()), &base, tiny).unwrap();
    let error = engine.call_blocking("search", json!({ "q": "museum", "size": 10 })).unwrap_err();
    assert!(error.message().contains("maxInflateBytes"), "{}", error.message());
}

/// Finding 7: a wide join whose children complete one at a time must poll
/// only the woken child each time — O(n) polls in total, not O(n²).
#[test]
fn join_all_polls_only_woken_children() {
    const N: usize = 500;
    let runtime = Runtime::new();
    let polls = Rc::new(Cell::new(0usize));
    let gates: Vec<Promise<usize>> = (0..N).map(|_| Promise::pending()).collect();
    let children: Vec<_> = gates
        .iter()
        .cloned()
        .map(|gate| {
            let polls = polls.clone();
            let mut gate = Box::pin(gate);
            std::future::poll_fn(move |cx| {
                polls.set(polls.get() + 1);
                match gate.as_mut().poll(cx) {
                    Poll::Ready(value) => Poll::Ready(value.unwrap()),
                    Poll::Pending => Poll::Pending,
                }
            })
        })
        .collect();
    let out = runtime.start(async move { Ok(join_all(children).await) });
    runtime.run_until_stalled();
    for (index, gate) in gates.iter().enumerate() {
        gate.resolve(index);
        runtime.run_until_stalled();
    }
    assert_eq!(out.peek().unwrap().unwrap(), (0..N).collect::<Vec<_>>());
    assert_eq!(polls.get(), 2 * N, "each child is polled once up front and once when woken");
}
