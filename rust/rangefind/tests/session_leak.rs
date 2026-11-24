#![cfg(feature = "engine")]
//! Finding 2: freeing a host session with a fetch still unanswered used to
//! leak the whole engine (runtime → task → engine → runtime cycle, ~166 KB
//! on the `blocks` fixture). Measured with a counting allocator, so this
//! binary holds exactly one test.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicIsize, Ordering};

use rangefind::host::Session;
use serde_json::{json, Value};

struct Counting;
static LIVE: AtomicIsize = AtomicIsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size() as isize, Ordering::Relaxed);
        System.alloc_zeroed(layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        LIVE.fetch_add(size as isize - layout.size() as isize, Ordering::Relaxed);
        System.realloc(ptr, layout, size)
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn serve(url: &str, ranges: &[Value]) -> Option<Vec<Vec<u8>>> {
    let data = std::fs::read(url).ok()?;
    if ranges.is_empty() {
        return Some(vec![data]);
    }
    Some(
        ranges
            .iter()
            .map(|range| {
                let offset = (range[0].as_u64().unwrap() as usize).min(data.len());
                let end = (offset + range[1].as_u64().unwrap() as usize).min(data.len());
                data[offset..end].to_vec()
            })
            .collect(),
    )
}

fn answer(session: &mut Session, requests: &[Value]) {
    for request in requests {
        let id = request["id"].as_u64().unwrap() as u32;
        match serve(request["url"].as_str().unwrap(), request["ranges"].as_array().unwrap()) {
            Some(parts) => session.complete(id, parts),
            None => session.complete_missing(id),
        }
    }
}

/// Open the index, start a search, and free the session once the search
/// has a request outstanding (or after it finishes, with `abandon` false).
/// Returns the bytes still allocated afterwards.
fn cycle(base: &str, abandon: bool) -> isize {
    let before = LIVE.load(Ordering::Relaxed);
    {
        let mut session = Session::new();
        let op = session.open(base, json!({}));
        let mut engine = 0u32;
        for _ in 0..1000 {
            let events = session.poll();
            if let Some(done) = events["done"].as_array().unwrap().iter().find(|done| done["op"] == json!(op)) {
                engine = done["value"]["engine"].as_u64().unwrap() as u32;
                break;
            }
            answer(&mut session, events["requests"].as_array().unwrap());
        }
        let search = session.call(engine, "search", json!({ "q": "museum", "size": 10 }));
        let mut abandoned = false;
        for _ in 0..1000 {
            let events = session.poll();
            if events["done"].as_array().unwrap().iter().any(|done| done["op"] == json!(search)) {
                break;
            }
            let requests = events["requests"].as_array().unwrap().clone();
            if abandon && !requests.is_empty() {
                abandoned = true;
                break;
            }
            answer(&mut session, &requests);
        }
        assert_eq!(abandoned, abandon);
        drop(session);
    }
    LIVE.load(Ordering::Relaxed) - before
}

#[test]
fn freeing_a_session_mid_query_releases_the_engine() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/parity/blocks/index");
    if !dir.exists() {
        eprintln!("skipping: run `node rust/tools/parity/build_fixtures.mjs` first");
        return;
    }
    let base = format!("{}/", dir.display());
    // Warm one-time statics (thread-locals, lazy tables) first.
    cycle(&base, false);
    cycle(&base, true);
    let completed = cycle(&base, false);
    let abandoned = cycle(&base, true);
    assert!(completed.abs() < 4096, "completed session leaked {completed} bytes");
    assert!(abandoned.abs() < 4096, "abandoned session leaked {abandoned} bytes");
}
