//! Host session: the engine driven by an external event loop.
//!
//! WebAssembly modules, Swift, Kotlin, Dart and C hosts all talk to the
//! engine the same way: they start operations (open an index, run a query),
//! then repeatedly `poll` for (a) fetch requests the engine needs performed
//! and (b) finished operations. The host performs fetches with whatever the
//! platform offers (`fetch`, URLSession, OkHttp, asset readers) and hands
//! the bytes back with `complete`. No threads, no callbacks across the
//! boundary, no async runtime on the host side — just data in and out.
//!
//! The C ABI in `ffi.rs` is a thin byte-buffer wrapper over this type.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde_json::{json, Value};

use crate::engine::{Engine, EngineOptions};
use crate::error::{Error, Result};
use crate::rt::{Promise, Runtime};
use crate::transport::{FetchResponse, HostTransport, Transport};
use crate::util::bytes::Bytes;

pub struct Session {
    runtime: Runtime,
    transport: Rc<HostTransport>,
    engines: Rc<RefCell<HashMap<u32, Engine>>>,
    next_engine: Rc<RefCell<u32>>,
    next_op: u32,
    ops: Vec<(u32, Promise<Value>)>,
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

impl Session {
    pub fn new() -> Self {
        Session {
            runtime: Runtime::new(),
            transport: Rc::new(HostTransport::new()),
            engines: Rc::new(RefCell::new(HashMap::new())),
            next_engine: Rc::new(RefCell::new(0)),
            next_op: 0,
            ops: Vec::new(),
        }
    }

    fn start(&mut self, future: impl std::future::Future<Output = Result<Value>> + 'static) -> u32 {
        self.next_op += 1;
        let op = self.next_op;
        let promise = self.runtime.start(future);
        self.ops.push((op, promise));
        self.runtime.run_until_stalled();
        op
    }

    /// Open an index. The operation resolves to `{ "engine": id, "manifest": {...} }`.
    pub fn open(&mut self, base_url: &str, options: Value) -> u32 {
        let transport: Rc<dyn Transport> = self.transport.clone();
        let base = base_url.to_string();
        let engines = self.engines.clone();
        let next = self.next_engine.clone();
        let runtime = self.runtime.clone();
        self.start(async move {
            let engine = Engine::open_with_runtime(transport, &base, EngineOptions::from_json(options), runtime).await?;
            let id = {
                let mut counter = next.borrow_mut();
                *counter += 1;
                *counter
            };
            let summary = engine.summary();
            engines.borrow_mut().insert(id, engine);
            Ok(json!({ "engine": id, "summary": summary }))
        })
    }

    /// Run `method` (`search`, `count`, `suggest`, `vectorSearch`,
    /// `authorityLookup`, `loadFacetValues`, ...) on an open engine.
    pub fn call(&mut self, engine: u32, method: &str, params: Value) -> u32 {
        let found = self.engines.borrow().get(&engine).cloned();
        let method = method.to_string();
        self.start(async move {
            match found {
                Some(engine) => engine.call(&method, params).await,
                None => Err(Error::new(format!("Rangefind engine {engine} is not open."))),
            }
        })
    }

    pub fn close(&mut self, engine: u32) -> bool {
        self.engines.borrow_mut().remove(&engine).is_some()
    }

    /// Deliver fetched bytes. `parts` are the per-range buffers in request
    /// order (one buffer for a whole-object request).
    pub fn complete(&mut self, request: u32, parts: Vec<Vec<u8>>) {
        self.transport.complete(request, Ok(FetchResponse::Ok(parts.into_iter().map(Bytes::new).collect())));
        self.runtime.run_until_stalled();
    }

    pub fn complete_missing(&mut self, request: u32) {
        self.transport.complete(request, Ok(FetchResponse::Missing));
        self.runtime.run_until_stalled();
    }

    pub fn complete_error(&mut self, request: u32, message: &str) {
        self.transport.complete(request, Err(Error::new(message)));
        self.runtime.run_until_stalled();
    }

    /// Pending fetch requests and finished operations since the last poll:
    /// `{ "requests": [{id, url, ranges: [[offset, length], ...]}],
    ///    "done": [{op, ok: true, value} | {op, ok: false, error, code}],
    ///    "pending": <ops still running> }`.
    pub fn poll(&mut self) -> Value {
        self.runtime.run_until_stalled();
        let requests: Vec<Value> = self
            .transport
            .take_requests()
            .into_iter()
            .map(|(id, request)| {
                json!({
                    "id": id,
                    "url": request.url,
                    "ranges": request.ranges.iter().map(|range| json!([range.offset, range.length])).collect::<Vec<_>>()
                })
            })
            .collect();
        let mut done = Vec::new();
        self.ops.retain(|(op, promise)| match promise.peek() {
            None => true,
            Some(Ok(value)) => {
                done.push(json!({ "op": op, "ok": true, "value": value }));
                false
            }
            Some(Err(error)) => {
                done.push(json!({ "op": op, "ok": false, "error": error.message(), "code": error.code() }));
                false
            }
        });
        json!({ "requests": requests, "done": done, "pending": self.ops.len() })
    }

    pub fn pending_ops(&self) -> usize {
        self.ops.len()
    }
}

impl Drop for Session {
    /// Release everything, even with operations in flight. Unfinished tasks
    /// hold engines that hold the runtime that holds the tasks, so without
    /// this a session freed mid-query (`rf_session_free` while a fetch is
    /// unanswered) would leak its whole engine graph.
    fn drop(&mut self) {
        self.ops.clear();
        self.runtime.shutdown();
        let engines = std::mem::take(&mut *self.engines.borrow_mut());
        drop(engines);
        self.transport.cancel_all();
    }
}
