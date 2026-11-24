//! The query engine: a port of `createSearch()` from `src/runtime.js`.
//!
//! [`Engine`] is the public handle. It wraps a single index
//! ([`index::IndexEngine`]) or a federation of indexes (generational or
//! sharded manifests). Requests and responses are JSON values with exactly
//! the shapes the JavaScript runtime uses, so the wasm build is a drop-in
//! replacement and the parity suite can compare responses byte-for-byte.

pub mod authority_lane;
pub mod bundles;
pub mod cache_cost;
pub mod docs;
pub mod federation;
pub mod filters;
pub mod geo_lanes;
pub mod geo_plan;
pub mod index;
pub mod io;
pub mod loaders;
pub mod options;
pub mod replica;
pub mod search;
pub mod text;
pub mod trace;
pub mod typo_lane;
pub mod vector_lane;

use std::future::Future;
use std::pin::Pin;
use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde_json::{json, Value};

use crate::error::{Error, Result};
use crate::rt::Runtime;
use crate::transport::{resolve_url, Transport};

pub use index::IndexEngine;
pub use options::EngineOptions;

#[derive(Clone)]
pub struct Engine {
    kind: EngineKind,
    runtime: Runtime,
    /// Per-engine state for layers built on top of the engine (the OSM
    /// query layer's locality/authority caches), shared by clones.
    extensions: Rc<RefCell<HashMap<&'static str, Rc<dyn Any>>>>,
}

#[derive(Clone)]
enum EngineKind {
    Single(Rc<IndexEngine>),
    Generational(Rc<federation::GenerationalEngine>),
    Sharded(Rc<federation::ShardedEngine>),
}

type LocalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Open the engine for `base` on a shared I/O layer, dispatching on the
/// manifest shape like `createSearch()`: sharded roots, generational roots
/// and single indexes. Federations open their children through this same
/// function, so the layers compose.
pub(crate) fn open_with_io(io: Rc<io::Io>, base: String, options: EngineOptions, runtime: Runtime) -> LocalFuture<'static, Result<Engine>> {
    Box::pin(async move {
        let manifest = match &options.manifest {
            Some(manifest) => manifest.clone(),
            None => load_manifest(&io, &base, options.manifest_name.as_deref()).await?,
        };
        if manifest.get("shards").map(Value::is_array).unwrap_or(false) {
            let sharded = federation::ShardedEngine::new(manifest, options, base, io, runtime.clone())?;
            return Ok(Engine { kind: EngineKind::Sharded(Rc::new(sharded)), runtime, extensions: Default::default() });
        }
        if manifest.get("generations").map(Value::is_array).unwrap_or(false) {
            let generational = federation::GenerationalEngine::open(manifest, options, base, io, runtime.clone()).await?;
            return Ok(Engine { kind: EngineKind::Generational(Rc::new(generational)), runtime, extensions: Default::default() });
        }
        let index = IndexEngine::new(manifest, base, options, io, runtime.clone())?;
        Ok(Engine { kind: EngineKind::Single(Rc::new(index)), runtime, extensions: Default::default() })
    })
}

impl Engine {
    /// Engine-scoped state for layers built on top of the engine.
    pub fn extension<T: 'static>(&self, key: &'static str) -> Option<Rc<T>> {
        self.extensions.borrow().get(key).cloned().and_then(|value| value.downcast::<T>().ok())
    }

    pub fn set_extension<T: 'static>(&self, key: &'static str, value: Rc<T>) {
        self.extensions.borrow_mut().insert(key, value);
    }

    /// Open the index at `base_url` (a directory URL or path ending in `/`).
    pub async fn open(transport: Rc<dyn Transport>, base_url: &str, options: EngineOptions) -> Result<Engine> {
        Self::open_with_runtime(transport, base_url, options, Runtime::new()).await
    }

    pub async fn open_with_runtime(
        transport: Rc<dyn Transport>,
        base_url: &str,
        options: EngineOptions,
        runtime: Runtime,
    ) -> Result<Engine> {
        let base = if base_url.is_empty() { "./rangefind/".to_string() } else { base_url.to_string() };
        let mut io = io::Io::new(transport, options.multi_range_requests, options.multi_range_max_ranges, Rc::new(trace::Trace::default()));
        if let Some(limit) = options.get("maxInflateBytes").as_f64().filter(|limit| limit.is_finite() && *limit >= 1.0) {
            io = io.with_max_inflate_bytes(limit as u64);
        }
        io = io.with_cache_budget(options::cache_budget_bytes(&options));
        let io = Rc::new(io);
        open_with_io(io, base, options, runtime).await
    }

    /// Open with a transport that completes synchronously (files, memory).
    pub fn open_blocking(transport: Rc<dyn Transport>, base_url: &str, options: EngineOptions) -> Result<Engine> {
        let runtime = Runtime::new();
        let base = base_url.to_string();
        let inner = runtime.clone();
        runtime.block_on(async move { Engine::open_with_runtime(transport, &base, options, inner).await })
    }

    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    pub fn manifest(&self) -> Value {
        match &self.kind {
            EngineKind::Single(index) => index.manifest_value(),
            EngineKind::Generational(engine) => engine.root.clone(),
            EngineKind::Sharded(engine) => engine.root.clone(),
        }
    }

    /// The single index behind this engine, if it is one.
    pub fn single(&self) -> Option<Rc<IndexEngine>> {
        match &self.kind {
            EngineKind::Single(index) => Some(index.clone()),
            _ => None,
        }
    }

    fn kind_name(&self) -> &'static str {
        match &self.kind {
            EngineKind::Single(_) => "single",
            EngineKind::Generational(_) => "generational",
            EngineKind::Sharded(_) => "sharded",
        }
    }

    fn io(&self) -> &io::Io {
        match &self.kind {
            EngineKind::Single(index) => &index.io,
            EngineKind::Generational(engine) => &engine.io,
            EngineKind::Sharded(engine) => &engine.io,
        }
    }

    /// `hydrateRows(rows, context)` on any engine kind (federations need
    /// `context.generation` / `context.shard` naming the owner).
    pub fn hydrate_rows<'a>(&'a self, rows: &'a [docs::Row], context: Value) -> LocalFuture<'a, Result<Vec<Value>>> {
        Box::pin(async move {
            let _scope = self.io().cache_budget.enter();
            match &self.kind {
                EngineKind::Single(index) => {
                    let mut context = docs::ResultContext::from_json(&context);
                    index.rows_to_results(rows, &mut context).await
                }
                EngineKind::Generational(engine) => engine.hydrate_rows(rows, context).await,
                EngineKind::Sharded(engine) => engine.hydrate_rows(rows, context).await,
            }
        })
    }

    /// Small description returned to hosts when an index opens.
    pub fn summary(&self) -> Value {
        let manifest = self.manifest();
        json!({
            "total": manifest.get("total").cloned().unwrap_or(Value::Null),
            "features": manifest.get("features").cloned().unwrap_or(Value::Null),
            "facets": manifest.get("facets").map(|facets| facets.as_object().map(|map| map.keys().cloned().collect::<Vec<_>>())).unwrap_or(None),
            "kind": self.kind_name()
        })
    }

    pub async fn search(&self, params: Value) -> Result<Value> {
        self.call("search", params).await
    }

    pub async fn count(&self, params: Value) -> Result<Value> {
        self.call("count", params).await
    }

    pub async fn suggest(&self, params: Value) -> Result<Value> {
        self.call("suggest", params).await
    }

    pub async fn vector_search(&self, params: Value) -> Result<Value> {
        self.call("vectorSearch", params).await
    }

    /// Dispatch by JS method name.
    pub fn call<'a>(&'a self, method: &'a str, params: Value) -> LocalFuture<'a, Result<Value>> {
        Box::pin(async move {
            // Everything this call touches stays cached until it ends; the
            // cache budget is enforced when it does.
            let _scope = self.io().cache_budget.enter();
            #[cfg(feature = "osm")]
            if let Some(name) = method.strip_prefix("osm.") {
                return crate::osm::query::dispatch(self, name, params).await;
            }
            let response = match &self.kind {
                EngineKind::Single(index) => index.call(method, params).await,
                EngineKind::Generational(engine) => engine.call(method, params).await,
                EngineKind::Sharded(engine) => engine.call(method, params).await,
            };
            match (method, response) {
                ("stats", Ok(Value::Object(mut stats))) => {
                    stats.insert("cache".into(), self.cache_stats());
                    Ok(Value::Object(stats))
                }
                (_, response) => response,
            }
        })
    }

    /// Run `method` to completion on the engine's runtime (synchronous
    /// transports only).
    pub fn call_blocking(&self, method: &str, params: Value) -> Result<Value> {
        let engine = self.clone();
        let method = method.to_string();
        self.runtime.block_on(async move { engine.call(&method, params).await })
    }

    pub fn block_on<T: Clone + 'static>(&self, future: impl Future<Output = Result<T>> + 'static) -> Result<T> {
        self.runtime.block_on(future)
    }

    /// Transfer statistics for this engine (requests, ranges, bytes per
    /// object bucket).
    pub fn transfer_stats(&self) -> Value {
        self.io().trace.to_json()
    }

    pub fn reset_transfer_stats(&self) {
        self.io().trace.reset()
    }

    /// The memory budget shared by this engine's caches (layers built on
    /// the engine, such as the OSM query caches, join it too).
    pub fn cache_budget(&self) -> Rc<crate::budget::CacheBudget> {
        self.io().cache_budget.clone()
    }

    /// Cache memory: budget, bytes in use (as of the end of the last call),
    /// entries and bytes per cache, and eviction counters. Also returned
    /// under `cache` by the `stats` call.
    pub fn cache_stats(&self) -> Value {
        self.io().cache_budget.stats_json()
    }
}

async fn load_manifest(io: &io::Io, base: &str, name: Option<&str>) -> Result<Value> {
    let found = match name {
        Some(name) => io.fetch_json_optional(&resolve_url(base, name)).await?,
        None => match io.fetch_json_optional(&resolve_url(base, "manifest.min.json")).await? {
            Some(manifest) => Some(manifest),
            None => io.fetch_json_optional(&resolve_url(base, "manifest.json")).await?,
        },
    };
    found.ok_or_else(|| Error::new("Rangefind manifest could not be loaded."))
}
