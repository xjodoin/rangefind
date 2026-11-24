//! The engine object the OSM layer drives. `query.js` takes any object with
//! `search`/`suggest`/`authorityLookup`/... methods (a `createSearch()`
//! engine, a federation, or a test double) and feature-tests them with
//! `typeof engine.x === "function"`. [`OsmEngine`] is that contract; it is
//! implemented for [`Engine`] and can be implemented by hosts and tests.
//!
//! The JS module keeps several `WeakMap` caches keyed by engine identity
//! (category lexicon, root-authority lookups, resolved localities).
//! [`OsmCaches`] holds them; every [`OsmEngine`] owns one.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use serde_json::Value;

use crate::engine::Engine;
use crate::error::Result;
use crate::budget::{json_cost, CacheBudget, CacheCost};
use crate::rt::{Promise, PromiseCache};

use super::deps::CategoryLexicon;
use super::js::{at, js_string, Obj};

pub type LocalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// The engine methods `query.js` calls. Method names and parameter shapes
/// are the Rust engine's `call` protocol:
///
/// | JS | `call(method, params)` |
/// | --- | --- |
/// | `search(p)` / `suggest(p)` | `"search"` / `"suggest"`, `p` |
/// | `authorityLookup(surface, { size })` | `"authorityLookup"`, `{ surface, size }` |
/// | `loadFacetValues(field)` | `"loadFacetValues"`, `{ field }` |
/// | `hydrateRows(rows, context)` | `"hydrateRows"`, `{ rows, context }` |
/// | `prefetchShards(names, { docValues })` | `"prefetchShards"`, `{ names, docValues }` |
pub trait OsmEngine {
    /// `engine.manifest` (only `shards`, `features`, `facets` and
    /// `category_lexicon` are read).
    fn manifest(&self) -> Rc<Value>;
    /// `typeof engine[method] === "function"`.
    fn has(&self, method: &str) -> bool;
    fn call<'a>(&'a self, method: &'a str, params: Value) -> LocalFuture<'a, Result<Value>>;
    /// The per-engine caches (the JS `WeakMap`s keyed by this engine).
    fn caches(&self) -> Rc<OsmCaches>;
    /// Start a search now and let it run in the background (the JS
    /// un-awaited speculative promise). `None` runs it lazily on first use.
    fn start_search(&self, params: Value) -> Option<Promise<Value>> {
        let _ = params;
        None
    }
}

/// `exactAuthorityFromLookup(...)`.
#[derive(Clone, Debug, PartialEq)]
pub struct ExactAuthority {
    pub shards: Vec<String>,
    pub text: Value,
    pub count: f64,
}

/// A resolved locality document plus the (non-enumerable in JS) stats of
/// the search that found it.
#[derive(Clone, Debug)]
pub struct Locality {
    pub doc: Obj,
    pub search_stats: Option<Value>,
}

impl Locality {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.doc.get(key)
    }

    pub fn trace(&self) -> Option<&Value> {
        self.search_stats.as_ref().and_then(|stats| stats.get("trace"))
    }
}

impl CacheCost for ExactAuthority {
    fn cache_cost(&self) -> usize {
        std::mem::size_of::<ExactAuthority>() + self.shards.iter().map(|shard| 24 + shard.len()).sum::<usize>() + json_cost(&self.text)
    }
}

/// The JS module-level `WeakMap` caches for one engine. The per-surface
/// caches are keyed by query text, so on a long-lived engine they join the
/// engine's cache budget ([`OsmCaches::with_budget`]); evicting one only
/// means the next query with that surface asks the engine again.
#[derive(Default)]
pub struct OsmCaches {
    pub(crate) category_lexicon: RefCell<Option<Promise<Rc<CategoryLexicon>>>>,
    pub(crate) authority_exact: PromiseCache<String, Option<ExactAuthority>>,
    pub(crate) authority_lookup: PromiseCache<String, Value>,
    pub(crate) locality: PromiseCache<String, Option<Obj>>,
    /// Manifest-derived data for the `Engine` adapter (the manifest of a
    /// live engine never changes the fields the OSM layer reads).
    pub(crate) manifest: RefCell<Option<Rc<Value>>>,
    /// The engine's Google Maps adapter (`osm.maps.*`), type-erased.
    pub(crate) maps_adapter: RefCell<Option<Rc<dyn std::any::Any>>>,
}

impl OsmCaches {
    /// Unbounded caches.
    pub fn new() -> Self {
        Self::default()
    }

    /// Caches whose per-surface entries share `budget` (an engine's
    /// `cacheBudgetBytes`).
    pub fn with_budget(budget: &Rc<CacheBudget>) -> Self {
        OsmCaches {
            authority_exact: PromiseCache::budgeted(budget, "osmExactAuthority"),
            authority_lookup: PromiseCache::budgeted(budget, "osmAuthorityLookups"),
            locality: PromiseCache::budgeted(budget, "osmLocalities"),
            ..Self::default()
        }
    }
}

// ---------------------------------------------------------- Engine adapter

const CACHE_EXTENSION: &str = "rangefind.osm.queryCaches";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Single,
    Generational,
    Sharded,
}

fn kind_of(engine: &Engine, manifest: &Value) -> Kind {
    if engine.single().is_some() {
        Kind::Single
    } else if manifest.get("shards").map(Value::is_array).unwrap_or(false) {
        Kind::Sharded
    } else {
        Kind::Generational
    }
}

/// Per-engine caches for [`Engine`]. Single indexes keep them in the index
/// engine's extension map, so every clone of the handle shares them, as the
/// JS `WeakMap` does. Federation handles expose no extension slot yet, so
/// their caches live for one top-level OSM call.
pub fn engine_caches(engine: &Engine) -> Rc<OsmCaches> {
    // Engine-scoped, so single, generational and sharded handles all keep
    // warm caches across calls (the JS WeakMaps keyed by engine).
    if let Some(caches) = engine.extension::<OsmCaches>(CACHE_EXTENSION) {
        return caches;
    }
    let caches = Rc::new(OsmCaches::with_budget(&engine.cache_budget()));
    engine.set_extension(CACHE_EXTENSION, caches.clone());
    caches
}

/// [`OsmEngine`] over the Rust engine. Build one per top-level call (cheap)
/// or keep it alongside the engine.
pub struct EngineHost<'e> {
    pub engine: &'e Engine,
    caches: Rc<OsmCaches>,
    manifest: Rc<Value>,
    kind: Kind,
}

impl<'e> EngineHost<'e> {
    pub fn new(engine: &'e Engine) -> Self {
        Self::with_caches(engine, engine_caches(engine))
    }

    /// Use caller-owned caches (keep one [`OsmCaches`] per engine to get the
    /// JS `WeakMap` lifetime on handles that have no extension slot).
    pub fn with_caches(engine: &'e Engine, caches: Rc<OsmCaches>) -> Self {
        let cached = caches.manifest.borrow().clone();
        let manifest = match cached {
            Some(manifest) => manifest,
            None => {
                let manifest = Rc::new(engine.manifest());
                *caches.manifest.borrow_mut() = Some(manifest.clone());
                manifest
            }
        };
        let kind = kind_of(engine, &manifest);
        EngineHost { engine, caches, manifest, kind }
    }
}

impl OsmEngine for EngineHost<'_> {
    fn manifest(&self) -> Rc<Value> {
        self.manifest.clone()
    }

    fn has(&self, method: &str) -> bool {
        let kind = self.kind;
        match method {
            "search" | "suggest" | "count" | "hydrateRows" | "loadFacetValues" | "vectorSearch" => true,
            "authorityLookup" => kind != Kind::Generational,
            "prefetchShards" => kind == Kind::Sharded,
            _ => false,
        }
    }

    fn call<'a>(&'a self, method: &'a str, params: Value) -> LocalFuture<'a, Result<Value>> {
        Box::pin(async move {
            if method == "prefetchShards" && self.kind != Kind::Sharded {
                return Ok(Value::Null);
            }
            self.engine.call(method, params).await
        })
    }

    fn caches(&self) -> Rc<OsmCaches> {
        self.caches.clone()
    }

    fn start_search(&self, params: Value) -> Option<Promise<Value>> {
        let engine = self.engine.clone();
        Some(self.engine.runtime().start(async move { engine.call("search", params).await }))
    }
}

// ------------------------------------------------------------ call context

/// What one OSM call needs: the engine, its caches and its manifest.
pub struct Ctx<'a> {
    pub engine: &'a dyn OsmEngine,
    pub caches: Rc<OsmCaches>,
    pub manifest: Rc<Value>,
}

impl<'a> Ctx<'a> {
    pub fn new(engine: &'a dyn OsmEngine) -> Self {
        Ctx { engine, caches: engine.caches(), manifest: engine.manifest() }
    }

    pub fn has(&self, method: &str) -> bool {
        self.engine.has(method)
    }

    pub async fn search(&self, params: Value) -> Result<Value> {
        let response = self.engine.call("search", params).await?;
        Ok(normalize_response(response))
    }

    pub async fn suggest(&self, params: Value) -> Result<Value> {
        let response = self.engine.call("suggest", params).await?;
        Ok(normalize_response(response))
    }

    /// A search started now and awaited later.
    pub fn start_search(&self, params: Value) -> LocalFuture<'a, Result<Value>> {
        match self.engine.start_search(params.clone()) {
            Some(promise) => Box::pin(async move { promise.await.map(normalize_response) }),
            None => {
                let engine = self.engine;
                Box::pin(async move { engine.call("search", params).await.map(normalize_response) })
            }
        }
    }

    /// `engine.authorityLookup(surface, { size: 32 })`.
    pub async fn authority_lookup(&self, surface: &str) -> Result<Value> {
        let mut params = Obj::new();
        params.insert("surface".into(), Value::String(surface.to_string()));
        params.insert("size".into(), Value::from(32));
        self.engine.call("authorityLookup", Value::Object(params)).await
    }

    /// `engine.prefetchShards?.(names, options)`.
    pub fn prefetch_shards(&self, names: &[String], doc_values: bool) {
        if !self.has("prefetchShards") {
            return;
        }
        let mut params = Obj::new();
        params.insert("names".into(), Value::from(names.to_vec()));
        if doc_values {
            params.insert("docValues".into(), Value::Bool(true));
        }
        // Fire-and-forget: the call itself only schedules warm-up work.
        let future = self.engine.call("prefetchShards", Value::Object(params));
        drop(poll_once(future));
    }

    pub fn shard_descriptors(&self) -> Vec<&Value> {
        self.manifest.get("shards").and_then(Value::as_array).map(|items| items.iter().collect()).unwrap_or_default()
    }

    pub fn manifest_features(&self) -> &Value {
        at(&self.manifest, "features")
    }
}

/// Engines answer search/suggest with an object; anything else is treated
/// as an empty object, like the JS `response.results || []` reads.
fn normalize_response(response: Value) -> Value {
    if response.is_object() {
        response
    } else {
        Value::Object(Obj::new())
    }
}

/// Poll a future once (the prefetch call completes synchronously after
/// spawning its warm-up tasks on the engine runtime).
fn poll_once<T>(mut future: LocalFuture<'_, T>) -> Option<T> {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn noop_raw() -> RawWaker {
        fn clone(_: *const ()) -> RawWaker {
            noop_raw()
        }
        fn noop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    let waker = unsafe { Waker::from_raw(noop_raw()) };
    let mut cx = Context::from_waker(&waker);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(value) => Some(value),
        Poll::Pending => None,
    }
}

/// `String(shard.id)`.
pub fn shard_id(shard: &Value) -> String {
    js_string(shard.get("id"))
}
