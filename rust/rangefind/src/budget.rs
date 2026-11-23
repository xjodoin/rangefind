//! Byte-budgeted LRU accounting for an engine's decoded-object caches.
//!
//! Every [`crate::rt::PromiseCache`] built with
//! [`crate::rt::PromiseCache::budgeted`] registers with one shared
//! [`CacheBudget`] (one per opened engine, federation children included:
//! they share the parent's I/O layer and therefore its budget). The budget
//! keeps a global use clock; each cache slot remembers the tick of its last
//! use and, once its promise settles, an estimate of the bytes it holds
//! ([`CacheCost`]).
//!
//! # What may be evicted
//!
//! Lanes routinely load objects and then read them back synchronously
//! (`ensure_doc_values_for_docs(..).await` followed by `doc_value(..)`,
//! which reads the cache with `ready`). An eviction between the two would
//! silently read "missing". So eviction is restricted to entries that no
//! running call can depend on:
//!
//! * only settled entries are evicted (in-flight promises never are);
//! * every top-level call runs inside a [`CacheScope`] that records the use
//!   clock when it started, and an entry is evictable only if its last use
//!   is older than the oldest active scope. Every cache access (`get`,
//!   `has`, `ready`, `reserve`, `set`) counts as a use, so anything a running
//!   call has touched stays put until that call finishes.
//!
//! Evicting only drops the cache's reference: a value some caller still
//! holds (an `Rc`) stays alive until that caller lets go.
//!
//! # When eviction runs
//!
//! When a scope ends. Accounting is incremental (only caches that gained
//! entries since the last check are visited), so the common case — nothing
//! new, under budget — is O(1). Over budget, the least recently used
//! evictable entries across all caches are dropped until usage is back
//! under [`TRIM_TARGET_NUM`]/[`TRIM_TARGET_DEN`] of the budget (hysteresis,
//! so the O(entries) trim pass is amortised over many calls).
//!
//! Usage can exceed the budget while calls run: their working set is
//! protected. The budget bounds what is retained *between* calls.

use std::cell::{Cell, RefCell};
use std::rc::{Rc, Weak};

use serde_json::Value;

/// Default budget (`cacheBudgetBytes` option): 256 MiB.
pub const DEFAULT_CACHE_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// Fixed per-entry bookkeeping estimate (hash slot, key, promise cell).
pub const ENTRY_OVERHEAD_BYTES: usize = 96;

/// A trim stops once usage is at or below this fraction of the budget.
pub const TRIM_TARGET_NUM: usize = 4;
pub const TRIM_TARGET_DEN: usize = 5;

/// Approximate heap bytes a cached value keeps alive.
///
/// Estimates should be cheap (lengths of the vectors and buffers the value
/// owns, not a deep walk where avoidable) and err on the high side.
pub trait CacheCost {
    /// Size when the value is first accounted.
    fn cache_cost(&self) -> usize;

    /// Values that grow after being cached (posting segments memoise
    /// decoded blocks in their term entries) set this, and report the
    /// growth through [`CacheCost::dynamic_cost`], re-read on every use.
    const DYNAMIC: bool = false;

    /// Bytes added since the value was built (only read when `DYNAMIC`).
    fn dynamic_cost(&self) -> usize {
        0
    }
}

/// The per-cache side of the budget (implemented by `PromiseCache`'s core).
pub(crate) trait Tracked {
    /// Account entries that settled since the cache was queued as dirty.
    /// Returns true while some are still in flight: the cache then stays
    /// flagged dirty and the budget keeps it queued; otherwise the flag is
    /// cleared so the next new entry queues it again. Lowers `oldest` to the
    /// last-use tick of every entry it accounts.
    fn account(&self, oldest: &mut u64) -> bool;
    /// Re-read dynamic costs of every accounted entry.
    fn remeasure(&self);
    /// Push `(last use, cost)` of every evictable entry used before
    /// `horizon`.
    fn candidates(&self, horizon: u64, out: &mut Vec<(u64, usize)>);
    /// Drop every evictable entry last used before `cutoff`.
    fn evict_before(&self, cutoff: u64) -> (usize, usize);
    fn usage(&self) -> (&'static str, usize, usize);
}

/// Shared budget for one engine's caches.
pub struct CacheBudget {
    limit: usize,
    used: Cell<usize>,
    clock: Cell<u64>,
    scopes: RefCell<Vec<u64>>,
    caches: RefCell<Vec<Weak<dyn Tracked>>>,
    dirty: RefCell<Vec<Weak<dyn Tracked>>>,
    busy: Cell<bool>,
    /// Horizon of the last trim that could not get back under budget: until
    /// the oldest scope ends nothing more is evictable, so trims are skipped.
    stuck_horizon: Cell<u64>,
    trims: Cell<u64>,
    evicted_entries: Cell<u64>,
    evicted_bytes: Cell<u64>,
}

impl CacheBudget {
    /// `limit` bytes; 0 disables the budget (caches are unbounded and pay
    /// no bookkeeping).
    pub fn new(limit: usize) -> Rc<CacheBudget> {
        Rc::new(CacheBudget {
            limit,
            used: Cell::new(0),
            clock: Cell::new(0),
            scopes: RefCell::new(Vec::new()),
            caches: RefCell::new(Vec::new()),
            dirty: RefCell::new(Vec::new()),
            busy: Cell::new(false),
            stuck_horizon: Cell::new(0),
            trims: Cell::new(0),
            evicted_entries: Cell::new(0),
            evicted_bytes: Cell::new(0),
        })
    }

    pub fn unbounded() -> Rc<CacheBudget> {
        CacheBudget::new(0)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn is_bounded(&self) -> bool {
        self.limit > 0
    }

    /// Bytes currently accounted to settled entries.
    pub fn used(&self) -> usize {
        self.used.get()
    }

    /// Advance the use clock and return the new tick.
    pub(crate) fn touch(&self) -> u64 {
        let tick = self.clock.get() + 1;
        self.clock.set(tick);
        tick
    }

    pub(crate) fn charge(&self, bytes: usize) {
        self.used.set(self.used.get().saturating_add(bytes));
    }

    pub(crate) fn release(&self, bytes: usize) {
        self.used.set(self.used.get().saturating_sub(bytes));
    }

    pub(crate) fn register(&self, cache: Weak<dyn Tracked>) {
        let mut caches = self.caches.borrow_mut();
        // Drop registrations of caches that no longer exist (child engines
        // or lane states that were released).
        if caches.len() >= 64 && caches.len().is_power_of_two() {
            caches.retain(|cache| cache.strong_count() > 0);
        }
        caches.push(cache);
    }

    pub(crate) fn mark_dirty(&self, cache: Weak<dyn Tracked>) {
        self.dirty.borrow_mut().push(cache);
    }

    /// Start a call: everything it uses stays cached until it ends.
    pub fn enter(self: &Rc<Self>) -> CacheScope {
        if !self.is_bounded() {
            return CacheScope { budget: None, start: 0 };
        }
        let start = self.clock.get();
        self.scopes.borrow_mut().push(start);
        CacheScope { budget: Some(self.clone()), start }
    }

    /// Entries last used before this tick are not protected by any scope.
    fn horizon(&self) -> u64 {
        self.scopes.borrow().iter().min().map(|start| start + 1).unwrap_or(u64::MAX)
    }

    fn live_caches(&self) -> Vec<Rc<dyn Tracked>> {
        let mut caches = self.caches.borrow_mut();
        caches.retain(|cache| cache.strong_count() > 0);
        caches.iter().filter_map(Weak::upgrade).collect()
    }

    /// Account newly settled entries and, over budget, evict least recently
    /// used unprotected entries.
    pub fn maybe_trim(&self) {
        if !self.is_bounded() || self.busy.replace(true) {
            return;
        }
        let dirty = std::mem::take(&mut *self.dirty.borrow_mut());
        let mut still_dirty = Vec::new();
        let mut oldest = u64::MAX;
        for weak in dirty {
            let Some(cache) = weak.upgrade() else { continue };
            if cache.account(&mut oldest) {
                still_dirty.push(weak);
            }
        }
        self.dirty.borrow_mut().extend(still_dirty);
        // Newly accounted entries older than a stuck horizon are evictable.
        if oldest < self.stuck_horizon.get() {
            self.stuck_horizon.set(0);
        }
        if self.used.get() > self.limit {
            self.trim();
        }
        self.busy.set(false);
    }

    fn trim(&self) {
        let horizon = self.horizon();
        if horizon == self.stuck_horizon.get() {
            return;
        }
        let caches = self.live_caches();
        for cache in &caches {
            cache.remeasure();
        }
        let used = self.used.get();
        if used <= self.limit {
            return;
        }
        let target = self.limit / TRIM_TARGET_DEN * TRIM_TARGET_NUM;
        let need = used - target.min(used);
        let mut candidates = Vec::new();
        for cache in &caches {
            cache.candidates(horizon, &mut candidates);
        }
        if candidates.is_empty() {
            self.stuck_horizon.set(horizon);
            return;
        }
        candidates.sort_unstable_by_key(|(tick, _)| *tick);
        let mut freed = 0usize;
        let mut cutoff = 0u64;
        for (tick, cost) in candidates {
            if freed >= need {
                break;
            }
            freed = freed.saturating_add(cost);
            cutoff = tick + 1;
        }
        let (mut entries, mut bytes) = (0usize, 0usize);
        for cache in &caches {
            let (count, size) = cache.evict_before(cutoff);
            entries += count;
            bytes += size;
        }
        self.stuck_horizon.set(if self.used.get() > self.limit { horizon } else { 0 });
        self.trims.set(self.trims.get() + 1);
        self.evicted_entries.set(self.evicted_entries.get() + entries as u64);
        self.evicted_bytes.set(self.evicted_bytes.get() + bytes as u64);
    }

    /// `{ budgetBytes, usedBytes, entries, activeCalls, trims,
    /// evictedEntries, evictedBytes, caches: { name: { entries, bytes } } }`.
    /// Usage is as of the last accounting pass (the end of the last call);
    /// `activeCalls` counts running calls (including the one asking).
    pub fn stats_json(&self) -> Value {
        let mut by_name: Vec<(&'static str, usize, usize)> = Vec::new();
        for cache in self.live_caches() {
            let (name, entries, bytes) = cache.usage();
            match by_name.iter_mut().find(|item| item.0 == name) {
                Some(item) => {
                    item.1 += entries;
                    item.2 += bytes;
                }
                None => by_name.push((name, entries, bytes)),
            }
        }
        by_name.sort_by(|a, b| b.2.cmp(&a.2).then(a.0.cmp(b.0)));
        let entries: usize = by_name.iter().map(|item| item.1).sum();
        let mut caches = serde_json::Map::new();
        for (name, count, bytes) in by_name {
            caches.insert(name.to_string(), serde_json::json!({ "entries": count, "bytes": bytes }));
        }
        serde_json::json!({
            "budgetBytes": self.limit,
            "usedBytes": self.used.get(),
            "entries": entries,
            "activeCalls": self.scopes.borrow().len(),
            "trims": self.trims.get(),
            "evictedEntries": self.evicted_entries.get(),
            "evictedBytes": self.evicted_bytes.get(),
            "caches": Value::Object(caches),
        })
    }
}

/// A running call (see the module docs). Ending it may trim the caches.
pub struct CacheScope {
    budget: Option<Rc<CacheBudget>>,
    start: u64,
}

impl Drop for CacheScope {
    fn drop(&mut self) {
        let Some(budget) = self.budget.take() else { return };
        {
            let mut scopes = budget.scopes.borrow_mut();
            if let Some(position) = scopes.iter().position(|start| *start == self.start) {
                scopes.swap_remove(position);
            }
        }
        budget.maybe_trim();
    }
}

// ------------------------------------------------------------ generic costs

impl<T: CacheCost + ?Sized> CacheCost for Rc<T> {
    const DYNAMIC: bool = T::DYNAMIC;
    fn cache_cost(&self) -> usize {
        (**self).cache_cost()
    }
    fn dynamic_cost(&self) -> usize {
        (**self).dynamic_cost()
    }
}

impl<T: CacheCost> CacheCost for Option<T> {
    const DYNAMIC: bool = T::DYNAMIC;
    fn cache_cost(&self) -> usize {
        self.as_ref().map(CacheCost::cache_cost).unwrap_or(0)
    }
    fn dynamic_cost(&self) -> usize {
        self.as_ref().map(CacheCost::dynamic_cost).unwrap_or(0)
    }
}

impl<A: CacheCost, B: CacheCost> CacheCost for (A, B) {
    const DYNAMIC: bool = A::DYNAMIC || B::DYNAMIC;
    fn cache_cost(&self) -> usize {
        self.0.cache_cost() + self.1.cache_cost()
    }
    fn dynamic_cost(&self) -> usize {
        self.0.dynamic_cost() + self.1.dynamic_cost()
    }
}

macro_rules! plain_cost {
    ($($ty:ty),*) => {
        $(impl CacheCost for $ty {
            fn cache_cost(&self) -> usize {
                std::mem::size_of::<$ty>()
            }
        })*
    };
}
plain_cost!(bool, u8, u32, u64, i32, i64, usize, f32, f64);

macro_rules! vec_of_plain_cost {
    ($($ty:ty),*) => {
        $(impl CacheCost for Vec<$ty> {
            fn cache_cost(&self) -> usize {
                std::mem::size_of::<Vec<$ty>>() + self.capacity() * std::mem::size_of::<$ty>()
            }
        })*
    };
}
vec_of_plain_cost!(u8, u16, u32, u64, i32, i64, f32, f64);

impl CacheCost for String {
    fn cache_cost(&self) -> usize {
        std::mem::size_of::<String>() + self.capacity()
    }
}

impl CacheCost for str {
    fn cache_cost(&self) -> usize {
        self.len()
    }
}

impl CacheCost for crate::util::bytes::Bytes {
    fn cache_cost(&self) -> usize {
        // The backing buffer, which a slice keeps alive whole.
        self.backing_len().max(self.len()) + 32
    }
}

impl CacheCost for crate::pointer::Pointer {
    fn cache_cost(&self) -> usize {
        std::mem::size_of::<crate::pointer::Pointer>() + 48
    }
}

/// Approximate heap size of a JSON value (one walk; JSON caches hold doc
/// payloads, which are small).
pub fn json_cost(value: &Value) -> usize {
    const NODE: usize = std::mem::size_of::<Value>();
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => NODE,
        Value::String(text) => NODE + text.len(),
        Value::Array(items) => NODE + items.iter().map(json_cost).sum::<usize>(),
        // preserve_order maps: entries vector plus hash index.
        Value::Object(map) => NODE + map.iter().map(|(key, item)| key.len() + 48 + json_cost(item)).sum::<usize>(),
    }
}

impl CacheCost for Value {
    fn cache_cost(&self) -> usize {
        json_cost(self)
    }
}

impl CacheCost for serde_json::Map<String, Value> {
    fn cache_cost(&self) -> usize {
        std::mem::size_of::<serde_json::Map<String, Value>>() + self.iter().map(|(key, item)| key.len() + 48 + json_cost(item)).sum::<usize>()
    }
}

impl CacheCost for Vec<Value> {
    fn cache_cost(&self) -> usize {
        std::mem::size_of::<Vec<Value>>() + self.iter().map(json_cost).sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rt::{Promise, PromiseCache};

    /// A value whose cost is its payload; `grown` models interior caches.
    #[derive(Clone)]
    struct Blob {
        size: usize,
        grown: Rc<Cell<usize>>,
    }

    fn blob(size: usize) -> Blob {
        Blob { size, grown: Rc::new(Cell::new(0)) }
    }

    impl CacheCost for Blob {
        fn cache_cost(&self) -> usize {
            self.size
        }
        const DYNAMIC: bool = true;
        fn dynamic_cost(&self) -> usize {
            self.grown.get()
        }
    }

    const E: usize = 100 + ENTRY_OVERHEAD_BYTES;

    fn put(cache: &PromiseCache<&'static str, Blob>, key: &'static str) {
        cache.set(key, Promise::resolved(blob(100)));
    }

    #[test]
    fn accounts_settled_entries_and_releases_removed_ones() {
        let budget = CacheBudget::new(1 << 20);
        let cache: PromiseCache<&'static str, Blob> = PromiseCache::budgeted(&budget, "blobs");
        put(&cache, "a");
        put(&cache, "b");
        let pending = cache.reserve("c");
        budget.maybe_trim();
        assert_eq!(budget.used(), 2 * E);
        pending.resolve(blob(100));
        budget.maybe_trim();
        assert_eq!(budget.used(), 3 * E);
        cache.delete(&"a");
        assert_eq!(budget.used(), 2 * E);
        let b = cache.get(&"b").unwrap();
        assert!(cache.evict_if_same(&"b", &b));
        assert_eq!(budget.used(), E);
        put(&cache, "c"); // replaces the accounted "c"
        assert_eq!(budget.used(), 0);
        budget.maybe_trim();
        assert_eq!(budget.used(), E);
        cache.clear();
        assert_eq!(budget.used(), 0);
        assert_eq!(budget.stats_json()["caches"]["blobs"]["bytes"], 0);
    }

    #[test]
    fn evicts_least_recently_used_across_caches_down_to_the_target() {
        // Room for three entries; trims go down to 80%.
        let budget = CacheBudget::new(3 * E);
        let first: PromiseCache<&'static str, Blob> = PromiseCache::budgeted(&budget, "first");
        let second: PromiseCache<&'static str, Blob> = PromiseCache::budgeted(&budget, "second");
        put(&first, "a");
        put(&second, "b");
        put(&first, "c");
        assert!(first.ready(&"a").is_some()); // "a" is now the most recent of the three
        put(&second, "d");
        budget.maybe_trim();
        // 4 entries > 3: evict "b" then "c" (least recently used) to reach 2.4.
        assert!(first.has(&"a"));
        assert!(!second.has(&"b"));
        assert!(!first.has(&"c"));
        assert!(second.has(&"d"));
        assert_eq!(budget.used(), 2 * E);
        let stats = budget.stats_json();
        assert_eq!(stats["evictedEntries"], 2);
        assert_eq!(stats["evictedBytes"], 2 * E);
        assert_eq!(stats["caches"]["first"]["entries"], 1);
    }

    #[test]
    fn never_evicts_in_flight_loads() {
        let budget = CacheBudget::new(1);
        let cache: PromiseCache<&'static str, Blob> = PromiseCache::budgeted(&budget, "blobs");
        let pending = cache.reserve("loading");
        put(&cache, "done");
        budget.maybe_trim();
        assert!(cache.has(&"loading"));
        assert!(!cache.has(&"done"));
        // Once it settles it is an ordinary entry again.
        pending.resolve(blob(100));
        budget.maybe_trim();
        assert!(!cache.has(&"loading"));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn running_scopes_protect_everything_they_touched() {
        let budget = CacheBudget::new(1);
        let cache: PromiseCache<&'static str, Blob> = PromiseCache::budgeted(&budget, "blobs");
        put(&cache, "old");
        let outer = budget.enter();
        put(&cache, "loaded");
        {
            // A nested call ends: only what no running call used may go.
            let _inner = budget.enter();
            assert!(cache.ready(&"loaded").is_some());
        }
        assert!(!cache.has(&"old"));
        assert!(cache.ready(&"loaded").is_some());
        // A call that started earlier and touched an older entry protects it
        // too, even though a later call ends first.
        put(&cache, "shared");
        let later = budget.enter();
        drop(later);
        assert!(cache.has(&"shared"));
        drop(outer);
        assert!(!cache.has(&"loaded"));
        assert!(!cache.has(&"shared"));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn entries_used_before_a_scope_started_are_not_protected_by_it() {
        let budget = CacheBudget::new(E);
        let cache: PromiseCache<&'static str, Blob> = PromiseCache::budgeted(&budget, "blobs");
        put(&cache, "stale");
        budget.maybe_trim();
        let scope = budget.enter();
        put(&cache, "fresh");
        {
            let _nested = budget.enter();
        }
        // Over budget: "stale" predates the running scope, "fresh" does not.
        assert!(!cache.has(&"stale"));
        assert!(cache.has(&"fresh"));
        drop(scope);
    }

    #[test]
    fn dynamic_costs_follow_growth() {
        let budget = CacheBudget::new(1 << 20);
        let cache: PromiseCache<&'static str, Blob> = PromiseCache::budgeted(&budget, "blobs");
        let value = blob(100);
        cache.set("a", Promise::resolved(value.clone()));
        budget.maybe_trim();
        assert_eq!(budget.used(), E);
        value.grown.set(50);
        assert!(cache.has(&"a"));
        assert_eq!(budget.used(), E + 50);
        cache.delete(&"a");
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn a_trim_that_cannot_free_enough_does_not_rescan_until_something_changes() {
        let budget = CacheBudget::new(E);
        let cache: PromiseCache<&'static str, Blob> = PromiseCache::budgeted(&budget, "blobs");
        let scope = budget.enter();
        put(&cache, "a");
        put(&cache, "b");
        for _ in 0..3 {
            let _nested = budget.enter();
        }
        assert_eq!(budget.stats_json()["trims"], 0);
        assert_eq!(cache.len(), 2);
        drop(scope);
        assert_eq!(cache.len(), 0);
        // With no scope running, newly accounted entries are evictable again.
        put(&cache, "c");
        put(&cache, "d");
        budget.maybe_trim();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn unbounded_budgets_keep_everything_without_bookkeeping() {
        let budget = CacheBudget::unbounded();
        let cache: PromiseCache<&'static str, Blob> = PromiseCache::budgeted(&budget, "blobs");
        let _scope = budget.enter();
        for key in ["a", "b", "c"] {
            put(&cache, key);
        }
        budget.maybe_trim();
        assert_eq!(cache.len(), 3);
        assert_eq!(budget.used(), 0);
    }
}
