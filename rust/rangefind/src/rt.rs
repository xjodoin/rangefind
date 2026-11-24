//! Minimal single-threaded async runtime.
//!
//! The engine is written as ordinary `async fn`s but must run identically
//! on native targets (blocking file or HTTP I/O) and inside a WebAssembly
//! module whose host performs the fetches. Rather than depend on an
//! executor crate, the runtime here is deliberately tiny:
//!
//! * [`Promise`] is a clonable, settle-once cell with JavaScript promise
//!   semantics. Every cache in the engine stores promises, so concurrent
//!   lanes share one in-flight request exactly like the JS runtime does.
//! * [`Runtime`] polls a set of local tasks until none can make progress.
//!   Progress stalls only when a promise is waiting on the host, at which
//!   point control returns to the embedder (see `host.rs`).
//!
//! Nothing here is `Send`: one engine lives on one thread.

use std::cell::{Cell, RefCell};
use std::rc::Weak;
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::rc::Rc;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use crate::budget::{CacheBudget, CacheCost, Tracked, ENTRY_OVERHEAD_BYTES};
use crate::error::{Error, Result};

pub type LocalFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

enum State<T> {
    Pending(Vec<Waker>),
    Settled(Result<T>),
}

/// A settle-once shared value. Cloning shares the same cell.
pub struct Promise<T> {
    inner: Rc<RefCell<State<T>>>,
}

impl<T> Clone for Promise<T> {
    fn clone(&self) -> Self {
        Promise { inner: self.inner.clone() }
    }
}

impl<T: Clone> Promise<T> {
    pub fn pending() -> Self {
        Promise { inner: Rc::new(RefCell::new(State::Pending(Vec::new()))) }
    }

    pub fn resolved(value: T) -> Self {
        Promise { inner: Rc::new(RefCell::new(State::Settled(Ok(value)))) }
    }

    pub fn rejected(error: Error) -> Self {
        Promise { inner: Rc::new(RefCell::new(State::Settled(Err(error)))) }
    }

    pub fn settle(&self, result: Result<T>) {
        let wakers = {
            let mut state = self.inner.borrow_mut();
            match &mut *state {
                State::Settled(_) => return,
                State::Pending(wakers) => {
                    let wakers = std::mem::take(wakers);
                    *state = State::Settled(result);
                    wakers
                }
            }
        };
        for waker in wakers {
            waker.wake();
        }
    }

    pub fn resolve(&self, value: T) {
        self.settle(Ok(value));
    }

    pub fn reject(&self, error: Error) {
        self.settle(Err(error));
    }

    /// The settled result, if any, without waiting.
    pub fn peek(&self) -> Option<Result<T>> {
        match &*self.inner.borrow() {
            State::Settled(result) => Some(result.clone()),
            State::Pending(_) => None,
        }
    }

    /// The resolved value if (and only if) the promise already fulfilled.
    pub fn ready(&self) -> Option<T> {
        match &*self.inner.borrow() {
            State::Settled(Ok(value)) => Some(value.clone()),
            _ => None,
        }
    }

    /// Look at the settled result (or `None` while pending) without
    /// cloning it.
    pub fn inspect<R>(&self, f: impl FnOnce(Option<&Result<T>>) -> R) -> R {
        match &*self.inner.borrow() {
            State::Settled(result) => f(Some(result)),
            State::Pending(_) => f(None),
        }
    }

    pub fn is_settled(&self) -> bool {
        matches!(&*self.inner.borrow(), State::Settled(_))
    }

    pub fn same(&self, other: &Promise<T>) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<T: Clone> Future for Promise<T> {
    type Output = Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.inner.borrow_mut();
        match &mut *state {
            State::Settled(result) => Poll::Ready(result.clone()),
            State::Pending(wakers) => {
                if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                    wakers.push(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }
}

/// Message of the error a dropped (cancelled) load settles its promise with.
pub const CANCELLED_MESSAGE: &str = "Rangefind request was cancelled.";

fn cancelled() -> Error {
    Error::new(CANCELLED_MESSAGE)
}

/// Rejects a still-pending promise if the task driving it is dropped, so
/// waiters sharing the promise fail instead of hanging forever.
pub struct SettleGuard<T: Clone> {
    promise: Promise<T>,
}

impl<T: Clone> SettleGuard<T> {
    pub fn new(promise: &Promise<T>) -> Self {
        SettleGuard { promise: promise.clone() }
    }
}

impl<T: Clone> Drop for SettleGuard<T> {
    fn drop(&mut self) {
        if !self.promise.is_settled() {
            self.promise.reject(cancelled());
        }
    }
}

/// A keyed promise cache: the Rust shape of the JS runtime's
/// `Map<key, Promise>` caches. A failed load is evicted so a later query
/// can retry it.
///
/// [`PromiseCache::new`] is unbounded. [`PromiseCache::budgeted`] joins a
/// shared [`CacheBudget`]: settled entries are measured ([`CacheCost`]) and
/// the least recently used ones that no running call has touched are
/// evicted when the budget is exceeded (see `crate::budget`).
pub struct PromiseCache<K, V> {
    core: Rc<CacheCore<K, V>>,
}

struct Slot<V> {
    promise: Promise<V>,
    /// Budget clock tick of the last use.
    tick: Cell<u64>,
    /// Static cost, then the cost currently charged (0 until accounted).
    base: Cell<usize>,
    cost: Cell<usize>,
    accounted: Cell<bool>,
}

impl<V> Slot<V> {
    fn new(promise: Promise<V>, tick: u64) -> Self {
        Slot { promise, tick: Cell::new(tick), base: Cell::new(0), cost: Cell::new(0), accounted: Cell::new(false) }
    }
}

struct Meter<K, V> {
    budget: Rc<CacheBudget>,
    name: &'static str,
    me: Weak<dyn Tracked>,
    cost: fn(&V) -> usize,
    dynamic: Option<fn(&V) -> usize>,
    /// Keys inserted since the last accounting pass.
    pending: RefCell<Vec<K>>,
    dirty: Cell<bool>,
    bytes: Cell<usize>,
}

struct CacheCore<K, V> {
    map: RefCell<HashMap<K, Slot<V>>>,
    meter: Option<Meter<K, V>>,
}

impl<K: Eq + Hash + Clone, V: Clone> Default for PromiseCache<K, V> {
    fn default() -> Self {
        PromiseCache { core: Rc::new(CacheCore { map: RefCell::new(HashMap::new()), meter: None }) }
    }
}

impl<K, V: Clone> CacheCore<K, V> {
    /// Record a use of `slot` (and re-read a dynamic cost).
    fn touch(&self, slot: &Slot<V>) {
        let Some(meter) = &self.meter else { return };
        slot.tick.set(meter.budget.touch());
        if let Some(dynamic) = meter.dynamic {
            if slot.accounted.get() {
                let extra = slot.promise.inspect(|state| match state {
                    Some(Ok(value)) => dynamic(value),
                    _ => 0,
                });
                self.recharge(slot, slot.base.get() + extra);
            }
        }
    }

    fn recharge(&self, slot: &Slot<V>, cost: usize) {
        let Some(meter) = &self.meter else { return };
        let old = slot.cost.get();
        if cost == old {
            return;
        }
        slot.cost.set(cost);
        if cost > old {
            meter.bytes.set(meter.bytes.get() + (cost - old));
            meter.budget.charge(cost - old);
        } else {
            meter.bytes.set(meter.bytes.get().saturating_sub(old - cost));
            meter.budget.release(old - cost);
        }
    }

    /// Un-charge a slot leaving the map.
    fn released(&self, slot: &Slot<V>) {
        if let Some(meter) = &self.meter {
            if slot.accounted.get() {
                meter.bytes.set(meter.bytes.get().saturating_sub(slot.cost.get()));
                meter.budget.release(slot.cost.get());
            }
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> CacheCore<K, V> {
    /// Queue a new key for accounting.
    fn inserted(&self, key: &K) {
        let Some(meter) = &self.meter else { return };
        meter.pending.borrow_mut().push(key.clone());
        if !meter.dirty.replace(true) {
            meter.budget.mark_dirty(meter.me.clone());
        }
    }

    fn new_slot(&self, promise: Promise<V>) -> Slot<V> {
        let tick = self.meter.as_ref().map(|meter| meter.budget.touch()).unwrap_or(0);
        Slot::new(promise, tick)
    }
}

impl<K: Eq + Hash + Clone + 'static, V: Clone + 'static> Tracked for CacheCore<K, V> {
    fn account(&self, oldest: &mut u64) -> bool {
        let Some(meter) = &self.meter else { return false };
        let keys = std::mem::take(&mut *meter.pending.borrow_mut());
        let mut waiting = Vec::new();
        {
            let map = self.map.borrow();
            for key in keys {
                let Some(slot) = map.get(&key) else { continue };
                if slot.accounted.get() {
                    continue;
                }
                let measured = slot.promise.inspect(|state| match state {
                    None => None,
                    Some(Ok(value)) => {
                        let base = ENTRY_OVERHEAD_BYTES + (meter.cost)(value);
                        Some((base, base + meter.dynamic.map(|dynamic| dynamic(value)).unwrap_or(0)))
                    }
                    Some(Err(_)) => Some((ENTRY_OVERHEAD_BYTES, ENTRY_OVERHEAD_BYTES)),
                });
                match measured {
                    None => waiting.push(key),
                    Some((base, cost)) => {
                        slot.base.set(base);
                        slot.cost.set(cost);
                        slot.accounted.set(true);
                        meter.bytes.set(meter.bytes.get() + cost);
                        meter.budget.charge(cost);
                        *oldest = (*oldest).min(slot.tick.get());
                    }
                }
            }
        }
        if waiting.is_empty() && meter.pending.borrow().is_empty() {
            meter.dirty.set(false);
            false
        } else {
            meter.pending.borrow_mut().extend(waiting);
            true
        }
    }

    fn remeasure(&self) {
        let Some(meter) = &self.meter else { return };
        let Some(dynamic) = meter.dynamic else { return };
        let map = self.map.borrow();
        for slot in map.values() {
            if slot.accounted.get() {
                let extra = slot.promise.inspect(|state| match state {
                    Some(Ok(value)) => dynamic(value),
                    _ => 0,
                });
                self.recharge(slot, slot.base.get() + extra);
            }
        }
    }

    fn candidates(&self, horizon: u64, out: &mut Vec<(u64, usize)>) {
        for slot in self.map.borrow().values() {
            if slot.accounted.get() && slot.tick.get() < horizon {
                out.push((slot.tick.get(), slot.cost.get()));
            }
        }
    }

    fn evict_before(&self, cutoff: u64) -> (usize, usize) {
        let Some(meter) = &self.meter else { return (0, 0) };
        let mut evicted = Vec::new();
        let (mut count, mut bytes) = (0usize, 0usize);
        self.map.borrow_mut().retain(|_, slot| {
            // Accounted implies settled: in-flight loads are never evicted.
            if slot.accounted.get() && slot.tick.get() < cutoff {
                count += 1;
                bytes += slot.cost.get();
                evicted.push(slot.promise.clone());
                false
            } else {
                true
            }
        });
        meter.bytes.set(meter.bytes.get().saturating_sub(bytes));
        meter.budget.release(bytes);
        // Values are dropped here, outside the map borrow.
        drop(evicted);
        (count, bytes)
    }

    fn usage(&self) -> (&'static str, usize, usize) {
        let name = self.meter.as_ref().map(|meter| meter.name).unwrap_or("");
        let bytes = self.meter.as_ref().map(|meter| meter.bytes.get()).unwrap_or(0);
        (name, self.map.borrow().len(), bytes)
    }
}

impl<K: Eq + Hash + Clone + 'static, V: Clone + CacheCost + 'static> PromiseCache<K, V> {
    /// A cache that joins `budget` under `name` (for stats). With an
    /// unbounded budget this is a plain [`PromiseCache::new`].
    pub fn budgeted(budget: &Rc<CacheBudget>, name: &'static str) -> Self {
        if !budget.is_bounded() {
            return PromiseCache::new();
        }
        let core = Rc::new_cyclic(|me: &Weak<CacheCore<K, V>>| CacheCore {
            map: RefCell::new(HashMap::new()),
            meter: Some(Meter {
                budget: budget.clone(),
                name,
                me: me.clone() as Weak<dyn Tracked>,
                cost: |value: &V| value.cache_cost(),
                dynamic: if V::DYNAMIC { Some(|value: &V| value.dynamic_cost()) } else { None },
                pending: RefCell::new(Vec::new()),
                dirty: Cell::new(false),
                bytes: Cell::new(0),
            }),
        });
        let tracked: Weak<dyn Tracked> = Rc::downgrade(&core) as Weak<dyn Tracked>;
        budget.register(tracked);
        PromiseCache { core }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> PromiseCache<K, V> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &K) -> Option<Promise<V>> {
        let map = self.core.map.borrow();
        let slot = map.get(key)?;
        self.core.touch(slot);
        Some(slot.promise.clone())
    }

    pub fn has(&self, key: &K) -> bool {
        match self.core.map.borrow().get(key) {
            Some(slot) => {
                self.core.touch(slot);
                true
            }
            None => false,
        }
    }

    pub fn set(&self, key: K, promise: Promise<V>) {
        let slot = self.core.new_slot(promise);
        self.core.inserted(&key);
        let old = self.core.map.borrow_mut().insert(key, slot);
        if let Some(old) = old {
            self.core.released(&old);
        }
    }

    pub fn delete(&self, key: &K) {
        let old = self.core.map.borrow_mut().remove(key);
        if let Some(old) = old {
            self.core.released(&old);
        }
    }

    /// Remove `key` only while it still maps to `promise`, so a failed or
    /// cancelled load never evicts a newer load of the same key.
    pub fn evict_if_same(&self, key: &K, promise: &Promise<V>) -> bool {
        let removed = {
            let mut map = self.core.map.borrow_mut();
            if map.get(key).map(|current| current.promise.same(promise)).unwrap_or(false) {
                map.remove(key)
            } else {
                None
            }
        };
        match removed {
            Some(old) => {
                self.core.released(&old);
                true
            }
            None => false,
        }
    }

    pub fn len(&self) -> usize {
        self.core.map.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        let old = std::mem::take(&mut *self.core.map.borrow_mut());
        for slot in old.values() {
            self.core.released(slot);
        }
    }

    /// The already-fulfilled value for `key`, if present.
    pub fn ready(&self, key: &K) -> Option<V> {
        let map = self.core.map.borrow();
        let slot = map.get(key)?;
        self.core.touch(slot);
        slot.promise.ready()
    }

    /// Reserve `key` with a fresh pending promise. Pair it with a
    /// [`ReservationGuard`] so a dropped load does not leave it pending.
    pub fn reserve(&self, key: K) -> Promise<V> {
        let promise = Promise::pending();
        self.set(key, promise.clone());
        promise
    }

    /// Return the cached promise's value, or run `load` once and share it.
    pub async fn get_or_load<F>(&self, key: K, load: F) -> Result<V>
    where
        F: Future<Output = Result<V>>,
    {
        if let Some(existing) = self.get(&key) {
            return existing.await;
        }
        let promise = self.reserve(key.clone());
        let mut guard = ReservationGuard::new(self);
        guard.push(key.clone(), promise.clone());
        let result = load.await;
        if result.is_err() {
            self.evict_if_same(&key, &promise);
        }
        promise.settle(result.clone());
        drop(guard);
        result
    }
}

/// Keys reserved in a [`PromiseCache`] by an in-flight load. If the load is
/// dropped before settling them (its future was cancelled), every unsettled
/// promise is rejected and evicted, so waiters fail instead of hanging and
/// later queries reload instead of inheriting the cancellation.
pub struct ReservationGuard<'c, K: Eq + Hash + Clone, V: Clone> {
    cache: &'c PromiseCache<K, V>,
    entries: Vec<(K, Promise<V>)>,
}

impl<'c, K: Eq + Hash + Clone, V: Clone> ReservationGuard<'c, K, V> {
    pub fn new(cache: &'c PromiseCache<K, V>) -> Self {
        ReservationGuard { cache, entries: Vec::new() }
    }

    pub fn push(&mut self, key: K, promise: Promise<V>) {
        self.entries.push((key, promise));
    }

    pub fn entries(&self) -> &[(K, Promise<V>)] {
        &self.entries
    }

    /// Reject and evict every still-pending reservation with `error`.
    pub fn fail_pending(&self, error: &Error) {
        for (key, promise) in &self.entries {
            if !promise.is_settled() {
                self.cache.evict_if_same(key, promise);
                promise.reject(error.clone());
            }
        }
    }
}

impl<K: Eq + Hash + Clone, V: Clone> Drop for ReservationGuard<'_, K, V> {
    fn drop(&mut self) {
        self.fail_pending(&cancelled());
    }
}

/// A single lazily-loaded shared value (`xPromise` fields in the JS runtime).
pub struct PromiseSlot<V> {
    slot: RefCell<Option<Promise<V>>>,
}

impl<V: Clone> Default for PromiseSlot<V> {
    fn default() -> Self {
        PromiseSlot { slot: RefCell::new(None) }
    }
}

impl<V: Clone> PromiseSlot<V> {
    pub fn ready(&self) -> Option<V> {
        self.slot.borrow().as_ref().and_then(|promise| promise.ready())
    }

    pub fn clear(&self) {
        *self.slot.borrow_mut() = None;
    }

    pub async fn get_or_load<F>(&self, load: F, retry_on_error: bool) -> Result<V>
    where
        F: Future<Output = Result<V>>,
    {
        let existing = self.slot.borrow().clone();
        if let Some(existing) = existing {
            return existing.await;
        }
        let promise = Promise::pending();
        *self.slot.borrow_mut() = Some(promise.clone());
        let guard = SlotGuard { slot: self, promise: promise.clone() };
        let result = load.await;
        if result.is_err() && retry_on_error {
            self.clear_if_same(&promise);
        }
        promise.settle(result.clone());
        drop(guard);
        result
    }

    fn clear_if_same(&self, promise: &Promise<V>) {
        let mut slot = self.slot.borrow_mut();
        if slot.as_ref().map(|current| current.same(promise)).unwrap_or(false) {
            *slot = None;
        }
    }
}

/// A [`PromiseSlot`] load that is dropped mid-flight clears the slot (a
/// cancellation is never cached, whatever `retry_on_error` says) and
/// rejects the promise its waiters hold.
struct SlotGuard<'s, V: Clone> {
    slot: &'s PromiseSlot<V>,
    promise: Promise<V>,
}

impl<V: Clone> Drop for SlotGuard<'_, V> {
    fn drop(&mut self) {
        if !self.promise.is_settled() {
            self.slot.clear_if_same(&self.promise);
            self.promise.reject(cancelled());
        }
    }
}

/// `Promise.all` without short-circuit cancellation: every future runs to
/// completion and results come back in input order.
pub async fn join_all<'a, T: 'a, I>(futures: I) -> Vec<T>
where
    I: IntoIterator,
    I::Item: Future<Output = T> + 'a,
{
    JoinAll::new(futures.into_iter().map(|future| Box::pin(future) as LocalFuture<'a, T>).collect()).await
}

/// Children woken since the join was last polled. Each child gets its own
/// waker, so a wide fan-out whose host completes one request at a time
/// re-polls only the child that request unblocked (O(woken) per wake, not
/// O(children)).
struct JoinShared {
    woken: Mutex<Vec<usize>>,
    queued: Vec<AtomicBool>,
    parent: Mutex<Option<Waker>>,
}

struct ChildWaker {
    index: usize,
    shared: Arc<JoinShared>,
}

impl Wake for ChildWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if !self.shared.queued[self.index].swap(true, Ordering::AcqRel) {
            if let Ok(mut woken) = self.shared.woken.lock() {
                woken.push(self.index);
            }
        }
        let parent = self.shared.parent.lock().ok().and_then(|parent| parent.clone());
        if let Some(parent) = parent {
            parent.wake();
        }
    }
}

struct JoinAll<'a, T> {
    futures: Vec<Option<LocalFuture<'a, T>>>,
    values: Vec<Option<T>>,
    wakers: Vec<Waker>,
    shared: Arc<JoinShared>,
    remaining: usize,
    started: bool,
}

impl<'a, T> JoinAll<'a, T> {
    fn new(futures: Vec<LocalFuture<'a, T>>) -> Self {
        let count = futures.len();
        let shared = Arc::new(JoinShared {
            woken: Mutex::new(Vec::new()),
            queued: (0..count).map(|_| AtomicBool::new(false)).collect(),
            parent: Mutex::new(None),
        });
        let wakers = (0..count).map(|index| Waker::from(Arc::new(ChildWaker { index, shared: shared.clone() }))).collect();
        JoinAll {
            futures: futures.into_iter().map(Some).collect(),
            values: (0..count).map(|_| None).collect(),
            wakers,
            shared,
            remaining: count,
            started: false,
        }
    }

    fn poll_child(&mut self, index: usize) {
        let Some(future) = self.futures[index].as_mut() else { return };
        self.shared.queued[index].store(false, Ordering::Release);
        let mut cx = Context::from_waker(&self.wakers[index]);
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            self.values[index] = Some(value);
            self.futures[index] = None;
            self.remaining -= 1;
        }
    }
}

// The children are boxed and the values are never pinned, so moving the
// join is sound whatever `T` is.
impl<T> Unpin for JoinAll<'_, T> {}

impl<T> Future for JoinAll<'_, T> {
    type Output = Vec<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Vec<T>> {
        let this = self.get_mut();
        if let Ok(mut parent) = this.shared.parent.lock() {
            if !parent.as_ref().map(|waker| waker.will_wake(cx.waker())).unwrap_or(false) {
                *parent = Some(cx.waker().clone());
            }
        }
        if !this.started {
            this.started = true;
            for index in 0..this.futures.len() {
                this.poll_child(index);
            }
        } else {
            let woken = this.shared.woken.lock().map(|mut woken| std::mem::take(&mut *woken)).unwrap_or_default();
            for index in woken {
                this.poll_child(index);
            }
        }
        if this.remaining > 0 {
            return Poll::Pending;
        }
        Poll::Ready(this.values.iter_mut().map(|value| value.take().expect("join_all child settled")).collect())
    }
}

/// `Promise.all` over fallible futures: waits for all, returns the first
/// error (in input order) if any failed.
pub async fn try_join_all<'a, T: 'a, I>(futures: I) -> Result<Vec<T>>
where
    I: IntoIterator,
    I::Item: Future<Output = Result<T>> + 'a,
{
    join_all(futures).await.into_iter().collect()
}

/// Run two futures concurrently.
pub async fn join2<'a, A: 'a, B: 'a>(
    a: impl Future<Output = A> + 'a,
    b: impl Future<Output = B> + 'a,
) -> (A, B) {
    enum Either<A, B> {
        A(A),
        B(B),
    }
    let futures: Vec<LocalFuture<'a, Either<A, B>>> = vec![
        Box::pin(async move { Either::A(a.await) }),
        Box::pin(async move { Either::B(b.await) }),
    ];
    let mut out = join_all(futures).await.into_iter();
    match (out.next(), out.next()) {
        (Some(Either::A(a)), Some(Either::B(b))) => (a, b),
        _ => unreachable!("join2 preserves order"),
    }
}

/// Wakes exactly one task by queueing its slot id.
struct TaskWaker {
    id: usize,
    ready: Arc<Mutex<VecDeque<usize>>>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if let Ok(mut ready) = self.ready.lock() {
            ready.push_back(self.id);
        }
    }
}

type Task = LocalFuture<'static, ()>;

/// Local task set. `spawn` queues fire-and-forget work (the JS runtime's
/// un-awaited warm-up promises); `run_until_stalled` drives everything.
///
/// Tasks live in a slab and are polled only when their waker fires, so a
/// host that completes fetches one at a time costs O(woken tasks) per
/// completion instead of re-polling every pending task.
#[derive(Clone, Default)]
pub struct Runtime {
    inner: Rc<RuntimeInner>,
}

#[derive(Default)]
struct RuntimeInner {
    slots: RefCell<Vec<Option<(Task, Waker)>>>,
    free: RefCell<Vec<usize>>,
    incoming: RefCell<Vec<Task>>,
    ready: Arc<Mutex<VecDeque<usize>>>,
    live: Cell<usize>,
    running: Cell<bool>,
    driven: Cell<bool>,
}

impl Runtime {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.inner.incoming.borrow_mut().push(Box::pin(future));
    }

    /// Spawn a fallible warm-up whose outcome nobody observes.
    pub fn spawn_detached<T: 'static>(&self, future: impl Future<Output = Result<T>> + 'static) {
        self.spawn(async move {
            let _ = future.await;
        });
    }

    /// Whether anything has ever driven this runtime (a host session, a
    /// blocking call, or an embedder calling [`Runtime::run_until_stalled`]).
    /// Fire-and-forget warm-ups only make sense on a driven runtime: on one
    /// nobody polls, a spawned task would just sit there holding the engine.
    pub fn is_driven(&self) -> bool {
        self.inner.driven.get()
    }

    /// Drop every task (queued or waiting on the host) without running it.
    /// Their load guards reject and evict whatever they had reserved, and
    /// the engine references the futures held are released, so a host
    /// session freed with work in flight does not leak its engines through
    /// the runtime ⇄ task ⇄ engine cycle.
    pub fn shutdown(&self) {
        loop {
            let slots = std::mem::take(&mut *self.inner.slots.borrow_mut());
            let incoming = std::mem::take(&mut *self.inner.incoming.borrow_mut());
            self.inner.free.borrow_mut().clear();
            self.inner.live.set(0);
            let idle = incoming.is_empty() && slots.iter().all(Option::is_none);
            // Dropped outside every borrow: a task's guards may wake or even
            // spawn, which touches the runtime again.
            drop(slots);
            drop(incoming);
            if idle {
                break;
            }
        }
        if let Ok(mut ready) = self.inner.ready.lock() {
            ready.clear();
        }
    }

    pub fn pending_tasks(&self) -> usize {
        self.inner.live.get() + self.inner.incoming.borrow().len()
    }

    fn admit_incoming(&self) {
        let incoming = std::mem::take(&mut *self.inner.incoming.borrow_mut());
        for task in incoming {
            let id = match self.inner.free.borrow_mut().pop() {
                Some(id) => id,
                None => {
                    let mut slots = self.inner.slots.borrow_mut();
                    slots.push(None);
                    slots.len() - 1
                }
            };
            let waker = Waker::from(Arc::new(TaskWaker { id, ready: self.inner.ready.clone() }));
            self.inner.slots.borrow_mut()[id] = Some((task, waker));
            self.inner.live.set(self.inner.live.get() + 1);
            if let Ok(mut ready) = self.inner.ready.lock() {
                ready.push_back(id);
            }
        }
    }

    fn next_ready(&self) -> Option<usize> {
        self.inner.ready.lock().ok().and_then(|mut ready| ready.pop_front())
    }

    /// Poll woken tasks until none are ready. Returns the number of tasks
    /// still pending (waiting on the host).
    pub fn run_until_stalled(&self) -> usize {
        if self.inner.running.replace(true) {
            return self.pending_tasks();
        }
        self.inner.driven.set(true);
        loop {
            self.admit_incoming();
            let Some(id) = self.next_ready() else {
                if self.inner.incoming.borrow().is_empty() {
                    break;
                }
                continue;
            };
            // Take the task out so it can spawn or wake others while polled.
            let Some((mut task, waker)) = self.inner.slots.borrow_mut().get_mut(id).and_then(Option::take) else { continue };
            let mut cx = Context::from_waker(&waker);
            match task.as_mut().poll(&mut cx) {
                Poll::Ready(()) => {
                    // A `shutdown` from inside a task cleared the slab; the
                    // id is then no longer ours to recycle.
                    if id < self.inner.slots.borrow().len() {
                        self.inner.free.borrow_mut().push(id);
                        self.inner.live.set(self.inner.live.get().saturating_sub(1));
                    }
                }
                Poll::Pending => {
                    if let Some(slot) = self.inner.slots.borrow_mut().get_mut(id) {
                        *slot = Some((task, waker));
                    }
                }
            }
        }
        self.inner.running.set(false);
        self.pending_tasks()
    }

    /// Start `future` as a task and return a promise for its result.
    pub fn start<T: Clone + 'static>(&self, future: impl Future<Output = Result<T>> + 'static) -> Promise<T> {
        let promise = Promise::pending();
        let settle = promise.clone();
        self.spawn(async move {
            let guard = SettleGuard::new(&settle);
            let result = future.await;
            settle.settle(result);
            drop(guard);
        });
        promise
    }

    /// Drive `future` to completion. Fails if it stalls, which means a
    /// transport returned a pending promise nobody will settle — the
    /// blocking API only works with transports that complete synchronously.
    /// A stalled future is dropped (its guards reject and evict what it had
    /// reserved) rather than left in the runtime holding the engine.
    pub fn block_on<T: Clone + 'static>(&self, future: impl Future<Output = Result<T>> + 'static) -> Result<T> {
        let root = Rc::new(RootCell { future: RefCell::new(Some(Box::pin(future))), waker: RefCell::new(None) });
        let task_root = root.clone();
        let promise = self.start(async move {
            std::future::poll_fn(|cx| {
                *task_root.waker.borrow_mut() = Some(cx.waker().clone());
                // Polled outside the borrow so `block_on` can cancel it.
                let Some(mut future) = task_root.future.borrow_mut().take() else {
                    return Poll::Ready(Err(cancelled()));
                };
                let polled = future.as_mut().poll(cx);
                if polled.is_pending() {
                    *task_root.future.borrow_mut() = Some(future);
                }
                polled
            })
            .await
        });
        self.run_until_stalled();
        if let Some(result) = promise.peek() {
            return result;
        }
        let stalled = root.future.borrow_mut().take();
        drop(stalled);
        if let Some(waker) = root.waker.borrow_mut().take() {
            waker.wake();
        }
        self.run_until_stalled();
        Err(Error::new(
            "Rangefind blocking call stalled: the transport did not complete synchronously. Use the host session API for asynchronous transports.",
        ))
    }
}

/// The cancellable future behind a [`Runtime::block_on`] call.
struct RootCell<T> {
    future: RefCell<Option<LocalFuture<'static, Result<T>>>>,
    waker: RefCell<Option<Waker>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn promises_resolve_waiters_in_order() {
        let runtime = Runtime::new();
        let promise: Promise<u32> = Promise::pending();
        let a = promise.clone();
        let b = promise.clone();
        let out = runtime.start(async move {
            let values = try_join_all(vec![a, b]).await?;
            Ok(values.iter().sum::<u32>())
        });
        assert_eq!(runtime.run_until_stalled(), 1);
        assert!(out.peek().is_none());
        promise.resolve(21);
        assert_eq!(runtime.run_until_stalled(), 0);
        assert_eq!(out.peek(), Some(Ok(42)));
    }

    #[test]
    fn cache_shares_one_load_and_evicts_failures() {
        let runtime = Runtime::new();
        let cache: Rc<PromiseCache<&'static str, u32>> = Rc::new(PromiseCache::new());
        let loads = Rc::new(Cell::new(0));
        let gate: Promise<u32> = Promise::pending();
        let mut outs = Vec::new();
        for _ in 0..3 {
            let cache = cache.clone();
            let loads = loads.clone();
            let gate = gate.clone();
            outs.push(runtime.start(async move {
                cache
                    .get_or_load("k", async {
                        loads.set(loads.get() + 1);
                        gate.await
                    })
                    .await
            }));
        }
        runtime.run_until_stalled();
        gate.reject(Error::new("boom"));
        runtime.run_until_stalled();
        assert_eq!(loads.get(), 1);
        assert!(outs.iter().all(|out| matches!(out.peek(), Some(Err(_)))));
        assert!(!cache.has(&"k"));
    }

    #[test]
    fn block_on_reports_stalls() {
        let runtime = Runtime::new();
        let never: Promise<u32> = Promise::pending();
        assert!(runtime.block_on(async move { never.await }).is_err());
        assert_eq!(runtime.block_on(async { Ok(7u32) }), Ok(7));
    }

    /// Poll `future` once with a no-op waker, then drop it.
    fn poll_once_and_drop<F: Future>(future: F) {
        let mut future = Box::pin(future);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(future.as_mut().poll(&mut cx).is_pending());
    }

    #[test]
    fn cancelled_cache_load_is_evicted_not_cached() {
        let cache: PromiseCache<&'static str, u32> = PromiseCache::new();
        let gate: Promise<u32> = Promise::pending();
        poll_once_and_drop(cache.get_or_load("k", gate.clone()));
        // The waiter-visible promise was rejected and the key evicted, so
        // the next load runs instead of replaying the cancellation.
        assert!(!cache.has(&"k"));
        let runtime = Runtime::new();
        assert_eq!(runtime.block_on(async move { cache.get_or_load("k", async { Ok(7) }).await }), Ok(7));
    }

    #[test]
    fn cancelled_cache_load_rejects_waiters() {
        let runtime = Runtime::new();
        let cache: Rc<PromiseCache<&'static str, u32>> = Rc::new(PromiseCache::new());
        let gate: Promise<u32> = Promise::pending();
        let mut owner = Box::pin({
            let cache = cache.clone();
            let gate = gate.clone();
            async move { cache.get_or_load("k", gate).await }
        });
        let mut cx = Context::from_waker(Waker::noop());
        assert!(owner.as_mut().poll(&mut cx).is_pending());
        let waiter = runtime.start({
            let cache = cache.clone();
            async move { cache.get_or_load("k", async { Ok(1) }).await }
        });
        runtime.run_until_stalled();
        assert!(waiter.peek().is_none());
        drop(owner);
        runtime.run_until_stalled();
        assert_eq!(waiter.peek().map(|result| result.map_err(|error| error.message().to_string())), Some(Err(CANCELLED_MESSAGE.to_string())));
        assert!(!cache.has(&"k"));
    }

    #[test]
    fn cancelled_slot_load_clears_even_without_retry() {
        let slot: PromiseSlot<u32> = PromiseSlot::default();
        poll_once_and_drop(slot.get_or_load(Promise::<u32>::pending(), false));
        let runtime = Runtime::new();
        let slot = Rc::new(slot);
        let again = slot.clone();
        assert_eq!(runtime.block_on(async move { again.get_or_load(async { Ok(3) }, false).await }), Ok(3));
        assert_eq!(slot.ready(), Some(3));
    }

    #[test]
    fn reservation_guard_rejects_and_evicts_only_its_own_promises() {
        let cache: PromiseCache<u32, u32> = PromiseCache::new();
        let mine = cache.reserve(1);
        let replaced = cache.reserve(2);
        {
            let mut guard = ReservationGuard::new(&cache);
            guard.push(1, mine.clone());
            guard.push(2, replaced.clone());
            // Someone else reloaded key 2 meanwhile: it must survive.
            cache.set(2, Promise::resolved(22));
        }
        assert!(matches!(mine.peek(), Some(Err(_))));
        assert!(matches!(replaced.peek(), Some(Err(_))));
        assert!(!cache.has(&1));
        assert_eq!(cache.ready(&2), Some(22));
    }

    #[test]
    fn shutdown_drops_parked_tasks() {
        let runtime = Runtime::new();
        let held = Rc::new(());
        let witness = held.clone();
        let never: Promise<u32> = Promise::pending();
        runtime.spawn(async move {
            let _keep = witness;
            let _ = never.await;
        });
        runtime.run_until_stalled();
        assert_eq!(Rc::strong_count(&held), 2);
        runtime.shutdown();
        assert_eq!(Rc::strong_count(&held), 1);
        assert_eq!(runtime.pending_tasks(), 0);
    }

    #[test]
    fn spawned_tasks_run() {
        let runtime = Runtime::new();
        let hit = Rc::new(Cell::new(false));
        let flag = hit.clone();
        runtime.spawn(async move { flag.set(true) });
        runtime.run_until_stalled();
        assert!(hit.get());
    }
}
