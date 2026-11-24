//! Fetch primitives shared by every lane (`fetchGzipArrayBuffer`,
//! `fetchRange`, `fetchRanges`, `readRangeGroups`, `inflateObject` in
//! `src/runtime.js`).
//!
//! Transports return exact bytes per requested range; HTTP quirks such as
//! servers ignoring `Range` or rejecting multipart requests are handled by
//! the transport implementation (see the JS glue for wasm), so the engine
//! only decides *which* ranges to ask for and how to batch them.

use std::cell::RefCell;
use std::collections::HashSet;
use std::io::Read;
use std::rc::Rc;

use crate::error::{Error, Result};
use crate::pointer::{group_ranges, Pointer, RangeGroup, RangePlan};
use crate::rt::{try_join_all, Promise};
use crate::transport::{url_origin, ByteRange, FetchRequest, FetchResponse, Transport};
use crate::util::bytes::Bytes;

use super::trace::Trace;

pub struct Io {
    pub transport: Rc<dyn Transport>,
    pub multi_range: bool,
    pub multi_range_max_ranges: usize,
    multi_range_unsupported: RefCell<HashSet<String>>,
    pub trace: Rc<Trace>,
    /// Largest inflated size accepted for a whole gzip object (option
    /// `maxInflateBytes`, default [`DEFAULT_MAX_INFLATE_BYTES`]).
    pub max_inflate_bytes: u64,
    /// Memory budget shared by every decoded-object cache of the engines on
    /// this I/O layer (a federation's children included). Option
    /// `cacheBudgetBytes`; unbounded unless set with [`Io::with_cache_budget`].
    pub cache_budget: Rc<crate::budget::CacheBudget>,
}

impl Io {
    pub fn new(transport: Rc<dyn Transport>, multi_range: bool, multi_range_max_ranges: usize, trace: Rc<Trace>) -> Self {
        Io {
            transport,
            multi_range,
            multi_range_max_ranges: multi_range_max_ranges.clamp(2, 64),
            multi_range_unsupported: RefCell::new(HashSet::new()),
            trace,
            max_inflate_bytes: DEFAULT_MAX_INFLATE_BYTES,
            cache_budget: crate::budget::CacheBudget::unbounded(),
        }
    }

    /// Bound the engines' caches to `bytes` (0 = unbounded).
    pub fn with_cache_budget(mut self, bytes: usize) -> Self {
        self.cache_budget = crate::budget::CacheBudget::new(bytes);
        self
    }

    pub fn with_max_inflate_bytes(mut self, limit: u64) -> Self {
        self.max_inflate_bytes = limit.max(1);
        self
    }

    /// gzip → bytes under this I/O layer's inflate cap.
    pub fn inflate(&self, compressed: &[u8]) -> Result<Bytes> {
        inflate_limited(compressed, self.max_inflate_bytes)
    }

    /// Whole object, or `None` when it does not exist.
    pub async fn fetch_optional(&self, url: &str) -> Result<Option<Bytes>> {
        self.trace.request(url, 0);
        match self.transport.fetch(FetchRequest { url: url.to_string(), ranges: Vec::new() }).await? {
            FetchResponse::Missing => Ok(None),
            FetchResponse::Ok(mut parts) => {
                let bytes = parts.pop().unwrap_or_else(Bytes::empty);
                self.trace.bytes(url, bytes.len());
                Ok(Some(bytes))
            }
        }
    }

    pub async fn fetch(&self, url: &str) -> Result<Bytes> {
        match self.fetch_optional(url).await? {
            Some(bytes) => Ok(bytes),
            None => Err(Error::new(format!("Unable to fetch {url}"))),
        }
    }

    pub async fn fetch_json_optional(&self, url: &str) -> Result<Option<serde_json::Value>> {
        match self.fetch_optional(url).await? {
            None => Ok(None),
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        }
    }

    pub async fn fetch_gzip(&self, url: &str) -> Result<Bytes> {
        let compressed = self.fetch(url).await?;
        self.inflate(&compressed)
    }

    /// `.gz` manifests are inflated; others are parsed as-is. `None` when
    /// the object is missing.
    pub async fn fetch_manifest_json(&self, url: &str) -> Result<Option<serde_json::Value>> {
        let Some(raw) = self.fetch_optional(url).await? else { return Ok(None) };
        let bytes = if url.ends_with(".gz") { self.inflate(&raw)? } else { raw };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    pub async fn fetch_range(&self, url: &str, offset: u64, length: u64) -> Result<Bytes> {
        let mut parts = self.fetch_ranges_raw(url, vec![ByteRange { offset, length }]).await?;
        Ok(parts.pop().unwrap_or_else(Bytes::empty))
    }

    async fn fetch_ranges_raw(&self, url: &str, ranges: Vec<ByteRange>) -> Result<Vec<Bytes>> {
        self.trace.request(url, ranges.len());
        let count = ranges.len();
        match self.transport.fetch(FetchRequest { url: url.to_string(), ranges }).await? {
            FetchResponse::Missing => Err(Error::new(format!("Range request failed for {url}"))),
            FetchResponse::Ok(parts) => {
                if parts.len() != count {
                    return Err(Error::new(format!("Range request failed for {url}")));
                }
                for part in &parts {
                    self.trace.bytes(url, part.len());
                }
                Ok(parts)
            }
        }
    }

    /// Several ranges of one object. With multi-range enabled the ranges go
    /// out as one request (chunked to `multi_range_max_ranges`); otherwise
    /// one request per range, issued concurrently.
    pub async fn fetch_ranges(&self, url: &str, ranges: Vec<ByteRange>) -> Result<Vec<Bytes>> {
        if ranges.len() <= 1 || !self.multi_range || self.multi_range_unsupported.borrow().contains(url_origin(url)) {
            if ranges.len() == 1 {
                return self.fetch_ranges_raw(url, ranges).await;
            }
            let futures = ranges.into_iter().map(|range| self.fetch_range(url, range.offset, range.length));
            return try_join_all(futures).await;
        }
        let mut out = Vec::with_capacity(ranges.len());
        for chunk in ranges.chunks(self.multi_range_max_ranges) {
            match self.fetch_ranges_raw(url, chunk.to_vec()).await {
                Ok(parts) => out.extend(parts),
                Err(_) => {
                    // Mirror the JS fallback: remember the origin and retry
                    // with exact single-range reads.
                    self.multi_range_unsupported.borrow_mut().insert(url_origin(url).to_string());
                    let futures = chunk.iter().map(|range| self.fetch_range(url, range.offset, range.length));
                    out.extend(try_join_all(futures).await?);
                }
            }
        }
        Ok(out)
    }

    /// Fetch every range group and hand each group's bytes to `read`.
    /// Groups of the same URL share one `fetch_ranges` call.
    pub async fn read_range_groups<T, U, R>(&self, groups: Vec<RangeGroup<T>>, url_for: U, read: R) -> Result<()>
    where
        U: Fn(&RangeGroup<T>) -> String,
        R: Fn(&RangeGroup<T>, Bytes) -> Result<()>,
    {
        if groups.is_empty() {
            return Ok(());
        }
        let mut batches: Vec<(String, Vec<RangeGroup<T>>)> = Vec::new();
        for group in groups {
            let url = url_for(&group);
            match batches.iter_mut().find(|(existing, _)| *existing == url) {
                Some((_, list)) => list.push(group),
                None => batches.push((url, vec![group])),
            }
        }
        let read = &read;
        let futures = batches.into_iter().map(|(url, batch)| async move {
            let ranges = batch.iter().map(|group| ByteRange { offset: group.start, length: group.end - group.start }).collect();
            let buffers = self.fetch_ranges(&url, ranges).await?;
            let mut first_error = None;
            for (group, buffer) in batch.iter().zip(buffers) {
                if let Err(error) = read(group, buffer) {
                    first_error.get_or_insert(error);
                }
            }
            match first_error {
                Some(error) => Err(error),
                None => Ok(()),
            }
        });
        try_join_all(futures).await.map(|_| ())
    }
}

/// Default ceiling on one inflated object: far above any legitimate index
/// object, low enough that a corrupt or hostile gzip stream (a "bomb")
/// fails with an error instead of exhausting memory and aborting.
pub const DEFAULT_MAX_INFLATE_BYTES: u64 = 512 << 20;

/// gzip → bytes. Accepts concatenated members, like `DecompressionStream`.
pub fn inflate(compressed: &[u8]) -> Result<Bytes> {
    inflate_limited(compressed, DEFAULT_MAX_INFLATE_BYTES)
}

/// [`inflate`], refusing outputs larger than `limit` bytes.
pub fn inflate_limited(compressed: &[u8], limit: u64) -> Result<Bytes> {
    let decoder = flate2::read::MultiGzDecoder::new(compressed);
    let hint = (compressed.len() as u64).saturating_mul(4).min(limit);
    let mut out = Vec::with_capacity(usize::try_from(hint).unwrap_or(0));
    decoder
        .take(limit.saturating_add(1))
        .read_to_end(&mut out)
        .map_err(|error| Error::new(format!("Rangefind could not inflate a compressed object: {error}")))?;
    if out.len() as u64 > limit {
        return Err(Error::new(format!(
            "Rangefind refused to inflate a compressed object beyond {limit} bytes (raise the maxInflateBytes option if the index really holds objects this large)."
        )));
    }
    Ok(Bytes::new(out))
}

/// The bytes of `item` inside a fetched group buffer.
pub fn group_item_bytes(group_buffer: &Bytes, group_start: u64, pointer: &Pointer) -> Bytes {
    let start = usize::try_from(pointer.offset.saturating_sub(group_start)).unwrap_or(usize::MAX);
    let length = usize::try_from(pointer.length).unwrap_or(usize::MAX);
    group_buffer.slice(start, start.saturating_add(length))
}

/// `inflateObject`: verify the compressed bytes against the pointer's
/// checksum (when enabled) and inflate. A pointer that records its
/// `logicalLength` bounds the output to a small multiple of it, so one
/// corrupt pack object cannot balloon to the global cap.
pub fn inflate_object(compressed: &Bytes, pointer: &Pointer, label: &str, verify: bool) -> Result<Bytes> {
    if verify {
        crate::codec::verify_pointer(compressed, pointer, label)?;
    }
    let limit = match pointer.logical_length {
        Some(logical) => logical.saturating_mul(4).saturating_add(1 << 20).min(DEFAULT_MAX_INFLATE_BYTES),
        None => DEFAULT_MAX_INFLATE_BYTES,
    };
    inflate_limited(compressed, limit)
}

pub fn plan_groups<T>(items: Vec<(Pointer, T)>, plan: &RangePlan) -> Vec<RangeGroup<T>> {
    group_ranges(items, plan)
}

/// Settle a batch of reserved promises with a shared failure.
pub fn reject_all<T: Clone>(promises: impl IntoIterator<Item = Promise<T>>, error: &Error) {
    for promise in promises {
        promise.reject(error.clone());
    }
}
