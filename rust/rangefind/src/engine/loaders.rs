//! Object loaders: lazy manifests, range directories, posting segments,
//! external posting blocks, doc values, sorted doc-value pages, filter
//! bitmaps and facet dictionaries.

use std::collections::HashSet;
use std::hash::Hash;
use std::rc::Rc;

use serde_json::Value;

use crate::codec::directory::{self, DirectoryPage, DirectoryRoot};
use crate::codec::doc_values::{self, Codes, DocValue, DocValueChunk, DocValuesMeta};
use crate::codec::filter_bitmap::{self, FilterBitmap};
use crate::codec::postings::{self, PostingSegment, TermEntry};
use crate::codec::sort_tree::{self, DocValueSortDirectory, SortPage};
use crate::error::{Error, Result};
use crate::pointer::{group_ranges, Pointer, RangePlan};
use crate::rt::{try_join_all, Promise, PromiseCache, ReservationGuard};
use crate::util::bytes::Bytes;
use crate::util::json::path;
use crate::util::jsstr::{cmp16, shard_key};

use super::index::{DirectoryState, IndexEngine, TermHit};
use super::io::{group_item_bytes, inflate_object};
use super::options::EXTERNAL_POSTING_BLOCK_PREFETCH;

#[derive(Clone, Copy, Debug, Default)]
pub struct BatchStats {
    pub wanted: usize,
    pub fetched: usize,
    pub groups: usize,
}

impl BatchStats {
    pub fn add(&mut self, other: BatchStats) {
        self.wanted += other.wanted;
        self.fetched += other.fetched;
        self.groups += other.groups;
    }
}

/// Cache identity of a term entry: its never-reused uid. (Its address is
/// not one: once a posting segment is evicted, a later segment's entry can
/// land at the same address and would read the old term's blocks.)
pub(crate) fn entry_key(entry: &TermEntry) -> u64 {
    entry.uid
}

impl IndexEngine {
    /// Reserve promises for uncached keys, group their byte ranges, fetch,
    /// decode each item from its slice and settle. Failures evict the keys
    /// so later queries retry.
    pub(crate) async fn load_batch<K, V, U, D>(
        &self,
        cache: &PromiseCache<K, V>,
        items: Vec<(K, Pointer)>,
        plan: &RangePlan,
        url_for: U,
        decode: D,
    ) -> Result<BatchStats>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        U: Fn(&str) -> String,
        D: Fn(&K, &Pointer, Bytes) -> Result<V>,
    {
        self.load_batch_observed(cache, items, plan, url_for, decode).await.map(|(stats, _)| stats)
    }

    /// [`Self::load_batch`], also returning the promise the batch observed
    /// for every distinct key (in input order): the one it reserved, or the
    /// one another query already had in flight. Callers that need the
    /// values await these rather than re-reading the cache, because a
    /// concurrent load that fails evicts its key — re-reading would then
    /// find nothing and silently treat the key as "no data".
    ///
    /// Reservations are guarded: if this future is dropped mid-flight, the
    /// keys it reserved are rejected and evicted instead of staying pending
    /// forever (which would hang every later query touching them).
    pub(crate) async fn load_batch_observed<K, V, U, D>(
        &self,
        cache: &PromiseCache<K, V>,
        items: Vec<(K, Pointer)>,
        plan: &RangePlan,
        url_for: U,
        decode: D,
    ) -> Result<(BatchStats, Vec<(K, Promise<V>)>)>
    where
        K: Eq + Hash + Clone,
        V: Clone,
        U: Fn(&str) -> String,
        D: Fn(&K, &Pointer, Bytes) -> Result<V>,
    {
        let mut stats = BatchStats::default();
        let mut observed: Vec<(K, Promise<V>)> = Vec::new();
        let mut guard = ReservationGuard::new(cache);
        let mut pending: Vec<(Pointer, (K, Promise<V>))> = Vec::new();
        let mut seen = HashSet::new();
        for (key, pointer) in items {
            stats.wanted += 1;
            if !seen.insert(key.clone()) {
                continue;
            }
            if let Some(existing) = cache.get(&key) {
                observed.push((key, existing));
                continue;
            }
            let promise = cache.reserve(key.clone());
            guard.push(key.clone(), promise.clone());
            observed.push((key.clone(), promise.clone()));
            pending.push((pointer, (key, promise)));
        }
        stats.fetched = pending.len();
        if pending.is_empty() {
            return Ok((stats, observed));
        }
        let groups = group_ranges(pending, plan);
        stats.groups = groups.len();
        let result = self
            .io
            .read_range_groups(groups, |group| url_for(&group.pack), |group, buffer| {
                let mut first = None;
                for (pointer, (key, promise)) in &group.items {
                    let slice = group_item_bytes(&buffer, group.start, pointer);
                    match decode(key, pointer, slice) {
                        Ok(value) => promise.resolve(value),
                        Err(error) => {
                            cache.evict_if_same(key, promise);
                            promise.reject(error.clone());
                            first.get_or_insert(error);
                        }
                    }
                }
                first.map_or(Ok(()), Err)
            })
            .await;
        if let Err(error) = &result {
            guard.fail_pending(error);
        }
        drop(guard);
        result.map(|_| (stats, observed))
    }

    // ------------------------------------------------------------ manifests

    pub async fn ensure_full_manifest(&self) -> Result<()> {
        if self.full_manifest_loaded.get() {
            return Ok(());
        }
        let path_value = self.manifest_at(&["lazy_manifests", "full"]);
        let Some(full_path) = path_value.as_str().map(str::to_string) else { return Ok(()) };
        self.full_manifest
            .get_or_load(
                async {
                    if let Some(full) = self.io.fetch_json_optional(&self.url(&full_path)).await? {
                        // The full manifest carries facet summaries ({ count })
                        // where lazily loaded dictionaries (value arrays) may
                        // already sit in manifest.facets; their cache entries
                        // stay resolved, so keep them across the merge.
                        let loaded_facets: Vec<(String, Value)> = self
                            .manifest
                            .borrow()
                            .get("facets")
                            .and_then(Value::as_object)
                            .map(|facets| facets.iter().filter(|(_, values)| values.is_array()).map(|(k, v)| (k.clone(), v.clone())).collect())
                            .unwrap_or_default();
                        crate::util::json::assign(&mut self.manifest.borrow_mut(), &full);
                        if !loaded_facets.is_empty() {
                            let mut manifest = self.manifest.borrow_mut();
                            if !manifest.get("facets").map(Value::is_object).unwrap_or(false) {
                                manifest["facets"] = Value::Object(Default::default());
                            }
                            for (field, values) in loaded_facets {
                                manifest["facets"][field.as_str()] = values;
                            }
                        }
                        self.refresh_from_manifest()?;
                        self.full_manifest_loaded.set(true);
                    }
                    Ok(true)
                },
                true,
            )
            .await
            .map(|_| ())
    }

    pub async fn ensure_doc_values_manifest(&self) -> Result<Option<Rc<DocValuesMeta>>> {
        if let Some(meta) = self.doc_values_meta() {
            return Ok(Some(meta));
        }
        let binary = if self.options.doc_values_binary_manifest {
            self.manifest_at(&["lazy_manifests", "doc_values_v2"]).as_str().map(str::to_string)
        } else {
            None
        };
        let json_path = self.manifest_at(&["lazy_manifests", "doc_values"]).as_str().map(str::to_string);
        if binary.is_none() && json_path.is_none() {
            return Ok(None);
        }
        let loaded = self
            .doc_values_slot
            .get_or_load(
                async {
                    let load_json = || async {
                        match &json_path {
                            Some(p) => match self.io.fetch_manifest_json(&self.url(p)).await? {
                                Some(value) => Ok::<_, Error>(Some(Rc::new(doc_values::doc_values_meta_from_json(&value)))),
                                None => Ok(None),
                            },
                            None => Ok(None),
                        }
                    };
                    let meta = match &binary {
                        Some(p) => {
                            let attempt: Result<Rc<DocValuesMeta>> = async {
                                let bytes = self.io.fetch_gzip(&self.url(p)).await?;
                                Ok(Rc::new(doc_values::parse_doc_value_manifest(&bytes)?))
                            }
                            .await;
                            match attempt {
                                Ok(meta) => Some(meta),
                                Err(_) => load_json().await?,
                            }
                        }
                        None => load_json().await?,
                    };
                    if let Some(meta) = &meta {
                        *self.doc_values.borrow_mut() = Some(meta.clone());
                    }
                    Ok(meta)
                },
                true,
            )
            .await?;
        Ok(loaded)
    }

    async fn lazy_json(&self, key: &str) -> Result<Option<Rc<Value>>> {
        let manifest_path = self.manifest_at(&["lazy_manifests", key]).as_str().map(str::to_string);
        match manifest_path {
            None => Ok(None),
            Some(p) => Ok(self.io.fetch_manifest_json(&self.url(&p)).await?.map(Rc::new)),
        }
    }

    pub async fn ensure_doc_value_sorted_manifest(&self) -> Result<Option<Rc<Value>>> {
        if let Some(meta) = self.doc_value_sorted_meta() {
            return Ok(Some(meta));
        }
        if self.manifest_at(&["lazy_manifests", "doc_value_sorted"]).is_null() {
            return Ok(None);
        }
        self.doc_value_sorted_slot
            .get_or_load(
                async {
                    let meta = self.lazy_json("doc_value_sorted").await?;
                    if let Some(meta) = &meta {
                        *self.doc_value_sorted.borrow_mut() = Some(meta.clone());
                        self.manifest.borrow_mut()["doc_value_sorted"] = (**meta).clone();
                    }
                    Ok(meta)
                },
                true,
            )
            .await
    }

    pub async fn ensure_filter_bitmap_manifest(&self) -> Result<Option<Rc<Value>>> {
        if let Some(meta) = self.filter_bitmaps_meta() {
            return Ok(Some(meta));
        }
        if self.manifest_at(&["lazy_manifests", "filter_bitmaps"]).is_null() {
            return Ok(None);
        }
        self.filter_bitmaps_slot
            .get_or_load(
                async {
                    let meta = self.lazy_json("filter_bitmaps").await?;
                    if let Some(meta) = &meta {
                        *self.filter_bitmaps.borrow_mut() = Some(meta.clone());
                    }
                    Ok(meta)
                },
                true,
            )
            .await
    }

    pub async fn ensure_facet_dictionary_manifest(&self) -> Result<Option<Rc<Value>>> {
        if let Some(meta) = self.facet_dictionaries.borrow().clone() {
            return Ok(Some(meta));
        }
        if self.manifest_at(&["lazy_manifests", "facet_dictionaries"]).is_null() {
            return Ok(None);
        }
        self.facet_dictionaries_slot
            .get_or_load(
                async {
                    let meta = self.lazy_json("facet_dictionaries").await?;
                    if let Some(meta) = &meta {
                        *self.facet_directory.borrow_mut() = DirectoryState::optional(path(meta, &["directory"]));
                        *self.facet_dictionaries.borrow_mut() = Some(meta.clone());
                    }
                    Ok(meta)
                },
                true,
            )
            .await
    }

    pub async fn load_segment_manifest(&self) -> Result<Option<Rc<Value>>> {
        let manifest_path = self.manifest_at(&["segments", "manifest"]).as_str().map(str::to_string);
        let Some(p) = manifest_path else { return Ok(None) };
        self.segment_manifest
            .get_or_load(async { Ok(self.io.fetch_manifest_json(&self.url(&p)).await?.map(Rc::new)) }, true)
            .await
    }

    pub async fn load_codes(&self) -> Result<Rc<Codes>> {
        self.codes
            .get_or_load(
                async {
                    let bytes = self.io.fetch_gzip(&self.url("codes.bin.gz")).await?;
                    Ok(Rc::new(doc_values::parse_codes(&bytes)?))
                },
                true,
            )
            .await
    }

    // ------------------------------------------------------------ directories

    pub async fn load_directory_root(&self, state: &DirectoryState) -> Result<Rc<DirectoryRoot>> {
        state
            .root
            .get_or_load(
                async {
                    let bytes = self.io.fetch_gzip(&self.url(&state.root_path)).await?;
                    Ok(Rc::new(directory::parse_directory_root(&bytes)?))
                },
                true,
            )
            .await
    }

    pub async fn load_directory_page(&self, state: &DirectoryState, file: &str) -> Result<Rc<DirectoryPage>> {
        state
            .pages
            .get_or_load(file.to_string(), async {
                let bytes = self.io.fetch_gzip(&self.url(&state.page_path(file))).await?;
                Ok(Rc::new(directory::parse_directory_page(&bytes, &state.pack_table)?))
            })
            .await
    }

    pub async fn directory_entry_from_root(&self, state: &DirectoryState, root: &DirectoryRoot, shard: &str) -> Result<Option<Pointer>> {
        let Some(page) = directory::find_directory_page(root, shard) else { return Ok(None) };
        let file = page.file.clone();
        let entries = self.load_directory_page(state, &file).await?;
        Ok(entries.get(shard).cloned())
    }

    /// `resolveDirectoryShard`: deepest existing shard key first.
    pub async fn resolve_directory_shard(
        &self,
        value: &str,
        state: &DirectoryState,
        base_depth: usize,
        max_depth: usize,
    ) -> Result<Option<(String, Pointer)>> {
        let root = self.load_directory_root(state).await?;
        let mut depth = max_depth;
        loop {
            if depth < base_depth {
                return Ok(None);
            }
            let key = shard_key(value, depth);
            if let Some(entry) = self.directory_entry_from_root(state, &root, &key).await? {
                return Ok(Some((key, entry)));
            }
            if depth == 0 {
                return Ok(None);
            }
            depth -= 1;
        }
    }

    /// `directoryEntriesForPrefix`.
    pub async fn directory_entries_for_prefix(
        &self,
        state: &DirectoryState,
        prefix: &str,
        max_entries: usize,
    ) -> Result<(Vec<(String, Pointer)>, usize, bool)> {
        let root = self.load_directory_root(state).await?;
        let mut upper = prefix.to_string();
        upper.push('\u{ffff}');
        let pages = &root.pages;
        let (mut lo, mut hi) = (0usize, pages.len());
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if cmp16(&pages[mid].last, prefix).is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let start = lo.saturating_sub(1);
        let mut found = Vec::new();
        let mut visited = 0;
        let mut truncated = false;
        for index in start..pages.len() {
            let page = &pages[index];
            if cmp16(&page.first, &upper).is_gt() && !prefix.starts_with(page.first.as_str()) {
                break;
            }
            if cmp16(&page.last, prefix).is_lt() && index != start {
                continue;
            }
            let entries = self.load_directory_page(state, &page.file.clone()).await?;
            visited += 1;
            for (shard, entry) in entries.entries.iter() {
                if !prefix.starts_with(shard.as_str()) && !shard.starts_with(prefix) {
                    continue;
                }
                if found.len() >= max_entries {
                    truncated = true;
                    break;
                }
                found.push((shard.clone(), entry.clone()));
            }
            if truncated {
                break;
            }
        }
        Ok((found, visited, truncated))
    }

    // ------------------------------------------------------------ postings

    pub async fn load_shards(&self, shards: &[(String, Pointer)]) -> Result<Vec<(String, Rc<PostingSegment>)>> {
        let manifest = self.posting_manifest.borrow().clone();
        let verify = self.verify_checksums;
        let items: Vec<(String, Pointer)> = shards.to_vec();
        let (_, observed) = self.load_batch_observed(
            &self.shard_cache,
            items,
            &self.options.range_plans.default,
            |pack| self.url(&format!("terms/packs/{pack}")),
            |shard, pointer, slice| {
                let inflated = inflate_object(&slice, pointer, &format!("posting segment {shard}"), verify)?;
                Ok(Rc::new(postings::parse_posting_segment(inflated, &manifest)?))
            },
        )
        .await?;
        // Await what the batch observed (distinct shards, input order): a
        // shard another query had in flight and then failed is an error
        // here, not a silently missing shard.
        let mut out = Vec::with_capacity(observed.len());
        for (shard, promise) in observed {
            out.push((shard, promise.await?));
        }
        Ok(out)
    }

    /// `termEntries`: resolve terms to posting entries, in term order.
    pub async fn term_entries(&self, terms: &[String]) -> Result<Vec<TermHit>> {
        let mut buckets: Vec<(String, Pointer, Vec<String>)> = Vec::new();
        for term in terms {
            let resolved = self
                .resolve_directory_shard(term, &self.term_directory, self.info.base_shard_depth, self.info.max_shard_depth)
                .await?;
            let Some((shard, entry)) = resolved else { continue };
            match buckets.iter_mut().find(|bucket| bucket.0 == shard) {
                Some(bucket) => bucket.2.push(term.clone()),
                None => buckets.push((shard, entry, vec![term.clone()])),
            }
        }
        let requests: Vec<(String, Pointer)> = buckets.iter().map(|(shard, entry, _)| (shard.clone(), entry.clone())).collect();
        let loaded = self.load_shards(&requests).await?;
        let mut out = Vec::new();
        for (shard, _, bucket_terms) in &buckets {
            let Some((_, data)) = loaded.iter().find(|(name, _)| name == shard) else { continue };
            let shard_name: Rc<str> = Rc::from(shard.as_str());
            for term in bucket_terms {
                if let Some(entry) = data.term(term) {
                    out.push(TermHit { term: term.clone(), shard_name: shard_name.clone(), shard: data.clone(), entry });
                }
            }
        }
        Ok(out)
    }

    /// `loadPostingBlockBatch`: fetch and decode external blocks.
    pub async fn load_posting_block_batch(&self, requests: &[(Rc<TermEntry>, usize)], plan: &RangePlan) -> Result<BatchStats> {
        let verify = self.verify_checksums;
        let mut items = Vec::new();
        let mut owners = std::collections::HashMap::new();
        let mut stats = BatchStats::default();
        for (entry, block_index) in requests {
            let key = (entry_key(entry), *block_index);
            owners.insert(key, entry.clone());
            match entry.blocks.get(*block_index).and_then(|block| block.range.clone()) {
                Some(range) => items.push((key, range)),
                None => {
                    stats.wanted += 1;
                    if !self.external_blocks.has(&key) {
                        self.external_blocks.set(key, Promise::resolved(Rc::new(Vec::new())));
                    }
                }
            }
        }
        let batch = self
            .load_batch(
                &self.external_blocks,
                items,
                plan,
                |pack| self.url(&format!("terms/block-packs/{pack}")),
                |key, pointer, slice| {
                    let inflated = inflate_object(&slice, pointer, &format!("posting block {}", key.1), verify)?;
                    let entry = &owners[key];
                    let codec = entry.blocks[key.1].codec;
                    Ok(Rc::new(postings::decode_posting_bytes(&inflated, codec)))
                },
            )
            .await?;
        stats.add(batch);
        Ok(stats)
    }

    pub async fn external_block_rows(&self, entry: &TermEntry, block_index: usize) -> Result<Rc<Vec<i32>>> {
        let key = (entry_key(entry), block_index);
        if let Some(promise) = self.external_blocks.get(&key) {
            return promise.await;
        }
        let Some(block) = entry.blocks.get(block_index) else { return Ok(Rc::new(Vec::new())) };
        let Some(range) = block.range.clone() else { return Ok(Rc::new(Vec::new())) };
        // Every caller batch-loads this block first, so a miss means the
        // load another query had in flight failed (or was cancelled) and
        // evicted the key. Load it once more so the real error surfaces
        // instead of the block reading as empty.
        let verify = self.verify_checksums;
        let codec = block.codec;
        let (_, observed) = self
            .load_batch_observed(
                &self.external_blocks,
                vec![(key, range)],
                &self.options.range_plans.posting_blocks,
                |pack| self.url(&format!("terms/block-packs/{pack}")),
                |key, pointer, slice| {
                    let inflated = inflate_object(&slice, pointer, &format!("posting block {}", key.1), verify)?;
                    Ok(Rc::new(postings::decode_posting_bytes(&inflated, codec)))
                },
            )
            .await?;
        match observed.into_iter().next() {
            Some((_, promise)) => promise.await,
            None => Ok(Rc::new(Vec::new())),
        }
    }

    pub fn has_external_block(&self, entry: &TermEntry, block_index: usize) -> bool {
        self.external_blocks.has(&(entry_key(entry), block_index))
    }

    /// `postingBlockPrefetchIndexes`.
    pub fn posting_block_prefetch_indexes(
        &self,
        entry: &TermEntry,
        block_index: usize,
        prefetch: usize,
        max_block_exclusive: Option<usize>,
    ) -> Vec<usize> {
        let available = entry.blocks.len();
        let total = available.min(max_block_exclusive.unwrap_or(available));
        if block_index >= total {
            return Vec::new();
        }
        let cached = |index: usize| self.has_external_block(entry, index);
        let mut out = vec![block_index];
        let add_missing = |start: usize, count: usize, out: &mut Vec<usize>| {
            let limit = total.min(start.saturating_add(count));
            for index in start..limit {
                if index != block_index && !cached(index) {
                    out.push(index);
                }
            }
        };
        if total <= prefetch * 2 {
            add_missing(block_index + 1, total - block_index - 1, &mut out);
            return out;
        }
        if !cached(block_index) {
            add_missing(block_index + 1, prefetch.saturating_sub(1), &mut out);
            return out;
        }
        let mut contiguous_end = block_index + 1;
        while contiguous_end < total && cached(contiguous_end) {
            contiguous_end += 1;
        }
        let cached_ahead = contiguous_end - block_index;
        let refill_threshold = (prefetch / 4).max(2);
        if cached_ahead <= refill_threshold {
            add_missing(contiguous_end, prefetch, &mut out);
        }
        out
    }

    pub async fn decode_entry_block(&self, hit: &TermHit, block_index: usize) -> Result<Rc<Vec<i32>>> {
        if !hit.entry.external {
            return postings::decode_posting_block(&hit.shard, &hit.entry, block_index);
        }
        let indexes = self.posting_block_prefetch_indexes(&hit.entry, block_index, EXTERNAL_POSTING_BLOCK_PREFETCH, None);
        let requests: Vec<(Rc<TermEntry>, usize)> = indexes.iter().map(|index| (hit.entry.clone(), *index)).collect();
        self.load_posting_block_batch(&requests, &self.options.range_plans.posting_blocks).await?;
        self.external_block_rows(&hit.entry, block_index).await
    }

    /// `decodeEntryPostings`: every block of a term, concatenated.
    pub async fn decode_entry_postings(&self, hit: &TermHit) -> Result<Rc<Vec<i32>>> {
        if !hit.entry.external {
            return postings::decode_postings(&hit.shard, &hit.entry);
        }
        if let Some(rows) = hit.entry.postings.borrow().clone() {
            return Ok(rows);
        }
        let requests: Vec<(Rc<TermEntry>, usize)> = (0..hit.entry.blocks.len()).map(|index| (hit.entry.clone(), index)).collect();
        self.load_posting_block_batch(&requests, &self.options.range_plans.posting_blocks).await?;
        let mut out = Vec::new();
        for index in 0..hit.entry.blocks.len() {
            out.extend_from_slice(&self.external_block_rows(&hit.entry, index).await?);
        }
        let out = Rc::new(out);
        *hit.entry.postings.borrow_mut() = Some(out.clone());
        hit.shard.note_decoded_rows(out.len());
        Ok(out)
    }

    /// `lookupEntryBlocks`: rows of `candidates` inside the given blocks,
    /// without decoding whole external blocks.
    pub async fn lookup_entry_blocks(
        &self,
        hit: &TermHit,
        block_indexes: &[usize],
        candidates: &HashSet<i32>,
    ) -> Result<Vec<(usize, Vec<i32>, usize)>> {
        let mut indexes: Vec<usize> = Vec::new();
        for index in block_indexes {
            if *index < hit.entry.blocks.len() && !indexes.contains(index) {
                indexes.push(*index);
            }
        }
        if indexes.is_empty() || candidates.is_empty() {
            return Ok(Vec::new());
        }
        if !hit.entry.external {
            return indexes
                .into_iter()
                .map(|block_index| {
                    let (rows, scanned) = postings::lookup_posting_block(&hit.shard, &hit.entry, block_index, candidates)?;
                    Ok((block_index, rows, scanned))
                })
                .collect();
        }
        let key_of = |index: usize| (entry_key(&hit.entry), index);
        // Snapshot the promises other queries already hold: if one of their
        // loads fails while we wait, it evicts its key, and re-reading the
        // cache afterwards would mistake the failure for an empty block.
        let snapshot: Vec<(Option<Promise<Rc<Vec<i32>>>>, Option<Promise<Bytes>>)> =
            indexes.iter().map(|index| (self.external_blocks.get(&key_of(*index)), self.external_block_bytes.get(&key_of(*index)))).collect();
        let uncached: Vec<(usize, Pointer)> = indexes
            .iter()
            .zip(&snapshot)
            .filter(|(_, (rows, bytes))| rows.is_none() && bytes.is_none())
            .filter_map(|(index, _)| hit.entry.blocks[*index].range.clone().map(|range| (*index, range)))
            .collect();
        let verify = self.verify_checksums;
        let (_, observed) = self
            .load_batch_observed(
                &self.external_block_bytes,
                uncached.into_iter().map(|(index, range)| (key_of(index), range)).collect(),
                &self.options.range_plans.posting_blocks,
                |pack| self.url(&format!("terms/block-packs/{pack}")),
                |key, pointer, slice| inflate_object(&slice, pointer, &format!("posting block {}", key.1), verify),
            )
            .await?;
        let mut out = Vec::new();
        for (block_index, (snap_rows, snap_bytes)) in indexes.into_iter().zip(snapshot) {
            let key = key_of(block_index);
            let observed_bytes = || observed.iter().find(|(observed_key, _)| *observed_key == key).map(|(_, promise)| promise.clone());
            if let Some(rows) = self.external_blocks.get(&key).or(snap_rows) {
                let rows = rows.await?;
                let (found, scanned) = postings::lookup_decoded_posting_rows(&rows, candidates);
                out.push((block_index, found, scanned));
            } else if let Some(bytes) = self.external_block_bytes.get(&key).or(snap_bytes).or_else(observed_bytes) {
                let bytes = bytes.await?;
                let (found, scanned) = postings::lookup_posting_bytes(&bytes, candidates, hit.entry.blocks[block_index].codec);
                out.push((block_index, found, scanned));
            } else {
                out.push((block_index, Vec::new(), 0));
            }
        }
        Ok(out)
    }

    // ------------------------------------------------------------ facets

    pub async fn load_facet_dictionary(&self, field: &str) -> Result<Rc<Vec<Value>>> {
        if let Some(values) = self.manifest.borrow().get("facets").and_then(|f| f.get(field)).and_then(Value::as_array) {
            return Ok(Rc::new(values.clone()));
        }
        if self.facet_directory.borrow().is_none() && !self.manifest_at(&["lazy_manifests", "facet_dictionaries"]).is_null() {
            self.ensure_facet_dictionary_manifest().await?;
        }
        if self.facet_directory.borrow().is_none() && !self.manifest_at(&["lazy_manifests", "full"]).is_null() {
            self.ensure_full_manifest().await?;
        }
        let Some(state) = self.facet_directory.borrow().clone() else { return Ok(Rc::new(Vec::new())) };
        let meta = self.facet_dictionaries.borrow().clone();
        let Some(meta) = meta else { return Ok(Rc::new(Vec::new())) };
        if path(&meta, &["fields", field]).is_null() {
            return Ok(Rc::new(Vec::new()));
        }
        let packs = meta.get("packs").and_then(Value::as_str).unwrap_or("facets/packs/").to_string();
        let verify = self.verify_checksums;
        self.facet_dictionary_cache
            .get_or_load(field.to_string(), async {
                let root = self.load_directory_root(&state).await?;
                let Some(entry) = self.directory_entry_from_root(&state, &root, field).await? else {
                    return Ok(Rc::new(Vec::new()));
                };
                let mut base = packs.clone();
                if !base.ends_with('/') {
                    base.push('/');
                }
                let buffer = self.io.fetch_range(&self.url(&format!("{base}{}", entry.pack)), entry.offset, entry.length).await?;
                let inflated = inflate_object(&buffer, &entry, &format!("facet dictionary {field}"), verify)?;
                let values = crate::codec::facet_dictionary::parse_facet_dictionary(&inflated)?;
                {
                    let mut manifest = self.manifest.borrow_mut();
                    if !manifest.get("facets").map(Value::is_object).unwrap_or(false) {
                        manifest["facets"] = Value::Object(Default::default());
                    }
                    manifest["facets"][field] = Value::Array(values.clone());
                }
                Ok(Rc::new(values))
            })
            .await
    }

    pub async fn ensure_facet_dictionaries(&self, fields: &[String]) -> Result<()> {
        try_join_all(fields.iter().map(|field| self.load_facet_dictionary(field))).await.map(|_| ())
    }

    // ------------------------------------------------------------ doc values

    fn chunk_index_for_doc(&self, meta: &DocValuesMeta, field: &str, doc: usize) -> Option<usize> {
        let chunk_size = meta.chunk_size_or(self.total());
        let index = doc / chunk_size.max(1);
        meta.field(field).and_then(|f| f.chunks.get(index)).and_then(|c| c.as_ref()).map(|_| index)
    }

    fn lookup_index_for_doc(&self, meta: &DocValuesMeta, field: &str, doc: usize) -> Option<usize> {
        let field_meta = meta.field(field)?;
        if field_meta.lookup_chunks.is_empty() {
            return None;
        }
        let chunk_size = meta.lookup_chunk_size.unwrap_or_else(|| meta.chunk_size_or(self.total())).max(1);
        let index = doc / chunk_size;
        field_meta.lookup_chunks.get(index).and_then(|c| c.as_ref()).map(|_| index)
    }

    /// Doc-value chunk requests: (field, lookup lane, chunk index).
    pub async fn load_doc_value_chunks(&self, requests: Vec<(String, bool, usize)>) -> Result<()> {
        let Some(meta) = self.doc_values_meta() else { return Ok(()) };
        if requests.is_empty() {
            return Ok(());
        }
        let mut items = Vec::new();
        for (field, lookup, index) in requests {
            let Some(field_meta) = meta.field(&field) else { continue };
            let chunks = if lookup { &field_meta.lookup_chunks } else { &field_meta.chunks };
            let Some(Some(chunk)) = chunks.get(index) else { continue };
            let key = (field, lookup, index);
            if self.doc_value_cache.has(&key) {
                continue;
            }
            items.push((key, chunk.pointer.clone()));
        }
        if items.is_empty() {
            return Ok(());
        }
        self.attach_doc_value_checksums(&meta, &mut items).await?;
        let verify = self.verify_checksums;
        self.load_batch(
            &self.doc_value_cache,
            items,
            &self.options.range_plans.default,
            |pack| self.url(&format!("doc-values/packs/{pack}")),
            |key, pointer, slice| {
                let inflated = inflate_object(&slice, pointer, &format!("doc-value {}:{}", key.0, key.2), verify)?;
                Ok(Rc::new(doc_values::parse_doc_value_chunk(&inflated)?))
            },
        )
        .await
        .map(|_| ())
    }

    /// `attachDocValueChecksumRows`: out-of-line sha256 rows (rfdvm-v1).
    async fn attach_doc_value_checksums(&self, meta: &DocValuesMeta, items: &mut [((String, bool, usize), Pointer)]) -> Result<()> {
        let Some(rows_meta) = meta.checksum_rows.as_ref() else { return Ok(()) };
        if !self.verify_checksums {
            return Ok(());
        }
        let row_bytes = rows_meta.row_bytes.max(1) as u64;
        let mut rows: Vec<u64> =
            items.iter().filter(|(_, pointer)| pointer.checksum.is_none()).filter_map(|(_, pointer)| pointer.checksum_row).collect();
        rows.sort_unstable();
        rows.dedup();
        if rows.is_empty() {
            return Ok(());
        }
        let mut ranges: Vec<crate::transport::ByteRange> = Vec::new();
        for row in rows {
            match ranges.last_mut() {
                Some(last) if row * row_bytes == last.offset + last.length => last.length += row_bytes,
                _ => ranges.push(crate::transport::ByteRange { offset: row * row_bytes, length: row_bytes }),
            }
        }
        let buffers = self.io.fetch_ranges(&self.url(&rows_meta.file), ranges.clone()).await?;
        let mut by_row = std::collections::HashMap::new();
        for (range, buffer) in ranges.iter().zip(buffers) {
            let mut at = 0usize;
            while at + row_bytes as usize <= buffer.len() {
                let value = crate::util::bytes::hex(&buffer[at..at + row_bytes as usize]);
                by_row.insert((range.offset + at as u64) / row_bytes, value);
                at += row_bytes as usize;
            }
        }
        for (_, pointer) in items.iter_mut() {
            if pointer.checksum.is_some() {
                continue;
            }
            if let Some(value) = pointer.checksum_row.and_then(|row| by_row.get(&row)) {
                pointer.checksum = Some(crate::pointer::Checksum {
                    algorithm: Rc::from(rows_meta.algorithm.as_str()),
                    value: Rc::from(value.as_str()),
                });
            }
        }
        Ok(())
    }

    /// `ensureDocValuesForDocs`.
    pub async fn ensure_doc_values_for_docs(&self, fields: &[String], docs: &[i32]) -> Result<bool> {
        if self.doc_values_meta().is_none() {
            self.ensure_doc_values_manifest().await?;
        }
        let Some(meta) = self.doc_values_meta() else { return Ok(false) };
        if fields.is_empty() || docs.is_empty() {
            return Ok(false);
        }
        let mut requests = Vec::new();
        let mut seen = HashSet::new();
        for field in fields {
            let Some(field_meta) = meta.field(field) else { continue };
            let lookup = !field_meta.lookup_chunks.is_empty();
            for doc in docs {
                if *doc < 0 {
                    continue;
                }
                let doc = *doc as usize;
                let index = if lookup { self.lookup_index_for_doc(&meta, field, doc) } else { self.chunk_index_for_doc(&meta, field, doc) };
                let Some(index) = index else { continue };
                let key = (field.clone(), lookup, index);
                if self.doc_value_cache.has(&key) || !seen.insert(key.clone()) {
                    continue;
                }
                requests.push(key);
            }
        }
        self.load_doc_value_chunks(requests).await?;
        Ok(true)
    }

    /// `ensureDocValueChunkIndexes`.
    pub async fn ensure_doc_value_chunk_indexes(&self, fields: &[String], indexes: &[usize]) -> Result<bool> {
        if self.doc_values_meta().is_none() {
            self.ensure_doc_values_manifest().await?;
        }
        if self.doc_values_meta().is_none() || fields.is_empty() || indexes.is_empty() {
            return Ok(false);
        }
        let mut requests = Vec::new();
        for field in fields {
            for index in indexes {
                requests.push((field.clone(), false, *index));
            }
        }
        self.load_doc_value_chunks(requests).await?;
        Ok(true)
    }

    /// `docValue(field, doc)` from already-loaded chunks.
    pub fn doc_value(&self, field: &str, doc: i32) -> DocValue {
        let Some(meta) = self.doc_values_meta() else { return DocValue::Missing };
        if doc < 0 {
            return DocValue::Missing;
        }
        let doc_index = doc as usize;
        if let Some(index) = self.lookup_index_for_doc(&meta, field, doc_index) {
            if let Some(chunk) = self.doc_value_cache.ready(&(field.to_string(), true, index)) {
                return chunk.value_at(doc_index);
            }
        }
        let Some(index) = self.chunk_index_for_doc(&meta, field, doc_index) else { return DocValue::Missing };
        match self.doc_value_cache.ready(&(field.to_string(), false, index)) {
            Some(chunk) => chunk.value_at(doc_index),
            None => DocValue::Missing,
        }
    }

    pub fn doc_value_chunk_ready(&self, field: &str, index: usize) -> Option<Rc<DocValueChunk>> {
        self.doc_value_cache.ready(&(field.to_string(), false, index))
    }

    // ------------------------------------------------------------ sorted doc values

    pub fn doc_value_sort_field(&self, field: &str) -> Option<Value> {
        self.doc_value_sorted_meta().and_then(|meta| meta.get("fields").and_then(|f| f.get(field)).cloned())
    }

    pub async fn load_doc_value_sort_directory(&self, field: &str) -> Result<Option<Rc<DocValueSortDirectory>>> {
        let Some(meta) = self.doc_value_sort_field(field) else { return Ok(None) };
        let Some(file) = path(&meta, &["directory", "file"]).as_str().map(str::to_string) else { return Ok(None) };
        self.doc_value_sort_directories
            .get_or_load(field.to_string(), async {
                let bytes = self.io.fetch_gzip(&self.url(&file)).await?;
                Ok(Some(Rc::new(sort_tree::parse_doc_value_sort_directory(&bytes)?)))
            })
            .await
    }

    pub async fn load_doc_value_sort_pages(
        &self,
        field: &str,
        directory: &DocValueSortDirectory,
        page_indexes: &[usize],
        stats: &mut BatchStats,
    ) -> Result<Vec<Rc<SortPage>>> {
        let mut wanted = Vec::new();
        let mut items = Vec::new();
        for index in page_indexes {
            if wanted.contains(index) {
                continue;
            }
            let Some(page) = directory.pages.get(*index) else { continue };
            wanted.push(*index);
            items.push(((field.to_string(), *index), page.pointer.clone()));
        }
        let verify = self.verify_checksums;
        let field_name = field.to_string();
        let (batch, observed) = self
            .load_batch_observed(
                &self.doc_value_sort_pages,
                items,
                &self.options.range_plans.doc_value_sort_pages,
                |pack| self.url(&format!("doc-values/sorted-packs/{pack}")),
                |key, pointer, slice| {
                    let inflated = inflate_object(&slice, pointer, &format!("doc-value sort page {}:{}", key.0, key.1), verify)?;
                    Ok(Rc::new(sort_tree::decode_doc_value_sort_page(&inflated, Some(&field_name))?))
                },
            )
            .await?;
        stats.wanted += wanted.len();
        stats.fetched += batch.fetched;
        stats.groups += batch.groups;
        // `observed` holds the distinct keys in `wanted` order.
        let mut out = Vec::with_capacity(observed.len());
        for (_, promise) in observed {
            out.push(promise.await?);
        }
        Ok(out)
    }

    // ------------------------------------------------------------ filter bitmaps

    pub fn filter_bitmap_field(&self, field: &str) -> Option<Value> {
        self.filter_bitmaps_meta().and_then(|meta| path(&meta, &["fields", field]).as_object().map(|_| path(&meta, &["fields", field]).clone()))
    }

    pub async fn load_filter_bitmap(&self, field: &str, value: &str) -> Result<Option<Rc<FilterBitmap>>> {
        if self.filter_bitmaps_meta().is_none() {
            self.ensure_filter_bitmap_manifest().await?;
        }
        let Some(field_meta) = self.filter_bitmap_field(field) else { return Ok(None) };
        let entry = path(&field_meta, &["values", value]).clone();
        if !entry.is_object() {
            return Ok(None);
        }
        let pointer = crate::codec::pointer_from_json(&entry);
        let verify = self.verify_checksums;
        self.filter_bitmap_cache
            .get_or_load((field.to_string(), value.to_string()), async {
                let buffer = self.io.fetch_range(&self.url(&format!("filter-bitmaps/packs/{}", pointer.pack)), pointer.offset, pointer.length).await?;
                let inflated = inflate_object(&buffer, &pointer, &format!("filter bitmap {field}:{value}"), verify)?;
                Ok(Some(Rc::new(filter_bitmap::parse_filter_bitmap(inflated)?)))
            })
            .await
    }

    /// Plain gzip fetch for lane modules.
    pub async fn fetch_gzip_path(&self, relative: &str) -> Result<Bytes> {
        self.io.fetch_gzip(&self.url(relative)).await
    }

    pub fn inflate_bytes(&self, bytes: &[u8]) -> Result<Bytes> {
        self.io.inflate(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashMap;

    use super::*;

    fn entry() -> Rc<TermEntry> {
        Rc::new(TermEntry {
            uid: postings::next_term_entry_uid(),
            df: 1,
            count: 1,
            offset: 0,
            byte_length: 0,
            block_size: 128,
            external: true,
            blocks: Vec::new(),
            superblocks: Vec::new(),
            doc_ranges: None,
            impact_tiers: None,
            block_postings: RefCell::new(HashMap::new()),
            postings: RefCell::new(None),
        })
    }

    /// External blocks are cached per entry. Once a posting segment can be
    /// evicted, a later segment's entry may be allocated at the address a
    /// dropped entry had, so the key must not derive from the address
    /// (`engine_parity`'s cache-pressure pass catches the resulting wrong
    /// postings end to end).
    #[test]
    fn entry_keys_are_never_reused() {
        let first = entry();
        let key = entry_key(&first);
        drop(first);
        let mut seen = std::collections::HashSet::from([key]);
        for _ in 0..256 {
            let next = entry();
            assert!(seen.insert(entry_key(&next)), "a cache key was reused");
        }
    }
}
