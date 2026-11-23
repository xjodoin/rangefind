//! Sort replicas: `runSortReplicaTextSearch` and the `sortReplica*`
//! helpers of `src/runtime.js` (replica plan keys, term directories, rank
//! maps, rank-ordered doc pointers/packs/pages, ranked and stop states).
//!
//! A replica re-indexes the postings of one sort order with documents
//! renumbered by sort rank, so a text query walks blocks in sort order and
//! stops as soon as no unread block can tie or beat the k-th row.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::codec::doc_pages::{decode_doc_page_columns, DOC_PAGE_ENCODING};
use crate::codec::doc_pointers::{decode_doc_page_pointer_record, decode_doc_pointer_record, DocPointerMeta};
use crate::codec::pointer_from_json;
use crate::codec::postings::{self, PostingManifest, PostingSegment, TermEntry};
use crate::error::{Error, Result};
use crate::pointer::{group_ranges, Pointer, RangePlan};
use crate::rt::PromiseCache;
use crate::util::bytes::Bytes;
use crate::util::json::{num, path, to_number, truthy};

use super::docs::{string_table, PreferDocPages, ResultContext, Row};
use super::index::{min_should_match_for, DirectoryState, IndexEngine, TermHit};
use super::io::inflate_object;
use super::options::SKIP_MAX_TERMS;
use super::search::SortPlan;
use super::text::{empty_text_response, Accumulator, ProofStats, TextRequest};

const STATE_KEY: &str = "sortReplicas";

/// `{ start, count, docs, values }` of a decoded rank-map chunk.
struct RankChunk {
    start: usize,
    docs: Vec<u32>,
    values: Vec<f64>,
}

#[derive(Clone, Copy)]
struct RankInfo {
    doc: u32,
    value: f64,
}

/// Replica caches (`sortReplica*Cache`), keyed by replica id.
struct ReplicaState {
    directories: RefCell<HashMap<String, Rc<DirectoryState>>>,
    shards: PromiseCache<(String, String), Rc<PostingSegment>>,
    /// Keyed by (`TermEntry::uid`, block index).
    blocks: PromiseCache<(u64, usize), Rc<Vec<i32>>>,
    ranks: PromiseCache<(String, usize), Rc<RankChunk>>,
    doc_pointers: PromiseCache<(String, usize), Pointer>,
    packed_docs: PromiseCache<(String, usize), Value>,
    doc_page_pointers: PromiseCache<(String, usize), Pointer>,
    doc_pages: PromiseCache<(String, usize), Rc<Vec<Value>>>,
}

#[derive(Default)]
struct RankStats {
    lookups: usize,
    chunks_wanted: usize,
    chunks_fetched: usize,
    chunk_fetch_groups: usize,
}

#[derive(Default)]
struct DocPackStats {
    pointer_lookups: usize,
    pointer_fetches: usize,
    pointer_fetch_groups: usize,
    fetches: usize,
    fetch_groups: usize,
    planned_fetch_groups: usize,
    planned_fetch_bytes: u64,
    skipped_reason: &'static str,
}

#[derive(Default)]
struct DocPageStats {
    lookups: usize,
    pages_wanted: usize,
    pages_fetched: usize,
    fetch_groups: usize,
    planned_fetch_groups: usize,
    planned_fetch_bytes: u64,
    skipped_reason: &'static str,
    pointer_pages_wanted: usize,
    pointer_pages_fetched: usize,
    pointer_fetch_groups: usize,
}

/// `sortReplicaRankedState` / `sortReplicaStopState`.
struct RankedState {
    eligible: Vec<Row>,
    rank_info: HashMap<i32, RankInfo>,
    stop: bool,
    exhausted: bool,
    boundary_sort_value: Option<f64>,
}

struct Cursor {
    hit: TermHit,
    is_base: bool,
    block_index: usize,
}

/// A replica's string id (`replica.id`) as used in paths and cache keys.
fn replica_id(replica: &Value) -> String {
    match replica.get("id") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Null) | None => "undefined".into(),
        Some(other) => other.to_string(),
    }
}

/// `(value || fallback).replace(/\/?$/u, "/")`.
fn dir_path(value: &Value, fallback: String) -> String {
    let mut out = match value {
        Value::String(text) if !text.is_empty() => text.clone(),
        _ => fallback,
    };
    if !out.ends_with('/') {
        out.push('/');
    }
    out
}

fn positive_int(value: &Value) -> Option<usize> {
    let n = to_number(value);
    if truthy(value) && n.is_finite() && n > 0.0 {
        Some(n as usize)
    } else {
        None
    }
}

/// `sortReplicaPlanKey`.
pub fn sort_replica_plan_key(sort: &SortPlan) -> String {
    if sort.field.is_empty() {
        return String::new();
    }
    format!("{}:{}", sort.field, if sort.desc { "desc" } else { "asc" })
}

/// `compareKnownSortRows`.
fn compare_known_sort_rows(a: &Row, b: &Row, desc: bool, values: &HashMap<i32, f64>) -> std::cmp::Ordering {
    let by_score = || b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0));
    match (values.get(&a.0), values.get(&b.0)) {
        (None, None) => by_score(),
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(left), Some(right)) if left != right => {
            let ordering = if desc { right.partial_cmp(left) } else { left.partial_cmp(right) };
            ordering.unwrap_or(std::cmp::Ordering::Equal)
        }
        _ => by_score(),
    }
}

/// `decodeSortReplicaRankChunk`.
fn decode_rank_chunk(bytes: &[u8], meta: &Value) -> RankChunk {
    let count = {
        let n = to_number(meta.get("count").unwrap_or(&Value::Null));
        if n.is_finite() && n > 0.0 {
            n.floor() as usize
        } else {
            0
        }
    };
    let start = positive_int(meta.get("start").unwrap_or(&Value::Null)).unwrap_or(0);
    let mut docs = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        let offset = index * 12;
        let doc = bytes.get(offset..offset + 4).map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]])).unwrap_or(0);
        let value = bytes
            .get(offset + 4..offset + 12)
            .map(|b| f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
            .unwrap_or(f64::NAN);
        docs.push(doc);
        values.push(value);
    }
    RankChunk { start, docs, values }
}

fn unique_in_range(ranks: &[i32], total: f64) -> Vec<usize> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for &rank in ranks {
        if rank >= 0 && (rank as f64) < total && seen.insert(rank) {
            out.push(rank as usize);
        }
    }
    out
}

fn planned_bytes<T>(groups: &[crate::pointer::RangeGroup<T>]) -> u64 {
    groups.iter().map(|group| group.end.saturating_sub(group.start)).sum()
}

/// A term entry's cache identity (see `loaders::entry_key`).
fn entry_key(entry: &TermEntry) -> u64 {
    entry.uid
}

impl crate::budget::CacheCost for RankChunk {
    fn cache_cost(&self) -> usize {
        std::mem::size_of::<RankChunk>() + self.docs.capacity() * 4 + self.values.capacity() * 8
    }
}

impl IndexEngine {
    fn replica_state(&self) -> Rc<ReplicaState> {
        if let Some(state) = self.extension::<ReplicaState>(STATE_KEY) {
            return state;
        }
        let budget = self.cache_budget();
        let state = Rc::new(ReplicaState {
            directories: RefCell::new(HashMap::new()),
            shards: PromiseCache::budgeted(budget, "replicaPostingSegments"),
            blocks: PromiseCache::budgeted(budget, "replicaPostingBlocks"),
            ranks: PromiseCache::budgeted(budget, "replicaRankChunks"),
            doc_pointers: PromiseCache::budgeted(budget, "replicaDocPointers"),
            packed_docs: PromiseCache::budgeted(budget, "replicaDocs"),
            doc_page_pointers: PromiseCache::budgeted(budget, "replicaDocPagePointers"),
            doc_pages: PromiseCache::budgeted(budget, "replicaDocPages"),
        });
        self.set_extension(STATE_KEY, state.clone());
        state
    }

    /// `sortReplicaForPlan`. Replicas from builders that ranked missing
    /// values as 0 are ignored; current ones (`missing_values: "last"`) rank
    /// them after every value with a NaN rank-map value.
    fn sort_replica_for_plan(&self, sort: &SortPlan) -> Option<Value> {
        let replicas = self.manifest_at(&["sort_replicas", "replicas"]);
        let replica = replicas.get(sort_replica_plan_key(sort))?;
        if truthy(replica) && replica.get("missing_values").and_then(Value::as_str) == Some("last") {
            Some(replica.clone())
        } else {
            None
        }
    }

    /// `sortReplicaDirectoryState`.
    fn sort_replica_directory_state(&self, replica: &Value) -> Option<Rc<DirectoryState>> {
        let key = [replica.get("id"), replica.get("key")]
            .into_iter()
            .flatten()
            .find(|v| truthy(v))
            .map(|v| v.as_str().map(str::to_string).unwrap_or_else(|| v.to_string()))?;
        let directory = path(replica, &["terms", "directory"]);
        if !truthy(directory) {
            return None;
        }
        let state = self.replica_state();
        if let Some(existing) = state.directories.borrow().get(&key) {
            return Some(existing.clone());
        }
        let created = DirectoryState::budgeted(directory, self.cache_budget(), "replicaDirectoryPages").ok()?;
        state.directories.borrow_mut().insert(key, created.clone());
        Some(created)
    }

    /// `sortReplicaPostingManifest`.
    fn sort_replica_posting_manifest(&self, replica: &Value) -> PostingManifest {
        let base = self.posting_manifest.borrow().clone();
        PostingManifest {
            block_filters: Rc::from(Vec::new()),
            posting_block_packs: string_table(Some(path(replica, &["terms", "block_pack_table"]))),
            object_pointers: base.object_pointers,
        }
    }

    /// `loadSortReplicaShards`.
    async fn load_sort_replica_shards(&self, replica: &Value, shards: &[(String, Pointer)]) -> Result<Vec<(String, Rc<PostingSegment>)>> {
        let id = replica_id(replica);
        let state = self.replica_state();
        let manifest = self.sort_replica_posting_manifest(replica);
        let verify = self.verify_checksums;
        let terms_path = dir_path(path(replica, &["terms", "packs_path"]), format!("sort-replicas/{id}/terms/packs"));
        let items: Vec<((String, String), Pointer)> = shards.iter().map(|(shard, entry)| ((id.clone(), shard.clone()), entry.clone())).collect();
        self.load_batch(
            &state.shards,
            items,
            &self.options.range_plans.default,
            |pack| self.url(&format!("{terms_path}{pack}")),
            |key, pointer, slice| {
                let inflated = inflate_object(&slice, pointer, &format!("sort replica segment {}:{}", key.0, key.1), verify)?;
                Ok(Rc::new(postings::parse_posting_segment(inflated, &manifest)?))
            },
        )
        .await?;
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        for (shard, _) in shards {
            if !seen.insert(shard.clone()) {
                continue;
            }
            if let Some(promise) = state.shards.get(&(id.clone(), shard.clone())) {
                out.push((shard.clone(), promise.await?));
            }
        }
        Ok(out)
    }

    /// `sortReplicaTermEntries`.
    async fn sort_replica_term_entries(&self, terms: &[String], replica: &Value) -> Result<Vec<TermHit>> {
        let Some(directory) = self.sort_replica_directory_state(replica) else { return Ok(Vec::new()) };
        let stats = |key: &str| self.manifest_at(&["stats", key]);
        let base_depth = positive_int(replica.get("base_shard_depth").unwrap_or(&Value::Null))
            .or_else(|| positive_int(&stats("base_shard_depth")))
            .unwrap_or(3);
        let max_depth = positive_int(replica.get("max_shard_depth").unwrap_or(&Value::Null))
            .or_else(|| positive_int(&stats("max_shard_depth")))
            .or_else(|| positive_int(&stats("base_shard_depth")))
            .unwrap_or(5);
        let mut buckets: Vec<(String, Pointer, Vec<String>)> = Vec::new();
        for term in terms {
            let Some((shard, entry)) = self.resolve_directory_shard(term, &directory, base_depth, max_depth).await? else { continue };
            match buckets.iter_mut().find(|bucket| bucket.0 == shard) {
                Some(bucket) => bucket.2.push(term.clone()),
                None => buckets.push((shard, entry, vec![term.clone()])),
            }
        }
        let requests: Vec<(String, Pointer)> = buckets.iter().map(|(shard, entry, _)| (shard.clone(), entry.clone())).collect();
        let loaded = self.load_sort_replica_shards(replica, &requests).await?;
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

    /// `decodeEntryBlockBatch` for one replica block: (rows, fetched,
    /// groups, wanted).
    async fn decode_sort_replica_block(&self, replica: &Value, hit: &TermHit, block_index: usize) -> Result<(Rc<Vec<i32>>, usize, usize, usize)> {
        if block_index >= hit.entry.blocks.len() {
            return Ok((Rc::new(Vec::new()), 0, 0, 0));
        }
        if !hit.entry.external {
            return Ok((postings::decode_posting_block(&hit.shard, &hit.entry, block_index)?, 0, 0, 1));
        }
        let state = self.replica_state();
        let key = (entry_key(&hit.entry), block_index);
        let id = replica_id(replica);
        let block_path = dir_path(path(replica, &["terms", "block_packs_path"]), format!("sort-replicas/{id}/terms/block-packs"));
        let verify = self.verify_checksums;
        let block = &hit.entry.blocks[block_index];
        let codec = block.codec;
        let mut wanted = 0;
        let mut fetched = 0;
        let mut groups = 0;
        match block.range.clone() {
            Some(range) => {
                let batch = self
                    .load_batch(
                        &state.blocks,
                        vec![(key, range)],
                        &self.options.range_plans.posting_blocks,
                        |pack| self.url(&format!("{block_path}{pack}")),
                        |key, pointer, slice| {
                            let inflated = inflate_object(&slice, pointer, &format!("posting block {}", key.1), verify)?;
                            Ok(Rc::new(postings::decode_posting_bytes(&inflated, codec)))
                        },
                    )
                    .await?;
                wanted += batch.wanted;
                fetched += batch.fetched;
                groups += batch.groups;
            }
            None => wanted += 1,
        }
        let rows = match state.blocks.get(&key) {
            Some(promise) => promise.await?,
            None => Rc::new(Vec::new()),
        };
        Ok((rows, fetched, groups, wanted))
    }

    /// `loadSortReplicaRankChunks` + `sortReplicaRankRows`.
    async fn sort_replica_rank_rows(&self, replica: &Value, ranks: &[i32], stats: &mut RankStats) -> Result<HashMap<i32, RankInfo>> {
        let rank_map = path(replica, &["rank_map"]);
        let total_value = match rank_map.get("total") {
            Some(v) if !v.is_null() => v.clone(),
            _ => replica.get("total").filter(|v| !v.is_null()).cloned().unwrap_or(json!(0)),
        };
        let total = to_number(&total_value).max(0.0);
        let chunk_size = {
            let configured = rank_map.get("chunk_size").unwrap_or(&Value::Null);
            let n = if truthy(configured) { to_number(configured) } else if total != 0.0 { total } else { 1.0 };
            n.max(1.0)
        };
        let wanted_ranks = unique_in_range(ranks, total);
        stats.lookups += wanted_ranks.len();
        let chunk_of = |rank: usize| (rank as f64 / chunk_size).floor() as usize;
        let mut chunk_indexes: Vec<usize> = Vec::new();
        for rank in &wanted_ranks {
            let chunk = chunk_of(*rank);
            if !chunk_indexes.contains(&chunk) {
                chunk_indexes.push(chunk);
            }
        }
        let id = replica_id(replica);
        let state = self.replica_state();
        let chunks = rank_map.get("chunks").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut items = Vec::new();
        let mut metas: HashMap<usize, Value> = HashMap::new();
        for chunk_index in &chunk_indexes {
            let Some(meta) = chunks.get(*chunk_index).filter(|m| truthy(m)) else { continue };
            metas.insert(*chunk_index, meta.clone());
            items.push(((id.clone(), *chunk_index), pointer_from_json(meta)));
        }
        let rank_path = dir_path(rank_map.get("packs_path").unwrap_or(&Value::Null), format!("sort-replicas/{id}/rank-packs"));
        let verify = self.verify_checksums;
        let batch = self
            .load_batch(
                &state.ranks,
                items,
                &self.options.range_plans.sort_replica_rank_maps,
                |pack| self.url(&format!("{rank_path}{pack}")),
                |key, pointer, slice| {
                    let inflated = inflate_object(&slice, pointer, &format!("sort replica rank {}:{}", key.0, key.1), verify)?;
                    Ok(Rc::new(decode_rank_chunk(&inflated, &metas[&key.1])))
                },
            )
            .await?;
        stats.chunks_wanted += batch.wanted;
        stats.chunks_fetched += batch.fetched;
        stats.chunk_fetch_groups += batch.groups;
        let mut out = HashMap::new();
        for rank in wanted_ranks {
            let Some(promise) = state.ranks.get(&(id.clone(), chunk_of(rank))) else { continue };
            let chunk = promise.await?;
            if rank < chunk.start {
                continue;
            }
            let offset = rank - chunk.start;
            if offset >= chunk.docs.len() {
                continue;
            }
            out.insert(rank as i32, RankInfo { doc: chunk.docs[offset], value: chunk.values[offset] });
        }
        Ok(out)
    }

    /// `loadSortReplicaDocPointers`: the wanted ranks.
    async fn load_sort_replica_doc_pointers(&self, replica: &Value, ranks: &[i32], stats: &mut DocPackStats) -> Result<Vec<usize>> {
        let id = replica_id(replica);
        let pointer_value = path(replica, &["doc_packs", "pointers"]);
        let Some(file) = pointer_value.get("file").and_then(Value::as_str).filter(|f| !f.is_empty()).map(str::to_string) else {
            return Err(Error::new(format!("Rangefind sort replica {id} is missing doc pointers.")));
        };
        let meta = DocPointerMeta::from_json(pointer_value);
        let pack_table = string_table(pointer_value.get("pack_table"));
        let total = {
            let replica_total = replica.get("total").unwrap_or(&Value::Null);
            let value = if truthy(replica_total) { to_number(replica_total) } else { meta.count as f64 };
            value.max(0.0)
        };
        let wanted = unique_in_range(ranks, total);
        let state = self.replica_state();
        let items: Vec<((String, usize), Pointer)> = wanted
            .iter()
            .map(|rank| ((id.clone(), *rank), Pointer::new(&file, (meta.data_offset + rank * meta.record_bytes) as u64, meta.record_bytes as u64)))
            .collect();
        let batch = self
            .load_batch(&state.doc_pointers, items, &self.options.range_plans.sort_replica_doc_pointers, |pack| self.url(pack), |_, _, slice| {
                decode_doc_pointer_record(&slice, 0, &meta, &pack_table)
            })
            .await?;
        stats.pointer_lookups += wanted.len();
        stats.pointer_fetches += batch.fetched;
        stats.pointer_fetch_groups += batch.groups;
        Ok(wanted)
    }

    /// `loadSortReplicaPackedDocs`.
    async fn load_sort_replica_packed_docs(&self, replica: &Value, ranks: &[i32], stats: &mut DocPackStats) -> Result<Option<HashMap<usize, Value>>> {
        if !truthy(path(replica, &["doc_packs", "pointers", "file"])) {
            return Ok(None);
        }
        let wanted = self.load_sort_replica_doc_pointers(replica, ranks, stats).await?;
        if wanted.is_empty() {
            return Ok(Some(HashMap::new()));
        }
        let id = replica_id(replica);
        let state = self.replica_state();
        let mut items = Vec::new();
        for rank in &wanted {
            let key = (id.clone(), *rank);
            if state.packed_docs.has(&key) || items.iter().any(|(k, _)| *k == key) {
                continue;
            }
            let pointer = state.doc_pointers.get(&key).ok_or_else(|| Error::new("sort replica doc pointer missing"))?.await?;
            items.push((key, pointer));
        }
        let plan: &RangePlan = &self.options.range_plans.sort_replica_docs;
        let groups = group_ranges(items.iter().map(|(key, pointer)| (pointer.clone(), key.clone())).collect(), plan);
        let planned = planned_bytes(&groups);
        let max_groups = self.raw_limit("sortReplicaDocMaxFetchGroups", 12.0);
        let max_bytes = self.raw_limit("sortReplicaDocMaxFetchBytes", 256.0 * 1024.0);
        if groups.len() as f64 > max_groups || planned as f64 > max_bytes {
            stats.skipped_reason = if groups.len() as f64 > max_groups { "fetch_groups" } else { "fetch_bytes" };
            stats.planned_fetch_groups = groups.len();
            stats.planned_fetch_bytes = planned;
            return Ok(None);
        }
        stats.planned_fetch_groups = groups.len();
        stats.planned_fetch_bytes = planned;
        stats.fetches += items.len();
        stats.fetch_groups += groups.len();
        let doc_path = dir_path(path(replica, &["doc_packs", "packs_path"]), format!("sort-replicas/{id}/docs/packs"));
        let verify = self.verify_checksums;
        self.load_batch(&state.packed_docs, items, plan, |pack| self.url(&format!("{doc_path}{pack}")), |key, pointer, slice: Bytes| {
            let inflated = inflate_object(&slice, pointer, &format!("sort replica doc {}:{}", key.0, key.1), verify)?;
            Ok(serde_json::from_slice::<Value>(&inflated)?)
        })
        .await?;
        let mut out = HashMap::new();
        for rank in wanted {
            if let Some(promise) = state.packed_docs.get(&(id.clone(), rank)) {
                let doc = promise.await?;
                if truthy(&doc) {
                    out.insert(rank, doc);
                }
            }
        }
        Ok(Some(out))
    }

    /// `Math.max(1, Number(options[key] || fallback))`.
    fn raw_limit(&self, key: &str, fallback: f64) -> f64 {
        let value = self.options.get(key);
        let n = if truthy(value) { to_number(value) } else { fallback };
        if n.is_nan() {
            n
        } else {
            n.max(1.0)
        }
    }

    fn sort_replica_doc_page_size(replica: &Value) -> usize {
        let n = to_number(path(replica, &["doc_pages", "page_size"]));
        if n.is_finite() && n >= 1.0 {
            n as usize
        } else {
            1
        }
    }

    /// `loadSortReplicaDocPages`.
    async fn load_sort_replica_doc_pages(&self, replica: &Value, ranks: &[i32], stats: &mut DocPageStats) -> Result<Option<HashMap<usize, Value>>> {
        let pages = path(replica, &["doc_pages"]);
        if !truthy(path(pages, &["pointers", "file"])) {
            return Ok(None);
        }
        let encoding = pages.get("encoding").and_then(Value::as_str).unwrap_or("");
        if encoding != DOC_PAGE_ENCODING {
            return Err(Error::new(format!(
                "Unsupported Rangefind sort replica doc page encoding {}.",
                if encoding.is_empty() { "unknown" } else { encoding }
            )));
        }
        let id = replica_id(replica);
        let page_size = Self::sort_replica_doc_page_size(replica);
        let total = match pages.get("total") {
            Some(v) if !v.is_null() => to_number(v),
            _ => replica.get("total").filter(|v| !v.is_null()).map(to_number).unwrap_or(0.0),
        };
        let wanted_ranks = unique_in_range(ranks, if total.is_nan() { 0.0 } else { total });
        if wanted_ranks.is_empty() {
            return Ok(Some(HashMap::new()));
        }
        let mut page_indexes: Vec<usize> = wanted_ranks.iter().map(|rank| rank / page_size).collect();
        page_indexes.sort_unstable();
        page_indexes.dedup();
        stats.lookups += wanted_ranks.len();
        stats.pages_wanted += page_indexes.len();

        // `loadSortReplicaDocPagePointers`.
        let state = self.replica_state();
        let pointer_value = path(pages, &["pointers"]);
        let file = pointer_value.get("file").and_then(Value::as_str).unwrap_or("").to_string();
        let pointer_meta = DocPointerMeta::from_json(pointer_value);
        let pack_table = string_table(pointer_value.get("pack_table"));
        let mut pointer_items = Vec::new();
        for page in &page_indexes {
            let key = (id.clone(), *page);
            if state.doc_page_pointers.has(&key) {
                continue;
            }
            if *page >= pointer_meta.count {
                return Err(Error::new(format!("Rangefind sort replica doc page {page} is outside {id}.")));
            }
            pointer_items.push((
                key,
                Pointer::new(&file, (pointer_meta.data_offset + page * pointer_meta.record_bytes) as u64, pointer_meta.record_bytes as u64),
            ));
        }
        let batch = self
            .load_batch(
                &state.doc_page_pointers,
                pointer_items,
                &self.options.range_plans.sort_replica_doc_page_pointers,
                |pack| self.url(pack),
                |_, _, slice| decode_doc_page_pointer_record(&slice, 0, &pointer_meta, &pack_table),
            )
            .await?;
        stats.pointer_pages_wanted += page_indexes.len();
        stats.pointer_pages_fetched += batch.fetched;
        stats.pointer_fetch_groups += batch.groups;

        let mut items = Vec::new();
        for page in &page_indexes {
            let key = (id.clone(), *page);
            if state.doc_pages.has(&key) {
                continue;
            }
            let pointer = state.doc_page_pointers.get(&key).ok_or_else(|| Error::new("sort replica doc page pointer missing"))?.await?;
            items.push((key, pointer));
        }
        let plan: &RangePlan = &self.options.range_plans.sort_replica_doc_pages;
        let groups = group_ranges(items.iter().map(|(key, pointer)| (pointer.clone(), key.clone())).collect(), plan);
        let planned = planned_bytes(&groups);
        let max_groups = self.raw_limit("sortReplicaDocPageMaxFetchGroups", 12.0);
        let max_bytes = self.raw_limit("sortReplicaDocPageMaxFetchBytes", 192.0 * 1024.0);
        if groups.len() as f64 > max_groups || planned as f64 > max_bytes {
            stats.skipped_reason = if groups.len() as f64 > max_groups { "fetch_groups" } else { "fetch_bytes" };
            stats.planned_fetch_groups = groups.len();
            stats.planned_fetch_bytes = planned;
            return Ok(None);
        }
        stats.planned_fetch_groups = groups.len();
        stats.planned_fetch_bytes = planned;
        stats.pages_fetched += items.len();
        stats.fetch_groups += groups.len();
        let fields: Vec<Value> = pages.get("fields").and_then(Value::as_array).cloned().unwrap_or_default();
        let page_path = dir_path(pages.get("packs_path").unwrap_or(&Value::Null), format!("sort-replicas/{id}/docs/page-packs"));
        let verify = self.verify_checksums;
        self.load_batch(&state.doc_pages, items, plan, |pack| self.url(&format!("{page_path}{pack}")), |key, pointer, slice| {
            let inflated = inflate_object(&slice, pointer, &format!("sort replica doc page {}:{}", key.0, key.1), verify)?;
            Ok(Rc::new(decode_doc_page_columns(&inflated, &fields, key.1 * page_size)?))
        })
        .await?;
        let mut out = HashMap::new();
        for rank in wanted_ranks {
            let page_index = rank / page_size;
            let Some(promise) = state.doc_pages.get(&(id.clone(), page_index)) else { continue };
            let page = promise.await?;
            if let Some(doc) = page.get(rank - page_index * page_size).filter(|doc| truthy(doc)) {
                out.insert(rank, doc.clone());
            }
        }
        Ok(Some(out))
    }

    /// `sortReplicaRankedState`.
    async fn sort_replica_ranked_state(
        &self,
        replica: &Value,
        acc: &Accumulator,
        min_should_match: usize,
        sort: &SortPlan,
        stats: &mut RankStats,
    ) -> Result<RankedState> {
        let mut eligible: Vec<Row> = acc.eligible(min_should_match).into_iter().map(|(rank, score)| (rank, score as f64)).collect();
        let ranks: Vec<i32> = eligible.iter().map(|row| row.0).collect();
        let rank_info = self.sort_replica_rank_rows(replica, &ranks, stats).await?;
        // `Number.isFinite(value) ? value : null`: a missing value sorts last.
        let values: HashMap<i32, f64> = rank_info.iter().filter(|(_, info)| info.value.is_finite()).map(|(rank, info)| (*rank, info.value)).collect();
        eligible.sort_by(|a, b| compare_known_sort_rows(a, b, sort.desc, &values));
        Ok(RankedState { eligible, rank_info, stop: false, exhausted: false, boundary_sort_value: None })
    }

    /// `sortReplicaStopState`.
    #[allow(clippy::too_many_arguments)]
    async fn sort_replica_stop_state(
        &self,
        replica: &Value,
        cursors: &[Cursor],
        active: &[usize],
        acc: &Accumulator,
        min_should_match: usize,
        k: f64,
        sort: &SortPlan,
        stats: &mut RankStats,
    ) -> Result<RankedState> {
        let mut state = self.sort_replica_ranked_state(replica, acc, min_should_match, sort, stats).await?;
        let Some(boundary_row) = super::text::kth(&state.eligible, k) else { return Ok(state) };
        let boundary = state.rank_info.get(&boundary_row.0).map(|info| info.value).filter(|value| value.is_finite());
        state.boundary_sort_value = boundary;
        let next_ranks: Vec<i32> = active
            .iter()
            .filter_map(|&index| {
                let cursor = &cursors[index];
                cursor.hit.entry.blocks.get(cursor.block_index).map(|block| block.doc_min as i32)
            })
            .collect();
        if next_ranks.is_empty() {
            state.stop = true;
            state.exhausted = true;
            return Ok(state);
        }
        let next_rows = self.sort_replica_rank_rows(replica, &next_ranks, stats).await?;
        // Missing values rank last: past a missing boundary any row may still
        // outscore it, while a missing row never reaches a valued boundary.
        let can_tie_or_beat = match boundary {
            Some(boundary) => next_rows.values().any(|row| {
                row.value.is_finite() && if sort.desc { row.value >= boundary } else { row.value <= boundary }
            }),
            None => true,
        };
        state.stop = !can_tie_or_beat;
        Ok(state)
    }

    /// `runSortReplicaTextSearch`.
    pub(crate) async fn run_sort_replica_text_search(&self, request: &TextRequest, sort: &SortPlan) -> Result<Option<Value>> {
        let Some(replica) = self.sort_replica_for_plan(sort) else { return Ok(None) };
        if request.base_terms.is_empty() || request.terms.len() > SKIP_MAX_TERMS {
            return Ok(None);
        }
        if request.rerank && !self.dependency_terms(&request.base_terms).is_empty() {
            return Ok(None);
        }
        if request.filters.has_filters() {
            return Ok(None);
        }
        let page = request.page;
        let size = request.size;
        let offset = request.offset();
        let k = request.k();
        let entries = self.sort_replica_term_entries(&request.terms, &replica).await?;
        let base_set: HashSet<&str> = request.base_terms.iter().map(String::as_str).collect();
        let min_should_match = min_should_match_for(request.base_terms.len());
        let present: HashSet<&str> = entries.iter().filter(|hit| base_set.contains(hit.term.as_str())).map(|hit| hit.term.as_str()).collect();
        if present.len() < min_should_match.max(1) {
            return Ok(Some(empty_text_response(page, size, request.terms.len(), &entries, request.base_terms.len().saturating_sub(present.len()))));
        }
        let mut cursors: Vec<Cursor> =
            entries.iter().map(|hit| Cursor { hit: hit.clone(), is_base: base_set.contains(hit.term.as_str()), block_index: 0 }).collect();
        if cursors.is_empty() {
            return Ok(Some(empty_text_response(page, size, request.terms.len(), &[], 0)));
        }

        let mut acc = Accumulator::default();
        let mut rank_stats = RankStats::default();
        let mut pack_stats = DocPackStats::default();
        let mut page_stats = DocPageStats::default();
        let mut proof = ProofStats::new(false, true, None);
        let frontier_size = self.options.posting_block_frontier;
        let (mut blocks_decoded, mut postings_decoded, mut postings_accepted) = (0usize, 0usize, 0usize);
        let (mut frontier_batches, mut frontier_blocks, mut frontier_max) = (0usize, 0usize, 0usize);
        let (mut fetched_blocks, mut fetch_groups, mut wanted_blocks) = (0usize, 0usize, 0usize);
        let mut stop_checks = 0usize;
        let mut stopped_by_sort_bound = false;
        let exhausted;
        let final_state;

        loop {
            let mut active: Vec<usize> = (0..cursors.len()).filter(|i| cursors[*i].block_index < cursors[*i].hit.entry.blocks.len()).collect();
            stop_checks += 1;
            proof.attempts += 1;
            let state = self.sort_replica_stop_state(&replica, &cursors, &active, &acc, min_should_match, k, sort, &mut rank_stats).await?;
            if state.stop {
                exhausted = state.exhausted;
                stopped_by_sort_bound = !state.exhausted;
                proof.successes += 1;
                proof.last_failure_reason = "";
                proof.last_threshold = state.boundary_sort_value.filter(|v| *v != 0.0 && !v.is_nan()).map(|v| v as i64).unwrap_or(0);
                proof.last_max_outside_potential = 0;
                final_state = state;
                break;
            }
            if active.is_empty() {
                exhausted = true;
                final_state = self.sort_replica_ranked_state(&replica, &acc, min_should_match, sort, &mut rank_stats).await?;
                proof.successes += 1;
                proof.last_failure_reason = "";
                proof.last_threshold = 0;
                proof.last_max_outside_potential = 0;
                break;
            }
            active.sort_by(|a, b| {
                let left = &cursors[*a].hit.entry.blocks[cursors[*a].block_index];
                let right = &cursors[*b].hit.entry.blocks[cursors[*b].block_index];
                left.doc_min.cmp(&right.doc_min).then(right.max_impact.cmp(&left.max_impact))
            });
            let frontier: Vec<usize> = active.into_iter().take(frontier_size).collect();
            frontier_batches += 1;
            frontier_blocks += frontier.len();
            frontier_max = frontier_max.max(frontier.len());
            for index in frontier {
                let block_index = cursors[index].block_index;
                let hit = cursors[index].hit.clone();
                let (rows, fetched, groups, wanted) = self.decode_sort_replica_block(&replica, &hit, block_index).await?;
                fetched_blocks += fetched;
                fetch_groups += groups;
                wanted_blocks += wanted;
                cursors[index].block_index += 1;
                blocks_decoded += 1;
                postings_decoded += rows.len() / 2;
                // `applySortReplicaBlockRows`.
                let is_base = cursors[index].is_base;
                for pair in rows.chunks_exact(2) {
                    acc.add_score(pair[0], pair[1] as i64);
                    if is_base {
                        acc.add_hit(pair[0]);
                    }
                    postings_accepted += 1;
                }
            }
        }

        let ranked = &final_state.eligible;
        let rows: Vec<Row> = super::text::page_slice(ranked, offset, size);
        let row_ranks: Vec<i32> = rows.iter().map(|row| row.0).collect();
        let mut rank_docs = self.load_sort_replica_doc_pages(&replica, &row_ranks, &mut page_stats).await?;
        let mut rank_doc_lane = if rank_docs.is_some() { "sortReplicaDocPages" } else { "" };
        if rank_docs.is_none() {
            rank_docs = self.load_sort_replica_packed_docs(&replica, &row_ranks, &mut pack_stats).await?;
            rank_doc_lane = if rank_docs.is_some() { "sortReplicaDocPacks" } else { "" };
        }
        let mut context = ResultContext::new(true, PreferDocPages::Yes);
        let results: Vec<Value> = match &rank_docs {
            Some(docs) => {
                context.lane = rank_doc_lane.to_string();
                context.pages = if rank_doc_lane == "sortReplicaDocPages" { page_stats.pages_wanted } else { 0 };
                context.rows = rows.len();
                context.overfetch_docs = if rank_doc_lane == "sortReplicaDocPages" {
                    page_stats.pages_wanted * Self::sort_replica_doc_page_size(&replica)
                } else {
                    rows.len()
                };
                context.adaptive = false;
                context.forced = false;
                rows.iter()
                    .filter_map(|(rank, score)| {
                        let doc = usize::try_from(*rank).ok().and_then(|rank| docs.get(&rank))?;
                        let mut map = match doc {
                            Value::Object(map) => map.clone(),
                            _ => Map::new(),
                        };
                        map.insert("score".into(), num(*score));
                        Some(Value::Object(map))
                    })
                    .collect()
            }
            None => {
                let ranks: Vec<i32> = rows.iter().map(|row| row.0).collect();
                let info = self.sort_replica_rank_rows(&replica, &ranks, &mut rank_stats).await?;
                let mapped: Vec<Row> = rows
                    .iter()
                    .filter_map(|(rank, score)| info.get(rank).or_else(|| final_state.rank_info.get(rank)).map(|i| (i.doc as i32, *score)))
                    .collect();
                self.rows_to_results(&mapped, &mut context).await?
            }
        };
        let shards: HashSet<&str> = entries.iter().map(|hit| &*hit.shard_name).collect();
        let mut stats = Map::new();
        for (key, value) in [
            ("exact", json!(true)),
            ("plannerLane", json!("sortReplicaText")),
            ("topKProven", json!(true)),
            ("totalExact", json!(exhausted)),
            ("tailExhausted", json!(exhausted)),
            ("terms", json!(request.terms.len())),
            ("shards", json!(shards.len())),
            ("blocksDecoded", json!(blocks_decoded)),
            ("postingsDecoded", json!(postings_decoded)),
            ("postingsAccepted", json!(postings_accepted)),
            ("skippedBlocks", json!(0)),
            ("sortReplicaText", json!(true)),
            ("sortReplicaId", replica.get("id").cloned().unwrap_or(Value::Null)),
            ("sortReplicaField", replica.get("field").cloned().unwrap_or(Value::Null)),
            ("sortReplicaDirection", json!(if sort.desc { "desc" } else { "asc" })),
            ("sortReplicaStopReason", json!(if stopped_by_sort_bound { "sort_bound" } else { "exhausted" })),
            ("sortReplicaStopChecks", json!(stop_checks)),
            ("sortReplicaFrontier", json!(frontier_size)),
            ("sortReplicaFrontierBatches", json!(frontier_batches)),
            ("sortReplicaFrontierBlocks", json!(frontier_blocks)),
            ("sortReplicaFrontierMax", json!(frontier_max)),
            ("sortReplicaFetchedBlocks", json!(fetched_blocks)),
            ("sortReplicaFetchGroups", json!(fetch_groups)),
            ("sortReplicaWantedBlocks", json!(wanted_blocks)),
            ("sortReplicaRankLookups", json!(rank_stats.lookups)),
            ("sortReplicaRankChunksWanted", json!(rank_stats.chunks_wanted)),
            ("sortReplicaRankChunksFetched", json!(rank_stats.chunks_fetched)),
            ("sortReplicaRankChunkFetchGroups", json!(rank_stats.chunk_fetch_groups)),
            ("sortReplicaDocPackPointerLookups", json!(pack_stats.pointer_lookups)),
            ("sortReplicaDocPackPointerFetches", json!(pack_stats.pointer_fetches)),
            ("sortReplicaDocPackPointerFetchGroups", json!(pack_stats.pointer_fetch_groups)),
            ("sortReplicaDocPackFetches", json!(pack_stats.fetches)),
            ("sortReplicaDocPackFetchGroups", json!(pack_stats.fetch_groups)),
            ("sortReplicaDocPackPlannedFetchGroups", json!(pack_stats.planned_fetch_groups)),
            ("sortReplicaDocPackPlannedFetchBytes", json!(pack_stats.planned_fetch_bytes)),
            ("sortReplicaDocPackSkippedReason", json!(pack_stats.skipped_reason)),
            ("sortReplicaDocPageLookups", json!(page_stats.lookups)),
            ("sortReplicaDocPagesWanted", json!(page_stats.pages_wanted)),
            ("sortReplicaDocPagesFetched", json!(page_stats.pages_fetched)),
            ("sortReplicaDocPageFetchGroups", json!(page_stats.fetch_groups)),
            ("sortReplicaDocPagePlannedFetchGroups", json!(page_stats.planned_fetch_groups)),
            ("sortReplicaDocPagePlannedFetchBytes", json!(page_stats.planned_fetch_bytes)),
            ("sortReplicaDocPageSkippedReason", json!(page_stats.skipped_reason)),
            ("sortReplicaDocPagePointerPagesWanted", json!(page_stats.pointer_pages_wanted)),
            ("sortReplicaDocPagePointerPagesFetched", json!(page_stats.pointer_pages_fetched)),
            ("sortReplicaDocPagePointerFetchGroups", json!(page_stats.pointer_fetch_groups)),
            ("docValueSortText", json!(false)),
            ("docValuePruning", json!(false)),
            ("sortedTextBlockScheduler", json!(false)),
            ("sortedTextCandidateLookup", json!(false)),
            ("plannerFallbackReason", json!("")),
        ] {
            stats.insert(key.into(), value);
        }
        ProofStats::write(Some(&proof), "", &mut stats);
        context.stats(&mut stats, true);
        for key in ["rerankCandidates", "dependencyFeatures", "dependencyTermsMatched", "dependencyPostingsScanned", "dependencyCandidateMatches"] {
            stats.insert(key.into(), json!(0));
        }
        Ok(Some(json!({
            "total": ranked.len(),
            "page": super::text::json_number(page),
            "size": size,
            "results": results,
            "approximate": !exhausted,
            "stats": Value::Object(stats)
        })))
    }
}
