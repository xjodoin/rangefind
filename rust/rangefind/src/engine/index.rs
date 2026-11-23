//! Single-index engine state: the closure scope of `createSearch()` in
//! `src/runtime.js`. Lanes live in sibling modules as further
//! `impl IndexEngine` blocks.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use serde_json::Value;

use crate::analysis::Analyzer;
use crate::codec::directory::{DirectoryPage, DirectoryRoot};
use crate::codec::doc_values::{Codes, DocValueChunk, DocValuesMeta};
use crate::codec::filter_bitmap::FilterBitmap;
use crate::codec::postings::{PostingManifest, PostingSegment};
use crate::codec::sort_tree::{DocValueSortDirectory, SortPage};
use crate::error::{Error, Result};
use crate::pointer::Pointer;
use crate::budget::CacheBudget;
use crate::rt::{Promise, PromiseCache, PromiseSlot, Runtime};
use crate::util::json::{path, to_number, truthy};

use super::io::Io;
use super::options::EngineOptions;

/// A paged range directory (`createDirectoryState`).
pub struct DirectoryState {
    pub root_path: String,
    pub pages_path: String,
    pub pack_table: Vec<String>,
    pub root: PromiseSlot<Rc<DirectoryRoot>>,
    pub pages: PromiseCache<String, Rc<DirectoryPage>>,
}

impl DirectoryState {
    pub fn from_manifest(directory: &Value) -> Result<Rc<DirectoryState>> {
        Self::from_manifest_with_pages(directory, PromiseCache::new())
    }

    /// A directory whose decoded pages join `budget` under `name`.
    pub fn budgeted(directory: &Value, budget: &Rc<CacheBudget>, name: &'static str) -> Result<Rc<DirectoryState>> {
        Self::from_manifest_with_pages(directory, PromiseCache::budgeted(budget, name))
    }

    fn from_manifest_with_pages(directory: &Value, page_cache: PromiseCache<String, Rc<DirectoryPage>>) -> Result<Rc<DirectoryState>> {
        let root = directory.get("root").and_then(Value::as_str);
        let pages = directory.get("pages").and_then(Value::as_str);
        let (Some(root), Some(pages)) = (root, pages) else {
            return Err(Error::new("Rangefind index is missing a range directory."));
        };
        let table = directory.get("pack_table").or_else(|| directory.get("packs"));
        let pack_table = table
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| match item {
                        Value::String(text) => text.clone(),
                        other => other.get("file").and_then(Value::as_str).unwrap_or("").to_string(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Rc::new(DirectoryState {
            root_path: root.to_string(),
            pages_path: pages.to_string(),
            pack_table,
            root: PromiseSlot::default(),
            pages: page_cache,
        }))
    }

    /// [`DirectoryState::optional`] with budgeted pages.
    pub fn optional_budgeted(directory: &Value, budget: &Rc<CacheBudget>, name: &'static str) -> Option<Rc<DirectoryState>> {
        if directory.is_null() {
            return None;
        }
        DirectoryState::budgeted(directory, budget, name).ok()
    }

    pub fn optional(directory: &Value) -> Option<Rc<DirectoryState>> {
        if directory.is_null() {
            return None;
        }
        DirectoryState::from_manifest(directory).ok()
    }

    pub fn page_path(&self, file: &str) -> String {
        let mut base = self.pages_path.clone();
        if !base.ends_with('/') {
            base.push('/');
        }
        base.push_str(file);
        base
    }
}

/// A resolved term: the posting segment it lives in and its entry.
#[derive(Clone)]
pub struct TermHit {
    pub term: String,
    pub shard_name: Rc<str>,
    pub shard: Rc<PostingSegment>,
    pub entry: Rc<crate::codec::postings::TermEntry>,
}

/// Engine-wide immutable-after-open numbers derived from the manifest.
pub struct ManifestInfo {
    pub total: usize,
    pub base_shard_depth: usize,
    pub max_shard_depth: usize,
    pub number_fields: HashMap<String, Value>,
    pub boolean_fields: HashSet<String>,
}

pub struct IndexEngine {
    pub manifest: RefCell<Value>,
    pub info: ManifestInfo,
    pub base_url: String,
    pub options: EngineOptions,
    pub io: Rc<Io>,
    pub runtime: Runtime,
    pub analyzer: Rc<Analyzer>,
    pub verify_checksums: bool,
    pub posting_manifest: RefCell<Rc<PostingManifest>>,
    pub term_directory: Rc<DirectoryState>,
    pub query_bundle_directory: Option<Rc<DirectoryState>>,
    pub authority_directory: Option<Rc<DirectoryState>>,
    pub facet_directory: RefCell<Option<Rc<DirectoryState>>>,
    pub block_filter_fields: RefCell<HashSet<String>>,
    pub facet_summary_uint32: Cell<bool>,
    pub top_k_block_budget: usize,

    // Lazy manifests.
    pub full_manifest_loaded: Cell<bool>,
    pub full_manifest: PromiseSlot<bool>,
    pub doc_values: RefCell<Option<Rc<DocValuesMeta>>>,
    pub doc_values_slot: PromiseSlot<Option<Rc<DocValuesMeta>>>,
    pub doc_value_sorted: RefCell<Option<Rc<Value>>>,
    pub doc_value_sorted_slot: PromiseSlot<Option<Rc<Value>>>,
    pub filter_bitmaps: RefCell<Option<Rc<Value>>>,
    pub filter_bitmaps_slot: PromiseSlot<Option<Rc<Value>>>,
    pub facet_dictionaries: RefCell<Option<Rc<Value>>>,
    pub facet_dictionaries_slot: PromiseSlot<Option<Rc<Value>>>,
    pub codes: PromiseSlot<Rc<Codes>>,
    pub segment_manifest: PromiseSlot<Option<Rc<Value>>>,

    // Object caches.
    pub shard_cache: PromiseCache<String, Rc<PostingSegment>>,
    /// Keyed by (`TermEntry::uid`, block index).
    pub external_blocks: PromiseCache<(u64, usize), Rc<Vec<i32>>>,
    pub external_block_bytes: PromiseCache<(u64, usize), crate::util::bytes::Bytes>,
    pub doc_pointer_cache: PromiseCache<usize, Pointer>,
    pub packed_doc_cache: PromiseCache<usize, Value>,
    pub doc_page_pointer_cache: PromiseCache<usize, Pointer>,
    pub doc_page_cache: PromiseCache<usize, Rc<Vec<Value>>>,
    pub doc_value_cache: PromiseCache<(String, bool, usize), Rc<DocValueChunk>>,
    pub doc_value_sort_directories: PromiseCache<String, Option<Rc<DocValueSortDirectory>>>,
    pub doc_value_sort_pages: PromiseCache<(String, usize), Rc<SortPage>>,
    pub filter_bitmap_cache: PromiseCache<(String, String), Option<Rc<FilterBitmap>>>,
    pub facet_dictionary_cache: PromiseCache<String, Rc<Vec<Value>>>,
    pub query_bundle_cache: PromiseCache<String, Option<Rc<crate::codec::query_bundle::QueryBundle>>>,

    // Query plan cache (`resolveQueryPlan`).
    pub query_plan_cache: RefCell<VecDeque<(String, Promise<Rc<crate::analysis::QueryPlan>>)>>,
    pub seen_query_plans: RefCell<VecDeque<String>>,

    /// Extension slots for lanes implemented in other modules (geo,
    /// vectors, sort replicas, authority, ...). Keyed by lane name.
    pub extensions: RefCell<HashMap<&'static str, Rc<dyn std::any::Any>>>,
}

fn usize_or(value: &Value, fallback: usize) -> usize {
    let number = to_number(value);
    if truthy(value) && number.is_finite() && number >= 0.0 {
        number as usize
    } else {
        fallback
    }
}

impl IndexEngine {
    pub fn new(manifest: Value, base_url: String, options: EngineOptions, io: Rc<Io>, runtime: Runtime) -> Result<Self> {
        let analyzer = Rc::new(crate::analysis::analyzer_from_manifest(&manifest)?);
        let verify_checksums = options.verify_checksums
            && (truthy(path(&manifest, &["features", "checksummedObjects"])) || truthy(path(&manifest, &["object_store", "checksum"])));
        let term_directory = DirectoryState::budgeted(path(&manifest, &["directory"]), &io.cache_budget, "termDirectoryPages")?;
        let query_bundle_directory =
            DirectoryState::optional_budgeted(path(&manifest, &["query_bundles", "directory"]), &io.cache_budget, "bundleDirectoryPages");
        let authority_directory =
            DirectoryState::optional_budgeted(path(&manifest, &["authority", "directory"]), &io.cache_budget, "authorityDirectoryPages");
        let facet_directory = DirectoryState::optional(path(&manifest, &["facet_dictionaries", "directory"]));
        let total = usize_or(path(&manifest, &["total"]), 0);
        let base_shard_depth = usize_or(path(&manifest, &["stats", "base_shard_depth"]), 3);
        let max_shard_depth = usize_or(
            path(&manifest, &["stats", "max_shard_depth"]),
            usize_or(path(&manifest, &["stats", "base_shard_depth"]), 5),
        );
        let number_fields = manifest
            .get("numbers")
            .and_then(Value::as_array)
            .map(|fields| {
                fields
                    .iter()
                    .filter_map(|field| field.get("name").and_then(Value::as_str).map(|name| (name.to_string(), field.clone())))
                    .collect()
            })
            .unwrap_or_default();
        let boolean_fields = manifest
            .get("booleans")
            .and_then(Value::as_array)
            .map(|fields| fields.iter().filter_map(|field| field.get("name").and_then(Value::as_str).map(str::to_string)).collect())
            .unwrap_or_default();
        let default_budget = if to_number(path(&manifest, &["total"])) >= 1_000_000.0 { 128 } else { 0 };
        let top_k_block_budget = options.top_k_block_budget.unwrap_or(default_budget);
        let doc_values = match manifest.get("doc_values") {
            Some(value) if value.is_object() => Some(Rc::new(crate::codec::doc_values::doc_values_meta_from_json(value))),
            _ => None,
        };
        let full_manifest_loaded = !truthy(path(&manifest, &["lazy_manifests", "full"]));
        let budget = io.cache_budget.clone();
        let engine = IndexEngine {
            info: ManifestInfo { total, base_shard_depth, max_shard_depth, number_fields, boolean_fields },
            posting_manifest: RefCell::new(Rc::new(PostingManifest::from_manifest(&manifest))),
            block_filter_fields: RefCell::new(block_filter_names(&manifest)),
            facet_summary_uint32: Cell::new(path(&manifest, &["features", "facetSummaryUint32"]) == &Value::Bool(true)),
            doc_value_sorted: RefCell::new(manifest.get("doc_value_sorted").filter(|v| v.is_object()).cloned().map(Rc::new)),
            filter_bitmaps: RefCell::new(manifest.get("filter_bitmaps").filter(|v| v.is_object()).cloned().map(Rc::new)),
            facet_dictionaries: RefCell::new(manifest.get("facet_dictionaries").filter(|v| v.is_object()).cloned().map(Rc::new)),
            manifest: RefCell::new(manifest),
            base_url,
            options,
            io,
            runtime,
            analyzer,
            verify_checksums,
            term_directory,
            query_bundle_directory,
            authority_directory,
            facet_directory: RefCell::new(facet_directory),
            top_k_block_budget,
            full_manifest_loaded: Cell::new(full_manifest_loaded),
            full_manifest: PromiseSlot::default(),
            doc_values: RefCell::new(doc_values),
            doc_values_slot: PromiseSlot::default(),
            doc_value_sorted_slot: PromiseSlot::default(),
            filter_bitmaps_slot: PromiseSlot::default(),
            facet_dictionaries_slot: PromiseSlot::default(),
            codes: PromiseSlot::default(),
            segment_manifest: PromiseSlot::default(),
            shard_cache: PromiseCache::budgeted(&budget, "postingSegments"),
            external_blocks: PromiseCache::budgeted(&budget, "postingBlocks"),
            external_block_bytes: PromiseCache::budgeted(&budget, "postingBlockBytes"),
            doc_pointer_cache: PromiseCache::budgeted(&budget, "docPointers"),
            packed_doc_cache: PromiseCache::budgeted(&budget, "docs"),
            doc_page_pointer_cache: PromiseCache::budgeted(&budget, "docPagePointers"),
            doc_page_cache: PromiseCache::budgeted(&budget, "docPages"),
            doc_value_cache: PromiseCache::budgeted(&budget, "docValues"),
            doc_value_sort_directories: PromiseCache::budgeted(&budget, "docValueSortDirectories"),
            doc_value_sort_pages: PromiseCache::budgeted(&budget, "docValueSortPages"),
            filter_bitmap_cache: PromiseCache::budgeted(&budget, "filterBitmaps"),
            facet_dictionary_cache: PromiseCache::budgeted(&budget, "facetDictionaries"),
            query_bundle_cache: PromiseCache::budgeted(&budget, "queryBundles"),
            query_plan_cache: RefCell::new(VecDeque::new()),
            seen_query_plans: RefCell::new(VecDeque::new()),
            extensions: RefCell::new(HashMap::new()),
        };
        Ok(engine)
    }

    pub fn manifest_value(&self) -> Value {
        self.manifest.borrow().clone()
    }

    /// `manifest?.a?.b` on the live (possibly merged) manifest.
    pub fn manifest_at(&self, keys: &[&str]) -> Value {
        path(&self.manifest.borrow(), keys).clone()
    }

    pub fn total(&self) -> usize {
        self.info.total
    }

    pub fn url(&self, relative: &str) -> String {
        crate::transport::resolve_url(&self.base_url, relative)
    }

    pub fn doc_values_meta(&self) -> Option<Rc<DocValuesMeta>> {
        self.doc_values.borrow().clone()
    }

    pub fn doc_value_sorted_meta(&self) -> Option<Rc<Value>> {
        self.doc_value_sorted.borrow().clone()
    }

    pub fn filter_bitmaps_meta(&self) -> Option<Rc<Value>> {
        self.filter_bitmaps.borrow().clone()
    }

    pub fn extension<T: 'static>(&self, key: &'static str) -> Option<Rc<T>> {
        self.extensions.borrow().get(key).cloned().and_then(|value| value.downcast::<T>().ok())
    }

    pub fn set_extension<T: 'static>(&self, key: &'static str, value: Rc<T>) {
        self.extensions.borrow_mut().insert(key, value);
    }

    /// The memory budget this engine's object caches share.
    pub fn cache_budget(&self) -> &Rc<crate::budget::CacheBudget> {
        &self.io.cache_budget
    }

    /// Re-derive the state `ensureFullManifest` refreshes after merging.
    pub(crate) fn refresh_from_manifest(&self) -> Result<()> {
        let manifest = self.manifest.borrow();
        if let Some(value) = manifest.get("doc_values").filter(|v| v.is_object()) {
            *self.doc_values.borrow_mut() = Some(Rc::new(crate::codec::doc_values::doc_values_meta_from_json(value)));
        }
        *self.doc_value_sorted.borrow_mut() = manifest.get("doc_value_sorted").filter(|v| v.is_object()).cloned().map(Rc::new);
        *self.filter_bitmaps.borrow_mut() = manifest.get("filter_bitmaps").filter(|v| v.is_object()).cloned().map(Rc::new);
        let facet_dictionaries = manifest.get("facet_dictionaries").filter(|v| v.is_object()).cloned().map(Rc::new);
        *self.facet_directory.borrow_mut() =
            facet_dictionaries.as_ref().and_then(|meta| DirectoryState::optional(path(meta, &["directory"])));
        *self.facet_dictionaries.borrow_mut() = facet_dictionaries;
        *self.block_filter_fields.borrow_mut() = block_filter_names(&manifest);
        self.facet_summary_uint32.set(path(&manifest, &["features", "facetSummaryUint32"]) == &Value::Bool(true));
        *self.posting_manifest.borrow_mut() = Rc::new(PostingManifest::from_manifest(&manifest));
        Ok(())
    }
}

fn block_filter_names(manifest: &Value) -> HashSet<String> {
    manifest
        .get("block_filters")
        .and_then(Value::as_array)
        .map(|filters| filters.iter().filter_map(|f| f.get("name").and_then(Value::as_str).map(str::to_string)).collect())
        .unwrap_or_default()
}

/// `minShouldMatchFor`.
pub fn min_should_match_for(count: usize) -> usize {
    if count <= 4 {
        count
    } else {
        count - count / 4
    }
}
