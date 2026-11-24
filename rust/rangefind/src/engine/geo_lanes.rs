//! Geo lanes from `src/runtime.js`: geo tree loading (`loadGeoTreeRoot`,
//! `loadPackedPages`, `loadGeoBranchPages`, `loadGeoLeafPages`), category
//! cell routing (`geoCellIndexForPlan`, `geoCellRoutedLeaves`,
//! `geoCellDirectoryEntries`, `loadGeoCellBlocks`), leaf traversal
//! (`geoLeafCandidate`, `geoCandidateLeafPages`, `geoRoutedLeafPages`), the
//! tree lanes (`runGeoBrowse` / `runGeoBrowseInner`: box/radius browse,
//! nearest-first with the early-stop proof, route sort), capsule hydration
//! (`rowsToGeoCapsuleResults`), text + geo doc sets
//! (`buildGeoDocSetIfCheap`, `estimateGeoFilterDocValueBytes`), text +
//! distance ordering (`runGeoTextNearestByDocValues` and the geo-sort branch
//! of `executeSearch`) and `attachGeoDistances`.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::geo::{
    box_contains_box_e7, box_intersects_e7, decode_geo_branch_page, decode_geo_cell_block, decode_geo_leaf_page, geo_cell_block,
    geo_cell_block_key, geo_cell_route_key, geo_cells_for_boxes, geo_cells_for_route, haversine_meters_e7, js_round, lat_to_e7_f64,
    lon_to_e7_f64, match_point_to_route, parse_geo_tree_root, point_to_box_distance_meters_e7, point_to_box_max_distance_meters_e7,
    route_match_public, BlockFilter, BlockFilterSummary, FilterSummaries, GeoBox, GeoBranchEntry, GeoBranchPage, GeoCell, GeoCellBlock,
    GeoLeafEntry, GeoLeafPage, GeoTreeRoot, RouteMatch,
};
use crate::pointer::{Pointer, RangePlan};
use crate::rt::{join2, PromiseCache};
use crate::util::bytes::Bytes;
use crate::util::json::{num, to_number, truthy, NULL};

use super::docs::{result_object, PreferDocPages, ResultContext, Row};
use super::filters::{words_match, BitmapStore, BlockFilterPlan, DocFilterPlan, Filters, ValueStore};
use super::geo_plan::GeoPlan;
use super::index::{DirectoryState, IndexEngine};
use super::io::inflate_object;
use super::loaders::BatchStats;

const GEO_LEAF_PAGE_FIRST_BATCH_SIZE: usize = 4;
const GEO_TEXT_LEAF_PAGE_FIRST_BATCH_SIZE: usize = 16;
const GEO_TEXT_DOC_SET_HARD_CAP: u64 = 1_000_000;
const GEO_TEXT_NEAREST_POINT_BYTES_ESTIMATE: f64 = 16.0;
const GEO_TEXT_NEAREST_MIN_TRAVERSAL_BYTES: f64 = (1u64 << 20) as f64;
const GEO_TEXT_SORT_BUDGET_MESSAGE: &str =
    "Rangefind: text geo sort exceeds the geoTextSortMaxDf posting budget; narrow the query or rank by relevance with geo.boost.";

/// Per-engine geo caches (`geoTreeRootCache`, `geoBranchPageCache`,
/// `geoLeafPageCache`, `geoCellDirectoryCache`, `geoCellBlockCache`).
struct GeoState {
    roots: PromiseCache<String, Rc<GeoTreeRoot>>,
    branches: PromiseCache<LeafKey, Rc<GeoBranchPage>>,
    leaves: PromiseCache<LeafKey, Rc<GeoLeafPage>>,
    cell_directories: RefCell<HashMap<String, Rc<DirectoryState>>>,
    cell_blocks: PromiseCache<String, Rc<GeoCellBlock>>,
}

/// `geoTraversalTracking()`.
#[derive(Default, Clone, Copy)]
struct Tracking {
    candidate_leaves: usize,
    candidate_branches: usize,
    leaf_fetch: BatchStats,
    branch_fetch: BatchStats,
}

/// `geoLeafCandidate` result over a leaf or branch entry.
#[derive(Clone)]
struct Candidate<T> {
    item: T,
    min_dist: f64,
    geo_definite: bool,
}

/// `geoCellRoutedLeaves` result.
struct CellRouting {
    leaves: Vec<GeoLeafEntry>,
    ordinals_by_leaf: HashMap<usize, HashSet<usize>>,
    facet: Option<String>,
    level: u32,
    cells: usize,
    blocks_wanted: usize,
    blocks_fetched: usize,
    block_fetch_groups: usize,
}

/// `geoCellIndexForPlan` result.
struct PlannedCellIndex {
    index: Value,
    selected: Vec<u32>,
    facet: Option<String>,
}

struct BestItem {
    doc: i32,
    dist: f64,
    route_match: Option<RouteMatch>,
}

/// Payload stats from `rowsToGeoCapsuleResults` / `rowsToResults`.
struct PayloadStats {
    lane: Option<String>,
    pages: usize,
    overfetch_docs: usize,
    capsule_hits: Option<usize>,
    capsule_misses: Option<usize>,
}

impl PayloadStats {
    fn from_context(context: &ResultContext) -> Self {
        PayloadStats {
            lane: if context.lane.is_empty() { None } else { Some(context.lane.clone()) },
            pages: context.pages,
            overfetch_docs: context.overfetch_docs,
            capsule_hits: None,
            capsule_misses: None,
        }
    }

    fn write(&self, stats: &mut Map<String, Value>) {
        stats.insert("docPayloadLane".into(), self.lane.clone().map(Value::from).unwrap_or(Value::Null));
        stats.insert("docPayloadPages".into(), json!(self.pages));
        stats.insert("docPayloadOverfetchDocs".into(), json!(self.overfetch_docs));
    }
}

fn round_tenth(value: f64) -> f64 {
    js_round(value * 10.0) / 10.0
}

/// `roundedDistanceMeters`.
fn rounded_distance_meters(distance: f64) -> Value {
    num(round_tenth(distance))
}

/// `blockMayPass` over a geo leaf/branch filter summary.
fn geo_block_may_pass(filters: &FilterSummaries, plan: &BlockFilterPlan) -> bool {
    if !plan.active {
        return true;
    }
    let get = |field: &str| filters.iter().find(|(name, _)| name == field).map(|(_, summary)| summary);
    for (field, selected) in &plan.facets {
        if let Some(summary) = get(field) {
            let words: &[u64] = match summary {
                BlockFilterSummary::Facet { words } => words,
                _ => &[],
            };
            if !selected.is_empty() && !words_match(words, selected) {
                return false;
            }
        }
    }
    for (field, range) in &plan.numbers {
        let (min, max) = match get(field) {
            Some(BlockFilterSummary::Number { min: Some(min), max: Some(max) }) => (*min, *max),
            Some(BlockFilterSummary::Range { min, max }) => (*min as f64, *max as f64),
            _ => continue,
        };
        if range.min.map(|v| max < v).unwrap_or(false) || range.max.map(|v| min > v).unwrap_or(false) {
            return false;
        }
    }
    for (field, code) in &plan.booleans {
        let code = *code as f64;
        let (min, max) = match get(field) {
            Some(BlockFilterSummary::Number { min: Some(min), max: Some(max) }) => (*min, *max),
            Some(BlockFilterSummary::Range { min, max }) => (*min as f64, *max as f64),
            _ => continue,
        };
        if max == 0.0 || max.is_nan() {
            continue;
        }
        if max < code || min > code {
            return false;
        }
    }
    true
}

/// `geoLeafCandidate` over a bbox + filter summary: `(minDist, geoDefinite)`.
fn geo_candidate(geo: &GeoPlan, bbox: &GeoBox, filters: Option<&FilterSummaries>, plan: Option<&BlockFilterPlan>) -> Option<(f64, bool)> {
    if let (Some(plan), Some(filters)) = (plan, filters) {
        if !geo_block_may_pass(filters, plan) {
            return None;
        }
    }
    let near = geo.near;
    if let Some(near) = near {
        if let Some(radius) = near.radius_meters {
            let min_dist = point_to_box_distance_meters_e7(near.lat_e7, near.lon_e7, bbox);
            if min_dist > radius {
                return None;
            }
            return Some((min_dist, point_to_box_max_distance_meters_e7(near.lat_e7, near.lon_e7, bbox) <= radius));
        }
    }
    let min_dist = near.map(|n| point_to_box_distance_meters_e7(n.lat_e7, n.lon_e7, bbox)).unwrap_or(0.0);
    if !geo.boxes.is_empty() {
        if !geo.boxes.iter().any(|b| box_intersects_e7(b, bbox)) {
            return None;
        }
        // Route boxes are only a conservative prune.
        let definite = geo.route.is_none() && geo.boxes.iter().any(|b| box_contains_box_e7(b, bbox));
        return Some((min_dist, definite));
    }
    Some((min_dist, true))
}

fn leaf_candidate(geo: &GeoPlan, leaf: &GeoLeafEntry, plan: Option<&BlockFilterPlan>) -> Option<Candidate<GeoLeafEntry>> {
    geo_candidate(geo, &leaf.bbox, leaf.filters.as_ref(), plan).map(|(min_dist, geo_definite)| Candidate { item: leaf.clone(), min_dist, geo_definite })
}

fn branch_candidate(geo: &GeoPlan, branch: &GeoBranchEntry, plan: Option<&BlockFilterPlan>) -> Option<Candidate<GeoBranchEntry>> {
    geo_candidate(geo, &branch.bbox, branch.filters.as_ref(), plan)
        .map(|(min_dist, geo_definite)| Candidate { item: branch.clone(), min_dist, geo_definite })
}

fn sort_by_distance<T>(items: &mut [Candidate<T>], index: impl Fn(&T) -> usize) {
    items.sort_by(|a, b| a.min_dist.partial_cmp(&b.min_dist).unwrap_or(Ordering::Equal).then(index(&a.item).cmp(&index(&b.item))));
}

fn merge_value_stores(primary: Option<ValueStore>, bitmaps: Option<Rc<BitmapStore>>) -> Option<ValueStore> {
    match (primary, bitmaps) {
        (primary, None) => primary,
        (None, Some(bitmaps)) => Some(ValueStore { bitmaps: Some(bitmaps), ..Default::default() }),
        (Some(mut primary), Some(bitmaps)) => {
            primary.bitmaps = Some(bitmaps);
            Some(primary)
        }
    }
}

/// Fields of the doc filter plan the bitmap store does not cover.
fn residual_filter_fields(plan: Option<&DocFilterPlan>, bitmaps: Option<&BitmapStore>) -> Vec<String> {
    let Some(plan) = plan.filter(|p| p.active) else { return Vec::new() };
    let mut out: Vec<String> = Vec::new();
    for field in IndexEngine::filter_plan_fields(Some(plan)) {
        if !out.contains(&field) {
            out.push(field);
        }
    }
    out.retain(|field| !bitmaps.map(|b| b.covered.contains(field)).unwrap_or(false));
    out
}

/// `routeMatchPublic` as the JSON object the runtime attaches.
fn route_match_json(found: &RouteMatch) -> Option<Value> {
    let public = route_match_public(Some(found))?;
    Some(json!({
        "distanceMeters": num(public.distance_meters),
        "progressMeters": num(public.progress_meters),
        "progressRatio": num(public.progress_ratio),
        "segmentIndex": public.segment_index,
        "segmentProgress": num(public.segment_progress),
        "bearingDegrees": num(public.bearing_degrees),
        "rejoinPoint": { "lat": num(public.rejoin_point.lat), "lon": num(public.rejoin_point.lon) }
    }))
}

/// Copy `routeMatch` and its flattened fields onto a result object.
fn attach_route_fields(result: &mut Map<String, Value>, public: Value, rank: Option<f64>) {
    result.insert("routeMatch".into(), public.clone());
    if let Some(rank) = rank {
        result.insert("routeRank".into(), num(round_tenth(rank)));
    }
    for (key, source) in [
        ("routeDistanceMeters", "distanceMeters"),
        ("routeProgressMeters", "progressMeters"),
        ("routeProgressRatio", "progressRatio"),
        ("routeBearingDegrees", "bearingDegrees"),
        ("rejoinPoint", "rejoinPoint"),
    ] {
        result.insert(key.into(), public.get(source).cloned().unwrap_or(Value::Null));
    }
}

/// JS `Number(value)` for a property that may be absent (`undefined` → NaN).
fn number_of_property(object: &Map<String, Value>, key: &str) -> f64 {
    match object.get(key) {
        None => f64::NAN,
        Some(value) => to_number(value),
    }
}

/// Dedupe `(key, pointer)` entries keeping first-occurrence order.
fn unique_entries<K: Eq + std::hash::Hash + Clone>(entries: Vec<(K, Pointer)>) -> Vec<(K, Pointer)> {
    let mut seen = HashSet::new();
    entries.into_iter().filter(|(key, _)| seen.insert(key.clone())).collect()
}

/// Lazily streams `(candidate, leafPage)` pairs: the Rust shape of the
/// `geoCandidateLeafPages` / `geoRoutedLeafPages` async generators.
struct LeafStream<'a> {
    engine: &'a IndexEngine,
    geo: &'a GeoPlan,
    root: Rc<GeoTreeRoot>,
    distance_sorted: bool,
    block_plan: Option<&'a BlockFilterPlan>,
    first_batch: usize,
    max_batch: usize,
    tracking: Tracking,
    queue: VecDeque<Candidate<GeoLeafEntry>>,
    batch_size: usize,
    ready: VecDeque<(Candidate<GeoLeafEntry>, Rc<GeoLeafPage>)>,
    mode: StreamMode,
}

enum StreamMode {
    /// Only the queued leaf candidates remain.
    Leaves,
    /// Two-level tree, index order: branch batches of 1, 2, 4, 8.
    Branches { branches: Vec<Candidate<GeoBranchEntry>>, position: usize, batch: usize, loaded: VecDeque<Rc<GeoBranchPage>> },
    /// Two-level tree, best-first merge across branches.
    Best { branches: Vec<Candidate<GeoBranchEntry>>, next: usize, pool: Vec<Candidate<GeoLeafEntry>> },
}

impl<'a> LeafStream<'a> {
    fn leaf_candidates(&mut self, leaves: &[GeoLeafEntry]) -> Vec<Candidate<GeoLeafEntry>> {
        let mut out: Vec<Candidate<GeoLeafEntry>> = leaves.iter().filter_map(|leaf| leaf_candidate(self.geo, leaf, self.block_plan)).collect();
        self.tracking.candidate_leaves += out.len();
        if self.distance_sorted {
            sort_by_distance(&mut out, |leaf| leaf.index);
        }
        out
    }

    /// `geoCandidateLeafPages(geoPlan, root, distanceSorted, tracking, blockFilterPlan, firstBatchHint)`.
    fn tree(
        engine: &'a IndexEngine,
        geo: &'a GeoPlan,
        root: Rc<GeoTreeRoot>,
        distance_sorted: bool,
        tracking: Tracking,
        block_plan: Option<&'a BlockFilterPlan>,
        first_batch_hint: usize,
    ) -> Self {
        let max_batch = engine.options.geo_leaf_page_batch_size;
        let first_batch = GEO_LEAF_PAGE_FIRST_BATCH_SIZE.max(first_batch_hint).min(max_batch);
        let mut stream = LeafStream {
            engine,
            geo,
            root: root.clone(),
            distance_sorted,
            block_plan,
            first_batch,
            max_batch,
            tracking,
            queue: VecDeque::new(),
            batch_size: first_batch,
            ready: VecDeque::new(),
            mode: StreamMode::Leaves,
        };
        if root.levels == 1 {
            let leaves = root.leaves.as_deref().unwrap_or(&[]);
            stream.queue = stream.leaf_candidates(leaves).into();
            return stream;
        }
        let mut branches: Vec<Candidate<GeoBranchEntry>> =
            root.branches.as_deref().unwrap_or(&[]).iter().filter_map(|branch| branch_candidate(geo, branch, block_plan)).collect();
        stream.tracking.candidate_branches = branches.len();
        if !distance_sorted {
            branches.sort_by_key(|item| item.item.index);
            stream.mode = StreamMode::Branches { branches, position: 0, batch: 1, loaded: VecDeque::new() };
        } else {
            sort_by_distance(&mut branches, |branch| branch.index);
            stream.mode = StreamMode::Best { branches, next: 0, pool: Vec::new() };
        }
        stream
    }

    /// `geoRoutedLeafPages(geoPlan, leaves, distanceSorted, tracking, blockFilterPlan, firstBatchHint)`.
    fn routed(
        engine: &'a IndexEngine,
        geo: &'a GeoPlan,
        root: Rc<GeoTreeRoot>,
        leaves: &[GeoLeafEntry],
        distance_sorted: bool,
        tracking: Tracking,
        block_plan: Option<&'a BlockFilterPlan>,
        first_batch_hint: usize,
    ) -> Self {
        let max_batch = engine.options.geo_leaf_page_batch_size;
        let first_batch = GEO_LEAF_PAGE_FIRST_BATCH_SIZE.max(first_batch_hint).min(max_batch);
        let mut candidates: Vec<Candidate<GeoLeafEntry>> = leaves.iter().filter_map(|leaf| leaf_candidate(geo, leaf, block_plan)).collect();
        let mut tracking = tracking;
        tracking.candidate_leaves += candidates.len();
        if distance_sorted {
            sort_by_distance(&mut candidates, |leaf| leaf.index);
        } else {
            candidates.sort_by_key(|item| item.item.index);
        }
        LeafStream {
            engine,
            geo,
            root,
            distance_sorted,
            block_plan,
            first_batch,
            max_batch,
            tracking,
            queue: candidates.into(),
            batch_size: first_batch,
            ready: VecDeque::new(),
            mode: StreamMode::Leaves,
        }
    }

    async fn load_leaves(&mut self, batch: Vec<Candidate<GeoLeafEntry>>) -> Result<()> {
        let entries: Vec<&GeoLeafEntry> = batch.iter().map(|c| &c.item).collect();
        let pages = self.engine.load_geo_leaf_pages(&self.geo.field, &entries, &mut self.tracking.leaf_fetch).await?;
        let by_index: HashMap<usize, Rc<GeoLeafPage>> = pages.into_iter().collect();
        for candidate in batch {
            let page = by_index.get(&candidate.item.index).cloned().ok_or_else(|| Error::new("Rangefind geo leaf page missing."))?;
            self.ready.push_back((candidate, page));
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<(Candidate<GeoLeafEntry>, Rc<GeoLeafPage>)>> {
        loop {
            if let Some(item) = self.ready.pop_front() {
                return Ok(Some(item));
            }
            if !self.queue.is_empty() {
                let take = self.batch_size.min(self.queue.len());
                let batch: Vec<_> = self.queue.drain(..take).collect();
                self.batch_size = (self.batch_size * 2).min(self.max_batch);
                self.load_leaves(batch).await?;
                continue;
            }
            match &mut self.mode {
                StreamMode::Leaves => return Ok(None),
                StreamMode::Branches { branches, position, batch, loaded } => {
                    if let Some(page) = loaded.pop_front() {
                        let leaves = page.leaves.clone();
                        self.queue = self.leaf_candidates(&leaves).into();
                        self.batch_size = self.first_batch;
                        continue;
                    }
                    if *position >= branches.len() {
                        return Ok(None);
                    }
                    let end = (*position + *batch).min(branches.len());
                    let slice: Vec<GeoBranchEntry> = branches[*position..end].iter().map(|c| c.item.clone()).collect();
                    *batch = (*batch * 2).min(8);
                    *position = end;
                    let refs: Vec<&GeoBranchEntry> = slice.iter().collect();
                    let root = self.root.clone();
                    let mut stats = self.tracking.branch_fetch;
                    let pages = self.engine.load_geo_branch_pages(&self.geo.field, &root, &refs, &mut stats).await?;
                    self.tracking.branch_fetch = stats;
                    if let StreamMode::Branches { loaded, .. } = &mut self.mode {
                        loaded.extend(pages);
                    }
                }
                StreamMode::Best { .. } => {
                    self.best_step().await?;
                    if self.ready.is_empty() {
                        if let StreamMode::Best { pool, branches, next } = &self.mode {
                            if pool.is_empty() && *next >= branches.len() {
                                return Ok(None);
                            }
                        }
                    }
                }
            }
        }
    }

    /// One emission round of the best-first branch merge.
    async fn best_step(&mut self) -> Result<()> {
        loop {
            let (should_open, branch) = match &self.mode {
                StreamMode::Best { branches, next, pool } => {
                    let open = *next < branches.len() && (pool.is_empty() || branches[*next].min_dist <= pool[0].min_dist);
                    (open, if open { Some(branches[*next].item.clone()) } else { None })
                }
                _ => return Ok(()),
            };
            if !should_open {
                break;
            }
            let branch = branch.expect("branch to open");
            let root = self.root.clone();
            let mut stats = self.tracking.branch_fetch;
            let pages = self.engine.load_geo_branch_pages(&self.geo.field, &root, &[&branch], &mut stats).await?;
            self.tracking.branch_fetch = stats;
            let leaves = pages.into_iter().next().map(|page| page.leaves.clone()).unwrap_or_default();
            let candidates = self.leaf_candidates(&leaves);
            if let StreamMode::Best { next, pool, .. } = &mut self.mode {
                for candidate in candidates {
                    let (mut lo, mut hi) = (0usize, pool.len());
                    while lo < hi {
                        let mid = (lo + hi) >> 1;
                        if pool[mid].min_dist < candidate.min_dist {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    pool.insert(lo, candidate);
                }
                *next += 1;
            }
        }
        let emit: Vec<Candidate<GeoLeafEntry>> = match &mut self.mode {
            StreamMode::Best { branches, next, pool } => {
                if pool.is_empty() {
                    return Ok(());
                }
                let limit = if *next < branches.len() { branches[*next].min_dist } else { f64::INFINITY };
                let mut emit = Vec::new();
                while !pool.is_empty() && pool[0].min_dist <= limit && emit.len() < self.max_batch {
                    emit.push(pool.remove(0));
                }
                emit
            }
            _ => return Ok(()),
        };
        if emit.is_empty() {
            return Ok(());
        }
        self.load_leaves(emit).await
    }
}

impl IndexEngine {
    fn geo_state(&self) -> Rc<GeoState> {
        if let Some(state) = self.extension::<GeoState>("geo") {
            return state;
        }
        let budget = self.cache_budget();
        let state = Rc::new(GeoState {
            roots: PromiseCache::budgeted(budget, "geoTreeRoots"),
            branches: PromiseCache::budgeted(budget, "geoBranchPages"),
            leaves: PromiseCache::budgeted(budget, "geoLeafPages"),
            cell_directories: RefCell::new(HashMap::new()),
            cell_blocks: PromiseCache::budgeted(budget, "geoCellBlocks"),
        });
        self.set_extension("geo", state.clone());
        state
    }

    fn geo_block_filters(&self) -> Vec<BlockFilter> {
        BlockFilter::list_from_manifest(&self.manifest_at(&["block_filters"]))
    }

    /// `loadGeoTreeRoot(field)`.
    pub(crate) async fn load_geo_tree_root(&self, field: &str) -> Result<Option<Rc<GeoTreeRoot>>> {
        let file = self.manifest_at(&["geo", "fields", field, "directory", "file"]);
        let Some(file) = file.as_str().filter(|f| !f.is_empty()).map(str::to_string) else { return Ok(None) };
        let state = self.geo_state();
        let filters = self.geo_block_filters();
        let root = state
            .roots
            .get_or_load(field.to_string(), async move {
                let bytes = self.fetch_gzip_path(&file).await?;
                Ok(Rc::new(parse_geo_tree_root(&bytes, &filters)?))
            })
            .await?;
        Ok(Some(root))
    }

    /// `loadPackedPages`: dedupe, batch-load uncached pages and return the
    /// decoded pages in first-occurrence order.
    #[allow(clippy::too_many_arguments)]
    async fn load_packed_pages<K, V, D>(
        &self,
        cache: &PromiseCache<K, V>,
        entries: Vec<(K, Pointer)>,
        plan: &RangePlan,
        pack_dir: &str,
        label: &str,
        decode: D,
        stats: &mut BatchStats,
    ) -> Result<Vec<(K, V)>>
    where
        K: Eq + std::hash::Hash + Clone + std::fmt::Display,
        V: Clone,
        D: Fn(Bytes) -> Result<V>,
    {
        let entries = unique_entries(entries);
        let keys: Vec<K> = entries.iter().map(|(key, _)| key.clone()).collect();
        let verify = self.verify_checksums;
        let batch = self
            .load_batch(cache, entries, plan, |pack| self.url(&format!("{pack_dir}/{pack}")), |key, pointer, slice| {
                let inflated = inflate_object(&slice, pointer, &format!("{label} {key}"), verify)?;
                decode(inflated)
            })
            .await?;
        stats.add(batch);
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let promise = cache.get(&key).ok_or_else(|| Error::new(format!("Rangefind {label} {key} is missing.")))?;
            out.push((key, promise.await?));
        }
        Ok(out)
    }

    /// `loadGeoLeafPages(field, leaves, stats)`: pages keyed by leaf index.
    async fn load_geo_leaf_pages(&self, field: &str, leaves: &[&GeoLeafEntry], stats: &mut BatchStats) -> Result<Vec<(usize, Rc<GeoLeafPage>)>> {
        let state = self.geo_state();
        let entries: Vec<(LeafKey, Pointer)> = leaves.iter().map(|leaf| (LeafKey(field.to_string(), leaf.index), leaf.pointer.clone())).collect();
        let name = field.to_string();
        let pages = self
            .load_packed_pages(
                &state.leaves,
                entries,
                &self.options.range_plans.geo_leaf_pages,
                "geo/point-packs",
                "geo leaf page",
                |inflated| Ok(Rc::new(decode_geo_leaf_page(&inflated, Some(&name))?)),
                stats,
            )
            .await?;
        Ok(pages.into_iter().map(|(key, page)| (key.1, page)).collect())
    }

    /// `loadGeoBranchPages(field, root, branches, stats)`.
    async fn load_geo_branch_pages(
        &self,
        field: &str,
        root: &GeoTreeRoot,
        branches: &[&GeoBranchEntry],
        stats: &mut BatchStats,
    ) -> Result<Vec<Rc<GeoBranchPage>>> {
        let state = self.geo_state();
        let entries: Vec<(LeafKey, Pointer)> = branches.iter().map(|branch| (LeafKey(field.to_string(), branch.index), branch.pointer.clone())).collect();
        let filters = self.geo_block_filters();
        let name = field.to_string();
        let pack_table = root.pack_table.clone();
        let pages = self
            .load_packed_pages(
                &state.branches,
                entries,
                &self.options.range_plans.geo_leaf_pages,
                "geo/point-packs",
                "geo branch page",
                |inflated| Ok(Rc::new(decode_geo_branch_page(&inflated, &pack_table, &filters, Some(&name))?)),
                stats,
            )
            .await?;
        Ok(pages.into_iter().map(|(_, page)| page).collect())
    }

    /// `geoCellDirectoryState(index)`.
    fn geo_cell_directory_state(&self, index: &Value) -> Option<Rc<DirectoryState>> {
        let directory = index.get("directory").unwrap_or(&NULL);
        let key = directory.get("root").and_then(Value::as_str).unwrap_or("");
        if key.is_empty() {
            return None;
        }
        let state = self.geo_state();
        if let Some(existing) = state.cell_directories.borrow().get(key) {
            return Some(existing.clone());
        }
        let created = DirectoryState::budgeted(directory, self.cache_budget(), "geoCellDirectoryPages").ok()?;
        state.cell_directories.borrow_mut().insert(key.to_string(), created.clone());
        Some(created)
    }

    /// `geoCellDirectoryEntries(index, keys)`: present keys, in key order.
    async fn geo_cell_directory_entries(&self, index: &Value, keys: &[String]) -> Result<Vec<(String, Pointer)>> {
        let Some(state) = self.geo_cell_directory_state(index) else { return Ok(Vec::new()) };
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let root = self.load_directory_root(&state).await?;
        let mut out = Vec::new();
        for key in keys {
            if let Some(pointer) = self.directory_entry_from_root(&state, &root, key).await? {
                out.push((key.clone(), pointer));
            }
        }
        Ok(out)
    }

    /// `loadGeoCellBlocks(index, entries, stats)`.
    async fn load_geo_cell_blocks(&self, index: &Value, entries: Vec<(String, Pointer)>, stats: &mut BatchStats) -> Result<Vec<(String, Rc<GeoCellBlock>)>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let state = self.geo_state();
        let pack_dir = match index.get("pack_dir").filter(|v| truthy(v)) {
            Some(value) => crate::util::json::string_or_empty(value),
            None => "geo/category-cells/packs".to_string(),
        };
        let pack_dir = pack_dir.trim_end_matches('/').to_string();
        self.load_packed_pages(
            &state.cell_blocks,
            entries,
            &self.options.range_plans.default,
            &pack_dir,
            "geo category cell block",
            |inflated| Ok(Rc::new(decode_geo_cell_block(&inflated)?)),
            stats,
        )
        .await
    }

    /// `geoCellIndexForPlan(geoPlan, filterPlan)`.
    fn geo_cell_index_for_plan(&self, geo: &GeoPlan, plan: Option<&DocFilterPlan>) -> Option<PlannedCellIndex> {
        if self.options.get("geoCellIndexes") == &Value::Bool(false) || geo.boxes.is_empty() {
            return None;
        }
        let levels = to_number(geo.meta.get("levels").filter(|v| truthy(v)).unwrap_or(&json!(0)));
        if !(levels > 1.0) {
            return None;
        }
        let indexes: Vec<Value> = geo.meta.get("category_cells").and_then(Value::as_array).cloned().unwrap_or_default();
        if geo.route.is_some() {
            let wildcard = indexes.iter().find(|index| {
                index.get("all_code").and_then(Value::as_f64).map(|n| n.is_finite() && n == n.trunc()).unwrap_or(false)
            });
            if let Some(wildcard) = wildcard {
                let code = wildcard["all_code"].as_f64().unwrap_or(0.0);
                return Some(PlannedCellIndex { index: wildcard.clone(), selected: vec![code as u32], facet: None });
            }
        }
        let plan = plan?;
        if plan.facets.is_empty() {
            return None;
        }
        for index in &indexes {
            let facet_name = index.get("facet").and_then(Value::as_str).unwrap_or("");
            let Some((_, codes)) = plan.facets.iter().find(|(field, _)| field == facet_name) else { continue };
            let mut selected: Vec<u32> = codes.iter().copied().collect();
            selected.sort_unstable();
            let indexed: HashSet<u64> =
                index.get("codes").and_then(Value::as_array).map(|items| items.iter().filter_map(Value::as_f64).map(|n| n as u64).collect()).unwrap_or_default();
            if !selected.is_empty() && selected.iter().all(|code| indexed.contains(&(*code as u64))) {
                return Some(PlannedCellIndex { index: index.clone(), selected, facet: Some(facet_name.to_string()) });
            }
        }
        None
    }

    /// `geoCellRoutedLeaves(geoPlan, rootPromise, filterPlan, tracking)`.
    async fn geo_cell_routed_leaves(&self, geo: &GeoPlan, plan: Option<&DocFilterPlan>, branch_stats: &mut BatchStats) -> Result<Option<CellRouting>> {
        let Some(planned) = self.geo_cell_index_for_plan(geo, plan) else { return Ok(None) };
        let PlannedCellIndex { index, selected, facet } = planned;
        let number = |key: &str, fallback: f64| {
            let value = index.get(key).unwrap_or(&NULL);
            if truthy(value) {
                to_number(value)
            } else {
                fallback
            }
        };
        let max_cells = crate::geo::js_max(1.0, number("max_cells_per_query", 48.0));
        let group_size = crate::geo::js_max(1.0, number("code_group_size", 16.0));
        let block_zoom = to_number(index.get("block_zoom").unwrap_or(&NULL));
        let index_facet = crate::util::json::string_or_empty(index.get("facet").unwrap_or(&NULL));
        let mut selected_groups: Vec<i64> = Vec::new();
        for code in &selected {
            let group = (*code as f64 / group_size).floor() as i64;
            if !selected_groups.contains(&group) {
                selected_groups.push(group);
            }
        }
        let mut levels: Vec<f64> = index.get("levels").and_then(Value::as_array).map(|items| items.iter().map(to_number).collect()).unwrap_or_default();
        levels.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));
        let mut chosen: Option<(Vec<GeoCell>, u32, Vec<(String, GeoCell, i64)>)> = None;
        for candidate in levels {
            let zoom = candidate as u32;
            let next = match &geo.route {
                Some(route) => geo_cells_for_route(route, zoom, max_cells),
                None => geo_cells_for_boxes(&geo.boxes, zoom, max_cells),
            };
            let Some(next) = next else { continue };
            let mut requests: Vec<(String, GeoCell, i64)> = Vec::new();
            let mut seen: HashSet<String> = HashSet::new();
            for cell in &next {
                let block = geo_cell_block(cell, block_zoom);
                for group in &selected_groups {
                    let key = geo_cell_block_key(&geo.field, &index_facet, *group, &block);
                    if seen.insert(key.clone()) {
                        requests.push((key, block, *group));
                    }
                }
            }
            if requests.len() as f64 <= max_cells {
                chosen = Some((next, zoom, requests));
                break;
            }
        }
        let Some((cells, level, requests)) = chosen else { return Ok(None) };

        let keys: Vec<String> = requests.iter().map(|(key, _, _)| key.clone()).collect();
        let available = self.geo_cell_directory_entries(&index, &keys).await?;
        let mut block_stats = BatchStats { wanted: requests.len(), fetched: 0, groups: 0 };
        let decoded = self.load_geo_cell_blocks(&index, available, &mut block_stats).await?;
        let blocks_by_key: HashMap<String, Rc<GeoCellBlock>> = decoded.into_iter().collect();
        let mut ordinals_by_leaf: HashMap<usize, HashSet<usize>> = HashMap::new();
        for cell in &cells {
            let block = geo_cell_block(cell, block_zoom);
            for code in &selected {
                let group = (*code as f64 / group_size).floor() as i64;
                let key = geo_cell_block_key(&geo.field, &index_facet, group, &block);
                let route_key = geo_cell_route_key(level as u64, cell.x as u64, cell.y as u64, *code as u64);
                let Some(route) = blocks_by_key.get(&key).and_then(|block| block.route(&route_key)) else { continue };
                for member in &route.members {
                    let accepted = ordinals_by_leaf.entry(member.leaf as usize).or_default();
                    for ordinal in &member.ordinals {
                        accepted.insert(*ordinal as usize);
                    }
                }
            }
        }

        let mut wanted: Vec<usize> = ordinals_by_leaf.keys().copied().collect();
        wanted.sort_unstable();
        let Some(root) = self.load_geo_tree_root(&geo.field).await? else { return Ok(None) };
        let mut leaves: Vec<GeoLeafEntry> = Vec::new();
        if root.levels == 1 {
            let all = root.leaves.as_deref().unwrap_or(&[]);
            for index_value in &wanted {
                if let Some(leaf) = all.get(*index_value) {
                    leaves.push(leaf.clone());
                }
            }
        } else {
            let branches = root.branches.as_deref().unwrap_or(&[]);
            let mut branch_indexes: Vec<usize> = Vec::new();
            let mut position = 0usize;
            for index_value in &wanted {
                let value = *index_value as u64;
                while position + 1 < branches.len() && branches[position].first_leaf_index + branches[position].leaf_count <= value {
                    position += 1;
                }
                if let Some(branch) = branches.get(position) {
                    if value >= branch.first_leaf_index && value < branch.first_leaf_index + branch.leaf_count && !branch_indexes.contains(&branch.index) {
                        branch_indexes.push(branch.index);
                    }
                }
            }
            branch_indexes.sort_unstable();
            let selected_branches: Vec<&GeoBranchEntry> = branch_indexes.iter().filter_map(|i| branches.get(*i)).collect();
            let pages = self.load_geo_branch_pages(&geo.field, &root, &selected_branches, branch_stats).await?;
            let mut leaf_map: HashMap<usize, GeoLeafEntry> = HashMap::new();
            for page in &pages {
                for leaf in &page.leaves {
                    leaf_map.insert(leaf.index, leaf.clone());
                }
            }
            for index_value in &wanted {
                if let Some(leaf) = leaf_map.get(index_value) {
                    leaves.push(leaf.clone());
                }
            }
        }
        Ok(Some(CellRouting {
            leaves,
            ordinals_by_leaf,
            facet,
            level,
            cells: cells.len(),
            blocks_wanted: requests.len(),
            blocks_fetched: block_stats.fetched,
            block_fetch_groups: block_stats.groups,
        }))
    }

    /// `rowsToGeoCapsuleResults(rows, capsuleByDoc, context)`.
    async fn rows_to_geo_capsule_results(&self, rows: &[Row], capsules: &HashMap<i32, Map<String, Value>>) -> Result<(Vec<Value>, PayloadStats)> {
        let missing: Vec<Row> = rows.iter().filter(|(doc, _)| !capsules.contains_key(doc)).copied().collect();
        let hits = rows.len() - missing.len();
        if missing.is_empty() {
            let results = rows.iter().map(|(doc, score)| result_object(Value::Object(capsules[doc].clone()), *score, *doc)).collect();
            let stats = PayloadStats { lane: Some("geoCapsules".into()), pages: 0, overfetch_docs: 0, capsule_hits: Some(hits), capsule_misses: Some(0) };
            return Ok((results, stats));
        }
        let mut fallback = ResultContext::new(false, PreferDocPages::Unset);
        let hydrated = self.rows_to_results(&missing, &mut fallback).await?;
        let hydrated_by_doc: HashMap<i32, Value> = missing.iter().map(|(doc, _)| *doc).zip(hydrated).collect();
        let fallback_lane = if fallback.lane.is_empty() { None } else { Some(fallback.lane.clone()) };
        let lane = if hits > 0 { Some(format!("geoCapsules+{}", fallback_lane.clone().unwrap_or_else(|| "packedDocs".into()))) } else { fallback_lane };
        let stats = PayloadStats {
            lane,
            pages: fallback.pages,
            overfetch_docs: if fallback.overfetch_docs != 0 { fallback.overfetch_docs } else { missing.len() },
            capsule_hits: Some(hits),
            capsule_misses: Some(missing.len()),
        };
        let results = rows
            .iter()
            .map(|(doc, score)| match capsules.get(doc) {
                Some(capsule) => result_object(Value::Object(capsule.clone()), *score, *doc),
                None => hydrated_by_doc.get(doc).cloned().unwrap_or(Value::Null),
            })
            .collect();
        Ok((results, stats))
    }

    /// `runGeoBrowse` / `runGeoBrowseInner`.
    pub(crate) async fn run_geo_browse(
        &self,
        page: f64,
        size: usize,
        filters: &Filters,
        geo: &Rc<GeoPlan>,
        has_filters: bool,
        text_match_docs: Option<&[i32]>,
    ) -> Result<Option<Value>> {
        let offset = super::text::page_offset(page, size);
        let k = offset + size as f64;
        let doc_filter_plan = if has_filters { Some(self.make_doc_filter_plan(filters)?) } else { None };
        let near = geo.near;
        let radius = near.and_then(|n| n.radius_meters);
        let distance_sorted = geo.sort;
        let route_sorted = geo.route_sort;
        let geo_ordered = distance_sorted || route_sorted;
        let mut tracking = Tracking::default();
        let geo_block_plan = if doc_filter_plan.as_ref().map(|p| p.active).unwrap_or(false) { Some(self.make_block_filter_plan(filters)?) } else { None };
        let mut cell_branch_stats = BatchStats::default();
        let (root, cell_routing) =
            join2(self.load_geo_tree_root(&geo.field), self.geo_cell_routed_leaves(geo, doc_filter_plan.as_ref(), &mut cell_branch_stats)).await;
        let (root, cell_routing) = (root?, cell_routing?);
        tracking.branch_fetch.add(cell_branch_stats);
        let Some(root) = root else { return Ok(None) };
        let effective_plan: Option<DocFilterPlan> = doc_filter_plan.map(|plan| {
            let mut plan = plan;
            if let Some(routing) = &cell_routing {
                plan.facets.retain(|(field, _)| Some(field) != routing.facet.as_ref());
            }
            plan.active = !plan.facets.is_empty() || !plan.numbers.is_empty() || !plan.booleans.is_empty() || plan.geo.is_some();
            plan
        });
        let plan_active = effective_plan.as_ref().map(|p| p.active).unwrap_or(false);
        let bitmap_store = match &effective_plan {
            Some(plan) if plan.active => self.filter_bitmap_store_for_plan(plan).await?,
            _ => None,
        };
        let residual_fields = residual_filter_fields(effective_plan.as_ref(), bitmap_store.as_deref());
        let text_set: Option<HashSet<i32>> = text_match_docs.map(|docs| docs.iter().copied().collect());

        let mut best: Vec<BestItem> = Vec::new();
        let mut collected: Vec<Row> = Vec::new();
        let mut distances: HashMap<i32, f64> = HashMap::new();
        let mut capsule_by_doc: HashMap<i32, Map<String, Value>> = HashMap::new();
        let mut leaves_visited = 0usize;
        let mut points_scanned = 0usize;
        let mut points_accepted = 0usize;
        let mut definite_leaves = 0usize;
        let mut stopped_early = false;
        let kth_distance = |best: &Vec<BestItem>| super::text::kth(best, k).map(|item| item.dist).unwrap_or(f64::INFINITY);
        let reverse = geo.route_reverse;
        let route_order = |found: &RouteMatch| -> f64 {
            let total = geo.route.as_ref().map(|r| r.total_meters).unwrap_or(0.0);
            let directed_progress = if reverse { total - found.progress_meters } else { found.progress_meters };
            let directed_position = if reverse { total - geo.route_position_meters } else { geo.route_position_meters };
            let behind = crate::geo::js_max(0.0, directed_position - directed_progress);
            let ahead = crate::geo::js_max(0.0, directed_progress - directed_position);
            let viewport = match geo.viewport_anchor {
                Some((lat, lon)) => haversine_meters_e7(
                    lat_to_e7_f64(lat).unwrap_or(0) as f64,
                    lon_to_e7_f64(lon).unwrap_or(0) as f64,
                    lat_to_e7_f64(found.rejoin_point.lat).unwrap_or(0) as f64,
                    lon_to_e7_f64(found.rejoin_point.lon).unwrap_or(0) as f64,
                ),
                None => 0.0,
            };
            found.distance_meters + ahead * 0.002 + viewport * 0.025 + behind * 1.25
        };

        let first_batch_hint = if text_match_docs.is_some() { GEO_TEXT_LEAF_PAGE_FIRST_BATCH_SIZE } else { GEO_LEAF_PAGE_FIRST_BATCH_SIZE };
        let mut stream = match &cell_routing {
            Some(routing) => {
                LeafStream::routed(self, geo, root.clone(), &routing.leaves, distance_sorted, tracking, geo_block_plan.as_ref(), first_batch_hint)
            }
            None => LeafStream::tree(self, geo, root.clone(), distance_sorted, tracking, geo_block_plan.as_ref(), first_batch_hint),
        };
        let capsules_enabled = self.options.get("geoCapsules") != &Value::Bool(false);
        while let Some((candidate, leaf_page)) = stream.next().await? {
            if distance_sorted && best.len() as f64 >= k && candidate.min_dist > kth_distance(&best) {
                stopped_early = true;
                break;
            }
            leaves_visited += 1;
            if candidate.geo_definite {
                definite_leaves += 1;
            }
            points_scanned += leaf_page.count;
            let mut code_data: Option<ValueStore> = bitmap_store.clone().map(|bitmaps| ValueStore { bitmaps: Some(bitmaps), ..Default::default() });
            if !residual_fields.is_empty() {
                let docs: Vec<i32> = leaf_page.docs.iter().map(|d| *d as i32).collect();
                code_data = merge_value_stores(self.value_store_for_docs(&residual_fields, &docs).await?, bitmap_store.clone());
            }
            for i in 0..leaf_page.count {
                let lat_e7 = leaf_page.lats_e7[i] as i64;
                let lon_e7 = leaf_page.lons_e7[i] as i64;
                if !candidate.geo_definite && !geo.point_matches_e7(lat_e7, lon_e7) {
                    continue;
                }
                let route_match = match &geo.route {
                    Some(route) => match_point_to_route(route, lat_e7 as f64 / 1e7, lon_e7 as f64 / 1e7).ok().flatten(),
                    None => None,
                };
                if let (Some(found), Some(route)) = (&route_match, &geo.route) {
                    if found.distance_meters > route.corridor_meters {
                        continue;
                    }
                }
                let mut dist = 0.0;
                if let Some(near) = near {
                    dist = haversine_meters_e7(near.lat_e7 as f64, near.lon_e7 as f64, lat_e7 as f64, lon_e7 as f64);
                    if radius.map(|r| dist > r).unwrap_or(false) {
                        continue;
                    }
                }
                if distance_sorted && best.len() as f64 >= k && dist > kth_distance(&best) {
                    continue;
                }
                let doc = leaf_page.docs[i] as i32;
                if let Some(routing) = &cell_routing {
                    if !routing.ordinals_by_leaf.get(&candidate.item.index).map(|set| set.contains(&i)).unwrap_or(false) {
                        continue;
                    }
                }
                if let Some(text_set) = &text_set {
                    if !text_set.contains(&doc) {
                        continue;
                    }
                }
                if plan_active {
                    if let Some(plan) = &effective_plan {
                        if !self.passes_filter_plan(doc, code_data.as_ref(), plan, None) {
                            continue;
                        }
                    }
                }
                points_accepted += 1;
                if capsules_enabled {
                    if let Some(capsule) = leaf_page.capsules.as_ref().and_then(|rows| rows.get(i)) {
                        capsule_by_doc.insert(doc, capsule.clone());
                    }
                }
                if geo_ordered {
                    let order = if route_sorted { route_match.as_ref().map(route_order).unwrap_or(f64::NAN) } else { dist };
                    let (mut lo, mut hi) = (0usize, best.len());
                    while lo < hi {
                        let mid = (lo + hi) >> 1;
                        if best[mid].dist < order || (best[mid].dist == order && best[mid].doc < doc) {
                            lo = mid + 1;
                        } else {
                            hi = mid;
                        }
                    }
                    best.insert(lo, BestItem { doc, dist: order, route_match });
                    // `best.length = Math.ceil(k)`.
                    if best.len() as f64 > k {
                        best.truncate(k.ceil() as usize);
                    }
                } else {
                    collected.push((doc, 0.0));
                    if near.is_some() {
                        distances.insert(doc, dist);
                    }
                    if collected.len() as f64 >= k {
                        stopped_early = true;
                        break;
                    }
                }
            }
            if stopped_early {
                break;
            }
        }
        let tracking = stream.tracking;
        drop(stream);

        let rows: Vec<Row> = if geo_ordered {
            {
                let (from, to) = super::federation::slice_bounds(best.len(), offset, offset + size as f64);
                best[from..to].iter().map(|item| (item.doc, 0.0)).collect()
            }
        } else {
            super::text::page_slice(&collected, offset, size)
        };
        if distance_sorted {
            for item in &best {
                distances.insert(item.doc, item.dist);
            }
        }
        let mut route_matches: HashMap<i32, (Option<RouteMatch>, f64)> = HashMap::new();
        if route_sorted {
            for item in &best {
                route_matches.insert(item.doc, (item.route_match.clone(), item.dist));
            }
        }
        let (mut results, payload) = if !capsule_by_doc.is_empty() {
            self.rows_to_geo_capsule_results(&rows, &capsule_by_doc).await?
        } else {
            let mut context = ResultContext::new(false, PreferDocPages::Yes);
            let results = self.rows_to_results(&rows, &mut context).await?;
            (results, PayloadStats::from_context(&context))
        };
        if near.is_some() {
            for result in results.iter_mut() {
                let Some(object) = result.as_object_mut() else { continue };
                let index = object.get("index").and_then(Value::as_i64).map(|i| i as i32);
                if let Some(dist) = index.and_then(|i| distances.get(&i)) {
                    object.insert("distanceMeters".into(), rounded_distance_meters(*dist));
                }
            }
        }
        if let Some(route) = &geo.route {
            for result in results.iter_mut() {
                let Some(object) = result.as_object_mut() else { continue };
                let index = object.get("index").and_then(Value::as_i64).map(|i| i as i32);
                let routed = index.and_then(|i| route_matches.get(&i));
                let mut found = routed.and_then(|(found, _)| found.clone());
                if found.is_none() {
                    let lat = number_of_property(object, "lat");
                    let lon = number_of_property(object, "lon");
                    if lat.is_finite() && lon.is_finite() {
                        found = match_point_to_route(route, lat, lon).ok().flatten();
                    }
                }
                let Some(found) = found else { continue };
                let Some(public) = route_match_json(&found) else { continue };
                attach_route_fields(object, public, routed.map(|(_, rank)| *rank));
            }
        }
        let matched = if route_sorted {
            points_accepted
        } else if distance_sorted {
            best.len()
        } else {
            collected.len()
        };
        let exact_total = !stopped_early;
        let mut lane = String::from(if distance_sorted {
            "nearest"
        } else if route_sorted {
            "route"
        } else {
            "browse"
        });
        if text_match_docs.is_some() {
            lane.push_str("Text");
        }
        if cell_routing.is_some() {
            lane.push_str("CategoryCells");
        }
        let mut stats = Map::new();
        stats.insert("exact".into(), json!(if geo_ordered { true } else { exact_total }));
        stats.insert("geoLane".into(), json!(lane));
        stats.insert("geoField".into(), json!(geo.field));
        stats.insert("geoDistanceSorted".into(), json!(distance_sorted));
        stats.insert("geoRouteSorted".into(), json!(route_sorted));
        if let Some(route) = &geo.route {
            stats.insert("geoRouteMeters".into(), num(js_round(route.total_meters)));
            stats.insert("geoRouteSegments".into(), json!(route.segments.len()));
            stats.insert("geoRouteCorridorMeters".into(), num(route.corridor_meters));
            stats.insert("geoRouteDirection".into(), json!(if geo.route_reverse { "reverse" } else { "forward" }));
        }
        stats.insert("geoTreeLevels".into(), json!(root.levels));
        stats.insert("geoDirectoryLeaves".into(), json!(root.leaf_count));
        stats.insert("geoCandidateBranches".into(), json!(tracking.candidate_branches));
        stats.insert("geoBranchPagesFetched".into(), json!(tracking.branch_fetch.fetched));
        stats.insert("geoCandidateLeaves".into(), json!(tracking.candidate_leaves));
        stats.insert("geoCategoryCellLevel".into(), cell_routing.as_ref().map(|r| json!(r.level)).unwrap_or(Value::Null));
        stats.insert("geoCategoryCells".into(), json!(cell_routing.as_ref().map(|r| r.cells).unwrap_or(0)));
        stats.insert("geoCategoryCellBlocksWanted".into(), json!(cell_routing.as_ref().map(|r| r.blocks_wanted).unwrap_or(0)));
        stats.insert("geoCategoryCellBlocksFetched".into(), json!(cell_routing.as_ref().map(|r| r.blocks_fetched).unwrap_or(0)));
        stats.insert("geoCategoryCellBlockFetchGroups".into(), json!(cell_routing.as_ref().map(|r| r.block_fetch_groups).unwrap_or(0)));
        stats.insert("geoCategoryCellLeaves".into(), json!(cell_routing.as_ref().map(|r| r.leaves.len()).unwrap_or(0)));
        stats.insert("geoLeavesVisited".into(), json!(leaves_visited));
        stats.insert("geoDefiniteLeaves".into(), json!(definite_leaves));
        stats.insert("geoLeafPagesPrefetched".into(), json!(tracking.leaf_fetch.wanted));
        stats.insert("geoLeafPagesFetched".into(), json!(tracking.leaf_fetch.fetched));
        stats.insert("geoLeafPageFetchGroups".into(), json!(tracking.leaf_fetch.groups));
        stats.insert("geoPointsScanned".into(), json!(points_scanned));
        stats.insert("geoPointsAccepted".into(), json!(points_accepted));
        stats.insert("geoCapsuleHits".into(), json!(payload.capsule_hits.unwrap_or(0)));
        stats.insert("geoCapsuleMisses".into(), json!(payload.capsule_misses.unwrap_or(rows.len())));
        payload.write(&mut stats);
        Ok(Some(json!({
            "total": if exact_total { json!(matched) } else { num(super::text::js_max(matched as f64, k)) },
            "results": results,
            "page": num(page),
            "size": size,
            "approximate": !exact_total,
            "stats": Value::Object(stats)
        })))
    }

    /// `estimateGeoFilterDocValueBytes(geoPlan, verifiedDocs)`.
    async fn estimate_geo_filter_doc_value_bytes(&self, geo: &GeoPlan, verified_docs: usize) -> Result<Option<f64>> {
        self.ensure_doc_values_manifest().await?;
        let Some(meta) = self.doc_values_meta() else { return Ok(None) };
        let mut bytes = 0.0f64;
        for field in [&geo.lat_field, &geo.lon_field] {
            let Some(field_meta) = meta.field(field) else { continue };
            let mut chunk_count = 0usize;
            let mut chunk_bytes = 0.0f64;
            for chunk in field_meta.chunks.iter().flatten() {
                chunk_count += 1;
                chunk_bytes += chunk.pointer.length as f64;
            }
            if chunk_count == 0 {
                continue;
            }
            bytes += (verified_docs.min(chunk_count) as f64) * (chunk_bytes / chunk_count as f64);
        }
        Ok(if bytes != 0.0 && !bytes.is_nan() { Some(bytes) } else { None })
    }

    /// `buildGeoDocSetIfCheap(geoPlan, textPostingsEstimate, filters)`.
    pub(crate) async fn build_geo_doc_set_if_cheap(&self, geo: &Rc<GeoPlan>, estimate: Option<usize>, filters: &Filters) -> Result<()> {
        if !geo.filtered {
            return Ok(());
        }
        let Some(root) = self.load_geo_tree_root(&geo.field).await? else { return Ok(()) };
        self.ensure_facet_dictionaries(&filters.facet_fields()).await?;
        let block_plan = self.make_block_filter_plan(filters)?;
        let mut tracking = Tracking::default();
        let mut candidate_points: u64 = 0;
        let mut candidate_leaf_bytes: u64 = 0;
        if root.levels == 1 {
            for leaf in root.leaves.as_deref().unwrap_or(&[]) {
                if geo_candidate(geo, &leaf.bbox, leaf.filters.as_ref(), Some(&block_plan)).is_some() {
                    candidate_points += leaf.count;
                    candidate_leaf_bytes += leaf.pointer.length;
                }
            }
        } else {
            let candidates: Vec<&GeoBranchEntry> = root
                .branches
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter(|branch| geo_candidate(geo, &branch.bbox, branch.filters.as_ref(), Some(&block_plan)).is_some())
                .collect();
            if candidates.len() > 32 {
                return Ok(());
            }
            let pages = self.load_geo_branch_pages(&geo.field, &root, &candidates, &mut tracking.branch_fetch).await?;
            for page in &pages {
                for leaf in &page.leaves {
                    if geo_candidate(geo, &leaf.bbox, leaf.filters.as_ref(), Some(&block_plan)).is_some() {
                        candidate_points += leaf.count;
                        candidate_leaf_bytes += leaf.pointer.length;
                    }
                }
            }
        }
        if candidate_points > self.options.geo_text_max_candidate_points as u64 {
            let Some(estimate) = estimate.filter(|e| *e > 0) else { return Ok(()) };
            if candidate_points > GEO_TEXT_DOC_SET_HARD_CAP {
                return Ok(());
            }
            let doc_value_bytes = self.estimate_geo_filter_doc_value_bytes(geo, estimate).await?;
            match doc_value_bytes {
                Some(bytes) if (candidate_leaf_bytes as f64) < bytes => {}
                _ => return Ok(()),
            }
        }
        let mut doc_set: HashSet<i32> = HashSet::new();
        let mut stream = LeafStream::tree(self, geo, root.clone(), false, tracking, Some(&block_plan), GEO_LEAF_PAGE_FIRST_BATCH_SIZE);
        while let Some((candidate, leaf_page)) = stream.next().await? {
            for i in 0..leaf_page.count {
                if candidate.geo_definite || geo.point_matches_e7(leaf_page.lats_e7[i] as i64, leaf_page.lons_e7[i] as i64) {
                    doc_set.insert(leaf_page.docs[i] as i32);
                }
            }
        }
        let tracking = stream.tracking;
        drop(stream);
        let matched = doc_set.len();
        *geo.doc_set.borrow_mut() = Some(doc_set);
        // Stored in the response shape `executeSearch` merges into stats
        // (`geoCandidateLeaves`, `geoPointsScanned`, ...).
        let mut stats = Map::new();
        stats.insert("geoCandidateLeaves".into(), json!(tracking.candidate_leaves));
        stats.insert("geoPointsScanned".into(), json!(candidate_points));
        stats.insert("geoPointsAccepted".into(), json!(matched));
        stats.insert("geoLeafPagesFetched".into(), json!(tracking.leaf_fetch.fetched));
        stats.insert("geoLeafPageFetchGroups".into(), json!(tracking.leaf_fetch.groups));
        *geo.doc_set_stats.borrow_mut() = Some(stats);
        Ok(())
    }

    /// `runGeoTextNearestByDocValues`.
    async fn run_geo_text_nearest_by_doc_values(
        &self,
        page: f64,
        size: usize,
        filters: &Filters,
        geo: &GeoPlan,
        has_filters: bool,
        match_docs: &[i32],
    ) -> Result<Option<Value>> {
        let option = self.options.get("geoTextNearestDocValues");
        if option == &Value::Bool(false) {
            return Ok(None);
        }
        let forced = option == &Value::Bool(true);
        let Some(root) = self.load_geo_tree_root(&geo.field).await? else { return Ok(None) };
        if root.total == 0 {
            return Ok(None);
        }
        let offset = super::text::page_offset(page, size);
        let k = offset + size as f64;
        let expected_traversal_bytes = k * (root.total as f64 / match_docs.len() as f64) * GEO_TEXT_NEAREST_POINT_BYTES_ESTIMATE;
        let mut doc_value_bytes = None;
        if !forced {
            if expected_traversal_bytes < GEO_TEXT_NEAREST_MIN_TRAVERSAL_BYTES {
                return Ok(None);
            }
            doc_value_bytes = self.estimate_geo_filter_doc_value_bytes(geo, match_docs.len()).await?;
            match doc_value_bytes {
                Some(bytes) if bytes < expected_traversal_bytes => {}
                _ => return Ok(None),
            }
        }
        let doc_filter_plan = if has_filters { Some(self.make_doc_filter_plan(filters)?) } else { None };
        let plan_active = doc_filter_plan.as_ref().map(|p| p.active).unwrap_or(false);
        let bitmap_store = match &doc_filter_plan {
            Some(plan) if plan.active => self.filter_bitmap_store_for_plan(plan).await?,
            _ => None,
        };
        let residual = residual_filter_fields(doc_filter_plan.as_ref(), bitmap_store.as_deref());
        let mut fields = vec![geo.lat_field.clone(), geo.lon_field.clone()];
        fields.extend(residual);
        let store = self.value_store_for_docs(&fields, match_docs).await?;
        let code_data = merge_value_stores(store.clone(), bitmap_store);
        let mut matched: Vec<(i32, f64)> = Vec::new();
        for &doc in match_docs {
            let lat = self.value_for_doc(store.as_ref(), &geo.lat_field, doc).as_number().and_then(lat_to_e7_f64);
            let lon = self.value_for_doc(store.as_ref(), &geo.lon_field, doc).as_number().and_then(lon_to_e7_f64);
            let (Some(lat_e7), Some(lon_e7)) = (lat, lon) else { continue };
            if !geo.point_matches_e7(lat_e7, lon_e7) {
                continue;
            }
            if plan_active {
                if let Some(plan) = &doc_filter_plan {
                    if !self.passes_filter_plan(doc, code_data.as_ref(), plan, None) {
                        continue;
                    }
                }
            }
            let dist = match geo.near {
                Some(near) => haversine_meters_e7(near.lat_e7 as f64, near.lon_e7 as f64, lat_e7 as f64, lon_e7 as f64),
                None => 0.0,
            };
            matched.push((doc, dist));
        }
        matched.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal).then(a.0.cmp(&b.0)));
        let rows: Vec<Row> = super::text::page_slice(&matched, offset, size).iter().map(|(doc, _)| (*doc, 0.0)).collect();
        let mut context = ResultContext::new(false, PreferDocPages::Yes);
        let mut results = self.rows_to_results(&rows, &mut context).await?;
        if geo.near.is_some() {
            let by_doc: HashMap<i32, f64> = matched.iter().copied().collect();
            for result in results.iter_mut() {
                let Some(object) = result.as_object_mut() else { continue };
                let index = object.get("index").and_then(Value::as_i64).map(|i| i as i32);
                if let Some(dist) = index.and_then(|i| by_doc.get(&i)) {
                    object.insert("distanceMeters".into(), rounded_distance_meters(*dist));
                }
            }
        }
        let mut stats = Map::new();
        stats.insert("exact".into(), json!(true));
        stats.insert("geoLane".into(), json!("nearestTextDocValues"));
        stats.insert("geoField".into(), json!(geo.field));
        stats.insert("geoDistanceSorted".into(), json!(true));
        stats.insert("geoRouteSorted".into(), json!(false));
        if let Some(bytes) = doc_value_bytes {
            stats.insert("geoDocValueBytesEstimated".into(), num(js_round(bytes)));
        }
        stats.insert("geoTraversalBytesEstimated".into(), num(js_round(expected_traversal_bytes)));
        PayloadStats::from_context(&context).write(&mut stats);
        Ok(Some(json!({
            "total": matched.len(),
            "results": results,
            "page": num(page),
            "size": size,
            "approximate": false,
            "stats": Value::Object(stats)
        })))
    }

    /// The text + `geo.sort` branch of `executeSearch`: resolve the exact
    /// text match set, then order it by distance or route position.
    pub(crate) async fn run_geo_text_sorted(&self, page: f64, size: usize, filters: &Filters, geo: &Rc<GeoPlan>, base_terms: &[String]) -> Result<Value> {
        // The tree root fetch overlaps the posting reads; its failure is
        // ignored here (the ordering lanes retry it).
        let (_, matched) = join2(self.load_geo_tree_root(&geo.field), self.collect_text_match_docs(base_terms)).await;
        let Some((docs, match_stats)) = matched? else {
            return Err(Error::with_code(GEO_TEXT_SORT_BUDGET_MESSAGE, "RANGEFIND_GEO_TEXT_SORT_BUDGET"));
        };
        let has_filters = filters.has_filters();
        self.ensure_facet_dictionaries(&filters.facet_fields()).await?;
        let mut response = if docs.is_empty() {
            Some(json!({
                "total": 0,
                "results": [],
                "page": num(page),
                "size": size,
                "approximate": false,
                "stats": {
                    "exact": true,
                    "geoLane": if geo.route_sort { "routeTextEmpty" } else { "nearestTextEmpty" },
                    "geoField": geo.field,
                    "geoDistanceSorted": geo.sort,
                    "geoRouteSorted": geo.route_sort
                }
            }))
        } else if !geo.route_sort {
            self.run_geo_text_nearest_by_doc_values(page, size, filters, geo, has_filters, &docs).await?
        } else {
            None
        };
        if response.is_none() {
            response = self.run_geo_browse(page, size, filters, geo, has_filters, Some(&docs)).await?;
        }
        let Some(mut response) = response else {
            return Err(Error::new("Rangefind: geo tree is unavailable for geographic sorting."));
        };
        let stat = |key: &str| match_stats.get(key).cloned().unwrap_or(Value::Null);
        if !response.get("stats").map(Value::is_object).unwrap_or(false) {
            response["stats"] = json!({});
        }
        if let Some(stats) = response["stats"].as_object_mut() {
            stats.insert("textMatchDocs".into(), json!(docs.len()));
            stats.insert("textMatchTerms".into(), stat("terms"));
            stats.insert("textMatchMinShouldMatch".into(), stat("minShouldMatch"));
            stats.insert("textMatchBlocksDecoded".into(), stat("blocksDecoded"));
            stats.insert("textMatchPostingsDecoded".into(), stat("postingsDecoded"));
        }
        Ok(response)
    }

    /// `attachGeoDistances(response, geoPlan)`.
    pub(crate) async fn attach_geo_distances(&self, mut response: Value, geo: Option<&Rc<GeoPlan>>) -> Result<Value> {
        let Some(geo) = geo else { return Ok(response) };
        if geo.near.is_none() && geo.route.is_none() {
            return Ok(response);
        }
        let indexes: Vec<i32> = match response.get("results").and_then(Value::as_array) {
            Some(results) if !results.is_empty() => results.iter().filter_map(|r| integer_index(r.get("index"))).collect(),
            _ => return Ok(response),
        };
        if indexes.is_empty() {
            return Ok(response);
        }
        let fields = vec![geo.lat_field.clone(), geo.lon_field.clone()];
        let store = self.value_store_for_docs(&fields, &indexes).await?;
        if let Some(results) = response.get_mut("results").and_then(Value::as_array_mut) {
            for result in results.iter_mut() {
                let Some(doc) = integer_index(result.get("index")) else { continue };
                let lat = self.value_for_doc(store.as_ref(), &geo.lat_field, doc).as_number().and_then(lat_to_e7_f64);
                let lon = self.value_for_doc(store.as_ref(), &geo.lon_field, doc).as_number().and_then(lon_to_e7_f64);
                let (Some(lat_e7), Some(lon_e7)) = (lat, lon) else { continue };
                let Some(object) = result.as_object_mut() else { continue };
                if let Some(near) = geo.near {
                    let dist = haversine_meters_e7(near.lat_e7 as f64, near.lon_e7 as f64, lat_e7 as f64, lon_e7 as f64);
                    object.insert("distanceMeters".into(), rounded_distance_meters(dist));
                }
                if let Some(route) = &geo.route {
                    let found = match_point_to_route(route, lat_e7 as f64 / 1e7, lon_e7 as f64 / 1e7)?;
                    if let Some(public) = found.as_ref().and_then(route_match_json) {
                        attach_route_fields(object, public, None);
                    }
                }
            }
        }
        if let Some(boost) = geo.boost {
            let mut window = 0usize;
            if let Some(results) = response.get_mut("results").and_then(Value::as_array_mut) {
                for result in results.iter_mut() {
                    let Some(object) = result.as_object_mut() else { continue };
                    let Some(distance) = object.get("distanceMeters").filter(|v| !v.is_null()).map(to_number) else { continue };
                    let score = object.get("score").map(to_number).unwrap_or(f64::NAN);
                    object.insert("score".into(), num(score + boost.weight * boost.pivot_meters / (boost.pivot_meters + distance)));
                }
                results.sort_by(|a, b| {
                    let score = |v: &Value| v.get("score").map(to_number).unwrap_or(f64::NAN);
                    let index = |v: &Value| v.get("index").map(to_number).unwrap_or(f64::NAN);
                    let diff = score(b) - score(a);
                    let diff = if diff == 0.0 || diff.is_nan() { index(a) - index(b) } else { diff };
                    if diff < 0.0 {
                        Ordering::Less
                    } else if diff > 0.0 {
                        Ordering::Greater
                    } else {
                        Ordering::Equal
                    }
                });
                window = results.len();
            }
            if !response.get("stats").map(Value::is_object).unwrap_or(false) {
                response["stats"] = json!({});
            }
            if let Some(stats) = response["stats"].as_object_mut() {
                stats.insert("geoBoost".into(), json!(true));
                stats.insert("geoBoostWindow".into(), json!(window));
            }
        }
        Ok(response)
    }
}

/// `Number.isInteger(result.index)` → the doc id.
fn integer_index(value: Option<&Value>) -> Option<i32> {
    let number = value?.as_f64()?;
    if number.is_finite() && number == number.trunc() {
        Some(number as i32)
    } else {
        None
    }
}

/// `(field, index)` cache key with a `field:index` label.
#[derive(Clone, PartialEq, Eq, Hash)]
struct LeafKey(String, usize);

impl std::fmt::Display for LeafKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}
