//! `createSearch(options)` knobs, normalized exactly like the JS runtime.

use serde_json::Value;

use crate::pointer::RangePlan;
use crate::util::json::{num_nullish, num_or, path, truthy};

pub const RERANK_CANDIDATES: usize = 30;
pub const DEPENDENCY_SCORE_SCALE: f64 = 0.12;
pub const SKIP_MAX_TERMS: usize = 30;
pub const EXTERNAL_POSTING_BLOCK_PREFETCH: usize = 16;
pub const POSTING_BLOCK_FRONTIER: usize = 4;
pub const FILTER_BITMAP_SPARSE_DOC_LIMIT: usize = 256;
pub const DOC_RANGE_PLANNER_MIN_CANDIDATE_RANGES: usize = 2;
pub const DOC_RANGE_PLANNER_MAX_CANDIDATE_BLOCK_RATIO: f64 = 0.12;
pub const DOC_RANGE_BLOCK_PRUNE_BATCH_SIZE: usize = 1024;
pub const DOC_RANGE_BLOCK_PRUNE_INITIAL_BATCH_SIZE: usize = 32;
pub const DOC_VALUE_SORT_PAGE_BATCH_SIZE: usize = 16;
pub const GEO_LEAF_PAGE_BATCH_SIZE: usize = 64;
pub const GEO_TEXT_MAX_CANDIDATE_POINTS: usize = 100_000;
pub const GEO_TEXT_SORT_MAX_DF: usize = 200_000;
pub const FACET_COUNT_MAX_CHUNKS: usize = 32;
pub const FACET_COUNT_SIZE: usize = 10;

#[derive(Clone, Debug)]
pub struct RangePlans {
    pub default: RangePlan,
    pub doc_pointers: RangePlan,
    pub docs: RangePlan,
    pub doc_page_pointers: RangePlan,
    pub doc_pages: RangePlan,
    pub doc_value_sort_pages: RangePlan,
    pub sort_replica_rank_maps: RangePlan,
    pub sort_replica_doc_pointers: RangePlan,
    pub sort_replica_docs: RangePlan,
    pub sort_replica_doc_page_pointers: RangePlan,
    pub sort_replica_doc_pages: RangePlan,
    pub authority: RangePlan,
    pub posting_blocks: RangePlan,
    pub posting_block_frontier: RangePlan,
    pub posting_doc_ranges: RangePlan,
    pub geo_leaf_pages: RangePlan,
    pub vector_cluster_pages: RangePlan,
    pub vector_refine: RangePlan,
}

const INF: f64 = f64::INFINITY;
const KB: f64 = 1024.0;

impl Default for RangePlans {
    fn default() -> Self {
        RangePlans {
            default: RangePlan::new(8.0 * KB, 64.0 * KB, 4.0),
            doc_pointers: RangePlan::new(32.0 * KB, 32.0 * KB, INF),
            docs: RangePlan::new(32.0 * KB, 8.0 * KB, INF),
            doc_page_pointers: RangePlan::new(32.0 * KB, 32.0 * KB, INF),
            doc_pages: RangePlan::new(64.0 * KB, 64.0 * KB, INF),
            doc_value_sort_pages: RangePlan::new(64.0 * KB, 64.0 * KB, INF),
            sort_replica_rank_maps: RangePlan::new(64.0 * KB, 64.0 * KB, INF),
            sort_replica_doc_pointers: RangePlan::new(4.0 * KB, 8.0 * KB, INF),
            sort_replica_docs: RangePlan::new(8.0 * KB, 16.0 * KB, INF),
            sort_replica_doc_page_pointers: RangePlan::new(4.0 * KB, 8.0 * KB, INF),
            sort_replica_doc_pages: RangePlan::new(8.0 * KB, 16.0 * KB, INF),
            authority: RangePlan::new(32.0 * KB, 32.0 * KB, INF),
            posting_blocks: RangePlan::new(256.0 * KB, 512.0 * KB, INF).with_max_merged(1024.0 * KB),
            posting_block_frontier: RangePlan::new(512.0 * KB, 1024.0 * KB, INF).with_max_merged(2048.0 * KB),
            posting_doc_ranges: RangePlan::new(512.0 * KB, 1024.0 * KB, INF).with_max_merged(2048.0 * KB),
            geo_leaf_pages: RangePlan::new(64.0 * KB, 64.0 * KB, INF),
            vector_cluster_pages: RangePlan::new(64.0 * KB, 64.0 * KB, INF),
            vector_refine: RangePlan::new(16.0 * KB, 32.0 * KB, 8.0),
        }
    }
}

fn plan_override(base: RangePlan, value: &Value) -> RangePlan {
    if !value.is_object() {
        return base;
    }
    let read = |key: &str, fallback: f64| match value.get(key).and_then(Value::as_f64) {
        Some(n) => n,
        None => fallback,
    };
    RangePlan {
        merge_gap_bytes: read("mergeGapBytes", 8.0 * KB),
        max_merged_bytes: read("maxMergedBytes", INF),
        max_overfetch_bytes: read("maxOverfetchBytes", INF),
        max_overfetch_ratio: read("maxOverfetchRatio", INF),
    }
}

/// Runtime options. Unknown keys are kept in `raw` so lanes can read the
/// rarer switches the same way the JS runtime does.
#[derive(Clone, Debug)]
pub struct EngineOptions {
    pub raw: Value,
    pub manifest: Option<Value>,
    pub manifest_name: Option<String>,
    pub verify_checksums: bool,
    pub multi_range_requests: bool,
    pub multi_range_max_ranges: usize,
    pub max_page_size: usize,
    pub top_k_proof_max_k: usize,
    pub query_plan_cache_size: usize,
    pub posting_block_frontier: usize,
    pub top_k_proof_check_interval: usize,
    pub top_k_proof_check_interval_max: usize,
    pub top_k_proof_check_scores_per_block: usize,
    pub top_k_block_budget: Option<usize>,
    pub doc_value_sort_page_batch_size: usize,
    pub geo_leaf_page_batch_size: usize,
    pub geo_text_max_candidate_points: usize,
    pub geo_text_sort_max_df: usize,
    pub facet_count_max_chunks: usize,
    pub doc_range_planner: bool,
    pub doc_range_block_prune_batch_size: usize,
    pub doc_range_block_prune_initial_batch_size: usize,
    pub doc_range_impact_planner: bool,
    pub doc_range_planner_max_candidate_block_ratio: f64,
    pub conjunction_tail: bool,
    pub text_doc_page_hydration: bool,
    pub text_doc_page_max_overfetch_docs: Option<f64>,
    pub segment_fanout: Option<bool>,
    pub authority: bool,
    pub query_bundles: bool,
    pub doc_values_binary_manifest: bool,
    pub trace: bool,
    pub range_plans: RangePlans,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions::from_json(Value::Null)
    }
}

fn clamp_floor(value: f64, min: f64, max: f64) -> usize {
    let floored = value.floor();
    if floored.is_nan() {
        return min as usize;
    }
    floored.max(min).min(max) as usize
}

impl EngineOptions {
    pub fn from_json(raw: Value) -> Self {
        let raw = if raw.is_object() { raw } else { Value::Object(Default::default()) };
        let get = |key: &str| raw.get(key).unwrap_or(&crate::util::json::NULL);
        let top_k_proof_check_interval = clamp_floor(num_or(get("topKProofCheckInterval"), 1.0), 1.0, 4096.0);
        let doc_range_block_prune_batch_size =
            clamp_floor(num_or(get("docRangeBlockPruneBatchSize"), DOC_RANGE_BLOCK_PRUNE_BATCH_SIZE as f64), 1.0, 2048.0);
        let mut plans = RangePlans::default();
        if let Some(overrides) = raw.get("rangePlans").and_then(Value::as_object) {
            for (key, value) in overrides {
                let slot = match key.as_str() {
                    "default" => &mut plans.default,
                    "docPointers" => &mut plans.doc_pointers,
                    "docs" => &mut plans.docs,
                    "docPagePointers" => &mut plans.doc_page_pointers,
                    "docPages" => &mut plans.doc_pages,
                    "docValueSortPages" => &mut plans.doc_value_sort_pages,
                    "sortReplicaRankMaps" => &mut plans.sort_replica_rank_maps,
                    "sortReplicaDocPointers" => &mut plans.sort_replica_doc_pointers,
                    "sortReplicaDocs" => &mut plans.sort_replica_docs,
                    "sortReplicaDocPagePointers" => &mut plans.sort_replica_doc_page_pointers,
                    "sortReplicaDocPages" => &mut plans.sort_replica_doc_pages,
                    "authority" => &mut plans.authority,
                    "postingBlocks" => &mut plans.posting_blocks,
                    "postingBlockFrontier" => &mut plans.posting_block_frontier,
                    "postingDocRanges" => &mut plans.posting_doc_ranges,
                    "geoLeafPages" => &mut plans.geo_leaf_pages,
                    "vectorClusterPages" => &mut plans.vector_cluster_pages,
                    "vectorRefine" => &mut plans.vector_refine,
                    _ => continue,
                };
                *slot = plan_override(*slot, value);
            }
        }
        let max_ratio = get("docRangePlannerMaxCandidateBlockRatio");
        EngineOptions {
            manifest: raw.get("manifest").filter(|value| value.is_object()).cloned(),
            manifest_name: raw.get("manifestName").and_then(Value::as_str).filter(|name| !name.is_empty()).map(str::to_string),
            verify_checksums: get("verifyChecksums") != &Value::Bool(false),
            multi_range_requests: get("multiRangeRequests") == &Value::Bool(true),
            multi_range_max_ranges: clamp_floor(num_or(get("multiRangeMaxRanges"), 32.0), 2.0, 64.0),
            max_page_size: clamp_floor(num_or(get("maxPageSize"), 100.0), 1.0, 1000.0),
            top_k_proof_max_k: clamp_floor(num_or(get("topKProofMaxK"), 100.0), 1.0, 1000.0),
            query_plan_cache_size: {
                let value = num_nullish(get("queryPlanCacheSize"), 128.0).floor();
                if value.is_nan() || value <= 0.0 {
                    0
                } else {
                    value.min(1024.0) as usize
                }
            },
            posting_block_frontier: clamp_floor(num_or(get("postingBlockFrontier"), POSTING_BLOCK_FRONTIER as f64), 1.0, 16.0),
            top_k_proof_check_interval,
            top_k_proof_check_interval_max: clamp_floor(num_or(get("topKProofCheckIntervalMax"), 32.0), 0.0, 4096.0)
                .max(top_k_proof_check_interval),
            top_k_proof_check_scores_per_block: num_or(get("topKProofCheckScoresPerBlock"), 2048.0).floor().max(1.0) as usize,
            top_k_block_budget: if get("topKBlockBudget").is_null() {
                None
            } else {
                Some(crate::util::json::to_number(get("topKBlockBudget")).floor().max(0.0) as usize)
            },
            doc_value_sort_page_batch_size: clamp_floor(
                num_or(get("docValueSortPageBatchSize"), DOC_VALUE_SORT_PAGE_BATCH_SIZE as f64),
                1.0,
                64.0,
            ),
            geo_leaf_page_batch_size: clamp_floor(num_or(get("geoLeafPageBatchSize"), GEO_LEAF_PAGE_BATCH_SIZE as f64), 1.0, 64.0),
            geo_text_max_candidate_points: num_nullish(get("geoTextMaxCandidatePoints"), GEO_TEXT_MAX_CANDIDATE_POINTS as f64)
                .floor()
                .max(0.0) as usize,
            geo_text_sort_max_df: num_nullish(get("geoTextSortMaxDf"), GEO_TEXT_SORT_MAX_DF as f64).floor().max(0.0) as usize,
            facet_count_max_chunks: num_nullish(get("facetCountMaxChunks"), FACET_COUNT_MAX_CHUNKS as f64).floor().max(1.0) as usize,
            doc_range_planner: get("docRangePlanner") != &Value::Bool(false),
            doc_range_block_prune_batch_size,
            doc_range_block_prune_initial_batch_size: clamp_floor(
                num_or(get("docRangeBlockPruneInitialBatchSize"), DOC_RANGE_BLOCK_PRUNE_INITIAL_BATCH_SIZE as f64),
                1.0,
                doc_range_block_prune_batch_size as f64,
            ),
            doc_range_impact_planner: get("docRangeImpactPlanner") != &Value::Bool(false),
            doc_range_planner_max_candidate_block_ratio: match crate::util::json::to_number(max_ratio) {
                n if !max_ratio.is_null() && n.is_finite() => n,
                _ => DOC_RANGE_PLANNER_MAX_CANDIDATE_BLOCK_RATIO,
            },
            conjunction_tail: get("conjunctionTail") != &Value::Bool(false),
            text_doc_page_hydration: get("textDocPageHydration") != &Value::Bool(false),
            text_doc_page_max_overfetch_docs: if truthy(get("textDocPageMaxOverfetchDocs")) {
                Some(crate::util::json::to_number(get("textDocPageMaxOverfetchDocs")))
            } else {
                None
            },
            segment_fanout: get("segmentFanout").as_bool(),
            authority: get("authority") != &Value::Bool(false),
            query_bundles: get("queryBundles") != &Value::Bool(false),
            doc_values_binary_manifest: get("docValuesBinaryManifest") != &Value::Bool(false),
            trace: truthy(get("trace")),
            range_plans: plans,
            raw,
        }
    }

    pub fn get(&self, key: &str) -> &Value {
        path(&self.raw, &[key])
    }
}

/// `cacheBudgetBytes`: bytes of decoded objects the engine keeps cached
/// between calls (shared by a federation's children). `0` disables the
/// budget (unbounded caches); missing or invalid values use
/// [`crate::budget::DEFAULT_CACHE_BUDGET_BYTES`].
pub fn cache_budget_bytes(options: &EngineOptions) -> usize {
    match options.get("cacheBudgetBytes").as_f64() {
        Some(bytes) if bytes.is_finite() && bytes >= 0.0 => bytes.min(usize::MAX as f64).floor() as usize,
        _ => crate::budget::DEFAULT_CACHE_BUDGET_BYTES,
    }
}
