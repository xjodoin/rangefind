export const INDEX_OPTIMIZER_FORMAT = "rfoptimizer-v1";
export const INDEX_OPTIMIZER_PATH = "debug/index-optimizer.json";

function finiteNumber(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function positiveInteger(value, fallback = 0) {
  return Math.max(0, Math.round(finiteNumber(value, fallback)));
}

function stat(stats, key) {
  return positiveInteger(stats?.[key] || 0);
}

function estimateIndexBytes(stats = {}) {
  return [
    "posting_segment_pack_bytes",
    "posting_segment_block_pack_bytes",
    "posting_segment_directory_bytes",
    "doc_pack_bytes",
    "doc_page_pack_bytes",
    "doc_pointer_bytes",
    "doc_page_pointer_bytes",
    "doc_value_pack_bytes",
    "doc_value_sorted_pack_bytes",
    "doc_value_sorted_directory_bytes",
    "facet_dictionary_bytes",
    "query_bundle_pack_bytes",
    "query_bundle_directory_bytes",
    "authority_pack_bytes",
    "authority_directory_bytes"
  ].reduce((sum, key) => sum + stat(stats, key), 0);
}

function optimizerBudget(config, stats) {
  const ratio = Math.max(0, finiteNumber(config.optimizationBudgetRatio, 0.08));
  const maxBytes = positiveInteger(config.optimizationBudgetMaxBytes, 50 * 1024 * 1024);
  const baseBytes = estimateIndexBytes(stats);
  const ratioBudget = Math.round(baseBytes * ratio);
  const bytes = maxBytes > 0 ? Math.min(maxBytes, ratioBudget) : ratioBudget;
  return {
    ratio,
    max_bytes: maxBytes,
    base_index_bytes: baseBytes,
    bytes
  };
}

function deferredMaterializations() {
  return [
    {
      kind: "champion-window",
      status: "deferred",
      reason: "wait_for_core_benchmark"
    },
    {
      kind: "phrase-materialization",
      status: "deferred",
      reason: "wait_for_core_benchmark"
    },
    {
      kind: "term-sort-materialization",
      status: "deferred",
      reason: "wait_for_core_benchmark"
    },
    {
      kind: "learned-sparse-import",
      status: "deferred",
      reason: "wait_for_exact_core_path"
    }
  ];
}

function coreDecisions(config, manifest) {
  const stats = manifest.stats || {};
  const superblocks = stat(stats, "posting_segment_superblocks");
  const layout = config._layoutDecisions || {};
  const codecMode = String(config.codecs?.mode || layout.codecs?.mode || stat(stats, "posting_segment_codec_mode") || "auto");
  const fieldCounts = {
    facets: Object.keys(manifest.facet_dictionaries?.fields || manifest.facets || {}).length,
    numbers: manifest.numbers?.length || 0,
    booleans: manifest.booleans?.length || 0,
    sorts: manifest.sorts?.length || 0,
    sorted_fields: stat(stats, "doc_value_sorted_fields")
  };

  return [
    {
      kind: "top-k-proof",
      status: "instrumented",
      scope: "runtime",
      current: ["block_max", "tail_exhaustion", "candidate_count", "tie_bound_doc", "remaining_term_upper_bound", "filter_unknown_reason"],
      next: ["sort_bound"],
      reason: "make exact top-k proof failures measurable before changing layout"
    },
    {
      kind: "posting-superblocks",
      status: superblocks > 0 ? "current" : "planned",
      scope: "rfsegpost",
      candidate_terms: stat(stats, "external_posting_segment_terms"),
      candidate_blocks: stat(stats, "external_posting_segment_blocks"),
      candidate_postings: stat(stats, "external_posting_segment_postings"),
      superblock_size: positiveInteger(config.postingSuperblockSize, stat(stats, "posting_segment_superblock_size")),
      superblocks,
      covered_terms: stat(stats, "posting_segment_superblock_terms"),
      covered_blocks: stat(stats, "posting_segment_superblock_blocks"),
      scheduler_status: superblocks > 0 ? "current" : "planned",
      reason: "skip competitive range-fetch groups before decoding child posting blocks"
    },
    {
      kind: "posting-block-scheduler",
      status: "current",
      scope: "runtime",
      block_size: positiveInteger(config.postingBlockSize, stat(stats, "posting_segment_block_size")),
      external_blocks: config.externalPostingBlocks !== false,
      reason: "baseline scheduler for the superblock refactor"
    },
    {
      kind: "doc-range-block-max",
      status: stat(stats, "posting_segment_doc_range_entries") > 0 ? "current" : "planned",
      scope: "rfsegpost",
      range_size: positiveInteger(config.postingDocRangeSize, stat(stats, "posting_segment_doc_range_size")),
      quantization_bits: positiveInteger(config.postingDocRangeQuantizationBits, stat(stats, "posting_segment_doc_range_quantization_bits")),
      covered_terms: stat(stats, "posting_segment_doc_range_terms"),
      range_entries: stat(stats, "posting_segment_doc_range_entries"),
      covered_blocks: stat(stats, "posting_segment_doc_range_blocks"),
      block_range_entries: stat(stats, "posting_segment_doc_range_block_entries"),
      reason: "safe docID-range upper bounds for block-max range planning"
    },
    {
      kind: "codec-layout",
      status: codecMode === "off" ? "off" : "current",
      scope: "posting-blocks",
      mode: codecMode,
      selected_codec: layout.codecs?.selected_posting_codec || stats.posting_segment_codec || "varint-impact-gzip-member",
      block_size: positiveInteger(config.postingBlockSize, stat(stats, "posting_segment_block_size")),
      block_size_source: layout.posting_block_size?.source || stats.posting_segment_block_size_source || "configured",
      superblock_size: positiveInteger(config.postingSuperblockSize, stat(stats, "posting_segment_superblock_size")),
      superblock_size_source: layout.posting_superblock_size?.source || stats.posting_segment_superblock_size_source || "configured",
      pair_varint_blocks: stat(stats, "posting_segment_block_codec_pair_varint_blocks"),
      impact_run_blocks: stat(stats, "posting_segment_block_codec_impact_run_blocks"),
      impact_bitset_blocks: stat(stats, "posting_segment_block_codec_impact_bitset_blocks"),
      partitioned_delta_blocks: stat(stats, "posting_segment_block_codec_partitioned_delta_blocks"),
      baseline_bytes: stat(stats, "posting_segment_block_codec_baseline_bytes"),
      selected_bytes: stat(stats, "posting_segment_block_codec_selected_bytes"),
      bytes_saved: stat(stats, "posting_segment_block_codec_bytes_saved"),
      avg_postings_per_term: Math.round(finiteNumber(layout.corpus?.avg_postings_per_term, 0) * 100) / 100,
      candidates: ["partitioned-elias-fano", "dense-bitset-container", "compact-impact-array"],
      reason: "optimize transferred bytes and browser decode work from corpus statistics"
    },
    {
      kind: "doc-id-layout",
      status: manifest.features?.docLocalityLayout ? "current" : "planned",
      scope: "docs-and-postings",
      current_strategy: manifest.docs?.layout?.strategy || "",
      primary_terms: stat(stats, "doc_layout_primary_terms"),
      reason: "improve compression and filter locality without changing external document ids"
    },
    {
      kind: "filter-summaries",
      status: manifest.block_filters ? "current" : "planned",
      scope: "posting-blocks",
      fields: fieldCounts.facets + fieldCounts.numbers + fieldCounts.booleans,
      max_facet_words: positiveInteger(config.blockFilterMaxFacetWords, 0),
      reason: "move filter pruning into the same proof path as block-max"
    },
    {
      kind: "sort-summaries",
      status: fieldCounts.sorted_fields > 0 ? "current" : "planned",
      scope: "doc-value-sorted-pages",
      fields: fieldCounts.sorted_fields,
      page_size: stat(stats, "doc_value_sorted_page_size"),
      reason: "make q plus sort prove earlier through core sorted value pages"
    }
  ];
}

export function buildIndexOptimizerReport({ config, manifest }) {
  const budget = optimizerBudget(config, manifest.stats || {});
  const core = coreDecisions(config, manifest);
  const deferred = deferredMaterializations();
  const selectedBytes = 0;

  return {
    format: INDEX_OPTIMIZER_FORMAT,
    status: "scaffold",
    generated_at: manifest.built_at,
    policy: "core-first",
    budget,
    selected_bytes: selectedBytes,
    core,
    deferred,
    rejected: [],
    summary: {
      format: INDEX_OPTIMIZER_FORMAT,
      status: "scaffold",
      path: INDEX_OPTIMIZER_PATH,
      policy: "core-first",
      budget_bytes: budget.bytes,
      selected_bytes: selectedBytes,
      core_decisions: core.length,
      deferred_decisions: deferred.length
    }
  };
}
