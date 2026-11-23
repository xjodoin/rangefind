//! Query bundles and the segment fan-out exact lane: `tryQueryBundleSearch`,
//! `loadQueryBundle`, `filterQueryBundleRows*`, `runSegmentFanoutSearch`,
//! `segmentTermEntries`, `loadSegmentTerms` and
//! `decodeSegmentEntryPostings` from `src/runtime.js`.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::codec::doc_values::{DocValue, FacetValue};
use crate::codec::query_bundle::{parse_query_bundle, BundleFilterValue, QueryBundle};
use crate::codec::segment::{decode_segment_rows, parse_segment_terms, SegmentTermEntry, SegmentTerms};
use crate::error::Result;
use crate::rt::{try_join_all, PromiseCache};
use crate::terms::query_bundle_keys_from_base_terms;
use crate::util::json::{num, path, to_number, truthy};

use super::docs::{PreferDocPages, ResultContext, Row};
use super::filters::{block_definitely_passes, block_may_pass, DocFilterPlan, ValueStore};
use super::index::{min_should_match_for, IndexEngine};
use super::io::inflate_object;
use super::text::{empty_rerank_stats, empty_text_response, js_batches, js_index, js_max, js_min, json_number, page_slice, Accumulator, ProofStats, TextRequest};

const STATE_KEY: &str = "bundles";

/// Lane caches (`queryBundleCache`, `segmentTermsCache`, `segmentRowsCache`).
struct BundleState {
    bundles: PromiseCache<String, Option<(Rc<QueryBundle>, u64)>>,
    segment_terms: PromiseCache<String, Option<Rc<SegmentTerms>>>,
    segment_rows: PromiseCache<String, Rc<Vec<i32>>>,
}

/// The outcome of `filterQueryBundleRows`.
struct FilterResult {
    ranked: Vec<Row>,
    top_k_proven: bool,
    scanned: usize,
    accepted: usize,
    exhausted: bool,
    progressive: bool,
    value_source: &'static str,
    used_doc_values: bool,
    filter_proof: &'static str,
}

/// One `{ term, segment, segmentOrdinal, entry }` of `segmentTermEntries`.
struct SegmentHit {
    term: String,
    segment: Value,
    ordinal: usize,
    entry: SegmentTermEntry,
}

/// `bundleProvesTopK`. `bundle.rows[k - 1]` with a fractional `k` reads
/// `undefined` (no proof).
fn bundle_proves_top_k(bundle: &QueryBundle, k: f64) -> bool {
    if bundle.complete {
        return true;
    }
    if k > bundle.rows.len() as f64 || bundle.rows.is_empty() {
        return false;
    }
    let Some(&boundary) = js_index(&bundle.rows, k - 1.0) else { return false };
    if bundle.next_score_bound < boundary.1 {
        return true;
    }
    bundle.next_score_bound == boundary.1 && bundle.next_tie_doc.map(|tie| tie > boundary.0).unwrap_or(false)
}

/// `queryBundleFilteredTopKProven`. `exhausted`: every candidate row has
/// been filtered. A complete bundle holds every matching doc, so it proves
/// the filtered top k once k rows passed or no candidate row is left.
fn filtered_top_k_proven(bundle: &QueryBundle, ranked: &[Row], k: f64, exhausted: bool) -> bool {
    if bundle.complete {
        return exhausted || ranked.len() as f64 >= k;
    }
    if (ranked.len() as f64) < k {
        return false;
    }
    match js_index(ranked, k - 1.0) {
        Some(boundary) => (bundle.next_score_bound as f64) < boundary.1,
        None => false,
    }
}

/// A bundle's embedded per-row filter value as the JS value it decodes to.
fn bundle_doc_value(value: &BundleFilterValue) -> DocValue {
    match value {
        BundleFilterValue::Facet(codes) => DocValue::Facet(FacetValue::Codes(codes.clone())),
        BundleFilterValue::Boolean(Some(flag)) => DocValue::Bool(*flag),
        BundleFilterValue::Number(Some(number)) => DocValue::Number(*number),
        BundleFilterValue::Boolean(None) | BundleFilterValue::Number(None) => DocValue::Missing,
    }
}

impl IndexEngine {
    fn bundle_state(&self) -> Rc<BundleState> {
        if let Some(state) = self.extension::<BundleState>(STATE_KEY) {
            return state;
        }
        let budget = self.cache_budget();
        let state = Rc::new(BundleState {
            bundles: PromiseCache::budgeted(budget, "queryBundleRows"),
            segment_terms: PromiseCache::budgeted(budget, "segmentTerms"),
            segment_rows: PromiseCache::budgeted(budget, "segmentRows"),
        });
        self.set_extension(STATE_KEY, state.clone());
        state
    }

    // ------------------------------------------------------------ bundles

    /// `loadQueryBundle`: the parsed bundle and its compressed byte length.
    async fn load_query_bundle(&self, key: &str) -> Result<Option<(Rc<QueryBundle>, u64)>> {
        let Some(directory) = self.query_bundle_directory.clone() else { return Ok(None) };
        if !self.options.query_bundles {
            return Ok(None);
        }
        let state = self.bundle_state();
        let verify = self.verify_checksums;
        state
            .bundles
            .get_or_load(key.to_string(), async {
                let root = self.load_directory_root(&directory).await?;
                let Some(entry) = self.directory_entry_from_root(&directory, &root, key).await? else { return Ok(None) };
                let buffer = self.io.fetch_range(&self.url(&format!("bundles/packs/{}", entry.pack)), entry.offset, entry.length).await?;
                let inflated = inflate_object(&buffer, &entry, &format!("query bundle {key}"), verify)?;
                let manifest = self.posting_manifest.borrow().clone();
                let bundle = parse_query_bundle(&inflated, &manifest)?;
                Ok(Some((Rc::new(bundle), entry.length)))
            })
            .await
    }

    /// `queryBundleFilterValueFields`.
    fn query_bundle_filter_value_fields(bundle: &QueryBundle, plan: &DocFilterPlan) -> Vec<String> {
        if !plan.active || bundle.filter_values.is_empty() {
            return Vec::new();
        }
        let mut out: Vec<String> = Vec::new();
        for field in Self::filter_plan_fields(Some(plan)) {
            if bundle.filter_values.iter().any(|(name, _)| *name == field) && !out.contains(&field) {
                out.push(field);
            }
        }
        out
    }

    /// `passesFilterPlanWithKnown` with several known fields (a known value
    /// of `undefined` — `None` — falls back to the value store).
    fn passes_with_known_values(&self, doc: i32, store: Option<&ValueStore>, plan: &DocFilterPlan, known: &[(String, Option<DocValue>)]) -> bool {
        if !plan.active {
            return true;
        }
        let value = |field: &str| -> DocValue {
            match known.iter().find(|(name, _)| name == field) {
                Some((_, Some(value))) => value.clone(),
                _ => self.value_for_doc(store, field, doc),
            }
        };
        let bitmaps = store.and_then(|s| s.bitmaps.as_deref());
        for (field, selected) in &plan.facets {
            if let Some((_, list)) = bitmaps.and_then(|b| b.facets.iter().find(|(name, _)| name == field)) {
                if !list.iter().any(|bitmap| bitmap.has(doc as i64)) {
                    return false;
                }
                continue;
            }
            if !value(field).facet_matches(selected) {
                return false;
            }
        }
        for (field, range) in &plan.numbers {
            let Some(number) = value(field).as_number() else { return false };
            if range.min.map(|min| number < min).unwrap_or(false) || range.max.map(|max| number > max).unwrap_or(false) {
                return false;
            }
        }
        for (field, expected) in &plan.booleans {
            if let Some((_, bitmap)) = bitmaps.and_then(|b| b.booleans.iter().find(|(name, _)| name == field)) {
                if !bitmap.has(doc as i64) {
                    return false;
                }
                continue;
            }
            match value(field).as_bool() {
                Some(flag) if flag == *expected => {}
                _ => return false,
            }
        }
        if let Some(geo) = &plan.geo {
            if !geo.doc_matches(self, store, doc, None) {
                return false;
            }
        }
        true
    }

    /// `filterQueryBundleRowsWithEmbeddedValues`.
    async fn filter_query_bundle_rows_with_embedded_values(
        &self,
        bundle: &QueryBundle,
        candidate_rows: &[Row],
        plan: &DocFilterPlan,
        k: f64,
        embedded: &[String],
    ) -> Result<FilterResult> {
        let batch_size = js_max(8.0, js_min(32.0, k));
        let mut ranked: Vec<Row> = Vec::new();
        let mut scanned = 0usize;
        let mut used_doc_values = false;
        let omitted: Vec<&str> = embedded.iter().map(String::as_str).collect();
        let needs_store = Self::filter_plan_fields(Some(plan)).iter().any(|field| !embedded.contains(field));
        let source = |used: bool| if used { "queryBundleRows+docValues" } else { "queryBundleRows" };
        for (from, to) in js_batches(candidate_rows.len(), batch_size) {
            let batch = &candidate_rows[from..to];
            let store = if needs_store {
                let docs: Vec<i32> = batch.iter().map(|row| row.0).collect();
                self.value_store_for_filter_plan(plan, &docs, &omitted).await?
            } else {
                None
            };
            if store.as_ref().map(|s| s.doc_values).unwrap_or(false) {
                used_doc_values = true;
            }
            scanned += batch.len();
            for row in batch {
                let known: Vec<(String, Option<DocValue>)> =
                    embedded.iter().map(|field| (field.clone(), bundle.filter_value(field, row.0 as u64).map(bundle_doc_value))).collect();
                if self.passes_with_known_values(row.0, store.as_ref(), plan, &known) {
                    ranked.push(*row);
                }
            }
            if filtered_top_k_proven(bundle, &ranked, k, scanned >= candidate_rows.len()) {
                let value_source = source(used_doc_values);
                let accepted = ranked.len();
                return Ok(FilterResult {
                    ranked,
                    top_k_proven: true,
                    scanned,
                    accepted,
                    exhausted: scanned >= candidate_rows.len(),
                    progressive: true,
                    value_source,
                    used_doc_values,
                    filter_proof: value_source,
                });
            }
        }
        let top_k_proven = filtered_top_k_proven(bundle, &ranked, k, true);
        let accepted = ranked.len();
        Ok(FilterResult {
            ranked,
            top_k_proven,
            scanned,
            accepted,
            exhausted: true,
            progressive: false,
            value_source: source(used_doc_values),
            used_doc_values,
            filter_proof: "",
        })
    }

    /// `filterQueryBundleRows`.
    async fn filter_query_bundle_rows(
        &self,
        bundle: &QueryBundle,
        candidate_rows: Vec<Row>,
        plan: &DocFilterPlan,
        k: f64,
        summary_proves_filters: bool,
    ) -> Result<FilterResult> {
        if summary_proves_filters {
            let top_k_proven = filtered_top_k_proven(bundle, &candidate_rows, k, true);
            let accepted = candidate_rows.len();
            return Ok(FilterResult {
                ranked: candidate_rows,
                top_k_proven,
                scanned: 0,
                accepted,
                exhausted: true,
                progressive: false,
                value_source: "rowGroupSummary",
                used_doc_values: false,
                filter_proof: "rowGroupSummary",
            });
        }
        let embedded = Self::query_bundle_filter_value_fields(bundle, plan);
        if !embedded.is_empty() {
            return self.filter_query_bundle_rows_with_embedded_values(bundle, &candidate_rows, plan, k, &embedded).await;
        }
        let batch_size = js_max(8.0, js_min(32.0, k));
        let mut ranked: Vec<Row> = Vec::new();
        let mut scanned = 0usize;
        for (from, to) in js_batches(candidate_rows.len(), batch_size) {
            let batch = &candidate_rows[from..to];
            let docs: Vec<i32> = batch.iter().map(|row| row.0).collect();
            let Some(store) = self.value_store_for_filter_plan(plan, &docs, &[]).await? else {
                let accepted = ranked.len();
                return Ok(FilterResult {
                    ranked,
                    top_k_proven: false,
                    scanned,
                    accepted,
                    exhausted: false,
                    progressive: true,
                    value_source: "",
                    used_doc_values: false,
                    filter_proof: "",
                });
            };
            scanned += batch.len();
            for row in batch {
                if self.passes_filter_plan(row.0, Some(&store), plan, None) {
                    ranked.push(*row);
                }
            }
            if filtered_top_k_proven(bundle, &ranked, k, scanned >= candidate_rows.len()) {
                let accepted = ranked.len();
                return Ok(FilterResult {
                    ranked,
                    top_k_proven: true,
                    scanned,
                    accepted,
                    exhausted: scanned >= candidate_rows.len(),
                    progressive: true,
                    value_source: "docValues",
                    used_doc_values: true,
                    filter_proof: "progressiveDocValues",
                });
            }
        }
        let top_k_proven = filtered_top_k_proven(bundle, &ranked, k, true);
        let accepted = ranked.len();
        Ok(FilterResult {
            ranked,
            top_k_proven,
            scanned,
            accepted,
            exhausted: true,
            progressive: false,
            value_source: "docValues",
            used_doc_values: true,
            filter_proof: "",
        })
    }

    /// `tryQueryBundleSearch` (always unsorted: the sorted branch never
    /// reaches it).
    pub(crate) async fn try_query_bundle_search(&self, request: &TextRequest) -> Result<Option<Value>> {
        let offset = request.offset();
        let size = request.size;
        let k = request.k();
        let has_filters = request.filters.has_filters();
        let max_rows = to_number(&self.manifest_at(&["query_bundles", "max_rows"]));
        let max_rows = if max_rows.is_nan() { 0.0 } else { max_rows };
        if request.base_terms.is_empty() || k > max_rows {
            return Ok(None);
        }
        if self.manifest_at(&["query_bundles", "coverage"]).as_str() != Some("all-base-docs") {
            return Ok(None);
        }
        if request.rerank && !self.dependency_terms(&request.base_terms).is_empty() {
            return Ok(None);
        }
        if !request.filters.facets.is_empty() {
            self.ensure_facet_dictionaries(&request.filters.facet_fields()).await?;
        }
        let doc_plan = if has_filters { Some(self.make_doc_filter_plan(&request.filters)?) } else { None };
        let block_plan = if has_filters { Some(self.make_block_filter_plan(&request.filters)?) } else { None };

        let mut lookups = 0usize;
        // A bundle stores the exact ranking of its own base terms: only one
        // over the query's full base-term list answers the query.
        let plans = query_bundle_keys_from_base_terms(&request.base_terms)
            .into_iter()
            .filter(|plan| plan.base_terms == request.base_terms);
        for plan in plans {
            lookups += 1;
            let Some((bundle, bytes)) = self.load_query_bundle(&plan.key).await? else { continue };
            let all_rows = || -> Vec<Row> { bundle.rows.iter().map(|(doc, score)| (*doc as i32, *score as f64)).collect() };
            let candidate_groups: Option<Vec<usize>> = if has_filters && !bundle.row_groups.is_empty() {
                Some((0..bundle.row_groups.len()).filter(|i| block_may_pass(&bundle.row_groups[*i].filters, block_plan.as_ref())).collect())
            } else {
                None
            };
            let candidate_rows: Vec<Row> = match &candidate_groups {
                Some(groups) => {
                    let mut out = Vec::new();
                    for &index in groups {
                        let group = &bundle.row_groups[index];
                        let start = (group.row_start as usize).min(bundle.rows.len());
                        let end = (group.row_start.saturating_add(group.row_count) as usize).min(bundle.rows.len());
                        out.extend(bundle.rows[start..end].iter().map(|(doc, score)| (*doc as i32, *score as f64)));
                    }
                    out
                }
                None => all_rows(),
            };
            let candidate_count = candidate_rows.len();
            let summary_proves_filters = has_filters
                && candidate_groups
                    .as_ref()
                    .map(|groups| {
                        !groups.is_empty() && groups.iter().all(|i| block_definitely_passes(&bundle.row_groups[*i].filters, doc_plan.as_ref()))
                    })
                    .unwrap_or(false);
            let filter_result = match &doc_plan {
                Some(plan) => Some(self.filter_query_bundle_rows(&bundle, candidate_rows, plan, k, summary_proves_filters).await?),
                None => None,
            };
            let proven = match &filter_result {
                Some(result) => result.top_k_proven,
                None => bundle_proves_top_k(&bundle, k),
            };
            if !proven {
                continue;
            }
            let ranked: Vec<Row> = match &filter_result {
                Some(result) => result.ranked.clone(),
                None => all_rows(),
            };
            let rows: Vec<Row> = page_slice(&ranked, offset, size);
            let mut context = ResultContext::new(true, PreferDocPages::Auto);
            let results = self.rows_to_search_results(&rows, &mut context, request.include_results).await?;
            let total_exact = !has_filters || (bundle.complete && filter_result.as_ref().map(|r| r.exhausted).unwrap_or(true));
            let total = if !has_filters {
                json!(bundle.total)
            } else if total_exact {
                json!(ranked.len())
            } else {
                json_number(js_max(candidate_count as f64, k))
            };
            let value_source = filter_result.as_ref().map(|r| r.value_source).unwrap_or("");
            let uses_doc_values = filter_result.as_ref().map(|r| r.used_doc_values).unwrap_or(false) || value_source == "docValues";
            let scanned = filter_result.as_ref().map(|r| r.scanned).unwrap_or(0);
            let accepted = filter_result.as_ref().map(|r| r.accepted).unwrap_or(0);
            let mut stats = Map::new();
            for (key, value) in [
                ("exact", json!(true)),
                ("plannerLane", json!("queryBundleExact")),
                ("topKProven", json!(true)),
                ("totalExact", json!(total_exact)),
                ("tailExhausted", json!(false)),
                ("blocksDecoded", json!(0)),
                ("postingsDecoded", json!(0)),
                ("postingsAccepted", json!(0)),
                ("skippedBlocks", json!(0)),
                ("terms", json!(plan.expanded_terms.len())),
                ("shards", json!(0)),
                ("queryBundleLookups", json!(lookups)),
                ("queryBundleHit", json!(true)),
                ("queryBundleFiltered", json!(has_filters)),
                ("queryBundleRows", json!(bundle.rows.len())),
                ("queryBundleRowGroups", json!(bundle.row_groups.len())),
                ("queryBundleRowGroupsScanned", json!(candidate_groups.as_ref().map(Vec::len).unwrap_or(bundle.row_groups.len()))),
                ("queryBundleRowsAccepted", json!(ranked.len())),
                ("queryBundleTotal", if has_filters { json!(ranked.len()) } else { json!(bundle.total) }),
                ("queryBundleBytes", json!(bytes)),
                ("queryBundleComplete", json!(bundle.complete)),
                ("queryBundleFilterProof", json!(filter_result.as_ref().map(|r| r.filter_proof).unwrap_or(""))),
                ("queryBundleFilterProgressive", json!(filter_result.as_ref().map(|r| r.progressive).unwrap_or(false))),
                ("queryBundleFilterExhausted", json!(filter_result.as_ref().map(|r| r.exhausted).unwrap_or(true))),
                ("queryBundleFilterValueSource", json!(value_source)),
                ("queryBundleFilterRowsScanned", json!(if has_filters { scanned } else { 0 })),
                ("queryBundleFilterRowsAccepted", json!(if has_filters { accepted } else { 0 })),
                ("docValueRowsScanned", json!(if has_filters && uses_doc_values { scanned } else { 0 })),
                ("docValueRowsAccepted", json!(if has_filters && uses_doc_values { accepted } else { 0 })),
            ] {
                stats.insert(key.into(), value);
            }
            context.stats(&mut stats, true);
            empty_rerank_stats(&mut stats);
            return Ok(Some(json!({
                "total": total,
                "page": json_number(request.page),
                "size": size,
                "approximate": !total_exact,
                "results": results,
                "stats": Value::Object(stats)
            })));
        }
        Ok(None)
    }

    // ------------------------------------------------------------ segments

    /// `loadSegmentTerms`.
    async fn load_segment_terms(&self, segment: &Value) -> Result<Option<Rc<SegmentTerms>>> {
        let key = match segment.get("id") {
            Some(id) if truthy(id) => id.as_str().map(str::to_string).unwrap_or_else(|| id.to_string()),
            _ => js_string(segment.get("ordinal").unwrap_or(&Value::Null)),
        };
        let terms_path = path(segment, &["files", "terms", "path"]).as_str().map(str::to_string);
        let state = self.bundle_state();
        state
            .segment_terms
            .get_or_load(key, async {
                let Some(terms_path) = terms_path.filter(|p| !p.is_empty()) else { return Ok(None) };
                let bytes = self.io.fetch(&self.url(&terms_path)).await?;
                Ok(Some(Rc::new(parse_segment_terms(&bytes)?)))
            })
            .await
    }

    /// `segmentTermEntries`: (segment manifest, entries, summed dfs).
    async fn segment_term_entries(&self, terms: &[String]) -> Result<Option<(Rc<Value>, Vec<SegmentHit>, HashMap<String, f64>)>> {
        let Some(meta) = self.load_segment_manifest().await? else { return Ok(None) };
        let segments: Vec<Value> = meta.get("segments").and_then(Value::as_array).cloned().unwrap_or_default();
        if !truthy(meta.get("published").unwrap_or(&Value::Null)) || segments.is_empty() || self.options.segment_fanout == Some(false) {
            return Ok(None);
        }
        let dictionaries = try_join_all(segments.iter().map(|segment| self.load_segment_terms(segment))).await?;
        let mut entries = Vec::new();
        let mut dfs: HashMap<String, f64> = HashMap::new();
        for term in terms {
            for (ordinal, segment) in segments.iter().enumerate() {
                let Some(entry) = dictionaries[ordinal].as_ref().and_then(|dictionary| dictionary.get(term)) else { continue };
                let df = if entry.df != 0 { entry.df } else { entry.count };
                *dfs.entry(term.clone()).or_insert(0.0) += df as f64;
                entries.push(SegmentHit { term: term.clone(), segment: segment.clone(), ordinal, entry: entry.clone() });
            }
        }
        Ok(Some((meta, entries, dfs)))
    }

    fn segment_hit_id(hit: &SegmentHit) -> String {
        match hit.segment.get("id") {
            Some(id) if truthy(id) => id.as_str().map(str::to_string).unwrap_or_else(|| id.to_string()),
            _ => hit.ordinal.to_string(),
        }
    }

    /// `decodeSegmentEntryPostings`.
    async fn decode_segment_entry_postings(&self, hit: &SegmentHit, df: Option<f64>) -> Result<Rc<Vec<i32>>> {
        let Some(postings_path) = path(&hit.segment, &["files", "postings", "path"]).as_str().filter(|p| !p.is_empty()).map(str::to_string) else {
            return Ok(Rc::new(Vec::new()));
        };
        let key = format!("{}\u{0}{}", Self::segment_hit_id(hit), hit.term);
        let total = to_number(&self.manifest_at(&["total"]));
        let total = if total.is_nan() { None } else { Some(total) };
        let state = self.bundle_state();
        state
            .segment_rows
            .get_or_load(key, async {
                let bytes = self.io.fetch_range(&self.url(&postings_path), hit.entry.offset, hit.entry.bytes).await?;
                Ok(Rc::new(decode_segment_rows(&bytes, &hit.entry, df, total)?))
            })
            .await
    }

    /// `runSegmentFanoutSearch`.
    pub(crate) async fn run_segment_fanout_search(&self, request: &TextRequest, reason: &str) -> Result<Option<Value>> {
        let offset = request.offset();
        let sort_plan = self.make_sort_plan(&request.sort);
        let has_filters = request.filters.has_filters();
        if sort_plan.is_some() {
            self.ensure_doc_values_manifest().await?;
        }
        self.ensure_facet_dictionaries(&request.filters.facet_fields()).await?;
        let Some((meta, entries, dfs)) = self.segment_term_entries(&request.terms).await? else { return Ok(None) };
        let base_set: HashSet<&str> = request.base_terms.iter().map(String::as_str).collect();
        let min_should_match = min_should_match_for(request.base_terms.len());
        let present: HashSet<&str> = entries.iter().filter(|hit| base_set.contains(hit.term.as_str())).map(|hit| hit.term.as_str()).collect();
        if present.len() < min_should_match.max(1) {
            let mut response =
                empty_text_response(request.page, request.size, request.terms.len(), &[], request.base_terms.len().saturating_sub(present.len()));
            // `new Set(entries.map(item => item.shardName)).size`: segment
            // entries carry no shard name, so every entry maps to `undefined`.
            response["stats"]["shards"] = json!(if entries.is_empty() { 0 } else { 1 });
            return Ok(Some(response));
        }

        let mut decoded: Vec<(bool, Rc<Vec<i32>>)> = Vec::with_capacity(entries.len());
        let mut postings_decoded = 0usize;
        for hit in &entries {
            let rows = self.decode_segment_entry_postings(hit, dfs.get(&hit.term).copied()).await?;
            postings_decoded += rows.len() / 2;
            decoded.push((base_set.contains(hit.term.as_str()), rows));
        }

        let doc_plan = if has_filters { Some(self.make_doc_filter_plan(&request.filters)?) } else { None };
        let mut store: Option<ValueStore> = if sort_plan.is_some() && self.doc_values_meta().is_none() {
            Some(ValueStore { codes: Some(self.load_codes().await?), ..Default::default() })
        } else {
            None
        };

        let mut acc = Accumulator::default();
        let mut postings_accepted = 0usize;
        for (is_base, rows) in &decoded {
            for pair in rows.chunks_exact(2) {
                acc.add_score(pair[0], pair[1] as i64);
                if *is_base {
                    acc.add_hit(pair[0]);
                }
                postings_accepted += 1;
            }
        }
        let mut ranked: Vec<Row> = acc.eligible(min_should_match).into_iter().map(|(doc, score)| (doc, score as f64)).collect();
        if let Some(plan) = &doc_plan {
            let docs: Vec<i32> = ranked.iter().map(|(doc, _)| *doc).collect();
            let filter_store = self.value_store_for_filter_plan(plan, &docs, &[]).await?;
            ranked.retain(|(doc, _)| self.passes_filter_plan(*doc, filter_store.as_ref(), plan, None));
            if store.is_none() {
                store = filter_store;
            }
        }
        let (mut ranked, rerank_stats) = if !request.rerank || sort_plan.is_some() {
            let mut stats = Map::new();
            empty_rerank_stats(&mut stats);
            (ranked, stats)
        } else {
            let limit = self.candidate_limit_for(&request.base_terms, request.k(), request.rerank);
            let outcome = self.rerank_with_dependencies(ranked, &request.base_terms, limit).await?;
            (outcome.ranked, outcome.stats)
        };
        if let Some(sort) = &sort_plan {
            if self.doc_values_meta().is_some() {
                let docs: Vec<i32> = ranked.iter().map(|(doc, _)| *doc).collect();
                store = self.value_store_for_docs(&[sort.field.clone()], &docs).await?;
            }
            self.sort_ranked(&mut ranked, store.as_ref(), sort);
        }
        let rows: Vec<Row> = page_slice(&ranked, offset, request.size);
        let mut context =
            ResultContext::new(!request.base_terms.is_empty(), if sort_plan.is_some() { PreferDocPages::Yes } else { PreferDocPages::Auto });
        let results = self.rows_to_search_results(&rows, &mut context, request.include_results).await?;
        let shards: HashSet<String> = entries.iter().map(Self::segment_hit_id).collect();
        let segment_count = {
            let count = meta.get("segmentCount").unwrap_or(&Value::Null);
            if truthy(count) {
                count.clone()
            } else {
                json!(meta.get("segments").and_then(Value::as_array).map(Vec::len).unwrap_or(0))
            }
        };
        let mut stats = Map::new();
        for (key, value) in [
            ("exact", json!(true)),
            ("plannerLane", json!("segmentFanoutExact")),
            ("topKProven", json!(true)),
            ("totalExact", json!(true)),
            ("tailExhausted", json!(true)),
            ("terms", json!(request.terms.len())),
            ("shards", json!(shards.len())),
            ("segments", segment_count),
            ("segmentEntries", json!(entries.len())),
            ("segmentPublished", json!(true)),
            ("postings", json!(postings_decoded)),
            ("blocksDecoded", json!(entries.len())),
            ("postingsDecoded", json!(postings_decoded)),
            ("postingsAccepted", json!(postings_accepted)),
            ("skippedBlocks", json!(0)),
            ("plannerFallbackReason", json!(reason)),
        ] {
            stats.insert(key.into(), value);
        }
        ProofStats::write(None, reason, &mut stats);
        context.stats(&mut stats, true);
        stats.extend(rerank_stats);
        Ok(Some(json!({
            "total": ranked.len(),
            "page": json_number(request.page),
            "size": request.size,
            "results": results,
            "approximate": false,
            "stats": Value::Object(stats)
        })))
    }
}

/// `String(value)` for the scalar ids segment manifests carry.
fn js_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(n) => match n.as_f64() {
            Some(f) => match num(f) {
                Value::Number(n) => n.to_string(),
                other => other.to_string(),
            },
            None => n.to_string(),
        },
        Value::Null => "undefined".into(),
        other => other.to_string(),
    }
}
