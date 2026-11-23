//! Request orchestration (`executeSearch`, `search`, `count`, browse and
//! sorted lanes, facet counts, rank-prior boost) from `src/runtime.js`.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::codec::doc_values::DocValue;
use crate::error::{Error, Result};
use crate::highlight::{apply_highlights, highlight_term_set, HighlightOptions};
use crate::util::json::{num, num_or, to_number, truthy, NULL};

use super::docs::{PreferDocPages, ResultContext, Row};
use super::filters::{page_definitely_passes, page_may_pass, DocFilterPlan, Filters, ValueStore};
use super::geo_plan::GeoPlan;
use super::index::{min_should_match_for, IndexEngine};
use super::loaders::BatchStats;
use super::options::{FACET_COUNT_SIZE, SKIP_MAX_TERMS};
use super::text::{empty_text_response, js_max, js_min, json_number, kth, page_offset, page_slice, ProofStats, TextRequest};

#[derive(Clone, Debug)]
pub struct SortPlan {
    pub field: String,
    pub desc: bool,
}

const LINKRANK_OVERFETCH: f64 = 4.0;
const LINKRANK_MAX_POOL: usize = 100;

fn cmp_f64(a: f64, b: f64) -> std::cmp::Ordering {
    a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
}

impl IndexEngine {
    /// `makeSortPlan`.
    pub fn make_sort_plan(&self, sort: &Value) -> Option<SortPlan> {
        let (field, order) = match sort {
            Value::String(text) if !text.is_empty() => {
                let field = text.strip_prefix('-').unwrap_or(text).to_string();
                (field, if text.starts_with('-') { "desc".to_string() } else { "asc".to_string() })
            }
            Value::Object(map) => {
                let field = map.get("field").and_then(Value::as_str).unwrap_or("").to_string();
                let order = map
                    .get("order")
                    .or_else(|| map.get("direction"))
                    .filter(|v| truthy(v))
                    .map(crate::util::json::string_or_empty)
                    .unwrap_or_else(|| "asc".into())
                    .to_lowercase();
                (field, order)
            }
            _ => return None,
        };
        if field.is_empty() {
            return None;
        }
        if !self.info.number_fields.contains_key(&field) && !self.info.boolean_fields.contains(&field) {
            return None;
        }
        Some(SortPlan { field, desc: order == "desc" })
    }

    /// The sort value `compareRankedRows` compares (`Number(value)`):
    /// booleans order as 0/1, a missing value is `None`.
    fn sort_key(&self, store: Option<&ValueStore>, field: &str, doc: i32) -> Option<f64> {
        match self.value_for_doc(store, field, doc) {
            DocValue::Bool(flag) => Some(if flag { 1.0 } else { 0.0 }),
            other => other.as_number(),
        }
    }

    /// `compareRankedRows`: missing values last, then score desc, doc asc.
    fn compare_rows(&self, a: &Row, b: &Row, store: Option<&ValueStore>, sort: &SortPlan) -> std::cmp::Ordering {
        let left = self.sort_key(store, &sort.field, a.0);
        let right = self.sort_key(store, &sort.field, b.0);
        match (left, right) {
            (None, None) => cmp_f64(b.1, a.1).then(a.0.cmp(&b.0)),
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(_), None) => std::cmp::Ordering::Less,
            (Some(l), Some(r)) => {
                if l != r {
                    if sort.desc {
                        cmp_f64(r, l)
                    } else {
                        cmp_f64(l, r)
                    }
                } else {
                    cmp_f64(b.1, a.1).then(a.0.cmp(&b.0))
                }
            }
        }
    }

    pub fn sort_ranked(&self, ranked: &mut [Row], store: Option<&ValueStore>, sort: &SortPlan) {
        ranked.sort_by(|a, b| self.compare_rows(a, b, store, sort));
    }

    /// `selectSortedTopK`.
    fn select_sorted_top_k(
        &self,
        candidates: &[Row],
        store: Option<&ValueStore>,
        sort: &SortPlan,
        k: f64,
        plan: Option<&DocFilterPlan>,
    ) -> (usize, Vec<Row>) {
        let mut top: Vec<Row> = Vec::new();
        let mut total = 0;
        for row in candidates {
            if let Some(plan) = plan {
                if !self.passes_filter_plan(row.0, store, plan, None) {
                    continue;
                }
            }
            total += 1;
            if (top.len() as f64) < k {
                top.push(*row);
                if top.len() as f64 == k {
                    top.sort_by(|a, b| self.compare_rows(a, b, store, sort));
                }
            } else if self.compare_rows(row, top.last().unwrap(), store, sort).is_lt() {
                *top.last_mut().unwrap() = *row;
                top.sort_by(|a, b| self.compare_rows(a, b, store, sort));
            }
        }
        if (top.len() as f64) < k {
            top.sort_by(|a, b| self.compare_rows(a, b, store, sort));
        }
        (total, top)
    }

    fn sorted_directory_pages(
        &self,
        directory: &crate::codec::sort_tree::DocValueSortDirectory,
        desc: bool,
        plan: Option<&DocFilterPlan>,
    ) -> Vec<usize> {
        let field = directory.field_name();
        let mut pages: Vec<usize> = (0..directory.pages.len()).filter(|i| page_may_pass(&directory.pages[*i], plan, field)).collect();
        pages.sort_by(|a, b| {
            let (pa, pb) = (&directory.pages[*a], &directory.pages[*b]);
            if desc {
                cmp_f64(pb.max, pa.max).then(pa.rank_start.cmp(&pb.rank_start))
            } else {
                cmp_f64(pa.min, pb.min).then(pa.rank_start.cmp(&pb.rank_start))
            }
        });
        pages
    }

    /// `docValueSortField(field)`: trees from builders that wrote missing
    /// values as 0 (and double-coded booleans) are ignored, so the exact
    /// lanes answer those indexes.
    fn sorted_tree_field(&self, field: &str) -> bool {
        let omitted = self.doc_value_sorted_meta().map(|meta| meta.get("missing_values").and_then(Value::as_str) == Some("omitted")).unwrap_or(false);
        omitted && self.doc_value_sort_field(field).is_some()
    }

    /// `sortLaneOmitsMissingDocs`: docs without a sort value are not in the
    /// tree and rank after every valued doc, so a lane that exhausted the
    /// tree may still owe them, unless a filter on the sort field rejects
    /// missing values.
    fn sort_lane_omits_missing_docs(&self, field_total: f64, field: &str, plan: Option<&DocFilterPlan>) -> bool {
        if field_total.is_finite() && field_total >= self.total() as f64 {
            return false;
        }
        if let Some(plan) = plan {
            if plan.numbers.iter().any(|(name, _)| name == field) || plan.booleans.iter().any(|(name, _)| name == field) {
                return false;
            }
        }
        true
    }

    // -------------------------------------------------------- browse lanes

    /// `runDocValueBrowse`.
    async fn run_doc_value_browse(&self, page: f64, size: usize, filters: &Filters, sort: &SortPlan, has_filters: bool) -> Result<Option<Value>> {
        let plan = if has_filters { Some(self.make_doc_filter_plan(filters)?) } else { None };
        if self.doc_value_sorted_meta().is_none() {
            self.ensure_doc_value_sorted_manifest().await?;
        }
        if !self.sorted_tree_field(&sort.field) {
            return Ok(None);
        }
        let Some(directory) = self.load_doc_value_sort_directory(&sort.field).await? else { return Ok(None) };
        if directory.pages.is_empty() {
            return Ok(None);
        }
        let offset = page_offset(page, size);
        let k = offset + size as f64;
        let pages = self.sorted_directory_pages(&directory, sort.desc, plan.as_ref());
        let filter_fields: Vec<String> = Self::filter_plan_fields(plan.as_ref()).into_iter().filter(|f| *f != sort.field).collect();
        let mut collected: Vec<Row> = Vec::new();
        let mut sort_values: HashMap<i32, f64> = HashMap::new();
        let (mut visited, mut scanned, mut accepted, mut definite_pages) = (0usize, 0usize, 0usize, 0usize);
        let mut stopped = false;
        let mut fetch = BatchStats::default();
        let batch_size = self.options.doc_value_sort_page_batch_size;
        let mut index = 0;
        while index < pages.len() && !stopped {
            let batch: Vec<usize> = pages[index..(index + batch_size).min(pages.len())].to_vec();
            let loaded = self.load_doc_value_sort_pages(&sort.field, &directory, &batch, &mut fetch).await?;
            for (slot, page_index) in batch.iter().enumerate() {
                let position = index + slot;
                visited += 1;
                let rows = loaded[slot].sorted_rows(sort.desc);
                scanned += rows.len();
                let meta = &directory.pages[*page_index];
                let definite = page_definitely_passes(meta, plan.as_ref(), &sort.field);
                if definite {
                    definite_pages += 1;
                }
                let store = if definite || filter_fields.is_empty() {
                    None
                } else {
                    let docs: Vec<i32> = rows.iter().map(|r| r.doc).collect();
                    self.value_store_for_filter_plan(plan.as_ref().unwrap(), &docs, &[&sort.field]).await?
                };
                for row in &rows {
                    if !definite {
                        if let Some(plan) = &plan {
                            if !self.passes_filter_plan(row.doc, store.as_ref(), plan, Some((&sort.field, &row.value))) {
                                continue;
                            }
                        }
                    }
                    sort_values.insert(row.doc, row.sort_value);
                    collected.push((row.doc, 0.0));
                    accepted += 1;
                }
                // Equal values can straddle pages, so stop only once the next
                // page cannot reach the k-th row.
                if (collected.len() as f64) >= k {
                    collected.sort_by(|a, b| compare_known_sort_rows(a, b, sort.desc, &sort_values));
                    let boundary = kth(&collected, k).and_then(|row| sort_values.get(&row.0).copied());
                    let next = pages.get(position + 1).map(|i| &directory.pages[*i]);
                    if !next_sort_page_can_tie(next, boundary, sort.desc) {
                        stopped = true;
                        break;
                    }
                }
            }
            index += batch.len();
        }
        if !stopped && self.sort_lane_omits_missing_docs(directory.total as f64, &sort.field, plan.as_ref()) {
            return Ok(None);
        }
        collected.sort_by(|a, b| compare_known_sort_rows(a, b, sort.desc, &sort_values));
        let mut context = ResultContext::new(false, PreferDocPages::Yes);
        let rows: Vec<Row> = page_slice(&collected, offset, size);
        let results = self.rows_to_results(&rows, &mut context).await?;
        let exact = !stopped;
        let mut stats = Map::new();
        for (key, value) in [
            ("exact", json!(exact)),
            ("docValuePruning", json!(true)),
            ("docValuePruneField", json!(sort.field)),
            ("docValueSortDirection", json!(if sort.desc { "desc" } else { "asc" })),
            ("docValueDirectoryPages", json!(directory.pages.len())),
            ("docValueCandidatePages", json!(pages.len())),
            ("docValuePagesPruned", json!(directory.pages.len() - pages.len())),
            ("docValuePagesVisited", json!(visited)),
            ("docValueSortPageBatchSize", json!(batch_size)),
            ("docValueSortPagesPrefetched", json!(fetch.wanted)),
            ("docValueSortPagesFetched", json!(fetch.fetched)),
            ("docValueSortPageFetchGroups", json!(fetch.groups)),
            ("docValueSortPageOverfetch", json!(fetch.wanted.saturating_sub(visited))),
            ("docValueRowsScanned", json!(scanned)),
            ("docValueRowsAccepted", json!(accepted)),
            ("docValueDefinitePages", json!(definite_pages)),
        ] {
            stats.insert(key.into(), value);
        }
        context.stats(&mut stats, false);
        let total = if !has_filters {
            json!(self.total())
        } else if exact {
            json!(collected.len())
        } else {
            json_number(js_max(collected.len() as f64, k))
        };
        Ok(Some(json!({
            "total": total,
            "results": results,
            "page": json_number(page),
            "size": size,
            // JS: `!exactTotal && hasFilters`, where hasFilters is a key count.
            "approximate": if exact { json!(false) } else { json!(filters.has_filters_js()) },
            "stats": Value::Object(stats)
        })))
    }

    /// `runDocValueChunkBrowse`.
    async fn run_doc_value_chunk_browse(&self, page: f64, size: usize, filters: &Filters, has_filters: bool) -> Result<Option<Value>> {
        let Some(meta) = self.doc_values_meta() else { return Ok(None) };
        if !has_filters {
            return Ok(None);
        }
        let plan = self.make_doc_filter_plan(filters)?;
        if !plan.active {
            return Ok(None);
        }
        let fields = Self::filter_plan_fields(Some(&plan));
        if fields.is_empty() {
            return Ok(None);
        }
        let offset = page_offset(page, size);
        let k = offset + size as f64;
        let chunks = self.candidate_doc_value_chunks(Some(&plan));
        let chunk_size = meta.chunk_size_or(self.total()).max(1);
        let mut collected: Vec<Row> = Vec::new();
        let (mut visited, mut scanned, mut accepted) = (0usize, 0usize, 0usize);
        let mut stopped = false;
        for chunk in &chunks {
            let loaded = self.ensure_doc_value_chunk_indexes(&fields, &[*chunk]).await?;
            let store = if loaded { Some(ValueStore { doc_values: true, ..Default::default() }) } else { None };
            visited += 1;
            let start = chunk * chunk_size;
            let end = self.total().min(start + chunk_size);
            for doc in start..end {
                scanned += 1;
                if !self.passes_filter_plan(doc as i32, store.as_ref(), &plan, None) {
                    continue;
                }
                collected.push((doc as i32, 0.0));
                accepted += 1;
                if collected.len() as f64 >= k {
                    stopped = true;
                    break;
                }
            }
            if stopped {
                break;
            }
        }
        let mut context = ResultContext::new(false, PreferDocPages::Yes);
        let rows: Vec<Row> = page_slice(&collected, offset, size);
        let results = self.rows_to_results(&rows, &mut context).await?;
        let exact = !stopped;
        let total_chunks = self.total().div_ceil(chunk_size);
        let mut stats = Map::new();
        for (key, value) in [
            ("exact", json!(exact)),
            ("docValueChunkPruning", json!(true)),
            ("docValueChunksTotal", json!(total_chunks)),
            ("docValueCandidateChunks", json!(chunks.len())),
            ("docValueChunksPruned", json!(total_chunks - chunks.len())),
            ("docValueChunksVisited", json!(visited)),
            ("docValueRowsScanned", json!(scanned)),
            ("docValueRowsAccepted", json!(accepted)),
        ] {
            stats.insert(key.into(), value);
        }
        context.stats(&mut stats, false);
        Ok(Some(json!({
            "total": if exact { json!(collected.len()) } else { json_number(js_max(collected.len() as f64, k)) },
            "results": results,
            "page": json_number(page),
            "size": size,
            "approximate": !exact,
            "stats": Value::Object(stats)
        })))
    }

    /// The `!q` branch of `executeSearch`.
    async fn browse(&self, page: f64, size: usize, filters: &Filters, user_filters: &Filters, geo: Option<&Rc<GeoPlan>>, sort: Option<&SortPlan>) -> Result<Value> {
        let has_filters = filters.has_filters();
        let offset = page_offset(page, size);
        if sort.is_some() {
            self.ensure_doc_value_sorted_manifest().await?;
        }
        self.ensure_facet_dictionaries(&filters.facet_fields()).await?;
        if let (Some(geo), None) = (geo, sort) {
            if let Some(response) = self.run_geo_browse(page, size, user_filters, geo, user_filters.has_filters(), None).await? {
                return Ok(response);
            }
        }
        // Sorting reads the sort field's doc values (the sorted lane defers
        // here when its tree cannot answer), so the lazy manifest loads too.
        if has_filters || sort.is_some() {
            self.ensure_doc_values_manifest().await?;
        }
        if sort.is_none() && !has_filters && geo.is_none() {
            let initial = self.manifest_at(&["initial_results"]);
            let docs: Vec<Value> = initial.as_array().map(|items| page_slice(items, offset, size)).unwrap_or_default();
            return Ok(json!({ "total": self.total(), "results": docs, "page": json_number(page), "size": size }));
        }
        let plan = if has_filters { Some(self.make_doc_filter_plan(filters)?) } else { None };
        if sort.is_none() && has_filters && self.doc_values_meta().is_some() {
            if let Some(response) = self.run_doc_value_chunk_browse(page, size, filters, has_filters).await? {
                return Ok(response);
            }
        }
        if let Some(sort) = sort {
            if self.doc_value_sorted_meta().is_some() {
                if let Some(response) = self.run_doc_value_browse(page, size, filters, sort, has_filters).await? {
                    return Ok(response);
                }
            }
        }
        let (store, candidates): (Option<ValueStore>, Vec<Row>) = match self.doc_values_meta() {
            Some(meta) => {
                let chunks = self.candidate_doc_value_chunks(plan.as_ref());
                let fields = Self::plan_fields(plan.as_ref(), sort.map(|s| s.field.as_str()));
                let loaded = self.ensure_doc_value_chunk_indexes(&fields, &chunks).await?;
                let chunk_size = meta.chunk_size_or(self.total()).max(1);
                let mut candidates = Vec::new();
                for chunk in chunks {
                    let start = chunk * chunk_size;
                    for doc in start..self.total().min(start + chunk_size) {
                        candidates.push((doc as i32, 0.0));
                    }
                }
                (if loaded { Some(ValueStore { doc_values: true, ..Default::default() }) } else { None }, candidates)
            }
            None => {
                let codes = self.load_codes().await?;
                (Some(ValueStore { codes: Some(codes), ..Default::default() }), (0..self.total()).map(|doc| (doc as i32, 0.0)).collect())
            }
        };
        let (total, ranked) = match sort {
            Some(sort) => self.select_sorted_top_k(&candidates, store.as_ref(), sort, offset + size as f64, if has_filters { plan.as_ref() } else { None }),
            None => {
                let ranked: Vec<Row> = candidates
                    .into_iter()
                    .filter(|row| !has_filters || self.passes_filter_plan(row.0, store.as_ref(), plan.as_ref().unwrap(), None))
                    .collect();
                (ranked.len(), ranked)
            }
        };
        let rows: Vec<Row> = page_slice(&ranked, offset, size);
        let mut context = ResultContext::new(false, PreferDocPages::Yes);
        let results = self.rows_to_results(&rows, &mut context).await?;
        let mut stats = Map::new();
        context.stats(&mut stats, false);
        Ok(json!({ "total": total, "results": results, "page": json_number(page), "size": size, "stats": Value::Object(stats) }))
    }

    // -------------------------------------------------------- sorted text

    /// `runSortedTextSearch`.
    async fn run_sorted_text_search(&self, request: &TextRequest, sort: &SortPlan) -> Result<Option<Value>> {
        if let Some(response) = self.run_sort_replica_text_search(request, sort).await? {
            return Ok(Some(response));
        }
        if self.doc_value_sorted_meta().is_none() {
            self.ensure_doc_value_sorted_manifest().await?;
        }
        if !self.sorted_tree_field(&sort.field) || request.base_terms.is_empty() || request.terms.len() > SKIP_MAX_TERMS {
            return Ok(None);
        }
        if request.rerank && !self.dependency_terms(&request.base_terms).is_empty() {
            return Ok(None);
        }
        let filters = &request.filters;
        let has_filters = filters.has_filters();
        self.ensure_facet_dictionaries(&filters.facet_fields()).await?;
        let Some(directory) = self.load_doc_value_sort_directory(&sort.field).await? else { return Ok(None) };
        if directory.pages.is_empty() {
            return Ok(None);
        }
        let entries = self.term_entries(&request.terms).await?;
        let base_set: HashSet<&str> = request.base_terms.iter().map(String::as_str).collect();
        let min_should_match = min_should_match_for(request.base_terms.len());
        let present: HashSet<&str> = entries.iter().filter(|h| base_set.contains(h.term.as_str())).map(|h| h.term.as_str()).collect();
        if present.len() < min_should_match.max(1) {
            return Ok(Some(empty_text_response(request.page, request.size, request.terms.len(), &entries, request.base_terms.len() - present.len())));
        }
        let offset = request.offset();
        let k = request.k();
        let plan = if has_filters { Some(self.make_doc_filter_plan(filters)?) } else { None };
        if let Some(plan) = &plan {
            if self.doc_filter_plan_needs_doc_values(plan).await? {
                self.ensure_doc_values_manifest().await?;
            }
        }
        let filter_fields: Vec<String> = Self::filter_plan_fields(plan.as_ref()).into_iter().filter(|f| *f != sort.field).collect();
        let pages = self.sorted_directory_pages(&directory, sort.desc, plan.as_ref());
        let mut collected: Vec<Row> = Vec::new();
        let mut sort_values: HashMap<i32, f64> = HashMap::new();
        let mut decoded_blocks: HashSet<(usize, usize)> = HashSet::new();
        let mut c = SortedCounters::default();
        let mut stopped = false;
        let mut fetch = BatchStats::default();
        let batch_size = self.options.doc_value_sort_page_batch_size;
        let compare = |a: &Row, b: &Row, values: &HashMap<i32, f64>| {
            match (values.get(&a.0), values.get(&b.0)) {
                (None, None) => cmp_f64(b.1, a.1).then(a.0.cmp(&b.0)),
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(_), None) => std::cmp::Ordering::Less,
                (Some(l), Some(r)) if l != r => {
                    if sort.desc {
                        cmp_f64(*r, *l)
                    } else {
                        cmp_f64(*l, *r)
                    }
                }
                _ => cmp_f64(b.1, a.1).then(a.0.cmp(&b.0)),
            }
        };
        let mut index = 0;
        while index < pages.len() && !stopped {
            let batch: Vec<usize> = pages[index..(index + batch_size).min(pages.len())].to_vec();
            let loaded = self.load_doc_value_sort_pages(&sort.field, &directory, &batch, &mut fetch).await?;
            for (slot, page_index) in batch.iter().enumerate() {
                let position = index + slot;
                c.pages_visited += 1;
                let rows = loaded[slot].sorted_rows(sort.desc);
                c.rows_scanned += rows.len();
                let meta = &directory.pages[*page_index];
                let definite = page_definitely_passes(meta, plan.as_ref(), &sort.field);
                if definite {
                    c.definite_pages += 1;
                }
                let store = if definite || filter_fields.is_empty() {
                    None
                } else {
                    let docs: Vec<i32> = rows.iter().map(|r| r.doc).collect();
                    self.value_store_for_filter_plan(plan.as_ref().unwrap(), &docs, &[&sort.field]).await?
                };
                let mut candidates: HashSet<i32> = HashSet::new();
                for row in &rows {
                    if !definite {
                        if let Some(plan) = &plan {
                            if !self.passes_filter_plan(row.doc, store.as_ref(), plan, Some((&sort.field, &row.value))) {
                                continue;
                            }
                        }
                    }
                    candidates.insert(row.doc);
                }
                let mut page_scores: HashMap<i32, i64> = HashMap::new();
                let mut page_hits: HashMap<i32, usize> = HashMap::new();
                if !candidates.is_empty() {
                    let page_filters = merge_sort_page_filter(self, filters, &sort.field, meta.min, meta.max);
                    let block_plan = self.make_block_filter_plan(&page_filters)?;
                    for (term_index, hit) in entries.iter().enumerate() {
                        let is_base = base_set.contains(hit.term.as_str());
                        let found = sorted_text_candidate_blocks(&hit.entry, &block_plan, &candidates);
                        c.candidate_blocks += found.indexes.len();
                        c.considered_blocks += found.considered_blocks;
                        c.skipped_blocks += found.skipped_blocks;
                        c.considered_superblocks += found.considered_superblocks;
                        c.skipped_superblocks += found.skipped_superblocks;
                        for (block_index, postings, scanned) in self.lookup_entry_blocks(hit, &found.indexes, &candidates).await? {
                            if decoded_blocks.insert((term_index, block_index)) {
                                c.blocks_decoded += 1;
                            }
                            c.rows_lookup_scanned += scanned;
                            c.lookup_hits += postings.len() / 2;
                            for pair in postings.chunks_exact(2) {
                                if !candidates.contains(&pair[0]) {
                                    continue;
                                }
                                *page_scores.entry(pair[0]).or_insert(0) += pair[1] as i64;
                                if is_base {
                                    *page_hits.entry(pair[0]).or_insert(0) += 1;
                                }
                            }
                        }
                    }
                }
                for row in &rows {
                    let Some(score) = page_scores.get(&row.doc) else { continue };
                    if page_hits.get(&row.doc).copied().unwrap_or(0) < min_should_match.max(1) {
                        continue;
                    }
                    sort_values.insert(row.doc, row.sort_value);
                    collected.push((row.doc, *score as f64));
                    c.rows_accepted += 1;
                }
                collected.sort_by(|a, b| compare(a, b, &sort_values));
                if collected.len() as f64 >= k {
                    let boundary = kth(&collected, k).and_then(|row| sort_values.get(&row.0).copied());
                    let next = pages.get(position + 1).map(|i| &directory.pages[*i]);
                    if !next_sort_page_can_tie(next, boundary, sort.desc) {
                        stopped = true;
                        break;
                    }
                }
            }
            index += batch.len();
        }
        if !stopped && self.sort_lane_omits_missing_docs(directory.total as f64, &sort.field, plan.as_ref()) {
            return Ok(None);
        }
        collected.sort_by(|a, b| compare(a, b, &sort_values));
        let rows: Vec<Row> = page_slice(&collected, offset, request.size);
        let mut context = ResultContext::new(true, PreferDocPages::Yes);
        let results = self.rows_to_results(&rows, &mut context).await?;
        let total_exact = !stopped;
        let mut stats = Map::new();
        for (key, value) in [
            ("exact", json!(true)),
            ("plannerLane", json!("sortPageText")),
            ("topKProven", json!(true)),
            ("totalExact", json!(total_exact)),
            ("tailExhausted", json!(total_exact)),
            ("terms", json!(request.terms.len())),
            ("shards", json!(entries.iter().map(|h| &*h.shard_name).collect::<HashSet<&str>>().len())),
            ("blocksDecoded", json!(c.blocks_decoded)),
            ("postingsDecoded", json!(c.lookup_hits)),
            ("postingsAccepted", json!(c.rows_accepted)),
            ("skippedBlocks", json!(c.skipped_blocks)),
            ("sortedTextBlockScheduler", json!(true)),
            ("sortedTextCandidateLookup", json!(true)),
            ("sortPagePostingBlocksConsidered", json!(c.considered_blocks)),
            ("sortPagePostingBlocksCandidate", json!(c.candidate_blocks)),
            ("sortPagePostingBlocksSkipped", json!(c.skipped_blocks)),
            ("sortPagePostingSuperblocksConsidered", json!(c.considered_superblocks)),
            ("sortPagePostingSuperblocksSkipped", json!(c.skipped_superblocks)),
            ("sortPagePostingRowsScanned", json!(c.rows_lookup_scanned)),
            ("sortPagePostingLookupHits", json!(c.lookup_hits)),
            ("docValueSortText", json!(true)),
            ("docValuePruning", json!(true)),
            ("docValuePruneField", json!(sort.field)),
            ("docValueSortDirection", json!(if sort.desc { "desc" } else { "asc" })),
            ("docValueDirectoryPages", json!(directory.pages.len())),
            ("docValueCandidatePages", json!(pages.len())),
            ("docValuePagesPruned", json!(directory.pages.len() - pages.len())),
            ("docValuePagesVisited", json!(c.pages_visited)),
            ("docValueSortPageBatchSize", json!(batch_size)),
            ("docValueSortPagesPrefetched", json!(fetch.wanted)),
            ("docValueSortPagesFetched", json!(fetch.fetched)),
            ("docValueSortPageFetchGroups", json!(fetch.groups)),
            ("docValueSortPageOverfetch", json!(fetch.wanted.saturating_sub(c.pages_visited))),
            ("docValueRowsScanned", json!(c.rows_scanned)),
            ("docValueRowsAccepted", json!(c.rows_accepted)),
            ("docValueDefinitePages", json!(c.definite_pages)),
            ("sortSummaryStopReason", json!(if stopped { "sort_bound" } else { "exhausted" })),
            ("plannerFallbackReason", json!("")),
        ] {
            stats.insert(key.into(), value);
        }
        ProofStats::write(Some(&ProofStats::new(has_filters, true, None)), "", &mut stats);
        context.stats(&mut stats, true);
        super::text::empty_rerank_stats(&mut stats);
        Ok(Some(json!({
            "total": if total_exact { json!(collected.len()) } else { json_number(js_max(collected.len() as f64, k)) },
            "page": json_number(request.page),
            "size": request.size,
            "results": results,
            "approximate": !total_exact,
            "stats": Value::Object(stats)
        })))
    }

    // -------------------------------------------------------- executeSearch

    pub(crate) async fn execute_search(&self, params: &Value) -> Result<Value> {
        if truthy(params.get("vector").unwrap_or(&NULL)) {
            return self.execute_hybrid_search(params).await;
        }
        let q = crate::util::json::string_or_empty(params.get("q").unwrap_or(&NULL)).trim().to_string();
        let page = request_page(params.get("page"));
        let size = request_size(params.get("size"), self.options.max_page_size);
        let user_filters = Filters::from_json(params.get("filters").unwrap_or(&NULL));
        let geo = match params.get("geo").filter(|g| truthy(g)) {
            Some(geo) => Some(Rc::new(self.make_geo_plan(geo)?)),
            None => None,
        };
        let filters = match &geo {
            Some(geo) => geo.with_geo_filters(&user_filters),
            None => user_filters.clone(),
        };
        let sort_value = params.get("sort").cloned().unwrap_or(Value::Null);
        let sort = self.make_sort_plan(&sort_value);
        if geo.as_ref().map(|g| g.sort || g.route_sort).unwrap_or(false) && sort.is_some() {
            return Err(Error::new("Rangefind: use either sort or geo.sort, not both."));
        }
        let include_results = params.get("includeResults") != Some(&Value::Bool(false));
        if q.is_empty() {
            return self.browse(page, size, &filters, &user_filters, geo.as_ref(), sort.as_ref()).await;
        }
        let has_filters = filters.has_filters();
        if let Some(mut response) = self.try_address_authority_fast_path(&q, page, size, has_filters, sort.as_ref(), geo.as_ref(), include_results, params.get("authority")).await? {
            if truthy(params.get("highlight").unwrap_or(&NULL)) {
                self.highlight_response(&mut response, &q, "", params.get("highlight").unwrap());
            }
            return Ok(response);
        }
        let plan = self.resolve_query_plan(&q).await?;
        let base_terms = plan.base_terms.clone();
        if let Some(geo) = geo.as_ref().filter(|g| g.sort || g.route_sort) {
            return self.run_geo_text_sorted(page, size, &user_filters, geo, &base_terms).await;
        }
        if let Some(geo) = geo.as_ref().filter(|g| g.filtered) {
            let estimate = if !base_terms.is_empty() {
                Some(self.term_entries(&base_terms).await?.iter().map(|h| h.entry.df as usize).sum::<usize>())
            } else {
                None
            };
            self.build_geo_doc_set_if_cheap(geo, estimate, &user_filters).await?;
        }
        let exact = truthy(params.get("exact").unwrap_or(&NULL));
        if exact {
            self.ensure_full_manifest().await?;
        } else if sort.is_some() {
            self.ensure_doc_value_sorted_manifest().await?;
        }
        let request = TextRequest {
            q: q.clone(),
            page,
            size,
            filters: filters.clone(),
            sort: sort_value,
            base_terms: base_terms.clone(),
            terms: plan.terms.clone(),
            rerank: params.get("rerank") != Some(&Value::Bool(false)),
            include_results,
        };
        let response = if exact {
            self.run_full_search(&request, "exact_requested").await?
        } else {
            self.run_skipped_search(&request).await?
        };
        let response = self.maybe_typo_fallback(&request, params, response, &plan).await?;
        let mut response = self.maybe_authority_rerank(&request, params, response).await?;
        if truthy(params.get("highlight").unwrap_or(&NULL)) && include_results && response["results"].as_array().map(|r| !r.is_empty()).unwrap_or(false) {
            let corrected = response.get("correctedQuery").and_then(Value::as_str).unwrap_or("").to_string();
            self.highlight_response(&mut response, &q, &corrected, params.get("highlight").unwrap());
        }
        if let Some(geo) = &geo {
            if let Some(doc_set_stats) = geo.doc_set_stats() {
                let stats = response["stats"].as_object_mut();
                if let Some(stats) = stats {
                    stats.insert("geoLane".into(), json!("textDocSet"));
                    stats.extend(doc_set_stats);
                }
            } else if geo.filtered {
                if let Some(stats) = response["stats"].as_object_mut() {
                    stats.insert("geoLane".into(), json!("textDocValues"));
                }
            }
        }
        self.attach_geo_distances(response, geo.as_ref()).await
    }

    /// `runSkippedSearch`: route sorted requests, then the unsorted lanes.
    async fn run_skipped_search(&self, request: &TextRequest) -> Result<Value> {
        if let Some(sort) = self.make_sort_plan(&request.sort) {
            if let Some(response) = self.run_sorted_text_search(request, &sort).await? {
                return Ok(response);
            }
            return self.run_full_search(request, "sort_requested").await;
        }
        self.run_skipped_text_search(request).await
    }

    fn highlight_response(&self, response: &mut Value, q: &str, corrected: &str, option: &Value) {
        let analyzer = &*self.analyzer;
        let term_set = highlight_term_set(q, corrected, Some(analyzer));
        let fields = option.get("fields").and_then(Value::as_array).map(|items| items.iter().filter_map(Value::as_str).map(str::to_string).collect());
        let max_chars = option.get("maxChars").filter(|v| !v.is_null()).map(to_number);
        let options = HighlightOptions { fields, max_chars, analyzer: Some(analyzer) };
        if let Some(results) = response.get_mut("results").and_then(Value::as_array_mut) {
            apply_highlights(results, &term_set, &options);
        }
    }

    /// `resolveQueryPlan` with its two-hit admission cache.
    pub async fn resolve_query_plan(&self, q: &str) -> Result<Rc<crate::analysis::QueryPlan>> {
        let cache_size = self.options.query_plan_cache_size;
        if cache_size == 0 || crate::util::jsstr::len16(q) > 64 {
            return self.resolve_query_plan_uncached(q).await;
        }
        let cached = {
            let mut cache = self.query_plan_cache.borrow_mut();
            match cache.iter().position(|(key, _)| key == q) {
                Some(index) => {
                    let item = cache.remove(index).unwrap();
                    let promise = item.1.clone();
                    cache.push_back(item);
                    Some(promise)
                }
                None => None,
            }
        };
        if let Some(promise) = cached {
            return promise.await;
        }
        let seen_before = {
            let mut seen = self.seen_query_plans.borrow_mut();
            match seen.iter().position(|key| key == q) {
                Some(index) => {
                    seen.remove(index);
                    true
                }
                None => {
                    seen.push_back(q.to_string());
                    if seen.len() > cache_size {
                        seen.pop_front();
                    }
                    false
                }
            }
        };
        if !seen_before {
            return self.resolve_query_plan_uncached(q).await;
        }
        let promise = crate::rt::Promise::pending();
        {
            let mut cache = self.query_plan_cache.borrow_mut();
            cache.push_back((q.to_string(), promise.clone()));
            if cache.len() > cache_size {
                cache.pop_front();
            }
        }
        let result = self.resolve_query_plan_uncached(q).await;
        if result.is_err() {
            self.query_plan_cache.borrow_mut().retain(|(key, p)| !(key == q && p.same(&promise)));
        }
        promise.settle(result.clone());
        result
    }

    async fn resolve_query_plan_uncached(&self, q: &str) -> Result<Rc<crate::analysis::QueryPlan>> {
        let plan = self.analyzer.query_plan(q);
        if plan.alt_plans.is_empty() || plan.base_terms.is_empty() {
            return Ok(Rc::new(plan));
        }
        let present = |found: &[super::index::TermHit], base: &[String]| {
            let set: HashSet<&str> = found.iter().map(|h| h.term.as_str()).collect();
            base.iter().filter(|t| set.contains(t.as_str())).count()
        };
        let primary = present(&self.term_entries(&plan.base_terms).await?, &plan.base_terms);
        if primary >= min_should_match_for(plan.base_terms.len()).max(1) {
            return Ok(Rc::new(plan));
        }
        let mut best: Option<usize> = None;
        let mut best_present = primary;
        for (index, alt) in plan.alt_plans.iter().enumerate() {
            let count = present(&self.term_entries(&alt.base_terms).await?, &alt.base_terms);
            if count > best_present {
                best = Some(index);
                best_present = count;
            }
        }
        let mut plan = plan;
        if let Some(index) = best {
            let alt = plan.alt_plans[index].clone();
            plan.language = alt.language;
            plan.analyzed_terms = alt.analyzed_terms;
            plan.base_terms = alt.base_terms;
        }
        Ok(Rc::new(plan))
    }

    // -------------------------------------------------------- count & facets

    async fn execute_count(&self, params: &Value) -> Result<Value> {
        let q = crate::util::json::string_or_empty(params.get("q").unwrap_or(&NULL)).trim().to_string();
        let filters = Filters::from_json(params.get("filters").unwrap_or(&NULL));
        if filters.has_filters() || truthy(params.get("sort").unwrap_or(&NULL)) || truthy(params.get("geo").unwrap_or(&NULL)) {
            return Err(Error::new("Rangefind count currently supports text-only queries without filters, sort, or geo."));
        }
        let plan = self.resolve_query_plan(&q).await?;
        self.run_count_search(&plan.base_terms).await
    }

    async fn facet_top_values(&self, field: &str, counts: &[(u32, usize)], size: usize) -> Result<Vec<Value>> {
        let dictionary = self.load_facet_dictionary(field).await?;
        let mut out: Vec<(String, Value, usize)> = Vec::new();
        for (code, count) in counts {
            let Some(item) = dictionary.get(*code as usize) else { continue };
            let value = item.get("value").unwrap_or(&NULL);
            if !truthy(value) {
                continue;
            }
            let label = item.get("label").filter(|l| truthy(l)).unwrap_or(value).clone();
            out.push((crate::util::json::string_or_empty(value), json!({ "value": value, "label": label, "count": count }), *count));
        }
        out.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| if crate::util::jsstr::cmp16(&a.0, &b.0).is_lt() { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater }));
        Ok(out.into_iter().take(size).map(|item| item.1).collect())
    }

    async fn facet_counts_from_dictionary(&self, fields: &[String], size: usize) -> Result<Map<String, Value>> {
        let mut out = Map::new();
        for field in fields {
            let dictionary = self.load_facet_dictionary(field).await?;
            let mut values: Vec<(String, Value, f64)> = dictionary
                .iter()
                .filter(|item| truthy(item.get("value").unwrap_or(&NULL)))
                .map(|item| {
                    let value = item.get("value").unwrap().clone();
                    let label = item.get("label").filter(|l| truthy(l)).unwrap_or(&value).clone();
                    let count = num_or(item.get("n").unwrap_or(&NULL), 0.0);
                    (crate::util::json::string_or_empty(&value), json!({ "value": value, "label": label, "count": num(count) }), count)
                })
                .collect();
            values.sort_by(|a, b| cmp_f64(b.2, a.2).then_with(|| if crate::util::jsstr::cmp16(&a.0, &b.0).is_lt() { std::cmp::Ordering::Less } else { std::cmp::Ordering::Greater }));
            let values: Vec<Value> = values.into_iter().take(size).map(|v| v.1).collect();
            out.insert(field.clone(), json!({ "values": values, "exact": true, "source": "dictionary" }));
        }
        Ok(out)
    }

    async fn facet_counts_over_chunks(
        &self,
        fields: &[String],
        size: usize,
        docs_by_chunk: &[(usize, Vec<i32>)],
        verify: Option<&DocFilterPlan>,
    ) -> Result<Map<String, Value>> {
        let mut chunk_indexes: Vec<usize> = docs_by_chunk.iter().map(|(index, _)| *index).collect();
        chunk_indexes.sort_unstable();
        let max = self.options.facet_count_max_chunks;
        let sampled = chunk_indexes.len() > max;
        let sampled_chunks: Vec<usize> = if sampled {
            let stride = chunk_indexes.len() as f64 / max as f64;
            (0..max).map(|i| chunk_indexes[(i as f64 * stride).floor() as usize]).collect()
        } else {
            chunk_indexes.clone()
        };
        let mut load_fields: Vec<String> = fields.to_vec();
        if let Some(plan) = verify {
            for field in Self::filter_plan_fields(Some(plan)) {
                if !load_fields.contains(&field) {
                    load_fields.push(field);
                }
            }
        }
        let loaded = self.ensure_doc_value_chunk_indexes(&load_fields, &sampled_chunks).await?;
        let store = if loaded { Some(ValueStore { doc_values: true, ..Default::default() }) } else { None };
        let mut tallies: Vec<Vec<(u32, usize)>> = vec![Vec::new(); fields.len()];
        let mut counted = 0usize;
        let mut raw_in_sample = 0usize;
        let by_chunk: HashMap<usize, &Vec<i32>> = docs_by_chunk.iter().map(|(i, docs)| (*i, docs)).collect();
        for chunk in &sampled_chunks {
            for doc in by_chunk[chunk].iter() {
                raw_in_sample += 1;
                if let Some(plan) = verify {
                    if !self.passes_filter_plan(*doc, store.as_ref(), plan, None) {
                        continue;
                    }
                }
                counted += 1;
                for (slot, field) in fields.iter().enumerate() {
                    let DocValue::Facet(value) = self.value_for_doc(store.as_ref(), field, *doc) else { continue };
                    let Some(codes) = value.codes() else { continue };
                    for code in codes {
                        match tallies[slot].iter_mut().find(|(c, _)| *c == code) {
                            Some(item) => item.1 += 1,
                            None => tallies[slot].push((code, 1)),
                        }
                    }
                }
            }
        }
        let total_enumerated: usize = docs_by_chunk.iter().map(|(_, docs)| docs.len()).sum();
        let scale = if sampled && raw_in_sample > 0 { total_enumerated as f64 / raw_in_sample as f64 } else { 1.0 };
        let mut out = Map::new();
        for (slot, field) in fields.iter().enumerate() {
            let scaled: Vec<(u32, usize)> = tallies[slot].iter().map(|(code, count)| (*code, js_round(*count as f64 * scale) as usize)).collect();
            out.insert(
                field.clone(),
                json!({ "values": self.facet_top_values(field, &scaled, size).await?, "exact": !sampled, "sampled_docs": counted }),
            );
        }
        Ok(out)
    }

    /// `computeFacetCounts`.
    pub(crate) async fn compute_facet_counts(&self, params: &Value, response: &mut Value) -> Result<()> {
        let Some((fields, size)) = normalize_facets_param(params.get("facets").unwrap_or(&NULL)) else { return Ok(()) };
        let set_lane = |response: &mut Value, lane: &str| {
            if !response.get("stats").map(Value::is_object).unwrap_or(false) {
                response["stats"] = json!({});
            }
            response["stats"]["facetCountLane"] = json!(lane);
        };
        if truthy(params.get("vector").unwrap_or(&NULL)) || truthy(params.get("geo").unwrap_or(&NULL)) {
            set_lane(response, "unsupported-lane");
            return Ok(());
        }
        let q = crate::util::json::string_or_empty(params.get("q").unwrap_or(&NULL)).trim().to_string();
        let filters = Filters::from_json(params.get("filters").unwrap_or(&NULL));
        let has_filters = filters.has_filters();
        self.ensure_facet_dictionaries(&filters.facet_fields()).await?;
        if q.is_empty() && !has_filters {
            response["facets"] = Value::Object(self.facet_counts_from_dictionary(&fields, size).await?);
            set_lane(response, "dictionary");
            return Ok(());
        }
        self.ensure_doc_values_manifest().await?;
        let plan = if has_filters { Some(self.make_doc_filter_plan(&filters)?) } else { None };
        let chunk_size = self.doc_values_meta().map(|m| m.chunk_size_or(self.total())).unwrap_or(self.total()).max(1);
        if !q.is_empty() {
            let base_terms = self.resolve_query_plan(&q).await?.base_terms.clone();
            match self.collect_text_match_docs(&base_terms).await? {
                None => {
                    let mut facets = self.facet_counts_from_dictionary(&fields, size).await?;
                    for field in &fields {
                        if let Some(item) = facets.get_mut(field) {
                            item["exact"] = json!(false);
                        }
                    }
                    response["facets"] = Value::Object(facets);
                    set_lane(response, "global-fallback");
                }
                Some((docs, _)) => {
                    let mut grouped: Vec<(usize, Vec<i32>)> = Vec::new();
                    for doc in docs {
                        let chunk = doc as usize / chunk_size;
                        match grouped.last_mut() {
                            Some((index, list)) if *index == chunk => list.push(doc),
                            _ => grouped.push((chunk, vec![doc])),
                        }
                    }
                    response["facets"] = Value::Object(self.facet_counts_over_chunks(&fields, size, &grouped, plan.as_ref()).await?);
                    set_lane(response, "text-match-set");
                }
            }
            return Ok(());
        }
        let mut grouped = Vec::new();
        for chunk in self.candidate_doc_value_chunks(plan.as_ref()) {
            let start = chunk * chunk_size;
            let end = self.total().min(start + chunk_size);
            grouped.push((chunk, (start as i32..end as i32).collect()));
        }
        response["facets"] = Value::Object(self.facet_counts_over_chunks(&fields, size, &grouped, plan.as_ref()).await?);
        set_lane(response, "chunk-scan");
        Ok(())
    }

    // -------------------------------------------------------- rank prior

    fn active_rank_prior(&self) -> Option<(Value, bool)> {
        let manifest = self.manifest.borrow();
        if let Some(prior) = manifest.get("rankPrior").filter(|v| truthy(v)) {
            return Some((prior.clone(), true));
        }
        manifest.get("linkGraph").filter(|v| truthy(v)).map(|v| (v.clone(), false))
    }

    fn rank_prior_boost_weight(&self, params: &Value) -> f64 {
        let Some((cfg, is_rank_prior)) = self.active_rank_prior() else { return 0.0 };
        if !truthy(cfg.get("field").unwrap_or(&NULL)) {
            return 0.0;
        }
        let get = |key: &str| params.get(key).unwrap_or(&NULL);
        if !truthy(get("q")) || truthy(get("sort")) || truthy(params.get("geo").and_then(|g| g.get("sort")).unwrap_or(&NULL)) {
            return 0.0;
        }
        if get("rankPrior") == &Value::Bool(false) || get("rankPriorBoost").as_f64() == Some(0.0) {
            return 0.0;
        }
        if !is_rank_prior && (get("linkRank") == &Value::Bool(false) || get("linkRankBoost").as_f64() == Some(0.0)) {
            return 0.0;
        }
        let pick = if !get("rankPriorBoost").is_null() {
            get("rankPriorBoost").clone()
        } else if !is_rank_prior && !get("linkRankBoost").is_null() {
            get("linkRankBoost").clone()
        } else {
            cfg.get("boost").cloned().unwrap_or(json!(0))
        };
        let weight = to_number(&pick);
        if weight > 0.0 {
            weight
        } else {
            0.0
        }
    }

    async fn apply_rank_prior(&self, results: &mut [Value], weight: f64) -> Result<bool> {
        let Some((cfg, _)) = self.active_rank_prior() else { return Ok(false) };
        let field = cfg.get("field").and_then(Value::as_str).unwrap_or("").to_string();
        if field.is_empty() || results.is_empty() {
            return Ok(false);
        }
        let payload_carries = self
            .manifest_at(&["docs", "pages", "fields"])
            .as_array()
            .map(|fields| fields.iter().any(|f| f.as_str() == Some(field.as_str()) || f.get("name").and_then(Value::as_str) == Some(field.as_str())))
            .unwrap_or(false);
        let embedded = |result: &Value| result.get(&field).and_then(Value::as_f64).filter(|n| n.is_finite());
        let missing: Vec<i32> = results
            .iter()
            .filter(|r| embedded(r).is_none() && !payload_carries)
            .filter_map(|r| r.get("index").and_then(Value::as_i64).map(|i| i as i32))
            .collect();
        let mut store = None;
        if !missing.is_empty() {
            self.ensure_doc_values_manifest().await?;
            store = self.value_store_for_docs(std::slice::from_ref(&field), &missing).await?;
        }
        let mut touched = false;
        for result in results.iter_mut() {
            let rank = match embedded(result) {
                Some(rank) => Some(rank),
                None => match (result.get("index").and_then(Value::as_i64), &store) {
                    (Some(index), Some(store)) => self.value_for_doc(Some(store), &field, index as i32).as_number(),
                    _ => None,
                },
            };
            if let Some(rank) = rank.filter(|r| r.is_finite() && *r > 0.0) {
                let score = result.get("score").and_then(Value::as_f64).unwrap_or(0.0);
                result["score"] = num(score * (1.0 + weight * rank));
                touched = true;
            }
        }
        if touched {
            results.sort_by(|a, b| {
                let sa = a.get("score").and_then(Value::as_f64).unwrap_or(0.0);
                let sb = b.get("score").and_then(Value::as_f64).unwrap_or(0.0);
                cmp_f64(sb, sa).then(a.get("index").and_then(Value::as_i64).cmp(&b.get("index").and_then(Value::as_i64)))
            });
        }
        Ok(touched)
    }

    async fn execute_boosted_search(&self, params: &Value, weight: f64) -> Result<Value> {
        let (cfg, is_rank_prior) = self.active_rank_prior().unwrap();
        // `Math.max(1, Math.floor(Number(params.size ?? 10)))` and the same
        // for the page: doubles (NaN propagates, huge values stay huge).
        let size = js_max(1.0, crate::util::json::num_nullish(params.get("size").unwrap_or(&NULL), 10.0).floor());
        let page = js_max(1.0, crate::util::json::num_nullish(params.get("page").unwrap_or(&NULL), 1.0).floor());
        let factor_value = if !params.get("rankPriorOverfetch").unwrap_or(&NULL).is_null() {
            params["rankPriorOverfetch"].clone()
        } else if !is_rank_prior && !params.get("linkRankOverfetch").unwrap_or(&NULL).is_null() {
            params["linkRankOverfetch"].clone()
        } else {
            cfg.get("overfetch").cloned().unwrap_or(json!(LINKRANK_OVERFETCH))
        };
        let factor = js_max(1.0, to_number(&factor_value));
        let window = js_min(LINKRANK_MAX_POOL as f64, js_max(size * page, (size * page * factor).ceil()));
        let mut wide_params = params.clone();
        wide_params["size"] = json_number(window);
        wide_params["page"] = json!(1);
        let mut wide = self.execute_search(&wide_params).await?;
        let mut results: Vec<Value> = wide.get("results").and_then(Value::as_array).cloned().unwrap_or_default();
        let boosted = self.apply_rank_prior(&mut results, weight).await?;
        let pool = results.len();
        let offset = (page - 1.0) * size;
        let window_results: Vec<Value> = super::text::js_slice(&results, offset, offset + size);
        let shown = window_results.len();
        wide["results"] = Value::Array(window_results);
        wide["page"] = json_number(page);
        wide["size"] = json_number(size);
        if !wide.get("stats").map(Value::is_object).unwrap_or(false) {
            wide["stats"] = json!({});
        }
        let stats = wide["stats"].as_object_mut().unwrap();
        stats.insert("rankPriorBoost".into(), json!(boosted));
        stats.insert("rankPriorField".into(), cfg.get("field").cloned().unwrap_or(NULL.clone()));
        stats.insert("rankPriorBoostPool".into(), json!(pool));
        stats.insert("rankPriorBoostWindow".into(), json!(shown));
        if !is_rank_prior {
            stats.insert("linkRankBoost".into(), json!(boosted));
            stats.insert("linkRankBoostPool".into(), json!(pool));
            stats.insert("linkRankBoostWindow".into(), json!(shown));
        }
        if truthy(params.get("facets").unwrap_or(&NULL)) {
            self.compute_facet_counts(params, &mut wide).await?;
        }
        Ok(wide)
    }

    // -------------------------------------------------------- public methods

    /// `search(params)`.
    pub async fn search(&self, params: Value) -> Result<Value> {
        let surface = crate::util::json::string_or_empty(params.get("q").unwrap_or(&NULL));
        let normalized = crate::authority::address::normalize_postal_code_spacing(&surface);
        let mut active = params.clone();
        if normalized != surface {
            active["q"] = json!(normalized);
        }
        let weight = self.rank_prior_boost_weight(&active);
        let mut response = if weight > 0.0 {
            self.execute_boosted_search(&active, weight).await?
        } else {
            let mut response = self.execute_search(&active).await?;
            if truthy(active.get("facets").unwrap_or(&NULL)) {
                self.compute_facet_counts(&active, &mut response).await?;
            }
            response
        };
        if normalized != surface {
            response["normalizedQuery"] = json!(normalized);
            if !response.get("stats").map(Value::is_object).unwrap_or(false) {
                response["stats"] = json!({});
            }
            response["stats"]["postalCodeNormalized"] = json!(true);
        }
        Ok(response)
    }

    pub async fn count(&self, params: Value) -> Result<Value> {
        let surface = crate::util::json::string_or_empty(params.get("q").unwrap_or(&NULL));
        let normalized = crate::authority::address::normalize_postal_code_spacing(&surface);
        let mut active = params;
        if normalized != surface {
            active["q"] = json!(normalized);
        }
        self.execute_count(&active).await
    }

    /// Dispatch by JS method name.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let _scope = self.io.cache_budget.enter();
        match method {
            "search" => self.search(params).await,
            "count" => self.count(params).await,
            "suggest" => self.suggest(params).await,
            "vectorSearch" => self.vector_search(params).await,
            "authorityLookup" => self.authority_lookup(params).await,
            "searchAddressAuthority" => self.search_address_authority(params).await,
            "loadFacetValues" => {
                let field = params.get("field").and_then(Value::as_str).unwrap_or("").to_string();
                Ok(Value::Array((*self.load_facet_dictionary(&field).await?).clone()))
            }
            "loadDocValues" => self.load_doc_values_api(&params).await,
            "hydrateRows" => {
                let mut rows: Vec<Row> = Vec::new();
                for row in params.get("rows").and_then(Value::as_array).map(Vec::as_slice).unwrap_or(&[]) {
                    // `Number.isInteger(index) && index >= 0 && index < total`.
                    let index = row.get(0).unwrap_or(&NULL);
                    let doc = index.as_f64().filter(|n| n.fract() == 0.0 && *n >= 0.0 && *n < self.total() as f64);
                    let Some(doc) = doc else {
                        let shown = match index {
                            Value::Number(_) => super::filters::js_string(index).replace("e", "e+").replace("e+-", "e-").replace("e++", "e+"),
                            other => super::filters::js_string(other),
                        };
                        return Err(Error::new(format!("Rangefind: hydrateRows row index {shown} is not a document index in [0, {}).", self.total())));
                    };
                    rows.push((doc as i32, row.get(1).and_then(Value::as_f64).unwrap_or(0.0)));
                }
                let mut context = ResultContext::from_json(params.get("context").unwrap_or(&NULL));
                Ok(Value::Array(self.rows_to_results(&rows, &mut context).await?))
            }
            "stats" => Ok(json!({ "transfer": self.io.trace.to_json(), "totals": {
                "requests": self.io.trace.totals().requests, "bytes": self.io.trace.totals().bytes } })),
            "manifest" => Ok(self.manifest_value()),
            other => Err(Error::new(format!("Rangefind: unknown method \"{other}\"."))),
        }
    }

    async fn load_doc_values_api(&self, params: &Value) -> Result<Value> {
        let fields: Vec<String> = params.get("fields").and_then(Value::as_array).map(|f| f.iter().filter_map(Value::as_str).map(str::to_string).collect()).unwrap_or_default();
        let indexes: Vec<i32> = params.get("indexes").and_then(Value::as_array).map(|f| f.iter().filter_map(Value::as_i64).map(|i| i as i32).collect()).unwrap_or_default();
        if indexes.is_empty() {
            return Ok(json!([]));
        }
        self.ensure_doc_values_manifest().await?;
        let store = self.value_store_for_docs(&fields, &indexes).await?;
        Ok(Value::Array(
            indexes
                .iter()
                .map(|index| {
                    let mut map = Map::new();
                    for field in &fields {
                        map.insert(field.clone(), self.value_for_doc(store.as_ref(), field, *index).to_json());
                    }
                    Value::Object(map)
                })
                .collect(),
        ))
    }
}

/// `requestPage(params.page)`: `Math.max(1, Number(page || 1))` as a
/// double, with a non-numeric page read as page 1.
pub(crate) fn request_page(value: Option<&Value>) -> f64 {
    let page = num_or(value.unwrap_or(&NULL), 1.0);
    if page.is_nan() {
        1.0
    } else {
        page.max(1.0)
    }
}

/// `requestSize(params.size, max)`: `Math.floor(Number(size || 10))`
/// clamped to [1, max], with a non-numeric size read as the default 10.
pub(crate) fn request_size(value: Option<&Value>, max: usize) -> usize {
    let size = num_or(value.unwrap_or(&NULL), 10.0).floor();
    if size.is_nan() {
        10.min(max)
    } else {
        size.clamp(1.0, max as f64) as usize
    }
}

/// `Math.round`: half rounds toward +infinity.
pub fn js_round(value: f64) -> f64 {
    (value + 0.5).floor()
}

fn normalize_facets_param(param: &Value) -> Option<(Vec<String>, usize)> {
    if !truthy(param) {
        return None;
    }
    let (fields, size_value) = match param {
        Value::Array(items) => (items.clone(), json!(0)),
        Value::Object(map) => (map.get("fields").and_then(Value::as_array).cloned()?, map.get("size").cloned().unwrap_or(NULL.clone())),
        _ => return None,
    };
    if fields.is_empty() {
        return None;
    }
    let size = num_or(&size_value, FACET_COUNT_SIZE as f64).floor().clamp(1.0, 100.0) as usize;
    Some((fields.iter().map(crate::util::json::string_or_empty).collect(), size))
}

/// `mergeSortPageFilter`.
fn merge_sort_page_filter(engine: &IndexEngine, filters: &Filters, field: &str, page_min: f64, page_max: f64) -> Filters {
    let mut out = filters.clone();
    if engine.info.boolean_fields.contains(field) {
        // Boolean sort pages hold codes 1 (false) and 2 (true): a
        // single-valued page narrows the posting blocks to that boolean.
        let user = out.booleans.iter().any(|(f, value)| f == field && !value.is_null());
        if !user && page_min == page_max && (page_min == 1.0 || page_min == 2.0) {
            let value = json!(page_min == 2.0);
            match out.booleans.iter_mut().find(|(f, _)| f == field) {
                Some(item) => item.1 = value,
                None => out.booleans.push((field.to_string(), value)),
            }
        }
        out.geo = None;
        return out;
    }
    let current = out.numbers.iter().find(|(f, _)| f == field).map(|(_, r)| r.clone()).unwrap_or(Value::Null);
    let normalized = engine.normalize_range(field, &current);
    let pmin = if page_min.is_finite() { Some(page_min) } else { None };
    let pmax = if page_max.is_finite() { Some(page_max) } else { None };
    let min = match (normalized.min, pmin) {
        (None, p) => p,
        (c, None) => c,
        (Some(c), Some(p)) => Some(c.max(p)),
    };
    let max = match (normalized.max, pmax) {
        (None, p) => p,
        (c, None) => c,
        (Some(c), Some(p)) => Some(c.min(p)),
    };
    let range = json!({ "min": min.map(num), "max": max.map(num) });
    match out.numbers.iter_mut().find(|(f, _)| f == field) {
        Some(item) => item.1 = range,
        None => out.numbers.push((field.to_string(), range)),
    }
    out.geo = None;
    out
}

/// `compareKnownSortRows` over a doc → sort value map.
fn compare_known_sort_rows(a: &Row, b: &Row, desc: bool, values: &HashMap<i32, f64>) -> std::cmp::Ordering {
    match (values.get(&a.0), values.get(&b.0)) {
        (None, None) => cmp_f64(b.1, a.1).then(a.0.cmp(&b.0)),
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(l), Some(r)) if l != r => {
            if desc {
                cmp_f64(*r, *l)
            } else {
                cmp_f64(*l, *r)
            }
        }
        _ => cmp_f64(b.1, a.1).then(a.0.cmp(&b.0)),
    }
}

/// `nextSortPageCanTie`.
fn next_sort_page_can_tie(page: Option<&crate::codec::sort_tree::SortPageMeta>, boundary: Option<f64>, desc: bool) -> bool {
    match (page, boundary) {
        (Some(page), Some(boundary)) => {
            if desc {
                page.max >= boundary
            } else {
                page.min <= boundary
            }
        }
        _ => false,
    }
}

#[derive(Default)]
struct SortedCounters {
    pages_visited: usize,
    rows_scanned: usize,
    rows_accepted: usize,
    blocks_decoded: usize,
    rows_lookup_scanned: usize,
    lookup_hits: usize,
    candidate_blocks: usize,
    skipped_blocks: usize,
    considered_blocks: usize,
    considered_superblocks: usize,
    skipped_superblocks: usize,
    definite_pages: usize,
}

/// `sortedTextCandidateBlockIndexes`.
fn sorted_text_candidate_blocks(entry: &crate::codec::postings::TermEntry, plan: &super::filters::BlockFilterPlan, candidates: &HashSet<i32>) -> super::text::CandidateBlocks {
    use super::filters::block_may_pass;
    let overlaps = |doc_min: i64, doc_max: i64| candidates.is_empty() || candidates.iter().any(|doc| (*doc as i64) >= doc_min && (*doc as i64) <= doc_max);
    let mut out = super::text::CandidateBlocks { indexes: Vec::new(), considered_blocks: 0, skipped_blocks: 0, considered_superblocks: 0, skipped_superblocks: 0 };
    let blocks = &entry.blocks;
    let consider = |index: usize, out: &mut super::text::CandidateBlocks| {
        out.considered_blocks += 1;
        let block = &blocks[index];
        if block_may_pass(&block.filters, Some(plan)) && overlaps(block.doc_min, block.doc_max) {
            out.indexes.push(index);
        } else {
            out.skipped_blocks += 1;
        }
    };
    if !entry.superblocks.is_empty() {
        for superblock in &entry.superblocks {
            out.considered_superblocks += 1;
            let first = superblock.first_block;
            let end = blocks.len().min(first + superblock.block_count);
            if !block_may_pass(&superblock.filters, Some(plan)) || !overlaps(superblock.doc_min, superblock.doc_max) {
                out.skipped_superblocks += 1;
                out.skipped_blocks += end.saturating_sub(first);
                continue;
            }
            for index in first..end {
                consider(index, &mut out);
            }
        }
        return out;
    }
    for index in 0..blocks.len() {
        consider(index, &mut out);
    }
    out
}
