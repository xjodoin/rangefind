//! `createGenerationalSearch` from `src/runtime.js`: indexes published by
//! `rangefind build --update`. Every generation is a complete single index;
//! this layer fans queries out, drops tombstoned documents and merges.
//! Scores are comparable by construction (delta builds bake impacts with
//! corpus-wide statistics).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::highlight::{apply_highlights, highlight_term_set, HighlightOptions};
use crate::rt::{join2, try_join_all, Runtime};
use crate::transport::resolve_url;
use crate::util::json::{num, string_or_empty, to_number, truthy, unum, NULL};
use crate::util::jsstr::cmp16;

use super::super::authority_lane::suggestion_hydration_context;
use super::super::docs::{ResultContext, Row};
use super::super::index::IndexEngine;
use super::super::io::Io;
use super::super::options::EngineOptions;
use super::super::open_with_io;
use super::{
    child_options, first_correction, geo_sort_is, index_key, js_number, js_slice, merge_boost_stats, merge_federated_facets,
    number_or_zero, ordering_of, page_param, parse_sort_plan, results_of, size_param, sortable_fields, spread, without, SortPlan,
};

pub struct GenerationalEngine {
    pub root: Value,
    pub engines: Vec<Rc<IndexEngine>>,
    pub io: Rc<Io>,
    tombstones: Vec<HashSet<i64>>,
    tombstone_total: usize,
    sortable: Vec<String>,
}

/// A merged row (`{ generation, index, score, ... }`).
#[derive(Clone)]
struct MergedRow {
    generation: usize,
    index: i64,
    score: f64,
    distance: Option<Value>,
    hybrid: Option<Map<String, Value>>,
    result: Option<Value>,
    key: Value,
}

impl MergedRow {
    fn new(generation: usize, index: i64, score: f64) -> Self {
        MergedRow { generation, index, score, distance: None, hybrid: None, result: None, key: Value::Null }
    }
}

/// `compareMergedTies`.
fn compare_merged_ties(a: &MergedRow, b: &MergedRow) -> Ordering {
    match ordering_of(b.score - a.score) {
        Ordering::Equal => a.generation.cmp(&b.generation).then(a.index.cmp(&b.index)),
        other => other,
    }
}

fn row_index(value: &Value) -> i64 {
    index_key(value.get("index")).unwrap_or(0)
}

impl GenerationalEngine {
    pub(crate) async fn open(root: Value, options: EngineOptions, base_url: String, io: Rc<Io>, runtime: Runtime) -> Result<GenerationalEngine> {
        let generations: Vec<Value> = root.get("generations").and_then(Value::as_array).cloned().unwrap_or_default();
        let opens = generations.iter().map(|generation| {
            let path = string_or_empty(generation.get("path").unwrap_or(&NULL));
            let manifest = generation.get("manifest").and_then(Value::as_str);
            let manifest_name = match manifest {
                Some(name) if !path.is_empty() && name.starts_with(&path) => name[path.len()..].to_string(),
                Some(name) if !name.is_empty() => name.to_string(),
                _ => "manifest.min.json".to_string(),
            };
            let base = resolve_url(&base_url, if path.is_empty() { "./" } else { &path });
            let child = child_options(&options, Some(manifest_name), &[("maxPageSize", json!(1000))]);
            open_with_io(io.clone(), base, child, runtime.clone())
        });
        let engines = try_join_all(opens)
            .await?
            .into_iter()
            .map(|engine| {
                engine
                    .single()
                    .ok_or_else(|| Error::new("Rangefind: a generation must be a single index, not a federated manifest."))
            })
            .collect::<Result<Vec<_>>>()?;
        let tombstones: Vec<HashSet<i64>> = generations
            .iter()
            .map(|generation| {
                generation
                    .get("tombstones")
                    .and_then(Value::as_array)
                    .map(|items| items.iter().filter_map(|item| index_key(Some(item))).collect())
                    .unwrap_or_default()
            })
            .collect();
        let tombstone_total = tombstones.iter().map(HashSet::len).sum();
        let sortable = engines.first().map(|engine| sortable_fields(&engine.manifest_value())).unwrap_or_default();
        Ok(GenerationalEngine { root, engines, io, tombstones, tombstone_total, sortable })
    }

    fn tombstoned(&self, generation: usize, index: Option<&Value>) -> bool {
        index_key(index).map(|key| self.tombstones[generation].contains(&key)).unwrap_or(false)
    }

    fn child_size(&self, generation: usize, need: f64) -> Value {
        num(super::js_min(1000.0, need + self.tombstones[generation].len() as f64))
    }

    /// Dispatch by JS method name.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        match method {
            "search" => self.search(params).await,
            "count" => self.count(params).await,
            "suggest" => self.suggest(params).await,
            "vectorSearch" => self.vector_search(params).await,
            "hydrateRows" => {
                let rows = parse_rows(params.get("rows").unwrap_or(&NULL));
                Ok(Value::Array(self.hydrate_rows(&rows, params.get("context").cloned().unwrap_or(json!({}))).await?))
            }
            "loadFacetValues" => match self.engines.first() {
                Some(engine) => engine.call("loadFacetValues", params).await,
                None => Err(Error::new("Cannot read properties of undefined (reading 'loadFacetValues')")),
            },
            "manifest" => Ok(self.root.clone()),
            "stats" => Ok(json!({ "transfer": self.io.trace.to_json(), "totals": {
                "requests": self.io.trace.totals().requests, "bytes": self.io.trace.totals().bytes } })),
            other => Err(Error::new(format!("Rangefind: unknown method \"{other}\"."))),
        }
    }

    /// `hydrateMergedRows`: hydrate per owning generation, then reassemble
    /// in merged order; row extras survive onto results.
    async fn hydrate_merged_rows(&self, page_rows: &[MergedRow]) -> Result<Vec<Value>> {
        let mut groups: Vec<(usize, Vec<&MergedRow>)> = Vec::new();
        for row in page_rows {
            match groups.iter_mut().find(|(generation, _)| *generation == row.generation) {
                Some((_, rows)) => rows.push(row),
                None => groups.push((row.generation, vec![row])),
            }
        }
        let hydrated = try_join_all(groups.iter().map(|(generation, rows)| async move {
            let input: Vec<Row> = rows.iter().map(|row| (row.index as i32, row.score)).collect();
            let mut context = ResultContext::from_json(&NULL);
            let results = self.engines[*generation].rows_to_results(&input, &mut context).await?;
            let mut out: Vec<((usize, i64), Value)> = Vec::new();
            for (i, row) in rows.iter().enumerate() {
                let mut map = results.get(i).map(spread).unwrap_or_default();
                map.insert("generation".into(), unum(*generation));
                if let Some(distance) = &row.distance {
                    map.insert("distanceMeters".into(), distance.clone());
                }
                if let Some(hybrid) = &row.hybrid {
                    map.insert("hybrid".into(), Value::Object(hybrid.clone()));
                }
                out.push(((*generation, row.index), Value::Object(map)));
            }
            Ok(out)
        }))
        .await?;
        let mut by_key: HashMap<(usize, i64), Value> = HashMap::new();
        for group in hydrated {
            for (key, value) in group {
                by_key.insert(key, value);
            }
        }
        Ok(page_rows.iter().filter_map(|row| by_key.get(&(row.generation, row.index)).cloned()).collect())
    }

    /// `applyMergedHighlights`: generations share one frozen analysis
    /// profile, so the first engine's analyzer speaks for all of them.
    fn apply_merged_highlights(&self, params: &Value, q: &str, results: &mut [Value], corrected: Option<&Value>) {
        let option = params.get("highlight").unwrap_or(&NULL);
        if !truthy(option) || results.is_empty() || q.is_empty() {
            return;
        }
        let Some(engine) = self.engines.first() else { return };
        let analyzer = &*engine.analyzer;
        let corrected = corrected.map(string_or_empty).unwrap_or_default();
        let term_set = highlight_term_set(q, &corrected, Some(analyzer));
        let option = if option == &Value::Bool(true) { json!({}) } else { option.clone() };
        let fields = option.get("fields").and_then(Value::as_array).map(|items| items.iter().filter_map(Value::as_str).map(str::to_string).collect());
        let max_chars = option.get("maxChars").filter(|v| !v.is_null()).map(to_number);
        apply_highlights(results, &term_set, &HighlightOptions { fields, max_chars, analyzer: Some(analyzer) });
    }

    /// `mergedResponseShell`.
    fn merged_response_shell(&self, responses: &[&Value], results: Vec<Value>, page: f64, size: f64) -> Map<String, Value> {
        let total = responses.iter().map(|response| number_or_zero(response.get("total"))).sum::<f64>() - self.tombstone_total as f64;
        let approximate = self.tombstone_total > 0 || responses.iter().any(|response| truthy(response.get("approximate").unwrap_or(&NULL)));
        let mut out = Map::new();
        out.insert("total".into(), num(super::js_max(results.len() as f64, total)));
        out.insert("results".into(), Value::Array(results));
        out.insert("page".into(), num(page));
        out.insert("size".into(), num(size));
        out.insert("approximate".into(), Value::Bool(approximate));
        if let Some((corrected, corrections)) = first_correction(responses) {
            out.insert("correctedQuery".into(), corrected);
            if let Some(corrections) = corrections {
                out.insert("corrections".into(), corrections);
            }
        }
        out.insert(
            "stats".into(),
            json!({
                "generations": self.engines.len(),
                "generationTotals": responses.iter().map(|response| num(number_or_zero(response.get("total")))).collect::<Vec<_>>(),
                "tombstones": self.tombstone_total
            }),
        );
        out
    }

    /// `generationalAddressAuthoritySearch`.
    async fn address_authority_search(&self, params: &Value, q: &str, page: f64, size: f64, offset: f64) -> Result<Option<Value>> {
        if page != 1.0 {
            return Ok(None);
        }
        let responses = try_join_all(self.engines.iter().enumerate().map(|(generation, engine)| {
            let mut child = spread(params);
            child.insert("includeResults".into(), Value::Bool(true));
            child.insert("page".into(), json!(1));
            child.insert("size".into(), self.child_size(generation, offset + size));
            engine.search_address_authority(Value::Object(child))
        }))
        .await?;
        if !responses.iter().any(truthy) {
            return Ok(None);
        }
        let lane_of = |response: &Value| super::stats_of(response).get("plannerLane").and_then(Value::as_str).map(str::to_string);
        let mut exact: Vec<MergedRow> = Vec::new();
        let mut interpolated: Vec<MergedRow> = Vec::new();
        for (generation, response) in responses.iter().enumerate() {
            if !truthy(response) {
                continue;
            }
            let target = match lane_of(response).as_deref() {
                Some("addressAuthorityExact") => &mut exact,
                Some("addressInterpolationExact") => &mut interpolated,
                _ => continue,
            };
            for result in results_of(response) {
                if self.tombstoned(generation, result.get("index")) {
                    continue;
                }
                let mut row = MergedRow::new(generation, row_index(result), number_or_zero(result.get("score")));
                row.result = Some(result.clone());
                target.push(row);
            }
        }
        let has_interpolation_lane = responses.iter().any(|response| lane_of(response).as_deref() == Some("addressInterpolationExact"));
        let exact_found = !exact.is_empty();
        let interpolated_found = !interpolated.is_empty();
        let mut selected = if exact_found { exact } else { interpolated };
        selected.sort_by(compare_merged_ties);
        let total = selected.len();
        let rows = js_slice(selected, offset, offset + size);
        let mut results: Vec<Value> = rows
            .iter()
            .map(|row| super::with_key(row.result.as_ref().unwrap_or(&NULL), "generation", unum(row.generation)))
            .collect();
        let lane = if exact_found || (!interpolated_found && !has_interpolation_lane) { "addressAuthorityExact" } else { "addressInterpolationExact" };
        let lane_responses: Vec<&Value> = responses.iter().filter(|response| lane_of(response).as_deref() == Some(lane)).collect();
        let complete = lane_responses.iter().all(|response| super::stats_of(response).get("topKProven") != Some(&Value::Bool(false)));
        let sum = |key: &str| lane_responses.iter().map(|response| to_number(super::stats_of(response).get(key).unwrap_or(&NULL))).sum::<f64>();
        let stats = json!({
            "generations": self.engines.len(),
            "generationTotals": responses.iter().map(|item| num(number_or_zero(item.get("total")))).collect::<Vec<_>>(),
            "tombstones": self.tombstone_total,
            "plannerLane": lane,
            "exact": complete,
            "topKProven": complete,
            "totalExact": complete,
            "tailExhausted": complete,
            "generationalAddressAuthority": true,
            "blocksDecoded": num(sum("blocksDecoded")),
            "postingsDecoded": num(sum("postingsDecoded"))
        });
        self.apply_merged_highlights(params, q, &mut results, None);
        Ok(Some(json!({
            "total": total,
            "results": results,
            "page": num(page),
            "size": num(size),
            "approximate": !complete,
            "stats": stats
        })))
    }

    pub async fn search(&self, params: Value) -> Result<Value> {
        let params = if params.is_object() { params } else { json!({}) };
        let surface = string_or_empty(params.get("q").unwrap_or(&NULL));
        let normalized = crate::authority::address::normalize_postal_code_spacing(&surface);
        let mut active = params.clone();
        if normalized != surface {
            active["q"] = json!(normalized);
        }
        if truthy(active.get("vector").unwrap_or(&NULL)) {
            return self.hybrid_search(&active).await;
        }
        let q = string_or_empty(active.get("q").unwrap_or(&NULL)).trim().to_string();
        let page = page_param(&params);
        let size = size_param(&params, 100.0, 10.0);
        let offset = (page - 1.0) * size;
        let sort_plan = parse_sort_plan(params.get("sort"), &self.sortable);
        let geo_distance_sort = geo_sort_is(&params, "distance");
        let geo_route_sort = geo_sort_is(&params, "route");
        if sort_plan.is_some() && (geo_distance_sort || geo_route_sort) {
            return Err(Error::new("Rangefind: use either sort or geo.sort, not both."));
        }
        if let Some(mut response) = self.address_authority_search(&active, &q, page, size, offset).await? {
            if normalized != surface {
                response["normalizedQuery"] = json!(normalized);
                response["stats"]["postalCodeNormalized"] = json!(true);
            }
            return Ok(response);
        }
        let geo_browse = truthy(params.get("geo").unwrap_or(&NULL)) && q.is_empty();
        if sort_plan.is_some() || geo_distance_sort || geo_route_sort || geo_browse {
            return self.ordered_search(&active, &q, page, size, offset, sort_plan, geo_distance_sort, geo_route_sort).await;
        }
        let responses = try_join_all(self.engines.iter().enumerate().map(|(generation, engine)| {
            let mut child = spread(&active);
            child.insert("includeResults".into(), Value::Bool(false));
            without(&mut child, &["highlight"]);
            child.insert("page".into(), json!(1));
            child.insert("size".into(), self.child_size(generation, offset + size));
            engine.search(Value::Object(child))
        }))
        .await?;
        let mut merged: Vec<MergedRow> = Vec::new();
        for (generation, response) in responses.iter().enumerate() {
            for row in results_of(response) {
                if self.tombstoned(generation, row.get("index")) {
                    continue;
                }
                let mut item = MergedRow::new(generation, row_index(row), number_or_zero(row.get("score")));
                item.distance = super::present(row.get("distanceMeters")).cloned();
                merged.push(item);
            }
        }
        if !q.is_empty() {
            merged.sort_by(compare_merged_ties);
        }
        let page_rows = js_slice(merged, offset, offset + size);
        let mut results = self.hydrate_merged_rows(&page_rows).await?;
        let refs: Vec<&Value> = responses.iter().collect();
        let corrected = first_correction(&refs).map(|(corrected, _)| corrected);
        self.apply_merged_highlights(&active, &q, &mut results, corrected.as_ref());
        let mut response = self.merged_response_shell(&refs, results, page, size);
        if let Some(stats) = response.get_mut("stats").and_then(Value::as_object_mut) {
            merge_boost_stats(stats, &refs);
        }
        if truthy(active.get("facets").unwrap_or(&NULL)) {
            response.insert("facets".into(), merge_federated_facets(&refs));
        }
        if normalized != surface {
            response.insert("normalizedQuery".into(), json!(normalized));
            response["stats"]["postalCodeNormalized"] = json!(true);
        }
        Ok(Value::Object(response))
    }

    /// `orderedSearch`: sorted browse, text + sort, geo browse and
    /// nearest-first geo. Each generation returns its own ordered hydrated
    /// page; the merge re-orders by the actual key.
    #[allow(clippy::too_many_arguments)]
    async fn ordered_search(
        &self,
        params: &Value,
        q: &str,
        page: f64,
        size: f64,
        offset: f64,
        sort_plan: Option<SortPlan>,
        geo_distance_sort: bool,
        geo_route_sort: bool,
    ) -> Result<Value> {
        let responses = try_join_all(self.engines.iter().enumerate().map(|(generation, engine)| {
            let mut child = spread(params);
            without(&mut child, &["highlight"]);
            child.insert("page".into(), json!(1));
            child.insert("size".into(), self.child_size(generation, offset + size));
            engine.search(Value::Object(child))
        }))
        .await?;
        let mut merged: Vec<MergedRow> = Vec::new();
        for (generation, response) in responses.iter().enumerate() {
            for result in results_of(response) {
                if self.tombstoned(generation, result.get("index")) {
                    continue;
                }
                let mut row = MergedRow::new(generation, row_index(result), number_or_zero(result.get("score")));
                row.result = Some(result.clone());
                merged.push(row);
            }
        }
        if let Some(plan) = &sort_plan {
            let keyed = try_join_all(self.engines.iter().enumerate().map(|(generation, engine)| {
                let indexes: Vec<i64> = merged.iter().filter(|row| row.generation == generation).map(|row| row.index).collect();
                let field = plan.field.clone();
                async move {
                    if indexes.is_empty() {
                        return Ok(Vec::new());
                    }
                    let values = engine.call("loadDocValues", json!({ "fields": [field.clone()], "indexes": indexes })).await?;
                    Ok(values
                        .as_array()
                        .map(|items| items.iter().map(|item| item.get(&field).cloned().unwrap_or(Value::Null)).collect())
                        .unwrap_or_default())
                }
            }))
            .await?;
            for (generation, values) in keyed.into_iter().enumerate() {
                let mut values = values.into_iter();
                for row in merged.iter_mut().filter(|row| row.generation == generation) {
                    row.key = values.next().unwrap_or(Value::Null);
                }
            }
            let desc = plan.desc;
            merged.sort_by(|a, b| {
                let left_missing = a.key.is_null();
                let right_missing = b.key.is_null();
                if left_missing || right_missing {
                    if left_missing && right_missing {
                        return compare_merged_ties(a, b);
                    }
                    return if left_missing { Ordering::Greater } else { Ordering::Less };
                }
                if a.key != b.key {
                    let (left, right) = (to_number(&a.key), to_number(&b.key));
                    return ordering_of(if desc { right - left } else { left - right });
                }
                compare_merged_ties(a, b)
            });
        } else if geo_distance_sort {
            merged.sort_by(|a, b| {
                let left = a.result.as_ref().and_then(|r| super::present(r.get("distanceMeters")));
                let right = b.result.as_ref().and_then(|r| super::present(r.get("distanceMeters")));
                match (left, right) {
                    (None, None) => compare_merged_ties(a, b),
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (Some(left), Some(right)) => match ordering_of(to_number(left) - to_number(right)) {
                        Ordering::Equal => compare_merged_ties(a, b),
                        other => other,
                    },
                }
            });
        } else if geo_route_sort {
            merged.sort_by(|a, b| {
                let left = route_key(a.result.as_ref().unwrap_or(&NULL));
                let right = route_key(b.result.as_ref().unwrap_or(&NULL));
                if !left.is_finite() || !right.is_finite() {
                    return compare_merged_ties(a, b);
                }
                match ordering_of(left - right) {
                    Ordering::Equal => compare_merged_ties(a, b),
                    other => other,
                }
            });
        }
        let mut results: Vec<Value> = js_slice(merged, offset, offset + size)
            .into_iter()
            .map(|row| super::with_key(row.result.as_ref().unwrap_or(&NULL), "generation", unum(row.generation)))
            .collect();
        let refs: Vec<&Value> = responses.iter().collect();
        let corrected = first_correction(&refs).map(|(corrected, _)| corrected);
        self.apply_merged_highlights(params, q, &mut results, corrected.as_ref());
        let mut response = self.merged_response_shell(&refs, results, page, size);
        if truthy(params.get("facets").unwrap_or(&NULL)) {
            response.insert("facets".into(), merge_federated_facets(&refs));
        }
        Ok(Value::Object(response))
    }

    /// `hybridSearch`: fuse text and vector lanes at the merged level.
    async fn hybrid_search(&self, params: &Value) -> Result<Value> {
        let q = string_or_empty(params.get("q").unwrap_or(&NULL)).trim().to_string();
        let page = page_param(params);
        let size = size_param(params, 100.0, 10.0);
        let offset = (page - 1.0) * size;

        let vector_lane = |need: f64| async move {
            let responses = try_join_all(self.engines.iter().enumerate().map(|(generation, engine)| {
                let mut child = Map::new();
                child.insert("q".into(), json!(""));
                for key in ["vector", "vectorField", "hybrid", "filters"] {
                    if let Some(value) = params.get(key) {
                        child.insert(key.into(), value.clone());
                    }
                }
                child.insert("page".into(), json!(1));
                child.insert("size".into(), self.child_size(generation, need));
                engine.search(Value::Object(child))
            }))
            .await?;
            let mut rows: Vec<MergedRow> = Vec::new();
            for (generation, response) in responses.iter().enumerate() {
                for result in results_of(response) {
                    if self.tombstoned(generation, result.get("index")) {
                        continue;
                    }
                    let mut row = MergedRow::new(generation, row_index(result), number_or_zero(result.get("score")));
                    row.result = Some(result.clone());
                    rows.push(row);
                }
            }
            rows.sort_by(compare_merged_ties);
            Ok::<_, Error>((js_slice(rows, 0.0, need), responses))
        };

        if q.is_empty() {
            let (rows, responses) = vector_lane(offset + size).await?;
            let results: Vec<Value> = js_slice(rows, offset, offset + size)
                .into_iter()
                .map(|row| super::with_key(row.result.as_ref().unwrap_or(&NULL), "generation", unum(row.generation)))
                .collect();
            let refs: Vec<&Value> = responses.iter().collect();
            let mut response = self.merged_response_shell(&refs, results, page, size);
            response.insert("approximate".into(), Value::Bool(true));
            return Ok(Value::Object(response));
        }

        let pool_size = super::js_max(size * 3.0, super::js_min(100.0, size * 5.0));
        let rrf_k = super::js_max(1.0, super::num_or_fallback(params.get("hybrid").and_then(|h| h.get("rrfK")), 60.0).floor());
        let text_lane = try_join_all(self.engines.iter().enumerate().map(|(generation, engine)| {
            let mut child = spread(params);
            without(&mut child, &["vector", "vectorField", "hybrid", "highlight"]);
            child.insert("includeResults".into(), Value::Bool(false));
            child.insert("page".into(), json!(1));
            child.insert("size".into(), self.child_size(generation, pool_size));
            engine.search(Value::Object(child))
        }));
        let (text_responses, vector) = join2(text_lane, vector_lane(pool_size)).await;
        let text_responses = text_responses?;
        let (vector_rows, _) = vector?;

        let mut text_rows: Vec<MergedRow> = Vec::new();
        for (generation, response) in text_responses.iter().enumerate() {
            for row in results_of(response) {
                if self.tombstoned(generation, row.get("index")) {
                    continue;
                }
                text_rows.push(MergedRow::new(generation, row_index(row), number_or_zero(row.get("score"))));
            }
        }
        text_rows.sort_by(compare_merged_ties);

        let mut fused: Vec<MergedRow> = Vec::new();
        let mut slots: HashMap<(usize, i64), usize> = HashMap::new();
        let mut add_ranked = |rows: &[MergedRow], lane: &str| {
            for (rank, row) in rows.iter().enumerate() {
                let key = (row.generation, row.index);
                let slot = *slots.entry(key).or_insert_with(|| {
                    let mut entry = MergedRow::new(row.generation, row.index, 0.0);
                    entry.hybrid = Some(Map::new());
                    fused.push(entry);
                    fused.len() - 1
                });
                let entry = &mut fused[slot];
                entry.score += 1.0 / (rrf_k + rank as f64 + 1.0);
                entry.hybrid.as_mut().unwrap().insert(lane.to_string(), unum(rank + 1));
            }
        };
        let text_pool = js_slice(text_rows, 0.0, pool_size);
        add_ranked(&text_pool, "text");
        add_ranked(&vector_rows, "vector");

        fused.sort_by(compare_merged_ties);
        let total = fused.len();
        let page_rows = js_slice(fused, offset, offset + size);
        let mut results = self.hydrate_merged_rows(&page_rows).await?;
        self.apply_merged_highlights(params, &q, &mut results, None);
        Ok(json!({
            "total": total,
            "results": results,
            "page": num(page),
            "size": num(size),
            "approximate": true,
            "stats": {
                "generations": self.engines.len(),
                "tombstones": self.tombstone_total,
                "hybrid": true
            }
        }))
    }

    /// Standalone ANN queries: similarities share one metric, so the
    /// cross-generation merge is a sort by score.
    pub async fn vector_search(&self, params: Value) -> Result<Value> {
        let params = if params.is_object() { params } else { json!({}) };
        let k = super::js_max(1.0, super::js_min(200.0, super::num_or_fallback(params.get("k"), 10.0).floor()));
        let responses = try_join_all(self.engines.iter().enumerate().map(|(generation, engine)| {
            let mut child = spread(&params);
            child.insert("includeResults".into(), Value::Bool(false));
            child.insert("k".into(), num(super::js_min(200.0, k + self.tombstones[generation].len() as f64)));
            engine.vector_search(Value::Object(child))
        }))
        .await?;
        let mut merged: Vec<MergedRow> = Vec::new();
        for (generation, response) in responses.iter().enumerate() {
            for row in results_of(response) {
                if self.tombstoned(generation, row.get("index")) {
                    continue;
                }
                let mut item = MergedRow::new(generation, row_index(row), number_or_zero(row.get("score")));
                item.result = Some(row.clone());
                merged.push(item);
            }
        }
        merged.sort_by(compare_merged_ties);
        let total = merged.len();
        let page_rows = js_slice(merged, 0.0, k);
        let results = if params.get("includeResults") == Some(&Value::Bool(false)) {
            page_rows
                .iter()
                .map(|row| {
                    json!({
                        "index": row.result.as_ref().and_then(|r| r.get("index")).cloned().unwrap_or(Value::Null),
                        "score": num(row.score),
                        "generation": row.generation
                    })
                })
                .collect()
        } else {
            self.hydrate_merged_rows(&page_rows).await?
        };
        Ok(json!({
            "total": total,
            "results": results,
            "stats": {
                "generations": self.engines.len(),
                "tombstones": self.tombstone_total,
                "perGeneration": responses.iter().map(|response| response.get("stats").cloned().filter(truthy).unwrap_or(json!({}))).collect::<Vec<_>>(),
                "exact": false,
                "approximate": true
            }
        }))
    }

    /// `hydrateGenerationalSuggestions`: advisory hydration through each
    /// suggestion's owning generation.
    async fn hydrate_suggestions(&self, suggestions: &mut [Map<String, Value>]) {
        let mut groups: Vec<(usize, Vec<i64>)> = Vec::new();
        for item in suggestions.iter() {
            let Some(generation) = generation_of(item) else { continue };
            let ordinals = suggestion_ordinals(item);
            if ordinals.is_empty() {
                continue;
            }
            let slot = match groups.iter().position(|(g, _)| *g == generation) {
                Some(slot) => slot,
                None => {
                    groups.push((generation, Vec::new()));
                    groups.len() - 1
                }
            };
            for ordinal in ordinals {
                if !groups[slot].1.contains(&ordinal) {
                    groups[slot].1.push(ordinal);
                }
            }
        }
        if groups.is_empty() {
            return;
        }
        let hydrated = crate::rt::join_all(groups.iter().map(|(generation, ordinals)| async move {
            let Some(engine) = self.engines.get(*generation) else { return HashMap::new() };
            let rows: Vec<Row> = ordinals.iter().map(|ordinal| (*ordinal as i32, 0.0)).collect();
            let mut context = suggestion_hydration_context(ordinals.len());
            let mut slots: HashMap<i64, Value> = HashMap::new();
            if let Ok(results) = engine.rows_to_results(&rows, &mut context).await {
                for (index, ordinal) in ordinals.iter().enumerate() {
                    if let Some(result) = results.get(index).filter(|value| truthy(value)) {
                        slots.insert(*ordinal, super::with_key(result, "generation", unum(*generation)));
                    }
                }
            }
            slots
        }))
        .await;
        for item in suggestions.iter_mut() {
            let Some(generation) = generation_of(item) else { continue };
            let slots = groups.iter().position(|(g, _)| *g == generation).map(|slot| &hydrated[slot]);
            let results: Vec<Value> =
                suggestion_ordinals(item).iter().filter_map(|ordinal| slots.and_then(|slots| slots.get(ordinal)).cloned()).collect();
            if let Some(first) = results.first().cloned() {
                item.insert("results".into(), Value::Array(results));
                item.insert("result".into(), first);
            }
        }
    }

    pub async fn suggest(&self, params: Value) -> Result<Value> {
        let params = if params.is_object() { params } else { json!({}) };
        let surface = string_or_empty(params.get("q").unwrap_or(&NULL));
        let normalized = crate::authority::address::normalize_postal_code_prefix_spacing(&surface);
        let mut active = params.clone();
        if normalized != surface {
            active["q"] = json!(normalized);
        }
        let size = super::js_max(1.0, super::js_min(50.0, super::num_or_fallback(params.get("size"), 8.0).floor()));
        let hydrate = active.get("hydrate") == Some(&Value::Bool(true));
        let mut forwarded = spread(&active);
        forwarded.shift_remove("hydrate");
        let forwarded = Value::Object(forwarded);
        let responses = try_join_all(self.engines.iter().map(|engine| engine.suggest(forwarded.clone()))).await?;
        let mut merged: Vec<Map<String, Value>> = Vec::new();
        let mut by_text: HashMap<String, usize> = HashMap::new();
        for (generation, response) in responses.iter().enumerate() {
            let items = response.get("suggestions").and_then(Value::as_array).ok_or_else(|| {
                Error::new("responses[genIndex].suggestions is not iterable")
            })?;
            for item in items {
                let item_map = spread(item);
                let docs: Vec<Value> = suggestion_doc_values(&item_map)
                    .into_iter()
                    .filter(|doc| !self.tombstoned(generation, Some(doc)))
                    .collect();
                let text_key = item.get("text").map(|text| text.to_string()).unwrap_or_default();
                match by_text.get(&text_key) {
                    Some(&slot) => {
                        let existing = &mut merged[slot];
                        let count = js_number(existing.get("count")) + js_number(item.get("count"));
                        existing.insert("count".into(), num(count));
                        let weight = js_number(item.get("weight"));
                        let existing_weight = js_number(existing.get("weight"));
                        if weight > existing_weight {
                            existing.insert("weight".into(), item.get("weight").cloned().unwrap_or(Value::Null));
                            set_generation_docs(existing, &docs, generation);
                        } else if weight == existing_weight {
                            set_generation_docs(existing, &[], generation);
                        }
                    }
                    None => {
                        let mut rest = item_map;
                        rest.shift_remove("doc");
                        rest.shift_remove("docs");
                        set_generation_docs(&mut rest, &docs, generation);
                        by_text.insert(text_key, merged.len());
                        merged.push(rest);
                    }
                }
            }
        }
        merged.sort_by(|a, b| {
            match ordering_of(js_number(b.get("weight")) - js_number(a.get("weight"))) {
                Ordering::Equal => {
                    let left = string_or_empty(a.get("text").unwrap_or(&NULL));
                    let right = string_or_empty(b.get("text").unwrap_or(&NULL));
                    if cmp16(&left, &right) == Ordering::Less {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                other => other,
            }
        });
        let mut suggestions = js_slice(merged, 0.0, size);
        if hydrate {
            self.hydrate_suggestions(&mut suggestions).await;
        }
        let mut out = Map::new();
        out.insert("q".into(), json!(surface));
        out.insert("suggestions".into(), Value::Array(suggestions.into_iter().map(Value::Object).collect()));
        let mut stats = Map::new();
        stats.insert("generations".into(), unum(self.engines.len()));
        if normalized != surface {
            out.insert("normalizedQuery".into(), json!(normalized));
            stats.insert("postalCodeNormalized".into(), json!(true));
        }
        out.insert("stats".into(), Value::Object(stats));
        Ok(Value::Object(out))
    }

    /// `hydrateRows(rows, context)`: `context.generation` names the owning
    /// generation; results are generation-stamped.
    pub async fn hydrate_rows(&self, rows: &[Row], context: Value) -> Result<Vec<Value>> {
        let generation = js_number(context.get("generation")).floor();
        if !generation.is_finite() || generation < 0.0 || generation >= self.engines.len() as f64 {
            return Err(Error::new("Rangefind: hydrateRows on a generational index requires context.generation naming one generation."));
        }
        let generation = generation as usize;
        let mut rest = spread(&context);
        rest.shift_remove("generation");
        let mut context = ResultContext::from_json(&Value::Object(rest));
        let docs = self.engines[generation].rows_to_results(rows, &mut context).await?;
        Ok(docs.iter().map(|doc| super::with_key(doc, "generation", unum(generation))).collect())
    }

    pub async fn count(&self, params: Value) -> Result<Value> {
        let params = if params.is_object() { params } else { json!({}) };
        let surface = string_or_empty(params.get("q").unwrap_or(&NULL));
        let normalized = crate::authority::address::normalize_postal_code_spacing(&surface);
        let mut active = params.clone();
        if normalized != surface {
            active["q"] = json!(normalized);
        }
        let responses = try_join_all(self.engines.iter().map(|engine| engine.count(active.clone()))).await?;
        let total: f64 = responses.iter().map(|response| number_or_zero(response.get("total"))).sum();
        let mut out = Map::new();
        out.insert("total".into(), num(total));
        out.insert(
            "totalExact".into(),
            Value::Bool(self.tombstone_total == 0 && responses.iter().all(|response| truthy(response.get("totalExact").unwrap_or(&NULL)))),
        );
        out.insert(
            "approximate".into(),
            Value::Bool(self.tombstone_total > 0 || responses.iter().any(|response| truthy(response.get("approximate").unwrap_or(&NULL)))),
        );
        let mut stats = Map::new();
        stats.insert("generations".into(), unum(self.engines.len()));
        stats.insert("tombstones".into(), unum(self.tombstone_total));
        if normalized != surface {
            out.insert("normalizedQuery".into(), json!(normalized));
            stats.insert("postalCodeNormalized".into(), json!(true));
        }
        out.insert("stats".into(), Value::Object(stats));
        Ok(Value::Object(out))
    }
}

/// `Number(a.result.routeRank ?? a.result.routeProgressMeters)`.
pub(crate) fn route_key(result: &Value) -> f64 {
    match super::present(result.get("routeRank")) {
        Some(rank) => to_number(rank),
        None => js_number(result.get("routeProgressMeters")),
    }
}

/// `item.docs || (item.doc != null ? [item.doc] : [])` as raw values.
fn suggestion_doc_values(item: &Map<String, Value>) -> Vec<Value> {
    match item.get("docs") {
        Some(Value::Array(docs)) => docs.clone(),
        Some(other) if truthy(other) => Vec::new(),
        _ => match super::present(item.get("doc")) {
            Some(doc) => vec![doc.clone()],
            None => Vec::new(),
        },
    }
}

fn suggestion_ordinals(item: &Map<String, Value>) -> Vec<i64> {
    suggestion_doc_values(item).iter().filter_map(|doc| index_key(Some(doc))).collect()
}

fn generation_of(item: &Map<String, Value>) -> Option<usize> {
    super::present(item.get("generation")).and_then(Value::as_u64).map(|g| g as usize)
}

/// `setSuggestionGenerationDocs(entry, docs, genIndex)`.
fn set_generation_docs(entry: &mut Map<String, Value>, docs: &[Value], generation: usize) {
    entry.shift_remove("doc");
    entry.shift_remove("docs");
    entry.shift_remove("generation");
    if docs.is_empty() {
        return;
    }
    entry.insert("doc".into(), docs[0].clone());
    if docs.len() > 1 {
        entry.insert("docs".into(), Value::Array(docs.to_vec()));
    }
    entry.insert("generation".into(), unum(generation));
}

/// `[[index, score], ...]` rows of a `hydrateRows` call.
pub(crate) fn parse_rows(value: &Value) -> Vec<Row> {
    value
        .as_array()
        .map(|rows| rows.iter().map(|r| (to_number(&r[0]) as i32, to_number(&r[1]))).collect())
        .unwrap_or_default()
}
