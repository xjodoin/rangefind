//! `createShardedSearch` from `src/runtime.js`: a root manifest listing
//! independently built shard indexes (typically geographic regions). Shard
//! engines open lazily; geo queries route by shard bbox, text queries route
//! through the root term directory, autocomplete through the root
//! suggest-routing authority artifact. Merged scores are comparable because
//! every shard bakes impacts from one frozen scoring-stats artifact.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::authority::suggest_routing::{active_suggest_routing, remap_suggest_routing_item, suggest_routing_directory, suggest_routing_shard_ids};
use crate::codec::text_routing::TEXT_ROUTING_FORMAT;
use crate::error::{Error, Result};
use crate::geo::route::prepare_route;
use crate::geo::shard_routing::{shard_bbox_distance_meters, shard_box_intersects, LatLonBox};
use crate::rt::{join2, try_join_all, PromiseSlot, Runtime};
use crate::transport::resolve_url;
use crate::typo::normalize_main_index_typo_options;
use crate::util::json::{num, path, string_or_empty, to_number, truthy, unum, NULL};
use crate::util::jsstr::cmp16;

use super::super::docs::Row;
use super::super::index::IndexEngine;
use super::super::io::Io;
use super::super::options::EngineOptions;
use super::super::{open_with_io, Engine};
use super::generational::{parse_rows, route_key};
use super::text_routing::{SuggestSelection, TextRoutingState};
use super::{
    child_options, first_correction, geo_sort_is, js_number, js_slice, js_string, merge_boost_stats, merge_federated_facets, number_or_zero,
    ordering_of, page_param, parse_sort_plan, present, results_of, size_param, sortable_fields, spread, with_key, without,
};

pub const SHARD_ROUTING_SLACK_RATIO: f64 = 1.05;
pub const SHARD_ROUTING_SLACK_METERS: f64 = 10000.0;

/// `shardRoutingBudget(meters)`.
pub fn shard_routing_budget(meters: f64) -> f64 {
    meters * SHARD_ROUTING_SLACK_RATIO + SHARD_ROUTING_SLACK_METERS
}

#[derive(Clone, Debug)]
pub struct ShardDescriptor {
    pub id: String,
    pub index: usize,
    pub path: String,
    pub manifest_name: String,
    pub total: Value,
    pub bbox: Option<[f64; 4]>,
    pub groups: Vec<String>,
}

pub struct ShardedEngine {
    pub root: Value,
    pub shards: Vec<ShardDescriptor>,
    pub io: Rc<Io>,
    base_url: String,
    options: EngineOptions,
    runtime: Runtime,
    engines: Vec<RefCell<Rc<PromiseSlot<Engine>>>>,
    sortable: Vec<String>,
    shard_id_index: HashMap<String, usize>,
    /// Group label → member shard indexes, in first-seen order.
    shard_groups: Vec<(String, Vec<usize>)>,
    text_routing: Option<TextRoutingState>,
    suggest_routing: Option<Value>,
    suggest_routing_ids: Vec<String>,
    root_authority: PromiseSlot<Rc<IndexEngine>>,
}

/// Geo/scope routing: a fixed selection or an expanding nearest-first
/// front (`{ index, meters }` in bbox-distance order).
#[derive(Clone)]
enum Route {
    Selected(Vec<usize>),
    Expanding(Vec<(usize, f64)>),
}

impl Route {
    fn indexes(&self) -> Vec<usize> {
        match self {
            Route::Selected(selected) => selected.clone(),
            Route::Expanding(front) => front.iter().map(|(index, _)| *index).collect(),
        }
    }
}

/// A merged row (`{ shardIndex, result }`).
struct ShardRow {
    shard: usize,
    result: Value,
}

/// `compareByScore`.
fn compare_by_score(a: &ShardRow, b: &ShardRow) -> Ordering {
    match ordering_of(number_or_zero(b.result.get("score")) - number_or_zero(a.result.get("score"))) {
        Ordering::Equal => match a.shard.cmp(&b.shard) {
            Ordering::Equal => ordering_of(number_or_zero(a.result.get("index")) - number_or_zero(b.result.get("index"))),
            other => other,
        },
        other => other,
    }
}

/// `sortKeyNumber(value)`.
fn sort_key_number(value: Option<&Value>) -> Option<f64> {
    let number = js_number(value);
    if number.is_finite() {
        Some(number)
    } else {
        None
    }
}

impl ShardedEngine {
    pub(crate) fn new(root: Value, options: EngineOptions, base_url: String, io: Rc<Io>, runtime: Runtime) -> Result<ShardedEngine> {
        let shards: Vec<ShardDescriptor> = root
            .get("shards")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .enumerate()
                    .map(|(index, shard)| {
                        let id = shard.get("id").filter(|value| truthy(value)).map(js_string).unwrap_or_else(|| format!("shard-{index}"));
                        let bbox = match shard.get("bbox").and_then(Value::as_array) {
                            Some(items) if items.len() == 4 => {
                                Some([to_number(&items[0]), to_number(&items[1]), to_number(&items[2]), to_number(&items[3])])
                            }
                            _ => None,
                        };
                        ShardDescriptor {
                            id,
                            index,
                            path: string_or_empty(shard.get("path").unwrap_or(&NULL)),
                            manifest_name: shard
                                .get("manifest")
                                .filter(|value| truthy(value))
                                .map(js_string)
                                .unwrap_or_else(|| "manifest.min.json".to_string()),
                            total: shard.get("total").filter(|value| truthy(value)).cloned().unwrap_or(json!(0)),
                            bbox,
                            groups: shard.get("groups").and_then(Value::as_array).map(|groups| groups.iter().map(js_string).collect()).unwrap_or_default(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        if shards.is_empty() {
            return Err(Error::new("Rangefind: sharded manifest has no shards."));
        }
        // `new Map(shards.map(...))`: a later duplicate id wins.
        let mut shard_id_index = HashMap::new();
        for shard in &shards {
            shard_id_index.insert(shard.id.clone(), shard.index);
        }
        let mut shard_groups: Vec<(String, Vec<usize>)> = Vec::new();
        for shard in &shards {
            for group in &shard.groups {
                match shard_groups.iter_mut().find(|(name, _)| name == group) {
                    Some((_, members)) => members.push(shard.index),
                    None => shard_groups.push((group.clone(), vec![shard.index])),
                }
            }
        }
        let text_block = root.get("text_routing").unwrap_or(&NULL);
        let text_routing = if text_block.get("format").and_then(Value::as_str) == Some(TEXT_ROUTING_FORMAT)
            && truthy(path(text_block, &["directory", "root"]))
        {
            let ids: Vec<String> = shards.iter().map(|shard| shard.id.clone()).collect();
            Some(TextRoutingState::new(text_block, &base_url, io.clone(), &ids, &|id| shard_id_index.get(id).copied()))
        } else {
            None
        };
        let suggest_routing = active_suggest_routing(&root).cloned();
        let suggest_routing_ids = suggest_routing_shard_ids(suggest_routing.as_ref());
        let engines = shards.iter().map(|_| RefCell::new(Rc::new(PromiseSlot::default()))).collect();
        Ok(ShardedEngine {
            sortable: sortable_fields(&root),
            root,
            shards,
            io,
            base_url,
            options,
            runtime,
            engines,
            shard_id_index,
            shard_groups,
            text_routing,
            suggest_routing,
            suggest_routing_ids,
            root_authority: PromiseSlot::default(),
        })
    }

    /// `engineAt(index)`: lazily opened (and cached) shard engine.
    async fn engine_at(&self, index: usize) -> Result<Engine> {
        let slot = self.engines[index].borrow().clone();
        let shard = &self.shards[index];
        slot.get_or_load(
            async {
                let manifest_name = if !shard.path.is_empty() && shard.manifest_name.starts_with(&shard.path) {
                    shard.manifest_name[shard.path.len()..].to_string()
                } else {
                    shard.manifest_name.clone()
                };
                let base = resolve_url(&self.base_url, if shard.path.is_empty() { "./" } else { &shard.path });
                let options = child_options(&self.options, Some(manifest_name), &[("maxPageSize", json!(1000)), ("trace", json!(false))]);
                open_with_io(self.io.clone(), base, options, self.runtime.clone()).await
            },
            false,
        )
        .await
    }

    /// `rootAuthorityEngine()`: the root suggest-routing artifact served
    /// through the single-index authority machinery via an injected
    /// manifest.
    async fn root_authority_engine(&self) -> Result<Rc<IndexEngine>> {
        let Some(routing) = &self.suggest_routing else { return Err(Error::new("Rangefind: no suggest routing.")) };
        self.root_authority
            .get_or_load(
                async {
                    let manifest = json!({
                        "version": 1,
                        "engine": "rangefind",
                        "features": {},
                        "total": number_or_zero(self.root.get("total")),
                        "directory": suggest_routing_directory(routing).clone(),
                        "authority": routing.get("authority").cloned().unwrap_or(Value::Null)
                    });
                    let mut options = child_options(&self.options, None, &[("trace", json!(false)), ("manifest", manifest.clone())]);
                    options.manifest = Some(manifest.clone());
                    Ok(Rc::new(IndexEngine::new(manifest, self.base_url.clone(), options, self.io.clone(), self.runtime.clone())?))
                },
                true,
            )
            .await
    }

    /// `candidateShards(params)`: explicit shard scoping by id or group.
    fn candidate_shards(&self, params: &Value) -> Result<Vec<usize>> {
        let Some(names) = present(params.get("shards")) else { return Ok((0..self.shards.len()).collect()) };
        let names: Vec<Value> = match names {
            Value::Array(items) => items.clone(),
            other => vec![other.clone()],
        };
        let mut picked: Vec<usize> = Vec::new();
        for name in &names {
            let id = js_string(name);
            if let Some(&index) = self.shard_id_index.get(&id) {
                if !picked.contains(&index) {
                    picked.push(index);
                }
                continue;
            }
            let Some((_, members)) = self.shard_groups.iter().find(|(group, _)| *group == id) else {
                let shards = self.shards.iter().map(|shard| shard.id.as_str()).collect::<Vec<_>>().join(", ");
                let groups = if self.shard_groups.is_empty() {
                    String::new()
                } else {
                    format!("; groups: {}", self.shard_groups.iter().map(|(group, _)| group.as_str()).collect::<Vec<_>>().join(", "))
                };
                return Err(Error::new(format!("Rangefind: unknown shard or group \"{id}\" (shards: {shards}{groups}).")));
            };
            for member in members {
                if !picked.contains(member) {
                    picked.push(*member);
                }
            }
        }
        picked.sort_unstable();
        Ok(picked)
    }

    /// `routeShards(geo, params)`.
    fn route_shards(&self, geo: &Value, params: &Value) -> Result<Route> {
        let candidates = self.candidate_shards(params)?;
        let all = Route::Selected(candidates.clone());
        if !truthy(geo) || !candidates.iter().any(|index| self.shards[*index].bbox.is_some()) {
            return Ok(all);
        }
        let bbox_of = |index: usize| self.shards[index].bbox;
        if truthy(geo.get("route").unwrap_or(&NULL)) {
            let mut route_options = Map::new();
            if let Some(corridor) = geo.get("corridorMeters") {
                route_options.insert("corridorMeters".into(), corridor.clone());
            }
            if let Some(precision) = geo.get("polylinePrecision") {
                route_options.insert("precision".into(), precision.clone());
            }
            let Ok(prepared) = prepare_route(&geo["route"], &Value::Object(route_options)) else { return Ok(all) };
            let boxes: Vec<LatLonBox> = prepared
                .boxes
                .iter()
                .map(|item| LatLonBox {
                    min_lat: item.min_lat_e7 as f64 / 1e7,
                    max_lat: item.max_lat_e7 as f64 / 1e7,
                    min_lon: item.min_lon_e7 as f64 / 1e7,
                    max_lon: item.max_lon_e7 as f64 / 1e7,
                })
                .collect();
            return Ok(Route::Selected(
                candidates
                    .into_iter()
                    .filter(|index| match bbox_of(*index) {
                        None => true,
                        Some(bbox) => boxes.iter().any(|item| shard_box_intersects(&bbox, item)),
                    })
                    .collect(),
            ));
        }
        if let Some(near) = geo.get("near").filter(|value| truthy(value)) {
            let lat = js_number(near.get("lat"));
            let lon = js_number(near.get("lon"));
            if !lat.is_finite() || !lon.is_finite() {
                return Ok(all);
            }
            let mut distances: Vec<(usize, f64)> = candidates
                .iter()
                .map(|index| (*index, bbox_of(*index).map(|bbox| shard_bbox_distance_meters(&bbox, lat, lon)).unwrap_or(0.0)))
                .collect();
            let radius = js_number(near.get("radiusMeters"));
            if radius.is_finite() && radius > 0.0 {
                return Ok(Route::Selected(
                    distances.into_iter().filter(|(_, meters)| *meters <= shard_routing_budget(radius)).map(|(index, _)| index).collect(),
                ));
            }
            distances.sort_by(|a, b| ordering_of(a.1 - b.1));
            return Ok(Route::Expanding(distances));
        }
        if let Some(query) = geo.get("box").filter(|value| truthy(value)) {
            let query = LatLonBox {
                min_lat: js_number(query.get("minLat")),
                max_lat: js_number(query.get("maxLat")),
                min_lon: js_number(query.get("minLon")),
                max_lon: js_number(query.get("maxLon")),
            };
            return Ok(Route::Selected(
                candidates.into_iter().filter(|index| bbox_of(*index).map(|bbox| shard_box_intersects(&bbox, &query)).unwrap_or(true)).collect(),
            ));
        }
        Ok(all)
    }

    /// `withTextRoute(route, q)`: narrow a geo/scope route to the shards
    /// whose terms can satisfy the query; fail open.
    async fn with_text_route(&self, route: Route, q: &str) -> (Route, Option<Value>) {
        let Some(routing) = &self.text_routing else { return (route, None) };
        if q.is_empty() {
            return (route, None);
        }
        let selection = match routing.select_shards(q).await {
            Ok(Some(selection)) => selection,
            Ok(None) => return (route, None),
            Err(_) => return (route, Some(json!({ "fallback": "error" }))),
        };
        let narrowed = match &route {
            Route::Expanding(front) => Route::Expanding(front.iter().filter(|(index, _)| selection.selected.contains(index)).copied().collect()),
            Route::Selected(selected) => Route::Selected(selected.iter().filter(|index| selection.selected.contains(index)).copied().collect()),
        };
        let kept = narrowed.indexes().len();
        if kept == 0 {
            return (route, Some(json!({ "terms": selection.terms, "fallback": "no-shard-support" })));
        }
        (narrowed, Some(json!({ "terms": selection.terms, "selected": kept })))
    }

    /// `withSuggestRoute(params, q)`.
    async fn with_suggest_route(&self, params: &Value, q: &str) -> Result<(Vec<usize>, Option<Value>)> {
        let candidates = self.candidate_shards(params)?;
        let Some(routing) = &self.text_routing else { return Ok((candidates, None)) };
        if q.is_empty() {
            return Ok((candidates, None));
        }
        let selection = match routing.select_suggest_shards(q).await {
            Ok(Some(selection)) => selection,
            Ok(None) => return Ok((candidates, None)),
            Err(_) => return Ok((candidates, Some(json!({ "fallback": "error" })))),
        };
        match selection {
            SuggestSelection::Fallback { prefixes, segments } => {
                Ok((candidates, Some(json!({ "fallback": "prefix-scan-limit", "prefixes": prefixes, "segments": segments }))))
            }
            SuggestSelection::Selected { selected, matched, prefixes, segments } => {
                let narrowed: Vec<usize> = candidates.iter().filter(|index| selected.contains(index)).copied().collect();
                if matched == 0 || narrowed.is_empty() {
                    return Ok((candidates, Some(json!({ "prefixes": prefixes, "segments": segments, "fallback": "no-shard-support" }))));
                }
                let kept = narrowed.len();
                Ok((narrowed, Some(json!({ "prefixes": prefixes, "segments": segments, "selected": kept }))))
            }
        }
    }

    /// `stampShard(result, shardIndex)`: hierarchical ids compose as paths.
    fn stamp_shard(&self, result: &Value, shard: usize) -> Value {
        let id = &self.shards[shard].id;
        let stamped = match present(result.get("shard")) {
            Some(inner) => format!("{id}/{}", js_string(inner)),
            None => id.clone(),
        };
        with_key(result, "shard", Value::String(stamped))
    }

    async fn search_shard(&self, index: usize, params: Value) -> Result<(usize, Value)> {
        let engine = self.engine_at(index).await?;
        Ok((index, engine.call("search", params).await?))
    }

    /// `expandingNearestQuery(front, params, need)`: visit shards in bbox
    /// distance order until the next cannot beat the page's worst kept
    /// distance.
    async fn expanding_nearest_query(&self, front: &[(usize, f64)], params: &Value, need: f64) -> Result<(Vec<(usize, Value)>, bool)> {
        const BATCH_SIZE: usize = 4;
        let mut queried: Vec<(usize, Value)> = Vec::new();
        let mut distances: Vec<f64> = Vec::new();
        let mut cursor = 0usize;
        while cursor < front.len() {
            if distances.len() as f64 >= need {
                distances.sort_by(|a, b| ordering_of(a - b));
                let worst = distances[need as usize - 1];
                if front[cursor].1 > shard_routing_budget(worst) {
                    break;
                }
            }
            let batch: Vec<usize> = front[cursor..(cursor + BATCH_SIZE).min(front.len())].iter().map(|(index, _)| *index).collect();
            cursor += batch.len();
            let results = try_join_all(batch.iter().map(|index| self.search_shard(*index, params.clone()))).await?;
            for item in results {
                for result in results_of(&item.1) {
                    if let Some(distance) = present(result.get("distanceMeters")) {
                        distances.push(to_number(distance));
                    }
                }
                queried.push(item);
            }
        }
        Ok((queried, cursor < front.len()))
    }

    /// `shardResponseShell(responses, results, page, size, meta)`.
    fn shard_response_shell(&self, responses: &[&Value], results: Vec<Value>, page: f64, size: f64, partial: bool, meta_stats: Option<Map<String, Value>>) -> Map<String, Value> {
        let total: f64 = responses.iter().map(|response| number_or_zero(response.get("total"))).sum();
        let mut out = Map::new();
        out.insert("total".into(), num(super::js_max(results.len() as f64, total)));
        out.insert("results".into(), Value::Array(results));
        out.insert("page".into(), num(page));
        out.insert("size".into(), num(size));
        out.insert(
            "approximate".into(),
            Value::Bool(partial || responses.iter().any(|response| truthy(response.get("approximate").unwrap_or(&NULL)))),
        );
        if let Some((corrected, corrections)) = first_correction(responses) {
            out.insert("correctedQuery".into(), corrected);
            if let Some(corrections) = corrections {
                out.insert("corrections".into(), corrections);
            }
        }
        let mut stats = Map::new();
        stats.insert("shards".into(), unum(self.shards.len()));
        stats.insert("shardsQueried".into(), unum(responses.len()));
        stats.insert("shardTotals".into(), Value::Array(responses.iter().map(|response| num(number_or_zero(response.get("total")))).collect()));
        for (key, value) in meta_stats.unwrap_or_default() {
            stats.insert(key, value);
        }
        out.insert("stats".into(), Value::Object(stats));
        out
    }

    /// Dispatch by JS method name.
    pub async fn call(self: &Rc<Self>, method: &str, params: Value) -> Result<Value> {
        match method {
            "search" => self.search(params).await,
            "count" => self.count(params).await,
            "suggest" => self.suggest(params).await,
            "vectorSearch" => self.vector_search(params).await,
            "authorityLookup" => self.authority_lookup(params).await,
            "hydrateRows" => {
                let rows = parse_rows(params.get("rows").unwrap_or(&NULL));
                Ok(Value::Array(self.hydrate_rows(&rows, params.get("context").cloned().unwrap_or(json!({}))).await?))
            }
            "loadFacetValues" => self.load_facet_values(params).await,
            "prefetchShards" => {
                self.prefetch_shards(&params);
                Ok(Value::Null)
            }
            "shards" => Ok(Value::Array(
                self.shards
                    .iter()
                    .map(|shard| json!({ "id": shard.id, "path": shard.path, "total": shard.total, "bbox": shard.bbox.map(|b| b.iter().map(|v| num(*v)).collect::<Vec<_>>()) }))
                    .collect(),
            )),
            "manifest" => Ok(self.root.clone()),
            "stats" => Ok(json!({ "transfer": self.io.trace.to_json(), "totals": {
                "requests": self.io.trace.totals().requests, "bytes": self.io.trace.totals().bytes } })),
            other => Err(Error::new(format!("Rangefind: unknown method \"{other}\"."))),
        }
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
        let need = offset + size;
        let sort_plan = parse_sort_plan(params.get("sort"), &self.sortable);
        let geo_distance_sort = geo_sort_is(&active, "distance");
        let geo_route_sort = geo_sort_is(&active, "route");
        if sort_plan.is_some() && (geo_distance_sort || geo_route_sort) {
            return Err(Error::new("Rangefind: use either sort or geo.sort, not both."));
        }

        let route = self.route_shards(active.get("geo").unwrap_or(&NULL), &active)?;
        let (route, route_stats) = self.with_text_route(route, &q).await;
        let mut per_shard = spread(&active);
        without(&mut per_shard, &["shards"]);
        per_shard.insert("page".into(), json!(1));
        per_shard.insert("size".into(), num(super::js_min(1000.0, need)));
        let per_shard = Value::Object(per_shard);

        let mut partial = need > 1000.0;
        let typo = normalize_main_index_typo_options(&self.options.raw, &self.root);
        let typo_deferred = typo.mode != "off" && !q.is_empty() && active.get("typo") != Some(&Value::Bool(false));
        let first_params = if typo_deferred { with_key(&per_shard, "typo", Value::Bool(false)) } else { per_shard.clone() };
        let mut queried = self.run_fanout(&route, &first_params, need, &mut partial).await?;
        if typo_deferred {
            let merged_total: f64 = queried.iter().map(|(_, response)| number_or_zero(response.get("total"))).sum();
            let weak = typo.trigger == "zero-or-weak" && merged_total <= typo.weak_result_total as f64;
            if merged_total == 0.0 || weak {
                queried = self.run_fanout(&route, &per_shard, need, &mut partial).await?;
            }
        }

        let mut rows: Vec<ShardRow> = Vec::new();
        for (index, response) in &queried {
            for result in results_of(response) {
                rows.push(ShardRow { shard: *index, result: result.clone() });
            }
        }
        if let Some(plan) = &sort_plan {
            let desc = plan.desc;
            let field = plan.field.clone();
            rows.sort_by(|a, b| {
                let left = sort_key_number(a.result.get(&field));
                let right = sort_key_number(b.result.get(&field));
                match (left, right) {
                    (None, None) => compare_by_score(a, b),
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (Some(left), Some(right)) => {
                        if left != right {
                            ordering_of(if desc { right - left } else { left - right })
                        } else {
                            compare_by_score(a, b)
                        }
                    }
                }
            });
        } else if geo_distance_sort {
            rows.sort_by(|a, b| {
                let left = present(a.result.get("distanceMeters"));
                let right = present(b.result.get("distanceMeters"));
                match (left, right) {
                    (None, None) => compare_by_score(a, b),
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (Some(left), Some(right)) => match ordering_of(to_number(left) - to_number(right)) {
                        Ordering::Equal => compare_by_score(a, b),
                        other => other,
                    },
                }
            });
        } else if geo_route_sort {
            rows.sort_by(|a, b| {
                let left = route_key(&a.result);
                let right = route_key(&b.result);
                if !left.is_finite() || !right.is_finite() {
                    return compare_by_score(a, b);
                }
                match ordering_of(left - right) {
                    Ordering::Equal => compare_by_score(a, b),
                    other => other,
                }
            });
        } else if !q.is_empty() {
            rows.sort_by(compare_by_score);
        }

        // Overlapping shard bboxes may place one logical document in more
        // than one shard: collapse by public id after ranking.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut unique: Vec<ShardRow> = Vec::new();
        let mut collapsed = 0usize;
        for row in rows {
            let id = match present(row.result.get("id")) {
                None => String::new(),
                Some(id) => js_string(id),
            };
            if !id.is_empty() && seen.contains(&id) {
                collapsed += 1;
                continue;
            }
            if !id.is_empty() {
                seen.insert(id);
            }
            unique.push(row);
        }
        let results: Vec<Value> = js_slice(unique, offset, need).iter().map(|row| self.stamp_shard(&row.result, row.shard)).collect();
        let responses: Vec<&Value> = queried.iter().map(|(_, response)| response).collect();
        let meta_stats = if route_stats.is_some() || collapsed > 0 {
            let mut stats = Map::new();
            if let Some(route_stats) = route_stats {
                stats.insert("textRouting".into(), route_stats);
            }
            if collapsed > 0 {
                stats.insert("federationDuplicatesCollapsed".into(), unum(collapsed));
            }
            Some(stats)
        } else {
            None
        };
        let mut response = self.shard_response_shell(&responses, results, page, size, partial, meta_stats);
        if collapsed > 0 {
            response.insert("approximate".into(), Value::Bool(true));
        }
        if let Some(stats) = response.get_mut("stats").and_then(Value::as_object_mut) {
            merge_boost_stats(stats, &responses);
        }
        if truthy(active.get("facets").unwrap_or(&NULL)) {
            let mut facets = merge_federated_facets(&responses);
            if partial {
                if let Some(map) = facets.as_object_mut() {
                    for facet in map.values_mut() {
                        facet["exact"] = Value::Bool(false);
                    }
                }
            }
            response.insert("facets".into(), facets);
        }
        if normalized != surface {
            response.insert("normalizedQuery".into(), json!(normalized));
            response["stats"]["postalCodeNormalized"] = json!(true);
        }
        Ok(Value::Object(response))
    }

    async fn run_fanout(&self, route: &Route, params: &Value, need: f64, partial: &mut bool) -> Result<Vec<(usize, Value)>> {
        match route {
            Route::Expanding(front) => {
                let (queried, cut) = self.expanding_nearest_query(front, params, need).await?;
                *partial = *partial || cut;
                Ok(queried)
            }
            Route::Selected(selected) => try_join_all(selected.iter().map(|index| self.search_shard(*index, params.clone()))).await,
        }
    }

    /// Hybrid text + vector across shards, fused at the merged level.
    async fn hybrid_search(&self, params: &Value) -> Result<Value> {
        let q = string_or_empty(params.get("q").unwrap_or(&NULL)).trim().to_string();
        let page = page_param(params);
        let size = size_param(params, 100.0, 10.0);
        let offset = (page - 1.0) * size;
        let route = self.route_shards(params.get("geo").unwrap_or(&NULL), params)?;
        let selected = route.indexes();
        let pool_size = super::js_max(size * 3.0, super::js_min(100.0, size * 5.0));

        let lane = |lane_params: Map<String, Value>| {
            let selected = selected.clone();
            async move {
                let mut child = lane_params;
                without(&mut child, &["shards"]);
                child.insert("page".into(), json!(1));
                child.insert("size".into(), num(super::js_min(1000.0, pool_size)));
                let child = Value::Object(child);
                let queried = try_join_all(selected.iter().map(|index| self.search_shard(*index, child.clone()))).await?;
                let mut rows: Vec<ShardRow> = Vec::new();
                for (index, response) in &queried {
                    for result in results_of(response) {
                        rows.push(ShardRow { shard: *index, result: result.clone() });
                    }
                }
                rows.sort_by(compare_by_score);
                Ok::<_, Error>((js_slice(rows, 0.0, pool_size), queried.into_iter().map(|(_, response)| response).collect::<Vec<_>>()))
            }
        };
        let mut vector_params = Map::new();
        vector_params.insert("q".into(), json!(""));
        for key in ["vector", "vectorField", "hybrid", "filters", "geo"] {
            if let Some(value) = params.get(key) {
                vector_params.insert(key.into(), value.clone());
            }
        }
        if q.is_empty() {
            let (rows, responses) = lane(vector_params).await?;
            let results: Vec<Value> = js_slice(rows, offset, offset + size).iter().map(|row| self.stamp_shard(&row.result, row.shard)).collect();
            let refs: Vec<&Value> = responses.iter().collect();
            let mut response = self.shard_response_shell(&refs, results, page, size, false, None);
            response.insert("approximate".into(), Value::Bool(true));
            return Ok(Value::Object(response));
        }
        let rrf_k = super::js_max(1.0, super::num_or_fallback(params.get("hybrid").and_then(|h| h.get("rrfK")), 60.0).floor());
        let mut text_params = spread(params);
        without(&mut text_params, &["vector", "vectorField", "hybrid"]);
        let (text_lane, vector_lane) = join2(lane(text_params), lane(vector_params)).await;
        let (text_rows, _) = text_lane?;
        let (vector_rows, _) = vector_lane?;

        struct Fused {
            shard: usize,
            result: Value,
            score: f64,
            hybrid: Map<String, Value>,
        }
        let mut fused: Vec<Fused> = Vec::new();
        let mut slots: HashMap<String, usize> = HashMap::new();
        let mut add_ranked = |rows: &[ShardRow], lane_name: &str| {
            for (rank, row) in rows.iter().enumerate() {
                let generation = present(row.result.get("generation")).map(js_string).unwrap_or_default();
                let index = row.result.get("index").map(js_string).unwrap_or_else(|| "undefined".to_string());
                let key = format!("{}:{generation}:{index}", row.shard);
                let slot = *slots.entry(key).or_insert_with(|| {
                    fused.push(Fused { shard: row.shard, result: row.result.clone(), score: 0.0, hybrid: Map::new() });
                    fused.len() - 1
                });
                let entry = &mut fused[slot];
                entry.score += 1.0 / (rrf_k + rank as f64 + 1.0);
                entry.hybrid.insert(lane_name.to_string(), unum(rank + 1));
            }
        };
        add_ranked(&text_rows, "text");
        add_ranked(&vector_rows, "vector");
        fused.sort_by(|a, b| match ordering_of(b.score - a.score) {
            Ordering::Equal => match a.shard.cmp(&b.shard) {
                Ordering::Equal => ordering_of(number_or_zero(a.result.get("index")) - number_or_zero(b.result.get("index"))),
                other => other,
            },
            other => other,
        });
        let total = fused.len();
        let results: Vec<Value> = js_slice(fused, offset, offset + size)
            .into_iter()
            .map(|entry| {
                let mut stamped = spread(&self.stamp_shard(&entry.result, entry.shard));
                stamped.insert("score".into(), num(entry.score));
                stamped.insert("hybrid".into(), Value::Object(entry.hybrid));
                Value::Object(stamped)
            })
            .collect();
        Ok(json!({
            "total": total,
            "results": results,
            "page": num(page),
            "size": num(size),
            "approximate": true,
            "stats": { "shards": self.shards.len(), "shardsQueried": selected.len(), "hybrid": true }
        }))
    }

    pub async fn vector_search(&self, params: Value) -> Result<Value> {
        let params = if params.is_object() { params } else { json!({}) };
        let k = super::js_max(1.0, super::js_min(200.0, super::num_or_fallback(params.get("k"), 10.0).floor()));
        let candidates = self.candidate_shards(&params)?;
        let mut child = spread(&params);
        without(&mut child, &["shards"]);
        child.insert("k".into(), num(k));
        let child = Value::Object(child);
        let queried = try_join_all(candidates.iter().map(|index| {
            let child = child.clone();
            async move {
                let engine = self.engine_at(*index).await?;
                Ok::<_, Error>((*index, engine.call("vectorSearch", child).await?))
            }
        }))
        .await?;
        let mut rows: Vec<ShardRow> = Vec::new();
        for (index, response) in &queried {
            for result in results_of(response) {
                rows.push(ShardRow { shard: *index, result: result.clone() });
            }
        }
        rows.sort_by(compare_by_score);
        let total = rows.len();
        let results: Vec<Value> = js_slice(rows, 0.0, k).iter().map(|row| self.stamp_shard(&row.result, row.shard)).collect();
        Ok(json!({
            "total": total,
            "results": results,
            "stats": {
                "shards": self.shards.len(),
                "perShard": queried.iter().map(|(_, response)| response.get("stats").cloned().filter(truthy).unwrap_or(json!({}))).collect::<Vec<_>>(),
                "exact": false,
                "approximate": true
            }
        }))
    }

    /// `hydrateShardedSuggestions`: batched per (shard, generation),
    /// advisory.
    async fn hydrate_suggestions(&self, suggestions: &mut [Value]) {
        struct Group {
            key: String,
            context: Value,
            ordinals: Vec<Value>,
        }
        let group_key = |item: &Value| -> Option<String> {
            present(item.get("doc"))?;
            let shard = item.get("docShard").filter(|value| truthy(value))?;
            let generation = present(item.get("generation")).map(js_string).unwrap_or_default();
            Some(format!("{}\u{0}{generation}", js_string(shard)))
        };
        let ordinals_of = |item: &Value| -> Vec<Value> {
            match item.get("docs").filter(|value| truthy(value)) {
                Some(Value::Array(docs)) => docs.clone(),
                Some(_) => Vec::new(),
                None => vec![item["doc"].clone()],
            }
        };
        let mut groups: Vec<Group> = Vec::new();
        for item in suggestions.iter() {
            let Some(key) = group_key(item) else { continue };
            let slot = match groups.iter().position(|group| group.key == key) {
                Some(slot) => slot,
                None => {
                    let mut context = Map::new();
                    context.insert("shard".into(), item["docShard"].clone());
                    if let Some(generation) = present(item.get("generation")) {
                        context.insert("generation".into(), generation.clone());
                    }
                    groups.push(Group { key, context: Value::Object(context), ordinals: Vec::new() });
                    groups.len() - 1
                }
            };
            for ordinal in ordinals_of(item) {
                if !groups[slot].ordinals.contains(&ordinal) {
                    groups[slot].ordinals.push(ordinal);
                }
            }
        }
        if groups.is_empty() {
            return;
        }
        let hydrated = crate::rt::join_all(groups.iter().map(|group| async move {
            let rows: Vec<Row> = group.ordinals.iter().map(|ordinal| (to_number(ordinal) as i32, 0.0)).collect();
            let mut context = Map::new();
            context.insert("hasTextTerms".into(), json!(false));
            context.insert("preferDocPages".into(), if group.ordinals.len() > 1 { json!("force") } else { json!(false) });
            for (key, value) in group.context.as_object().into_iter().flatten() {
                context.insert(key.clone(), value.clone());
            }
            let mut slots: Vec<Option<Value>> = vec![None; group.ordinals.len()];
            if let Ok(results) = self.hydrate_rows(&rows, Value::Object(context)).await {
                for (index, slot) in slots.iter_mut().enumerate() {
                    if let Some(result) = results.get(index).filter(|value| truthy(value)) {
                        *slot = Some(result.clone());
                    }
                }
            }
            slots
        }))
        .await;
        for item in suggestions.iter_mut() {
            let Some(key) = group_key(item) else { continue };
            let Some(slot) = groups.iter().position(|group| group.key == key) else { continue };
            let group = &groups[slot];
            let results: Vec<Value> = ordinals_of(item)
                .iter()
                .filter_map(|ordinal| group.ordinals.iter().position(|o| o == ordinal).and_then(|i| hydrated[slot][i].clone()))
                .collect();
            if let Some(first) = results.first().cloned() {
                item["results"] = Value::Array(results);
                item["result"] = first;
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

        if self.suggest_routing.is_some() && present(params.get("shards")).is_none() {
            if let Ok(response) = self.root_suggest(&forwarded, size, hydrate).await {
                let mut out = Map::new();
                out.insert("q".into(), json!(surface));
                out.insert("suggestions".into(), response.0);
                let mut stats = response.1;
                stats.insert("shards".into(), unum(self.shards.len()));
                stats.insert("shardsQueried".into(), json!(0));
                stats.insert("suggestRouting".into(), json!("root-authority"));
                if normalized != surface {
                    out.insert("normalizedQuery".into(), json!(normalized));
                    stats.insert("postalCodeNormalized".into(), json!(true));
                }
                out.insert("stats".into(), Value::Object(stats));
                return Ok(Value::Object(out));
            }
        }

        let forwarded = Value::Object(forwarded);
        let (routed, route_stats) = self.with_suggest_route(&forwarded, normalized.trim()).await?;
        let mut child = spread(&forwarded);
        without(&mut child, &["shards"]);
        let child = Value::Object(child);
        let responses = try_join_all(routed.iter().map(|index| {
            let child = child.clone();
            async move {
                let engine = self.engine_at(*index).await?;
                Ok::<_, Error>((*index, engine.call("suggest", child).await?))
            }
        }))
        .await?;
        let mut merged: Vec<Map<String, Value>> = Vec::new();
        let mut by_text: HashMap<String, usize> = HashMap::new();
        for (shard, response) in &responses {
            let shard_id = self.shards[*shard].id.clone();
            let items = response
                .get("suggestions")
                .and_then(Value::as_array)
                .ok_or_else(|| Error::new("response.suggestions is not iterable"))?;
            for item in items {
                let text_key = item.get("text").map(|text| text.to_string()).unwrap_or_default();
                match by_text.get(&text_key) {
                    Some(&slot) => {
                        let existing = &mut merged[slot];
                        let count = js_number(existing.get("count")) + js_number(item.get("count"));
                        existing.insert("count".into(), num(count));
                        let weight = js_number(item.get("weight"));
                        let existing_weight = js_number(existing.get("weight"));
                        let listed = existing
                            .get("shards")
                            .and_then(Value::as_array)
                            .map(|ids| ids.iter().any(|id| id.as_str() == Some(shard_id.as_str())))
                            .unwrap_or(false);
                        if weight > existing_weight {
                            existing.insert("weight".into(), item.get("weight").cloned().unwrap_or(Value::Null));
                            existing.insert("shards".into(), json!([shard_id.clone()]));
                            set_shard_docs(existing, item, &shard_id);
                        } else if weight == existing_weight && !listed {
                            if let Some(Value::Array(ids)) = existing.get_mut("shards") {
                                ids.push(json!(shard_id.clone()));
                            }
                            set_shard_docs(existing, &json!({}), &shard_id);
                        }
                    }
                    None => {
                        let mut entry = spread(item);
                        for key in ["doc", "docs", "generation"] {
                            entry.shift_remove(key);
                        }
                        entry.insert("shards".into(), json!([shard_id.clone()]));
                        set_shard_docs(&mut entry, item, &shard_id);
                        by_text.insert(text_key, merged.len());
                        merged.push(entry);
                    }
                }
            }
        }
        merged.sort_by(|a, b| match ordering_of(js_number(b.get("weight")) - js_number(a.get("weight"))) {
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
        });
        let mut suggestions: Vec<Value> = js_slice(merged, 0.0, size).into_iter().map(Value::Object).collect();
        if hydrate {
            self.hydrate_suggestions(&mut suggestions).await;
        }
        let mut out = Map::new();
        out.insert("q".into(), json!(surface));
        out.insert("suggestions".into(), Value::Array(suggestions));
        let mut stats = Map::new();
        stats.insert("shards".into(), unum(self.shards.len()));
        stats.insert("shardsQueried".into(), unum(routed.len()));
        if let Some(route_stats) = route_stats {
            stats.insert("textRouting".into(), route_stats);
        }
        if normalized != surface {
            out.insert("normalizedQuery".into(), json!(normalized));
            stats.insert("postalCodeNormalized".into(), json!(true));
        }
        out.insert("stats".into(), Value::Object(stats));
        Ok(Value::Object(out))
    }

    /// The root-authority suggest path: `(suggestions, response.stats)`.
    async fn root_suggest(&self, forwarded: &Map<String, Value>, size: f64, hydrate: bool) -> Result<(Value, Map<String, Value>)> {
        let root_engine = self.root_authority_engine().await?;
        let mut request = forwarded.clone();
        request.insert("size".into(), num(size));
        let response = root_engine.suggest(Value::Object(request)).await?;
        let is_known = |id: &str| self.shard_id_index.contains_key(id);
        let mut suggestions: Vec<Value> = response
            .get("suggestions")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(|item| Value::Object(remap_suggest_routing_item(&spread(item), &self.suggest_routing_ids, &is_known)))
                    .collect()
            })
            .unwrap_or_default();
        if hydrate {
            self.hydrate_suggestions(&mut suggestions).await;
        }
        Ok((Value::Array(suggestions), response.get("stats").map(spread).unwrap_or_default()))
    }

    pub async fn count(&self, params: Value) -> Result<Value> {
        let params = if params.is_object() { params } else { json!({}) };
        let surface = string_or_empty(params.get("q").unwrap_or(&NULL));
        let normalized = crate::authority::address::normalize_postal_code_spacing(&surface);
        let mut active = params.clone();
        if normalized != surface {
            active["q"] = json!(normalized);
        }
        let q = string_or_empty(active.get("q").unwrap_or(&NULL)).trim().to_string();
        let route = self.route_shards(active.get("geo").unwrap_or(&NULL), &active)?;
        let (route, route_stats) = self.with_text_route(route, &q).await;
        let selected = route.indexes();
        let mut child = spread(&active);
        without(&mut child, &["shards"]);
        let child = Value::Object(child);
        let responses = try_join_all(selected.iter().map(|index| {
            let child = child.clone();
            async move {
                let engine = self.engine_at(*index).await?;
                engine.call("count", child).await
            }
        }))
        .await?;
        let mut out = Map::new();
        out.insert("total".into(), num(responses.iter().map(|response| number_or_zero(response.get("total"))).sum()));
        out.insert("totalExact".into(), Value::Bool(responses.iter().all(|response| truthy(response.get("totalExact").unwrap_or(&NULL)))));
        out.insert("approximate".into(), Value::Bool(responses.iter().any(|response| truthy(response.get("approximate").unwrap_or(&NULL)))));
        let mut stats = Map::new();
        stats.insert("shards".into(), unum(self.shards.len()));
        stats.insert("shardsQueried".into(), unum(selected.len()));
        if let Some(route_stats) = route_stats {
            stats.insert("textRouting".into(), route_stats);
        }
        if normalized != surface {
            out.insert("normalizedQuery".into(), json!(normalized));
            stats.insert("postalCodeNormalized".into(), json!(true));
        }
        out.insert("stats".into(), Value::Object(stats));
        Ok(Value::Object(out))
    }

    /// `loadFacetValues(field)`: dictionaries merged across every shard.
    async fn load_facet_values(&self, params: Value) -> Result<Value> {
        let dictionaries = try_join_all(self.shards.iter().map(|shard| {
            let params = params.clone();
            async move {
                let engine = self.engine_at(shard.index).await?;
                engine.call("loadFacetValues", params).await
            }
        }))
        .await?;
        let mut merged: Vec<Map<String, Value>> = Vec::new();
        let mut by_value: HashMap<String, usize> = HashMap::new();
        for values in &dictionaries {
            for item in values.as_array().into_iter().flatten() {
                let Some(value) = item.get("value").filter(|value| truthy(value)) else { continue };
                let key = value.to_string();
                match by_value.get(&key) {
                    Some(&slot) => {
                        let n = to_number(merged[slot].get("n").unwrap_or(&NULL)) + number_or_zero(item.get("n"));
                        merged[slot].insert("n".into(), num(n));
                    }
                    None => {
                        by_value.insert(key, merged.len());
                        merged.push(spread(item));
                    }
                }
            }
        }
        merged.sort_by(|a, b| match ordering_of(number_or_zero(b.get("n")) - number_or_zero(a.get("n"))) {
            Ordering::Equal => super::less_or_greater(a.get("value").unwrap_or(&NULL), b.get("value").unwrap_or(&NULL)),
            other => other,
        });
        Ok(Value::Array(merged.into_iter().map(Value::Object).collect()))
    }

    /// `authorityLookup(surface, params)` against the root suggest-routing
    /// artifact; advisory (null on any failure).
    async fn authority_lookup(&self, params: Value) -> Result<Value> {
        if self.suggest_routing.is_none() {
            return Ok(Value::Null);
        }
        let lookup = async {
            let root_engine = self.root_authority_engine().await?;
            root_engine.authority_lookup(params).await
        };
        let Ok(response) = lookup.await else { return Ok(Value::Null) };
        if !truthy(&response) {
            return Ok(Value::Null);
        }
        let is_known = |id: &str| self.shard_id_index.contains_key(id);
        let mut out = spread(&response);
        let matches: Vec<Value> = response
            .get("matches")
            .and_then(Value::as_array)
            .map(|items| items.iter().map(|item| Value::Object(remap_suggest_routing_item(&spread(item), &self.suggest_routing_ids, &is_known))).collect())
            .unwrap_or_default();
        out.insert("matches".into(), Value::Array(matches));
        Ok(Value::Object(out))
    }

    /// `hydrateRows(rows, context)`: `context.shard` names the owning shard.
    pub async fn hydrate_rows(&self, rows: &[Row], context: Value) -> Result<Vec<Value>> {
        let id = string_or_empty(context.get("shard").unwrap_or(&NULL));
        let Some(&index) = self.shard_id_index.get(&id) else {
            return Err(Error::new(format!("Rangefind: hydrateRows on a sharded index requires context.shard naming one shard (got \"{id}\").")));
        };
        let engine = self.engine_at(index).await?;
        let mut rest = spread(&context);
        rest.shift_remove("shard");
        let docs = Box::pin(engine.hydrate_rows(rows, Value::Object(rest))).await?;
        Ok(docs.iter().map(|doc| self.stamp_shard(doc, index)).collect())
    }

    /// `prefetchShards(names, { docValues })`: fire-and-forget warm-up of
    /// shard engines; failures clear the slot so a real access retries.
    fn prefetch_shards(self: &Rc<Self>, params: &Value) {
        // `prefetchShards(names, { docValues })` as `{ names, docValues }`
        // (or the bare names).
        let names = match params {
            Value::Object(_) => params.get("names").or_else(|| params.get("shards")).cloned().unwrap_or(Value::Null),
            other => other.clone(),
        };
        let doc_values = truthy(path(params, &["docValues"])) || truthy(path(params, &["options", "docValues"]));
        // A warm-up is a task on the engine's runtime. An engine opened with
        // `Engine::open` owns a private runtime nothing polls, so spawning
        // there would only park tasks (each holding this engine) forever;
        // prefetching is an optimisation, so it is skipped instead.
        if !self.runtime.is_driven() {
            return;
        }
        let Ok(picked) = self.candidate_shards(&json!({ "shards": names })) else { return };
        for index in picked {
            let this = self.clone();
            let slot = self.engines[index].borrow().clone();
            self.runtime.spawn(async move {
                match this.engine_at(index).await {
                    Ok(engine) => {
                        if doc_values {
                            if let Some(single) = engine.single() {
                                let _ = single.ensure_doc_values_manifest().await;
                            }
                        }
                    }
                    Err(_) => {
                        let mut current = this.engines[index].borrow_mut();
                        if Rc::ptr_eq(&current, &slot) {
                            *current = Rc::new(PromiseSlot::default());
                        }
                    }
                }
            });
        }
    }
}

/// `setSuggestionShardDocs(entry, item, shardId)`.
fn set_shard_docs(entry: &mut Map<String, Value>, item: &Value, shard_id: &str) {
    for key in ["doc", "docs", "docShard", "generation"] {
        entry.shift_remove(key);
    }
    let Some(doc) = present(item.get("doc")) else { return };
    entry.insert("doc".into(), doc.clone());
    if let Some(Value::Array(docs)) = item.get("docs") {
        if docs.len() > 1 {
            entry.insert("docs".into(), Value::Array(docs.clone()));
        }
    }
    entry.insert("docShard".into(), Value::String(shard_id.to_string()));
    if let Some(generation) = present(item.get("generation")) {
        entry.insert("generation".into(), generation.clone());
    }
}
