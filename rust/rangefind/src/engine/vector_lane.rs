//! Vector and hybrid search: `vectorFieldMeta`, `loadVectorRoot`,
//! `vectorTopK`, `executeVectorSearch`, `executeHybridSearch` and the
//! `vectorSearch` wrapper in `src/runtime.js`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::highlight::{apply_highlights, highlight_term_set, HighlightOptions};
use crate::pointer::{group_ranges, Pointer};
use crate::rt::{join2, PromiseCache};
use crate::util::json::{num, num_or, string_or_empty, to_number, truthy, NULL};
use crate::vector::{
    apply_permutation, decode_vector_cluster_page, dot_int8, dot_int8_bytes, normalized_query_vector, parse_vector_root, VectorClusterPage,
    VectorRoot,
};

use super::docs::{PreferDocPages, ResultContext, Row};
use super::filters::Filters;
use super::index::IndexEngine;
use super::io::inflate_object;

const VECTOR_STATE_KEY: &str = "vector";

/// `vectorRootCache` + `vectorClusterPageCache`.
struct VectorState {
    roots: PromiseCache<String, Rc<VectorRoot>>,
    pages: PromiseCache<(String, usize), Rc<VectorClusterPage>>,
}

/// The parts of a `manifest.vectors.fields[name]` entry the runtime reads.
struct VectorFieldMeta {
    name: String,
    directory_file: String,
    pack_dir: String,
    refine_pack_dir: String,
}

/// `vectorTopK`'s return value.
struct VectorTopK {
    rows: Vec<Row>,
    stats: Map<String, Value>,
}

/// `Math.max` / `Math.min`: NaN in, NaN out.
fn js_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}

fn js_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.min(b)
    }
}

/// The element count `array.slice(0, end)` keeps for a non-negative `end`.
fn slice_end(end: f64, len: usize) -> usize {
    if end.is_nan() || end <= 0.0 {
        0
    } else if end >= len as f64 {
        len
    } else {
        end as usize
    }
}

fn str_field(value: &Value, key: &str) -> String {
    value.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

impl IndexEngine {
    fn vector_state(&self) -> Rc<VectorState> {
        if let Some(state) = self.extension::<VectorState>(VECTOR_STATE_KEY) {
            return state;
        }
        let budget = self.cache_budget();
        let state = Rc::new(VectorState {
            roots: PromiseCache::budgeted(budget, "vectorRoots"),
            pages: PromiseCache::budgeted(budget, "vectorClusterPages"),
        });
        self.set_extension(VECTOR_STATE_KEY, state.clone());
        state
    }

    /// `vectorFieldMeta(name)`.
    fn vector_field_meta(&self, name: &Value) -> Result<VectorFieldMeta> {
        let fields = self.manifest_at(&["vectors", "fields"]);
        let field_map = fields.as_object().cloned().unwrap_or_default();
        if field_map.is_empty() {
            return Err(Error::new("Rangefind: this index has no vector fields (configure `vectors` at build time)."));
        }
        let requested = string_or_empty(name);
        let field = if truthy(name) {
            requested.clone()
        } else if field_map.len() == 1 {
            field_map.keys().next().cloned().unwrap_or_default()
        } else {
            String::new()
        };
        let meta = match field_map.get(&field) {
            Some(meta) if truthy(meta) => meta,
            _ => return Err(Error::new(format!("Rangefind: unknown vector field \"{requested}\"."))),
        };
        Ok(VectorFieldMeta {
            name: string_or_empty(meta.get("name").unwrap_or(&NULL)),
            directory_file: meta.get("directory").map(|d| str_field(d, "file")).unwrap_or_default(),
            pack_dir: string_or_empty(meta.get("pack_dir").unwrap_or(&NULL)),
            refine_pack_dir: string_or_empty(meta.get("refine_pack_dir").unwrap_or(&NULL)),
        })
    }

    /// `loadVectorRoot(meta)`: cached per field; failures are evicted.
    async fn load_vector_root(&self, meta: &VectorFieldMeta) -> Result<Rc<VectorRoot>> {
        let state = self.vector_state();
        let url = self.url(&meta.directory_file);
        state
            .roots
            .get_or_load(meta.name.clone(), async {
                let bytes = self.io.fetch_gzip(&url).await?;
                Ok(Rc::new(parse_vector_root(&bytes)?))
            })
            .await
    }

    /// `vectorTopK(params)`: IVF traversal over int8 centroids, coarse
    /// cluster-page scoring, then full-dimension refine-row reranking.
    async fn vector_top_k(&self, params: &Value) -> Result<VectorTopK> {
        let meta = self.vector_field_meta(params.get("field").unwrap_or(&NULL))?;
        let root = self.load_vector_root(&meta).await?;
        let dims = root.dims;
        let normalized = normalized_query_vector(params.get("vector").unwrap_or(&NULL), dims)?;
        let mut query = vec![0f32; dims];
        apply_permutation(&normalized, 0, &root.permutation, &mut query);

        let get = |key: &str| params.get(key).unwrap_or(&NULL);
        let k = js_max(1.0, js_min(200.0, num_or(get("k"), 10.0).floor()));
        let nprobe = js_max(1.0, js_min(root.cluster_count as f64, num_or(get("nprobe"), 8.0).floor()));
        let refine_factor = js_max(1.0, js_min(64.0, num_or(get("refineFactor"), 8.0).floor()));
        let refine = get("refine") != &Value::Bool(false);

        let mut centroid_order: Vec<(usize, f64)> = (0..root.cluster_count)
            .map(|c| (c, dot_int8(&query, &root.centroid_codes, c * dims, dims, root.centroid_scales[c] as f64)))
            .collect();
        centroid_order.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        let probe_count = slice_end(nprobe, centroid_order.len());

        // loadPackedPages: unique cluster indexes in probe order.
        let state = self.vector_state();
        let mut wanted: Vec<usize> = Vec::new();
        let mut items: Vec<((String, usize), Pointer)> = Vec::new();
        for &(c, _) in &centroid_order[..probe_count] {
            let Some(cluster) = root.clusters.get(c) else { continue };
            if wanted.contains(&cluster.index) {
                continue;
            }
            wanted.push(cluster.index);
            items.push(((meta.name.clone(), cluster.index), cluster.pointer.clone()));
        }
        let verify = self.verify_checksums;
        let pack_dir = meta.pack_dir.clone();
        let field_name = meta.name.clone();
        let page_stats = self
            .load_batch(
                &state.pages,
                items,
                &self.options.range_plans.vector_cluster_pages,
                |pack| self.url(&format!("{pack_dir}/{pack}")),
                |key, pointer, slice| {
                    let label = format!("vector cluster page {}:{}", key.0, key.1);
                    let inflated = inflate_object(&slice, pointer, &label, verify)?;
                    Ok(Rc::new(decode_vector_cluster_page(&inflated, Some(&field_name))?))
                },
            )
            .await?;
        let mut pages = Vec::with_capacity(wanted.len());
        for index in &wanted {
            let key = (meta.name.clone(), *index);
            let promise = state.pages.get(&key).ok_or_else(|| Error::new("vector cluster page missing"))?;
            pages.push(promise.await?);
        }

        let mut candidates: Vec<(i32, u32, f64)> = Vec::new();
        let mut scanned = 0usize;
        for page in &pages {
            for i in 0..page.count {
                scanned += 1;
                candidates.push((
                    page.docs[i] as i32,
                    page.ordinals[i],
                    dot_int8(&query, &page.codes, i * page.coarse_dims, page.coarse_dims, page.scales[i] as f64),
                ));
            }
        }
        candidates.sort_by(|a, b| match b.2.partial_cmp(&a.2) {
            Some(std::cmp::Ordering::Equal) | None => a.0.cmp(&b.0),
            Some(order) => order,
        });
        let shortlist_end = js_max(k, js_min(candidates.len() as f64, k * refine_factor));
        candidates.truncate(slice_end(shortlist_end, candidates.len()));
        let shortlist = candidates;
        let k_count = slice_end(k, shortlist.len());

        let mut refine_groups = 0usize;
        let rows: Vec<Row> = if !refine || shortlist.is_empty() {
            shortlist.iter().take(k_count).map(|&(doc, _, score)| (doc, score)).collect()
        } else {
            let layout = &root.refine;
            let items: Vec<(Pointer, i32)> = shortlist
                .iter()
                .map(|&(doc, ordinal, _)| {
                    let ordinal = ordinal as u64;
                    let (pack, offset) = if layout.rows_per_pack == 0 {
                        ("", 0)
                    } else {
                        let pack = layout.packs.get((ordinal / layout.rows_per_pack) as usize).map(String::as_str).unwrap_or("");
                        (pack, (ordinal % layout.rows_per_pack) * layout.row_bytes)
                    };
                    (Pointer::new(pack, offset, layout.row_bytes), doc)
                })
                .collect();
            let groups = group_ranges(items, &self.options.range_plans.vector_refine);
            refine_groups = groups.len();
            let scored: RefCell<Vec<Row>> = RefCell::new(Vec::new());
            let refine_dir = meta.refine_pack_dir.clone();
            self.io
                .read_range_groups(groups, |group| self.url(&format!("{refine_dir}/{}", group.pack)), |group, buffer| {
                    let bytes: &[u8] = &buffer;
                    for (pointer, doc) in &group.items {
                        let at = (pointer.offset - group.start) as usize;
                        let scale_bytes = bytes.get(at..at + 4).ok_or_else(|| Error::new("Offset is outside the bounds of the DataView"))?;
                        let scale = f32::from_le_bytes([scale_bytes[0], scale_bytes[1], scale_bytes[2], scale_bytes[3]]);
                        scored.borrow_mut().push((*doc, dot_int8_bytes(&query, bytes, at + 4, dims, scale as f64)));
                    }
                    Ok(())
                })
                .await?;
            let mut scored = scored.into_inner();
            scored.sort_by(|a, b| match b.1.partial_cmp(&a.1) {
                Some(std::cmp::Ordering::Equal) | None => a.0.cmp(&b.0),
                Some(order) => order,
            });
            scored.truncate(k_count);
            scored
        };

        let mut stats = Map::new();
        stats.insert("vectorField".into(), json!(meta.name));
        stats.insert("vectorClusters".into(), json!(root.cluster_count));
        stats.insert("vectorClustersProbed".into(), num(nprobe));
        stats.insert("vectorClusterPagesFetched".into(), json!(page_stats.fetched));
        stats.insert("vectorCandidatesScanned".into(), json!(scanned));
        stats.insert("vectorCandidatesRefined".into(), json!(if refine { shortlist.len() } else { 0 }));
        stats.insert("vectorRefineFetchGroups".into(), json!(refine_groups));
        Ok(VectorTopK { rows, stats })
    }

    /// `executeVectorSearch(params)`.
    async fn execute_vector_search(&self, params: &Value) -> Result<Value> {
        let VectorTopK { rows, mut stats } = self.vector_top_k(params).await?;
        let results: Vec<Value> = if params.get("includeResults") == Some(&Value::Bool(false)) {
            rows.iter().map(|(index, score)| json!({ "index": index, "score": num(*score) })).collect()
        } else {
            let mut context = ResultContext::new(false, PreferDocPages::No);
            self.rows_to_results(&rows, &mut context).await?
        };
        stats.insert("exact".into(), json!(false));
        stats.insert("approximate".into(), json!(true));
        Ok(json!({
            "total": rows.len(),
            "results": results,
            "stats": Value::Object(stats)
        }))
    }

    /// `executeHybridSearch(params)`: text + vector retrieval fused with
    /// reciprocal rank fusion; filters apply to both lanes.
    pub(crate) async fn execute_hybrid_search(&self, params: &Value) -> Result<Value> {
        let get = |key: &str| params.get(key).unwrap_or(&NULL);
        let size = super::search::request_size(params.get("size"), self.options.max_page_size);
        let page = super::search::request_page(params.get("page"));
        let pool_size = (size * 3).max(100.min(size * 5));
        let hybrid = get("hybrid");
        let rrf_k = js_max(1.0, num_or(hybrid.get("rrfK").unwrap_or(&NULL), 60.0).floor());
        let q = string_or_empty(get("q")).trim().to_string();
        let filters = Filters::from_json(get("filters"));
        let has_filters = filters.has_filters();

        let mut text_params = params.clone();
        if let Some(map) = text_params.as_object_mut() {
            map.remove("vector");
            map.remove("hybrid");
            map.remove("highlight");
            map.insert("includeResults".into(), json!(false));
            map.insert("size".into(), json!(pool_size));
            map.insert("page".into(), json!(1));
        }
        let mut vector_params = match hybrid {
            Value::Object(map) => map.clone(),
            _ => Map::new(),
        };
        vector_params.insert("field".into(), get("vectorField").clone());
        vector_params.insert("vector".into(), get("vector").clone());
        vector_params.insert("k".into(), json!(pool_size));
        let vector_params = Value::Object(vector_params);

        let text_future = async {
            if q.is_empty() {
                Ok(None)
            } else {
                // Type-erased: executeSearch dispatches back here on `vector`.
                let search: std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + '_>> = Box::pin(self.execute_search(&text_params));
                search.await.map(Some)
            }
        };
        let (text_response, vector_response) = join2(text_future, self.vector_top_k(&vector_params)).await;
        let text_response: Option<Value> = text_response?;
        let VectorTopK { rows: mut vector_rows, stats: vector_stats } = vector_response?;

        if has_filters && !vector_rows.is_empty() {
            self.ensure_facet_dictionaries(&filters.facet_fields()).await?;
            let plan = self.make_doc_filter_plan(&filters)?;
            let docs: Vec<i32> = vector_rows.iter().map(|(doc, _)| *doc).collect();
            let store = self.value_store_for_filter_plan(&plan, &docs, &[]).await?;
            vector_rows.retain(|(doc, _)| self.passes_filter_plan(*doc, store.as_ref(), &plan, None));
        }

        let offset = super::text::page_offset(page, size);
        if q.is_empty() {
            let rows: Vec<Row> = super::text::page_slice(&vector_rows, offset, size);
            let mut context = ResultContext::new(false, PreferDocPages::No);
            let results = self.rows_to_results(&rows, &mut context).await?;
            let mut stats = vector_stats;
            stats.insert("exact".into(), json!(false));
            return Ok(json!({
                "total": vector_rows.len(),
                "results": results,
                "page": crate::util::json::num(page),
                "size": size,
                "approximate": true,
                "stats": Value::Object(stats)
            }));
        }

        // RRF over both ranked lists; insertion order kept for stable ties.
        struct Fused {
            doc: i32,
            score: f64,
            lanes: Map<String, Value>,
        }
        let mut fused: Vec<Fused> = Vec::new();
        let mut positions: HashMap<i32, usize> = HashMap::new();
        let mut add_ranked = |list: Vec<i32>, key: &str| {
            for (rank, doc) in list.into_iter().enumerate() {
                let at = *positions.entry(doc).or_insert_with(|| {
                    fused.push(Fused { doc, score: 0.0, lanes: Map::new() });
                    fused.len() - 1
                });
                let entry = &mut fused[at];
                entry.score += 1.0 / (rrf_k + rank as f64 + 1.0);
                entry.lanes.insert(key.to_string(), json!(rank + 1));
            }
        };
        let text_results: Vec<Value> =
            text_response.as_ref().and_then(|r| r.get("results")).and_then(Value::as_array).cloned().unwrap_or_default();
        add_ranked(text_results.iter().map(|item| to_number(item.get("index").unwrap_or(&NULL)) as i32).collect(), "text");
        add_ranked(vector_rows.iter().map(|(doc, _)| *doc).collect(), "vector");

        let mut ranked: Vec<usize> = (0..fused.len()).collect();
        ranked.sort_by(|&a, &b| match fused[b].score.partial_cmp(&fused[a].score) {
            Some(std::cmp::Ordering::Equal) | None => fused[a].doc.cmp(&fused[b].doc),
            Some(order) => order,
        });
        let rows: Vec<Row> = super::text::page_slice(&ranked, offset, size).iter().map(|&i| (fused[i].doc, fused[i].score)).collect();
        let mut context = ResultContext::new(true, PreferDocPages::No);
        let mut results = self.rows_to_results(&rows, &mut context).await?;
        for result in results.iter_mut() {
            let index = to_number(result.get("index").unwrap_or(&NULL)) as i32;
            if let Some(&at) = positions.get(&index) {
                if let Some(map) = result.as_object_mut() {
                    map.insert("hybrid".into(), Value::Object(fused[at].lanes.clone()));
                }
            }
        }
        let highlight = get("highlight");
        if truthy(highlight) && !results.is_empty() {
            let analyzer = &*self.analyzer;
            let corrected = text_response.as_ref().and_then(|r| r.get("correctedQuery")).and_then(Value::as_str).unwrap_or("");
            let term_set = highlight_term_set(&q, corrected, Some(analyzer));
            let fields = highlight
                .get("fields")
                .and_then(Value::as_array)
                .map(|items| items.iter().filter_map(Value::as_str).map(str::to_string).collect());
            let max_chars = highlight.get("maxChars").filter(|v| !v.is_null()).map(to_number);
            let options = HighlightOptions { fields, max_chars, analyzer: Some(analyzer) };
            apply_highlights(&mut results, &term_set, &options);
        }

        let mut stats = Map::new();
        stats.insert("hybrid".into(), json!(true));
        stats.insert("hybridPool".into(), json!(pool_size));
        stats.insert("hybridTextResults".into(), json!(text_results.len()));
        stats.insert("hybridVectorResults".into(), json!(vector_rows.len()));
        for (key, value) in vector_stats {
            stats.insert(key, value);
        }
        let planner_lane = text_response.as_ref().and_then(|r| r.get("stats")).and_then(|s| s.get("plannerLane")).cloned().unwrap_or(Value::Null);
        stats.insert("textLane".into(), if truthy(&planner_lane) { planner_lane } else { json!("text") });
        Ok(json!({
            "total": ranked.len(),
            "results": results,
            "page": crate::util::json::num(page),
            "size": size,
            "approximate": true,
            "stats": Value::Object(stats)
        }))
    }

    /// `vectorSearch(params)`.
    pub(crate) async fn vector_search(&self, params: Value) -> Result<Value> {
        self.execute_vector_search(&params).await
    }
}
