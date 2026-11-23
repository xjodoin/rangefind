//! Result hydration: dense doc-id pointer table + retrieval-local doc
//! packs, or columnar doc pages, chosen per request (`loadDocs`,
//! `docPagePlan`, `rowsToResults` in `src/runtime.js`).

use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::codec::doc_pages::{self, DocPagePointerMeta};
use crate::codec::doc_pointers::{self, DocPointerMeta};
use crate::error::{Error, Result};
use crate::pointer::Pointer;
use crate::util::json::{num, path, to_number, truthy};

use super::index::IndexEngine;
use super::io::inflate_object;

/// How a caller wants payloads loaded (`preferDocPages`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreferDocPages {
    Unset,
    Auto,
    Yes,
    Force,
    No,
}

/// `resultContext`: hints in, payload-lane stats out.
#[derive(Clone, Debug)]
pub struct ResultContext {
    pub has_text_terms: bool,
    pub prefer_doc_pages: PreferDocPages,
    pub lane: String,
    pub pages: usize,
    pub rows: usize,
    pub overfetch_docs: usize,
    pub adaptive: bool,
    pub forced: bool,
}

impl ResultContext {
    pub fn new(has_text_terms: bool, prefer_doc_pages: PreferDocPages) -> Self {
        ResultContext {
            has_text_terms,
            prefer_doc_pages,
            lane: String::new(),
            pages: 0,
            rows: 0,
            overfetch_docs: 0,
            adaptive: false,
            forced: false,
        }
    }

    pub fn from_json(value: &Value) -> Self {
        let prefer = match value.get("preferDocPages") {
            Some(Value::String(text)) if text == "force" => PreferDocPages::Force,
            Some(Value::String(text)) if text == "auto" => PreferDocPages::Auto,
            Some(Value::Bool(true)) => PreferDocPages::Yes,
            Some(Value::Bool(false)) => PreferDocPages::No,
            _ => PreferDocPages::Unset,
        };
        ResultContext::new(truthy(value.get("hasTextTerms").unwrap_or(&Value::Null)), prefer)
    }

    /// Stats every hydrating lane reports.
    pub fn stats(&self, out: &mut Map<String, Value>, full: bool) {
        if self.lane.is_empty() {
            out.insert("docPayloadLane".into(), Value::Null);
        } else {
            out.insert("docPayloadLane".into(), Value::from(self.lane.clone()));
        }
        out.insert("docPayloadPages".into(), Value::from(self.pages));
        out.insert("docPayloadOverfetchDocs".into(), Value::from(self.overfetch_docs));
        if full {
            out.insert("docPayloadAdaptive".into(), Value::from(self.adaptive));
            out.insert("docPayloadForced".into(), Value::from(self.forced));
        }
    }
}

pub struct DocPagePlan {
    pub pages: Vec<usize>,
    pub page_size: usize,
    pub payload_docs: usize,
    pub adaptive: bool,
    pub forced: bool,
}

/// A ranked row: document id and score.
pub type Row = (i32, f64);

impl IndexEngine {
    fn doc_pages_meta(&self) -> Value {
        self.manifest_at(&["docs", "pages"])
    }

    fn doc_page_size(&self) -> usize {
        let size = to_number(&self.manifest_at(&["docs", "pages", "page_size"]));
        if size.is_finite() && size >= 1.0 {
            size as usize
        } else {
            1
        }
    }

    /// `docPagePlan`.
    pub fn doc_page_plan(&self, indexes: &[i32], context: &ResultContext) -> Option<DocPagePlan> {
        let hard_forced = context.prefer_doc_pages == PreferDocPages::Force;
        let forced = hard_forced || context.prefer_doc_pages == PreferDocPages::Yes;
        let adaptive = self.options.text_doc_page_hydration
            && (context.prefer_doc_pages == PreferDocPages::Auto || (context.prefer_doc_pages == PreferDocPages::Unset && context.has_text_terms));
        let meta = self.doc_pages_meta();
        if path(&meta, &["pointers", "file"]).as_str().map(str::is_empty).unwrap_or(true) || (!forced && !adaptive) {
            return None;
        }
        let mut unique: Vec<i32> = Vec::new();
        for index in indexes {
            if !unique.contains(index) {
                unique.push(*index);
            }
        }
        if unique.is_empty() {
            return None;
        }
        let page_size = self.doc_page_size();
        let mut pages: Vec<usize> = unique.iter().map(|index| (*index).max(0) as usize / page_size).collect();
        pages.sort_unstable();
        pages.dedup();
        let payload_docs = pages.len() * page_size;
        let configured = {
            let value = to_number(meta.get("max_overfetch_docs").unwrap_or(&Value::Null));
            if truthy(meta.get("max_overfetch_docs").unwrap_or(&Value::Null)) { value } else { 16.0 }.max(1.0)
        };
        let max_overfetch = if forced { configured } else { self.options.text_doc_page_max_overfetch_docs.unwrap_or(configured).max(1.0) };
        let max_payload_docs = (page_size as f64).max(unique.len() as f64 * max_overfetch);
        if !hard_forced && payload_docs as f64 > max_payload_docs {
            return None;
        }
        if !forced && pages.len() * 2 >= unique.len() * 3 {
            return None;
        }
        Some(DocPagePlan { pages, page_size, payload_docs, adaptive: !forced, forced: hard_forced })
    }

    async fn load_doc_pointers(&self, indexes: &[usize]) -> Result<()> {
        let meta_value = self.manifest_at(&["docs", "pointers"]);
        let Some(file) = meta_value.get("file").and_then(Value::as_str).map(str::to_string) else {
            return Err(Error::new("Rangefind index is missing dense doc pointers."));
        };
        let order = meta_value.get("order").and_then(Value::as_str).unwrap_or("doc-id");
        if order != "doc-id" {
            return Err(Error::new(format!("Unsupported Rangefind doc pointer order {order}")));
        }
        let meta = DocPointerMeta::from_json(&meta_value);
        let pack_table: Vec<String> = string_table(meta_value.get("pack_table"));
        let items = indexes
            .iter()
            .map(|index| (*index, Pointer::new(&file, meta.data_offset as u64 + (*index * meta.record_bytes) as u64, meta.record_bytes as u64)))
            .collect();
        self.load_batch(&self.doc_pointer_cache, items, &self.options.range_plans.doc_pointers, |pack| self.url(pack), |_, _, slice| {
            doc_pointers::decode_doc_pointer_record(&slice, 0, &meta, &pack_table)
        })
        .await
        .map(|_| ())
    }

    async fn load_packed_docs(&self, indexes: &[i32]) -> Result<Vec<Value>> {
        let mut unique: Vec<usize> = Vec::new();
        for index in indexes {
            let index = (*index).max(0) as usize;
            if !unique.contains(&index) {
                unique.push(index);
            }
        }
        self.load_doc_pointers(&unique).await?;
        let mut items = Vec::new();
        for index in &unique {
            if self.packed_doc_cache.has(index) {
                continue;
            }
            let pointer = self.doc_pointer_cache.get(index).ok_or_else(|| Error::new("doc pointer missing"))?.await?;
            items.push((*index, pointer));
        }
        let verify = self.verify_checksums;
        self.load_batch(&self.packed_doc_cache, items, &self.options.range_plans.docs, |pack| self.url(&format!("docs/packs/{pack}")), |index, pointer, slice| {
            let inflated = inflate_object(&slice, pointer, &format!("doc {index}"), verify)?;
            Ok(serde_json::from_slice::<Value>(&inflated)?)
        })
        .await?;
        let mut out = Vec::with_capacity(indexes.len());
        for index in indexes {
            let key = (*index).max(0) as usize;
            out.push(self.packed_doc_cache.get(&key).ok_or_else(|| Error::new("doc missing"))?.await?);
        }
        Ok(out)
    }

    async fn load_doc_pages(&self, indexes: &[i32], plan: &DocPagePlan) -> Result<Vec<Value>> {
        let meta_value = self.doc_pages_meta();
        let pointer_meta_value = meta_value.get("pointers").cloned().unwrap_or(Value::Null);
        let Some(file) = pointer_meta_value.get("file").and_then(Value::as_str).map(str::to_string) else {
            return Err(Error::new("Rangefind index is missing dense doc page pointers."));
        };
        let pointer_meta = DocPagePointerMeta::from_json(&pointer_meta_value);
        let pack_table = string_table(pointer_meta_value.get("pack_table"));
        let mut items = Vec::new();
        for page in &plan.pages {
            if *page >= pointer_meta.count {
                return Err(Error::new(format!("Rangefind doc page {page} is outside the index.")));
            }
            items.push((*page, Pointer::new(&file, pointer_meta.data_offset as u64 + (*page * pointer_meta.record_bytes) as u64, pointer_meta.record_bytes as u64)));
        }
        self.load_batch(&self.doc_page_pointer_cache, items, &self.options.range_plans.doc_page_pointers, |pack| self.url(pack), |_, _, slice| {
            doc_pages::decode_doc_page_pointer_record(&slice, 0, &pointer_meta, &pack_table)
        })
        .await?;
        let encoding = meta_value.get("encoding").and_then(Value::as_str).unwrap_or("");
        if encoding != doc_pages::DOC_PAGE_ENCODING {
            return Err(Error::new(format!(
                "Unsupported Rangefind doc page encoding {}.",
                if encoding.is_empty() { "unknown" } else { encoding }
            )));
        }
        let fields: Vec<Value> = meta_value.get("fields").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut page_items = Vec::new();
        for page in &plan.pages {
            if self.doc_page_cache.has(page) {
                continue;
            }
            let pointer = self.doc_page_pointer_cache.get(page).ok_or_else(|| Error::new("doc page pointer missing"))?.await?;
            page_items.push((*page, pointer));
        }
        let verify = self.verify_checksums;
        let page_size = plan.page_size;
        self.load_batch(
            &self.doc_page_cache,
            page_items,
            &self.options.range_plans.doc_pages,
            |pack| self.url(&format!("docs/page-packs/{pack}")),
            |page, pointer, slice| {
                let inflated = inflate_object(&slice, pointer, &format!("doc page {page}"), verify)?;
                Ok(Rc::new(doc_pages::decode_doc_page_columns(&inflated, &fields, page * page_size)?))
            },
        )
        .await?;
        let mut out = Vec::with_capacity(indexes.len());
        for index in indexes {
            let index = (*index).max(0) as usize;
            let page_index = index / plan.page_size;
            let page = self.doc_page_cache.get(&page_index).ok_or_else(|| Error::new("doc page missing"))?.await?;
            match page.get(index - page_index * plan.page_size) {
                Some(doc) if !doc.is_null() => out.push(doc.clone()),
                _ => return Err(Error::new(format!("Rangefind doc page {page_index} is missing document {index}."))),
            }
        }
        Ok(out)
    }

    /// `loadDocs`.
    pub async fn load_docs(&self, indexes: &[i32], context: &mut ResultContext) -> Result<Vec<Value>> {
        let plan = self.doc_page_plan(indexes, context);
        context.lane = if plan.is_some() { "docPages" } else { "packedDocs" }.to_string();
        context.pages = plan.as_ref().map(|p| p.pages.len()).unwrap_or(0);
        context.rows = indexes.len();
        context.overfetch_docs = plan.as_ref().map(|p| p.payload_docs).unwrap_or(indexes.len());
        context.adaptive = plan.as_ref().map(|p| p.adaptive).unwrap_or(false);
        context.forced = plan.as_ref().map(|p| p.forced).unwrap_or(false);
        match plan {
            Some(plan) => self.load_doc_pages(indexes, &plan).await,
            None => self.load_packed_docs(indexes).await,
        }
    }

    /// `rowsToResults`: `{ ...doc, score, index }`.
    pub async fn rows_to_results(&self, rows: &[Row], context: &mut ResultContext) -> Result<Vec<Value>> {
        let indexes: Vec<i32> = rows.iter().map(|(doc, _)| *doc).collect();
        let docs = self.load_docs(&indexes, context).await?;
        Ok(docs.into_iter().zip(rows).map(|(doc, (index, score))| result_object(doc, *score, *index)).collect())
    }

    /// `rowsToSearchResults`.
    pub async fn rows_to_search_results(&self, rows: &[Row], context: &mut ResultContext, include_results: bool) -> Result<Vec<Value>> {
        if !include_results {
            context.lane = "skipped".into();
            context.pages = 0;
            context.rows = rows.len();
            context.overfetch_docs = 0;
            context.adaptive = false;
            context.forced = false;
            return Ok(rows.iter().map(|(index, score)| json!({ "index": index, "score": num(*score) })).collect());
        }
        self.rows_to_results(rows, context).await
    }
}

pub fn result_object(doc: Value, score: f64, index: i32) -> Value {
    let mut map = match doc {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    map.insert("score".into(), num(score));
    map.insert("index".into(), Value::from(index));
    Value::Object(map)
}

pub fn string_table(value: Option<&Value>) -> Vec<String> {
    value
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
        .unwrap_or_default()
}
