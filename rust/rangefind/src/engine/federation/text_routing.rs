//! `createTextRoutingState` from `createShardedSearch` (`src/runtime.js`):
//! the root-level term directory (`rftextroute-v1`) mapping every analyzed
//! term to the shards that contain it.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::rc::Rc;

use serde_json::{json, Value};

use crate::analysis::{analyzer_from_manifest, Analyzer};
use crate::codec::directory::{parse_directory_page, parse_directory_root, DirectoryPage, DirectoryRoot};
use crate::codec::text_routing::{floor_directory_page_index, parse_text_routing_segment, TextRoutingSegment};
use crate::codec::JsKeyOrd;
use crate::error::Result;
use crate::rt::{try_join_all, PromiseCache, PromiseSlot};
use crate::transport::resolve_url;
use crate::util::json::{path, truthy};
use crate::util::jsstr::cmp16;

use super::super::index::min_should_match_for;
use super::super::io::{inflate, Io};
use super::js_string;

pub const TEXT_ROUTING_PREFIX_MIN_LENGTH: usize = 3;
pub const TEXT_ROUTING_PREFIX_SEGMENT_LIMIT: usize = 8;

pub struct TextRoutingState {
    block: Value,
    base_url: String,
    io: Rc<Io>,
    shard_index_by_ordinal: Vec<i64>,
    always_selected: Vec<usize>,
    pack_table: Vec<String>,
    analyzer: RefCell<Option<Rc<Analyzer>>>,
    root: PromiseSlot<Rc<DirectoryRoot>>,
    pages: PromiseCache<String, Rc<DirectoryPage>>,
    segments: PromiseCache<(String, u64), Rc<TextRoutingSegment>>,
}

/// `selectShards(q)` result.
pub struct TextSelection {
    pub selected: HashSet<usize>,
    pub terms: usize,
}

/// `selectSuggestShards(q)` result.
pub enum SuggestSelection {
    Fallback { prefixes: usize, segments: usize },
    Selected { selected: HashSet<usize>, matched: usize, prefixes: usize, segments: usize },
}

struct PrefixScan {
    indexes: Vec<usize>,
    complete: bool,
    segments: usize,
}

impl TextRoutingState {
    /// `shard_index_of(id)` is `shardIdIndex.get(id)`.
    pub fn new(block: &Value, base_url: &str, io: Rc<Io>, shard_ids: &[String], shard_index_of: &dyn Fn(&str) -> Option<usize>) -> Self {
        let ordinal_ids: Vec<String> = block.get("shard_ids").and_then(Value::as_array).map(|ids| ids.iter().map(js_string).collect()).unwrap_or_default();
        let routed: HashSet<&str> = ordinal_ids.iter().map(String::as_str).collect();
        let shard_index_by_ordinal = ordinal_ids.iter().map(|id| shard_index_of(id).map(|index| index as i64).unwrap_or(-1)).collect();
        let always_selected = shard_ids.iter().enumerate().filter(|(_, id)| !routed.contains(id.as_str())).map(|(index, _)| index).collect();
        let pack_table = path(block, &["directory", "pack_table"])
            .as_array()
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
        let budget = io.cache_budget.clone();
        TextRoutingState {
            block: block.clone(),
            base_url: base_url.to_string(),
            io,
            shard_index_by_ordinal,
            always_selected,
            pack_table,
            analyzer: RefCell::new(None),
            root: PromiseSlot::default(),
            pages: PromiseCache::budgeted(&budget, "textRoutingPages"),
            segments: PromiseCache::budgeted(&budget, "textRoutingSegments"),
        }
    }

    fn routing_analyzer(&self) -> Result<Rc<Analyzer>> {
        if let Some(analyzer) = self.analyzer.borrow().clone() {
            return Ok(analyzer);
        }
        let analysis = self.block.get("analysis").filter(|value| truthy(value)).cloned().unwrap_or(Value::Null);
        let analyzer = Rc::new(analyzer_from_manifest(&json!({ "analysis": analysis }))?);
        *self.analyzer.borrow_mut() = Some(analyzer.clone());
        Ok(analyzer)
    }

    async fn load_root(&self) -> Result<Rc<DirectoryRoot>> {
        self.root
            .get_or_load(
                async {
                    let root = path(&self.block, &["directory", "root"]);
                    let bytes = self.io.fetch_gzip(&resolve_url(&self.base_url, &js_string(root))).await?;
                    Ok(Rc::new(parse_directory_root(&bytes)?))
                },
                false,
            )
            .await
    }

    async fn load_page(&self, file: &str) -> Result<Rc<DirectoryPage>> {
        self.pages
            .get_or_load(file.to_string(), async {
                let pages = path(&self.block, &["directory", "pages"]);
                let mut base = if truthy(pages) { js_string(pages) } else { "text-routing/directory-pages/".to_string() };
                if !base.ends_with('/') {
                    base.push('/');
                }
                let bytes = self.io.fetch_gzip(&resolve_url(&self.base_url, &format!("{base}{file}"))).await?;
                Ok(Rc::new(parse_directory_page(&bytes, &self.pack_table)?))
            })
            .await
    }

    async fn load_segment(&self, entry: &crate::pointer::Pointer) -> Result<Rc<TextRoutingSegment>> {
        let key = (entry.pack.to_string(), entry.offset);
        self.segments
            .get_or_load(key, async {
                let url = resolve_url(&self.base_url, &format!("text-routing/packs/{}", entry.pack));
                let compressed = self.io.fetch_range(&url, entry.offset, entry.length).await?;
                let bytes = inflate(&compressed)?;
                Ok(Rc::new(parse_text_routing_segment(&bytes)?))
            })
            .await
    }

    /// `shardIndexesForTerm(term)`: root shard indexes containing `term`.
    async fn shard_indexes_for_term(&self, term: &str) -> Result<Vec<usize>> {
        let root = self.load_root().await?;
        let page_index = floor_directory_page_index(&root, term);
        if page_index < 0 {
            return Ok(Vec::new());
        }
        let page = self.load_page(&root.pages[page_index as usize].file).await?;
        let key_index = page.floor_key_index(term);
        if key_index < 0 {
            return Ok(Vec::new());
        }
        let key = &page.keys[key_index as usize];
        let Some(entry) = page.get(&key.text).cloned() else { return Ok(Vec::new()) };
        let segment = self.load_segment(&entry).await?;
        let Some(ordinals) = segment.get(term) else { return Ok(Vec::new()) };
        Ok(ordinals
            .iter()
            .filter_map(|ordinal| self.shard_index_by_ordinal.get(*ordinal as usize).copied().filter(|index| *index >= 0).map(|index| index as usize))
            .collect())
    }

    /// `shardIndexesForPrefix(prefix)`: bounded forward walk over the
    /// sorted term range owning `prefix`.
    async fn shard_indexes_for_prefix(&self, prefix: &str) -> Result<PrefixScan> {
        let root = self.load_root().await?;
        if root.pages.is_empty() {
            return Ok(PrefixScan { indexes: Vec::new(), complete: true, segments: 0 });
        }
        let mut page_index = floor_directory_page_index(&root, prefix).max(0) as usize;
        let upper = format!("{prefix}\u{ffff}");
        let mut selected: Vec<usize> = Vec::new();
        let add = |selected: &mut Vec<usize>, index: usize| {
            if !selected.contains(&index) {
                selected.push(index);
            }
        };
        let mut segments = 0usize;
        while page_index < root.pages.len() {
            let summary = &root.pages[page_index];
            if cmp16(&summary.first, &upper) == Ordering::Greater {
                break;
            }
            let page = self.load_page(&summary.file).await?;
            let mut key_index = page.floor_key_index(prefix).max(0) as usize;
            while key_index < page.keys.len() {
                let key = &page.keys[key_index];
                if key.cmp_js(&upper) == Ordering::Greater {
                    return Ok(PrefixScan { indexes: selected, complete: true, segments });
                }
                if segments >= TEXT_ROUTING_PREFIX_SEGMENT_LIMIT {
                    return Ok(PrefixScan { indexes: Vec::new(), complete: false, segments });
                }
                segments += 1;
                let entry = page.entries[key_index].1.clone();
                let segment = self.load_segment(&entry).await?;
                for (term, ordinals) in &segment.terms {
                    if term.cmp_js(prefix) == Ordering::Less {
                        continue;
                    }
                    if term.cmp_js(&upper) == Ordering::Greater {
                        return Ok(PrefixScan { indexes: selected, complete: true, segments });
                    }
                    if term.units.is_some() || !term.text.starts_with(prefix) {
                        continue;
                    }
                    for ordinal in ordinals {
                        if let Some(index) = self.shard_index_by_ordinal.get(*ordinal as usize).copied().filter(|index| *index >= 0) {
                            add(&mut selected, index as usize);
                        }
                    }
                }
                key_index += 1;
            }
            page_index += 1;
        }
        Ok(PrefixScan { indexes: selected, complete: true, segments })
    }

    /// `selectShards(q)`: a shard qualifies when it supports
    /// `minShouldMatch` of any query plan's base terms.
    pub async fn select_shards(&self, q: &str) -> Result<Option<TextSelection>> {
        let plan = self.routing_analyzer()?.query_plan(q);
        let mut plans: Vec<&[String]> = vec![&plan.base_terms];
        for alt in &plan.alt_plans {
            plans.push(&alt.base_terms);
        }
        let mut unique_terms: Vec<&String> = Vec::new();
        for terms in &plans {
            for term in terms.iter() {
                if !unique_terms.contains(&term) {
                    unique_terms.push(term);
                }
            }
        }
        if unique_terms.is_empty() {
            return Ok(None);
        }
        let by_term = try_join_all(unique_terms.iter().map(|term| self.shard_indexes_for_term(term))).await?;
        let mut selected: HashSet<usize> = self.always_selected.iter().copied().collect();
        for terms in &plans {
            let mut plan_terms: Vec<&String> = Vec::new();
            for term in terms.iter() {
                if !plan_terms.contains(&term) {
                    plan_terms.push(term);
                }
            }
            if plan_terms.is_empty() {
                continue;
            }
            let need = plan_terms.len().min(min_should_match_for(terms.len()).max(1));
            let mut support: Vec<(usize, usize)> = Vec::new();
            for term in &plan_terms {
                let slot = unique_terms.iter().position(|item| item == term).unwrap();
                for index in &by_term[slot] {
                    match support.iter_mut().find(|(i, _)| i == index) {
                        Some((_, count)) => *count += 1,
                        None => support.push((*index, 1)),
                    }
                }
            }
            for (index, count) in support {
                if count >= need {
                    selected.insert(index);
                }
            }
        }
        Ok(Some(TextSelection { selected, terms: unique_terms.len() }))
    }

    /// `selectSuggestShards(q)`: prefix routing for autocomplete.
    pub async fn select_suggest_shards(&self, q: &str) -> Result<Option<SuggestSelection>> {
        if self.block.get("suggest_prefix") != Some(&Value::Bool(true)) {
            return Ok(None);
        }
        let plan = self.routing_analyzer()?.query_plan(q);
        let mut prefixes: Vec<String> = Vec::new();
        let last = |terms: &[String]| terms.last().cloned().unwrap_or_default();
        let mut candidates = vec![last(&plan.base_terms)];
        for alt in &plan.alt_plans {
            candidates.push(last(&alt.base_terms));
        }
        for prefix in candidates {
            if prefix.chars().count() >= TEXT_ROUTING_PREFIX_MIN_LENGTH && !prefixes.contains(&prefix) {
                prefixes.push(prefix);
            }
        }
        if prefixes.is_empty() {
            return Ok(None);
        }
        let scans = try_join_all(prefixes.iter().map(|prefix| self.shard_indexes_for_prefix(prefix))).await?;
        let segments = scans.iter().map(|scan| scan.segments).sum();
        if scans.iter().any(|scan| !scan.complete) {
            return Ok(Some(SuggestSelection::Fallback { prefixes: prefixes.len(), segments }));
        }
        let mut matched: HashSet<usize> = HashSet::new();
        for scan in &scans {
            matched.extend(scan.indexes.iter().copied());
        }
        let mut selected: HashSet<usize> = self.always_selected.iter().copied().collect();
        selected.extend(matched.iter().copied());
        Ok(Some(SuggestSelection::Selected { selected, matched: matched.len(), prefixes: prefixes.len(), segments }))
    }
}
