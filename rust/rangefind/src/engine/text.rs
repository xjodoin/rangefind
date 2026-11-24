//! Text retrieval lanes (`runSkippedSearch`, `runDocRangeUpperBoundSearch`,
//! `runFullSearch`, `runCountSearch`, `collectTextMatchDocs`,
//! `rerankWithDependencies` in `src/runtime.js`).
//!
//! Scores are integer impacts until the dependency rerank adds weighted
//! proximity features, so accumulation stays in `i64` and converts to
//! `f64` exactly once, mirroring JS double arithmetic on integers.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::codec::postings::{self, PostingBlock, TermEntry};
use crate::error::Result;
use crate::terms::proximity_term;

use super::docs::{PreferDocPages, ResultContext, Row};
use super::filters::{block_definitely_passes, block_may_pass, BlockFilterPlan, DocFilterPlan, Filters, ValueStore};
use super::index::{min_should_match_for, IndexEngine, TermHit};
use super::options::{
    DEPENDENCY_SCORE_SCALE, DOC_RANGE_PLANNER_MIN_CANDIDATE_RANGES, EXTERNAL_POSTING_BLOCK_PREFETCH, RERANK_CANDIDATES, SKIP_MAX_TERMS,
};

const INF: i64 = i64::MAX;

/// Inputs shared by the text lanes.
#[derive(Clone)]
pub struct TextRequest {
    pub q: String,
    /// `Math.max(1, Number(params.page || 1))`: a double, so fractional and
    /// huge pages page by double offsets exactly like the JS.
    pub page: f64,
    pub size: usize,
    pub filters: Filters,
    pub sort: Value,
    pub base_terms: Vec<String>,
    pub terms: Vec<String>,
    pub rerank: bool,
    pub include_results: bool,
}

impl TextRequest {
    /// `(page - 1) * size`.
    pub fn offset(&self) -> f64 {
        page_offset(self.page, self.size)
    }

    /// `offset + size`: the rows a lane must rank.
    pub fn k(&self) -> f64 {
        self.offset() + self.size as f64
    }
}

// ---------------------------------------------------------------- JS paging

/// `(page - 1) * size` as the JS double.
pub fn page_offset(page: f64, size: usize) -> f64 {
    (page - 1.0) * size as f64
}

/// `rows.slice(offset, offset + size)` with a double offset (truncated, and
/// clamped to the rows like `Array.prototype.slice`).
pub fn page_slice<T: Clone>(rows: &[T], offset: f64, size: usize) -> Vec<T> {
    js_slice(rows, offset, offset + size as f64)
}

/// `rows.slice(start, end)` with double bounds.
pub fn js_slice<T: Clone>(rows: &[T], start: f64, end: f64) -> Vec<T> {
    let (from, to) = super::federation::slice_bounds(rows.len(), start, end);
    rows[from..to].to_vec()
}

/// The element JS reads at `rows[k - 1]` once `rows.length >= k` (the lanes
/// index with `Math.ceil(k) - 1`, which exists whenever the length check
/// passed).
pub fn kth<T>(rows: &[T], k: f64) -> Option<&T> {
    if !((rows.len() as f64) >= k) || !(k >= 1.0) {
        return None;
    }
    rows.get(k.ceil() as usize - 1)
}

/// `array[index]` for a double index: only integral in-range indexes hit.
pub fn js_index<T>(rows: &[T], index: f64) -> Option<&T> {
    if index >= 0.0 && index.fract() == 0.0 {
        rows.get(index as usize)
    } else {
        None
    }
}

/// `Math.max(a, b)` (NaN-propagating).
pub fn js_max(a: f64, b: f64) -> f64 {
    super::federation::js_max(a, b)
}

/// `Math.min(a, b)` (NaN-propagating).
pub fn js_min(a: f64, b: f64) -> f64 {
    super::federation::js_min(a, b)
}

/// Half-open row ranges of `for (start = 0; start < len; start += batch)
/// rows.slice(start, start + batch)` with a double batch size.
pub fn js_batches(len: usize, batch: f64) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = 0.0f64;
    while start < len as f64 {
        out.push(super::federation::slice_bounds(len, start, start + batch));
        start += batch;
        if out.len() > len + 1 {
            break;
        }
    }
    out
}

/// A JSON number for a double count (`Math.max(rows, k)` style totals).
pub fn json_number(value: f64) -> Value {
    crate::util::json::num(value)
}

// ---------------------------------------------------------------- proof stats

#[derive(Default)]
pub struct ProofStats {
    pub attempts: usize,
    pub successes: usize,
    pub failures: HashMap<&'static str, usize>,
    pub last_failure_reason: &'static str,
    pub last_threshold: i64,
    pub last_max_outside_potential: i64,
    pub last_remaining_terms: usize,
    pub last_remaining_term_upper_bound: i64,
    pub filter_aware: bool,
    pub sort_aware: bool,
    pub doc_range_aware: bool,
    pub filter_unknown: bool,
    pub unknown_filter_fields: Vec<String>,
}

#[derive(Clone, Copy, Default)]
struct ProofDetail {
    threshold: Option<i64>,
    max_outside: Option<i64>,
    remaining_terms: Option<usize>,
    remaining_bound: Option<i64>,
}

impl ProofStats {
    pub fn new(has_filters: bool, sort_aware: bool, block_plan: Option<&BlockFilterPlan>) -> Self {
        ProofStats {
            filter_aware: has_filters,
            sort_aware,
            filter_unknown: block_plan.map(|p| !p.unknown_fields.is_empty()).unwrap_or(false),
            unknown_filter_fields: block_plan.map(|p| p.unknown_fields.clone()).unwrap_or_default(),
            ..Default::default()
        }
    }

    fn apply(&mut self, detail: ProofDetail) {
        if let Some(v) = detail.threshold {
            self.last_threshold = v;
        }
        if let Some(v) = detail.max_outside {
            self.last_max_outside_potential = v;
        }
        if let Some(v) = detail.remaining_terms {
            self.last_remaining_terms = v;
        }
        if let Some(v) = detail.remaining_bound {
            self.last_remaining_term_upper_bound = v;
        }
    }

    fn fail(&mut self, reason: &'static str, detail: ProofDetail) {
        *self.failures.entry(reason).or_default() += 1;
        self.last_failure_reason = reason;
        self.apply(detail);
    }

    fn succeed(&mut self, detail: ProofDetail) {
        self.successes += 1;
        self.last_failure_reason = "";
        self.apply(detail);
    }

    /// `topKProofStatsObject`.
    pub fn write(stats: Option<&ProofStats>, fallback_reason: &str, out: &mut Map<String, Value>) {
        match stats {
            None => {
                for (key, value) in [
                    ("topKProofAttempts", json!(0)),
                    ("topKProofSuccesses", json!(0)),
                    ("topKProofFailureReason", json!(fallback_reason)),
                    ("topKProofFailureCandidateCount", json!(0)),
                    ("topKProofFailureScoreBound", json!(0)),
                    ("topKProofFailureTieBound", json!(0)),
                    ("topKProofThreshold", json!(0)),
                    ("topKProofMaxOutsidePotential", json!(0)),
                    ("topKProofRemainingTerms", json!(0)),
                    ("topKProofRemainingTermUpperBound", json!(0)),
                    ("topKProofFilterAware", json!(false)),
                    ("topKProofSortAware", json!(false)),
                    ("topKProofDocRangeAware", json!(false)),
                    ("topKProofFilterUnknown", json!(false)),
                    ("topKProofUnknownFilterFields", json!("")),
                ] {
                    out.insert(key.into(), value);
                }
            }
            Some(s) => {
                let reason = if !fallback_reason.is_empty() { fallback_reason } else { s.last_failure_reason };
                for (key, value) in [
                    ("topKProofAttempts", json!(s.attempts)),
                    ("topKProofSuccesses", json!(s.successes)),
                    ("topKProofFailureReason", json!(reason)),
                    ("topKProofFailureCandidateCount", json!(s.failures.get("candidate_count").copied().unwrap_or(0))),
                    ("topKProofFailureScoreBound", json!(s.failures.get("score_bound").copied().unwrap_or(0))),
                    ("topKProofFailureTieBound", json!(s.failures.get("tie_bound").copied().unwrap_or(0))),
                    ("topKProofThreshold", json!(s.last_threshold)),
                    ("topKProofMaxOutsidePotential", json!(s.last_max_outside_potential)),
                    ("topKProofRemainingTerms", json!(s.last_remaining_terms)),
                    ("topKProofRemainingTermUpperBound", json!(s.last_remaining_term_upper_bound)),
                    ("topKProofFilterAware", json!(s.filter_aware)),
                    ("topKProofSortAware", json!(s.sort_aware)),
                    ("topKProofDocRangeAware", json!(s.doc_range_aware)),
                    ("topKProofFilterUnknown", json!(s.filter_unknown)),
                    ("topKProofUnknownFilterFields", json!(s.unknown_filter_fields.join(","))),
                ] {
                    out.insert(key.into(), value);
                }
            }
        }
    }
}

// ---------------------------------------------------------------- accumulators

/// Score/hit/mask accumulation (`scores`, `hits`, `masks` maps). `order`
/// keeps the docs in the order they were first scored: the JS `scores` Map
/// iterates in insertion order, and the proof loops report the first doc
/// that fails, so their stats depend on it.
#[derive(Default)]
pub struct Accumulator {
    pub scores: HashMap<i32, i64>,
    pub hits: HashMap<i32, u32>,
    pub masks: HashMap<i32, u32>,
    pub order: Vec<i32>,
}

impl Accumulator {
    #[inline]
    fn add(&mut self, doc: i32, impact: i64, bit: u32, is_base: bool) {
        self.add_score(doc, impact);
        if bit != 0 {
            *self.masks.entry(doc).or_insert(0) |= bit;
        }
        if is_base {
            self.add_hit(doc);
        }
    }

    /// `scores.set(doc, (scores.get(doc) || 0) + impact)`.
    #[inline]
    pub fn add_score(&mut self, doc: i32, impact: i64) {
        match self.scores.entry(doc) {
            std::collections::hash_map::Entry::Occupied(mut entry) => *entry.get_mut() += impact,
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(impact);
                self.order.push(doc);
            }
        }
    }

    /// `hits.set(doc, (hits.get(doc) || 0) + 1)`.
    #[inline]
    pub fn add_hit(&mut self, doc: i32) {
        *self.hits.entry(doc).or_insert(0) += 1;
    }

    /// `for (const [doc, score] of scores)`: insertion order.
    pub fn scores_in_order(&self) -> impl Iterator<Item = (i32, i64)> + '_ {
        self.order.iter().map(move |doc| (*doc, self.scores[doc]))
    }

    /// `collectEligibleScores`: all docs reaching minShouldMatch, by score
    /// desc then doc asc.
    pub fn eligible(&self, min_should_match: usize) -> Vec<(i32, i64)> {
        let need = min_should_match.max(1) as u32;
        let mut out: Vec<(i32, i64)> =
            self.scores.iter().filter(|(doc, _)| self.hits.get(doc).copied().unwrap_or(0) >= need).map(|(d, s)| (*d, *s)).collect();
        out.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        out
    }

    /// `topEligibleScores`: (eligible count, best k sorted). The limit is
    /// `Math.max(1, Math.floor(k))`.
    pub fn top_eligible(&self, min_should_match: usize, k: f64) -> (usize, Vec<(i32, i64)>) {
        let need = min_should_match.max(1) as u32;
        let limit = js_max(1.0, k.floor()) as usize;
        let mut heap: std::collections::BinaryHeap<(std::cmp::Reverse<i64>, i32)> = std::collections::BinaryHeap::new();
        let mut count = 0;
        for (doc, score) in &self.scores {
            if self.hits.get(doc).copied().unwrap_or(0) < need {
                continue;
            }
            count += 1;
            // Max-heap on (−score, doc): the root is the weakest kept row.
            let item = (std::cmp::Reverse(*score), *doc);
            if heap.len() < limit {
                heap.push(item);
            } else if let Some(top) = heap.peek() {
                if item < *top {
                    heap.pop();
                    heap.push(item);
                }
            }
        }
        let mut top: Vec<(i32, i64)> = heap.into_iter().map(|(score, doc)| (doc, score.0)).collect();
        top.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        (count, top)
    }
}

fn to_rows(rows: Vec<(i32, i64)>) -> Vec<Row> {
    rows.into_iter().map(|(doc, score)| (doc, score as f64)).collect()
}

// ---------------------------------------------------------------- cursors

pub struct Cursor {
    pub hit: TermHit,
    pub term_index: usize,
    pub is_base: bool,
    pub block_index: usize,
    pub superblock_index: usize,
    pub skipped_blocks: usize,
    pub skipped_superblocks: usize,
    pub superblocks_considered: usize,
    pub superblocks_decoded: usize,
    superblocks_seen: HashSet<usize>,
    superblocks_decoded_set: HashSet<usize>,
    remaining_impacts: Vec<i64>,
    remaining_tie_docs: Vec<i64>,
    doc_range_max: Option<HashMap<i64, i64>>,
}

impl Cursor {
    pub fn new(hit: TermHit, term_index: usize, is_base: bool) -> Self {
        let blocks = &hit.entry.blocks;
        let mut impacts = vec![0i64; blocks.len() + 1];
        let mut tie_docs = vec![INF; blocks.len() + 1];
        for index in (0..blocks.len()).rev() {
            let block = &blocks[index];
            let impact = block.max_impact;
            let tie = block.max_impact_doc;
            let (next_impact, next_tie) = (impacts[index + 1], tie_docs[index + 1]);
            if impact > next_impact {
                impacts[index] = impact;
                tie_docs[index] = tie;
            } else if impact == next_impact && impact > 0 {
                impacts[index] = impact;
                tie_docs[index] = tie.min(next_tie);
            } else {
                impacts[index] = next_impact;
                tie_docs[index] = next_tie;
            }
        }
        let doc_range_max = hit
            .entry
            .doc_ranges
            .as_ref()
            .filter(|ranges| !ranges.ranges.is_empty())
            .map(|ranges| ranges.ranges.iter().map(|range| (range.index, range.max_impact)).collect());
        Cursor {
            hit,
            term_index,
            is_base,
            block_index: 0,
            superblock_index: 0,
            skipped_blocks: 0,
            skipped_superblocks: 0,
            superblocks_considered: 0,
            superblocks_decoded: 0,
            superblocks_seen: HashSet::new(),
            superblocks_decoded_set: HashSet::new(),
            remaining_impacts: impacts,
            remaining_tie_docs: tie_docs,
            doc_range_max,
        }
    }

    pub fn entry(&self) -> &TermEntry {
        &self.hit.entry
    }

    fn block(&self) -> Option<&PostingBlock> {
        self.hit.entry.blocks.get(self.block_index)
    }

    fn bit(&self) -> u32 {
        if self.term_index < SKIP_MAX_TERMS {
            1u32 << self.term_index
        } else {
            0
        }
    }

    /// `cursorSuperblock`.
    fn superblock(&mut self) -> Option<usize> {
        let superblocks = &self.hit.entry.superblocks;
        if superblocks.is_empty() {
            return None;
        }
        let mut index = self.superblock_index.min(superblocks.len() - 1);
        while index < superblocks.len() && self.block_index >= superblocks[index].first_block + superblocks[index].block_count {
            index += 1;
        }
        while index > 0 && index < superblocks.len() && self.block_index < superblocks[index].first_block {
            index -= 1;
        }
        self.superblock_index = index;
        let superblock = superblocks.get(index)?;
        if self.block_index >= superblock.first_block && self.block_index < superblock.first_block + superblock.block_count {
            Some(index)
        } else {
            None
        }
    }

    /// `advanceCursor`: skip blocks whose summaries cannot pass the filter.
    pub fn advance(&mut self, plan: Option<&BlockFilterPlan>) -> bool {
        let entry = self.hit.entry.clone();
        while self.block_index < entry.blocks.len() {
            if let Some(index) = self.superblock() {
                if self.superblocks_seen.insert(index) {
                    self.superblocks_considered += 1;
                }
                let superblock = &entry.superblocks[index];
                if !block_may_pass(&superblock.filters, plan) {
                    let end = entry.blocks.len().min(superblock.first_block + superblock.block_count);
                    self.skipped_blocks += end.saturating_sub(self.block_index);
                    self.skipped_superblocks += 1;
                    self.block_index = end;
                    self.superblock_index += 1;
                    continue;
                }
            }
            if block_may_pass(&entry.blocks[self.block_index].filters, plan) {
                return true;
            }
            self.skipped_blocks += 1;
            self.block_index += 1;
        }
        false
    }

    fn mark_superblock_decoded(&mut self) {
        if let Some(index) = self.superblock() {
            if self.superblocks_decoded_set.insert(index) {
                self.superblocks_decoded += 1;
            }
        }
    }

    fn remaining_max(&self) -> i64 {
        let index = self.block_index.min(self.hit.entry.blocks.len());
        self.remaining_impacts[index]
    }

    fn remaining_tie_doc(&self) -> i64 {
        let index = self.block_index.min(self.hit.entry.blocks.len());
        self.remaining_tie_docs[index]
    }

    fn doc_range_max_for_doc(&self, doc: i32) -> Option<i64> {
        let ranges = self.hit.entry.doc_ranges.as_ref()?;
        if ranges.range_size == 0 {
            return None;
        }
        let index = doc as i64 / ranges.range_size as i64;
        Some(self.doc_range_max.as_ref().and_then(|m| m.get(&index).copied()).unwrap_or(0))
    }

    fn impact(&self) -> i64 {
        self.block().map(|b| b.max_impact).unwrap_or(0)
    }

    fn superblock_impact(&mut self) -> i64 {
        match self.superblock() {
            Some(index) => {
                let impact = self.hit.entry.superblocks[index].max_impact;
                if impact != 0 {
                    impact
                } else {
                    self.impact()
                }
            }
            None => self.impact(),
        }
    }

    fn apply_rows(
        &self,
        rows: &[i32],
        range: Option<(i32, i32)>,
        engine: &IndexEngine,
        store: Option<&ValueStore>,
        plan: Option<&DocFilterPlan>,
        acc: &mut Accumulator,
    ) -> usize {
        let bit = self.bit();
        let mut accepted = 0;
        for pair in rows.chunks_exact(2) {
            let doc = pair[0];
            if let Some((start, end)) = range {
                if doc < start || doc >= end {
                    continue;
                }
            }
            if let (Some(store), Some(plan)) = (store, plan) {
                if !engine.passes_filter_plan(doc, Some(store), plan, None) {
                    continue;
                }
            }
            acc.add(doc, pair[1] as i64, bit, self.is_base);
            accepted += 1;
        }
        accepted
    }
}

struct Potential {
    potential: i64,
    tie_doc_lower_bound: i64,
    terms: usize,
}

/// `remainingPotentialInfo`.
fn remaining_potential(cursors: &mut [Cursor], mask: u32, plan: Option<&BlockFilterPlan>, doc: Option<i32>) -> Potential {
    let mut potential = 0;
    let mut tie = 0;
    let mut has = false;
    let mut terms = 0;
    for cursor in cursors.iter_mut() {
        if !cursor.advance(plan) || (cursor.term_index < 32 && mask & (1 << cursor.term_index) != 0) {
            continue;
        }
        let Some(block) = cursor.block() else { continue };
        let block_impact = match cursor.remaining_max() {
            0 => block.max_impact,
            value => value,
        };
        potential += match doc.and_then(|doc| cursor.doc_range_max_for_doc(doc)) {
            Some(range_impact) => block_impact.min(range_impact),
            None => block_impact,
        };
        tie = tie.max(cursor.remaining_tie_doc());
        has = true;
        terms += 1;
    }
    Potential { potential, tie_doc_lower_bound: if has { tie } else { INF }, terms }
}

/// `remainingDocRangePotentialInfo`.
fn remaining_doc_range_potential(cursors: &mut [Cursor], plan: Option<&BlockFilterPlan>) -> Potential {
    let mut bounds: Vec<(i64, i64)> = Vec::new();
    let mut index_of: HashMap<i64, usize> = HashMap::new();
    let mut fallback = 0;
    let mut has_ranges = false;
    let mut terms = 0;
    for cursor in cursors.iter_mut() {
        if !cursor.advance(plan) {
            continue;
        }
        let Some(block) = cursor.block() else { continue };
        let block_impact = block.max_impact;
        terms += 1;
        let ranges = cursor.hit.entry.doc_ranges.as_ref().map(|r| &r.ranges).filter(|r| !r.is_empty());
        let remaining = match cursor.remaining_max() {
            0 => block_impact,
            value => value,
        };
        let Some(ranges) = ranges else {
            fallback += remaining;
            continue;
        };
        has_ranges = true;
        for range in ranges {
            let impact = remaining.min(range.max_impact);
            if impact == 0 {
                continue;
            }
            match index_of.get(&range.index) {
                Some(&at) => bounds[at].1 += impact,
                None => {
                    index_of.insert(range.index, bounds.len());
                    bounds.push((range.index, impact));
                }
            }
        }
    }
    if !has_ranges {
        return remaining_potential(cursors, 0, plan, None);
    }
    let mut potential = fallback;
    let mut best = false;
    for (_, bound) in &bounds {
        if bound + fallback > potential {
            potential = bound + fallback;
            best = true;
        }
    }
    Potential { potential, tie_doc_lower_bound: if best { 0 } else { INF }, terms }
}

/// `stableTopK`: prove the current top-k cannot change.
fn stable_top_k(
    acc: &Accumulator,
    cursors: &mut [Cursor],
    min_should_match: usize,
    k: f64,
    plan: Option<&BlockFilterPlan>,
    stats: &mut ProofStats,
) -> Option<Vec<(i32, i64)>> {
    stats.attempts += 1;
    let (count, top) = acc.top_eligible(min_should_match, k);
    if (count as f64) < k {
        stats.fail("candidate_count", ProofDetail::default());
        return None;
    }
    let top_docs: HashSet<i32> = top.iter().map(|(doc, _)| *doc).collect();
    let (boundary_doc, threshold) = *top.last()?;
    let has_doc_ranges = cursors.iter().any(|c| c.hit.entry.doc_ranges.as_ref().map(|r| r.ranges.len() >= 2).unwrap_or(false));
    let baseline = remaining_potential(cursors, 0, plan, None);
    let range_unseen = if has_doc_ranges { Some(remaining_doc_range_potential(cursors, plan)) } else { None };
    let doc_range_aware = range_unseen.as_ref().map(|r| r.potential < baseline.potential).unwrap_or(false);
    if doc_range_aware {
        stats.doc_range_aware = true;
    }
    let unseen = if doc_range_aware { range_unseen.unwrap() } else { baseline };
    let mut max_outside = unseen.potential;
    let mut max_outside_tie = unseen.tie_doc_lower_bound;
    let mut max_terms = unseen.terms;
    let mut max_bound = unseen.potential;
    let detail = |max_outside: i64, terms: usize, bound: i64| ProofDetail {
        threshold: Some(threshold),
        max_outside: Some(max_outside),
        remaining_terms: Some(terms),
        remaining_bound: Some(bound),
    };
    if max_outside > threshold {
        stats.fail("score_bound", detail(max_outside, max_terms, max_bound));
        return None;
    }
    if max_outside == threshold && max_outside_tie <= boundary_doc as i64 {
        stats.fail("tie_bound", detail(max_outside, max_terms, max_bound));
        return None;
    }
    for (doc, score) in acc.scores_in_order() {
        if top_docs.contains(&doc) {
            continue;
        }
        let mask = acc.masks.get(&doc).copied().unwrap_or(0);
        let remaining = remaining_potential(cursors, mask, plan, if doc_range_aware { Some(doc) } else { None });
        let potential = score + remaining.potential;
        if potential > max_outside || (potential == max_outside && (doc as i64) < max_outside_tie) {
            max_outside = potential;
            max_outside_tie = doc as i64;
            max_terms = remaining.terms;
            max_bound = remaining.potential;
        }
        if potential > threshold {
            stats.fail("score_bound", detail(potential, remaining.terms, remaining.potential));
            return None;
        }
        if potential == threshold && doc < boundary_doc {
            stats.fail("tie_bound", detail(potential, remaining.terms, remaining.potential));
            return None;
        }
    }
    if max_outside > threshold {
        stats.fail("score_bound", detail(max_outside, max_terms, max_bound));
        return None;
    }
    if max_outside == threshold && max_outside_tie <= boundary_doc as i64 {
        stats.fail("tie_bound", detail(max_outside, max_terms, max_bound));
        return None;
    }
    stats.succeed(detail(max_outside, max_terms, max_bound));
    Some(top)
}

fn posting_docs(rows: &[i32]) -> Vec<i32> {
    rows.chunks_exact(2).map(|pair| pair[0]).collect()
}

fn posting_docs_in_range(rows: &[i32], start: i32, end: i32) -> Vec<i32> {
    rows.chunks_exact(2).map(|pair| pair[0]).filter(|doc| *doc >= start && *doc < end).collect()
}

pub fn empty_rerank_stats(out: &mut Map<String, Value>) {
    for key in ["rerankCandidates", "dependencyFeatures", "dependencyTermsMatched", "dependencyPostingsScanned", "dependencyCandidateMatches"] {
        out.insert(key.into(), json!(0));
    }
}

fn distinct_shards(entries: &[TermHit]) -> usize {
    entries.iter().map(|hit| &*hit.shard_name).collect::<HashSet<&str>>().len()
}

/// `emptyTextSearchResponse`.
pub fn empty_text_response(page: f64, size: usize, terms: usize, entries: &[TermHit], missing_base_terms: usize) -> Value {
    json!({
        "total": 0,
        "page": json_number(page),
        "size": size,
        "results": [],
        "approximate": false,
        "stats": {
            "exact": true,
            "plannerLane": "empty",
            "topKProven": true,
            "totalExact": true,
            "tailExhausted": true,
            "blocksDecoded": 0,
            "postingsDecoded": 0,
            "postingsAccepted": 0,
            "skippedBlocks": 0,
            "terms": terms,
            "shards": distinct_shards(entries),
            "missingBaseTerms": missing_base_terms
        }
    })
}

pub struct RerankOutcome {
    pub ranked: Vec<Row>,
    pub stats: Map<String, Value>,
}

impl IndexEngine {
    /// `dependencyTerms`: proximity features with their weights.
    pub fn dependency_terms(&self, base_terms: &[String]) -> Vec<(String, f64)> {
        if base_terms.len() < 3 {
            return Vec::new();
        }
        let window = {
            let value = crate::util::json::num_or(&self.manifest_at(&["stats", "proximity_window"]), 5.0);
            if value.is_finite() {
                value as usize
            } else {
                5
            }
        };
        let mut out: Vec<(String, f64)> = Vec::new();
        for i in 0..base_terms.len() {
            let end = base_terms.len().min(i + window + 1);
            for j in (i + 1)..end {
                let term = proximity_term(&base_terms[i], &base_terms[j]);
                if term.is_empty() {
                    continue;
                }
                let weight = DEPENDENCY_SCORE_SCALE / ((j - i).max(1) as f64);
                match out.iter_mut().find(|(existing, _)| *existing == term) {
                    Some(item) => item.1 += weight,
                    None => out.push((term, weight)),
                }
            }
        }
        out
    }

    /// `candidateLimitFor`.
    pub fn candidate_limit_for(&self, base_terms: &[String], k: f64, rerank: bool) -> f64 {
        if !rerank || self.dependency_terms(base_terms).is_empty() {
            k
        } else {
            js_max(RERANK_CANDIDATES as f64, k)
        }
    }

    /// `rerankWithDependencies`.
    pub async fn rerank_with_dependencies(&self, ranked: Vec<Row>, base_terms: &[String], candidate_limit: f64) -> Result<RerankOutcome> {
        let features = self.dependency_terms(base_terms);
        // `Math.min(ranked.length, candidateLimit)`; head/tail split by
        // `slice(0, limit)` / `slice(limit)`.
        let limit_value = js_min(ranked.len() as f64, candidate_limit);
        let mut stats = Map::new();
        stats.insert("rerankCandidates".into(), json_number(limit_value));
        stats.insert("dependencyFeatures".into(), json!(features.len()));
        if features.is_empty() || !(limit_value > 1.0) {
            stats.insert("dependencyTermsMatched".into(), json!(0));
            stats.insert("dependencyPostingsScanned".into(), json!(0));
            stats.insert("dependencyCandidateMatches".into(), json!(0));
            return Ok(RerankOutcome { ranked, stats });
        }
        let limit = super::federation::slice_bounds(ranked.len(), 0.0, limit_value).1;
        let mut head: Vec<Row> = ranked[..limit].to_vec();
        let tail: Vec<Row> = ranked[limit..].to_vec();
        let mut scores: HashMap<i32, (f64, usize)> = head.iter().enumerate().map(|(rank, (doc, score))| (*doc, (*score, rank))).collect();
        let weights: HashMap<&str, f64> = features.iter().map(|(term, weight)| (term.as_str(), *weight)).collect();
        let mut matched = 0usize;
        let mut scanned = 0usize;
        let mut candidate_matches = 0usize;
        let feature_terms: Vec<String> = features.iter().map(|(term, _)| term.clone()).collect();
        for hit in self.term_entries(&feature_terms).await? {
            let weight = weights.get(hit.term.as_str()).copied().unwrap_or(0.0);
            if weight == 0.0 {
                continue;
            }
            matched += 1;
            let postings = self.decode_entry_postings(&hit).await?;
            scanned += postings.len() / 2;
            for pair in postings.chunks_exact(2) {
                if let Some(candidate) = scores.get_mut(&pair[0]) {
                    candidate.0 += pair[1] as f64 * weight;
                    candidate_matches += 1;
                }
            }
        }
        head.sort_by(|a, b| {
            let left = scores[&a.0];
            let right = scores[&b.0];
            right.0.partial_cmp(&left.0).unwrap_or(std::cmp::Ordering::Equal).then(left.1.cmp(&right.1)).then(a.0.cmp(&b.0))
        });
        let mut out: Vec<Row> = head.into_iter().map(|(doc, _)| (doc, scores[&doc].0)).collect();
        out.extend(tail);
        stats.insert("dependencyTermsMatched".into(), json!(matched));
        stats.insert("dependencyPostingsScanned".into(), json!(scanned));
        stats.insert("dependencyCandidateMatches".into(), json!(candidate_matches));
        Ok(RerankOutcome { ranked: out, stats })
    }

    async fn rerank_or_skip(&self, ranked: Vec<Row>, request: &TextRequest, candidate_k: f64, skip: bool) -> Result<RerankOutcome> {
        if !request.rerank || skip {
            let mut stats = Map::new();
            empty_rerank_stats(&mut stats);
            return Ok(RerankOutcome { ranked, stats });
        }
        self.rerank_with_dependencies(ranked, &request.base_terms, candidate_k).await
    }

    /// Decode one block per frontier cursor (`decodeCursorFrontier`).
    async fn decode_cursor_frontier(&self, cursors: &mut [Cursor], frontier: &[usize]) -> Result<(Vec<Rc<Vec<i32>>>, super::loaders::BatchStats)> {
        let mut requests = Vec::new();
        for &index in frontier {
            let cursor = &mut cursors[index];
            if !cursor.hit.entry.external {
                continue;
            }
            let max_exclusive = cursor.superblock().map(|sb| {
                let superblock = &cursor.hit.entry.superblocks[sb];
                superblock.first_block + superblock.block_count
            });
            for block_index in self.posting_block_prefetch_indexes(&cursor.hit.entry, cursor.block_index, EXTERNAL_POSTING_BLOCK_PREFETCH, max_exclusive) {
                requests.push((cursor.hit.entry.clone(), block_index));
            }
        }
        let batch = self.load_posting_block_batch(&requests, &self.options.range_plans.posting_block_frontier).await?;
        let mut out = Vec::with_capacity(frontier.len());
        for &index in frontier {
            let cursor = &cursors[index];
            let rows = if cursor.hit.entry.external {
                self.external_block_rows(&cursor.hit.entry, cursor.block_index).await?
            } else {
                postings::decode_posting_block(&cursor.hit.shard, &cursor.hit.entry, cursor.block_index)?
            };
            out.push(rows);
        }
        Ok((out, batch))
    }

    /// `decodeCursorBlockBatch` for the doc-range lane.
    async fn decode_block_batch(&self, cursors: &[Cursor], items: &[(usize, usize)]) -> Result<(Vec<(usize, usize, Rc<Vec<i32>>)>, super::loaders::BatchStats)> {
        let mut unique: Vec<(usize, usize)> = Vec::new();
        for item in items {
            if item.1 < cursors[item.0].hit.entry.blocks.len() && !unique.contains(item) {
                unique.push(*item);
            }
        }
        let requests: Vec<(Rc<TermEntry>, usize)> = unique
            .iter()
            .filter(|(cursor, _)| cursors[*cursor].hit.entry.external)
            .map(|(cursor, block)| (cursors[*cursor].hit.entry.clone(), *block))
            .collect();
        let mut batch = self.load_posting_block_batch(&requests, &self.options.range_plans.posting_doc_ranges).await?;
        batch.wanted = unique.len();
        let mut out = Vec::with_capacity(unique.len());
        for (cursor_index, block_index) in unique {
            let cursor = &cursors[cursor_index];
            let rows = if cursor.hit.entry.external {
                self.external_block_rows(&cursor.hit.entry, block_index).await?
            } else {
                postings::decode_posting_block(&cursor.hit.shard, &cursor.hit.entry, block_index)?
            };
            out.push((cursor_index, block_index, rows));
        }
        Ok((out, batch))
    }

    /// `applyDeferredGeoFilter`: (ranked, shortfall, stats).
    pub async fn apply_deferred_geo_filter(
        &self,
        ranked: Vec<Row>,
        deferred: Option<&DocFilterPlan>,
        exhausted: bool,
        k: f64,
    ) -> Result<(Vec<Row>, bool, Map<String, Value>)> {
        let Some(plan) = deferred else { return Ok((ranked, false, Map::new())) };
        if ranked.is_empty() {
            return Ok((ranked, false, Map::new()));
        }
        let docs: Vec<i32> = ranked.iter().map(|(doc, _)| *doc).collect();
        let store = self.value_store_for_filter_plan(plan, &docs, &[]).await?;
        let before = ranked.len();
        let filtered: Vec<Row> = ranked.into_iter().filter(|(doc, _)| self.passes_filter_plan(*doc, store.as_ref(), plan, None)).collect();
        let shortfall = !exhausted && (filtered.len() as f64) < js_min(k, before as f64);
        let mut stats = Map::new();
        stats.insert("geoFilterDeferred".into(), json!(true));
        stats.insert("geoFilterDeferredCandidates".into(), json!(before));
        stats.insert("geoFilterDeferredAccepted".into(), json!(filtered.len()));
        Ok((filtered, shortfall, stats))
    }

    /// `runSkippedSearch` for unsorted text (the sort branch is handled by
    /// the caller in `search.rs`).
    pub async fn run_skipped_text_search(&self, request: &TextRequest) -> Result<Value> {
        let offset = request.offset();
        let k = request.k();
        let candidate_k = self.candidate_limit_for(&request.base_terms, k, request.rerank);
        if request.base_terms.is_empty() || request.terms.len() > SKIP_MAX_TERMS || k > self.options.top_k_proof_max_k as f64 {
            let reason = if request.base_terms.is_empty() {
                "no_text_terms"
            } else if request.terms.len() > SKIP_MAX_TERMS {
                "too_many_terms"
            } else {
                "top_k_limit"
            };
            return self.run_full_search(request, reason).await;
        }
        let has_filters = request.filters.has_filters();
        if let Some(response) = self.try_query_bundle_search(request).await? {
            return Ok(response);
        }
        self.ensure_facet_dictionaries(&request.filters.facet_fields()).await?;
        let block_plan = if has_filters { Some(self.make_block_filter_plan(&request.filters)?) } else { None };
        let full_doc_plan = if has_filters { Some(self.make_doc_filter_plan(&request.filters)?) } else { None };
        let (doc_plan, deferred_plan) = match full_doc_plan {
            Some(plan) => split_deferred_geo(plan),
            None => (None, None),
        };
        let has_inline = doc_plan.as_ref().map(|p| p.active).unwrap_or(false);

        let entries = self.term_entries(&request.terms).await?;
        let base_set: HashSet<&str> = request.base_terms.iter().map(String::as_str).collect();
        let min_should_match = min_should_match_for(request.base_terms.len());
        let present: HashSet<&str> = entries.iter().filter(|hit| base_set.contains(hit.term.as_str())).map(|hit| hit.term.as_str()).collect();
        if present.len() < min_should_match.max(1) {
            return Ok(empty_text_response(request.page, request.size, request.terms.len(), &entries, request.base_terms.len().saturating_sub(present.len())));
        }
        if let Some(response) = self
            .run_doc_range_search(request, &entries, has_inline, block_plan.as_ref(), doc_plan.as_ref(), deferred_plan.as_ref(), candidate_k, min_should_match)
            .await?
        {
            return Ok(response);
        }
        let mut cursors: Vec<Cursor> =
            entries.iter().enumerate().map(|(index, hit)| Cursor::new(hit.clone(), index, base_set.contains(hit.term.as_str()))).collect();
        if cursors.is_empty() {
            return Ok(empty_text_response(request.page, request.size, request.terms.len(), &[], 0));
        }

        let mut acc = Accumulator::default();
        let (mut blocks_decoded, mut postings_decoded, mut postings_accepted) = (0usize, 0usize, 0usize);
        let mut stable: Option<Vec<(i32, i64)>> = None;
        let mut exhausted = false;
        let (mut frontier_batches, mut frontier_blocks, mut frontier_max) = (0usize, 0usize, 0usize);
        let mut frontier_stats = super::loaders::BatchStats::default();
        let mut filter_summary_proof_blocks = 0usize;
        let mut proof_interval = self.options.top_k_proof_check_interval;
        let mut blocks_since_proof = self.options.top_k_proof_check_interval;
        let mut budget_exhausted = false;
        let mut proof = ProofStats::new(has_filters, false, block_plan.as_ref());
        let mut tail_candidates: Option<usize> = None;
        let mut tail_blocks = 0usize;
        let frontier_size = self.options.posting_block_frontier;

        loop {
            let active: Vec<usize> = (0..cursors.len()).filter(|i| cursors[*i].advance(block_plan.as_ref())).collect();
            if active.is_empty() {
                exhausted = true;
                break;
            }
            let active_base = active.iter().filter(|i| cursors[**i].is_base).count();
            if active_base < min_should_match && self.options.conjunction_tail {
                // `drainConjunctionTail`.
                let mut candidates: HashSet<i32> = HashSet::new();
                for (doc, count) in &acc.hits {
                    if *count as usize + active_base >= min_should_match {
                        candidates.insert(*doc);
                    }
                }
                tail_candidates = Some(candidates.len());
                let mut sorted: Vec<i32> = candidates.iter().copied().collect();
                sorted.sort_unstable();
                let overlaps = |block: &PostingBlock| {
                    let low = sorted.partition_point(|doc| (*doc as i64) < block.doc_min);
                    low < sorted.len() && (sorted[low] as i64) <= block.doc_max
                };
                for &index in &active {
                    let (wanted, hit) = {
                        let cursor = &mut cursors[index];
                        let blocks = &cursor.hit.entry.blocks;
                        let mut wanted = Vec::new();
                        for block_index in cursor.block_index..blocks.len() {
                            if !candidates.is_empty() && block_may_pass(&blocks[block_index].filters, block_plan.as_ref()) && overlaps(&blocks[block_index]) {
                                wanted.push(block_index);
                            } else {
                                cursor.skipped_blocks += 1;
                            }
                        }
                        cursor.block_index = blocks.len();
                        (wanted, cursor.hit.clone())
                    };
                    if wanted.is_empty() {
                        continue;
                    }
                    tail_blocks += wanted.len();
                    for (_, rows, _) in self.lookup_entry_blocks(&hit, &wanted, &candidates).await? {
                        if rows.is_empty() {
                            continue;
                        }
                        let store = if has_inline {
                            self.value_store_for_filter_plan(doc_plan.as_ref().unwrap(), &posting_docs(&rows), &[]).await?
                        } else {
                            None
                        };
                        blocks_decoded += 1;
                        postings_decoded += rows.len() / 2;
                        postings_accepted += cursors[index].apply_rows(&rows, None, self, store.as_ref(), doc_plan.as_ref(), &mut acc);
                    }
                }
                exhausted = true;
                break;
            }
            if blocks_since_proof >= proof_interval {
                stable = stable_top_k(&acc, &mut cursors, min_should_match, candidate_k, block_plan.as_ref(), &mut proof);
                if stable.is_some() {
                    break;
                }
                blocks_since_proof = 0;
                proof_interval = self
                    .options
                    .top_k_proof_check_interval_max
                    .min(self.options.top_k_proof_check_interval.max(acc.scores.len() / self.options.top_k_proof_check_scores_per_block));
            }
            let mut order: Vec<(i64, i64, usize)> =
                active.iter().map(|&i| (cursors[i].superblock_impact(), cursors[i].impact(), i)).collect();
            order.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
            let frontier: Vec<usize> = order.iter().take(frontier_size).map(|item| item.2).collect();
            frontier_batches += 1;
            frontier_blocks += frontier.len();
            frontier_max = frontier_max.max(frontier.len());
            let (decoded, batch) = self.decode_cursor_frontier(&mut cursors, &frontier).await?;
            frontier_stats.add(batch);
            let proves: Vec<bool> = frontier
                .iter()
                .map(|&i| has_inline && cursors[i].block().map(|b| block_definitely_passes(&b.filters, doc_plan.as_ref())).unwrap_or(false))
                .collect();
            if has_inline {
                let docs: Vec<Vec<i32>> = decoded.iter().zip(&proves).filter(|(_, p)| !**p).map(|(rows, _)| posting_docs(rows)).collect();
                self.prefetch_filter_plan_doc_values(doc_plan.as_ref().unwrap(), &docs).await?;
            }
            for (slot, &index) in frontier.iter().enumerate() {
                let rows = &decoded[slot];
                let proves_block = proves[slot];
                if proves_block {
                    filter_summary_proof_blocks += 1;
                }
                cursors[index].mark_superblock_decoded();
                cursors[index].block_index += 1;
                let store = if has_inline && !proves_block {
                    self.value_store_for_filter_plan(doc_plan.as_ref().unwrap(), &posting_docs(rows), &[]).await?
                } else {
                    None
                };
                blocks_decoded += 1;
                postings_decoded += rows.len() / 2;
                let plan = if proves_block { None } else { doc_plan.as_ref() };
                postings_accepted += cursors[index].apply_rows(rows, None, self, store.as_ref(), plan, &mut acc);
                blocks_since_proof += 1;
                if blocks_since_proof >= proof_interval {
                    stable = stable_top_k(&acc, &mut cursors, min_should_match, candidate_k, block_plan.as_ref(), &mut proof);
                    if stable.is_some() {
                        break;
                    }
                    blocks_since_proof = 0;
                    proof_interval = self
                        .options
                        .top_k_proof_check_interval_max
                        .min(self.options.top_k_proof_check_interval.max(acc.scores.len() / self.options.top_k_proof_check_scores_per_block));
                }
                if self.top_k_block_budget > 0 && blocks_decoded >= self.top_k_block_budget {
                    budget_exhausted = true;
                    break;
                }
            }
            if stable.is_some() || budget_exhausted {
                break;
            }
        }

        let ranked = if exhausted {
            acc.eligible(min_should_match)
        } else {
            match stable.clone() {
                Some(top) => top,
                None => acc.top_eligible(min_should_match, k).1,
            }
        };
        let (ranked, shortfall, deferred_stats) = self.apply_deferred_geo_filter(to_rows(ranked), deferred_plan.as_ref(), exhausted, k).await?;
        if shortfall {
            return self.run_full_search(request, "filter_deferred_shortfall").await;
        }
        let reranked = self.rerank_or_skip(ranked, request, candidate_k, false).await?;
        let rows: Vec<Row> = page_slice(&reranked.ranked, offset, request.size);
        let mut context = ResultContext::new(true, PreferDocPages::Auto);
        let results = self.rows_to_search_results(&rows, &mut context, request.include_results).await?;
        let lane = if budget_exhausted { "blockBudget" } else if exhausted { "fullFallback" } else { "tailProof" };
        let fallback = if budget_exhausted { "block_budget" } else if exhausted { "tail_exhausted" } else { "" };
        let mut stats = Map::new();
        stats.insert("exact".into(), json!(exhausted && !budget_exhausted));
        stats.insert("plannerLane".into(), json!(lane));
        stats.extend(deferred_stats);
        stats.insert("topKProven".into(), json!(!budget_exhausted && (stable.is_some() || exhausted)));
        stats.insert("totalExact".into(), json!(exhausted && !budget_exhausted));
        stats.insert("tailExhausted".into(), json!(exhausted));
        stats.insert("blocksDecoded".into(), json!(blocks_decoded));
        stats.insert("postingsDecoded".into(), json!(postings_decoded));
        stats.insert("postingsAccepted".into(), json!(postings_accepted));
        stats.insert("skippedBlocks".into(), json!(cursors.iter().map(|c| c.skipped_blocks).sum::<usize>()));
        stats.insert("terms".into(), json!(request.terms.len()));
        stats.insert("shards".into(), json!(distinct_shards(&entries)));
        stats.insert("postingBlockFrontier".into(), json!(frontier_size));
        stats.insert("postingBlockFrontierBatches".into(), json!(frontier_batches));
        stats.insert("postingBlockFrontierBlocks".into(), json!(frontier_blocks));
        stats.insert("postingBlockFrontierMax".into(), json!(frontier_max));
        stats.insert("postingBlockFrontierFetchedBlocks".into(), json!(frontier_stats.fetched));
        stats.insert("postingBlockFrontierFetchGroups".into(), json!(frontier_stats.groups));
        stats.insert("postingBlockFrontierWantedBlocks".into(), json!(frontier_stats.wanted));
        stats.insert("topKProofCheckInterval".into(), json!(self.options.top_k_proof_check_interval));
        stats.insert("topKBlockBudget".into(), json!(self.top_k_block_budget));
        stats.insert("topKBlockBudgetExhausted".into(), json!(budget_exhausted));
        stats.insert("postingSuperblockScheduler".into(), json!(cursors.iter().any(|c| !c.hit.entry.superblocks.is_empty())));
        stats.insert("postingSuperblocks".into(), json!(entries.iter().map(|h| h.entry.superblocks.len()).sum::<usize>()));
        stats.insert("postingSuperblocksConsidered".into(), json!(cursors.iter().map(|c| c.superblocks_considered).sum::<usize>()));
        stats.insert("postingSuperblocksSkipped".into(), json!(cursors.iter().map(|c| c.skipped_superblocks).sum::<usize>()));
        stats.insert("postingSuperblocksDecoded".into(), json!(cursors.iter().map(|c| c.superblocks_decoded).sum::<usize>()));
        stats.insert("filterSummaryProofBlocks".into(), json!(filter_summary_proof_blocks));
        if let Some(candidates) = tail_candidates {
            stats.insert("conjunctionTailCandidates".into(), json!(candidates));
            stats.insert("conjunctionTailBlocks".into(), json!(tail_blocks));
        }
        stats.insert("plannerFallbackReason".into(), json!(fallback));
        ProofStats::write(Some(&proof), fallback, &mut stats);
        context.stats(&mut stats, true);
        stats.extend(reranked.stats);
        Ok(json!({
            "total": reranked.ranked.len(),
            "page": json_number(request.page),
            "size": request.size,
            "approximate": budget_exhausted || !exhausted,
            "results": results,
            "stats": Value::Object(stats)
        }))
    }

    /// `runDocRangeUpperBoundSearch`.
    #[allow(clippy::too_many_arguments)]
    async fn run_doc_range_search(
        &self,
        request: &TextRequest,
        entries: &[TermHit],
        has_filters: bool,
        block_plan: Option<&BlockFilterPlan>,
        doc_plan: Option<&DocFilterPlan>,
        deferred_plan: Option<&DocFilterPlan>,
        candidate_k: f64,
        min_should_match: usize,
    ) -> Result<Option<Value>> {
        if !self.options.doc_range_planner || entries.is_empty() {
            return Ok(None);
        }
        let base_set: HashSet<&str> = request.base_terms.iter().map(String::as_str).collect();
        // `buildDocRangeUpperBoundPlan`.
        let mut range_size: Option<usize> = None;
        let mut term_range_entries = 0usize;
        let mut ranges: Vec<RangeCandidate> = Vec::new();
        let mut range_at: HashMap<i64, usize> = HashMap::new();
        for hit in entries {
            let Some(doc_ranges) = hit.entry.doc_ranges.as_ref().filter(|r| r.range_size > 0 && !r.ranges.is_empty()) else {
                return Ok(None);
            };
            match range_size {
                None => range_size = Some(doc_ranges.range_size as usize),
                Some(size) if size != doc_ranges.range_size as usize => return Ok(None),
                _ => {}
            }
            let is_base = base_set.contains(hit.term.as_str());
            for range in &doc_ranges.ranges {
                let at = *range_at.entry(range.index).or_insert_with(|| {
                    ranges.push(RangeCandidate { index: range.index, upper_bound: 0, base_hits: 0, term_hits: 0 });
                    ranges.len() - 1
                });
                ranges[at].upper_bound += range.max_impact;
                ranges[at].term_hits += 1;
                if is_base {
                    ranges[at].base_hits += 1;
                }
                term_range_entries += 1;
            }
        }
        let range_size = range_size.unwrap_or(1).max(1);
        let mut candidates: Vec<RangeCandidate> =
            ranges.into_iter().filter(|r| r.upper_bound > 0 && r.base_hits >= min_should_match.max(1)).collect();
        candidates.sort_by(|a, b| b.upper_bound.cmp(&a.upper_bound).then(a.index.cmp(&b.index)));
        if candidates.len() < DOC_RANGE_PLANNER_MIN_CANDIDATE_RANGES {
            return Ok(None);
        }
        let total = self.total() as i64;
        let span = |index: i64| -> (i32, i32) {
            let start = index * range_size as i64;
            (start as i32, (start + range_size as i64).min(total) as i32)
        };
        // `docRangePlannerSelectivity`.
        let sample_size = candidates.len().min(self.options.posting_block_frontier.max(1));
        let (mut sample_blocks, mut available_blocks) = (0usize, 0usize);
        for range in candidates.iter().take(sample_size) {
            let (start, end) = span(range.index);
            for hit in entries {
                available_blocks += hit.entry.blocks.len();
                sample_blocks += doc_range_candidate_blocks(&hit.entry, start, end, block_plan).indexes.len();
            }
        }
        let ratio = if available_blocks > 0 { sample_blocks as f64 / available_blocks as f64 } else { 1.0 };
        if ratio > self.options.doc_range_planner_max_candidate_block_ratio {
            return Ok(None);
        }

        let offset = request.offset();
        let cursors: Vec<Cursor> =
            entries.iter().enumerate().map(|(index, hit)| Cursor::new(hit.clone(), index, base_set.contains(hit.term.as_str()))).collect();
        let impact_tier_terms = cursors.iter().filter(|c| c.hit.entry.impact_tiers.as_ref().map(|t| !t.blocks.is_empty()).unwrap_or(false)).count();
        let batch_cap = self.options.doc_range_block_prune_batch_size;
        // `Math.min(cap, Math.max(initial, Math.ceil(candidateK / 16) * initial))`.
        let initial_size = self.options.doc_range_block_prune_initial_batch_size as f64;
        let initial_batch = js_min(batch_cap as f64, js_max(initial_size, (candidate_k / 16.0).ceil() * initial_size)) as usize;
        let mut states: Vec<RangeState> = candidates
            .iter()
            .enumerate()
            .map(|(ordinal, range)| RangeState {
                range: range.clone(),
                ordinal,
                initialized: false,
                exhausted: false,
                queues: Vec::new(),
                bounds: vec![0; cursors.len()],
                tie_docs: vec![INF; cursors.len()],
                batch_limit: initial_batch,
            })
            .collect();
        let state_at: HashMap<i64, usize> = states.iter().enumerate().map(|(i, s)| (s.range.index, i)).collect();
        let mut acc = Accumulator::default();
        let mut decoded_blocks: HashSet<(usize, usize)> = HashSet::new();
        let mut proof = ProofStats::new(has_filters, false, block_plan);
        let mut stable: Option<Vec<(i32, i64)>>;
        let mut counters = DocRangeCounters::default();
        let mut stop_upper_bound;
        let impact_ranks: Vec<Option<HashMap<i32, usize>>> = cursors
            .iter()
            .map(|cursor| {
                if !self.options.doc_range_impact_planner {
                    return None;
                }
                cursor
                    .hit
                    .entry
                    .impact_tiers
                    .as_ref()
                    .filter(|t| !t.blocks.is_empty())
                    .map(|tiers| {
                        let mut ranks = HashMap::new();
                        for (rank, block) in tiers.blocks.iter().enumerate() {
                            ranks.insert(*block, rank);
                        }
                        ranks
                    })
            })
            .collect();

        loop {
            stop_upper_bound = max_outside_potential(&states, range_size).potential;
            stable = stable_doc_range_top_k(&acc, min_should_match, candidate_k, range_size, &state_at, &states, &mut proof);
            if stable.is_some() {
                counters.inner_blocks_pruned +=
                    states.iter().filter(|s| s.initialized).map(|s| s.remaining_queue_blocks()).sum::<usize>();
                break;
            }
            let Some(best) = best_range_state(&states, range_size) else { break };
            {
                let state = &mut states[best];
                if state.initialized {
                    counters.range_revisits += 1;
                } else {
                    state.initialized = true;
                    counters.ranges_visited += 1;
                    let (start, end) = span(state.range.index);
                    for cursor in &cursors {
                        let found = doc_range_candidate_blocks(&cursor.hit.entry, start, end, block_plan);
                        let ranks = &impact_ranks[cursor.term_index];
                        let mut tasks: Vec<RangeTask> = found
                            .indexes
                            .iter()
                            .map(|&block_index| {
                                let block = &cursor.hit.entry.blocks[block_index];
                                let upper = block_upper_bound_in_range(&cursor.hit.entry, block, state.range.index);
                                RangeTask {
                                    block_index,
                                    upper_bound: upper,
                                    tie_doc_lower_bound: task_tie_doc(block, start as i64, end as i64, upper),
                                    impact_rank: ranks.as_ref().and_then(|r| r.get(&(block_index as i32)).copied()).unwrap_or(usize::MAX),
                                }
                            })
                            .filter(|task| task.upper_bound > 0)
                            .collect();
                        tasks.sort_by(compare_tasks);
                        counters.candidate_blocks += tasks.len();
                        counters.considered_blocks += found.considered_blocks;
                        counters.skipped_blocks += found.skipped_blocks;
                        counters.considered_superblocks += found.considered_superblocks;
                        counters.skipped_superblocks += found.skipped_superblocks;
                        counters.impact_tier_tasks += tasks.iter().filter(|t| t.impact_rank != usize::MAX).count();
                        if tasks.is_empty() {
                            continue;
                        }
                        let queue = RangeQueue { cursor: cursor.term_index, tasks, offset: 0 };
                        state.bounds[queue.cursor] = queue.tasks[0].upper_bound;
                        state.tie_docs[queue.cursor] = queue.tasks[0].tie_doc_lower_bound;
                        state.queues.push(queue);
                    }
                    if state.remaining_queue_blocks() == 0 {
                        state.exhausted = true;
                    }
                }
                if state.exhausted {
                    continue;
                }
            }
            let (start, end) = span(states[best].range.index);
            let mut batch: Vec<(usize, usize)> = Vec::new();
            {
                let state = &mut states[best];
                let limit = batch_cap.min(state.batch_limit);
                while batch.len() < limit {
                    let mut best_queue: Option<usize> = None;
                    for (qi, queue) in state.queues.iter().enumerate() {
                        let Some(task) = queue.tasks.get(queue.offset) else { continue };
                        let better = match best_queue {
                            None => true,
                            Some(bq) => {
                                let current = &state.queues[bq].tasks[state.queues[bq].offset];
                                compare_tasks(task, current).is_lt()
                            }
                        };
                        if better {
                            best_queue = Some(qi);
                        }
                    }
                    let Some(qi) = best_queue else { break };
                    let queue = &mut state.queues[qi];
                    let task = queue.tasks[queue.offset].clone();
                    queue.offset += 1;
                    let next = queue.tasks.get(queue.offset);
                    state.bounds[queue.cursor] = next.map(|t| t.upper_bound).unwrap_or(0);
                    state.tie_docs[queue.cursor] = next.map(|t| t.tie_doc_lower_bound).unwrap_or(INF);
                    batch.push((queue.cursor, task.block_index));
                }
                if batch.is_empty() {
                    state.exhausted = true;
                    continue;
                }
            }
            counters.block_batches += 1;
            let (decoded, fetch) = self.decode_block_batch(&cursors, &batch).await?;
            counters.fetch.add(fetch);
            let mut pending: Vec<(usize, Rc<Vec<i32>>, Vec<i32>, bool)> = Vec::new();
            for (cursor_index, block_index, rows) in decoded {
                if decoded_blocks.insert((cursor_index, block_index)) {
                    counters.blocks_decoded += 1;
                }
                counters.blocks_visited += 1;
                counters.rows_scanned += rows.len() / 2;
                let docs_in_range = if has_filters { posting_docs_in_range(&rows, start, end) } else { Vec::new() };
                if has_filters && docs_in_range.is_empty() {
                    continue;
                }
                let proves = has_filters && block_definitely_passes(&cursors[cursor_index].hit.entry.blocks[block_index].filters, doc_plan);
                if proves {
                    counters.filter_summary_proof_blocks += 1;
                }
                pending.push((cursor_index, rows, docs_in_range, proves));
            }
            if has_filters {
                let docs: Vec<Vec<i32>> = pending.iter().filter(|p| !p.3).map(|p| p.2.clone()).collect();
                self.prefetch_filter_plan_doc_values(doc_plan.unwrap(), &docs).await?;
            }
            for (cursor_index, rows, docs_in_range, proves) in pending {
                let store = if has_filters && !proves {
                    self.value_store_for_filter_plan(doc_plan.unwrap(), &docs_in_range, &[]).await?
                } else {
                    None
                };
                counters.postings_decoded += rows.len() / 2;
                let plan = if proves { None } else { doc_plan };
                counters.postings_accepted += cursors[cursor_index].apply_rows(&rows, Some((start, end)), self, store.as_ref(), plan, &mut acc);
            }
            let state = &mut states[best];
            state.exhausted = state.remaining_queue_blocks() == 0;
            state.batch_limit = batch_cap.min((state.batch_limit * 2).max(1));
        }
        let exhausted = stable.is_none() && max_outside_potential(&states, range_size).potential <= 0;
        let ranked = if exhausted {
            acc.eligible(min_should_match)
        } else {
            match stable.clone() {
                Some(top) => top,
                None => acc.top_eligible(min_should_match, candidate_k).1,
            }
        };
        let (ranked, shortfall, deferred_stats) = self.apply_deferred_geo_filter(to_rows(ranked), deferred_plan, exhausted, request.k()).await?;
        if shortfall {
            return Ok(None);
        }
        let reranked = self.rerank_or_skip(ranked, request, candidate_k, false).await?;
        let rows: Vec<Row> = page_slice(&reranked.ranked, offset, request.size);
        let mut context = ResultContext::new(true, PreferDocPages::Auto);
        let results = self.rows_to_search_results(&rows, &mut context, request.include_results).await?;
        let reason = if exhausted { "range_exhausted" } else { "" };
        let mut stats = Map::new();
        let c = &counters;
        for (key, value) in [
            ("exact", json!(exhausted)),
            ("plannerLane", json!(if exhausted { "docRangeBlockMaxExhausted" } else { "docRangeBlockMax" })),
            ("topKProven", json!(stable.is_some() || exhausted)),
            ("totalExact", json!(exhausted)),
            ("tailExhausted", json!(exhausted)),
            ("blocksDecoded", json!(c.blocks_decoded)),
            ("postingsDecoded", json!(c.postings_decoded)),
            ("postingsAccepted", json!(c.postings_accepted)),
            ("skippedBlocks", json!(c.skipped_blocks)),
            ("terms", json!(request.terms.len())),
            ("shards", json!(distinct_shards(entries))),
            ("docRangeBlockMax", json!(true)),
            ("docRangeSize", json!(range_size)),
            ("docRangeCandidateRanges", json!(candidates.len())),
            ("docRangeRangesVisited", json!(c.ranges_visited)),
            ("docRangeRangesPruned", json!(candidates.len().saturating_sub(c.ranges_visited))),
            ("docRangeRangeRevisits", json!(c.range_revisits)),
            ("docRangeNextUpperBound", json!(stop_upper_bound)),
            ("docRangeTermRangeEntries", json!(term_range_entries)),
            ("docRangeCandidateBlockRatio", crate::util::json::num(ratio)),
            ("docRangeSelectivitySampleBlocks", json!(sample_blocks)),
            ("docRangeSelectivityAvailableBlocks", json!(available_blocks)),
            ("docRangeBlocksVisited", json!(c.blocks_visited)),
            ("docRangePostingRowsScanned", json!(c.rows_scanned)),
            ("docRangePostingBlocksConsidered", json!(c.considered_blocks)),
            ("docRangePostingBlocksCandidate", json!(c.candidate_blocks)),
            ("docRangePostingBlocksProcessed", json!(c.blocks_visited)),
            ("docRangePostingBlocksSkipped", json!(c.skipped_blocks)),
            ("docRangeInnerBlockBatches", json!(c.block_batches)),
            ("docRangeInnerBlocksPruned", json!(c.inner_blocks_pruned)),
            ("docRangeInitialBatchLimit", json!(initial_batch)),
            ("docRangeImpactPlanner", json!(self.options.doc_range_impact_planner && impact_tier_terms > 0)),
            ("docRangeImpactTierTerms", json!(impact_tier_terms)),
            ("docRangeImpactTierTasks", json!(c.impact_tier_tasks)),
            ("docRangeImpactSeed", json!(false)),
            ("docRangeImpactSeedBlocks", json!(0)),
            ("docRangeImpactSeedRowsScanned", json!(0)),
            ("docRangeImpactSeedPostingsAccepted", json!(0)),
            ("docRangeImpactSeedFetchedBlocks", json!(0)),
            ("docRangeImpactSeedFetchGroups", json!(0)),
            ("docRangeImpactSeedWantedBlocks", json!(0)),
            ("docRangeImpactSeedIndexedTerms", json!(impact_tier_terms)),
            ("docRangeImpactSeedScannedTerms", json!(0)),
            ("docRangePostingSuperblocksConsidered", json!(c.considered_superblocks)),
            ("docRangePostingSuperblocksSkipped", json!(c.skipped_superblocks)),
            ("docRangeFetchedBlocks", json!(c.fetch.fetched)),
            ("docRangeFetchGroups", json!(c.fetch.groups)),
            ("docRangeWantedBlocks", json!(c.fetch.wanted)),
            ("filterSummaryProofBlocks", json!(c.filter_summary_proof_blocks)),
        ] {
            stats.insert(key.into(), value);
        }
        stats.extend(deferred_stats);
        stats.insert("plannerFallbackReason".into(), json!(reason));
        ProofStats::write(Some(&proof), reason, &mut stats);
        context.stats(&mut stats, true);
        stats.extend(reranked.stats);
        Ok(Some(json!({
            "total": reranked.ranked.len(),
            "page": json_number(request.page),
            "size": request.size,
            "approximate": !exhausted,
            "results": results,
            "stats": Value::Object(stats)
        })))
    }

    /// Exact conjunction-first scoring of every posting (`runFullSearch`
    /// without the sort/segment branches, which `search.rs` routes).
    pub async fn run_full_search(&self, request: &TextRequest, reason: &str) -> Result<Value> {
        if reason == "exact_requested" || self.options.segment_fanout == Some(true) {
            if let Some(response) = self.run_segment_fanout_search(request, reason).await? {
                return Ok(response);
            }
        }
        let offset = request.offset();
        let sort_plan = self.make_sort_plan(&request.sort);
        let has_filters = request.filters.has_filters();
        if sort_plan.is_some() {
            self.ensure_doc_values_manifest().await?;
        }
        self.ensure_facet_dictionaries(&request.filters.facet_fields()).await?;
        let entries = self.term_entries(&request.terms).await?;
        let base_set: HashSet<&str> = request.base_terms.iter().map(String::as_str).collect();
        let min_should_match = min_should_match_for(request.base_terms.len());
        let present: HashSet<&str> = entries.iter().filter(|hit| base_set.contains(hit.term.as_str())).map(|hit| hit.term.as_str()).collect();
        if present.len() < min_should_match.max(1) {
            return Ok(empty_text_response(request.page, request.size, request.terms.len(), &entries, request.base_terms.len().saturating_sub(present.len())));
        }
        let doc_plan = if has_filters { Some(self.make_doc_filter_plan(&request.filters)?) } else { None };
        let mut store: Option<ValueStore> =
            if sort_plan.is_some() && self.doc_values_meta().is_none() { Some(ValueStore { codes: Some(self.load_codes().await?), ..Default::default() }) } else { None };
        let mut acc = Accumulator::default();
        for hit in &entries {
            let postings = self.decode_entry_postings(hit).await?;
            let is_base = base_set.contains(hit.term.as_str());
            for pair in postings.chunks_exact(2) {
                acc.add(pair[0], pair[1] as i64, 0, is_base);
            }
        }
        let mut ranked = to_rows(acc.eligible(min_should_match));
        if let Some(plan) = &doc_plan {
            let docs: Vec<i32> = ranked.iter().map(|(doc, _)| *doc).collect();
            let filter_store = self.value_store_for_filter_plan(plan, &docs, &[]).await?;
            ranked.retain(|(doc, _)| self.passes_filter_plan(*doc, filter_store.as_ref(), plan, None));
            if store.is_none() {
                store = filter_store;
            }
        }
        let candidate_k = self.candidate_limit_for(&request.base_terms, offset + request.size as f64, request.rerank);
        let reranked = self.rerank_or_skip(ranked, request, candidate_k, sort_plan.is_some()).await?;
        let mut ranked = reranked.ranked;
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
        let postings: usize = entries.iter().map(|hit| hit.entry.count as usize).sum();
        let mut stats = Map::new();
        for (key, value) in [
            ("exact", json!(true)),
            ("plannerLane", json!("fullFallback")),
            ("topKProven", json!(true)),
            ("totalExact", json!(true)),
            ("tailExhausted", json!(true)),
            ("terms", json!(request.terms.len())),
            ("shards", json!(distinct_shards(&entries))),
            ("postings", json!(postings)),
            ("blocksDecoded", json!(entries.iter().map(|hit| hit.entry.blocks.len()).sum::<usize>())),
            ("postingsDecoded", json!(postings)),
            ("postingsAccepted", json!(ranked.len())),
            ("skippedBlocks", json!(0)),
            ("plannerFallbackReason", json!(reason)),
        ] {
            stats.insert(key.into(), value);
        }
        ProofStats::write(None, reason, &mut stats);
        context.stats(&mut stats, true);
        stats.extend(reranked.stats);
        Ok(json!({
            "total": ranked.len(),
            "page": json_number(request.page),
            "size": request.size,
            "results": results,
            "approximate": false,
            "stats": Value::Object(stats)
        }))
    }

    fn empty_count(&self, base_terms: &[String], entries: &[TermHit], missing: usize, lane: &str) -> Value {
        json!({
            "total": 0,
            "totalExact": true,
            "approximate": false,
            "stats": {
                "exact": true,
                "plannerLane": lane,
                "countLane": lane,
                "totalExact": true,
                "terms": base_terms.len(),
                "baseTermCount": base_terms.len(),
                "minShouldMatch": if base_terms.is_empty() { 0 } else { min_should_match_for(base_terms.len()) },
                "termEntriesVisited": entries.len(),
                "shards": distinct_shards(entries),
                "missingBaseTerms": missing,
                "blocksDecoded": 0,
                "postingsDecoded": 0,
                "postingsAccepted": 0
            }
        })
    }

    /// `runCountSearch`.
    pub async fn run_count_search(&self, base_terms: &[String]) -> Result<Value> {
        if base_terms.is_empty() {
            return Ok(self.empty_count(base_terms, &[], 0, "countTermless"));
        }
        let min_should_match = min_should_match_for(base_terms.len());
        let entries = self.term_entries(base_terms).await?;
        let present: HashSet<&str> = entries.iter().map(|hit| hit.term.as_str()).collect();
        let missing = base_terms.len().saturating_sub(present.len());
        if present.len() < min_should_match.max(1) {
            return Ok(self.empty_count(base_terms, &entries, missing, "countMissingTerms"));
        }
        let mut stats = Map::new();
        for (key, value) in [
            ("exact", json!(true)),
            ("totalExact", json!(true)),
            ("terms", json!(base_terms.len())),
            ("baseTermCount", json!(base_terms.len())),
            ("minShouldMatch", json!(min_should_match)),
            ("termEntriesVisited", json!(entries.len())),
            ("shards", json!(distinct_shards(&entries))),
            ("missingBaseTerms", json!(missing)),
        ] {
            stats.insert(key.into(), value);
        }
        if base_terms.len() == 1 {
            let total: usize = entries.iter().map(|hit| (if hit.entry.df > 0 { hit.entry.df } else { hit.entry.count }) as usize).sum();
            stats.insert("plannerLane".into(), json!("countSingleTermDf"));
            stats.insert("countLane".into(), json!("countSingleTermDf"));
            stats.insert("blocksDecoded".into(), json!(0));
            stats.insert("postingsDecoded".into(), json!(0));
            stats.insert("postingsAccepted".into(), json!(total));
            return Ok(json!({ "total": total, "totalExact": true, "approximate": false, "stats": Value::Object(stats) }));
        }
        let (total, blocks, decoded) = self.count_matches(&entries, base_terms.len(), min_should_match).await?;
        stats.insert("plannerLane".into(), json!("countPostingCounter"));
        stats.insert("countLane".into(), json!("countPostingCounter"));
        stats.insert("blocksDecoded".into(), json!(blocks));
        stats.insert("postingsDecoded".into(), json!(decoded));
        stats.insert("postingsAccepted".into(), json!(total));
        Ok(json!({ "total": total, "totalExact": true, "approximate": false, "stats": Value::Object(stats) }))
    }

    async fn count_matches(&self, entries: &[TermHit], base_terms: usize, min_should_match: usize) -> Result<(usize, usize, usize)> {
        let mut counters = MatchCounters::for_base_terms(base_terms, self.total());
        let (mut blocks, mut decoded) = (0usize, 0usize);
        for hit in entries {
            let postings = self.decode_entry_postings(hit).await?;
            blocks += hit.entry.blocks.len();
            decoded += postings.len() / 2;
            counters.count(&postings);
        }
        let total = counters.matching(min_should_match).count();
        Ok((total, blocks, decoded))
    }

    /// `collectTextMatchDocs`: the exact match set, or `None` above the
    /// `geoTextSortMaxDf` budget. Docs come back in ascending order.
    pub async fn collect_text_match_docs(&self, base_terms: &[String]) -> Result<Option<(Vec<i32>, Map<String, Value>)>> {
        let min_should_match = min_should_match_for(base_terms.len());
        let entries = self.term_entries(base_terms).await?;
        let present: HashSet<&str> = entries.iter().map(|hit| hit.term.as_str()).collect();
        let mut stats = Map::new();
        stats.insert("terms".into(), json!(base_terms.len()));
        stats.insert("minShouldMatch".into(), json!(min_should_match));
        stats.insert("termEntriesVisited".into(), json!(entries.len()));
        if present.len() < min_should_match.max(1) {
            stats.insert("blocksDecoded".into(), json!(0));
            stats.insert("postingsDecoded".into(), json!(0));
            return Ok(Some((Vec::new(), stats)));
        }
        let total_df: usize = entries.iter().map(|hit| (if hit.entry.df > 0 { hit.entry.df } else { hit.entry.count }) as usize).sum();
        if total_df > self.options.geo_text_sort_max_df {
            return Ok(None);
        }
        let mut counters = MatchCounters::for_base_terms(base_terms.len(), self.total());
        let (mut blocks, mut decoded) = (0usize, 0usize);
        for hit in &entries {
            let postings = self.decode_entry_postings(hit).await?;
            blocks += hit.entry.blocks.len();
            decoded += postings.len() / 2;
            counters.count(&postings);
        }
        stats.insert("blocksDecoded".into(), json!(blocks));
        stats.insert("postingsDecoded".into(), json!(decoded));
        let docs = counters.matching(min_should_match).map(|doc| doc as i32).collect();
        Ok(Some((docs, stats)))
    }
}

/// Per-doc base-term match counters sized like `counterArrayForBaseTerms`:
/// one byte per doc up to 255 base terms, two up to 65,535, else four
/// (wrapping like the typed arrays).
enum MatchCounters {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
}

impl MatchCounters {
    fn for_base_terms(base_terms: usize, total: usize) -> Self {
        if base_terms <= 255 {
            MatchCounters::U8(vec![0; total])
        } else if base_terms <= 65_535 {
            MatchCounters::U16(vec![0; total])
        } else {
            MatchCounters::U32(vec![0; total])
        }
    }

    fn count(&mut self, postings: &[i32]) {
        fn bump<T: Copy>(counters: &mut [T], postings: &[i32], add: impl Fn(T) -> T) {
            for pair in postings.chunks_exact(2) {
                if pair[0] >= 0 {
                    if let Some(counter) = counters.get_mut(pair[0] as usize) {
                        *counter = add(*counter);
                    }
                }
            }
        }
        match self {
            MatchCounters::U8(counters) => bump(counters, postings, |c| c.wrapping_add(1)),
            MatchCounters::U16(counters) => bump(counters, postings, |c| c.wrapping_add(1)),
            MatchCounters::U32(counters) => bump(counters, postings, |c| c.wrapping_add(1)),
        }
    }

    /// Docs (ascending) whose counter reaches `minShouldMatch`.
    fn matching(&self, min_should_match: usize) -> Box<dyn Iterator<Item = usize> + '_> {
        match self {
            MatchCounters::U8(counters) => Box::new(counters.iter().enumerate().filter(move |(_, c)| **c as usize >= min_should_match).map(|(doc, _)| doc)),
            MatchCounters::U16(counters) => Box::new(counters.iter().enumerate().filter(move |(_, c)| **c as usize >= min_should_match).map(|(doc, _)| doc)),
            MatchCounters::U32(counters) => Box::new(counters.iter().enumerate().filter(move |(_, c)| **c as usize >= min_should_match).map(|(doc, _)| doc)),
        }
    }
}

/// `splitDeferredGeoFilterPlan` → (inline, deferred).
pub fn split_deferred_geo(plan: DocFilterPlan) -> (Option<DocFilterPlan>, Option<DocFilterPlan>) {
    let Some(geo) = plan.geo.clone().filter(|g| !g.has_doc_set()) else { return (Some(plan), None) };
    let is_geo_field = |field: &String| *field == geo.lat_field || *field == geo.lon_field;
    let inline_numbers: Vec<_> = plan.numbers.iter().filter(|(f, _)| !is_geo_field(f)).cloned().collect();
    let deferred_numbers: Vec<_> = plan.numbers.iter().filter(|(f, _)| is_geo_field(f)).cloned().collect();
    let inline_active = !plan.facets.is_empty() || !inline_numbers.is_empty() || !plan.booleans.is_empty();
    let inline = DocFilterPlan { facets: plan.facets.clone(), numbers: inline_numbers, booleans: plan.booleans.clone(), geo: None, active: inline_active };
    let deferred = DocFilterPlan { facets: Vec::new(), numbers: deferred_numbers, booleans: Vec::new(), geo: Some(geo), active: true };
    (Some(inline), Some(deferred))
}

// ---------------------------------------------------------------- doc-range planner helpers

#[derive(Clone)]
struct RangeCandidate {
    index: i64,
    upper_bound: i64,
    base_hits: usize,
    term_hits: usize,
}

#[derive(Clone)]
struct RangeTask {
    block_index: usize,
    upper_bound: i64,
    tie_doc_lower_bound: i64,
    impact_rank: usize,
}

fn compare_tasks(a: &RangeTask, b: &RangeTask) -> std::cmp::Ordering {
    b.upper_bound
        .cmp(&a.upper_bound)
        .then(a.impact_rank.cmp(&b.impact_rank))
        .then(a.tie_doc_lower_bound.cmp(&b.tie_doc_lower_bound))
        .then(a.block_index.cmp(&b.block_index))
}

struct RangeQueue {
    cursor: usize,
    tasks: Vec<RangeTask>,
    offset: usize,
}

struct RangeState {
    range: RangeCandidate,
    ordinal: usize,
    initialized: bool,
    exhausted: bool,
    queues: Vec<RangeQueue>,
    bounds: Vec<i64>,
    tie_docs: Vec<i64>,
    batch_limit: usize,
}

impl RangeState {
    fn remaining_queue_blocks(&self) -> usize {
        self.queues.iter().map(|q| q.tasks.len().saturating_sub(q.offset)).sum()
    }
}

#[derive(Default)]
struct DocRangeCounters {
    ranges_visited: usize,
    range_revisits: usize,
    candidate_blocks: usize,
    considered_blocks: usize,
    skipped_blocks: usize,
    considered_superblocks: usize,
    skipped_superblocks: usize,
    impact_tier_tasks: usize,
    blocks_decoded: usize,
    blocks_visited: usize,
    postings_decoded: usize,
    rows_scanned: usize,
    postings_accepted: usize,
    fetch: super::loaders::BatchStats,
    filter_summary_proof_blocks: usize,
    block_batches: usize,
    inner_blocks_pruned: usize,
}

pub struct CandidateBlocks {
    pub indexes: Vec<usize>,
    pub considered_blocks: usize,
    pub skipped_blocks: usize,
    pub considered_superblocks: usize,
    pub skipped_superblocks: usize,
}

fn block_overlaps_span(doc_min: i64, doc_max: i64, min: i64, max: i64) -> bool {
    doc_max >= min && doc_min <= max
}

/// `blockUpperBoundInDocRange`.
fn block_upper_bound_in_range(entry: &TermEntry, block: &PostingBlock, range_index: i64) -> i64 {
    if let Some(ranges) = block.doc_ranges.as_ref().filter(|r| !r.is_empty()) {
        return ranges.iter().find(|r| r.index == range_index).map(|r| r.max_impact).unwrap_or(0);
    }
    let range_impact = entry
        .doc_ranges
        .as_ref()
        .and_then(|d| d.ranges.iter().find(|r| r.index == range_index))
        .map(|r| r.max_impact)
        .unwrap_or(0);
    if range_impact != 0 {
        block.max_impact.min(range_impact)
    } else {
        block.max_impact
    }
}

fn task_tie_doc(block: &PostingBlock, start: i64, end: i64, upper_bound: i64) -> i64 {
    if block.max_impact == upper_bound && block.max_impact_doc >= start && block.max_impact_doc < end {
        return block.max_impact_doc;
    }
    start.max((end - 1).min(block.doc_min))
}

/// `docRangeCandidateBlockIndexes`.
pub fn doc_range_candidate_blocks(entry: &TermEntry, start: i32, end: i32, plan: Option<&BlockFilterPlan>) -> CandidateBlocks {
    let mut out = CandidateBlocks { indexes: Vec::new(), considered_blocks: 0, skipped_blocks: 0, considered_superblocks: 0, skipped_superblocks: 0 };
    let blocks = &entry.blocks;
    let max_doc = (start as i64).max(end as i64 - 1);
    let range_index = entry.doc_ranges.as_ref().filter(|r| r.range_size > 0).map(|r| start as i64 / r.range_size as i64);
    let may_contribute = |block: &PostingBlock| range_index.map(|index| block_upper_bound_in_range(entry, block, index) > 0).unwrap_or(true);
    let consider = |block_index: usize, out: &mut CandidateBlocks| {
        out.considered_blocks += 1;
        let block = &blocks[block_index];
        if block_may_pass(&block.filters, plan) && block_overlaps_span(block.doc_min, block.doc_max, start as i64, max_doc) && may_contribute(block) {
            out.indexes.push(block_index);
        } else {
            out.skipped_blocks += 1;
        }
    };
    if !entry.superblocks.is_empty() {
        for superblock in &entry.superblocks {
            out.considered_superblocks += 1;
            let first = superblock.first_block;
            let end_block = blocks.len().min(first + superblock.block_count);
            if !block_may_pass(&superblock.filters, plan) || !block_overlaps_span(superblock.doc_min, superblock.doc_max, start as i64, max_doc) {
                out.skipped_superblocks += 1;
                out.skipped_blocks += end_block.saturating_sub(first);
                continue;
            }
            for block_index in first..end_block {
                consider(block_index, &mut out);
            }
        }
        return out;
    }
    for block_index in 0..blocks.len() {
        consider(block_index, &mut out);
    }
    out
}

fn range_state_potential(state: &RangeState, range_size: usize) -> Potential {
    if state.exhausted {
        return Potential { potential: 0, tie_doc_lower_bound: INF, terms: 0 };
    }
    if !state.initialized {
        return Potential { potential: state.range.upper_bound, tie_doc_lower_bound: state.range.index * range_size as i64, terms: state.range.term_hits };
    }
    remaining_block_potential(&state.bounds, 0, &state.tie_docs)
}

/// `remainingBlockPotential`.
fn remaining_block_potential(bounds: &[i64], mask: u32, tie_docs: &[i64]) -> Potential {
    let mut potential = 0;
    let mut tie = 0;
    let mut has = false;
    let mut terms = 0;
    for (index, bound) in bounds.iter().enumerate() {
        if *bound == 0 {
            continue;
        }
        if index < SKIP_MAX_TERMS && mask & (1 << index) != 0 {
            continue;
        }
        potential += bound;
        tie = tie.max(tie_docs[index]);
        has = true;
        terms += 1;
    }
    Potential { potential, tie_doc_lower_bound: if has { tie } else { INF }, terms }
}

fn best_range_state(states: &[RangeState], range_size: usize) -> Option<usize> {
    let mut best: Option<(usize, Potential)> = None;
    for (index, state) in states.iter().enumerate() {
        let current = range_state_potential(state, range_size);
        if current.potential <= 0 {
            continue;
        }
        let better = match &best {
            None => true,
            Some((best_index, best_potential)) => {
                if current.potential != best_potential.potential {
                    current.potential > best_potential.potential
                } else if current.tie_doc_lower_bound != best_potential.tie_doc_lower_bound {
                    current.tie_doc_lower_bound < best_potential.tie_doc_lower_bound
                } else {
                    state.ordinal < states[*best_index].ordinal
                }
            }
        };
        if better {
            best = Some((index, current));
        }
    }
    best.map(|(index, _)| index)
}

fn max_outside_potential(states: &[RangeState], range_size: usize) -> Potential {
    let mut out = Potential { potential: 0, tie_doc_lower_bound: INF, terms: 0 };
    for state in states {
        let current = range_state_potential(state, range_size);
        if current.potential > out.potential {
            out = current;
        } else if current.potential == out.potential && current.potential > 0 {
            out.tie_doc_lower_bound = out.tie_doc_lower_bound.min(current.tie_doc_lower_bound);
            out.terms = out.terms.max(current.terms);
        }
    }
    out
}

/// `stableDocRangeGlobalTopK`.
fn stable_doc_range_top_k(
    acc: &Accumulator,
    min_should_match: usize,
    k: f64,
    range_size: usize,
    state_at: &HashMap<i64, usize>,
    states: &[RangeState],
    stats: &mut ProofStats,
) -> Option<Vec<(i32, i64)>> {
    stats.attempts += 1;
    stats.doc_range_aware = true;
    // `collectEligibleScores(...).slice(0, k)`: the same rows as the bounded
    // selection for every k >= 1.
    let (count, top) = acc.top_eligible(min_should_match, k);
    let outside = max_outside_potential(states, range_size);
    if (count as f64) < k {
        stats.fail("candidate_count", ProofDetail { max_outside: Some(outside.potential), ..Default::default() });
        return None;
    }
    let top_docs: HashSet<i32> = top.iter().map(|(doc, _)| *doc).collect();
    let (boundary_doc, threshold) = *top.last()?;
    let mut max_outside = outside.potential;
    let mut max_tie = outside.tie_doc_lower_bound;
    let mut max_terms = outside.terms;
    let mut max_bound = outside.potential;
    let detail = |m: i64, t: usize, b: i64| ProofDetail { threshold: Some(threshold), max_outside: Some(m), remaining_terms: Some(t), remaining_bound: Some(b) };
    if max_outside > threshold || (max_outside == threshold && max_tie <= boundary_doc as i64) {
        stats.fail(if max_outside == threshold { "tie_bound" } else { "score_bound" }, detail(max_outside, max_terms, max_bound));
        return None;
    }
    for (doc, score) in acc.scores_in_order() {
        if top_docs.contains(&doc) {
            continue;
        }
        let remaining = match state_at.get(&(doc as i64 / range_size as i64)).map(|i| &states[*i]) {
            Some(state) if state.initialized && !state.exhausted => {
                remaining_block_potential(&state.bounds, acc.masks.get(&doc).copied().unwrap_or(0), &state.tie_docs)
            }
            _ => Potential { potential: 0, tie_doc_lower_bound: INF, terms: 0 },
        };
        let potential = score + remaining.potential;
        if potential > max_outside || (potential == max_outside && (doc as i64) < max_tie) {
            max_outside = potential;
            max_tie = doc as i64;
            max_terms = remaining.terms;
            max_bound = remaining.potential;
        }
        if potential > threshold || (potential == threshold && doc < boundary_doc) {
            stats.fail(if potential == threshold { "tie_bound" } else { "score_bound" }, detail(potential, remaining.terms, remaining.potential));
            return None;
        }
    }
    stats.succeed(detail(max_outside, max_terms, max_bound));
    Some(top)
}
