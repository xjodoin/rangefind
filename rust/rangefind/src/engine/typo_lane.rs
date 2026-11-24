//! Main-index typo correction: a port of `maybeTypoFallback` and its
//! helpers (`shouldAttemptTypoFallback`, `typoCorrectionShouldReplace`,
//! `maybeSurfaceExactFallback`, `correctedTypoQuery`,
//! `typoCandidatesForToken`, `rankTypoCorrectionPlans`,
//! `rawSurfaceFallbackTerms`) from `src/runtime.js`.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::analysis::{AnalyzedTerm, QueryPlan};
use crate::error::Result;
use crate::pointer::Pointer;
use crate::terms::expanded_terms_from_base_terms;
use crate::typo::{
    best_main_index_typo_distance, is_typo_correction_token, main_index_typo_candidate_score, main_index_typo_probe_values,
    ngram_overlap, normalize_main_index_typo_options, typo_max_edits_for, MainIndexTypoOptions,
};
use crate::util::json::{num, to_number, truthy, NULL};
use crate::util::jsstr::{cmp16, len16};

use super::index::{min_should_match_for, IndexEngine, TermHit};
use super::text::TextRequest;

const TYPO_CORRECTION_CANDIDATES_PER_TOKEN: usize = 2;
const TYPO_CORRECTION_PLAN_LIMIT: usize = 6;
const TYPO_CORRECTION_EXECUTION_PLAN_LIMIT: usize = 3;
const TYPO_CORRECTION_RELATIVE_SCORE: f64 = 0.5;

/// Engine-lifetime typo settings (`runtimeTypo`,
/// `typoCorrectionExecutionPlanLimit`).
pub struct TypoConfig {
    pub options: MainIndexTypoOptions,
    pub execution_plan_limit: usize,
    pub debug: bool,
}

#[derive(Clone)]
struct TypoCandidate {
    surface: String,
    term: String,
    df: f64,
    distance: u32,
    score: f64,
}

impl TypoCandidate {
    fn to_json(&self) -> Value {
        json!({
            "surface": self.surface,
            "term": self.term,
            "df": num(self.df),
            "distance": self.distance,
            "score": num(self.score)
        })
    }
}

#[derive(Clone)]
struct TypoPlan {
    q: String,
    base_terms: Vec<String>,
    corrections: Value,
    score: f64,
    estimated_upper_bound: f64,
    estimated_range_upper_bound: f64,
    estimated_block_upper_bound: f64,
    estimated_present_base_terms: usize,
    estimated_terms: usize,
}

impl TypoPlan {
    fn to_json(&self, estimated: bool) -> Value {
        let mut map = Map::new();
        map.insert("q".into(), json!(self.q));
        map.insert("baseTerms".into(), json!(self.base_terms));
        map.insert("corrections".into(), self.corrections.clone());
        map.insert("score".into(), num(self.score));
        if estimated {
            map.insert("estimatedUpperBound".into(), num(self.estimated_upper_bound));
            map.insert("estimatedRangeUpperBound".into(), num(self.estimated_range_upper_bound));
            map.insert("estimatedBlockUpperBound".into(), num(self.estimated_block_upper_bound));
            map.insert("estimatedPresentBaseTerms".into(), json!(self.estimated_present_base_terms));
            map.insert("estimatedTerms".into(), json!(self.estimated_terms));
        }
        Value::Object(map)
    }
}

struct Correction {
    plans: Vec<TypoPlan>,
    stats: Vec<(&'static str, Value)>,
}

#[derive(Default)]
struct TypoDebug {
    shards: HashSet<String>,
    candidates: usize,
    scanned: usize,
    tokens: Vec<Value>,
}

/// `a.localeCompare(b)` for the analyzed terms and queries the planner
/// compares. Terms are folded tokens, where ICU root collation agrees with
/// UTF-16 order (space < digits < letters); the engine avoids ICU.
fn locale_compare(left: &str, right: &str) -> Ordering {
    cmp16(left, right)
}

fn response_total(response: &Value) -> f64 {
    let total = to_number(response.get("total").unwrap_or(&NULL));
    if total.is_nan() {
        0.0
    } else {
        total
    }
}

/// `{ ...response, stats: { ...(response.stats || {}), ...extra } }`.
pub(crate) fn merge_stats(response: &mut Value, extra: Vec<(&str, Value)>) {
    if !response.is_object() {
        *response = json!({});
    }
    let map = response.as_object_mut().unwrap();
    let stats = map.entry("stats").or_insert_with(|| json!({}));
    if !stats.is_object() {
        *stats = json!({});
    }
    let stats = stats.as_object_mut().unwrap();
    for (key, value) in extra {
        stats.insert(key.to_string(), value);
    }
}

/// `buildDocRangeUpperBoundPlan(...).candidates[0].upperBound || 0`.
fn doc_range_upper_bound(entries: &[TermHit], base: &HashSet<&str>, min_should_match: usize) -> i64 {
    let mut range_size: Option<i64> = None;
    let mut ranges: Vec<(i64, i64, usize)> = Vec::new();
    let mut positions: HashMap<i64, usize> = HashMap::new();
    for item in entries {
        let Some(doc_ranges) = &item.entry.doc_ranges else { return 0 };
        if doc_ranges.range_size == 0 || doc_ranges.ranges.is_empty() {
            return 0;
        }
        match range_size {
            None => range_size = Some(doc_ranges.range_size),
            Some(size) if size != doc_ranges.range_size => return 0,
            _ => {}
        }
        let is_base = base.contains(item.term.as_str());
        for range in &doc_ranges.ranges {
            let position = *positions.entry(range.index).or_insert_with(|| {
                ranges.push((range.index, 0, 0));
                ranges.len() - 1
            });
            ranges[position].1 += range.max_impact;
            if is_base {
                ranges[position].2 += 1;
            }
        }
    }
    let need = min_should_match.max(1);
    ranges.iter().filter(|(_, upper, hits)| *upper > 0 && *hits >= need).map(|(_, upper, _)| *upper).max().unwrap_or(0)
}

impl IndexEngine {
    pub(crate) fn typo_config(&self) -> Rc<TypoConfig> {
        if let Some(config) = self.extension::<TypoConfig>("typo") {
            return config;
        }
        let options = normalize_main_index_typo_options(&self.options.raw, &self.manifest.borrow());
        let plan_limit = if options.max_query_plans == 0 { TYPO_CORRECTION_PLAN_LIMIT as f64 } else { options.max_query_plans as f64 };
        let searches = if options.max_corrected_searches == 0 {
            TYPO_CORRECTION_EXECUTION_PLAN_LIMIT as f64
        } else {
            options.max_corrected_searches as f64
        };
        let raw = self.options.get("typoCorrectionExecutionPlans");
        let requested = if truthy(raw) { to_number(raw) } else { searches }.floor();
        let limit = plan_limit.min(searches).min(requested);
        let execution_plan_limit = if limit.is_nan() { 0 } else { limit.max(1.0) as usize };
        let debug = truthy(self.options.get("typoDebug"));
        let config = Rc::new(TypoConfig { options, execution_plan_limit, debug });
        self.set_extension("typo", config.clone());
        config
    }

    /// `maybeTypoFallback(params, response, baseTerms, analyzedTerms)`.
    pub(crate) async fn maybe_typo_fallback(&self, request: &TextRequest, params: &Value, response: Value, plan: &QueryPlan) -> Result<Value> {
        let config = self.typo_config();
        if !should_attempt_typo_fallback(&config.options, request, params, &response) {
            return Ok(response);
        }
        if let Some(surface) = self.maybe_surface_exact_fallback(request, &response, &plan.base_terms, &plan.analyzed_terms).await? {
            return Ok(surface);
        }
        let Some(correction) = self.corrected_typo_query(&config, &plan.base_terms, &plan.analyzed_terms).await? else {
            let mut response = response;
            merge_stats(&mut response, vec![("typoAttempted", json!(true)), ("typoApplied", json!(false))]);
            return Ok(response);
        };
        let original_total = response_total(&response);
        let execution_plans: Vec<&TypoPlan> = correction.plans.iter().take(config.execution_plan_limit).collect();
        let mut best: Option<(f64, &TypoPlan, Value)> = None;
        for plan in &execution_plans {
            let corrected_request = TextRequest {
                q: plan.q.clone(),
                base_terms: plan.base_terms.clone(),
                terms: expanded_terms_from_base_terms(&plan.base_terms),
                ..request.clone()
            };
            let corrected = self.run_skipped_text_search(&corrected_request).await?;
            let corrected_total = response_total(&corrected);
            if corrected_total <= original_total {
                continue;
            }
            let value = plan.score + corrected_total.min(20.0) * 0.05;
            if best.as_ref().map(|(current, _, _)| value > *current).unwrap_or(true) {
                best = Some((value, plan, corrected));
            }
        }
        let executed = execution_plans.len();
        let Some((_, best_plan, best_response)) = best else {
            let mut response = response;
            let mut extra = vec![("typoAttempted", json!(true)), ("typoApplied", json!(false))];
            extra.extend(correction.stats.iter().cloned());
            extra.push(("typoCorrectionPlansExecuted", json!(executed)));
            merge_stats(&mut response, extra);
            return Ok(response);
        };
        if !typo_correction_should_replace(original_total, response_total(&best_response)) {
            let mut response = response;
            response["suggestedQuery"] = json!(best_plan.q);
            response["suggestions"] = json!([{ "q": best_plan.q, "corrections": best_plan.corrections }]);
            let mut extra = vec![
                ("typoAttempted", json!(true)),
                ("typoApplied", json!(false)),
                ("typoSuggested", json!(true)),
                ("typoOriginalTotal", response.get("total").cloned().unwrap_or(NULL.clone())),
                ("typoSuggestedQuery", json!(best_plan.q)),
                ("typoCorrectionPlansExecuted", json!(executed)),
                ("typoCorrectedUpperBound", num(best_plan.estimated_upper_bound)),
            ];
            extra.extend(correction.stats.iter().cloned());
            merge_stats(&mut response, extra);
            return Ok(response);
        }
        let mut corrected = best_response;
        corrected["correctedQuery"] = json!(best_plan.q);
        corrected["corrections"] = best_plan.corrections.clone();
        let mut extra = vec![
            ("typoAttempted", json!(true)),
            ("typoApplied", json!(true)),
            ("typoOriginalTotal", response.get("total").cloned().unwrap_or(NULL.clone())),
            ("typoCorrectedQuery", json!(best_plan.q)),
            ("typoCorrections", best_plan.corrections.clone()),
            ("typoCorrectionPlansExecuted", json!(executed)),
            ("typoCorrectedUpperBound", num(best_plan.estimated_upper_bound)),
        ];
        extra.extend(correction.stats.iter().cloned());
        merge_stats(&mut corrected, extra);
        Ok(corrected)
    }

    /// `maybeSurfaceExactFallback`: retry with the raw surface forms of
    /// stemmed tokens when the stemmed query found nothing.
    async fn maybe_surface_exact_fallback(
        &self,
        request: &TextRequest,
        response: &Value,
        base_terms: &[String],
        analyzed_terms: &[AnalyzedTerm],
    ) -> Result<Option<Value>> {
        let original_total = response_total(response);
        if request.page != 1.0 || original_total > 0.0 {
            return Ok(None);
        }
        if !analyzed_terms.iter().any(|item| !item.raw.is_empty() && item.raw != item.term) {
            return Ok(None);
        }
        let mut raw_terms: Vec<String> = Vec::new();
        for item in analyzed_terms {
            if !item.raw.is_empty() && !raw_terms.contains(&item.raw) {
                raw_terms.push(item.raw.clone());
            }
        }
        let present: HashSet<String> = self.term_entries(&raw_terms).await?.into_iter().map(|hit| hit.term).collect();
        let Some(fallback_terms) = raw_surface_fallback_terms(base_terms, analyzed_terms, &present) else { return Ok(None) };
        let fallback_request = TextRequest {
            q: fallback_terms.join(" "),
            base_terms: fallback_terms.clone(),
            terms: expanded_terms_from_base_terms(&fallback_terms),
            ..request.clone()
        };
        let mut fallback = self.run_skipped_text_search(&fallback_request).await?;
        if response_total(&fallback) <= original_total {
            return Ok(None);
        }
        fallback["surfaceFallbackQuery"] = json!(fallback_terms.join(" "));
        merge_stats(
            &mut fallback,
            vec![
                ("surfaceFallbackAttempted", json!(true)),
                ("surfaceFallbackApplied", json!(true)),
                ("surfaceFallbackTerms", json!(fallback_terms)),
                ("typoAttempted", json!(false)),
                ("typoApplied", json!(false)),
                ("typoSkippedReason", json!("surface-exact")),
            ],
        );
        Ok(Some(fallback))
    }

    /// `correctedTypoQuery(baseTerms, analyzedTerms)`.
    async fn corrected_typo_query(
        &self,
        config: &TypoConfig,
        base_terms: &[String],
        analyzed_terms: &[AnalyzedTerm],
    ) -> Result<Option<Correction>> {
        if base_terms.is_empty() || base_terms.len() > 8 {
            return Ok(None);
        }
        let mut present: HashMap<String, f64> = HashMap::new();
        for hit in self.term_entries(base_terms).await? {
            present.insert(hit.term.clone(), hit.entry.df as f64);
        }
        let has_missing = base_terms.iter().any(|term| !present.contains_key(term));
        let mut plans: Vec<TypoPlan> = Vec::new();
        let mut debug = TypoDebug::default();
        let mut indexes: Vec<usize> =
            (0..analyzed_terms.len()).filter(|index| !has_missing || !present.contains_key(&analyzed_terms[*index].term)).collect();
        let df_of = |index: usize| present.get(&analyzed_terms[index].term).copied().unwrap_or(-1.0);
        indexes.sort_by(|left, right| {
            let diff = df_of(*left) - df_of(*right);
            if diff < 0.0 {
                Ordering::Less
            } else if diff > 0.0 {
                Ordering::Greater
            } else {
                left.cmp(right)
            }
        });
        for index in indexes {
            let item = &analyzed_terms[index];
            let candidates = self.typo_candidates_for_token(config, item, &mut debug).await?;
            if config.debug {
                debug.tokens.push(json!({
                    "token": item.term,
                    "candidates": candidates.iter().map(TypoCandidate::to_json).collect::<Vec<_>>()
                }));
            }
            let strong: Vec<&TypoCandidate> = candidates.iter().filter(|c| c.score >= 0.5).collect();
            let best_score = strong.first().map(|c| c.score).unwrap_or(0.0);
            let min_score = if best_score >= 1.0 { best_score * TYPO_CORRECTION_RELATIVE_SCORE } else { 0.5 };
            let per_token = if config.options.max_token_candidates == 0 {
                TYPO_CORRECTION_CANDIDATES_PER_TOKEN
            } else {
                config.options.max_token_candidates as usize
            };
            for candidate in strong.into_iter().filter(|c| c.score >= min_score).take(per_token) {
                let mut corrected = base_terms.to_vec();
                if index >= corrected.len() {
                    corrected.resize(index + 1, String::new());
                }
                corrected[index] = candidate.term.clone();
                if corrected[index] == item.term {
                    continue;
                }
                let plan = TypoPlan {
                    q: corrected.join(" "),
                    base_terms: corrected,
                    corrections: json!([{
                        "from": item.raw,
                        "to": candidate.term,
                        "surface": candidate.surface,
                        "distance": candidate.distance,
                        "df": num(candidate.df),
                        "score": num(super::authority_lane::js_to_fixed(candidate.score, 3))
                    }]),
                    score: candidate.score,
                    estimated_upper_bound: 0.0,
                    estimated_range_upper_bound: 0.0,
                    estimated_block_upper_bound: 0.0,
                    estimated_present_base_terms: 0,
                    estimated_terms: 0,
                };
                match plans.iter_mut().find(|existing| existing.q == plan.q) {
                    Some(existing) => {
                        if plan.score > existing.score {
                            *existing = plan;
                        }
                    }
                    None => plans.push(plan),
                }
            }
        }
        plans.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal).then_with(|| locale_compare(&a.q, &b.q)));
        if plans.is_empty() {
            return Ok(None);
        }
        let limit = if config.options.max_query_plans == 0 { TYPO_CORRECTION_PLAN_LIMIT } else { config.options.max_query_plans as usize };
        let selected = self.rank_typo_correction_plans(plans.iter().take(limit).cloned().collect()).await?;
        let mut stats: Vec<(&'static str, Value)> = vec![
            ("typoCandidateTerms", json!(debug.candidates)),
            ("typoCorrectionPlans", json!(selected.len())),
            ("typoCorrectionPlansEstimated", json!(selected.len())),
            ("typoCorrectionBestUpperBound", num(selected.first().map(|p| p.estimated_upper_bound).unwrap_or(0.0))),
            ("typoShardLookups", json!(debug.shards.len())),
            ("typoCandidateShardLookups", json!(debug.shards.len())),
            ("typoCandidateTermsScanned", json!(debug.scanned)),
        ];
        if config.debug {
            stats.push(("typoDebugTokens", Value::Array(debug.tokens)));
            stats.push(("typoDebugPlans", Value::Array(plans.iter().map(|p| p.to_json(false)).collect())));
        }
        Ok(Some(Correction { plans: selected, stats }))
    }

    /// `typoCandidatesForToken(item, debug)`.
    async fn typo_candidates_for_token(&self, config: &TypoConfig, item: &AnalyzedTerm, debug: &mut TypoDebug) -> Result<Vec<TypoCandidate>> {
        let raw = item.raw.as_str();
        let token = if item.term.is_empty() { raw } else { item.term.as_str() };
        let scoring = if is_typo_correction_token(raw) { raw } else { token };
        if !is_typo_correction_token(scoring) || !is_typo_correction_token(token) {
            return Ok(Vec::new());
        }
        let runtime = &config.options;
        let max_edits = typo_max_edits_for(scoring, runtime);
        let probe_options = MainIndexTypoOptions { max_shard_lookups: (runtime.max_shard_lookups * 2).max(24), ..runtime.clone() };
        let probes = main_index_typo_probe_values(raw, token, &probe_options);
        let mut by_shard: Vec<(String, Pointer)> = Vec::new();
        for probe in &probes {
            let resolved = self
                .resolve_directory_shard(probe, &self.term_directory, self.info.base_shard_depth, self.info.max_shard_depth)
                .await?;
            let Some((shard, entry)) = resolved else { continue };
            if by_shard.iter().any(|(name, _)| *name == shard) {
                continue;
            }
            if !debug.shards.contains(&shard) && debug.shards.len() + by_shard.len() >= runtime.max_shard_lookups as usize {
                break;
            }
            by_shard.push((shard, entry));
        }
        let loaded = self.load_shards(&by_shard).await?;
        let token_len = len16(token) as i64;
        let scoring_len = len16(scoring);
        let scoring_first = scoring.encode_utf16().next();
        let mut candidates: Vec<TypoCandidate> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        for (shard, _) in &by_shard {
            debug.shards.insert(shard.clone());
            let Some((_, data)) = loaded.iter().find(|(name, _)| name == shard) else { continue };
            for (candidate_term, entry) in &data.terms {
                debug.scanned += 1;
                if candidate_term == token || !is_typo_correction_token(candidate_term) {
                    continue;
                }
                if (len16(candidate_term) as i64 - token_len).abs() > max_edits as i64 + 4 {
                    continue;
                }
                let overlap = ngram_overlap(scoring, candidate_term).max(ngram_overlap(token, candidate_term));
                let threshold = if scoring_len <= 5 { 0.2 } else { 0.25 };
                if overlap < threshold && scoring_first != candidate_term.encode_utf16().next() {
                    continue;
                }
                let best = best_main_index_typo_distance(scoring, candidate_term, max_edits);
                let mut distance = best.distance;
                let mut surface = best.surface;
                if distance > max_edits && token != scoring {
                    let term_distance = best_main_index_typo_distance(token, candidate_term, max_edits);
                    distance = term_distance.distance;
                    surface = term_distance.surface;
                }
                if distance == 0 || distance > max_edits {
                    continue;
                }
                let df = entry.df as f64;
                let score = main_index_typo_candidate_score(scoring, &surface, df, distance as f64);
                let candidate = TypoCandidate { surface, term: candidate_term.clone(), df, distance, score };
                match positions.get(candidate_term) {
                    Some(&position) => {
                        let previous = &candidates[position];
                        if candidate.score > previous.score || (candidate.score == previous.score && candidate.df > previous.df) {
                            candidates[position] = candidate;
                        }
                    }
                    None => {
                        positions.insert(candidate_term.clone(), candidates.len());
                        candidates.push(candidate);
                    }
                }
            }
        }
        candidates.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| a.distance.cmp(&b.distance))
                .then_with(|| b.df.partial_cmp(&a.df).unwrap_or(Ordering::Equal))
                .then_with(|| locale_compare(&a.term, &b.term))
        });
        candidates.truncate(runtime.max_token_candidates as usize);
        debug.candidates += candidates.len();
        Ok(candidates)
    }

    /// `rankTypoCorrectionPlans(plans)`: order plans by their cheapest
    /// provable score upper bound.
    async fn rank_typo_correction_plans(&self, plans: Vec<TypoPlan>) -> Result<Vec<TypoPlan>> {
        let futures = plans.iter().map(|plan| async move {
            let terms = expanded_terms_from_base_terms(&plan.base_terms);
            self.term_entries(&terms).await
        });
        let all_entries = crate::rt::try_join_all(futures).await?;
        let mut estimated: Vec<TypoPlan> = plans
            .into_iter()
            .zip(all_entries)
            .map(|(mut plan, entries)| {
                let base: HashSet<&str> = plan.base_terms.iter().map(String::as_str).collect();
                let range = doc_range_upper_bound(&entries, &base, min_should_match_for(plan.base_terms.len())) as f64;
                let block: f64 = entries
                    .iter()
                    .map(|item| item.entry.blocks.iter().fold(0i64, |max, block| max.max(block.max_impact)) as f64)
                    .sum();
                let present = entries.iter().filter(|item| base.contains(item.term.as_str())).count();
                plan.estimated_upper_bound = range.max(block);
                plan.estimated_range_upper_bound = range;
                plan.estimated_block_upper_bound = block;
                plan.estimated_present_base_terms = present;
                plan.estimated_terms = entries.len();
                plan
            })
            .collect();
        estimated.sort_by(|a, b| {
            b.estimated_upper_bound
                .partial_cmp(&a.estimated_upper_bound)
                .unwrap_or(Ordering::Equal)
                .then_with(|| b.estimated_present_base_terms.cmp(&a.estimated_present_base_terms))
                .then_with(|| b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal))
                .then_with(|| locale_compare(&a.q, &b.q))
        });
        Ok(estimated)
    }
}

/// `shouldAttemptTypoFallback(params, response)`.
fn should_attempt_typo_fallback(options: &MainIndexTypoOptions, request: &TextRequest, params: &Value, response: &Value) -> bool {
    if options.mode == "off" {
        return false;
    }
    if params.get("typo") == Some(&Value::Bool(false)) {
        return false;
    }
    if request.page != 1.0 || truthy(&request.sort) {
        return false;
    }
    let total = to_number(response.get("total").unwrap_or(&NULL));
    if total == 0.0 {
        return true;
    }
    if options.trigger != "zero-or-weak" {
        return false;
    }
    total <= options.weak_result_total as f64
}

/// `typoCorrectionShouldReplace(original, corrected)`.
fn typo_correction_should_replace(original: f64, corrected: f64) -> bool {
    if original <= 0.0 {
        return corrected > 0.0;
    }
    corrected >= (original + 2.0).max(original * 1.5)
}

/// `rawSurfaceFallbackTerms(baseTerms, analyzedTerms, presentTerms)`.
fn raw_surface_fallback_terms(base_terms: &[String], analyzed_terms: &[AnalyzedTerm], present: &HashSet<String>) -> Option<Vec<String>> {
    let mut changed = false;
    let terms: Vec<String> = base_terms
        .iter()
        .enumerate()
        .map(|(index, term)| match analyzed_terms.get(index) {
            Some(item) if !item.raw.is_empty() && item.raw != *term && present.contains(&item.raw) => {
                changed = true;
                item.raw.clone()
            }
            _ => term.clone(),
        })
        .collect();
    if changed {
        Some(terms)
    } else {
        None
    }
}
