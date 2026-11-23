//! Authority caches and the place resolvers of `query.js`:
//! `resolveLocality`, the ambiguous/named category-locality probes, global
//! named text, intersections and street + locality addresses.

use std::rc::Rc;

use serde_json::{json, Value};

use super::core::*;
use super::deps::{default_category_lexicon, fold, fold_value, build_category_lexicon, CategoryLexicon};
use super::host::{Ctx, ExactAuthority, Locality};
use super::js::*;
use crate::error::Result;
use crate::rt::{join2, join_all, Promise};

/// `value || fallback` for a JSON value with a string fallback.
pub fn value_or(value: Option<&Value>, fallback: &str) -> Value {
    match value {
        Some(value) if truthy_value(value) => value.clone(),
        _ => Value::String(fallback.to_string()),
    }
}

/// `String(value || fallback)`.
pub fn string_or(value: Option<&Value>, fallback: &str) -> String {
    js_string(Some(&value_or(value, fallback)))
}

// ------------------------------------------------------------ caches

/// `engineCategoryLexicon(engine)`.
pub async fn engine_category_lexicon(ctx: &Ctx<'_>) -> Rc<CategoryLexicon> {
    let existing = ctx.caches.category_lexicon.borrow().clone();
    if let Some(promise) = existing {
        return promise.await.unwrap_or_else(|_| default_category_lexicon());
    }
    let promise: Promise<Rc<CategoryLexicon>> = Promise::pending();
    *ctx.caches.category_lexicon.borrow_mut() = Some(promise.clone());
    let lexicon = load_category_lexicon(ctx).await;
    promise.resolve(lexicon.clone());
    lexicon
}

async fn load_category_lexicon(ctx: &Ctx<'_>) -> Rc<CategoryLexicon> {
    let embedded = at(&ctx.manifest, "category_lexicon");
    if field(embedded, "types").and_then(Value::as_array).map(|types| !types.is_empty()).unwrap_or(false) {
        return Rc::new(build_category_lexicon(Some(embedded)));
    }
    let sharded = truthy_value(at(at(&ctx.manifest, "features"), "shards"));
    if !sharded && ctx.has("loadFacetValues") {
        if let Ok(values) = ctx.engine.call("loadFacetValues", json!({ "field": "type" })).await {
            if values.as_array().map(|items| !items.is_empty()).unwrap_or(false) {
                return Rc::new(build_category_lexicon(Some(&values)));
            }
        }
    }
    default_category_lexicon()
}

/// `authorityLookupCached(engine, surface)`; `Value::Null` when the engine
/// has no `authorityLookup`.
pub async fn authority_lookup_cached(ctx: &Ctx<'_>, surface: &str) -> Result<Value> {
    if !ctx.has("authorityLookup") {
        return Ok(Value::Null);
    }
    let key = fold(surface);
    let existing = ctx.caches.authority_lookup.get(&key);
    if let Some(promise) = existing {
        return promise.await;
    }
    let promise: Promise<Value> = Promise::pending();
    ctx.caches.authority_lookup.set(key.clone(), promise.clone());
    let result = ctx.authority_lookup(surface).await;
    if result.is_err() {
        ctx.caches.authority_lookup.evict_if_same(&key, &promise);
    }
    promise.settle(result.clone());
    result
}

/// `exactAuthorityScope(engine, surface)`.
pub async fn exact_authority_scope(ctx: &Ctx<'_>, surface: &str) -> Option<ExactAuthority> {
    if !ctx.has("authorityLookup") {
        return None;
    }
    let key = fold(surface);
    let existing = ctx.caches.authority_exact.get(&key);
    if let Some(promise) = existing {
        return promise.await.ok().flatten();
    }
    let promise: Promise<Option<ExactAuthority>> = Promise::pending();
    ctx.caches.authority_exact.set(key.clone(), promise.clone());
    let exact = match authority_lookup_cached(ctx, surface).await {
        Ok(lookup) => exact_authority_from_lookup(&lookup, &key),
        Err(_) => {
            // A failed read is not an answer: evict it so the next query
            // retries instead of losing the shortcut for this surface.
            ctx.caches.authority_exact.evict_if_same(&key, &promise);
            None
        }
    };
    promise.resolve(exact.clone());
    exact
}

// ------------------------------------------------------------ resolveLocality

#[derive(Default, Clone)]
pub struct LocalityOptions {
    pub authority_miss_is_final: bool,
    /// `options.authorityLookup` (falsy values trigger a fresh read).
    pub authority_lookup: Value,
}

impl LocalityOptions {
    pub fn final_miss() -> Self {
        LocalityOptions { authority_miss_is_final: true, authority_lookup: Value::Null }
    }
}

fn shards_param(params: &Value) -> Option<&Value> {
    field(params, "shards").filter(|value| !value.is_null())
}

/// `resolveLocality(engine, surface, params, options)`.
pub async fn resolve_locality(ctx: &Ctx<'_>, surface: &str, params: &Value, options: LocalityOptions) -> Result<Option<Locality>> {
    let normalized = fold(surface);
    let shard_scope: Vec<String> = match shards_param(params) {
        None => Vec::new(),
        Some(Value::Array(items)) => sorted16(items.iter().map(|item| js_string(Some(item))).collect()),
        Some(other) => vec![js_string(Some(other))],
    };
    let locality_key = format!("{normalized}\0{}", shard_scope.join("\0"));
    let trace_param = field(params, "trace").filter(|v| truthy_value(v)).cloned();
    let cached = ctx.caches.locality.ready(&locality_key);
    if let Some(cached) = cached {
        let Some(doc) = cached else { return Ok(None) };
        let search_stats = trace_param.as_ref().map(|_| json!({ "trace": { "totalMs": 0, "totalBytes": 0, "spans": [] } }));
        return Ok(Some(Locality { doc, search_stats }));
    }
    let mut authority_lookup = options.authority_lookup.clone();
    if !truthy_value(&authority_lookup) && shard_scope.is_empty() && ctx.has("authorityLookup") {
        if let Ok(lookup) = authority_lookup_cached(ctx, surface).await {
            authority_lookup = lookup;
        }
    }
    let exact_matches_of = |lookup: &Value, key: &str| -> Vec<Value> {
        match field(lookup, "matches") {
            Some(Value::Array(items)) => items.iter().filter(|item| fold_value(field(item, "text")) == key).cloned().collect(),
            _ => Vec::new(),
        }
    };
    let weight = |item: &Value| number_or(field(item, "weight"), 0.0);
    let mut exact_matches = exact_matches_of(&authority_lookup, &normalized);
    let mut postal_authority = exact_matches.iter().any(|item| weight(item) == POSTCODE_AUTHORITY_WEIGHT);
    let canadian = match_canadian_postal_code(surface);
    let mut postal_search_surface = trim(surface).to_string();
    if canadian.is_none() && !postal_authority && shard_scope.is_empty() && has_ascii_digit(&postal_search_surface) && ctx.has("authorityLookup") {
        let tokens: Vec<String> = split_ws(&postal_search_surface).into_iter().map(str::to_string).collect();
        let mut length = tokens.len().saturating_sub(1);
        while length >= 1 {
            let prefix = tokens[..length].join(" ");
            let Ok(prefix_lookup) = authority_lookup_cached(ctx, &prefix).await else { break };
            let prefix_key = fold(&prefix);
            let prefix_matches: Vec<Value> =
                exact_matches_of(&prefix_lookup, &prefix_key).into_iter().filter(|item| weight(item) == POSTCODE_AUTHORITY_WEIGHT).collect();
            if prefix_matches.is_empty() {
                length -= 1;
                continue;
            }
            authority_lookup = prefix_lookup;
            exact_matches = prefix_matches;
            postal_authority = true;
            postal_search_surface = prefix;
            break;
        }
    }
    let postal_intent = canadian.is_some() || postal_authority;
    let expected_postal = strip_non_alnum(&fold(&match &canadian {
        Some((first, second)) => format!("{first} {second}"),
        None => postal_search_surface.clone(),
    }));
    let best_match = |results: &[Value]| -> Option<Value> {
        let mut matches: Vec<&Value> = results
            .iter()
            .filter(|result| {
                if finite_number(field(result, "lat")).is_none() || finite_number(field(result, "lon")).is_none() {
                    return false;
                }
                if postal_intent {
                    let actual = strip_non_alnum(&fold_value(field(result, "postcode")));
                    return field(result, "type") == Some(&json!("postal_code")) && actual == expected_postal;
                }
                is_locality_type(field(result, "type")) && fold_value(name_or_title(result)) == normalized
            })
            .collect();
        matches.sort_by(|left, right| {
            let delta = type_priority(field(right, "type")) - type_priority(field(left, "type"));
            if delta != 0.0 {
                return delta.partial_cmp(&0.0).unwrap();
            }
            let delta = number_or(field(right, "population"), 0.0) - number_or(field(left, "population"), 0.0);
            if delta != 0.0 && !delta.is_nan() {
                return delta.partial_cmp(&0.0).unwrap();
            }
            std::cmp::Ordering::Equal
        });
        matches.first().map(|value| (*value).clone())
    };

    let mut authority_shards: Option<Vec<Value>> = None;
    let mut authoritative_miss = false;
    if shard_scope.is_empty() && truthy_value(&authority_lookup) {
        let mut scoped: Vec<Value> = Vec::new();
        let mut exact_match_without_shard = false;
        let best_weight = if exact_matches.is_empty() { None } else { Some(exact_matches.iter().map(weight).fold(f64::NEG_INFINITY, js_max)) };
        for item in &exact_matches {
            if Some(weight(item)) != best_weight {
                continue;
            }
            let shards = field(item, "shards").and_then(Value::as_array).filter(|shards| !shards.is_empty());
            let Some(shards) = shards else {
                exact_match_without_shard = true;
                continue;
            };
            for id in shards {
                if !scoped.iter().any(|seen| same_value_zero(seen, id)) {
                    scoped.push(id.clone());
                }
            }
            if scoped.len() >= 4 {
                break;
            }
        }
        if !scoped.is_empty() {
            scoped.truncate(4);
            scoped.sort_by(|a, b| crate::util::jsstr::cmp16(&js_string(Some(a)), &js_string(Some(b))));
            authority_shards = Some(scoped);
        }
        authoritative_miss = !authority_lookup.is_null() && authority_shards.is_none() && !exact_match_without_shard;
    }
    if options.authority_miss_is_final && authoritative_miss {
        return Ok(None);
    }

    let attempt = |scope: Option<Value>| {
        let postal_search_surface = postal_search_surface.clone();
        let trace_param = trace_param.clone();
        let best_match = &best_match;
        async move {
            let mut request = Obj::new();
            if postal_intent {
                request.insert("q".into(), Value::String(postal_search_surface));
                request.insert("size".into(), json!(8));
            } else {
                request.insert("q".into(), Value::String(surface.to_string()));
                request.insert("filters".into(), json!({ "facets": { "category": ["place"] } }));
                request.insert("size".into(), json!(32));
            }
            if let Some(trace) = &trace_param {
                request.insert("trace".into(), trace.clone());
            }
            if let Some(scope) = &scope {
                request.insert("shards".into(), scope.clone());
            }
            let response = ctx.search(Value::Object(request)).await?;
            let mut resolved = best_match(&results_of(&response));
            let mut resolved_stats = match field(&response, "stats") {
                Some(stats) if truthy_value(stats) => stats.clone(),
                _ => json!({}),
            };
            if !postal_intent && number(field(&response, "total")) > 0.0 && type_priority(resolved.as_ref().and_then(|r| field(r, "type"))) <= 2.0 {
                let mut request = Obj::new();
                request.insert("q".into(), Value::String(surface.to_string()));
                request.insert("filters".into(), json!({ "facets": { "category": ["place"] }, "numbers": { "population": { "min": 25000 } } }));
                request.insert("size".into(), json!(8));
                if let Some(trace) = &trace_param {
                    request.insert("trace".into(), trace.clone());
                }
                if let Some(scope) = &scope {
                    request.insert("shards".into(), scope.clone());
                }
                let populous_response = ctx.search(Value::Object(request)).await?;
                if let Some(populous) = best_match(&results_of(&populous_response)) {
                    resolved = Some(populous);
                    let trace = merge_runtime_traces(&[trace_of(&response), trace_of(&populous_response)]);
                    let mut stats = stats_of(&populous_response);
                    if let Some(trace) = trace {
                        stats.insert("trace".into(), trace);
                    }
                    resolved_stats = Value::Object(stats);
                }
            }
            Ok::<_, crate::error::Error>((resolved, resolved_stats))
        }
    };
    let scope_value = |scope: &[String]| (!scope.is_empty()).then(|| strings(scope));
    let first_scope = match &authority_shards {
        Some(shards) => Some(Value::Array(shards.clone())),
        None => scope_value(&shard_scope),
    };
    let mut outcome = attempt(first_scope).await?;
    if outcome.0.is_none() && authority_shards.is_some() {
        outcome = attempt(scope_value(&shard_scope)).await?;
    }
    let (resolved, resolved_stats) = outcome;
    match resolved {
        Some(Value::Object(doc)) => {
            ctx.caches.locality.set(locality_key, Promise::resolved(Some(doc.clone())));
            Ok(Some(Locality { doc, search_stats: Some(resolved_stats) }))
        }
        _ => {
            ctx.caches.locality.set(locality_key, Promise::resolved(None));
            Ok(None)
        }
    }
}

async fn resolve_pair(
    ctx: &Ctx<'_>,
    whole: &str,
    locality: &str,
    params: &Value,
    whole_options: LocalityOptions,
    locality_options: LocalityOptions,
) -> Result<(Option<Locality>, Option<Locality>)> {
    let (whole_place, split) =
        join2(resolve_locality(ctx, whole, params, whole_options), resolve_locality(ctx, locality, params, locality_options)).await;
    Ok((whole_place?, split?))
}

/// `resolveAmbiguousCategoryLocality(engine, wholeSurface, localitySurface, params)`
/// → `(wholePlace, locality)`.
pub async fn resolve_ambiguous_category_locality(
    ctx: &Ctx<'_>,
    whole: &str,
    locality: &str,
    params: &Value,
) -> Result<(Option<Locality>, Option<Locality>)> {
    if shards_param(params).is_none() && ctx.has("authorityLookup") {
        let (whole_lookup, locality_lookup) = join2(authority_lookup_cached(ctx, whole), authority_lookup_cached(ctx, locality)).await;
        let (whole_lookup, locality_lookup) = match (whole_lookup, locality_lookup) {
            (Ok(a), Ok(b)) => (a, b),
            _ => (Value::Null, Value::Null),
        };
        if whole_lookup.is_null() || locality_lookup.is_null() {
            return resolve_pair(ctx, whole, locality, params, LocalityOptions::final_miss(), LocalityOptions::default()).await;
        }
        let whole_authority = exact_authority_from_lookup(&whole_lookup, whole);
        if let Some(authority) = &whole_authority {
            if authority.shards.is_empty() {
                return resolve_pair(
                    ctx,
                    whole,
                    locality,
                    params,
                    LocalityOptions { authority_miss_is_final: true, authority_lookup: whole_lookup },
                    LocalityOptions { authority_miss_is_final: false, authority_lookup: locality_lookup },
                )
                .await;
            }
            let whole_place =
                resolve_locality(ctx, whole, params, LocalityOptions { authority_miss_is_final: true, authority_lookup: whole_lookup }).await?;
            if whole_place.is_some() {
                return Ok((whole_place, None));
            }
        }
        let split = resolve_locality(ctx, locality, params, LocalityOptions { authority_miss_is_final: false, authority_lookup: locality_lookup }).await?;
        return Ok((None, split));
    }
    resolve_pair(ctx, whole, locality, params, LocalityOptions::final_miss(), LocalityOptions::default()).await
}

// ------------------------------------------------------------ named probes

/// `resolveNamedCategoryLocality(engine, intent, params)`.
pub async fn resolve_named_category_locality(ctx: &Ctx<'_>, intent: &QueryIntent, params: &Value) -> Result<Option<Value>> {
    if intent.has_connector() || intent.order != "category-locality" {
        return Ok(None);
    }
    let tokens = split_ws(&intent.locality);
    if tokens.len() < 2 {
        return Ok(None);
    }
    struct Candidate {
        locality_surface: String,
        name_surface: String,
        distinctive: Vec<String>,
    }
    let max_locality_tokens = 4.min(tokens.len() - 1);
    let mut candidates: Vec<Candidate> = Vec::new();
    for locality_length in (1..=max_locality_tokens).rev() {
        let split = tokens.len() - locality_length;
        let name_tokens = &tokens[..split];
        let distinctive: Vec<String> = name_tokens.iter().map(|token| fold(token)).filter(|token| len16(token) >= 3).collect();
        if distinctive.is_empty() {
            continue;
        }
        candidates.push(Candidate { locality_surface: tokens[split..].join(" "), name_surface: name_tokens.join(" "), distinctive });
    }
    if candidates.is_empty() {
        return Ok(None);
    }
    let full = exact_authority_scope(ctx, &intent.locality).await;
    if full.map(|authority| !authority.shards.is_empty()).unwrap_or(false) {
        return Ok(None);
    }
    let authorities = join_all(candidates.iter().map(|candidate| exact_authority_scope(ctx, &candidate.locality_surface))).await;
    let requested_size = number_or(field(params, "size"), 10.0);
    let explicit = explicit_shard_scope(params);
    let mut proven_miss: Option<Value> = None;
    for (candidate, authority) in candidates.iter().zip(authorities) {
        let Some(authority) = authority.filter(|authority| !authority.shards.is_empty()) else { continue };
        let scope = if explicit.is_empty() { authority.shards.clone() } else { explicit.clone() };
        let facet_safe = shard_scope_supports_feature(ctx, &scope, "facetSummaryUint32");
        let mut request = without(
            &with(
                params,
                vec![
                    ("q", Value::String(candidate.name_surface.clone())),
                    ("typo", json!(false)),
                    ("shards", strings(&scope)),
                    ("size", num(js_min(100.0, js_max(32.0, requested_size * 2.0)))),
                ],
            ),
            &["geo"],
        );
        if facet_safe {
            request = with_type_facet(&request, &intent.category.type_);
        }
        let response = ctx.search(request).await?;
        let category_type = fold(&intent.category.type_);
        let results: Vec<Value> = results_of(&response)
            .into_iter()
            .filter(|result| {
                let name = fold_value(name_or_title(result));
                let compact = compact_fold_value(name_or_title(result));
                let type_or_category = if is_truthy(field(result, "type")) { field(result, "type") } else { field(result, "category") };
                let category_matches = facet_safe || fold_value(type_or_category) == category_type;
                category_matches
                    && candidate.distinctive.iter().all(|token| name.contains(token.as_str()) || compact.contains(&compact_fold(token)))
                    && result_matches_locality(result, &candidate.locality_surface)
            })
            .collect();
        let locality_label = value_or(Some(&authority.text), &candidate.locality_surface);
        let mut stats = vec![
            ("plannerLane", json!("osmNamedCategoryLocality")),
            ("osmIntentCategory", json!(intent.category.query)),
            ("osmIntentCategoryType", json!(intent.category.type_)),
        ];
        if facet_safe {
            stats.push(("osmIntentCategoryFacet", json!(true)));
        }
        stats.extend([
            ("osmIntentLocality", locality_label.clone()),
            ("osmIntentLocalityType", json!("authority")),
            ("osmIntentLocalityAuthority", json!(true)),
            ("osmIntentNameTokens", strings(&candidate.distinctive)),
            ("osmIntentCoverageShards", strings(&scope)),
        ]);
        let found = !results.is_empty();
        let resolved = with_stats(
            &with(
                &response,
                vec![
                    ("total", num(results.len() as f64)),
                    ("size", num(requested_size)),
                    ("results", Value::Array(slice_to(results, requested_size))),
                    (
                        "resolvedQuery",
                        Value::String(format!("{} {} {}", intent.category.label, candidate.name_surface, js_string(Some(&locality_label)))),
                    ),
                ],
            ),
            stats,
        );
        if found {
            return Ok(Some(resolved));
        }
        if proven_miss.is_none() {
            proven_miss = Some(resolved);
        }
    }
    Ok(proven_miss)
}

/// `resolveGlobalNamedText(engine, surface, params, { exactOnly })`.
pub async fn resolve_global_named_text(ctx: &Ctx<'_>, surface: &str, params: &Value, exact_only: bool) -> Result<Option<Value>> {
    if shards_param(params).is_some() || !nullish(field(params, "geo")) {
        return Ok(None);
    }
    let tokens = split_ws(trim(surface));
    if tokens.len() < 2 || tokens.len() > 9 || tokens.iter().any(|token| has_ascii_digit(token)) {
        return Ok(None);
    }
    let requested_size = js_max(1.0, number_or(field(params, "size"), 10.0));
    let folded_surface = fold(surface);
    if let Some(whole) = exact_authority_scope(ctx, surface).await.filter(|authority| !authority.shards.is_empty()) {
        let request = with(
            params,
            vec![
                ("q", Value::String(surface.to_string())),
                ("typo", json!(false)),
                ("shards", strings(&whole.shards)),
                ("size", num(js_min(64.0, js_max(24.0, requested_size * 2.0)))),
            ],
        );
        let response = ctx.search(request).await?;
        let exact_results: Vec<Value> =
            results_of(&response).into_iter().filter(|result| fold_value(name_or_title(result)) == folded_surface).collect();
        if let Some(exact_locality) = exact_results.iter().find(|result| is_locality_type(field(result, "type"))) {
            let locality = Locality { doc: spread(exact_locality), search_stats: None };
            let base = locality_exact_response(&locality, params, surface);
            let mut stats = stats_of(&response);
            stats.insert("plannerLane".into(), json!("osmLocalityExact"));
            stats.insert("osmIntentLocality".into(), value_or(field(exact_locality, "name"), surface));
            stats.insert("osmIntentLocalityType".into(), value_or(field(exact_locality, "type"), ""));
            stats.insert("osmIntentCoverageShards".into(), strings(&whole.shards));
            return Ok(Some(set(base, vec![("stats", Value::Object(stats))])));
        }
        let results: Vec<Value> = exact_results.into_iter().filter(|result| !is_locality_type(field(result, "type"))).collect();
        if !results.is_empty() {
            let out = with(
                &response,
                vec![
                    ("total", num(results.len() as f64)),
                    ("size", num(requested_size)),
                    ("results", Value::Array(slice_to(results, requested_size))),
                ],
            );
            return Ok(Some(with_stats(
                &out,
                vec![
                    ("plannerLane", json!("osmGlobalExactText")),
                    ("osmIntentCoverageShards", strings(&whole.shards)),
                    ("osmIntentAuthorityName", value_or(Some(&whole.text), surface)),
                ],
            )));
        }
    }
    if exact_only {
        return Ok(None);
    }
    let mut candidates: Vec<(String, String)> = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    let max_locality_tokens = 4.min(tokens.len() - 1);
    for locality_length in (1..=max_locality_tokens).rev() {
        for trailing in [true, false] {
            let (locality_tokens, name_tokens) = if trailing {
                (&tokens[tokens.len() - locality_length..], &tokens[..tokens.len() - locality_length])
            } else {
                (&tokens[..locality_length], &tokens[locality_length..])
            };
            let locality_surface = locality_tokens.join(" ");
            let name_surface = name_tokens.join(" ");
            let key = format!("{}\0{}", fold(&locality_surface), fold(&name_surface));
            if name_surface.is_empty() || seen.contains(&key) {
                continue;
            }
            seen.push(key);
            candidates.push((locality_surface, name_surface));
        }
    }
    let authorities = join_all(candidates.iter().map(|(locality, _)| exact_authority_scope(ctx, locality))).await;
    let mut proven_miss: Option<Value> = None;
    for ((locality_surface, name_surface), authority) in candidates.iter().zip(authorities) {
        let Some(authority) = authority.filter(|authority| !authority.shards.is_empty()) else { continue };
        let request = with(
            params,
            vec![
                ("q", Value::String(name_surface.clone())),
                ("shards", strings(&authority.shards)),
                ("size", num(js_min(64.0, js_max(24.0, requested_size * 2.0)))),
            ],
        );
        let response = ctx.search(request).await?;
        let locality_label = value_or(Some(&authority.text), locality_surface);
        let locality_text = js_string(Some(&locality_label));
        let results: Vec<Value> = results_of(&response)
            .into_iter()
            .filter(|result| result_name_matches_surface(result, name_surface, true) && result_matches_locality(result, &locality_text))
            .collect();
        let found = !results.is_empty();
        let resolved = with_stats(
            &with(
                &response,
                vec![
                    ("total", num(results.len() as f64)),
                    ("size", num(requested_size)),
                    ("results", Value::Array(slice_to(results, requested_size))),
                    ("resolvedQuery", Value::String(format!("{name_surface}, {locality_text}"))),
                ],
            ),
            vec![
                ("plannerLane", json!("osmNamedTextLocality")),
                ("osmIntentLocality", locality_label),
                ("osmIntentLocalityAuthority", json!(true)),
                ("osmIntentName", Value::String(name_surface.clone())),
                ("osmIntentCoverageShards", strings(&authority.shards)),
            ],
        );
        if found {
            return Ok(Some(resolved));
        }
        if proven_miss.is_none() {
            proven_miss = Some(resolved);
        }
    }
    Ok(proven_miss)
}

// ------------------------------------------------------------ intersections

fn authority_locality(authority: &ExactAuthority, surface: &str) -> Locality {
    let mut doc = Obj::new();
    doc.insert("name".into(), value_or(Some(&authority.text), surface));
    doc.insert("type".into(), json!("authority"));
    doc.insert("shard".into(), Value::String(authority.shards[0].clone()));
    Locality { doc, search_stats: None }
}

fn locality_shard(locality: &Locality) -> String {
    let shard = string_or(locality.get("shard"), "");
    shard.split('/').next().unwrap_or("").to_string()
}

fn lat_lon_of(result: &Value) -> Option<(f64, f64)> {
    Some((finite_number(field(result, "lat"))?, finite_number(field(result, "lon"))?))
}

/// Haversine from a locality whose coordinates may be missing (NaN).
fn distance_from(locality: &Locality, lat: f64, lon: f64) -> f64 {
    haversine_meters(number(locality.get("lat")), number(locality.get("lon")), lat, lon)
}

fn miss_response(params: &Value, stats: Obj) -> Value {
    json!({
        "total": 0,
        "page": 1,
        "size": num(number_or(field(params, "size"), 10.0)),
        "approximate": false,
        "results": [],
        "stats": Value::Object(stats)
    })
}

/// `resolveIntersectionLocality(engine, surface, params, anchor)`.
pub async fn resolve_intersection_locality(ctx: &Ctx<'_>, surface: &str, params: &Value, anchor: Option<LatLon>) -> Result<Option<Value>> {
    let Some((left, right)) = parse_intersection_surface(surface) else { return Ok(None) };
    let right_tokens: Vec<String> = split_ws(&right).into_iter().map(str::to_string).collect();
    let coverage = match anchor {
        Some(anchor) if shards_param(params).is_none() => anchor_coverage_shards(ctx, anchor),
        _ => Vec::new(),
    };
    let routed_params = if coverage.is_empty() { params.clone() } else { with(params, vec![("shards", strings(&coverage))]) };
    let max_locality_tokens = 4i64.min(right_tokens.len() as i64 - 2);
    let mut found: Option<(Locality, ParsedStreet)> = None;
    let mut locality_length = 1i64;
    while locality_length <= max_locality_tokens {
        let split = right_tokens.len() - locality_length as usize;
        locality_length += 1;
        let Some(candidate_street) = parsed_street_surface(&right_tokens[..split].join(" ")) else { continue };
        let locality_surface = right_tokens[split..].join(" ");
        let candidate = match exact_authority_scope(ctx, &locality_surface).await.filter(|authority| !authority.shards.is_empty()) {
            Some(authority) => Some(authority_locality(&authority, &locality_surface)),
            None => resolve_locality(ctx, &locality_surface, &routed_params, LocalityOptions::final_miss()).await?,
        };
        if let Some(candidate) = candidate {
            found = Some((candidate, candidate_street));
            break;
        }
    }
    let Some((mut locality, right_street)) = found else {
        let mut stats = Obj::new();
        stats.insert("plannerLane".into(), json!("osmIntersectionMiss"));
        if !coverage.is_empty() {
            stats.insert("osmIntentCoverageShards".into(), strings(&coverage));
        }
        return Ok(Some(miss_response(params, stats)));
    };
    let shard = locality_shard(&locality);
    let locality_name_value = match locality.get("name") {
        Some(name) if truthy_value(name) => name.clone(),
        _ => Value::String(right_tokens.last().cloned().unwrap_or_default()),
    };
    let locality_name = js_string(Some(&locality_name_value));
    let intersection_scope = if shard.is_empty() { coverage.clone() } else { vec![shard.clone()] };
    let size = number_or(field(params, "size"), 10.0);
    let mut request = without(
        &with(
            params,
            vec![
                ("q", Value::String(trim(&format!("{} {} {}", left.core, right_street.core, locality_name)).to_string())),
                ("typo", json!(false)),
                ("size", num(js_max(20.0, size))),
            ],
        ),
        &["geo"],
    );
    if !intersection_scope.is_empty() {
        request = set(request, vec![("shards", strings(&intersection_scope))]);
    }
    let direct = ctx.search(request).await?;
    let street_tokens_match = |name: Option<&Value>, street: &ParsedStreet| {
        let haystack_text = fold_value(name);
        let haystack = alnum_tokens(&haystack_text);
        let needles_text = fold(&street.core);
        let needles = alnum_tokens(&needles_text);
        !needles.is_empty() && needles.iter().all(|token| haystack.contains(token))
    };
    let folded_locality_name = fold(&locality_name);
    let explicit = results_of(&direct).into_iter().find(|result| {
        let Some((lat, lon)) = lat_lon_of(result) else { return false };
        street_tokens_match(name_or_title(result), &left)
            && street_tokens_match(name_or_title(result), &right_street)
            && (fold_value(field(result, "city")) == folded_locality_name
                || distance_from(&locality, lat, lon) <= locality_radius_meters(locality.get("type")))
    });
    if let Some(explicit) = explicit {
        let name = name_or_title(&explicit).cloned().unwrap_or(Value::Null);
        let trace = merge_runtime_traces(&[locality.trace(), trace_of(&direct)]);
        let (lat, lon) = lat_lon_of(&explicit).unwrap();
        let mut entries = vec![
            ("name", name.clone()),
            ("title", name.clone()),
            ("type", value_or(field(&explicit, "type"), "intersection")),
            ("category", value_or(field(&explicit, "category"), "highway")),
        ];
        if let Some(anchor) = anchor {
            entries.push(("distanceMeters", num(haversine_meters(anchor.lat, anchor.lon, lat, lon))));
        }
        let result = set(explicit, entries);
        let mut stats = stats_of(&direct);
        if let Some(trace) = trace {
            stats.insert("trace".into(), trace);
        }
        stats.insert("plannerLane".into(), json!("osmIntersectionDocument"));
        stats.insert("osmIntentLocality".into(), locality_name_value.clone());
        stats.insert("osmIntentStreets".into(), json!([left.text, right_street.text]));
        if !intersection_scope.is_empty() {
            stats.insert("osmIntentCoverageShards".into(), strings(&intersection_scope));
        }
        return Ok(Some(with(
            &direct,
            vec![
                ("total", json!(1)),
                ("size", num(size)),
                ("approximate", json!(false)),
                ("results", json!([result])),
                ("resolvedQuery", Value::String(format!("{}, {}", js_string(Some(&name)), locality_name))),
                ("stats", Value::Object(stats)),
            ],
        )));
    }

    if finite_number(locality.get("lat")).is_none() || finite_number(locality.get("lon")).is_none() {
        if let Some(resolved) = resolve_locality(ctx, &locality_name, &routed_params, LocalityOptions::final_miss()).await? {
            locality = resolved;
        }
    }

    let search_street = |street: &ParsedStreet| {
        let mut scoped = without(&with(params, vec![("size", json!(100)), ("typo", json!(false))]), &["geo"]);
        if !shard.is_empty() {
            scoped = set(scoped, vec![("shards", json!([shard.clone()]))]);
        } else if !coverage.is_empty() {
            scoped = set(scoped, vec![("shards", strings(&coverage))]);
        }
        let core = street.core.clone();
        let locality_name = locality_name.clone();
        async move {
            let local = ctx.search(set(scoped.clone(), vec![("q", Value::String(trim(&format!("{core} {locality_name}")).to_string()))])).await?;
            if number(field(&local, "total")) > 0.0 {
                return Ok(local);
            }
            let primary = ctx.search(set(scoped.clone(), vec![("q", Value::String(core.clone()))])).await?;
            if number(field(&primary, "total")) > 0.0 {
                return Ok(primary);
            }
            let mut terms: Vec<&str> = split_non_alnum(&core).into_iter().filter(|token| code_points(token) >= 3).collect();
            terms.sort_by(|a, b| code_points(b).cmp(&code_points(a)));
            let Some(fallback_term) = terms.first().map(|term| term.to_string()) else { return Ok(primary) };
            if fold(&fallback_term) == fold(&core) {
                return Ok(primary);
            }
            let fallback = ctx.search(set(scoped, vec![("q", Value::String(fallback_term.clone()))])).await?;
            let trace = merge_runtime_traces(&[trace_of(&primary), trace_of(&fallback)]);
            let mut entries = Vec::new();
            if let Some(trace) = trace {
                entries.push(("trace", trace));
            }
            entries.push(("osmIntersectionStreetFallback", Value::String(fallback_term)));
            Ok::<_, crate::error::Error>(set_stats(fallback, entries))
        }
    };
    let (left_response, right_response) = join2(search_street(&left), search_street(&right_street)).await;
    let (left_response, right_response) = (left_response?, right_response?);
    let radius = locality_radius_meters(locality.get("type"));
    let street_points = |response: &Value, street: &ParsedStreet| -> Vec<(f64, f64)> {
        results_of(response)
            .iter()
            .filter_map(|result| {
                let (lat, lon) = lat_lon_of(result)?;
                let near = fold_value(field(result, "city")) == folded_locality_name || distance_from(&locality, lat, lon) <= radius;
                let street_field = fold_value(field(result, "street"));
                let named = fold_value(name_or_title(result)) == street.key || street_field == street.key || street_field.starts_with(&format!("{} ", street.key));
                (near && named).then_some((lat, lon))
            })
            .collect()
    };
    let left_points = street_points(&left_response, &left);
    let right_points = street_points(&right_response, &right_street);
    let mut nearest: Option<((f64, f64), (f64, f64), f64)> = None;
    for l in &left_points {
        for r in &right_points {
            let distance = haversine_meters(l.0, l.1, r.0, r.1);
            if nearest.map(|n| distance < n.2).unwrap_or(true) {
                nearest = Some((*l, *r, distance));
            }
        }
    }
    let trace = merge_runtime_traces(&[locality.trace(), trace_of(&left_response), trace_of(&right_response)]);
    let nearest = match nearest {
        Some(nearest) if !(nearest.2 > 2000.0) => nearest,
        other => {
            let mut stats = Obj::new();
            if let Some(trace) = trace {
                stats.insert("trace".into(), trace);
            }
            stats.insert("plannerLane".into(), json!("osmIntersectionMiss"));
            if let Some(name) = locality.get("name") {
                stats.insert("osmIntentLocality".into(), name.clone());
            }
            stats.insert("osmIntentIntersectionCandidateDistanceMeters".into(), other.map(|n| num(n.2)).unwrap_or(Value::Null));
            if !shard.is_empty() {
                stats.insert("osmIntentCoverageShards".into(), json!([shard]));
            }
            return Ok(Some(miss_response(params, stats)));
        }
    };
    let lat = (nearest.0 .0 + nearest.1 .0) / 2.0;
    let lon = (nearest.0 .1 + nearest.1 .1) / 2.0;
    let name = format!("{} & {}", left.text, right_street.text);
    let city_value = value_or(locality.get("name"), "");
    let city = js_string(Some(&city_value));
    let address = if city.is_empty() { name.clone() } else { format!("{name}, {city}") };
    let mut result = Obj::new();
    result.insert("id".into(), Value::String(format!("intersection:{shard}:{}", fold(&name))));
    result.insert("name".into(), Value::String(name.clone()));
    result.insert("title".into(), Value::String(name.clone()));
    result.insert("address".into(), Value::String(address.clone()));
    result.insert("city".into(), city_value.clone());
    result.insert("category".into(), json!("highway"));
    result.insert("type".into(), json!("intersection"));
    result.insert("shard".into(), Value::String(shard.clone()));
    result.insert("lat".into(), num(lat));
    result.insert("lon".into(), num(lon));
    if let Some(anchor) = anchor {
        result.insert("distanceMeters".into(), num(haversine_meters(anchor.lat, anchor.lon, lat, lon)));
    }
    let mut stats = stats_of(&left_response);
    if let Some(trace) = trace {
        stats.insert("trace".into(), trace);
    }
    stats.insert("plannerLane".into(), json!("osmIntersectionLocality"));
    stats.insert("osmIntentLocality".into(), city_value);
    stats.insert("osmIntentStreets".into(), json!([left.text, right_street.text]));
    stats.insert("osmIntentIntersectionCandidateDistanceMeters".into(), num(nearest.2));
    if !shard.is_empty() {
        stats.insert("osmIntentCoverageShards".into(), json!([shard]));
    }
    Ok(Some(json!({
        "total": 1,
        "page": 1,
        "size": num(size),
        "approximate": nearest.2 > 100.0,
        "results": [Value::Object(result)],
        "resolvedQuery": address,
        "stats": Value::Object(stats)
    })))
}

// ------------------------------------------------------------ streets

/// `resolveStreetLocality(engine, surface, params)`.
pub async fn resolve_street_locality(ctx: &Ctx<'_>, surface: &str, params: &Value) -> Result<Option<Value>> {
    let collapsed = collapse_commas(trim(surface));
    let tokens: Vec<String> = split_ws(&collapsed).into_iter().map(str::to_string).collect();
    if tokens.len() < 3 {
        return Ok(None);
    }
    let designator = |token: &str| is_street_designator(&fold(token));
    if !tokens.iter().any(|token| designator(token)) {
        return Ok(None);
    }
    let size = number_or(field(params, "size"), 10.0);
    let max_locality_tokens = 4.min(tokens.len() - 2);
    for locality_length in 1..=max_locality_tokens {
        let split = tokens.len() - locality_length;
        let street_tokens = &tokens[..split];
        if !street_tokens.iter().any(|token| designator(token)) {
            continue;
        }
        let core_tokens: Vec<&String> = street_tokens.iter().filter(|token| !designator(token)).collect();
        if core_tokens.is_empty() {
            continue;
        }
        let locality_surface = tokens[split..].join(" ");
        let street_surface = street_tokens.join(" ");
        let street_key = fold(&street_surface);
        let civic_lookup = has_house_number(&street_surface);
        let distinctive = core_tokens.iter().flat_map(|token| split_non_alnum(token)).filter(|token| !token.is_empty()).last().map(str::to_string);
        let street_lookup_query = if civic_lookup {
            street_surface.clone()
        } else {
            distinctive.unwrap_or_else(|| core_tokens.iter().map(|t| t.as_str()).collect::<Vec<_>>().join(" "))
        };
        let search_street = |shards: Option<Vec<String>>| {
            let mut request = without(
                &with(
                    params,
                    vec![
                        ("q", Value::String(street_lookup_query.clone())),
                        ("typo", json!(false)),
                        ("size", num(js_max(30.0, size))),
                    ],
                ),
                &["geo"],
            );
            if let Some(shards) = shards.filter(|shards| !shards.is_empty()) {
                request = set(request, vec![("shards", strings(&shards))]);
            }
            ctx.search(request)
        };
        let exact_street_result = |result: &Value| {
            fold_value(name_or_title(result)) == street_key
                || (!nullish(field(result, "house_number"))
                    && is_truthy(field(result, "street"))
                    && civic_key(result) == street_key)
                || (is_truthy(field(result, "street")) && fold_value(field(result, "street")) == street_key)
        };
        let mut locality: Option<Locality> = None;
        let mut response: Option<Value> = None;
        let mut authority_proven = false;
        // The authority-scoped street search, kept even when its city check
        // fails: if the resolved locality lands on that same scope, the
        // search below would be the identical request.
        let mut authority_search: Option<(Vec<String>, Value)> = None;
        if shards_param(params).is_none() {
            if let Some(authority) = exact_authority_scope(ctx, &locality_surface).await.filter(|a| !a.shards.is_empty()) {
                if let Ok(candidate) = search_street(Some(authority.shards.clone())).await {
                    let keys: Vec<String> =
                        [fold(&locality_surface), fold_value(Some(&authority.text))].into_iter().filter(|key| !key.is_empty()).collect();
                    let results = field(&candidate, "results").and_then(Value::as_array);
                    let validated = results
                        .map(|results| results.iter().any(|result| exact_street_result(result) && keys.contains(&fold_value(field(result, "city")))))
                        .unwrap_or(false);
                    if validated {
                        locality = Some(authority_locality(&authority, &locality_surface));
                        response = Some(candidate);
                        authority_proven = true;
                    } else {
                        authority_search = Some((authority.shards.clone(), candidate));
                    }
                }
            }
        }
        let locality = match locality {
            Some(locality) => locality,
            None => match resolve_locality(ctx, &locality_surface, params, LocalityOptions::default()).await? {
                Some(locality) => locality,
                None => continue,
            },
        };
        let shard = locality_shard(&locality);
        let reusable = authority_search.filter(|(scope, _)| !shard.is_empty() && scope.len() == 1 && scope[0] == shard).map(|(_, response)| response);
        let response = match response.or(reusable) {
            Some(response) => response,
            None => match search_street((!shard.is_empty()).then(|| vec![shard.clone()])).await {
                Ok(response) => response,
                Err(_) => return Ok(None),
            },
        };
        let radius = (!authority_proven).then(|| locality_radius_meters(locality.get("type")));
        let authority_keys: Vec<String> =
            [fold(&locality_surface), fold_value(locality.get("name"))].into_iter().filter(|key| !key.is_empty()).collect();
        let near_locality = |result: &Value, lat: f64, lon: f64| {
            if authority_proven {
                authority_keys.contains(&fold_value(field(result, "city")))
            } else {
                distance_from(&locality, lat, lon) <= radius.unwrap()
            }
        };
        let results = results_of(&response);
        let located = |result: &&Value| lat_lon_of(result).map(|(lat, lon)| near_locality(result, lat, lon)).unwrap_or(false);
        let street = results.iter().find(|result| located(result) && fold_value(name_or_title(result)) == street_key);
        let civic = if street.is_some() {
            None
        } else {
            results.iter().find(|result| {
                located(result) && !nullish(field(result, "house_number")) && is_truthy(field(result, "street")) && civic_key(result) == street_key
            })
        };
        let street_anchor = if street.is_some() || civic.is_some() {
            None
        } else {
            results.iter().find(|result| located(result) && is_truthy(field(result, "street")) && fold_value(field(result, "street")) == street_key)
        };
        let locality_label = value_or(locality.get("name"), &locality_surface);
        let locality_type = value_or(locality.get("type"), "");
        let common_stats = |stats: &mut Obj| {
            stats.insert("plannerLane".into(), json!("osmStreetLocality"));
            stats.insert("osmIntentStreet".into(), Value::String(street_surface.clone()));
            stats.insert("osmIntentStreetLookup".into(), Value::String(street_lookup_query.clone()));
            stats.insert("osmIntentLocality".into(), locality_label.clone());
            stats.insert("osmIntentLocalityType".into(), locality_type.clone());
            if let Some(radius) = radius {
                stats.insert("osmIntentRadiusMeters".into(), num(radius));
            }
            if authority_proven {
                stats.insert("osmIntentLocalityAuthority".into(), json!(true));
            }
        };
        if let Some(civic) = civic {
            let civic_city = match field(civic, "city") {
                Some(city) if truthy_value(city) => city.clone(),
                _ => locality_label.clone(),
            };
            let address = format!("{} {}, {}", js_string(field(civic, "house_number")), js_string(field(civic, "street")), js_string(Some(&civic_city)));
            let trace = merge_runtime_traces(&[locality.trace(), trace_of(&response)]);
            let result = without(&with(civic, vec![("address", Value::String(address.clone())), ("city", civic_city)]), &["distanceMeters"]);
            let mut stats = stats_of(&response);
            if let Some(trace) = trace {
                stats.insert("trace".into(), trace);
            }
            common_stats(&mut stats);
            stats.insert("osmIntentCivicAddress".into(), json!(true));
            return Ok(Some(json!({
                "total": 1,
                "page": 1,
                "size": num(size),
                "approximate": false,
                "results": [result],
                "resolvedQuery": address,
                "stats": Value::Object(stats)
            })));
        }
        let Some(anchor) = street.or(street_anchor) else { continue };
        let trace = merge_runtime_traces(&[locality.trace(), trace_of(&response)]);
        let label = js_string(Some(&locality_label));
        let result = without(
            &with(
                anchor,
                vec![
                    ("name", Value::String(street_surface.clone())),
                    ("address", Value::String(format!("{street_surface}, {label}"))),
                    ("city", locality_label.clone()),
                    ("category", json!("highway")),
                    ("type", json!("street")),
                ],
            ),
            &["distanceMeters"],
        );
        let mut stats = stats_of(&response);
        if let Some(trace) = trace {
            stats.insert("trace".into(), trace);
        }
        common_stats(&mut stats);
        return Ok(Some(json!({
            "total": 1,
            "page": 1,
            "size": num(size),
            "approximate": false,
            "results": [result],
            "resolvedQuery": format!("{street_surface}, {label}"),
            "stats": Value::Object(stats)
        })));
    }
    Ok(None)
}

/// `fold(`${result.house_number} ${result.street}`)`.
fn civic_key(result: &Value) -> String {
    fold(&format!("{} {}", js_string(field(result, "house_number")), js_string(field(result, "street"))))
}
