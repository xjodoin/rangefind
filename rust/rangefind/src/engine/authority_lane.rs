//! Authority lanes: a port of the authority machinery in `src/runtime.js`
//! — the address fast paths (`tryAddressAuthorityFastPath`,
//! `tryAddressInterpolationFastPath`, `synthesizeInterpolatedAddress`),
//! the post-search rerank (`maybeAuthorityRerank`), shard and lexicon
//! loaders (`authorityEntries`, `loadAuthorityShards`,
//! `loadAuthorityLexiconRoot/Segment/HotList`,
//! `pagedAuthorityLexiconCandidates`), the suggest path (`suggest`,
//! `executeSuggest`, `executeLexiconSuggest`, `executeAddressSuggest`,
//! `executeLegacyAuthoritySuggest`, `hydrateSuggestionDocs`) and the
//! federation hooks (`authorityLookup`, `searchAddressAuthority`).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use serde_json::{json, Map, Value};

use crate::authority::{
    address_range_contains, address_range_query_candidates, authority_address_range_key, authority_keys_for_query,
    authority_normalize_surface, compare_autocomplete, interpolate_address_range_point, looks_like_address_query,
    normalize_postal_code_prefix_spacing, parse_authority_hot_list, parse_authority_lexicon_root,
    parse_authority_lexicon_segment, parse_authority_shard, parse_autocomplete_key, suggest_key, AuthorityEntry,
    AuthorityKeyKind, AuthorityLexiconRoot, AuthorityLexiconSegment, AuthorityShard, AutocompleteOrder, HotListEntry,
    ADDRESS_RANGE_BUCKET_SIZE, AUTHORITY_LEXICON_PAGED_FORMAT, AUTOCOMPLETE_PREFIX, AUTOCOMPLETE_SEPARATOR,
};
use crate::codec::directory::find_directory_page;
use crate::codec::text_routing::floor_directory_page_index;
use crate::error::{Error, Result};
use crate::pointer::Pointer;
use crate::rt::{try_join_all, PromiseCache, PromiseSlot};
use crate::typo::js_string;
use crate::util::json::{num, path, string_or_empty, to_number, truthy, NULL};
use crate::util::jsstr::{cmp16, len16};

use super::docs::{PreferDocPages, ResultContext, Row};
use super::filters::Filters;
use super::geo_plan::GeoPlan;
use super::index::{DirectoryState, IndexEngine};
use super::io::inflate_object;
use super::search::SortPlan;
use super::text::TextRequest;

// ------------------------------------------------------------------ helpers

/// `Number(value.toFixed(digits))`: JS rounds the exact binary value half
/// away from zero.
pub(crate) fn js_to_fixed(value: f64, digits: usize) -> f64 {
    if !value.is_finite() || value.abs() >= 1e21 {
        return value;
    }
    let text = format!("{:.90}", value.abs());
    let (int, frac) = text.split_once('.').unwrap_or((&text, ""));
    let mut units: Vec<u8> = int.bytes().chain(frac.bytes().take(digits)).collect();
    let round_up = frac.as_bytes().get(digits).map(|d| *d >= b'5').unwrap_or(false);
    if round_up {
        let mut index = units.len();
        loop {
            if index == 0 {
                units.insert(0, b'1');
                break;
            }
            index -= 1;
            if units[index] == b'9' {
                units[index] = b'0';
            } else {
                units[index] += 1;
                break;
            }
        }
    }
    let split = units.len() - digits;
    let mut out = String::new();
    if value < 0.0 {
        out.push('-');
    }
    out.push_str(std::str::from_utf8(&units[..split]).unwrap_or("0"));
    if digits > 0 {
        out.push('.');
        out.push_str(std::str::from_utf8(&units[split..]).unwrap_or("0"));
    }
    out.parse::<f64>().unwrap_or(value)
}

/// JS `\s` / `String.prototype.trim` whitespace.
fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\u{9}'..='\u{d}' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'
    )
}

fn js_trim(value: &str) -> &str {
    value.trim_matches(is_js_space)
}

/// `Number(field)` where a missing property is `undefined` (NaN).
fn js_field_number(object: &Value, key: &str) -> f64 {
    match object.get(key) {
        None => f64::NAN,
        Some(value) => to_number(value),
    }
}

/// `String(value || "")`, JS-truthiness aware.
fn js_string_or_empty(value: Option<&Value>) -> String {
    match value {
        Some(value) if truthy(value) => js_string(value),
        _ => String::new(),
    }
}

/// A positive, truthy JS number (`x || fallback` chains).
fn truthy_number(value: &Value) -> Option<f64> {
    if truthy(value) {
        let number = to_number(value);
        if number.is_nan() {
            None
        } else {
            Some(number)
        }
    } else {
        None
    }
}

fn depth(value: f64) -> usize {
    if value.is_finite() && value > 0.0 {
        value as usize
    } else {
        0
    }
}

/// `suggestionHydrationContext(count)`.
pub fn suggestion_hydration_context(count: usize) -> ResultContext {
    ResultContext::new(false, if count > 1 { PreferDocPages::Force } else { PreferDocPages::No })
}

fn payload_stats(context: &ResultContext, out: &mut Map<String, Value>) {
    context.stats(out, true);
}

// ------------------------------------------------------------------ state

/// An autocomplete candidate parsed from an authority shard entry
/// (`autocompleteCandidatesForShard`).
pub struct AutoCandidate {
    pub key: String,
    pub normalized: String,
    pub display: String,
    display_len: usize,
    pub weight: f64,
    pub count: f64,
    pub full: bool,
    pub rows: Option<Vec<[u64; 2]>>,
    pub doc_rows: Option<Vec<[u64; 2]>>,
    pub root_doc: Option<(u64, u64)>,
    pub rank: f64,
}

impl crate::budget::CacheCost for Vec<Rc<AutoCandidate>> {
    fn cache_cost(&self) -> usize {
        let rows = |rows: &Option<Vec<[u64; 2]>>| rows.as_ref().map(|rows| rows.capacity() * 16).unwrap_or(0);
        self.capacity() * 8
            + self
                .iter()
                .map(|item| {
                    std::mem::size_of::<AutoCandidate>() + 16 + item.key.len() + item.normalized.len() + item.display.len() + rows(&item.rows) + rows(&item.doc_rows)
                })
                .sum::<usize>()
    }
}

impl AutocompleteOrder for AutoCandidate {
    fn rank_or_weight(&self) -> f64 {
        self.rank
    }
    fn display(&self) -> &str {
        &self.display
    }
    fn normalized(&self) -> &str {
        &self.normalized
    }
    fn display_length(&self) -> usize {
        self.display_len
    }
}

/// The loaded lexicon root (`loadAuthorityLexiconRoot`).
pub struct LexiconRoot {
    pub root: AuthorityLexiconRoot,
    pub weighted: bool,
    pub paged: bool,
}

/// One lexicon candidate shard summary: `{ shard, maxRank, entry? }`.
#[derive(Clone)]
struct ShardSummary {
    shard: String,
    max_rank: u64,
    entry: Option<Pointer>,
}

/// A resolved authority key: `{ key, shardName, entry }`.
#[derive(Clone)]
pub struct AuthorityHit {
    pub key: String,
    pub shard_name: String,
    pub entry: AuthorityEntry,
}

/// Authority lane state that lives for the engine's lifetime.
pub struct AuthorityState {
    pub address_enabled: bool,
    pub interpolation_enabled: bool,
    pub base_depth: usize,
    pub max_depth: usize,
    pub autocomplete: Value,
    pub segment_directory: Option<Rc<DirectoryState>>,
    pub pack_table: Vec<String>,
    pub shards: PromiseCache<String, Rc<AuthorityShard>>,
    /// Autocomplete candidates derived per shard (as large as the shard).
    pub candidates: PromiseCache<String, Rc<Vec<Rc<AutoCandidate>>>>,
    pub lexicon_root: PromiseSlot<Option<Rc<LexiconRoot>>>,
    pub segments: PromiseCache<String, Rc<AuthorityLexiconSegment>>,
    pub hot: PromiseCache<String, Rc<Vec<HotListEntry>>>,
}

fn rows_of(entry: &AuthorityEntry) -> impl Iterator<Item = (i32, f64)> + '_ {
    entry.rows.iter().map(|row| (row[0] as i32, row[1] as f64))
}

fn sort_rows(rows: &mut [Row]) {
    rows.sort_by(|left, right| {
        right.1.partial_cmp(&left.1).unwrap_or(Ordering::Equal).then_with(|| left.0.cmp(&right.0))
    });
}

/// `lexiconSuggestion(candidate)`.
fn lexicon_suggestion(candidate: &AutoCandidate) -> Map<String, Value> {
    let mut suggestion = Map::new();
    suggestion.insert("text".into(), json!(candidate.display));
    suggestion.insert("weight".into(), num(candidate.weight));
    suggestion.insert("count".into(), num(candidate.count));
    if let Some(rows) = candidate.rows.as_ref().filter(|rows| !rows.is_empty()) {
        let top = rows[0][1];
        let ordinals: Vec<u64> = rows.iter().take_while(|row| row[1] == top).map(|row| row[0]).collect();
        suggestion.insert("ordinals".into(), json!(ordinals));
    }
    if let Some(rows) = candidate.doc_rows.as_ref().filter(|rows| !rows.is_empty()) {
        suggestion.insert("doc".into(), json!(rows[0][0]));
        if rows.len() > 1 {
            suggestion.insert("docs".into(), json!(rows.iter().map(|row| row[0]).collect::<Vec<_>>()));
        }
    }
    if let Some((doc, ordinal)) = candidate.root_doc {
        suggestion.insert("doc".into(), json!(doc));
        suggestion.insert("docOrdinal".into(), json!(ordinal));
    }
    suggestion
}

/// `autocompleteLowerBound(candidates, keyPrefix)`.
fn autocomplete_lower_bound(candidates: &[Rc<AutoCandidate>], key_prefix: &str) -> usize {
    let (mut lo, mut hi) = (0usize, candidates.len());
    while lo < hi {
        let mid = (lo + hi) >> 1;
        if cmp16(&candidates[mid].key, key_prefix).is_lt() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// `mergeAutocompleteCandidate(best, item)`.
fn merge_autocomplete_candidate(best: &mut HashMap<String, Rc<AutoCandidate>>, item: &Rc<AutoCandidate>) {
    let replace = match best.get(&item.display) {
        None => true,
        Some(previous) => compare_autocomplete(&**item, &**previous) == Ordering::Less,
    };
    if replace {
        best.insert(item.display.clone(), item.clone());
    }
}

fn sorted_best(best: &HashMap<String, Rc<AutoCandidate>>) -> Vec<Rc<AutoCandidate>> {
    let mut sorted: Vec<Rc<AutoCandidate>> = best.values().cloned().collect();
    sorted.sort_by(|a, b| compare_autocomplete(&**a, &**b));
    sorted
}

/// `addressSuggestionParts(q)`: `/^(\d+)\s+(.+)$/u` on the trimmed query.
fn address_suggestion_parts(q: &str) -> Option<(f64, String)> {
    let trimmed = js_trim(q);
    let digits_end = trimmed.find(|c: char| !c.is_ascii_digit()).unwrap_or(trimmed.len());
    if digits_end == 0 {
        return None;
    }
    let rest = &trimmed[digits_end..];
    let tail_start = rest.find(|c: char| !is_js_space(c)).unwrap_or(rest.len());
    if tail_start == 0 {
        return None;
    }
    let tail = &rest[tail_start..];
    if tail.is_empty() || tail.contains(['\n', '\r', '\u{2028}', '\u{2029}']) {
        return None;
    }
    let house: f64 = trimmed[..digits_end].parse().unwrap_or(f64::NAN);
    if !(house.is_finite() && house.trunc() == house && house <= 9_007_199_254_740_991.0) || house < 0.0 {
        return None;
    }
    Some((house, js_trim(tail).to_string()))
}

/// The `^\d+[\p{L}]?(?:\s*[–-]\s*\d+[\p{L}]?)?\s+` prefix length.
fn house_number_prefix_len(value: &str) -> Option<usize> {
    let chars: Vec<(usize, char)> = value.char_indices().collect();
    let at = |i: usize| chars.get(i).map(|(_, c)| *c);
    let digits = |mut i: usize| {
        let start = i;
        while at(i).map(|c| c.is_ascii_digit()).unwrap_or(false) {
            i += 1;
        }
        if i == start {
            None
        } else {
            Some(i)
        }
    };
    let letter = |i: usize| if at(i).map(crate::typo::is_letter).unwrap_or(false) { i + 1 } else { i };
    let spaces = |mut i: usize| {
        while at(i).map(is_js_space).unwrap_or(false) {
            i += 1;
        }
        i
    };
    let end_of = |i: usize| chars.get(i).map(|(offset, _)| *offset).unwrap_or(value.len());
    let head = letter(digits(0)?);
    let group = (|| {
        let mut i = spaces(head);
        if !matches!(at(i), Some('\u{2013}') | Some('-')) {
            return None;
        }
        i = spaces(i + 1);
        Some(letter(digits(i)?))
    })();
    if let Some(after_group) = group {
        let end = spaces(after_group);
        if end > after_group {
            return Some(end_of(end));
        }
    }
    let end = spaces(head);
    if end > head {
        Some(end_of(end))
    } else {
        None
    }
}

/// `replaceSuggestionHouseNumber(value, houseNumber)`.
fn replace_suggestion_house_number(value: &str, house_number: f64) -> String {
    let trimmed = js_trim(value);
    let tail = match house_number_prefix_len(trimmed) {
        Some(len) => &trimmed[len..],
        None => trimmed,
    };
    if tail.is_empty() {
        String::new()
    } else {
        format!("{} {tail}", crate::util::json::js_number_to_string(house_number))
    }
}

/// `interpolatedAddressLabel(result, houseNumber)`.
fn interpolated_address_label(result: &Value, house_number: u64) -> String {
    let mut base_parts = vec![house_number.to_string()];
    if let Some(street) = result.get("street").filter(|value| truthy(value)) {
        base_parts.push(js_string(street));
    }
    let base = base_parts.join(" ");
    let mut parts: Vec<String> = Vec::new();
    let mut push = |text: String| {
        let text = js_trim(&text).to_string();
        if !text.is_empty() && !parts.contains(&text) {
            parts.push(text);
        }
    };
    push(base);
    for key in ["suburb", "city", "district", "state", "postcode", "country"] {
        push(js_string_or_empty(result.get(key)));
    }
    parts.join(", ")
}

/// `resultTitleMatchesQuery(result, query)`.
fn result_title_matches_query(result: Option<&Value>, query: &str) -> bool {
    let title = authority_normalize_surface(&js_string_or_empty(result.and_then(|r| r.get("title"))));
    let surface = authority_normalize_surface(query);
    !title.is_empty() && !surface.is_empty() && title == surface
}

/// `sameDocOrder(rows, results)`.
fn same_doc_order(rows: &[Row], results: &[Value]) -> bool {
    if rows.len() != results.len() {
        return false;
    }
    rows.iter().zip(results).all(|((doc, _), result)| result.get("index").and_then(Value::as_f64) == Some(*doc as f64))
}

/// `authorityEntryRows(entries)`.
fn authority_entry_rows(entries: &[AuthorityHit]) -> usize {
    entries.iter().map(|hit| hit.entry.rows.len()).sum()
}

fn unique_keys(keys: &[crate::authority::AuthorityKey], kind: AuthorityKeyKind) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for item in keys.iter().filter(|item| item.kind == kind && !item.key.is_empty()) {
        if !out.contains(&item.key) {
            out.push(item.key.clone());
        }
    }
    out
}

impl IndexEngine {
    // ------------------------------------------------------------ state

    pub(crate) fn authority_state(&self) -> Rc<AuthorityState> {
        if let Some(state) = self.extension::<AuthorityState>("authority") {
            return state;
        }
        let manifest = self.manifest.borrow();
        let fields = path(&manifest, &["authority", "fields"]).as_array().cloned().unwrap_or_default();
        let normalizer = |name: &str| fields.iter().any(|field| field.get("normalizer").and_then(Value::as_str) == Some(name));
        let authority_base = truthy_number(path(&manifest, &["authority", "base_shard_depth"]));
        let stats_base = truthy_number(path(&manifest, &["stats", "base_shard_depth"]));
        let base_depth = depth(authority_base.or(stats_base).unwrap_or(3.0));
        let max_depth = depth(
            truthy_number(path(&manifest, &["authority", "max_shard_depth"]))
                .or_else(|| truthy_number(path(&manifest, &["stats", "max_shard_depth"])))
                .or(authority_base)
                .unwrap_or(5.0),
        );
        let autocomplete = {
            let value = path(&manifest, &["authority", "autocomplete"]);
            if truthy(value) {
                value.clone()
            } else {
                Value::Null
            }
        };
        let segment_directory = if autocomplete.get("format").and_then(Value::as_str) == Some(AUTHORITY_LEXICON_PAGED_FORMAT)
            && truthy(path(&autocomplete, &["segments", "directory"]))
        {
            DirectoryState::budgeted(path(&autocomplete, &["segments", "directory"]), self.cache_budget(), "authorityLexiconDirectoryPages").ok()
        } else {
            None
        };
        let packs = match path(&manifest, &["authority", "packs"]) {
            Value::Array(items) => items.clone(),
            _ => path(&manifest, &["object_store", "pack_table", "authority"]).as_array().cloned().unwrap_or_default(),
        };
        let pack_table = packs
            .iter()
            .map(|pack| match pack {
                Value::String(text) => text.clone(),
                other => js_string_or_empty(other.get("file")),
            })
            .collect();
        let state = Rc::new(AuthorityState {
            address_enabled: normalizer("address"),
            interpolation_enabled: normalizer("address-range"),
            base_depth,
            max_depth,
            autocomplete,
            segment_directory,
            pack_table,
            shards: PromiseCache::budgeted(self.cache_budget(), "authorityShards"),
            candidates: PromiseCache::budgeted(self.cache_budget(), "authorityCandidates"),
            lexicon_root: PromiseSlot::default(),
            segments: PromiseCache::budgeted(self.cache_budget(), "authorityLexiconSegments"),
            hot: PromiseCache::budgeted(self.cache_budget(), "authorityHotLists"),
        });
        drop(manifest);
        self.set_extension("authority", state.clone());
        state
    }

    // ------------------------------------------------------------ loaders

    /// `loadAuthorityShards(shards)`.
    pub async fn load_authority_shards(&self, shards: &[(String, Pointer)]) -> Result<Vec<(String, Rc<AuthorityShard>)>> {
        let state = self.authority_state();
        let verify = self.verify_checksums;
        let mut wanted: Vec<String> = Vec::new();
        let mut items: Vec<(String, Pointer)> = Vec::new();
        for (shard, pointer) in shards {
            if wanted.contains(shard) {
                continue;
            }
            wanted.push(shard.clone());
            items.push((shard.clone(), pointer.clone()));
        }
        self.load_batch(
            &state.shards,
            items,
            &self.options.range_plans.authority,
            |pack| self.url(&format!("authority/packs/{pack}")),
            |shard, pointer, slice| {
                let inflated = inflate_object(&slice, pointer, &format!("authority shard {shard}"), verify)?;
                Ok(Rc::new(parse_authority_shard(&inflated)?))
            },
        )
        .await?;
        let mut out = Vec::new();
        for shard in wanted {
            if let Some(promise) = state.shards.get(&shard) {
                let data = promise.await?;
                out.push((shard, data));
            }
        }
        Ok(out)
    }

    /// `authorityEntries(keys)`.
    pub async fn authority_entries(&self, keys: &[String]) -> Result<Vec<AuthorityHit>> {
        let Some(directory) = self.authority_directory.clone() else { return Ok(Vec::new()) };
        if !self.options.authority || keys.is_empty() {
            return Ok(Vec::new());
        }
        let state = self.authority_state();
        let mut buckets: Vec<(String, Pointer, Vec<String>)> = Vec::new();
        for key in keys {
            let resolved = self.resolve_directory_shard(key, &directory, state.base_depth, state.max_depth).await?;
            let Some((shard, entry)) = resolved else { continue };
            match buckets.iter_mut().find(|bucket| bucket.0 == shard) {
                Some(bucket) => bucket.2.push(key.clone()),
                None => buckets.push((shard, entry, vec![key.clone()])),
            }
        }
        let requests: Vec<(String, Pointer)> = buckets.iter().map(|(shard, entry, _)| (shard.clone(), entry.clone())).collect();
        let loaded = self.load_authority_shards(&requests).await?;
        let mut out = Vec::new();
        for (shard, _, bucket_keys) in &buckets {
            let Some((_, data)) = loaded.iter().find(|(name, _)| name == shard) else { continue };
            for key in bucket_keys {
                if let Some(entry) = data.get(key) {
                    out.push(AuthorityHit { key: key.clone(), shard_name: shard.clone(), entry: entry.clone() });
                }
            }
        }
        Ok(out)
    }

    /// `loadAuthorityLexiconRoot()`.
    async fn load_authority_lexicon_root(&self) -> Result<Option<Rc<LexiconRoot>>> {
        let state = self.authority_state();
        let meta = &state.autocomplete;
        let paged = meta.get("format").and_then(Value::as_str) == Some(AUTHORITY_LEXICON_PAGED_FORMAT);
        let file = if paged { path(meta, &["root", "file"]) } else { path(meta, &["directory", "file"]) };
        if !truthy(file) {
            return Ok(None);
        }
        let file = js_string(file);
        let weighted_meta = meta.get("weighted") == Some(&Value::Bool(true));
        let url = self.url(&file);
        state
            .lexicon_root
            .get_or_load(
                async {
                    let bytes = self.io.fetch_gzip(&url).await?;
                    let root = parse_authority_lexicon_root(&bytes)?;
                    let weighted = if paged { weighted_meta } else { root.weighted };
                    Ok(Some(Rc::new(LexiconRoot { root, weighted, paged })))
                },
                true,
            )
            .await
    }

    /// `loadAuthorityLexiconSegment(item)`.
    async fn load_authority_lexicon_segment(&self, shard: &str, entry: &Pointer) -> Result<Rc<AuthorityLexiconSegment>> {
        let state = self.authority_state();
        let key = format!("{}:{}:{}", entry.pack, entry.offset, entry.length);
        let mut pack_dir = js_string_or_empty(state.autocomplete.get("segments").and_then(|s| s.get("pack_dir")));
        if pack_dir.is_empty() {
            pack_dir = "authority/lexicon-packs/".into();
        }
        if !pack_dir.ends_with('/') {
            pack_dir.push('/');
        }
        let url = self.url(&format!("{pack_dir}{}", entry.pack));
        let verify = self.verify_checksums;
        let label = format!("authority lexicon segment {shard}");
        let pack_table = state.pack_table.clone();
        state
            .segments
            .get_or_load(key, async move {
                let compressed = self.io.fetch_range(&url, entry.offset, entry.length).await?;
                let inflated = inflate_object(&compressed, entry, &label, verify)?;
                Ok(Rc::new(parse_authority_lexicon_segment(&inflated, &pack_table)?))
            })
            .await
    }

    /// `authorityLexiconSegmentPointers(prefix)`.
    async fn authority_lexicon_segment_pointers(&self, prefix: &str) -> Result<(Vec<(String, Pointer)>, usize)> {
        let state = self.authority_state();
        let Some(directory) = state.segment_directory.clone() else { return Ok((Vec::new(), 0)) };
        let root = self.load_directory_root(&directory).await?;
        if root.pages.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let mut upper = prefix.to_string();
        upper.push('\u{ffff}');
        let mut page_index = floor_directory_page_index(&root, prefix).max(0) as usize;
        let mut entries: Vec<(String, Pointer)> = Vec::new();
        let mut pages_visited = 0;
        let mut first = true;
        while page_index < root.pages.len() {
            let summary = &root.pages[page_index];
            if !first && cmp16(&summary.first, &upper).is_gt() {
                break;
            }
            let page = self.load_directory_page(&directory, &summary.file.clone()).await?;
            pages_visited += 1;
            let start = if first { page.floor_key_index(prefix).max(0) as usize } else { 0 };
            first = false;
            for (shard, entry) in page.entries.iter().skip(start) {
                if !entries.is_empty() && cmp16(shard, &upper).is_gt() {
                    return Ok((entries, pages_visited));
                }
                entries.push((shard.clone(), entry.clone()));
            }
            page_index += 1;
        }
        Ok((entries, pages_visited))
    }

    /// `pagedAuthorityLexiconCandidates(prefix)` → (entries, pagesVisited,
    /// segmentsVisited).
    async fn paged_authority_lexicon_candidates(&self, prefix: &str) -> Result<(Vec<ShardSummary>, usize, usize)> {
        let (pointers, pages_visited) = self.authority_lexicon_segment_pointers(prefix).await?;
        let mut entries = Vec::new();
        for chunk in pointers.chunks(32) {
            let segments = try_join_all(chunk.iter().map(|(shard, entry)| self.load_authority_lexicon_segment(shard, entry))).await?;
            for segment in segments {
                for item in &segment.entries {
                    if prefix.starts_with(item.shard.as_str()) || item.shard.starts_with(prefix) {
                        entries.push(ShardSummary { shard: item.shard.clone(), max_rank: item.max_rank, entry: Some(item.entry.clone()) });
                    }
                }
            }
        }
        Ok((entries, pages_visited, pointers.len()))
    }

    /// `loadAuthorityHotList(prefix, entry)`.
    async fn load_authority_hot_list(&self, prefix: &str, file: &str) -> Result<Rc<Vec<HotListEntry>>> {
        if file.is_empty() {
            return Ok(Rc::new(Vec::new()));
        }
        let state = self.authority_state();
        let url = self.url(file);
        state
            .hot
            .get_or_load(prefix.to_string(), async move {
                let bytes = self.io.fetch_gzip(&url).await?;
                Ok(Rc::new(parse_authority_hot_list(&bytes)?))
            })
            .await
    }

    /// `autocompleteCandidatesForShard(shard, weighted)` (cached per shard).
    fn autocomplete_candidates_for_shard(&self, name: &str, shard: &AuthorityShard, weighted: bool) -> Rc<Vec<Rc<AutoCandidate>>> {
        let state = self.authority_state();
        if let Some(list) = state.candidates.ready(&name.to_string()) {
            return list;
        }
        let mut list = Vec::new();
        for entry in &shard.entries {
            let Some(parsed) = parse_autocomplete_key(&entry.key) else { continue };
            let weight = match entry.autocomplete_weight.filter(|w| *w != 0) {
                Some(weight) => weight as f64,
                None => entry.total as f64,
            };
            let count = if entry.total != 0 { entry.total as f64 } else { 1.0 };
            let full = suggest_key(&parsed.display) == parsed.normalized;
            let has_rows = !entry.rows.is_empty();
            let rank = crate::authority::autocomplete_rank(weight, full, weighted);
            list.push(Rc::new(AutoCandidate {
                key: entry.key.clone(),
                display_len: parsed.display.chars().count(),
                normalized: parsed.normalized,
                display: parsed.display,
                weight,
                count,
                full,
                rows: if entry.ordinal_rows && has_rows { Some(entry.rows.clone()) } else { None },
                doc_rows: if entry.doc_rows && has_rows { Some(entry.rows.clone()) } else { None },
                root_doc: match (entry.ordinal_rows, entry.doc, entry.doc_ordinal) {
                    (true, Some(doc), Some(ordinal)) => Some((doc, ordinal)),
                    _ => None,
                },
                rank,
            }));
        }
        let list = Rc::new(list);
        state.candidates.set(name.to_string(), crate::rt::Promise::resolved(list.clone()));
        list
    }

    // ------------------------------------------------------------ address fast paths

    /// `synthesizeInterpolatedAddress(result, houseNumber)`.
    fn synthesize_interpolated_address(&self, result: &Value, house_number: u64) -> Result<Option<Value>> {
        let start = js_field_number(result, "_address_range_start");
        let end = js_field_number(result, "_address_range_end");
        let step = js_field_number(result, "_address_range_step");
        if !address_range_contains(start, end, step, house_number as f64) {
            return Ok(None);
        }
        let geometry = js_string_or_empty(result.get("_address_range_geometry"));
        let Some(point) = interpolate_address_range_point(&geometry, start, end, house_number as f64)? else { return Ok(None) };
        let address = interpolated_address_label(result, house_number);
        let mut map = result.as_object().cloned().unwrap_or_default();
        let id = match result.get("id") {
            None => "undefined".to_string(),
            Some(value) => js_string(value),
        };
        let accuracy = result.get("_address_range_inclusion").filter(|v| truthy(v)).cloned().unwrap_or(json!("actual"));
        let kind = result.get("_address_range_kind").filter(|v| truthy(v)).cloned().unwrap_or(json!(""));
        map.insert("id".into(), json!(format!("{id}/{house_number}")));
        map.insert("title".into(), json!(address));
        map.insert("name".into(), json!(address));
        map.insert("address".into(), json!(address));
        map.insert("house_number".into(), json!(house_number.to_string()));
        map.insert("type".into(), json!("interpolated_address"));
        map.insert("lat".into(), num(js_to_fixed(point.lat, 7)));
        map.insert("lon".into(), num(js_to_fixed(point.lon, 7)));
        map.insert("interpolated".into(), json!(true));
        map.insert("address_accuracy".into(), accuracy);
        map.insert("interpolation".into(), kind);
        for key in [
            "_address_range_start",
            "_address_range_end",
            "_address_range_step",
            "_address_range_geometry",
            "_address_range_kind",
            "_address_range_inclusion",
        ] {
            map.shift_remove(key);
        }
        Ok(Some(Value::Object(map)))
    }

    /// `tryAddressInterpolationFastPath({ q, page, size, includeResults })`.
    async fn try_address_interpolation_fast_path(&self, q: &str, page: f64, size: usize, include_results: bool) -> Result<Option<Value>> {
        let state = self.authority_state();
        if !state.interpolation_enabled {
            return Ok(None);
        }
        let candidates = address_range_query_candidates(q, ADDRESS_RANGE_BUCKET_SIZE);
        if candidates.is_empty() {
            return Ok(None);
        }
        let mut houses_by_key: Vec<(String, Vec<u64>)> = Vec::new();
        for candidate in &candidates {
            let key = authority_address_range_key(&candidate.lookup_value);
            if key.is_empty() {
                continue;
            }
            let position = match houses_by_key.iter().position(|(existing, _)| *existing == key) {
                Some(position) => position,
                None => {
                    houses_by_key.push((key, Vec::new()));
                    houses_by_key.len() - 1
                }
            };
            if !houses_by_key[position].1.contains(&candidate.house_number) {
                houses_by_key[position].1.push(candidate.house_number);
            }
        }
        let keys: Vec<String> = houses_by_key.iter().map(|(key, _)| key.clone()).collect();
        let entries = self.authority_entries(&keys).await?;
        // doc → (score, houses), in first-seen order.
        let mut rows: Vec<(i32, f64, Vec<u64>)> = Vec::new();
        let mut positions: HashMap<i32, usize> = HashMap::new();
        let mut complete = true;
        for hit in &entries {
            complete = complete && hit.entry.complete;
            let houses = houses_by_key.iter().find(|(key, _)| *key == hit.key).map(|(_, houses)| houses.clone()).unwrap_or_default();
            for (doc, score) in rows_of(&hit.entry) {
                let position = *positions.entry(doc).or_insert_with(|| {
                    rows.push((doc, score, Vec::new()));
                    rows.len() - 1
                });
                let row = &mut rows[position];
                row.1 = row.1.max(score);
                for house in &houses {
                    if !row.2.contains(house) {
                        row.2.push(*house);
                    }
                }
            }
        }
        if rows.is_empty() {
            return Ok(None);
        }
        rows.sort_by(|left, right| right.1.partial_cmp(&left.1).unwrap_or(Ordering::Equal).then_with(|| left.0.cmp(&right.0)));
        if !include_results {
            // Count-only callers still need the compact range payloads to
            // prove containment, so defer to the regular planner.
            return Ok(None);
        }
        let mut context = ResultContext::new(true, PreferDocPages::Auto);
        let ranked: Vec<Row> = rows.iter().map(|(doc, score, _)| (*doc, *score)).collect();
        let hydrated = self.rows_to_results(&ranked, &mut context).await?;
        let mut matches: Vec<Value> = Vec::new();
        for (index, (_, _, houses)) in rows.iter().enumerate() {
            for house in houses {
                if let Some(result) = self.synthesize_interpolated_address(&hydrated[index], *house)? {
                    matches.push(result);
                }
            }
        }
        let mut stats = Map::new();
        stats.insert("exact".into(), json!(true));
        stats.insert("plannerLane".into(), json!("addressInterpolationExact"));
        if matches.is_empty() {
            if !complete {
                return Ok(None);
            }
            for (key, value) in [
                ("topKProven", json!(true)),
                ("totalExact", json!(true)),
                ("tailExhausted", json!(true)),
                ("addressInterpolationKeys", json!(houses_by_key.len())),
                ("addressInterpolationRows", json!(rows.len())),
                ("addressInterpolationMatches", json!(0)),
                ("addressInterpolationComplete", json!(true)),
                ("blocksDecoded", json!(0)),
                ("postingsDecoded", json!(0)),
            ] {
                stats.insert(key.into(), value);
            }
            payload_stats(&context, &mut stats);
            return Ok(Some(json!({
                "total": 0, "page": num(page), "size": size, "approximate": false, "results": [], "stats": Value::Object(stats)
            })));
        }
        let score_of = |value: &Value| value.get("score").map(to_number).unwrap_or(f64::NAN);
        let index_of = |value: &Value| value.get("index").map(to_number).unwrap_or(f64::NAN);
        matches.sort_by(|left, right| {
            let by_score = score_of(right) - score_of(left);
            if by_score != 0.0 && !by_score.is_nan() {
                return if by_score > 0.0 { Ordering::Greater } else { Ordering::Less };
            }
            let by_index = index_of(left) - index_of(right);
            if by_index != 0.0 && !by_index.is_nan() {
                return if by_index > 0.0 { Ordering::Greater } else { Ordering::Less };
            }
            cmp16(
                left.get("house_number").and_then(Value::as_str).unwrap_or(""),
                right.get("house_number").and_then(Value::as_str).unwrap_or(""),
            )
        });
        let total = matches.len();
        matches.truncate(size);
        for (key, value) in [
            ("topKProven", json!(complete)),
            ("totalExact", json!(complete)),
            ("tailExhausted", json!(complete)),
            ("addressInterpolationKeys", json!(houses_by_key.len())),
            ("addressInterpolationRows", json!(rows.len())),
            ("addressInterpolationMatches", json!(total)),
            ("addressInterpolationComplete", json!(complete)),
            ("blocksDecoded", json!(0)),
            ("postingsDecoded", json!(0)),
        ] {
            stats.insert(key.into(), value);
        }
        payload_stats(&context, &mut stats);
        Ok(Some(json!({
            "total": total, "page": num(page), "size": size, "approximate": !complete, "results": matches, "stats": Value::Object(stats)
        })))
    }

    /// `tryAddressAuthorityFastPath({ q, page, size, hasFilters, sortPlan,
    /// geoPlan, includeResults, authority })`.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn try_address_authority_fast_path(
        &self,
        q: &str,
        page: f64,
        size: usize,
        has_filters: bool,
        sort: Option<&SortPlan>,
        geo: Option<&Rc<GeoPlan>>,
        include_results: bool,
        authority: Option<&Value>,
    ) -> Result<Option<Value>> {
        let state = self.authority_state();
        if !state.address_enabled || !self.options.authority || authority == Some(&Value::Bool(false)) {
            return Ok(None);
        }
        if page != 1.0 || has_filters || sort.is_some() || geo.is_some() || !looks_like_address_query(q) {
            return Ok(None);
        }
        let analyzer = self.analyzer.clone();
        let tokenize = move |value: &str| analyzer.tokenize(value, &analyzer.detect_language(value), false);
        let key_plans = authority_keys_for_query::<String>(q, &[], &tokenize, true);
        let address_keys = unique_keys(&key_plans, AuthorityKeyKind::Address);
        if address_keys.is_empty() {
            return Ok(None);
        }
        let entries = self.authority_entries(&address_keys).await?;
        let mut rows: Vec<Row> = Vec::new();
        let mut positions: HashMap<i32, usize> = HashMap::new();
        let mut total: f64 = 0.0;
        let mut complete = true;
        for hit in &entries {
            total = total.max(hit.entry.total as f64);
            complete = complete && hit.entry.complete;
            for (doc, score) in rows_of(&hit.entry) {
                match positions.get(&doc) {
                    Some(&position) => rows[position].1 = rows[position].1.max(score),
                    None => {
                        positions.insert(doc, rows.len());
                        rows.push((doc, score.max(0.0)));
                    }
                }
            }
        }
        if rows.is_empty() {
            return self.try_address_interpolation_fast_path(q, page, size, include_results).await;
        }
        sort_rows(&mut rows);
        // Authority shards retain a bounded top list. Fall back to postings
        // if a caller requests more rows than the exact key retained.
        if (rows.len() as f64) < (size as f64).min(total) {
            return Ok(None);
        }
        let page_rows: Vec<Row> = rows.iter().take(size).copied().collect();
        let mut context = ResultContext::new(true, PreferDocPages::Auto);
        let results = if include_results { self.rows_to_results(&page_rows, &mut context).await? } else { Vec::new() };
        let mut stats = Map::new();
        for (key, value) in [
            ("exact", json!(true)),
            ("plannerLane", json!("addressAuthorityExact")),
            ("topKProven", json!(true)),
            ("totalExact", json!(true)),
            ("tailExhausted", json!(complete)),
            ("addressAuthorityKeys", json!(address_keys.len())),
            ("addressAuthorityRows", json!(rows.len())),
            ("addressAuthorityComplete", json!(complete)),
            ("blocksDecoded", json!(0)),
            ("postingsDecoded", json!(0)),
        ] {
            stats.insert(key.into(), value);
        }
        if include_results {
            payload_stats(&context, &mut stats);
        }
        Ok(Some(json!({
            "total": num(total), "page": num(page), "size": size, "approximate": false, "results": results, "stats": Value::Object(stats)
        })))
    }

    // ------------------------------------------------------------ rerank

    /// `maybeAuthorityRerank(params, response)`.
    pub(crate) async fn maybe_authority_rerank(&self, request: &TextRequest, params: &Value, response: Value) -> Result<Value> {
        if self.authority_directory.is_none() || !self.options.authority || params.get("authority") == Some(&Value::Bool(false)) {
            return Ok(response);
        }
        if !request.include_results || request.page != 1.0 || truthy(&request.sort) || request.filters.has_filters() {
            return Ok(response);
        }
        let authority_query = {
            let pick = ["correctedQuery", "surfaceFallbackQuery"].iter().find_map(|key| response.get(*key).filter(|v| truthy(v)).map(js_string));
            let raw = pick.unwrap_or_else(|| request.q.clone());
            js_trim(&raw).to_string()
        };
        let results: Vec<Value> = response.get("results").and_then(Value::as_array).cloned().unwrap_or_default();
        if authority_query.is_empty() || result_title_matches_query(results.first(), &authority_query) {
            return Ok(response);
        }
        let state = self.authority_state();
        let authority_terms = self.analyzer.query_plan(&authority_query).base_terms;
        let analyzer = self.analyzer.clone();
        let tokenize = move |value: &str| analyzer.tokenize(value, &analyzer.detect_language(value), false);
        let address = state.address_enabled && looks_like_address_query(&authority_query);
        let key_plans = authority_keys_for_query(&authority_query, &authority_terms, &tokenize, address);
        let address_keys = unique_keys(&key_plans, AuthorityKeyKind::Address);
        let surface_keys = unique_keys(&key_plans, AuthorityKeyKind::Surface);
        let exact_keys = unique_keys(&key_plans, AuthorityKeyKind::Exact);
        let token_keys = unique_keys(&key_plans, AuthorityKeyKind::Tokens);
        if surface_keys.is_empty() && exact_keys.is_empty() && token_keys.is_empty() {
            return Ok(response);
        }
        let response_total = {
            let value = response.get("total").unwrap_or(&NULL);
            if truthy(value) {
                to_number(value)
            } else {
                0.0
            }
        };
        let using_address = !address_keys.is_empty();
        let mut loaded_keys = if using_address { address_keys.clone() } else { surface_keys.clone() };
        let mut entries = self.authority_entries(&loaded_keys).await?;
        if authority_entry_rows(&entries) == 0 && !surface_keys.is_empty() && using_address {
            loaded_keys = surface_keys.clone();
            entries = self.authority_entries(&surface_keys).await?;
        }
        if authority_entry_rows(&entries) == 0 && !exact_keys.is_empty() {
            loaded_keys = exact_keys.clone();
            entries = self.authority_entries(&exact_keys).await?;
        }
        if authority_entry_rows(&entries) == 0 && response_total == 0.0 && !token_keys.is_empty() {
            loaded_keys = token_keys.clone();
            entries = self.authority_entries(&token_keys).await?;
        }
        // doc → score, first-seen order (baseline results first).
        let mut rows: Vec<Row> = Vec::new();
        let mut positions: HashMap<i32, usize> = HashMap::new();
        for result in &results {
            let Some(index) = result.get("index").filter(|v| !v.is_null()) else { continue };
            let doc = to_number(index) as i32;
            let score_value = result.get("score").unwrap_or(&NULL);
            let score = if truthy(score_value) { to_number(score_value) } else { 0.0 };
            match positions.get(&doc) {
                Some(&position) => rows[position].1 = score,
                None => {
                    positions.insert(doc, rows.len());
                    rows.push((doc, score));
                }
            }
        }
        let mut authority_rows = 0usize;
        let mut authority_total: f64 = 0.0;
        let mut authority_complete = true;
        for hit in &entries {
            authority_total = authority_total.max(hit.entry.total as f64);
            authority_complete = authority_complete && hit.entry.complete;
            for (doc, score) in rows_of(&hit.entry) {
                authority_rows += 1;
                match positions.get(&doc) {
                    Some(&position) => rows[position].1 += score,
                    None => {
                        positions.insert(doc, rows.len());
                        rows.push((doc, 0.0 + score));
                    }
                }
            }
        }
        sort_rows(&mut rows);
        let size = (request.size as f64).clamp(1.0, 100.0) as usize;
        let page_rows: Vec<Row> = rows.iter().take(size).copied().collect();
        let result_indexes: HashSet<i64> =
            results.iter().filter_map(|result| result.get("index").and_then(Value::as_f64)).map(|v| v as i64).collect();
        let injected = page_rows.iter().filter(|(doc, _)| !result_indexes.contains(&(*doc as i64))).count();
        let mut response = response;
        if authority_rows == 0 || (injected == 0 && same_doc_order(&page_rows, &results)) {
            super::typo_lane::merge_stats(
                &mut response,
                vec![
                    ("authorityAttempted", json!(true)),
                    ("authorityApplied", json!(false)),
                    ("authorityKeys", json!(loaded_keys.len())),
                    ("authorityRows", json!(authority_rows)),
                ],
            );
            return Ok(response);
        }
        let mut context = ResultContext::new(true, PreferDocPages::Auto);
        let reranked = self.rows_to_results(&page_rows, &mut context).await?;
        let top_k_proven = truthy(path(&response, &["stats", "topKProven"])) && authority_complete;
        let fallback_total = if authority_total != 0.0 { authority_total } else { page_rows.len() as f64 };
        response["total"] = num(response_total.max(fallback_total));
        response["results"] = Value::Array(reranked);
        let mut extra = vec![
            ("topKProven", json!(top_k_proven)),
            ("authorityAttempted", json!(true)),
            ("authorityApplied", json!(true)),
            ("authorityComplete", json!(authority_complete)),
            ("authorityKeys", json!(loaded_keys.len())),
            ("authorityEntries", json!(entries.len())),
            ("authorityRows", json!(authority_rows)),
            ("authorityInjected", json!(injected)),
        ];
        let mut payload = Map::new();
        payload_stats(&context, &mut payload);
        let payload: Vec<(String, Value)> = payload.into_iter().collect();
        for (key, value) in &payload {
            extra.push((key.as_str(), value.clone()));
        }
        super::typo_lane::merge_stats(&mut response, extra);
        Ok(response)
    }

    // ------------------------------------------------------------ suggest

    /// `authoritySuggestField()`: the single exact `title` field, if any.
    fn authority_suggest_field(&self) -> bool {
        let manifest = self.manifest.borrow();
        let fields: Vec<&Value> = path(&manifest, &["authority", "fields"])
            .as_array()
            .map(|fields| fields.iter().filter(|field| field.get("exact") != Some(&Value::Bool(false))).collect())
            .unwrap_or_default();
        fields.len() == 1 && fields[0].get("path").and_then(Value::as_str) == Some("title")
    }

    /// `executeLegacyAuthoritySuggest(params, q, prefix, size, hydrate)`.
    async fn execute_legacy_authority_suggest(&self, params: &Value, q: &str, prefix: &str, size: usize, hydrate: bool) -> Result<Option<Value>> {
        let Some(directory) = self.authority_directory.clone() else { return Ok(None) };
        if !self.options.authority || !self.authority_suggest_field() {
            return Ok(None);
        }
        let key_prefix = format!("x|{prefix}");
        let raw = match params.get("authorityMaxShards").filter(|v| !v.is_null()) {
            Some(value) => to_number(value),
            None => match self.options.get("authoritySuggestMaxShards") {
                Value::Null => 16.0,
                value => to_number(value),
            },
        };
        let max_shards = if raw.is_nan() { usize::MAX } else { raw.floor().clamp(1.0, 64.0) as usize };
        let (range_entries, pages_visited, truncated) = self.directory_entries_for_prefix(&directory, &key_prefix, max_shards).await?;
        // (key, doc, weight, count)
        let mut candidates: Vec<(i32, f64, f64)> = Vec::new();
        let mut shards_visited = 0usize;
        let mut entries_scanned = 0usize;
        for shard in &range_entries {
            let loaded = self.load_authority_shards(std::slice::from_ref(shard)).await?;
            let Some((_, data)) = loaded.iter().find(|(name, _)| *name == shard.0) else { continue };
            shards_visited += 1;
            for entry in &data.entries {
                if cmp16(&entry.key, &key_prefix).is_lt() {
                    continue;
                }
                if !entry.key.starts_with(&key_prefix) {
                    break;
                }
                entries_scanned += 1;
                let Some(row) = entry.rows.first() else { continue };
                candidates.push((row[0] as i32, row[1] as f64, if entry.total != 0 { entry.total as f64 } else { 1.0 }));
                if candidates.len() >= size {
                    break;
                }
            }
            if candidates.len() >= size {
                break;
            }
        }
        let mut context = suggestion_hydration_context(candidates.len());
        let docs = if candidates.is_empty() {
            Vec::new()
        } else {
            let rows: Vec<Row> = candidates.iter().map(|(doc, weight, _)| (*doc, *weight)).collect();
            self.rows_to_results(&rows, &mut context).await?
        };
        let mut suggestions = Vec::new();
        for (index, doc) in docs.into_iter().enumerate() {
            let text = js_string_or_empty(doc.get("title"));
            if text.is_empty() {
                continue;
            }
            let (doc_index, weight, count) = candidates[index];
            let mut suggestion = Map::new();
            suggestion.insert("text".into(), json!(text));
            suggestion.insert("weight".into(), num(weight));
            suggestion.insert("count".into(), num(count));
            suggestion.insert("doc".into(), json!(doc_index));
            if hydrate {
                suggestion.insert("result".into(), doc.clone());
                suggestion.insert("results".into(), json!([doc]));
            }
            suggestions.push(Value::Object(suggestion));
        }
        suggestions.truncate(size);
        let exact = candidates.len() >= size || !truncated;
        let mut stats = Map::new();
        stats.insert("exact".into(), json!(exact));
        stats.insert("suggestLane".into(), json!("authority-title"));
        stats.insert("suggestDirectoryPagesVisited".into(), json!(pages_visited));
        stats.insert("suggestCandidateShards".into(), json!(range_entries.len()));
        stats.insert("suggestShardsVisited".into(), json!(shards_visited));
        stats.insert("suggestEntriesScanned".into(), json!(entries_scanned));
        if !candidates.is_empty() {
            let mut payload = Map::new();
            context.stats(&mut payload, false);
            stats.extend(payload);
        }
        Ok(Some(json!({ "q": q, "prefix": prefix, "suggestions": suggestions, "stats": Value::Object(stats) })))
    }

    /// `executeLexiconSuggest(q, prefix, size, root)`.
    async fn execute_lexicon_suggest(&self, q: &str, prefix: &str, size: usize, root: &LexiconRoot) -> Result<Value> {
        if !root.root.hot.is_empty() {
            let hot_entry = root.root.hot_get(prefix);
            let complete_missing_single = prefix.chars().count() == 1 && hot_entry.is_none();
            if complete_missing_single || hot_entry.map(|entry| size as u64 <= entry.count).unwrap_or(false) {
                let hot = match hot_entry {
                    Some(entry) => self.load_authority_hot_list(prefix, &entry.file).await?,
                    None => Rc::new(Vec::new()),
                };
                let suggestions: Vec<Value> = hot
                    .iter()
                    .take(size)
                    .map(|item| {
                        let mut suggestion = Map::new();
                        suggestion.insert("text".into(), json!(item.display));
                        suggestion.insert("weight".into(), json!(item.weight));
                        suggestion.insert("count".into(), json!(item.count));
                        if let Some(doc) = item.doc {
                            suggestion.insert("doc".into(), json!(doc));
                        }
                        if let Some(docs) = &item.docs {
                            if docs.len() > 1 && docs.iter().all(|row| row.ordinal.is_none()) {
                                suggestion.insert("docs".into(), json!(docs.iter().map(|row| row.doc).collect::<Vec<_>>()));
                            }
                            if let Some(ordinal) = docs.first().and_then(|row| row.ordinal) {
                                suggestion.insert("docOrdinal".into(), json!(ordinal));
                            }
                        }
                        Value::Object(suggestion)
                    })
                    .collect();
                return Ok(json!({
                    "q": q,
                    "prefix": prefix,
                    "suggestions": suggestions,
                    "stats": {
                        "exact": true,
                        "suggestLane": "authority-hot",
                        "suggestCandidateShards": 0,
                        "suggestShardsVisited": 0,
                        "suggestEntriesScanned": hot.len()
                    }
                }));
            }
        }

        let key_prefix = format!("{AUTOCOMPLETE_PREFIX}{prefix}");
        let (candidates, paged_pages, segments_visited) = if root.paged {
            self.paged_authority_lexicon_candidates(&key_prefix).await?
        } else {
            let shards = root
                .root
                .shards
                .iter()
                .filter(|item| key_prefix.starts_with(item.shard.as_str()) || item.shard.starts_with(&key_prefix))
                .map(|item| ShardSummary { shard: item.shard.clone(), max_rank: item.max_rank, entry: None })
                .collect();
            (shards, 0, 0)
        };
        let mut ordered = candidates.clone();
        ordered.sort_by(|left, right| right.max_rank.cmp(&left.max_rank).then_with(|| cmp16(&left.shard, &right.shard)));
        let mut best: HashMap<String, Rc<AutoCandidate>> = HashMap::new();
        let mut entries_scanned = 0usize;
        let mut shards_visited = 0usize;
        let mut directory_pages_visited = paged_pages;
        let mut directory_pages: HashSet<String> = HashSet::new();
        let directory_root = if root.paged {
            None
        } else {
            let Some(directory) = self.authority_directory.clone() else {
                return Err(Error::new("Cannot read properties of null (reading 'rootPromise')"));
            };
            Some((directory.clone(), self.load_directory_root(&directory).await?))
        };
        let batch = 8;
        let mut position = 0;
        while position < ordered.len() {
            let boundary = kth_rank(&mut best, size);
            if boundary >= 0.0 && (ordered[position].max_rank as f64) < boundary {
                break;
            }
            let slice = &ordered[position..(position + batch).min(ordered.len())];
            let mut resolved: Vec<(String, Pointer)> = Vec::new();
            for summary in slice {
                if root.paged {
                    if let Some(entry) = &summary.entry {
                        resolved.push((summary.shard.clone(), entry.clone()));
                    }
                    continue;
                }
                let (directory, droot) = directory_root.as_ref().unwrap();
                if let Some(page) = find_directory_page(droot, &summary.shard) {
                    directory_pages.insert(page.file.clone());
                }
                if let Some(entry) = self.directory_entry_from_root(directory, droot, &summary.shard).await? {
                    resolved.push((summary.shard.clone(), entry));
                }
            }
            if !root.paged {
                directory_pages_visited = directory_pages.len();
            }
            let loaded = self.load_authority_shards(&resolved).await?;
            for (name, _) in &resolved {
                let Some((_, shard)) = loaded.iter().find(|(loaded_name, _)| loaded_name == name) else { continue };
                shards_visited += 1;
                let shard_candidates = self.autocomplete_candidates_for_shard(name, shard, root.weighted);
                for candidate in shard_candidates.iter().skip(autocomplete_lower_bound(&shard_candidates, &key_prefix)) {
                    if !candidate.key.starts_with(&key_prefix) {
                        break;
                    }
                    entries_scanned += 1;
                    merge_autocomplete_candidate(&mut best, candidate);
                }
            }
            position += slice.len();
        }
        let ranked: Vec<Value> = sorted_best(&best).iter().take(size).map(|candidate| Value::Object(lexicon_suggestion(candidate))).collect();
        let mut stats = Map::new();
        stats.insert("exact".into(), json!(true));
        stats.insert("suggestLane".into(), json!("authority-lexicon"));
        stats.insert("suggestCandidateShards".into(), json!(candidates.len()));
        stats.insert("suggestShardsVisited".into(), json!(shards_visited));
        stats.insert("suggestDirectoryPagesVisited".into(), json!(directory_pages_visited));
        if root.paged {
            stats.insert("suggestLexiconSegmentsVisited".into(), json!(segments_visited));
        }
        stats.insert("suggestEntriesScanned".into(), json!(entries_scanned));
        Ok(json!({ "q": q, "prefix": prefix, "suggestions": ranked, "stats": Value::Object(stats) }))
    }

    /// `executeAddressSuggest(q, prefix, size, root, hydrate)`.
    async fn execute_address_suggest(&self, q: &str, prefix: &str, size: usize, root: &LexiconRoot, hydrate: bool) -> Result<Option<Value>> {
        if !self.authority_state().interpolation_enabled {
            return Ok(None);
        }
        let Some((house_number, tail)) = address_suggestion_parts(q) else { return Ok(None) };
        if tail.is_empty() {
            return Ok(None);
        }
        let normalized_tail = authority_normalize_surface(&tail);
        let active = normalized_tail.split(' ').filter(|part| !part.is_empty()).last().unwrap_or("");
        if len16(active) < 3 {
            return Ok(None);
        }
        let tail_prefix = suggest_key(&tail);
        if tail_prefix.is_empty() {
            return Ok(None);
        }
        let tail_size = (size * 3).clamp(12, 32);
        let tail_response = self.execute_lexicon_suggest(&tail, &tail_prefix, tail_size, root).await?;
        let tail_suggestions = tail_response["suggestions"].as_array().cloned().unwrap_or_default();
        let lookup_limit = (size * 2).clamp(12, 16);
        let mut seen_queries: HashSet<String> = HashSet::new();
        let mut candidates: Vec<(Value, String)> = Vec::new();
        for candidate in &tail_suggestions {
            let text = js_string_or_empty(candidate.get("text"));
            let candidate_query = replace_suggestion_house_number(&text, house_number);
            let normalized = authority_normalize_surface(&candidate_query);
            if candidate_query.is_empty() || seen_queries.contains(&normalized) {
                continue;
            }
            seen_queries.insert(normalized);
            candidates.push((candidate.clone(), candidate_query));
            if candidates.len() >= lookup_limit {
                break;
            }
        }
        let authority_true = Value::Bool(true);
        let responses = try_join_all(
            candidates
                .iter()
                .map(|(_, query)| self.try_address_authority_fast_path(query, 1.0, 1, false, None, None, true, Some(&authority_true))),
        )
        .await?;
        let mut suggestions: Vec<Value> = Vec::new();
        let mut seen_addresses: HashSet<String> = HashSet::new();
        for (index, (candidate, _)) in candidates.iter().enumerate() {
            let Some(result) = responses[index].as_ref().and_then(|r| r.get("results")).and_then(|r| r.get(0)) else { continue };
            let pick = ["address", "name", "title"].iter().find_map(|key| result.get(*key).filter(|v| truthy(v)).map(js_string));
            let text = js_trim(&pick.unwrap_or_default()).to_string();
            let normalized = authority_normalize_surface(&text);
            if text.is_empty() || seen_addresses.contains(&normalized) {
                continue;
            }
            seen_addresses.insert(normalized);
            let weight_value = candidate.get("weight").unwrap_or(&NULL);
            let weight = if truthy(weight_value) { to_number(weight_value) } else { 0.0 };
            let mut suggestion = Map::new();
            suggestion.insert("text".into(), json!(text));
            suggestion.insert("weight".into(), num(js_max(1.0, weight)));
            suggestion.insert("count".into(), json!(1));
            suggestion.insert("interpolated".into(), json!(truthy(result.get("interpolated").unwrap_or(&NULL))));
            if hydrate {
                suggestion.insert("result".into(), result.clone());
                suggestion.insert("results".into(), json!([result]));
            }
            suggestions.push(Value::Object(suggestion));
            if suggestions.len() >= size {
                break;
            }
        }
        if suggestions.is_empty() {
            return Ok(None);
        }
        let tail_stats = &tail_response["stats"];
        let stat_or_zero = |key: &str| {
            let value = tail_stats.get(key).unwrap_or(&NULL);
            if truthy(value) {
                value.clone()
            } else {
                json!(0)
            }
        };
        Ok(Some(json!({
            "q": q,
            "prefix": prefix,
            "suggestions": suggestions,
            "stats": {
                "exact": true,
                "suggestLane": "address-authority",
                "addressSuggestionTail": tail,
                "addressSuggestionCandidates": tail_suggestions.len(),
                "addressSuggestionLookups": candidates.len(),
                "addressSuggestionMatches": suggestions.len(),
                "addressSuggestionTailLane": js_string_or_empty(tail_stats.get("suggestLane")),
                "suggestCandidateShards": stat_or_zero("suggestCandidateShards"),
                "suggestShardsVisited": stat_or_zero("suggestShardsVisited"),
                "suggestDirectoryPagesVisited": stat_or_zero("suggestDirectoryPagesVisited"),
                "suggestEntriesScanned": stat_or_zero("suggestEntriesScanned")
            }
        })))
    }

    /// `hydrateSuggestionDocs(response)`.
    async fn hydrate_suggestion_docs(&self, mut response: Value) -> Result<Value> {
        let doc_list = |item: &Value| -> Vec<Value> {
            match item.get("docs") {
                Some(Value::Array(docs)) => docs.clone(),
                Some(other) if truthy(other) => Vec::new(),
                _ => match item.get("doc").filter(|doc| !doc.is_null()) {
                    Some(doc) => vec![doc.clone()],
                    None => Vec::new(),
                },
            }
        };
        let skip = |item: &Value| item.get("docOrdinal").map(|v| !v.is_null()).unwrap_or(false);
        let suggestions = response.get("suggestions").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut ordered: Vec<i64> = Vec::new();
        let mut slots: HashMap<i64, usize> = HashMap::new();
        for item in &suggestions {
            if skip(item) {
                continue;
            }
            for ordinal in doc_list(item) {
                let ordinal = to_number(&ordinal) as i64;
                if let std::collections::hash_map::Entry::Vacant(slot) = slots.entry(ordinal) {
                    slot.insert(ordered.len());
                    ordered.push(ordinal);
                }
            }
        }
        if ordered.is_empty() {
            return Ok(response);
        }
        let mut context = suggestion_hydration_context(ordered.len());
        let rows: Vec<Row> = ordered.iter().map(|ordinal| (*ordinal as i32, 0.0)).collect();
        match self.rows_to_results(&rows, &mut context).await {
            Ok(hydrated) => {
                let mut updated = suggestions.clone();
                for item in updated.iter_mut() {
                    if skip(item) {
                        continue;
                    }
                    let results: Vec<Value> = doc_list(item)
                        .iter()
                        .filter_map(|ordinal| slots.get(&(to_number(ordinal) as i64)).and_then(|slot| hydrated.get(*slot)).cloned())
                        .filter(truthy)
                        .collect();
                    if !results.is_empty() {
                        let first = results[0].clone();
                        if let Some(map) = item.as_object_mut() {
                            map.insert("results".into(), Value::Array(results));
                            map.insert("result".into(), first);
                        }
                    }
                }
                response["suggestions"] = Value::Array(updated);
                let mut payload = Map::new();
                context.stats(&mut payload, false);
                super::typo_lane::merge_stats(
                    &mut response,
                    vec![
                        ("suggestDocsHydrated", json!(ordered.len())),
                        ("docPayloadLane", payload.get("docPayloadLane").cloned().unwrap_or(Value::Null)),
                        ("docPayloadPages", payload.get("docPayloadPages").cloned().unwrap_or(Value::Null)),
                    ],
                );
            }
            Err(_) => {
                super::typo_lane::merge_stats(&mut response, vec![("suggestDocsHydrated", json!(0))]);
            }
        }
        Ok(response)
    }

    /// `executeSuggest(params)`.
    async fn execute_suggest(&self, params: &Value) -> Result<Value> {
        let q = string_or_empty(params.get("q").unwrap_or(&NULL));
        let size = {
            let value = params.get("size").unwrap_or(&NULL);
            let number = if truthy(value) { to_number(value) } else { 8.0 }.floor();
            if number.is_nan() {
                1
            } else {
                number.clamp(1.0, 50.0) as usize
            }
        };
        let hydrate = params.get("hydrate") == Some(&Value::Bool(true));
        let prefix = suggest_key(&q);
        if prefix.is_empty() {
            return Ok(json!({
                "q": q, "prefix": prefix, "suggestions": [],
                "stats": { "exact": true, "suggestShardsVisited": 0, "suggestEntriesScanned": 0 }
            }));
        }
        if let Some(root) = self.load_authority_lexicon_root().await? {
            if let Some(address) = self.execute_address_suggest(&q, &prefix, size, &root, hydrate).await? {
                return Ok(address);
            }
            let response = self.execute_lexicon_suggest(&q, &prefix, size, &root).await?;
            return if hydrate { self.hydrate_suggestion_docs(response).await } else { Ok(response) };
        }
        if let Some(legacy) = self.execute_legacy_authority_suggest(params, &q, &prefix, size, hydrate).await? {
            return Ok(legacy);
        }
        Err(Error::new(
            "Rangefind: this index has no authority autocomplete lexicon (configure `suggest` fields at build time).",
        ))
    }

    /// `suggest(params)`: postal-code prefix normalization around
    /// `executeSuggest`.
    pub(crate) async fn suggest(&self, params: Value) -> Result<Value> {
        let surface = string_or_empty(params.get("q").unwrap_or(&NULL));
        let normalized = normalize_postal_code_prefix_spacing(&surface);
        let mut active = params;
        if normalized != surface {
            active["q"] = json!(normalized);
        }
        let mut response = self.execute_suggest(&active).await?;
        if normalized != surface {
            response["q"] = json!(surface);
            response["normalizedQuery"] = json!(normalized);
            super::typo_lane::merge_stats(&mut response, vec![("postalCodeNormalized", json!(true))]);
        }
        Ok(response)
    }

    /// `authorityLookup(surface, params)`: exact-surface lookup against the
    /// autocomplete lexicon. Accepts a surface string or `{ surface | q,
    /// size }`.
    pub(crate) async fn authority_lookup(&self, params: Value) -> Result<Value> {
        let (surface, options) = match &params {
            Value::String(text) => (text.clone(), Value::Null),
            other => {
                let value = other.get("surface").filter(|v| !v.is_null()).or_else(|| other.get("q")).unwrap_or(&NULL);
                (string_or_empty(value), other.clone())
            }
        };
        let Some(root) = self.load_authority_lexicon_root().await? else { return Ok(Value::Null) };
        let size = {
            let value = options.get("size").unwrap_or(&NULL);
            let number = if truthy(value) { to_number(value) } else { 8.0 }.floor();
            if number.is_nan() {
                1
            } else {
                number.clamp(1.0, 50.0) as usize
            }
        };
        let prefix = suggest_key(&surface);
        if prefix.is_empty() {
            return Ok(json!({ "surface": surface, "prefix": prefix, "matches": [] }));
        }
        let key_prefix = format!("{AUTOCOMPLETE_PREFIX}{prefix}{AUTOCOMPLETE_SEPARATOR}");
        let resolved: Vec<(String, Pointer)> = if root.paged {
            let (entries, _, _) = self.paged_authority_lexicon_candidates(&key_prefix).await?;
            entries.into_iter().filter_map(|summary| summary.entry.map(|entry| (summary.shard, entry))).collect()
        } else {
            let Some(directory) = self.authority_directory.clone() else {
                return Err(Error::new("Cannot read properties of null (reading 'rootPromise')"));
            };
            let directory_root = self.load_directory_root(&directory).await?;
            let mut out = Vec::new();
            for summary in root.root.shards.iter().filter(|item| key_prefix.starts_with(item.shard.as_str()) || item.shard.starts_with(&key_prefix)) {
                if let Some(entry) = self.directory_entry_from_root(&directory, &directory_root, &summary.shard).await? {
                    out.push((summary.shard.clone(), entry));
                }
            }
            out
        };
        let loaded = self.load_authority_shards(&resolved).await?;
        let mut matches: Vec<Rc<AutoCandidate>> = Vec::new();
        for (name, _) in &resolved {
            let Some((_, shard)) = loaded.iter().find(|(loaded_name, _)| loaded_name == name) else { continue };
            let shard_candidates = self.autocomplete_candidates_for_shard(name, shard, root.weighted);
            for candidate in shard_candidates.iter().skip(autocomplete_lower_bound(&shard_candidates, &key_prefix)) {
                if !candidate.key.starts_with(&key_prefix) {
                    break;
                }
                matches.push(candidate.clone());
            }
        }
        matches.sort_by(|a, b| compare_autocomplete(&**a, &**b));
        let matches: Vec<Value> = matches
            .iter()
            .take(size)
            .map(|candidate| {
                let mut suggestion = lexicon_suggestion(candidate);
                suggestion.insert("full".into(), json!(candidate.full));
                Value::Object(suggestion)
            })
            .collect();
        Ok(json!({ "surface": surface, "prefix": prefix, "matches": matches }))
    }

    /// `searchAddressAuthority(params)`: the federation hook resolving exact
    /// and interpolated addresses without falling through to postings.
    pub(crate) async fn search_address_authority(&self, params: Value) -> Result<Value> {
        let q = js_trim(&string_or_empty(params.get("q").unwrap_or(&NULL))).to_string();
        let page = super::search::request_page(params.get("page"));
        let size = super::search::request_size(params.get("size"), self.options.max_page_size);
        let has_filters = Filters::from_json(params.get("filters").unwrap_or(&NULL)).has_filters();
        let flag = |key: &str| truthy(params.get(key).unwrap_or(&NULL));
        if page != 1.0
            || has_filters
            || flag("sort")
            || flag("geo")
            || flag("vector")
            || params.get("authority") == Some(&Value::Bool(false))
            || !looks_like_address_query(&q)
        {
            return Ok(Value::Null);
        }
        let response = self
            .try_address_authority_fast_path(&q, 1.0, size, false, None, None, true, params.get("authority"))
            .await?;
        Ok(response.unwrap_or(Value::Null))
    }
}

/// `Math.max(a, b)` (NaN-propagating).
fn js_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}

/// The `kthRank()` closure of `executeLexiconSuggest`.
fn kth_rank(best: &mut HashMap<String, Rc<AutoCandidate>>, size: usize) -> f64 {
    if best.len() < size {
        return -1.0;
    }
    let sorted = sorted_best(best);
    if sorted.len() > size * 4 {
        best.clear();
        for item in sorted.iter().take(size * 2) {
            best.insert(item.display.clone(), item.clone());
        }
    }
    sorted[size - 1].rank
}
