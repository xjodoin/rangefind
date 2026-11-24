//! Main-index typo correction helpers (`src/typo_main_index.js`): option
//! normalization, probe-term generation, bounded Damerau-Levenshtein,
//! suffix-aware surfaces, n-gram overlap and candidate scoring.
//!
//! JS strings are UTF-16: every length, index, slice and comparison here
//! works on `u16` code units so results (including probes that split a
//! surrogate pair) match the JS exactly. `Math.log1p` is the fdlibm
//! algorithm V8 ships, ported in [`js_log1p`].

use std::collections::HashSet;

use serde_json::Value;
use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};

use crate::util::json::js_number_to_string;

pub const COMMON_SUBSTITUTIONS: &str = "aeiourstnlcmpdbfgvhqxyzkjw";
pub const COMMON_SUFFIXES: [&str; 18] = [
    "", "s", "e", "es", "er", "eur", "eurs", "euse", "ement", "ements", "ation", "ations", "ique", "iques", "ance", "ances", "ite",
    "ites",
];

/// The object `normalizeMainIndexTypoOptions` returns (and
/// `MAIN_INDEX_TYPO_DEFAULTS` via [`Default`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MainIndexTypoOptions {
    /// `"off"` or `"main-index"`.
    pub mode: String,
    /// `"zero"` or `"zero-or-weak"`.
    pub trigger: String,
    pub max_edits: u32,
    pub max_token_candidates: u32,
    pub max_query_plans: u32,
    pub max_corrected_searches: u32,
    pub max_shard_lookups: u32,
    pub weak_result_total: u32,
}

impl Default for MainIndexTypoOptions {
    fn default() -> Self {
        MainIndexTypoOptions {
            mode: "main-index".into(),
            trigger: "zero-or-weak".into(),
            max_edits: 2,
            max_token_candidates: 8,
            max_query_plans: 5,
            max_corrected_searches: 3,
            max_shard_lookups: 12,
            weak_result_total: 0,
        }
    }
}

/// `{ surface, distance }` from `bestMainIndexTypoDistance`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypoDistance {
    pub surface: String,
    pub distance: u32,
}

// ------------------------------------------------------------------ JS value coercions

fn is_js_whitespace(c: char) -> bool {
    matches!(
        c,
        '\u{9}'..='\u{d}'
            | ' '
            | '\u{a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

/// JS `String(value)` for JSON values (`null` inside arrays joins as "").
pub fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => js_number_to_string(number.as_f64().unwrap_or(f64::NAN)),
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| if item.is_null() { String::new() } else { js_string(item) })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

/// JS `Number(value)` for JSON values.
pub fn js_number(value: &Value) -> f64 {
    match value {
        Value::Null => 0.0,
        Value::Bool(flag) => f64::from(u8::from(*flag)),
        Value::Number(number) => number.as_f64().unwrap_or(f64::NAN),
        Value::String(text) => js_string_to_number(text),
        Value::Array(_) => js_string_to_number(&js_string(value)),
        Value::Object(_) => f64::NAN,
    }
}

/// ECMAScript `StringToNumber`.
pub fn js_string_to_number(text: &str) -> f64 {
    let text = text.trim_matches(is_js_whitespace);
    if text.is_empty() {
        return 0.0;
    }
    match text {
        "Infinity" | "+Infinity" => return f64::INFINITY,
        "-Infinity" => return f64::NEG_INFINITY,
        _ => {}
    }
    let bytes = text.as_bytes();
    if bytes.len() > 2 && bytes[0] == b'0' {
        let radix = match bytes[1] {
            b'x' | b'X' => 16,
            b'o' | b'O' => 8,
            b'b' | b'B' => 2,
            _ => 0,
        };
        if radix != 0 {
            let mut value = 0f64;
            for &b in &bytes[2..] {
                match (b as char).to_digit(radix) {
                    Some(digit) => value = value * radix as f64 + digit as f64,
                    None => return f64::NAN,
                }
            }
            return value;
        }
    }
    if valid_decimal_literal(bytes) {
        text.parse::<f64>().unwrap_or(f64::NAN)
    } else {
        f64::NAN
    }
}

fn valid_decimal_literal(bytes: &[u8]) -> bool {
    let mut i = 0;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        i += 1;
    }
    let int_start = i;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        i += 1;
    }
    let mut digits = i - int_start;
    if i < bytes.len() && bytes[i] == b'.' {
        i += 1;
        let frac_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        digits += i - frac_start;
    }
    if digits == 0 {
        return false;
    }
    if i < bytes.len() && (bytes[i] == b'e' || bytes[i] == b'E') {
        i += 1;
        if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
            i += 1;
        }
        let exp_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start {
            return false;
        }
    }
    i == bytes.len()
}

fn nullish(value: Option<&Value>) -> Option<&Value> {
    value.filter(|v| !v.is_null())
}

/// `positiveInt(value, fallback, min, max)`; `value` is `None` when nullish.
fn positive_int(value: Option<&Value>, fallback: u32, min: u32, max: u32) -> u32 {
    let parsed = match value {
        Some(value) => js_number(value).floor(),
        None => fallback as f64,
    };
    if !parsed.is_finite() {
        return fallback;
    }
    parsed.min(max as f64).max(min as f64) as u32
}

// ------------------------------------------------------------------ options

/// `normalizeMainIndexTypoOptions(options, manifest)`: runtime options
/// (`typoMode`, `typoTrigger`, `typoMaxEdits`, ...) take precedence over
/// `manifest.search.typo`, then the defaults.
pub fn normalize_main_index_typo_options(options: &Value, manifest: &Value) -> MainIndexTypoOptions {
    let typo = manifest.get("search").and_then(|search| search.get("typo"));
    let manifest_options = typo.filter(|value| crate::util::json::truthy(value));
    let pick = |option_key: &str, manifest_key: &str| -> Option<&Value> {
        nullish(options.get(option_key)).or_else(|| nullish(manifest_options.and_then(|m| m.get(manifest_key))))
    };
    let defaults = MainIndexTypoOptions::default();
    let mode = pick("typoMode", "mode").map(js_string).unwrap_or_else(|| defaults.mode.clone()).to_lowercase();
    let trigger = pick("typoTrigger", "trigger").map(js_string).unwrap_or_else(|| defaults.trigger.clone()).to_lowercase();
    MainIndexTypoOptions {
        mode: if mode == "off" || mode == "false" || mode == "none" { "off" } else { "main-index" }.into(),
        trigger: if trigger == "zero" { "zero" } else { "zero-or-weak" }.into(),
        max_edits: positive_int(pick("typoMaxEdits", "maxEdits"), defaults.max_edits, 1, 3),
        max_token_candidates: positive_int(pick("typoMaxTokenCandidates", "maxTokenCandidates"), defaults.max_token_candidates, 1, 32),
        max_query_plans: positive_int(pick("typoMaxQueryPlans", "maxQueryPlans"), defaults.max_query_plans, 1, 32),
        max_corrected_searches: positive_int(
            pick("typoMaxCorrectedSearches", "maxCorrectedSearches"),
            defaults.max_corrected_searches,
            1,
            8,
        ),
        max_shard_lookups: positive_int(pick("typoMaxShardLookups", "maxShardLookups"), defaults.max_shard_lookups, 1, 64),
        weak_result_total: positive_int(pick("typoWeakResultTotal", "weakResultTotal"), defaults.weak_result_total, 0, 10),
    }
}

/// `typoMaxEditsFor(term, options)`: the full budget only for terms of at
/// least 8 code units.
pub fn typo_max_edits_for(term: &str, options: &MainIndexTypoOptions) -> u32 {
    let max = options.max_edits;
    if term.encode_utf16().count() >= 8 {
        max
    } else {
        max.min(1)
    }
}

// ------------------------------------------------------------------ tokens

/// Code points assigned after Unicode 16.0 (the tables of the Node/V8
/// build the JS spec runs on) that `unicode-properties` (Unicode 18)
/// already classifies as letters. They stay outside `\p{L}` here so token
/// predicates match the JS. Derived exhaustively by the parity fixture.
const POST_UNICODE_16_LETTERS: &[(u32, u32)] = &[
    (0x558, 0x558), (0x58b, 0x58c), (0x88f, 0x88f), (0xc5c, 0xc5c), (0xcdc, 0xcdc), (0x208f, 0x208f),
    (0x209d, 0x209f), (0xa7ce, 0xa7cf), (0xa7d2, 0xa7d2), (0xa7d4, 0xa7d4), (0xa7dd, 0xa7dd),
    (0xa7e2, 0xa7e2), (0xa7f1, 0xa7f1), (0xab6c, 0xab6d), (0x107bb, 0x107bf), (0x10940, 0x10959),
    (0x10ec5, 0x10ec7), (0x10ed9, 0x10eee), (0x11b0a, 0x11b0a), (0x11db0, 0x11ddb), (0x11df1, 0x11df1),
    (0x16ea0, 0x16eb8), (0x16ebb, 0x16ed3), (0x16ff2, 0x16ff3), (0x187f8, 0x187ff), (0x18cd6, 0x18cda),
    (0x18d09, 0x18d20), (0x18d80, 0x18df2), (0x18e00, 0x19191), (0x191a0, 0x191d2), (0x1b123, 0x1b128),
    (0x1b168, 0x1b168), (0x1d6a6, 0x1d6a6), (0x1df1f, 0x1df24), (0x1df2b, 0x1df81), (0x1df90, 0x1df96),
    (0x1dfcd, 0x1dfff), (0x1e6c0, 0x1e6de), (0x1e6e0, 0x1e6e2), (0x1e6e4, 0x1e6e5), (0x1e6e7, 0x1e6ed),
    (0x1e6f0, 0x1e6f4), (0x1e6fe, 0x1e6ff), (0x2b73a, 0x2b73f), (0x2b81e, 0x2b81e), (0x2cea2, 0x2cead),
    (0x323b0, 0x33479), (0x3d000, 0x3fc3f),
];

/// Same for `\p{N}`.
const POST_UNICODE_16_NUMBERS: &[(u32, u32)] =
    &[(0x11de0, 0x11de9), (0x1246f, 0x1246f), (0x12475, 0x1247f), (0x12550, 0x12686), (0x16ff4, 0x16ff6)];

fn in_ranges(ranges: &[(u32, u32)], cp: u32) -> bool {
    ranges
        .binary_search_by(|&(start, end)| {
            if end < cp {
                std::cmp::Ordering::Less
            } else if start > cp {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

/// `/^\p{L}$/u` for one code point.
pub fn is_letter(c: char) -> bool {
    c.general_category_group() == GeneralCategoryGroup::Letter && !in_ranges(POST_UNICODE_16_LETTERS, c as u32)
}

/// `/^\p{N}$/u` for one code point.
pub fn is_number(c: char) -> bool {
    c.general_category_group() == GeneralCategoryGroup::Number && !in_ranges(POST_UNICODE_16_NUMBERS, c as u32)
}

fn is_typo_correction_token16(units: &[u16]) -> bool {
    if units.len() < 3 || units.len() > 32 {
        return false;
    }
    let mut first = true;
    for decoded in char::decode_utf16(units.iter().copied()) {
        let Ok(c) = decoded else { return false };
        let ok = if first { is_letter(c) } else { is_letter(c) || is_number(c) };
        if !ok {
            return false;
        }
        first = false;
    }
    // `!/^\d+$/u`: unreachable once the first code point is a letter, kept
    // for fidelity.
    !units.iter().all(|&u| (b'0' as u16..=b'9' as u16).contains(&u))
}

/// `isTypoCorrectionToken(token)`: a letter followed by letters/numbers
/// (any script), not all digits, 3..=32 UTF-16 code units.
pub fn is_typo_correction_token(token: &str) -> bool {
    is_typo_correction_token16(&token.encode_utf16().collect::<Vec<_>>())
}

/// Insertion-ordered set of UTF-16 strings (a JS `Set` of strings).
struct OrderedSet {
    items: Vec<Vec<u16>>,
    seen: HashSet<Vec<u16>>,
}

impl OrderedSet {
    fn new() -> Self {
        OrderedSet { items: Vec::new(), seen: HashSet::new() }
    }

    fn add(&mut self, value: Vec<u16>) {
        if self.seen.insert(value.clone()) {
            self.items.push(value);
        }
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn finish(self, max: usize) -> Vec<String> {
        self.items
            .into_iter()
            .filter(|item| is_typo_correction_token16(item))
            .take(max)
            .map(|item| String::from_utf16_lossy(&item))
            .collect()
    }
}

fn unique_positions(candidates: [isize; 5], len: usize) -> Vec<usize> {
    let mut out: Vec<usize> = Vec::new();
    for index in candidates {
        if index >= 0 && (index as usize) < len && !out.contains(&(index as usize)) {
            out.push(index as usize);
        }
    }
    out
}

/// `mainIndexTypoProbeValues(raw, term, options)`: the seed tokens, then
/// deletions, adjacent transpositions and common-letter substitutions at
/// priority positions of the first seed of 4+ code units, capped at
/// `max(1, options.max_shard_lookups || 12)` before filtering.
pub fn main_index_typo_probe_values(raw: &str, term: &str, options: &MainIndexTypoOptions) -> Vec<String> {
    let max_shard_lookups = if options.max_shard_lookups == 0 { 12 } else { options.max_shard_lookups };
    let max = max_shard_lookups.max(1) as usize;
    let seeds: Vec<Vec<u16>> = [term, raw]
        .iter()
        .map(|value| value.encode_utf16().collect::<Vec<u16>>())
        .filter(|units| is_typo_correction_token16(units))
        .collect();
    let mut out = OrderedSet::new();
    for seed in &seeds {
        out.add(seed.clone());
    }
    let target: Vec<u16> = seeds
        .iter()
        .find(|value| value.len() >= 4)
        .or_else(|| seeds.first())
        .cloned()
        .unwrap_or_default();
    if target.is_empty() {
        return Vec::new();
    }
    let len = target.len();
    let last = len as isize - 1;

    let priority = unique_positions([0, 1, 2, 3, last], len);
    for &i in &priority {
        let mut value = target[..i].to_vec();
        value.extend_from_slice(&target[i + 1..]);
        out.add(value);
        if out.len() >= max {
            return out.finish(max);
        }
    }
    for &i in priority.iter().filter(|&&i| i + 1 < len) {
        let mut value = target[..i].to_vec();
        value.push(target[i + 1]);
        value.push(target[i]);
        value.extend_from_slice(&target[i + 2..]);
        out.add(value);
        if out.len() >= max {
            return out.finish(max);
        }
    }
    let substitution = unique_positions([1, 0, 2, 3, last], len);
    'outer: for &i in &substitution {
        for ch in COMMON_SUBSTITUTIONS.bytes() {
            let unit = ch as u16;
            if unit == target[i] {
                continue;
            }
            let mut value = target[..i].to_vec();
            value.push(unit);
            value.extend_from_slice(&target[i + 1..]);
            out.add(value);
            if out.len() >= max {
                break 'outer;
            }
        }
    }
    out.finish(max)
}

// ------------------------------------------------------------------ distances

fn bounded_damerau_levenshtein16(a: &[u16], b: &[u16], max_distance: u32) -> u32 {
    if a == b {
        return 0;
    }
    if a.len().abs_diff(b.len()) > max_distance as usize {
        return max_distance + 1;
    }
    let width = b.len() + 1;
    let mut prev_prev = vec![0u32; width];
    let mut prev: Vec<u32> = (0..width as u32).collect();
    let mut current = vec![0u32; width];
    for i in 1..=a.len() {
        current[0] = i as u32;
        let mut row_min = current[0];
        for j in 1..=b.len() {
            let cost = u32::from(a[i - 1] != b[j - 1]);
            let mut value = (prev[j] + 1).min(current[j - 1] + 1).min(prev[j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                value = value.min(prev_prev[j - 2] + 1);
            }
            current[j] = value;
            if value < row_min {
                row_min = value;
            }
        }
        if row_min > max_distance {
            return max_distance + 1;
        }
        // [prevPrev, prev, current] = [prev, current, prevPrev]
        std::mem::swap(&mut prev_prev, &mut prev);
        std::mem::swap(&mut prev, &mut current);
    }
    prev[b.len()]
}

/// `boundedDamerauLevenshtein(a, b, maxDistance)` (optimal string alignment
/// over UTF-16 units; `maxDistance + 1` once every row exceeds the bound).
pub fn bounded_damerau_levenshtein(a: &str, b: &str, max_distance: u32) -> u32 {
    let a: Vec<u16> = a.encode_utf16().collect();
    let b: Vec<u16> = b.encode_utf16().collect();
    bounded_damerau_levenshtein16(&a, &b, max_distance)
}

/// `bestMainIndexTypoDistance(token, candidate, maxEdits)`: the candidate or
/// the first `candidate + suffix` surface with the strictly smallest
/// distance.
pub fn best_main_index_typo_distance(token: &str, candidate: &str, max_edits: u32) -> TypoDistance {
    let token16: Vec<u16> = token.encode_utf16().collect();
    let candidate16: Vec<u16> = candidate.encode_utf16().collect();
    let mut best_suffix: Option<&str> = None;
    let mut best_distance = bounded_damerau_levenshtein16(&token16, &candidate16, max_edits);
    for suffix in COMMON_SUFFIXES {
        if suffix.is_empty() {
            continue;
        }
        let mut surface = candidate16.clone();
        surface.extend(suffix.encode_utf16());
        if surface.len().abs_diff(token16.len()) > max_edits as usize {
            continue;
        }
        let distance = bounded_damerau_levenshtein16(&token16, &surface, max_edits);
        if distance < best_distance {
            best_distance = distance;
            best_suffix = Some(suffix);
        }
    }
    TypoDistance { surface: format!("{candidate}{}", best_suffix.unwrap_or("")), distance: best_distance }
}

/// `ngramOverlap(left, right)`: shared distinct n-grams (bigrams when the
/// shorter side has at most 5 code units, else trigrams) over the larger
/// gram set.
pub fn ngram_overlap(left: &str, right: &str) -> f64 {
    let left: Vec<u16> = left.encode_utf16().collect();
    let right: Vec<u16> = right.encode_utf16().collect();
    let n = if left.len().min(right.len()) <= 5 { 2 } else { 3 };
    let a = grams(&left, n);
    let b = grams(&right, n);
    if a.is_empty() || b.is_empty() {
        return if left.first() == right.first() { 0.25 } else { 0.0 };
    }
    let shared = a.iter().filter(|gram| b.contains(*gram)).count();
    shared as f64 / a.len().max(b.len()) as f64
}

fn grams(value: &[u16], n: usize) -> HashSet<&[u16]> {
    if value.len() < n {
        return HashSet::new();
    }
    value.windows(n).collect()
}

fn common_prefix_length(a: &[u16], b: &[u16]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}

fn lcs_length(a: &[u16], b: &[u16]) -> usize {
    let mut previous = vec![0usize; b.len() + 1];
    let mut current = vec![0usize; b.len() + 1];
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            current[j] = if a[i - 1] == b[j - 1] { previous[j - 1] + 1 } else { previous[j].max(current[j - 1]) };
        }
        previous.copy_from_slice(&current);
    }
    previous[b.len()]
}

/// `mainIndexTypoCandidateScore(token, surface, df, distance)`.
pub fn main_index_typo_candidate_score(token: &str, surface: &str, df: f64, distance: f64) -> f64 {
    let token: Vec<u16> = token.encode_utf16().collect();
    let surface: Vec<u16> = surface.encode_utf16().collect();
    let prefix = common_prefix_length(&token, &surface);
    let sequence_similarity = lcs_length(&token, &surface) as f64 / (token.len().max(1)) as f64;
    let same_first = if !token.is_empty() && token.first() == surface.first() { 1.2 } else { -1.2 };
    let same_last = if token.last() == surface.last() { 0.8 } else { 0.0 };
    let length_penalty = token.len().abs_diff(surface.len()) as f64 * 0.35;
    js_log1p(df) * 0.35 + prefix.min(4) as f64 * 0.25 + sequence_similarity * 5.0 + same_first + same_last
        - distance * 2.6
        - length_penalty
}

// ------------------------------------------------------------------ Math.log1p

/// `a * b + c` the way clang compiles it inside V8's C++ math builtins:
/// `-ffp-contract=on` fuses it into one FMA on arm64, while x86-64 (no FMA
/// in the baseline ISA) and wasm round the product separately.
#[inline]
fn contract(a: f64, b: f64, c: f64) -> f64 {
    if cfg!(target_arch = "aarch64") {
        a.mul_add(b, c)
    } else {
        a * b + c
    }
}

/// `Math.log1p` exactly as V8 computes it: fdlibm `s_log1p.c` (V8's
/// `base::ieee754::log1p`) with the multiply-add contractions clang applies
/// to it, so scores do not depend on the platform libm.
pub fn js_log1p(x: f64) -> f64 {
    const LN2_HI: f64 = 6.931_471_803_691_238_164_90e-01;
    const LN2_LO: f64 = 1.908_214_929_270_587_700_02e-10;
    const TWO54: f64 = 1.801_439_850_948_198_400_00e+16;
    const LP1: f64 = 6.666_666_666_666_735_130e-01;
    const LP2: f64 = 3.999_999_999_940_941_908e-01;
    const LP3: f64 = 2.857_142_874_366_239_149e-01;
    const LP4: f64 = 2.222_219_843_214_978_396e-01;
    const LP5: f64 = 1.818_357_216_161_805_012e-01;
    const LP6: f64 = 1.531_383_769_920_937_332e-01;
    const LP7: f64 = 1.479_819_860_511_658_591e-01;

    fn high(x: f64) -> i32 {
        (x.to_bits() >> 32) as i32
    }
    fn with_high(x: f64, hi: i32) -> f64 {
        f64::from_bits(((hi as u32 as u64) << 32) | (x.to_bits() & 0xffff_ffff))
    }

    let hx = high(x);
    let ax = hx & 0x7fff_ffff;
    let mut k: i32 = 1;
    let mut f = 0f64;
    let mut hu: i32 = 0;
    let mut c = 0f64;
    if hx < 0x3FDA_827A {
        if ax >= 0x3ff0_0000 {
            if x == -1.0 {
                return f64::NEG_INFINITY;
            }
            return f64::NAN;
        }
        if ax < 0x3e20_0000 {
            if TWO54 + x > 0.0 && ax < 0x3c90_0000 {
                return x;
            }
            return contract(-(x * x), 0.5, x);
        }
        if hx > 0 || hx <= 0xbfd2_bec4u32 as i32 {
            k = 0;
            f = x;
            hu = 1;
        }
    }
    if hx >= 0x7ff0_0000 {
        return x + x;
    }
    if k != 0 {
        let mut u;
        if hx < 0x4340_0000 {
            u = 1.0 + x;
            hu = high(u);
            k = (hu >> 20) - 1023;
            c = if k > 0 { 1.0 - (u - x) } else { x - (u - 1.0) };
            c /= u;
        } else {
            u = x;
            hu = high(u);
            k = (hu >> 20) - 1023;
            c = 0.0;
        }
        hu &= 0x000f_ffff;
        if hu < 0x6a09e {
            u = with_high(u, hu | 0x3ff0_0000);
        } else {
            k += 1;
            u = with_high(u, hu | 0x3fe0_0000);
            hu = (0x0010_0000 - hu) >> 2;
        }
        f = u - 1.0;
    }
    let hfsq = 0.5 * f * f;
    let kf = k as f64;
    if hu == 0 {
        if f == 0.0 {
            if k == 0 {
                return 0.0;
            }
            c = contract(kf, LN2_LO, c);
            return contract(kf, LN2_HI, c);
        }
        let r = hfsq * contract(-0.666_666_666_666_666_66, f, 1.0);
        if k == 0 {
            return f - r;
        }
        return contract(kf, LN2_HI, -((r - contract(kf, LN2_LO, c)) - f));
    }
    let s = f / (2.0 + f);
    let z = s * s;
    let poly = contract(z, LP7, LP6);
    let poly = contract(z, poly, LP5);
    let poly = contract(z, poly, LP4);
    let poly = contract(z, poly, LP3);
    let poly = contract(z, poly, LP2);
    let poly = contract(z, poly, LP1);
    let r = z * poly;
    if k == 0 {
        f - contract(-s, hfsq + r, hfsq)
    } else {
        let tail = contract(s, hfsq + r, contract(kf, LN2_LO, c));
        contract(kf, LN2_HI, -((hfsq - tail) - f))
    }
}
