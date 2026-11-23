//! Address key normalization, query-side address readings, postal-code
//! spacing and address-range interpolation (`src/address.js`).
//!
//! Builder-only exports (`addressRangeLookupValues`,
//! `encodeAddressRangeGeometry`) are not ported.

use std::collections::HashSet;

use super::abbreviations::{
    ADDRESS_TOKEN_READINGS, CONCATENATED_STREET_SUFFIXES, CONCATENATED_SUFFIX_READINGS, STREET_TYPE_TOKENS,
};
use super::text::{
    all_digits, ascii_alnum_runs, digits_optional_letter, digits_then_letter, is_ascii_word, is_digit, is_fsa,
    is_fsa_iu, is_js_space, is_ldu, is_ldu_iu, is_word_iu, lower_letters,
};
use crate::error::{Error, Result};
use crate::util::fold::fold_multi;

/// `ADDRESS_RANGE_BUCKET_SIZE`.
pub const ADDRESS_RANGE_BUCKET_SIZE: f64 = 16.0;

/// `TOKEN_ALIASES`, in JS `Map` order.
pub static TOKEN_ALIASES: &[(&str, &str)] = &[
    ("north", "n"),
    ("south", "s"),
    ("east", "e"),
    ("west", "w"),
    ("northeast", "ne"),
    ("northwest", "nw"),
    ("southeast", "se"),
    ("southwest", "sw"),
    ("street", "st"),
    ("str", "st"),
    ("avenue", "ave"),
    ("av", "ave"),
    ("boulevard", "blvd"),
    ("boul", "blvd"),
    ("bd", "blvd"),
    ("road", "rd"),
    ("drive", "dr"),
    ("lane", "ln"),
    ("court", "ct"),
    ("circle", "cir"),
    ("highway", "hwy"),
    ("parkway", "pkwy"),
    ("place", "pl"),
    ("plaza", "plz"),
    ("square", "sq"),
    ("terrace", "ter"),
    ("trail", "trl"),
    ("route", "rte"),
    ("apartment", "apt"),
    ("suite", "ste"),
    ("unit", "unit"),
];

fn token_alias(token: &str) -> Option<&'static str> {
    TOKEN_ALIASES.iter().find(|(from, _)| *from == token).map(|(_, to)| *to)
}

static SIMPLE_ORDINALS: &[(&str, u32)] = &[
    ("first", 1),
    ("second", 2),
    ("third", 3),
    ("fourth", 4),
    ("fifth", 5),
    ("sixth", 6),
    ("seventh", 7),
    ("eighth", 8),
    ("ninth", 9),
    ("tenth", 10),
    ("eleventh", 11),
    ("twelfth", 12),
    ("thirteenth", 13),
    ("fourteenth", 14),
    ("fifteenth", 15),
    ("sixteenth", 16),
    ("seventeenth", 17),
    ("eighteenth", 18),
    ("nineteenth", 19),
];

static ORDINAL_TENS: &[(&str, u32)] = &[
    ("twentieth", 20),
    ("thirtieth", 30),
    ("fortieth", 40),
    ("fiftieth", 50),
    ("sixtieth", 60),
    ("seventieth", 70),
    ("eightieth", 80),
    ("ninetieth", 90),
];

static CARDINAL_TENS: &[(&str, u32)] = &[
    ("twenty", 20),
    ("thirty", 30),
    ("forty", 40),
    ("fifty", 50),
    ("sixty", 60),
    ("seventy", 70),
    ("eighty", 80),
    ("ninety", 90),
];

fn lookup(table: &[(&str, u32)], token: Option<&String>) -> Option<u32> {
    let token = token?;
    table.iter().find(|(name, _)| name == token).map(|(_, value)| *value)
}

fn ordinal(value: u32) -> String {
    let mod100 = value % 100;
    let suffix = if (11..=13).contains(&mod100) {
        "th"
    } else {
        match value % 10 {
            1 => "st",
            2 => "nd",
            3 => "rd",
            _ => "th",
        }
    };
    format!("{value}{suffix}")
}

fn normalize_ordinal_tokens(tokens: Vec<String>) -> Vec<String> {
    let mut out = Vec::with_capacity(tokens.len());
    let mut index = 0;
    while index < tokens.len() {
        let token = &tokens[index];
        if let Some(simple) = lookup(SIMPLE_ORDINALS, Some(token)) {
            out.push(ordinal(simple));
            index += 1;
            continue;
        }
        if let Some(tens) = lookup(ORDINAL_TENS, Some(token)) {
            out.push(ordinal(tens));
            index += 1;
            continue;
        }
        let tens = lookup(CARDINAL_TENS, Some(token));
        let next = lookup(SIMPLE_ORDINALS, tokens.get(index + 1));
        if let (Some(tens), Some(next)) = (tens, next) {
            if next < 10 {
                out.push(ordinal(tens + next));
                index += 2;
                continue;
            }
        }
        out.push(token.clone());
        index += 1;
    }
    out
}

/// `/\b<first>[\s-]*<second>\b/gu` → `replacement` (ASCII `\b`: the regex
/// has no `i` flag).
fn replace_direction_phrase(value: &str, first: &str, second: &str, replacement: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let first: Vec<char> = first.chars().collect();
    let second: Vec<char> = second.chars().collect();
    let starts_with = |at: usize, word: &[char]| chars.len() >= at + word.len() && chars[at..at + word.len()] == *word;
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < chars.len() {
        let boundary = index == 0 || !is_ascii_word(chars[index - 1]);
        if boundary && starts_with(index, &first) {
            let mut cursor = index + first.len();
            while cursor < chars.len() && (is_js_space(chars[cursor]) || chars[cursor] == '-') {
                cursor += 1;
            }
            if starts_with(cursor, &second) {
                let end = cursor + second.len();
                if end == chars.len() || !is_ascii_word(chars[end]) {
                    out.push_str(replacement);
                    index = end;
                    continue;
                }
            }
        }
        out.push(chars[index]);
        index += 1;
    }
    out
}

const DIRECTION_PHRASES: [(&str, &str, &str); 4] =
    [("north", "east", "ne"), ("north", "west", "nw"), ("south", "east", "se"), ("south", "west", "sw")];

/// `normalizeAddressKey(value)`: the deterministic address key shared with
/// the builder.
pub fn normalize_address_key(value: &str) -> String {
    let mut normalized = fold_multi(value, true).to_lowercase();
    for (first, second, replacement) in DIRECTION_PHRASES {
        normalized = replace_direction_phrase(&normalized, first, second, replacement);
    }
    let tokens = normalize_ordinal_tokens(ascii_alnum_runs(&normalized));
    let mapped: Vec<&str> = tokens.iter().map(|token| token_alias(token).unwrap_or(token)).collect();
    mapped.join(" ")
}

fn sorted_join(tokens: &[String]) -> String {
    let mut sorted: Vec<&str> = tokens.iter().map(String::as_str).collect();
    sorted.sort_by(|left, right| crate::util::jsstr::cmp16(left, right));
    sorted.join(" ")
}

/// `normalizeAddressAuthorityKey(value)`: the key's tokens sorted (set-like
/// equality, duplicates kept).
pub fn normalize_address_authority_key(value: &str) -> String {
    let key = normalize_address_key(value);
    if key.is_empty() {
        return String::new();
    }
    let tokens: Vec<String> = key.split(' ').map(str::to_string).collect();
    sorted_join(&tokens)
}

struct Variant {
    tokens: Vec<String>,
    cost: f64,
}

/// `/^[abceghj-nprstvxy]\d[abceghj-nprstvwxyz]$/u`.
fn canadian_postal_fsa(token: &str) -> bool {
    let chars: Vec<char> = token.chars().collect();
    chars.len() == 3 && is_fsa(chars[0]) && is_digit(chars[1]) && is_ldu(chars[2])
}

/// `/^\d[abceghj-nprstvwxyz]\d$/u`.
fn canadian_postal_ldu(token: &str) -> bool {
    let chars: Vec<char> = token.chars().collect();
    chars.len() == 3 && is_digit(chars[0]) && is_ldu(chars[1]) && is_digit(chars[2])
}

fn trailing_keep(token: &str) -> bool {
    token == "o" || TOKEN_ALIASES.iter().any(|(_, to)| *to == token)
}

fn without_index(tokens: &[String], skip: usize) -> Vec<String> {
    tokens.iter().enumerate().filter(|(index, _)| *index != skip).map(|(_, token)| token.clone()).collect()
}

fn unit_letter_variants(tokens: &[String]) -> Vec<Variant> {
    let mut variants = vec![Variant { tokens: tokens.to_vec(), cost: 0.0 }];
    if let Some(number_index) = tokens.iter().position(|token| all_digits(token)) {
        let next = tokens.get(number_index + 1).map(String::as_str).unwrap_or("");
        if lower_letters(next, 1, 1) {
            variants.push(Variant { tokens: without_index(tokens, number_index + 1), cost: 1.0 });
            let merged: Vec<String> = tokens
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != number_index + 1)
                .map(|(index, token)| if index == number_index { format!("{token}{next}") } else { token.clone() })
                .collect();
            variants.push(Variant { tokens: merged, cost: 1.0 });
        }
    }
    if let Some(compact_index) = tokens.iter().position(|token| digits_then_letter(token)) {
        let split: Vec<String> = tokens
            .iter()
            .enumerate()
            .map(|(index, token)| if index == compact_index { token[..token.len() - 1].to_string() } else { token.clone() })
            .collect();
        variants.push(Variant { tokens: split, cost: 1.0 });
    }
    variants
}

fn token_readings(token: &str) -> Option<Vec<Vec<String>>> {
    let readings = ADDRESS_TOKEN_READINGS
        .binary_search_by(|(key, _)| key.as_bytes().cmp(token.as_bytes()))
        .ok()
        .map(|position| ADDRESS_TOKEN_READINGS[position].1);
    let owned = |list: &[&[&str]]| -> Vec<Vec<String>> {
        list.iter().map(|reading| reading.iter().map(|part| part.to_string()).collect()).collect()
    };
    for (abbrev, expansions) in CONCATENATED_SUFFIX_READINGS {
        // Tokens and the table are ASCII, so byte length == UTF-16 length.
        if token.len() <= abbrev.len() + 2 || !token.ends_with(abbrev) {
            continue;
        }
        let stem = &token[..token.len() - abbrev.len()];
        let mut out = readings.map(owned).unwrap_or_default();
        out.extend(expansions.iter().map(|expansion| vec![format!("{stem}{expansion}")]));
        return Some(out);
    }
    readings.map(owned)
}

fn substitute(tokens: &[String], entries: &[(usize, &Vec<String>)]) -> Vec<String> {
    let mut ordered: Vec<&(usize, &Vec<String>)> = entries.iter().collect();
    ordered.sort_by(|left, right| right.0.cmp(&left.0));
    let mut out = tokens.to_vec();
    for (index, reading) in ordered {
        let mut next = out[..*index].to_vec();
        next.extend(reading.iter().cloned());
        next.extend(out[index + 1..].iter().cloned());
        out = next;
    }
    out
}

fn readings_variants(base: &[String], max_variants: usize) -> Vec<Variant> {
    let mut positions: Vec<(usize, Vec<Vec<String>>)> = Vec::new();
    let mut index = 0;
    while index < base.len() && positions.len() < 5 {
        if let Some(readings) = token_readings(&base[index]) {
            if !readings.is_empty() {
                positions.push((index, readings));
            }
        }
        index += 1;
    }
    let mut variants = vec![Variant { tokens: base.to_vec(), cost: 0.0 }];
    if positions.is_empty() {
        return variants;
    }
    for (index, readings) in &positions {
        let cost = if base[*index].chars().count() == 1 { 0.5 } else { 1.0 };
        for reading in readings {
            variants.push(Variant { tokens: substitute(base, &[(*index, reading)]), cost });
            if variants.len() >= max_variants {
                return variants;
            }
        }
    }
    for left in 0..positions.len() {
        for right in left + 1..positions.len() {
            for left_reading in &positions[left].1 {
                for right_reading in &positions[right].1 {
                    variants.push(Variant {
                        tokens: substitute(base, &[(positions[left].0, left_reading), (positions[right].0, right_reading)]),
                        cost: 3.0,
                    });
                    if variants.len() >= max_variants {
                        return variants;
                    }
                }
            }
        }
    }
    variants
}

const UNIT_MARKER_TOKENS: [&str; 4] = ["unit", "apt", "ste", "app"];

const STREET_TYPE_INSERTIONS: [&str; 12] =
    ["st", "rue", "ave", "rd", "dr", "blvd", "ln", "chemin", "ct", "calle", "via", "rua"];

fn is_street_type(token: &str) -> bool {
    STREET_TYPE_TOKENS.binary_search_by(|item| item.as_bytes().cmp(token.as_bytes())).is_ok()
}

fn street_type_insertion_variants(tokens: &[String]) -> Vec<Variant> {
    let mut variants = vec![Variant { tokens: tokens.to_vec(), cost: 0.0 }];
    let Some(number_index) = tokens.iter().position(|token| all_digits(token)) else {
        return variants;
    };
    if tokens.len() < 3 || tokens.iter().any(|token| is_street_type(token)) {
        return variants;
    }
    for kind in STREET_TYPE_INSERTIONS {
        let mut next = tokens[..=number_index].to_vec();
        next.push(kind.to_string());
        next.extend(tokens[number_index + 1..].iter().cloned());
        variants.push(Variant { tokens: next, cost: 2.0 });
    }
    variants
}

fn concatenation_variants(tokens: &[String]) -> Vec<Variant> {
    let mut variants = vec![Variant { tokens: tokens.to_vec(), cost: 0.0 }];
    let mut index = 1;
    while index < tokens.len() && variants.len() <= 2 {
        let is_suffix = CONCATENATED_STREET_SUFFIXES.binary_search_by(|item| item.as_bytes().cmp(tokens[index].as_bytes())).is_ok();
        let stem = &tokens[index - 1];
        if is_suffix && lower_letters(stem, 3, usize::MAX) {
            let mut next = tokens[..index - 1].to_vec();
            next.push(format!("{stem}{}", tokens[index]));
            next.extend(tokens[index + 1..].iter().cloned());
            variants.push(Variant { tokens: next, cost: 1.0 });
        }
        index += 1;
    }
    variants
}

fn unit_shed_variants(tokens: &[String]) -> Vec<Vec<String>> {
    let marker = tokens.iter().enumerate().position(|(index, token)| {
        UNIT_MARKER_TOKENS.contains(&token.as_str())
            && digits_optional_letter(tokens.get(index + 1).map(String::as_str).unwrap_or(""))
    });
    if let Some(marker_index) = marker {
        let shed: Vec<String> = tokens
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != marker_index && *index != marker_index + 1)
            .map(|(_, token)| token.clone())
            .collect();
        return vec![tokens.to_vec(), shed];
    }
    if let Some(lettered) = tokens.iter().position(|token| digits_then_letter(token)) {
        if tokens.len() > 2 && tokens.iter().any(|token| all_digits(token)) {
            return vec![tokens.to_vec(), without_index(tokens, lettered)];
        }
    }
    vec![tokens.to_vec()]
}

fn tail_variants(tokens: &[String]) -> Vec<Variant> {
    let mut variants = Vec::new();
    for (branch, branch_tokens) in unit_shed_variants(tokens).into_iter().enumerate() {
        let mut trimmed = branch_tokens;
        let mut cost = branch as f64;
        variants.push(Variant { tokens: trimmed.clone(), cost });
        let mut push = |next: Vec<String>, trimmed: &mut Vec<String>, cost: &mut f64| {
            if !next.is_empty() && next.len() != trimmed.len() {
                *trimmed = next.clone();
                *cost += 0.5;
                variants.push(Variant { tokens: next, cost: *cost });
            }
        };
        let postal_index = (0..trimmed.len()).find(|index| {
            canadian_postal_fsa(&trimmed[*index])
                && canadian_postal_ldu(trimmed.get(index + 1).map(String::as_str).unwrap_or(""))
        });
        if let Some(postal_index) = postal_index {
            let next: Vec<String> = trimmed
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != postal_index && *index != postal_index + 1)
                .map(|(_, token)| token.clone())
                .collect();
            push(next, &mut trimmed, &mut cost);
        } else if trimmed.len() > 1 && {
            let last = &trimmed[trimmed.len() - 1];
            last.len() == 5 && all_digits(last)
        } && trimmed[..trimmed.len() - 1].iter().any(|token| all_digits(token))
        {
            let next = trimmed[..trimmed.len() - 1].to_vec();
            push(next, &mut trimmed, &mut cost);
        }
        let last = trimmed.last().cloned().unwrap_or_default();
        if trimmed.len() > 2 && lower_letters(&last, 2, 3) && !trailing_keep(&last) {
            let next = trimmed[..trimmed.len() - 1].to_vec();
            push(next, &mut trimmed, &mut cost);
        }
    }
    variants
}

/// `addressQueryTokenVariants(value, maxVariants = 40)`: every plausible
/// reading of a typed address, cheapest first; the first entry is always the
/// canonical `normalizeAddressKey` reading.
pub fn address_query_token_variants(value: &str, max_variants: usize) -> Vec<Vec<String>> {
    let base = normalize_address_key(value);
    if base.is_empty() {
        return Vec::new();
    }
    let base_tokens: Vec<String> = base.split(' ').map(str::to_string).collect();
    struct Candidate {
        tokens: Vec<String>,
        cost: f64,
        order: usize,
    }
    let mut candidates: Vec<Candidate> = Vec::new();
    for reading in readings_variants(&base_tokens, 12) {
        for insertion in street_type_insertion_variants(&reading.tokens) {
            for concatenation in concatenation_variants(&insertion.tokens) {
                for unit in unit_letter_variants(&concatenation.tokens) {
                    for tail in tail_variants(&unit.tokens) {
                        let order = candidates.len();
                        candidates.push(Candidate {
                            tokens: tail.tokens,
                            cost: reading.cost + insertion.cost + concatenation.cost + unit.cost + tail.cost,
                            order,
                        });
                    }
                }
            }
        }
    }
    candidates.sort_by(|left, right| {
        left.cost.partial_cmp(&right.cost).unwrap_or(std::cmp::Ordering::Equal).then(left.order.cmp(&right.order))
    });
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for candidate in candidates {
        let key = candidate.tokens.join(" ");
        if key.is_empty() || !seen.insert(key) {
            continue;
        }
        out.push(candidate.tokens);
        if out.len() >= max_variants {
            break;
        }
    }
    out
}

/// `addressAuthorityQueryKeys(value)`: sorted set-like keys for every
/// reading of a typed address (without the `a|` prefix).
pub fn address_authority_query_keys(value: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut seen = HashSet::new();
    for tokens in address_query_token_variants(value, 40) {
        let key = sorted_join(&tokens);
        if seen.insert(key.clone()) {
            keys.push(key);
        }
    }
    keys
}

/// `/^\s*[abceghj-nprstvxy]\d[abceghj-nprstvwxyz]\s*[0-9][abceghj-nprstvwxyz][0-9]\s*$/iu`.
fn canadian_postal_code_query(value: &str) -> bool {
    let chars: Vec<char> = value.chars().collect();
    let mut index = 0;
    let skip_spaces = |index: &mut usize| {
        while *index < chars.len() && is_js_space(chars[*index]) {
            *index += 1;
        }
    };
    skip_spaces(&mut index);
    if chars.len() < index + 3 || !is_fsa_iu(chars[index]) || !is_digit(chars[index + 1]) || !is_ldu_iu(chars[index + 2]) {
        return false;
    }
    index += 3;
    skip_spaces(&mut index);
    if chars.len() < index + 3 || !is_digit(chars[index]) || !is_ldu_iu(chars[index + 1]) || !is_digit(chars[index + 2]) {
        return false;
    }
    index += 3;
    skip_spaces(&mut index);
    index == chars.len()
}

/// `/^\d+[a-z]?(?:-\d+[a-z]?)?$/u`.
fn house_number_token(token: &str) -> bool {
    match token.split_once('-') {
        None => digits_optional_letter(token),
        Some((left, right)) => digits_optional_letter(left) && digits_optional_letter(right),
    }
}

/// `/^\d+(?:st|nd|rd|th)$/u`.
fn ordinal_number_token(token: &str) -> bool {
    token.len() > 2
        && ["st", "nd", "rd", "th"].iter().any(|suffix| token.ends_with(suffix))
        && all_digits(&token[..token.len() - 2])
}

/// `looksLikeAddressQuery(value)`.
pub fn looks_like_address_query(value: &str) -> bool {
    if canadian_postal_code_query(value) {
        return true;
    }
    let key = normalize_address_key(value);
    if key.is_empty() {
        return false;
    }
    let tokens: Vec<&str> = key.split(' ').collect();
    if tokens.len() < 2 {
        return false;
    }
    tokens.iter().any(|token| house_number_token(token) || ordinal_number_token(token))
}

/// Matches `[abceghj-nprstvxy]\d[abceghj-nprstvwxyz]\s*` at `index` (after a
/// `\b` under the `iu` word set); returns the index after the spaces.
fn postal_head(chars: &[char], index: usize) -> Option<usize> {
    if index > 0 && is_word_iu(chars[index - 1]) {
        return None;
    }
    if chars.len() < index + 3 || !is_fsa_iu(chars[index]) || !is_digit(chars[index + 1]) || !is_ldu_iu(chars[index + 2]) {
        return None;
    }
    let mut cursor = index + 3;
    while cursor < chars.len() && is_js_space(chars[cursor]) {
        cursor += 1;
    }
    Some(cursor)
}

/// `normalizePostalCodeSpacing(value)`:
/// `value.replace(/\b([abceghj-nprstvxy]\d[abceghj-nprstvwxyz])\s*([0-9][abceghj-nprstvwxyz][0-9])\b/giu, "$1 $2")`.
pub fn normalize_postal_code_spacing(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let mut out = String::with_capacity(value.len() + 4);
    let mut index = 0;
    while index < chars.len() {
        if let Some(cursor) = postal_head(&chars, index) {
            let end = cursor + 3;
            if chars.len() >= end
                && is_digit(chars[cursor])
                && is_ldu_iu(chars[cursor + 1])
                && is_digit(chars[cursor + 2])
                && (end == chars.len() || !is_word_iu(chars[end]))
            {
                out.extend(&chars[index..index + 3]);
                out.push(' ');
                out.extend(&chars[cursor..end]);
                index = end;
                continue;
            }
        }
        out.push(chars[index]);
        index += 1;
    }
    out
}

/// `normalizePostalCodePrefixSpacing(value)`:
/// `value.replace(/\b([abceghj-nprstvxy]\d[abceghj-nprstvwxyz])\s*([0-9](?:[abceghj-nprstvwxyz](?:[0-9])?)?)$/iu, "$1 $2")`.
pub fn normalize_postal_code_prefix_spacing(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    for index in 0..chars.len() {
        let Some(cursor) = postal_head(&chars, index) else { continue };
        if cursor >= chars.len() || !is_digit(chars[cursor]) {
            continue;
        }
        let rest = &chars[cursor + 1..];
        let tail_ok = match rest.len() {
            0 => true,
            1 => is_ldu_iu(rest[0]),
            2 => is_ldu_iu(rest[0]) && is_digit(rest[1]),
            _ => false,
        };
        if !tail_ok {
            continue;
        }
        let mut out: String = chars[..index + 3].iter().collect();
        out.push(' ');
        out.extend(&chars[cursor..]);
        return out;
    }
    value.to_string()
}

/// One interpolation-range probe (`addressRangeQueryCandidates` item).
#[derive(Clone, Debug, PartialEq)]
pub struct AddressRangeCandidate {
    /// A safe, non-negative integer.
    pub house_number: u64,
    /// The value passed to `authority_address_range_key`.
    pub lookup_value: String,
}

/// `Number.prototype.toString(36)` for integral doubles (bucket indexes).
fn to_base36(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    let negative = value < 0.0;
    let mut magnitude = value.abs() as u128;
    if magnitude == 0 {
        return "0".into();
    }
    let mut digits = Vec::new();
    while magnitude > 0 {
        digits.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(magnitude % 36) as usize]);
        magnitude /= 36;
    }
    if negative {
        digits.push(b'-');
    }
    digits.reverse();
    String::from_utf8(digits).expect("ascii digits")
}

fn range_lookup_value(bucket: f64, parity: u64, tail: &str) -> String {
    format!("{tail} {} {parity}", to_base36(bucket))
}

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// `addressRangeQueryCandidates(value, bucketSize = 16)`: every plausible
/// numeric house token of every reading, each probed with both parity keys.
pub fn address_range_query_candidates(value: &str, bucket_size: f64) -> Vec<AddressRangeCandidate> {
    let mut candidates = Vec::new();
    let mut seen = HashSet::new();
    for tokens in address_query_token_variants(value, 24) {
        for index in 0..tokens.len() {
            if !all_digits(&tokens[index]) {
                continue;
            }
            let house_number: f64 = tokens[index].parse().unwrap_or(f64::NAN);
            if !(house_number.is_finite() && house_number.trunc() == house_number && house_number.abs() <= MAX_SAFE_INTEGER)
                || house_number < 0.0
            {
                continue;
            }
            let tail = sorted_join(&without_index(&tokens, index));
            if tail.is_empty() {
                continue;
            }
            let house = house_number as u64;
            let parity = house % 2;
            let bucket = (house_number / bucket_size).floor();
            for candidate_parity in [parity, 1 - parity] {
                let lookup_value = range_lookup_value(bucket, candidate_parity, &tail);
                if !seen.insert(lookup_value.clone()) {
                    continue;
                }
                candidates.push(AddressRangeCandidate { house_number: house, lookup_value });
            }
        }
    }
    candidates
}

/// `Math.max` / `Math.min` with JS NaN propagation.
fn js_max(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else {
        left.max(right)
    }
}

fn js_min(left: f64, right: f64) -> f64 {
    if left.is_nan() || right.is_nan() {
        f64::NAN
    } else {
        left.min(right)
    }
}

fn is_integer(value: f64) -> bool {
    value.is_finite() && value.trunc() == value
}

/// `addressRangeContains(start, end, step, houseNumber)`. Arguments are the
/// JS `Number(...)` conversions of the doc fields (a missing field is NaN).
pub fn address_range_contains(start: f64, end: f64, step: f64, house_number: f64) -> bool {
    let increment = js_max(1.0, step.floor());
    if !(is_integer(start) && is_integer(end) && is_integer(house_number)) || start == end {
        return false;
    }
    if house_number < start.min(end) || house_number > start.max(end) {
        return false;
    }
    (house_number - start).abs() % increment == 0.0
}

/// A decoded interpolation-geometry vertex (degrees).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AddressPoint {
    pub lat: f64,
    pub lon: f64,
}

/// `decodeAddressRangeGeometry(encoded)`: Google-style delta polyline at
/// 1e-6 degrees, decoded over UTF-16 code units with int32 bit semantics.
pub fn decode_address_range_geometry(encoded: &str) -> Result<Vec<AddressPoint>> {
    let units: Vec<u16> = encoded.encode_utf16().collect();
    let mut points = Vec::new();
    let mut index = 0usize;
    let mut lat = 0f64;
    let mut lon = 0f64;
    let next_delta = |index: &mut usize| -> Result<f64> {
        let mut result: i32 = 0;
        let mut shift: u32 = 0;
        loop {
            if *index >= units.len() {
                return Err(Error::new("Truncated address interpolation geometry."));
            }
            let value = units[*index] as i32 - 63;
            *index += 1;
            result |= (value & 0x1f).wrapping_shl(shift);
            if value < 0x20 {
                break;
            }
            shift = shift.wrapping_add(5);
        }
        let unsigned = result as u32;
        let half = (unsigned >> 1) as i32;
        Ok(if unsigned & 1 != 0 { !half } else { half } as f64)
    };
    while index < units.len() {
        lat += next_delta(&mut index)?;
        lon += next_delta(&mut index)?;
        points.push(AddressPoint { lat: lat / 1e6, lon: lon / 1e6 });
    }
    Ok(points)
}

/// V8's `Math.hypot` (normalized Kahan summation), bit-for-bit.
fn js_hypot(x: f64, y: f64) -> f64 {
    let values = [x, y];
    let mut max = 0f64;
    let mut nan = false;
    let mut abs = [0f64; 2];
    for (slot, value) in values.iter().enumerate() {
        if value.is_nan() {
            nan = true;
        } else {
            abs[slot] = value.abs();
            if abs[slot] > max {
                max = abs[slot];
            }
        }
    }
    if max == f64::INFINITY {
        return f64::INFINITY;
    }
    if nan {
        return f64::NAN;
    }
    if max == 0.0 {
        return 0.0;
    }
    let mut sum = 0f64;
    let mut compensation = 0f64;
    for value in abs {
        let n = value / max;
        let summand = n * n - compensation;
        let preliminary = sum + summand;
        compensation = (preliminary - sum) - summand;
        sum = preliminary;
    }
    sum.sqrt() * max
}

fn segment_length(left: &AddressPoint, right: &AddressPoint) -> f64 {
    let mean_lat = (left.lat + right.lat) * std::f64::consts::PI / 360.0;
    let dx = (right.lon - left.lon) * super::fdlibm::cos(mean_lat);
    let dy = right.lat - left.lat;
    js_hypot(dx, dy)
}

/// `interpolateAddressRangePoint(encodedGeometry, start, end, houseNumber)`.
/// Numeric arguments are JS `Number(...)` conversions; `Ok(None)` is the JS
/// `null` (empty geometry); a truncated geometry is the JS throw.
pub fn interpolate_address_range_point(
    encoded_geometry: &str,
    start: f64,
    end: f64,
    house_number: f64,
) -> Result<Option<AddressPoint>> {
    let points = decode_address_range_geometry(encoded_geometry)?;
    if points.is_empty() {
        return Ok(None);
    }
    if points.len() == 1 {
        return Ok(Some(points[0]));
    }
    let denominator = end - start;
    let fraction = if denominator != 0.0 && !denominator.is_nan() {
        js_max(0.0, js_min(1.0, (house_number - start) / denominator))
    } else {
        0.0
    };
    let mut lengths = Vec::with_capacity(points.len() - 1);
    let mut total = 0f64;
    for index in 0..points.len() - 1 {
        let length = segment_length(&points[index], &points[index + 1]);
        lengths.push(length);
        total += length;
    }
    if total == 0.0 || total.is_nan() {
        return Ok(Some(points[0]));
    }
    let mut remaining = total * fraction;
    for index in 0..lengths.len() {
        if remaining > lengths[index] && index + 1 < lengths.len() {
            remaining -= lengths[index];
            continue;
        }
        let local = if lengths[index] != 0.0 && !lengths[index].is_nan() { remaining / lengths[index] } else { 0.0 };
        return Ok(Some(AddressPoint {
            lat: points[index].lat + (points[index + 1].lat - points[index].lat) * local,
            lon: points[index].lon + (points[index + 1].lon - points[index].lon) * local,
        }));
    }
    Ok(points.last().copied())
}
