//! JavaScript semantics the OSM query layer depends on: `String(x)`,
//! `Number(x)`, `toFixed`, `localeCompare`, and hand-ported versions of the
//! regular expressions in `query.js` (PORTING.md: no regex crate).

use serde_json::{Map, Value};

use crate::util::json::{to_number, truthy};

pub type Obj = Map<String, Value>;

pub static UNDEFINED: Value = Value::Null;

/// `value?.[key]`, `None` when the key is absent (JS `undefined`).
pub fn field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    value.as_object().and_then(|map| map.get(key))
}

/// `value?.[key]` with `undefined` and `null` both mapped to `&Value::Null`.
pub fn at<'a>(value: &'a Value, key: &str) -> &'a Value {
    field(value, key).unwrap_or(&UNDEFINED)
}

/// `{ ...value }` (non-objects spread to `{}`).
pub fn spread(value: &Value) -> Obj {
    value.as_object().cloned().unwrap_or_default()
}

/// `value == null` with `undefined` (absent) and `null` both nullish.
pub fn nullish(value: Option<&Value>) -> bool {
    matches!(value, None | Some(Value::Null))
}

pub fn is_truthy(value: Option<&Value>) -> bool {
    value.map(truthy).unwrap_or(false)
}

/// `Number(value)` where an absent value is `undefined` (NaN) and `null` is 0.
pub fn number(value: Option<&Value>) -> f64 {
    match value {
        None => f64::NAN,
        Some(value) => to_number(value),
    }
}

/// `Number(value || fallback)`.
pub fn number_or(value: Option<&Value>, fallback: f64) -> f64 {
    match value {
        Some(value) if truthy(value) => to_number(value),
        _ => fallback,
    }
}

/// `Number(value ?? fallback)`.
pub fn number_nullish(value: Option<&Value>, fallback: f64) -> f64 {
    match value {
        None | Some(Value::Null) => fallback,
        Some(value) => to_number(value),
    }
}

/// `Number.isFinite(value)`: true only for actual finite numbers.
pub fn finite_number(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(number)) => number.as_f64().filter(|n| n.is_finite()),
        _ => None,
    }
}

pub fn js_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else if a == 0.0 && b == 0.0 {
        if a.is_sign_negative() {
            b
        } else {
            a
        }
    } else if a > b {
        a
    } else {
        b
    }
}

pub fn js_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else if a == 0.0 && b == 0.0 {
        if a.is_sign_negative() {
            a
        } else {
            b
        }
    } else if a < b {
        a
    } else {
        b
    }
}

/// A JSON number the way `JSON.stringify` writes a double.
pub fn num(n: f64) -> Value {
    crate::util::json::num(n)
}

/// `String(number)` (Number::toString(10)).
pub fn number_to_string(n: f64) -> String {
    if n.is_nan() {
        return "NaN".into();
    }
    if n.is_infinite() {
        return if n > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    if n == 0.0 {
        return "0".into();
    }
    // Shortest round-trip digits from Rust's exponent formatting.
    let formatted = format!("{:e}", n.abs());
    let (mantissa, exponent) = formatted.split_once('e').unwrap_or((&formatted, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let k = digits.len() as i32;
    let point = exponent + 1; // JS "n"
    let mut out = String::new();
    if n < 0.0 {
        out.push('-');
    }
    if k <= point && point <= 21 {
        out.push_str(&digits);
        for _ in 0..(point - k) {
            out.push('0');
        }
    } else if 0 < point && point <= 21 {
        out.push_str(&digits[..point as usize]);
        out.push('.');
        out.push_str(&digits[point as usize..]);
    } else if -6 < point && point <= 0 {
        out.push_str("0.");
        for _ in 0..(-point) {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        let e = point - 1;
        out.push_str(&digits[..1]);
        if k > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if e < 0 { '-' } else { '+' });
        out.push_str(&e.abs().to_string());
    }
    out
}

/// `String(value)` with `undefined` for an absent value.
pub fn js_string(value: Option<&Value>) -> String {
    match value {
        None => "undefined".into(),
        Some(Value::Null) => "null".into(),
        Some(Value::Bool(flag)) => flag.to_string(),
        Some(Value::Number(number)) => number_to_string(number.as_f64().unwrap_or(f64::NAN)),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => js_string(Some(other)),
            })
            .collect::<Vec<_>>()
            .join(","),
        Some(Value::Object(_)) => "[object Object]".into(),
    }
}

/// `String(value || "")`.
pub fn js_string_or_empty(value: Option<&Value>) -> String {
    if is_truthy(value) {
        js_string(value)
    } else {
        String::new()
    }
}

/// `Number.prototype.toFixed(digits)` for |x| < 1e21 (exact decimal
/// rounding of the double, ties away from zero like the spec).
pub fn to_fixed(x: f64, digits: usize) -> String {
    if x.is_nan() {
        return "NaN".into();
    }
    if x.abs() >= 1e21 {
        return number_to_string(x);
    }
    let negative = x < 0.0;
    let magnitude = x.abs();
    // The exact decimal expansion of a double below 1e21 has at most 1074
    // fractional digits; 1100 keeps it exact so ties are visible.
    let exact = format!("{:.1100}", magnitude);
    let (int_part, frac_part) = exact.split_once('.').unwrap_or((&exact, ""));
    let mut digits_vec: Vec<u8> = int_part.bytes().chain(frac_part.bytes().take(digits)).map(|b| b - b'0').collect();
    let rest = &frac_part[digits.min(frac_part.len())..];
    let round_up = rest.as_bytes().first().map(|b| *b >= b'5').unwrap_or(false);
    if round_up {
        let mut index = digits_vec.len();
        loop {
            if index == 0 {
                digits_vec.insert(0, 1);
                break;
            }
            index -= 1;
            if digits_vec[index] == 9 {
                digits_vec[index] = 0;
            } else {
                digits_vec[index] += 1;
                break;
            }
        }
    }
    let int_len = digits_vec.len() - digits;
    let mut out = String::new();
    let all_zero = digits_vec.iter().all(|d| *d == 0);
    if negative && !(all_zero && x == 0.0) {
        out.push('-');
    }
    for d in &digits_vec[..int_len] {
        out.push((b'0' + d) as char);
    }
    if digits > 0 {
        out.push('.');
        for d in &digits_vec[int_len..] {
            out.push((b'0' + d) as char);
        }
    }
    out
}

/// `/\s/u`: WhiteSpace plus LineTerminator (the set `trim` strips).
pub fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}'
            | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'
    )
}

/// `/[\p{L}\p{N}]/u`.
pub fn is_letter_or_number(c: char) -> bool {
    use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};
    matches!(c.general_category_group(), GeneralCategoryGroup::Letter | GeneralCategoryGroup::Number)
}

/// `String.prototype.trim`.
pub fn trim(value: &str) -> &str {
    value.trim_matches(is_js_space)
}

/// `value.split(/\s+/u).filter(Boolean)`.
pub fn split_ws(value: &str) -> Vec<&str> {
    value.split(is_js_space).filter(|token| !token.is_empty()).collect()
}

/// `value.split(/[^\p{L}\p{N}]+/u)` (keeps empty edge tokens like JS).
pub fn split_non_alnum(value: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut piece_start = 0usize;
    let mut previous_separator = false;
    for (index, ch) in value.char_indices() {
        if is_letter_or_number(ch) {
            if previous_separator {
                piece_start = index;
            }
            previous_separator = false;
        } else {
            if !previous_separator {
                out.push(&value[piece_start..index]);
            }
            previous_separator = true;
            piece_start = index + ch.len_utf8();
        }
    }
    out.push(&value[piece_start..]);
    out
}

/// `value.split(/[^\p{L}\p{N}]+/u).filter(Boolean)`.
pub fn alnum_tokens(value: &str) -> Vec<&str> {
    value.split(|c: char| !is_letter_or_number(c)).filter(|token| !token.is_empty()).collect()
}

/// `value.replace(/[^\p{L}\p{N}]+/gu, "")`.
pub fn strip_non_alnum(value: &str) -> String {
    value.chars().filter(|c| is_letter_or_number(*c)).collect()
}

/// UTF-16 length (`.length`).
pub fn len16(value: &str) -> usize {
    crate::util::jsstr::len16(value)
}

/// `/\d/u.test(value)`.
pub fn has_ascii_digit(value: &str) -> bool {
    value.bytes().any(|b| b.is_ascii_digit())
}

/// `/[^\x00-\x7f]/u.test(value)`.
pub fn has_non_ascii(value: &str) -> bool {
    !value.is_ascii()
}

/// `DECIMAL_COORDINATES` exec: `^\s*(NUM)(?:\s*,\s*|\s+)(NUM)\s*$` with
/// `NUM = [+-]?(?:\d+(?:\.\d*)?|\.\d+)`.
pub fn match_decimal_coordinates(value: &str) -> Option<(String, String)> {
    let trimmed: Vec<char> = trim(value).chars().collect();
    let first_end = scan_coordinate_number(&trimmed, 0)?;
    let mut index = first_end;
    let spaces_start = index;
    while index < trimmed.len() && is_js_space(trimmed[index]) {
        index += 1;
    }
    if index < trimmed.len() && trimmed[index] == ',' {
        index += 1;
        while index < trimmed.len() && is_js_space(trimmed[index]) {
            index += 1;
        }
    } else if index == spaces_start {
        return None;
    }
    let second_start = index;
    let second_end = scan_coordinate_number(&trimmed, second_start)?;
    if second_end != trimmed.len() {
        return None;
    }
    Some((trimmed[..first_end].iter().collect(), trimmed[second_start..second_end].iter().collect()))
}

fn scan_coordinate_number(chars: &[char], start: usize) -> Option<usize> {
    let mut index = start;
    if index < chars.len() && (chars[index] == '+' || chars[index] == '-') {
        index += 1;
    }
    let digits_start = index;
    while index < chars.len() && chars[index].is_ascii_digit() {
        index += 1;
    }
    if index > digits_start {
        if index < chars.len() && chars[index] == '.' {
            index += 1;
            while index < chars.len() && chars[index].is_ascii_digit() {
                index += 1;
            }
        }
        return Some(index);
    }
    if index < chars.len() && chars[index] == '.' {
        let after = index + 1;
        let mut end = after;
        while end < chars.len() && chars[end].is_ascii_digit() {
            end += 1;
        }
        if end > after {
            return Some(end);
        }
    }
    None
}

/// `Number(text)` for a string matched by the coordinate grammar.
pub fn parse_number_text(text: &str) -> f64 {
    to_number(&Value::String(text.to_string()))
}

/// `INTERSECTION_CONNECTOR.exec(surface)`:
/// `/\s+(?:&|and|at|et|x)\s+|\s*\/\s*/iu`. Returns the match as char
/// offsets `(start, end)`.
pub fn match_intersection_connector(chars: &[char]) -> Option<(usize, usize)> {
    let lower = |c: char| c.to_ascii_lowercase();
    for start in 0..=chars.len() {
        // Alternative 1: \s+ (?:&|and|at|et|x) \s+
        if start < chars.len() && is_js_space(chars[start]) {
            let mut index = start;
            while index < chars.len() && is_js_space(chars[index]) {
                index += 1;
            }
            for word in ["&", "and", "at", "et", "x"] {
                let word_chars: Vec<char> = word.chars().collect();
                if index + word_chars.len() > chars.len() {
                    continue;
                }
                if !word_chars.iter().enumerate().all(|(offset, c)| lower(chars[index + offset]) == *c) {
                    continue;
                }
                let mut end = index + word_chars.len();
                let after = end;
                while end < chars.len() && is_js_space(chars[end]) {
                    end += 1;
                }
                if end > after {
                    return Some((start, end));
                }
            }
        }
        // Alternative 2: \s* \/ \s*
        let mut index = start;
        while index < chars.len() && is_js_space(chars[index]) {
            index += 1;
        }
        if index < chars.len() && chars[index] == '/' {
            let mut end = index + 1;
            while end < chars.len() && is_js_space(chars[end]) {
                end += 1;
            }
            return Some((start, end));
        }
    }
    None
}

/// `value.replaceAll(/\s*,\s*/gu, " ")`.
pub fn collapse_commas(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let mut out = String::with_capacity(value.len());
    let mut index = 0;
    while index < chars.len() {
        let mut probe = index;
        while probe < chars.len() && is_js_space(chars[probe]) {
            probe += 1;
        }
        if probe < chars.len() && chars[probe] == ',' {
            probe += 1;
            while probe < chars.len() && is_js_space(chars[probe]) {
                probe += 1;
            }
            out.push(' ');
            index = probe;
            continue;
        }
        out.push(chars[index]);
        index += 1;
    }
    out
}

/// `/^\d+[\p{L}]?(?:-\d+[\p{L}]?)?$/u.test(token)`.
pub fn is_house_number_token(token: &str) -> bool {
    let chars: Vec<char> = token.chars().collect();
    let mut index = 0;
    let digits = |chars: &[char], mut index: usize| {
        let start = index;
        while index < chars.len() && chars[index].is_ascii_digit() {
            index += 1;
        }
        (index > start).then_some(index)
    };
    let letter = |chars: &[char], index: usize| {
        if index < chars.len() && is_letter(chars[index]) {
            index + 1
        } else {
            index
        }
    };
    let Some(next) = digits(&chars, index) else { return false };
    index = letter(&chars, next);
    if index == chars.len() {
        return true;
    }
    if chars[index] != '-' {
        return false;
    }
    let Some(next) = digits(&chars, index + 1) else { return false };
    letter(&chars, next) == chars.len()
}

/// `/\p{L}/u`.
pub fn is_letter(c: char) -> bool {
    use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};
    c.general_category_group() == GeneralCategoryGroup::Letter
}

/// `value.split(/[^\p{L}\p{N}-]+/u)`.
pub fn split_house_number_tokens(value: &str) -> Vec<&str> {
    value.split(|c: char| !(is_letter_or_number(c) || c == '-')).collect()
}

/// `value.replace(/^\s*\d+[\p{L}]?(?:\s*[-–—]\s*\d+[\p{L}]?)?\s+/u, "")`.
pub fn strip_leading_house_number(value: &str) -> String {
    let chars: Vec<char> = value.chars().collect();
    let mut index = 0;
    while index < chars.len() && is_js_space(chars[index]) {
        index += 1;
    }
    let digits_start = index;
    while index < chars.len() && chars[index].is_ascii_digit() {
        index += 1;
    }
    if index == digits_start {
        return value.to_string();
    }
    if index < chars.len() && is_letter(chars[index]) {
        index += 1;
    }
    // Optional range group: \s*[-–—]\s*\d+[\p{L}]?
    let mut group = index;
    while group < chars.len() && is_js_space(chars[group]) {
        group += 1;
    }
    if group < chars.len() && matches!(chars[group], '-' | '–' | '—') {
        group += 1;
        while group < chars.len() && is_js_space(chars[group]) {
            group += 1;
        }
        let second = group;
        while group < chars.len() && chars[group].is_ascii_digit() {
            group += 1;
        }
        if group > second {
            if group < chars.len() && is_letter(chars[group]) {
                group += 1;
            }
            if group < chars.len() && is_js_space(chars[group]) {
                index = group;
            }
        }
    }
    let spaces = index;
    while index < chars.len() && is_js_space(chars[index]) {
        index += 1;
    }
    if index == spaces {
        return value.to_string();
    }
    chars[index..].iter().collect()
}

/// `CANADIAN_POSTAL_CODE` exec:
/// `^\s*([abceghj-nprstvxy]\d[abceghj-nprstvwxyz])\s*([0-9][abceghj-nprstvwxyz][0-9])\s*$/iu`.
pub fn match_canadian_postal_code(value: &str) -> Option<(String, String)> {
    // Under `iu`, U+017F folds to s and U+212A to k.
    let fold_iu = |c: char| match c {
        'A'..='Z' => c.to_ascii_lowercase(),
        '\u{17f}' => 's',
        '\u{212a}' => 'k',
        other => other,
    };
    let fsa = |c: char| matches!(c, 'a' | 'b' | 'c' | 'e' | 'g' | 'h' | 'j'..='n' | 'p' | 'r' | 's' | 't' | 'v' | 'x' | 'y');
    let is_fsa_iu = |c: char| fsa(fold_iu(c));
    let is_ldu_iu = |c: char| {
        let c = fold_iu(c);
        fsa(c) || c == 'w' || c == 'z'
    };
    let chars: Vec<char> = trim(value).chars().collect();
    if chars.len() < 6 {
        return None;
    }
    if !(is_fsa_iu(chars[0]) && chars[1].is_ascii_digit() && is_ldu_iu(chars[2])) {
        return None;
    }
    let mut index = 3;
    while index < chars.len() && is_js_space(chars[index]) {
        index += 1;
    }
    if chars.len() - index != 3 {
        return None;
    }
    if !(chars[index].is_ascii_digit() && is_ldu_iu(chars[index + 1]) && chars[index + 2].is_ascii_digit()) {
        return None;
    }
    Some((chars[..3].iter().collect(), chars[index..].iter().collect()))
}

/// `Array.from(value).length` (code points).
pub fn code_points(value: &str) -> usize {
    value.chars().count()
}

/// `JSON.stringify` of a string or null, used for composite map keys.
pub fn json_key(parts: &[Value]) -> String {
    Value::Array(parts.to_vec()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_print_like_js() {
        assert_eq!(number_to_string(45.5019), "45.5019");
        assert_eq!(number_to_string(-73.5674), "-73.5674");
        assert_eq!(number_to_string(1e-7), "1e-7");
        assert_eq!(number_to_string(0.000001), "0.000001");
        assert_eq!(number_to_string(1e21), "1e+21");
        assert_eq!(number_to_string(123.0), "123");
        assert_eq!(to_fixed(0.0078125, 6), "0.007813");
        assert_eq!(to_fixed(-0.0, 6), "0.000000");
        assert_eq!(to_fixed(45.5019, 6), "45.501900");
        assert_eq!(to_fixed(-73.5674, 6), "-73.567400");
        assert_eq!(to_fixed(-1e-7, 6), "-0.000000");
    }

    #[test]
    fn coordinate_grammar() {
        assert_eq!(match_decimal_coordinates("45.5019, -73.5674"), Some(("45.5019".into(), "-73.5674".into())));
        assert_eq!(match_decimal_coordinates(" 45.5 -73 "), Some(("45.5".into(), "-73".into())));
        assert_eq!(match_decimal_coordinates("45.5,.5"), Some(("45.5".into(), ".5".into())));
        assert_eq!(match_decimal_coordinates("4551"), None);
        assert_eq!(match_decimal_coordinates("845 Sherbrooke"), None);
        assert_eq!(match_decimal_coordinates("1. ,2."), Some(("1.".into(), "2.".into())));
    }

    #[test]
    fn regex_ports() {
        let chars: Vec<char> = "Rue A & Rue B".chars().collect();
        assert_eq!(match_intersection_connector(&chars), Some((5, 8)));
        let chars: Vec<char> = "Rue A/Rue B".chars().collect();
        assert_eq!(match_intersection_connector(&chars), Some((5, 6)));
        assert_eq!(collapse_commas("a , b,c"), "a b c");
        assert!(is_house_number_token("12"));
        assert!(is_house_number_token("12a-14b"));
        assert!(!is_house_number_token("a12"));
        assert_eq!(strip_leading_house_number(" 12 - 14 rue x"), "rue x");
        assert_eq!(strip_leading_house_number("12a rue"), "rue");
        assert_eq!(strip_leading_house_number("12 -rue"), "-rue");
        assert_eq!(split_non_alnum("-a--b-"), vec!["", "a", "b", ""]);
        assert_eq!(split_non_alnum("ab"), vec!["ab"]);
        assert_eq!(split_non_alnum(""), vec![""]);
        assert_eq!(match_canadian_postal_code(" h2x 1y4 "), Some(("h2x".into(), "1y4".into())));
    }
}
