//! JavaScript value and regex-character semantics shared by the OSM
//! query-time modules (constraints, opening hours, schema, Maps adapter).
//!
//! `Option<&Value>` models a JS property read: `None` is `undefined` (a
//! missing key, or a property read on a non-object), `Some(Value::Null)` is
//! `null`. The distinction matters for `??` chains and `Number(...)`.

use serde_json::{Map, Value};

use crate::util::json::{to_number, truthy};

/// `/\s/u`: ECMAScript WhiteSpace and LineTerminator code points.
pub(crate) fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n'
            | '\u{0b}'
            | '\u{0c}'
            | '\r'
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

/// `String.prototype.trim`.
pub(crate) fn js_trim(value: &str) -> &str {
    value.trim_matches(is_js_space)
}

/// Regex canonicalization under the `iu` flags, restricted to what the OSM
/// patterns need: every pattern letter is lowercase ASCII, and the only
/// non-ASCII code points whose simple case folding lands on ASCII are
/// U+017F LATIN SMALL LETTER LONG S (s) and U+212A KELVIN SIGN (k).
pub(crate) fn fold_iu(c: char) -> char {
    match c {
        'A'..='Z' => c.to_ascii_lowercase(),
        '\u{17f}' => 's',
        '\u{212a}' => 'k',
        other => other,
    }
}

/// `\w` under the `iu` flags (`[A-Za-z0-9_]` plus the two extra word
/// characters whose case folding is ASCII).
pub(crate) fn is_word_iu(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '\u{17f}' || c == '\u{212a}'
}

/// `\w` under the `u` flag alone.
pub(crate) fn is_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Case-insensitive (`iu`) match of `pattern` (lowercase ASCII) against the
/// chars of `text` starting at byte `at`; returns the end byte offset.
pub(crate) fn match_iu(text: &str, at: usize, pattern: &str) -> Option<usize> {
    let mut chars = text[at..].char_indices();
    for expected in pattern.chars() {
        let (_, actual) = chars.next()?;
        if fold_iu(actual) != expected {
            return None;
        }
    }
    Some(chars.next().map(|(offset, _)| at + offset).unwrap_or(text.len()))
}

/// `Number(n).toString()`.
pub(crate) fn js_number_string(n: f64) -> String {
    if n.is_nan() {
        return "NaN".into();
    }
    if n == 0.0 {
        return "0".into();
    }
    if n.is_infinite() {
        return if n > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    let sign = if n < 0.0 { "-" } else { "" };
    // Shortest round-trip digits, as `d.ddddde±x`.
    let sci = format!("{:e}", n.abs());
    let (mantissa, exponent) = sci.split_once('e').expect("scientific format");
    let exponent: i32 = exponent.parse().expect("exponent");
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let k = digits.len() as i32;
    let point = exponent + 1;
    let body = if k <= point && point <= 21 {
        format!("{digits}{}", "0".repeat((point - k) as usize))
    } else if 0 < point && point <= 21 {
        format!("{}.{}", &digits[..point as usize], &digits[point as usize..])
    } else if -6 < point && point <= 0 {
        format!("0.{}{digits}", "0".repeat((-point) as usize))
    } else {
        let exp_sign = if point - 1 < 0 { "-" } else { "+" };
        let tail = if k > 1 { format!(".{}", &digits[1..]) } else { String::new() };
        format!("{}{tail}e{exp_sign}{}", &digits[..1], (point - 1).abs())
    };
    format!("{sign}{body}")
}

/// `String(value)` for a defined JSON value.
pub(crate) fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => js_number_string(number.as_f64().unwrap_or(f64::NAN)),
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| if item.is_null() { String::new() } else { js_string(item) })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

/// `String(value || "")`.
pub(crate) fn js_string_or_empty(value: Option<&Value>) -> String {
    match value {
        Some(value) if truthy(value) => js_string(value),
        _ => String::new(),
    }
}

/// `String(value)` where `undefined` prints as `"undefined"` (template
/// literals).
pub(crate) fn js_template(value: Option<&Value>) -> String {
    value.map(js_string).unwrap_or_else(|| "undefined".into())
}

pub(crate) fn is_truthy(value: Option<&Value>) -> bool {
    value.map(truthy).unwrap_or(false)
}

/// `value != null`.
pub(crate) fn is_present(value: Option<&Value>) -> bool {
    matches!(value, Some(value) if !value.is_null())
}

/// `value?.[key]`.
pub(crate) fn prop<'a>(value: Option<&'a Value>, key: &str) -> Option<&'a Value> {
    value.and_then(|value| value.as_object()).and_then(|map| map.get(key))
}

/// `left || right`.
pub(crate) fn or<'a>(left: Option<&'a Value>, right: Option<&'a Value>) -> Option<&'a Value> {
    if is_truthy(left) {
        left
    } else {
        right
    }
}

/// `left ?? right`.
pub(crate) fn nullish<'a>(left: Option<&'a Value>, right: Option<&'a Value>) -> Option<&'a Value> {
    if is_present(left) {
        left
    } else {
        right
    }
}

/// `Number(value)`; `undefined` is NaN, `null` is 0.
pub(crate) fn js_number(value: Option<&Value>) -> f64 {
    match value {
        None => f64::NAN,
        Some(Value::Array(items)) => {
            // `Number([x])` is `Number(String([x]))`.
            let text = js_string(&Value::Array(items.clone()));
            to_number(&Value::String(text))
        }
        Some(value) => to_number(value),
    }
}

/// Object spread `{ ...value }` source entries: own enumerable properties of
/// objects, index keys of arrays and strings, nothing for primitives.
pub(crate) fn spread_into(target: &mut Map<String, Value>, value: Option<&Value>) {
    match value {
        Some(Value::Object(map)) => {
            for (key, item) in map {
                target.insert(key.clone(), item.clone());
            }
        }
        Some(Value::Array(items)) => {
            for (index, item) in items.iter().enumerate() {
                target.insert(index.to_string(), item.clone());
            }
        }
        Some(Value::String(text)) => {
            for (index, unit) in text.encode_utf16().enumerate() {
                target.insert(index.to_string(), Value::String(String::from_utf16_lossy(&[unit])));
            }
        }
        _ => {}
    }
}

/// Insert `key: value` unless the value is `undefined` (which
/// `JSON.stringify` would drop).
pub(crate) fn put(target: &mut Map<String, Value>, key: &str, value: Option<&Value>) {
    if let Some(value) = value {
        target.insert(key.to_string(), value.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers_print_like_js() {
        assert_eq!(js_number_string(1.0), "1");
        assert_eq!(js_number_string(-12.5), "-12.5");
        assert_eq!(js_number_string(1e21), "1e+21");
        assert_eq!(js_number_string(1.5e-7), "1.5e-7");
        assert_eq!(js_number_string(0.000001), "0.000001");
        assert_eq!(js_number_string(123456789012345680000.0), "123456789012345680000");
        assert_eq!(js_number_string(0.1), "0.1");
    }

    #[test]
    fn iu_matching_folds_long_s_and_kelvin() {
        assert_eq!(match_iu("ſHop", 0, "shop"), Some("ſHop".len()));
        assert_eq!(match_iu("\u{212a}ey", 0, "key"), Some(5));
        assert_eq!(match_iu("sho", 0, "shop"), None);
    }
}
