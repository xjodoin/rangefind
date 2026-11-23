//! Small helpers for the dynamic JSON the engine reads (manifests, query
//! params) and writes (responses). They mirror the JS idioms the runtime
//! uses — optional chaining, `Number(x || default)`, truthiness — so ported
//! logic keeps the same edge-case behavior.

use serde_json::{Map, Number, Value};

pub static NULL: Value = Value::Null;

/// `value?.a?.b?.c`
pub fn path<'a>(value: &'a Value, keys: &[&str]) -> &'a Value {
    let mut current = value;
    for key in keys {
        match current.get(*key) {
            Some(next) => current = next,
            None => return &NULL,
        }
    }
    current
}

/// JS truthiness.
pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().map(|n| n != 0.0 && !n.is_nan()).unwrap_or(false),
        Value::String(text) => !text.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// `Number(value)` for the JSON subset: NaN for anything non-numeric.
pub fn to_number(value: &Value) -> f64 {
    match value {
        Value::Null => 0.0,
        Value::Bool(flag) => {
            if *flag {
                1.0
            } else {
                0.0
            }
        }
        Value::Number(number) => number.as_f64().unwrap_or(f64::NAN),
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                0.0
            } else {
                parse_js_number(trimmed)
            }
        }
        Value::Array(items) => match items.len() {
            0 => 0.0,
            1 => to_number(&items[0]),
            _ => f64::NAN,
        },
        Value::Object(_) => f64::NAN,
    }
}

fn parse_js_number(text: &str) -> f64 {
    if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
        return u64::from_str_radix(hex, 16).map(|v| v as f64).unwrap_or(f64::NAN);
    }
    match text {
        "Infinity" | "+Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        _ => {
            // Rust accepts "inf"/"nan" spellings JS rejects.
            if text.bytes().all(|b| b.is_ascii_digit() || matches!(b, b'+' | b'-' | b'.' | b'e' | b'E')) {
                text.parse::<f64>().unwrap_or(f64::NAN)
            } else {
                f64::NAN
            }
        }
    }
}

/// `Number(value || fallback)`
pub fn num_or(value: &Value, fallback: f64) -> f64 {
    if truthy(value) {
        to_number(value)
    } else {
        fallback
    }
}

/// `value ?? fallback` then `Number(...)`.
pub fn num_nullish(value: &Value, fallback: f64) -> f64 {
    if value.is_null() {
        fallback
    } else {
        to_number(value)
    }
}

pub fn finite(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64().filter(|n| n.is_finite()),
        _ => None,
    }
}

pub fn as_usize(value: &Value) -> Option<usize> {
    finite(value).filter(|n| *n >= 0.0).map(|n| n as usize)
}

pub fn str_of(value: &Value) -> Option<&str> {
    value.as_str()
}

/// `String(value || "")`
pub fn string_or_empty(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        Value::Bool(false) => String::new(),
        Value::Bool(true) => "true".to_string(),
        Value::Number(number) => {
            let n = number.as_f64().unwrap_or(0.0);
            if n == 0.0 || n.is_nan() {
                String::new()
            } else {
                js_number_to_string(n)
            }
        }
        other => other.to_string(),
    }
}

/// `String(number)` for finite doubles in the range the engine handles.
pub fn js_number_to_string(n: f64) -> String {
    if n.is_nan() {
        return "NaN".into();
    }
    if n.is_infinite() {
        return if n > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    if n == n.trunc() && n.abs() < 1e21 {
        return format!("{}", n as i128);
    }
    let mut buffer = serde_json::to_string(&n).unwrap_or_default();
    // serde prints 1e-7 as "1e-7" like JS; exponents >= 21 print as "1e21"
    // in both. Nothing further to normalize for the values we format.
    if buffer.ends_with(".0") {
        buffer.truncate(buffer.len() - 2);
    }
    buffer
}

/// Encode a double the way `JSON.stringify` would: integral values without
/// a fractional part, non-finite values as null.
pub fn num(n: f64) -> Value {
    if !n.is_finite() {
        return Value::Null;
    }
    if n == n.trunc() && n.abs() <= 9_007_199_254_740_992.0 {
        if n == 0.0 {
            return Value::Number(Number::from(0));
        }
        return Value::Number(Number::from(n as i64));
    }
    Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null)
}

pub fn unum(n: usize) -> Value {
    Value::Number(Number::from(n as u64))
}

pub fn obj() -> Map<String, Value> {
    Map::new()
}

/// `Object.keys(value || {}).length`
pub fn key_count(value: &Value) -> usize {
    value.as_object().map(|map| map.len()).unwrap_or(0)
}

/// Shallow `Object.assign(target, source)`.
pub fn assign(target: &mut Value, source: &Value) {
    if let (Some(target_map), Some(source_map)) = (target.as_object_mut(), source.as_object()) {
        for (key, value) in source_map {
            target_map.insert(key.clone(), value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn numbers_follow_js() {
        assert_eq!(to_number(&json!("12")), 12.0);
        assert_eq!(to_number(&json!("")), 0.0);
        assert!(to_number(&json!("abc")).is_nan());
        assert!(to_number(&json!("nan")).is_nan());
        assert_eq!(num_or(&json!(0), 10.0), 10.0);
        assert_eq!(num_nullish(&json!(0), 10.0), 0.0);
        assert_eq!(num(3.0), json!(3));
        assert_eq!(num(3.5), json!(3.5));
        assert_eq!(js_number_to_string(12.0), "12");
        assert_eq!(js_number_to_string(0.5), "0.5");
    }

    #[test]
    fn paths_and_truthiness() {
        let value = json!({ "a": { "b": [1] }, "z": "" });
        assert!(truthy(path(&value, &["a", "b"])));
        assert!(!truthy(path(&value, &["a", "x", "y"])));
        assert!(!truthy(path(&value, &["z"])));
    }
}
