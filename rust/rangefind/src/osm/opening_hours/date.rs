//! `new Date(value).getTime()` for the values the evaluator accepts as
//! `options.at`: epoch milliseconds, booleans, and date strings in the
//! ECMAScript date-time format (with V8's leniencies: `t`/space separators,
//! lowercase `z`, `±HHMM` offsets, 1+ fraction digits, day overflow up to
//! 31). V8's legacy fallback formats ("Aug 3 2026", RFC 2822, "2026-8-3")
//! are not parsed and yield NaN.

use serde_json::Value;

use super::tz::{days_from_civil, Zone};
use crate::osm::constraints::js::js_string;

const MS_PER_DAY: f64 = 86_400_000.0;

/// `TimeClip(t)`.
pub(crate) fn time_clip(t: f64) -> f64 {
    if !t.is_finite() || t.abs() > 8.64e15 {
        return f64::NAN;
    }
    t.trunc() + 0.0
}

/// `new Date(value).getTime()` for a defined, non-null `value`. `local`
/// resolves local date-times (no offset) to UTC.
pub(crate) fn date_value(value: &Value, local: &dyn Fn() -> std::rc::Rc<Zone>) -> f64 {
    match value {
        Value::Null => f64::NAN,
        Value::Bool(flag) => f64::from(u8::from(*flag)),
        Value::Number(number) => time_clip(number.as_f64().unwrap_or(f64::NAN)),
        Value::String(text) => parse_date(text, local),
        Value::Array(_) => parse_date(&js_string(value), local),
        Value::Object(_) => f64::NAN,
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn eat(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn digits(&mut self, count: usize) -> Option<i64> {
        let slice = self.bytes.get(self.at..self.at + count)?;
        if !slice.iter().all(u8::is_ascii_digit) {
            return None;
        }
        self.at += count;
        Some(slice.iter().fold(0i64, |acc, b| acc * 10 + i64::from(b - b'0')))
    }
}

fn parse_date(text: &str, local: &dyn Fn() -> std::rc::Rc<Zone>) -> f64 {
    parse_iso(text, local).map(time_clip).unwrap_or(f64::NAN)
}

fn parse_iso(text: &str, local: &dyn Fn() -> std::rc::Rc<Zone>) -> Option<f64> {
    let mut cursor = Cursor { bytes: text.as_bytes(), at: 0 };
    let year = match cursor.peek()? {
        sign @ (b'+' | b'-') => {
            cursor.at += 1;
            let value = cursor.digits(6)?;
            if sign == b'-' && value == 0 {
                return None;
            }
            if sign == b'-' {
                -value
            } else {
                value
            }
        }
        _ => cursor.digits(4)?,
    };
    let mut month = 1;
    let mut day = 1;
    if cursor.eat(b'-') {
        month = cursor.digits(2)?;
        if !(1..=12).contains(&month) {
            return None;
        }
        if cursor.eat(b'-') {
            day = cursor.digits(2)?;
            if !(1..=31).contains(&day) {
                return None;
            }
        }
    }
    let date_ms = days_from_civil(year, month, 1) as f64 * MS_PER_DAY + (day - 1) as f64 * MS_PER_DAY;
    if cursor.at == cursor.bytes.len() {
        return Some(date_ms);
    }
    if matches!(cursor.peek(), Some(b'Z' | b'z')) && cursor.at + 1 == cursor.bytes.len() {
        return Some(date_ms);
    }
    if !matches!(cursor.peek(), Some(b'T' | b't' | b' ')) {
        return None;
    }
    cursor.at += 1;
    let hour = cursor.digits(2)?;
    if !cursor.eat(b':') {
        return None;
    }
    let minute = cursor.digits(2)?;
    let mut second = 0;
    let mut millis = 0;
    if cursor.eat(b':') {
        second = cursor.digits(2)?;
        if cursor.eat(b'.') {
            let start = cursor.at;
            while cursor.peek().is_some_and(|b| b.is_ascii_digit()) {
                cursor.at += 1;
            }
            let fraction = &cursor.bytes[start..cursor.at];
            if fraction.is_empty() {
                return None;
            }
            for (index, scale) in [100, 10, 1].into_iter().enumerate() {
                if let Some(b) = fraction.get(index) {
                    millis += i64::from(b - b'0') * scale;
                }
            }
        }
    }
    if hour > 24 || minute > 59 || second > 59 || (hour == 24 && (minute > 0 || second > 0 || millis > 0)) {
        return None;
    }
    let time_ms = ((hour * 60 + minute) * 60 + second) as f64 * 1000.0 + millis as f64;
    let local_ms = date_ms + time_ms;
    let offset_ms = match cursor.peek() {
        None => return Some(local_to_utc(local_ms, &local())),
        Some(b'Z' | b'z') => {
            cursor.at += 1;
            0.0
        }
        Some(sign @ (b'+' | b'-')) => {
            cursor.at += 1;
            let hours = cursor.digits(2)?;
            cursor.eat(b':');
            let minutes = cursor.digits(2)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let value = ((hours * 60 + minutes) * 60_000) as f64;
            if sign == b'-' {
                -value
            } else {
                value
            }
        }
        _ => return None,
    };
    if cursor.at != cursor.bytes.len() {
        return None;
    }
    Some(local_ms - offset_ms)
}

/// ECMA-262 `UTC(t)`: the earlier instant for repeated local times, the
/// pre-transition offset for skipped ones.
fn local_to_utc(local_ms: f64, zone: &Zone) -> f64 {
    if !local_ms.is_finite() || local_ms.abs() > 8.64e15 + MS_PER_DAY * 2.0 {
        return f64::NAN;
    }
    let local = (local_ms / 1000.0).floor() as i64;
    let before = zone.offset_at(local - 86_400 * 2);
    let after = zone.offset_at(local + 86_400 * 2);
    let mut valid: Vec<i64> = Vec::new();
    for offset in [before, after] {
        if zone.offset_at(local - offset) == offset && !valid.contains(&offset) {
            valid.push(offset);
        }
    }
    let offset = match valid.iter().max() {
        // Earliest instant = largest offset.
        Some(&largest) => largest,
        None => before,
    };
    local_ms - offset as f64 * 1000.0
}
