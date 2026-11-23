//! Port of `src/integrations/osm/opening_hours.js`: a conservative
//! evaluator for the common OSM `opening_hours` grammar. Unsupported
//! calendar/holiday constructs evaluate to "unknown" rather than claiming a
//! business is open; later rules override earlier ones.
//!
//! The JS resolves the local weekday/time through `Intl.DateTimeFormat`;
//! here [`zoned_parts`] does the same through [`tz`] (built-in offset/UTC
//! zones plus the system tz database), and a host without a tz database can
//! supply [`OpeningHoursOptions::zone_resolver`]. Every zone ever resolved
//! is cached, where the JS builds a new formatter per evaluation.

mod date;
mod tz;

use std::rc::Rc;

use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::osm::constraints::js::{is_js_space, is_word_iu, js_string, js_string_or_empty, js_trim, match_iu};

/// The local calendar view of an instant (`zonedParts` in JS).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZonedParts {
    /// 0 = Sunday … 6 = Saturday.
    pub weekday: u8,
    /// Minutes since local midnight (0..1440).
    pub minutes: u32,
    /// `YYYY-MM-DD` as `Intl` formats it (era year, so 1 BC prints "1").
    pub date: String,
}

/// Custom zone resolution: `(epoch_ms, time_zone)` → parts, or `None` for an
/// invalid zone. `time_zone` is `None` for the host default.
pub type ZoneResolver = Rc<dyn Fn(f64, Option<&str>) -> Option<ZonedParts>>;

/// `evaluateOpeningHours` options.
#[derive(Clone, Default)]
pub struct OpeningHoursOptions {
    /// `options.at`: epoch ms (number), a date string, or `null` for "now".
    pub at: Value,
    /// `options.timeZone`: `null` means the host default zone; any other
    /// non-string is stringified like `Intl` does (and is then invalid).
    pub time_zone: Value,
    /// `options.requireComplete`: only an explicit `false` disables it.
    pub require_complete: Value,
    /// Pins `Date.now()` (epoch ms) for `at: null`.
    pub now_ms: Option<f64>,
    /// Overrides the host default zone (an IANA name or offset).
    pub default_time_zone: Option<String>,
    /// Replaces the built-in zone resolution.
    pub zone_resolver: Option<ZoneResolver>,
}

/// The evaluation object (`{state, isOpen, reason, rule?, raw?, localDate?}`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpeningHoursEvaluation {
    /// "open", "closed" or "unknown".
    pub state: &'static str,
    pub is_open: Option<bool>,
    /// "missing", "24/7", "invalid-time-zone", "unsupported",
    /// "outside-schedule", "unsupported-rule", "no-applicable-rule" or
    /// "evaluated".
    pub reason: &'static str,
    pub rule: Option<String>,
    pub raw: Option<String>,
    pub local_date: Option<String>,
}

impl OpeningHoursEvaluation {
    pub fn to_json(&self) -> Value {
        let mut out = Map::new();
        out.insert("state".into(), Value::String(self.state.into()));
        out.insert("isOpen".into(), self.is_open.map(Value::Bool).unwrap_or(Value::Null));
        out.insert("reason".into(), Value::String(self.reason.into()));
        if let Some(rule) = &self.rule {
            out.insert("rule".into(), Value::String(rule.clone()));
        }
        if let Some(raw) = &self.raw {
            out.insert("raw".into(), Value::String(raw.clone()));
        }
        if let Some(date) = &self.local_date {
            out.insert("localDate".into(), Value::String(date.clone()));
        }
        Value::Object(out)
    }
}

const DAYS: [&str; 7] = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];

fn day_index(token: &str) -> Option<u8> {
    DAYS.iter().position(|day| *day == token).map(|index| index as u8)
}

fn format_parts(local_ms: i64) -> ZonedParts {
    let days = local_ms.div_euclid(86_400_000);
    let minutes = (local_ms.rem_euclid(86_400_000) / 60_000) as u32;
    let (year, month, day) = tz::civil_from_days(days);
    let era_year = if year <= 0 { 1 - year } else { year };
    ZonedParts {
        weekday: (days + 4).rem_euclid(7) as u8,
        minutes,
        date: format!("{era_year}-{month:02}-{day:02}"),
    }
}

/// `zonedParts(at, timeZone)` with the built-in zone resolution. `None`
/// when the zone is invalid (where `Intl` throws a RangeError). `at_ms` is a
/// valid time value (integral, within ±8.64e15).
pub fn zoned_parts(at_ms: f64, time_zone: Option<&str>, default_time_zone: Option<&str>) -> Option<ZonedParts> {
    let zone = match time_zone.or(default_time_zone) {
        Some(name) => tz::resolve_zone(name)?,
        None => tz::default_zone(),
    };
    let at = at_ms as i64;
    let offset = zone.offset_at(at.div_euclid(1000));
    Some(format_parts(at + offset * 1000))
}

/// `new Date(value).getTime()` (NaN when invalid) for a non-null JSON
/// value: epoch ms, booleans, and ECMAScript date-time strings; local
/// date-times resolve in `default_time_zone` (else the host zone).
pub fn js_date_ms(value: &Value, default_time_zone: Option<&str>) -> f64 {
    let local_zone = || match default_time_zone.and_then(tz::resolve_zone) {
        Some(zone) => zone,
        None => tz::default_zone(),
    };
    date::date_value(value, &local_zone)
}

/// One parsed `;`-separated rule.
#[derive(Clone, Debug)]
enum Rule {
    Unsupported,
    Closed { days: u8 },
    Open { days: u8, intervals: Vec<(u32, u32)> },
}

/// `/\b(?:words)\b/iu.test(text)`.
fn contains_word_iu(text: &str, words: &[&str]) -> bool {
    let mut previous: Option<char> = None;
    for (at, c) in text.char_indices() {
        if !previous.is_some_and(is_word_iu) {
            for word in words {
                if let Some(end) = match_iu(text, at, word) {
                    let next = text[end..].chars().next();
                    // The word itself is all word chars, so `\b` before it
                    // needs a non-word predecessor and one after it a
                    // non-word successor.
                    if !next.is_some_and(is_word_iu) {
                        return true;
                    }
                }
            }
        }
        previous = Some(c);
    }
    false
}

fn has_four_digits(text: &str) -> bool {
    let mut run = 0;
    for b in text.bytes() {
        if b.is_ascii_digit() {
            run += 1;
            if run == 4 {
                return true;
            }
        } else {
            run = 0;
        }
    }
    false
}

const UNSUPPORTED_WORDS: &[&str] = &["ph", "sh", "week", "sunrise", "sunset", "dawn", "dusk", "easter"];
const MONTHS: &[&str] = &["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];

/// `^(Mo|..)(?:-(Mo|..))?(?:,(Mo|..)(?:-(Mo|..))?)*\s+` → the day set and
/// the matched length.
fn match_day_prefix(clean: &str) -> Option<(u8, usize)> {
    let bytes = clean.as_bytes();
    let day_at = |at: usize| -> Option<u8> { bytes.get(at..at + 2).and_then(|slice| std::str::from_utf8(slice).ok()).and_then(day_index) };
    let mut at = 0;
    let mut days = 0u8;
    loop {
        let start = day_at(at)?;
        at += 2;
        let mut end = start;
        if bytes.get(at) == Some(&b'-') {
            if let Some(day) = day_at(at + 1) {
                end = day;
                at += 3;
            }
        }
        // `daySet`: walk start..=end, wrapping through Sunday.
        let mut current = start;
        for _ in 0..7 {
            days |= 1 << current;
            if current == end {
                break;
            }
            current = (current + 1) % 7;
        }
        if bytes.get(at) == Some(&b',') && day_at(at + 1).is_some() {
            at += 1;
            continue;
        }
        break;
    }
    // `\s+` (the greedy day list cannot give anything back usefully: any
    // shorter match is followed by `-` or `,`, never whitespace).
    let rest = &clean[at..];
    let spaces: usize = rest.chars().take_while(|c| is_js_space(*c)).map(char::len_utf8).sum();
    (spaces > 0).then_some((days, at + spaces))
}

/// `/^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})(?:\+)?$/u` → minutes or `None`
/// (no match, or a clock value outside 0..=1440).
fn parse_interval(item: &str) -> Option<Option<(u32, u32)>> {
    let bytes = item.as_bytes();
    let mut at = 0;
    let clock = |at: &mut usize| -> Option<u32> {
        let start = *at;
        while *at < bytes.len() && bytes[*at].is_ascii_digit() && *at - start < 2 {
            *at += 1;
        }
        if *at == start {
            return None;
        }
        let hour: u32 = item[start..*at].parse().ok()?;
        if bytes.get(*at) != Some(&b':') {
            return None;
        }
        *at += 1;
        let minute_bytes = bytes.get(*at..*at + 2)?;
        if !minute_bytes.iter().all(u8::is_ascii_digit) {
            return None;
        }
        *at += 2;
        Some(hour * 60 + (u32::from(minute_bytes[0] - b'0') * 10 + u32::from(minute_bytes[1] - b'0')))
    };
    let start = clock(&mut at)?;
    if bytes.get(at) != Some(&b'-') {
        return None;
    }
    at += 1;
    let end = clock(&mut at)?;
    if bytes.get(at) == Some(&b'+') {
        at += 1;
    }
    if at != bytes.len() {
        return None;
    }
    // `clockMinutes`: 0..=1440, else the rule is unsupported.
    Some((start <= 1440 && end <= 1440).then_some((start, end)))
}

fn strip_quoted(surface: &str) -> String {
    // `surface.replace(/"[^"]*"/gu, " ")`
    let mut out = String::with_capacity(surface.len());
    let mut rest = surface;
    while let Some(open) = rest.find('"') {
        let after = &rest[open + 1..];
        match after.find('"') {
            Some(close) => {
                out.push_str(&rest[..open]);
                out.push(' ');
                rest = &after[close + 1..];
            }
            None => break,
        }
    }
    out.push_str(rest);
    out
}

fn eq_iu(text: &str, word: &str) -> bool {
    match_iu(text, 0, word) == Some(text.len())
}

/// `parseRule(surface)` → `None` for an empty rule.
fn parse_rule(surface: &str) -> Option<(Rule, String)> {
    let clean = js_trim(&strip_quoted(surface)).to_string();
    if clean.is_empty() {
        return None;
    }
    if contains_word_iu(&clean, UNSUPPORTED_WORDS) || has_four_digits(&clean) || contains_word_iu(&clean, MONTHS) {
        return Some((Rule::Unsupported, clean));
    }
    let (days, body) = match match_day_prefix(&clean) {
        Some((days, length)) => (days, js_trim(&clean[length..]).to_string()),
        None => (0x7f, clean.clone()),
    };
    if eq_iu(&body, "off") || eq_iu(&body, "closed") {
        return Some((Rule::Closed { days }, clean));
    }
    if eq_iu(&body, "open") || body == "24/7" {
        return Some((Rule::Open { days, intervals: vec![(0, 1440)] }, clean));
    }
    let mut intervals = Vec::new();
    for item in body.split(',') {
        match parse_interval(js_trim(item)) {
            Some(Some(interval)) => intervals.push(interval),
            _ => return Some((Rule::Unsupported, clean)),
        }
    }
    Some((Rule::Open { days, intervals }, clean))
}

fn has_day(days: u8, weekday: u8) -> bool {
    days & (1 << weekday) != 0
}

fn rule_state(rule: &Rule, weekday: u8, minutes: u32) -> Option<bool> {
    match rule {
        Rule::Unsupported => None,
        Rule::Closed { days } => has_day(*days, weekday).then_some(false),
        Rule::Open { days, intervals } => {
            for &(start, end) in intervals {
                if end > start && has_day(*days, weekday) && minutes >= start && minutes < end {
                    return Some(true);
                }
                if end <= start {
                    if has_day(*days, weekday) && minutes >= start {
                        return Some(true);
                    }
                    if has_day(*days, (weekday + 6) % 7) && minutes < end {
                        return Some(true);
                    }
                }
            }
            has_day(*days, weekday).then_some(false)
        }
    }
}

/// `Date.now()`. `wasm32-unknown-unknown` has no clock (`SystemTime::now`
/// panics there), so the JavaScript host supplies it through the
/// `rangefind.host_now_ms` import.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn now_ms() -> f64 {
    tz::host::now_ms()
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as f64)
        .unwrap_or(0.0)
}

fn evaluation(
    state: &'static str,
    is_open: Option<bool>,
    reason: &'static str,
    rule: Option<String>,
    raw: Option<&str>,
    local_date: Option<&str>,
) -> OpeningHoursEvaluation {
    OpeningHoursEvaluation {
        state,
        is_open,
        reason,
        rule,
        raw: raw.map(str::to_string),
        local_date: local_date.map(str::to_string),
    }
}

/// `evaluateOpeningHours(value, options)`. `value` is the raw
/// `details.opening_hours` (any JSON; stringified like `String(value || "")`).
/// Errs with the JS RangeError text when `options.at` is not a valid date.
pub fn evaluate_opening_hours(value: &Value, options: &OpeningHoursOptions) -> Result<OpeningHoursEvaluation> {
    let raw_string = js_string_or_empty(Some(value));
    let raw = js_trim(&raw_string);
    if raw.is_empty() {
        return Ok(evaluation("unknown", None, "missing", None, None, None));
    }
    if raw == "24/7" {
        return Ok(evaluation("open", Some(true), "24/7", None, Some(raw), None));
    }
    let at = if options.at.is_null() {
        date::time_clip(options.now_ms.unwrap_or_else(now_ms))
    } else {
        js_date_ms(&options.at, options.default_time_zone.as_deref())
    };
    if at.is_nan() {
        return Err(Error::new("Opening-hours evaluation requires a valid date."));
    }
    let time_zone = if options.time_zone.is_null() { None } else { Some(js_string(&options.time_zone)) };
    let local = match &options.zone_resolver {
        Some(resolver) => resolver(at, time_zone.as_deref()),
        None => zoned_parts(at, time_zone.as_deref(), options.default_time_zone.as_deref()),
    };
    let Some(local) = local else {
        return Ok(evaluation("unknown", None, "invalid-time-zone", None, Some(raw), None));
    };
    let rules: Vec<(Rule, String)> = raw.split(';').filter_map(parse_rule).collect();
    if rules.is_empty() {
        return Ok(evaluation("unknown", None, "unsupported", None, Some(raw), None));
    }
    let mut state: Option<bool> = None;
    let mut matched: Option<&str> = None;
    let mut unsupported = false;
    for (rule, clean) in &rules {
        if matches!(rule, Rule::Unsupported) {
            unsupported = true;
            continue;
        }
        if let Some(next) = rule_state(rule, local.weekday, local.minutes) {
            state = Some(next);
            matched = Some(clean);
        }
    }
    let date = Some(local.date.as_str());
    let Some(state) = state else {
        if !unsupported {
            return Ok(evaluation("closed", Some(false), "outside-schedule", None, Some(raw), date));
        }
        return Ok(evaluation("unknown", None, "unsupported-rule", None, Some(raw), date));
    };
    if unsupported && options.require_complete != Value::Bool(false) {
        return Ok(evaluation("unknown", None, "unsupported-rule", None, Some(raw), date));
    }
    Ok(evaluation(
        if state { "open" } else { "closed" },
        Some(state),
        "evaluated",
        matched.map(str::to_string),
        Some(raw),
        date,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(iso: &str) -> OpeningHoursOptions {
        OpeningHoursOptions { at: json!(iso), time_zone: json!("UTC"), ..Default::default() }
    }

    #[test]
    fn evaluates_like_the_js_tests() {
        let monday = at("2026-08-03T15:00:00Z");
        assert_eq!(evaluate_opening_hours(&json!("Mo-Fr 09:00-17:00"), &monday).unwrap().state, "open");
        assert_eq!(evaluate_opening_hours(&json!("Mo-Fr 09:00-14:00"), &monday).unwrap().state, "closed");
        let night = at("2026-08-04T01:00:00Z");
        assert_eq!(evaluate_opening_hours(&json!("Mo-Fr 22:00-02:00"), &night).unwrap().state, "open");
        assert_eq!(evaluate_opening_hours(&json!("24/7"), &monday).unwrap().state, "open");
        assert_eq!(evaluate_opening_hours(&json!("Mo-Fr 09:00-17:00; PH off"), &monday).unwrap().state, "unknown");
    }
}
