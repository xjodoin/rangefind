//! Time-zone resolution for the opening-hours evaluator: the subset of
//! `Intl.DateTimeFormat`'s `timeZone` handling the evaluator relies on,
//! without ICU.
//!
//! * Offset zones (`+05:30`, `-0500`, `+05`) and the UTC/GMT spellings are
//!   built in, as are `Etc/GMT±N`, so they work on every target (wasm
//!   included).
//! * Named IANA zones (`America/Toronto`, case-insensitive like ICU) are
//!   read from the system tz database (`$TZDIR` or `/usr/share/zoneinfo`,
//!   TZif v1–v4 including the POSIX TZ footer for instants past the last
//!   transition). Hosts without one can plug a resolver in through
//!   [`super::OpeningHoursOptions::zone_resolver`].
//! * The default zone (`timeZone: undefined`) is `$TZ`, else
//!   `/etc/localtime`, else UTC — what Node reports as its host zone.
//! * On `wasm32-unknown-unknown` there is no tz database: named zones and
//!   the default zone come from the JavaScript host through the
//!   `rangefind.host_zone_offset_minutes` import (`Intl` in the glue), so
//!   they match the JS layer exactly.
//!
//! Parsed zones are cached per thread.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

/// A resolved zone: seconds east of UTC as a function of the instant.
#[derive(Debug)]
pub(crate) enum Zone {
    Fixed(i64),
    Tzif(Tzif),
    /// Resolved by the wasm host (`""` = the host's default zone).
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    Host(String),
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) mod host {
    #[link(wasm_import_module = "rangefind")]
    extern "C" {
        /// UTC offset of `zone` (UTF-8; empty = host default) at `at_ms`, in
        /// minutes east of UTC; NaN when the zone is invalid.
        fn host_zone_offset_minutes(zone_ptr: *const u8, zone_len: usize, at_ms: f64) -> f64;
        /// `Date.now()`.
        fn host_now_ms() -> f64;
    }

    pub(crate) fn offset_seconds(zone: &str, at_ms: f64) -> Option<i64> {
        // SAFETY: the host reads `zone_len` bytes of linear memory at `zone_ptr`.
        let minutes = unsafe { host_zone_offset_minutes(zone.as_ptr(), zone.len(), at_ms) };
        minutes.is_finite().then(|| (minutes * 60.0).round() as i64)
    }

    pub(crate) fn now_ms() -> f64 {
        // SAFETY: plain value import.
        unsafe { host_now_ms() }
    }
}

#[derive(Debug)]
pub(crate) struct Tzif {
    transitions: Vec<i64>,
    transition_types: Vec<u8>,
    offsets: Vec<i64>,
    footer: Option<PosixTz>,
}

#[derive(Debug, Clone)]
struct PosixTz {
    std_offset: i64,
    dst: Option<PosixDst>,
}

#[derive(Debug, Clone)]
struct PosixDst {
    offset: i64,
    start: (PosixDate, i64),
    end: (PosixDate, i64),
}

#[derive(Debug, Clone, Copy)]
enum PosixDate {
    /// `Jn`: 1..=365, February 29 never counted.
    Julian1(i64),
    /// `n`: 0..=365, February 29 counted.
    Julian0(i64),
    /// `Mm.w.d`.
    MonthWeekDay(i64, i64, i64),
}

impl Zone {
    /// Seconds east of UTC at `unix` seconds.
    pub(crate) fn offset_at(&self, unix: i64) -> i64 {
        match self {
            Zone::Fixed(offset) => *offset,
            Zone::Tzif(tzif) => tzif.offset_at(unix),
            #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
            Zone::Host(name) => host::offset_seconds(name, unix as f64 * 1000.0).unwrap_or(0),
        }
    }
}

impl Tzif {
    fn offset_at(&self, unix: i64) -> i64 {
        if self.offsets.is_empty() {
            return self.footer.as_ref().map(|footer| footer.offset_at(unix)).unwrap_or(0);
        }
        match self.transitions.last() {
            None => return self.footer.as_ref().map(|footer| footer.offset_at(unix)).unwrap_or(self.offsets[0]),
            Some(&last) if unix >= last => {
                if let Some(footer) = &self.footer {
                    return footer.offset_at(unix);
                }
            }
            _ => {}
        }
        if unix < self.transitions[0] {
            return self.offsets[0];
        }
        // Last transition at or before `unix`.
        let slot = self.transitions.partition_point(|&at| at <= unix) - 1;
        self.offsets.get(self.transition_types[slot] as usize).copied().unwrap_or(0)
    }
}

pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(crate) fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn month_length(year: i64, month: i64) -> i64 {
    match month {
        2 if is_leap(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

impl PosixDate {
    /// Days since the epoch of this rule date in `year`.
    fn day(self, year: i64) -> i64 {
        let jan1 = days_from_civil(year, 1, 1);
        match self {
            PosixDate::Julian1(n) => jan1 + n - 1 + i64::from(is_leap(year) && n >= 60),
            PosixDate::Julian0(n) => jan1 + n,
            PosixDate::MonthWeekDay(month, week, weekday) => {
                let first = days_from_civil(year, month, 1);
                let first_weekday = (first + 4).rem_euclid(7);
                let mut day = 1 + (weekday - first_weekday).rem_euclid(7) + 7 * (week - 1);
                while day > month_length(year, month) {
                    day -= 7;
                }
                first + day - 1
            }
        }
    }
}

impl PosixTz {
    fn offset_at(&self, unix: i64) -> i64 {
        let Some(dst) = &self.dst else { return self.std_offset };
        let year = civil_from_days((unix + self.std_offset).div_euclid(86_400)).0;
        let start = dst.start.0.day(year) * 86_400 + dst.start.1 - self.std_offset;
        let end = dst.end.0.day(year) * 86_400 + dst.end.1 - dst.offset;
        let in_dst = if start < end { unix >= start && unix < end } else { !(unix >= end && unix < start) };
        if in_dst {
            dst.offset
        } else {
            self.std_offset
        }
    }

    fn parse(text: &str) -> Option<PosixTz> {
        let bytes = text.as_bytes();
        let mut at = 0;
        skip_name(bytes, &mut at)?;
        // POSIX offsets count west of UTC.
        let std_offset = -parse_hms(bytes, &mut at, true)?;
        if at == bytes.len() {
            return Some(PosixTz { std_offset, dst: None });
        }
        skip_name(bytes, &mut at)?;
        let mut dst_offset = std_offset + 3600;
        if at < bytes.len() && bytes[at] != b',' {
            dst_offset = -parse_hms(bytes, &mut at, true)?;
        }
        if at >= bytes.len() || bytes[at] != b',' {
            // No rule: tzcode's default (US rules) is obsolete; treat as
            // standard time.
            return Some(PosixTz { std_offset, dst: None });
        }
        at += 1;
        let start = parse_rule(bytes, &mut at)?;
        if at >= bytes.len() || bytes[at] != b',' {
            return None;
        }
        at += 1;
        let end = parse_rule(bytes, &mut at)?;
        if at != bytes.len() {
            return None;
        }
        Some(PosixTz { std_offset, dst: Some(PosixDst { offset: dst_offset, start, end }) })
    }
}

fn skip_name(bytes: &[u8], at: &mut usize) -> Option<()> {
    if *at < bytes.len() && bytes[*at] == b'<' {
        let close = bytes[*at..].iter().position(|&b| b == b'>')?;
        *at += close + 1;
        return Some(());
    }
    let start = *at;
    while *at < bytes.len() && bytes[*at].is_ascii_alphabetic() {
        *at += 1;
    }
    (*at - start >= 3).then_some(())
}

fn parse_number(bytes: &[u8], at: &mut usize) -> Option<i64> {
    let start = *at;
    while *at < bytes.len() && bytes[*at].is_ascii_digit() {
        *at += 1;
    }
    if *at == start {
        return None;
    }
    std::str::from_utf8(&bytes[start..*at]).ok()?.parse().ok()
}

fn parse_hms(bytes: &[u8], at: &mut usize, signed: bool) -> Option<i64> {
    let mut sign = 1;
    if signed && *at < bytes.len() && (bytes[*at] == b'+' || bytes[*at] == b'-') {
        if bytes[*at] == b'-' {
            sign = -1;
        }
        *at += 1;
    }
    let mut seconds = parse_number(bytes, at)? * 3600;
    for scale in [60, 1] {
        if *at < bytes.len() && bytes[*at] == b':' {
            *at += 1;
            seconds += parse_number(bytes, at)? * scale;
        } else {
            break;
        }
    }
    Some(sign * seconds)
}

fn parse_rule(bytes: &[u8], at: &mut usize) -> Option<(PosixDate, i64)> {
    let date = match bytes.get(*at)? {
        b'J' => {
            *at += 1;
            PosixDate::Julian1(parse_number(bytes, at)?)
        }
        b'M' => {
            *at += 1;
            let month = parse_number(bytes, at)?;
            if bytes.get(*at) != Some(&b'.') {
                return None;
            }
            *at += 1;
            let week = parse_number(bytes, at)?;
            if bytes.get(*at) != Some(&b'.') {
                return None;
            }
            *at += 1;
            let weekday = parse_number(bytes, at)?;
            if !(1..=12).contains(&month) || !(1..=5).contains(&week) || !(0..=6).contains(&weekday) {
                return None;
            }
            PosixDate::MonthWeekDay(month, week, weekday)
        }
        _ => PosixDate::Julian0(parse_number(bytes, at)?),
    };
    let mut time = 7200;
    if bytes.get(*at) == Some(&b'/') {
        *at += 1;
        time = parse_hms(bytes, at, true)?;
    }
    Some((date, time))
}

fn be32(bytes: &[u8], at: usize) -> Option<i64> {
    let slice: [u8; 4] = bytes.get(at..at + 4)?.try_into().ok()?;
    Some(i64::from(i32::from_be_bytes(slice)))
}

fn be64(bytes: &[u8], at: usize) -> Option<i64> {
    let slice: [u8; 8] = bytes.get(at..at + 8)?.try_into().ok()?;
    Some(i64::from_be_bytes(slice))
}

/// Parse a TZif file (RFC 8536).
pub(crate) fn parse_tzif(bytes: &[u8]) -> Option<Tzif> {
    struct Header {
        isutcnt: usize,
        isstdcnt: usize,
        leapcnt: usize,
        timecnt: usize,
        typecnt: usize,
        charcnt: usize,
    }
    fn header(bytes: &[u8], at: usize) -> Option<(u8, Header)> {
        if bytes.get(at..at + 4)? != b"TZif" {
            return None;
        }
        let version = *bytes.get(at + 4)?;
        let count = |index: usize| be32(bytes, at + 20 + index * 4).map(|value| value as usize);
        Some((
            version,
            Header {
                isutcnt: count(0)?,
                isstdcnt: count(1)?,
                leapcnt: count(2)?,
                timecnt: count(3)?,
                typecnt: count(4)?,
                charcnt: count(5)?,
            },
        ))
    }
    let (version, first) = header(bytes, 0)?;
    let v1_len = first.timecnt * 5 + first.typecnt * 6 + first.charcnt + first.leapcnt * 8 + first.isstdcnt + first.isutcnt;
    let (head, data_start, time_size) = if version >= b'2' {
        let (_, second) = header(bytes, 44 + v1_len)?;
        (second, 44 + v1_len + 44, 8)
    } else {
        (first, 44, 4)
    };
    let mut at = data_start;
    let mut transitions = Vec::with_capacity(head.timecnt);
    for _ in 0..head.timecnt {
        transitions.push(if time_size == 8 { be64(bytes, at)? } else { be32(bytes, at)? });
        at += time_size;
    }
    let transition_types = bytes.get(at..at + head.timecnt)?.to_vec();
    at += head.timecnt;
    let mut offsets = Vec::with_capacity(head.typecnt);
    for _ in 0..head.typecnt {
        offsets.push(be32(bytes, at)?);
        at += 6;
    }
    at += head.charcnt + head.leapcnt * (time_size + 4) + head.isstdcnt + head.isutcnt;
    let footer = if version >= b'2' && bytes.get(at) == Some(&b'\n') {
        let rest = &bytes[at + 1..];
        rest.iter()
            .position(|&b| b == b'\n')
            .and_then(|end| std::str::from_utf8(&rest[..end]).ok())
            .filter(|text| !text.is_empty())
            .and_then(PosixTz::parse)
    } else {
        None
    };
    if transition_types.iter().any(|&index| index as usize >= offsets.len()) {
        return None;
    }
    Some(Tzif { transitions, transition_types, offsets, footer })
}

/// `±HH`, `±HHMM`, `±HH:MM` (what V8 accepts as an offset time zone).
fn parse_offset_zone(name: &str) -> Option<i64> {
    let bytes = name.as_bytes();
    let sign = match bytes.first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let digits = |slice: &[u8]| -> Option<i64> {
        (slice.len() == 2 && slice.iter().all(u8::is_ascii_digit))
            .then(|| i64::from(slice[0] - b'0') * 10 + i64::from(slice[1] - b'0'))
    };
    let (hours, minutes) = match bytes.len() {
        3 => (digits(&bytes[1..3])?, 0),
        5 => (digits(&bytes[1..3])?, digits(&bytes[3..5])?),
        6 if bytes[3] == b':' => (digits(&bytes[1..3])?, digits(&bytes[4..6])?),
        _ => return None,
    };
    (hours <= 23 && minutes <= 59).then_some(sign * (hours * 3600 + minutes * 60))
}

const UTC_NAMES: &[&str] = &[
    "utc",
    "etc/utc",
    "gmt",
    "etc/gmt",
    "uct",
    "etc/uct",
    "universal",
    "etc/universal",
    "zulu",
    "etc/zulu",
    "greenwich",
    "etc/greenwich",
    "gmt0",
    "etc/gmt0",
    "gmt+0",
    "gmt-0",
    "etc/gmt+0",
    "etc/gmt-0",
];

fn builtin_zone(name: &str) -> Option<Zone> {
    if let Some(offset) = parse_offset_zone(name) {
        return Some(Zone::Fixed(offset));
    }
    let lower = name.to_ascii_lowercase();
    if UTC_NAMES.contains(&lower.as_str()) {
        return Some(Zone::Fixed(0));
    }
    let rest = lower.strip_prefix("etc/gmt")?;
    let (sign, digits) = (rest.as_bytes().first().copied()?, &rest[1..]);
    if digits.is_empty() || digits.len() > 2 || digits.starts_with('0') || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let hours: i64 = digits.parse().ok()?;
    match sign {
        // POSIX-style: Etc/GMT+5 is five hours *behind* UTC.
        b'+' if hours <= 12 => Some(Zone::Fixed(-hours * 3600)),
        b'-' if hours <= 14 => Some(Zone::Fixed(hours * 3600)),
        _ => None,
    }
}

fn zoneinfo_root() -> PathBuf {
    std::env::var_os("TZDIR").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/usr/share/zoneinfo"))
}

/// Names ICU would never accept even though a file of that name exists.
fn plausible_zone_name(name: &str) -> bool {
    if name.is_empty()
        || name.starts_with('/')
        || !name.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'+' | b'-' | b'/'))
        || name.split('/').any(|part| part.is_empty())
    {
        return false;
    }
    let lower = name.to_ascii_lowercase();
    let first = lower.split('/').next().unwrap_or("");
    !matches!(first, "posix" | "right" | "posixrules" | "localtime" | "factory" | "systemv")
}

fn read_zone_file(path: &Path) -> Option<Zone> {
    let bytes = std::fs::read(path).ok()?;
    parse_tzif(&bytes).map(Zone::Tzif)
}

fn walk_zone_names(root: &Path, dir: &Path, depth: usize, out: &mut HashMap<String, PathBuf>) {
    if depth > 3 {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else { return };
    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(relative) = path.strip_prefix(root) else { continue };
        let name = relative.to_string_lossy().replace('\\', "/");
        if path.is_dir() {
            walk_zone_names(root, &path, depth + 1, out);
        } else {
            out.entry(name.to_ascii_lowercase()).or_insert(path);
        }
    }
}

fn named_zone(name: &str) -> Option<Zone> {
    if !plausible_zone_name(name) {
        return None;
    }
    let root = zoneinfo_root();
    if let Some(zone) = read_zone_file(&root.join(name)) {
        return Some(zone);
    }
    // ICU matches zone ids case-insensitively; case-sensitive file systems
    // need a lowercase index.
    thread_local! {
        static NAMES: RefCell<Option<HashMap<String, PathBuf>>> = const { RefCell::new(None) };
    }
    let path = NAMES.with(|names| {
        let mut names = names.borrow_mut();
        let index = names.get_or_insert_with(|| {
            let mut out = HashMap::new();
            walk_zone_names(&root, &root, 0, &mut out);
            out
        });
        index.get(&name.to_ascii_lowercase()).cloned()
    })?;
    read_zone_file(&path)
}

thread_local! {
    static ZONES: RefCell<HashMap<String, Option<Rc<Zone>>>> = RefCell::new(HashMap::new());
    static DEFAULT_ZONE: RefCell<Option<Rc<Zone>>> = const { RefCell::new(None) };
}

/// Resolve an explicit `timeZone` option; `None` means ICU would throw
/// "Invalid time zone specified".
pub(crate) fn resolve_zone(name: &str) -> Option<Rc<Zone>> {
    if let Some(cached) = ZONES.with(|zones| zones.borrow().get(name).cloned()) {
        return cached;
    }
    let zone = builtin_zone(name).or_else(|| named_zone(name)).map(Rc::new);
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let zone = zone.or_else(|| {
        // Validate once: an invalid name is NaN at any instant.
        host::offset_seconds(name, 0.0).map(|_| Rc::new(Zone::Host(name.to_string())))
    });
    ZONES.with(|zones| zones.borrow_mut().insert(name.to_string(), zone.clone()));
    zone
}

/// The host default zone (`timeZone: undefined`).
pub(crate) fn default_zone() -> Rc<Zone> {
    if let Some(zone) = DEFAULT_ZONE.with(|zone| zone.borrow().clone()) {
        return zone;
    }
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    {
        let zone = Rc::new(Zone::Host(String::new()));
        DEFAULT_ZONE.with(|slot| *slot.borrow_mut() = Some(zone.clone()));
        return zone;
    }
    #[allow(unreachable_code)]
    let from_env = std::env::var("TZ").ok().filter(|value| !value.is_empty()).and_then(|value| {
        let value = value.strip_prefix(':').unwrap_or(&value).to_string();
        if value.starts_with('/') {
            read_zone_file(Path::new(&value)).map(Rc::new)
        } else {
            resolve_zone(&value)
        }
    });
    let zone = from_env
        .or_else(|| read_zone_file(Path::new("/etc/localtime")).map(Rc::new))
        .unwrap_or_else(|| Rc::new(Zone::Fixed(0)));
    DEFAULT_ZONE.with(|slot| *slot.borrow_mut() = Some(zone.clone()));
    zone
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_round_trip() {
        for days in [-719_468i64, -1, 0, 1, 19_000, 2_932_896] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
    }

    #[test]
    fn posix_rules_follow_dst() {
        let tz = PosixTz::parse("EST5EDT,M3.2.0,M11.1.0").unwrap();
        // 2026-07-01T12:00Z is EDT, 2026-01-15T12:00Z is EST.
        assert_eq!(tz.offset_at(1_782_907_200), -4 * 3600);
        assert_eq!(tz.offset_at(1_768_478_400), -5 * 3600);
        assert_eq!(parse_offset_zone("+05:30"), Some(19_800));
        assert_eq!(parse_offset_zone("-0500"), Some(-18_000));
        assert_eq!(parse_offset_zone("+24:00"), None);
    }
}
