//! Filter plans (`makeDocFilterPlan`, `makeBlockFilterPlan`), the value
//! store abstraction (`valueStoreForFilterPlan`, `valueForDoc`) and the
//! summary predicates used to prune blocks, chunks and sorted pages.

use std::collections::HashSet;
use std::rc::Rc;

use serde_json::Value;

use crate::codec::doc_values::{Codes, DocValue, DocValueChunkMeta};
use crate::codec::filter_bitmap::FilterBitmap;
use crate::codec::postings::{BlockFilters, FilterSummary};
use crate::error::{Error, Result};
use crate::util::json::{to_number, NULL};

use super::geo_plan::GeoPlan;
use super::index::IndexEngine;
use super::options::FILTER_BITMAP_SPARSE_DOC_LIMIT;

/// The caller's `filters` object, order-preserving.
#[derive(Clone, Default)]
pub struct Filters {
    pub facets: Vec<(String, Vec<Value>)>,
    pub numbers: Vec<(String, Value)>,
    pub booleans: Vec<(String, Value)>,
    pub geo: Option<Rc<GeoPlan>>,
    /// The TypeError `new Set(values)` throws for the first facet value
    /// list that is not iterable (a number, boolean or object), raised
    /// where the JS builds a filter plan.
    pub facet_error: Option<String>,
}

impl Filters {
    pub fn from_json(value: &Value) -> Filters {
        let entries = |key: &str| -> Vec<(String, Value)> {
            value.get(key).and_then(Value::as_object).map(|map| map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()).unwrap_or_default()
        };
        let mut facet_error = None;
        let facets = entries("facets")
            .into_iter()
            .map(|(field, values)| {
                let list = match values {
                    Value::Array(items) => items,
                    Value::String(text) => text.chars().map(|c| Value::String(c.to_string())).collect(),
                    Value::Null => Vec::new(),
                    other => {
                        facet_error.get_or_insert_with(|| not_iterable_message(&other));
                        Vec::new()
                    }
                };
                (field, list)
            })
            .collect();
        Filters { facets, numbers: entries("numbers"), booleans: entries("booleans"), geo: None, facet_error }
    }

    /// Raise the facet TypeError, if any (`makeDocFilterPlan` /
    /// `makeBlockFilterPlan` evaluate `new Set(values)` first).
    pub fn check_facets(&self) -> Result<()> {
        match &self.facet_error {
            Some(message) => Err(Error::new(message.clone())),
            None => Ok(()),
        }
    }

    /// `Object.keys(...).length` over the three maps.
    pub fn has_filters(&self) -> bool {
        !self.facets.is_empty() || !self.numbers.is_empty() || !self.booleans.is_empty()
    }

    /// The JS `hasFilters` value itself (`a.length || b.length || c.length`),
    /// which some responses leak as a number (e.g. `approximate: 0`).
    pub fn has_filters_js(&self) -> usize {
        if !self.facets.is_empty() {
            self.facets.len()
        } else if !self.numbers.is_empty() {
            self.numbers.len()
        } else {
            self.booleans.len()
        }
    }

    pub fn facet_fields(&self) -> Vec<String> {
        self.facets.iter().map(|(field, _)| field.clone()).collect()
    }

    pub fn without_geo(&self) -> Filters {
        Filters { geo: None, ..self.clone() }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct NumberRange {
    pub min: Option<f64>,
    pub max: Option<f64>,
}

#[derive(Clone, Default)]
pub struct DocFilterPlan {
    pub facets: Vec<(String, Rc<HashSet<u32>>)>,
    pub numbers: Vec<(String, NumberRange)>,
    pub booleans: Vec<(String, bool)>,
    pub geo: Option<Rc<GeoPlan>>,
    pub active: bool,
}

#[derive(Clone, Default)]
pub struct BlockFilterPlan {
    pub facets: Vec<(String, Rc<HashSet<u32>>)>,
    pub numbers: Vec<(String, NumberRange)>,
    pub booleans: Vec<(String, u64)>,
    pub unknown_fields: Vec<String>,
    pub active: bool,
}

/// Loaded filter bitmaps covering some facet/boolean clauses.
pub struct BitmapStore {
    pub facets: Vec<(String, Vec<Rc<FilterBitmap>>)>,
    pub booleans: Vec<(String, Rc<FilterBitmap>)>,
    pub covered: HashSet<String>,
}

/// `codeData` in the JS runtime: where per-doc values come from.
#[derive(Clone, Default)]
pub struct ValueStore {
    pub doc_values: bool,
    pub codes: Option<Rc<Codes>>,
    pub bitmaps: Option<Rc<BitmapStore>>,
    pub geo_doc_set: bool,
}

/// V8's TypeError text for `new Set(value)` on a non-iterable value.
fn not_iterable_message(value: &Value) -> String {
    let described = match value {
        Value::Bool(flag) => format!("boolean {flag}"),
        Value::Number(number) => {
            let mut text = crate::util::json::js_number_to_string(number.as_f64().unwrap_or(0.0));
            // `String(1e21)` is "1e+21".
            if let Some(at) = text.find('e') {
                if !text[at + 1..].starts_with('-') && !text[at + 1..].starts_with('+') {
                    text.insert(at + 1, '+');
                }
            }
            format!("number {text}")
        }
        _ => "object".to_string(),
    };
    format!("{described} is not iterable (cannot read property Symbol(Symbol.iterator))")
}

/// `booleanCode`: 2 = true, 1 = false.
pub fn boolean_code(value: &Value) -> Option<u64> {
    match value {
        Value::Bool(true) => Some(2),
        Value::Bool(false) => Some(1),
        Value::Number(n) if n.as_f64() == Some(1.0) => Some(2),
        Value::Number(n) if n.as_f64() == Some(0.0) => Some(1),
        Value::String(text) if text == "true" || text == "1" => Some(2),
        Value::String(text) if text == "false" || text == "0" => Some(1),
        _ => None,
    }
}

/// `Date.parse(text)`: a port of V8's `DateParser` (ES date-time strings
/// plus the legacy fallback: month names, `/` and `.` separators, RFC 2822,
/// AM/PM, `GMT+hhmm` offsets, parenthesized comments), `MakeDay`/`MakeTime`
/// and `TimeClip`. `None` is NaN. Strings without a zone that V8 reads as
/// local time (date-times, legacy dates) are read as UTC here: the JS is
/// specified, and its parity fixtures are recorded, with `TZ=UTC`.
pub fn parse_date(text: &str) -> Option<f64> {
    date_parse::parse(text)
}

mod date_parse {
    const NONE: i64 = i32::MAX as i64;
    const MAX_SIGNIFICANT_DIGITS: usize = 9;
    const MAX_TIME_MS: f64 = 8.64e15;

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Keyword {
        Invalid,
        MonthName,
        TimeZoneName,
        TimeSeparator,
        AmPm,
    }

    #[derive(Clone, Copy, PartialEq, Debug)]
    enum Token {
        Invalid,
        Unknown,
        WhiteSpace,
        Number { value: i64, length: usize },
        Symbol(u16),
        EndOfInput,
        Keyword { kind: Keyword, value: i64, length: usize },
    }

    impl Token {
        fn is_fixed_number(&self, length: usize) -> bool {
            matches!(self, Token::Number { length: l, .. } if *l == length)
        }
        fn number(&self) -> i64 {
            match self {
                Token::Number { value, .. } => *value,
                _ => 0,
            }
        }
        fn is_symbol(&self, symbol: u8) -> bool {
            *self == Token::Symbol(symbol as u16)
        }
        fn is_sign(&self) -> bool {
            self.is_symbol(b'+') || self.is_symbol(b'-')
        }
        fn sign(&self) -> i64 {
            if self.is_symbol(b'-') {
                -1
            } else {
                1
            }
        }
        fn is_keyword_z(&self) -> bool {
            matches!(self, Token::Keyword { kind: Keyword::TimeZoneName, value: 0, length: 1 })
        }
        fn is_keyword(&self, kind: Keyword) -> bool {
            matches!(self, Token::Keyword { kind: k, .. } if *k == kind)
        }
    }

    const KEYWORDS: &[([u8; 3], Keyword, i64)] = &[
        (*b"jan", Keyword::MonthName, 1),
        (*b"feb", Keyword::MonthName, 2),
        (*b"mar", Keyword::MonthName, 3),
        (*b"apr", Keyword::MonthName, 4),
        (*b"may", Keyword::MonthName, 5),
        (*b"jun", Keyword::MonthName, 6),
        (*b"jul", Keyword::MonthName, 7),
        (*b"aug", Keyword::MonthName, 8),
        (*b"sep", Keyword::MonthName, 9),
        (*b"oct", Keyword::MonthName, 10),
        (*b"nov", Keyword::MonthName, 11),
        (*b"dec", Keyword::MonthName, 12),
        (*b"am\0", Keyword::AmPm, 0),
        (*b"pm\0", Keyword::AmPm, 12),
        (*b"ut\0", Keyword::TimeZoneName, 0),
        (*b"utc", Keyword::TimeZoneName, 0),
        (*b"z\0\0", Keyword::TimeZoneName, 0),
        (*b"gmt", Keyword::TimeZoneName, 0),
        (*b"cdt", Keyword::TimeZoneName, -5),
        (*b"cst", Keyword::TimeZoneName, -6),
        (*b"edt", Keyword::TimeZoneName, -4),
        (*b"est", Keyword::TimeZoneName, -5),
        (*b"mdt", Keyword::TimeZoneName, -6),
        (*b"mst", Keyword::TimeZoneName, -7),
        (*b"pdt", Keyword::TimeZoneName, -7),
        (*b"pst", Keyword::TimeZoneName, -8),
        (*b"t\0\0", Keyword::TimeSeparator, 0),
    ];

    fn lookup(prefix: [u32; 3], length: usize) -> (Keyword, i64) {
        for (word, kind, value) in KEYWORDS {
            let matched = (0..3).all(|i| prefix[i] == word[i] as u32);
            // A word longer than the keyword only matches a month name.
            if matched && (length <= 3 || *kind == Keyword::MonthName) {
                return (*kind, *value);
            }
        }
        (Keyword::Invalid, 0)
    }

    fn is_white_space(c: u32) -> bool {
        matches!(c, 0x09 | 0x0B | 0x0C | 0x20 | 0xA0 | 0xFEFF | 0x1680 | 0x2000..=0x200A | 0x202F | 0x205F | 0x3000)
    }

    fn is_line_terminator(c: u32) -> bool {
        matches!(c, 0x0A | 0x0D | 0x2028 | 0x2029)
    }

    /// `InputReader` + `DateStringTokenizer` over UTF-16 code units; a NUL
    /// code unit ends the input like V8's reader.
    struct Scanner {
        units: Vec<u16>,
        index: usize,
        ch: u32,
        next: Token,
    }

    impl Scanner {
        fn new(text: &str) -> Self {
            let mut scanner = Scanner { units: text.encode_utf16().collect(), index: 0, ch: 0, next: Token::EndOfInput };
            scanner.advance();
            scanner.next = scanner.scan();
            scanner
        }

        fn advance(&mut self) {
            self.ch = self.units.get(self.index).copied().unwrap_or(0) as u32;
            self.index += 1;
        }

        fn position(&self) -> usize {
            self.index
        }

        fn is_digit(&self) -> bool {
            (b'0' as u32..=b'9' as u32).contains(&self.ch)
        }

        fn skip(&mut self, c: u8) -> bool {
            if self.ch == c as u32 {
                self.advance();
                true
            } else {
                false
            }
        }

        fn read_unsigned_numeral(&mut self) -> i64 {
            let mut n = 0i64;
            let mut digits = 0usize;
            while self.ch == b'0' as u32 {
                self.advance();
            }
            while self.is_digit() {
                if digits < MAX_SIGNIFICANT_DIGITS {
                    n = n * 10 + (self.ch - b'0' as u32) as i64;
                }
                digits += 1;
                self.advance();
            }
            n
        }

        fn is_word_char(&self) -> bool {
            self.ch >= b'A' as u32 && !is_white_space(self.ch)
        }

        fn scan(&mut self) -> Token {
            let start = self.position();
            if self.ch == 0 {
                return Token::EndOfInput;
            }
            if self.is_digit() {
                let value = self.read_unsigned_numeral();
                return Token::Number { value, length: self.position() - start };
            }
            for symbol in [b':', b'-', b'+', b'.', b')'] {
                if self.skip(symbol) {
                    return Token::Symbol(symbol as u16);
                }
            }
            if self.is_word_char() {
                let mut prefix = [0u32; 3];
                let mut length = 0usize;
                while self.is_word_char() {
                    if length < 3 {
                        prefix[length] = self.ch | 0x20;
                    }
                    length += 1;
                    self.advance();
                }
                let (kind, value) = lookup(prefix, length);
                return Token::Keyword { kind, value, length };
            }
            if is_white_space(self.ch) || is_line_terminator(self.ch) {
                self.advance();
                return Token::WhiteSpace;
            }
            if self.ch == b'(' as u32 {
                let mut balance = 0i64;
                loop {
                    if self.ch == b')' as u32 {
                        balance -= 1;
                    } else if self.ch == b'(' as u32 {
                        balance += 1;
                    }
                    self.advance();
                    if !(balance > 0 && self.ch != 0) {
                        break;
                    }
                }
                return Token::Unknown;
            }
            self.advance();
            Token::Unknown
        }

        fn next_token(&mut self) -> Token {
            let token = self.next;
            self.next = self.scan();
            token
        }

        fn peek(&self) -> Token {
            self.next
        }

        fn skip_symbol(&mut self, symbol: u8) -> bool {
            if self.next.is_symbol(symbol) {
                self.next_token();
                true
            } else {
                false
            }
        }
    }

    fn between(x: i64, low: i64, high: i64) -> bool {
        x >= low && x <= high
    }
    fn is_month(x: i64) -> bool {
        between(x, 1, 12)
    }
    fn is_day(x: i64) -> bool {
        between(x, 1, 31)
    }
    fn is_hour(x: i64) -> bool {
        between(x, 0, 23)
    }
    fn is_minute(x: i64) -> bool {
        between(x, 0, 59)
    }

    #[derive(Default)]
    struct Day {
        comp: [i64; 3],
        index: usize,
        named_month: Option<i64>,
        iso: bool,
    }

    impl Day {
        fn add(&mut self, n: i64) -> bool {
            if self.index == 3 {
                return false;
            }
            self.comp[self.index] = n;
            self.index += 1;
            true
        }

        /// (year, month 0-based, day).
        fn write(&mut self) -> Option<(i64, i64, i64)> {
            if self.index < 1 {
                return None;
            }
            while self.index < 3 {
                self.comp[self.index] = 1;
                self.index += 1;
            }
            let (mut year, month, day);
            match self.named_month {
                None => {
                    if self.iso || !is_day(self.comp[0]) {
                        year = self.comp[0];
                        month = self.comp[1];
                        day = self.comp[2];
                    } else {
                        month = self.comp[0];
                        day = self.comp[1];
                        year = self.comp[2];
                    }
                }
                Some(named) => {
                    month = named;
                    if !is_day(self.comp[0]) {
                        year = self.comp[0];
                        day = self.comp[1];
                    } else {
                        day = self.comp[0];
                        year = self.comp[1];
                    }
                }
            }
            if !self.iso {
                if between(year, 0, 49) {
                    year += 2000;
                } else if between(year, 50, 99) {
                    year += 1900;
                }
            }
            if !is_month(month) || !is_day(day) {
                return None;
            }
            Some((year, month - 1, day))
        }
    }

    struct Time {
        comp: [i64; 4],
        index: usize,
        hour_offset: Option<i64>,
    }

    impl Time {
        fn is_empty(&self) -> bool {
            self.index == 0
        }
        fn is_expecting(&self, n: i64) -> bool {
            (self.index == 1 && is_minute(n)) || (self.index == 2 && is_minute(n)) || (self.index == 3 && between(n, 0, 999))
        }
        fn add(&mut self, n: i64) -> bool {
            if self.index < 4 {
                self.comp[self.index] = n;
                self.index += 1;
                true
            } else {
                false
            }
        }
        fn add_final(&mut self, n: i64) -> bool {
            if !self.add(n) {
                return false;
            }
            while self.index < 4 {
                self.comp[self.index] = 0;
                self.index += 1;
            }
            true
        }
        fn write(&mut self) -> Option<[i64; 4]> {
            while self.index < 4 {
                self.comp[self.index] = 0;
                self.index += 1;
            }
            let [mut hour, minute, second, millisecond] = self.comp;
            if let Some(offset) = self.hour_offset {
                if !between(hour, 0, 12) {
                    return None;
                }
                hour %= 12;
                hour += offset;
            }
            if !is_hour(hour) || !is_minute(minute) || !is_minute(second) || !between(millisecond, 0, 999) {
                // A 24th hour is allowed if minutes, seconds and ms are 0.
                if hour != 24 || minute != 0 || second != 0 || millisecond != 0 {
                    return None;
                }
            }
            Some([hour, minute, second, millisecond])
        }
    }

    struct Zone {
        sign: i64,
        hour: i64,
        minute: i64,
    }

    impl Zone {
        fn set(&mut self, offset_hours: i64) {
            self.sign = if offset_hours < 0 { -1 } else { 1 };
            self.hour = offset_hours * self.sign;
            self.minute = 0;
        }
        fn set_sign(&mut self, sign: i64) {
            self.sign = if sign < 0 { -1 } else { 1 };
        }
        fn is_expecting(&self, n: i64) -> bool {
            self.hour != NONE && self.minute == NONE && is_minute(n)
        }
        fn is_utc(&self) -> bool {
            self.hour == 0 && self.minute == 0
        }
        fn is_empty(&self) -> bool {
            self.hour == NONE
        }
        /// UTC offset in seconds; `Some(None)` = local time.
        fn write(&mut self) -> Option<Option<i64>> {
            if self.sign == NONE {
                return Some(None);
            }
            if self.hour == NONE {
                self.hour = 0;
            }
            if self.minute == NONE {
                self.minute = 0;
            }
            let total = (self.hour as u32).wrapping_mul(3600).wrapping_add((self.minute as u32).wrapping_mul(60));
            if total as i64 > i32::MAX as i64 {
                return None;
            }
            let total = total as i64;
            Some(Some(if self.sign < 0 { -total } else { total }))
        }
    }

    /// `ReadMilliseconds`: the first three significant digits.
    fn read_milliseconds(token: Token) -> i64 {
        let Token::Number { value, length } = token else { return 0 };
        let mut number = value;
        if length < 3 {
            if length == 1 {
                number *= 100;
            } else if length == 2 {
                number *= 10;
            }
        } else if length > 3 {
            let mut length = length.min(MAX_SIGNIFICANT_DIGITS);
            let mut factor = 1;
            loop {
                factor *= 10;
                length -= 1;
                if length <= 3 {
                    break;
                }
            }
            number /= factor;
        }
        number
    }

    /// `ParseES5DateTime`: the next token the legacy parser must handle.
    fn parse_es5(scanner: &mut Scanner, day: &mut Day, time: &mut Time, tz: &mut Zone) -> Token {
        if scanner.peek().is_sign() {
            let sign_token = scanner.next_token();
            if !scanner.peek().is_fixed_number(6) {
                return sign_token;
            }
            let sign = sign_token.sign();
            let year = scanner.next_token().number();
            if sign < 0 && year == 0 {
                return sign_token;
            }
            day.add(sign * year);
        } else if scanner.peek().is_fixed_number(4) {
            let year = scanner.next_token().number();
            day.add(year);
        } else {
            return scanner.next_token();
        }
        if scanner.skip_symbol(b'-') {
            if !scanner.peek().is_fixed_number(2) || !is_month(scanner.peek().number()) {
                return scanner.next_token();
            }
            let month = scanner.next_token().number();
            day.add(month);
            if scanner.skip_symbol(b'-') {
                if !scanner.peek().is_fixed_number(2) || !is_day(scanner.peek().number()) {
                    return scanner.next_token();
                }
                let date = scanner.next_token().number();
                day.add(date);
            }
        }
        if !scanner.peek().is_keyword(Keyword::TimeSeparator) {
            if scanner.peek() != Token::EndOfInput {
                return scanner.next_token();
            }
        } else {
            scanner.next_token();
            if !scanner.peek().is_fixed_number(2) || !between(scanner.peek().number(), 0, 24) {
                return Token::Invalid;
            }
            let hour_is_24 = scanner.peek().number() == 24;
            let hour = scanner.next_token().number();
            time.add(hour);
            if !scanner.skip_symbol(b':') {
                return Token::Invalid;
            }
            if !scanner.peek().is_fixed_number(2) || !is_minute(scanner.peek().number()) || (hour_is_24 && scanner.peek().number() > 0) {
                return Token::Invalid;
            }
            let minute = scanner.next_token().number();
            time.add(minute);
            if scanner.skip_symbol(b':') {
                if !scanner.peek().is_fixed_number(2) || !is_minute(scanner.peek().number()) || (hour_is_24 && scanner.peek().number() > 0) {
                    return Token::Invalid;
                }
                let second = scanner.next_token().number();
                time.add(second);
                if scanner.skip_symbol(b'.') {
                    if !matches!(scanner.peek(), Token::Number { .. }) || (hour_is_24 && scanner.peek().number() > 0) {
                        return Token::Invalid;
                    }
                    let token = scanner.next_token();
                    time.add(read_milliseconds(token));
                }
            }
            if scanner.peek().is_keyword_z() {
                scanner.next_token();
                tz.set(0);
            } else if scanner.peek().is_sign() {
                let sign = scanner.next_token().sign();
                tz.set_sign(sign);
                if scanner.peek().is_fixed_number(4) {
                    let hour_minute = scanner.next_token().number();
                    let (hour, minute) = (hour_minute / 100, hour_minute % 100);
                    if !is_hour(hour) || !is_minute(minute) {
                        return Token::Invalid;
                    }
                    tz.hour = hour;
                    tz.minute = minute;
                } else {
                    if !scanner.peek().is_fixed_number(2) || !is_hour(scanner.peek().number()) {
                        return Token::Invalid;
                    }
                    tz.hour = scanner.next_token().number();
                    if !scanner.skip_symbol(b':') {
                        return Token::Invalid;
                    }
                    if !scanner.peek().is_fixed_number(2) || !is_minute(scanner.peek().number()) {
                        return Token::Invalid;
                    }
                    tz.minute = scanner.next_token().number();
                }
            }
            if scanner.peek() != Token::EndOfInput {
                return Token::Invalid;
            }
        }
        // Date-only forms without a zone are UTC; date-times are local.
        if tz.is_empty() && time.is_empty() {
            tz.set(0);
        }
        day.iso = true;
        Token::EndOfInput
    }

    /// `MakeDay(year, month, date)` (finite inputs).
    fn make_day(year: i64, month: i64, date: i64) -> Option<f64> {
        if !(-1_000_000..=1_000_000).contains(&year) || !(-10_000_000..=10_000_000).contains(&month) {
            return None;
        }
        let mut y = year;
        let mut m = month;
        y += m / 12;
        m %= 12;
        if m < 0 {
            m += 12;
            y -= 1;
        }
        const YEAR_DELTA: i64 = 399_999;
        let days_to = |year: i64| 365 * (year + YEAR_DELTA) + (year + YEAR_DELTA) / 4 - (year + YEAR_DELTA) / 100 + (year + YEAR_DELTA) / 400;
        let base_day = days_to(1970);
        let mut day_from_year = days_to(y) - base_day;
        const COMMON: [i64; 12] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
        const LEAP: [i64; 12] = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335];
        let leap = !((y % 4 != 0) || (y % 100 == 0 && y % 400 != 0));
        day_from_year += if leap { LEAP[m as usize] } else { COMMON[m as usize] };
        Some((day_from_year - 1) as f64 + date as f64)
    }

    pub fn parse(text: &str) -> Option<f64> {
        let mut scanner = Scanner::new(text);
        let mut day = Day::default();
        let mut time = Time { comp: [0; 4], index: 0, hour_offset: None };
        let mut tz = Zone { sign: NONE, hour: NONE, minute: NONE };
        let first = parse_es5(&mut scanner, &mut day, &mut time, &mut tz);
        if first == Token::Invalid {
            return None;
        }
        let mut has_read_number = day.index > 0;
        let mut token = first;
        while token != Token::EndOfInput {
            match token {
                Token::Number { value: n, .. } => {
                    has_read_number = true;
                    if scanner.skip_symbol(b':') {
                        if scanner.skip_symbol(b':') {
                            if !time.is_empty() {
                                return None;
                            }
                            time.add(n);
                            time.add(0);
                        } else {
                            if !time.add(n) {
                                return None;
                            }
                            if scanner.peek().is_symbol(b'.') {
                                scanner.next_token();
                            }
                        }
                    } else if scanner.skip_symbol(b'.') && time.is_expecting(n) {
                        time.add(n);
                        if !matches!(scanner.peek(), Token::Number { .. }) {
                            return None;
                        }
                        let ms = read_milliseconds(scanner.next_token());
                        if ms < 0 {
                            return None;
                        }
                        time.add_final(ms);
                    } else if tz.is_expecting(n) {
                        tz.minute = n;
                    } else if time.is_expecting(n) {
                        time.add_final(n);
                        let peek = scanner.peek();
                        if !(peek == Token::EndOfInput || peek == Token::WhiteSpace || peek.is_keyword_z() || peek.is_sign()) {
                            return None;
                        }
                    } else {
                        if !day.add(n) {
                            return None;
                        }
                        scanner.skip_symbol(b'-');
                    }
                }
                Token::Keyword { kind, value, .. } => {
                    if kind == Keyword::AmPm && !time.is_empty() {
                        time.hour_offset = Some(value);
                    } else if kind == Keyword::MonthName {
                        day.named_month = Some(value);
                        scanner.skip_symbol(b'-');
                    } else if kind == Keyword::TimeZoneName && has_read_number {
                        tz.set(value);
                    } else {
                        // Garbage words are illegal once a number was read,
                        // and must be separated from the first number.
                        if has_read_number || matches!(scanner.peek(), Token::Number { .. }) {
                            return None;
                        }
                    }
                }
                _ if token.is_sign() && (tz.is_utc() || !time.is_empty()) => {
                    tz.set_sign(token.sign());
                    let (mut n, mut length) = (0, 0);
                    if let Token::Number { value, length: l } = scanner.peek() {
                        scanner.next_token();
                        n = value;
                        length = l;
                    }
                    has_read_number = true;
                    if scanner.peek().is_symbol(b':') {
                        tz.hour = n;
                        tz.minute = NONE;
                    } else if length == 2 || length == 1 {
                        tz.hour = n;
                        tz.minute = 0;
                    } else if length == 4 || length == 3 {
                        tz.hour = n / 100;
                        tz.minute = n % 100;
                    } else {
                        return None;
                    }
                }
                _ if (token.is_sign() || token.is_symbol(b')')) && has_read_number => return None,
                _ => {}
            }
            token = scanner.next_token();
        }
        let (year, month, date) = day.write()?;
        let [hour, minute, second, millisecond] = time.write()?;
        let offset = tz.write()?;
        let day_number = make_day(year, month, date)?;
        let time_of_day = (hour * 3_600_000 + minute * 60_000 + second * 1000 + millisecond) as f64;
        let mut value = day_number * 86_400_000.0 + time_of_day;
        match offset {
            // Local time, read as UTC (`TZ=UTC`); V8 rejects local times
            // beyond the time range plus ten days before converting.
            None => {
                if value.abs() > MAX_TIME_MS + 864_000_000.0 {
                    return None;
                }
            }
            Some(seconds) => {
                value -= seconds as f64 * 1000.0;
                if value.abs() > MAX_TIME_MS {
                    return None;
                }
            }
        }
        // `TimeClip`.
        if value.abs() > MAX_TIME_MS {
            return None;
        }
        Some(value.trunc() + 0.0)
    }
}

/// `normalizeRangeValue`.
pub fn normalize_range_value(value: &Value, field: Option<&Value>) -> Option<f64> {
    match value {
        Value::Null => return None,
        Value::String(text) if text.is_empty() => return None,
        _ => {}
    }
    let is_date = field.and_then(|f| f.get("type")).and_then(Value::as_str) == Some("date");
    if is_date {
        // `typeof value === "number" ? value : Date.parse(String(value))`.
        let time = match value {
            Value::Number(n) => n.as_f64(),
            other => parse_date(&js_string(other)),
        };
        return time.filter(|t| t.is_finite());
    }
    let number = to_number(value);
    if number.is_finite() {
        Some(number)
    } else {
        None
    }
}

/// `String(value)` for a JSON value (arrays join with commas, objects are
/// "[object Object]").
pub(crate) fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => crate::util::json::js_number_to_string(number.as_f64().unwrap_or(0.0)),
        Value::String(text) => text.clone(),
        Value::Array(items) => items.iter().map(|item| if item.is_null() { String::new() } else { js_string(item) }).collect::<Vec<_>>().join(","),
        Value::Object(_) => "[object Object]".into(),
    }
}

fn values_contain(selected: &[Value], item: &Value) -> bool {
    !item.is_null() && selected.iter().any(|value| value == item)
}

/// `blockFacetMatches` / `chunkFacetMayPass` over 32-bit summary words.
pub fn words_match(words: &[u64], selected: &HashSet<u32>) -> bool {
    selected.iter().any(|value| {
        let word = (*value / 32) as usize;
        let bit = value % 32;
        words.get(word).map(|w| (w >> bit) & 1 == 1).unwrap_or(false)
    })
}

impl IndexEngine {
    /// `selectedFacetCodes`.
    pub fn selected_facet_codes(&self, field: &str, selected: &[Value]) -> Option<Rc<HashSet<u32>>> {
        if selected.is_empty() {
            return None;
        }
        let manifest = self.manifest.borrow();
        let values = manifest.get("facets").and_then(|f| f.get(field)).and_then(Value::as_array);
        let mut out = HashSet::new();
        if let Some(values) = values {
            for (index, item) in values.iter().enumerate() {
                if values_contain(selected, item.get("value").unwrap_or(&NULL)) || values_contain(selected, item.get("label").unwrap_or(&NULL)) {
                    out.insert(index as u32);
                }
            }
        }
        Some(Rc::new(out))
    }

    fn number_field_meta(&self, field: &str) -> Option<&Value> {
        self.info.number_fields.get(field)
    }

    pub fn normalize_range(&self, field: &str, range: &Value) -> NumberRange {
        let meta = self.number_field_meta(field);
        NumberRange {
            min: normalize_range_value(range.get("min").unwrap_or(&NULL), meta),
            max: normalize_range_value(range.get("max").unwrap_or(&NULL), meta),
        }
    }

    /// `makeDocFilterPlan`.
    pub fn make_doc_filter_plan(&self, filters: &Filters) -> Result<DocFilterPlan> {
        filters.check_facets()?;
        let facets: Vec<(String, Rc<HashSet<u32>>)> = filters
            .facets
            .iter()
            .filter_map(|(field, values)| self.selected_facet_codes(field, values).filter(|s| !s.is_empty()).map(|s| (field.clone(), s)))
            .collect();
        let numbers: Vec<(String, NumberRange)> = filters
            .numbers
            .iter()
            .map(|(field, range)| (field.clone(), self.normalize_range(field, range)))
            .filter(|(_, range)| range.min.is_some() || range.max.is_some())
            .collect();
        let booleans: Vec<(String, bool)> =
            filters.booleans.iter().filter_map(|(field, expected)| boolean_code(expected).map(|code| (field.clone(), code == 2))).collect();
        let geo = filters.geo.clone().filter(|geo| geo.filtered);
        let numbers = match &geo {
            Some(geo) if geo.has_doc_set() => {
                numbers.into_iter().filter(|(field, _)| *field != geo.lat_field && *field != geo.lon_field).collect()
            }
            _ => numbers,
        };
        let active = !facets.is_empty() || !numbers.is_empty() || !booleans.is_empty() || geo.is_some();
        Ok(DocFilterPlan { facets, numbers, booleans, geo, active })
    }

    /// `makeBlockFilterPlan`.
    pub fn make_block_filter_plan(&self, filters: &Filters) -> Result<BlockFilterPlan> {
        filters.check_facets()?;
        let block_fields = self.block_filter_fields.borrow();
        let uint32 = self.facet_summary_uint32.get();
        let mut plan = BlockFilterPlan::default();
        for (field, values) in &filters.facets {
            let Some(selected) = self.selected_facet_codes(field, values).filter(|s| !s.is_empty()) else { continue };
            if block_fields.contains(field) && uint32 {
                plan.facets.push((field.clone(), selected));
            } else {
                plan.unknown_fields.push(field.clone());
            }
        }
        for (field, range) in &filters.numbers {
            let normalized = self.normalize_range(field, range);
            if normalized.min.is_none() && normalized.max.is_none() {
                continue;
            }
            if block_fields.contains(field) {
                plan.numbers.push((field.clone(), normalized));
            } else {
                plan.unknown_fields.push(field.clone());
            }
        }
        for (field, value) in &filters.booleans {
            let Some(code) = boolean_code(value) else { continue };
            if block_fields.contains(field) {
                plan.booleans.push((field.clone(), code));
            } else {
                plan.unknown_fields.push(field.clone());
            }
        }
        plan.active = !plan.facets.is_empty() || !plan.numbers.is_empty() || !plan.booleans.is_empty();
        Ok(plan)
    }

    /// `valueForDoc`.
    pub fn value_for_doc(&self, store: Option<&ValueStore>, field: &str, doc: i32) -> DocValue {
        match store {
            Some(store) if store.doc_values => self.doc_value(field, doc),
            Some(ValueStore { codes: Some(codes), .. }) => if doc < 0 { DocValue::Missing } else { codes.value(field, doc as usize) },
            _ => DocValue::Missing,
        }
    }

    fn value_with_known(&self, store: Option<&ValueStore>, field: &str, doc: i32, known: Option<(&str, &DocValue)>) -> DocValue {
        match known {
            Some((name, value)) if name == field => value.clone(),
            _ => self.value_for_doc(store, field, doc),
        }
    }

    /// `passesFilterPlan` / `passesFilterPlanWithKnown`.
    pub fn passes_filter_plan(&self, doc: i32, store: Option<&ValueStore>, plan: &DocFilterPlan, known: Option<(&str, &DocValue)>) -> bool {
        if !plan.active {
            return true;
        }
        let bitmaps = store.and_then(|s| s.bitmaps.as_deref());
        for (field, selected) in &plan.facets {
            if let Some(bitmaps) = bitmaps {
                if let Some((_, list)) = bitmaps.facets.iter().find(|(name, _)| name == field) {
                    if !list.iter().any(|bitmap| bitmap.has(doc as i64)) {
                        return false;
                    }
                    continue;
                }
            }
            if !self.value_with_known(store, field, doc, known).facet_matches(selected) {
                return false;
            }
        }
        for (field, range) in &plan.numbers {
            let Some(value) = self.value_with_known(store, field, doc, known).as_number() else { return false };
            if range.min.map(|min| value < min).unwrap_or(false) || range.max.map(|max| value > max).unwrap_or(false) {
                return false;
            }
        }
        for (field, expected) in &plan.booleans {
            if let Some(bitmaps) = bitmaps {
                if let Some((_, bitmap)) = bitmaps.booleans.iter().find(|(name, _)| name == field) {
                    if !bitmap.has(doc as i64) {
                        return false;
                    }
                    continue;
                }
            }
            match self.value_with_known(store, field, doc, known).as_bool() {
                Some(value) if value == *expected => {}
                _ => return false,
            }
        }
        if let Some(geo) = &plan.geo {
            if !geo.doc_matches(self, store, doc, known) {
                return false;
            }
        }
        true
    }

    /// `filterPlanFields`.
    pub fn filter_plan_fields(plan: Option<&DocFilterPlan>) -> Vec<String> {
        let Some(plan) = plan.filter(|p| p.active) else { return Vec::new() };
        let mut out: Vec<String> = plan.facets.iter().map(|(f, _)| f.clone()).collect();
        out.extend(plan.numbers.iter().map(|(f, _)| f.clone()));
        out.extend(plan.booleans.iter().map(|(f, _)| f.clone()));
        if let Some(geo) = &plan.geo {
            if !geo.has_doc_set() {
                out.push(geo.lat_field.clone());
                out.push(geo.lon_field.clone());
            }
        }
        out
    }

    /// `planFields`.
    pub fn plan_fields(plan: Option<&DocFilterPlan>, sort_field: Option<&str>) -> Vec<String> {
        let mut out = Vec::new();
        for field in Self::filter_plan_fields(plan).into_iter().chain(sort_field.map(str::to_string)) {
            if !out.contains(&field) {
                out.push(field);
            }
        }
        out
    }

    /// `filterBitmapStoreForPlan`.
    pub async fn filter_bitmap_store_for_plan(&self, plan: &DocFilterPlan) -> Result<Option<Rc<BitmapStore>>> {
        if !plan.active || (plan.facets.is_empty() && plan.booleans.is_empty()) {
            return Ok(None);
        }
        if self.filter_bitmaps_meta().is_none() {
            self.ensure_filter_bitmap_manifest().await?;
        }
        let Some(meta) = self.filter_bitmaps_meta() else { return Ok(None) };
        if !meta.get("fields").map(Value::is_object).unwrap_or(false) {
            return Ok(None);
        }
        let mut store = BitmapStore { facets: Vec::new(), booleans: Vec::new(), covered: HashSet::new() };
        for (field, selected) in &plan.facets {
            if self.filter_bitmap_field(field).and_then(|m| m.get("kind").cloned()) != Some(Value::from("facet")) {
                continue;
            }
            let mut codes: Vec<u32> = selected.iter().copied().collect();
            codes.sort_unstable();
            let mut bitmaps = Vec::new();
            for code in codes {
                if let Some(bitmap) = self.load_filter_bitmap(field, &code.to_string()).await? {
                    bitmaps.push(bitmap);
                }
            }
            if bitmaps.is_empty() {
                continue;
            }
            store.facets.push((field.clone(), bitmaps));
            store.covered.insert(field.clone());
        }
        for (field, expected) in &plan.booleans {
            if self.filter_bitmap_field(field).and_then(|m| m.get("kind").cloned()) != Some(Value::from("boolean")) {
                continue;
            }
            let Some(bitmap) = self.load_filter_bitmap(field, if *expected { "true" } else { "false" }).await? else { continue };
            store.booleans.push((field.clone(), bitmap));
            store.covered.insert(field.clone());
        }
        Ok(if store.covered.is_empty() { None } else { Some(Rc::new(store)) })
    }

    /// `docFilterPlanNeedsDocValues`.
    pub async fn doc_filter_plan_needs_doc_values(&self, plan: &DocFilterPlan) -> Result<bool> {
        if !plan.active {
            return Ok(false);
        }
        if !plan.numbers.is_empty() {
            return Ok(true);
        }
        if plan.geo.as_ref().map(|g| !g.has_doc_set()).unwrap_or(false) {
            return Ok(true);
        }
        if self.filter_bitmaps_meta().is_none() {
            self.ensure_filter_bitmap_manifest().await?;
        }
        let Some(meta) = self.filter_bitmaps_meta() else { return Ok(true) };
        if !meta.get("fields").map(Value::is_object).unwrap_or(false) {
            return Ok(true);
        }
        for (field, selected) in &plan.facets {
            let Some(field_meta) = self.filter_bitmap_field(field) else { return Ok(true) };
            if field_meta.get("kind").and_then(Value::as_str) != Some("facet") {
                return Ok(true);
            }
            for code in selected.iter() {
                if field_meta.get("values").and_then(|v| v.get(code.to_string())).map(|v| v.is_null()).unwrap_or(true) {
                    return Ok(true);
                }
            }
        }
        for (field, expected) in &plan.booleans {
            let Some(field_meta) = self.filter_bitmap_field(field) else { return Ok(true) };
            if field_meta.get("kind").and_then(Value::as_str) != Some("boolean") {
                return Ok(true);
            }
            let key = if *expected { "true" } else { "false" };
            if field_meta.get("values").and_then(|v| v.get(key)).map(|v| v.is_null()).unwrap_or(true) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// `valueStoreForDocs`.
    pub async fn value_store_for_docs(&self, fields: &[String], docs: &[i32]) -> Result<Option<ValueStore>> {
        if fields.is_empty() {
            return Ok(None);
        }
        if self.doc_values_meta().is_none() {
            self.ensure_doc_values_manifest().await?;
        }
        if self.doc_values_meta().is_some() {
            let loaded = self.ensure_doc_values_for_docs(fields, docs).await?;
            return Ok(if loaded { Some(ValueStore { doc_values: true, ..Default::default() }) } else { None });
        }
        Ok(Some(ValueStore { codes: Some(self.load_codes().await?), ..Default::default() }))
    }

    /// `valueStoreForFilterPlan`.
    pub async fn value_store_for_filter_plan(&self, plan: &DocFilterPlan, docs: &[i32], omitted: &[&str]) -> Result<Option<ValueStore>> {
        if !plan.active {
            return Ok(None);
        }
        let bitmaps = if docs.len() <= FILTER_BITMAP_SPARSE_DOC_LIMIT || self.doc_values_meta().is_none() {
            self.filter_bitmap_store_for_plan(plan).await?
        } else {
            None
        };
        let fields: Vec<String> = Self::filter_plan_fields(Some(plan))
            .into_iter()
            .filter(|field| !omitted.contains(&field.as_str()) && !bitmaps.as_ref().map(|b| b.covered.contains(field)).unwrap_or(false))
            .collect();
        let primary = self.value_store_for_docs(&fields, docs).await?;
        let store = match (primary, bitmaps) {
            (primary, None) => primary,
            (None, Some(bitmaps)) => Some(ValueStore { bitmaps: Some(bitmaps), ..Default::default() }),
            (Some(mut primary), Some(bitmaps)) => {
                primary.bitmaps = Some(bitmaps);
                Some(primary)
            }
        };
        if store.is_none() && plan.geo.as_ref().map(|g| g.has_doc_set()).unwrap_or(false) {
            return Ok(Some(ValueStore { geo_doc_set: true, ..Default::default() }));
        }
        Ok(store)
    }

    /// `prefetchFilterPlanDocValues`.
    pub async fn prefetch_filter_plan_doc_values(&self, plan: &DocFilterPlan, docs_per_block: &[Vec<i32>]) -> Result<()> {
        if !plan.active {
            return Ok(());
        }
        let union: Vec<i32> = docs_per_block.iter().flatten().copied().collect();
        if union.is_empty() {
            return Ok(());
        }
        let bitmaps = self.filter_bitmap_store_for_plan(plan).await?;
        let fields: Vec<String> = Self::filter_plan_fields(Some(plan))
            .into_iter()
            .filter(|field| !bitmaps.as_ref().map(|b| b.covered.contains(field)).unwrap_or(false))
            .collect();
        if fields.is_empty() {
            return Ok(());
        }
        if self.doc_values_meta().is_none() {
            self.ensure_doc_values_manifest().await?;
        }
        if self.doc_values_meta().is_none() {
            return Ok(());
        }
        self.ensure_doc_values_for_docs(&fields, &union).await.map(|_| ())
    }

    // -------------------------------------------------------- summaries

    /// `chunkMayPass`.
    pub fn chunk_may_pass(&self, index: usize, plan: Option<&DocFilterPlan>) -> bool {
        let Some(meta) = self.doc_values_meta() else { return true };
        let Some(plan) = plan.filter(|p| p.active) else { return true };
        let chunk = |field: &str| -> Option<DocValueChunkMeta> { meta.field(field).and_then(|f| f.chunks.get(index).cloned().flatten()) };
        for (field, selected) in &plan.facets {
            let Some(summary) = chunk(field) else { continue };
            if let Some(words) = &summary.words {
                if !words_match(words, selected) {
                    return false;
                }
            }
        }
        for (field, range) in &plan.numbers {
            let Some(summary) = chunk(field) else { return false };
            let (Some(min), Some(max)) = (summary.min, summary.max) else { return false };
            if range.min.map(|v| max < v).unwrap_or(false) || range.max.map(|v| min > v).unwrap_or(false) {
                return false;
            }
        }
        for (field, expected) in &plan.booleans {
            let Some(summary) = chunk(field) else { return false };
            let code = if *expected { 2.0 } else { 1.0 };
            let (Some(min), Some(max)) = (summary.min, summary.max) else { return false };
            if max < code || min > code {
                return false;
            }
        }
        true
    }

    /// `candidateDocValueChunks`.
    pub fn candidate_doc_value_chunks(&self, plan: Option<&DocFilterPlan>) -> Vec<usize> {
        let Some(meta) = self.doc_values_meta() else { return Vec::new() };
        let chunk_size = meta.chunk_size_or(self.total()).max(1);
        let count = self.total().div_ceil(chunk_size);
        (0..count).filter(|index| self.chunk_may_pass(*index, plan)).collect()
    }
}

/// `blockMayPass` over a block or superblock summary.
pub fn block_may_pass(filters: &BlockFilters, plan: Option<&BlockFilterPlan>) -> bool {
    let Some(plan) = plan.filter(|p| p.active) else { return true };
    for (field, selected) in &plan.facets {
        if let Some(FilterSummary::Facet { words }) = filters.get(field) {
            if !selected.is_empty() && !words_match(words, selected) {
                return false;
            }
        }
    }
    for (field, range) in &plan.numbers {
        // The JS reads `summary.min`/`summary.max` whatever the summary
        // kind, so a numeric range on a boolean field (sorted text pages
        // merge the sort page's value range into the filters) compares
        // against the boolean codes too.
        let bounds = match filters.get(field) {
            Some(FilterSummary::Number { min: Some(min), max: Some(max) }) => Some((*min, *max)),
            Some(FilterSummary::Boolean { min, max }) => Some((*min as f64, *max as f64)),
            _ => None,
        };
        if let Some((min, max)) = bounds {
            if range.min.map(|v| max < v).unwrap_or(false) || range.max.map(|v| min > v).unwrap_or(false) {
                return false;
            }
        }
    }
    for (field, code) in &plan.booleans {
        match filters.get(field) {
            Some(FilterSummary::Boolean { min, max }) if *max != 0 => {
                if *max < *code || *min > *code {
                    return false;
                }
            }
            Some(FilterSummary::Number { min: Some(min), max: Some(max) }) if *max != 0.0 => {
                let code = *code as f64;
                if *max < code || *min > code {
                    return false;
                }
            }
            _ => {}
        }
    }
    true
}

/// `blockDefinitelyPassesDocFilter`.
pub fn block_definitely_passes(filters: &BlockFilters, plan: Option<&DocFilterPlan>) -> bool {
    let Some(plan) = plan.filter(|p| p.active) else { return true };
    if !plan.facets.is_empty() || plan.geo.is_some() {
        return false;
    }
    for (field, range) in &plan.numbers {
        let Some(FilterSummary::Number { min: Some(min), max: Some(max) }) = filters.get(field) else { return false };
        if range.min.map(|v| *min < v).unwrap_or(false) || range.max.map(|v| *max > v).unwrap_or(false) {
            return false;
        }
    }
    for (field, expected) in &plan.booleans {
        let code = if *expected { 2 } else { 1 };
        match filters.get(field) {
            Some(FilterSummary::Boolean { min, max }) if *min == code && *max == code => {}
            _ => return false,
        }
    }
    true
}

/// Sorted-page summaries (`pageSummaryForField`).
pub fn page_summary(page: &crate::codec::sort_tree::SortPageMeta, field: &str, sorted_field: &str) -> Option<NumberRange> {
    if let Some(summary) = page.summary(field) {
        return Some(NumberRange { min: summary.min, max: summary.max });
    }
    if field == sorted_field && page.min.is_finite() && page.max.is_finite() {
        return Some(NumberRange { min: Some(page.min), max: Some(page.max) });
    }
    None
}

/// `pageMayPassDocValueFilter`.
pub fn page_may_pass(page: &crate::codec::sort_tree::SortPageMeta, plan: Option<&DocFilterPlan>, sorted_field: &str) -> bool {
    let Some(plan) = plan.filter(|p| p.active) else { return true };
    for (field, range) in &plan.numbers {
        let Some(NumberRange { min: Some(min), max: Some(max) }) = page_summary(page, field, sorted_field) else { return false };
        if range.min.map(|v| max < v).unwrap_or(false) || range.max.map(|v| min > v).unwrap_or(false) {
            return false;
        }
    }
    for (field, expected) in &plan.booleans {
        let code = if *expected { 2.0 } else { 1.0 };
        let Some(NumberRange { min: Some(min), max: Some(max) }) = page_summary(page, field, sorted_field) else { return false };
        if max < code || min > code {
            return false;
        }
    }
    true
}

/// `pageDefinitelyPassesDocValueFilter`.
pub fn page_definitely_passes(page: &crate::codec::sort_tree::SortPageMeta, plan: Option<&DocFilterPlan>, sorted_field: &str) -> bool {
    let Some(plan) = plan.filter(|p| p.active) else { return true };
    if !plan.facets.is_empty() || plan.geo.is_some() {
        return false;
    }
    for (field, range) in &plan.numbers {
        let Some(NumberRange { min: Some(min), max: Some(max) }) = page_summary(page, field, sorted_field) else { return false };
        if range.min.map(|v| min < v).unwrap_or(false) || range.max.map(|v| max > v).unwrap_or(false) {
            return false;
        }
    }
    for (field, expected) in &plan.booleans {
        let code = if *expected { 2.0 } else { 1.0 };
        let Some(NumberRange { min: Some(min), max: Some(max) }) = page_summary(page, field, sorted_field) else { return false };
        if min != code || max != code {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_iso_dates_like_date_parse() {
        assert_eq!(parse_date("1970-01-01"), Some(0.0));
        assert_eq!(parse_date("2010-01-01"), Some(1_262_304_000_000.0));
        assert_eq!(parse_date("2010-01-01T01:00:00Z"), Some(1_262_307_600_000.0));
        assert_eq!(parse_date("2010-01-01T01:00:00+01:00"), Some(1_262_304_000_000.0));
        assert_eq!(parse_date("2026"), Some(1_767_225_600_000.0));
        assert_eq!(parse_date("nope"), None);
    }

    #[test]
    fn boolean_codes() {
        assert_eq!(boolean_code(&Value::Bool(true)), Some(2));
        assert_eq!(boolean_code(&Value::from("0")), Some(1));
        assert_eq!(boolean_code(&Value::from("yes")), None);
    }
}
