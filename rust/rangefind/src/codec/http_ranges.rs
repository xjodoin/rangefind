//! `multipart/byteranges` responses — `parseMultipartByteRanges` and
//! `selectMultipartByteRanges` from `src/http_ranges.js`.
//!
//! The JS decodes the body with `TextDecoder("iso-8859-1")`, which is
//! windows-1252: one UTF-16 unit per byte, so text offsets are byte offsets.
//! The search runs over bytes here, with the marker encoded back to
//! windows-1252 (an unencodable boundary can never match, as in JS).

use crate::error::{Error, Result};
use crate::util::bytes::Bytes;

/// windows-1252 code points for bytes 0x80..=0x9F.
const CP1252_HIGH: [u16; 32] = [
    0x20ac, 0x0081, 0x201a, 0x0192, 0x201e, 0x2026, 0x2020, 0x2021, 0x02c6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008d, 0x017d, 0x008f,
    0x0090, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2013, 0x2014, 0x02dc, 0x2122, 0x0161, 0x203a, 0x0153, 0x009d, 0x017e, 0x0178,
];

fn encode_cp1252(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len());
    for unit in text.encode_utf16() {
        let byte = if unit < 0x80 || (0xa0..=0xff).contains(&unit) {
            unit as u8
        } else if let Some(index) = CP1252_HIGH.iter().position(|&cp| cp == unit) {
            0x80 + index as u8
        } else {
            return None;
        };
        out.push(byte);
    }
    Some(out)
}

/// JS `\s` (Unicode whitespace + line terminators).
fn js_whitespace(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'
    )
}

/// `\s` over windows-1252 bytes: ASCII whitespace and NBSP (0xA0).
fn byte_whitespace(byte: u8) -> bool {
    matches!(byte, b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | b' ' | 0xa0)
}

/// `/(?:^|;)\s*boundary=(?:"([^"]+)"|([^;\s]+))/iu`
pub fn multipart_boundary(content_type: &str) -> Option<String> {
    let chars: Vec<char> = content_type.chars().collect();
    let keyword: Vec<char> = "boundary=".chars().collect();
    for start in 0..=chars.len() {
        let mut i = if start == 0 {
            0
        } else if chars[start - 1] == ';' {
            start
        } else {
            continue;
        };
        while i < chars.len() && js_whitespace(chars[i]) {
            i += 1;
        }
        // `\s*` is greedy but may backtrack; "boundary=" never starts with
        // whitespace, so only the maximal run can match.
        if i + keyword.len() > chars.len() {
            continue;
        }
        if !chars[i..i + keyword.len()].iter().zip(&keyword).all(|(a, b)| a.to_lowercase().eq(b.to_lowercase())) {
            continue;
        }
        i += keyword.len();
        if chars.get(i) == Some(&'"') {
            let body: String = chars[i + 1..].iter().take_while(|c| **c != '"').collect();
            let close = i + 1 + body.chars().count();
            if !body.is_empty() && close < chars.len() {
                return Some(body);
            }
        }
        let token: String = chars[i..].iter().take_while(|c| **c != ';' && !js_whitespace(**c)).collect();
        if !token.is_empty() {
            return Some(token);
        }
    }
    None
}

fn find(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if from > haystack.len() {
        return None;
    }
    if needle.is_empty() {
        return Some(from);
    }
    haystack[from..].windows(needle.len()).position(|window| window == needle).map(|i| i + from)
}

/// `/(?:^|\r?\n)content-range:\s*bytes\s+(\d+)-(\d+)\/(?:\d+|\*)/iu`
fn content_range(headers: &[u8]) -> Option<(f64, f64)> {
    const NAME: &[u8] = b"content-range:";
    let mut starts = vec![0usize];
    for (i, byte) in headers.iter().enumerate() {
        if *byte == b'\n' {
            starts.push(i + 1);
        }
    }
    let digits = |pos: usize| -> (usize, usize) {
        let mut end = pos;
        while end < headers.len() && headers[end].is_ascii_digit() {
            end += 1;
        }
        (pos, end)
    };
    let number = |range: (usize, usize)| -> f64 { std::str::from_utf8(&headers[range.0..range.1]).ok().and_then(|text| text.parse::<f64>().ok()).unwrap_or(f64::NAN) };
    for start in starts {
        let mut i = start;
        if i + NAME.len() > headers.len() || !headers[i..i + NAME.len()].eq_ignore_ascii_case(NAME) {
            continue;
        }
        i += NAME.len();
        while i < headers.len() && byte_whitespace(headers[i]) {
            i += 1;
        }
        if i + 5 > headers.len() || !headers[i..i + 5].eq_ignore_ascii_case(b"bytes") {
            continue;
        }
        i += 5;
        let ws = i;
        while i < headers.len() && byte_whitespace(headers[i]) {
            i += 1;
        }
        if i == ws {
            continue;
        }
        let first = digits(i);
        if first.0 == first.1 || headers.get(first.1) != Some(&b'-') {
            continue;
        }
        let second = digits(first.1 + 1);
        if second.0 == second.1 || headers.get(second.1) != Some(&b'/') {
            continue;
        }
        let tail = second.1 + 1;
        let total = digits(tail);
        if total.0 == total.1 && headers.get(tail) != Some(&b'*') {
            continue;
        }
        return Some((number(first), number(second)));
    }
    None
}

/// One part of a multipart byte-range response.
#[derive(Clone, Debug)]
pub struct MultipartPart {
    pub start: u64,
    /// Exclusive end.
    pub end: u64,
    pub bytes: Bytes,
}

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// `parseMultipartByteRanges(buffer, contentType)`.
pub fn parse_multipart_byte_ranges(body: &Bytes, content_type: &str) -> Result<Vec<MultipartPart>> {
    let boundary = multipart_boundary(content_type).ok_or_else(|| Error::new("Multipart byte-range response is missing its boundary."))?;
    let text: &[u8] = body;
    let marker = encode_cp1252(&format!("--{boundary}"));
    let mut parts = Vec::new();
    let mut cursor = 0usize;
    if let Some(marker) = marker {
        while cursor < text.len() {
            let Some(marker_offset) = find(text, &marker, cursor) else { break };
            let mut header_offset = marker_offset + marker.len();
            if text[header_offset.min(text.len())..].starts_with(b"--") {
                break;
            }
            if text[header_offset.min(text.len())..].starts_with(b"\r\n") {
                header_offset += 2;
            } else if text[header_offset.min(text.len())..].starts_with(b"\n") {
                header_offset += 1;
            }
            let crlf = find(text, b"\r\n\r\n", header_offset);
            let header_end = match crlf {
                Some(at) => Some(at),
                None => find(text, b"\n\n", header_offset),
            };
            let separator = if crlf.is_some() { 4 } else { 2 };
            let Some(header_end) = header_end else {
                return Err(Error::new("Multipart byte-range response has malformed headers."));
            };
            let headers = &text[header_offset.min(header_end)..header_end];
            let Some((start, inclusive_end)) = content_range(headers) else {
                return Err(Error::new("Multipart byte-range part is missing Content-Range."));
            };
            let body_offset = header_end + separator;
            let length = inclusive_end - start + 1.0;
            let safe = start.is_finite() && start == start.trunc() && start.abs() <= MAX_SAFE_INTEGER;
            if !safe || !(length > 0.0) || body_offset as f64 + length > text.len() as f64 {
                return Err(Error::new("Multipart byte-range part has an invalid Content-Range."));
            }
            let length = length as usize;
            parts.push(MultipartPart { start: start as u64, end: (inclusive_end + 1.0) as u64, bytes: body.slice(body_offset, body_offset + length) });
            cursor = body_offset + length;
        }
    }
    if parts.is_empty() {
        return Err(Error::new("Multipart byte-range response contains no parts."));
    }
    Ok(parts)
}

/// `selectMultipartByteRanges(parts, ranges)`: one buffer per requested
/// `(offset, length)`.
pub fn select_multipart_byte_ranges(parts: &[MultipartPart], ranges: &[(u64, u64)]) -> Result<Vec<Bytes>> {
    ranges
        .iter()
        .map(|&(offset, length)| {
            let end = offset + length;
            let part = parts.iter().find(|item| item.start <= offset && item.end >= end);
            match part {
                Some(part) => Ok(part.bytes.slice((offset - part.start) as usize, (end - part.start) as usize)),
                None => Err(Error::new(format!("Multipart byte-range response omitted bytes {}-{}.", offset, end as i128 - 1))),
            }
        })
        .collect()
}
