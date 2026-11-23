//! Hand-ported JS regular-expression primitives used by the authority
//! modules. Each predicate states the exact JS construct it reproduces; the
//! parity fixtures compare them against V8 over every code point.

use unicode_properties::{GeneralCategoryGroup, UnicodeGeneralCategory};

/// `/\s/u` — also the set `String.prototype.trim` strips (WhiteSpace plus
/// LineTerminator). Differs from Rust's `char::is_whitespace`: U+FEFF is
/// included, U+0085 is not.
pub(crate) fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}'
            | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'
    )
}

/// `String.prototype.trim`.
pub(crate) fn js_trim(value: &str) -> &str {
    value.trim_matches(is_js_space)
}

/// `/[\p{L}\p{N}]/u`.
pub(crate) fn is_letter_or_number(c: char) -> bool {
    matches!(c.general_category_group(), GeneralCategoryGroup::Letter | GeneralCategoryGroup::Number)
}

/// `/\w/u` (no `i` flag): ASCII word characters only.
pub(crate) fn is_ascii_word(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// `/\w/iu`: under the `iu` flags ES adds every character whose simple case
/// folding is an ASCII word character — U+017F (ſ → s) and U+212A (Kelvin
/// sign → k). This is the word set `\b` uses in an `iu` regex.
pub(crate) fn is_word_iu(c: char) -> bool {
    is_ascii_word(c) || c == '\u{17f}' || c == '\u{212a}'
}

/// Simple case folding restricted to what can land on ASCII letters.
fn fold_iu(c: char) -> char {
    match c {
        'A'..='Z' => c.to_ascii_lowercase(),
        '\u{17f}' => 's',
        '\u{212a}' => 'k',
        other => other,
    }
}

fn fsa_letter(c: char) -> bool {
    matches!(c, 'a' | 'b' | 'c' | 'e' | 'g' | 'h' | 'j'..='n' | 'p' | 'r' | 's' | 't' | 'v' | 'x' | 'y')
}

fn ldu_letter(c: char) -> bool {
    fsa_letter(c) || c == 'w' || c == 'z'
}

/// `/[abceghj-nprstvxy]/` (case-sensitive).
pub(crate) fn is_fsa(c: char) -> bool {
    fsa_letter(c)
}

/// `/[abceghj-nprstvwxyz]/` (case-sensitive).
pub(crate) fn is_ldu(c: char) -> bool {
    ldu_letter(c)
}

/// `/[abceghj-nprstvxy]/iu`.
pub(crate) fn is_fsa_iu(c: char) -> bool {
    fsa_letter(fold_iu(c))
}

/// `/[abceghj-nprstvwxyz]/iu`.
pub(crate) fn is_ldu_iu(c: char) -> bool {
    ldu_letter(fold_iu(c))
}

/// `/\d/` (any flags): ASCII digits only.
pub(crate) fn is_digit(c: char) -> bool {
    c.is_ascii_digit()
}

/// `value.replace(/[class]+/gu, with)`: every maximal run of code points
/// matching `pred` becomes `with`.
pub(crate) fn replace_runs(value: &str, pred: impl Fn(char) -> bool, with: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut in_run = false;
    for c in value.chars() {
        if pred(c) {
            if !in_run {
                out.push_str(with);
                in_run = true;
            }
        } else {
            out.push(c);
            in_run = false;
        }
    }
    out
}

/// `value.trim().replace(/\s+/gu, " ")`.
pub(crate) fn trim_collapse(value: &str) -> String {
    replace_runs(js_trim(value), is_js_space, " ")
}

/// `value.match(/[a-z0-9]+/gu) || []`.
pub(crate) fn ascii_alnum_runs(value: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for c in value.chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            current.push(c);
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// `^\d+$` over a whole token.
pub(crate) fn all_digits(token: &str) -> bool {
    !token.is_empty() && token.bytes().all(|b| b.is_ascii_digit())
}

/// `^[a-z]+$` with a length window (code points; patterns are ASCII so a
/// non-ASCII token never matches).
pub(crate) fn lower_letters(token: &str, min: usize, max: usize) -> bool {
    let len = token.len();
    len >= min && len <= max && token.bytes().all(|b| b.is_ascii_lowercase())
}

/// `^\d+[a-z]$`.
pub(crate) fn digits_then_letter(token: &str) -> bool {
    let bytes = token.as_bytes();
    bytes.len() >= 2
        && bytes[bytes.len() - 1].is_ascii_lowercase()
        && bytes[..bytes.len() - 1].iter().all(u8::is_ascii_digit)
}

/// `^\d+[a-z]?$`.
pub(crate) fn digits_optional_letter(token: &str) -> bool {
    all_digits(token) || digits_then_letter(token)
}
