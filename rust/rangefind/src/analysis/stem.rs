//! Light per-language stemmers (`src/analysis_stemmers.js`).
//!
//! The JS stemmers measure and index strings in UTF-16 code units
//! (`t.length`, `t[n - 1]`, `t.slice(0, -k)`). Every suffix and prefix table
//! is BMP-only, so a suffix match always ends on a char boundary; lengths
//! are counted with [`len16`] and single-unit lookups (`t[n - 1]`) treat an
//! astral char as a lone surrogate that equals nothing in the tables.

use std::sync::OnceLock;

use crate::util::fold::fold_multi;
use crate::util::jsstr::len16;

/// A light stemmer: folded token in, stem out.
pub type Stemmer = fn(&str) -> String;

/// `STEMMER_LANGUAGES`.
pub const STEMMER_LANGUAGES: &[&str] = &[
    "en", "fr", "de", "es", "it", "pt", "nl", "sv", "no", "da", "fi", "ru", "el", "ar", "hi",
];

/// `stemmerFor(language)`: `None` when the language has no light stemmer.
pub fn stemmer_for(language: &str) -> Option<Stemmer> {
    Some(match language {
        "en" => stem_english,
        "fr" => stem_french,
        "de" => stem_german,
        "es" => stem_spanish,
        "it" => stem_italian,
        "pt" => stem_portuguese,
        "nl" => stem_dutch,
        "sv" => stem_swedish,
        "no" => stem_norwegian,
        "da" => stem_danish,
        "fi" => stem_finnish,
        "ru" => stem_russian,
        "el" => stem_greek,
        "ar" => stem_arabic,
        "hi" => stem_hindi,
        _ => return None,
    })
}

fn is_vowel(ch: char) -> bool {
    matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u' | 'y')
}

fn has_vowel(token: &str) -> bool {
    token.chars().any(is_vowel)
}

/// The last UTF-16 code unit as a BMP char (`t[n - 1]`); `None` when the
/// string is empty or ends in an astral char (a lone low surrogate in JS,
/// which matches no table entry).
fn last_unit(token: &str) -> Option<char> {
    token.chars().next_back().filter(|ch| (*ch as u32) <= 0xffff)
}

/// `t[n - 2]` as a BMP char; `None` when that unit is a surrogate.
fn second_last_unit(token: &str) -> Option<char> {
    let mut chars = token.chars().rev();
    let last = chars.next()?;
    if (last as u32) > 0xffff {
        // t[n - 2] is the high surrogate of the final astral char.
        return None;
    }
    chars.next().filter(|ch| (*ch as u32) <= 0xffff)
}

/// `t.slice(0, -1)` for a token whose final char is BMP.
fn drop_last_char(token: &str) -> String {
    let mut chars = token.chars();
    chars.next_back();
    chars.as_str().to_string()
}

fn drop_suffix<'a>(token: &'a str, suffix: &str) -> &'a str {
    &token[..token.len() - suffix.len()]
}

// Drop a doubled final consonant ("hopped" → "hopp" → "hop"). l/s/z stay
// doubled, matching Porter step 1b ("fall", "guess", "buzz").
fn undouble(token: &str) -> String {
    if len16(token) < 4 {
        return token.to_string();
    }
    let (Some(last), Some(previous)) = (last_unit(token), second_last_unit(token)) else {
        return token.to_string();
    };
    if previous != last || is_vowel(last) || matches!(last, 'l' | 's' | 'z') {
        return token.to_string();
    }
    drop_last_char(token)
}

fn folded(suffixes: &[&str]) -> Vec<String> {
    suffixes.iter().map(|suffix| fold_multi(suffix, true)).collect()
}

/// Strip the first matching suffix that leaves at least `min_stem` code
/// units. Returns the token unchanged when nothing applies.
fn strip_one<S: AsRef<str>>(token: &str, suffixes: &[S], min_stem: isize) -> String {
    let length = len16(token) as isize;
    for suffix in suffixes {
        let suffix = suffix.as_ref();
        if length - len16(suffix) as isize >= min_stem && token.ends_with(suffix) {
            return drop_suffix(token, suffix).to_string();
        }
    }
    token.to_string()
}

pub fn stem_english(token: &str) -> String {
    let mut t = token.to_string();
    let n = len16(&t);
    if n > 4 && t.ends_with("ies") && !t.ends_with("eies") && !t.ends_with("aies") {
        t = format!("{}y", drop_suffix(&t, "ies"));
    } else if n > 3 && t.ends_with("es") && !t.ends_with("aes") && !t.ends_with("ees") && !t.ends_with("oes") {
        t = drop_suffix(&t, "s").to_string();
    } else if n > 3 && t.ends_with('s') && !t.ends_with("ss") && !t.ends_with("us") && !t.ends_with("is") {
        t = drop_suffix(&t, "s").to_string();
    }
    let n = len16(&t);
    if n > 5 && t.ends_with("ing") && has_vowel(drop_suffix(&t, "ing")) {
        t = undouble(drop_suffix(&t, "ing"));
    } else if n > 4 && t.ends_with("ed") && has_vowel(drop_suffix(&t, "ed")) {
        t = undouble(drop_suffix(&t, "ed"));
    }
    t
}

fn french_suffixes() -> &'static [String] {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| {
        folded(&[
            "issement", "atrice", "ateur", "ation", "ement", "euse", "ante", "ence", "ité", "eur", "ive", "ant",
            "ent", "ée", "if", "er", "ez", "e",
        ])
    })
}

pub fn stem_french(token: &str) -> String {
    let n = len16(token);
    if n > 5 && token.ends_with("eaux") {
        return drop_suffix(token, "x").to_string();
    }
    if n > 4 && token.ends_with("aux") {
        return format!("{}l", drop_suffix(token, "ux"));
    }
    let mut t = token;
    if n > 3 && (t.ends_with('x') || t.ends_with('s') || t.ends_with('z')) {
        t = &t[..t.len() - 1];
    }
    undouble(&strip_one(t, french_suffixes(), 3))
}

fn german_suffixes() -> &'static [String] {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| folded(&["heiten", "ungen", "heit", "ung", "isch", "ern", "en", "er", "es", "em", "e"]))
}

pub fn stem_german(token: &str) -> String {
    let t = strip_one(token, german_suffixes(), 3);
    // Plural -s only ever follows these consonants in native inflection.
    if len16(&t) > 3 && t.ends_with('s') && second_last_unit(&t).is_some_and(|ch| "bdfghklmnt".contains(ch)) {
        return drop_suffix(&t, "s").to_string();
    }
    t
}

pub fn stem_spanish(token: &str) -> String {
    if len16(token) > 4 && token.ends_with("ces") {
        return format!("{}z", drop_suffix(token, "ces"));
    }
    let mut t = token;
    if len16(t) > 3 && t.ends_with('s') && !t.ends_with("ss") {
        t = &t[..t.len() - 1];
    }
    if len16(t) > 3 && (t.ends_with('o') || t.ends_with('a') || t.ends_with('e')) {
        t = &t[..t.len() - 1];
    }
    t.to_string()
}

pub fn stem_italian(token: &str) -> String {
    let mut t = token;
    if len16(t) > 3 && last_unit(t).is_some_and(|ch| matches!(ch, 'a' | 'e' | 'i' | 'o')) {
        t = &t[..t.len() - 1];
    }
    undouble(t)
}

pub fn stem_portuguese(token: &str) -> String {
    let n = len16(token);
    // Folded forms: ções → coes, ães → aes, ão → ao.
    if n > 5 && token.ends_with("oes") {
        return format!("{}ao", drop_suffix(token, "oes"));
    }
    if n > 4 && token.ends_with("aes") {
        return format!("{}ao", drop_suffix(token, "aes"));
    }
    if n > 4 && (token.ends_with("ais") || token.ends_with("eis") || token.ends_with("ois")) {
        return format!("{}l", drop_suffix(token, "is"));
    }
    let mut t = token;
    if len16(t) > 3 && t.ends_with('s') && !t.ends_with("ss") {
        t = &t[..t.len() - 1];
    }
    if len16(t) > 3 && (t.ends_with('a') || t.ends_with('e') || t.ends_with('o')) {
        t = &t[..t.len() - 1];
    }
    t.to_string()
}

pub fn stem_dutch(token: &str) -> String {
    let n = len16(token);
    if n > 5 && token.ends_with("jes") {
        return drop_suffix(token, "jes").to_string();
    }
    if n > 4 && token.ends_with("je") {
        return drop_suffix(token, "je").to_string();
    }
    if n > 4 && token.ends_with("en") {
        return undouble(drop_suffix(token, "en"));
    }
    if n > 3 && token.ends_with('s') && !token.ends_with("ss") {
        return drop_suffix(token, "s").to_string();
    }
    if n > 4 && token.ends_with('e') {
        return drop_suffix(token, "e").to_string();
    }
    token.to_string()
}

fn swedish_suffixes() -> &'static [String] {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| {
        folded(&[
            "arnas", "ernas", "ornas", "arna", "erna", "orna", "ande", "aste", "aren", "are", "ast", "ens", "ans",
            "or", "ar", "er", "en", "et", "as", "es", "at", "a", "e", "s",
        ])
    })
}

pub fn stem_swedish(token: &str) -> String {
    strip_one(token, swedish_suffixes(), 3)
}

fn norwegian_suffixes() -> &'static [String] {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| {
        folded(&["endes", "enes", "erne", "eren", "ede", "ene", "ens", "ers", "ets", "en", "er", "es", "et", "a", "e", "s"])
    })
}

pub fn stem_norwegian(token: &str) -> String {
    strip_one(token, norwegian_suffixes(), 3)
}

fn danish_suffixes() -> &'static [String] {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| {
        folded(&["erendes", "erende", "endes", "erne", "ende", "ene", "ens", "ers", "ets", "en", "er", "es", "et", "e", "s"])
    })
}

pub fn stem_danish(token: &str) -> String {
    strip_one(token, danish_suffixes(), 3)
}

fn finnish_enclitics() -> &'static [String] {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| folded(&["kaan", "kään", "kin", "han", "hän", "pa", "pä", "ko", "kö"]))
}

fn finnish_suffixes() -> &'static [String] {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| {
        folded(&[
            "issa", "issä", "ista", "istä", "illa", "illä", "ilta", "iltä", "ille", "ssa", "ssä", "sta", "stä", "lla",
            "llä", "lta", "ltä", "lle", "ksi", "tta", "ttä", "nsa", "nsä", "aan", "een", "iin", "in", "en", "an", "än",
        ])
    })
}

pub fn stem_finnish(token: &str) -> String {
    let t = strip_one(token, finnish_enclitics(), 4);
    strip_one(&t, finnish_suffixes(), 3)
}

fn russian_suffixes() -> &'static [String] {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| {
        folded(&[
            "иями", "ями", "ами", "иях", "иям", "ием", "его", "ого", "ему", "ому", "ими", "ыми", "ах", "ях", "ам",
            "ям", "ом", "ем", "им", "ым", "ов", "ев", "ей", "ий", "ый", "ой", "ая", "яя", "ую", "юю", "ое", "ее",
            "ые", "ие", "ья", "ье", "ью", "ия", "ию", "а", "я", "о", "е", "и", "ы", "у", "ю", "ь",
        ])
    })
}

pub fn stem_russian(token: &str) -> String {
    strip_one(token, russian_suffixes(), 3)
}

fn greek_suffixes() -> &'static [String] {
    static TABLE: OnceLock<Vec<String>> = OnceLock::new();
    TABLE.get_or_init(|| {
        folded(&["ματων", "ματα", "ουσ", "εων", "εισ", "ος", "ες", "ας", "ης", "ων", "οι", "αι", "ο", "η", "α", "ι"])
    })
}

pub fn stem_greek(token: &str) -> String {
    strip_one(token, greek_suffixes(), 3)
}

const ARABIC_PREFIXES: &[&str] = &["وال", "بال", "كال", "فال", "ال", "لل"];
const ARABIC_SUFFIXES: &[&str] = &["ها", "ان", "ات", "ون", "ين", "يه", "ية", "ه", "ة", "ي"];

pub fn stem_arabic(token: &str) -> String {
    let mut t = token;
    if len16(t) > 3 {
        if let Some(rest) = t.strip_prefix('و') {
            t = rest;
        }
    }
    for prefix in ARABIC_PREFIXES {
        if len16(t) as isize - len16(prefix) as isize >= 2 {
            if let Some(rest) = t.strip_prefix(prefix) {
                t = rest;
                break;
            }
        }
    }
    for suffix in ARABIC_SUFFIXES {
        if len16(t) as isize - len16(suffix) as isize >= 2 && t.ends_with(suffix) {
            t = drop_suffix(t, suffix);
        }
    }
    t.to_string()
}

const HINDI_SUFFIXES_LONG: &[&str] = &[
    "ाएगी", "ाएगा", "ाओगी", "ाओगे", "एंगी", "एंगे", "ूंगी", "ूंगा", "ातीं", "नाओं", "नाएं", "ताओं", "ताएं", "ियाँ", "ियों", "ियां",
];
const HINDI_SUFFIXES_MID: &[&str] = &[
    "ाकर", "ाइए", "ाईं", "ाया", "ेगी", "ेगा", "ोगी", "ोगे", "ाने", "ाना", "ाते", "ाती", "ाता", "तीं", "ाओं", "ाएं", "ुओं", "ुएं", "ुआं",
];
const HINDI_SUFFIXES_SHORT: &[&str] =
    &["कर", "ाओ", "िए", "ाई", "ाए", "ने", "नी", "ना", "ते", "ीं", "ती", "ता", "ाँ", "ां", "ों", "ें"];
const HINDI_SUFFIXES_MIN: &[&str] = &["ो", "े", "ू", "ु", "ी", "ि", "ा"];

pub fn stem_hindi(token: &str) -> String {
    let n = len16(token) as isize;
    if n > 5 {
        let t = strip_one(token, HINDI_SUFFIXES_LONG, n - 4);
        if t != token {
            return t;
        }
    }
    if n > 4 {
        let t = strip_one(token, HINDI_SUFFIXES_MID, n - 3);
        if t != token {
            return t;
        }
    }
    if n > 3 {
        let t = strip_one(token, HINDI_SUFFIXES_SHORT, n - 2);
        if t != token {
            return t;
        }
    }
    if n > 2 {
        return strip_one(token, HINDI_SUFFIXES_MIN, n - 1);
    }
    token.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn astral_tails_never_undouble() {
        // JS compares the two surrogates of the final astral char, which
        // never match; a char-based check would strip one 𐌰.
        assert_eq!(stem_english("a𐌰𐌰ing"), "a𐌰𐌰");
        assert_eq!(undouble("abbb"), "abb");
        assert_eq!(undouble("ab𐌰𐌰"), "ab𐌰𐌰");
        // Length gates count UTF-16 units: "a😀s" is 4 units.
        assert_eq!(stem_english("a😀s"), "a😀");
        assert_eq!(stem_english("😀s"), "😀s");
    }

    #[test]
    fn known_stems() {
        assert_eq!(stem_english("studies"), "study");
        assert_eq!(stem_english("hopping"), "hop");
        assert_eq!(stem_french("chevaux"), "cheval");
        assert_eq!(stem_french("bateaux"), "bateau");
        assert_eq!(stem_spanish("luces"), "luz");
        assert_eq!(stem_portuguese("nacoes"), "nacao");
    }
}
