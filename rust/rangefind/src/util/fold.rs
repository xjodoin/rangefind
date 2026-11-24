//! Multilingual text folding (`src/analysis_fold.js`). A leaf module: the
//! analyzer, stemmers, address and authority normalizers all fold through
//! this exact function so index terms and query terms cannot drift.

use unicode_normalization::UnicodeNormalization;

/// `foldMulti(text, { foldDiacritics })`.
pub fn fold_multi(text: &str, fold_diacritics: bool) -> String {
    let lowered: String = text.nfkd().collect::<String>().to_lowercase();
    let mut out = String::with_capacity(lowered.len());
    for ch in lowered.chars() {
        match ch {
            'ß' => out.push_str("ss"),
            'œ' => out.push_str("oe"),
            'æ' => out.push_str("ae"),
            'ø' => out.push('o'),
            'đ' => out.push('d'),
            'ł' => out.push('l'),
            'ı' => out.push('i'),
            '\u{0300}'..='\u{036f}' if fold_diacritics => {}
            'ς' => out.push('σ'),
            '\u{0640}' | '\u{064b}'..='\u{065f}' | '\u{0670}' => {}
            'ٱ' => out.push('ا'),
            'ى' => out.push('ي'),
            '\u{0591}'..='\u{05c7}' => {}
            other => out.push(other),
        }
    }
    out
}

pub fn fold(text: &str) -> String {
    fold_multi(text, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folds_like_js() {
        assert_eq!(fold("Élève Straße Œuvre"), "eleve strasse oeuvre");
        assert_eq!(fold("ΌΣΟΣ"), "οσοσ");
        assert_eq!(fold_multi("Élève", false), "e\u{301}le\u{300}ve");
        assert_eq!(fold("ﬁ"), "fi");
    }
}
