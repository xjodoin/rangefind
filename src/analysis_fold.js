// Multilingual text folding for the "multi-v1" analysis profile.
//
// This is a leaf module: the stemmer and analyzer modules both import it so
// suffix lists, stopword lists, and tokens all pass through the exact same
// normalization. Everything here is plain deterministic string code — index
// terms are produced under Node and query terms under whatever browser the
// visitor runs, so no locale- or ICU-dependent API is allowed.

// Latin letters that never decompose under NFKD, so the combining-mark strip
// alone cannot fold them.
const LATIN_EXTRAS_RE = /[ßœæøđłı]/g;
const LATIN_EXTRAS = {
  "ß": "ss", // ß
  "œ": "oe", // œ
  "æ": "ae", // æ
  "ø": "o",  // ø
  "đ": "d",  // đ
  "ł": "l",  // ł
  "ı": "i"   // dotless ı
};

// Arabic tatweel, harakat, and superscript alef. The harakat range also
// covers the combining maddah/hamza marks NFKD leaves behind when it
// decomposes آ/أ/إ, so every alef variant folds down to bare alef.
const ARABIC_MARKS_RE = /[\u0640\u064b-\u065f\u0670]/g;
// Hebrew niqqud and cantillation marks.
const HEBREW_MARKS_RE = /[\u0591-\u05c7]/g;
// The Latin/Greek/Cyrillic combining block: covers stripped accents plus the
// decomposed forms of й and ё.
const COMBINING_MARKS_RE = /[\u0300-\u036f]/g;

export function foldMulti(text, options = {}) {
  const foldDiacritics = options.foldDiacritics !== false;
  let out = String(text || "")
    .normalize("NFKD")
    .toLowerCase()
    .replace(LATIN_EXTRAS_RE, ch => LATIN_EXTRAS[ch]);
  if (foldDiacritics) out = out.replace(COMBINING_MARKS_RE, "");
  return out
    .replace(/ς/g, "σ") // Greek final sigma ς → σ
    .replace(ARABIC_MARKS_RE, "")
    .replace(/ٱ/g, "ا") // alef wasla ٱ → ا
    .replace(/ى/g, "ي") // alef maqsura ى → ي
    .replace(HEBREW_MARKS_RE, "");
}
