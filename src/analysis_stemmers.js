// Light per-language stemmers for the "multi-v1" analysis profile.
//
// These follow the "light stemming" school (Savoy; Lucene's *LightStemmer
// family): strip plural, gender, case, and a few very common derivational
// endings, and never try full morphological analysis. For ranked retrieval,
// light stemmers match or beat aggressive Snowball stemmers in most
// published evaluations while making far fewer embarrassing conflations.
//
// Every stemmer receives tokens that already passed foldMulti (lowercased,
// diacritics folded, script-specific normalization applied), so suffix
// tables are folded once at module load and rules are written against the
// folded forms.

import { foldMulti } from "./analysis_fold.js";

const VOWELS = new Set("aeiouy");

function hasVowel(token) {
  for (const ch of token) if (VOWELS.has(ch)) return true;
  return false;
}

// Drop a doubled final consonant ("hopped" → "hopp" → "hop"). l/s/z stay
// doubled, matching Porter step 1b ("fall", "guess", "buzz").
function undouble(token) {
  const n = token.length;
  if (n < 4) return token;
  const last = token[n - 1];
  if (token[n - 2] !== last || VOWELS.has(last) || "lsz".includes(last)) return token;
  return token.slice(0, -1);
}

function folded(suffixes) {
  return suffixes.map(suffix => foldMulti(suffix));
}

// Strip the first (longest-first) matching suffix that leaves at least
// minStem characters. Returns the token unchanged when nothing applies.
function stripOne(token, suffixes, minStem) {
  for (const suffix of suffixes) {
    if (token.length - suffix.length >= minStem && token.endsWith(suffix)) {
      return token.slice(0, token.length - suffix.length);
    }
  }
  return token;
}

function stemEnglish(token) {
  let t = token;
  if (t.length > 4 && t.endsWith("ies") && !t.endsWith("eies") && !t.endsWith("aies")) {
    t = `${t.slice(0, -3)}y`;
  } else if (t.length > 3 && t.endsWith("es") && !t.endsWith("aes") && !t.endsWith("ees") && !t.endsWith("oes")) {
    t = t.slice(0, -1);
  } else if (t.length > 3 && t.endsWith("s") && !t.endsWith("ss") && !t.endsWith("us") && !t.endsWith("is")) {
    t = t.slice(0, -1);
  }
  if (t.length > 5 && t.endsWith("ing") && hasVowel(t.slice(0, -3))) {
    t = undouble(t.slice(0, -3));
  } else if (t.length > 4 && t.endsWith("ed") && hasVowel(t.slice(0, -2))) {
    t = undouble(t.slice(0, -2));
  }
  return t;
}

const FRENCH_SUFFIXES = folded([
  "issement", "atrice", "ateur", "ation", "ement", "euse", "ante", "ence",
  "ité", "eur", "ive", "ant", "ent", "ée", "if", "er", "ez", "e"
]);

function stemFrench(token) {
  let t = token;
  if (t.length > 5 && t.endsWith("eaux")) return t.slice(0, -1);
  if (t.length > 4 && t.endsWith("aux")) return `${t.slice(0, -2)}l`;
  if (t.length > 3 && (t.endsWith("x") || t.endsWith("s") || t.endsWith("z"))) t = t.slice(0, -1);
  t = stripOne(t, FRENCH_SUFFIXES, 3);
  return undouble(t);
}

const GERMAN_SUFFIXES = folded([
  "heiten", "ungen", "heit", "ung", "isch", "ern", "en", "er", "es", "em", "e"
]);

function stemGerman(token) {
  let t = stripOne(token, GERMAN_SUFFIXES, 3);
  // Plural -s only ever follows these consonants in native inflection.
  if (t.length > 3 && t.endsWith("s") && "bdfghklmnt".includes(t[t.length - 2])) {
    t = t.slice(0, -1);
  }
  return t;
}

function stemSpanish(token) {
  let t = token;
  if (t.length > 4 && t.endsWith("ces")) return `${t.slice(0, -3)}z`;
  if (t.length > 3 && t.endsWith("s") && !t.endsWith("ss")) t = t.slice(0, -1);
  if (t.length > 3 && (t.endsWith("o") || t.endsWith("a") || t.endsWith("e"))) t = t.slice(0, -1);
  return t;
}

function stemItalian(token) {
  let t = token;
  if (t.length > 3 && "aeio".includes(t[t.length - 1])) t = t.slice(0, -1);
  return undouble(t);
}

function stemPortuguese(token) {
  let t = token;
  // Folded forms: ções → coes, ães → aes, ão → ao.
  if (t.length > 5 && t.endsWith("oes")) return `${t.slice(0, -3)}ao`;
  if (t.length > 4 && t.endsWith("aes")) return `${t.slice(0, -3)}ao`;
  if (t.length > 4 && t.endsWith("ais")) return `${t.slice(0, -2)}l`;
  if (t.length > 4 && t.endsWith("eis")) return `${t.slice(0, -2)}l`;
  if (t.length > 4 && t.endsWith("ois")) return `${t.slice(0, -2)}l`;
  if (t.length > 3 && t.endsWith("s") && !t.endsWith("ss")) t = t.slice(0, -1);
  if (t.length > 3 && (t.endsWith("a") || t.endsWith("e") || t.endsWith("o"))) t = t.slice(0, -1);
  return t;
}

function stemDutch(token) {
  let t = token;
  if (t.length > 5 && t.endsWith("jes")) return t.slice(0, -3);
  if (t.length > 4 && t.endsWith("je")) return t.slice(0, -2);
  if (t.length > 4 && t.endsWith("en")) return undouble(t.slice(0, -2));
  if (t.length > 3 && t.endsWith("s") && !t.endsWith("ss")) return t.slice(0, -1);
  if (t.length > 4 && t.endsWith("e")) return t.slice(0, -1);
  return t;
}

const SWEDISH_SUFFIXES = folded([
  "arnas", "ernas", "ornas", "arna", "erna", "orna", "ande", "aste", "aren",
  "are", "ast", "ens", "ans", "or", "ar", "er", "en", "et", "as", "es", "at",
  "a", "e", "s"
]);

function stemSwedish(token) {
  return stripOne(token, SWEDISH_SUFFIXES, 3);
}

const NORWEGIAN_SUFFIXES = folded([
  "endes", "enes", "erne", "eren", "ede", "ene", "ens", "ers", "ets", "en",
  "er", "es", "et", "a", "e", "s"
]);

function stemNorwegian(token) {
  return stripOne(token, NORWEGIAN_SUFFIXES, 3);
}

const DANISH_SUFFIXES = folded([
  "erendes", "erende", "endes", "erne", "ende", "ene", "ens", "ers", "ets",
  "en", "er", "es", "et", "e", "s"
]);

function stemDanish(token) {
  return stripOne(token, DANISH_SUFFIXES, 3);
}

// Enclitic particles first, then the most common case endings. Finnish
// morphology is far richer than this; light stemming only shaves the
// highest-frequency machinery.
const FINNISH_ENCLITICS = folded(["kaan", "kään", "kin", "han", "hän", "pa", "pä", "ko", "kö"]);
const FINNISH_SUFFIXES = folded([
  "issa", "issä", "ista", "istä", "illa", "illä", "ilta", "iltä", "ille",
  "ssa", "ssä", "sta", "stä", "lla", "llä", "lta", "ltä", "lle", "ksi",
  "tta", "ttä", "nsa", "nsä", "aan", "een", "iin", "in", "en", "an", "än"
]);

function stemFinnish(token) {
  let t = stripOne(token, FINNISH_ENCLITICS, 4);
  t = stripOne(t, FINNISH_SUFFIXES, 3);
  return t;
}

// Written against folded Cyrillic, where й → и and ё → е, so ый/ий/ой
// arrive as ыи/ии/ои. Adjective endings, then noun case endings.
const RUSSIAN_SUFFIXES = folded([
  "иями", "ями", "ами", "иях", "иям", "ием", "его", "ого", "ему", "ому",
  "ими", "ыми", "ах", "ях", "ам", "ям", "ом", "ем", "им", "ым", "ов", "ев",
  "ей", "ий", "ый", "ой", "ая", "яя", "ую", "юю", "ое", "ее", "ые", "ие",
  "ья", "ье", "ью", "ия", "ию", "а", "я", "о", "е", "и", "ы", "у", "ю", "ь"
]);

function stemRussian(token) {
  return stripOne(token, RUSSIAN_SUFFIXES, 3);
}

// Folded Greek: final sigma already normalized to σ, tonos stripped.
const GREEK_SUFFIXES = folded([
  "ματων", "ματα", "ουσ", "εων", "εισ", "ος", "ες", "ας", "ης", "ων", "οι",
  "αι", "ο", "η", "α", "ι"
]);

function stemGreek(token) {
  return stripOne(token, GREEK_SUFFIXES, 3);
}

// Larkey's light10: definite-article prefixes, conjunction waw, and the ten
// highest-frequency suffixes. Normalization (harakat, alef variants, alef
// maqsura) already happened in foldMulti.
const ARABIC_PREFIXES = ["وال", "بال", "كال", "فال", "ال", "لل"];
const ARABIC_SUFFIXES = ["ها", "ان", "ات", "ون", "ين", "يه", "ية", "ه", "ة", "ي"];

function stemArabic(token) {
  let t = token;
  if (t.length > 3 && t.startsWith("و")) t = t.slice(1);
  for (const prefix of ARABIC_PREFIXES) {
    if (t.length - prefix.length >= 2 && t.startsWith(prefix)) {
      t = t.slice(prefix.length);
      break;
    }
  }
  for (const suffix of ARABIC_SUFFIXES) {
    if (t.length - suffix.length >= 2 && t.endsWith(suffix)) {
      t = t.slice(0, t.length - suffix.length);
    }
  }
  return t;
}

// Modeled on Lucene's HindiStemmer suffix table (the most frequent
// inflectional endings, longest first, guarded by minimum stem lengths).
const HINDI_SUFFIXES_LONG = ["ाएगी", "ाएगा", "ाओगी", "ाओगे", "एंगी", "एंगे", "ूंगी", "ूंगा", "ातीं", "नाओं", "नाएं", "ताओं", "ताएं", "ियाँ", "ियों", "ियां"];
const HINDI_SUFFIXES_MID = ["ाकर", "ाइए", "ाईं", "ाया", "ेगी", "ेगा", "ोगी", "ोगे", "ाने", "ाना", "ाते", "ाती", "ाता", "तीं", "ाओं", "ाएं", "ुओं", "ुएं", "ुआं"];
const HINDI_SUFFIXES_SHORT = ["कर", "ाओ", "िए", "ाई", "ाए", "ने", "नी", "ना", "ते", "ीं", "ती", "ता", "ाँ", "ां", "ों", "ें"];
const HINDI_SUFFIXES_MIN = ["ो", "े", "ू", "ु", "ी", "ि", "ा"];

function stemHindi(token) {
  if (token.length > 5) {
    const t = stripOne(token, HINDI_SUFFIXES_LONG, token.length - 4);
    if (t !== token) return t;
  }
  if (token.length > 4) {
    const t = stripOne(token, HINDI_SUFFIXES_MID, token.length - 3);
    if (t !== token) return t;
  }
  if (token.length > 3) {
    const t = stripOne(token, HINDI_SUFFIXES_SHORT, token.length - 2);
    if (t !== token) return t;
  }
  if (token.length > 2) {
    return stripOne(token, HINDI_SUFFIXES_MIN, token.length - 1);
  }
  return token;
}

const STEMMERS = {
  en: stemEnglish,
  fr: stemFrench,
  de: stemGerman,
  es: stemSpanish,
  it: stemItalian,
  pt: stemPortuguese,
  nl: stemDutch,
  sv: stemSwedish,
  no: stemNorwegian,
  da: stemDanish,
  fi: stemFinnish,
  ru: stemRussian,
  el: stemGreek,
  ar: stemArabic,
  hi: stemHindi
};

export const STEMMER_LANGUAGES = Object.freeze(Object.keys(STEMMERS));

// Returns the light stemmer for a language, or null when the language has
// none (CJK bigrams and agglutinative languages we do not light-stem yet).
export function stemmerFor(language) {
  return STEMMERS[language] || null;
}
