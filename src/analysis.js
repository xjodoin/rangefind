// Multilingual analysis for Rangefind ("multi-v1" profile).
//
// One serializable profile object fully determines analysis behavior. The
// builder stores the profile in the manifest and the browser runtime
// reconstructs the identical analyzer from it, so index terms and query
// terms can never drift apart. For the same reason nothing here may depend
// on ICU dictionaries or locale data (no Intl.Segmenter): Node builds the
// index, arbitrary browsers query it, and both must tokenize identically.
//
// Tokenization: Unicode word runs, split at script boundaries. Alphabetic
// runs become one token each (folded, stopword-filtered, light-stemmed per
// language). Han/Kana/Hangul/Thai-class runs become overlapping character
// bigrams (the CJKAnalyzer approach), which needs no dictionary and is
// exactly reproducible everywhere.
//
// Indexes without an analysis profile keep the legacy English/French
// analyzer in analyzer.js, byte-for-byte.

import {
  DEFAULT_STOPWORDS as LEGACY_STOPWORDS,
  analyzeTerms as legacyAnalyzeTerms,
  expandedTermsFromBaseTerms,
  fold as legacyFold,
  queryTerms as legacyQueryTerms,
  stem as legacyStem,
  termCounts as legacyTermCounts,
  tokenize as legacyTokenize
} from "./analyzer.js";
import { foldMulti } from "./analysis_fold.js";
import { rawStopwordLists } from "./analysis_data.js";
import { stemmerFor } from "./analysis_stemmers.js";

export const ANALYSIS_PROFILE_MULTI = "multi-v1";

// Mirrors the runtime's SKIP_MAX_TERMS: queries whose term list exceeds it
// lose the exact skip-search lane, so alternate-language expansion must not
// inflate past it.
const QUERY_TERM_BUDGET = 30;

// ー (katakana prolonged sound mark) and 々 (ideographic iteration
// mark) are Script=Common but only occur inside CJK words.
const WORD_RE = /[\p{L}\p{M}\p{N}ー々]+/gu;
const BIGRAM_CHAR_RE = /[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}\p{Script=Hangul}\p{Script=Thai}\p{Script=Lao}\p{Script=Khmer}\p{Script=Myanmar}ー々]/u;

const SCRIPT_RES = {
  latin: /\p{Script=Latin}/u,
  cyrillic: /\p{Script=Cyrillic}/u,
  greek: /\p{Script=Greek}/u,
  arabic: /\p{Script=Arabic}/u,
  hebrew: /\p{Script=Hebrew}/u,
  devanagari: /\p{Script=Devanagari}/u,
  han: /\p{Script=Han}/u,
  kana: /[\p{Script=Hiragana}\p{Script=Katakana}]/u,
  hangul: /\p{Script=Hangul}/u,
  thai: /\p{Script=Thai}/u
};

const LANG_SCRIPT = {
  ru: "cyrillic", uk: "cyrillic", bg: "cyrillic", sr: "cyrillic",
  be: "cyrillic", mk: "cyrillic",
  el: "greek",
  ar: "arabic", fa: "arabic", ur: "arabic",
  he: "hebrew", yi: "hebrew",
  hi: "devanagari", mr: "devanagari", ne: "devanagari",
  th: "thai",
  zh: "han", ja: "kana", ko: "hangul"
};

function scriptForLanguage(language) {
  return LANG_SCRIPT[language] || "latin";
}

function isSingleDigit(value) {
  if (value.length !== 1) return false;
  const code = value.charCodeAt(0);
  return code >= 48 && code <= 57;
}

// Local copy of config.getPath: config.js imports node:fs, which the
// browser runtime bundle must never pull in.
function pathValue(object, path) {
  if (!path) return "";
  let value = object;
  for (const part of String(path).split(".")) {
    if (value == null) return "";
    value = value[part];
  }
  if (Array.isArray(value)) return value.join(" ");
  return value == null ? "" : String(value);
}

const LANGUAGE_CODE_RE = /^[a-z]{2,3}$/;

function normalizeLanguageCode(value) {
  const code = String(value || "").trim().toLowerCase().split(/[-_]/)[0];
  return LANGUAGE_CODE_RE.test(code) ? code : "";
}

// Turns the user-facing `analysis` config block into the canonical
// serializable profile the manifest stores. Returns null when the block is
// absent (legacy analyzer).
export function normalizeAnalysisConfig(raw) {
  if (raw == null || raw === false) return null;
  if (typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("Rangefind analysis config must be an object.");
  }
  const languages = [];
  for (const value of Array.isArray(raw.languages) ? raw.languages : []) {
    const code = normalizeLanguageCode(value);
    if (!code) throw new Error(`Rangefind analysis language "${value}" is not a valid ISO 639 code.`);
    if (!languages.includes(code)) languages.push(code);
  }
  if (!languages.length) languages.push("en");
  const primary = normalizeLanguageCode(raw.primary);
  const stemming = String(raw.stemming || "light").toLowerCase();
  if (!["light", "off"].includes(stemming)) {
    throw new Error(`Rangefind analysis stemming "${raw.stemming}" must be "light" or "off".`);
  }
  const stopwords = String(raw.stopwords || "default").toLowerCase();
  if (!["default", "off"].includes(stopwords)) {
    throw new Error(`Rangefind analysis stopwords "${raw.stopwords}" must be "default" or "off".`);
  }
  const minLength = Math.max(1, Math.min(4, Math.floor(Number(raw.minLength ?? 2)) || 2));
  return {
    profile: ANALYSIS_PROFILE_MULTI,
    languages,
    primary: primary && languages.includes(primary) ? primary : languages[0],
    languageField: String(raw.languageField || ""),
    detect: raw.detect !== false,
    stemming,
    stopwords,
    foldDiacritics: raw.foldDiacritics !== false,
    minLength
  };
}

function splitScriptRuns(word) {
  const runs = [];
  let current = "";
  let currentBigram = false;
  for (const ch of word) {
    const bigram = BIGRAM_CHAR_RE.test(ch);
    if (current && bigram === currentBigram) {
      current += ch;
    } else {
      if (current) runs.push({ text: current, bigram: currentBigram });
      current = ch;
      currentBigram = bigram;
    }
  }
  if (current) runs.push({ text: current, bigram: currentBigram });
  return runs;
}

function bigramTokens(run, out) {
  const chars = [...run];
  if (chars.length === 1) {
    out.push(chars[0]);
    return;
  }
  for (let i = 0; i < chars.length - 1; i++) {
    out.push(chars[i] + chars[i + 1]);
  }
}

function createMultiAnalyzer(profile) {
  const foldOptions = { foldDiacritics: profile.foldDiacritics };
  const fold = text => foldMulti(text, foldOptions);
  const stopwordSets = new Map();
  const rawLists = rawStopwordLists();

  function stopwordsFor(language) {
    if (profile.stopwords === "off") return EMPTY_SET;
    let set = stopwordSets.get(language);
    if (!set) {
      set = new Set((rawLists[language] || []).map(word => fold(word)).filter(Boolean));
      stopwordSets.set(language, set);
    }
    return set;
  }

  function stemFor(language) {
    return profile.stemming === "light" ? stemmerFor(language) : null;
  }

  // Full normalization for one alphabetic (non-bigram) raw token. Returns
  // "" when the token is dropped.
  function normalizeAlphaToken(raw, language, stopwords, stemmer) {
    if (raw.length < profile.minLength && !isSingleDigit(raw)) return "";
    if (stopwords.has(raw)) return "";
    const term = stemmer ? stemmer(raw) : raw;
    if (term.length < profile.minLength && !isSingleDigit(term)) return "";
    if (stopwords.has(term)) return "";
    return term;
  }

  // Emits [rawToken, term] pairs in text order; term is "" for dropped
  // tokens is never emitted. Bigram tokens pass through untouched.
  function emitTokens(text, language, callback) {
    const lang = profile.languages.includes(language) ? language : profile.primary;
    const stopwords = stopwordsFor(lang);
    const stemmer = stemFor(lang);
    const folded = fold(text);
    WORD_RE.lastIndex = 0;
    let match;
    const bigrams = [];
    while ((match = WORD_RE.exec(folded))) {
      for (const run of splitScriptRuns(match[0])) {
        if (run.bigram) {
          bigrams.length = 0;
          bigramTokens(run.text, bigrams);
          for (const token of bigrams) callback(token, token);
        } else {
          const term = normalizeAlphaToken(run.text, lang, stopwords, stemmer);
          if (term) callback(run.text, term);
        }
      }
    }
  }

  function tokenize(text, options = {}) {
    const unique = options.unique !== false;
    const out = [];
    const seen = unique ? new Set() : null;
    emitTokens(text, options.lang || "", (raw, term) => {
      if (seen) {
        if (seen.has(term)) return;
        seen.add(term);
      }
      out.push(term);
    });
    return out;
  }

  function termCounts(text, options = {}) {
    const counts = new Map();
    emitTokens(text, options.lang || "", (raw, term) => {
      counts.set(term, (counts.get(term) || 0) + 1);
    });
    return counts;
  }

  function analyzeTerms(text, options = {}) {
    const out = [];
    const seen = new Set();
    emitTokens(text, options.lang || "", (raw, term) => {
      if (seen.has(term)) return;
      seen.add(term);
      out.push({ raw, term });
    });
    return out;
  }

  // Script histogram over a bounded sample; letters only.
  function scriptCounts(text) {
    const sample = String(text || "").slice(0, 1600);
    const counts = {};
    for (const ch of sample) {
      for (const [script, re] of Object.entries(SCRIPT_RES)) {
        if (re.test(ch)) {
          counts[script] = (counts[script] || 0) + 1;
          break;
        }
      }
    }
    return counts;
  }

  // Stopword voting among candidate languages sharing one script. Requires
  // two hits and a strict winner; anything weaker falls back to "".
  function stopwordVote(text, candidates) {
    if (candidates.length === 1) return candidates[0];
    const folded = fold(String(text || "").slice(0, 1600));
    const words = folded.match(WORD_RE) || [];
    let best = "";
    let bestHits = 0;
    let secondHits = 0;
    for (const language of candidates) {
      const set = profile.stopwords === "off"
        ? new Set((rawLists[language] || []).map(word => fold(word)))
        : stopwordsFor(language);
      let hits = 0;
      for (const word of words) if (set.has(word)) hits++;
      if (hits > bestHits) {
        secondHits = bestHits;
        bestHits = hits;
        best = language;
      } else if (hits > secondHits) {
        secondHits = hits;
      }
    }
    return bestHits >= 2 && bestHits > secondHits ? best : "";
  }

  function detectLanguage(text) {
    const counts = scriptCounts(text);
    const kana = counts.kana || 0;
    const han = counts.han || 0;
    const hangul = counts.hangul || 0;
    if (kana > 0 && profile.languages.includes("ja")) return "ja";
    if (hangul > 0 && profile.languages.includes("ko")) return "ko";
    if (han > 0) {
      if (profile.languages.includes("zh")) return "zh";
      if (profile.languages.includes("ja")) return "ja";
    }
    let dominant = "";
    let dominantCount = 0;
    for (const [script, count] of Object.entries(counts)) {
      if (script === "han" || script === "kana" || script === "hangul") continue;
      if (count > dominantCount) {
        dominant = script;
        dominantCount = count;
      }
    }
    if (!dominant) return "";
    const candidates = profile.languages.filter(language => scriptForLanguage(language) === dominant);
    if (candidates.length === 1) return candidates[0];
    if (!candidates.length) return "";
    return stopwordVote(text, candidates) || "";
  }

  function docLanguage(doc, config) {
    if (profile.languageField) {
      const explicit = normalizeLanguageCode(pathValue(doc, profile.languageField));
      if (explicit && profile.languages.includes(explicit)) return explicit;
    }
    if (profile.detect) {
      const parts = [];
      let length = 0;
      for (const field of config?.fields || []) {
        const value = pathValue(doc, field.path);
        if (!value) continue;
        parts.push(value.slice(0, 800));
        length += Math.min(value.length, 800);
        if (length >= 1600) break;
      }
      const detected = detectLanguage(parts.join(" "));
      if (detected) return detected;
    }
    return profile.primary;
  }

  function queryPlan(text) {
    const language = detectLanguage(text) || profile.primary;
    const analyzedTerms = analyzeTerms(text, { lang: language });
    const baseTerms = analyzedTerms.map(item => item.term);
    const terms = new Set(expandedTermsFromBaseTerms(baseTerms));
    // Alternate-language plans: recall insurance when detection guesses
    // wrong or the index mixes languages. The runtime picks the base plan
    // whose stems actually have postings; here every distinct candidate is
    // prepared and its terms join the retrieval union — base stems before
    // phrase expansions, capped at the runtime planner's skip-search term
    // budget (SKIP_MAX_TERMS) so extra languages never silently push a
    // query onto the full-scan path.
    const altPlans = [];
    const seenPlans = new Set([baseTerms.join(" ")]);
    for (const alt of profile.languages) {
      if (alt === language) continue;
      const altAnalyzed = analyzeTerms(text, { lang: alt });
      if (!altAnalyzed.length) continue;
      const altBase = altAnalyzed.map(item => item.term);
      const key = altBase.join(" ");
      if (seenPlans.has(key)) continue;
      seenPlans.add(key);
      altPlans.push({ language: alt, analyzedTerms: altAnalyzed, baseTerms: altBase });
    }
    for (const alt of altPlans) {
      for (const term of alt.baseTerms) {
        if (terms.size >= QUERY_TERM_BUDGET) break;
        terms.add(term);
      }
    }
    for (const alt of altPlans) {
      for (const term of expandedTermsFromBaseTerms(alt.baseTerms)) {
        if (terms.size >= QUERY_TERM_BUDGET) break;
        terms.add(term);
      }
    }
    return { language, analyzedTerms, baseTerms, terms: [...terms], altPlans };
  }

  function queryTerms(text) {
    return queryPlan(text).terms;
  }

  function highlightTerms(query, correctedQuery = "") {
    const terms = new Set();
    for (const language of profile.languages) {
      for (const item of analyzeTerms(String(query || ""), { lang: language })) terms.add(item.term);
      if (correctedQuery) {
        for (const item of analyzeTerms(String(correctedQuery || ""), { lang: language })) terms.add(item.term);
      }
    }
    return terms;
  }

  // Match ranges inside one raw display word, relative to the word start.
  // Alphabetic words match whole (any language's stem); bigram-script runs
  // match per bigram so CJK highlights stay tight.
  function wordMatchRanges(word, termSet) {
    const ranges = [];
    let offset = 0;
    for (const run of splitScriptRuns(word)) {
      if (run.bigram) {
        const chars = [...run.text];
        if (chars.length === 1) {
          if (termSet.has(chars[0])) ranges.push([offset, offset + chars[0].length]);
        } else {
          let charOffset = offset;
          for (let i = 0; i < chars.length - 1; i++) {
            const bigram = chars[i] + chars[i + 1];
            if (termSet.has(bigram)) {
              const start = charOffset;
              const end = charOffset + chars[i].length + chars[i + 1].length;
              const previous = ranges[ranges.length - 1];
              if (previous && previous[1] >= start) previous[1] = end;
              else ranges.push([start, end]);
            }
            charOffset += chars[i].length;
          }
        }
        offset += run.text.length;
      } else {
        const folded = fold(run.text);
        let matched = termSet.has(folded);
        if (!matched && profile.stemming === "light") {
          for (const language of profile.languages) {
            const stemmer = stemmerFor(language);
            if (stemmer && termSet.has(stemmer(folded))) {
              matched = true;
              break;
            }
          }
        }
        if (matched) ranges.push([offset, offset + run.text.length]);
        offset += run.text.length;
      }
    }
    return ranges;
  }

  // Canonical form of a display word for counting distinct matched terms
  // in snippet windows: folded, then primary-language stem.
  function canonicalTerm(word) {
    const foldedWord = fold(word);
    const stemmer = stemFor(profile.primary);
    return stemmer ? stemmer(foldedWord) : foldedWord;
  }

  return {
    isMultilingual: true,
    profile,
    languages: profile.languages,
    fold,
    tokenize,
    termCounts,
    analyzeTerms,
    detectLanguage,
    docLanguage,
    queryPlan,
    queryTerms,
    highlightTerms,
    wordMatchRanges,
    canonicalTerm
  };
}

const EMPTY_SET = new Set();

// The legacy analyzer as an instance with the same surface, so call sites
// can hold one object regardless of profile. Behavior is identical to the
// module functions in analyzer.js.
export const LEGACY_ANALYZER = {
  isMultilingual: false,
  profile: null,
  languages: [],
  fold: legacyFold,
  tokenize: (text, options = {}) => legacyTokenize(text, options),
  termCounts: (text, options = {}) => legacyTermCounts(text, options),
  analyzeTerms: (text) => legacyAnalyzeTerms(text),
  detectLanguage: () => "",
  docLanguage: () => "",
  queryPlan(text) {
    const analyzedTerms = legacyAnalyzeTerms(text);
    return {
      language: "",
      analyzedTerms,
      baseTerms: analyzedTerms.map(item => item.term),
      terms: legacyQueryTerms(text),
      altPlans: []
    };
  },
  queryTerms: (text) => legacyQueryTerms(text),
  highlightTerms(query, correctedQuery = "") {
    const terms = new Set();
    for (const item of legacyAnalyzeTerms(String(query || ""))) terms.add(item.term);
    for (const item of legacyAnalyzeTerms(String(correctedQuery || ""))) terms.add(item.term);
    return terms;
  },
  wordMatchRanges(word, termSet) {
    const folded = legacyFold(word);
    if (termSet.has(folded)) return [[0, word.length]];
    const stemmed = legacyStem(folded);
    if (stemmed !== folded && termSet.has(stemmed)) return [[0, word.length]];
    return [];
  },
  canonicalTerm(word) {
    return legacyStem(legacyFold(word));
  }
};

export function createAnalyzer(profile) {
  if (!profile) return LEGACY_ANALYZER;
  if (profile.profile !== ANALYSIS_PROFILE_MULTI) {
    throw new Error(`Rangefind analysis profile "${profile.profile}" is not supported by this runtime; upgrade rangefind.`);
  }
  return createMultiAnalyzer(profile);
}

const configAnalyzers = new WeakMap();

// Builder-side accessor: reconstructs (and caches) the analyzer from the
// serializable config, so worker processes that receive config as JSON get
// an identical instance.
export function analyzerForConfig(config) {
  if (!config || typeof config !== "object") return LEGACY_ANALYZER;
  let analyzer = configAnalyzers.get(config);
  if (!analyzer) {
    analyzer = createAnalyzer(config.analysis || null);
    configAnalyzers.set(config, analyzer);
  }
  return analyzer;
}

const manifestAnalyzers = new WeakMap();

// Runtime-side accessor: the manifest's analysis profile decides which
// analyzer queries run through.
export function analyzerFromManifest(manifest) {
  if (!manifest || typeof manifest !== "object") return LEGACY_ANALYZER;
  let analyzer = manifestAnalyzers.get(manifest);
  if (!analyzer) {
    analyzer = createAnalyzer(manifest.analysis || null);
    manifestAnalyzers.set(manifest, analyzer);
  }
  return analyzer;
}

// Legacy stopwords re-export keeps the "one obvious import" property for
// callers that need the default set alongside profile-aware code.
export { LEGACY_STOPWORDS };
