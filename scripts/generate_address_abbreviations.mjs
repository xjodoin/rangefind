#!/usr/bin/env node

// Regenerates src/address_abbreviations.js from libpostal's address
// dictionaries (https://github.com/openvenues/libpostal, MIT license).
//
// The table maps a normalized query token (post TOKEN_ALIASES) to alternative
// readings — token sequences the same abbreviation could stand for. Readings
// power query-side authority key variants only; the deterministic key shared
// with the builder (normalizeAddressKey) never changes here.
//
// Usage:
//   node scripts/generate_address_abbreviations.mjs            # fetch pinned commit
//   node scripts/generate_address_abbreviations.mjs --dict-dir=path/to/resources/dictionaries
//
// Selection rules keep the table bounded and probe-safe:
// - only single-word, letters-only abbreviations of 2+ characters (single
//   letters are curated by hand — "o" means Ouest, but "c" could be a unit);
// - readings are canonicalized through TOKEN_ALIASES and dropped when they
//   collapse to the abbreviation itself (ave -> avenue -> ave);
// - at most 3 readings per abbreviation, dictionary order (en before fr).

import { writeFileSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { TOKEN_ALIASES } from "../src/address.js";
import { foldMulti } from "../src/analysis_fold.js";

const LIBPOSTAL_COMMIT = "25099c506612b34b23b1bfe286ca6321fcf06f35";
// Language order is reading priority: when one abbreviation has readings in
// several languages ("st", "al", "pl"), earlier languages keep their slot
// under the per-token cap. Unit types come last so street readings win.
const DICTIONARY_LANGUAGES = ["en", "fr", "es", "de", "pt", "it", "nl", "ca", "sv", "da", "nb", "pl", "cs", "ro"];
const DICTIONARIES = [
  ...DICTIONARY_LANGUAGES.flatMap(lang => [`${lang}/street_types.txt`, `${lang}/directionals.txt`]),
  "en/qualifiers.txt",
  "en/unit_types_numbered.txt"
];
// Not every language ships every dictionary; missing files are skipped.
const OPTIONAL_DICTIONARIES = new Set(DICTIONARIES.filter(name => !name.startsWith("en/") && !name.startsWith("fr/")));
const MAX_READINGS_PER_TOKEN = 4;
const MAX_READING_TOKENS = 3;

// Germanic and Scandinavian street names concatenate their type suffix
// ("Marktstraße", "Kerkstraat", "Storgatan"). These full suffixes drive the
// query-side adjacent-pair merge variant ("Markt Straße" -> "marktstrasse");
// the German list comes from libpostal's concatenated_suffixes_separable,
// the Dutch and Scandinavian entries are curated.
const CURATED_CONCATENATED_SUFFIXES = [
  "straat", "laan", "plein", "gracht",
  "gata", "gatan", "vag", "vagen", "gade", "vej", "gate", "veien"
];

// Abbreviated concatenated suffixes ("Hauptstr", "Kerkstr") rewrite to every
// full suffix the abbreviation stands for across languages.
const CURATED_CONCATENATED_SUFFIX_READINGS = {
  str: ["strasse", "straat"]
};

// Hand-curated readings libpostal keeps in other models (saints, unit
// synonyms mapped onto our formatted "Unit" premises, single-letter French
// directionals) plus single-token state/province names in both directions.
// Curated entries take precedence over dictionary-derived readings.
const CURATED = {
  o: [["ouest"]],
  e: [["est"]],
  n: [["nord"]],
  s: [["sud"]],
  st: [["saint"]],
  saint: [["st"]],
  ste: [["sainte"], ["unit"]],
  sainte: [["ste"]],
  mtee: [["montee"]],
  apt: [["unit"]],
  app: [["unit"], ["apt"]],
  suite: [["unit"]]
};

const US_STATES = {
  alabama: "al", alaska: "ak", arizona: "az", arkansas: "ar",
  california: "ca", colorado: "co", connecticut: "ct", delaware: "de",
  florida: "fl", georgia: "ga", hawaii: "hi", idaho: "id", illinois: "il",
  indiana: "in", iowa: "ia", kansas: "ks", kentucky: "ky", louisiana: "la",
  maine: "me", maryland: "md", massachusetts: "ma", michigan: "mi",
  minnesota: "mn", mississippi: "ms", missouri: "mo", montana: "mt",
  nebraska: "ne", nevada: "nv", ohio: "oh", oklahoma: "ok", oregon: "or",
  pennsylvania: "pa", tennessee: "tn", texas: "tx", utah: "ut",
  vermont: "vt", virginia: "va", washington: "wa", wisconsin: "wi",
  wyoming: "wy"
};

const CANADIAN_PROVINCES = {
  alberta: "ab", manitoba: "mb", ontario: "on", quebec: "qc",
  saskatchewan: "sk", yukon: "yt", nunavut: "nu"
};

// These abbreviations collide with everyday address words ("Rue de la Gare",
// "Indiana in ..."), so only the name -> abbreviation direction is kept.
const STATE_ABBREVIATION_STOPWORDS = new Set(["de", "la", "in"]);

function canonToken(token) {
  return TOKEN_ALIASES.get(token) || token;
}

function normalizeEntry(value) {
  return foldMulti(value).toLowerCase().match(/[a-z0-9]+/gu) || [];
}

async function loadDictionary(name, dictDir) {
  if (dictDir) return readFile(resolve(dictDir, name), "utf8");
  const url = `https://raw.githubusercontent.com/openvenues/libpostal/${LIBPOSTAL_COMMIT}/resources/dictionaries/${name}`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
  return response.text();
}

function addReading(readings, token, reading) {
  const key = reading.join(" ");
  if (!key || key === canonToken(token)) return;
  if (!readings.has(token)) readings.set(token, []);
  const existing = readings.get(token);
  if (existing.some(item => item.join(" ") === key)) return;
  if (existing.length >= MAX_READINGS_PER_TOKEN) return;
  existing.push(reading);
}

const dictDirArg = process.argv.find(arg => arg.startsWith("--dict-dir="));
const dictDir = dictDirArg ? dictDirArg.slice("--dict-dir=".length) : "";

const readings = new Map();
for (const [token, tokenReadings] of Object.entries(CURATED)) {
  for (const reading of tokenReadings) addReading(readings, token, reading.map(canonToken));
}
for (const [name, abbrev] of [...Object.entries(US_STATES), ...Object.entries(CANADIAN_PROVINCES)]) {
  addReading(readings, name, [abbrev]);
  if (!STATE_ABBREVIATION_STOPWORDS.has(abbrev)) addReading(readings, abbrev, [name]);
}

for (const name of DICTIONARIES) {
  let text;
  try {
    text = await loadDictionary(name, dictDir);
  } catch (error) {
    if (OPTIONAL_DICTIONARIES.has(name)) continue;
    throw error;
  }
  for (const line of text.split("\n")) {
    const [canonical, ...aliases] = line.trim().split("|");
    if (!canonical || !aliases.length) continue;
    const reading = normalizeEntry(canonical).map(canonToken);
    if (!reading.length || reading.length > MAX_READING_TOKENS) continue;
    for (const alias of aliases) {
      const folded = normalizeEntry(alias);
      if (folded.length !== 1 || !/^[a-z]{2,}$/u.test(folded[0])) continue;
      addReading(readings, canonToken(folded[0]), reading);
    }
  }
}

const concatenatedSuffixes = new Set(CURATED_CONCATENATED_SUFFIXES);
try {
  const text = await loadDictionary("de/concatenated_suffixes_separable.txt", dictDir);
  for (const line of text.split("\n")) {
    const [canonical] = line.trim().split("|");
    const folded = normalizeEntry(canonical || "");
    if (folded.length === 1 && folded[0].length >= 3) concatenatedSuffixes.add(folded[0]);
  }
} catch {
  // The suffix list is additive; the curated entries alone remain valid.
}

const sorted = [...readings.entries()]
  .filter(([, tokenReadings]) => tokenReadings.length)
  .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);

const body = sorted
  .map(([token, tokenReadings]) => `  ["${token}", [${tokenReadings.map(reading => `["${reading.join('", "')}"]`).join(", ")}]]`)
  .join(",\n");

const suffixList = [...concatenatedSuffixes].sort();
const suffixReadings = Object.entries(CURATED_CONCATENATED_SUFFIX_READINGS)
  .map(([abbrev, expansions]) => `  ["${abbrev}", ["${expansions.join('", "')}"]]`)
  .join(",\n");

const output = `// Generated by scripts/generate_address_abbreviations.mjs — do not edit.
// Derived from libpostal's address dictionaries for
// ${DICTIONARY_LANGUAGES.join(", ")}
// (https://github.com/openvenues/libpostal @ ${LIBPOSTAL_COMMIT}, MIT license)
// plus curated saints, unit synonyms, French directionals, and single-token
// state/province names. Keys and reading tokens are already canonicalized
// through TOKEN_ALIASES; readings power query-side authority key variants
// only and never change the deterministic key shared with the builder.
export const ADDRESS_TOKEN_READINGS = new Map([
${body}
]);

// Full street-type suffixes that concatenate onto the name in Germanic and
// Scandinavian addresses ("Marktstraße", "Kerkstraat", "Storgatan"). Queries
// probe an adjacent-pair merge so the spaced form reaches the concatenated
// indexed key.
export const CONCATENATED_STREET_SUFFIXES = new Set(${JSON.stringify(suffixList)});

// Abbreviated concatenated suffixes ("Hauptstr" -> "Hauptstraße",
// "Kerkstr" -> "Kerkstraat") rewritten in place on the token.
export const CONCATENATED_SUFFIX_READINGS = new Map([
${suffixReadings}
]);
`;

writeFileSync(resolve(import.meta.dirname, "../src/address_abbreviations.js"), output);
console.log(`address_abbreviations.js: ${sorted.length} tokens, ${sorted.reduce((sum, [, r]) => sum + r.length, 0)} readings`);
