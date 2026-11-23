// Parity fixtures for the Rust OSM query-time modules
// (rust/rangefind/src/osm/{category_lexicon,constraints,opening_hours,
// schema,google_maps_adapter}.rs).
//
// Runs the real JS modules from src/integrations/osm/ over broad,
// deterministic inputs and writes their outputs to
// rust/target/fixtures/osm_modules/*.json.
//
// Usage (from the repo root): node rust/tools/fixtures/osm_modules.mjs

import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  CATEGORY_EXCLUDED_TYPES,
  OSM_CANONICAL_TYPES,
  OSM_CATEGORY_LEXICON_VERSION,
  OSM_TYPE_ALIASES,
  buildCategoryLexicon,
  buildCategoryLexiconArtifact,
  defaultCategoryLexicon,
  fold,
  lookupCategory,
  lookupCategoryFuzzy,
  typeQueryText,
  withinOneEdit
} from "../../../src/integrations/osm/category_lexicon.js";
import {
  OSM_CONSTRAINT_DEFINITIONS,
  annotateConstraintResult,
  compileOsmConstraintFilters,
  evaluateOsmConstraints,
  parseOsmConstraints
} from "../../../src/integrations/osm/constraints.js";
import { evaluateOpeningHours } from "../../../src/integrations/osm/opening_hours.js";
import {
  OSM_DISPLAY_FIELDS,
  OSM_INTEGRATION_SCHEMA_VERSION,
  createOsmIndexConfig
} from "../../../src/integrations/osm/schema.js";
import {
  createRangefindMapsAdapter,
  toMigrationPlace
} from "../../../src/integrations/osm/google_maps_adapter.js";

const here = dirname(fileURLToPath(import.meta.url));
const outDir = join(here, "../../target/fixtures/osm_modules");
mkdirSync(outDir, { recursive: true });

function write(name, value) {
  writeFileSync(join(outDir, name), JSON.stringify(value));
  const count = Array.isArray(value) ? value.length : Object.keys(value).length;
  console.log(`${name}: ${count}`);
}

// mulberry32
function rng(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const random = rng(0x05a1);
const pick = list => list[Math.floor(random() * list.length)];
const chance = p => random() < p;

const num = value => (Number.isNaN(value) ? "NaN" : value);

// ---------------------------------------------------------------- tables

const defaultTimeZone = new Intl.DateTimeFormat().resolvedOptions().timeZone;
write("tables.json", {
  canonicalTypes: OSM_CANONICAL_TYPES,
  typeAliases: Object.entries(OSM_TYPE_ALIASES),
  excludedTypes: [...CATEGORY_EXCLUDED_TYPES],
  lexiconVersion: OSM_CATEGORY_LEXICON_VERSION,
  schemaVersion: OSM_INTEGRATION_SCHEMA_VERSION,
  displayFields: OSM_DISPLAY_FIELDS,
  constraintDefinitions: Object.entries(OSM_CONSTRAINT_DEFINITIONS).map(([key, definition]) => ({
    key,
    facet: definition.facet,
    detail: definition.detail ?? null,
    values: definition.values
  })),
  defaultTimeZone
});

// ---------------------------------------------------------------- words

const aliasWords = Object.values(OSM_TYPE_ALIASES).flat();
const extraWords = [
  "", " ", "  -- ", "Cinéma", "CINEMAS", "boulangeries", "hôpitaux", "Chevaux", "churches",
  "parcs", "Café", "café-bar", "crème glacée", "Crème-Glacée", "boîte de nuit", "Straße",
  "ŒUVRE", "ﬁsh", "ﬂorist", "Ⅻ", "½", "日本料理", "ラーメン", "кафе", "Аптека", "εστιατόριο",
  "ΣΟΦΟΣ", "ΌΣΟΣ ΣΟΣ", "مطعم", "מסעדה", "😀 cafe", "cafe 😀", "𝐂𝐚𝐟𝐞", "Ĳssel", "İstanbul",
  "ǅemal", "ŉ", "ß", "café", "élan", "eLaN", "a1b2", "123", "4 seasons", "x", "xx", "es", "aux",
  "s", "the cinema", "restaurant laval", "St-Jérôme", "Québec", "Montréal", "Trois-Rivières",
  "fast_food", "fast food", "fast-food", "FAST FOOD", "ice_cream", "ice cream", "post_office",
  "bureau de poste", "gas station", "Gas Stations", "station-service", "lave-auto", "pâtisseries",
  "cinema paradiso", "tab\tcafe", "line\nbreak", "zero​width", "soft­hyphen",
  "under_score", "semi;colon", "café;bar", "école", "ecole", "écoles", "ÉCOLE",
  "spa", "Spa", "city", "Quebec City", "church tent", "school mother touch"
];

function typos(word) {
  const out = [];
  const units = [...word];
  if (units.length < 3) return out;
  const i = Math.floor(random() * units.length);
  out.push(units.slice(0, i).concat(units.slice(i + 1)).join(""));
  const letters = "abcdefghijklmnopqrstuvwxyzéè";
  out.push(units.slice(0, i).concat([pick([...letters])], units.slice(i)).join(""));
  out.push(units.slice(0, i).concat([pick([...letters])], units.slice(i + 1)).join(""));
  if (i + 1 < units.length) {
    const swapped = units.slice();
    [swapped[i], swapped[i + 1]] = [swapped[i + 1], swapped[i]];
    out.push(swapped.join(""));
  }
  const j = Math.floor(random() * units.length);
  out.push(units.filter((_, index) => index !== i && index !== j).join(""));
  return out;
}

function variants(word) {
  const out = [word];
  if (chance(0.5)) out.push(word.toUpperCase());
  if (chance(0.5)) out.push(word[0]?.toUpperCase() + word.slice(1));
  if (chance(0.4)) out.push(`${word}s`);
  if (chance(0.2)) out.push(`${word}es`);
  if (chance(0.2)) out.push(`${word}x`);
  if (chance(0.3)) out.push(` ${word}! `);
  return out;
}

const baseWords = [...OSM_CANONICAL_TYPES, ...aliasWords, ...extraWords];
const surfaces = [];
for (const word of baseWords) {
  surfaces.push(...variants(word));
  surfaces.push(...typos(word.replaceAll("_", " ")));
}

// ---------------------------------------------------------------- fold

const foldInputs = [...new Set([...surfaces, ...baseWords, "a_b;c__d", ";;;", "_x_", " _ "])];
write("fold.json", foldInputs.map(input => [input, fold(input), typeQueryText(input)]));

// ---------------------------------------------------------------- lexicons

const corpusTypes = [
  "restaurant", "cafe", "fast_food", "bar", "spa", "city", "town", "coffee", "church tent",
  "school mother touch", "very long type value with many words", "a_b_c_d", "x".repeat(40),
  "école", "日本料理", "кафе", "pharmacy", "cinema", "cinemas", "bakery", "123", "_", "",
  "sports_centre", "fitness_centre", "gym", "parking", "parking_entrance", "post_box",
  "ice_cream", "crème", "creme", "carwash", "car_wash", "carpet", "carpets", "cart"
];
const facetRows = corpusTypes.map((value, index) => ({ value, n: (index * 97) % 600 }));
const artifactFromRows = buildCategoryLexiconArtifact(facetRows);
const lexiconInputs = [
  ["null", null],
  ["empty", []],
  ["strings", corpusTypes],
  ["rows", facetRows],
  ["mixed", ["cafe", { value: "bar" }, { value: null }, null, 5, false, { value: 7 }, ["x", "y"], { n: 3 }]],
  ["artifact", artifactFromRows],
  ["artifact-hand", {
    types: ["cafe", "bakery", "coffee", "spa", "kino", null, "", "boulangerie_x"],
    aliases: { "kino": "cinema", "Petit Café": "cafe", "": "bar", "bäckerei": "bakery", "coffee": "coffee" }
  }],
  ["artifact-no-aliases", { types: ["pharmacy", "bar"] }],
  ["object-without-types", { foo: 1 }]
];
const lexicons = [];
for (const [name, input] of lexiconInputs) {
  const lexicon = buildCategoryLexicon(input);
  const entries = [...lexicon].map(([key, entry]) => [key, entry.type, entry.query]);
  const lookups = surfaces.map(surface => [
    surface,
    lookupCategory(lexicon, surface),
    lookupCategoryFuzzy(lexicon, surface)
  ]);
  lexicons.push({ name, input, entries, lookups });
}
// The default lexicon must be the same object every time.
if (defaultCategoryLexicon() !== defaultCategoryLexicon()) throw new Error("default lexicon not cached");
write("lexicons.json", lexicons);

const artifactCases = [
  [facetRows, null],
  [facetRows, 0],
  [facetRows, 400],
  [corpusTypes, null],
  [[...OSM_CANONICAL_TYPES].map((value, index) => ({ value, n: index })), 100],
  [[{ value: "spa", n: 250 }, { value: "spa", n: 1 }, { value: "x1", n: "300" }, "cafe", null], null],
  [[], null]
];
write("artifacts.json", artifactCases.map(([input, minCount]) => [
  input,
  minCount,
  minCount == null ? buildCategoryLexiconArtifact(input) : buildCategoryLexiconArtifact(input, { minCount })
]));

// ---------------------------------------------------------------- edits

const editWords = [...new Set([...baseWords, ...surfaces])].map(fold).filter(Boolean);
const edits = [];
for (let index = 0; index < 4000; index++) {
  const left = pick(editWords);
  const right = chance(0.6) ? pick(typos(left).concat([left])) || left : pick(editWords);
  edits.push([left, right, withinOneEdit(left, right)]);
}
for (const [left, right] of [["", ""], ["", "a"], ["a", ""], ["ab", "ba"], ["abc", "acb"], ["abcd", "abdc"],
  ["abcd", "badc"], ["😀a", "a😀"], ["😀", "😁"], ["😀", ""], ["cafe", "cafes"], ["cinma", "cinema"]]) {
  edits.push([left, right, withinOneEdit(left, right)]);
}
write("edits.json", edits);

// ---------------------------------------------------------------- constraints

const phraseSurfaces = [
  "wheelchair accessible", "wheelchair-accessible", "Wheelchair-Accessible toilets", "wheelchair accessible toilet",
  "toilettes accessibles en fauteuil roulant", "toilette accessible en fauteuil roulant", "accessible to wheelchairs",
  "accessible en fauteuil roulant", "with contactless", "contactless payment", "with contactless payment", "tap",
  "tap payment", "TAP", "paiement sans contact", "with delivery", "delivery available", "avec livraison",
  "takeaway", "with takeaway", "takeout", "pour emporter", "drive-through", "drive through", "service au volant",
  "outdoor seating", "patio", "avec terrasse", "wifi", "wi-fi", "with internet", "Internet", "takes reservations",
  "accept reservation", "accepts reservations", "free", "free admission", "free entry", "no fee", "open now",
  "OPEN NOW", "currently open", "ouvert maintenant", "ouverte maintenant", "ouverts maintenant",
  "ouvertes maintenant", "ſtakeout", "taKeout", "patios", "freedom", "tapas", "wifi6", "_wifi", "wifi_",
  "openNow", "open  now", "open\tnow", "open-now", "wheelchair_accessible", "drive–through"
];
const subjects = ["pharmacy", "cafe", "café", "restaurant", "boulangerie", "bar", "", "parc", "museum Laval",
  "pizza", "Tim Hortons", "épicerie", "with", "and", "WITH", "with and", "and with"];
const joiners = [" ", "  ", " with ", " and ", ", ", " - ", "\t", " AND ", " ", " With "];
const constraintQueries = [
  "", "wheelchair-accessible pharmacy open now with contactless", "with wifi cafe", "cafe with", "cafe and",
  "with cafe", "and cafe", " with  cafe  and ", "with", "and", "with and", "within", "andrew", "cafe with  ",
  "WITH cafe AND", "café avec terrasse ouvert maintenant", "free free free", "freewifi", "wifi-free",
  "tap tap", "drive-through-takeout", " open now ", "﻿patio﻿", "\u0085patio\u0085"
];
for (let index = 0; index < 900; index++) {
  const parts = [];
  const count = 1 + Math.floor(random() * 4);
  for (let part = 0; part < count; part++) {
    parts.push(chance(0.55) ? pick(phraseSurfaces) : pick(subjects));
  }
  let text = parts[0];
  for (let part = 1; part < parts.length; part++) text += pick(joiners) + parts[part];
  if (chance(0.2)) text = ` ${text} `;
  if (chance(0.1)) text = text.toUpperCase();
  constraintQueries.push(text);
}
const explicitPool = [
  null, undefined, {}, [], "wheelchair", 5, { wheelchair: true }, { wheelchair: false }, { openNow: 0 },
  { openNow: "yes", free: null }, { bogus: true, takeaway: 1 }, { internet: true, wheelchair: true, openNow: true },
  { driveThrough: "", outdoorSeating: [] }, { openNow: false, contactless: false }
];
const parseCases = constraintQueries.map((query, index) => {
  const explicit = explicitPool[index % explicitPool.length];
  return [query, explicit ?? null, parseOsmConstraints(query, explicit)];
});
write("constraints_parse.json", parseCases);

const facetSets = [
  [], ["wheelchair", "payment_contactless"], ["wheelchair", "toilets_wheelchair", "payment_contactless", "delivery",
    "takeaway", "drive_through", "outdoor_seating", "internet_access", "reservation", "fee"], ["fee", "openNow"]
];
const compileCases = [];
for (const [, , parsed] of parseCases) {
  for (const facets of facetSets) {
    if (chance(0.25)) compileCases.push([parsed.constraints, facets, compileOsmConstraintFilters(parsed.constraints, new Set(facets))]);
  }
}
write("constraints_compile.json", compileCases);

// ---------------------------------------------------------------- opening hours

const dayParts = ["", "Mo ", "Mo-Fr ", "Sa-Mo ", "Mo,We,Fr ", "Mo-We,Fr-Su ", "Su ", "Fr-Mo ", "Tu-Tu ", "Mo,Mo ",
  "mo-fr ", "Mo-Fr  ", "Mo-Fr\t", "Mo-Fr ", "Mo-", "Mo-Fr,", "Mo-Xx ", "Mo-Tu-We ", "Mo, Tu "];
const bodyParts = ["09:00-17:00", "22:00-02:00", "00:00-24:00", "10:00-12:00,13:00-18:00", "08:00-18:00+",
  "25:00-26:00", "9:00-17:00", "09:00-17", "24:00-24:00", "00:00-00:00", "12:00-12:00", "10:99-11:00",
  "off", "closed", "OFF", "Closed", "open", "OPEN", "24/7", "09:00-17:00 , 18:00-20:00", "09:00–17:00",
  "123:00-14:00", "07:30-23:59", "18:00-06:00", "00:00-01:00", "23:00-24:00", "09:00-17:00, ", "garbage", ""];
const validDays = ["", "Mo ", "Mo-Fr ", "Sa-Mo ", "Mo,We,Fr ", "Mo-We,Fr-Su ", "Su ", "Fr-Mo ", "Tu-Tu ", "Sa,Su "];
const validBodies = ["09:00-17:00", "22:00-02:00", "00:00-24:00", "10:00-12:00,13:00-18:00", "08:00-18:00+",
  "off", "closed", "open", "24/7", "18:00-06:00", "07:30-23:59", "06:00-22:00", "11:00-14:00, 17:00-23:00", "00:00-00:00"];
const extraRules = ["PH off", "SH off", "ph 10:00-12:00", "Mo-Fr sunrise-sunset", "week 1-10 Mo 09:00-10:00",
  "Jan-Mar Mo-Fr 09:00-17:00", "Dec 25 off", "2026 Mo-Fr 09:00-17:00", "dawn-dusk", "easter off",
  "\"by appointment\"", "Mo-Fr 09:00-17:00 \"call first\"", "\"a\" Mo 10:00-11:00 \"b\"", "ſH off",
  "weeK 1", "PHONE 10:00-11:00", "MAYBE", "Mo-Fr 09:00-17:00 \"unclosed"];
const openingValues = [
  null, "", "   ", "24/7", " 24/7 ", "24/7;", "Mo-Fr 08:00-18:00; Sa off", "Mo-Fr 09:00-17:00; PH off",
  "Mo-Su 00:00-24:00", "Mo-Fr 22:00-02:00", ";", ";;", "Mo-Fr", "garbage", 0, 123, true, ["Mo-Fr 09:00-17:00"],
  { a: 1 }, "Mo-Fr 09:00-17:00;Sa 10:00-14:00;Su off", "open", "off", "closed"
];
for (let index = 0; index < 500; index++) {
  const count = 1 + Math.floor(random() * 3);
  const rules = [];
  for (let rule = 0; rule < count; rule++) {
    const body = chance(0.6) ? pick(validBodies) : pick(bodyParts);
    const days = chance(0.6) ? pick(validDays) : pick(dayParts);
    rules.push(chance(0.12) ? pick(extraRules) : `${days}${body}`);
  }
  openingValues.push(rules.join(pick(["; ", ";", " ; "])));
}

// Africa/Casablanca is left out on purpose: its rules changed in recent tz
// releases, so the system tz database and Node's bundled ICU data disagree.
const zones = [undefined, "UTC", "utc", "Etc/UTC", "GMT", "America/Toronto", "america/new_york", "Europe/Paris",
  "Australia/Sydney", "Australia/Lord_Howe", "Asia/Kolkata", "America/St_Johns", "Pacific/Chatham",
  "Asia/Kathmandu", "America/Sao_Paulo", "Pacific/Apia", "Europe/Dublin", "Etc/GMT+5",
  "Etc/GMT-14", "+05:30", "-0330", "+14", "-12:00", "EST", "US/Eastern", "America/Montreal", "Asia/Tokyo",
  "Invalid/Zone", "Z", "UTC+5", "", "posixrules", "+24:00", "Etc/GMT+13", 5];
const instants = [
  Date.UTC(2026, 7, 3, 15, 0), Date.UTC(2026, 7, 4, 1, 0), 0, -1, 1, 86_399_999, Date.UTC(2026, 2, 8, 6, 59),
  Date.UTC(2026, 2, 8, 7, 0), Date.UTC(2026, 10, 1, 5, 59), Date.UTC(2026, 10, 1, 6, 0), Date.UTC(2026, 2, 29, 0, 59),
  Date.UTC(2026, 2, 29, 1, 0), Date.UTC(2026, 9, 25, 0, 59), Date.UTC(2026, 9, 25, 1, 0), Date.UTC(2026, 3, 4, 15, 59),
  Date.UTC(2026, 3, 4, 16, 0), Date.UTC(2026, 9, 3, 15, 59), Date.UTC(2026, 9, 3, 16, 0), Date.UTC(2037, 11, 31, 23, 59),
  Date.UTC(2038, 0, 19, 3, 14, 8), Date.UTC(2040, 6, 1, 12, 0), Date.UTC(2100, 2, 14, 7, 30), Date.UTC(2999, 11, 31, 23, 59),
  Date.UTC(1999, 11, 31, 23, 59, 59, 999), Date.UTC(2000, 1, 29, 12, 0), Date.UTC(1980, 5, 15, 3, 3),
  Date.UTC(1971, 0, 1, 0, 0)
];
for (let index = 0; index < 220; index++) {
  instants.push(Math.floor(Date.UTC(1972, 0, 1) + random() * (Date.UTC(2090, 0, 1) - Date.UTC(1972, 0, 1))));
}

// `zonedParts` from opening_hours.js (not exported), verbatim.
const DAY_INDEX = new Map([["Su", 0], ["Mo", 1], ["Tu", 2], ["We", 3], ["Th", 4], ["Fr", 5], ["Sa", 6]]);
function zonedParts(at, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone, weekday: "short", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit",
    minute: "2-digit", hourCycle: "h23"
  });
  const parts = Object.fromEntries(formatter.formatToParts(at).map(part => [part.type, part.value]));
  const weekday = DAY_INDEX.get(parts.weekday?.slice(0, 2));
  return { weekday, minutes: Number(parts.hour) * 60 + Number(parts.minute), date: `${parts.year}-${parts.month}-${parts.day}` };
}
const zoneCases = [];
const extremeInstants = [-8.64e15, 8.64e15, -62198755200000, -62135596800001, 253402300800000, -2208988800000,
  Date.UTC(1850, 0, 1), Date.UTC(1900, 5, 1), Date.UTC(1960, 0, 1)];
for (const timeZone of zones) {
  for (const at of [...instants, ...extremeInstants]) {
    let parts;
    try {
      parts = zonedParts(new Date(at), timeZone);
    } catch {
      parts = null;
    }
    zoneCases.push([timeZone ?? null, at, parts]);
  }
}
write("zones.json", zoneCases);

const dateInputs = [
  "2026-08-03T15:00:00Z", "2026-08-03T15:00:00", "2026-08-03", "2026-08", "2026", "+002026-08-03T15:00:00Z",
  "-000001-01-01T00:00:00Z", "-000000-01-01T00:00:00Z", "2026-02-30", "2026-02-29", "2026-02-31T00:00:00Z",
  "2026-13-01", "2026-08-03T24:00:00Z", "2026-08-03T24:00:01Z", "2026-08-03T15:00Z", "2026-08-03T15:00:00.5Z",
  "2026-08-03T15:00:00.123456Z", "2026-08-03T15:00:00+05:30", "2026-08-03T15:00:00+0530", "2026-08-03 15:00:00Z",
  "2026-08-03t15:00:00z", "2026-08-03T15Z", "garbage", "", " 2026-08-03T15:00:00Z", "2026-08-03T15:00:00.Z",
  "2026-08-03T15:60:00Z", "2026-08-03T15:00:60Z", "2026-03-08T02:30:00", "2026-11-01T01:30:00", "1000",
  "20260803", "2026-08-03T15:00:00-00:00", "2026-08-03T15:00:00+24:00", "2026-08-03T15:00:00+23:59",
  "2026-02-32", "2026-04-31", "2026-00-10", "2026-08-00", "2026-08-03T24:00", "2026-08-03T24:00:00.000Z",
  "2026-08-03T24:00:00.001Z", "2026-08-03T15:00:00+05", "2026-08-03T15:00:00+5:30", "0000-01-01", "+000000-01-01",
  "2026-08-03T15:00:00Z ", "2026-08-03T", "2026-08-03T15", "2026-08-03T15:00:00.", "2026-08-03T1:00",
  "2026-08-03T15:00:00+05:3", "-002026-01-01T00:00:00Z", "+275760-09-13T00:00:00Z", "+275760-09-13T00:00:00.001Z",
  "-271821-04-20T00:00:00Z", "-271821-04-19T23:59:59Z", "2026-08-03T15:00:00GMT", "2026-08-03T15:00:00 Z",
  "2026-08-03T15:00:00+05:30Z", "2026-08-03Z", "2026-08-03T15:00:00UTC", "2026-08-03T15:00:00.000+05:30",
  "2026T15:00Z", "2026-08T15:00Z", "2026-08-03T15:00:00-05:00", "1970-01-01T00:00:00.000Z", "2026-08-03T15:00:00.5",
  "2026-08-03T25:00", "2026-08-03T15:00:00+23:60", "2026-07-01 12:00", "2026-01-15T08:00", "1960-06-01T00:00",
  0, 1, -1, 1.9, -1.9, 8.64e15, -8.64e15, 8.64e15 + 1, 1785769200000, 1e20, true, false, [1000],
  ["2026-08-03T15:00:00Z"], [], {}, "NaN"
];
write("dates.json", dateInputs.map(input => [input, num(new Date(input).getTime())]));

const openingCases = [];
const evaluate = (value, options) => {
  try {
    return evaluateOpeningHours(value, options);
  } catch (error) {
    return { error: error.message };
  }
};
for (const value of openingValues) {
  const count = 6;
  for (let index = 0; index < count; index++) {
    const at = chance(0.85) ? pick(instants) : pick(dateInputs.filter(item => typeof item === "string"));
    const timeZone = chance(0.6) ? pick(["UTC", "America/Toronto", "Europe/Paris", "Asia/Kolkata", "+05:30"]) : pick(zones);
    const options = { at };
    if (timeZone !== undefined) options.timeZone = timeZone;
    if (chance(0.25)) options.requireComplete = pick([false, true, 0, null, "no"]);
    openingCases.push([value, options, evaluate(value, options)]);
  }
}
write("opening_hours.json", openingCases);

// ---------------------------------------------------------------- constraint evaluation

const detailPool = [
  undefined, null, {}, "string-details", { opening_hours: "24/7" }, { opening_hours: "Mo-Fr 09:00-17:00; PH off" },
  { opening_hours: "Mo-Su off", wheelchair: "yes" }, { wheelchair: "Designated", payment_contactless: "YES" },
  { wheelchair: "no;yes", toilets_wheelchair: " yes ; no " }, { delivery: "only", takeaway: "yes", drive_through: 1 },
  { internet_access: "WLAN", outdoor_seating: "yes", reservation: "Recommended", fee: "no" },
  { fee: "yes", internet_access: "no", opening_hours: "Mo-Fr 22:00-02:00" }, { fee: false, opening_hours: 7 },
  { wheelchair: ["yes"], opening_hours: ["24/7"] }, { opening_hours: "Mo-Fr 08:00-18:00; Sa off" },
  { opening_hours: "Mo-Su 00:00-24:00", wheelchair: "yes", payment_contactless: "yes", internet_access: "wlan" },
  { opening_hours: "Mo-Su 06:00-23:00; PH off", takeaway: "only", delivery: "yes", fee: "no" }
];
const evalCases = [];
for (let index = 0; index < 1200; index++) {
  const details = pick(detailPool);
  const result = { id: `r${index}`, name: "Place", ...(details === undefined ? {} : { details }) };
  if (chance(0.1)) result.openNow = "stale";
  const constraints = pick(parseCases)[2].constraints;
  const options = {};
  if (chance(0.9)) options.at = pick(instants);
  else if (chance(0.5)) options.at = pick(["garbage", "2026-08-03T15:00:00Z", "2026-08-03T10:00"]);
  else options.at = 1785769200000;
  if (chance(0.8)) options.timeZone = pick(["UTC", "America/Toronto", "Asia/Tokyo", "Bad/Zone"]);
  if (chance(0.3)) options.includeUnknownOpenNow = pick([true, false, 1, "true"]);
  if (chance(0.3)) options.requireCompleteOpeningHours = pick([true, false, 0]);
  let evaluation;
  let annotated;
  try {
    evaluation = evaluateOsmConstraints(result, constraints, options);
    annotated = annotateConstraintResult(result, evaluation);
  } catch (error) {
    evaluation = { error: error.message };
    annotated = null;
  }
  evalCases.push([result, constraints, options, evaluation, annotated]);
}
write("constraints_eval.json", evalCases);

// ---------------------------------------------------------------- schema

const schemaOptions = [
  {}, { workerCount: 4 }, { workerCount: "3.7" }, { workerCount: 0 }, { workerCount: -5 }, { workerCount: "abc" },
  { workerCount: "Infinity" }, { rqa: true }, { rqa: 1, additionalSources: [null, { source: "X" }, 0, "y"] },
  { additionalSources: "nope", rqa: true }, { additionalSources: [] }, { meta: { generator: "test", source: "Mine" } },
  { meta: "ab" }, { input: "in.jsonl", output: "out" }, { input: "", output: 0 }, { region: "us" }, { region: "ca" },
  { buildProgressLogMs: 0 }, { buildProgressLogMs: null }, { buildProgressLogMs: "250" }, { buildProgressLogMs: "x" },
  { overrides: { fields: [], output: "override", extra: 1 } }, { region: "us", overrides: { postingGzipLevel: 9 } },
  { overrides: "zz" }
];
write("schema.json", schemaOptions.map(options => [options, createOsmIndexConfig(options)]));

// ---------------------------------------------------------------- Maps adapter

const resultPool = [
  { id: "node/42", name: "Cafe Range", type: "cafe", lat: 45.5, lon: -73.6, address: "42 Range Street, Montreal",
    distanceMeters: 123, details: { opening_hours: "24/7", wheelchair: "yes", phone: "+1-555-0100", website: "https://example.test" },
    openNow: true, openingHoursState: "open", score: 3.5 },
  { id: "postal/CA/J7A1V6", name: "J7A 1V6, Rosemère", type: "postal_code", postcode: "J7A 1V6", source: "GeoNames",
    lat: 45.64, lon: -73.7971 },
  { osm_type: "way", osm_id: 77, title: "Titled", types: ["restaurant", "bar"], category: ["food", "bar"],
    lat: "45.1", lng: "-73.2", score: "2", distanceMeters: 50 },
  { osm_type: "", osm_id: 5, house_number: 12, street: "Rue Principale", city: "Laval", state: "QC", country: "CA",
    category: "amenity", type: "fuel", score: 9, distanceMeters: null },
  { id: 0, name: "", title: 0, lat: null, lon: 1, details: "text", geometry: { type: "Point" }, openNow: null },
  { id: "", lat: 45, lon: -73, name: "No id", type: "gas_station", distanceMeters: 5, score: 1 },
  { name: "Anon", lat: 45.2, lon: -73.3, type: "movie_theater", types: ["cinema", 7, null, ""], score: 4 },
  { id: "cafe/1", name: "Cafe", type: "cafe", lat: 45.5, lon: -73.6, distanceMeters: 90, score: 1 },
  { id: "cafe/1", name: "Cafe better", type: "cafe", lat: 45.5, lon: -73.6, distanceMeters: 30, score: 5 },
  { id: "fuel/1", name: "Fuel", type: "fuel", lat: 45.5, lon: -73.6, distanceMeters: 40 },
  { id: "restaurant/1", name: "Restaurant", type: "restaurant", lat: 45.5, lon: -73.6, distanceMeters: 20, score: 2,
    routeDistanceMeters: 25, rejoinPoint: { lat: 45.5, lon: -73.6 }, constraintMatches: { openNow: true },
    locationType: "ROOFTOP", reverseGeocodeAccuracy: 3, shard: "qc" },
  { id: "x/2", formattedAddress: "Formatted 1", address: "ignored", details: { opening_hours: "" }, openNow: false,
    openingHoursState: "", source: 0, osm_id: 0 },
  { id: 12.5, name: "Num id", type: "park", category: [], lat: 1e-7, lon: 123456789012, score: -1, distanceMeters: "7" },
  { id: "q/1", name: "Shopping", types: ["shopping_mall", "tourist_attraction"], lat: 10, lon: 20, distanceMeters: 15 }
];
const suggestionPool = [
  { text: "Range Cafe, Montreal", mainText: "Range Cafe", secondaryText: "Montreal", types: ["cafe"],
    matchedRanges: [{ start: 0, end: 5 }], selection: { query: "Range Cafe", shards: ["quebec"] } },
  { text: "Laval", description: "Laval, QC", types: ["city"] },
  { description: "", text: "", types: ["coffee_shop"] },
  { types: ["movie_theater", "gas_station"] },
  { text: "Bare" },
  { text: "Mall", mainText: "", secondaryText: null, types: ["shopping_mall"], matchedRanges: null, selection: null }
];
const locations = [
  { latitude: 45.5, longitude: -73.6 }, { lat: 45.5, lng: -73.6 }, { lat: "45.5", lon: "-73.6" },
  { lat: null, lng: 3 }, { latitude: null, lat: 4, longitude: undefined, lng: 5 }, { lat: "x", lng: 1 }, {}, "str"
];
const biases = [
  undefined, null, { circle: { center: pick(locations), radius: 5000 } }, { circle: { center: { latitude: 1, longitude: 2 } } },
  { center: { lat: 3, lng: 4 }, radius: 10 }, { circle: { radius: 5 } }, { rectangle: { low: { latitude: 1, longitude: 2 }, high: { latitude: 3, longitude: 5 } } },
  { rectangle: { north: 46, south: 45, east: -73, west: -74 } }, { north: "46", south: 45, east: -73, west: -74 },
  { rectangle: { maxLat: 2, minLat: 1, maxLon: 4, minLon: 3 } }, { rectangle: { north: 1 } }, { low: { lat: 9, lng: 8 }, high: { lat: 1, lng: 2 } },
  { rectangle: { low: { lat: "bad" }, high: { lat: 1, lng: 1 } } }, "bias", 0
];
const restrictions = [
  undefined, { circle: { center: { latitude: 45.5, longitude: -73.6 }, radius: 1500 } },
  { circle: { center: { lat: 45.5, lng: -73.6 }, radiusMeters: "200" } }, { circle: { center: { lat: 1, lng: 2 }, radius: 0 } },
  { center: { lat: 1, lng: 2 }, radius: -4 }, { rectangle: { north: 46, south: 45, east: -73, west: -74 } },
  { low: { latitude: 3, longitude: 2 }, high: { latitude: 1, longitude: 4 } }, { rectangle: {} }, { circle: {} }
];
const queryPool = [undefined, null, "", "  cafe Montreal ", "Range", 42, "pharmacy open now", "épicerie"];
const typePool = [[], ["cafe"], ["cafe", "restaurant"], ["coffee_shop", "", null, "gas_station"], ["movie_theater"],
  [" shopping_mall "], ["tourist_attraction", "park_and_ride"], ["fuel", "cafe"]];
const defaultsPool = [undefined, {}, { timeZone: "America/Toronto" }, { near: { lat: 1, lon: 2 }, trace: true },
  { trace: false, timeZone: "" }, { near: null, trace: null }];

function randomRequest(method) {
  const request = {};
  const maybe = (key, values, p = 0.5) => {
    if (chance(p)) {
      const value = pick(values);
      if (value !== undefined) request[key] = value;
    }
  };
  switch (method) {
    case "autocomplete":
      maybe("input", queryPool, 0.7);
      maybe("query", queryPool, 0.3);
      maybe("inputOffset", [9, 0, null], 0.3);
      maybe("maxResultCount", [3, "4", 100, 0, null, "abc", 2.7], 0.4);
      maybe("size", [5, 60], 0.2);
      maybe("locationBias", biases, 0.5);
      maybe("shards", [["quebec"], [], null], 0.2);
      maybe("trace", [true, false, null], 0.2);
      maybe("includedPrimaryTypes", typePool, 0.4);
      break;
    case "textSearch":
      maybe("textQuery", queryPool, 0.7);
      maybe("query", queryPool, 0.3);
      maybe("includedType", ["cafe", "gas_station", "", null, " movie_theater "], 0.3);
      maybe("maxResultCount", [3, 100, "7"], 0.3);
      maybe("pageSize", [5, -1], 0.2);
      maybe("locationBias", biases, 0.4);
      maybe("locationRestriction", restrictions, 0.3);
      maybe("openNow", [true, false, "true"], 0.4);
      maybe("constraints", [{ wheelchair: true }, { openNow: false }, null, "ab", [true]], 0.3);
      maybe("timeZone", ["UTC", "", null], 0.3);
      maybe("at", [1785769200000, "2026-08-03T15:00:00Z", 0, ""], 0.3);
      maybe("shards", [["a"], null], 0.2);
      maybe("trace", [true, null], 0.2);
      break;
    case "nearbySearch":
      if (chance(0.6)) request.locationRestriction = pick(restrictions.slice(1, 3));
      else maybe("locationRestriction", restrictions, 0.9);
      maybe("includedTypes", typePool, 0.7);
      maybe("excludedTypes", typePool, 0.4);
      maybe("maxResultCount", [1, 2, 5, 40, "3"], 0.5);
      maybe("rankPreference", ["DISTANCE", "POPULARITY", "popularity"], 0.6);
      maybe("openNow", [true], 0.3);
      maybe("constraints", [{ free: true }], 0.2);
      maybe("timeZone", ["UTC"], 0.2);
      maybe("trace", [true], 0.2);
      break;
    case "geocode":
      maybe("address", queryPool, 0.8);
      maybe("query", queryPool, 0.3);
      maybe("maxResultCount", [1, 30, "x"], 0.3);
      maybe("locationBias", biases, 0.4);
      maybe("trace", [true], 0.2);
      break;
    case "reverseGeocode":
      if (chance(0.6)) request.location = pick(locations);
      else Object.assign(request, pick(locations) instanceof Object ? pick(locations) : {});
      maybe("radiusMeters", [100, null], 0.3);
      maybe("maxResultCount", [2, 50], 0.3);
      maybe("size", [3], 0.2);
      maybe("resultTypes", [["street_address"], null], 0.3);
      maybe("locationTypes", [["ROOFTOP"]], 0.2);
      maybe("localityRadiusMeters", [5000], 0.2);
      maybe("shards", [["qc"]], 0.2);
      maybe("trace", [true], 0.2);
      break;
    case "searchAlongRoute":
      maybe("route", ["encoded-route", { type: "LineString", coordinates: [[0, 0], [1, 1]] }, "", null], 0.9);
      maybe("query", queryPool, 0.5);
      maybe("textQuery", queryPool, 0.3);
      maybe("corridorMeters", [1200, null], 0.4);
      maybe("polylinePrecision", [5, 6], 0.2);
      maybe("routePositionMeters", [100], 0.2);
      maybe("routeDirection", ["forward"], 0.2);
      maybe("viewport", [{ minLat: 1 }], 0.2);
      maybe("openNow", [true, 1], 0.4);
      maybe("constraints", [{ takeaway: true }], 0.2);
      maybe("timeZone", ["Europe/Paris", ""], 0.3);
      maybe("at", ["2026-08-03T15:00:00Z", null], 0.3);
      maybe("maxResultCount", [3, 99], 0.3);
      maybe("limit", [7], 0.2);
      maybe("shards", [["a"]], 0.2);
      maybe("trace", [false], 0.2);
      break;
    case "placeDetails":
      maybe("placeId", ["node/42", "cafe/1", "way/77", "missing", "", null, 12.5, "node/42", "cafe/1", "restaurant/1", "fuel/1"], 0.8);
      maybe("id", ["restaurant/1", "q/1"], 0.3);
      break;
    default:
      break;
  }
  return request;
}

function randomResponse(kind) {
  if (kind === "suggest") {
    const count = Math.floor(random() * 4);
    const response = { suggestions: Array.from({ length: count }, () => pick(suggestionPool)) };
    if (chance(0.5)) response.normalizedQuery = pick(["range", "", null]);
    if (chance(0.5)) response.stats = pick([{ lane: "suggest" }, null]);
    return response;
  }
  const count = Math.floor(random() * 5);
  const response = { results: Array.from({ length: count }, () => pick(resultPool)) };
  if (chance(0.6)) response.total = pick([count, 100, null, "7"]);
  if (chance(0.4)) response.approximate = pick([true, false, "true"]);
  if (chance(0.3)) response.correctedQuery = pick(["cafe", ""]);
  if (chance(0.5)) response.stats = pick([{ plannerLane: "test" }, null, {}]);
  if (chance(0.05)) delete response.results;
  return response;
}

const methods = ["autocomplete", "textSearch", "nearbySearch", "geocode", "reverseGeocode", "searchAlongRoute", "placeDetails"];
const kindFor = { search: "searchOsmQuery", suggest: "suggestOsmQuery", reverse: "reverseGeocodeOsm", route: "searchAlongRouteOsm" };

async function runSession(defaults, ops) {
  const queues = { search: [], suggest: [], reverse: [], route: [] };
  let calls = [];
  const options = {};
  if (defaults !== undefined) options.defaults = defaults;
  for (const [kind, name] of Object.entries(kindFor)) {
    options[name] = async (_engine, params) => {
      calls.push([kind, JSON.parse(JSON.stringify(params))]);
      return queues[kind].shift() ?? { results: [] };
    };
  }
  const adapter = createRangefindMapsAdapter({ manifest: {} }, options);
  const outputs = [];
  for (const op of ops) {
    calls = [];
    for (const [kind, list] of Object.entries(op.responses)) queues[kind] = list.map(item => JSON.parse(JSON.stringify(item)));
    let output;
    try {
      output = { result: JSON.parse(JSON.stringify(await adapter[op.method](op.request))) };
    } catch (error) {
      output = { error: error.message };
    }
    if (op.clearAfter) adapter.clearPlaceCache();
    outputs.push({ ...output, calls });
  }
  return outputs;
}

const sessions = [];
// The JS unit-test scenarios.
const testResult = { id: "node/1", name: "Range Cafe", type: "cafe", lat: 45.51, lon: -73.61, distanceMeters: 80,
  details: { opening_hours: "24/7" }, openNow: true };
sessions.push({
  defaults: { timeZone: "America/Toronto" },
  ops: [
    { method: "textSearch", request: { textQuery: "cafe Montreal", openNow: true,
      locationBias: { circle: { center: { latitude: 45.5, longitude: -73.6 }, radius: 5000 } } },
    responses: { search: [{ total: 1, results: [testResult], stats: { plannerLane: "test" } }] } },
    { method: "autocomplete", request: { input: "range caf", inputOffset: 9 }, responses: { suggest: [{ suggestions: [suggestionPool[0]] }] } },
    { method: "reverseGeocode", request: { location: { latitude: 45.5, longitude: -73.6 }, resultTypes: ["street_address"] },
      responses: { reverse: [{ total: 1, results: [testResult] }] } },
    { method: "searchAlongRoute", request: { route: "encoded-route", query: "cafe", corridorMeters: 1200, openNow: true },
      responses: { route: [{ total: 1, results: [{ ...testResult, routeDistanceMeters: 25, rejoinPoint: { lat: 45.5, lon: -73.6 } }] }] } },
    { method: "placeDetails", request: { placeId: "node/1" }, responses: {} }
  ]
});
sessions.push({
  defaults: undefined,
  ops: [{
    method: "nearbySearch",
    request: { includedTypes: ["cafe", "restaurant"], excludedTypes: ["gas_station"], maxResultCount: 5, rankPreference: "DISTANCE",
      locationRestriction: { circle: { center: { latitude: 45.5, longitude: -73.6 }, radius: 1500 } } },
    responses: { search: [
      { results: [resultPool[7], resultPool[9]] },
      { results: [resultPool[10], resultPool[7], resultPool[8]] }
    ] }
  }]
});
for (let index = 0; index < 220; index++) {
  const ops = [];
  const count = 1 + Math.floor(random() * 5);
  for (let op = 0; op < count; op++) {
    const method = op > 0 && chance(0.3) ? "placeDetails" : pick(methods);
    const responses = {
      search: Array.from({ length: 4 }, () => randomResponse("search")),
      suggest: [randomResponse("suggest")],
      reverse: [randomResponse("reverse")],
      route: [randomResponse("route")]
    };
    ops.push({ method, request: randomRequest(method), responses, clearAfter: chance(0.1) });
  }
  sessions.push({ defaults: pick(defaultsPool), ops });
}
for (const session of sessions) session.outputs = await runSession(session.defaults, session.ops);
write("adapter_sessions.json", sessions.map(session => ({ ...session, defaults: session.defaults ?? null })));

const places = resultPool.map(result => [result, JSON.parse(JSON.stringify(toMigrationPlace(result)))]);
for (let index = 0; index < 150; index++) {
  const result = {};
  for (const source of [pick(resultPool), pick(resultPool)]) {
    for (const [key, value] of Object.entries(source)) if (chance(0.5)) result[key] = value;
  }
  places.push([result, JSON.parse(JSON.stringify(toMigrationPlace(result)))]);
}
write("places.json", places);
