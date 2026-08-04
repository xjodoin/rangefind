import assert from "node:assert/strict";
import test from "node:test";
import { DEFAULT_ANALYZER } from "../src/analysis.js";

const parse = query => DEFAULT_ANALYZER.queryPlan(query).baseTerms;

/**
 * Query parsing is where a user's typing meets the index, and it is the one
 * place where being wrong is silent: a query that tokenizes badly returns
 * nothing at all, and nothing looks exactly like "no such place".
 *
 * The cases below are written so their expectations are true by construction
 * rather than by transcription. Most are equivalences — two spellings a
 * person would consider the same thing, which must therefore reach the same
 * terms — and the rest are invariants that hold whatever the analyzer does
 * internally. Only the handful with settled, checked answers assert exact
 * output, so stemming can evolve without this file becoming a liability.
 */

// --- Equivalences: different typing, same intent -------------------------

const EQUIVALENT = [
  ["case: street", "Maurice Cullen", "maurice cullen"],
  ["case: shouting", "MAURICE CULLEN", "maurice cullen"],
  ["case: mixed", "MaUrIcE cUlLeN", "maurice cullen"],
  ["hyphen: surname", "Maurice-Cullen", "Maurice Cullen"],
  ["hyphen: saint", "Ste-Foy", "Ste Foy"],
  ["hyphen: multi", "rue-de-la-paix", "rue de la paix"],
  ["hyphen: leading", "-Laval", "Laval"],
  ["hyphen: trailing", "Laval-", "Laval"],
  ["hyphen: doubled", "Maurice--Cullen", "Maurice Cullen"],
  ["em dash", "Maurice—Cullen", "Maurice Cullen"],
  ["en dash", "Maurice–Cullen", "Maurice Cullen"],
  ["accent: e acute", "café", "cafe"],
  ["accent: uppercase", "CAFÉ", "cafe"],
  ["accent: city", "Montréal", "Montreal"],
  ["accent: circumflex", "hôpital", "hopital"],
  ["accent: cedilla", "façade", "facade"],
  ["accent: grave", "où", "ou"],
  ["accent: diaeresis", "Noël", "Noel"],
  // Composed vs decomposed: e-acute as one code point and as "e" plus a
  // combining accent must not become two different terms.
  ["accent: composed vs decomposed", "caf\u00e9", "cafe\u0301"],
  ["elision: apostrophe", "l'église", "église"],
  ["elision: typographic", "l’église", "l'église"],
  ["elision: d'", "d'Orléans", "Orléans"],
  ["apostrophe: irish", "O'Brien", "Brien"],
  ["comma separators", "Laval, QC", "Laval QC"],
  ["semicolon separators", "Laval; QC", "Laval QC"],
  ["slash separators", "Laval/QC", "Laval QC"],
  ["pipe separators", "Laval|QC", "Laval QC"],
  ["period separators", "St. Denis", "St Denis"],
  ["colon separators", "Laval: QC", "Laval QC"],
  ["whitespace: leading", "   Laval", "Laval"],
  ["whitespace: trailing", "Laval   ", "Laval"],
  ["whitespace: internal", "Maurice     Cullen", "Maurice Cullen"],
  ["whitespace: tab", "Maurice\tCullen", "Maurice Cullen"],
  ["whitespace: newline", "Maurice\nCullen", "Maurice Cullen"],
  ["whitespace: non-breaking", "Maurice Cullen", "Maurice Cullen"],
  ["quotes: double", '"Maurice Cullen"', "Maurice Cullen"],
  ["quotes: single", "'Maurice Cullen'", "Maurice Cullen"],
  ["quotes: curly", "“Maurice Cullen”", "Maurice Cullen"],
  ["parentheses", "(Maurice Cullen)", "Maurice Cullen"],
  ["brackets", "[Maurice Cullen]", "Maurice Cullen"],
  ["exclamation", "Laval!", "Laval"],
  ["question mark", "Laval?", "Laval"],
  ["ampersand spacing", "Tim & Tom", "Tim Tom"],
  ["emoji prefix", "🚗 taxi", "taxi"],
  ["emoji suffix", "taxi 🚗", "taxi"],
  ["emoji only padding", "🚗🚕 taxi 🚙", "taxi"],
  ["postal: spacing kept apart", "H7C 2T8", "H7C  2T8"],
  ["postal: lowercase", "h7c 2t8", "H7C 2T8"],
  ["address: comma noise", "5505 Rue Maurice-Cullen, Laval", "5505 Rue Maurice Cullen Laval"],
  ["zero width space", "Mau\u200brice", "Mau rice"],
  ["soft hyphen", "Mau\u00adrice Cullen", "Mau rice Cullen"]
];

for (const [label, left, right] of EQUIVALENT) {
  test(`query parsing equivalence — ${label}`, () => {
    assert.deepEqual(
      parse(left),
      parse(right),
      `${JSON.stringify(left)} and ${JSON.stringify(right)} should reach the same terms`
    );
  });
}

// --- Settled answers ------------------------------------------------------

const EXACT = [
  ["plain address", "5505 Rue Maurice-Cullen, Laval, QC H7C 2T8",
    ["5505", "rue", "maurice", "cullen", "laval", "qc", "h7c", "2t8"]],
  ["house number survives", "5505 Rue Maurice-Cullen", ["5505", "rue", "maurice", "cullen"]],
  ["postal split on space", "H7C 2T8", ["h7c", "2t8"]],
  ["postal joined stays joined", "H7C2T8", ["h7c2t8"]],
  ["accent folded", "Montréal", ["montreal"]],
  ["elision dropped", "l'église", ["eglise"]],
  ["cjk kept", "东京", ["东京"]],
  ["digits alone", "5505", ["5505"]],
  ["single letter dropped", "a", []],
  ["empty string", "", []],
  ["only spaces", "   ", []],
  ["only punctuation", "!!!", []],
  ["only emoji", "🚗", []],
  ["only symbols", "@#$%^&*", []]
];

for (const [label, query, expected] of EXACT) {
  test(`query parsing exact — ${label}`, () => {
    assert.deepEqual(parse(query), expected);
  });
}

// --- Invariants over hostile input ---------------------------------------

const HOSTILE = [
  "", " ", "\t", "\n", "\r\n", "\u00a0", "\u200b",
  "!", "?", ".", ",", ";", ":", "-", "--", "/", "\\", "|", "@", "#", "$", "%",
  "()", "[]", "{}", "<>", '""', "''", "«»",
  "a", "ab", "0", "00", "007",
  "5505 Rue Maurice-Cullen, Laval, QC H7C 2T8",
  "  5505   Rue   Maurice-Cullen  ",
  "MAURICE-CULLEN!!!",
  "café frappé à l'îlot",
  "Ste-Anne-de-Bellevue",
  "Saint-Jean-sur-Richelieu, QC J3B 6X3",
  "東京都渋谷区",
  "Москва, Тверская улица",
  "القاهرة",
  "עברית",
  "🚗🚕🚙🚌🚎",
  "taxi 🚗 laval",
  "a".repeat(500),
  "x ".repeat(300),
  "5505".repeat(50),
  "<script>alert(1)</script>",
  "'; DROP TABLE places; --",
  "../../etc/passwd",
  "%00%01%02",
  "\u0000\u0001\u0002",
  "\u00a0\u00a0",
  "\ufffd",
  "\uD83D",
  "e\u0301\u0301\u0301",
  "\u1160\u1160\u1160",
  "1/2 rue de la Paix",
  "n°5 rue de la Paix",
  "#5 Main St",
  "Apt. 3B, 5505 Maurice-Cullen",
  "Unit 12 - 5505 Maurice Cullen",
  "5505-5545 Rue Maurice-Cullen",
  "45.57,-73.75",
  "45.57 -73.75",
  "-73.75",
  "+1 514 555 0199",
  "www.example.com",
  "https://example.com/a?b=c",
  "user@example.com"
];

test("query parsing never throws, whatever it is handed", () => {
  for (const query of HOSTILE) {
    assert.doesNotThrow(() => parse(query), `threw on ${JSON.stringify(query)}`);
  }
});

test("parsed terms are never empty, padded, or upper case", () => {
  for (const query of HOSTILE) {
    for (const term of parse(query)) {
      assert.ok(term.length > 0, `empty term from ${JSON.stringify(query)}`);
      assert.equal(term, term.trim(), `padded term ${JSON.stringify(term)}`);
      assert.ok(!/\s/u.test(term), `whitespace inside term ${JSON.stringify(term)}`);
      assert.equal(term, term.toLowerCase(), `upper case term ${JSON.stringify(term)}`);
      // Control characters would survive into the term dictionary and never
      // match anything a builder wrote.
      assert.ok(
        !/[\u0000-\u001f\u007f]/u.test(term),
        `control char in ${JSON.stringify(term)}`
      );
    }
  }
});

test("parsing is idempotent: re-parsing the terms yields the same terms", () => {
  for (const query of HOSTILE) {
    const once = parse(query);
    if (!once.length) continue;
    // Terms already analyzed must survive a second pass unchanged, or a
    // suggestion resolved back into a query would drift away from the
    // postings it was drawn from.
    assert.deepEqual(parse(once.join(" ")), once, `not idempotent for ${JSON.stringify(query)}`);
  }
});

test("adding noise never removes a term that was already found", () => {
  // Guards the failure this file was written for: extra tokens in a pasted
  // address must add to the query, never quietly subtract from it.
  const base = "5505 Rue Maurice-Cullen";
  const found = new Set(parse(base));
  for (const suffix of [", Laval", ", Laval, QC", ", Laval, QC H7C 2T8", " apt 3", " #12"]) {
    const widened = new Set(parse(base + suffix));
    for (const term of found) {
      assert.ok(widened.has(term), `${JSON.stringify(suffix)} dropped ${JSON.stringify(term)}`);
    }
  }
});

test("the query-parsing table covers at least 100 cases", () => {
  const cases = EQUIVALENT.length + EXACT.length + HOSTILE.length;
  assert.ok(cases >= 100, `expected at least 100 cases, found ${cases}`);
});
