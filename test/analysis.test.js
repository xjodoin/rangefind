import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import {
  LEGACY_ANALYZER,
  analyzerForConfig,
  analyzerFromManifest,
  createAnalyzer,
  normalizeAnalysisConfig
} from "../src/analysis.js";
import { foldMulti } from "../src/analysis_fold.js";
import { stemmerFor } from "../src/analysis_stemmers.js";
import { analyzeTerms, fold, queryTerms, tokenize } from "../src/analyzer.js";
import { findMatchRanges, highlightTermSet } from "../src/highlight.js";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";

function multiAnalyzer(languages, extra = {}) {
  return createAnalyzer(normalizeAnalysisConfig({ languages, ...extra }));
}

test("foldMulti folds scripts deterministically", () => {
  assert.equal(foldMulti("Montréal"), "montreal");
  assert.equal(foldMulti("Straße"), "strasse");
  assert.equal(foldMulti("Œuvre"), "oeuvre");
  assert.equal(foldMulti("København"), "kobenhavn");
  assert.equal(foldMulti("ΓΛΏΣΣΑ"), "γλωσσα");
  assert.equal(foldMulti("τέλος"), "τελοσ");
  assert.equal(foldMulti("новый ёж"), "новыи еж");
  assert.equal(foldMulti("كِتَابٌ"), "كتاب");
  assert.equal(foldMulti("أسد"), "اسد");
  assert.equal(foldMulti("مصطفى"), "مصطفي");
  assert.equal(foldMulti("שָׁלוֹם"), "שלום");
  assert.equal(foldMulti("ＡＢＣ１２３"), "abc123");
  // Arabic-Indic digits must survive the mark strip.
  assert.equal(foldMulti("١٢٣"), "١٢٣");
  // foldDiacritics: false keeps accents (in decomposed form) but still
  // lowercases; both sides normalize identically so terms stay consistent.
  assert.equal(foldMulti("Montréal", { foldDiacritics: false }), "montréal");
});

test("analysis profile normalization validates and canonicalizes", () => {
  assert.equal(normalizeAnalysisConfig(null), null);
  assert.equal(normalizeAnalysisConfig(undefined), null);
  const profile = normalizeAnalysisConfig({ languages: ["FR", "en", "fr"], primary: "en" });
  assert.equal(profile.profile, "multi-v1");
  assert.deepEqual(profile.languages, ["fr", "en"]);
  assert.equal(profile.primary, "en");
  assert.equal(profile.stemming, "light");
  assert.equal(profile.stopwords, "default");
  assert.equal(profile.minLength, 2);
  // Primary outside the language list falls back to the first language.
  assert.equal(normalizeAnalysisConfig({ languages: ["de", "en"], primary: "fr" }).primary, "de");
  assert.equal(normalizeAnalysisConfig({}).languages[0], "en");
  assert.throws(() => normalizeAnalysisConfig({ languages: ["1234"] }), /ISO 639/);
  // BCP47 tags reduce to their primary subtag.
  assert.deepEqual(normalizeAnalysisConfig({ languages: ["fr-CA"] }).languages, ["fr"]);
  assert.throws(() => normalizeAnalysisConfig({ stemming: "snowball" }), /stemming/);
  assert.throws(() => normalizeAnalysisConfig({ stopwords: "custom" }), /stopwords/);
  assert.throws(() => createAnalyzer({ profile: "future-v9" }), /not supported/);
});

test("multilingual tokenization per language", () => {
  const analyzer = multiAnalyzer(["fr", "en", "de", "ru", "ja", "zh", "ar"]);
  assert.deepEqual(
    analyzer.tokenize("Les chanteuses des châteaux", { lang: "fr", unique: false }),
    ["chant", "chateau"]
  );
  assert.deepEqual(
    analyzer.tokenize("Die Häuser und Wohnungen", { lang: "de", unique: false }),
    ["haus", "wohn"]
  );
  assert.deepEqual(
    analyzer.tokenize("Новые русские книги", { lang: "ru", unique: false }),
    ["нов", "русск", "книг"]
  );
  // CJK runs become overlapping character bigrams; no dictionary involved.
  assert.deepEqual(
    analyzer.tokenize("東京タワー", { lang: "ja", unique: false }),
    ["東京", "京タ", "タワ", "ワー"]
  );
  assert.deepEqual(
    analyzer.tokenize("中文搜索", { lang: "zh", unique: false }),
    ["中文", "文搜", "搜索"]
  );
  // A single CJK character still emits a unigram.
  assert.deepEqual(analyzer.tokenize("水", { lang: "zh", unique: false }), ["水"]);
  assert.deepEqual(
    analyzer.tokenize("المكتبات العربية", { lang: "ar", unique: false }),
    ["مكتب", "عرب"]
  );
  // Mixed scripts split at script boundaries.
  assert.deepEqual(
    analyzer.tokenize("iPhone 15 発売", { lang: "en", unique: false }),
    ["iphone", "15", "発売"]
  );
  // Stopwords and minLength apply to alphabetic tokens only.
  assert.deepEqual(analyzer.tokenize("the a x 5", { lang: "en", unique: false }), ["5"]);
  // Unknown language falls back to the primary profile language.
  assert.deepEqual(
    analyzer.tokenize("chanteuses", { lang: "xx", unique: false }),
    analyzer.tokenize("chanteuses", { lang: "fr", unique: false })
  );
});

test("light stemmers merge inflected forms", () => {
  const pairs = [
    ["en", "walking", "walk"], ["en", "houses", "house"], ["en", "running", "run"],
    ["fr", "chevaux", "cheval"], ["fr", "châteaux", "chateau"],
    ["es", "canciones", "cancion"], ["es", "luces", "luz"],
    ["it", "libri", "libr"], ["it", "libro", "libr"],
    ["pt", "canções", "cancao"],
    ["nl", "boeken", "boek"], ["nl", "mannen", "man"],
    ["sv", "flickorna", "flick"],
    ["fi", "talossa", "talo"],
    ["ru", "книги", "книг"], ["ru", "книга", "книг"],
    ["el", "γλωσσα", "γλωσσ"],
    ["ar", "والكتاب", "كتاب"],
    ["hi", "लड़कों", "लड़क"]
  ];
  for (const [lang, word, expected] of pairs) {
    const stem = stemmerFor(lang);
    assert.ok(stem, `stemmer for ${lang}`);
    assert.equal(stem(foldMulti(word)), foldMulti(expected), `${lang}: ${word}`);
  }
  // Singular and plural land on the same stem.
  const de = stemmerFor("de");
  assert.equal(de(foldMulti("Häuser")), de(foldMulti("Haus")));
  const fr = stemmerFor("fr");
  assert.equal(fr(foldMulti("chanteuses")), fr(foldMulti("chanteuse")));
  // Languages without a light stemmer report null.
  assert.equal(stemmerFor("tr"), null);
  assert.equal(stemmerFor("ja"), null);
});

test("language detection by script and stopword vote", () => {
  const analyzer = multiAnalyzer(["en", "fr", "ru", "ja", "ar", "el", "hi"]);
  assert.equal(analyzer.detectLanguage("the singers are in the house with their dog"), "en");
  assert.equal(analyzer.detectLanguage("les chanteuses sont dans la maison avec le chien"), "fr");
  assert.equal(analyzer.detectLanguage("новые книги на русском языке"), "ru");
  assert.equal(analyzer.detectLanguage("東京の天気は晴れです"), "ja");
  assert.equal(analyzer.detectLanguage("المكتبة العربية الكبيرة في المدينة"), "ar");
  assert.equal(analyzer.detectLanguage("η γλωσσα των βιβλιων"), "el");
  assert.equal(analyzer.detectLanguage("यह एक हिंदी वाक्य है"), "hi");
  // Ambiguous Latin text without stopword evidence resolves to "".
  assert.equal(analyzer.detectLanguage("chocolat"), "");

  const config = { fields: [{ name: "title", path: "title" }, { name: "body", path: "body" }] };
  const withField = multiAnalyzer(["en", "fr"], { languageField: "lang" });
  assert.equal(withField.docLanguage({ lang: "fr-CA", title: "x" }, config), "fr");
  // Unconfigured explicit language falls through to detection/primary.
  assert.equal(withField.docLanguage({ lang: "de", title: "the house of the dog is here" }, config), "en");
  assert.equal(
    analyzer.docLanguage({ title: "Русская литература", body: "Большая коллекция русских книг" }, config),
    "ru"
  );
});

test("query plans keep primary sequence and union alternate stems within budget", () => {
  const analyzer = multiAnalyzer(["fr", "en"]);
  const plan = analyzer.queryPlan("chanteuses françaises");
  assert.equal(plan.language, "fr");
  assert.deepEqual(plan.baseTerms, ["chant", "francais"]);
  // Primary phrase expansion present.
  assert.ok(plan.terms.includes("chant_francais"));
  // English alternate stems join the retrieval union.
  assert.ok(plan.terms.includes("chanteuse"));
  assert.ok(plan.terms.length <= 30);

  // Long queries stay within the planner budget despite alternate lanes.
  const crowded = multiAnalyzer(["fr", "en", "de", "es", "it"]);
  const longPlan = crowded.queryPlan("grandes maisons anciennes pierres jardins fleuris");
  assert.ok(longPlan.terms.length <= 30, `terms ${longPlan.terms.length}`);
});

test("legacy analyzer instance matches analyzer.js exactly", () => {
  const samples = [
    "Learning to walk before running",
    "Les châteaux de la Loire",
    "mixed CASE Text-With—punctuation 42"
  ];
  for (const sample of samples) {
    assert.deepEqual(LEGACY_ANALYZER.tokenize(sample, { unique: false }), tokenize(sample, { unique: false }));
    assert.deepEqual(LEGACY_ANALYZER.analyzeTerms(sample), analyzeTerms(sample));
    assert.deepEqual(LEGACY_ANALYZER.queryPlan(sample).terms, queryTerms(sample));
    assert.equal(LEGACY_ANALYZER.fold(sample), fold(sample));
  }
  assert.equal(LEGACY_ANALYZER.docLanguage({ title: "anything" }, {}), "");
  // No analysis profile anywhere → legacy instance.
  assert.equal(analyzerForConfig({}), LEGACY_ANALYZER);
  assert.equal(analyzerFromManifest({}), LEGACY_ANALYZER);
  assert.ok(analyzerFromManifest({ analysis: normalizeAnalysisConfig({ languages: ["fr"] }) }).isMultilingual);
});

test("highlighting matches across languages and scripts", () => {
  const analyzer = multiAnalyzer(["fr", "en", "ja"]);
  const termSet = highlightTermSet("châteaux", "", analyzer);
  const ranges = findMatchRanges("Les Châteaux de la Loire", termSet, analyzer);
  assert.deepEqual(ranges, [[4, 12]]);

  // CJK: only the queried bigram span highlights, not the whole run.
  const jaSet = highlightTermSet("東京", "", analyzer);
  const jaRanges = findMatchRanges("私は東京に住む", jaSet, analyzer);
  assert.deepEqual(jaRanges, [[2, 4]]);

  // Legacy default still highlights stemmed English.
  const legacySet = highlightTermSet("walking");
  assert.deepEqual(findMatchRanges("I was walking home", legacySet), [[6, 13]]);
});

async function serveStatic(root) {
  const server = createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://localhost");
      const path = resolve(root, `.${decodeURIComponent(url.pathname)}`);
      if (!path.startsWith(resolve(root))) {
        response.writeHead(403).end();
        return;
      }
      const data = await readFile(path);
      const range = request.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (range) {
        const start = Number(range[1]);
        const end = Math.min(Number(range[2]), data.length - 1);
        response.writeHead(206, {
          "Accept-Ranges": "bytes",
          "Content-Length": String(end - start + 1),
          "Content-Range": `bytes ${start}-${end}/${data.length}`
        });
        response.end(data.subarray(start, end + 1));
        return;
      }
      response.writeHead(200, { "Content-Length": String(data.length) });
      response.end(data);
    } catch {
      response.writeHead(404).end();
    }
  });
  await new Promise(resolveListen => server.listen(0, "127.0.0.1", resolveListen));
  const { port } = server.address();
  return {
    baseUrl: `http://127.0.0.1:${port}/rangefind/`,
    close: () => new Promise(resolveClose => server.close(resolveClose))
  };
}

test("multilingual index is searchable end to end in every language", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-multiling-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, [
    JSON.stringify({ id: "fr1", title: "Les châteaux de la Loire", body: "Une chanteuse visite les châteaux avec les chanteuses de la troupe française.", url: "/fr1" }),
    JSON.stringify({ id: "en1", title: "Walking the old town", body: "The singer keeps walking through houses in the old town every morning.", url: "/en1" }),
    JSON.stringify({ id: "de1", title: "Das alte Haus", body: "Die Häuser und Wohnungen der Stadt sind sehr alt und schön gebaut.", url: "/de1" }),
    JSON.stringify({ id: "ru1", title: "Русские книги", body: "Новая книга о русской литературе вышла в московском издательстве недавно.", url: "/ru1" }),
    JSON.stringify({ id: "ja1", title: "東京タワーの案内", body: "東京タワーは日本の有名な観光地です。夜景がとても綺麗です。", url: "/ja1" })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    analysis: { languages: ["fr", "en", "de", "ru", "ja"] },
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    display: ["title", "url", "body"]
  }));
  await build({ configPath });

  const manifest = JSON.parse(await readFile(join(root, "public", "rangefind", "manifest.min.json"), "utf8"));
  assert.equal(manifest.analysis?.profile, "multi-v1");
  assert.deepEqual(manifest.analysis.languages, ["fr", "en", "de", "ru", "ja"]);

  const site = await serveStatic(join(root, "public"));
  t.after(() => site.close());
  const client = await createSearch({ baseUrl: site.baseUrl });
  assert.ok(client.analyzer.isMultilingual);

  async function expectTopHit(query, id) {
    const response = await client.search({ q: query, size: 3 });
    assert.ok(response.results.length, `no results for ${query}`);
    assert.equal(response.results[0].id, id, `top hit for "${query}" was ${response.results[0].id}`);
    return response;
  }

  // Same-language inflection: query forms differ from document forms.
  await expectTopHit("chanteuses", "fr1");
  await expectTopHit("château", "fr1");
  await expectTopHit("walking singer", "en1");
  await expectTopHit("Häuser", "de1");
  // Accent-less typing still matches folded terms.
  await expectTopHit("hauser wohnungen", "de1");
  await expectTopHit("книги", "ru1");
  await expectTopHit("русская литература", "ru1");
  await expectTopHit("東京タワー", "ja1");

  // Highlights carry analyzer-aware ranges (CJK bigram spans included).
  const highlighted = await client.search({ q: "東京", size: 1, highlight: true });
  assert.equal(highlighted.results[0].id, "ja1");
  const ranges = highlighted.results[0].highlights?.title?.ranges;
  assert.ok(ranges?.length, "expected CJK highlight ranges");

  // Counts run through the same query plan.
  const counted = await client.count({ q: "châteaux" });
  assert.ok(counted.total >= 1);
});

test("delta updates refuse a changed analysis profile", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-analysis-frozen-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  const baseConfig = {
    input: "docs.jsonl",
    output: "public/rangefind",
    analysis: { languages: ["fr", "en"] },
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55 },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    display: ["title", "url"]
  };
  await writeFile(docsPath, [
    JSON.stringify({ id: "a", title: "Première page", body: "Un document en français pour la génération zéro.", url: "/a" })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify(baseConfig));
  await build({ configPath });

  await writeFile(docsPath, [
    JSON.stringify({ id: "b", title: "Zweite Seite", body: "Ein neues Dokument für die Delta-Generation.", url: "/b" })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    ...baseConfig,
    analysis: { languages: ["de", "en"] }
  }));
  await assert.rejects(() => build({ configPath, update: true }), /analysis profile differs/);

  // Same profile updates cleanly.
  await writeFile(configPath, JSON.stringify(baseConfig));
  await build({ configPath, update: true });
  const rootManifest = JSON.parse(await readFile(join(root, "public", "rangefind", "manifest.min.json"), "utf8"));
  assert.ok(Array.isArray(rootManifest.generations));
  assert.equal(rootManifest.generations.length, 2);
});
