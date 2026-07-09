import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import {
  addSuggestionRow,
  compareSuggestions,
  decodeSuggestBranchPage,
  decodeSuggestPage,
  encodeSuggestBranchPage,
  encodeSuggestPage,
  encodeSuggestRoot,
  finalizeSuggestionRows,
  parseSuggestRoot,
  suggestKey
} from "../src/suggest_index.js";

test("suggest keys fold diacritics and punctuation but keep every script", () => {
  assert.equal(suggestKey("Montréal"), "montreal");
  assert.equal(suggestKey("Saint-Denis"), "saint denis");
  assert.equal(suggestKey("  L'Île-des-Sœurs  "), "l ile des soeurs");
  assert.equal(suggestKey("Tokyo 東京"), "tokyo 東京");
  assert.equal(suggestKey("---"), "");
});

test("suggestion rows aggregate counts, weights, and token prefixes", () => {
  const rows = new Map();
  addSuggestionRow(rows, "Café de la Gare", 0, {});
  addSuggestionRow(rows, "Café de la Gare", 0, {});
  addSuggestionRow(rows, "Café de la Gare", 7, { count: 3 });
  addSuggestionRow(rows, "Gare Centrale", 0, { tokenPrefixes: false });
  const entries = finalizeSuggestionRows(rows);
  const byKey = Object.fromEntries(entries.map(entry => [`${entry.key}|${entry.display}`, entry]));
  const full = byKey["cafe de la gare|Café de la Gare"];
  assert.equal(full.count, 5);
  assert.equal(full.weight, 7);
  // Token-prefix keys make mid-label tokens completable.
  assert.ok(byKey["gare|Café de la Gare"]);
  assert.ok(byKey["de la gare|Café de la Gare"]);
  // tokenPrefixes: false emits only the full key.
  assert.ok(byKey["gare centrale|Gare Centrale"]);
  assert.equal(byKey["centrale|Gare Centrale"], undefined);
});

test("suggest pages, branch pages, and roots round trip", () => {
  const rows = new Map();
  for (let i = 0; i < 300; i++) {
    addSuggestionRow(rows, `Place ${String(i).padStart(3, "0")} unique`, i % 7, {});
  }
  const entries = finalizeSuggestionRows(rows);
  assert.deepEqual(entries, entries.slice().sort(compareSuggestions));
  const encoded = encodeSuggestPage(entries);
  const decoded = decodeSuggestPage(encoded);
  assert.equal(decoded.count, entries.length);
  assert.deepEqual(
    decoded.entries,
    entries.map(({ key, display, weight, count }) => ({ key, display, weight, count }))
  );

  const packTable = ["0000.abc.bin"];
  const packIndexes = new Map([["0000.abc.bin", 0]]);
  const entry = {
    pack: "0000.abc.bin",
    offset: 100,
    length: 50,
    physicalLength: 50,
    logicalLength: 90,
    checksum: { algorithm: "sha256", value: "ab".repeat(32) }
  };
  const pages = Array.from({ length: 10 }, (_, i) => ({
    minKey: `key ${String(i).padStart(2, "0")}`,
    maxWeight: 10 - i,
    count: 30,
    entry
  }));
  const flatRoot = parseSuggestRoot(encodeSuggestRoot({
    total: 300,
    pageSize: 30,
    pages,
    branches: null,
    packTable,
    packIndexes
  }).buffer);
  assert.equal(flatRoot.levels, 1);
  assert.equal(flatRoot.pages.length, 10);
  assert.equal(flatRoot.pages[3].minKey, "key 03");
  assert.equal(flatRoot.pages[3].maxWeight, 7);
  assert.equal(flatRoot.pages[3].offset, 100);

  const branchPageBuffer = encodeSuggestBranchPage({
    branchIndex: 1,
    firstPageIndex: 5,
    pages: pages.slice(5),
    packIndexes
  });
  const branchPage = decodeSuggestBranchPage(branchPageBuffer, packTable);
  assert.equal(branchPage.firstPageIndex, 5);
  assert.equal(branchPage.pages[0].index, 5);
  assert.equal(branchPage.pages[0].minKey, "key 05");

  const branches = [
    { minKey: "key 00", maxWeight: 10, count: 150, firstPageIndex: 0, pageCount: 5, entry },
    { minKey: "key 05", maxWeight: 5, count: 150, firstPageIndex: 5, pageCount: 5, entry }
  ];
  const branchRoot = parseSuggestRoot(encodeSuggestRoot({
    total: 300,
    pageSize: 30,
    pages: null,
    branches,
    packTable,
    packIndexes
  }).buffer);
  assert.equal(branchRoot.levels, 2);
  assert.equal(branchRoot.branches.length, 2);
  assert.equal(branchRoot.branches[1].firstPageIndex, 5);
  assert.equal(branchRoot.branches[1].maxWeight, 5);
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

function suggestFixtureDocs() {
  const docs = [];
  const chains = ["Tim Hortons", "Subway", "Café Dépôt"];
  for (let i = 0; i < 90; i++) {
    docs.push({
      id: `chain-${i}`,
      title: chains[i % 3],
      body: "chain location",
      population: 0
    });
  }
  const places = [
    ["Montréal", 1700000],
    ["Montreal West", 5100],
    ["Mont-Tremblant", 9600],
    ["Sainte-Anne-de-Bellevue", 5000],
    ["Saint-Denis-sur-Richelieu", 2400],
    ["Boulevard Saint-Laurent", 0],
    ["Rue Saint-Denis", 0],
    ["Tour Eiffel Restaurant", 0]
  ];
  for (const [name, population] of places) {
    docs.push({ id: name, title: name, body: "place", population });
  }
  for (let i = 0; i < 400; i++) {
    docs.push({
      id: `filler-${i}`,
      title: `Zone industrielle ${String(i).padStart(3, "0")}`,
      body: "filler",
      population: 0
    });
  }
  // Many distinct displays sharing one token key ("centre"), so the shared
  // key spans several pages and consecutive page minKeys are equal —
  // regression coverage for the candidate-run lower bound.
  for (let i = 0; i < 80; i++) {
    docs.push({
      id: `centre-${i}`,
      title: `Dépanneur ${String(i).padStart(2, "0")} Centre`,
      body: "shared token",
      population: i === 41 ? 999 : 0
    });
  }
  return docs;
}

// Independent oracle: aggregate + expand exactly like the builder, then
// compute top-k by full scan, deduped by display.
function oracleSuggest(docs, q, size) {
  const rows = new Map();
  for (const doc of docs) {
    addSuggestionRow(rows, doc.title, Number(doc.population) || 0, {});
  }
  const entries = finalizeSuggestionRows(rows);
  const prefix = suggestKey(q);
  const best = new Map();
  for (const entry of entries) {
    if (!entry.key.startsWith(prefix)) continue;
    const existing = best.get(entry.display);
    if (!existing
      || entry.weight > existing.weight
      || (entry.weight === existing.weight && entry.key < existing.key)) {
      best.set(entry.display, entry);
    }
  }
  return [...best.values()]
    .sort((a, b) => b.weight - a.weight
      || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0)
      || (a.display < b.display ? -1 : a.display > b.display ? 1 : 0))
    .slice(0, size)
    .map(entry => ({ text: entry.display, weight: entry.weight, count: entry.count }));
}

async function runSuggestOracleSuite(configOverrides, assertManifest) {
  const root = await mkdtemp(join(tmpdir(), "rangefind-suggest-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  const docs = suggestFixtureDocs();
  await writeFile(docsPath, docs.map(doc => JSON.stringify(doc)).join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    suggest: [{ path: "title", weightPath: "population" }],
    display: ["title"],
    ...configOverrides
  }));
  await build({ configPath });
  const manifest = JSON.parse(await readFile(join(output, "manifest.min.json"), "utf8"));
  assert.equal(manifest.features.suggest, true);
  assert.ok(manifest.suggest.directory.file.startsWith("suggest/"));
  if (assertManifest) assertManifest(manifest);

  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });

    for (const q of ["m", "mont", "montr", "MONTRÉ", "montreal w", "saint", "s", "tim", "eiffel", "zone industrielle 0", "xyz-no-match", "centre", "cent", "depanneur 4"]) {
      const response = await engine.suggest({ q, size: 6 });
      const expected = oracleSuggest(docs, q, 6);
      assert.deepEqual(
        response.suggestions,
        expected,
        `suggestions for ${JSON.stringify(q)} diverged from the oracle`
      );
      assert.equal(response.stats.exact, true);
    }

    // Weighted places outrank fillers; popularity ranks the chains.
    const mont = await engine.suggest({ q: "mont", size: 3 });
    assert.equal(mont.suggestions[0].text, "Montréal");
    const chains = await engine.suggest({ q: "t", size: 2 });
    assert.ok(chains.suggestions.some(item => item.text === "Tim Hortons"));
    const tim = await engine.suggest({ q: "tim", size: 2 });
    assert.equal(tim.suggestions[0].count, 30);

    // Diacritic-insensitive both ways.
    const folded = await engine.suggest({ q: "cafe dep", size: 2 });
    assert.equal(folded.suggestions[0]?.text, "Café Dépôt");

    // Empty and unmatched queries return cleanly.
    const empty = await engine.suggest({ q: "   " });
    assert.deepEqual(empty.suggestions, []);
  } finally {
    await server.close();
  }
}

test("suggestions agree with an exhaustive oracle (single-level root)", async () => {
  await runSuggestOracleSuite({ suggestPageSize: 32 }, manifest => {
    assert.equal(manifest.suggest.levels, 1);
  });
});

test("suggestions agree with an exhaustive oracle (branch-paged root)", async () => {
  await runSuggestOracleSuite({ suggestPageSize: 16, suggestBranchPages: 4 }, manifest => {
    assert.equal(manifest.suggest.levels, 2);
    assert.ok(manifest.suggest.branches >= 2);
  });
});

test("title authority provides bounded autocomplete without a suggestion sidecar", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-authority-suggest-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  const titles = [
    "Artificial intelligence",
    "Artificial Intelligence Act",
    "Artificial Intelligence: A Modern Approach",
    "Artificial intelligence and copyright",
    "Café Alpha",
    "Cafe Beta"
  ];
  await writeFile(docsPath, titles.map((title, id) => JSON.stringify({ id, title, body: title })).join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    fields: [{ name: "title", path: "title", weight: 4.5, b: 0.55 }],
    authority: [{ name: "title", path: "title" }],
    authorityBaseShardDepth: 3,
    authorityMaxShardDepth: 8,
    authorityTargetShardRows: 1,
    display: ["title"]
  }));
  await build({ configPath });
  const manifest = JSON.parse(await readFile(join(output, "manifest.min.json"), "utf8"));
  assert.equal(manifest.features.suggest, false);
  assert.ok(manifest.authority.shards > 1);
  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });
    const response = await engine.suggest({ q: "artificial int", size: 3 });
    assert.deepEqual(response.suggestions.map(item => item.text), [
      "Artificial intelligence",
      "Artificial Intelligence: A Modern Approach",
      "Artificial Intelligence Act"
    ]);
    assert.equal(response.stats.exact, true);
    assert.equal(response.stats.suggestLane, "authority-title");
    assert.ok(response.stats.suggestShardsVisited > 0);

    const folded = await engine.suggest({ q: "cafe", size: 2 });
    assert.deepEqual(folded.suggestions.map(item => item.text), ["Café Alpha", "Cafe Beta"]);
  } finally {
    await server.close();
  }
});

test("indexes without suggest fields reject suggest queries clearly", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-nosuggest-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, JSON.stringify({ id: "a", title: "Plain doc", body: "text" }));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    fields: [{ name: "title", path: "title", weight: 4.5, b: 0.55 }],
    display: ["title"]
  }));
  await build({ configPath });
  const manifest = JSON.parse(await readFile(join(root, "public", "rangefind", "manifest.min.json"), "utf8"));
  assert.equal(manifest.features.suggest, false);
  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });
    await assert.rejects(engine.suggest({ q: "pla" }), /no suggestion sidecar/);
  } finally {
    await server.close();
  }
});
