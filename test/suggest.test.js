import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import {
  autocompleteKeysForValue,
  encodeAuthorityHotList,
  encodeAuthorityLexiconRoot,
  encodeAuthorityLexiconRootChunks,
  encodeAuthorityLexiconSegment,
  parseAuthorityHotList,
  parseAuthorityLexiconRoot,
  parseAuthorityLexiconSegment,
  suggestKey
} from "../src/authority_lexicon.js";

test("suggest keys fold diacritics and punctuation but keep every script", () => {
  assert.equal(suggestKey("Montréal"), "montreal");
  assert.equal(suggestKey("Saint-Denis"), "saint denis");
  assert.equal(suggestKey("  L'Île-des-Sœurs  "), "l ile des soeurs");
  assert.equal(suggestKey("Tokyo 東京"), "tokyo 東京");
  assert.equal(suggestKey("---"), "");
});

test("authority autocomplete keys preserve display and add token suffixes", () => {
  const keys = autocompleteKeysForValue("Café de la Gare");
  assert.deepEqual(keys.map(item => item.normalized), ["cafe de la gare", "de la gare", "la gare", "gare"]);
  assert.ok(keys.every(item => item.display === "Café de la Gare"));
  assert.deepEqual(
    autocompleteKeysForValue("Gare Centrale", { tokenPrefixes: false }).map(item => item.normalized),
    ["gare centrale"]
  );
});

test("authority lexicon shard summaries and hot lists round trip", () => {
  const shards = [
    { shard: "s|a", maxRank: 9, count: 20 },
    { shard: "s|b", maxRank: 7, count: 12 }
  ];
  const hotItems = [
    { normalized: "alpha", display: "Alpha", weight: 9, count: 2 },
    { normalized: "azure", display: "Azure", weight: 5, count: 1 }
  ];
  const hot = new Map([["a", { file: "authority/hot/abc.bin.gz", bytes: 42, count: 2 }]]);
  const decoded = parseAuthorityLexiconRoot(encodeAuthorityLexiconRoot({ shards, hot, weighted: true }));
  assert.equal(decoded.weighted, true);
  assert.deepEqual(decoded.shards, shards);
  assert.deepEqual(decoded.hot.get("a"), hot.get("a"));
  assert.deepEqual(parseAuthorityHotList(encodeAuthorityHotList(hotItems)), hotItems);
});

test("streamed authority lexicon root matches the in-memory codec", async () => {
  const shards = [
    { shard: "s|alpha", maxRank: 19, count: 40 },
    { shard: "s|alpine", maxRank: 17, count: 31 },
    { shard: "s|beta", maxRank: 11, count: 22 }
  ];
  const hot = new Map([
    ["a", { file: "authority/hot/a.bin.gz", bytes: 42, count: 2 }],
    ["b", { file: "authority/hot/b.bin.gz", bytes: 31, count: 1 }]
  ]);
  const chunks = [];
  for await (const chunk of encodeAuthorityLexiconRootChunks({
    shards: (async function* () { yield* shards; })(),
    shardCount: shards.length,
    hot,
    weighted: true
  })) chunks.push(chunk);
  assert.deepEqual(
    Buffer.concat(chunks),
    encodeAuthorityLexiconRoot({ shards, hot, weighted: true })
  );
});

test("paged authority lexicon segments preserve direct range pointers", () => {
  const entries = [
    {
      shard: "s|alpha",
      maxRank: 19,
      count: 40,
      packIndex: 1,
      offset: 120,
      length: 42,
      logicalLength: 180,
      checksum: { algorithm: "sha256", value: "a".repeat(64) }
    },
    {
      shard: "s|alpine",
      maxRank: 17,
      count: 31,
      packIndex: 2,
      offset: 500,
      length: 54,
      logicalLength: 210,
      checksum: { algorithm: "sha256", value: "b".repeat(64) }
    }
  ];
  const parsed = parseAuthorityLexiconSegment(
    encodeAuthorityLexiconSegment(entries),
    { packTable: ["zero.bin", "one.bin", "two.bin"] }
  );
  assert.equal(parsed.format, "rflexicon-segment-v1");
  assert.equal(parsed.first, "s|alpha");
  assert.equal(parsed.last, "s|alpine");
  assert.deepEqual(parsed.entries, entries.map(entry => ({
    shard: entry.shard,
    maxRank: entry.maxRank,
    count: entry.count,
    entry: {
      pack: entry.packIndex === 1 ? "one.bin" : "two.bin",
      offset: entry.offset,
      length: entry.length,
      physicalLength: entry.length,
      logicalLength: entry.logicalLength,
      checksum: entry.checksum
    }
  })));
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
  // Cross the adaptive multi-letter hot-list threshold so "mo" exercises
  // the national-scale constant-cost lane rather than the normal small-shard
  // fallback used by rare prefixes.
  for (let i = 0; i < 300; i++) {
    docs.push({
      id: `mountain-${i}`,
      title: `Mountain Feature ${String(i).padStart(3, "0")}`,
      body: "broad prefix filler",
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
    const display = String(doc.title || "").trim().replace(/\s+/gu, " ");
    const normalized = suggestKey(display);
    const keys = [normalized];
    let start = normalized.indexOf(" ");
    while (start !== -1 && keys.length < 4) {
      const key = normalized.slice(start + 1);
      if (key.length > 1 && !keys.includes(key)) keys.push(key);
      start = normalized.indexOf(" ", start + 1);
    }
    for (const key of keys) {
      const mapKey = `${key}\0${display}`;
      const previous = rows.get(mapKey) || { key, display, weight: 0, count: 0, full: key === normalized };
      previous.count++;
      previous.weight = Math.max(previous.weight, Number(doc.population) || 0);
      rows.set(mapKey, previous);
    }
  }
  const entries = [...rows.values()];
  for (const entry of entries) {
    if (!entry.weight) entry.weight = entry.count;
    entry.rank = entry.weight * 2 + (entry.full ? 1 : 0);
  }
  const prefix = suggestKey(q);
  const best = new Map();
  for (const entry of entries) {
    if (!entry.key.startsWith(prefix)) continue;
    const existing = best.get(entry.display);
    if (!existing
      || entry.rank > existing.rank
      || (entry.rank === existing.rank && entry.display.length < existing.display.length)
      || (entry.rank === existing.rank && entry.display.length === existing.display.length && entry.key < existing.key)) {
      best.set(entry.display, entry);
    }
  }
  return [...best.values()]
    .sort((a, b) => b.rank - a.rank
      || a.display.length - b.display.length
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
  assert.equal(manifest.suggest, undefined);
  assert.ok(manifest.authority.autocomplete.directory.file.startsWith("authority/"));
  assert.equal(manifest.object_store.pack_table.suggest, undefined);
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
    const mo = await engine.suggest({ q: "mo", size: 3 });
    assert.equal(mo.stats.suggestLane, "authority-hot");
    assert.equal(mo.stats.suggestShardsVisited, 0);
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

test("unified authority autocomplete agrees with an exhaustive oracle", async () => {
  await runSuggestOracleSuite({}, manifest => {
    assert.ok(manifest.authority.autocomplete.shards > 0);
  });
});

test("unified autocomplete remains exact across recursively split authority shards", async () => {
  await runSuggestOracleSuite({ authorityTargetShardRows: 8 }, manifest => {
    assert.ok(manifest.authority.autocomplete.shards > 20);
  });
});

test("legacy title authority provides bounded autocomplete without explicit suggest fields", async () => {
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
    await assert.rejects(engine.suggest({ q: "pla" }), /no authority autocomplete lexicon/);
  } finally {
    await server.close();
  }
});
