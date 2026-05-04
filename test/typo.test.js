import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gunzipSync } from "node:zlib";
import { findDirectoryPage, parseDirectoryPage, parseDirectoryRoot } from "../src/directory.js";
import {
  addTypoIndexTerm,
  addTypoSurfacePairs,
  addTypoSurfacePairsToBuffer,
  boundedDamerauLevenshtein,
  createTypoSurfacePairBuffer,
  createTypoRunBuffer,
  flushTypoSurfacePairBuffer,
  parseTypoShard,
  reduceTypoRuns,
  surfacePairsForFields,
  typoCandidateScore,
  typoCandidatesForDeleteKey,
  typoDeleteKeys,
  typoOptions
} from "../src/typo.js";
import { parseTypoLexiconShard, typoLexiconShardKey } from "../src/typo_lexicon.js";

test("delete keys support single-edit surface correction", () => {
  assert.ok(typoDeleteKeys("static", typoOptions({})).has("stati"));
  assert.equal(boundedDamerauLevenshtein("statik", "static", 1), 1);
  assert.ok(typoCandidateScore("statik", "static", 20, 1) > 0);
});

test("typo sidecar builds and parses packed delete-key shards", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-typo-"));
  const options = { ...typoOptions({}), baseShardDepth: 2, maxShardDepth: 2, targetShardCandidates: 100, packBytes: 65536 };
  const buffer = createTypoRunBuffer(join(root, "_runs"), options);
  const doc = { title: "Static range search" };
  const fields = [{ path: "title" }];
  addTypoSurfacePairs(buffer, surfacePairsForFields(doc, fields, (item, field) => item[field.path]));
  addTypoIndexTerm(buffer, "static", 10, 100);
  addTypoIndexTerm(buffer, "changement", 4, 100);
  const manifest = await reduceTypoRuns(buffer, root);
  assert.equal(manifest.packs.length, 1);
  assert.match(manifest.manifest, /^typo\/manifest\.[0-9a-f]{24}\.json$/u);
  assert.ok(await readFile(join(root, manifest.manifest), "utf8"));
  assert.match(manifest.directory.root, /^typo\/directory-root\.[0-9a-f]{24}\.bin\.gz$/u);
  assert.match(manifest.packs[0].file, /^0000\.[0-9a-f]{24}\.bin$/u);
  const rootBytes = gunzipSync(await readFile(join(root, manifest.directory.root)));
  const directoryRoot = parseDirectoryRoot(rootBytes);
  const pageMeta = findDirectoryPage(directoryRoot, "st");
  assert.ok(pageMeta);
  const pageBytes = gunzipSync(await readFile(join(root, "typo", "directory-pages", pageMeta.file)));
  const directoryPage = parseDirectoryPage(pageBytes, { packTable: manifest.directory.pack_table });
  const range = directoryPage.get("st");
  assert.ok(range);
  assert.equal(range.checksum.algorithm, "sha256");
  assert.equal(range.checksum.value.length, 64);
  const pack = manifest.packs.find(item => item.file === range.pack);
  assert.ok(pack);
  const bytes = await readFile(join(root, "typo", "packs", pack.file));
  const compressed = bytes.subarray(range.offset, range.offset + range.length);
  const shard = parseTypoShard(gunzipSync(compressed));
  const candidates = typoCandidatesForDeleteKey(shard, "stati");
  assert.ok(candidates.some(candidate => candidate.surface === "static" && candidate.term === "static"));
  assert.equal(manifest.lexicon.format, "rftermlex-v2");
  assert.ok(manifest.stats.lexicon_entries > 0);
  assert.ok(manifest.lexicon.stats.trie_nodes > 0);
  const lexiconRootBytes = gunzipSync(await readFile(join(root, manifest.lexicon.directory.root)));
  const lexiconRoot = parseDirectoryRoot(lexiconRootBytes);
  const lexiconShard = typoLexiconShardKey("changement", manifest.lexicon.shard_depth);
  const lexiconPageMeta = findDirectoryPage(lexiconRoot, lexiconShard);
  assert.ok(lexiconPageMeta);
  const lexiconPageBytes = gunzipSync(await readFile(join(root, "typo", "lexicon", "directory-pages", lexiconPageMeta.file)));
  const lexiconPage = parseDirectoryPage(lexiconPageBytes, { packTable: manifest.lexicon.directory.pack_table });
  const lexiconRange = lexiconPage.get(lexiconShard);
  assert.ok(lexiconRange);
  const lexiconBytes = await readFile(join(root, "typo", "lexicon-packs", lexiconRange.pack));
  const lexiconShardData = parseTypoLexiconShard(gunzipSync(lexiconBytes.subarray(lexiconRange.offset, lexiconRange.offset + lexiconRange.length)));
  assert.ok(lexiconShardData.entries.some(entry => entry.surface === "changement" && entry.term === "changement"));
  assert.ok(lexiconShardData.trie.children.size > 0);
});

test("typo surface pairs can be deduplicated across a segment flush", () => {
  const options = { ...typoOptions({}), flushLines: 1000 };
  const root = mkdtempSync(join(tmpdir(), "rangefind-typo-dedupe-"));
  const buffer = createTypoRunBuffer(join(root, "_runs"), options);
  const pairBuffer = createTypoSurfacePairBuffer();
  const doc = { title: "Static range search" };
  const fields = [{ path: "title" }];
  const pairs = surfacePairsForFields(doc, fields, (item, field) => item[field.path]);

  addTypoSurfacePairsToBuffer(pairBuffer, pairs);
  addTypoSurfacePairsToBuffer(pairBuffer, surfacePairsForFields(doc, fields, (item, field) => item[field.path]));
  flushTypoSurfacePairBuffer(buffer, pairBuffer);

  assert.equal(buffer.terms, 3);
  assert.equal(pairBuffer.size, 0);
  assert.ok(buffer.deletePairs > buffer.terms);
});
