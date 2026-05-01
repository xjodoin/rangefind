import assert from "node:assert/strict";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gunzipSync } from "node:zlib";
import { findDirectoryPage, parseDirectoryPage, parseDirectoryRoot } from "../src/directory.js";
import {
  addTypoIndexTerm,
  addTypoSurfacePairs,
  boundedDamerauLevenshtein,
  createTypoRunBuffer,
  parseTypoShard,
  reduceTypoRuns,
  surfacePairsForFields,
  typoCandidateScore,
  typoCandidatesForDeleteKey,
  typoDeleteKeys,
  typoOptions
} from "../src/typo.js";

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
  const manifest = await reduceTypoRuns(buffer, root);
  assert.equal(manifest.packs.length, 1);
  const rootBytes = gunzipSync(await readFile(join(root, "typo", "directory-root.bin.gz")));
  const directoryRoot = parseDirectoryRoot(rootBytes);
  const pageMeta = findDirectoryPage(directoryRoot, "st");
  assert.ok(pageMeta);
  const pageBytes = gunzipSync(await readFile(join(root, "typo", "directory-pages", pageMeta.file)));
  const directoryPage = parseDirectoryPage(pageBytes);
  const range = directoryPage.get("st");
  assert.ok(range);
  const pack = manifest.packs.find(item => item.file === range.pack);
  assert.ok(pack);
  const bytes = await readFile(join(root, "typo", "packs", pack.file));
  const compressed = bytes.subarray(range.offset, range.offset + range.length);
  const shard = parseTypoShard(gunzipSync(compressed));
  const candidates = typoCandidatesForDeleteKey(shard, "stati");
  assert.ok(candidates.some(candidate => candidate.surface === "static" && candidate.term === "static"));
});
