import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { collectScoringStats, loadScoringStats, openDfFile } from "../src/scoring_stats.js";
import { readConfig } from "../src/config.js";

const TOPICS = ["glacier", "harbor", "meadow", "quarry", "lagoon"];

function makeDoc(i, latBase = 40) {
  const topic = TOPICS[i % TOPICS.length];
  return {
    id: `doc-${i}`,
    title: `Entry ${i} about ${topic}`,
    body: `${topic} study number ${i} common shared corpus text`,
    lat: latBase + (i % 10) / 10,
    lon: -70 + (i % 10) / 10
  };
}

async function writeConfigAt(root, overrides = {}) {
  const config = {
    input: "docs.jsonl",
    output: "public/rangefind",
    targetPostingsPerDoc: 32,
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
    display: ["title"],
    ...overrides
  };
  const path = join(root, "rangefind.config.json");
  await writeFile(path, JSON.stringify(config));
  return readConfig(path);
}

test("collectScoringStats sums totals and counts df across inputs", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-stats-"));
  const docsA = Array.from({ length: 40 }, (_, i) => makeDoc(i, 40));
  const docsB = Array.from({ length: 60 }, (_, i) => makeDoc(40 + i, 50));
  await writeFile(join(root, "a.jsonl"), docsA.map(doc => JSON.stringify(doc)).join("\n"));
  await writeFile(join(root, "b.jsonl"), docsB.map(doc => JSON.stringify(doc)).join("\n"));
  const config = await writeConfigAt(root);

  const { statsPath, dfPath, stats } = await collectScoringStats({
    config,
    inputs: [
      { id: "a", input: join(root, "a.jsonl") },
      { id: "b", input: join(root, "b.jsonl") }
    ],
    outDir: join(root, "stats")
  });

  assert.equal(stats.total, 100);
  assert.equal(stats.inputs.length, 2);
  assert.equal(stats.inputs[0].total, 40);
  assert.equal(stats.inputs[1].total, 60);
  // Shard A spans lat 40.0-40.9, shard B 50.0-50.9.
  assert.ok(Math.abs(stats.inputs[0].bbox[0] - 40) < 1e-9);
  assert.ok(stats.inputs[1].bbox[0] >= 50);

  const loaded = loadScoringStats(statsPath);
  assert.equal(loaded.total, 100);
  assert.ok(loaded.dfPath.endsWith("scoring-stats-df.bin"));

  const reader = openDfFile(dfPath);
  // Every doc contains "common" (body), so df must be the corpus total.
  assert.equal(reader.lookup("common"), 100);
  // "glacier" appears in title+body of every 5th doc.
  assert.equal(reader.lookup("glacier"), 20);
  assert.equal(reader.lookup("no-such-term"), undefined);
  reader.close();
});

test("collectScoringStats reads gzip-compressed JSONL inputs", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-stats-gzip-"));
  const docs = Array.from({ length: 25 }, (_, i) => makeDoc(i));
  const input = join(root, "docs.jsonl.gz");
  await writeFile(input, gzipSync(`${docs.map(doc => JSON.stringify(doc)).join("\n")}\n`));
  const config = await writeConfigAt(root);

  const { stats } = await collectScoringStats({
    config,
    inputs: [{ id: "gzip", input }],
    outDir: join(root, "stats")
  });

  assert.equal(stats.total, 25);
  assert.equal(stats.inputs[0].total, 25);
});

test("df accumulation survives spill-and-merge with tiny thresholds", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-stats-spill-"));
  const docs = Array.from({ length: 120 }, (_, i) => makeDoc(i));
  await writeFile(join(root, "docs.jsonl"), docs.map(doc => JSON.stringify(doc)).join("\n"));
  const config = await writeConfigAt(root);

  const spilled = await collectScoringStats({
    config,
    inputs: [{ id: "all", input: join(root, "docs.jsonl") }],
    outDir: join(root, "stats-spill"),
    spillTerms: 16,
    blockTerms: 8
  });
  const plain = await collectScoringStats({
    config,
    inputs: [{ id: "all", input: join(root, "docs.jsonl") }],
    outDir: join(root, "stats-plain")
  });

  assert.equal(spilled.stats.df_terms, plain.stats.df_terms);
  const spilledReader = openDfFile(spilled.dfPath);
  const plainReader = openDfFile(plain.dfPath);
  for (const term of ["common", "glacier", "harbor", "study", "entry", "120", "corpus"]) {
    assert.equal(spilledReader.lookup(term), plainReader.lookup(term), `df mismatch for "${term}"`);
  }
  spilledReader.close();
  plainReader.close();
});

test("worker and sequential stats passes agree", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-stats-workers-"));
  const docs = Array.from({ length: 300 }, (_, i) => makeDoc(i));
  await writeFile(join(root, "docs.jsonl"), docs.map(doc => JSON.stringify(doc)).join("\n"));
  const sequentialConfig = await writeConfigAt(root, { scanWorkers: 1 });
  const workerConfig = await writeConfigAt(root, { scanWorkers: 3 });

  const sequential = await collectScoringStats({
    config: sequentialConfig,
    inputs: [{ id: "all", input: join(root, "docs.jsonl") }],
    outDir: join(root, "stats-seq")
  });
  const parallel = await collectScoringStats({
    config: workerConfig,
    inputs: [{ id: "all", input: join(root, "docs.jsonl") }],
    outDir: join(root, "stats-par")
  });

  assert.equal(sequential.stats.total, parallel.stats.total);
  assert.deepEqual(sequential.stats.field_totals, parallel.stats.field_totals);
  assert.equal(sequential.stats.df_terms, parallel.stats.df_terms);
  assert.deepEqual(sequential.stats.inputs[0].bbox, parallel.stats.inputs[0].bbox);
});
