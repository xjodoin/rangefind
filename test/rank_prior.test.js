import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createNodeSearch } from "../src/runtime.node.js";

test("generic rankPrior reranks relevance ties from an embedded numeric field", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-rank-prior-"));
  const input = join(root, "docs.jsonl");
  const output = join(root, "rangefind");
  await writeFile(input, [
    { id: "minor", title: "Central Station", prominence: 0.05 },
    { id: "major", title: "Central Station", prominence: 1 }
  ].map(row => JSON.stringify(row)).join("\n") + "\n");
  await writeFile(join(root, "config.json"), JSON.stringify({
    input,
    output,
    idPath: "id",
    urlPath: "id",
    fields: [{ name: "title", path: "title", weight: 5, phrase: true }],
    alwaysIndexFields: ["title"],
    numbers: [{ name: "prominence", path: "prominence", type: "double" }],
    rankPrior: { field: "prominence", boost: 1, overfetch: 4 },
    display: ["title", "prominence"]
  }));
  await build({ configPath: join(root, "config.json") });
  const manifest = JSON.parse(await readFile(join(output, "manifest.json"), "utf8"));
  assert.deepEqual(manifest.rankPrior, { field: "prominence", boost: 1, overfetch: 4 });

  const engine = await createNodeSearch({ source: output });
  const plain = await engine.search({ q: "Central Station", size: 1, rankPriorBoost: 0 });
  assert.equal(plain.results[0].id, "minor");
  const ranked = await engine.search({ q: "Central Station", size: 1, trace: true });
  assert.equal(ranked.results[0].id, "major");
  assert.equal(ranked.stats.rankPriorBoost, true);
  assert.equal(ranked.stats.rankPriorField, "prominence");
  assert.ok(!ranked.stats.trace.spans.some(span => span.name === "docValues.fetch"));
});

test("rankPrior configuration rejects missing numeric fields", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-rank-prior-invalid-"));
  await writeFile(join(root, "config.json"), JSON.stringify({
    input: "docs.jsonl",
    output: "rangefind",
    rankPrior: { field: "missing", boost: 1 }
  }));
  await assert.rejects(
    build({ configPath: join(root, "config.json") }),
    /unknown numeric field/u
  );
});
