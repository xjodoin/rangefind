// End-to-end tests for the query CLI: build a real index, run the actual
// bin with search/suggest/count/info, and parse the output.

import assert from "node:assert/strict";
import test from "node:test";
import { execFileSync } from "node:child_process";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { build } from "../src/builder.js";

const BIN = resolve("bin/rangefind.js");
const TOPICS = ["glacier", "harbor", "meadow"];

async function buildFixture() {
  const root = await mkdtemp(join(tmpdir(), "rangefind-cli-"));
  const docs = Array.from({ length: 60 }, (_, i) => ({
    id: `doc-${i}`,
    title: `Entry ${i} about ${TOPICS[i % 3]}`,
    body: `${TOPICS[i % 3]} study number ${i} common corpus text`,
    topic: TOPICS[i % 3],
    year: 2000 + (i % 10),
    lat: 45 + i / 100,
    lon: -73 - i / 100
  }));
  await writeFile(join(root, "docs.jsonl"), docs.map(d => JSON.stringify(d)).join("\n"));
  await writeFile(join(root, "rangefind.config.json"), JSON.stringify({
    input: "docs.jsonl",
    output: "index",
    targetPostingsPerDoc: 32,
    fields: [
      { name: "title", path: "title", weight: 4, phrase: true },
      { name: "body", path: "body", weight: 1 }
    ],
    facets: [{ name: "topic", path: "topic" }],
    numbers: [{ name: "year", path: "year", type: "int" }],
    geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
    suggest: [{ path: "title" }],
    display: ["title", "topic", "year"],
    meta: { source: "cli-fixture", license: "MIT" }
  }));
  await build({ configPath: join(root, "rangefind.config.json") });
  return join(root, "index");
}

function run(...argv) {
  return execFileSync(process.execPath, [BIN, ...argv], { encoding: "utf8", timeout: 60000 });
}

test("CLI search/suggest/count/info work end to end", { timeout: 120000 }, async () => {
  const index = await buildFixture();

  const search = JSON.parse(run("search", index, "harbor", "study", "--size", "5", "--facets", "topic", "--json"));
  assert.ok(search.results.length > 0);
  assert.match(search.results[0].title, /harbor/i);
  assert.ok(search.facets.topic.values.some(v => v.value === "harbor"));

  const filtered = JSON.parse(run("search", index, "study", "--filter", "topic=glacier", "--filter", "year=2004..2006", "--json"));
  assert.ok(filtered.results.length > 0);
  assert.ok(filtered.results.length > 0 && filtered.results.every(r => r.topic === "glacier" && r.year >= 2004 && r.year <= 2006));

  const nearest = JSON.parse(run("search", index, "--near", "45.1,-73.1", "--size", "3", "--json"));
  assert.ok(nearest.results.every(r => typeof r.distanceMeters === "number"));

  const human = run("search", index, "glacier", "--size", "2");
  assert.match(human, /results · \d+ ms/);
  assert.match(human, /1\. Entry \d+ about glacier/);

  const suggest = run("suggest", index, "entr");
  assert.match(suggest, /Entry \d+/);

  const count = run("count", index, "glacier").trim();
  assert.equal(count, "20");

  const info = JSON.parse(run("info", index, "--json"));
  assert.equal(info.total, 60);
  assert.equal(info.meta.license, "MIT");

  // Errors are one-line messages with a failing exit code, not stacks.
  try {
    run("search", join(index, "nope"), "x");
    assert.fail("expected a failure");
  } catch (error) {
    assert.equal(error.status, 1);
    assert.doesNotMatch(String(error.stderr), /at .*runtime\.js/);
  }
});
