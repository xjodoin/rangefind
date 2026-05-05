import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import { serveStatic } from "../scripts/bench_support.mjs";

async function buildCountFixture() {
  const root = await mkdtemp(join(tmpdir(), "rangefind-count-"));
  const docsPath = join(root, "docs.jsonl");
  const output = join(root, "public", "rangefind");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, [
    JSON.stringify({ id: "0", title: "Common alpha beta", body: "alpha beta gamma delta common", category: "head", url: "/0" }),
    JSON.stringify({ id: "1", title: "Common gamma delta", body: "alpha beta gamma delta common", category: "head", url: "/1" }),
    JSON.stringify({ id: "2", title: "Common alpha gamma", body: "alpha gamma delta common", category: "tail", url: "/2" }),
    JSON.stringify({ id: "3", title: "Common beta gamma", body: "beta gamma delta common", category: "tail", url: "/3" }),
    JSON.stringify({ id: "4", title: "Common delta epsilon", body: "delta epsilon common", category: "tail", url: "/4" }),
    JSON.stringify({ id: "5", title: "Zeta unique", body: "zeta unique", category: "tail", url: "/5" })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    baseShardDepth: 1,
    maxShardDepth: 1,
    targetShardPostings: 1000,
    queryBundles: false,
    typoMode: "off",
    fields: [
      { name: "title", path: "title", weight: 2.0 },
      { name: "body", path: "body", weight: 1.0 }
    ],
    facets: [{ name: "category", path: "category" }],
    display: ["title", "url", "category"]
  }));
  await build({ configPath });
  return { root, output };
}

async function runServeProtocol(indexPath, lines) {
  const servePath = resolve("scripts", "search_benchmark_game", "rangefind", "serve.mjs");
  return new Promise((resolveRun, reject) => {
    const child = spawn(process.execPath, [servePath, indexPath], {
      cwd: resolve("."),
      env: { ...process.env, RANGEFIND_REPO: resolve(".") },
      stdio: ["pipe", "pipe", "pipe"]
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", chunk => {
      stdout += chunk;
    });
    child.stderr.on("data", chunk => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", code => {
      if (code) {
        reject(new Error(`serve.mjs exited with ${code}: ${stderr}`));
        return;
      }
      resolveRun(stdout.trim().split(/\n/u).filter(Boolean));
    });
    child.stdin.end(`${lines.join("\n")}\n`);
  });
}

test("runtime count returns exact normalized text totals without scoring payloads", async (t) => {
  const { root } = await buildCountFixture();
  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: new URL("rangefind/", server.url), typoMode: "off" });

  async function assertCountMatchesExactSearch(q, expected, expectedLane) {
    const counted = await search.count({ q, trace: true });
    const exact = await search.search({ q, size: 20, exact: true, rerank: false, includeResults: false, authority: false });
    assert.equal(counted.total, expected);
    assert.equal(counted.total, exact.total);
    assert.equal(counted.totalExact, true);
    assert.equal(counted.approximate, false);
    assert.equal(counted.stats.totalExact, true);
    assert.equal(counted.stats.plannerLane, expectedLane);
    assert.ok(counted.trace?.spans?.some(span => span.name === "count.total"));
    return counted;
  }

  const single = await assertCountMatchesExactSearch("common", 5, "countSingleTermDf");
  assert.equal(single.stats.postingsDecoded, 0);
  assert.equal(single.stats.postingsAccepted, 5);

  const multi = await assertCountMatchesExactSearch("alpha beta", 2, "countPostingCounter");
  assert.ok(multi.stats.postingsDecoded > 0);
  assert.equal(multi.stats.minShouldMatch, 2);

  const minShouldMatch = await assertCountMatchesExactSearch("alpha beta gamma delta epsilon", 2, "countPostingCounter");
  assert.equal(minShouldMatch.stats.minShouldMatch, 4);

  await assertCountMatchesExactSearch("the and of", 0, "countTermless");
  await assertCountMatchesExactSearch("alpha alpha alpha beta", 2, "countPostingCounter");
  await assert.rejects(
    () => search.count({ q: "common", filters: { facets: { category: ["head"] } } }),
    /text-only queries/
  );
});

test("search benchmark game adapter returns numeric COUNT protocol responses", async () => {
  const { output } = await buildCountFixture();
  const responses = await runServeProtocol(output, [
    "COUNT\tcommon",
    "UNOPTIMIZED_COUNT\tcommon",
    "TOP_10_COUNT\tcommon",
    "TOP_100_COUNT\tcommon",
    "TOP_1000_COUNT\tcommon",
    "COUNT\tthe and of",
    "TOP_10\tcommon"
  ]);
  assert.deepEqual(responses, ["5", "5", "5", "5", "5", "0", "1"]);
});
