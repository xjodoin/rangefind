import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";

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
  return {
    baseUrl: `http://127.0.0.1:${server.address().port}/rangefind/`,
    close: () => new Promise(resolveClose => server.close(resolveClose))
  };
}

// Regression: with doc-id-ordered postings (the default), the doc-range-aware
// top-k proof bound must cap each range with the max impact of the REMAINING
// blocks, not the current block. Using the current block understates a
// high-impact posting at a high doc id and the proof wrongly succeeds: this
// fixture puts a medium-impact doc at doc 0 (so the proven threshold beats
// the low filler impacts), fillers in between, and the best doc last — the
// buggy bound returned "mid" as top-1 after decoding a single block.
test("doc-range top-k proof bound covers high-impact postings in later blocks", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-topk-docrange-"));
  const docs = [
    JSON.stringify({ id: "mid", title: "Filler 0", body: "zebra zebra zebra strong mention", url: "/mid" })
  ];
  for (let i = 1; i < 12; i++) {
    docs.push(JSON.stringify({ id: `low${i}`, title: `Filler ${i}`, body: `zebra mention ${i}`, url: `/low${i}` }));
  }
  docs.push(JSON.stringify({ id: "big", title: "zebra zebra zebra zebra zebra", body: "zebra zebra zebra zebra zebra zebra zebra zebra", url: "/big" }));
  await writeFile(join(root, "docs.jsonl"), docs.join("\n"));
  await writeFile(join(root, "rangefind.config.json"), JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    // Tiny blocks and ranges so the entry spans several doc ranges and the
    // proof runs while high-impact blocks are still undecoded.
    postingBlockSize: 2,
    postingSuperblockSize: 2,
    maxTermsPerDoc: 64,
    postingDocRangeBlockMax: true,
    postingDocRangeSize: 4,
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0,
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55 },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    display: ["title", "url"]
  }));
  await build({ configPath: join(root, "rangefind.config.json") });

  const server = await serveStatic(join(root, "public"));
  try {
    // The doc-range planner lane is disabled so the query exercises the
    // skip lane's stableTopK proof directly.
    const search = await createSearch({ baseUrl: server.baseUrl, docRangePlanner: false });
    const exact = await search.search({ q: "zebra", size: 1, exact: true });
    const skipped = await search.search({ q: "zebra", size: 1 });
    assert.equal(exact.results[0].id, "big");
    assert.equal(skipped.results[0].id, exact.results[0].id);
    assert.equal(Math.round(skipped.results[0].score), Math.round(exact.results[0].score));
    assert.equal(skipped.stats.topKProven, true);
  } finally {
    await server.close();
  }
});

// Regression: the block-budget stop used to floor `total` at k even when no
// document reached minShouldMatch within the budget — the response claimed
// "5 results" while carrying an empty page ("st hubert terrebonne" against a
// Quebec OSM index). An approximate total is a lower bound on eligible
// documents actually seen; with none seen it must be zero.
test("block-budget exhaustion with no eligible documents reports total 0", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-topk-budget-"));
  // alpha/beta/gamma each appear in many documents but never together, so a
  // three-term query (minShouldMatch = 3) has zero eligible documents while
  // every term still has plenty of posting blocks to scan.
  const docs = [];
  const terms = ["alpha", "beta", "gamma"];
  for (let i = 0; i < 60; i++) {
    const term = terms[i % 3];
    docs.push(JSON.stringify({ id: `doc${i}`, title: `${term} entry ${i}`, body: `${term} filler ${i} common corpus text`, url: `/doc${i}` }));
  }
  await writeFile(join(root, "docs.jsonl"), docs.join("\n"));
  await writeFile(join(root, "rangefind.config.json"), JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    postingBlockSize: 2,
    maxTermsPerDoc: 64,
    externalPostingBlockMinBlocks: 1,
    externalPostingBlockMinBytes: 0,
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55 },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    display: ["title", "url"]
  }));
  await build({ configPath: join(root, "rangefind.config.json") });

  const server = await serveStatic(join(root, "public"));
  try {
    // A one-block budget forces the budget stop before the scan can prove
    // anything; the conjunction tail is disabled so the lane cannot finish
    // the scan exactly, and typo correction must not replace the query.
    const search = await createSearch({
      baseUrl: server.baseUrl,
      docRangePlanner: false,
      conjunctionTail: false,
      topKBlockBudget: 1
    });
    const response = await search.search({ q: "alpha beta gamma", size: 5, typo: false });
    assert.equal(response.stats.plannerLane, "blockBudget");
    assert.equal(response.results.length, 0);
    assert.equal(response.total, 0);
    assert.equal(response.approximate, true);

    // Control: the same query without a budget scans to exhaustion and
    // agrees there is nothing to find.
    const exact = await createSearch({ baseUrl: server.baseUrl, docRangePlanner: false });
    const full = await exact.search({ q: "alpha beta gamma", size: 5, typo: false });
    assert.equal(full.total, 0);
  } finally {
    await server.close();
  }
});
