import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createSearch } from "../src/runtime.js";
import { findMatchRanges, highlightTermSet, highlightText } from "../src/highlight.js";

function marked(highlight) {
  let out = "";
  let cursor = 0;
  for (const [start, end] of highlight.ranges) {
    out += `${highlight.text.slice(cursor, start)}[${highlight.text.slice(start, end)}]`;
    cursor = end;
  }
  return out + highlight.text.slice(cursor);
}

test("match ranges are analyzer-consistent in both directions", () => {
  const terms = highlightTermSet("montreal walking");
  const text = "Walk from Montréal to MONTREAL-Est, walked far.";
  const ranges = findMatchRanges(text, terms);
  const words = ranges.map(([start, end]) => text.slice(start, end));
  assert.deepEqual(words, ["Walk", "Montréal", "MONTREAL", "walked"]);
});

test("highlight windows pick the densest passage and snap to words", () => {
  const filler = "nothing relevant here at all just filler words to push the interesting part far away. ".repeat(6);
  const text = `${filler}The static range index answers range requests, and the range proof is exact.`;
  const highlight = highlightText(text, highlightTermSet("range proof"), { maxChars: 90 });
  assert.ok(highlight.text.startsWith("… "));
  assert.ok(highlight.text.includes("[") === false);
  const rendered = marked(highlight);
  assert.ok(rendered.includes("[range]"), rendered);
  assert.ok(rendered.includes("[proof]"), rendered);
  // Every range must slice cleanly inside the snippet.
  for (const [start, end] of highlight.ranges) {
    assert.ok(start >= 0 && end <= highlight.text.length && start < end);
  }
});

test("no match yields null instead of an arbitrary snippet", () => {
  assert.equal(highlightText("Nothing to see", highlightTermSet("zebra")), null);
  assert.equal(highlightText("", highlightTermSet("zebra")), null);
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

test("search results carry highlights when requested", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-highlight-"));
  const docsPath = join(root, "docs.jsonl");
  const configPath = join(root, "rangefind.config.json");
  await writeFile(docsPath, [
    JSON.stringify({
      id: "a",
      title: "Static range search",
      body: `${"Filler sentence with no relevant content. ".repeat(10)}Rangefind builds a static index and serves range requests from a proof-checked directory.`
    }),
    JSON.stringify({ id: "b", title: "Unrelated article", body: "Nothing here about the query." })
  ].join("\n"));
  await writeFile(configPath, JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    fields: [
      { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
      { name: "body", path: "body", weight: 1.0, b: 0.75 }
    ],
    display: ["title", { name: "body", path: "body", maxChars: 800 }]
  }));
  await build({ configPath });
  const server = await serveStatic(join(root, "public"));
  try {
    const engine = await createSearch({ baseUrl: server.baseUrl });
    const plain = await engine.search({ q: "range proof" });
    assert.equal(plain.results[0].highlights, undefined);

    const highlighted = await engine.search({ q: "range proof", highlight: { maxChars: 120 } });
    const hit = highlighted.results.find(item => item.id === "a");
    assert.ok(hit.highlights.title, "title should highlight");
    assert.ok(hit.highlights.body, "body should highlight");
    const rendered = marked(hit.highlights.body);
    assert.ok(rendered.includes("[range]"), rendered);
    assert.ok(rendered.includes("[proof"), rendered);
    assert.ok(hit.highlights.body.text.length <= 130 + 4);

    const scoped = await engine.search({ q: "range", highlight: { fields: ["title"] } });
    const scopedHit = scoped.results.find(item => item.id === "a");
    assert.ok(scopedHit.highlights.title);
    assert.equal(scopedHit.highlights.body, undefined);
  } finally {
    await server.close();
  }
});
