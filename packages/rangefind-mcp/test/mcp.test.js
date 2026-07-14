// Real end-to-end test: build a rangefind index, launch bin.js as a child
// process, and drive it with the official MCP SDK client over stdio.

import assert from "node:assert/strict";
import test from "node:test";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { build } from "rangefind/builder";

const PKG_DIR = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const TOPICS = ["glacier", "harbor", "meadow"];

async function buildFixture() {
  const root = await mkdtemp(join(tmpdir(), "rangefind-mcp-"));
  const docs = Array.from({ length: 60 }, (_, i) => ({
    id: `doc-${i}`,
    title: `Entry ${i} about ${TOPICS[i % 3]}`,
    body: `${TOPICS[i % 3]} study number ${i} common corpus text`,
    topic: TOPICS[i % 3],
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
    geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
    suggest: [{ path: "title" }],
    display: ["title", "topic", "lat", "lon"],
    meta: { source: "fixture", license: "MIT" }
  }));
  await build({ configPath: join(root, "rangefind.config.json") });
  return join(root, "index");
}

test("MCP server answers search, geo, suggest, count, and info over stdio", { timeout: 120000 }, async (t) => {
  const indexDir = await buildFixture();
  const client = new Client({ name: "test-client", version: "0.0.0" });
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [join(PKG_DIR, "bin.js"), "--index", `fixture=${indexDir}`],
    stderr: "ignore"
  });
  await client.connect(transport);
  t.after(() => client.close());

  const { tools } = await client.listTools();
  const names = tools.map(tool => tool.name).sort();
  assert.deepEqual(names, [
    "rangefind_count",
    "rangefind_info",
    "rangefind_list_indexes",
    "rangefind_search",
    "rangefind_suggest"
  ]);
  assert.ok(tools.every(tool => tool.annotations?.readOnlyHint === true));

  const listed = await client.callTool({ name: "rangefind_list_indexes", arguments: {} });
  assert.equal(listed.structuredContent.indexes[0].name, "fixture");

  const info = await client.callTool({ name: "rangefind_info", arguments: { index: "fixture" } });
  assert.equal(info.structuredContent.total, 60);
  assert.equal(info.structuredContent.meta.license, "MIT");

  const search = await client.callTool({
    name: "rangefind_search",
    arguments: { index: "fixture", q: "harbor study", size: 5, facets: ["topic"] }
  });
  assert.equal(search.isError ?? false, false);
  const payload = search.structuredContent;
  assert.ok(payload.results.length > 0);
  assert.match(payload.results[0].title, /harbor/i);
  assert.ok(payload.facets.topic.values.some(v => v.value === "harbor"));

  const geo = await client.callTool({
    name: "rangefind_search",
    arguments: { index: "fixture", q: "", geo: { near: { lat: 45.1, lon: -73.1 } }, size: 3 }
  });
  assert.ok(geo.structuredContent.results.every(r => typeof r.distanceMeters === "number"));

  const suggest = await client.callTool({
    name: "rangefind_suggest",
    arguments: { index: "fixture", q: "entr" }
  });
  assert.ok(suggest.structuredContent.suggestions.length > 0);

  const count = await client.callTool({
    name: "rangefind_count",
    arguments: { index: "fixture", q: "glacier" }
  });
  assert.equal(count.structuredContent.total, 20);
  assert.equal(count.structuredContent.totalExact, true);

  // Configured mode: unknown names come back as actionable tool errors, not
  // protocol failures.
  const unknown = await client.callTool({
    name: "rangefind_search",
    arguments: { index: "nope", q: "x" }
  });
  assert.equal(unknown.isError, true);
  assert.match(unknown.content[0].text, /Configured indexes: fixture/);
});
