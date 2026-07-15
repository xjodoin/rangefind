import assert from "node:assert/strict";
import { createServer } from "node:http";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { readConfig } from "../src/config.js";
import { collectScoringStats } from "../src/scoring_stats.js";
import { writeShardedRootManifest } from "../src/index_shards.js";
import { createSearch } from "../src/runtime.js";

async function serveStatic(root) {
  const server = createServer(async (rq, rs) => {
    try {
      const u = new URL(rq.url, "http://x");
      const p = resolve(root, `.${decodeURIComponent(u.pathname)}`);
      if (!p.startsWith(resolve(root))) return void rs.writeHead(403).end();
      const data = await readFile(p);
      const r = rq.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (r) {
        const a = +r[1], b = Math.min(+r[2], data.length - 1);
        rs.writeHead(206, { "Accept-Ranges": "bytes", "Content-Length": String(b - a + 1), "Content-Range": `bytes ${a}-${b}/${data.length}` });
        return void rs.end(data.subarray(a, b + 1));
      }
      rs.writeHead(200, { "Content-Length": String(data.length) });
      rs.end(data);
    } catch { rs.writeHead(404).end(); }
  });
  await new Promise(done => server.listen(0, "127.0.0.1", done));
  return { baseUrl: `http://127.0.0.1:${server.address().port}/rangefind/`, close: () => new Promise(d => server.close(d)) };
}

// All documents share identical indexable text so BM25 ties and authority
// (linkRank) is the only differentiator.
const BASE_CONFIG = {
  input: "docs.jsonl",
  output: "public/rangefind",
  scanWorkers: 2,
  targetPostingsPerDoc: 32,
  fields: [
    { name: "title", path: "title", weight: 4.5, b: 0.55, phrase: true },
    { name: "body", path: "body", weight: 1.0, b: 0.75 }
  ],
  geo: [{ name: "location", latPath: "lat", lonPath: "lon" }],
  numbers: [{ name: "linkRank", path: "linkRank", type: "double", sortable: true }],
  sorts: [{ field: "linkRank", order: "desc" }],
  linkGraph: { field: "linkRank", boost: 0.5 },
  display: ["title"]
};

const doc = (id, linkRank, lat, lon) => ({ id, title: "widget", body: "widget shared body text", linkRank, lat, lon });

test("generational (--update) index applies the authority prior", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-gen-lr-"));
  const config = { ...BASE_CONFIG, output: "rangefind" };
  const write = async (docs, opts) => {
    await writeFile(join(root, "docs.jsonl"), docs.map(d => JSON.stringify(d)).join("\n"));
    await writeFile(join(root, "rangefind.config.json"), JSON.stringify(config));
    await build({ configPath: join(root, "rangefind.config.json"), ...opts });
  };
  // Base: six low-authority docs. Delta: adds the authoritative page.
  await write([doc("b0", 0.1, 45, 2), doc("b1", 0.1, 45, 2), doc("b2", 0.1, 45, 2), doc("b3", 0.1, 45, 2), doc("b4", 0.1, 45, 2), doc("b5", 0.1, 45, 2)]);
  await write([doc("hub", 1.0, 45, 2), doc("d1", 0.1, 45, 2), doc("d2", 0.1, 45, 2)], { update: true });

  const server = await serveStatic(root);
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });
  assert.equal(search.manifest.generations.length, 2, "index is generational");

  const off = await search.search({ q: "widget", size: 3, linkRankBoost: 0 });
  assert.equal(off.results.some(r => r.id === "hub"), false, "hub is off-page without the prior");

  const on = await search.search({ q: "widget", size: 3 });
  assert.equal(on.results[0].id, "hub", "authority surfaces the hub across generations");
  assert.equal(on.stats.linkRankBoost, true, "merged stats report the boost");
});

test("sharded index applies the authority prior across shards", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-shard-lr-"));
  const bands = [1, 2, 3];
  // 6 tied docs per band; the authoritative hub lives in band 2.
  const docsByBand = bands.map(band => Array.from({ length: 6 }, (_, i) =>
    doc(`b${band}-${i}`, band === 2 && i === 0 ? 1.0 : 0.1, band * 10, -70 + i * 0.01)));

  for (const band of bands) {
    await writeFile(join(root, `band-${band}.jsonl`), docsByBand[band - 1].map(d => JSON.stringify(d)).join("\n"));
  }
  await writeFile(join(root, "template.config.json"), JSON.stringify(BASE_CONFIG));
  const templateConfig = await readConfig(join(root, "template.config.json"));
  const stats = await collectScoringStats({
    config: templateConfig,
    inputs: bands.map(band => ({ id: `band-${band}`, input: join(root, `band-${band}.jsonl`) })),
    outDir: join(root, "scoring-stats")
  });
  for (const band of bands) {
    await writeFile(join(root, `band-${band}.config.json`), JSON.stringify({
      ...BASE_CONFIG,
      input: `band-${band}.jsonl`,
      output: `public/rangefind/shards/band-${band}`,
      scoringStats: "scoring-stats/scoring-stats.json"
    }));
    await build({ configPath: join(root, `band-${band}.config.json`) });
  }
  await writeShardedRootManifest({
    outDir: join(root, "public/rangefind"),
    shards: bands.map(band => ({
      id: `band-${band}`,
      path: `shards/band-${band}/`,
      bbox: stats.stats.inputs.find(item => item.id === `band-${band}`)?.bbox
    })),
    scoringStats: stats.stats
  });

  const server = await serveStatic(join(root, "public"));
  t.after(() => server.close());
  const search = await createSearch({ baseUrl: server.baseUrl });

  const off = await search.search({ q: "widget", size: 5, linkRankBoost: 0 });
  const on = await search.search({ q: "widget", size: 5 });
  assert.equal(on.results[0].id, "b2-0", "authority surfaces the hub across shards");
  assert.equal(on.stats.linkRankBoost, true, "merged stats report the boost");
  // Sanity: without the prior the tie resolves differently.
  assert.notEqual(off.results[0].id === "b2-0" && off.stats?.linkRankBoost, true);
});
