// Regression tests: every fast lane that claims an exact answer (query
// bundles, sorted doc-value trees, sort replicas) must return what the exact
// lane (`exact: true`, or a full sort for browse) returns.
import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { build } from "../src/builder.js";
import { createNodeSearch } from "../src/runtime.node.js";

const FILLER = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor".split(" ");

function corpus() {
  const docs = [];
  const push = (title, body, extra = {}) => docs.push({ id: String(docs.length), title, body, ...extra });
  // "alpha beta" seeds a complete 2-term bundle. Short titles rank first;
  // the featured ones are long, so they rank below the first filter batch.
  for (let i = 0; i < 30; i++) push(`alpha beta story ${i}`, FILLER.slice(0, 3 + (i % 5)).join(" "), { featured: false });
  for (let i = 0; i < 5; i++) push(`alpha beta ${FILLER.join(" ")} ${i}`, `${FILLER.join(" ")} ${FILLER.join(" ")}`, { featured: true });
  // "gamma alpha beta" seeds the "alpha beta" window but not "alpha beta gamma".
  for (let i = 0; i < 3; i++) push(`gamma alpha beta tale ${i}`, "gamma", { featured: false });
  // Sort fields: missing values, negative values, long runs of equal values
  // (the tree splits ties across pages at 4x the page size) and booleans.
  for (let i = 0; i < docs.length; i++) {
    const doc = docs[i];
    if (i % 7 !== 3) doc.year = 2000 + (i % 3);
    if (i % 5 !== 1) doc.temperature = (i % 4) - 2;
    if (i % 9 === 4) delete doc.featured;
    doc.category = ["culture", "science", "food"][i % 3];
  }
  return docs;
}

let fixture = null;
async function index() {
  if (fixture) return fixture;
  const root = await mkdtemp(join(tmpdir(), "rangefind-fast-lanes-"));
  const docs = corpus();
  await writeFile(join(root, "docs.jsonl"), docs.map(doc => JSON.stringify(doc)).join("\n"));
  await writeFile(join(root, "rangefind.config.json"), JSON.stringify({
    input: "docs.jsonl",
    output: "public/rangefind",
    idPath: "id",
    queryBundles: true,
    queryBundleMinSeedDocs: 1,
    docValueSortedPageSize: 2,
    docValueChunkSize: 8,
    sortReplicas: [{ field: "year", order: "desc" }, { field: "temperature", order: "asc" }],
    fields: [
      { name: "title", path: "title", weight: 4.5, phrase: true },
      { name: "body", path: "body", weight: 1.0 }
    ],
    facets: [{ name: "category", path: "category" }],
    numbers: [{ name: "year", path: "year", type: "int" }, { name: "temperature", path: "temperature", type: "int" }],
    booleans: [{ name: "featured", path: "featured" }],
    display: ["id", "title", "year", "temperature", "featured", "category"]
  }));
  const log = console.log;
  console.log = () => {};
  try {
    await build({ configPath: join(root, "rangefind.config.json") });
  } finally {
    console.log = log;
  }
  fixture = { dir: join(root, "public", "rangefind"), docs };
  return fixture;
}

async function engine() {
  const { dir } = await index();
  return createNodeSearch({ source: dir });
}

const rows = response => response.results.map(item => [item.id, item.score]);

// compareRankedRows: missing values last in both orders, then doc order.
function expectedBrowse(docs, field, desc, keep = () => true) {
  const value = doc => (doc[field] == null ? null : Number(doc[field]));
  return docs.filter(keep).sort((a, b) => {
    const left = value(a);
    const right = value(b);
    if (left == null || right == null) return left == null && right == null ? Number(a.id) - Number(b.id) : left == null ? 1 : -1;
    return left !== right ? (desc ? right - left : left - right) : Number(a.id) - Number(b.id);
  }).map(doc => doc.id);
}

test("query bundles answer only a query over their full base-term list", async () => {
  const search = await engine();
  const params = { q: "alpha beta gamma", size: 3, rerank: false };
  const fast = await search.search(params);
  const exact = await search.search({ ...params, exact: true });
  assert.deepEqual(rows(fast), rows(exact));
  assert.deepEqual(fast.results.map(item => item.title.startsWith("gamma")), [true, true, true]);
  assert.notEqual(fast.stats.plannerLane, "queryBundleExact");
  const pair = await search.search({ q: "alpha beta", size: 3, rerank: false });
  assert.equal(pair.stats.plannerLane, "queryBundleExact");
});

test("a complete query bundle proves filtered top k only after k rows pass", async () => {
  const search = await engine();
  const params = { q: "alpha beta", size: 3, filters: { booleans: { featured: true } } };
  const fast = await search.search(params);
  const exact = await search.search({ ...params, exact: true });
  assert.equal(fast.stats.plannerLane, "queryBundleExact");
  assert.equal(fast.results.length, 3);
  assert.deepEqual(rows(fast), rows(exact));
  assert.ok(fast.stats.queryBundleFilterRowsScanned > 8);
  const all = await search.search({ ...params, size: 10 });
  assert.deepEqual(rows(all), rows(await search.search({ ...params, size: 10, exact: true })));
  assert.equal(all.total, 4);
});

test("loading the full manifest keeps lazily loaded facet dictionaries", async () => {
  const search = await engine();
  const filters = { facets: { category: ["culture", "science"] } };
  const before = await search.search({ q: "alpha", size: 50, filters });
  await search.search({ q: "alpha beta", size: 3, exact: true });
  const after = await search.search({ q: "alpha", size: 50, filters });
  assert.ok(after.results.length > 0);
  assert.ok(after.results.every(item => item.category === "culture" || item.category === "science"));
  assert.deepEqual(rows(after), rows(before));
});

test("sorted browse lanes rank like the exact sort: missing last, booleans, split ties", async () => {
  const search = await engine();
  const { docs } = await index();
  for (const sort of ["year", "-year", "temperature", "-temperature", "featured", "-featured"]) {
    const field = sort.replace(/^-/u, "");
    const desc = sort.startsWith("-");
    const expected = expectedBrowse(docs, field, desc);
    for (const [page, size] of [[1, 5], [2, 7], [1, 100], [5, 9]]) {
      const response = await search.search({ q: "", sort, page, size });
      assert.deepEqual(response.results.map(item => item.id), expected.slice((page - 1) * size, page * size), `${sort} page ${page} size ${size}`);
      assert.equal(response.total, docs.length);
    }
    const filtered = await search.search({ q: "", sort, size: 100, filters: { facets: { category: ["culture"] } } });
    const keep = doc => doc.category === "culture";
    assert.deepEqual(filtered.results.map(item => item.id), expectedBrowse(docs, field, desc, keep), `${sort} filtered`);
    assert.equal(filtered.total, docs.filter(keep).length);
  }
});

test("sorted text lanes and sort replicas match the exact lane", async () => {
  const search = await engine();
  for (const sort of ["year", "-year", "temperature", "-temperature", "featured", "-featured"]) {
    for (const params of [
      { q: "alpha beta", size: 5, rerank: false },
      { q: "alpha beta", size: 50, rerank: false },
      { q: "alpha", size: 4, page: 3 },
      { q: "story", size: 10, filters: { facets: { category: ["science"] } } }
    ]) {
      const request = { ...params, sort };
      const fast = await search.search(request);
      const exact = await search.search({ ...request, exact: true });
      assert.deepEqual(rows(fast), rows(exact), `${JSON.stringify(request)} via ${fast.stats.plannerLane}`);
      if (!fast.approximate) assert.equal(fast.total, exact.total);
    }
  }
  const replica = await search.search({ q: "alpha beta", size: 50, sort: "-year", rerank: false });
  assert.equal(replica.stats.plannerLane, "sortReplicaText");
  assert.equal(replica.results.at(-1).year ?? "", "");
});
