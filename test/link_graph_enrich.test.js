import assert from "node:assert/strict";
import test from "node:test";
import enrich, { config } from "../examples/link-graph-enrich.mjs";

test("enrich module declares the linkRank number, sort, and boost block", () => {
  assert.equal(config.numbers[0].name, "linkRank");
  assert.equal(config.numbers[0].sortable, true);
  assert.equal(config.sorts[0].field, "linkRank");
  assert.equal(config.linkGraph.field, "linkRank");
  assert.ok(config.linkGraph.boost > 0);
});

test("enrich computes linkRank from a per-document links field", async () => {
  const docs = [
    { id: "hub", links: [] },
    { id: "a", links: ["hub", "missing", "a"] }, // unknown + self ignored
    { id: "b", links: ["hub"] },
    { id: "c", links: ["hub"] }
  ];
  const out = await enrich(docs);
  const byId = Object.fromEntries(out.map(doc => [doc.id, doc.linkRank]));
  assert.equal(byId.hub, 1, "the linked hub is the top authority");
  for (const id of ["a", "b", "c"]) {
    assert.ok(byId[id] >= 0 && byId[id] < 1, `${id} ranks below the hub`);
  }
});
