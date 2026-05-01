import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { encodeRunRecord, readRunRecords } from "../src/runs.js";

test("binary run records round-trip strings and numbers", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-runs-"));
  const path = join(root, "postings.run");
  await writeFile(path, Buffer.concat([
    encodeRunRecord(["string", "number", "number"], ["static", 12, 3456]),
    encodeRunRecord(["string", "number", "number"], ["range_search", 987654, 7])
  ]));

  const rows = [];
  for await (const row of readRunRecords(path, ["string", "number", "number"])) rows.push(row);

  assert.deepEqual(rows, [
    ["static", 12, 3456],
    ["range_search", 987654, 7]
  ]);
});

test("binary run reader rejects truncated records", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-runs-"));
  const path = join(root, "truncated.run");
  const encoded = encodeRunRecord(["string", "number"], ["static", 12]);
  await writeFile(path, encoded.subarray(0, encoded.length - 1));

  await assert.rejects(async () => {
    for await (const _ of readRunRecords(path, ["string", "number"])) {
      // Exhaust the async iterator.
    }
  }, /Truncated Rangefind run file/);
});
