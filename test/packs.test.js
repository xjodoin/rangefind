import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { OBJECT_NAME_HASH_LENGTH } from "../src/object_store.js";
import { createPackWriter, finalizePackWriter, writePackedShard } from "../src/packs.js";

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

test("pack writer uses content-addressed immutable names", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-packs-"));
  const writer = createPackWriter(root, 1024);
  const bytes = Buffer.from("compressed object bytes");
  writePackedShard(writer, "a", bytes, { kind: "unit", codec: "bytes" });
  finalizePackWriter(writer);

  const hash = sha256Hex(bytes);
  assert.equal(writer.packs[0].file, `0000.${hash.slice(0, OBJECT_NAME_HASH_LENGTH)}.bin`);
  assert.equal(writer.packs[0].contentHash, hash);
  assert.equal(writer.packs[0].immutable, true);
  assert.equal(writer.entries.a.pack, writer.packs[0].file);
  assert.equal(existsSync(join(root, writer.packs[0].file)), true);
});

test("pack writer deduplicates exact compressed objects without changing pointers", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-packs-"));
  const writer = createPackWriter(root, 1024);
  const bytes = Buffer.from("same compressed object");
  const first = writePackedShard(writer, "a", bytes, { kind: "unit", codec: "bytes", logicalLength: 20 });
  const second = writePackedShard(writer, "b", bytes, { kind: "unit", codec: "bytes", logicalLength: 20 });
  finalizePackWriter(writer);

  assert.equal(writer.packs.length, 1);
  assert.equal(writer.bytes, bytes.length);
  assert.equal(writer.dedupedObjects, 1);
  assert.equal(writer.dedupedBytes, bytes.length);
  assert.equal(writer.packs[0].objects, 1);
  assert.equal(writer.packs[0].references, 2);
  assert.equal(writer.packs[0].dedupedObjects, 1);
  assert.equal(first.offset, second.offset);
  assert.equal(writer.entries.a.pack, writer.entries.b.pack);
  assert.equal(writer.entries.a.checksum.value, writer.entries.b.checksum.value);
});
