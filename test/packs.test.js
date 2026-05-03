import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gunzipSync } from "node:zlib";
import { OBJECT_NAME_HASH_LENGTH } from "../src/object_store.js";
import { createAppendOnlyPackWriter, createPackWriter, finalizePackWriter, resolvePackEntry, writePackedShard, writePackedShardChunks } from "../src/packs.js";

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

test("append-only pack writer returns pointers without retaining an entry map", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-packs-append-only-"));
  const writer = createAppendOnlyPackWriter(root, 1024);
  const first = writePackedShard(writer, "a", Buffer.from("first object"), { kind: "unit" });
  const second = writePackedShard(writer, "b", Buffer.from("second object"), { kind: "unit" });
  finalizePackWriter(writer);

  assert.equal(writer.entryCount, 2);
  assert.deepEqual(writer.entries, {});
  assert.equal(writer.objects.size, 0);
  assert.equal(resolvePackEntry(writer, first).pack, writer.packs[0].file);
  assert.equal(resolvePackEntry(writer, second).pack, writer.packs[0].file);
});

test("append-only pack writer can gzip chunks directly into a pack", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-packs-stream-"));
  const writer = createAppendOnlyPackWriter(root, 1024);
  const chunks = [Buffer.from("streamed "), Buffer.from("compressed "), Buffer.from("object")];
  const entry = await writePackedShardChunks(writer, "streamed", chunks, {
    kind: "unit",
    codec: "chunked",
    logicalLength: 26,
    streamMinBytes: 0
  });
  finalizePackWriter(writer);

  const compressed = readFileSync(join(root, writer.packs[0].file)).subarray(entry.offset, entry.offset + entry.length);
  assert.equal(gunzipSync(compressed).toString("utf8"), "streamed compressed object");
  assert.equal(entry.logicalLength, 26);
  assert.equal(entry.length, compressed.length);
  assert.equal(entry.checksum.value, sha256Hex(compressed));
  assert.equal(resolvePackEntry(writer, entry).pack, writer.packs[0].file);
});

test("pack writers can share an atomic pack index counter", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-packs-shared-counter-"));
  const counter = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
  const left = createAppendOnlyPackWriter(root, 8, { indexCounter: counter });
  const right = createAppendOnlyPackWriter(root, 8, { indexCounter: counter });

  writePackedShard(left, "left-a", Buffer.from("left-a"));
  writePackedShard(right, "right-a", Buffer.from("right"));
  writePackedShard(left, "left-b", Buffer.from("left-b"));
  writePackedShard(right, "right-b", Buffer.from("right-b"));
  finalizePackWriter(left);
  finalizePackWriter(right);

  const files = [...left.packs, ...right.packs].map(pack => pack.file).sort();
  assert.deepEqual(files.map(file => file.slice(0, 4)), ["0000", "0001", "0002", "0003"]);
  assert.equal(new Set(files).size, 4);
  for (const file of files) assert.equal(existsSync(join(root, file)), true);
});
