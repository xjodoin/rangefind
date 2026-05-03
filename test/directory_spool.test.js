import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { writeDirectoryFilesFromSortedEntries } from "../src/directory_writer.js";
import { appendDirectoryEntry, createDirectoryEntrySpool, readDirectoryEntrySpool, sortedDirectoryEntrySpool } from "../src/directory_spool.js";

test("directory entry spool round-trips sorted immutable pack entries", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-directory-spool-"));
  const spool = createDirectoryEntrySpool(join(root, "entries.bin"));
  appendDirectoryEntry(spool, "beta", {
    pack: "0000.bin",
    offset: 4,
    length: 8,
    logicalLength: 16,
    checksum: { algorithm: "sha256", value: "b".repeat(64) }
  });
  appendDirectoryEntry(spool, "alpha", {
    pack: "0001.bin",
    offset: 12,
    length: 20,
    logicalLength: 40,
    checksum: { algorithm: "sha256", value: "a".repeat(64) }
  });

  const entries = await readDirectoryEntrySpool(spool, {
    packNameMap: new Map([["0000.bin", "0000.hash.bin"], ["0001.bin", "0001.hash.bin"]]),
    packIndexes: new Map([["0000.hash.bin", 0], ["0001.hash.bin", 1]])
  });

  assert.equal(spool.entries, 2);
  assert.ok(spool.bytes > 0);
  assert.deepEqual(entries.map(entry => entry.shard), ["alpha", "beta"]);
  assert.equal(entries[0].pack, "0001.hash.bin");
  assert.equal(entries[0].packIndex, 1);
  assert.equal(entries[1].pack, "0000.hash.bin");
  assert.equal(entries[1].packIndex, 0);
});

test("directory entry spool sorts through bounded chunks", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-directory-spool-chunks-"));
  const spool = createDirectoryEntrySpool(join(root, "entries.bin"));
  for (const shard of ["delta", "alpha", "charlie", "bravo", "echo"]) {
    appendDirectoryEntry(spool, shard, {
      pack: "0000.bin",
      offset: shard.length,
      length: 8,
      logicalLength: 16,
      checksum: { algorithm: "sha256", value: shard[0].repeat(64) }
    });
  }

  const entries = [];
  for await (const entry of sortedDirectoryEntrySpool(spool, {
    chunkEntries: 2,
    packNameMap: new Map([["0000.bin", "0000.hash.bin"]]),
    packIndexes: new Map([["0000.hash.bin", 0]])
  })) {
    entries.push(entry);
  }

  assert.deepEqual(entries.map(entry => entry.shard), ["alpha", "bravo", "charlie", "delta", "echo"]);
  assert.ok(entries.every(entry => entry.pack === "0000.hash.bin" && entry.packIndex === 0));
});

test("streaming directory writer builds paged files from sorted entries", async () => {
  const root = await mkdtemp(join(tmpdir(), "rangefind-directory-stream-"));
  const entries = [
    {
      shard: "alpha",
      pack: "0000.hash.bin",
      packIndex: 0,
      offset: 0,
      length: 10,
      logicalLength: 20,
      checksum: { algorithm: "sha256", value: "a".repeat(64) }
    },
    {
      shard: "beta",
      pack: "0000.hash.bin",
      packIndex: 0,
      offset: 10,
      length: 10,
      logicalLength: 20,
      checksum: { algorithm: "sha256", value: "b".repeat(64) }
    }
  ];

  const directory = await writeDirectoryFilesFromSortedEntries(root, entries, entries.length, 1024, "terms", { packTable: ["0000.hash.bin"] });

  assert.equal(directory.format, "rfdir-v2");
  assert.equal(directory.entries, 2);
  assert.equal(directory.page_files, 1);
  assert.deepEqual(directory.pack_table, ["0000.hash.bin"]);
  assert.match(directory.root, /^terms\/directory-root\.[0-9a-f]{24}\.bin\.gz$/u);
});
