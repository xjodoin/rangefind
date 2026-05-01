import assert from "node:assert/strict";
import test from "node:test";
import {
  buildDocPointerTable,
  buildDocOrdinalTable,
  decodeDocOrdinalRecord,
  decodeDocPointerRecord,
  DOC_ORDINAL_FORMAT,
  DOC_POINTER_FORMAT,
  parseDocOrdinalHeader,
  parseDocOrdinalTable,
  parseDocPointerHeader,
  parseDocPointerPage
} from "../src/doc_pointers.js";

const checksumA = { algorithm: "sha256", value: "a".repeat(64) };
const checksumB = { algorithm: "sha256", value: "b".repeat(64) };

test("dense doc pointer table supports direct fixed-record lookup", () => {
  const { buffer, meta } = buildDocPointerTable([
    { pack: "0000.hash.bin", offset: 10, length: 20, logicalLength: 100, checksum: checksumA },
    { pack: "0001.hash.bin", offset: 30, length: 40, logicalLength: 200, checksum: checksumB }
  ], new Map([["0000.hash.bin", 0], ["0001.hash.bin", 1]]));

  assert.equal(meta.format, DOC_POINTER_FORMAT);
  assert.equal(meta.count, 2);
  assert.ok(meta.recordBytes > 32);
  const header = parseDocPointerHeader(buffer);
  assert.deepEqual(header, meta);

  const second = decodeDocPointerRecord(buffer, meta.dataOffset + meta.recordBytes, meta, ["0000.hash.bin", "0001.hash.bin"]);
  assert.deepEqual(second, {
    pack: "0001.hash.bin",
    offset: 30,
    length: 40,
    physicalLength: 40,
    logicalLength: 200,
    checksum: checksumB
  });
});

test("dense doc pointer table can still be parsed as a whole for diagnostics", () => {
  const { buffer } = buildDocPointerTable([
    { pack: "0000.hash.bin", offset: 10, length: 20, logicalLength: 0, checksum: checksumA }
  ], new Map([["0000.hash.bin", 0]]));
  const parsed = parseDocPointerPage(buffer, { packTable: ["0000.hash.bin"] });
  assert.equal(parsed.entries.length, 1);
  assert.equal(parsed.entries[0].pack, "0000.hash.bin");
  assert.equal(parsed.entries[0].logicalLength, null);
});

test("dense doc pointer table rejects malformed object pointers", () => {
  assert.throws(
    () => buildDocPointerTable([
      { pack: "missing.bin", offset: 10, length: 20, logicalLength: 0, checksum: checksumA }
    ], new Map([["0000.hash.bin", 0]])),
    /unknown pack/
  );
  assert.throws(
    () => buildDocPointerTable([
      { pack: "0000.hash.bin", offset: 10, length: 20, logicalLength: 0, checksum: { value: "not-a-checksum" } }
    ], new Map([["0000.hash.bin", 0]])),
    /SHA-256 checksum/
  );
});

test("dense doc ordinal table maps document ids to layout order", () => {
  const { buffer, meta } = buildDocOrdinalTable([2, 0, 1], 3);
  assert.equal(meta.format, DOC_ORDINAL_FORMAT);
  assert.equal(meta.count, 3);
  assert.equal(meta.width, 1);
  assert.deepEqual(parseDocOrdinalHeader(buffer), meta);
  assert.equal(decodeDocOrdinalRecord(buffer, meta.dataOffset, meta), 1);
  assert.deepEqual(parseDocOrdinalTable(buffer).entries, [1, 2, 0]);
});

test("dense doc ordinal table rejects invalid layout orders", () => {
  assert.throws(() => buildDocOrdinalTable([0, 0], 2), /invalid document id/);
  assert.throws(() => buildDocOrdinalTable([0], 2), /missing document 1/);
});
