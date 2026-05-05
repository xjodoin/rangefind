import assert from "node:assert/strict";
import test from "node:test";
import {
  buildDocPointerTable,
  decodeDocPointerRecord,
  DOC_POINTER_FORMAT,
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
