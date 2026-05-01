import assert from "node:assert/strict";
import test from "node:test";
import {
  buildDocPagePointerTable,
  decodeDocPagePointerRecord,
  decodeDocPageColumns,
  encodeDocPageColumns,
  DOC_PAGE_ENCODING,
  DOC_PAGE_POINTER_FORMAT,
  parseDocPagePointerHeader,
  parseDocPagePointerPage
} from "../src/doc_pages.js";

const checksum = { algorithm: "sha256", value: "c".repeat(64) };

test("doc page pointer table uses a distinct generic display-page format", () => {
  const { buffer, meta } = buildDocPagePointerTable([
    { pack: "0000.hash.bin", offset: 128, length: 256, logicalLength: 1024, checksum }
  ], new Map([["0000.hash.bin", 0]]));

  assert.equal(meta.format, DOC_PAGE_POINTER_FORMAT);
  assert.deepEqual(parseDocPagePointerHeader(buffer), meta);
  const decoded = decodeDocPagePointerRecord(buffer, meta.dataOffset, meta, ["0000.hash.bin"]);
  assert.equal(decoded.pack, "0000.hash.bin");
  assert.equal(decoded.offset, 128);
  assert.equal(decoded.length, 256);
  assert.deepEqual(decoded.checksum, checksum);

  const parsed = parseDocPagePointerPage(buffer, { packTable: ["0000.hash.bin"] });
  assert.equal(parsed.format, DOC_PAGE_POINTER_FORMAT);
  assert.equal(parsed.entries[0].logicalLength, 1024);
});

test("doc page column encoding round-trips display payloads with implicit indexes", () => {
  const fields = ["id", "title", "url", "featured"];
  const encoded = encodeDocPageColumns([
    { id: "a", index: 32, title: "Alpha", url: "/a", featured: true },
    { id: "b", index: 33, title: "Beta", url: "/b", featured: false }
  ], fields);
  assert.equal(DOC_PAGE_ENCODING, "rfdocpagecols-v1");
  assert.deepEqual(decodeDocPageColumns(encoded, fields, 32), [
    { id: "a", index: 32, title: "Alpha", url: "/a", featured: true },
    { id: "b", index: 33, title: "Beta", url: "/b", featured: false }
  ]);
});
