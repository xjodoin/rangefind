import assert from "node:assert/strict";
import test from "node:test";
import {
  DOC_VALUE_SORT_DIRECTORY_FORMAT,
  DOC_VALUE_SORT_PAGE_FORMAT,
  decodeDocValueSortPage,
  encodeDocValueSortDirectory,
  encodeDocValueSortPage,
  parseDocValueSortDirectory
} from "../src/doc_value_tree.js";

const checksum = { algorithm: "sha256", value: "d".repeat(64) };

test("sorted doc-value pages round-trip typed values and doc ids", () => {
  const field = { name: "published", kind: "number", type: "date" };
  const encoded = encodeDocValueSortPage(field, 10, [
    { doc: 7, value: 1767225600000 },
    { doc: 3, value: 1767312000000 }
  ]);

  assert.equal(encoded.meta.count, 2);
  assert.equal(encoded.meta.min, 1767225600000);
  assert.deepEqual(decodeDocValueSortPage(encoded.buffer, field).rows, [
    { doc: 7, value: 1767225600000, sortValue: 1767225600000, rank: 10 },
    { doc: 3, value: 1767312000000, sortValue: 1767312000000, rank: 11 }
  ]);
});

test("sorted doc-value directories keep page ranges, summaries, and immutable pack pointers", () => {
  const field = { name: "bodyLength", kind: "number", type: "int" };
  const directory = encodeDocValueSortDirectory({
    field,
    pageSize: 512,
    total: 2,
    packTable: ["0000.hash.bin"],
    packIndexes: new Map([["0000.hash.bin", 0]]),
    summaryFields: [
      field,
      { name: "hasCategories", kind: "boolean", type: "boolean" }
    ],
    pages: [{
      rankStart: 0,
      count: 2,
      min: 80,
      max: 120,
      entry: {
        pack: "0000.hash.bin",
        offset: 16,
        length: 32,
        physicalLength: 32,
        logicalLength: 64,
        checksum
      },
      summaries: {
        bodyLength: { min: 80, max: 120 },
        hasCategories: { min: 2, max: 2 }
      }
    }]
  });

  assert.equal(directory.meta.format, DOC_VALUE_SORT_DIRECTORY_FORMAT);
  assert.equal(directory.meta.page_format, DOC_VALUE_SORT_PAGE_FORMAT);
  const parsed = parseDocValueSortDirectory(directory.buffer);
  assert.equal(parsed.field.name, "bodyLength");
  assert.equal(parsed.pages[0].pack, "0000.hash.bin");
  assert.deepEqual(parsed.pages[0].checksum, checksum);
  assert.deepEqual(parsed.pages[0].summaries.hasCategories, { min: 2, max: 2 });
});
