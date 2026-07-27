import assert from "node:assert/strict";
import test from "node:test";
import { parseMultipartByteRanges, selectMultipartByteRanges } from "../src/http_ranges.js";

function multipartBody(boundary, parts) {
  const chunks = [];
  for (const part of parts) {
    chunks.push(Buffer.from(
      `--${boundary}\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes ${part.start}-${part.start + part.body.length - 1}/1000\r\n\r\n`
    ));
    chunks.push(part.body);
    chunks.push(Buffer.from("\r\n"));
  }
  chunks.push(Buffer.from(`--${boundary}--\r\n`));
  return Buffer.concat(chunks);
}

test("multipart byte ranges preserve exact binary slices", () => {
  const boundary = "rangefind-test-boundary";
  const body = multipartBody(boundary, [
    { start: 10, body: Buffer.from([0, 13, 10, 255]) },
    { start: 100, body: Buffer.from([1, 2, 3, 4, 5, 6]) }
  ]);
  const parts = parseMultipartByteRanges(
    body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength),
    `multipart/byteranges; boundary="${boundary}"`
  );
  const selected = selectMultipartByteRanges(parts, [
    { offset: 11, length: 2 },
    { offset: 102, length: 3 }
  ]);
  assert.deepEqual([...new Uint8Array(selected[0])], [13, 10]);
  assert.deepEqual([...new Uint8Array(selected[1])], [3, 4, 5]);
});

test("multipart byte ranges reject omitted requested bytes", () => {
  const boundary = "rangefind-test-boundary";
  const body = multipartBody(boundary, [{ start: 10, body: Buffer.from([1, 2, 3]) }]);
  const parts = parseMultipartByteRanges(
    body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength),
    `multipart/byteranges; boundary=${boundary}`
  );
  assert.throws(
    () => selectMultipartByteRanges(parts, [{ offset: 20, length: 1 }]),
    /omitted bytes/
  );
});
