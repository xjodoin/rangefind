import assert from "node:assert/strict";
import test from "node:test";
import { fixedWidth, pushVarint, readFixedInt, readVarint, writeFixedInt } from "../src/binary.js";

test("varints round-trip boundary values", () => {
  const values = [0, 1, 127, 128, 255, 16384, 999999];
  const bytes = [];
  for (const value of values) pushVarint(bytes, value);
  const state = { pos: 0 };
  assert.deepEqual(values.map(() => readVarint(bytes, state)), values);
  assert.equal(state.pos, bytes.length);
});

test("fixed-width integers choose the smallest safe width", () => {
  assert.equal(fixedWidth([0, 255]), 1);
  assert.equal(fixedWidth([256, 65535]), 2);
  assert.equal(fixedWidth([65536]), 4);
});

test("fixed-width integers write and read little-endian values", () => {
  const buffer = Buffer.alloc(7);
  writeFixedInt(buffer, 0, 1, 250);
  writeFixedInt(buffer, 1, 2, 513);
  writeFixedInt(buffer, 3, 4, 70000);
  assert.equal(readFixedInt(buffer, 0, 1), 250);
  assert.equal(readFixedInt(buffer, 1, 2), 513);
  assert.equal(readFixedInt(buffer, 3, 4), 70000);
});
