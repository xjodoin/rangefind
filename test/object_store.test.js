import assert from "node:assert/strict";
import test from "node:test";
import {
  OBJECT_CHECKSUM_ALGORITHM,
  pointerChecksum,
  sha256Hex,
  verifyBlockPointer
} from "../src/object_store.js";

const encoder = new TextEncoder();

test("sha256Hex hashes byte ranges used by object pointers", async () => {
  const bytes = encoder.encode("abc");
  assert.equal(await sha256Hex(bytes), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
});

test("pointer helpers read structured checksum metadata", () => {
  assert.deepEqual(pointerChecksum({ checksum: { value: "11bb" } }), {
    algorithm: OBJECT_CHECKSUM_ALGORITHM,
    value: "11bb"
  });
});

test("verifyBlockPointer accepts matching compressed object bytes", async () => {
  const bytes = encoder.encode("rangefind object");
  const checksum = await sha256Hex(bytes);
  await verifyBlockPointer(bytes, { checksum: { algorithm: OBJECT_CHECKSUM_ALGORITHM, value: checksum } }, "unit object");
});

test("verifyBlockPointer rejects corrupt or unsupported object pointers", async () => {
  const bytes = encoder.encode("rangefind object");
  await assert.rejects(
    () => verifyBlockPointer(bytes, { checksum: { algorithm: OBJECT_CHECKSUM_ALGORITHM, value: "deadbeef" } }, "unit object"),
    /checksum mismatch/
  );
  await assert.rejects(
    () => verifyBlockPointer(bytes, { checksum: { algorithm: "crc32c", value: "00000000" } }, "unit object"),
    /unsupported checksum crc32c/
  );
  await assert.rejects(
    () => verifyBlockPointer(bytes, {}, "unit object"),
    /missing checksum/
  );
});
