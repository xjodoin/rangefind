const FILTER_BITMAP_MAGIC = [0x52, 0x46, 0x42, 0x4d]; // RFBM

export const FILTER_BITMAP_FORMAT = "rffilterbitmap-v1";

export function createFilterBitmap(total) {
  return new Uint8Array(Math.ceil(Math.max(0, total) / 8));
}

export function setFilterBitmapBit(bytes, doc) {
  if (!Number.isInteger(doc) || doc < 0) return;
  bytes[doc >> 3] |= 1 << (doc & 7);
}

export function filterBitmapHas(bitmap, doc) {
  if (!bitmap || !Number.isInteger(doc) || doc < 0 || doc >= bitmap.total) return false;
  return ((bitmap.bytes[doc >> 3] || 0) & (1 << (doc & 7))) !== 0;
}

export function encodeFilterBitmap(total, bytes) {
  const out = Buffer.alloc(FILTER_BITMAP_MAGIC.length + 8 + bytes.length);
  FILTER_BITMAP_MAGIC.forEach((value, index) => {
    out[index] = value;
  });
  out.writeUInt32LE(1, FILTER_BITMAP_MAGIC.length);
  out.writeUInt32LE(total, FILTER_BITMAP_MAGIC.length + 4);
  Buffer.from(bytes).copy(out, FILTER_BITMAP_MAGIC.length + 8);
  return out;
}

export function parseFilterBitmap(buffer) {
  const bytes = new Uint8Array(buffer);
  for (let index = 0; index < FILTER_BITMAP_MAGIC.length; index++) {
    if (bytes[index] !== FILTER_BITMAP_MAGIC[index]) throw new Error("Unsupported Rangefind filter bitmap.");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const version = view.getUint32(FILTER_BITMAP_MAGIC.length, true);
  if (version !== 1) throw new Error(`Unsupported Rangefind filter bitmap version ${version}.`);
  const total = view.getUint32(FILTER_BITMAP_MAGIC.length + 4, true);
  return {
    format: FILTER_BITMAP_FORMAT,
    total,
    bytes: bytes.subarray(FILTER_BITMAP_MAGIC.length + 8)
  };
}
