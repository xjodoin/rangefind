export const TERM_SHARD_MAGIC = [0x52, 0x46, 0x53, 0x42]; // RFSB
export const TERM_RANGE_MAGIC = [0x52, 0x46, 0x52, 0x47]; // RFRG
export const CODE_MAGIC = [0x52, 0x46, 0x43, 0x42]; // RFCB
export const TYPO_SHARD_MAGIC = [0x52, 0x46, 0x54, 0x42]; // RFTB

export function pushVarint(out, value) {
  let n = Math.max(0, Math.floor(value));
  while (n >= 0x80) {
    out.push((n % 0x80) | 0x80);
    n = Math.floor(n / 0x80);
  }
  out.push(n);
}

export function readVarint(bytes, state) {
  let value = 0;
  let multiplier = 1;
  while (state.pos < bytes.length) {
    const byte = bytes[state.pos++];
    value += (byte & 0x7f) * multiplier;
    if ((byte & 0x80) === 0) return value;
    multiplier *= 0x80;
  }
  return value;
}

export function fixedWidth(values) {
  let max = 0;
  for (const value of values) if (value > max) max = value;
  if (max <= 0xff) return 1;
  if (max <= 0xffff) return 2;
  return 4;
}

export function writeFixedInt(buffer, offset, width, value) {
  if (width === 1) buffer[offset] = value;
  else if (width === 2) buffer.writeUInt16LE(value, offset);
  else buffer.writeUInt32LE(value, offset);
}

export function readFixedInt(bytes, offset, width) {
  let value = 0;
  for (let i = 0; i < width; i++) value += bytes[offset + i] * (2 ** (8 * i));
  return value;
}
