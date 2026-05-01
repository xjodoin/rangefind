export const TERM_SHARD_MAGIC = [0x52, 0x46, 0x53, 0x42]; // RFSB
export const DIRECTORY_ROOT_MAGIC = [0x52, 0x46, 0x44, 0x52]; // RFDR
export const DIRECTORY_PAGE_MAGIC = [0x52, 0x46, 0x44, 0x50]; // RFDP
export const CODE_MAGIC = [0x52, 0x46, 0x43, 0x42]; // RFCB
export const DOC_VALUE_MAGIC = [0x52, 0x46, 0x56, 0x42]; // RFVB
export const FACET_DICT_MAGIC = [0x52, 0x46, 0x46, 0x44]; // RFFD
export const TYPO_SHARD_MAGIC = [0x52, 0x46, 0x54, 0x42]; // RFTB
export const DOC_POINTER_PAGE_MAGIC = [0x52, 0x46, 0x50, 0x44]; // RFPD
export const DOC_ORDINAL_TABLE_MAGIC = [0x52, 0x46, 0x44, 0x4f]; // RFDO
export const DOC_PAGE_POINTER_MAGIC = [0x52, 0x46, 0x50, 0x47]; // RFPG
export const DOC_PAGE_PAYLOAD_MAGIC = [0x52, 0x46, 0x50, 0x43]; // RFPC
export const DOC_VALUE_SORT_DIRECTORY_MAGIC = [0x52, 0x46, 0x44, 0x54]; // RFDT
export const DOC_VALUE_SORT_PAGE_MAGIC = [0x52, 0x46, 0x44, 0x56]; // RFDV

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
  if (max <= 0xffffffff) return 4;
  if (max <= 0xffffffffffff) return 6;
  return 8;
}

export function writeFixedInt(buffer, offset, width, value) {
  let remaining = BigInt(Math.max(0, Math.floor(value || 0)));
  for (let i = 0; i < width; i++) {
    buffer[offset + i] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
}

export function readFixedInt(bytes, offset, width) {
  let value = 0;
  for (let i = 0; i < width; i++) value += bytes[offset + i] * (2 ** (8 * i));
  return value;
}
