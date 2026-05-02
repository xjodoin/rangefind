import { createReadStream } from "node:fs";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function varintLength(value) {
  let n = Math.max(0, Math.floor(Number(value) || 0));
  let bytes = 1;
  while (n >= 0x80) {
    bytes++;
    n = Math.floor(n / 0x80);
  }
  return bytes;
}

export function writeVarint(buffer, offset, value) {
  let n = Math.max(0, Math.floor(Number(value) || 0));
  let pos = offset;
  while (n >= 0x80) {
    buffer[pos++] = (n % 0x80) | 0x80;
    n = Math.floor(n / 0x80);
  }
  buffer[pos++] = n;
  return pos;
}

export function tryReadVarint(bytes, state) {
  let value = 0;
  let multiplier = 1;
  let pos = state.pos;
  while (pos < bytes.length) {
    const byte = bytes[pos++];
    value += (byte & 0x7f) * multiplier;
    if (byte < 0x80) {
      state.pos = pos;
      return value;
    }
    multiplier *= 0x80;
  }
  return null;
}

export function encodeRunRecord(schema, values) {
  const parts = new Array(schema.length);
  let total = 0;
  for (let i = 0; i < schema.length; i++) {
    if (schema[i] === "string") {
      const bytes = Buffer.from(textEncoder.encode(String(values[i] || "")));
      parts[i] = bytes;
      total += varintLength(bytes.length) + bytes.length;
    } else {
      const value = Number(values[i]) || 0;
      parts[i] = value;
      total += varintLength(value);
    }
  }
  const out = Buffer.allocUnsafe(total);
  let pos = 0;
  for (let i = 0; i < schema.length; i++) {
    if (schema[i] === "string") {
      const bytes = parts[i];
      pos = writeVarint(out, pos, bytes.length);
      bytes.copy(out, pos);
      pos += bytes.length;
    } else {
      pos = writeVarint(out, pos, parts[i]);
    }
  }
  return out;
}

export async function* readRunRecords(path, schema) {
  let pending = Buffer.alloc(0);
  for await (const chunk of createReadStream(path)) {
    const bytes = pending.length ? Buffer.concat([pending, chunk]) : chunk;
    const state = { pos: 0 };

    while (state.pos < bytes.length) {
      const start = state.pos;
      const record = [];
      let complete = true;

      for (const type of schema) {
        if (type === "string") {
          const length = tryReadVarint(bytes, state);
          if (length == null || state.pos + length > bytes.length) {
            complete = false;
            break;
          }
          record.push(textDecoder.decode(bytes.subarray(state.pos, state.pos + length)));
          state.pos += length;
        } else {
          const value = tryReadVarint(bytes, state);
          if (value == null) {
            complete = false;
            break;
          }
          record.push(value);
        }
      }

      if (!complete) {
        state.pos = start;
        break;
      }
      yield record;
    }

    pending = state.pos < bytes.length ? bytes.subarray(state.pos) : Buffer.alloc(0);
  }
  if (pending.length) throw new Error(`Truncated Rangefind run file: ${path}`);
}
