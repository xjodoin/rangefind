import { createReadStream } from "node:fs";
import { pushVarint } from "./binary.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function pushUtf8(out, value) {
  const bytes = textEncoder.encode(String(value || ""));
  pushVarint(out, bytes.length);
  for (const byte of bytes) out.push(byte);
}

function tryReadVarint(bytes, state) {
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
  const out = [];
  for (let i = 0; i < schema.length; i++) {
    if (schema[i] === "string") pushUtf8(out, values[i]);
    else pushVarint(out, Number(values[i]) || 0);
  }
  return Buffer.from(Uint8Array.from(out));
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
