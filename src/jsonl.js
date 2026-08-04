import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { createGunzip } from "node:zlib";

export function createJsonlReadStream(path) {
  const input = createReadStream(path);
  return String(path).toLowerCase().endsWith(".gz")
    ? input.pipe(createGunzip())
    : input;
}

export async function eachJsonLine(path, fn) {
  const rl = createInterface({ input: createJsonlReadStream(path), crlfDelay: Infinity });
  let index = 0;
  for await (const line of rl) {
    if (!line.trim()) continue;
    await fn(JSON.parse(line), index++);
  }
  return index;
}
