import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";

export async function eachJsonLine(path, fn) {
  const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  let index = 0;
  for await (const line of rl) {
    if (!line.trim()) continue;
    await fn(JSON.parse(line), index++);
  }
  return index;
}
