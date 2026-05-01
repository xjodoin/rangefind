#!/usr/bin/env node

import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { resolve, extname } from "node:path";
import { createSearch } from "../dist/runtime.browser.js";

const root = resolve("examples/basic/public");
const server = createServer(async (req, res) => {
  let path = decodeURIComponent(new URL(req.url, "http://x").pathname);
  if (path.endsWith("/")) path += "index.html";
  const file = resolve(root, "." + path);
  if (!file.startsWith(root)) return res.writeHead(403).end();
  try {
    const s = await stat(file);
    const headers = {
      "content-type": extname(file) === ".json" ? "application/json" : "application/octet-stream",
      "accept-ranges": "bytes"
    };
    if (req.headers.range) {
      const match = /^bytes=(\d+)-(\d+)$/u.exec(req.headers.range);
      const start = Number(match[1]);
      const end = Number(match[2]);
      const body = (await readFile(file)).subarray(start, end + 1);
      return res.writeHead(206, {
        ...headers,
        "content-range": `bytes ${start}-${end}/${s.size}`,
        "content-length": body.length
      }).end(body);
    }
    const body = await readFile(file);
    res.writeHead(200, { ...headers, "content-length": body.length }).end(body);
  } catch {
    res.writeHead(404).end();
  }
});

await new Promise(resolveListen => server.listen(0, resolveListen));
const port = server.address().port;
try {
  const engine = await createSearch({ baseUrl: `http://localhost:${port}/rangefind/` });
  const result = await engine.search({ q: "range static search", size: 3 });
  if (!result.results.length) throw new Error("No search results returned");
  if (!/Range|Static|search/i.test(result.results[0].title)) throw new Error("Unexpected top result");
  console.log(`Smoke OK: ${result.total} results, top="${result.results[0].title}"`);
} finally {
  server.close();
}
