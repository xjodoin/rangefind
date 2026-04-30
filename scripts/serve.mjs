#!/usr/bin/env node

import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, resolve } from "node:path";
import { gzipSync } from "node:zlib";

const ROOT = resolve(process.argv[2] || "examples/basic/public");
const PORT = Number(process.argv[3] || process.env.PORT || 5178);
const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".gz": "application/gzip",
  ".bin": "application/octet-stream"
};
const GZIP_TYPES = new Set([".html", ".js", ".json"]);

function parseRange(header, size) {
  const match = /^bytes=(\d*)-(\d*)$/u.exec(String(header || ""));
  if (!match) return null;
  let start = match[1] ? Number(match[1]) : null;
  let end = match[2] ? Number(match[2]) : null;
  if (start == null) {
    if (!Number.isInteger(end) || end <= 0) return null;
    start = Math.max(0, size - end);
    end = size - 1;
  } else if (end == null) {
    end = size - 1;
  }
  if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || end < start || start >= size) return null;
  return { start, end: Math.min(end, size - 1) };
}

createServer(async (req, res) => {
  let path = decodeURIComponent(new URL(req.url, "http://x").pathname);
  if (path.endsWith("/")) path += "index.html";
  const file = resolve(ROOT, "." + path);
  if (!file.startsWith(ROOT)) return res.writeHead(403).end();
  try {
    const s = await stat(file);
    if (s.isDirectory()) return res.writeHead(404).end();
    const ext = extname(file);
    const headers = {
      "content-type": MIME[ext] || "application/octet-stream",
      "accept-ranges": "bytes",
      "cache-control": "no-store"
    };
    if (req.headers.range) {
      const range = parseRange(req.headers.range, s.size);
      if (!range) return res.writeHead(416, { ...headers, "content-range": `bytes */${s.size}` }).end();
      const body = (await readFile(file)).subarray(range.start, range.end + 1);
      return res.writeHead(206, {
        ...headers,
        "content-range": `bytes ${range.start}-${range.end}/${s.size}`,
        "content-length": body.length
      }).end(body);
    }
    let body = await readFile(file);
    if (GZIP_TYPES.has(ext) && (req.headers["accept-encoding"] || "").includes("gzip")) {
      body = gzipSync(body, { level: 6 });
      headers["content-encoding"] = "gzip";
    }
    res.writeHead(200, { ...headers, "content-length": body.length }).end(body);
  } catch {
    res.writeHead(404, { "content-type": "text/plain" }).end("Not found\n");
  }
}).listen(PORT, () => {
  console.log(`Serving ${ROOT}`);
  console.log(`http://localhost:${PORT}/`);
});
