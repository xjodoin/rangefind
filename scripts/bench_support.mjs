import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { existsSync, readdirSync, statSync } from "node:fs";
import { extname, resolve } from "node:path";

export function parseArgs(argv, defaults = {}) {
  const args = { ...defaults };
  for (const arg of argv) {
    if (arg === "--json") args.json = true;
    else if (arg.startsWith("--root=")) args.root = arg.slice("--root=".length);
    else if (arg.startsWith("--base-path=")) args.basePath = arg.slice("--base-path=".length);
    else if (arg.startsWith("--docs=")) args.docs = arg.slice("--docs=".length);
    else if (arg.startsWith("--runs=")) args.runs = Number(arg.slice("--runs=".length)) || args.runs;
    else if (arg.startsWith("--size=")) args.size = Number(arg.slice("--size=".length)) || args.size;
    else if (arg.startsWith("--queries=")) args.queries = arg.slice("--queries=".length).split("|");
    else if (arg.startsWith("--known=")) args.known = Number(arg.slice("--known=".length)) || args.known;
    else if (arg.startsWith("--typos=")) args.typos = Number(arg.slice("--typos=".length)) || args.typos;
  }
  return args;
}

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

export async function serveStatic(root) {
  const absoluteRoot = resolve(root);
  const server = createServer(async (req, res) => {
    let path = decodeURIComponent(new URL(req.url, "http://x").pathname);
    if (path.endsWith("/")) path += "index.html";
    const file = resolve(absoluteRoot, "." + path);
    if (!file.startsWith(absoluteRoot)) return res.writeHead(403).end();
    try {
      const s = await stat(file);
      if (s.isDirectory()) return res.writeHead(404).end();
      const headers = {
        "content-type": extname(file) === ".json" ? "application/json" : "application/octet-stream",
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
      const body = await readFile(file);
      res.writeHead(200, { ...headers, "content-length": body.length }).end(body);
    } catch {
      res.writeHead(404).end();
    }
  });
  await new Promise(resolveListen => server.listen(0, "127.0.0.1", resolveListen));
  const { port } = server.address();
  return {
    url: `http://127.0.0.1:${port}/`,
    close: () => new Promise(resolveClose => server.close(resolveClose))
  };
}

export async function readJsonLines(path) {
  const text = await readFile(path, "utf8");
  return text.split(/\n/u).filter(line => line.trim()).map(line => JSON.parse(line));
}

export function createFetchMeter(match = /\/rangefind\//u, classify = () => "matched") {
  const nativeFetch = globalThis.fetch;
  const meter = { requests: 0, bytes: 0, by: {} };
  globalThis.fetch = async (input, init) => {
    const response = await nativeFetch(input, init);
    const url = String(input?.url || input);
    if (match.test(url)) {
      const bucket = classify(url, init, response) || "matched";
      if (!meter.by[bucket]) meter.by[bucket] = { requests: 0, bytes: 0 };
      meter.requests++;
      meter.by[bucket].requests++;
      const length = Number(response.headers.get("content-length") || 0);
      if (Number.isFinite(length) && length > 0) {
        meter.bytes += length;
        meter.by[bucket].bytes += length;
      }
    }
    return response;
  };
  return {
    snapshot() {
      return {
        requests: meter.requests,
        bytes: meter.bytes,
        by: Object.fromEntries(Object.entries(meter.by).map(([key, value]) => [key, { ...value }]))
      };
    },
    reset() {
      meter.requests = 0;
      meter.bytes = 0;
      meter.by = {};
    },
    restore() {
      globalThis.fetch = nativeFetch;
    }
  };
}

export function quantile(values, q) {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * q) - 1));
  return sorted[index];
}

export function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

export function kb(bytes) {
  return bytes / 1024;
}

export function dirStats(path, options = {}) {
  if (!existsSync(path)) return { files: 0, bytes: 0 };
  const s = statSync(path);
  if (s.isFile()) return { files: 1, bytes: s.size };
  if (!s.isDirectory()) return { files: 0, bytes: 0 };
  const skipNames = new Set(options.skipNames || []);
  let files = 0;
  let bytes = 0;
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    if (skipNames.has(entry.name)) continue;
    const child = dirStats(resolve(path, entry.name), options);
    files += child.files;
    bytes += child.bytes;
  }
  return { files, bytes };
}

export function fold(text) {
  return String(text || "")
    .normalize("NFKD")
    .toLowerCase()
    .replace(/[\u0300-\u036f]/gu, "")
    .replace(/œ/g, "oe")
    .replace(/æ/g, "ae");
}

export function mutateToken(token, seed) {
  if (token.length < 5) return token;
  const innerStart = token.length > 6 ? 1 : 0;
  const span = Math.max(1, token.length - innerStart - (token.length > 6 ? 1 : 0));
  const pos = innerStart + (seed % span);
  const op = seed % 4;
  if (op === 0) return token.slice(0, pos) + token.slice(pos + 1);
  if (op === 1 && pos < token.length - 1) return token.slice(0, pos) + token[pos + 1] + token[pos] + token.slice(pos + 2);
  if (op === 2) {
    const alphabet = "etaoinshrdlucmpgfbvyq";
    const replacement = alphabet[(alphabet.indexOf(token[pos]) + seed + 7) % alphabet.length] || "e";
    return token.slice(0, pos) + replacement + token.slice(pos + 1);
  }
  return token.slice(0, pos) + token[pos] + token.slice(pos);
}
