// Shared helpers for the competitive static-search benchmark.
import { createReadStream } from "node:fs";
import { createInterface } from "node:readline";
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

// Load the first `n` article records from the cached frwiki fixture. Each record
// is normalized to { id, title, url, body } — the fields every engine indexes.
export async function loadCorpus(path, n) {
  const docs = [];
  const rl = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    try {
      const r = JSON.parse(line);
      if (!r.title || !r.body) continue;
      docs.push({ id: String(r.id), title: String(r.title), url: String(r.url || ""), body: String(r.body) });
      if (docs.length >= n) break;
    } catch { /* skip */ }
  }
  return docs;
}

// Known-item queries: pick documents with distinctive titles; the query is the
// title, the correct answer is that document. Analyzer-agnostic and fair — it
// asks each engine "given this title, do you retrieve this article?".
export function knownItemQueries(docs, count, seed = 0x1234) {
  let s = seed >>> 0;
  const rng = () => (s = (Math.imul(s, 1664525) + 1013904223) >>> 0) / 0x100000000;
  const eligible = docs.filter(d => d.title.length >= 8 && d.title.length <= 48 && /\s/.test(d.title));
  const picked = [];
  const used = new Set();
  while (picked.length < count && used.size < eligible.length) {
    const i = Math.floor(rng() * eligible.length);
    if (used.has(i)) continue;
    used.add(i);
    picked.push({ q: eligible[i].title, target: eligible[i].id });
  }
  return picked;
}

// recall@k and MRR@k over ranked id lists.
export function scoreQuality(queries, ranker, k = 10) {
  let hit = 0;
  let rrSum = 0;
  for (const { q, target } of queries) {
    const ids = ranker(q).slice(0, k);
    const rank = ids.indexOf(target);
    if (rank >= 0) { hit++; rrSum += 1 / (rank + 1); }
  }
  return { recall: hit / queries.length, mrr: rrSum / queries.length, n: queries.length };
}

// A range-capable static server that counts total response bytes, so we can
// attribute transfer to a query window.
export async function byteCountingServer(root) {
  let bytes = 0;
  let requests = 0;
  const server = createServer(async (rq, rs) => {
    try {
      const u = new URL(rq.url, "http://x");
      const p = resolve(root, `.${decodeURIComponent(u.pathname)}`);
      if (!p.startsWith(resolve(root))) return void rs.writeHead(403).end();
      const data = await readFile(p);
      const r = rq.headers.range?.match(/^bytes=(\d+)-(\d+)$/);
      if (r) {
        const a = +r[1], b = Math.min(+r[2], data.length - 1);
        const slice = data.subarray(a, b + 1);
        bytes += slice.length; requests++;
        rs.writeHead(206, { "Accept-Ranges": "bytes", "Content-Length": String(slice.length), "Content-Range": `bytes ${a}-${b}/${data.length}` });
        return void rs.end(slice);
      }
      bytes += data.length; requests++;
      rs.writeHead(200, { "Content-Length": String(data.length) });
      rs.end(data);
    } catch { rs.writeHead(404).end(); }
  });
  await new Promise(done => server.listen(0, "127.0.0.1", done));
  return {
    baseUrl: `http://127.0.0.1:${server.address().port}/`,
    reset() { bytes = 0; requests = 0; },
    read() { return { bytes, requests }; },
    close: () => new Promise(d => server.close(d))
  };
}

export function pct(sorted, p) {
  return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] : 0;
}
export const nowMs = () => Number(process.hrtime.bigint()) / 1e6;
