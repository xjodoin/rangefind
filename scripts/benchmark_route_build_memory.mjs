#!/usr/bin/env node

import { createHash } from "node:crypto";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const option = (name, fallback) => {
  const prefix = `--${name}=`;
  const value = process.argv.slice(2).find(argument => argument.startsWith(prefix));
  return value ? value.slice(prefix.length) : fallback;
};

const rows = Number(option("rows", "1000"));
const columns = Number(option("columns", "1000"));
const modulePath = option("module", new URL("../src/route_graph_build.js", import.meta.url).pathname);
const expectedRoot = option("expect-root", "");
const expectedSha256 = option("expect-sha256", "");
const radixMinNodes = Number(option("radix-min-nodes", "4096"));
if (!Number.isSafeInteger(rows) || rows < 2 || !Number.isSafeInteger(columns) || columns < 2) {
  throw new Error("--rows and --columns must be integers of at least 2");
}

const { buildRouteGraph } = await import(pathToFileURL(modulePath));
const nodeCount = rows * columns;
const edgeCount = (rows * (columns - 1) + columns * (rows - 1)) * 2;
const nodeLat = new Int32Array(nodeCount);
const nodeLon = new Int32Array(nodeCount);
for (let row = 0; row < rows; row++) {
  for (let column = 0; column < columns; column++) {
    const node = row * columns + column;
    nodeLat[node] = 450000000 + row * 10;
    nodeLon[node] = -740000000 + column * 10;
  }
}

const edgeFrom = new Uint32Array(edgeCount);
const edgeTo = new Uint32Array(edgeCount);
let edge = 0;
const connect = (from, to) => {
  edgeFrom[edge] = from;
  edgeTo[edge++] = to;
  edgeFrom[edge] = to;
  edgeTo[edge++] = from;
};
for (let row = 0; row < rows; row++) {
  for (let column = 0; column < columns; column++) {
    const node = row * columns + column;
    if (column + 1 < columns) connect(node, node + 1);
    if (row + 1 < rows) connect(node, node + columns);
  }
}

const geomOffsets = new Uint32Array(edgeCount + 1);
const laneOffsets = new Uint32Array(edgeCount + 1);
for (let i = 0; i <= edgeCount; i++) {
  geomOffsets[i] = i;
  laneOffsets[i] = i;
}
const graph = {
  profile: "car",
  classes: ["residential"],
  names: [""],
  condRules: [],
  signs: [],
  nodeLat,
  nodeLon,
  edgeFrom,
  edgeTo,
  edgeWeightDs: new Uint32Array(edgeCount).fill(100),
  edgeDistDm: new Uint32Array(edgeCount).fill(1000),
  edgeName: new Uint32Array(edgeCount),
  edgeClass: new Uint8Array(edgeCount),
  edgeJunction: new Uint8Array(edgeCount),
  edgeSpeed: new Uint8Array(edgeCount).fill(50),
  edgeCond: new Uint8Array(edgeCount),
  edgeSign: new Uint32Array(edgeCount),
  edgeFlags: new Uint8Array(edgeCount),
  geomOffsets,
  geomBytes: new Uint8Array(edgeCount),
  laneOffsets,
  laneBytes: new Uint8Array(edgeCount)
};

const output = mkdtempSync(join(tmpdir(), "rangefind-route-build-memory-"));
try {
  globalThis.gc?.();
  const started = performance.now();
  const phases = [];
  const summary = buildRouteGraph(graph, output, {
    leafNodes: 1024,
    fanout: 8,
    topMaxCells: 8,
    shards: 4,
    overlayRadixMinNodes: radixMinNodes,
    releaseSource: true,
    collectGarbage: globalThis.gc,
    log: message => phases.push({
      message,
      elapsedMs: Math.round(performance.now() - started),
      rssMiB: Math.round(process.memoryUsage().rss / 1024 / 1024),
      externalMiB: Math.round(process.memoryUsage().external / 1024 / 1024)
    })
  });
  const elapsedMs = performance.now() - started;
  const maxRssMiB = process.resourceUsage().maxRSS / 1024;
  const hash = createHash("sha256");
  let outputBytes = 0;
  const visit = directory => {
    for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) visit(path);
      else {
        const relative = path.slice(output.length + 1);
        const bytes = readFileSync(path);
        outputBytes += statSync(path).size;
        hash.update(relative).update("\0").update(bytes);
      }
    }
  };
  visit(output);
  const outputSha256 = hash.digest("hex");
  if (expectedRoot && summary.rootFile !== expectedRoot) {
    throw new Error(`route root changed: expected ${expectedRoot}, got ${summary.rootFile}`);
  }
  if (expectedSha256 && outputSha256 !== expectedSha256) {
    throw new Error(`route output changed: expected ${expectedSha256}, got ${outputSha256}`);
  }
  console.log(JSON.stringify({
    rows,
    columns,
    nodeCount,
    edgeCount,
    rootFile: summary.rootFile,
    outputSha256,
    outputBytes,
    elapsedMs: Math.round(elapsedMs),
    maxRssMiB: Math.round(maxRssMiB),
    phases
  }));
} finally {
  rmSync(output, { recursive: true, force: true });
}
