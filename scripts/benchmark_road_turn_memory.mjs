#!/usr/bin/env node

import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";

const option = name => {
  const prefix = `--${name}=`;
  const item = process.argv.slice(2).find(value => value.startsWith(prefix));
  return item ? item.slice(prefix.length) : null;
};

const spokes = Number(option("spokes") || 1500);
const modulePath = option("module") || new URL("./osm_road_graph.mjs", import.meta.url).pathname;
if (!Number.isSafeInteger(spokes) || spokes < 2) {
  throw new Error("--spokes must be an integer of at least 2");
}

const { expandTurnCosts } = await import(pathToFileURL(modulePath));
const nodeLat = new Int32Array(spokes + 1);
const nodeLon = new Int32Array(spokes + 1);
const edgeCount = spokes * 2;
const edgeFrom = new Uint32Array(edgeCount);
const edgeTo = new Uint32Array(edgeCount);
const edgeWay = new Float64Array(edgeCount);
for (let i = 0; i < spokes; i++) {
  const leaf = i + 1;
  const angle = i * Math.PI * 2 / spokes;
  nodeLat[leaf] = Math.round(Math.sin(angle) * 100000);
  nodeLon[leaf] = Math.round(Math.cos(angle) * 100000);
  edgeFrom[i * 2] = leaf;
  edgeTo[i * 2] = 0;
  edgeFrom[i * 2 + 1] = 0;
  edgeTo[i * 2 + 1] = leaf;
  edgeWay[i * 2] = i + 1;
  edgeWay[i * 2 + 1] = i + 1;
}

const zeros32 = new Uint32Array(edgeCount);
const zeros8 = new Uint8Array(edgeCount);
const offsets = new Uint32Array(edgeCount + 1);
for (let i = 0; i <= edgeCount; i++) offsets[i] = i;
const context = {
  restrictions: [],
  nodeIndex: new Map(),
  nodeLat,
  nodeLon,
  edgeFrom,
  edgeTo,
  edgeWeightDs: new Uint32Array(edgeCount).fill(100),
  edgeDistDm: new Uint32Array(edgeCount).fill(1000),
  edgeName: zeros32,
  edgeWay,
  edgeClass: zeros8,
  edgeJunction: zeros8,
  edgeSpeed: zeros8,
  edgeCond: zeros8,
  edgeSign: zeros32,
  edgeFlags: zeros8,
  geomOffsets: offsets,
  geomBytes: { data: new Uint8Array(edgeCount), length: edgeCount },
  laneOffsets: null,
  laneBytes: null,
  log: () => {}
};

const started = performance.now();
const graph = expandTurnCosts(context, {
  uturn: 150,
  left: 40,
  right: 15,
  slightLeft: 20,
  slightRight: 8
});
const elapsedMs = performance.now() - started;
const maxRssKiB = process.resourceUsage().maxRSS;

// Hash the format-level values without converting a full legacy JS column at
// once. This makes the same command useful for compatibility checks against
// old builders without turning the digest itself into the memory peak.
const digest = createHash("sha256");
const columns = [
  ["nodeLat", graph.nodeLat, Int32Array],
  ["nodeLon", graph.nodeLon, Int32Array],
  ["edgeFrom", graph.edgeFrom, Uint32Array],
  ["edgeTo", graph.edgeTo, Uint32Array],
  ["edgeWeightDs", graph.edgeWeightDs, Uint32Array],
  ["edgeDistDm", graph.edgeDistDm, Uint32Array],
  ["edgeName", graph.edgeName, Uint32Array],
  ["edgeClass", graph.edgeClass, Uint8Array],
  ["edgeJunction", graph.edgeJunction, Uint8Array],
  ["edgeSpeed", graph.edgeSpeed, Uint8Array],
  ["edgeCond", graph.edgeCond, Uint8Array],
  ["edgeSign", graph.edgeSign, Uint32Array],
  ["edgeFlags", graph.edgeFlags, Uint8Array],
  ["geomOffsets", graph.geomOffsets, Uint32Array],
  ["geomBytes", graph.geomBytes, Uint8Array],
  ["laneOffsets", graph.laneOffsets, Uint32Array],
  ["laneBytes", graph.laneBytes, Uint8Array]
];
for (const [name, source, Type] of columns) {
  const values = source?.view ? source.view() : source;
  digest.update(`${name}:${values.length}:`);
  if (values instanceof Type) {
    digest.update(Buffer.from(values.buffer, values.byteOffset, values.byteLength));
    continue;
  }
  const chunk = new Type(Math.min(values.length, 65536));
  for (let offset = 0; offset < values.length; offset += chunk.length) {
    const length = Math.min(chunk.length, values.length - offset);
    for (let i = 0; i < length; i++) chunk[i] = values[offset + i];
    digest.update(Buffer.from(chunk.buffer, 0, length * Type.BYTES_PER_ELEMENT));
  }
}

console.log(JSON.stringify({
  spokes,
  baseEdges: edgeCount,
  expandedEdges: graph.edgeFrom.length,
  elapsedMs: Math.round(elapsedMs),
  maxRssMiB: Math.round(maxRssKiB / 1024),
  sha256: digest.digest("hex")
}));
