#!/usr/bin/env node

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { buildRouteGraph } from "../src/route_graph_build.js";
import { readRoadGraph } from "./osm_road_graph.mjs";

const [graphArgument, outputArgument, configArgument] = process.argv.slice(2);
if (!graphArgument || !outputArgument) {
  throw new Error("usage: benchmark_route_build_file.mjs <graph.bin> <output-dir> [worker-config.json]");
}
const graphPath = resolve(graphArgument);
const outputPath = resolve(outputArgument);
const config = configArgument ? JSON.parse(readFileSync(resolve(configArgument), "utf8")) : {};
const phases = [];
const started = performance.now();
const graph = await readRoadGraph(graphPath);
const loadedMs = performance.now() - started;
globalThis.gc?.();
const buildStarted = performance.now();
const summary = buildRouteGraph(graph, outputPath, {
  ...(config.buildOptions || config),
  releaseSource: true,
  collectGarbage: globalThis.gc,
  log: message => phases.push({
    message,
    elapsedMs: Math.round(performance.now() - buildStarted),
    rssMiB: Math.round(process.memoryUsage().rss / 1024 / 1024),
    externalMiB: Math.round(process.memoryUsage().external / 1024 / 1024)
  })
});
const expectedRoot = String(process.env.EXPECTED_ROUTE_ROOT || "").trim();
if (expectedRoot && summary.rootFile !== expectedRoot) {
  throw new Error(`route output changed: expected ${expectedRoot}, got ${summary.rootFile}`);
}
console.log(JSON.stringify({
  graphPath,
  outputPath,
  loadedMs: Math.round(loadedMs),
  buildMs: Math.round(performance.now() - buildStarted),
  maxRssMiB: Math.round(process.resourceUsage().maxRSS / 1024),
  rootFile: summary.rootFile,
  nodes: summary.nodes,
  edges: summary.edges,
  leaves: summary.leaves,
  phases
}));
