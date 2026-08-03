#!/usr/bin/env node

// Copies a built rfroutegraph-v1 index into the OSM demo's public directory
// so the map client can open it at route-graph/. Build one first with:
//   node scripts/osm_road_graph.mjs luxembourg-latest.osm.pbf luxembourg.graph.bin
//   node scripts/route_bench.mjs build luxembourg.graph.bin bench/route/luxembourg-index --peak

import { cpSync, existsSync, rmSync } from "node:fs";
import { resolve } from "node:path";

const source = resolve(process.argv[2] || "bench/route/luxembourg-index");
const target = resolve("examples/osm-geo/public/route-graph");

if (!existsSync(resolve(source, "manifest.json"))) {
  console.error(`No route index at ${source} (missing manifest.json).`);
  console.error("Build one first; see the header of this script for the commands.");
  process.exit(1);
}

rmSync(target, { recursive: true, force: true });
cpSync(source, target, { recursive: true });
console.log(`Copied ${source} -> ${target}`);
