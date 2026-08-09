#!/usr/bin/env node

// Production smoke/latency benchmark for client-only inter-region routing.
// Run after at least two adjacent regional graphs have rfrouteportals-v1:
//   node scripts/route_federation_bench.mjs [catalog] [profile]

import { performance } from "node:perf_hooks";
import { openRouteCatalogUrl } from "../src/route_federation.js";

const catalog = process.argv[2] || "https://osm.rangefind.dev/routes/catalog.json";
const profile = process.argv[3] || "car";
const from = { lat: 45.5019, lon: -73.5674 }; // Montreal, Quebec
const to = { lat: 43.6532, lon: -79.3832 };   // Toronto, Ontario

async function measured(label, engine, geometry) {
  const started = performance.now();
  const result = await engine.route({ from, to, geometry });
  const milliseconds = performance.now() - started;
  if (!result.federated || result.regions?.length < 2 || !result.transitions?.length) {
    throw new Error(`${label}: query did not traverse federated regions.`);
  }
  if (!(result.seconds > 0) || (geometry && !result.geometry?.length)) {
    throw new Error(`${label}: incomplete route result.`);
  }
  return {
    label,
    milliseconds: Math.round(milliseconds * 10) / 10,
    seconds: result.seconds,
    regions: result.regions,
    portals: result.transitions.map(transition => transition.osmNodeId),
    requests: result.stats?.httpRequests || 0,
    fetchedMiB: Math.round(((result.stats?.bytesFetched || 0) / 1024 / 1024) * 100) / 100
  };
}

const cold = await measured("cold-no-geometry", await openRouteCatalogUrl(catalog, { profile }), false);
const warmEngine = await openRouteCatalogUrl(catalog, { profile });
await warmEngine.route({ from, to, geometry: false });
const warm = await measured("warm-no-geometry", warmEngine, false);
const geometry = await measured("warm-with-geometry", warmEngine, true);
console.log(JSON.stringify({ catalog, profile, results: [cold, warm, geometry] }, null, 2));
