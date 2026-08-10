#!/usr/bin/env node

// Production smoke/latency benchmark for client-only inter-region routing.
// Run after at least two adjacent regional graphs have rfrouteportals-v2:
//   node scripts/route_federation_bench.mjs [catalog] [profile]

import { performance } from "node:perf_hooks";
import { openRouteCatalogUrl } from "../src/route_federation.js";

const catalog = process.argv[2] || "https://osm.rangefind.dev/routes/catalog.json";
const profile = process.argv[3] || "car";
const from = { lat: 45.5019, lon: -73.5674 }; // Montreal, Quebec
const to = { lat: 43.6532, lon: -79.3832 };   // Toronto, Ontario
let network = { requests: 0, bytes: 0, cache: {} };

const measuredFetch = async (...args) => {
  network.requests++;
  const response = await fetch(...args);
  const cache = response.headers.get("cf-cache-status") || "none";
  network.cache[cache] = (network.cache[cache] || 0) + 1;
  return new Proxy(response, {
    get(target, property) {
      if (property === "arrayBuffer") return async () => {
        const value = await target.arrayBuffer();
        network.bytes += value.byteLength;
        return value;
      };
      if (property === "json") return async () => {
        const value = await target.arrayBuffer();
        network.bytes += value.byteLength;
        return JSON.parse(new TextDecoder().decode(value));
      };
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    }
  });
};

const resetNetwork = () => { network = { requests: 0, bytes: 0, cache: {} }; };

async function measured(label, engine, geometry) {
  resetNetwork();
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
    requests: network.requests,
    fetchedMiB: Math.round((network.bytes / 1024 / 1024) * 100) / 100,
    cache: { ...network.cache },
    routeRangeRequestsCumulative: result.stats?.httpRequests || 0,
    routeRangeOverfetchMiBCumulative: Math.round(((result.stats?.rangeOverfetchBytes || 0) / 1024 / 1024) * 100) / 100
  };
}

const cold = await measured("cold-no-geometry", await openRouteCatalogUrl(catalog, { profile, fetch: measuredFetch }), false);
const warmEngine = await openRouteCatalogUrl(catalog, { profile, fetch: measuredFetch });
await warmEngine.route({ from, to, geometry: false });
const warm = await measured("warm-no-geometry", warmEngine, false);
const geometry = await measured("warm-with-geometry", warmEngine, true);
console.log(JSON.stringify({ catalog, profile, results: [cold, warm, geometry] }, null, 2));
