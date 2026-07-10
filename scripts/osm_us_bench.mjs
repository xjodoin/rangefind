#!/usr/bin/env node

// Cross-platform full-US OSM benchmark driver. Keep this corpus outside the
// example tree by default; RANGEFIND_OSM_US_ROOT can point at a large volume.

import { execFileSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(process.env.RANGEFIND_OSM_US_ROOT || ".cache/osm-us");
const scripts = dirname(fileURLToPath(import.meta.url));
const run = (script, args) => execFileSync(process.execPath, [script, ...args], { stdio: "inherit" });

run(resolve(scripts, "osm_fixture.mjs"), ["all", "--region=us", `--root=${root}`]);
run(resolve(scripts, "osm_geo_bench.mjs"), [`--root=${root}`]);
