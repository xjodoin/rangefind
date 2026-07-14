import { copyFileSync, existsSync, mkdirSync, writeFileSync } from "node:fs";
import { availableParallelism } from "node:os";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { build } from "../../../builder.js";
import { createOsmIndexConfig } from "../schema.js";

export * from "../documents.js";
export * from "../schema.js";
export * from "./rqa.js";
export * from "./shards.js";

export const OSM_DEMO_VIEWS = Object.freeze({
  luxembourg: { center: [6.13, 49.61], zoom: 11 },
  quebec: { center: [-73.6, 45.55], zoom: 12 },
  switzerland: { center: [8.23, 46.82], zoom: 8 },
  us: { center: [-98.58, 39.83], zoom: 4 }
});

export function writeOsmSite(options = {}) {
  const root = resolve(options.root || "examples/osm-geo");
  const region = options.region || "luxembourg";
  const rqa = Boolean(options.rqa);
  const workerCount = options.workerCount
    || Math.max(1, availableParallelism() - 2);
  const config = createOsmIndexConfig({
    region,
    rqa,
    workerCount,
    buildProgressLogMs: options.buildProgressLogMs,
    input: options.input,
    output: options.output,
    overrides: options.config
  });
  const configPath = resolve(root, "rangefind.config.json");
  const publicRoot = resolve(root, "public");
  mkdirSync(publicRoot, { recursive: true });
  writeFileSync(configPath, JSON.stringify(config, null, 2));
  writeFileSync(
    resolve(publicRoot, "osm-demo.json"),
    JSON.stringify({ region, rqa, ...(options.demoView || OSM_DEMO_VIEWS[region] || {}) }, null, 2)
  );
  const bundles = [
    [resolve(options.runtimeBundlePath || "dist/runtime.browser.js"), resolve(publicRoot, "runtime.browser.js")],
    [resolve(options.osmBundlePath || "dist/osm.browser.js"), resolve(publicRoot, "osm.browser.js")]
  ];
  for (const [source, target] of bundles) {
    if (existsSync(source)) copyFileSync(source, target);
  }
  return { config, configPath, publicRoot };
}

export async function buildOsmIndex(options = {}) {
  const site = writeOsmSite(options);
  const started = performance.now();
  const result = await build({ configPath: site.configPath });
  return {
    ...site,
    result,
    seconds: Math.round((performance.now() - started) / 100) / 10
  };
}
