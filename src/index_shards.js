import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { resolve } from "node:path";

// Sharded index root: a corpus split into N independently built Rangefind
// indexes (typically by geography), published side by side and federated at
// query time. The root manifest is intentionally tiny — shard descriptors
// plus the field metadata the merge layer needs — so opening a planet-scale
// sharded index costs one small fetch; shard engines open lazily as queries
// route into them.
//
// Score comparability across shards comes from the frozen scoring-stats
// artifact every shard build consumes (see scoring_stats.js), the same
// invariant generational deltas rely on.

export const SHARDED_INDEX_FORMAT = "rangefind-sharded-v1";

export { TEXT_ROUTING_FORMAT, writeShardTermSet, writeTextRoutingIndex } from "./text_routing.js";

function normalizedShardPath(path) {
  const trimmed = String(path || "").replace(/^\.?\//u, "");
  return trimmed.endsWith("/") || trimmed === "" ? trimmed : `${trimmed}/`;
}

function normalizedBbox(bbox) {
  if (!Array.isArray(bbox) || bbox.length !== 4 || bbox.some(value => !Number.isFinite(Number(value)))) return null;
  return bbox.map(Number);
}

// Writes manifest.json/manifest.min.json for a sharded root.
// shards: [{ id, path, bbox? }] with paths relative to outDir; each must
// already contain a built index (single or generational).
// textRouting: the block returned by writeTextRoutingIndex (text_routing.js);
// when present, text queries route to the shards that contain their terms.
export function writeShardedRootManifest({ outDir, shards, scoringStats = null, textRouting = null, extra = {} }) {
  const root = resolve(outDir);
  mkdirSync(root, { recursive: true });
  const entries = (shards || []).map((shard, index) => {
    const path = normalizedShardPath(shard.path ?? `shards/${shard.id}/`);
    const manifestName = shard.manifest || "manifest.min.json";
    const manifest = JSON.parse(readFileSync(resolve(root, path, manifestName), "utf8"));
    return {
      manifest,
      entry: {
        id: String(shard.id || `shard-${index}`),
        path,
        manifest: manifestName,
        total: manifest.total || 0,
        bbox: normalizedBbox(shard.bbox),
        // Hierarchy labels ("canada", "north-america", …): queries scope to
        // a whole group with shards: ["canada"] instead of listing members.
        ...(Array.isArray(shard.groups) && shard.groups.length
          ? { groups: shard.groups.map(String) }
          : {})
      }
    };
  });
  if (!entries.length) throw new Error("Rangefind sharded index: no shards.");
  const first = entries[0].manifest;
  const rootManifest = {
    format: SHARDED_INDEX_FORMAT,
    built_at: new Date().toISOString(),
    // Provenance (attribution, license, generator, …): shards share one
    // corpus family, so the first shard's meta speaks for the root unless
    // the caller overrides it via `extra.meta`.
    ...(first.meta ? { meta: first.meta } : {}),
    features: { shards: true },
    total: entries.reduce((sum, item) => sum + (item.entry.total || 0), 0),
    // The merge layer parses sort plans and geo params without opening a
    // shard first; shards share one schema, so the first speaks for all.
    numbers: first.numbers || [],
    booleans: first.booleans || [],
    geo: first.geo || null,
    ...(scoringStats ? {
      scoring_stats: {
        total: scoringStats.total,
        df_terms: scoringStats.df_terms || 0
      }
    } : {}),
    ...(textRouting ? { text_routing: textRouting } : {}),
    shards: entries.map(item => item.entry),
    ...extra
  };
  const payload = JSON.stringify(rootManifest);
  writeFileSync(resolve(root, "manifest.json"), payload);
  writeFileSync(resolve(root, "manifest.min.json"), payload);
  return rootManifest;
}
