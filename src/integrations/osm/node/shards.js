import { mkdirSync, writeFileSync } from "node:fs";
import { availableParallelism } from "node:os";
import { resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { build } from "../../../builder.js";
import { readConfig } from "../../../config.js";
import { collectScoringStats } from "../../../scoring_stats.js";
import { writeShardedRootManifest } from "../../../index_shards.js";
import { writeTextRoutingIndex } from "../../../text_routing.js";
import { writeSuggestRoutingIndex } from "../../../suggest_routing.js";
import { createNodeSearch } from "../../../runtime.node.js";
import { buildCategoryLexiconArtifact } from "../category_lexicon.js";
import { createOsmIndexConfig } from "../schema.js";

// Sharded OSM build: one Rangefind index per region, every shard consuming
// one frozen scoring-stats artifact so cross-shard rankings merge exactly.
//
// output/
//   manifest.json         sharded root: shard list + coverage bboxes
//   shards/<id>/…         one complete index per region (CDN-publishable)
//   _scoring-stats/       build-time artifact — not needed at query time
//   _shard-configs/       generated per-shard build configs
//
// Region shards rebuild independently: refresh a region's JSONL, re-run its
// shard build, rewrite the root manifest. Regenerate the stats artifact only
// when the corpus shifts enough to matter (idf drifts slowly at this scale);
// the build fingerprint tracks the stats file, so unchanged shards resume
// cleanly.
export async function buildOsmShardedIndex(options = {}) {
  const started = performance.now();
  const output = resolve(options.output || "public/rangefind");
  const shardSpecs = (options.shards || []).map((shard, index) => ({
    id: String(shard.id || `shard-${index}`),
    input: resolve(shard.input),
    region: shard.region,
    overrides: shard.overrides || null
  }));
  if (!shardSpecs.length) throw new Error("Rangefind OSM shards: pass shards: [{ id, input }].");
  const workerCount = Math.max(1, Math.floor(Number(options.workerCount
    || Math.max(1, availableParallelism() - 2))));
  const configDir = resolve(output, "_shard-configs");
  mkdirSync(configDir, { recursive: true });

  function configFor(spec, scoringStatsPath) {
    return createOsmIndexConfig({
      region: spec.region ?? options.region,
      rqa: Boolean(options.rqa),
      workerCount,
      buildProgressLogMs: options.buildProgressLogMs,
      input: spec.input,
      output: resolve(output, "shards", spec.id),
      overrides: {
        ...(options.overrides || {}),
        ...(spec.overrides || {}),
        ...(scoringStatsPath ? { scoringStats: scoringStatsPath } : {})
      }
    });
  }

  // The stats pass runs the exact analysis profile the shard builds use.
  const templatePath = resolve(configDir, "_stats-template.json");
  writeFileSync(templatePath, JSON.stringify(configFor(shardSpecs[0], ""), null, 2));
  const templateConfig = await readConfig(templatePath);
  const stats = await collectScoringStats({
    config: templateConfig,
    inputs: shardSpecs.map(spec => ({ id: spec.id, input: spec.input })),
    outDir: resolve(output, "_scoring-stats"),
    log: line => console.log(`Rangefind: ${line}`)
  });

  const shardEntries = [];
  for (const spec of shardSpecs) {
    const configPath = resolve(configDir, `${spec.id}.json`);
    writeFileSync(configPath, JSON.stringify(configFor(spec, stats.statsPath), null, 2));
    console.log(`Rangefind: building shard ${spec.id}`);
    await build({ configPath });
    const inputStats = stats.stats.inputs.find(item => item.id === spec.id);
    shardEntries.push({
      id: spec.id,
      path: `shards/${spec.id}/`,
      bbox: inputStats?.bbox || null
    });
  }

  // Text routing directory: term → shard-set lookups let federated text
  // queries open only the shards that can satisfy them (fan-out killer).
  let textRouting = null;
  if (options.textRouting !== false) {
    console.log("Rangefind: building text routing directory");
    textRouting = await writeTextRoutingIndex({
      outDir: output,
      shards: shardEntries.map(entry => ({ id: entry.id, dir: resolve(output, entry.path) }))
    });
    console.log(`Rangefind: text routing ready — ${textRouting.term_count.toLocaleString()} terms`);
  }

  // Root suggest artifact: the merged authority lexicon answers federated
  // autocomplete and locality resolution from the root instead of fanning
  // out to every shard's authority sidecar per keystroke.
  let suggestRouting = null;
  if (options.suggestRouting !== false) {
    console.log("Rangefind: building suggest routing artifact");
    suggestRouting = await writeSuggestRoutingIndex({
      outDir: output,
      shards: shardEntries.map(entry => ({ id: entry.id, dir: resolve(output, entry.path) }))
    });
    console.log(`Rangefind: suggest routing ready — ${suggestRouting.keys.toLocaleString()} lexicon keys`);
  }

  // Category lexicon: the merged `type` facet vocabulary across every shard,
  // joined with the package's alias table and embedded in the root manifest.
  // Bare category words ("cinema", "boulangerie") then gate on the corpus's
  // own vocabulary at query time — no fetch, no hardcoded list.
  let categoryLexicon = null;
  if (options.categoryLexicon !== false) {
    console.log("Rangefind: merging category lexicon");
    const typeCounts = new Map();
    for (const entry of shardEntries) {
      const engine = await createNodeSearch({ source: resolve(output, entry.path) });
      for (const item of await engine.loadFacetValues("type")) {
        if (!item?.value) continue;
        typeCounts.set(item.value, (typeCounts.get(item.value) || 0) + Number(item.n || 0));
      }
    }
    categoryLexicon = buildCategoryLexiconArtifact(
      [...typeCounts].map(([value, n]) => ({ value, n }))
    );
    console.log(`Rangefind: category lexicon ready — ${categoryLexicon.types.length} types, ${Object.keys(categoryLexicon.aliases).length} aliases`);
  }

  const rootManifest = writeShardedRootManifest({
    outDir: output,
    shards: shardEntries,
    scoringStats: stats.stats,
    textRouting,
    suggestRouting,
    ...(categoryLexicon ? { extra: { category_lexicon: categoryLexicon } } : {})
  });
  const seconds = Math.round((performance.now() - started) / 100) / 10;
  console.log(`Rangefind: sharded index ready — ${rootManifest.shards.length} shards, ${rootManifest.total.toLocaleString()} docs, ${seconds}s`);
  return { output, rootManifest, statsPath: stats.statsPath, seconds };
}
