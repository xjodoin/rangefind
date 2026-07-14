---
title: Sharded indexes
lede: Split a planet-sized corpus into regional indexes that build, update, and cache independently — and still behave as one engine with identical rankings.
description: Geographic index sharding in rangefind — the frozen scoring-stats artifact, query-time federation with bbox routing, multi-level shard scoping, incremental per-shard deltas, and the planet OSM workflow.
order: 12
---

One giant index has four walls: full rebuilds for every update, per-query
costs that grow with corpus size, builder memory ceilings, and a single blast
radius. Sharding removes all four — *if* results from different shards can
be merged honestly. That's the hard part, and it's the part rangefind
solves exactly.

## Exact cross-shard scoring

BM25 scores depend on corpus-wide statistics (document counts, average
field lengths, per-term document frequencies). Rangefind collects them once
over **all** shard inputs into a *scoring-stats artifact*, and every shard
build freezes against it:

```js
import { collectScoringStats } from "rangefind/scoring-stats";
import { writeShardedRootManifest } from "rangefind/shards";
import { build } from "rangefind/builder";

const { statsPath, stats } = await collectScoringStats({
  config, // your resolved rangefind config
  inputs: [
    { id: "quebec",  input: "quebec.jsonl" },
    { id: "ontario", input: "ontario.jsonl" }
  ],
  outDir: "scoring-stats"
});

// each shard: a normal build with { "scoringStats": statsPath } in its config
await build({ configPath: "quebec.config.json" });
await build({ configPath: "ontario.config.json" });

writeShardedRootManifest({
  outDir: "public/rangefind",
  shards: [
    { id: "quebec",  bbox: stats.inputs[0].bbox, groups: ["canada"] },
    { id: "ontario", bbox: stats.inputs[1].bbox, groups: ["canada"] }
  ],
  scoringStats: stats
});
```

The guarantee, asserted in the test suite: **a sharded index reproduces the
monolithic build's rankings exactly** — same result sets, scores equal to
1e-9. Measured on a 6.1M-place Québec corpus, building four shards
(including the stats pass) took 423 s versus 421 s for the monolith, at the
same total size.

## Query-time federation

`createSearch` on the root manifest returns one engine over all shards:

- **Lazy** — opening the index costs one small manifest; shard engines open
  on first use.
- **Geo-routed** — radius and box queries touch only shards whose coverage
  bbox can match; nearest-first uses an expanding shard front with an
  early-stop proof. Geo lanes on the sharded Québec index run **faster**
  and transfer **less** than the monolith.
- **Scoped on demand** — `shards: ["quebec"]` restricts any query; names
  resolve as shard ids *or* group labels (`"canada"` expands to every
  province, `"europe"` to a continent). Every shard is also a complete
  standalone index: point `createSearch` at `…/shards/quebec/` and skip
  federation entirely.
- **Composable** — a shard may itself be generational (nightly deltas), or
  even another sharded root (hierarchical federation across hosts); routing
  and merging recurse.

## Updating one region

Refresh a region's data, diff it against what the shard reflects, and ship
the changes as a [generational delta](../incremental-updates/) against the
same stats artifact — proven identical to rebuilding the shard from
scratch. A nightly OpenStreetMap region update uploads **kilobytes**, and
every other shard's CDN cache stays warm. Regenerate the stats artifact only
on major corpus shifts (it intentionally invalidates every shard, since
statistics are frozen corpus-wide).

## Planet OSM, concretely

The reference pipeline
([osm-rangefind-index](https://github.com/xjodoin/osm-rangefind-index))
builds all of OpenStreetMap as **310 shards** — countries, split to states
and provinces where large — from ~79 GB of Geofabrik extracts, on a single
machine's idle hours:

- deadline-aware runs that stop cleanly and resume from build checkpoints;
- nightly diffs shipped as per-region delta generations;
- reader-safe publishing to object storage (packs before manifests);
- steady-state disk of roughly a compressed corpus per region;
- total hosting cost on zero-egress object storage: **~$11/month**.

For the full design — including how `buildOsmShardedIndex` wraps all of the
above in one call — see the architecture document in the repository:
`docs/sharded-osm.md`.
