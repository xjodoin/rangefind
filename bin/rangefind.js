#!/usr/bin/env node

function usage() {
  console.log(`rangefind

Usage:
  rangefind build <dir> [--output <dir>] [--base-url <url>] [--root <dir>] [--enrich <module.mjs>]
  rangefind build --config path/to/rangefind.config.json [--update | --compact]
  rangefind search  <index> <query...> [options]
  rangefind suggest <index> <prefix>   [--size N] [--json]
  rangefind count   <index> <query...> [--shards a,b]
  rangefind info    <index> [--json]

<index> is a local index directory or an http(s) index URL — single,
generational, or sharded indexes all work.

Commands:
  build   Build a static range-packed search index.

          With a positional <dir>, crawl that built static site: extract
          text from every .html/.htm file and index it directly.
            --output    Output directory (default: <dir>/rangefind).
            --base-url  URL prefix or origin for result URLs (default: "/").
            --root      Directory whose relative paths define ids/URLs
                        (default: <dir>).
            --enrich    Path to an ES module whose default export is an
                        async function(docs) run on the crawled documents
                        before indexing (add embeddings, metadata, ...).
                        The module may also export "config": overrides
                        merged into the generated build config.

          With --config, build from a JSONL corpus described by the config.
            --update    Treat the config's input as a delta (new or replaced
                        documents) added as a new generation over the output.
            --compact   Fold a generational index back into one index: a full
                        rebuild (the input must be the FULL corpus) that then
                        removes the old generation directories.

  search  Query an index.
            --size N          Results per page (default 10).
            --page N          Page number.
            --facets a,b      Count these facets for the query.
            --filter k=v      Facet filter (repeatable). Numeric ranges:
                              k=min..max (either side optional).
            --sort field      Sort by a number/boolean field ("-field" desc).
            --near lat,lon[,radiusMeters]
                              Geo query; nearest-first when the query text is
                              empty or no radius is given.
            --box minLat,minLon,maxLat,maxLon
                              Geo bounding box (e.g. a map viewport).
            --shards a,b      Sharded indexes: shard ids or group labels.
            --json            Raw JSON response instead of formatted output.

  suggest Search-as-you-type autocomplete for a prefix.
  count   Exact match count for a text query.
  info    Index metadata: totals, build time, provenance, features, shards.

MCP:      an MCP server over any index ships as the rangefind-mcp package:
            npx rangefind-mcp --index docs=./public/rangefind
`);
}

function parseArgs(argv) {
  const args = { positionals: [], filters: [] };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    const take = name => (arg === `--${name}` ? argv[++i] : arg.startsWith(`--${name}=`) ? arg.slice(name.length + 3) : undefined);
    let value;
    if ((value = take("config")) !== undefined) args.config = value;
    else if ((value = take("output")) !== undefined) args.output = value;
    else if ((value = take("base-url")) !== undefined) args.baseUrl = value;
    else if ((value = take("root")) !== undefined) args.root = value;
    else if ((value = take("enrich")) !== undefined) args.enrich = value;
    else if ((value = take("size")) !== undefined) args.size = Number(value);
    else if ((value = take("page")) !== undefined) args.page = Number(value);
    else if ((value = take("facets")) !== undefined) args.facets = value.split(",").map(s => s.trim()).filter(Boolean);
    else if ((value = take("filter")) !== undefined) args.filters.push(value);
    else if ((value = take("sort")) !== undefined) args.sort = value;
    else if ((value = take("near")) !== undefined) args.near = value;
    else if ((value = take("box")) !== undefined) args.box = value;
    else if ((value = take("shards")) !== undefined) args.shards = value.split(",").map(s => s.trim()).filter(Boolean);
    else if (arg === "--json") args.json = true;
    else if (arg === "--update") args.update = true;
    else if (arg === "--compact") args.compact = true;
    else if (arg === "--help" || arg === "-h") args.help = true;
    else if (arg.startsWith("--")) args.unknown = arg;
    else args.positionals.push(arg);
  }
  return args;
}

function fatal(message) {
  console.error(message);
  usage();
  process.exit(1);
}

function parseFilters(pairs) {
  // Engine filter shape: { facets: {k: [v]}, numbers: {k: {min, max}}, booleans: {k: bool} }.
  const filters = { facets: {}, numbers: {}, booleans: {} };
  for (const pair of pairs) {
    const eq = pair.indexOf("=");
    if (eq <= 0) fatal(`--filter expects key=value, got "${pair}"`);
    const key = pair.slice(0, eq);
    const value = pair.slice(eq + 1);
    const range = value.match(/^(-?[\d.]+)?\.\.(-?[\d.]+)?$/);
    if (range && (range[1] !== undefined || range[2] !== undefined)) {
      filters.numbers[key] = {
        ...(range[1] !== undefined ? { min: Number(range[1]) } : {}),
        ...(range[2] !== undefined ? { max: Number(range[2]) } : {})
      };
    } else if (value === "true" || value === "false") {
      filters.booleans[key] = value === "true";
    } else {
      filters.facets[key] = [...(filters.facets[key] || []), value];
    }
  }
  return filters;
}

function parseGeo(args, hasQuery) {
  if (!args.near && !args.box) return undefined;
  if (args.near && args.box) fatal("Pass either --near or --box, not both.");
  if (args.near) {
    const [lat, lon, radiusMeters] = args.near.split(",").map(Number);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) fatal(`--near expects lat,lon[,radiusMeters], got "${args.near}"`);
    const near = { lat, lon, ...(Number.isFinite(radiusMeters) ? { radiusMeters } : {}) };
    // Nearest-first when browsing, or when no radius bounds the query.
    const sort = !hasQuery || !Number.isFinite(radiusMeters) ? "distance" : undefined;
    return { near, ...(sort ? { sort } : {}) };
  }
  const [minLat, minLon, maxLat, maxLon] = args.box.split(",").map(Number);
  if ([minLat, minLon, maxLat, maxLon].some(v => !Number.isFinite(v))) {
    fatal(`--box expects minLat,minLon,maxLat,maxLon, got "${args.box}"`);
  }
  return { box: { minLat, minLon, maxLat, maxLon } };
}

async function openIndex(ref) {
  if (!ref) fatal("Missing index (a local directory or an http(s) index URL).");
  const { createNodeSearch } = await import("../src/runtime.node.js");
  return createNodeSearch({ baseUrl: ref });
}

function formatResult(result, rank) {
  const score = result.distanceMeters != null
    ? `${Math.round(result.distanceMeters)}m`
    : result.score != null ? String(Math.round(result.score * 100) / 100) : "";
  const title = result.title ?? result.name ?? result.id;
  const where = [result.shard, result.url].filter(Boolean).join("  ");
  const lines = [`${String(rank).padStart(2)}. ${title}  ${score ? `(${score})` : ""}`];
  if (where) lines.push(`    ${where}`);
  const excerpt = result.excerpt || result.description;
  if (excerpt) lines.push(`    ${String(excerpt).replace(/\s+/g, " ").slice(0, 160)}`);
  return lines.join("\n");
}

for (const event of ["unhandledRejection", "uncaughtException"]) {
  process.on(event, error => {
    console.error(error?.message || String(error));
    process.exit(1);
  });
}

const [command, ...rest] = process.argv.slice(2);
const args = parseArgs(rest);

if (!command || args.help) {
  usage();
  process.exit(command ? 0 : 1);
}
if (args.unknown) fatal(`Unknown option: ${args.unknown}`);

if (command === "build") {
  const dir = args.positionals[0];
  if (dir && args.config) fatal("Pass either a directory to crawl or --config, not both.");
  if (dir) {
    const { buildFromCrawl } = await import("../src/crawler.js");
    await buildFromCrawl({
      root: args.root || dir,
      scanDir: dir,
      output: args.output || `${dir.replace(/\/+$/, "")}/rangefind`,
      baseUrl: args.baseUrl || "/",
      enrich: args.enrich
    });
  } else if (args.config) {
    const { build } = await import("../src/builder.js");
    await build({ configPath: args.config, update: !!args.update, compact: !!args.compact });
  } else {
    fatal("Missing directory to crawl or --config");
  }
} else if (command === "search") {
  const [ref, ...words] = args.positionals;
  const q = words.join(" ");
  const engine = await openIndex(ref);
  const params = {
    q,
    size: args.size,
    page: args.page,
    ...(args.facets ? { facets: args.facets } : {}),
    ...(args.filters.length ? { filters: parseFilters(args.filters) } : {}),
    ...(args.sort ? { sort: args.sort } : {}),
    ...(args.shards ? { shards: args.shards } : {})
  };
  const geo = parseGeo(args, Boolean(q));
  if (geo) params.geo = geo;
  const started = performance.now();
  const response = await engine.search(params);
  const ms = Math.round(performance.now() - started);
  if (args.json) {
    console.log(JSON.stringify(response, null, 2));
  } else {
    console.log(`${response.results.length} of ${response.approximate ? "≈" : ""}${response.total} results · ${ms} ms${response.correctedQuery ? ` · corrected to “${response.correctedQuery}”` : ""}`);
    response.results.forEach((result, i) => console.log(formatResult(result, (params.page - 1 || 0) * (params.size || 10) + i + 1)));
    for (const [name, facet] of Object.entries(response.facets || {})) {
      if (!facet.values.length) continue;
      console.log(`${name}: ${facet.values.slice(0, 8).map(v => `${v.value} (${v.count})`).join(" · ")}${facet.exact ? "" : " ≈"}`);
    }
  }
} else if (command === "suggest") {
  const [ref, ...words] = args.positionals;
  const engine = await openIndex(ref);
  const response = await engine.suggest({ q: words.join(" "), size: args.size, ...(args.shards ? { shards: args.shards } : {}) });
  if (args.json) console.log(JSON.stringify(response, null, 2));
  else response.suggestions.forEach(s => console.log(`${s.text}  (${s.weight})`));
} else if (command === "count") {
  const [ref, ...words] = args.positionals;
  const engine = await openIndex(ref);
  const response = await engine.count({ q: words.join(" "), ...(args.shards ? { shards: args.shards } : {}) });
  if (args.json) console.log(JSON.stringify(response, null, 2));
  else console.log(`${response.total}${response.totalExact ? "" : " (approximate)"}`);
} else if (command === "info") {
  const engine = await openIndex(args.positionals[0]);
  const manifest = engine.manifest || {};
  const info = {
    total: manifest.total ?? null,
    built_at: manifest.built_at ?? null,
    engine: manifest.engine ?? manifest.format ?? null,
    meta: manifest.meta ?? null,
    features: manifest.features ?? null,
    ...(Array.isArray(manifest.shards) ? { shards: manifest.shards.map(s => ({ id: s.id, total: s.total, groups: s.groups ?? [] })) } : {}),
    ...(Array.isArray(manifest.generations) ? { generations: manifest.generations.length } : {})
  };
  if (args.json) {
    console.log(JSON.stringify(info, null, 2));
  } else {
    console.log(`documents: ${info.total?.toLocaleString?.() ?? "?"}   built: ${info.built_at ?? "?"}`);
    if (info.meta) {
      const meta = info.meta;
      console.log(`source: ${[meta.source, meta.attribution, meta.license].filter(Boolean).join(" · ")}`);
      if (meta.data_version) console.log(`data version: ${meta.data_version}`);
    }
    if (info.shards) {
      console.log(`shards (${info.shards.length}):`);
      for (const shard of info.shards.slice(0, 30)) {
        console.log(`  ${shard.id}  ${Number(shard.total).toLocaleString()} docs${shard.groups.length ? `  [${shard.groups.join(", ")}]` : ""}`);
      }
      if (info.shards.length > 30) console.log(`  … ${info.shards.length - 30} more`);
    }
    if (info.generations) console.log(`generations: ${info.generations}`);
  }
} else {
  fatal(`Unknown command: ${command}`);
}
