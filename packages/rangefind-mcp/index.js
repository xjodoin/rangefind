// rangefind-mcp — an MCP server over rangefind static search indexes.
//
// Exposes any rangefind index (a local directory or an http(s) URL; single,
// generational, or sharded) as agent tools: full-text + geo search,
// autocomplete, exact counts, and index metadata. Engines are cached per
// index with rangefind's Node runtime, so repeated tool calls reuse warmed
// directory pages and disk-cached packs.
//
// Index access modes:
//   configured — createRangefindMcpServer({ indexes: Map(name → path/url) })
//     restricts tools to the named indexes (discoverable via
//     rangefind_list_indexes);
//   open — the `index` argument may be any path or URL (the default when no
//     indexes are configured; combinable with configured names via
//     { open: true }).

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { createNodeSearch } from "rangefind/node";

const INDEX_ARG = z
  .string()
  .describe(
    "Which index to query: a configured index name (see rangefind_list_indexes), or — in open mode — a local directory path or http(s) URL of any rangefind index."
  );

const SHARDS_ARG = z
  .array(z.string())
  .optional()
  .describe(
    'Sharded indexes only: restrict to shard ids or group labels (e.g. ["quebec"] or ["canada"]). Unknown names fail listing the available options.'
  );

const GEO_ARG = z
  .object({
    near: z
      .object({
        lat: z.number(),
        lon: z.number(),
        radiusMeters: z.number().positive().optional()
      })
      .optional()
      .describe("Center point. Omit radiusMeters (with sort \"distance\") for pure nearest-first."),
    box: z
      .object({ minLat: z.number(), maxLat: z.number(), minLon: z.number(), maxLon: z.number() })
      .optional()
      .describe("Bounding box, e.g. a map viewport."),
    sort: z.literal("distance").optional().describe("Order results nearest-first."),
    boost: z
      .object({ weight: z.number().optional(), pivotMeters: z.number().optional() })
      .optional()
      .describe("Keep relevance order but prefer nearby results.")
  })
  .optional()
  .describe("Geo query over an index with geo fields.");

function buildSearchParams(args) {
  const params = { q: String(args.q ?? ""), page: args.page, size: args.size, highlight: false };
  if (args.facets?.length) params.facets = args.facets;
  if (args.filters) params.filters = args.filters;
  if (args.sort) params.sort = args.sort;
  if (args.shards) params.shards = args.shards;
  if (args.geo) {
    const geo = { ...args.geo };
    // A bare `near` with no query means "what's here": default to
    // nearest-first, which is also required when no radius is given.
    if (geo.near && !geo.sort && (!params.q || !geo.near.radiusMeters)) geo.sort = "distance";
    params.geo = geo;
  }
  return params;
}

function compactResults(response) {
  return {
    total: response.total,
    approximate: Boolean(response.approximate),
    ...(response.correctedQuery ? { correctedQuery: response.correctedQuery } : {}),
    results: (response.results || []).map(({ index, highlights, ...fields }) => fields),
    ...(response.facets ? { facets: response.facets } : {})
  };
}

function indexInfo(engine) {
  const manifest = engine.manifest || {};
  return {
    total: manifest.total ?? null,
    built_at: manifest.built_at ?? null,
    engine: manifest.engine ?? manifest.format ?? null,
    meta: manifest.meta ?? null,
    features: manifest.features ?? null,
    ...(Array.isArray(manifest.shards)
      ? {
          shards: manifest.shards.map(shard => ({
            id: shard.id,
            total: shard.total,
            bbox: shard.bbox ?? null,
            groups: shard.groups ?? []
          }))
        }
      : {}),
    ...(Array.isArray(manifest.generations) ? { generations: manifest.generations.length } : {})
  };
}

function ok(result) {
  return {
    content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
    structuredContent: result
  };
}

function fail(message) {
  return { content: [{ type: "text", text: `Error: ${message}` }], isError: true };
}

/**
 * @param {object} [options]
 * @param {Map<string,string>|Record<string,string>} [options.indexes] Named indexes (name → directory path or URL).
 * @param {boolean} [options.open] Allow arbitrary paths/URLs as the `index` argument (default: true when no indexes are configured).
 * @param {string} [options.version] Server version reported to clients.
 */
export function createRangefindMcpServer(options = {}) {
  const indexes =
    options.indexes instanceof Map ? options.indexes : new Map(Object.entries(options.indexes || {}));
  const configured = indexes.size > 0;
  const open = options.open ?? !configured;
  const engines = new Map();

  function hint() {
    if (configured) {
      return `Configured indexes: ${[...indexes.keys()].join(", ")}${open ? " (paths/URLs also accepted)" : ""}.`;
    }
    return "Pass a local rangefind index directory or an http(s) index URL.";
  }

  function engineFor(ref) {
    const key = String(ref ?? "");
    const target = indexes.get(key) ?? (open ? key : null);
    if (!key || target == null) {
      throw new Error(`Unknown index "${key}". ${hint()}`);
    }
    if (!engines.has(target)) {
      engines.set(
        target,
        createNodeSearch({ baseUrl: target }).catch(error => {
          engines.delete(target);
          throw new Error(`Could not open index "${key}" (${target}): ${error.message}`);
        })
      );
    }
    return engines.get(target);
  }

  // Tool failures come back as isError results the agent can read and adjust
  // to (wrong index name, unknown shard, malformed geo), not protocol errors.
  const guard = handler => async args => {
    try {
      return ok(await handler(args));
    } catch (error) {
      return fail(error.message);
    }
  };

  const server = new McpServer({ name: "rangefind", version: options.version || "0.3.1" });

  if (configured) {
    server.registerTool(
      "rangefind_list_indexes",
      {
        title: "List configured indexes",
        description:
          "List the rangefind indexes this server is configured with. Use these names as the `index` argument of the other tools.",
        annotations: { readOnlyHint: true, openWorldHint: false }
      },
      guard(async () => ({
        indexes: [...indexes.entries()].map(([name, target]) => ({ name, target }))
      }))
    );
  }

  server.registerTool(
    "rangefind_info",
    {
      title: "Index metadata",
      description:
        "Metadata about a rangefind index: document total, build time, provenance (source/attribution/license/data version), enabled features, and — for sharded indexes — the shard list with ids, groups, coverage bounding boxes, and totals. Call this first to learn what an index contains and how to scope queries.",
      inputSchema: { index: INDEX_ARG },
      annotations: { readOnlyHint: true, openWorldHint: false }
    },
    guard(async args => indexInfo(await engineFor(args.index)))
  );

  server.registerTool(
    "rangefind_search",
    {
      title: "Search an index",
      description:
        "Full-text search over a rangefind index: BM25F relevance with typo correction, facet counts, facet/numeric filters, sorting, geo queries (radius, bounding box, nearest-first, distance boost), shard scoping, and pagination. Returns ranked results with their display fields and scores. `total` may be a fast lower bound flagged `approximate`; use rangefind_count for exact totals.",
      inputSchema: {
        index: INDEX_ARG,
        q: z.string().default("").describe("Query text. May be empty for pure geo/filter browsing."),
        page: z.number().int().min(1).default(1),
        size: z.number().int().min(1).max(100).default(10),
        facets: z.array(z.string()).optional().describe("Facet field names to count for this query."),
        filters: z
          .object({
            facets: z.record(z.array(z.string())).optional(),
            numbers: z.record(z.object({ min: z.number().optional(), max: z.number().optional() })).optional(),
            booleans: z.record(z.boolean()).optional()
          })
          .optional()
          .describe('Filters, e.g. {"facets":{"category":["restaurant"]},"numbers":{"year":{"min":1990,"max":2000}},"booleans":{"open":true}}.'),
        sort: z
          .string()
          .optional()
          .describe('Sort by a number/boolean field: "field" ascending or "-field" descending. Omit for relevance.'),
        geo: GEO_ARG,
        shards: SHARDS_ARG
      },
      annotations: { readOnlyHint: true, openWorldHint: false }
    },
    guard(async args => compactResults(await (await engineFor(args.index)).search(buildSearchParams(args))))
  );

  server.registerTool(
    "rangefind_suggest",
    {
      title: "Autocomplete",
      description:
        "Search-as-you-type autocomplete: prefix and mid-token suggestions ranked by weight. Useful to complete a partial place or entity name before a full search.",
      inputSchema: {
        index: INDEX_ARG,
        q: z.string().min(1).describe("The typed prefix."),
        size: z.number().int().min(1).max(50).default(8),
        shards: SHARDS_ARG
      },
      annotations: { readOnlyHint: true, openWorldHint: false }
    },
    guard(async args => {
      const { suggestions } = await (await engineFor(args.index)).suggest({
        q: args.q,
        size: args.size,
        shards: args.shards
      });
      return { suggestions };
    })
  );

  server.registerTool(
    "rangefind_count",
    {
      title: "Exact match count",
      description:
        "Exact number of documents matching a text query, without fetching results. Cheaper than rangefind_search when only the total matters.",
      inputSchema: { index: INDEX_ARG, q: z.string().min(1), shards: SHARDS_ARG },
      annotations: { readOnlyHint: true, openWorldHint: false }
    },
    guard(async args => {
      const { total, totalExact, approximate } = await (await engineFor(args.index)).count({
        q: args.q,
        shards: args.shards
      });
      return { total, totalExact: Boolean(totalExact), approximate: Boolean(approximate) };
    })
  );

  return server;
}
