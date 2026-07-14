export default {
  name: "Rangefind",
  // Origin only — page URLs already carry the path prefix via the url filter.
  origin: process.env.SITE_ORIGIN || "https://rangefind.dev",
  // GA4 measurement id; the tag is only rendered when set (production
  // deploys set it in pages.yml — local/dev builds stay analytics-free).
  analyticsId: process.env.GA_MEASUREMENT_ID || "",
  tagline: "Search, without the server.",
  description:
    "Rangefind packs your site into a static search index and lets the browser read only the byte ranges it needs. BM25F relevance, typo correction, facets, autocomplete, geo, and vectors — from plain files on any static host.",
  repo: "https://github.com/xjodoin/rangefind",
  npm: "https://www.npmjs.com/package/rangefind",
  docsNav: [
    {
      section: "Start here",
      items: [
        { title: "Getting started", url: "/docs/" },
        { title: "How it works", url: "/docs/how-it-works/" },
        { title: "Hosting the index", url: "/docs/hosting/" }
      ]
    },
    {
      section: "Building indexes",
      items: [
        { title: "Build configuration", url: "/docs/build-configuration/" },
        { title: "Multilingual analysis", url: "/docs/multilingual/" },
        { title: "Incremental updates", url: "/docs/incremental-updates/" }
      ]
    },
    {
      section: "Querying",
      items: [
        { title: "Query API", url: "/docs/query-api/" },
        { title: "Search component", url: "/docs/search-component/" },
        { title: "Node runtime", url: "/docs/node-runtime/" },
        { title: "CLI & MCP server", url: "/docs/cli-mcp/" }
      ]
    },
    {
      section: "Integrations",
      items: [
        { title: "Plugins", url: "/docs/plugins/" }
      ]
    },
    {
      section: "Geo & scale",
      items: [
        { title: "Geo search", url: "/docs/geo-search/" },
        { title: "Sharded indexes", url: "/docs/sharded-indexes/" },
        { title: "Benchmarks", url: "/docs/benchmarks/" }
      ]
    }
  ]
};
