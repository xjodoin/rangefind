# Ecosystem directory submissions

Tracking for getting the rangefind integrations listed in each ecosystem's
plugin directory. Last updated: 2026-07-14.

## Open pull requests

| Ecosystem | PR | What it adds | Status |
| --- | --- | --- | --- |
| MkDocs catalog | [mkdocs/catalog#422](https://github.com/mkdocs/catalog/pull/422) | `mkdocs-rangefind` in `projects.yaml` under 🔍 Search & tables of content | open |
| Eleventy plugins | [11ty/docs#2339](https://github.com/11ty/docs/pull/2339) | `src/_data/plugins/eleventy-plugin-rangefind.json` for the 11ty.dev plugins page | open |
| Docusaurus resources | [facebook/docusaurus#12286](https://github.com/facebook/docusaurus/pull/12286) | `docusaurus-plugin-rangefind` in the community resources Search section (maintainer-discretion review) | open |
| awesome-mcp-servers | [punkpeye/awesome-mcp-servers#10109](https://github.com/punkpeye/awesome-mcp-servers/pull/10109) | `rangefind-mcp` under Search & Data Extraction, with Glama score badge | open — bot requires the Glama listing below to go live |

## No PR needed

| Ecosystem | Mechanism | Status |
| --- | --- | --- |
| Astro integrations catalog | Automatic weekly npm scan of the `astro-integration` keyword (already on published `rangefind-astro`) | waiting for the next index pass |
| Official MCP Registry | `mcp-publisher publish` with `server.json` | **published**: [`io.github.xjodoin/rangefind` v0.3.2](https://registry.modelcontextprotocol.io/v0.1/servers?search=io.github.xjodoin/rangefind) |
| Glama MCP directory | Web submission at glama.ai/mcp/servers (requires account login) | submitted 2026-07-14, pending review — repo carries `packages/rangefind-mcp/Dockerfile` (verified to boot + answer introspection) and a root `glama.json` maintainer claim |

## Follow-ups

- When the Glama listing is approved, the score badge in the
  awesome-mcp-servers entry resolves automatically; nudge PR #10109 if the
  bot doesn't re-check on its own.
- On future `rangefind-mcp` releases: bump the version in the registry
  `server.json` and re-run `mcp-publisher publish`, and update the pinned
  version in `packages/rangefind-mcp/Dockerfile`.
- If any PR gets review feedback, the fork branches live under
  `github.com/xjodoin/{catalog,docs,docusaurus,awesome-mcp-servers}`.
