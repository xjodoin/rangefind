---
title: Plugins
lede: First-class integrations for Eleventy, Astro, Docusaurus, MkDocs, and Hugo — each one runs the real crawler over your real build output.
description: Official rangefind integrations — eleventy-plugin-rangefind, rangefind-astro, docusaurus-plugin-rangefind, mkdocs-rangefind on PyPI, and Hugo layouts — plus the plain CLI for everything else.
order: 10
---

Every plugin does the same three things at the end of your normal build:
crawl the generated HTML into a static index, copy the
[search component](../search-component/) assets next to it, and give you an
idiomatic way to mount the search box. No build pipeline forks, no separate
indexing service.

All of them also share two hooks:

- **`config`** — rangefind config overrides merged into the crawler's
  generated config (tuning knobs, provenance `meta`, a `vectors`
  declaration…).
- **`enrich`** — an async `function(docs)` (or, everywhere a function can't
  be expressed — the CLI, MkDocs — a path to an ES module default-exporting
  one, optionally exporting `config` overrides too) that runs on the
  crawled documents before indexing. This is how the search on this site
  adds MiniLM embeddings for semantic ranking; it works equally for
  external metadata, tags, or computed weights.

```js
// enrich.mjs — usable via any plugin option or `--enrich`
export const config = { vectors: [{ name: "embedding", path: "embedding", dims: 384 }] };
export default async docs => { /* add doc.embedding to each */ return docs; };
```

## Eleventy

```bash
npm install eleventy-plugin-rangefind
```

```js
// eleventy.config.js
import rangefind from "eleventy-plugin-rangefind";

export default function (eleventyConfig) {
  eleventyConfig.addPlugin(rangefind, {
    baseUrl: "/",            // prefix baked into result URLs
    src: "/rangefind/",      // where the index is served
    assetsBase: "/_rangefind"
  });
}
```

```njk
{% raw %}{% rangefindSearch placeholder="Search…", hotkey=true %}{% endraw %}
```

The shortcode works in Nunjucks, Liquid, Markdown, Handlebars, and 11ty.js
templates. **This site is built with it.**

## Astro

```bash
npm install rangefind-astro
```

```js
// astro.config.mjs
import { defineConfig } from "astro/config";
import rangefind from "rangefind-astro";

export default defineConfig({ integrations: [rangefind()] });
```

```astro
---
import RangefindSearch from "rangefind-astro/RangefindSearch.astro";
---
<RangefindSearch placeholder="Search…" hotkey />
```

## Docusaurus

```bash
npm install docusaurus-plugin-rangefind
```

```js
// docusaurus.config.js
plugins: [["docusaurus-plugin-rangefind", { baseUrl: "/" }]]
```

Indexes the built docs site and injects the search widget — your docs search
stays on your own hosting instead of a third-party service.

## MkDocs

```bash
pip install mkdocs-rangefind
```

```yaml
# mkdocs.yml
plugins:
  - rangefind
```

Runs after `mkdocs build`, indexes `site/`, and injects the widget into the
theme. (Node ≥ 22 must be available on the build machine — the indexer is
the rangefind CLI.)

## Hugo

Hugo has no plugin system, so the integration is copy-in `layouts/` partials
plus one post-build command:

```bash
hugo && npx rangefind build ./public
```

See `integrations/hugo/` in the repository for the partials and a verify
script.

## Everything else

If your tool produces HTML, the plain CLI is the integration:

```bash
npx rangefind build ./out --enrich ./enrich.mjs   # --enrich is optional
```

Add [`data-rangefind-*` attributes](../) to your templates for body scoping,
facets, and metadata, then mount the component or call the
[query API](../query-api/) yourself.
