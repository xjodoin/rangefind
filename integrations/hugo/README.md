# Rangefind + Hugo

Add fast, serverless search to a Hugo site. No search server, no client-side
index download — just a static index served with HTTP Range requests.

## How this works (read this first)

Hugo is a single Go binary. It has **no JavaScript plugin system and no way to
hook a Node tool into its build**. So, exactly like every other JS-based static
search tool's Hugo integration, Rangefind runs as a **separate command right
after `hugo`**: Hugo builds your site to its output directory (`public/` by
default), and Rangefind's crawler indexes that built HTML directly. There is no
"Hugo plugin" to install and none is needed — the crawler already does the one
thing required, which is turn a directory of built HTML into an index.

## Prerequisites

- Rangefind installed. Either globally:

  ```bash
  npm install -g rangefind
  ```

  or invoked on demand with `npx rangefind ...` (no install). Requires Node ≥ 22.
- A Hugo site that already builds with `hugo` to its publish directory
  (`public/` unless you set `publishDir` in `hugo.toml` / `config.toml`).

## 1. Build the index

Run Hugo, then point Rangefind at the output directory:

```bash
hugo && npx rangefind build public
```

Replace `public` with your `publishDir` if you changed it. This writes the
index to `public/rangefind/` (the crawler's default: `<dir>/rangefind`). Because
the index lands *inside* `public/`, it deploys with the rest of your site
automatically — and the crawler **auto-prunes its own nested output directory
from the crawl**, so re-running the command never indexes the index.

Useful flags (from `rangefind build <dir> ...`):

- `--base-url <url>` — URL prefix or origin for result URLs (default `/`). Set
  this to match your Hugo `baseURL` when you need URLs computed from a site
  root other than `/`. A path like `--base-url /blog/` prefixes every result
  URL; an absolute origin like `--base-url https://example.com/` produces
  absolute result URLs. If your site is served at the domain root and you use
  the search UI component (which resolves URLs relatively), the default `/` is
  fine.
- `--output <dir>` — write the index somewhere other than `public/rangefind`.
  The nested default is the sane choice (it ships with the site and needs no
  extra copy step), so only change this if you have a reason to.
- `--root <dir>` — directory whose relative paths define ids/URLs (default is
  the crawled dir); useful when indexing a subtree.

## 2. Wire it into your deploy pipeline

Any pipeline that already runs `hugo` just appends the Rangefind step.

**`package.json` one-liner** (if you drive builds through npm):

```json
{
  "scripts": {
    "build": "hugo && rangefind build public"
  }
}
```

**Netlify** — build command:

```bash
hugo && npx rangefind build public
```

**Vercel** — build command:

```bash
hugo && npx rangefind build public
```

**GitHub Actions** — a build step:

```yaml
- run: hugo --minify && npx rangefind build public
```

The `public/` directory (now including `public/rangefind/`) is your deploy
artifact as usual. Any host with HTTP Range support works — GitHub Pages,
Netlify, Vercel, S3/CloudFront, nginx. Do not let the host gzip-transcode the
`.bin` files; serve them as-is (see the deployment notes in the main docs).

## 3. Add the search UI

Rangefind ships a drop-in `<rangefind-search>` Web Component. Two files serve
it: `rangefind-search.js` (self-contained ES module, bundles the runtime) and
`rangefind-search.css` (optional theme).

**Recommended: vendor them into `static/`.** Hugo copies `static/` verbatim to
the site root, so this needs no extra build step:

```bash
cp node_modules/rangefind/dist/rangefind-search.js  static/rangefind-search.js
cp node_modules/rangefind/dist/rangefind-search.css static/rangefind-search.css
```

(If you installed Rangefind globally rather than as a project dependency, copy
from wherever `npm root -g` points, e.g.
`$(npm root -g)/rangefind/dist/rangefind-search.js`.)

**Alternative: reference a CDN** instead of vendoring, e.g. an unpkg-style URL
like `https://unpkg.com/rangefind/dist/rangefind-search.js`. This avoids
committing the bundle, but adds a third-party runtime dependency and a
cross-origin fetch. Vendoring into `static/` is simpler and fully self-hosted,
so it is the recommended default.

**Then add the partial.** Copy [`layouts/partials/rangefind-search.html`](layouts/partials/rangefind-search.html)
from this directory into your site at `layouts/partials/rangefind-search.html`
and include it wherever you want the box — for example in your header partial or
`baseof.html`:

```go-html-template
{{ partial "rangefind-search.html" . }}
```

The partial references the assets through Hugo's `relURL` template function so
they resolve correctly under your `baseURL` and any subpath deploy:

```go-html-template
<link rel="stylesheet" href="{{ "rangefind-search.css" | relURL }}">
<script type="module" src="{{ "rangefind-search.js" | relURL }}"></script>
<rangefind-search src="{{ "rangefind/" | relURL }}"></rangefind-search>
```

Style it with your own CSS (the component renders into light DOM, so your
styles and Tailwind utilities apply directly) or keep the linked default theme.
Per-part class hooks (`input-class`, `panel-class`, `option-class`,
`mark-class`, …) and all other attributes are documented in the main reference
under "Search UI component".

## Verify it works

The [`verify/`](verify/) directory is a minimal, real Hugo site that exercises
this exact recipe end to end (Hugo build → `rangefind build` → served search
query). To run it yourself:

```bash
cd verify
hugo --minify
node ../../../bin/rangefind.js build public
node --test test.mjs
```

The test serves `public/` with a Range-capable server and asserts that
searching for `xylophone` returns the second post.

## Enriching the index

Add computed fields (embeddings for semantic search, external metadata) with
an enrich module — its default export runs on the crawled documents, and an
optional `config` export merges overrides into the generated build config:

```bash
hugo && npx rangefind build ./public --enrich ./enrich.mjs
```
