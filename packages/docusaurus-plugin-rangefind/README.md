# docusaurus-plugin-rangefind

Add fast, fully static search to a [Docusaurus](https://docusaurus.io) site with
[Rangefind](https://github.com/xjodoin/rangefind) — a search engine that ships a
**static index** and answers queries in the browser with **HTTP Range
requests**. No search server, no third-party service, no crawl-time API keys.

At `docusaurus build` time the plugin:

1. Crawls the freshly built HTML in `outDir` and writes a Rangefind index to
   `build/rangefind/` (via `buildFromCrawl` from `rangefind/crawler`).
2. Copies the drop-in search web component and its optional theme into
   `build/_rangefind/`.
3. Injects the component's `<script type="module">` (and, by default, the theme
   `<link rel="stylesheet">`) on **every page**.

You then place the `<rangefind-search>` box wherever you want it.

## Install

```bash
npm install --save-dev docusaurus-plugin-rangefind rangefind
```

`rangefind` is a runtime peer used at build time (crawler + client bundle);
`@docusaurus/core` >= 3 is a peer.

## Configure

`docusaurus.config.js`:

```js
module.exports = {
  // ...
  plugins: ["docusaurus-plugin-rangefind"]
};
```

Or with options:

```js
module.exports = {
  plugins: [
    ["docusaurus-plugin-rangefind", { theme: true }]
  ]
};
```

## Place the search box

The plugin injects the component's script/style globally but **does not** render
a search box for you — that would force a duplicate box onto every page with no
control over placement (this mirrors how other Docusaurus search plugins work).
Instead, `<rangefind-search>` is a plain custom element, so you drop it in
wherever you like. Point its `src` at the index, which lives at
`<baseUrl>rangefind/` (so `/rangefind/` for a root deploy, `/my-repo/rangefind/`
under a sub-path `baseUrl`).

**Option A — a navbar item (recommended).** Use Docusaurus's built-in `html`
navbar item type:

```js
themeConfig: {
  navbar: {
    items: [
      {
        type: "html",
        position: "right",
        value: '<rangefind-search src="/rangefind/" placeholder="Search…"></rangefind-search>'
      }
    ]
  }
}
```

**Option B — directly in any Markdown/MDX page.** Because it is a custom
element, no React wrapper is needed once the script is injected:

```mdx
<rangefind-search src="/rangefind/" placeholder="Search the docs…"></rangefind-search>
```

**Option C — a swizzled component.** Swizzle e.g. `Navbar/Content` and render
`<rangefind-search src="/rangefind/" />` in your JSX. React passes custom
elements through untouched.

## Options

All options are optional.

| Option | Type | Default | Effect |
| --- | --- | --- | --- |
| `enabled` | `boolean` | `true` | Set `false` to skip indexing and injection entirely (e.g. in a preview build). |
| `theme` | `boolean` | `true` | Inject the optional `rangefind-search.css` theme link. Set `false` if you style the component yourself. |
| `baseUrl` | `string` | site `baseUrl` | URL prefix for result URLs and the injected asset tags. Defaults to Docusaurus's own `baseUrl`, which is correct for sub-path deploys. |
| `outputDir` | `string` | `"rangefind"` | Index directory, relative to `outDir` (absolute paths are used as-is). This is also where your `<rangefind-search src>` should point (`<baseUrl><outputDir>/`). |
| `assetsDir` | `string` | `"_rangefind"` | Directory (relative to `outDir`) for the copied `rangefind-search.js` / `.css`. |

## Styling

Zero framework is required beyond Docusaurus. The `<rangefind-search>` element
renders into its **light DOM** with no styling of its own, so:

- The bundled theme (`theme: true`, on by default) gives you a usable look with
  light/dark palettes out of the box.
- With Tailwind or any CSS framework, disable the theme (`theme: false`) and use
  the component's per-part `*-class` attributes (`input-class`, `option-class`,
  `mark-class`, …) or namespaced `rf-search__*` hook classes.

See the Rangefind monorepo's component reference (the "Search UI component"
section of `docs/reference.md`) for the full attribute list, class hooks,
events, and accessibility details — this plugin does not change that API, it
only wires the component into the Docusaurus build.

## How it works

- **Hooks:** the plugin uses the official `postBuild(props)` lifecycle hook
  (crawl + asset copy, keyed off `props.outDir`) and `injectHtmlTags()`
  (global script + optional style in `postBodyTags`).
- **Asset resolution:** the client bundle and theme are located with
  `require.resolve("rangefind/element")` and
  `require.resolve("rangefind/element.css")`, so they always match the installed
  `rangefind` version; the crawler is loaded via a dynamic
  `import("rangefind/crawler")`.
- **Deployment:** Rangefind needs a static host that supports HTTP `Range`
  requests (GitHub Pages, Netlify, S3/CloudFront, nginx, …). See the Rangefind
  deployment notes for MIME/caching guidance.
