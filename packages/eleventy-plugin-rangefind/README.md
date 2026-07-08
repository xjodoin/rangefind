# eleventy-plugin-rangefind

Add fast, framework-free search to any [Eleventy](https://www.11ty.dev/) site
with [Rangefind](https://github.com/xjodoin/rangefind) — a static search engine
that ships a **packed static index** and fetches only the byte ranges it needs
via **HTTP Range requests**. No search server, no third-party service, no
runtime index download.

The plugin:

1. Crawls your built site's HTML on `eleventy.after` and writes a Rangefind
   index into your output directory.
2. Copies the drop-in `<rangefind-search>` Web Component (JS + optional CSS
   theme) alongside it.
3. Registers a universal `{% rangefindSearch %}` shortcode that mounts the
   component.

## Install

```bash
npm install eleventy-plugin-rangefind rangefind
```

`rangefind` is a peer of this plugin — install it in your project too.

## Register

Eleventy configs are ESM-friendly. In `eleventy.config.js`:

```js
import rangefindPlugin from "eleventy-plugin-rangefind";

export default function (eleventyConfig) {
  eleventyConfig.addPlugin(rangefindPlugin, {
    baseUrl: "/"          // URL prefix baked into result URLs
  });
}
```

CommonJS projects (`.eleventy.js`) can use dynamic import:

```js
module.exports = async function (eleventyConfig) {
  const { default: rangefindPlugin } = await import("eleventy-plugin-rangefind");
  eleventyConfig.addPlugin(rangefindPlugin);
};
```

## Use in templates

Drop the shortcode anywhere — it works in Nunjucks, Liquid, Markdown, and
11ty.js templates:

```njk
{% rangefindSearch %}
```

With options (Nunjucks / 11ty.js object-argument form):

```njk
{% rangefindSearch {
  placeholder: "Search the docs",
  theme: true,
  pageSize: 5,
  inputClass: "w-full border rounded px-3 py-2",
  markClass: "bg-yellow-200"
} %}
```

The shortcode renders:

```html
<link rel="stylesheet" href="/_rangefind/rangefind-search.css">   <!-- only when theme: true -->
<script type="module" src="/_rangefind/rangefind-search.js"></script>
<rangefind-search src="/rangefind/"
  placeholder="Search the docs" page-size="5"
  input-class="w-full border rounded px-3 py-2"
  mark-class="bg-yellow-200"></rangefind-search>
```

## Plugin options

| Option | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | Set `false` to skip indexing and shortcode registration entirely. |
| `outputDir` | `"rangefind"` | Index directory, relative to Eleventy's output dir. |
| `assetsDir` | `"_rangefind"` | Directory the client JS/CSS are copied into, relative to output. |
| `baseUrl` | `"/"` | URL prefix or origin baked into indexed result URLs. A path like `/blog/` prefixes every URL; an absolute origin like `https://example.com/` produces absolute URLs. |
| `src` | `"/rangefind/"` | Default `src` (index base URL) for the shortcode's element. |
| `assetsBase` | `"/_rangefind"` | Default URL base the shortcode points `<script>`/`<link>` at. |
| `theme` | `false` | Default for whether the shortcode emits the optional CSS theme link. |
| `shortcodeName` | `"rangefindSearch"` | Name to register the shortcode under. |

> `outputDir`/`assetsDir` are filesystem paths inside your output folder;
> `src`/`assetsBase` are the **URLs** the browser uses. They are separate so you
> can deploy under a subpath or CDN without changing where files are written.

## Shortcode arguments

All are optional. Control keys shape the markup; every other key becomes an
attribute on `<rangefind-search>`.

| Argument | Effect |
| --- | --- |
| `src` | Override the index base URL for this element (defaults to plugin `src`). |
| `assetsBase` | Override the asset URL base for this call (defaults to plugin `assetsBase`). |
| `theme` | `true` to emit the optional stylesheet link for this call. |
| *any element attribute* | Passed through to the element, normalized to kebab-case. `pageSize` → `page-size`, `inputClass` → `input-class`. |

Recognized element attributes (from the underlying Web Component): `placeholder`,
`label`, `page-size`, `debounce`, `min-length`, `highlight`, `suggest`,
`router`, `open-on-focus`, `hotkey`, `empty-text`, `loading-text`, `error-text`,
plus the per-part class hooks `root-class`, `input-class`, `panel-class`,
`list-class`, `option-class`, `option-title-class`, `option-snippet-class`,
`option-url-class`, `empty-class`, `status-class`, `suggest-class`,
`suggest-item-class`, `mark-class`. Unrecognized names are dropped with a
warning rather than rendered.

Boolean arguments follow the component's semantics: `router: true` renders a
bare `router` attribute; `highlight: false` renders `highlight="false"` to turn
a default-on feature off.

## Zero framework required, any CSS

`<rangefind-search>` renders into its **light DOM** and ships **no styling of
its own** — the host page's CSS applies directly. Style it with your own CSS
targeting the `rf-search__*` class hooks, with Tailwind (or any utility CSS) via
the `*-class` attributes shown above, or opt into the bundled theme with
`theme: true`. See the [Rangefind component docs](https://github.com/xjodoin/rangefind/blob/main/docs/reference.md#search-ui-component)
for the full attribute, class-hook, and event reference.

## How it works

- **Indexing** runs on Eleventy's official `eleventy.after` event, so the crawl
  sees the final built HTML. Extraction rules (title, headings, body,
  `data-rangefind-*` attributes) are documented in the
  [Rangefind crawler reference](https://github.com/xjodoin/rangefind/blob/main/docs/reference.md#crawling-a-static-site).
- **Assets** are resolved from the installed `rangefind` package via
  `import.meta.resolve("rangefind/element")` and
  `import.meta.resolve("rangefind/element.css")`, so they always match the
  installed Rangefind version.
