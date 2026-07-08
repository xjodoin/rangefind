# mkdocs-rangefind

An MkDocs plugin that wires [Rangefind](https://github.com/xjodoin/rangefind)
static search into your docs site. After MkDocs builds your site, the plugin
crawls the rendered HTML into a static, range-request search index and injects
the Rangefind search widget into every page — so you get working, serverless
search with **zero manual HTML editing**.

## How it works (and the one prerequisite)

Rangefind is a Node.js tool; MkDocs is a Python static-site generator. There is
no way to run Rangefind's indexing logic from pure Python, so this plugin is a
small, honest bridge: its MkDocs lifecycle hooks **shell out to the Rangefind
Node CLI** after the site is built.

**Prerequisite:** Node.js (>= 22) and the `rangefind` npm package must be
reachable from your MkDocs project directory. The simplest setup:

```bash
npm install rangefind        # adds ./node_modules/rangefind + the CLI bin
pip install mkdocs-rangefind
```

With `rangefind` installed locally, both `npx rangefind` (the index build) and
Node's module resolution (locating the widget's JS/CSS assets) work offline,
including in CI.

## Usage

Add the plugin to `mkdocs.yml`:

```yaml
plugins:
  - search        # optional; Rangefind replaces the need for it
  - rangefind
```

Then `mkdocs build` (or `mkdocs gh-deploy`). The plugin will:

1. Run `rangefind build <site_dir> --output <site_dir>/rangefind --base-url <…>`
   to crawl the built HTML and produce the index under `site/rangefind/`.
2. Copy the search Web Component assets into `site/_rangefind/`.
3. Inject a `<script type="module">` for the component, an optional theme
   `<link>`, and (by default) a `<rangefind-search>` box on every page.

That's it — search works on the deployed static site with no server.

## Configuration

All options are optional. Defaults shown.

```yaml
plugins:
  - rangefind:
      enabled: true            # master switch; false = complete no-op
      base_url: ""             # URL prefix baked into result links stored in
                               # the index (CLI --base-url). Empty = derive from
                               # mkdocs `site_url` path, falling back to "/".
      output_dir: rangefind    # index output dir, relative to the built site
      assets_dir: _rangefind   # where the widget JS/CSS are copied, rel. to site
      theme: false             # also inject the optional framework-free stylesheet
      node_command: npx        # command used to run the Rangefind CLI
      placement: body_end      # "body_end" | "manual" (see below)
      selector: ""             # optional literal HTML marker to place the widget after
      placeholder: Search      # input placeholder for the injected element
      element_attributes: {}   # extra attributes for <rangefind-search>
      element_js_path: ""       # explicit path to rangefind-search.js (advanced)
      element_css_path: ""      # explicit path to rangefind-search.css (advanced)
```

### `base_url`

This is passed to the crawler as `--base-url` and controls the URL prefix stored
in the **result links** inside the index. Left empty, the plugin derives it from
your MkDocs `site_url` path (e.g. `https://example.com/docs/` → `/docs/`),
falling back to `/`. Set it explicitly if you need a different prefix. Note this
is unrelated to where the widget loads the index from — that is always resolved
as a path *relative to each page*, so nested and subpath-deployed sites work
without configuration.

### Placement modes

- **`body_end`** (default): the plugin injects
  `<rangefind-search src="…"></rangefind-search>` right before `</body>` on every
  page. Search "just works" out of the box.
- **`manual`**: the plugin injects the component's `<script>` (and theme `<link>`
  if `theme: true`) but does **not** inject the `<rangefind-search>` element.
  Hand-place it yourself in a
  [theme override](https://www.mkdocs.org/user-guide/customizing-your-theme/#using-the-theme-custom_dir),
  e.g. `<rangefind-search src="/rangefind/"></rangefind-search>`. Use this when
  you want the search box in a specific spot (a header, a sidebar) rather than at
  the end of the body.

  ```yaml
  plugins:
    - rangefind:
        placement: manual
  ```

- **`selector`**: a middle ground. Provide a literal HTML substring that appears
  in your rendered pages (for example a placeholder you put in a template or
  Markdown page: `<div id="rangefind"></div>`). The `<rangefind-search>` element
  is inserted immediately after the first occurrence of that exact string. If the
  marker is not found on a page, the plugin falls back to `body_end`.

### Styling / Tailwind

The `<rangefind-search>` component renders into **light DOM** and ships no
styling of its own, so your site's CSS applies directly. For manual placement
you can pass Tailwind (or any) classes through the component's `*-class`
attributes via `element_attributes`:

```yaml
plugins:
  - rangefind:
      element_attributes:
        input-class: "w-full border rounded px-3 py-2"
        mark-class: "bg-yellow-200"
        hotkey: true      # boolean attributes: use `true`
        router: true
```

The full attribute and class-hook vocabulary (`input-class`, `option-class`,
`mark-class`, …, plus attributes like `page-size`, `debounce`, `suggest`,
`hotkey`, `router`) is documented in the Rangefind monorepo's
[reference guide](https://github.com/xjodoin/rangefind/blob/main/docs/reference.md#search-ui-component)
— this plugin does not duplicate it, it just forwards attributes verbatim.

### Asset resolution

The plugin needs the component's `rangefind-search.js` / `.css`. Rather than
guess where npm installed them, it asks Node to resolve them through the
`rangefind` package's own export map (`rangefind/element` and
`rangefind/element.css`). This is robust across npm/pnpm/yarn layouts because it
uses the package's declared entry points rather than assuming a path. If you have
an unusual setup, set `element_js_path` / `element_css_path` explicitly (paths
are resolved relative to the MkDocs project directory).

## MkDocs hooks used

- `on_post_build` — builds the index (shell-out) and copies the widget assets.
- `on_post_page` — injects the `<script>`/`<link>`/`<rangefind-search>` tags,
  computing correct page-relative asset paths for nested pages.

## License

MIT
