# rangefind-astro

An [Astro](https://astro.build) integration for
[Rangefind](https://github.com/xjodoin/rangefind) — build a static search index
from your site at `astro build` time and drop an accessible search box into any
page. No search server: the index is static files served with HTTP `Range`
requests.

## How it works

On the `astro:build:done` hook the integration:

1. Crawls your built static output with Rangefind's crawler, indexing every
   `.html` page's title, headings, and body.
2. Writes the index into your build output (default `dist/rangefind/`) so it
   deploys alongside the rest of your site.
3. Copies the drop-in `<rangefind-search>` web component and its optional theme
   into `dist/_rangefind/` for the `RangefindSearch` component to load.

## Install

```bash
npm install rangefind-astro
```

`astro` and `rangefind` are the only requirements — no framework (React/Vue/…)
is needed; the search UI is a framework-free web component.

## Add the integration

```js
// astro.config.mjs
import { defineConfig } from "astro/config";
import rangefind from "rangefind-astro";

export default defineConfig({
  integrations: [rangefind()]
});
```

## Add the search box

Drop the component into any `.astro` page or layout:

```astro
---
import RangefindSearch from "rangefind-astro/RangefindSearch.astro";
---
<RangefindSearch placeholder="Search the docs…" />
```

That renders the `<rangefind-search>` element and a module script that loads it
from the copied assets. It is **headless by default** — no styling of its own —
so your CSS (Tailwind utilities, global stylesheets) applies directly. Pass
`theme` to also link Rangefind's optional stylesheet.

## Integration options

`rangefind(options)`:

| Option | Default | Purpose |
| --- | --- | --- |
| `enabled` | `true` | Set `false` to skip index building (per-environment opt-out). |
| `baseUrl` | `"/"` | URL prefix/origin for result URLs. A path like `/docs/` prefixes every URL; an absolute origin like `https://example.com/` produces absolute URLs. Match your Astro `base`/`site` if you deploy under a subpath. |
| `outputDir` | `"rangefind"` | Where the index is written, relative to the build output (or an absolute path). Nested under the site output by default so it deploys with everything else. |
| `assetsDir` | `"_rangefind"` | Where the client component assets are copied, relative to the build output (or absolute). |

If you change `outputDir` / `assetsDir`, point the component's `src` /
`assetsBase` props at the matching URLs.

## `<RangefindSearch />` props

| Prop | Default | Purpose |
| --- | --- | --- |
| `src` | `"/rangefind/"` | Index base URL (passed to `createSearch({ baseUrl })`). Set to match `outputDir` / your deploy base. |
| `assetsBase` | `"/_rangefind/"` | URL directory the component `.js`/`.css` were copied to. |
| `theme` | `false` | When truthy, link Rangefind's optional theme stylesheet. Leave off to style it yourself. |
| `placeholder` | `Search` | Input placeholder. |

Every other attribute of the underlying `<rangefind-search>` web component
passes straight through. Config attributes: `label`, `page-size`, `debounce`,
`min-length`, `highlight`, `suggest`, `router`, `open-on-focus`, `hotkey`,
`empty-text`, `loading-text`, `error-text`. Per-part class hooks (anything
matching `*-class`, e.g. `input-class`, `option-class`, `mark-class`) also pass
through — ideal for Tailwind:

```astro
<RangefindSearch
  placeholder="Search…"
  hotkey
  input-class="w-full border rounded px-3 py-2"
  mark-class="bg-yellow-200"
/>
```

Boolean props map faithfully: `highlight={false}` renders `highlight="false"`
(disabling it), while an unset prop keeps the component's own default.

For the full attribute, class-hook, event, and accessibility reference, see the
Rangefind docs' **Search UI component** section:
[`docs/reference.md`](https://github.com/xjodoin/rangefind/blob/main/docs/reference.md).

## Notes

- The `astro:build:done` hook only runs on `astro build`, so `astro dev` is
  unaffected (there is no index during dev).
- The build fails loudly if Rangefind finds no indexable HTML — the integration
  was added on purpose, so an empty index is treated as an error.
