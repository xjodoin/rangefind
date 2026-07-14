---
title: Search component
lede: An accessible, dependency-free web component — the search box at the top of this page is one.
description: The <rangefind-search> web component — every attribute, keyboard behavior, theming through CSS custom properties, and class hooks for Tailwind or custom CSS.
order: 8
---

```html
<script type="module" src="/_rangefind/rangefind-search.js"></script>
<link rel="stylesheet" href="/_rangefind/rangefind-search.css">

<rangefind-search src="/rangefind/" placeholder="Search…" hotkey></rangefind-search>
```

Both files ship inside the npm package (`rangefind/element` and
`rangefind/element.css`); the [plugins](../plugins/) copy them into your
build output automatically. The stylesheet is **opt-in** — the component
renders semantic, unstyled light DOM you can style however you like.

## Attributes

| Attribute | Default | Meaning |
| --- | --- | --- |
| `src` | `/rangefind/` | index base URL |
| `placeholder` | `Search` | input placeholder |
| `page-size` | 10 | results per page |
| `debounce` | — | input debounce in ms |
| `min-length` | — | minimum query length before searching |
| `highlight` | on | mark matched terms in snippets (`highlight="false"` to disable) |
| `suggest` | — | show autocomplete suggestions while typing |
| `router` | off | update `?q=` in the URL and restore on load |
| `open-on-focus` | off | open the panel when the input focuses |
| `hotkey` | off | focus the box with <kbd>/</kbd> or <kbd>⌘K</kbd> |
| `label` | — | accessible label for the input |
| `empty-text`, `loading-text`, `error-text` | sensible | state messages |

Booleans accept bare attributes (`hotkey`) or explicit values
(`highlight="false"`).

## Accessibility and keyboard

The component renders a combobox pattern: `role="combobox"` input,
`aria-expanded`, an options listbox with `aria-activedescendant`, arrow-key
navigation, <kbd>Enter</kbd> to open the active result, <kbd>Esc</kbd> to
close. Status text (result counts, loading, errors) lives in a polite live
region.

## Theming

The default stylesheet exposes custom properties — override them on
`.rf-search` and the component follows your palette in light and dark:

```css
.rf-search {
  --rf-accent: #0e6f63;
  --rf-mark-bg: #ffc940;   /* match highlight */
  --rf-radius: 8px;
  --rf-bg: var(--your-surface);
  --rf-panel-bg: var(--your-surface);
}
```

Prefer utility classes? Skip the stylesheet and target the part hooks with
`*-class` attributes instead — `input-class`, `panel-class`, `option-class`,
`option-title-class`, `option-snippet-class`, `mark-class`, and friends map
straight onto Tailwind:

```html
<rangefind-search src="/rangefind/"
  input-class="w-full rounded-lg border px-3 py-2"
  panel-class="rounded-xl border bg-white shadow-xl"
  option-class="px-3 py-2 hover:bg-amber-50"
  mark-class="bg-amber-300 rounded-sm">
</rangefind-search>
```

## Semantic search in the component

For per-query behavior — like embedding the query for
[hybrid semantic search](../query-api/#vectors-and-hybrid) — set an async
`transform` on the element's `searchOptions`. It receives the params the
component is about to send and returns the params to use:

```js
const box = document.querySelector("rangefind-search");
box.searchOptions = {
  transform: async params => {
    if (params.q) params.vector = await embedQuery(params.q); // your model
    return params;
  }
};
```

The search boxes on this site do exactly this: pages are embedded at build
time through the Eleventy plugin's `enrich` hook, and the browser lazily
loads the same MiniLM model to embed queries — lexical results are instant,
and rankings upgrade to hybrid once the model is warm. Static `search`
params (extra filters, facets) can be set the same way via
`searchOptions.search`.

## In static site generators

Every plugin renders this markup for you with a shortcode or component —
`{% raw %}{% rangefindSearch %}{% endraw %}` in Eleventy,
`<RangefindSearch />` in Astro, and the equivalents for Docusaurus and
MkDocs. See [Plugins](../plugins/).

For fully custom UIs, skip the component and drive the
[query API](../query-api/) directly — the component is a convenience, not a
requirement.
