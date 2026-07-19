---
title: Getting started
lede: From zero to a working search box in about five minutes — no server, no account, no API key.
description: Install rangefind, index a built static site or a JSONL corpus, and query it from the browser with the drop-in search component.
order: 1
permalink: /docs/
---

Rangefind builds a **static search index**: a directory of immutable, packed
files plus one small manifest. You deploy it next to your site like any other
asset, and the browser runtime answers queries by fetching only the byte
ranges it needs with HTTP `Range` requests.

## Install

```bash
npm install rangefind
```

Node ≥ 22 is required for building. Querying happens in any modern browser
(or in Node via [`rangefind/node`](node-runtime/)).

## Option A — index a built site

If any static site generator already produces your HTML, point the crawler at
the output directory:

```bash
npx rangefind build ./dist
```

This writes the index to `./dist/rangefind/`. Titles, headings, and body text
are extracted from each page automatically. To refine what gets indexed, use
attributes in your templates:

| Attribute | Effect |
| --- | --- |
| `data-rangefind-body` | index only the text inside these elements |
| `data-rangefind-ignore` | drop this subtree (page-level on `<html>`/`<body>` skips the page) |
| `data-rangefind-meta="name"` or `"name:attr"` | store element text (or an attribute) as metadata |
| `data-rangefind-filter="key"` | multi-valued facet, value = element text |
| `data-rangefind-sort="key"` | sort value, value = element text |

Then drop the search component into your pages:

```html
<script type="module" src="/_rangefind/rangefind-search.js"></script>
<link rel="stylesheet" href="/_rangefind/rangefind-search.css">

<rangefind-search src="/rangefind/" placeholder="Search…" hotkey></rangefind-search>
```

If you use Eleventy, Astro, Docusaurus, MkDocs, or Hugo, a
[plugin](plugins/) wires all of this into your build automatically —
including copying the component assets.

## Option B — index structured data

For anything that isn't a website — products, places, records — describe a
schema and feed newline-delimited JSON:

```json
{
  "input": "docs.jsonl",
  "output": "public/rangefind",
  "fields": [
    { "name": "title", "path": "title", "weight": 4.5, "phrase": true },
    { "name": "body", "path": "body", "weight": 1.0 }
  ],
  "facets": [{ "name": "topic", "path": "topic" }],
  "numbers": [{ "name": "year", "path": "year", "type": "int" }],
  "display": ["title", "topic", "year"]
}
```

```bash
npx rangefind build --config rangefind.config.json
```

Every relevance, facet, geo, and vector capability is driven from this one
config — see the [build configuration reference](build-configuration/).

## Query it

```js
import { createSearch } from "rangefind";

const rf = await createSearch({ baseUrl: "/rangefind/" });

const res = await rf.search({ q: "harbor lights", size: 10 });
// res.results: [{ id, title, url, score, ... }]

const counts = await rf.count({ q: "harbor" });
const typing = await rf.suggest({ q: "harb" });
```

The same `createSearch` call transparently handles single indexes,
[incrementally updated](incremental-updates/) generational indexes, and
[sharded](sharded-indexes/) indexes — the manifest tells the runtime what
it's looking at.

## Serve it

Any static host that supports HTTP range requests works — which is nearly all
of them. During development:

```bash
npx serve dist   # or your generator's dev server
```

For production hosting, caching headers, and CDN guidance, read
[Hosting the index](hosting/).

## Where next

- [How it works](how-it-works/) — what's actually in those files.
- [Search component](search-component/) — every attribute and CSS hook.
- [Query API](query-api/) — filters, facets, sorting, geo, vectors.
