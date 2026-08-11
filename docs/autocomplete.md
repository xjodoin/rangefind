# Autocomplete guide

How to build search-as-you-type on Rangefind so it is **fast, stable, and
cheap** — the patterns, what each one costs, and the traps.

The API surface is documented in the [reference](reference.md#other-query-methods);
this guide is about choosing between the options. Every number below was
measured against two real indexes served over HTTP range requests, each
scenario in a cold process (no warm caches):

- **wiki** — 200 documents, small display payloads.
- **OSM Luxembourg** — 339,445 documents, rich display payloads
  (`examples/osm-geo`, ~1 MB per doc page).

---

## The three costs

An autocomplete UI pays for up to three separate things. Keeping them
separate is the whole game, because they differ by three orders of magnitude.

| | What it is | OSM (339k docs) | wiki (200 docs) |
| --- | --- | --- | --- |
| **1. Completions** | `suggest()` — text, weights, counts | **47 KB** / 6 requests | **4.5 KB** / 5 requests |
| **2. Previews** | resolving suggestions into documents | 0.5 KB per focused row → 4.3 MB if bulk-hydrated per keystroke | 90 KB bulk |
| **3. Selection** | turning the pick into a result | **0.5 KB** hydrated · **215 KB** re-searched | — |

*(Costs 1 and 2 are for a whole typed word — seven keystrokes, `p` →
`philhar` — not per character; the lexicon lane caches aggressively, so most
keystrokes after the first cost nothing.)*

Three rules follow directly:

1. **Completions are nearly free.** Never hesitate to call `suggest()` per
   keystroke.
2. **Selection must never re-run the query.** Hydrating the suggestion's
   document is ~430× cheaper than searching for its text (0.5 KB vs 215 KB,
   4 ms vs 32 ms). This is pure win — do it always.
3. **Previews are the only real decision.** They are cheap on small indexes
   and expensive on large ones with rich payloads. Pick a pattern below.

---

## Pattern 1 — Completions only (baseline)

The cheapest possible dropdown: text, and a search when the user commits.

```js
const { suggestions } = await engine.suggest({ q, size: 8 });
// [{ text: "Boulangerie Fischer", weight: 12, count: 12, doc: 41 }, …]
```

Use it when your rows only need text. Even here, **keep `doc`** and use
Pattern 3 for selection.

## Pattern 2 — Unified dropdown (recommended for small/medium indexes)

`hydrate: true` resolves each suggestion's documents into real search hits, so
the dropdown renders the *same cards* the results list does — the single
biggest fix for "autocomplete shows different information than search".

```js
const { suggestions } = await engine.suggest({ q, size: 8, hydrate: true });

for (const item of suggestions) {
  if (item.result) renderResultCard(item.result);   // same shape as search()
  else renderTextRow(item.text, item.count);        // ambiguous/composed rows
}
```

- `result` — the best document behind the surface.
- `results` — every kept document row (`suggestMaxDocRows`, default 3), useful
  for "3 locations" style previews.
- Hydration is **advisory**: if it fails, the text suggestions still arrive.

**Cost:** one batched doc-payload pass. 90 KB for a whole typed word on the
wiki index — negligible. On a large index with rich payloads it is not
negligible (786 KB via `engine.suggest`, 4.3 MB via `suggestOsmQuery`, which
overfetches candidates before collapsing): use Pattern 2b there.

## Pattern 2b — Focus previews (recommended for large indexes)

Hydrating **one** document is two small range reads (~0.4 KB). Hydrating eight
scattered ones must touch eight distinct doc pages (~1 MB each on OSM). So on
a big index, preview the row the user is actually considering rather than all
of them:

```js
// Completions per keystroke (cheap), previews per focused row (cheap).
const response = await suggestOsmQuery(engine, { q, size: 8, near });
renderRows(response.suggestions);
await hydrateOsmSuggestions(engine, response.suggestions.slice(0, 1)); // top row
// …and on hover / arrow-key focus:
await hydrateOsmSuggestions(engine, [response.suggestions[focusedIndex]]);
```

Measured on OSM for the same seven-keystroke session, previewing the top row
of every query plus three arrowed rows: **509 KB**, versus 4,259 KB
bulk-hydrating every row of every keystroke — 8× cheaper for the previews a
user actually looks at. Each additional focused row is ~0.4 KB.

The generic equivalent outside the OSM integration is `hydrateRows` with the
suggestion's provenance (see Pattern 3).

`examples/osm-geo` implements exactly this: the focused row gets a map pin and
a distance label; pins accumulate as the user arrows through and clear with
the dropdown.

## Pattern 3 — Instant selection (always do this)

A suggestion knows which document it names. Use it instead of searching for
its own text.

```js
const picked = suggestions[selectedIndex];

if (picked.doc != null) {
  const [document] = await engine.hydrateRows([[picked.doc, 0]], {
    ...(picked.docShard ? { shard: picked.docShard } : {}),        // sharded roots
    ...(picked.generation != null ? { generation: picked.generation } : {}) // generational
  });
  showDocument(document);          // 0.5 KB, ~4 ms
} else {
  runSearch(picked.text);          // composed/ambiguous rows still need the planner
}
```

With `hydrate: true` this is already done for you — `picked.result` *is* the
document, so selection costs nothing at all.

When `doc` is absent the suggestion is genuinely ambiguous (a shared surface,
a collapsed street, a category composition, or a cross-shard rank tie): fall
back to a search, and scope it with `picked.shards` when present so it does
not fan out.

OSM callers get this as `resolveOsmSuggestion(engine, suggestion)` (0.5 KB,
`plannerLane: "osmSuggestEntity"`).

## Pattern 4 — Speculative warming

The top completion is stable across keystrokes ("boulan" and "boulang" both
complete to "boulangerie"), so the search for it can be fetched *before* the
user commits:

```js
const { suggestions } = await engine.suggest({ q, size: 8 });
renderRows(suggestions);
warmSearch(suggestions[0]?.text);  // memoized by query; result arrives before Enter
```

`<rangefind-search>` does this automatically with a 16-entry LRU, so accepting
a suggestion renders from cache.

---

## Choosing a pattern

| Index shape | Completions | Previews | Selection |
| --- | --- | --- | --- |
| Small/medium (docs site, catalog, ≲100k small payloads) | per keystroke | **Pattern 2** (`hydrate: true`) | Pattern 3 |
| Large / rich payloads (OSM-scale, big display fields) | per keystroke | **Pattern 2b** (focus previews) | Pattern 3 |
| Metered or offline clients ([mobile](mobile.md)) | per keystroke | focus previews, or none | Pattern 3 |

Not sure? Measure it: hydration cost is dominated by your **doc page size**
(display payload × `docPageSize`), not by document count. Count the bytes for
one `suggest({ hydrate: true })` on your own index and decide.

---

## Build-time tuning

Configure `suggest` fields ([reference](reference.md#suggest)); the knobs that
matter for autocomplete quality and cost:

| Setting | Default | Effect |
| --- | --- | --- |
| `weightPath` | — | The ranking signal (population, popularity, importance). Without it, direct-prefix priority then popularity decides. **The single biggest quality lever.** |
| `suggestMaxDocRows` | `3` | Documents kept per surface — what `docs`/`results` can preview. `1` is enough if you only ever use `result`; it shrinks the sidecar slightly. |
| `suggestMaxTokenKeys` | `4` | Mid-label completion ("eiffel" → "Tour Eiffel"). More keys = more surfaces = a bigger lexicon. |
| `suggestMinKeyLength` | `1` | Drops very short keys from the lexicon. |
| `suggestHotListSize` | `64` | Size of the precomputed first-keystroke lists. Raise it if your UI shows more than 8 rows, so early keystrokes stay on the constant-cost hot lane. |

Rank quality also depends on the surfaces themselves: index the *display*
name, and let `count` (documents sharing a surface) carry ambiguity into the
UI rather than hiding it — showing "×42" next to a completion is what makes a
partial result read as honest instead of broken.

---

## Federation notes

Everything above works on sharded and generational indexes; the difference is
provenance.

- **Sharded roots** answer autocomplete from the root suggest-routing artifact
  — no shard is opened (`stats.suggestRouting: "root-authority"`). Since
  `rfsuggestroute-v2` the artifact also carries the winning row's document, so
  suggestions have `doc` + `docShard` and Pattern 3 works without a fan-out.
  Older `rfsuggestroute-v1` artifacts have no document: selection falls back
  to a shard-scoped search (correct, just not instant). Rebuild the routing
  artifact after shard rebuilds — see [sharded OSM](sharded-osm.md).
- **Generational indexes** stamp `generation`; tombstoned documents are
  filtered out of suggestion provenance, so a stale surface keeps its text but
  loses its `doc` rather than resolving to a deleted document.
- **Cross-shard / cross-generation rank ties** clear `doc` on purpose: no
  single owner means no unambiguous document to open.

---

## Anti-patterns

Each of these has a measured cost.

- **Re-running the query when a suggestion is picked.** 215 KB and 32 ms
  instead of 0.5 KB and 4 ms, and the results can rank differently from the
  dropdown — the "why does clicking change what I saw?" bug. Use Pattern 3.
- **Bulk-hydrating a rich-payload index on every keystroke.** 4.3 MB per typed
  word on OSM. Use focus previews.
- **Hand-rolling `hydrateRows` for several suggestions without a lane hint.**
  Suggestion documents are scattered corpus-wide, so the packed lane's pointer
  reads merge into ranges covering most of the dense pointer file — measured
  13.6 MB for 2 documents on the OSM index, versus 0.43 MB via doc pages. The
  built-in hydration paths pick the lane for you
  (`suggestionHydrationContext`); if you call `hydrateRows` yourself for more
  than one suggestion, pass `{ preferDocPages: "force" }`. For a *single*
  document the default (packed) is already optimal.
- **Skipping the debounce, or not caching by query.** Completions are cheap
  but not free; an LRU keyed by the typed prefix removes almost all repeat
  work (the demos cache 16–24 entries).
- **`size: 50` "just in case".** You pay ranking and (with `hydrate`)
  hydration for rows nobody sees. Ask for what you render.
- **Prefix-expanding a text query instead of using `suggest()`.** On a
  planet-scale index, prefix expansion through the main term index fans out to
  thousands of requests; the lexicon answers the same keystroke in one or two.

---

## Checklist

- [ ] `suggest` fields configured, with `weightPath` if you have a popularity
      signal.
- [ ] Completions requested per keystroke, debounced and cached by query.
- [ ] Selection uses `doc` (+ `docShard`/`generation`), never a re-search.
- [ ] Previews match the index shape: bulk `hydrate` when small, focused when
      large.
- [ ] Ambiguity surfaced (`count`), not hidden.
- [ ] On sharded roots: suggest-routing artifact rebuilt as
      `rfsuggestroute-v2` so selection is instant.
- [ ] Dropdown rows and result cards render through the *same* component.
