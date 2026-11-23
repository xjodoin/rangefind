# Porting an engine lane (parallel work)

The Rust query engine (`rust/rangefind/src/engine/`) is a port of
`createSearch()` in `src/runtime.js`. The core is done and compiles: lazy
manifests, directories, posting segments, external blocks, doc values,
filter plans, filter bitmaps, doc hydration, every text lane (block-max
tail proof, doc-range planner, conjunction tail, full scan, count), browse
and sorted lanes, facet counts and the rank-prior boost. Several lanes are
**stubs** that return `None`/errors; each stub file is owned by exactly one
person. Read `rust/PORTING.md` too — its rules on JS semantics apply.

## Rules

- **Edit only your file(s).** Other people are editing sibling stub files in
  the same crate at the same time. If you need a small hook in a shared core
  file (`search.rs`, `text.rs`, `loaders.rs`, `filters.rs`, `docs.rs`,
  `index.rs`, `codec/*`), keep it minimal, additive, and mention it in your
  final report. Never refactor shared code.
- **Keep the crate compiling at every save.** Others build the same crate;
  a broken save in your file breaks their build. Write in small compiling
  steps. If someone else's file is broken when you build, wait 30–60 s and
  retry instead of touching it.
- **Use your own target dir:**
  `cd rust/rangefind && PATH=/opt/homebrew/opt/rustup/bin:$PATH CARGO_TARGET_DIR=target/<lane> cargo test --release --test engine_parity`
- **The JS runtime is the spec.** Port the exact behavior, including stats
  keys, tie-breaks, `||` vs `??` defaults, and quirks such as numbers leaking
  into booleans (`approximate: 0`). Response objects must have the same keys
  in the same order where reasonable (`json!` / `serde_json::Map` preserve
  insertion order).

## Engine APIs you will use (read the files for details)

- `IndexEngine` (`engine/index.rs`): `manifest_at(&[..])`, `url(relative)`,
  `io` (`fetch`, `fetch_gzip`, `fetch_range(s)`, `fetch_manifest_json`),
  `options` (`EngineOptions`: typed knobs plus `get(key)` for raw ones),
  `analyzer`, `total()`, `verify_checksums`, `info`, the `PromiseCache`
  fields, and `extension::<T>(key)` / `set_extension(key, Rc<T>)` for lane
  state that needs a cache (use these instead of adding fields to the struct).
- Loaders (`engine/loaders.rs`): `load_batch(cache, items, plan, url_for,
  decode)` — the generic reserve→group→fetch→decode loader every batched
  read should use; `term_entries`, `load_directory_root/page`,
  `resolve_directory_shard`, `directory_entries_for_prefix`,
  `decode_entry_postings`, `lookup_entry_blocks`, doc values
  (`ensure_doc_values_for_docs`, `doc_value`), sorted pages, facet
  dictionaries, filter bitmaps, `ensure_*_manifest`.
- Filters (`engine/filters.rs`): `Filters`, `make_doc_filter_plan`,
  `make_block_filter_plan`, `passes_filter_plan`, `value_store_for_*`,
  `block_may_pass`, `words_match`.
- Docs (`engine/docs.rs`): `rows_to_results`, `rows_to_search_results`,
  `ResultContext` (+ `stats(&mut map, full)` for `docPayload*` keys),
  `result_object`, `Row = (i32 doc, f64 score)`.
- Text (`engine/text.rs`): `TextRequest`, `ProofStats::write`,
  `empty_rerank_stats`, `rerank_with_dependencies`, `run_full_search`,
  `run_skipped_text_search`, `collect_text_match_docs`, `Accumulator`.
- Search (`engine/search.rs`): `execute_search`, `make_sort_plan`,
  `sort_ranked`, `resolve_query_plan`, `compute_facet_counts`, `js_round`.
- Module ports (all parity-verified against JS): `crate::analysis`,
  `crate::terms`, `crate::highlight`, `crate::codec::*`, `crate::geo::*`,
  `crate::authority::*`, `crate::vector`, `crate::typo`. Use them; do not
  re-implement their logic.
- Async: everything is single-threaded `async`. Use `crate::rt::{join_all,
  try_join_all, PromiseCache, PromiseSlot}` for concurrency and caching.
  Caches of decoded objects join the engine's memory budget: build them
  with `PromiseCache::budgeted(self.cache_budget(), "name")` and implement
  `crate::budget::CacheCost` for the value (see `engine/cache_cost.rs`).
  Entries a running call touched are never evicted, so "load, then read
  back with `ready`" is safe within a call; never key a cache by an
  object's address (`TermEntry::uid` exists for per-entry caches).

## Verifying

Fixtures (already built, do not rebuild): `rust/target/parity/<index>/index`
plus `<index>.cases.json` with the JS responses. The parity test compares
every case; narrow it and dump all failures:

```
RANGEFIND_PARITY_ONLY=geo,geocapsules \
RANGEFIND_PARITY_DUMP=$PWD/target/<lane>-fail.txt \
PATH=/opt/homebrew/opt/rustup/bin:$PATH CARGO_TARGET_DIR=target/<lane> \
cargo test --release --test engine_parity
```

`RANGEFIND_PARITY_STATS=1` also prints which stats keys differ anywhere.
Use `node` with `src/runtime.node.js` (`createNodeSearch({ baseUrl:
pathToFileURL(dir + "/").href })`) to inspect the full JS response for any
case. Failures in lanes you do not own are expected; your goal is zero
failures attributable to your lane. Finish with a short report: what you
ported, remaining failures in your lane (with the reason), and any shared
file you touched.
