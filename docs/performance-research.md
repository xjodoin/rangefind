# Performance Research Notes

Rangefind's browser runtime is constrained by network round trips and transferred
bytes as much as by local scoring CPU. These papers are the current design
anchors for the next performance work:

- Ding and Suel, "Faster Top-k Document Retrieval Using Block-Max Indexes",
  SIGIR 2011: block-level maximum scores enable safe early termination without
  exhaustively scoring every posting.
  https://research.engineering.nyu.edu/~suel/papers/bmw.pdf
- Bast et al., "IO-Top-k: Index-access Optimized Top-k Query Processing",
  VLDB 2006: top-k performance depends on scheduling sequential and random
  index accesses, and random accesses need an explicit cost model.
  https://www.vldb.org/conf/2006/p475-bast.pdf
- Mallia et al., "Faster Learned Sparse Retrieval with Block-Max Pruning",
  SIGIR 2024: block-max style pruning remains useful when term distributions
  become wider and less classical under learned sparse representations.
  https://arxiv.org/abs/2405.01117
- LazyBM, SIGIR 2020: range-level upper bounds should be used as a planner
  input, not as a blind replacement for the existing high-impact traversal.
- Dimopoulos, Nepomnyachiy, and Suel, "Optimizing Top-k Document Retrieval
  Strategies for Block-Max Indexes": block-max metadata combines well with
  candidate filtering when the planner can cheaply reject low-ceiling regions.
- "Dynamic Superblock Pruning for Fast Learned Sparse Retrieval", 2025:
  superblock selection can prune groups of blocks before child blocks are
  visited, which maps naturally to Rangefind's static range-fetch model.
  https://arxiv.org/abs/2504.17045
- Schulz and Mihov, "Fast String Correction with Levenshtein Automata", plus
  Daciuk et al.'s minimal acyclic finite-state automata work: typo correction
  should enumerate plausible vocabulary paths before running corrected searches.
- Column-store zone maps and PageIndex-style min/max metadata: page-level
  summaries let range predicates skip whole compression units before fetching
  column payload bytes. Rangefind applies the same idea to browser doc-values
  with lazy sorted directories and per-page summaries.

Current implementation consequence: Rangefind already stores block-max metadata
for posting pruning, but browser cold-query cost also includes random result
payload fetches. Long display fields can dominate transfer bytes even when term
shards are small. Result payloads are therefore packed as independently ranged
gzip members, and `display[].maxChars` caps returned fields independently from
indexed text so large pages can be fully indexed while search results stay
compact.

The next format step is now implemented for high-df terms: eligible posting
blocks are stored in `terms/block-packs/*.bin` and referenced from
`rfsegpost-v6` posting-segment metadata. Each posting block carries doc-id
min/max bounds, sparse per-block doc-id-range max-impact rows, each superblock
carries an aggregate doc-id span, and each term can emit sparse quantized
doc-id-range max-impact rows plus compact impact-tier block references. The
runtime uses those references as a best-first scheduler input: it sums
query-term range upper bounds, lazily opens the most promising doc-id ranges,
orders each term/range queue by local block upper bound and impact tier, scales
the first decode quantum with the requested top-k, and coalesces selected
term/block pairs before issuing range requests. The result is still exact: after
each batch, a global proof compares the kth score with every unopened range,
every partially opened range, and every scored document's remaining per-term
potential.

The doc-id range lane is deliberately guarded by a selectivity check. On the
current frwiki impact-ordered block layout, forcing pure range evaluation can
decode more posting blocks than the existing tail-proof traversal. Range bounds
therefore also feed the stable top-k proof path, but only when the doc-local
bound is strictly tighter than the global unseen bound. This keeps the metadata
generic for future doc-id-local layouts without regressing the present high-df
path.

Typo fallback now follows the same "estimate before execute" rule. The runtime
uses analyzer-normalized query tokens to probe bounded main term shards, filters
the loaded vocabulary by length and n-gram overlap, verifies candidates with a
bounded Damerau-Levenshtein pass, ranks correction plans by document frequency
and posting upper-bound evidence, and executes only the strongest correction
plans through the normal search path.

The metadata browse path now uses the same pruning principle. Sorted numeric,
date, and boolean fields get `rfdocvaluesortdir-v1` directories plus
`rfdocvaluesortpage-v1` value pages. Sorted top-k views fetch only the next
value page in sort order and stop when the result page is full. Unsorted range
browsing uses doc-id chunk summaries and early stop so broad filters can keep
the dense doc-page payload lane.
