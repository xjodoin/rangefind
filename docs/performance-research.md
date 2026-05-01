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
- "Dynamic Superblock Pruning for Fast Learned Sparse Retrieval", 2025:
  superblock selection can prune groups of blocks before child blocks are
  visited, which maps naturally to Rangefind's static range-fetch model.
  https://arxiv.org/abs/2504.17045
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
blocks are stored in `terms/block-packs/*.bin` and referenced from the term-shard
metadata. The runtime applies the existing block-max schedule before fetching
those blocks, with a small adjacent prefetch window that behaves like a dynamic
superblock for medium lists.

The metadata browse path now uses the same pruning principle. Sorted numeric,
date, and boolean fields get `rfdocvaluesortdir-v1` directories plus
`rfdocvaluesortpage-v1` value pages. Sorted top-k views fetch only the next
value page in sort order and stop when the result page is full. Unsorted range
browsing uses doc-id chunk summaries and early stop so broad filters can keep
the dense doc-page payload lane.
